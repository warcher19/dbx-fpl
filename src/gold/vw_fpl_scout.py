"""
Gold transfer recommendation surface.

Shows each player's current form, value metrics, and upcoming fixture
difficulty rating (FDR) for the next 3 gameweeks. Intended as the
primary read surface for the MCP agent and AI/BI dashboards.
"""
from pyspark import pipelines as dp


@dp.materialized_view(
    name="fpl.gold.vw_fpl_scout",
    comment=(
        "Transfer recommendation view — current form, value-for-money, and "
        "upcoming fixture difficulty for all active FPL players."
    ),
    cluster_by_auto=True,
)
def vw_fpl_scout():
    return spark.sql("""
        WITH latest_gw AS (
            -- Most recent completed gameweek from the API (current season only)
            SELECT MAX(gameweek) AS gw
            FROM fpl.gold.fact_gameweek_performance
            WHERE data_source = 'api'
        ),

        current_form AS (
            SELECT
                f.player_name,
                p.team_name,
                p.position,
                p.price_m,
                f.rolling_5gw_pts   AS form,
                f.points_per_million AS value_for_money,
                f.points_per_90,
                f.season_total_pts,
                f.rolling_5gw_xG,
                f.rolling_5gw_xA,
                f.xGI,
                f.ict_index
            FROM fpl.gold.fact_gameweek_performance f
            JOIN fpl.gold.dim_player p ON f.player_name = p.full_name
            JOIN latest_gw l ON f.gameweek = l.gw
            WHERE f.data_source = 'api'
        ),

        upcoming_fixtures AS (
            -- Average FDR over the next 3 unplayed fixtures per team
            -- Split into home/away unions so the optimizer can push down each join
            SELECT
                p.full_name AS player_name,
                ROUND(AVG(fdr), 2) AS avg_upcoming_fdr,
                COUNT(*) AS upcoming_fixtures_count
            FROM fpl.gold.dim_player p
            JOIN (
                SELECT home_team_id AS team_id, home_difficulty AS fdr, gameweek, finished
                FROM fpl.gold.dim_fixture
                UNION ALL
                SELECT away_team_id AS team_id, away_difficulty AS fdr, gameweek, finished
                FROM fpl.gold.dim_fixture
            ) f ON p.team_id = f.team_id
            JOIN latest_gw l ON f.gameweek > l.gw AND f.gameweek <= l.gw + 3
            WHERE f.finished = FALSE OR f.finished IS NULL
            GROUP BY p.full_name
        )

        SELECT
            cf.*,
            COALESCE(uf.avg_upcoming_fdr, 3)        AS avg_upcoming_fdr,
            COALESCE(uf.upcoming_fixtures_count, 0)  AS upcoming_fixtures_count,
            -- Composite scout score: high points, low FDR, good value
            ROUND(
                (cf.form * 0.4)
                + (cf.value_for_money * 0.3)
                + ((5 - COALESCE(uf.avg_upcoming_fdr, 3)) * 0.3),
            3) AS scout_score
        FROM current_form cf
        LEFT JOIN upcoming_fixtures uf ON cf.player_name = uf.player_name
        ORDER BY scout_score DESC
    """)

"""
Gold fact table: unified gameweek performance across all seasons.

Sources:
  - fact_player_performance  (current season, from FPL API)
  - fact_historical_performance (2016-17 → 2024-25, from vaastav)

Rolling metrics (5-game window, season totals) are computed here using
Spark window functions so they are always consistent regardless of data source.
"""
from pyspark import pipelines as dp


@dp.materialized_view(
    name="fpl.gold.fact_gameweek_performance",
    comment=(
        "Unified gameweek performance: FPL API (current season) + vaastav historical (2016-25). "
        "Includes rolling form, points-per-90, and value metrics."
    ),
    cluster_by_auto=True,
    table_properties={"delta.enableRowTracking": "true"},
)
def fact_gameweek_performance():
    return spark.sql("""
        WITH current_season AS (
            SELECT
                p.full_name                                             AS player_name,
                p.position,
                p.team_name,
                fp.was_home,
                '2025-26'                                               AS season,
                f.gameweek,
                f.kickoff_time,
                fp.total_points,
                fp.minutes,
                fp.goals_scored,
                fp.assists,
                fp.clean_sheets,
                fp.goals_conceded,
                fp.own_goals,
                fp.penalties_missed,
                fp.penalties_saved,
                fp.yellow_cards,
                fp.red_cards,
                fp.saves,
                fp.bonus,
                fp.bps,
                fp.expected_goals                   AS xG,
                fp.expected_assists                 AS xA,
                fp.expected_goal_involvements       AS xGI,
                fp.expected_goals_conceded          AS xGC,
                fp.influence,
                fp.creativity,
                fp.threat,
                fp.ict_index,
                fp.value / 10.0                     AS price_m,
                fp.selected,
                fp.transfers_balance,
                CASE WHEN fp.was_home
                    THEN f.away_difficulty
                    ELSE f.home_difficulty
                END                                 AS fixture_difficulty,
                'api'                               AS data_source
            FROM fact_player_performance fp
            JOIN dim_fixture   f  ON fp.fixture_id  = f.fixture_id
            JOIN fpl.gold.dim_player p ON fp.player_id = p.player_id
        ),

        historical AS (
            SELECT
                player_name,
                position,
                team                                AS team_name,
                was_home,
                season,
                gameweek,
                kickoff_time,
                total_points,
                minutes,
                goals_scored,
                assists,
                clean_sheets,
                goals_conceded,
                own_goals,
                penalties_missed,
                penalties_saved,
                yellow_cards,
                red_cards,
                saves,
                bonus,
                bps,
                expected_goals                      AS xG,
                expected_assists                    AS xA,
                expected_goal_involvements          AS xGI,
                expected_goals_conceded             AS xGC,
                influence,
                creativity,
                threat,
                ict_index,
                price_m,
                selected,
                (transfers_in - transfers_out)      AS transfers_balance,
                NULL                                AS fixture_difficulty,
                data_source
            FROM fact_historical_performance
        ),

        combined AS (
            SELECT * FROM current_season
            UNION ALL
            SELECT * FROM historical
        )

        SELECT
            *,
            -- 5-game rolling average points (form indicator)
            AVG(total_points) OVER (
                PARTITION BY player_name
                ORDER BY kickoff_time
                ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
            )                                       AS rolling_5gw_pts,

            -- 5-game rolling xG
            AVG(xG) OVER (
                PARTITION BY player_name
                ORDER BY kickoff_time
                ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
            )                                       AS rolling_5gw_xG,

            -- 5-game rolling xA
            AVG(xA) OVER (
                PARTITION BY player_name
                ORDER BY kickoff_time
                ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
            )                                       AS rolling_5gw_xA,

            -- Season cumulative total
            SUM(total_points) OVER (
                PARTITION BY player_name, season
            )                                       AS season_total_pts,

            -- Value metrics
            ROUND(total_points / NULLIF(price_m, 0), 3)             AS points_per_million,
            ROUND(total_points / NULLIF(minutes / 90.0, 0), 3)      AS points_per_90
        FROM combined
    """)

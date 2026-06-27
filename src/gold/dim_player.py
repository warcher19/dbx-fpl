from pyspark import pipelines as dp


@dp.materialized_view(
    name="fpl.gold.dim_player",
    comment="Enriched player dimension with team name and readable position label",
    cluster_by_auto=True,
)
def dim_player():
    return spark.sql("""
        SELECT
            p.player_id,
            p.first_name,
            p.second_name,
            p.first_name || ' ' || p.second_name AS full_name,
            p.web_name,
            t.name        AS team_name,
            t.short_name  AS team_short,
            CASE p.position_id
                WHEN 1 THEN 'GK'
                WHEN 2 THEN 'DEF'
                WHEN 3 THEN 'MID'
                WHEN 4 THEN 'FWD'
            END           AS position,
            p.current_price / 10.0 AS price_m,
            p.team_id,
            p.position_id
        FROM dim_player   p
        JOIN dim_team     t ON p.team_id = t.team_id
    """)

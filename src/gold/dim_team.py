from pyspark import pipelines as dp


@dp.materialized_view(
    name="fpl.gold.dim_team",
    comment="Premier League team dimension with all Opta strength metrics",
    cluster_by_auto=True,
)
def dim_team():
    return spark.sql("""
        SELECT
            team_id,
            name,
            short_name,
            strength,
            strength_overall_home,
            strength_overall_away,
            strength_attack_home,
            strength_attack_away,
            strength_defence_home,
            strength_defence_away
        FROM dim_team
    """)

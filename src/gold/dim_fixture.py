from pyspark import pipelines as dp


@dp.materialized_view(
    name="fpl.gold.dim_fixture",
    comment="FPL fixture dimension enriched with team names",
    cluster_by_auto=True,
)
def dim_fixture():
    return spark.sql("""
        SELECT
            f.fixture_id,
            f.gameweek,
            f.kickoff_time,
            f.home_team_id,
            f.away_team_id,
            ht.name        AS home_team_name,
            at.name        AS away_team_name,
            f.home_score,
            f.away_score,
            f.home_difficulty,
            f.away_difficulty,
            f.finished
        FROM dim_fixture f
        LEFT JOIN dim_team ht ON f.home_team_id = ht.team_id
        LEFT JOIN dim_team at ON f.away_team_id = at.team_id
    """)

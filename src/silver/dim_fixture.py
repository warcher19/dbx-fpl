from pyspark import pipelines as dp
from pyspark.sql.functions import col, to_timestamp


@dp.expect("valid_gameweek", "gameweek BETWEEN 1 AND 38")
@dp.expect_or_drop("has_fixture_id", "fixture_id IS NOT NULL")
@dp.temporary_view()
def fixtures_stream():
    """Cast and standardise fixture fields before CDC processing."""
    return (
        spark.readStream.table("fpl.bronze.fixtures")
        .select(
            col("id").cast("long").alias("fixture_id"),
            col("event").cast("integer").alias("gameweek"),
            to_timestamp(col("kickoff_time")).alias("kickoff_time"),
            col("team_h").cast("long").alias("home_team_id"),
            col("team_a").cast("long").alias("away_team_id"),
            col("team_h_score").cast("integer").alias("home_score"),
            col("team_a_score").cast("integer").alias("away_score"),
            col("team_h_difficulty").cast("integer").alias("home_difficulty"),
            col("team_a_difficulty").cast("integer").alias("away_difficulty"),
            col("finished").cast("boolean"),
            col("_ingestion_timestamp"),
        )
    )


dp.create_streaming_table(
    name="dim_fixture",
    comment="FPL fixture/match dimension — SCD Type 1 (scores updated post-match)",
    cluster_by_auto=True,
)

dp.create_auto_cdc_flow(
    target="dim_fixture",
    source="fixtures_stream",
    keys=["fixture_id"],
    sequence_by=col("_ingestion_timestamp"),
    stored_as_scd_type="1",
    ignore_null_updates=False,
)

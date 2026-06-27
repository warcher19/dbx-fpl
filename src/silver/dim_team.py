from pyspark import pipelines as dp
from pyspark.sql.functions import col, explode


@dp.temporary_view()
def bootstrap_teams_stream():
    """Explode teams array from bootstrap snapshot before CDC processing."""
    return (
        spark.readStream.table("fpl.bronze.bootstrap_static")
        .select(explode("teams").alias("t"))
        .select(
            col("t.id").cast("long").alias("team_id"),
            col("t.name"),
            col("t.short_name"),
            col("t.strength").cast("integer"),
            col("t.strength_overall_home").cast("integer"),
            col("t.strength_overall_away").cast("integer"),
            col("t.strength_attack_home").cast("integer"),
            col("t.strength_attack_away").cast("integer"),
            col("t.strength_defence_home").cast("integer"),
            col("t.strength_defence_away").cast("integer"),
            col("_ingestion_timestamp"),
        )
    )


dp.create_streaming_table(
    name="dim_team",
    comment="Current Premier League team dimension — SCD Type 1",
    cluster_by_auto=True,
)

dp.create_auto_cdc_flow(
    target="dim_team",
    source="bootstrap_teams_stream",
    keys=["team_id"],
    sequence_by=col("_ingestion_timestamp"),
    stored_as_scd_type="1",
    ignore_null_updates=True,
)

from pyspark import pipelines as dp
from pyspark.sql.functions import col, explode, current_timestamp


@dp.temporary_view()
def bootstrap_players_stream():
    """Explode elements array from bootstrap snapshot before CDC processing."""
    return (
        spark.readStream.table("fpl.bronze.bootstrap_static")
        .select(explode("elements").alias("e"))
        .select(
            col("e.id").cast("long").alias("player_id"),
            col("e.first_name"),
            col("e.second_name"),
            col("e.team").cast("long").alias("team_id"),
            col("e.element_type").cast("integer").alias("position_id"),
            col("e.now_cost").cast("double").alias("current_price"),
            col("e.web_name"),
            col("_ingestion_timestamp"),
        )
    )


dp.create_streaming_table(
    name="dim_player",
    comment="Current FPL player dimension — SCD Type 1 (latest state per player)",
    cluster_by_auto=True,
)

dp.create_auto_cdc_flow(
    target="dim_player",
    source="bootstrap_players_stream",
    keys=["player_id"],
    sequence_by=col("_ingestion_timestamp"),
    stored_as_scd_type="1",
    ignore_null_updates=True,
)

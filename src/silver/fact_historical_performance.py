"""
Silver transformation of vaastav historical gameweek data.

CDC key is composite (player_name, season, gameweek) because vaastav CSVs have
no integer player_id — names are the only cross-season identity available.
"""
from pyspark import pipelines as dp
from pyspark.sql.functions import col, to_timestamp, lit


@dp.expect_or_drop("valid_player_name", "player_name IS NOT NULL")
@dp.expect_or_drop("valid_season",      "season IS NOT NULL")
@dp.expect_or_drop("valid_gameweek",    "gameweek BETWEEN 1 AND 38")
@dp.temporary_view()
def historical_gw_stream():
    """
    Standardise vaastav CSV columns to match fact_player_performance schema.
    Adds data_source column for lineage tracking in the Gold UNION.
    """
    return (
        spark.readStream.table("fpl.bronze.historical_gw_raw")
        .select(
            col("name").alias("player_name"),
            col("position"),
            col("team"),
            col("season"),
            col("round").cast("integer").alias("gameweek"),
            col("opponent_team").cast("long").alias("opponent_team_id"),
            col("was_home").cast("boolean"),
            to_timestamp(col("kickoff_time")).alias("kickoff_time"),
            col("total_points").cast("integer"),
            col("minutes").cast("integer"),
            col("goals_scored").cast("integer"),
            col("assists").cast("integer"),
            col("clean_sheets").cast("integer"),
            col("goals_conceded").cast("integer"),
            col("own_goals").cast("integer"),
            col("penalties_saved").cast("integer"),
            col("penalties_missed").cast("integer"),
            col("yellow_cards").cast("integer"),
            col("red_cards").cast("integer"),
            col("saves").cast("integer"),
            col("bonus").cast("integer"),
            col("bps").cast("integer"),
            col("influence").cast("double"),
            col("creativity").cast("double"),
            col("threat").cast("double"),
            col("ict_index").cast("double"),
            col("expected_goals").cast("double"),
            col("expected_assists").cast("double"),
            col("expected_goal_involvements").cast("double"),
            col("expected_goals_conceded").cast("double"),
            # vaastav stores value in £0.1m units — divide by 10 for £m
            (col("value").cast("double") / 10.0).alias("price_m"),
            col("selected").cast("integer"),
            col("transfers_in").cast("integer"),
            col("transfers_out").cast("integer"),
            lit("vaastav").alias("data_source"),
            col("_ingestion_timestamp"),
        )
        .filter(col("player_name").isNotNull() & col("season").isNotNull())
    )


dp.create_streaming_table(
    name="fact_historical_performance",
    comment="Historical FPL gameweek performance from vaastav (2016-17 to 2024-25) — SCD Type 1",
    cluster_by_auto=True,
)

dp.create_auto_cdc_flow(
    target="fact_historical_performance",
    source="historical_gw_stream",
    keys=["player_name", "season", "gameweek"],
    sequence_by=col("_ingestion_timestamp"),
    stored_as_scd_type="1",
    ignore_null_updates=False,
)

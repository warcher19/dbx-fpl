from pyspark import pipelines as dp
from pyspark.sql.functions import col, explode, to_timestamp


@dp.expect_or_drop("valid_player",  "player_id IS NOT NULL")
@dp.expect_or_drop("valid_fixture", "fixture_id IS NOT NULL")
@dp.expect("valid_minutes",         "minutes >= 0 AND minutes <= 120")
@dp.temporary_view()
def element_history_stream():
    """
    Explode the history array from element-summary responses.
    Each row is one player's performance in one fixture for the current season.
    """
    return (
        spark.readStream.table("fpl.bronze.element_data")
        .select(explode("history").alias("h"))
        .select(
            col("h.element").cast("long").alias("player_id"),
            col("h.fixture").cast("long").alias("fixture_id"),
            col("h.opponent_team").cast("long").alias("opponent_team_id"),
            col("h.total_points").cast("integer"),
            col("h.was_home").cast("boolean"),
            to_timestamp(col("h.kickoff_time")).alias("kickoff_time"),
            col("h.team_h_score").cast("integer"),
            col("h.team_a_score").cast("integer"),
            col("h.round").cast("integer").alias("gameweek"),
            col("h.minutes").cast("integer"),
            col("h.goals_scored").cast("integer"),
            col("h.assists").cast("integer"),
            col("h.clean_sheets").cast("integer"),
            col("h.goals_conceded").cast("integer"),
            col("h.own_goals").cast("integer"),
            col("h.penalties_saved").cast("integer"),
            col("h.penalties_missed").cast("integer"),
            col("h.yellow_cards").cast("integer"),
            col("h.red_cards").cast("integer"),
            col("h.saves").cast("integer"),
            col("h.bonus").cast("integer"),
            col("h.bps").cast("integer"),
            col("h.influence").cast("double"),
            col("h.creativity").cast("double"),
            col("h.threat").cast("double"),
            col("h.ict_index").cast("double"),
            col("h.starts").cast("integer"),
            col("h.expected_goals").cast("double"),
            col("h.expected_assists").cast("double"),
            col("h.expected_goal_involvements").cast("double"),
            col("h.expected_goals_conceded").cast("double"),
            col("h.value").cast("integer"),
            col("h.transfers_balance").cast("integer"),
            col("h.selected").cast("integer"),
            col("h.transfers_in").cast("integer"),
            col("h.transfers_out").cast("integer"),
            col("h.clearances_blocks_interceptions").cast("integer"),
            col("h.recoveries").cast("integer"),
            col("h.tackles").cast("integer"),
            col("_ingestion_timestamp"),
        )
    )


dp.create_streaming_table(
    name="fact_player_performance",
    comment="Current-season player performance per fixture — SCD Type 1",
    cluster_by_auto=True,
)

dp.create_auto_cdc_flow(
    target="fact_player_performance",
    source="element_history_stream",
    keys=["player_id", "fixture_id"],
    sequence_by=col("_ingestion_timestamp"),
    stored_as_scd_type="1",
    ignore_null_updates=False,
)

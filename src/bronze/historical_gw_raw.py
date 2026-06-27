"""
Bronze ingestion for vaastav/Fantasy-Premier-League historical gameweek data.

Uses a one-time Backfill Flow (once=True) — runs on the first pipeline update
and never again. Subsequent pipeline runs skip this flow automatically.

Source: 9 seasons of merged_gws.csv (2016-17 → 2024-25) uploaded to the
landing zone by scripts/download_vaastav_history.py before the first run.
"""
from pyspark import pipelines as dp
from pyspark.sql.functions import current_timestamp, col, regexp_extract
LANDING = "/Volumes/fpl/bronze/landing"
SCHEMA_LOC = "/Volumes/fpl/bronze/autoloader_meta/historical"

# Shell table — populated exclusively by the backfill flow below
dp.create_streaming_table(
    name="fpl.bronze.historical_gw_raw",
    comment="Raw historical gameweek CSVs from vaastav/Fantasy-Premier-League (2016-17 to 2024-25)",
    cluster_by_auto=True,
)


@dp.append_flow(target="fpl.bronze.historical_gw_raw", once=True, name="vaastav_backfill")
def vaastav_backfill():
    """
    One-time backfill of all 9 historical seasons. The `once=True` flag ensures
    this flow is skipped on every pipeline run after the first successful completion.
    """
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .option("header", "true")
        .option("inferSchema", "true")
        .option("cloudFiles.schemaLocation", SCHEMA_LOC)
        .load(f"{LANDING}/historical/")
        .withColumn("_ingestion_timestamp", current_timestamp())
        .withColumn("_source_file", col("_metadata.file_path"))
        # Extract season label (e.g. "2023-24") from the file path
        .withColumn("season", regexp_extract(col("_metadata.file_path"), r"(\d{4}-\d{2})", 1))
    )

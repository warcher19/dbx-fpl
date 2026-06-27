from pyspark import pipelines as dp
from pyspark.sql.functions import current_timestamp, col
import os

LANDING = os.getenv("LANDING_ZONE_PATH", "abfss://fpl-landing@<storage_account>.dfs.core.windows.net")
SCHEMA_LOC = "/Volumes/fpl/bronze/autoloader_meta/fixtures"


@dp.expect_or_drop("has_fixture_id", "id IS NOT NULL")
@dp.table(
    name="fpl.bronze.fixtures",
    comment="Raw FPL fixtures API responses (match schedule and results)",
    cluster_by_auto=True,
    table_properties={"delta.enableChangeDataFeed": "true"},
)
def fixtures():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", SCHEMA_LOC)
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("multiLine", "true")
        .load(f"{LANDING}/fixtures/")
        .withColumn("_ingestion_timestamp", current_timestamp())
        .withColumn("_source_file", col("_metadata.file_path"))
    )

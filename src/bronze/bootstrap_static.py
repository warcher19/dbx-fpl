from pyspark import pipelines as dp
from pyspark.sql.functions import current_timestamp, col
LANDING = "/Volumes/fpl/bronze/landing"
SCHEMA_LOC = "/Volumes/fpl/bronze/autoloader_meta/bootstrap"


@dp.expect_or_drop("has_elements", "elements IS NOT NULL")
@dp.expect_or_drop("has_teams", "teams IS NOT NULL")
@dp.table(
    name="fpl.bronze.bootstrap_static",
    comment="Raw FPL bootstrap-static API snapshots (players, teams, positions)",
    cluster_by_auto=True,
    table_properties={"delta.enableChangeDataFeed": "true"},
)
def bootstrap_static():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", SCHEMA_LOC)
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(f"{LANDING}/bootstrap-static/")
        .withColumn("_ingestion_timestamp", current_timestamp())
        .withColumn("_source_file", col("_metadata.file_path"))
    )

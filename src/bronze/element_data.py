from pyspark import pipelines as dp
from pyspark.sql.functions import current_timestamp, col
LANDING = "/Volumes/fpl/bronze/landing"
SCHEMA_LOC = "/Volumes/fpl/bronze/autoloader_meta/element_data"


@dp.expect("has_history", "history IS NOT NULL OR history_past IS NOT NULL")
@dp.table(
    name="fpl.bronze.element_data",
    comment="Raw FPL element-summary API responses (player match history + past seasons)",
    cluster_by_auto=True,
    table_properties={"delta.enableChangeDataFeed": "true"},
)
def element_data():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", SCHEMA_LOC)
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(f"{LANDING}/element-data/")
        .withColumn("_ingestion_timestamp", current_timestamp())
        .withColumn("_source_file", col("_metadata.file_path"))
    )

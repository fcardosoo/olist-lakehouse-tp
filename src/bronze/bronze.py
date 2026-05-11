import dlt

from pyspark.sql.functions import (
    current_timestamp,
    input_file_name
)

@dlt.table(
    name="bronze_orders",
    comment="Raw orders data from landing zone"
)
def bronze_orders():

    return (
        spark.read.format("csv")
        .option("header", True)
        .option("inferSchema", True)
        .option("mode", "PERMISSIVE")
        .load("/Volumes/ecommerce_lakehouse/landing/olist_raw_files/olist_orders_dataset.csv")
        .withColumn("_ingestion_timestamp", current_timestamp())
        .withColumn("_source_file", input_file_name())
    )
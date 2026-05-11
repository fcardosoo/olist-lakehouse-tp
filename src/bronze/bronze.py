import dlt

from pyspark.sql.functions import (
    current_timestamp,
    col
)

VOLUME_PATH = "/Volumes/ecommerce_lakehouse/landing/olist_raw_files/"


def read_bronze_csv(file_name):
    return (
        spark.read.format("csv")
        .option("header", True)
        .option("inferSchema", True)
        .option("mode", "PERMISSIVE")
        .load(f"{VOLUME_PATH}/{file_name}")
        .withColumn("_ingestion_timestamp", current_timestamp())
        .withColumn("_source_file", col("_metadata.file_path"))
    )


@dlt.table(
    name="bronze_orders",
    comment="Raw orders data"
)
def bronze_orders():
    return read_bronze_csv("olist_orders_dataset.csv")


@dlt.table(
    name="bronze_order_items",
    comment="Raw order items data"
)
def bronze_order_items():
    return read_bronze_csv("olist_order_items_dataset.csv")


@dlt.table(
    name="bronze_products",
    comment="Raw products data"
)
def bronze_products():
    return read_bronze_csv("olist_products_dataset.csv")


@dlt.table(
    name="bronze_customers",
    comment="Raw customers data"
)
def bronze_customers():
    return read_bronze_csv("olist_customers_dataset.csv")


@dlt.table(
    name="bronze_payments",
    comment="Raw payments data"
)
def bronze_payments():
    return read_bronze_csv("olist_order_payments_dataset.csv")


@dlt.table(
    name="bronze_category_translation",
    comment="Product category translations"
)
def bronze_category_translation():
    return read_bronze_csv("product_category_name_translation.csv")
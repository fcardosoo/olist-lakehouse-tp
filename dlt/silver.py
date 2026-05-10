from pyspark import pipelines as dp
from pyspark.sql.functions import count

@dp.table(
    name="quarantine_invalid_customers",
    comment="Customers with fewer than 2 orders"
)
def quarantine_invalid_customers():

    customers = spark.read.table("ecommerce_lakehouse.silver.silver_customers")
    orders = spark.read.table("ecommerce_lakehouse.silver.silver_orders")

    customer_order_counts = (
        customers.alias("c")
        .join(orders.alias("o"), "customer_id", "left")
        .groupBy(
            "customer_id",
            "customer_unique_id",
            "customer_zip_code_prefix",
            "customer_city",
            "customer_state",
            "latitude",
            "longitude"
        )
        .agg(count("order_id").alias("order_count"))
    )

    return customer_order_counts.filter("order_count < 2")
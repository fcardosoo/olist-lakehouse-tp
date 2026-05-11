import dlt

from pyspark.sql.functions import (
    count,
    col
)


@dlt.table(
    name="quarantine_invalid_customers",
    comment="Customers with fewer than 2 orders"
)
def quarantine_invalid_customers():

    customers = dlt.read("silver_customers")

    orders = dlt.read("silver_orders")

    customer_order_counts = (
        customers.alias("c")
        .join(
            orders.alias("o"),
            "customer_id",
            "left"
        )
        .groupBy(
            "customer_id",
            "customer_unique_id",
            "customer_zip_code_prefix",
            "customer_city",
            "customer_state"
        )
        .agg(
            count("order_id").alias("order_count")
        )
    )

    return (
        customer_order_counts
        .filter(col("order_count") < 2)
    )
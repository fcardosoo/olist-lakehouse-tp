import dlt

from pyspark.sql.functions import (
    col,
    count,
    datediff,
    to_timestamp
)


@dlt.table(
    name="silver_customers",
    comment="Cleaned and standardized customers table"
)
def silver_customers():

    customers = spark.read.table("LIVE.bronze_customers")

    return (
        customers
        .select(
            col("customer_id"),
            col("customer_unique_id"),
            col("customer_zip_code_prefix"),
            col("customer_city"),
            col("customer_state")
        )
        .dropDuplicates(["customer_id"])
    )


@dlt.table(
    name="silver_orders",
    comment="Orders enriched with delivery metrics and item counts"
)
def silver_orders():

    orders = spark.read.table("LIVE.bronze_orders")

    order_items = spark.read.table("LIVE.bronze_order_items")

    item_counts = (
        order_items
        .groupBy("order_id")
        .agg(count("order_item_id").alias("item_count"))
    )

    return (
        orders.alias("o")
        .join(item_counts.alias("i"), "order_id", "left")
        .select(
            col("o.order_id"),
            col("o.customer_id"),
            col("o.order_status"),

            to_timestamp("o.order_purchase_timestamp")
                .alias("order_purchase_timestamp"),

            to_timestamp("o.order_approved_at")
                .alias("order_approved_at"),

            to_timestamp("o.order_delivered_customer_date")
                .alias("order_delivered_customer_date"),

            to_timestamp("o.order_estimated_delivery_date")
                .alias("order_estimated_delivery_date"),

            datediff(
                col("o.order_approved_at"),
                col("o.order_purchase_timestamp")
            ).alias("approval_time_days"),

            datediff(
                col("o.order_delivered_customer_date"),
                col("o.order_purchase_timestamp")
            ).alias("delivery_time_days"),

            datediff(
                col("o.order_delivered_customer_date"),
                col("o.order_estimated_delivery_date")
            ).alias("delivery_delay_days"),

            col("i.item_count")
        )
    )


@dlt.table(
    name="silver_payments",
    comment="Cleaned payments table"
)
def silver_payments():

    payments = spark.read.table("LIVE.bronze_payments")

    return (
        payments.select(
            col("order_id"),
            col("payment_sequential"),
            col("payment_type"),
            col("payment_installments"),
            col("payment_value")
        )
    )
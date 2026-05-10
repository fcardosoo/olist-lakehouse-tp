from pyspark import pipelines as dp
from pyspark.sql.functions import (
    col,
    countDistinct,
    sum,
    avg,
    month,
    dayofmonth
)
@dp.table(
    name="gold_customer_payment_analytics",
    comment="Analytical table consolidating payments by customer, payment method, day and month"
)
def gold_customer_payment_analytics():

    customers = spark.read.table("ecommerce_lakehouse.silver.silver_customers")
    orders = spark.read.table("ecommerce_lakehouse.silver.silver_orders")
    payments = spark.read.table("ecommerce_lakehouse.silver.silver_payments")
    return (
        orders.alias("o")
        .join(customers.alias("c"), "customer_id")
        .join(payments.alias("p"), "order_id")
        .groupBy(
            "c.customer_unique_id",
            "p.payment_type",
            dayofmonth("o.order_purchase_timestamp").alias("purchase_day"),
            month("o.order_purchase_timestamp").alias("purchase_month")
        )
        .agg(
            countDistinct("o.order_id").alias("order_count"),
            sum("o.item_count").alias("total_items"),
            sum("p.payment_value").alias("total_paid"),
            avg("o.item_count").alias("avg_items"),
            avg("p.payment_value").alias("avg_payment_value"),
            avg("p.payment_installments").alias("avg_installments")
        )
    )

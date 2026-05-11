import dlt

from pyspark.sql.functions import (
    col,
    countDistinct,
    sum,
    avg,
    month,
    dayofmonth
)


@dlt.table(
    name="gold_customer_payment_analytics",
    comment="Analytical table for customer payments and purchases"
)
def gold_customer_payment_analytics():

    customers = dlt.read("silver_customers")

    orders = dlt.read("silver_orders")

    payments = dlt.read("silver_payments")

    return (

        orders.alias("o")

        .join(
            customers.alias("c"),
            "customer_id"
        )

        .join(
            payments.alias("p"),
            "order_id"
        )

        .groupBy(

            col("c.customer_unique_id"),

            col("p.payment_type"),

            dayofmonth(
                col("o.order_purchase_timestamp")
            ).alias("purchase_day"),

            month(
                col("o.order_purchase_timestamp")
            ).alias("purchase_month")
        )

        .agg(

            countDistinct("o.order_id")
                .alias("order_count"),

            sum("o.item_count")
                .alias("total_items"),

            sum("p.payment_value")
                .alias("total_paid"),

            avg("o.item_count")
                .alias("avg_items"),

            avg("p.payment_value")
                .alias("avg_payment_value"),

            avg("p.payment_installments")
                .alias("avg_installments")
        )
    )

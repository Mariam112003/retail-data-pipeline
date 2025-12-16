from pyspark.sql.functions import count, sum
from retail_orders.config import SILVER_PATH, GOLD_PATH
from retail_orders.utils import write_delta




def run_gold(spark):
    orders = spark.read.format("delta").load(f"{SILVER_PATH}/orders")
    customer_region = spark.read.format("delta").load(f"{SILVER_PATH}/customer_region")
    regions = spark.read.format("delta").load(f"{SILVER_PATH}/regions")

    # Use LEFT JOIN to keep all orders
    orders_with_region = orders \
        .join(customer_region, "customer_id", "left") \
        .join(regions, "region_id", "left") \
        .groupBy("region") \
        .agg(
            count("order_id").alias("total_orders"),
            sum("total_amount").alias("total_sales")
        )

    write_delta(
        orders_with_region,
        f"{GOLD_PATH}/sales_by_region"
    )





"""from pyspark.sql.functions import count, sum
from retail_orders.config import SILVER_PATH, GOLD_PATH
from retail_orders.utils import write_delta


def run_gold(spark):
    orders = spark.read.format("delta").load(f"{SILVER_PATH}/orders")
    customer_region = spark.read.format("delta").load(f"{SILVER_PATH}/customer_region")
    regions = spark.read.format("delta").load(f"{SILVER_PATH}/regions")

    orders_with_region = orders \
        .join(customer_region, "customer_id") \
        .join(regions, "region_id") \
        .groupBy("region") \
        .agg(
            count("order_id").alias("total_orders"),
            sum("total_amount").alias("total_sales")
        )

    write_delta(
        orders_with_region,
        f"{GOLD_PATH}/sales_by_region"
    )"""

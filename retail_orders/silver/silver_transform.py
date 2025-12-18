from pyspark.sql.functions import col, to_date, when
from retail_orders.config import BRONZE_PATH, SILVER_PATH
from retail_orders.utils import write_delta


def run_silver(spark):
    # -------------------------
    # READ BRONZE TABLES
    # -------------------------
    customers = spark.read.format("delta").load(f"{BRONZE_PATH}/customers")
    orders = spark.read.format("delta").load(f"{BRONZE_PATH}/orders")
    products = spark.read.format("delta").load(f"{BRONZE_PATH}/products")
    regions = spark.read.format("delta").load(f"{BRONZE_PATH}/regions")

    # -------------------------
    # CLEAN CUSTOMERS
    # -------------------------
    customers_silver = (
        customers
        .dropDuplicates(["customer_id"])
        .filter(col("customer_id").isNotNull())
    )

    # -------------------------
    # CLEAN ORDERS
    # -------------------------
    orders_silver = (
        orders
        .dropDuplicates(["order_id"])
        .withColumn("order_date", to_date("order_date"))
    )

    # -------------------------
    # CLEAN PRODUCTS
    # -------------------------
    products_silver = products.dropDuplicates(["product_id"])

    # -------------------------
    # CLEAN REGIONS
    # -------------------------
    regions_silver = regions.dropDuplicates(["region_id"])

    # -------------------------
    # CREATE customer_region (FIX)
    # -------------------------
    customer_region = (
    customers_silver
    .withColumn(
        "region_id",
        when(col("state").isin("NY", "NJ", "PA"), "R01")
        .when(col("state").isin("CA", "WA"), "R02")
        .when(col("state").isin("TX", "FL"), "R03")
        .otherwise("R00")
    )
    .select("customer_id", "region_id")
)


    # -------------------------
    # WRITE SILVER TABLES
    # -------------------------
    write_delta(customers_silver, f"{SILVER_PATH}/customers")
    write_delta(orders_silver, f"{SILVER_PATH}/orders")
    write_delta(products_silver, f"{SILVER_PATH}/products")
    write_delta(regions_silver, f"{SILVER_PATH}/regions")
    write_delta(customer_region, f"{SILVER_PATH}/customer_region")

from retail_orders.config import RAW_PATH, BRONZE_PATH
from retail_orders.utils import read_parquet, write_delta


def run_bronze(spark):
    customers_1 = read_parquet(spark, f"{RAW_PATH}/customer_first.parquet")
    customers_2 = read_parquet(spark, f"{RAW_PATH}/customers_second.parquet")

    orders_1 = read_parquet(spark, f"{RAW_PATH}/orders_first.parquet")
    orders_2 = read_parquet(spark, f"{RAW_PATH}/orders_second.parquet")

    products_1 = read_parquet(spark, f"{RAW_PATH}/products_first.parquet")
    products_2 = read_parquet(spark, f"{RAW_PATH}/products_second.parquet")

    regions = read_parquet(spark, f"{RAW_PATH}/regions.parquet")

    customers = customers_1.unionByName(customers_2)
    orders = orders_1.unionByName(orders_2)
    products = products_1.unionByName(products_2)

    write_delta(customers, f"{BRONZE_PATH}/customers")
    write_delta(orders, f"{BRONZE_PATH}/orders")
    write_delta(products, f"{BRONZE_PATH}/products")
    write_delta(regions, f"{BRONZE_PATH}/regions")

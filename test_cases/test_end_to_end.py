try:
    dbutils.fs.ls("/")  # Test if running in Databricks
    IN_DATABRICKS = True
except NameError:
    IN_DATABRICKS = False
    dbutils = None  # Avoid NameError



def test_order_count_reconciliation(spark):
    bronze_count = spark.read.format("delta").load(
        "/Volumes/workspace/default/pipeine/bronze/orders"
    ).count()

    silver_count = spark.read.format("delta").load(
        "/Volumes/workspace/default/pipeine/silver/orders"
    ).count()

    assert silver_count <= bronze_count


def test_sales_reconciliation(spark):
    silver_total = spark.read.format("delta").load(
        "/Volumes/workspace/default/pipeine/silver/orders"
    ).agg({"total_amount": "sum"}).collect()[0][0]

    gold_total = spark.read.format("delta").load(
        "/Volumes/workspace/default/pipeine/gold/orders_by_region"
    ).agg({"total_sales": "sum"}).collect()[0][0]

    assert round(silver_total, 2) == round(gold_total, 2)

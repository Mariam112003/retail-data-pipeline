try:
    dbutils.fs.ls("/")  # Test if running in Databricks
    IN_DATABRICKS = True
except NameError:
    IN_DATABRICKS = False
    dbutils = None  # Avoid NameError



def test_bronze_customers_not_empty(spark):
    df = spark.read.format("delta").load(
        "/Volumes/workspace/default/pipeine/bronze/customers"
    )
    assert df.count() > 0


def test_bronze_orders_not_empty(spark):
    df = spark.read.format("delta").load(
        "/Volumes/workspace/default/pipeine/bronze/orders"
    )
    assert df.count() > 0

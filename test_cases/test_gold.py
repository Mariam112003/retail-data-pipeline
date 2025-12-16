try:
    dbutils.fs.ls("/")  # Test if running in Databricks
    IN_DATABRICKS = True
except NameError:
    IN_DATABRICKS = False
    dbutils = None  # Avoid NameError






def test_gold_orders_by_region_exists(spark):
    df = spark.read.format("delta").load(
        "/Volumes/workspace/default/pipeine/gold/sales_by_region"
    )
    assert df.count() > 0


def test_gold_columns(spark):
    df = spark.read.format("delta").load(
        "/Volumes/workspace/default/pipeine/gold/sales_by_region"
    )
    cols = df.columns
    assert "region" in cols
    assert "total_orders" in cols
    assert "total_sales" in cols

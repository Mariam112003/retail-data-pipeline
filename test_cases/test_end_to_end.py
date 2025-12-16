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
    # Corrected paths
    silver_path = "/Volumes/workspace/default/pipeine/silver/orders"
    gold_path = "/Volumes/workspace/default/pipeine/gold/sales_by_region"

    # Read Silver total
    silver_total = (
        spark.read.format("delta").load(silver_path)
        .agg({"total_amount": "sum"})
        .collect()[0][0]
    )

    # Read Gold total
    gold_total = (
        spark.read.format("delta").load(gold_path)
        .agg({"total_sales": "sum"})
        .collect()[0][0]
    )

    # Print totals for debugging
    print(f"Silver total: {silver_total}")
    print(f"Gold total: {gold_total}")
    print(f"Difference: {silver_total - gold_total}")

    # Safe assertion with tolerance for floating-point differences
    assert abs(silver_total - gold_total) < 0.01, "Sales totals mismatch between Silver and Gold layers"


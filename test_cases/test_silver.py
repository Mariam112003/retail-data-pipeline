def test_customers_no_null_ids(spark):
    df = spark.read.format("delta").load(
        "/Volumes/workspace/default/pipeine/silver/customers"
    )
    assert df.filter("customer_id IS NULL").count() == 0


def test_orders_no_duplicate_ids(spark):
    df = spark.read.format("delta").load(
        "/Volumes/workspace/default/pipeine/silver/orders"
    )
    duplicates = (
        df.groupBy("order_id")
        .count()
        .filter("count > 1")
        .count()
    )
    assert duplicates == 0


def test_customer_region_schema(spark):
    df = spark.read.format("delta").load(
        "/Volumes/workspace/default/pipeine/silver/customer_region"
    )
    fields = [f.name for f in df.schema.fields]
    assert "customer_id" in fields
    assert "region_id" in fields

def test_bronze_paths_exist(spark):
    paths = dbutils.fs.ls("/Volumes/workspace/default/pipeine/bronze")
    assert len(paths) > 0


def test_silver_paths_exist(spark):
    paths = dbutils.fs.ls("/Volumes/workspace/default/pipeine/silver")
    assert len(paths) > 0


def test_gold_paths_exist(spark):
    paths = dbutils.fs.ls("/Volumes/workspace/default/pipeine/gold")
    assert len(paths) > 0

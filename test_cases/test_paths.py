from pathlib import Path

def list_paths(path: str):
    """
    Returns a list of files/folders in the given path.
    Works in both Databricks (dbutils) and local environment.
    """
    try:
        # Try Databricks environment
        return dbutils.fs.ls(path)
    except NameError:
        # dbutils doesn't exist → local fallback
        local_path = Path(path)
        return list(local_path.iterdir()) if local_path.exists() else []

def test_bronze_paths_exist(spark=None):
    paths = list_paths("/Volumes/workspace/default/pipeine/bronze")
    assert len(paths) > 0, "Bronze path is missing or empty"

def test_silver_paths_exist(spark=None):
    paths = list_paths("/Volumes/workspace/default/pipeine/silver")
    assert len(paths) > 0, "Silver path is missing or empty"

def test_gold_paths_exist(spark=None):
    paths = list_paths("/Volumes/workspace/default/pipeine/gold")
    assert len(paths) > 0, "Gold path is missing or empty"

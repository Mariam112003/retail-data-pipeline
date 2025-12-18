
def read_parquet(spark, path):
    return spark.read.parquet(path)


def write_delta(df, path, mode="overwrite"):
    df.write.format("delta").mode(mode).save(path)

from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("minio-smoke-test")
    .getOrCreate()
)

df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "val"])

out = "s3a://lakehouse/bronze/smoke_test"
df.write.mode("overwrite").parquet(out)

print("Wrote:", out)
print("Count:", spark.read.parquet(out).count())

spark.stop()
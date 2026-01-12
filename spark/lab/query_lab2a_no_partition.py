from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("lab2_no_partition")
    .getOrCreate()
)

df = spark.read.parquet("data/silver_np/orders")

df.filter("order_ts >= '2026-01-10' AND order_ts < '2026-01-11'") \
  .groupBy("country") \
  .count() \
  .explain("formatted")

df.show()

spark.stop()
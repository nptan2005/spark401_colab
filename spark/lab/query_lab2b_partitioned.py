from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("query_lab2_partitioned")
    .getOrCreate()
)

df = spark.read.parquet("data/silver_p/orders")

df.filter("dt = '2026-01-10'") \
  .groupBy("country") \
  .count() \
  .explain("formatted")

df.show()

spark.stop()
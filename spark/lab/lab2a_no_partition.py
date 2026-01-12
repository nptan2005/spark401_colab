from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("lab2_no_partition")
    .getOrCreate()
)

df = spark.read.parquet("data/silver/orders_enriched")

(
    df
    .write
    .mode("overwrite")
    .parquet("data/silver_np/orders")
)

print("DONE: no partition")
spark.stop()
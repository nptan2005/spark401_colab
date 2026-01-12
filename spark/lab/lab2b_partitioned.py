from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, col

spark = (
    SparkSession.builder
    .appName("lab2_partitioned")
    .getOrCreate()
)

df = spark.read.parquet("data/silver/orders_enriched")

df = df.withColumn("dt", to_date(col("order_ts")))

(
    df
    .write
    .mode("overwrite")
    .partitionBy("dt")
    .parquet("data/silver_p/orders")
)

print("DONE: partitioned by dt")
spark.stop()
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date

spark = (SparkSession.builder
  .appName("lab2d_partitioned_write")
  .config("spark.sql.shuffle.partitions", "50")   # giảm mặc định 200 cho máy local
  .getOrCreate())

df = (spark.read.parquet("data/silver/orders_enriched")
      .withColumn("dt", to_date(col("order_ts"))))

# (1) repartition theo dt trước khi write:
# - mục tiêu: mỗi dt gom về 1 số partition ổn định -> write ít file hơn, đỡ MemoryManager warn
df = df.repartition("dt")

(df.write
   .mode("overwrite")
   .partitionBy("dt")
   .parquet("data/silver_p_lab2d/orders"))




spark.stop()
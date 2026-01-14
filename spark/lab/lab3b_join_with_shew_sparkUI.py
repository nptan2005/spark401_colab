from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count as _count

ORDERS_PATH = "data/silver/orders"       # 2M rows
CUSTOMERS_PATH = "data/silver/customers" # 50k rows

spark = (
    SparkSession.builder
    .appName("lab3b_join_skew_shuffle")
    .config("spark.sql.shuffle.partitions", "50")
    .config("spark.sql.autoBroadcastJoinThreshold", "-1")  # ép shuffle để thấy skew rõ
    .getOrCreate()
)

o = spark.read.parquet(ORDERS_PATH).alias("o")
c = spark.read.parquet(CUSTOMERS_PATH).select(
    col("customer_id").alias("c_customer_id"),
    col("segment").alias("c_segment")
).alias("c")

j = o.join(c, col("o.customer_id") == col("c.c_customer_id"), "left")

# làm một aggregation để tạo workload
res = (j.groupBy("c_segment")
         .agg(_count("*").alias("txns")))

res.explain("formatted")
res.show()

spark.stop()
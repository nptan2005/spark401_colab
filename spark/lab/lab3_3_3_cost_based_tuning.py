from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

spark = (
    SparkSession.builder
    .appName("lab3_3_3_cost_based_tuning")
    .config("spark.sql.shuffle.partitions", "200")  # để AQE coalesce xuống
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    .config("spark.sql.adaptive.skewJoin.enabled", "true")   # xử skew tự động
    .config("spark.sql.autoBroadcastJoinThreshold", "10485760")  # 10MB
    .getOrCreate()
)


BASE = "data"
SILVER = f"{BASE}/silver_lab32"
BRONZE = f"{BASE}/bronze_lab33"

CUSTOMERS_PATH = f"{SILVER}/customers_dim"
MERCHANTS_PATH = f"{SILVER}/merchants_dim"
ORDERS_PATH = f"{SILVER}/orders_fact_dt"

BRONZE_ORDERS_PATH = f"{BRONZE}/orders_raw"

fact = spark.read.parquet(ORDERS_PATH).where(col("dt") == "2026-01-10").alias("f")
cust = spark.read.parquet(CUSTOMERS_PATH).alias("c")

# Để Spark tự chọn (AQE + skew join)
res = (
    fact.join(cust, "customer_id", "left")
        .groupBy("country")
        .agg(count("*").alias("txns"))
)

res.explain("formatted")
res.show(10, truncate=False)

spark.stop()
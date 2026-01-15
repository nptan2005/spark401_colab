from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, row_number
from pyspark.sql.window import Window

spark = (
    SparkSession.builder
    .appName("lab3_3_1_incremental_overwrite_partition")
    .config("spark.sql.shuffle.partitions", "50")
    # overwrite theo partition (Spark 3+)
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
    .getOrCreate()
)


BASE = "data"
SILVER = f"{BASE}/silver_lab32"
BRONZE = f"{BASE}/bronze_lab33"

CUSTOMERS_PATH = f"{SILVER}/customers_dim"
MERCHANTS_PATH = f"{SILVER}/merchants_dim"
ORDERS_PATH = f"{SILVER}/orders_fact_dt"

BRONZE_ORDERS_PATH = f"{BRONZE}/orders_raw"

# 1) Đọc batch late data (vd chỉ dt=2026-01-10)
late = (
    spark.read.parquet(BRONZE_ORDERS_PATH)
    .where(col("dt") == "2026-01-10")
)

# 2) Đọc silver hiện tại của đúng partition đó (chỉ đọc partition cần)
current = spark.read.parquet(ORDERS_PATH).where(col("dt") == "2026-01-10")

# 3) Merge + dedup theo order_id (giữ bản mới nhất theo ingest_ts hoặc event_ts)
#    Nếu bạn chưa có ingest_ts, tạm giả sử có cột event_ts
merged = current.unionByName(late, allowMissingColumns=True)

w = Window.partitionBy("order_id").orderBy(col("event_ts").desc_nulls_last())
dedup = (
    merged
    .withColumn("rn", row_number().over(w))
    .where(col("rn") == 1)
    .drop("rn")
)

# 4) Overwrite đúng partition dt (dynamic mode)
(
    dedup.write
    .mode("overwrite")
    .partitionBy("dt")
    .parquet(ORDERS_PATH)
)

print("✅ Incremental overwrite partition dt=2026-01-10 done")
spark.stop()
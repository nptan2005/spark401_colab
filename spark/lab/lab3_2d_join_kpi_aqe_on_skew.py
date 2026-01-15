from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, avg as _avg, count as _count

ORDERS_PATH = "data/silver_lab32/orders_fact_dt"
CUSTOMERS_PATH = "data/silver_lab32/customers_dim"

spark = (
    SparkSession.builder
    .appName("lab3_2_join_kpi_aqe_on_skew_v2")
    .config("spark.sql.shuffle.partitions", "50")

    # BẬT AQE (production default thường bật)
    .config("spark.sql.adaptive.enabled", "true")

    # BẬT xử lý skew join (Spark sẽ tự split skew partition)
    .config("spark.sql.adaptive.skewJoin.enabled", "true")

    # Buộc shuffle join để AQE có đất diễn (nếu broadcast, skew join ít ý nghĩa)
    .config("spark.sql.autoBroadcastJoinThreshold", "-1")

    # (tùy chọn) set log AQE rõ hơn
    .config("spark.sql.adaptive.forceApply", "true")
    .getOrCreate()
)

o = spark.read.parquet(ORDERS_PATH).alias("o")
c = spark.read.parquet(CUSTOMERS_PATH).select(
    col("customer_id").alias("c_customer_id"),
    col("segment").alias("c_segment"),
    col("risk_tier").alias("c_risk_tier")
).alias("c")

# filtered = o.where(col("dt") == "2026-01-10")

filtered = o

j = filtered.join(
    c,
    col("o.customer_id") == col("c.c_customer_id"),
    "left"
)

kpi = (j.groupBy("dt", "country", "c_segment", "c_risk_tier")
         .agg(
             _count("*").alias("txns"),
             _sum("amount").alias("total_amount"),
             _avg("amount").alias("avg_amount")
         )
         .orderBy("country", "c_segment", "c_risk_tier")
)

kpi.explain("formatted")
kpi.show(50, truncate=False)

spark.stop()
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum, avg, broadcast

spark = (
    SparkSession.builder
    .appName("lab3_3_2_multi_dim_join")
    .config("spark.sql.shuffle.partitions", "50")
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

cust = spark.read.parquet(CUSTOMERS_PATH).select(
    col("customer_id").alias("c_customer_id"),
    col("segment").alias("segment"),
    col("risk_tier").alias("risk_tier")
).alias("c")

mer = spark.read.parquet(MERCHANTS_PATH).select(
    col("merchant_id").alias("m_merchant_id"),
    col("mcc").alias("mcc"),
    col("merchant_tier").alias("merchant_tier")
).alias("m")

# Thực tế: broadcast dim nếu nhỏ
j = (
    fact
    .join(broadcast(cust), col("f.customer_id") == col("c.c_customer_id"), "left")
    .join(broadcast(mer),  col("f.merchant_id") == col("m.m_merchant_id"), "left")
)

kpi = (
    j.groupBy("dt", "country", "segment", "risk_tier", "mcc", "merchant_tier")
    .agg(
        count("*").alias("txns"),
        sum("amount").alias("total_amount"),
        avg("amount").alias("avg_amount"),
    )
)

kpi.explain("formatted")
kpi.show(5, truncate=False)
spark.stop()
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, avg as _avg, count as _count

ORDERS_P_PATH = "data/silver_p_lab2d/orders"
CUSTOMERS_PATH = "data/silver/customers"
DT = "2026-01-10"

spark = (
    SparkSession.builder
    .appName("lab2g_join_force_shuffle")
    .config("spark.sql.shuffle.partitions", "50")
    .config("spark.sql.autoBroadcastJoinThreshold", "-1")  # tắt broadcast
    .getOrCreate()
)

o = spark.read.parquet(ORDERS_P_PATH).alias("o")
c = spark.read.parquet(CUSTOMERS_PATH).select(
    col("customer_id").alias("c_customer_id"),
    col("segment").alias("c_segment"),
    col("risk_tier").alias("c_risk_tier"),
).alias("c")

o_d = o.where(col("o.dt") == DT)

j = o_d.join(
    c,
    col("o.customer_id") == col("c.c_customer_id"),
    "left"
)

j2 = j.select(
    col("o.dt").alias("dt"),
    col("o.country").alias("country"),
    col("c.c_segment").alias("segment"),
    col("c.c_risk_tier").alias("risk_tier"),
    col("o.amount").alias("amount"),
)

kpi = (
    j2.groupBy("dt", "country", "segment", "risk_tier")
      .agg(
          _count("*").alias("txns"),
          _sum("amount").alias("total_amount"),
          _avg("amount").alias("avg_amount"),
      )
)

kpi.explain("formatted")
spark.stop()
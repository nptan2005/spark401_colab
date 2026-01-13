from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, avg as _avg, count as _count, broadcast

ORDERS_P_PATH = "data/silver_p_lab2d/orders"   # dataset partitioned by dt (có dt=...)
CUSTOMERS_PATH = "data/silver/customers"
DT = "2026-01-10"

spark = (
    SparkSession.builder
    .appName("lab2g_join_with_pruning")
    .config("spark.sql.shuffle.partitions", "50")
    .config("spark.sql.autoBroadcastJoinThreshold", "10MB")
    .getOrCreate()
)

# alias để tránh ambiguous
o = spark.read.parquet(ORDERS_P_PATH).alias("o")
c = spark.read.parquet(CUSTOMERS_PATH).select(
    col("customer_id").alias("c_customer_id"),
    col("segment").alias("c_segment"),
    col("risk_tier").alias("c_risk_tier"),
).alias("c")

# filter dt => partition pruning
o_d = o.where(col("o.dt") == DT)

# broadcast join customers
j = (
    o_d.join(
        broadcast(c),
        col("o.customer_id") == col("c.c_customer_id"),
        "left"
    )
)

# CHỈ chọn các cột cần (và đặt tên rõ)
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

print("\n=== EXPLAIN (formatted) ===")
kpi.explain("formatted")

print("\n=== SAMPLE OUTPUT ===")
kpi.orderBy(col("txns").desc()).show(20, truncate=False)

spark.stop()
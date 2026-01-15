from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, avg, count
from pyspark.sql.functions import broadcast

# sortMergeJoin

# spark = (
#     SparkSession.builder
#     .appName("lab3_2b_join_force_shuffle")
#     .config("spark.sql.shuffle.partitions", "50")
#     # 1) Cấm broadcast
#     .config("spark.sql.autoBroadcastJoinThreshold", "-1")
#     # 2) AQE có thể tự "lách" đổi sang broadcast → tắt luôn join switch
#     .config("spark.sql.adaptive.enabled", "true")
#     .config("spark.sql.adaptive.join.enabled", "false")
#     # 3) Hint merge (ưu tiên SMJ)
#     .getOrCreate()
# )

# Spark auto
spark = (
    SparkSession.builder
    .appName("lab3_2_baseline")
    .config("spark.sql.shuffle.partitions", "50")
    .getOrCreate()
)

orders = spark.read.parquet("data/silver_lab32/orders_fact_dt")
customers = spark.read.parquet("data/silver_lab32/customers_dim")

## 1. Join Baseline
# res = (
#     orders
#     .where(col("dt") == "2026-01-10")
#     .join(customers, "customer_id", "left")
#     .groupBy("dt", "country", "segment", "risk_tier")
#     .agg(
#         count("*").alias("txns"),
#         sum("amount").alias("total_amount"),
#         avg("amount").alias("avg_amount")
#     )
# )

## tối ưu với broadcash

res = (
    orders
    .where(col("dt") == "2026-01-10")
    .join(broadcast(customers), "customer_id", "left")
    .groupBy("dt", "country", "segment", "risk_tier")
    .agg(
        count("*").alias("txns"),
        sum("amount").alias("total_amount"),
        avg("amount").alias("avg_amount")
    )
)

res.explain("formatted")
res.show(5)

spark.stop()
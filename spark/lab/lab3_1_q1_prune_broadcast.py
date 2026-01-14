from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, avg as _avg, count as _count

BASE = "data/silver_lab31"
FACT = f"{BASE}/orders_fact_dt"
DIM  = f"{BASE}/customers"

def spark(app: str) -> SparkSession:
    return (
        SparkSession.builder
        .appName(app)
        .config("spark.sql.shuffle.partitions", "50")
        .config("spark.sql.adaptive.enabled", "true")
        # threshold 10MB default thường đủ để broadcast customers 50k
        .getOrCreate()
    )

def main() -> None:
    s = spark("lab3_1_q1_prune_broadcast")

    orders = s.read.parquet(FACT).alias("o")
    customers = (
        s.read.parquet(DIM)
        .select(
            col("customer_id").alias("c_customer_id"),
            col("segment").alias("c_segment"),
            col("risk_tier").alias("c_risk_tier"),
        )
        .alias("c")
    )

    target_dt = "2026-01-10"

    q = (
        orders
        .where(col("dt") == target_dt)  # ✅ pruning (nếu dt là partition column)
        .join(customers, col("o.customer_id") == col("c.c_customer_id"), "left")
        .groupBy("dt", "country", "c_segment", "c_risk_tier")
        .agg(
            _count("*").alias("txns"),
            _sum("amount").alias("total_amount"),
            _avg("amount").alias("avg_amount"),
        )
        .orderBy("country", "c_segment", "c_risk_tier")
    )

    q.explain("formatted")
    q.show(20, truncate=False)

    s.stop()

if __name__ == "__main__":
    main()
from pathlib import Path
from pyspark.sql import SparkSession, functions as F

ROOT = Path.cwd().resolve()
EVENTS_DIR = (Path.cwd() /  "logs" / "spark-events").resolve()
WH_DIR = (ROOT / "warehouse").resolve()

SILVER_DIR = (ROOT / "data" / "silver" / "orders_enriched").resolve()
GOLD_DIR = (ROOT / "data" / "gold" / "kpi_daily").resolve()

spark = (
    SparkSession.builder
    .appName("lab2-silver-to-gold")
    .config("spark.eventLog.enabled", "true")
    .config("spark.eventLog.dir", f"file://{EVENTS_DIR}")
    .config("spark.sql.warehouse.dir", f"file://{WH_DIR}")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")
print("Spark UI:", spark.sparkContext.uiWebUrl)

# 1) Read Silver
df = spark.read.parquet(f"file://{SILVER_DIR}")
print("rows =", df.count())

# 2) Quick skew check: top customers
top = (
    df.groupBy("customer_id")
      .count()
      .orderBy(F.desc("count"))
      .limit(10)
)
top.show(truncate=False)

# 3) Gold KPI: daily + segment + risk_tier
# (nhẹ nhưng đúng “gold”: aggregation)
gold = (
    df.withColumn("dt", F.to_date("order_ts"))
      .groupBy("dt", "segment", "risk_tier", "country", "channel")
      .agg(
          F.count("*").alias("txns"),
          F.sum("amount").alias("total_amount"),
          F.avg("amount").alias("avg_amount"),
      )
)

print("\n=== EXPLAIN (formatted) ===")
gold.explain("formatted")

# 4) Write Gold
GOLD_DIR.parent.mkdir(parents=True, exist_ok=True)
gold.write.mode("overwrite").parquet(f"file://{GOLD_DIR}")
print("Wrote gold:", GOLD_DIR)

spark.stop()
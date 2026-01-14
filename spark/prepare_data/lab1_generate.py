from pathlib import Path
from pyspark.sql import SparkSession,functions as F


EVENTS_DIR = (Path.cwd() / "logs" / "spark-events").resolve()
EVENTS_URI = f"file://{EVENTS_DIR}"

WH_DIR = (Path.cwd() / "warehouse").resolve()
WH_URI = f"file://{WH_DIR}"


DATA_DIR = (Path.cwd() / "data").resolve()
SILVER_DIR = (DATA_DIR / "silver" / "orders_enriched").resolve()
SILVER_URI = f"file://{SILVER_DIR}"


spark = (
    SparkSession.builder
    .appName("lab1-generate")
    # event log để History Server đọc
    .config("spark.eventLog.enabled", "true")
    .config("spark.eventLog.dir", EVENTS_URI)
    .config("spark.sql.warehouse.dir", WH_URI)
    .getOrCreate()
)

print("Event log dir:", EVENTS_URI)
print("Spark UI:", spark.sparkContext.uiWebUrl)

spark.sparkContext.setLogLevel("WARN")

# -----------------------
# 1) customers (50k)
# -----------------------
n_customers = 50_000
segments = ["MASS", "AFFLUENT", "SME"]
risk = ["LOW", "MED", "HIGH"]

seg_arr = F.array(*[F.lit(x) for x in segments])
risk_arr = F.array(*[F.lit(x) for x in risk])

# element_at cần index kiểu INT (Spark báo lỗi BIGINT như bạn gặp)
seg_idx = (F.pmod(F.col("id"), F.lit(len(segments))) + F.lit(1)).cast("int")
risk_idx = (F.pmod(F.col("id"), F.lit(len(risk))) + F.lit(1)).cast("int")

customers = (
    spark.range(0, n_customers)
    .select(
        (F.col("id") + 1).cast("string").alias("customer_id"),
        F.element_at(seg_arr, seg_idx).alias("segment"),
        F.element_at(risk_arr, risk_idx).alias("risk_tier"),
        (F.current_date() - F.expr("INTERVAL 1 DAYS") - F.pmod(F.col("id"), F.lit(365)).cast("int")).alias("created_date"),
    )
)

# -----------------------
# 2) orders (2M) + skew hot key
# -----------------------
n_orders = 2_000_000
channels = ["POS", "ECOM", "ATM"]
countries = ["VN", "SG", "TH", "ID", "MY"]
statuses = ["SUCCESS", "FAILED", "REVERSED"]

HOT_CUSTOMER = "1"
HOT_RATIO = 0.25

ch_arr = F.array(*[F.lit(x) for x in channels])
cty_arr = F.array(*[F.lit(x) for x in countries])
st_arr = F.array(*[F.lit(x) for x in statuses])

ch_idx = (F.pmod(F.col("id"), F.lit(len(channels))) + F.lit(1)).cast("int")
cty_idx = (F.pmod(F.col("id"), F.lit(len(countries))) + F.lit(1)).cast("int")
st_idx = (F.pmod(F.col("id"), F.lit(len(statuses))) + F.lit(1)).cast("int")

orders = (
    spark.range(0, n_orders)
    .select(
        (F.col("id") + 1).cast("string").alias("order_id"),
        F.when(F.rand(seed=7) < F.lit(HOT_RATIO), F.lit(HOT_CUSTOMER))
         .otherwise((F.pmod(F.col("id") * 17, F.lit(n_customers - 1)) + 2).cast("string"))
         .alias("customer_id"),
        (F.rand(seed=11) * 5000).cast("double").alias("amount"),
        (F.current_timestamp() - (F.pmod(F.col("id"), F.lit(30)).cast("int") * F.expr("INTERVAL 1 DAYS"))).alias("order_ts"),
        F.element_at(ch_arr, ch_idx).alias("channel"),
        F.element_at(cty_arr, cty_idx).alias("country"),
        F.element_at(st_arr, st_idx).alias("status"),
    )
)

customers.cache()
orders.cache()

print("customers =", customers.count())
print("orders    =", orders.count())

customers.show(5, truncate=False)
orders.show(5, truncate=False)

# -----------------------
# 3) mini pipeline
# -----------------------
# NOTE: cố tình tạo 1 join có skew để lab sau xử lý
silver = (
    orders.join(customers, "customer_id", "left")
    .withColumn("amount_bucket", F.when(F.col("amount") < 1000, "LOW")
                              .when(F.col("amount") < 3000, "MID")
                              .otherwise("HIGH"))
)

print("\n=== EXPLAIN (formatted) ===")
silver.explain("formatted")

# write parquet
silver.write.mode("overwrite").parquet(SILVER_URI)
print("Wrote:", SILVER_URI)

spark.stop()
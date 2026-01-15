# spark/lab/lab32_prepare_data.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, expr, rand, floor, pmod, element_at,
    to_timestamp, to_date, current_timestamp, lit
)

# =========================
# CONFIG (bạn có thể giảm/tăng)
# =========================
BASE = "data"
SILVER = f"{BASE}/silver_lab32"
BRONZE = f"{BASE}/bronze_lab33"

CUSTOMERS_PATH = f"{SILVER}/customers_dim"
MERCHANTS_PATH = f"{SILVER}/merchants_dim"
ORDERS_PATH = f"{SILVER}/orders_fact_dt"

BRONZE_ORDERS_PATH = f"{BRONZE}/orders_raw"

N_CUSTOMERS = 200_000
N_MERCHANTS = 50_000

# Local M2 Max 32GB: bạn có thể thử 3M -> 10M
N_ORDERS = 3_000_000
N_DAYS = 30

# Skew: % order dồn về customer_id='1'
SKEW_RATIO = 0.30

spark = (
    SparkSession.builder
    .appName("lab32_prepare_data")
    .config("spark.sql.shuffle.partitions", "50")
    .getOrCreate()
)

# =========================
# 1) CUSTOMERS DIM
# =========================
customers = (
    spark.range(0, N_CUSTOMERS)
    .select(
        (col("id") + 1).cast("string").alias("customer_id"),
        element_at(
            expr("array('MASS','AFFLUENT','SME','CORP')"),
            (pmod(col("id"), 4) + 1).cast("int")
        ).alias("segment"),
        element_at(
            expr("array('LOW','MED','HIGH')"),
            (pmod(col("id"), 3) + 1).cast("int")
        ).alias("risk_tier"),
        expr("date_sub(date('2026-01-11'), cast(pmod(id, 365) as int))").alias("created_date")
    )
)

(customers.write.mode("overwrite").parquet(CUSTOMERS_PATH))
print("✅ customers_dim:", customers.count(), "->", CUSTOMERS_PATH)

# =========================
# 2) MERCHANTS DIM
# =========================
merchants = (
    spark.range(0, N_MERCHANTS)
    .select(
        (col("id") + 1).cast("string").alias("merchant_id"),
        element_at(
            expr("array('5411','5812','5999','4111','4829')"),  # grocery/restaurant/retail/transport/telecom
            (pmod(col("id"), 5) + 1).cast("int")
        ).alias("mcc"),
        element_at(
            expr("array('TIER_1','TIER_2','TIER_3')"),
            (pmod(col("id"), 3) + 1).cast("int")
        ).alias("merchant_tier"),
        element_at(
            expr("array('VN','SG','TH','ID','MY')"),
            (pmod(col("id"), 5) + 1).cast("int")
        ).alias("merchant_country")
    )
)

(merchants.write.mode("overwrite").parquet(MERCHANTS_PATH))
print("✅ merchants_dim:", merchants.count(), "->", MERCHANTS_PATH)

# =========================
# 3) ORDERS FACT (partition by dt)
# =========================
orders = (
    spark.range(0, N_ORDERS)
    .select(
        (col("id") + 1).cast("string").alias("order_id"),

        # skew: 30% về customer_id = '1'
        expr(f"""
            CASE WHEN rand(7) < {SKEW_RATIO}
                 THEN '1'
                 ELSE cast(pmod(id * 17, {N_CUSTOMERS-2}) + 2 as string)
            END
        """).alias("customer_id"),

        # merchant_id random
        expr(f"cast(pmod(id * 13, {N_MERCHANTS}) + 1 as string)").alias("merchant_id"),

        (rand(11) * 5000).alias("amount"),

        # event_ts: rải trong 30 ngày
        expr(f"timestamp('2026-01-12 10:00:00') - interval 1 day * cast(pmod(id, {N_DAYS}) as int)")
            .alias("event_ts"),

        element_at(expr("array('POS','ECOM','ATM')"), (pmod(col('id'), 3) + 1).cast("int")).alias("channel"),
        element_at(expr("array('VN','SG','TH','ID','MY')"), (pmod(col('id'), 5) + 1).cast("int")).alias("country"),
        element_at(expr("array('SUCCESS','FAILED','REVERSED')"), (pmod(col('id'), 3) + 1).cast("int")).alias("status"),
    )
    .withColumn("dt", to_date(col("event_ts")))
)

(
    orders.write
    .mode("overwrite")
    .partitionBy("dt")
    .parquet(ORDERS_PATH)
)
print("✅ orders_fact_dt:", orders.count(), "->", ORDERS_PATH)

# =========================
# 4) BRONZE orders_raw (append style cho streaming)
#    tạo 2 đợt:
#    - batch1: đủ 30 ngày
#    - batch2: late data cho 2026-01-10 (duplicate order_id + record mới)
# =========================
batch1 = (
    orders.select(
        "order_id","customer_id","merchant_id","amount","event_ts","channel","country","status","dt"
    )
    .withColumn("ingest_ts", current_timestamp())
)

# ghi batch1 partition dt (append)
(batch1.write.mode("overwrite").partitionBy("dt").parquet(BRONZE_ORDERS_PATH))
print("✅ bronze batch1 written ->", BRONZE_ORDERS_PATH)

# late batch: lấy dt=2026-01-10, tạo bản ghi trùng order_id (simulate update) + bản ghi mới
late_base = batch1.where(col("dt") == lit("2026-01-10")).limit(50_000)

late_updates = (
    late_base
    .withColumn("amount", col("amount") * lit(1.05))   # giả sử update amount
    .withColumn("status", lit("SUCCESS"))
    .withColumn("ingest_ts", current_timestamp())
)

late_new = (
    spark.range(0, 10_000)
    .select(
        expr("concat('LATE_', cast(id as string))").alias("order_id"),
        lit("1").alias("customer_id"),
        lit("1").alias("merchant_id"),
        (rand(99)*5000).alias("amount"),
        to_timestamp(lit("2026-01-10 12:00:00")).alias("event_ts"),
        lit("ECOM").alias("channel"),
        lit("VN").alias("country"),
        lit("SUCCESS").alias("status"),
        to_date(lit("2026-01-10")).alias("dt"),
        current_timestamp().alias("ingest_ts")
    )
)

late_batch2 = late_updates.unionByName(late_new)

# append late vào bronze
(late_batch2.write.mode("append").partitionBy("dt").parquet(BRONZE_ORDERS_PATH))
print("✅ bronze batch2 (late) appended for dt=2026-01-10")

spark.stop()
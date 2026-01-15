from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, expr, rand, floor, pmod, element_at, to_timestamp, to_date
)

# =========================
# CONFIG / PATHS
# =========================
BASE = "data/silver_lab32"
ORDERS_PATH = f"{BASE}/orders_fact_dt"
CUSTOMERS_PATH = f"{BASE}/customers_dim"

# Data scale (local máy bạn có thể tăng/giảm)
N_CUSTOMERS = 2_000_000        # dim lớn hơn (thực tế có thể vài triệu)
N_ORDERS = 10_000_000        # fact lớn (local có thể 2-10M tuỳ RAM)
N_DAYS = 30                  # 30 partitions theo dt

# Skew ratio: % order đổ về customer_id = '1'
SKEW_RATIO = 0.30            # 30% cực lệch (bạn có thể thử 0.1 / 0.2 / 0.4)

spark = (
    SparkSession.builder
    .appName("lab3_2_prepare_big_data")
    # giảm shuffle partitions để phù hợp máy local
    .config("spark.sql.shuffle.partitions", "50")
    .getOrCreate()
)

# =========================
# 1) CUSTOMERS DIM
# =========================
# spark.range tạo DataFrame gồm 1 cột id kiểu BIGINT: [0..N_CUSTOMERS-1]
customers = (
    spark.range(0, N_CUSTOMERS)
    .select(
        # customer_id dạng string cho giống data thực tế (hay gặp trong DW)
        (col("id") + 1).cast("string").alias("customer_id"),

        # segment/risk_tier là dim attribute
        element_at(
            expr("array('MASS','AFFLUENT','SME','CORP')"),
            (pmod(col("id"), 4) + 1).cast("int")
        ).alias("segment"),

        element_at(
            expr("array('LOW','MED','HIGH')"),
            (pmod(col("id"), 3) + 1).cast("int")
        ).alias("risk_tier"),

        # thêm vài cột thực tế hơn
        element_at(
            expr("array('VN','SG','TH','ID','MY','PH')"),
            (pmod(col("id"), 6) + 1).cast("int")
        ).alias("home_country"),

        # có thể làm “status” khách hàng
        expr("CASE WHEN pmod(id, 10)=0 THEN 'INACTIVE' ELSE 'ACTIVE' END").alias("cust_status")
    )
)

(
    customers.write
    .mode("overwrite")
    .parquet(CUSTOMERS_PATH)
)

print("✔ customers_dim written")

# =========================
# 2) ORDERS FACT (SKEW + PARTITION dt)
# =========================
# Ý tưởng:
# - 30% records customer_id='1' (skew key)
# - phần còn lại trải đều lên 2..N_CUSTOMERS
# - order_ts trải trong 30 ngày => dt = to_date(order_ts) => partition dt
orders = (
    spark.range(0, N_ORDERS)
    .select(
        (col("id") + 1).cast("string").alias("order_id"),

        # tạo skew:
        # rand(7) < SKEW_RATIO => customer_id='1'
        # else => customer_id random-ish (2..N_CUSTOMERS)
        expr(f"""
            CASE
              WHEN rand(7) < {SKEW_RATIO} THEN '1'
              ELSE cast(pmod(id * 17, {N_CUSTOMERS - 2}) + 2 as string)
            END
        """).alias("customer_id"),

        # số tiền: float 0..5000
        (rand(11) * 5000).alias("amount"),

        # country phát sinh giao dịch
        element_at(
            expr("array('VN','SG','TH','ID','MY','PH')"),
            (pmod(col("id"), 6) + 1).cast("int")
        ).alias("country"),

        # channel
        element_at(
            expr("array('POS','ECOM','ATM','QR')"),
            (pmod(col("id"), 4) + 1).cast("int")
        ).alias("channel"),

        # status
        element_at(
            expr("array('SUCCESS','FAILED','REVERSED')"),
            (pmod(col("id"), 3) + 1).cast("int")
        ).alias("status"),

        # timestamp: base - (id % N_DAYS) days
        # NOTE: tránh "interval" vì bạn từng gặp lỗi UNRESOLVED_ROUTINE interval
        # dùng expr timestamp + make_interval(days=...)
        expr(f"timestamp('2026-01-12 10:00:00') - make_interval(0,0,0, cast(pmod(id,{N_DAYS}) as int), 0,0,0)")
        .alias("order_ts")
    )
    .withColumn("dt", to_date(col("order_ts")))
)

# PartitionBy dt để tạo partition pruning cho lab
(
    orders.write
    .mode("overwrite")
    .partitionBy("dt")
    .parquet(ORDERS_PATH)
)

print("✔ orders_fact_dt written")

spark.stop()
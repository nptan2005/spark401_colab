from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, rand, expr, element_at, pmod, date_sub, to_date
)

# =========================
# CONFIG (Python best-practice)
# =========================
BASE = "data/silver_lab31"
CUSTOMERS_PATH = f"{BASE}/customers"
ORDERS_RAW_PATH = f"{BASE}/orders_raw"
ORDERS_FACT_PATH = f"{BASE}/orders_fact_dt"   # fact partitioned by dt

N_CUSTOMERS = 50_000
N_ORDERS = 2_000_000

def build_spark(app: str) -> SparkSession:
    """
    Tạo SparkSession theo phong cách production:
    - shuffle partitions giảm cho máy local
    - bật AQE để Spark tự tối ưu runtime
    """
    return (
        SparkSession.builder
        .appName(app)
        .config("spark.sql.shuffle.partitions", "50")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )

def main() -> None:
    spark = build_spark("lab3_1_prepare_fact_partitioned")

    # =========================
    # 1) CUSTOMERS DIM (50k)
    # =========================
    customers = (
        spark.range(0, N_CUSTOMERS)
        .select(
            (col("id") + 1).cast("string").alias("customer_id"),

            # element_at(array(...), idx) yêu cầu idx INT (Spark hay ra BIGINT)
            element_at(
                expr("array('MASS','AFFLUENT','SME')"),
                (pmod(col("id"), 3) + 1).cast("int")
            ).alias("segment"),

            element_at(
                expr("array('LOW','MED','HIGH')"),
                (pmod(col("id"), 3) + 1).cast("int")
            ).alias("risk_tier"),

            date_sub(expr("date('2026-01-11')"), pmod(col("id"), 365).cast("int")).alias("created_date"),
        )
    )

    (customers.write.mode("overwrite").parquet(CUSTOMERS_PATH))
    print("✔ customers:", customers.count())

    # =========================
    # 2) ORDERS RAW (2M)
    #    Cố tình tạo SKEW: customer_id='1' xuất hiện ~25%
    # =========================
    orders = (
        spark.range(0, N_ORDERS)
        .select(
            (col("id") + 1).cast("string").alias("order_id"),

            # 25% record về customer_id=1 => skew
            expr("""
                CASE
                  WHEN rand(7) < 0.25 THEN '1'
                  ELSE cast(pmod(id * 17, 49999) + 2 as string)
                END
            """).alias("customer_id"),

            (rand(11) * 5000).alias("amount"),

            # KHÔNG dùng "interval" (bạn gặp lỗi UNRESOLVED_ROUTINE)
            # Dùng expr timestamp - make_interval/day subtraction dạng an toàn:
            expr("timestamp('2026-01-12 10:23:17')").alias("base_ts"),
            (pmod(col("id"), 30).cast("int")).alias("day_back"),

            element_at(expr("array('POS','ECOM','ATM')"),
                       (pmod(col('id'), 3) + 1).cast("int")).alias("channel"),

            element_at(expr("array('VN','SG','TH','ID','MY')"),
                       (pmod(col('id'), 5) + 1).cast("int")).alias("country"),

            element_at(expr("array('SUCCESS','FAILED','REVERSED')"),
                       (pmod(col('id'), 3) + 1).cast("int")).alias("status")
        )
        # tạo order_ts từ base_ts - day_back
        .withColumn("order_ts", expr("base_ts - make_interval(0,0,0,day_back,0,0,0)"))
        .drop("base_ts", "day_back")
        # cột partition dt
        .withColumn("dt", to_date(col("order_ts")))
    )

    (orders.write.mode("overwrite").parquet(ORDERS_RAW_PATH))
    print("✔ orders_raw:", orders.count())

    # =========================
    # 3) ORDERS FACT PARTITIONED BY dt
    #    Đây là điểm "gần thực tế": fact thường partition theo ngày
    # =========================
    (
        orders
        .write
        .mode("overwrite")
        .partitionBy("dt")
        .parquet(ORDERS_FACT_PATH)
    )
    print("✔ orders_fact_dt written:", ORDERS_FACT_PATH)

    spark.stop()

if __name__ == "__main__":
    main()
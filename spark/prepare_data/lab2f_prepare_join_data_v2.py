from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, rand, expr, element_at, pmod, date_sub, to_date, broadcast, when
)

# =========================
# Config
# =========================
BASE_TS = "2026-01-12 10:23:17"
CUSTOMERS_N = 50_000
ORDERS_N = 2_000_000

OUT_CUSTOMERS = "data/silver/customers"
OUT_ORDERS = "data/silver/orders"
OUT_ORDERS_ENRICHED = "data/silver/orders_enriched"
OUT_ORDERS_PARTITIONED = "data/silver_p_lab2f/orders_enriched_by_dt"  # partitionBy dt

def build_spark():
    return (
        SparkSession.builder
        .appName("lab2f_prepare_join_data_v2")
        .config("spark.sql.shuffle.partitions", "50")  # local tune
        .config("spark.sql.adaptive.enabled", "true")  # AQE on
        .getOrCreate()
    )

def gen_customers(spark):
    customers = (
        spark.range(0, CUSTOMERS_N)
        .select(
            (col("id") + 1).cast("string").alias("customer_id"),
            element_at(expr("array('MASS','AFFLUENT','SME')"),
                       (pmod(col("id"), 3) + 1).cast("int")).alias("segment"),
            element_at(expr("array('LOW','MED','HIGH')"),
                       (pmod(col("id"), 3) + 1).cast("int")).alias("risk_tier"),
            date_sub(expr("date('2026-01-11')"),
                     pmod(col("id"), 365).cast("int")).alias("created_date"),
        )
    )
    return customers

def gen_orders(spark):
    orders = (
        spark.range(0, ORDERS_N)
        .select(
            (col("id") + 1).cast("string").alias("order_id"),
            expr("""
                CASE
                  WHEN rand(7) < 0.25 THEN '1'
                  ELSE cast(pmod(id * 17, 49999) + 2 as string)
                END
            """).alias("customer_id"),
            (rand(11) * 5000).alias("amount"),

            # ✅ FIX interval dynamic: Spark style (giống plan bạn đã thấy)
            expr(f"timestamp('{BASE_TS}') + -(INTERVAL 1 DAY * cast(pmod(id, 30) as int))").alias("order_ts"),

            element_at(expr("array('POS','ECOM','ATM')"),
                       (pmod(col("id"), 3) + 1).cast("int")).alias("channel"),
            element_at(expr("array('VN','SG','TH','ID','MY')"),
                       (pmod(col("id"), 5) + 1).cast("int")).alias("country"),
            element_at(expr("array('SUCCESS','FAILED','REVERSED')"),
                       (pmod(col("id"), 3) + 1).cast("int")).alias("status"),
        )
    )
    return orders

def enrich_orders(orders, customers, use_broadcast=False):
    c = broadcast(customers) if use_broadcast else customers

    enriched = (
        orders.join(c, on="customer_id", how="left")
        .withColumn(
            "amount_bucket",
            when(col("amount") < 1000, "LOW")
            .when(col("amount") < 3000, "MID")
            .otherwise("HIGH")
        )
        .withColumn("dt", to_date(col("order_ts")))
    )
    return enriched

def main():
    spark = build_spark()

    # ---- Generate
    customers = gen_customers(spark)
    orders = gen_orders(spark)

    # ---- Persist raw data (cho join lab / compare)
    customers.write.mode("overwrite").parquet(OUT_CUSTOMERS)
    orders.write.mode("overwrite").parquet(OUT_ORDERS)

    # ---- Enrich (join + dt) – default (không ép broadcast)
    orders_enriched = enrich_orders(orders, customers, use_broadcast=False)
    orders_enriched.write.mode("overwrite").parquet(OUT_ORDERS_ENRICHED)

    # ---- Partitioned dataset by dt (cho pruning labs)
    # Gợi ý: nếu muốn “ít file mỗi dt”, hãy repartition theo dt trước khi write:
    orders_enriched_repart_dt = orders_enriched.repartition(30, "dt")  # tune: 30~100 tùy máy
    (orders_enriched_repart_dt
        .write
        .mode("overwrite")
        .partitionBy("dt")
        .parquet(OUT_ORDERS_PARTITIONED)
    )

    # ---- Quick sanity
    print("✔ customers:", spark.read.parquet(OUT_CUSTOMERS).count())
    print("✔ orders:", spark.read.parquet(OUT_ORDERS).count())
    print("✔ orders_enriched:", spark.read.parquet(OUT_ORDERS_ENRICHED).count())
    print("✔ orders_enriched_by_dt:", OUT_ORDERS_PARTITIONED)

    spark.stop()

if __name__ == "__main__":
    main()
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, rand, expr, element_at, pmod, date_sub

spark = (SparkSession.builder
    .appName("lab0_prepare_join_data")
    .config("spark.sql.shuffle.partitions", "50")
    .getOrCreate())

# 1) CUSTOMERS
customers = (
    spark.range(0, 50_000)
    .select(
        (col("id") + 1).cast("string").alias("customer_id"),
        element_at(expr("array('MASS','AFFLUENT','SME')"), (pmod(col("id"), 3) + 1).cast("int")).alias("segment"),
        element_at(expr("array('LOW','MED','HIGH')"), (pmod(col("id"), 3) + 1).cast("int")).alias("risk_tier"),
        date_sub(expr("date('2026-01-11')"), pmod(col("id"), 365).cast("int")).alias("created_date"),
    )
)

customers.write.mode("overwrite").parquet("data/silver/customers")
print("✔ customers written:", customers.count())

# 2) ORDERS
orders = (
    spark.range(0, 2_000_000)
    .select(
        (col("id") + 1).cast("string").alias("order_id"),
        expr("""
            CASE 
              WHEN rand(7) < 0.25 THEN '1'
              ELSE cast(pmod(id * 17, 49999) + 2 as string)
            END
        """).alias("customer_id"),
        (rand(11) * 5000).alias("amount"),

        # ✅ FIX interval: dùng INTERVAL 1 DAY * n
        expr("timestamp('2026-01-12 10:23:17') + -(INTERVAL 1 DAY * cast(pmod(id, 30) as int))").alias("order_ts"),

        # ✅ cast int cho element_at index
        element_at(expr("array('POS','ECOM','ATM')"), (pmod(col('id'), 3) + 1).cast("int")).alias("channel"),
        element_at(expr("array('VN','SG','TH','ID','MY')"), (pmod(col('id'), 5) + 1).cast("int")).alias("country"),
        element_at(expr("array('SUCCESS','FAILED','REVERSED')"), (pmod(col('id'), 3) + 1).cast("int")).alias("status"),
    )
)

orders.write.mode("overwrite").parquet("data/silver/orders_enriched")
print("✔ orders written:", orders.count())

spark.stop()
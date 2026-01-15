from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, rand, current_timestamp, to_date

BRONZE = "data/bronze_lab33/orders_raw"

spark = (
    SparkSession.builder
    .appName("append_more_bronze")
    .config("spark.sql.shuffle.partitions", "50")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# Append 5k new rows into Bronze (partitioned by dt)
df = (
    spark.range(0, 5000)
    .selectExpr(
        "concat('NEW_', cast(id as string)) as order_id",
        "'1' as customer_id",
        "'1' as merchant_id",
        "(rand(1)*5000) as amount",
        "timestamp('2026-01-10 13:00:00') as event_ts",
        "'ECOM' as channel",
        "'VN' as country",
        "'SUCCESS' as status",
    )
    .withColumn("dt", to_date(lit("2026-01-10")))
    .withColumn("ingest_ts", current_timestamp())
)

(df.write
   .mode("append")
   .partitionBy("dt")
   .parquet(BRONZE)
)

print("✅ appended 5k rows to bronze:", BRONZE)

spark.stop()
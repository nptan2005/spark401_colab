from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp

BRONZE_PATH = "s3a://lakehouse/bronze/orders"
SILVER_PATH = "s3a://lakehouse/silver/orders"
CHECKPOINT  = "s3a://lakehouse/_checkpoints/orders_bronze_to_silver"

spark = (
    SparkSession.builder
    .appName("orders-bronze-to-silver")
    .getOrCreate()
)

# ====== IMPORTANT: streaming file source phải có schema ======
# infer schema từ dữ liệu parquet hiện có ở bronze
static_df = spark.read.parquet(BRONZE_PATH)
schema = static_df.schema

stream_df = (
    spark.readStream
    .schema(schema)
    .format("parquet")
    .load(BRONZE_PATH)
)

# ví dụ chuẩn hoá nhẹ (tuỳ lab bạn)
out_df = (
    stream_df
    .withColumn("event_ts", to_timestamp(col("event_ts")))
)

q = (
    out_df.writeStream
    .format("parquet")
    .option("path", SILVER_PATH)
    .option("checkpointLocation", CHECKPOINT)
    .outputMode("append")
    .start()
)

q.awaitTermination()
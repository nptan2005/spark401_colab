# spark/lab/labA1_kafka_to_silver_orders.py
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    to_timestamp,
    current_timestamp,
    to_date,
    expr,
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

TOPIC = "orders"
KAFKA_BOOTSTRAP = "localhost:9094"  # compose expose EXTERNAL = 9094
CHECKPOINT = "checkpoints/orders_kafka_to_silver"
SILVER_OUT = "data/silver_kafka/orders_fact_dt"

# ---- 1) JSON schema (streaming phải khai báo schema rõ ràng)
schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("merchant_id", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("event_ts", StringType(), True),  # parse sang timestamp sau
    StructField("channel", StringType(), True),
    StructField("country", StringType(), True),
    StructField("status", StringType(), True),
    StructField("op", StringType(), True),        # I/U/D (CDC simplified)
])

spark = (
    SparkSession.builder
    .appName("labA1_kafka_to_silver_orders")
    # streaming: AQE sẽ bị tắt tự động (warning là bình thường)
    .config("spark.sql.shuffle.partitions", "24")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# ---- 2) Read Kafka stream
raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", TOPIC)
    .option("startingOffsets", "earliest")   # lab: đọc từ đầu
    .option("failOnDataLoss", "false")       # local: tránh fail khi log bị dọn
    .load()
)

# Kafka message: value là binary -> cast string
json_df = raw.selectExpr("CAST(value AS STRING) AS json_str")

# ---- 3) Parse JSON -> columns
parsed = (
    json_df
    .select(from_json(col("json_str"), schema).alias("j"))
    .select("j.*")
    .withColumn("event_ts", to_timestamp("event_ts"))
    .withColumn("ingest_ts", current_timestamp())
    .withColumn("dt", to_date(col("event_ts")))
)

# ---- 4) Late data + dedup (idempotency)
# watermark: chấp nhận data trễ tối đa 3 ngày theo event time
# dropDuplicates theo order_id để loại duplicate trong watermark window
clean = (
    parsed
    .withWatermark("event_ts", "3 days")
    .dropDuplicates(["order_id"])
)

# ---- 5) CDC simplified: bỏ op = 'D'
silver_ready = clean.filter((col("op").isNull()) | (col("op") != expr("'D'")))

# ---- 6) Write Silver (append-only) + partition dt
query = (
    silver_ready.writeStream
    .format("parquet")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT)
    .partitionBy("dt")
    .trigger(processingTime="5 seconds")
    .start(SILVER_OUT)
)

print("🚀 Streaming started.")
print("   Kafka UI: http://localhost:8089")
print("   Silver out:", os.path.abspath(SILVER_OUT))
print("   Checkpoint:", os.path.abspath(CHECKPOINT))

query.awaitTermination()
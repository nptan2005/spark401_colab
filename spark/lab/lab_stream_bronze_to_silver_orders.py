import os
import signal
import sys
import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, current_timestamp


spark = (
    SparkSession.builder
    .appName("orders-bronze-silver")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

BRONZE_PATH = os.getenv("BRONZE_PATH", "s3a://lakehouse/bronze/orders")
SILVER_PATH = os.getenv("SILVER_PATH", "s3a://lakehouse/silver/orders")
CHECKPOINT_PATH = os.getenv("CHECKPOINT_PATH", "s3a://lakehouse/_checkpoints/orders_silver")

print(f"[config] bronze={BRONZE_PATH}")
print(f"[config] silver={SILVER_PATH}")
print(f"[config] checkpoint={CHECKPOINT_PATH}")

# 1) Infer schema from existing bronze parquet files (static read)
static_df = spark.read.parquet(BRONZE_PATH)
bronze_schema = static_df.schema
print("[schema] inferred from bronze:")
print(bronze_schema.simpleString())

# 2) Streaming read with schema
bronze_stream = (
    spark.readStream
    .schema(bronze_schema)
    .format("parquet")
    .load(BRONZE_PATH)
)

# 3) Basic cleaning/typing (example)
silver_df = (
    bronze_stream
    .withColumn("event_time", to_timestamp(col("event_ts")))
    .withColumn("ingest_ts", current_timestamp())
    .dropna(subset=["order_id", "event_ts"])
)

query = (
    silver_df.writeStream
    .format("parquet")
    .outputMode("append")
    .option("path", SILVER_PATH)
    .option("checkpointLocation", CHECKPOINT_PATH)
    .trigger(processingTime="30 seconds")
    .start()
)


def _graceful_shutdown(signum, frame):
    # Avoid Py4J "reentrant call" / broken pipe when Ctrl+C happens while JVM is busy.
    try:
        print("\n[shutdown] signal received, stopping streaming query ...", flush=True)
        if query is not None and query.isActive:
            query.stop()
    except Exception as e:
        print(f"[shutdown] query.stop() error: {e}", flush=True)
    try:
        print("[shutdown] stopping SparkSession ...", flush=True)
        spark.stop()
    except Exception as e:
        print(f"[shutdown] spark.stop() error: {e}", flush=True)
    time.sleep(0.5)
    sys.exit(0)


signal.signal(signal.SIGINT, _graceful_shutdown)
signal.signal(signal.SIGTERM, _graceful_shutdown)

print("[run] streaming started. Press Ctrl+C to stop.")
query.awaitTermination()
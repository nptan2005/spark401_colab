# spark/lab/lab_stream_to_silver_orders.py
import os
import shutil
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType, DateType
)

BRONZE_ORDERS_PATH = "data/bronze_lab33/orders_raw"
SILVER_OUT = "data/silver/orders_fact_dt_stream"
CHECKPOINT = "checkpoints/orders_to_silver"

# 1) Streaming file source MUST have schema
ORDERS_SCHEMA = StructType([
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("merchant_id", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("event_ts", TimestampType(), True),
    StructField("channel", StringType(), True),
    StructField("country", StringType(), True),
    StructField("status", StringType(), True),
    StructField("ingest_ts", TimestampType(), True),
    StructField("dt", DateType(), True),  # partition key
])

spark = (
    SparkSession.builder
    .appName("lab_stream_to_silver_orders")
    .config("spark.sql.shuffle.partitions", "50")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

def _path_exists(p: str) -> bool:
    return os.path.exists(p) and (len(os.listdir(p)) > 0)

def upsert_partition(microbatch_df, batch_id: int):
    """
    microbatch_df: static DF of 1 micro-batch (NOT streaming inside this function)
    batch_id: int
    """
    if microbatch_df.rdd.isEmpty():
        print(f"✅ batch={batch_id} empty")
        return

    # Avoid re-reading/rewriting the same DF multiple times
    df = microbatch_df.select(
        "order_id", "customer_id", "merchant_id", "amount",
        "event_ts", "channel", "country", "status", "ingest_ts", "dt"
    )

    # Collect list of affected partitions (dt) in this micro-batch
    dts = [r["dt"] for r in df.select("dt").distinct().collect()]

    for dt_val in dts:
        # ---- 1) Take only this dt from the micro-batch
        part_new = df.where(F.col("dt") == F.lit(dt_val))

        # ---- 2) Read existing partition (if any)
        part_path = os.path.join(SILVER_OUT, f"dt={dt_val}")
        if os.path.exists(part_path):
            part_old = spark.read.parquet(part_path)
            merged = part_old.unionByName(part_new, allowMissingColumns=True)
        else:
            merged = part_new

        # ---- 3) Dedup / Upsert by business key (order_id)
        # Keep newest record per order_id (ingest_ts desc, event_ts desc)
        w = Window.partitionBy("order_id").orderBy(F.col("ingest_ts").desc(), F.col("event_ts").desc())
        dedup = (
            merged
            .withColumn("_rn", F.row_number().over(w))
            .where(F.col("_rn") == 1)
            .drop("_rn")
        )

        # Optional: count BEFORE we replace files (safe)
        rows = dedup.count()
        print(f"✅ batch={batch_id} dt={dt_val} upsert_rows={rows}")

        # ---- 4) Write to a temp location, then atomic replace the partition folder
        tmp_part = os.path.join(SILVER_OUT, f"_tmp_dt={dt_val}_batch={batch_id}")
        if os.path.exists(tmp_part):
            shutil.rmtree(tmp_part)

        # IMPORTANT:
        # Write WITHOUT partitionBy here because we're already writing a single dt partition folder
        # Also: drop dt from file payload to prevent dt-in-file in the future
        (dedup.drop("dt")
              .write
              .mode("overwrite")
              .parquet(tmp_part))

        # Replace partition folder
        if os.path.exists(part_path):
            shutil.rmtree(part_path)
        os.makedirs(SILVER_OUT, exist_ok=True)
        shutil.move(tmp_part, part_path)

    # Clear any cached file listing / metadata
    spark.catalog.clearCache()

# ===== Streaming read =====
stream_df = (
    spark.readStream
    .schema(ORDERS_SCHEMA)
    .format("parquet")
    .load(BRONZE_ORDERS_PATH)
    # Watermark: for later labs (late data)
    .withWatermark("event_ts", "3 days")
)

query = (
    stream_df.writeStream
    .foreachBatch(upsert_partition)
    .option("checkpointLocation", CHECKPOINT)
    .trigger(processingTime="5 seconds")
    .start()
)

# Spark UI
print("🚀 Streaming started. Spark UI:", spark.sparkContext.uiWebUrl)

try:
    query.awaitTermination()
except KeyboardInterrupt:
    print("🛑 Caught Ctrl+C. Stopping query...")
    query.stop()
finally:
    spark.stop()
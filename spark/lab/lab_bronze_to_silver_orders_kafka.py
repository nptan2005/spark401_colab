from __future__ import annotations

import os
import shutil
import json
from typing import List

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, TimestampType, DateType,
    LongType, IntegerType
)

BRONZE_PATH = "data/bronze_kafka/orders_raw"
SILVER_PATH = "data/silver_kafka/orders_silver"
CHECKPOINT = "checkpoints/silver_orders_kafka"
TMP_ROOT = "data/tmp/silver_orders_upsert"

# ============ Governance tables ============
GOV_DB = "data/gov"
JOB_RUNS = f"{GOV_DB}/job_runs"
SCHEMA_SNAP = f"{GOV_DB}/schema_snapshots"

# Explicit schemas (avoid PySpark schema inference failures in foreachBatch)
JOB_RUN_SCHEMA = StructType([
    StructField("job_name", StringType(), False),
    StructField("batch_id", LongType(), False),
    StructField("source_path", StringType(), True),
    StructField("target_path", StringType(), True),
    StructField("input_rows", LongType(), True),
    StructField("output_rows", LongType(), True),
    StructField("dq_failed_rows", LongType(), True),
    StructField("status", StringType(), True),
    StructField("error_message", StringType(), True),
    StructField("ts", TimestampType(), True),
])


def ensure_dir(p: str) -> None:
    os.makedirs(p, exist_ok=True)


def path_exists(p: str) -> bool:
    return os.path.exists(p)


def replace_partition_atomic(target_part_dir: str, tmp_part_dir: str) -> None:
    """
    Local FS atomic-ish replace for one partition directory:
    - move existing -> backup
    - move tmp -> target
    - delete backup
    """
    backup = target_part_dir + ".__bak"
    if os.path.exists(backup):
        shutil.rmtree(backup)

    if os.path.exists(target_part_dir):
        os.rename(target_part_dir, backup)

    os.rename(tmp_part_dir, target_part_dir)

    if os.path.exists(backup):
        shutil.rmtree(backup)


def log_job_run(
    spark: SparkSession,
    job_name: str,
    batch_id: int,
    source_path: str,
    target_path: str,
    input_rows: int,
    output_rows: int,
    dq_failed_rows: int,
    status: str,
    error_message: str
) -> None:
    ensure_dir(JOB_RUNS)

    rows = [(
        str(job_name),
        int(batch_id),
        str(source_path) if source_path is not None else None,
        str(target_path) if target_path is not None else None,
        int(input_rows),
        int(output_rows),
        int(dq_failed_rows),
        str(status) if status is not None else None,
        (error_message or "")[:1000],
        None,
    )]

    df = spark.createDataFrame(rows, schema=JOB_RUN_SCHEMA)
    df = df.withColumn("ts", F.current_timestamp())

    df.write.mode("append").parquet(JOB_RUNS)


def snapshot_schema(spark: SparkSession, df: DataFrame, table_name: str, location: str) -> None:
    ensure_dir(SCHEMA_SNAP)
    fields = [{"name": f.name, "type": str(f.dataType), "nullable": bool(f.nullable)} for f in df.schema.fields]
    snap = spark.createDataFrame([{
        "table_name": table_name,
        "location": location,
        "schema_json": json.dumps(fields, ensure_ascii=False),
    }]).withColumn("ts", F.current_timestamp())
    snap.write.mode("append").parquet(SCHEMA_SNAP)


# ===== Schema phải khai báo khi đọc streaming từ file =====
bronze_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("merchant_id", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("event_ts", TimestampType(), True),
    StructField("channel", StringType(), True),
    StructField("country", StringType(), True),
    StructField("status", StringType(), True),
    StructField("k", StringType(), True),
    StructField("kafka_ts", TimestampType(), True),
    StructField("ingest_ts", TimestampType(), True),
    StructField("dt", DateType(), True),
])

spark = (
    SparkSession.builder
    .appName("lab_bronze_to_silver_orders_kafka")
    .config("spark.sql.shuffle.partitions", "50")
    # AQE is not supported for streaming; turn off to avoid warning noise
    .config("spark.sql.adaptive.enabled", "false")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

ensure_dir(SILVER_PATH)
ensure_dir(CHECKPOINT)
ensure_dir(TMP_ROOT)
ensure_dir(GOV_DB)

job_name = "silver_orders_kafka_upsert"


def dedup_latest(df: DataFrame) -> DataFrame:
    """
    Dedup theo order_id, giữ record mới nhất theo (event_ts desc, ingest_ts desc).
    """
    w = Window.partitionBy("order_id").orderBy(F.col("event_ts").desc_nulls_last(),
                                              F.col("ingest_ts").desc_nulls_last())
    return (
        df.withColumn("_rn", F.row_number().over(w))
          .filter(F.col("_rn") == 1)
          .drop("_rn")
    )


def dq_split(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """
    DQ rất thực tế:
    - amount > 0
    - order_id/customer_id/merchant_id not null
    - event_ts not null
    """
    bad = df.filter(
        (F.col("order_id").isNull()) |
        (F.col("customer_id").isNull()) |
        (F.col("merchant_id").isNull()) |
        (F.col("event_ts").isNull()) |
        (F.col("amount").isNull()) |
        (F.col("amount") <= 0)
    )
    good = df.subtract(bad)
    return good, bad


def upsert_microbatch(batch_df: DataFrame, batch_id: int) -> None:
    try:
        in_rows = batch_df.count()

        # 1) DQ
        good, bad = dq_split(batch_df)
        dq_failed = bad.count()

        # 2) Dedup trong batch
        good_dedup = dedup_latest(good)

        # 3) Lấy danh sách partition dt bị ảnh hưởng
        dts: List[str] = [r["dt"].strftime("%Y-%m-%d") for r in good_dedup.select("dt").distinct().collect() if r["dt"]]
        print(f"✅ batch={batch_id} in_rows={in_rows} dq_failed={dq_failed} affected_dts={dts}")

        # Nếu batch không có dữ liệu hợp lệ
        if not dts:
            log_job_run(spark, job_name, batch_id, BRONZE_PATH, SILVER_PATH, in_rows, 0, dq_failed, "SUCCESS", "")
            return

        # 4) Upsert theo từng dt (rewrite partition)
        out_rows_total = 0

        for dt_str in dts:
            target_part = os.path.join(SILVER_PATH, f"dt={dt_str}")
            tmp_part_root = os.path.join(TMP_ROOT, f"batch={batch_id}", f"dt={dt_str}")
            ensure_dir(tmp_part_root)

            incoming_dt = good_dedup.filter(F.col("dt") == F.to_date(F.lit(dt_str)))

            # đọc existing partition nếu có
            if path_exists(target_part):
                existing = spark.read.parquet(target_part)
                merged = existing.unionByName(incoming_dt, allowMissingColumns=True)
            else:
                merged = incoming_dt

            # dedup sau merge (đảm bảo idempotent)
            merged_dedup = dedup_latest(merged)

            # ghi ra tmp (không partitionBy nữa vì đang ở đúng partition dt=)
            merged_dedup.write.mode("overwrite").parquet(tmp_part_root)

            # IMPORTANT: do NOT call actions on `merged_dedup` after we swap/overwrite partition dirs.
            # Count rows from the freshly written tmp files BEFORE moving them.
            dt_out = spark.read.parquet(tmp_part_root).count()

            # replace partition dir
            replace_partition_atomic(target_part, tmp_part_root)

            out_rows_total += dt_out
            print(f"   ↪ upserted dt={dt_str} rows={dt_out}")

        # 5) Snapshot schema (mỗi batch cũng được, hoặc mỗi ngày)
        snapshot_schema(spark, good_dedup, "silver.orders", SILVER_PATH)

        log_job_run(
            spark, job_name, batch_id,
            BRONZE_PATH, SILVER_PATH,
            in_rows, out_rows_total, dq_failed,
            "SUCCESS", ""
        )

    except Exception as e:
        log_job_run(
            spark, job_name, batch_id,
            BRONZE_PATH, SILVER_PATH,
            0, 0, 0,
            "FAILED", str(e)
        )
        raise


# Streaming read from Bronze (file source)
bronze_stream = (
    spark.readStream
    .schema(bronze_schema)
    .format("parquet")
    .load(BRONZE_PATH)
)

# Watermark để chuẩn streaming (dù upsert theo dt)
# -> giúp Spark quản lý state tốt hơn khi bạn làm aggregation/window trong tương lai
bronze_stream = bronze_stream.withWatermark("event_ts", "3 days")

query = (
    bronze_stream.writeStream
    .foreachBatch(upsert_microbatch)
    .option("checkpointLocation", CHECKPOINT)
    .trigger(processingTime="10 seconds")
    .start()
)

print("✅ Streaming started: Bronze(parquet) -> Silver(upsert partition dt)")
print("   - bronze:", BRONZE_PATH)
print("   - silver:", SILVER_PATH)

try:
    query.awaitTermination()
except KeyboardInterrupt:
    # Ctrl+C graceful-ish
    print("⏹ stopping stream...")
    query.stop()
finally:
    spark.stop()
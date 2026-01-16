# spark/lab/lab_stream_to_silver_orders_v2.py
import os
import shutil
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType, DateType
)
from pyspark.sql.functions import col, to_timestamp, to_date, lit, row_number
from pyspark.sql.window import Window

BRONZE_ORDERS_PATH = "data/bronze_lab33/orders_raw"
SILVER_OUT = "data/silver/orders_fact_dt_stream"
CHECKPOINT = "checkpoints/orders_to_silver_v2"

TMP_ROOT = "data/tmp/orders_upsert"

os.makedirs(SILVER_OUT, exist_ok=True)
os.makedirs(CHECKPOINT, exist_ok=True)
os.makedirs(TMP_ROOT, exist_ok=True)

spark = (
    SparkSession.builder
    .appName("lab_stream_to_silver_orders_v2")
    .config("spark.sql.shuffle.partitions", "50")
    # streaming không dùng AQE: Spark sẽ tự warning & disable
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# 1) Streaming file source bắt buộc schema (bạn gặp lỗi này lần đầu)
schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("merchant_id", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("event_ts", StringType(), True),   # bronze là string
    StructField("channel", StringType(), True),
    StructField("country", StringType(), True),
    StructField("status", StringType(), True),
    StructField("ingest_ts", StringType(), True),
    StructField("dt", StringType(), True),         # bronze dt string
])

def atomic_replace_dir(src_dir: str, dst_dir: str):
    """
    Thao tác “an toàn tương đối” trên local:
    - Xoá dst nếu tồn tại
    - Move src -> dst (rename/move là atomic hơn copy)
    """
    if os.path.exists(dst_dir):
        shutil.rmtree(dst_dir)
    os.makedirs(os.path.dirname(dst_dir), exist_ok=True)
    shutil.move(src_dir, dst_dir)

def upsert_batch(batch_df, batch_id: int):
    """
    foreachBatch: chạy trên mỗi micro-batch (DataFrame tĩnh).
    Mục tiêu:
    - watermark + dedup theo order_id (giả lập idempotent)
    - upsert theo dt: rewrite từng dt partition bằng swap folder
    """
    if batch_df.rdd.isEmpty():
        print(f"ℹ️ batch={batch_id} empty")
        return

    # 2) Chuẩn hoá kiểu dữ liệu
    df = (
        batch_df
        .withColumn("event_ts", to_timestamp(col("event_ts")))
        .withColumn("ingest_ts", to_timestamp(col("ingest_ts")))
        .withColumn("dt", to_date(col("dt")))
    )

    # 3) Dedup thực tế: giữ record mới nhất theo ingest_ts cho mỗi order_id
    w = Window.partitionBy("order_id").orderBy(col("ingest_ts").desc())
    dedup = (
        df
        .withColumn("rn", row_number().over(w))
        .where(col("rn") == 1)
        .drop("rn")
    )

    # Lấy danh sách dt trong batch để upsert từng partition
    dts = [r["dt"] for r in dedup.select("dt").distinct().collect()]

    # Tính rows trước khi write để không trigger đọc sau khi replace
    rows_in_batch = dedup.count()
    print(f"✅ batch={batch_id} rows_after_dedup={rows_in_batch} partitions={len(dts)} dts={dts}")

    for dtv in dts:
        dt_str = dtv.isoformat()
        part_path = os.path.join(SILVER_OUT, f"dt={dt_str}")
        tmp_path = os.path.join(TMP_ROOT, f"batch={batch_id}", f"dt={dt_str}")

        # (a) lọc partition dt của batch
        part_new = dedup.where(col("dt") == lit(dtv))

        # (b) nếu partition đã tồn tại → đọc cũ, union, dedup lại theo order_id
        if os.path.exists(part_path):
            old = spark.read.parquet(part_path)
            merged = old.unionByName(part_new)

            w2 = Window.partitionBy("order_id").orderBy(col("ingest_ts").desc())
            final_part = (
                merged
                .withColumn("rn", row_number().over(w2))
                .where(col("rn") == 1)
                .drop("rn")
            )
        else:
            final_part = part_new

        # (c) write ra tmp (không đụng partition thật)
        if os.path.exists(tmp_path):
            shutil.rmtree(tmp_path)
        os.makedirs(tmp_path, exist_ok=True)

        # (
        #     final_part
        #     .coalesce(1)   # local lab: 1 file/partition cho dễ nhìn (production: KHÔNG làm vậy)
        #     .write
        #     .mode("overwrite")
        #     .parquet(tmp_path)
        # )

        # (c) write ra tmp (không đụng partition thật)
        # IMPORTANT: drop("dt") để dt chỉ lấy từ partition folder dt=...
        (
            final_part
            .drop("dt")
            .coalesce(1)   # lab local cho dễ nhìn
            .write
            .mode("overwrite")
            .parquet(tmp_path)
        )

        # (d) swap folder: tmp -> dt=...
        atomic_replace_dir(tmp_path, part_path)
        print(f"   ↪ upserted dt={dt_str} ok")

# 4) Đọc stream từ file source
stream_df = (
    spark.readStream
    .schema(schema)
    .json(BRONZE_ORDERS_PATH)
)

# 5) Start streaming
query = (
    stream_df
    .writeStream
    .foreachBatch(upsert_batch)
    .option("checkpointLocation", CHECKPOINT)
    # .trigger(processingTime="5 seconds")
    # thay vì .trigger(processingTime="5 seconds")
    .trigger(availableNow=True)
    .start()
)

print("🚀 Streaming started. Spark UI:", spark.sparkContext.uiWebUrl)

query.awaitTermination()
import os
import shutil
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StringType, TimestampType, DoubleType
from datetime import datetime



spark = (SparkSession.builder
         .appName("bronze_to_silver_upsert")
         .getOrCreate())

# Đường dẫn
BRONZE_PATH = "data/bronze_kafka/orders_raw"
SILVER_PATH = "data/silver_kafka/orders_silver"
STAGING_BASE = "data/silver_kafka/_staging"
JOB_RUNS_PATH = "data/gov/job_runs"

# Định nghĩa schema tường minh để tránh inference
schema = (StructType()
          .add("order_id", StringType())
          .add("customer_id", StringType())
          .add("merchant_id", StringType())
          .add("amount", DoubleType())
          .add("event_ts", TimestampType())
          .add("channel", StringType())
          .add("country", StringType())
          .add("status", StringType())
          .add("dt", StringType())
          .add("ingest_ts", TimestampType()))

# Đọc stream từ Bronze (FileStreamSource), không infer schema mà dùng schema đã biết
bronze_stream = (spark.readStream
                 .schema(schema)
                 .parquet(BRONZE_PATH))

# Xử lý watermark và dedup theo event_ts + ingest_ts (giữ bản ghi mới nhất)
with_watermark = bronze_stream.withWatermark("event_ts", "3 days")

def foreach_batch_function(batch_df: DataFrame, batch_id: int):
    from pyspark.sql.functions import max as spark_max

    if batch_df.isEmpty():
        print(f"Batch {batch_id}: empty batch, skipping")
        return

    # Lấy danh sách dt trong batch
    dt_list = [row.dt for row in batch_df.select("dt").distinct().collect()]
    print(f"Batch {batch_id}: processing dt partitions {dt_list}")

    for dt in dt_list:
        batch_dt = batch_df.filter(col("dt") == dt)

        # Đọc partition silver hiện tại nếu có
        silver_partition_path = os.path.join(SILVER_PATH, f"dt={dt}")
        if os.path.exists(silver_partition_path):
            existing_df = spark.read.parquet(silver_partition_path)
        else:
            existing_df = spark.createDataFrame([], schema)

        # Union batch mới với dữ liệu hiện có
        combined_df = existing_df.union(batch_dt)

        # Dedup giữ bản ghi mới nhất theo event_ts, ingest_ts
        window_spec = Window.partitionBy("order_id").orderBy(col("event_ts").desc(), col("ingest_ts").desc())
        dedup_df = (combined_df
                    .withColumn("rn", row_number().over(window_spec))
                    .filter(col("rn") == 1)
                    .drop("rn"))

        # Viết ra thư mục staging theo batch_id để tránh ghi đè không nguyên tử
        staging_path = os.path.join(STAGING_BASE, f"dt={dt}", f"batch={batch_id}")
        dedup_df.write.mode("overwrite").parquet(staging_path)

        # Xóa thư mục final cũ nếu tồn tại
        if os.path.exists(silver_partition_path):
            shutil.rmtree(silver_partition_path)

        # Di chuyển thư mục staging thành thư mục final (atomic swap)
        shutil.move(staging_path, silver_partition_path)

        print(f"Batch {batch_id}: dt={dt} upserted to silver")

    # Ghi log job run
    from pyspark.sql import Row
    job_run_schema = (StructType()
                      .add("batch_id", StringType())
                      .add("timestamp", TimestampType())
                      .add("rows_processed", StringType()))

    rows_processed = batch_df.count()
    job_run = spark.createDataFrame([Row(batch_id=str(batch_id),
                                         timestamp=datetime.now(),
                                         rows_processed=str(rows_processed))], schema=job_run_schema)

    # Append log job run
    job_run.write.mode("append").parquet(JOB_RUNS_PATH)
    print(f"Batch {batch_id}: logged job run with {rows_processed} rows")

query = (with_watermark
         .dropDuplicates(["order_id"])
         .writeStream
         .foreachBatch(foreach_batch_function)
         .option("checkpointLocation", "checkpoints/silver_kafka_upsert")
         .start())



try:
    query.awaitTermination()
except KeyboardInterrupt:
    print("\n⛔ Caught Ctrl+C. Stopping streaming query...")
    try:
        import signal, sys
        def _stop(*_):
            try: query.stop()
            except: pass
            try: spark.stop()
            except: pass
            sys.exit(0)
        signal.signal(signal.SIGINT, _stop)
        signal.signal(signal.SIGTERM, _stop)
    except Exception as e:
        print(f"WARN: query.stop() failed: {e}")
finally:
    try:
        spark.stop()
    except Exception:
        pass
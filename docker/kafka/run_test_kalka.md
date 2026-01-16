## 1) Test Kafka đúng “context”

### 1.1. Chạy lệnh trong container kafka → dùng localhost:9092

```bash
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --list
```

Tạo topic:

```bash
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --topic orders --partitions 3 --replication-factor 1
```

Describe:

```bash
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --describe --topic orders
```

Produce (gõ vài dòng JSON, Enter từng dòng, Ctrl+C để thoát):

```bash
docker exec -it kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 --topic orders
```

Consume:

```bash
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic orders --from-beginning
```

### 1.2. Chạy lệnh từ máy host (Mac) → dùng localhost:9094
-	App chạy ngoài Docker (Spark/Python trên host) phải dùng 9094 vì bạn advertise EXTERNAL localhost:9094.

---


# Streaming + Governance (đi đúng “professional”)

## A) Data Streaming chuyên nghiệp (Kafka → Bronze → Silver → Gold)

### Mục tiêu thực tế:
-	Exactly-once-ish bằng checkpoint + idempotent write pattern
-	Late data: event-time watermark
-	Dedup: theo order_id hoặc (order_id, event_ts) + window
-	Upsert Silver: “rewrite partition” (Parquet) hoặc Delta/Iceberg (chuẩn hơn)

### A1) Spark Structured Streaming đọc Kafka (host)

Ví dụ “orders” JSON:

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_date
from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType

spark = (SparkSession.builder
  .appName("kafka_to_bronze")
  .getOrCreate())

schema = (StructType()
  .add("order_id", StringType())
  .add("customer_id", StringType())
  .add("merchant_id", StringType())
  .add("amount", DoubleType())
  .add("event_ts", TimestampType())
  .add("channel", StringType())
  .add("country", StringType())
  .add("status", StringType())
)

kafka = (spark.readStream
  .format("kafka")
  .option("kafka.bootstrap.servers", "localhost:9094")
  .option("subscribe", "orders")
  .option("startingOffsets", "latest")
  .load())

parsed = (kafka.selectExpr("CAST(value AS STRING) as json")
  .select(from_json(col("json"), schema).alias("o"))
  .select("o.*")
  .withColumn("dt", to_date(col("event_ts")))
)

query = (parsed.writeStream
  .format("parquet")
  .option("path", "data/bronze_kafka/orders_raw")
  .option("checkpointLocation", "checkpoints/bronze_kafka_orders")
  .partitionBy("dt")
  .outputMode("append")
  .start())

query.awaitTermination()
```

#### Ghi chú “thực tế”
-	Bronze nên ghi append-only + partition theo dt / ingest_dt.
-	Đừng join dim ở Bronze (để sau).

### A2) Bronze → Silver (dedup + late data + upsert partition)

#### Silver pattern (Parquet) chuẩn cho lab:
-	withWatermark("event_ts", "3 days")
-	dedup theo order_id (giữ record mới nhất)
-	foreachBatch: mỗi batch tách theo dt, rewrite partition dt=...

### A2.1) Vì sao bạn gặp lỗi FILE_NOT_EXIST (đọc/ghi cùng lúc)

- Trong `foreachBatch`, code thường làm: đọc partition silver hiện tại → union với batch mới → overwrite lại chính partition đó → rồi gọi `.count()` hoặc các thao tác khác trên DataFrame mà vẫn tham chiếu đến file cũ đã bị xóa/ghi đè.
- Spark sẽ cố đọc các file đã bị xóa hoặc ghi đè, dẫn đến lỗi FILE_NOT_EXIST.
- Trên filesystem local, lỗi này xảy ra thường xuyên hơn; trên object storage thì biểu hiện khác nhưng nguyên nhân gốc là do ghi đè không nguyên tử (non-atomic overwrite) và kế hoạch thực thi (plan) hoặc cache bị lỗi thời.

### A2.2) Pattern Upsert Parquet an toàn (staging + atomic swap)

```python
# file: spark/lab/lab_bronze_to_silver_orders_kafka_v2.py

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

query.awaitTermination()
```

### Runbook

- Xóa checkpoint và dữ liệu silver để chạy lại sạch:

```bash
rm -rf checkpoints/silver_kafka_upsert
rm -rf data/silver_kafka/orders_silver
rm -rf data/silver_kafka/_staging
```

- Chạy job upsert:

```bash
python spark/lab/lab_bronze_to_silver_orders_kafka_v2.py
```

- Kiểm tra số lượng bản ghi silver và xem vài dòng:

```bash
python - <<'PY'
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
df = spark.read.parquet("data/silver_kafka/orders_silver")
print("Silver rows =", df.count())
df.orderBy("event_ts", ascending=False).show(5, truncate=False)
spark.stop()
PY
```

---

# Test streaming manual

### Start Spark đọc streaming

```bash
python spark/lab/lab_kafka_order_json.py
```

#### Produce vài JSON message mẫu

```bash
docker exec -it kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server kafka:9092 --topic orders
```

#### Paste từng dòng JSON (Enter mỗi dòng), ví dụ:

```json
{"order_id":"o1","customer_id":"c1","merchant_id":"m1","amount":120.5,"event_ts":"2026-01-16T15:50:00","channel":"ECOM","country":"VN","status":"SUCCESS"}
{"order_id":"o2","customer_id":"c2","merchant_id":"m2","amount":42.0,"event_ts":"2026-01-16T15:51:00","channel":"POS","country":"VN","status":"FAILED"}
```

### 3) Verify đã ghi xuống Bronze chưa

Mở terminal khác:

```bash
find data/bronze_kafka/orders_raw -maxdepth 2 -type d | head
find data/bronze_kafka/orders_raw -name "*.parquet" | head -n 3
```

Đọc nhanh

```bash
python - <<'PY'
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
df = spark.read.parquet("data/bronze_kafka/orders_raw")
print("rows =", df.count())
df.orderBy("ingest_ts", ascending=False).show(10, truncate=False)
spark.stop()
PY
```

---

### Script test

```python
# file: spark/test/kafka_producer_orders.py
# pip install kafka-python

import argparse
import json
import random
import string
import time
from datetime import datetime, timedelta, timezone

from kafka import KafkaProducer


CHANNELS = ["ECOM", "POS", "ATM"]
COUNTRIES = ["VN", "TH", "MY", "SG"]
STATUSES = ["SUCCESS", "FAILED", "REVERSED"]


def rand_id(prefix: str, n: int = 6) -> str:
    return prefix + "".join(random.choices(string.digits, k=n))


def iso_ts(dt: datetime) -> str:
    # Kafka message đang dùng kiểu "2026-01-16T15:50:00"
    return dt.strftime("%Y-%m-%dT%H:%M:%S")


def make_event(order_id: str | None = None) -> dict:
    now = datetime.now(timezone.utc).astimezone()  # local tz
    # random event time trong 0..180 phút trước (để test late-ish)
    event_dt = now - timedelta(minutes=random.randint(0, 180))

    oid = order_id or rand_id("o", 7)
    return {
        "order_id": oid,
        "customer_id": rand_id("c", 5),
        "merchant_id": rand_id("m", 5),
        "amount": round(random.uniform(1.0, 5000.0), 2),
        "event_ts": iso_ts(event_dt),
        "channel": random.choice(CHANNELS),
        "country": random.choice(COUNTRIES),
        "status": random.choice(STATUSES),
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--brokers", default="localhost:9094", help="Kafka bootstrap servers")
    ap.add_argument("--topic", default="orders", help="Topic name")
    ap.add_argument("--rate", type=float, default=2.0, help="Messages per second")
    ap.add_argument("--seconds", type=int, default=60, help="How long to run")
    ap.add_argument("--dup_rate", type=float, default=0.2, help="Chance to resend an old order_id (0..1)")
    ap.add_argument("--seed", type=int, default=None, help="Random seed")
    args = ap.parse_args()

    if args.seed is not None:
        random.seed(args.seed)

    producer = KafkaProducer(
        bootstrap_servers=args.brokers.split(","),
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        acks="all",
        retries=10,
        linger_ms=20,
    )

    # giữ 1 pool order_id để bắn duplicate
    seen_order_ids: list[str] = []

    start = time.time()
    sent = 0
    interval = 1.0 / max(args.rate, 0.0001)

    try:
        while time.time() - start < args.seconds:
            use_dup = (seen_order_ids and random.random() < args.dup_rate)
            if use_dup:
                oid = random.choice(seen_order_ids)
                event = make_event(order_id=oid)
            else:
                event = make_event()
                seen_order_ids.append(event["order_id"])
                if len(seen_order_ids) > 2000:
                    seen_order_ids = seen_order_ids[-2000:]

            future = producer.send(args.topic, value=event)
            # chờ ack để log ổn định (có thể bỏ để max throughput)
            meta = future.get(timeout=10)

            sent += 1
            print(f"✅ sent #{sent} topic={meta.topic} partition={meta.partition} offset={meta.offset} -> {event}")

            time.sleep(interval)

    except KeyboardInterrupt:
        print("\n⛔ stopped by user")
    finally:
        producer.flush()
        producer.close()
        print(f"Done. total_sent={sent}")


if __name__ == "__main__":
    main()
```

---

### Cách test:

```bash
python spark/test/kafka_producer_orders.py \
  --brokers localhost:9094 \
  --topic orders \
  --rate 5 \
  --seconds 120 \
  --dup_rate 0.3
```

### Kiểm tra kafka có nhận được hay ko?

```bash
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 --topic orders --from-beginning
```

---

## 1) Test Kafka đúng “context”

### 1.1. Chạy lệnh trong container kafka → dùng localhost:9092

```bash
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --list
```

Tạo topic:

```bash
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --topic orders --partitions 3 --replication-factor 1
```

Describe:

```bash
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --describe --topic orders
```

Produce (gõ vài dòng JSON, Enter từng dòng, Ctrl+C để thoát):

```bash
docker exec -it kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 --topic orders
```

Consume:

```bash
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic orders --from-beginning
```

### 1.2. Chạy lệnh từ máy host (Mac) → dùng localhost:9094
-	App chạy ngoài Docker (Spark/Python trên host) phải dùng 9094 vì bạn advertise EXTERNAL localhost:9094.

---


# Streaming + Governance (đi đúng “professional”)

## A) Data Streaming chuyên nghiệp (Kafka → Bronze → Silver → Gold)

### Mục tiêu thực tế:
-	Exactly-once-ish bằng checkpoint + idempotent write pattern
-	Late data: event-time watermark
-	Dedup: theo order_id hoặc (order_id, event_ts) + window
-	Upsert Silver: “rewrite partition” (Parquet) hoặc Delta/Iceberg (chuẩn hơn)

### A1) Spark Structured Streaming đọc Kafka (host)

Ví dụ “orders” JSON:

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_date
from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType

spark = (SparkSession.builder
  .appName("kafka_to_bronze")
  .getOrCreate())

schema = (StructType()
  .add("order_id", StringType())
  .add("customer_id", StringType())
  .add("merchant_id", StringType())
  .add("amount", DoubleType())
  .add("event_ts", TimestampType())
  .add("channel", StringType())
  .add("country", StringType())
  .add("status", StringType())
)

kafka = (spark.readStream
  .format("kafka")
  .option("kafka.bootstrap.servers", "localhost:9094")
  .option("subscribe", "orders")
  .option("startingOffsets", "latest")
  .load())

parsed = (kafka.selectExpr("CAST(value AS STRING) as json")
  .select(from_json(col("json"), schema).alias("o"))
  .select("o.*")
  .withColumn("dt", to_date(col("event_ts")))
)

query = (parsed.writeStream
  .format("parquet")
  .option("path", "data/bronze_kafka/orders_raw")
  .option("checkpointLocation", "checkpoints/bronze_kafka_orders")
  .partitionBy("dt")
  .outputMode("append")
  .start())

query.awaitTermination()
```

#### Ghi chú “thực tế”
-	Bronze nên ghi append-only + partition theo dt / ingest_dt.
-	Đừng join dim ở Bronze (để sau).

### A2) Bronze → Silver (dedup + late data + upsert partition)

#### Silver pattern (Parquet) chuẩn cho lab:
-	withWatermark("event_ts", "3 days")
-	dedup theo order_id (giữ record mới nhất)
-	foreachBatch: mỗi batch tách theo dt, rewrite partition dt=...

### A2.1) Vì sao bạn gặp lỗi FILE_NOT_EXIST (đọc/ghi cùng lúc)

- Trong `foreachBatch`, code thường làm: đọc partition silver hiện tại → union với batch mới → overwrite lại chính partition đó → rồi gọi `.count()` hoặc các thao tác khác trên DataFrame mà vẫn tham chiếu đến file cũ đã bị xóa/ghi đè.
- Spark sẽ cố đọc các file đã bị xóa hoặc ghi đè, dẫn đến lỗi FILE_NOT_EXIST.
- Trên filesystem local, lỗi này xảy ra thường xuyên hơn; trên object storage thì biểu hiện khác nhưng nguyên nhân gốc là do ghi đè không nguyên tử (non-atomic overwrite) và kế hoạch thực thi (plan) hoặc cache bị lỗi thời.

### A2.2) Pattern Upsert Parquet an toàn (staging + atomic swap)

```python
# file: spark/lab/lab_bronze_to_silver_orders_kafka_v2.py

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

query.awaitTermination()
```

### Runbook

- Xóa checkpoint và dữ liệu silver để chạy lại sạch:

```bash
rm -rf checkpoints/silver_kafka_upsert
rm -rf data/silver_kafka/orders_silver
rm -rf data/silver_kafka/_staging
```

- Chạy job upsert:

```bash
python spark/lab/lab_bronze_to_silver_orders_kafka_v2.py
```

- Kiểm tra số lượng bản ghi silver và xem vài dòng:

```bash
python - <<'PY'
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
df = spark.read.parquet("data/silver_kafka/orders_silver")
print("Silver rows =", df.count())
df.orderBy("event_ts", ascending=False).show(5, truncate=False)
spark.stop()
PY
```

---

# Test streaming manual

### Start Spark đọc streaming

```bash
python spark/lab/lab_kafka_order_json.py
```

#### Produce vài JSON message mẫu

```bash
docker exec -it kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server kafka:9092 --topic orders
```

#### Paste từng dòng JSON (Enter mỗi dòng), ví dụ:

```json
{"order_id":"o1","customer_id":"c1","merchant_id":"m1","amount":120.5,"event_ts":"2026-01-16T15:50:00","channel":"ECOM","country":"VN","status":"SUCCESS"}
{"order_id":"o2","customer_id":"c2","merchant_id":"m2","amount":42.0,"event_ts":"2026-01-16T15:51:00","channel":"POS","country":"VN","status":"FAILED"}
```

### 3) Verify đã ghi xuống Bronze chưa

Mở terminal khác:

```bash
find data/bronze_kafka/orders_raw -maxdepth 2 -type d | head
find data/bronze_kafka/orders_raw -name "*.parquet" | head -n 3
```

Đọc nhanh

```bash
python - <<'PY'
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
df = spark.read.parquet("data/bronze_kafka/orders_raw")
print("rows =", df.count())
df.orderBy("ingest_ts", ascending=False).show(10, truncate=False)
spark.stop()
PY
```

---

### Script test

```python
# file: spark/test/kafka_producer_orders.py
# pip install kafka-python

import argparse
import json
import random
import string
import time
from datetime import datetime, timedelta, timezone

from kafka import KafkaProducer


CHANNELS = ["ECOM", "POS", "ATM"]
COUNTRIES = ["VN", "TH", "MY", "SG"]
STATUSES = ["SUCCESS", "FAILED", "REVERSED"]


def rand_id(prefix: str, n: int = 6) -> str:
    return prefix + "".join(random.choices(string.digits, k=n))


def iso_ts(dt: datetime) -> str:
    # Kafka message đang dùng kiểu "2026-01-16T15:50:00"
    return dt.strftime("%Y-%m-%dT%H:%M:%S")


def make_event(order_id: str | None = None) -> dict:
    now = datetime.now(timezone.utc).astimezone()  # local tz
    # random event time trong 0..180 phút trước (để test late-ish)
    event_dt = now - timedelta(minutes=random.randint(0, 180))

    oid = order_id or rand_id("o", 7)
    return {
        "order_id": oid,
        "customer_id": rand_id("c", 5),
        "merchant_id": rand_id("m", 5),
        "amount": round(random.uniform(1.0, 5000.0), 2),
        "event_ts": iso_ts(event_dt),
        "channel": random.choice(CHANNELS),
        "country": random.choice(COUNTRIES),
        "status": random.choice(STATUSES),
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--brokers", default="localhost:9094", help="Kafka bootstrap servers")
    ap.add_argument("--topic", default="orders", help="Topic name")
    ap.add_argument("--rate", type=float, default=2.0, help="Messages per second")
    ap.add_argument("--seconds", type=int, default=60, help="How long to run")
    ap.add_argument("--dup_rate", type=float, default=0.2, help="Chance to resend an old order_id (0..1)")
    ap.add_argument("--seed", type=int, default=None, help="Random seed")
    args = ap.parse_args()

    if args.seed is not None:
        random.seed(args.seed)

    producer = KafkaProducer(
        bootstrap_servers=args.brokers.split(","),
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        acks="all",
        retries=10,
        linger_ms=20,
    )

    # giữ 1 pool order_id để bắn duplicate
    seen_order_ids: list[str] = []

    start = time.time()
    sent = 0
    interval = 1.0 / max(args.rate, 0.0001)

    try:
        while time.time() - start < args.seconds:
            use_dup = (seen_order_ids and random.random() < args.dup_rate)
            if use_dup:
                oid = random.choice(seen_order_ids)
                event = make_event(order_id=oid)
            else:
                event = make_event()
                seen_order_ids.append(event["order_id"])
                if len(seen_order_ids) > 2000:
                    seen_order_ids = seen_order_ids[-2000:]

            future = producer.send(args.topic, value=event)
            # chờ ack để log ổn định (có thể bỏ để max throughput)
            meta = future.get(timeout=10)

            sent += 1
            print(f"✅ sent #{sent} topic={meta.topic} partition={meta.partition} offset={meta.offset} -> {event}")

            time.sleep(interval)

    except KeyboardInterrupt:
        print("\n⛔ stopped by user")
    finally:
        producer.flush()
        producer.close()
        print(f"Done. total_sent={sent}")


if __name__ == "__main__":
    main()
```

---

### Cách test:

```bash
python spark/test/kafka_producer_orders.py \
  --brokers localhost:9094 \
  --topic orders \
  --rate 5 \
  --seconds 120 \
  --dup_rate 0.3
```
Or

```bash
python spark/test/kafka_producer_orders.py --brokers localhost:9094 --topic orders --rate 5 --seconds 120 --dup_rate 0.3
```

### Kiểm tra kafka có nhận được hay ko?

```bash
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 --topic orders --from-beginning
```

---

# Governance/Provenance (trực quan)

## Option 1 (Local lab): OpenLineage + Marquez

```yaml
version: '3.8'

services:
  postgres:
    image: postgres:13
    environment:
      POSTGRES_USER: marquez
      POSTGRES_PASSWORD: marquez
      POSTGRES_DB: marquez
    ports:
      - "5432:5432"

  marquez:
    image: marquezproject/marquez:latest
    depends_on:
      - postgres
    environment:
      MARQUEZ_DB_HOST: postgres
      MARQUEZ_DB_PORT: 5432
      MARQUEZ_DB_USER: marquez
      MARQUEZ_DB_PASSWORD: marquez
      MARQUEZ_DB_NAME: marquez
    ports:
      - "5000:5000"

  marquez-ui:
    image: marquezproject/marquez-ui:latest
    ports:
      - "3000:3000"
```

- Spark Structured Streaming có thể emit OpenLineage events qua listener.
- Ở lab, ta sẽ log lineage vào bảng của mình trước, sau đó có thể tích hợp OpenLineage đầy đủ hơn.

## Option 2 (GCP): Dataplex + Data Catalog + Lineage

- Storage: Google Cloud Storage (GCS)
- Processing: Dataproc / Dataproc Serverless
- Governance: Dataplex (metadata, policies, data quality)
- Catalog/Search: Data Catalog
- Lineage: Dataplex Lineage / Data Lineage API
- Quality: Dataplex DQ / Great Expectations


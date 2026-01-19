# 🚀 LAB 01 – Kafka → Spark Structured Streaming → Bronze (MinIO)

---

## 🎯 Mục tiêu
-	Consume Kafka topic orders_raw
-	Parse JSON → schema rõ ràng
-	Xử lý event time + watermark
-	Ghi Bronze layer (Parquet) lên MinIO (S3A)
-	Có checkpoint để restart an toàn
-	Sẵn sàng nối sang Silver

---

## 🧱 Kiến trúc lab

```code
Kafka (orders_raw)
        |
        v
Spark Structured Streaming
        |
        v
MinIO (s3a://lakehouse/bronze/orders/)
        |
   checkpoint (exactly-once)
```

---

## 1️⃣ Schema chuẩn (production mindset)

```python
from pyspark.sql.types import *

order_schema = StructType([
    StructField("order_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("merchant_id", StringType()),
    StructField("amount", DoubleType()),
    StructField("event_ts", StringType()),   # parse sau
    StructField("channel", StringType()),
    StructField("country", StringType()),
    StructField("status", StringType())
])
```

---

## 2️⃣ Code LAB – Bronze Streaming

#### 📁 `spark/lab/lab_stream_kafka_to_bronze_orders.py`

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = (
    SparkSession.builder
    .appName("orders-kafka-bronze")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# ===== Config =====
KAFKA_BOOTSTRAP = "localhost:9094"
TOPIC = "orders_raw"

BRONZE_PATH = "s3a://lakehouse/bronze/orders"
CHECKPOINT_PATH = "s3a://lakehouse/_checkpoints/orders_bronze"

# ===== Schema =====
order_schema = StructType([
    StructField("order_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("merchant_id", StringType()),
    StructField("amount", DoubleType()),
    StructField("event_ts", StringType()),
    StructField("channel", StringType()),
    StructField("country", StringType()),
    StructField("status", StringType())
])

# ===== Read Kafka =====
raw_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", TOPIC)
    .option("startingOffsets", "latest")
    .load()
)

# ===== Parse JSON =====
parsed_df = (
    raw_df
    .selectExpr("CAST(value AS STRING) AS json")
    .select(from_json(col("json"), order_schema).alias("data"))
    .select("data.*")
    .withColumn("event_time", to_timestamp("event_ts"))
    .withWatermark("event_time", "10 minutes")
)

# ===== Write Bronze =====
query = (
    parsed_df
    .writeStream
    .format("parquet")
    .outputMode("append")
    .option("path", BRONZE_PATH)
    .option("checkpointLocation", CHECKPOINT_PATH)
    .partitionBy("country", "channel")
    .trigger(processingTime="30 seconds")
    .start()
)

query.awaitTermination()
```

---

## 3️⃣ Chạy lab (đúng setup của bạn)

```bash
scripts/spark_submit.sh \
  spark/lab/lab_stream_kafka_to_bronze_orders.py
```

👉 KHÔNG cần thêm config gì nữa
(vì `spark_submit.sh` + `S3A` + `Kafka` đã OK)

---

## 4️⃣ Bắn data test (Kafka Producer)

Bạn đã có script random producer → dùng luôn:

```bash
python spark/test/kafka_producer_orders.py
```

#### Hoặc kiểm tra nhanh:

```bash
docker exec -it kafka sh -lc \
  '/opt/kafka/bin/kafka-console-producer.sh \
   --bootstrap-server localhost:9092 \
   --topic orders_raw'
```

Paste:

```code
{"order_id":"o999","customer_id":"c1","merchant_id":"m1","amount":120.5,"event_ts":"2026-01-19T13:30:00","channel":"ECOM","country":"VN","status":"SUCCESS"}
```

---

## 5️⃣ Verify kết quả (Bronze)

#### 🔎 Kiểm tra bằng Spark batch


```bash
scripts/spark_submit.sh - <<EOF
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
df = spark.read.parquet("s3a://lakehouse/bronze/orders")
df.show(5, truncate=False)
print("count =", df.count())
EOF
```

#### 📂 Trên MinIO UI

```code
lakehouse/
 └── bronze/
     └── orders/
         ├── country=VN/
         │   ├── channel=ECOM/
         │   └── channel=POS/
```

---

## 6️⃣ Vì sao lab này gần production

- ✔ Structured Streaming
- ✔ Event-time + watermark
- ✔ Exactly-once (checkpoint)
- ✔ Partition theo query pattern
- ✔ Bronze = append-only, raw but typed
- ✔ Dễ nối Silver

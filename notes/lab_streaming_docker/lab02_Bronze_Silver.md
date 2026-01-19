# LAB 02 – Bronze → Silver

-	Dedup theo order_id
-	Filter status = SUCCESS
-	Chuẩn hoá schema
-	Upsert (MERGE) bằng:
>+	Iceberg (chuẩn prod nhất) 🔥
>+	hoặc Delta (nếu muốn)

---

##  LAB SILVER (Bronze -> Silver)

### 🎯 Silver rules (đơn giản nhưng đúng production mindset)
	
**Đọc stream từ Bronze (Parquet on MinIO)**

***Chuẩn hoá:**
-	event_time = to_timestamp(event_ts)
-	amount cast chuẩn
-	Dedup theo order_id (giữ bản ghi mới nhất theo event_time)
-	Ghi Silver Parquet, partition theo event_date, country
-	Dùng foreachBatch để xử lý “micro-batch merge-lite”

---

## 📁 File: `spark/lab/lab_stream_bronze_to_silver_orders.py`

```python

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

spark = (
    SparkSession.builder
    .appName("orders-bronze-to-silver")
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

BRONZE_PATH = "s3a://lakehouse/bronze/orders"
SILVER_PATH = "s3a://lakehouse/silver/orders"
CHECKPOINT_PATH = "s3a://lakehouse/_checkpoints/orders_silver"

def upsert_like(batch_df, batch_id: int):
    if batch_df.rdd.isEmpty():
        return

    df = (
        batch_df
        .withColumn("event_time", to_timestamp("event_ts"))
        .withColumn("event_date", to_date(col("event_time")))
        .withColumn("amount", col("amount").cast("double"))
        .filter(col("order_id").isNotNull())
        .filter(col("event_time").isNotNull())
    )

    # keep latest per order_id in this micro-batch
    w = Window.partitionBy("order_id").orderBy(col("event_time").desc())
    df_latest = (
        df.withColumn("rn", row_number().over(w))
          .filter(col("rn") == 1)
          .drop("rn")
    )

    (
        df_latest
        .write
        .mode("overwrite")   # dynamic partition overwrite (only touched partitions)
        .format("parquet")
        .partitionBy("event_date", "country")
        .save(SILVER_PATH)
    )

bronze_stream = (
    spark.readStream
    .format("parquet")
    .load(BRONZE_PATH)
)

query = (
    bronze_stream.writeStream
    .foreachBatch(upsert_like)
    .option("checkpointLocation", CHECKPOINT_PATH)
    .trigger(processingTime="30 seconds")
    .start()
)

query.awaitTermination()
```

---

## ▶️ Chạy LAB SILVER

```bash
scripts/spark_submit.sh spark/lab/lab_stream_bronze_to_silver_orders.py
```

---

## ✅ Verify Silver nhanh

```bash
scripts/spark_inline.sh <<'PY'
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

df = spark.read.parquet("s3a://lakehouse/silver/orders")
df.orderBy("event_ts", ascending=False).show(10, truncate=False)
print("count =", df.count())
PY
```


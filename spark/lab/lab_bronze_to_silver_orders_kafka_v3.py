from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, current_timestamp, sha2, concat_ws, expr
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql.window import Window
import os

# Allow running both from host (localhost:9094) and from Docker network (kafka:9092)
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9094")
TOPIC = os.getenv("KAFKA_TOPIC", "orders_raw")

BRONZE_PATH = os.getenv("BRONZE_PATH", "data/bronze/orders_kafka")
SILVER_PATH = os.getenv("SILVER_PATH", "data/silver/orders_clean")
CKPT_BRONZE = os.getenv("CKPT_BRONZE", "checkpoints/orders_bronze")
CKPT_SILVER = os.getenv("CKPT_SILVER", "checkpoints/orders_silver")

schema = StructType([
    StructField("order_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("merchant_id", StringType()),
    StructField("amount", DoubleType()),
    StructField("event_ts", StringType()),
    StructField("channel", StringType()),
    StructField("country", StringType()),
    StructField("status", StringType()),
])

spark = (SparkSession.builder
         .appName("orders-kafka-bronze-silver")
         # local lab
         .master("local[*]")
         .getOrCreate())

spark.sparkContext.setLogLevel("WARN")

raw = (spark.readStream
       .format("kafka")
       .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
       .option("subscribe", TOPIC)
       .option("startingOffsets", "latest")
       .load())

# Parse JSON
parsed = (raw
          .selectExpr("CAST(value AS STRING) as value", "timestamp as kafka_ts")
          .withColumn("json", from_json(col("value"), schema))
          .select("json.*", "value", "kafka_ts")
          # Handles strings like 2026-01-19T03:47:10.646442Z (UTC)
          .withColumn(
              "event_time",
              to_timestamp(col("event_ts"), "yyyy-MM-dd'T'HH:mm:ss[.SSSSSS]'Z'")
          )
          .withColumn("ingest_ts", current_timestamp())
          .withColumn("event_date", expr("to_date(event_time)"))
          .withColumn("event_hash", sha2(concat_ws("||", col("order_id"), col("event_ts"), col("status")), 256))
          )

# Bronze: lưu full record (append)
bronze_q = (parsed.writeStream
            .format("parquet")
            .option("path", BRONZE_PATH)
            .option("checkpointLocation", CKPT_BRONZE)
            .partitionBy("event_date")
            .outputMode("append")
            .start())

# Silver: clean + de-dup (keep latest per order_id)
clean = (
    parsed
    .filter(col("order_id").isNotNull() & col("event_time").isNotNull())
    .withWatermark("event_time", "10 minutes")
    # keep latest record per order_id in watermark window
    .withColumn(
        "rn",
        expr("row_number() over (partition by order_id order by event_time desc, kafka_ts desc)")
    )
    .filter(col("rn") == 1)
    .drop("rn")
    .select(
        "order_id",
        "customer_id",
        "merchant_id",
        "amount",
        "event_time",
        "channel",
        "country",
        "status",
        "kafka_ts",
        "ingest_ts",
        "event_date",
        "event_hash",
        "value",
    )
)

silver_q = (clean.writeStream
            .format("parquet")
            .option("path", SILVER_PATH)
            .option("checkpointLocation", CKPT_SILVER)
            .partitionBy("event_date")
            .outputMode("append")
            .start())

spark.streams.awaitAnyTermination()
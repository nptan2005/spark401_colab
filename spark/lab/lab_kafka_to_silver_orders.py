from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, to_date, current_timestamp,
    expr, row_number
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from pyspark.sql.window import Window

KAFKA_BOOTSTRAP = "localhost:9094"   # host app -> EXTERNAL listener
TOPIC = "orders_raw"
CHECKPOINT = "checkpoints/kafka_to_silver_orders"
SILVER_OUT = "data/silver/orders_fact_dt_kafka"

schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("merchant_id", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("event_ts", StringType(), True),   # nhận string ISO
    StructField("channel", StringType(), True),
    StructField("country", StringType(), True),
    StructField("status", StringType(), True),
])

spark = (
    SparkSession.builder
    .appName("lab_kafka_to_silver_orders")
    .config("spark.sql.shuffle.partitions", "50")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", TOPIC)
    .option("startingOffsets", "earliest")
    .load()
)

# Kafka value là bytes -> cast string -> parse JSON
parsed = (
    raw.selectExpr("CAST(value AS STRING) AS json_str")
       .select(from_json(col("json_str"), schema).alias("j"))
       .select("j.*")
       .withColumn("event_ts", to_timestamp("event_ts"))     # string -> timestamp
       .withColumn("ingest_ts", current_timestamp())         # ingestion time
       .withColumn("dt", to_date(col("event_ts")))           # partition key
)

# Watermark + dedup (realistic streaming)
# Dedup theo order_id trong cửa sổ 3 ngày
dedup = (
    parsed
    .withWatermark("event_ts", "3 days")
    .dropDuplicates(["order_id"])
)

# Write Silver
q = (
    dedup.writeStream
    .format("parquet")
    .option("path", SILVER_OUT)
    .option("checkpointLocation", CHECKPOINT)
    .outputMode("append")
    .partitionBy("dt")
    .trigger(processingTime="5 seconds")
    .start()
)

print("🚀 Streaming started. Silver =", SILVER_OUT)
q.awaitTermination()
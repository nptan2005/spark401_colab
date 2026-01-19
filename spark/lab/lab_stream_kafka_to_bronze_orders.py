import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = (
    SparkSession.builder
    .appName("orders-kafka-bronze")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# ===== Config (FROM ENV) =====
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9094")
TOPIC = os.getenv("KAFKA_TOPIC_ORDERS", "orders_raw")

BRONZE_PATH = "s3a://lakehouse/bronze/orders"
CHECKPOINT_PATH = "s3a://lakehouse/_checkpoints/orders_bronze"

print(f"[config] kafka={KAFKA_BOOTSTRAP}, topic={TOPIC}")

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
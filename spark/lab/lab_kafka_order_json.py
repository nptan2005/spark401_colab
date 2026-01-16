from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_date, current_timestamp
from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType

# NOTE:
# Your error `Failed to find data source: kafka` means Spark is missing the Kafka connector JAR.
# We load it via spark.jars.packages (Maven coordinates).
# Spark 4.x uses Scala 2.13 => artifact suffix _2.13.
KAFKA_CONNECTOR = "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1"
# Helpful in some environments; usually brought transitively, but safe to add:
KAFKA_CLIENTS = "org.apache.kafka:kafka-clients:3.7.2"

spark = (
    SparkSession.builder
    .appName("lab_kafka_to_bronze_orders")
    # Load Kafka source/sink connector
    .config("spark.jars.packages", f"{KAFKA_CONNECTOR},{KAFKA_CLIENTS}")
    # Local dev defaults
    .config("spark.sql.shuffle.partitions", "50")
    .getOrCreate()
)

# Reduce noise while you test
spark.sparkContext.setLogLevel("WARN")

schema = (
    StructType()
    .add("order_id", StringType())
    .add("customer_id", StringType())
    .add("merchant_id", StringType())
    .add("amount", DoubleType())
    .add("event_ts", TimestampType())
    .add("channel", StringType())
    .add("country", StringType())
    .add("status", StringType())
)

# Host apps (PySpark on macOS) should use the EXTERNAL listener: localhost:9094
BOOTSTRAP = "localhost:9094"
TOPIC = "orders"

kafka = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", BOOTSTRAP)
    .option("subscribe", TOPIC)
    .option("startingOffsets", "latest")
    # small lab safety
    .option("failOnDataLoss", "false")
    .load()
)

parsed = (
    kafka
    .selectExpr(
        "CAST(key AS STRING) as k",
        "CAST(value AS STRING) as json",
        "timestamp as kafka_ts"
    )
    .withColumn("ingest_ts", current_timestamp())
    .select(from_json(col("json"), schema).alias("o"), "k", "kafka_ts", "ingest_ts")
    .select("o.*", "k", "kafka_ts", "ingest_ts")
    .withColumn("dt", to_date(col("event_ts")))
)

query = (
    parsed.writeStream
    .format("parquet")
    .option("path", "data/bronze_kafka/orders_raw")
    .option("checkpointLocation", "checkpoints/bronze_kafka_orders")
    .partitionBy("dt")
    .outputMode("append")
    .start()
)

print("✅ Streaming started: Kafka -> data/bronze_kafka/orders_raw")
print("   - bootstrap:", BOOTSTRAP)
print("   - topic:", TOPIC)

query.awaitTermination()
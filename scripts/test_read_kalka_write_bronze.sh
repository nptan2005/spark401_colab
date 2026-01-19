spark_inline.sh <<'PY'
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

spark = SparkSession.builder.getOrCreate()

bootstrap = "localhost:9092"      # TODO: đổi theo kafka của bạn
topic = "orders"                  # TODO: đổi topic

schema = StructType([
    StructField("order_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("merchant_id", StringType()),
    StructField("amount", DoubleType()),
    StructField("event_ts", TimestampType()),
    StructField("status", StringType()),
    StructField("country", StringType()),
    StructField("channel", StringType()),
])

raw = (spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", bootstrap)
    .option("subscribe", topic)
    .option("startingOffsets", "latest")
    .load())

parsed = (raw
    .selectExpr("CAST(value AS STRING) AS json_str", "timestamp AS kafka_ts")
    .select(from_json(col("json_str"), schema).alias("data"), col("kafka_ts"))
    .select("data.*", "kafka_ts"))

out_path = "s3a://lakehouse/bronze/orders"
ckpt = "s3a://lakehouse/_checkpoints/orders_bronze"

q = (parsed.writeStream
    .format("parquet")
    .option("path", out_path)
    .option("checkpointLocation", ckpt)
    .outputMode("append")
    .trigger(processingTime="10 seconds")
    .start())

print("Streaming query started. id=", q.id)
q.awaitTermination()
PY
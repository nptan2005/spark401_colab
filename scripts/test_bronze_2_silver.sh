scripts/spark_inline.sh <<'PY'
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
spark = SparkSession.builder.getOrCreate()

bronze = "s3a://lakehouse/bronze/orders"
silver = "s3a://lakehouse/silver/orders"
ckpt  = "s3a://lakehouse/_checkpoints/orders_silver"

df = spark.readStream.parquet(bronze)

# ví dụ dedup trong watermark window (tùy bài)
df2 = (df
    .withWatermark("event_ts", "30 minutes")
    .dropDuplicates(["order_id"])
)

q = (df2.writeStream
    .format("parquet")
    .option("path", silver)
    .option("checkpointLocation", ckpt)
    .outputMode("append")
    .trigger(processingTime="10 seconds")
    .start()
)

print("Silver stream started. id=", q.id)
q.awaitTermination()
PY
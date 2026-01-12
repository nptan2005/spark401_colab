from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = (SparkSession.builder
  .appName("lab2f_join_broadcast_default")
  .config("spark.sql.shuffle.partitions", "50")
  .getOrCreate())

orders = spark.read.parquet("data/silver/orders_enriched").select(
    "order_id","customer_id","amount","order_ts","country","channel","status"
)

customers = spark.read.parquet("data/silver/customers").select(
    "customer_id","segment","risk_tier","created_date"
)

# Join + 1 action để Spark thật sự chạy
df = (orders.join(customers, on="customer_id", how="left")
      .groupBy("segment").count())

df.explain("formatted")
df.show(20, False)

spark.stop()
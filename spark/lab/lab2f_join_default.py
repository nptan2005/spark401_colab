from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = (SparkSession.builder
  .appName("lab2f_join_default")
  .config("spark.sql.shuffle.partitions", "50")
  .config("spark.sql.adaptive.enabled", "true")
  .getOrCreate())

orders = spark.read.parquet("data/silver/orders")
customers = spark.read.parquet("data/silver/customers")

# Join + 1 aggregation nhẹ để plan rõ hơn
q = (orders
     .join(customers, "customer_id", "left")
     .groupBy("segment")
     .count())

q.explain("formatted")
q.show(10, False)

spark.stop()
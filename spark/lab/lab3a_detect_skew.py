from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc

ORDERS_PATH = "data/silver/orders"  # hoặc orders_enriched, tùy bạn muốn

spark = (
    SparkSession.builder
    .appName("lab3a_detect_skew")
    .config("spark.sql.shuffle.partitions", "50")
    .getOrCreate()
)

orders = spark.read.parquet(ORDERS_PATH)

# xem top keys để confirm skew
top = (orders.groupBy("customer_id")
            .count()
            .orderBy(desc("count"))
            .limit(20))

top.show(20, truncate=False)

# xem plan để thấy shuffle cho groupBy
top.explain("formatted")

spark.stop()
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count as _count

ORDERS_PATH = "data/silver_lab32/orders_fact_dt"

spark = (
    SparkSession.builder
    .appName("lab3_2_check_skew")
    .config("spark.sql.shuffle.partitions", "50")
    .getOrCreate()
)

df = spark.read.parquet(ORDERS_PATH).select("customer_id")

# Đếm top keys để thấy skew
res = (df.groupBy("customer_id")
         .agg(_count("*").alias("cnt"))
         .orderBy(col("cnt").desc())
         .limit(20))

res.explain("formatted")
res.show(truncate=False)

spark.stop()
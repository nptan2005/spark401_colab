from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, floor, rand, count as _count

ORDERS_PATH = "data/silver/orders"
CUSTOMERS_PATH = "data/silver/customers"

SALT_N = 16

spark = (
    SparkSession.builder
    .appName("lab3c_join_skew_salt")
    .config("spark.sql.shuffle.partitions", "50")
    .config("spark.sql.autoBroadcastJoinThreshold", "-1")  # ép shuffle
    .getOrCreate()
)

o = spark.read.parquet(ORDERS_PATH)
c = spark.read.parquet(CUSTOMERS_PATH).select(
    col("customer_id").alias("c_customer_id"),
    col("segment").alias("c_segment")
)

# 1) Salt orders
o_s = o.withColumn("salt", floor(rand(7) * SALT_N).cast("int")).alias("o_s")

# 2) Expand customers
c_exp = c.withColumn("salt", expr(f"explode(sequence(0, {SALT_N-1}))")).alias("c_exp")

# 3) Join on (customer_id, salt)
j = o_s.join(
    c_exp,
    (col("o_s.customer_id") == col("c_exp.c_customer_id")) &
    (col("o_s.salt") == col("c_exp.salt")),
    "left"
)

res = (j.groupBy("c_segment")
         .agg(_count("*").alias("txns")))

res.explain("formatted")
res.show()

spark.stop()
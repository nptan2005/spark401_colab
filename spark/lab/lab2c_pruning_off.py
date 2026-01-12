from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, col

spark = SparkSession.builder.appName("lab2c_pruning_off").getOrCreate()
df = spark.read.parquet("data/silver_p/orders")

q2 = (
    df.filter(to_date(col("order_ts")) == "2026-01-10")
      .groupBy("country")
      .count()
)

q2.explain("formatted")
q2.show(20, False)

spark.stop()

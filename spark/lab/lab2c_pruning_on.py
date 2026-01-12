from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("lab2c_pruning_on").getOrCreate()
df = spark.read.parquet("data/silver_p/orders")

q1 = (
    df.filter("dt = '2026-01-10'")
      .groupBy("country")
      .count()
)

q1.explain("formatted")
q1.show(20, False)

spark.stop()
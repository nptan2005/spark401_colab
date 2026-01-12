from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, col

spark = SparkSession.builder.appName("lab2d_repartition").getOrCreate()

df = spark.read.parquet("data/silver/orders_enriched") \
          .withColumn("dt", to_date(col("order_ts")))

# 1) partitionBy dt nhưng sẽ ra N files mỗi dt (do partitions)
df.write.mode("overwrite").partitionBy("dt").parquet("data/tmp_p1")

# 2) ép mỗi dt ~ ít file hơn: repartition theo dt trước khi write
(df.repartition("dt")
   .write.mode("overwrite")
   .partitionBy("dt")
   .parquet("data/tmp_p2"))

df.show()

spark.stop()
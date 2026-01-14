from pyspark.sql import SparkSession

spark = (SparkSession.builder
  .appName("test-config")
  .config("spark.sql.shuffle.partitions", "50")
  .getOrCreate())

print("autoBroadcastJoinThreshold =", spark.conf.get("spark.sql.autoBroadcastJoinThreshold"))
print("adaptive.enabled =", spark.conf.get("spark.sql.adaptive.enabled"))
print("shuffle.partitions =", spark.conf.get("spark.sql.shuffle.partitions"))

spark.stop()
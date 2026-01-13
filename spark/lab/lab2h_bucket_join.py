from pyspark.sql import SparkSession

spark = (SparkSession.builder
    .appName("lab2h_bucket_join")
    .config("spark.sql.autoBroadcastJoinThreshold", "-1") # Tắt broadcast để ép dùng Join lớn
    .enableHiveSupport()
    .getOrCreate())

# Đọc từ Table (không phải từ file trực tiếp) để lấy metadata về bucket
df_orders = spark.table("bucketed_orders")
df_customers = spark.table("bucketed_customers")

# Thực hiện Join
result = df_orders.join(df_customers, "customer_id", "left")

print("=== EXPLAIN (BUCKET JOIN) ===")
result.explain("formatted")

result.show(5)
spark.stop()
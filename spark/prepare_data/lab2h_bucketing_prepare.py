from pyspark.sql import SparkSession

spark = (SparkSession.builder
    .appName("lab2h_bucketing_prepare")
    .enableHiveSupport() # Cần thiết để quản lý Metadata của Buckets
    .getOrCreate())

# 1. Đọc dữ liệu Silver hiện có
orders = spark.read.parquet("data/silver/orders")
customers = spark.read.parquet("data/silver/customers")

# 2. Ghi bảng Orders thành 16 buckets (chia túi theo customer_id)
# Lưu ý: Phải dùng saveAsTable để Spark lưu thông tin bucket vào metastore
(orders.write
    .mode("overwrite")
    .bucketBy(16, "customer_id")
    .sortBy("customer_id")
    .saveAsTable("bucketed_orders"))

# 3. Ghi bảng Customers thành 16 buckets (bắt buộc số lượng bucket phải bằng hoặc là bội số của nhau)
(customers.write
    .mode("overwrite")
    .bucketBy(16, "customer_id")
    .sortBy("customer_id")
    .saveAsTable("bucketed_customers"))

print("✔ Đã lưu xong 2 bảng dưới dạng Bucketed Tables")
spark.stop()
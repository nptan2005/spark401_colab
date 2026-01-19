#!/usr/bin/env bash
set -euo pipefail

# ===== ENV =====
export SPARK_HOME=/Users/nptan2005/SourceCode/Python/spark-4.0.1-bin-hadoop3
export PYSPARK_PYTHON=/opt/homebrew/anaconda3/envs/cdp_env/bin/python
export PYSPARK_DRIVER_PYTHON=/opt/homebrew/anaconda3/envs/cdp_env/bin/python

KAFKA_BOOTSTRAP=localhost:9094
S3A_ENDPOINT=http://127.0.0.1:9000
S3A_ACCESS_KEY=minioadmin
S3A_SECRET_KEY=minioadmin123

APP=${1:-spark/test/minio_smoke_test.py}

# ===== SUBMIT =====
$SPARK_HOME/bin/spark-submit \
  --name spark401-lab \
  --master local[*] \
  --packages \
org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1,\
org.apache.spark:spark-token-provider-kafka-0-10_2.13:4.0.1,\
org.apache.hadoop:hadoop-aws:3.4.1,\
com.amazonaws:aws-java-sdk-bundle:1.12.782 \
  --conf spark.sql.shuffle.partitions=8 \
  --conf spark.hadoop.fs.s3a.endpoint=$S3A_ENDPOINT \
  --conf spark.hadoop.fs.s3a.access.key=$S3A_ACCESS_KEY \
  --conf spark.hadoop.fs.s3a.secret.key=$S3A_SECRET_KEY \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  $APP
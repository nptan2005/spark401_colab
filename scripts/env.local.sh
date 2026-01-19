#!/usr/bin/env bash
set -euo pipefail

# Project root (works in bash/zsh; prefers git root)
if command -v git >/dev/null 2>&1 && git rev-parse --show-toplevel >/dev/null 2>&1; then
  REPO_ROOT="$(git rev-parse --show-toplevel)"
else
  SCRIPT_SOURCE="${BASH_SOURCE[0]-$0}"
  REPO_ROOT="$(cd "$(dirname "$SCRIPT_SOURCE")/.." && pwd)"
fi
export REPO_ROOT

# ---- Spark ----
export SPARK_HOME="${SPARK_HOME:-$HOME/SourceCode/Python/spark-4.0.1-bin-hadoop3}"

# ---- Python (prefer activated env) ----
ACTIVE_PY="$(command -v python 2>/dev/null || true)"
DEFAULT_PY="$HOME/opt/homebrew/anaconda3/envs/cdp_env/bin/python"
if [[ -n "${ACTIVE_PY}" ]]; then
  DEFAULT_PY="$ACTIVE_PY"
fi

export PYSPARK_PYTHON="${PYSPARK_PYTHON:-$DEFAULT_PY}"
export PYSPARK_DRIVER_PYTHON="${PYSPARK_DRIVER_PYTHON:-$PYSPARK_PYTHON}"

# Guard: notebooks sometimes set PYSPARK_DRIVER_PYTHON=jupyter -> breaks spark-submit
if [[ "$PYSPARK_DRIVER_PYTHON" == "jupyter"* ]]; then
  export PYSPARK_DRIVER_PYTHON="$PYSPARK_PYTHON"
fi

# ---- Stack endpoints ----
export KAFKA_BOOTSTRAP="${KAFKA_BOOTSTRAP:-localhost:9094}"
export MINIO_ENDPOINT="${MINIO_ENDPOINT:-http://127.0.0.1:9000}"
export MINIO_ACCESS_KEY="${MINIO_ACCESS_KEY:-minioadmin}"
export MINIO_SECRET_KEY="${MINIO_SECRET_KEY:-minioadmin123}"
# Kafka topics
export KAFKA_TOPIC_ORDERS="orders_raw"

# ---- Ivy cache pinned to repo ----
export IVY_HOME="${IVY_HOME:-$REPO_ROOT/.cache/ivy}"

# ---- Maven coords ----
export SPARK_KAFKA_PKGS="${SPARK_KAFKA_PKGS:-org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1,org.apache.spark:spark-token-provider-kafka-0-10_2.13:4.0.1}"
export SPARK_AWS_PKGS="${SPARK_AWS_PKGS:-org.apache.hadoop:hadoop-aws:3.4.1,com.amazonaws:aws-java-sdk-bundle:1.12.782}"
export SPARK_PKGS="${SPARK_PKGS:-$SPARK_KAFKA_PKGS,$SPARK_AWS_PKGS}"

# ---- S3A (MinIO) opts ----
export S3A_CONF_OPTS="--conf spark.hadoop.fs.s3a.endpoint=${MINIO_ENDPOINT} \
  --conf spark.hadoop.fs.s3a.access.key=${MINIO_ACCESS_KEY} \
  --conf spark.hadoop.fs.s3a.secret.key=${MINIO_SECRET_KEY} \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem"

# ---- common opts ----
export SPARK_SUBMIT_COMMON_OPTS="--conf spark.sql.session.timeZone=Asia/Ho_Chi_Minh \
  --conf spark.eventLog.enabled=true \
  --conf spark.eventLog.dir=$REPO_ROOT/logs/spark-events \
  --conf spark.jars.ivy=$IVY_HOME \
  --conf spark.jars.repositories=https://repo.maven.apache.org/maven2,https://repos.spark-packages.org,https://maven-central.storage-download.googleapis.com/maven2"
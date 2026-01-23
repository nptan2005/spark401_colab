#!/usr/bin/env bash
# NOTE: source file -> do NOT set -u
set -eo pipefail

# Project root
if command -v git >/dev/null 2>&1 && git rev-parse --show-toplevel >/dev/null 2>&1; then
  REPO_ROOT="$(git rev-parse --show-toplevel)"
else
  _SRC="${BASH_SOURCE[0]-$0}"
  REPO_ROOT="$(cd "$(dirname "$_SRC")/.." && pwd)"
fi
export REPO_ROOT

# ---- Spark ----
export SPARK_HOME="${SPARK_HOME:-$HOME/SourceCode/Python/spark-4.0.1-bin-hadoop3}"

# ---- Python for Spark ----
if [[ -n "${CONDA_PREFIX:-}" && -x "${CONDA_PREFIX}/bin/python" ]]; then
  _PY="${CONDA_PREFIX}/bin/python"
else
  _PY="/opt/homebrew/anaconda3/envs/cdp_env/bin/python"
fi
export PYSPARK_PYTHON="${PYSPARK_PYTHON:-$_PY}"
export PYSPARK_DRIVER_PYTHON="${PYSPARK_DRIVER_PYTHON:-$PYSPARK_PYTHON}"

# Guard: notebooks sometimes set jupyter
if [[ "${PYSPARK_DRIVER_PYTHON}" == jupyter* ]]; then
  export PYSPARK_DRIVER_PYTHON="$PYSPARK_PYTHON"
fi

# ---- Stack endpoints ----
export KAFKA_BOOTSTRAP="${KAFKA_BOOTSTRAP:-localhost:9094}"
export MINIO_ENDPOINT="${MINIO_ENDPOINT:-http://127.0.0.1:9000}"
export MINIO_ACCESS_KEY="${MINIO_ACCESS_KEY:-minioadmin}"
export MINIO_SECRET_KEY="${MINIO_SECRET_KEY:-minioadmin123}"
export KAFKA_TOPIC_ORDERS="${KAFKA_TOPIC_ORDERS:-orders_raw}"

# ---- Marquez / OpenLineage (Airflow-level) ----
export OPENLINEAGE_URL="${OPENLINEAGE_URL:-http://localhost:5001/api/v1/lineage}"
export OPENLINEAGE_NAMESPACE="${OPENLINEAGE_NAMESPACE:-cpd}"

# Spark OpenLineage agent: OFF for Spark 4 (prevents crash)
export ENABLE_OPENLINEAGE="${ENABLE_OPENLINEAGE:-0}"

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
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  --conf spark.hadoop.fs.s3a.connection.timeout=60000 \
  --conf spark.hadoop.fs.s3a.connection.establish.timeout=60000 \
  --conf spark.hadoop.fs.s3a.attempts.maximum=20"

# ---- common opts ----
mkdir -p "$REPO_ROOT/logs/spark-events" "$IVY_HOME/cache" "$IVY_HOME/jars" >/dev/null 2>&1 || true
export SPARK_SUBMIT_COMMON_OPTS="--conf spark.sql.session.timeZone=Asia/Ho_Chi_Minh \
  --conf spark.eventLog.enabled=true \
  --conf spark.eventLog.dir=$REPO_ROOT/logs/spark-events \
  --conf spark.jars.ivy=$IVY_HOME \
  --conf spark.jars.repositories=https://repo.maven.apache.org/maven2,https://repos.spark-packages.org,https://maven-central.storage-download.googleapis.com/maven2"
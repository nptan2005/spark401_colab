#!/usr/bin/env bash
set -euo pipefail

export SPARK_HOME="${SPARK_HOME:-~/SourceCode/Python/spark-4.0.1-bin-hadoop3}"

# Force python driver/executor (tránh dính jupyter từ môi trường global)
if [[ -n "${CONDA_PREFIX:-}" && -x "${CONDA_PREFIX}/bin/python" ]]; then
  export PYSPARK_PYTHON="${CONDA_PREFIX}/bin/python"
  export PYSPARK_DRIVER_PYTHON="${CONDA_PREFIX}/bin/python"
else
  export PYSPARK_PYTHON="$(command -v python3)"
  export PYSPARK_DRIVER_PYTHON="$(command -v python3)"
fi
unset PYSPARK_DRIVER_PYTHON_OPTS || true

# Host chạy Spark -> dùng Kafka port public 9094 (bạn đang set đúng)
export KAFKA_BOOTSTRAP="${KAFKA_BOOTSTRAP:-localhost:9094}"

export SPARK_SUBMIT_COMMON_OPTS="${SPARK_SUBMIT_COMMON_OPTS:---master local[*] --conf spark.sql.shuffle.partitions=4}"


export MINIO_ENDPOINT="http://localhost:9000"
export MINIO_ACCESS_KEY="minioadmin"
export MINIO_SECRET_KEY="minioadmin123"
export S3A_BUCKET="lakehouse"
export S3A_BASE="s3a://${S3A_BUCKET}"
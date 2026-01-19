#!/usr/bin/env bash
set -euo pipefail

if command -v git >/dev/null 2>&1 && git rev-parse --show-toplevel >/dev/null 2>&1; then
  REPO_ROOT="$(git rev-parse --show-toplevel)"
else
  SCRIPT_SOURCE="${BASH_SOURCE[0]-$0}"
  REPO_ROOT="$(cd "$(dirname "$SCRIPT_SOURCE")/.." && pwd)"
fi

# load env
source "$REPO_ROOT/scripts/env.local.sh"

IVY_HOME="${IVY_HOME:-$REPO_ROOT/.cache/ivy}"
mkdir -p "$IVY_HOME/cache" "$IVY_HOME/jars" "$REPO_ROOT/logs/spark-events"

if [[ "${1-}" == "--warmup" ]]; then
  echo "[warmup] ivy=${IVY_HOME/#$HOME/~}"
  EXAMPLES_JAR="$SPARK_HOME/examples/jars/spark-examples_2.13-4.0.1.jar"
  "$SPARK_HOME/bin/spark-submit" \
    --master local[1] \
    --class org.apache.spark.examples.SparkPi \
    --packages "$SPARK_PKGS" \
    --conf "spark.jars.ivy=$IVY_HOME" \
    $S3A_CONF_OPTS \
    $SPARK_SUBMIT_COMMON_OPTS \
    "$EXAMPLES_JAR" 5
  echo "[warmup] done"
  exit 0
fi

APP="${1:-}"
shift || true

# force python (avoid jupyter)
if [[ "$PYSPARK_DRIVER_PYTHON" == "jupyter"* ]]; then
  export PYSPARK_DRIVER_PYTHON="$PYSPARK_PYTHON"
fi
export PYSPARK_PYTHON PYSPARK_DRIVER_PYTHON

"$SPARK_HOME/bin/spark-submit" \
  --master local[*] \
  --packages "$SPARK_PKGS" \
  --conf "spark.jars.ivy=$IVY_HOME" \
  $S3A_CONF_OPTS \
  $SPARK_SUBMIT_COMMON_OPTS \
  "$APP" "$@"
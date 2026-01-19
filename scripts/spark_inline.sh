#!/usr/bin/env bash
set -euo pipefail

# repo root (zsh-safe)
if command -v git >/dev/null 2>&1 && git rev-parse --show-toplevel >/dev/null 2>&1; then
  REPO_ROOT="$(git rev-parse --show-toplevel)"
else
  SCRIPT_SOURCE="${BASH_SOURCE[0]-$0}"
  REPO_ROOT="$(cd "$(dirname "$SCRIPT_SOURCE")/.." && pwd)"
fi

# load env
if [[ -f "$REPO_ROOT/scripts/env.local.sh" ]]; then
  # shellcheck disable=SC1091
  source "$REPO_ROOT/scripts/env.local.sh"
fi

: "${SPARK_HOME:?SPARK_HOME not set}"
: "${SPARK_PKGS:?SPARK_PKGS not set}"
: "${PYSPARK_PYTHON:?PYSPARK_PYTHON not set}"
: "${PYSPARK_DRIVER_PYTHON:?PYSPARK_DRIVER_PYTHON not set}"

# force python (avoid jupyter)
if [[ "$PYSPARK_DRIVER_PYTHON" == "jupyter"* ]]; then
  PYSPARK_DRIVER_PYTHON="$PYSPARK_PYTHON"
fi
export PYSPARK_PYTHON PYSPARK_DRIVER_PYTHON

IVY_HOME="${IVY_HOME:-$REPO_ROOT/.cache/ivy}"
mkdir -p "$IVY_HOME/cache" "$IVY_HOME/jars" "$REPO_ROOT/logs/spark-events"

TMP="$(mktemp -t spark_inline_XXXXXX).py"
cat > "$TMP"

echo "[env] PYSPARK_DRIVER_PYTHON=${PYSPARK_DRIVER_PYTHON/#$HOME/~}"
echo "[env] PYSPARK_PYTHON=${PYSPARK_PYTHON/#$HOME/~}"
echo "[inline] tmp=${TMP/#$HOME/~}"

"$SPARK_HOME/bin/spark-submit" \
  --master local[*] \
  --packages "$SPARK_PKGS" \
  --conf "spark.jars.ivy=$IVY_HOME" \
  $S3A_CONF_OPTS \
  $SPARK_SUBMIT_COMMON_OPTS \
  "$TMP"

rc=$?
rm -f "$TMP"
exit $rc
#!/usr/bin/env bash
set -euo pipefail

# Repo root
if command -v git >/dev/null 2>&1 && git rev-parse --show-toplevel >/dev/null 2>&1; then
  REPO_ROOT="$(git rev-parse --show-toplevel)"
else
  _SRC="${BASH_SOURCE[0]-$0}"
  REPO_ROOT="$(cd "$(dirname "$_SRC")/.." && pwd)"
fi

# shellcheck disable=SC1091
source "$REPO_ROOT/scripts/env.local.sh"

APP="${1:-}"
if [[ -z "$APP" ]]; then
  echo "Usage: scripts/spark_submit.sh <python_job.py> [args...]" >&2
  exit 2
fi
shift || true

if [[ "${PYSPARK_DRIVER_PYTHON:-}" == jupyter* ]]; then
  export PYSPARK_DRIVER_PYTHON="$PYSPARK_PYTHON"
fi

SUBMIT=(
  "$SPARK_HOME/bin/spark-submit"
  --master "local[*]"
  --packages "$SPARK_PKGS"
)

# shellcheck disable=SC2206
SUBMIT+=( $SPARK_SUBMIT_COMMON_OPTS )
# shellcheck disable=SC2206
SUBMIT+=( $S3A_CONF_OPTS )

# Spark OL agent OFF (Spark4 crash). Lineage dùng Airflow -> Marquez.
if [[ "${ENABLE_OPENLINEAGE:-0}" == "1" ]]; then
  echo "[spark_submit] ENABLE_OPENLINEAGE=1 requested but Spark 4 agent is not safe. Keeping it OFF." >&2
fi

exec "${SUBMIT[@]}" "$APP" "$@"
#!/usr/bin/env bash
set -euo pipefail

if command -v git >/dev/null 2>&1 && git rev-parse --show-toplevel >/dev/null 2>&1; then
  REPO_ROOT="$(git rev-parse --show-toplevel)"
else
  _SRC="${BASH_SOURCE[0]-$0}"
  REPO_ROOT="$(cd "$(dirname "$_SRC")/.." && pwd)"
fi

tmp="$(mktemp -t spark_inline_XXXXXX.py)"
echo "[inline] tmp=$tmp"
cat > "$tmp"

"$REPO_ROOT/scripts/spark_submit.sh" "$tmp"

RC=$?
rm -f "$TMP"
exit $RC
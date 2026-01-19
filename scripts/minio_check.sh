#!/usr/bin/env bash
set -euo pipefail

echo "== MinIO containers =="
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}" | grep -E "minio|NAMES" || true

echo
echo "== Health =="
curl -fsS http://localhost:9000/minio/health/live >/dev/null && echo "[OK] minio live" || echo "[ERR] minio not live"

echo
echo "== Buckets =="
docker exec -it minio-init sh -lc "mc ls local/lakehouse || true"
#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR/docker/minio"

docker compose up -d
echo "[OK] MinIO up. Console: http://localhost:9001  S3: http://localhost:9000"
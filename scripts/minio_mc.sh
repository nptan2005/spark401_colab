#!/usr/bin/env bash
set -euo pipefail

MINIO_ALIAS="${MINIO_ALIAS:-local}"
MINIO_ENDPOINT="${MINIO_ENDPOINT:-http://minio:9000}"
MINIO_USER="${MINIO_USER:-minioadmin}"
MINIO_PASS="${MINIO_PASS:-minioadmin123}"

docker run --rm --network "$(docker network ls --format '{{.Name}}' | grep -E '^spark401_colab|^docker_default|default$' | head -n1)" \
  minio/mc:latest sh -lc "
    mc alias set $MINIO_ALIAS $MINIO_ENDPOINT $MINIO_USER $MINIO_PASS >/dev/null
    mc ls $MINIO_ALIAS
  "
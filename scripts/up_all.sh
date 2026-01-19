#!/usr/bin/env bash
set -euo pipefail
( cd docker/kafka   && docker compose up -d )
( cd docker/marquez && docker compose up -d )
( cd docker/airflow && docker compose up -d )
echo "[OK] all stacks up"
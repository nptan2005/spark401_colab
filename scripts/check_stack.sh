#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/env.local.sh"

echo "== Docker containers =="
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}" | egrep "kafka|kafka-ui|marquez|airflow" || true
echo

echo "== Kafka broker api versions (in-container) =="
docker exec -it kafka sh -lc '/opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092 | head -n 5'
echo

echo "== Kafka topics =="
docker exec -it kafka sh -lc '/opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list | head -n 50'
echo

echo "== Marquez namespaces (host) =="
curl -sf http://localhost:5001/api/v1/namespaces | head -c 200; echo
echo

echo "== Airflow health (host) =="
curl -sf http://localhost:8080/health; echo
echo

echo "[OK] stack looks ready."
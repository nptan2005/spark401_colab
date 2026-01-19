#!/usr/bin/env bash
set -euo pipefail

cmd="${1:-list}"
topic="${2:-orders_raw}"

case "$cmd" in
  list)
    docker exec -it kafka sh -lc '/opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list'
    ;;
  consume)
    docker exec -it kafka sh -lc "/opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic $topic --from-beginning --max-messages 5"
    ;;
  create)
    docker exec -it kafka sh -lc "/opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic $topic --partitions 3 --replication-factor 1 || true"
    ;;
  *)
    echo "Usage: scripts/kafka_tools.sh {list|consume|create} [topic]"
    exit 1
    ;;
esac
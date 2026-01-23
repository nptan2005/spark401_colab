#!/bin/bash
set -e

# Nếu chạy master
if [ "$SPARK_MODE" = "master" ]; then
    echo "[INFO] Starting Spark Master..."
    exec $SPARK_HOME/sbin/start-master.sh -h 0.0.0.0
fi

# Nếu chạy worker
if [ "$SPARK_MODE" = "worker" ]; then
    echo "[INFO] Starting Spark Worker..."
    exec $SPARK_HOME/sbin/start-worker.sh $SPARK_MASTER
fi

# Nếu không có mode thì chạy lệnh bình thường
exec "$@"
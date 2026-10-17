#!/bin/bash
set -e
wait_for() {
  python3 - "$1" "$2" <<'PY'
import socket, sys, time
host, port = sys.argv[1], int(sys.argv[2])
for _ in range(120):
    try:
        socket.create_connection((host, port), 3).close(); sys.exit(0)
    except OSError:
        time.sleep(2)
sys.exit("timeout waiting for %s:%s" % (host, port))
PY
}
echo "[entrypoint] waiting for Kafka and HBase Thrift..."
wait_for "${KAFKA_HOST:-kafka}" 9092
wait_for "${HBASE_HOST:-hbase}" "${HBASE_THRIFT_PORT:-9090}"

if [ ! -d "${MODEL_DIR:-/models}/crowding_gbt" ] || [ "${RETRAIN:-0}" = "1" ]; then
  echo "[entrypoint] training crowding model with Spark MLlib..."
  spark-submit --master "local[*]" --driver-memory "${TRAIN_DRIVER_MEMORY:-1200m}" /app/jobs/train_crowding_model.py \
    || echo "[entrypoint] training failed - streaming will use the profile-ratio fallback"
fi

echo "[entrypoint] starting streaming pipeline"
exec spark-submit --master "${SPARK_MASTER:-local[3]}" --driver-memory "${DRIVER_MEMORY:-1200m}" \
  --conf spark.ui.port=4040 --conf spark.sql.adaptive.enabled=false \
  /app/jobs/streaming_app.py

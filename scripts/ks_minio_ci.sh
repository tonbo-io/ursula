#!/usr/bin/env bash
# Starts MinIO for the keyed-streams e2e and drill jobs (S3 at 127.0.0.1:9000, minioadmin /
# minioadmin) and waits until it is live. The test stack creates its own S3 buckets.
#
# Uses Docker when available, otherwise a MinIO binary (MINIO_BIN, or `minio` on PATH).
set -euo pipefail

data="${MINIO_DATA:-${RUNNER_TEMP:-/tmp}/minio-data}"
mkdir -p "$data"
if command -v docker >/dev/null 2>&1 && docker info >/dev/null 2>&1; then
  docker run -d --name ks-minio -p 127.0.0.1:9000:9000 \
    -e MINIO_ROOT_USER=minioadmin -e MINIO_ROOT_PASSWORD=minioadmin \
    "${MINIO_IMAGE:-minio/minio:latest}" server /data >/dev/null
else
  bin="${MINIO_BIN:-$(command -v minio)}"
  MINIO_ROOT_USER=minioadmin MINIO_ROOT_PASSWORD=minioadmin \
    nohup "$bin" server "$data" --address 127.0.0.1:9000 --quiet >"$data.log" 2>&1 &
fi
for _ in $(seq 1 60); do
  if curl -fsS http://127.0.0.1:9000/minio/health/live >/dev/null 2>&1; then
    echo "MinIO is live at http://127.0.0.1:9000"
    exit 0
  fi
  sleep 1
done
echo "MinIO did not become live" >&2
docker logs ks-minio 2>&1 | tail -50 >&2 || true
exit 1

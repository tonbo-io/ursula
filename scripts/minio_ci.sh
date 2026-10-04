#!/usr/bin/env bash
# Starts MinIO for the CI jobs that need S3 (the sqlite-vfs cluster e2e and the memory soak) at
# 127.0.0.1:9000 (minioadmin / minioadmin) and waits until it is live. The test stack creates its
# own S3 buckets.
#
# Tries, in order: a MinIO binary (MINIO_BIN or `minio` on PATH), a container (MINIO_IMAGE, else
# quay.io/minio/minio then cgr.dev/chainguard/minio; Docker Hub's minio/minio is gone), and finally
# the release binary from dl.min.io.
set -euo pipefail

data="${MINIO_DATA:-${RUNNER_TEMP:-/tmp}/minio-data}"
mkdir -p "$data"
export MINIO_ROOT_USER=minioadmin MINIO_ROOT_PASSWORD=minioadmin

run_binary() {
  nohup "$1" server "$data" --address 127.0.0.1:9000 --console-address 127.0.0.1:9001 --quiet >"$data.log" 2>&1 &
}

started=""
bin="${MINIO_BIN:-$(command -v minio || true)}"
if [ -n "$bin" ]; then
  run_binary "$bin"
  started="binary $bin"
elif command -v docker >/dev/null 2>&1 && docker info >/dev/null 2>&1; then
  for image in ${MINIO_IMAGE:-quay.io/minio/minio:latest cgr.dev/chainguard/minio:latest}; do
    if docker run -d --name ci-minio -p 127.0.0.1:9000:9000 \
      -e MINIO_ROOT_USER -e MINIO_ROOT_PASSWORD "$image" server /data >/dev/null; then
      started="container $image"
      break
    fi
    docker rm -f ci-minio >/dev/null 2>&1 || true
  done
fi
if [ -z "$started" ]; then
  case "$(uname -m)" in
    x86_64) arch=amd64 ;;
    aarch64 | arm64) arch=arm64 ;;
    *) echo "unsupported architecture $(uname -m)" >&2; exit 1 ;;
  esac
  bin="${RUNNER_TEMP:-/tmp}/minio"
  curl -fsSL -o "$bin" "https://dl.min.io/server/minio/release/linux-$arch/minio"
  chmod +x "$bin"
  run_binary "$bin"
  started="downloaded binary"
fi
for _ in $(seq 1 60); do
  if curl -fsS http://127.0.0.1:9000/minio/health/live >/dev/null 2>&1; then
    echo "MinIO ($started) is live at http://127.0.0.1:9000"
    exit 0
  fi
  sleep 1
done
echo "MinIO ($started) did not become live" >&2
cat "$data.log" >&2 2>/dev/null || docker logs ci-minio 2>&1 | tail -50 >&2 || true
exit 1

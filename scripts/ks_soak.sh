#!/usr/bin/env bash
# MANUAL TOOL, NOT RUN IN CI. No workflow calls this script; the soak is planned as a run on AWS
# (ECS or EKS) against real S3. Run it by hand as shown below.
#
# Keyed-streams soak (docs/architecture/keyed-streams-pi-durable.md §10 M4): builds the release `ursula` unless
# URSULA_BIN is set, then runs clients/pi-durable-ursula's soak (3 nodes + gateway + keyed indexer on
# MinIO, a mixed Pi harness population) for SOAK_DURATION_S (default 3600). Needs `minio` on PATH,
# MINIO_BIN, or URSULA_S3_ENDPOINT. Results (samples.jsonl, summary.json, progress.log) go to
# SOAK_RESULTS_DIR (default clients/pi-durable-ursula/soak-results/<timestamp>). Exits non-zero
# when a pass criterion fails; SOAK_ASSERT=0 only records.
#
# Long-run example: SOAK_DURATION_S=21600 SOAK_RESULTS_DIR=$RUNNER_TEMP/soak scripts/ks_soak.sh
set -euo pipefail

repo="$(cd "$(dirname "$0")/.." && pwd)"
if [ -z "${URSULA_BIN:-}" ]; then
  cargo build --release -p ursula --bin ursula --manifest-path "$repo/Cargo.toml"
  export URSULA_BIN="$repo/target/release/ursula"
fi
cd "$repo/clients/pi-durable-ursula"
[ -d node_modules ] || npm ci
exec npm run soak:manual

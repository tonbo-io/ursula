#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

run_count="${URSULA_WAL_BENCH_RUNS:-3}"
benchmark_filter="${URSULA_WAL_BENCH_FILTER:-groups=16/payload=256}"
criterion_group="${URSULA_WAL_BENCH_GROUP:-disk_wal_append_durable}"
artifact_root="${1:-target/raft-wal-bench}"
mkdir -p "$artifact_root"

{
  echo "git_revision=$(git rev-parse HEAD)"
  echo "rustc=$(rustc --version)"
  echo "cargo=$(cargo --version)"
  echo "kernel=$(uname -a)"
  echo "filesystem=$( (df -T . 2>/dev/null || df .) | tail -n 1)"
  echo "benchmark_filter=$benchmark_filter"
  echo "criterion_group=$criterion_group"
  echo "run_count=$run_count"
} >"$artifact_root/environment.txt"

for run in $(seq 1 "$run_count"); do
  run_dir="$artifact_root/run-$run"
  mkdir -p "$run_dir"
  cargo bench -p ursula-raft --bench disk_wal -- "$benchmark_filter" \
    --warm-up-time "${URSULA_WAL_BENCH_WARMUP_SECONDS:-1}" \
    --measurement-time "${URSULA_WAL_BENCH_MEASUREMENT_SECONDS:-2}" \
    --sample-size "${URSULA_WAL_BENCH_SAMPLE_SIZE:-20}" \
    2>&1 | tee "$run_dir/output.txt"
  rm -rf "$run_dir/criterion"
  cp -R "target/criterion/$criterion_group" "$run_dir/criterion"
done

python3 scripts/summarize_raft_wal.py "$artifact_root"

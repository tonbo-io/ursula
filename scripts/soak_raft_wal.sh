#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

cycles="${URSULA_WAL_SOAK_CYCLES:-3}"
artifact_root="${1:-target/raft-wal-soak}"
mkdir -p "$artifact_root"

{
  echo "git_revision=$(git rev-parse HEAD)"
  echo "rustc=$(rustc --version)"
  echo "cargo=$(cargo --version)"
  echo "kernel=$(uname -a)"
  echo "filesystem=$(df -k . | tail -n 1)"
  echo "cycles=$cycles"
  echo "wal_backend=disk"
  echo "topology=three-voter replication plus two-voter late-learner install"
} >"$artifact_root/environment.txt"

cargo test -p ursula-raft \
  log_store::file::tests::online_reclaim_converges_at_production_threshold \
  --lib -- --exact --ignored --nocapture \
  2>&1 | tee "$artifact_root/online-reclaim.txt"

tests=(
  cli_static_grpc_raft_log_dir_replicates_between_nodes
  cli_static_grpc_raft_log_dir_installs_snapshot_for_late_learner
  cli_static_grpc_raft_log_dir_recovers_with_bootstrap_enabled_after_restart
)

for cycle in $(seq 1 "$cycles"); do
  cycle_dir="$artifact_root/cycle-$cycle"
  mkdir -p "$cycle_dir"
  for test_name in "${tests[@]}"; do
    cargo test -p ursula --test static_cluster_cli "$test_name" -- --exact --nocapture \
      2>&1 | tee "$cycle_dir/$test_name.txt"
  done
done

{
  echo "completed_cycles=$cycles"
  echo "tests_per_cycle=${#tests[@]}"
  echo "result=pass"
} >"$artifact_root/summary.txt"

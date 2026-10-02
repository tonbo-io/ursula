#!/usr/bin/env bash
# Full local M0c suite: for one WAL configuration, start a fresh 3-node cluster
# per writer count, run the matrix (no background + background), tear down and
# wipe. Fresh clusters keep snapshot size and WAL growth from one cell out of
# the next.
#
#   scripts/ks_latency_suite.sh <label> <wal: disk|memory> <data_root> <port_base> <bg_rate> <out_dir>
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
label="${1:?label}"
wal="${2:?wal}"
root="${3:?data root}"
base="${4:?port base}"
bg_rate="${5:?background appends/s}"
out="${6:?out dir}"

for n in ${KS_WRITERS:-1 4 8 16}; do
  rm -rf "$root"
  "$repo_root/scripts/ks_latency_cluster.sh" up "$wal" "$root" "$base"
  KS_WRITERS="$n" KS_BG_RATE="$bg_rate" \
    "$repo_root/scripts/ks_latency_matrix.sh" "http://127.0.0.1:$base" "$label" "$out" || true
  curl -fsS "http://127.0.0.1:$((base + 1))/__ursula/metrics" >"$out/$label-n$n.node1-metrics.json" || true
  "$repo_root/scripts/ks_latency_cluster.sh" down "$root"
  rm -rf "$root"
done

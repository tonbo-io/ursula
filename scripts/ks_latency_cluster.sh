#!/usr/bin/env bash
# Local 3-node Ursula cluster + gateway for the keyed-streams M0c latency spike.
#
#   scripts/ks_latency_cluster.sh up   <wal: disk|memory> <data_root> [port_base]
#   scripts/ks_latency_cluster.sh down <data_root>
#
# Ports (port_base defaults to 15400):
#   gateway       port_base
#   node i        port_base + i        (i = 1..3)
#   node i admin  port_base + 10 + i
# Cold storage is the per-process `memory` backend with a 200 ms flush tick, so
# cold flush runs under load (followers cannot serve cold reads; this spike only
# measures writes). Binaries come from $URSULA_BIN (default target/release/ursula).
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
bin="${URSULA_BIN:-$repo_root/target/release/ursula}"
cmd="${1:?up|down}"

case "$cmd" in
up)
  wal="${2:?wal backend}"
  root="${3:?data root}"
  base="${4:-15400}"
  groups="${KS_GROUPS:-32}"
  cores="${KS_CORES:-2}"
  mkdir -p "$root"
  peers=""
  for i in 1 2 3; do
    peers+=$'[[raft.peers]]\n'"node_id = $i"$'\n'"url = \"http://127.0.0.1:$((base + i))\""$'\n\n'
  done
  for i in 1 2 3; do
    node_dir="$root/node$i"
    mkdir -p "$node_dir"
    {
      echo "[server]"
      echo "listen = \"127.0.0.1:$((base + i))\""
      echo "admin_listen = \"127.0.0.1:$((base + 10 + i))\""
      echo
      echo "[runtime]"
      echo "core_count = $cores"
      echo
      echo "[raft]"
      echo "node_id = $i"
      echo "group_count = $groups"
      if [ "$i" = 1 ]; then echo "init_membership = true"; else echo "init_membership = false"; fi
      echo "init_membership_per_group = false"
      if [ -n "${KS_SNAPSHOT_LOGS:-}" ]; then echo "snapshot_logs_since_last = $KS_SNAPSHOT_LOGS"; fi
      echo
      echo "[raft.wal]"
      echo "backend = \"$wal\""
      if [ "$wal" = "disk" ]; then
        echo "path = \"$node_dir/wal\""
      else
        echo "allow_volatile_multi_peer = true"
      fi
      echo
      printf '%s' "$peers"
      echo "[storage.cold]"
      echo "backend = \"memory\""
      echo "flush_interval = \"200ms\""
      echo "gc_interval = \"1s\""
    } >"$node_dir/ursula.toml"
    "$bin" server --config "$node_dir/ursula.toml" >"$node_dir/stdout.log" 2>"$node_dir/stderr.log" &
    echo $! >"$node_dir/pid"
  done
  "$bin" gateway --listen "127.0.0.1:$base" \
    --upstream "http://127.0.0.1:$((base + 1))" \
    --upstream "http://127.0.0.1:$((base + 2))" \
    --upstream "http://127.0.0.1:$((base + 3))" \
    --raft-group-count "$groups" >"$root/gw.stdout.log" 2>"$root/gw.stderr.log" &
  echo $! >"$root/gw.pid"
  # Wait for every node to report ready and the gateway to accept a PUT.
  for _ in $(seq 1 120); do
    ok=0
    for i in 1 2 3; do
      if curl -fsS "http://127.0.0.1:$((base + i))/__ursula/ready" >/dev/null 2>&1; then ok=$((ok + 1)); fi
    done
    if [ "$ok" = 3 ] && curl -fsS -X PUT "http://127.0.0.1:$base/ks-ready-probe" >/dev/null 2>&1 &&
      curl -fsS -X PUT -H 'content-type: application/json' "http://127.0.0.1:$base/ks-ready-probe/probe" >/dev/null 2>&1; then
      # Let every group finish its first election before measuring.
      sleep "${KS_SETTLE_SECS:-8}"
      echo "cluster up: wal=$wal gateway=http://127.0.0.1:$base"
      exit 0
    fi
    sleep 0.5
  done
  echo "cluster did not become ready; see $root/*/stderr.log" >&2
  exit 1
  ;;
down)
  root="${2:?data root}"
  for pidfile in "$root"/gw.pid "$root"/node*/pid; do
    [ -f "$pidfile" ] || continue
    kill "$(cat "$pidfile")" 2>/dev/null || true
  done
  sleep 1
  for pidfile in "$root"/gw.pid "$root"/node*/pid; do
    [ -f "$pidfile" ] || continue
    kill -9 "$(cat "$pidfile")" 2>/dev/null || true
    rm -f "$pidfile"
  done
  ;;
*)
  echo "usage: $0 up <disk|memory> <data_root> [port_base] | down <data_root>" >&2
  exit 2
  ;;
esac

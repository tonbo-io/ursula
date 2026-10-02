#!/usr/bin/env bash
# Run the M0c record-match latency matrix against one running cluster.
#
#   scripts/ks_latency_matrix.sh <gateway_url> <label> <out_dir>
#
# Env:
#   KS_WRITERS     writer counts            (default "1 4 8 16")
#   KS_RUNS        runs per cell            (default 3)
#   KS_DURATION    measured seconds per run (default 20)
#   KS_WARMUP      warm-up seconds          (default 3)
#   KS_BG_RATE     background appends/s; 0 disables the background cells (default 0)
#   KS_BG_STREAMS  background streams       (default 64)
#   KS_BG_BYTES    background payload bytes (default 1024)
# Writes one JSON file per run: <out_dir>/<label>-bg<0|1>-n<N>-r<run>.json
# plus <out_dir>/<label>-bg1-n<N>-r<run>.bg.json for the background generator.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
bench="${URSULA_BENCH_BIN:-$repo_root/target/release/ursula-bench}"
target="${1:?gateway url}"
label="${2:?label}"
out="${3:?out dir}"
mkdir -p "$out"

writers="${KS_WRITERS:-1 4 8 16}"
runs="${KS_RUNS:-3}"
duration="${KS_DURATION:-20}"
warmup="${KS_WARMUP:-3}"
bg_rate="${KS_BG_RATE:-0}"
bg_streams="${KS_BG_STREAMS:-64}"
bg_bytes="${KS_BG_BYTES:-1024}"

json_only() { sed -n '/^{/,$p'; }

bg_modes="0"
if [ "$bg_rate" != 0 ]; then bg_modes="0 1"; fi

for bg in $bg_modes; do
  for n in $writers; do
    for run in $(seq 1 "$runs"); do
      name="$label-bg$bg-n$n-r$run"
      bg_pid=""
      if [ "$bg" = 1 ]; then
        per_stream=$(((bg_rate + bg_streams - 1) / bg_streams))
        "$bench" multi-stream --target "$target" --bucket "bg-$name" --streams "$bg_streams" \
          --rate-per-stream "$per_stream" --payload-bytes "$bg_bytes" \
          --duration-secs $((duration + warmup + 4)) 2>/dev/null | json_only >"$out/$name.bg.json" &
        bg_pid=$!
        sleep 2
      fi
      load=$(uptime | sed 's/.*load averages*: //')
      "$bench" record-match --target "$target" --bucket "rm-$label" --stream-prefix "$name" \
        --writers "$n" --duration-secs "$duration" --warmup-secs "$warmup" --payload-mix pi \
        --samples-out "$out/$name.samples" \
        2>"$out/$name.err" | json_only >"$out/$name.json"
      python3 - "$out/$name.json" "$load" <<'EOF'
import json, sys
p, load = sys.argv[1], sys.argv[2]
d = json.load(open(p))
d["load_avg_at_start"] = load
json.dump(d, open(p, "w"), indent=1)
l = d["latency_ms"]
print(f"{p.split('/')[-1]}: {d['commits_per_sec']:.0f}/s mean {l['mean_ms']:.2f} p50 {l['p50_ms']:.2f} p99 {l['p99_ms']:.2f} p999 {l['p999_ms']:.2f} err={sum(d['errors'].values())} 412={d['precondition_failed']} load={load}")
EOF
      if [ -n "$bg_pid" ]; then wait "$bg_pid" || true; fi
    done
  done
done

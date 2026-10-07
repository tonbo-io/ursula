#!/usr/bin/env bash
# Run the official Durable Streams conformance suite against one local Ursula
# node (single-voter Raft, its WAL in the work directory) through proxy.mjs.
#
# Usage: scripts/ds-conformance/run.sh <ursula-binary> [base-port]
# Listens on <base-port> (Ursula) and <base-port>+1 (proxy); default 15500.
# Extra vitest arguments can be passed in VITEST_ARGS.
#
# Ursula does not implement every upstream feature (forks, JSON-array reads,
# absolute Location), so the run is gated on expected-failures.txt: it fails
# when a test outside that list fails, and reports listed tests that now pass
# so the list can be trimmed. Set EXPECTED_FAILURES= (empty) to get vitest's
# own exit status instead.
set -euo pipefail

binary=${1:?usage: run.sh <ursula-binary> [base-port]}
port=${2:-15500}
proxy_port=$((port + 1))
here=$(cd "$(dirname "$0")" && pwd)
work=$(mktemp -d)
pids=()
cleanup() {
  for pid in "${pids[@]}"; do
    kill "$pid" 2>/dev/null || true
    wait "$pid" 2>/dev/null || true
  done
  rm -rf "$work"
}
trap cleanup EXIT

if [ ! -d "$here/node_modules/@durable-streams/server-conformance-tests" ]; then
  (cd "$here" && npm ci --no-audit --no-fund --legacy-peer-deps >/dev/null)
fi

cat >"$work/ursula.toml" <<TOML
[server]
listen = "127.0.0.1:$port"

[raft.wal]
path = "$work/wal"
TOML
"$binary" server --config "$work/ursula.toml" --node-id 1 >"$work/ursula.log" 2>&1 &
pids+=($!)
node "$here/proxy.mjs" "$proxy_port" "http://127.0.0.1:$port" conformance >"$work/proxy.log" 2>&1 &
pids+=($!)

for _ in $(seq 1 60); do
  if curl -fsS -o /dev/null -X PUT "http://127.0.0.1:$port/conformance" 2>/dev/null; then
    break
  fi
  sleep 1
done
curl -fsS -o /dev/null -X PUT "http://127.0.0.1:$port/conformance" || {
  cat "$work/ursula.log"
  exit 1
}

cd "$here"
expected=${EXPECTED_FAILURES-$here/expected-failures.txt}
status=0
CONFORMANCE_TEST_URL="http://127.0.0.1:$proxy_port" NO_COLOR=1 \
  ./node_modules/.bin/vitest run --no-coverage --reporter=default --reporter=json \
  --outputFile.json="$work/results.json" ${VITEST_ARGS:-} || status=$?
if [ -z "$expected" ]; then
  exit "$status"
fi
if [ ! -s "$work/results.json" ]; then
  echo "conformance: vitest produced no results" >&2
  exit 1
fi

node -e '
  const results = JSON.parse(require("fs").readFileSync(process.argv[1], "utf8"));
  for (const file of results.testResults)
    for (const test of file.assertionResults)
      if (test.status === "failed")
        console.log([...test.ancestorTitles, test.title].join(" > "));
' "$work/results.json" | sort -u >"$work/failed.txt"
grep -v '^#' "$expected" | grep -v '^$' | sort -u >"$work/expected.txt" || true
unexpected=$(comm -23 "$work/failed.txt" "$work/expected.txt")
fixed=$(comm -13 "$work/failed.txt" "$work/expected.txt")
if [ -n "$fixed" ]; then
  echo "conformance: expected failures that now pass (trim expected-failures.txt):"
  echo "$fixed" | sed 's/^/  /'
fi
if [ -n "$unexpected" ]; then
  echo "conformance: unexpected failures:" >&2
  echo "$unexpected" | sed 's/^/  /' >&2
  exit 1
fi
echo "conformance: no unexpected failures ($(wc -l <"$work/failed.txt" | tr -d ' ') expected)"

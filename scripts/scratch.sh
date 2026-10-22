#!/usr/bin/env bash
set -uo pipefail
export RUSTFLAGS="--cfg madsim"
fail=0
run() { echo "::group::$*"; eval "$@" 2>&1 | grep -av "^ *[0-9]*: \|^ *at " | tail -n 150; s=${PIPESTATUS[0]}; echo "::endgroup::"; echo "STATUS $s :: $*"; [ "$s" -eq 0 ] || fail=1; }
run cargo\ test\ -p\ ursula-raft\ --lib
run cargo\ test\ -p\ ursula-sim\ --lib
run cargo\ test\ -p\ ursula-sim\ smoke_corpus_replays\ --\ --nocapture
run cargo\ test\ -p\ ursula-raft\ snapshot_metadata_\ --\ --nocapture
run RECOVERY_GATE_SEEDS=1..=24\ POISON_MARKER_SEEDS=1..=24\ cargo\ test\ -p\ ursula-sim\ --lib\ --\ recovery_tests\ poison_marker\ --nocapture
echo "OVERALL $fail"; exit $fail

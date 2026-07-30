#!/usr/bin/env bash
set -euo pipefail

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
guest_manifest="${repo_dir}/crates/ursula-wasm/tests/fixtures/reducer-guest/Cargo.toml"
guest_wasm="${repo_dir}/crates/ursula-wasm/tests/fixtures/reducer-guest/target/wasm32-unknown-unknown/release/ursula_reducer_test_guest.wasm"
component_wasm="${repo_dir}/crates/ursula-wasm/tests/fixtures/reducer-guest/target/ursula-reducer-test.component.wasm"

cargo build --manifest-path "${guest_manifest}" --release --target wasm32-unknown-unknown
cargo run --manifest-path "${repo_dir}/Cargo.toml" -p ursula-wasm --example componentize -- "${guest_wasm}" "${component_wasm}"
cargo test --manifest-path "${repo_dir}/Cargo.toml" -p ursula wasm_reducer_commits_state_and_records_through_http -- --ignored --nocapture

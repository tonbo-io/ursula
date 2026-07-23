set shell := ["bash", "-eu", "-o", "pipefail", "-c"]

build:
    cargo build --workspace

build-jemalloc-prof:
    cargo build -p ursula --bin ursula --features jemalloc-prof

test:
    cargo test --workspace

fmt-check:
    cargo fmt --all -- --check

clippy:
    cargo clippy --workspace --all-targets -- -D warnings

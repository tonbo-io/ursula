# Ursula Agent Guide

## Project Pitch

Ursula is a self-hosted, distributed Durable Streams server for replayable, append-only event timelines over plain HTTP/SSE, with Raft durability and S3-backed cold storage.

## Repository Layout

- `crates/ursula`: HTTP server, CLI, bootstrap wiring, and end-to-end protocol tests.
- `crates/ursula-runtime`: per-core runtime, group engine boundary, hot/cold read and write paths, WAL engine, cold store integration, and runtime benchmarks.
- `crates/ursula-raft`: OpenRaft-backed group engine, log store, snapshot handling, and gRPC Raft plumbing.
- `crates/ursula-stream`: deterministic stream state machine, stream commands, responses, snapshots, payload metadata, and validation.
- `crates/ursula-shard`: bucket/stream routing, core ownership, Raft group placement, and shared shard identifiers.
- `crates/ursula-proto`: shared protobuf schema and generated types used by Raft logs, requests, and persisted metadata.
- `crates/ursula-bench`: HTTP/client benchmark harnesses.
- `docs/web`: documentation site content and Mintlify/Astro web docs.
- `scripts`: repository helper scripts.

## Engineering Posture

This project is currently in the `0.1.x` phase. Breaking compatibility is acceptable when it makes the implementation more correct, simpler, or easier to maintain.

Prefer the direct Rust-native design over compatibility shims or legacy fallback paths. Use the type system to encode invariants where practical, keep ownership and lifetimes explicit, and choose data structures that make the hot path and persisted state easy to reason about.

Optimize for local readability and whole-system readability at the same time: keep changes scoped to the relevant module, avoid clever abstractions without a real payoff, and preserve clear boundaries between routing, runtime actors, Raft replication, stream state, and cold storage.

For performance-oriented changes, add or update a focused micro benchmark that validates the expected benefit.

Use `tracing` logging macros for Rust diagnostics and operational logging. Do not add `eprintln!` logging in Rust code; use the appropriate `tracing::{error,warn,info,debug,trace}!` level instead.

## Before Commit

Run formatting and lint checks before committing:

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
```

Run focused tests for the area you changed. For broader runtime, state machine, or protocol changes, also run the relevant package or workspace tests before committing.

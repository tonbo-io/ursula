# Ursula Agent Guide

## Project Overview

Ursula is a self-hosted, distributed Durable Streams server for replayable, append-only event timelines over plain HTTP/SSE, with Raft durability and S3-backed cold storage. It implements the [Durable Streams Protocol](https://github.com/durable-streams/durable-streams) and is designed for open-source self-hosting, low write latency (sub-50ms P99), plain S3 economics, and quorum-replicated durability.

The server uses a thread-per-core, multi-Raft architecture: each stream hashes to one Raft group, that group has one replica on each voter node, and the same group ID is owned by a deterministic core on every node. Groups replicate independently with no cross-group transaction path.

- **Version**: 0.7.0 (pre-1.0)
- **License**: Apache-2.0
- **Repository**: <https://github.com/tonbo-io/ursula>
- **Homepage**: <https://ursula.tonbo.io>
- **Language**: Rust (Edition 2024)
- **Toolchain**: Nightly (`nightly-2026-06-01`)

## Technology Stack

- **Language**: Rust 2024 Edition, nightly toolchain
- **Async Runtime**: Tokio (multi-thread + single-thread worker pools)
- **HTTP Server**: axum (stateless front door)
- **Raft Consensus**: OpenRaft `=0.10.0-alpha.28` from crates.io. The only `[patch.crates-io]` entry is the vendored `futures-macro` (`third_party/futures-macro`) for madsim determinism
- **gRPC**: tonic (inter-node Raft RPCs)
- **Serialization**: serde, prost (protobuf)
- **Cold Storage**: OpenDAL (S3, memory backends)
- **Observability**: tracing, OpenTelemetry OTLP
- **Allocator**: mimalloc
- **Simulation**: madsim (deterministic simulation testing)
- **Benchmarking**: criterion
- **Property Testing**: proptest
- **Documentation Site**: Vite + React + MDX (docs/web/)
- **Deployment**: Docker, Helm (Kubernetes)
- **CI/CD**: GitHub Actions

## Repository Layout

### Rust Workspace Crates (`crates/`)

| Crate                  | Description                                                                                                                                   |
| ---------------------- | --------------------------------------------------------------------------------------------------------------------------------------------- |
| `ursula`               | HTTP server, CLI binaries (`ursula`, `ursulagw`), bootstrap wiring, and end-to-end protocol tests.                                            |
| `ursula-runtime`       | Per-core actor runtime: hot ring, cold-tier flush, group engine boundary, cold store integration, and runtime benchmarks.                   |
| `ursula-raft`          | OpenRaft-backed group engine: network, log store, snapshot handling, gRPC Raft plumbing.                                                      |
| `ursula-stream`        | Deterministic stream state machine: bucket/stream commands, responses, snapshots, payload metadata, validation.                               |
| `ursula-shard`         | Bucket/stream routing, core ownership, Raft group placement, shared shard identifiers (`CoreId`, `ShardId`, `RaftGroupId`, `BucketStreamId`). |
| `ursula-proto`         | Shared protobuf schemas (model value types and snapshot frames) reused across crates; replicated commands/responses travel as serde MessagePack. |
| `ursula-config`        | Configuration types and TOML loading for Ursula server.                                                                                       |
| `ursula-observability` | Shared tracing/OpenTelemetry initialization for Ursula binaries.                                                                              |
| `ursula-gateway`       | Gateway binary (`ursulagw`) that routes client HTTP traffic while hiding internal leader redirects.                                           |
| `ursula-bench`         | HTTP/client benchmark harnesses for performance testing.                                                                                      |
| `ursula-ctl`           | Operational CLI (`ursulactl`) for logical cluster management over the admin HTTP API: drain leaderships, wait for catch-up, readiness gates.                                 |
| `ursula-sim`           | Deterministic simulation harnesses using madsim for fault injection and invariant checking.                                                   |
| `ursula-state-probe`   | Bounded-state measurement harness (W1-W6, L2 engine probes, counting allocator) and the CI state-growth ratchet (`docs/architecture/bounded-stream-state.md` §7). |

### Other Top-Level Directories

- `docs/web/`: Documentation site content (Vite + React + MDX). Deployed to Cloudflare Pages.
- `docs/architecture/`: Architecture design documents (DST, thread-per-core multi-Raft, runtime evaluation).
- `charts/ursula/`: Helm chart for Kubernetes deployment.
- `scripts/`: Repository helper scripts including chaos testing (`ursula_chaos_agent.py`) and DST audit tools (`scripts/dst/`).
- `target/`: Cargo build output.

## Build System

The project uses Cargo as its build system with a workspace configuration in the root `Cargo.toml`.

### Key Configuration Files

- **`Cargo.toml`**: Workspace manifest with shared dependencies, lints, and profiles.
- **`rust-toolchain.toml`**: Pins the nightly toolchain (`nightly-2026-06-01`) and required components (`rustfmt`, `clippy`, `rust-src`, `miri`, `rust-analyzer`).
- **`clippy.toml`**: Clippy-specific settings including disallowed types/macros (`once_cell::sync::Lazy`, `lazy_static!`), test-specific relaxations, and API-breaking avoidance.
- **`rustfmt.toml`**: Formatting configuration with import reordering (`reorder_imports = true`, `imports_granularity = "Item"`, `group_imports = "StdExternalCrate"`).
- **`justfile`**: Provides convenient recipes for common tasks (`build`, `test`, `fmt-check`, `clippy`).

### Build Profiles

- **`dev`**: Default development profile with `overflow-checks = true`, `incremental = true`.
- **`release`**: Optimized with `lto = "thin"`, `opt-level = "s"`, `codegen-units = 1`, `overflow-checks = false`.
- **`ci`**: Inherits release, enables `overflow-checks = true` and `debug-assertions = true`.
- **`bench`**: For benchmarks with `debug = true`, `overflow-checks = false`, `debug-assertions = true`.
- **`dist`**: Full LTO (`fat`), `codegen-units = 1`.

### Common Build Commands

```bash
# Build the entire workspace
cargo build --workspace

# Build release binaries (ursula, ursulactl, ursulagw)
cargo build --release --bin ursula --bin ursulactl --bin ursulagw

# Run a single in-memory node (zero-config dev mode)
cargo run --bin ursula

# Or with an explicit preset
cargo run --bin ursula -- --preset tiny --node-id 1

# Or with a configuration file
cargo run --bin ursula -- --config ./ursula.toml

# Run the benchmark client
cargo run --bin ursula-bench

# Run the gateway
cargo run --bin ursulagw

# Run the operational CLI
cargo run --bin ursulactl
```

## Code Style Guidelines

### Formatting

Run `cargo fmt --all -- --check` before committing. Formatting is controlled by `rustfmt.toml`:

- Edition 2024
- Reorder imports (`reorder_imports = true`)
- Import granularity per item (`imports_granularity = "Item"`)
- Group imports: std, external, crate (`group_imports = "StdExternalCrate"`)
- Single-line where clauses where possible
- Trailing commas on vertical multiline expressions
- Format code in doc comments
- Normalize comments

### Linting

Every crate inherits the workspace lint configuration:

```toml
[lints]
workspace = true
```

Do not add crate-level `[lints.*]` tables. Cargo ignores the workspace lints for a crate that declares its own, which silently disables everything below. Add new lints to `[workspace.lints]` in the root `Cargo.toml` instead.

The workspace configuration in `Cargo.toml` under `[workspace.lints.clippy]`:

**Panic Prevention**: `string_slice`, `indexing_slicing`, `unwrap_used`, `panic`, `todo`, `unimplemented`, `get_unwrap`, `unwrap_in_result`, `unchecked_time_subtraction`, `panic_in_result_fn`, `arithmetic_side_effects` are all warned.

**Silent Failure Prevention**: `let_underscore_future`, `let_underscore_must_use`, `unused_result_ok`, `map_err_ignore`, `assertions_on_result_states`.

**Async Safety**: `await_holding_lock`, `await_holding_refcell_ref`, `large_futures`.

**Memory Safety**: `mem_forget`, `undocumented_unsafe_blocks`, `multiple_unsafe_ops_per_block`.

**Code Quality**: `float_cmp`, `rc_mutex`, `dbg_macro`, `wildcard_imports` (deny).

**Attribute Discipline**: `allow_attributes`, `allow_attributes_without_reason`.

**Note**: Tests are allowed to use `unwrap`, `panic`, `expect`, `dbg`, and indexing/slicing (configured in `clippy.toml`). Those allowances cover only `#[test]` functions and `#[cfg(test)]` modules, so integration test crates (`tests/*.rs`) and benchmarks state the same exemptions with a crate-level `#![expect(..., reason = "...")]`.

**Suppressions**: Suppress a lint only with `#[expect(lint, reason = "...")]` on the narrowest item. Do not use `#[allow]`. An unfulfilled `expect` fails the build, so a suppression disappears once the code no longer needs it.

**Known debt**: `arithmetic_side_effects` and `assertions_on_result_states` predate lint enforcement in many modules. Those modules carry a module-level `#![expect(..., reason = "pre-existing ... debt")]`. Do not add these markers to new modules. When you change a marked module, fix its sites and delete the marker.

### Unsafe Code

`unsafe_code = "deny"` is set at the workspace level. Any unsafe code requires an explicit `#[expect(unsafe_code, reason = "...")]` with strong justification and a `// SAFETY:` comment on every unsafe block. The only current uses are the counting `GlobalAlloc` implementations used for allocation measurement. Build scripts pass the vendored `protoc` through `prost_build::Config::protoc_executable` rather than `std::env::set_var`.

### Error Handling

Follow the typed-error style of the early crates: `ursula-runtime`'s `RuntimeError`, `ursula-stream`'s `StreamSnapshotError` and `ursula-raft`'s `GrpcRpcError`.

- Library code returns a `thiserror` enum defined at the module or crate boundary. Variants carry structured fields (identifiers as newtypes such as `RaftGroupId`), not pre-formatted strings.
- Do not return `Result<_, String>`, `Box<dyn Error>` or `anyhow::Result` from library APIs. `anyhow` is for binary `main`/CLI glue and tests. Do not wrap failures that are not I/O in `io::Error::other`.
- Keep the source error with `#[from]` or `#[source]` instead of `to_string()`. Discard it only when it carries no information (an integer conversion overflow, a poisoned lock, a closed channel), and then name the binding (`|_overflow|`, `|_poisoned|`, `|_closed|`) so the discard is deliberate. Never write `map_err(|_| ...)`.
- Make every distinction a caller acts on a variant: retryable or permanent, not leader or protocol mismatch, outcome unknown or failed. Expose classification through methods such as `RuntimeError::leader_hint()`, never by matching message text.
- Convert to transport errors in one place per transport (`impl From<GrpcRpcError> for tonic::Status`, `impl IntoResponse`) and choose the precise status code.
- Tests assert the variant, for example `assert!(matches!(err, Error::StaleFence { .. }))`, not `is_err()` or a message substring.

### Code Idioms

Follow the conventions of the early crates (`ursula-stream`, `ursula-shard`, `ursula-runtime`) instead of introducing new patterns:

- Model state with enums so that invalid combinations cannot be represented. Do not encode stages as a set of `Option` and `bool` fields checked by a `validate()` method.
- Drive a state machine through one dispatcher with one method per command, as `StreamStateMachine::apply(StreamCommand) -> StreamResponse` does. Keep decision logic pure: no I/O, with time passed in (as `now_ms` in `StreamCommand`). Keep the async drivers around it thin.
- Use newtypes for identifiers (`CoreId`, `ShardId`, `RaftGroupId`, `BucketStreamId`) and parse at the boundary with `TryFrom` or `#[serde(try_from)]` rather than validating afterwards.
- Both sides of a protocol (server and `ursulactl`) share typed request and response structs. Do not build JSON with `json!` or `object.insert`, and do not match string literals across Rust and shell.
- Keep one implementation of each policy check. When two call sites need the same predicate, call a shared function.
- Wait on `watch` channels instead of sleep-polling, keep the `JoinHandle` of every long-lived task, and do not perform blocking I/O while holding a `std::sync::Mutex`.
- Cover state machines with many transitions with `proptest`, as `ursula-stream/src/state_machine/tests.rs` does.
- Tests go through production entry points. Put test-only APIs behind `#[cfg(test)]`.

### Logging

Use `tracing` macros exclusively. Do not use `eprintln!`:

```rust
tracing::error!("...");
tracing::warn!("...");
tracing::info!("...");
tracing::debug!("...");
tracing::trace!("...");
```

The `release-max-info` feature (enabled by default) compiles out `trace!`/`debug!` in release builds to keep the hot path free.

### Documentation

Every crate `lib.rs` should contain a module map in its top-level doc comment explaining the purpose of each module. Follow the existing pattern:

```rust
//! Module map:
//!
//! - [`module_name`]: brief description.
```

## Testing Strategy

Ursula employs multiple testing layers:

### 1. Unit Tests

Inline `#[cfg(test)]` modules within source files. Run with:

```bash
cargo test --workspace --lib --bins
```

### 2. Integration Tests

- `crates/ursula/tests/static_cluster_cli.rs`: Full static gRPC Raft cluster tests (spawns real processes).
- `crates/ursula-runtime/tests/s3_cold_path.rs`: S3 cold-path integration test. Requires `URSULA_COLD_S3_INTEGRATION=1` and `URSULA_COLD_S3_BUCKET` environment variable.
- The S3 tests (`s3_cold_path.rs`, the S3 cold-manifest restart in `static_cluster_cli.rs`, and `crates/ursula-index/tests/s3.rs`) put their roots under `URSULA_S3_PREFIX` when it is set. CI's Integration workflow runs them on real AWS S3 through GitHub OIDC (`.github/actions/ci-s3`), one prefix per job; fork PRs fall back to MinIO.

```bash
# S3 integration test
URSULA_COLD_S3_INTEGRATION=1 URSULA_COLD_S3_BUCKET=my-bucket cargo test -p ursula-runtime --test s3_cold_path

# Static cluster test
cargo test -p ursula --test static_cluster_cli
```

### 3. Documentation Tests

```bash
cargo test --workspace --doc
```

### 4. Benchmarks

Runtime benchmarks in `crates/ursula-runtime/benches/`:

- `cold_cache`: Cold store cache performance
- `hot_snapshot`: Hot ring snapshot performance
- `append_apply`: Append and apply throughput
- `atomic_padding`: Atomic padding overhead

Run benchmarks with `cargo bench -p ursula-runtime`.

Observability benchmark in `crates/ursula-observability/benches/span_overhead.rs`.

### 5. Deterministic Simulation Testing (DST)

This is a **critical** part of Ursula's testing strategy. The `ursula-sim` crate uses `madsim` to run deterministic simulations with fault injection.

**Key Concepts**:

- **Scenarios**: Define what the system does under test.
- **Faults**: Injected failures (network partitions, node stops, disk corruption, delays).
- **Invariants**: Properties that must always hold (read-your-writes, no data loss, etc.).
- **Corpus**: A JSON corpus of seed schedules lives in `crates/ursula-sim/corpus/schedule-smoke.json`.

**Running DST**:

```bash
# Audit DST guards (8 audits)
python3 -m scripts.dst all

# Run smoke corpus replays
RUSTFLAGS="--cfg madsim" cargo test -p ursula-sim smoke_corpus_replays -- --nocapture

# Run a smoke sweep
RUSTFLAGS="--cfg madsim" cargo run -p ursula-sim --bin ursula-sim -- smoke \
  --failure-dir target/ursula-sim-failures \
  --seed-range 60..=64

# Replay a recorded artifact
RUSTFLAGS="--cfg madsim" cargo run -p ursula-sim --bin ursula-sim -- replay \
  --artifact path/to/record.json

# Minimize a failure
RUSTFLAGS="--cfg madsim" cargo run -p ursula-sim --bin ursula-sim -- minimize \
  --artifact path/to/failure.json \
  --invariant some_invariant_name \
  --output minimized.json
```

**DST Nightly CI** runs a comprehensive seed sweep (seeds 60-199+) with both passing and expected-failure scenarios.

### 6. Property-Based Tests

The `ursula-stream` crate uses `proptest` for state machine property testing in `src/state_machine/tests.rs`.

## Pre-Commit Checklist

1. **Format**: `cargo fmt --all -- --check`
2. **Lint**: `cargo clippy --workspace --all-targets -- -D warnings`
3. **Unit tests**: `cargo test --workspace --lib --bins`
4. **Doc tests**: `cargo test --workspace --doc`
5. **Focused integration tests** for the area changed
6. **Semantic commit messages** with component scope:
   - `feat(ursula-stream): add snapshot validation`
   - `fix(ursula-raft): preserve snapshot metadata`
   - `docs(web): remove redundant subtitles`
7. **Semantic PR titles**: PR titles must follow the same semantic commit message format (including component scope where applicable), because squash merge uses the PR title as the resulting commit's first line.
8. **No co-author information**: Do not include co-author information or `Co-authored-by` trailers in commit messages or PR descriptions.
9. **Concise commit bodies**: Keep any explanation after the first line brief and focused on the reason for the change and essential context. Omit redundant details, exhaustive change lists, and work logs; omit the body when the subject is sufficient.

## Architecture Boundaries

Maintain clear boundaries between these layers:

1. **Routing**: axum HTTP handlers in `ursula` — parse, route, render protocol.
2. **Runtime Actors**: `ursula-runtime` — per-core workers, group actors, hot ring, cold flush.
3. **Raft Replication**: `ursula-raft` — OpenRaft integration, gRPC network, log stores.
4. **Stream State**: `ursula-stream` — deterministic state machine, commands, snapshots.
5. **Cold Storage**: `ursula-runtime` cold store / OpenDAL integration.

Keep changes scoped to the relevant module. Avoid clever abstractions without real payoff.

## Deployment

### Docker

A multi-stage `Dockerfile` builds three binaries (`ursula`, `ursulactl`, `ursulagw`) and produces a `debian:bookworm-slim` runtime image with a non-root `ursula` user (UID 10001). Exposes port 4437.

### Helm

The Helm chart in `charts/ursula/` supports Kubernetes deployment with configurable values schema.

### GitHub Container Registry

The reviewed Stable Release workflow publishes crates, images, and Helm charts,
verifies them, and only then creates the Git tag and GitHub Release:

- Docker image: `ghcr.io/tonbo-io/ursula`
- Helm chart: `oci://ghcr.io/tonbo-io/charts/ursula`

GHCR holds only what tagged stable releases publish: the images, the charts,
and nothing else. Image indexes carry no attestation manifests. Commit
candidates come from the Commit Candidate Build workflow, which saves the image
in the Depot Registry. A candidate's chart is installed from a checkout of the
same commit.

## Security Considerations

- `unsafe_code = "deny"` at the workspace level.
- Clippy lints warn on `unwrap_used`, `panic`, `string_slice`, `indexing_slicing`, and arithmetic side effects.
- Protobuf compilation uses `protoc-bin-vendored` to avoid system protoc dependencies.
- Docker image runs as non-root user.
- Published Docker images carry build provenance and an SPDX SBOM as GitHub artifact attestations, verifiable with `gh attestation verify oci://ghcr.io/tonbo-io/ursula:<version> --repo tonbo-io/ursula` (add `--predicate-type https://spdx.dev/Document/v2.3` for the SBOM).

## Development Tips

- Use `just build`, `just test`, `just fmt-check`, `just clippy` for common tasks.
- The project is pre-1.0 (`0.x`); breaking compatibility is acceptable when it improves correctness or simplicity.
- For performance work, add/update a micro benchmark in `ursula-runtime/benches/`.
- When modifying protobuf schemas, remember both `ursula-proto` and `ursula-raft` have build scripts that regenerate code.
- The `tokio-console` feature can be enabled by building with `--no-default-features --features tokio-console` for async debugging.
- `cfg(madsim)` guards simulation-only code; be careful not to break madsim builds when adding async code.

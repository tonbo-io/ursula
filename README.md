# Ursula

Ursula is the clean migration workspace for a thread-per-core, multi-Raft durable stream runtime.

This repository starts as an architecture and scaffolding project. The migration target is not a direct line-by-line port of `riverrun`; the first milestone is to establish shard ownership, routing, runtime boundaries, and Raft-group placement before moving HTTP, storage, and protocol handlers.

## Current Scope

- Thread-per-core shard ownership model.
- Multi-Raft placement and stream-to-group routing primitives.
- Pure Durable Streams state-machine semantics for core bucket/stream lifecycle and catch-up read rules.
- Thread-per-core and hosted-Tokio shard actor modes for validating ownership and metrics.
- Replaceable group-engine boundary using the stream state machine, ready for a future OpenRaft adapter.
- Optional per-Raft-group WAL/recovery prototype for exercising the same HTTP path with local persistence.
- Minimal HTTP adapter for the `perf_compare`
  create/append/read/head/SSE/mixed subset and runtime metrics inspection.
- Runtime ecosystem evaluation, especially Tokio/axum versus monoio tradeoffs.
- Migration notes from the existing `riverrun` implementation.
- Benchmark-driven CPU saturation target for `perf_compare`.

## Commands

```bash
just build
just test
just fmt-check
just clippy
```

Run the current HTTP prototype:

```bash
cargo run -p ursula-http --bin ursula-http -- \
  --listen 127.0.0.1:4437 \
  --core-count 8 \
  --raft-group-count 128
```

Run the same HTTP prototype with the WAL-backed group engine:

```bash
cargo run -p ursula-http --bin ursula-http -- \
  --listen 127.0.0.1:4437 \
  --core-count 8 \
  --raft-group-count 128 \
  --wal-dir ./data/wal
```

Run the HTTP prototype through OpenRaft with an in-memory Raft log store:

```bash
cargo run -p ursula-http --bin ursula-http -- \
  --listen 127.0.0.1:4437 \
  --core-count 8 \
  --raft-group-count 128 \
  --raft-memory
```

## Performance Target

The migration is successful only when Ursula can use the available CPU under
`perf_compare` write and mixed workloads. The current `riverrun` implementation
is known to plateau at roughly three to four busy cores, which indicates a
global serialization point or shared contention. See
`docs/migration/perf-compare-cpu-saturation.md` for the acceptance gate.

## EC2 Operations Helper

The migration EC2 deployment loop is captured in `scripts/ursula_ec2.py` instead
of one-off shell snippets. It can start/stop a static multi-Raft cluster, wait
for group leaders, inspect metrics, run a configured `perf_compare` client, and
clean an S3 cold-root prefix from a JSON manifest.

See `docs/operations/ec2-static-cluster.md` for the manifest shape and commands.

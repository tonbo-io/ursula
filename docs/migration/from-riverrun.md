# Migration From Riverrun

## Starting Point

The existing `riverrun` implementation already contains the protocol surface, Raft state machine semantics, append coalescing pipeline, hot/cold tiering, and operational tooling. It is also deeply integrated with Tokio, axum, tonic, reqwest, and shared `AppState` handles.

The new `ursula` project should not copy that structure mechanically. The first migration milestone is to define the ownership boundary that the existing code can be moved behind.

## Initial Extraction Candidates

- Durable stream protocol types and validation.
- Stream command/response model.
- Pure state-machine apply logic.
- Offset and retained-message boundary helpers.
- Snapshot/bootstrap semantics.
- Append batching frame parser.

## Code That Needs Redesign Before Moving

- Global `AppState` sharing of Raft and state-machine handles.
- Append coalescer worker topology.
- Live-tail watch registry placement.
- Raft gRPC transport tied to tonic.
- Public HTTP server tied to axum's Tokio serve path.
- RocksDB fsync and hot payload persistence scheduling.

## First Concrete Milestone

Implement a Tokio-hosted shard actor layer in `ursula` that can accept synthetic durable stream commands and route them by `BucketStreamId` to a stable shard placement. Once that layer is verified, move real append/read command handling into it.

The first version of this exists in `crates/ursula-runtime`. It is deliberately
small: the default `InMemoryGroupEngine` should be replaced by an OpenRaft
adapter only after the mailbox, ownership, error, and metrics contracts are
stable.

The first pure stream-semantic slice exists in `crates/ursula-stream`. It covers
bucket and stream lifecycle invariants that can be tested without HTTP, storage,
or Raft. It is intentionally smaller than `riverrun`'s full
`stream::StreamCommand` surface. It now includes a deterministic
serde-compatible snapshot/restore core for buckets, stream metadata, payloads,
stream sequence state, visible protocol snapshots, retained message boundaries,
and producer state. It also covers the first idempotent producer path:
duplicate retry deduplication for single appends and append-batch, sequence gap
rejection, stale epoch fencing, and duplicate final append success after close.
Protocol snapshot/bootstrap is now routed through the same group-owned state
boundary; the remaining correctness gate is an official compatibility rerun
after that extension work.

`crates/ursula-runtime` now uses that stream-semantic slice in the default
in-memory group engine. Synthetic runtime append tests must create streams
first. Catch-up reads, close-only, and HEAD-style metadata paths route through
the same owning group. This matches the `perf_compare` setup path and avoids a
separate runtime-only append/read state machine.

The runtime also exposes a group snapshot boundary. `ShardRuntime::snapshot_group`
routes by `RaftGroupId` to the owner core and returns the group-local commit
index, stream snapshot, and per-stream append counts. `ShardRuntime::install_group_snapshot`
validates that the recorded placement still matches the configured shard map
before routing install through the owner core. This is not OpenRaft snapshot
transport yet, but it keeps snapshot generation and installation inside the
same shard-owned group boundary that the future OpenRaft adapter should use.

The runtime also has an optional `WalGroupEngineFactory` that records stream
commands per Raft group and replays them after restart. It is a recovery
boundary prototype for the new ownership model, not the final OpenRaft log or
storage layout. HTTP append-batch reaches a group-level append-batch method, so
the WAL prototype can write a batch of records before one group-local sync.
`ursula-http --wal-dir DIR` runs the HTTP adapter against this WAL-backed engine
so perf-shaped HTTP traffic can exercise the recovery path when needed.

`crates/ursula-http` is the first HTTP adapter over this runtime. It covers the
ordinary `perf_compare` create, post-append, append-batch, catch-up read, HEAD,
long-poll, SSE live-tail, and stream delete paths, but does not yet cover
OpenRaft bootstrap orchestration, retention, or OpenRaft snapshot transport.
Append, close-only, and append-batch requests parse producer headers for the
prototype idempotent producer path, echo producer epoch/sequence on success, and
return expected/received sequence or current epoch headers on producer
conflicts. Append-batch duplicate retries return the stored per-item offsets
without appending retry bytes.

# JSON Record Coordinates Design Validation

Status: Phase 0 decision and executable-model gate

Normative proposal: PR #85

Historical proposal: issue #84

Timestamp decision: issue #86

Excluded transport work: issue #87

## Gate

Production data-path implementation starts only after the reference model and conformance vectors in `crates/ursula-stream/tests/record_coordinates_reference.rs` pass. The model is deliberately independent of Ursula's state machine so later HTTP and persistence tests can compare implementation behavior with a small oracle rather than restating implementation details.

The gate proves these design properties:

1. Only `application/json` streams activate `json-record-coordinates-v1`.
2. JSON normalization defines the records: a non-array value is one record, a top-level array is flattened exactly once, and an empty create array creates no records while an empty append array fails atomically.
3. Every successful append receives one contiguous ordinal range in committed order.
4. Every ordinal resolves to the same logical boundary as one canonical opaque offset.
5. A deduplicated retry returns its original range without assigning new ordinals or mutating canonical bytes.
6. Record-aligned reads return complete canonical NDJSON messages and matching record/offset continuations.
7. Retention advances the first retained ordinal without renumbering or moving surviving boundaries.
8. Client event time may be out of order and never changes committed bytes, offsets, or ordinals.

The persistence phase must extend the same oracle with snapshot, WAL, cold-flush, compaction, restart, bootstrap, and deterministic-failure cases before those paths are considered conformant.

## Timestamp decision

Record Coordinates v1 has no server commit timestamp. For browser telemetry, the one authoritative timestamp is the client-captured event time stored in the JSON value, conventionally as `captured_at` or `captured_at_ms`.

Issue #86 considered four choices against the actual #85 baseline:

| Option | Exact event time | Native timestamp seek | Offline/backfill | Concurrent producers | Extra DS state |
| --- | --- | --- | --- | --- | --- |
| A. Keep #85 without time indexing | Yes, in JSON | No | Safe | Safe | None |
| B. Clamp client time monotonically | Only if also duplicated in JSON | Yes | Loses ordering information through clamping | Safe after commit serialization | Per-record timestamp index |
| C. Reject decreasing time | Yes for accepted records | Yes | Fragile | Fragile | Per-record timestamp index |
| D. External event-time index contract | Yes, in JSON | Through derived index | Safe | Safe | None in the DS data plane |

Option D is selected. Option B creates a second derived timestamp that is not the original event time, so applications still need `captured_at` in JSON. Option C rejects valid offline, retry, and multi-producer telemetry. Option A preserves a clean protocol but provides no portable time-query integration.

## External index boundary

The external index is derived state, not another Durable Streams coordinate:

- Ursula remains canonical for bytes, offsets, record ordinals, retention, and replay.
- An indexer consumes records in ordinal order and extracts the client timestamp from application JSON.
- Event timestamps may be out of order and do not reorder the source stream.
- The index publishes `indexed_through_record`, the exclusive ordinal through which all retained source records have been processed.
- A time query returns record ordinals or coalesced ordinal ranges; callers read canonical data from Ursula by record coordinate.
- Equal timestamps are ordered by record ordinal in query results.
- The index must report when its answer is incomplete because source retention advanced beyond its rebuild point.
- The index can be deleted and rebuilt from retained Ursula records without changing the stream.

This contract will be validated in the browser telemetry example. It is intentionally ordinary HTTP and does not depend on the Append Session proposal in #87 or the Table Streams proposal in #81.

## Implementation acceptance

The production implementation is accepted only when the shared vectors are exercised at the HTTP boundary and recovery tests prove that the state machine, Raft log, snapshots, cold storage, compaction, and restart preserve the reference model's observable state. Performance evidence must report index bytes per record plus representative append, record-seek, and record-aligned-read latency.

## Performance evidence

The in-memory ordinal index stores one `u64` canonical start offset per retained record: 8 bytes per record, plus one 24-byte `Vec` descriptor per indexed stream and allocator capacity overhead. Snapshot and WAL serialization use the same one-offset-per-record representation.

`cargo bench -p ursula-runtime --bench append_apply -- record_coordinates` reports three representative operations against the production state-machine structures: JSON record append, exact seek in a 100,000-record stream, and a record-aligned 100-record read plan. Benchmark results are environment-specific and should be attached to the implementation PR rather than frozen into the protocol document.

## Implementation audit

| Contract | Production path | Evidence |
| --- | --- | --- |
| Activation and scoped capability advertisement | JSON create, append, read, HEAD, live, snapshot, and bootstrap responses | HTTP JSON-mode and snapshot tests; non-JSON compatibility suite |
| Atomic normalization and committed ranges | Canonical NDJSON boundaries feed the replicated record index | Reference vectors; HTTP array and concurrent-append tests |
| Idempotent ranges and record-tail preconditions | Producer state persists original ranges; `Stream-Record-Match` is checked in the state owner | HTTP retry/precondition tests; WAL recovery after retention |
| Exact record seek and aligned reads | Owner resolves ordinal to canonical offset and caps reads at complete boundaries | Reference vectors; catch-up, tail, max-records, and envelope tests |
| Live continuation | Long-poll and SSE carry record cursors; envelope SSE emits one record per data event | Live wake/reconnect HTTP test; deterministic HTTP simulation |
| Retention, snapshots, and bootstrap | JSON logical message boundaries equal record boundaries; snapshot/bootstrap expose retained range | State snapshot/cold-flush test; HTTP snapshot/bootstrap test |
| Raft, protobuf, WAL, and restart | Record starts and producer ranges are encoded in durable commands and snapshots | Codec/workspace tests; focused WAL restart test; madsim replay |
| Append Batch | Each normalized frame receives its own range and deduplicated frames recover saved ranges | HTTP and runtime batch tests |
| Client event time | `captured_at` remains JSON data; `ursula-indexer` persists content-addressed Parquet parts and versioned manifests in S3, with only a disposable local cache | Browser telemetry example; restart with an empty cache, concurrent-writer CAS, pagination, compaction, and retention-gap tests |
| Base compatibility | Offsets remain opaque and ordinary offset reads remain byte-oriented | Existing Durable Streams HTTP suite and partial-offset JSON test |

Append Session (#87) and Table Streams (#81) are intentionally outside this audit and implementation.

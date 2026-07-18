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

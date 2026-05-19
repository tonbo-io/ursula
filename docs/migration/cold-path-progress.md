# Cold Path Progress

## Direction

Cold path changes are structural, not a compatibility patch. The durable stream
engine should keep Raft apply deterministic and fast: S3 upload happens outside
the state-machine apply path, and Raft only publishes metadata for bytes that
already reached cold storage.

The current boundary is:

1. Owning group plans one or more bounded hot-prefix flush candidates.
2. Runtime uploads candidate bytes to cold storage outside the group actor.
3. Runtime submits `FlushCold` or a group-local `Batch` of `FlushCold`
   commands to publish chunk refs and compact the hot prefix.
4. Reads use the committed manifest to reassemble cold segments plus remaining
   hot bytes.

This matches the riverrun state-machine rule that `FlushToS3` is a metadata
publish; bytes are uploaded before the command is emitted.

## Implemented

- `crates/ursula-stream`
  - Reuses shared durable schema types from `crates/ursula-proto` for
    `ProducerRequest`, `ExternalPayloadRef`, and `ColdChunkRef`, so stream
    semantics, runtime cold-path metadata, and Raft app-log commands no longer
    maintain separate copies of these persistent/protocol structs.
  - Added `ColdChunkRef`, `ColdFlushCandidate`, `StreamReadPlan`, and cold
    segment planning.
  - Added `StreamCommand::FlushCold` and `StreamResponse::ColdFlushed`.
  - Added `hot_start_offset` and `cold_chunks` to snapshots.
  - `FlushCold` validates contiguous hot payload publication, advances the hot
    start offset, drains the hot payload range, and keeps apply pure.
  - `FlushCold` can coalesce multiple contiguous hot segments into one cold
    object, so background flush is not forced into one object per append.
  - Cold flush planning also coalesces contiguous hot segments before applying
    `min_hot_bytes`. This matters for append-batch/small-event workloads where
    each frame is recorded as a small retained message; without planner-side
    coalescing, a stream with many 128-byte hot segments never reached a
    64 KiB or 1 MiB candidate threshold even though the group hot-byte cap was
    full.
  - Added deterministic group-local candidate selection via
    `plan_next_cold_flush`.

- `crates/ursula-runtime`
  - Added opendal-backed `ColdStore` with `memory`, `fs`, and `s3` modes.
  - `URSULA_COLD_BACKEND=s3` uses `URSULA_COLD_S3_BUCKET` plus optional region,
    endpoint, root, access key, secret key, and session token.
  - Read path reassembles committed cold segments from `ColdStore` and appends
    remaining hot bytes.
  - `ColdStore::read_chunk_range` uses object range reads so catch-up reads do
    not fetch full cold objects when the requested slice is small.
  - Added `plan_cold_flush` and `flush_cold_once`; upload is outside the group
    actor, then `FlushCold` is committed through the owner group.
  - Added `plan_next_cold_flush`, `plan_next_cold_flush_batch`,
    `flush_cold_group_once`, and `flush_cold_all_groups_once` so background
    offload scans Raft groups instead of reading stream state through a global
    registry.
  - Background group flush can now plan multiple candidates on a preview state,
    upload them outside apply, and publish their metadata as a single
    group-owned Raft `Batch`. This removes the durable-log fsync amplification
    that came from committing one cold metadata update per tiny chunk.
  - Added `flush_cold_all_groups_once_bounded` so the runtime can cap concurrent
    group flush uploads while still scanning all groups.
  - Added cold upload, metadata publish, and orphan cleanup counters to runtime
    metrics.
  - If metadata publish fails after an upload, runtime attempts to delete the
    uploaded object and records cleanup attempts/errors plus orphan bytes.
  - Added group-owned cold write admission. When configured, create/append/
    append-batch preview the post-write group hot bytes inside the group engine
    and reject new bytes that would exceed the per-group hot-byte limit.
  - Added current and high-watermark hot-byte gauges plus cold backpressure
    counters to runtime metrics.
  - Group-local background flush skips soft-deleted streams and treats stale
    publish candidates for deleted streams as non-fatal after object cleanup, so
    a deleted/forked stream does not pin worker progress for its Raft group.
  - Added a steady-state regression for repeated append/flush cycles across
    multiple Raft groups. It keeps accepting new writes, flushes one bounded
    batch per group, asserts `cold_hot_bytes` returns to zero after each round,
    verifies no cold backpressure or orphan cleanup events occurred, and reads
    the complete cold-backed stream contents after all rounds.

- `crates/ursula-raft`
  - Added a shared `ursula-proto` crate for durable app-log/public persistent
    schema. `FlushCold` is now represented in `RaftGroupCommandV1` through the
    local OpenRaft wrapper `RaftGroupCommand`, rather than a private
    `raft_internal_proto` command copy.
  - OpenRaft applies cold metadata through the same committed-write path.
  - Cold-aware reads work when the group state machine is created with a cold
    store handle.

- `crates/ursula-http`
  - Runtime constructors wire `ColdStore::from_env()` into default, WAL,
    in-memory OpenRaft, and durable OpenRaft modes.
  - Added `POST /__ursula/flush-cold/{bucket}/{stream}` for explicit single
    stream flush testing.
  - When a cold store is configured, starts an optional background flush worker.
    Defaults:
    - `URSULA_COLD_FLUSH_INTERVAL_MS=1000`
    - `URSULA_COLD_FLUSH_MIN_HOT_BYTES=8388608`
    - `URSULA_COLD_FLUSH_MAX_BYTES=8388608`
    - `URSULA_COLD_FLUSH_MAX_CONCURRENCY=4`
    - `URSULA_COLD_MAX_HOT_BYTES_PER_GROUP=67108864`
    - set `URSULA_COLD_FLUSH_INTERVAL_MS=0` to disable the worker.
    - set `URSULA_COLD_MAX_HOT_BYTES_PER_GROUP=0` to disable cold write
      admission.
  - `GET /__ursula/metrics` exposes total cold upload/publish/orphan counters,
    per-group hot-byte gauges, hot-byte high-water marks, and cold backpressure
    counters.
  - Live long-poll/SSE waiter admission is bounded per owner core through
    `URSULA_LIVE_READ_MAX_WAITERS_PER_CORE` (default `65536`; `0` disables the
    limit for explicit experiments). Excess waiters return `503` and increment
    total/per-core live-read backpressure metrics.

- `crates/ursula-runtime/tests`
  - Added a gated real-S3 integration test. It requires
    `URSULA_COLD_S3_INTEGRATION=1` and `URSULA_COLD_S3_BUCKET`; without that
    explicit opt-in it reports a skip and returns successfully.
  - The test writes hot bytes, flushes a prefix to S3, reads cold+hot bytes back
    through runtime, checks cold metrics, discovers the actual object path from
    a group snapshot, and deletes the uploaded test object.

## Verification

From `/Users/xing/Idea/ursula`:

```bash
cargo test -p ursula-stream --lib flush_cold_moves_hot_prefix_to_manifest_and_read_plan_splits
cargo test -p ursula-stream --lib flush_cold_can_coalesce_contiguous_hot_segments
cargo test -p ursula-stream --lib plan_cold_flush_coalesces_contiguous_hot_segments
cargo test -p ursula-stream --lib plan_next_cold_flush_selects_deterministic_eligible_stream
cargo test -p ursula-stream --lib plan_next_cold_flush_batch_advances_on_preview_state
cargo test -p ursula-stream --lib hot_payload_byte_metrics_follow_cold_flush -- --nocapture
cargo test -p ursula-runtime --lib cold -- --nocapture
cargo test -p ursula-runtime flush_cold_group_batch_once_publishes_multiple_chunks -- --nocapture
cargo test -p ursula-runtime repeated_cold_flush_keeps_hot_bytes_bounded_while_writes_continue -- --nocapture
cargo test -p ursula-runtime --lib cold_write_admission -- --nocapture
cargo test -p ursula-runtime --test s3_cold_path -- --nocapture
cargo test -p ursula-http --lib metrics_expose_per_core_and_group_append_distribution -- --nocapture
cargo test -p ursula-http --lib cold_backpressure_returns_service_unavailable_and_metrics -- --nocapture
cargo test -p ursula-http --lib flush_cold_endpoint_uploads_and_reads_back_segments
cargo check --workspace --all-targets
cargo fmt --all -- --check
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
```

## EC2 cold-enabled perf_compare finding

On 2026-05-18, a three-node EC2 static gRPC cluster with durable OpenRaft logs
and real S3 cold storage exposed a planner-level root cause under
`perf_compare` small-event load.

Before planner coalescing, node 1 accepted exactly 2,097,152 128-byte appends
across 64 Raft groups, reached `cold_hot_bytes=268435456`, and hit the
4 MiB per-group hot cap. Background flush did not upload any chunks for
90 seconds:

```text
cold_hot_bytes=268435456
cold_hot_group_bytes_max=4194304
cold_backpressure_events=49637
cold_flush_uploads=0
cold_flush_publishes=0
```

Lowering `URSULA_COLD_FLUSH_MIN_HOT_BYTES` from 1 MiB to 64 KiB did not help,
because every append-batch frame was a separate 128-byte hot segment and the
planner considered only the first segment. A manual
`POST /__ursula/flush-cold/benchcmp/{stream}?min_hot_bytes=1` proved S3 upload
and metadata publish still worked, but it only advanced the stream by one
128-byte segment.

After changing `StreamStateMachine::plan_cold_flush()` to coalesce contiguous
hot segments before thresholding, the same EC2 shape with binary sha256
`50ad58c2ba3da5a6e8230322ef0cac05efad8fc7f5fda928c11f9e8685e9d33b` completed
a 30-second `perf_compare` run with zero 503s:

```text
write_throughput: ok_requests=2219776 errors=0 requests_per_sec=73492.38
small_event_write: ok_requests=1064240 errors=0 requests_per_sec=35135.73
```

Immediate post-run node 1 metrics showed active cold offload and bounded hot
memory:

```text
accepted_appends=3284016
active_cores=16
active_groups=64
cold_hot_bytes=25194496
cold_hot_group_bytes_max=1558528
cold_backpressure_events=0
cold_flush_uploads=4919
cold_flush_upload_bytes=395159552
cold_flush_publishes=4919
wal_batches=414428
wal_records=414428
mailbox_full_events=0
```

S3 contained 4,919 chunks totaling 395,159,552 bytes before cleanup. The
remaining hot bytes stayed below the 64 KiB candidate threshold per stream and
well below the 4 MiB per-group cap.

The same binary was also run on the same three-node EC2 static gRPC cluster with
`--raft-memory` and `URSULA_COLD_BACKEND=memory` to isolate durable log and S3
IO from the multi-Raft/runtime path. With 64 groups, 16 owner cores, 128-byte
payloads, append-batch size 16, and minimal acks, the 30-second write/small run
completed with zero errors:

```text
write_throughput: ok_requests=5626752 requests_per_sec=187280.41
small_event_write: ok_requests=4902592 requests_per_sec=163039.77
```

The following 60-second mixed run also completed with zero errors:

```text
mixed append: ok_requests=5971520 requests_per_sec=99525.33
mixed read: ok_requests=261178 requests_per_sec=4352.97
mixed SSE p99=54.04ms
```

Final metrics showed diskless Raft and active memory cold offload:

```text
accepted_appends=16501092
active_cores=16
active_groups=64
wal_batches=0
wal_records=0
cold_hot_bytes=36364800
cold_hot_group_bytes_max=1399808
cold_backpressure_events=0
cold_flush_uploads=24686
cold_flush_upload_bytes=1908572416
cold_flush_publishes=24686
mailbox_full_events=0
```

Compared with the durable-log/S3 run above, the in-memory backend was roughly
2.5x faster for write throughput, 4.6x faster for small-event write throughput,
and 1.8x faster for mixed appends. The memory backend therefore confirms that
the cold planner no longer blocks progress, while durable log and S3 IO remain
large contributors to the current cold-enabled EC2 performance gap.

A follow-up run used `--raft-memory` with `URSULA_COLD_BACKEND=s3` to isolate
the S3 cold path without local Raft WAL. Four independent client processes used
separate buckets and append-batch load against the same three-node static gRPC
cluster. The aggregate accepted rate was about 128k 128-byte events/s. Node 1
was near CPU saturation, while node 2, node 3, and the client host remained
mostly idle. During the 45-second window, node 1 uploaded 665,104,384 cold
bytes, about 14.8 MB/s, with zero cold backpressure and zero WAL records. After
another 30 seconds of background flushing it had uploaded 811,112,448 bytes and
still retained 67,092,480 hot bytes.

That result means the current in-memory-Raft/S3 path does not hit S3 bandwidth
first. The earlier 4 MiB per-group cap can create artificial 503s if cold flush
falls behind, but once that cap is raised for capacity testing the first
observed limit is leader-node CPU and ingress/cold-planning concentration, not
S3 throughput.

After adding `--raft-init-membership-per-group`, the same in-memory-Raft/S3
capacity shape was rerun with leaders distributed across all three EC2 server
nodes. Four client processes with unique buckets reached about 229k 128-byte
events/s with zero errors, zero cold backpressure, zero mailbox-full events, and
zero WAL records. Cold upload work was distributed across the three nodes:

```text
node1 cold_flush_upload_bytes=434958336
node2 cold_flush_upload_bytes=429983744
node3 cold_flush_upload_bytes=387004416
```

The aggregate S3 upload rate was still only about 27.8 MB/s, so the S3 backend
was not saturated. The meaningful improvement is structural: cold planning and
upload are no longer pinned to node 1 when static group leaders are distributed.

All commands passed on 2026-05-17.

The gated real-S3 integration also passed against the existing
`riverrun-e2e-us-east-1` bucket. The local run used AWS SSO credentials exported
as temporary explicit S3 credentials because opendal did not consume the SSO
profile directly:

```bash
URSULA_COLD_S3_INTEGRATION=1 \
URSULA_COLD_BACKEND=s3 \
URSULA_COLD_S3_BUCKET=riverrun-e2e-us-east-1 \
URSULA_COLD_S3_REGION=us-east-1 \
URSULA_COLD_ROOT=codex/ursula-cold-path-test \
cargo test -p ursula-runtime --test s3_cold_path -- --nocapture
```

```text
test s3_cold_path_flushes_reads_and_cleans_up_object ... ok
```

The same gated test passed on EC2 after syncing the current Ursula checkout to
`ursula-c7g-beast-node-1` (`c7g.4xlarge`, aarch64) and running against the same
bucket with an isolated root:

```bash
URSULA_COLD_S3_INTEGRATION=1 \
URSULA_COLD_BACKEND=s3 \
URSULA_COLD_S3_BUCKET=riverrun-e2e-us-east-1 \
URSULA_COLD_S3_REGION=us-east-1 \
URSULA_COLD_ROOT=codex/ursula-cold-path-test-ec2 \
cargo test -p ursula-runtime --test s3_cold_path -- --nocapture
```

```text
test s3_cold_path_flushes_reads_and_cleans_up_object ... ok
```

The official Durable Streams in-memory OpenRaft conformance suite also passed
after the cold-path range-read, bounded flush, metrics, orphan cleanup, and
write-admission work:

```text
Tests  300 passed (300)
```

The same official suite also passed with the memory cold store enabled, an
aggressive background flush configuration, and log verification that the
background worker did not emit stale `StreamGone`/`StreamNotFound` errors:

```bash
URSULA_COLD_BACKEND=memory \
URSULA_COLD_FLUSH_INTERVAL_MS=1 \
URSULA_COLD_FLUSH_MIN_HOT_BYTES=1 \
URSULA_COLD_FLUSH_MAX_BYTES=1 \
URSULA_COLD_FLUSH_MAX_CONCURRENCY=4 \
URSULA_COLD_MAX_HOT_BYTES_PER_GROUP=67108864 \
cargo run -p ursula-http --bin ursula-http -- \
  --listen 127.0.0.1:4478 \
  --core-count 4 \
  --raft-group-count 32 \
  --raft-memory

CONFORMANCE_TEST_URL=http://127.0.0.1:4478 \
pnpm exec vitest run --project server \
  packages/server/test/ursula-conformance.test.ts \
  --no-coverage --reporter=dot
```

```text
Tests  300 passed (300)
```

The current checkout was rerun against the same official suite after the static
multi-Raft transport switched to tonic gRPC and `FlushCold` was extended to
coalesce contiguous hot segments. The run again passed `300 / 300` with the
memory cold store enabled. Runtime metrics after the run showed the background
cold path was active:

```text
cold_flush_uploads=15034
cold_flush_publishes=15034
```

The EC2 static-cluster S3 smoke has now also passed with the current tonic gRPC
internal Raft transport. It ran the same `ursula-http` binary on three
`c7g.4xlarge` nodes with a `c7gn.8xlarge` client, used port `4477`, and pointed
all nodes at `URSULA_COLD_BACKEND=s3` with bucket
`ursula-c7g-beast-us-east-1`. The smoke verified follower write redirect to
node 1, leader create/append commit through gRPC quorum replication, redirected
readback from node 3, background S3 flush, `cold_flush_uploads=5`,
`cold_flush_publishes=5`, `cold_hot_bytes=0`, five S3 chunks under the smoke
root, and post-flush S3-backed readback. Temporary `4477` processes and the S3
smoke objects were cleaned up. Details are in
`docs/migration/ec2-static-cluster-s3-smoke.md`.

The official Durable Streams suite also passed from the EC2 `c7gn.8xlarge`
client against the same static gRPC cluster shape, with S3 enabled and root
`ursula-grpc-conformance/20260518T034603Z`:

```text
Tests  300 passed (300)
Duration  20.36s
```

That run used the S3 backend for the official large-payload case, producing one
10 MiB external object. Background flush did not run in that conformance run
because the remaining hot backlog was below the 1 MiB flush threshold. The
temporary `4477` processes and S3 object were cleaned up.

The local OpenRaft snapshot-transfer gap is now covered by
`openraft_installs_snapshot_for_lagging_learner`. The test snapshots and purges
a two-voter leader, adds a previously empty third node as a learner, verifies
that replication invokes `RaftNetworkV2::full_snapshot`, and then reads the
stream from the learner's installed snapshot. This uncovered a real bug: the
leader snapshot builder advanced OpenRaft's snapshot metric but did not retain
the snapshot bytes for `get_current_snapshot`, so later snapshot transmission
failed with `snapshot not found`. The builder now stores the generated
`CurrentSnapshot` as part of `build_snapshot`.

The same snapshot-transfer path now also has local TCP router coverage in
`static_grpc_raft_installs_snapshot_for_late_learner_over_tcp`. That test starts
a two-node static gRPC Ursula cluster, writes through the leader HTTP API,
snapshots and purges the leader, starts a third HTTP router late, adds it as an
OpenRaft learner with its gRPC address, waits for snapshot installation, and
then reads the restored stream through the late learner's HTTP endpoint.

`GET /__ursula/metrics` now includes read-only OpenRaft group metrics from the
runtime-owned `RaftGroupHandleRegistry`: `raft_group_count`, one `raft_groups`
entry per registered group, node id, current term/leader, log frontiers,
snapshot index/term, purged index/term, voters, and learners. The local TCP
late-learner snapshot test asserts those metrics after leader snapshot/purge
and after the late learner installs the snapshot. This gives the next EC2 or
restart-level exercise direct evidence for snapshot and purge progress instead
of treating a successful read as the only proxy signal.

The aggressive official conformance run also exposed a stale cold-flush
candidate race in the background worker: a candidate can be planned and uploaded,
then the stream can be hard-deleted and recreated before metadata publish. If
the recreated stream has a shorter tail, publish returns `InvalidColdFlush` with
the candidate end beyond the current tail. That is a stale candidate, not a
protocol-visible failure. The runtime now classifies that invalid flush family
as stale for cleanup, deletes the just-uploaded object, and continues. Local
regression tests cover both layers:

- `stale_cold_flush_candidate_after_delete_recreate_is_invalid_without_mutation`
  proves the stream state machine rejects the old candidate without mutating the
  recreated stream.
- `stale_cold_flush_batch_after_delete_recreate_is_classified_for_cleanup`
  proves runtime batch publish classifies the error as stale, records one orphan
  cleanup attempt, and leaves the recreated stream readable.

The official suite was rerun after this fix with local `--raft-memory`, memory
cold store, and aggressive 1-byte background flush. It passed `300 / 300` in
17.01s. Metrics showed `cold_flush_uploads=112883`,
`cold_flush_publishes=112867`, `cold_orphan_cleanup_attempts=16`,
`cold_orphan_cleanup_errors=0`, `cold_backpressure_events=0`, and no cold worker
error printed during shutdown.

## Remaining Work

- Keep official Durable Streams conformance as the protocol gate. The official
  suite now covers cold-aware behavior with the memory cold store, but it still
  does not exercise AWS S3 because the upstream test matrix has no external S3
  fixture. The gated integration above covers AWS credentials, bucket policy,
  network, object upload, range read, and cleanup for the current S3 store
  boundary.
- Consider per-core/per-group cold flush upload/publish metrics if cold offload
  itself becomes a CPU-saturation diagnostic bottleneck. Current upload/publish
  metrics are total counters; hot-byte and backpressure metrics are per group.

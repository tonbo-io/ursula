# Official Durable Streams Conformance

## Source

The compatibility gate is the official Durable Streams repository:

- `https://github.com/durable-streams/durable-streams`
- Local checkout used for this audit: `/tmp/durable-streams-official`
- Commit: `8d78524`

The upstream package CLI did not run cleanly from the monorepo checkout because
Vitest excluded the built `dist/test-runner.js` path. For local iteration, a
thin wrapper was added under the official checkout:

```ts
import { runConformanceTests } from "../../server-conformance-tests/src/index"

const baseUrl = process.env.CONFORMANCE_TEST_URL

if (!baseUrl) {
  throw new Error("CONFORMANCE_TEST_URL is required")
}

runConformanceTests({ baseUrl })
```

## Current Command

Run Ursula in the in-memory OpenRaft path:

```bash
cargo run -p ursula-http --bin ursula-http -- \
  --listen 127.0.0.1:4477 \
  --core-count 4 \
  --raft-group-count 32 \
  --raft-memory
```

Run the official conformance wrapper from `/tmp/durable-streams-official`:

```bash
CONFORMANCE_TEST_URL=http://127.0.0.1:4477 \
  pnpm exec vitest run \
  --project server \
  packages/server/test/ursula-conformance.test.ts \
  --no-coverage \
  --reporter=dot
```

## Current Result

Latest verified result on the current Ursula checkout after the internal Raft
transport moved to tonic gRPC and `FlushCold` learned to coalesce contiguous hot
segments:

```text
Tests  300 passed (300)
```

The latest current-code run used the official checkout at `origin/main`
`8d78524`, after the shared `ursula-proto` app-log schema work and serde
boundary cleanup. Ursula ran in the in-memory OpenRaft path plus the memory
cold store and aggressive background flush:

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
Test Files  1 passed (1)
Tests  300 passed (300)
Duration  16.65s
```

The post-run metrics confirmed the cold background path was active during the
suite:

```text
cold_flush_uploads=11732
cold_flush_publishes=11731
cold_flush_upload_bytes=11732
cold_flush_publish_bytes=11731
cold_orphan_cleanup_attempts=1
cold_orphan_cleanup_errors=0
```

The suite was rerun after the cold-admission Raft proposal coalescing change,
the stale cold-flush candidate cleanup fix, and the logical Raft write metric.
The same local `--raft-memory`, memory cold store, and aggressive 1-byte
background flush shape passed:

```text
Test Files  1 passed (1)
Tests  300 passed (300)
Duration  16.86s
```

Post-run metrics showed active background flushing and stale-candidate cleanup
without exposing worker errors. The difference between outer
`raft_write_many_commands` and `raft_write_many_logical_commands` confirms that
cold metadata publishes were being coalesced into Raft `Batch` commands:

```text
accepted_appends=1055
applied_mutations=114290
wal_batches=0
wal_records=0
raft_write_many_batches=2279
raft_write_many_commands=2279
raft_write_many_logical_commands=112468
cold_flush_uploads=112468
cold_flush_publishes=112447
cold_hot_bytes=47
cold_backpressure_events=0
cold_orphan_cleanup_attempts=21
cold_orphan_cleanup_errors=0
mailbox_full_events=0
```

The suite was rerun again after the read-plan/cold-payload split moved payload
materialization out of the group actor turn and bounded spawned materializers
with a runtime semaphore. The same local `--raft-memory`, memory cold store,
and aggressive 1-byte background flush shape passed against the official
checkout `8d78524`:

```text
Test Files  1 passed (1)
Tests  300 passed (300)
Duration  17.79s
```

Post-run metrics showed the cold background path remained active, no hot/cold
backpressure was exposed, and no mailbox filled while the official suite ran:

```text
accepted_appends=1050
applied_mutations=64618
active_cores=4
active_groups=32
wal_batches=0
wal_records=0
raft_write_many_batches=1513
raft_write_many_commands=1513
raft_write_many_logical_commands=62784
cold_flush_uploads=62784
cold_flush_publishes=62780
cold_hot_bytes=49903
cold_backpressure_events=0
cold_orphan_cleanup_attempts=4
cold_orphan_cleanup_errors=0
mailbox_full_events=0
group_mailbox_full_events=0
live_read_backpressure_events=0
```

The same official suite also passed locally against the durable OpenRaft
file-log path. Ursula ran from a debug binary with `--raft-log-dir`, memory cold
store configured, and background cold flush disabled so this isolates protocol
correctness through durable OpenRaft log persistence:

```bash
CARGO_TARGET_DIR=/tmp/ursula-conformance-target \
cargo build -p ursula-http --bin ursula-http

URSULA_COLD_BACKEND=memory \
URSULA_COLD_FLUSH_INTERVAL_MS=0 \
URSULA_COLD_MAX_HOT_BYTES_PER_GROUP=67108864 \
/tmp/ursula-conformance-target/debug/ursula-http \
  --listen 127.0.0.1:4480 \
  --core-count 4 \
  --raft-group-count 32 \
  --raft-log-dir /tmp/ursula-conformance-raft-log-noflush

CONFORMANCE_TEST_URL=http://127.0.0.1:4480 \
pnpm exec vitest run --project server \
  packages/server/test/ursula-conformance.test.ts \
  --no-coverage --reporter=dot
```

```text
Test Files  1 passed (1)
Tests  300 passed (300)
Duration  34.60s
```

Post-run metrics:

```text
accepted_appends=1009
applied_mutations=1797
active_cores=4
active_groups=32
wal_batches=3836
wal_records=3836
wal_write_ns=295426143
wal_sync_ns=14342304938
raft_apply_entries=1838
raft_apply_ns=46787479
cold_flush_uploads=0
cold_flush_publishes=0
cold_hot_bytes=112159
cold_backpressure_events=0
wal_write_ms_per_batch=0.077
wal_sync_ms_per_batch=3.739
```

The deliberately aggressive combination of `--raft-log-dir` plus 1-byte
background cold flush initially exposed a structural write-amplification limit:
`298 / 300` tests passed and two property-based tests timed out at the official
5 second per-test limit. Metrics from that run showed 23,300 durable log
batches/records, about 60.9s aggregate log write time, and about 148.5s
aggregate sync time.

After changing background cold flush to plan multiple group-local candidates on
a preview state, upload them outside apply, and publish their metadata through a
single Raft `Batch` command, the same debug-mode durable file-log run passed:

```text
Test Files  1 passed (1)
Tests  300 passed (300)
Duration  47.02s
```

Post-run metrics:

```text
accepted_appends=1069
applied_mutations=107003
wal_batches=9072
wal_records=9072
wal_write_ns=10729099756
wal_sync_ns=34999994990
raft_apply_entries=4455
raft_apply_ns=677276327
raft_write_many_batches=2557
raft_write_many_commands=2557
raft_write_many_responses=2557
cold_flush_uploads=105214
cold_flush_publishes=105146
cold_hot_bytes=7408
cold_backpressure_events=0
wal_write_ms_per_batch=1.183
wal_sync_ms_per_batch=3.858
cold_publish_ms_per_publish=0.420
```

This keeps the stress case intentionally tiny-chunked while removing the
one-Raft-fsync-per-cold-chunk shape.

The same durable file-log plus aggressive cold-flush conformance shape was
rerun after adding the narrow Raft admin endpoints used for EC2 snapshot
validation (`/__ursula/raft/{group}/snapshot`, `purge`, and `learners/{node}`).
Those endpoints are outside the public Durable Streams protocol surface, and
the official suite still passed:

```text
Test Files  1 passed (1)
Tests  300 passed (300)
Duration  47.44s
```

Post-run metrics included:

```text
accepted_appends=1079
applied_mutations=114614
active_cores=4
active_groups=32
wal_batches=9308
wal_records=9308
wal_write_ns=11017214493
wal_sync_ns=36202483019
cold_flush_uploads=112751
cold_flush_publishes=112747
cold_hot_bytes=4
cold_backpressure_events=0
```

The local three-process static gRPC multi-Raft path also passes the official
suite when using in-memory OpenRaft logs. This validates the real binary
cluster path rather than a single-process runtime proxy:

```bash
URSULA_COLD_BACKEND=memory \
URSULA_COLD_FLUSH_INTERVAL_MS=0 \
target/debug/ursula-http \
  --listen 127.0.0.1:4481 \
  --core-count 4 \
  --raft-group-count 32 \
  --raft-memory \
  --raft-node-id 1 \
  --raft-peer 1=http://127.0.0.1:4481 \
  --raft-peer 2=http://127.0.0.1:4482 \
  --raft-peer 3=http://127.0.0.1:4483 \
  --raft-init-membership

CONFORMANCE_TEST_URL=http://127.0.0.1:4481 \
pnpm exec vitest run --project server \
  packages/server/test/ursula-conformance.test.ts \
  --no-coverage --reporter=dot
```

```text
Test Files  1 passed (1)
Tests  300 passed (300)
Duration  17.85s
```

Node 1 metrics from that run:

```text
accepted_appends=1036
applied_mutations=1824
mutation_apply_ns=1446717215
group_engine_exec_ns=1581203498
wal_batches=0
wal_records=0
cold_flush_publishes=0
```

The same local three-process static gRPC in-memory Raft-log cluster also passed
with the background cold worker enabled:

```text
Test Files  1 passed (1)
Tests  300 passed (300)
Duration  18.77s
```

That run used `URSULA_COLD_FLUSH_INTERVAL_MS=1`,
`URSULA_COLD_FLUSH_MIN_HOT_BYTES=1`, `URSULA_COLD_FLUSH_MAX_BYTES=1024`, and
`URSULA_COLD_FLUSH_MAX_CONCURRENCY=4`. Node 1 metrics showed the cold path was
active and drained the hot backlog:

```text
accepted_appends=1100
applied_mutations=3319
raft_write_many_batches=899
cold_flush_uploads=1431
cold_flush_publishes=1431
cold_flush_publish_ns=1148259172
cold_hot_bytes=0
wal_batches=0
```

The local three-process static gRPC durable file-log cluster is currently a
latency diagnostic rather than a green conformance gate on this MacBook. With
1-byte background cold flush it reached `295 / 300`; metrics showed
`cold_flush_publishes=109538`, `cold_flush_publish_ns=110437098133`, and
`wal_sync_ns=69546911040`, with one group receiving roughly 99k cold-flush
mutations. With `URSULA_COLD_FLUSH_MAX_BYTES=1024`, failures dropped to
`298 / 300` and cold publishes dropped to `1367`, but `wal_sync_ns` was still
`37249623877`. With the cold worker disabled, only the
`offsets are always monotonically increasing` property test timed out in the
full suite, while the failing property tests passed when run alone. The
evidence points to local file-log fsync/replication latency exceeding several
official property tests' default 5s wrapper, not to a protocol-visible semantic
failure.

The same official suite also passed from the dedicated `c7gn.8xlarge` client
against the EC2 static gRPC cluster on node 1 private IP `10.99.1.48:4477`.
That cluster ran three `c7g.4xlarge` servers, 64 Raft groups, `--raft-memory`,
node 1 `--raft-init-membership`, current tonic gRPC internal Raft transport,
and `URSULA_COLD_BACKEND=s3` with root
`ursula-grpc-conformance/20260518T034603Z`:

```bash
cd /tmp/durable-streams-official

CONFORMANCE_TEST_URL=http://10.99.1.48:4477 \
pnpm exec vitest run --project server \
  packages/server/test/ursula-conformance.test.ts \
  --no-coverage --reporter=dot
```

```text
Test Files  1 passed (1)
Tests  300 passed (300)
Duration  20.36s
```

Post-run node 1 metrics:

```text
accepted_appends=1093
active_cores=16
active_groups=64
cold_flush_uploads=0
cold_flush_publishes=0
cold_hot_bytes=112587
```

The EC2 conformance run used the S3 backend for the official large-payload case:
S3 listed one 10 MiB object under the conformance root. Background flush did not
run because the remaining hot backlog was below the 1 MiB flush threshold. Node
logs had no `error`, `panic`, `StreamGone`, `StreamNotFound`, or
`cold flush worker` lines. The temporary `4477` processes and the S3 object
were cleaned up after the run.

The upstream official suite still does not directly cover the Ursula
snapshot/bootstrap extension endpoints. A local search of
`packages/server-conformance-tests/src` found no `Stream-Snapshot` header,
`/snapshot`, or `/bootstrap` endpoint tests; the `snapshot` mentions there refer
to ordinary catch-up read snapshots during concurrent readers. Ursula carries
local HTTP extension tests for `/snapshot` and `/bootstrap` until an upstream
extension suite exists.

## Per-Group Leader Conformance Follow-Up

After adding `--raft-init-membership-per-group`, a local three-process static
gRPC cluster was rerun against the official suite shape with memory cold flush
enabled:

```bash
URSULA_COLD_BACKEND=memory
URSULA_COLD_FLUSH_INTERVAL_MS=1
URSULA_COLD_FLUSH_MIN_HOT_BYTES=1
URSULA_COLD_FLUSH_MAX_BYTES=1024
URSULA_COLD_FLUSH_MAX_CONCURRENCY=4
URSULA_COLD_MAX_HOT_BYTES_PER_GROUP=67108864
target/debug/ursula-http \
  --listen 127.0.0.1:4491 \
  --core-count 4 \
  --raft-group-count 32 \
  --raft-memory \
  --raft-node-id 1 \
  --raft-peer 1=http://127.0.0.1:4491 \
  --raft-peer 2=http://127.0.0.1:4492 \
  --raft-peer 3=http://127.0.0.1:4493 \
  --raft-init-membership-per-group
```

The first run failed `247 / 300`: single-base-url official clients hit node 1
while many groups had node 2 or node 3 leaders. Returning `307` for write
requests exposed Node/fetch body replay issues and redirect loops around fork
creation. The HTTP adapter now forwards public write requests to the group
leader over the existing internal tonic gRPC service via `ForwardHttpWrite`,
reusing the same Axum handler and runtime semantics on the leader instead of
duplicating create/append/close logic. Forwarded write channels are cached per
leader, `Host` is preserved so `Location` remains an absolute public URL, and
GET/HEAD still use ordinary redirects so streaming SSE responses are not
buffered through gRPC.

Targeted official sub-runs after that change:

```text
Protocol Edge Cases|SSE Mode: 41 passed / 41
State Hash Verification: 4 passed / 4
Stream Closure: 33 passed / 33
```

The remaining stable failure is fork under distributed per-group leaders. A
minimal fork request now returns quickly instead of timing out:

```text
HTTP/1.1 500 Internal Server Error
core 0 raft group 16 append failed: OpenRaft head_stream has to forward request to leader
```

This is a real ownership-boundary issue rather than an HTTP adapter issue.
`ShardRuntime::create_fork_stream()` performs source `head_stream`,
source `read_stream`, source `add_fork_ref`, and target `create_stream` as
separate group operations. When source and target groups have different Raft
leaders, the current `GroupEngine` returns `ForwardToLeader` for those internal
group operations but does not transparently proxy the group command to the
leader. Node 1 as leader for all groups masked this gap; per-group leader
distribution exposes it. The next structural fix is group-engine-level gRPC
command forwarding for `GroupWriteCommand` and leader-routed group reads, not
more HTTP redirect handling.

Earlier checkpoints during the same audit:

- Initial `/v1/stream` compatibility path: `206 passed / 300`.
- After `offset=now`, live cursor, SSE initial control, and long-poll timeout:
  `213 passed / 300`.
- After producer success status and HTTP body limit fixes:
  `230 passed / 300`.
- After closed-stream error header fix:
  `234 passed / 300`.
- After preserving structured Raft/runtime stream errors:
  `236 passed / 300`.
- After producer ack propagation and JSON mode normalization/projection:
  `253 passed / 300`.
- After deterministic TTL/Expires-At enforcement and committed TTL read-touch:
  `264 passed / 300`.
- After fork creation copied the source prefix into the fork's own stream:
  `291 passed / 300`.
- After fork source refcount, soft-delete, 410/409 mapping, and cascade GC:
  `300 passed / 300`.
- After changing cold flush planning to coalesce contiguous hot segments before
  applying `min_hot_bytes`, reran the official suite against current local
  `ursula-http --raft-memory` with `URSULA_COLD_BACKEND=memory`,
  `URSULA_COLD_FLUSH_INTERVAL_MS=1`, `URSULA_COLD_FLUSH_MIN_HOT_BYTES=1`,
  `URSULA_COLD_FLUSH_MAX_BYTES=1024`, and
  `URSULA_COLD_FLUSH_MAX_CONCURRENCY=4`: `300 passed / 300` in 16.54s.
  Post-run metrics showed `accepted_appends=1038`, `active_cores=4`,
  `active_groups=16`, `cold_hot_bytes=24`, `cold_flush_uploads=856`,
  `cold_flush_publishes=855`, `cold_backpressure_events=0`, and
  `wal_batches=0`.

## Adapter Fixes Already Made

- Added `/v1/stream/{*path}` routing while keeping the existing
  `/{bucket}/{stream}` perf path.
- Normalized content type handling and rejected body-bearing appends without a
  content type.
- Parsed and surfaced `Stream-TTL` and `Stream-Expires-At` metadata headers.
- Implemented `offset=now` as a non-cacheable tail read; JSON streams return
  `[]` for this special empty response.
- Made SSE send an initial control event instead of waiting forever on an empty
  tail.
- Added monotonic numeric live cursors for long-poll and SSE responses.
- Reduced default long-poll timeout to fit the official test window.
- Raised the HTTP body limit so 10 MiB append tests do not reset the connection.
- Matched official producer append status: first accepted producer append
  returns `200`, duplicate retry remains `204`.
- Added `Stream-Closed: true` on closed-stream conflict responses.
- Preserved structured stream errors through Raft/runtime so conflict responses
  can expose `Stream-Next-Offset`.
- Propagated accepted producer ack state through runtime responses so duplicate
  producer retries return the highest accepted epoch/sequence.
- Normalized JSON append bodies by validating and flattening the protocol's
  top-level wrapper array; JSON reads and SSE data events project stored JSON
  messages as arrays.
- Enforced TTL/Expires-At at the stream state-machine boundary using explicit
  command timestamps. Reads and writes renew TTL, `HEAD` does not renew TTL,
  Expires-At remains absolute, expired streams return not found, and expired
  streams can be recreated. The OpenRaft path commits read-triggered TTL touch
  or expiry through `TouchStreamAccess`.
- Implemented fork creation by copying the source's visible prefix into the
  fork's own stream at creation time. Fork reads and later appends therefore
  stay on the fork owner's Raft group instead of doing cross-lineage reads.
- Implemented source fork refcount commands in the committed group-write
  boundary. Deleting a source with live forks produces a soft-deleted `410`
  source, recreation is blocked with `409`, deleting the last fork releases the
  parent ref, and recursive soft-deleted parents are garbage-collected.

## Per-Group Leader Follow-Up

After enabling per-group initial membership, the first distributed-leader
official run exposed a structural routing bug rather than a Durable Streams
semantic bug. Public HTTP writes that landed on followers were fixed by
internal gRPC forwarding, and isolated fork creation then passed after adding
group-engine read/write forwarding. Full-suite runs still produced intermittent
`404`, missing initial bytes, stale closed headers, and fork reads that skipped
inherited data.

The root cause was follower-local preflight before Raft writes. Operations such
as append, close, publish snapshot, append batch, and cold-admission variants
called `ensure_stream_access()` or admission checks against the local
state-machine before proposing the command. On a follower that had not applied
the stream creation yet, this returned `StreamNotFound` locally, so the request
never reached OpenRaft's `ForwardToLeader` path. That matches the observed
failures: successful create followed by append `404`, reads missing the first
append, inconsistent closed state, and forks missing inherited bytes.

The group engine now forwards the complete write command to the current group
leader before any local state-machine preflight when the local node is a
follower. The leader then performs the same preflight and commits through the
normal Raft command path, keeping the implementation DRY and independent of the
HTTP endpoint that triggered it.

Validation after the fix:

- Three local processes, `--raft-memory`, 32 groups, per-group distributed
  initial membership, memory cold backend, cold flush disabled:
  `300 passed / 300`.
- Same topology with memory cold backend and aggressive cold flush
  (`URSULA_COLD_FLUSH_INTERVAL_MS=1`,
  `URSULA_COLD_FLUSH_MIN_HOT_BYTES=1`,
  `URSULA_COLD_FLUSH_MAX_BYTES=1024`):
  `300 passed / 300`.
- The current release binary was then built on `ursula-c7g-beast-node-1`
  (`c7g.4xlarge`, aarch64) from the current Ursula checkout, sha256
  `cd4c005ce8106423a1239280e8de45114d6ec2f0b4c8e985825b29b63f113982`,
  and deployed to the three EC2 server nodes on port `4489` with
  `--raft-memory`, 64 groups, 16 cores, `--raft-init-membership-per-group`,
  and `URSULA_COLD_BACKEND=s3`.
  The official suite from the `c7gn.8xlarge` client against
  `http://10.99.1.48:4489` passed `300 passed / 300` in 22.91s.
  Post-run metrics showed no WAL records, no cold backpressure, and leader
  distribution across all three nodes (`22/26/16` as observed from each node).
  An explicit cold-flush proof on the same fresh S3-enabled cluster wrote a
  3 MiB stream, flushed a 1 MiB chunk successfully, read the prefix back through
  HTTP, and observed background S3 offload on all three nodes:
  node1 `cold_flush_uploads=1`, node2 `=9`, node3 `=10`. S3 listed 19 objects
  totaling 29,360,128 bytes under the temporary root before cleanup.
  A single `InvalidColdFlush` worker log was observed after combining manual
  explicit flush with the background worker on the same stream; the official
  suite was already green, reads succeeded, and the temporary `4489` processes,
  S3 objects, and uploaded artifact were cleaned up.

## Remaining Non-Conformance Work

### S3 Cold Path

The current conformance run exercises the in-memory OpenRaft path only. The
final goal also requires a bounded-memory cold path that offloads event data to
S3 while accepting new writes.

Current cold-path progress is tracked in
`docs/migration/cold-path-progress.md`. Ursula now has a pure state-machine
manifest transition (`FlushCold`), hot-prefix read planning, opendal-backed
memory/fs/S3 cold store configuration, transparent cold+hot read reassembly
when a store is configured, actor-external upload before committed metadata
publish, group-local flush candidate selection, explicit single-stream flush,
object range reads for cold slices, bounded background flush concurrency,
cold upload/publish/orphan metrics, orphan cleanup on metadata-publish failure,
group-owned write admission with per-group hot-byte limits, hot-backlog gauges,
cold backpressure counters, and an optional background worker that scans Raft
groups. The official suite also passes with the memory cold store enabled and
aggressive background flush, including a log check that stale deleted-stream
flush candidates do not surface as worker errors. The gated runtime S3
integration has also passed locally and on `ursula-c7g-beast-node-1` against
the real `riverrun-e2e-us-east-1` bucket, covering actual object upload, range
read, manifest readback, metrics, and cleanup. A gated binary-level S3
integration has also passed locally against `ursula-c7g-beast-us-east-1`: it
starts three real `ursula-http` processes with independent `--raft-log-dir`
roots, replicates a cold manifest to S3, flushes until hot bytes reach zero,
stops and restarts every node without reinitializing membership, reads the
S3-backed stream through a restarted follower, and cleans up the unique S3
root.

## Next Structural Work

1. Add persisted/static peer config hardening, exercise lagging-follower
   snapshot transfer at EC2/restart level, and extend the EC2 static gRPC Raft
   smoke into longer-running S3 cold-path validation. The metrics endpoint now
   exposes per-group OpenRaft snapshot and purge indexes, so that EC2 exercise
   should prove catch-up using those fields rather than only proving post-catchup
   stream readback.
2. Re-run `perf_compare` after protocol coverage is green and cold-path
   behavior is enabled, because cold offload changes memory pressure, read
   planning, and request mix behavior.

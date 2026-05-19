# Perf Compare CPU Saturation Gate

## Goal

The final migration target is to make Ursula capable of saturating available CPU
under `perf_compare`. The current implementation reportedly plateaus at roughly
three to four busy cores even when benchmark concurrency increases.

That plateau is a primary signal for the new architecture: the thread-per-core
multi-Raft design must remove the global bottlenecks that stop CPU utilization
from scaling across independent streams.

## Benchmark Shape

`perf_compare` write throughput creates one stream per concurrency slot and
spawns one append loop per stream. For Ursula, it can use ordinary `POST` or the
Ursula-specific `append-batch` route:

```bash
cargo run -p perf-compare --bin perf_compare -- \
  --targets ursula \
  --ursula http://127.0.0.1:4437 \
  --durable http://127.0.0.1:1 \
  --s2 http://127.0.0.1:1 \
  --phases write,small,mixed \
  --concurrency 256 \
  --throughput-secs 30 \
  --ursula-append-mode batch \
  --ursula-append-batch-size 16
```

The benchmark is useful for this migration because independent streams should
route to independent shard actors and Raft groups. If CPU use stops at a few
cores, the server still has a global contention point.

## Acceptance Criteria

The migration is not complete until a perf run shows:

- CPU utilization scales beyond the current three-to-four-core plateau.
- Increasing `--concurrency` distributes work across shard-owned cores.
- `write` and `small` phases both use multiple Raft groups, not one global
  append path.
- `mixed` phase does not collapse on a single SSE/watch registry or metadata
  lock.
- Throughput improvement comes from server-side scaling, not only larger public
  append batches.
- Error rates remain zero or are explicitly understood.

## Required Server Metrics

The new runtime should expose enough counters to explain CPU utilization:

- requests accepted per core;
- appends proposed per Raft group;
- Raft commits applied per core;
- append queue depth per shard;
- WAL/group-commit latency per core;
- live-tail watcher count per shard;
- cross-core routed request count;
- rejected or backpressured request count.

Without these metrics, a green benchmark result is not sufficient evidence that
the architecture is working.

The current vNext HTTP prototype exposes the first append-distribution slice at
`GET /__ursula/metrics`:

- accepted append total derived from per-core counters;
- active core count;
- active Raft group count;
- append counts per core;
- append counts per Raft group;
- successful state mutation counts per core;
- successful state mutation counts per Raft group;
- successful state mutation apply time per core;
- successful state mutation apply time per Raft group;
- routed request counts per core;
- mailbox send wait nanoseconds per core;
- mailbox-full enqueue event counts per core;
- WAL batch counts, WAL record counts, WAL write time, and WAL sync time per
  core/group;
- live read waiter count per core;
- mailbox depth and capacity per core.

It does not yet expose Raft commit, cross-core routing, explicit rejection
metrics, or production OpenRaft WAL latency. The mailbox send-wait and
mailbox-full counters are the current backpressure signals: if CPU is not
saturated but these rise on one owner core, the server is still bottlenecked
before group execution. The optional WAL-backed group engine now reports
prototype group-local WAL write/sync counters, but those counters are still not
a substitute for production OpenRaft group commit metrics.

HTTP append-batch now maps to one shard-owned runtime command per HTTP batch.
The runtime still records each frame as an accepted append and applied mutation,
but `routed_requests` counts the batch once. That metric distinction matters:
high logical append throughput can coexist with low server CPU if each HTTP
request carries many frames and the underlying in-memory state-machine work is
cheap.

The in-memory group engine also applies batch frames directly instead of
recursively calling the boxed single-append future for each frame, and the
runtime aggregates append/mutation metric updates per successful batch. These are
hot-path cleanup changes, not CPU-saturation evidence by themselves.

The HTTP append-batch response path renders directly from runtime item results.
The all-success benchmark path emits repeated static `{"status":204}` ack
objects without first allocating an intermediate status vector.

The HTTP append-batch parser keeps each frame as a `Bytes` slice of the request
body, and the in-memory state machine exposes a borrowed append path. The
default in-memory benchmark path therefore avoids a parser-side payload copy for
each frame; the WAL path still materializes `Vec<u8>` records because serialized
commands own their payloads.

## Bottleneck Hypotheses From The Current Design

The current codebase has several structures that can plausibly cap CPU at a few
cores:

- one Raft group serializes all stream mutations;
- append coalescing and commit combining are global or coarse-sharded rather
  than Raft-group local;
- state-machine metadata and hot payload indexes are shared through process-wide
  structures;
- WAL fsync is durable but not organized as per-core group commit;
- SSE and long-poll watchers can introduce shared notification pressure;
- tonic/axum/Tokio are not the main issue until these ownership boundaries are
  removed.

These are hypotheses, not conclusions. They should be validated with profiles
and per-core metrics before each migration phase is accepted.

## Design Implication

The first implementation milestone should be a Tokio-hosted shard actor model,
not monoio. Once shard ownership is explicit and `perf_compare` shows work
spreading across cores, monoio can be tested as a runtime optimization behind
the same actor boundary.

`crates/ursula-runtime` provides the initial actor model. Its unit tests verify
that many benchmark-shaped streams reach every configured core and many Raft
groups. That is necessary evidence for the architecture, but not sufficient for
the final gate: a real server must still show CPU saturation under
`perf_compare`.

## Current vNext HTTP Prototype

`crates/ursula-http` can serve the basic Ursula target shape used by
`perf_compare`:

- `PUT /benchcmp`
- `PUT /benchcmp/{stream}`
- `POST /benchcmp/{stream}` in ordinary post mode
- `POST /benchcmp/{stream}/append-batch` in Ursula batch mode
- `GET /benchcmp/{stream}?offset=0&max_bytes={read_bytes}`
- `GET /benchcmp/{stream}?offset=now&live=long-poll`
- `GET /benchcmp/{stream}?offset=now&live=sse`
- `GET /__ursula/metrics`

This is enough to start validating ordinary create/write/read HTTP paths against
the thread-per-core runtime, including the small-event batch append path and
live-tail paths used by the mixed phase.

The same binary also accepts `--raft-memory` to run the HTTP adapter through
OpenRaft with the in-memory `RaftGroupLogStore`. That is the diskless OpenRaft
benchmark mode for isolating OpenRaft/runtime overhead from local WAL
durability. `--wal-dir DIR` runs the diagnostic group-level WAL engine, and
`--raft-log-dir DIR` runs the durable OpenRaft file-log engine. Those durable
paths are useful for recovery and durability smokes, but they are not the
diskless CPU saturation target.

## Current Integration Smoke

A short local smoke run has validated that the existing `perf_compare` Ursula
target can drive the vNext HTTP prototype for the write and small-event phases:

```bash
cargo run -p perf-compare --bin perf_compare -- \
  --targets ursula \
  --ursula http://127.0.0.1:4447 \
  --durable http://127.0.0.1:1 \
  --s2 http://127.0.0.1:1 \
  --phases write,small \
  --concurrency 16 \
  --throughput-secs 1 \
  --payload-bytes 128 \
  --small-payload-bytes 32 \
  --ursula-append-mode batch \
  --ursula-append-batch-size 4 \
  --request-timeout-secs 5 \
  --setup-concurrency 64
```

Against `ursula-http` running with `--core-count 4 --raft-group-count 64`, the
smoke produced zero benchmark errors:

- write phase: 83,308 accepted appends, all `204`.
- small-event phase: 81,340 accepted appends, all `204`.
- server metrics after the run: 164,648 accepted appends, 4 active cores, 30
  active Raft groups, mailbox depths all zero.

A release-mode local smoke has also been run with wider placement:

```bash
cargo run --release -p perf-compare --bin perf_compare -- \
  --targets ursula \
  --ursula http://127.0.0.1:4462 \
  --durable http://127.0.0.1:1 \
  --s2 http://127.0.0.1:1 \
  --phases write,small \
  --concurrency 256 \
  --throughput-secs 5 \
  --payload-bytes 128 \
  --small-payload-bytes 32 \
  --ursula-append-mode batch \
  --ursula-append-batch-size 16 \
  --request-timeout-secs 10 \
  --setup-concurrency 256
```

Against release `ursula-http` running with `--core-count 10
--raft-group-count 160`, that run produced zero benchmark errors:

- write phase: 4,356,800 successful appends at 870,826.84 req/s.
- small-event phase: 4,152,880 successful appends at 830,097.4 req/s.
- server metrics after the run: 8,509,680 accepted appends, 8,510,192 applied
  mutations, 10 active cores, 160 active Raft groups, `mailbox_full_events: 0`,
  and mailbox depths all zero.

This is still not CPU saturation proof because CPU utilization was not captured
alongside the run. It does prove the current vNext HTTP path can distribute a
high-concurrency release workload across all configured cores and groups.

A three-node EC2 run on 2026-05-18 exercised the static gRPC multi-Raft shape
with diskless OpenRaft and memory cold storage:

```text
/tmp/ursula-http-current-coalesce \
  --listen 0.0.0.0:4485 \
  --core-count 16 \
  --raft-group-count 64 \
  --raft-memory \
  --raft-node-id {1,2,3} \
  --raft-peer 1=http://10.99.1.48:4485 \
  --raft-peer 2=http://10.99.2.206:4485 \
  --raft-peer 3=http://10.99.3.236:4485

URSULA_COLD_BACKEND=memory
URSULA_COLD_FLUSH_INTERVAL_MS=200
URSULA_COLD_FLUSH_MIN_HOT_BYTES=65536
URSULA_COLD_FLUSH_MAX_BYTES=1048576
URSULA_COLD_FLUSH_MAX_CONCURRENCY=16
URSULA_COLD_MAX_HOT_BYTES_PER_GROUP=4194304
```

The comparable 30-second write/small run used 512 client concurrency,
128-byte payloads, append-batch size 16, and minimal acks. It completed with
zero errors:

```text
write_throughput: ok_requests=5626752 requests_per_sec=187280.41 MiB/s=22.86
small_event_write: ok_requests=4902592 requests_per_sec=163039.77 MiB/s=19.90
```

Post-run node 1 metrics confirmed that this was the in-memory Raft path and
that cold offload remained active without admission backpressure:

```text
accepted_appends=10529344
active_cores=16
active_groups=64
wal_batches=0
wal_records=0
cold_hot_bytes=28706816
cold_hot_group_bytes_max=1298432
cold_backpressure_events=0
cold_flush_uploads=17082
cold_flush_upload_bytes=1319049216
cold_flush_publishes=17082
mailbox_full_events=0
```

A 60-second mixed run on the same cluster used 256 appenders, 128 readers, and
8 SSE readers. It also completed with zero errors:

```text
mixed append: ok_requests=5971520 requests_per_sec=99525.33 MiB/s=9.49
mixed read: ok_requests=261178 requests_per_sec=4352.97 MiB/s=0.53
mixed SSE latency: count=800 p50=41.35ms p90=49.03ms p95=50.90ms p99=54.04ms
```

Final node 1 metrics after both runs:

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
live_read_waiters=0
mailbox_full_events=0
```

The same binary and 64-group/16-core shape with durable OpenRaft file logs and
S3 cold storage previously produced about 73k write req/s, 35k small-event
req/s, and 56k mixed append req/s. This in-memory result shows that durable log
and S3 cold IO materially lower the current EC2 throughput ceiling, while the
thread-per-core placement itself can keep all 16 configured owner cores active.
CPU sampling was not captured in this run, so it is throughput evidence rather
than final CPU-saturation proof.

Another EC2 run isolated the S3 cold path while keeping Raft diskless:
`--raft-memory` with `URSULA_COLD_BACKEND=s3`. The service ran on the same
three `c7g.4xlarge` server nodes with 64 Raft groups and 16 owner cores. The
first attempt used multiple `perf_compare` processes, but that was invalid
because `perf_compare` hard-coded the same `benchcmp` bucket in every process;
the processes raced stream setup/deletion and produced large `404` counts.

The valid multi-process run used a temporary asyncio HTTP/1.1 append-batch load
generator, one unique bucket per process, 4 client processes, 512 keep-alive
connections per process, 128-byte frames, batch size 16, and 45 seconds of
measured load against port 4486. The cluster used a 64 MiB per-group hot cap and
64 cold upload concurrency to avoid a 4 MiB admission cap becoming the first
limit.

Each process completed with only `204` responses:

```text
process 1: 31,814 events/s, 3.88 MiB/s
process 2: 32,471 events/s, 3.96 MiB/s
process 3: 32,044 events/s, 3.91 MiB/s
process 4: 31,880 events/s, 3.89 MiB/s
aggregate: about 128,209 events/s, 15.64 MiB/s
```

CPU sampling showed the leader/ingress node was already full while the client
host and followers were not:

```text
node1 tail samples: about 91-96% user+system+softirq busy, near 0% idle
node2 tail samples: about 10-17% busy
node3 tail samples: about 8-12% busy
client tail samples: about 1% busy
```

Node 1 metrics over the measured window:

```text
accepted_appends delta=5809808
wal_batches delta=0
wal_records delta=0
cold_backpressure_events delta=0
cold_flush_uploads delta=9478
cold_flush_upload_bytes delta=665104384
cold_flush_publishes delta=9434
mailbox_full_events delta=0
```

The S3 upload delta was about 665 MB over 45 seconds, or roughly 14.8 MB/s.
Thirty seconds after the load stopped, node 1 had uploaded 811,112,448 bytes in
total and still had `cold_hot_bytes=67092480`, so the background cold path was
not saturating S3 bandwidth. The observed first limit in this configuration is
leader-node CPU before S3 bandwidth. Followers staying mostly idle also shows
that this static benchmark shape still concentrates public ingress and cold
planning/upload work on the node receiving client traffic.

The structural reason was the static initialization shape: previous EC2 runs
started only node 1 with `--raft-init-membership`, so every warmed Raft group was
initialized from node 1 and elected node 1 as leader. Ursula now has an explicit
`--raft-init-membership-per-group` mode, also available in JSON config as
`init_membership_per_group`, where each group is initialized by
`raft_group_id % sorted_peer_count`. The focused test
`static_grpc_per_group_membership_initializers_distribute_leaders` starts three
local static gRPC nodes, warms six groups, and verifies leaders rotate
`1, 2, 3, 1, 2, 3`. This does not by itself prove EC2 CPU saturation, but it
removes the known static-cluster setup artifact that forced all leader/cold work
onto node 1.

The new initializer mode has also passed a short EC2 deployment smoke. A Linux
aarch64 release binary with sha256
`237a3bef56a4e331307b19645d70977b8efb47d46d3d8af5d2f53ca6e0f94ae7` was
deployed to the three `c7g.4xlarge` server nodes on port `4487` with
`--raft-memory`, 12 Raft groups, and `--raft-init-membership-per-group` enabled
on every node. Metrics from node 1, node 2, and node 3 all reported group
leaders:

```text
[1,2,3,1,2,3,1,2,3,1,2,3]
```

A minimal public create/read smoke through node 1 succeeded after following
leader redirects. Temporary `4487` processes and logs were cleaned up.

The same binary was then run on port `4488` with
`--raft-init-membership-per-group`, `--raft-memory`, 64 Raft groups, and
`URSULA_COLD_BACKEND=s3`. The configured S3 root was
`ursula-grpc-pergroup-s3/20260518T-s3-pergroup-4488`, with 64 MiB per-group hot
cap and 64 cold upload concurrency. All three nodes reported leader distribution
of 22, 21, and 21 groups across node 1, node 2, and node 3.

With four independent client processes, each using a unique bucket, 512
keep-alive connections, 128-byte frames, batch size 16, and 45 seconds of load,
all responses were `204`. Aggregate accepted throughput was about 229k
events/s:

```text
process 1: 59,574 events/s, 7.27 MiB/s
process 2: 57,350 events/s, 7.00 MiB/s
process 3: 55,586 events/s, 6.79 MiB/s
process 4: 57,275 events/s, 6.99 MiB/s
aggregate: about 229,785 events/s, 28.05 MiB/s
```

CPU and cold-upload work were no longer concentrated on node 1. Tail `mpstat`
samples during the measured window showed node 1 around 78-83% busy, node 2
around 76-78% busy, and node 3 around 72-76% busy, while the client host stayed
near 1-2% busy. Metrics deltas over the measured window:

```text
node1 accepted_appends=3660464 cold_flush_upload_bytes=434958336
node2 accepted_appends=3523888 cold_flush_upload_bytes=429983744
node3 accepted_appends=3217920 cold_flush_upload_bytes=387004416
total accepted_appends=10402372
total cold_flush_upload_bytes=1251946496
```

The aggregate S3 upload delta was about 1.25 GB over 45 seconds, or roughly
27.8 MB/s. There were no WAL records, cold backpressure events, or mailbox-full
events. Raising to eight client processes reduced accepted throughput to about
131k events/s while server CPU stayed in roughly the same 70-80% busy band and
still produced no mailbox or cold backpressure. That points to this temporary
HTTP/1 load generator and redirect/connection interaction becoming less
efficient past four processes, not to S3 bandwidth saturation.

Compared with the prior single-initializer run, per-group initialization fixed
the structural node-1 concentration and increased the best observed
`--raft-memory + S3` accepted rate from about 128k events/s to about 230k
events/s. It still does not hit S3 bandwidth, and it does not yet prove final
CPU saturation under a production-grade client harness.

A release-mode local smoke also validated the OpenRaft in-memory log path:

```bash
cargo run --release -p ursula-http --bin ursula-http -- \
  --listen 127.0.0.1:4457 \
  --core-count 4 \
  --raft-group-count 32 \
  --raft-memory

/Users/xing/Idea/riverrun/target/release/perf_compare \
  --targets ursula \
  --ursula http://127.0.0.1:4457 \
  --durable http://127.0.0.1:1 \
  --s2 http://127.0.0.1:1 \
  --phases write,small \
  --concurrency 32 \
  --throughput-secs 2 \
  --payload-bytes 128 \
  --small-payload-bytes 32 \
  --request-timeout-secs 10 \
  --setup-concurrency 64 \
  --ursula-append-mode batch \
  --ursula-append-batch-size 8 \
  --ursula-append-batch-minimal-ack
```

That run completed with zero benchmark errors:

- write phase: 1,067,800 successful appends at 533,850.59 appends/s.
- small-event phase: 1,072,368 successful appends at 536,127.53 appends/s.
- server metrics after the run: 2,140,168 accepted appends, 2,140,232 applied
  mutations, 4 active cores, 28 active groups, `mailbox_full_events: 0`, and
  empty mailboxes.
- OpenRaft in-memory log evidence: `wal_batches: 0`, `wal_records: 0`,
  `wal_write_ns: 0`, and `wal_sync_ns: 0`.

This is a wiring and diskless-mode smoke, not CPU saturation evidence. It proves
that `perf_compare` can drive the OpenRaft runtime path without local WAL
persistence.

Two CPU-sampled release runs were then captured with release `ursula-http`
running at `--core-count 10 --raft-group-count 160`.

At `--concurrency 256`, `--throughput-secs 10`, and batch appends of 16 frames:

- write phase: 8,305,488 successful appends at 830,280.37 req/s.
- small-event phase: 8,664,208 successful appends at 866,320.82 req/s.
- server metrics after the run: 16,969,696 accepted appends, 16,970,208 applied
  mutations, 10 active cores, 160 active Raft groups, `mailbox_full_events: 0`,
  and mailbox depths all zero.
- server CPU samples above 100% averaged 476.5% CPU and peaked at 541.0%, or
  about 4.77 average cores and 5.41 peak cores.

At `--concurrency 1024`, `--throughput-secs 10`, and batch appends of 16
frames:

- write phase: 9,005,968 successful appends at 900,088.08 req/s.
- small-event phase: 8,400,544 successful appends at 838,319.01 req/s.
- server CPU samples above 100% averaged 478.1% CPU and peaked at 572.3%, or
  about 4.78 average cores and 5.72 peak cores.

These sampled runs are the current gate result. The vNext prototype has removed
the original placement problem enough to reach every configured shard core and
all 160 groups, but it still does not satisfy the CPU saturation goal. Higher
benchmark concurrency raises throughput only modestly and leaves the server
near a five-to-six-core plateau on this machine.

After changing append-batch to route once per HTTP batch and apply every frame
on the owning core, the same `--concurrency 1024`, `--throughput-secs 10`,
batch-size-16 run produced:

- write phase: 10,913,344 successful logical appends at 1,091,086.74 req/s.
- small-event phase: 11,253,056 successful logical appends at 1,125,166.59
  req/s.
- server metrics after the run: 22,166,400 accepted appends, 22,168,448 applied
  mutations, 10 active cores, 160 active Raft groups, 1,387,448 routed runtime
  requests, `mailbox_full_events: 0`, and mailbox depths all zero.
- server CPU samples above 100% averaged 325.8% CPU and peaked at 341.6%, or
  about 3.26 average cores and 3.42 peak cores.

This improves logical append throughput but does not improve CPU saturation.
The result is diagnostic: a significant part of the previous CPU cost was
per-frame runtime routing and oneshot response overhead, not useful
state-machine or consensus work.

A post-mode control run at `--concurrency 1024` after the same change reached
79,851.19 write req/s and 81,038.93 small-event req/s with zero benchmark
errors. Server CPU samples averaged 292.3% CPU and peaked at 310.3%, or about
2.92 average cores and 3.10 peak cores.

A higher-concurrency batch run at `--concurrency 4096` did not pass the error
bar. Its write phase reported 424,976 status-0/client errors and only 13,196.38
req/s over 22.857 seconds. Its small-event phase had zero errors and reached
1,094,909.46 req/s, but server CPU samples still averaged only 318.8% CPU and
peaked at 341.3%, or about 3.19 average cores and 3.41 peak cores.

After the in-memory group engine stopped calling the boxed single-append future
for each batch frame and batch metrics were aggregated, the same plateau
remained. A single-process small-event run with `--concurrency 1024`,
`--throughput-secs 15`, and batch size 16 completed with zero errors:

- logical append rate: 1,110,037.25 appends/s.
- server metrics after the run: 16,652,256 accepted appends, 16,653,280 applied
  mutations, 10 active cores, 154 active groups, 1,041,790 routed runtime
  requests, `mailbox_full_events: 0`, and empty mailboxes.
- server CPU samples averaged 296.6% CPU and peaked at 327.0%, or about 2.97
  average cores and 3.27 peak cores.

A stack sample taken during a similar 20-second run showed meaningful time in
Hyper/Axum request handling, allocator/free/realloc, memmove, SipHash/path
hashing, Tokio wakeups, runtime enqueue, and stream append. That sample does not
prove a single remaining bottleneck, but it argues against spending more time on
group-internal boxed append futures or per-frame metrics as the main plateau
cause.

After the HTTP batch ack renderer was changed to avoid the intermediate status
vector and fast-path all-204 responses, a 10-second small-event run at the same
concurrency and batch size completed with zero errors:

- logical append rate: 1,125,134.01 appends/s.
- server metrics after the run: 11,252,960 accepted appends, 11,253,984 applied
  mutations, 10 active cores, 156 active groups, 704,334 routed runtime
  requests, `mailbox_full_events: 0`, and empty mailboxes.
- server CPU samples averaged 288.7% CPU and peaked at 323.4%, or about 2.89
  average cores and 3.23 peak cores.

This again does not change the CPU gate result.

After the HTTP append-batch parser started returning `Bytes` slices and the
in-memory state machine appended from borrowed payload bytes, another 10-second
small-event run at the same concurrency and batch size completed with zero
errors:

- logical append rate: 1,112,423.64 appends/s.
- server metrics after the run: 11,126,048 accepted appends, 11,127,072 applied
  mutations, 10 active cores, 160 active groups, 696,402 routed runtime
  requests, `mailbox_full_events: 0`, and empty mailboxes.
- server CPU samples averaged 275.5% CPU and peaked at 307.7%, or about 2.76
  average cores and 3.08 peak cores.

This removes another obvious allocator/memmove source from the in-memory batch
path, but still does not change the CPU gate result.

An explicit minimal-ack control then added `Prefer: return=minimal` to Ursula
append-batch requests. When every frame succeeds, Ursula returns `204 No
Content` and `perf_compare` counts the whole batch as successful without parsing
per-item JSON. A 10-second small-event run at the same concurrency and batch
size completed with zero errors:

- logical append rate: 1,097,653.11 appends/s.
- server metrics after the run: 10,978,224 accepted appends, 10 active cores,
  160 active groups, and no observed mailbox-full pressure.
- server CPU samples averaged 278.1% CPU and peaked at 314.5%, or about 2.78
  average cores and 3.15 peak cores.

This removes successful-response body generation and client-side JSON decode
from the measured hot path, but still does not change the CPU gate result.

A raw HTTP/1 diagnostic server was added as `ursula-http-raw`. It implements
only the `perf_compare` create and append-batch subset over the same
`ShardRuntime`, bypassing axum/hyper routing and response rendering. With
`Prefer: return=minimal`, `--concurrency 1024`, `--throughput-secs 10`, and batch
size 16, it completed with zero errors:

- logical append rate: 1,085,967.92 appends/s.
- accepted appends: 10,861,408.
- server CPU samples averaged 207.3% CPU and peaked at 242.2%, or about 2.07
  average cores and 2.42 peak cores.

This did not increase `perf_compare` request rate. It reduced the amount of CPU
spent per request on the server, which makes axum/hyper routing an unlikely
explanation for the CPU saturation failure.

`perf_compare` then gained an explicit Ursula-only
`--ursula-append-pipeline-depth` control to keep multiple append requests in
flight per stream slot. This tests whether the original loop is limited by one
awaited request per stream. It did not produce valid saturation evidence:

- depth 2, `--concurrency 1024`, minimal ack, batch size 16: 2,422,896
  successful logical appends, 71,760 status-0/client errors, server CPU averaged
  49.7% and peaked at 110.8%.
- depth 4, same shape: 320,960 successful logical appends, 94,976
  status-0/client errors, server CPU averaged 7.4% and peaked at 26.3%.

This is consistent with the earlier `--concurrency 4096` and multi-process
controls: adding in-flight reqwest pressure on the same machine reaches
client/OS failure modes before it produces sustained server CPU saturation.

Multi-process `perf_compare` was also tried to test whether one reqwest client
process was the only limit. Four concurrent client processes at
`--concurrency 1024` were not valid acceptance evidence: two processes reported
large status-0 error counts, one timed out during stream creation, and one hit
`Can't assign requested address`, which indicates client/OS connection pressure.

Four concurrent client processes at `--concurrency 256`, each running the
small-event batch phase for 15 seconds, completed with zero errors:

- per-process logical append rates: 517,557.23, 513,563.15, 517,217.75, and
  522,363.38 req/s.
- aggregate logical append rate: about 2.07M appends/s.
- server metrics after the run: 31,093,456 accepted appends, 31,094,480 applied
  mutations, 10 active cores, 160 active groups, 1,944,365 routed runtime
  requests, `mailbox_full_events: 0`, and empty mailboxes.
- server CPU samples above 100% averaged 431.6% CPU and peaked at 454.8%, or
  about 4.32 average cores and 4.55 peak cores.

Eight concurrent client processes at `--concurrency 256` also completed with
zero errors, but did not increase server-side pressure:

- per-process logical append rates ranged from 242,264.18 to 249,525.86 req/s.
- aggregate logical append rate: about 1.96M appends/s.
- server metrics after the run: 29,412,912 accepted appends, 29,414,960 applied
  mutations, 10 active cores, 160 active groups, 1,840,355 routed runtime
  requests, `mailbox_full_events: 0`, and empty mailboxes.
- server CPU samples above 100% averaged 421.8% CPU and peaked at 460.5%, or
  about 4.22 average cores and 4.61 peak cores.

The current same-machine HTTP benchmark ceiling is therefore around
120k-130k HTTP batch requests/s, or about two million logical appends/s with a
batch size of 16. Adding more client processes does not approach the direct
runtime stress ceiling.

Increasing public batch size also does not prove CPU saturation. A single
`perf_compare` client with `--concurrency 1024`, `--throughput-secs 15`, and
`--ursula-append-batch-size 64` completed the small-event phase with zero
errors:

- logical append rate: 2,983,891.9 appends/s.
- server metrics after the run: 44,773,632 accepted appends, 44,774,656 applied
  mutations, 10 active cores, 160 active groups, 700,612 routed runtime
  requests, `mailbox_full_events: 0`, and empty mailboxes.
- server CPU samples above 100% averaged 353.6% CPU and peaked at 373.5%, or
  about 3.54 average cores and 3.73 peak cores.

The larger batch increased logical append throughput, but reduced HTTP batch
request rate to about 46k requests/s and did not raise server CPU. For the CPU
gate, logical append rate alone remains a weak proxy.

The WAL-backed HTTP path was smoke-tested after append-batch started using a
group-level WAL batch boundary. Release `ursula-http --wal-dir` with
`--core-count 10 --raft-group-count 160` and release `perf_compare` with
`--phases small --concurrency 256 --throughput-secs 5
--ursula-append-mode batch --ursula-append-batch-size 16` completed with zero
errors:

- logical append rate: 2,502.87 appends/s.
- server metrics after the run: 16,624 accepted appends, 16,880 applied
  mutations, 10 active cores, 116 active groups, 1,295 routed runtime requests,
  `mailbox_full_events: 0`, and empty mailboxes.
- server CPU samples above 100% averaged 372.5% CPU and peaked at 399.4%, or
  about 3.73 average cores and 3.99 peak cores.
- the WAL directory contained 116 group log files and used about 6.0 MiB.

This is useful durability smoke evidence for the group-local WAL boundary. It
is not CPU saturation proof: the prototype still syncs each arriving HTTP batch
for its group and lacks OpenRaft log batching, cross-proposal group commit, and
WAL latency histograms.

The OpenRaft-backed HTTP path was then smoke-tested with the same benchmark
shape after adding `ursula-http --raft-log-dir` and wiring the OpenRaft file log
into the existing per-core/per-group durable-log metrics. Release
`ursula-http --raft-log-dir` with `--core-count 10 --raft-group-count 160` and
release `perf_compare` with `--phases small --concurrency 256
--throughput-secs 5 --ursula-append-mode batch --ursula-append-batch-size 16
--ursula-append-batch-minimal-ack` completed with zero errors:

- logical append rate: 1,303.68 appends/s.
- server metrics after the run: 10,592 accepted appends, 10,848 applied
  mutations, 10 active cores, 116 active groups, 918 routed runtime requests,
  `mailbox_full_events: 0`, and empty mailboxes.
- OpenRaft file-log metrics after the run: 2,416 durable-log batches, 2,416
  durable-log records, about 106.5s aggregate write time, and about 13.6s
  aggregate sync time across all cores.
- server CPU samples above 100% averaged 295.7% CPU and peaked at 337.6%, or
  about 2.96 average cores and 3.38 peak cores.
- the OpenRaft log directory contained 116 group log files and used about
  5.6 MiB.

Keeping the group file handle open and switching the per-record flush to
`sync_data` did not materially change throughput or CPU utilization. The
current OpenRaft prototype is therefore dominated by per-record durable-log
waits and by the fact that each core worker awaits one group command before
dispatching the next command for any other group on that core. The next
CPU-saturation fix should introduce a real per-core storage scheduling boundary:
either per-group actors with async/offloaded durable-log flush, or explicit
group commit that lets a core keep other groups moving while one group waits
for storage.

The runtime dispatcher was then changed so a core worker dispatches group
commands into per-group async mutexes instead of awaiting every command in the
mailbox loop. `RaftGroupFileLogStore` also moved blocking file writes into
Tokio's blocking pool. The same release `perf_compare` command completed with
zero errors:

- logical append rate: 1,246.8 appends/s.
- server metrics after the run: 9,904 accepted appends, 10,160 applied
  mutations, 10 active cores, 128 active groups, 875 routed runtime requests,
  `mailbox_full_events: 0`, and empty mailboxes.
- OpenRaft file-log metrics after the run: 2,390 durable-log batches, 2,390
  durable-log records, about 950.4s aggregate file-log write-path time, and
  about 22.6s aggregate sync time across all cores.
- server CPU samples above 100% averaged 357.9% CPU and peaked at 590.9%, or
  about 3.58 average cores and 5.91 peak cores.
- the OpenRaft log directory contained 128 group log files and used about
  4.4 MiB.

This validates the scheduler boundary but still does not meet the CPU gate.
The higher peak CPU suggests more work can run while individual groups wait,
but the average remains around the old plateau and throughput is not improved.
The next implementation target should therefore be the durable-log layout and
commit path itself: fewer records per client write, an indexed binary log, and
group commit or a per-core storage journal that can batch syncs across owned
groups without introducing a process-wide serialization point.

The OpenRaft file log then gained a core-local journal writer. Every owner core
has one `journal.bin`; groups on that core submit durable-log records to the
same writer, which batches requests briefly and syncs the core journal once per
batch. The same release `perf_compare` command completed with zero errors:

- logical append rate: 1,475.4 appends/s.
- server metrics after the run: 10,672 accepted appends, 10,928 applied
  mutations, 10 active cores, 132 active groups, 923 routed runtime requests,
  `mailbox_full_events: 0`, and empty mailboxes.
- OpenRaft file-log metrics after the run: 2,506 durable-log batches, 2,506
  durable-log records, about 95.3s aggregate file-log write-path time, and
  about 5.3s aggregate sync time across all cores.
- server CPU samples averaged 499.0% CPU and peaked at 803.5%, or about 4.99
  average cores and 8.04 peak cores.
- the OpenRaft log directory contained 142 files and used about 9.7 MiB.

This is progress but still not the CPU gate. The per-core journal reduced
aggregate sync time substantially versus the previous offloaded per-group file
path, and peak CPU can now reach about eight cores during bursts. Average CPU
is still around five cores and throughput is still dominated by the durable
OpenRaft proposal/log path, so the remaining work is fewer OpenRaft log records
per public append batch, a non-JSON indexed log format, and a production
group-commit scheduler with explicit latency/backpressure metrics.

The next control removed diagnostic per-group file writes from the durable
runtime path. Recovery already uses the core journal, so writing a per-group
file in the hot path duplicated serialization and append I/O. With only the
core journal left, the same release `perf_compare` command completed with zero
errors:

- logical append rate: 2,616.82 appends/s.
- server metrics after the run: 16,416 accepted appends, 16,672 applied
  mutations, 10 active cores, 140 active groups, 1,282 routed runtime requests,
  `mailbox_full_events: 0`, and empty mailboxes.
- OpenRaft file-log metrics after the run: 3,264 durable-log batches, 3,264
  durable-log records, about 85.7s aggregate file-log write-path time, and
  about 6.3s aggregate sync time across all cores.
- average durable-log write time per batch fell to about 26.2ms.
- the OpenRaft log directory contained 10 core journal files and used about
  7.4 MiB.

The core journal was then changed from JSON lines to length-prefixed MessagePack
records, while keeping the same per-core journal/recovery model. The same
release `perf_compare` command completed with zero errors:

- logical append rate: 14,666.97 appends/s.
- server metrics after the run: 78,880 accepted appends, 79,136 applied
  mutations, 10 active cores, 132 active groups, 5,186 routed runtime requests,
  `mailbox_full_events: 0`, and empty mailboxes.
- OpenRaft file-log metrics after the run: 11,032 durable-log batches, 11,032
  durable-log records, about 44.8s aggregate file-log write-path time, and
  about 26.4s aggregate sync time across all cores.
- average durable-log write time per batch fell again to about 4.1ms; average
  sync time per batch was about 2.4ms.
- server CPU samples averaged 24.6% and peaked at 32.0%, which means this run
  is still waiting on durable-log/proposal progress rather than saturating CPU.
- the OpenRaft log directory contained 10 core journal files and used about
  8.7 MiB.

This validates the serialization hypothesis: JSON was a major durable-log
write-path cost. It still does not pass the CPU gate. The next durable-path
work should not be compression on the hot journal; it should be a production
binary block format with checksums plus real group commit so each fsync commits
more OpenRaft records.

An in-memory-only control was then run after changing the OpenRaft
`RaftGroupEngine::write_batch` path to submit one public append batch through
OpenRaft `client_write_many`, instead of wrapping the whole batch in one
application-level `GroupWriteCommand::Batch` entry. The intent was to test
whether OpenRaft's own client-write batching path can raise server CPU without
any disk WAL in the loop.

The change was first checked with:

```bash
cargo test -p ursula-raft -p ursula-runtime
```

That passed all 14 `ursula-raft` tests and all 35 `ursula-runtime` tests. The
release server was then rebuilt:

```bash
cargo build --release -p ursula-http
```

The OpenRaft in-memory run used:

```bash
target/release/ursula-http \
  --listen 127.0.0.1:4463 \
  --core-count 10 \
  --raft-group-count 160 \
  --raft-memory
```

and:

```bash
/Users/xing/Idea/riverrun/target/release/perf_compare \
  --targets ursula \
  --ursula http://127.0.0.1:4463 \
  --durable http://127.0.0.1:1 \
  --s2 http://127.0.0.1:1 \
  --phases small \
  --concurrency 1024 \
  --throughput-secs 20 \
  --payload-bytes 128 \
  --small-payload-bytes 32 \
  --latency-count 1 \
  --request-timeout-secs 10 \
  --setup-concurrency 256 \
  --ursula-append-mode batch \
  --ursula-append-batch-size 16 \
  --ursula-append-batch-minimal-ack
```

The OpenRaft in-memory run completed with zero errors:

- logical append rate: 876,842.57 appends/s.
- server metrics after the run: 17,540,080 accepted appends, 10 active cores,
  160 active groups, 1,097,279 routed runtime requests, `mailbox_full_events: 0`,
  and empty mailboxes.
- disk WAL metrics were all zero: `wal_batches: 0`, `wal_records: 0`,
  `wal_write_ns: 0`, and `wal_sync_ns: 0`.
- server CPU samples averaged 291.0% CPU and peaked at 533.7%, or about 2.91
  average cores and 5.34 peak cores.

The pure runtime in-memory engine was then run with the same server shape and
the same `perf_compare` command, but without `--raft-memory`, `--wal-dir`, or
`--raft-log-dir`. It also completed with zero errors:

- logical append rate: 1,026,262.07 appends/s.
- server metrics after the run: 20,526,960 accepted appends, 10 active cores,
  160 active groups, 1,283,959 routed runtime requests, `mailbox_full_events: 0`,
  and empty mailboxes.
- disk WAL metrics were again all zero.
- server CPU samples averaged 182.0% CPU and peaked at 285.2%, or about 1.82
  average cores and 2.85 peak cores.

This in-memory-only comparison rules out local disk WAL as the cause of the
current `perf_compare` CPU plateau. It also shows that OpenRaft in-memory can
consume more CPU than the pure in-memory engine under the same HTTP benchmark,
but still does not saturate the configured 10 cores. The structural root cause
therefore remains above the disk layer: either the same-machine `perf_compare`
HTTP ingress cannot provide enough server-side work, or the current
OpenRaft/runtime command boundary still serializes too much work per routed
batch. The next useful evidence is task-level wait attribution for the
`--raft-memory` path, especially where group actors are waiting between
OpenRaft proposal submission, response delivery, and state-machine apply.

Tokio-console was then enabled for the same OpenRaft in-memory shape to locate
where tasks sit while `perf_compare` cannot fill the server CPUs. The console
build requires Tokio unstable instrumentation:

```bash
RUSTFLAGS="--cfg tokio_unstable" \
  cargo build --release -p ursula-http --features tokio-console
```

The profiled server used:

```bash
URSULA_TOKIO_CONSOLE=1 \
TOKIO_CONSOLE_BIND=127.0.0.1:6670 \
TOKIO_CONSOLE_RETENTION=60s \
TOKIO_CONSOLE_PUBLISH_INTERVAL=250ms \
target/release/ursula-http \
  --listen 127.0.0.1:4463 \
  --core-count 10 \
  --raft-group-count 160 \
  --raft-memory
```

and a shorter 10s `perf_compare` run with the same high-concurrency in-memory
append shape:

```bash
/Users/xing/Idea/riverrun/target/release/perf_compare \
  --targets ursula \
  --ursula http://127.0.0.1:4463 \
  --durable http://127.0.0.1:1 \
  --s2 http://127.0.0.1:1 \
  --phases small \
  --concurrency 1024 \
  --throughput-secs 10 \
  --payload-bytes 128 \
  --small-payload-bytes 32 \
  --latency-count 1 \
  --request-timeout-secs 10 \
  --setup-concurrency 256 \
  --ursula-append-mode batch \
  --ursula-append-batch-size 16 \
  --ursula-append-batch-minimal-ack
```

That run completed 8,434,336 logical appends at 841,830.63 appends/s with zero
errors. The `tokio-console` CLI needs a TTY, so this capture was qualitative
TUI evidence rather than a machine-readable trace.

The task view showed about 810 tasks. During load, only a small minority were
running at a time, commonly around 16 to 20 tasks, while hundreds were idle.
The ten shard-runtime `block_on` tasks each accumulated useful busy time, but
their lifetime was dominated by idle time: representative rows showed about
14s to 18s busy and about 1m26s to 1m50s idle, with scheduler delay at or near
zero. This does not look like Tokio ready-queue starvation. The runtime cores
are mostly waiting for work or for async proposal/response progress between
bursts.

The resource view was also consistent with an async wait boundary inside
OpenRaft rather than disk I/O. Newer resources were dominated by OpenRaft
`tokio::time::Sleep` timers, while older long-lived resources included
OpenRaft `tokio::sync::oneshot` sender/receiver resources. This is not precise
enough to assign blame to one OpenRaft stage, but it narrows the next
instrumentation target: measure submit time, response-stream wait time, and
state-machine apply callback time around `client_write_many`, and separate
that from HTTP ingress pressure.

That instrumentation was then added to the runtime metrics surface:

- `raft_write_many_batches`
- `raft_write_many_commands`
- `raft_write_many_responses`
- `raft_write_many_submit_ns`
- `raft_write_many_response_ns`
- `raft_apply_entries`
- `raft_apply_ns`

The first instrumented run, before changing the single append-batch path,
completed 8,447,568 logical appends at 844,550.63 appends/s with zero errors.
The server reported 528,997 routed requests, but only 181,494
`raft_write_many_commands`. `group_engine_exec_ns` was about 109.0s,
`raft_write_many_response_ns` was about 44.3s, and `raft_apply_ns` was about
3.9s. This exposed a measurement and structure gap: the group actor only used
`write_batch` / `client_write_many` when it could immediately drain multiple
append-batch requests from the same group mailbox. Single append-batch requests
still used `append_batch` -> `client_write`, bypassing the new proposal/response
stage metrics.

`RaftGroupEngine::append_batch` was therefore changed to route even one public
append-batch request through `write_commands(vec![...])`. The targeted check:

```bash
cargo test -p ursula-raft -p ursula-runtime -p ursula-http
```

passed all 14 `ursula-raft` tests, all 35 `ursula-runtime` tests, and all 16
`ursula-http` tests. A release `--raft-memory` run with the same 10s
`perf_compare` shape then completed with zero errors:

- logical append rate: 922,988.66 appends/s.
- accepted appends: 9,232,496.
- routed requests: 578,055.
- `raft_write_many_batches`: 478,948.
- `raft_write_many_commands`: 577,031.
- `raft_write_many_responses`: 577,031.
- `group_engine_exec_ns`: about 88.25s.
- `raft_write_many_submit_ns`: about 0.66s.
- `raft_write_many_response_ns`: about 87.10s.
- `raft_apply_entries`: 578,055.
- `raft_apply_ns`: about 3.98s.
- `group_lock_wait_ns`: 0.
- `mailbox_full_events`: 0.
- disk WAL metrics remained zero.

This is the strongest evidence so far against the interpretation that the
current multi-Raft sharding layer is primarily blocked on shard lock contention.
The group actor's execution time is almost entirely time spent awaiting
OpenRaft's `client_write_many` response stream, while actual state-machine apply
CPU is small and balanced across cores. The structural CPU plateau is therefore
inside the OpenRaft single-node proposal/response pipeline or the way Ursula
feeds that pipeline, not inside local disk WAL, Tokio ready-queue delay, or a
measured shard lock wait.

A cooperative-yield coalescing experiment was also tried at the group actor
append-batch boundary. It raised the average commands per `client_write_many`
from about 1.20 to about 1.36 and reduced aggregate
`raft_write_many_response_ns` from about 87s to about 58s, but throughput was
effectively unchanged at 920,451.03 appends/s. Because this is a latency
tradeoff without meaningful throughput improvement, it was not kept as a
structural fix.

A more structural detached-write experiment was then tried: `RaftGroupEngine`
used a cloned OpenRaft handle to return a `'static` write future, and the group
actor allowed multiple in-flight append-batch proposals per group before
waiting for completions. This directly tested whether the current single
in-flight group actor wait was the primary throughput limiter. It was also not
kept. With one `perf_compare` client at concurrency 1024, throughput stayed
near the same level at 961,502.52 appends/s and server CPU averaged about
439.8%. With four client processes at concurrency 256 each, server CPU averaged
530.9% and peaked at 603.2%, but per-client logical append rates fell to about
364k-369k appends/s, below the previous non-detached 4-client result around
398k-400k appends/s per client. Metrics still showed the same dominant wait:
`group_engine_exec_ns` about 2617.8s and `raft_write_many_response_ns` about
2011.7s, with `raft_apply_ns` only about 24.0s and disk WAL metrics at zero.

That falsifies a simple "spawn more in-flight OpenRaft client writes per group"
fix. More client pressure can raise CPU from the single-client plateau, but the
system remains dominated by OpenRaft proposal-response waiting and does not
turn that wait into useful state-machine CPU. The next structural fix should
change the ack/visibility contract for the minimal-ack hot path or use a
lower-level proposal/commit observer, not just detach more `client_write_many`
futures.

A more conservative variant with only one in-flight detached write per group
and an append-only buffer behind it was also tried to preserve FIFO ordering
across reads, closes, deletes, and snapshots. That did not help either: one
client at concurrency 1024 completed 9,266,416 logical appends at 926,418.10
appends/s, with server CPU averaging 419.2%. `raft_write_many_response_ns`
increased to about 132.9s while `raft_apply_ns` stayed around 4.2s. This variant
was also removed.

Direct runtime stress was added to separate the ShardRuntime ceiling from the
HTTP/reqwest/perf_compare ceiling:

```bash
target/release/ursula-runtime-stress \
  --core-count 10 \
  --raft-group-count 160 \
  --stream-count 8192 \
  --producer-count 2048 \
  --setup-concurrency 2048 \
  --batch-size 16 \
  --payload-bytes 128 \
  --duration-secs 10 \
  --mode batch
```

That direct batch run bypassed HTTP and completed 74,938,448 accepted appends at
7,353,566.27 appends/s, with 4,691,845 routed runtime requests at 460,401.76
routed req/s, 10 active cores, 160 active groups, and `mailbox_full_events: 0`.
Whole-process CPU samples above 100% averaged 781.8% CPU and peaked at 818.9%,
or about 7.82 average cores and 8.19 peak cores.

A direct append-mode run with `--producer-count 4096` completed 7,489,217
accepted appends at 740,136.32 appends/s, with 7,497,409 routed runtime requests
at 740,945.92 routed req/s, 10 active cores, 160 active groups, and
`mailbox_full_events: 0`. Whole-process CPU samples averaged 832.4% CPU and
peaked at 849.2%, or about 8.32 average cores and 8.49 peak cores.

These direct runs are not acceptance evidence because they bypass
`perf_compare`, HTTP, and the future OpenRaft/WAL path. They are useful
diagnostics: the same shard runtime can drive substantially more CPU than the
HTTP `perf_compare` path, so the next CPU-saturation work should focus on
ingress/harness pressure and production consensus/storage work instead of
stream-to-core placement.

After adding `group_mailbox_depth`, `per_group_group_mailbox_depth`,
`group_mailbox_max_depth`, and `per_group_group_mailbox_max_depth` metrics, the
OpenRaft in-memory HTTP path was sampled again with release `ursula-http` at
`--core-count 10 --raft-group-count 160 --raft-memory`.

A single `perf_compare` process at `--concurrency 1024`, `--phases small`,
`--throughput-secs 10`, `--ursula-append-mode batch`,
`--ursula-append-batch-size 16`, and minimal acks completed 9,429,488 logical
appends with zero errors, about 942,818 logical appends/s. Server CPU averaged
about 347.5% and peaked at 450.5%. Server metrics reported 590,367 routed
runtime requests, 542,942 OpenRaft write-many batches, 31.98s total
`raft_write_many_response_ns`, 4.12s `raft_apply_ns`, 10 active cores, 160
active groups, and `mailbox_full_events: 0`. The core mailbox depth sum peaked
at 834, while group mailbox depth sum only peaked at 133, max per group was 11,
and the run ended with all mailbox depths at zero.

Two concurrent `perf_compare` processes with the same per-process shape also
completed with zero client errors, but total throughput fell to 5,955,328
logical appends over the 10-second window. Server CPU averaged about 261.9% and
peaked at 424.6%. Group mailbox depth sum peaked at 192, max per group was 20,
and the run again ended with all mailbox depths at zero. This does not support
the hypothesis that the CPU plateau is caused by a large unbounded
`core -> group` backlog hidden behind the HTTP layer.

To separate HTTP ingress from OpenRaft, a diagnostic
`ursula-raft-runtime-stress` binary now drives `ShardRuntime` directly with
`RaftGroupEngineFactory`. With `--core-count 10 --raft-group-count 160
--stream-count 4096 --producer-count 4096 --batch-size 16 --payload-bytes 100
--duration-secs 10`, it completed 46,914,512 logical appends at 4,529,831.68
appends/s, with 2,936,253 routed runtime requests at 283,509.97 routed req/s.
Whole-process CPU averaged about 631.0% and peaked at 788.5%. It used all 10
cores and all 160 groups, recorded `mailbox_full_events: 0`, and had
`group_mailbox_max_depth: 45`.

The direct OpenRaft run is still not final acceptance evidence because it
bypasses HTTP and `perf_compare`, but it is a stronger control than the earlier
pure in-memory direct stress. It shows that the OpenRaft in-memory group path
can consume substantially more CPU and routed request rate than the current
HTTP `perf_compare` path. A 5-second `sample` during a 15-second HTTP run showed
active CPU spread across hyper/axum HTTP/1 parsing, body collection, socket
read/write, allocation/free, time/hash/wakeup overhead, and OpenRaft
watch/engine-command notification. It did not show S3, local disk WAL, or a
sustained group-mailbox backlog as the root cause.

The same boundary reproduced on EC2. On May 17, 2026, release binaries were
built on a `c7g.4xlarge` and tested with one `c7gn.8xlarge` client plus three
`c7g.4xlarge` servers. Existing disk-backed `ursula` processes on port 4437
were stopped for the test; vNext HTTP servers ran separately on port 4466 with
`--core-count 16 --raft-group-count 256 --raft-memory`.

The EC2 direct OpenRaft control saturated one server host: with
`ursula-raft-runtime-stress --core-count 16 --raft-group-count 256
--stream-count 8192 --producer-count 8192 --setup-concurrency 2048
--batch-size 16 --payload-bytes 100 --duration-secs 12`, `pidstat` reached
1,598% CPU on a `c7g.4xlarge`. The run completed 63,192,384 logical appends at
3,462,259.92 appends/s and 216,840.08 routed runtime requests/s, with all 16
cores and all 256 groups active and no mailbox-full events.

EC2 `perf_compare` did not saturate the same server:

- `--concurrency 1024`, batch size 16, minimal ack, pipeline depth 1:
  8,449,872 logical appends in 15.011s, 529,142 routed runtime requests, server
  CPU about 2.9 active cores, client CPU about 2.3 cores, and
  `group_mailbox_max_depth: 5`.
- `--concurrency 4096` with the same batch shape: 7,844,208 logical appends in
  15.044s, 494,360 routed runtime requests, and no downstream backlog.
- `--concurrency 1024 --ursula-append-pipeline-depth 4`: 9,236,752 logical
  appends in 15.087s, 578,322 routed runtime requests, server CPU peaking at
  3.13 cores, and `group_mailbox_max_depth: 22`.
- batch size 64 with pipeline depth 4: 12,832,320 logical appends in 15.275s,
  but only 201,530 routed runtime requests and lower server CPU, about
  1.8 cores.
- three parallel `perf_compare` processes from the `c7gn.8xlarge`, one per
  `c7g.4xlarge` server, reached about 1.61M aggregate logical appends/s. Each
  server still stayed around 2.55 active cores, with group mailbox max depth
  only 7-8 and no mailbox-full events.

This EC2 run changes the diagnosis from "maybe the MacBook cannot generate
enough load" to "a single current `perf_compare` HTTP target has an ingress
ceiling around tens of thousands of routed batch requests per second." The
client host can scale aggregate pressure by running multiple benchmark
processes against different servers, but each individual server remains far
below the direct OpenRaft runtime CPU ceiling.

The next EC2 split tested whether that ceiling belongs to the server target or
to a single `perf_compare` process. Three `perf_compare` processes were pointed
at the same `c7g.4xlarge` server. They completed with zero errors and pushed
that single server to 746.7% average active CPU, with a 967.0% peak. The server
recorded 25,484,960 accepted appends, 1,595,885 routed runtime requests,
`mailbox_full_events: 0`, `group_mailbox_depth: 0`, and
`group_mailbox_max_depth: 49`. This shows the server can be driven well past
the single-process 2.5-3.1-core plateau; one `perf_compare` process is the
first limiter.

More client processes can push CPU higher, but the system starts entering
overload rather than clean throughput scaling:

- four `perf_compare` processes against one server pushed active server CPU to
  943.1% average and 1,346.0% peak. The server accepted 29,572,976 appends and
  1,852,411 routed runtime requests, but the benchmark reported 1,008 total
  status-0 errors across the four processes and the runtime recorded 2,876
  `mailbox_full_events`.
- five `perf_compare` processes pushed active server CPU to 1,062.8% average
  and 1,513.0% peak, nearly saturating the 16-vCPU host at peak. The server
  accepted 30,748,864 appends and 1,926,929 routed runtime requests, but the
  benchmark reported 1,056 total status-0 errors and the runtime recorded 7,638
  `mailbox_full_events`.

This confirms there is additional CPU headroom beyond the three-process run,
but it is reached by overfeeding the server and creating queueing/backpressure.
It is useful as a stress signal, not as the clean final-goal shape.

The raw HTTP diagnostic was then run on EC2 to remove axum/hyper routing from
the server. A single `perf_compare` process against `ursula-http-raw` produced
9,089,008 logical appends in 15.089s, or 602,372.71 logical appends/s. That is
the same throughput class as the axum/OpenRaft HTTP server under the same
client shape, while the raw server used only about 1.39 active cores. Therefore
server-side axum/hyper routing is not the primary reason one current
`perf_compare` process cannot feed more requests.

`perf record` on the single-process client made the limiter concrete. The
client-side hot symbols were allocator and memory-copy work, Tokio/hyper
scheduling atomics and semaphores, URL parsing, reqwest request execution,
Hyper HTTP/1 parsing/flush, and `HeaderMap` operations. This corresponds to the
benchmark code path in `append_batch_statuses`: every batch append formats a
fresh URL, clones the batch bytes into a new request body, constructs a new
reqwest request and headers, sends it through Hyper HTTP/1, and expands a 204
minimal ack into a new status vector.

The refined root cause is:

```text
single perf_compare process
  -> per batch request reqwest/HTTP construction + allocation + URL/header work
  -> response-driven future scheduling
  -> too few HTTP batch requests per second per target
  -> Ursula server runtime remains underfed
```

This is narrower than "HTTP is slow." The server-side OpenRaft runtime can
saturate a host when driven directly, and the same HTTP server can become much
busier when multiple benchmark processes feed it. The current final-goal gap is
the single-process, per-request HTTP benchmark ingress model.

The read phase has also been smoke-tested against the same vNext HTTP prototype:

```bash
cargo run -p perf-compare --bin perf_compare -- \
  --targets ursula \
  --ursula http://127.0.0.1:4447 \
  --durable http://127.0.0.1:1 \
  --s2 http://127.0.0.1:1 \
  --phases read \
  --concurrency 16 \
  --throughput-secs 1 \
  --read-payload-bytes 128 \
  --request-timeout-secs 5 \
  --setup-concurrency 64
```

That run produced 26,954 successful catch-up reads, all `200`, with zero
benchmark errors. The server metrics after read setup reported 16 accepted seed
appends, 4 active cores, 16 active Raft groups, and empty mailboxes.

The SSE phase has also been smoke-tested:

```bash
cargo run -p perf-compare --bin perf_compare -- \
  --targets ursula \
  --ursula http://127.0.0.1:4447 \
  --durable http://127.0.0.1:1 \
  --s2 http://127.0.0.1:1 \
  --phases sse \
  --sse-count 10 \
  --sse-readers 2 \
  --sse-payload-bytes 128 \
  --request-timeout-secs 10
```

That run delivered 20 SSE events to 2 readers with zero benchmark errors.

The mixed phase has also been smoke-tested:

```bash
cargo run -p perf-compare --bin perf_compare -- \
  --targets ursula \
  --ursula http://127.0.0.1:4447 \
  --durable http://127.0.0.1:1 \
  --s2 http://127.0.0.1:1 \
  --phases mixed \
  --throughput-secs 1 \
  --payload-bytes 128 \
  --read-payload-bytes 128 \
  --sse-payload-bytes 128 \
  --sse-count 10 \
  --mixed-appenders 8 \
  --mixed-readers 8 \
  --mixed-sse-readers 2 \
  --ursula-append-mode batch \
  --ursula-append-batch-size 4 \
  --request-timeout-secs 10 \
  --setup-concurrency 64
```

That run produced 60,284 successful appends, 19,017 successful reads, 20 SSE
deliveries, and zero benchmark errors. Server metrics after the run reported
60,302 accepted appends, 4 active cores, 17 active Raft groups, and empty
mailboxes.

A latency smoke with `--phases latency --latency-count 20 --payload-bytes 128`
also completed with zero errors.

## EC2 cold-enabled durable-log run

After S3 cold path was wired through static gRPC and durable OpenRaft logs, a
30-second EC2 `perf_compare` run exposed a cold-planning bottleneck before it
became a CPU-saturation question.

The cluster shape was three `c7g.4xlarge` servers, one `c7gn.8xlarge` client,
64 Raft groups, 16 cores, independent `--raft-log-dir` roots, and
`URSULA_COLD_BACKEND=s3`. With 512 concurrent streams, 128-byte payloads,
append-batch size 16, minimal acks, and a 4 MiB per-group hot cap, the old
planner reached the cap and returned 503s in the small-event phase:

```text
write_throughput: ok_requests=1751856 errors=0 requests_per_sec=57838.0
small_event_write: ok_requests=345296 errors=794192 status_counts={204,503}
node1 cold_hot_bytes=268435456
node1 cold_hot_group_bytes_max=4194304
node1 cold_backpressure_events=49637
node1 cold_flush_uploads=0
```

Lowering the background threshold to 64 KiB produced the same result. The root
cause was not S3 throughput: the planner considered only the first hot segment
of a stream, while append-batch records each 128-byte frame as its own retained
message. Therefore no stream produced a candidate above 64 KiB or 1 MiB even as
the group aggregate reached the hot-byte cap. A manual single-stream flush with
`min_hot_bytes=1` uploaded and published one 128-byte chunk, proving the upload
and Raft metadata publish path still worked.

After changing `StreamStateMachine::plan_cold_flush()` to coalesce contiguous
hot segments before thresholding, the same EC2 shape with binary sha256
`50ad58c2ba3da5a6e8230322ef0cac05efad8fc7f5fda928c11f9e8685e9d33b` completed
the same 30-second run with zero errors:

```text
write_throughput: ok_requests=2219776 errors=0 requests_per_sec=73492.38
small_event_write: ok_requests=1064240 errors=0 requests_per_sec=35135.73
```

Post-run node 1 metrics:

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

This is still not the final CPU-saturation gate. It proves that cold-enabled
durable-log writes can keep accepting a multi-stream small-event load while
offloading to S3 and keeping hot bytes bounded. The remaining CPU-saturation
work is again above this cold-planning bug: benchmark ingress and server-side
CPU utilization under the accepted workload.

These smoke runs prove protocol-harness compatibility for the current
latency/write/small/read/SSE/mixed subset. They are not CPU saturation proof.
The release CPU-sampled write/small runs above are the relevant current gate,
and that gate is not yet passed.

## EC2 distributed-leader S3 mixed SSE finding

A later EC2 run switched the static cluster to `--raft-memory`,
`--raft-init-membership-per-group`, 64 Raft groups, and
`URSULA_COLD_BACKEND=s3`, then drove write, small, read, and mixed phases from
the `c7gn.8xlarge` client with read bases spread across all three server
nodes. The non-SSE portions were clean for 45 seconds:

```text
write_throughput: ok_requests=7,238,336 errors=0 requests_per_sec=160,044.18
small_event_write: ok_requests=5,461,200 errors=0 requests_per_sec=121,017.2
read_throughput: ok_requests=2,471,126 errors=0 requests_per_sec=54,839.45
mixed_live.append: ok_requests=4,168,336 errors=0 requests_per_sec=92,629.69
mixed_live.read: ok_requests=1,039,984 errors=0 requests_per_sec=23,110.76
```

The same run reported only 6 mixed SSE deliveries and 794 mixed SSE errors.
`perf_compare` counts this phase by opening `offset=now&live=sse` readers, then
for each appended token requiring every SSE reader to receive that token within
10 seconds. That failure mode was therefore not a generic throughput drop.

The root cause was a live-tail ownership bug introduced by combining
leader-forwarded group reads with runtime-local live waiter registration.
`wait_read_stream()` registered the SSE/long-poll waiter in the runtime that
accepted the HTTP connection. On a follower node, the initial `read_stream`
inside waiter admission could forward to the Raft leader and correctly observe
"up to date", but the waiter itself remained in the follower runtime's
`read_watchers` map. Later appends applied on the leader runtime and invoked
`notify_read_watchers()` only there, so follower-accepted live readers could
sleep until client timeout even though the stream was progressing.

The fix is structural: live reads now preflight
`ShardRuntime::require_local_live_read_owner()` before HTTP sends SSE headers
or enters long-poll. The default in-memory group engine accepts local live
waiters; the OpenRaft group engine requires the local node to be the group
leader and otherwise returns the existing `GroupLeaderHint`. HTTP converts that
to the ordinary leader `307` for `GET`, so SSE/long-poll connections land on
the node that will apply appends and wake owner-local waiters. Ordinary
catch-up `GET` still uses leader-forwarded reads and is not forced to redirect.

Local evidence after the fix:

```bash
cargo test -p ursula-http \
  static_grpc_raft_group_engine_replicates_between_routers -- --nocapture
```

The focused test now verifies both sides of the boundary: a follower catch-up
read still succeeds through internal gRPC forwarding, while a follower
`GET ?offset=now&live=sse` returns `307` with `x-ursula-raft-leader-id: 1` and
a leader `Location`.

A local three-process mixed run then exercised the actual `perf_compare` SSE
reader behavior with read bases spread across node 1, node 2, and node 3. This
run used node 1 as leader for all groups so that follower read bases would
exercise the live-read redirect path deterministically:

```bash
/Users/xing/Idea/riverrun/target/debug/perf_compare \
  --targets ursula \
  --ursula http://127.0.0.1:4501 \
  --ursula-read-bases \
    http://127.0.0.1:4501,http://127.0.0.1:4502,http://127.0.0.1:4503 \
  --phases mixed \
  --throughput-secs 5 \
  --ursula-append-mode batch \
  --ursula-append-batch-size 8 \
  --ursula-append-batch-minimal-ack \
  --mixed-appenders 16 \
  --mixed-readers 12 \
  --mixed-sse-readers 6 \
  --sse-count 20 \
  --validate-read-len
```

Result:

```text
mixed append: ok_requests=67,048 errors=0
mixed read: ok_requests=13,477 errors=0
mixed SSE: count=120 errors=0 p50=7.8ms p99=14.43ms
node metrics after run: live_read_waiters=0, live_read_backpressure_events=0
```

```bash
cargo test -p ursula-http --all-targets
```

The library and binary tests passed. One `static_cluster_cli` test hit a
readiness-timeout flake during the full run and passed when rerun directly:

```bash
cargo test -p ursula-http --test static_cluster_cli \
  cli_static_grpc_raft_log_dir_replicates_between_nodes -- --nocapture
```

The fixed binary was then rebuilt on EC2 as Linux aarch64
`be9950e4579cf676cfd386ae3640d205af2a7a829f72e5da57e5cff7d249216e`, deployed
to the three `c7g.4xlarge` servers and the `c7gn.8xlarge` client, and run on
port `4490` with `--raft-memory`, 64 groups, per-group distributed leaders,
and `URSULA_COLD_BACKEND=s3` under
`ursula-livefix-s3/20260518T161533Z`.

Manual SSE probes from the client confirmed the wire behavior:

```text
GET node2 /benchcmp/{stream}?offset=now&live=sse
307 Temporary Redirect
location: http://10.99.1.48:4490/benchcmp/{stream}?offset=now&live=sse
x-ursula-raft-leader-id: 1
```

Following that redirect with `curl -N -L`, then appending through node 1,
delivered the expected control/data/control SSE sequence for both a direct
leader connection and a follower-redirected connection.

The actual `perf_compare` mixed workload also passed when run as the isolated
mixed phase:

```text
mixed append: ok_requests=2,168,512 errors=0 requests_per_sec=72,283.73
mixed read: ok_requests=823,972 errors=0 requests_per_sec=27,465.73
mixed SSE: count=800 errors=0 p50=23.95ms p99=33.38ms
```

A 15-second `write,mixed` matrix also passed:

```text
write: ok_requests=854,816 errors=0
mixed append: ok_requests=726,144 errors=0
mixed read: ok_requests=224,409 errors=0
mixed SSE: count=400 errors=0 p50=24.36ms p99=38.71ms
```

The remaining unresolved observation is phase interaction, not the original
live waiter placement bug. Full `write,small,mixed,read` and `small,mixed`
runs still produced no mixed SSE deliveries after a preceding high-pressure
write phase. Post-failure metrics did not show mailbox-full, live-read
backpressure, cold backpressure, or residual live waiters, but the S3 cold
backlog had grown substantially:

```text
node1 accepted_appends=36,880,560 cold_hot_bytes=1,158,740,044
node2 cold_hot_bytes=325,427,744
node3 cold_hot_bytes=328,787,040
mailbox_full_events=0
live_read_waiters=0
live_read_backpressure_events=0
cold_backpressure_events=0
```

So the live-read-owner fix is validated in EC2 for direct SSE, follower
redirect SSE, and isolated heavy mixed load. The next performance question is
why a prior sustained write phase poisons the following mixed SSE phase without
surfacing as explicit mailbox/live/cold backpressure.

## SSE HTTP instrumentation for phase interaction

The next diagnostic added HTTP-layer SSE counters to `GET /__ursula/metrics`:

- `sse_streams_opened`
- `sse_read_iterations`
- `sse_data_events`
- `sse_control_events`
- `sse_error_events`

These counters are deliberately outside the runtime state machine. They answer
whether the failing `small,mixed` shape opens SSE responses at all, whether the
response body is being polled, whether the server renders data/control events,
and whether the SSE path exits through an error event.

Use the counters in the next EC2 replay as follows:

- If `sse_streams_opened` stays at zero, the mixed phase is not reaching the
  server-side SSE route; investigate the client phase transition or connection
  pool.
- If streams open but `sse_read_iterations` stays low, the HTTP body is not
  being polled enough; investigate client consumption or HTTP scheduling.
- If iterations grow but `sse_data_events` stays zero while appends succeed,
  the live wait/read path is not observing committed appends under the
  post-write phase state.
- If `sse_data_events` grows but `perf_compare` reports zero deliveries, the
  bug is likely after server rendering, in the client SSE parser/reader or
  connection handling.

Local verification:

```bash
cargo fmt --all -- --check
cargo test -p ursula-http \
  sse_live_tail_delivers_appended_text_and_closed_control -- --nocapture
cargo test -p ursula-http \
  metrics_expose_per_core_and_group_append_distribution -- --nocapture
cargo check -p ursula-http --all-targets
cargo clippy -p ursula-http --all-targets -- -D warnings
```

All commands passed. The SSE test now verifies one opened SSE stream, one data
event, one control event, and zero error events after a close-delivered live
tail. The metrics regression test verifies that a non-SSE workload exposes the
new fields with zero values.

## Leader-runtime write forwarding fix

The SSE counters isolated the remaining full-phase failure. In the failed
`write,small,mixed,read` run, `perf_compare` opened the mixed SSE stream on
node 2:

```text
node2 sse_streams_opened=8
node2 sse_read_iterations=16
node2 sse_data_events=0
node2 sse_control_events=8
node2 sse_error_events=0
```

This proved the HTTP route and response body were active, but node 2 never
rendered data for the mixed SSE stream. At the same time, all non-SSE appends
were returning `204`.

The structural bug was below HTTP but above Raft: follower `RaftGroupEngine`
methods were internally forwarding group writes directly to the group leader
over the Raft-internal gRPC `group_write` path, then returning the successful
write result to the original runtime. That made follower-originated writes
look successful to HTTP, but the leader runtime did not execute the original
runtime command path and therefore did not call `notify_read_watchers()` in
the leader-local watcher registry. SSE readers on the leader could sit on an
up-to-date waiter while appends were applied through an internal group path
that bypassed the runtime wake boundary.

The fix is to preserve the ownership boundary: follower group writes now
return a leader hint instead of performing group-level write forwarding. The
existing HTTP write-forward path then forwards the whole write request to the
leader node, where the leader runtime owns both the Raft group write and the
live watcher notification. Group-level read forwarding remains valid for
catch-up reads because it does not need to mutate the leader-local watcher
registry.

Local verification:

```bash
cargo fmt --all -- --check
cargo check -p ursula-http --all-targets
cargo clippy -p ursula-http --all-targets -- -D warnings
cargo test -p ursula-http \
  static_grpc_raft_group_engine_replicates_between_routers -- --nocapture
```

All commands passed. The focused static-gRPC test now includes the exact
regression shape: open an SSE stream on the group leader, append to the same
stream through a follower HTTP endpoint, and require the leader SSE body to
receive the appended token. The previous group-level forwarding boundary could
return a successful follower append while bypassing the leader runtime watcher
wake; this test protects that ownership invariant.

The fixed Linux aarch64 binary was rebuilt as:

```text
57d54d56d1628e66e4fd4a1da10736d18f6129eafba67b8eb64d5cf4d8c6ecac  /tmp/ursula-http-leader-runtime-forward
```

It was deployed to the three `c7g.4xlarge` servers and the `c7gn.8xlarge`
client on port `4491`, with `--raft-memory`, 64 groups, per-group distributed
leaders, and real S3 cold storage under:

```text
ursula-leader-runtime-forward-20260518T170906Z
```

The same full EC2 workload that previously failed completed successfully:

```text
phases: write,small,mixed,read
write: ok_requests=4,524,368 errors=0 requests_per_sec=150,403.93
small: ok_requests=2,815,408 errors=0 requests_per_sec=93,264.22
mixed append: ok_requests=2,335,552 errors=0 requests_per_sec=77,851.73
mixed read: ok_requests=290,281 errors=0 requests_per_sec=9,676.03
mixed SSE: count=800 errors=0 p50=27.38ms p99=61.89ms
read: ok_requests=1,680,589 errors=0 requests_per_sec=55,833.04
```

Final metrics confirmed the ownership fix: the mixed SSE stream leader was
node 2, and node 2 rendered all data events while also accepting local appends:

```text
node1 accepted_appends=3,118,519 active_cores=16 active_groups=22
node2 accepted_appends=3,572,525 active_cores=16 active_groups=21
node2 sse_streams_opened=8 sse_read_iterations=816
node2 sse_data_events=800 sse_control_events=808 sse_error_events=0
node3 accepted_appends=2,984,704 active_cores=16 active_groups=21
mailbox_full_events=0
live_read_waiters=0
live_read_backpressure_events=0
cold_backpressure_events=0
```

The temporary EC2 services were stopped and the S3 root was deleted after the
run.

## EC2 docs-web benchmark refresh with multi-process clients

The benchmark page was refreshed with a documented high-concurrency Ursula-only
run using the same EC2 topology: three `c7g.4xlarge` servers, one
`c7gn.8xlarge` load generator, port `4492`, `--raft-memory`, 64 Raft groups,
per-group initial membership, and `URSULA_COLD_BACKEND=s3`. Server binaries
were the node-1 native aarch64 build at sha256
`a9176bade65e258d297959b371f21f4082e752c817cb22dc436365affcbcd050`; the client
used the newer native `perf_compare` binary at sha256
`d4d3809892e2c88e4599dc0f9fb7ad0d18dc15dd73ffeba86afdc953cd014e54`.

The workload followed `docs/web/src/pages/BenchmarkPage.tsx`'s session-event
shape:

```text
--phases latency,small
--payload-bytes 100
--small-payload-bytes 100
--latency-count 50
--throughput-secs 8
--ursula-append-mode batch
--ursula-append-batch-size 16
```

For c1024 and above the load generator used multiple `perf_compare` processes
with disjoint buckets so one reqwest client process did not cap the server-side
load. Results:

```text
c1024 = 4 x c256, rotated entrypoints:
  small_event_write_ok=5,429,280
  small_event_write_rps_sum=673,505.32
  small_event_write_mib_s_sum=64.24
  append_p50_median=2.32ms
  append_p99_max=3.28ms
  read_one_p50_median=0.65ms
  read_one_p99_max=1.78ms
  errors=0

c2048 = 8 x c256, fresh cluster/root, rotated entrypoints:
  small_event_write_ok=5,443,056
  small_event_write_rps_sum=673,734.78
  small_event_write_mib_s_sum=64.26
  append_p50_median=1.55ms
  append_p99_max=50.54ms
  read_one_p50_median=0.75ms
  read_one_p99_max=4.19ms
  errors=0

c4096 = 16 x c256, fresh cluster/root, rotated entrypoints:
  small_event_write_ok=5,654,032
  small_event_write_rps_sum=693,308.93
  small_event_write_mib_s_sum=66.11
  append_p50_median=2.06ms
  append_p99_max=58.47ms
  read_one_p50_median=0.80ms
  read_one_p99_max=63.34ms
  errors=0
```

Post-run metrics from the first back-to-back run confirmed that this was an
S3-cold-enabled run, not a pure in-memory smoke. The three nodes had leader
distribution `22/21/21` and had uploaded about 657 MiB of cold data in
aggregate:

```text
node1 accepted_appends=3,585,551 cold_hot_bytes=17,663,900 cold_upload_bytes=340,532,800
node2 accepted_appends=2,022,118 cold_hot_bytes=37,397,400 cold_upload_bytes=160,515,200
node3 accepted_appends=1,968,095 cold_hot_bytes=23,676,700 cold_upload_bytes=156,196,800
```

Two earlier c2048 runs diagnosed the client-side benchmark artifact. The first
back-to-back run was only 388,413.21 req/s because it ran immediately after
c1024 without restarting services or clearing cold/background state. A fresh
fixed-node1-entrypoint rerun improved to 507,389.20 req/s, but node 1 still did
about twice as many mutations as node 2 or node 3 because every client process
entered through node 1 and non-local group leaders were reached through
forwarding. Rotating `--ursula` entrypoints across node 1, node 2, and node 3
raised c2048 to 673,734.78 req/s and balanced mutations:

```text
node1 accepted_appends=1,858,124 applied_mutations=1,860,874
node2 accepted_appends=1,825,897 applied_mutations=1,828,453
node3 accepted_appends=1,759,443 applied_mutations=1,762,121
mailbox_full_events=0
group_mailbox_full_events=0
cold_backpressure_events=0
```

The root cause of the apparent c2048 regression was therefore the benchmark
client's single-ingress-node shape, not S3 bandwidth, cold backpressure,
mailbox saturation, or an inherent c2048 server-side regression. The
`perf-many` helper now rotates Ursula targets by default and keeps
`--target-mode first` for reproducing the old single-entrypoint behavior.

The apparent c4096 regression on the benchmark page had the same evidence
problem: c1024/c2048 had been refreshed with multi-process rotated entrypoints,
while c4096 still came from an older single-process/single-entrypoint sweep.
Rerunning c4096 with the same 16-process rotated-entrypoint shape reached
693,308.93 req/s with balanced node work and no mailbox or cold backpressure:

```text
node1 accepted_appends=1,899,688 applied_mutations=1,903,120
node2 accepted_appends=1,883,321 applied_mutations=1,886,766
node3 accepted_appends=1,871,839 applied_mutations=1,875,165
mailbox_full_events=0
group_mailbox_full_events=0
cold_backpressure_events=0
```

The page records the clean rotated c1024/c2048/c4096 values in the "Latest
Ursula EC2 Refresh" table and in the session-event trend. Older c8192/c16384
session points were removed from the refreshed chart until they can be rerun
under the same client/entrypoint shape. The temporary `4492` services were
stopped and the S3 root `ursula-ec2-e2e-20260518-4492` was deleted after the
run.

## EC2 reference comparison against Durable Streams and S2 Lite

On `2026-05-18`, a same-client-host reference comparison was run from the
`c7gn.8xlarge` client against services hosted on EC2 node 1, using the same
node1-native-built `perf_compare` binary and the same workload shape:

```text
phases=write,small,read
concurrency=128
throughput_secs=30
payload_bytes=128
small_payload_bytes=128
read_payload_bytes=1024
setup_concurrency=128
validate_read_len=true
```

The compared services were not durability-equivalent:

- Durable Streams official reference server from
  `durable-streams/durable-streams` commit
  `8d7852494c30a315f618253e9bce1e846b5c5937`, file-backed with `dataDir` on
  node 1, port `4438`.
- S2 Lite `0.33.0`, started with
  `s2 lite --bucket ursula-c7g-beast-us-east-1 --path s2-lite-compare-20260518
  --port 8081`, so it used S3 object storage.
- Ursula three-node static cluster on port `4492`, using the node1-native
  `ursula-http` binary with sha256
  `a9176bade65e258d297959b371f21f4082e752c817cb22dc436365affcbcd050`,
  `--raft-memory`, distributed per-group leaders, and S3 cold storage under
  `ursula-ec2-e2e-20260518-4492`.

Throughput results:

```text
durable official reference, file-backed:
  write: 205,710 ok, 6,853.91 req/s, 0 errors
  small: 184,818 ok, 6,156.94 req/s, 0 errors
  read:  176,941 ok, 5,893.86 req/s, 0 errors

s2 lite, S3-backed:
  write: 58,784 ok, 1,957.92 req/s, 0 errors
  small: 59,454 ok, 1,981.49 req/s, 0 errors
  read:  1,708,059 ok, 56,934.24 req/s, 0 errors

ursula, three-node raft-memory + S3, plain post append:
  write: 422,880 ok, 14,067.81 req/s, 0 errors
  small: 410,370 ok, 13,651.80 req/s, 0 errors
  read:  1,866,213 ok, 62,000.21 req/s, 0 errors

ursula, three-node raft-memory + S3, append-batch extension:
  write: 8,904,544 ok, 296,322.81 req/s, 0 errors
  small: 7,273,936 ok, 242,061.08 req/s, 0 errors
  read:  1,877,327 ok, 62,371.55 req/s, 0 errors
```

The useful conclusion is not a single ranking. The Durable Streams reference
server is a correctness/reference implementation backed by local files, S2 Lite
was using S3 directly, and Ursula was a three-node Raft service with only the
cold path on S3 and an in-memory Raft log. The comparison is still useful
because it used one EC2 client host and one client harness. It shows Ursula's
ordinary per-event HTTP append path is already above the official reference
server on this workload, while Ursula's high-throughput numbers come from its
batch append extension and should be reported separately from the common append
shape.

## EC2 S3 telemetry with all server IAM roles fixed

The `4491` CPU telemetry above was followed by a cleaner `4492` rerun after
checking the EC2 instance profiles on all three server nodes. The first `4492`
attempt exposed an environment issue rather than a Ursula cold-path bug: node 1
and node 3 could fetch an IMDSv2 token but had no IAM role attached, while node
2 had the `riverrun-e2e-node` instance profile. As a result only node 2 could
upload to S3, and node 1/node 3 accumulated hot bytes while logging credential
loading failures. The missing instance profile was attached to node 1 and node
3, `aws sts get-caller-identity` was verified on all three nodes, the services
were restarted, and the S3 root was cleared before the measured rerun.

Deployment shape:

```text
three c7g.4xlarge servers, one c7gn.8xlarge client
port 4492
--raft-memory
--raft-init-membership-per-group
64 Raft groups
leaders 22/21/21 on every node
URSULA_COLD_BACKEND=s3
URSULA_COLD_S3_BUCKET=ursula-c7g-beast-us-east-1
URSULA_COLD_S3_ROOT=ursula-ec2-e2e-20260518-4492
```

The full single-process `perf_compare` workload used concurrency 256,
batch-size 16, minimal append-batch acknowledgements, real S3 cold storage,
and three read bases. It completed with zero errors:

```text
write: ok_requests=10,668,496 requests_per_sec=355,023.17 MiB/s=43.34
small: ok_requests=8,593,392 requests_per_sec=284,264.87 MiB/s=34.70
read: ok_requests=1,651,221 requests_per_sec=54,747.94 MiB/s=53.46
mixed append: ok_requests=4,935,504 requests_per_sec=164,516.80 MiB/s=20.08
mixed read: ok_requests=361,517 requests_per_sec=12,050.57 MiB/s=11.77
mixed SSE: count=800 errors=0 p50=13.67ms p99=21.51ms
```

All three nodes uploaded to S3 and drained hot bytes back to a small steady
state by the end of the run:

```text
node1 accepted_appends=9,574,142 cold_upload_bytes=1,219,737,600 cold_hot_bytes=5,851,136
node2 accepted_appends=7,532,790 cold_upload_bytes=957,751,296 cold_hot_bytes=6,121,472
node3 accepted_appends=7,090,880 cold_upload_bytes=901,881,856 cold_hot_bytes=5,595,136

cold_backpressure_events=0
mailbox_full_events=0
wal_records=0
```

Server `pidstat` tails during the later high-CPU window showed every node well
past the original 3-4 core plateau:

```text
node1 tail total CPU: about 12.9-13.5 vCPU
node2 tail total CPU: about 10.8-11.3 vCPU
node3 tail total CPU: about 10.0-10.6 vCPU
```

A follow-up higher write-pressure run used the same cluster with
`--phases write,small`, concurrency 512, append-batch size 16, minimal acks, and
pipeline depth 2. Throughput was lower than the concurrency-256 full run's
write window, so this parameter set is not a better throughput shape:

```text
write: ok_requests=8,088,768 requests_per_sec=268,687.67
small: ok_requests=7,218,064 requests_per_sec=238,034.48
```

It did confirm that the server can be driven near per-node CPU saturation when
the client creates enough runnable write work:

```text
node1 samples=40 avg_total_cpu=1263.15 max_total_cpu=1516.00
node2 samples=40 avg_total_cpu=853.27 max_total_cpu=1110.00
node3 samples=40 avg_total_cpu=786.91 max_total_cpu=944.00
```

This result changes the performance status: the old single-machine 3-4 core
ceiling and the earlier EC2 leader-node concentration are no longer the observed
limits for the `--raft-memory + S3` static cluster. The remaining gate is not
"can Ursula use more than a few cores"; it can. The remaining gate is to run a
longer accepted workload with a production-grade multi-process or lower-overhead
client harness, while keeping S3 hot bytes bounded and all protocol phases
healthy for the duration.

## Read Scaling Boundary

The current read path has a different scaling boundary than the write path.
Writes can distribute independent streams across many Raft groups and group
leaders, but a single stream/group read is intentionally leader-owned:

- public writes and local live reads execute on the Raft group leader, so each
  group uses one replica for strongly consistent serving;
- OpenRaft exposes state-machine access through `with_state_machine`, an
  exclusive access point rather than a read/write lock, so same-group reads are
  serialized at the group state-machine boundary;
- read capacity therefore scales primarily with the number of active groups and
  leader distribution, not with replica count inside one group.

That tradeoff is acceptable for the initial strongly consistent implementation,
but it changes how to interpret read and mixed benchmarks. A three-replica group
does not imply three-way read fan-out capacity unless Ursula later adds a
follower-read protocol with a freshness proof or a separate read view.

The live-tail/SSE notification path had an additional amplification bug:
`notify_read_watchers` removed all waiters for a stream and then reran
`read_stream` once per waiter. With many identical SSE subscribers, one append
could become N independent read-plan builds and payload assemblies on the owner
group. The runtime now groups watchers by identical `ReadStreamRequest`, runs
one group read per distinct request, and broadcasts the cloned result to the
matching waiters.

The watcher registry is now group-actor-local state rather than an
`Arc<Mutex<HashMap<...>>>` shared with the core worker. Wait registration and
cancellation are routed as group commands, so notification cannot race with a
second task mutating the same watcher map while `notify_read_watchers` awaits a
pending read. This keeps the Tokio task-per-group model but makes the actor
ownership boundary explicit: the core worker routes, and the group actor owns
live-tail state.

Focused tests:

```bash
cargo test -p ursula-runtime cancel_read_watcher_removes_group_local_waiter -- --nocapture
cargo test -p ursula-runtime notify_read_watchers_shares_identical_reads_across_watchers -- --nocapture
cargo test -p ursula-runtime wait_read -- --nocapture
```

The read path now splits metadata planning from payload materialization at the
`GroupEngine` boundary. `read_stream_parts` returns either materialized bytes or
a `StreamReadPlan` plus cold-store handle. The group actor only waits for that
planning step, then spawns response materialization in a separate task; cold
object ranges are read while the group actor continues processing its mailbox.
Ordinary reads, long-poll admission checks, and live-tail watcher notification
all use the same parts path, so an SSE fan-out wake can share one read plan per
distinct request without doing object IO on the actor.
OpenRaft leader reads also compute the plan inside `with_state_machine` and read
cold bytes after the state-machine access returns. This removes the worst
cold-read head-of-line cases: S3 range IO no longer holds OpenRaft's internal
state-machine lock or the Ursula group actor turn.

Read materialization is also explicitly bounded by a runtime-wide semaphore,
currently sized from `RuntimeConfig::mailbox_capacity`. That keeps S3/fs range
reads off the actor while preventing an unbounded number of spawned
materialization tasks from becoming the next memory/backpressure bug. A focused
test pins the important invariant: when one materialization task is blocked and
holding the only permit, the owner group actor can still serve a HEAD request,
while a second read waits for the materialization permit rather than occupying
the actor.

Focused tests:

```bash
cargo test -p ursula-runtime runtime_read_uses_group_read_parts_fast_path -- --nocapture
cargo test -p ursula-runtime read_materialization_is_bounded_without_blocking_group_actor -- --nocapture
cargo test -p ursula-runtime notify_read_watchers_shares_identical_reads_across_watchers -- --nocapture
cargo test -p ursula-raft raft_group_engine_cold_admission_coalesces_append_batch_many_into_one_raft_entry -- --nocapture
cargo test -p ursula-http static_grpc_raft_durable_cold_flush_replicates_manifest -- --nocapture
```

## Core Reactor Boundary

The longer-term thread-per-core target is that the core, not the group, is the
execution scheduler. A Raft group should be an ownership/data unit inside the
core-owned map, not necessarily its own Tokio task:

```text
core thread
  mailbox.recv()
  group = groups.get_mut(group_id)
  group.apply(command)
```

The current implementation still has both a core mailbox and per-group actor
tasks. That is a useful migration shape because it already moves stream state
under core/group ownership and avoids one global state machine, but it is not
the final high-performance reactor model. A group actor that awaits Raft, S3, or
per-watcher reads becomes a scheduling unit again, and Tokio decides fairness
between groups on that core.

The implementation should therefore move in two steps:

- first remove long awaits from group command execution, especially cold object
  reads and live-tail fan-out;
- then consider collapsing `CoreWorker` and `GroupActor` so a core owns
  `HashMap<RaftGroupId, GroupState>` directly, with Raft/network IO handled by
  separate workers or reactors and callbacks routed back to the owner core.

This keeps the semantic migration incremental while preserving the final design
direction: group ownership is local to a core; scheduling control belongs to the
core event loop.

## EC2 cold-admission Raft proposal coalescing

A follow-up profile of the same three-server `c7g.4xlarge` plus one-client
`c7gn.8xlarge` shape narrowed the remaining write-path CPU cost. The cluster
used `--raft-memory`, per-group distributed leaders, real S3 cold storage, and
four concurrent `perf_compare` processes with disjoint buckets:

```text
--phases write,small
--concurrency 256
--throughput-secs 15
--payload-bytes 128
--small-payload-bytes 100
--ursula-append-mode batch
--ursula-append-batch-size 16
--ursula-append-batch-minimal-ack
```

Before the fix, `perf record` on node 1 showed the hot on-CPU path in internal
Raft gRPC/HTTP2 rather than in the Durable Streams state machine:

```text
hyper::proto::h2::server::H2Stream
tonic RaftInternal::append
h2 frame/header decode and response send
rmp_serde decode
```

The runtime metrics explained why: enabling the S3 cold path also enabled cold
write admission, and the group actor only coalesced append-batch commands when
cold write admission was disabled. The S3 path therefore fell back to one
OpenRaft proposal per public append-batch request. In the baseline node 1
metrics, `accepted_appends=1,257,872` and `raft_write_many_batches=121,740`, or
about 10.3 logical appends per OpenRaft write batch. That is close to the public
append-batch size after setup and phase effects, not real group-level
coalescing.

The structural fix keeps cold admission enabled but moves coalescing ahead of
the OpenRaft proposal: the group actor now collects multiple append-batch
requests for the same Raft group, validates cold admission against a sequential
state-machine preview, and commits them as one `GroupWriteCommand::Batch` app
log entry. The same EC2 workload after the fix exited with zero client errors
and improved aggregate client-reported throughput:

```text
baseline write total: 157,234.90 logical appends/s
fixed    write total: 292,606.99 logical appends/s
baseline small total:  83,429.02 logical appends/s
fixed    small total: 173,050.71 logical appends/s
```

Server-side metrics also show the intended shape. Node 1 reported
`accepted_appends=2,621,344` and `raft_write_many_batches=22,832`, or about
114.8 logical appends per OpenRaft write batch. Nodes 2 and 3 showed the same
class of result, with roughly 74.7 and 74.8 logical appends per OpenRaft write
batch. There were no mailbox-full events, no cold backpressure events, and no
S3 uploads during this short run, so the measurement isolated the hot
append-to-Raft path rather than S3 bandwidth.

The process CPU profile also changed in the desired direction. Node 1 averaged
932.36% CPU and sustained around 14-15 vCPU during the measured write windows,
with substantially higher user-mode work than before. This does not close the
full CPU-saturation gate, but it removes a real structural regression where the
S3 cold path disabled Raft proposal coalescing and forced avoidable internal
gRPC/HTTP2 fanout.

The regression guard is local and mechanism-level, not only an EC2 benchmark:
`raft_group_engine_cold_admission_coalesces_append_batch_many_into_one_raft_entry`
creates a stream, records OpenRaft `last_log_index`, runs three
`append_batch_many_with_cold_admission` requests for the same group, and asserts
that the log index advances by exactly one while all logical appends remain
readable in order. That covers the specific failure mode where cold admission
turns public append batches back into one Raft proposal per request.

The metrics now also expose this shape directly. `raft_write_many_commands`
continues to count outer OpenRaft `client_write_many` app-log commands, while
`raft_write_many_logical_commands` recursively expands `GroupWriteCommand::Batch`
and counts the inner stream mutations. The regression test
`raft_metrics_count_logical_commands_inside_coalesced_batches` runs the real
OpenRaft group engine through the shard runtime with cold admission enabled and
asserts that three coalesced append-batch requests add three logical commands
even though the outer OpenRaft proposal count can stay lower. Future EC2 runs
can therefore report proposal coalescing from metrics directly instead of
inferring it from accepted logical appends divided by outer Raft batches.

## EC2 client telemetry after leader-runtime fix

The next run kept the same three-server `--raft-memory` plus S3 shape and added
client-side telemetry on the `c7gn.8xlarge` host. The first attempt used the
wrong metrics field name for readiness (`leader_id` instead of
`current_leader`), so no workload ran; the temporary processes were stopped by
pid file. The corrected run used port `4491`, 64 groups, per-group distributed
leaders, and S3 root:

```text
ursula-client-profile-20260518T173553Z
```

The full workload passed again:

```text
write: ok_requests=3,874,720 errors=0 requests_per_sec=128,757.53
small: ok_requests=3,272,496 errors=0 requests_per_sec=108,540.71
read: ok_requests=1,695,147 errors=0 requests_per_sec=56,297.02
mixed append: ok_requests=1,935,632 errors=0 requests_per_sec=64,521.07
mixed read: ok_requests=308,158 errors=0 requests_per_sec=10,271.93
mixed SSE: count=800 errors=0 p50=96.84ms p99=136.50ms
```

Final server metrics were still clean: 16 active cores on every node, active
groups distributed 22/21/21, no mailbox-full events, no live-read
backpressure, no cold backpressure, and SSE `800/800` rendered on the leader.
Node 2 uploaded 441 cold chunks totaling 462,422,016 bytes to S3. The S3 root
was deleted after the run.

Client `pidstat` on the single `perf_compare` process is the useful new signal:

```text
all samples: samples=121 avg_cpu=104.51 max_cpu=307.00
samples 0-29:   avg_cpu=263.95 max_cpu=307.00
samples 30-59:  avg_cpu=63.77  max_cpu=221.00
samples 60-89:  avg_cpu=39.27  max_cpu=79.00
samples 90-119: avg_cpu=52.40  max_cpu=65.00
```

The first 30-second write window only consumed about 2.6-3.1 client vCPU, and
the later read/mixed windows dropped below one client vCPU on average. That
rules out the client host as CPU-saturated, but it also shows the current
single-process reqwest harness is not generating enough runnable client work to
keep all server cores busy once the workload moves beyond the initial write
phase.

Client `perf record` captured 5,030 samples with zero lost samples. The top
symbols were HTTP client overhead rather than Ursula server work or S3:

```text
5.63% perf_compare  __aarch64_ldadd8_rel
5.16% libc.so.6     _int_free
4.20% libc.so.6     _int_malloc
```

The resolved stacks point at `bytes` reference counting and drops,
`reqwest::RequestBuilder::send`, `hyper` HTTP/1 response parsing, header map
allocation, `BytesMut::reserve_inner`, and tokio task scheduling. Combined with
the clean server-side metrics, this moves the remaining CPU-saturation gap to
the benchmark ingress path: the current single-process `perf_compare`
reqwest/HTTP shape is a limiter, especially for read and mixed phases. The next
valid saturation experiment should either run multiple independent client
processes with disjoint buckets or replace the client hot path with a lower
overhead generator; it should not be treated as a Raft/shard lock bottleneck
without new server-side evidence.

To make the multi-process experiment valid, `/Users/xing/Idea/riverrun`'s
`perf_compare` now accepts `--ursula-bucket`, defaulting to the previous
`benchcmp` value. This keeps existing commands comparable while allowing each
client process to target a disjoint Ursula bucket instead of racing on the same
hard-coded namespace. `cargo fmt -p perf-compare -- --check` and
`cargo check -p perf-compare` passed after the change.

The EC2 helper now has a `perf-many` subcommand that runs several configured
`perf_compare` processes concurrently from the client host and assigns each one
its own bucket by prefix. That makes the next "can more client processes drive
the servers harder?" experiment reproducible without hand-written EC2 shell.

The helper path was then smoke-tested on EC2 with two concurrent
`perf_compare` processes, disjoint buckets `benchcmp-smoke-00` and
`benchcmp-smoke-01`, the three `c7g.4xlarge` servers, `--raft-memory`,
per-group distributed leaders, and real S3 cold storage. The short workload was
`write,small` for 10 seconds per phase at per-process concurrency 128. Both
client processes exited with status 0. The post-run status showed leader
distribution 22/21/21 on every node and process CPU around 4.3-5.2 vCPU per
server:

```text
node1 pcpu=518 accepted_appends=994,304 cold_hot_bytes=127,270,912
node2 pcpu=469 accepted_appends=1,000,176 cold_upload_bytes=18,874,368
node3 pcpu=429 accepted_appends=936,400 cold_hot_bytes=119,859,200
```

This validates the `perf-many` path and confirms that more independent client
processes can push more aggregate server CPU than the single-process harness in
this workload. It still is not full saturation; the next run should scale the
process count and collect server/client pidstat in the same helper-controlled
shape. The temporary services were stopped and the S3 prefix check showed no
remaining `ursula-perfmany-smoke-*` objects.

## EC2 CPU telemetry after leader-runtime fix

After the leader-runtime write forwarding fix, the same full
`write,small,mixed,read` workload was rerun with `pidstat` sampling each server
process once per second for 125 seconds. The deployment shape was still three
`c7g.4xlarge` servers, one `c7gn.8xlarge` client, port `4491`,
`--raft-memory`, per-group distributed leaders, and real S3 cold storage under:

```text
ursula-cpu-telemetry-20260518T172108Z
```

Workload result:

```text
write: ok_requests=4,280,704 errors=0 requests_per_sec=142,075.27
small: ok_requests=3,171,840 errors=0 requests_per_sec=104,861.87
mixed append: ok_requests=2,150,544 errors=0 requests_per_sec=71,684.8
mixed read: ok_requests=252,371 errors=0 requests_per_sec=8,412.37
mixed SSE: count=800 errors=0 p50=28.62ms p99=114.71ms
read: ok_requests=1,676,981 errors=0 requests_per_sec=55,711.64
```

`pidstat` process CPU summaries:

```text
node1 samples=125 avg_cpu=810.568 max_cpu=1400.00
node2 samples=125 avg_cpu=562.568 max_cpu=1217.00
node3 samples=125 avg_cpu=805.386 max_cpu=1318.00
```

This is no longer the earlier three-to-four-core plateau. Across the three
servers the workload averaged about 21.8 vCPU and individual nodes peaked at
12.17 to 14.00 vCPU. It still is not full cluster CPU saturation: node 2
averaged materially lower than node 1 and node 3, and none of the nodes
averaged close to all 16 vCPU for the whole run.

Final metrics:

```text
node1 accepted_appends=3,095,732 active_cores=16 active_groups=22
node2 accepted_appends=3,629,527 active_cores=16 active_groups=21
node3 accepted_appends=2,878,249 active_cores=16 active_groups=21
mailbox_full_events=0
group_mailbox_depth=0
live_read_waiters=0
live_read_backpressure_events=0
cold_backpressure_events=0
```

The next performance question is therefore narrower: the architecture can drive
substantially more than a few cores under the real multi-phase S3 workload, but
we still need host/client telemetry or profiles to explain why average CPU does
not stay near 16 vCPU on every server.

The temporary EC2 services were stopped and the S3 root was deleted after the
run.

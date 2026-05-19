# Current CPU Plateau Analysis

## Observation

The current `riverrun` Ursula server reportedly stops scaling after roughly
three to four busy cores under `perf_compare`, even when the benchmark increases
concurrency.

The current evidence points to architecture-level serialization rather than a
client-side benchmark ceiling.

The first Ursula vNext release prototype improves distribution and throughput,
but it has not removed the CPU plateau. With release `ursula-http` running at
`--core-count 10 --raft-group-count 160`, CPU-sampled release `perf_compare`
write/small runs reached every configured core and all 160 groups, but the
server process still averaged about 4.8 busy cores and peaked below 5.8 busy
cores.

After append-batch was changed to route once per HTTP batch instead of once per
frame, logical append throughput improved, but server CPU fell to roughly 3.3
busy cores. That confirms the removed cost was per-frame runtime overhead, not
the final useful work needed to saturate CPU.

After the in-memory group engine was further changed to apply batch frames
directly instead of recursively calling the boxed single-append future, and
runtime batch metrics were changed to aggregate counter updates per batch,
`perf_compare` still stayed at roughly the same plateau. A 15-second
single-process small-event run at concurrency 1024 and batch size 16 reached
about 1.11M logical appends/s while server CPU averaged 296.6% and peaked at
327.0%. After the HTTP batch ack renderer was changed to avoid the intermediate
status vector and fast-path all-204 responses, a 10-second sample still reached
only about 1.125M logical appends/s while server CPU averaged 288.7% and peaked
at 323.4%. This makes the remaining bottleneck more likely to be
HTTP/Tokio/client ingress and allocator/response-path pressure than
group-internal future, metrics, or simple ack-rendering overhead.
After the parser was changed to keep each frame as a `Bytes` slice of the HTTP
body and the in-memory state machine gained a borrowed append path, another
10-second sample still reached only about 1.11M logical appends/s while server
CPU averaged 275.5% and peaked at 307.7%. Removing the parser's per-frame copy
therefore also does not solve the plateau.
After an explicit minimal-ack path changed successful append-batch responses to
`204 No Content` when requested by `perf_compare`, a 10-second sample reached
about 1.10M logical appends/s while server CPU averaged 278.1% and peaked at
314.5%. Removing successful-response JSON and client JSON decode also does not
solve the plateau.
A raw HTTP/1 diagnostic server that bypasses axum/hyper and implements only the
`perf_compare` create/append-batch subset reached about 1.09M logical appends/s
with server CPU averaging 207.3% and peaking at 242.2%. Making ingress thinner
therefore lowers server CPU but does not raise `perf_compare` request pressure.

## Perf Compare Workload Shape

`perf_compare` write throughput creates `--concurrency` independent streams and
spawns one append loop per stream. For Ursula, the benchmark can use either:

- standard `POST /{bucket}/{stream}`;
- `POST /{bucket}/{stream}/append-batch` with `--ursula-append-mode batch`.

This means the benchmark already gives Ursula many independent stream keys. A
thread-per-core multi-Raft architecture should be able to route those streams
across cores and Raft groups.

Relevant current code:

- `riverrun/crates/perf-compare/src/main.rs`: `measure_write_throughput`
  creates one stream and task per concurrency slot.
- `riverrun/crates/perf-compare/src/main.rs`: `measure_mixed_live` combines
  appenders, replay readers, and SSE readers.

## Current Ursula Bottleneck Candidates

These are code-level candidates to verify with profiling and metrics.

1. Single shared Raft handle

   `AppStateInner` stores one `Raft`, one `StateMachineStore`, and one
   `AppendCoalescer`. That shape makes every public write eventually converge
   on one consensus pipeline.

   Current source:

   - `riverrun/crates/ursula/src/api/mod.rs`: `AppStateInner { raft,
     state_machine, append_coalescer, ... }`
   - `riverrun/crates/ursula/src/lib.rs`: one `Raft::new(...)` is created for
     the node.

2. Sharded admission still converges on one Raft group

   The append coalescer has multiple queue shards, but prepared commits still
   call `exec.raft.client_write(StreamCommand::MultiAppend { ... })`. This can
   reduce HTTP/request overhead, but it cannot make independent streams commit
   on independent consensus groups.

   Current source:

   - `riverrun/crates/ursula/src/api/coalescer.rs`: `run_global_batch_worker`
     receives per-shard append queues.
   - `riverrun/crates/ursula/src/api/coalescer.rs`: `commit_prepared_batches`
     submits combined appends through one `exec.raft.client_write(...)`.

3. Stream locks and shared preview state

   The coalescer maintains a process-wide `DashMap<BucketStreamId,
   Arc<tokio::sync::Mutex<()>>>` for stream locks. That is correct for
   per-stream ordering, but the map and preview state are still process-wide
   coordination structures rather than shard-owned state.

   Current source:

   - `riverrun/crates/ursula/src/api/coalescer.rs`: `stream_locks:
     DashMap<BucketStreamId, Arc<tokio::sync::Mutex<()>>>`
   - `riverrun/crates/ursula/src/store/mod.rs`: `append_preview_index` is a
     shared `DashMap` derived from global state-machine data.

4. Storage flush path is not per-core group commit

   The current WAL writes are durable, but the write path is organized around
   one log store for one Raft instance. A multi-Raft design must avoid replacing
   one global Raft bottleneck with one global fsync bottleneck.

   Current source:

   - `riverrun/crates/ursula/src/log_store.rs`: `append` writes entries and
     flushes WAL through RocksDB.

## vNext Plateau Evidence

The current vNext prototype is no longer bottlenecked by stream placement:
benchmark-shaped streams activate all configured shard cores and all configured
groups. The remaining plateau is therefore a different problem.

Observed with release `ursula-http` at 10 cores and 160 groups:

- concurrency 256, 10 seconds, batch size 16: write reached 830,280.37 req/s,
  small-event reached 866,320.82 req/s, and server CPU samples averaged 476.5%
  CPU with a 541.0% peak.
- concurrency 1024, 10 seconds, batch size 16: write reached 900,088.08 req/s,
  small-event reached 838,319.01 req/s, and server CPU samples averaged 478.1%
  CPU with a 572.3% peak.
- after shard-owned append-batch routing, concurrency 1024, 10 seconds, batch
  size 16: write reached 1,091,086.74 req/s, small-event reached 1,125,166.59
  req/s, and server CPU samples averaged 325.8% CPU with a 341.6% peak.
- after the same change, a post-mode concurrency 1024 control run reached
  79,851.19 write req/s and 81,038.93 small-event req/s, with server CPU
  averaging 292.3% and peaking at 310.3%.
- after the same change, a batch-mode concurrency 4096 run was not acceptance
  evidence because the write phase produced 424,976 status-0/client errors.
  The zero-error small-event phase reached 1,094,909.46 req/s, with server CPU
  averaging 318.8% and peaking at 341.3%.
- after direct in-memory batch application and batch-level metrics aggregation,
  a concurrency 1024, 15-second, batch-size-16 small-event run reached
  1,110,037.25 logical appends/s with zero errors, while server CPU averaged
  296.6% and peaked at 327.0%.
- after direct HTTP batch ack rendering, a concurrency 1024, 10-second,
  batch-size-16 small-event run reached 1,125,134.01 logical appends/s with zero
  errors, while server CPU averaged 288.7% and peaked at 323.4%.
- after zero-copy HTTP batch parsing with borrowed in-memory append, a
  concurrency 1024, 10-second, batch-size-16 small-event run reached
  1,112,423.64 logical appends/s with zero errors, while server CPU averaged
  275.5% and peaked at 307.7%.
- after explicit minimal successful batch acks with `Prefer: return=minimal`, a
  concurrency 1024, 10-second, batch-size-16 small-event run reached
  1,097,653.11 logical appends/s with zero errors, while server CPU averaged
  278.1% and peaked at 314.5%.
- with the raw HTTP/1 diagnostic server and the same minimal-ack workload, the
  run reached 1,085,967.92 logical appends/s with zero errors, while server CPU
  averaged 207.3% and peaked at 242.2%.

This means vNext currently improves throughput and distribution, but still does
not meet the final CPU-saturation goal. The next profiling target should be the
HTTP ingress, request batching, mailbox execution, response path, and
same-machine benchmark/client overhead, not stream-to-core placement. It also
means CPU saturation should not be judged from logical append throughput alone:
large public batches can raise logical appends per second while lowering server
CPU per logical append.

Direct `ShardRuntime` stress gives a sharper boundary:

- direct batch mode, bypassing HTTP and `perf_compare`, with 10 cores, 160
  groups, 8192 streams, 2048 producers, batch size 16, and 128-byte payloads:
  7,353,566.27 logical appends/s, 460,401.76 routed runtime requests/s, 10
  active cores, 160 active groups, no mailbox-full events, and whole-process CPU
  averaging 781.8% with an 818.9% peak.
- direct append mode, same placement shape and 4096 producers: 740,136.32
  appends/s, 740,945.92 routed runtime requests/s, 10 active cores, 160 active
  groups, no mailbox-full events, and whole-process CPU averaging 832.4% with
  an 849.2% peak.

Those direct runs are not final-goal evidence because the final goal is under
`perf_compare`, and they do not include HTTP, OpenRaft, WAL, or real network
ingress. They do show that the current shard runtime can use much more CPU than
the HTTP `perf_compare` path. The next plateau question is therefore whether
`perf_compare`/reqwest/HTTP ingress can produce enough server-side work, and
how that changes after real OpenRaft and durable group commit are added.

Same-machine multi-process `perf_compare` narrows that question further:

- 4 client processes at 1024 concurrency each were invalid: two processes
  produced large status-0 error counts, one timed out creating streams, and one
  hit `Can't assign requested address`.
- 4 client processes at 256 concurrency each completed with zero errors and
  reached about 2.07M logical appends/s aggregate, but server CPU averaged only
  431.6% and peaked at 454.8%.
- 8 client processes at 256 concurrency each also completed with zero errors,
  but aggregate throughput fell slightly to about 1.96M logical appends/s and
  server CPU averaged 421.8% with a 460.5% peak.

That puts the current same-machine HTTP benchmark ceiling near 120k-130k HTTP
batch requests/s. Above that, adding client processes either does not increase
server pressure or creates client/OS connection failures. This is not an Ursula
success condition, but it explains why the current `perf_compare` setup cannot
prove CPU saturation for the in-memory vNext runtime.

Increasing public batch size does not change that conclusion. A single
`perf_compare` process with batch size 64 reached 2,983,891.9 logical
appends/s with zero errors, but only about 46k HTTP batch requests/s. Server CPU
averaged 353.6% and peaked at 373.5%. Larger batches can inflate logical
append throughput while lowering request rate and keeping server CPU below the
runtime ceiling.

Minimal successful batch acks also do not change that conclusion. They remove
the most obvious response-body and client JSON parsing work from the successful
batch path, but the server still stays around the same three-core plateau under
the single-process `perf_compare` small-event workload.

The raw HTTP/1 diagnostic narrows this further: axum/hyper routing and response
rendering are not the reason `perf_compare` fails to saturate server CPU. A
thinner server uses less CPU at roughly the same logical append rate.

Pipelining append requests inside `perf_compare` does not provide a usable
escape hatch either. With `--ursula-append-pipeline-depth 2`, the run produced
71,760 status-0/client errors and only 49.7% average server CPU; depth 4
produced 94,976 status-0/client errors and only 7.4% average server CPU. The
benchmark hits client/OS failure modes before extra in-flight reqwest requests
can make the server busy.

EC2 testing on Graviton narrows the boundary further. The setup used one
`c7gn.8xlarge` client/build host and three `c7g.4xlarge` server hosts, with
release `ursula-http --core-count 16 --raft-group-count 256 --raft-memory` on
the servers and release `perf_compare` on the client.

First, direct OpenRaft runtime stress on one `c7g.4xlarge`, bypassing HTTP and
`perf_compare`, proved the machine can saturate the vNext runtime. With
`ursula-raft-runtime-stress --core-count 16 --raft-group-count 256
--stream-count 8192 --producer-count 8192 --setup-concurrency 2048
--batch-size 16 --payload-bytes 100 --duration-secs 12`, the process completed
63,192,384 logical appends at 3,462,259.92 appends/s and 216,840.08 routed
runtime requests/s. `pidstat` samples reached 1,598% CPU, i.e. essentially all
16 vCPUs. The run used all 16 cores and 256 groups, had
`mailbox_full_events: 0`, and ended with empty mailboxes.

The HTTP path on the same EC2 machines still stayed far below that runtime
ceiling:

- one `perf_compare` process, batch size 16, minimal ack, pipeline depth 1,
  concurrency 1024: 8,449,872 logical appends in 15.011s, 529,142 routed
  runtime requests, server CPU active samples around 2.9 cores, client CPU
  around 2.3 cores, `group_mailbox_max_depth: 5`, and no mailbox-full events.
- raising concurrency to 4096 did not help: 7,844,208 logical appends in
  15.044s, 494,360 routed runtime requests, server CPU around 2.7-2.8 cores,
  and `group_mailbox_max_depth: 10`.
- pipeline depth 4 improved logical throughput modestly to 9,236,752 appends
  in 15.087s, with 578,322 routed runtime requests and server CPU peaking at
  3.13 cores, but still did not create downstream backlog.
- batch size 64 raised logical throughput to 12,832,320 appends in 15.275s
  while lowering routed runtime requests to 201,530 and server CPU to roughly
  1.8 cores. This confirms larger public batches mostly hide the HTTP request
  ceiling; they do not prove CPU saturation.
- three parallel `perf_compare` processes from the `c7gn.8xlarge`, one per
  `c7g.4xlarge` server, reached about 1.61M aggregate logical appends/s. Each
  server still stayed around 2.55 active cores, while the client consumed about
  8 cores in aggregate.

The EC2 result rules out the MacBook as the primary explanation. The same
structural boundary remains on real client/server hosts: direct OpenRaft
runtime can consume a full `c7g.4xlarge`, but the current HTTP `perf_compare`
ingress shape feeds only about 33k-38k routed batch requests/s per target.
There is no evidence of S3, local disk, WAL, or `core -> group` mailbox backlog
on this diskless OpenRaft path.

Follow-up EC2 experiments refined this from "HTTP layer" to a narrower root
cause: the single-process `perf_compare` reqwest load generator is the first
hard limiter for the current benchmark shape.

Three `perf_compare` processes against the same `c7g.4xlarge` server pushed
the server far past the single-process plateau. Each process used the same
batch-size-16, minimal-ack, pipeline-depth-4 workload and completed with zero
errors. The single server recorded 25,484,960 accepted appends and 1,595,885
routed runtime requests, while active server CPU averaged 746.7% and peaked at
967.0%. Mailboxes still did not fill: `mailbox_full_events: 0`,
`group_mailbox_depth: 0`, and `group_mailbox_max_depth: 49`. This proves the
server is not intrinsically capped at the 2.5-3.1 cores seen with one
`perf_compare` process.

Four and five client processes can push the same server higher, but the signal
turns into overload. Four processes reached 943.1% average active CPU and
1,346.0% peak, while producing 1,008 total client-side status-0 errors and
2,876 runtime `mailbox_full_events`. Five processes reached 1,062.8% average
and 1,513.0% peak, close to the 16-vCPU host at peak, but produced 1,056
status-0 errors and 7,638 `mailbox_full_events`. The CPU headroom exists, but
getting there with the current benchmark means overfeeding the HTTP/runtime
queues instead of measuring a clean steady-state workload.

The raw HTTP diagnostic also rules out axum/hyper routing as the primary
single-process ceiling. A single `perf_compare` process against
`ursula-http-raw` reached 9,089,008 logical appends in 15.089s, or 602,372.71
logical appends/s, nearly the same request-rate class as the axum/OpenRaft HTTP
path. The raw server used only about 1.39 active cores. Removing axum/hyper from
the server therefore lowers server CPU cost but does not let one reqwest-based
client process feed materially more requests.

`perf record` on the single-process client confirmed where that client-side
work goes. The hottest flat samples were allocator and memory-copy work
(`_int_free`, `_int_malloc`, `malloc`, `cfree`, `malloc_consolidate`,
`__GI___memcpy_sve`), Tokio/hyper scheduling atomics and semaphores
(`tokio::sync::batch_semaphore`), URL parsing (`url::parser::*`), reqwest
request execution, Hyper HTTP/1 parse/flush, and `HeaderMap` operations. This
matches the benchmark code path: every append-batch request builds a new URL
with `format!`, clones the prebuilt batch body with `body.to_vec()`, constructs
a new reqwest request and headers, and still allocates a `Vec<u16>` for the
minimal-ack status expansion.

The precise current blocker is therefore not Ursula's in-memory OpenRaft group
runtime. It is the benchmark ingress shape: one `perf_compare` process produces
too few HTTP batch requests per second per target because it pays per-request
reqwest/HTTP construction, allocation, URL/header work, and response-driven
future scheduling. Multiple benchmark processes can feed the same server more
work, and direct runtime stress can saturate the host, so the remaining work for
the final goal is to replace or bypass this per-request HTTP/reqwest bottleneck
with a streaming/multiplexed ingest path or a lower-overhead benchmark client.

## First Profiling Questions

Before moving large amounts of code, capture these under `perf_compare`:

- Which thread owns the hot samples when CPU plateaus?
- How much time is in `client_write`, Raft replication, apply, WAL flush, and
  append preview/prepare?
- Are coalescer shards balanced by stream hash?
- Does `append-batch` increase throughput without increasing busy cores?
- Is the leader CPU-bound, fsync-bound, or waiting on locks/channels?

## Migration Consequence

The first production migration step should not be monoio. It should be
ownership extraction:

```text
HTTP handler
  -> stream router
  -> shard mailbox
  -> shard-owned append pipeline
  -> shard-owned Raft group
```

Once `perf_compare` shows independent streams distributed across shard-owned
workers, runtime experiments become meaningful. Until then, changing from Tokio
to monoio would mostly preserve the same global bottlenecks.

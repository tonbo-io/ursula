# Thread-Per-Core Multi-Raft Architecture

## Objective

Migrate Ursula from a single process-wide Raft runtime toward a sharded durable stream runtime where each core owns a set of stream partitions and Raft groups.

The goal is to make stream ownership explicit before moving protocol handlers, storage, and background workers. This avoids a direct port that preserves global locks and Tokio-oriented sharing patterns.

## Workload Fit

Durable streams are a strong fit for sharding because the protocol requires strict ordering only within a stream. Independent streams can be placed in independent Raft groups without cross-stream transactions.

This architecture optimizes for:

- Many independent streams.
- Small append-heavy writes.
- Low-latency tailing via long-poll and SSE.
- Per-stream replay from retained offsets.
- Snapshot and bootstrap semantics at stream or shard boundaries.
- CPU saturation under `perf_compare` write and mixed workloads.

It does not remove the serial bottleneck for a single hot stream. One stream still maps to one Raft group so append order remains total and deterministic.

## Ownership Model

Each runtime thread owns:

- A local event loop.
- A set of shard actors.
- The Raft groups assigned to those shards.
- Hot stream metadata and live-tail watcher state for owned streams.
- Per-core write admission and backpressure counters.

No shard-owned state is accessed by other threads directly. Other threads interact by sending commands to the owning actor.

```text
client request
    -> ingress runtime
    -> stream router(bucket_id, stream_id)
    -> owning core mailbox
    -> shard actor
    -> Raft group propose/apply
    -> response channel
```

## Raft Group Placement

A Raft group owns a hash bucket of streams, not a single stream. The current
static placement is implemented by `StaticShardMap`:

```text
raft_group = fnv1a64(bucket_id + "/" + stream_id) % raft_group_count
core       = raft_group % core_count
```

This is not rendezvous hashing or consistent hashing. It is intentionally simple
for bootstrapping fixed-size test clusters, and it gives stable placement only
while `raft_group_count` and `core_count` are unchanged. Changing
`raft_group_count` remaps a large fraction of streams, so this layer should be
treated as a static bootstrap map, not as the split/merge or online-resize
mechanism.

Dynamic split/merge should introduce an explicit routing layer above
`StaticShardMap`, for example a persisted virtual-bucket table or routing table
that maps stream-hash ranges to Raft groups. That layer needs to be replicated
as cluster metadata and consulted before stream commands enter the owner-core
mailbox. It should support:

- Hot group movement between cores.
- Leader placement balancing between nodes.
- Group split/merge for long-running hot partitions.
- Stable placement metadata persisted through cluster membership changes.

## Runtime Boundary

The architecture should be runtime-neutral at the durable stream core boundary.

The recommended layering is:

- `runtime-core`: shard actors, routing, command envelopes, Raft group ownership.
- `stream-core`: pure Durable Streams state-machine semantics.
- `protocol-http`: HTTP parsing/response adapters.
- `raft-transport`: node-to-node Raft RPC transport.
- `storage`: WAL, hot payload store, cold tier, snapshots.

Tokio can remain the first production runtime while the core ownership model is introduced. Monoio should be evaluated behind a separate runtime adapter only after the HTTP and Raft transport dependencies are proven viable.

## Current Runtime Prototype

`crates/ursula-runtime` is the first concrete runtime-core artifact. It can run
core workers on per-core OS threads or inside a hosted Tokio runtime. It is not
the final production runtime.

It establishes these contracts:

- public callers submit append-shaped commands through `ShardRuntime`;
- `BucketStreamId` is mapped through `StaticShardMap` to one `CoreId` and one
  `RaftGroupId`;
- each core actor owns group placement and dispatches commands for local groups;
- each touched Raft group is represented by a core-local `GroupEngine` protected
  by a per-group async mutex, so one group can wait on I/O without forcing the
  core dispatcher to stop receiving commands for other groups;
- stream offsets advance only inside the owning actor;
- runtime metrics expose accepted appends, successful state mutations, routed
  requests, mutation apply time, mailbox send wait, and mailbox-full events per
  core/group without a hot-path process-wide accepted counter.

The default `InMemoryGroupEngine` is intentionally not durable. It wraps the
pure `ursula-stream` state machine and exists to test routing, ownership,
engine construction, stream command application, error propagation, and
observability before Raft transport and storage are moved.

`WalGroupEngineFactory` is an optional durability prototype behind the same
group-engine boundary. It keeps the default in-memory path unchanged, but can
create one WAL-backed engine per touched Raft group. Each group writes command
records under a core/group-specific path and replays them when the group is
constructed again. HTTP append-batch reaches a group-level append-batch engine
method, so the WAL prototype can write several records and call `sync_data`
once for that group batch. This proves the recovery boundary can stay
group-local and that public batches do not have to devolve into per-frame
fsyncs. It is not a substitute for the production OpenRaft log, snapshots, or a
production group-commit policy.

The optional diagnostic WAL engine remains part of the HTTP prototype for recovery
smokes. User-facing disk storage now uses the production OpenRaft log path through
the typed Ursula config file.

`RuntimeConfig::new` defaults to `RuntimeThreading::ThreadPerCore`: each core
worker runs on its own OS thread with a current-thread Tokio event loop. Tests
also cover `RuntimeThreading::HostedTokio` so the runtime can still be embedded
inside an existing Tokio process while the server migration is incomplete.

This prototype does not yet prove CPU saturation. It only proves that the
request path can be shaped so independent streams do not need to share a single
global mutable state machine.

Metrics on the mutation, append, and routing paths use padded per-core and
per-group atomics. Snapshot reads derive total accepted appends, successful
state mutations, mutation apply time, routed requests, and mailbox send wait
from per-core counters. Mailbox-full events are also per-core, so measurement
does not add a global atomic write to every append.

The WAL prototype uses the same metrics shape. `GroupEngineMetrics` lets a
group engine record WAL batch count, record count, write time, and sync time on
the owning core/group. In-memory engines leave these counters at zero. This is
diagnostic only; production OpenRaft storage still needs real group-commit
histograms and log replication metrics.

## OpenRaft Integration

OpenRaft supports custom async runtimes, including monoio through `openraft-rt-monoio`. The important design consequence is that monoio mode requires OpenRaft's `single-threaded` feature. That means a `Raft` handle is not `Send` or `Sync`, which is compatible with shard-local actor ownership but incompatible with the current clone-and-share `AppState` shape.

The migration should therefore avoid sharing `Raft` handles across request handlers. Instead, request handlers should route commands to shard mailboxes and receive responses through one-shot channels.

The runtime crate's `GroupEngine` trait is the intended OpenRaft adapter
boundary. A production adapter should own exactly one OpenRaft group instance
inside the owning core worker and should translate append/read/admin commands
into Raft proposals without exposing the Raft handle through shared application
state.

Runtime writes are represented by `GroupWriteCommand`, a serializable
group-level command envelope for create, append, append-batch, close, and
delete. `InMemoryGroupEngine::apply_committed_write` is the current committed
apply boundary: the prototype engine applies the same command shape that a
future OpenRaft state machine should receive after log commit. The WAL
prototype records this envelope per group, but it is still a local diagnostic
durability layer, not an OpenRaft log store.

`crates/ursula-raft` contains the first OpenRaft-specific adapter boundary.
Durable app-log schema is now defined in `crates/ursula-proto` instead of in
the Raft transport protobuf. `ProducerRequestV1`, `ExternalPayloadRefV1`, and
`ColdChunkRefV1` are reused directly by `ursula-stream` and `ursula-runtime`.
`BucketStreamId` remains a local semantic key in `ursula-shard` because it is
used for hashing, display, validation, and map keys, but `ursula-shard` owns the
conversion to and from shared `BucketStreamIdV1` so Raft does not duplicate that
schema mapping. `RaftGroupCommandV1` and `RaftGroupResponseV1` are the shared
protobuf application schema for Raft log commands and responses. `ursula-raft`
keeps thin local `RaftGroupCommand` / `RaftGroupResponse` wrapper types only
because OpenRaft needs local Rust trait implementations such as `Display`.
Focused tests cover prost roundtrips for those shared command/response schemas
back into runtime domain types. In the generated proto crate, serde derives are
kept only for the three shared runtime JSON/snapshot value types that still need
them:
`ProducerRequestV1`, `ExternalPayloadRefV1`, and `ColdChunkRefV1`.
`UrsulaRaftTypeConfig` uses those wrappers as OpenRaft application data,
`RaftGroupLogStore` provides an in-memory OpenRaft log-store implementation for
the same type config, and `RaftGroupFileLogStore` persists the same vote,
committed pointer, purge pointer, and log entries as length-prefixed protobuf
records. Normal application log entries in those records embed the shared
`RaftGroupCommandV1` prost message directly, not a second serde encoding or an
opaque command byte blob.
When used by `DurableRaftGroupEngineFactory`, groups on the same owner core now
share a `CoreFileLogWriter` and a core-local `journal.bin`: the writer batches
length-prefixed binary records from owned groups and syncs the core journal
once per batch. That validates recovery semantics from a per-core durable
journal and exposes write/sync timing through the durable-log metrics before
adding indexing, compaction, production backpressure, and a real group-commit
policy.

`RaftGroupStateMachine` applies committed OpenRaft entries through
`InMemoryGroupEngine::apply_committed_write`. The adapter can build and install
OpenRaft snapshots backed by `GroupSnapshot` bytes, and the unit tests now prove
a single-node OpenRaft group can initialize, elect itself, apply create/append
writes through `client_write`, and recover those writes after reopening the
file log. `RaftGroupEngine` also has a constructor path that accepts an injected
`RaftNetworkFactory`; the focused three-node in-process test uses that path to
exercise real OpenRaft Vote and AppendEntries replication and then reads the
replicated Durable Streams payload from all three state machines.
`ShardRuntime::warm_group` can instantiate a group engine on the owning core
without using a stream mutation as the trigger, and
`RegisteredRaftGroupEngineFactory` can put the resulting runtime-owned `Raft`
handle into a `RaftGroupHandleRegistry` keyed by `RaftGroupId`. That registry is
the local dispatch boundary a cross-process Raft RPC layer needs before it can
serve AppendEntries, Vote, or full-snapshot messages for groups that have not
yet received client traffic on the follower node.
`router_with_raft_registry` wires that registry into an internal tonic gRPC
service mounted on the same Axum listener as the public HTTP API. Vote,
AppendEntries, and full-snapshot transfer use typed `RaftInternal` protobuf
messages for OpenRaft votes, log ids, log entries, membership, snapshot
metadata, and responses. The durable application command schema itself is not a
private Raft transport schema; it is the shared `ursula-proto` schema embedded
inside those Raft protobuf records.
`GrpcRaftNetworkFactory` and `GrpcRaftNetwork` implement the outbound OpenRaft
network trait for those RPCs. `StaticGrpcRaftGroupEngineFactory` wires that
network into runtime-owned group construction with a static peer map and carries
the configured cold store into each group's state machine. The cross-router test
starts three Ursula HTTP routers, warms four groups on each node, initializes
node 1 with three voters for every group, writes streams placed on all four
groups through node 1, and waits until the replicated payloads are readable
from every runtime.
`ursula` now exposes this static cluster shape through the typed config file:
`raft.node_id`, `[[raft.peers]]`, `raft.wal.backend`, and
`raft.init_membership` / `raft.init_membership_per_group`. It warms every group
at startup so followers can receive Raft RPCs before client traffic touches those
groups, and serves the registered Raft gRPC routes in the production binary. OpenRaft
`ForwardToLeader` is preserved as a structured `GroupLeaderHint`, and public
write requests that land on followers now return a `307` redirect to the known
leader instead of having the follower proxy the body over node-to-node HTTP.
Full snapshots use the same gRPC transport. A short EC2 smoke has also run the
static cluster shape across three `c7g.4xlarge` nodes with a `c7gn.8xlarge`
client and S3 cold storage enabled using the current gRPC internal Raft
transport. The smoke verified follower leader redirects, a leader write that
committed through gRPC quorum replication, background S3 flush, and S3-backed
post-flush readback. The official Durable Streams conformance suite also passed
against this EC2 static gRPC shape from the `c7gn.8xlarge` client. The next
missing pieces are dynamic/reconfigurable membership and long-running
`perf_compare` with S3 cold path enabled. A later EC2 smoke also covered the
same static gRPC shape with independent durable OpenRaft log roots plus real S3
cold storage, then restarted all nodes without reinitializing membership and
read the cold-backed stream through a restarted follower. Another EC2 smoke
covered a restarted late learner: node 1 snapshotted and purged a two-voter
durable-log/S3 cluster, node 3 started from an empty durable log root, node 1
added it as a learner, node 3 installed snapshot index 4 through gRPC
full-snapshot transfer, and node 3 served the restored cold-backed stream.
The local automatic snapshot path is now covered by
`openraft_installs_snapshot_for_lagging_learner`, which forces a purged leader
to catch up a newly added learner through `RaftNetworkV2::full_snapshot`. That
test also fixed the state-machine snapshot retention bug that made leader-side
snapshot transfer fail after a successful snapshot build.
`static_grpc_raft_installs_snapshot_for_late_learner_over_tcp` extends that
coverage to local TCP routers by writing through the leader HTTP API and reading
the restored stream through the late learner's HTTP endpoint after gRPC snapshot
installation.

`RaftGroupEngine` wraps the group behind the runtime `GroupEngine` trait,
routing writes through OpenRaft and routing read/head/snapshot calls through
OpenRaft's state-machine access API. `GroupEngineFactory` is now async so group
creation can await OpenRaft initialization instead of blocking a core worker,
and `RaftGroupEngineFactory` proves `ShardRuntime` can instantiate a
runtime-owned local Raft group. This is not yet the production runtime topology:
production work still needs an indexed/high-throughput OpenRaft log format,
dynamic deployment membership management, and long-running EC2
conformance/performance validation of the multi-group static cluster path.

In the typed Ursula config, `raft.wal.backend = "memory"` selects the OpenRaft
group-engine factory with the in-memory `RaftGroupLogStore`. That path lets the
HTTP subset drive OpenRaft `client_write` without local WAL persistence, which is
the right benchmark mode when isolating OpenRaft/runtime overhead from disk
durability. `raft.wal.backend = "disk"` with `raft.wal.path = "..."` selects the
durable OpenRaft group-engine factory and adds group-local file-log persistence
through the same shard-owned runtime boundary. The static gRPC cluster path uses
the same durable log backend by combining disk WAL config with `raft.node_id`,
`[[raft.peers]]`, and membership settings; the HTTP layer chooses the backend
through `DurableRaftLogStoreFactory` instead of owning the core journal writer. The
local `static_grpc_raft_runtime_recovers_from_core_journal_after_restart` test
also covers the single-node production shape restarting from the same
`journal.bin` and reading back an HTTP-committed payload without reinitializing
membership. The local
`static_grpc_raft_group_engine_replicates_with_core_journals` test covers the
three-router static gRPC shape with independent durable log roots, replicated
writes across two groups, non-zero durable-log metrics on every node, and a
`journal.bin` per node. `static_grpc_raft_durable_cold_flush_replicates_manifest`
adds the cold-path version of that shape: three static gRPC nodes with
independent durable journals share a cold store, the leader writes and batch
flushes cold metadata, and every node's Raft state machine reads the complete
payload through the replicated cold manifest while retaining a non-empty core
journal. The gated
`cli_static_grpc_raft_log_dir_recovers_replicated_s3_cold_manifest_after_restart`
test covers the same shape through real `ursula` processes with independent
durable log roots and shared S3 cold storage: it replicates the cold manifest,
stops all nodes, restarts without initial membership, reads the cold-backed
stream through a restarted follower, and cleans up the unique S3 root after
readback. The binary also exposes narrow Raft admin endpoints under
`/__ursula/raft/{group}` for validation workflows: trigger snapshot, trigger
purge, and add a learner. `cli_static_grpc_raft_log_dir_installs_snapshot_for_late_learner`
uses only real `ursula` processes plus those endpoints to prove a late
third node installs the leader snapshot after the leader has snapshotted and
purged its log. The
same late-learner snapshot TCP flow also runs in a durable variant,
`static_grpc_raft_installs_snapshot_for_late_learner_with_core_journals`, which
forces leader snapshot and purge, adds a previously absent learner over gRPC,
waits for snapshot installation, reads the restored stream through the learner
HTTP endpoint, and verifies non-empty core journals on leader, follower, and
learner. The binary-level `cli_static_grpc_raft_log_dir_recovers_after_restart`
test covers the same production `ursula` binary used in EC2 deployment: typed
static cluster config with `raft.wal.backend = "disk"` and `raft.wal.path`,
HTTP write, process restart without reinitializing membership, and HTTP readback
from the recovered journal.
`cli_static_grpc_raft_log_dir_replicates_between_nodes` extends that binary
coverage to three real `ursula` processes with independent log dirs,
leader write, follower readback, and non-empty `journal.bin` files for every
node. This is correctness and durability scaffolding, not a
throughput-optimized log layout. The same static durable-log/S3 shape now has a
short multi-node EC2 restart smoke, but still needs longer soak and performance
validation before it can be treated as a production storage layout.

The same boundary now includes group snapshots. `ShardRuntime::snapshot_group`
routes by `RaftGroupId` to the owning core, and `GroupEngine::snapshot` returns
that group's placement, commit index, deterministic stream snapshot, and
per-stream append counts. `ShardRuntime::install_group_snapshot` validates the
recorded placement before routing and installs the snapshot through the owning
core. This is still a prototype control-plane API, but it establishes the shape
needed for an OpenRaft state-machine adapter without adding a global snapshot
lock or shared state-machine handle.

## Stream Semantics

`crates/ursula-stream` holds the first pure state-machine slice. It models
bucket creation/deletion, stream creation, append, catch-up read, close,
delete, metadata lookup, content-type checks, stream sequence ordering, and the
monotonic closed state without HTTP, storage, or runtime side effects. It also
models the first idempotent producer path for single append/close operations:
per-stream producer epoch/sequence state, duplicate retry deduplication,
sequence-gap rejection, stale-epoch fencing, and duplicate final append success
after stream close.

The stream state machine also exposes a deterministic, serde-compatible
snapshot format. Snapshot buckets and streams are sorted before export, and
restore validates duplicate buckets, duplicate streams, missing buckets, and
payload length versus tail offset. Producer state is included in the stream
snapshot, so producer retry safety survives snapshot restore. Runtime group
snapshots add the group-local commit index and stream append counts that are
outside the pure stream state-machine payload but still needed for restored
response continuity. This is the semantic snapshot core that an OpenRaft
state-machine adapter should encode into Raft snapshots; OpenRaft snapshot
transport, bootstrap orchestration, retention, and storage integration still
need separate production work.

This crate should become the semantic core used behind OpenRaft apply. Protocol
handlers may parse headers and bodies, but they should not define hidden stream
state transitions outside this command model.

The runtime prototype already uses this crate in its default group engine:
`ShardRuntime::create_stream`, `ShardRuntime::append`,
`ShardRuntime::read_stream`, `ShardRuntime::close_stream`, and
`ShardRuntime::head_stream` route to the owning core, then the group-local
engine applies stream commands or reads metadata/payload from its local
state-machine instance. This keeps benchmark-shaped runtime tests and protocol
semantic tests on the same transition code.

## HTTP Prototype

`crates/ursula` is the first protocol adapter over the shard-owned
runtime. It uses axum on Tokio and intentionally implements only the subset
needed to put the runtime under a real HTTP workload:

- `PUT /{bucket}` for benchmark bucket setup.
- `PUT /{bucket}/{stream}` for stream creation.
- `POST /{bucket}/{stream}` for append and close-only requests.
- `POST /{bucket}/{stream}/append-batch` for Ursula's length-prefixed batch
  append extension.
- `GET /{bucket}/{stream}?offset=N&max_bytes=M` for catch-up reads.
- `GET /{bucket}/{stream}?offset=N&live=long-poll` for live long-poll reads.
- `GET /{bucket}/{stream}?offset=N&live=sse` for SSE live-tail reads.
- `HEAD /{bucket}/{stream}` for metadata.
- `DELETE /{bucket}/{stream}` for stream deletion.
- `GET /__ursula/metrics` for append distribution across cores and Raft groups.

The HTTP adapter does not own stream state. It parses protocol inputs, routes
to `ShardRuntime`, and renders protocol-shaped status codes and headers. Batch
frames are decoded at the HTTP edge, but each append still goes through the
runtime and owning group. This keeps HTTP out of the durable stream ownership
boundary and avoids a global request-state registry on the measured write path.
When a batch client sends `Prefer: return=minimal`, an all-success append-batch
can return `204 No Content`; partial failures still return the JSON per-item
status array.
Current local validation separates protocol correctness from local disk
latency. A three-process static gRPC cluster with 32 Raft groups and
`raft.wal.backend = "memory"` passes the official Durable Streams suite
(`300 / 300`) both with the background cold worker disabled and with memory cold
storage actively flushing 1024-byte chunks. The same local three-process shape
with durable OpenRaft file logs is protocol-correct but can exceed the official
property tests' default 5s wrapper on a MacBook: metrics point to file-log
fsync/replication latency and, for 1-byte cold flush, cold-manifest
write-amplification. This is why EC2 and in-memory Raft-log conformance remain
the current gates for multi-node protocol behavior, while local durable-log
runs are treated as latency diagnostics.
The `ursula` binary can run with `raft.wal.backend = "memory"` for the
OpenRaft in-memory log engine or `raft.wal.backend = "disk"` plus
`raft.wal.path` for the OpenRaft-backed file-log engine. These modes keep
protocol handling in the same HTTP adapter while changing only the runtime's
group-engine factory.
Append, close-only, and append-batch requests parse `Producer-Id`,
`Producer-Epoch`, and `Producer-Seq` together and route them into the state
machine. Successful producer requests echo epoch/sequence headers, producer
sequence conflicts return expected/received sequence headers, stale epochs
return the current epoch header, and append-batch duplicate retries return the
stored per-item offsets without appending retry bytes.
The metrics route snapshots runtime counters and mailbox depths. It derives
totals from per-core counters rather than recording a process-wide append
counter on every request.

Live long-poll and SSE reads are driven by shard-owned one-shot read watchers.
When a reader is at the tail of an open stream, the owning core stores the
waiter next to the group state and completes it from the same actor after append
or close. The HTTP layer only renders long-poll responses or SSE events from
those owner-core reads. Dropped waiters send cancellation back to the owning
mailbox so live-tail state does not depend on a process-wide watch registry.

## Storage Policy

Do not let every Raft group independently fsync small writes without coordination. The storage layer should evaluate:

- Per-core WAL files with group-id tagged records.
- Group commit across Raft groups owned by the same core.
- Dedicated blocking/fsync workers for RocksDB or file-backed stores.
- Direct I/O or io_uring storage only after correctness and recovery semantics are settled.

The current WAL prototype intentionally uses one file per group to make the
ownership and recovery boundary testable. Production storage may collapse this
into per-core group commit, but it must not reintroduce a single process-wide
fsync queue for all groups.

## Migration Phases

1. Establish routing primitives and static shard placement.
2. Introduce shard-owned command actors inside the existing Tokio server.
3. Move append preview/prepare/commit state behind shard mailboxes.
4. Split the single Raft group into multiple groups with static placement.
5. Move live-tail watcher state and stream metadata into owning shards.
6. Add placement metadata, rebalance hooks, and leader balancing.
7. Evaluate monoio for shard actors and Raft runtime after HTTP/transport feasibility is proven.

## CPU Saturation Gate

The migration target is not only lower latency. Ursula must be able to drive all
available cores under the `perf_compare` Ursula write path. The existing plateau
at roughly three to four busy cores is treated as a release blocker for this
architecture.

The design must therefore remove process-wide write bottlenecks before
optimizing runtime choice:

- no single global append coalescer for all streams;
- no single Raft group for all stream commands;
- no shared stream metadata lock on the measured append path;
- no single WAL/fsync queue that serializes all Raft groups without batching;
- no single live-tail watch registry lock for mixed workloads.

The first acceptance target is linear CPU utilization growth as
`perf_compare --concurrency` increases across independent streams, until either
the host cores, disk, or network are saturated.

## Raft Progress Observability

`GET /__ursula/metrics` exposes one OpenRaft progress record per registered
Raft group. Each record includes the node id, current term/leader, log frontier,
committed/applied/snapshot/purged indexes and terms, voters, and learners. This
is intentionally read-only and sourced from OpenRaft's watched metrics through
the `RaftGroupHandleRegistry`.

This matters for the migration because lagging-follower and restart-level tests
must distinguish "the stream is readable" from "snapshot transfer and log purge
actually happened." The local TCP late-learner snapshot test now checks those
metrics after leader snapshot/purge and after the learner installs the snapshot;
the same fields should be used as the EC2 evidence when exercising a stopped or
late-starting follower.

## Non-Goals For The First Milestone

- No per-stream Raft group.
- No cross-stream transactions.
- No immediate monoio rewrite of axum/tonic code.
- No migration of all current operational workers before ownership boundaries are stable.

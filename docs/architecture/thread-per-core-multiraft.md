# Thread-Per-Core Multi-Raft Architecture

This document describes how one Ursula node divides streams among cores and Raft groups, how a request reaches the group that owns its stream, and how groups replicate and persist their logs. The public [architecture overview](../web/src/content/docs/pages/architecture/overview.mdx) covers the cluster topology, the write and read paths and the storage layout. This document covers the internals that page leaves out.

## Workload fit and non-goals

The Durable Streams protocol requires strict ordering only within a stream. Independent streams can therefore live in independent Raft groups with no cross-stream transaction. The design optimizes for:

- many independent streams
- small, append-heavy writes
- low-latency tailing over long-poll and SSE
- per-stream replay from retained offsets

One stream maps to exactly one Raft group, so its append order is total and deterministic. A single hot stream is a serial bottleneck by design.

Non-goals:

- A Raft group per stream. A group owns a hash bucket of many streams.
- Cross-stream or cross-group transactions.

## Ownership and request flow

Each owner core runs on one OS thread with a current-thread Tokio runtime. It owns the Raft groups placed on it, and through their group actors the hot stream state, producer state and live-tail watchers of those groups. No other thread reads or writes owned state. Other threads send a command to the owner and receive the result on a oneshot channel.

```text
HTTP request (axum)
  -> ShardRuntime: StaticShardMap::locate(bucket_id, stream_id)
  -> core mailbox (CoreWorker on the owner core)
  -> group mailbox (GroupActor)
  -> GroupEngine (in-memory or OpenRaft)
  -> oneshot response
```

The HTTP layer parses protocol input, routes to `ShardRuntime` and renders the response. It holds no stream state. It serves three routers: the client plane (the Durable Streams protocol), the cluster plane (the internal Raft gRPC service, served on `server.cluster_listen` when set and otherwise on the main listener) and the admin plane. axum and tonic both run on Tokio, so the owner cores use Tokio current-thread runtimes as well.

## Placement

`StaticShardMap` (`crates/ursula-shard/src/lib.rs`) places every stream:

```text
raft_group = fnv1a64(bucket_id + "/" + stream_id) % raft.group_count
core       = raft_group % runtime.core_count
```

This is a plain modulo map, not consistent hashing. `raft.group_count` (default `16 * core_count`) is fixed for the life of a cluster and every node must use the same value, so a group ID names the same streams on every node. Changing it remaps most streams, which is why groups never split, merge or resize online. The core is a local choice that depends only on the node's own `runtime.core_count`.

## Owner-core runtime

`ShardRuntime` holds one bounded mailbox per core (`mailbox_capacity`, 1024 commands). The `CoreWorker` on each core keeps a map of its groups. The first command for a group, or an explicit warm request, calls `GroupEngineFactory::create` on the owner core, installs the group's ReadIndex barrier and spawns a `GroupActor` with its own bounded mailbox. Each actor is a separate task on the core's runtime, so a group waiting on Raft does not stop the other groups on that core.

The factory is chosen once at startup:

- `InMemoryGroupEngineFactory`: non-replicated groups over the pure `ursula-stream` state machine. Used by the zero-config development node and by tests.
- `DurableRaftGroupEngineFactory`: single-node OpenRaft groups over the per-core journal.
- `StaticGrpcRaftGroupEngineFactory`: OpenRaft groups over the per-core journal with a static peer map, each group's seed voters and initializer (`ConfigSeeds`) and the gRPC network.

Threads:

- `RuntimeThreading::ThreadPerCore` (the default) runs each core worker on a thread named `ursula-core-N`. `RuntimeThreading::HostedTokio` runs workers as tasks of an existing runtime and is the mode used under madsim.
- `runtime.cpu_affinity` controls Linux pinning. Unset, owner threads pin only when the inherited allowed cpuset size equals `runtime.core_count`, because a CFS quota alone is not an exclusive cpuset. `false` disables pinning and `true` forces it, round-robin over the allowed CPUs when oversubscribed. Pinning failures are logged. Other platforms run one thread per owner without pinning.
- Each active core journal has its own writer thread with a current-thread runtime. Disk reads of older log entries run on Tokio's blocking pool. The main Tokio runtime and its blocking pool are additional threads, so the process thread count is larger than `runtime.core_count`.

An actor handles one command at a time. When its engine supports batched appends (the Raft engine does), an append drains up to 32 already-queued appends and 1 MiB of payload. It stops at the first non-append command, which runs next, and adds no batching delay. A request larger than 1 MiB is submitted alone. The Raft engine checks cold-write admission for the batch with cumulative reservations, so each item is admitted against the bytes accepted before it, and then proposes the commands together. Responses keep mailbox order. Live readers are woken only after the writers have their replies.

## Raft integration

The OpenRaft application data type is `RaftCommand` (`crates/ursula-raft/src/types.rs`), defined as `enum RaftCommand { Stream(StreamCommand), Meta(MetaCommand) }`. A data group's entries carry `Stream`, whose bytes are those of format epoch 5. `Meta` is the format of the meta group's commands (`crates/ursula-raft/src/meta/mod.rs`). Every node that runs Raft runs a meta replica on its own journal, started before the node warms its data groups (`crates/ursula/src/bootstrap/meta.rs`). Commands, responses, forwarded reads and OpenRaft's own RPC and log types are encoded as compact MessagePack straight from their serde derives (`crates/ursula-stream/src/msgpack.rs`, used by `crates/ursula-raft/src/codec.rs`). A struct is an array of its fields in declaration order and an enum variant keeps its name, so field order is part of the format and every field is required. The fixtures in `crates/ursula-raft/src/fixtures` pin the bytes of every root. The gRPC messages (`crates/ursula-raft/proto/raft_internal.proto`) carry a protobuf preamble with the protocol version, the plane (`DATA` or `META`), the group ID and the node the request is for, and an opaque MessagePack `payload`, so routing and validation never decode the payload. The protocol version is the format epoch, and a node refuses peers of another epoch.

A receiver checks the protocol version first, then the target (`crates/ursula-raft/src/target.rs`), then looks the group up. It refuses a request that names no plane, a `META` request that names a group other than 0, and a command of the other plane. It refuses a request for another node with `FAILED_PRECONDITION` and its own ID in `ursula-node-id` status metadata, because the address the sender dialed serves another node now. A refused `AppendStream` item carries that ID in its `refusing_node` field, and the sender reads it back as the same status metadata. A sender reads that refusal as an unreachable peer, or a leader it cannot reach, and never as a format-epoch mismatch. A request for the meta group goes to the node's meta replica. A node that runs none, because its meta journal did not open, answers it `NOT_FOUND`, as for a group the node does not run. A `GroupRead` for the meta group is refused with `INVALID_ARGUMENT`, because the meta group serves no stream reads.

Only the owner core holds a raw `Raft` handle. `OwnerRaftHandle` (`crates/ursula-raft/src/owner.rs`) wraps it in a mailbox task on the owner's runtime. Remote callers keep a sender and the read-only metrics receivers, which may cross threads. Inbound protocol RPCs, forwarded reads and writes, admin triggers and ReadIndex confirmation all run as mailbox jobs. Jobs are polled concurrently, so a proposal waiting for quorum never blocks the vote or AppendEntries that lets it complete.

`RaftGroupHandleRegistry` publishes the hosted groups as an immutable `ArcSwap` snapshot of entries holding `OwnerRaftHandle`s. Inbound RPC routing reads that snapshot and takes no process-wide mutex.

A create, an append or close, and a snapshot publish are routed before the node reads their body. The HTTP layer reads the group's leadership from the node's own Raft metrics (`GroupLeadership`, in `crates/ursula-raft/src/leadership.rs`), without a trip through the group's core, and goes to the core only for a leader's early answer to a producer repeat. A follower answers `307` with a `Location` on the leader's configured peer URL and `x-ursula-raft-leader-id`, `x-ursula-raft-group` and `x-ursula-raft-term`. A node that does not host the group redirects to a voter that does and names the group alone. With no leader known the node answers `503` with `Retry-After` and `x-ursula-raft-group`. Once a node has read a body it never redirects: a leadership change answers `503`. Other requests a follower cannot serve turn OpenRaft's `ForwardToLeader` into a `LeadershipRefusal` (`GroupEngineError::NotLeader`). It names the leader only together with the term the same metrics view knows it in, and says whether the command may have been proposed. Such a request gets the same typed `307`, or the `503` when no leader is named. Client writes are never proxied, because admission checks must run on the leader before replication. The administrative bucket purge is the one write forwarded over the `GroupWrite` RPC. Reads that must run on the leader are forwarded over `GroupRead`.

With static peers, the server warms every hosted group at startup (up to 8 concurrent warms per core) so followers can answer Raft RPCs before any client touches a group. A group whose replay stops is logged and the other groups stay available.

Snapshots transfer as one `FullSnapshot` message (vote, metadata and payload, at most 256 MiB). Installs are serialized per group and limited node-wide by `raft.snapshot_install_max_concurrency` (default 1). Builds are limited by `raft.snapshot_build_max_concurrency` (default 2).

## Raft transport

Each owner core has its own `CoreRaftTransport` holding its peer channels and one long-lived bidirectional `AppendStream` session per peer. Groups on the same core share that session. Different cores use independent sessions, queues and encoders. The session task drains already-queued calls into one frame of at most 32 items and stops adding calls once the frame holds 4 MiB of encoded calls. It adds no batching delay. Each item carries a transport-local request ID and receives its own result, so no group inherits another group's ordering, commit index or acknowledgement. Stream requests are zstd-compressed, and unary requests are compressed from 1 KiB up. A session that has calls outstanding and no response for 15 seconds is closed, and the next call opens a new stream.

Two node-wide budgets, each sized by `raft.append_transport_budget_bytes` (default 128 MiB), bound queued append bytes in each direction regardless of the number of cores or peers:

- Send: a call takes budget in FIFO order before it is queued, and gives it back when the HTTP/2 encoder takes the frame. A call not admitted within its OpenRaft deadline fails as `Unavailable` (backlog full), which OpenRaft treats as an unreachable peer and backs off on. Queued calls whose caller has already timed out are dropped unsent.
- Receive: a follower charges each decoded inbound frame against the budget before reading the next one, so a slow follower pushes back through HTTP/2 flow control. Each inbound stream processes up to 64 items concurrently.

The budgets bound accounted frames, not total RSS or tonic and socket buffers.

Each stream names the plane of its items in `ursula-raft-plane` request metadata, and the receiver refuses an item of another plane.

The other RPCs are unary: `Vote`, `FullSnapshot`, `TransferLeader`, `RejoinBarrier` (the recovery boundary a gated replica asks its leader for, answered with the leader's committed vote, the index to apply to, and the committed and effective memberships of the leader view it confirmed), `GroupWrite`, `GroupRead` and `GenesisProbe` (the meta group's genesis probe, answered by the node's meta replica). The server also answers the unary `Append` RPC through the same handler as stream items. The outbound network never calls it and sends every AppendEntries over `AppendStream`.

`crates/ursula-raft/src/grpc.rs` also counts logical bytes for cross-AZ cost analysis: `raft_grpc_append_heartbeat_*`, `raft_grpc_append_replication_*` (requests, bytes and entries), `raft_grpc_append_response_bytes`, and the vote and snapshot request, payload and response bytes. They count encoded protobuf bytes before zstd and HTTP/2 framing, so comparing them with cloud network counters separates application, transport and billing amplification. They are process-wide atomics, with heartbeat and replication counters on separate cache lines.

## Log submission

Groups on one core share a segmented journal under `<raft.wal.path>/raft-log/core-N`. [Raft WAL](raft-wal.md) describes the on-disk format, recovery and reclamation. This section covers how appends reach it.

The log store keeps at most one pending, readable append batch per group. `append` publishes the batch, queues it to the core's journal writer and returns before any disk I/O. The writer calls `IOFlushed` only after it has written the record, applied it to the group's durable log index and satisfied the configured sync policy. Later log mutations of the group (truncate, purge, vote, committed pointer) wait for that callback, which preserves journal order. A write error poisons the pending state and is reported to OpenRaft. Readers merge the pending suffix with the durable index. Replication of a new batch therefore overlaps local journal I/O without an unbounded pending-log cache.

On every non-clean restart, a committed vote for the node itself is rewritten as the same vote uncommitted before the group's Raft core starts. A crashed leader may have lost tail entries its followers hold, and the demotion stops it from reusing their log IDs.

## Live tail watchers

Long-poll and SSE reads at the tail of an open stream register a watcher in the owning group actor. The watcher map belongs to the actor and changes only during its turns, with no lock. After an append or close commits, the actor completes the matching watchers in the same turn and hands materialization to a spawned task. A dropped waiter sends a cancel command back to the owner's mailbox. `runtime.live_read_max_waiters_per_core` (default 65,536, `0` disables) caps waiters per core. No process-wide watch registry exists.

## Scalability invariants

The write path has no process-wide serialization point:

- no global append coalescer: each group actor batches its own appends
- no single Raft group for all streams
- no shared stream metadata lock: stream state lives in its group
- no single WAL or fsync queue: each core has its own journal writer that batches across that core's groups
- no global live-tail watch registry
- no process-wide mutex on inbound AppendEntries routing

Some counters still update process-wide atomics on the append path, namely the gRPC transport counters and a few high watermarks.

## Observability and DST coverage

`GET /__ursula/metrics` reports runtime counters kept in padded per-core and per-group atomics, with totals derived when the snapshot is read. It also reports one Raft progress record per registered group from OpenRaft's watched metrics: node ID, term, leader, last log, committed, applied, snapshot and purged log IDs, voters, learners, maintenance state and log applied since the last snapshot. The gRPC transport counters above are reported in the same response.

Deterministic simulation routes AppendEntries and votes through the production registry and owner mailbox over an in-process network, with hosted tasks and a simulated disk. It covers ordering and failure semantics, including an abrupt writer abort while followers hold a replicated pending leader tail. It does not exercise the gRPC `AppendStream` transport, physical CPU placement, cache-line contention or multicore throughput, which depend on real threads and hardware.

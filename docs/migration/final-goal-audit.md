# Final Goal Audit

## Objective

Migrate the Durable Streams semantics already validated in
`/Users/xing/Idea/riverrun` into Ursula vNext's thread-per-core, shard-owned,
multi-Raft architecture. The target is a DRY and efficient Durable Streams
engine where stream metadata, append/read/close, snapshot/bootstrap, producer
idempotency, error precedence, retention-visible behavior, and cold-path S3
offload are implemented once at the correct ownership boundary and executed
through per-shard Raft groups without global serialization points.

## Success Criteria

1. Thread-per-core ownership

   Each core owns shard actors, hot stream state, live-tail watcher state, and
   the Raft groups assigned to that core. Hot state is not shared across cores.

2. Multi-Raft write scaling

   Independent stream writes are distributed across multiple Raft groups. There
   is no single global Raft group for all stream commands.

3. Protocol correctness

   Durable Streams semantics remain correct: create, append, close, delete,
   head, catch-up read, long-poll, SSE, offsets, idempotent producers, snapshots,
   bootstrap, and retention behavior.

4. S3 cold path with bounded hot memory

   Event bytes can move from hot state to S3 in the background while new writes
   continue, and admission/metrics keep hot memory growth bounded.

5. No hidden global serialization point

   Append admission, metadata mutation, Raft commit, WAL flush, watcher
   notification, and metrics collection must not collapse all work onto one
   process-wide lock, queue, actor, state machine, or fsync path.

6. Performance-ready structure

   The structure supports later real workload CPU-saturation work without
   rewriting the core Durable Streams semantics or adding compatibility shims.

## Current Evidence

| Requirement | Current Artifact | Evidence | Status |
| --- | --- | --- | --- |
| New vNext project exists | `/Users/xing/Idea/ursula` | Workspace, git repo, README, docs, crates | Started |
| Static stream placement | `crates/ursula-shard`, `StaticShardMap`, `changing_raft_group_count_remaps_many_streams` | `StaticShardMap` maps `BucketStreamId` to `CoreId` and `RaftGroupId` by `fnv1a64(bucket_id + "/" + stream_id) % raft_group_count`, then `raft_group_id % core_count`. This is a fixed-cluster bootstrap placement, not rendezvous/consistent hashing. A regression test documents that changing `raft_group_count` remaps many streams, so dynamic split/merge needs a future persisted routing table or virtual-bucket layer rather than changing the modulo directly. | Implemented prototype; not dynamic-placement ready |
| Pure stream state-machine slice | `crates/ursula-stream`, `append_conflict_precedence_reports_closed_before_mismatch_or_seq` | `StreamStateMachine::apply` and `StreamStateMachine::read` cover bucket create/delete, stream create/append/read/close/delete, head metadata, content type checks, stream seq ordering, and closed-state monotonicity. Append conflict precedence is pinned to report closed-stream conflicts before content-type mismatch or stream-seq regression, matching the Durable Streams spec guidance that clients should receive `Stream-Closed: true` on closed streams even when other conflicts are also present. | Tested prototype |
| Stream snapshot semantic core | `StreamSnapshot`, `StreamStateMachine::snapshot`, `StreamStateMachine::restore`, `snapshot_restore_round_trips_payload_metadata_and_stream_seq`, `snapshot_restore_preserves_visible_snapshot_and_message_records` | Stream snapshots serialize buckets, stream metadata, payload bytes, stream sequence state, visible protocol snapshots, retained message boundaries, and producer state in deterministic order; restore rejects duplicate buckets, duplicate producers, missing buckets, inconsistent retained message boundaries, visible snapshot offsets beyond tail, and tail-offset/payload-length mismatches | Tested prototype |
| Idempotent producer semantic core | `ProducerRequest`, `ProducerSnapshot`, `producer_headers_deduplicate_retries_and_fence_stale_epochs`, `producer_append_batch_deduplicates_retries_without_partial_mutation`, `producer_state_survives_snapshot_restore`, `producer_duplicate_final_append_remains_idempotent_after_close` | Stream state records per-stream producer epoch/sequence state, deduplicates exact retries without changing payload, preserves per-item batch offsets for batch retry acks, fences stale epochs, rejects sequence gaps, preserves producer state through snapshots, and keeps a final append idempotent after close | Tested prototype |
| Shard-owned group snapshot boundary | `GroupEngine::snapshot`, `ShardRuntime::snapshot_group`, `snapshot_group_routes_to_owner_core_and_captures_only_group_state` | Group snapshots route by `RaftGroupId` to the owner core, return placement plus group commit index and per-stream append counts, include only that group's stream state, and reject out-of-range groups before mailbox routing | Tested prototype |
| Shard-owned group snapshot install boundary | `GroupEngine::install_snapshot`, `ShardRuntime::install_group_snapshot`, `install_group_snapshot_restores_group_state_and_append_counts`, `install_group_snapshot_rejects_mismatched_placement_before_routing` | Group snapshots install through the owning core, restore stream state plus per-stream append counts, preserve subsequent append offsets/counts, and reject placement mismatches before mailbox routing | Tested prototype |
| Shard-owned group warmup boundary | `ShardRuntime::warm_group`, `ShardRuntime::warm_all_groups`, `warm_group_instantiates_engine_on_owner_core_without_stream_mutation` | Runtime can instantiate a group engine on the owning core by `RaftGroupId` without routing a stream mutation or snapshot read through the engine. Repeated warmup is idempotent for an already-created group, and `warm_all_groups` can precreate all statically assigned groups. This is the local ownership hook needed before a deployed Raft RPC layer can expose follower handles for groups that have not yet received client traffic on that node | Tested prototype |
| Shard-owned actor path | `crates/ursula-runtime`, `core_worker_dispatches_other_groups_while_one_group_waits` | `ShardRuntime::create_stream`, `ShardRuntime::append`, `ShardRuntime::read_stream`, `ShardRuntime::close_stream`, and `ShardRuntime::head_stream` route through core mailboxes; `CoreWorker` owns group placement and dispatches group commands into per-group async mutexes, so one blocked group does not stop the core dispatcher from completing work for another owned group | Tested prototype |
| Thread-per-core worker mode | `RuntimeThreading::ThreadPerCore` | `RuntimeConfig::new` defaults to one current-thread Tokio runtime per core worker thread | Implemented prototype |
| Replaceable Raft-group boundary | `GroupEngine`, `GroupEngineFactory`, `InMemoryGroupEngine` | Default engine wraps `ursula-stream`; custom engine tests prove one engine is created per touched group on the owning core | Implemented prototype |
| Replicated group write command boundary | `GroupWriteCommand`, `GroupWriteResponse`, `InMemoryGroupEngine::apply_committed_write`, `group_write_command_round_trips_as_log_payload`, `committed_write_command_is_state_machine_apply_boundary` | Runtime write operations now have a serializable group-level command envelope suitable for a future OpenRaft log/proposal payload; the in-memory engine applies committed writes through one state-machine boundary, and the WAL prototype records the same envelope while still replaying older `StreamCommand` records | Tested prototype |
| Shared durable protobuf schema | `crates/ursula-proto`, `durable.proto`, `BucketStreamIdV1`, `ProducerRequestV1`, `ExternalPayloadRefV1`, `ColdChunkRefV1`, `RaftGroupCommandV1`, `RaftGroupResponseV1`, `bucket_stream_id_round_trips_through_shared_proto`, `raft_group_command_uses_shared_protobuf_log_schema`, `raft_group_response_serde_uses_shared_protobuf_log_schema` | Durable protocol/persistent schema now lives in a lower-level shared protobuf crate instead of in `raft_internal.proto`. `ursula-stream` and `ursula-runtime` reuse the prost structs for producer requests and cold/external payload references, and only those three shared runtime JSON/snapshot types derive serde in the generated proto crate. `BucketStreamId` remains a local semantic key for hashing/display/validation/map ownership, but `ursula-shard` owns conversion to and from shared `BucketStreamIdV1`, so `ursula-raft` no longer hand-codes that schema mapping. `ursula-raft` uses thin local OpenRaft wrapper types over `RaftGroupCommandV1` / `RaftGroupResponseV1` rather than defining a private Raft app-log schema; their serde implementations encode/decode the shared prost messages as protobuf bytes for OpenRaft's current serde-bound containers. Focused Raft tests cover both command prost roundtrip back to the runtime command and response rmp-container roundtrip back to the runtime response. Identity conversion helpers for producer, external payload, and cold chunk refs were removed from the Raft adapter; those fields now pass through as shared prost values. Remaining serde/rmp usage is limited to the OpenRaft RPC/container/journal compatibility layer, not a second durable command schema. | Tested prototype |
| OpenRaft local group adapter | `crates/ursula-raft`, `UrsulaRaftTypeConfig`, `RaftGroupLogStore`, `RaftGroupFileLogStore`, `CoreFileLogWriter`, `RaftGroupStateMachine`, `RaftGroupEngine`, `RaftGroupEngineFactory`, `DurableRaftGroupEngineFactory`, `single_node_openraft_group_applies_client_writes`, `raft_group_engine_implements_runtime_group_engine_over_openraft`, `raft_group_engine_recovers_client_writes_from_file_log`, `raft_file_log_store_recovers_vote_committed_and_entries`, `raft_file_log_store_recovers_truncate_and_purge`, `durable_raft_group_engine_records_file_log_metrics`, `durable_raft_group_engine_recovers_from_core_journal`, `shard_runtime_uses_raft_group_engine_factory_for_owned_group`, `openraft_state_machine_applies_group_write_commands`, `openraft_snapshot_round_trips_group_state` | A real OpenRaft type config uses local `RaftGroupCommand` / `RaftGroupResponse` wrappers around shared `ursula-proto` app-log messages as application data; the in-memory log store covers append/read, vote, committed pointer, truncate, and purge; the standalone file log store persists those OpenRaft log-store fields as append-only group-local journal records and recovers them across reopen, with normal application entries stored as `RaftGroupCommandV1` protobuf bytes inside the outer journal record; the durable runtime factory gives groups on each owner core a shared core-local journal writer, persists length-prefixed binary records to `core-{id}/journal.bin`, recovers group state from that core journal, and records file-log write/sync metrics into the per-core/per-group durable-log counters; tests assert vote/commit/append/truncate/purge produce multiple journal lines instead of full-state rewrites; a single-node OpenRaft group initializes, elects itself, applies create/append writes through Ursula's group state-machine boundary, and can recover client writes from the file log; `RaftGroupEngine` implements the runtime `GroupEngine` shape over OpenRaft `client_write` plus state-machine reads; the runtime async engine factory can instantiate a runtime-owned local Raft group; OpenRaft snapshots encode and restore `GroupSnapshot` bytes | Tested adapter/durability prototype |
| OpenRaft multi-node replication probe | `RaftGroupEngine::new_node_with_log_store_and_network`, `three_node_openraft_group_replicates_group_writes` | `RaftGroupEngine` now has a constructor path that accepts an injected `RaftNetworkFactory` instead of being hardwired to `SingleNodeRaftNetworkFactory`. A focused in-process three-node test boots one Ursula group on three OpenRaft nodes, initializes three-voter membership, waits for election, submits create/append through the elected leader, waits for all three nodes to apply the append log index, and reads the replicated payload from each node's state machine. This proves the Ursula group command/state-machine boundary works under real OpenRaft Vote and AppendEntries replication, but it is not yet a production cross-process transport, persisted peer configuration, leader-routing layer, or deployment CLI. | Tested transport/replication prototype |
| Runtime-owned Raft handle registry | `RaftGroupHandleRegistry`, `RegisteredRaftGroupEngineFactory`, `warm_group_registers_runtime_owned_raft_handle` | A registered OpenRaft factory can create a runtime-owned `RaftGroupEngine` on the owner core and register the resulting local `Raft` handle by `RaftGroupId`. The registry exposes lookup plus local AppendEntries, Vote, and full-snapshot dispatch helpers used by the gRPC Raft transport. The current test warms group 3 through `ShardRuntime`, verifies the handle is registered exactly once, and waits for the registered single-node group to elect itself. This is the local dispatch boundary; cross-router behavior is covered by the gRPC Raft transport row | Tested local RPC-dispatch boundary |
| Per-group WAL/recovery boundary | `WalGroupEngineFactory`, `WalGroupEngine`, `wal_group_engine_recovers_multiple_groups_from_per_group_logs`, `wal_group_engine_batches_append_records_and_recovers`, `wal_group_engine_persists_installed_snapshot`, `wal_group_engine_recovers_producer_append_batch_dedup_state` | Optional engine writes JSONL command records to `core-{id}/group-{id}.jsonl`, replays them on group construction, recovers streams on distinct Raft groups, persists HTTP-batch-shaped appends through a group-level batch boundary, skips duplicate producer batch WAL records, and replays installed group snapshots after restart | Tested prototype |
| HTTP WAL/recovery path | `--wal-dir`, `spawn_wal_runtime`, `wal_runtime_recovers_http_stream_after_restart` | `ursula-http` can run the WAL-backed group engine explicitly, and router tests verify append-batch data remains readable after runtime restart | Tested prototype |
| HTTP OpenRaft in-memory path | `--raft-memory`, `spawn_raft_memory_runtime`, `RaftGroupEngineFactory`, `raft_memory_runtime_serves_http_subset_without_wal_metrics` | `ursula-http` can run the OpenRaft-backed group engine explicitly without local WAL persistence; router tests verify create, append, read, snapshot publish, bootstrap readback, and zero WAL metrics for the in-memory Raft log store | Tested adapter/diskless prototype |
| Internal gRPC Raft RPC transport | `router_with_raft_registry`, `router_with_static_raft_cluster`, `RaftInternal::{Vote,Append,FullSnapshot}`, `RAFT_GRPC_*_PATH`, `GrpcRaftNetworkFactory`, `GrpcRaftNetwork`, `StaticGrpcRaftGroupEngineFactory`, `GroupLeaderHint`, `spawn_static_grpc_raft_memory_runtime`, `spawn_static_grpc_raft_runtime`, `DurableRaftLogStoreFactory`, `--raft-node-id`, `--raft-peer`, `--raft-init-membership`, `--raft-cluster-config`, `--raft-log-dir`, `__ursula/raft/{group}/snapshot`, `__ursula/raft/{group}/purge`, `__ursula/raft/{group}/learners/{node}`, `raft_grpc_network_dispatches_to_registered_runtime_owned_group`, `openraft_installs_snapshot_for_lagging_learner`, `static_grpc_raft_group_engine_replicates_between_routers`, `static_grpc_raft_group_engine_replicates_with_core_journals`, `static_grpc_raft_durable_cold_flush_replicates_manifest`, `static_grpc_raft_installs_snapshot_for_late_learner_over_tcp`, `static_grpc_raft_installs_snapshot_for_late_learner_with_core_journals`, `static_grpc_raft_runtime_can_use_core_journal`, `static_grpc_raft_runtime_recovers_from_core_journal_after_restart`, `cli_static_grpc_raft_cluster_redirects_follower_writes`, `cli_static_grpc_raft_log_dir_recovers_after_restart`, `cli_static_grpc_raft_log_dir_recovers_cold_manifest_after_restart`, `cli_static_grpc_raft_log_dir_replicates_between_nodes`, `cli_static_grpc_raft_log_dir_installs_snapshot_for_late_learner`, `cli_static_grpc_raft_log_dir_replicates_cold_manifest`, `cli_static_grpc_raft_log_dir_recovers_replicated_cold_manifest_after_restart`, `cli_static_grpc_raft_log_dir_recovers_replicated_s3_cold_manifest_after_restart`, `parses_static_grpc_raft_cluster_args`, `parses_static_grpc_raft_cluster_config_file`, `parses_static_grpc_raft_cluster_with_durable_log_dir`, `rejects_conflicting_raft_node_id_from_config_file` | The HTTP adapter now mounts a tonic `RaftInternal` service for node-to-node Vote, AppendEntries, and full-snapshot transfer instead of owning JSON Raft endpoints. The gRPC RPC metadata is protobuf, while OpenRaft's request/response containers, vote, log entries, membership, and snapshot metadata still use the transitional `openraft-rmp-serde-v1` payload codec. Durable app-log command/response schema has moved out of the transport proto into shared `ursula-proto`. The static gRPC factory wires this transport into runtime-owned `RaftGroupEngine` construction with static peers and carries the configured cold store into each group's state machine. It can now use either in-memory OpenRaft logs or the same core-local durable OpenRaft `journal.bin` backend selected by `--raft-log-dir`; `DurableRaftLogStoreFactory` keeps the core journal writer in `ursula-raft` so the HTTP layer selects storage without duplicating write/sync logic. Public write requests landing on followers preserve OpenRaft `ForwardToLeader` as a structured `GroupLeaderHint`; the Raft group adapter returns that hint instead of performing group-level write forwarding, and the HTTP adapter forwards the complete write request to the leader runtime so runtime-owned watcher notification and write-side preflight run at the same ownership boundary. Public leader-owned live reads and `HEAD`/`GET` paths still return ordinary leader redirects where redirect semantics are required, while catch-up reads can use internal group read forwarding. A real TCP test starts three Ursula HTTP routers, warms four Raft groups on each node, initializes three-voter membership on node 1 for all groups, verifies follower routing behavior, writes create/append through node 1's runtime, waits for replicated payloads to become readable on every runtime for streams placed on all four groups, and sends a full snapshot from node 1 to node 2 through `GrpcRaftNetwork::full_snapshot`. A durable variant starts three local routers with independent `--raft-log-dir` roots, warms two Raft groups, writes one stream per group through the leader, waits for every node's state machine to read the replicated payloads, verifies non-zero durable-log metrics on every router, and checks each node's `core-0/journal.bin`. A durable cold variant starts three static gRPC nodes with independent journals and a shared cold store, writes through the leader, batch-flushes cold metadata, verifies every node's Raft state machine reads the complete payload through the replicated cold manifest, and checks non-empty core journals. The binary can read static node id, peer URLs, and initial-membership intent from a JSON `--raft-cluster-config` file instead of only from repeated CLI flags; parser tests cover the file shape, durable `--raft-log-dir` static-cluster args, and conflicting node ids. Binary-level durable smokes start real `ursula-http` processes with `--raft-cluster-config` and `--raft-log-dir`: one writes through HTTP, kills the process, restarts from the same log dir without reinitializing membership, and reads back the committed payload; another starts three processes with independent log dirs, writes through node 1, reads the replicated payload through node 3, and checks each node's `core-0/journal.bin`; a CLI late-learner variant starts a two-node durable cluster, commits a stream, uses admin HTTP endpoints to trigger leader snapshot and purge, starts a previously absent third real process, adds it as an OpenRaft learner over HTTP, waits for the learner metrics to report the installed snapshot, then reads the stream after snapshot catch-up; cold-path variants cover fs and real S3 cold roots, flush until `cold_hot_bytes` reaches zero, replicate cold manifests, restart all nodes without reinitializing membership, and read back through restarted followers. The local single-node static gRPC durable-log tests warm the registered group, wait for leadership, write through HTTP, read back, verify durable-log metrics, check that `core-0/journal.bin` contains records, and restart against the same log dir with membership loaded from the journal before reading back the committed payload. The in-process OpenRaft test `openraft_installs_snapshot_for_lagging_learner` snapshots and purges a two-voter leader, adds a previously empty third node as a learner, asserts that the replication layer calls `RaftNetworkV2::full_snapshot`, and verifies the learner can read the stream restored from the installed snapshot. The router-level TCP test `static_grpc_raft_installs_snapshot_for_late_learner_over_tcp` starts a two-node static gRPC cluster, writes through the leader HTTP API, snapshots and purges the leader, starts a third Ursula HTTP router late, adds it as an OpenRaft learner with its gRPC address, waits for the learner to install the snapshot, and reads the restored stream through the late learner's HTTP endpoint. The durable variant runs the same late-learner snapshot flow with independent node log roots and verifies a non-empty `core-0/journal.bin` for leader, follower, and learner. Those tests exposed and fixed a structural bug where the leader's snapshot builder returned snapshot bytes to OpenRaft but did not retain them in `get_current_snapshot`, causing later snapshot transmission to fail with `snapshot not found`. The current EC2 smoke ran the same gRPC transport across three `c7g.4xlarge` nodes with S3 cold storage enabled, and the official suite passed against that EC2 shape. A follow-up EC2 smoke used the current durable-log-capable binary with independent `--raft-log-dir` roots, real S3 cold storage, all-node restart without reinitializing membership, and cold-backed follower readback after restart. This proves local cross-router gRPC AppendEntries replication, full-snapshot gRPC transfer, automatic lagging-learner snapshot catch-up in process and through local TCP routers, CLI-accessible snapshot/purge/add-learner controls for EC2 validation, multi-group static-cluster warmup/replication, no retained HTTP internal Raft path, static peer config file parsing, static gRPC plus durable core journal write/restart recovery through both library and binary paths, local multi-node durable-log replication through both library and binary paths, replicated cold manifests over durable static gRPC, binary-level durable cold-manifest replication/restart recovery including real S3, durable late-learner snapshot transfer through library, local TCP, and real binary CLI shapes, a short real EC2 current-transport deployment, a short EC2 durable-log/S3 restart deployment, and a short EC2 late-learner full-snapshot deployment; it still lacks dynamic/reconfigurable membership and longer durable-log/S3 soak and performance validation | Tested local cross-router gRPC prototype; local lagging learner snapshot tests, static config parser, static durable-log write/restart, binary restart, multi-node replication, replicated cold manifest, binary cold manifest replication/restart recovery, binary late-learner snapshot, late-learner snapshot wiring, EC2 static gRPC smoke, EC2 durable-log/S3 restart smoke, and EC2 late-learner snapshot smoke passed |
| Distributed static group initializers | `--raft-init-membership-per-group`, `init_membership_per_group`, `StaticGrpcRaftGroupEngineFactory::with_per_group_membership_initializers`, `static_grpc_per_group_membership_initializers_distribute_leaders`, `parses_static_grpc_per_group_membership_initializers`, `parses_static_grpc_per_group_membership_initializers_from_config_file` | Static gRPC has an explicit mode where every node may be started with membership initialization enabled, but each Raft group is initialized by exactly one node chosen by `raft_group_id % sorted_peer_count`. The default `--raft-init-membership` behavior is unchanged for single-initializer deployments. The focused router test starts three local nodes, warms six groups on every node, and verifies observed leaders rotate 1, 2, 3, 1, 2, 3 across the groups. CLI and JSON config parser tests cover the new mode. This addresses the EC2 perf finding that initializing all groups from node 1 concentrates leader and cold-path work on node 1. A Linux aarch64 release binary with sha256 `237a3bef56a4e331307b19645d70977b8efb47d46d3d8af5d2f53ca6e0f94ae7` was deployed to the three EC2 `c7g.4xlarge` nodes on port `4487` with `--raft-memory`, 12 groups, and `--raft-init-membership-per-group` on every node. Metrics from all three nodes reported leaders `[1,2,3,1,2,3,1,2,3,1,2,3]`, and a minimal public create/read smoke through node 1 succeeded. The same binary was then run on port `4488` with 64 groups and `URSULA_COLD_BACKEND=s3`; all nodes reported leader distribution 22/21/21, and four client processes with unique buckets reached about 229k 128-byte events/s with zero errors. Accepted appends and cold uploads were distributed across all three nodes, and temporary processes, logs, credential env files, loader files, and the S3 root were cleaned up. | Tested locally and EC2 S3 perf smoke passed |
| EC2 static multi-group gRPC S3 smoke | `docs/migration/ec2-static-cluster-s3-smoke.md` | On 2026-05-18, Linux aarch64 `ursula-http` binary `d48e353915876857d8a6049a202fec64286b8ecf72ce0a9a004a8d6eda1f9c9c` ran on three existing `c7g.4xlarge` nodes across `us-east-1a/b/c`, with a `c7gn.8xlarge` client. The cluster used `--raft-memory`, 16 cores, 64 Raft groups, current tonic gRPC internal Raft transport on port `4477`, node 1 `--raft-init-membership`, and `URSULA_COLD_BACKEND=s3` pointed at `ursula-c7g-beast-us-east-1` under `ursula-grpc-smoke/20260518T033351Z`. A client `PUT` to follower node 2 returned `307` with leader id 1 and a node-1 `Location`, leader node 1 create returned `201`, a 4096-byte append returned `204` with next offset 4123, node 3 read back through leader redirect, node 1 metrics reached `cold_flush_uploads=5`, `cold_flush_publishes=5`, and `cold_hot_bytes=0`, S3 listed five cold chunks under the smoke root, and a post-flush node-3 redirected read returned both the cold prefix and a cold range slice. Node logs had no error/panic/cold-worker lines. A second EC2 smoke built current binary `99d29c53ea1b51a52ea4df03df9e87fade99ff1f55eb04c0665bb930f87711b8`, ran the same three-node static gRPC shape on port `4478` with independent `--raft-log-dir` roots and real S3 cold storage, wrote a 4127-byte stream through the owner leader, observed node 3 metrics `cold_flush_uploads=5`, `cold_flush_publishes=5`, `cold_hot_bytes=0`, `wal_batches=285`, and `wal_records=285`, verified five S3 chunks totaling 4127 bytes, stopped all nodes, restarted them from the same durable log roots without `--raft-init-membership`, and read the cold-backed prefix through a restarted follower. Temporary `4477`/`4478` processes, remote credential env files, log roots, and S3 objects were cleaned up. | Current gRPC S3 and durable-log restart smokes passed |
| Official Durable Streams conformance | `docs/migration/official-conformance.md`, official `durable-streams/durable-streams` checkout at `8d78524` | Current in-memory OpenRaft HTTP path passes all 300 official conformance tests after the snapshot/bootstrap extension implementation. The suite was rerun on the current Ursula checkout after the internal Raft transport moved to tonic gRPC and `FlushCold` learned to coalesce contiguous hot segments; it passed `300 / 300` with `URSULA_COLD_BACKEND=memory`, aggressive background flush, and post-run metrics showing `cold_flush_uploads=15034` and `cold_flush_publishes=15034`. After planner-side hot-segment coalescing was added, the suite was rerun again against current local `--raft-memory` with aggressive memory cold flush and passed `300 / 300` in 16.54s, with `cold_flush_uploads=856`, `cold_flush_publishes=855`, `cold_backpressure_events=0`, and `wal_batches=0`. The current checkout was rerun after cold-admission Raft proposal coalescing, stale cold-flush candidate cleanup, and the logical Raft write metric; the local `--raft-memory`, memory cold store, aggressive 1-byte background flush shape passed `300 / 300` in 16.86s, with `raft_write_many_commands=2279`, `raft_write_many_logical_commands=112468`, `cold_flush_uploads=112468`, `cold_flush_publishes=112447`, `cold_orphan_cleanup_attempts=21`, `cold_orphan_cleanup_errors=0`, `cold_backpressure_events=0`, and no mailbox-full events. The official suite also passed locally against the durable OpenRaft `--raft-log-dir` path with memory cold store configured but background flush disabled, proving the upstream protocol semantics through the file-log persistence path: `300 / 300` in 34.60s, with 3,836 durable-log batches/records and non-zero sync metrics. The deliberately aggressive combination of durable file-log plus 1-byte background cold flush initially exposed a performance timeout at `298 / 300`; after group-local cold flush planning and metadata publish through a single Raft `Batch`, the same debug-mode durable file-log run passed `300 / 300` in 47.02s, with 9,072 durable-log batches/records, 34.999s aggregate sync time, 105,214 cold uploads, and 105,146 cold publishes. The same official suite also passed from the `c7gn.8xlarge` client against the EC2 static gRPC cluster at node 1 private IP `10.99.1.48:4477`, with three `c7g.4xlarge` servers, 64 Raft groups, S3 backend root `ursula-grpc-conformance/20260518T034603Z`, and result `300 / 300` in 20.36s. That EC2 run used S3 for the official large-payload case, producing one 10 MiB external object; background flush did not run because remaining hot bytes were below the 1 MiB threshold. The final base-suite fixes added fork prefix-copy creation, source content-type/offset validation, fork TTL/Expires-At inheritance, source refcount, soft-delete `410`, recreation/fork conflict handling, and cascade GC through recursive fork chains. The upstream suite still does not directly exercise Ursula's `/snapshot` or `/bootstrap` extension endpoints, based on a local search of `packages/server-conformance-tests/src`; Ursula now carries local HTTP extension coverage for those endpoints. | Met for upstream base suite locally and on EC2; local extension coverage added; durable-log plus aggressive cold flush now passes locally |
| Snapshot/bootstrap protocol extensions | `docs/specs/extensions.md` in `/Users/xing/Idea/riverrun`, `StreamCommand::PublishSnapshot`, `StreamStateMachine::{latest_snapshot,read_snapshot,delete_snapshot,bootstrap_plan}`, `GroupEngine::{publish_snapshot,read_snapshot,delete_snapshot,bootstrap_stream}`, `ShardRuntime::{publish_snapshot,read_snapshot,delete_snapshot,bootstrap_stream}`, HTTP routes `/{bucket}/{stream}/snapshot`, `/{bucket}/{stream}/snapshot/{snapshot_offset}`, `/{bucket}/{stream}/bootstrap`, `publish_snapshot_advances_retention_on_message_boundary`, `publish_snapshot_rejects_unaligned_offset`, `snapshot_and_bootstrap_routes_follow_extension_semantics`, `bootstrap_without_snapshot_emits_empty_snapshot_part_and_rejects_live`, `snapshot_publish_errors_and_overwrite_follow_extension_statuses`, `bootstrap_reads_retained_updates_from_cold_chunk_after_snapshot` | Protocol-visible snapshots are owned by stream state, not HTTP-local state. Snapshot publish is a group write, validates committed message boundaries, rejects invalid/reserved offsets with `400`, rejects offsets beyond tail with `409`, rejects offsets older than retained with `410`, advances retained offset, compacts hot prefix, and makes ordinary reads below retained offset return `410`. `HEAD` exposes `Stream-Snapshot-Offset`; latest snapshot redirects with `307` or returns `404` when absent; snapshot read returns the stored blob/content type and continuation offset; overwrite hides superseded snapshots and makes old reads/deletes return `404`; deleting the latest snapshot returns `409`; `/bootstrap` rejects `live`, emits an empty first part and `Stream-Snapshot-Offset: -1` when no snapshot exists, and returns multipart snapshot-plus-retained-updates using the same message records, including retained updates that live inside cold chunks and must be fetched by range read. The OpenRaft-backed HTTP path also exercises snapshot publish and bootstrap. | Tested prototype |
| S3 cold path | `crates/ursula-stream`, `crates/ursula-runtime`, `crates/ursula-raft`, `crates/ursula-http`, `crates/ursula-runtime/tests/s3_cold_path.rs`, `docs/migration/cold-path-progress.md` | Stream state now has `FlushCold` metadata publish, `ColdChunkRef` manifest entries, hot-start offsets, cold-aware snapshots, read planning over cold+hot segments, hot-prefix compaction, deterministic group-local candidate selection, preview-state batch candidate planning, hot-byte accounting, and deleted-stream skipping for background flush scans. Runtime has an opendal-backed memory/fs/S3 `ColdStore`, transparent cold+hot read reassembly when configured, object range reads for cold slices, `plan_cold_flush`, actor-external `flush_cold_once` upload then metadata publish, bounded group flush scanning via `flush_cold_all_groups_once_bounded`, group-local batch cold flush that uploads multiple candidates outside apply then publishes metadata through one Raft `Batch`, orphan cleanup when metadata publish fails after upload, stale-candidate handling for streams deleted between plan and publish, stale invalid-flush cleanup for delete/recreate races, group-owned cold write admission with per-group hot-byte limits, per-group hot-byte gauges, cold backpressure counters, S3 root override for isolated integration tests, and S3 recursive cleanup for temporary cold roots. HTTP starts an optional background flush worker with `URSULA_COLD_FLUSH_MAX_CONCURRENCY`, configures `URSULA_COLD_MAX_HOT_BYTES_PER_GROUP` by default when a cold store is enabled, returns `503` for cold backpressure, exposes cold upload/publish/orphan/hot-byte/backpressure metrics, and keeps `POST /__ursula/flush-cold/{bucket}/{stream}` for explicit single-stream flush testing. Tests cover metadata publish, store-backed read reassembly, actor-external single flush, group-local selection, batch candidate planning, bounded multi-group flush, repeated append/flush steady state with `cold_hot_bytes` returning to zero while new writes continue, range reads, metrics exposure, write admission, stale candidate rejection after delete/recreate without stream mutation, runtime stale-candidate orphan cleanup, HTTP backpressure status, endpoint-driven readback, cold-enabled official conformance with memory backend, durable-log aggressive cold flush conformance, gated real-S3 runtime integration, and gated real-S3 three-process static-gRPC durable-log restart integration. The runtime S3 gate passed locally and on `ursula-c7g-beast-node-1` against `riverrun-e2e-us-east-1`, covering actual S3 upload, range read, manifest readback, metrics, and cleanup. The binary S3 gate passed locally against `ursula-c7g-beast-us-east-1` using exported AWS SSO credentials, covering three real `ursula-http` processes with independent durable logs, replicated S3 cold manifests, full-node restart without reinitializing membership, follower readback from S3-backed chunks, and unique S3 root cleanup | Tested |
| HTTP OpenRaft file-log path | `--raft-log-dir`, `spawn_raft_runtime`, `DurableRaftGroupEngineFactory`, `raft_runtime_serves_http_subset_and_writes_core_journal` | `ursula-http` can run the OpenRaft-backed durable group engine explicitly; router tests verify create, append, read, non-zero file-log metrics, and a core-local OpenRaft `journal.bin` under the configured core directory | Tested adapter/durability prototype |
| Runtime uses stream semantics | `InMemoryGroupEngine`, `append_before_stream_setup_uses_stream_state_machine_error`, `create_stream_is_routed_and_idempotent_for_matching_metadata` | Runtime create/append/read/close/head flows apply `ursula-stream` commands or reads and propagate semantic errors with core/group context | Tested |
| Per-core/per-group metrics | `crates/ursula-runtime` | `RuntimeMetricsSnapshot` exposes accepted appends, per-core appends, per-group appends; accepted total is derived from per-core counters | Implemented prototype |
| Per-core/per-group mutation metrics | `runtime_metrics_track_owner_core_routing_and_mailbox_wait`, `GET /__ursula/metrics` | Runtime tracks successful state mutations and mutation apply time across create/append/close/delete per owner core and per group; HTTP metrics expose totals and arrays | Tested prototype |
| Per-core routing/backpressure metrics | `runtime_metrics_track_owner_core_routing_and_mailbox_wait`, `mailbox_full_events_record_owner_core_backpressure`, `GET /__ursula/metrics` | Runtime tracks routed requests, mailbox send wait nanoseconds, and mailbox-full enqueue events per owner core; HTTP metrics expose totals and per-core arrays for CPU plateau diagnosis | Tested prototype |
| OpenRaft group progress metrics | `RaftGroupHandleRegistry::metrics_snapshot`, `RaftGroupMetricsSnapshot`, `GET /__ursula/metrics`, `raft_grpc_network_dispatches_to_registered_runtime_owned_group`, `static_grpc_raft_installs_snapshot_for_late_learner_over_tcp` | The HTTP metrics endpoint now exposes one read-only OpenRaft progress record per registered Raft group, including node id, current term/leader, last log index, committed/applied/snapshot/purged log frontiers, voters, and learners. The registered-router test verifies group metrics appear through HTTP, and the local TCP late-learner snapshot test asserts leader snapshot/purge indexes plus late learner installed-snapshot index through the same endpoint. This gives EC2/restart validation a direct root-cause signal for snapshot transfer and compaction instead of relying only on readback. | Tested local HTTP metrics path |
| Per-core/per-group WAL metrics | `GroupEngineMetrics`, `wal_group_engine_batches_append_records_and_recovers`, `GET /__ursula/metrics` | WAL-backed group engine records batch count, record count, write nanoseconds, and sync nanoseconds per owner core and group; HTTP exposes the counters | Tested prototype |
| Per-core mailbox depth metrics | `RuntimeMailboxSnapshot`, `mailbox_snapshot_reports_per_core_depths_and_capacities` | Runtime exposes mailbox depth and capacity per core using Tokio bounded-channel capacity, without adding a hot-path write | Tested |
| Metrics avoid a hot global append counter | `PaddedAtomicU64`, `RuntimeMetricsInner::record_append` | Append success records only padded per-core and per-group counters; snapshot computes total accepted appends | Implemented prototype |
| Intra-stream ordering | `repeated_appends_to_one_stream_are_ordered` | 100 appends to one created stream advance offsets monotonically inside owning actor | Tested |
| Independent stream distribution | `independent_streams_reach_all_cores_and_many_groups` | 4096 created streams reach every configured core and more than 48 of 64 groups | Tested |
| Metadata query ownership | `head_stream_reflects_append_and_closed_state_on_owner_group` | HEAD-style metadata query routes to the owning group and observes content type, tail offset, and closed state after append | Tested |
| Catch-up read ownership | `read_stream_returns_payload_slice_from_owner_group` | Read query routes to owning group and returns payload slices with next offset and up-to-date status | Tested |
| Shard-owned live-tail waiters | `wait_read_stream_completes_after_owner_append`, `wait_read_stream_completes_on_close_at_tail`, `canceled_wait_read_stream_removes_owner_waiter` | Readers at an open tail wait on the owning core, complete after append/close, and cancellation removes owner-core waiter state | Tested |
| Live-tail waiter lifecycle limits | `RuntimeConfig::live_read_max_waiters_per_core`, `URSULA_LIVE_READ_MAX_WAITERS_PER_CORE`, `live_read_waiter_limit_rejects_excess_waiters_on_owner_core`, `long_poll_returns_service_unavailable_when_live_waiters_are_full`, `live_read_backpressure_events` metrics | Live long-poll/SSE waiter admission is now bounded at the owner-core watcher map instead of at the HTTP connection layer. The runtime default is a high but finite per-core limit, `0` in the env var disables it for explicit experiments, excess waiters return `LiveReadBackpressure`, HTTP maps that to `503 Service Unavailable`, and metrics expose total/per-core live-read backpressure counts. | Tested prototype |
| Close-only protocol path | `close_stream_allows_close_only_and_rejects_later_appends` | Empty close command routes to owning group, closes without advancing offset, rejects later append, and does not increment append metrics | Tested |
| Delete stream ownership | `delete_stream_removes_state_on_owner_group` | Stream deletion routes to the owning group, removes state, and later HEAD/append return stream-not-found errors | Tested |
| Shard-owned append-batch path | `AppendBatchRequest`, `CoreCommand::AppendBatch`, `append_batch_routes_once_and_applies_each_payload_on_owner_core` | HTTP batch frames are routed once to the owning core, then applied sequentially on that core while counting each frame as an accepted append/mutation | Tested prototype |
| Minimal HTTP adapter | `crates/ursula-http` | Axum adapter routes PUT/POST/GET/HEAD requests to `ShardRuntime` without owning stream state | Implemented prototype |
| HTTP perf subset | `create_append_read_and_head_match_perf_compare_subset`, TCP curl smoke test | `PUT /benchcmp/{stream}`, `POST /benchcmp/{stream}`, and `GET /benchcmp/{stream}?offset=2&max_bytes=3` work through router tests and real TCP | Tested |
| HTTP append-batch subset | `append_batch_matches_perf_compare_frame_format`, `append_batch_minimal_ack_skips_success_body_but_keeps_item_errors`, TCP curl smoke test | `POST /benchcmp/{stream}/append-batch` parses big-endian length-prefixed frames, returns `[{\"status\":204}]` acks by default, can return `204 No Content` for all-success batches when `Prefer: return=minimal` is sent, preserves JSON per-item status on partial errors, and appends readable payload bytes | Tested |
| HTTP close-only path | `close_only_post_sets_closed_state_and_rejects_later_append` | `POST` with `stream-closed: true` and empty body closes the stream and later append returns conflict | Tested |
| HTTP stream sequence header | `stream_seq_header_rejects_regressing_appends` | `stream-seq` is passed from HTTP into the stream state machine and duplicate/regressing sequence values return conflict | Tested |
| HTTP producer headers | `producer_headers_deduplicate_retries_and_fence_stale_epochs`, `append_batch_producer_headers_deduplicate_retries` | `Producer-Id`, `Producer-Epoch`, and `Producer-Seq` must be supplied together for append, close, and append-batch flows; success responses echo producer epoch/seq; duplicate retries do not append bytes again; sequence gaps return conflict with expected/received headers; stale epochs return forbidden with the current epoch header | Tested prototype |
| HTTP delete stream path | `delete_stream_removes_http_visible_state` | `DELETE /{bucket}/{stream}` returns no-content and subsequent HEAD/POST return not found | Tested |
| HTTP long-poll path | `long_poll_times_out_with_no_content_and_cleans_waiter`, `long_poll_returns_append_from_owner_waiter` | `GET ?offset=now&live=long-poll` returns `204` with cursor on timeout, wakes with appended payload, and leaves no live waiter leak | Tested |
| HTTP SSE path | `sse_live_tail_delivers_appended_text_and_closed_control`, `notify_read_watchers_shares_identical_reads_across_watchers` | `GET ?offset=now&live=sse` streams appended text events and emits closed control when the owner group closes. Runtime waiter notification now groups identical `ReadStreamRequest` values, performs one owner-group read per distinct request, and broadcasts the result instead of rerunning one read per subscriber. | Tested |
| Group-owned live-tail watcher state | `GroupActor::read_watchers`, `GroupCommand::WaitRead`, `GroupCommand::CancelWaitRead`, `cancel_read_watcher_removes_group_local_waiter`, `canceled_wait_read_stream_removes_owner_waiter`, `live_read_waiter_limit_rejects_excess_waiters_on_owner_core` | Live-tail waiter registration, cancellation, and notification are now group-actor-local state. The core worker only routes wait/cancel commands to the owner group, removing the prior shared `Arc<Mutex<HashMap<...>>>` watcher registry and its cross-task mutation boundary. | Tested |
| Read-plan / cold-payload split | `GroupEngine::read_stream_parts`, `GroupReadStreamParts`, `GroupReadStreamBody`, `InMemoryGroupEngine::read_stream_plan_after_access`, `InMemoryGroupEngine::read_payload_from_plan`, `RaftGroupEngine::read_stream_parts`, `runtime_read_uses_group_read_parts_fast_path`, `read_materialization_is_bounded_without_blocking_group_actor`, `notify_read_watchers_shares_identical_reads_across_watchers`, `raft_group_engine_cold_admission_coalesces_append_batch_many_into_one_raft_entry`, `static_grpc_raft_durable_cold_flush_replicates_manifest` | Runtime reads now ask the group engine for response parts first. Engines can return a `StreamReadPlan` plus cold-store handle; the group actor records planning time and spawns payload materialization separately, so cold S3/fs range reads no longer occupy the owner group actor turn. Materialization is bounded by a runtime-wide semaphore currently sized from `RuntimeConfig::mailbox_capacity`, so offloaded cold reads cannot grow unbounded while the actor stays available for later commands. Ordinary reads, long-poll wait admission, and live-tail watcher notification all use this parts path, so SSE fan-out can share one read plan per distinct request without doing object IO on the actor. OpenRaft leader reads compute the stream read plan inside `with_state_machine` and materialize hot/cold payload bytes after the state-machine access returns, so cold reads no longer hold OpenRaft's state-machine mutex either. Existing cold-read and replicated cold-manifest tests continue to pass. | Tested structural split |
| HTTP metrics endpoint | `GET /__ursula/metrics`, `metrics_expose_per_core_and_group_append_distribution`, `sse_live_tail_delivers_appended_text_and_closed_control` | HTTP exposes append, mutation, routing, mailbox send-wait, live-read-waiter, mailbox depth/capacity snapshots, group-mailbox depth/max-depth/full-event snapshots, and HTTP-layer SSE counters for opened streams, read-loop iterations, rendered data events, rendered control events, and rendered error events. The SSE counters are diagnostic only and intentionally live outside the runtime state machine. | Tested |
| Zero-copy HTTP/runtime payload path | `parse_append_batch`, `AppendBatchRequest`, `AppendRequest`, `CreateStreamRequest`, `PublishSnapshotRequest`, `StreamStateMachine::append_borrowed`, `append_batch_parser_returns_body_slices` | HTTP batch frames are represented as `Bytes` slices of the request body and the in-memory benchmark path appends from borrowed bytes instead of allocating one `Vec<u8>` per parsed frame. Single append, create initial payload, and snapshot publish requests now also carry `Bytes` through the HTTP/runtime command boundary before materializing only at the stream state-machine or protobuf boundary. | Tested prototype |
| HTTP append-batch ack hot path | `render_batch_results`, `append_batch_matches_perf_compare_frame_format`, `append_batch_minimal_ack_skips_success_body_but_keeps_item_errors` | HTTP renders batch ack JSON directly from runtime results, fast-paths all-success JSON responses without an intermediate status vector, supports requested no-body success acks, and still reports per-item failures | Tested prototype |
| In-memory append-batch hot path | `InMemoryGroupEngine::append_batch`, `RuntimeMetricsInner::record_append_batch`, `append_batch_reports_item_errors_without_stopping_later_payloads` | Batch frames are applied directly inside the group engine rather than by recursively calling the boxed single-append future, and successful batch metrics are aggregated while preserving per-item error behavior | Tested prototype |
| Raw HTTP ingress diagnostic | `ursula-http-raw` | Diagnostic HTTP/1 server implements only the `perf_compare` create and append-batch subset over the same `ShardRuntime`, bypassing axum/hyper routing and response rendering for ingress ceiling checks | Diagnostic implemented |
| `perf_compare` write/small smoke | `cargo run -p perf-compare --bin perf_compare -- --targets ursula --phases write,small ...` | Existing `perf_compare` Ursula target drove `ursula-http` write and small-event batch append phases with zero errors | Smoke tested |
| Release `perf_compare` write/small smoke | `cargo run --release -p perf-compare --bin perf_compare -- --targets ursula --phases write,small --concurrency 256 --throughput-secs 5 ...` | Release `ursula-http` with 10 cores and 160 groups completed write/small batch phases with zero errors; server metrics reported 10 active cores and 160 active groups | Smoke tested |
| CPU-sampled release `perf_compare` | release `ursula-http` plus release `perf_compare` write/small runs at concurrency 256, 1024, and 4096 | Throughput reached up to 1.13M logical appends/s and all configured cores/groups were active, but server CPU still stayed far below 10 cores | Not met |
| EC2 CPU-saturation check | `c7gn.8xlarge` client, three `c7g.4xlarge` servers, release `ursula-http --raft-memory`, release `perf_compare`, `ursula-raft-runtime-stress`, `ursula-http-raw`, `perf record` | Direct OpenRaft runtime stress on one `c7g.4xlarge` reached 1,598% CPU, proving the host/runtime can saturate 16 vCPUs; one `perf_compare` process remained around 2.5-3.1 server cores per target, three processes against one server pushed it to 746.7% average active CPU and 967.0% peak with no group-mailbox backlog, and five processes reached 1,513.0% peak but with status-0 errors and mailbox-full events; raw HTTP kept the same single-process throughput class; client `perf` showed per-request reqwest/HTTP allocation, URL/header construction, and scheduling as the first limiter | Not met |
| EC2 cold-enabled durable-log `perf_compare` | `docs/migration/cold-path-progress.md`, `docs/migration/perf-compare-cpu-saturation.md`, binary sha256 `50ad58c2ba3da5a6e8230322ef0cac05efad8fc7f5fda928c11f9e8685e9d33b` | A three-node `c7g.4xlarge` static gRPC cluster with independent `--raft-log-dir` roots and real S3 cold storage first exposed a structural cold-planning bug: 512-stream 128-byte append-batch load filled the 4 MiB per-group hot cap, returned 503s, and produced `cold_flush_uploads=0` because `plan_cold_flush` only considered the first 128-byte hot segment. After planner-side contiguous hot-segment coalescing, the same 30s EC2 run completed with zero errors: write phase 2,219,776 ok requests at 73,492.38 req/s, small-event phase 1,064,240 ok requests at 35,135.73 req/s, node 1 `accepted_appends=3284016`, `active_cores=16`, `active_groups=64`, `cold_backpressure_events=0`, `cold_flush_uploads=4919`, `cold_flush_upload_bytes=395159552`, `cold_hot_bytes=25194496`, and no mailbox-full events. S3 held 4,919 chunks totaling 395,159,552 bytes before cleanup. | Cold-enabled write path met for this 30s run; CPU saturation still not met |
| EC2 static-cluster ops helper | `scripts/ursula_ec2.py`, `docs/operations/ec2-static-cluster.md`, `just ec2-start`, `just ec2-stop`, `just ec2-status`, `just ec2-upload-server`, `just ec2-upload-client`, `just ec2-perf-many` | The one-off EC2 benchmark deployment loop is now a small manifest-driven helper: it injects short-lived EC2 Instance Connect SSH keys, uploads binaries to servers or client, starts/stops one Ursula process per configured server, waits for per-group Raft leadership, prints process plus metrics summaries, runs a configured `perf_compare` client with a disjoint Ursula bucket, runs concurrent `perf_compare` processes with generated disjoint buckets through `perf-many`, and cleans an explicit S3 cold-root prefix. The docs include the manifest shape, prerequisites, examples, and the cleanup safety note that stop uses pid files instead of broad `pkill` patterns. A real no-service smoke against the migration EC2 hosts verified EIC SSH to the three server nodes, `status` reporting not-running nodes without starting Ursula, and `upload-binary --target client` copying, executing, and removing a small client-host executable. | Implemented helper; syntax/help, example parse, and EC2 SSH/SCP smoke checked |
| `perf_compare` read smoke | `cargo run -p perf-compare --bin perf_compare -- --targets ursula --phases read ...` | Existing `perf_compare` Ursula target drove catch-up reads against `ursula-http` with zero errors | Smoke tested |
| `perf_compare` SSE smoke | `cargo run -p perf-compare --bin perf_compare -- --targets ursula --phases sse ...` | Existing `perf_compare` Ursula target delivered SSE live-tail events to multiple readers with zero errors | Smoke tested |
| `perf_compare` mixed smoke | `cargo run -p perf-compare --bin perf_compare -- --targets ursula --phases mixed ...` | Existing `perf_compare` Ursula target drove concurrent append, catch-up read, and SSE live-tail work with zero errors | Smoke tested |
| `perf_compare` latency smoke | `cargo run -p perf-compare --bin perf_compare -- --targets ursula --phases latency ...` | Existing `perf_compare` Ursula target completed append/read latency probes with zero errors | Smoke tested |
| Thread-per-core distribution | `thread_per_core_runtime_reaches_all_configured_cores` | Synthetic appends through per-core worker threads reach every configured core | Tested |
| Group engine placement | `custom_group_engine_is_created_once_per_touched_group_on_owner_core` | Custom factory observes per-group engine construction and validates core ownership | Tested |
| Group engine failures | `group_engine_errors_include_group_context_and_do_not_record_success_metrics` | Runtime reports engine errors with core/group context and does not count failed appends as accepted | Tested |
| Direct runtime stress harness | `ursula-runtime-stress` | Release binary bypasses HTTP and drives `ShardRuntime` directly in append or batch mode with configurable cores, groups, producers, streams, and duration | Diagnostic implemented |
| Runtime/WAL producer dedup path | `producer_duplicate_append_returns_prior_offsets_without_mutating_metrics`, `producer_duplicate_append_batch_returns_prior_offsets_without_mutating_metrics`, `wal_group_engine_recovers_producer_dedup_state`, `wal_group_engine_recovers_producer_append_batch_dedup_state` | Duplicate producer retries return prior offsets without incrementing append or mutation metrics, batch retries return the stored per-item offsets, and WAL replay restores producer dedup state so retries after restart are not appended twice | Tested prototype |
| Stream semantic invariants | `ursula-stream` unit tests | 24 tests cover explicit bucket creation, idempotent stream create, content-type mismatch, catch-up read bounds, close idempotency, append-after-close rejection, stream seq monotonicity, producer dedup/fencing, append-batch producer dedup, producer snapshot restore, producer duplicate final append after close, protocol snapshot publish/retention/bootstrap planning, snapshot offset alignment, visible snapshot restore, non-empty bucket deletion rejection, deterministic snapshot ordering, serde snapshot roundtrip, malformed snapshot rejection, cold-prefix flush/read planning, deterministic cold-flush candidate selection, soft-deleted stream skipping during cold candidate selection, hot-byte accounting, TTL sliding-window expiry, absolute Expires-At expiry/recreate behavior, and fork-ref soft-delete/release | Tested |
| CPU saturation gate | `docs/migration/perf-compare-cpu-saturation.md` | Defines `perf_compare` command shape, acceptance criteria, and required metrics | Documented |
| Current plateau analysis | `docs/migration/current-cpu-plateau-analysis.md` | Identifies likely single-Raft/shared-state bottleneck candidates in `riverrun` | Documented |
| Monoio caution | `docs/architecture/runtime-ecosystem-evaluation.md` | Records OpenRaft monoio support and axum/tonic ecosystem concerns | Documented |

## Verification Commands

Last verified from `/Users/xing/Idea/ursula`:

```bash
cargo fmt --all -- --check
cargo check --workspace --all-targets
cargo test --workspace --all-targets
cargo clippy --workspace --all-targets -- -D warnings
```

Results:

- `cargo fmt --all -- --check`: passed.
- `cargo check --workspace --all-targets`: passed.
- `cargo test --workspace --all-targets`: passed after the shared
  `ursula-proto` app-log schema work and serde boundary cleanup. The run
  covered 128 unit/integration tests across `ursula-http`, `ursula-raft`,
  `ursula-runtime`, `ursula-shard`, and `ursula-stream`; doc tests had no
  runnable tests.
- `cargo clippy --workspace --all-targets -- -D warnings`: passed.
- Focused cold-path runtime tests:
  `cargo test -p ursula-runtime --lib cold -- --nocapture` passed.
- Focused steady-state cold-path runtime test:
  `cargo test -p ursula-runtime
  repeated_cold_flush_keeps_hot_bytes_bounded_while_writes_continue --
  --nocapture` passed.
- Focused cold write-admission runtime tests:
  `cargo test -p ursula-runtime --lib cold_write_admission -- --nocapture`
  passed.
- Gated S3 integration test:
  `cargo test -p ursula-runtime --test s3_cold_path -- --nocapture` passed in
  local skip mode when no AWS/S3 environment variables were configured.
- Real S3 integration test:
  `URSULA_COLD_S3_INTEGRATION=1 URSULA_COLD_BACKEND=s3
  URSULA_COLD_S3_BUCKET=riverrun-e2e-us-east-1
  URSULA_COLD_S3_REGION=us-east-1
  cargo test -p ursula-runtime --test s3_cold_path -- --nocapture` passed
  locally with temporary exported AWS SSO credentials and passed on
  `ursula-c7g-beast-node-1` (`c7g.4xlarge`, aarch64) after syncing the current
  Ursula checkout to `/tmp/ursula-vnext-s3`.
- EC2 static multi-group gRPC S3 smoke:
  `docs/migration/ec2-static-cluster-s3-smoke.md` records a current transport
  three-server `c7g.4xlarge` plus one-client `c7gn.8xlarge` run on port
  `4477`, using tonic gRPC internal Raft transport, `--raft-init-membership`,
  and `URSULA_COLD_BACKEND=s3`. The smoke verified follower write redirect,
  leader commit through gRPC quorum replication, redirected readback, five S3
  cold chunks, `cold_flush_uploads=5`, `cold_flush_publishes=5`,
  `cold_hot_bytes=0`, and post-flush S3-backed readback. Temporary `4477`
  processes and smoke S3 objects were cleaned up.
- Focused cold-path HTTP tests:
  `metrics_expose_per_core_and_group_append_distribution` and
  `flush_cold_endpoint_uploads_and_reads_back_segments` passed.
- Focused HTTP cold backpressure test:
  `cold_backpressure_returns_service_unavailable_and_metrics` passed.
- Focused live-tail backpressure tests:
  `cargo test -p ursula-runtime
  live_read_waiter_limit_rejects_excess_waiters_on_owner_core -- --nocapture`
  and `cargo test -p ursula-http
  long_poll_returns_service_unavailable_when_live_waiters_are_full --
  --nocapture` passed. The HTTP metrics regression test
  `metrics_expose_per_core_and_group_append_distribution` also passed after
  adding live-read backpressure counters.
- Static Raft cluster config parser tests:
  `cargo test -p ursula-http --bin ursula-http raft_cluster_config --
  --nocapture` and `cargo test -p ursula-http --bin ursula-http
  rejects_conflicting_raft_node_id_from_config_file -- --nocapture` passed.
- Static gRPC durable-log wiring tests:
  `cargo test -p ursula-http static_grpc_raft_runtime_can_use_core_journal
  -- --nocapture`, `cargo test -p ursula-http
  static_grpc_raft_runtime_recovers_from_core_journal_after_restart --
  --nocapture`, `cargo test -p ursula-http
  static_grpc_raft_group_engine_replicates_with_core_journals --
  --nocapture`, `cargo test -p ursula-http
  static_grpc_raft_durable_cold_flush_replicates_manifest -- --nocapture`,
  `cargo test -p ursula-http
  static_grpc_raft_installs_snapshot_for_late_learner_with_core_journals --
  --nocapture`, `cargo test -p ursula-http --test static_cluster_cli
  cli_static_grpc_raft_log_dir_recovers_after_restart --
  --nocapture`, `cargo test -p ursula-http --test static_cluster_cli
  cli_static_grpc_raft_log_dir_recovers_cold_manifest_after_restart --
  --nocapture`, `cargo test -p ursula-http --test static_cluster_cli
  cli_static_grpc_raft_log_dir_replicates_between_nodes --
  --nocapture`, `cargo test -p ursula-http --test static_cluster_cli
  cli_static_grpc_raft_log_dir_replicates_cold_manifest -- --nocapture`, and
  `cargo test -p ursula-http --test static_cluster_cli
  cli_static_grpc_raft_log_dir_recovers_replicated_cold_manifest_after_restart
  -- --nocapture` passed, covering a three-process durable cold-manifest
  cluster restart from independent node journals and a shared cold root.
  `cargo test -p ursula-http --test static_cluster_cli
  cli_static_grpc_raft_log_dir_installs_snapshot_for_late_learner --
  --nocapture` passed, covering a two-node real-binary durable cluster,
  HTTP-triggered leader snapshot and purge, late third-process learner startup,
  HTTP add-learner, learner metrics proving snapshot installation, and
  post-catchup readback.
  `eval "$(aws configure export-credentials --format env)" &&
  URSULA_COLD_S3_INTEGRATION=1
  URSULA_COLD_S3_BUCKET=ursula-c7g-beast-us-east-1
  URSULA_COLD_S3_REGION=us-east-1 AWS_REGION=us-east-1
  AWS_DEFAULT_REGION=us-east-1 cargo test -p ursula-http --test
  static_cluster_cli
  cli_static_grpc_raft_log_dir_recovers_replicated_s3_cold_manifest_after_restart
  -- --nocapture` passed locally, covering the same three-process durable-log
  restart path with real S3 cold chunks and cleanup under a unique cold root.
  `cargo test -p ursula-http --bin ursula-http
  parses_static_grpc_raft_cluster_with_durable_log_dir -- --nocapture` passed.
  The binary cold-manifest tests now explicitly flush until `cold_hot_bytes`
  reaches zero before readback/restart, so those checks are not relying on a
  residual hot suffix to mask cold-manifest recovery bugs.
  `cargo test -p ursula-http --test static_cluster_cli
  cli_static_grpc_raft_log_dir_background_cold_flush_bounds_hot_bytes_during_writes
  -- --nocapture` passed, covering a three-process durable-log static gRPC
  cluster with shared fs cold storage, background cold flush enabled, repeated
  writes while `cold_hot_bytes` is required to drain, full payload readback from
  a follower, cluster restart without membership reinitialization, and
  post-restart cold-backed follower readback.
- Official Durable Streams conformance against `ursula-http --raft-memory`:
  `300 passed / 300` after the cold write-admission changes.
- Official Durable Streams conformance against `ursula-http --raft-memory` with
  `URSULA_COLD_BACKEND=memory`, `URSULA_COLD_FLUSH_MIN_HOT_BYTES=1`, and
  `URSULA_COLD_FLUSH_MAX_BYTES=1`: `300 passed / 300`; server log check found
  no `cold flush worker error`, `StreamGone`, or `StreamNotFound`.
- Current-code official Durable Streams conformance after the tonic gRPC
  transport switch and cross-segment cold flush fix: `300 passed / 300` with
  `URSULA_COLD_BACKEND=memory`, `URSULA_COLD_FLUSH_INTERVAL_MS=1`,
  `URSULA_COLD_FLUSH_MIN_HOT_BYTES=1`, `URSULA_COLD_FLUSH_MAX_BYTES=1`, and
  post-run metrics `cold_flush_uploads=15034`,
  `cold_flush_publishes=15034`.
- Current-code official Durable Streams conformance after the shared
  `ursula-proto` app-log schema work and serde boundary cleanup:
  `300 passed / 300` against official checkout `8d78524`, with
  `URSULA_COLD_BACKEND=memory`, `URSULA_COLD_FLUSH_INTERVAL_MS=1`,
  `URSULA_COLD_FLUSH_MIN_HOT_BYTES=1`, `URSULA_COLD_FLUSH_MAX_BYTES=1`, and
  post-run metrics `cold_flush_uploads=11732`,
  `cold_flush_publishes=11731`, `cold_orphan_cleanup_attempts=1`, and
  `cold_orphan_cleanup_errors=0`.
- Current-code official Durable Streams conformance after cold-admission Raft
  proposal coalescing, stale cold-flush candidate cleanup, and logical Raft
  write metrics: `300 passed / 300` in 16.86s against official checkout
  `8d78524`, with `URSULA_COLD_BACKEND=memory`,
  `URSULA_COLD_FLUSH_INTERVAL_MS=1`, `URSULA_COLD_FLUSH_MIN_HOT_BYTES=1`,
  `URSULA_COLD_FLUSH_MAX_BYTES=1`, and post-run metrics
  `raft_write_many_commands=2279`,
  `raft_write_many_logical_commands=112468`,
  `cold_flush_uploads=112468`, `cold_flush_publishes=112447`,
  `cold_orphan_cleanup_attempts=21`, `cold_orphan_cleanup_errors=0`,
  `cold_backpressure_events=0`, and `mailbox_full_events=0`.
- Current-code official Durable Streams conformance after the read-plan /
  cold-payload split and bounded read materialization semaphore: `300 passed /
  300` in 17.79s against official checkout `8d78524`, with
  `URSULA_COLD_BACKEND=memory`, aggressive 1-byte background flush,
  `accepted_appends=1050`, `applied_mutations=64618`,
  `raft_write_many_commands=1513`,
  `raft_write_many_logical_commands=62784`,
  `cold_flush_uploads=62784`, `cold_flush_publishes=62780`,
  `cold_orphan_cleanup_attempts=4`, `cold_orphan_cleanup_errors=0`,
  `cold_backpressure_events=0`, `mailbox_full_events=0`, and
  `group_mailbox_full_events=0`.
- Current workspace regression after the same read materialization change:
  `cargo test --workspace --all-targets` passed. Coverage included HTTP
  snapshot/bootstrap routes, static gRPC replication, static gRPC durable-log
  restart, static gRPC cold-manifest replication and restart, OpenRaft
  lagging-learner snapshot install, runtime group-owned live-tail waiters,
  bounded read materialization, repeated cold flush bounded-hot-byte behavior,
  producer idempotency, and stream snapshot/restore invariants. The first full
  `static_cluster_cli` run had one readiness-timeout flake in
  `cli_static_grpc_raft_log_dir_replicates_between_nodes`; rerunning that
  exact test passed, and the subsequent full workspace test also passed.
- Current lint/format gate after the same change: `cargo fmt --all --
  --check` and `cargo clippy --workspace --all-targets -- -D warnings`
  passed.
- Current-code official Durable Streams conformance against durable OpenRaft
  file-log path:
  `300 passed / 300` against official checkout `8d78524` using
  `ursula-http --raft-log-dir /tmp/ursula-conformance-raft-log-noflush`,
  `URSULA_COLD_BACKEND=memory`, and `URSULA_COLD_FLUSH_INTERVAL_MS=0`.
  Duration was 34.60s. Post-run metrics included `wal_batches=3836`,
  `wal_records=3836`, `wal_write_ns=295426143`, `wal_sync_ns=14342304938`,
  `cold_flush_uploads=0`, and `cold_hot_bytes=112159`.
- Current-code official Durable Streams conformance against durable OpenRaft
  file-log path plus 1-byte aggressive background cold flush:
  after group-local cold flush planning and metadata publish through one Raft
  `Batch`, `300 passed / 300` in 47.02s. Post-run metrics included
  `wal_batches=9072`, `wal_records=9072`, `wal_write_ns=10729099756`,
  `wal_sync_ns=34999994990`, `raft_write_many_batches=2557`,
  `cold_flush_uploads=105214`, and `cold_flush_publishes=105146`.
- Current-code official Durable Streams conformance after adding the narrow
  Raft admin endpoints for snapshot/purge/add-learner validation:
  `300 passed / 300` in 47.44s against official checkout `8d78524`, using
  `ursula-http --raft-log-dir /tmp/ursula-current-conformance-raft-log`,
  `URSULA_COLD_BACKEND=memory`, `URSULA_COLD_FLUSH_INTERVAL_MS=1`,
  `URSULA_COLD_FLUSH_MIN_HOT_BYTES=1`, and `URSULA_COLD_FLUSH_MAX_BYTES=1`.
  Post-run metrics included `accepted_appends=1079`,
  `applied_mutations=114614`, `active_cores=4`, `active_groups=32`,
  `wal_batches=9308`, `wal_records=9308`, `wal_write_ns=11017214493`,
  `wal_sync_ns=36202483019`, `cold_flush_uploads=112751`,
  `cold_flush_publishes=112747`, `cold_hot_bytes=4`, and
  `cold_backpressure_events=0`.
- Local three-process static gRPC official Durable Streams conformance against
  the shard-owned multi-Raft path with in-memory OpenRaft logs:
  `300 passed / 300` in 17.85s. The cluster used node 1 as the initialized
  voter, nodes 2 and 3 as peers, `--raft-memory`, 32 Raft groups, 4 runtime
  cores, `URSULA_COLD_BACKEND=memory`, and `URSULA_COLD_FLUSH_INTERVAL_MS=0`.
  Post-run node 1 metrics included `accepted_appends=1036`,
  `applied_mutations=1824`, `mutation_apply_ns=1446717215`,
  `group_engine_exec_ns=1581203498`, `wal_batches=0`, `wal_records=0`, and
  `cold_flush_publishes=0`.
- The same local three-process static gRPC in-memory Raft-log shape also passed
  with the background cold worker enabled:
  `300 passed / 300` in 18.77s using `URSULA_COLD_FLUSH_INTERVAL_MS=1`,
  `URSULA_COLD_FLUSH_MIN_HOT_BYTES=1`, `URSULA_COLD_FLUSH_MAX_BYTES=1024`, and
  `URSULA_COLD_FLUSH_MAX_CONCURRENCY=4`. Post-run node 1 metrics included
  `accepted_appends=1100`, `applied_mutations=3319`,
  `raft_write_many_batches=899`, `cold_flush_uploads=1431`,
  `cold_flush_publishes=1431`, `cold_flush_publish_ns=1148259172`,
  `cold_hot_bytes=0`, and `wal_batches=0`.
- Local three-process static gRPC with durable OpenRaft file logs is
  protocol-correct but too slow for several official property tests' default
  5s per-test timeout on this MacBook. With 1-byte cold flush, the full suite
  reached `295 passed / 300`; node 1 metrics showed
  `cold_flush_publishes=109538`, `cold_flush_publish_ns=110437098133`, and
  `wal_sync_ns=69546911040`, with one group receiving about 99k mutations. With
  1024-byte cold flush, failures dropped to `298 passed / 300` and cold publish
  count dropped to `1367`; node 1 still showed `wal_sync_ns=37249623877`. With
  cold worker disabled, only `offsets are always monotonically increasing`
  timed out in the full suite, while that test plus `replay produces identical
  content hash` passed when run alone. This points to local durable file-log
  fsync/replication latency, not a protocol-visible semantic mismatch.
- TCP smoke test against `ursula-http`: `PUT` returned `201`, `POST` returned
  `204`, and `GET ?offset=2&max_bytes=3` returned `cde`.
- TCP append-batch smoke test against `ursula-http`: `POST /append-batch`
  returned `[{"status":204},{"status":204}]`, and readback returned `abcde`.
- TCP metrics smoke test against `ursula-http`: after two batch appends,
  `GET /__ursula/metrics` returned `accepted_appends: 2`, `active_cores: 1`,
  `active_groups: 1`, per-core counts `[2,0]`, one active Raft group count,
  and per-core mailbox depth/capacity arrays.
- TCP metrics smoke test after routing metric addition: after one create and
  one append, `GET /__ursula/metrics` returned `accepted_appends: 1`,
  `routed_requests: 2`, `per_core_routed_requests`, `mailbox_send_wait_ns`, and
  `per_core_mailbox_send_wait_ns`. Router tests also cover
  `mailbox_full_events` and `per_core_mailbox_full_events`.
- TCP mutation metrics smoke test: after one create and one append,
  `GET /__ursula/metrics` returned `applied_mutations: 2`,
  `per_core_applied_mutations`, `per_group_applied_mutations`,
  `mutation_apply_ns`, `per_core_mutation_apply_ns`, and
  `per_group_mutation_apply_ns`.
- HTTP metrics include WAL fields: default in-memory metrics report
  `wal_batches: 0`, `wal_records: 0`, `wal_write_ns: 0`, `wal_sync_ns: 0`, and
  per-core/per-group WAL arrays. The WAL HTTP recovery test reports
  `wal_batches: 2`, `wal_records: 3`, `wal_write_ns`, and `wal_sync_ns` after
  create plus append-batch.
- Runtime group snapshot install test: snapshot from one runtime installed into
  another runtime through `ShardRuntime::install_group_snapshot`, readback
  returned the installed payload, and the next append continued from the
  restored offset with `stream_append_count: 3`.
- Snapshot placement validation test: a snapshot whose recorded core did not
  match the configured owner for its Raft group returned
  `SnapshotPlacementMismatch` before any mailbox routing.
- WAL snapshot install test: a WAL-backed runtime installed a group snapshot,
  restarted from the same WAL directory, read back the installed payload, and
  the next append preserved the restored append count.
- Stream producer tests: `Producer-Id`, `Producer-Epoch`, and `Producer-Seq`
  state deduplicates duplicate retries, rejects sequence gaps, fences stale
  epochs, survives stream snapshot restore, and treats the duplicate final
  append that closed a stream as an idempotent success.
- Runtime producer test: duplicate producer append returns the prior offsets
  with `deduplicated: true`, does not append retry bytes, and does not increment
  accepted append or applied mutation metrics.
- WAL producer test: after restart from per-group WAL, retrying an already
  accepted producer tuple is deduplicated and the next contiguous producer
  sequence appends at the correct offset.
- Append-batch producer tests: stream/runtime/HTTP tests now verify batch-level
  producer retries return stored per-item offsets without appending retry bytes
  or incrementing append/mutation metrics; WAL recovery preserves the same batch
  dedup state and skips duplicate batch WAL records.
- TCP metrics smoke test after mailbox-full metric addition: empty runtime
  metrics returned `mailbox_full_events: 0` and
  `per_core_mailbox_full_events`.
- TCP WAL recovery smoke test against `ursula-http --wal-dir`: create returned
  `201`, append returned `204`, server restart with the same WAL directory
  succeeded, and `GET ?offset=0&max_bytes=32` returned body `wal-payload` with
  status `200`.
- Router WAL recovery test now exercises `POST /append-batch` against
  `spawn_wal_runtime`: append-batch returns `200`, restart with the same WAL
  directory succeeds, and readback returns `persisted-batch`.
- Router OpenRaft file-log test now exercises create, append, and read against
  `spawn_raft_runtime`: readback returns `raft-payload`, and the owning group
  writes through an OpenRaft core journal under `core-0/journal.bin`; the same
  test verifies non-zero durable-log write/sync metrics are exposed through
  `GET /__ursula/metrics`.
- Runtime OpenRaft file-log metrics test: `DurableRaftGroupEngineFactory`
  records non-zero durable-log batches, records, write nanoseconds, and sync
  nanoseconds into the per-core/per-group metrics after create plus append.
- Runtime OpenRaft core-journal recovery test:
  `DurableRaftGroupEngineFactory` writes group records through the owner core's
  `journal.bin`, restarts from the same root, and recovers stream payload plus
  offsets from the core journal.
- Runtime group dispatch test: while one group engine is blocked inside create,
  a command for another group owned by the same core still completes, proving
  the core mailbox loop now dispatches group work instead of awaiting one group
  command at a time.
- TCP long-poll smoke test against `ursula-http`: timeout returned `204` with
  `stream-next-offset`, `stream-up-to-date`, and `stream-cursor`; wake-up after
  append returned `200` with body `wake`; metrics reported `live_read_waiters: 0`
  after both paths.
- TCP stream-seq smoke test against `ursula-http`: append with `stream-seq:
  0002` returned `204`, repeated `0002` returned `409 StreamSeqConflict`,
  append with `0003` returned `204`, and readback returned `ac`.
- Stream/HTTP append error-precedence tests:
  `append_conflict_precedence_reports_closed_before_mismatch_or_seq` and
  `append_conflict_precedence_reports_closed_header_before_mismatch_or_seq`
  verify that a closed stream reports `StreamClosed` before content-type
  mismatch or stream-seq regression. The HTTP response returns `409` with
  `Stream-Closed: true` and the closed stream's next offset.
- HTTP producer-header router test: duplicate `Producer-Id`/`Producer-Epoch`/
  `Producer-Seq` retry returned `204` with the original next offset and did not
  append retry bytes; success responses echoed `Producer-Epoch` and
  `Producer-Seq`; a producer sequence gap returned `409` with
  `Producer-Expected-Seq` and `Producer-Received-Seq`; a stale epoch returned
  `403` with the current `Producer-Epoch`; append-batch with producer headers
  returns batch acks for the original per-item offsets and does not append retry
  bytes.
- TCP delete smoke test against `ursula-http`: create returned `201`, append
  returned `204`, delete returned `204`, subsequent HEAD returned `404`, and
  append-after-delete returned `404 StreamNotFound`.
- Local `perf_compare` smoke against `ursula-http` with `--core-count 4` and
  `--raft-group-count 64`: write phase accepted 83,308 appends with zero
  errors; small-event phase accepted 81,340 appends with zero errors; server
  metrics after the run reported 164,648 accepted appends, 4 active cores, 30
  active Raft groups, and empty mailboxes.
- Release local `perf_compare` write/small smoke against `ursula-http` with
  `--core-count 10`, `--raft-group-count 160`, `--concurrency 256`,
  `--throughput-secs 5`, batch appends of 16 frames, and zero benchmark errors:
  write phase completed 4,356,800 requests at 870,826.84 req/s; small-event
  phase completed 4,152,880 requests at 830,097.4 req/s. Server metrics after
  the run reported 8,509,680 accepted appends, 8,510,192 applied mutations, 10
  active cores, 160 active Raft groups, `mailbox_full_events: 0`, and empty
  mailboxes. This is distribution and throughput evidence, not CPU saturation
  proof, because CPU utilization was not captured during the run.
- CPU-sampled release `perf_compare` write/small run against `ursula-http` with
  `--core-count 10`, `--raft-group-count 160`, `--concurrency 256`,
  `--throughput-secs 10`, and batch appends of 16 frames: write phase completed
  8,305,488 requests at 830,280.37 req/s; small-event phase completed
  8,664,208 requests at 866,320.82 req/s. Server CPU samples above 100%
  averaged 476.5% CPU and peaked at 541.0%, or about 4.77 average cores and
  5.41 peak cores. Server metrics after the run reported 16,969,696 accepted
  appends, 16,970,208 applied mutations, 10 active cores, 160 active Raft
  groups, `mailbox_full_events: 0`, and empty mailboxes.
- CPU-sampled release `perf_compare` write/small run against `ursula-http` with
  the same server shape and `--concurrency 1024`, `--throughput-secs 10`, and
  batch appends of 16 frames: write phase completed 9,005,968 requests at
  900,088.08 req/s; small-event phase completed 8,400,544 requests at
  838,319.01 req/s. Server CPU samples above 100% averaged 478.1% CPU and
  peaked at 572.3%, or about 4.78 average cores and 5.72 peak cores. This is
  not CPU saturation; it shows the current vNext prototype still plateaus well
  below the configured 10 shard cores despite balanced placement.
- After moving HTTP append-batch to a single shard-owned runtime command per
  HTTP batch, release `perf_compare` at `--concurrency 1024`,
  `--throughput-secs 10`, and batch appends of 16 frames completed 10,913,344
  write requests at 1,091,086.74 req/s and 11,253,056 small-event requests at
  1,125,166.59 req/s with zero errors. Server metrics reported 22,166,400
  accepted appends, 22,168,448 applied mutations, 10 active cores, 160 active
  groups, `mailbox_full_events: 0`, empty mailboxes, and 1,387,448 routed
  runtime requests. Server CPU samples above 100% averaged 325.8% CPU and
  peaked at 341.6%, or about 3.26 average cores and 3.42 peak cores. This
  improves logical append throughput by removing per-frame mailbox/oneshot
  overhead, but it moves further away from CPU saturation.
- After changing the in-memory group engine to apply batch frames directly
  instead of recursively calling the boxed single-append future, and after
  aggregating batch metrics updates, targeted runtime tests verified both normal
  batch routing and mixed success/error frame behavior. A release
  `perf_compare` small-event run at `--concurrency 1024`,
  `--throughput-secs 15`, and batch size 16 completed 16,652,256 logical appends
  at 1,110,037.25 appends/s with zero errors. Server metrics reported 10 active
  cores, 154 active groups, 1,041,790 routed runtime requests,
  `mailbox_full_events: 0`, and empty mailboxes. Server CPU averaged 296.6% and
  peaked at 327.0%, or about 2.97 average cores and 3.27 peak cores. This
  preserves correctness and reduces known hot-path overhead, but it does not
  change the CPU-saturation conclusion.
- After changing the HTTP batch ack renderer to avoid an intermediate status
  vector and fast-path all-204 responses, release `perf_compare` small-event at
  `--concurrency 1024`, `--throughput-secs 10`, and batch size 16 completed
  11,252,960 logical appends at 1,125,134.01 appends/s with zero errors. Server
  metrics reported 10 active cores, 156 active groups, 704,334 routed runtime
  requests, `mailbox_full_events: 0`, and empty mailboxes. Server CPU averaged
  288.7% and peaked at 323.4%, or about 2.89 average cores and 3.23 peak cores.
  This also does not change the CPU-saturation conclusion.
- After changing HTTP batch parsing to return `Bytes` slices and adding
  `StreamStateMachine::append_borrowed` for the in-memory benchmark path, release
  `perf_compare` small-event at `--concurrency 1024`, `--throughput-secs 10`,
  and batch size 16 completed 11,126,048 logical appends at 1,112,423.64
  appends/s with zero errors. Server metrics reported 10 active cores, 160 active
  groups, 696,402 routed runtime requests, `mailbox_full_events: 0`, and empty
  mailboxes. Server CPU averaged 275.5% and peaked at 307.7%, or about 2.76
  average cores and 3.08 peak cores. This removes parser-side per-frame payload
  copies from the in-memory path, but still does not change the CPU-saturation
  conclusion.
- A minimal-ack control run added `Prefer: return=minimal` for Ursula
  append-batch and taught `perf_compare` to treat `204 No Content` as all frames
  accepted. Release `perf_compare` small-event at `--concurrency 1024`,
  `--throughput-secs 10`, and batch size 16 completed 10,978,224 logical appends
  at 1,097,653.11 appends/s with zero errors. Server metrics reported 10 active
  cores and 160 active groups. Server CPU averaged 278.1% and peaked at 314.5%,
  or about 2.78 average cores and 3.15 peak cores. Removing the successful batch
  response body and client JSON decode does not change the CPU-saturation
  conclusion.
- A raw HTTP/1 ingress control used `ursula-http-raw` with the same
  `ShardRuntime`, `Prefer: return=minimal`, `--concurrency 1024`,
  `--throughput-secs 10`, and batch size 16. It completed 10,861,408 logical
  appends at 1,085,967.92 appends/s with zero errors. Server CPU averaged
  207.3% and peaked at 242.2%, or about 2.07 average cores and 2.42 peak cores.
  Bypassing axum/hyper routing did not increase `perf_compare` request rate; it
  reduced server-side work per request, which points back to ingress/harness
  pressure rather than axum response overhead.
- A `perf_compare` pipelining control added `--ursula-append-pipeline-depth` to
  keep multiple append requests in flight per stream slot. Depth 2 at
  `--concurrency 1024`, `--throughput-secs 10`, batch size 16, and minimal ack
  was not valid saturation evidence: it completed 2,422,896 successful logical
  appends but also reported 71,760 status-0/client errors, with server CPU
  averaging 49.7% and peaking at 110.8%. Depth 4 was worse: 320,960 successful
  logical appends, 94,976 status-0/client errors, and server CPU averaging 7.4%.
  Adding in-flight client pressure through reqwest triggers client/OS failures
  before it makes the server busy.
- A post-mode control run after the same change at `--concurrency 1024` reached
  79,851.19 write req/s and 81,038.93 small-event req/s with zero errors.
  Server CPU samples averaged 292.3% CPU and peaked at 310.3%, or about 2.92
  average cores and 3.10 peak cores.
- A higher-concurrency batch run at `--concurrency 4096` is not acceptance
  evidence: the write phase reported 424,976 status-0/client errors and only
  13,196.38 req/s over 22.857 seconds. The small-event phase still reached
  1,094,909.46 req/s with zero errors, while server CPU samples averaged 318.8%
  CPU and peaked at 341.3%, or about 3.19 average cores and 3.41 peak cores.
- Multi-process `perf_compare` control: four concurrent client processes at
  `--concurrency 1024` were invalid because they produced large status-0 error
  counts, stream-creation timeouts, and one `Can't assign requested address`
  client error. Four concurrent client processes at `--concurrency 256`
  completed the small-event batch phase with zero errors and reached about
  2.07M aggregate logical appends/s; server CPU samples averaged 431.6% CPU and
  peaked at 454.8%, or about 4.32 average cores and 4.55 peak cores. Eight
  concurrent client processes at `--concurrency 256` also completed with zero
  errors, but fell to about 1.96M aggregate logical appends/s; server CPU
  averaged 421.8% and peaked at 460.5%, or about 4.22 average cores and 4.61
  peak cores. This shows same-machine `perf_compare`/HTTP ingress currently
  plateaus near 120k-130k HTTP batch requests/s.
- Larger public batch control: single-process `perf_compare` with
  `--concurrency 1024`, `--throughput-secs 15`, and
  `--ursula-append-batch-size 64` completed the small-event phase with zero
  errors at 2,983,891.9 logical appends/s. Server metrics reported 44,773,632
  accepted appends, 44,774,656 applied mutations, 10 active cores, 160 active
  groups, 700,612 routed runtime requests, `mailbox_full_events: 0`, and empty
  mailboxes. Server CPU averaged 353.6% and peaked at 373.5%, or about 3.54
  average cores and 3.73 peak cores. This confirms larger batches increase
  logical append throughput but do not by themselves satisfy the CPU gate.
- WAL-backed release `perf_compare` smoke after adding the group-level
  append-batch WAL path: `ursula-http --wal-dir` with `--core-count 10` and
  `--raft-group-count 160`, small-event phase, `--concurrency 256`,
  `--throughput-secs 5`, and batch size 16 completed 16,624 logical appends at
  2,502.87 appends/s with zero errors. Server metrics reported 16,624 accepted
  appends, 16,880 applied mutations, 10 active cores, 116 active groups,
  1,295 routed runtime requests, `mailbox_full_events: 0`, and empty mailboxes.
  Server CPU averaged 372.5% and peaked at 399.4%, or about 3.73 average cores
  and 3.99 peak cores. The WAL directory contained 116 group log files and used
  about 6.0 MiB. This is durability smoke evidence, not CPU saturation proof.
- OpenRaft-backed release `perf_compare` smoke after adding `--raft-log-dir`
  and file-log metrics: `ursula-http --raft-log-dir` with `--core-count 10`
  and `--raft-group-count 160`, small-event phase, `--concurrency 256`,
  `--throughput-secs 5`, batch size 16, and minimal append-batch acks completed
  10,592 logical appends at 1,303.68 appends/s with zero errors. Server metrics
  reported 10 active cores, 116 active groups, 918 routed runtime requests,
  2,416 durable-log batches, 2,416 durable-log records, about 106.5s aggregate
  file-log write time, and about 13.6s aggregate file-log sync time. Server CPU
  averaged 295.7% above the 100% sample cutoff and peaked at 337.6%, or about
  2.96 average cores and 3.38 peak cores. This shows the current OpenRaft
  file-log path is storage-wait dominated and still does not satisfy the CPU
  saturation gate.
- OpenRaft-backed release `perf_compare` smoke after changing the core worker
  to dispatch per-group commands and moving blocking file-log writes to Tokio's
  blocking pool: the same server and benchmark shape completed 9,904 logical
  appends at 1,246.8 appends/s with zero errors. Server metrics reported 10
  active cores, 128 active groups, 875 routed runtime requests, 2,390 durable-log
  batches, 2,390 durable-log records, about 950.4s aggregate file-log write-path
  time, and about 22.6s aggregate file-log sync time. Server CPU averaged
  357.9% above the 100% sample cutoff and peaked at 590.9%, or about 3.58
  average cores and 5.91 peak cores. This validates the dispatch boundary but
  still does not satisfy the CPU gate; the durable-log format and commit path
  remain the next bottleneck.
- OpenRaft-backed release `perf_compare` smoke after adding the per-core
  OpenRaft journal writer: the same server and benchmark shape completed
  10,672 logical appends at 1,475.4 appends/s with zero errors. Server metrics
  reported 10 active cores, 132 active groups, 923 routed runtime requests,
  2,506 durable-log batches, 2,506 durable-log records, about 95.3s aggregate
  file-log write-path time, and about 5.3s aggregate file-log sync time. Server
  CPU averaged 499.0% and peaked at 803.5%, or about 4.99 average cores and
  8.04 peak cores. This reduces sync time and raises burst CPU utilization, but
  still does not satisfy the CPU saturation gate.
- OpenRaft-backed release `perf_compare` smoke after removing diagnostic
  per-group file writes from the durable runtime path: the same server and
  benchmark shape completed 16,416 logical appends at 2,616.82 appends/s with
  zero errors. Server metrics reported 10 active cores, 140 active groups,
  1,282 routed runtime requests, 3,264 durable-log batches, 3,264 durable-log
  records, about 85.7s aggregate file-log write-path time, and about 6.3s
  aggregate file-log sync time. The OpenRaft log directory contained only 10
  core journal files. This confirms the previous diagnostic per-group files were
  duplicated hot-path I/O.
- OpenRaft-backed release `perf_compare` smoke after changing the per-core
  journal from JSON lines to length-prefixed MessagePack records: the same
  server and benchmark shape completed 78,880 logical appends at 14,666.97
  appends/s with zero errors. Server metrics reported 10 active cores, 132
  active groups, 5,186 routed runtime requests, 11,032 durable-log batches,
  11,032 durable-log records, about 44.8s aggregate file-log write-path time,
  and about 26.4s aggregate file-log sync time. Average durable-log write time
  per batch fell to about 4.1ms. CPU samples averaged only 24.6% and peaked at
  32.0%, so this is a durable-log serialization improvement, not CPU saturation
  evidence.
- OpenRaft in-memory release `perf_compare` smoke after adding
  `ursula-http --raft-memory`: release `ursula-http` with `--core-count 4` and
  `--raft-group-count 32`, release `perf_compare` write/small phases at
  `--concurrency 32`, `--throughput-secs 2`, batch size 8, and minimal ack
  completed 1,067,800 write appends at 533,850.59 appends/s and 1,072,368
  small-event appends at 536,127.53 appends/s with zero errors. Server metrics
  reported 4 active cores, 28 active groups, `mailbox_full_events: 0`, empty
  mailboxes, and `wal_batches`, `wal_records`, `wal_write_ns`, and
  `wal_sync_ns` all equal to zero. This is diskless OpenRaft wiring evidence,
  not CPU saturation proof.
- OpenRaft in-memory release `perf_compare` control after changing
  `RaftGroupEngine::write_batch` to use OpenRaft `client_write_many`: targeted
  `cargo test -p ursula-raft -p ursula-runtime` passed, and release
  `ursula-http --raft-memory` with `--core-count 10`, `--raft-group-count 160`,
  `perf_compare --phases small`, `--concurrency 1024`, `--throughput-secs 20`,
  batch size 16, and minimal ack completed 17,540,080 logical appends at
  876,842.57 appends/s with zero errors. Server metrics reported 10 active
  cores, 160 active groups, 1,097,279 routed runtime requests, no mailbox-full
  events, empty mailboxes, and zero disk-WAL metrics. Server CPU averaged
  291.0% and peaked at 533.7%. A same-shape pure in-memory engine control
  completed 20,526,960 logical appends at 1,026,262.07 appends/s with zero
  errors, zero disk-WAL metrics, and server CPU averaging 182.0% with a 285.2%
  peak. This confirms the current CPU plateau is not caused by local disk WAL.
- Tokio-console attribution for the same OpenRaft in-memory shape: rebuilding
  release `ursula-http` with `--features tokio-console` and
  `RUSTFLAGS="--cfg tokio_unstable"`, then running `perf_compare --phases small`
  at concurrency 1024 for 10s, completed 8,434,336 logical appends at
  841,830.63 appends/s with zero errors. Console task view showed about 810
  tasks, only a small running set during load, shard-runtime `block_on` tasks
  mostly idle with scheduler delay near zero, and resource view dominated by
  OpenRaft timers plus long-lived oneshot sender/receiver resources. This
  points away from local disk WAL and Tokio ready-queue starvation; the next
  structural measurement should split HTTP ingress pressure from OpenRaft
  `client_write_many` submit, response wait, and state-machine apply time.
- OpenRaft write-stage metrics were added for `raft_write_many_*` and
  `raft_apply_*`, and `RaftGroupEngine::append_batch` was changed so even a
  single public append-batch request routes through `write_commands(vec![...])`
  instead of bypassing `client_write_many`. Targeted
  `cargo test -p ursula-raft -p ursula-runtime -p ursula-http` passed. A
  release `--raft-memory` `perf_compare --phases small` run at concurrency 1024
  for 10s completed 9,232,496 logical appends at 922,988.66 appends/s with zero
  errors. Metrics reported 578,055 routed requests, 577,031
  `raft_write_many_commands`, 87.10s `raft_write_many_response_ns`, 0.66s
  `raft_write_many_submit_ns`, 3.98s `raft_apply_ns`, 88.25s
  `group_engine_exec_ns`, `group_lock_wait_ns: 0`, `mailbox_full_events: 0`,
  and zero disk-WAL metrics. This is direct evidence that the current plateau is
  dominated by waiting for OpenRaft proposal responses, not shard lock
  contention.
- A cooperative-yield coalescing experiment at the group actor append-batch
  boundary raised average commands per `client_write_many` from about 1.20 to
  about 1.36 and reduced aggregate response-wait time, but throughput stayed
  effectively flat at 920,451.03 appends/s. It was not kept because it is a
  latency tradeoff rather than a structural throughput fix.
- A detached per-group write experiment was also tried and not kept. The group
  actor submitted multiple in-flight append-batch proposals through cloned
  OpenRaft handles before waiting for completions. One client at concurrency
  1024 stayed near the same throughput at 961,502.52 appends/s with CPU
  averaging 439.8%. Four clients at concurrency 256 each raised CPU to 530.9%
  average and 603.2% peak, but per-client throughput fell to about 364k-369k
  appends/s, below the previous non-detached 4-client result. Metrics still
  showed `raft_write_many_response_ns` dominating `group_engine_exec_ns`, while
  `raft_apply_ns` remained small and disk-WAL metrics stayed zero. This
  falsifies "more detached `client_write_many` futures per group" as the
  structural fix.
- A one-in-flight detached variant with an append-only buffer behind the active
  write was also tried to preserve FIFO ordering across reads, closes, deletes,
  and snapshots. One client at concurrency 1024 completed 9,266,416 logical
  appends at 926,418.10 appends/s, with CPU averaging 419.2%; response-wait time
  grew to about 132.9s while apply time stayed about 4.2s. This variant was
  removed too.
- Direct release `ursula-runtime-stress` batch run bypassing HTTP with
  `--core-count 10`, `--raft-group-count 160`, `--stream-count 8192`,
  `--producer-count 2048`, `--batch-size 16`, `--payload-bytes 128`, and
  `--duration-secs 10`: completed 74,938,448 accepted appends at 7,353,566.27
  appends/s, 4,691,845 routed runtime requests at 460,401.76 routed req/s, 10
  active cores, 160 active groups, and `mailbox_full_events: 0`. Whole-process
  CPU samples above 100% averaged 781.8% CPU and peaked at 818.9%, or about
  7.82 average cores and 8.19 peak cores. This is diagnostic evidence only,
  because it bypasses `perf_compare` and HTTP.
- Direct release `ursula-runtime-stress` append-mode run with the same
  placement shape, `--producer-count 4096`, `--payload-bytes 128`, and
  `--duration-secs 10`: completed 7,489,217 accepted appends at 740,136.32
  appends/s, 7,497,409 routed runtime requests at 740,945.92 routed req/s, 10
  active cores, 160 active groups, and `mailbox_full_events: 0`. Whole-process
  CPU samples above 100% averaged 832.4% CPU and peaked at 849.2%, or about
  8.32 average cores and 8.49 peak cores. This suggests the current
  `perf_compare` HTTP path is not applying enough server-side pressure to hit
  the runtime's own CPU ceiling.
- Group-mailbox instrumentation now exposes `group_mailbox_depth`,
  `per_group_group_mailbox_depth`, `group_mailbox_max_depth`, and
  `per_group_group_mailbox_max_depth` from `GET /__ursula/metrics`. A release
  OpenRaft in-memory HTTP run with one `perf_compare` client at concurrency
  1024 completed 9,429,488 logical appends in 10 seconds with zero errors,
  server CPU averaging about 347.5%, core mailbox depth sum peaking at 834,
  group mailbox depth sum peaking at 133, and max per-group depth 11. Two
  concurrent clients completed with zero errors but reduced total throughput to
  5,955,328 logical appends, CPU averaging about 261.9%, and max per-group group
  mailbox depth 20. These runs do not support a sustained unbounded
  `core -> group` backlog as the current CPU plateau root cause.
- Direct release `ursula-raft-runtime-stress` now bypasses HTTP while keeping
  `RaftGroupEngineFactory`. With 10 cores, 160 groups, 4096 streams, 4096
  producers, batch size 16, 100-byte payloads, and 10 seconds, it completed
  46,914,512 logical appends at 4,529,831.68 appends/s and 2,936,253 routed
  runtime requests at 283,509.97 routed req/s. Whole-process CPU averaged about
  631.0% and peaked at 788.5%, with all cores/groups active,
  `mailbox_full_events: 0`, and `group_mailbox_max_depth: 45`. This stronger
  OpenRaft control points the `perf_compare` plateau back at HTTP/client ingress
  request rate and per-request HTTP overhead, not S3 or local WAL.
- A 5-second macOS `sample` during a 15-second release HTTP OpenRaft in-memory
  `perf_compare` run showed active CPU spread across hyper/axum HTTP/1 parsing,
  body collection, socket read/write, allocation/free, time/hash/wakeup
  overhead, and OpenRaft watch/engine-command notification. The sample did not
  show S3, local disk WAL, or a sustained group-mailbox backlog as the root
  cause.
- Local `perf_compare` read smoke against `ursula-http`: read phase completed
  26,954 successful catch-up reads with zero errors; server metrics after setup
  reported 16 accepted seed appends, 4 active cores, 16 active Raft groups, and
  empty mailboxes.
- Local `perf_compare` SSE smoke against `ursula-http`: SSE phase delivered 20
  events to 2 readers with zero errors.
- Local `perf_compare` mixed smoke against `ursula-http`: mixed phase completed
  60,284 successful appends, 19,017 successful reads, and 20 SSE deliveries
  with zero errors; server metrics after the run reported 60,302 accepted
  appends, 4 active cores, 17 active Raft groups, and empty mailboxes.
- Local `perf_compare` latency smoke against `ursula-http`: 20 append probes and
  20 read probes completed with zero errors.

## Missing Work

The final objective is not complete. The base Durable Streams conformance gate
and the real S3 cold-path gate now pass, but the prompt-to-artifact audit still
has uncovered requirements.

Major missing pieces:

- Production multi-node Raft transport and membership management. Current
  OpenRaft evidence now includes a real in-process three-node replication probe,
  owner-core group warmup, a runtime-owned Raft handle registry, and an internal
  gRPC transport for Vote/AppendEntries/full-snapshot that has replicated four
  groups across three local TCP routers. The binary-level static cluster launch
  path now accepts a JSON `--raft-cluster-config` file for node id, peer URLs,
  and initial-membership intent, while still supporting explicit CLI flags. The
  current gRPC transport has passed a three-node EC2 S3 smoke with follower
  redirects, quorum-committed leader writes, background S3 flush, and
  post-flush S3-backed readback. A follow-up EC2 smoke with the current binary
  also covered independent durable OpenRaft log roots plus real S3 cold
  storage, full three-node process restart without reinitializing membership,
  and follower readback of the cold-backed stream. A later EC2 smoke started a
  two-voter durable-log cluster with real S3 cold storage, wrote and cold-flushed
  a stream, snapshotted and purged the leader, started a third node from an
  empty durable log root, added it as a learner, observed node 3 install
  snapshot index 4, and read the restored cold-backed stream through node 3.
  Local durable static gRPC
  coverage now also
  includes replicated cold manifests over independent node journals, a real
  binary static-cluster cold-manifest smoke with `URSULA_COLD_BACKEND=fs`,
  binary restart recovery of a cold-backed stream from the same durable log
  directory and cold root, and a gated local real-S3 version of the
  three-process durable-log restart path. A 30-second cold-enabled EC2
  `perf_compare` run then exposed and fixed a stream cold-planning bug for
  append-batch small events; the rerun completed write and small-event phases
  with zero errors while uploading 395,159,552 bytes to S3 and keeping hot
  bytes below the per-group cap. A later per-group leader run added
  `--raft-init-membership-per-group` and HTTP-adapter internal gRPC forwarding
  for public writes that land on a follower; targeted official sub-runs now pass
  for `Protocol Edge Cases|SSE Mode`, `State Hash Verification`, and
  `Stream Closure` under distributed leaders. The subsequent root cause was
  follower-local state-machine preflight before Raft writes: append, close,
  publish snapshot, append batch, and cold-admission variants could return
  `StreamNotFound` on a stale follower before the full command reached the
  owning group leader. The group engine now forwards complete write commands to
  the current group leader before these preflights when the local node is a
  follower. With that fix, the official upstream suite passes under local
  three-process per-group distributed leaders both with cold flush disabled and
  with the memory cold backend plus aggressive background flush enabled
  (`300/300` in both runs). The current release binary
  `cd4c005ce8106423a1239280e8de45114d6ec2f0b4c8e985825b29b63f113982`
  was also deployed to the three EC2 `c7g.4xlarge` nodes on port `4489`
  with `--raft-memory`, 64 groups, per-group distributed leaders, and
  `URSULA_COLD_BACKEND=s3`; the official suite from the `c7gn.8xlarge`
  client passed `300/300` in 22.91s. A follow-up cold-flush proof on that
  fresh S3-enabled cluster wrote a 3 MiB stream, successfully flushed/read
  S3-backed data, and observed 19 temporary S3 objects totaling 29,360,128
  bytes before cleanup. A subsequent 45-second EC2 distributed-leader
  `--raft-memory` plus S3 `perf_compare` run completed write, small, read,
  mixed append, and mixed read phases with zero errors while uploading about
  1.8 GiB to S3 and recording no cold backpressure or mailbox-full events, but
  exposed a mixed SSE live-tail bug: only 6 of the expected 800 mixed SSE
  deliveries arrived. The root cause was live waiter registration on follower
  runtimes. A follower could forward the initial read to the group leader and
  register a local waiter, but later appends applied on the leader and only
  woke leader-local watchers. Live reads now preflight local live-read
  ownership before sending SSE headers or entering long-poll; the OpenRaft
  group engine redirects follower live reads to the group leader, while
  ordinary catch-up reads still use gRPC read forwarding. The focused
  `static_grpc_raft_group_engine_replicates_between_routers` test verifies
  follower catch-up read success and follower `live=sse` leader redirect.
  A local three-process `perf_compare --phases mixed` run with read bases
  spread across all three nodes then delivered all 120 expected SSE events with
  zero mixed append/read/SSE errors and no residual live waiters. The fixed
  Linux aarch64 binary
  `be9950e4579cf676cfd386ae3640d205af2a7a829f72e5da57e5cff7d249216e` was then
  deployed to the EC2 three-server shape on port `4490` with
  `--raft-memory`, per-group distributed leaders, and real S3 cold storage.
  Manual client probes verified that follower `live=sse` returns a leader 307
  and that `curl -N -L` receives data after append. Isolated EC2 heavy mixed
  load passed with 2,168,512 mixed appends, 823,972 mixed reads, and all 800
  expected SSE deliveries at zero errors. A 15-second `write,mixed` matrix also
  delivered 400/400 SSE events with zero errors. Full `write,small,mixed,read`
  and `small,mixed` runs still produced no mixed SSE deliveries after a
  preceding sustained write phase, despite no mailbox-full, live-read
  backpressure, cold backpressure, or residual live waiters. This narrows the
  remaining issue to a phase-interaction or cold-backlog/client-state problem
  after sustained write pressure, not the original follower-local waiter bug.
  HTTP-layer SSE diagnostics were added so the next EC2 run can distinguish
  no SSE route entry, unpolled response bodies, live wait/read starvation, and
  client-side delivery/parsing failure: `sse_streams_opened`,
  `sse_read_iterations`, `sse_data_events`, `sse_control_events`, and
  `sse_error_events`. Focused local tests verify the new counters are exposed
  at zero for non-SSE metrics and increment for one completed SSE live-tail.
  Those counters then isolated the remaining full-phase failure: node 2 opened
  8 mixed SSE streams and rendered 8 initial control events but 0 data events,
  proving that the server-side route and response body were active while the
  leader-local watcher registry was not being woken. The structural root cause
  was group-level write forwarding from follower `RaftGroupEngine` instances:
  follower writes could be applied on the group leader through the
  Raft-internal `group_write` path while bypassing the leader runtime command
  path that owns `notify_read_watchers()`. Follower group writes now return a
  leader hint instead of forwarding internally, so the existing HTTP
  write-forward path sends the whole write request to the leader runtime. The
  fixed Linux aarch64 binary
  `57d54d56d1628e66e4fd4a1da10736d18f6129eafba67b8eb64d5cf4d8c6ecac` was
  deployed to the EC2 three-server shape on port `4491` with `--raft-memory`,
  per-group distributed leaders, and real S3 cold storage. The previously
  failing full `write,small,mixed,read` run then passed: write 4,524,368
  requests, small 2,815,408 requests, mixed append 2,335,552 requests, mixed
  read 290,281 requests, read 1,680,589 requests, all with zero errors, and
  mixed SSE delivered 800/800 events with zero errors and p99 61.89ms. Final
  metrics showed the SSE leader node rendered `sse_data_events=800` and had
  `accepted_appends=3,572,525`, while all three nodes had active cores/groups
  and no mailbox-full, live-read backpressure, or cold backpressure events.
  The focused `static_grpc_raft_group_engine_replicates_between_routers`
  regression test now opens SSE on the leader and appends through a follower
  HTTP endpoint, requiring the leader SSE body to receive the appended token.
  This pins the leader-runtime write/watcher ownership invariant locally.
  `cargo check -p ursula-raft --all-targets`,
  `cargo clippy -p ursula-raft --all-targets -- -D warnings`, and
  `cargo test -p ursula-raft --all-targets` also passed after removing
  group-level write forwarding from the Raft adapter.
  A full local workspace regression also passed:
  `cargo check --workspace --all-targets`,
  `cargo test --workspace --all-targets`, and
  `cargo clippy --workspace --all-targets -- -D warnings`.
  A follow-up EC2 full workload with `pidstat` server telemetry used the same
  three-server `--raft-memory` plus S3 shape and passed again with zero errors:
  write 4,280,704 requests, small 3,171,840 requests, mixed append 2,150,544
  requests, mixed read 252,371 requests, read 1,676,981 requests, and mixed
  SSE 800/800 events. Server process CPU averaged 810.568%, 562.568%, and
  805.386% on nodes 1, 2, and 3, with peaks of 1400%, 1217%, and 1318%.
  This clears the old three-to-four-core plateau but is still not full
  three-node CPU saturation. Final metrics showed all nodes had 16 active
  cores, distributed active groups, no mailbox-full events, no live-read
  backpressure, and no cold backpressure. Temporary EC2 services and the S3
  root were cleaned up.
  A cleaner `4492` EC2 rerun then fixed the test environment itself: node 1 and
  node 3 were missing the `riverrun-e2e-node` IAM instance profile, so their
  first S3 background flush attempts failed while node 2 uploaded normally. The
  instance profile was attached to all three servers, `aws sts
  get-caller-identity` was verified on each node, the S3 root was cleared, and
  the three-node `--raft-memory` plus S3 cluster was restarted with leaders
  still distributed 22/21/21. The full single-process `perf_compare`
  `write,small,mixed,read` workload at concurrency 256 and batch size 16 then
  passed with zero errors: write 10,668,496 requests at 355,023.17/s, small
  8,593,392 at 284,264.87/s, read 1,651,221 at 54,747.94/s, mixed append
  4,935,504 at 164,516.80/s, mixed read 361,517 at 12,050.57/s, and mixed SSE
  800/800 events with p99 21.51ms. All three nodes uploaded to S3
  (`1,219,737,600`, `957,751,296`, and `901,881,856` bytes), ended with only
  about 5-6 MiB of hot bytes each, and reported zero cold backpressure,
  mailbox-full events, and WAL records. A higher write-pressure follow-up at
  concurrency 512 and pipeline depth 2 reduced throughput to 268,687.67/s for
  write and 238,034.48/s for small, so that parameter set is not a better
  throughput shape, but it drove server CPU to node1 average/peak
  1263.15%/1516.00%, node2 853.27%/1110.00%, and node3
  786.91%/944.00%. This removes the old "cannot get past 3-4 cores" finding
  for the `--raft-memory` plus S3 static-cluster shape, while still leaving the
  longer accepted-workload soak and production-grade client-harness saturation
  gate open.
  `cargo test -p ursula-http --all-targets` passed for lib/bin tests; one CLI
  static-cluster readiness flake passed on direct rerun. Missing pieces are
  dynamic/reconfigurable membership,
  longer cold-enabled soak/performance validation, and CPU saturation under
  the accepted workload.
- Production configuration for one durable `RaftGroupEngine` per owned group
  now exists for the local/static gRPC shape through `--raft-log-dir`, and has
  a short multi-node EC2 durable-log/S3 restart smoke plus a local
  background-cold-flush steady-state restart regression. It still needs longer
  EC2 S3 soak/performance validation before being treated as a production
  storage layout.
- Indexed/high-throughput production OpenRaft log format, compaction, group
  commit, and recovery for multiple Raft groups.
- Production storage scheduling and group commit that keep every owned group
  moving while one group waits for durable-log I/O.
- Extend cold-enabled `perf_compare` beyond the 30-second write/small run into
  a longer soak and mixed/read workload, because S3 offload changes memory
  pressure, read planning, background IO, live-tail owner placement, and
  overload behavior.
- Elimination or proof of absence for global locks/queues/fsync paths under the
  cold-enabled and durable-log-enabled runtime shape.
- Monoio runtime adapter experiment and ecosystem decision record, if monoio is
  still being considered after the Tokio/OpenRaft path is structurally clean.

## Current Conclusion

The current state materially advances the final goal by establishing the
shard-owned group boundary, passing the official base Durable Streams
conformance suite locally and on EC2, proving real S3 cold-path
upload/readback/cleanup both locally and on EC2, and refreshing the EC2 static
multi-group smoke on the current tonic gRPC Raft transport with per-group
distributed leaders. Snapshot/bootstrap protocol extensions are now implemented
through the stream state-machine, group command, runtime, OpenRaft adapter, and
HTTP boundary, and local extension tests cover snapshot/bootstrap behavior
because the upstream official suite does not exercise those endpoints. It does
not satisfy the final goal yet because dynamic/reconfigurable membership is
still not implemented as deployment management, the cold-enabled durable-log/S3
path has only short correctness/performance smokes rather than a longer soak,
and the accepted workload still needs to demonstrate CPU saturation under the
intended EC2 shape. The previously unresolved write-pressure-to-mixed-SSE
interaction is fixed for the `--raft-memory` plus S3 static EC2 shape by
preserving the leader-runtime write ownership boundary. The first host CPU
telemetry run shows the workload now drives about 21.8 average server vCPU
across three nodes with per-node peaks above 12 vCPU, so the original
three-to-four-core plateau is gone. A follow-up client telemetry run on the
`c7gn.8xlarge` host showed the current single-process `perf_compare`
reqwest/HTTP harness is itself a limiter: the first write window used only about
2.6-3.1 client vCPU, later read/mixed windows dropped below one client vCPU on
average, and client `perf record` was dominated by `reqwest`/`hyper`/`bytes`
allocation, reference counting, header parsing, and tokio scheduling. That
narrowed the next CPU-saturation experiment to multiple independent client
processes with disjoint buckets. The first helper-driven EC2 `perf-many` smoke
with two concurrent clients, disjoint buckets, `--raft-memory`, per-group
leaders, and real S3 exited successfully for both clients and pushed the three
servers to roughly 4.3-5.2 vCPU each during a short write/small run. Scaling to
four client processes then exposed a server-side structural bottleneck in the
S3-enabled path: cold write admission bypassed the group actor's append-batch
coalescing, so the real S3 configuration was effectively issuing one OpenRaft
proposal per public append-batch. A CPU profile on node 1 was dominated by
internal tonic/hyper h2 Raft append handling and `rmp_serde`, matching that
proposal fanout. The runtime/Raft adapter now coalesces same-group append-batch
requests before cold admission and commits one outer Raft `Batch` command.
Under the same four-process EC2 write/small workload this raised aggregate write
throughput from 157,234.90 to 292,606.99 logical appends/s and small-event
throughput from 83,429.02 to 173,050.71 logical appends/s with zero errors,
zero mailbox-full events, and zero cold backpressure. Node 1's proxy ratio moved
from about 10.3 logical appends per OpenRaft write batch to about 114.8. This
removes the evidenced cold-admission proposal fanout bottleneck. The metrics now
also expose `raft_write_many_logical_commands`, which recursively expands
`GroupWriteCommand::Batch`, so future EC2 runs can measure inner command
coalescing directly instead of inferring it from accepted appends divided by
outer Raft batches. A later `4492` EC2 run corrected missing IAM instance
profiles on node 1 and node 3, proving that all three servers can upload to S3
and drain hot bytes under the full `write,small,mixed,read` workload. That
clean run reached 355k write events/s and 284k small-event/s with zero errors,
SSE p99 21.51ms, 3.08 GiB of aggregate S3 uploads, hot bytes down to about
5-6 MiB per node, no cold backpressure, and no mailbox-full events. A higher
write-pressure follow-up drove node 1 to 15.16 peak vCPU and node 2/node 3 to
11.10/9.44 peak vCPU, so the old 3-4 core CPU ceiling is no longer present in
the `--raft-memory` plus S3 static-cluster shape. A same-client-host reference
comparison against the official Durable Streams reference server and S2 Lite is
now recorded in `perf-compare-cpu-saturation.md`. That comparison is deliberately
not treated as durability-equivalent because the official reference server was
file-backed, S2 Lite used S3 object storage, and Ursula used three-node
`--raft-memory` with S3 cold storage. It is still useful as a harness sanity
check: Ursula's plain per-event HTTP append path reached about 14k writes/s and
13.7k small writes/s, while the Ursula append-batch extension reached about
296k writes/s and 242k small writes/s under the same client workload. The final
performance gate still needs a longer mixed/read/S3-offload soak and a
production-grade multi-process or lower-overhead client harness before CPU
saturation under the accepted workload is considered complete.

The latest docs-web benchmark refresh used that multi-process harness shape for
the session-event workload on the same three-node EC2 static cluster with
`--raft-memory` and S3 cold storage enabled. The `perf-many` helper now rotates
Ursula entrypoints across service nodes by default so multi-process clients do
not all enter through node 1. With clean cluster/S3 roots, c1024 reached
673,505.32 small-event writes/s, c2048 reached 673,734.78 writes/s, and c4096
reached 693,308.93 writes/s, all with zero errors. The earlier c2048 drop was traced to benchmark artifacts: the
388,413.21 req/s run was contaminated by previous cold/background state, and
the 507,389.20 req/s fresh run still used node 1 as the only ingress target,
causing node 1 to apply about twice as many mutations as the other nodes. With
rotated entrypoints, node mutations were balanced at about 1.86M/1.83M/1.76M,
with no mailbox or cold backpressure. The apparent c4096 cliff was the same
kind of charting artifact: c4096 had still been using an older
single-process/single-entrypoint result while c1024/c2048 had been refreshed.
The refreshed c4096 run balanced node mutations at about 1.90M/1.89M/1.88M and
showed no mailbox or cold backpressure. This fixes the benchmark client shape
for refreshed session-event points but does not close the final performance
gate; the next production-grade benchmark still needs longer
mixed/read/S3-offload soak and CPU telemetry at higher concurrency. The page data was updated in
`docs/web/src/pages/BenchmarkPage.tsx`, and the detailed run notes are recorded
in `docs/migration/perf-compare-cpu-saturation.md`.

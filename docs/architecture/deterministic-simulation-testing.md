# Deterministic Simulation Testing

## 1. Purpose and scope

Deterministic simulation testing (DST) runs Ursula's Raft, runtime, stream state machine, cold store and in-process HTTP surface inside [madsim](https://github.com/madsim-rs/madsim), where time, task scheduling, the Raft network, the disk and the cold store are all driven by one `u64` seed. A failing seed replays the same schedule, so a bug found by a sweep becomes a checked-in regression. Unit tests cover single components on real Tokio. End-to-end tests (`crates/ursula/tests/static_cluster_cli.rs`) cover real processes, gRPC and TCP. Chaos runs cover real hosts, real S3 and long-running operational drift. DST covers the interleavings those layers reach only by chance: message ordering against timers, crash and power-loss points, retry races and storage faults at exact steps.

## 2. Test layers

**Property tests.** `crates/ursula-stream/src/state_machine/tests.rs` runs [proptest](https://github.com/proptest-rs/proptest) over generated command sequences against `StreamStateMachine::apply`. Properties cover offset monotonicity and snapshot round trips, producer retry idempotence and stale-epoch fencing, TTL expiry under generated `now_ms`, cold flush planning and restore, and stale cold flushes after delete and recreate. These run in the normal unit-test job.

**Raft unit tests under madsim.** CI runs both `cargo test -p ursula-raft --lib` and `RUSTFLAGS="--cfg madsim" cargo test -p ursula-raft --lib`. The latter runs every test that compiles under `cfg(madsim)`, without a name filter. Native tests remain part of the regular workspace unit-test job.

| Fixture | Native coverage | Simulation coverage |
| --- | --- | --- |
| Journal storage contract | OpenRaft storage suite on a real temporary WAL: reopen, truncate, purge, vote and commit persistence | The same OpenRaft storage suite on `SimDisk`. `ursula-sim` disk/recovery tests exercise page loss, reordered writes, fsync failure and gated restart |
| Replication and linearizable reads | Real Tokio Raft actors, sockets and thread-per-core ownership | Raft unit strict append/read replay and partition/heal. Smoke corpus `snapshot_catch_up`, leader-failover and leader-read-linearizability families |
| Runtime-owned Raft and cold tier | Native factory, cold-index cache, compaction and object reference integration | Hosted runtime/Raft network, cold flush/restart and cold-write-failure smoke families |
| Snapshot reference publication and host concurrency | Native cancellation, publication retries, install permits, OS threads and real filesystem metadata | Snapshot codec, state-machine install, prefetch, reference pinning and publication failure/retry contracts run on SimDisk. OS-thread concurrency stays native |
| Batch hot-capacity reservation | Native factory and append batch | The same unapplied-entry capacity assertions on a SimDisk-backed engine |
| Control state and wire formats | Pure state machines, log stores and codec tests | The same pure tests and in-memory OpenRaft meta actor startup |

A fixture that opens `tempfile`/`std::fs`, real sockets or OS threads is marked `cfg(not(madsim))` together with its private helpers. Starting a Tokio runtime does not simulate those dependencies. Simulator fixtures explicitly enter `MadsimOpenRaftRuntime::scope` and provision `SimDisk` paths. Do not gate pure tests with an entire mixed module, or remove native assertions to make the simulated suite pass. The ignored strict-replay diagnostic subprobes stay opt-in. The full strict append/read replay runs by default.

**Multi-node simulator.** `crates/ursula-sim` runs three simulated nodes in one process against seeded fault schedules, checks named invariants and records replayable artifacts. Sections 4 to 8 describe it.

Directed failpoints (`fail_point!`) are not used. Every fault is injected through the simulator seams, and the `layer2` audit keeps the count at zero.

## 3. Determinism mechanisms

- **`cfg(madsim)`.** Every simulation build sets `RUSTFLAGS="--cfg madsim"`. Simulation-only code and the simulated disk compile only under it, and code that touches OS threads, sockets or real files is gated `cfg(not(madsim))`.
- **`rt` shim.** `crates/ursula-runtime/src/rt.rs` and `crates/ursula-raft/src/rt.rs` re-export `spawn`, `sync`, `time` (and in `ursula-raft` also `select` and `task`) from Tokio in normal builds and from `madsim-tokio` under `cfg(madsim)`. Project code goes through the shim so virtual time and scheduling reach every call site.
- **HostedTokio only.** `RuntimeThreading::ThreadPerCore` does not exist under `cfg(madsim)`, and `RuntimeConfig::new` defaults to `HostedTokio` there (`crates/ursula-runtime/src/runtime.rs:160-165`, `:204-209`). Core workers spawn as madsim tasks.
- **OpenRaft runtime.** `MadsimOpenRaftRuntime` (`crates/ursula-raft/src/sim_runtime.rs:40`) is OpenRaft's `DeterministicRng` wrapper over a madsim-backed `AsyncRuntime`. The harness runs every workload inside `MadsimOpenRaftRuntime::scope(seed, ...)` so OpenRaft's randomness, including election timeouts, derives from the seed.
- **Watch channel.** `MadsimWatch` (`sim_runtime.rs:310-320`) replaces `tokio::sync::watch` for OpenRaft. Tokio parks receivers on one of eight `Notify`s chosen by a thread-local RNG seeded from a process-wide counter. `MadsimWatch` parks every receiver on one `Notify`, which wakes them in the order they started waiting.
- **`select!` patch.** The workspace patches `futures-macro` with `third_party/futures-macro` (`Cargo.toml` `[patch.crates-io]`). Under `cfg(madsim)` the `select!` and `stream_select!` branch shuffle draws from a hash of madsim's simulated clock and a per-call-site counter instead of futures-util's thread-local RNG (`third_party/futures-macro/src/shuffle.rs`). The `sim-patches` audit checks that `Cargo.lock` builds the vendored copy at the same version as `futures-util`.
- **Strict replay.** `madsim::runtime::Runtime::check_determinism` runs a workload twice with one seed and fails at the first diverging draw of madsim's RNG. `strict_replay_tests.rs` applies it to a three-node cluster on the per-core journal: no fault, follower restart, leader failover, and segment rotation with purge and disk reads. Outcome-level replays cannot see such divergence.
- **Nondeterminism audit.** The `nondeterminism` audit scans sim-reachable source for `SystemTime::now`, `std::time::Instant`, Tokio runtime builders, `std::thread::spawn` and `std::thread::sleep`. Every finding must match an entry in `scripts/dst/nondeterminism_whitelist.json` with a reason, and stale entries fail the audit.

## 4. Harness architecture

```
seed (u64)
  |
  v
SimSchedule::generate / generate_*        scenario + SimFaultPlan
  |                                         steps: [SimFaultStep { phase, action: SimFaultAction }]
  v
ThreeNodeRaftSim::run_schedule            madsim Runtime::with_seed_and_config(seed), 30 s virtual limit
  |                                         MadsimOpenRaftRuntime::scope(seed, workload)
  v
+--------------------------------------------------------------------------+
| node 1, 2, 3                                                             |
|   Raft group engine or ShardRuntime (HostedTokio)                        |
|   journal  SimNodeWal -> per-core journal -> JournalDisk = SimDisk       |
|   network  InProcessRaftNetworkFactory + Policy + FaultScript            |
|   cold     ColdStore::memory + fault policy + delay fn                   |
|   http     router_with_http_state + injected WallClock                   |
+--------------------------------------------------------------------------+
  |
  v
SimTrace (SimEvent stream) -> invariant helpers -> SimReport { scenario, outcome }
```

`crates/ursula-sim/src/madsim_harness/` module map:

- `mod.rs`: `SimScenario`, `SimSchedule::generate`, seed range constants, `SimReport`, regression record types and the madsim runner.
- `generators.rs`: `SimSchedule::generate_*`, one per seed family.
- `faults_inner.rs`: `SimFaultPlan`, `SimFaultStep`, `SimFaultAction` and the seeded workload plans (`RuntimeInterleavingPlan`, `RuntimeRaftNetworkWorkloadPlan`, `HttpProtocolSurfacePlan`).
- `dispatch.rs`: routes a schedule to its scenario runner and wraps the result in a `SimReport`.
- `introspect.rs`: fault-plan predicates and trace helpers shared by dispatch and the minimizer.
- `raft_scenarios.rs`, `runtime_scenarios.rs`, `cold_path.rs`, `http.rs`, `leader_reads.rs`: scenario runners.
- `trace.rs`: `SimTrace` and `SimEvent`, the stable observation surface.
- `sim_wal.rs`, `recovery_wiring.rs`: the production journal and recovery gate wired onto simulated nodes.
- `disk_tests.rs`, `recovery_tests.rs`, `recovery_regression_tests.rs`, `poison_marker_tests.rs`, `rolling_restart.rs`, `strict_replay_tests.rs`, `tests.rs`: `#[test]` scenarios outside the seed-family CLI.

A schedule is pure data. `SimSchedule::generate(seed)` routes a few fixed seeds and seed ranges to dedicated scenarios and maps the remaining seeds onto Raft and cold-path scenarios by `seed % 12`. The `generate_*` functions build one family each. Seeded workload plans use `SplitMix64` so a seed fixes the plan before the run starts, and the plan is stored in the schedule.

Simulation seams:

- **Raft network.** `crates/ursula-raft/src/in_process.rs` (compiled for `cfg(any(test, madsim))`) implements OpenRaft's `RaftNetworkFactory`/`RaftNetworkV2` over `InProcessRaftRegistry`. `InProcessRaftNetworkPolicy` applies a virtual-time delay and directional partitions, and records each decision as an `InProcessRaftNetworkEvent`. `InProcessRaftFaultScript` applies `InProcessRaftFaultAction` (`SetDelay`, `PartitionBidirectional`, `HealBidirectional`, `Clear`) at named workload phases. Production uses `GrpcRaftNetworkFactory` in `grpc.rs`.
- **Disk.** The per-core journal performs every file operation through `JournalDisk` (`crates/ursula-raft/src/log_store/disk.rs`). Under `cfg(madsim)` it is `SimDisk` (`log_store/sim_disk.rs`), a path-keyed in-memory disk scoped to one madsim runtime that keeps the page cache apart from synced pages and directory entries. `SimDisk` also stands in for the host boot id, so a power loss starts a new boot. Under `cfg(madsim)` the journal rotates segments at 4 KiB and caches 1 KiB of entries per group (`log_store/writer.rs:145-158`), so scenarios rotate segments and read evicted entries from disk. No scenario uses an in-memory Raft log store.
- **Cold store.** `ColdStore::memory()` builds an OpenDAL memory operator through the private `ColdStore::from_operator`. `set_fault_policy` returns a `ColdStoreFaultEffect` (delay, error or `truncate_read_to`) per operation, and `set_delay_fn` routes delays through `madsim::time::sleep`. Observers emit `ColdStoreEvent`s into the raw event log.
- **Wall clock.** `ursula::WallClock` (`crates/ursula/src/lib.rs:268`) supplies protocol-visible `now_ms`. HTTP scenarios build the router with `HttpState::with_wall_clock` and advance a simulated clock for TTL and expiry.
- **Runtime.** Sim-only methods `shutdown_group_engine_for_simulation` and `install_group_engine_for_simulation` let a hosted `ShardRuntime` swap its group engine after an OpenRaft leader change.

## 5. Scenarios and seed families

`ursula-sim smoke --seed-family <name>` resolves a family from `SEED_FAMILIES` in `crates/ursula-sim/src/bin/ursula-sim/smoke.rs`. `--seed-range A..=B` runs `SimSchedule::generate` on each seed. Families prefixed `pipeline-smoke-` corrupt a harness expectation (`corrupt_*_expectation`) to prove the failure, minimize and replay pipeline end to end. Other `*-failures` families inject a real fault or omit a heal. Both run with `--expect-failures`. CI column: PR and nightly run the family by name, "range" means the nightly `60..=199` range reaches it through `SimSchedule::generate`, "manual" means no workflow runs it.

| Family | Seeds | Layer | Faults | Invariant | CI |
| --- | --- | --- | --- | --- | --- |
| `leader-read-linearizability` | 360-364 | HTTP + runtime + 3-node Raft | brief partition, leader isolation, failover | `leader_read_linearizable` | PR, nightly |
| `leader-read-linearizability-extended` | 700-799 | same | same | `leader_read_linearizable` | nightly |
| `producer-repeats-exactly-once` | 365-369 | HTTP + runtime + 3-node Raft | leader deposed with a write in flight, client timeouts, duplicated and delayed resends, clones | `producer_repeats_are_exactly_once` | PR, nightly |
| `producer-repeats-exactly-once-extended` | 800-849 | same | same | `producer_repeats_are_exactly_once` | nightly |
| `idle-expiry-cannot-reopen-an-epoch` | 370-374 | HTTP + runtime + 3-node Raft | 7-day idle expiry, `TidyStream`, leader kill | `idle_expiry_cannot_reopen_an_epoch` | PR, nightly |
| `history-rewind-under-leader-churn` | 375-379 | HTTP + runtime + 3-node Raft | leader killed before genesis, leader isolated or killed with an import in flight or just answered, repeated imports | `rewind_applies_exactly_once` | PR, nightly |
| `history-rewind-under-leader-churn-extended` | 850-879 | same | same | `rewind_applies_exactly_once` | nightly |
| `stale-writers-after-restore` | 380-384 | HTTP + runtime + 3-node Raft | producers of the source history resend after a restore, with the old token, no token and the new token, across leader kills | `stale_writers_never_apply_after_restore` | PR, nightly |
| `recovery-plan-cut-under-restarts` | 385-389 | journal-backed replicas + 3-node Raft (`JournalCluster`) | leader cut off with an uncommitted write, a quorum that lost power and every unsynced page, a recovery plan, which usually cuts since the cut-off leader's last log term is below the vote term the others kept, every member restarting in a seeded order | `accepted_loss_never_applies_a_stale_entry` | PR, nightly |
| `recovery-plan-cut-under-restarts-extended` | 880-899 | same | same | `accepted_loss_never_applies_a_stale_entry` | nightly |
| `recovery-plan-survivor-campaigns` | 390-394 | same | the same quorum loss, with the plan sent one replica at a time, the leader first, pausing one to four election timeouts before each while the cut-off leader, the lagging open survivor, campaigns | `recovery_plan_never_elects_an_unplanned_leader`, `accepted_loss_never_applies_a_stale_entry` | PR, nightly |
| `recovery-plan-survivor-campaigns-extended` | 900-919 | same | same | same | nightly |
| `runtime-interleaving` | 72-96 | runtime actors + cold store | seeded client delays, cold flush, cold-read delay | `runtime_interleaving_read_your_write` | PR, nightly |
| `runtime-raft-engine` | 97-101 | runtime + 1-node Raft | none | committed read | range |
| `runtime-raft-network` | 102-106 | runtime + 3-node Raft | none | `runtime_raft_network_read_your_write` | range |
| `runtime-raft-network-recovery` | 107-111 | runtime + 3-node Raft | partition, heal | `runtime_raft_network_follower_catchup` | manual |
| `runtime-raft-network-cold-live-recovery` | 112-116 | + cold store | partition, heal, cold flush | `runtime_raft_network_cold_live_read_integrity` | manual |
| `runtime-raft-network-cold-live-restart` | 117-121 | + cold store | partition, heal, follower stop/restart around flush | same | manual |
| `runtime-raft-network-cold-live-write-recovery` | 317-321 | + cold store | partition, heal, failed cold write, retry | `runtime_raft_network_cold_live_write_integrity` | PR, nightly |
| `leader-failover` | 122-126 | 3-node Raft | stop leader, restart old leader | `leader_failover_no_loss_or_dup` | range |
| `runtime-raft-network-leader-failover` | 127-131 | runtime + 3-node Raft | stop leader, engine swap, restart | `runtime_raft_network_leader_failover_no_loss_or_dup` | manual |
| `runtime-raft-snapshot-install` | 132-136 | runtime + 3-node Raft | snapshot into fresh runtime | `runtime_raft_snapshot_install_integrity` | range |
| `runtime-raft-network-randomized` | 137-156 | runtime + Raft + cold | seeded mix (below) | all `runtime_raft_network_*` | PR 137-140, range |
| `runtime-raft-network-randomized-extended` | 400-499 | same | same | same | nightly |
| `http-protocol-surface-randomized` | 277-296 | HTTP + runtime | seeded `HttpProtocolSurfacePlan` | `http_protocol_randomized_*` | PR, nightly |
| `pipeline-smoke-http-protocol-surface-randomized-corruption` | 297-301 | HTTP | final-read expectation | `http_protocol_randomized_read_your_write` | PR, nightly |
| `pipeline-smoke-http-protocol-surface-randomized-sse-corruption` | 302-306 | HTTP | SSE next-offset expectation | `http_protocol_randomized_sse_delivery` | PR, nightly |
| `pipeline-smoke-http-protocol-surface-randomized-backpressure-corruption` | 307-311 | HTTP | backpressure metric expectation | `http_protocol_randomized_live_waiter_backpressure` | PR, nightly |
| `pipeline-smoke-runtime-raft-network-randomized-read-corruption` | 242-246 | runtime + Raft | read expectation | `runtime_raft_network_read_your_write` | PR, nightly |
| `pipeline-smoke-runtime-raft-network-partial-read-corruption` | 247-251 | runtime + Raft | partial-read expectation | `runtime_raft_network_partial_read_integrity` | PR, nightly |
| `pipeline-smoke-runtime-raft-network-tail-read-corruption` | 337-341 | runtime + Raft | tail-read expectation | `runtime_raft_network_tail_read_empty` | PR, nightly |
| `pipeline-smoke-runtime-raft-network-close-state-corruption` | 342-346 | runtime + Raft | closed-flag expectation | `runtime_raft_network_close_state` | PR, nightly |
| `pipeline-smoke-runtime-raft-network-snapshot-corruption` | 347-351 | runtime + Raft | snapshot payload expectation | `runtime_raft_network_snapshot_publish_read` | PR, nightly |
| `pipeline-smoke-runtime-raft-network-leader-failover-read-corruption` | 252-256 | runtime + Raft | post-failover read expectation | `runtime_raft_network_leader_failover_no_loss_or_dup` | PR, nightly |
| `runtime-raft-network-leader-failover-cold-live-read-failures` | 327-331 | runtime + Raft + cold | leader failover, truncated cold read | `runtime_raft_network_leader_failover_cold_live_read_integrity` | PR, nightly |
| `runtime-raft-network-randomized-cold-read-failures` | 322-326 | runtime + Raft + cold | truncated cold read | `runtime_raft_network_cold_live_read_integrity` | PR, nightly |
| `runtime-raft-snapshot-install-failures` | 232-236 | runtime + Raft | corrupted snapshot append counts | `runtime_raft_snapshot_install_integrity` | PR, nightly |
| `runtime-raft-network-cold-live-truncate-failures` | 222-226 | runtime + Raft + cold | partition, heal, truncated cold read | `runtime_raft_network_cold_live_read_integrity` | PR, nightly |
| `runtime-raft-network-cold-live-write-failures` | 312-316 | runtime + Raft + cold | partition, heal, failed cold write | `runtime_raft_network_cold_live_write_integrity` | PR, nightly |
| `runtime-raft-network-partition-failures` | 212-216 | runtime + Raft | partition without heal | `runtime_raft_network_follower_catchup` | manual |
| `pipeline-smoke-runtime-interleaving-read-corruption` | 172-176 | runtime actors | read expectation of one client | `runtime_interleaving_read_your_write` | manual |
| `runtime-interleaving-truncate-failures` | 182-186 | runtime + cold | truncated cold read | `runtime_interleaving_cold_read_integrity` | manual |
| `runtime-interleaving-write-failures` | 192-196 | runtime + cold | failed cold write | `runtime_interleaving_cold_write_integrity` | PR, nightly |
| `pipeline-smoke-http-producer-retry-corruption` | 262-266 | HTTP | duplicate-retry expectation | `http_producer_retry_idempotence` | PR, nightly |
| `pipeline-smoke-http-live-sse-corruption` | 267-271 | HTTP | SSE next-offset expectation | `http_live_sse_delivery` | PR, nightly |
| `pipeline-smoke-http-live-waiter-corruption` | 272-276 | HTTP | backpressure metric expectation | `http_live_waiter_backpressure` | PR, nightly |
| `pipeline-smoke-http-snapshot-body-corruption` | 332-336 | HTTP | snapshot body expectation | `http_snapshot_protocol_surface_read` | PR, nightly |
| `raft-partition-failures` | 202-206 | 3-node Raft | partition without heal | `raft_partition_follower_catchup` | manual |

Seeds outside the families map by `seed % 12` onto no-fault, partition/heal, snapshot catch-up, follower restart, cold/live read, cold read fault, cold write fault and cold read delay. Seeds 54-59 and 68-71 select the fixed HTTP producer, live-limit, live and base protocol scenarios, cold delete fault, cold write delay, cold read truncation and the three runtime actor scenarios.

The randomized runtime/Raft plan chooses stream count, per-stream append counts, partition and heal, runtime-owned leader failover, cold flush with write failure and retry, write or read delay, truncated read and retry, follower restart during cold flush, producer sessions, epoch bumps, concurrent producers, partial reads, tail reads, stream close with append-after-close, and snapshot publish/read/bootstrap. Catch-up waits target the leader's OpenRaft `last_log_id`, not the stream state machine's commit index.

Disk and recovery scenarios run as `#[test]` functions on the production journal and recovery gate, three nodes with two Raft groups sharing each node's core journal:

- `disk_tests`: `SimDisk` semantics, power loss keeping every acknowledged write, write and `fsync` errors poisoning a node until restart, purges and segment deletion surviving power loss, reordered unsynced tails, a removed run state, and vote and run-state durability under `raft.wal.fsync = "never"`.
- `recovery_tests`: follower, leader, staggered and majority power losses, a follower that lost its disk rebuilt while writes continue, gated replicas across restarts, a stale-term leader, and membership change during power loss. The simulated network records each vote answer with the target's gate, and no gated replica grants a vote. `recovery_regression_tests` holds schedules that once broke the gate or the run state.
- `poison_marker_tests`: a failed poison-marker publication across process and host crashes never loses an acknowledged prefix and refuses corrupt history.
- `rolling_restart`: `ursula_ctl::plan_drain` and `check_readiness` against a three-node OpenRaft cluster, including aborts when successors are partitioned or a restart fails.
- `tests.rs` bounded-state cases: producer state after a mid-stream snapshot install and external locators across ambiguous commits, staged snapshot references and ambiguous compaction.

## 6. Fault vocabulary

`SimFaultAction` (`madsim_harness/faults_inner.rs:252-319`) is the schedulable fault and workload step set. Each `SimFaultStep` binds an action to a named phase of the scenario.

- **Network:** `PartitionSeededFollower`, `HealSeededFollower`.
- **Node:** `StopSeededFollower`, `RestartStoppedFollower`, `StopCurrentLeader`, `RestartStoppedLeader`.
- **Raft snapshot and membership:** `CreateLeaderSnapshot`, `PurgeLeaderLog`, `AddLaggingLearner { node_id }`, `CorruptRuntimeRaftSnapshotAppendCounts`.
- **Cold store:** `WriteColdChunk`, `PublishColdFlush`, `FailNextColdRead`, `FailNextColdWrite`, `FailNextColdDelete`, `RetryColdWriteAfterFailure`, `RetryColdReadAfterFailure`, `DelayNextColdWrite { delay_ms }`, `DelayNextColdRead { delay_ms }`, `TruncateNextColdRead { returned_len }`, `VerifyHotReadAfterColdWriteFailure`, `CorruptColdLiveReadExpectation { node_id }`.
- **Runtime:** `StartRuntimeWaitRead`, `DelayRuntimeAppend`, `StartRuntimeConcurrentClients`, `DelayRuntimeClientAppends`, `RunRuntimeColdFlushAllGroups`, `VerifyRuntimeColdLiveReads`.
- **Seeded workloads:** `RunRuntimeSeededInterleaving { plan }`, `RunRuntimeRaftNetworkWorkload { plan }`, `RunHttpProtocolSurfaceWorkload { plan }`. Their plans carry the seeded branches and the `corrupt_*_expectation` flags.
- **HTTP expectation corruption:** `CorruptHttpProducerDuplicateExpectation`, `CorruptHttpLiveSseNextOffsetExpectation`, `CorruptHttpLiveLimitBackpressureExpectation`, `CorruptHttpSnapshotBodyExpectation`.

Disk faults live in `SimDisk` (`crates/ursula-raft/src/log_store/sim_disk.rs`) and are driven directly by the disk and recovery tests:

- `SimDisk::inject_fault(path, SimDiskFault)` fails the next matching operation: `Write`, `PartialWrite` (half the bytes, then an error), `Sync`, `Remove` or `Rename`. A failed `fsync` marks pages clean without persisting them, as Linux does.
- `SimDisk::power_loss(prefix)` keeps or drops each unsynced 4 KiB page with madsim's RNG, which can leave zero-filled holes, and reverts directory entries not made durable by a directory `fsync`. `power_loss_losing_unsynced` drops every unsynced page.
- `SimDisk::process_crash(prefix)` keeps the page cache. `RaftGroupFileLogStore::pause_simulated_writer` and `abort_simulated_writer` hold or discard queued journal writes.

## 7. Invariants

Each invariant helper records `SimEvent::InvariantFailed { invariant, ... }` in the trace and then panics, so failure artifacts and replay assert the invariant by name.

| Invariant | Observes | Families |
| --- | --- | --- |
| `leader_read_linearizable` | a leader read that answers 200 reflects every append acknowledged before it, a leader cut off from quorum answers 503 | `leader-read-linearizability*` |
| `producer_repeats_are_exactly_once` | every key applies one body once: a resend after an unknown outcome or a client timeout is answered as a repeat, a clone with another `Producer-Digest` gets `key-conflict`, and the stream holds each key's body exactly once where its receipt says. Negative controls in `write_keys.rs` tests: writes without digests, a renumbering client | `producer-repeats-exactly-once*` |
| `idle_expiry_cannot_reopen_an_epoch` | after a producer retires, a writer with `Stream-Incarnation` never applies a write of its retired epoch again, across a leader kill, and a claim above the floor applies. Negative control: the stale writer without the token | `idle-expiry-cannot-reopen-an-epoch` |
| `rewind_applies_exactly_once` | an import repeated under leader churn applies one history event: every answer names one history, every replica holds it with the backup's digest, `HEAD` serves the restored token and names the source's token and tail, the bytes are the backup's, and later leader changes change no token. Negative control in `history.rs` tests: the import not keyed by its backup | `history-rewind-under-leader-churn*` |
| `stale_writers_never_apply_after_restore` | after a restore, a write with the source's token answers `412`, a write above the surviving seq answers `403 epoch-closed;by=history`, the surviving write answers its receipt, and the stream holds the backup's bytes plus exactly the writes applied after the restore. The one write let through is a claim by a producer whose record did not survive (G2). Negative controls in `history.rs` tests: the history fence (row 11) off, the token check (G1) off, through `ursula_stream::sim_controls` | `stale-writers-after-restore` |
| `accepted_loss_never_applies_a_stale_entry` | after a quorum loss and a recovery plan, no replica ever applies the write a cut-off leader held uncommitted, which the lost history may have overwritten, across member restarts in any order, and every replica applies the plan's history event. Negative control in `accepted_loss.rs` tests: the cut deferred to each member's next start (B1 of the #581 review) | `recovery-plan-*` |
| `recovery_plan_never_elects_an_unplanned_leader` | until a recovery plan's history event applied, every replica that leads its group holds the plan. Negative control in `accepted_loss.rs` tests: the plan sent to the members first, without the leader's plan term, and to the leader last (N1 of the second #581 review) | `recovery-plan-survivor-campaigns*` |
| `leader_failover_no_loss_or_dup` | all nodes read exactly the pre- and post-failover payload | `leader-failover` |
| `raft_partition_follower_catchup` | an isolated follower catches up after heal | `raft-partition-failures` |
| `cold_live_read_consistency` | cold plus hot reads equal the committed payload on every node | cold-path seeds |
| `runtime_interleaving_read_your_write` | each client reads its acknowledged appends | `runtime-interleaving*` |
| `runtime_interleaving_cold_read_integrity`, `runtime_interleaving_cold_write_integrity` | cold reads and flushes after cold faults | interleaving failure families |
| `runtime_raft_network_read_your_write` | runtime reads through the Raft-owned group return acknowledged bytes | runtime/Raft families |
| `runtime_raft_network_follower_catchup` | a follower applies the leader's last log index after heal | recovery, partition failures |
| `runtime_raft_network_leader_failover_no_loss_or_dup` | post-failover reads equal pre- plus post-failover appends | failover families |
| `runtime_raft_network_partial_read_integrity`, `runtime_raft_network_tail_read_empty` | non-zero offset reads and empty reads at the tail | randomized |
| `runtime_raft_network_close_state` | closed flag survives, append after close is rejected | randomized |
| `runtime_raft_network_snapshot_publish_read` | latest and exact snapshot reads and bootstrap agree | randomized |
| `runtime_raft_network_cold_live_read_integrity`, `runtime_raft_network_cold_live_write_integrity`, `runtime_raft_network_leader_failover_cold_live_read_integrity` | cold/live reads after cold faults, recovery or failover | cold-live families |
| `runtime_raft_snapshot_install_integrity` | a restored runtime continues append counts and offsets from the snapshot | snapshot-install families |
| `http_producer_retry_idempotence`, `http_live_sse_delivery`, `http_live_waiter_backpressure`, `http_snapshot_protocol_surface_read` | producer retry, SSE delivery, waiter backpressure and snapshot read through the axum handlers | HTTP families |
| `http_protocol_randomized_read_your_write`, `http_protocol_randomized_sse_delivery`, `http_protocol_randomized_live_waiter_backpressure` | the same properties under a seeded plan | `http-protocol-surface-randomized*` |

The disk and recovery tests assert their properties directly: every acknowledged write survives or the group is gated, no gated replica grants a vote, and the log never forks.

The simulator catches only what its invariants observe. Every new workload names the invariant it sharpens. OpenRaft owns internal Raft safety. DST checks how Ursula wires Raft, runtime actors, stream state, cold storage and reads into client-visible guarantees.

## 8. Corpus, replay and minimization

The corpus lives in `crates/ursula-sim/corpus/`. `smoke_corpus_replays` (`madsim_harness/tests.rs:1791`) and the startup of `ursula-sim smoke` replay all three files.

- `smoke.json`: 5 `SimRegressionRecord`s (scenario, seed, stream, outcome with trace) for the base Raft and cold scenarios.
- `schedule-smoke.json`: 27 `SimScheduledRecord`s. Replay asserts that each stored schedule still equals `SimSchedule::generate(seed)` and that the outcome matches.
- `failure-smoke.json`: 18 minimized `SimFailureRegressionRecord`s. Each replays as an expected panic and asserts its invariant, with an exact panic string or stable `panic_contains` substrings when the message names seed-dependent objects.

On failure, `smoke` writes three files per seed to `--failure-dir`: `seed-N-failure.json` (seed and schedule), `seed-N-stable-trace.json` (the comparison surface, with nondeterministic raw cold-store events filtered and metric counters reduced to presence bits) and `seed-N-raw-events.json` (every event, including per-RPC network decisions and cold-store operations). By default the first failure fails the command. With `--expect-failures` every seed runs and the command fails only if no seed failed.

`ursula-sim` subcommands (all require `--cfg madsim`):

- `record <seed> [output]` writes a `SimScheduledRecord`.
- `replay --seed N | --artifact PATH` reruns a schedule. `--expect-invariant`, `--expect-panic-contains` and `--expect-artifact-panic` turn an intentional panic into a pass.
- `minimize --artifact PATH` shrinks a schedule while a target holds: `--invariant`, `--panic-contains`, `--event` with `--event-min-count`, `--stable-prefix` or `--stable-exact`. The report embeds `minimized.record` for a passing target or `minimized.failure` (schedule, panic, stable trace, raw event log) for a reproduced panic.
- `assert-shape --artifact PATH --steps-exact A,B,...` checks the minimized fault steps by `SimFaultAction` variant, so a rename is a compile error.

Shrink operations:

- remove a fault step, or an orphan heal or retry, and add a missing heal to test whether the invariant still needs the partition
- remove runtime interleaving clients and remap the corrupt-read target, zero timing delays, lower the flush group limit
- remove or lower cold read and write delays, truncation lengths and corrupt-read node ids
- drop runtime/Raft workload branches (cold/live verification, restart during flush, leader failover, partial and tail reads, close, snapshot, producer sub-features) and `corrupt_*_expectation` flags
- shrink append and failover payload counts to one and collapse multi-stream workloads to one stream
- drop HTTP plan branches and move or drop `panic_after` targets

## 9. Audits and tools

`python3 -m scripts.dst <cmd>`. `all` runs every entry in `AUDITS` (`scripts/dst/audits.py`).

- `nondeterminism`: sim-reachable code has no wall clock, real `Instant`, Tokio runtime builder or OS thread call without a whitelist entry (`--report`, `--json`, `--update-baseline`).
- `pipeline-smoke`: every corpus record carrying a `corrupt_*_expectation` field falls in a `pipeline-smoke-*` seed range.
- `layer2`: no `fail_point!` macro exists under `crates/`.
- `ci-shape`: no workflow asserts multi-element fault-step lists with strict-equality `jq`. Use `assert-shape` instead.
- `modularity`: `madsim_harness/mod.rs` stays within `LINE_BUDGET = 2_500` lines.
- `seed-inventory`: PR and nightly workflows run the expected families and ranges, every family exists in `SEED_FAMILIES`, and seed totals stay within budget.
- `failure-guards`: every `failure-smoke.json` record has a PR minimize command, an embedded-trace assertion and a minimized-artifact replay.
- `sim-patches`: each vendored crate under `third_party/` is the version `Cargo.lock` builds.
- `seed-report`: writes PR and nightly seed inventory to `target/ursula-sim-seed-inventory/`.
- `result-summary`: summarizes `seed-*-failure.json` artifacts under `target/` into `target/ursula-sim-result-summary/`.
- `coverage-report`: counts distinct stable trace events across a sweep into `target/dst-coverage-report.{json,md}`.
- `throughput`: times `ursula-sim smoke` per seed and suggests PR and nightly seed counts.

## 10. CI

The PR job `deterministic-simulation` in `.github/workflows/ci.yml` runs, in order: `python3 -m scripts.dst all`, the `ursula-raft` unit tests under madsim, every negative control (`cargo test -p ursula-sim --lib negative_control`: each switches off exactly one defence and asserts its scenario fails the invariant that defence keeps), `smoke_corpus_replays`, a smoke sweep of `--seed-range 60..=64`, `--seed-range 137..=140` and the families `http-protocol-surface-randomized`, `runtime-raft-network-cold-live-write-recovery`, `runtime-interleaving`, `leader-read-linearizability`, `producer-repeats-exactly-once`, `idle-expiry-cannot-reopen-an-epoch`, `history-rewind-under-leader-churn`, `stale-writers-after-restore`, `recovery-plan-cut-under-restarts` and `recovery-plan-survivor-campaigns`, the bounded-state subset, `recovery_tests`, the journal segment and poison-marker tests, `snapshot_metadata_`, `strict_replay`, and every expected-failure family marked PR above. For each failure family the job minimizes a checked-in seed, checks the minimized steps with `assert-shape` or `jq`, asserts the embedded trace carries the `invariant_failed` event, and replays the minimized artifact with `--expect-invariant`. Artifacts upload on failure.

`.github/workflows/dst-nightly.yml` runs daily at 03:17 UTC and on dispatch. It writes the nondeterminism and seed reports, sweeps `--seed-range 60..=199` with the PR families plus `runtime-raft-network-randomized-extended`, `leader-read-linearizability-extended`, `producer-repeats-exactly-once-extended`, `history-rewind-under-leader-churn-extended`, `recovery-plan-cut-under-restarts-extended` and `recovery-plan-survivor-campaigns-extended`, runs every negative control and every PR failure family, and widens test seed ranges through environment variables (`RECOVERY_GATE_SEEDS`, `POISON_MARKER_SEEDS`, `EXTERNAL_LOCATOR_AMBIGUITY_SEEDS`, `EXTERNAL_LOCATOR_SNAPSHOT_SEEDS`, `AMBIGUOUS_COMPACTION_SEEDS`). It always writes and uploads the result summary and coverage report.

The `seed-inventory` audit enforces seed budgets of `PR_SEED_BUDGET = 200` and `NIGHTLY_SEED_BUDGET = 1500`, derived from 2 and 30 minute budgets at about 70 seeds per minute per core for one bundled `smoke` invocation. When throughput changes, measure with `python3 -m scripts.dst throughput --measure 20` on the CI runner and adjust the budgets and workflow ranges together.

## 11. Out of scope

- tonic wire encoding and production gRPC (`GrpcRaftNetworkFactory`), covered by end-to-end tests
- real TCP and multi-process routing
- `ThreadPerCore` threading, CPU affinity and OS-thread concurrency, covered by native tests and benchmarks
- real S3 semantics such as consistency, throttling and multipart behavior, covered by the S3 integration tests in CI
- network message reordering and duplication: the in-process network delays and partitions but never reorders or duplicates RPCs

# Deterministic Simulation Testing

## Objective

Add a deterministic simulation testing (DST) layer to Ursula so that protocol-level correctness can be exercised under reproducible fault schedules. The target is a seed-driven simulator that can run many schedules in CI/nightly jobs and replay a failure from the same seed and minimized trace.

DST complements the existing EC2 chaos test rather than replacing it. Chaos covers integration with real processes, real network, real S3, systemd, and long-running operational drift. DST covers the schedule space that wall-clock chaos reaches only by probability: message ordering, timer interleavings, precise crash/restart points, retry races, and storage-fault timing.

This document is the implementation baseline. It intentionally separates what Ursula already has from what must be built before a full simulator is credible.

## Motivation

The current 24/7 EC2 chaos runner is useful, but it is not deterministic simulation.

It already does more than a single clean-stop smoke test:

- It can inject node stops, netem delay, netem loss, and asymmetric partitions through the chaos fault daemon.
- It continuously writes deterministic payloads, checks readable offsets, samples node metrics, verifies live/cold reads, and probes producer duplicate/stale-epoch behavior.
- It exercises real deployment wiring, real object storage configuration, service restarts, and recovery SLOs.

Those are the right jobs for chaos. The gaps are different:

- **Reproducible interleavings.** A failed chaos run gives logs and timestamps, not a tick-by-tick replay of the exact schedule.
- **Message ordering and timer edges.** Bugs that require "AppendEntries A arrives, election timer fires, then snapshot install completes" are unlikely to appear reliably in wall-clock tests.
- **Fine-grained storage faults.** EC2 can stop a node or impair a network, but it cannot cheaply inject "write succeeded, fsync failed", "cold object upload succeeded, publish failed", or "range read returned a truncated body" at exact instruction points.
- **Large seed sweeps.** Chaos validates a small number of long real-world runs. DST should run many short schedules and keep failing seeds as regression tests.
- **Client-session invariants.** Read-your-write, idempotent retry, cold/live consistency, and SSE/long-poll behavior need adversarial timing, not only steady traffic.

DST is the standard companion to chaos for distributed systems with a non-trivial protocol surface. FoundationDB, TigerBeetle, and RisingWave are the relevant precedent. The important lesson for Ursula is not the framework name; it is the discipline: every source of nondeterminism must be either virtualized, removed, or made explicit in the seed.

## Current Position

Ursula has several useful DST entry points, but it is not yet DST-ready.

- **Application-level randomness is low.** The server crates do not use `rand::thread_rng`, `Uuid::new_v4`, or `fastrand` for behavioral choices. Most ordering is delegated to Raft and deterministic state-machine application. This is a strong starting point, but not a complete determinism proof because dependencies, clocks, task scheduling, filesystem behavior, and tests still introduce nondeterminism.
- **The stream state machine is command-driven.** `ursula-stream` has no async runtime or I/O dependency. `StreamStateMachine::apply(command)` is a function of the existing state and the command, with time supplied as `now_ms` in the command/request. This makes it the best first target for property tests.
- **Raft networking is trait-based.** OpenRaft uses `RaftNetworkV2<UrsulaRaftTypeConfig>` / `RaftNetworkFactory` as the network boundary. Production uses `crates/ursula-raft/src/grpc.rs`. The spike has extracted the in-process network into `crates/ursula-raft/src/registry.rs` and added `InProcessRaftNetworkPolicy` for source/target partitions and simulated delay. This gives Phase 3 a reusable Raft network seam without touching production gRPC.
- **Raft log storage is trait-based.** `RaftLogStorage` and `RaftLogReader` have memory and file-backed implementations. This gives us a clear place to run memory-only schedules first, then add file-log/failpoint coverage.
- **Cold storage is already behind `ColdStore`.** `ColdStore` uses opendal memory or S3 backends. The simulator can now build a store from a prebuilt `Operator`, attach an optional fault policy, and override the delay function. The current policy is enough to inject deterministic write/read/delete errors, virtual-time read/write delay, and truncated range reads.
- **Runtime threading has a usable hosted mode.** `RuntimeThreading::HostedTokio` runs workers on the caller's async runtime. The runtime crate now has a small `rt` shim so project-owned spawn/sync/time calls use Tokio in normal builds and `madsim-tokio` under `cfg(madsim)`. The production default, `ThreadPerCore`, still starts OS threads and builds per-core Tokio runtimes directly, and is intentionally rejected under `cfg(madsim)`.
- **Runtime can now run on a Raft-backed group engine under madsim.** The `runtime-raft-engine` seed family runs `ShardRuntime::HostedTokio` with a scoped `RaftGroupEngineFactory`, creates a stream, appends through OpenRaft, reads the committed payload back through the runtime API, and records Raft write/apply metrics. This is single-node Raft convergence, not yet a multi-node runtime/Raft network fault scenario.
- **Runtime can now drive a multi-node in-process Raft group under madsim.** The `runtime-raft-network` seed family runs `ShardRuntime::HostedTokio` with a sim-only factory that creates a three-node in-process OpenRaft group, returns the leader engine to the runtime, keeps follower engines alive, and verifies that runtime append/read observes committed data while raw artifacts contain delivered in-process Raft RPCs. The `runtime-raft-network-recovery` family adds a directed partition/heal schedule on the same path: isolate one follower during a runtime append, prove it lags, heal it, wait for catch-up, and verify the committed data remains readable through the runtime API. The `runtime-raft-network-cold-live-recovery` family then flushes a cold chunk through the same runtime-owned Raft group and verifies cold/live read consistency after recovery. The `runtime-raft-network-cold-live-restart` family stops a seeded follower before the cold-flush commit, restarts it from the same in-memory log store, waits for it to catch up to the cold-flush log index, and then verifies cold/live read. The `runtime-raft-network-cold-live-write-recovery` family injects a one-shot cold `WriteChunk` failure during the runtime-owned cold flush, retries the flush, then verifies cold/live reads and cold upload/publish metrics. The lower-level `leader-failover` family stops the current OpenRaft leader, waits for a new leader, appends after failover, restarts the old leader from its log store, and verifies all nodes read the exact combined payload. The `runtime-raft-network-leader-failover` family now uses a sim-only runtime seam to shut down the runtime-owned leader engine, install the newly elected leader engine into the same `ShardRuntime`, append/read through the runtime API after failover, restart the old leader, and verify catch-up. The matching failure families cover omitted-heal Raft catch-up, post-recovery cold-read truncation, runtime-owned cold-flush write-fault invariants, and leader-failover plus cold/live read truncation.
- **Initial HTTP/axum protocol-surface scenarios exist.** The `http-protocol-surface` seed builds Ursula's axum router in-process with an injected `WallClock`, creates a stream with TTL, appends through producer headers, verifies duplicate retry and stale-epoch fencing responses, reads the committed bytes through HTTP, publishes a visible snapshot, follows latest-snapshot redirect/read/bootstrap/delete-conflict semantics, and advances deterministic wall time to verify expiry. The `http-producer-protocol-surface` seed adds same-stream concurrent producers, duplicate retry, producer sequence gap rejection, epoch bump, stale-epoch rejection, and final HTTP read verification. The `http-live-protocol-surface` seed drives long-poll wakeup and SSE tail delivery through the same in-process router. The `http-live-limit-protocol-surface` seed covers long-poll timeout cleanup and live-read waiter backpressure. This covers handler parsing/rendering, protocol-visible wall time, producer-session response headers, selected snapshot behavior, and selected live-read behavior, but not TCP, production gRPC, or full process routing.

The main friction points are:

- **Wall-clock time has a first HTTP/server seam.** `Instant::now()` is mostly metrics/span timing. Protocol-visible `now_ms` for TTL, expiry, access renewal, and read/write commands now flows through `HttpState`'s injectable `WallClock` in the HTTP/server layer. Runtime-internal request builders and non-server helpers should continue moving away from direct `SystemTime::now()` as they enter DST scope.
- **Tokio usage is partially routed.** The runtime actor path uses the local `rt` shim for project-owned spawn/sync/time calls, which lets `HostedTokio` run inside madsim. The in-process axum/router path has first simulator scenarios, and HTTP long-poll timeout now goes through a small `cfg(madsim)` time seam. The tonic/gRPC, TCP server, and `ThreadPerCore` paths still need explicit treatment before full-node simulation can be claimed.
- **Runtime-owned leader failover is still a simulation seam, not production rerouting.** The simulator can now shut down and install a group engine in a hosted `ShardRuntime`, which is enough to test runtime API behavior after OpenRaft leadership changes. Production HTTP/gRPC client routing and multi-process leader discovery are still outside the current simulator boundary.
- **Production gRPC and full HTTP networking are not automatically in scope.** The first simulator should focus on the Raft/runtime/state-machine protocol plus selected in-process protocol-surface checks. Wire-format bugs in tonic/HTTP, real TCP behavior, and process-level routing remain covered by unit/e2e/EC2 tests unless we explicitly add a later full-node simulation mode.

## Approach

Build the DST stack in three layers. Each layer should deliver standalone value.

### Layer 1 - Property tests on the state machine

`ursula-stream` should get `proptest` coverage over generated command sequences. The first properties should cover:

- offset monotonicity and contiguity;
- producer epoch/sequence idempotence and fencing;
- stream TTL/expiry transitions under generated `now_ms`;
- cold flush planning/application consistency;
- snapshot round-trip equivalence.

This layer requires no runtime abstraction and should be the first implementation step.

### Layer 2 - Directed failpoint injection (retired — subsumed by simulator faults)

The original plan was to add `fail-rs` named fail points at storage and replication boundaries (file-log write/sync, WAL write/sync, post-commit pre-apply response, snapshot build/install, cold upload/publish/cleanup, cold range read).

**This layer is now retired.** Every transition it would have exercised is covered by the Layer 3 simulator's fault vocabulary:

| Original failpoint target | Now covered by simulator fault |
|---|---|
| file-log / WAL write/sync error | `LogWriteError`, `LogSyncError` (planned) — file log is `cfg(not(madsim))`-only, so simulator coverage uses the memory log store with `ColdWriteError`-style errors on the equivalent boundary |
| post-commit pre-apply response | runtime-raft-network workload with `corrupt_runtime_raft_snapshot_append_counts`-style real perturbation and `runtime_raft_network_*` invariants |
| snapshot build/install | `runtime-raft-network-snapshot-install-failures` (real append-count corruption) + `runtime-raft-network-snapshot-corruption` pipeline-smoke |
| cold upload/publish | `fail_next_cold_write`, `delay_next_cold_write`, `runtime-raft-network-cold-live-write-failures` |
| cold range read / cleanup | `truncate_next_cold_read`, `delay_next_cold_read`, `runtime-raft-network-cold-live-truncate-failures`, `cold_delete_fault` |

Why retired: keeping both `fail-rs` infrastructure and the simulator's fault vocabulary alive means writing every failure twice and reasoning about two execution models. The simulator already injects errors at the same boundaries, with the bonus that the failure is replayable from a u64 seed. `fail-rs` would only add value for non-simulator integration tests, and those are covered by EC2 chaos.

If a future use case actually needs failpoints (e.g. a non-simulator integration test that wants to inject errors into production gRPC), revisit this decision. Until then, **DoD #4 is satisfied by absence**: `grep -rn 'fail_point!' crates/ursula*/src` returning zero matches is the correct state, paired with this retirement note.

### Layer 3 - Multi-node deterministic simulator

Run N virtual Ursula nodes in one process with a virtualized clock, network, and cold storage, driven by a seeded schedule. The simulator should own every source of nondeterminism it introduces: fault selection, network delay/drop/reorder, client workload choices, crash/restart points, and storage errors.

Layer 3 is the largest commitment. The rest of this document is mostly about making that layer implementable without overclaiming what the first version will cover.

## Framework Choice

The recommendation is to spike **madsim** first, but to treat it as a go/no-go decision until it compiles and runs Ursula's minimal OpenRaft path.

| Framework | What it provides | Fit for Ursula |
|---|---|---|
| **madsim** | Tokio-like deterministic runtime, virtual time, scheduler, process/network controls, and companion crates such as `madsim-tonic`. | Best first candidate. It matches the "run real async code under a simulator" model and has RisingWave precedent. Ursula still needs a spike because OpenRaft, tonic, opendal, and direct runtime/thread construction must all line up. |
| **turmoil** | Deterministic hosts, time, simulated networking, seeded network manipulation, and unstable filesystem support. | Useful fallback or comparison point, but it is less directly aligned with Ursula's OpenRaft + tonic dependency graph. It would likely require more explicit networking substitution. |
| **shuttle** | Randomized and controlled scheduling for concurrent Rust code. | Complementary. Useful for isolated actor/concurrency tests if `core_worker` or watcher code can be wrapped, but not a full distributed-system simulator. |
| **stateright** | Model checking for protocol-level state spaces. | Optional for small subprotocols such as membership change or leader transfer. It should not block the main simulator. |
| **loom** | Exhaustive interleaving for atomics and low-level synchronization. | Out of scope for the initial DST effort. Ursula's risk is distributed scheduling and actor behavior, not custom lock-free data structures. |
| Custom runtime trait | A hand-rolled abstraction over async runtime, time, network, and storage. | Rejected as the primary path. It is too much surface area. Small explicit seams for clock, worker spawning, and storage construction are still required. |

### Why madsim first

- It is designed for deterministic distributed-system testing, not only unit-level async tests.
- It has a package-replacement integration model for Tokio/tonic-style crates and a `cfg(madsim)` execution mode.
- `madsim-tonic` has tonic-versioned releases, so the current tonic 0.12 dependency is plausible rather than obviously blocked.
- RisingWave provides a real production-scale reference for this style of Rust simulation.

### madsim spike status and risks

- **OpenRaft compatibility is plausible.** The current spike adds a `cfg(madsim)` OpenRaft `AsyncRuntime` backed by `madsim-tokio` and proves that a three-node memory Raft group can elect a leader, commit a create/append workload, replicate it, and read from every node under a fixed seed.
- **Strict replay now covers the minimal OpenRaft path.** `madsim::runtime::Runtime::check_determinism` passes for a three-node in-memory group that creates a stream, appends, waits for every node to apply, and reads the payload from every node. Diagnostic probes also pass for append enqueue, quorum commit notification, apply completion/response, leader-local read, follower log replication, follower apply, and follower read.
- **The spike still has harness limits.** Multiple `Runtime::check_determinism` probes should not be run as one default test group in the same test binary, so diagnostic probes are ignored and run individually. The strict replay smoke test covers append/replicate/read but not restart/shutdown; restart belongs in the next simulator-harness phase.
- **Cargo replacement is not just an import rewrite.** madsim usually replaces crates such as `tokio` and `tonic` at the dependency level. A local `rt` re-export helps with project-owned code, but it does not by itself virtualize third-party Tokio usage.
- **Thread-per-core bypass is required.** `ThreadPerCore` creates OS threads and real Tokio runtimes. The first simulator should run `HostedTokio` only, or add an explicit worker-spawn abstraction that maps to madsim tasks.
- **OpenRaft deterministic RNG scope must cross runtime worker tasks.** A `ShardRuntime` worker spawned with plain `madsim-tokio` does not automatically inherit OpenRaft's task-local deterministic RNG. The simulator's Raft-backed runtime scenario wraps the Raft `GroupEngine` factory and engine calls in `MadsimOpenRaftRuntime::scope(...)`. A cleaner shared spawn-scope seam remains a good follow-up before broad multi-node runtime/Raft sweeps.
- **Wall-clock semantics need a seam.** `SystemTime::now()` cannot remain the source for `now_ms` in a deterministic run.
- **opendal/S3 semantics may need narrowing.** The MVP can use opendal memory plus a fault layer. Faithful S3 behavior is a later target, not a prerequisite for the first simulator.

## Architecture

```
                +-----------------------------------------+
                |            Simulator Driver             |
                |     seed -> schedule -> event loop      |
                +----------+-------------+----------------+
                           |             |
               +-----------v---+     +---v-------------+
               | Virtual Clock |     | Fault Schedule  |
               +-----------+---+     +---+-------------+
                           |             |
          +----------------v-------------v----------------+
          |              Simulated Ursula Cluster          |
          |                                                |
          |  Node 1      Node 2      Node 3      ...       |
          |  runtime     runtime     runtime               |
          |  raft        raft        raft                  |
          |  cold store  cold store  cold store            |
          +----------------+-------------------------------+
                           |
               +-----------v------------+
               | Workload + Invariants  |
               +------------------------+
```

The MVP simulator should not try to exercise every production boundary at once. Start with the runtime/raft/state-machine path and memory cold storage. Add selected in-process HTTP/axum protocol-surface checks once clock/runtime seams exist. Add SSE, production gRPC wire encoding, real TCP process routing, and real file-log behavior only after the deterministic core is stable.

### Runtime and task abstraction

The runtime abstraction has two jobs:

1. make project-owned Tokio imports easy to swap or centralize;
2. keep simulated execution away from real OS threads and real runtime builders.

A local re-export module exists in `ursula-runtime` and covers the current hosted actor path. It uses Tokio in ordinary builds and `madsim-tokio` under `cfg(madsim)`:

```rust
pub use runtime_shim::{spawn, sync, time};
```

This is not enough by itself. The simulator also needs a worker-spawn policy:

- production `ThreadPerCore` keeps using one OS thread plus current-thread Tokio runtime per core;
- tests and simulation use `HostedTokio`;
- direct `tokio::runtime::Builder` paths are excluded from `cfg(madsim)` until proven safe.

### Clock

Ursula needs two explicit time concepts:

- **Monotonic time** for metrics spans, timeouts, sleeps, and Raft/runtime scheduling.
- **Wall-clock milliseconds** for protocol-visible `now_ms`, TTL, expiry, and access-renewal decisions.

`Instant::now()` call sites can move through the runtime abstraction or madsim package replacement. Protocol-visible `unix_time_ms()` should flow through an explicit clock provider so tests and the simulator can supply deterministic wall-clock values.

The first clock seam can be small:

```rust
pub trait WallClock: Send + Sync + 'static {
    fn unix_time_ms(&self) -> u64;
}
```

The HTTP/server layer should use this instead of calling `SystemTime::now()` directly. State-machine tests can keep passing generated `now_ms` through commands.

This seam now exists in `HttpState`: ordinary routers use `SystemWallClock`, while tests and future simulator routers can call `HttpState::with_wall_clock(...)` or `with_wall_clock_handle(...)` and build the app with `router_with_http_state(...)`.

### Network

The production network remains `GrpcRaftNetworkFactory` in `crates/ursula-raft/src/grpc.rs`.

The simulator starts from the extracted in-process network in `crates/ursula-raft/src/registry.rs`. It preserves the OpenRaft trait boundary:

```rust
pub struct InProcessRaftNetworkFactory;
pub struct InProcessRaftNetworkPolicy;
pub struct InProcessRaftFaultScript;
```

The current policy handle supports:

- deliver immediately;
- delay all messages by a configured duration, using virtual time under `cfg(madsim)`;
- fail directionally for asymmetric partitions.

The current script handle can apply policy actions at named workload phases. The madsim smoke path uses this to pick a follower from a seed, partition it before append, verify the majority still commits, heal the link, and verify the isolated follower catches up. Later simulator work should extend this phase script to per-message seeded decisions for drop, duplicate, and bounded reorder.

The MVP does not need to exercise tonic wire encoding. A later full-node simulation can either compile production gRPC under `madsim-tonic` or keep wire encoding covered by targeted e2e tests plus EC2 chaos.

### Cold Store

The simulator should use opendal memory storage wrapped with a faulting layer. The first constructor seam exists: `ColdStore::from_operator(operator, info)` accepts a prebuilt `Operator` plus `ColdStoreInfo`. `ColdStore` also has an optional callback-style fault policy that can fail a write or range read before the underlying opendal operation and emit a `ColdStoreEvent::FaultInjected` event.

The first fault layer should cover:

- write error;
- read error;
- delayed read/write using virtual time;
- truncated range read;
- delete/cleanup error.

Current coverage is partial: the checked-in madsim schedule corpus includes a `cold_read_fault` seed that writes and publishes a cold chunk, injects a deterministic failure into the next cold range read, records the observed error, then verifies the same committed bytes remain readable after the fault is consumed. It also includes a `cold_write_fault` seed that injects a deterministic upload failure before publishing cold metadata and verifies the committed hot payload remains readable. A `cold_read_delay` seed overrides the cold-store delay hook with `madsim::time::sleep`, injects a one-shot virtual-time delay before cold range materialization, and verifies the committed cold+hot payload remains readable. A `cold_write_delay` seed injects the same kind of virtual-time delay before cold chunk upload, then publishes cold metadata and verifies the committed cold+hot payload remains readable. A `cold_read_truncate` seed simulates a short range-read body and verifies the cold read path rejects it as `InvalidData` before a later retry succeeds. A `cold_delete_fault` seed drives the runtime orphan-cleanup path by uploading a stale cold candidate, injecting a deterministic `delete_chunk` failure during cleanup, and verifying the cleanup error metric is recorded while the recreated stream remains readable. Prefix cleanup through `remove_all` is instrumented but does not yet have a dedicated generated workload.

Faithful S3 behavior is a later phase. Ursula stores cold chunks as immutable objects, so memory-plus-faults is enough for the first correctness surface.

### Workload and Oracle

The simulator should keep workload generation separate from invariant checks:

```rust
trait Workload {
    fn step(&mut self, ctx: &mut SimContext) -> Vec<ClientOp>;
}

trait Invariant {
    fn check(&self, ctx: &SimContext) -> Result<(), Violation>;
}
```

The workload owns client-side expectations: acknowledged appends, producer sequence state, expected readable bytes, expected setsum, open SSE/long-poll sessions, and cold-confirmed samples. The invariant checker compares those expectations with observable cluster state.

## Invariants

The minimum viable simulator should check these on every quiescent point or after every delivered event:

1. **Committed data remains readable.** Any append that returned success to the client remains readable from the stream after failover, restart, snapshot install, and cold eviction.
2. **Per-stream integrity.** Client-tracked expected setsum equals the server-observed live/total setsum under the current hot/cold state; no record is lost or duplicated.
3. **Producer idempotence.** A `(Producer-Id, Producer-Epoch, Producer-Seq)` triple commits at most once, and stale epochs or sequence gaps are rejected consistently.
4. **Read-your-write.** Within one client session, reads after an acknowledged write observe that write.
5. **Cold/live equivalence.** Moving bytes from hot buffers to cold chunks does not change the readable byte sequence or total integrity.
6. **Quorum loss behavior.** A minority partition must not acknowledge writes that later disappear.
7. **Snapshot install correctness.** A node restored from snapshot reaches the leader's committed state at the snapshot index and can serve the same committed data.
8. **Raft integration safety.** For entries visible through Ursula's log/state-machine surfaces, a committed log index is never applied as two different commands.

The simulator should not claim stronger coverage than the invariants actually observe. OpenRaft's internal Raft safety remains OpenRaft's responsibility; Ursula DST should focus on how Ursula wires Raft, runtime actors, stream state, cold storage, reads, and client-visible guarantees together.

## Fault Vocabulary

Each fault is schedulable by virtual tick/time and is derived from the seed.

**Network.** `Partition(set_a, set_b)`, `AsymmetricDrop(from, to)`, `Delay(from, to, duration)`, `MessageLoss(rate)`, `Reorder(window)`, `DuplicateMessage(rate)`.

**Node/runtime.** `Pause(node, duration)`, `Crash(node)`, `Restart(node)`, `DropInFlightClient(node)`, `SlowActor(node_or_group, duration)`.

**Storage.** `LogWriteError(node)`, `LogSyncError(node)`, `SnapshotBuildError(node)`, `SnapshotInstallError(node)`, `ColdWriteError(rate)`, `ColdReadError(rate)`, `ColdReadTruncate(rate)`, `ColdDeleteError(rate)`, `ColdSlow(duration)`.

**Cluster.** `MembershipAdd(node)`, `MembershipRemove(node)`, `LeaderHint(node)` or leader-transfer if/when Ursula exposes that path.

**Clock.** `WallClockJump(node, delta)` is optional and should wait until Ursula has an explicit wall-clock provider. Monotonic Raft/runtime time should remain simulator-controlled and deterministic.

## Seed Throughput Budget

`scripts/dst_seed_throughput.py` measures `seeds/min/core` so CI seed counts can be derived rather than guessed.

Measured baselines (local dev, single core, `--cfg madsim`):

- One-seed-per-`cargo run` (worst case for short seeds): **40 seeds/min/core** — dominated by `cargo run --quiet` startup.
- Bundled invocation (one `cargo run` with many `--seed-range` / `--seed-family` flags, which is how CI actually invokes the smoke binary): **~100 seeds/min/core** estimated, since the cargo startup amortises to ~zero across many seeds.
- A conservative CI assumption with ~30% runner headroom is **70 seeds/min/core** for bundled runs.

Locked budgets, enforced by `scripts/check_dst_seed_inventory.py`:

| Track | Wall-clock budget | Implied seed budget |
|-------|---|---|
| PR `deterministic-simulation` job | 2 min  | ≤ 200 seeds |
| Nightly `dst-nightly` job         | 30 min | ≤ 1500 seeds |

When throughput changes (faster harness, slower runner, new heavy family), re-run `dst_seed_throughput.py` (use `--measure ≥ 20` to amortise overhead), update the table above, and adjust the `--seed-range` / `--seed-family` counts in the CI YAML. The inventory check fails if the YAML exceeds the locked seed budget. The bundled-invocation throughput number should be re-measured on the actual CI runner before tightening these budgets — local dev numbers are an upper bound.

## Harness Modularity (DoD #3)

The simulator's per-scenario logic currently lives in a single 12k-line file (`crates/ursula-sim/src/madsim_harness.rs`). The DoD #3 target architecture is:

```
crates/ursula-sim/src/
  scenarios/   # one file per scenario; each is a (Workload, Vec<Fault>, Vec<Invariant>) triple
  workloads/   # one file per workload kind (HTTP protocol surface, runtime/Raft network, ...)
  invariants/  # one file per invariant + a paired mutation regression test (DoD #5)
  faults/      # SimFaultAction enum + per-variant impl + helpers
```

Two enforced properties:

- **Trait scaffold in place.** `pub trait Scenario { workload, faults, invariants }`, `pub trait Workload`, `pub trait Invariant`, `pub trait Fault` are declared and visible. `scripts/check_dst_harness_modularity.py` fails CI if a sub-mod or its trait is missing.
- **Ratcheting line budget.** The same script asserts `madsim_harness.rs` ≤ `LINE_BUDGET`. Today's value is 12,500 (≈ current actual + small slack); the DoD target is 2,500. Every PR that migrates code drops `LINE_BUDGET` toward the target. CI rejects regressions (the file can't grow).
- **No `if seed == N` routing.** Once migration is done, `SimSchedule::generate` is a registry of `Scenario` impls rather than a chain of seed-range branches. Until then the existing routing is tracked under the same ratchet.

The migration is best done per scenario family: pick a family, move its workload + invariants + fault wiring into the sub-mods, shrink `LINE_BUDGET`, repeat. The Phase-A audits (`check_dst_nondeterminism.py`, `check_dst_pipeline_smoke_namespace.py`, `check_dst_ci_shape_assertions.py`, `check_dst_layer2_status.py`, `check_dst_harness_modularity.py`) all remain in CI throughout, so the refactor can't silently re-introduce coupling, expectation-mutation invariants, or new strict-equality jq.

## Phased Delivery

Each phase should land as a reviewable change and leave useful tests behind.

### Phase 0 - State-machine property tests

Scope: `crates/ursula-stream`.

Add `proptest` generators for buckets, streams, appends, producer headers, TTL/expiry times, cold flushes, deletes, and snapshots. Check offset monotonicity, dedup/fencing, expiry behavior, cold flush consistency, and snapshot round-trip equivalence.

Deliverable: property tests that run in normal CI without simulator dependencies.

### Phase 1 - madsim/OpenRaft feasibility spike

Scope: minimal workspace experiment, not a broad refactor.

Prove or disprove:

- `openraft` with Ursula's current feature set can compile under `cfg(madsim)`;
- tonic/prost generated code can either compile under `madsim-tonic` or be excluded from the MVP sim path;
- a tiny three-node OpenRaft cluster using memory log/state machine and an in-process network can elect a leader and commit one command under a seed;
- the simulated path can run without `ThreadPerCore`, OS thread spawning, or direct `tokio::runtime::Builder` construction.

Deliverable: a small spike branch or PR with the exact Cargo/config shape and a written go/no-go result. If this fails, revisit turmoil or a narrower custom simulator before doing broad rewrites.

Current result: go. `RUSTFLAGS="--cfg madsim" cargo test -p ursula-raft madsim_three_node_openraft_group -- --nocapture` passes the default madsim smoke set. That set includes a fixed-seed run that executes create/append/replicate/read twice and compares the result, plus a strict replay run that creates a stream, appends, waits for every node to apply, and reads the payload from every node under `Runtime::check_determinism`. The spike avoids production gRPC/tonic and `ThreadPerCore`. Remaining Phase 1 follow-up is harness hygiene, not a go/no-go blocker: diagnostic `check_determinism` probes need to run individually because madsim's check state is process-global, and restart/shutdown should be covered by the Phase 3 simulator rather than this minimal OpenRaft spike.

### Phase 2 - Simulation seams

Scope: production-safe seams with normal tests.

Add:

- a clock provider for HTTP/server `unix_time_ms()`;
- a runtime/task module or equivalent local import discipline for project-owned Tokio use;
- an explicit worker-spawn policy that keeps simulation on `HostedTokio`;
- an extracted in-process Raft network support module with deterministic delay/drop/partition policy; (initial source/target partition, delay policy, and named-phase fault script exist)
- a `ColdStore` constructor/factory for pre-layered opendal operators. (`ColdStore::from_operator` now exists)

Deliverable: existing unit/e2e tests still pass on normal Tokio, and a sim/test-support build can construct the same components without private test-only code.

### Phase 3 - Minimal simulator crate

Scope: new crate, `crates/ursula-sim`.

Build:

- `SimContext`;
- seeded scheduler/fault schedule shell;
- three virtual Ursula nodes with memory Raft log/state and memory cold store;
- one workload: create stream, append, read;
- invariants 1, 2, and 3 from the list above;
- failing-seed capture.

Current result: initial skeleton. `crates/ursula-sim` owns the first simulator-facing API: `ThreeNodeRaftSim`, `ThreeNodeRaftSimConfig`, `ThreeNodeRaftSimOutcome`, `SimScenario`, `SimReport`, `SimSchedule`, `SimScheduledRecord`, `SimRegressionRecord`, `SimTrace`, and `SimEvent`. It has nineteen madsim workloads:

- no-fault baseline: create stream, append, wait for all three nodes to apply, read from all nodes, and compare two runs with the same seed and trace;
- partition/heal: choose an isolated follower from the seed, partition it before append, verify majority commit while isolated, heal, verify follower catch-up/read, and compare two runs with the same seed and trace.
- leader failover: append on the initial leader, stop and unregister that leader, wait for a new leader, append after failover, restart the old leader from the same log store, verify old leader catch-up, and read the exact combined payload from all three nodes.
- snapshot/catch-up: start with two voters and one unregistered lagging node, append data, trigger leader snapshot and log purge, add the lagging node as a learner, verify `full_snapshot` transfer, catch-up, and readable committed data.
- restart follower: stop and unregister one follower, append while it is offline, recreate the follower with the same per-node log store, verify catch-up, and read committed data from the restarted node.
- cold/live read: attach a shared memory cold store to all three nodes, append data, write a deterministic cold chunk, publish the cold flush through Raft, then verify every node can read the full cold+hot payload and hot suffix.
- cold read fault: write and publish a deterministic cold chunk, inject a seeded one-shot cold range-read error, record the observed read failure, then verify the committed cold+hot payload is readable after the fault is consumed.
- cold write fault: plan a cold flush, inject a seeded one-shot cold upload error before publish, record the observed write failure, then verify no cold metadata was published and the committed hot payload is still readable.
- cold read delay: write and publish a deterministic cold chunk, inject a seeded one-shot virtual-time delay before cold range materialization, then verify the delayed committed cold+hot read still succeeds.
- cold read truncation: write and publish a deterministic cold chunk, truncate the next cold range-read result, verify the short body is rejected, then verify the committed cold+hot payload is readable on retry.
- runtime actor scheduling: spawn a real `ShardRuntime` in `HostedTokio` mode under madsim, start a `wait_read_stream` before append, delay the append in virtual time, then verify the wait-read and a follow-up read observe the committed payload.
- runtime multi-client actors: spawn a real `ShardRuntime` in `HostedTokio` mode under madsim, create four streams spanning multiple cores and Raft groups, run delayed concurrent append/read clients, and verify per-stream ordering plus read-your-write for every client.
- runtime cold-flush worker: spawn a real `ShardRuntime` with a simulated memory cold store in `HostedTokio` mode, create streams spanning multiple cores and Raft groups, run `flush_cold_all_groups_once_bounded` through the runtime API, then verify cold+live reads and cold flush upload/publish metrics.
- runtime seeded interleaving: derive client count, client append delays, cold flush group limit, flush delay, read verification delay, and selected runtime cold-read delay faults from the seed, store that plan in the schedule record, run clients and cold flush concurrently through `ShardRuntime::HostedTokio`, and verify the same seed replays the same interleaving plan and stable trace.
- runtime raft engine: spawn a real `ShardRuntime` in `HostedTokio` mode with a scoped single-node `RaftGroupEngineFactory`, append through OpenRaft, read through the runtime API, and assert the trace includes Raft write/apply metrics.
- runtime raft network: spawn a real `ShardRuntime` in `HostedTokio` mode with a sim-only three-node in-process Raft factory, append through the runtime-owned leader engine, read through the runtime API, and assert the raw event log includes delivered in-process Raft RPCs.
- runtime raft network leader failover: start from the runtime-owned three-node Raft path, verify the first runtime append/read, shut down the current runtime-owned leader engine, install the newly elected leader engine into the same runtime group, append/read through the runtime API after failover, restart the old leader from its log store, and verify catch-up.
- runtime raft snapshot install: create, append, read, and snapshot through a runtime-owned three-node Raft group; install that `GroupSnapshot` into a fresh Raft-backed hosted runtime; verify the restored read, append after restore, and verify offsets continue from the snapshot state.
- HTTP protocol surface: build Ursula's axum router in-process with deterministic wall time, cover producer retry/fencing, visible snapshot publish/latest/read/bootstrap/delete-conflict semantics, TTL expiry, and selected live-read behavior through real handler parsing and response rendering.
- HTTP randomized protocol surface: build Ursula's axum router in-process with deterministic wall time, choose TTL, producer-session retry, sequence-gap rejection, epoch bump, same-stream concurrent producers, partial non-zero-offset reads, long-poll wakeup, independent long-poll timeout cleanup, SSE close, and live-read waiter-limit branches from the seed, store that plan in the schedule, and verify final HTTP reads plus selected response headers/metrics.

The first checked-in corpus lives at `crates/ursula-sim/corpus/smoke.json`. Each record stores schema version, scenario, seed, stream id, selected fault target if any, committed log index, and trace events. The madsim test suite parses the corpus and replays every record, so drift in seed behavior or trace shape fails locally.

The first schedule layer also exists: `SimSchedule::generate(seed)` deterministically picks one scenario and a bounded abstract fault plan, and `SimScheduledRecord` stores the generated schedule plus observed outcome. A checked-in schedule corpus lives at `crates/ursula-sim/corpus/schedule-smoke.json`. The default madsim smoke test verifies each stored schedule still equals `SimSchedule::generate(seed)` and replays the recorded outcome, so future failures can be captured as schedule input plus trace output. `RUSTFLAGS="--cfg madsim" cargo run -p ursula-sim --bin ursula-sim -- record <seed> [output.json]` writes the same record format for a single seed. `ursula-sim smoke --seed-family runtime-interleaving` runs the first passing runtime interleaving seed family, currently seeds 72 through 96. `ursula-sim smoke --seed-family runtime-raft-engine` runs the first passing Raft-backed runtime family, currently seeds 97 through 101, using single-node OpenRaft behind the runtime API. `ursula-sim smoke --seed-family runtime-raft-network` runs the first passing multi-node Raft-backed runtime family, currently seeds 102 through 106, using a three-node in-process OpenRaft group behind the runtime API. `SimSchedule::generate_runtime_raft_network_recovery(seed)` and `ursula-sim smoke --seed-family runtime-raft-network-recovery` provide the first passing runtime-owned Raft/network recovery family, currently seeds 107 through 111, that partitions a follower during a runtime append, heals it after proving it lags, waits for catch-up, and verifies the committed data remains readable through the runtime API. `SimSchedule::generate_runtime_raft_network_cold_live_recovery(seed)` and `ursula-sim smoke --seed-family runtime-raft-network-cold-live-recovery` provide the first combined runtime/Raft/cold-live recovery family, currently seeds 112 through 116, that performs the same partition/heal recovery and then verifies a cold flush plus cold/live read through the runtime-owned multi-node Raft group. `SimSchedule::generate_runtime_raft_network_cold_live_restart(seed)` and `ursula-sim smoke --seed-family runtime-raft-network-cold-live-restart` provide the first combined runtime/Raft/cold-live restart family, currently seeds 117 through 121, that stops a seeded follower before the cold-flush commit, restarts it from the same log store, waits for catch-up, and then verifies cold/live read. `SimSchedule::generate_runtime_raft_network_cold_live_write_recovery(seed)` and `ursula-sim smoke --seed-family runtime-raft-network-cold-live-write-recovery` provide the first passing runtime/Raft/cold-live write recovery family, currently seeds 317 through 321, that injects a one-shot cold `WriteChunk` failure, records the failed upload/publish counters before retry, retries the runtime-owned Raft cold flush, and verifies cold/live read. Seed 317 is checked into the schedule corpus. `ursula-sim smoke --seed-family leader-failover` provides a lower-level OpenRaft leader-failover family, currently seeds 122 through 126, that stops the current leader, waits for a new leader, appends after failover, restarts the old leader, and verifies no loss/duplication by reading the exact combined payload from all nodes. `SimSchedule::generate_runtime_raft_network_leader_failover(seed)` and `ursula-sim smoke --seed-family runtime-raft-network-leader-failover` provide the runtime-owned leader-failover family, currently seeds 127 through 131, that shuts down the runtime-owned leader engine, installs the newly elected leader engine into the same runtime group, appends after failover through the runtime API, restarts the old leader, waits for catch-up, and verifies the runtime read returns the full post-failover payload. `ursula-sim smoke --seed-family runtime-raft-snapshot-install` provides the runtime/Raft snapshot-install family, currently seeds 132 through 136, that snapshots a runtime-owned three-node Raft group and installs the snapshot into a fresh Raft-backed hosted runtime before verifying restored reads and post-restore appends. `SimSchedule::generate_runtime_interleaving_failure(seed)` and `ursula-sim smoke --seed-family pipeline-smoke-runtime-interleaving-read-corruption --expect-failures` provide an opt-in failure-oriented family, currently seeds 172 through 176, that generates read-verification corruption faults. `SimSchedule::generate_runtime_interleaving_truncate_failure(seed)` and `ursula-sim smoke --seed-family runtime-interleaving-truncate-failures --expect-failures` provide a second opt-in failure-oriented family, currently seeds 182 through 186, that injects real cold-store range-read truncation during runtime interleaving. `SimSchedule::generate_runtime_interleaving_write_failure(seed)` and `ursula-sim smoke --seed-family runtime-interleaving-write-failures --expect-failures` provide a third opt-in failure-oriented family, currently seeds 192 through 196, that injects real cold-store write errors during runtime interleaving. `SimSchedule::generate_raft_partition_failure(seed)` and `ursula-sim smoke --seed-family raft-partition-failures --expect-failures` provide a Raft/network failure family, currently seeds 202 through 206, that keeps an isolated follower partitioned after majority commit and records the catch-up failure. `SimSchedule::generate_runtime_raft_network_partition_failure(seed)` and `ursula-sim smoke --seed-family runtime-raft-network-partition-failures --expect-failures` provide the corresponding runtime-owned Raft/network failure family, currently seeds 212 through 216, that partitions a follower during a runtime append and records `runtime_raft_network_follower_catchup` when the follower remains isolated. `SimSchedule::generate_runtime_raft_network_cold_live_truncate_failure(seed)` and `ursula-sim smoke --seed-family runtime-raft-network-cold-live-truncate-failures --expect-failures` provide the combined runtime/Raft/cold-live read failure family, currently seeds 222 through 226, that heals the Raft partition and then injects a truncated cold range read, recording invariant `runtime_raft_network_cold_live_read_integrity`. `SimSchedule::generate_runtime_raft_snapshot_install_failure(seed)` and `ursula-sim smoke --seed-family runtime-raft-snapshot-install-failures --expect-failures` provide the runtime/Raft snapshot-install failure family, currently seeds 232 through 236, that corrupts snapshot append-count metadata after capture and records invariant `runtime_raft_snapshot_install_integrity` when restored append counts do not continue from the snapshot state. `SimSchedule::generate_runtime_raft_network_cold_live_write_failure(seed)` and `ursula-sim smoke --seed-family runtime-raft-network-cold-live-write-failures --expect-failures` provide the runtime/Raft/cold-live write failure family, currently seeds 312 through 316, that partitions and heals a follower, then injects a one-shot cold `WriteChunk` failure during the runtime-owned Raft cold flush and records invariant `runtime_raft_network_cold_live_write_integrity`. These opt-in families keep CI passing sweeps separate from intentional failure sweeps.

The HTTP producer protocol-surface failure family is `SimSchedule::generate_http_producer_protocol_surface_failure(seed)`, exposed as `ursula-sim smoke --seed-family pipeline-smoke-http-producer-retry-corruption` for seeds 262 through 266. It corrupts the duplicate-retry expectation and records invariant `http_producer_retry_idempotence`, proving that producer retry/idempotence response semantics on the in-process HTTP handler path can be replayed, minimized, and checked as a structured invariant failure. The HTTP live protocol-surface failure family is `SimSchedule::generate_http_live_protocol_surface_failure(seed)`, exposed as `ursula-sim smoke --seed-family pipeline-smoke-http-live-sse-corruption` for seeds 267 through 271. It corrupts the SSE next-offset expectation and records invariant `http_live_sse_delivery`, proving that long-poll wakeup plus SSE delivery can flow through the same failure artifact and minimization path. The HTTP live-limit protocol-surface failure family is `SimSchedule::generate_http_live_limit_protocol_surface_failure(seed)`, exposed as `ursula-sim smoke --seed-family pipeline-smoke-http-live-waiter-corruption` for seeds 272 through 276. It corrupts the expected live-read backpressure metric after the second long-poll is rejected and records invariant `http_live_waiter_backpressure`, covering waiter cleanup plus backpressure accounting on the in-process HTTP/runtime path. The HTTP snapshot protocol-surface failure family is `SimSchedule::generate_http_snapshot_protocol_surface_failure(seed)`, exposed as `ursula-sim smoke --seed-family pipeline-smoke-http-snapshot-body-corruption` for seeds 332 through 336. It corrupts the expected snapshot body after a visible snapshot read and records invariant `http_snapshot_protocol_surface_read`, covering snapshot publish/read response semantics on the in-process HTTP/runtime path.

The first passing randomized HTTP protocol-surface family is `SimSchedule::generate_http_protocol_surface_randomized(seed)`, exposed as `ursula-sim smoke --seed-family http-protocol-surface-randomized` for seeds 277 through 296. Each schedule stores an explicit `HttpProtocolSurfacePlan` with seed-selected booleans for TTL, producer sessions, producer sequence-gap rejection, producer epoch bump, same-stream concurrent producers, partial non-zero-offset reads, long-poll wakeup, independent long-poll timeout cleanup, SSE close, and live-read waiter-limit/backpressure. The family verifies final readable bytes through HTTP and checks selected response headers or metrics for the enabled branches. Seeds 277, 281, and 285 are checked into `crates/ursula-sim/corpus/schedule-smoke.json` so default corpus replay validates the generated schedule/event shape plus combined sequence-gap/concurrent-producer/partial-read and independent live-timeout branches. The paired final-read expected-failure family is `SimSchedule::generate_http_protocol_surface_randomized_failure(seed)`, exposed as `ursula-sim smoke --seed-family pipeline-smoke-http-protocol-surface-randomized-corruption` for seeds 297 through 301. It sets `corrupt_final_read_expectation` inside the generated plan and records invariant `http_protocol_randomized_read_your_write`; the minimizer can remove unrelated HTTP branches while preserving the final-read invariant failure. The randomized SSE expected-failure family is `SimSchedule::generate_http_protocol_surface_randomized_sse_failure(seed)`, exposed as `ursula-sim smoke --seed-family pipeline-smoke-http-protocol-surface-randomized-sse-corruption` for seeds 302 through 306. It forces `sse_close`, corrupts the expected SSE next-offset, records invariant `http_protocol_randomized_sse_delivery`, and minimizes to the SSE branch plus `corrupt_sse_next_offset_expectation`. The randomized backpressure expected-failure family is `SimSchedule::generate_http_protocol_surface_randomized_backpressure_failure(seed)`, exposed as `ursula-sim smoke --seed-family pipeline-smoke-http-protocol-surface-randomized-backpressure-corruption` for seeds 307 through 311. It forces `live_limit`, corrupts the expected live-read backpressure metric, records invariant `http_protocol_randomized_live_waiter_backpressure`, and minimizes to the live-limit branch plus `corrupt_live_limit_backpressure_expectation`.

The first broader runtime/Raft generated family is `SimSchedule::generate_runtime_raft_network_randomized(seed)`, exposed as `ursula-sim smoke --seed-family runtime-raft-network-randomized` for seeds 137 through 156 and `runtime-raft-network-randomized-extended` for seeds 400 through 499. It still uses the same `RuntimeRaftNetwork` runner and invariants, but the seed chooses combinations of partition/heal, runtime-owned leader failover, cold/live flush/read, one-shot cold-write fault plus retry, one-shot cold-write delay, one-shot cold-read truncation plus retry, one-shot cold-read delay, follower restart during cold flush, stream count, append batch lengths, producer-session retries, producer epoch bumps, concurrent producers on the same stream, partial non-zero-offset reads, tail reads at the current next offset, stream close/append-after-close checks, and runtime snapshot publish/read/bootstrap checks. Multi-stream/batch generated workloads now run inside the same passing sweep, including partition/heal and leader-failover combinations. Producer-session seeds append with generated `ProducerRequest` values, retry the same producer sequence with different payloads, assert the retry is deduplicated and returns the original offsets, and then verify the stream payload still matches the first acknowledged append. Epoch-bump seeds then append at the next producer epoch and reject a stale old-epoch write. Concurrent-producer seeds spawn two independent producer ids against the same stream, sort the committed responses by `start_offset`, assert the results are contiguous and not deduplicated, and include the committed bytes in the later read/cold-live/failover invariants. Partial-read seeds verify short reads from non-zero offsets after ordinary runtime/Raft reads and, when selected by the same seed, after leader failover reads and cold/live reads. Tail-read seeds read at the current next offset after ordinary runtime/Raft reads and, when combined by the same seed, after leader failover reads and cold/live reads, then assert the read returns an empty payload without advancing the offset. Close-stream seeds close each stream after the ordinary or failover read path, verify the committed payload remains readable with `closed=true`, reject append-after-close through the runtime API, and when cold/live verification is also selected assert the cold/live read preserves the closed flag. Snapshot-publish seeds publish a snapshot at each stream's committed tail, verify both latest and exact `read_snapshot` return the same offset, content type, body, and next offset, then verify bootstrap exposes the snapshot and tail next offset. Cold-write retry seeds record `runtime_raft_network_cold_write_fault_recovered` before verifying cold/live reads. Cold-write delay seeds install a one-shot simulated `WriteChunk` delay before runtime/Raft cold flush and record `runtime_raft_network_cold_write_delay_verified` after upload/publish still complete and cold/live reads remain consistent. Cold-read retry seeds install a one-shot truncated cold range read, record `runtime_raft_network_cold_read_fault_recovered`, then retry the same cold/live read and verify the committed payload. Cold-read delay seeds install a one-shot simulated `ReadObjectRange` delay before the runtime/Raft cold/live read and record `runtime_raft_network_cold_read_delay_verified` after the delayed read still satisfies the invariant. Seed 139 covers runtime-owned leader failover, cold/live verification with cold-write retry and follower restart during cold flush, producer sessions, producer epoch bumps, concurrent producers, partial reads, and runtime snapshot publish/read/bootstrap. Seed 146 is checked in and covers two streams, producer sessions, concurrent producers, cold/live verification, and a one-shot cold-write delay. Seed 147 is checked in and covers three streams, producer sessions, producer epoch bumps, partial reads, tail reads, cold/live verification, and a one-shot cold-read delay. Seed 137 now combines partition/heal, multi-stream producer sessions, concurrent producers, partial reads, cold-write retry, and cold/live verification in the regular randomized passing sweep and is checked into the schedule corpus. Seed 155 is also checked in and covers runtime-owned leader failover, stopped-leader restart, three streams, producer sessions, producer epoch bumps, concurrent producers, cold/live verification, cold-write retry, and cold-read truncate retry. The opt-in `pipeline-smoke-runtime-raft-network-randomized-read-corruption` family reuses the same randomized schedule generator, then enables a workload-plan read-expectation corruption so the normal runtime/Raft read-your-write invariant fails and writes failure artifacts for replay/minimization. The opt-in `pipeline-smoke-runtime-raft-network-partial-read-corruption` family also reuses the randomized generator, forces partial reads on, and corrupts only the partial-read expectation so invariant `runtime_raft_network_partial_read_integrity` fails through the same non-zero-offset read helper used by passing seeds. The opt-in `pipeline-smoke-runtime-raft-network-tail-read-corruption` family forces tail reads on and corrupts only the expected tail next-offset so invariant `runtime_raft_network_tail_read_empty` fails through the same read-at-next-offset helper used by passing seeds. The opt-in `pipeline-smoke-runtime-raft-network-close-state-corruption` family forces close-stream checks on and corrupts only the expected closed flag so invariant `runtime_raft_network_close_state` fails through the same close/read/append-after-close helper used by passing seeds. The opt-in `pipeline-smoke-runtime-raft-network-snapshot-corruption` family forces snapshot publish/read checks on and corrupts only the expected snapshot payload so invariant `runtime_raft_network_snapshot_publish_read` fails through the same runtime snapshot helper used by passing seeds. The opt-in `pipeline-smoke-runtime-raft-network-leader-failover-read-corruption` family forces runtime-owned leader failover and corrupts only the post-failover read expectation so invariant `runtime_raft_network_leader_failover_no_loss_or_dup` fails after the old leader restarts and catches up. Failover seeds continue from the current producer epoch after the runtime-owned leader has been replaced. An earlier expected-failure family for multi-stream recovery was removed after diagnosis showed the harness was waiting on Ursula's stream state-machine `group_commit_index` as if it were an OpenRaft log/applied index. Batch appends can advance the state-machine commit index by payload item, while OpenRaft advances by log entry. Runtime/Raft catch-up waits now derive the target from the leader's OpenRaft log store `last_log_id`, then use OpenRaft metrics only to wait for followers to apply that log index. Stream commit index remains useful only as stream-layer response metadata. This is the first step beyond one scenario per directed family; it is not yet coverage-guided.

Failure artifacts are split into stable replay data and raw diagnostics. On failure, `ursula-sim smoke` writes a summary JSON with the seed and generated schedule, a stable trace artifact with nondeterministic raw cold-store events filtered out, and a raw event log with the full observed simulator event stream. The stable trace is the comparison surface; the raw event log is for diagnosis. Stable replay also normalizes `RuntimeRaftNetworkReadVerified` metric counters to presence bits, so corpus replay checks the correctness boundary while raw artifacts retain exact diagnostic counts. By default the first failed seed makes `ursula-sim smoke` return an error; with `--expect-failures`, the smoke binary runs every requested seed, writes artifacts for each failure, returns success if at least one failure was observed, and returns an error if no seed failed. `crates/ursula-sim/corpus/failure-smoke.json` stores checked-in minimized failure regressions for the runtime/Raft full-read, partial-read, tail-read, close-state, snapshot publish/read, leader-failover read, runtime/Raft/cold-live truncation, runtime/Raft/cold-live write, runtime interleaving cold-write, runtime/Raft snapshot-install, and HTTP snapshot protocol-surface invariants; the default madsim smoke corpus test and smoke binary both replay those schedules as expected panics and assert the recorded invariant. Failure-corpus records can assert an exact panic string, or stable panic substrings when the full diagnostic message includes intentionally variable details such as cold object names. `RUSTFLAGS="--cfg madsim" cargo run -p ursula-sim --bin ursula-sim -- replay --seed <seed>` reruns one generated schedule, and `--artifact <path>` accepts a failure summary, stable trace artifact, or full scheduled record. Replay can also run expected-panic assertions with `--expect-panic-contains`, `--expect-invariant`, or `--expect-artifact-panic`, so a minimized failure artifact can be checked in CI without treating the intentional panic as a job failure. `ursula-sim minimize --artifact <path>` currently supports `runtime_seeded_interleaving`, `partition_heal`, `runtime_raft_network`, `runtime_raft_snapshot_install`, the HTTP producer/live/live-limit/randomized/snapshot protocol-surface schedules, `cold_live_read`, `cold_read_fault`, `cold_write_fault`, `cold_read_delay`, and `cold_read_truncate` schedules, reporting whether each candidate passed, reproduced a panic, or preserved a target predicate. Supported predicates include panic substring, stable trace exact match, stable trace prefix match, stable `SimEvent` count by serialized event name such as `runtime_interleaving_verified`, `runtime_interleaving_cold_read_delay_verified`, `cold_read_fault_observed`, `cold_write_fault_observed`, `cold_read_delay_verified`, or `cold_read_truncate_observed`, and structured invariant failure by invariant name. The runtime interleaving schedule can also carry a `panic_after` fault that panics after a named stable event; `ursula-sim smoke --runtime-panic-after <event> [--runtime-invariant <name>] [--panic-message <text>]` records that failure through the same summary/stable-trace/raw-event artifact path used for ordinary failures. Runtime interleaving can also inject a read-verification corruption with `--runtime-corrupt-read-client <id>` or through the opt-in `pipeline-smoke-runtime-interleaving-read-corruption` family, which exercises the real read-your-write invariant helper instead of directly injecting a panic. Generated runtime interleaving schedules can also carry `runtime_cold_read_delay_ms`, which installs a one-shot simulated cold range-read delay before final runtime cold/live verification and records `runtime_interleaving_cold_read_delay_verified` when the invariant still passes. The opt-in truncate failure family carries `runtime_cold_read_truncate_len`, which installs a one-shot simulated truncated cold range-read and records the failure as invariant `runtime_interleaving_cold_read_integrity`. The opt-in write failure family carries `runtime_cold_write_failure`, which installs a one-shot simulated cold write error before final runtime cold flush and records the failure as invariant `runtime_interleaving_cold_write_integrity`. The opt-in Raft partition family carries only `partition_seeded_follower`, intentionally omits heal, and records the failure as invariant `raft_partition_follower_catchup`; raw artifacts include partitioned in-process Raft RPC decisions. The passing runtime/Raft network recovery family carries `partition_seeded_follower` plus `heal_seeded_follower`, records `isolated_follower_lagged`, `follower_caught_up`, and `runtime_raft_network_read_verified`, and keeps raw partitioned RPC decisions for diagnosis. The combined runtime/Raft/cold-live recovery family adds `verify_runtime_cold_live_reads`, records `runtime_raft_network_cold_live_read_verified`, and raw artifacts include cold object write/read events as well as partitioned Raft RPC decisions. The combined runtime/Raft/cold-live restart family adds `stop_seeded_follower` and `restart_stopped_follower`, records `node_stopped`, `node_restarted`, and a post-restart `follower_caught_up` at the cold-flush commit index. The combined runtime/Raft/cold-live write recovery family adds `fail_next_cold_write` plus `retry_cold_write_after_failure`, records `runtime_raft_network_cold_write_fault_recovered` with zero uploads/publishes before retry, then records `runtime_raft_network_cold_live_read_verified` after the retried flush publishes cold metadata. The combined runtime/Raft/cold-live truncate failure family adds `truncate_next_cold_read`, records invariant `runtime_raft_network_cold_live_read_integrity`, and raw artifacts include `cold_store_truncate_injected`. The combined runtime/Raft/cold-live write failure family adds `fail_next_cold_write`, records invariant `runtime_raft_network_cold_live_write_integrity`, and uses stable panic substrings because the cold object name is intentionally seed/path dependent. The opt-in runtime/Raft network partition family carries only `partition_seeded_follower`, intentionally omits heal, and records the failure as invariant `runtime_raft_network_follower_catchup` through the runtime-owned append path; raw artifacts include partitioned in-process Raft RPC decisions. The runtime/Raft randomized failure family carries `corrupt_read_expectation` inside the generated workload plan and records invariant `runtime_raft_network_read_your_write` through the same runtime-owned read helper used by passing randomized seeds. The runtime/Raft partial-read failure family carries `partial_reads` plus `corrupt_partial_read_expectation` and records invariant `runtime_raft_network_partial_read_integrity` through the same non-zero-offset partial-read helper used by passing randomized seeds. The runtime/Raft tail-read failure family carries `tail_reads` plus `corrupt_tail_read_expectation` and records invariant `runtime_raft_network_tail_read_empty` through the same read-at-next-offset helper used by passing seeds. The runtime/Raft close-state failure family carries `close_streams` plus `corrupt_close_state_expectation` and records invariant `runtime_raft_network_close_state` through the same close/read/append-after-close helper used by passing seeds. The runtime/Raft snapshot publish/read failure family carries `publish_snapshots` plus `corrupt_snapshot_expectation` and records invariant `runtime_raft_network_snapshot_publish_read` through the same runtime snapshot helper used by passing seeds. The runtime/Raft leader-failover read failure family carries `corrupt_leader_failover_read_expectation` and records invariant `runtime_raft_network_leader_failover_no_loss_or_dup` through the same post-failover read helper used by passing randomized seeds. The runtime/Raft snapshot-install failure family carries `corrupt_runtime_raft_snapshot_append_counts`, records invariant `runtime_raft_snapshot_install_integrity`, and proves the restored runtime would continue stream append-count metadata incorrectly. The HTTP snapshot protocol-surface failure family carries `corrupt_http_snapshot_body_expectation`, records invariant `http_snapshot_protocol_surface_read`, and proves snapshot read response semantics can be replayed and minimized through the in-process axum path. The cold/live scenario can inject a read-verification corruption with `--cold-corrupt-read-node <id>`, which exercises the real cold/live consistency helper while still reading through the Raft runtime and simulated cold store. When an invariant helper fails, the failure trace includes `invariant_failed` with the invariant name, triggering event, and message before the panic. The minimizer runs a greedy shrink loop for runtime interleavings by removing clients, zeroing timing edges, lowering flush group limits, removing or lowering runtime cold-read delay faults, removing or shrinking runtime cold-read truncation faults, removing runtime cold-write failure faults, preserving or remapping corrupt-read fault targets when clients are removed, lowering `corrupt_read_client_id`, removing the corrupt-read fault when the selected target allows it, removing `panic_after` when the target allows it, moving `panic_after.after_event` from `runtime_interleaving_verified` to the earlier `runtime_interleaving_flush_completed` boundary when that still preserves the panic target, and removing synthetic `panic_after.invariant` labels when the target is only the panic payload. For Raft partition and runtime/Raft network partition schedules it can remove the partition, remove an orphan heal, or add the missing heal to prove whether the selected catch-up invariant still reproduces. For runtime/Raft network workload schedules it can now remove cold/live verification and cold-flush restart steps, remove leader failover, disable read-expectation corruption, disable partial-read expectation corruption, disable tail-read expectation corruption, disable close-state expectation corruption, disable leader-failover read expectation corruption, disable partial reads, disable tail reads, disable close-stream checks, disable snapshot publish/read checks, disable producer-session subfeatures, shrink append/failover batches to one item, and collapse multi-stream workloads to a single stream when the selected target remains reproducible. For runtime/Raft snapshot-install schedules it can remove the append-count corruption fault to prove whether `runtime_raft_snapshot_install_integrity` still reproduces. For HTTP snapshot protocol-surface schedules it can remove `corrupt_http_snapshot_body_expectation` to prove whether `http_snapshot_protocol_surface_read` still reproduces. For cold path schedules, including runtime/Raft network schedules carrying a cold fault, it removes cold-store faults or shrinks their parameters while checking whether the selected target is preserved: `cold_live_read` can lower the corrupt read-verification node id, `cold_read_fault` can prove the one-shot read error is required, `cold_write_fault` can prove the one-shot upload error is required, `cold_read_delay` can lower the schedule-driven nonzero virtual delay, and `cold_read_truncate` can lower the schedule-driven `returned_len` used by the simulated truncated range read. For runtime/Raft cold-live write failures, the minimizer proves that removing `fail_next_cold_write` makes the schedule pass and therefore does not preserve invariant `runtime_raft_network_cold_live_write_integrity`; the minimized schedule keeps only `verify_runtime_cold_live_reads` and `fail_next_cold_write`. When the minimized schedule completes successfully, the report includes a `minimized.record` full scheduled record that can be replayed directly with `ursula-sim replay --artifact`; when it still panics, the report includes `minimized.failure`, a failure-summary artifact that can be replayed directly with an expected-panic assertion.

The checked-in failure corpus now also includes the minimized HTTP producer retry/idempotence failure for seed 262, the minimized HTTP live/SSE delivery failure for seed 267, the minimized HTTP live-limit/backpressure failure for seed 272, the minimized HTTP randomized final-read failure for seed 297, the minimized HTTP randomized SSE failure for seed 302, the minimized HTTP randomized backpressure failure for seed 307, the minimized runtime/Raft/cold-live write failure for seed 312, the minimized runtime/Raft randomized cold-read truncation failure for seed 322, the minimized HTTP snapshot read failure for seed 332, and the minimized runtime/Raft tail-read failure for seed 337, the minimized runtime/Raft close-state failure for seed 342, and the minimized runtime/Raft snapshot publish/read failure for seed 347. The CI expected-failure step runs the HTTP protocol-surface failure families plus the runtime/Raft/cold-live write and randomized cold-read failure families, minimizes seed 262, seed 267, seed 272, seed 297, seed 302, seed 307, seed 312, seed 322, seed 332, seed 337, seed 342, and seed 347, asserts that only the corresponding protocol, cold-write, cold-read, tail-read, close-state, or snapshot expectation fault remains, and replays each minimized artifact with `--expect-invariant`.

Minimized failure artifacts are now self-contained. When `ursula-sim minimize` preserves a panic target, `minimized.failure` includes the minimized schedule, the panic string, the minimized stable trace, and the raw event log. Replay uses the schedule and expected invariant or panic to prove the failure still reproduces; CI can inspect the embedded stable trace and raw event log to assert that the minimized artifact contains the final interleaving evidence, such as the ordered leader-failover stage chain and `invariant_failed` event. Full trace equality is intentionally not required for failure artifacts because low-level RPC scheduling details can differ while the seed, schedule, fault, and invariant remain reproducible.

The leader-failover plus cold/live read failure family is `SimSchedule::generate_runtime_raft_network_leader_failover_cold_live_read_failure(seed)`, exposed as `ursula-sim smoke --seed-family runtime-raft-network-leader-failover-cold-live-read-failures --expect-failures` for seeds 327 through 331. It stops the runtime-owned leader, installs the replacement leader, restarts and catches up the old leader, flushes cold data, injects a truncated cold range read, and records invariant `runtime_raft_network_leader_failover_cold_live_read_integrity`. This family is intentionally separate from the generic cold/live read truncation failure because minimization should preserve the leader-failover interleaving when that is the invariant under test.

As with the Phase 1 strict replay probes, individual scenario diagnostics are kept as ignored tests and should be run one at a time. The default madsim suite runs the smoke corpus replay to avoid process-global madsim runtime interference between different scenario tests in one test binary.

This is still not the full DST system: the simulator has bounded runtime actor scenarios, one seed-driven runtime interleaving scenario, single-node plus multi-node Raft-backed runtime scenarios, directed runtime/Raft partition/recovery schedules, an initial runtime/Raft generated-combination family, and a passing generated in-process HTTP protocol family, but does not yet run coverage-guided randomized interleaving sweeps, continuous background workers, production TCP/gRPC, thread-per-core behavior, or passing multi-stream partition/failover recovery. The current value is that the first Raft/state-machine/cold-read/runtime-actor/protocol scenarios can return a uniform `SimReport` and can be promoted into a replayed regression or schedule record.

Deliverable: one deterministic no-fault seed and a few deterministic directed-fault seeds.

### Phase 4 - Fault injection and nightly seed sweeps

Scope: simulator faults plus CI/nightly integration.

Add the first useful fault set:

- network partition/asymmetric drop;
- node pause/crash/restart;
- cold write/read/delete errors;
- snapshot install interruption;
- file-log or failpoint-backed sync errors if file storage is in scope.

Add a nightly job that runs a bounded seed range and persists failing seeds/traces. Per-PR CI should run a small smoke corpus, not an unbounded fuzz sweep.

Current result: initial automation. `.github/workflows/ci.yml` has a `deterministic-simulation` job that runs `scripts/check_dst_failure_guards.py` and `scripts/check_dst_seed_inventory.py` before starting madsim. The failure-guard audit parses `crates/ursula-sim/corpus/failure-smoke.json` and the PR CI workflow, then fails if any checked-in failure seed lacks a fresh minimize command, embedded-trace assertion, or minimized-failure replay guard. The seed-inventory audit parses PR and nightly workflows, checks every workflow family is supported by `ursula-sim smoke`, and requires the expected PR/nightly seed-family and seed-range sets to remain present. The same job runs the checked-in madsim corpus, a small PR seed range, the first four runtime/Raft randomized-combination seeds, the HTTP randomized protocol-surface family, the runtime/Raft cold-live write recovery family, and the `runtime-interleaving` seed family through `ursula-sim smoke`, uploading simulator artifacts on failure. The checked-in corpus now includes `crates/ursula-sim/corpus/failure-smoke.json`; both `smoke_corpus_replays` and the startup path of `ursula-sim smoke` replay those minimized failure regressions as expected panics, so CI validates the stable failure corpus before running fresh seeds. The same CI job now also runs the opt-in `runtime-interleaving-write-failures` family with `--expect-failures`, minimizes seed `192` against invariant `runtime_interleaving_cold_write_integrity`, checks that the minimized schedule keeps one runtime interleaving workload, one client, zero timing delays, flush group limit `1`, and the seeded cold-write fault, then replays the minimized failure artifact with the matching expected invariant. It runs the opt-in `pipeline-smoke-runtime-raft-network-randomized-read-corruption` family with `--expect-failures`, minimizes seed `244` against invariant `runtime_raft_network_read_your_write`, checks that the minimized schedule preserves the panic with only the runtime/Raft workload and `corrupt_read_expectation` fault left, then replays the minimized failure artifact with `ursula-sim replay --expect-invariant`. It also runs `pipeline-smoke-runtime-raft-network-partial-read-corruption`, minimizes seed `248` against invariant `runtime_raft_network_partial_read_integrity`, checks that the minimized schedule keeps only the runtime/Raft workload with `partial_reads` and `corrupt_partial_read_expectation`, then replays that minimized failure artifact with the matching expected invariant. It also runs `pipeline-smoke-runtime-raft-network-tail-read-corruption`, minimizes seed `337` against invariant `runtime_raft_network_tail_read_empty`, checks that the minimized schedule keeps only the runtime/Raft workload with `tail_reads` and `corrupt_tail_read_expectation`, then replays that minimized failure artifact with the matching expected invariant. It also runs `pipeline-smoke-runtime-raft-network-close-state-corruption`, minimizes seed `342` against invariant `runtime_raft_network_close_state`, checks that the minimized schedule keeps only the runtime/Raft workload with `close_streams` and `corrupt_close_state_expectation`, then replays that minimized failure artifact with the matching expected invariant. It also runs `pipeline-smoke-runtime-raft-network-snapshot-corruption`, minimizes seed `347` against invariant `runtime_raft_network_snapshot_publish_read`, checks that the minimized schedule keeps only the runtime/Raft workload with `publish_snapshots` and `corrupt_snapshot_expectation`, then replays that minimized failure artifact with the matching expected invariant. It also runs `pipeline-smoke-runtime-raft-network-leader-failover-read-corruption`, minimizes seed `253` against invariant `runtime_raft_network_leader_failover_no_loss_or_dup`, checks that the minimized schedule keeps the leader-failover steps plus the runtime/Raft workload with `corrupt_leader_failover_read_expectation`, then replays that minimized failure artifact with the matching expected invariant. It also runs `runtime-raft-network-cold-live-truncate-failures`, minimizes seed `222` against invariant `runtime_raft_network_cold_live_read_integrity`, checks that the minimized schedule keeps only cold/live verification and `truncate_next_cold_read`, then replays that minimized failure artifact with the matching expected invariant. It also runs `runtime-raft-network-randomized-cold-read-failures`, minimizes seed `322` against invariant `runtime_raft_network_cold_live_read_integrity`, checks that the minimized schedule keeps only the runtime/Raft workload, cold/live verification, and `truncate_next_cold_read`, then replays that minimized failure artifact with the matching expected invariant. It also runs `runtime-raft-snapshot-install-failures`, minimizes seed `232` against invariant `runtime_raft_snapshot_install_integrity`, checks that the minimized schedule keeps only `corrupt_runtime_raft_snapshot_append_counts`, then replays that minimized failure artifact with the matching expected invariant. It extracts checked-in schedule seed `146`, minimizes it against `runtime_raft_network_cold_write_delay_verified`, checks that the minimized passing schedule keeps one runtime/Raft workload, cold/live verification, and `delay_next_cold_write` shrunk to `1` ms, then replays the minimized scheduled record. It also extracts checked-in schedule seed `147`, minimizes it against `runtime_raft_network_cold_read_delay_verified`, checks that the minimized passing schedule keeps one runtime/Raft workload, cold/live verification, and `delay_next_cold_read` shrunk to `1` ms, then replays the minimized scheduled record. It also runs `runtime-raft-network-cold-live-write-failures`, minimizes seed `312` against invariant `runtime_raft_network_cold_live_write_integrity`, checks that the minimized schedule keeps `verify_runtime_cold_live_reads` plus `fail_next_cold_write`, then replays that minimized failure artifact with the matching expected invariant. The PR artifact upload includes every expected-failure output directory, including the runtime-interleaving cold-write, directed and randomized cold-read, tail-read, close-state, snapshot publish/read, and snapshot-install directories, so failed CI runs retain the seed summary, stable trace, and raw event log for these invariants. `.github/workflows/dst-nightly.yml` runs the same inventory audits and the same expected seed-family set as PR, plus a longer `60..=199` seed range instead of the PR `60..=64` and `137..=140` ranges. The nightly range includes the runtime/Raft randomized-combination seeds 137 through 156, while the explicit nightly families cover the HTTP randomized protocol-surface, runtime interleaving, runtime/Raft cold-live write recovery, the extended runtime/Raft randomized seeds 400 through 499, every PR expected-failure family, and the artifact upload paths for those families. Nightly also runs `scripts/write_dst_seed_report.py` and always uploads `target/ursula-sim-seed-inventory`, which contains JSON and Markdown summaries of PR/nightly seed ranges, seed families, expected-failure families, and failure directories. After the sweep, nightly runs `scripts/write_dst_result_summary.py` and always uploads `target/ursula-sim-result-summary`, which summarizes the actual `seed-*-failure.json` artifacts found under `target` by directory, seed, scenario, invariant, and trace paths. The smoke command sorts and deduplicates requested seeds, so overlapping ranges and families do not rerun the same schedule twice. The smoke sweep caught and fixed one harness bug: follower verification reads were accidentally allowed to fall through to production gRPC leader forwarding or observe an insufficiently quiesced local read. The simulator now uses a `cfg(madsim)` local read helper and bounded local-read polling for invariant checks. Failure artifacts are split into a summary file, a stable replay trace artifact, and a raw event log artifact so replay comparison can stay deterministic while raw cold-store/network events remain available for diagnosis; successful records can still be materialized with `ursula-sim record`. Failure-only boundary events now include applied-index waits, read attempts/satisfaction, heartbeat retries, network policy changes, per-RPC network decisions, delivered RPCs, missing-target RPCs, cold object writes, cold object range reads, cold-store injected faults, cold-store injected delays, cold-store injected truncations, and cold cleanup/delete faults. The remaining artifact gap is lower-level spawned-task event capture and broader protocol-surface event capture.

The PR expected-failure guard also runs the leader-failover cold/live read failure family, minimizes seed `327` against `runtime_raft_network_leader_failover_cold_live_read_integrity`, checks that the minimized schedule keeps the leader-failover steps, one-item runtime/Raft workload, cold/live verification, and `truncate_next_cold_read`, then replays the minimized failure artifact with the matching expected invariant. The seed `327` assertion additionally checks the embedded minimized stable trace for the ordered leader-failover stage chain and invariant event, and checks that `raw_event_log` carries lower-level diagnostic events beyond the stable trace.

Deliverable: reproducible failures by seed, plus a regression corpus checked into the repo when bugs are fixed.

### Phase 5 - Protocol surfaces and richer workloads

Scope: user-visible protocol behavior.

Add multi-client workloads:

- richer producer retry/fencing combinations, including concurrent producers and producer-session behavior through more protocol surfaces;
- long-poll and SSE sessions;
- cold eviction while readers are active;
- snapshot publish/read/delete and latest-snapshot redirect;
- membership changes if supported.

Current result: the first in-process HTTP/axum protocol-surface seeds exist. The base seed uses `router_with_http_state(...)` and an injected `WallClock` to create a TTL stream, append through producer headers, verify duplicate retry/idempotence, verify stale producer epoch fencing, read the committed bytes, publish a visible snapshot, verify latest-snapshot redirect and snapshot read headers/body, verify bootstrap includes the snapshot plus live suffix, verify retained-prefix reads are gone, verify deleting the visible snapshot returns the expected conflict, and advance deterministic wall time until the stream expires. The producer-session seed runs two same-stream producers concurrently, retries one producer sequence with a different payload, verifies sequence-gap rejection, accepts an epoch bump, rejects a stale old epoch, and verifies the final committed bytes through HTTP. The live seed drives a long-poll read from `offset=now`, wakes it with a later append, then opens an SSE tail and closes the stream with a final append, verifying the data event, closed control event, and SSE metrics. The live-limit seed drives a virtual-time long-poll timeout, verifies the waiter is cleaned up, then fills the live-read waiter limit and verifies the HTTP 503/backpressure metrics path. This is useful protocol coverage, but it is still an in-process router/service scenario over the hosted runtime and in-memory group engine. It does not cover real TCP, production gRPC, or full-node process routing.

Continue deciding explicitly whether each new protocol workload should run through HTTP/axum in-process, direct runtime APIs, or both. Keep gRPC/HTTP wire-format coverage explicit instead of assuming the simulator covers it.

### Phase 6 - Long-term investment

Continuous improvements:

- failing-seed minimization;
- schedule trace shrinking;
- coverage-guided workload generation;
- corpus management;
- dashboards for nightly seed counts, failure classes, and runtime.

## Tradeoffs

**madsim vs custom runtime.** madsim reduces the amount of simulator infrastructure Ursula must build, but it brings dependency-shape and ecosystem lock-in. We accept a spike first because a custom runtime/network/storage simulator is much larger than Ursula should write up front.

**HostedTokio vs ThreadPerCore in simulation.** The first simulator should use `HostedTokio`, even though production defaults to thread-per-core. DST is primarily about protocol correctness under schedule/fault interleavings. Thread-per-core behavior remains covered by runtime tests, benchmarks, and EC2 chaos. A later phase can add targeted tests for thread-per-core-specific behavior if needed.

**In-process Raft network vs production gRPC.** The MVP simulator should bypass gRPC to keep the deterministic core small. This means wire-format bugs stay outside the first DST scope. That is acceptable as long as the document and CI matrix say so plainly.

**Memory cold store vs S3 semantics.** Memory plus injected faults catches most Ursula integration mistakes around cold manifests and hot/cold reads. It does not model all S3 behavior. If S3-specific semantics become correctness-critical, add a more faithful object-store simulator later.

**Failpoints vs full DST.** Failpoints are cheaper and should land early, but they do not explore multi-node schedule combinatorics. They are a complement, not a substitute.

**Invariant ceiling.** The simulator only catches bugs the invariants can observe. Every new workload should state which invariant it sharpens.

## Open Questions

- **madsim integration shape.** Should Ursula use madsim package replacement globally, cfg-gated dependency aliases, or a dedicated simulator crate with patched dependencies?
- **OpenRaft support.** Does OpenRaft 0.10.0-alpha.20 with `tokio-rt` run cleanly under madsim, or do we need a different feature/configuration path?
- **tonic build strategy.** Should production gRPC compile under `madsim-tonic`, or should the simulator cfg-exclude production gRPC until a full-node simulation phase?
- **Clock ownership beyond HTTP.** `HttpState` owns the first wall-clock seam. Should runtime-side request builders and cold-store object-name generation also carry a clock handle, or remain explicit inputs/test-only helpers?
- **Cold-store injection API.** Should `ColdStore` expose a test-only `from_operator`, a public builder, or a trait-based factory?
- **CI budget.** Phase 4 must set concrete per-PR and nightly seed counts based on measured simulator speed.

## References

- RisingWave, [Deterministic Simulation: A New Era of Distributed System Testing](https://www.risingwave.com/blog/deterministic-simulation-a-new-era-of-distributed-system-testing/).
- RisingWave source, [`src/tests/simulation/`](https://github.com/risingwavelabs/risingwave/tree/main/src/tests/simulation).
- madsim, [https://github.com/madsim-rs/madsim](https://github.com/madsim-rs/madsim).
- turmoil, [https://docs.rs/turmoil](https://docs.rs/turmoil).
- TigerBeetle, [A Database Without Dynamic Memory Allocation](https://tigerbeetle.com/blog/a-database-without-dynamic-memory-allocation/).
- fail-rs, [https://github.com/tikv/fail-rs](https://github.com/tikv/fail-rs).
- proptest, [https://github.com/proptest-rs/proptest](https://github.com/proptest-rs/proptest).

# Deterministic Simulation Testing

## Objective

Add a deterministic simulation testing (DST) layer to Ursula so that protocol-level correctness can be exercised across millions of fault schedules per night, with bit-exact reproduction from a seed.

DST complements the existing 24/7 EC2 chaos test rather than replacing it. Chaos covers integration, real I/O, and long-running drift; DST covers the combinatorial schedule space — message orderings, fault interleavings, and timing edges that real-time chaos cannot hit by probability alone.

## Motivation

The current chaos runner is a single-fault, clean-failure smoke test. It catches regressions in the bootstrap and recovery paths and validates per-stream setsum durability under one specific failure shape — `aws ec2 stop-instances`. It cannot reach:

- **Network failure shapes.** Asymmetric partitions, packet loss, latency spikes, message reorder, slow-but-alive nodes. Most Raft implementation bugs live here.
- **Schedule coverage.** Bugs that require a specific ordering of two messages plus a timer firing in between. Wall-clock chaos hits these only by luck.
- **Reproducibility.** A failing chaos run gives only logs. There is no way to roll the clock back and replay tick-by-tick, which is the standard prerequisite for fixing distributed bugs.
- **Read path under fault.** Reads, SSE consumers, and cold-tier retrieval are not exercised by the current workload.
- **Exactly-once under contention.** `Producer-Id` / `Producer-Epoch` / `Producer-Seq` dedup is verified only by a single monotonic producer.

DST is the standard companion to chaos for systems with non-trivial protocol surface. FoundationDB, TigerBeetle, and RisingWave all rely on it as the primary correctness lever.

## Current Position

Ursula starts unusually close to DST-ready.

- **Zero behavioral randomness.** The repository contains no `rand::thread_rng`, `Uuid::new_v4`, or `fastrand` calls. All ordering derives from Raft. TigerBeetle spent significant effort to reach this point; Ursula already has it.
- **Raft network is trait-based.** `RaftNetworkV2<UrsulaRaftTypeConfig>` from openraft is the boundary. `crates/ursula-raft/src/grpc.rs` is the production implementation, and `crates/ursula-raft/src/tests.rs` already contains `InProcessRaftNetwork` — an in-memory variant that a simulator can build on directly.
- **Log store is trait-based.** `RaftLogStorage` + `RaftLogReader` have both memory and file implementations.
- **Cold store uses opendal.** Backends are pluggable (memory for local tests, S3 for shared cold storage). Opendal exposes a `Layer` middleware mechanism that can wrap any backend with fault injection without modifying call sites.
- **Stream state machine is pure.** `ursula-stream` contains no time or I/O calls; `apply(cmd)` is a function of state and the command alone.

Two friction points remain.

- **Time is pervasive.** Roughly 78 call sites read `Instant::now()` or `SystemTime::now()`, concentrated in `core_worker.rs` (about 30 for metrics span timing) and `runtime.rs` (cold flush and snapshot deadlines). Most are observability; a minority drive real scheduling decisions.
- **Tokio is wired directly.** `tokio::spawn`, `tokio::sync::{mpsc, oneshot, Semaphore}`, `JoinSet`, and `select!` are used throughout the runtime with no abstraction layer. There is no runtime trait.

The tokio coupling is the central design decision.

## Approach

We propose three layers, listed from cheapest and most independently valuable to most invasive.

### Layer 1 — Property tests on the state machine

`ursula-stream` is a pure state machine. `proptest` can drive `apply(sequence_of_commands)` against the existing manual tests and check invariants on the result (offset monotonicity, dedup correctness, snapshot round-trip). No architectural change required. Catches state machine bugs that targeted unit tests miss.

### Layer 2 — Failpoint injection

Add `fail-rs` (TiKV's `failpoint!` macro) at named injection points in the runtime: before fsync, after Raft commit, mid-snapshot install, before cold flush, around opendal calls. Failpoints are compile-time-guarded — zero runtime cost in release builds. Tests can then trigger specific code paths without restructuring abstractions.

This is the cheapest way to reach failure modes the EC2 chaos test cannot easily reach — for example, "what if the WAL write succeeds but fsync fails."

### Layer 3 — Multi-node deterministic simulator

A real DST framework that runs N virtual Ursula nodes in a single process, with virtualized clock, network, and storage, driven by a deterministic event loop seeded by a single integer.

The remainder of this document is mostly about Layer 3, since it is the largest commitment and most needs design upfront. Layers 1 and 2 can land in any order and yield value immediately.

## Framework Choice

The Rust ecosystem has converged on a small set of options.

| Framework | What it does | Fit for Ursula |
|---|---|---|
| **madsim** | Drop-in `tokio` replacement with deterministic time, scheduling, RNG, network, FS. Same API as tokio. | Primary choice. Used by RisingWave at production scale. Compatible with tonic via `madsim-tonic`. |
| **turmoil** | tokio-rs official. Virtualizes network only; relies on `tokio::time::pause()`. | Insufficient — we need FS and full scheduler virtualization for cold-flush timing. |
| **shuttle** | AWS Labs. PCT-style randomized scheduling to find async race conditions. | Complementary, not alternative. Worth running over `core_worker` actor code. |
| **stateright** | Rust analog to TLA+. Model checker for protocol-level state spaces. | Optional, for verifying subprotocols (leader transfer, membership change) before implementation. |
| **loom** | Exhaustive interleaving of atomics and mutexes. | Out of scope. For lock-free data structures, not full systems. |
| Custom runtime trait | Hand-rolled abstraction over tokio. | Rejected. Roughly a year of work to match what madsim provides today. |

The recommendation is **madsim**, modeled on RisingWave's adoption.

### Why madsim

- Mature, actively maintained, used in a real production database (RisingWave) with public case studies.
- Same author lineage as TiKV — designed with distributed-systems testing as the primary use case, not as a general-purpose runtime.
- Provides `madsim-tonic` for gRPC, which is the most likely blocker for any Rust system using openraft + tonic.
- The integration pattern is `#[cfg(madsim)]` conditional imports. We can opt in per-crate, ship both real and simulated builds, and migrate incrementally.

### Risks of madsim

- **openraft compatibility is unverified.** openraft itself uses tokio internally. We must validate that openraft compiles and runs under `cfg(madsim)` before committing to this path. A short spike (one to two days) on Phase 1 should answer this.
- **Mechanical refactor cost.** Every `use tokio::*` in the workspace must move behind a re-export. This is touch-many-files but logic-preserving. Should be split across several small PRs.
- **Ecosystem lock-in.** Once we depend on madsim, swapping it out later is also a workspace-wide refactor. Mitigated because the re-export trick means non-DST code paths stay on real tokio with zero overhead.

## Architecture

```
                ┌───────────────────────────────────────────┐
                │            Simulator Driver                │
                │   (seed → event loop → tick → tick ...)    │
                └──────┬────────────────┬──────────────┬─────┘
                       │                │              │
        ┌──────────────▼──┐    ┌────────▼──────┐  ┌────▼──────┐
        │  Virtual Clock  │    │ Virtual Net   │  │  Faults   │
        │  (madsim time)  │    │ (delay / loss │  │ (schedule │
        │                 │    │  / partition) │  │  of fault │
        └────────┬────────┘    └──────┬────────┘  │  events)  │
                 │                    │           └─────┬─────┘
        ┌────────▼────────────────────▼─────────────────▼─────┐
        │             N virtual Ursula nodes                    │
        │  ┌──────────┐  ┌──────────┐  ┌──────────┐            │
        │  │ Runtime  │  │ Runtime  │  │ Runtime  │   ...      │
        │  │  + Raft  │  │  + Raft  │  │  + Raft  │            │
        │  │  + Cold  │  │  + Cold  │  │  + Cold  │            │
        │  └──────────┘  └──────────┘  └──────────┘            │
        └───────────────────────────────────────────────────────┘
                                 │
                ┌────────────────▼─────────────────┐
                │    Workload + Invariant checker  │
                └──────────────────────────────────┘
```

The simulator owns the only source of nondeterminism — a single seeded RNG that drives fault injection, network jitter, and any otherwise-random decisions. Time advances only via madsim's virtual clock. All node-to-node traffic flows through the simulated network. All cold storage I/O flows through an opendal `Layer` that can inject faults.

### Runtime abstraction

A new module `crates/ursula-runtime/src/rt.rs` re-exports the runtime primitives:

```rust
#[cfg(madsim)]
pub use madsim::tokio::{spawn, time, sync, select, task};
#[cfg(not(madsim))]
pub use tokio::{spawn, time, sync, select, task};
```

Existing call sites change from `use tokio::sync::mpsc` to `use crate::rt::sync::mpsc`. This is mechanical and reviewable in small chunks.

### Clock

Almost all 78 time call sites become `crate::rt::time::Instant::now()`. Under `cfg(madsim)`, madsim provides a virtual `Instant` that advances only when the simulator drives it. No separate `Clock` trait is needed.

### Network

The existing `InProcessRaftNetwork` is the seed. The simulator extends it with:

```rust
pub struct SimNetwork {
    inboxes: BTreeMap<NodeId, mpsc::Sender<Frame>>,
    faults: NetworkFaults,
    rng: SimRng,
}

pub struct NetworkFaults {
    partitions: Vec<HashSet<NodeId>>,    // bidirectional
    asymmetric: Vec<(NodeId, NodeId)>,   // directed drops
    delay_fn: Box<dyn Fn(NodeId, NodeId) -> Duration>,
    drop_rate: f64,
}
```

Every message decision (drop, delay, deliver in order, reorder) reads from the simulator's seeded RNG. Production gRPC code remains in `crates/ursula-raft/src/grpc.rs` and is excluded under `cfg(madsim)`.

### Cold store

opendal's `Layer` mechanism wraps any backend without changing call sites:

```rust
let op = Operator::new(memory::Builder::default())?
    .layer(FaultLayer::new(sim_clock.clone(), sim_rng.clone()))
    .finish();
```

`FaultLayer` implements opendal's `Layer` trait and injects scheduled faults: HTTP 5xx responses, slow responses bounded by the virtual clock, truncated bodies, simulated IAM expiry. No changes to `ColdStore`.

### Workload and oracle

```rust
trait Workload {
    fn step(&mut self, ctx: &mut SimContext) -> Vec<ClientOp>;
}

trait Invariant {
    fn check(&self, ctx: &SimContext) -> Result<(), Violation>;
}
```

The main loop pulls events from the scheduler, advances the virtual clock to the event time, delivers it, then runs every invariant. Workloads and invariants are independently testable.

## Invariants

The minimum set worth checking on every tick:

1. **Raft safety.** A log entry once committed at index `i` on any node must equal that value at index `i` on every node at every future point in time.
2. **Leader completeness.** In any term, all entries committed by leaders share the same prefix.
3. **Per-stream setsum.** Client-tracked expected setsum equals server live setsum plus eviction; no record lost or duplicated.
4. **Producer-Seq idempotence.** A `(Producer-Id, Producer-Epoch, Producer-Seq)` triple commits at most once, regardless of retry, partition, or failover.
5. **No phantom commits.** Any append the client received a success response for must remain readable forever (potentially from cold tier).
6. **Read-your-write.** Within a single client session, reads after a committed write observe that write.
7. **Cold/live consistency.** Eviction from live to cold does not change total setsum.
8. **Quorum loss behavior.** Under minority partition, the minority side must surface errors rather than acknowledge writes.
9. **Snapshot install.** A node restored from snapshot has the same setsum as the leader at the snapshot's index.

Invariants 1, 3, 4 are minimum viable. The rest can be added incrementally.

## Fault Vocabulary

Each fault is parameterized and schedulable (fires at tick T, clears at tick T+N).

**Network.** `Partition(set_a, set_b)`, `AsymmetricDrop(from, to)`, `Delay(node, ms)`, `MessageLoss(rate)`, `Reorder(window_ms)`, `DuplicateMessage(rate)`.

**Node.** `Pause(node, ms)` to simulate GC, `Crash(node)` + `Restart(node)` preserving log, `SlowDisk(node, factor)`, `ClockSkew(node, offset)`.

**Storage.** `S3Error(rate, code)`, `S3Slow(latency)`, `S3Truncate(rate)`.

**Cluster.** `Membership(add, remove)`, `LeaderHint(node)` for leader transfer.

Schedules are seed-derived. A `FaultSchedule::generate(seed, duration)` produces a deterministic sequence of fault events; CI nightly fuzz iterates seeds and saves failing seeds as regression corpus.

## Phased Delivery

Each phase delivers value standalone. Earlier phases unblock later phases but do not depend on them being fully complete.

### Phase 0 — State machine property tests

Scope: `crates/ursula-stream/src/state_machine/tests.rs`. Add `proptest`. Drive `apply` with generated command sequences. Check offset monotonicity, dedup correctness, snapshot round-trip equivalence.

Estimated effort: 1 week. No architectural change. Yields immediate bug surface.

### Phase 1 — Runtime abstraction + madsim spike

Scope: add `crates/ursula-runtime/src/rt.rs`. Run a one-day spike to compile openraft + tonic under `cfg(madsim)` with `madsim-tonic`. If the spike succeeds, proceed with mechanical `use tokio` → `use crate::rt` migration across the workspace, one crate per PR. Add a CI job that runs the existing test suite under `cfg(madsim)`.

Estimated effort: 2 weeks (1 day spike, then refactor).

The spike is the project's go/no-go decision point. If openraft cannot compile under madsim, the design needs to revisit either the openraft dependency or the runtime abstraction strategy.

### Phase 2 — Simulator core

Scope: new crate `crates/ursula-sim`. Implements `SimContext`, `SimNetwork` (extension of `InProcessRaftNetwork`), `SimColdStore` (opendal memory + `FaultLayer`), invariants 1, 3, 4. Drives a minimal workload (one client, one stream, no faults).

Estimated effort: 3 weeks. First seed is reproducible.

### Phase 3 — Fault injection

Scope: implement the subset of the fault vocabulary that covers immediate Tier 1 gaps — `Partition`, `Crash`, `S3Error`, `Pause`. Wire `FaultSchedule::generate(seed)`. Add nightly CI job running thousands of seeds, persisting failures to a regression corpus.

Estimated effort: 2 weeks.

### Phase 4 — Full invariants and complex workloads

Scope: invariants 2, 5, 6, 7, 8, 9. Multi-client, multi-producer-epoch workloads. Concurrent writes on the same stream. Membership change workloads. Snapshot install workloads.

Estimated effort: 2 weeks.

### Phase 5 — Long-term investment

Coverage-guided fuzzing (cargo-fuzz or LibAFL), failing-seed minimization to shortest reproducing trace, per-PR smoke seeds in CI. Continuous.

## Tradeoffs

**madsim vs custom runtime.** Custom gives full control and zero external dependency. madsim costs us flexibility in async runtime choice for the entire project. We accept the lock-in because the custom path is ~50× the engineering cost and madsim has demonstrated production fit. If madsim becomes unmaintained, the `crate::rt` indirection means migration cost is bounded.

**Mechanical refactor risk.** Renaming `use tokio` workspace-wide is a high-churn change. We mitigate by splitting into per-crate PRs and validating each against the existing test suite before proceeding. Worst case is a temporary code freeze on the affected crate during the transition window.

**gRPC bypassed in simulation.** The simulator uses `InProcessRaftNetwork` and does not exercise tonic wire encoding. Wire-format bugs (proto decoding, framing) remain the responsibility of the EC2 chaos test and unit tests. This is the right split — DST tests protocol, chaos tests integration.

**Sim time vs metrics.** Metrics use elapsed time as a span measurement; under sim time those measurements lose meaning. Simulator runs should disable metrics export or route them to a separate sim-time-aware sink.

**Failpoints vs full DST.** Failpoints (Layer 2) reach many of the same failure modes much more cheaply, but cannot drive multi-node schedule combinatorics. They are complement, not substitute. We deliver both.

**Invariant ceiling.** The simulator only catches bugs the invariants are sharp enough to notice. Incomplete invariants are silent failure. Phase 4 is gated on the invariant set being honest about what it claims to check.

## Open Questions

- **openraft + madsim compatibility.** The Phase 1 spike must answer this before further work is committed. Plan B would be either forking openraft or moving raft behind our own trait.
- **Whether to vendor opendal's S3 backend simulator or build a minimal in-memory replica.** opendal's `services::memory` plus a `FaultLayer` may be sufficient. If S3 semantics (eventual consistency, range request edge cases) become test targets, a more faithful simulator is needed.
- **Whether stateright is worth the additional surface area for verifying leader transfer and membership change subprotocols at design time.** Optional, deferrable.
- **CI budget.** Nightly fuzzing at million-seed scale needs compute; Phase 3 should include a budget estimate and a per-PR smoke target (e.g., 100 seeds per PR, full sweep nightly).

## References

- RisingWave, [Deterministic Simulation: A New Era of Distributed System Testing](https://www.risingwave.com/blog/deterministic-simulation-a-new-era-of-distributed-system-testing/).
- RisingWave source, [`src/tests/simulation/`](https://github.com/risingwavelabs/risingwave/tree/main/src/tests/simulation).
- madsim, [https://github.com/madsim-rs/madsim](https://github.com/madsim-rs/madsim).
- TigerBeetle, [A Database Without Dynamic Memory Allocation](https://tigerbeetle.com/blog/a-database-without-dynamic-memory-allocation/).
- fail-rs, [https://github.com/tikv/fail-rs](https://github.com/tikv/fail-rs).
- proptest, [https://github.com/proptest-rs/proptest](https://github.com/proptest-rs/proptest).

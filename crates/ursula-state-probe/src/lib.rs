//! Bounded-state measurement harness (`docs/architecture/bounded-stream-state.md`
//! §7.1 to §7.3, milestone B0).
//!
//! It drives the real `StreamStateMachine` with the exact commands the runtime
//! issues (L1) and the real `ShardRuntime` on the in-memory and the
//! single-node OpenRaft engines (L2), counts requested heap with a counting
//! allocator, sizes group snapshots with the production codec, and checks the
//! results against a ratchet file in CI.
//!
//! Module map:
//!
//! - [`alloc`]: counting global allocator (live bytes, blocks, big-allocation census).
//! - [`codec`]: snapshot sizes from `ursula_raft::group_snapshot_frames`, per field and per stream.
//! - [`payload`]: deterministic JSON record generators.
//! - [`smx`]: L1 command drivers (append, external append, flush and pack, retention).
//! - [`out`]: JSONL sink, per-checkpoint measurement, and the [`out::Outcome`] metrics and checks.
//! - [`formula`]: the §7.2 per-structure formula checks with each stream's U, K and P.
//! - [`workloads`]: W1 to W6, the planner probe, the F12e snapshot-cadence
//!   driver, and the L2 runtime drivers.
//! - [`gate`]: the per-PR and nightly suites and the ratchet comparison.

pub mod alloc;
pub mod codec;
pub mod formula;
pub mod gate;
pub mod out;
pub mod payload;
pub mod smx;
pub mod workloads;

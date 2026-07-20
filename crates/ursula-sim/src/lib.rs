//! Deterministic simulation harnesses for Ursula.
//!
//! Module map:
//!
//! - `madsim_harness` (private): scenarios, schedules, traces, and fault
//!   plans; its public surface is re-exported at the crate root.
//! - [`artifact`]: shared artifact schemas and helpers for the `ursula-sim`
//!   CLI subcommands.

#[cfg(madsim)]
mod madsim_harness;

#[cfg(madsim)]
pub mod artifact;

#[cfg(madsim)]
pub use madsim_harness::HttpProtocolSurfacePlan;
#[cfg(madsim)]
pub use madsim_harness::LEADER_FAILOVER_SEEDS;
#[cfg(madsim)]
pub use madsim_harness::RAFT_PARTITION_FAILURE_SEEDS;
#[cfg(madsim)]
pub use madsim_harness::RUNTIME_INTERLEAVING_FAILURE_SEEDS;
#[cfg(madsim)]
pub use madsim_harness::RUNTIME_INTERLEAVING_SEEDS;
#[cfg(madsim)]
pub use madsim_harness::RUNTIME_INTERLEAVING_TRUNCATE_FAILURE_SEEDS;
#[cfg(madsim)]
pub use madsim_harness::RUNTIME_INTERLEAVING_WRITE_FAILURE_SEEDS;
#[cfg(madsim)]
pub use madsim_harness::RUNTIME_RAFT_ENGINE_SEEDS;
#[cfg(madsim)]
pub use madsim_harness::RUNTIME_RAFT_NETWORK_COLD_LIVE_RECOVERY_SEEDS;
#[cfg(madsim)]
pub use madsim_harness::RUNTIME_RAFT_NETWORK_COLD_LIVE_RESTART_SEEDS;
#[cfg(madsim)]
pub use madsim_harness::RUNTIME_RAFT_NETWORK_COLD_LIVE_TRUNCATE_FAILURE_SEEDS;
#[cfg(madsim)]
pub use madsim_harness::RUNTIME_RAFT_NETWORK_COLD_LIVE_WRITE_RECOVERY_SEEDS;
#[cfg(madsim)]
pub use madsim_harness::RUNTIME_RAFT_NETWORK_LEADER_FAILOVER_SEEDS;
#[cfg(madsim)]
pub use madsim_harness::RUNTIME_RAFT_NETWORK_PARTITION_FAILURE_SEEDS;
#[cfg(madsim)]
pub use madsim_harness::RUNTIME_RAFT_NETWORK_RANDOMIZED_COLD_READ_FAILURE_SEEDS;
#[cfg(madsim)]
pub use madsim_harness::RUNTIME_RAFT_NETWORK_RANDOMIZED_SEEDS;
#[cfg(madsim)]
pub use madsim_harness::RUNTIME_RAFT_NETWORK_RECOVERY_SEEDS;
#[cfg(madsim)]
pub use madsim_harness::RUNTIME_RAFT_NETWORK_SEEDS;
#[cfg(madsim)]
pub use madsim_harness::RUNTIME_RAFT_SNAPSHOT_INSTALL_FAILURE_SEEDS;
#[cfg(madsim)]
pub use madsim_harness::RUNTIME_RAFT_SNAPSHOT_INSTALL_SEEDS;
#[cfg(madsim)]
pub use madsim_harness::RuntimeInterleavingClient;
#[cfg(madsim)]
pub use madsim_harness::RuntimeInterleavingPanic;
#[cfg(madsim)]
pub use madsim_harness::RuntimeInterleavingPlan;
#[cfg(madsim)]
pub use madsim_harness::RuntimeRaftNetworkWorkloadPlan;
#[cfg(madsim)]
pub use madsim_harness::SIM_REGRESSION_SCHEMA_VERSION;
#[cfg(madsim)]
pub use madsim_harness::SimEvent;
#[cfg(madsim)]
pub use madsim_harness::SimFailureRegressionRecord;
#[cfg(madsim)]
pub use madsim_harness::SimFaultAction;
#[cfg(madsim)]
pub use madsim_harness::SimFaultPlan;
#[cfg(madsim)]
pub use madsim_harness::SimFaultStep;
#[cfg(madsim)]
pub use madsim_harness::SimRegressionRecord;
#[cfg(madsim)]
pub use madsim_harness::SimReport;
#[cfg(madsim)]
pub use madsim_harness::SimScenario;
#[cfg(madsim)]
pub use madsim_harness::SimSchedule;
#[cfg(madsim)]
pub use madsim_harness::SimScheduledRecord;
#[cfg(madsim)]
pub use madsim_harness::SimTrace;
#[cfg(madsim)]
pub use madsim_harness::ThreeNodeRaftSim;
#[cfg(madsim)]
pub use madsim_harness::ThreeNodeRaftSimConfig;
#[cfg(madsim)]
pub use madsim_harness::ThreeNodeRaftSimOutcome;
#[cfg(madsim)]
pub use madsim_harness::stable_replay_outcome;

#[cfg(not(madsim))]
pub struct ThreeNodeRaftSimUnavailable;

//! Deterministic simulation harnesses for Ursula.

#[cfg(madsim)]
mod madsim_harness;

// DoD #3 scaffold: trait surfaces for the Workload / Invariant / Fault axes.
// Empty until the Phase B.1 refactor migrates scenarios out of
// `madsim_harness/mod.rs`; line-budget ratchet lives in the audit at
// `scripts/dst/audits.py::audit_modularity` (invoked via
// `python3 -m scripts.dst modularity`).
#[cfg(madsim)]
pub mod faults;
#[cfg(madsim)]
pub mod invariants;
#[cfg(madsim)]
pub mod scenarios;
#[cfg(madsim)]
pub mod workloads;

#[cfg(madsim)]
pub use madsim_harness::{
    HttpProtocolSurfacePlan, LEADER_FAILOVER_SEEDS, RAFT_PARTITION_FAILURE_SEEDS,
    RUNTIME_INTERLEAVING_FAILURE_SEEDS, RUNTIME_INTERLEAVING_SEEDS,
    RUNTIME_INTERLEAVING_TRUNCATE_FAILURE_SEEDS, RUNTIME_INTERLEAVING_WRITE_FAILURE_SEEDS,
    RUNTIME_RAFT_ENGINE_SEEDS, RUNTIME_RAFT_NETWORK_COLD_LIVE_RECOVERY_SEEDS,
    RUNTIME_RAFT_NETWORK_COLD_LIVE_RESTART_SEEDS,
    RUNTIME_RAFT_NETWORK_COLD_LIVE_TRUNCATE_FAILURE_SEEDS,
    RUNTIME_RAFT_NETWORK_COLD_LIVE_WRITE_RECOVERY_SEEDS,
    RUNTIME_RAFT_NETWORK_LEADER_FAILOVER_SEEDS, RUNTIME_RAFT_NETWORK_PARTITION_FAILURE_SEEDS,
    RUNTIME_RAFT_NETWORK_RANDOMIZED_COLD_READ_FAILURE_SEEDS, RUNTIME_RAFT_NETWORK_RANDOMIZED_SEEDS,
    RUNTIME_RAFT_NETWORK_RECOVERY_SEEDS, RUNTIME_RAFT_NETWORK_SEEDS,
    RUNTIME_RAFT_SNAPSHOT_INSTALL_FAILURE_SEEDS, RUNTIME_RAFT_SNAPSHOT_INSTALL_SEEDS,
    RuntimeInterleavingClient, RuntimeInterleavingPanic, RuntimeInterleavingPlan,
    RuntimeRaftNetworkWorkloadPlan, SIM_REGRESSION_SCHEMA_VERSION, SimEvent,
    SimFailureRegressionRecord, SimFaultAction, SimFaultPlan, SimFaultStep, SimRegressionRecord,
    SimReport, SimScenario, SimSchedule, SimScheduledRecord, SimTrace, ThreeNodeRaftSim,
    ThreeNodeRaftSimConfig, ThreeNodeRaftSimOutcome, stable_replay_outcome,
};

#[cfg(not(madsim))]
pub struct ThreeNodeRaftSimUnavailable;

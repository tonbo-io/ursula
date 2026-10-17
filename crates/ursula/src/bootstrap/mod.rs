//! Process-level orchestration: typed-config `ShardRuntime` constructors and
//! raft-related background workers.
//!
//! This module is responsible for:
//! - validating configuration,
//! - constructing the [`ShardRuntime`]
//! - spawning raft-related background workers.
//!
//! Runtime internals (engine factories, cold-store handles, etc.) do not leak
//! here; they are assembled by the caller and passed to the builder.

mod cold_health;
mod commit_stall;
mod egress;
mod format_epoch;
mod health_gate;
mod leadership;
mod participation;
mod peer_address;
mod runtime;
mod snapshot;
mod store_health;
mod topology;
mod util;
mod wal_disk;

pub(crate) use format_epoch::check_and_stamp_format_epoch;
pub(crate) use leadership::handoff_shutdown_leadership;
pub use peer_address::PeerAddressChanged;
pub(crate) use peer_address::check_own_recorded_address;
pub use runtime::SpawnRuntimeError;
pub use runtime::SpawnedRuntime;
pub use runtime::spawn_runtime;
pub(crate) use runtime::spawn_runtime_with_maintenance_drain;
pub use snapshot::SnapshotDriveSettings;
pub use snapshot::SnapshotDriver;
// Internals the crate's unit tests exercise directly.
#[cfg(test)]
pub(crate) use test_reexports::*;
pub use topology::Persistence;
pub use topology::Topology;
pub(crate) use wal_disk::initialize_wal_disk_monitor;
pub(crate) use wal_disk::spawn_wal_disk_gate;
#[cfg(test)]
mod test_reexports {
    pub(crate) use super::cold_health::ColdHealthDecision;
    pub(crate) use super::cold_health::ColdHealthSample;
    pub(crate) use super::cold_health::ColdHealthTracker;
    pub(crate) use super::cold_health::spawn_cold_health_gate;
    pub(crate) use super::commit_stall::CommitStallAction;
    pub(crate) use super::commit_stall::CommitStallTracker;
    pub(crate) use super::egress::ClusterEgressProbeScope;
    pub(crate) use super::egress::cluster_egress_probe_groups;
    pub(crate) use super::egress::spawn_egress_gate;
    pub(crate) use super::health_gate::LeadershipShed;
    pub(crate) use super::health_gate::ShedDrain;
    pub(crate) use super::health_gate::ShedHandoff;
    pub(crate) use super::health_gate::ShedPace;
    pub(crate) use super::health_gate::ShedPlan;
    pub(crate) use super::health_gate::ShedTargets;
    pub(crate) use super::leadership::plan_leadership_balance;
    pub(crate) use super::leadership::plan_leadership_balance_with_eligible_nodes;
    pub(crate) use super::participation::PeerParticipation;
    pub(crate) use super::snapshot::ScrubPurge;
    pub(crate) use super::snapshot::StoreGateInputs;
    pub(crate) use super::snapshot::Trigger;
    pub(crate) use super::snapshot::deleted_data_sample;
    pub(crate) use super::snapshot::group_log_progress;
    pub(crate) use super::snapshot::groups_to_drive;
    pub(crate) use super::snapshot::plan_scrub_purges;
    pub(crate) use super::snapshot::plan_snapshot_drive;
    pub(crate) use super::snapshot::resolve_snapshot_drive_interval_ms;
    pub(crate) use super::snapshot::spawn_snapshot_driver;
    pub(crate) use super::snapshot::trigger_snapshot_build;
    pub(crate) use super::util::leader_counts;
    pub(crate) use super::util::prioritized_transfer_targets;
}

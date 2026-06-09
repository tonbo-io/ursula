//! Process-level orchestration: env-driven `ShardRuntime` constructors and
//! raft-related background workers.
//!
//! This module is responsible for:
//! - reading process environment variables,
//! - validating configuration,
//! - constructing the [`ShardRuntime`]
//! - spawning raft-related background workers.
//!
//! Runtime internals (engine factories, cold-store handles, etc.) do not leak
//! here; they are assembled by the caller and passed to the builder.

mod cold_health;
mod commit_stall;
mod egress;
mod leadership;
mod runtime;
mod snapshot;
mod topology;
mod util;

pub use runtime::SpawnedRuntime;
pub use runtime::spawn_runtime;
pub use topology::Persistence;
pub use topology::Topology;

// Re-export test-visible internals so existing tests don't break.
#[allow(unused_imports)]
mod test_reexports {
    pub(crate) use super::cold_health::ColdHealthDecision;
    pub(crate) use super::cold_health::ColdHealthSample;
    pub(crate) use super::cold_health::ColdHealthTracker;
    pub(crate) use super::commit_stall::CommitStallAction;
    pub(crate) use super::commit_stall::CommitStallTracker;
    pub(crate) use super::egress::ClusterEgressProbeScope;
    pub(crate) use super::egress::ClusterEgressShedAction;
    pub(crate) use super::egress::cluster_egress_probe_groups;
    pub(crate) use super::egress::plan_cluster_egress_shed;
    pub(crate) use super::egress::spawn_egress_gate;
    pub(crate) use super::leadership::LeadershipBalanceAction;
    pub(crate) use super::leadership::plan_leadership_balance;
    pub(crate) use super::leadership::plan_leadership_balance_with_eligible_nodes;
    pub(crate) use super::snapshot::next_snapshot_to_drive;
    pub(crate) use super::snapshot::resolve_snapshot_drive_interval_ms;
    pub(crate) use super::snapshot::should_drive_snapshot_for_group;
    pub(crate) use super::util::leader_counts;
    pub(crate) use super::util::prioritized_transfer_targets;
    pub(crate) use super::util::reenable_elections_if_campaign_allowed;
}
#[allow(unused_imports)]
pub(crate) use test_reexports::*;

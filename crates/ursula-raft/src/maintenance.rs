//! Local Raft eligibility for Kubernetes readiness and maintenance tooling.
//!
//! Expected inventory comes from static configuration or a validated managed
//! placement projection, never from observed groups. This local eligibility
//! check does not reserve permission for cluster-wide disruption.

use std::collections::BTreeMap;
use std::collections::BTreeSet;

use serde::Deserialize;
use serde::Serialize;

use crate::types::RaftGroupMetricsSnapshot;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RaftMaintenanceIssue {
    EmptyExpectedInventory,
    MissingGroup,
    UnexpectedGroup,
    DuplicateGroup,
    WrongNodeIdentity,
    RaftStopped,
    RecoveryBarrier,
    StoppedForOperator,
    JointMembership,
    IncompleteVoterSet,
    MembershipNotApplied,
    LeaderUnknown,
    LeaderOutsideVoters,
    NotApplied,
    ApplyLag,
    ManagedMigration,
    ReceiverUncertain,
    AssignmentDrift,
}

/// Local assignment role, including actors which exist before placement publish.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ManagedReplicaRole {
    Voter,
    PreparingLearner,
    Learner,
    Retiring,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManagedRaftInventory {
    pub applied_meta_index: u64,
    pub active_migration_id: Option<u64>,
    pub replica_roles: BTreeMap<u32, ManagedReplicaRole>,
    pub receiver_fenced: bool,
    pub receiver_pending: bool,
    pub assignment_drift: bool,
    /// Local serving/registration eligibility, separate from disruption gates.
    pub serving_ready: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RaftMaintenanceReport {
    pub version: u32,
    pub node_id: u64,
    pub lag_tolerance: u64,
    pub expected_groups: BTreeMap<u32, BTreeSet<u64>>,
    pub node_issues: Vec<RaftMaintenanceIssue>,
    pub group_issues: BTreeMap<u32, Vec<RaftMaintenanceIssue>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub managed_inventory: Option<ManagedRaftInventory>,
}

impl RaftMaintenanceReport {
    pub fn serving_ready(&self) -> bool {
        if self.version == 3 {
            return self
                .managed_inventory
                .as_ref()
                .is_some_and(|inventory| inventory.serving_ready && !inventory.assignment_drift);
        }
        self.ready()
    }

    pub fn ready(&self) -> bool {
        (match self.version {
            1 => !self.expected_groups.is_empty() && self.managed_inventory.is_none(),
            2 => self.managed_inventory.is_none(),
            3 => self.managed_inventory.as_ref().is_some_and(|inventory| {
                inventory.active_migration_id.is_none()
                    && !inventory.receiver_fenced
                    && !inventory.receiver_pending
                    && !inventory.assignment_drift
                    && inventory
                        .replica_roles
                        .keys()
                        .eq(self.expected_groups.keys())
                    && inventory
                        .replica_roles
                        .values()
                        .all(|role| *role == ManagedReplicaRole::Voter)
            }),
            _ => false,
        }) && self.node_issues.is_empty()
            && self.group_issues.is_empty()
    }
}

/// Require every expected local replica to be running, recovered, a member of
/// the complete uniform voter set, and applied through its membership entry.
/// Bound application lag and require a leader belonging to that voter set.
pub fn check_raft_maintenance(
    groups: &[RaftGroupMetricsSnapshot],
    node_id: u64,
    expected_groups: BTreeMap<u32, BTreeSet<u64>>,
    lag_tolerance: u64,
) -> RaftMaintenanceReport {
    check_expected(groups, node_id, expected_groups, lag_tolerance, 1)
}

/// Managed inventory comes from a complete validated placement projection.
/// An explicitly unassigned node may be locally ready with zero replicas;
/// missing assigned replicas and unexpected resident replicas still fail closed.
/// Version 2 distinguishes this contract from legacy static inventory.
pub fn check_managed_raft_maintenance(
    groups: &[RaftGroupMetricsSnapshot],
    node_id: u64,
    expected_groups: BTreeMap<u32, BTreeSet<u64>>,
    lag_tolerance: u64,
) -> RaftMaintenanceReport {
    check_expected(groups, node_id, expected_groups, lag_tolerance, 2)
}

/// Version 3 distinguishes transient learner/retiring assignments from voters.
/// Participation during an active operation is diagnostic, never disruption
/// permission or a substitute for a group's native quorum read.
pub fn check_managed_raft_inventory(
    groups: &[RaftGroupMetricsSnapshot],
    node_id: u64,
    expected_groups: BTreeMap<u32, BTreeSet<u64>>,
    lag_tolerance: u64,
    inventory: ManagedRaftInventory,
) -> RaftMaintenanceReport {
    let mut report = check_expected(groups, node_id, expected_groups, lag_tolerance, 3);
    if inventory.active_migration_id.is_some() {
        report
            .node_issues
            .push(RaftMaintenanceIssue::ManagedMigration);
    }
    if inventory.receiver_fenced || inventory.receiver_pending {
        report
            .node_issues
            .push(RaftMaintenanceIssue::ReceiverUncertain);
    }
    if inventory.assignment_drift {
        report
            .node_issues
            .push(RaftMaintenanceIssue::AssignmentDrift);
    }
    report.managed_inventory = Some(inventory);
    report
}

fn check_expected(
    groups: &[RaftGroupMetricsSnapshot],
    node_id: u64,
    expected_groups: BTreeMap<u32, BTreeSet<u64>>,
    lag_tolerance: u64,
    version: u32,
) -> RaftMaintenanceReport {
    use RaftMaintenanceIssue as Issue;

    let mut report = RaftMaintenanceReport {
        version,
        node_id,
        lag_tolerance,
        expected_groups,
        node_issues: Vec::new(),
        group_issues: BTreeMap::new(),
        managed_inventory: None,
    };
    if version == 1 && report.expected_groups.is_empty() {
        report.node_issues.push(Issue::EmptyExpectedInventory);
    }
    if node_id == 0 {
        report.node_issues.push(Issue::WrongNodeIdentity);
    }
    let mut seen = BTreeSet::new();
    for group in groups {
        let mut issues = Vec::new();
        if !seen.insert(group.raft_group_id) {
            issues.push(Issue::DuplicateGroup);
        }
        let Some(expected) = report.expected_groups.get(&group.raft_group_id) else {
            issues.push(Issue::UnexpectedGroup);
            report
                .group_issues
                .entry(group.raft_group_id)
                .or_default()
                .extend(issues);
            continue;
        };
        if group.node_id != node_id {
            issues.push(Issue::WrongNodeIdentity);
        }
        if !group.maintenance.running {
            issues.push(Issue::RaftStopped);
        }
        if !group.maintenance.recovery_ready {
            issues.push(Issue::RecoveryBarrier);
        }
        if group.maintenance.stopped_for_operator {
            issues.push(Issue::StoppedForOperator);
        }
        if group.maintenance.membership_joint {
            issues.push(Issue::JointMembership);
        }
        let voters = group.voter_ids.iter().copied().collect::<BTreeSet<_>>();
        if &voters != expected || !voters.contains(&node_id) || !group.learner_ids.is_empty() {
            issues.push(Issue::IncompleteVoterSet);
        }
        let applied = group.last_applied.map(|id| id.index);
        if group.maintenance.membership_log_index.is_none()
            || applied < group.maintenance.membership_log_index
        {
            issues.push(Issue::MembershipNotApplied);
        }
        match group.current_leader {
            None => issues.push(Issue::LeaderUnknown),
            Some(leader) if !expected.contains(&leader) => issues.push(Issue::LeaderOutsideVoters),
            Some(_) => {}
        }
        match (group.committed, applied) {
            (Some(committed), Some(applied))
                if committed.index.saturating_sub(applied) > lag_tolerance =>
            {
                issues.push(Issue::ApplyLag)
            }
            (Some(_), Some(_)) => {}
            _ => issues.push(Issue::NotApplied),
        }
        if !issues.is_empty() {
            report
                .group_issues
                .entry(group.raft_group_id)
                .or_default()
                .extend(issues);
        }
    }
    for group in report.expected_groups.keys() {
        if !seen.contains(group) {
            report
                .group_issues
                .entry(*group)
                .or_default()
                .push(Issue::MissingGroup);
        }
    }
    report
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::RaftGroupMaintenanceState;
    use crate::RaftLogProgressSnapshot;

    fn healthy(id: u32) -> RaftGroupMetricsSnapshot {
        let progress = RaftLogProgressSnapshot { term: 1, index: 20 };
        RaftGroupMetricsSnapshot {
            raft_group_id: id,
            node_id: 1,
            current_term: 1,
            current_leader: Some(2),
            last_log_index: Some(20),
            committed: Some(progress),
            last_applied: Some(progress),
            snapshot: None,
            purged: None,
            voter_ids: vec![1, 2, 3],
            learner_ids: vec![],
            maintenance: RaftGroupMaintenanceState {
                running: true,
                recovery_ready: true,
                accepting_transfers: true,
                membership_joint: false,
                membership_log_index: Some(10),
                stopped_for_operator: false,
            },
            log: Default::default(),
        }
    }

    fn report(groups: &[RaftGroupMetricsSnapshot]) -> RaftMaintenanceReport {
        check_raft_maintenance(
            groups,
            1,
            BTreeMap::from([
                (0, BTreeSet::from([1, 2, 3])),
                (1, BTreeSet::from([1, 2, 3])),
            ]),
            16,
        )
    }

    #[test]
    fn inventory_is_configuration_backed_even_if_every_observation_omits_a_group() {
        assert!(report(&[healthy(0), healthy(1)]).ready());
        let missing = report(&[healthy(0)]);
        assert!(!missing.ready());
        assert_eq!(missing.group_issues[&1], vec![
            RaftMaintenanceIssue::MissingGroup
        ]);
        assert!(!report(&[]).ready());
        assert!(!check_raft_maintenance(&[], 1, BTreeMap::new(), 16).ready());
    }

    #[test]
    fn managed_empty_assignment_is_ready_only_without_resident_replicas() {
        let idle = check_managed_raft_maintenance(&[], 1, BTreeMap::new(), 16);
        assert!(idle.ready());
        assert_eq!(idle.version, 2);
        assert!(
            check_managed_raft_maintenance(&[], 0, BTreeMap::new(), 16)
                .node_issues
                .contains(&RaftMaintenanceIssue::WrongNodeIdentity)
        );
        let unexpected = check_managed_raft_maintenance(&[healthy(0)], 1, BTreeMap::new(), 16);
        assert!(!unexpected.ready());
        assert_eq!(unexpected.group_issues[&0], vec![
            RaftMaintenanceIssue::UnexpectedGroup
        ]);
        let mut unknown = idle;
        unknown.version = 3;
        assert!(!unknown.ready());
    }

    #[test]
    fn managed_inventory_requires_complete_rf5_membership_and_all_assigned_groups() {
        let expected = BTreeMap::from([(0, BTreeSet::from([1, 2, 3, 4, 5]))]);
        let incomplete = check_managed_raft_maintenance(&[healthy(0)], 1, expected.clone(), 16);
        assert!(!incomplete.ready());
        assert!(incomplete.group_issues[&0].contains(&RaftMaintenanceIssue::IncompleteVoterSet));
        let mut group = healthy(0);
        group.voter_ids = vec![1, 2, 3, 4, 5];
        assert!(check_managed_raft_maintenance(&[group], 1, expected.clone(), 16).ready());
        assert_eq!(
            check_managed_raft_maintenance(&[], 1, expected, 16).group_issues[&0],
            vec![RaftMaintenanceIssue::MissingGroup]
        );
    }

    #[test]
    fn managed_v3_transient_roles_and_unresolved_receivers_never_certify_readiness() {
        let expected = BTreeMap::from([(0, BTreeSet::from([1, 2, 3]))]);
        let settled = ManagedRaftInventory {
            applied_meta_index: 7,
            active_migration_id: None,
            replica_roles: BTreeMap::from([(0, ManagedReplicaRole::Voter)]),
            receiver_fenced: false,
            receiver_pending: false,
            assignment_drift: false,
            serving_ready: true,
        };
        assert!(
            check_managed_raft_inventory(&[healthy(0)], 1, expected.clone(), 16, settled.clone())
                .ready()
        );
        for role in [
            ManagedReplicaRole::PreparingLearner,
            ManagedReplicaRole::Learner,
            ManagedReplicaRole::Retiring,
        ] {
            let mut transient = settled.clone();
            transient.replica_roles.insert(0, role);
            assert!(
                !check_managed_raft_inventory(&[healthy(0)], 1, expected.clone(), 16, transient)
                    .ready()
            );
        }
        for reason in ["migration", "pending", "fenced", "drift"] {
            let mut blocked = settled.clone();
            match reason {
                "migration" => blocked.active_migration_id = Some(1),
                "pending" => blocked.receiver_pending = true,
                "fenced" => blocked.receiver_fenced = true,
                "drift" => blocked.assignment_drift = true,
                _ => unreachable!(),
            }
            assert!(
                !check_managed_raft_inventory(&[healthy(0)], 1, expected.clone(), 16, blocked)
                    .ready()
            );
        }
        let mut idle = settled;
        idle.replica_roles.clear();
        let idle = check_managed_raft_inventory(&[], 1, BTreeMap::new(), 16, idle);
        assert!(idle.ready());
        let mut unknown = idle;
        unknown.version = 4;
        assert!(!unknown.ready());
    }

    #[test]
    fn a_new_ready_process_cannot_replace_a_voter_before_promotion_is_applied() {
        let mut new = healthy(1);
        new.voter_ids = vec![2, 3];
        new.learner_ids = vec![1];
        assert!(
            report(&[healthy(0), new.clone()]).group_issues[&1]
                .contains(&RaftMaintenanceIssue::IncompleteVoterSet)
        );
        new.voter_ids = vec![1, 2, 3];
        new.learner_ids.clear();
        new.maintenance.membership_log_index = Some(21);
        assert!(
            report(&[healthy(0), new.clone()]).group_issues[&1]
                .contains(&RaftMaintenanceIssue::MembershipNotApplied)
        );
        new.last_applied = Some(RaftLogProgressSnapshot { term: 1, index: 21 });
        assert!(report(&[healthy(0), new]).ready());
    }

    #[test]
    fn stopped_recovering_joint_and_operator_stopped_replicas_are_ineligible() {
        for maintenance in [
            RaftGroupMaintenanceState {
                running: false,
                ..healthy(1).maintenance
            },
            RaftGroupMaintenanceState {
                recovery_ready: false,
                ..healthy(1).maintenance
            },
            RaftGroupMaintenanceState {
                membership_joint: true,
                ..healthy(1).maintenance
            },
            RaftGroupMaintenanceState {
                stopped_for_operator: true,
                ..healthy(1).maintenance
            },
            RaftGroupMaintenanceState::default(),
        ] {
            let mut group = healthy(1);
            group.maintenance = maintenance;
            assert!(!report(&[healthy(0), group]).ready());
        }
    }

    #[test]
    fn a_missing_leader_or_large_apply_gap_prevents_maintenance() {
        let mut group = healthy(1);
        group.current_leader = None;
        assert!(!report(&[healthy(0), group.clone()]).ready());
        group.current_leader = Some(4);
        assert!(!report(&[healthy(0), group.clone()]).ready());
        group.current_leader = Some(2);
        group.committed = Some(RaftLogProgressSnapshot { term: 1, index: 37 });
        assert!(!report(&[healthy(0), group.clone()]).ready());
        group.committed = Some(RaftLogProgressSnapshot { term: 1, index: 36 });
        assert!(report(&[healthy(0), group]).ready());
    }

    #[test]
    fn duplicate_foreign_and_wrong_identity_observations_do_not_count_as_redundancy() {
        assert!(!report(&[healthy(0), healthy(1), healthy(1)]).ready());
        assert!(!report(&[healthy(0), healthy(1), healthy(2)]).ready());
        let mut wrong = healthy(1);
        wrong.node_id = 2;
        assert!(!report(&[healthy(0), wrong]).ready());
    }
}

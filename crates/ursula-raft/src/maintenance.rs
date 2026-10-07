//! Local Raft eligibility for Kubernetes readiness and maintenance tooling.
//!
//! The expected inventory comes from configuration, never from the observed
//! groups. This is a local eligibility check; cluster-wide disruption control
//! must also observe every configured peer and serialize maintenance operations.

use std::collections::BTreeMap;
use std::collections::BTreeSet;

pub use ursula_proto::admin::RaftMaintenanceIssue;
pub use ursula_proto::admin::RaftMaintenanceReport;

use crate::types::RaftGroupMetricsSnapshot;

/// Require every expected local replica to be running, recovered, a member of
/// the complete uniform voter set, and applied through its membership entry.
/// Bound application lag and require a leader belonging to that voter set.
pub fn check_raft_maintenance(
    groups: &[RaftGroupMetricsSnapshot],
    node_id: u64,
    expected_groups: BTreeMap<u32, BTreeSet<u64>>,
    lag_tolerance: u64,
) -> RaftMaintenanceReport {
    use RaftMaintenanceIssue as Issue;

    let mut report = RaftMaintenanceReport {
        version: 1,
        node_id,
        lag_tolerance,
        expected_groups,
        node_issues: Vec::new(),
        group_issues: BTreeMap::new(),
    };
    if report.expected_groups.is_empty() {
        report.node_issues.push(Issue::EmptyExpectedInventory);
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
        if !voters.contains(&node_id) {
            issues.push(Issue::LocalReplicaNotVoter);
        }
        if &voters != expected || !group.learner_ids.is_empty() {
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
            installed_replica_identities: Default::default(),
            apply_failure: None,
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

    #[test]
    fn serving_keeps_survivors_but_excludes_the_rebuilding_learner() {
        let mut survivor = healthy(0);
        survivor.voter_ids = vec![1, 2];
        survivor.learner_ids = vec![3];
        let expected = BTreeMap::from([(0, BTreeSet::from([1, 2, 3]))]);
        let report = check_raft_maintenance(&[survivor.clone()], 1, expected.clone(), 16);
        assert!(report.serving_ready());
        assert!(!report.ready());
        survivor.node_id = 3;
        let rebuilding = check_raft_maintenance(&[survivor], 3, expected, 16);
        assert!(!rebuilding.serving_ready());
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

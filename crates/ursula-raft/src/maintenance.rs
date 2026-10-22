//! Local Raft eligibility for Kubernetes readiness and maintenance tooling
//! (design 10).
//!
//! [`check_raft_maintenance`] is one pure decision over typed inputs: the
//! node's data replicas, whether each counts as a voter by the one voter
//! predicate ([`RaftGroupHandleRegistry::counts_as_voter`]), and what the
//! node knows of its placement ([`NodePlacement`]). The inventory comes
//! from the node's bootstrapped view, never from the observed groups, and
//! the voter checks from each replica's effective membership. A replica
//! that does not count as a voter is checked only for being stopped.
//! [`raft_maintenance_report`] gathers the inputs on a node.
//!
//! This is a local eligibility check. Cluster-wide disruption control must
//! also observe every peer and serialize maintenance operations.

use std::collections::BTreeSet;
use std::time::Duration;

use ursula_control::Cluster;
use ursula_control::ClusterView;
use ursula_control::Counts;
use ursula_control::GroupPlacement;
use ursula_control::MetaReplicaStatus;
use ursula_control::PlacementStatus;
use ursula_control::RegistrationStatus;
use ursula_control::Standing;
use ursula_proto::admin::NodePlacementReport;
pub use ursula_proto::admin::RaftMaintenanceIssue;
pub use ursula_proto::admin::RaftMaintenanceReport;
use ursula_proto::placement::ExpectedReplica;
use ursula_proto::placement::GenesisClaim;
use ursula_proto::placement::NodeId;
use ursula_proto::placement::Registered;
use ursula_proto::placement::ReplicaRole;
use ursula_proto::placement::UnplacedReplica;
use ursula_shard::RaftGroupId;

use crate::HostingStatus;
use crate::RaftGroupHandleRegistry;
use crate::meta::status::MetaStatusReader;
use crate::types::RaftGroupMetricsSnapshot;

/// How far a replica's applied index may trail its committed one.
pub const LAG_TOLERANCE: u64 = 16;

/// How long a node may know no meta leader before it reports
/// `meta_unavailable`: the window after which it also registers again.
pub const META_UNAVAILABLE_AFTER: Duration = crate::meta::registration::REREGISTER_AFTER;

/// How long the leader of a moving group may see its reconciler make no
/// progress before it reports `move_stalled` (design 11.4).
pub const MOVE_STALLED_AFTER: Duration = Duration::from_secs(5 * 60);

/// What a node's maintenance report reads of its placement.
#[derive(Debug, Clone, Copy)]
pub struct NodePlacement<'a> {
    /// The node's placement status, from its latest view.
    pub status: &'a PlacementStatus,
    /// The node's genesis claim: its configured group count and effective
    /// replication factor.
    pub claim: &'a GenesisClaim,
    /// How long the node has known no meta leader, `None` while it knows
    /// one.
    pub leaderless_for: Option<Duration>,
    /// The hosting driver's last status.
    pub hosting: &'a HostingStatus,
    /// The node's standing in this run, its registration's answers
    /// included ([`RaftGroupHandleRegistry::learn_standing`]).
    pub standing: Standing,
}

/// The inputs of [`check_raft_maintenance`].
#[derive(Debug, Clone, Copy)]
pub struct MaintenanceInputs<'a, C> {
    pub node_id: u64,
    pub lag_tolerance: u64,
    /// The wall clock the reconcile statuses' times are read against.
    pub now_ms: u64,
    /// The node's data replicas' metrics.
    pub groups: &'a [RaftGroupMetricsSnapshot],
    /// Whether the node's replica of a group counts as a voter, by the one
    /// voter predicate, also when it is closed.
    pub counts: C,
    /// `None` on a Raft node without a meta runtime.
    pub placement: Option<NodePlacement<'a>>,
}

/// The node's maintenance report (see the module docs).
///
/// Node issues: a node without a meta runtime reports `placement_unknown`.
/// A damaged meta replica reports `meta_damaged` and replaces every issue
/// the view gives, the inventory included. A node that knows itself
/// removed, by its standing or its view, reports `node_removed` alone among
/// its node issues. No group places it ([`Standing::placed_role`]), even in
/// a view from before its removal, so its inventory is empty, each replica
/// it runs or its plan keeps open is `unplaced_replica`, and its running
/// replicas keep their voter checks. Otherwise the bootstrapped view gives
/// the inventory and the mismatches with the node's genesis claim. A view
/// that has not bootstrapped gives `placement_not_bootstrapped` when the
/// node holds or runs replicas, and `placement_unknown` when it holds none,
/// confirmed no view in this run, and was not answered `forming` or
/// `unreferenced`. `meta_unavailable` comes after
/// [`META_UNAVAILABLE_AFTER`] without a meta leader, unless the node holds
/// nothing and is placed nowhere.
///
/// Group issues: every running replica is checked against its effective
/// membership if it counts as a voter, and only for being stopped if not.
/// Against the bootstrapped view, a replica held or running without a role
/// is `unplaced_replica`, a counting replica whose membership is not the
/// placement's target is `incomplete_voter_set`, and one whose membership
/// names a node without a role is `unplaced_member`. A `voter` or
/// `incoming` role without a running replica is `missing_group`, and any
/// role in a group the view shows moving is `move_in_progress`. The leader
/// of a moving group whose reconciler made no progress for
/// [`MOVE_STALLED_AFTER`] also reports `move_stalled`.
pub fn check_raft_maintenance(
    inputs: MaintenanceInputs<'_, impl Fn(RaftGroupId) -> Counts>,
) -> RaftMaintenanceReport {
    use RaftMaintenanceIssue as Issue;

    let MaintenanceInputs {
        node_id,
        lag_tolerance,
        now_ms,
        groups,
        counts,
        placement,
    } = inputs;
    let counts = |group: u32| counts(RaftGroupId(group)) == Counts::Voter;
    let mut report = RaftMaintenanceReport {
        version: ursula_proto::admin::SchemaVersion,
        node_id,
        lag_tolerance,
        placement: None,
        expected_groups: Default::default(),
        unplaced: Default::default(),
        node_issues: Vec::new(),
        group_issues: Default::default(),
    };
    let running = groups
        .iter()
        .map(|group| group.raft_group_id)
        .collect::<BTreeSet<_>>();
    let cluster = match placement {
        Some(placement) => {
            let cluster = read_placement(&mut report, placement, &running, &counts);
            let status = placement.status;
            if placement
                .standing
                .knows_removed(shown_cluster(&status.view), status.node)
            {
                report.node_issues = vec![Issue::NodeRemoved];
            }
            cluster
        }
        None => {
            report.node_issues.push(Issue::PlacementUnknown);
            None
        }
    };
    let mut seen = BTreeSet::new();
    for group in groups {
        let id = group.raft_group_id;
        let mut issues = Vec::new();
        if !seen.insert(id) {
            issues.push(Issue::DuplicateGroup);
        }
        if group.node_id != node_id {
            issues.push(Issue::WrongNodeIdentity);
        }
        if report.unplaced.contains_key(&id) {
            issues.push(Issue::UnplacedReplica);
        }
        let placed = cluster
            .filter(|_| report.expected_groups.contains_key(&id))
            .and_then(|cluster| cluster.group(RaftGroupId(id)));
        voter_checks(
            group,
            node_id,
            counts(id),
            placed,
            lag_tolerance,
            now_ms,
            &mut issues,
        );
        if !issues.is_empty() {
            report.group_issues.entry(id).or_default().extend(issues);
        }
    }
    for (group, expected) in &report.expected_groups {
        if !seen.contains(group)
            && matches!(expected.role, ReplicaRole::Voter | ReplicaRole::Incoming)
        {
            report
                .group_issues
                .entry(*group)
                .or_default()
                .push(Issue::MissingGroup);
        }
        let moving = cluster
            .and_then(|cluster| cluster.group(RaftGroupId(*group)))
            .is_some_and(|placed| placed.moving().is_some());
        if moving {
            report
                .group_issues
                .entry(*group)
                .or_default()
                .push(Issue::MoveInProgress);
        }
    }
    // A held replica that does not run is unplaced all the same.
    for group in report.unplaced.keys() {
        if !seen.contains(group) {
            report
                .group_issues
                .entry(*group)
                .or_default()
                .push(Issue::UnplacedReplica);
        }
    }
    report
}

/// The cluster of a bootstrapped view, `None` before Bootstrap.
fn shown_cluster(view: &ClusterView) -> Option<&Cluster> {
    match view {
        ClusterView::Bootstrapped(cluster) => Some(cluster),
        ClusterView::MetaGenesis { .. }
        | ClusterView::MetaCatchingUp
        | ClusterView::Registering { .. }
        | ClusterView::FormationCut => None,
    }
}

/// Fills the report's placement, inventory and node issues from
/// `placement`, and returns the bootstrapped view's cluster the group
/// checks read, `None` before a bootstrapped view and while meta is
/// damaged.
fn read_placement<'a>(
    report: &mut RaftMaintenanceReport,
    placement: NodePlacement<'a>,
    running: &BTreeSet<u32>,
    counts: &impl Fn(u32) -> bool,
) -> Option<&'a Cluster> {
    use RaftMaintenanceIssue as Issue;

    let me = NodeId::new(report.node_id);
    let status = placement.status;
    let (meta_role, meta_leader) = match &status.meta {
        MetaReplicaStatus::Running { role, leader } => (Some(*role), *leader),
        MetaReplicaStatus::Damaged { .. } | MetaReplicaStatus::Absent => (None, None),
    };
    let registration = match &status.registration {
        RegistrationStatus::Answered(answer) => Some(*answer),
        RegistrationStatus::Pending
        | RegistrationStatus::RootRefused { .. }
        | RegistrationStatus::Refused(_)
        | RegistrationStatus::Removed => None,
    };
    let cluster = shown_cluster(&status.view);
    report.placement = Some(NodePlacementReport {
        view: status.view.stage(),
        applied: status.applied,
        meta_role,
        meta_leader,
        lifecycle: cluster
            .and_then(|cluster| cluster.node(me))
            .map(|record| record.lifecycle),
        registration,
    });
    if matches!(status.meta, MetaReplicaStatus::Damaged { .. }) {
        report.node_issues.push(Issue::MetaDamaged);
        return None;
    }
    let hosting = placement.hosting;
    let holds = !hosting.held.is_empty() || !running.is_empty();
    match cluster {
        Some(cluster) => {
            let params = cluster.params();
            if params.group_count != placement.claim.group_count {
                report.node_issues.push(Issue::GroupCountMismatch);
            }
            if params.replication_factor != placement.claim.replication_factor {
                report.node_issues.push(Issue::ReplicationFactorMismatch);
            }
            for (group, placed) in cluster.groups().iter() {
                if let Some(role) = placement.standing.placed_role(cluster, group, me) {
                    report.expected_groups.insert(group.0, ExpectedReplica {
                        role,
                        since: placed.since(),
                        target: placed.target_voters().clone(),
                        counts_as_voter: counts(group.0),
                    });
                }
            }
            let unplaced = hosting
                .unplaced
                .iter()
                .map(|group| group.0)
                .chain(running.iter().copied())
                .filter(|group| !report.expected_groups.contains_key(group))
                .collect::<BTreeSet<_>>();
            for group in unplaced {
                report.unplaced.insert(group, UnplacedReplica {
                    counts_as_voter: counts(group),
                    last_answer: hosting
                        .answers
                        .get(&RaftGroupId(group))
                        .map(|answer| answer.answer),
                    stage: hosting.teardowns.get(&RaftGroupId(group)).copied(),
                });
            }
        }
        None if holds => report.node_issues.push(Issue::PlacementNotBootstrapped),
        None => {
            let answered = matches!(
                registration,
                Some(Registered::Forming | Registered::Unreferenced(_))
            );
            if hosting.last_confirmed.is_none() && !answered {
                report.node_issues.push(Issue::PlacementUnknown);
            }
        }
    }
    if let ClusterView::MetaGenesis {
        genesis_mismatch, ..
    } = &status.view
        && !genesis_mismatch.is_empty()
    {
        report.node_issues.push(Issue::GenesisMismatch);
    }
    let placed_nowhere = match cluster {
        Some(_) => report.expected_groups.is_empty(),
        None => matches!(registration, Some(Registered::Unreferenced(_))),
    };
    if placement
        .leaderless_for
        .is_some_and(|leaderless| leaderless >= META_UNAVAILABLE_AFTER)
        && (holds || !placed_nowhere)
    {
        report.node_issues.push(Issue::MetaUnavailable);
    }
    cluster
}

/// The checks of one running replica: only for being stopped when it does
/// not count as a voter, else against its effective membership, and
/// against `placed`, its group's placement, when the node expects it.
fn voter_checks(
    group: &RaftGroupMetricsSnapshot,
    node_id: u64,
    counts: bool,
    placed: Option<&GroupPlacement>,
    lag_tolerance: u64,
    now_ms: u64,
    issues: &mut Vec<RaftMaintenanceIssue>,
) {
    use RaftMaintenanceIssue as Issue;

    // A stopped group's Raft observations are stale. The stop is its only
    // issue, so serving and maintenance policy can each decide on it.
    if group.apply_failure.is_some() {
        issues.push(Issue::ApplyStopped);
        return;
    }
    if !group.maintenance.running {
        issues.push(Issue::RaftStopped);
    }
    if !counts {
        return;
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
    if group.voter_configurations.is_empty()
        || !group
            .voter_configurations
            .iter()
            .all(|config| config.contains(&node_id))
    {
        issues.push(Issue::LocalReplicaNotVoter);
    }
    let voters = group.voter_ids.iter().copied().collect::<BTreeSet<_>>();
    if let Some(placed) = placed {
        let target = placed
            .target_voters()
            .iter()
            .map(NodeId::get)
            .collect::<BTreeSet<_>>();
        if voters != target || !group.learner_ids.is_empty() {
            issues.push(Issue::IncompleteVoterSet);
        }
        // Nothing restores a missing target voter. The leader removes a
        // learner without a role once the group is seeded or its voters
        // equal the target, and a voter only by a move or by leaving a joint
        // configuration.
        if voters
            .iter()
            .chain(&group.learner_ids)
            .any(|member| placed.role(NodeId::new(*member)).is_none())
        {
            issues.push(Issue::UnplacedMember);
        }
        let stalled_after = u64::try_from(MOVE_STALLED_AFTER.as_millis()).unwrap_or(u64::MAX);
        if placed.moving().is_some()
            && group.current_leader == Some(node_id)
            && group
                .reconcile
                .as_ref()
                .is_some_and(|status| now_ms.saturating_sub(status.progress_ms) >= stalled_after)
        {
            issues.push(Issue::MoveStalled);
        }
    }
    let applied = group.last_applied.map(|id| id.index);
    if group.maintenance.membership_log_index.is_none()
        || applied < group.maintenance.membership_log_index
    {
        issues.push(Issue::MembershipNotApplied);
    }
    match group.current_leader {
        None => issues.push(Issue::LeaderUnknown),
        Some(leader) if !voters.contains(&leader) => issues.push(Issue::LeaderOutsideVoters),
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
}

/// The maintenance report of node `node_id`: its registry's data replicas,
/// each judged by the one voter predicate, against its placement as `meta`
/// reads it and the hosting driver's last status, the default before the
/// driver's first pass. `meta` is `None` on a node without a meta runtime.
pub fn raft_maintenance_report(
    registry: &RaftGroupHandleRegistry,
    node_id: u64,
    meta: Option<&MetaStatusReader>,
    hosting: Option<&HostingStatus>,
) -> RaftMaintenanceReport {
    let groups = registry.metrics_snapshot();
    let before_first_pass = HostingStatus::default();
    let status = meta.map(MetaStatusReader::placement_status);
    let placement = meta
        .zip(status.as_ref())
        .map(|(reader, status)| NodePlacement {
            status,
            claim: reader.claim(),
            leaderless_for: reader.meta_leaderless_for(),
            hosting: hosting.unwrap_or(&before_first_pass),
            standing: registry.learn_standing(
                status.registration.standing(),
                shown_cluster(&status.view),
                status.node,
            ),
        });
    check_raft_maintenance(MaintenanceInputs {
        node_id,
        lag_tolerance: LAG_TOLERANCE,
        now_ms: ursula_runtime::unix_time_ms(),
        groups: &groups,
        counts: |group| registry.counts_as_voter(group),
        placement,
    })
}

#[cfg(test)]
mod tests;

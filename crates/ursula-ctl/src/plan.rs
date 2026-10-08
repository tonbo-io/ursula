use std::collections::BTreeMap;

use crate::metrics::ClusterSnapshot;
use crate::metrics::RaftGroupView;

#[derive(Debug, Clone)]
pub struct GroupTransfer {
    pub raft_group_id: u64,
    pub leader_node_id: u64,
    pub target_to_drain: u64,
    pub preferred_successor: u64,
}

#[derive(Debug, Clone, Default)]
pub struct DrainPlan {
    pub transfers: Vec<GroupTransfer>,
}

impl DrainPlan {
    pub fn is_empty(&self) -> bool {
        self.transfers.is_empty()
    }
}

/// Build the drain plan from the perspective of the target node's own metrics.
/// For every group the target currently leads, picks a preferred successor that
/// is both caught up and currently carrying the fewest leaders. Returns an empty
/// plan if the target leads nothing.
pub fn plan_drain(snapshot: &ClusterSnapshot, target_node_id: u64) -> DrainPlan {
    plan_drain_at_barriers(snapshot, target_node_id, &BTreeMap::new())
}

/// Use a fixed observed committed prefix while waiting for successor apply.
/// Otherwise continuously advancing writes and sequential metrics sampling
/// can move the target forever. The actual Raft handoff still waits for the
/// successor's log to cover the leader's transfer request before campaigning.
pub(crate) fn plan_drain_at_barriers(
    snapshot: &ClusterSnapshot,
    target_node_id: u64,
    required: &BTreeMap<u64, u64>,
) -> DrainPlan {
    let led = snapshot.groups_led_by(target_node_id);
    let mut leader_counts = leader_counts(snapshot);
    let mut transfers = Vec::with_capacity(led.len());
    for group in led {
        let barrier = required
            .get(&group.raft_group_id)
            .copied()
            .or(group.committed_index);
        let Some(successor) =
            pick_successor(snapshot, &group, target_node_id, &leader_counts, barrier)
        else {
            tracing::warn!(
                "no eligible successor voter; restart cannot proceed safely: raft_group_id={} target={target_node_id}",
                group.raft_group_id
            );
            continue;
        };
        leader_counts
            .entry(target_node_id)
            .and_modify(|count| *count = count.saturating_sub(1))
            .or_insert(0);
        let successor_count = leader_counts.entry(successor).or_insert(0);
        *successor_count = successor_count.saturating_add(1);
        transfers.push(GroupTransfer {
            raft_group_id: group.raft_group_id,
            leader_node_id: target_node_id,
            target_to_drain: target_node_id,
            preferred_successor: successor,
        });
    }
    DrainPlan { transfers }
}

fn leader_counts(snapshot: &ClusterSnapshot) -> BTreeMap<u64, usize> {
    let mut group_leaders = BTreeMap::new();
    for view in &snapshot.per_node {
        for group in &view.groups {
            if let Some(leader) = group.current_leader {
                group_leaders.entry(group.raft_group_id).or_insert(leader);
            }
        }
    }

    let mut counts = BTreeMap::new();
    for leader in group_leaders.into_values() {
        let count = counts.entry(leader).or_insert(0_usize);
        *count = count.saturating_add(1);
    }
    counts
}

fn pick_successor(
    snapshot: &ClusterSnapshot,
    group: &RaftGroupView,
    target_node_id: u64,
    leader_counts: &BTreeMap<u64, usize>,
    required_applied: Option<u64>,
) -> Option<u64> {
    let peer_views = snapshot.peer_views(group.raft_group_id, target_node_id);
    let mut scored: Vec<(u64, usize, Option<u64>)> = group
        .voter_ids
        .iter()
        .copied()
        .filter(|id| *id != target_node_id)
        .filter(|id| {
            peer_views.get(id).is_some_and(|peer| {
                peer.participation_ready()
                    && peer.maintenance.accepting_transfers
                    && peer.voter_ids.contains(id)
                    && peer.last_applied_index.is_some()
                    && peer.last_applied_index >= required_applied
            })
        })
        .map(|id| {
            let applied = peer_views.get(&id).and_then(|view| view.last_applied_index);
            let leader_count = leader_counts.get(&id).copied().unwrap_or(0);
            (id, leader_count, applied)
        })
        .collect();
    // Eligible, observed, caught-up peers only; prefer fewer leaders first,
    // then highest applied.
    scored.sort_by(|a, b| match (a.1, b.1) {
        (a_count, b_count) if a_count != b_count => a_count.cmp(&b_count),
        _ => match (a.2, b.2) {
            (Some(a), Some(b)) => b.cmp(&a),
            (Some(_), None) => std::cmp::Ordering::Less,
            (None, Some(_)) => std::cmp::Ordering::Greater,
            (None, None) => a.0.cmp(&b.0),
        },
    });
    scored.first().map(|(id, _, _)| *id)
}

#[derive(Debug, Clone)]
pub struct ReadinessReport {
    pub all_ready: bool,
    pub per_group: BTreeMap<u64, GroupReadiness>,
    pub maintenance_issues: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct GroupReadiness {
    pub raft_group_id: u64,
    pub voter_member: bool,
    pub target_applied_index: Option<u64>,
    pub peer_max_committed_index: Option<u64>,
    pub catch_up_gap: Option<u64>,
    pub ready: bool,
}

/// A target node is ready when, in every raft group that any peer reports:
///   1. The target is listed in voter_ids (membership intact).
///   2. The target's last_applied_index >= max peer committed_index - lag_tolerance.
///      For an expected single-voter group, use its own committed prefix.
///   3. The target has applied something once any peer has committed past the
///      initial membership entry. An empty voter (a replica that lost its log
///      and has not been rebuilt yet) is never ready, whatever the lag
///      tolerance: counting it ready leaves the group on two real copies.
///
/// Groups invisible to the target (e.g. because it just restarted and hasn't
/// caught up enough to know about them) are treated as not-ready.
pub fn check_readiness(
    snapshot: &ClusterSnapshot,
    target_node_id: u64,
    lag_tolerance: u64,
) -> ReadinessReport {
    let target_view = snapshot.node(target_node_id);
    let mut all_group_ids: std::collections::BTreeSet<u64> = std::collections::BTreeSet::new();
    let maintenance = target_view.and_then(|view| view.raft_maintenance.as_ref());
    if let Some(report) = maintenance {
        all_group_ids.extend(report.expected_groups.keys().copied().map(u64::from));
    }
    for view in &snapshot.per_node {
        for g in &view.groups {
            if !group_is_initialized(g) {
                continue;
            }
            all_group_ids.insert(g.raft_group_id);
        }
    }

    let mut per_group = BTreeMap::new();
    let mut all_ready = !all_group_ids.is_empty()
        && target_view.is_some()
        && maintenance.is_some_and(|report| report.node_id == target_node_id && report.ready());
    for group_id in all_group_ids {
        let peers = snapshot.peer_views(group_id, target_node_id);
        let target_group = target_view.and_then(|v| v.group(group_id));
        let voter_member = target_group
            .map(|g| g.voter_ids.contains(&target_node_id))
            .unwrap_or(false);
        let target_applied = target_group.and_then(|g| g.last_applied_index);
        let peer_max_committed = peers.values().filter_map(|v| v.committed_index).max();
        let expected_voters = u32::try_from(group_id)
            .ok()
            .and_then(|group| maintenance.and_then(|report| report.expected_groups.get(&group)));
        let only_target_votes = target_group
            .is_some_and(|group| group.voter_ids.as_slice() == [target_node_id])
            && expected_voters
                .is_some_and(|voters| voters.len() == 1 && voters.contains(&target_node_id));
        // A sole voter has no peer observation. Its own committed prefix is
        // the reference only when both membership and expected inventory agree.
        let reference_committed = if only_target_votes {
            target_group.and_then(|group| group.committed_index)
        } else {
            peer_max_committed
        };
        let catch_up_gap = match (reference_committed, target_applied) {
            (Some(peer), Some(target)) => Some(peer.saturating_sub(target)),
            (Some(peer), None) => Some(peer),
            (None, _) => None,
        };
        let within_lag = catch_up_gap
            .map(|gap| gap <= lag_tolerance)
            .unwrap_or(false);
        let empty_replica =
            target_applied.is_none() && reference_committed.is_some_and(|committed| committed > 0);
        let ready = voter_member
            && within_lag
            && target_group.is_some_and(RaftGroupView::participation_ready)
            && !empty_replica;
        if !ready {
            all_ready = false;
        }
        per_group.insert(group_id, GroupReadiness {
            raft_group_id: group_id,
            voter_member,
            target_applied_index: target_applied,
            peer_max_committed_index: peer_max_committed,
            catch_up_gap,
            ready,
        });
    }
    ReadinessReport {
        all_ready,
        per_group,
        maintenance_issues: maintenance
            .map(|report| {
                report
                    .node_issues
                    .iter()
                    .map(|issue| format!("{issue:?}"))
                    .chain(
                        report
                            .group_issues
                            .iter()
                            .map(|(id, issues)| format!("group {id}: {issues:?}")),
                    )
                    .chain(
                        (report.node_id != target_node_id)
                            .then(|| "invalid maintenance report identity or version".to_owned()),
                    )
                    .collect()
            })
            .unwrap_or_default(),
    }
}

fn group_is_initialized(group: &RaftGroupView) -> bool {
    !group.voter_ids.is_empty()
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use url::Url;

    use super::*;
    use crate::metrics::NodeMetricsView;
    use crate::provider::NodeInfo;

    fn node(id: u64) -> NodeInfo {
        NodeInfo {
            expected_process_incarnation: None,
            expected_maintenance_fence: None,
            id,
            admin_url: Url::parse(&format!("http://10.0.0.{id}:4438")).unwrap(),
            host: format!("10.0.0.{id}"),
            http_url: Some(Url::parse(&format!("http://10.0.0.{id}:8080")).unwrap()),
            metrics_url: None,
        }
    }

    fn view(node_id: u64, groups: Vec<RaftGroupView>) -> NodeMetricsView {
        {
            let fixture_node = node(node_id);
            let fixture_groups = groups;
            let fixture_report = Some(crate::metrics::test_maintenance_report(
                fixture_node.id,
                &fixture_groups,
            ));
            NodeMetricsView {
                node: fixture_node.clone(),
                metrics: ursula_proto::admin::NodeMetrics {
                    process_incarnation: ursula_proto::admin::ProcessIncarnation::from_bits(1),
                    maintenance_fence: ursula_proto::admin::MaintenanceFenceState::Unclaimed,
                    maintenance_fence_uncertain: false,
                    process_node_id: Some(fixture_node.id),
                    groups: fixture_groups,
                    raft_maintenance: fixture_report,
                },
            }
        }
    }

    fn group(
        raft_group_id: u64,
        reporting_node: u64,
        leader: Option<u64>,
        applied: Option<u64>,
        committed: Option<u64>,
        voters: Vec<u64>,
    ) -> RaftGroupView {
        {
            let fixture_term = 1;
            let fixture_committed = committed;
            let fixture_applied = applied;
            RaftGroupView {
                raft_group_id,
                node_id: reporting_node,
                current_term: fixture_term,
                current_leader: leader,
                committed_index: fixture_committed,
                last_applied_index: fixture_applied,
                voter_ids: voters,
                learner_ids: vec![],
                maintenance: ursula_proto::admin::RaftGroupMaintenanceState {
                    running: true,
                    recovery_ready: true,
                    accepting_transfers: true,
                    membership_joint: false,
                    membership_log_index: Some(0),
                    stopped_for_operator: false,
                },
                last_log_index: fixture_committed.into_iter().chain(fixture_applied).max(),
                committed_term: fixture_committed.map(|_| fixture_term),
                last_applied_term: fixture_applied.map(|_| fixture_term),
                snapshot_term: None,
                snapshot_index: None,
                purged_term: None,
                purged_index: None,
                log_bytes_since_snapshot: 0,
                log_entries_since_snapshot: 0,
                last_snapshot_bytes: 0,
                has_snapshot: false,
            }
        }
    }

    fn empty_group(raft_group_id: u64, reporting_node: u64) -> RaftGroupView {
        {
            let fixture_term = 0;
            let fixture_committed = None;
            let fixture_applied = None;
            RaftGroupView {
                raft_group_id,
                node_id: reporting_node,
                current_term: fixture_term,
                current_leader: None,
                committed_index: fixture_committed,
                last_applied_index: fixture_applied,
                voter_ids: vec![],
                learner_ids: vec![],
                maintenance: ursula_proto::admin::RaftGroupMaintenanceState {
                    running: true,
                    recovery_ready: true,
                    accepting_transfers: true,
                    membership_joint: false,
                    membership_log_index: Some(0),
                    stopped_for_operator: false,
                },
                last_log_index: fixture_committed.into_iter().chain(fixture_applied).max(),
                committed_term: fixture_committed.map(|_| fixture_term),
                last_applied_term: fixture_applied.map(|_| fixture_term),
                snapshot_term: None,
                snapshot_index: None,
                purged_term: None,
                purged_index: None,
                log_bytes_since_snapshot: 0,
                log_entries_since_snapshot: 0,
                last_snapshot_bytes: 0,
                has_snapshot: false,
            }
        }
    }

    #[test]
    fn plan_drain_picks_most_caught_up_successor() {
        let snapshot = ClusterSnapshot {
            per_node: vec![
                view(1, vec![group(7, 1, Some(1), Some(100), Some(100), vec![
                    1, 2, 3,
                ])]),
                view(2, vec![group(7, 2, Some(1), Some(100), Some(100), vec![
                    1, 2, 3,
                ])]),
                view(3, vec![group(7, 3, Some(1), Some(95), Some(100), vec![
                    1, 2, 3,
                ])]),
            ],
        };
        let plan = plan_drain(&snapshot, 1);
        assert_eq!(plan.transfers.len(), 1);
        assert_eq!(plan.transfers[0].raft_group_id, 7);
        assert_eq!(plan.transfers[0].preferred_successor, 2);
    }

    #[test]
    fn drain_never_selects_an_unknown_lagging_recovering_or_drained_successor() {
        let mut snapshot = ClusterSnapshot {
            per_node: vec![
                view(1, vec![group(7, 1, Some(1), Some(100), Some(100), vec![
                    1, 2, 3,
                ])]),
                view(2, vec![group(7, 2, Some(1), Some(99), Some(100), vec![
                    1, 2, 3,
                ])]),
            ],
        };
        assert!(plan_drain(&snapshot, 1).is_empty());
        let peer = &mut snapshot.per_node[1].groups[0];
        peer.last_applied_index = Some(100);
        peer.maintenance = ursula_proto::admin::RaftGroupMaintenanceState {
            running: true,
            recovery_ready: false,
            accepting_transfers: true,
            membership_joint: false,
            membership_log_index: Some(0),
            stopped_for_operator: false,
        };
        assert!(plan_drain(&snapshot, 1).is_empty());
        snapshot.per_node[1].groups[0].maintenance.recovery_ready = true;
        assert_eq!(plan_drain(&snapshot, 1).transfers[0].preferred_successor, 2);
        snapshot.per_node[1].groups[0]
            .maintenance
            .accepting_transfers = false;
        assert!(plan_drain(&snapshot, 1).is_empty());
    }

    #[test]
    fn drain_apply_barrier_does_not_chase_continuously_advancing_writes() {
        let snapshot = ClusterSnapshot {
            per_node: vec![
                view(1, vec![group(7, 1, Some(1), Some(110), Some(110), vec![
                    1, 2, 3,
                ])]),
                view(2, vec![group(7, 2, Some(1), Some(100), Some(100), vec![
                    1, 2, 3,
                ])]),
            ],
        };
        assert!(plan_drain(&snapshot, 1).is_empty());
        let plan = plan_drain_at_barriers(&snapshot, 1, &BTreeMap::from([(7, 100)]));
        assert_eq!(plan.transfers[0].preferred_successor, 2);
        assert!(plan_drain_at_barriers(&snapshot, 1, &BTreeMap::from([(7, 101)])).is_empty());
    }

    #[test]
    fn configured_maintenance_inventory_catches_a_group_missing_from_every_peer() {
        let mut snapshot = ClusterSnapshot {
            per_node: (1..=3)
                .map(|id| {
                    view(id, vec![group(7, id, Some(1), Some(100), Some(100), vec![
                        1, 2, 3,
                    ])])
                })
                .collect(),
        };
        snapshot.per_node[0].raft_maintenance = Some(ursula_proto::admin::RaftMaintenanceReport {
            version: ursula_proto::admin::SchemaVersion,
            node_id: 1,
            lag_tolerance: 16,
            expected_groups: BTreeMap::from([
                (7, BTreeSet::from([1, 2, 3])),
                (8, BTreeSet::from([1, 2, 3])),
            ]),
            node_issues: vec![],
            group_issues: BTreeMap::from([(8, vec![
                ursula_proto::admin::RaftMaintenanceIssue::MissingGroup,
            ])]),
        });
        let report = check_readiness(&snapshot, 1, 16);
        assert!(!report.all_ready);
        assert!(!report.per_group[&8].ready);
        assert!(
            report
                .maintenance_issues
                .iter()
                .any(|issue| issue.contains("MissingGroup"))
        );
        snapshot.per_node[0]
            .raft_maintenance
            .as_mut()
            .unwrap()
            .node_id = 2;
        assert!(!check_readiness(&snapshot, 1, 16).all_ready);
    }

    #[test]
    fn plan_drain_empty_when_target_leads_nothing() {
        let snapshot = ClusterSnapshot {
            per_node: vec![view(1, vec![group(
                7,
                1,
                Some(2),
                Some(100),
                Some(100),
                vec![1, 2, 3],
            )])],
        };
        assert!(plan_drain(&snapshot, 1).is_empty());
    }

    #[test]
    fn plan_drain_spreads_multiple_transfers_by_projected_leader_count() {
        let groups = vec![
            group(0, 2, Some(1), Some(100), Some(100), vec![1, 2, 3]),
            group(1, 2, Some(1), Some(100), Some(100), vec![1, 2, 3]),
            group(2, 2, Some(2), Some(100), Some(100), vec![1, 2, 3]),
            group(3, 2, Some(2), Some(100), Some(100), vec![1, 2, 3]),
            group(4, 2, Some(3), Some(100), Some(100), vec![1, 2, 3]),
            group(5, 2, Some(3), Some(100), Some(100), vec![1, 2, 3]),
        ];
        let snapshot = ClusterSnapshot {
            per_node: vec![
                view(1, groups.clone()),
                view(2, groups.clone()),
                view(3, groups),
            ],
        };

        let plan = plan_drain(&snapshot, 2);

        assert_eq!(plan.transfers.len(), 2);
        let targets: std::collections::BTreeSet<u64> = plan
            .transfers
            .iter()
            .map(|transfer| transfer.preferred_successor)
            .collect();
        assert_eq!(targets, [1, 3].into_iter().collect());
    }

    #[test]
    fn readiness_requires_voter_membership_and_low_lag() {
        let snapshot = ClusterSnapshot {
            per_node: vec![
                view(1, vec![group(7, 1, Some(2), Some(99), Some(99), vec![
                    1, 2, 3,
                ])]),
                view(2, vec![group(7, 2, Some(2), Some(100), Some(100), vec![
                    1, 2, 3,
                ])]),
            ],
        };
        let report = check_readiness(&snapshot, 1, 5);
        assert!(report.all_ready, "{:?}", report);

        // Same snapshot but target is missing from voter_ids on every peer.
        let snapshot = ClusterSnapshot {
            per_node: vec![
                view(1, vec![group(7, 1, Some(2), Some(99), Some(99), vec![
                    2, 3,
                ])]),
                view(2, vec![group(7, 2, Some(2), Some(100), Some(100), vec![
                    2, 3,
                ])]),
            ],
        };
        let report = check_readiness(&snapshot, 1, 5);
        assert!(!report.all_ready);
    }

    #[test]
    fn readiness_fails_on_large_gap() {
        let snapshot = ClusterSnapshot {
            per_node: vec![
                view(1, vec![group(7, 1, Some(2), Some(50), Some(50), vec![
                    1, 2, 3,
                ])]),
                view(2, vec![group(7, 2, Some(2), Some(100), Some(100), vec![
                    1, 2, 3,
                ])]),
            ],
        };
        let report = check_readiness(&snapshot, 1, 5);
        assert!(!report.all_ready);
        let g = &report.per_group[&7];
        assert_eq!(g.catch_up_gap, Some(50));
    }

    #[test]
    fn single_voter_readiness_uses_its_own_committed_prefix() {
        let mut snapshot = ClusterSnapshot {
            per_node: vec![view(1, vec![group(
                7,
                1,
                Some(1),
                Some(9),
                Some(10),
                vec![1],
            )])],
        };
        let lagging = check_readiness(&snapshot, 1, 0);
        assert!(!lagging.all_ready);
        assert_eq!(lagging.per_group[&7].catch_up_gap, Some(1));
        snapshot.per_node[0].metrics.groups[0].last_applied_index = Some(10);
        assert!(check_readiness(&snapshot, 1, 0).all_ready);
        snapshot.per_node[0].metrics.groups[0].committed_index = None;
        assert!(!check_readiness(&snapshot, 1, 0).all_ready);
    }

    #[test]
    fn absent_expected_peers_do_not_become_a_single_voter_baseline() {
        let mut snapshot = ClusterSnapshot {
            per_node: vec![view(1, vec![group(
                7,
                1,
                Some(1),
                Some(10),
                Some(10),
                vec![1, 2, 3],
            )])],
        };
        let report = check_readiness(&snapshot, 1, 0);
        assert!(!report.all_ready);
        assert_eq!(report.per_group[&7].catch_up_gap, None);
        // An incomplete local membership cannot override the expected voters.
        snapshot.per_node[0].metrics.groups[0].voter_ids = vec![1];
        assert!(!check_readiness(&snapshot, 1, 0).all_ready);
    }

    #[test]
    fn readiness_cannot_infer_zero_lag_from_unknown_peer_commit() {
        let snapshot = ClusterSnapshot {
            per_node: vec![
                view(1, vec![group(7, 1, Some(2), Some(99), Some(99), vec![
                    1, 2, 3,
                ])]),
                view(2, vec![group(7, 2, Some(2), Some(100), None, vec![
                    1, 2, 3,
                ])]),
            ],
        };
        let report = check_readiness(&snapshot, 1, 5);
        assert!(!report.all_ready);
        assert_eq!(report.per_group[&7].catch_up_gap, None);
    }

    #[test]
    fn readiness_rejects_configured_uninitialized_groups() {
        let snapshot = ClusterSnapshot {
            per_node: vec![
                view(1, vec![
                    group(7, 1, Some(2), Some(99), Some(99), vec![1, 2, 3]),
                    empty_group(8, 1),
                ]),
                view(2, vec![
                    group(7, 2, Some(2), Some(100), Some(100), vec![1, 2, 3]),
                    empty_group(8, 2),
                ]),
            ],
        };
        let report = check_readiness(&snapshot, 1, 5);
        assert!(!report.all_ready, "{report:?}");
        assert!(!report.per_group[&8].ready);
    }

    #[test]
    fn readiness_rejects_an_empty_voter_within_lag_tolerance() {
        let snapshot = ClusterSnapshot {
            per_node: vec![
                view(3, vec![group(5, 3, Some(1), None, None, vec![1, 2, 3])]),
                view(1, vec![group(5, 1, Some(1), Some(11), Some(11), vec![
                    1, 2, 3,
                ])]),
            ],
        };
        let report = check_readiness(&snapshot, 3, 16);
        assert!(!report.all_ready, "{report:?}");
        assert!(!report.per_group[&5].ready);
    }
}

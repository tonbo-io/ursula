use std::collections::BTreeMap;

use crate::metrics::{ClusterSnapshot, RaftGroupView};

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
/// For every group the target currently leads, picks a preferred successor — the
/// voter with the highest last_applied_index that is not the target. Returns an
/// empty plan if the target leads nothing.
pub fn plan_drain(snapshot: &ClusterSnapshot, target_node_id: u64) -> DrainPlan {
    let led = snapshot.groups_led_by(target_node_id);
    let mut transfers = Vec::with_capacity(led.len());
    for group in led {
        let Some(successor) = pick_successor(snapshot, &group, target_node_id) else {
            tracing::warn!(
                "no eligible successor voter; restart cannot proceed safely: raft_group_id={} target={target_node_id}",
                group.raft_group_id
            );
            continue;
        };
        transfers.push(GroupTransfer {
            raft_group_id: group.raft_group_id,
            leader_node_id: target_node_id,
            target_to_drain: target_node_id,
            preferred_successor: successor,
        });
    }
    DrainPlan { transfers }
}

fn pick_successor(
    snapshot: &ClusterSnapshot,
    group: &RaftGroupView,
    target_node_id: u64,
) -> Option<u64> {
    let peer_views = snapshot.peer_views(group.raft_group_id, target_node_id);
    let mut scored: Vec<(u64, Option<u64>)> = group
        .voter_ids
        .iter()
        .copied()
        .filter(|id| *id != target_node_id)
        .map(|id| {
            let applied = peer_views.get(&id).and_then(|view| view.last_applied_index);
            (id, applied)
        })
        .collect();
    // Highest applied first; unknown peers sort last but are still candidates.
    scored.sort_by(|a, b| match (a.1, b.1) {
        (Some(a), Some(b)) => b.cmp(&a),
        (Some(_), None) => std::cmp::Ordering::Less,
        (None, Some(_)) => std::cmp::Ordering::Greater,
        (None, None) => a.0.cmp(&b.0),
    });
    scored.first().map(|(id, _)| *id)
}

#[derive(Debug, Clone)]
pub struct ReadinessReport {
    pub all_ready: bool,
    pub per_group: BTreeMap<u64, GroupReadiness>,
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
    for view in &snapshot.per_node {
        for g in &view.groups {
            all_group_ids.insert(g.raft_group_id);
        }
    }

    let mut per_group = BTreeMap::new();
    let mut all_ready = !all_group_ids.is_empty() && target_view.is_some();
    for group_id in all_group_ids {
        let peers = snapshot.peer_views(group_id, target_node_id);
        let target_group = target_view.and_then(|v| v.group(group_id));
        let voter_member = target_group
            .map(|g| g.voter_ids.contains(&target_node_id))
            .unwrap_or(false);
        let target_applied = target_group.and_then(|g| g.last_applied_index);
        let peer_max_committed = peers.values().filter_map(|v| v.committed_index).max();
        let catch_up_gap = match (peer_max_committed, target_applied) {
            (Some(peer), Some(target)) => Some(peer.saturating_sub(target)),
            (Some(peer), None) => Some(peer),
            (None, _) => Some(0),
        };
        let within_lag = catch_up_gap
            .map(|gap| gap <= lag_tolerance)
            .unwrap_or(false);
        let ready = voter_member && within_lag && target_group.is_some();
        if !ready {
            all_ready = false;
        }
        per_group.insert(
            group_id,
            GroupReadiness {
                raft_group_id: group_id,
                voter_member,
                target_applied_index: target_applied,
                peer_max_committed_index: peer_max_committed,
                catch_up_gap,
                ready,
            },
        );
    }
    ReadinessReport {
        all_ready,
        per_group,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::NodeMetricsView;
    use crate::provider::NodeInfo;
    use url::Url;

    fn node(id: u64) -> NodeInfo {
        NodeInfo {
            id,
            http_url: Url::parse(&format!("http://10.0.0.{id}:8080")).unwrap(),
            host: format!("10.0.0.{id}"),
            name: None,
        }
    }

    fn view(node_id: u64, groups: Vec<RaftGroupView>) -> NodeMetricsView {
        NodeMetricsView {
            node: node(node_id),
            groups,
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
        RaftGroupView {
            raft_group_id,
            node_id: reporting_node,
            current_leader: leader,
            committed_index: committed,
            last_applied_index: applied,
            voter_ids: voters,
            learner_ids: vec![],
        }
    }

    #[test]
    fn plan_drain_picks_most_caught_up_successor() {
        let snapshot = ClusterSnapshot {
            per_node: vec![
                view(
                    1,
                    vec![group(7, 1, Some(1), Some(100), Some(100), vec![1, 2, 3])],
                ),
                view(
                    2,
                    vec![group(7, 2, Some(1), Some(98), Some(100), vec![1, 2, 3])],
                ),
                view(
                    3,
                    vec![group(7, 3, Some(1), Some(95), Some(100), vec![1, 2, 3])],
                ),
            ],
        };
        let plan = plan_drain(&snapshot, 1);
        assert_eq!(plan.transfers.len(), 1);
        assert_eq!(plan.transfers[0].raft_group_id, 7);
        assert_eq!(plan.transfers[0].preferred_successor, 2);
    }

    #[test]
    fn plan_drain_empty_when_target_leads_nothing() {
        let snapshot = ClusterSnapshot {
            per_node: vec![view(
                1,
                vec![group(7, 1, Some(2), Some(100), Some(100), vec![1, 2, 3])],
            )],
        };
        assert!(plan_drain(&snapshot, 1).is_empty());
    }

    #[test]
    fn readiness_requires_voter_membership_and_low_lag() {
        let snapshot = ClusterSnapshot {
            per_node: vec![
                view(
                    1,
                    vec![group(7, 1, Some(2), Some(99), Some(99), vec![1, 2, 3])],
                ),
                view(
                    2,
                    vec![group(7, 2, Some(2), Some(100), Some(100), vec![1, 2, 3])],
                ),
            ],
        };
        let report = check_readiness(&snapshot, 1, 5);
        assert!(report.all_ready, "{:?}", report);

        // Same snapshot but target is missing from voter_ids on every peer.
        let snapshot = ClusterSnapshot {
            per_node: vec![
                view(
                    1,
                    vec![group(7, 1, Some(2), Some(99), Some(99), vec![2, 3])],
                ),
                view(
                    2,
                    vec![group(7, 2, Some(2), Some(100), Some(100), vec![2, 3])],
                ),
            ],
        };
        let report = check_readiness(&snapshot, 1, 5);
        assert!(!report.all_ready);
    }

    #[test]
    fn readiness_fails_on_large_gap() {
        let snapshot = ClusterSnapshot {
            per_node: vec![
                view(
                    1,
                    vec![group(7, 1, Some(2), Some(50), Some(50), vec![1, 2, 3])],
                ),
                view(
                    2,
                    vec![group(7, 2, Some(2), Some(100), Some(100), vec![1, 2, 3])],
                ),
            ],
        };
        let report = check_readiness(&snapshot, 1, 5);
        assert!(!report.all_ready);
        let g = &report.per_group[&7];
        assert_eq!(g.catch_up_gap, Some(50));
    }
}

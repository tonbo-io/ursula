use std::collections::HashMap;

pub(crate) fn leader_counts(
    snaps: &[ursula_raft::RaftGroupMetricsSnapshot],
) -> HashMap<u64, usize> {
    let mut leader_count = HashMap::new();
    for snap in snaps {
        if let Some(leader) = snap.current_leader {
            *leader_count.entry(leader).or_insert(0) += 1;
        }
    }
    leader_count
}

pub(crate) fn prioritized_transfer_targets(
    snap: &ursula_raft::RaftGroupMetricsSnapshot,
    my_id: u64,
    leader_count: &HashMap<u64, usize>,
) -> Vec<u64> {
    let mut targets: Vec<u64> = snap
        .voter_ids
        .iter()
        .copied()
        .filter(|voter| *voter != my_id)
        .collect();
    targets.sort_by_key(|target| (leader_count.get(target).copied().unwrap_or(0), *target));
    targets
}

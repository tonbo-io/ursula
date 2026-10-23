use std::collections::HashMap;

use ursula_control::Cluster;
use ursula_proto::placement::NodeId;
use ursula_shard::RaftGroupId;

pub(crate) fn leader_counts(
    snaps: &[ursula_raft::RaftGroupMetricsSnapshot],
) -> HashMap<u64, usize> {
    let mut leader_count = HashMap::new();
    for snap in snaps {
        if let Some(leader) = snap.current_leader {
            let count = leader_count.entry(leader).or_insert(0_usize);
            *count = count.saturating_add(1);
        }
    }
    leader_count
}

/// The nodes `me` may hand the leadership of `snap`'s group to, the one
/// exclusion of every leadership handoff (design 5.9): the other voters of
/// the group's effective membership that `admits` takes, such as the
/// replicas whose node answered that they may take the leadership. Under a bootstrapped view (`cluster`) a voter must
/// also have a role in the group's placement that takes a leadership
/// ([`ursula_proto::placement::ReplicaRole::takes_leadership`]), so no
/// outgoing or abandoned replica, nor a member without a role, is handed
/// one. The meta group has no placement, so its handoffs pass no `cluster`.
pub(crate) fn handoff_targets(
    snap: &ursula_raft::RaftGroupMetricsSnapshot,
    me: u64,
    cluster: Option<&Cluster>,
    mut admits: impl FnMut(u64) -> bool,
) -> Vec<u64> {
    let group = RaftGroupId(snap.raft_group_id);
    snap.voter_ids
        .iter()
        .copied()
        .filter(|voter| *voter != me && admits(*voter))
        .filter(|voter| {
            cluster.is_none_or(|cluster| {
                cluster
                    .role(group, NodeId::new(*voter))
                    .is_some_and(|role| role.takes_leadership())
            })
        })
        .collect()
}

/// [`handoff_targets`], the ones leading the fewest groups in
/// `leader_count` first, then by node id.
pub(crate) fn prioritized_transfer_targets(
    snap: &ursula_raft::RaftGroupMetricsSnapshot,
    me: u64,
    cluster: Option<&Cluster>,
    admits: impl FnMut(u64) -> bool,
    leader_count: &HashMap<u64, usize>,
) -> Vec<u64> {
    let mut targets = handoff_targets(snap, me, cluster, admits);
    targets.sort_by_key(|target| (leader_count.get(target).copied().unwrap_or(0), *target));
    targets
}

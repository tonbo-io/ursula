use std::collections::HashMap;
use std::collections::HashSet;
use std::time::Duration;

use ursula_raft::RaftGroupHandleRegistry;
use ursula_shard::RaftGroupId;

/// M1 — leadership balancing. Spreads raft-group leadership evenly across
/// nodes so a single node's failure or network impairment can stall at most
/// its `ceil(groups / nodes)` share rather than the whole cluster (leadership
/// concentration is what turns one impaired node into a cluster-wide ops drop).
///
/// Each node runs this independently and only moves groups it currently leads;
/// `current_leader` is globally agreed, so every node computes the same
/// distribution and converges. One transfer per tick keeps convergence gentle
/// and avoids thundering-herd handoffs. Before planning a handoff, M1 asks
/// peers for their leadership-shed policy state and only targets nodes whose
/// policy says they may campaign. That prevents balancing into a hard-yielded
/// peer that cannot safely lead. Cold-health remains campaign-eligible because
/// it is a cluster-wide pressure signal under backlog: excluding every hot
/// peer can deadlock leadership movement. `transfer_leader` still brings
/// lagging eligible targets up to date before handing off.
/// One planned leader handoff for the M1 balancer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LeadershipBalanceAction {
    pub group_id: u32,
    pub target: u64,
    pub fair: usize,
}

#[derive(Debug, serde::Deserialize)]
struct LeadershipShedPeerStatus {
    should_campaign: bool,
}

async fn leadership_balance_eligible_nodes(
    registry: &RaftGroupHandleRegistry,
    node_id: u64,
    peers: &[(u64, String)],
    client: &reqwest::Client,
) -> HashSet<u64> {
    let mut eligible = HashSet::new();
    if registry.leadership_shed_state().should_campaign() {
        eligible.insert(node_id);
    }

    for (peer_id, peer_url) in peers {
        if *peer_id == node_id {
            continue;
        }
        let url = format!("{peer_url}{}", crate::LEADERSHIP_SHED_PATH);
        let Ok(response) = client.get(url).send().await else {
            continue;
        };
        if !response.status().is_success() {
            continue;
        }
        let Ok(body) = response.text().await else {
            continue;
        };
        let Ok(status) = serde_json::from_str::<LeadershipShedPeerStatus>(&body) else {
            continue;
        };
        if status.should_campaign {
            eligible.insert(*peer_id);
        }
    }

    eligible
}

/// Plan the leader handoffs this tick should attempt. Pure function — split
/// out so it tests against synthetic snapshots without spawning a task.
///
/// Each node runs this independently using the cluster-wide leader map
/// (`current_leader` is globally agreed), so every node computes the same
/// distribution and only schedules transfers it itself can perform (i.e. for
/// groups where it is currently leader). The result respects `max_per_tick`
/// and produces a deterministic order (smallest group id first, smallest
/// target id on ties) so retries on the same snapshot are stable.
#[allow(dead_code)]
pub(crate) fn plan_leadership_balance(
    snaps: &[ursula_raft::RaftGroupMetricsSnapshot],
    my_id: u64,
    max_per_tick: usize,
) -> Vec<LeadershipBalanceAction> {
    let eligible_nodes: HashSet<u64> = snaps
        .iter()
        .flat_map(|snap| snap.voter_ids.iter().copied())
        .collect();
    plan_leadership_balance_with_eligible_nodes(snaps, my_id, max_per_tick, &eligible_nodes)
}

pub(crate) fn plan_leadership_balance_with_eligible_nodes(
    snaps: &[ursula_raft::RaftGroupMetricsSnapshot],
    my_id: u64,
    max_per_tick: usize,
    eligible_nodes: &HashSet<u64>,
) -> Vec<LeadershipBalanceAction> {
    if snaps.is_empty() {
        return Vec::new();
    }
    let node_ids: HashSet<u64> = snaps
        .iter()
        .flat_map(|snap| snap.voter_ids.iter().copied())
        .collect();
    let eligible_voters: HashSet<u64> = node_ids
        .iter()
        .copied()
        .filter(|node_id| eligible_nodes.contains(node_id))
        .collect();
    if eligible_voters.is_empty() {
        return Vec::new();
    }
    let node_count = eligible_voters.len();
    let group_count = snaps.len();
    let fair = group_count.div_ceil(node_count);

    let leader_count = crate::bootstrap::util::leader_counts(snaps);
    let my_load = leader_count.get(&my_id).copied().unwrap_or(0);
    if my_load <= fair {
        return Vec::new();
    }
    let mut excess = my_load - fair;
    let mut groups_we_lead: Vec<&ursula_raft::RaftGroupMetricsSnapshot> = snaps
        .iter()
        .filter(|s| s.current_leader == Some(my_id))
        .collect();
    groups_we_lead.sort_by_key(|s| s.raft_group_id);

    let mut planned_load: HashMap<u64, usize> = leader_count.clone();
    let mut actions = Vec::new();
    for snap in groups_we_lead {
        if excess == 0 {
            break;
        }
        if max_per_tick > 0 && actions.len() >= max_per_tick {
            break;
        }
        let mut peers: Vec<u64> = snap
            .voter_ids
            .iter()
            .copied()
            .filter(|v| *v != my_id)
            .filter(|v| eligible_voters.contains(v))
            .collect();
        peers.sort_by_key(|v| (planned_load.get(v).copied().unwrap_or(0), *v));
        let Some(&target) = peers
            .iter()
            .find(|v| planned_load.get(*v).copied().unwrap_or(0) < fair)
        else {
            continue;
        };
        actions.push(LeadershipBalanceAction {
            group_id: snap.raft_group_id,
            target,
            fair,
        });
        *planned_load.entry(target).or_insert(0) += 1;
        if let Some(slot) = planned_load.get_mut(&my_id) {
            *slot = slot.saturating_sub(1);
        }
        excess -= 1;
    }
    actions
}

/// Config-driven leadership balancer.
pub fn spawn_leadership_balancer(
    registry: &RaftGroupHandleRegistry,
    node_id: u64,
    peers: &[(u64, String)],
    lb_cfg: &ursula_config::LeadershipBalanceConfig,
) {
    let interval_ms = lb_cfg.interval.as_duration().as_millis() as usize;
    if interval_ms == 0 {
        return;
    }
    let max_per_tick = lb_cfg.max_per_tick;
    let peer_timeout_ms = lb_cfg.peer_timeout.as_duration().as_millis() as usize;
    let registry = registry.clone();
    let peers: Vec<(u64, String)> = peers.to_vec();
    tokio::spawn(async move {
        let interval = Duration::from_millis(u64::try_from(interval_ms).unwrap_or(5_000));
        let client = match reqwest::Client::builder()
            .timeout(Duration::from_millis(
                u64::try_from(peer_timeout_ms).unwrap_or(500),
            ))
            .build()
        {
            Ok(client) => client,
            Err(err) => {
                tracing::error!("leadership-balance: failed to build peer-status client: {err}");
                return;
            }
        };
        loop {
            tokio::time::sleep(interval).await;
            let snaps = registry.metrics_snapshot();
            if snaps.is_empty() {
                continue;
            }
            let my_id = snaps[0].node_id;
            let eligible_nodes =
                leadership_balance_eligible_nodes(&registry, node_id, &peers, &client).await;
            let actions = plan_leadership_balance_with_eligible_nodes(
                &snaps,
                my_id,
                max_per_tick,
                &eligible_nodes,
            );
            for action in actions {
                let Some(raft) = registry.get(RaftGroupId(action.group_id)) else {
                    continue;
                };
                match raft.trigger().transfer_leader(action.target).await {
                    Ok(()) => tracing::warn!(
                        "leadership-balance: node {my_id} handing group {} -> node {} (fair={})",
                        action.group_id,
                        action.target,
                        action.fair
                    ),
                    Err(err) => tracing::error!(
                        "leadership-balance: transfer_leader group {} -> {} failed: {err}",
                        action.group_id,
                        action.target
                    ),
                }
            }
        }
    });
}

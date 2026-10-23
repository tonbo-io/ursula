//! M1, leadership balancing (design 5.9). Spreads data-group leadership
//! evenly across the nodes that may lead, so one node's failure or network
//! impairment stalls at most its fair share of the groups rather than the
//! whole cluster.
//!
//! Each node runs it independently and moves only the groups it leads. A
//! node's fair share is `ceil(groups / eligible nodes)`: every data group
//! of the bootstrapped view, over the nodes that may campaign and have a
//! role in some group's placement that takes a leadership. The counts come
//! from the participation answers, each peer reporting how many groups it
//! leads, so a node needs no replica of a group to count its leader. A
//! transfer from this node to `t` needs `mine > fair`, `count(t) < fair`
//! and `count(t) + 1 < mine`, the transfers planned before it counted, so
//! each one strictly lowers the sum of the squared counts and the balancer
//! settles at zero transfers. A target is one of [`handoff_targets`]: a
//! voter that may campaign, never an outgoing or abandoned replica. A
//! planned handoff fires only when the registry's handoff policy accepts the
//! target (it answered recently, holds every committed entry and did not
//! lose its log). Otherwise the group is retried on a later tick.

use std::collections::BTreeMap;
use std::collections::HashSet;
use std::time::Duration;

use tokio::time::Instant;
use ursula_control::Cluster;
use ursula_raft::LeadershipShedReason;
use ursula_raft::RaftGroupHandleRegistry;
use ursula_shard::RaftGroupId;

use crate::bootstrap::health_gate::LeadershipShed;
use crate::bootstrap::health_gate::ShedMark;
use crate::bootstrap::health_gate::ShedPace;
use crate::bootstrap::participation::CampaignStance;
use crate::bootstrap::participation::CampaignStances;
use crate::bootstrap::participation::PeerParticipation;
use crate::bootstrap::util::handoff_targets;

/// How long the shutdown handoff waits between its steps.
const SHUTDOWN_HANDOFF_STEP: Duration = Duration::from_millis(50);

/// One planned leader handoff for the M1 balancer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LeadershipBalanceAction {
    pub group_id: u32,
    pub target: u64,
    pub fair: usize,
}

/// A group this node leads, with the nodes that may take its leadership
/// over ([`handoff_targets`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LedGroup {
    pub group_id: u32,
    pub targets: Vec<u64>,
}

/// What one balancer tick plans from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BalanceInputs {
    pub me: u64,
    /// The fair share of leaderships per node ([`fair_share`]).
    pub fair: usize,
    /// How many data groups each node leads, this node's included. A node
    /// without a count did not answer and takes no leadership.
    pub counts: BTreeMap<u64, usize>,
    pub led: Vec<LedGroup>,
}

/// `ceil(groups / eligible)`, `None` without an eligible node.
pub(crate) fn fair_share(groups: usize, eligible: usize) -> Option<usize> {
    (eligible > 0).then(|| groups.div_ceil(eligible))
}

/// The handoffs one tick attempts, at most `max_per_tick` (0: no cap), in
/// group order, each to the target that leads the fewest groups, then the
/// smallest id, that the rule of the module docs admits. Pure, so that
/// every node computes the same plan from the same counts.
pub(crate) fn plan_leadership_balance(
    inputs: &BalanceInputs,
    max_per_tick: usize,
) -> Vec<LeadershipBalanceAction> {
    let BalanceInputs {
        me,
        fair,
        counts,
        led,
    } = inputs;
    let mut planned = counts.clone();
    let Some(mut mine) = planned.get(me).copied() else {
        return Vec::new();
    };
    let mut led: Vec<&LedGroup> = led.iter().collect();
    led.sort_by_key(|group| group.group_id);
    let mut actions = Vec::new();
    for group in led {
        if mine <= *fair || (max_per_tick > 0 && actions.len() >= max_per_tick) {
            break;
        }
        let target = group
            .targets
            .iter()
            .filter_map(|target| Some((*planned.get(target)?, *target)))
            .filter(|(count, _)| *count < *fair && count.saturating_add(1) < mine)
            .min();
        let Some((_, target)) = target else {
            continue;
        };
        actions.push(LeadershipBalanceAction {
            group_id: group.group_id,
            target,
            fair: *fair,
        });
        if let Some(count) = planned.get_mut(&target) {
            *count = count.saturating_add(1);
        }
        mine = mine.saturating_sub(1);
        planned.insert(*me, mine);
    }
    actions
}

/// The inputs of node `me`'s tick: its replicas' metrics `snaps`, its
/// bootstrapped view's `cluster`, and the `stances` and counts its peers
/// answered. `None` when no node may lead.
pub(crate) fn balance_inputs(
    snaps: &[ursula_raft::RaftGroupMetricsSnapshot],
    me: u64,
    cluster: &Cluster,
    stances: &CampaignStances,
) -> Option<BalanceInputs> {
    let may_campaign = stances.may_campaign();
    let groups = cluster.groups().len();
    let eligible = cluster
        .nodes()
        .keys()
        .filter(|node| may_campaign.contains(&node.get()))
        .filter(|node| {
            cluster.groups().iter().any(|(_, placement)| {
                placement
                    .role(**node)
                    .is_some_and(|role| role.takes_leadership())
            })
        })
        .count();
    let fair = fair_share(groups, eligible)?;
    let led = snaps
        .iter()
        .filter(|snap| snap.current_leader == Some(me))
        .map(|snap| LedGroup {
            group_id: snap.raft_group_id,
            targets: handoff_targets(snap, me, Some(cluster), |node| may_campaign.contains(&node)),
        })
        .collect();
    Some(BalanceInputs {
        me,
        fair,
        counts: stances.leader_counts(),
        led,
    })
}

/// The shutdown handoff, while Raft transport is still available, for at
/// most `grace`: the maintenance-drain shed of every leadership this node
/// holds, at once ([`LeadershipShed`], [`ShedPace::AllAtOnce`]), the drain
/// the health gates run. Every [`SHUTDOWN_HANDOFF_STEP`] it asks the peers
/// which of them may take a transfer, logging each peer's answer whenever
/// one changes, and submits every queued leadership to one of them that the
/// registry's handoff check accepts. A leadership no peer could take on one
/// step is tried again on the next, and the groups no peer can take are
/// reported with the check's refusals. A submitted leadership waits for its
/// transfer to settle (`handoff_settle_time`, longer than the grace with
/// the shipped timeouts), so within the grace each leadership is submitted
/// at most once. Its shed also takes on the leaderships the node gains
/// while it runs. The drain mark it sets holds until the process exits
/// ([`ShedMark::UntilExit`]), so the node does not campaign again, and makes
/// the meta shed hand meta's leadership away (`super::meta_shed`), the one
/// place meta's leadership is handed off.
///
/// Answers how many leaderships are left here. When some are once the
/// grace ends, it reports each with the handoff check's last refusals, and
/// the caller steps the node down from them
/// (`RaftGroupHandleRegistry::step_down_leaderships`). Enqueuing a transfer
/// is not completion: a group counts until another node leads it.
pub(crate) async fn handoff_shutdown_leadership(
    registry: &RaftGroupHandleRegistry,
    node_id: u64,
    grace: Duration,
) -> usize {
    let deadline = Instant::now()
        .checked_add(grace)
        .unwrap_or_else(Instant::now);
    let mut shed = LeadershipShed::new(
        registry,
        LeadershipShedReason::MaintenanceDrain,
        node_id,
        ShedPace::AllAtOnce,
        ShedMark::UntilExit,
    );
    shed.begin();
    let led_groups: HashSet<_> = registry
        .metrics_snapshot()
        .iter()
        .filter(|snapshot| snapshot.current_leader == Some(node_id))
        .map(|snapshot| snapshot.raft_group_id)
        .collect();
    let led_meta = crate::bootstrap::meta_shed::leads_meta(registry, node_id);
    // Meta counts until another node leads it, as a data group does.
    let meta_remains = || {
        registry.meta_metrics_snapshot().is_some_and(|meta| {
            meta.current_leader == Some(node_id) || (led_meta && meta.current_leader.is_none())
        })
    };
    let remaining_leaders = || {
        registry
            .metrics_snapshot()
            .iter()
            .filter(|snapshot| {
                snapshot.current_leader == Some(node_id)
                    || (led_groups.contains(&snapshot.raft_group_id)
                        && snapshot.current_leader.is_none())
            })
            .count()
            .saturating_add(usize::from(meta_remains()))
    };
    if remaining_leaders() == 0 {
        return 0;
    }
    let participation = match reqwest::Client::builder()
        .timeout(Duration::from_millis(500))
        .build()
    {
        Ok(client) => PeerParticipation::new(node_id, client),
        Err(err) => {
            tracing::warn!(%err, "shutdown: cannot discover eligible leadership targets");
            return remaining_leaders();
        }
    };
    let mut answered: Option<BTreeMap<u64, CampaignStance>> = None;
    let handoff = async {
        loop {
            if remaining_leaders() == 0 {
                return;
            }
            let stances = participation.campaign_stances(registry).await;
            if answered.as_ref() != Some(stances.by_node()) {
                tracing::warn!(
                    node_id,
                    stances = ?stances.by_node(),
                    "shutdown: the peers' answers on taking a leadership transfer"
                );
                answered = Some(stances.by_node().clone());
            }
            shed.step(&stances.shed_targets(), &[]).await;
            tokio::time::sleep(SHUTDOWN_HANDOFF_STEP).await;
        }
    };
    if tokio::time::timeout_at(deadline, handoff).await.is_err() {
        shed.report_pending();
    }
    remaining_leaders()
}

/// Config-driven leadership balancer.
pub fn spawn_leadership_balancer(
    registry: &RaftGroupHandleRegistry,
    participation: &PeerParticipation,
    lb_cfg: &ursula_config::LeadershipBalanceConfig,
) {
    let interval_ms = lb_cfg.interval.as_duration().as_millis() as usize;
    if interval_ms == 0 {
        return;
    }
    let max_per_tick = lb_cfg.max_per_tick;
    let registry = registry.clone();
    let participation = participation.clone();
    let my_id = participation.node_id();
    tokio::spawn(async move {
        let interval = Duration::from_millis(u64::try_from(interval_ms).unwrap_or(5_000));
        loop {
            tokio::time::sleep(interval).await;
            let view = registry.placement_watch().current();
            // Without a bootstrapped view no node has a role to lead by.
            let Some(cluster) = view.cluster() else {
                continue;
            };
            let snaps = registry.metrics_snapshot();
            if !snaps.iter().any(|snap| snap.current_leader == Some(my_id)) {
                continue;
            }
            let stances = participation.campaign_stances(&registry).await;
            let Some(inputs) = balance_inputs(&snaps, my_id, cluster, &stances) else {
                continue;
            };
            let actions = plan_leadership_balance(&inputs, max_per_tick);
            for action in actions {
                if let Err(err) =
                    registry.check_handoff(RaftGroupId(action.group_id), action.target)
                {
                    tracing::debug!(
                        "leadership-balance: node {my_id} skips group {} -> node {}: {err}",
                        action.group_id,
                        action.target
                    );
                    continue;
                }
                match registry
                    .transfer_leader(RaftGroupId(action.group_id), action.target)
                    .await
                {
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

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::collections::BTreeSet;

    use proptest::prelude::*;
    use ursula_control::samples;
    use ursula_raft::RaftGroupMetricsSnapshot;

    use super::BalanceInputs;
    use super::LedGroup;
    use super::balance_inputs;
    use super::fair_share;
    use super::plan_leadership_balance;
    use crate::bootstrap::participation::CampaignStance;
    use crate::bootstrap::participation::CampaignStances;
    use crate::bootstrap::util::handoff_targets;

    /// Group `group`'s metrics on node 1, led by `leader`, with `voters`.
    fn snap(group: u32, leader: u64, voters: &[u64]) -> RaftGroupMetricsSnapshot {
        RaftGroupMetricsSnapshot {
            apply_failure: None,
            raft_group_id: group,
            node_id: 1,
            current_term: 1,
            current_leader: Some(leader),
            last_log: None,
            committed: None,
            last_applied: None,
            snapshot: None,
            purged: None,
            voter_ids: voters.to_vec(),
            voter_configurations: vec![voters.iter().copied().collect()],
            committed_voter_configurations: vec![voters.iter().copied().collect()],
            learner_ids: Vec::new(),
            maintenance: ursula_raft::RaftGroupMaintenanceState::default(),
            log: Default::default(),
            history: None,
            recovery: None,
            recovery_gate: ursula_proto::admin::RecoveryGateStatus::Open,
            leads_live: true,
            reconcile: None,
            needs_vote_floor: false,
        }
    }

    /// The one exclusion of design 5.9. The bugs it catches: a leadership
    /// handed to an outgoing or abandoned replica, to a member the
    /// placement gives no role, or to a node that may not campaign.
    #[test]
    fn a_handoff_goes_only_to_a_voter_whose_role_takes_a_leadership() {
        // Group 2 moves from {1, 3} to {1, 2}, with 5 abandoned.
        let state = samples::sample_state();
        let cluster = state.cluster().unwrap();
        let moving = snap(2, 1, &[1, 2, 3, 5, 6]);
        assert_eq!(handoff_targets(&moving, 1, Some(cluster), |_| true), vec![
            2
        ]);
        assert_eq!(
            handoff_targets(&moving, 1, Some(cluster), |node| node != 2),
            Vec::<u64>::new()
        );
        // The meta group, or a node without a bootstrapped view, has no
        // placement: every other admitted voter.
        assert_eq!(handoff_targets(&moving, 1, None, |node| node != 6), vec![
            2, 3, 5
        ]);
    }

    /// The fair share counts every group of the view over the nodes that
    /// may campaign and have a role that takes a leadership, and the counts
    /// are the answered ones. The bugs it catches: a share over the groups
    /// and voters this node sees, which never settles once a node hosts
    /// fewer than all groups, and a share that counts a node about to leave
    /// every group it is in.
    #[test]
    fn the_fair_share_counts_every_group_over_the_nodes_that_may_lead() {
        // Three groups. Node 5 is only an abandoned target and node 4 has
        // no role, so nodes 1, 2 and 3 may lead.
        let state = samples::sample_state();
        let cluster = state.cluster().unwrap();
        let stances = |refusing: u64| {
            CampaignStances::of_nodes(
                1,
                (1..=5).map(|node| {
                    let stance = if node == refusing {
                        CampaignStance::MayNotCampaign
                    } else {
                        CampaignStance::MayCampaign
                    };
                    (node, stance)
                }),
            )
            .with_leader_counts([(1, 2), (2, 1)])
        };
        let inputs = balance_inputs(&[], 1, cluster, &stances(0)).unwrap();
        assert_eq!(inputs.fair, 1);
        assert_eq!(inputs.counts, BTreeMap::from([(1, 2), (2, 1)]));
        assert!(inputs.led.is_empty());
        assert_eq!(
            balance_inputs(&[], 1, cluster, &stances(3)).unwrap().fair,
            2
        );
        let nobody = CampaignStances::of_nodes(1, [(1, CampaignStance::MayNotCampaign)]);
        assert_eq!(balance_inputs(&[], 1, cluster, &nobody), None);
    }

    /// A transfer goes only to a node below its share that ends below this
    /// one, the least loaded first, and never to a node without a count.
    #[test]
    fn a_transfer_goes_to_the_least_loaded_counted_target_below_its_share() {
        let inputs = BalanceInputs {
            me: 1,
            fair: 3,
            counts: BTreeMap::from([(1, 6), (2, 2), (3, 1), (4, 3)]),
            led: (0..6)
                .map(|group_id| LedGroup {
                    group_id,
                    targets: vec![2, 3, 4, 5],
                })
                .collect(),
        };
        let targets: Vec<(u32, u64)> = plan_leadership_balance(&inputs, 0)
            .into_iter()
            .map(|action| (action.group_id, action.target))
            .collect();
        // Group 0 goes to node 3, which leads one group, group 1 to node 2,
        // the lower id of the two leading two, and group 2 to node 3 again.
        // Node 1 then holds its share. Node 4 holds its share already, and
        // node 5 did not answer.
        assert_eq!(targets, vec![(0, 3), (1, 2), (2, 3)]);
        assert_eq!(plan_leadership_balance(&inputs, 1).len(), 1);
    }

    /// Deterministic mixing for the generated layouts.
    fn mix(a: u64, b: u64) -> u64 {
        (a ^ b.wrapping_mul(0x9E37_79B9_7F4A_7C15))
            .rotate_left(29)
            .wrapping_mul(0xBF58_476D_1CE4_E5B9)
    }

    fn sum_of_squares(counts: &BTreeMap<u64, usize>) -> usize {
        counts
            .values()
            .map(|count| count.saturating_mul(*count))
            .fold(0, usize::saturating_add)
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(256))]

        /// Design 5.9 for `N` 3 to 7 and RF 2 and 3: the nodes take turns,
        /// each planning from the counts as they stand, every transfer meets
        /// the three conditions and strictly lowers the sum of the squared
        /// counts, and the cluster settles at zero transfers. Every node gets
        /// the same correct `fair`, so the property never reaches a wrong
        /// share: the old share over the voters a node sees is
        /// `the_fair_share_counts_every_group_over_the_nodes_that_may_lead`'s
        /// to catch. With a shared `fair`, `count(t) + 1 < mine` also follows
        /// from `mine > fair` and `count(t) < fair`, so it is never exercised
        /// on its own. The bugs it catches are in the transfer rule: one that
        /// does not settle, such as `mine > fair` with no condition on the
        /// target, which moves a group back and forth between a node at
        /// `fair + 1` and one at `fair`, and one without `count(t) < fair`,
        /// which the per-transfer checks fail once it pushes a target over
        /// its share.
        #[test]
        fn the_balancer_settles_and_every_transfer_strictly_improves(
            nodes in 3_u64..=7,
            rf in 2_usize..=3,
            layout in prop::collection::vec(any::<u64>(), 1..=48),
            max_per_tick in 0_usize..=4,
        ) {
            let ids: Vec<u64> = (1..=nodes).collect();
            let voters: Vec<Vec<u64>> = layout
                .iter()
                .map(|seed| {
                    let mut order = ids.clone();
                    order.sort_by_key(|node| mix(*seed, *node));
                    order.truncate(rf);
                    order
                })
                .collect();
            let mut leaders: Vec<u64> = layout
                .iter()
                .zip(&voters)
                .map(|(seed, voters)| {
                    voters[usize::try_from(mix(*seed, 0)).unwrap().checked_rem(rf).unwrap()]
                })
                .collect();
            let fair = fair_share(voters.len(), ids.len()).unwrap();
            let counts = |leaders: &[u64]| {
                let mut counts: BTreeMap<u64, usize> = ids.iter().map(|node| (*node, 0)).collect();
                for leader in leaders {
                    let count = counts.get_mut(leader).unwrap();
                    *count = count.saturating_add(1);
                }
                counts
            };
            let bound = sum_of_squares(&counts(&leaders));
            let mut settled = false;
            for _round in 0..=bound {
                let mut moved = false;
                for me in &ids {
                    let inputs = BalanceInputs {
                        me: *me,
                        fair,
                        counts: counts(&leaders),
                        led: leaders
                            .iter()
                            .enumerate()
                            .filter(|(_, leader)| *leader == me)
                            .map(|(group, _)| LedGroup {
                                group_id: u32::try_from(group).unwrap(),
                                targets: voters[group].iter().copied().filter(|node| node != me).collect(),
                            })
                            .collect(),
                    };
                    for action in plan_leadership_balance(&inputs, max_per_tick) {
                        let group = usize::try_from(action.group_id).unwrap();
                        let before = counts(&leaders);
                        prop_assert_eq!(leaders[group], *me);
                        prop_assert!(voters[group].contains(&action.target));
                        prop_assert!(before[me] > fair);
                        prop_assert!(before[&action.target] < fair);
                        prop_assert!(before[&action.target].saturating_add(1) < before[me]);
                        leaders[group] = action.target;
                        prop_assert!(sum_of_squares(&counts(&leaders)) < sum_of_squares(&before));
                        moved = true;
                    }
                }
                if !moved {
                    settled = true;
                    break;
                }
            }
            prop_assert!(settled, "no settled state within {} rounds", bound);
            let led: BTreeSet<u64> = leaders.iter().copied().collect();
            prop_assert!(led.iter().all(|node| ids.contains(node)));
        }
    }
}

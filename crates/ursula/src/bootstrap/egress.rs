use std::collections::BTreeMap;
use std::collections::BTreeSet;

use futures_util::future::join_all;
use ursula_proto::admin::EgressProbeStatus;
use ursula_raft::LeadershipShedReason;
use ursula_raft::PeerDirectory;
use ursula_raft::RaftGroupHandleRegistry;
use ursula_shard::RaftGroupId;

use crate::bootstrap::health_gate::HealthHysteresis;
use crate::bootstrap::health_gate::HealthTransition;
use crate::bootstrap::health_gate::LeadershipShed;
use crate::bootstrap::health_gate::ShedMark;
use crate::bootstrap::health_gate::ShedPace;
use crate::bootstrap::health_gate::TickHealth;
use crate::bootstrap::participation::CampaignStances;
use crate::bootstrap::participation::PeerParticipation;

/// M2 — egress-health leadership gate. Each node actively measures its own
/// egress to peers over the cluster plane (a payload POST with a tight
/// deadline, sized so 15% loss / added delay fails it while a healthy link
/// passes trivially — a heartbeat-sized probe would mask loss). A node
/// that cannot push to a quorum of peers cannot replicate, so it sheds
/// leadership and disables elections, but only while that is a fault of
/// this node ([`EgressFault`]): some peer can do better, a peer whose own
/// last probe was clean, as its participation answer reports, or too few
/// peers answer this node at all for a quorum of any of its groups, as when
/// it is cut off or on the minority side of a partition. A delay or loss on
/// every node's path is no fault of this node: every peer still answers
/// within the bound a Raft append gets, and handing its leaderships to
/// nodes no better off would only leave groups without a leader while each
/// refuses them. A node that cannot reach a quorum must stop campaigning,
/// though: Ursula runs without pre-vote, so each of its elections raises
/// the group's term, and once the cut heals that term deposes the group's
/// leader. So it sheds on its first tick that hears too few peers, which
/// ends one tick interval plus [`ursula_raft::PEER_ANSWER_TIMEOUT`] after
/// the cut at the latest, 2 s with the default 500 ms interval. A replica
/// of it that follows a group may campaign once before that, but not
/// twice: its first election comes at least the minimum election timeout
/// less a heartbeat wait (721 ms) after the cut, 779 ms, and its second a
/// minimum election timeout (1.5 s) later. Only pre-vote, or a quorum
/// check before campaigning, would keep that one election from raising
/// the group's term. The node clears this reason once its egress is
/// clean, or the fault is gone, and re-enables elections only if no other
/// shed reason remains.
///
/// Unlike inferring health from commit progress (load-dependent, and gone once
/// the client gives up), this probe is role-independent: a yielded follower
/// keeps measuring its own egress, so recovery is self-evident even though
/// egress-only loss leaves its inbound replication looking healthy. That is
/// what prevents the flap that a commit-stall step-down suffers.
///
/// The egress probe of one tick: every peer of this node's groups, probed
/// once whatever the group count, and each voter config of each group
/// judged by how many of its other voters answered.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ClusterEgressProbePlan {
    /// The cluster URL of every peer to probe, by node id.
    pub peer_urls: BTreeMap<u64, String>,
    /// One entry per voter config of each of this node's groups, each with
    /// the peers it needs answers from.
    pub groups: Vec<ClusterEgressProbeGroup>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ClusterEgressProbeGroup {
    pub raft_group_id: RaftGroupId,
    /// The config's voters other than this node that `raft.peers` lists.
    pub peers: BTreeSet<u64>,
    pub needed_peers: usize,
}

impl ClusterEgressProbePlan {
    pub(crate) fn is_empty(&self) -> bool {
        self.groups.is_empty()
    }

    /// The first group config that fewer of its peers answered than its
    /// quorum needs, with how many did.
    pub(crate) fn degraded(
        &self,
        answered: &BTreeSet<u64>,
    ) -> Option<(&ClusterEgressProbeGroup, usize)> {
        self.groups.iter().find_map(|group| {
            let healthy = group.peers.intersection(answered).count();
            (healthy < group.needed_peers).then_some((group, healthy))
        })
    }

    /// Whether fewer of its peers `answered` than a quorum needs in every
    /// group config of the plan: this node could lead none of its groups.
    pub(crate) fn isolated(&self, answered: &BTreeSet<u64>) -> bool {
        !self.groups.is_empty()
            && self
                .groups
                .iter()
                .all(|group| group.peers.intersection(answered).count() < group.needed_peers)
    }
}

/// Why this node's degraded probe is a fault of its own, which sheds its
/// leaderships ([`egress_fault`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum EgressFault {
    /// Fewer peers answered this node's participation question than a
    /// quorum needs in every one of its group configs, `answered` of them:
    /// the node is cut off from its peers, or on the minority side of a
    /// partition, leads none of its groups and must not campaign. A node
    /// that hears a quorum of one of its groups is not isolated, since it
    /// stops campaigning in every group at once, and some group it can
    /// lead would be left without a candidate.
    Isolated { answered: usize },
    /// `peer` answers that its own last probe was clean: it reaches its
    /// quorums where this node does not.
    PeerDoesBetter { peer: u64 },
}

/// Whether a degraded probe under `plan` is a fault of this node, by its
/// peers' `stances`: too few of them answered for a quorum of any of its
/// groups ([`ClusterEgressProbePlan::isolated`] over the peers that
/// answered), or one of them reports a clean probe. `None` when every peer
/// answered as degraded too: the delay or loss is on every node's path.
pub(crate) fn egress_fault(
    plan: &ClusterEgressProbePlan,
    stances: &CampaignStances,
) -> Option<EgressFault> {
    let answered = stances.answered_peers();
    if plan.isolated(&answered) {
        return Some(EgressFault::Isolated {
            answered: answered.len(),
        });
    }
    stances
        .peer_with_clean_egress()
        .map(|peer| EgressFault::PeerDoesBetter { peer })
}

/// The other voters a quorum of `voters` needs besides `node_id`: a
/// majority, less this node when it is one of them.
fn remote_peers_needed_for_quorum(node_id: u64, voters: &BTreeSet<u64>) -> usize {
    let quorum = (voters.len() / 2).saturating_add(1);
    quorum.saturating_sub(usize::from(voters.contains(&node_id)))
}

/// The probe plan of `node_id` over `groups`, the voter configs of the
/// effective membership of each replica that counts as a voter
/// ([`RaftGroupHandleRegistry::counting_voter_configs`]).
/// A group none of whose configs lists this node is not probed: the node
/// cannot lead it, so the group's peers say nothing of the leaderships it
/// would shed, even while the replica counts by its placed role. Every
/// config of a group that lists the node in one is probed, since a leader
/// of a joint membership needs a quorum of each. A config none of whose
/// other voters `raft.peers` lists is not probed.
pub(crate) fn cluster_egress_probe_plan(
    node_id: u64,
    peers: &PeerDirectory,
    groups: &BTreeMap<RaftGroupId, Vec<BTreeSet<u64>>>,
) -> ClusterEgressProbePlan {
    let mut peer_urls = BTreeMap::new();
    let groups = groups
        .iter()
        .filter(|(_, configs)| configs.iter().any(|voters| voters.contains(&node_id)))
        .flat_map(|(raft_group_id, configs)| {
            configs.iter().map(move |voters| (*raft_group_id, voters))
        })
        .filter_map(|(raft_group_id, voters)| {
            let group_peers: BTreeSet<u64> = voters
                .iter()
                .copied()
                .filter(|id| *id != node_id)
                .filter(|id| match peers.cluster_url(*id) {
                    Some(url) => {
                        peer_urls.entry(*id).or_insert_with(|| url.to_owned());
                        true
                    }
                    None => false,
                })
                .collect();
            if group_peers.is_empty() {
                return None;
            }
            Some(ClusterEgressProbeGroup {
                raft_group_id,
                peers: group_peers,
                needed_peers: remote_peers_needed_for_quorum(node_id, voters),
            })
        })
        .collect();
    ClusterEgressProbePlan { peer_urls, groups }
}

/// Cluster egress gate. Each tick it probes the peers of `participation`
/// ([`PeerParticipation::peers`], the bound `raft.peers` read at each tick)
/// that are voters of the groups whose replica on this node counts as a
/// voter ([`cluster_egress_probe_plan`]), and records the verdict in the
/// registry, which this node's participation answer reports. A degraded
/// tick counts against the node only while that is its own fault
/// ([`egress_fault`]), which the peers' answers decide. Each tick asks for
/// them alongside its probes, and a degraded tick waits up to
/// [`ursula_raft::PEER_ANSWER_TIMEOUT`] for them. One tick that hears too
/// few peers for a quorum of any of its groups sheds ([`EgressFault::Isolated`]), and a peer
/// that does better sheds after `unhealthy_ticks` ticks. While it
/// sheds, the gate hands every leadership it held when it shed to its
/// peers at once, and plans again on every tick for the ones still
/// pending, each only to a peer that answered that it may campaign
/// ([`CampaignStances::shed_targets`]).
pub fn spawn_egress_gate(
    registry: &RaftGroupHandleRegistry,
    participation: &PeerParticipation,
    cp_cfg: &ursula_config::ClusterProbeConfig,
) {
    let node_id = participation.node_id();
    let interval = cp_cfg.interval.as_duration();
    if interval.is_zero() {
        return;
    }
    let probe_bytes = cp_cfg.probe_size.as_bytes() as usize;
    let unhealthy_ticks = cp_cfg.unhealthy_ticks;
    let heal_ticks = cp_cfg.heal_ticks;
    let client = match reqwest::Client::builder()
        .timeout(cp_cfg.timeout.as_duration())
        .build()
    {
        Ok(client) => client,
        Err(err) => {
            tracing::error!("cluster-egress: failed to build probe client: {err}");
            return;
        }
    };
    let registry = registry.clone();
    let participation = participation.clone();
    tokio::spawn(async move {
        let payload = vec![0u8; probe_bytes];
        let mut hysteresis = HealthHysteresis::new(unhealthy_ticks, heal_ticks);
        let mut degraded_without_fault = false;
        // A node that cannot reach a quorum cannot replicate, so its
        // leaderships stall where they are: hand them all off at once. The
        // shed ends when the task does, however it stops.
        let mut shed = LeadershipShed::new(
            &registry,
            LeadershipShedReason::ClusterEgress,
            node_id,
            ShedPace::AllAtOnce,
            ShedMark::UntilEnd,
        );
        loop {
            tokio::time::sleep(interval).await;
            let plan = cluster_egress_probe_plan(
                node_id,
                &participation.peers(&registry),
                &registry.counting_voter_configs(),
            );
            if plan.is_empty() {
                registry.record_egress_probe(EgressProbeStatus::Unprobed);
                continue;
            }
            // The peers' answers are asked in the same round as the probes,
            // so a node cut off from its peers knows it one answer timeout
            // after the round starts. They decide something only for a
            // degraded probe or a shed that drains, so otherwise the round
            // drops the question once the probes are in, rather than hold
            // the next tick for a silent peer.
            let stances = {
                let participation = participation.clone();
                let registry = registry.clone();
                tokio::spawn(async move { participation.campaign_stances(&registry).await })
            };
            // Each peer once per tick, whatever the group count, all at once.
            let answered: BTreeSet<u64> = join_all(plan.peer_urls.iter().map(|(peer_id, url)| {
                let probe = client
                    .post(format!("{url}{}", crate::CLUSTER_PROBE_PATH))
                    .body(payload.clone());
                async move {
                    match probe.send().await {
                        Ok(resp) if resp.status().is_success() => Some(*peer_id),
                        _ => None,
                    }
                }
            }))
            .await
            .into_iter()
            .flatten()
            .collect();
            let degraded_probe = plan.degraded(&answered).map(|(group, healthy_peers)| {
                (
                    group.raft_group_id,
                    healthy_peers,
                    group.peers.len(),
                    group.needed_peers,
                )
            });
            registry.record_egress_probe(if degraded_probe.is_some() {
                EgressProbeStatus::Degraded
            } else {
                EgressProbeStatus::Clean
            });
            let stances = if degraded_probe.is_some() || shed.is_draining() {
                match stances.await {
                    Ok(stances) => Some(stances),
                    Err(err) => {
                        tracing::warn!(%err, "cluster-egress: the peers' answers were lost");
                        None
                    }
                }
            } else {
                stances.abort();
                None
            };
            let fault = degraded_probe.and(
                stances
                    .as_ref()
                    .and_then(|stances| egress_fault(&plan, stances)),
            );
            // A degraded probe that is no fault of this node turns the gate
            // off for it: say so once per such stretch, since a probe that
            // fails on every node turns it off for the whole cluster.
            let unattributed = degraded_probe.is_some() && fault.is_none();
            if unattributed && !degraded_without_fault {
                tracing::warn!(
                    "cluster-egress: node {node_id} egress degraded, but enough peers answer for its quorums and none reports a clean probe; keeping leadership"
                );
            }
            degraded_without_fault = unattributed;
            // A node that hears too few peers for a quorum sheds on its
            // first such tick: in every tick it waited, its replicas that
            // follow a group could campaign and raise the group's term.
            let tick = match fault {
                Some(EgressFault::Isolated { .. }) => TickHealth::Disqualifying,
                Some(EgressFault::PeerDoesBetter { .. }) => TickHealth::Bad,
                None => TickHealth::Good,
            };
            match hysteresis.observe(tick) {
                HealthTransition::Shed => {
                    if let (Some((group, healthy_peers, peer_count, needed_peers)), Some(fault)) =
                        (&degraded_probe, fault)
                    {
                        match fault {
                            EgressFault::Isolated { answered } => tracing::warn!(
                                "cluster-egress: node {node_id} group {} egress degraded (reached {healthy_peers}/{peer_count} peers, need {needed_peers}), and {answered} peers answered, too few for a quorum of any of its groups; yielding leadership and elections",
                                group.0,
                            ),
                            EgressFault::PeerDoesBetter { peer } => tracing::warn!(
                                "cluster-egress: node {node_id} group {} egress degraded (reached {healthy_peers}/{peer_count} peers, need {needed_peers}) while node {peer} reports a clean probe; yielding leadership",
                                group.0,
                            ),
                        }
                    }
                    shed.begin();
                }
                HealthTransition::Heal => shed.end(),
                HealthTransition::Hold => {}
            }
            // A shed that begins this tick began on a degraded probe, so
            // the answers are in whenever a shed drains.
            if shed.is_draining()
                && let Some(stances) = &stances
            {
                shed.step(&stances.shed_targets(), &[]).await;
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::collections::BTreeSet;

    use ursula_proto::admin::EgressProbeStatus;
    use ursula_shard::RaftGroupId;

    use super::ClusterEgressProbeGroup;
    use super::ClusterEgressProbePlan;
    use super::EgressFault;
    use super::egress_fault;
    use crate::bootstrap::participation::CampaignStance;
    use crate::bootstrap::participation::CampaignStances;

    /// Node 1's plan over group 0 of voters {1, 2, 3} and group 1 of
    /// voters {1, 2, 3, 4, 5}: one other voter answers for a quorum of
    /// group 0, two for one of group 1.
    fn plan() -> ClusterEgressProbePlan {
        ClusterEgressProbePlan {
            peer_urls: (2..=5)
                .map(|id| (id, format!("http://node-{id}")))
                .collect(),
            groups: vec![
                ClusterEgressProbeGroup {
                    raft_group_id: RaftGroupId(0),
                    peers: BTreeSet::from([2, 3]),
                    needed_peers: 1,
                },
                ClusterEgressProbeGroup {
                    raft_group_id: RaftGroupId(1),
                    peers: BTreeSet::from([2, 3, 4, 5]),
                    needed_peers: 2,
                },
            ],
        }
    }

    /// Node 1's view of its peers: each of `answered` answered with the
    /// egress verdict it holds, and every other peer did not answer.
    fn stances(answered: &[(u64, EgressProbeStatus)]) -> CampaignStances {
        let answered: BTreeMap<u64, EgressProbeStatus> = answered.iter().copied().collect();
        let by_node = (1..=5).map(|id| {
            let stance = if id == 1 || answered.contains_key(&id) {
                CampaignStance::MayCampaign
            } else {
                CampaignStance::Unknown
            };
            (id, stance)
        });
        CampaignStances::of_nodes(1, by_node).with_egress(answered)
    }

    /// A node that too few peers answer for a quorum of any of its groups
    /// is isolated, whatever their probes say: it must stop campaigning,
    /// which a peer's clean probe alone would not say. A node that hears a
    /// quorum of one group is not, since it stops campaigning in every
    /// group at once and would leave that one without a candidate. Every
    /// peer answering degraded, with enough of them for each quorum, is no
    /// fault of this node.
    #[test]
    fn only_too_few_answers_for_every_quorum_isolate_a_node() {
        use EgressProbeStatus::Clean;
        use EgressProbeStatus::Degraded;

        let plan = plan();
        assert_eq!(
            egress_fault(&plan, &stances(&[(4, Clean)])),
            Some(EgressFault::Isolated { answered: 1 }),
            "node 4 is no quorum of group 0, and one answer none of group 1"
        );
        assert_eq!(
            egress_fault(&plan, &stances(&[])),
            Some(EgressFault::Isolated { answered: 0 })
        );
        assert_eq!(
            egress_fault(&plan, &stances(&[(2, Degraded)])),
            None,
            "node 2 is a quorum of group 0, though not of group 1"
        );
        assert_eq!(
            egress_fault(&plan, &stances(&[(2, Degraded), (4, Clean)])),
            Some(EgressFault::PeerDoesBetter { peer: 4 })
        );
        assert_eq!(
            egress_fault(
                &plan,
                &stances(&[(2, Degraded), (3, Degraded), (5, Degraded)])
            ),
            None
        );
    }
}

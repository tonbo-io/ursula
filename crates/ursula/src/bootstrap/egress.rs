use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::time::Duration;

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
use crate::bootstrap::participation::PeerParticipation;

const DEFAULT_CLUSTER_PROBE_INTERVAL_MS: usize = 500;
const DEFAULT_CLUSTER_PROBE_TIMEOUT_MS: usize = 200;

/// M2 — egress-health leadership gate. Each node actively measures its own
/// egress to peers over the cluster plane (a payload POST with a tight
/// deadline, sized so 15% loss / added delay fails it while a healthy link
/// passes trivially — a heartbeat-sized probe would mask loss). A node
/// that cannot push to a quorum of peers cannot replicate, so it sheds
/// leadership and disables elections; it clears this reason once its egress is
/// clean and re-enables elections only if no other shed reason remains.
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
/// voter ([`cluster_egress_probe_plan`]) and, while it sheds, hands every
/// leadership it held when it shed to its peers at once,
/// and plans again on every tick for the ones still pending. The peers'
/// answers to `participation` travel the path the gate judges impaired, so
/// a leadership's first handoff may also go to a peer that did not answer,
/// while a leadership whose handoff did not move goes only to a peer that
/// answered that it may campaign
/// ([`crate::bootstrap::participation::CampaignStances::shed_targets_over_impaired_path`]).
pub fn spawn_egress_gate(
    registry: &RaftGroupHandleRegistry,
    participation: &PeerParticipation,
    cp_cfg: &ursula_config::ClusterProbeConfig,
) {
    let node_id = participation.node_id();
    let interval_ms = cp_cfg.interval.as_duration().as_millis() as usize;
    if interval_ms == 0 {
        return;
    }
    let probe_bytes = cp_cfg.probe_size.as_bytes() as usize;
    let probe_timeout_ms = cp_cfg.timeout.as_duration().as_millis() as usize;
    let unhealthy_ticks = cp_cfg.unhealthy_ticks;
    let heal_ticks = cp_cfg.heal_ticks;
    let registry = registry.clone();
    let participation = participation.clone();
    tokio::spawn(async move {
        let interval = Duration::from_millis(
            u64::try_from(interval_ms)
                .unwrap_or(u64::try_from(DEFAULT_CLUSTER_PROBE_INTERVAL_MS).unwrap_or(500)),
        );
        let client = match reqwest::Client::builder()
            .timeout(Duration::from_millis(
                u64::try_from(probe_timeout_ms)
                    .unwrap_or(u64::try_from(DEFAULT_CLUSTER_PROBE_TIMEOUT_MS).unwrap_or(200)),
            ))
            .build()
        {
            Ok(client) => client,
            Err(err) => {
                tracing::error!("cluster-egress: failed to build probe client: {err}");
                return;
            }
        };
        let payload = vec![0u8; probe_bytes];
        let mut hysteresis = HealthHysteresis::new(unhealthy_ticks, heal_ticks);
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
                continue;
            }
            // Each peer once per tick, whatever the group count.
            let mut answered = BTreeSet::new();
            for (peer_id, url) in &plan.peer_urls {
                let probe_url = format!("{url}{}", crate::CLUSTER_PROBE_PATH);
                if let Ok(resp) = client.post(&probe_url).body(payload.clone()).send().await
                    && resp.status().is_success()
                {
                    answered.insert(*peer_id);
                }
            }
            let degraded_probe = plan.degraded(&answered).map(|(group, healthy_peers)| {
                (
                    group.raft_group_id,
                    healthy_peers,
                    group.peers.len(),
                    group.needed_peers,
                )
            });
            let tick = if degraded_probe.is_none() {
                TickHealth::Good
            } else {
                TickHealth::Bad
            };
            match hysteresis.observe(tick) {
                HealthTransition::Shed => {
                    if let Some((group, healthy_peers, peer_count, needed_peers)) = &degraded_probe
                    {
                        tracing::warn!(
                            "cluster-egress: node {node_id} group {} egress degraded (reached {healthy_peers}/{peer_count} peers, need {needed_peers}); yielding leadership",
                            group.0,
                        );
                    }
                    shed.begin();
                }
                HealthTransition::Heal => shed.end(),
                HealthTransition::Hold => {}
            }
            if shed.is_draining() {
                let stances = participation.campaign_stances(&registry).await;
                shed.step(&stances.shed_targets_over_impaired_path(), &[])
                    .await;
            }
        }
    });
}

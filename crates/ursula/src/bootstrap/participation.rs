//! Which nodes may take over a leadership: the ones whose leadership-shed
//! policy lets them campaign. This node reads its own policy and asks each
//! peer for its state (`GET /__ursula/leadership-shed`). The leadership
//! balancer, the shutdown handoff and the snapshot-store and cold-health
//! gates hand leaderships only to the nodes known to be allowed to campaign.
//! The same answers tell the meta leader each peer's WAL-disk state
//! ([`ParticipationSheds`]).
//! The cluster-egress gate, whose questions travel the path it judges
//! impaired, may also try a peer that did not answer, once per leadership
//! ([`CampaignStances::shed_targets_over_impaired_path`]). An answer from a
//! node of another cluster (its `cluster_id` differs) is an unknown stance.
//! A node that may
//! not campaign (under drain, behind its recovery gate, or shedding for its
//! snapshot store, cluster egress or WAL disk) refuses a transfer without
//! telling the leader, which then forwards writes to it until Raft abandons
//! the transfer.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashSet;

use futures_util::future::BoxFuture;
use futures_util::future::join_all;
use ursula_control::PeerGate;
use ursula_proto::admin::LeadershipShedStatus;
use ursula_proto::admin::RaftTargetKey;
use ursula_proto::placement::NodeId;
use ursula_raft::LeadershipShedState;
use ursula_raft::PeerDirectory;
use ursula_raft::PeerGates;
use ursula_raft::RaftGroupHandleRegistry;
use ursula_shard::RaftTarget;

use crate::bootstrap::health_gate::ShedTargets;

/// What a node's leadership-shed policy lets it do, as far as this node
/// knows. Every reader picks handoff targets, so a node's stance is
/// whether it takes a leadership transfer (`should_accept_transfer`): it
/// may campaign and runs a meta replica.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CampaignStance {
    /// Its policy lets it campaign and take a transfer.
    MayCampaign,
    /// It answered that its policy does not let it take a transfer.
    MayNotCampaign,
    /// It did not answer in time, or its answer could not be read.
    Unknown,
}

/// The campaign stance of each node: this node's from its own policy, and
/// each peer's from its answer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CampaignStances {
    /// This node.
    node_id: u64,
    by_node: BTreeMap<u64, CampaignStance>,
    /// How many data groups each node leads: this node's from its own
    /// replicas, each peer's from its answer. A peer that did not answer
    /// has none.
    leader_counts: BTreeMap<u64, u32>,
}

impl CampaignStances {
    /// Each node's stance: this node's and every peer's it asked.
    pub(crate) fn by_node(&self) -> &BTreeMap<u64, CampaignStance> {
        &self.by_node
    }

    /// The nodes known to be allowed to campaign.
    pub(crate) fn may_campaign(&self) -> HashSet<u64> {
        self.nodes(|stance| stance == CampaignStance::MayCampaign)
    }

    /// Where a gate that asks its peers over a healthy path hands leaderships
    /// off: only to the nodes known to be allowed to campaign, on every
    /// handoff.
    pub(crate) fn shed_targets(&self) -> ShedTargets {
        ShedTargets::for_every_handoff(self.may_campaign())
    }

    /// Where a gate whose questions travel the path it judges impaired, the
    /// cluster-egress gate, hands leaderships off. A lost answer there says
    /// little about the peer, so a leadership's first handoff may also go to
    /// a peer that did not answer, and the registry's handoff check, whose
    /// Raft acks are fresher evidence than a lost request, decides. A peer
    /// that may not campaign still refuses that handoff without a word, so a
    /// leadership whose handoff did not move goes only to a peer that
    /// answered that it may campaign. A peer that never answers thus costs
    /// each leadership at most one stalled handoff per shed. And on a tick
    /// when a peer answered that it may not campaign, the shed may be
    /// cluster-wide, so first handoffs pass over the peers that did not
    /// answer too.
    pub(crate) fn shed_targets_over_impaired_path(&self) -> ShedTargets {
        let may_campaign = self.may_campaign();
        let a_peer_refuses = self.by_node.iter().any(|(node_id, stance)| {
            *node_id != self.node_id && *stance == CampaignStance::MayNotCampaign
        });
        let first = if a_peer_refuses {
            may_campaign.clone()
        } else {
            self.nodes(|stance| stance != CampaignStance::MayNotCampaign)
        };
        ShedTargets::new(first, may_campaign)
    }

    fn nodes(&self, keep: impl Fn(CampaignStance) -> bool) -> HashSet<u64> {
        self.by_node
            .iter()
            .filter(|(_, stance)| keep(**stance))
            .map(|(node_id, _)| *node_id)
            .collect()
    }

    /// How many data groups each node leads: this node's own count, and
    /// each peer's that answered.
    pub(crate) fn leader_counts(&self) -> BTreeMap<u64, usize> {
        self.leader_counts
            .iter()
            .map(|(node_id, count)| (*node_id, usize::try_from(*count).unwrap_or(usize::MAX)))
            .collect()
    }

    /// The stances of `node_id` and its peers as given, none of which
    /// answered a leader count.
    #[cfg(test)]
    pub(crate) fn of_nodes(
        node_id: u64,
        stances: impl IntoIterator<Item = (u64, CampaignStance)>,
    ) -> Self {
        Self {
            node_id,
            by_node: stances.into_iter().collect(),
            leader_counts: BTreeMap::new(),
        }
    }

    /// These stances with `counts` as the nodes' leader counts.
    #[cfg(test)]
    pub(crate) fn with_leader_counts(
        mut self,
        counts: impl IntoIterator<Item = (u64, u32)>,
    ) -> Self {
        self.leader_counts = counts.into_iter().collect();
        self
    }

    #[cfg(test)]
    pub(crate) fn of(&self, node_id: u64) -> Option<CampaignStance> {
        self.by_node.get(&node_id).copied()
    }
}

/// The peers a node polls and probes ([`PeerParticipation::peers`]), with
/// the identity of the same bind.
#[derive(Debug)]
pub(crate) enum PolledPeers {
    /// The registry's bound `raft.peers`.
    Bound(ursula_raft::BoundPeers),
    /// A test's own addresses, with the identity the registry had bound
    /// when they were read.
    #[cfg(test)]
    Fixed(
        std::sync::Arc<PeerDirectory>,
        Option<ursula_raft::PeerIdentity>,
    ),
}

impl PolledPeers {
    /// This node's cluster id, from the bind the peers come from: an
    /// answer of another cluster says nothing about this one. `None`
    /// before any bind, when there are no peers either.
    fn cluster_id(&self) -> Option<&ursula_headers::ClusterId> {
        let identity = match self {
            Self::Bound(peers) => peers.identity(),
            #[cfg(test)]
            Self::Fixed(_, identity) => identity.as_ref(),
        };
        identity.map(ursula_raft::PeerIdentity::cluster_id)
    }
}

impl std::ops::Deref for PolledPeers {
    type Target = PeerDirectory;

    fn deref(&self) -> &PeerDirectory {
        match self {
            Self::Bound(peers) => peers,
            #[cfg(test)]
            Self::Fixed(peers, _) => peers,
        }
    }
}

/// The client that asks a node's cluster peers, the ones its registry's
/// peer cluster binds (`raft.peers`, read at each poll), for their
/// participation answers (`GET /__ursula/leadership-shed`).
#[derive(Debug, Clone)]
pub(crate) struct PeerParticipation {
    node_id: u64,
    client: reqwest::Client,
    /// A test's own peer addresses for the polls and probes, apart from
    /// the ones its Raft dials.
    #[cfg(test)]
    addresses: Option<std::sync::Arc<PeerDirectory>>,
}

impl PeerParticipation {
    /// `client` bounds each peer's answer.
    pub(crate) fn new(node_id: u64, client: reqwest::Client) -> Self {
        Self {
            node_id,
            client,
            #[cfg(test)]
            addresses: None,
        }
    }

    /// Polls and probes `addresses` instead of the bound peers, for a test
    /// whose polls or probes must miss a peer its Raft reaches.
    #[cfg(test)]
    pub(crate) fn with_addresses(
        node_id: u64,
        addresses: PeerDirectory,
        client: reqwest::Client,
    ) -> Self {
        Self {
            addresses: Some(std::sync::Arc::new(addresses)),
            ..Self::new(node_id, client)
        }
    }

    pub(crate) fn node_id(&self) -> u64 {
        self.node_id
    }

    /// The peers this node polls for their answers and probes for its
    /// egress: the bound `raft.peers`, read at each use.
    pub(crate) fn peers(&self, registry: &RaftGroupHandleRegistry) -> PolledPeers {
        #[cfg(test)]
        if let Some(addresses) = &self.addresses {
            return PolledPeers::Fixed(addresses.clone(), registry.peer_cluster().identity());
        }
        PolledPeers::Bound(registry.peer_cluster().directory())
    }

    /// The answer of every peer `ask` names, `None` for a peer that did not
    /// answer in time, answered unreadably or answered for another cluster.
    async fn answers(
        &self,
        registry: &RaftGroupHandleRegistry,
        ask: impl Fn(u64) -> bool,
    ) -> Vec<(u64, Option<LeadershipShedStatus>)> {
        let peers = self.peers(registry);
        let Some(local) = peers.cluster_id() else {
            return Vec::new();
        };
        join_all(
            peers
                .iter()
                .filter(|(peer_id, _)| *peer_id != self.node_id && ask(*peer_id))
                .map(|(peer_id, endpoints)| async move {
                    (
                        peer_id,
                        self.answer(peer_id, endpoints.cluster_url(), local).await,
                    )
                }),
        )
        .await
    }

    /// The campaign stance and leader count of this node and of each peer.
    /// A node shedding for cold health may campaign, because that reason
    /// does not stop it from campaigning: cold-health backlogs are often
    /// cluster-wide, and leaving every hot node out could leave no node to
    /// lead.
    pub(crate) async fn campaign_stances(
        &self,
        registry: &RaftGroupHandleRegistry,
    ) -> CampaignStances {
        let answers = self.answers(registry, |_peer| true).await;
        let mut by_node = BTreeMap::new();
        let mut leader_counts = BTreeMap::new();
        for (peer_id, answer) in answers {
            let stance = match &answer {
                Some(status) if status.should_accept_transfer => CampaignStance::MayCampaign,
                Some(_) => CampaignStance::MayNotCampaign,
                None => CampaignStance::Unknown,
            };
            by_node.insert(peer_id, stance);
            if let Some(status) = answer {
                leader_counts.insert(peer_id, status.leader_count);
            }
        }
        let own = if registry.should_accept_transfer() {
            CampaignStance::MayCampaign
        } else {
            CampaignStance::MayNotCampaign
        };
        by_node.insert(self.node_id, own);
        leader_counts.insert(self.node_id, registry.leader_count());
        CampaignStances {
            node_id: self.node_id,
            by_node,
            leader_counts,
        }
    }

    /// Each answering peer's WAL-disk state, `true` when its answer says it
    /// sheds leadership for WAL disk pressure. A peer that does not answer
    /// in time, or answers for another cluster, is left out: its state is
    /// unknown.
    pub(crate) async fn wal_disk_shedding(
        &self,
        registry: &RaftGroupHandleRegistry,
    ) -> BTreeMap<NodeId, bool> {
        self.answers(registry, |_peer| true)
            .await
            .into_iter()
            .filter_map(|(peer_id, answer)| {
                let sheds = LeadershipShedState::from_bits_truncate(answer?.bits)
                    .contains(LeadershipShedState::WAL_DISK_PRESSURE);
                Some((NodeId::new(peer_id), sheds))
            })
            .collect()
    }

    /// The recovery gate each of `learners` answers for `replica`'s group,
    /// asked of the learners only. A learner without an answer, or whose
    /// answer lists no gate of the group, is left out, so it counts as
    /// `Unknown`.
    pub(crate) async fn recovery_gates(
        &self,
        registry: &RaftGroupHandleRegistry,
        replica: RaftTarget,
        learners: &BTreeSet<NodeId>,
    ) -> BTreeMap<NodeId, PeerGate> {
        let key = match replica {
            RaftTarget::Data(group) => RaftTargetKey::Data(group.0),
            RaftTarget::Meta => RaftTargetKey::Meta,
        };
        self.answers(registry, |peer| learners.contains(&NodeId::new(peer)))
            .await
            .into_iter()
            .filter_map(|(peer_id, answer)| {
                let status = answer?.recovery_gates.get(&key).copied()?;
                Some((NodeId::new(peer_id), PeerGate::of_status(status)))
            })
            .collect()
    }

    /// The answer of `peer_id` at `peer_url`, when it is one of this node's
    /// cluster, `local`.
    async fn answer(
        &self,
        peer_id: u64,
        peer_url: &str,
        local: &ursula_headers::ClusterId,
    ) -> Option<LeadershipShedStatus> {
        let url = format!("{peer_url}{}", crate::LEADERSHIP_SHED_PATH);
        let response = match self.client.get(url).send().await {
            Ok(response) if response.status().is_success() => response,
            Ok(response) => {
                tracing::debug!(
                    peer_id,
                    status = %response.status(),
                    "leadership-shed state request refused"
                );
                return None;
            }
            Err(err) => {
                tracing::debug!(peer_id, %err, "leadership-shed state request failed");
                return None;
            }
        };
        let body = match response.text().await {
            Ok(body) => body,
            Err(err) => {
                tracing::debug!(peer_id, %err, "leadership-shed state body failed");
                return None;
            }
        };
        match serde_json::from_str::<LeadershipShedStatus>(&body) {
            Ok(status) if *local != status.cluster_id => {
                tracing::debug!(
                    peer_id,
                    peer_cluster_id = %status.cluster_id,
                    "ignoring the leadership-shed state of a node of another cluster"
                );
                None
            }
            Ok(status) => Some(status),
            Err(err) => {
                tracing::debug!(peer_id, %err, "unreadable leadership-shed state");
                None
            }
        }
    }
}

/// The meta leader's source of its peers' WAL-disk states
/// ([`ursula_raft::meta::liveness::WalDiskSheds`]): their participation
/// answers.
#[derive(Debug)]
pub(crate) struct ParticipationSheds {
    pub(crate) participation: PeerParticipation,
    pub(crate) registry: RaftGroupHandleRegistry,
}

impl ursula_raft::meta::liveness::WalDiskSheds for ParticipationSheds {
    fn shedding(&self) -> ursula_raft::meta::liveness::Shedding<'_> {
        Box::pin(self.participation.wal_disk_shedding(&self.registry))
    }
}

/// A node's peer gates (design 5.8): each learner's recovery gate as its
/// participation answer shows it, asked right before a promotion.
#[derive(Debug, Clone)]
pub(crate) struct ParticipationGates {
    pub(crate) participation: PeerParticipation,
    pub(crate) registry: RaftGroupHandleRegistry,
}

impl PeerGates for ParticipationGates {
    fn gates(
        &self,
        replica: RaftTarget,
        learners: &BTreeSet<NodeId>,
    ) -> BoxFuture<'_, BTreeMap<NodeId, PeerGate>> {
        let learners = learners.clone();
        Box::pin(async move {
            self.participation
                .recovery_gates(&self.registry, replica, &learners)
                .await
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::collections::BTreeSet;
    use std::collections::HashSet;
    use std::time::Duration;

    use axum::Json;
    use axum::Router;
    use axum::routing::get;
    use ursula_control::PeerGate;
    use ursula_proto::admin::LeadershipShedStatus;
    use ursula_proto::admin::RaftTargetKey;
    use ursula_proto::admin::RecoveryGateStatus;
    use ursula_proto::placement::NodeId;
    use ursula_raft::LeadershipShedReason;
    use ursula_raft::RaftGroupHandleRegistry;
    use ursula_shard::RaftGroupId;
    use ursula_shard::RaftTarget;

    use super::CampaignStance;
    use super::CampaignStances;
    use super::PeerParticipation;
    use crate::bootstrap::health_gate::ShedTargets;
    use crate::shared_peers;

    const THIS_CLUSTER: &str = "this-cluster";

    fn cluster(id: &str) -> ursula_headers::ClusterId {
        ursula_headers::ClusterId::new(id).unwrap()
    }

    /// Node 1's registry, of [`THIS_CLUSTER`].
    /// Node 1 of [`THIS_CLUSTER`], with `peers` bound as its `raft.peers`.
    fn this_registry(peers: ursula_raft::PeerDirectory) -> RaftGroupHandleRegistry {
        let registry = RaftGroupHandleRegistry::default();
        registry.peer_cluster().bind(
            ursula_raft::PeerIdentity::new(1, cluster(THIS_CLUSTER)),
            peers,
        );
        registry
    }

    /// A peer of [`THIS_CLUSTER`] that answers its leadership-shed state
    /// with `should_campaign`.
    async fn answering_peer(should_campaign: bool) -> (String, tokio::task::JoinHandle<()>) {
        answering_peer_in(THIS_CLUSTER, should_campaign).await
    }

    /// A node of `cluster_id` that answers its leadership-shed state with
    /// `should_campaign`.
    async fn answering_peer_in(
        cluster_id: &str,
        should_campaign: bool,
    ) -> (String, tokio::task::JoinHandle<()>) {
        serving(status(cluster_id, should_campaign)).await
    }

    /// The answer of a node of `cluster_id` that leads nothing and runs no
    /// gated replica.
    fn status(cluster_id: &str, should_campaign: bool) -> LeadershipShedStatus {
        LeadershipShedStatus {
            cluster_id: cluster(cluster_id),
            bits: 0,
            state: String::new(),
            should_accept_transfer: should_campaign,
            should_campaign,
            recovery_barriers_ready: true,
            should_shed_current_leaders: false,
            recovery_gates: BTreeMap::new(),
            leader_count: 0,
        }
    }

    /// A peer that answers `status`.
    async fn serving(status: LeadershipShedStatus) -> (String, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind peer");
        let address = listener.local_addr().expect("peer address");
        let app = Router::new().route(
            crate::LEADERSHIP_SHED_PATH,
            get(move || {
                let status = status.clone();
                async move { Json(status) }
            }),
        );
        let task = tokio::spawn(async move {
            axum::serve(listener, app).await.expect("serve peer");
        });
        (format!("http://{address}"), task)
    }

    /// A peer that accepts connections and never answers. The listener must
    /// outlive the request.
    async fn silent_peer() -> (String, tokio::net::TcpListener) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind peer");
        let address = listener.local_addr().expect("peer address");
        (format!("http://{address}"), listener)
    }

    /// A node of another cluster at a peer's address answers the poll with
    /// its own cluster id, so its answer does not count, even before this
    /// node ever contacted it over Raft. A node with nothing bound has no
    /// peers to ask.
    #[tokio::test]
    async fn a_shed_answer_of_another_cluster_is_an_unknown_stance() {
        let (willing, willing_task) = answering_peer(true).await;
        let (foreign, foreign_task) = answering_peer_in("other-cluster", true).await;
        let participation = PeerParticipation::new(
            1,
            reqwest::Client::builder()
                .timeout(Duration::from_millis(300))
                .build()
                .expect("client"),
        );
        let stances = participation
            .campaign_stances(&this_registry(shared_peers([(2, willing), (3, foreign)])))
            .await;
        assert_eq!(stances.of(2), Some(CampaignStance::MayCampaign));
        assert_eq!(stances.of(3), Some(CampaignStance::Unknown));
        assert_eq!(stances.may_campaign(), HashSet::from([1, 2]));
        let unbound = participation
            .campaign_stances(&RaftGroupHandleRegistry::default())
            .await;
        assert_eq!(unbound.of(2), None);
        willing_task.abort();
        foreign_task.abort();
    }

    #[tokio::test]
    async fn each_peer_has_the_stance_it_answered() {
        let (willing, willing_task) = answering_peer(true).await;
        let (refusing, refusing_task) = answering_peer(false).await;
        let (silent, _listener) = silent_peer().await;
        let participation = PeerParticipation::new(
            1,
            reqwest::Client::builder()
                .timeout(Duration::from_millis(300))
                .build()
                .expect("client"),
        );
        let registry = this_registry(shared_peers([
            (1, "http://unused".to_owned()),
            (2, willing),
            (3, refusing),
            (4, silent),
        ]));

        let stances = participation.campaign_stances(&registry).await;
        assert_eq!(stances.of(1), Some(CampaignStance::MayCampaign));
        assert_eq!(stances.of(2), Some(CampaignStance::MayCampaign));
        assert_eq!(stances.of(3), Some(CampaignStance::MayNotCampaign));
        assert_eq!(stances.of(4), Some(CampaignStance::Unknown));
        assert_eq!(stances.may_campaign(), HashSet::from([1, 2]));

        // This node reads its own policy.
        registry.mark_leadership_shed(LeadershipShedReason::MaintenanceDrain);
        let stances = participation.campaign_stances(&registry).await;
        assert_eq!(stances.of(1), Some(CampaignStance::MayNotCampaign));
        assert_eq!(stances.may_campaign(), HashSet::from([2]));

        willing_task.abort();
        refusing_task.abort();
    }

    /// A `[[raft.peers]]` url that ends in `/` still reaches the peer's
    /// leadership-shed route.
    #[tokio::test]
    async fn a_peer_url_with_a_trailing_slash_is_polled_at_its_route() {
        let (willing, willing_task) = answering_peer(true).await;
        let participation = PeerParticipation::new(
            1,
            reqwest::Client::builder()
                .timeout(Duration::from_millis(300))
                .build()
                .expect("client"),
        );
        let stances = participation
            .campaign_stances(&this_registry(shared_peers([(2, format!("{willing}/"))])))
            .await;
        assert_eq!(stances.of(2), Some(CampaignStance::MayCampaign));
        willing_task.abort();
    }

    /// A learner's gate comes from its own answer for the group: open only
    /// when it answers `open`, closed in every gated state, and unknown
    /// (left out) when it does not answer, answers for another cluster or
    /// lists no gate of the group. The bugs it catches: a missing gate read
    /// as open, which promotes a learner that refuses votes, and an answer
    /// of another group or cluster trusted. Each peer's leader count comes
    /// from its answer.
    #[tokio::test]
    async fn a_learner_gate_and_a_leader_count_come_from_the_peer_answer() {
        let group = RaftGroupId(7);
        let answer = |gates: &[(RaftTargetKey, RecoveryGateStatus)], leads| LeadershipShedStatus {
            recovery_gates: gates.iter().copied().collect(),
            leader_count: leads,
            ..status(THIS_CLUSTER, true)
        };
        let (open, open_task) = serving(answer(
            &[
                (RaftTargetKey::Data(7), RecoveryGateStatus::Open),
                (RaftTargetKey::Data(8), RecoveryGateStatus::CatchingUp),
            ],
            5,
        ))
        .await;
        let (closed, closed_task) = serving(answer(
            &[
                (RaftTargetKey::Data(7), RecoveryGateStatus::CatchingUp),
                (RaftTargetKey::Meta, RecoveryGateStatus::Open),
            ],
            2,
        ))
        .await;
        let (other_group, other_task) = serving(answer(
            &[(RaftTargetKey::Data(8), RecoveryGateStatus::Open)],
            3,
        ))
        .await;
        let (foreign, foreign_task) = serving(LeadershipShedStatus {
            recovery_gates: BTreeMap::from([(RaftTargetKey::Data(7), RecoveryGateStatus::Open)]),
            ..status("other-cluster", true)
        })
        .await;
        let (silent, _listener) = silent_peer().await;
        let participation = PeerParticipation::new(
            1,
            reqwest::Client::builder()
                .timeout(Duration::from_millis(300))
                .build()
                .expect("client"),
        );
        let registry = this_registry(shared_peers([
            (2, open),
            (3, closed),
            (4, other_group),
            (5, foreign),
            (6, silent),
        ]));
        let learners: BTreeSet<NodeId> = (2..=6).map(NodeId::new).collect();
        let gates = participation
            .recovery_gates(&registry, RaftTarget::Data(group), &learners)
            .await;
        assert_eq!(
            gates,
            BTreeMap::from([
                (NodeId::new(2), PeerGate::Open),
                (NodeId::new(3), PeerGate::Closed),
            ])
        );
        // Only learners are asked, and the meta group by its own key.
        let only_three = participation
            .recovery_gates(
                &registry,
                RaftTarget::Meta,
                &BTreeSet::from([NodeId::new(3)]),
            )
            .await;
        assert_eq!(
            only_three,
            BTreeMap::from([(NodeId::new(3), PeerGate::Open)])
        );

        let counts = participation
            .campaign_stances(&registry)
            .await
            .leader_counts();
        assert_eq!(counts, BTreeMap::from([(1, 0), (2, 5), (3, 2), (4, 3)]));
        for task in [open_task, closed_task, other_task, foreign_task] {
            task.abort();
        }
    }

    #[test]
    fn a_gate_that_asks_over_a_healthy_path_hands_off_only_to_nodes_that_may_campaign() {
        let stances = CampaignStances::of_nodes(1, [
            (1, CampaignStance::MayNotCampaign),
            (2, CampaignStance::MayCampaign),
            (3, CampaignStance::Unknown),
            (4, CampaignStance::MayNotCampaign),
        ]);
        assert_eq!(
            stances.shed_targets(),
            ShedTargets::for_every_handoff(HashSet::from([2]))
        );
    }

    #[test]
    fn over_an_impaired_path_a_peer_that_did_not_answer_takes_only_first_handoffs() {
        // This node sheds, so it may not campaign. That is no sign that the
        // shed is cluster-wide.
        let stances = CampaignStances::of_nodes(1, [
            (1, CampaignStance::MayNotCampaign),
            (2, CampaignStance::MayCampaign),
            (3, CampaignStance::Unknown),
        ]);
        assert_eq!(
            stances.shed_targets_over_impaired_path(),
            ShedTargets::new(HashSet::from([2, 3]), HashSet::from([2]))
        );

        let silent = CampaignStances::of_nodes(1, [
            (1, CampaignStance::MayNotCampaign),
            (2, CampaignStance::Unknown),
            (3, CampaignStance::Unknown),
        ]);
        assert_eq!(
            silent.shed_targets_over_impaired_path(),
            ShedTargets::new(HashSet::from([2, 3]), HashSet::new())
        );
    }

    #[test]
    fn over_an_impaired_path_a_peer_that_refuses_rules_out_the_ones_that_did_not_answer() {
        let stances = CampaignStances::of_nodes(1, [
            (1, CampaignStance::MayNotCampaign),
            (2, CampaignStance::MayNotCampaign),
            (3, CampaignStance::Unknown),
            (4, CampaignStance::MayCampaign),
        ]);
        assert_eq!(
            stances.shed_targets_over_impaired_path(),
            ShedTargets::for_every_handoff(HashSet::from([4]))
        );

        let refused = CampaignStances::of_nodes(1, [
            (1, CampaignStance::MayNotCampaign),
            (2, CampaignStance::MayNotCampaign),
            (3, CampaignStance::Unknown),
        ]);
        assert_eq!(
            refused.shed_targets_over_impaired_path(),
            ShedTargets::default()
        );
    }
}

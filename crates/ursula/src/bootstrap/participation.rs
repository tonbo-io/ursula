//! Which nodes may take over a leadership: the ones whose leadership-shed
//! policy lets them campaign. This node reads its own policy and asks each
//! peer for its state (`GET /__ursula/leadership-shed`). The leadership
//! balancer, the shutdown handoff and the snapshot-store and cold-health
//! gates hand leaderships only to the nodes known to be allowed to campaign.
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
use std::collections::HashSet;

use futures_util::future::join_all;
use ursula_proto::admin::LeadershipShedStatus;
use ursula_raft::PeerDirectory;
use ursula_raft::RaftGroupHandleRegistry;

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
}

impl CampaignStances {
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

    /// The stances of `node_id` and its peers as given.
    #[cfg(test)]
    pub(crate) fn of_nodes(
        node_id: u64,
        stances: impl IntoIterator<Item = (u64, CampaignStance)>,
    ) -> Self {
        Self {
            node_id,
            by_node: stances.into_iter().collect(),
        }
    }

    #[cfg(test)]
    pub(crate) fn of(&self, node_id: u64) -> Option<CampaignStance> {
        self.by_node.get(&node_id).copied()
    }
}

/// A node's cluster peers, and the client that asks them for their
/// leadership-shed state.
#[derive(Debug, Clone)]
pub(crate) struct PeerParticipation {
    node_id: u64,
    /// `raft.peers`, this node among them.
    peers: PeerDirectory,
    client: reqwest::Client,
}

impl PeerParticipation {
    /// `peers` may list this node too. `client` bounds each peer's answer.
    pub(crate) fn new(node_id: u64, peers: &PeerDirectory, client: reqwest::Client) -> Self {
        Self {
            node_id,
            peers: peers.clone(),
            client,
        }
    }

    pub(crate) fn node_id(&self) -> u64 {
        self.node_id
    }

    /// The campaign stance of this node and of each peer. A node shedding
    /// for cold health may campaign, because that reason does not stop it
    /// from campaigning: cold-health backlogs are often cluster-wide, and
    /// leaving every hot node out could leave no node to lead.
    pub(crate) async fn campaign_stances(
        &self,
        registry: &RaftGroupHandleRegistry,
    ) -> CampaignStances {
        // Only an answer of this node's cluster counts: a node of another
        // cluster at a peer's address says nothing about this one.
        let identity = registry.peer_cluster().identity();
        let local = identity.as_ref().map(ursula_raft::PeerIdentity::cluster_id);
        let answers = join_all(
            self.peers
                .iter()
                .filter(|(peer_id, _)| *peer_id != self.node_id)
                .map(|(peer_id, endpoints)| async move {
                    (
                        peer_id,
                        self.peer_stance(peer_id, endpoints.cluster_url(), local)
                            .await,
                    )
                }),
        )
        .await;
        let mut by_node: BTreeMap<u64, CampaignStance> = answers.into_iter().collect();
        let own = if registry.should_accept_transfer() {
            CampaignStance::MayCampaign
        } else {
            CampaignStance::MayNotCampaign
        };
        by_node.insert(self.node_id, own);
        CampaignStances {
            node_id: self.node_id,
            by_node,
        }
    }

    /// The nodes known to be allowed to campaign
    /// ([`CampaignStances::may_campaign`]). A peer that does not answer in
    /// time is left out.
    pub(crate) async fn campaign_eligible_nodes(
        &self,
        registry: &RaftGroupHandleRegistry,
    ) -> HashSet<u64> {
        self.campaign_stances(registry).await.may_campaign()
    }

    async fn peer_stance(
        &self,
        peer_id: u64,
        peer_url: &str,
        local: Option<&ursula_headers::ClusterId>,
    ) -> CampaignStance {
        let url = format!("{peer_url}{}", crate::LEADERSHIP_SHED_PATH);
        let response = match self.client.get(url).send().await {
            Ok(response) if response.status().is_success() => response,
            Ok(response) => {
                tracing::debug!(
                    peer_id,
                    status = %response.status(),
                    "leadership-shed state request refused"
                );
                return CampaignStance::Unknown;
            }
            Err(err) => {
                tracing::debug!(peer_id, %err, "leadership-shed state request failed");
                return CampaignStance::Unknown;
            }
        };
        let body = match response.text().await {
            Ok(body) => body,
            Err(err) => {
                tracing::debug!(peer_id, %err, "leadership-shed state body failed");
                return CampaignStance::Unknown;
            }
        };
        match serde_json::from_str::<LeadershipShedStatus>(&body) {
            Ok(status) if local != Some(&status.cluster_id) => {
                tracing::debug!(
                    peer_id,
                    peer_cluster_id = %status.cluster_id,
                    "ignoring the leadership-shed state of a node of another cluster"
                );
                CampaignStance::Unknown
            }
            Ok(status) if status.should_accept_transfer => CampaignStance::MayCampaign,
            Ok(_) => CampaignStance::MayNotCampaign,
            Err(err) => {
                tracing::debug!(peer_id, %err, "unreadable leadership-shed state");
                CampaignStance::Unknown
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::time::Duration;

    use axum::Json;
    use axum::Router;
    use axum::routing::get;
    use ursula_proto::admin::LeadershipShedStatus;
    use ursula_raft::LeadershipShedReason;
    use ursula_raft::RaftGroupHandleRegistry;

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
    fn this_registry() -> RaftGroupHandleRegistry {
        let registry = RaftGroupHandleRegistry::default();
        registry.peer_cluster().bind(
            ursula_raft::PeerIdentity::new(1, cluster(THIS_CLUSTER)),
            ursula_raft::PeerDirectory::default(),
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
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind peer");
        let address = listener.local_addr().expect("peer address");
        let status = LeadershipShedStatus {
            cluster_id: cluster(cluster_id),
            bits: 0,
            state: String::new(),
            should_accept_transfer: should_campaign,
            should_campaign,
            recovery_barriers_ready: true,
            should_shed_current_leaders: false,
        };
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
    /// node ever contacted it over Raft. A node with no cluster id trusts no
    /// answer.
    #[tokio::test]
    async fn a_shed_answer_of_another_cluster_is_an_unknown_stance() {
        let (willing, willing_task) = answering_peer(true).await;
        let (foreign, foreign_task) = answering_peer_in("other-cluster", true).await;
        let participation = PeerParticipation::new(
            1,
            &shared_peers([(2, willing), (3, foreign)]),
            reqwest::Client::builder()
                .timeout(Duration::from_millis(300))
                .build()
                .expect("client"),
        );
        let stances = participation.campaign_stances(&this_registry()).await;
        assert_eq!(stances.of(2), Some(CampaignStance::MayCampaign));
        assert_eq!(stances.of(3), Some(CampaignStance::Unknown));
        assert_eq!(stances.may_campaign(), HashSet::from([1, 2]));
        let unbound = participation
            .campaign_stances(&RaftGroupHandleRegistry::default())
            .await;
        assert_eq!(unbound.of(2), Some(CampaignStance::Unknown));
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
            &shared_peers([
                (1, "http://unused".to_owned()),
                (2, willing),
                (3, refusing),
                (4, silent),
            ]),
            reqwest::Client::builder()
                .timeout(Duration::from_millis(300))
                .build()
                .expect("client"),
        );
        let registry = this_registry();

        let stances = participation.campaign_stances(&registry).await;
        assert_eq!(stances.of(1), Some(CampaignStance::MayCampaign));
        assert_eq!(stances.of(2), Some(CampaignStance::MayCampaign));
        assert_eq!(stances.of(3), Some(CampaignStance::MayNotCampaign));
        assert_eq!(stances.of(4), Some(CampaignStance::Unknown));
        assert_eq!(stances.may_campaign(), HashSet::from([1, 2]));
        assert_eq!(
            participation.campaign_eligible_nodes(&registry).await,
            HashSet::from([1, 2])
        );

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
            &shared_peers([(2, format!("{willing}/"))]),
            reqwest::Client::builder()
                .timeout(Duration::from_millis(300))
                .build()
                .expect("client"),
        );
        let stances = participation.campaign_stances(&this_registry()).await;
        assert_eq!(stances.of(2), Some(CampaignStance::MayCampaign));
        willing_task.abort();
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

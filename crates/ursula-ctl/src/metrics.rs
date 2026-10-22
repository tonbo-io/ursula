use std::collections::BTreeSet;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use anyhow::Context;
use anyhow::Result;
use anyhow::anyhow;
use anyhow::bail;
use reqwest::Client;
use reqwest::Method;
use reqwest::RequestBuilder;
use ursula_proto::admin::PROCESS_INCARNATION_HEADER;
pub use ursula_proto::admin::TransferLeaderResponse;
use ursula_proto::placement::ProcessIncarnation;

use crate::provider::NodeInfo;

#[derive(Debug, Clone)]
pub struct MetricsClient {
    client: Client,
    timeout: Duration,
    incarnations: Arc<Mutex<HashMap<u64, ProcessIncarnation>>>,
}

impl MetricsClient {
    /// A client that follows no redirect: a node's admin answer is its
    /// own, and a `307` names a client-plane URL, so a command that tries
    /// several nodes asks the next one instead.
    pub fn new(timeout: Duration) -> Result<Self> {
        let client = Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(timeout)
            .build()
            .context("build reqwest client")?;
        Ok(Self {
            client,
            timeout,
            incarnations: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    pub fn timeout(&self) -> Duration {
        self.timeout
    }

    pub(crate) fn http_client(&self) -> &Client {
        &self.client
    }

    /// Remembers the first incarnation this client observed for a node and
    /// refuses another one, so a command stops when a node restarts under it.
    fn pin_incarnation(&self, node: &NodeInfo, observed: &ProcessIncarnation) -> Result<()> {
        let mut pins = self
            .incarnations
            .lock()
            .map_err(|_poisoned| anyhow!("process identity cache lock poisoned"))?;
        if let Some(pinned) = pins.get(&node.id) {
            if pinned != observed {
                bail!(
                    "node {} process incarnation changed during this operation; refusing to refresh its plan",
                    node.id
                );
            }
        } else {
            pins.insert(node.id, *observed);
        }
        Ok(())
    }

    async fn observed_incarnation(&self, node: &NodeInfo) -> Result<ProcessIncarnation> {
        let existing = self
            .incarnations
            .lock()
            .map_err(|_poisoned| anyhow!("process identity cache lock poisoned"))?
            .get(&node.id)
            .cloned();
        if let Some(incarnation) = existing {
            self.pin_incarnation(node, &incarnation)?;
            return Ok(incarnation);
        }
        Ok(self.fetch_node(node).await?.process_incarnation)
    }

    pub(crate) async fn admin_request(
        &self,
        node: &NodeInfo,
        method: Method,
        url: url::Url,
    ) -> Result<RequestBuilder> {
        let incarnation = self.observed_incarnation(node).await?;
        Ok(self
            .client
            .request(method, url)
            .header(PROCESS_INCARNATION_HEADER, incarnation.to_string()))
    }

    pub async fn serving_readiness(
        &self,
        node: &NodeInfo,
    ) -> Result<(reqwest::StatusCode, ursula_proto::admin::ServingReadiness)> {
        let url = metrics_base_url(node)
            .join("/__ursula/ready")
            .with_context(|| format!("compose readiness url for node {}", node.id))?;
        let response = self.client.get(url).send().await?;
        if response.status() != reqwest::StatusCode::SERVICE_UNAVAILABLE {
            response.error_for_status_ref()?;
        }
        Ok((response.status(), response.json().await?))
    }

    pub async fn fetch_node(&self, node: &NodeInfo) -> Result<NodeMetricsView> {
        let url = metrics_base_url(node)
            .join("/__ursula/metrics")
            .with_context(|| format!("compose metrics url for node {}", node.id))?;
        let resp = self
            .client
            .get(url.clone())
            .send()
            .await
            .with_context(|| format!("GET {url}"))?;
        if !resp.status().is_success() {
            bail!("metrics from node {} returned {}", node.id, resp.status());
        }
        let body: RawMetrics = resp
            .json()
            .await
            .with_context(|| format!("decode metrics from node {}", node.id))?;
        if body.process_node_id != Some(node.id) {
            return Err(MetricsIdentityError::Node {
                expected: node.id,
                observed: body.process_node_id,
            }
            .into());
        }
        if let Some(group) = body.groups.iter().find(|group| group.node_id != node.id) {
            return Err(MetricsIdentityError::Group {
                group: group.raft_group_id,
                expected: node.id,
                observed: group.node_id,
            }
            .into());
        }
        if let Some(report) = &body.raft_maintenance
            && report.node_id != node.id
        {
            return Err(MetricsIdentityError::Report {
                expected: node.id,
                observed: report.node_id,
            }
            .into());
        }
        self.pin_incarnation(node, &body.process_incarnation)?;
        Ok(NodeMetricsView::new(node.clone(), body))
    }

    pub async fn fetch_cluster(&self, nodes: &[NodeInfo]) -> Result<ClusterSnapshot> {
        let mut per_node = Vec::with_capacity(nodes.len());
        for node in nodes {
            per_node.push(self.fetch_node(node).await?);
        }
        Ok(ClusterSnapshot { per_node })
    }

    pub async fn try_fetch_cluster(&self, nodes: &[NodeInfo]) -> ClusterSnapshot {
        let mut per_node = Vec::with_capacity(nodes.len());
        for node in nodes {
            match self.fetch_node(node).await {
                Ok(view) => per_node.push(view),
                Err(err) => {
                    tracing::debug!("metrics fetch failed: node_id={} error={err}", node.id);
                }
            }
        }
        ClusterSnapshot { per_node }
    }

    pub async fn transfer_leader(
        &self,
        leader: &NodeInfo,
        raft_group_id: u64,
        to: u64,
    ) -> Result<TransferLeaderResponse> {
        let path = format!("/__ursula/raft/{raft_group_id}/leader/transfer/{to}");
        let url = leader
            .admin_url
            .join(&path)
            .with_context(|| format!("compose transfer-leader url at node {}", leader.id))?;
        let resp = self
            .admin_request(leader, Method::POST, url.clone())
            .await?
            .send()
            .await
            .with_context(|| format!("POST {url}"))?;
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        if status == reqwest::StatusCode::CONFLICT {
            let rejection: TransferLeaderResponse =
                serde_json::from_str(&body).context("decode rejected leadership transfer")?;
            if !rejection.transferred
                && rejection
                    .rejection
                    .is_some_and(|reason| reason.should_replan())
            {
                return Ok(rejection);
            }
        }
        if status.is_success() {
            serde_json::from_str::<TransferLeaderResponse>(&body)
                .with_context(|| format!("decode transfer-leader response: {body}"))
        } else {
            Err(anyhow!(
                "transfer-leader at node {} (group {} → {}) returned {}: {}",
                leader.id,
                raft_group_id,
                to,
                status,
                body
            ))
        }
    }

    /// Request an election through the incarnation-bound admin protocol.
    pub async fn request_self_election(
        &self,
        voter: &NodeInfo,
        raft_group_id: u64,
        current_term: u64,
    ) -> Result<()> {
        self.observed_incarnation(voter).await?;
        let url = voter
            .admin_url
            .join(&format!("/__ursula/raft/{raft_group_id}/self-election"))?;
        let response = self
            .admin_request(voter, Method::POST, url)
            .await?
            .json(&ursula_proto::admin::SelfElectionRequest { current_term })
            .send()
            .await?;
        if !response.status().is_success() {
            bail!(
                "self-election at node {} returned {}: {}",
                voter.id,
                response.status(),
                response.text().await?
            );
        }
        Ok(())
    }

    pub async fn confirm_quorum(
        &self,
        leader: &NodeInfo,
        group: u32,
    ) -> Result<ursula_proto::admin::QuorumPrefix> {
        self.observed_incarnation(leader).await?;
        let url = leader
            .admin_url
            .join(&format!("/__ursula/raft/{group}/quorum"))?;
        let response = self
            .admin_request(leader, Method::GET, url)
            .await?
            .send()
            .await?;
        let status = response.status();
        if !status.is_success() {
            bail!(
                "quorum proof at node {} returned {}: {}",
                leader.id,
                status,
                response.text().await?
            );
        }
        let proof: ursula_proto::admin::QuorumPrefix = response.json().await?;
        if proof.raft_group_id != group || proof.leader_id != leader.id {
            bail!("quorum proof identity differs from the requested group and leader");
        }
        Ok(proof)
    }

    pub async fn set_maintenance_drain(&self, node: &NodeInfo, enabled: bool) -> Result<()> {
        let url = node
            .admin_url
            .join("/__ursula/leadership-shed/maintenance")
            .with_context(|| format!("compose maintenance-drain url for node {}", node.id))?;
        let request = self
            .admin_request(
                node,
                if enabled {
                    Method::POST
                } else {
                    Method::DELETE
                },
                url.clone(),
            )
            .await?;
        let resp = request
            .send()
            .await
            .with_context(|| format!("{} {url}", if enabled { "POST" } else { "DELETE" }))?;
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        if status.is_success() {
            Ok(())
        } else {
            Err(anyhow!(
                "maintenance-drain {} at node {} returned {}: {}",
                if enabled { "mark" } else { "clear" },
                node.id,
                status,
                body
            ))
        }
    }

    pub async fn change_membership(
        &self,
        leader: &NodeInfo,
        raft_group_id: u64,
        voters: &BTreeSet<u64>,
    ) -> Result<()> {
        let path = format!("/__ursula/raft/{raft_group_id}/membership");
        let url = leader.admin_url.join(&path).with_context(|| {
            format!(
                "compose membership url at leader node {} for group {}",
                leader.id, raft_group_id
            )
        })?;
        let voter_list = voters
            .iter()
            .map(u64::to_string)
            .collect::<Vec<_>>()
            .join(",");
        let query = ursula_proto::admin::MembershipQuery { voters: voter_list };
        let resp = self
            .admin_request(leader, Method::POST, url.clone())
            .await?
            .query(&query)
            .send()
            .await
            .with_context(|| format!("POST {url}"))?;
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        if status.is_success() {
            let _: ursula_proto::admin::MembershipResponse =
                serde_json::from_str(&body).context("decode admin response")?;
            Ok(())
        } else {
            Err(anyhow!(
                "change membership at leader node {} (group {} voters={:?}) returned {}: {}",
                leader.id,
                raft_group_id,
                voters,
                status,
                body
            ))
        }
    }
}

/// Base URL of the read-only observations, metrics and serving readiness:
/// `metrics_url`, else `client_url`, else `admin_url`. The client and admin
/// planes serve them, so the rollout's Pod-bound admin tunnel suffices. The
/// Raft `http_url` is never used: a node's cluster listener serves neither.
fn metrics_base_url(node: &NodeInfo) -> &url::Url {
    node.metrics_url
        .as_ref()
        .or(node.client_url.as_ref())
        .unwrap_or(&node.admin_url)
}

#[derive(Debug, thiserror::Error)]
enum MetricsIdentityError {
    #[error("metrics process node {observed:?} differs from configured node {expected}")]
    Node {
        expected: u64,
        observed: Option<u64>,
    },
    #[error("metrics group {group} belongs to node {observed}, expected {expected}")]
    Group {
        group: u64,
        expected: u64,
        observed: u64,
    },
    #[error("maintenance report belongs to node {observed}, expected {expected}")]
    Report { expected: u64, observed: u64 },
}

type RawMetrics = ursula_proto::admin::NodeMetrics;

/// Bind a decoded shared contract to the endpoint that supplied it.
#[derive(Debug, Clone)]
pub struct NodeMetricsView {
    pub node: NodeInfo,
    pub metrics: ursula_proto::admin::NodeMetrics,
}
impl std::ops::Deref for NodeMetricsView {
    type Target = ursula_proto::admin::NodeMetrics;
    fn deref(&self) -> &Self::Target {
        &self.metrics
    }
}
impl std::ops::DerefMut for NodeMetricsView {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.metrics
    }
}
impl NodeMetricsView {
    fn new(node: NodeInfo, metrics: ursula_proto::admin::NodeMetrics) -> Self {
        Self { node, metrics }
    }
    pub fn group(&self, raft_group_id: u64) -> Option<&RaftGroupView> {
        self.groups
            .iter()
            .find(|g| g.raft_group_id == raft_group_id)
    }

    pub fn led_groups(&self) -> impl Iterator<Item = &RaftGroupView> {
        let self_id = self.node.id;
        self.groups
            .iter()
            .filter(move |g| g.current_leader == Some(self_id))
    }

    /// Whether this node reports that it leads the meta group. Meta is
    /// never one of [`Self::led_groups`], where its group 0 would meet data
    /// group 0.
    pub fn leads_meta(&self) -> bool {
        self.meta
            .as_ref()
            .is_some_and(|meta| meta.current_leader == Some(self.node.id))
    }
}

pub use ursula_proto::admin::RaftGroupMetrics as RaftGroupView;

#[derive(Debug, Clone)]
pub struct ClusterSnapshot {
    pub per_node: Vec<NodeMetricsView>,
}

impl ClusterSnapshot {
    pub fn node(&self, node_id: u64) -> Option<&NodeMetricsView> {
        self.per_node.iter().find(|view| view.node.id == node_id)
    }

    /// Returns groups the target node currently leads, observed from the
    /// target's own metrics. Empty if metrics for the target node are missing
    /// — callers should treat that as "do not proceed".
    pub fn groups_led_by(&self, node_id: u64) -> Vec<RaftGroupView> {
        self.node(node_id)
            .map(|view| view.led_groups().cloned().collect())
            .unwrap_or_default()
    }

    pub fn groups_reported_led_by(&self, node_id: u64) -> Vec<RaftGroupView> {
        let mut groups = HashMap::new();
        for view in &self.per_node {
            for group in &view.groups {
                if group.current_leader == Some(node_id) {
                    groups
                        .entry(group.raft_group_id)
                        .or_insert_with(|| group.clone());
                }
            }
        }
        groups.into_values().collect()
    }

    /// Whether any node reports `node_id` as the meta leader.
    pub fn meta_led_by(&self, node_id: u64) -> bool {
        self.per_node.iter().any(|view| {
            view.meta
                .as_ref()
                .is_some_and(|meta| meta.current_leader == Some(node_id))
        })
    }

    /// Peers' view of a raft group, keyed by reporting node_id, excluding the target.
    pub fn peer_views(
        &self,
        raft_group_id: u64,
        exclude_node_id: u64,
    ) -> HashMap<u64, RaftGroupView> {
        let mut map = HashMap::new();
        for view in &self.per_node {
            if view.node.id == exclude_node_id {
                continue;
            }
            if let Some(group) = view.group(raft_group_id) {
                map.insert(view.node.id, group.clone());
            }
        }
        map
    }
}

/// The incarnation of run `run` of one sample root.
#[cfg(test)]
pub(crate) fn test_incarnation(run: u32) -> ProcessIncarnation {
    ProcessIncarnation {
        root: ursula_proto::placement::RootId::new(std::num::NonZeroU64::MIN),
        run,
        nonce: 0,
    }
}

#[cfg(test)]
pub(crate) fn test_metrics(
    id: u64,
    identity: Option<ProcessIncarnation>,
) -> ursula_proto::admin::NodeMetrics {
    ursula_proto::admin::NodeMetrics {
        cluster_id: ursula_headers::ClusterId::new("test").unwrap(),
        process_incarnation: identity.expect("complete fixture identity"),
        process_node_id: Some(id),
        groups: vec![],
        meta: None,
        raft_maintenance: None,
    }
}

/// The voters a fixture places a group on when its replica reports no
/// membership yet. A placement always names voters, even before the replica
/// initializes, so the expectation cannot come from the empty membership.
#[cfg(test)]
pub(crate) const TEST_SEED_VOTERS: [u64; 3] = [1, 2, 3];

/// A maintenance report that expects each reported group on the voters its
/// replica reports, or on [`TEST_SEED_VOTERS`] while it is uninitialized.
#[cfg(test)]
pub(crate) fn test_maintenance_report(
    node_id: u64,
    groups: &[RaftGroupView],
) -> ursula_proto::admin::RaftMaintenanceReport {
    ursula_proto::admin::RaftMaintenanceReport {
        version: ursula_proto::admin::SchemaVersion,
        node_id,
        lag_tolerance: 16,
        placement: None,
        expected_groups: groups
            .iter()
            .map(|group| {
                let placed = if group.voter_ids.is_empty() {
                    TEST_SEED_VOTERS.as_slice()
                } else {
                    group.voter_ids.as_slice()
                };
                (
                    u32::try_from(group.raft_group_id).expect("fixture group fits u32"),
                    test_expected(placed),
                )
            })
            .collect(),
        unplaced: Default::default(),
        node_issues: vec![],
        group_issues: Default::default(),
    }
}

/// A seeded voter's expected replica of a group placed on `voters`.
#[cfg(test)]
pub(crate) fn test_expected(voters: &[u64]) -> ursula_proto::placement::ExpectedReplica {
    use ursula_proto::placement::NodeId;
    ursula_proto::placement::ExpectedReplica {
        role: ursula_proto::placement::ReplicaRole::Voter,
        since: ursula_proto::placement::PlacementEpoch::new(
            ursula_proto::placement::MetaLogId::new(ursula_proto::placement::RaftLogId {
                term: 1,
                leader: NodeId::new(1),
                index: 1,
            }),
        ),
        target: ursula_proto::placement::VoterSet::new(
            voters.iter().copied().map(NodeId::new).collect(),
        )
        .expect("fixture voters"),
        counts_as_voter: true,
    }
}

/// A healthy group replica for unit fixtures. Terms follow the reported indexes.
#[cfg(test)]
pub(crate) fn test_group(
    raft_group_id: u64,
    node_id: u64,
    current_term: u64,
    current_leader: Option<u64>,
    committed_index: Option<u64>,
    last_applied_index: Option<u64>,
    voter_ids: Vec<u64>,
) -> RaftGroupView {
    RaftGroupView {
        raft_group_id,
        node_id,
        current_term,
        current_leader,
        committed_index,
        last_applied_index,
        voter_configs: vec![voter_ids.iter().copied().collect()],
        committed_voter_configs: vec![voter_ids.iter().copied().collect()],
        voter_ids,
        learner_ids: vec![],
        maintenance: ursula_proto::admin::RaftGroupMaintenanceState {
            running: true,
            recovery_ready: true,
            accepting_transfers: true,
            membership_joint: false,
            membership_log_index: Some(0),
            stopped_for_operator: false,
        },
        last_log_index: committed_index.max(last_applied_index),
        last_log_term: committed_index
            .max(last_applied_index)
            .map(|_| current_term),
        committed_term: committed_index.map(|_| current_term),
        committed_node: committed_index.map(|_| node_id),
        last_applied_term: last_applied_index.map(|_| current_term),
        snapshot_term: None,
        snapshot_index: None,
        purged_term: None,
        purged_index: None,
        log_bytes_since_snapshot: 0,
        log_entries_since_snapshot: 0,
        last_snapshot_bytes: 0,
        has_snapshot: false,
        apply_failure: None,
        history: None,
        recovery: None,
        recovery_gate: ursula_proto::admin::RecoveryGateStatus::Open,
        leads_live: false,
        reconcile: None,
    }
}

/// A node whose maintenance report expects exactly its groups.
#[cfg(test)]
pub(crate) fn test_view(node: NodeInfo, groups: Vec<RaftGroupView>) -> NodeMetricsView {
    let raft_maintenance = Some(test_maintenance_report(node.id, &groups));
    let process_node_id = Some(node.id);
    NodeMetricsView::new(node, ursula_proto::admin::NodeMetrics {
        cluster_id: ursula_headers::ClusterId::new("test").unwrap(),
        process_incarnation: crate::metrics::test_incarnation(1),
        process_node_id,
        groups,
        meta: None,
        raft_maintenance,
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::Mutex;

    use axum::Router;
    use axum::http::StatusCode;
    use axum::routing::post;
    use url::Url;

    use super::*;

    async fn incarnation_node(
        id: u64,
        identity: Option<ProcessIncarnation>,
    ) -> (
        NodeInfo,
        Arc<Mutex<Option<ProcessIncarnation>>>,
        Arc<std::sync::atomic::AtomicUsize>,
        tokio::task::JoinHandle<()>,
    ) {
        let current = Arc::new(Mutex::new(identity));
        let applied = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let metrics_identity = current.clone();
        let mutation_identity = current.clone();
        let mutation_count = applied.clone();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let app = Router::new()
            .route(
                "/__ursula/metrics",
                axum::routing::get(move || {
                    let identity = *metrics_identity.lock().unwrap();
                    async move {
                        let mut body = serde_json::to_value(test_metrics(
                            id,
                            Some(crate::metrics::test_incarnation(1)),
                        ))
                        .unwrap();
                        body["process_incarnation"] = serde_json::to_value(identity).unwrap();
                        axum::Json(body)
                    }
                }),
            )
            .route(
                "/__ursula/leadership-shed/maintenance",
                post(move |headers: axum::http::HeaderMap| {
                    let identity = *mutation_identity.lock().unwrap();
                    let count = mutation_count.clone();
                    async move {
                        if let Some(identity) = identity {
                            let Some(observed) = headers.get(PROCESS_INCARNATION_HEADER) else {
                                return StatusCode::PRECONDITION_REQUIRED;
                            };
                            if observed.to_str().ok() != Some(identity.to_string().as_str()) {
                                return StatusCode::PRECONDITION_FAILED;
                            }
                        }
                        count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        StatusCode::OK
                    }
                }),
            );
        let task = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        (
            NodeInfo {
                id,
                host: address.to_string(),
                admin_url: Url::parse(&format!("http://{address}")).unwrap(),
                http_url: None,
                metrics_url: None,
                client_url: None,
            },
            current,
            applied,
            task,
        )
    }

    #[tokio::test]
    async fn metrics_client_rejects_missing_safety_fields_and_unknown_versions() {
        for missing in [Some("raft_maintenance"), None] {
            let mut wire =
                serde_json::to_value(test_metrics(1, Some(crate::metrics::test_incarnation(1))))
                    .unwrap();
            if let Some(field) = missing {
                wire.as_object_mut().unwrap().remove(field);
            } else {
                wire["raft_maintenance"] = serde_json::json!({
                    "version": 3, "node_id": 1, "lag_tolerance": 0, "placement": null,
                    "expected_groups": {}, "unplaced": {}, "node_issues": [], "group_issues": {}
                });
            }
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let app = Router::new().route(
                "/__ursula/metrics",
                axum::routing::get(move || {
                    let wire = wire.clone();
                    async move { axum::Json(wire) }
                }),
            );
            let task = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
            let node = NodeInfo {
                id: 1,
                host: address.to_string(),
                admin_url: format!("http://{address}").parse().unwrap(),
                http_url: None,
                metrics_url: None,
                client_url: None,
            };
            let result = MetricsClient::new(Duration::from_secs(1))
                .unwrap()
                .fetch_node(&node)
                .await;
            let error = result.expect_err("unsafe or unsupported metrics must be rejected");
            assert!(
                error
                    .chain()
                    .filter_map(|source| source.downcast_ref::<reqwest::Error>())
                    .any(reqwest::Error::is_decode)
            );
            task.abort();
        }
    }

    #[tokio::test]
    async fn cached_incarnation_rejects_rebound_endpoint_and_cannot_be_refreshed() {
        let (node, identity, applied, task) =
            incarnation_node(1, Some(crate::metrics::test_incarnation(1))).await;
        let client = MetricsClient::new(Duration::from_secs(1)).unwrap();
        let pinned = client.fetch_node(&node).await.unwrap().process_incarnation;
        *identity.lock().unwrap() = Some(crate::metrics::test_incarnation(2));
        let error = client
            .clone()
            .set_maintenance_drain(&node, true)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("412"), "{error}");
        assert_eq!(applied.load(std::sync::atomic::Ordering::SeqCst), 0);
        assert!(
            client
                .fetch_node(&node)
                .await
                .unwrap_err()
                .to_string()
                .contains("changed during")
        );
        assert_eq!(pinned, crate::metrics::test_incarnation(1));
        task.abort();
    }

    #[tokio::test]
    async fn identity_checks_apply_even_before_raft_groups_are_registered() {
        let (mut node, _, applied, task) =
            incarnation_node(1, Some(crate::metrics::test_incarnation(1))).await;
        node.id = 2;
        let client = MetricsClient::new(Duration::from_secs(1)).unwrap();
        let error = client.set_maintenance_drain(&node, true).await.unwrap_err();
        assert!(matches!(
            error.downcast_ref::<MetricsIdentityError>(),
            Some(MetricsIdentityError::Node {
                expected: 2,
                observed: Some(1)
            })
        ));
        assert_eq!(applied.load(std::sync::atomic::Ordering::SeqCst), 0);
        task.abort();
    }

    #[tokio::test]
    async fn consensus_admin_operations_require_process_identity() {
        let (node, _, applied, task) = incarnation_node(1, None).await;
        let client = MetricsClient::new(Duration::from_secs(1)).unwrap();
        let election = client.request_self_election(&node, 0, 7).await.unwrap_err();
        assert!(
            election
                .downcast_ref::<reqwest::Error>()
                .unwrap()
                .is_decode()
        );
        let quorum = client.confirm_quorum(&node, 0).await.unwrap_err();
        assert!(quorum.downcast_ref::<reqwest::Error>().unwrap().is_decode());
        assert_eq!(applied.load(std::sync::atomic::Ordering::SeqCst), 0);
        task.abort();
    }

    #[test]
    fn metrics_require_non_null_process_identity() {
        for body in [
            r#"{"process_node_id":1}"#,
            r#"{"process_node_id":1,"process_incarnation":null}"#,
        ] {
            let error = serde_json::from_str::<RawMetrics>(body).unwrap_err();
            assert!(error.is_data());
        }
    }

    #[tokio::test]
    async fn current_self_election_is_guarded_and_never_falls_back_after_identity_failure() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let app = Router::new()
            .route(
                "/__ursula/metrics",
                axum::routing::get(|| async {
                    axum::Json(crate::metrics::test_metrics(
                        1,
                        Some(crate::metrics::test_incarnation(1)),
                    ))
                }),
            )
            .route(
                "/__ursula/raft/0/self-election",
                post(
                    |headers: axum::http::HeaderMap,
                     axum::Json(body): axum::Json<serde_json::Value>| async move {
                        assert_eq!(
                            headers[PROCESS_INCARNATION_HEADER],
                            "00000000000000010000000100000000"
                        );
                        assert_eq!(body["current_term"], 7);
                        StatusCode::PRECONDITION_FAILED
                    },
                ),
            );
        let task = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let node = NodeInfo {
            id: 1,
            host: address.to_string(),
            admin_url: format!("http://{address}").parse().unwrap(),
            http_url: None,
            metrics_url: None,
            client_url: None,
        };
        let error = MetricsClient::new(Duration::from_secs(1))
            .unwrap()
            .request_self_election(&node, 0, 7)
            .await
            .unwrap_err();
        assert!(
            error.to_string().contains("412"),
            "must report the guarded HTTP failure without using the absent legacy RPC endpoint: {error}"
        );
        task.abort();
    }

    #[tokio::test]
    async fn current_quorum_proof_uses_guarded_admin_without_a_raft_endpoint() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let scrapes = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let observed = scrapes.clone();
        let app = Router::new()
            .route(
                "/__ursula/metrics",
                axum::routing::get(move || {
                    let observed = observed.clone();
                    async move {
                        observed.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        axum::Json(crate::metrics::test_metrics(
                            1,
                            Some(crate::metrics::test_incarnation(1)),
                        ))
                    }
                }),
            )
            .route(
                "/__ursula/raft/0/quorum",
                axum::routing::get(|headers: axum::http::HeaderMap| async move {
                    assert_eq!(
                        headers[PROCESS_INCARNATION_HEADER],
                        "00000000000000010000000100000000"
                    );
                    axum::Json(ursula_proto::admin::QuorumPrefix {
                        raft_group_id: 0,
                        leader_id: 1,
                        leader_term: 3,
                        required_applied_index: 17,
                    })
                }),
            );
        let task = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let node = NodeInfo {
            id: 1,
            host: address.to_string(),
            admin_url: format!("http://{address}").parse().unwrap(),
            http_url: None,
            metrics_url: None,
            client_url: None,
        };
        let client = MetricsClient::new(Duration::from_secs(1)).unwrap();
        client.fetch_node(&node).await.unwrap();
        for _ in 0..3 {
            let proof = client.confirm_quorum(&node, 0).await.unwrap();
            assert_eq!(proof.required_applied_index, 17);
        }
        assert_eq!(scrapes.load(std::sync::atomic::Ordering::SeqCst), 1);
        task.abort();
    }

    #[tokio::test]
    async fn transfer_conflicts_replan_only_for_typed_retryable_rejections() {
        use ursula_proto::admin::TransferRejection;

        for (reason, retryable) in [
            (TransferRejection::NotLeader, true),
            (TransferRejection::UnreadyTarget, true),
            (TransferRejection::InvalidTarget, false),
        ] {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let app = Router::new()
                .route(
                    "/__ursula/metrics",
                    axum::routing::get(|| async {
                        axum::Json(crate::metrics::test_metrics(
                            1,
                            Some(crate::metrics::test_incarnation(1)),
                        ))
                    }),
                )
                .route(
                    "/__ursula/raft/0/leader/transfer/2",
                    post(move || async move {
                        (
                            axum::http::StatusCode::CONFLICT,
                            axum::Json(TransferLeaderResponse {
                                raft_group_id: 0,
                                from: Some(1),
                                to: Some(2),
                                current_leader: None,
                                transferred: false,
                                rejection: Some(reason),
                                reason: None,
                            }),
                        )
                    }),
                );
            let task = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
            let node = NodeInfo {
                id: 1,
                host: address.to_string(),
                admin_url: format!("http://{address}").parse().unwrap(),
                http_url: None,
                metrics_url: None,
                client_url: None,
            };
            let result = MetricsClient::new(Duration::from_secs(1))
                .unwrap()
                .transfer_leader(&node, 0, 2)
                .await;
            if retryable {
                assert_eq!(result.unwrap().rejection, Some(reason));
            } else {
                assert!(result.unwrap_err().to_string().contains("409"));
            }
            task.abort();
        }
    }

    #[tokio::test]
    async fn tunneled_metrics_do_not_replace_the_advertised_address() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let app = Router::new().route(
            "/__ursula/metrics",
            axum::routing::get(|| async {
                axum::Json(test_metrics(1, Some(crate::metrics::test_incarnation(1))))
            }),
        );
        let task = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let tunnel = Url::parse(&format!("http://{address}")).unwrap();
        let advertised = Url::parse("http://replacement.invalid:4437").unwrap();
        let node = NodeInfo {
            id: 1,
            admin_url: tunnel.clone(),
            host: "replacement".to_owned(),
            http_url: Some(advertised.clone()),
            metrics_url: Some(tunnel),
            client_url: None,
        };
        let client = MetricsClient::new(Duration::from_secs(1)).unwrap();
        let view = client.fetch_node(&node).await.unwrap();
        assert_eq!(view.node.http_url, Some(advertised));
        task.abort();
    }

    #[tokio::test]
    async fn metrics_refuse_each_misdirected_identity_before_pinning() {
        for identity in 0..3 {
            let mut wire =
                serde_json::to_value(test_metrics(1, Some(crate::metrics::test_incarnation(1))))
                    .unwrap();
            match identity {
                0 => wire["process_node_id"] = serde_json::json!(2),
                1 => {
                    wire["raft_groups"] = serde_json::json!([{
                        "raft_group_id":0,"node_id":2,"current_term":1,"current_leader":1,
                        "committed_index":1,"committed_node":1,"last_applied_index":1,"voter_ids":[1,2,3],"voter_configs":[[1,2,3]],"committed_voter_configs":[[1,2,3]],"learner_ids":[],
                        "maintenance":{"running":true,"recovery_ready":true,"accepting_transfers":true,"membership_joint":false,"membership_log_index":0,"stopped_for_operator":false},
                        "last_log_index":1,"last_log_term":1,"committed_term":1,"last_applied_term":1,
                        "snapshot_term":null,"snapshot_index":null,"purged_term":null,"purged_index":null,
                        "log_bytes_since_snapshot":0,"log_entries_since_snapshot":0,"last_snapshot_bytes":0,"has_snapshot":false,
                        "apply_failure":null,"history":null,"recovery":null,"recovery_gate":"open","leads_live":false,
                        "reconcile":null
                    }])
                }
                _ => {
                    wire["raft_maintenance"] =
                        serde_json::to_value(test_maintenance_report(2, &[])).unwrap()
                }
            }
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let task = tokio::spawn(async move {
                axum::serve(
                    listener,
                    Router::new().route(
                        "/__ursula/metrics",
                        axum::routing::get(move || {
                            let wire = wire.clone();
                            async move { axum::Json(wire) }
                        }),
                    ),
                )
                .await
                .unwrap();
            });
            let node = NodeInfo {
                id: 1,
                admin_url: Url::parse(&format!("http://{address}")).unwrap(),
                host: address.to_string(),
                http_url: None,
                metrics_url: None,
                client_url: None,
            };
            let client = MetricsClient::new(Duration::from_secs(1)).unwrap();
            let error = client.fetch_node(&node).await.unwrap_err();
            assert!(matches!(
                (identity, error.downcast_ref::<MetricsIdentityError>()),
                (
                    0,
                    Some(MetricsIdentityError::Node {
                        expected: 1,
                        observed: Some(2)
                    })
                ) | (
                    1,
                    Some(MetricsIdentityError::Group {
                        group: 0,
                        expected: 1,
                        observed: 2
                    })
                ) | (
                    2,
                    Some(MetricsIdentityError::Report {
                        expected: 1,
                        observed: 2
                    })
                )
            ));
            task.abort();
        }
    }

    #[test]
    fn metrics_use_client_url_when_available() {
        let node = NodeInfo {
            id: 1,
            admin_url: Url::parse("http://127.0.0.1:4438").expect("admin url"),
            host: "127.0.0.1".to_owned(),
            http_url: Some(Url::parse("http://127.0.0.1:4439").expect("raft url")),
            metrics_url: None,
            client_url: Some(Url::parse("http://127.0.0.1:4437").expect("client url")),
        };

        assert_eq!(metrics_base_url(&node).port(), Some(4437));
        // Without a client URL, metrics never go to the Raft URL.
        let raft_only = NodeInfo {
            client_url: None,
            ..node
        };
        assert_eq!(metrics_base_url(&raft_only).port(), Some(4438));
    }

    #[test]
    fn metrics_fall_back_to_admin_url_for_legacy_manifests() {
        let node = NodeInfo {
            id: 1,
            admin_url: Url::parse("http://127.0.0.1:4438").expect("admin url"),
            host: "127.0.0.1".to_owned(),
            http_url: None,
            metrics_url: None,
            client_url: None,
        };

        assert_eq!(metrics_base_url(&node).port(), Some(4438));
    }
}

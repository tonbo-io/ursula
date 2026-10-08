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
use ursula_proto::admin::MAINTENANCE_FENCE_HEADER;
use ursula_proto::admin::MaintenanceFenceState;
use ursula_proto::admin::PROCESS_INCARNATION_HEADER;
use ursula_proto::admin::ProcessIncarnation;
pub use ursula_proto::admin::TransferLeaderResponse;

use crate::provider::NodeInfo;

#[derive(Debug, Clone)]
pub struct MetricsClient {
    client: Client,
    timeout: Duration,
    incarnations: Arc<Mutex<HashMap<u64, ProcessIncarnation>>>,
}

impl MetricsClient {
    pub fn new(timeout: Duration) -> Result<Self> {
        let client = Client::builder()
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

    fn pin_incarnation(&self, node: &NodeInfo, observed: &ProcessIncarnation) -> Result<()> {
        if node
            .expected_process_incarnation
            .as_ref()
            .is_some_and(|expected| observed != expected)
        {
            bail!(
                "node {} process incarnation differs from its maintenance plan",
                node.id
            );
        }
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
            pins.insert(node.id, observed.clone());
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
        Ok(self.fetch_node(node).await?.process_incarnation.clone())
    }

    pub(crate) async fn admin_request(
        &self,
        node: &NodeInfo,
        method: Method,
        url: url::Url,
    ) -> Result<RequestBuilder> {
        let incarnation = self.observed_incarnation(node).await?;
        let mut request = self
            .client
            .request(method, url)
            .header(PROCESS_INCARNATION_HEADER, incarnation.as_str());
        if let Some(fence) = &node.expected_maintenance_fence {
            request = request.header(MAINTENANCE_FENCE_HEADER, fence.header_value());
        }
        Ok(request)
    }

    /// Install/retire the caller's already-admitted token on one fixed process.
    /// This does not acquire a cell reservation or refresh either identity.
    pub async fn set_maintenance_fence(
        &self,
        node: &NodeInfo,
        retire: bool,
    ) -> Result<MaintenanceFenceState> {
        let fence = node
            .expected_maintenance_fence
            .as_ref()
            .context("maintenance lifecycle requires a saved executor token")?;
        node.expected_process_incarnation
            .as_ref()
            .context("maintenance lifecycle requires a saved process identity")?;
        let mut observed = node.clone();
        // Lifecycle admission intentionally handles Unclaimed/pending/Retired
        // metrics, but must preserve the immutable process pin.
        observed.expected_maintenance_fence = None;
        self.fetch_node(&observed).await?;
        let operation = if retire { "retire" } else { "activate" };
        let url = node
            .admin_url
            .join(&format!("/__ursula/maintenance/fence/{operation}"))?;
        let response = self
            .admin_request(&observed, Method::POST, url)
            .await?
            .json(fence)
            .send()
            .await?;
        let status = response.status();
        let body = response.text().await?;
        if !status.is_success() {
            bail!(
                "maintenance {operation} at node {} returned {status}: {body}; stop without refreshing authority",
                node.id
            );
        }
        let reported: MaintenanceFenceState = serde_json::from_str(&body)?;
        let expected = if retire {
            MaintenanceFenceState::Retired {
                fence: fence.clone(),
            }
        } else {
            MaintenanceFenceState::Active {
                fence: fence.clone(),
            }
        };
        if reported != expected {
            bail!(
                "node {} acknowledged a different maintenance executor state",
                node.id
            );
        }
        Ok(reported)
    }

    /// Return a manifest pinned to each observed server instance. Refreshing
    /// one explicit replacement never refreshes another voter's authority.
    pub async fn pin_nodes(
        &self,
        nodes: &[NodeInfo],
        replacement: Option<u64>,
    ) -> Result<Vec<NodeInfo>> {
        crate::provider::validate_maintenance_fences(nodes)?;
        let ids = nodes.iter().map(|node| node.id).collect::<BTreeSet<_>>();
        if ids.len() != nodes.len()
            || nodes.is_empty()
            || replacement.is_some_and(|id| !ids.contains(&id))
        {
            bail!("incarnation binding requires unique configured voters and a known replacement");
        }
        let mut pinned = Vec::with_capacity(nodes.len());
        for configured in nodes {
            let mut node = configured.clone();
            if replacement == Some(node.id) {
                node.expected_process_incarnation = None;
            }
            let mut observed = node.clone();
            if replacement == Some(node.id) {
                observed.expected_maintenance_fence = None;
            }
            let view = self.fetch_node(&observed).await?;
            if replacement == Some(node.id)
                && let Some(expected) = &node.expected_maintenance_fence
            {
                let unclaimed = view.maintenance_fence == MaintenanceFenceState::Unclaimed;
                let same = view.maintenance_fence
                    == (MaintenanceFenceState::Active {
                        fence: expected.clone(),
                    });
                if (!unclaimed && !same) || view.maintenance_fence_uncertain {
                    bail!(
                        "replacement node {} has another or unresolved maintenance authority",
                        node.id
                    );
                }
            }
            if replacement.is_some()
                && replacement != Some(node.id)
                && let Some(expected) = &node.expected_maintenance_fence
                && view.maintenance_fence
                    != (MaintenanceFenceState::Active {
                        fence: expected.clone(),
                    })
            {
                bail!(
                    "surviving node {} lacks the saved active maintenance authority",
                    node.id
                );
            }
            node.expected_process_incarnation = Some(view.process_incarnation.clone());
            pinned.push(node);
        }
        Ok(pinned)
    }

    pub async fn serving_readiness(
        &self,
        node: &NodeInfo,
    ) -> Result<(reqwest::StatusCode, ursula_proto::admin::ServingReadiness)> {
        let url = metrics_base_url(node).join("/__ursula/ready")?;
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
        if let Some(expected) = &node.expected_maintenance_fence {
            let bound = matches!(&body.maintenance_fence,
                MaintenanceFenceState::Active { fence } | MaintenanceFenceState::Retired { fence } if fence == expected);
            if !bound || body.maintenance_fence_uncertain {
                bail!(
                    "node {} maintenance executor differs from the saved plan or has unresolved work",
                    node.id
                );
            }
        }
        Ok(NodeMetricsView::new(node.clone(), body))
    }

    pub async fn fetch_cluster(&self, nodes: &[NodeInfo]) -> Result<ClusterSnapshot> {
        crate::provider::validate_maintenance_fences(nodes)?;
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

    pub async fn add_learner(
        &self,
        leader: &NodeInfo,
        raft_group_id: u64,
        target: &NodeInfo,
    ) -> Result<()> {
        let address = target.http_url.as_ref().ok_or_else(|| {
            anyhow!(
                "node {} has no public Raft/client address for learner attachment",
                target.id
            )
        })?;
        let path = format!("/__ursula/raft/{raft_group_id}/learners/{}", target.id);
        let url = leader.admin_url.join(&path).with_context(|| {
            format!(
                "compose add-learner url at leader node {} for group {}",
                leader.id, raft_group_id
            )
        })?;
        if address.query().is_some() || address.fragment().is_some() {
            bail!(
                "node {} learner address must not contain a query or fragment",
                target.id
            );
        }
        let query = ursula_proto::admin::AddLearnerQuery {
            addr: address.to_string(),
            blocking: Some(false),
        };
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
            let _: ursula_proto::admin::AddLearnerResponse =
                serde_json::from_str(&body).context("decode admin response")?;
            Ok(())
        } else {
            Err(anyhow!(
                "add learner at leader node {} (group {} node {}) returned {}: {}",
                leader.id,
                raft_group_id,
                target.id,
                status,
                body
            ))
        }
    }
}

fn metrics_base_url(node: &NodeInfo) -> &url::Url {
    node.metrics_url
        .as_ref()
        .or(node.http_url.as_ref())
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

#[cfg(test)]
pub(crate) fn test_metrics(
    id: u64,
    identity: Option<ProcessIncarnation>,
) -> ursula_proto::admin::NodeMetrics {
    ursula_proto::admin::NodeMetrics {
        process_incarnation: identity.expect("complete fixture identity"),
        process_node_id: Some(id),
        maintenance_fence: MaintenanceFenceState::Unclaimed,
        maintenance_fence_uncertain: false,
        groups: vec![],
        raft_maintenance: None,
    }
}

#[cfg(test)]
pub(crate) fn test_maintenance_report(
    node_id: u64,
    groups: &[RaftGroupView],
) -> ursula_proto::admin::RaftMaintenanceReport {
    ursula_proto::admin::RaftMaintenanceReport {
        version: ursula_proto::admin::SchemaVersion,
        node_id,
        lag_tolerance: 16,
        expected_groups: groups
            .iter()
            .map(|group| {
                (
                    u32::try_from(group.raft_group_id).expect("fixture group fits u32"),
                    group.voter_ids.iter().copied().collect(),
                )
            })
            .collect(),
        node_issues: vec![],
        group_issues: Default::default(),
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
        committed_term: committed_index.map(|_| current_term),
        last_applied_term: last_applied_index.map(|_| current_term),
        snapshot_term: None,
        snapshot_index: None,
        purged_term: None,
        purged_index: None,
        log_bytes_since_snapshot: 0,
        log_entries_since_snapshot: 0,
        last_snapshot_bytes: 0,
        has_snapshot: false,
    }
}

/// An unclaimed node whose maintenance report expects exactly its groups.
#[cfg(test)]
pub(crate) fn test_view(node: NodeInfo, groups: Vec<RaftGroupView>) -> NodeMetricsView {
    let raft_maintenance = Some(test_maintenance_report(node.id, &groups));
    let process_node_id = Some(node.id);
    NodeMetricsView::new(node, ursula_proto::admin::NodeMetrics {
        process_incarnation: ProcessIncarnation::from_bits(1),
        process_node_id,
        maintenance_fence: MaintenanceFenceState::Unclaimed,
        maintenance_fence_uncertain: false,
        groups,
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
                    let identity = metrics_identity.lock().unwrap().clone();
                    async move {
                        let mut body = serde_json::to_value(test_metrics(
                            id,
                            Some(ProcessIncarnation::from_bits(1)),
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
                    let identity = mutation_identity.lock().unwrap().clone();
                    let count = mutation_count.clone();
                    async move {
                        if let Some(identity) = identity {
                            let Some(observed) = headers.get(PROCESS_INCARNATION_HEADER) else {
                                return StatusCode::PRECONDITION_REQUIRED;
                            };
                            if observed.to_str().ok() != Some(identity.as_str()) {
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
                expected_process_incarnation: None,
                expected_maintenance_fence: None,
            },
            current,
            applied,
            task,
        )
    }

    #[tokio::test]
    async fn metrics_client_rejects_missing_safety_fields_and_unknown_versions() {
        for missing in [
            Some("maintenance_fence_uncertain"),
            Some("maintenance_fence"),
            Some("raft_maintenance"),
            None,
        ] {
            let mut wire =
                serde_json::to_value(test_metrics(1, Some(ProcessIncarnation::from_bits(1))))
                    .unwrap();
            if let Some(field) = missing {
                wire.as_object_mut().unwrap().remove(field);
            } else {
                wire["raft_maintenance"] = serde_json::json!({
                    "version": 2, "node_id": 1, "lag_tolerance": 0,
                    "expected_groups": {"0": [1]}, "node_issues": [], "group_issues": {}
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
                expected_process_incarnation: None,
                expected_maintenance_fence: None,
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
            incarnation_node(1, Some(ProcessIncarnation::from_bits(1))).await;
        let client = MetricsClient::new(Duration::from_secs(1)).unwrap();
        let pinned = client
            .fetch_node(&node)
            .await
            .unwrap()
            .process_incarnation
            .clone();
        *identity.lock().unwrap() = Some(ProcessIncarnation::from_bits(2));
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
        let mut refresh = node.clone();
        refresh.expected_process_incarnation = Some(ProcessIncarnation::from_bits(2));
        assert!(
            client
                .set_maintenance_drain(&refresh, true)
                .await
                .unwrap_err()
                .to_string()
                .contains("maintenance plan")
        );
        assert_eq!(pinned, ProcessIncarnation::from_bits(1));
        task.abort();
    }

    #[tokio::test]
    async fn saved_manifest_binds_survivors_and_only_explicit_replacement_can_change() {
        let (a, a_identity, _, a_task) =
            incarnation_node(1, Some(ProcessIncarnation::from_bits(1))).await;
        let (b, b_identity, _, b_task) =
            incarnation_node(2, Some(ProcessIncarnation::from_bits(2))).await;
        let nodes = MetricsClient::new(Duration::from_secs(1))
            .unwrap()
            .pin_nodes(&[a, b], None)
            .await
            .unwrap();
        *a_identity.lock().unwrap() = Some(ProcessIncarnation::from_bits(3));
        let client = MetricsClient::new(Duration::from_secs(1)).unwrap();
        client
            .pin_nodes(&nodes, None)
            .await
            .expect_err("a changed process incarnation must not match the saved manifest");
        let replaced = MetricsClient::new(Duration::from_secs(1))
            .unwrap()
            .pin_nodes(&nodes, Some(1))
            .await
            .unwrap();
        assert_eq!(
            replaced[0].expected_process_incarnation,
            Some(ProcessIncarnation::from_bits(3))
        );
        assert_eq!(
            replaced[1].expected_process_incarnation,
            nodes[1].expected_process_incarnation
        );
        *b_identity.lock().unwrap() = Some(ProcessIncarnation::from_bits(4));
        MetricsClient::new(Duration::from_secs(1))
            .unwrap()
            .pin_nodes(&nodes, Some(1))
            .await
            .expect_err("a changed survivor incarnation must not be replaced silently");
        a_task.abort();
        b_task.abort();
    }

    #[tokio::test]
    async fn missing_identity_cannot_be_pinned_even_for_an_explicit_replacement() {
        let (mut node, _, _, task) = incarnation_node(1, None).await;
        let client = MetricsClient::new(Duration::from_secs(1)).unwrap();
        for replacement in [None, Some(1)] {
            client
                .pin_nodes(&[node.clone()], replacement)
                .await
                .expect_err("missing identity cannot authorize a maintenance participant");
        }
        node.expected_process_incarnation = Some(ProcessIncarnation::from_bits(1));
        MetricsClient::new(Duration::from_secs(1))
            .unwrap()
            .fetch_node(&node)
            .await
            .expect_err("a legacy identity must never match a saved pin");
        task.abort();
    }

    #[tokio::test]
    async fn identity_checks_apply_even_before_raft_groups_are_registered() {
        let (mut node, _, applied, task) =
            incarnation_node(1, Some(ProcessIncarnation::from_bits(1))).await;
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
                        Some(ursula_proto::admin::ProcessIncarnation::from_bits(1)),
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
                            "00000000000000000000000000000001"
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
            expected_process_incarnation: None,
            expected_maintenance_fence: None,
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
                            Some(ursula_proto::admin::ProcessIncarnation::from_bits(1)),
                        ))
                    }
                }),
            )
            .route(
                "/__ursula/raft/0/quorum",
                axum::routing::get(|headers: axum::http::HeaderMap| async move {
                    assert_eq!(
                        headers[PROCESS_INCARNATION_HEADER],
                        "00000000000000000000000000000001"
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
            expected_process_incarnation: None,
            expected_maintenance_fence: None,
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
            (TransferRejection::RecoveringTarget, true),
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
                            Some(ursula_proto::admin::ProcessIncarnation::from_bits(1)),
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
                expected_process_incarnation: None,
                expected_maintenance_fence: None,
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
    async fn tunneled_metrics_do_not_replace_the_advertised_learner_address() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let query = Arc::new(Mutex::new(None));
        let received = query.clone();
        let app = Router::new()
            .route(
                "/__ursula/metrics",
                axum::routing::get(|| async {
                    axum::Json(test_metrics(1, Some(ProcessIncarnation::from_bits(1))))
                }),
            )
            .route(
                "/__ursula/raft/0/learners/1",
                post(
                    move |axum::extract::Query(value): axum::extract::Query<
                        ursula_proto::admin::AddLearnerQuery,
                    >| async move {
                        *received.lock().unwrap() = Some(value);
                        axum::Json(ursula_proto::admin::AddLearnerResponse {
                            raft_group_id: 0,
                            node_id: 1,
                            log_index: 7,
                        })
                    },
                ),
            );
        let task = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let tunnel = Url::parse(&format!("http://{address}")).unwrap();
        let advertised = Url::parse("http://replacement.invalid:4437").unwrap();
        let node = NodeInfo {
            expected_process_incarnation: None,
            expected_maintenance_fence: None,
            id: 1,
            admin_url: tunnel.clone(),
            host: "replacement".to_owned(),
            http_url: Some(advertised.clone()),
            metrics_url: Some(tunnel),
        };
        let client = MetricsClient::new(Duration::from_secs(1)).unwrap();
        let view = client.fetch_node(&node).await.unwrap();
        assert_eq!(view.node.http_url, Some(advertised.clone()));
        client.add_learner(&node, 0, &node).await.unwrap();
        let received = query.lock().unwrap();
        let received = received.as_ref().unwrap();
        assert_eq!(received.addr, advertised.as_str());
        assert_eq!(received.blocking, Some(false));
        task.abort();
    }

    #[tokio::test]
    async fn metrics_refuse_each_misdirected_identity_before_pinning() {
        for identity in 0..3 {
            let mut wire =
                serde_json::to_value(test_metrics(1, Some(ProcessIncarnation::from_bits(1))))
                    .unwrap();
            match identity {
                0 => wire["process_node_id"] = serde_json::json!(2),
                1 => {
                    wire["raft_groups"] = serde_json::json!([{
                        "raft_group_id":0,"node_id":2,"current_term":1,"current_leader":1,
                        "committed_index":1,"last_applied_index":1,"voter_ids":[1,2,3],"learner_ids":[],
                        "maintenance":{"running":true,"recovery_ready":true,"accepting_transfers":true,"membership_joint":false,"membership_log_index":0,"stopped_for_operator":false},
                        "last_log_index":1,"committed_term":1,"last_applied_term":1,
                        "snapshot_term":null,"snapshot_index":null,"purged_term":null,"purged_index":null,
                        "log_bytes_since_snapshot":0,"log_entries_since_snapshot":0,"last_snapshot_bytes":0,"has_snapshot":false
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
                expected_process_incarnation: None,
                expected_maintenance_fence: None,
                id: 1,
                admin_url: Url::parse(&format!("http://{address}")).unwrap(),
                host: address.to_string(),
                http_url: None,
                metrics_url: None,
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
            expected_process_incarnation: None,
            expected_maintenance_fence: None,
            id: 1,
            admin_url: Url::parse("http://127.0.0.1:4438").expect("admin url"),
            host: "127.0.0.1".to_owned(),
            http_url: Some(Url::parse("http://127.0.0.1:4437").expect("client url")),
            metrics_url: None,
        };

        assert_eq!(metrics_base_url(&node).port(), Some(4437));
    }

    #[test]
    fn metrics_fall_back_to_admin_url_for_legacy_manifests() {
        let node = NodeInfo {
            expected_process_incarnation: None,
            expected_maintenance_fence: None,
            id: 1,
            admin_url: Url::parse("http://127.0.0.1:4438").expect("admin url"),
            host: "127.0.0.1".to_owned(),
            http_url: None,
            metrics_url: None,
        };

        assert_eq!(metrics_base_url(&node).port(), Some(4438));
    }
}

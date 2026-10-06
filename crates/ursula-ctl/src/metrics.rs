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
use reqwest::StatusCode;
use serde::Deserialize;
use ursula_proto::admin::MAINTENANCE_FENCE_HEADER;
use ursula_proto::admin::MaintenanceFenceState;
use ursula_proto::admin::PROCESS_INCARNATION_HEADER;
use ursula_proto::admin::ProcessIncarnation;

use crate::provider::NodeInfo;

#[derive(Debug, Clone)]
pub struct MetricsClient {
    client: Client,
    timeout: Duration,
    incarnations: Arc<Mutex<HashMap<u64, Option<ProcessIncarnation>>>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RestartQuiesceCapability {
    Supported,
    LegacyUnavailable,
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

    fn pin_incarnation(
        &self,
        node: &NodeInfo,
        observed: &Option<ProcessIncarnation>,
    ) -> Result<()> {
        if node
            .expected_process_incarnation
            .as_ref()
            .is_some_and(|expected| observed.as_ref() != Some(expected))
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

    async fn observed_incarnation(&self, node: &NodeInfo) -> Result<Option<ProcessIncarnation>> {
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
        let mut request = self.client.request(method, url);
        if let Some(identity) = &incarnation {
            request = request.header(PROCESS_INCARNATION_HEADER, identity.as_str());
        }
        if let Some(fence) = &node.expected_maintenance_fence {
            if incarnation.is_none() {
                bail!(
                    "executor fencing requires process identity at node {}",
                    node.id
                );
            }
            request = request.header(MAINTENANCE_FENCE_HEADER, fence.header_value());
        }
        // Concrete migration consumers: deployed servers through 0.6.2 lack
        // the identity field and cannot enforce this precondition. Preserve
        // their existing transport only until those sources are retired.
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
        allow_legacy: bool,
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
                let unclaimed =
                    view.maintenance_fence.as_ref() == Some(&MaintenanceFenceState::Unclaimed);
                let same = view.maintenance_fence.as_ref()
                    == Some(&MaintenanceFenceState::Active {
                        fence: expected.clone(),
                    });
                if (!unclaimed && !same)
                    || view.maintenance_fence_uncertain
                    || view.process_incarnation.is_none()
                {
                    bail!(
                        "replacement node {} has another or unresolved maintenance authority",
                        node.id
                    );
                }
            }
            if replacement.is_some()
                && replacement != Some(node.id)
                && let Some(expected) = &node.expected_maintenance_fence
                && view.maintenance_fence.as_ref()
                    != Some(&MaintenanceFenceState::Active {
                        fence: expected.clone(),
                    })
            {
                bail!(
                    "surviving node {} lacks the saved active maintenance authority",
                    node.id
                );
            }
            if view.process_incarnation.is_none() && !allow_legacy {
                bail!(
                    "node {} lacks process identity; only the deployed legacy migration may opt in",
                    node.id
                );
            }
            node.expected_process_incarnation = view.process_incarnation;
            pinned.push(node);
        }
        Ok(pinned)
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
        if (body.process_incarnation.is_some() && body.process_node_id != Some(node.id))
            || body
                .raft_groups
                .iter()
                .any(|group| group.node_id != node.id)
            || body
                .raft_maintenance
                .as_ref()
                .is_some_and(|report| report.node_id != node.id)
        {
            bail!(
                "metrics identity differs from configured node {}; refusing to operate through a misdirected endpoint",
                node.id
            );
        }
        self.pin_incarnation(node, &body.process_incarnation)?;
        if let Some(expected) = &node.expected_maintenance_fence {
            let bound = matches!(body.maintenance_fence.as_ref(),
                Some(MaintenanceFenceState::Active { fence } | MaintenanceFenceState::Retired { fence }) if fence == expected);
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

    /// Trigger a bound admin election on current instances. The deployed
    /// 0.6.2 migration source retains its shared `TransferLeader` RPC.
    pub async fn request_self_election(
        &self,
        voter: &NodeInfo,
        raft_group_id: u64,
        current_term: u64,
    ) -> Result<()> {
        // Re-observe before selecting the legacy RPC: a cached legacy absence
        // must never route an election to a newly guarded replacement.
        if self.fetch_node(voter).await?.process_incarnation.is_some() {
            let url = voter
                .admin_url
                .join(&format!("/__ursula/raft/{raft_group_id}/self-election"))?;
            let response = self
                .admin_request(voter, Method::POST, url.clone())
                .await?
                .json(&serde_json::json!({"current_term": current_term}))
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
            return Ok(());
        }
        // The deployed 0.6.2 migration source has only the shared Raft RPC;
        // current instances use the incarnation-bound admin route above.
        let endpoint = voter.http_url.as_ref().ok_or_else(|| {
            anyhow!(
                "node {} has no public Raft/client address for survivor term handoff",
                voter.id
            )
        })?;
        if endpoint.path() != "/" || endpoint.query().is_some() || endpoint.fragment().is_some() {
            bail!(
                "node {} Raft/client address must not contain a path, query, or fragment",
                voter.id
            );
        }
        let raft_group_id = u32::try_from(raft_group_id)
            .with_context(|| format!("raft group id exceeds u32: {raft_group_id}"))?;
        ursula_raft::request_self_election_via_transfer(
            endpoint.as_str(),
            raft_group_id,
            voter.id,
            current_term,
            self.timeout,
        )
        .await
        .map_err(anyhow::Error::msg)
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
        let mut url = leader.admin_url.join(&path).with_context(|| {
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
        // Compatibility consumer: 0.4.8 servers split query strings without
        // URL-decoding values. Keep the comma literal while 0.4.8 remains a
        // supported rolling-upgrade source; ordinary query-pair encoding turns
        // it into `%2C` and makes the old server reject every membership change.
        url.set_query(Some(&format!("voters={voter_list}")));
        let resp = self
            .admin_request(leader, Method::POST, url.clone())
            .await?
            .send()
            .await
            .with_context(|| format!("POST {url}"))?;
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        if status.is_success() {
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
        let mut url = leader.admin_url.join(&path).with_context(|| {
            format!(
                "compose add-learner url at leader node {} for group {}",
                leader.id, raft_group_id
            )
        })?;
        // The same 0.4.8 compatibility boundary applies to learner addresses:
        // its parser expects a literal URL. Remove this raw query construction
        // once 0.4.8 is no longer a supported rolling-upgrade source.
        if address.query().is_some() || address.fragment().is_some() {
            bail!(
                "node {} learner address must not contain a query or fragment",
                target.id
            );
        }
        url.set_query(Some(&format!("addr={address}&blocking=false")));
        let resp = self
            .admin_request(leader, Method::POST, url.clone())
            .await?
            .send()
            .await
            .with_context(|| format!("POST {url}"))?;
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        if status.is_success() {
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

    pub async fn quiesce_for_restart(&self, node: &NodeInfo) -> Result<()> {
        let url = node
            .admin_url
            .join("/__ursula/raft/quiesce-for-restart")
            .with_context(|| format!("compose restart-quiesce url for node {}", node.id))?;
        let resp = self
            .admin_request(node, Method::POST, url.clone())
            .await?
            .send()
            .await
            .with_context(|| format!("POST {url}"))?;
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        if status.is_success() {
            Ok(())
        } else {
            Err(anyhow!(
                "restart quiesce at node {} returned {}: {}",
                node.id,
                status,
                body
            ))
        }
    }

    /// Probe route presence without mutating restart state.
    ///
    /// Ursula 0.4.8 has no restart-quiesce route and returns 404. A current
    /// POST-only Axum route returns 405 to GET. This is the narrow rolling
    /// upgrade discriminator for that source release; every other HTTP or
    /// transport result fails closed.
    pub async fn restart_quiesce_capability(
        &self,
        node: &NodeInfo,
    ) -> Result<RestartQuiesceCapability> {
        let url = node
            .admin_url
            .join("/__ursula/raft/quiesce-for-restart")
            .with_context(|| format!("compose restart-quiesce url for node {}", node.id))?;
        let resp = self
            .client
            .get(url.clone())
            .send()
            .await
            .with_context(|| format!("GET {url}"))?;
        let status = resp.status();
        match status {
            StatusCode::NOT_FOUND => Ok(RestartQuiesceCapability::LegacyUnavailable),
            StatusCode::METHOD_NOT_ALLOWED if resp.headers().contains_key("allow") => {
                Ok(RestartQuiesceCapability::Supported)
            }
            status if status.is_success() => Ok(RestartQuiesceCapability::Supported),
            _ => {
                let body = resp.text().await.unwrap_or_default();
                bail!(
                    "restart-quiesce capability at node {} returned {}: {}",
                    node.id,
                    status,
                    body
                )
            }
        }
    }
}

fn metrics_base_url(node: &NodeInfo) -> &url::Url {
    node.metrics_url
        .as_ref()
        .or(node.http_url.as_ref())
        .unwrap_or(&node.admin_url)
}

#[derive(Debug, Clone, Deserialize)]
pub struct TransferLeaderResponse {
    pub raft_group_id: u64,
    #[serde(default)]
    pub from: Option<u64>,
    #[serde(default)]
    pub to: Option<u64>,
    #[serde(default)]
    pub current_leader: Option<u64>,
    pub transferred: bool,
    #[serde(default)]
    pub reason: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct RawMetrics {
    #[serde(default)]
    process_incarnation: Option<ProcessIncarnation>,
    #[serde(default)]
    process_node_id: Option<u64>,
    #[serde(default)]
    maintenance_fence: Option<MaintenanceFenceState>,
    #[serde(default)]
    maintenance_fence_uncertain: bool,
    #[serde(default)]
    raft_groups: Vec<RawRaftGroup>,
    /// Raft WAL backend (`"memory"`/`"disk"`); absent on older servers.
    #[serde(default)]
    wal_backend: Option<String>,
    #[serde(default)]
    raft_maintenance: Option<ursula_raft::RaftMaintenanceReport>,
}

#[derive(Debug, Clone, Deserialize)]
struct RawRaftGroup {
    raft_group_id: u64,
    node_id: u64,
    #[serde(default)]
    current_term: Option<u64>,
    #[serde(default)]
    current_leader: Option<u64>,
    #[serde(default)]
    committed_index: Option<u64>,
    #[serde(default)]
    last_applied_index: Option<u64>,
    #[serde(default)]
    voter_ids: Vec<u64>,
    #[serde(default)]
    learner_ids: Vec<u64>,
    #[serde(default)]
    maintenance: Option<ursula_raft::RaftGroupMaintenanceState>,
}

#[derive(Debug, Clone)]
pub struct NodeMetricsView {
    pub node: NodeInfo,
    pub groups: Vec<RaftGroupView>,
    /// Raft WAL backend this node reports (`"memory"`/`"disk"`); `None` on
    /// servers predating the field.
    pub wal_backend: Option<String>,
    pub raft_maintenance: Option<ursula_raft::RaftMaintenanceReport>,
    pub process_incarnation: Option<ProcessIncarnation>,
    pub maintenance_fence: Option<MaintenanceFenceState>,
    pub maintenance_fence_uncertain: bool,
}

impl NodeMetricsView {
    fn new(node: NodeInfo, raw: RawMetrics) -> Self {
        let groups = raw
            .raft_groups
            .into_iter()
            .map(|g| RaftGroupView {
                raft_group_id: g.raft_group_id,
                node_id: g.node_id,
                current_term: g.current_term,
                current_leader: g.current_leader,
                committed_index: g.committed_index,
                last_applied_index: g.last_applied_index,
                voter_ids: g.voter_ids,
                learner_ids: g.learner_ids,
                maintenance: g.maintenance,
            })
            .collect();
        Self {
            node,
            groups,
            wal_backend: raw.wal_backend,
            raft_maintenance: raw.raft_maintenance,
            process_incarnation: raw.process_incarnation,
            maintenance_fence: raw.maintenance_fence,
            maintenance_fence_uncertain: raw.maintenance_fence_uncertain,
        }
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

#[derive(Debug, Clone)]
pub struct RaftGroupView {
    pub raft_group_id: u64,
    pub node_id: u64,
    pub current_term: Option<u64>,
    pub current_leader: Option<u64>,
    pub committed_index: Option<u64>,
    pub last_applied_index: Option<u64>,
    pub voter_ids: Vec<u64>,
    pub learner_ids: Vec<u64>,
    /// Absent on the supported 0.6.2 upgrade source. Such sources retain
    /// their legacy checks until replaced; they cannot certify the repaired
    /// memory-WAL participation guarantee. Remove when no retained upgrade
    /// source predates this metrics contract.
    pub maintenance: Option<ursula_raft::RaftGroupMaintenanceState>,
}

impl RaftGroupView {
    pub fn participation_ready(&self) -> bool {
        self.maintenance.as_ref().is_none_or(|health| {
            health.running
                && health.recovery_ready
                && !health.membership_joint
                && !health.stopped_for_operator
                && health.membership_log_index.is_some()
                && self.last_applied_index >= health.membership_log_index
        })
    }
}

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
#[expect(
    clippy::assertions_on_result_states,
    reason = "pre-existing result-state assertion debt; see Known debt in AGENTS.md"
)]
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
            .route("/__ursula/metrics", axum::routing::get(move || {
                let identity = metrics_identity.lock().unwrap().clone();
                async move { axum::Json(serde_json::json!({"process_incarnation": identity, "process_node_id": id, "raft_groups": []})) }
            }))
            .route("/__ursula/leadership-shed/maintenance", post(move |headers: axum::http::HeaderMap| {
                let identity = mutation_identity.lock().unwrap().clone();
                let count = mutation_count.clone();
                async move {
                    if let Some(identity) = identity {
                        let Some(observed) = headers.get(PROCESS_INCARNATION_HEADER) else { return StatusCode::PRECONDITION_REQUIRED; };
                        if observed.to_str().ok() != Some(identity.as_str()) { return StatusCode::PRECONDITION_FAILED; }
                    }
                    count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    StatusCode::OK
                }
            }));
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
    async fn cached_incarnation_rejects_rebound_endpoint_and_cannot_be_refreshed() {
        let (node, identity, applied, task) =
            incarnation_node(1, Some(ProcessIncarnation::from_bits(1))).await;
        let client = MetricsClient::new(Duration::from_secs(1)).unwrap();
        let pinned = client.fetch_node(&node).await.unwrap().process_incarnation;
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
        assert_eq!(pinned, Some(ProcessIncarnation::from_bits(1)));
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
            .pin_nodes(&[a, b], None, false)
            .await
            .unwrap();
        *a_identity.lock().unwrap() = Some(ProcessIncarnation::from_bits(3));
        let client = MetricsClient::new(Duration::from_secs(1)).unwrap();
        assert!(client.pin_nodes(&nodes, None, false).await.is_err());
        let replaced = MetricsClient::new(Duration::from_secs(1))
            .unwrap()
            .pin_nodes(&nodes, Some(1), false)
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
        assert!(
            MetricsClient::new(Duration::from_secs(1))
                .unwrap()
                .pin_nodes(&nodes, Some(1), false)
                .await
                .is_err()
        );
        a_task.abort();
        b_task.abort();
    }

    #[tokio::test]
    async fn legacy_identity_requires_explicit_manifest_migration_and_never_matches_a_saved_pin() {
        let (mut node, _, _, task) = incarnation_node(1, None).await;
        assert!(
            MetricsClient::new(Duration::from_secs(1))
                .unwrap()
                .pin_nodes(&[node.clone()], None, false)
                .await
                .is_err()
        );
        let migrated = MetricsClient::new(Duration::from_secs(1))
            .unwrap()
            .pin_nodes(&[node.clone()], None, true)
            .await
            .unwrap();
        assert!(migrated[0].expected_process_incarnation.is_none());
        node.expected_process_incarnation = Some(ProcessIncarnation::from_bits(1));
        assert!(
            MetricsClient::new(Duration::from_secs(1))
                .unwrap()
                .fetch_node(&node)
                .await
                .is_err()
        );
        task.abort();
    }

    #[tokio::test]
    async fn identity_checks_apply_even_before_raft_groups_are_registered() {
        let (mut node, _, applied, task) =
            incarnation_node(1, Some(ProcessIncarnation::from_bits(1))).await;
        node.id = 2;
        let client = MetricsClient::new(Duration::from_secs(1)).unwrap();
        assert!(
            client
                .set_maintenance_drain(&node, true)
                .await
                .unwrap_err()
                .to_string()
                .contains("metrics identity differs")
        );
        assert_eq!(applied.load(std::sync::atomic::Ordering::SeqCst), 0);
        task.abort();
    }

    #[tokio::test]
    async fn legacy_to_current_replacement_cannot_reuse_the_unbound_election_rpc() {
        let (node, identity, _, task) = incarnation_node(1, None).await;
        let client = MetricsClient::new(Duration::from_secs(1)).unwrap();
        client.fetch_node(&node).await.unwrap();
        *identity.lock().unwrap() = Some(ProcessIncarnation::from_bits(1));
        let error = client.request_self_election(&node, 0, 7).await.unwrap_err();
        assert!(
            error.to_string().contains("changed during"),
            "cached legacy transport must stop before HTTP or consensus mutation: {error}"
        );
        task.abort();
    }

    #[tokio::test]
    async fn current_self_election_is_guarded_and_never_falls_back_after_identity_failure() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let app = Router::new()
            .route(
                "/__ursula/metrics",
                axum::routing::get(|| async {
                    axum::Json(serde_json::json!({
                "process_node_id":1,"process_incarnation":"00000000000000000000000000000001"}))
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
    async fn tunneled_metrics_do_not_replace_the_advertised_learner_address() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let query = Arc::new(Mutex::new(None));
        let received = query.clone();
        let app = Router::new()
            .route(
                "/__ursula/metrics",
                axum::routing::get(|| async {
                    axum::Json(serde_json::json!({
                        "raft_groups": [{"raft_group_id": 0, "node_id": 1}]
                    }))
                }),
            )
            .route(
                "/__ursula/raft/0/learners/1",
                post(move |axum::extract::RawQuery(value)| async move {
                    *received.lock().unwrap() = value;
                    StatusCode::OK
                }),
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
        assert_eq!(
            query.lock().unwrap().as_deref(),
            Some(format!("addr={advertised}&blocking=false").as_str())
        );
        task.abort();
    }

    #[tokio::test]
    async fn metrics_refuse_an_admin_tunnel_that_reaches_a_different_voter() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let task = tokio::spawn(async move {
            axum::serve(listener, Router::new().route("/__ursula/metrics", axum::routing::get(|| async {
                axum::Json(serde_json::json!({"raft_groups": [{"raft_group_id": 0, "node_id": 2}]}))
            }))).await.unwrap();
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
        let error = MetricsClient::new(Duration::from_secs(1))
            .unwrap()
            .fetch_node(&node)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("metrics identity differs"));
        task.abort();
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

    async fn quiesce_route() -> StatusCode {
        StatusCode::OK
    }

    async fn capability_node(app: Router) -> anyhow::Result<NodeInfo> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        Ok(NodeInfo {
            expected_process_incarnation: None,
            expected_maintenance_fence: None,
            id: 1,
            admin_url: Url::parse(&format!("http://{address}"))?,
            host: address.to_string(),
            http_url: None,
            metrics_url: None,
        })
    }

    #[tokio::test]
    async fn restart_quiesce_probe_distinguishes_legacy_route_absence() {
        let client = MetricsClient::new(Duration::from_secs(1)).expect("metrics client");
        let supported = capability_node(
            Router::new().route("/__ursula/raft/quiesce-for-restart", post(quiesce_route)),
        )
        .await
        .expect("supported node");
        let legacy = capability_node(Router::new()).await.expect("legacy node");

        assert_eq!(
            client
                .restart_quiesce_capability(&supported)
                .await
                .expect("probe supported node"),
            RestartQuiesceCapability::Supported
        );
        assert_eq!(
            client
                .restart_quiesce_capability(&legacy)
                .await
                .expect("probe legacy node"),
            RestartQuiesceCapability::LegacyUnavailable
        );
    }
}

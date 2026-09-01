use std::collections::BTreeSet;
use std::collections::HashMap;
use std::time::Duration;

use anyhow::Context;
use anyhow::Result;
use anyhow::anyhow;
use anyhow::bail;
use reqwest::Client;
use reqwest::StatusCode;
use serde::Deserialize;

use crate::provider::NodeInfo;

#[derive(Debug, Clone)]
pub struct MetricsClient {
    client: Client,
    timeout: Duration,
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
        Ok(Self { client, timeout })
    }

    pub fn timeout(&self) -> Duration {
        self.timeout
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
            .client
            .post(url.clone())
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

    pub async fn set_maintenance_drain(&self, node: &NodeInfo, enabled: bool) -> Result<()> {
        let url = node
            .admin_url
            .join("/__ursula/leadership-shed/maintenance")
            .with_context(|| format!("compose maintenance-drain url for node {}", node.id))?;
        let request = if enabled {
            self.client.post(url.clone())
        } else {
            self.client.delete(url.clone())
        };
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
            .client
            .post(url.clone())
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
            .client
            .post(url.clone())
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
            .client
            .post(url.clone())
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
    node.http_url.as_ref().unwrap_or(&node.admin_url)
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
    raft_groups: Vec<RawRaftGroup>,
    /// Raft WAL backend (`"memory"`/`"disk"`); absent on older servers.
    #[serde(default)]
    wal_backend: Option<String>,
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
}

#[derive(Debug, Clone)]
pub struct NodeMetricsView {
    pub node: NodeInfo,
    pub groups: Vec<RaftGroupView>,
    /// Raft WAL backend this node reports (`"memory"`/`"disk"`); `None` on
    /// servers predating the field.
    pub wal_backend: Option<String>,
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
            })
            .collect();
        Self {
            node,
            groups,
            wal_backend: raw.wal_backend,
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
mod tests {
    use axum::Router;
    use axum::http::StatusCode;
    use axum::routing::post;
    use url::Url;

    use super::*;

    #[test]
    fn metrics_use_client_url_when_available() -> anyhow::Result<()> {
        let node = NodeInfo {
            id: 1,
            admin_url: Url::parse("http://127.0.0.1:4438")?,
            host: "127.0.0.1".to_owned(),
            http_url: Some(Url::parse("http://127.0.0.1:4437")?),
        };

        assert_eq!(metrics_base_url(&node).port(), Some(4437));
        Ok(())
    }

    #[test]
    fn metrics_fall_back_to_admin_url_for_legacy_manifests() -> anyhow::Result<()> {
        let node = NodeInfo {
            id: 1,
            admin_url: Url::parse("http://127.0.0.1:4438")?,
            host: "127.0.0.1".to_owned(),
            http_url: None,
        };

        assert_eq!(metrics_base_url(&node).port(), Some(4438));
        Ok(())
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
            id: 1,
            admin_url: Url::parse(&format!("http://{address}"))?,
            host: address.to_string(),
            http_url: None,
        })
    }

    #[tokio::test]
    async fn restart_quiesce_probe_distinguishes_legacy_route_absence() -> anyhow::Result<()> {
        let client = MetricsClient::new(Duration::from_secs(1))?;
        let supported = capability_node(
            Router::new().route("/__ursula/raft/quiesce-for-restart", post(quiesce_route)),
        )
        .await?;
        let legacy = capability_node(Router::new()).await?;

        assert_eq!(
            client.restart_quiesce_capability(&supported).await?,
            RestartQuiesceCapability::Supported
        );
        assert_eq!(
            client.restart_quiesce_capability(&legacy).await?,
            RestartQuiesceCapability::LegacyUnavailable
        );
        Ok(())
    }
}

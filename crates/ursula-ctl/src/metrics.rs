use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::time::Duration;

use anyhow::Context;
use anyhow::Result;
use anyhow::anyhow;
use anyhow::bail;
use reqwest::Client;
use serde::Deserialize;
use serde::Serialize;
use url::Url;

use crate::provider::NodeInfo;

#[derive(Debug, Clone)]
pub struct MetricsClient {
    client: Client,
    timeout: Duration,
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
        let url = node
            .http_url
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
            .http_url
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
            .http_url
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

    pub async fn allow_next_revert(
        &self,
        leader: &NodeInfo,
        raft_group_id: u64,
        node_id: u64,
    ) -> Result<()> {
        let path = format!("/__ursula/raft/{raft_group_id}/nodes/{node_id}/allow-next-revert");
        let url = leader.http_url.join(&path).with_context(|| {
            format!(
                "compose allow-next-revert url at leader node {} for node {}",
                leader.id, node_id
            )
        })?;
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
                "allow-next-revert at leader node {} (group {} node {}) returned {}: {}",
                leader.id,
                raft_group_id,
                node_id,
                status,
                body
            ))
        }
    }

    pub async fn register_node(
        &self,
        admin_url: &Url,
        node_id: u64,
        client_url: &str,
        cluster_url: &str,
    ) -> Result<RegisterNodeResponse> {
        let path = register_node_path(node_id, client_url, cluster_url);
        let url = admin_url
            .join(&path)
            .with_context(|| format!("compose node-register url at {admin_url}"))?;
        let resp = self
            .client
            .post(url.clone())
            .send()
            .await
            .with_context(|| format!("POST {url}"))?;
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        if status.is_success() {
            serde_json::from_str::<RegisterNodeResponse>(&body)
                .with_context(|| format!("decode node-register response: {body}"))
        } else {
            Err(anyhow!(
                "node-register at {admin_url} for node {node_id} returned {status}: {body}"
            ))
        }
    }

    pub async fn group_placement(
        &self,
        admin_url: &Url,
        raft_group_id: u64,
    ) -> Result<GroupPlacementResponse> {
        let path = group_placement_path(raft_group_id);
        let url = admin_url
            .join(&path)
            .with_context(|| format!("compose group-placement url at {admin_url}"))?;
        let resp = self
            .client
            .get(url.clone())
            .send()
            .await
            .with_context(|| format!("GET {url}"))?;
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        if status.is_success() {
            serde_json::from_str::<GroupPlacementResponse>(&body)
                .with_context(|| format!("decode group-placement response: {body}"))
        } else {
            Err(anyhow!(
                "group-placement at {admin_url} for group {raft_group_id} returned {status}: {body}"
            ))
        }
    }

    pub async fn begin_migration(
        &self,
        admin_url: &Url,
        raft_group_id: u64,
        target_voters: &BTreeSet<u64>,
        retain_removed: bool,
    ) -> Result<BeginMigrationResponse> {
        let path = begin_migration_path(raft_group_id, target_voters, retain_removed);
        let url = admin_url
            .join(&path)
            .with_context(|| format!("compose begin-migration url at {admin_url}"))?;
        let resp = self
            .client
            .post(url.clone())
            .send()
            .await
            .with_context(|| format!("POST {url}"))?;
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        if status.is_success() {
            serde_json::from_str::<BeginMigrationResponse>(&body)
                .with_context(|| format!("decode begin-migration response: {body}"))
        } else {
            Err(anyhow!(
                "begin-migration at {admin_url} for group {raft_group_id} returned {status}: {body}"
            ))
        }
    }

    pub async fn prepare_local_engine(
        &self,
        admin_url: &Url,
        raft_group_id: u64,
    ) -> Result<PrepareLocalEngineResponse> {
        let path = prepare_local_engine_path(raft_group_id);
        let url = admin_url
            .join(&path)
            .with_context(|| format!("compose prepare-local-engine url at {admin_url}"))?;
        let resp = self
            .client
            .post(url.clone())
            .send()
            .await
            .with_context(|| format!("POST {url}"))?;
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        if status.is_success() {
            serde_json::from_str::<PrepareLocalEngineResponse>(&body)
                .with_context(|| format!("decode prepare-local-engine response: {body}"))
        } else {
            Err(anyhow!(
                "prepare-local-engine at {admin_url} for group {raft_group_id} returned {status}: {body}"
            ))
        }
    }
}

fn register_node_path(node_id: u64, client_url: &str, cluster_url: &str) -> String {
    format!(
        "/__ursula/admin/nodes/{node_id}/register?client_url={client_url}&cluster_url={cluster_url}"
    )
}

fn group_placement_path(raft_group_id: u64) -> String {
    format!("/__ursula/admin/groups/{raft_group_id}/placement")
}

fn begin_migration_path(
    raft_group_id: u64,
    target_voters: &BTreeSet<u64>,
    retain_removed: bool,
) -> String {
    let target_voters = target_voters
        .iter()
        .map(u64::to_string)
        .collect::<Vec<_>>()
        .join(",");
    format!(
        "/__ursula/admin/groups/{raft_group_id}/migrations?target_voters={target_voters}&retain_removed={retain_removed}"
    )
}

fn prepare_local_engine_path(raft_group_id: u64) -> String {
    format!("/__ursula/admin/groups/{raft_group_id}/local-engine")
}

#[derive(Debug, Clone, Deserialize)]
pub struct RegisterNodeResponse {
    pub node_id: u64,
    pub registered: bool,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct GroupPlacementResponse {
    pub raft_group_id: u64,
    pub voters: BTreeSet<u64>,
    pub learners: BTreeSet<u64>,
    pub draining: BTreeSet<u64>,
    pub epoch: u64,
    pub nodes: BTreeMap<u64, PlacementNodeResponse>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PlacementNodeResponse {
    pub node_id: u64,
    pub client_url: String,
    pub cluster_url: String,
    pub state: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct BeginMigrationResponse {
    pub raft_group_id: u64,
    pub migration_id: u64,
    pub started: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PrepareLocalEngineResponse {
    pub raft_group_id: u64,
    pub core_id: u64,
    pub prepared: bool,
    pub already_allowed: bool,
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
}

#[derive(Debug, Clone, Deserialize)]
struct RawRaftGroup {
    raft_group_id: u64,
    node_id: u64,
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
}

impl NodeMetricsView {
    fn new(node: NodeInfo, raw: RawMetrics) -> Self {
        let groups = raw
            .raft_groups
            .into_iter()
            .map(|g| RaftGroupView {
                raft_group_id: g.raft_group_id,
                node_id: g.node_id,
                current_leader: g.current_leader,
                committed_index: g.committed_index,
                last_applied_index: g.last_applied_index,
                voter_ids: g.voter_ids,
                learner_ids: g.learner_ids,
            })
            .collect();
        Self { node, groups }
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
    use super::*;

    #[test]
    fn node_register_path_preserves_raw_urls_for_server_query_parser() {
        assert_eq!(
            register_node_path(5, "http://node5:4491", "http://node5:4492"),
            "/__ursula/admin/nodes/5/register?client_url=http://node5:4491&cluster_url=http://node5:4492"
        );
    }

    #[test]
    fn group_placement_path_builds_admin_projection_path() {
        assert_eq!(
            group_placement_path(7),
            "/__ursula/admin/groups/7/placement"
        );
    }

    #[test]
    fn begin_migration_path_builds_admin_query() {
        assert_eq!(
            begin_migration_path(2, &BTreeSet::from([2, 3, 4]), true),
            "/__ursula/admin/groups/2/migrations?target_voters=2,3,4&retain_removed=true"
        );
    }

    #[test]
    fn prepare_local_engine_path_builds_admin_path() {
        assert_eq!(
            prepare_local_engine_path(2),
            "/__ursula/admin/groups/2/local-engine"
        );
    }
}

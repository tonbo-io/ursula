//! Placement verbs over the nodes' admin planes: confirmed placement views
//! and `node replace-disk`.
//!
//! `node replace-disk` releases the root (the WAL disk) a node id is bound
//! to in meta, so an empty disk under that id binds. It reads the node's
//! slot from a view another node confirmed by a meta read barrier, refuses
//! a node whose own process still answers on the root or serves (a typo
//! guard that `--force` passes), and sends the release, a compare-and-set
//! on the expected root, to another node's admin plane. A released root
//! never binds again, and the release is safe only once its disk is
//! revoked ([`RELEASE_RULE`]).

use anyhow::Context;
use anyhow::Result;
use reqwest::Method;
use ursula_control::ClusterView;
use ursula_control::PlacementApiError;
use ursula_control::PlacementStatus;
use ursula_control::ReplaceDiskRequest;
use ursula_proto::placement::NodeId;
use ursula_proto::placement::NodeRecord;
use ursula_proto::placement::RootId;
use ursula_proto::placement::RootSlot;

use crate::metrics::MetricsClient;
use crate::provider::NodeInfo;

/// What `node replace-disk` prints before and after the release.
pub const RELEASE_RULE: &str = "A release cannot be undone: the released root never binds this \
                                node id again. Release a root only once its disk is revoked: the \
                                old process ended, its volume was detached or destroyed, or its \
                                host is shut down. On Kubernetes that is a pod deleted normally, \
                                a force-deleted pod whose VolumeAttachment is gone, or a node \
                                tainted out of service. Release a disk, never a process.";

/// Why `node replace-disk` sent no release.
#[derive(Debug, thiserror::Error)]
pub enum ReplaceDiskRefusal {
    #[error("node {node} is not in the manifest")]
    NotInManifest { node: u64 },
    #[error(
        "no node other than {node} answered a confirmed placement view: the meta group must \
         answer the release"
    )]
    NoConfirmedView { node: u64 },
    #[error("the confirmed placement view holds no root slot for node {node}")]
    NoSlot { node: u64 },
    #[error(
        "node {node} still runs on root {root}: its disk is not revoked. Stop that process and \
         revoke its disk first, or pass --force"
    )]
    RunsOnRoot { node: u64, root: RootId },
    #[error(
        "node {node} reports ready: replace-disk is for a node whose disk is lost. Check the node \
         id, or pass --force"
    )]
    Serving { node: u64 },
    #[error("node {via} refused the release ({status}): {refusal:?}")]
    Refused {
        via: u64,
        status: reqwest::StatusCode,
        refusal: Box<PlacementApiError>,
    },
}

/// A node's slot, as a view that another node confirmed shows it.
#[derive(Debug, Clone)]
pub struct ConfirmedSlot {
    /// The node whose admin plane confirmed the view.
    pub via: NodeInfo,
    pub slot: RootSlot,
}

/// What `node replace-disk` did.
#[derive(Debug, Clone)]
pub struct ReplaceDiskOutcome {
    /// The slot before the release, and the node that confirmed it.
    pub before: ConfirmedSlot,
    /// The node's record with its released root.
    pub record: NodeRecord,
}

/// The confirmed placement status of the first node of `nodes` that
/// answers one (`GET /__ursula/placement?confirmed=true` on its admin
/// plane), with that node. `None` when none does.
pub async fn confirmed_status<'a>(
    client: &MetricsClient,
    nodes: impl IntoIterator<Item = &'a NodeInfo>,
) -> Option<(&'a NodeInfo, PlacementStatus)> {
    for node in nodes {
        let url = match node.admin_url.join("/__ursula/placement?confirmed=true") {
            Ok(url) => url,
            Err(error) => {
                tracing::warn!(node_id = node.id, %error, "placement: no placement url");
                continue;
            }
        };
        let response = match client.http_client().get(url).send().await {
            Ok(response) => response,
            Err(error) => {
                tracing::warn!(node_id = node.id, %error, "placement: node did not answer a confirmed view");
                continue;
            }
        };
        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            tracing::warn!(node_id = node.id, %status, body, "placement: node refused a confirmed view");
            continue;
        }
        match response.json::<PlacementStatus>().await {
            Ok(placement) => return Some((node, placement)),
            Err(error) => {
                tracing::warn!(node_id = node.id, %error, "placement: undecodable placement status");
            }
        }
    }
    None
}

/// Node `node`'s slot in `view`, `None` when the view holds none.
pub fn slot_in(view: &ClusterView, node: u64) -> Option<RootSlot> {
    let node = NodeId::new(node);
    match view {
        ClusterView::Registering { registered, .. } => registered.get(&node).copied(),
        ClusterView::Bootstrapped(cluster) => cluster.node(node).map(|record| record.root),
        ClusterView::MetaGenesis { .. }
        | ClusterView::MetaCatchingUp
        | ClusterView::FormationCut => None,
    }
}

/// Node `node`'s slot, from a view another node of `nodes` confirmed.
pub async fn confirmed_slot(
    client: &MetricsClient,
    nodes: &[NodeInfo],
    node: u64,
) -> Result<ConfirmedSlot> {
    if !nodes.iter().any(|candidate| candidate.id == node) {
        return Err(ReplaceDiskRefusal::NotInManifest { node }.into());
    }
    let (via, status) = confirmed_status(client, nodes.iter().filter(|other| other.id != node))
        .await
        .ok_or(ReplaceDiskRefusal::NoConfirmedView { node })?;
    let slot = slot_in(&status.view, node).ok_or(ReplaceDiskRefusal::NoSlot { node })?;
    Ok(ConfirmedSlot {
        via: via.clone(),
        slot,
    })
}

/// The typo guard: refuses when node `node`'s own process answers on
/// `root`, or reports ready. A node that does not answer passes: its old
/// process is gone, or its replacement has not started.
pub async fn check_replaceable(
    client: &MetricsClient,
    nodes: &[NodeInfo],
    node: u64,
    root: RootId,
) -> Result<()> {
    let target = nodes
        .iter()
        .find(|candidate| candidate.id == node)
        .ok_or(ReplaceDiskRefusal::NotInManifest { node })?;
    let metrics = target
        .admin_url
        .join("/__ursula/metrics")
        .context("compose the metrics url")?;
    if let Ok(response) = client.http_client().get(metrics).send().await
        && response.status().is_success()
        && let Ok(metrics) = response.json::<ursula_proto::admin::NodeMetrics>().await
        && metrics.process_incarnation.root == root
    {
        return Err(ReplaceDiskRefusal::RunsOnRoot { node, root }.into());
    }
    let ready = target
        .admin_url
        .join("/__ursula/ready")
        .context("compose the readiness url")?;
    if let Ok(response) = client.http_client().get(ready).send().await
        && response.status().is_success()
    {
        return Err(ReplaceDiskRefusal::Serving { node }.into());
    }
    Ok(())
}

/// Sends the release of `root`, node `node`'s root, to `via`'s admin
/// plane, and returns the node's record with its released root.
pub async fn release_root(
    client: &MetricsClient,
    via: &NodeInfo,
    node: u64,
    root: RootId,
) -> Result<NodeRecord> {
    let url = via
        .admin_url
        .join(&format!("/__ursula/placement/nodes/{node}/replace-disk"))
        .context("compose the replace-disk url")?;
    let response = client
        .admin_request(via, Method::POST, url.clone())
        .await?
        .json(&ReplaceDiskRequest { expect_root: root })
        .send()
        .await
        .with_context(|| format!("POST {url}"))?;
    let status = response.status();
    if status.is_success() {
        return response
            .json()
            .await
            .with_context(|| format!("decode the record of node {node}"));
    }
    let body = response.text().await.unwrap_or_default();
    match serde_json::from_str::<PlacementApiError>(&body) {
        Ok(refusal) => Err(ReplaceDiskRefusal::Refused {
            via: via.id,
            status,
            refusal: Box::new(refusal),
        }
        .into()),
        Err(_not_a_refusal) => anyhow::bail!("POST {url} answered {status}: {body}"),
    }
}

/// `node replace-disk`: reads node `node`'s slot from a confirmed view,
/// applies the typo guard unless `force`, and releases `root` through the
/// node that confirmed the view.
pub async fn replace_disk(
    client: &MetricsClient,
    nodes: &[NodeInfo],
    node: u64,
    root: RootId,
    force: bool,
) -> Result<ReplaceDiskOutcome> {
    let before = confirmed_slot(client, nodes, node).await?;
    if !force {
        check_replaceable(client, nodes, node, root).await?;
    }
    let record = release_root(client, &before.via, node, root).await?;
    Ok(ReplaceDiskOutcome { before, record })
}

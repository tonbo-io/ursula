//! Placement verbs over the nodes' admin planes: placement views, moves,
//! node lifecycles and `node replace-disk`.
//!
//! - [`status`]: `placement status`, the view and each moving group's
//!   leader.
//! - [`moves`]: `placement move`, `cancel` and `set-move-limit`, pre-checked
//!   with the kernel's own `check` on a confirmed view.
//! - [`lifecycle`]: `node cordon`, `uncordon` and `decommission`, pre-checked
//!   the same way, and `node verify-removed`, `verify-joinable` and
//!   `verify-vacant`.
//!
//! `node replace-disk` releases the root (the WAL disk) a node id is bound
//! to in meta, so an empty disk under that id binds. It reads the node's
//! slot from a view another node confirmed by a meta read barrier, refuses
//! a node whose own process still answers on the root or serves (a typo
//! guard that `--force` passes), and sends the release, a compare-and-set
//! on the expected root, to another node's admin plane. A released root
//! never binds again, and the release is safe only once its disk is
//! revoked ([`RELEASE_RULE`]).

pub mod lifecycle;
pub mod moves;
pub mod status;

use anyhow::Context;
use anyhow::Result;
use reqwest::Method;
use reqwest::StatusCode;
use ursula_control::ClusterView;
use ursula_control::PlacementApiError;
use ursula_control::PlacementGroupStatus;
use ursula_control::PlacementStatus;
use ursula_control::ReplaceDiskRequest;
use ursula_proto::placement::NodeId;
use ursula_proto::placement::NodeRecord;
use ursula_proto::placement::RootId;
use ursula_proto::placement::RootSlot;
use ursula_shard::RaftGroupId;

use crate::metrics::MetricsClient;
use crate::metrics::metrics_base_url;
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
    first_status(client, nodes, true).await
}

/// The placement status of the first node of `nodes` that answers one:
/// confirmed by a meta read barrier on its admin plane when `confirmed`,
/// else its current view over its read URL.
pub async fn first_status<'a>(
    client: &MetricsClient,
    nodes: impl IntoIterator<Item = &'a NodeInfo>,
    confirmed: bool,
) -> Option<(&'a NodeInfo, PlacementStatus)> {
    for node in nodes {
        let url = if confirmed {
            node.admin_url.join("/__ursula/placement?confirmed=true")
        } else {
            metrics_base_url(node).join("/__ursula/placement")
        };
        match read_json(client, url).await {
            Ok(placement) => return Some((node, placement)),
            Err(error) => {
                tracing::warn!(node_id = node.id, confirmed, %error, "placement: no placement status");
            }
        }
    }
    None
}

/// `GET /__ursula/placement/groups/{group}` over `node`'s read URL: the
/// group's placement in its current view and its replica of the group.
pub async fn group_status(
    client: &MetricsClient,
    node: &NodeInfo,
    group: RaftGroupId,
) -> Result<PlacementGroupStatus> {
    let url = metrics_base_url(node).join(&format!("/__ursula/placement/groups/{}", group.0));
    read_json(client, url).await
}

async fn read_json<T: serde::de::DeserializeOwned>(
    client: &MetricsClient,
    url: Result<url::Url, url::ParseError>,
) -> Result<T> {
    let url = url.context("compose the placement url")?;
    let response = client
        .http_client()
        .get(url.clone())
        .send()
        .await
        .with_context(|| format!("GET {url}"))?;
    let status = response.status();
    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        anyhow::bail!("GET {url} answered {status}: {body}");
    }
    response
        .json()
        .await
        .with_context(|| format!("decode GET {url}"))
}

/// What a node answered to a placement write.
#[derive(Debug)]
pub enum WriteAnswer<T> {
    /// The write applied, with its answer.
    Applied(T),
    /// The node refused it with a placement refusal.
    Refused {
        status: StatusCode,
        refusal: Box<PlacementApiError>,
    },
    /// The node answered something that is no placement refusal, such as a
    /// stale process incarnation (`412`).
    Unexpected { status: StatusCode, body: String },
    /// No answer arrived: the write may have applied.
    Lost(anyhow::Error),
}

/// Sends `body` to `path` on `via`'s admin plane, with the incarnation the
/// client pinned for it.
pub async fn post_write<B: serde::Serialize, T: serde::de::DeserializeOwned>(
    client: &MetricsClient,
    via: &NodeInfo,
    path: &str,
    body: &B,
) -> WriteAnswer<T> {
    send_write(client, via, path, Some(body)).await
}

/// Sends a write without a body, such as a lifecycle change, to `path` on
/// `via`'s admin plane, with the incarnation the client pinned for it.
pub async fn post_empty<T: serde::de::DeserializeOwned>(
    client: &MetricsClient,
    via: &NodeInfo,
    path: &str,
) -> WriteAnswer<T> {
    send_write::<(), T>(client, via, path, None).await
}

async fn send_write<B: serde::Serialize, T: serde::de::DeserializeOwned>(
    client: &MetricsClient,
    via: &NodeInfo,
    path: &str,
    body: Option<&B>,
) -> WriteAnswer<T> {
    let sent = async {
        let url = via
            .admin_url
            .join(path)
            .context("compose the placement url")?;
        let request = client.admin_request(via, Method::POST, url.clone()).await?;
        let request = match body {
            Some(body) => request.json(body),
            None => request,
        };
        request.send().await.with_context(|| format!("POST {url}"))
    };
    let response = match sent.await {
        Ok(response) => response,
        Err(error) => return WriteAnswer::Lost(error),
    };
    let status = response.status();
    if status.is_success() {
        return match response.json().await {
            Ok(answer) => WriteAnswer::Applied(answer),
            Err(error) => WriteAnswer::Lost(
                anyhow::Error::new(error).context(format!("decode the answer to POST {path}")),
            ),
        };
    }
    let body = response.text().await.unwrap_or_default();
    match serde_json::from_str::<PlacementApiError>(&body) {
        Ok(refusal) => WriteAnswer::Refused {
            status,
            refusal: Box::new(refusal),
        },
        Err(_not_a_refusal) => WriteAnswer::Unexpected { status, body },
    }
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
    let path = format!("/__ursula/placement/nodes/{node}/replace-disk");
    let request = ReplaceDiskRequest { expect_root: root };
    match post_write(client, via, &path, &request).await {
        WriteAnswer::Applied(record) => Ok(record),
        WriteAnswer::Refused { status, refusal } => Err(ReplaceDiskRefusal::Refused {
            via: via.id,
            status,
            refusal,
        }
        .into()),
        WriteAnswer::Unexpected { status, body } => {
            anyhow::bail!("POST {path} on node {} answered {status}: {body}", via.id)
        }
        WriteAnswer::Lost(error) => Err(error),
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

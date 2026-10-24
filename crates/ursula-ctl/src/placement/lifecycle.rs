//! `node cordon`, `uncordon` and `decommission`, and `node verify-removed`,
//! `verify-joinable` and `verify-vacant`.
//!
//! A lifecycle verb runs in passes, as a move does ([`super::moves`]). Each
//! pass reads a view a meta read barrier confirmed and decides every named
//! node with the kernel's own [`ControlPlaneState::check`], so the tool holds
//! no copy of a rule. A write that may have applied is decided again from
//! the next view, and a change to the lifecycle a node already has changes
//! nothing, so a rerun finishes what an interrupted run began.
//!
//! `decommission` checks every named node before it sends anything, so a
//! meta voter among them stops the command with nothing changed. It cordons
//! every named node that is active first, so no group moves onto a node that
//! leaves in the same scale-down, and only then decommissions them. The meta
//! leader moves their groups away, and a decommissioning node is removed for
//! good once it has no role ([`IRREVERSIBLE`]). The meta leader then
//! completes the removal and marks the id `vacant`, and an empty disk may
//! register under it as a new node.
//!
//! `verify-removed`, `verify-joinable` and `verify-vacant` read the meta
//! leader's own view over the nodes' read URLs ([`crate::view`]), which a
//! chart hook reaches, and decide each node with one check
//! ([`check_removal`]). The two that start a node wait while a node is
//! removed and its removal has not completed, printing what it waits for
//! from the meta leader's report.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::io::Write;
use std::time::Duration;

use anyhow::Result;
use reqwest::StatusCode;
use ursula_control::Cluster;
use ursula_control::ClusterView;
use ursula_control::ControlCommand;
use ursula_control::ControlError;
use ursula_control::ControlPlaneState;
use ursula_control::LifecycleChange;
use ursula_control::LifecycleResponse;
use ursula_control::PendingRemoval;
use ursula_control::PlacementApiReason;
use ursula_control::RemovalDirectory;
use ursula_control::Requeue;
use ursula_proto::placement::NodeId;
use ursula_proto::placement::NodeLifecycle;
use ursula_proto::placement::WriteEffect;

use super::WriteAnswer;
use super::confirmed_status;
use super::post_empty;
use super::status::join;
use crate::maintenance::Deadline;
use crate::metrics::MetricsClient;
use crate::provider::NodeInfo;
use crate::view::ViewRefusal;

/// What `node decommission` prints before it decommissions a node.
pub const IRREVERSIBLE: &str = "A decommissioning node is removed for good once its last group \
                                moved away: its process and its disk never take part again. Once \
                                its removal completes the id is vacant, and an empty disk started \
                                under it registers as a new node. `node uncordon` aborts a \
                                decommission only while the node still has a role.";

/// Why a lifecycle verb sends no change for a node, or stopped.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum LifecycleRefusal {
    #[error(
        "node {node} is a meta voter, which is never decommissioned: replace its host under the \
         same id with an empty disk (operations guide, Rejoining a wiped node). Nothing was sent"
    )]
    MetaVoter { node: NodeId },
    /// The meta state machine refuses the change, on the confirmed view or
    /// in its answer.
    #[error("node {node}: {error}")]
    Kernel { node: NodeId, error: ControlError },
    #[error("node {via} answered {status}: {body}")]
    Unexpected {
        via: u64,
        status: StatusCode,
        body: String,
    },
}

impl LifecycleRefusal {
    fn of(node: NodeId, error: ControlError) -> Self {
        match error {
            ControlError::MetaVoter { node } => Self::MetaVoter { node },
            error => Self::Kernel { node, error },
        }
    }
}

/// How a lifecycle verb runs.
#[derive(Debug, Clone)]
pub struct LifecycleOptions {
    pub nodes: BTreeSet<NodeId>,
    /// `decommission`: wait until every node is removed.
    pub wait: bool,
    /// Bounds the whole command.
    pub timeout: Duration,
    pub poll_interval: Duration,
}

/// How a lifecycle verb ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LifecycleEnd {
    /// Every node is at the lifecycle the verb asks for: removed for a
    /// `decommission --wait`.
    Done,
    /// `decommission` without `wait`: every node is decommissioning or
    /// removed, and these still have a role.
    Leaving(BTreeSet<NodeId>),
    /// The timeout passed before these nodes got there.
    TimedOut(BTreeSet<NodeId>),
}

impl LifecycleEnd {
    pub fn is_failure(&self) -> bool {
        matches!(self, Self::TimedOut(_))
    }
}

/// What a decommission pass does next.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DecommissionPass {
    /// Cordon these active nodes, before any named node is decommissioned.
    Cordon(BTreeSet<NodeId>),
    /// Every named node is cordoned or further: decommission these.
    Decommission(BTreeSet<NodeId>),
    /// Every named node is decommissioning or removed, and these still have
    /// a role.
    Leaving(BTreeSet<NodeId>),
}

fn command(node: NodeId, change: LifecycleChange) -> ControlCommand {
    ControlCommand::SetNodeLifecycle {
        node,
        change,
        now_ms: 0,
    }
}

fn lifecycle_of(state: &ControlPlaneState, node: NodeId) -> Option<NodeLifecycle> {
    state
        .cluster()
        .and_then(|cluster| cluster.node(node))
        .map(|record| record.lifecycle)
}

/// Decides a decommission pass of `nodes` on `state`, a confirmed view. A
/// removed node is done. Every other one must pass the meta state
/// machine's check of its decommission, on every pass, so a refusal stops
/// the command before it sends anything.
pub fn plan_decommission(
    state: &ControlPlaneState,
    nodes: &BTreeSet<NodeId>,
) -> Result<DecommissionPass, LifecycleRefusal> {
    let mut cordon = BTreeSet::new();
    let mut decommission = BTreeSet::new();
    let mut leaving = BTreeSet::new();
    for &node in nodes {
        let lifecycle = lifecycle_of(state, node);
        if lifecycle.is_some_and(|lifecycle| !lifecycle.has_life()) {
            continue;
        }
        state
            .check(&command(node, LifecycleChange::Decommission))
            .map_err(|error| LifecycleRefusal::of(node, error))?;
        match lifecycle {
            Some(NodeLifecycle::Active) => cordon.insert(node),
            Some(NodeLifecycle::Cordoned) => decommission.insert(node),
            // The check refuses a node the view does not hold.
            Some(
                NodeLifecycle::Decommissioning | NodeLifecycle::Removed | NodeLifecycle::Vacant,
            )
            | None => leaving.insert(node),
        };
    }
    Ok(if !cordon.is_empty() {
        DecommissionPass::Cordon(cordon)
    } else if !decommission.is_empty() {
        DecommissionPass::Decommission(decommission)
    } else {
        DecommissionPass::Leaving(leaving)
    })
}

/// The lifecycle `change` leaves a node at, for `cordon` and `uncordon`.
fn wanted(change: LifecycleChange) -> NodeLifecycle {
    match change {
        LifecycleChange::Cordon => NodeLifecycle::Cordoned,
        LifecycleChange::Uncordon => NodeLifecycle::Active,
        LifecycleChange::Decommission => NodeLifecycle::Decommissioning,
    }
}

/// The nodes of `nodes` that `change` is still to be sent for on `state`,
/// refusing as the meta state machine would.
pub fn pending_changes(
    state: &ControlPlaneState,
    nodes: &BTreeSet<NodeId>,
    change: LifecycleChange,
) -> Result<BTreeSet<NodeId>, LifecycleRefusal> {
    let mut pending = BTreeSet::new();
    for &node in nodes {
        if lifecycle_of(state, node) == Some(wanted(change)) {
            continue;
        }
        state
            .check(&command(node, change))
            .map_err(|error| LifecycleRefusal::of(node, error))?;
        pending.insert(node);
    }
    Ok(pending)
}

/// Classifies `via`'s answer to a lifecycle change of `node`: the
/// lifecycle it applied, `None` to decide the node again from the next
/// confirmed view, or a refusal no later view changes.
pub fn answered(
    via: u64,
    node: NodeId,
    answer: WriteAnswer<LifecycleResponse>,
) -> Result<Option<NodeLifecycle>, LifecycleRefusal> {
    match answer {
        WriteAnswer::Applied(LifecycleResponse { lifecycle }) => Ok(Some(lifecycle)),
        WriteAnswer::Lost(error) => {
            tracing::warn!(node_id = via, %error, "placement: no answer to a lifecycle change");
            Ok(None)
        }
        WriteAnswer::Unexpected { status, body } => {
            Err(LifecycleRefusal::Unexpected { via, status, body })
        }
        WriteAnswer::Refused { status: _, refusal } => match refusal.reason {
            PlacementApiReason::Refused(error)
                if refusal.write_effect == WriteEffect::NotApplied
                    && error.requeue() == Requeue::Permanent =>
            {
                Err(LifecycleRefusal::of(node, error))
            }
            PlacementApiReason::Refused(_)
            | PlacementApiReason::NoLeader
            | PlacementApiReason::ForwardFailed { .. }
            | PlacementApiReason::LeaderRefused { .. }
            | PlacementApiReason::Stopped
            | PlacementApiReason::Unconfirmed => Ok(None),
        },
    }
}

fn route(node: NodeId, change: LifecycleChange) -> String {
    let verb = match change {
        LifecycleChange::Cordon => "cordon",
        LifecycleChange::Uncordon => "uncordon",
        LifecycleChange::Decommission => "decommission",
    };
    format!("/__ursula/placement/nodes/{node}/{verb}")
}

/// Sends `change` of each of `nodes` through `via` and prints what each
/// answer applied.
async fn send_changes(
    client: &MetricsClient,
    via: &NodeInfo,
    nodes: &BTreeSet<NodeId>,
    change: LifecycleChange,
    out: &mut impl Write,
) -> Result<()> {
    for &node in nodes {
        let answer = post_empty(client, via, &route(node, change)).await;
        match answered(via.id, node, answer)? {
            Some(lifecycle) => writeln!(out, "node {node}: {lifecycle}")?,
            None => writeln!(
                out,
                "node {node}: the change may not have applied, reading a confirmed view"
            )?,
        }
    }
    Ok(())
}

fn state_of(view: ClusterView) -> ControlPlaneState {
    match view {
        ClusterView::Bootstrapped(cluster) => ControlPlaneState::Bootstrapped(cluster),
        ClusterView::FormationCut => ControlPlaneState::FormationCut,
        ClusterView::MetaGenesis { .. }
        | ClusterView::MetaCatchingUp
        | ClusterView::Registering { .. } => ControlPlaneState::default(),
    }
}

/// `node cordon` and `node uncordon`: sends `change` of every node not yet
/// at its lifecycle, until a confirmed view shows each there.
pub async fn set_lifecycles(
    client: &MetricsClient,
    nodes: &[NodeInfo],
    change: LifecycleChange,
    options: &LifecycleOptions,
    out: &mut impl Write,
) -> Result<LifecycleEnd> {
    let deadline = Deadline::after(options.timeout);
    let mut open = options.nodes.clone();
    loop {
        match confirmed_status(client, nodes).await {
            Some((via, status)) => {
                open = pending_changes(&state_of(status.view), &options.nodes, change)?;
                if open.is_empty() {
                    return Ok(LifecycleEnd::Done);
                }
                send_changes(client, via, &open, change, out).await?;
            }
            None => writeln!(out, "no node answered a confirmed placement view")?,
        }
        if deadline.is_reached() {
            return Ok(LifecycleEnd::TimedOut(open));
        }
        tokio::time::sleep(options.poll_interval).await;
    }
}

/// `node decommission`: cordons every named node that is active, then
/// decommissions each, and with `wait` waits until each is removed,
/// printing how many groups each still has a role in and the groups the
/// meta leader's last pass could not move.
pub async fn decommission(
    client: &MetricsClient,
    nodes: &[NodeInfo],
    options: &LifecycleOptions,
    out: &mut impl Write,
) -> Result<LifecycleEnd> {
    let deadline = Deadline::after(options.timeout);
    let mut open = options.nodes.clone();
    let mut warned = false;
    let mut progress = String::new();
    loop {
        match confirmed_status(client, nodes).await {
            Some((via, status)) => {
                let state = state_of(status.view);
                match plan_decommission(&state, &options.nodes)? {
                    DecommissionPass::Cordon(cordon) => {
                        writeln!(out, "cordoning nodes {} first", join(cordon.iter()))?;
                        send_changes(client, via, &cordon, LifecycleChange::Cordon, out).await?;
                    }
                    DecommissionPass::Decommission(leaving) => {
                        if !warned {
                            writeln!(out, "warning: {IRREVERSIBLE}")?;
                            warned = true;
                        }
                        let change = LifecycleChange::Decommission;
                        send_changes(client, via, &leaving, change, out).await?;
                    }
                    DecommissionPass::Leaving(leaving) => {
                        if leaving.is_empty() {
                            writeln!(out, "nodes {} are removed", join(options.nodes.iter()))?;
                            return Ok(LifecycleEnd::Done);
                        }
                        if !options.wait {
                            return Ok(LifecycleEnd::Leaving(leaving));
                        }
                        let line = leaving_progress(client, nodes, state.cluster(), &leaving).await;
                        if line != progress {
                            writeln!(out, "{line}")?;
                            progress = line;
                        }
                        open = leaving;
                    }
                }
            }
            None => writeln!(out, "no node answered a confirmed placement view")?,
        }
        if deadline.is_reached() {
            return Ok(LifecycleEnd::TimedOut(open));
        }
        tokio::time::sleep(options.poll_interval).await;
    }
}

/// How many groups each leaving node still has a role in, and the groups
/// the meta leader's last decommission pass could not move.
async fn leaving_progress(
    client: &MetricsClient,
    nodes: &[NodeInfo],
    cluster: Option<&Cluster>,
    leaving: &BTreeSet<NodeId>,
) -> String {
    let mut line = leaving
        .iter()
        .map(|&node| {
            let groups = cluster.map_or(0, |cluster| {
                cluster
                    .groups()
                    .iter()
                    .filter(|(_, placement)| placement.role(node).is_some())
                    .count()
            });
            format!("node {node}: decommissioning, a role in {groups} groups")
        })
        .collect::<Vec<_>>()
        .join("; ");
    if let Ok(Ok(status)) = crate::view::leader_status(client, nodes).await
        && let Some(report) = status.leader_report
        && !report.decommission_blocked.is_empty()
    {
        let blocked: BTreeMap<_, _> = report
            .decommission_blocked
            .iter()
            .map(|blocked| (blocked.group.0, blocked.reason))
            .collect();
        line.push_str(&format!(
            "; groups the meta leader cannot move: {blocked:?}"
        ));
    }
    line
}

/// What `verify-removed`, `verify-joinable` and `verify-vacant` check.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Removal {
    /// Every node is removed, vacant or never registered: a scale-down may
    /// drop it.
    Leaving,
    /// Every node may start: it never registered, it is vacant, so an
    /// empty disk registers as a new node, or it is in a life, so its own
    /// process comes back on its disk. A removed node waits for its removal
    /// to complete.
    Joining,
    /// Every node is vacant, so a volume left under its id holds a removed
    /// life's disk, or a root that never bound. A removed node waits for its
    /// removal to complete.
    Vacant,
}

/// Why a removal check failed.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RemovalRefusal {
    #[error(
        "node {node} is {lifecycle}, not removed: run `ursulactl node decommission --node {node} \
         --wait` before it leaves"
    )]
    NotRemoved {
        node: NodeId,
        lifecycle: NodeLifecycle,
    },
    #[error(
        "the cluster has not bootstrapped, so no node is removed, and a node that leaves now may \
         still be given groups"
    )]
    NotBootstrapped,
    /// A removed node whose removal had not completed by the timeout, with
    /// what it waits for as the meta leader last reported it.
    #[error(
        "node {node} is removed and its removal has not completed: a disk started under its id \
         waits until the id is vacant. It waits for {}",
        pending.as_ref().map_or_else(|| "the meta leader's next pass".to_owned(), pending_text)
    )]
    RemovalPending {
        node: NodeId,
        pending: Option<PendingRemoval>,
    },
    /// `verify-vacant`: the node is in a life, so a disk under its id is
    /// that life's.
    #[error("node {node} is {lifecycle}: a disk under its id is its own, keep it")]
    InLife {
        node: NodeId,
        lifecycle: NodeLifecycle,
    },
    /// `verify-vacant`: the node never registered, so a disk under its id
    /// is no node's of this cluster.
    #[error("node {node} never registered: a disk under its id belongs to no node of this cluster")]
    NotRegistered { node: NodeId },
    #[error(transparent)]
    View(ViewRefusal),
}

/// What a pending removal waits for, in words.
fn pending_text(pending: &PendingRemoval) -> String {
    let mut waits = Vec::new();
    if !pending.unconfirmed.is_empty() {
        waits.push(format!(
            "a live leader of groups {} to confirm the node no member (a group without one is \
             down or lost its quorum)",
            join(pending.unconfirmed.iter().map(|group| group.0))
        ));
    }
    match &pending.directory {
        RemovalDirectory::Swept => {}
        RemovalDirectory::Unlisted => {
            waits.push("its snapshot directory to list".to_owned());
        }
        RemovalDirectory::Unswept(groups) => waits.push(format!(
            "its snapshot objects of groups {} to be swept",
            join(groups.iter().map(|group| group.0))
        )),
    }
    if pending.meta_member {
        waits.push("the meta group to drop it as a learner".to_owned());
    }
    if pending.process_answers {
        waits.push(format!(
            "its old process, on root {}, to stop answering: stop it, and never start its disk \
             again",
            pending.root
        ));
    }
    if waits.is_empty() {
        "the meta leader's next pass".to_owned()
    } else {
        waits.join("; ")
    }
}

/// Checks `nodes` against `cluster`, the meta leader's bootstrapped view,
/// `None` before Bootstrap. The one decision of every removal check.
pub fn check_removal(
    cluster: Option<&Cluster>,
    nodes: &BTreeSet<NodeId>,
    removal: Removal,
) -> Result<(), RemovalRefusal> {
    let Some(cluster) = cluster else {
        return match removal {
            Removal::Leaving => Err(RemovalRefusal::NotBootstrapped),
            Removal::Joining => Ok(()),
            Removal::Vacant => match nodes.first() {
                Some(&node) => Err(RemovalRefusal::NotRegistered { node }),
                None => Ok(()),
            },
        };
    };
    for &node in nodes {
        let lifecycle = cluster.node(node).map(|record| record.lifecycle);
        match (removal, lifecycle) {
            (Removal::Leaving, Some(lifecycle)) if lifecycle.has_life() => {
                return Err(RemovalRefusal::NotRemoved { node, lifecycle });
            }
            (Removal::Joining | Removal::Vacant, Some(NodeLifecycle::Removed)) => {
                return Err(RemovalRefusal::RemovalPending {
                    node,
                    pending: None,
                });
            }
            (Removal::Vacant, Some(lifecycle)) if lifecycle.has_life() => {
                return Err(RemovalRefusal::InLife { node, lifecycle });
            }
            (Removal::Vacant, None) => return Err(RemovalRefusal::NotRegistered { node }),
            (Removal::Leaving | Removal::Joining | Removal::Vacant, _) => {}
        }
    }
    Ok(())
}

/// `verify-removed`, `verify-joinable` and `verify-vacant`: checks `ids`
/// against the meta leader's own view, reading it again while no node names
/// a meta leader and, for a check that starts a node, while a node's
/// removal has not completed, until `timeout`. Each change of what a
/// pending removal waits for is printed to `out`.
pub async fn verify_removal(
    client: &MetricsClient,
    nodes: &[NodeInfo],
    ids: &BTreeSet<NodeId>,
    removal: Removal,
    timeout: Duration,
    poll_interval: Duration,
    out: &mut impl Write,
) -> Result<()> {
    let deadline = Deadline::after(timeout);
    let mut printed = None;
    loop {
        let status = match crate::view::leader_status(client, nodes).await? {
            Ok(status) => status,
            Err(refusal) => {
                if deadline.is_reached() {
                    return Err(RemovalRefusal::View(refusal).into());
                }
                tokio::time::sleep(poll_interval).await;
                continue;
            }
        };
        let cluster = match &status.view {
            ClusterView::Bootstrapped(cluster) => Some(cluster),
            ClusterView::MetaGenesis { .. }
            | ClusterView::MetaCatchingUp
            | ClusterView::Registering { .. }
            | ClusterView::FormationCut => None,
        };
        match check_removal(cluster, ids, removal) {
            Err(RemovalRefusal::RemovalPending { node, .. }) => {
                let pending = status
                    .leader_report
                    .and_then(|report| report.removal_pending.get(&node).cloned());
                let refusal = RemovalRefusal::RemovalPending { node, pending };
                if deadline.is_reached() {
                    return Err(refusal.into());
                }
                let line = refusal.to_string();
                if printed.as_ref() != Some(&line) {
                    writeln!(out, "waiting: {line}")?;
                    printed = Some(line);
                }
            }
            checked => return Ok(checked?),
        }
        tokio::time::sleep(poll_interval).await;
    }
}

#[cfg(test)]
mod tests;

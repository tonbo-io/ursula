//! `placement status`: a node's placement view and, for each moving group
//! (or the one group asked for), what its leader's reconcile shell does.
//!
//! The leader is found by asking `GET /__ursula/placement/groups/{group}` of
//! every node the group moves from or to, over their read URLs: the replica
//! that reports itself as the leader at the highest term. A leader whose
//! `target_since` trails the view's placement pursues an older placement,
//! and a moving group that no answering replica leads makes no progress.

use std::collections::BTreeSet;
use std::io::Write;

use anyhow::Result;
use serde::Serialize;
use ursula_control::ClusterView;
use ursula_control::GroupPlacement;
use ursula_control::MetaReplicaStatus;
use ursula_control::PlacementStatus;
use ursula_control::ReconcileStatus;
use ursula_control::Target;
use ursula_proto::placement::NodeId;
use ursula_shard::RaftGroupId;

use super::first_status;
use super::group_status;
use crate::metrics::MetricsClient;
use crate::provider::NodeInfo;

/// Which groups `placement status` lists.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StatusFilter {
    /// Every moving group.
    Moving,
    /// Every group the node has a role in.
    Node(NodeId),
    /// One group, moving or not.
    Group(RaftGroupId),
}

/// What `placement status` found, as `--json` prints it.
#[derive(Debug, Clone, Serialize)]
pub struct PlacementReport {
    /// The node whose view this is.
    pub via: u64,
    /// The view was confirmed by a meta read barrier.
    pub confirmed: bool,
    pub status: PlacementStatus,
    /// The groups the filter selects, in ascending id.
    pub groups: Vec<GroupReport>,
}

/// One listed group.
#[derive(Debug, Clone, Serialize)]
pub struct GroupReport {
    pub group: u32,
    pub placement: GroupPlacement,
    /// The group's leader, asked for a moving group or the one group asked
    /// for. `null` when it was not asked.
    pub leader: Option<LeaderLookup>,
}

/// What the group's replicas answered about its leader.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum LeaderLookup {
    /// The replica that leads at the highest term.
    Found {
        node: u64,
        term: u64,
        /// What its reconcile shell does, `null` when it runs none.
        reconcile: Option<ReconcileStatus>,
        /// The placement it pursues is older than the view's.
        trails_placement: bool,
    },
    /// No replica that answered leads the group.
    NotFound {
        answered: BTreeSet<u64>,
        unanswered: BTreeSet<u64>,
    },
}

impl LeaderLookup {
    /// The leader's node and reconcile state, which `move --wait` prints
    /// when they change.
    pub(crate) fn progress(&self) -> String {
        match self {
            Self::Found {
                node,
                term,
                reconcile,
                trails_placement,
            } => {
                let state = reconcile.as_ref().map_or_else(
                    || "runs no reconcile shell".to_owned(),
                    |reconcile| describe_reconcile(reconcile, *trails_placement),
                );
                format!("leader node {node} (term {term}): {state}")
            }
            Self::NotFound {
                answered,
                unanswered,
            } => format!(
                "no leader: answered {}, unanswered {}",
                join(answered.iter()),
                join(unanswered.iter())
            ),
        }
    }
}

/// `placement status`: the first answering node's view, confirmed on its
/// admin plane when `confirmed`, and the groups `filter` selects.
pub async fn placement_status(
    client: &MetricsClient,
    nodes: &[NodeInfo],
    filter: StatusFilter,
    confirmed: bool,
) -> Result<PlacementReport> {
    let (via, status) = first_status(client, nodes, confirmed)
        .await
        .ok_or_else(|| anyhow::anyhow!("no node answered a placement status"))?;
    let mut groups = Vec::new();
    if let ClusterView::Bootstrapped(cluster) = &status.view {
        for (group, placement) in cluster.groups().iter() {
            let listed = match filter {
                StatusFilter::Moving => placement.moving().is_some(),
                StatusFilter::Node(node) => placement.role(node).is_some(),
                StatusFilter::Group(wanted) => group == wanted,
            };
            if !listed {
                continue;
            }
            let asked = placement.moving().is_some() || matches!(filter, StatusFilter::Group(_));
            let leader = if asked {
                Some(find_leader(client, nodes, group, placement).await)
            } else {
                None
            };
            groups.push(GroupReport {
                group: group.0,
                placement: placement.clone(),
                leader,
            });
        }
        if let StatusFilter::Group(wanted) = filter
            && cluster.group(wanted).is_none()
        {
            anyhow::bail!("group {} is not in the view of node {}", wanted.0, via.id);
        }
    }
    Ok(PlacementReport {
        via: via.id,
        confirmed,
        status,
        groups,
    })
}

/// Asks every node `placement` moves from or to for its replica of `group`,
/// and picks the replica that leads at the highest term.
pub async fn find_leader(
    client: &MetricsClient,
    nodes: &[NodeInfo],
    group: RaftGroupId,
    placement: &GroupPlacement,
) -> LeaderLookup {
    let asked: BTreeSet<u64> = placement
        .current_voters()
        .iter()
        .chain(placement.target_voters().iter())
        .map(NodeId::get)
        .collect();
    let answers = futures_util::future::join_all(
        nodes
            .iter()
            .filter(|node| asked.contains(&node.id))
            .map(|node| async move { (node.id, group_status(client, node, group).await) }),
    )
    .await;
    let mut answered = BTreeSet::new();
    let mut leader: Option<(u64, u64, Option<ReconcileStatus>)> = None;
    for (node, answer) in answers {
        let Ok(answer) = answer else {
            continue;
        };
        answered.insert(node);
        let Some(replica) = answer.replica else {
            continue;
        };
        let leads = replica.current_leader == Some(replica.node_id);
        let higher = leader
            .as_ref()
            .is_none_or(|(_, term, _)| replica.current_term > *term);
        if leads && higher {
            leader = Some((node, replica.current_term, replica.reconcile));
        }
    }
    match leader {
        Some((node, term, reconcile)) => {
            let trails_placement = reconcile
                .as_ref()
                .and_then(|reconcile| reconcile.target_since)
                .is_some_and(|pursued| pursued < placement.since());
            LeaderLookup::Found {
                node,
                term,
                reconcile,
                trails_placement,
            }
        }
        None => LeaderLookup::NotFound {
            unanswered: asked.difference(&answered).copied().collect(),
            answered,
        },
    }
}

/// Prints `report` for an operator.
pub fn write_report(out: &mut impl Write, report: &PlacementReport) -> std::io::Result<()> {
    let status = &report.status;
    let confirmed = if report.confirmed {
        "confirmed"
    } else {
        "current"
    };
    let applied = status
        .applied
        .map_or_else(|| "nothing".to_owned(), |applied| applied.to_string());
    let meta = match &status.meta {
        MetaReplicaStatus::Running { role, leader } => format!(
            "meta {role:?}, leader {}",
            leader.map_or_else(|| "unknown".to_owned(), |leader| leader.to_string())
        ),
        MetaReplicaStatus::Damaged { kind, message } => {
            format!("meta damaged ({kind:?}): {message}")
        }
        MetaReplicaStatus::Absent => "no meta replica".to_owned(),
    };
    writeln!(
        out,
        "node {} ({confirmed} view): {:?}, applied {applied}, {meta}",
        report.via,
        status.view.stage()
    )?;
    let ClusterView::Bootstrapped(cluster) = &status.view else {
        return Ok(());
    };
    let params = cluster.params();
    writeln!(
        out,
        "{} groups at replication factor {}, {} moving, move limit {}",
        cluster.groups().len(),
        params.replication_factor.get(),
        cluster.moving(),
        params.max_concurrent_moves.get()
    )?;
    for (node, record) in cluster.nodes() {
        writeln!(
            out,
            "node {node}: {}, root {}",
            record.lifecycle, record.root
        )?;
    }
    for group in &report.groups {
        writeln!(
            out,
            "group {}: {}",
            group.group,
            describe_placement(&group.placement)
        )?;
        if let Some(leader) = &group.leader {
            writeln!(out, "  {}", leader.progress())?;
        }
    }
    Ok(())
}

/// A placement in one line: where the group lives and its key.
pub fn describe_placement(placement: &GroupPlacement) -> String {
    match placement {
        GroupPlacement::Seeded {
            since,
            voters,
            initializer,
        } => format!(
            "seeded on {} (initializer {initializer}), since {since}",
            join(voters.iter())
        ),
        GroupPlacement::Live {
            since,
            converged_at,
            target: Target::Stable { voters },
        } => format!(
            "stable on {}, since {since}, converged at {converged_at}",
            join(voters.iter())
        ),
        GroupPlacement::Live {
            since,
            converged_at,
            target: Target::Moving(moving),
        } => {
            let abandoned = if moving.abandoned.is_empty() {
                String::new()
            } else {
                format!(", abandoned {}", join(moving.abandoned.iter()))
            };
            format!(
                "moving from {} to {}{abandoned}, since {since}, converged at {converged_at}",
                join(moving.from.iter()),
                join(moving.to.iter())
            )
        }
    }
}

/// A reconcile status in one line: its state as admin JSON writes it, the
/// attempts and last failure, and whether it pursues an older placement.
fn describe_reconcile(reconcile: &ReconcileStatus, trails_placement: bool) -> String {
    let state = serde_json::to_string(&reconcile.state)
        .unwrap_or_else(|_unencodable| format!("{:?}", reconcile.state));
    let failure = reconcile.last_failure.map_or_else(String::new, |failure| {
        format!(", last failure {:?} at {:?}", failure.kind, failure.step)
    });
    let trails = if trails_placement {
        let pursued = reconcile
            .target_since
            .map_or_else(String::new, |since| since.to_string());
        format!(", TRAILS the placement: pursues {pursued}")
    } else {
        String::new()
    };
    format!("{state}, attempts {}{failure}{trails}", reconcile.attempts)
}

/// Node ids as `1,2,3`.
pub(crate) fn join<T: std::fmt::Display>(ids: impl Iterator<Item = T>) -> String {
    let ids: Vec<String> = ids.map(|id| id.to_string()).collect();
    if ids.is_empty() {
        "none".to_owned()
    } else {
        ids.join(",")
    }
}

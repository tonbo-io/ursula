//! Read-only verbs that operate on `/__ursula/metrics`. These are direct ports
//! of `ursula_ec2.py`'s `status` / `wait-ready` — same metrics surface, no SSH
//! dependency.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::io::Write;
use std::time::{Duration, Instant};

use anyhow::{Result, bail};

use crate::metrics::{ClusterSnapshot, MetricsClient};
use crate::provider::NodeInfo;

#[derive(Debug, Clone)]
pub struct StatusReport {
    pub per_node: Vec<NodeStatus>,
}

#[derive(Debug, Clone)]
pub struct NodeStatus {
    pub id: u64,
    pub host: String,
    /// `None` when metrics fetch failed for this node.
    pub group_count: Option<usize>,
    /// node_id → number of groups led by that node, observed from this node's metrics.
    pub leadership_counts: BTreeMap<u64, usize>,
    pub error: Option<String>,
}

/// Build a status report by fetching metrics from every node. Missing nodes are
/// recorded with `error: Some(_)` rather than aborting the whole report —
/// `status` is meant to surface partial cluster health.
pub async fn collect_status(client: &MetricsClient, nodes: &[NodeInfo]) -> StatusReport {
    let mut per_node = Vec::with_capacity(nodes.len());
    for node in nodes {
        match client.fetch_node(node).await {
            Ok(view) => {
                let mut counts: BTreeMap<u64, usize> = BTreeMap::new();
                for group in &view.groups {
                    if !group_is_initialized(group) {
                        continue;
                    }
                    if let Some(leader) = group.current_leader {
                        *counts.entry(leader).or_default() += 1;
                    }
                }
                per_node.push(NodeStatus {
                    id: node.id,
                    host: node.host.clone(),
                    group_count: Some(view.groups.len()),
                    leadership_counts: counts,
                    error: None,
                });
            }
            Err(err) => per_node.push(NodeStatus {
                id: node.id,
                host: node.host.clone(),
                group_count: None,
                leadership_counts: BTreeMap::new(),
                error: Some(format!("{err:#}")),
            }),
        }
    }
    StatusReport { per_node }
}

pub fn write_status<W: Write>(out: &mut W, report: &StatusReport) -> std::io::Result<()> {
    for status in &report.per_node {
        write!(out, "node {} ({})", status.id, status.host)?;
        match &status.error {
            Some(err) => writeln!(out, ": metrics unavailable — {err}")?,
            None => {
                let groups = status.group_count.unwrap_or(0);
                let leaders = format_leaders(&status.leadership_counts);
                writeln!(out, ": groups={groups} leaders={leaders}")?;
            }
        }
    }
    Ok(())
}

fn format_leaders(counts: &BTreeMap<u64, usize>) -> String {
    if counts.is_empty() {
        return "{}".to_owned();
    }
    let mut out = String::from("{");
    for (i, (id, count)) in counts.iter().enumerate() {
        if i > 0 {
            out.push_str(", ");
        }
        let _ = write!(out, "{id}: {count}");
    }
    out.push('}');
    out
}

/// Block until every node reports `expected_groups` raft groups and every
/// initialized group has a leader observed by that node. Empty groups with no
/// voters are allowed because raft groups are initialized lazily.
pub async fn wait_ready(
    client: &MetricsClient,
    nodes: &[NodeInfo],
    expected_groups: usize,
    timeout: Duration,
    poll_interval: Duration,
) -> Result<ClusterSnapshot> {
    let deadline = Instant::now() + timeout;
    let mut last_summary = String::from("no metrics yet");
    loop {
        let snapshot = client.try_fetch_cluster(nodes).await;
        if cluster_ready(&snapshot, nodes.len(), expected_groups, &mut last_summary) {
            return Ok(snapshot);
        }
        if Instant::now() >= deadline {
            bail!("cluster not ready after {:?}: {last_summary}", timeout);
        }
        tokio::time::sleep(poll_interval).await;
    }
}

fn cluster_ready(
    snapshot: &ClusterSnapshot,
    expected_nodes: usize,
    expected_groups: usize,
    summary: &mut String,
) -> bool {
    if snapshot.per_node.len() != expected_nodes {
        *summary = format!(
            "only {}/{expected_nodes} nodes reported metrics",
            snapshot.per_node.len()
        );
        return false;
    }
    for view in &snapshot.per_node {
        if view.groups.len() != expected_groups {
            *summary = format!(
                "node {} reports {} groups, expected {expected_groups}",
                view.node.id,
                view.groups.len()
            );
            return false;
        }
        let groups_without_leader = view
            .groups
            .iter()
            .filter(|g| group_is_initialized(g))
            .filter(|g| g.current_leader.is_none())
            .count();
        if groups_without_leader > 0 {
            *summary = format!(
                "node {} has {groups_without_leader} group(s) without a leader",
                view.node.id
            );
            return false;
        }
    }
    *summary = format!(
        "all {expected_nodes} nodes report {expected_groups} groups; initialized groups have leaders"
    );
    true
}

fn group_is_initialized(group: &crate::metrics::RaftGroupView) -> bool {
    !group.voter_ids.is_empty()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::{NodeMetricsView, RaftGroupView};
    use url::Url;

    fn node(id: u64) -> NodeInfo {
        NodeInfo {
            id,
            http_url: Url::parse(&format!("http://10.0.0.{id}/")).unwrap(),
            host: format!("10.0.0.{id}"),
            name: None,
        }
    }

    fn group(raft_group_id: u64, leader: Option<u64>) -> RaftGroupView {
        RaftGroupView {
            raft_group_id,
            node_id: 0,
            current_leader: leader,
            committed_index: Some(1),
            last_applied_index: Some(1),
            voter_ids: vec![1, 2, 3],
            learner_ids: vec![],
        }
    }

    fn empty_group(raft_group_id: u64) -> RaftGroupView {
        RaftGroupView {
            raft_group_id,
            node_id: 0,
            current_leader: None,
            committed_index: None,
            last_applied_index: None,
            voter_ids: vec![],
            learner_ids: vec![],
        }
    }

    #[test]
    fn cluster_ready_requires_every_node_and_leader_per_group() {
        let snapshot = ClusterSnapshot {
            per_node: vec![
                NodeMetricsView {
                    node: node(1),
                    groups: vec![group(7, Some(1)), group(8, Some(2))],
                },
                NodeMetricsView {
                    node: node(2),
                    groups: vec![group(7, Some(1)), group(8, Some(2))],
                },
            ],
        };
        let mut summary = String::new();
        assert!(cluster_ready(&snapshot, 2, 2, &mut summary));
    }

    #[test]
    fn cluster_ready_false_when_group_lacks_leader() {
        let snapshot = ClusterSnapshot {
            per_node: vec![NodeMetricsView {
                node: node(1),
                groups: vec![group(7, None)],
            }],
        };
        let mut summary = String::new();
        assert!(!cluster_ready(&snapshot, 1, 1, &mut summary));
        assert!(summary.contains("without a leader"));
    }

    #[test]
    fn cluster_ready_allows_uninitialized_empty_groups() {
        let snapshot = ClusterSnapshot {
            per_node: vec![NodeMetricsView {
                node: node(1),
                groups: vec![group(7, Some(1)), empty_group(8)],
            }],
        };
        let mut summary = String::new();
        assert!(cluster_ready(&snapshot, 1, 2, &mut summary));
    }

    #[test]
    fn write_status_emits_one_line_per_node() {
        let report = StatusReport {
            per_node: vec![NodeStatus {
                id: 1,
                host: "h1".into(),
                group_count: Some(4),
                leadership_counts: BTreeMap::from([(1u64, 2usize), (2u64, 2usize)]),
                error: None,
            }],
        };
        let mut out = Vec::new();
        write_status(&mut out, &report).unwrap();
        let s = String::from_utf8(out).unwrap();
        assert!(s.contains("node 1 (h1)"));
        assert!(s.contains("groups=4"));
        assert!(s.contains("leaders={1: 2, 2: 2}"));
    }
}

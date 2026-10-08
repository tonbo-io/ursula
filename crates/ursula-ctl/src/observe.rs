//! Read-only verbs that operate on `/__ursula/metrics`. These are direct ports
//! of the retired `ursula_ec2.py` `status` / `wait-ready` — same metrics surface, no SSH
//! dependency.

use std::collections::BTreeMap;
use std::io::Write;
use std::time::Duration;
use std::time::Instant;

use anyhow::Result;

use crate::metrics::ClusterSnapshot;
use crate::metrics::MetricsClient;
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
                        let count = counts.entry(leader).or_default();
                        *count = count.saturating_add(1);
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
    let entries: Vec<String> = counts
        .iter()
        .map(|(id, count)| format!("{id}: {count}"))
        .collect();
    format!("{{{}}}", entries.join(", "))
}

/// A refusal retains the report or serving reason that prevents maintenance.
#[derive(Debug, thiserror::Error)]
pub enum ReadinessRefusal {
    #[error("node {node_id} lacks complete maintenance evidence: {report:?}")]
    Maintenance {
        node_id: u64,
        report: Option<ursula_proto::admin::RaftMaintenanceReport>,
    },
    #[error("node {node_id} has uncertain maintenance authority")]
    FenceUncertain { node_id: u64 },
    #[error("node {node_id} is not serving (HTTP {status}): {reason:?}")]
    Serving {
        node_id: u64,
        status: u16,
        reason: Option<ursula_proto::admin::ServingReadinessReason>,
    },
    #[error(
        "cluster inventory is incomplete: {observed_nodes}/{expected_nodes} nodes, expecting {expected_groups} initialized groups with leaders per node"
    )]
    Inventory {
        expected_nodes: usize,
        observed_nodes: usize,
        expected_groups: usize,
    },
}

/// Check existing metrics, without requiring a newer admin endpoint.
pub fn check_maintenance_snapshot(
    snapshot: &ClusterSnapshot,
    expected_nodes: usize,
    expected_groups: usize,
) -> std::result::Result<(), ReadinessRefusal> {
    for view in &snapshot.per_node {
        if view.maintenance_fence_uncertain {
            return Err(ReadinessRefusal::FenceUncertain {
                node_id: view.node.id,
            });
        }
        if !view
            .raft_maintenance
            .as_ref()
            .is_some_and(|report| report.node_id == view.node.id && report.ready())
        {
            return Err(ReadinessRefusal::Maintenance {
                node_id: view.node.id,
                report: view.raft_maintenance.clone(),
            });
        }
    }
    let mut detail = String::new();
    if !cluster_ready(snapshot, expected_nodes, expected_groups, &mut detail) {
        return Err(ReadinessRefusal::Inventory {
            expected_nodes,
            observed_nodes: snapshot.per_node.len(),
            expected_groups,
        });
    }
    Ok(())
}

/// Wait for complete metrics evidence and the existing local serving probe.
/// Transport, decoding, and identity errors retain their original source.
pub async fn wait_ready(
    client: &MetricsClient,
    nodes: &[NodeInfo],
    expected_groups: usize,
    timeout: Duration,
    poll_interval: Duration,
) -> Result<ClusterSnapshot> {
    let started = Instant::now();
    loop {
        let snapshot = client.fetch_cluster(nodes).await?;
        let mut refusal = check_maintenance_snapshot(&snapshot, nodes.len(), expected_groups).err();
        if refusal.is_none() {
            for node in nodes {
                let (status, report) = client.serving_readiness(node).await?;
                if status != reqwest::StatusCode::OK || !report.ready {
                    refusal = Some(ReadinessRefusal::Serving {
                        node_id: node.id,
                        status: status.as_u16(),
                        reason: report.reason,
                    });
                    break;
                }
            }
        }
        let Some(refusal) = refusal else {
            return Ok(snapshot);
        };
        if started.elapsed() >= timeout {
            return Err(refusal.into());
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
        let uninitialized_groups = view
            .groups
            .iter()
            .filter(|g| !group_is_initialized(g))
            .count();
        if uninitialized_groups > 0 {
            *summary = format!(
                "node {} has {uninitialized_groups} uninitialized group(s)",
                view.node.id
            );
            return false;
        }
        let groups_without_leader = view
            .groups
            .iter()
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
        "all {expected_nodes} nodes report {expected_groups} initialized groups with leaders"
    );
    true
}

fn group_is_initialized(group: &crate::metrics::RaftGroupView) -> bool {
    !group.voter_ids.is_empty()
}

#[cfg(test)]
mod tests {
    use url::Url;

    use super::*;
    use crate::metrics::RaftGroupView;

    #[tokio::test]
    async fn wait_ready_uses_existing_probes_and_preserves_refusal_causes() {
        use axum::response::IntoResponse;
        use ursula_proto::admin::ServingReadinessReason;
        for mode in 0..7 {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let mut metrics = crate::metrics::test_metrics(
                1,
                Some(ursula_proto::admin::ProcessIncarnation::from_bits(1)),
            );
            let mut replica = group(0, Some(1));
            replica.node_id = 1;
            metrics.groups.push(replica);
            metrics.raft_maintenance =
                Some(crate::metrics::test_maintenance_report(1, &metrics.groups));
            if mode == 1 {
                metrics.maintenance_fence_uncertain = true;
            }
            if mode == 2 {
                metrics
                    .raft_maintenance
                    .as_mut()
                    .unwrap()
                    .group_issues
                    .insert(0, vec![
                        ursula_proto::admin::RaftMaintenanceIssue::IncompleteVoterSet,
                    ]);
            }
            let app = axum::Router::new()
                .route(
                    "/__ursula/metrics",
                    axum::routing::get(move || {
                        let metrics = metrics.clone();
                        async move { axum::Json(metrics) }
                    }),
                )
                .route(
                    "/__ursula/ready",
                    axum::routing::get(move || async move {
                        if mode == 4 {
                            return axum::http::StatusCode::NOT_FOUND.into_response();
                        }
                        if mode == 5 {
                            return (axum::http::StatusCode::OK, "not json").into_response();
                        }
                        let ready = mode != 3;
                        (
                            if ready && mode != 6 {
                                axum::http::StatusCode::OK
                            } else {
                                axum::http::StatusCode::SERVICE_UNAVAILABLE
                            },
                            axum::Json(ursula_proto::admin::ServingReadiness {
                                ready,
                                reason: (!ready).then_some(ServingReadinessReason::WalDiskPressure),
                                format_epoch_mismatch: false,
                                recovery_barriers_ready: true,
                                raft_maintenance: None,
                                recovery_stalled_groups: vec![],
                                wal_disk_pressure: !ready,
                                wal_available_bytes: 0,
                                wal_min_available_bytes: 0,
                                wal_resume_available_bytes: 0,
                                wal_disk_stat_errors: 0,
                            }),
                        )
                            .into_response()
                    }),
                );
            // No /maintenance/ready route: this is the existing 0.7 endpoint set.
            let task = tokio::spawn(async move {
                axum::serve(listener, app).await.unwrap();
            });
            let mut node = node(1);
            node.http_url = None;
            node.admin_url = format!("http://{address}").parse().unwrap();
            let result = wait_ready(
                &MetricsClient::new(Duration::from_secs(1)).unwrap(),
                &[node],
                1,
                Duration::ZERO,
                Duration::from_millis(1),
            )
            .await;
            if mode == 0 {
                assert_eq!(result.unwrap().per_node.len(), 1);
            } else {
                let error = result.unwrap_err();
                match mode {
                    1 => assert!(matches!(
                        error.downcast_ref::<ReadinessRefusal>(),
                        Some(ReadinessRefusal::FenceUncertain { node_id: 1 })
                    )),
                    2 => assert!(matches!(
                        error.downcast_ref::<ReadinessRefusal>(),
                        Some(ReadinessRefusal::Maintenance { node_id: 1, .. })
                    )),
                    3 => assert!(matches!(
                        error.downcast_ref::<ReadinessRefusal>(),
                        Some(ReadinessRefusal::Serving {
                            node_id: 1,
                            reason: Some(ServingReadinessReason::WalDiskPressure),
                            status: 503,
                        })
                    )),
                    4 => assert_eq!(
                        error.downcast_ref::<reqwest::Error>().unwrap().status(),
                        Some(reqwest::StatusCode::NOT_FOUND)
                    ),
                    5 => assert!(error.downcast_ref::<reqwest::Error>().unwrap().is_decode()),
                    _ => assert!(matches!(
                        error.downcast_ref::<ReadinessRefusal>(),
                        Some(ReadinessRefusal::Serving {
                            node_id: 1,
                            status: 503,
                            reason: None
                        })
                    )),
                }
            }
            task.abort();
        }
    }

    fn node(id: u64) -> NodeInfo {
        NodeInfo {
            expected_process_incarnation: None,
            expected_maintenance_fence: None,
            id,
            admin_url: Url::parse(&format!("http://10.0.0.{id}:4438")).unwrap(),
            host: format!("10.0.0.{id}"),
            http_url: Some(Url::parse(&format!("http://10.0.0.{id}/")).unwrap()),
            metrics_url: None,
        }
    }

    fn group(raft_group_id: u64, leader: Option<u64>) -> RaftGroupView {
        crate::metrics::test_group(raft_group_id, 0, 1, leader, Some(1), Some(1), vec![1, 2, 3])
    }

    fn empty_group(raft_group_id: u64) -> RaftGroupView {
        crate::metrics::test_group(raft_group_id, 0, 0, None, None, None, vec![])
    }

    #[test]
    fn cluster_ready_requires_every_node_and_leader_per_group() {
        let snapshot = ClusterSnapshot {
            per_node: vec![
                crate::metrics::test_view(node(1), vec![group(7, Some(1)), group(8, Some(2))]),
                crate::metrics::test_view(node(2), vec![group(7, Some(1)), group(8, Some(2))]),
            ],
        };
        let mut summary = String::new();
        assert!(cluster_ready(&snapshot, 2, 2, &mut summary));
    }

    #[test]
    fn cluster_ready_false_when_group_lacks_leader() {
        let snapshot = ClusterSnapshot {
            per_node: vec![crate::metrics::test_view(node(1), vec![group(7, None)])],
        };
        let mut summary = String::new();
        assert!(!cluster_ready(&snapshot, 1, 1, &mut summary));
        assert!(summary.contains("without a leader"));
    }

    #[test]
    fn cluster_ready_false_when_group_is_uninitialized() {
        let snapshot = ClusterSnapshot {
            per_node: vec![crate::metrics::test_view(node(1), vec![
                group(7, Some(1)),
                empty_group(8),
            ])],
        };
        let mut summary = String::new();
        assert!(!cluster_ready(&snapshot, 1, 2, &mut summary));
        assert!(summary.contains("uninitialized"));
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

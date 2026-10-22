//! Read-only verbs over `/__ursula/metrics`, `/__ursula/ready` and the
//! meta leader's placement view: `status` and `wait-ready`.

use std::collections::BTreeMap;
use std::io::Write;
use std::time::Duration;
use std::time::Instant;

use anyhow::Result;

use crate::metrics::ClusterSnapshot;
use crate::metrics::MetricsClient;
use crate::provider::NodeInfo;
use crate::view::GroupRefusal;
use crate::view::MetaLeaderView;
use crate::view::ViewRefusal;

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

/// A refusal retains the report, serving reason or placement check that
/// prevents maintenance.
#[derive(Debug, thiserror::Error)]
pub enum ReadinessRefusal {
    #[error("node {node_id} lacks complete maintenance evidence: {report:?}")]
    Maintenance {
        node_id: u64,
        report: Option<Box<ursula_proto::admin::RaftMaintenanceReport>>,
    },
    #[error("node {node_id} is not serving (HTTP {status}): {reason:?}")]
    Serving {
        node_id: u64,
        status: u16,
        reason: Option<ursula_proto::admin::ServingReadinessReason>,
    },
    #[error(transparent)]
    View(#[from] ViewRefusal),
    #[error(transparent)]
    Group(#[from] GroupRefusal),
}

/// Checks that every node of `snapshot` gave its own maintenance report and
/// that the report is ready.
pub fn check_maintenance_snapshot(
    snapshot: &ClusterSnapshot,
) -> std::result::Result<(), ReadinessRefusal> {
    for view in &snapshot.per_node {
        if !view
            .raft_maintenance
            .as_ref()
            .is_some_and(|report| report.node_id == view.node.id && report.ready())
        {
            return Err(ReadinessRefusal::Maintenance {
                node_id: view.node.id,
                report: view.raft_maintenance.clone().map(Box::new),
            });
        }
    }
    Ok(())
}

/// Wait until every node's maintenance report is ready and it serves, the
/// meta leader's view shows the cluster bootstrapped with every group live
/// and no move, and every group runs on exactly its target voters with a
/// leader among them ([`crate::view::check_groups`]). Transport failures
/// and refusals are retried until the deadline. Decoding failures are
/// terminal. Every error retains its original source.
///
/// Each poll observes the processes afresh with `client`'s HTTP settings, so
/// a node may restart during the wait.
pub async fn wait_ready(
    client: &MetricsClient,
    nodes: &[NodeInfo],
    timeout: Duration,
    poll_interval: Duration,
) -> Result<(ClusterSnapshot, MetaLeaderView)> {
    let started = Instant::now();
    loop {
        let poll = MetricsClient::new(client.timeout())?;
        let observation: Result<(ClusterSnapshot, MetaLeaderView)> = async {
            let snapshot = poll.fetch_cluster(nodes).await?;
            check_maintenance_snapshot(&snapshot)?;
            for node in nodes {
                let (status, report) = poll.serving_readiness(node).await?;
                if status != reqwest::StatusCode::OK || !report.ready {
                    return Err(ReadinessRefusal::Serving {
                        node_id: node.id,
                        status: status.as_u16(),
                        reason: report.reason,
                    }
                    .into());
                }
            }
            let view = crate::view::read_view(&poll, nodes)
                .await
                .map_err(ReadinessRefusal::from)?;
            let targets = view.targets().map_err(ReadinessRefusal::from)?;
            crate::view::check_groups(&targets, &snapshot, None).map_err(ReadinessRefusal::from)?;
            Ok((snapshot, view))
        }
        .await;
        match observation {
            Ok(ready) => return Ok(ready),
            Err(error) => {
                let retryable = error.downcast_ref::<ReadinessRefusal>().is_some()
                    || error
                        .downcast_ref::<reqwest::Error>()
                        .is_some_and(|source| {
                            source.is_connect()
                                || source.is_timeout()
                                || source.is_body()
                                || (source.is_request() && !source.is_builder())
                        });
                if !retryable || started.elapsed() >= timeout {
                    return Err(error);
                }
            }
        }
        tokio::time::sleep(poll_interval.min(timeout.saturating_sub(started.elapsed()))).await;
    }
}

fn group_is_initialized(group: &crate::metrics::RaftGroupView) -> bool {
    !group.voter_ids.is_empty()
}

#[cfg(test)]
mod tests {
    use url::Url;

    use super::*;
    use crate::metrics::RaftGroupView;

    /// A loopback port nothing listens on: connects to it are refused while
    /// this lives. The port is the local end of an open connection, so
    /// `bind(0)` never returns it and nothing listens on it. A socket that is only bound
    /// would hold the port too, but macOS drops a connect to it instead of
    /// refusing it, so the connect waits for its timeout.
    struct RefusingPort {
        url: Url,
        _connection: (tokio::net::TcpStream, tokio::net::TcpStream),
    }

    impl RefusingPort {
        async fn hold() -> Self {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let socket = tokio::net::TcpSocket::new_v4().unwrap();
            socket.bind("127.0.0.1:0".parse().unwrap()).unwrap();
            let connection = socket
                .connect(listener.local_addr().unwrap())
                .await
                .unwrap();
            let (accepted, _peer) = listener.accept().await.unwrap();
            Self {
                url: format!("http://{}", connection.local_addr().unwrap())
                    .parse()
                    .unwrap(),
                _connection: (connection, accepted),
            }
        }
    }

    #[tokio::test]
    async fn wait_ready_uses_existing_probes_and_preserves_refusal_causes() {
        use axum::response::IntoResponse;
        use ursula_proto::admin::ServingReadinessReason;
        for mode in [0, 2, 3, 4, 5, 6, 7, 8] {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let mut metrics =
                crate::metrics::test_metrics(1, Some(crate::metrics::test_incarnation(1)));
            let mut replica = group(0, Some(1));
            replica.node_id = 1;
            metrics.groups.push(replica);
            metrics.raft_maintenance =
                Some(crate::metrics::test_maintenance_report(1, &metrics.groups));
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
                                cluster_id: ursula_headers::ClusterId::new("test").unwrap(),
                                peer_cluster_mismatches: vec![],
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
            node.http_url = Some(format!("http://{address}").parse().unwrap());
            node.admin_url = format!("http://{address}").parse().unwrap();
            let unavailable = if mode >= 7 {
                Some(RefusingPort::hold().await)
            } else {
                None
            };
            if let Some(unavailable) = &unavailable {
                let url = unavailable.url.clone();
                if mode == 7 {
                    node.metrics_url = Some(url);
                } else {
                    node.client_url = Some(url);
                }
            }
            let timeout = if mode >= 7 {
                Duration::from_millis(40)
            } else {
                Duration::ZERO
            };
            let started = Instant::now();
            let result = wait_ready(
                &MetricsClient::new(Duration::from_secs(1)).unwrap(),
                &[node],
                timeout,
                Duration::from_millis(1),
            )
            .await;
            if mode == 0 {
                // A ready, serving node that answers no placement status
                // names no meta leader.
                assert!(matches!(
                    result.unwrap_err().downcast_ref::<ReadinessRefusal>(),
                    Some(ReadinessRefusal::View(ViewRefusal::NoMetaLeader))
                ));
            } else {
                let error = result.unwrap_err();
                match mode {
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
                    7 | 8 => {
                        assert!(error.downcast_ref::<reqwest::Error>().unwrap().is_connect());
                        assert!(started.elapsed() >= timeout);
                    }
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

    #[tokio::test]
    async fn wait_ready_follows_a_restart() {
        use std::sync::Arc;
        use std::sync::atomic::AtomicU64;
        use std::sync::atomic::Ordering;

        use ursula_proto::admin::RaftMaintenanceIssue;

        let polls = Arc::new(AtomicU64::new(0));
        let metrics_polls = Arc::clone(&polls);
        let app = axum::Router::new()
            .route(
                "/__ursula/metrics",
                axum::routing::get(move || {
                    // The first process still lags. Its replacement is ready.
                    let poll = metrics_polls.fetch_add(1, Ordering::SeqCst);
                    let incarnation = if poll == 0 { 1 } else { 2 };
                    let mut metrics = crate::metrics::test_metrics(
                        1,
                        Some(crate::metrics::test_incarnation(incarnation)),
                    );
                    let mut replica = group(0, Some(1));
                    replica.node_id = 1;
                    metrics.groups.push(replica);
                    let mut report = crate::metrics::test_maintenance_report(1, &metrics.groups);
                    if poll == 0 {
                        report
                            .group_issues
                            .insert(0, vec![RaftMaintenanceIssue::ApplyLag]);
                    }
                    metrics.raft_maintenance = Some(report);
                    async move { axum::Json(metrics) }
                }),
            )
            .route(
                "/__ursula/ready",
                axum::routing::get(|| async {
                    axum::Json(ursula_proto::admin::ServingReadiness {
                        ready: true,
                        reason: None,
                        cluster_id: ursula_headers::ClusterId::new("test").unwrap(),
                        peer_cluster_mismatches: vec![],
                        format_epoch_mismatch: false,
                        recovery_barriers_ready: true,
                        raft_maintenance: None,
                        recovery_stalled_groups: vec![],
                        wal_disk_pressure: false,
                        wal_available_bytes: 0,
                        wal_min_available_bytes: 0,
                        wal_resume_available_bytes: 0,
                        wal_disk_stat_errors: 0,
                    })
                }),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base: Url = format!("http://{}", listener.local_addr().unwrap())
            .parse()
            .unwrap();
        let task = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let mut node = node(1);
        node.http_url = Some(base.clone());
        node.admin_url = base;
        let client = MetricsClient::new(Duration::from_secs(1)).unwrap();
        let timeout = Duration::from_millis(300);

        // The replacement passes the maintenance and serving checks, so the
        // wait gets as far as the placement view, which this node does not
        // serve: no poll was refused for the changed incarnation.
        let refusal = wait_ready(
            &client,
            std::slice::from_ref(&node),
            timeout,
            Duration::from_millis(1),
        )
        .await
        .unwrap_err();
        assert!(matches!(
            refusal.downcast_ref::<ReadinessRefusal>(),
            Some(ReadinessRefusal::View(ViewRefusal::NoMetaLeader))
        ));
        assert!(polls.load(Ordering::SeqCst) >= 2);

        task.abort();
    }

    fn node(id: u64) -> NodeInfo {
        NodeInfo {
            id,
            admin_url: Url::parse(&format!("http://10.0.0.{id}:4438")).unwrap(),
            host: format!("10.0.0.{id}"),
            http_url: Some(Url::parse(&format!("http://10.0.0.{id}/")).unwrap()),
            metrics_url: None,
            client_url: None,
        }
    }

    fn group(raft_group_id: u64, leader: Option<u64>) -> RaftGroupView {
        crate::metrics::test_group(raft_group_id, 0, 1, leader, Some(1), Some(1), vec![1, 2, 3])
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

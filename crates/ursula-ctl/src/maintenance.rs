//! Node maintenance verbs: drain, undrain, and catch-up wait.
//!
//! These operate purely on Ursula's admin/metrics HTTP surface and never
//! execute anything on a host. Physical lifecycle (stopping and starting the
//! process) belongs to the platform that owns it: Kubernetes and Helm for pod
//! clusters, systemd for bare-metal hosts. A safe rolling restart runs these
//! verbs around the platform's own restart, one node at a time: drain the
//! node, restart it, wait until it is a caught-up voter again, undrain it. A
//! node that lost entries it had acknowledged (a host crash under
//! `raft.wal.fsync = never`, or a lost disk) is gated and rebuilt by its
//! groups' leaders; waiting covers that rebuild too.

use std::collections::BTreeMap;
use std::time::Duration;
use std::time::Instant;

use anyhow::Context;
use anyhow::Result;
use anyhow::bail;

use crate::metrics::MetricsClient;
use crate::plan::DrainPlan;
use crate::plan::check_readiness;
use crate::plan::plan_drain_at_barriers;
use crate::provider::NodeInfo;

/// A timeout measured from its start. Comparing elapsed time cannot overflow
/// the way `Instant::now() + timeout` does for an operator-supplied timeout.
#[derive(Debug, Clone, Copy)]
struct Deadline {
    started: Instant,
    timeout: Duration,
}

impl Deadline {
    fn after(timeout: Duration) -> Self {
        Self {
            started: Instant::now(),
            timeout,
        }
    }

    fn reached_at(&self, now: Instant) -> bool {
        now.saturating_duration_since(self.started) >= self.timeout
    }

    fn is_reached(&self) -> bool {
        self.reached_at(Instant::now())
    }
}

/// Knobs for [`drain_node`].
#[derive(Debug, Clone)]
pub struct DrainOptions {
    /// How long the target may keep leading groups before the drain aborts.
    pub drain_timeout: Duration,
    /// Budget for the surrounding whole-cluster readiness waits.
    pub ready_timeout: Duration,
    pub poll_interval: Duration,
    pub lag_tolerance: u64,
    /// Compute and return the transfer plan without mutating anything.
    pub dry_run: bool,
}

impl Default for DrainOptions {
    fn default() -> Self {
        Self {
            drain_timeout: Duration::from_secs(60),
            ready_timeout: Duration::from_secs(120),
            poll_interval: Duration::from_secs(2),
            lag_tolerance: 16,
            dry_run: false,
        }
    }
}

#[derive(Debug, Clone)]
pub enum DrainOutcome {
    /// The target leads zero groups and its drain mark is still set. Callers
    /// clear it with [`undrain_node`] once the maintenance window is over.
    Drained,
    /// Dry run: the transfer plan that a real drain would start from.
    DryRun(DrainPlan),
    Aborted {
        reason: String,
    },
}

/// Mark `target` as draining and transfer away every leadership it holds.
///
/// On success the maintenance-drain mark is intentionally left set so the node
/// does not re-acquire leaderships while it is being restarted or serviced.
/// Clear it with [`undrain_node`]. On failure the mark is restored to clear.
pub async fn drain_node(
    nodes: &[NodeInfo],
    target: &NodeInfo,
    client: &MetricsClient,
    options: &DrainOptions,
) -> Result<DrainOutcome> {
    if !options.dry_run {
        wait_cluster_ready(
            "pre-flight cluster readiness",
            nodes,
            client,
            options.ready_timeout,
            options.poll_interval,
            options.lag_tolerance,
        )
        .await?;
        client
            .set_maintenance_drain(target, true)
            .await
            .with_context(|| format!("mark maintenance-drain on node {}", target.id))?;
    }

    let snapshot = match client.fetch_cluster(nodes).await {
        Ok(snapshot) => snapshot,
        Err(err) => {
            clear_maintenance_drain(client, target).await;
            return Err(err).context("pre-flight metrics");
        }
    };
    let mut barriers = snapshot
        .groups_led_by(target.id)
        .into_iter()
        .filter_map(|group| {
            group
                .committed_index
                .map(|index| (group.raft_group_id, index))
        })
        .collect::<BTreeMap<_, _>>();
    let plan = plan_drain_at_barriers(&snapshot, target.id, &barriers);
    tracing::info!(
        "drain plan computed: target_node_id={} led_groups={}",
        target.id,
        plan.transfers.len()
    );
    if options.dry_run {
        return Ok(DrainOutcome::DryRun(plan));
    }

    let deadline = Deadline::after(options.drain_timeout);
    loop {
        let snap = match client.fetch_cluster(nodes).await {
            Ok(snap) => snap,
            Err(err) => {
                clear_maintenance_drain(client, target).await;
                return Err(err).context("drain poll");
            }
        };
        let still_leads = snap.groups_reported_led_by(target.id);
        if still_leads.is_empty() {
            if let Err(err) = wait_cluster_ready(
                "post-drain cluster readiness",
                nodes,
                client,
                options.ready_timeout,
                options.poll_interval,
                options.lag_tolerance,
            )
            .await
            {
                clear_maintenance_drain(client, target).await;
                return Err(err);
            }
            return Ok(DrainOutcome::Drained);
        }
        for group in snap.groups_led_by(target.id) {
            if let Some(index) = group.committed_index {
                barriers.entry(group.raft_group_id).or_insert(index);
            }
        }
        let plan = plan_drain_at_barriers(&snap, target.id, &barriers);
        if plan.transfers.is_empty() {
            if !deadline.is_reached() {
                tokio::time::sleep(options.poll_interval).await;
                continue;
            }
            clear_maintenance_drain(client, target).await;
            return Ok(DrainOutcome::Aborted {
                reason: format!(
                    "target still leads {} group(s), but no safe transfer target is available",
                    still_leads.len()
                ),
            });
        }
        if let Err(err) = transfer_drain_plan(target, client, &plan).await {
            clear_maintenance_drain(client, target).await;
            return Err(err);
        }
        if deadline.is_reached() {
            clear_maintenance_drain(client, target).await;
            return Ok(DrainOutcome::Aborted {
                reason: format!(
                    "drain timeout: target still leads {} group(s) after {:?}",
                    still_leads.len(),
                    options.drain_timeout
                ),
            });
        }
        tokio::time::sleep(options.poll_interval).await;
    }
}

/// Clear the maintenance-drain mark on `target` so it may hold leaderships
/// again.
pub async fn undrain_node(client: &MetricsClient, target: &NodeInfo) -> Result<()> {
    client
        .set_maintenance_drain(target, false)
        .await
        .with_context(|| format!("clear maintenance-drain on node {}", target.id))
}

/// Best-effort mark clearing for error paths where the primary error must win.
pub(crate) async fn clear_maintenance_drain(client: &MetricsClient, target: &NodeInfo) {
    if let Err(err) = undrain_node(client, target).await {
        tracing::warn!(
            "failed to clear maintenance-drain: target_node_id={} error={err}",
            target.id
        );
    }
}

/// Knobs for [`wait_node_ready`].
#[derive(Debug, Clone)]
pub struct CatchUpOptions {
    /// Abort when the target makes no catch-up progress (no new applied
    /// entries, no new voter memberships) for this long. This is the real
    /// control: a rebuild that keeps advancing is never timed out.
    pub stall_timeout: Duration,
    /// Absolute backstop above the stall detector.
    pub ready_timeout: Duration,
    pub poll_interval: Duration,
    pub lag_tolerance: u64,
}

impl Default for CatchUpOptions {
    fn default() -> Self {
        Self {
            stall_timeout: Duration::from_secs(90),
            ready_timeout: Duration::from_secs(1800),
            poll_interval: Duration::from_secs(2),
            lag_tolerance: 16,
        }
    }
}

#[derive(Debug, Clone)]
pub enum CatchUpOutcome {
    Ready,
    Stalled { reason: String },
}

/// Wait until `target` is back as a voter in every group and its applied index
/// is within `lag_tolerance` of peers' committed index. Progress-gated, not a
/// fixed timeout: any forward motion resets the stall clock.
pub async fn wait_node_ready(
    nodes: &[NodeInfo],
    target: &NodeInfo,
    client: &MetricsClient,
    options: &CatchUpOptions,
) -> Result<CatchUpOutcome> {
    let ceiling = Deadline::after(options.ready_timeout);
    let mut best = TargetProgress::default();
    let mut last_advance = Instant::now();
    loop {
        let snap = client.try_fetch_cluster(nodes).await;
        let report = check_readiness(&snap, target.id, options.lag_tolerance);
        if report.all_ready {
            return Ok(CatchUpOutcome::Ready);
        }

        let now = Instant::now();
        let current = TargetProgress::of(&report);
        if current.advanced_past(&best) {
            best = current;
            last_advance = now;
        }

        let stalled = now.duration_since(last_advance) >= options.stall_timeout;
        let hit_ceiling = ceiling.reached_at(now);
        if stalled || hit_ceiling {
            let cause = if hit_ceiling {
                format!(
                    "readiness backstop reached after {:?}",
                    options.ready_timeout
                )
            } else {
                format!("no catch-up progress for {:?}", options.stall_timeout)
            };
            let mut reason = format!("{cause}: {}", format_unready(&report));
            if let Some(hint) = missing_target_timeout_hint(&report) {
                reason.push_str("; ");
                reason.push_str(hint);
            }
            return Ok(CatchUpOutcome::Stalled { reason });
        }
        tokio::time::sleep(options.poll_interval).await;
    }
}

/// Wait until every node in the cluster is a voter everywhere it should be and
/// caught up, sampled twice to avoid acting on a transient view.
pub async fn wait_cluster_ready(
    phase: &str,
    nodes: &[NodeInfo],
    client: &MetricsClient,
    timeout: Duration,
    poll_interval: Duration,
    lag_tolerance: u64,
) -> Result<()> {
    let deadline = Deadline::after(timeout);
    let mut ready_streak = 0usize;
    loop {
        let snap = client.try_fetch_cluster(nodes).await;
        let mut unready = Vec::new();
        for node in nodes {
            let report = check_readiness(&snap, node.id, lag_tolerance);
            if !report.all_ready {
                unready.push(format!("node {}: {}", node.id, format_unready(&report)));
            }
        }
        if unready.is_empty() {
            ready_streak = ready_streak.saturating_add(1);
            if ready_streak >= 2 {
                tracing::info!("{phase}: ready");
                return Ok(());
            }
            tracing::debug!("{phase}: ready sample {ready_streak}/2");
        } else {
            ready_streak = 0;
            tracing::debug!("{phase}: not ready: {}", unready.join("; "));
        }
        if deadline.is_reached() {
            let diagnostic = if unready.is_empty() {
                format!("cluster was ready for {ready_streak}/2 required consecutive sample(s)")
            } else {
                unready.join("; ")
            };
            bail!("{phase} timeout after {timeout:?}: {diagnostic}");
        }
        tokio::time::sleep(poll_interval).await;
    }
}

/// A monotonic snapshot of how far a restarting target has caught up. Applied
/// indices from already-ready groups are deliberately excluded: unrelated
/// writes there must not keep a wholly missing group alive forever.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct TargetProgress {
    ready_groups: usize,
    voters_ready: usize,
    unready_applied_sum: u128,
}

impl TargetProgress {
    fn of(report: &crate::plan::ReadinessReport) -> Self {
        let mut p = TargetProgress::default();
        for g in report.per_group.values() {
            if g.ready {
                p.ready_groups = p.ready_groups.saturating_add(1);
            } else {
                p.unready_applied_sum = p
                    .unready_applied_sum
                    .saturating_add(u128::from(g.target_applied_index.unwrap_or(0)));
            }
            if g.voter_member {
                p.voters_ready = p.voters_ready.saturating_add(1);
            }
        }
        p
    }

    /// Compare lexicographically so a readiness or membership regression can
    /// never be disguised as progress by writes in some other group.
    fn advanced_past(&self, prev: &TargetProgress) -> bool {
        (
            self.ready_groups,
            self.voters_ready,
            self.unready_applied_sum,
        ) > (
            prev.ready_groups,
            prev.voters_ready,
            prev.unready_applied_sum,
        )
    }
}

/// A target that reports no applied entries in any group after the readiness
/// window either never attached as a learner or never came back up; plain gap
/// numbers do not tell an operator that.
fn missing_target_timeout_hint(report: &crate::plan::ReadinessReport) -> Option<&'static str> {
    let all_unapplied = !report.per_group.is_empty()
        && report
            .per_group
            .values()
            .all(|g| g.target_applied_index.is_none());
    if !all_unapplied {
        return None;
    }
    Some(
        "target reports no applied entries in any group; check that the \
         replacement process started and that it can reach its groups' \
         leaders, which rebuild a replica that lost its log",
    )
}

async fn transfer_drain_plan(
    target: &NodeInfo,
    client: &MetricsClient,
    plan: &DrainPlan,
) -> Result<()> {
    for transfer in &plan.transfers {
        tracing::info!(
            "transferring leadership: target_node_id={} raft_group_id={} to={}",
            target.id,
            transfer.raft_group_id,
            transfer.preferred_successor
        );
        let resp = client
            .transfer_leader(target, transfer.raft_group_id, transfer.preferred_successor)
            .await?;
        if !resp.transferred {
            if resp.rejection.is_some_and(|reason| reason.should_replan()) {
                // The next drain iteration re-observes leadership and eligibility.
                return Ok(());
            }
            bail!(
                "leader transfer rejected for group {}: {}",
                transfer.raft_group_id,
                resp.reason.unwrap_or_else(|| "unknown".into())
            );
        }
    }
    Ok(())
}

pub(crate) fn format_unready(report: &crate::plan::ReadinessReport) -> String {
    let mut parts = report.maintenance_issues.clone();
    for (id, g) in &report.per_group {
        if !g.ready {
            parts.push(format!(
                "group {id}: voter={} applied={:?} peer_committed={:?} gap={:?}",
                g.voter_member, g.target_applied_index, g.peer_max_committed_index, g.catch_up_gap,
            ));
        }
    }
    if parts.is_empty() {
        "no groups observed".into()
    } else {
        parts.join("; ")
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::Ordering;

    use axum::Json;
    use axum::Router;
    use axum::extract::Path;
    use axum::extract::State;
    use axum::http::StatusCode;
    use axum::routing::get;
    use axum::routing::post;
    use serde_json::json;

    use super::*;
    use crate::metrics::ClusterSnapshot;
    use crate::metrics::NodeMetricsView;
    use crate::metrics::RaftGroupView;
    use crate::provider::NodeInfo;

    /// Three nodes that each lead nothing but group 7's leader, node 2.
    struct MockCluster {
        recovery_gate_ready: AtomicBool,
        drained_nodes: Mutex<Vec<u64>>,
        operations: Mutex<Vec<String>>,
    }

    #[derive(Clone)]
    struct MockNode {
        node_id: u64,
        cluster: Arc<MockCluster>,
    }

    async fn mock_metrics(State(state): State<MockNode>) -> Json<serde_json::Value> {
        Json(json!({
            "process_node_id": state.node_id,
            "process_incarnation": ursula_proto::admin::ProcessIncarnation::from_bits(u128::from(state.node_id)),
            "raft_groups": [{
                "raft_group_id": 7,
                "node_id": state.node_id,
                "current_term": 1,
                "current_leader": 2,
                "committed_index": 100,
                "last_applied_index": 100,
                "voter_ids": [1, 2, 3],
                "learner_ids": [],
                "maintenance": {
                    "running": true,
                    "recovery_ready": state.cluster.recovery_gate_ready.load(Ordering::SeqCst),
                    "accepting_transfers": true,
                    "membership_joint": false,
                    "membership_log_index": 0,
                    "stopped_for_operator": false
                }
            }]
        }))
    }

    async fn mock_drain(State(state): State<MockNode>) -> StatusCode {
        state
            .cluster
            .drained_nodes
            .lock()
            .unwrap()
            .push(state.node_id);
        state
            .cluster
            .operations
            .lock()
            .unwrap()
            .push(format!("drain:{}", state.node_id));
        StatusCode::OK
    }

    async fn mock_undrain(State(state): State<MockNode>) -> StatusCode {
        state
            .cluster
            .operations
            .lock()
            .unwrap()
            .push(format!("undrain:{}", state.node_id));
        StatusCode::OK
    }

    async fn mock_transfer(
        State(state): State<MockNode>,
        Path((_group_id, to)): Path<(u64, u64)>,
    ) -> Json<serde_json::Value> {
        state
            .cluster
            .operations
            .lock()
            .unwrap()
            .push(format!("transfer:{}->{to}", state.node_id));
        Json(json!({
            "raft_group_id": 7,
            "from": state.node_id,
            "to": to,
            "current_leader": to,
            "transferred": true
        }))
    }

    async fn mock_cluster() -> (Vec<NodeInfo>, Arc<MockCluster>) {
        let cluster = Arc::new(MockCluster {
            recovery_gate_ready: AtomicBool::new(true),
            drained_nodes: Mutex::new(Vec::new()),
            operations: Mutex::new(Vec::new()),
        });
        let mut nodes = Vec::new();
        for node_id in 1..=3 {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let state = MockNode {
                node_id,
                cluster: Arc::clone(&cluster),
            };
            let app = Router::new()
                .route("/__ursula/metrics", get(mock_metrics))
                .route(
                    "/__ursula/leadership-shed/maintenance",
                    post(mock_drain).delete(mock_undrain),
                )
                .route(
                    "/__ursula/raft/{group_id}/leader/transfer/{to}",
                    post(mock_transfer),
                )
                .with_state(state);
            tokio::spawn(async move {
                axum::serve(listener, app).await.unwrap();
            });
            let url = url::Url::parse(&format!("http://{address}")).unwrap();
            nodes.push(NodeInfo {
                expected_process_incarnation: None,
                expected_maintenance_fence: None,
                id: node_id,
                admin_url: url.clone(),
                host: address.to_string(),
                http_url: Some(url),
                metrics_url: None,
            });
        }
        (nodes, cluster)
    }

    fn n(id: u64, host: &str) -> NodeInfo {
        NodeInfo {
            expected_process_incarnation: None,
            expected_maintenance_fence: None,
            id,
            admin_url: url::Url::parse(&format!("http://{host}:4438")).unwrap(),
            host: host.to_owned(),
            http_url: Some(url::Url::parse(&format!("http://{host}:8080")).unwrap()),
            metrics_url: None,
        }
    }

    #[tokio::test]
    async fn cluster_verification_timeout_explains_a_single_ready_sample() {
        let (nodes, _) = mock_cluster().await;
        let error = wait_cluster_ready(
            "strict cluster verification",
            &nodes,
            &MetricsClient::new(Duration::from_secs(1)).unwrap(),
            Duration::ZERO,
            Duration::from_millis(1),
            0,
        )
        .await
        .unwrap_err();

        assert!(
            error
                .to_string()
                .contains("cluster was ready for 1/2 required consecutive sample(s)"),
            "{error:#}"
        );
    }

    #[tokio::test]
    async fn drain_requires_recovery_proofs_before_any_maintenance_mutation() {
        let (nodes, cluster) = mock_cluster().await;
        cluster.recovery_gate_ready.store(false, Ordering::SeqCst);
        let error = drain_node(
            &nodes,
            &nodes[2],
            &MetricsClient::new(Duration::from_secs(1)).unwrap(),
            &DrainOptions {
                ready_timeout: Duration::ZERO,
                poll_interval: Duration::from_millis(1),
                ..Default::default()
            },
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("pre-flight cluster readiness"));
        assert!(cluster.drained_nodes.lock().unwrap().is_empty());
        assert!(cluster.operations.lock().unwrap().is_empty());
    }

    fn group(
        raft_group_id: u64,
        node_id: u64,
        current_leader: Option<u64>,
        applied: u64,
        committed: u64,
    ) -> RaftGroupView {
        RaftGroupView {
            raft_group_id,
            node_id,
            current_term: Some(1),
            current_leader,
            committed_index: Some(committed),
            last_applied_index: Some(applied),
            voter_ids: vec![1, 2, 3],
            learner_ids: vec![],
            maintenance: None,
        }
    }

    #[test]
    fn cluster_readiness_formats_each_unready_node() {
        let snapshot = ClusterSnapshot {
            per_node: vec![
                NodeMetricsView {
                    process_incarnation: ursula_proto::admin::ProcessIncarnation::from_bits(1),
                    maintenance_fence: None,
                    maintenance_fence_uncertain: false,
                    node: n(1, "10.0.0.1"),
                    groups: vec![group(7, 1, Some(1), 50, 50)],
                    raft_maintenance: None,
                },
                NodeMetricsView {
                    process_incarnation: ursula_proto::admin::ProcessIncarnation::from_bits(1),
                    maintenance_fence: None,
                    maintenance_fence_uncertain: false,
                    node: n(2, "10.0.0.2"),
                    groups: vec![group(7, 2, Some(1), 100, 100)],
                    raft_maintenance: None,
                },
                NodeMetricsView {
                    process_incarnation: ursula_proto::admin::ProcessIncarnation::from_bits(1),
                    maintenance_fence: None,
                    maintenance_fence_uncertain: false,
                    node: n(3, "10.0.0.3"),
                    groups: vec![group(7, 3, Some(1), 95, 100)],
                    raft_maintenance: None,
                },
            ],
        };

        let report = check_readiness(&snapshot, 1, 5);

        assert!(!report.all_ready);
        let formatted = format_unready(&report);
        assert!(formatted.contains("gap=Some(50)"), "{formatted}");
    }

    #[test]
    fn missing_target_timeout_hint_points_to_the_leaders_rebuild() {
        let snapshot = ClusterSnapshot {
            per_node: vec![NodeMetricsView {
                process_incarnation: ursula_proto::admin::ProcessIncarnation::from_bits(1),
                maintenance_fence: None,
                maintenance_fence_uncertain: false,
                node: n(2, "10.0.0.2"),
                groups: vec![group(7, 2, Some(2), 100, 100)],
                raft_maintenance: None,
            }],
        };
        let report = check_readiness(&snapshot, 1, 5);
        assert!(!report.all_ready);

        let hint = missing_target_timeout_hint(&report).expect("missing target hint");
        assert!(
            hint.contains("rebuild a replica that lost its log"),
            "{hint}"
        );
    }

    #[test]
    fn target_progress_advances_on_applied_or_voter_gain() {
        use std::collections::BTreeMap;

        use crate::plan::GroupReadiness;
        use crate::plan::ReadinessReport;

        let report = |voter: bool, applied: Option<u64>| {
            let mut per_group = BTreeMap::new();
            per_group.insert(7, GroupReadiness {
                raft_group_id: 7,
                voter_member: voter,
                target_applied_index: applied,
                peer_max_committed_index: Some(100),
                catch_up_gap: None,
                ready: false,
            });
            ReadinessReport {
                maintenance_issues: vec![],
                all_ready: false,
                per_group,
            }
        };

        let none = TargetProgress::of(&report(false, None));
        let voter = TargetProgress::of(&report(true, None));
        let applying = TargetProgress::of(&report(true, Some(50)));
        let more = TargetProgress::of(&report(true, Some(80)));

        assert!(voter.advanced_past(&none)); // rejoined voter set
        assert!(applying.advanced_past(&voter)); // applied index climbing
        assert!(more.advanced_past(&applying));
        assert!(!applying.advanced_past(&applying)); // no motion → stall clock keeps running
        assert!(!voter.advanced_past(&more)); // a regression is not progress
    }

    #[test]
    fn target_progress_ignores_writes_in_already_ready_groups() {
        use std::collections::BTreeMap;

        use crate::plan::GroupReadiness;
        use crate::plan::ReadinessReport;

        let report = |ready_applied: u64| {
            let mut per_group = BTreeMap::new();
            per_group.insert(7, GroupReadiness {
                raft_group_id: 7,
                voter_member: true,
                target_applied_index: Some(ready_applied),
                peer_max_committed_index: Some(ready_applied),
                catch_up_gap: Some(0),
                ready: true,
            });
            per_group.insert(8, GroupReadiness {
                raft_group_id: 8,
                voter_member: false,
                target_applied_index: None,
                peer_max_committed_index: Some(8),
                catch_up_gap: Some(8),
                ready: false,
            });
            ReadinessReport {
                maintenance_issues: vec![],
                all_ready: false,
                per_group,
            }
        };

        let before = TargetProgress::of(&report(10));
        let unrelated_write = TargetProgress::of(&report(11));
        assert!(!unrelated_write.advanced_past(&before));
    }

    #[test]
    fn missing_target_timeout_hint_absent_when_target_has_applied_entries() {
        let snapshot = ClusterSnapshot {
            per_node: vec![
                NodeMetricsView {
                    process_incarnation: ursula_proto::admin::ProcessIncarnation::from_bits(1),
                    maintenance_fence: None,
                    maintenance_fence_uncertain: false,
                    node: n(1, "10.0.0.1"),
                    groups: vec![group(7, 1, Some(2), 50, 50)],
                    raft_maintenance: None,
                },
                NodeMetricsView {
                    process_incarnation: ursula_proto::admin::ProcessIncarnation::from_bits(1),
                    maintenance_fence: None,
                    maintenance_fence_uncertain: false,
                    node: n(2, "10.0.0.2"),
                    groups: vec![group(7, 2, Some(2), 100, 100)],
                    raft_maintenance: None,
                },
            ],
        };
        let report = check_readiness(&snapshot, 1, 5);
        assert!(!report.all_ready);
        assert!(missing_target_timeout_hint(&report).is_none());
    }

    #[test]
    fn drain_uses_every_nodes_leader_reports() {
        let snapshot = ClusterSnapshot {
            per_node: vec![
                NodeMetricsView {
                    process_incarnation: ursula_proto::admin::ProcessIncarnation::from_bits(1),
                    maintenance_fence: None,
                    maintenance_fence_uncertain: false,
                    node: n(1, "10.0.0.1"),
                    groups: vec![group(7, 1, Some(2), 100, 100)],
                    raft_maintenance: None,
                },
                NodeMetricsView {
                    process_incarnation: ursula_proto::admin::ProcessIncarnation::from_bits(1),
                    maintenance_fence: None,
                    maintenance_fence_uncertain: false,
                    node: n(2, "10.0.0.2"),
                    groups: vec![group(7, 2, Some(2), 100, 100)],
                    raft_maintenance: None,
                },
                NodeMetricsView {
                    process_incarnation: ursula_proto::admin::ProcessIncarnation::from_bits(1),
                    maintenance_fence: None,
                    maintenance_fence_uncertain: false,
                    node: n(3, "10.0.0.3"),
                    groups: vec![group(7, 3, Some(1), 100, 100)],
                    raft_maintenance: None,
                },
            ],
        };

        assert!(snapshot.groups_led_by(1).is_empty());

        let still_led = snapshot.groups_reported_led_by(1);
        assert_eq!(still_led.len(), 1);
        assert_eq!(still_led[0].raft_group_id, 7);
    }
}

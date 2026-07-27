//! Node maintenance verbs: drain, undrain, catch-up wait, and empty-log
//! rejoin arming.
//!
//! These operate purely on Ursula's admin/metrics HTTP surface and never
//! execute anything on a host. Physical lifecycle (stopping and starting the
//! process) belongs to the platform that owns it: Kubernetes and Helm for pod
//! clusters, systemd for bare-metal hosts. A safe rolling restart runs these
//! verbs around the platform's own restart, one node at a time.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::time::Duration;
use std::time::Instant;

use anyhow::Context;
use anyhow::Result;
use anyhow::anyhow;
use anyhow::bail;
use futures_util::StreamExt;
use futures_util::TryStreamExt;

use crate::metrics::ClusterSnapshot;
use crate::metrics::MetricsClient;
use crate::plan::DrainPlan;
use crate::plan::check_readiness;
use crate::plan::plan_drain;
use crate::provider::NodeInfo;

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

/// Bounds for arming an empty-log rejoin immediately before a memory-WAL
/// process restart.
#[derive(Debug, Clone)]
pub struct RejoinOptions {
    /// Total budget for retrying transient leader changes.
    pub timeout: Duration,
    /// Maximum number of leader admin requests in flight.
    pub max_concurrency: usize,
    /// Delay before retrying a snapshot that observed leader movement.
    pub retry_interval: Duration,
}

impl Default for RejoinOptions {
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(30),
            max_concurrency: 32,
            retry_interval: Duration::from_millis(100),
        }
    }
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
    let plan = plan_drain(&snapshot, target.id);
    tracing::info!(
        "drain plan computed: target_node_id={} led_groups={}",
        target.id,
        plan.transfers.len()
    );
    if options.dry_run {
        return Ok(DrainOutcome::DryRun(plan));
    }

    let deadline = Instant::now() + options.drain_timeout;
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
        let plan = plan_drain(&snap, target.id);
        if plan.transfers.is_empty() {
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
        if Instant::now() >= deadline {
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
///
/// `empty_rejoin_armed` only affects the diagnostic hint attached to a stall
/// on a node that never reports an applied entry.
pub async fn wait_node_ready(
    nodes: &[NodeInfo],
    target: &NodeInfo,
    client: &MetricsClient,
    options: &CatchUpOptions,
    empty_rejoin_armed: bool,
) -> Result<CatchUpOutcome> {
    let ceiling = Instant::now() + options.ready_timeout;
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
        let hit_ceiling = now >= ceiling;
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
            if let Some(hint) = amnesiac_timeout_hint(&report, empty_rejoin_armed) {
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
    let deadline = Instant::now() + timeout;
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
        if Instant::now() >= deadline {
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

/// Ask every group's stable leader to accept one empty-log rejoin from
/// `target`, then prove that the same leaders still own those groups.
///
/// Permissions are leader-local OpenRaft state. A sequential 256-group loop
/// takes long enough for the leadership balancer to move some groups after
/// they were armed, silently invalidating the permission. Requests therefore
/// run concurrently and the leader map is sampled again before success. Any
/// movement retries the complete round so the platform can restart the target
/// immediately after this function returns.
pub async fn arm_empty_rejoin(
    nodes: &[NodeInfo],
    target: &NodeInfo,
    client: &MetricsClient,
    options: &RejoinOptions,
) -> Result<()> {
    if options.max_concurrency == 0 {
        bail!("rejoin max_concurrency must be positive");
    }
    let deadline = Instant::now() + options.timeout;
    let mut last_error: anyhow::Error;
    loop {
        let before = client.fetch_cluster(nodes).await?;
        let group_ids = peer_reported_rejoin_groups(&before, target.id);
        if group_ids.is_empty() {
            bail!(
                "no initialized raft group reported by a peer includes target node {}; \
                 cannot allow empty raft rejoin",
                target.id
            );
        }
        let plan = match rejoin_leader_plan(nodes, target.id, &before, &group_ids) {
            Ok(plan) => plan,
            Err(err) => {
                last_error = err;
                if Instant::now() >= deadline {
                    break;
                }
                tokio::time::sleep(options.retry_interval).await;
                continue;
            }
        };

        let result = futures_util::stream::iter(plan.iter().map(|(group_id, leader)| async move {
            tracing::debug!(
                "allowing empty raft rejoin: target_node_id={} raft_group_id={} leader_node_id={}",
                target.id,
                group_id,
                leader.id
            );
            client
                .allow_next_revert(leader, *group_id, target.id)
                .await
                .with_context(|| {
                    format!(
                        "allow target node {} to rejoin group {} through leader {}",
                        target.id, group_id, leader.id
                    )
                })
        }))
        .buffer_unordered(options.max_concurrency)
        .try_collect::<Vec<()>>()
        .await;
        if let Err(err) = result {
            last_error = err;
        } else {
            let after = client.fetch_cluster(nodes).await?;
            match rejoin_leader_plan(nodes, target.id, &after, &group_ids) {
                Ok(after_plan)
                    if plan.iter().all(|(group_id, leader)| {
                        after_plan
                            .get(group_id)
                            .is_some_and(|after| after.id == leader.id)
                    }) =>
                {
                    tracing::info!(
                        "empty raft rejoin armed on stable leaders: target_node_id={} groups={}",
                        target.id,
                        plan.len()
                    );
                    return Ok(());
                }
                Ok(_) => {
                    last_error = anyhow!(
                        "one or more raft leaders changed while arming node {}",
                        target.id
                    );
                }
                Err(err) => last_error = err,
            }
        }
        if Instant::now() >= deadline {
            break;
        }
        tokio::time::sleep(options.retry_interval).await;
    }
    Err(last_error).context("empty-log rejoin did not reach a stable leader map")
}

fn rejoin_leader_plan<'a>(
    nodes: &'a [NodeInfo],
    target_node_id: u64,
    snap: &ClusterSnapshot,
    group_ids: &BTreeSet<u64>,
) -> Result<BTreeMap<u64, &'a NodeInfo>> {
    let mut plan = BTreeMap::new();
    for group_id in group_ids {
        let leader_id = stable_non_target_leader(snap, *group_id, target_node_id)?;
        let leader = nodes
            .iter()
            .find(|node| node.id == leader_id)
            .ok_or_else(|| {
                anyhow!(
                    "leader node {} for group {} is not present in provider",
                    leader_id,
                    group_id
                )
            })?;
        plan.insert(*group_id, leader);
    }
    Ok(plan)
}

fn peer_reported_rejoin_groups(snap: &ClusterSnapshot, target_node_id: u64) -> BTreeSet<u64> {
    snap.per_node
        .iter()
        .filter(|view| view.node.id != target_node_id)
        .flat_map(|view| &view.groups)
        .filter(|group| group.voter_ids.contains(&target_node_id))
        .map(|group| group.raft_group_id)
        .collect()
}

/// Auto-derive the empty-log rejoin policy from the cluster's reported WAL
/// backend. `memory` needs it (every restart is amnesiac) and `disk` refuses
/// it (an empty rejoin there means a wiped node the leader should reject). An
/// older server that omits the field honors the explicit `force` flag.
pub async fn resolve_empty_rejoin_policy(
    client: &MetricsClient,
    nodes: &[NodeInfo],
    force: bool,
) -> Result<bool> {
    let snap = client.try_fetch_cluster(nodes).await;
    let backends: Vec<Option<&str>> = snap
        .per_node
        .iter()
        .map(|v| v.wal_backend.as_deref())
        .collect();
    let decision = decide_empty_rejoin(&backends, force)?;
    match decision {
        EmptyRejoinDecision::Memory => {
            tracing::info!("empty-log rejoin: enabled (raft-memory backend detected)")
        }
        EmptyRejoinDecision::Disk => {
            tracing::info!("empty-log rejoin: disabled (disk WAL backend detected)")
        }
        EmptyRejoinDecision::UnknownHonorFlag => tracing::info!(
            "empty-log rejoin: cluster did not report wal_backend; honoring the explicit flag ({force})"
        ),
    }
    Ok(decision.allow(force))
}

/// Resolve restart preparation without guessing when the server is too old or
/// unreachable to report its WAL backend.
///
/// Automated rollouts must fail closed here: treating an unknown memory-WAL
/// cluster as disk-backed would recreate an amnesiac voter without first
/// granting the leader-local rejoin permission.
pub async fn resolve_restart_rejoin_policy(
    client: &MetricsClient,
    nodes: &[NodeInfo],
) -> Result<bool> {
    let snap = client.try_fetch_cluster(nodes).await;
    if snap.per_node.len() != nodes.len() {
        bail!(
            "cannot prepare an automated restart because only {}/{} nodes reported metrics",
            snap.per_node.len(),
            nodes.len()
        );
    }
    let backends: Vec<Option<&str>> = snap
        .per_node
        .iter()
        .map(|v| v.wal_backend.as_deref())
        .collect();
    decide_restart_rejoin(&backends, nodes.len())
}

fn decide_restart_rejoin(backends: &[Option<&str>], expected_nodes: usize) -> Result<bool> {
    if backends.len() != expected_nodes {
        bail!(
            "cannot prepare an automated restart because only {}/{} nodes reported metrics",
            backends.len(),
            expected_nodes
        );
    }
    if backends.iter().any(Option::is_none) {
        bail!(
            "cannot prepare an automated restart because at least one node omitted wal_backend; \
             refusing to guess whether empty-log rejoin is required"
        );
    }
    let memory_count = backends
        .iter()
        .filter(|backend| **backend == Some("memory"))
        .count();
    let disk_count = backends
        .iter()
        .filter(|backend| **backend == Some("disk"))
        .count();
    match (memory_count, disk_count) {
        (memory, 0) if memory == expected_nodes => Ok(true),
        (0, disk) if disk == expected_nodes => Ok(false),
        _ => bail!(
            "cannot prepare an automated restart because nodes report inconsistent or unsupported \
             WAL backends: {backends:?}"
        ),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum EmptyRejoinDecision {
    Memory,
    Disk,
    UnknownHonorFlag,
}

impl EmptyRejoinDecision {
    pub(crate) fn allow(self, force: bool) -> bool {
        match self {
            EmptyRejoinDecision::Memory => true,
            EmptyRejoinDecision::Disk => false,
            EmptyRejoinDecision::UnknownHonorFlag => force,
        }
    }
}

/// Pure policy: `memory` anywhere enables empty rejoin. All-`disk` disables it
/// and refuses an explicit `force`, because that would auto-accept a wiped
/// node the leader must reject. An all-unknown cluster (older server) honors
/// the flag.
pub(crate) fn decide_empty_rejoin(
    backends: &[Option<&str>],
    force: bool,
) -> Result<EmptyRejoinDecision> {
    let any_memory = backends.contains(&Some("memory"));
    let any_known = backends
        .iter()
        .any(|b| matches!(*b, Some("memory") | Some("disk")));
    if any_memory {
        return Ok(EmptyRejoinDecision::Memory);
    }
    if !any_known {
        return Ok(EmptyRejoinDecision::UnknownHonorFlag);
    }
    if force {
        bail!(
            "empty-log rejoin was requested but every node reports a disk WAL backend; \
             an empty rejoin on a durable cluster means a wiped node the leader must \
             reject, so refusing rather than auto-accepting potential data loss"
        );
    }
    Ok(EmptyRejoinDecision::Disk)
}

/// A monotonic snapshot of how far a restarting target has caught up. Both
/// components only grow during a healthy rebuild: `applied_sum` climbs as
/// entries (or a whole snapshot) are applied, and `voters_ready` climbs as the
/// target rejoins each group's voter set.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct TargetProgress {
    applied_sum: u128,
    voters_ready: usize,
}

impl TargetProgress {
    fn of(report: &crate::plan::ReadinessReport) -> Self {
        let mut p = TargetProgress::default();
        for g in report.per_group.values() {
            p.applied_sum = p
                .applied_sum
                .saturating_add(u128::from(g.target_applied_index.unwrap_or(0)));
            if g.voter_member {
                p.voters_ready = p.voters_ready.saturating_add(1);
            }
        }
        p
    }

    /// True if either dimension advanced past `prev`. Any forward motion
    /// resets the stall clock.
    fn advanced_past(&self, prev: &TargetProgress) -> bool {
        self.applied_sum > prev.applied_sum || self.voters_ready > prev.voters_ready
    }
}

/// A target that reports no applied entries in any group after the readiness
/// window either never got permission to rejoin with an empty log or never
/// came back up at all; plain gap numbers do not tell an operator that.
fn amnesiac_timeout_hint(
    report: &crate::plan::ReadinessReport,
    empty_rejoin_armed: bool,
) -> Option<&'static str> {
    let all_unapplied = !report.per_group.is_empty()
        && report
            .per_group
            .values()
            .all(|g| g.target_applied_index.is_none());
    if !all_unapplied {
        return None;
    }
    if empty_rejoin_armed {
        Some(
            "target reports no applied entries in any group despite an armed \
             empty-log rejoin; it may be failing to start (check its \
             service logs, e.g. a raft-memory bootstrap marker refusing \
             restart) or still installing snapshots, so consider a larger \
             --ready-timeout-secs",
        )
    } else {
        Some(
            "target reports no applied entries in any group; if this cluster \
             runs the volatile raft-memory backend, arm an empty-log rejoin \
             (allow-rejoin, or restart with --allow-empty-raft-rejoin) and \
             allow enough time for full snapshot rebuilds (often 10+ minutes)",
        )
    }
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
            bail!(
                "leader transfer rejected for group {}: {}",
                transfer.raft_group_id,
                resp.reason.unwrap_or_else(|| "unknown".into())
            );
        }
    }
    Ok(())
}

fn stable_non_target_leader(
    snap: &ClusterSnapshot,
    raft_group_id: u64,
    target_node_id: u64,
) -> Result<u64> {
    let mut leader = None;
    for view in &snap.per_node {
        let Some(group) = view.group(raft_group_id) else {
            continue;
        };
        let Some(candidate) = group.current_leader else {
            continue;
        };
        if candidate == target_node_id {
            bail!(
                "target node {} is still reported as leader for group {} by node {}",
                target_node_id,
                raft_group_id,
                view.node.id
            );
        }
        if let Some(existing) = leader {
            if existing != candidate {
                bail!(
                    "conflicting leaders for group {} while allowing node {} rejoin: {} vs {}",
                    raft_group_id,
                    target_node_id,
                    existing,
                    candidate
                );
            }
        } else {
            leader = Some(candidate);
        }
    }
    leader.ok_or_else(|| {
        anyhow!(
            "group {} has no stable non-target leader; cannot allow empty raft rejoin for node {}",
            raft_group_id,
            target_node_id
        )
    })
}

pub(crate) fn format_unready(report: &crate::plan::ReadinessReport) -> String {
    let mut parts = Vec::new();
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
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;

    use axum::Json;
    use axum::Router;
    use axum::extract::State;
    use axum::http::StatusCode;
    use axum::routing::get;
    use axum::routing::post;
    use serde_json::json;

    use super::*;
    use crate::metrics::NodeMetricsView;
    use crate::metrics::RaftGroupView;
    use crate::provider::NodeInfo;

    #[derive(Clone, Copy)]
    enum LeaderScenario {
        DriftOnce,
        AlwaysDrift,
        Stable,
    }

    struct MockCluster {
        scenario: LeaderScenario,
        metrics_fetches: AtomicUsize,
        armed_on_nodes: Mutex<Vec<u64>>,
    }

    #[derive(Clone)]
    struct MockNode {
        node_id: u64,
        cluster: Arc<MockCluster>,
    }

    async fn mock_metrics(State(state): State<MockNode>) -> Json<serde_json::Value> {
        let fetch = state.cluster.metrics_fetches.fetch_add(1, Ordering::SeqCst);
        let sample = fetch / 3;
        let leader = match state.cluster.scenario {
            LeaderScenario::DriftOnce if sample == 0 => 2,
            LeaderScenario::DriftOnce => 3,
            LeaderScenario::AlwaysDrift if sample.is_multiple_of(2) => 2,
            LeaderScenario::AlwaysDrift => 3,
            LeaderScenario::Stable => 2,
        };
        Json(json!({
            "wal_backend": "memory",
            "raft_groups": [{
                "raft_group_id": 7,
                "node_id": state.node_id,
                "current_leader": leader,
                "committed_index": 100,
                "last_applied_index": 100,
                "voter_ids": [1, 2, 3],
                "learner_ids": []
            }]
        }))
    }

    async fn mock_arm(State(state): State<MockNode>) -> StatusCode {
        state
            .cluster
            .armed_on_nodes
            .lock()
            .unwrap()
            .push(state.node_id);
        StatusCode::OK
    }

    async fn mock_cluster(scenario: LeaderScenario) -> (Vec<NodeInfo>, Arc<MockCluster>) {
        let cluster = Arc::new(MockCluster {
            scenario,
            metrics_fetches: AtomicUsize::new(0),
            armed_on_nodes: Mutex::new(Vec::new()),
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
                    "/__ursula/raft/{group_id}/nodes/{node_id}/allow-next-revert",
                    post(mock_arm),
                )
                .with_state(state);
            tokio::spawn(async move {
                axum::serve(listener, app).await.unwrap();
            });
            let url = url::Url::parse(&format!("http://{address}")).unwrap();
            nodes.push(NodeInfo {
                id: node_id,
                admin_url: url.clone(),
                host: address.to_string(),
                http_url: Some(url),
            });
        }
        (nodes, cluster)
    }

    fn n(id: u64, host: &str) -> NodeInfo {
        NodeInfo {
            id,
            admin_url: url::Url::parse(&format!("http://{host}:4438")).unwrap(),
            host: host.to_owned(),
            http_url: Some(url::Url::parse(&format!("http://{host}:8080")).unwrap()),
        }
    }

    #[tokio::test]
    async fn rejoin_retries_the_complete_round_after_leader_drift() {
        let (nodes, cluster) = mock_cluster(LeaderScenario::DriftOnce).await;
        arm_empty_rejoin(
            &nodes,
            &nodes[0],
            &MetricsClient::new(Duration::from_secs(1)).unwrap(),
            &RejoinOptions {
                timeout: Duration::from_secs(1),
                max_concurrency: 2,
                retry_interval: Duration::from_millis(1),
            },
        )
        .await
        .unwrap();

        assert_eq!(*cluster.armed_on_nodes.lock().unwrap(), vec![2, 3]);
        assert_eq!(cluster.metrics_fetches.load(Ordering::SeqCst), 12);
    }

    #[tokio::test]
    async fn rejoin_fails_when_the_leader_map_never_stabilizes() {
        let (nodes, cluster) = mock_cluster(LeaderScenario::AlwaysDrift).await;
        let error = arm_empty_rejoin(
            &nodes,
            &nodes[0],
            &MetricsClient::new(Duration::from_secs(1)).unwrap(),
            &RejoinOptions {
                timeout: Duration::from_millis(40),
                max_concurrency: 2,
                retry_interval: Duration::from_millis(1),
            },
        )
        .await
        .unwrap_err();

        assert!(
            error
                .to_string()
                .contains("empty-log rejoin did not reach a stable leader map"),
            "{error:#}"
        );
        assert!(!cluster.armed_on_nodes.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn cluster_verification_timeout_explains_a_single_ready_sample() {
        let (nodes, _) = mock_cluster(LeaderScenario::Stable).await;
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
            current_leader,
            committed_index: Some(committed),
            last_applied_index: Some(applied),
            voter_ids: vec![1, 2, 3],
            learner_ids: vec![],
        }
    }

    #[test]
    fn cluster_readiness_formats_each_unready_node() {
        let snapshot = ClusterSnapshot {
            per_node: vec![
                NodeMetricsView {
                    node: n(1, "10.0.0.1"),
                    groups: vec![group(7, 1, Some(1), 50, 50)],
                    wal_backend: None,
                },
                NodeMetricsView {
                    node: n(2, "10.0.0.2"),
                    groups: vec![group(7, 2, Some(1), 100, 100)],
                    wal_backend: None,
                },
                NodeMetricsView {
                    node: n(3, "10.0.0.3"),
                    groups: vec![group(7, 3, Some(1), 95, 100)],
                    wal_backend: None,
                },
            ],
        };

        let report = check_readiness(&snapshot, 1, 5);

        assert!(!report.all_ready);
        let formatted = format_unready(&report);
        assert!(formatted.contains("gap=Some(50)"), "{formatted}");
    }

    #[test]
    fn amnesiac_timeout_hint_suggests_arming_only_when_unarmed() {
        let snapshot = ClusterSnapshot {
            per_node: vec![NodeMetricsView {
                node: n(2, "10.0.0.2"),
                groups: vec![group(7, 2, Some(2), 100, 100)],
                wal_backend: None,
            }],
        };
        let report = check_readiness(&snapshot, 1, 5);
        assert!(!report.all_ready);

        let hint = amnesiac_timeout_hint(&report, false).expect("hint when rejoin unarmed");
        assert!(hint.contains("allow-rejoin"), "{hint}");

        let hint = amnesiac_timeout_hint(&report, true).expect("hint when rejoin armed");
        assert!(hint.contains("failing to start"), "{hint}");
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
    fn empty_rejoin_policy_follows_reported_backend() {
        // memory anywhere → on
        assert_eq!(
            decide_empty_rejoin(&[Some("disk"), Some("memory")], false).unwrap(),
            EmptyRejoinDecision::Memory
        );
        // all disk, not forced → off
        assert_eq!(
            decide_empty_rejoin(&[Some("disk"), Some("disk")], false).unwrap(),
            EmptyRejoinDecision::Disk
        );
        // all disk, forced → refused
        assert!(decide_empty_rejoin(&[Some("disk")], true).is_err());
        // unknown (older server) honors the flag
        assert!(
            decide_empty_rejoin(&[None, None], true)
                .unwrap()
                .allow(true)
        );
        assert!(!decide_empty_rejoin(&[None], false).unwrap().allow(false));
    }

    #[test]
    fn automated_restart_policy_fails_closed() {
        assert!(decide_restart_rejoin(&[Some("memory"); 3], 3).unwrap());
        assert!(!decide_restart_rejoin(&[Some("disk"); 3], 3).unwrap());
        assert!(decide_restart_rejoin(&[Some("memory"), Some("disk")], 2).is_err());
        assert!(decide_restart_rejoin(&[Some("memory"), None], 2).is_err());
        assert!(decide_restart_rejoin(&[Some("memory")], 2).is_err());
    }

    #[test]
    fn rejoin_plan_requires_a_stable_non_target_leader() {
        let nodes = vec![n(1, "10.0.0.1"), n(2, "10.0.0.2"), n(3, "10.0.0.3")];
        let groups = BTreeSet::from([7]);
        let stable = ClusterSnapshot {
            per_node: vec![
                NodeMetricsView {
                    node: nodes[1].clone(),
                    groups: vec![group(7, 2, Some(2), 100, 100)],
                    wal_backend: Some("memory".into()),
                },
                NodeMetricsView {
                    node: nodes[2].clone(),
                    groups: vec![group(7, 3, Some(2), 100, 100)],
                    wal_backend: Some("memory".into()),
                },
            ],
        };
        assert_eq!(
            rejoin_leader_plan(&nodes, 1, &stable, &groups)
                .unwrap()
                .get(&7)
                .map(|node| node.id),
            Some(2)
        );

        let conflicting = ClusterSnapshot {
            per_node: vec![
                NodeMetricsView {
                    node: nodes[1].clone(),
                    groups: vec![group(7, 2, Some(2), 100, 100)],
                    wal_backend: Some("memory".into()),
                },
                NodeMetricsView {
                    node: nodes[2].clone(),
                    groups: vec![group(7, 3, Some(3), 100, 100)],
                    wal_backend: Some("memory".into()),
                },
            ],
        };
        assert!(rejoin_leader_plan(&nodes, 1, &conflicting, &groups).is_err());
    }

    #[test]
    fn amnesiac_timeout_hint_absent_when_target_has_applied_entries() {
        let snapshot = ClusterSnapshot {
            per_node: vec![
                NodeMetricsView {
                    node: n(1, "10.0.0.1"),
                    groups: vec![group(7, 1, Some(2), 50, 50)],
                    wal_backend: None,
                },
                NodeMetricsView {
                    node: n(2, "10.0.0.2"),
                    groups: vec![group(7, 2, Some(2), 100, 100)],
                    wal_backend: None,
                },
            ],
        };
        let report = check_readiness(&snapshot, 1, 5);
        assert!(!report.all_ready);
        assert!(amnesiac_timeout_hint(&report, false).is_none());
    }

    #[test]
    fn peer_reported_target_leader_keeps_drain_active() {
        let snapshot = ClusterSnapshot {
            per_node: vec![
                NodeMetricsView {
                    node: n(1, "10.0.0.1"),
                    groups: vec![group(7, 1, Some(2), 100, 100)],
                    wal_backend: None,
                },
                NodeMetricsView {
                    node: n(2, "10.0.0.2"),
                    groups: vec![group(7, 2, Some(2), 100, 100)],
                    wal_backend: None,
                },
                NodeMetricsView {
                    node: n(3, "10.0.0.3"),
                    groups: vec![group(7, 3, Some(1), 100, 100)],
                    wal_backend: None,
                },
            ],
        };

        assert!(snapshot.groups_led_by(1).is_empty());

        let still_led = snapshot.groups_reported_led_by(1);
        assert_eq!(still_led.len(), 1);
        assert_eq!(still_led[0].raft_group_id, 7);

        let err = stable_non_target_leader(&snapshot, 7, 1).unwrap_err();
        assert!(
            err.to_string().contains("still reported as leader"),
            "{err:#}"
        );
    }

    #[test]
    fn empty_target_group_uses_peer_membership_for_rejoin() {
        let mut empty_target_group = group(7, 1, None, 0, 0);
        empty_target_group.voter_ids.clear();
        empty_target_group.committed_index = None;
        empty_target_group.last_applied_index = None;
        let snapshot = ClusterSnapshot {
            per_node: vec![
                NodeMetricsView {
                    node: n(1, "10.0.0.1"),
                    groups: vec![empty_target_group],
                    wal_backend: Some("memory".into()),
                },
                NodeMetricsView {
                    node: n(2, "10.0.0.2"),
                    groups: vec![group(7, 2, Some(2), 100, 100)],
                    wal_backend: Some("memory".into()),
                },
                NodeMetricsView {
                    node: n(3, "10.0.0.3"),
                    groups: vec![group(7, 3, Some(2), 100, 100)],
                    wal_backend: Some("memory".into()),
                },
            ],
        };

        assert_eq!(
            peer_reported_rejoin_groups(&snapshot, 1),
            [7].into_iter().collect()
        );
        assert_eq!(stable_non_target_leader(&snapshot, 7, 1).unwrap(), 2);
    }
}

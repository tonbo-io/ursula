//! Logical cluster-state verbs: drain, undrain, catch-up wait, and empty-log
//! rejoin arming.
//!
//! These operate purely on Ursula's admin/metrics HTTP surface and never
//! execute anything on a host. Physical lifecycle (stopping and starting the
//! process) belongs to the platform that owns it: Kubernetes and Helm for pod
//! clusters, systemd for bare-metal hosts. [`crate::orchestrate::run_restart`]
//! composes these verbs with a physical restart command for environments with
//! no platform controller.

use std::time::Duration;
use std::time::Instant;

use anyhow::Context;
use anyhow::Result;
use anyhow::anyhow;
use anyhow::bail;

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
            bail!("{phase} timeout after {timeout:?}: {}", unready.join("; "));
        }
        tokio::time::sleep(poll_interval).await;
    }
}

/// Ask every group's stable leader to accept one empty-log rejoin from
/// `target`. This is the recovery permission a raft-memory node needs after
/// losing its volatile log; leaders reject unsolicited empty rejoins.
pub async fn arm_empty_rejoin(
    nodes: &[NodeInfo],
    target: &NodeInfo,
    client: &MetricsClient,
    snap: &ClusterSnapshot,
) -> Result<()> {
    let Some(target_view) = snap.node(target.id) else {
        bail!(
            "target node {} missing from metrics; cannot allow empty raft rejoin",
            target.id
        );
    };
    for group in &target_view.groups {
        if !group.voter_ids.contains(&target.id) {
            continue;
        }
        let leader_id = stable_non_target_leader(snap, group.raft_group_id, target.id)?;
        let Some(leader) = nodes.iter().find(|node| node.id == leader_id) else {
            bail!(
                "leader node {} for group {} is not present in provider",
                leader_id,
                group.raft_group_id
            );
        };
        tracing::info!(
            "allowing empty raft rejoin: target_node_id={} raft_group_id={} leader_node_id={}",
            target.id,
            group.raft_group_id,
            leader.id
        );
        client
            .allow_next_revert(leader, group.raft_group_id, target.id)
            .await?;
    }
    Ok(())
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
    use super::*;
    use crate::metrics::NodeMetricsView;
    use crate::metrics::RaftGroupView;
    use crate::provider::NodeInfo;

    fn n(id: u64, host: &str) -> NodeInfo {
        NodeInfo {
            id,
            admin_url: url::Url::parse(&format!("http://{host}:4438")).unwrap(),
            host: host.to_owned(),
            instance_id: None,
            ssh_host: None,
            http_url: Some(url::Url::parse(&format!("http://{host}:8080")).unwrap()),
            name: Some(format!("node-{id}")),
        }
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
}

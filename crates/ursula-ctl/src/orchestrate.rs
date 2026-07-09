use std::collections::BTreeSet;
use std::process::Stdio;
use std::time::Duration;
use std::time::Instant;

use anyhow::Context;
use anyhow::Result;
use anyhow::anyhow;
use anyhow::bail;
use tokio::process::Command;

use crate::metrics::ClusterSnapshot;
use crate::metrics::MetricsClient;
use crate::plan::check_readiness;
use crate::plan::plan_drain;
use crate::provider::NodeInfo;

#[derive(Debug, Clone)]
pub struct RestartOptions {
    pub drain_timeout: Duration,
    pub ready_timeout: Duration,
    pub poll_interval: Duration,
    pub lag_tolerance: u64,
    /// Force empty-log rejoin on regardless of detected backend. Normally the
    /// policy is auto-derived from each node's reported `wal_backend`: `memory`
    /// enables it, `disk` refuses it, an older server with no field honors this
    /// flag. Forcing it on a `disk` cluster is refused (that would auto-accept a
    /// wiped node the leader is meant to reject).
    pub force_allow_empty: bool,
    /// Per-node ids to restart, in order. Empty means every node from the provider.
    pub only: Option<Vec<u64>>,
    /// Print plan without executing the restart command.
    pub dry_run: bool,
}

impl Default for RestartOptions {
    fn default() -> Self {
        Self {
            drain_timeout: Duration::from_secs(60),
            ready_timeout: Duration::from_secs(120),
            poll_interval: Duration::from_secs(2),
            lag_tolerance: 16,
            force_allow_empty: false,
            only: None,
            dry_run: false,
        }
    }
}

#[derive(Debug, Clone)]
pub enum RestartOutcome {
    Restarted,
    Skipped { reason: String },
    Aborted { reason: String },
}

#[derive(Debug, Clone)]
pub struct RestartReport {
    pub per_node: Vec<(u64, RestartOutcome)>,
}

impl RestartReport {
    pub fn all_succeeded(&self) -> bool {
        self.per_node.iter().all(|(_, outcome)| {
            matches!(
                outcome,
                RestartOutcome::Restarted | RestartOutcome::Skipped { .. }
            )
        })
    }
}

pub async fn run_restart(
    nodes: &[NodeInfo],
    client: &MetricsClient,
    provider: &crate::operation::OperationProvider,
    options: &RestartOptions,
) -> Result<RestartReport> {
    if nodes.is_empty() {
        bail!("provider returned no nodes");
    }
    let ordered: Vec<&NodeInfo> = match &options.only {
        Some(ids) => {
            let id_set: BTreeSet<u64> = ids.iter().copied().collect();
            let filtered: Vec<&NodeInfo> = ids
                .iter()
                .map(|id| {
                    nodes
                        .iter()
                        .find(|n| n.id == *id)
                        .ok_or_else(|| anyhow!("node id {id} not present in provider"))
                })
                .collect::<Result<_>>()?;
            if filtered.len() != id_set.len() {
                bail!("--only contains duplicate node ids");
            }
            filtered
        }
        None => nodes.iter().collect(),
    };

    // Decide the empty-log rejoin policy once from the cluster's reported WAL
    // backend, so the operator does not have to know it per invocation.
    let allow_empty = if options.dry_run {
        false
    } else {
        resolve_empty_rejoin_policy(client, nodes, options.force_allow_empty).await?
    };

    let mut report = RestartReport {
        per_node: Vec::new(),
    };
    for (idx, target) in ordered.iter().enumerate() {
        tracing::info!(
            "begin per-node restart: target_node_id={} step={} total={}",
            target.id,
            idx + 1,
            ordered.len()
        );
        let outcome = restart_one(nodes, target, client, provider, options, allow_empty).await;
        match &outcome {
            Ok(RestartOutcome::Aborted { reason }) => {
                tracing::error!(
                    "aborting rollout: target_node_id={} reason={reason}",
                    target.id
                );
                report.per_node.push((target.id, RestartOutcome::Aborted {
                    reason: reason.clone(),
                }));
                return Ok(report);
            }
            Ok(o) => {
                tracing::info!("node done: target_node_id={} outcome={o:?}", target.id);
                report.per_node.push((target.id, o.clone()));
            }
            Err(err) => {
                let reason = format!("{err:#}");
                tracing::error!(
                    "aborting rollout: target_node_id={} reason={reason}",
                    target.id
                );
                report
                    .per_node
                    .push((target.id, RestartOutcome::Aborted { reason }));
                return Ok(report);
            }
        }
    }
    Ok(report)
}

/// Auto-derive the empty-log rejoin policy from the cluster's reported WAL
/// backend. `memory` needs it (every restart is amnesiac); `disk` refuses it
/// (an empty rejoin there means a wiped node the leader should reject); an
/// older server that omits the field honors the explicit `force` flag.
async fn resolve_empty_rejoin_policy(
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
            "empty-log rejoin: cluster did not report wal_backend; using --allow-empty-raft-rejoin={force}"
        ),
    }
    Ok(decision.allow(force))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EmptyRejoinDecision {
    Memory,
    Disk,
    UnknownHonorFlag,
}

impl EmptyRejoinDecision {
    fn allow(self, force: bool) -> bool {
        match self {
            EmptyRejoinDecision::Memory => true,
            EmptyRejoinDecision::Disk => false,
            EmptyRejoinDecision::UnknownHonorFlag => force,
        }
    }
}

/// Pure policy: `memory` anywhere enables empty rejoin; all-`disk` disables it
/// and refuses an explicit `force` (that would auto-accept a wiped node the
/// leader must reject); an all-unknown cluster (older server) honors the flag.
fn decide_empty_rejoin(backends: &[Option<&str>], force: bool) -> Result<EmptyRejoinDecision> {
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
            "--allow-empty-raft-rejoin was set but every node reports a disk WAL backend; \
             an empty-log rejoin on a durable cluster means a wiped node the leader must \
             reject — refusing rather than auto-accepting potential data loss"
        );
    }
    Ok(EmptyRejoinDecision::Disk)
}

async fn restart_one(
    nodes: &[NodeInfo],
    target: &NodeInfo,
    client: &MetricsClient,
    provider: &crate::operation::OperationProvider,
    options: &RestartOptions,
    allow_empty: bool,
) -> Result<RestartOutcome> {
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
    }

    if !options.dry_run {
        client
            .set_maintenance_drain(target, true)
            .await
            .with_context(|| format!("mark maintenance-drain on node {}", target.id))?;
    }

    // Pre-flight cluster snapshot.
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

    // Wait until target leads zero groups.
    if !options.dry_run {
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
                let snap = match client.fetch_cluster(nodes).await {
                    Ok(snap) => snap,
                    Err(err) => {
                        clear_maintenance_drain(client, target).await;
                        return Err(err).context("post-drain metrics");
                    }
                };
                if allow_empty
                    && let Err(err) = allow_empty_raft_rejoin(nodes, target, client, &snap).await
                {
                    clear_maintenance_drain(client, target).await;
                    return Err(err);
                }
                break;
            }
            let plan = plan_drain(&snap, target.id);
            if plan.transfers.is_empty() {
                clear_maintenance_drain(client, target).await;
                return Ok(RestartOutcome::Aborted {
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
                return Ok(RestartOutcome::Aborted {
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

    if options.dry_run {
        return Ok(RestartOutcome::Skipped {
            reason: "dry-run".into(),
        });
    }

    // Execute the provider's restart command for this node.
    let restart_cmd = match provider.restart_command(target) {
        Ok(cmd) => cmd,
        Err(err) => {
            clear_maintenance_drain(client, target).await;
            return Err(err)
                .with_context(|| format!("build restart command for node {}", target.id));
        }
    };
    if let Err(err) = execute_restart_cmd(target, &restart_cmd).await {
        clear_maintenance_drain(client, target).await;
        return Err(err).with_context(|| format!("restart command for node {}", target.id));
    }

    // Wait for readiness.
    let deadline = Instant::now() + options.ready_timeout;
    loop {
        let snap = client.try_fetch_cluster(nodes).await;
        let report = check_readiness(&snap, target.id, options.lag_tolerance);
        if report.all_ready {
            clear_maintenance_drain(client, target).await;
            wait_cluster_ready(
                "post-restart cluster readiness",
                nodes,
                client,
                options.ready_timeout,
                options.poll_interval,
                options.lag_tolerance,
            )
            .await?;
            return Ok(RestartOutcome::Restarted);
        }
        if Instant::now() >= deadline {
            clear_maintenance_drain(client, target).await;
            let mut reason = format!(
                "readiness timeout after {:?}: {}",
                options.ready_timeout,
                format_unready(&snap, &report)
            );
            if let Some(hint) = amnesiac_timeout_hint(&report, allow_empty) {
                reason.push_str("; ");
                reason.push_str(hint);
            }
            return Ok(RestartOutcome::Aborted { reason });
        }
        tokio::time::sleep(options.poll_interval).await;
    }
}

/// A target that reports no applied entries in any group after the readiness
/// window either never got permission to rejoin with an empty log or never
/// came back up at all; plain gap numbers do not tell an operator that.
fn amnesiac_timeout_hint(
    report: &crate::plan::ReadinessReport,
    allow_empty: bool,
) -> Option<&'static str> {
    let all_unapplied = !report.per_group.is_empty()
        && report
            .per_group
            .values()
            .all(|g| g.target_applied_index.is_none());
    if !all_unapplied {
        return None;
    }
    if allow_empty {
        Some(
            "target reports no applied entries in any group despite \
             --allow-empty-raft-rejoin; it may be failing to start (check its \
             service logs, e.g. a raft-memory bootstrap marker refusing \
             restart) or still installing snapshots — consider a larger \
             --ready-timeout-secs",
        )
    } else {
        Some(
            "target reports no applied entries in any group; if this cluster \
             runs the volatile raft-memory backend, rerun with \
             --allow-empty-raft-rejoin and a --ready-timeout-secs large \
             enough for full snapshot rebuilds (often 10+ minutes)",
        )
    }
}

async fn wait_cluster_ready(
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
                unready.push(format!(
                    "node {}: {}",
                    node.id,
                    format_unready(&snap, &report)
                ));
            }
        }
        if unready.is_empty() {
            ready_streak += 1;
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

async fn transfer_drain_plan(
    target: &NodeInfo,
    client: &MetricsClient,
    plan: &crate::plan::DrainPlan,
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

async fn clear_maintenance_drain(client: &MetricsClient, target: &NodeInfo) {
    if let Err(err) = client.set_maintenance_drain(target, false).await {
        tracing::warn!(
            "failed to clear maintenance-drain: target_node_id={} error={err}",
            target.id
        );
    }
}

async fn allow_empty_raft_rejoin(
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

fn format_unready(_snap: &ClusterSnapshot, report: &crate::plan::ReadinessReport) -> String {
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

/// Run the provider-built restart command (already fully rendered) under `sh -c`.
async fn execute_restart_cmd(target: &NodeInfo, rendered: &str) -> Result<()> {
    tracing::info!(
        "exec restart command: target_node_id={} cmd={rendered}",
        target.id
    );
    let status = Command::new("sh")
        .arg("-c")
        .arg(rendered)
        .stdin(Stdio::null())
        .status()
        .await
        .with_context(|| format!("spawn restart cmd: {rendered}"))?;
    if !status.success() {
        bail!("restart command exited with {status}: {rendered}");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::ClusterSnapshot;
    use crate::metrics::NodeMetricsView;
    use crate::metrics::RaftGroupView;

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

    #[test]
    fn ssh_provider_builds_restart_command() {
        use crate::operation::OperationProvider;
        use crate::operation::ProviderKind;
        let provider = OperationProvider {
            kind: ProviderKind::Ssh,
            ssh_user: Some("ec2-user".to_owned()),
            restart_unit: Some("ursula-chaos.service".to_owned()),
            ..Default::default()
        };
        let rendered = provider.restart_command(&n(3, "10.0.0.3")).unwrap();
        assert_eq!(
            rendered,
            "ssh -o StrictHostKeyChecking=no -o BatchMode=yes ec2-user@10.0.0.3 'sudo systemctl restart ursula-chaos.service'"
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
        let formatted = format_unready(&snapshot, &report);
        assert!(formatted.contains("gap=Some(50)"), "{formatted}");
    }

    #[test]
    fn amnesiac_timeout_hint_suggests_rejoin_flag_only_when_unset() {
        let snapshot = ClusterSnapshot {
            per_node: vec![NodeMetricsView {
                node: n(2, "10.0.0.2"),
                groups: vec![group(7, 2, Some(2), 100, 100)],
                wal_backend: None,
            }],
        };
        let report = check_readiness(&snapshot, 1, 5);
        assert!(!report.all_ready);

        let hint = amnesiac_timeout_hint(&report, false).expect("hint when rejoin off");
        assert!(hint.contains("--allow-empty-raft-rejoin"), "{hint}");

        let hint = amnesiac_timeout_hint(&report, true).expect("hint when rejoin on");
        assert!(hint.contains("failing to start"), "{hint}");
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

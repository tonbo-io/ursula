use std::collections::BTreeSet;
use std::process::Stdio;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow, bail};
use tokio::process::Command;

use crate::metrics::{ClusterSnapshot, MetricsClient};
use crate::plan::{check_readiness, plan_drain};
use crate::provider::NodeInfo;

#[derive(Debug, Clone)]
pub struct RestartOptions {
    pub restart_cmd: String,
    pub drain_timeout: Duration,
    pub ready_timeout: Duration,
    pub poll_interval: Duration,
    pub lag_tolerance: u64,
    /// Before restarting a target, ask current leaders to allow one empty-log
    /// rejoin from that target. Use for volatile `--raft-memory` clusters only.
    pub allow_empty_raft_rejoin: bool,
    /// Per-node ids to restart, in order. Empty means every node from the provider.
    pub only: Option<Vec<u64>>,
    /// Print plan without executing the restart_cmd.
    pub dry_run: bool,
}

impl Default for RestartOptions {
    fn default() -> Self {
        Self {
            restart_cmd: String::new(),
            drain_timeout: Duration::from_secs(60),
            ready_timeout: Duration::from_secs(120),
            poll_interval: Duration::from_secs(2),
            lag_tolerance: 16,
            allow_empty_raft_rejoin: false,
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
    options: &RestartOptions,
) -> Result<RestartReport> {
    if nodes.is_empty() {
        bail!("provider returned no nodes");
    }
    if options.restart_cmd.trim().is_empty() && !options.dry_run {
        bail!("--restart-cmd is required unless --dry-run is set");
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
        let outcome = restart_one(nodes, target, client, options).await;
        match &outcome {
            Ok(RestartOutcome::Aborted { reason }) => {
                tracing::error!(
                    "aborting rollout: target_node_id={} reason={reason}",
                    target.id
                );
                report.per_node.push((
                    target.id,
                    RestartOutcome::Aborted {
                        reason: reason.clone(),
                    },
                ));
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

async fn restart_one(
    nodes: &[NodeInfo],
    target: &NodeInfo,
    client: &MetricsClient,
    options: &RestartOptions,
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
                if options.allow_empty_raft_rejoin
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

    // Execute --restart-cmd.
    if let Err(err) = execute_restart_cmd(target, &options.restart_cmd).await {
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
            return Ok(RestartOutcome::Aborted {
                reason: format!(
                    "readiness timeout after {:?}: {}",
                    options.ready_timeout,
                    format_unready(&snap, &report)
                ),
            });
        }
        tokio::time::sleep(options.poll_interval).await;
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

async fn execute_restart_cmd(target: &NodeInfo, template: &str) -> Result<()> {
    let rendered = render_template(template, target);
    tracing::info!(
        "exec restart command: target_node_id={} cmd={rendered}",
        target.id
    );
    let status = Command::new("sh")
        .arg("-c")
        .arg(&rendered)
        .stdin(Stdio::null())
        .status()
        .await
        .with_context(|| format!("spawn restart cmd: {rendered}"))?;
    if !status.success() {
        bail!("restart command exited with {status}: {rendered}");
    }
    Ok(())
}

fn render_template(template: &str, node: &NodeInfo) -> String {
    template
        .replace("{node_id}", &node.id.to_string())
        .replace("{host}", &node.host)
        .replace("{http_url}", node.http_url.as_str())
        .replace(
            "{name}",
            node.name.as_deref().unwrap_or(&node.id.to_string()),
        )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::{ClusterSnapshot, NodeMetricsView, RaftGroupView};

    fn n(id: u64, host: &str) -> NodeInfo {
        NodeInfo {
            id,
            http_url: url::Url::parse(&format!("http://{host}:8080")).unwrap(),
            host: host.to_owned(),
            name: Some(format!("node-{id}")),
        }
    }

    #[test]
    fn render_template_substitutes_known_placeholders() {
        let rendered = render_template(
            "ssh ec2-user@{host} sudo systemctl restart ursula-chaos@{node_id}.service # {name}",
            &n(3, "10.0.0.3"),
        );
        assert_eq!(
            rendered,
            "ssh ec2-user@10.0.0.3 sudo systemctl restart ursula-chaos@3.service # node-3"
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
                },
                NodeMetricsView {
                    node: n(2, "10.0.0.2"),
                    groups: vec![group(7, 2, Some(1), 100, 100)],
                },
                NodeMetricsView {
                    node: n(3, "10.0.0.3"),
                    groups: vec![group(7, 3, Some(1), 95, 100)],
                },
            ],
        };

        let report = check_readiness(&snapshot, 1, 5);

        assert!(!report.all_ready);
        let formatted = format_unready(&snapshot, &report);
        assert!(formatted.contains("gap=Some(50)"), "{formatted}");
    }

    #[test]
    fn peer_reported_target_leader_keeps_drain_active() {
        let snapshot = ClusterSnapshot {
            per_node: vec![
                NodeMetricsView {
                    node: n(1, "10.0.0.1"),
                    groups: vec![group(7, 1, Some(2), 100, 100)],
                },
                NodeMetricsView {
                    node: n(2, "10.0.0.2"),
                    groups: vec![group(7, 2, Some(2), 100, 100)],
                },
                NodeMetricsView {
                    node: n(3, "10.0.0.3"),
                    groups: vec![group(7, 3, Some(1), 100, 100)],
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

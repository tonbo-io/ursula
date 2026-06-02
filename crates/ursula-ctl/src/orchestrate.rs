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
    // Pre-flight cluster snapshot.
    let snapshot = client
        .fetch_cluster(nodes)
        .await
        .context("pre-flight metrics")?;
    let plan = plan_drain(&snapshot, target.id);
    tracing::info!(
        "drain plan computed: target_node_id={} led_groups={}",
        target.id,
        plan.transfers.len()
    );

    // Drain leaderships.
    for transfer in &plan.transfers {
        tracing::info!(
            "transferring leadership: target_node_id={} raft_group_id={} to={}",
            target.id,
            transfer.raft_group_id,
            transfer.preferred_successor
        );
        if options.dry_run {
            continue;
        }
        let resp = client
            .transfer_leader(target, transfer.raft_group_id, transfer.preferred_successor)
            .await?;
        if !resp.transferred {
            return Ok(RestartOutcome::Aborted {
                reason: format!(
                    "leader transfer rejected for group {}: {}",
                    transfer.raft_group_id,
                    resp.reason.unwrap_or_else(|| "unknown".into())
                ),
            });
        }
    }

    // Wait until target leads zero groups.
    if !options.dry_run {
        let deadline = Instant::now() + options.drain_timeout;
        loop {
            let snap = client.fetch_cluster(nodes).await.context("drain poll")?;
            let still_leads = snap.groups_led_by(target.id);
            if still_leads.is_empty() {
                break;
            }
            if Instant::now() >= deadline {
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
    execute_restart_cmd(target, &options.restart_cmd)
        .await
        .with_context(|| format!("restart command for node {}", target.id))?;

    // Wait for readiness.
    let deadline = Instant::now() + options.ready_timeout;
    loop {
        let snap = client.try_fetch_cluster(nodes).await;
        let report = check_readiness(&snap, target.id, options.lag_tolerance);
        if report.all_ready {
            return Ok(RestartOutcome::Restarted);
        }
        if Instant::now() >= deadline {
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
}

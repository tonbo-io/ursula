//! Rolling-restart orchestration for clusters with no platform controller.
//!
//! This composes the logical verbs in [`crate::maintenance`] (drain, catch-up
//! wait, empty-log rejoin arming) with a physical restart command built by an
//! [`crate::operation::OperationProvider`]. It is the bare-metal counterpart
//! of what a drain-aware Kubernetes rollout does: on platforms that own
//! process lifecycle, run the logical verbs individually and let the platform
//! perform the restart.

use std::collections::BTreeSet;
use std::process::Stdio;
use std::time::Duration;

use anyhow::Context;
use anyhow::Result;
use anyhow::anyhow;
use anyhow::bail;
use tokio::process::Command;

use crate::maintenance::CatchUpOptions;
use crate::maintenance::CatchUpOutcome;
use crate::maintenance::DrainOptions;
use crate::maintenance::DrainOutcome;
use crate::maintenance::arm_empty_rejoin;
use crate::maintenance::clear_maintenance_drain;
use crate::maintenance::drain_node;
use crate::maintenance::resolve_empty_rejoin_policy;
use crate::maintenance::wait_cluster_ready;
use crate::maintenance::wait_node_ready;
use crate::metrics::MetricsClient;
use crate::provider::NodeInfo;

#[derive(Debug, Clone)]
pub struct RestartOptions {
    pub drain_timeout: Duration,
    pub ready_timeout: Duration,
    pub poll_interval: Duration,
    pub lag_tolerance: u64,
    /// Abort the post-restart readiness wait when the target makes no catch-up
    /// progress (no new applied entries, no new voter memberships) for this
    /// long. This is the real control: a rebuild that keeps advancing is never
    /// timed out, while a stuck or crash-looping node aborts quickly.
    /// `ready_timeout` is only an absolute backstop above it.
    pub stall_timeout: Duration,
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
            stall_timeout: Duration::from_secs(90),
            force_allow_empty: false,
            only: None,
            dry_run: false,
        }
    }
}

impl RestartOptions {
    fn drain_options(&self) -> DrainOptions {
        DrainOptions {
            drain_timeout: self.drain_timeout,
            ready_timeout: self.ready_timeout,
            poll_interval: self.poll_interval,
            lag_tolerance: self.lag_tolerance,
            dry_run: self.dry_run,
        }
    }

    fn catch_up_options(&self) -> CatchUpOptions {
        CatchUpOptions {
            stall_timeout: self.stall_timeout,
            ready_timeout: self.ready_timeout,
            poll_interval: self.poll_interval,
            lag_tolerance: self.lag_tolerance,
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

async fn restart_one(
    nodes: &[NodeInfo],
    target: &NodeInfo,
    client: &MetricsClient,
    provider: &crate::operation::OperationProvider,
    options: &RestartOptions,
    allow_empty: bool,
) -> Result<RestartOutcome> {
    // Logical phase 1: drain leaderships. The drain mark stays set through the
    // restart so the node does not re-acquire groups mid-rollout.
    match drain_node(nodes, target, client, &options.drain_options()).await? {
        DrainOutcome::Drained => {}
        DrainOutcome::DryRun(_plan) => {
            return Ok(RestartOutcome::Skipped {
                reason: "dry-run".into(),
            });
        }
        DrainOutcome::Aborted { reason } => {
            return Ok(RestartOutcome::Aborted { reason });
        }
    }

    // Logical phase 2 (raft-memory clusters only): arm one empty-log rejoin per
    // group so leaders accept the amnesiac node back.
    if allow_empty {
        let snap = match client.fetch_cluster(nodes).await {
            Ok(snap) => snap,
            Err(err) => {
                clear_maintenance_drain(client, target).await;
                return Err(err).context("post-drain metrics");
            }
        };
        if let Err(err) = arm_empty_rejoin(nodes, target, client, &snap).await {
            clear_maintenance_drain(client, target).await;
            return Err(err);
        }
    }

    // Physical phase: execute the provider's restart command for this node.
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

    // Logical phase 3: progress-gated catch-up wait, then release the drain
    // mark and confirm the whole cluster settled.
    match wait_node_ready(
        nodes,
        target,
        client,
        &options.catch_up_options(),
        allow_empty,
    )
    .await?
    {
        CatchUpOutcome::Ready => {
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
            Ok(RestartOutcome::Restarted)
        }
        CatchUpOutcome::Stalled { reason } => {
            clear_maintenance_drain(client, target).await;
            Ok(RestartOutcome::Aborted { reason })
        }
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
}

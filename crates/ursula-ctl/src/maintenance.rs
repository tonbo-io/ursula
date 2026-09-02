//! Node maintenance verbs: drain, undrain, restart repair, and catch-up wait.
//!
//! These operate purely on Ursula's admin/metrics HTTP surface and never
//! execute anything on a host. Physical lifecycle (stopping and starting the
//! process) belongs to the platform that owns it: Kubernetes and Helm for pod
//! clusters, systemd for bare-metal hosts. A safe rolling restart runs these
//! verbs around the platform's own restart, one node at a time.

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
use crate::metrics::NodeMetricsView;
use crate::plan::DrainPlan;
use crate::plan::check_readiness;
use crate::plan::classify_amnesiac_voter;
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

/// Bounds for rebuilding one restarted voter through committed Raft
/// membership transitions.
#[derive(Debug, Clone)]
pub struct MembershipRepairOptions {
    /// Maximum number of independent Raft groups repaired concurrently.
    pub max_concurrency: usize,
    /// Maximum time spent waiting for one ambiguous HTTP operation attempt.
    pub operation_timeout: Duration,
    /// Maximum time spent reconciling one operation to its observed Raft
    /// postcondition. Timed-out requests are never assumed to have failed.
    pub operation_reconcile_timeout: Duration,
    /// Abort when learner catch-up makes no progress for this long.
    pub stall_timeout: Duration,
    /// Absolute backstop for learner catch-up.
    pub ready_timeout: Duration,
    /// Delay between learner progress samples.
    pub poll_interval: Duration,
}

/// Durable-for-the-maintenance-window description of a prepared restart.
///
/// `leader_anchor` is the one surviving voter that keeps every leadership
/// while the target process is replaced. Every other surviving voter is
/// maintenance-drained until [`finish_prepared_restart`] or
/// [`abort_prepared_restart`] releases the fence.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RestartPreparation {
    pub missing_group_count: usize,
    pub leader_anchor: u64,
    pub fenced_node_ids: Vec<u64>,
}

impl Default for MembershipRepairOptions {
    fn default() -> Self {
        Self {
            max_concurrency: 16,
            operation_timeout: Duration::from_secs(60),
            operation_reconcile_timeout: Duration::from_secs(300),
            stall_timeout: Duration::from_secs(300),
            ready_timeout: Duration::from_secs(1800),
            poll_interval: Duration::from_secs(2),
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

/// Quiesce an already-drained voter and pin every group leadership to one
/// surviving anchor until the platform has replaced and verified the voter.
///
/// The survivor fence keeps membership repair on a stable leader after the
/// replacement starts. It is part of every prepared restart, including
/// disk-WAL restarts, so the platform has one completion and abort contract.
pub async fn prepare_restart(
    nodes: &[NodeInfo],
    target: &NodeInfo,
    client: &MetricsClient,
    drain_options: &DrainOptions,
) -> Result<RestartPreparation> {
    if drain_options.dry_run {
        bail!("restart preparation does not support dry-run DrainOptions");
    }
    let configured_node_ids = recovery_inventory(nodes, target)?;
    client
        .quiesce_for_restart(target)
        .await
        .with_context(|| format!("quiesce node {} before restart", target.id))?;

    let prepared = async {
        let fence =
            pin_restart_leaders(nodes, target, client, drain_options, &configured_node_ids).await?;
        Ok::<_, anyhow::Error>(fence)
    }
    .await;
    match prepared {
        Ok(preparation) => Ok(preparation),
        Err(error) => {
            best_effort_abort_prepared_restart(nodes, target, client).await;
            Err(error)
        }
    }
}

fn restart_fence_layout(nodes: &[NodeInfo], target: &NodeInfo) -> Result<(u64, Vec<u64>)> {
    let mut survivor_ids = nodes
        .iter()
        .map(|node| node.id)
        .filter(|node_id| *node_id != target.id)
        .collect::<Vec<_>>();
    survivor_ids.sort_unstable();
    survivor_ids.dedup();
    if survivor_ids.len() + 1 != nodes.len() {
        bail!("restart fence requires an exact, unique configured voter inventory");
    }
    let leader_anchor = survivor_ids
        .first()
        .copied()
        .ok_or_else(|| anyhow!("restart fence requires at least one surviving voter"))?;
    let fenced_node_ids = survivor_ids.into_iter().skip(1).collect();
    Ok((leader_anchor, fenced_node_ids))
}

async fn pin_restart_leaders(
    nodes: &[NodeInfo],
    target: &NodeInfo,
    client: &MetricsClient,
    drain_options: &DrainOptions,
    configured_node_ids: &BTreeSet<u64>,
) -> Result<RestartPreparation> {
    let (leader_anchor, fenced_node_ids) = restart_fence_layout(nodes, target)?;
    let mut marked = Vec::new();
    for node_id in &fenced_node_ids {
        let node = nodes
            .iter()
            .find(|node| node.id == *node_id)
            .ok_or_else(|| anyhow!("restart fence node {node_id} is not configured"))?;
        if let Err(error) = client.set_maintenance_drain(node, true).await {
            for marked_id in &marked {
                if let Some(marked_node) = nodes.iter().find(|node| node.id == *marked_id) {
                    clear_maintenance_drain(client, marked_node).await;
                }
            }
            return Err(error)
                .with_context(|| format!("mark restart-fence node {node_id} drained"));
        }
        marked.push(*node_id);
    }

    let deadline = Instant::now() + drain_options.drain_timeout;
    let last_error = loop {
        let outcome = converge_restart_leader_fence_once(
            nodes,
            target,
            client,
            configured_node_ids,
            drain_options.lag_tolerance,
            leader_anchor,
            &fenced_node_ids,
        )
        .await;
        let error = match outcome {
            Ok(true) => {
                tracing::info!(
                    target_node_id = target.id,
                    leader_anchor,
                    fenced_nodes = ?fenced_node_ids,
                    "restart leaders pinned"
                );
                return Ok(RestartPreparation {
                    missing_group_count: 0,
                    leader_anchor,
                    fenced_node_ids,
                });
            }
            Ok(false) => anyhow!("restart-fence leadership transfers are still converging"),
            Err(error) => error,
        };
        if Instant::now() >= deadline {
            break error;
        }
        tokio::time::sleep(drain_options.poll_interval).await;
    };

    for node_id in &marked {
        if let Some(node) = nodes.iter().find(|node| node.id == *node_id) {
            clear_maintenance_drain(client, node).await;
        }
    }
    Err(last_error).context(format!(
        "restart leader fence did not converge on node {leader_anchor} before {:?}",
        drain_options.drain_timeout
    ))
}

async fn converge_restart_leader_fence_once(
    nodes: &[NodeInfo],
    target: &NodeInfo,
    client: &MetricsClient,
    configured_node_ids: &BTreeSet<u64>,
    lag_tolerance: u64,
    leader_anchor: u64,
    fenced_node_ids: &[u64],
) -> Result<bool> {
    let snapshot = client
        .fetch_cluster(nodes)
        .await
        .context("fetch cluster while pinning restart leaders")?;
    validate_surviving_voters(&snapshot, configured_node_ids, target.id, lag_tolerance)?;
    let group_ids = survivor_group_inventory(&snapshot, target.id);
    if group_ids.is_empty() {
        bail!(
            "no initialized group inventory remains while pinning restart leaders for node {}",
            target.id
        );
    }

    let mut transfers = Vec::new();
    for group_id in group_ids {
        match stable_non_target_leader(&snapshot, group_id, target.id)? {
            current_leader if current_leader == leader_anchor => {}
            current_leader if fenced_node_ids.contains(&current_leader) => {
                transfers.push((group_id, current_leader));
            }
            current_leader => bail!(
                "group {group_id} is led by unexpected node {current_leader} while pinning restart anchor {leader_anchor}"
            ),
        }
    }
    if transfers.is_empty() {
        return Ok(true);
    }
    for (group_id, current_leader) in transfers {
        let leader = nodes
            .iter()
            .find(|node| node.id == current_leader)
            .expect("leader plan only contains configured nodes");
        let response = client
            .transfer_leader(leader, group_id, leader_anchor)
            .await?;
        if !response.transferred {
            bail!(
                "restart-fence transfer for group {group_id} from {current_leader} to {leader_anchor} was rejected: {}",
                response.reason.unwrap_or_else(|| "unknown".to_owned())
            );
        }
    }
    Ok(false)
}

fn survivor_group_inventory(snap: &ClusterSnapshot, target_node_id: u64) -> BTreeSet<u64> {
    snap.per_node
        .iter()
        .filter(|view| view.node.id != target_node_id)
        .flat_map(|view| &view.groups)
        .filter(|group| !group.voter_ids.is_empty())
        .map(|group| group.raft_group_id)
        .collect()
}

async fn release_prepared_restart(
    nodes: &[NodeInfo],
    target: &NodeInfo,
    client: &MetricsClient,
    include_target: bool,
) -> Result<()> {
    let (_, fenced_node_ids) = restart_fence_layout(nodes, target)?;
    let mut release_ids = fenced_node_ids;
    if include_target {
        release_ids.insert(0, target.id);
    }
    let mut errors = Vec::new();
    for node_id in release_ids {
        let node = nodes
            .iter()
            .find(|node| node.id == node_id)
            .expect("restart fence contains configured nodes");
        if let Err(error) = client.set_maintenance_drain(node, false).await {
            errors.push(format!("node {node_id}: {error}"));
        }
    }
    if errors.is_empty() {
        Ok(())
    } else {
        bail!(
            "release prepared restart fence failed: {}",
            errors.join("; ")
        )
    }
}

/// Clear the target drain and every survivor fence after catch-up succeeded.
pub async fn finish_prepared_restart(
    nodes: &[NodeInfo],
    target: &NodeInfo,
    client: &MetricsClient,
) -> Result<()> {
    release_prepared_restart(nodes, target, client, true).await
}

/// Release survivor fences after a failed replacement while leaving the
/// uncertain target drained and unable to acquire leadership.
pub async fn abort_prepared_restart(
    nodes: &[NodeInfo],
    target: &NodeInfo,
    client: &MetricsClient,
) -> Result<()> {
    release_prepared_restart(nodes, target, client, false).await
}

async fn best_effort_abort_prepared_restart(
    nodes: &[NodeInfo],
    target: &NodeInfo,
    client: &MetricsClient,
) {
    if let Err(error) = abort_prepared_restart(nodes, target, client).await {
        tracing::warn!(target_node_id = target.id, %error, "failed to release restart survivor fence");
    }
}

/// Prepare exactly one partially amnesiac memory-WAL voter for a platform
/// restart. This recovery path is deliberately narrower than ordinary drain:
/// the target may be missing whole groups, but every other configured voter
/// must be complete, mutually consistent, and caught up for every group.
///
/// On success the maintenance-drain mark remains set and the target's Raft
/// cores are stopped. The caller must restart that node, rebuild missing groups
/// with [`repair_restarted_voter`], wait for catch-up, and then call
/// [`finish_prepared_restart`].
pub async fn prepare_amnesiac_restart(
    nodes: &[NodeInfo],
    target: &NodeInfo,
    client: &MetricsClient,
    drain_options: &DrainOptions,
) -> Result<RestartPreparation> {
    if drain_options.dry_run {
        bail!("amnesiac restart preparation does not support dry-run DrainOptions");
    }
    let configured_node_ids = nodes.iter().map(|node| node.id).collect::<Vec<_>>();
    let configured_node_id_set = configured_node_ids.iter().copied().collect::<BTreeSet<_>>();
    let initial = client.fetch_cluster(nodes).await?;
    let candidate =
        classify_amnesiac_voter(&initial, &configured_node_ids, drain_options.lag_tolerance)
            .map_err(anyhow::Error::msg)?
            .ok_or_else(|| {
                anyhow!("cluster is fully ready; amnesiac recovery is not applicable")
            })?;
    if candidate.node_id != target.id {
        bail!(
            "node {} is not the uniquely recoverable amnesiac voter (node {} is)",
            target.id,
            candidate.node_id
        );
    }
    if resolve_restart_wal_backend(client, nodes).await? != RestartWalBackend::Memory {
        bail!("amnesiac restart preparation requires a memory-WAL cluster");
    }

    client
        .set_maintenance_drain(target, true)
        .await
        .with_context(|| format!("mark maintenance-drain on amnesiac node {}", target.id))?;
    let prepared = async {
        let deadline = Instant::now() + drain_options.drain_timeout;
        loop {
            let snapshot = client.fetch_cluster(nodes).await?;
            let current = classify_amnesiac_voter(
                &snapshot,
                &configured_node_ids,
                drain_options.lag_tolerance,
            )
            .map_err(anyhow::Error::msg)?
            .ok_or_else(|| anyhow!("cluster changed while preparing amnesiac recovery"))?;
            if current.node_id != target.id {
                bail!(
                    "amnesiac recovery target changed from node {} to node {}",
                    target.id,
                    current.node_id
                );
            }
            let still_leads = snapshot.groups_reported_led_by(target.id);
            if still_leads.is_empty() {
                break;
            }
            let plan = plan_drain(&snapshot, target.id);
            if plan.transfers.len() != still_leads.len() {
                bail!(
                    "amnesiac node {} still leads {} group(s), but only {} safe transfer(s) exist",
                    target.id,
                    still_leads.len(),
                    plan.transfers.len()
                );
            }
            transfer_drain_plan(target, client, &plan).await?;
            if Instant::now() >= deadline {
                bail!(
                    "amnesiac drain timeout: node {} still leads {} group(s) after {:?}",
                    target.id,
                    still_leads.len(),
                    drain_options.drain_timeout
                );
            }
            tokio::time::sleep(drain_options.poll_interval).await;
        }
        client
            .quiesce_for_restart(target)
            .await
            .with_context(|| format!("quiesce amnesiac node {} before replacement", target.id))?;
        let fence = pin_restart_leaders(
            nodes,
            target,
            client,
            drain_options,
            &configured_node_id_set,
        )
        .await?;
        Ok::<_, anyhow::Error>(RestartPreparation {
            missing_group_count: candidate.missing_group_ids.len(),
            leader_anchor: fence.leader_anchor,
            fenced_node_ids: fence.fenced_node_ids,
        })
    }
    .await;
    match prepared {
        Ok(preparation) => Ok(preparation),
        Err(error) => {
            best_effort_abort_prepared_restart(nodes, target, client).await;
            clear_maintenance_drain(client, target).await;
            Err(error)
        }
    }
}

/// Rebuild a restarted or partially recovered voter through committed Raft
/// membership transitions.
///
/// The target remains maintenance-drained throughout. Every unready group is
/// first reduced to the complete surviving voter set, which discards any
/// stale replication progress for the target. The target is then attached as
/// a blocking learner and promoted only after it has caught up. The operation
/// is idempotent across a mixture of full-voter, detached, and learner states,
/// so a later rollout Job can resume a partially completed 256-group repair.
pub async fn repair_restarted_voter(
    nodes: &[NodeInfo],
    target: &NodeInfo,
    client: &MetricsClient,
    drain_options: &DrainOptions,
    repair_options: &MembershipRepairOptions,
) -> Result<RestartPreparation> {
    if drain_options.dry_run {
        bail!("restarted voter repair does not support dry-run DrainOptions");
    }
    if repair_options.max_concurrency == 0 {
        bail!("membership repair max_concurrency must be positive");
    }
    if repair_options.operation_timeout.is_zero()
        || repair_options.operation_reconcile_timeout.is_zero()
    {
        bail!("membership repair operation timeouts must be positive");
    }
    let configured_node_ids = recovery_inventory(nodes, target)?;
    let survivor_node_ids = configured_node_ids
        .iter()
        .copied()
        .filter(|node_id| *node_id != target.id)
        .collect::<BTreeSet<_>>();

    // A partial replacement may have caught up far enough to receive a
    // leadership just before recovery starts. Maintenance drain prevents new
    // campaigns and inbound transfers, but does not itself move a current
    // leader. Explicitly drain the target before fencing the other survivor,
    // otherwise the anchor can never become the sole leader authority.
    drain_recovery_target(nodes, target, client, drain_options, &configured_node_ids)
        .await
        .context("drain current leaders from restarted voter before membership repair")?;
    stabilize_survivor_leadership(nodes, target, client, drain_options, &configured_node_ids)
        .await
        .context("stabilize surviving voter terms before membership repair")?;
    let fence =
        pin_restart_leaders(nodes, target, client, drain_options, &configured_node_ids).await?;
    let initial = client.fetch_cluster(nodes).await?;
    validate_surviving_voters(
        &initial,
        &configured_node_ids,
        target.id,
        drain_options.lag_tolerance,
    )?;
    let anchor_view = initial.node(fence.leader_anchor).ok_or_else(|| {
        anyhow!(
            "restart anchor {} did not report metrics",
            fence.leader_anchor
        )
    })?;
    let readiness = check_readiness(&initial, target.id, drain_options.lag_tolerance);
    let mut work_groups = BTreeSet::new();
    for group_id in survivor_group_inventory(&initial, target.id) {
        let anchor_group = anchor_view.group(group_id).ok_or_else(|| {
            anyhow!(
                "restart anchor {} does not host group {}",
                fence.leader_anchor,
                group_id
            )
        })?;
        let anchor_voters = anchor_group
            .voter_ids
            .iter()
            .copied()
            .collect::<BTreeSet<_>>();
        let target_ready = readiness
            .per_group
            .get(&group_id)
            .is_some_and(|group| group.ready);
        if !target_ready
            || anchor_voters != configured_node_ids
            || anchor_group.learner_ids.contains(&target.id)
        {
            work_groups.insert(group_id);
        }
    }
    if work_groups.is_empty() {
        return Ok(fence);
    }

    let mut detach_groups = Vec::new();
    for group_id in &work_groups {
        let group = anchor_view.group(*group_id).ok_or_else(|| {
            anyhow!(
                "restart anchor {} does not host group {}",
                fence.leader_anchor,
                group_id
            )
        })?;
        let voters = group.voter_ids.iter().copied().collect::<BTreeSet<_>>();
        if voters == configured_node_ids {
            detach_groups.push(*group_id);
        } else if voters != survivor_node_ids {
            bail!(
                "group {} has unexpected voter set {:?} during node {} repair",
                group_id,
                voters,
                target.id
            );
        }
    }
    let survivor_voters = &survivor_node_ids;
    run_group_phase(
        detach_groups,
        repair_options.max_concurrency,
        move |group_id| {
            reconcile_group_operation(
                nodes,
                target.id,
                client,
                group_id,
                repair_options,
                "detach restarted voter",
                move |group| {
                    group
                        .voter_ids
                        .iter()
                        .copied()
                        .collect::<BTreeSet<_>>()
                        .eq(survivor_voters)
                },
                GroupOperation::ChangeMembership(survivor_voters),
            )
        },
    )
    .await
    .context("detach restarted voter from unready groups")?;

    let detached = client.fetch_cluster(nodes).await?;
    validate_surviving_voters(
        &detached,
        &configured_node_ids,
        target.id,
        drain_options.lag_tolerance,
    )?;
    let anchor_view = detached.node(fence.leader_anchor).ok_or_else(|| {
        anyhow!(
            "restart anchor {} disappeared after voter detach",
            fence.leader_anchor
        )
    })?;
    let mut attach_groups = Vec::new();
    for group_id in &work_groups {
        let group = anchor_view.group(*group_id).ok_or_else(|| {
            anyhow!(
                "restart anchor {} lost group {} after voter detach",
                fence.leader_anchor,
                group_id
            )
        })?;
        let voters = group.voter_ids.iter().copied().collect::<BTreeSet<_>>();
        if voters != survivor_node_ids {
            bail!(
                "group {} did not converge to survivor voter set {:?}: {:?}",
                group_id,
                survivor_node_ids,
                voters
            );
        }
        if !group.learner_ids.contains(&target.id) {
            attach_groups.push(*group_id);
        }
    }
    run_group_phase(
        attach_groups,
        repair_options.max_concurrency,
        move |group_id| {
            reconcile_group_operation(
                nodes,
                target.id,
                client,
                group_id,
                repair_options,
                "attach restarted voter as a learner",
                |group| group.learner_ids.contains(&target.id),
                GroupOperation::AddLearner(target),
            )
        },
    )
    .await
    .context("attach restarted voter as a learner")?;

    wait_repair_learners_caught_up(
        nodes,
        target,
        client,
        fence.leader_anchor,
        &work_groups,
        drain_options.lag_tolerance,
        repair_options,
    )
    .await?;

    let configured_voters = &configured_node_ids;
    run_group_phase(
        work_groups.iter().copied().collect(),
        repair_options.max_concurrency,
        move |group_id| {
            reconcile_group_operation(
                nodes,
                target.id,
                client,
                group_id,
                repair_options,
                "promote caught-up learner back to voter",
                move |group| {
                    group
                        .voter_ids
                        .iter()
                        .copied()
                        .collect::<BTreeSet<_>>()
                        .eq(configured_voters)
                },
                GroupOperation::ChangeMembership(configured_voters),
            )
        },
    )
    .await
    .context("promote caught-up learner back to voter")?;

    Ok(RestartPreparation {
        missing_group_count: work_groups.len(),
        ..fence
    })
}

#[derive(Clone, Copy)]
enum GroupOperation<'a> {
    ChangeMembership(&'a BTreeSet<u64>),
    AddLearner(&'a NodeInfo),
}

impl GroupOperation<'_> {
    async fn execute(self, client: &MetricsClient, leader: &NodeInfo, group_id: u64) -> Result<()> {
        match self {
            Self::ChangeMembership(voters) => {
                client.change_membership(leader, group_id, voters).await
            }
            Self::AddLearner(target) => client.add_learner(leader, group_id, target).await,
        }
    }
}

/// Reconcile an HTTP operation to a committed Raft postcondition.
///
/// Dropping an HTTP request at a timeout boundary does not establish whether
/// OpenRaft committed the operation. Leadership can also change while a
/// membership operation commits, so discover the current self-reporting
/// non-target leader before every retry and observe its durable group state.
async fn reconcile_group_operation<P>(
    nodes: &[NodeInfo],
    target_node_id: u64,
    client: &MetricsClient,
    group_id: u64,
    options: &MembershipRepairOptions,
    operation_name: &str,
    postcondition: P,
    operation: GroupOperation<'_>,
) -> Result<()>
where
    P: Fn(&crate::metrics::RaftGroupView) -> bool,
{
    let deadline = Instant::now() + options.operation_reconcile_timeout;
    let mut last_error = anyhow!("postcondition has not been observed");
    let mut saw_ambiguous_result = false;
    loop {
        let leader = match client.fetch_cluster(nodes).await {
            Ok(snapshot) => match stable_non_target_leader(&snapshot, group_id, target_node_id) {
                Ok(leader_id) => {
                    let leader = nodes
                        .iter()
                        .find(|node| node.id == leader_id)
                        .expect("observed leader is a configured node");
                    let group = snapshot
                        .node(leader_id)
                        .and_then(|view| view.group(group_id))
                        .expect("stable leader was selected from its own group view");
                    if postcondition(group) {
                        if saw_ambiguous_result {
                            tracing::info!(
                                leader_node_id = leader.id,
                                raft_group_id = group_id,
                                operation = operation_name,
                                "Raft postcondition resolved an ambiguous HTTP operation"
                            );
                        }
                        return Ok(());
                    }
                    leader
                }
                Err(error) => {
                    last_error = error.context(format!(
                        "discover a stable leader while reconciling {operation_name} for group {group_id}"
                    ));
                    if Instant::now() >= deadline {
                        break;
                    }
                    tokio::time::sleep(options.poll_interval).await;
                    continue;
                }
            },
            Err(error) => {
                last_error = error.context(format!(
                    "observe {operation_name} postcondition across the cluster for group {group_id}"
                ));
                if Instant::now() >= deadline {
                    break;
                }
                tokio::time::sleep(options.poll_interval).await;
                continue;
            }
        };

        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            break;
        }
        let attempt_timeout = options.operation_timeout.min(remaining);
        last_error = match tokio::time::timeout(
            attempt_timeout,
            operation.execute(client, leader, group_id),
        )
        .await
        {
            Ok(Ok(())) => anyhow!("HTTP operation returned success before its postcondition"),
            Ok(Err(error)) => {
                saw_ambiguous_result = true;
                error
            }
            Err(_) => {
                saw_ambiguous_result = true;
                anyhow!("HTTP operation attempt exceeded {attempt_timeout:?}")
            }
        };
        if saw_ambiguous_result {
            tracing::warn!(
                leader_node_id = leader.id,
                raft_group_id = group_id,
                operation = operation_name,
                error = %last_error,
                "Raft HTTP result is ambiguous; reconciling its postcondition before retry"
            );
        }
        if Instant::now() >= deadline {
            break;
        }
        tokio::time::sleep(options.poll_interval).await;
    }
    Err(last_error).context(format!(
        "{operation_name} did not converge for group {group_id} within {:?}",
        options.operation_reconcile_timeout
    ))
}

async fn wait_repair_learners_caught_up(
    nodes: &[NodeInfo],
    target: &NodeInfo,
    client: &MetricsClient,
    leader_anchor: u64,
    group_ids: &BTreeSet<u64>,
    lag_tolerance: u64,
    options: &MembershipRepairOptions,
) -> Result<()> {
    let ceiling = Instant::now() + options.ready_timeout;
    let mut last_advance = Instant::now();
    let mut best = LearnerRepairProgress::default();
    loop {
        let snapshot = client.fetch_cluster(nodes).await?;
        let anchor = snapshot.node(leader_anchor).ok_or_else(|| {
            anyhow!("restart anchor {leader_anchor} disappeared during learner catch-up")
        })?;
        let target_view = snapshot.node(target.id).ok_or_else(|| {
            anyhow!(
                "restarted node {} disappeared during learner catch-up",
                target.id
            )
        })?;
        let mut progress = LearnerRepairProgress::default();
        let mut pending = Vec::new();
        for group_id in group_ids {
            let anchor_group = anchor.group(*group_id).ok_or_else(|| {
                anyhow!(
                    "restart anchor {leader_anchor} lost group {group_id} during learner catch-up"
                )
            })?;
            if !anchor_group.learner_ids.contains(&target.id) {
                bail!(
                    "group {group_id} does not contain restarted node {} as a learner",
                    target.id
                );
            }
            progress.attached_groups += 1;
            let target_applied = target_view
                .group(*group_id)
                .and_then(|group| group.last_applied_index);
            if let Some(applied) = target_applied {
                progress.applied_sum += u128::from(applied);
            }
            let peer_committed = snapshot
                .peer_views(*group_id, target.id)
                .values()
                .filter_map(|group| group.committed_index)
                .max();
            let caught_up =
                peer_committed
                    .zip(target_applied)
                    .is_some_and(|(committed, applied)| {
                        committed.saturating_sub(applied) <= lag_tolerance
                    });
            if caught_up {
                progress.caught_up_groups += 1;
            } else {
                pending.push(format!(
                    "group {group_id}: applied={target_applied:?} peer_committed={peer_committed:?}"
                ));
            }
        }
        if progress.caught_up_groups == group_ids.len() {
            return Ok(());
        }
        let now = Instant::now();
        if progress > best {
            best = progress;
            last_advance = now;
        }
        if now.duration_since(last_advance) >= options.stall_timeout {
            bail!(
                "learner catch-up made no progress for {:?}: {}",
                options.stall_timeout,
                pending.join("; ")
            );
        }
        if now >= ceiling {
            bail!(
                "learner catch-up exceeded {:?}: {}",
                options.ready_timeout,
                pending.join("; ")
            );
        }
        tokio::time::sleep(options.poll_interval).await;
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord)]
struct LearnerRepairProgress {
    caught_up_groups: usize,
    attached_groups: usize,
    applied_sum: u128,
}

async fn run_group_phase<F, Fut>(
    group_ids: Vec<u64>,
    max_concurrency: usize,
    operation: F,
) -> Result<()>
where
    F: Fn(u64) -> Fut,
    Fut: std::future::Future<Output = Result<()>>,
{
    futures_util::stream::iter(group_ids.into_iter().map(operation))
        .buffer_unordered(max_concurrency)
        .try_collect::<Vec<()>>()
        .await?;
    Ok(())
}

/// Drain a partial memory-WAL replacement without calling restart quiescence.
///
/// This is the narrow upgrade bridge for a replacement process that predates
/// `/__ursula/raft/quiesce-for-restart`. Every surviving voter is validated
/// before the target loses leadership. On success its process-local drain
/// fence intentionally remains set; the platform must immediately replace it
/// with a binary that supports [`repair_restarted_voter`].
pub async fn prepare_recovery_handoff(
    nodes: &[NodeInfo],
    target: &NodeInfo,
    client: &MetricsClient,
    drain_options: &DrainOptions,
) -> Result<usize> {
    if drain_options.dry_run {
        bail!("recovery handoff does not support dry-run DrainOptions");
    }
    let configured_node_ids = recovery_inventory(nodes, target)?;
    if resolve_restart_wal_backend(client, nodes).await? != RestartWalBackend::Memory {
        bail!("recovery handoff is only applicable to a memory-WAL cluster");
    }
    let prepared =
        drain_recovery_target(nodes, target, client, drain_options, &configured_node_ids).await;
    match prepared {
        Ok(snapshot) => {
            Ok(wholly_missing_groups(&snapshot, target.id, drain_options.lag_tolerance).len())
        }
        Err(error) => {
            clear_maintenance_drain(client, target).await;
            Err(error)
        }
    }
}

fn recovery_inventory(nodes: &[NodeInfo], target: &NodeInfo) -> Result<BTreeSet<u64>> {
    let configured_node_ids = nodes.iter().map(|node| node.id).collect::<BTreeSet<_>>();
    if configured_node_ids.len() != nodes.len() || !configured_node_ids.contains(&target.id) {
        bail!("recovery restart requires an exact, unique configured voter inventory");
    }
    Ok(configured_node_ids)
}

async fn drain_recovery_target(
    nodes: &[NodeInfo],
    target: &NodeInfo,
    client: &MetricsClient,
    drain_options: &DrainOptions,
    configured_node_ids: &BTreeSet<u64>,
) -> Result<ClusterSnapshot> {
    client
        .set_maintenance_drain(target, true)
        .await
        .with_context(|| format!("mark maintenance-drain on recovery node {}", target.id))?;
    let deadline = Instant::now() + drain_options.drain_timeout;
    loop {
        let snapshot = client.fetch_cluster(nodes).await?;
        validate_surviving_voters(
            &snapshot,
            configured_node_ids,
            target.id,
            drain_options.lag_tolerance,
        )?;
        let still_leads = snapshot.groups_reported_led_by(target.id);
        if still_leads.is_empty() {
            return Ok(snapshot);
        }
        let plan = plan_drain(&snapshot, target.id);
        if plan.transfers.len() != still_leads.len() {
            bail!(
                "restarted node {} still leads {} group(s), but only {} safe transfer(s) exist",
                target.id,
                still_leads.len(),
                plan.transfers.len()
            );
        }
        transfer_drain_plan(target, client, &plan).await?;
        if Instant::now() >= deadline {
            bail!(
                "recovery restart timeout: node {} still leads {} group(s) after {:?}",
                target.id,
                still_leads.len(),
                drain_options.drain_timeout
            );
        }
        tokio::time::sleep(drain_options.poll_interval).await;
    }
}

fn validate_surviving_voters(
    snapshot: &ClusterSnapshot,
    configured_node_ids: &BTreeSet<u64>,
    target_node_id: u64,
    lag_tolerance: u64,
) -> Result<()> {
    let reported = snapshot
        .per_node
        .iter()
        .map(|view| view.node.id)
        .collect::<BTreeSet<_>>();
    if &reported != configured_node_ids {
        bail!(
            "recovery restart voter inventory differs: configured={configured_node_ids:?} reported={reported:?}"
        );
    }
    for node_id in configured_node_ids {
        if *node_id != target_node_id
            && !check_readiness(snapshot, *node_id, lag_tolerance).all_ready
        {
            bail!("surviving voter {node_id} is not complete and caught up");
        }
    }
    Ok(())
}

fn wholly_missing_groups(
    snapshot: &ClusterSnapshot,
    target_node_id: u64,
    lag_tolerance: u64,
) -> BTreeSet<u64> {
    check_readiness(snapshot, target_node_id, lag_tolerance)
        .per_group
        .into_values()
        .filter(|group| !group.ready && !group.voter_member && group.target_applied_index.is_none())
        .map(|group| group.raft_group_id)
        .collect()
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

/// Resolve the cluster WAL backend without guessing when a server is too old
/// or unreachable to report it. Recovery decisions fail closed on incomplete,
/// mixed, or unsupported reports.
async fn resolve_restart_wal_backend(
    client: &MetricsClient,
    nodes: &[NodeInfo],
) -> Result<RestartWalBackend> {
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
    decide_restart_wal_backend(&backends, nodes.len())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RestartWalBackend {
    Memory,
    Disk,
}

fn decide_restart_wal_backend(
    backends: &[Option<&str>],
    expected_nodes: usize,
) -> Result<RestartWalBackend> {
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
             refusing to guess the restart recovery contract"
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
        (memory, 0) if memory == expected_nodes => Ok(RestartWalBackend::Memory),
        (0, disk) if disk == expected_nodes => Ok(RestartWalBackend::Disk),
        _ => bail!(
            "cannot prepare an automated restart because nodes report inconsistent or unsupported \
             WAL backends: {backends:?}"
        ),
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
         replacement process started and that durable membership repair \
         attached it as a learner before promotion",
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
            bail!(
                "leader transfer rejected for group {}: {}",
                transfer.raft_group_id,
                resp.reason.unwrap_or_else(|| "unknown".into())
            );
        }
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SurvivorTermHandoff {
    raft_group_id: u64,
    stale_leader: u64,
    stale_term: u64,
    higher_term_voter: u64,
    higher_term: u64,
}

async fn stabilize_survivor_leadership(
    nodes: &[NodeInfo],
    target: &NodeInfo,
    client: &MetricsClient,
    drain_options: &DrainOptions,
    configured_node_ids: &BTreeSet<u64>,
) -> Result<()> {
    let deadline = Instant::now() + drain_options.drain_timeout;
    let mut last_error = anyhow!("surviving voter leadership has not converged");
    let mut standard_handoff_attempted = BTreeSet::new();
    loop {
        let snapshot = client
            .fetch_cluster(nodes)
            .await
            .context("fetch cluster while stabilizing surviving voter leadership")?;
        validate_surviving_voters(
            &snapshot,
            configured_node_ids,
            target.id,
            drain_options.lag_tolerance,
        )?;
        let group_ids = survivor_group_inventory(&snapshot, target.id);
        if group_ids.is_empty() {
            bail!(
                "no initialized group inventory remains while stabilizing survivors for node {}",
                target.id
            );
        }

        let mut all_stable = true;
        let mut handoffs = Vec::new();
        for group_id in group_ids {
            match stable_non_target_leader(&snapshot, group_id, target.id) {
                Ok(_) => {}
                Err(error) => {
                    all_stable = false;
                    last_error = error;
                    if let Some(handoff) =
                        stale_survivor_term_handoff(&snapshot, group_id, target.id)?
                    {
                        handoffs.push(handoff);
                    }
                }
            }
        }
        if all_stable {
            return Ok(());
        }

        for handoff in handoffs {
            if standard_handoff_attempted.insert(handoff.raft_group_id) {
                let stale_leader = nodes
                    .iter()
                    .find(|node| node.id == handoff.stale_leader)
                    .expect("term handoff leader is a configured node");
                tracing::warn!(
                    raft_group_id = handoff.raft_group_id,
                    stale_leader = handoff.stale_leader,
                    stale_term = handoff.stale_term,
                    higher_term_voter = handoff.higher_term_voter,
                    higher_term = handoff.higher_term,
                    "requesting the standard Raft leader transfer before using the term bridge"
                );
                match client
                    .transfer_leader(
                        stale_leader,
                        handoff.raft_group_id,
                        handoff.higher_term_voter,
                    )
                    .await
                {
                    Ok(response) if response.transferred => {}
                    Ok(response) => {
                        last_error = anyhow!(
                            "standard survivor term handoff for group {} was rejected: {}",
                            handoff.raft_group_id,
                            response.reason.unwrap_or_else(|| "unknown".to_owned())
                        );
                    }
                    Err(error) => {
                        last_error = error.context(format!(
                            "trigger standard survivor term handoff for group {}",
                            handoff.raft_group_id
                        ));
                    }
                }
                continue;
            }
            let higher_term_voter = nodes
                .iter()
                .find(|node| node.id == handoff.higher_term_voter)
                .expect("term handoff voter is a configured node");
            let fresh_higher_view = match client.fetch_node(higher_term_voter).await {
                Ok(view) => view,
                Err(error) => {
                    last_error = error.context(format!(
                        "refresh higher-term voter {} before survivor term handoff for group {}",
                        handoff.higher_term_voter, handoff.raft_group_id
                    ));
                    continue;
                }
            };
            let handoff = match refreshed_survivor_term_handoff(
                &snapshot,
                &fresh_higher_view,
                handoff,
                target.id,
            ) {
                Ok(handoff) => handoff,
                Err(error) => {
                    last_error = error.context(format!(
                        "revalidate survivor term handoff for group {}",
                        handoff.raft_group_id
                    ));
                    continue;
                }
            };
            tracing::warn!(
                raft_group_id = handoff.raft_group_id,
                stale_leader = handoff.stale_leader,
                stale_term = handoff.stale_term,
                higher_term_voter = handoff.higher_term_voter,
                higher_term = handoff.higher_term,
                "asking the caught-up higher-term voter to start a Raft-native election"
            );
            match client
                .request_self_election(
                    higher_term_voter,
                    handoff.raft_group_id,
                    handoff.higher_term,
                )
                .await
            {
                Ok(()) => {}
                Err(error) => {
                    last_error = error.context(format!(
                        "trigger survivor term handoff for group {}",
                        handoff.raft_group_id
                    ));
                }
            }
        }

        if Instant::now() >= deadline {
            break;
        }
        tokio::time::sleep(drain_options.poll_interval).await;
    }
    Err(last_error).context(format!(
        "surviving voter leadership did not stabilize before {:?}",
        drain_options.drain_timeout
    ))
}

fn refreshed_survivor_term_handoff(
    snapshot: &ClusterSnapshot,
    fresh_higher_view: &NodeMetricsView,
    expected: SurvivorTermHandoff,
    target_node_id: u64,
) -> Result<SurvivorTermHandoff> {
    if fresh_higher_view.node.id != expected.higher_term_voter {
        bail!(
            "refreshed survivor term handoff for group {} returned node {}, expected higher-term voter {}",
            expected.raft_group_id,
            fresh_higher_view.node.id,
            expected.higher_term_voter
        );
    }
    let stale_view = snapshot.node(expected.stale_leader).ok_or_else(|| {
        anyhow!(
            "stale leader {} disappeared while refreshing survivor term handoff for group {}",
            expected.stale_leader,
            expected.raft_group_id
        )
    })?;
    let refreshed_snapshot = ClusterSnapshot {
        per_node: vec![stale_view.clone(), fresh_higher_view.clone()],
    };
    let refreshed =
        stale_survivor_term_handoff(&refreshed_snapshot, expected.raft_group_id, target_node_id)?
            .ok_or_else(|| {
            anyhow!(
                "group {} no longer has the validated stale-leader/higher-term-voter shape",
                expected.raft_group_id
            )
        })?;
    if refreshed.stale_leader != expected.stale_leader
        || refreshed.higher_term_voter != expected.higher_term_voter
    {
        bail!(
            "group {} survivor term handoff identity changed from stale leader {}/higher voter {} to {}/{}",
            expected.raft_group_id,
            expected.stale_leader,
            expected.higher_term_voter,
            refreshed.stale_leader,
            refreshed.higher_term_voter
        );
    }
    Ok(refreshed)
}

fn stale_survivor_term_handoff(
    snap: &ClusterSnapshot,
    raft_group_id: u64,
    target_node_id: u64,
) -> Result<Option<SurvivorTermHandoff>> {
    let mut self_leader = None;
    let mut max_term_voter = None;
    for view in snap
        .per_node
        .iter()
        .filter(|view| view.node.id != target_node_id)
    {
        let Some(group) = view.group(raft_group_id) else {
            continue;
        };
        let current_term = group.current_term.ok_or_else(|| {
            anyhow!(
                "group {} on surviving node {} does not report current_term",
                raft_group_id,
                view.node.id
            )
        })?;
        if group.voter_ids.contains(&view.node.id)
            && max_term_voter.is_none_or(|(_, term, _)| current_term > term)
        {
            max_term_voter = Some((view.node.id, current_term, group));
        }
        if group.current_leader == Some(view.node.id) {
            if self_leader.is_some() {
                // A successful term handoff can be visible on the new leader
                // before the old leader processes the higher-term append and
                // steps down. This is not a safe handoff shape, but it is also
                // not terminal: the stabilization loop must keep the target
                // drained and wait for one authoritative survivor leader.
                return Ok(None);
            }
            self_leader = Some((view.node.id, current_term, group));
        }
    }
    let (stale_leader, stale_term, stale_group) = match self_leader {
        Some(leader) => leader,
        None => return Ok(None),
    };
    let (higher_term_voter, higher_term, higher_group) = max_term_voter.ok_or_else(|| {
        anyhow!(
            "group {} has no surviving voter term while repairing node {}",
            raft_group_id,
            target_node_id
        )
    })?;
    if stale_term >= higher_term {
        return Ok(None);
    }
    if !stale_group.voter_ids.contains(&higher_term_voter) {
        bail!(
            "group {} stale leader {} does not list higher-term node {} as a voter",
            raft_group_id,
            stale_leader,
            higher_term_voter
        );
    }
    if higher_group.current_leader.is_some() {
        bail!(
            "group {} higher-term voter {} still reports leader {:?}; refusing an inferred self-election",
            raft_group_id,
            higher_term_voter,
            higher_group.current_leader
        );
    }
    if higher_group.voter_ids != stale_group.voter_ids {
        bail!(
            "group {} survivor voter sets differ during term handoff: stale leader {} has {:?}, higher-term voter {} has {:?}",
            raft_group_id,
            stale_leader,
            stale_group.voter_ids,
            higher_term_voter,
            higher_group.voter_ids
        );
    }
    let stale_committed = stale_group.committed_index.ok_or_else(|| {
        anyhow!(
            "group {} stale leader {} does not report committed_index",
            raft_group_id,
            stale_leader
        )
    })?;
    let higher_committed = higher_group.committed_index.ok_or_else(|| {
        anyhow!(
            "group {} higher-term voter {} does not report committed_index",
            raft_group_id,
            higher_term_voter
        )
    })?;
    let stale_applied = stale_group.last_applied_index.ok_or_else(|| {
        anyhow!(
            "group {} stale leader {} does not report last_applied_index",
            raft_group_id,
            stale_leader
        )
    })?;
    let higher_applied = higher_group.last_applied_index.ok_or_else(|| {
        anyhow!(
            "group {} higher-term voter {} does not report last_applied_index",
            raft_group_id,
            higher_term_voter
        )
    })?;
    if higher_committed < stale_committed || higher_applied < stale_applied {
        bail!(
            "group {} higher-term voter {} is behind stale leader {} (committed {} < {} or applied {} < {}); refusing election handoff",
            raft_group_id,
            higher_term_voter,
            stale_leader,
            higher_committed,
            stale_committed,
            higher_applied,
            stale_applied
        );
    }
    Ok(Some(SurvivorTermHandoff {
        raft_group_id,
        stale_leader,
        stale_term,
        higher_term_voter,
        higher_term,
    }))
}

fn stable_non_target_leader(
    snap: &ClusterSnapshot,
    raft_group_id: u64,
    target_node_id: u64,
) -> Result<u64> {
    let mut leader = None;
    let mut leader_term = None;
    let mut max_survivor_term = None;
    for view in snap
        .per_node
        .iter()
        .filter(|view| view.node.id != target_node_id)
    {
        let Some(group) = view.group(raft_group_id) else {
            continue;
        };
        let current_term = group.current_term.ok_or_else(|| {
            anyhow!(
                "group {} on surviving node {} does not report current_term",
                raft_group_id,
                view.node.id
            )
        })?;
        max_survivor_term =
            Some(max_survivor_term.map_or(current_term, |term: u64| term.max(current_term)));
        let Some(candidate) = group.current_leader else {
            continue;
        };
        // A follower's current_leader observation may remain stale across an
        // otherwise completed transfer. The target is quiesced or drained, so
        // its frozen view is especially unsuitable as an authority. A
        // leader's own view is the narrow authoritative signal for the next
        // membership mutation.
        if candidate != view.node.id {
            continue;
        }
        if let Some(existing) = leader {
            if existing != candidate {
                bail!(
                    "multiple peers self-report leadership for group {} while repairing node {}: {} vs {}",
                    raft_group_id,
                    target_node_id,
                    existing,
                    candidate
                );
            }
        } else {
            leader = Some(candidate);
            leader_term = Some(current_term);
        }
    }
    let leader = leader.ok_or_else(|| {
        anyhow!(
            "group {} has no stable non-target leader while repairing node {}",
            raft_group_id,
            target_node_id
        )
    })?;
    let leader_term = leader_term.expect("a selected leader has a reported term");
    let max_survivor_term = max_survivor_term.expect("a selected leader is a survivor");
    if leader_term < max_survivor_term {
        bail!(
            "group {} self-reported leader {} is stale at term {} while a surviving voter reports term {}",
            raft_group_id,
            leader,
            leader_term,
            max_survivor_term
        );
    }
    Ok(leader)
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
    use axum::extract::OriginalUri;
    use axum::extract::Path;
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
        Stable,
        TargetMissing,
        RepairableTarget,
        DivergedSurvivorTerm,
    }

    struct MockCluster {
        scenario: LeaderScenario,
        pinned_leader: AtomicUsize,
        membership_phase: AtomicUsize,
        stalled_detach_mode: AtomicUsize,
        promotion_leader_handoff: AtomicUsize,
        survivor_terms_aligned: AtomicUsize,
        transient_dual_leader_reports: AtomicUsize,
        drained_nodes: Mutex<Vec<u64>>,
        undrained_nodes: Mutex<Vec<u64>>,
        quiesced_nodes: Mutex<Vec<u64>>,
        operations: Mutex<Vec<String>>,
    }

    #[derive(Clone)]
    struct MockNode {
        node_id: u64,
        cluster: Arc<MockCluster>,
    }

    fn mock_current_leader(cluster: &MockCluster) -> u64 {
        match cluster.pinned_leader.load(Ordering::SeqCst) {
            0 => match cluster.scenario {
                LeaderScenario::Stable | LeaderScenario::TargetMissing => 2,
                LeaderScenario::RepairableTarget => 1,
                LeaderScenario::DivergedSurvivorTerm => 2,
            },
            pinned => u64::try_from(pinned).unwrap(),
        }
    }

    async fn mock_metrics(State(state): State<MockNode>) -> Json<serde_json::Value> {
        let leader = mock_current_leader(&state.cluster);
        if matches!(state.cluster.scenario, LeaderScenario::TargetMissing) && state.node_id == 3 {
            return Json(json!({
                "wal_backend": "memory",
                "raft_groups": [{
                    "raft_group_id": 7,
                    "node_id": state.node_id,
                    "current_term": 1,
                    "current_leader": null,
                    "committed_index": null,
                    "last_applied_index": null,
                    "voter_ids": [],
                    "learner_ids": []
                }]
            }));
        }
        if matches!(
            state.cluster.scenario,
            LeaderScenario::RepairableTarget | LeaderScenario::DivergedSurvivorTerm
        ) {
            let phase = state.cluster.membership_phase.load(Ordering::SeqCst);
            let (voters, learners, applied) = match phase {
                0 if matches!(state.cluster.scenario, LeaderScenario::DivergedSurvivorTerm)
                    && state.node_id == 3 =>
                {
                    (vec![], vec![], None)
                }
                0 => (vec![1, 2, 3], vec![], (state.node_id != 3).then_some(100)),
                1 if state.node_id == 3 => (vec![1, 2, 3], vec![], Some(100)),
                1 => (vec![1, 2], vec![], Some(100)),
                2 => (vec![1, 2], vec![3], Some(100)),
                _ => (vec![1, 2, 3], vec![], Some(100)),
            };
            let terms_aligned = state.cluster.survivor_terms_aligned.load(Ordering::SeqCst) != 0;
            let transient_dual_leader = state
                .cluster
                .transient_dual_leader_reports
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                    remaining.checked_sub(1)
                })
                .is_ok();
            let (current_term, current_leader) =
                if matches!(state.cluster.scenario, LeaderScenario::DivergedSurvivorTerm)
                    && !terms_aligned
                {
                    match state.node_id {
                        1 => (31, Some(1)),
                        2 => (100, None),
                        _ => (101, None),
                    }
                } else if transient_dual_leader {
                    match state.node_id {
                        1 => (100, Some(1)),
                        2 => (100, Some(2)),
                        _ => (100, None),
                    }
                } else {
                    (100, Some(leader))
                };
            return Json(json!({
                "wal_backend": "memory",
                "raft_groups": [{
                    "raft_group_id": 7,
                    "node_id": state.node_id,
                    "current_term": current_term,
                    "current_leader": current_leader,
                    "committed_index": 100,
                    "last_applied_index": applied,
                    "voter_ids": voters,
                    "learner_ids": learners
                }]
            }));
        }
        Json(json!({
            "wal_backend": "memory",
            "raft_groups": [{
                "raft_group_id": 7,
                "node_id": state.node_id,
                "current_term": 1,
                "current_leader": leader,
                "committed_index": 100,
                "last_applied_index": 100,
                "voter_ids": [1, 2, 3],
                "learner_ids": []
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
            .undrained_nodes
            .lock()
            .unwrap()
            .push(state.node_id);
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
        if matches!(state.cluster.scenario, LeaderScenario::DivergedSurvivorTerm)
            && state
                .cluster
                .survivor_terms_aligned
                .swap(1, Ordering::SeqCst)
                == 0
        {
            // Model one metrics snapshot in which the new higher-term leader
            // and the old leader both still self-report leadership.
            state
                .cluster
                .transient_dual_leader_reports
                .store(3, Ordering::SeqCst);
        }
        state
            .cluster
            .pinned_leader
            .store(usize::try_from(to).unwrap(), Ordering::SeqCst);
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

    async fn mock_quiesce(State(state): State<MockNode>) -> StatusCode {
        state
            .cluster
            .quiesced_nodes
            .lock()
            .unwrap()
            .push(state.node_id);
        state
            .cluster
            .operations
            .lock()
            .unwrap()
            .push(format!("quiesce:{}", state.node_id));
        StatusCode::OK
    }

    async fn mock_membership(
        State(state): State<MockNode>,
        Path(_group_id): Path<u64>,
        OriginalUri(uri): OriginalUri,
    ) -> StatusCode {
        let voters = uri
            .query()
            .and_then(|query| query.strip_prefix("voters="))
            .unwrap_or_default();
        let phase = match voters {
            "1,2" => 1,
            "1,2,3" => 3,
            _ => return StatusCode::BAD_REQUEST,
        };
        if voters == "1,2,3" {
            let handoff = state
                .cluster
                .promotion_leader_handoff
                .swap(0, Ordering::SeqCst);
            if handoff != 0 {
                state.cluster.pinned_leader.store(handoff, Ordering::SeqCst);
            }
        }
        if state.node_id != mock_current_leader(&state.cluster) {
            return StatusCode::CONFLICT;
        }
        let stall_mode = if voters == "1,2" {
            state.cluster.stalled_detach_mode.swap(0, Ordering::SeqCst)
        } else {
            0
        };
        if stall_mode == 1 {
            return std::future::pending::<StatusCode>().await;
        }
        state
            .cluster
            .membership_phase
            .store(phase, Ordering::SeqCst);
        state
            .cluster
            .operations
            .lock()
            .unwrap()
            .push(format!("membership:{}:{voters}", state.node_id));
        if stall_mode == 2 {
            return std::future::pending::<StatusCode>().await;
        }
        StatusCode::OK
    }

    async fn mock_add_learner(
        State(state): State<MockNode>,
        Path((_group_id, target_node_id)): Path<(u64, u64)>,
        OriginalUri(uri): OriginalUri,
    ) -> StatusCode {
        let address = uri
            .query()
            .and_then(|query| query.strip_prefix("addr="))
            .and_then(|query| query.strip_suffix("&blocking=false"))
            .unwrap_or_default();
        if !address.starts_with("http://") || address.contains('%') {
            return StatusCode::BAD_REQUEST;
        }
        state.cluster.membership_phase.store(2, Ordering::SeqCst);
        state
            .cluster
            .operations
            .lock()
            .unwrap()
            .push(format!("learner:{}:{target_node_id}", state.node_id));
        StatusCode::OK
    }

    async fn mock_cluster(scenario: LeaderScenario) -> (Vec<NodeInfo>, Arc<MockCluster>) {
        let cluster = Arc::new(MockCluster {
            scenario,
            pinned_leader: AtomicUsize::new(0),
            membership_phase: AtomicUsize::new(0),
            stalled_detach_mode: AtomicUsize::new(0),
            promotion_leader_handoff: AtomicUsize::new(0),
            survivor_terms_aligned: AtomicUsize::new(0),
            transient_dual_leader_reports: AtomicUsize::new(0),
            drained_nodes: Mutex::new(Vec::new()),
            undrained_nodes: Mutex::new(Vec::new()),
            quiesced_nodes: Mutex::new(Vec::new()),
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
                .route(
                    "/__ursula/raft/{group_id}/membership",
                    post(mock_membership),
                )
                .route(
                    "/__ursula/raft/{group_id}/learners/{node_id}",
                    post(mock_add_learner),
                )
                .route("/__ursula/raft/quiesce-for-restart", post(mock_quiesce))
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
    async fn restarted_voter_is_rebuilt_through_durable_membership() {
        let (nodes, cluster) = mock_cluster(LeaderScenario::RepairableTarget).await;
        cluster.pinned_leader.store(3, Ordering::SeqCst);
        let preparation = repair_restarted_voter(
            &nodes,
            &nodes[2],
            &MetricsClient::new(Duration::from_secs(1)).unwrap(),
            &DrainOptions {
                drain_timeout: Duration::from_secs(1),
                ready_timeout: Duration::ZERO,
                poll_interval: Duration::from_millis(1),
                lag_tolerance: 16,
                dry_run: false,
            },
            &MembershipRepairOptions {
                max_concurrency: 2,
                poll_interval: Duration::ZERO,
                ..MembershipRepairOptions::default()
            },
        )
        .await
        .unwrap();

        assert_eq!(preparation.missing_group_count, 1);
        assert_eq!(preparation.leader_anchor, 1);
        assert_eq!(preparation.fenced_node_ids, vec![2]);
        assert_eq!(cluster.membership_phase.load(Ordering::SeqCst), 3);
        assert_eq!(*cluster.operations.lock().unwrap(), vec![
            "drain:3",
            "transfer:3->1",
            "drain:2",
            "membership:1:1,2",
            "learner:1:3",
            "membership:1:1,2,3"
        ]);
    }

    #[tokio::test]
    async fn restarted_voter_reconciles_a_stale_self_reported_survivor_leader() {
        let (nodes, cluster) = mock_cluster(LeaderScenario::DivergedSurvivorTerm).await;
        let preparation = repair_restarted_voter(
            &nodes,
            &nodes[2],
            &MetricsClient::new(Duration::from_secs(1)).unwrap(),
            &DrainOptions {
                drain_timeout: Duration::from_secs(1),
                ready_timeout: Duration::ZERO,
                poll_interval: Duration::from_millis(1),
                lag_tolerance: 16,
                dry_run: false,
            },
            &MembershipRepairOptions {
                max_concurrency: 2,
                poll_interval: Duration::ZERO,
                ..MembershipRepairOptions::default()
            },
        )
        .await
        .unwrap();

        assert_eq!(preparation.missing_group_count, 1);
        assert_eq!(preparation.leader_anchor, 1);
        assert_eq!(*cluster.operations.lock().unwrap(), vec![
            "drain:3",
            "transfer:1->2",
            "drain:2",
            "transfer:2->1",
            "membership:1:1,2",
            "learner:1:3",
            "membership:1:1,2,3"
        ]);
    }

    #[tokio::test]
    async fn membership_repair_retries_a_timed_out_uncommitted_request() {
        let (nodes, cluster) = mock_cluster(LeaderScenario::RepairableTarget).await;
        cluster.pinned_leader.store(3, Ordering::SeqCst);
        cluster.stalled_detach_mode.store(1, Ordering::SeqCst);

        let preparation = repair_restarted_voter(
            &nodes,
            &nodes[2],
            &MetricsClient::new(Duration::from_millis(20)).unwrap(),
            &DrainOptions {
                drain_timeout: Duration::from_secs(1),
                ready_timeout: Duration::ZERO,
                poll_interval: Duration::from_millis(1),
                lag_tolerance: 16,
                dry_run: false,
            },
            &MembershipRepairOptions {
                max_concurrency: 1,
                operation_timeout: Duration::from_millis(20),
                operation_reconcile_timeout: Duration::from_secs(1),
                poll_interval: Duration::from_millis(1),
                ..MembershipRepairOptions::default()
            },
        )
        .await
        .unwrap();

        assert_eq!(preparation.missing_group_count, 1);
        assert_eq!(cluster.membership_phase.load(Ordering::SeqCst), 3);
        assert_eq!(*cluster.operations.lock().unwrap(), vec![
            "drain:3",
            "transfer:3->1",
            "drain:2",
            "membership:1:1,2",
            "learner:1:3",
            "membership:1:1,2,3"
        ]);
    }

    #[tokio::test]
    async fn membership_repair_accepts_a_committed_request_with_a_lost_response() {
        let (nodes, cluster) = mock_cluster(LeaderScenario::RepairableTarget).await;
        cluster.pinned_leader.store(3, Ordering::SeqCst);
        cluster.stalled_detach_mode.store(2, Ordering::SeqCst);

        let preparation = repair_restarted_voter(
            &nodes,
            &nodes[2],
            &MetricsClient::new(Duration::from_millis(20)).unwrap(),
            &DrainOptions {
                drain_timeout: Duration::from_secs(1),
                ready_timeout: Duration::ZERO,
                poll_interval: Duration::from_millis(1),
                lag_tolerance: 16,
                dry_run: false,
            },
            &MembershipRepairOptions {
                max_concurrency: 1,
                operation_timeout: Duration::from_millis(20),
                operation_reconcile_timeout: Duration::from_secs(1),
                poll_interval: Duration::from_millis(1),
                ..MembershipRepairOptions::default()
            },
        )
        .await
        .unwrap();

        assert_eq!(preparation.missing_group_count, 1);
        assert_eq!(cluster.membership_phase.load(Ordering::SeqCst), 3);
        assert_eq!(*cluster.operations.lock().unwrap(), vec![
            "drain:3",
            "transfer:3->1",
            "drain:2",
            "membership:1:1,2",
            "learner:1:3",
            "membership:1:1,2,3"
        ]);
    }

    #[tokio::test]
    async fn membership_repair_rediscovers_leader_after_promotion_handoff() {
        let (nodes, cluster) = mock_cluster(LeaderScenario::RepairableTarget).await;
        cluster.pinned_leader.store(3, Ordering::SeqCst);
        cluster.promotion_leader_handoff.store(2, Ordering::SeqCst);

        let preparation = repair_restarted_voter(
            &nodes,
            &nodes[2],
            &MetricsClient::new(Duration::from_millis(20)).unwrap(),
            &DrainOptions {
                drain_timeout: Duration::from_secs(1),
                ready_timeout: Duration::ZERO,
                poll_interval: Duration::from_millis(1),
                lag_tolerance: 16,
                dry_run: false,
            },
            &MembershipRepairOptions {
                max_concurrency: 1,
                operation_timeout: Duration::from_millis(20),
                operation_reconcile_timeout: Duration::from_secs(1),
                poll_interval: Duration::from_millis(1),
                ..MembershipRepairOptions::default()
            },
        )
        .await
        .unwrap();

        assert_eq!(preparation.missing_group_count, 1);
        assert_eq!(cluster.membership_phase.load(Ordering::SeqCst), 3);
        assert_eq!(*cluster.operations.lock().unwrap(), vec![
            "drain:3",
            "transfer:3->1",
            "drain:2",
            "membership:1:1,2",
            "learner:1:3",
            "membership:2:1,2,3"
        ]);
    }

    #[tokio::test]
    async fn durable_membership_repair_resumes_after_committed_detach() {
        let (nodes, cluster) = mock_cluster(LeaderScenario::RepairableTarget).await;
        cluster.membership_phase.store(1, Ordering::SeqCst);

        repair_restarted_voter(
            &nodes,
            &nodes[2],
            &MetricsClient::new(Duration::from_secs(1)).unwrap(),
            &DrainOptions {
                drain_timeout: Duration::from_secs(1),
                ready_timeout: Duration::ZERO,
                poll_interval: Duration::from_millis(1),
                lag_tolerance: 16,
                dry_run: false,
            },
            &MembershipRepairOptions {
                max_concurrency: 2,
                poll_interval: Duration::ZERO,
                ..MembershipRepairOptions::default()
            },
        )
        .await
        .unwrap();

        assert_eq!(cluster.membership_phase.load(Ordering::SeqCst), 3);
        assert_eq!(*cluster.operations.lock().unwrap(), vec![
            "drain:3",
            "drain:2",
            "learner:1:3",
            "membership:1:1,2,3"
        ]);
    }

    #[tokio::test]
    async fn legacy_partial_replacement_handoff_drains_without_quiescing() {
        let (nodes, cluster) = mock_cluster(LeaderScenario::TargetMissing).await;
        let missing = prepare_recovery_handoff(
            &nodes,
            &nodes[2],
            &MetricsClient::new(Duration::from_secs(1)).unwrap(),
            &DrainOptions {
                drain_timeout: Duration::from_secs(1),
                ready_timeout: Duration::ZERO,
                poll_interval: Duration::from_millis(1),
                lag_tolerance: 16,
                dry_run: false,
            },
        )
        .await
        .unwrap();

        assert_eq!(missing, 1);
        assert_eq!(*cluster.drained_nodes.lock().unwrap(), vec![3]);
        assert!(cluster.quiesced_nodes.lock().unwrap().is_empty());
        assert_eq!(*cluster.operations.lock().unwrap(), vec!["drain:3"]);
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
            current_term: Some(1),
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
    fn missing_target_timeout_hint_points_to_membership_repair() {
        let snapshot = ClusterSnapshot {
            per_node: vec![NodeMetricsView {
                node: n(2, "10.0.0.2"),
                groups: vec![group(7, 2, Some(2), 100, 100)],
                wal_backend: None,
            }],
        };
        let report = check_readiness(&snapshot, 1, 5);
        assert!(!report.all_ready);

        let hint = missing_target_timeout_hint(&report).expect("missing target hint");
        assert!(hint.contains("membership repair"), "{hint}");
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
                all_ready: false,
                per_group,
            }
        };

        let before = TargetProgress::of(&report(10));
        let unrelated_write = TargetProgress::of(&report(11));
        assert!(!unrelated_write.advanced_past(&before));
    }

    #[test]
    fn restart_wal_backend_detection_fails_closed() {
        assert_eq!(
            decide_restart_wal_backend(&[Some("memory"); 3], 3).unwrap(),
            RestartWalBackend::Memory
        );
        assert_eq!(
            decide_restart_wal_backend(&[Some("disk"); 3], 3).unwrap(),
            RestartWalBackend::Disk
        );
        assert!(decide_restart_wal_backend(&[Some("memory"), Some("disk")], 2).is_err());
        assert!(decide_restart_wal_backend(&[Some("memory"), None], 2).is_err());
        assert!(decide_restart_wal_backend(&[Some("memory")], 2).is_err());
    }

    #[test]
    fn membership_repair_requires_a_stable_non_target_leader() {
        let stable = ClusterSnapshot {
            per_node: vec![
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
        assert_eq!(stable_non_target_leader(&stable, 7, 1).unwrap(), 2);

        let conflicting = ClusterSnapshot {
            per_node: vec![
                NodeMetricsView {
                    node: n(2, "10.0.0.2"),
                    groups: vec![group(7, 2, Some(2), 100, 100)],
                    wal_backend: Some("memory".into()),
                },
                NodeMetricsView {
                    node: n(3, "10.0.0.3"),
                    groups: vec![group(7, 3, Some(3), 100, 100)],
                    wal_backend: Some("memory".into()),
                },
            ],
        };
        assert!(stable_non_target_leader(&conflicting, 7, 1).is_err());
        assert_eq!(
            stale_survivor_term_handoff(&conflicting, 7, 1).unwrap(),
            None
        );

        let mut stale_term = stable;
        stale_term.per_node[0].groups[0].current_term = Some(31);
        stale_term.per_node[1].groups[0].current_term = Some(100);
        stale_term.per_node[1].groups[0].current_leader = None;
        let error = stable_non_target_leader(&stale_term, 7, 1).unwrap_err();
        assert!(error.to_string().contains("stale at term 31"), "{error:#}");
        let expected_handoff = SurvivorTermHandoff {
            raft_group_id: 7,
            stale_leader: 2,
            stale_term: 31,
            higher_term_voter: 3,
            higher_term: 100,
        };
        assert_eq!(
            stale_survivor_term_handoff(&stale_term, 7, 1).unwrap(),
            Some(expected_handoff)
        );

        let mut fresh_higher_view = stale_term.per_node[1].clone();
        fresh_higher_view.groups[0].current_term = Some(101);
        assert_eq!(
            refreshed_survivor_term_handoff(&stale_term, &fresh_higher_view, expected_handoff, 1,)
                .unwrap(),
            SurvivorTermHandoff {
                higher_term: 101,
                ..expected_handoff
            }
        );

        fresh_higher_view.groups[0].current_leader = Some(2);
        let error =
            refreshed_survivor_term_handoff(&stale_term, &fresh_higher_view, expected_handoff, 1)
                .unwrap_err();
        assert!(
            error.to_string().contains("still reports leader"),
            "{error:#}"
        );

        let mut higher_term_reports_a_leader = stale_term.clone();
        higher_term_reports_a_leader.per_node[1].groups[0].current_leader = Some(2);
        let error = stale_survivor_term_handoff(&higher_term_reports_a_leader, 7, 1).unwrap_err();
        assert!(
            error.to_string().contains("still reports leader"),
            "{error:#}"
        );

        let mut higher_term_is_behind = stale_term;
        higher_term_is_behind.per_node[1].groups[0].committed_index = Some(99);
        let error = stale_survivor_term_handoff(&higher_term_is_behind, 7, 1).unwrap_err();
        assert!(
            error.to_string().contains("is behind stale leader"),
            "{error:#}"
        );
    }

    #[test]
    fn missing_target_timeout_hint_absent_when_target_has_applied_entries() {
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
        assert!(missing_target_timeout_hint(&report).is_none());
    }

    #[test]
    fn drain_uses_all_reports_but_repair_uses_a_peer_self_leader() {
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

        assert_eq!(stable_non_target_leader(&snapshot, 7, 1).unwrap(), 2);
    }

    #[test]
    fn repair_ignores_the_quiesced_targets_frozen_leader_view() {
        let snapshot = ClusterSnapshot {
            per_node: vec![
                NodeMetricsView {
                    node: n(1, "10.0.0.1"),
                    groups: vec![group(159, 1, Some(1), 100, 100)],
                    wal_backend: Some("memory".into()),
                },
                NodeMetricsView {
                    node: n(2, "10.0.0.2"),
                    groups: vec![group(159, 2, Some(3), 100, 100)],
                    wal_backend: Some("memory".into()),
                },
                NodeMetricsView {
                    node: n(3, "10.0.0.3"),
                    groups: vec![group(159, 3, Some(1), 100, 100)],
                    wal_backend: Some("memory".into()),
                },
            ],
        };

        assert_eq!(stable_non_target_leader(&snapshot, 159, 2).unwrap(), 1);
    }

    #[test]
    fn empty_target_group_uses_survivor_inventory_for_repair() {
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
            survivor_group_inventory(&snapshot, 1),
            [7].into_iter().collect()
        );
        assert_eq!(stable_non_target_leader(&snapshot, 7, 1).unwrap(), 2);
    }
}

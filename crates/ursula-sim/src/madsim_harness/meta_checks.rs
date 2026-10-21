//! The meta invariants of the `placement-move` family (design 15.2), over a
//! [`MetaCluster`]:
//!
//! - I6 ([`I6`]): with meta's quorum lost, every data group with a quorum
//!   commits.
//! - On a placed cluster, the hosting checks of [`HostingChecks`] (I2, I3
//!   and the hosting check) at every observation.
//! - I10 ([`I10`]): every acknowledged control write survives power loss on
//!   every meta replica, and a power loss comes after at least one
//!   acknowledged write.
//! - I11 ([`I11`]): a registered node is a meta learner (or the voter it is
//!   claimed as) whose view reaches the meta leader's applied entry within
//!   a bound, after a meta plan too.
//! - I12 ([`I12`]), in its S1 form: one genesis history event per run. No
//!   two nodes ever apply meta histories that began at different geneses,
//!   and none while the claims differ. Data history alone does not block a
//!   genesis while data forms statically, so I12 reads meta's replicas, not
//!   the cores.
//! - The meta plan check ([`PLAN`]): a meta plan adds one history event,
//!   which every placement's `since` moves to, drops every retirement, cuts
//!   a formation, and marks no `PlacementUnavailable` shed (r1 L12).
//!
//! A failure records [`SimEvent::InvariantFailed`] and panics.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::time::Duration;

use ursula_control::ControlApplied;
use ursula_control::ControlCommand;
use ursula_control::ControlPlaneState;
use ursula_control::MetaReplicaStatus;
use ursula_control::MetaRole;
use ursula_control::MoveLimit;
use ursula_control::RegistrationStatus;
use ursula_proto::placement::NodeId;
use ursula_proto::placement::ViewStage;
use ursula_proto::placement::WriteEffect;

use super::SimEvent;
use super::SimTrace;
use super::journal_cluster::JOURNAL_GROUPS;
use super::meta_cluster::MetaCluster;
use super::placed_hosting::HostingChecks;

pub(super) const I6: &str = "meta_loss_leaves_data_writable";
pub(super) const I10: &str = "acknowledged_meta_writes_survive_power_loss";
pub(super) const I11: &str = "registered_nodes_learn_the_view";
pub(super) const I12: &str = "one_meta_genesis_per_run";
pub(super) const PLAN: &str = "meta_plan_moves_every_since";

/// How often a wait looks again.
pub(super) const STEP: Duration = Duration::from_millis(50);
/// How long a node's view may take to reach what the checks wait for: a
/// few meta elections (up to 3 s each) and heal rounds.
pub(super) const BOUND: Duration = Duration::from_secs(20);

pub(super) fn fail(trace: &mut SimTrace, invariant: &str, after_event: &str, message: String) -> ! {
    trace.push(SimEvent::InvariantFailed {
        invariant: invariant.to_owned(),
        after_event: after_event.to_owned(),
        message: message.clone(),
    });
    panic!("invariant `{invariant}` failed after `{after_event}`: {message}");
}

/// What the checks keep across a run.
#[derive(Debug, Default)]
pub(super) struct MetaChecks {
    /// The hosting checks of a placed cluster, at every observation.
    hosting: HostingChecks,
    /// Every genesis history id some node applied, with the node first seen
    /// holding it.
    geneses: BTreeMap<u64, u64>,
    /// Every history id some node applied that replaced another, with the
    /// history it replaced.
    replacements: BTreeMap<u64, u64>,
    /// The move limit of the last acknowledged `SetMoveLimit`.
    acknowledged: Option<u32>,
    /// The limits of the writes after it whose outcome is unknown
    /// ([`WriteEffect::MayHaveApplied`]): any of them may have applied.
    uncertain: BTreeSet<u32>,
}

impl MetaChecks {
    /// I12, after every action and every step of a wait: the histories the
    /// running meta replicas applied all began at one genesis. On a placed
    /// cluster, the hosting checks too ([`HostingChecks::observe`]).
    pub(super) fn observe(
        &mut self,
        cluster: &MetaCluster,
        trace: &mut SimTrace,
        after_event: &str,
    ) {
        self.hosting.observe(cluster, trace, after_event);
        for node_id in cluster.running() {
            let Some(event) = cluster
                .registry(node_id)
                .meta()
                .and_then(|meta| meta.applied_history())
            else {
                continue;
            };
            if event.previous == 0 {
                self.geneses.entry(event.id.get()).or_insert(node_id);
            } else {
                self.replacements.insert(event.id.get(), event.previous);
            }
        }
        if self.geneses.len() > 1 {
            fail(
                trace,
                I12,
                after_event,
                format!(
                    "the run applied more than one meta genesis (history id: first node): {:?}",
                    self.geneses
                ),
            );
        }
    }

    /// I12 while the claims differ: no node applied a genesis.
    pub(super) fn no_genesis(&self, trace: &mut SimTrace, after_event: &str) {
        if !self.geneses.is_empty() {
            fail(
                trace,
                I12,
                after_event,
                format!("meta formed while the claims differ: {:?}", self.geneses),
            );
        }
    }

    /// Writes `count` `SetMoveLimit`s through the nodes of `via` in turn,
    /// each with a new limit, and keeps the last one acknowledged for I10.
    /// A write that did not apply ([`WriteEffect::NotApplied`]) is written
    /// again within the bound, as a client would. Only a write whose outcome
    /// is unknown counts as one that may have applied.
    pub(super) async fn write_limits(&mut self, cluster: &MetaCluster, via: &[u64], count: u32) {
        let base = self
            .acknowledged
            .unwrap_or(2)
            .max(self.uncertain.last().copied().unwrap_or(0));
        for (step, node_id) in (1..=count).zip(via.iter().copied().cycle()) {
            let limit = base.saturating_add(step);
            let deadline = madsim::time::Instant::now() + BOUND;
            loop {
                match cluster
                    .handle(node_id)
                    .submit(ControlCommand::SetMoveLimit {
                        limit: MoveLimit::new(limit),
                    })
                    .await
                {
                    Ok(ControlApplied::MoveLimitSet { limit: set }) if set.get() == limit => {
                        self.acknowledged = Some(limit);
                        self.uncertain.clear();
                        break;
                    }
                    Ok(answer) => panic!("node {node_id}: SetMoveLimit answered {answer:?}"),
                    Err(error) => match error.write_effect() {
                        WriteEffect::MayHaveApplied => {
                            self.uncertain.insert(limit);
                            break;
                        }
                        WriteEffect::NotApplied if madsim::time::Instant::now() < deadline => {
                            madsim::time::sleep(STEP).await;
                        }
                        WriteEffect::NotApplied => break,
                    },
                }
            }
        }
    }

    /// Waits until the view of every node of `nodes` is bootstrapped, so the
    /// control writes after it are not refused as written before bootstrap
    /// (I11: a registered node learns the view, its bootstrap included).
    pub(super) async fn bootstrapped(
        &mut self,
        cluster: &MetaCluster,
        trace: &mut SimTrace,
        after_event: &str,
        nodes: &[u64],
    ) {
        let deadline = madsim::time::Instant::now() + BOUND;
        loop {
            self.observe(cluster, trace, after_event);
            let forming: Vec<u64> = nodes
                .iter()
                .copied()
                .filter(|node_id| cluster.view(*node_id).cluster().is_none())
                .collect();
            if forming.is_empty() {
                return;
            }
            if madsim::time::Instant::now() >= deadline {
                fail(
                    trace,
                    I11,
                    after_event,
                    format!("the views of nodes {forming:?} are not bootstrapped"),
                );
            }
            madsim::time::sleep(STEP).await;
        }
    }

    /// I10: every meta replica of `nodes` shows the last acknowledged move
    /// limit, or one written after it whose outcome is unknown, within the
    /// bound. A power loss that came with no write acknowledged fails it,
    /// since it would check nothing.
    pub(super) async fn acknowledged_writes_survive(
        &self,
        cluster: &MetaCluster,
        trace: &mut SimTrace,
        after_event: &str,
        nodes: &[u64],
    ) {
        let Some(acknowledged) = self.acknowledged else {
            fail(
                trace,
                I10,
                after_event,
                "no control write was acknowledged before the power loss".to_owned(),
            );
        };
        let deadline = madsim::time::Instant::now() + BOUND;
        loop {
            let seen: BTreeMap<u64, Option<u32>> = nodes
                .iter()
                .map(|node_id| {
                    let limit = cluster
                        .view(*node_id)
                        .cluster()
                        .map(|bootstrapped| bootstrapped.params().max_concurrent_moves.get());
                    (*node_id, limit)
                })
                .collect();
            let shows = |limit: &Option<u32>| {
                limit.is_some_and(|limit| limit == acknowledged || self.uncertain.contains(&limit))
            };
            if seen.values().all(shows) {
                return;
            }
            if madsim::time::Instant::now() >= deadline {
                fail(
                    trace,
                    I10,
                    after_event,
                    format!(
                        "the acknowledged move limit {acknowledged} (or one of {:?}) is not on \
                         every meta replica: {seen:?}",
                        self.uncertain
                    ),
                );
            }
            madsim::time::sleep(STEP).await;
        }
    }

    /// I11: every node of `nodes` registered, is the meta voter its claim
    /// names or else a meta learner, and its view reaches the entry the
    /// meta leader applied when the check began, within the bound.
    pub(super) async fn registered_nodes_learn(
        &mut self,
        cluster: &MetaCluster,
        trace: &mut SimTrace,
        after_event: &str,
        nodes: &[u64],
    ) {
        let deadline = madsim::time::Instant::now() + BOUND;
        let mut target = None;
        loop {
            self.observe(cluster, trace, after_event);
            if target.is_none() {
                target = meta_leader_applied(cluster);
            }
            let lagging: Vec<String> = match target {
                None => vec!["no meta leader".to_owned()],
                Some(target) => nodes
                    .iter()
                    .filter_map(|node_id| learn_lag(cluster, *node_id, target))
                    .collect(),
            };
            if lagging.is_empty() {
                return;
            }
            if madsim::time::Instant::now() >= deadline {
                fail(trace, I11, after_event, lagging.join("; "));
            }
            madsim::time::sleep(STEP).await;
        }
    }

    /// The meta plan check, once the plan's history event applied on every
    /// node of `nodes`: exactly one history replaced the one `before` names,
    /// every placement's `since` is the plan's event, and no retirement or
    /// formation is left.
    pub(super) async fn plan_applied(
        &mut self,
        cluster: &MetaCluster,
        trace: &mut SimTrace,
        after_event: &str,
        nodes: &[u64],
        before: u64,
    ) {
        let deadline = madsim::time::Instant::now() + BOUND;
        loop {
            self.observe(cluster, trace, after_event);
            let applied = nodes.iter().all(|node_id| {
                cluster
                    .registry(*node_id)
                    .meta()
                    .and_then(|meta| meta.applied_history())
                    .is_some_and(|event| event.previous == before)
            });
            if applied {
                break;
            }
            if madsim::time::Instant::now() >= deadline {
                fail(
                    trace,
                    PLAN,
                    after_event,
                    format!(
                        "the meta plan's history event did not apply on every node of {nodes:?}"
                    ),
                );
            }
            madsim::time::sleep(STEP).await;
        }
        let replacing: Vec<u64> = self
            .replacements
            .iter()
            .filter(|(_, previous)| **previous == before)
            .map(|(id, _)| *id)
            .collect();
        if replacing.len() != 1 {
            fail(
                trace,
                PLAN,
                after_event,
                format!(
                    "the plan began {} histories after {before:#x}: {replacing:?}",
                    replacing.len()
                ),
            );
        }
        for node_id in nodes {
            let view = cluster.view(*node_id);
            let ViewStage::Applied { history, .. } = view.stage else {
                fail(
                    trace,
                    PLAN,
                    after_event,
                    format!("node {node_id}: an empty view after the plan"),
                );
            };
            let wrong = match &*view.state {
                ControlPlaneState::Bootstrapped(bootstrapped) => {
                    let moved: Vec<String> = bootstrapped
                        .groups()
                        .iter()
                        .filter(|(_, placement)| placement.since().log_id() != history)
                        .map(|(group, placement)| {
                            format!("group {} since {}", group.0, placement.since().log_id())
                        })
                        .collect();
                    if !moved.is_empty() {
                        Some(format!(
                            "placements not at the plan's event {history}: {moved:?}"
                        ))
                    } else if !bootstrapped.retirements().is_empty() {
                        Some(format!(
                            "retirements left: {:?}",
                            bootstrapped.retirements()
                        ))
                    } else {
                        None
                    }
                }
                ControlPlaneState::Forming { .. } => {
                    Some("a formation the plan did not cut".to_owned())
                }
                ControlPlaneState::FormationCut => None,
            };
            let shed = cluster
                .registry(*node_id)
                .leadership_shed_state()
                .contains(ursula_raft::LeadershipShedState::PLACEMENT_UNAVAILABLE);
            let wrong = wrong.or_else(|| {
                shed.then(|| "the plan's restart marked PlacementUnavailable".to_owned())
            });
            if let Some(wrong) = wrong {
                fail(trace, PLAN, after_event, format!("node {node_id}: {wrong}"));
            }
        }
    }

    /// The genesis history id the run applied, once one did.
    pub(super) fn genesis(&self) -> Option<u64> {
        self.geneses.keys().next().copied()
    }
}

/// The history id `node_id`'s meta replica applied last.
pub(super) fn history_of(cluster: &MetaCluster, node_id: u64) -> Option<u64> {
    cluster
        .registry(node_id)
        .meta()
        .and_then(|meta| meta.applied_history())
        .map(|event| event.id.get())
}

/// I6: while meta has no quorum, or has not formed, every data group whose
/// voters keep a quorum takes appends.
pub(super) async fn data_commits_without_meta(
    cluster: &mut MetaCluster,
    trace: &mut SimTrace,
    after_event: &str,
) {
    for group in JOURNAL_GROUPS {
        if !cluster.has_quorum(group) {
            continue;
        }
        if !cluster.try_append(group, 3).await {
            fail(
                trace,
                I6,
                after_event,
                format!(
                    "data group {group} with a quorum took no append while meta could not commit"
                ),
            );
        }
    }
}

/// The entry the meta leader some running node names applied, if that
/// leader runs.
fn meta_leader_applied(cluster: &MetaCluster) -> Option<u64> {
    let leader = cluster.running().into_iter().find_map(|node_id| {
        match cluster.runtimes[&node_id]
            .status_reader()
            .placement_status()
            .meta
        {
            MetaReplicaStatus::Running {
                leader: Some(leader),
                ..
            } if cluster.runtimes.contains_key(&leader.get()) => Some(leader.get()),
            _ => None,
        }
    })?;
    match cluster.view(leader).stage {
        ViewStage::Applied { applied, .. } => Some(applied.get().index),
        ViewStage::Empty => None,
    }
}

/// Why `node_id` has not learned the view up to `target`, if it has not.
fn learn_lag(cluster: &MetaCluster, node_id: u64, target: u64) -> Option<String> {
    let Some(runtime) = cluster.runtimes.get(&node_id) else {
        return Some(format!("node {node_id} runs no meta runtime"));
    };
    let status = runtime.status_reader().placement_status();
    if !matches!(status.registration, RegistrationStatus::Answered(_)) {
        return Some(format!(
            "node {node_id} is not registered: {:?}",
            status.registration
        ));
    }
    let voter = cluster.claims[&node_id]
        .meta_voters
        .contains(NodeId::new(node_id));
    let role = if voter {
        MetaRole::Voter
    } else {
        MetaRole::Learner
    };
    match status.meta {
        MetaReplicaStatus::Running { role: held, .. } if held == role => {}
        meta => return Some(format!("node {node_id} is not a meta {role:?}: {meta:?}")),
    }
    match status.applied {
        Some(applied) if applied.get().index >= target => None,
        applied => Some(format!(
            "node {node_id} applied {applied:?}, below {target}"
        )),
    }
}

//! The membership reconcile shell: the driver around the pure membership
//! planner (`ursula_control::plan_membership_step`) on a group leader.
//!
//! On each due wake-up of its [`LeaderTick`] the shell reads its replica's
//! metrics into a [`MembershipView`], reads the target from its
//! [`MembershipTargetSource`], asks the planner for one step and runs it.
//!
//! - **Membership steps** go through [`OwnerRaftHandle::change_membership_if`]
//!   guarded by the committed leader id the step was planned under and the
//!   membership log id it was planned from, so a step planned on an older
//!   membership or in another leadership changes nothing and is planned
//!   again.
//! - **Kept futures**: every step runs on a task of its own whose handle the
//!   shell keeps until OpenRaft answers, and nothing else is planned
//!   meanwhile. OpenRaft completes a queued change when its entry applies or
//!   is truncated, so a stopped shell leaves its step to finish.
//! - **Transfers** go through the replica's one handoff policy, and a target
//!   whose transfer did not move the leadership is skipped for one plan.
//! - **Confirmations**: a voter change from a uniform config needs a view
//!   the target source confirmed within the current leadership. The shell
//!   keeps a confirmation only while its vote is unchanged and every view it
//!   reads carries its stamp.
//! - **Reports** (design 5.7): a `Converged` step under a seeded or moving
//!   target at `since` proves the leadership with a fresh quorum barrier,
//!   re-reads the vote and the committed uniform membership, and reports
//!   them through the target source ([`MembershipTargetSource::report`]).
//!   An accepted report waits until the view moves past `since`. A report
//!   the meta group refuses as stale publishes `StaleView`, and one that
//!   may have applied, or that a refusal says to plan again from a
//!   confirmed view, makes the shell confirm its view before it plans
//!   again. Each later report takes a new proof.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use futures_util::future::BoxFuture;
use openraft::ChangeMembers;
use openraft::EmptyNode;
use openraft::RaftMetrics;
use openraft::alias::LogIdOf;
use openraft::error::ChangeMembershipError;
use openraft::error::ClientWriteError;
use openraft::error::PreconditionFailed;
use openraft::error::RaftError;
use openraft::raft::ClientWriteResponse;
use openraft::raft::Precondition;
use openraft::rt::WatchReceiver;
use openraft::rt::WatchSender;
use openraft::storage::RaftStateMachine;
use openraft::type_config::TypeConfigExt;
use openraft::vote::RaftLeaderId;
use ursula_control::ConfirmedStamp;
use ursula_control::ControlApplied;
use ursula_control::ControlError;
use ursula_control::EffectiveMembership;
use ursula_control::LeaderView;
use ursula_control::Leadership;
use ursula_control::MembershipStep;
use ursula_control::MembershipTarget;
use ursula_control::MembershipView;
use ursula_control::PeerGate;
use ursula_control::Requeue;
use ursula_control::TargetStage;
use ursula_control::ViewFreshness;
use ursula_control::plan_membership_step;
use ursula_proto::placement::MembershipLogId;
use ursula_proto::placement::NodeId;
use ursula_proto::placement::PlacementEpoch;
use ursula_proto::placement::RaftLogId;
use ursula_proto::placement::ReconcileState;
use ursula_proto::placement::ReconcileStatus;
use ursula_proto::placement::StepFailure;
use ursula_proto::placement::StepFailureKind;
use ursula_proto::placement::StepKind;
use ursula_proto::placement::ViewStage;
use ursula_proto::placement::VoterSet;
use ursula_proto::placement::WaitReason;
use ursula_shard::RaftGroupId;
use ursula_shard::RaftTarget;

use super::leader_tick::FollowerLogReversions;
use super::leader_tick::LeaderTick;
use super::leader_tick::StepRetry;
use super::leader_tick::matched_indexes;
use super::leads_live;
use super::log_index;
use crate::election::ElectionPolicy;
use crate::election::LeadershipTransferError;
use crate::memberships::MembershipKind;
use crate::memberships::membership_config;
use crate::meta::confirm::MetaBarrierError;
use crate::meta::handle::MetaWriteError;
use crate::peer_cluster::PeerCluster;
use crate::registry::QuorumProofError;
use crate::replica::ReplicaCore;
use crate::rt::sync::watch;
use crate::target::WireTarget;
use crate::types::SnapshotData;
use crate::types::UrsulaRaftTypeConfig as C;
use crate::types::UrsulaVote;

/// How long a step may stay unanswered before the shell warns that it is
/// slow. It keeps waiting: OpenRaft answers every queued change.
const STEP_SLOW: Duration = Duration::from_secs(10);

/// Where a group's membership target comes from: its placement for a data
/// group, the registered nodes for the meta group.
pub trait MembershipTargetSource: Send + Sync + 'static {
    /// The target and the view it was read from, `None` while the
    /// membership is managed externally.
    fn target(&self) -> Option<TargetRead>;
    /// A meta read barrier: the stamp a view must carry to be confirmed.
    fn confirm(&self) -> BoxFuture<'_, Result<ConfirmedStamp, MetaBarrierError>>;
    /// Reports to the meta group that the group's committed uniform
    /// membership reached the target of the placement at
    /// [`Convergence::since`] (`ControlCommand::ReportConverged`). The
    /// shell reports only after it proved its leadership.
    fn report(
        &self,
        convergence: Convergence,
    ) -> BoxFuture<'_, Result<ControlApplied, MetaWriteError>>;
}

/// What a proven leader reports of its group: the placement it pursued, and
/// the voters of its committed uniform membership at `membership`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Convergence {
    pub since: PlacementEpoch,
    pub voters: VoterSet,
    pub membership: MembershipLogId,
}

/// A target and the stage of the placement view it was read from, read
/// together so a confirmation is checked against the view the target came
/// from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TargetRead {
    pub target: MembershipTarget,
    pub view: ViewStage,
}

/// The recovery gates of a group's caught-up learners, which a leader
/// promotes only when open.
pub trait PeerGates: Send + Sync + 'static {
    /// The gates of `learners` of `replica`'s group. A learner without an
    /// answer is missing or `Unknown`.
    fn gates(
        &self,
        replica: RaftTarget,
        learners: &BTreeSet<NodeId>,
    ) -> BoxFuture<'_, BTreeMap<NodeId, PeerGate>>;
}

/// No gates: every learner's gate is unknown, so no learner is promoted.
/// The meta group never promotes one.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoPeerGates;

impl PeerGates for NoPeerGates {
    fn gates(
        &self,
        _replica: RaftTarget,
        _learners: &BTreeSet<NodeId>,
    ) -> BoxFuture<'_, BTreeMap<NodeId, PeerGate>> {
        Box::pin(async { BTreeMap::new() })
    }
}

/// What the shell has under way, which OpenRaft's metrics do not show.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PendingStep {
    None,
    /// A step's future has not answered.
    #[cfg_attr(
        not(all(test, not(madsim))),
        expect(
            dead_code,
            reason = "the shell plans nothing while a step is pending, so only its tests plan under one"
        )
    )]
    Step,
    /// The shell's own leadership transfer to `to` has not settled.
    Transfer {
        to: NodeId,
    },
}

/// The planner inputs the shell gathers beside the metrics. The gates
/// come once the view names the learners it needs them of
/// ([`ursula_control::LeaderView::gates_needed`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ShellInputs {
    pub(crate) freshness: ViewFreshness,
    pub(crate) handoff_ready: BTreeSet<NodeId>,
    pub(crate) unlisted: BTreeSet<NodeId>,
}

/// The planner's view of the replica whose metrics are `metrics`.
///
/// A replica leads only while it leads live (`leads_live`) under a
/// committed vote. Its membership is `Committed` when the effective
/// membership is the committed one, by log id, and `Uncommitted`
/// otherwise. `live` holds the members that acknowledged this leader
/// within `ack_window`.
pub(crate) fn membership_view(
    metrics: &RaftMetrics<C>,
    me: NodeId,
    ack_window: Duration,
    pending: PendingStep,
    inputs: ShellInputs,
) -> MembershipView {
    let leadership = leadership(metrics, ack_window, pending, inputs);
    MembershipView { me, leadership }
}

fn leadership(
    metrics: &RaftMetrics<C>,
    ack_window: Duration,
    pending: PendingStep,
    inputs: ShellInputs,
) -> Leadership {
    if !leads_live(metrics) || !metrics.vote.is_committed() {
        return Leadership::NotLeading;
    }
    match pending {
        PendingStep::Transfer { to } => return Leadership::Transferring { to },
        PendingStep::Step => return Leadership::StepPending,
        PendingStep::None => {}
    }
    let Some(effective) = metrics.membership_config.log_id() else {
        return Leadership::NotLeading;
    };
    let log_id = membership_log_id(effective);
    let membership =
        if metrics.committed_membership_config.log_id() == metrics.membership_config.log_id() {
            match membership_config(&metrics.membership_config, MembershipKind::Effective) {
                Ok(config) => EffectiveMembership::Committed { log_id, config },
                Err(error) => {
                    tracing::error!(%error, "a leader's membership has no valid voter config");
                    return Leadership::NotLeading;
                }
            }
        } else {
            EffectiveMembership::Uncommitted { log_id }
        };
    let matched = matched_indexes(metrics)
        .into_iter()
        .map(|(node, matched)| (NodeId::new(node), matched))
        .collect();
    let live = metrics
        .heartbeat
        .iter()
        .flatten()
        .filter(|(_, acked)| {
            acked
                .as_ref()
                .is_some_and(|acked| openraft::Instant::elapsed(&**acked) <= ack_window)
        })
        .map(|(node, _)| NodeId::new(*node))
        .collect();
    Leadership::Leading(Box::new(LeaderView {
        membership,
        committed_index: log_index(metrics.local_committed.as_ref()).unwrap_or(0),
        matched,
        live,
        handoff_ready: inputs.handoff_ready,
        gates: BTreeMap::new(),
        unlisted: inputs.unlisted,
        view: inputs.freshness,
    }))
}

fn membership_log_id(log_id: &LogIdOf<C>) -> MembershipLogId {
    MembershipLogId::new(RaftLogId {
        term: log_id.leader_id.term(),
        leader: NodeId::new(*log_id.leader_id.node_id()),
        index: log_id.index,
    })
}

/// The OpenRaft log id of a data group's membership entry as meta names it.
pub(super) fn openraft_log_id(log_id: MembershipLogId) -> LogIdOf<C> {
    let log_id = log_id.get();
    LogIdOf::<C> {
        leader_id: <C as openraft::RaftTypeConfig>::LeaderId::new(log_id.term, log_id.leader.get()),
        index: log_id.index,
    }
}

/// How a step's future ended.
#[derive(Debug)]
enum StepOutcome {
    /// The change committed, or the transfer was submitted.
    Done,
    /// The target source confirmed a view at this stamp.
    Confirmed(ConfirmedStamp),
    /// The second entry of a voter change found its guard moved: the joint
    /// entry committed, and the next plan flattens it.
    JointCommitted,
    /// Refused against the current membership: planned again.
    Replan {
        kind: StepFailureKind,
        error: Box<ClientWriteError<C>>,
    },
    /// This replica no longer leads under the step's leadership.
    NotLeader,
    /// The handoff policy refused the transfer.
    TransferRefused(LeadershipTransferError),
    /// The meta read barrier failed.
    ConfirmFailed(MetaBarrierError),
    /// OpenRaft refused the change as one no planned step makes: an empty
    /// membership or a transition it does not support. A planner bug.
    Unplannable(Box<ClientWriteError<C>>),
    /// The meta group applied the report of the placement at `since`.
    Reported {
        since: PlacementEpoch,
        applied: ControlApplied,
    },
    /// The meta group refused the report: the placement moved from
    /// `reported` to `current`.
    StalePlacement {
        reported: PlacementEpoch,
        current: PlacementEpoch,
    },
    /// The meta group refused the report for another reason.
    ReportRefused(ControlError),
    /// The report may have applied.
    ReportUnknown(Box<MetaWriteError>),
    /// The report did not apply.
    ReportFailed(Box<MetaWriteError>),
    /// The leadership proof before a report failed.
    ProofFailed(Box<QuorumProofError>),
    /// The committed membership moved during the proof.
    MembershipMoved,
    /// The Raft stopped.
    Stopped,
}

impl StepOutcome {
    fn name(&self) -> &'static str {
        match self {
            Self::Done => "done",
            Self::Confirmed(_) => "confirmed",
            Self::JointCommitted => "joint_committed",
            Self::Replan { .. } => "replan",
            Self::NotLeader => "not_leader",
            Self::TransferRefused(_) => "transfer_refused",
            Self::ConfirmFailed(_) => "confirm_failed",
            Self::Unplannable(_) => "unplannable",
            Self::Reported { .. } => "reported",
            Self::StalePlacement { .. } => "stale_placement",
            Self::ReportRefused(_) => "report_refused",
            Self::ReportUnknown(_) => "report_unknown",
            Self::ReportFailed(_) => "report_failed",
            Self::ProofFailed(_) => "proof_failed",
            Self::MembershipMoved => "membership_moved",
            Self::Stopped => "stopped",
        }
    }

    /// The failure a status records, `None` for a step that did its part.
    fn failure(&self) -> Option<StepFailureKind> {
        match self {
            Self::Done
            | Self::Confirmed(_)
            | Self::JointCommitted
            | Self::Reported { .. }
            | Self::Stopped => None,
            Self::Replan { kind, .. } => Some(*kind),
            Self::NotLeader => Some(StepFailureKind::NotLeader),
            Self::TransferRefused(LeadershipTransferError::NotLeader { .. }) => {
                Some(StepFailureKind::NotLeader)
            }
            Self::TransferRefused(_) => Some(StepFailureKind::TransferAbandoned),
            Self::ConfirmFailed(_) | Self::ReportUnknown(_) | Self::ReportFailed(_) => {
                Some(StepFailureKind::MetaUnavailable)
            }
            Self::Unplannable(_)
            | Self::StalePlacement { .. }
            | Self::ReportRefused(_)
            | Self::MembershipMoved => Some(StepFailureKind::PreconditionFailed),
            Self::ProofFailed(_) => Some(StepFailureKind::LeadershipProofFailed),
        }
    }

    /// Whether the shell confirms its view before it plans again: the
    /// report may have applied, or the meta group says the view it was
    /// planned from is behind.
    fn rereads(&self) -> bool {
        match self {
            Self::StalePlacement { .. } | Self::ReportUnknown(_) => true,
            Self::ReportRefused(error) => error.requeue() == Requeue::ConfirmView,
            Self::Done
            | Self::Confirmed(_)
            | Self::JointCommitted
            | Self::Replan { .. }
            | Self::NotLeader
            | Self::TransferRefused(_)
            | Self::ConfirmFailed(_)
            | Self::Unplannable(_)
            | Self::Reported { .. }
            | Self::ReportFailed(_)
            | Self::ProofFailed(_)
            | Self::MembershipMoved
            | Self::Stopped => false,
        }
    }
}

/// How the meta group answered the report of the placement at `since`.
fn report_outcome(
    result: Result<ControlApplied, MetaWriteError>,
    since: PlacementEpoch,
) -> StepOutcome {
    match result {
        Ok(applied) => StepOutcome::Reported { since, applied },
        Err(MetaWriteError::Refused(ControlError::StalePlacement { current, .. })) => {
            StepOutcome::StalePlacement {
                reported: since,
                current: current.since(),
            }
        }
        Err(MetaWriteError::Refused(error)) => StepOutcome::ReportRefused(error),
        Err(error) => match error.write_effect() {
            ursula_proto::placement::WriteEffect::MayHaveApplied => {
                StepOutcome::ReportUnknown(Box::new(error))
            }
            ursula_proto::placement::WriteEffect::NotApplied => {
                StepOutcome::ReportFailed(Box::new(error))
            }
        },
    }
}

/// How OpenRaft answered a membership step planned from `expected`.
fn membership_outcome(
    result: Result<ClientWriteResponse<C>, RaftError<C, ClientWriteError<C>>>,
    expected: LogIdOf<C>,
) -> StepOutcome {
    let error = match result {
        Ok(_response) => return StepOutcome::Done,
        Err(RaftError::Fatal(_stopped)) => return StepOutcome::Stopped,
        Err(RaftError::APIError(error)) => error,
    };
    if matches!(
        error,
        ClientWriteError::ChangeMembershipError(
            ChangeMembershipError::EmptyMembership(_)
                | ChangeMembershipError::UnsupportedMembershipTransition(_)
        )
    ) {
        return StepOutcome::Unplannable(Box::new(error));
    }
    let kind = match &error {
        ClientWriteError::ForwardToLeader(_) | ClientWriteError::LogEntryDiscarded(_) => {
            return StepOutcome::NotLeader;
        }
        ClientWriteError::PreconditionFailed(PreconditionFailed::CommittedLeaderIdMismatch {
            ..
        }) => return StepOutcome::NotLeader,
        // OpenRaft guards a voter change's second entry with the joint
        // entry's log id, which the shell never passes: a mismatch on it
        // means the joint entry committed.
        ClientWriteError::PreconditionFailed(PreconditionFailed::LastMembershipLogIdMismatch {
            expected: guarded,
            ..
        }) if guarded.as_ref() != Some(&expected) => return StepOutcome::JointCommitted,
        ClientWriteError::PreconditionFailed(_) => StepFailureKind::PreconditionFailed,
        ClientWriteError::ChangeMembershipError(
            ChangeMembershipError::InProgress(_) | ChangeMembershipError::UncommittedLeaderLog(_),
        ) => StepFailureKind::InProgress,

        // A learner that left, or a change the membership moved under:
        // both re-plan from the membership as it is.
        ClientWriteError::ChangeMembershipError(_) => StepFailureKind::PreconditionFailed,
    };
    StepOutcome::Replan {
        kind,
        error: Box::new(error),
    }
}

/// How many membership steps this node's reconcile shells finished, by step
/// and outcome, the meta group's included. One per node, in its registry,
/// so it only grows. Both keys are bounded: a step kind and a
/// [`StepOutcome`] name.
#[derive(Debug, Clone, Default)]
pub(crate) struct ReconcileSteps(
    Arc<std::sync::Mutex<std::collections::HashMap<(StepKind, &'static str), u64>>>,
);

impl ReconcileSteps {
    fn record(&self, step: StepKind, outcome: &'static str) {
        let mut counts = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let count = counts.entry((step, outcome)).or_insert(0);
        *count = count.saturating_add(1);
    }

    /// Each step kind and outcome name with its count.
    pub(crate) fn snapshot(&self) -> Vec<(StepKind, &'static str, u64)> {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .map(|((step, outcome), count)| (*step, *outcome, *count))
            .collect()
    }
}

/// A confirmation the shell holds: the stamp, and the vote it was taken
/// under on both sides of the barrier.
#[derive(Debug, Clone, Copy)]
struct Confirmation {
    vote: UrsulaVote,
    stamp: ConfirmedStamp,
}

/// A transfer the shell submitted, until it settles.
#[derive(Debug, Clone, Copy)]
struct Transfer {
    to: NodeId,
    vote: UrsulaVote,
    at: crate::rt::time::Instant,
}

/// A step whose future runs.
struct Pending {
    step: MembershipStep,
    kind: StepKind,
    vote: UrsulaVote,
    started: crate::rt::time::Instant,
    slow: bool,
    handle: crate::rt::task::JoinHandle<StepOutcome>,
}

/// The membership reconcile shell of one replica.
pub(crate) struct ReconcileShell<SM, S: ?Sized, G>
where SM: RaftStateMachine<C, SnapshotData = SnapshotData> + 'static
{
    pub(crate) core: ReplicaCore<SM>,
    pub(crate) replica: RaftTarget,
    pub(crate) me: NodeId,
    pub(crate) source: Arc<S>,
    pub(crate) gates: Arc<G>,
    pub(crate) peers: PeerCluster,
    pub(crate) election: ElectionPolicy,
    pub(crate) status: watch::Sender<ReconcileStatus>,
    /// The node's step counts, which each finished step counts into.
    pub(crate) steps: ReconcileSteps,
}

/// A fresh status: waiting as no leader.
pub(crate) fn initial_status(now_ms: u64) -> ReconcileStatus {
    ReconcileStatus {
        state: ReconcileState::Waiting(WaitReason::NotLeader),
        since_ms: now_ms,
        last_failure: None,
        attempts: 0,
        target_since: None,
        view_applied: None,
    }
}

/// The reconcile shell's private state across ticks.
#[derive(Default)]
struct ShellState {
    pending: Option<Pending>,
    transfer: Option<Transfer>,
    confirmed: Option<Confirmation>,
    /// A transfer target skipped for the next plan.
    skip: Option<NodeId>,
    retry: StepRetry<MembershipStep>,
    /// Plan on the next wake-up even when it is not due.
    force: bool,
    /// The placement whose report the meta group applied: nothing is
    /// reported again until the view moves past it.
    reported: Option<PlacementEpoch>,
    /// Confirm the view before the next plan.
    reread: bool,
}

/// What a report names beside the `Converged` step's voters: the placement
/// it reports, and the membership it was planned from.
#[derive(Debug, Clone, Copy)]
struct ReportKey {
    since: PlacementEpoch,
    membership: MembershipLogId,
}

impl<SM, S, G> ReconcileShell<SM, S, G>
where
    SM: RaftStateMachine<C, SnapshotData = SnapshotData> + 'static,
    S: MembershipTargetSource + ?Sized,
    G: PeerGates,
{
    /// Runs the shell until its Raft stops. A step still pending then keeps
    /// running on its own task.
    pub(crate) async fn run(self, interval: Duration, reversions: FollowerLogReversions) {
        let owner = self.core.owner().clone();
        // A replica without a recovery gate has no gate changes: the sender
        // is kept so its receiver never closes.
        let (_no_gate, no_gate) = C::watch_channel(());
        let gate = self
            .core
            .recovery
            .as_ref()
            .map_or(no_gate, |rejoin| rejoin.changes.subscribe());
        let mut tick = LeaderTick::new(
            self.replica,
            self.me.get(),
            owner.server_metrics(),
            owner.metrics(),
            gate,
            interval,
            reversions,
        );
        let mut state = ShellState::default();
        loop {
            let event = crate::rt::select! {
                biased;
                joined = wait_pending(&mut state.pending) => Event::Joined(joined),
                planned = tick.next(|metrics| Box::new(metrics.clone())) => Event::Tick(planned),
            };
            let metrics = match event {
                Event::Joined(joined) => {
                    let Some(pending) = state.pending.take() else {
                        continue;
                    };
                    let outcome = joined.unwrap_or_else(|error| {
                        tracing::error!(
                            target: "ursula::placement",
                            plane = self.replica.plane().as_str_name(),
                            group = self.replica.wire_group_id(),
                            node = self.me.get(),
                            step = ?pending.kind,
                            %error,
                            "a membership step's task ended without an answer"
                        );
                        StepOutcome::Stopped
                    });
                    self.finish(&mut state, pending, outcome);
                    state.force = true;
                    continue;
                }
                Event::Tick(None) => return,
                Event::Tick(Some(Some(metrics))) => metrics,
                Event::Tick(Some(None)) if state.force => {
                    Box::new(owner.metrics().borrow_watched().clone())
                }
                Event::Tick(Some(None)) => {
                    self.warn_slow(&mut state);
                    continue;
                }
            };
            state.force = false;
            self.warn_slow(&mut state);
            self.plan(&mut state, &metrics, interval).await;
        }
    }

    fn warn_slow(&self, state: &mut ShellState) {
        if let Some(pending) = state.pending.as_mut()
            && !pending.slow
            && pending.started.elapsed() >= STEP_SLOW
        {
            pending.slow = true;
            tracing::warn!(
                target: "ursula::placement",
                plane = self.replica.plane().as_str_name(),
                group = self.replica.wire_group_id(),
                node = self.me.get(),
                step = ?pending.kind,
                outcome = "step_slow",
                waited = ?pending.started.elapsed(),
                "a membership step has not been answered yet"
            );
        }
    }

    async fn plan(&self, state: &mut ShellState, metrics: &RaftMetrics<C>, interval: Duration) {
        // Nothing is planned while a step's future runs: its status stays
        // `Stepping` until it answers.
        if state.pending.is_some() {
            return;
        }
        let leading = metrics.current_leader == Some(metrics.id);
        if let Some(transfer) = state.transfer {
            let settled = metrics.vote != transfer.vote
                || !leading
                || transfer.at.elapsed() >= self.core.owner().handoff_settle_time();
            if settled {
                state.transfer = None;
                if leading {
                    // This node leads still, or again after abandoning the
                    // transfer: the target did not take over.
                    state.skip = Some(transfer.to);
                }
            }
        }
        let Some(read) = self.source.target() else {
            return;
        };
        state.confirmed = state.confirmed.filter(|confirmed| {
            confirmed.vote == metrics.vote && confirmed.stamp.covers(read.view)
        });
        // A view the meta group said is behind, or a report that may have
        // applied, is confirmed before anything is planned from it. The
        // state stays as published meanwhile.
        if state.reread && leads_live(metrics) {
            let step = MembershipStep::ConfirmView;
            if state.retry.admit(&step, interval) {
                state.pending = self.start(step, StepKind::ConfirmView, metrics, None);
            }
            return;
        }
        let pending = match state.transfer {
            Some(transfer) => PendingStep::Transfer { to: transfer.to },
            None => PendingStep::None,
        };
        let inputs = if leads_live(metrics) {
            self.inputs(state, metrics, &read.target)
        } else {
            ShellInputs {
                freshness: ViewFreshness::Unconfirmed,
                handoff_ready: BTreeSet::new(),
                unlisted: BTreeSet::new(),
            }
        };
        let mut view = membership_view(
            metrics,
            self.me,
            self.core.owner().handoff_ack_window(),
            pending,
            inputs,
        );
        if let Leadership::Leading(leader) = &mut view.leadership {
            let needed = leader.gates_needed(&read.target);
            if !needed.is_empty() {
                leader.gates = self.gates.gates(self.replica, &needed).await;
            }
        }
        let step = plan_membership_step(&view, &read.target);
        let target_since = match read.target.stage {
            TargetStage::Seeded { since } | TargetStage::Moving { since } => Some(since),
            TargetStage::Settled => None,
        };
        let view_applied = match read.view {
            ViewStage::Empty => None,
            ViewStage::Applied { applied, .. } => Some(applied),
        };
        let (step, report, reconcile) = match step {
            MembershipStep::Wait(reason) => {
                self.publish(
                    ReconcileState::Waiting(reason),
                    target_since,
                    view_applied,
                    false,
                );
                return;
            }
            MembershipStep::Converged { voters } => {
                let (Some(since), Leadership::Leading(leader)) = (target_since, &view.leadership)
                else {
                    state.reported = None;
                    self.publish(ReconcileState::Converged, target_since, view_applied, false);
                    return;
                };
                let EffectiveMembership::Committed {
                    log_id: membership, ..
                } = leader.membership
                else {
                    return;
                };
                if state.reported == Some(since) {
                    // Applied: the view has not caught up with the report.
                    self.publish(ReconcileState::Reporting, target_since, view_applied, false);
                    return;
                }
                let report = ReportKey { since, membership };
                (
                    MembershipStep::Converged { voters },
                    Some(report),
                    ReconcileState::Reporting,
                )
            }
            step => match step.kind() {
                Some(kind) => (step, None, ReconcileState::Stepping { step: kind }),
                // Only a wait has no kind.
                None => return,
            },
        };
        let Some(kind) = step.kind() else {
            return;
        };
        if !state.retry.admit(&step, interval) {
            return;
        }
        self.publish(reconcile, target_since, view_applied, true);
        state.pending = self.start(step, kind, metrics, report);
    }

    /// The handoff targets, and the target voters this node's address book
    /// lacks.
    fn inputs(
        &self,
        state: &mut ShellState,
        metrics: &RaftMetrics<C>,
        target: &MembershipTarget,
    ) -> ShellInputs {
        let group = RaftGroupId(self.replica.wire_group_id());
        let membership = metrics.membership_config.membership();
        let voters: BTreeSet<u64> = membership.voter_ids().collect();
        let skip = state.skip.take();
        let handoff_ready = voters
            .iter()
            .copied()
            .filter(|voter| *voter != metrics.id && Some(NodeId::new(*voter)) != skip)
            .filter(|voter| {
                self.core
                    .validate_handoff(&self.election, group, *voter, metrics)
                    .is_ok()
            })
            .map(NodeId::new)
            .collect();
        let unlisted = target
            .voters
            .iter()
            .filter(|node| *node != self.me && !voters.contains(&node.get()))
            .filter(|node| self.peers.cluster_url(node.get()).is_none())
            .collect();
        let freshness = if state.confirmed.is_some() {
            ViewFreshness::Confirmed
        } else {
            ViewFreshness::Unconfirmed
        };
        ShellInputs {
            freshness,
            handoff_ready,
            unlisted,
        }
    }

    /// Spawns `step`'s future, a report of the `Converged` step when
    /// `report` names one. Its handle is kept until it answers.
    fn start(
        &self,
        step: MembershipStep,
        kind: StepKind,
        metrics: &RaftMetrics<C>,
        report: Option<ReportKey>,
    ) -> Option<Pending> {
        let vote = metrics.vote;
        let handle = match (&step, report) {
            (MembershipStep::Converged { voters }, Some(report)) => {
                self.report(voters.clone(), report, vote)
            }
            (MembershipStep::AddLearners { expected, nodes }, _) => self.change(
                *expected,
                vote,
                ChangeMembers::AddNodes(
                    nodes
                        .iter()
                        .map(|node| (node.get(), EmptyNode {}))
                        .collect(),
                ),
                true,
            ),
            (MembershipStep::ChangeVoters { expected, voters }, _) => self.change(
                *expected,
                vote,
                ChangeMembers::ReplaceAllVoters(voters.iter().map(NodeId::get).collect()),
                false,
            ),
            (MembershipStep::RemoveLearners { expected, nodes }, _) => self.change(
                *expected,
                vote,
                ChangeMembers::RemoveNodes(nodes.iter().map(|node| node.get()).collect()),
                false,
            ),
            (MembershipStep::TransferLeadership { to }, _) => {
                let core = self.core.clone();
                let election = self.election.clone();
                let group = RaftGroupId(self.replica.wire_group_id());
                let to = to.get();
                crate::rt::spawn(async move {
                    match core.transfer_leader(&election, group, to).await {
                        Ok(()) => StepOutcome::Done,
                        Err(error) => StepOutcome::TransferRefused(error),
                    }
                })
            }
            (MembershipStep::ConfirmView, _) => {
                let source = self.source.clone();
                crate::rt::spawn(async move {
                    match source.confirm().await {
                        Ok(stamp) => StepOutcome::Confirmed(stamp),
                        Err(error) => StepOutcome::ConfirmFailed(error),
                    }
                })
            }
            (MembershipStep::Converged { .. }, None) | (MembershipStep::Wait(_), _) => return None,
        };
        tracing::info!(
            target: "ursula::placement",
            plane = self.replica.plane().as_str_name(),
            group = self.replica.wire_group_id(),
            node = self.me.get(),
            step = ?kind,
            outcome = "started",
            planned = ?step,
            %vote,
            "membership step"
        );
        Some(Pending {
            step,
            kind,
            vote,
            started: crate::rt::time::Instant::now(),
            slow: false,
            handle,
        })
    }

    /// Spawns a membership change guarded by the committed leader id of
    /// `vote` and the membership log id `expected`.
    fn change(
        &self,
        expected: MembershipLogId,
        vote: UrsulaVote,
        members: ChangeMembers<u64, EmptyNode>,
        retain: bool,
    ) -> crate::rt::task::JoinHandle<StepOutcome> {
        let owner = self.core.owner().clone();
        let expected = openraft_log_id(expected);
        let preconditions = vec![
            Precondition::CommittedLeaderId {
                committed_leader_id: vote.leader_id().to_committed(),
            },
            Precondition::LastMembershipLogId {
                last_membership_log_id: Some(expected),
            },
        ];
        crate::rt::spawn(async move {
            let result = owner
                .change_membership_if(members, retain, preconditions)
                .await;
            membership_outcome(result, expected)
        })
    }

    /// Spawns the report of `voters`, the committed uniform membership at
    /// `report.membership`, for the placement at `report.since`, planned
    /// under `vote` (design 5.7). A fresh quorum barrier proves the
    /// leadership first, which a deposed leader fails. The vote and the
    /// committed uniform membership are read again after it, and the report
    /// goes only when both are still the ones it was planned from.
    fn report(
        &self,
        voters: VoterSet,
        report: ReportKey,
        vote: UrsulaVote,
    ) -> crate::rt::task::JoinHandle<StepOutcome> {
        let ReportKey { since, membership } = report;
        let core = self.core.clone();
        let source = self.source.clone();
        let group = RaftGroupId(self.replica.wire_group_id());
        let expected = openraft_log_id(membership);
        crate::rt::spawn(async move {
            let proof = match core.confirm_barrier(group).await {
                Ok(proof) => proof,
                Err(error) => return StepOutcome::ProofFailed(Box::new(error)),
            };
            let (now, committed, effective) = {
                let metrics = core.owner().metrics();
                let metrics = metrics.borrow_watched();
                (
                    metrics.vote,
                    *metrics.committed_membership_config.log_id(),
                    *metrics.membership_config.log_id(),
                )
            };
            if proof.vote != vote || now != vote {
                return StepOutcome::NotLeader;
            }
            if committed != Some(expected) || effective != Some(expected) {
                return StepOutcome::MembershipMoved;
            }
            let convergence = Convergence {
                since,
                voters,
                membership,
            };
            report_outcome(source.report(convergence).await, since)
        })
    }

    /// Records how `pending` ended.
    fn finish(&self, state: &mut ShellState, pending: Pending, outcome: StepOutcome) {
        let (vote, leading) = {
            let metrics = self.core.owner().metrics();
            let metrics = metrics.borrow_watched();
            (metrics.vote, metrics.current_leader == Some(metrics.id))
        };
        if outcome.rereads() {
            state.reread = true;
        }
        match &outcome {
            StepOutcome::Done => {
                if let MembershipStep::TransferLeadership { to } = pending.step {
                    state.transfer = Some(Transfer {
                        to,
                        vote: pending.vote,
                        at: crate::rt::time::Instant::now(),
                    });
                }
            }
            StepOutcome::Confirmed(stamp) => {
                // The view was read again, whichever leadership the barrier
                // ran in.
                state.reread = false;
                // A confirmation counts only when it ran wholly in one
                // leadership: the vote after the barrier is the vote before
                // it.
                if vote == pending.vote && leading {
                    state.confirmed = Some(Confirmation {
                        vote: pending.vote,
                        stamp: *stamp,
                    });
                }
            }
            StepOutcome::TransferRefused(_) => {
                if let MembershipStep::TransferLeadership { to } = pending.step {
                    state.skip = Some(to);
                }
            }
            StepOutcome::Reported { since, .. } => state.reported = Some(*since),
            StepOutcome::StalePlacement { reported, current } => {
                // A report crossed a placement change: the confirmation the
                // shell holds is of an older view.
                state.confirmed = None;
                let view_applied = self.status.borrow().view_applied;
                self.publish(
                    ReconcileState::StaleView {
                        reported: *reported,
                        current: *current,
                    },
                    Some(*reported),
                    view_applied,
                    false,
                );
            }
            StepOutcome::Unplannable(error) => tracing::error!(
                target: "ursula::placement",
                plane = self.replica.plane().as_str_name(),
                group = self.replica.wire_group_id(),
                node = self.me.get(),
                step = ?pending.kind,
                planned = ?pending.step,
                %error,
                "OpenRaft refused a planned membership step that no plan should make"
            ),
            StepOutcome::JointCommitted
            | StepOutcome::Replan { .. }
            | StepOutcome::NotLeader
            | StepOutcome::ConfirmFailed(_)
            | StepOutcome::ReportRefused(_)
            | StepOutcome::ReportUnknown(_)
            | StepOutcome::ReportFailed(_)
            | StepOutcome::ProofFailed(_)
            | StepOutcome::MembershipMoved
            | StepOutcome::Stopped => {}
        }
        let detail = match &outcome {
            StepOutcome::Replan { error, .. } => Some(error.to_string()),
            StepOutcome::TransferRefused(error) => Some(error.to_string()),
            StepOutcome::ConfirmFailed(error) => Some(error.to_string()),
            StepOutcome::Reported { applied, .. } => Some(format!("{applied:?}")),
            StepOutcome::ReportRefused(error) => Some(error.to_string()),
            StepOutcome::ReportUnknown(error) | StepOutcome::ReportFailed(error) => {
                Some(error.to_string())
            }
            StepOutcome::ProofFailed(error) => Some(error.to_string()),
            _ => None,
        };
        self.steps.record(pending.kind, outcome.name());
        tracing::info!(
            target: "ursula::placement",
            plane = self.replica.plane().as_str_name(),
            group = self.replica.wire_group_id(),
            node = self.me.get(),
            step = ?pending.kind,
            outcome = outcome.name(),
            planned = ?pending.step,
            waited = ?pending.started.elapsed(),
            detail,
            "membership step"
        );
        if let Some(kind) = outcome.failure() {
            let failure = StepFailure {
                step: pending.kind,
                kind,
                at_ms: ursula_runtime::unix_time_ms(),
            };
            self.status.send_if_modified(|status| {
                status.last_failure = Some(failure);
                true
            });
        }
    }

    /// Publishes `reconcile`: a new state restarts `since_ms` and the
    /// attempts, which count the steps started in one state.
    fn publish(
        &self,
        reconcile: ReconcileState,
        target_since: Option<PlacementEpoch>,
        view_applied: Option<ursula_proto::placement::MetaLogId>,
        attempt: bool,
    ) {
        self.status.send_if_modified(|status| {
            let before = status.clone();
            if status.state != reconcile {
                status.state = reconcile;
                status.since_ms = ursula_runtime::unix_time_ms();
                status.attempts = 0;
            }
            if attempt {
                status.attempts = status.attempts.saturating_add(1);
            }
            status.target_since = target_since;
            status.view_applied = view_applied;
            *status != before
        });
    }
}

enum Event {
    Joined(Result<StepOutcome, crate::rt::task::JoinError>),
    Tick(Option<Option<Box<RaftMetrics<C>>>>),
}

/// The answer of the pending step, never ready without one: an error when
/// its task panicked or was cancelled.
async fn wait_pending(
    pending: &mut Option<Pending>,
) -> Result<StepOutcome, crate::rt::task::JoinError> {
    match pending {
        Some(pending) => (&mut pending.handle).await,
        None => std::future::pending().await,
    }
}

#[cfg(all(test, not(madsim)))]
#[path = "reconcile_tests.rs"]
mod tests;

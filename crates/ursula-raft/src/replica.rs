//! One replica of a Raft group on this node, of either plane, and the
//! screens every inbound RPC to it passes.
//!
//! A [`ReplicaCore`] holds what every plane's replica has: its owner
//! mailbox behind its ReadIndex barrier, its recovery gate, its history and
//! its apply health. The screens of the recovery gate (the vote floor, the
//! commit screen of appends and snapshot installs, the vote screen), the
//! handoff policy, incoming leadership transfers, the recovery barrier
//! with its history fence and the orchestration of a recovery plan
//! ([`ReplicaCore::accept_plan`]) run here, generic over the state machine,
//! so a second plane reuses them instead of copying them. The data plane's
//! registry entry wraps one core with its data-only resources.
//!
//! [`ReplicaStateMachine`] is what a plane's state machine tells the shared
//! drivers: the history event its history driver proposes, and how it reads
//! that event's refusal.

use std::sync::Arc;
use std::time::Duration;

use futures_util::future::BoxFuture;
use openraft::RaftMetrics;
use openraft::alias::LogIdOf;
use openraft::alias::StoredMembershipOf;
use openraft::raft::AppendEntriesRequest;
use openraft::raft::AppendEntriesResponse;
use openraft::raft::TransferLeaderRequest;
use openraft::raft::VoteRequest;
use openraft::raft::VoteResponse;
use openraft::rt::WatchReceiver;
use openraft::storage::RaftStateMachine;
use ursula_proto::admin::RecoveryPlanReport;
use ursula_proto::admin::RecoveryPlanRequest;
use ursula_runtime::GroupEngineError;
use ursula_runtime::GroupInfraError;
use ursula_runtime::HistoryEvent;
use ursula_shard::RaftGroupId;
use ursula_shard::RaftTarget;

use crate::apply_failure::ApplyHealth;
use crate::election::ElectionPolicy;
use crate::election::IncomingTransferError;
use crate::election::LeadershipTransferError;
use crate::history::GroupHistory;
use crate::owner::OwnerRaftHandle;
use crate::read_index::ReadIndexBarrier;
use crate::registry::QuorumProofError;
use crate::rejoin::GroupRejoin;
use crate::rejoin::PlanTaken;
use crate::rejoin::RaftPlanCore;
use crate::rejoin::RecoveryGateError;
use crate::state_machine::RaftGroupStateMachine;
use crate::types::RaftCommand;
use crate::types::RaftGroupResponse;
use crate::types::SnapshotData;
use crate::types::UrsulaRaftTypeConfig as C;
use crate::types::UrsulaVote;
use crate::types::VoteRound;

/// The history event a leader's history driver proposes
/// ([`crate::history::run_history_driver`]), before its plane encodes it.
#[derive(Debug, Clone, Copy)]
pub(crate) struct HistoryProposal {
    /// The new history's id, drawn by the proposer.
    pub(crate) history: std::num::NonZeroU64,
    pub(crate) now_ms: u64,
    /// The id of the history this one replaces, 0 at the genesis.
    pub(crate) previous: u64,
    /// The last log index the new history keeps from the one it replaces.
    pub(crate) kept: u64,
    pub(crate) leader: ursula_stream::HistoryLeader,
    pub(crate) cause: HistoryCause,
}

/// Why a leader proposes a history event.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum HistoryCause {
    /// The group's genesis.
    Genesis,
    /// The recovery plan at `point`.
    Plan { point: u64 },
}

/// A plane's state machine, as the drivers every replica runs see it.
pub(crate) trait ReplicaStateMachine:
    RaftStateMachine<C, SnapshotData = SnapshotData> + Send + Sync + 'static
{
    /// What a written command of this plane answers.
    type Answer: Send + 'static;

    /// The command that proposes `proposal` on this plane.
    fn history_command(proposal: HistoryProposal) -> RaftCommand;
    /// Whether the applied answer to a history command refused it: the
    /// event named another predecessor than the group's history, or on the
    /// meta plane another leader than its entry's, and applied nothing, so
    /// the driver proposes again.
    fn history_refused(response: &RaftGroupResponse) -> bool;
    /// The answer of a written command in `response`, of the kind
    /// `expected`, or the refusal of a response of another kind.
    fn answer(
        response: RaftGroupResponse,
        expected: ursula_runtime::ResponseKind,
    ) -> Result<Result<Self::Answer, GroupEngineError>, GroupEngineError>;
}

impl ReplicaStateMachine for RaftGroupStateMachine {
    type Answer = ursula_runtime::GroupWriteResponse;

    fn answer(
        response: RaftGroupResponse,
        expected: ursula_runtime::ResponseKind,
    ) -> Result<Result<Self::Answer, GroupEngineError>, GroupEngineError> {
        crate::forward::write_result_from_raft_response(response, expected)
    }

    fn history_command(proposal: HistoryProposal) -> RaftCommand {
        let HistoryProposal {
            history,
            now_ms,
            previous,
            kept,
            leader,
            cause: _,
        } = proposal;
        RaftCommand::Stream(ursula_stream::StreamCommand::NewHistory {
            history,
            now_ms,
            previous,
            kept,
            leader,
        })
    }

    fn history_refused(response: &RaftGroupResponse) -> bool {
        matches!(response, RaftGroupResponse::Write(Err(_)))
    }
}

/// How long a recovery plan's accept waits for a restarted replica to
/// publish the membership it rebuilt from its cut log (#613).
const RESTARTED_MEMBERSHIP_WAIT: Duration = Duration::from_secs(5);

/// One replica's gate screens and recovery barrier, of either plane.
pub(crate) struct ReplicaCore<
    SM: RaftStateMachine<C, SnapshotData = SnapshotData> = RaftGroupStateMachine,
> {
    /// The replica's ReadIndex barrier, which owns its mailbox.
    pub(crate) barrier: Arc<ReadIndexBarrier<SM>>,
    pub(crate) recovery: Option<Arc<GroupRejoin>>,
    /// The group's history on this replica (#581 section 4.5): a leader
    /// whose history is pending proposes, serves and confirms nothing.
    pub(crate) history: Arc<GroupHistory>,
    pub(crate) apply_health: ApplyHealth,
    /// What the replica's membership reconcile shell publishes, served in
    /// its metrics. `None` for a replica that runs no shell.
    pub(crate) reconcile:
        Option<crate::rt::sync::watch::Receiver<ursula_proto::placement::ReconcileStatus>>,
    /// The snapshot sends of the replica's network clients that run.
    pub(crate) snapshot_sends: crate::openraft_work::SnapshotSends,
}

impl<SM: RaftStateMachine<C, SnapshotData = SnapshotData>> Clone for ReplicaCore<SM> {
    fn clone(&self) -> Self {
        Self {
            barrier: self.barrier.clone(),
            recovery: self.recovery.clone(),
            history: self.history.clone(),
            apply_health: self.apply_health.clone(),
            reconcile: self.reconcile.clone(),
            snapshot_sends: self.snapshot_sends.clone(),
        }
    }
}

impl<SM: RaftStateMachine<C, SnapshotData = SnapshotData>> std::fmt::Debug for ReplicaCore<SM> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReplicaCore")
            .field("recovery", &self.recovery.is_some())
            .field("history", &self.history)
            .finish_non_exhaustive()
    }
}

impl<SM: RaftStateMachine<C, SnapshotData = SnapshotData>> ReplicaCore<SM> {
    pub(crate) fn owner(&self) -> &OwnerRaftHandle<SM> {
        self.barrier.owner()
    }

    /// The recovery gate that governs this node's participation. A replica
    /// stopped by an apply failure reports the stop instead: its gate can
    /// no longer open, and it must not hold back the node's other replicas.
    pub(crate) fn participating_gate(&self) -> Option<&Arc<GroupRejoin>> {
        if self.apply_health.failure().is_some() {
            return None;
        }
        self.recovery.as_ref()
    }

    pub(crate) fn check_recovery_vote(
        &self,
        group: RaftTarget,
        vote: UrsulaVote,
    ) -> Result<Option<UrsulaVote>, GroupEngineError> {
        let Some(rejoin) = &self.recovery else {
            return Ok(None);
        };
        // A replica restarting for a plan takes no replication, and one
        // holding a plan none from a leader elected before it (M1, #581
        // review).
        if let Some(refusal) = rejoin.replication_refusal(&vote) {
            return Err(GroupEngineError::Infra(refusal));
        }
        let floor = rejoin.recovery_vote().ok_or(GroupEngineError::Infra(
            GroupInfraError::RecoveryVoteFloor { group },
        ))?;
        Ok((vote < floor).then_some(floor))
    }

    /// An inbound append, through the recovery gate's screens: the vote
    /// floor, then, in the owner job, a plan that closed the replica
    /// meanwhile and the commit screen (#613).
    pub(crate) async fn append_entries(
        &self,
        group: RaftTarget,
        request: AppendEntriesRequest<C>,
    ) -> Result<AppendEntriesResponse<C>, GroupEngineError> {
        // Contact counts before any screen: a leader whose appends this
        // replica refuses still holds its readiness back.
        let append = self
            .recovery
            .as_ref()
            .and_then(|rejoin| rejoin.observe_leader_contact(&request));
        if let Some(floor) = self.check_recovery_vote(group, request.vote)? {
            return Ok(AppendEntriesResponse::HigherVote(floor));
        }
        let raft = self.owner().clone();
        let rejoin = self.recovery.clone();
        let response = raft.call(move |raft| async move {
            if let Some(rejoin) = rejoin {
                // A recovery plan may have closed the replica since the
                // check above: an entry it acknowledged now would be cut, or
                // come from a leader elected before the plan.
                if let Some(refusal) = rejoin.replication_refusal(&request.vote) {
                    return Err(refusal);
                }
                let marker = rejoin
                    .effective_marker(&WatchReceiver::borrow_watched(&raft.metrics()))
                    .inspect_err(|refusal| {
                        tracing::warn!(%refusal, "recovery: refused an append while the log lags");
                    })?;
                if let Some(refusal) =
                    rejoin.committed_overwrite(crate::rejoin::Incoming::Append(&request), marker)
                {
                    tracing::error!(%refusal, "recovery: refused an append below the commit marker");
                    return Err(refusal);
                }
                rejoin.observe_inbound_append(&request);
            }
            Ok(raft.append_entries(request).await)
        })
        .await
        .map_err(crate::owner::owner_stopped)?
        .map_err(GroupEngineError::Infra)?
        .map_err(crate::owner::owner_raft_error)?;
        if let (Some(rejoin), Some(append)) = (&self.recovery, append) {
            rejoin.observe_append_answer(append, &response);
        }
        Ok(response)
    }

    /// An inbound vote or pre-vote for `group` (`round`), through the node
    /// fence's hold and the recovery gate's vote screen. While the fence
    /// holds the replica out ([`crate::predecessor_wait::fence_holds`]) it
    /// grants no vote: in any group or meta while this run owes its
    /// predecessor wait, whose predecessor may still vote as this node, and
    /// in a data group while this run's unbound root awaits its binding.
    /// The bootstrap probe vote is still answered, as the gate answers it.
    /// A pre-vote passes the same screens as a vote, so a replica that
    /// would refuse a candidate's vote also refuses its pre-vote, and the
    /// candidate does not raise its term for an election it cannot win.
    pub(crate) async fn vote(
        &self,
        group: RaftTarget,
        round: VoteRound,
        request: VoteRequest<C>,
    ) -> Result<VoteResponse<C>, GroupEngineError> {
        let raft = self.owner().clone();
        let rejoin = self.recovery.clone();
        let fence = raft.fence().clone();
        raft.call(move |raft| async move {
            let probe = request == crate::rejoin::bootstrap_probe_vote();
            if crate::predecessor_wait::fence_holds(&fence, group) && !probe {
                let metrics = WatchReceiver::borrow_watched(&raft.metrics()).clone();
                let last_log = WatchReceiver::borrow_watched(&raft.data_metrics()).last_log;
                tracing::info!(
                    node_id = metrics.id,
                    candidate = %request.vote,
                    round = round.rpc(),
                    "refusing a vote while the node fence holds this replica out: a predecessor that \
                     may still run is waited out, or this unbound root awaits its binding"
                );
                return Ok(VoteResponse::new(metrics.vote, last_log, false));
            }
            if let Some(refusal) = rejoin.and_then(|rejoin| rejoin.screen_vote(&request)) {
                return Ok(refusal);
            }
            match round {
                VoteRound::Vote => raft.vote(request).await,
                VoteRound::PreVote => raft.pre_vote(request).await,
            }
        })
        .await
        .map_err(crate::owner::owner_stopped)?
        .map_err(crate::owner::owner_raft_error)
    }

    /// The screens of a snapshot install, run in the owner job right before
    /// the install: a recovery plan that closed the replica while the
    /// install waited, and the commit screen (#613), which a snapshot whose
    /// history is known (`incoming_history`) passes like an append does.
    pub(crate) async fn screen_install(
        &self,
        vote: UrsulaVote,
        snapshot_last: Option<LogIdOf<C>>,
        incoming_history: Option<Option<HistoryEvent>>,
    ) -> Result<(), GroupInfraError> {
        let Some(rejoin) = self.recovery.as_ref() else {
            return Ok(());
        };
        if let Some(refusal) = rejoin.replication_refusal(&vote) {
            return Err(refusal);
        }
        // #613: like an append, a snapshot never replaces entries this
        // replica knows are committed.
        let Some(incoming) = incoming_history else {
            return Ok(());
        };
        // The marker and the applied index, read once: the history at the
        // marker is read against them.
        let (marker, applied_index) = {
            let watched = self.owner().metrics();
            let metrics = WatchReceiver::borrow_watched(&watched);
            (
                rejoin.effective_marker(&metrics),
                metrics.last_applied.map(|applied| applied.index),
            )
        };
        let marker = marker.inspect_err(|refusal| {
            tracing::warn!(
                %refusal,
                "recovery: refused a snapshot install while the log lags"
            );
        })?;
        let local = rejoin
            .history_at_marker(self.history.applied(), applied_index, marker)
            .await;
        if let Some(refusal) = rejoin.committed_overwrite(
            crate::rejoin::Incoming::Snapshot {
                last: snapshot_last,
                history: incoming,
                local,
            },
            marker,
        ) {
            tracing::error!(
                %refusal,
                "recovery: refused a snapshot install that would replace committed entries"
            );
            rejoin.record_refused_snapshot(snapshot_last, &refusal);
            return Err(refusal);
        }
        Ok(())
    }

    /// Hands `request` to this replica's Raft once
    /// [`ElectionPolicy::validate_incoming_transfer`] accepts it, so the gate
    /// checked is the gate of the Raft that takes the transfer.
    pub(crate) async fn deliver_transfer(
        &self,
        election: &ElectionPolicy,
        group: RaftGroupId,
        request: TransferLeaderRequest<C>,
    ) -> Result<openraft::raft::TransferLeaderResponse<C>, IncomingTransferError> {
        let raft = self.owner();
        // Read first: a metrics guard held through the policy check would hold up the core's
        // next metrics update while the check takes the recovery gate's locks.
        let node_id = raft.metrics().borrow_watched().id;
        election.validate_incoming_transfer(group, node_id, &request, self.recovery.as_deref())?;
        raft.handle_transfer_leader(request)
            .await
            .map_err(|source| IncomingTransferError::Raft { group, source })
    }

    /// This replica's metrics snapshot as group `raft_group_id`: its Raft
    /// metrics, recovery gate, history and apply health, with the log
    /// progress since its last snapshot (`log`) and the membership its
    /// state machine applied last (`applied`), which each plane tracks its
    /// own way.
    pub(crate) fn metrics_snapshot(
        &self,
        raft_group_id: u32,
        election: &ElectionPolicy,
        log: crate::snapshot_cadence::GroupLogProgress,
        applied: StoredMembershipOf<C>,
    ) -> crate::types::RaftGroupMetricsSnapshot {
        let raft = self.owner();
        let metrics = raft.metrics().borrow_watched().clone();
        // `RaftMetrics` carry only the index of the last log id. The data
        // metrics carry the whole id, so its term and index match.
        let last_log = raft.data_metrics().borrow_watched().last_log;
        let membership = metrics.membership_config.membership();
        let apply_failure = self.apply_health.failure();
        let apply_running = apply_failure.is_none();
        let running = apply_running
            && metrics.running_state.is_ok()
            && metrics.state != openraft::ServerState::Shutdown;
        let progress = crate::registry::log_progress_snapshot;
        let recovery = self.recovery.as_deref();
        crate::types::RaftGroupMetricsSnapshot {
            apply_failure,
            raft_group_id,
            node_id: metrics.id,
            current_term: metrics.current_term,
            // A stopped replica leads nothing. Its last observation is
            // stale, so leader counts and drains must not see it.
            current_leader: metrics.current_leader.filter(|_| running),
            last_log: last_log.map(progress),
            committed: metrics.local_committed.map(progress),
            last_applied: metrics.last_applied.map(progress),
            snapshot: metrics.snapshot.map(progress),
            purged: metrics.purged.map(progress),
            voter_ids: membership.voter_ids().collect(),
            voter_configurations: membership.get_joint_config().clone(),
            committed_voter_configurations: crate::rejoin::kept_membership_of(
                recovery,
                metrics.local_committed.map(|id| id.index),
                applied,
            )
            .membership()
            .get_joint_config()
            .clone(),
            learner_ids: membership.learner_ids().collect(),
            maintenance: crate::types::RaftGroupMaintenanceState {
                running,
                recovery_ready: apply_running && recovery.is_none_or(GroupRejoin::vote_gate_open),
                accepting_transfers: apply_running && election.may_accept_transfer(recovery),
                membership_joint: membership.get_joint_config().len() != 1,
                membership_log_index: metrics.membership_config.log_id().map(|id| id.index()),
                stopped_for_operator: recovery.is_some_and(|rejoin| rejoin.status().is_stalled()),
                // A replica without a recovery gate runs only in tests.
                leader_acknowledgement: recovery.map_or(
                    ursula_proto::admin::LeaderAcknowledgement::Acknowledged,
                    |rejoin| rejoin.leader_acknowledgement(crate::rejoin::leads_live(&metrics)),
                ),
            },
            log,
            history: self
                .history
                .applied()
                .map(|event| ursula_proto::admin::RaftGroupHistory {
                    event_index: event.event_index,
                    imported: event.imported.is_some(),
                }),
            recovery: recovery
                .and_then(GroupRejoin::recovery_record)
                .map(|record| ursula_proto::admin::RaftGroupRecovery {
                    leader: record.leader,
                    point: record.point,
                    leader_term: record.leader_term,
                    cut_pending: matches!(record.cut, crate::log_store::PlanCut::Pending { .. }),
                }),
            leads_live: crate::rejoin::leads_live(&metrics),
            needs_vote_floor: recovery.is_some_and(GroupRejoin::needs_vote_floor),
            recovery_gate: recovery.map_or(
                ursula_proto::admin::RecoveryGateStatus::Open,
                GroupRejoin::status,
            ),
            reconcile: self
                .reconcile
                .as_ref()
                .map(|status| status.borrow().clone()),
        }
    }

    /// The Raft metrics a handoff is checked against, from one read.
    pub(crate) fn handoff_view(&self) -> RaftMetrics<C> {
        self.owner().metrics().borrow_watched().clone()
    }

    /// Whether this replica could hand its leadership to `target` now, by
    /// the election policy, as `metrics` show it.
    pub(crate) fn validate_handoff(
        &self,
        election: &ElectionPolicy,
        group: RaftGroupId,
        target: u64,
        metrics: &RaftMetrics<C>,
    ) -> Result<(), LeadershipTransferError> {
        let target_ack_age = metrics
            .heartbeat
            .as_ref()
            .and_then(|acks| acks.get(&target))
            .and_then(|acked| acked.as_ref())
            .map(|acked| openraft::Instant::elapsed(&**acked));
        election.validate_handoff(
            group,
            target,
            metrics,
            target_ack_age,
            self.owner().handoff_ack_window(),
        )
    }

    /// Hands this replica's leadership to `target` under the one handoff
    /// policy ([`Self::validate_handoff`] on one view of the metrics), and
    /// abandons the transfer once its deadline passed without moving the
    /// leadership. The receiver checks its own recovery gate and OpenRaft
    /// never reports its refusal, so the deadline is the only answer.
    pub(crate) async fn transfer_leader(
        &self,
        election: &ElectionPolicy,
        group: RaftGroupId,
        target: u64,
    ) -> Result<(), LeadershipTransferError> {
        let metrics = self.handoff_view();
        self.validate_handoff(election, group, target, &metrics)?;
        let owner = self.owner();
        owner
            .trigger()
            .transfer_leader(target)
            .await
            .map_err(|source| LeadershipTransferError::Raft { group, source })?;
        owner.abandon_stalled_transfer(metrics.vote, target);
        Ok(())
    }

    /// The recovery barrier this replica answers as its group's leader. A
    /// leader whose history is pending confirms no barrier: a gate opened on
    /// its state would follow a history it has not settled (#581 section
    /// 4.5, #577 N1). It waits for the event a moment, as a fenced proposal
    /// does.
    pub(crate) async fn confirm_barrier(
        &self,
        group: RaftGroupId,
    ) -> Result<crate::BarrierAnswer, QuorumProofError> {
        if self.history.admit_on(self.owner().metrics()).await.is_err() {
            return Err(QuorumProofError::HistoryPending { group });
        }
        crate::registry::confirm_recovery_barrier(group, self.owner(), self.barrier.as_ref()).await
    }

    /// Lets the node's election policy decide whether this replica
    /// campaigns, against its own recovery gate.
    pub(crate) fn refresh_elections(&self, election: &ElectionPolicy) {
        election.refresh(self.owner(), self.recovery.clone());
    }

    /// Takes a recovery plan on this replica (#581 section 4.5; see
    /// [`GroupRejoin::accept_recovery_plan`]) as one owner job: its leader
    /// fence learns the plan, and its elections follow it. `applied` is the
    /// membership its state machine applied.
    ///
    /// A plan that cuts the log restarts the replica before it answers:
    /// `restart` stops it, runs the cut and returns the restarted replica,
    /// `None` when none is registered. This core is dropped first. A plan
    /// whose leader the cut leaves no voter is dropped again (#613).
    pub(crate) async fn accept_plan<'r, R>(
        self,
        election: &ElectionPolicy,
        group: RaftTarget,
        plan: &RecoveryPlanRequest,
        applied: StoredMembershipOf<C>,
        restart: R,
    ) -> Result<RecoveryPlanReport, RecoveryGateError>
    where
        R: FnOnce() -> BoxFuture<'r, Result<Option<ReplicaCore<SM>>, RecoveryGateError>>,
    {
        let rejoin = self
            .recovery
            .clone()
            .ok_or(RecoveryGateError::NotRegistered { group })?;
        let owner = self.owner().clone();
        let plan = *plan;
        let job = rejoin.clone();
        let history = self.history.clone();
        // m1 (#581 review): the job, with its rollback, runs in a task of
        // its own, so a caller that stops waiting never cancels it half
        // way. The restart that follows a cut runs in the caller's future:
        // the HTTP route runs the plan in a task of its own (n6).
        let job = crate::rt::spawn(async move {
            owner
                .call(move |raft| async move {
                    let core = RaftPlanCore { raft: &raft, group };
                    job.accept_recovery_plan(&plan, &core, &history, applied)
                        .await
                })
                .await
                .map_err(|source| RecoveryGateError::OwnerStopped { group, source })?
        });
        let taken = job
            .await
            .map_err(|_aborted| RecoveryGateError::JobAborted { group })
            .and_then(|taken| taken);
        // m6 (#581 review): the job stopped the core campaigning; the
        // node's election policy decides again, taken or refused.
        self.refresh_elections(election);
        let report = match taken? {
            PlanTaken::Holds(report) => {
                self.refresh_elections(election);
                return Ok(report);
            }
            PlanTaken::Restart(report) => report,
        };
        // B1 (#581 review): the cut runs before the plan answers. The
        // group's engine stops, its log store closes, and the new replica's
        // recovery gate cuts the log before its Raft core starts, then takes
        // the plan's gate. A cut that cannot run drops the plan and closes
        // the gate (m3).
        drop(rejoin);
        drop(self);
        let restarted = restart()
            .await?
            .ok_or(RecoveryGateError::NotRegistered { group })?;
        let restarted_gate = restarted
            .recovery
            .as_ref()
            .ok_or(RecoveryGateError::NotRegistered { group })?;
        if let Some((committed, held)) = restarted_gate.cut_refused() {
            return Err(RecoveryGateError::CutRefused {
                group,
                committed,
                held,
            });
        }
        // #613: the restart rebuilt the membership from the cut log. A
        // plan's leader that it leaves no voter can never campaign: drop
        // the plan rather than wedge its members. The restarted core
        // publishes the membership it rebuilt once it runs; until then its
        // metrics hold none. Off under the R2 negative control, as before
        // #613.
        let checks_kept = !ursula_stream::sim_controls::off(
            ursula_stream::sim_controls::Defence::PlanKeptMembership,
        );
        if checks_kept && plan.cut && plan.leader == report.node_id {
            let published = restarted
                .owner()
                .wait(Some(RESTARTED_MEMBERSHIP_WAIT))
                .metrics(
                    |metrics| metrics.membership_config.log_id().is_some(),
                    "the restarted replica publishes its membership",
                )
                .await;
            // A restart whose membership never shows cannot be shown to
            // campaign: fail closed, as for no vote.
            let refusal = match published {
                Ok(metrics)
                    if metrics
                        .membership_config
                        .membership()
                        .voter_ids()
                        .any(|voter| voter == plan.leader) =>
                {
                    None
                }
                Ok(_left_without_vote) => Some(RecoveryGateError::CutLeftNoVote { group }),
                Err(_timed_out) => Some(RecoveryGateError::CutMembershipUnknown { group }),
            };
            if let Some(refusal) = refusal {
                restarted_gate
                    .drop_plan_left_without_vote(&restarted.history)
                    .await?;
                restarted.refresh_elections(election);
                return Err(refusal);
            }
        }
        restarted.refresh_elections(election);
        Ok(report)
    }
}

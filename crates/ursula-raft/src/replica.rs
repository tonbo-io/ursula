//! One replica of a Raft group on this node, of either plane, and the
//! screens every inbound RPC to it passes.
//!
//! A [`ReplicaCore`] holds what every plane's replica has: its owner
//! mailbox behind its ReadIndex barrier, its recovery gate, its history and
//! its apply health. The screens of the recovery gate (the vote floor, the
//! commit screen of appends and snapshot installs, the vote screen), the
//! handoff policy, incoming leadership transfers and the recovery barrier
//! with its history fence run here, generic over the state machine, so a
//! second plane reuses them instead of copying them. The data plane's
//! registry entry wraps one core with its data-only resources.
//!
//! [`ReplicaStateMachine`] is what a plane's state machine tells the shared
//! drivers: the history event its history driver proposes, and how it reads
//! that event's refusal.

use std::sync::Arc;

use openraft::RaftMetrics;
use openraft::alias::LogIdOf;
use openraft::raft::AppendEntriesRequest;
use openraft::raft::AppendEntriesResponse;
use openraft::raft::TransferLeaderRequest;
use openraft::raft::VoteRequest;
use openraft::raft::VoteResponse;
use openraft::rt::WatchReceiver;
use openraft::storage::RaftStateMachine;
use ursula_runtime::GroupEngineError;
use ursula_runtime::GroupInfraError;
use ursula_runtime::HistoryEvent;
use ursula_shard::RaftGroupId;

use crate::apply_failure::ApplyHealth;
use crate::election::ElectionPolicy;
use crate::election::IncomingTransferError;
use crate::election::LeadershipTransferError;
use crate::history::GroupHistory;
use crate::owner::OwnerRaftHandle;
use crate::read_index::ReadIndexBarrier;
use crate::registry::QuorumProofError;
use crate::rejoin::GroupRejoin;
use crate::state_machine::RaftGroupStateMachine;
use crate::types::RaftCommand;
use crate::types::RaftGroupResponse;
use crate::types::SnapshotData;
use crate::types::UrsulaRaftTypeConfig as C;
use crate::types::UrsulaVote;

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
    #[expect(
        dead_code,
        reason = "the data plane's event does not record its cause; the meta plane's reads it"
    )]
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
    /// The command that proposes `proposal` on this plane.
    fn history_command(proposal: HistoryProposal) -> RaftCommand;
    /// Whether the applied answer to a history command refused it: the
    /// event named another predecessor than the group's history and applied
    /// nothing, so the driver proposes again.
    fn history_refused(response: &RaftGroupResponse) -> bool;
}

impl ReplicaStateMachine for RaftGroupStateMachine {
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
}

impl<SM: RaftStateMachine<C, SnapshotData = SnapshotData>> Clone for ReplicaCore<SM> {
    fn clone(&self) -> Self {
        Self {
            barrier: self.barrier.clone(),
            recovery: self.recovery.clone(),
            history: self.history.clone(),
            apply_health: self.apply_health.clone(),
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

    pub(crate) fn check_recovery_vote(
        &self,
        group: RaftGroupId,
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
            GroupInfraError::RecoveryVoteFloor {
                raft_group_id: group,
            },
        ))?;
        Ok((vote < floor).then_some(floor))
    }

    /// An inbound append, through the recovery gate's screens: the vote
    /// floor, then, in the owner job, a plan that closed the replica
    /// meanwhile and the commit screen (#613).
    pub(crate) async fn append_entries(
        &self,
        group: RaftGroupId,
        request: AppendEntriesRequest<C>,
    ) -> Result<AppendEntriesResponse<C>, GroupEngineError> {
        if let Some(floor) = self.check_recovery_vote(group, request.vote)? {
            return Ok(AppendEntriesResponse::HigherVote(floor));
        }
        let raft = self.owner().clone();
        let rejoin = self.recovery.clone();
        raft.call(move |raft| async move {
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
        .map_err(crate::owner::owner_raft_error)
    }

    /// An inbound vote, through the recovery gate's vote screen.
    pub(crate) async fn vote(
        &self,
        request: VoteRequest<C>,
    ) -> Result<VoteResponse<C>, GroupEngineError> {
        let raft = self.owner().clone();
        let rejoin = self.recovery.clone();
        raft.call(move |raft| async move {
            if let Some(refusal) = rejoin.and_then(|rejoin| rejoin.screen_vote(&request)) {
                return Ok(refusal);
            }
            raft.vote(request).await
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
}

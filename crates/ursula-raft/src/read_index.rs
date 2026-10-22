//! Coalesced ReadIndex confirmation for one Raft group (D10).
//!
//! OpenRaft queues each `get_read_linearizer` call until a heartbeat round
//! that began after it reaches a quorum, and reads queued together share
//! that round. This barrier still runs at most one confirmation round per
//! group at a time, so concurrent readers make one call and one wait for the
//! local apply. Readers that arrive while a round is in flight share the next
//! round, which starts when the current one ends. A reader never shares a
//! round that started before it arrived, so every round's read index is
//! taken after each of its readers began.
//!
//! The runtime calls the barrier before it queues a linearizable read
//! (`ursula_runtime::LinearizableReadBarrier`), and the engine calls it
//! itself for reads that arrive without a confirmed index (forwarded gRPC
//! reads, direct engine calls).

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::PoisonError;
use std::time::Duration;

use futures_util::FutureExt;
use futures_util::future::BoxFuture;
use futures_util::future::Shared;
use openraft::LinearizerOption;
use openraft::Raft;
use openraft::ReadPolicy;
use openraft::alias::LogIdOf;
use openraft::storage::RaftStateMachine;
use ursula_runtime::GroupEngineError;
use ursula_runtime::LeadershipReason;
use ursula_runtime::LinearizableReadBarrier;
use ursula_runtime::Proposal;
use ursula_runtime::ReadIndexFuture;

use crate::GroupLeadership;
use crate::forward::unknown_leader;
use crate::openraft_error::group_engine_linearizable_read_error;
use crate::state_machine::RaftGroupStateMachine;
use crate::types::SnapshotData;
use crate::types::UrsulaRaftTypeConfig;

/// `Ok(Some(read_log_id))` once the confirmed leader has applied the
/// round's read log id, `Ok(None)` when this replica does not lead.
type Outcome = Result<Option<LogIdOf<UrsulaRaftTypeConfig>>, GroupEngineError>;
type Round = Shared<BoxFuture<'static, Outcome>>;

const OPERATION: &str = "linearizable read";

/// The barrier of one replica, of either plane: generic over its state
/// machine, the data plane's by default.
pub(crate) struct ReadIndexBarrier<
    SM: RaftStateMachine<UrsulaRaftTypeConfig, SnapshotData = SnapshotData> = RaftGroupStateMachine,
> {
    raft: crate::owner::OwnerRaftHandle<SM>,
    rounds: Arc<Mutex<Rounds>>,
}

#[derive(Default)]
struct Rounds {
    /// The newest round: open, in flight, or finished.
    latest: Option<Round>,
    /// `latest` has not started confirming, so a reader arriving now may
    /// share it.
    open: bool,
}

impl<SM: RaftStateMachine<UrsulaRaftTypeConfig, SnapshotData = SnapshotData>> ReadIndexBarrier<SM> {
    pub(crate) fn owner(&self) -> &crate::owner::OwnerRaftHandle<SM> {
        &self.raft
    }

    pub(crate) fn new(
        raft: Raft<UrsulaRaftTypeConfig, SM>,
        fence: ursula_runtime::NodeFence,
    ) -> Self {
        Self {
            raft: crate::owner::OwnerRaftHandle::new(raft, fence),
            rounds: Arc::default(),
        }
    }

    /// The read log id of a round that starts after this call: the entry
    /// the confirmed leader has applied before the round answers. Its leader
    /// id names the leadership the round confirmed, because a leader reads
    /// at its own first entry or at a later committed one.
    ///
    /// Joins the open round, or opens one that starts once the round in
    /// flight has finished.
    pub(crate) fn confirm_read_log_id(&self) -> Round {
        let mut rounds = self.rounds.lock().unwrap_or_else(PoisonError::into_inner);
        if rounds.open
            && let Some(round) = rounds.latest.as_ref()
        {
            return round.clone();
        }
        let previous = rounds.latest.take();
        let raft = self.raft.clone();
        // Weak: `rounds` holds this round, which must not keep it alive.
        let state = Arc::downgrade(&self.rounds);
        let round = async move {
            if let Some(previous) = previous {
                // Its outcome belongs to its own readers. Should it ever
                // panic, this round must still close, or every later reader
                // would join the poisoned round.
                let _previous = std::panic::AssertUnwindSafe(previous).catch_unwind().await;
            }
            // Close the round: readers arriving from now on open the next.
            if let Some(state) = state.upgrade() {
                state.lock().unwrap_or_else(PoisonError::into_inner).open = false;
            }
            let fence = raft.fence().clone();
            raft.call(move |raft| async move { confirm(&raft, &fence).await })
                .await
                .map_err(crate::owner::owner_stopped)?
        }
        .boxed()
        .shared();
        rounds.latest = Some(round.clone());
        rounds.open = true;
        round
    }
}

impl<SM: RaftStateMachine<UrsulaRaftTypeConfig, SnapshotData = SnapshotData>> std::fmt::Debug
    for ReadIndexBarrier<SM>
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReadIndexBarrier").finish_non_exhaustive()
    }
}

impl<SM: RaftStateMachine<UrsulaRaftTypeConfig, SnapshotData = SnapshotData>>
    LinearizableReadBarrier for ReadIndexBarrier<SM>
{
    fn confirm(&self) -> ReadIndexFuture {
        Box::pin(
            self.confirm_read_log_id()
                .map(|outcome| outcome.map(|read_log_id| read_log_id.map(|log_id| log_id.index()))),
        )
    }
}

/// One confirmation round: confirm leadership with a quorum (ReadIndex),
/// then wait until the local state machine has applied the read index.
///
/// The round takes no earlier acknowledgement (a zero age, so never a lease
/// read): OpenRaft queues it until a heartbeat round that began after it
/// reaches a quorum. It waits at most `election_timeout_min` for that
/// quorum, then fails with `QuorumNotEnough` (a 503), or with
/// `ForwardToLeader` once this node steps down. The wait for the local apply
/// is bounded by the same timeout and also ends in a 503, so a read never
/// hangs on a stalled state machine.
async fn confirm<SM: RaftStateMachine<UrsulaRaftTypeConfig>>(
    raft: &Raft<UrsulaRaftTypeConfig, SM>,
    fence: &ursula_runtime::NodeFence,
) -> Outcome {
    if !GroupLeadership::of_raft(raft, fence).leads() {
        return Ok(None);
    }
    let budget = Duration::from_millis(raft.config().election_timeout_min);
    let fresh_quorum = LinearizerOption::from(ReadPolicy::ReadIndex).with_wait_timeout(budget);
    let linearizer = raft
        .get_read_linearizer(fresh_quorum)
        .await
        .map_err(|err| {
            group_engine_linearizable_read_error(
                err,
                OPERATION,
                &GroupLeadership::of_raft(raft, fence),
            )
        })?;
    let read_log_id = *linearizer.read_log_id().log_id();
    match linearizer.try_await_ready(raft, Some(budget)).await {
        Ok(Ok(_)) => Ok(Some(read_log_id)),
        Ok(Err(state)) => {
            tracing::debug!(
                "OpenRaft {OPERATION} timed out waiting to apply the read index: {state:?}"
            );
            Err(unknown_leader(
                LeadershipReason::ReadIndexBehind,
                Proposal::NotProposed,
            ))
        }
        Err(fatal) => Err(crate::owner::owner_stopped(fatal)),
    }
}

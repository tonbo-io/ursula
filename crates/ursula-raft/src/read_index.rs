//! Coalesced ReadIndex confirmation for one Raft group (D10).
//!
//! openraft 0.10.0-alpha.21 sends a heartbeat to every voter for each
//! `get_read_linearizer` call; it does not batch concurrent callers. This
//! barrier runs at most one confirmation round per group at a time. Readers
//! that arrive while a round is in flight share the next round, which starts
//! when the current one ends. A reader never shares a round that started
//! before it arrived, so every round's read index is taken after each of its
//! readers began.
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
use openraft::ReadPolicy;
use openraft::error::LinearizableReadError;
use openraft::rt::WatchReceiver;
use openraft::type_config::TypeConfigExt;
use ursula_runtime::GroupEngineError;
use ursula_runtime::LinearizableReadBarrier;
use ursula_runtime::ReadIndexFuture;

use crate::forward::group_engine_leader_read_unavailable;
use crate::forward::group_engine_linearizable_read_error;
use crate::registry::RaftGroupHandle;
use crate::types::UrsulaRaftTypeConfig;

/// `Ok(Some(read_index))` once the confirmed leader has applied the read
/// index, `Ok(None)` when this replica does not lead.
type Outcome = Result<Option<u64>, GroupEngineError>;
type Round = Shared<BoxFuture<'static, Outcome>>;

const OPERATION: &str = "linearizable read";

pub(crate) struct ReadIndexBarrier {
    raft: RaftGroupHandle,
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

impl ReadIndexBarrier {
    pub(crate) fn new(raft: RaftGroupHandle) -> Self {
        Self {
            raft,
            rounds: Arc::default(),
        }
    }

    /// Joins the open round, or opens one that starts once the round in
    /// flight has finished.
    pub(crate) fn round(&self) -> Round {
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
                // Its outcome belongs to its own readers.
                let _previous = previous.await;
            }
            // Close the round: readers arriving from now on open the next.
            if let Some(state) = state.upgrade() {
                state.lock().unwrap_or_else(PoisonError::into_inner).open = false;
            }
            confirm(&raft).await
        }
        .boxed()
        .shared();
        rounds.latest = Some(round.clone());
        rounds.open = true;
        round
    }
}

impl std::fmt::Debug for ReadIndexBarrier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReadIndexBarrier").finish_non_exhaustive()
    }
}

impl LinearizableReadBarrier for ReadIndexBarrier {
    fn confirm(&self) -> ReadIndexFuture {
        Box::pin(self.round())
    }
}

/// One confirmation round: confirm leadership with a quorum (ReadIndex),
/// then wait until the local state machine has applied the read index.
///
/// One heartbeat round waits one heartbeat interval for the quorum, which a
/// loaded leader can miss. While this node still believes it leads,
/// `QuorumNotEnough` is retried (at most one round per heartbeat interval)
/// until `election_timeout_min` has passed; only then is it a 503. The wait
/// for the local apply is bounded by the same timeout and also ends in a
/// 503, so a read never hangs on a stalled state machine.
async fn confirm(raft: &RaftGroupHandle) -> Outcome {
    if !raft.is_leader() {
        return Ok(None);
    }
    let config = raft.config();
    let budget = Duration::from_millis(config.election_timeout_min);
    let round = Duration::from_millis(config.heartbeat_interval);
    let deadline = UrsulaRaftTypeConfig::now() + budget;
    let self_id = || raft.metrics().borrow_watched().id;
    let linearizer = loop {
        let started = UrsulaRaftTypeConfig::now();
        match raft.get_read_linearizer(ReadPolicy::ReadIndex).await {
            Ok(linearizer) => break linearizer,
            Err(err)
                if matches!(
                    err.api_error(),
                    Some(LinearizableReadError::QuorumNotEnough(_))
                ) && raft.is_leader()
                    && UrsulaRaftTypeConfig::now() < deadline =>
            {
                tracing::debug!("OpenRaft {OPERATION} retrying leadership confirmation: {err}");
                let next_round = (started + round).min(deadline);
                UrsulaRaftTypeConfig::sleep_until(next_round).await;
            }
            Err(err) => {
                return Err(group_engine_linearizable_read_error(
                    err,
                    OPERATION,
                    self_id(),
                ));
            }
        }
    };
    let read_index = linearizer.read_log_id().index();
    match linearizer.try_await_ready(raft, Some(budget)).await {
        Ok(Ok(_)) => Ok(Some(read_index)),
        Ok(Err(state)) => {
            tracing::debug!(
                "OpenRaft {OPERATION} timed out waiting to apply the read index: {state:?}"
            );
            Err(group_engine_leader_read_unavailable(
                format!("OpenRaft {OPERATION} did not apply the read index in time"),
                self_id(),
            ))
        }
        Err(fatal) => Err(GroupEngineError::new(format!(
            "OpenRaft {OPERATION} could not apply the read index: {fatal}"
        ))),
    }
}

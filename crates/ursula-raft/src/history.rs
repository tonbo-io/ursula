//! History identity on a Raft group (#581 sections 4.3 and 4.5): the
//! leader's history fence and its obligation to propose the group's history
//! event.
//!
//! - **Pending history.** A group's history is pending on a replica while
//!   it applied no history event yet (a new group before its genesis), or
//!   while it holds a recovery plan ([`RecoveryRecord`]) that no applied
//!   history event above the plan's point satisfies yet. The test reads
//!   replicated state, so it holds after an apply, a snapshot install and a
//!   startup replay alike, on every replica at the same index.
//! - **The fence.** A replica that leads its group while the history is
//!   pending admits no proposal but the history event, serves no engine
//!   read and confirms no rejoin barrier ([`GroupHistory::admit`]). A
//!   fenced proposal or read waits a moment for the event before it is
//!   refused with [`GroupInfraError::RecoveryPending`] (`503`). Followers
//!   are never fenced: before the event every replica applies only states
//!   the old history committed, which they may serve under the old token.
//! - **The obligation** ([`run_history_driver`]). Once a leader applied an
//!   entry of its own term (its blank entry, so every earlier committed
//!   entry applied too), and the history is still pending, it proposes
//!   `NewHistory` with a fresh random id, once per term. An event that an
//!   earlier attempt already committed applies before the leader looks, so
//!   it proposes no second one. The driver also retires a satisfied
//!   recovery record on every replica, outside OpenRaft's state-machine
//!   worker.

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::PoisonError;
use std::sync::Weak;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::Duration;

use openraft::RaftMetrics;
use openraft::ServerState;
use openraft::rt::WatchReceiver;
use ursula_runtime::GroupEngineError;
use ursula_runtime::GroupInfraError;
use ursula_runtime::GroupWriteCommand;
use ursula_runtime::HistoryEvent;
use ursula_runtime::HistoryWatch;
use ursula_shard::RaftGroupId;
use ursula_stream::StreamCommand;

use crate::log_store::RecoveryRecord;
use crate::registry::RaftGroupHandle;
use crate::rejoin::GroupRejoin;
use crate::rt::sync::Notify;
use crate::types::UrsulaRaftTypeConfig;

/// How long a fenced proposal or read waits for the group's history event
/// before it is refused. A new group's genesis takes one round trip after
/// its first election, so writes that race it wait instead of failing.
pub(crate) const HISTORY_PENDING_WAIT: Duration = Duration::from_secs(2);

/// Whether a recovery plan with point `point` is satisfied by `applied`: a
/// history event at a log index above the point applied.
pub(crate) fn plan_satisfied(plan: &RecoveryRecord, applied: Option<HistoryEvent>) -> bool {
    applied.is_some_and(|event| event.event_index > plan.point)
}

/// Whether a replica that applied `applied` and holds `plan` has a pending
/// history.
pub(crate) fn history_pending(
    applied: Option<HistoryEvent>,
    plan: Option<&RecoveryRecord>,
) -> bool {
    applied.is_none() || plan.is_some_and(|plan| !plan_satisfied(plan, applied))
}

/// One replica's view of its group's history: what it applied, and the
/// recovery plan it holds. Waiters wake through `Notify`s, in the order they
/// began to wait, so a simulation replays them (see
/// [`ursula_runtime::HistoryPublisher`]).
#[derive(Debug)]
pub struct GroupHistory {
    raft_group_id: RaftGroupId,
    applied: HistoryWatch,
    plan: Mutex<Option<RecoveryRecord>>,
    /// Bumped by every change of the plan, under its lock.
    plan_version: AtomicU64,
    plan_changed: Notify,
    /// The recovery gate that keeps the plan durable, once bound.
    rejoin: Mutex<Option<Weak<GroupRejoin>>>,
}

impl GroupHistory {
    pub(crate) fn new(raft_group_id: RaftGroupId, applied: HistoryWatch) -> Arc<Self> {
        Arc::new(Self {
            raft_group_id,
            applied,
            plan: Mutex::new(None),
            plan_version: AtomicU64::new(0),
            plan_changed: Notify::new(),
            rejoin: Mutex::new(None),
        })
    }

    /// The history event this replica applied last.
    pub fn applied(&self) -> Option<HistoryEvent> {
        self.applied.current()
    }

    /// A receiver of the applied history, for the group actor's wake.
    pub(crate) fn subscribe(&self) -> HistoryWatch {
        self.applied.clone()
    }

    /// The recovery plan this replica holds.
    pub fn plan(&self) -> Option<RecoveryRecord> {
        *self.plan.lock().unwrap_or_else(PoisonError::into_inner)
    }

    pub(crate) fn set_plan(&self, plan: Option<RecoveryRecord>) {
        let mut current = self.plan.lock().unwrap_or_else(PoisonError::into_inner);
        if *current == plan {
            return;
        }
        *current = plan;
        self.plan_version.fetch_add(1, Ordering::SeqCst);
        drop(current);
        self.plan_changed.notify_waiters();
    }

    /// Waits until the plan changes after `seen`, its version as the caller
    /// last read it, and returns the new version.
    async fn plan_changed(&self, seen: u64) -> u64 {
        loop {
            let notified = self.plan_changed.notified();
            let mut notified = std::pin::pin!(notified);
            notified.as_mut().enable();
            let version = self.plan_version.load(Ordering::SeqCst);
            if version != seen {
                return version;
            }
            notified.await;
        }
    }

    /// Binds the recovery gate that keeps the plan durable, and takes the
    /// plan it recovered from its log store.
    pub(crate) fn bind_rejoin(&self, rejoin: &Arc<GroupRejoin>) {
        *self.rejoin.lock().unwrap_or_else(PoisonError::into_inner) = Some(Arc::downgrade(rejoin));
        self.set_plan(rejoin.recovery_record());
    }

    fn rejoin(&self) -> Option<Arc<GroupRejoin>> {
        self.rejoin
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .as_ref()
            .and_then(Weak::upgrade)
    }

    /// Whether the history is pending on this replica.
    pub fn pending(&self) -> bool {
        history_pending(self.applied(), self.plan().as_ref())
    }

    /// Whether this replica leads the group while its history is pending.
    pub(crate) fn fences(&self, metrics: &RaftMetrics<UrsulaRaftTypeConfig>) -> bool {
        metrics.state == ServerState::Leader && self.pending()
    }

    /// The fence of a leader whose history is pending: waits up to
    /// [`HISTORY_PENDING_WAIT`] for the history event or a lost leadership,
    /// then refuses. Never fences a follower.
    pub(crate) async fn admit(&self, raft: &RaftGroupHandle) -> Result<(), GroupEngineError> {
        self.admit_on(raft.metrics()).await
    }

    /// [`Self::admit`] for the replica whose Raft metrics `metrics` watches.
    pub(crate) async fn admit_on(&self, mut metrics: MetricsWatch) -> Result<(), GroupEngineError> {
        if !self.fences(&metrics.borrow_watched()) {
            return Ok(());
        }
        let mut applied = self.applied.clone();
        let mut plan = self.plan_version.load(Ordering::SeqCst);
        let wait = async {
            while self.fences(&metrics.borrow_watched())
                && history_changed(self, &mut applied, &mut plan, &mut metrics).await
            {}
        };
        if crate::rt::time::timeout(HISTORY_PENDING_WAIT, wait)
            .await
            .is_err()
            || self.fences(&metrics.borrow_watched())
        {
            return Err(GroupEngineError::Infra(GroupInfraError::RecoveryPending {
                raft_group_id: self.raft_group_id,
            }));
        }
        Ok(())
    }

    /// Admits `command` on `raft`: the history event always, anything else
    /// through [`Self::admit`].
    pub(crate) async fn admit_proposal(
        &self,
        raft: &RaftGroupHandle,
        command: &GroupWriteCommand,
    ) -> Result<(), GroupEngineError> {
        if matches!(
            command,
            GroupWriteCommand::Stream(StreamCommand::NewHistory { .. })
        ) {
            return Ok(());
        }
        self.admit(raft).await
    }
}

/// Whether `metrics` show a leader that applied an entry of its own term:
/// its blank entry, and with it every entry an earlier term committed.
fn leader_applied_own_term(metrics: &RaftMetrics<UrsulaRaftTypeConfig>) -> Option<u64> {
    if metrics.state != ServerState::Leader || !metrics.vote.is_committed() {
        return None;
    }
    let term = metrics.current_term;
    metrics
        .last_applied
        .filter(|applied| applied.leader_id.term == term)
        .map(|_| term)
}

/// The history driver of one replica (see the module docs): proposes the
/// history event a pending leader owes, once per term, and retires a
/// satisfied recovery record. Runs until the group stops.
pub(crate) async fn run_history_driver(raft: RaftGroupHandle, history: Arc<GroupHistory>) {
    let mut metrics = raft.metrics();
    let mut applied = history.applied.clone();
    let mut plan = history.plan_version.load(Ordering::SeqCst);
    let mut proposed_in_term = None;
    loop {
        if let Some(record) = history.plan()
            && plan_satisfied(&record, history.applied())
        {
            history.set_plan(None);
            if let Some(rejoin) = history.rejoin()
                && let Err(error) = rejoin.retire_recovery_plan(record).await
            {
                tracing::warn!(
                    raft_group_id = history.raft_group_id.0,
                    %error,
                    "could not remove a satisfied recovery plan; the next start removes it"
                );
            }
        }
        let owed = leader_applied_own_term(&metrics.borrow_watched())
            .filter(|term| history.pending() && proposed_in_term != Some(*term));
        if let Some(term) = owed {
            proposed_in_term = Some(term);
            let command = GroupWriteCommand::Stream(StreamCommand::NewHistory {
                history: ursula_runtime::draw_history_id(),
                now_ms: ursula_runtime::unix_time_ms(),
            });
            match raft.client_write(command).await {
                Ok(_) => tracing::info!(
                    raft_group_id = history.raft_group_id.0,
                    term,
                    "a new history started"
                ),
                Err(error) => tracing::warn!(
                    raft_group_id = history.raft_group_id.0,
                    term,
                    %error,
                    "the history event this leader proposed did not answer; a later leader \
                     proposes another if it did not commit"
                ),
            }
            continue;
        }
        if !history_changed(&history, &mut applied, &mut plan, &mut metrics).await {
            return;
        }
    }
}

type MetricsWatch = openraft::type_config::alias::WatchReceiverOf<
    UrsulaRaftTypeConfig,
    RaftMetrics<UrsulaRaftTypeConfig>,
>;

/// Waits for a change of the applied history, the plan (whose version the
/// caller last saw is `plan`) or the Raft metrics. `false` once the metrics
/// stopped: the group stopped.
async fn history_changed(
    history: &GroupHistory,
    applied: &mut HistoryWatch,
    plan: &mut u64,
    metrics: &mut MetricsWatch,
) -> bool {
    use futures_util::future::Either;
    let changed = futures_util::future::select(
        futures_util::future::select(
            Box::pin(applied.changed()),
            Box::pin(history.plan_changed(*plan)),
        ),
        Box::pin(metrics.changed()),
    )
    .await;
    match changed {
        Either::Left((Either::Left(((), _)), _)) => true,
        Either::Left((Either::Right((version, _)), _)) => {
            *plan = version;
            true
        }
        Either::Right((result, _)) => result.is_ok(),
    }
}

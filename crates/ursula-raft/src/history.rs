//! History identity on a Raft group (#581 sections 4.3 and 4.5): the
//! leader's history fence and its obligation to propose the group's history
//! event.
//!
//! - **Pending history.** A group's history is pending on a replica while
//!   it applied no history event yet (a new group before its genesis), or
//!   while it holds a recovery plan ([`RecoveryRecord`]). A plan is
//!   satisfied once an applied history event above its point reads so in
//!   replicated state, after an apply, a snapshot install and a startup
//!   replay alike; the history stays pending until the satisfied record is
//!   removed from disk, so a restart never finds a record whose fence it
//!   already lifted.
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
//!   `NewHistory` with a fresh random id, once per term and debt (the
//!   genesis, or one plan), and again after a proposal that did not answer.
//!   It looks only once it applied its whole log, so an event an earlier
//!   attempt committed applies first and it proposes no second one. Only the
//!   driver proposes `NewHistory`: every other write path refuses it. The
//!   driver also retires a satisfied recovery record on every replica,
//!   outside OpenRaft's state-machine worker, before the fence lifts.

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
/// history: no genesis applied, or it still holds a recovery plan. A
/// satisfied plan keeps the history pending until its record is removed
/// from disk (M1, #581 review), so a leader that restarts with the record
/// never served without it.
pub(crate) fn history_pending(
    applied: Option<HistoryEvent>,
    plan: Option<&RecoveryRecord>,
) -> bool {
    applied.is_none() || plan.is_some()
}

/// The history event a leader owes its group.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Owed {
    /// The group's genesis.
    Genesis,
    /// The event of the recovery plan at `point`.
    Plan { point: u64 },
}

/// What a replica that applied `applied` and holds `plan` owes, if it leads.
fn owed(applied: Option<HistoryEvent>, plan: Option<&RecoveryRecord>) -> Option<Owed> {
    match plan {
        Some(plan) if !plan_satisfied(plan, applied) => Some(Owed::Plan { point: plan.point }),
        Some(_) => None,
        None => applied.is_none().then_some(Owed::Genesis),
    }
}

/// How long the driver waits before it proposes a history event again after
/// a proposal that did not answer.
const PROPOSAL_RETRY: Duration = Duration::from_millis(200);

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

    /// Clears the plan when it is still `record` (m1, #581 review): a plan
    /// taken meanwhile stays.
    pub(crate) fn clear_plan(&self, record: RecoveryRecord) {
        let mut current = self.plan.lock().unwrap_or_else(PoisonError::into_inner);
        if *current != Some(record) {
            return;
        }
        *current = None;
        self.plan_version.fetch_add(1, Ordering::SeqCst);
        drop(current);
        self.plan_changed.notify_waiters();
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

    /// Admits `command` on `raft`: refused when the proposal path may not
    /// carry it (a history event included, which only the driver proposes),
    /// then through [`Self::admit`].
    pub(crate) async fn admit_proposal(
        &self,
        raft: &RaftGroupHandle,
        command: &GroupWriteCommand,
    ) -> Result<(), GroupEngineError> {
        crate::forward::validate_proposal(command)?;
        self.admit(raft).await
    }
}

/// Whether `metrics` show a leader that applied an entry of its own term
/// (its blank entry, and with it every entry an earlier term committed) and
/// every entry of its log: a history event an earlier proposal committed has
/// then applied, so the leader never proposes a second one.
fn leader_applied_own_term(metrics: &RaftMetrics<UrsulaRaftTypeConfig>) -> Option<u64> {
    if metrics.state != ServerState::Leader || !metrics.vote.is_committed() {
        return None;
    }
    let term = metrics.current_term;
    metrics
        .last_applied
        .filter(|applied| {
            applied.leader_id.term == term && Some(applied.index) >= metrics.last_log_index
        })
        .map(|_| term)
}

/// The history driver of one replica (see the module docs): proposes the
/// history event a pending leader owes, once per term and debt, again after
/// a proposal that did not answer, and retires a satisfied recovery record,
/// removing it from disk before the fence lifts. Runs until the group stops.
pub(crate) async fn run_history_driver(raft: RaftGroupHandle, history: Arc<GroupHistory>) {
    let mut metrics = raft.metrics();
    let mut applied = history.applied.clone();
    let mut plan = history.plan_version.load(Ordering::SeqCst);
    let mut proposed: Option<(u64, Owed)> = None;
    // m6 (#581 review): the applied index when it last moved or the plan
    // last changed, the plan's version then, and when.
    let mut progress = (
        metrics
            .borrow_watched()
            .last_applied
            .map(|applied| applied.index),
        plan,
        crate::rt::time::Instant::now(),
    );
    loop {
        if let Some(record) = history.plan()
            && plan_satisfied(&record, history.applied())
        {
            let retired = match history.rejoin() {
                Some(rejoin) => rejoin.retire_recovery_plan(record).await.map(|_removed| ()),
                None => Ok(()),
            };
            match retired {
                Ok(()) => history.clear_plan(record),
                Err(error) => {
                    tracing::warn!(
                        raft_group_id = history.raft_group_id.0,
                        %error,
                        "could not remove a satisfied recovery plan; retrying"
                    );
                    crate::rt::time::sleep(PROPOSAL_RETRY).await;
                    continue;
                }
            }
        }
        let owed = leader_applied_own_term(&metrics.borrow_watched())
            .zip(owed(history.applied(), history.plan().as_ref()))
            .filter(|debt| proposed != Some(*debt));
        if let Some((term, debt)) = owed {
            proposed = Some((term, debt));
            let command = GroupWriteCommand::Stream(StreamCommand::NewHistory {
                history: ursula_runtime::draw_history_id(),
                now_ms: ursula_runtime::unix_time_ms(),
            });
            match raft.client_write(command).await {
                Ok(_) => tracing::info!(
                    raft_group_id = history.raft_group_id.0,
                    term,
                    ?debt,
                    "a new history started"
                ),
                Err(error) => {
                    tracing::warn!(
                        raft_group_id = history.raft_group_id.0,
                        term,
                        ?debt,
                        %error,
                        "the history event this leader proposed did not answer; it proposes \
                         again unless the event applied"
                    );
                    // The next look waits until the leader applied its whole
                    // log, so an event this proposal committed applies first
                    // and no second history starts.
                    proposed = None;
                    crate::rt::time::sleep(PROPOSAL_RETRY).await;
                }
            }
            continue;
        }
        // m6 (#581 review): a plan under which nothing applies for the
        // stall timeout is wedged; its gate reports the group stalled, and
        // the operator plans again.
        let rejoin = history.rejoin().filter(|_| {
            history
                .plan()
                .is_some_and(|record| !plan_satisfied(&record, history.applied()))
        });
        let applied_now = metrics
            .borrow_watched()
            .last_applied
            .map(|applied| applied.index);
        // A plan's stall timeout runs from when the replica took it, or
        // last applied something under it. Only applied progress clears a
        // stall (m9, #581 review).
        if applied_now != progress.0 || plan != progress.1 {
            if applied_now != progress.0
                && let Some(rejoin) = &rejoin
            {
                rejoin.unstall_plan();
            }
            progress = (applied_now, plan, crate::rt::time::Instant::now());
        }
        let wait = rejoin.as_ref().map(|rejoin| {
            let stall_after = rejoin.stall_after();
            if progress.2.elapsed() >= stall_after {
                rejoin.stall_plan();
                stall_after
            } else {
                stall_after.saturating_sub(progress.2.elapsed())
            }
        });
        drop(rejoin);
        let changed = history_changed(&history, &mut applied, &mut plan, &mut metrics);
        let changed = match wait {
            // The stall timeout elapsed: check the plan again.
            Some(wait) => crate::rt::time::timeout(wait, changed)
                .await
                .unwrap_or(true),
            None => changed.await,
        };
        if !changed {
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

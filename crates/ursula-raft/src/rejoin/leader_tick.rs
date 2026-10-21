//! What every leader-side membership driver shares: its wake-ups, its rate
//! limit and the follower log reversion observer.
//!
//! The heal driver and the membership reconcile shell both run on a
//! [`LeaderTick`], so a follower that lost entries is counted once, by one
//! observer, whichever driver a replica runs.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::PoisonError;
use std::time::Duration;

use openraft::RaftMetrics;
use openraft::ServerState;
use openraft::alias::LogIdOf;
use openraft::metrics::RaftServerMetrics;
use openraft::rt::WatchReceiver;
use openraft::type_config::alias::WatchReceiverOf;

use super::attach::Wake;
use super::attach::wait_change;
use super::log_index;
use crate::target::RaftTarget;
use crate::types::UrsulaRaftTypeConfig as C;
use crate::types::UrsulaVote;

/// A follower whose matched index went down within one leadership: OpenRaft
/// rewound it because it answered a conflict below what it had matched, so
/// it lost entries it had acknowledged.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct LogReversion {
    pub follower: u64,
    pub matched: u64,
    pub now: Option<u64>,
}

/// What this leader saw each follower match in its current leadership: the
/// passive observer of follower log reversions. It reads the replication
/// metrics only, and changes nothing OpenRaft does.
#[derive(Debug, Default)]
pub(crate) struct ReversionWatch {
    leader: Option<UrsulaVote>,
    matched: BTreeMap<u64, u64>,
}

impl ReversionWatch {
    /// The followers whose matched index went down since the previous
    /// observation under the leadership `vote`, read from OpenRaft's
    /// replication metrics. A new leadership starts afresh, since a new
    /// leader learns what each follower holds. Allocates only to report a
    /// reversion or to track a follower it has not seen match yet.
    pub(crate) fn observe(
        &mut self,
        vote: &UrsulaVote,
        replication: &BTreeMap<u64, Option<LogIdOf<C>>>,
    ) -> Vec<LogReversion> {
        if self.leader.as_ref() != Some(vote) {
            self.leader = Some(*vote);
            self.matched.clear();
        }
        // A follower leaving the membership leaves the replication metrics:
        // that is no reversion.
        self.matched
            .retain(|follower, _| replication.contains_key(follower));
        let mut reversions = Vec::new();
        for (follower, now) in replication {
            let now = log_index(now.as_ref());
            if let Some(previous) = self.matched.get(follower).copied()
                && now.is_none_or(|now| now < previous)
            {
                reversions.push(LogReversion {
                    follower: *follower,
                    matched: previous,
                    now,
                });
            }
            // Replacing the value of a follower already tracked allocates
            // nothing.
            match now {
                Some(now) => {
                    self.matched.insert(*follower, now);
                }
                None => {
                    self.matched.remove(follower);
                }
            }
        }
        reversions
    }

    /// This replica stopped leading: the next leadership starts afresh.
    pub(crate) fn forget(&mut self) {
        self.leader = None;
        self.matched.clear();
    }
}

/// How many times each follower lost entries it had acknowledged to a
/// leader on this node, by follower node id. One per node, in its registry,
/// so it only grows: a group's restart or a stopped group keeps its counts.
#[derive(Debug, Clone, Default)]
pub(crate) struct FollowerLogReversions(Arc<Mutex<BTreeMap<u64, u64>>>);

impl FollowerLogReversions {
    fn record(&self, follower: u64) {
        let mut counts = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        let count = counts.entry(follower).or_insert(0);
        *count = count.saturating_add(1);
    }

    pub(crate) fn snapshot(&self) -> BTreeMap<u64, u64> {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

/// Each follower's matched log index, from a leader's replication metrics.
pub(super) fn matched_indexes(metrics: &RaftMetrics<C>) -> BTreeMap<u64, Option<u64>> {
    metrics
        .replication
        .as_ref()
        .map(|replication| {
            replication
                .iter()
                .map(|(node_id, matched)| (*node_id, log_index(matched.as_ref())))
                .collect::<BTreeMap<_, _>>()
        })
        .unwrap_or_default()
}

/// What a plan depends on besides replication progress: a change of it
/// plans at once, anything else waits for the interval.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PlanInputs {
    state: ServerState,
    membership: Option<LogIdOf<C>>,
}

/// The wake-ups and the rate limit of a leader-side membership driver, and
/// its reversion observer.
///
/// A follower wakes on server-state changes only. A leader wakes on every
/// metrics change, replication progress included, so the observer sees a
/// rewound follower's matched index drop before it matches again. A wake-up
/// is due for a plan when the gate changed, the interval passed, or the
/// server state or the membership changed since the last plan.
pub(super) struct LeaderTick {
    replica: RaftTarget,
    node_id: u64,
    server: WatchReceiverOf<C, RaftServerMetrics<C>>,
    metrics: WatchReceiverOf<C, RaftMetrics<C>>,
    gate: WatchReceiverOf<C, ()>,
    interval: Duration,
    leading: bool,
    planned: Option<(PlanInputs, crate::rt::time::Instant)>,
    watch: ReversionWatch,
    reversions: FollowerLogReversions,
}

impl LeaderTick {
    pub(super) fn new(
        replica: RaftTarget,
        node_id: u64,
        server: WatchReceiverOf<C, RaftServerMetrics<C>>,
        metrics: WatchReceiverOf<C, RaftMetrics<C>>,
        gate: WatchReceiverOf<C, ()>,
        interval: Duration,
        reversions: FollowerLogReversions,
    ) -> Self {
        Self {
            replica,
            node_id,
            server,
            metrics,
            gate,
            interval,
            leading: false,
            planned: None,
            watch: ReversionWatch::default(),
            reversions,
        }
    }

    /// Waits for the next wake-up, counts and logs the follower log
    /// reversions it shows, and runs `plan` on the metrics when a plan is
    /// due: `Some(None)` when it is not, `None` once the Raft stopped.
    pub(super) async fn next<R>(
        &mut self,
        plan: impl FnOnce(&RaftMetrics<C>) -> R,
    ) -> Option<Option<R>> {
        let wake = if self.leading {
            wait_change(&mut self.metrics, &mut self.gate, self.interval).await
        } else {
            wait_change(&mut self.server, &mut self.gate, self.interval).await
        }?;
        let (planned, reverted) = {
            let current = self.metrics.borrow_watched();
            if current.running_state.is_err() {
                return None;
            }
            self.leading = current.state == ServerState::Leader;
            let reverted = match current.replication.as_ref() {
                Some(replication) if self.leading => self.watch.observe(&current.vote, replication),
                _ => {
                    self.watch.forget();
                    Vec::new()
                }
            };
            let inputs = PlanInputs {
                state: current.state,
                membership: *current.membership_config.log_id(),
            };
            let due = wake != Wake::Metrics
                || self.planned.is_none_or(|(previous, at)| {
                    previous != inputs || at.elapsed() >= self.interval
                });
            let planned = due.then(|| {
                self.planned = Some((inputs, crate::rt::time::Instant::now()));
                plan(&current)
            });
            (planned, reverted)
        };
        for reversion in reverted {
            self.reversions.record(reversion.follower);
            tracing::warn!(
                node_id = self.node_id,
                raft_group_id = self.replica.wire_group_id(),
                plane = self.replica.plane().as_str_name(),
                follower = reversion.follower,
                matched_index = reversion.matched,
                now_matched_index = ?reversion.now,
                "recovery: follower lost Raft log entries it had acknowledged; replicating \
                 this leader's log to it again"
            );
        }
        Some(planned)
    }
}

/// The unchanged-step retry rule: the same step again waits for the
/// interval, a different step goes at once. Membership proposals publish
/// metrics themselves, so a step that changed something is not retried.
#[derive(Debug)]
pub(super) struct StepRetry<S> {
    last: Option<(S, crate::rt::time::Instant)>,
}

impl<S> Default for StepRetry<S> {
    fn default() -> Self {
        Self { last: None }
    }
}

impl<S: Clone + PartialEq> StepRetry<S> {
    /// Whether `step` may run now, recording the attempt when it may.
    pub(super) fn admit(&mut self, step: &S, interval: Duration) -> bool {
        if self
            .last
            .as_ref()
            .is_some_and(|(previous, at)| previous == step && at.elapsed() < interval)
        {
            return false;
        }
        self.last = Some((step.clone(), crate::rt::time::Instant::now()));
        true
    }
}

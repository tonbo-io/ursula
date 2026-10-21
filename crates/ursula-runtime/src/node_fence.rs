//! The node fence: whether this process may take part in Raft and write
//! to the object stores.
//!
//! A node whose Raft state lives on a WAL root holds a disk lease
//! ([`DiskLease`]). A probe rewrites and syncs a file on the root every
//! second, and the lease holds while no probe failed since the last
//! success and the monotonic clock is below that success's start plus the
//! lease's length (`raft.wal.lease`). Measured from the start, a late
//! success never extends the lease, and a hung probe does not hold it:
//! checks read the clock, never the probe. A probe that started after the
//! lease ended and succeeds lifts the fence ([`LeaseChange::Lifted`]), and
//! a leader then leads again only once a quorum acknowledged it after the
//! lift ([`NodeFence::lifted_at`]).
//!
//! [`NodeFence`] publishes the lease in atomics, so [`NodeFence::admits`]
//! is two loads and a clock read. Every Raft input and output checks it,
//! on every transport, and so do the leader predicate and every PUT and
//! DELETE to the cold and snapshot stores. A shutdown closes it for good
//! ([`NodeFence::close`]), and so does a supersession
//! ([`NodeFence::supersede`]): the meta group binds this node id to another
//! root, or released this one, so another process may answer as this node.
//!
//! The fence also carries the predecessor wait ([`NodeFence::owes_wait`]):
//! while a process of an earlier run of the root may still run, this one
//! grants no vote, takes no transfer, opens no gate and writes no snapshot
//! object. A root that was empty at its open owes its wait from the binding
//! the meta group answers, and until that binding its data replicas take
//! part in nothing ([`NodeFence::awaits_binding`]): no vote, no gate, no
//! floor and no snapshot object. Its meta replica is not held by it, so a
//! forming cluster's meta group still elects.
//!
//! A process without a WAL root, or a test, runs [`NodeFence::unleased`]:
//! no lease and no wait, only the shutdown's close.

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::PoisonError;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::Duration;

/// The clock of the lease: the monotonic clock, the simulation's under
/// madsim.
#[cfg(madsim)]
pub type FenceInstant = crate::rt::time::Instant;
/// The clock of the lease: the monotonic clock, the simulation's under
/// madsim.
#[cfg(not(madsim))]
pub type FenceInstant = std::time::Instant;

/// The lease clock now.
pub fn fence_now() -> FenceInstant {
    FenceInstant::now()
}

/// A probe's outcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProbeOutcome {
    /// The probe's write reached the disk.
    Synced,
    /// The probe's write or sync failed.
    Failed,
}

/// What a successful probe did to the lease.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeaseChange {
    /// The first success: the lease begins.
    Began,
    /// A success while the lease held: it now ends at the success's start
    /// plus the length, or later.
    Extended,
    /// A probe that started after the lease ended succeeded: it holds
    /// again.
    Lifted,
    /// A probe that started before the lease ended succeeded after it, or
    /// took longer than the lease: nothing changes.
    TooLate,
}

/// The disk lease, a pure decision: time is passed in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DiskLease {
    length: Duration,
    /// The end of the last success's lease: its start plus `length`.
    /// `None` before the first success.
    until: Option<FenceInstant>,
    /// When a probe last failed, if one failed after the last success.
    failed_at: Option<FenceInstant>,
}

impl DiskLease {
    pub const fn new(length: Duration) -> Self {
        Self {
            length,
            until: None,
            failed_at: None,
        }
    }

    pub fn length(&self) -> Duration {
        self.length
    }

    /// Whether the lease holds at `now`.
    pub fn holds(&self, now: FenceInstant) -> bool {
        self.failed_at.is_none() && self.until.is_some_and(|until| now < until)
    }

    /// When the lease ended, if it held once and does not at `now`: at the
    /// last failure, else at its expiry.
    fn ended(&self, now: FenceInstant) -> Option<FenceInstant> {
        let until = self.until?;
        match self.failed_at {
            Some(failed) => Some(failed),
            None => (now >= until).then_some(until),
        }
    }

    /// A probe that started at `started` synced, seen at `now`.
    pub fn succeeded(&mut self, started: FenceInstant, now: FenceInstant) -> LeaseChange {
        let Some(until) = started.checked_add(self.length) else {
            return LeaseChange::TooLate;
        };
        if until <= now {
            return LeaseChange::TooLate;
        }
        let change = match (self.until, self.ended(now)) {
            (None, _) => LeaseChange::Began,
            (Some(_), Some(ended)) if started >= ended => LeaseChange::Lifted,
            (Some(_), Some(_)) => return LeaseChange::TooLate,
            (Some(_), None) => LeaseChange::Extended,
        };
        self.until = Some(match (change, self.until) {
            (LeaseChange::Extended, Some(previous)) => previous.max(until),
            _ => until,
        });
        self.failed_at = None;
        change
    }

    /// A probe failed at `now`: the lease ends at once.
    pub fn failed(&mut self, now: FenceInstant) {
        self.failed_at = Some(now);
    }

    /// The end of the lease while no probe failed since the last success.
    fn published_until(&self) -> Option<FenceInstant> {
        self.until.filter(|_| self.failed_at.is_none())
    }
}

/// What the fence says now.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FenceStatus {
    /// Raft traffic and object writes pass.
    Admits,
    /// The disk lease does not hold.
    DiskLeaseLost,
    /// The process is shutting down.
    Closed,
    /// Another process may answer as this node: this one takes part in
    /// nothing again, writes nothing more to its root and never records a
    /// clean end.
    Superseded,
}

/// Why the fence refused a PUT or DELETE.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteRefusal {
    DiskLeaseLost,
    Closed,
    Superseded,
    /// A snapshot object, while a predecessor wait is owed or an empty
    /// root awaits its binding.
    PredecessorWait,
}

/// A PUT or DELETE the fence refused before it was sent. The store call
/// fails as a store call does, and its job runs again later.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("this node is fenced ({refusal:?}): the {operation} of '{path}' was not sent")]
pub struct FencedWrite {
    pub refusal: WriteRefusal,
    pub operation: &'static str,
    pub path: String,
}

/// Which writes [`NodeFence::check_write`] checks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FencedObject {
    /// A cold object: the lease decides.
    Cold,
    /// A snapshot object: the lease and the predecessor wait decide.
    Snapshot,
}

#[derive(Debug)]
struct FenceState {
    origin: FenceInstant,
    lease: Option<Mutex<DiskLease>>,
    /// The end of the lease in nanoseconds after `origin`, 0 while no
    /// probe succeeded since the last failure.
    until: AtomicU64,
    /// The last lift in nanoseconds after `origin`, plus one. 0 for none.
    lifted: AtomicU64,
    closed: AtomicBool,
    superseded: AtomicBool,
    owes_wait: AtomicBool,
    /// An empty root that the meta group has not bound yet.
    awaits_binding: AtomicBool,
    /// Wakes the waiters of [`NodeFence::waited`] and
    /// [`NodeFence::bound_and_waited`].
    waited: crate::rt::sync::Notify,
}

/// This process's fence (see the module docs). Cloning shares it.
#[derive(Debug, Clone)]
pub struct NodeFence(Arc<FenceState>);

impl NodeFence {
    fn with_lease(lease: Option<DiskLease>) -> Self {
        Self(Arc::new(FenceState {
            origin: fence_now(),
            lease: lease.map(Mutex::new),
            until: AtomicU64::new(0),
            lifted: AtomicU64::new(0),
            closed: AtomicBool::new(false),
            superseded: AtomicBool::new(false),
            owes_wait: AtomicBool::new(false),
            awaits_binding: AtomicBool::new(false),
            waited: crate::rt::sync::Notify::new(),
        }))
    }

    /// A fence without a lease or a wait, for a process without a WAL root
    /// and for tests: it admits until it closes.
    pub fn unleased() -> Self {
        Self::with_lease(None)
    }

    /// A fence over a disk lease of `length`. It admits nothing until the
    /// first probe succeeds.
    pub fn leased(length: Duration) -> Self {
        Self::with_lease(Some(DiskLease::new(length)))
    }

    /// The lease's length, `None` for an unleased fence.
    pub fn lease_length(&self) -> Option<Duration> {
        self.lease().map(|lease| lease.length())
    }

    fn lease(&self) -> Option<std::sync::MutexGuard<'_, DiskLease>> {
        self.0
            .lease
            .as_ref()
            .map(|lease| lease.lock().unwrap_or_else(PoisonError::into_inner))
    }

    fn nanos(&self, at: FenceInstant) -> u64 {
        u64::try_from(at.saturating_duration_since(self.0.origin).as_nanos()).unwrap_or(u64::MAX)
    }

    /// Records a probe that started at `started` and ended with `outcome`
    /// at `now`. `None` for an unleased fence or a failure.
    pub fn record_probe(
        &self,
        started: FenceInstant,
        outcome: ProbeOutcome,
        now: FenceInstant,
    ) -> Option<LeaseChange> {
        let mut lease = self.lease()?;
        let change = match outcome {
            ProbeOutcome::Synced => Some(lease.succeeded(started, now)),
            ProbeOutcome::Failed => {
                lease.failed(now);
                None
            }
        };
        if change == Some(LeaseChange::Lifted) {
            self.0
                .lifted
                .store(self.nanos(now).saturating_add(1), Ordering::Release);
        }
        let until = lease
            .published_until()
            .map_or(0, |until| self.nanos(until).max(1));
        self.0.until.store(until, Ordering::Release);
        change
    }

    /// Whether Raft traffic and object writes pass at `now`.
    pub fn admits(&self, now: FenceInstant) -> bool {
        self.status(now) == FenceStatus::Admits
    }

    /// Whether Raft traffic and object writes pass now.
    pub fn admits_now(&self) -> bool {
        self.admits(fence_now())
    }

    pub fn status(&self, now: FenceInstant) -> FenceStatus {
        if self.0.superseded.load(Ordering::Acquire) {
            return FenceStatus::Superseded;
        }
        if self.0.closed.load(Ordering::Acquire) {
            return FenceStatus::Closed;
        }
        if self.0.lease.is_some() && self.nanos(now) >= self.0.until.load(Ordering::Acquire) {
            return FenceStatus::DiskLeaseLost;
        }
        FenceStatus::Admits
    }

    /// When the fence last lifted after a lost lease, if it did.
    pub fn lifted_at(&self) -> Option<FenceInstant> {
        let lifted = self.0.lifted.load(Ordering::Acquire).checked_sub(1)?;
        self.0.origin.checked_add(Duration::from_nanos(lifted))
    }

    /// The check before a store call that PUTs or DELETEs `object` at
    /// `path`. The store's own retries of the call are not checked again.
    pub fn check_write(
        &self,
        object: FencedObject,
        operation: &'static str,
        path: &str,
    ) -> Result<(), FencedWrite> {
        let refusal = match self.status(fence_now()) {
            FenceStatus::Admits if object == FencedObject::Snapshot && self.holds_data() => {
                WriteRefusal::PredecessorWait
            }
            FenceStatus::Admits => return Ok(()),
            FenceStatus::DiskLeaseLost => WriteRefusal::DiskLeaseLost,
            FenceStatus::Closed => WriteRefusal::Closed,
            FenceStatus::Superseded => WriteRefusal::Superseded,
        };
        Err(FencedWrite {
            refusal,
            operation,
            path: path.to_owned(),
        })
    }

    /// Closes the fence for good: a shutdown does it before it records a
    /// clean end of the run.
    pub fn close(&self) {
        self.0.closed.store(true, Ordering::Release);
    }

    /// Closes the fence for good because another process may answer as
    /// this node. Unlike a shutdown's close, it keeps the run from
    /// recording a clean end. Returns whether this call superseded it.
    pub fn supersede(&self) -> bool {
        let first = !self.0.superseded.swap(true, Ordering::AcqRel);
        self.0.closed.store(true, Ordering::Release);
        first
    }

    /// Whether the fence closed for good ([`Self::close`],
    /// [`Self::supersede`]).
    pub fn is_closed(&self) -> bool {
        self.0.closed.load(Ordering::Acquire)
    }

    /// Whether a predecessor of this run may still run and is not waited
    /// out yet.
    pub fn owes_wait(&self) -> bool {
        self.0.owes_wait.load(Ordering::Acquire)
    }

    /// This run owes the predecessor wait. A leased fence only.
    pub fn begin_wait(&self) {
        if self.0.lease.is_some() {
            self.0.owes_wait.store(true, Ordering::Release);
        }
    }

    /// The predecessor wait finished.
    pub fn end_wait(&self) {
        self.0.owes_wait.store(false, Ordering::Release);
        self.0.waited.notify_waiters();
    }

    /// Returns once no predecessor wait is owed.
    pub async fn waited(&self) {
        self.until(|fence| !fence.owes_wait()).await;
    }

    /// This run's root was empty at its open and owes its wait from the
    /// meta group's binding: until [`Self::bind`], its data replicas take
    /// part in nothing. A leased fence only.
    pub fn await_binding(&self) {
        if self.0.lease.is_some() {
            self.0.awaits_binding.store(true, Ordering::Release);
        }
    }

    /// The meta group answered this empty root's registration. `owes_wait`
    /// says that the answer bound it in a bootstrapped cluster, so the run
    /// owes the predecessor wait from now on: it begins before the hold
    /// ends, so a data replica is never free in between.
    pub fn bind(&self, owes_wait: bool) {
        if owes_wait {
            self.begin_wait();
        }
        self.0.awaits_binding.store(false, Ordering::Release);
        self.0.waited.notify_waiters();
    }

    /// Whether this run's empty root awaits its binding ([`Self::await_binding`]).
    pub fn awaits_binding(&self) -> bool {
        self.0.awaits_binding.load(Ordering::Acquire)
    }

    /// Whether a data replica is held out now: a predecessor wait is owed,
    /// or the empty root awaits its binding.
    pub fn holds_data(&self) -> bool {
        self.owes_wait() || self.awaits_binding()
    }

    /// Returns once a data replica is no longer held out
    /// ([`Self::holds_data`]).
    pub async fn bound_and_waited(&self) {
        self.until(|fence| !fence.holds_data()).await;
    }

    async fn until(&self, done: impl Fn(&Self) -> bool) {
        loop {
            let notified = self.0.waited.notified();
            let mut notified = std::pin::pin!(notified);
            notified.as_mut().enable();
            if done(self) {
                return;
            }
            notified.await;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::DiskLease;
    use super::FenceStatus;
    use super::FencedObject;
    use super::LeaseChange;
    use super::NodeFence;
    use super::ProbeOutcome;
    use super::WriteRefusal;
    use super::fence_now;

    const L: Duration = Duration::from_secs(5);

    fn at(base: super::FenceInstant, millis: u64) -> super::FenceInstant {
        base.checked_add(Duration::from_millis(millis)).unwrap()
    }

    /// Each rule of the lease with time passed in: a hung probe ends it `L`
    /// after its last success's start, an error ends it at once, a late
    /// success never extends it, and only a probe that started after it
    /// ended lifts it.
    #[test]
    fn the_lease_holds_from_each_success_s_start_and_lifts_only_from_a_later_probe() {
        let t = fence_now();
        let mut lease = DiskLease::new(L);
        assert!(!lease.holds(t), "nothing holds before the first success");
        assert_eq!(lease.succeeded(t, at(t, 10)), LeaseChange::Began);
        assert!(lease.holds(at(t, 4_999)));
        // A hung probe: no later success, so the lease ends L after the
        // start of the last success, whatever the probe does.
        assert!(!lease.holds(at(t, 5_000)));
        // A success of a probe that started before the end, seen after it,
        // is too late: it never extends or lifts the lease.
        assert_eq!(
            lease.succeeded(at(t, 4_000), at(t, 6_000)),
            LeaseChange::TooLate
        );
        assert!(!lease.holds(at(t, 6_000)));
        // A probe that started after the end lifts it, measured from its
        // own start.
        assert_eq!(
            lease.succeeded(at(t, 6_000), at(t, 6_100)),
            LeaseChange::Lifted
        );
        assert!(lease.holds(at(t, 10_999)));
        assert!(!lease.holds(at(t, 11_000)));
        // A success while it holds extends it from its start, never from
        // when it was seen.
        assert_eq!(
            lease.succeeded(at(t, 7_000), at(t, 9_500)),
            LeaseChange::Extended
        );
        assert!(lease.holds(at(t, 11_999)));
        assert!(!lease.holds(at(t, 12_000)));
        // An older success seen later never moves the end back.
        assert_eq!(
            lease.succeeded(at(t, 6_500), at(t, 9_600)),
            LeaseChange::Extended
        );
        assert!(lease.holds(at(t, 11_999)));
        // An error ends it at once.
        lease.failed(at(t, 9_700));
        assert!(!lease.holds(at(t, 9_700)));
        // A probe that started before the error does not lift it.
        assert_eq!(
            lease.succeeded(at(t, 9_650), at(t, 9_800)),
            LeaseChange::TooLate
        );
        assert!(!lease.holds(at(t, 9_800)));
        assert_eq!(
            lease.succeeded(at(t, 9_900), at(t, 9_950)),
            LeaseChange::Lifted
        );
        assert!(lease.holds(at(t, 14_899)));
        // A probe that took longer than the lease proves nothing.
        assert_eq!(
            lease.succeeded(at(t, 20_000), at(t, 25_000)),
            LeaseChange::TooLate
        );
        assert!(!lease.holds(at(t, 25_000)));
    }

    /// The fence publishes the lease: it admits nothing before the first
    /// success, stops admitting at the lease's end or at a failure, records
    /// a lift, and stays closed after a shutdown whatever a probe says.
    #[test]
    fn the_fence_follows_its_lease_and_a_closed_fence_never_admits_again() {
        let fence = NodeFence::leased(L);
        let t = fence_now();
        assert_eq!(fence.status(t), FenceStatus::DiskLeaseLost);
        assert_eq!(
            fence.record_probe(t, ProbeOutcome::Synced, t),
            Some(LeaseChange::Began)
        );
        assert!(fence.admits(at(t, 4_999)));
        assert!(!fence.admits(at(t, 5_000)));
        assert_eq!(fence.lifted_at(), None);
        assert_eq!(
            fence.record_probe(at(t, 1_000), ProbeOutcome::Failed, at(t, 1_000)),
            None
        );
        assert_eq!(fence.status(at(t, 1_000)), FenceStatus::DiskLeaseLost);
        assert_eq!(
            fence.record_probe(at(t, 2_000), ProbeOutcome::Synced, at(t, 2_100)),
            Some(LeaseChange::Lifted)
        );
        assert!(fence.admits(at(t, 6_999)));
        assert!(fence.lifted_at().is_some());
        fence.close();
        assert_eq!(fence.status(at(t, 2_200)), FenceStatus::Closed);
        fence.record_probe(at(t, 2_300), ProbeOutcome::Synced, at(t, 2_300));
        assert!(!fence.admits(at(t, 2_400)));
        // An unleased fence admits until it closes, and owes no wait.
        let unleased = NodeFence::unleased();
        assert!(unleased.admits(at(t, 1_000_000)));
        unleased.begin_wait();
        assert!(!unleased.owes_wait());
        unleased.close();
        assert!(!unleased.admits_now());
        // A superseded fence never admits again, whatever a probe says, and
        // a later shutdown's close keeps it superseded.
        let superseded = NodeFence::leased(L);
        superseded.record_probe(t, ProbeOutcome::Synced, t);
        // Only the first call supersedes.
        assert!(superseded.supersede());
        assert!(!superseded.supersede());
        superseded.record_probe(at(t, 100), ProbeOutcome::Synced, at(t, 100));
        superseded.close();
        assert_eq!(superseded.status(at(t, 200)), FenceStatus::Superseded);
        assert!(superseded.is_closed());
        assert_eq!(
            superseded
                .check_write(FencedObject::Cold, "PUT", "cold/x")
                .unwrap_err()
                .refusal,
            WriteRefusal::Superseded
        );
    }

    /// A wait owed holds back object writes, though the fence admits.
    #[test]
    fn an_owed_wait_refuses_object_writes_until_it_ends() {
        let fence = NodeFence::leased(L);
        let t = fence_now();
        fence.record_probe(t, ProbeOutcome::Synced, t);
        fence.begin_wait();
        assert!(fence.admits(t));
        let refused = fence
            .check_write(FencedObject::Snapshot, "PUT", "snapshots/x")
            .unwrap_err();
        assert_eq!(refused.refusal, WriteRefusal::PredecessorWait);
        fence
            .check_write(FencedObject::Cold, "PUT", "cold/x")
            .unwrap();
        fence.end_wait();
        fence
            .check_write(FencedObject::Snapshot, "PUT", "snapshots/x")
            .unwrap();
        fence.close();
        let refused = fence
            .check_write(FencedObject::Cold, "DELETE", "cold/x")
            .unwrap_err();
        assert_eq!(refused.refusal, WriteRefusal::Closed);
    }

    /// An empty root awaiting its binding holds data replicas and snapshot
    /// objects out, and a binding in a bootstrapped cluster hands the hold
    /// to the wait with no gap. Catches a `bind` that ends the hold before
    /// the wait begins.
    #[test]
    fn a_binding_hands_the_hold_to_the_wait_with_no_gap() {
        let fence = NodeFence::leased(L);
        let t = fence_now();
        fence.record_probe(t, ProbeOutcome::Synced, t);
        fence.await_binding();
        assert!(fence.holds_data() && !fence.owes_wait());
        assert_eq!(
            fence
                .check_write(FencedObject::Snapshot, "PUT", "snapshots/x")
                .unwrap_err()
                .refusal,
            WriteRefusal::PredecessorWait
        );
        fence.bind(true);
        assert!(fence.holds_data() && fence.owes_wait() && !fence.awaits_binding());
        fence.end_wait();
        assert!(!fence.holds_data());
        // A forming cluster's answer settles the hold with no wait.
        let forming = NodeFence::leased(L);
        forming.await_binding();
        forming.bind(false);
        assert!(!forming.holds_data());
        // An unleased fence never holds.
        let unleased = NodeFence::unleased();
        unleased.await_binding();
        assert!(!unleased.holds_data());
    }
}

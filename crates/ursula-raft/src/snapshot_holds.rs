//! [`SnapshotHolds`]: what one engine incarnation of a group may still use
//! of its own snapshot objects, and its GC pass over them (#687, #669).
//!
//! A node writes its snapshot objects under its own directory of the store,
//! `{prefix}/node-{node}/group-{gid}/`, each under a name no object had
//! before, and only that node deletes them. A follower that installs a
//! peer's snapshot writes its own copy and commits the copy. So whatever a
//! node may still use of an object is the node's own business, and it is
//! held here, in memory, never in the store:
//!
//! - `current`: the object the durable snapshot metadata names;
//! - `failed_candidate`: the object a metadata publication that failed may
//!   have named, kept until the next restart resolves it;
//! - a [`SnapshotLease`] on every object a build or an install copy is about
//!   to write, taken before its PUT, and on every object a send names, kept
//!   until the RPC returns.
//!
//! A GC pass ([`SnapshotHolds::collect_garbage`]) lists the node's directory
//! for the group, then reads the holds, then deletes every listed object
//! that is not held. It runs only once this incarnation's restore ended,
//! so that the holds are authoritative, and only as admitted work of this
//! incarnation, while its engine runs: the engine's shutdown stops a pass
//! and waits for it, and a pass on a stopped incarnation's holds never runs
//! (`GroupSnapshots::fenced_pass`).
//!
//! Proof sketch. Let a pass delete `k`. The LIST showed `k`, so its PUT was
//! sent before the LIST returned, and its writer held `k` before it sent
//! the PUT. The pass read the holds after the LIST, the leases and the
//! state in one step, and found `k` released,
//! and a released name is never held again: every write draws a fresh
//! name, and `current` moves only to an object just written. Every send of
//! `k` held it, so it ended before the release, and no other node commits
//! `k`, since a receiver commits only its own copy. A stale LIST only leaves
//! a key out, for a later pass, or lists a deleted one again, whose second
//! DELETE does nothing. No step reads a clock or a modification time.
//!
//! A pass is owed at once when a key leaves the holds, and after the
//! restore. A write that failed may still land, so it owes a second pass
//! one `raft.install_snapshot_timeout` later, and so does the restore, for
//! a write the previous process sent. A failed pass runs again later,
//! backed off ([`gc_retry_delay`]).

use std::collections::BTreeSet;
use std::fmt;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::PoisonError;
use std::sync::Weak;
use std::time::Duration;

use ursula_runtime::KeyHold;
use ursula_runtime::KeyHolds;
use ursula_runtime::OwnedLocation;
use ursula_runtime::Release;
use ursula_runtime::SharedSnapshotStore;
use ursula_runtime::SnapshotLocation;
use ursula_runtime::SnapshotStoreError;
use ursula_runtime::StagedSnapshot;
use ursula_shard::RaftGroupId;

use crate::rt::time::Instant;

/// How long after a failed GC pass of a group the next one runs, doubled
/// for each failed pass in a row up to [`SNAPSHOT_GC_MAX_RETRY`].
const SNAPSHOT_GC_RETRY: Duration = Duration::from_secs(30);
/// The longest wait before a failed group's next GC pass.
const SNAPSHOT_GC_MAX_RETRY: Duration = Duration::from_secs(15 * 60);

/// The wait before the next GC pass after `failures` failed passes in a
/// row: [`SNAPSHOT_GC_RETRY`], doubled for each further failure, at most
/// [`SNAPSHOT_GC_MAX_RETRY`].
fn gc_retry_delay(failures: u32) -> Duration {
    SNAPSHOT_GC_RETRY
        .saturating_mul(2_u32.saturating_pow(failures.saturating_sub(1).min(16)))
        .min(SNAPSHOT_GC_MAX_RETRY)
}

/// The GC passes a group owes: the earliest and the latest only. A pass
/// deletes every object nothing holds at its time, so the latest pass
/// covers every reason to run one before it, and the earliest keeps the
/// next pass prompt. The list stays bounded with or without a driver.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
enum GcPasses {
    #[default]
    None,
    One(Instant),
    Two {
        earliest: Instant,
        latest: Instant,
    },
}

impl GcPasses {
    fn add(&mut self, at: Instant) {
        *self = match *self {
            Self::None => Self::One(at),
            Self::One(owed) if owed == at => Self::One(at),
            Self::One(owed) => Self::Two {
                earliest: owed.min(at),
                latest: owed.max(at),
            },
            Self::Two { earliest, latest } => Self::Two {
                earliest: earliest.min(at),
                latest: latest.max(at),
            },
        };
    }

    fn earliest(self) -> Option<Instant> {
        match self {
            Self::None => None,
            Self::One(at) | Self::Two { earliest: at, .. } => Some(at),
        }
    }

    /// Drops the passes due by `now`. Returns whether any was.
    fn take_due(&mut self, now: Instant) -> bool {
        let (next, due) = match *self {
            Self::None => (Self::None, false),
            Self::One(at) if at <= now => (Self::None, true),
            Self::One(at) => (Self::One(at), false),
            Self::Two { latest, .. } if latest <= now => (Self::None, true),
            Self::Two { earliest, latest } if earliest <= now => (Self::One(latest), true),
            two @ Self::Two { .. } => (two, false),
        };
        *self = next;
        due
    }
}

/// Whether this incarnation's restore ended, so that the holds name every
/// object the incarnation may use.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
enum Restore {
    #[default]
    Pending,
    Done,
}

#[derive(Debug, Default)]
struct HoldState {
    restore: Restore,
    /// The object the durable snapshot metadata names.
    current: Option<String>,
    /// A terminal metadata failure may have published this candidate before
    /// returning an error. The publication is latched failed after it, so
    /// there is at most one, and it stays held for the rest of the
    /// incarnation: the next restore reads which pointer is durable.
    failed_candidate: Option<String>,
    /// The GC passes the group owes.
    passes: GcPasses,
    /// GC passes of the group that failed in a row, which back the next one
    /// off ([`gc_retry_delay`]).
    gc_failures: u32,
}

/// What one engine incarnation of a group holds of its own snapshot
/// objects, and the GC passes it owes. Every incarnation starts with its
/// own, empty and not restored.
pub(crate) struct SnapshotHolds {
    group: RaftGroupId,
    /// The objects builds, install copies and sends lease, each until its
    /// last lease drops.
    leases: KeyHolds<(), Release>,
    state: Mutex<HoldState>,
    /// `raft.install_snapshot_timeout`: how long after a write that failed,
    /// and after a restore, the second GC pass is owed, for a PUT that
    /// lands late.
    late_write: Duration,
    /// What runs within a pass's read of the holds, for a test that races a
    /// send and a build against the pass.
    #[cfg(test)]
    seam: Mutex<Option<HeldReadSeam>>,
}

/// A test's step within a pass's read of the holds, and how the pass reads
/// them around it.
#[cfg(test)]
pub(crate) struct HeldReadSeam {
    pub(crate) read: HeldRead,
    pub(crate) between: Box<dyn FnOnce() + Send>,
}

/// How a pass reads the holds in a test: as production does, the leases
/// under the state's guard with the step between the guard and the leases,
/// or, as the negative control, the leases, then the step, then the state.
#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum HeldRead {
    OneStep,
    TwoSteps,
}

impl fmt::Debug for SnapshotHolds {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SnapshotHolds")
            .field("group", &self.group)
            .field("leases", &self.leases)
            .field("state", &self.state())
            .field("late_write", &self.late_write)
            .finish()
    }
}

/// What one GC pass of a group did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SnapshotPass {
    /// The incarnation's restore has not ended, so the holds may not name
    /// every object it uses yet: the pass listed and deleted nothing.
    NotRestored,
    /// The pass deleted `deleted` objects that nothing held.
    Collected { deleted: u64 },
}

impl SnapshotPass {
    /// The objects the pass deleted.
    pub(crate) fn deleted(self) -> u64 {
        match self {
            Self::NotRestored => 0,
            Self::Collected { deleted } => deleted,
        }
    }
}

/// A GC pass of one group that failed. The pass runs again later, backed
/// off by [`gc_retry_delay`].
#[derive(Debug, thiserror::Error)]
pub(crate) enum SnapshotGcError {
    /// Listing the node's directory for the group failed.
    #[error("list the group's own snapshot objects")]
    List(#[source] SnapshotStoreError),
    /// Deleting an object nothing held failed.
    #[error("delete a snapshot object the group no longer holds")]
    Delete(#[source] SnapshotStoreError),
}

/// An object a build or an install copy wrote and checked, still held.
pub(crate) struct HeldWrite {
    /// What the write created: inline, or the owned key its stage drew.
    pub(crate) location: OwnedLocation,
    /// The object's lease, which the caller keeps until the object is
    /// current or given up; `None` for an inline snapshot.
    pub(crate) lease: Option<SnapshotLease>,
}

/// Why [`SnapshotHolds::write`] gave its caller no object.
#[derive(Debug, thiserror::Error)]
pub(crate) enum HeldWriteError {
    /// The PUT failed. A name collision on the node's own fresh name is
    /// likelier its own create retried after a lost answer than another
    /// write, so it is a failure like any other: the name is not held.
    #[error("write the snapshot object {key}")]
    Write {
        key: String,
        #[source]
        source: SnapshotStoreError,
    },
    /// The HEAD after the PUT did not find the object as written.
    #[error("check the snapshot object {key}")]
    Verify {
        key: String,
        #[source]
        source: SnapshotStoreError,
    },
}

/// The second GC pass a write owes unless it ended with its object checked:
/// a PUT that failed, timed out or was cancelled may still land, within one
/// install timeout ([`SnapshotHolds::owe_late_write_pass`]).
struct LateWrite<'h> {
    holds: Option<&'h SnapshotHolds>,
}

impl LateWrite<'_> {
    fn done(mut self) {
        self.holds = None;
    }
}

impl Drop for LateWrite<'_> {
    fn drop(&mut self) {
        if let Some(holds) = self.holds {
            holds.owe_late_write_pass();
        }
    }
}

/// A hold on one snapshot object of this node: a build's or an install
/// copy's, from before its PUT until the object became current or was
/// given up, or a send's, until the RPC that names the object returns. A
/// clone is another hold on the same object. When the last hold on an
/// object that is not current drops, its group owes a GC pass at once.
pub struct SnapshotLease {
    /// The hold, until the lease drops.
    hold: Option<KeyHold<(), Release>>,
    /// The holds the lease belongs to. Weak: a lease a send keeps after its
    /// engine shut down holds nothing of the engine.
    holds: Weak<SnapshotHolds>,
}

impl SnapshotLease {
    /// The held object's key.
    pub fn key(&self) -> &str {
        self.hold.as_ref().map_or("", KeyHold::key)
    }
}

impl Clone for SnapshotLease {
    fn clone(&self) -> Self {
        Self {
            hold: self.hold.clone(),
            holds: self.holds.clone(),
        }
    }
}

impl fmt::Debug for SnapshotLease {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SnapshotLease")
            .field("key", &self.key())
            .finish()
    }
}

impl Drop for SnapshotLease {
    fn drop(&mut self) {
        let Some(hold) = self.hold.take() else {
            return;
        };
        let key = hold.key().to_owned();
        // Released first, so that the pass this owes sees the release.
        drop(hold);
        if let Some(holds) = self.holds.upgrade() {
            holds.released(&key);
        }
    }
}

#[cfg(test)]
impl Default for SnapshotHolds {
    fn default() -> Self {
        Self::new(
            RaftGroupId(7),
            crate::state_machine::DEFAULT_LATE_WRITE_PASS,
        )
    }
}

impl SnapshotHolds {
    /// The empty holds of a new incarnation of `group`, not restored yet.
    pub(crate) fn new(group: RaftGroupId, late_write: Duration) -> Self {
        Self {
            group,
            leases: KeyHolds::new(),
            state: Mutex::new(HoldState::default()),
            late_write,
            #[cfg(test)]
            seam: Mutex::new(None),
        }
    }

    fn state(&self) -> std::sync::MutexGuard<'_, HoldState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// A lease on the object `key` names, taken before it is written or
    /// sent.
    pub(crate) fn lease(self: &Arc<Self>, key: &str) -> SnapshotLease {
        SnapshotLease {
            hold: Some(self.leases.hold(key, ())),
            holds: Arc::downgrade(self),
        }
    }

    /// Writes `staged` and checks its object with a HEAD, holding the object
    /// from before its PUT (R2 of #687). The one policy of every object a
    /// node writes, a build's and an install copy's. A write that does not
    /// end with its object checked, a cancelled one included, owes the
    /// second GC pass, and its lease drops at once, which owes the first.
    pub(crate) async fn write(
        self: &Arc<Self>,
        store: &SharedSnapshotStore,
        staged: StagedSnapshot,
    ) -> Result<HeldWrite, HeldWriteError> {
        let key = match &staged {
            StagedSnapshot::Object(object) => {
                let key = object.path();
                // An `OwnedLocation` names no group, so the group of the
                // object these holds commit is checked here, before the PUT.
                if object.key().owner().raft_group_id() != self.group {
                    return Err(HeldWriteError::Write {
                        source: SnapshotStoreError::NotOwnedHere { path: key.clone() },
                        key,
                    });
                }
                Some(key)
            }
            StagedSnapshot::Inline { .. } => None,
        };
        // The negative control takes the lease once the PUT answered.
        let lease_after_put = key.is_some()
            && ursula_stream::sim_controls::off(
                ursula_stream::sim_controls::Defence::LeaseBeforePut,
            );
        let mut lease = key
            .as_deref()
            .filter(|_| !lease_after_put)
            .map(|key| self.lease(key));
        let late = LateWrite {
            holds: key.is_some().then_some(self.as_ref()),
        };
        let location =
            store
                .write_staged(staged)
                .await
                .map_err(|source| HeldWriteError::Write {
                    key: key.clone().unwrap_or_default(),
                    source,
                })?;
        if let Some(key) = key {
            if lease_after_put {
                ursula_stream::sim_controls::note_fired(
                    ursula_stream::sim_controls::Defence::LeaseBeforePut,
                );
                lease = Some(self.lease(&key));
            }
            store
                .verify_uploaded(location.location())
                .await
                .map_err(|source| HeldWriteError::Verify { key, source })?;
        }
        late.done();
        Ok(HeldWrite { location, lease })
    }

    /// A lease on the object `location` names, if it names one.
    #[cfg(test)]
    pub(crate) fn lease_location(
        self: &Arc<Self>,
        location: &SnapshotLocation,
    ) -> Option<SnapshotLease> {
        match location {
            SnapshotLocation::S3 { key, .. } => Some(self.lease(key)),
            SnapshotLocation::Inline { .. } | SnapshotLocation::Local { .. } => None,
        }
    }

    /// Whether `key` is held now, read in one step as [`Self::held_keys`]
    /// reads every key.
    fn holds_key(&self, key: &str) -> bool {
        let state = self.state();
        self.leases.contains(key)
            || state.current.as_deref() == Some(key)
            || state.failed_candidate.as_deref() == Some(key)
    }

    /// Owes a pass at once if the last lease on `key` just dropped and
    /// nothing else holds it.
    fn released(&self, key: &str) {
        if !self.holds_key(key) {
            self.owe_pass(Instant::now());
        }
    }

    /// Every key held now, in one step: the leases are read under the
    /// state's guard, so no commit moves `current` between the two reads.
    ///
    /// Two steps in either order lose an object in use. Leases, then state:
    /// a send leases the current object after the first read, a build then
    /// replaces it, and the second read finds it neither leased nor
    /// current. State, then leases: a build commits its object after the
    /// first read and drops its lease before the second. Under the guard,
    /// a send leases only an object that is current at the read, since
    /// [`Self::commit_current`] runs under the same lock as the send's read
    /// of the current snapshot, and a build's lease drops only after its
    /// commit, which waits for the guard.
    pub(crate) fn held_keys(&self) -> BTreeSet<String> {
        #[cfg(test)]
        let seam = self.take_seam();
        #[cfg(test)]
        if let Some(HeldReadSeam {
            read: HeldRead::TwoSteps,
            between,
        }) = seam
        {
            return self.held_keys_in_two_steps(between);
        }
        let state = self.state();
        // A test's step runs here, between the guard and the read of the
        // leases, so a production read that took the leases before the
        // guard would miss what the step leased.
        #[cfg(test)]
        if let Some(HeldReadSeam { between, .. }) = seam {
            between();
        }
        let mut held: BTreeSet<String> = self.leases.keys().into_iter().collect();
        held.extend(state.current.iter().cloned());
        held.extend(state.failed_candidate.iter().cloned());
        held
    }

    /// The step a test set for the next read of the holds, if any.
    #[cfg(test)]
    fn take_seam(&self) -> Option<HeldReadSeam> {
        self.seam
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
    }

    /// The negative control of [`Self::held_keys`]: the leases read first,
    /// with no guard, then a test's step, then the state.
    #[cfg(test)]
    fn held_keys_in_two_steps(&self, between: Box<dyn FnOnce() + Send>) -> BTreeSet<String> {
        let mut held: BTreeSet<String> = self.leases.keys().into_iter().collect();
        between();
        let state = self.state();
        held.extend(state.current.iter().cloned());
        held.extend(state.failed_candidate.iter().cloned());
        held
    }

    /// Sets the step the next read of the holds runs between its reads.
    #[cfg(test)]
    pub(crate) fn set_seam(&self, seam: HeldReadSeam) {
        *self.seam.lock().unwrap_or_else(PoisonError::into_inner) = Some(seam);
    }

    /// The object the durable metadata names now, if it names one.
    #[cfg(test)]
    pub(crate) fn current(&self) -> Option<String> {
        self.state().current.clone()
    }

    /// Records that the durable metadata names `location` now. Call it while
    /// the location's lease, if it names an object, still stands. The object
    /// it replaces leaves the holds unless a lease keeps it, and then the
    /// group owes a pass.
    pub(crate) fn commit_current(&self, location: &SnapshotLocation) {
        let next = match location {
            SnapshotLocation::S3 { key, .. } => Some(key.clone()),
            SnapshotLocation::Inline { .. } | SnapshotLocation::Local { .. } => None,
        };
        let replaced = {
            let mut state = self.state();
            state.failed_candidate = None;
            let previous = std::mem::replace(&mut state.current, next);
            previous.filter(|previous| state.current.as_ref() != Some(previous))
        };
        if let Some(replaced) = replaced {
            self.released(&replaced);
        }
    }

    /// Keeps `location`, which a metadata publication that failed may have
    /// named, held for the rest of the incarnation.
    pub(crate) fn retain_failed_candidate(&self, location: &SnapshotLocation) {
        if let SnapshotLocation::S3 { key, .. } = location {
            self.state().failed_candidate = Some(key.clone());
        }
    }

    /// Records that this incarnation's restore ended: the holds name every
    /// object it uses. A pass is owed at once, which deletes what the last
    /// incarnation left behind, and another one one install timeout later,
    /// for a PUT that the last incarnation sent and that lands late.
    pub(crate) fn mark_restored(&self) {
        let now = Instant::now();
        let mut state = self.state();
        state.restore = Restore::Done;
        state.passes.add(now);
        if let Some(late) = now.checked_add(self.late_write) {
            state.passes.add(late);
        }
    }

    /// Owes a GC pass at `at`.
    pub(crate) fn owe_pass(&self, at: Instant) {
        self.state().passes.add(at);
    }

    /// Owes the second pass of a write that failed or may have: its PUT may
    /// still land, within one install timeout.
    pub(crate) fn owe_late_write_pass(&self) {
        let now = Instant::now();
        let mut state = self.state();
        state.passes.add(now);
        if let Some(late) = now.checked_add(self.late_write) {
            state.passes.add(late);
        }
    }

    /// Whether a GC pass is owed by `now`. A group whose restore has not
    /// ended owes none yet.
    pub(crate) fn gc_due(&self, now: Instant) -> bool {
        let state = self.state();
        state.restore == Restore::Done && state.passes.earliest().is_some_and(|at| at <= now)
    }

    /// One GC pass of the group at `now`: lists the node's directory for
    /// the group, then reads the holds, then deletes every listed object
    /// that is not held. The restore must have ended. A failed pass runs
    /// again later, backed off exponentially ([`gc_retry_delay`]) until a
    /// pass succeeds.
    pub(crate) async fn collect_garbage(
        &self,
        store: &SharedSnapshotStore,
        now: Instant,
    ) -> Result<SnapshotPass, SnapshotGcError> {
        {
            let mut state = self.state();
            if state.restore == Restore::Pending {
                return Ok(SnapshotPass::NotRestored);
            }
            state.passes.take_due(now);
            // The negative control: a pass that lists and deletes nothing.
            if ursula_stream::sim_controls::off(ursula_stream::sim_controls::Defence::SnapshotGc) {
                ursula_stream::sim_controls::note_fired(
                    ursula_stream::sim_controls::Defence::SnapshotGc,
                );
                return Ok(SnapshotPass::Collected { deleted: 0 });
            }
        }
        let pass = self.pass(store).await;
        let failures = {
            let mut state = self.state();
            state.gc_failures = if pass.is_err() {
                state.gc_failures.saturating_add(1)
            } else {
                0
            };
            state.gc_failures
        };
        if failures > 0
            && let Some(at) = Instant::now().checked_add(gc_retry_delay(failures))
        {
            self.owe_pass(at);
        }
        pass
    }

    async fn pass(&self, store: &SharedSnapshotStore) -> Result<SnapshotPass, SnapshotGcError> {
        // The negative control reads the holds before the LIST.
        let held_before_list =
            ursula_stream::sim_controls::off(ursula_stream::sim_controls::Defence::HoldsAfterList)
                .then(|| self.held_keys());
        let listing = store
            .list_own(self.group)
            .await
            .map_err(SnapshotGcError::List)?;
        // Read after the LIST returned: an object whose PUT the LIST shows
        // was held before that PUT was sent.
        let held = match held_before_list {
            Some(held) => {
                ursula_stream::sim_controls::note_fired(
                    ursula_stream::sim_controls::Defence::HoldsAfterList,
                );
                held
            }
            None => self.held_keys(),
        };
        if !listing.foreign.is_empty() {
            tracing::warn!(
                raft_group_id = self.group.0,
                files = ?listing.foreign,
                "snapshot GC leaves files in the node's snapshot directory that are not its objects"
            );
        }
        let mut deleted = 0_u64;
        for object in listing.owned {
            if held.contains(object.path()) {
                continue;
            }
            store
                .delete_own(object)
                .await
                .map_err(SnapshotGcError::Delete)?;
            deleted = deleted.saturating_add(1);
        }
        if deleted > 0 {
            tracing::info!(
                raft_group_id = self.group.0,
                deleted,
                held = held.len(),
                "deleted snapshot objects this node no longer holds"
            );
        }
        Ok(SnapshotPass::Collected { deleted })
    }
}

#[cfg(test)]
#[cfg(not(madsim))]
mod tests {
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::time::Duration;

    use bytes::Bytes;
    use ursula_runtime::OwnedKey;
    use ursula_runtime::S3SnapshotStore;
    use ursula_runtime::SharedSnapshotStore;
    use ursula_runtime::SnapshotLocation;
    use ursula_runtime::SnapshotPrefix;
    use ursula_runtime::SnapshotStore;
    use ursula_runtime::SnapshotStoreError;
    use ursula_shard::RaftGroupId;

    use super::GcPasses;
    use super::HeldRead;
    use super::HeldReadSeam;
    use super::SNAPSHOT_GC_MAX_RETRY;
    use super::SNAPSHOT_GC_RETRY;
    use super::SnapshotGcError;
    use super::SnapshotHolds;
    use super::SnapshotPass;
    use super::gc_retry_delay;
    use crate::rt::time::Instant;

    const GROUP: RaftGroupId = RaftGroupId(7);
    const LATE: Duration = Duration::from_secs(120);

    fn later(now: Instant, by: Duration) -> Instant {
        now.checked_add(by).unwrap()
    }

    fn holds() -> Arc<SnapshotHolds> {
        Arc::new(SnapshotHolds::new(GROUP, LATE))
    }

    fn store() -> SharedSnapshotStore {
        Arc::new(S3SnapshotStore::memory_for_tests("snapshots", 1).unwrap())
    }

    async fn write(
        store: &SharedSnapshotStore,
        index: u64,
        body: &'static [u8],
    ) -> SnapshotLocation {
        let staged = store
            .stage(
                GROUP,
                index,
                Box::new(std::iter::once(Ok(Bytes::from_static(body)))),
            )
            .await
            .unwrap();
        store.write_staged(staged).await.unwrap().into_location()
    }

    fn key(location: &SnapshotLocation) -> &str {
        let SnapshotLocation::S3 { key, .. } = location else {
            panic!("expected an S3 location")
        };
        key
    }

    fn gone(result: Result<Vec<u8>, SnapshotStoreError>) -> bool {
        matches!(result, Err(SnapshotStoreError::NotFound { .. }))
    }

    /// A pass before the restore ended deletes nothing, even an object no
    /// hold names: the holds are not authoritative yet. Once restored, the
    /// pass deletes it and keeps the current object.
    #[tokio::test]
    async fn a_pass_before_the_restore_deletes_nothing() {
        let store = store();
        let holds = holds();
        let orphan = write(&store, 1, b"orphan").await;
        let current = write(&store, 2, b"current").await;
        holds.commit_current(&current);
        assert!(!holds.gc_due(later(Instant::now(), LATE)));
        assert_eq!(
            holds.collect_garbage(&store, Instant::now()).await.unwrap(),
            SnapshotPass::NotRestored
        );
        assert_eq!(store.download(&orphan).await.unwrap(), b"orphan");

        holds.mark_restored();
        assert!(holds.gc_due(Instant::now()));
        assert_eq!(
            holds.collect_garbage(&store, Instant::now()).await.unwrap(),
            SnapshotPass::Collected { deleted: 1 }
        );
        assert!(gone(store.download(&orphan).await));
        assert_eq!(store.download(&current).await.unwrap(), b"current");
        // The restore owes a second pass one install timeout later.
        assert!(!holds.gc_due(Instant::now()));
        assert!(holds.gc_due(later(Instant::now(), LATE)));
    }

    /// A replaced current leaves the holds and owes a pass at once, unless a
    /// lease keeps it; then the pass is owed when the lease drops.
    #[tokio::test]
    async fn a_replaced_current_goes_once_no_lease_keeps_it() {
        let store = store();
        let holds = holds();
        holds.mark_restored();
        holds.collect_garbage(&store, Instant::now()).await.unwrap();
        let old = write(&store, 1, b"old").await;
        let new = write(&store, 2, b"new").await;
        holds.commit_current(&old);
        let send = holds.lease_location(&old).expect("an object lease");
        assert_eq!(send.key(), key(&old));
        holds.commit_current(&new);
        assert!(!holds.gc_due(Instant::now()), "a send still holds it");
        // A pass that runs anyway keeps the leased object.
        holds.owe_pass(Instant::now());
        holds.collect_garbage(&store, Instant::now()).await.unwrap();
        assert_eq!(store.download(&old).await.unwrap(), b"old");
        drop(send);
        assert!(holds.gc_due(Instant::now()), "the release owes a pass");
        assert_eq!(
            holds.collect_garbage(&store, Instant::now()).await.unwrap(),
            SnapshotPass::Collected { deleted: 1 }
        );
        assert!(gone(store.download(&old).await));
        assert_eq!(store.download(&new).await.unwrap(), b"new");
    }

    /// A lease on the current object owes nothing when it drops, and an
    /// inline current holds no object.
    #[tokio::test]
    async fn a_lease_on_the_current_object_owes_no_pass() {
        let holds = holds();
        holds.mark_restored();
        let store = store();
        holds.collect_garbage(&store, Instant::now()).await.unwrap();
        holds
            .collect_garbage(&store, later(Instant::now(), LATE))
            .await
            .unwrap();
        let current = write(&store, 1, b"current").await;
        holds.commit_current(&current);
        drop(holds.lease_location(&current));
        assert!(!holds.gc_due(Instant::now()));
        let inline = SnapshotLocation::Inline { bytes: vec![1] };
        assert!(holds.lease_location(&inline).is_none());
        holds.commit_current(&inline);
        assert!(holds.held_keys().is_empty());
        assert!(
            holds.gc_due(Instant::now()),
            "the replaced object owes a pass"
        );
    }

    /// A send that leases the current object while a pass reads the holds,
    /// and a build that then replaces the object, leave the object to the
    /// send: the pass reads the leases and the state in one step, so it
    /// finds the object current. The negative control reads them in two
    /// steps: the pass finds the object neither leased, at its first read,
    /// nor current, at its second, and deletes it while the send runs.
    #[tokio::test]
    async fn a_send_that_leases_during_a_pass_keeps_its_object() {
        for read in [HeldRead::OneStep, HeldRead::TwoSteps] {
            let store = store();
            let holds = holds();
            holds.mark_restored();
            holds.collect_garbage(&store, Instant::now()).await.unwrap();
            holds
                .collect_garbage(&store, later(Instant::now(), LATE))
                .await
                .unwrap();
            let sent = write(&store, 1, b"sent").await;
            holds.commit_current(&sent);
            let replacing = write(&store, 2, b"replacing").await;
            // The build leased its object before its PUT.
            let build = Arc::new(Mutex::new(holds.lease_location(&replacing)));
            let send = Arc::new(Mutex::new(None));
            holds.set_seam(HeldReadSeam {
                read,
                between: Box::new({
                    let holds = holds.clone();
                    let sent = sent.clone();
                    let replacing = replacing.clone();
                    let send = send.clone();
                    let build = build.clone();
                    move || {
                        // The send leases the object current at its read.
                        *send.lock().unwrap() =
                            Some(holds.lease_location(&sent).expect("an object lease"));
                        // With the state's guard released, the build's
                        // commit runs here, and its lease drops after it.
                        // Under the guard, the commit waits for the read
                        // to end, so the test runs it after the pass.
                        if read == HeldRead::TwoSteps {
                            holds.commit_current(&replacing);
                            drop(build.lock().unwrap().take());
                        }
                    }
                }),
            });
            holds.owe_pass(Instant::now());
            let pass = holds.collect_garbage(&store, Instant::now()).await.unwrap();
            let send = send.lock().unwrap().take().expect("the send's lease");
            match read {
                HeldRead::OneStep => {
                    assert_eq!(pass, SnapshotPass::Collected { deleted: 0 });
                    holds.commit_current(&replacing);
                    drop(build.lock().unwrap().take());
                    assert_eq!(store.download(&sent).await.unwrap(), b"sent");
                    drop(send);
                    assert!(holds.gc_due(Instant::now()), "the release owes a pass");
                    holds.collect_garbage(&store, Instant::now()).await.unwrap();
                    assert!(gone(store.download(&sent).await));
                }
                HeldRead::TwoSteps => {
                    assert!(
                        gone(store.download(&sent).await),
                        "control: two reads lose the object a send holds"
                    );
                    assert_eq!(send.key(), key(&sent));
                }
            }
            assert_eq!(holds.current().as_deref(), Some(key(&replacing)));
            assert_eq!(store.download(&replacing).await.unwrap(), b"replacing");
        }
    }

    /// A failed candidate stays held, and a commit clears it.
    #[tokio::test]
    async fn a_failed_candidate_stays_held_until_a_commit() {
        let store = store();
        let holds = holds();
        let candidate = write(&store, 1, b"candidate").await;
        holds.retain_failed_candidate(&candidate);
        assert!(holds.held_keys().contains(key(&candidate)));
        holds.mark_restored();
        holds.collect_garbage(&store, Instant::now()).await.unwrap();
        assert_eq!(store.download(&candidate).await.unwrap(), b"candidate");
        holds.commit_current(&SnapshotLocation::Inline { bytes: Vec::new() });
        assert!(holds.held_keys().is_empty());
    }

    /// A failed write owes a pass at once and another one install timeout
    /// later, for a PUT that lands late.
    #[tokio::test]
    async fn a_failed_write_owes_a_second_pass_an_install_timeout_later() {
        let store = store();
        let holds = holds();
        holds.mark_restored();
        holds.collect_garbage(&store, Instant::now()).await.unwrap();
        holds
            .collect_garbage(&store, later(Instant::now(), LATE))
            .await
            .unwrap();
        assert!(!holds.gc_due(later(Instant::now(), LATE.saturating_mul(2))));
        holds.owe_late_write_pass();
        assert!(holds.gc_due(Instant::now()));
        holds.collect_garbage(&store, Instant::now()).await.unwrap();
        assert!(!holds.gc_due(Instant::now()));
        assert!(holds.gc_due(later(Instant::now(), LATE)));
    }

    /// The pass deletes only the node's own objects: node 10's directory
    /// is untouched by node 1's pass.
    #[tokio::test]
    async fn a_pass_stays_in_its_nodes_directory() {
        let one = S3SnapshotStore::memory_for_tests("snapshots", 1).unwrap();
        let ten: SharedSnapshotStore = Arc::new(one.sibling_for_tests(10));
        let one: SharedSnapshotStore = Arc::new(one);
        let foreign = write(&ten, 1, b"ten").await;
        let own = write(&one, 1, b"one").await;
        let holds = holds();
        holds.mark_restored();
        assert_eq!(
            holds.collect_garbage(&one, Instant::now()).await.unwrap(),
            SnapshotPass::Collected { deleted: 1 }
        );
        assert!(gone(one.download(&own).await));
        assert_eq!(ten.download(&foreign).await.unwrap(), b"ten");
        let prefix = SnapshotPrefix::try_from("snapshots").unwrap();
        assert_eq!(
            OwnedKey::parse(&prefix, key(&foreign))
                .unwrap()
                .owner()
                .node_id(),
            10
        );
    }

    /// The holds of one group write no object of another group: the check
    /// runs before the PUT, so nothing lands and nothing is held.
    #[tokio::test]
    async fn the_holds_of_a_group_write_no_object_of_another() {
        let store = store();
        let holds = holds();
        let staged = store
            .stage(
                RaftGroupId(8),
                1,
                Box::new(std::iter::once(Ok(Bytes::from_static(b"other")))),
            )
            .await
            .unwrap();
        let refused = holds.write(&store, staged).await.err().expect("refused");
        assert!(
            matches!(&refused, super::HeldWriteError::Write {
                source: SnapshotStoreError::NotOwnedHere { .. },
                ..
            }),
            "{refused:?}"
        );
        assert!(holds.held_keys().is_empty());
        assert!(
            store
                .list_own(RaftGroupId(8))
                .await
                .unwrap()
                .owned
                .is_empty()
        );
    }

    /// The owed passes stay two, the earliest and the latest, however many
    /// are owed, and the latest stays owed once the earliest ran.
    #[test]
    fn owed_passes_stay_bounded() {
        let mut passes = GcPasses::default();
        let start = Instant::now();
        for seconds in [30, 10, 50, 20, 40] {
            passes.add(later(start, Duration::from_secs(seconds)));
        }
        assert_eq!(passes, GcPasses::Two {
            earliest: later(start, Duration::from_secs(10)),
            latest: later(start, Duration::from_secs(50)),
        });
        assert!(passes.take_due(later(start, Duration::from_secs(10))));
        assert_eq!(
            passes.earliest(),
            Some(later(start, Duration::from_secs(50)))
        );
        assert!(!passes.take_due(later(start, Duration::from_secs(49))));
        assert!(passes.take_due(later(start, Duration::from_secs(50))));
        assert_eq!(passes, GcPasses::None);
    }

    /// The retry after failed passes doubles from 30 s and stops at 15 min.
    #[test]
    fn gc_retries_back_off_exponentially_up_to_a_cap() {
        assert_eq!(gc_retry_delay(1), SNAPSHOT_GC_RETRY);
        assert_eq!(gc_retry_delay(2), Duration::from_secs(60));
        assert_eq!(gc_retry_delay(3), Duration::from_secs(120));
        assert_eq!(gc_retry_delay(5), Duration::from_secs(480));
        assert_eq!(gc_retry_delay(6), SNAPSHOT_GC_MAX_RETRY);
        assert_eq!(gc_retry_delay(u32::MAX), SNAPSHOT_GC_MAX_RETRY);
    }

    /// A store whose LIST fails.
    #[derive(Debug)]
    struct UnlistableStore;

    impl SnapshotStore for UnlistableStore {
        fn namespace(&self, raft_group_id: RaftGroupId) -> ursula_runtime::OwnedNamespace {
            SnapshotStore::namespace(&ursula_runtime::InlineSnapshotStore, raft_group_id)
        }

        fn stage<'a>(
            &'a self,
            raft_group_id: RaftGroupId,
            index: u64,
            chunks: ursula_runtime::SnapshotBytesIterator,
        ) -> ursula_runtime::SnapshotStoreFuture<'a, ursula_runtime::StagedSnapshot> {
            static INLINE: ursula_runtime::InlineSnapshotStore =
                ursula_runtime::InlineSnapshotStore;
            SnapshotStore::stage(&INLINE, raft_group_id, index, chunks)
        }

        fn write_staged<'a>(
            &'a self,
            staged: ursula_runtime::StagedSnapshot,
        ) -> ursula_runtime::SnapshotStoreFuture<'a, ursula_runtime::OwnedLocation> {
            static INLINE: ursula_runtime::InlineSnapshotStore =
                ursula_runtime::InlineSnapshotStore;
            SnapshotStore::write_staged(&INLINE, staged)
        }

        fn list_own<'a>(
            &'a self,
            _raft_group_id: RaftGroupId,
        ) -> ursula_runtime::SnapshotStoreFuture<'a, ursula_runtime::OwnListing> {
            Box::pin(async {
                Err(SnapshotStoreError::backend(
                    ursula_runtime::SnapshotStoreOperation::List,
                    "snapshots/node-1/group-7/",
                    opendal::Error::new(opendal::ErrorKind::Unexpected, "down"),
                ))
            })
        }

        fn delete_own<'a>(
            &'a self,
            object: ursula_runtime::OwnedObject,
        ) -> ursula_runtime::SnapshotStoreFuture<'a, ()> {
            static INLINE: ursula_runtime::InlineSnapshotStore =
                ursula_runtime::InlineSnapshotStore;
            SnapshotStore::delete_own(&INLINE, object)
        }

        fn read_stored<'a>(
            &'a self,
            location: &'a SnapshotLocation,
        ) -> ursula_runtime::SnapshotStoreFuture<'a, ursula_runtime::StoredSnapshot> {
            static INLINE: ursula_runtime::InlineSnapshotStore =
                ursula_runtime::InlineSnapshotStore;
            SnapshotStore::read_stored(&INLINE, location)
        }
    }

    /// Failed passes in a row back the next pass off, and a pass that
    /// succeeds resets the back-off.
    #[tokio::test(start_paused = true)]
    async fn failed_gc_passes_back_off_until_one_succeeds() {
        let failing: SharedSnapshotStore = Arc::new(UnlistableStore);
        let healthy: SharedSnapshotStore = Arc::new(ursula_runtime::InlineSnapshotStore);
        let holds = holds();
        holds.mark_restored();
        // Both passes the restore owes, so that only the back-off is owed.
        holds
            .collect_garbage(&healthy, later(Instant::now(), LATE))
            .await
            .unwrap();
        assert!(!holds.gc_due(later(Instant::now(), LATE.saturating_mul(10))));
        for (failures, delay) in [(1_u32, 30_u64), (2, 60), (3, 120)] {
            let now = Instant::now();
            let error = holds.collect_garbage(&failing, now).await.unwrap_err();
            assert!(matches!(error, SnapshotGcError::List(_)), "{error}");
            assert_eq!(holds.state().gc_failures, failures);
            assert!(!holds.gc_due(later(now, Duration::from_secs(delay.saturating_sub(1)))));
            assert!(holds.gc_due(later(now, Duration::from_secs(delay))));
            tokio::time::advance(Duration::from_secs(delay)).await;
        }
        holds
            .collect_garbage(&healthy, Instant::now())
            .await
            .unwrap();
        assert_eq!(holds.state().gc_failures, 0);
    }
}

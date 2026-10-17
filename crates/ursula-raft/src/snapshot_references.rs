//! External snapshot pins and current-reference publication outside RaftCore.
//!
//! Prepared operations retain a pin until their synchronous pointer transition
//! completes. Serialize reference I/O without holding a lock across a Raft call,
//! so installation cannot deadlock a concurrent snapshot builder.
//!
//! Snapshot GC (#669): the external object a replica replaces stays pinned
//! for the GC grace G (`RaftConfig::snapshot_gc_grace`), which covers a send
//! of its pointer that started before the switch. Once the pin expires, the
//! next reference publication drops it, and the snapshot driver runs a GC
//! pass of the group ([`SnapshotReferences::collect_garbage`]) that prunes
//! every object nothing names any more. A node without a driver prunes after
//! each build instead.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::PoisonError;
use std::time::Duration;

use ursula_runtime::SharedSnapshotStore;
use ursula_runtime::SnapshotLocation;
use ursula_runtime::SnapshotPrune;
use ursula_runtime::SnapshotStoreError;

use crate::rt::sync::Mutex as AsyncMutex;
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
/// The longest G a group uses. A server's G is at most a few hours
/// (`RaftConfig::snapshot_gc_grace`), and this clamp keeps every deadline G
/// after an instant representable.
const MAX_GC_GRACE: Duration = Duration::from_secs(30 * 24 * 60 * 60);

#[derive(Debug)]
pub(crate) struct SnapshotReferences {
    serial: AsyncMutex<PublicationRetry>,
    state: Mutex<ReferenceState>,
    /// G: how long a replaced current object stays pinned, and how long
    /// after an upload or a restore the group's GC pass runs.
    gc_grace: Duration,
}

#[derive(Debug, Default)]
struct PublicationRetry {
    failures: u32,
    not_before: Option<Instant>,
}

#[derive(Debug, Default)]
struct ReferenceState {
    current_known: bool,
    current: Option<SnapshotLocation>,
    active: BTreeMap<String, (SnapshotLocation, usize)>,
    // A terminal metadata failure may have published this candidate before
    // returning an error. Keep it beyond the failed engine's lease lifetime.
    failed_candidate: Option<SnapshotLocation>,
    /// The objects this node replaced, pinned until they expire (#669).
    retiring: Vec<RetiringObject>,
    /// The GC passes the group owes (#669).
    passes: GcPasses,
    /// GC passes of the group that failed in a row, which back the next
    /// one off ([`gc_retry_delay`]).
    gc_failures: u32,
}

/// An external object that was this node's current snapshot until another
/// replaced it (#669). It stays pinned until it expires, then a GC pass
/// deletes it unless another replica names or pins it.
#[derive(Debug, Clone)]
struct RetiringObject {
    location: SnapshotLocation,
    expiry: Expiry,
}

/// When a retiring object's pin expires.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Expiry {
    At(Instant),
    /// The deadline is not representable: the pin never expires, so the
    /// object is never deleted for it (fail closed).
    Never,
}

impl Expiry {
    fn by(self, now: Instant) -> bool {
        match self {
            Self::At(until) => until <= now,
            Self::Never => false,
        }
    }
}

/// The GC passes a group owes (#669): the earliest and the latest only. A
/// pass deletes every object old enough at its time, so the latest pass
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

impl ReferenceState {
    /// Turns the retiring objects whose pin expired by `now` into a GC pass
    /// owed when they expired, so that their pins drop at the next
    /// reconcile, with or without a snapshot driver.
    fn settle(&mut self, now: Instant) {
        let mut expired = Vec::new();
        self.retiring.retain(|object| match object.expiry {
            Expiry::At(until) if until <= now => {
                expired.push(until);
                false
            }
            Expiry::At(_) | Expiry::Never => true,
        });
        for until in expired {
            self.passes.add(until);
        }
    }

    /// The external locations this node pins at `now`.
    fn retained(&mut self, now: Instant) -> Vec<SnapshotLocation> {
        self.settle(now);
        self.current
            .iter()
            .cloned()
            .chain(self.failed_candidate.iter().cloned())
            .chain(self.active.values().map(|(location, _)| location.clone()))
            .chain(self.retiring.iter().map(|object| object.location.clone()))
            .collect()
    }
}

/// A GC pass of one group that failed (#669). The pass runs again later,
/// backed off by [`gc_retry_delay`].
#[derive(Debug, thiserror::Error)]
pub(crate) enum SnapshotGcError {
    /// Publishing this node's current reference, or reconciling its pins,
    /// failed, so an expired pin may still stand.
    #[error("publish the current snapshot reference: {0}")]
    Publish(#[source] SnapshotStoreError),
    /// Listing, reading or deleting the group's objects failed.
    #[error("prune retired snapshot objects: {0}")]
    Prune(#[source] SnapshotStoreError),
}

#[derive(Debug)]
pub(crate) struct SnapshotReferenceLease {
    references: Arc<SnapshotReferences>,
    key: String,
}

impl Drop for SnapshotReferenceLease {
    fn drop(&mut self) {
        let mut state = self
            .references
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if let Some((_, count)) = state.active.get_mut(&self.key) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                state.active.remove(&self.key);
            }
        }
    }
}

#[cfg(test)]
impl Default for SnapshotReferences {
    fn default() -> Self {
        Self::new(crate::state_machine::DEFAULT_SNAPSHOT_GC_GRACE)
    }
}

impl SnapshotReferences {
    /// References whose replaced objects stay pinned for `gc_grace` (G),
    /// at most [`MAX_GC_GRACE`].
    pub(crate) fn new(gc_grace: Duration) -> Self {
        Self {
            serial: AsyncMutex::new(PublicationRetry::default()),
            state: Mutex::new(ReferenceState::default()),
            gc_grace: gc_grace.min(MAX_GC_GRACE),
        }
    }

    pub(crate) async fn prepare(
        self: &Arc<Self>,
        store: &SharedSnapshotStore,
        group: u32,
        location: &SnapshotLocation,
    ) -> Result<Option<SnapshotReferenceLease>, SnapshotStoreError> {
        let SnapshotLocation::S3 { key, .. } = location else {
            return Ok(None);
        };
        let _serial = self.serial.lock().await;
        let mut retained = self
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .retained(Instant::now());
        retained.push(location.clone());
        // Reconcile abandoned pins before creating more. On durable startup,
        // the incoming location is the restored pointer and must be retained.
        store.reconcile_reference_pins(group, &retained).await?;
        store.pin_reference(group, location).await?;
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        let (_, count) = state
            .active
            .entry(key.clone())
            .or_insert((location.clone(), 0));
        *count = count.saturating_add(1);
        Ok(Some(SnapshotReferenceLease {
            references: self.clone(),
            key: key.clone(),
        }))
    }

    /// Metadata publication is terminally latched after this failure, so there
    /// can be only one failed candidate per incarnation. It need not have
    /// reached rename; retaining it conservatively is bounded and safe.
    pub(crate) fn retain_failed_candidate(&self, location: &SnapshotLocation) {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        if matches!(location, SnapshotLocation::S3 { .. }) {
            state.failed_candidate = Some(location.clone());
        }
    }

    /// Call after durable metadata and the current pointer change. A prepared
    /// lease protects the new external pointer during this transition.
    ///
    /// The external object that was current until now retires: it stays
    /// pinned for G, so a send of its pointer that started before the
    /// switch still finds it, and then a GC pass deletes it (#669).
    pub(crate) fn commit_current(&self, location: &SnapshotLocation) {
        let now = Instant::now();
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        state.failed_candidate = None;
        state.current_known = true;
        let next = match location {
            SnapshotLocation::S3 { .. } => Some(location.clone()),
            SnapshotLocation::Inline { .. } | SnapshotLocation::Local { .. } => None,
        };
        // An object that is current again is pinned as current.
        state
            .retiring
            .retain(|object| Some(&object.location) != next.as_ref());
        if let Some(previous) = std::mem::replace(&mut state.current, next)
            && state.current.as_ref() != Some(&previous)
        {
            let expiry = now
                .checked_add(self.gc_grace)
                .map_or(Expiry::Never, Expiry::At);
            state.retiring.push(RetiringObject {
                location: previous,
                expiry,
            });
        }
        state.settle(now);
    }

    /// Owes a GC pass of the group at `at`.
    pub(crate) fn schedule_pass(&self, at: Instant) {
        self.state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .passes
            .add(at);
    }

    /// Owes a GC pass `delay` from now. A deadline that is not
    /// representable owes none, which deletes nothing.
    fn schedule_pass_in(&self, delay: Duration) {
        if let Some(at) = Instant::now().checked_add(delay) {
            self.schedule_pass(at);
        }
    }

    /// An external upload owes a GC pass G later (#669). If the upload never
    /// becomes current, as when a newer install overtook its build or its
    /// pin failed and the build fell back inline, no retirement covers it,
    /// and that pass deletes it once it is G old.
    pub(crate) fn schedule_pass_after_upload(&self) {
        self.schedule_pass_in(self.gc_grace);
    }

    /// A restore owes a GC pass about G later. Its reconcile dropped the
    /// pins of the objects that were retiring before the restart: the
    /// restart ended this process's sends of them, so they need no pin, and
    /// any node's pass may delete them at once. This pass only makes sure
    /// that one does. The passes of a node's groups spread over a further
    /// quarter of G by group, so that a restart does not bring every
    /// group's pass due at once.
    pub(crate) fn schedule_pass_after_restore(&self, group: u32) {
        let spread = self
            .gc_grace
            .checked_div(64)
            .unwrap_or_default()
            .saturating_mul(group % 16);
        self.schedule_pass_in(self.gc_grace.saturating_add(spread));
    }

    /// Whether a pin expired or a GC pass is owed by `now`.
    pub(crate) fn gc_due(&self, now: Instant) -> bool {
        let state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        state.retiring.iter().any(|object| object.expiry.by(now))
            || state.passes.earliest().is_some_and(|at| at <= now)
    }

    /// Drops the retirements due by `now`: the pins that expired, which the
    /// next reference publication removes from the store, and the passes
    /// owed. Returns whether any was due.
    pub(crate) fn expire_retiring(&self, now: Instant) -> bool {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        state.settle(now);
        state.passes.take_due(now)
    }

    /// This node's current snapshot location, inline when it has none in
    /// the store.
    fn current_location(&self) -> SnapshotLocation {
        self.state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .current
            .clone()
            .unwrap_or(SnapshotLocation::Inline { bytes: Vec::new() })
    }

    /// Deletes the group's objects that nothing names any more
    /// ([`ursula_runtime::SnapshotStore::prune_retired`]).
    pub(crate) async fn prune(
        &self,
        store: &SharedSnapshotStore,
        group: u32,
    ) -> Result<SnapshotPrune, SnapshotGcError> {
        store
            .prune_retired(group, &self.current_location())
            .await
            .map_err(SnapshotGcError::Prune)
    }

    /// One GC pass of the group at `now` (#669): drops the retirements due,
    /// publishes the current reference, whose reconcile removes the expired
    /// pins, and prunes. A failed pass runs again later, backed off
    /// exponentially ([`gc_retry_delay`]) until a pass succeeds.
    pub(crate) async fn collect_garbage(
        &self,
        store: &SharedSnapshotStore,
        group: u32,
        now: Instant,
    ) -> Result<SnapshotPrune, SnapshotGcError> {
        self.expire_retiring(now);
        let pass = match self.publish_current(store, group).await {
            Ok(()) => self.prune(store, group).await,
            Err(error) => Err(SnapshotGcError::Publish(error)),
        };
        let failures = {
            let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
            state.gc_failures = if pass.is_err() {
                state.gc_failures.saturating_add(1)
            } else {
                0
            };
            state.gc_failures
        };
        if failures > 0 {
            self.schedule_pass_in(gc_retry_delay(failures));
        }
        pass
    }

    pub(crate) async fn publish_current(
        &self,
        store: &SharedSnapshotStore,
        group: u32,
    ) -> Result<(), SnapshotStoreError> {
        let mut retry = self.serial.lock().await;
        if let Some(not_before) = retry.not_before {
            crate::rt::time::sleep_until(not_before).await;
        }
        let (known, current) = {
            let state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
            (state.current_known, state.current.clone())
        };
        // Keep the current pin even after a successful PUT. A prepared pointer
        // may become current during that PUT; its pin protects a lagging primary.
        let publication = if known {
            store
                .publish_reference(
                    group,
                    &current.unwrap_or(SnapshotLocation::Inline { bytes: Vec::new() }),
                )
                .await
        } else {
            Ok(())
        };
        // An expired pin drops here, also on a node that runs no snapshot
        // driver.
        let retained = self
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .retained(Instant::now());
        let cleanup = store.reconcile_reference_pins(group, &retained).await;
        let result = publication.and(cleanup);
        if result.is_err() {
            retry.failures = retry.failures.saturating_add(1);
            let delay = Duration::from_millis(
                100_u64
                    .saturating_mul(2_u64.saturating_pow(retry.failures.saturating_sub(1).min(6))),
            )
            .min(Duration::from_secs(5));
            retry.not_before = Instant::now().checked_add(delay);
        } else {
            *retry = PublicationRetry::default();
        }
        result
    }
}

#[cfg(test)]
#[cfg(not(madsim))]
mod tests {
    use std::collections::BTreeMap;
    use std::collections::BTreeSet;
    use std::sync::Arc;
    use std::time::Duration;

    use bytes::Bytes;
    use ursula_runtime::S3SnapshotStore;
    use ursula_runtime::SharedSnapshotStore;
    use ursula_runtime::SnapshotKey;
    use ursula_runtime::SnapshotLocation;
    use ursula_runtime::SnapshotPrune;
    use ursula_runtime::SnapshotReferenceConfig;
    use ursula_runtime::SnapshotStore;
    use ursula_runtime::SnapshotStoreError;
    use ursula_runtime::SnapshotStoreFuture;

    use super::SNAPSHOT_GC_MAX_RETRY;
    use super::SNAPSHOT_GC_RETRY;
    use super::SnapshotGcError;
    use super::SnapshotReferences;
    use super::gc_retry_delay;
    use crate::rt::time::Instant;

    const GRACE: Duration = Duration::from_secs(60);

    fn s3(key: &str) -> SnapshotLocation {
        SnapshotLocation::S3 {
            key: format!("snapshots/group-7/objects/{key}.snap"),
            size_bytes: 1,
            stored_size_bytes: 1,
            compression: ursula_runtime::SnapshotCompression::Zstd,
            shared_object: true,
        }
    }

    fn later(now: Instant, by: Duration) -> Instant {
        now.checked_add(by).unwrap()
    }

    fn retained(references: &SnapshotReferences) -> Vec<SnapshotLocation> {
        references.state.lock().unwrap().retained(Instant::now())
    }

    /// #669: the object a new current replaces stays retained, so its pin
    /// stays, until G passed and its retirement expired.
    #[tokio::test]
    async fn a_replaced_current_retires_for_the_grace() {
        let references = SnapshotReferences::new(GRACE);
        let start = Instant::now();
        references.commit_current(&s3("old"));
        assert!(!references.gc_due(later(start, GRACE.saturating_mul(2))));
        references.commit_current(&s3("new"));
        assert_eq!(retained(&references), vec![s3("new"), s3("old")]);
        let now = Instant::now();
        assert!(!references.gc_due(now));
        assert!(!references.expire_retiring(now));
        assert_eq!(retained(&references), vec![s3("new"), s3("old")]);
        let expired = later(now, GRACE);
        assert!(references.gc_due(expired));
        assert!(references.expire_retiring(expired));
        assert!(!references.gc_due(expired));
        assert_eq!(retained(&references), vec![s3("new")]);
    }

    /// An inline current retires the object it replaces too, the same
    /// object committed again does not retire, and an inline snapshot has
    /// nothing to retire.
    #[tokio::test]
    async fn only_a_replaced_external_object_retires() {
        let references = SnapshotReferences::new(GRACE);
        references.commit_current(&SnapshotLocation::Inline { bytes: vec![1] });
        references.commit_current(&SnapshotLocation::Inline { bytes: vec![2] });
        assert!(retained(&references).is_empty());
        references.commit_current(&s3("a"));
        references.commit_current(&s3("a"));
        assert_eq!(retained(&references), vec![s3("a")]);
        references.commit_current(&SnapshotLocation::Inline { bytes: vec![3] });
        assert_eq!(retained(&references), vec![s3("a")]);
        // An object current again is no longer retiring.
        references.commit_current(&s3("a"));
        assert_eq!(retained(&references), vec![s3("a")]);
        assert!(!references.expire_retiring(later(Instant::now(), GRACE)));
    }

    /// A restore owes one GC pass G later, and an owed pass pins nothing.
    #[tokio::test]
    async fn a_restore_owes_a_pass_after_the_grace() {
        let references = SnapshotReferences::new(GRACE);
        let start = Instant::now();
        references.commit_current(&s3("restored"));
        references.schedule_pass_after_restore(0);
        assert_eq!(retained(&references), vec![s3("restored")]);
        assert!(!references.gc_due(start));
        let due = later(Instant::now(), GRACE);
        assert!(references.gc_due(due));
        // A pass owed no later is not owed twice.
        references.schedule_pass(due);
        assert!(references.expire_retiring(due));
        assert!(!references.gc_due(due));
    }

    fn store() -> SharedSnapshotStore {
        Arc::new(
            S3SnapshotStore::memory_for_tests("snapshots")
                .unwrap()
                .with_references(SnapshotReferenceConfig {
                    node_id: 1,
                    default_voters: BTreeSet::from([1]),
                    per_group_voters: BTreeMap::new(),
                })
                .with_gc_grace(Duration::ZERO),
        )
    }

    async fn upload(store: &SharedSnapshotStore, body: &'static [u8]) -> SnapshotLocation {
        store
            .upload(
                SnapshotKey {
                    raft_group_id: 7,
                    snapshot_id: "s".to_owned(),
                },
                Bytes::from_static(body),
            )
            .await
            .unwrap()
    }

    async fn switch_to(
        references: &Arc<SnapshotReferences>,
        store: &SharedSnapshotStore,
        location: &SnapshotLocation,
    ) {
        let lease = references.prepare(store, 7, location).await.unwrap();
        references.commit_current(location);
        drop(lease);
        references.publish_current(store, 7).await.unwrap();
    }

    /// #669: a GC pass spares the replaced object while it retires and
    /// deletes it once its pin expired, keeping the current one.
    #[tokio::test]
    async fn a_gc_pass_deletes_the_replaced_object_once_its_pin_expired() {
        let store = store();
        let references = Arc::new(SnapshotReferences::new(GRACE));
        let old = upload(&store, b"old").await;
        let new = upload(&store, b"new").await;
        switch_to(&references, &store, &old).await;
        switch_to(&references, &store, &new).await;

        let now = Instant::now();
        assert_eq!(
            references.collect_garbage(&store, 7, now).await.unwrap(),
            SnapshotPrune::Pruned { deleted: 0 }
        );
        assert_eq!(store.download(&old).await.unwrap(), b"old");
        assert_eq!(
            references
                .collect_garbage(&store, 7, later(now, GRACE))
                .await
                .unwrap(),
            SnapshotPrune::Pruned { deleted: 1 }
        );
        assert!(matches!(
            store.download(&old).await,
            Err(SnapshotStoreError::NotFound(_))
        ));
        assert_eq!(store.download(&new).await.unwrap(), b"new");
        assert!(!references.gc_due(later(now, GRACE.saturating_mul(10))));
    }

    /// An upload owes a pass G later, so an upload that never becomes
    /// current is deleted too.
    #[tokio::test]
    async fn an_upload_owes_a_pass_after_the_grace() {
        let references = SnapshotReferences::new(GRACE);
        let start = Instant::now();
        references.schedule_pass_after_upload();
        assert!(!references.gc_due(start));
        assert!(references.gc_due(later(Instant::now(), GRACE)));
    }

    /// The owed passes stay two, the earliest and the latest, however many
    /// are owed, and the latest stays owed once the earliest ran.
    #[tokio::test]
    async fn owed_passes_stay_bounded() {
        let references = SnapshotReferences::new(GRACE);
        let start = Instant::now();
        for seconds in [30, 10, 50, 20, 40] {
            references.schedule_pass(later(start, Duration::from_secs(seconds)));
        }
        assert_eq!(
            references.state.lock().unwrap().passes,
            super::GcPasses::Two {
                earliest: later(start, Duration::from_secs(10)),
                latest: later(start, Duration::from_secs(50)),
            }
        );
        assert!(references.expire_retiring(later(start, Duration::from_secs(10))));
        assert!(!references.gc_due(later(start, Duration::from_secs(49))));
        assert!(references.gc_due(later(start, Duration::from_secs(50))));
    }

    /// B1 of the review: without a snapshot driver, an expired pin drops at
    /// the next reference publication, so a build's prune deletes the object
    /// it pinned, and the retiring list stays bounded.
    #[tokio::test(start_paused = true)]
    async fn an_expired_pin_drops_without_a_driver() {
        let store = store();
        let references = Arc::new(SnapshotReferences::new(GRACE));
        let old = upload(&store, b"old").await;
        let new = upload(&store, b"new").await;
        switch_to(&references, &store, &old).await;
        switch_to(&references, &store, &new).await;
        assert_eq!(
            references.prune(&store, 7).await.unwrap(),
            SnapshotPrune::Pruned { deleted: 0 }
        );
        assert_eq!(store.download(&old).await.unwrap(), b"old");
        tokio::time::advance(GRACE.saturating_add(Duration::from_secs(1))).await;
        let newer = upload(&store, b"newer").await;
        // A build: a new current and its prune, with no GC pass in between.
        switch_to(&references, &store, &newer).await;
        assert_eq!(
            references.prune(&store, 7).await.unwrap(),
            SnapshotPrune::Pruned { deleted: 1 }
        );
        assert!(matches!(
            store.download(&old).await,
            Err(SnapshotStoreError::NotFound(_))
        ));
        assert_eq!(store.download(&new).await.unwrap(), b"new");
        assert_eq!(store.download(&newer).await.unwrap(), b"newer");
        assert_eq!(references.state.lock().unwrap().retiring.len(), 1);
    }

    /// A store whose reference publication fails.
    #[derive(Debug)]
    struct UnpublishableStore;

    impl SnapshotStore for UnpublishableStore {
        fn upload<'a>(
            &'a self,
            _key: SnapshotKey,
            _bytes: Bytes,
        ) -> SnapshotStoreFuture<'a, SnapshotLocation> {
            Box::pin(async { Err(SnapshotStoreError::Backend("down".to_owned())) })
        }

        fn download<'a>(
            &'a self,
            _location: &'a SnapshotLocation,
        ) -> SnapshotStoreFuture<'a, Vec<u8>> {
            Box::pin(async { Err(SnapshotStoreError::Backend("down".to_owned())) })
        }

        fn delete<'a>(&'a self, _location: &'a SnapshotLocation) -> SnapshotStoreFuture<'a, ()> {
            Box::pin(async { Ok(()) })
        }

        fn publish_reference<'a>(
            &'a self,
            _raft_group_id: u32,
            _location: &'a SnapshotLocation,
        ) -> SnapshotStoreFuture<'a, ()> {
            Box::pin(async { Err(SnapshotStoreError::Backend("down".to_owned())) })
        }
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

    /// Failed passes in a row back the next pass off, and a pass that
    /// succeeds resets the back-off.
    #[tokio::test(start_paused = true)]
    async fn failed_gc_passes_back_off_until_one_succeeds() {
        let failing: SharedSnapshotStore = Arc::new(UnpublishableStore);
        let references = SnapshotReferences::new(GRACE);
        for (failures, delay) in [(1_u32, 30_u64), (2, 60), (3, 120)] {
            let now = Instant::now();
            references
                .collect_garbage(&failing, 7, now)
                .await
                .unwrap_err();
            assert_eq!(references.state.lock().unwrap().gc_failures, failures);
            assert!(!references.gc_due(later(now, Duration::from_secs(delay.saturating_sub(1)))));
            assert!(references.gc_due(later(now, Duration::from_secs(delay))));
            tokio::time::advance(Duration::from_secs(delay)).await;
        }
        // The default store publishes and prunes nothing, and succeeds.
        let healthy: SharedSnapshotStore = Arc::new(ursula_runtime::InlineSnapshotStore);
        // Past the publication back-off the failures left.
        tokio::time::advance(Duration::from_secs(10)).await;
        references
            .collect_garbage(&healthy, 7, Instant::now())
            .await
            .unwrap();
        assert_eq!(references.state.lock().unwrap().gc_failures, 0);
        assert!(!references.gc_due(later(Instant::now(), SNAPSHOT_GC_MAX_RETRY)));
    }

    /// A failed pass keeps the pin it could not drop and runs again later.
    #[tokio::test]
    async fn a_failed_gc_pass_runs_again_later() {
        let store: SharedSnapshotStore = Arc::new(UnpublishableStore);
        let references = SnapshotReferences::new(GRACE);
        references.commit_current(&s3("old"));
        references.commit_current(&s3("new"));
        let expired = later(Instant::now(), GRACE);
        let error = references
            .collect_garbage(&store, 7, expired)
            .await
            .unwrap_err();
        assert!(matches!(error, SnapshotGcError::Publish(_)), "{error}");
        assert!(!references.gc_due(Instant::now()));
        assert!(references.gc_due(later(Instant::now(), SNAPSHOT_GC_RETRY)));
    }
}

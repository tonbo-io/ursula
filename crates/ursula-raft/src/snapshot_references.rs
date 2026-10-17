//! External snapshot pins and current-reference publication outside RaftCore.
//!
//! Prepared operations retain a pin until their synchronous pointer transition
//! completes. Serialize reference I/O without holding a lock across a Raft call,
//! so installation cannot deadlock a concurrent snapshot builder.
//!
//! Snapshot GC (#669): the external object a replica replaces stays pinned
//! for the GC grace G (`min(raft.deleted_data_max_age / 4, 1h)`), which covers
//! a send of its pointer that started before the switch. Once the pin
//! expires, the snapshot driver runs a GC pass of the group
//! ([`SnapshotReferences::collect_garbage`]): the reference publication drops
//! the pin, and the store prunes every object nothing names any more.

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

/// How long after a failed GC pass of a group the next one runs.
const SNAPSHOT_GC_RETRY: Duration = Duration::from_secs(30);

#[derive(Debug)]
pub(crate) struct SnapshotReferences {
    serial: AsyncMutex<PublicationRetry>,
    state: Mutex<ReferenceState>,
    /// G: how long a replaced current object stays pinned, and how long
    /// after a restore the group's GC pass runs.
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
    /// The objects this node replaced, pinned until they expire, and the GC
    /// passes the group owes.
    retiring: Vec<Retirement>,
}

/// What the group's snapshot GC still owes (#669).
#[derive(Debug, Clone, PartialEq, Eq)]
enum Retirement {
    /// The external object that was this node's current snapshot until
    /// another replaced it. It stays pinned until `until`, then a GC pass
    /// drops the pin and deletes it unless another replica names it.
    Object {
        location: SnapshotLocation,
        until: Instant,
    },
    /// A GC pass owed at `at` that pins nothing: G after a restore, whose
    /// reconcile dropped the pins of objects retiring before the restart,
    /// right after an install, or the retry of a failed pass.
    Pass { at: Instant },
}

impl Retirement {
    fn due(&self) -> Instant {
        match self {
            Self::Object { until, .. } => *until,
            Self::Pass { at } => *at,
        }
    }
}

impl ReferenceState {
    fn retained(&self) -> Vec<SnapshotLocation> {
        self.current
            .iter()
            .cloned()
            .chain(self.failed_candidate.iter().cloned())
            .chain(self.active.values().map(|(location, _)| location.clone()))
            .chain(
                self.retiring
                    .iter()
                    .filter_map(|retirement| match retirement {
                        Retirement::Object { location, .. } => Some(location.clone()),
                        Retirement::Pass { .. } => None,
                    }),
            )
            .collect()
    }
}

/// A GC pass of one group that failed (#669). The pass runs again
/// [`SNAPSHOT_GC_RETRY`] later.
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
    /// References whose replaced objects stay pinned for `gc_grace` (G).
    pub(crate) fn new(gc_grace: Duration) -> Self {
        Self {
            serial: AsyncMutex::new(PublicationRetry::default()),
            state: Mutex::new(ReferenceState::default()),
            gc_grace,
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
            .retained();
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
        state.retiring.retain(|retirement| {
            !matches!(retirement, Retirement::Object { location, .. } if Some(location) == next.as_ref())
        });
        if let Some(previous) = std::mem::replace(&mut state.current, next)
            && state.current.as_ref() != Some(&previous)
        {
            // G is at most an hour (`RaftConfig::snapshot_gc_grace`), so the
            // deadline does not overflow.
            let until = now.checked_add(self.gc_grace).unwrap_or(now);
            state.retiring.push(Retirement::Object {
                location: previous,
                until,
            });
        }
    }

    /// Owes a GC pass of the group at `at`, unless one is owed by then.
    pub(crate) fn schedule_pass(&self, at: Instant) {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        if !state
            .retiring
            .iter()
            .any(|retirement| matches!(retirement, Retirement::Pass { at: owed } if *owed <= at))
        {
            state.retiring.push(Retirement::Pass { at });
        }
    }

    /// A restore owes a GC pass G later: its reconcile dropped the pins of
    /// the objects that were retiring before the restart, and once they are
    /// G old the pass deletes them.
    pub(crate) fn schedule_pass_after_restore(&self) {
        let now = Instant::now();
        self.schedule_pass(now.checked_add(self.gc_grace).unwrap_or(now));
    }

    /// Whether a pin expired or a GC pass is owed by `now`.
    pub(crate) fn gc_due(&self, now: Instant) -> bool {
        self.state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .retiring
            .iter()
            .any(|retirement| retirement.due() <= now)
    }

    /// Drops the retirements due by `now`: the pins that expired, which the
    /// next reference publication removes from the store, and the passes
    /// owed. Returns whether any was due.
    pub(crate) fn expire_retiring(&self, now: Instant) -> bool {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        let owed = state.retiring.len();
        state.retiring.retain(|retirement| retirement.due() > now);
        state.retiring.len() < owed
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
    /// pins, and prunes. A failed pass runs again [`SNAPSHOT_GC_RETRY`]
    /// later.
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
        if pass.is_err() {
            self.schedule_pass(now.checked_add(SNAPSHOT_GC_RETRY).unwrap_or(now));
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
        let retained = self
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .retained();
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

#[cfg(all(test, not(madsim)))]
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

    use super::SNAPSHOT_GC_RETRY;
    use super::SnapshotGcError;
    use super::SnapshotReferences;
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
        references.state.lock().unwrap().retained()
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
        references.schedule_pass_after_restore();
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
        assert!(!references.gc_due(expired));
        assert!(references.gc_due(later(expired, SNAPSHOT_GC_RETRY)));
    }
}

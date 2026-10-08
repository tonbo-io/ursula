use std::collections::BTreeMap;
use std::fmt::Debug;
use std::io;
use std::io::Cursor;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use futures_util::Stream;
use futures_util::TryStreamExt;
use openraft::EntryPayload;
use openraft::alias::LogIdOf;
use openraft::alias::SnapshotDataOf;
use openraft::alias::SnapshotMetaOf;
use openraft::alias::SnapshotOf;
use openraft::alias::StoredMembershipOf;
use openraft::storage::EntryResponder;
use openraft::storage::RaftSnapshotBuilder;
use openraft::storage::RaftStateMachine;
use serde::Deserialize;
use serde::Serialize;
use ursula_runtime::AppendRequest;
use ursula_runtime::BootstrapStreamRequest;
use ursula_runtime::BootstrapStreamResponse;
use ursula_runtime::ColdFlushCandidate;
use ursula_runtime::ColdGcPlanEntry;
use ursula_runtime::ColdHotBacklog;
use ursula_runtime::ColdStoreHandle;
use ursula_runtime::ColdWriteAdmission;
use ursula_runtime::CreateStreamRequest;
use ursula_runtime::GroupEngine;
use ursula_runtime::GroupEngineError;
use ursula_runtime::GroupEngineMetrics;
use ursula_runtime::GroupSnapshot;
use ursula_runtime::HeadStreamRequest;
use ursula_runtime::HeadStreamResponse;
use ursula_runtime::InMemoryGroupEngine;
use ursula_runtime::PlanColdFlushRequest;
use ursula_runtime::PlanGroupColdFlushRequest;
use ursula_runtime::ReadSnapshotRequest;
use ursula_runtime::ReadSnapshotResponse;
use ursula_runtime::ReadStreamRequest;
use ursula_runtime::ReadStreamResponse;
use ursula_runtime::SharedSnapshotStore;
use ursula_runtime::SnapshotKey;
use ursula_runtime::SnapshotLocation;
use ursula_runtime::SnapshotPointer;
use ursula_runtime::decode_snapshot_envelope;
use ursula_runtime::default_snapshot_store;
use ursula_runtime::encode_binary_envelope;
use ursula_shard::BucketStreamId;
use ursula_shard::ShardPlacement;

use crate::engine::group_engine_io_error;
use crate::engine::invalid_data;
use crate::log_store::elapsed_ns;
use crate::rt::sync::OwnedSemaphorePermit;
use crate::rt::sync::Semaphore;
use crate::rt::time::Instant;
use crate::snapshot_cadence::GroupLogGauge;
use crate::snapshot_cadence::GroupLogMark;
use crate::snapshot_cadence::GroupLogProgress;
use crate::snapshot_codec::decode_group_snapshot;
use crate::snapshot_codec::group_snapshot_frames;
use crate::snapshot_references::SnapshotReferenceLease;
use crate::snapshot_references::SnapshotReferences;
use crate::types::RaftGroupResponse;
use crate::types::UrsulaRaftTypeConfig;

#[derive(Debug, Clone)]
pub struct SnapshotBuildCoordinator {
    inner: Arc<SnapshotBuildCoordinatorInner>,
}

#[derive(Debug)]
struct SnapshotBuildCoordinatorInner {
    semaphore: Arc<Semaphore>,
    /// Per-group log gauges (F12e), shared node-wide like the permit, so
    /// the snapshot driver reads what every group's state machine counts.
    log_gauges: Arc<Mutex<BTreeMap<u32, Arc<GroupLogGauge>>>>,
    /// Build permits the snapshot driver took for a group it is about to
    /// trigger; that group's next build uses it instead of competing for one.
    handoffs: Mutex<BTreeMap<u32, OwnedSemaphorePermit>>,
    /// Set while the node's unsnapshotted Raft log is over its hard limit;
    /// client writes are refused with 503 until snapshots bring it back.
    log_pressure: Arc<AtomicBool>,
}

impl Default for SnapshotBuildCoordinator {
    fn default() -> Self {
        Self::new(1)
    }
}

impl SnapshotBuildCoordinator {
    pub fn new(max_concurrency: usize) -> Self {
        Self {
            inner: Arc::new(SnapshotBuildCoordinatorInner {
                semaphore: Arc::new(Semaphore::new(max_concurrency.max(1))),
                log_gauges: Arc::default(),
                handoffs: Mutex::default(),
                log_pressure: Arc::default(),
            }),
        }
    }

    /// A coordinator with a new build concurrency that keeps this one's log
    /// gauges.
    pub fn with_max_concurrency(&self, max_concurrency: usize) -> Self {
        Self {
            inner: Arc::new(SnapshotBuildCoordinatorInner {
                semaphore: Arc::new(Semaphore::new(max_concurrency.max(1))),
                log_gauges: Arc::clone(&self.inner.log_gauges),
                handoffs: Mutex::default(),
                log_pressure: Arc::clone(&self.inner.log_pressure),
            }),
        }
    }

    /// The log gauge of `raft_group_id`, created on first use (F12e).
    pub fn log_gauge(&self, raft_group_id: u32) -> Arc<GroupLogGauge> {
        let mut gauges = self
            .inner
            .log_gauges
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        Arc::clone(gauges.entry(raft_group_id).or_default())
    }

    /// The log progress of every group with a gauge (F12e).
    pub fn log_progress(&self) -> BTreeMap<u32, GroupLogProgress> {
        self.inner
            .log_gauges
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .map(|(group, gauge)| (*group, gauge.progress()))
            .collect()
    }

    pub async fn acquire(&self) -> Result<OwnedSemaphorePermit, GroupEngineError> {
        self.inner
            .semaphore
            .clone()
            .acquire_owned()
            .await
            .map_err(|err| GroupEngineError::new(format!("snapshot build gate closed: {err}")))
    }

    /// Takes a build permit only if one is free right now. The policy path
    /// uses this so that one group's apply worker never waits behind another
    /// group's snapshot build (bounded-stream-state F12d).
    pub fn try_acquire(&self) -> Option<OwnedSemaphorePermit> {
        self.inner.semaphore.clone().try_acquire_owned().ok()
    }

    /// Reserves `permit` for the next snapshot build of `raft_group_id`.
    ///
    /// The snapshot driver acquires a permit (waiting for the previous build
    /// to finish) and hands it to the group it triggers. A triggered build
    /// otherwise only *tries* for a permit and is refused while another group
    /// builds, so a driver firing many groups at once built about one per
    /// tick and the Raft log outgrew memory under sustained writes.
    pub fn hand_off(&self, raft_group_id: u32, permit: OwnedSemaphorePermit) {
        self.handoffs().insert(raft_group_id, permit);
    }

    /// Whether `raft_group_id`'s handed-off permit is still unclaimed.
    pub fn handoff_pending(&self, raft_group_id: u32) -> bool {
        self.handoffs().contains_key(&raft_group_id)
    }

    /// Takes back an unclaimed handed-off permit (the trigger was dropped);
    /// returns whether there was one.
    pub fn reclaim_handoff(&self, raft_group_id: u32) -> bool {
        self.handoffs().remove(&raft_group_id).is_some()
    }

    fn take_handoff(&self, raft_group_id: u32) -> Option<OwnedSemaphorePermit> {
        self.handoffs().remove(&raft_group_id)
    }

    fn handoffs(&self) -> std::sync::MutexGuard<'_, BTreeMap<u32, OwnedSemaphorePermit>> {
        self.inner
            .handoffs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Whether the node's unsnapshotted Raft log is over its hard limit.
    pub fn log_pressured(&self) -> bool {
        self.inner.log_pressure.load(Ordering::Acquire)
    }

    /// Updates the log-pressure flag from the node's unsnapshotted log
    /// bytes, with hysteresis: it sets above `limit_bytes` and clears below
    /// `resume_bytes`. Returns the new state when it changed.
    pub fn observe_log_bytes(
        &self,
        log_bytes: u64,
        limit_bytes: u64,
        resume_bytes: u64,
    ) -> Option<bool> {
        let pressured = self.log_pressured();
        let next = if pressured {
            log_bytes >= resume_bytes
        } else {
            log_bytes > limit_bytes
        };
        (next != pressured).then(|| {
            self.inner.log_pressure.store(next, Ordering::Release);
            next
        })
    }

    #[cfg(test)]
    fn available_permits(&self) -> usize {
        self.inner.semaphore.available_permits()
    }
}

#[derive(Debug, Clone)]
pub struct SnapshotInstallCoordinator {
    inner: Arc<SnapshotInstallCoordinatorInner>,
}

#[derive(Debug, Clone, Copy, Default)]
struct InstallActivity {
    closed: bool,
    active: usize,
}

/// Owned by one engine incarnation, including canceled RPCs' admitted work.
#[derive(Debug)]
pub(crate) struct SnapshotInstallLifecycle {
    activity: crate::rt::sync::watch::Sender<InstallActivity>,
}

impl Default for SnapshotInstallLifecycle {
    fn default() -> Self {
        Self {
            activity: crate::rt::sync::watch::channel(InstallActivity::default()).0,
        }
    }
}

impl SnapshotInstallLifecycle {
    pub(crate) fn admit(self: &Arc<Self>) -> Option<SnapshotInstallLease> {
        let mut admitted = false;
        self.activity.send_if_modified(|state| {
            if state.closed {
                return false;
            }
            state.active = state.active.saturating_add(1);
            admitted = true;
            true
        });
        admitted.then(|| SnapshotInstallLease(self.clone()))
    }

    pub(crate) fn close(&self) {
        self.activity.send_modify(|state| state.closed = true);
    }

    pub(crate) async fn drain(&self) {
        let mut activity = self.activity.subscribe();
        while activity.borrow_and_update().active != 0 {
            if activity.changed().await.is_err() {
                return;
            }
        }
    }
}

pub(crate) struct SnapshotInstallLease(Arc<SnapshotInstallLifecycle>);

impl Drop for SnapshotInstallLease {
    fn drop(&mut self) {
        self.0
            .activity
            .send_modify(|state| state.active = state.active.saturating_sub(1));
    }
}

#[derive(Debug)]
struct SnapshotInstallCoordinatorInner {
    semaphore: Arc<Semaphore>,
    next_install: AtomicU64,
    /// Snapshots downloaded and decoded before OpenRaft's install, keyed by
    /// pointer. Install consumes the decoded group, so it decodes once.
    prefetched: Mutex<BTreeMap<String, PrefetchedGroupSnapshot>>,
    references: Mutex<BTreeMap<u32, Arc<SnapshotReferences>>>,
    installs: Mutex<BTreeMap<u32, Arc<crate::rt::sync::Mutex<()>>>>,
}

#[derive(Debug)]
pub(crate) struct PrefetchedGroupSnapshot {
    pub(crate) snapshot: GroupSnapshot,
    reference: Option<SnapshotReferenceLease>,
    original_snapshot_id: String,
}

impl Default for SnapshotInstallCoordinator {
    fn default() -> Self {
        Self::new(1)
    }
}

impl SnapshotInstallCoordinator {
    pub fn new(max_concurrency: usize) -> Self {
        Self {
            inner: Arc::new(SnapshotInstallCoordinatorInner {
                semaphore: Arc::new(Semaphore::new(max_concurrency.max(1))),
                next_install: AtomicU64::new(0),
                prefetched: Mutex::new(BTreeMap::new()),
                references: Mutex::new(BTreeMap::new()),
                installs: Mutex::new(BTreeMap::new()),
            }),
        }
    }

    pub async fn acquire(&self) -> Result<OwnedSemaphorePermit, GroupEngineError> {
        self.inner
            .semaphore
            .clone()
            .acquire_owned()
            .await
            .map_err(|err| GroupEngineError::new(format!("snapshot install gate closed: {err}")))
    }

    pub(crate) fn install_lock(&self, group: u32) -> Arc<crate::rt::sync::Mutex<()>> {
        self.inner
            .installs
            .lock()
            .expect("snapshot install mutex")
            .entry(group)
            .or_default()
            .clone()
    }

    pub fn cache_key(snapshot_id: &str, location: &SnapshotLocation) -> String {
        match location {
            SnapshotLocation::Inline { bytes } => {
                format!("{snapshot_id}:inline:{}", bytes.len())
            }
            SnapshotLocation::Local { path, size_bytes } => {
                format!("{snapshot_id}:local:{}:{size_bytes}", path.display())
            }
            SnapshotLocation::S3 {
                key,
                size_bytes,
                stored_size_bytes,
                compression,
                ..
            } => {
                format!("{snapshot_id}:s3:{key}:{size_bytes}:{stored_size_bytes}:{compression:?}")
            }
        }
    }

    pub(crate) fn references(&self, group: u32) -> Arc<SnapshotReferences> {
        self.inner
            .references
            .lock()
            .expect("snapshot references mutex")
            .entry(group)
            .or_default()
            .clone()
    }

    pub(crate) fn cache_prefetched(
        &self,
        pointer: &mut SnapshotPointer,
        snapshot: GroupSnapshot,
        reference: Option<SnapshotReferenceLease>,
    ) -> String {
        let install = self.inner.next_install.fetch_add(1, Ordering::Relaxed);
        let original_snapshot_id = pointer.snapshot_id.clone();
        // This token exists only on the in-process handoff. Installation
        // restores the original pointer before persisting or advertising it.
        pointer.snapshot_id = format!("install-{install}:{}", pointer.snapshot_id);
        let key = Self::cache_key(&pointer.snapshot_id, &pointer.location);
        self.inner
            .prefetched
            .lock()
            .expect("snapshot install prefetch cache mutex")
            .insert(key.clone(), PrefetchedGroupSnapshot {
                snapshot,
                reference,
                original_snapshot_id,
            });
        key
    }

    pub(crate) fn take_prefetched(
        &self,
        pointer: &SnapshotPointer,
    ) -> Option<PrefetchedGroupSnapshot> {
        let key = Self::cache_key(&pointer.snapshot_id, &pointer.location);
        self.clear_prefetched_key(&key)
    }

    pub(crate) fn clear_prefetched_key(&self, key: &str) -> Option<PrefetchedGroupSnapshot> {
        self.inner
            .prefetched
            .lock()
            .expect("snapshot install prefetch cache mutex")
            .remove(key)
    }

    #[cfg(test)]
    fn available_permits(&self) -> usize {
        self.inner.semaphore.available_permits()
    }
}

#[derive(Debug, Clone)]
pub(crate) struct CurrentSnapshot {
    pub(crate) meta: SnapshotMetaOf<UrsulaRaftTypeConfig>,
    /// Bytes that ride through openraft's `SnapshotData`. With the default
    /// [`ursula_runtime::InlineSnapshotStore`] this is the full snapshot; with
    /// out-of-line backends (Local/S3) this is a tiny [`SnapshotPointer`].
    pointer_bytes: Vec<u8>,
}

const RETAINED_RETIRED_EXTERNAL_SNAPSHOTS: usize = 1;

pub struct RaftGroupStateMachine {
    pub(crate) placement: ShardPlacement,
    pub(crate) engine: InMemoryGroupEngine,
    pub(crate) metrics: Option<GroupEngineMetrics>,
    pub(crate) last_applied_log_id: Option<LogIdOf<UrsulaRaftTypeConfig>>,
    pub(crate) last_membership: StoredMembershipOf<UrsulaRaftTypeConfig>,
    pub(crate) current_snapshot: Arc<Mutex<Option<CurrentSnapshot>>>,
    pub(crate) metadata_serial: Arc<crate::rt::sync::Mutex<()>>,
    pub(crate) snapshot_store: SharedSnapshotStore,
    pub(crate) snapshot_build: SnapshotBuildCoordinator,
    pub(crate) snapshot_install: SnapshotInstallCoordinator,
    snapshot_metadata_path: Option<PathBuf>,
    /// Log applied since the last snapshot (F12e), read by the driver.
    log_gauge: Arc<GroupLogGauge>,
}

/// Node-local record of the current snapshot (`group-N.snapshot.json`),
/// in the MessagePack envelope (bounded-state F12a).
#[derive(Debug, Serialize, Deserialize)]
struct PersistedSnapshot {
    meta: SnapshotMetaOf<UrsulaRaftTypeConfig>,
    #[serde(with = "serde_bytes")]
    pointer_bytes: Vec<u8>,
}

impl RaftGroupStateMachine {
    pub fn new(placement: ShardPlacement) -> Self {
        Self::new_with_metrics(placement, None)
    }

    pub(crate) fn new_with_metrics(
        placement: ShardPlacement,
        metrics: Option<GroupEngineMetrics>,
    ) -> Self {
        Self::new_with_metrics_and_cold_store(placement, metrics, None)
    }

    pub(crate) fn new_with_metrics_and_cold_store(
        placement: ShardPlacement,
        metrics: Option<GroupEngineMetrics>,
        cold_store: Option<ColdStoreHandle>,
    ) -> Self {
        Self::new_with_stores(placement, metrics, cold_store, default_snapshot_store())
    }

    pub(crate) fn new_with_stores(
        placement: ShardPlacement,
        metrics: Option<GroupEngineMetrics>,
        cold_store: Option<ColdStoreHandle>,
        snapshot_store: SharedSnapshotStore,
    ) -> Self {
        Self::new_with_stores_and_snapshot_install(
            placement,
            metrics,
            cold_store,
            snapshot_store,
            SnapshotBuildCoordinator::default(),
            SnapshotInstallCoordinator::default(),
            None,
        )
    }

    pub(crate) fn new_with_stores_and_snapshot_install(
        placement: ShardPlacement,
        metrics: Option<GroupEngineMetrics>,
        cold_store: Option<ColdStoreHandle>,
        snapshot_store: SharedSnapshotStore,
        snapshot_build: SnapshotBuildCoordinator,
        snapshot_install: SnapshotInstallCoordinator,
        snapshot_metadata_path: Option<PathBuf>,
    ) -> Self {
        let log_gauge = snapshot_build.log_gauge(placement.raft_group_id.0);
        Self {
            placement,
            engine: match cold_store {
                Some(cold_store) => InMemoryGroupEngine::with_cold_store(cold_store),
                None => InMemoryGroupEngine::default(),
            },
            metrics,
            last_applied_log_id: None,
            last_membership: StoredMembershipOf::<UrsulaRaftTypeConfig>::default(),
            current_snapshot: Arc::new(Mutex::new(None)),
            metadata_serial: Arc::default(),
            snapshot_store,
            snapshot_build,
            snapshot_install,
            snapshot_metadata_path,
            log_gauge,
        }
    }

    pub(crate) async fn restore_persisted_snapshot(&mut self) -> Result<(), io::Error> {
        let Some(path) = &self.snapshot_metadata_path else {
            return Ok(());
        };
        if !path.exists() {
            return Ok(());
        }

        let persisted = decode_snapshot_envelope::<PersistedSnapshot>(&std::fs::read(path)?)
            .map_err(|err| invalid_data(io::Error::other(err.to_string())))?;
        let pointer = SnapshotPointer::decode(&persisted.pointer_bytes)
            .map_err(|err| invalid_data(io::Error::other(err.to_string())))?;
        let references = self
            .snapshot_install
            .references(self.placement.raft_group_id.0);
        let reference = references
            .prepare(
                &self.snapshot_store,
                self.placement.raft_group_id.0,
                &pointer.location,
            )
            .await
            .map_err(|err| err.into_io())?;
        let snapshot_bytes = match &pointer.location {
            SnapshotLocation::Inline { bytes } => bytes.clone(),
            location => self
                .snapshot_store
                .download(location)
                .await
                .map_err(|err| err.into_io())?,
        };
        let group_snapshot = decode_group_snapshot(&snapshot_bytes).map_err(|err| err.into_io())?;
        self.engine
            .install_snapshot(group_snapshot)
            .await
            .map_err(group_engine_io_error)?;
        self.last_applied_log_id = persisted.meta.last_log_id;
        self.last_membership = persisted.meta.last_membership.clone();
        self.log_gauge
            .record_snapshot(self.log_gauge.mark(), pointer.location.size_hint());
        *self.current_snapshot.lock().expect("snapshot mutex") = Some(CurrentSnapshot {
            meta: persisted.meta,
            pointer_bytes: persisted.pointer_bytes,
        });
        references.commit_current(&pointer.location);
        drop(reference);
        if let Err(err) = references
            .publish_current(&self.snapshot_store, self.placement.raft_group_id.0)
            .await
        {
            tracing::warn!(%err, "restored snapshot remains pinned while current-reference publication is unavailable");
        }
        Ok(())
    }

    /// Log applied since this group's last snapshot (F12e).
    pub fn log_progress(&self) -> GroupLogProgress {
        self.log_gauge.progress()
    }

    pub async fn group_snapshot(&mut self) -> Result<GroupSnapshot, io::Error> {
        self.engine
            .snapshot(self.placement)
            .await
            .map_err(group_engine_io_error)
    }

    pub async fn head_stream(
        &mut self,
        request: HeadStreamRequest,
        placement: ShardPlacement,
    ) -> Result<HeadStreamResponse, GroupEngineError> {
        self.engine.head_stream(request, placement).await
    }

    pub async fn read_stream(
        &mut self,
        request: ReadStreamRequest,
        placement: ShardPlacement,
    ) -> Result<ReadStreamResponse, GroupEngineError> {
        self.engine.read_stream(request, placement).await
    }

    pub async fn read_snapshot(
        &mut self,
        request: ReadSnapshotRequest,
        placement: ShardPlacement,
    ) -> Result<ReadSnapshotResponse, GroupEngineError> {
        self.engine.read_snapshot(request, placement).await
    }

    pub async fn bootstrap_stream(
        &mut self,
        request: BootstrapStreamRequest,
        placement: ShardPlacement,
    ) -> Result<BootstrapStreamResponse, GroupEngineError> {
        self.engine.bootstrap_stream(request, placement).await
    }

    pub async fn access_requires_write(
        &mut self,
        stream_id: &BucketStreamId,
        now_ms: u64,
        renew_ttl: bool,
    ) -> Result<bool, GroupEngineError> {
        self.engine
            .access_requires_write(stream_id, now_ms, renew_ttl)
    }

    pub async fn plan_cold_flush(
        &mut self,
        request: PlanColdFlushRequest,
        placement: ShardPlacement,
    ) -> Result<Option<ColdFlushCandidate>, GroupEngineError> {
        self.engine.plan_cold_flush(request, placement).await
    }

    pub async fn plan_next_cold_flush_batch(
        &mut self,
        request: PlanGroupColdFlushRequest,
        placement: ShardPlacement,
        max_candidates: usize,
    ) -> Result<Vec<ColdFlushCandidate>, GroupEngineError> {
        self.engine
            .plan_next_cold_flush_batch(request, placement, max_candidates)
            .await
    }

    pub async fn cold_hot_backlog(
        &mut self,
        stream_id: BucketStreamId,
        placement: ShardPlacement,
    ) -> Result<ColdHotBacklog, GroupEngineError> {
        self.engine.cold_hot_backlog(stream_id, placement).await
    }

    pub async fn plan_cold_gc(
        &mut self,
        max: usize,
        placement: ShardPlacement,
    ) -> Result<Vec<ColdGcPlanEntry>, GroupEngineError> {
        self.engine.plan_cold_gc(max, placement).await
    }

    pub async fn check_create_stream_cold_admission(
        &mut self,
        request: CreateStreamRequest,
        placement: ShardPlacement,
        admission: ColdWriteAdmission,
    ) -> Result<(), GroupEngineError> {
        let _ = placement;
        self.engine.check_cold_write_admission(
            &request.stream_id,
            admission,
            u64::try_from(request.initial_payload.len()).expect("payload len fits u64"),
        )?;
        Ok(())
    }

    pub async fn check_append_cold_admission(
        &mut self,
        request: AppendRequest,
        placement: ShardPlacement,
        admission: ColdWriteAdmission,
    ) -> Result<(), GroupEngineError> {
        let _ = placement;
        self.engine.check_cold_write_admission(
            &request.stream_id,
            admission,
            u64::try_from(request.payload.len()).expect("payload len fits u64"),
        )?;
        Ok(())
    }

    pub async fn install_group_snapshot(
        &mut self,
        snapshot: GroupSnapshot,
    ) -> Result<(), GroupEngineError> {
        self.engine.install_snapshot(snapshot).await
    }

    async fn snapshot_builder_with_permit(
        &mut self,
        build_permit: OwnedSemaphorePermit,
    ) -> RaftGroupSnapshotBuilder {
        let snapshot = self
            .group_snapshot()
            .await
            .expect("in-memory group snapshot should not fail");
        RaftGroupSnapshotBuilder {
            placement: self.placement,
            snapshot: Arc::new(snapshot),
            meta: self.snapshot_meta(),
            current_snapshot: self.current_snapshot.clone(),
            metadata_serial: self.metadata_serial.clone(),
            snapshot_store: self.snapshot_store.clone(),
            metrics: self.metrics.clone(),
            _build_permit: build_permit,
            snapshot_metadata_path: self.snapshot_metadata_path.clone(),
            log_gauge: Arc::clone(&self.log_gauge),
            log_mark: self.log_gauge.mark(),
            references: self
                .snapshot_install
                .references(self.placement.raft_group_id.0),
        }
    }

    pub(crate) fn snapshot_meta(&self) -> SnapshotMetaOf<UrsulaRaftTypeConfig> {
        SnapshotMetaOf::<UrsulaRaftTypeConfig> {
            last_log_id: self.last_applied_log_id,
            last_membership: self.last_membership.clone(),
            snapshot_id: self
                .last_applied_log_id
                .map(|log_id| {
                    format!(
                        "group-{}-{}-{}",
                        self.placement.raft_group_id.0,
                        log_id.committed_leader_id(),
                        log_id.index()
                    )
                })
                .unwrap_or_else(|| format!("group-{}-empty", self.placement.raft_group_id.0)),
        }
    }
}

impl RaftStateMachine<UrsulaRaftTypeConfig> for RaftGroupStateMachine {
    type SnapshotBuilder = RaftGroupSnapshotBuilder;

    async fn applied_state(
        &mut self,
    ) -> Result<
        (
            Option<LogIdOf<UrsulaRaftTypeConfig>>,
            StoredMembershipOf<UrsulaRaftTypeConfig>,
        ),
        io::Error,
    > {
        Ok((self.last_applied_log_id, self.last_membership.clone()))
    }

    async fn apply<Strm>(&mut self, mut entries: Strm) -> Result<(), io::Error>
    where Strm: Stream<Item = Result<EntryResponder<UrsulaRaftTypeConfig>, io::Error>>
            + Unpin
            + openraft::OptionalSend {
        let mut applied_entries = 0usize;
        let mut apply_ns = 0u64;
        while let Some((entry, responder)) = entries.try_next().await? {
            self.last_applied_log_id = Some(entry.log_id);

            self.log_gauge
                .record_applied(crate::types::entry_log_bytes(&entry));
            let response = match entry.payload {
                EntryPayload::Blank => RaftGroupResponse::Blank,
                EntryPayload::Normal(command) => {
                    let apply_started_at = Instant::now();
                    applied_entries = applied_entries.saturating_add(1);
                    let response = RaftGroupResponse::Write(
                        self.engine.apply_committed_write(command, self.placement),
                    );
                    apply_ns = apply_ns.saturating_add(elapsed_ns(apply_started_at));
                    response
                }
                EntryPayload::Membership(membership) => {
                    self.last_membership = StoredMembershipOf::<UrsulaRaftTypeConfig>::new(
                        Some(entry.log_id),
                        membership,
                    );
                    RaftGroupResponse::Membership
                }
            };

            if let Some(responder) = responder {
                responder.send(response);
            }
        }
        if applied_entries > 0
            && let Some(metrics) = &self.metrics
        {
            metrics.record_raft_apply_batch(self.placement, applied_entries, apply_ns);
        }
        Ok(())
    }

    async fn try_create_snapshot_builder(&mut self, force: bool) -> Option<Self::SnapshotBuilder> {
        if force {
            return Some(self.get_snapshot_builder().await);
        }
        // The snapshot driver hands the permit over before it triggers.
        if let Some(build_permit) = self
            .snapshot_build
            .take_handoff(self.placement.raft_group_id.0)
        {
            return Some(self.snapshot_builder_with_permit(build_permit).await);
        }
        // A policy-triggered build defers instead of waiting for the
        // node-wide permit on this group's state-machine worker; OpenRaft
        // retries on a later trigger.
        let Some(build_permit) = self.snapshot_build.try_acquire() else {
            tracing::debug!(
                raft_group_id = self.placement.raft_group_id.0,
                "deferring OpenRaft snapshot build while another group holds the build permit"
            );
            return None;
        };
        Some(self.snapshot_builder_with_permit(build_permit).await)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        // A permit already handed to this group would otherwise be held
        // while this build waits for another one.
        let build_permit = match self
            .snapshot_build
            .take_handoff(self.placement.raft_group_id.0)
        {
            Some(build_permit) => build_permit,
            None => self
                .snapshot_build
                .acquire()
                .await
                .expect("snapshot build coordinator should not close"),
        };
        self.snapshot_builder_with_permit(build_permit).await
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<SnapshotDataOf<UrsulaRaftTypeConfig>, io::Error> {
        Ok(Cursor::new(Vec::new()))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMetaOf<UrsulaRaftTypeConfig>,
        snapshot: SnapshotDataOf<UrsulaRaftTypeConfig>,
    ) -> Result<(), io::Error> {
        let mut pointer_bytes = snapshot.into_inner();
        // A publication retry still passes through Raft's vote checks, but the
        // already durable snapshot needs neither download nor installation.
        if self
            .current_snapshot
            .lock()
            .expect("snapshot mutex")
            .as_ref()
            .is_some_and(|current| current.meta == *meta && current.pointer_bytes == pointer_bytes)
        {
            return Ok(());
        }
        let mut pointer = SnapshotPointer::decode(&pointer_bytes)
            .map_err(|err| invalid_data(io::Error::other(err.to_string())))?;
        // Decode exactly once (bounded-stream-state F12c): inline bytes are
        // decoded in place, and a prefetched external snapshot arrives
        // already decoded.
        let mut reference = None;
        let group_snapshot = match &pointer.location {
            SnapshotLocation::Inline { bytes } => {
                decode_group_snapshot(bytes).map_err(|err| err.into_io())?
            }
            location => match self.snapshot_install.take_prefetched(&pointer) {
                Some(prefetched) => {
                    reference = prefetched.reference;
                    pointer.snapshot_id = prefetched.original_snapshot_id;
                    pointer_bytes = pointer.encode_binary().map_err(|err| err.into_io())?;
                    prefetched.snapshot
                }
                None => {
                    if matches!(location, SnapshotLocation::S3 { .. }) {
                        return Err(io::Error::other(
                            "external S3 snapshots must be pinned and prefetched before Raft installation",
                        ));
                    }
                    let bytes = self
                        .snapshot_store
                        .download(location)
                        .await
                        .map_err(|err| err.into_io())?;
                    decode_group_snapshot(&bytes).map_err(|err| err.into_io())?
                }
            },
        };
        self.engine
            .install_snapshot(group_snapshot)
            .await
            .map_err(group_engine_io_error)?;
        self.last_applied_log_id = meta.last_log_id;
        self.last_membership = meta.last_membership.clone();
        self.log_gauge
            .record_snapshot(self.log_gauge.mark(), pointer.location.size_hint());
        let current_snapshot = self.current_snapshot.clone();
        let metadata_path = self.snapshot_metadata_path.clone();
        let references = self
            .snapshot_install
            .references(self.placement.raft_group_id.0);
        let meta = meta.clone();
        snapshot_metadata_work(self.metadata_serial.clone(), move || {
            let _reference = reference;
            persist_snapshot_metadata(metadata_path.as_deref(), &meta, &pointer_bytes)?;
            *current_snapshot.lock().expect("snapshot mutex") = Some(CurrentSnapshot {
                meta,
                pointer_bytes,
            });
            references.commit_current(&pointer.location);
            Ok(())
        })
        .await?;
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<SnapshotOf<UrsulaRaftTypeConfig>>, io::Error> {
        Ok(self
            .current_snapshot
            .lock()
            .expect("snapshot mutex")
            .as_ref()
            .map(|snapshot| SnapshotOf::<UrsulaRaftTypeConfig> {
                meta: snapshot.meta.clone(),
                snapshot: Cursor::new(snapshot.pointer_bytes.clone()),
            }))
    }
}

pub struct RaftGroupSnapshotBuilder {
    placement: ShardPlacement,
    /// Shared with the frame iterators, so an upload and any inline fallback
    /// encode the same group without deep-cloning it (F12c).
    snapshot: Arc<GroupSnapshot>,
    pub(crate) meta: SnapshotMetaOf<UrsulaRaftTypeConfig>,
    current_snapshot: Arc<Mutex<Option<CurrentSnapshot>>>,
    metadata_serial: Arc<crate::rt::sync::Mutex<()>>,
    snapshot_store: SharedSnapshotStore,
    metrics: Option<GroupEngineMetrics>,
    _build_permit: OwnedSemaphorePermit,
    snapshot_metadata_path: Option<PathBuf>,
    log_gauge: Arc<GroupLogGauge>,
    /// The applied log this snapshot covers (F12e).
    log_mark: GroupLogMark,
    references: Arc<SnapshotReferences>,
}

impl RaftSnapshotBuilder<UrsulaRaftTypeConfig> for RaftGroupSnapshotBuilder {
    async fn build_snapshot(&mut self) -> Result<SnapshotOf<UrsulaRaftTypeConfig>, io::Error> {
        let started_at = Instant::now();
        let stream_count = self.snapshot.stream_snapshot.streams.len();
        let snapshot_id = self.meta.snapshot_id.clone();
        let key = SnapshotKey {
            raft_group_id: self.placement.raft_group_id.0,
            snapshot_id: snapshot_id.clone(),
        };
        let location = match self
            .snapshot_store
            .upload_iter(key, group_snapshot_frames(Arc::clone(&self.snapshot)))
            .await
        {
            Ok(location) => {
                // Re-stat immediately so a silent partial-success (multipart
                // Complete failing after the parts uploaded, opendal retry
                // caching, etc.) is caught HERE rather than 10 minutes later
                // as an install_snapshot NotFound on a follower. Cheap
                // relative to the upload itself.
                match self.snapshot_store.verify_uploaded(&location).await {
                    Ok(()) => location,
                    Err(err) => {
                        tracing::warn!(
                            snapshot_id,
                            error = %err,
                            "falling back to inline OpenRaft snapshot after external snapshot verification failed"
                        );
                        SnapshotLocation::Inline {
                            bytes: group_snapshot_frames(Arc::clone(&self.snapshot))
                                .collect::<Result<Vec<_>, _>>()
                                .map_err(|err| err.into_io())?
                                .into_iter()
                                .flat_map(|chunk| chunk.to_vec())
                                .collect(),
                        }
                    }
                }
            }
            Err(err) => {
                tracing::warn!(
                    snapshot_id,
                    error = %err,
                    "falling back to inline OpenRaft snapshot after external snapshot upload failed"
                );
                SnapshotLocation::Inline {
                    bytes: group_snapshot_frames(Arc::clone(&self.snapshot))
                        .collect::<Result<Vec<_>, _>>()
                        .map_err(|err| err.into_io())?
                        .into_iter()
                        .flat_map(|chunk| chunk.to_vec())
                        .collect(),
                }
            }
        };
        let mut pointer = SnapshotPointer {
            snapshot_id: snapshot_id.clone(),
            location,
        };
        let reference = match self
            .references
            .prepare(
                &self.snapshot_store,
                self.placement.raft_group_id.0,
                &pointer.location,
            )
            .await
        {
            Ok(reference) => reference,
            Err(err) => {
                tracing::warn!(snapshot_id, %err, "falling back to inline snapshot after external pin failure");
                pointer.location = SnapshotLocation::Inline {
                    bytes: group_snapshot_frames(Arc::clone(&self.snapshot))
                        .collect::<Result<Vec<_>, _>>()
                        .map_err(|err| err.into_io())?
                        .into_iter()
                        .flat_map(|chunk| chunk.to_vec())
                        .collect(),
                };
                None
            }
        };
        let pointer_bytes = pointer.encode_binary().map_err(|err| err.into_io())?;
        let external_upload = !matches!(pointer.location, SnapshotLocation::Inline { .. });
        let inline_fallback = !external_upload;
        if let Some(metrics) = &self.metrics {
            metrics.record_raft_snapshot_build(
                self.placement,
                stream_count,
                usize::try_from(pointer.location.size_hint()).unwrap_or(usize::MAX),
                pointer_bytes.len(),
                elapsed_ns(started_at),
                external_upload,
                inline_fallback,
            );
        }
        let current_snapshot = self.current_snapshot.clone();
        let metadata_path = self.snapshot_metadata_path.clone();
        let meta = self.meta.clone();
        let references = self.references.clone();
        let log_gauge = self.log_gauge.clone();
        let log_mark = self.log_mark;
        let chosen = snapshot_metadata_work(self.metadata_serial.clone(), move || {
            let _reference = reference;
            let previous = current_snapshot.lock().expect("snapshot mutex").clone();
            // A build captured before a newer install must not overwrite its
            // durable metadata, reference or pointer when the upload finishes.
            if let Some(current) = previous.as_ref()
                && current.meta.last_log_id > meta.last_log_id
            {
                Ok(current.clone())
            } else {
                persist_snapshot_metadata(metadata_path.as_deref(), &meta, &pointer_bytes)?;
                let current = CurrentSnapshot {
                    meta,
                    pointer_bytes,
                };
                current_snapshot
                    .lock()
                    .expect("snapshot mutex")
                    .replace(current.clone());
                references.commit_current(&pointer.location);
                log_gauge.record_snapshot(log_mark, pointer.location.size_hint());
                Ok(current)
            }
        })
        .await?;
        if let Err(err) = self
            .references
            .publish_current(&self.snapshot_store, self.placement.raft_group_id.0)
            .await
        {
            tracing::warn!(snapshot_id, %err, "snapshot remains pinned while current-reference publication is unavailable");
        }
        let chosen_pointer =
            SnapshotPointer::decode(&chosen.pointer_bytes).map_err(|err| err.into_io())?;
        if let Err(err) = self
            .snapshot_store
            .prune_retired(
                self.placement.raft_group_id.0,
                &chosen_pointer.location,
                RETAINED_RETIRED_EXTERNAL_SNAPSHOTS,
            )
            .await
        {
            tracing::warn!(
                snapshot_id = pointer.snapshot_id,
                error = %err,
                "failed to prune retired OpenRaft snapshots"
            );
        }
        Ok(SnapshotOf::<UrsulaRaftTypeConfig> {
            meta: chosen.meta,
            snapshot: Cursor::new(chosen.pointer_bytes),
        })
    }
}

/// Keep metadata fsync and the serialized pointer transition off core workers.
pub(crate) async fn snapshot_metadata_work<T: Send + 'static>(
    serial: Arc<crate::rt::sync::Mutex<()>>,
    work: impl FnOnce() -> Result<T, io::Error> + Send + 'static,
) -> Result<T, io::Error> {
    let serial = serial.lock_owned().await;
    let work = move || {
        let _serial = serial;
        work()
    };
    #[cfg(not(madsim))]
    {
        tokio::task::spawn_blocking(work)
            .await
            .map_err(io::Error::other)?
    }
    #[cfg(madsim)]
    {
        work()
    }
}

fn persist_snapshot_metadata(
    path: Option<&Path>,
    meta: &SnapshotMetaOf<UrsulaRaftTypeConfig>,
    pointer_bytes: &[u8],
) -> Result<(), io::Error> {
    let Some(path) = path else {
        return Ok(());
    };
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let encoded = encode_binary_envelope(&PersistedSnapshot {
        meta: meta.clone(),
        pointer_bytes: pointer_bytes.to_vec(),
    })
    .map_err(|err| invalid_data(io::Error::other(err.to_string())))?;
    let temporary = path.with_extension("json.tmp");
    {
        use std::io::Write;

        let mut file = std::fs::File::create(&temporary)?;
        file.write_all(&encoded)?;
        file.sync_all()?;
    }
    std::fs::rename(&temporary, path)?;
    if let Some(parent) = path.parent() {
        let directory = std::fs::File::open(parent)?;
        directory.sync_all()?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    #[cfg(all(unix, not(madsim)))]
    #[test]
    fn snapshot_metadata_rejects_an_unsyncable_parent_directory() {
        use std::os::unix::fs::PermissionsExt;
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("snapshot.json");
        let meta = SnapshotMetaOf::<UrsulaRaftTypeConfig> {
            last_log_id: None,
            last_membership: Default::default(),
            snapshot_id: "directory-sync-failure".to_owned(),
        };
        // Write/search permits file creation and rename; lack of read permission
        // prevents opening the directory for fsync after the rename succeeds.
        std::fs::set_permissions(root.path(), std::fs::Permissions::from_mode(0o300)).unwrap();
        let can_open = std::fs::File::open(root.path());
        let result = persist_snapshot_metadata(Some(&path), &meta, b"pointer");
        std::fs::set_permissions(root.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        let error = can_open.expect_err(
            "directory-permission fault must be exercised: run this test as an unprivileged user on a filesystem that enforces directory read permissions",
        );
        assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
        assert!(
            path.exists(),
            "the metadata rename must finish before the injected failure"
        );
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::PermissionDenied);
    }
    #[cfg(not(madsim))]
    #[tokio::test]
    async fn canceled_metadata_waiter_retains_serial_until_publication() {
        let serial = Arc::new(crate::rt::sync::Mutex::new(()));
        let (release, wait) = std::sync::mpsc::channel();
        let entered = Arc::new(crate::rt::sync::Notify::new());
        let worker_entered = entered.clone();
        let published = Arc::new(AtomicBool::new(false));
        let worker_published = published.clone();
        let work = crate::rt::spawn(snapshot_metadata_work(serial.clone(), move || {
            worker_entered.notify_one();
            wait.recv_timeout(std::time::Duration::from_secs(2))
                .map_err(io::Error::other)?;
            worker_published.store(true, Ordering::Release);
            Ok(())
        }));
        entered.notified().await;
        work.abort();
        assert!(work.await.unwrap_err().is_cancelled());
        assert!(
            serial.try_lock().is_err(),
            "cancellation must retain the publication guard"
        );
        release.send(()).unwrap();
        let _drained = tokio::time::timeout(std::time::Duration::from_secs(2), serial.lock())
            .await
            .unwrap();
        assert!(published.load(Ordering::Acquire));
    }

    #[cfg(not(madsim))]
    #[tokio::test(flavor = "current_thread")]
    async fn metadata_work_does_not_block_the_core_executor() {
        let (release, wait) = std::sync::mpsc::channel();
        let entered = Arc::new(crate::rt::sync::Notify::new());
        let worker_entered = entered.clone();
        let metadata = crate::rt::spawn(snapshot_metadata_work(Arc::default(), move || {
            worker_entered.notify_one();
            wait.recv_timeout(std::time::Duration::from_secs(2))
                .map_err(io::Error::other)
        }));
        entered.notified().await;
        // This runs on the same single-thread executor as snapshot installation.
        // Running blocking metadata work inline would time out before reaching it.
        release.send(()).unwrap();
        metadata.await.unwrap().unwrap();
    }
    #[cfg(not(madsim))]
    use bytes::Bytes;

    use super::*;

    #[cfg(not(madsim))]
    fn test_log_id(index: u64) -> LogIdOf<UrsulaRaftTypeConfig> {
        use openraft::LogId;
        use openraft::vote::RaftLeaderId;

        type LeaderId = <UrsulaRaftTypeConfig as openraft::RaftTypeConfig>::LeaderId;
        LogId {
            leader_id: LeaderId::new(1, 1),
            index,
        }
    }

    #[cfg(not(madsim))]
    fn test_snapshot_meta(index: u64) -> SnapshotMetaOf<UrsulaRaftTypeConfig> {
        let log_id = test_log_id(index);
        SnapshotMetaOf::<UrsulaRaftTypeConfig> {
            last_log_id: Some(log_id),
            last_membership: StoredMembershipOf::<UrsulaRaftTypeConfig>::default(),
            snapshot_id: format!(
                "group-7-{}-{}",
                log_id.committed_leader_id(),
                log_id.index()
            ),
        }
    }

    #[cfg(not(madsim))]
    fn test_group_snapshot(placement: ShardPlacement, commit_index: u64) -> GroupSnapshot {
        GroupSnapshot {
            placement,
            group_commit_index: commit_index,
            stream_snapshot: Default::default(),
            stream_append_counts: Vec::new(),
        }
    }

    #[cfg(not(madsim))]
    async fn test_build_permit() -> OwnedSemaphorePermit {
        SnapshotBuildCoordinator::default()
            .acquire()
            .await
            .expect("test snapshot build permit")
    }

    #[cfg(not(madsim))]
    #[tokio::test]
    async fn persisted_snapshot_restores_state_machine_before_log_replay() {
        use ursula_shard::CoreId;
        use ursula_shard::RaftGroupId;
        use ursula_shard::ShardId;

        let placement = ShardPlacement {
            core_id: CoreId(0),
            shard_id: ShardId(0),
            raft_group_id: RaftGroupId(7),
        };
        let directory =
            std::env::temp_dir().join(format!("ursula-persisted-snapshot-{}", std::process::id()));
        crate::tests::remove_test_path(&directory);
        std::fs::create_dir_all(&directory).expect("snapshot metadata directory");
        let metadata_path = directory.join("group-7.snapshot.json");
        let current_snapshot = Arc::new(Mutex::new(None));
        let mut builder = RaftGroupSnapshotBuilder {
            placement,
            snapshot: Arc::new(test_group_snapshot(placement, 42)),
            meta: test_snapshot_meta(42),
            current_snapshot,
            metadata_serial: Arc::default(),
            snapshot_store: default_snapshot_store(),
            metrics: None,
            _build_permit: test_build_permit().await,
            log_gauge: Arc::default(),
            log_mark: GroupLogMark::default(),
            references: Arc::default(),
            snapshot_metadata_path: Some(metadata_path.clone()),
        };
        builder.build_snapshot().await.expect("persist snapshot");

        let mut restored = RaftGroupStateMachine::new_with_stores_and_snapshot_install(
            placement,
            None,
            None,
            default_snapshot_store(),
            SnapshotBuildCoordinator::default(),
            SnapshotInstallCoordinator::default(),
            Some(metadata_path),
        );
        restored
            .restore_persisted_snapshot()
            .await
            .expect("restore persisted snapshot");

        assert_eq!(restored.last_applied_log_id, Some(test_log_id(42)));
        assert_eq!(
            restored
                .group_snapshot()
                .await
                .expect("snapshot restored state")
                .group_commit_index,
            42
        );
        crate::tests::remove_test_path(directory);
    }

    /// F12a: the persisted snapshot record and the pointer inside it use the
    /// MessagePack envelope (format epoch 2 writes nothing else), and a node
    /// restores from it.
    #[cfg(not(madsim))]
    #[tokio::test]
    async fn persisted_snapshot_restores_from_the_binary_envelope() {
        use ursula_shard::CoreId;
        use ursula_shard::RaftGroupId;
        use ursula_shard::ShardId;

        let placement = ShardPlacement {
            core_id: CoreId(0),
            shard_id: ShardId(0),
            raft_group_id: RaftGroupId(7),
        };
        let directory = std::env::temp_dir().join(format!(
            "ursula-persisted-binary-snapshot-{}",
            std::process::id()
        ));
        crate::tests::remove_test_path(&directory);
        std::fs::create_dir_all(&directory).expect("snapshot metadata directory");
        let metadata_path = directory.join("group-7.snapshot.json");
        let mut builder = RaftGroupSnapshotBuilder {
            placement,
            snapshot: Arc::new(test_group_snapshot(placement, 42)),
            meta: test_snapshot_meta(42),
            current_snapshot: Arc::new(Mutex::new(None)),
            metadata_serial: Arc::default(),
            snapshot_store: default_snapshot_store(),
            metrics: None,
            _build_permit: test_build_permit().await,
            log_gauge: Arc::default(),
            log_mark: GroupLogMark::default(),
            references: Arc::default(),
            snapshot_metadata_path: Some(metadata_path.clone()),
        };
        builder.build_snapshot().await.expect("persist snapshot");

        let binary = std::fs::read(&metadata_path).expect("persisted record");
        let persisted =
            decode_snapshot_envelope::<PersistedSnapshot>(&binary).expect("decode binary record");
        SnapshotPointer::decode(&persisted.pointer_bytes).expect("binary pointer");

        let mut restored = RaftGroupStateMachine::new_with_stores_and_snapshot_install(
            placement,
            None,
            None,
            default_snapshot_store(),
            SnapshotBuildCoordinator::default(),
            SnapshotInstallCoordinator::default(),
            Some(metadata_path),
        );
        restored
            .restore_persisted_snapshot()
            .await
            .expect("restore the binary record");
        assert_eq!(restored.last_applied_log_id, Some(test_log_id(42)));
        assert_eq!(
            restored
                .group_snapshot()
                .await
                .expect("snapshot restored state")
                .group_commit_index,
            42
        );
        crate::tests::remove_test_path(directory);
    }

    #[cfg(not(madsim))]
    #[tokio::test]
    async fn older_snapshot_build_cannot_overwrite_a_newer_installed_pointer_or_metadata() {
        let placement = ShardPlacement {
            core_id: ursula_shard::CoreId(0),
            shard_id: ursula_shard::ShardId(0),
            raft_group_id: ursula_shard::RaftGroupId(7),
        };
        let directory = tempfile::tempdir().unwrap();
        let metadata = directory.path().join("group-7.snapshot.json");
        let store: SharedSnapshotStore =
            Arc::new(ursula_runtime::S3SnapshotStore::memory_for_tests("stale-build").unwrap());
        let mut state = RaftGroupStateMachine::new_with_stores_and_snapshot_install(
            placement,
            None,
            None,
            store.clone(),
            SnapshotBuildCoordinator::default(),
            SnapshotInstallCoordinator::default(),
            Some(metadata.clone()),
        );
        state
            .engine
            .install_snapshot(test_group_snapshot(placement, 1))
            .await
            .unwrap();
        state.last_applied_log_id = Some(test_log_id(1));
        let mut old_builder = state.get_snapshot_builder().await;
        let bytes = group_snapshot_frames(Arc::new(test_group_snapshot(placement, 2)))
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
            .into_iter()
            .flat_map(|chunk| chunk.to_vec())
            .collect();
        let pointer = SnapshotPointer {
            snapshot_id: "newer-install".to_owned(),
            location: SnapshotLocation::Inline { bytes },
        };
        state
            .install_snapshot(
                &test_snapshot_meta(2),
                Cursor::new(pointer.encode_binary().unwrap()),
            )
            .await
            .unwrap();
        // Complete the upload captured at index 1 only after installing index 2.
        let returned = old_builder.build_snapshot().await.unwrap();
        assert_eq!(returned.meta.last_log_id, Some(test_log_id(2)));
        let pointer = SnapshotPointer::decode(returned.snapshot.get_ref()).unwrap();
        assert_eq!(pointer.snapshot_id, "newer-install");
        let mut restored = RaftGroupStateMachine::new_with_stores_and_snapshot_install(
            placement,
            None,
            None,
            store,
            SnapshotBuildCoordinator::default(),
            SnapshotInstallCoordinator::default(),
            Some(metadata),
        );
        restored.restore_persisted_snapshot().await.unwrap();
        assert_eq!(restored.last_applied_log_id, Some(test_log_id(2)));
        assert_eq!(
            restored.group_snapshot().await.unwrap().group_commit_index,
            2
        );
    }

    #[cfg(not(madsim))]
    #[tokio::test]
    async fn snapshot_builder_keeps_external_snapshots_referenced_by_published_pointers() {
        use std::sync::Arc;

        use ursula_runtime::S3SnapshotStore;
        use ursula_runtime::SnapshotStore;
        use ursula_shard::CoreId;
        use ursula_shard::RaftGroupId;
        use ursula_shard::ShardId;

        let placement = ShardPlacement {
            core_id: CoreId(0),
            shard_id: ShardId(0),
            raft_group_id: RaftGroupId(7),
        };
        let raw_store = Arc::new(
            S3SnapshotStore::memory_for_tests(format!(
                "state-machine-snapshot-retention-{}",
                std::process::id()
            ))
            .expect("memory S3 snapshot store"),
        );
        let snapshot_store: SharedSnapshotStore = raw_store.clone();
        let current_snapshot = Arc::new(Mutex::new(None));

        let mut first = RaftGroupSnapshotBuilder {
            placement,
            snapshot: Arc::new(test_group_snapshot(placement, 1)),
            meta: test_snapshot_meta(1),
            current_snapshot: current_snapshot.clone(),
            metadata_serial: Arc::default(),
            snapshot_store: snapshot_store.clone(),
            metrics: None,
            _build_permit: test_build_permit().await,
            log_gauge: Arc::default(),
            log_mark: GroupLogMark::default(),
            references: Arc::default(),
            snapshot_metadata_path: None,
        };
        let first_snapshot = first.build_snapshot().await.expect("first snapshot");
        let first_pointer =
            SnapshotPointer::decode(&first_snapshot.snapshot.into_inner()).expect("first pointer");

        let mut second = RaftGroupSnapshotBuilder {
            placement,
            snapshot: Arc::new(test_group_snapshot(placement, 2)),
            meta: test_snapshot_meta(2),
            current_snapshot: current_snapshot.clone(),
            metadata_serial: Arc::default(),
            snapshot_store: snapshot_store.clone(),
            metrics: None,
            _build_permit: test_build_permit().await,
            log_gauge: Arc::default(),
            log_mark: GroupLogMark::default(),
            references: Arc::default(),
            snapshot_metadata_path: None,
        };
        let second_snapshot = second.build_snapshot().await.expect("second snapshot");
        let second_pointer = SnapshotPointer::decode(&second_snapshot.snapshot.into_inner())
            .expect("second pointer");

        let first_bytes = raw_store
            .download(&first_pointer.location)
            .await
            .expect("previous snapshot remains readable");
        let second_bytes = raw_store
            .download(&second_pointer.location)
            .await
            .expect("current snapshot remains readable");
        let first_group = decode_group_snapshot(&first_bytes).expect("decode first group snapshot");
        let second_group =
            decode_group_snapshot(&second_bytes).expect("decode second group snapshot");
        assert_eq!(first_group.group_commit_index, 1);
        assert_eq!(second_group.group_commit_index, 2);

        // Simulate a process restart: the published snapshot pointer survives
        // in external storage, but builder-local retired state is gone.
        let current_snapshot = Arc::new(Mutex::new(None));
        let mut third = RaftGroupSnapshotBuilder {
            placement,
            snapshot: Arc::new(test_group_snapshot(placement, 3)),
            meta: test_snapshot_meta(3),
            current_snapshot,
            metadata_serial: Arc::default(),
            snapshot_store,
            metrics: None,
            _build_permit: test_build_permit().await,
            log_gauge: Arc::default(),
            log_mark: GroupLogMark::default(),
            references: Arc::default(),
            snapshot_metadata_path: None,
        };
        let third_snapshot = third.build_snapshot().await.expect("third snapshot");
        let third_pointer =
            SnapshotPointer::decode(&third_snapshot.snapshot.into_inner()).expect("third pointer");

        raw_store
            .download(&first_pointer.location)
            .await
            .expect("old published snapshot pointer remains readable");
        raw_store
            .download(&second_pointer.location)
            .await
            .expect("previous retired snapshot remains readable");
        raw_store
            .download(&third_pointer.location)
            .await
            .expect("current snapshot remains readable");
    }

    #[cfg(not(madsim))]
    #[tokio::test]
    async fn snapshot_builder_falls_back_to_inline_when_external_upload_fails() {
        use ursula_runtime::SnapshotStore;
        use ursula_runtime::SnapshotStoreError;
        use ursula_runtime::SnapshotStoreFuture;
        use ursula_shard::CoreId;
        use ursula_shard::RaftGroupId;
        use ursula_shard::ShardId;

        #[derive(Debug)]
        struct FailingSnapshotStore;

        impl SnapshotStore for FailingSnapshotStore {
            fn upload<'a>(
                &'a self,
                _key: SnapshotKey,
                _bytes: Bytes,
            ) -> SnapshotStoreFuture<'a, SnapshotLocation> {
                Box::pin(async move {
                    Err(SnapshotStoreError::Backend(
                        "seeded upload failure".to_owned(),
                    ))
                })
            }

            fn download<'a>(
                &'a self,
                _location: &'a SnapshotLocation,
            ) -> SnapshotStoreFuture<'a, Vec<u8>> {
                Box::pin(async move {
                    Err(SnapshotStoreError::Backend(
                        "download should not be used".to_owned(),
                    ))
                })
            }

            fn delete<'a>(
                &'a self,
                _location: &'a SnapshotLocation,
            ) -> SnapshotStoreFuture<'a, ()> {
                Box::pin(async move { Ok(()) })
            }
        }

        let placement = ShardPlacement {
            core_id: CoreId(0),
            shard_id: ShardId(0),
            raft_group_id: RaftGroupId(7),
        };
        let current_snapshot = Arc::new(Mutex::new(None));
        let mut builder = RaftGroupSnapshotBuilder {
            placement,
            snapshot: Arc::new(test_group_snapshot(placement, 3)),
            meta: test_snapshot_meta(3),
            current_snapshot: current_snapshot.clone(),
            metadata_serial: Arc::default(),
            snapshot_store: Arc::new(FailingSnapshotStore),
            metrics: None,
            _build_permit: test_build_permit().await,
            log_gauge: Arc::default(),
            log_mark: GroupLogMark::default(),
            references: Arc::default(),
            snapshot_metadata_path: None,
        };

        let snapshot = builder.build_snapshot().await.expect("inline fallback");
        let pointer =
            SnapshotPointer::decode(&snapshot.snapshot.into_inner()).expect("snapshot pointer");
        let SnapshotLocation::Inline { bytes } = pointer.location else {
            panic!("expected inline fallback");
        };
        let group = decode_group_snapshot(&bytes).expect("decode inline snapshot");
        assert_eq!(group.group_commit_index, 3);
        assert!(current_snapshot.lock().expect("snapshot mutex").is_some());
        // F12c: the upload attempt and the inline fallback both encoded from
        // the shared group without keeping or deep-cloning it.
        assert_eq!(Arc::strong_count(&builder.snapshot), 1);
    }

    #[tokio::test]
    async fn snapshot_install_coordinator_defaults_to_single_permit() {
        let coordinator = SnapshotInstallCoordinator::default();

        let permit = coordinator.acquire().await.expect("acquire install permit");
        assert_eq!(coordinator.available_permits(), 0);

        drop(permit);
        assert_eq!(coordinator.available_permits(), 1);
    }

    #[tokio::test]
    async fn snapshot_install_coordinator_clamps_zero_to_one_permit() {
        let coordinator = SnapshotInstallCoordinator::new(0);

        let permit = coordinator.acquire().await.expect("acquire install permit");
        assert_eq!(coordinator.available_permits(), 0);

        drop(permit);
        assert_eq!(coordinator.available_permits(), 1);
    }

    #[cfg(not(madsim))]
    fn test_state_machine(
        build: SnapshotBuildCoordinator,
        install: SnapshotInstallCoordinator,
    ) -> RaftGroupStateMachine {
        use ursula_shard::CoreId;
        use ursula_shard::RaftGroupId;
        use ursula_shard::ShardId;

        RaftGroupStateMachine::new_with_stores_and_snapshot_install(
            ShardPlacement {
                core_id: CoreId(0),
                shard_id: ShardId(0),
                raft_group_id: RaftGroupId(7),
            },
            None,
            None,
            default_snapshot_store(),
            build,
            install,
            None,
        )
    }

    /// F12d: a policy-triggered snapshot never waits on the node-wide build
    /// permit; it defers while another group builds, and a forced one (needed
    /// for replication) still waits for the permit.
    #[cfg(not(madsim))]
    #[tokio::test]
    async fn policy_snapshot_build_defers_while_another_group_holds_the_permit() {
        let build = SnapshotBuildCoordinator::new(1);
        let mut state_machine =
            test_state_machine(build.clone(), SnapshotInstallCoordinator::default());
        let held = build.acquire().await.expect("other group's build permit");

        let deferred = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            state_machine.try_create_snapshot_builder(false),
        )
        .await
        .expect("policy build must not wait for the permit");
        assert!(deferred.is_none());

        drop(held);
        let builder = state_machine
            .try_create_snapshot_builder(false)
            .await
            .expect("free permit builds");
        assert_eq!(build.available_permits(), 0);
        drop(builder);
        assert_eq!(build.available_permits(), 1);
        assert!(
            state_machine
                .try_create_snapshot_builder(true)
                .await
                .is_some()
        );
    }

    /// The snapshot driver's permit handoff: a group handed a permit builds
    /// on it even while the node's only other permit is gone, and a group
    /// without one still defers.
    #[cfg(not(madsim))]
    #[tokio::test]
    async fn handed_off_permit_builds_instead_of_deferring() {
        let build = SnapshotBuildCoordinator::new(1);
        let mut state_machine =
            test_state_machine(build.clone(), SnapshotInstallCoordinator::default());
        let group = state_machine.placement.raft_group_id.0;
        let permit = build.acquire().await.expect("driver's build permit");
        build.hand_off(group, permit);
        assert!(build.handoff_pending(group));

        let builder = state_machine
            .try_create_snapshot_builder(false)
            .await
            .expect("a handed-off permit builds");
        assert!(!build.handoff_pending(group));
        assert!(!build.reclaim_handoff(group));
        assert_eq!(build.available_permits(), 0);
        // The next trigger without a handoff defers while this build runs.
        assert!(
            state_machine
                .try_create_snapshot_builder(false)
                .await
                .is_none()
        );
        drop(builder);
        assert_eq!(build.available_permits(), 1);

        // An unclaimed handoff is taken back.
        build.hand_off(group, build.acquire().await.expect("permit"));
        assert!(build.reclaim_handoff(group));
        assert_eq!(build.available_permits(), 1);
    }

    #[test]
    fn log_pressure_uses_hysteresis() {
        let build = SnapshotBuildCoordinator::new(1);
        assert_eq!(build.observe_log_bytes(150, 200, 100), None);
        assert_eq!(build.observe_log_bytes(201, 200, 100), Some(true));
        assert!(build.log_pressured());
        assert_eq!(build.observe_log_bytes(150, 200, 100), None);
        assert!(build.log_pressured());
        assert_eq!(build.observe_log_bytes(99, 200, 100), Some(false));
        assert!(!build.log_pressured());
        // A coordinator rebuilt with another concurrency shares the flag.
        let resized = build.with_max_concurrency(2);
        assert_eq!(resized.observe_log_bytes(500, 200, 100), Some(true));
        assert!(build.log_pressured());
    }

    /// F12c: installing an inline snapshot decodes it exactly once.
    #[cfg(not(madsim))]
    #[tokio::test]
    async fn inline_snapshot_install_decodes_once() {
        use crate::snapshot_codec::decode_calls_on_this_thread;

        let mut state_machine = test_state_machine(
            SnapshotBuildCoordinator::default(),
            SnapshotInstallCoordinator::default(),
        );
        let placement = state_machine.placement;
        let bytes = group_snapshot_frames(Arc::new(test_group_snapshot(placement, 9)))
            .collect::<Result<Vec<_>, _>>()
            .expect("encode group snapshot")
            .concat();
        let meta = test_snapshot_meta(9);
        let pointer = SnapshotPointer {
            snapshot_id: meta.snapshot_id.clone(),
            location: SnapshotLocation::Inline { bytes },
        };
        let before = decode_calls_on_this_thread();
        state_machine
            .install_snapshot(
                &meta,
                Cursor::new(pointer.encode_binary().expect("pointer")),
            )
            .await
            .expect("install inline snapshot");
        assert_eq!(decode_calls_on_this_thread() - before, 1);
        assert_eq!(
            state_machine
                .group_snapshot()
                .await
                .expect("installed group")
                .group_commit_index,
            9
        );
    }
}

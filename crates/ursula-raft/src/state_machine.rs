use std::collections::BTreeMap;
use std::fmt::Debug;
use std::io;
use std::io::Cursor;
use std::sync::Arc;
use std::sync::Mutex;

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
use ursula_runtime::AppendBatchRequest;
use ursula_runtime::AppendRequest;
use ursula_runtime::BootstrapStreamRequest;
use ursula_runtime::BootstrapStreamResponse;
use ursula_runtime::ColdFlushCandidate;
use ursula_runtime::ColdGcEntry;
use ursula_runtime::ColdHotBacklog;
use ursula_runtime::ColdStoreHandle;
use ursula_runtime::ColdWriteAdmission;
use ursula_runtime::CreateStreamRequest;
use ursula_runtime::DeleteSnapshotRequest;
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
use ursula_runtime::default_snapshot_store;
use ursula_shard::BucketStreamId;
use ursula_shard::ShardPlacement;

use crate::codec::group_write_command_from_proto;
use crate::codec::raft_blank_response;
use crate::codec::raft_membership_response;
use crate::codec::raft_write_applied_response;
use crate::codec::raft_write_rejected_response;
use crate::engine::group_engine_io_error;
use crate::engine::invalid_data;
use crate::log_store::elapsed_ns;
use crate::rt::sync::OwnedSemaphorePermit;
use crate::rt::sync::Semaphore;
use crate::rt::time::Instant;
use crate::snapshot_codec::decode_group_snapshot;
use crate::snapshot_codec::group_snapshot_frames;
use crate::types::UrsulaRaftTypeConfig;

#[derive(Debug, Clone)]
pub struct SnapshotBuildCoordinator {
    inner: Arc<SnapshotBuildCoordinatorInner>,
}

#[derive(Debug)]
struct SnapshotBuildCoordinatorInner {
    semaphore: Arc<Semaphore>,
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
            }),
        }
    }

    pub async fn acquire(&self) -> Result<OwnedSemaphorePermit, GroupEngineError> {
        self.inner
            .semaphore
            .clone()
            .acquire_owned()
            .await
            .map_err(|err| GroupEngineError::new(format!("snapshot build gate closed: {err}")))
    }
}

#[derive(Debug, Clone)]
pub struct SnapshotInstallCoordinator {
    inner: Arc<SnapshotInstallCoordinatorInner>,
}

#[derive(Debug)]
struct SnapshotInstallCoordinatorInner {
    semaphore: Arc<Semaphore>,
    prefetched: Mutex<BTreeMap<String, Arc<Vec<u8>>>>,
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
                prefetched: Mutex::new(BTreeMap::new()),
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
            } => {
                format!(
                    "{snapshot_id}:s3:{key}:{size_bytes}:{}:{compression:?}",
                    stored_size_bytes.unwrap_or(*size_bytes)
                )
            }
        }
    }

    pub fn cache_prefetched(
        &self,
        snapshot_id: &str,
        location: &SnapshotLocation,
        bytes: Vec<u8>,
    ) -> String {
        let key = Self::cache_key(snapshot_id, location);
        self.inner
            .prefetched
            .lock()
            .expect("snapshot install prefetch cache mutex")
            .insert(key.clone(), Arc::new(bytes));
        key
    }

    pub fn take_prefetched(&self, pointer: &SnapshotPointer) -> Option<Arc<Vec<u8>>> {
        let key = Self::cache_key(&pointer.snapshot_id, &pointer.location);
        self.clear_prefetched_key(&key)
    }

    pub fn clear_prefetched_key(&self, key: &str) -> Option<Arc<Vec<u8>>> {
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
    pub(crate) snapshot_store: SharedSnapshotStore,
    pub(crate) snapshot_build: SnapshotBuildCoordinator,
    pub(crate) snapshot_install: SnapshotInstallCoordinator,
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
        )
    }

    pub(crate) fn new_with_stores_and_snapshot_install(
        placement: ShardPlacement,
        metrics: Option<GroupEngineMetrics>,
        cold_store: Option<ColdStoreHandle>,
        snapshot_store: SharedSnapshotStore,
        snapshot_build: SnapshotBuildCoordinator,
        snapshot_install: SnapshotInstallCoordinator,
    ) -> Self {
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
            snapshot_store,
            snapshot_build,
            snapshot_install,
        }
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

    pub async fn delete_snapshot(
        &mut self,
        request: DeleteSnapshotRequest,
        placement: ShardPlacement,
    ) -> Result<(), GroupEngineError> {
        self.engine.delete_snapshot(request, placement).await
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
    ) -> Result<Vec<ColdGcEntry>, GroupEngineError> {
        self.engine.plan_cold_gc(max, placement).await
    }

    pub async fn check_create_stream_cold_admission(
        &mut self,
        request: CreateStreamRequest,
        placement: ShardPlacement,
        admission: ColdWriteAdmission,
    ) -> Result<(), GroupEngineError> {
        let _ = placement;
        self.engine.check_cold_write_admission_bytes(
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
        self.engine.check_cold_write_admission_bytes(
            &request.stream_id,
            admission,
            u64::try_from(request.payload.len()).expect("payload len fits u64"),
        )?;
        Ok(())
    }

    pub async fn check_append_batch_cold_admission(
        &mut self,
        request: AppendBatchRequest,
        placement: ShardPlacement,
        admission: ColdWriteAdmission,
    ) -> Result<(), GroupEngineError> {
        let _ = placement;
        let incoming_bytes = request
            .payloads
            .iter()
            .map(|payload| u64::try_from(payload.len()).expect("payload len fits u64"))
            .sum();
        self.engine.check_cold_write_admission_bytes(
            &request.stream_id,
            admission,
            incoming_bytes,
        )?;
        Ok(())
    }

    pub async fn check_append_batch_many_cold_admission(
        &mut self,
        requests: Vec<AppendBatchRequest>,
        placement: ShardPlacement,
        admission: ColdWriteAdmission,
    ) -> Result<(), GroupEngineError> {
        let _ = placement;
        if requests.is_empty() {
            return Ok(());
        }
        let stream_id = requests[0].stream_id.clone();
        let incoming_bytes = requests
            .iter()
            .flat_map(|request| request.payloads.iter())
            .map(|payload| u64::try_from(payload.len()).expect("payload len fits u64"))
            .sum();
        self.engine
            .check_cold_write_admission_bytes(&stream_id, admission, incoming_bytes)?;
        Ok(())
    }

    pub async fn install_group_snapshot(
        &mut self,
        snapshot: GroupSnapshot,
    ) -> Result<(), GroupEngineError> {
        self.engine.install_snapshot(snapshot).await
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

            let response = match entry.payload {
                EntryPayload::Blank => raft_blank_response(),
                EntryPayload::Normal(command) => {
                    let apply_started_at = Instant::now();
                    applied_entries += 1;
                    let response =
                        match group_write_command_from_proto(command).and_then(|command| {
                            self.engine.apply_committed_write(command, self.placement)
                        }) {
                            Ok(response) => raft_write_applied_response(response),
                            Err(err) => raft_write_rejected_response(err),
                        };
                    apply_ns = apply_ns.saturating_add(elapsed_ns(apply_started_at));
                    response
                }
                EntryPayload::Membership(membership) => {
                    self.last_membership = StoredMembershipOf::<UrsulaRaftTypeConfig>::new(
                        Some(entry.log_id),
                        membership,
                    );
                    raft_membership_response()
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

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        let build_permit = self
            .snapshot_build
            .acquire()
            .await
            .expect("snapshot build coordinator should not close");
        let snapshot = self
            .group_snapshot()
            .await
            .expect("in-memory group snapshot should not fail");
        RaftGroupSnapshotBuilder {
            placement: self.placement,
            snapshot,
            meta: self.snapshot_meta(),
            current_snapshot: self.current_snapshot.clone(),
            snapshot_store: self.snapshot_store.clone(),
            metrics: self.metrics.clone(),
            _build_permit: build_permit,
        }
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
        let pointer_bytes = snapshot.into_inner();
        let pointer = SnapshotPointer::decode(&pointer_bytes)
            .map_err(|err| invalid_data(io::Error::other(err.to_string())))?;
        let snapshot_bytes = match &pointer.location {
            SnapshotLocation::Inline { bytes } => Arc::new(bytes.clone()),
            location => {
                if let Some(bytes) = self.snapshot_install.take_prefetched(&pointer) {
                    bytes
                } else {
                    Arc::new(
                        self.snapshot_store
                            .download(location)
                            .await
                            .map_err(|err| err.into_io())?,
                    )
                }
            }
        };
        let group_snapshot =
            decode_group_snapshot(snapshot_bytes.as_slice()).map_err(|err| err.into_io())?;
        self.engine
            .install_snapshot(group_snapshot)
            .await
            .map_err(group_engine_io_error)?;
        self.last_applied_log_id = meta.last_log_id;
        self.last_membership = meta.last_membership.clone();
        *self.current_snapshot.lock().expect("snapshot mutex") = Some(CurrentSnapshot {
            meta: meta.clone(),
            pointer_bytes,
        });
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
    snapshot: GroupSnapshot,
    pub(crate) meta: SnapshotMetaOf<UrsulaRaftTypeConfig>,
    current_snapshot: Arc<Mutex<Option<CurrentSnapshot>>>,
    snapshot_store: SharedSnapshotStore,
    metrics: Option<GroupEngineMetrics>,
    _build_permit: OwnedSemaphorePermit,
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
            .upload_iter(key, group_snapshot_frames(self.snapshot.clone()))
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
                            bytes: group_snapshot_frames(self.snapshot.clone())
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
                    bytes: group_snapshot_frames(self.snapshot.clone())
                        .collect::<Result<Vec<_>, _>>()
                        .map_err(|err| err.into_io())?
                        .into_iter()
                        .flat_map(|chunk| chunk.to_vec())
                        .collect(),
                }
            }
        };
        let pointer = SnapshotPointer {
            snapshot_id,
            location,
        };
        let pointer_bytes = pointer.encode().map_err(|err| err.into_io())?;
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
        {
            let mut guard = self.current_snapshot.lock().expect("snapshot mutex");
            guard.replace(CurrentSnapshot {
                meta: self.meta.clone(),
                pointer_bytes: pointer_bytes.clone(),
            });
        }
        if let Err(err) = self
            .snapshot_store
            .prune_retired(
                self.placement.raft_group_id.0,
                &pointer.location,
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
            meta: self.meta.clone(),
            snapshot: Cursor::new(pointer_bytes),
        })
    }
}

#[cfg(test)]
mod tests {
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
            snapshot: test_group_snapshot(placement, 1),
            meta: test_snapshot_meta(1),
            current_snapshot: current_snapshot.clone(),
            snapshot_store: snapshot_store.clone(),
            metrics: None,
            _build_permit: test_build_permit().await,
        };
        let first_snapshot = first.build_snapshot().await.expect("first snapshot");
        let first_pointer =
            SnapshotPointer::decode(&first_snapshot.snapshot.into_inner()).expect("first pointer");

        let mut second = RaftGroupSnapshotBuilder {
            placement,
            snapshot: test_group_snapshot(placement, 2),
            meta: test_snapshot_meta(2),
            current_snapshot: current_snapshot.clone(),
            snapshot_store: snapshot_store.clone(),
            metrics: None,
            _build_permit: test_build_permit().await,
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
            snapshot: test_group_snapshot(placement, 3),
            meta: test_snapshot_meta(3),
            current_snapshot,
            snapshot_store,
            metrics: None,
            _build_permit: test_build_permit().await,
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
            snapshot: test_group_snapshot(placement, 3),
            meta: test_snapshot_meta(3),
            current_snapshot: current_snapshot.clone(),
            snapshot_store: Arc::new(FailingSnapshotStore),
            metrics: None,
            _build_permit: test_build_permit().await,
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
}

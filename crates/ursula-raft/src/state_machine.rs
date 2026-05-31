use futures_util::TryStreamExt;
use std::fmt::Debug;
use std::io;
use std::io::Cursor;
use std::sync::Arc;
use std::sync::Mutex;

use crate::rt::time::Instant;

use futures_util::Stream;
use openraft::EntryPayload;
use openraft::alias::LogIdOf;
use openraft::alias::SnapshotDataOf;
use openraft::alias::SnapshotMetaOf;
use openraft::alias::SnapshotOf;
use openraft::alias::StoredMembershipOf;
use openraft::storage::EntryResponder;
use openraft::storage::RaftSnapshotBuilder;
use openraft::storage::RaftStateMachine;
use ursula_runtime::{
    AppendBatchRequest, AppendRequest, BootstrapStreamRequest, BootstrapStreamResponse,
    ColdFlushCandidate, ColdGcEntry, ColdHotBacklog, ColdStoreHandle, ColdWriteAdmission,
    CreateStreamRequest, DeleteSnapshotRequest, GroupEngine, GroupEngineError, GroupEngineMetrics,
    GroupSnapshot, HeadStreamRequest, HeadStreamResponse, InMemoryGroupEngine,
    PlanColdFlushRequest, PlanGroupColdFlushRequest, ReadSnapshotRequest, ReadSnapshotResponse,
    ReadStreamRequest, ReadStreamResponse, SharedSnapshotStore, SnapshotKey, SnapshotPointer,
    default_snapshot_store,
};
use ursula_shard::BucketStreamId;
use ursula_shard::ShardPlacement;

use crate::codec::*;
use crate::engine::*;
use crate::log_store::*;
use crate::types::*;

#[derive(Debug, Clone)]
pub(crate) struct CurrentSnapshot {
    pub(crate) meta: SnapshotMetaOf<UrsulaRaftTypeConfig>,
    /// Bytes that ride through openraft's `SnapshotData`. With the default
    /// [`ursula_runtime::InlineSnapshotStore`] this is the full snapshot; with
    /// out-of-line backends (Local/S3) this is a tiny [`SnapshotPointer`].
    pointer_bytes: Vec<u8>,
}

pub struct RaftGroupStateMachine {
    pub(crate) placement: ShardPlacement,
    pub(crate) engine: InMemoryGroupEngine,
    pub(crate) metrics: Option<GroupEngineMetrics>,
    pub(crate) last_applied_log_id: Option<LogIdOf<UrsulaRaftTypeConfig>>,
    pub(crate) last_membership: StoredMembershipOf<UrsulaRaftTypeConfig>,
    pub(crate) current_snapshot: Arc<Mutex<Option<CurrentSnapshot>>>,
    pub(crate) snapshot_store: SharedSnapshotStore,
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

    pub async fn plan_next_cold_flush(
        &mut self,
        request: PlanGroupColdFlushRequest,
        placement: ShardPlacement,
    ) -> Result<Option<ColdFlushCandidate>, GroupEngineError> {
        self.engine.plan_next_cold_flush(request, placement).await
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
        let mut preview = self.engine.clone();
        preview
            .create_stream_with_cold_admission(request, placement, admission)
            .await?;
        Ok(())
    }

    pub async fn check_append_cold_admission(
        &mut self,
        request: AppendRequest,
        placement: ShardPlacement,
        admission: ColdWriteAdmission,
    ) -> Result<(), GroupEngineError> {
        let mut preview = self.engine.clone();
        preview
            .append_with_cold_admission(request, placement, admission)
            .await?;
        Ok(())
    }

    pub async fn check_append_batch_cold_admission(
        &mut self,
        request: AppendBatchRequest,
        placement: ShardPlacement,
        admission: ColdWriteAdmission,
    ) -> Result<(), GroupEngineError> {
        let mut preview = self.engine.clone();
        preview
            .append_batch_with_cold_admission(request, placement, admission)
            .await?;
        Ok(())
    }

    pub async fn check_append_batch_many_cold_admission(
        &mut self,
        requests: Vec<AppendBatchRequest>,
        placement: ShardPlacement,
        admission: ColdWriteAdmission,
    ) -> Result<(), GroupEngineError> {
        let mut preview = self.engine.clone();
        for request in requests {
            preview
                .append_batch_with_cold_admission(request, placement, admission)
                .await?;
        }
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
    where
        Strm: Stream<Item = Result<EntryResponder<UrsulaRaftTypeConfig>, io::Error>>
            + Unpin
            + openraft::OptionalSend,
    {
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
        let snapshot_bytes = self
            .snapshot_store
            .download(&pointer.location)
            .await
            .map_err(|err| err.into_io())?;
        let group_snapshot: GroupSnapshot =
            serde_json::from_slice(&snapshot_bytes).map_err(invalid_data)?;
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
}

impl RaftSnapshotBuilder<UrsulaRaftTypeConfig> for RaftGroupSnapshotBuilder {
    async fn build_snapshot(&mut self) -> Result<SnapshotOf<UrsulaRaftTypeConfig>, io::Error> {
        let body = serde_json::to_vec(&self.snapshot).map_err(invalid_data)?;
        let key = SnapshotKey {
            raft_group_id: self.placement.raft_group_id.0,
            snapshot_id: self.meta.snapshot_id.clone(),
        };
        let location = self
            .snapshot_store
            .upload(key, body)
            .await
            .map_err(|err| err.into_io())?;
        // Re-stat immediately so a silent partial-success (multipart Complete
        // failing after the parts uploaded, opendal retry caching, etc.) is
        // caught HERE rather than 10 minutes later as an install_snapshot
        // NotFound on a follower. Cheap relative to the upload itself.
        self.snapshot_store
            .verify_uploaded(&location)
            .await
            .map_err(|err| err.into_io())?;
        let pointer = SnapshotPointer {
            snapshot_id: self.meta.snapshot_id.clone(),
            location,
        };
        let pointer_bytes = pointer.encode().map_err(|err| err.into_io())?;
        let previous = {
            let mut guard = self.current_snapshot.lock().expect("snapshot mutex");
            guard.replace(CurrentSnapshot {
                meta: self.meta.clone(),
                pointer_bytes: pointer_bytes.clone(),
            })
        };
        schedule_previous_snapshot_gc(self.snapshot_store.clone(), previous, &pointer_bytes);
        Ok(SnapshotOf::<UrsulaRaftTypeConfig> {
            meta: self.meta.clone(),
            snapshot: Cursor::new(pointer_bytes),
        })
    }
}

/// Number of seconds to wait before deleting the previous snapshot's bytes
/// after a new one has been published. Lets in-flight `install_snapshot`
/// downloads complete before the underlying object disappears.
#[cfg(not(madsim))]
fn snapshot_gc_grace_secs() -> u64 {
    std::env::var("URSULA_SNAPSHOT_GC_GRACE_SECS")
        .ok()
        .and_then(|raw| raw.parse::<u64>().ok())
        .unwrap_or(300)
}

#[cfg(not(madsim))]
fn schedule_previous_snapshot_gc(
    store: ursula_runtime::SharedSnapshotStore,
    previous: Option<CurrentSnapshot>,
    new_pointer_bytes: &[u8],
) {
    let Some(previous) = previous else { return };
    let Ok(prev_pointer) = SnapshotPointer::decode(&previous.pointer_bytes) else {
        return;
    };
    // Inline locations are kept in-pointer; nothing to GC out-of-line.
    if matches!(
        prev_pointer.location,
        ursula_runtime::SnapshotLocation::Inline { .. }
    ) {
        return;
    }
    // Same-key self-GC guard: snapshot_id is derived from last_applied_log_id
    // (state_machine.rs::snapshot_meta), so two consecutive builds that race
    // an apply-idle interval produce the SAME key. Without this check we'd
    // schedule a GC against the very object we just wrote — `delete` runs 300s
    // later and silently nukes the current snapshot, leaving `current_snapshot`
    // pointing at a 404. This is exactly how N3 wedged group-4 transfers on
    // 2026-05-31 (snapshot_id collided across repeated 15s driver ticks while
    // apply was hot-tier-blocked, the GC nuked the live object).
    if let Ok(new_pointer) = SnapshotPointer::decode(new_pointer_bytes)
        && new_pointer.location == prev_pointer.location
    {
        return;
    }
    let grace = std::time::Duration::from_secs(snapshot_gc_grace_secs());
    tokio::spawn(async move {
        tokio::time::sleep(grace).await;
        if let Err(err) = store.delete(&prev_pointer.location).await {
            eprintln!(
                "ursula raft snapshot gc: delete of previous {} failed: {err}",
                prev_pointer.snapshot_id,
            );
        }
    });
}

#[cfg(madsim)]
fn schedule_previous_snapshot_gc(
    _store: ursula_runtime::SharedSnapshotStore,
    _previous: Option<CurrentSnapshot>,
    _new_pointer_bytes: &[u8],
) {
    // madsim has no fs/network store path that needs deferred GC; the inline
    // backend keeps everything in-pointer so this is always a no-op there.
}

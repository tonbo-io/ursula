use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
#[cfg(not(madsim))]
use std::time::SystemTime;
#[cfg(not(madsim))]
use std::time::UNIX_EPOCH;

#[cfg(not(madsim))]
use tokio::task::JoinSet;
use ursula_shard::BucketStreamId;
use ursula_shard::CoreId;
use ursula_shard::RaftGroupId;
use ursula_shard::ShardId;
use ursula_shard::ShardPlacement;
use ursula_shard::StaticShardMap;
use ursula_stream::ColdChunkRef;
use ursula_stream::ColdFlushCandidate;
use ursula_stream::ColdGcEntry;
use ursula_stream::ColdGcTarget;

use crate::admission::RaftUncommittedAdmission;
use crate::admission::RaftUncommittedBytesTracker;
use crate::cold_index::ColdStoreColdIndexPageStore;
use crate::cold_index::cold_index_prefix;
use crate::cold_index::load_cold_chunks_from_pages;
use crate::cold_index::select_cold_chunk_compaction;
use crate::cold_store::ColdStoreHandle;
use crate::cold_store::ColdStoreInfo;
use crate::cold_store::cold_chunk_prefix;
use crate::cold_store::new_cold_chunk_path;
use crate::command::GroupSnapshot;
use crate::core_worker::CoreCommand;
use crate::core_worker::CoreMailbox;
use crate::core_worker::CoreWorker;
use crate::core_worker::WaitReadCancel;
use crate::engine::GroupEngineFactory;
use crate::engine::in_memory::InMemoryGroupEngineFactory;
use crate::error::RuntimeError;
use crate::group_actor::GroupCommand;
use crate::metrics::COLD_FLUSH_GROUP_BATCH_MAX_CHUNKS;
use crate::metrics::RuntimeMailboxSnapshot;
use crate::metrics::RuntimeMetrics;
use crate::metrics::RuntimeMetricsInner;
use crate::metrics::append_batch_payload_bytes;
use crate::metrics::elapsed_ns;
use crate::metrics::is_stale_cold_flush_candidate_error;
use crate::request::AckColdGcResponse;
use crate::request::AppendBatchRequest;
use crate::request::AppendBatchResponse;
use crate::request::AppendExternalRequest;
use crate::request::AppendRequest;
use crate::request::AppendResponse;
use crate::request::BootstrapStreamRequest;
use crate::request::BootstrapStreamResponse;
use crate::request::CloseStreamRequest;
use crate::request::CloseStreamResponse;
use crate::request::ColdWriteAdmission;
use crate::request::CompactColdRequest;
use crate::request::CompactColdResponse;
use crate::request::CreateStreamExternalRequest;
use crate::request::CreateStreamRequest;
use crate::request::CreateStreamResponse;
use crate::request::DeleteSnapshotRequest;
use crate::request::DeleteStreamRequest;
use crate::request::DeleteStreamResponse;
use crate::request::FlushColdRequest;
use crate::request::FlushColdResponse;
use crate::request::GetStreamAttrsRequest;
use crate::request::GetStreamAttrsResponse;
use crate::request::HeadStreamRequest;
use crate::request::HeadStreamResponse;
use crate::request::PlanColdFlushRequest;
use crate::request::PlanGroupColdFlushRequest;
use crate::request::PublishSnapshotRequest;
use crate::request::PublishSnapshotResponse;
use crate::request::ReadSnapshotRequest;
use crate::request::ReadSnapshotResponse;
use crate::request::ReadStreamRequest;
use crate::request::ReadStreamResponse;
use crate::request::UpdateStreamAttrsRequest;
use crate::request::UpdateStreamAttrsResponse;
use crate::rt::sync::Semaphore;
use crate::rt::sync::mpsc;
use crate::rt::sync::oneshot;
use crate::rt::time::Instant;
use crate::trace::Traced;

#[derive(Debug, Clone)]
pub struct RuntimeConfig {
    pub core_count: usize,
    pub raft_group_count: usize,
    pub mailbox_capacity: usize,
    pub threading: RuntimeThreading,
    pub cold_max_hot_bytes_per_group: Option<u64>,
    /// Per-group cap on raft-submitted-but-not-yet-applied payload bytes.
    /// `None` disables the admission (default). Catches "raft replication slow"
    /// before in-memory queues grow unbounded.
    pub raft_max_uncommitted_bytes_per_group: Option<u64>,
    pub live_read_max_waiters_per_core: Option<u64>,
}

impl RuntimeConfig {
    pub fn new(core_count: usize, raft_group_count: usize) -> Self {
        #[cfg(not(madsim))]
        let threading = RuntimeThreading::ThreadPerCore;
        #[cfg(madsim)]
        let threading = RuntimeThreading::HostedTokio;
        Self {
            core_count,
            raft_group_count,
            mailbox_capacity: 1024,
            threading,
            cold_max_hot_bytes_per_group: None,
            raft_max_uncommitted_bytes_per_group: None,
            live_read_max_waiters_per_core: Some(65_536),
        }
    }

    pub fn with_cold_max_hot_bytes_per_group(mut self, value: Option<u64>) -> Self {
        self.cold_max_hot_bytes_per_group = value;
        self
    }

    pub fn with_raft_max_uncommitted_bytes_per_group(mut self, value: Option<u64>) -> Self {
        self.raft_max_uncommitted_bytes_per_group = value;
        self
    }

    pub fn with_live_read_max_waiters_per_core(mut self, value: Option<u64>) -> Self {
        self.live_read_max_waiters_per_core = value;
        self
    }

    /// Build runtime configuration from a typed `ursula_config::RuntimeConfig`.
    pub fn from_ursula_config(cfg: &ursula_config::RuntimeConfig, raft_group_count: usize) -> Self {
        let mut config = Self::new(cfg.core_count, raft_group_count);
        config.live_read_max_waiters_per_core = cfg
            .live_read_max_waiters_per_core
            .and_then(|n| if n == 0 { None } else { Some(n as u64) });
        config
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimeThreading {
    #[cfg(not(madsim))]
    ThreadPerCore,
    HostedTokio,
}

#[derive(Debug, Clone)]
pub struct ShardRuntime {
    shard_map: StaticShardMap,
    mailboxes: Vec<CoreMailbox>,
    metrics: Arc<RuntimeMetricsInner>,
    next_waiter_id: Arc<AtomicU64>,
    cold_store: Option<ColdStoreHandle>,
}

impl ShardRuntime {
    pub fn spawn(config: RuntimeConfig) -> Result<Self, RuntimeError> {
        Self::spawn_with_engine_factory(config, InMemoryGroupEngineFactory::default())
    }

    pub fn spawn_with_engine_factory(
        config: RuntimeConfig,
        engine_factory: impl GroupEngineFactory,
    ) -> Result<Self, RuntimeError> {
        Self::spawn_with_engine_factory_and_cold_store(config, engine_factory, None)
    }

    pub fn spawn_with_engine_factory_and_cold_store(
        config: RuntimeConfig,
        engine_factory: impl GroupEngineFactory,
        cold_store: Option<ColdStoreHandle>,
    ) -> Result<Self, RuntimeError> {
        let shard_map = StaticShardMap::new(config.core_count, config.raft_group_count)?;
        let metrics = Arc::new(RuntimeMetricsInner::new(
            usize::from(shard_map.core_count()),
            usize::try_from(shard_map.raft_group_count()).expect("u32 fits usize"),
        ));
        let cold_write_admission = ColdWriteAdmission {
            max_hot_bytes_per_group: config.cold_max_hot_bytes_per_group,
        };
        let raft_uncommitted_admission = RaftUncommittedAdmission {
            max_uncommitted_bytes_per_group: config.raft_max_uncommitted_bytes_per_group,
        };
        let raft_uncommitted_bytes = Arc::new(RaftUncommittedBytesTracker::new(
            usize::try_from(shard_map.raft_group_count()).expect("u32 fits usize"),
        ));
        let engine_factory: Arc<dyn GroupEngineFactory> = Arc::new(engine_factory);
        let read_materialization = Arc::new(Semaphore::new(config.mailbox_capacity.max(1)));
        let mut mailboxes = Vec::with_capacity(usize::from(shard_map.core_count()));
        for raw_core_id in 0..shard_map.core_count() {
            let core_id = CoreId(raw_core_id);
            let (tx, rx) = mpsc::channel(config.mailbox_capacity.max(1));
            let worker = CoreWorker {
                core_id,
                rx,
                engine_factory: engine_factory.clone(),
                groups: HashMap::new(),
                metrics: metrics.clone(),
                group_mailbox_capacity: config.mailbox_capacity.max(1),
                cold_write_admission,
                raft_uncommitted_admission,
                raft_uncommitted_bytes: raft_uncommitted_bytes.clone(),
                live_read_max_waiters_per_core: config.live_read_max_waiters_per_core,
                read_materialization: read_materialization.clone(),
            };
            spawn_core_worker(config.threading, worker)?;
            mailboxes.push(CoreMailbox { core_id, tx });
        }
        Ok(Self {
            shard_map,
            mailboxes,
            metrics,
            next_waiter_id: Arc::new(AtomicU64::new(1)),
            cold_store,
        })
    }

    pub fn locate(&self, stream_id: &BucketStreamId) -> ShardPlacement {
        self.shard_map.locate(stream_id)
    }

    pub fn has_cold_store(&self) -> bool {
        self.cold_store.is_some()
    }

    pub fn cold_store(&self) -> Option<ColdStoreHandle> {
        self.cold_store.clone()
    }

    pub fn cold_store_info(&self) -> Option<ColdStoreInfo> {
        self.cold_store
            .as_ref()
            .map(|cold_store| cold_store.info().clone())
    }

    pub async fn wait_read_stream(
        &self,
        request: ReadStreamRequest,
    ) -> Result<ReadStreamResponse, RuntimeError> {
        let placement = self.shard_map.locate(&request.stream_id);
        let mailbox = &self.mailboxes[usize::from(placement.core_id.0)];
        let waiter_id = self.next_waiter_id.fetch_add(1, Ordering::Relaxed);
        let stream_id = request.stream_id.clone();
        let (response_tx, response_rx) = oneshot::channel();
        self.enqueue_core_command(mailbox, CoreCommand::Group {
            placement,
            admission: None,
            command: GroupCommand::WaitRead {
                request,
                waiter_id,
                response_tx,
            },
        })
        .await?;
        let mut cancel = WaitReadCancel::new(mailbox.tx.clone(), stream_id, placement, waiter_id);
        let response = response_rx
            .await
            .map_err(|_| RuntimeError::ResponseDropped {
                core_id: mailbox.core_id,
            })?;
        cancel.disarm();
        response
    }

    pub async fn require_local_live_read_owner(
        &self,
        stream_id: &BucketStreamId,
    ) -> Result<(), RuntimeError> {
        let placement = self.shard_map.locate(stream_id);
        let (response_tx, response_rx) = oneshot::channel();
        self.group_rpc(
            placement,
            None,
            GroupCommand::RequireLiveReadOwner { response_tx },
            response_rx,
        )
        .await
    }

    pub async fn flush_cold_once(
        &self,
        request: PlanColdFlushRequest,
    ) -> Result<Option<FlushColdResponse>, RuntimeError> {
        let Some(candidate) = self.plan_cold_flush(request).await? else {
            return Ok(None);
        };
        self.flush_cold_candidate(candidate).await.map(Some)
    }

    pub async fn flush_cold_group_once(
        &self,
        raft_group_id: RaftGroupId,
        request: PlanGroupColdFlushRequest,
    ) -> Result<Option<FlushColdResponse>, RuntimeError> {
        let mut candidates = self
            .plan_next_cold_flush_batch(raft_group_id, request, 1)
            .await?;
        let Some(candidate) = candidates.pop() else {
            return Ok(None);
        };
        match self.flush_cold_candidate(candidate).await {
            Ok(response) => Ok(Some(response)),
            Err(err) if is_stale_cold_flush_candidate_error(&err) => Ok(None),
            Err(err) => Err(err),
        }
    }

    pub async fn flush_cold_group_batch_once(
        &self,
        raft_group_id: RaftGroupId,
        request: PlanGroupColdFlushRequest,
        max_candidates: usize,
    ) -> Result<Vec<FlushColdResponse>, RuntimeError> {
        let candidates = self
            .plan_next_cold_flush_batch(raft_group_id, request, max_candidates)
            .await?;
        if candidates.is_empty() {
            return Ok(Vec::new());
        }
        self.flush_cold_candidates_batch(candidates).await
    }

    async fn flush_cold_candidate(
        &self,
        candidate: ColdFlushCandidate,
    ) -> Result<FlushColdResponse, RuntimeError> {
        let Some(cold_store) = self.cold_store.as_ref() else {
            return Err(RuntimeError::ColdStoreConfig {
                message: "cold backend must be configured before flushing cold chunks".to_owned(),
            });
        };
        let path = new_cold_chunk_path(
            &candidate.stream_id,
            candidate.start_offset,
            candidate.end_offset,
        );
        let upload_started_at = Instant::now();
        let object_size = match cold_store.write_chunk(&path, &candidate.payload).await {
            Ok(object_size) => object_size,
            Err(err) => {
                // Surfaces "this node can't write to S3" to the snapshot driver's
                // health/yield logic, which drives leadership-yield off real cold
                // flush failures rather than a stat probe a keep-alive connection
                // can mask.
                self.metrics.record_cold_flush_write_error();
                return Err(RuntimeError::ColdStoreIo {
                    message: err.to_string(),
                });
            }
        };
        self.metrics
            .record_cold_upload(object_size, elapsed_ns(upload_started_at));
        let chunk = ColdChunkRef {
            start_offset: candidate.start_offset,
            end_offset: candidate.end_offset,
            s3_path: path.clone(),
            object_size,
        };
        let publish_started_at = Instant::now();
        let publish = self
            .flush_cold(FlushColdRequest {
                stream_id: candidate.stream_id,
                chunk,
            })
            .await;
        match publish {
            Ok(response) => {
                self.metrics
                    .record_cold_publish(object_size, elapsed_ns(publish_started_at));
                Ok(response)
            }
            Err(err) => Err(err),
        }
    }

    pub(crate) async fn flush_cold_candidates_batch(
        &self,
        candidates: Vec<ColdFlushCandidate>,
    ) -> Result<Vec<FlushColdResponse>, RuntimeError> {
        let mut responses = Vec::with_capacity(candidates.len());
        for candidate in candidates {
            match self.flush_cold_candidate(candidate).await {
                Ok(response) => responses.push(response),
                Err(err) if is_stale_cold_flush_candidate_error(&err) => {}
                Err(err) => return Err(err),
            }
        }
        Ok(responses)
    }

    #[cfg(madsim)]
    pub async fn flush_cold_candidates_batch_for_simulation(
        &self,
        candidates: Vec<ColdFlushCandidate>,
    ) -> Result<Vec<FlushColdResponse>, RuntimeError> {
        self.flush_cold_candidates_batch(candidates).await
    }

    pub async fn flush_cold_all_groups_once(
        &self,
        request: PlanGroupColdFlushRequest,
    ) -> Result<usize, RuntimeError> {
        self.flush_cold_all_groups_once_bounded(request, 1).await
    }

    pub async fn flush_cold_all_groups_once_bounded(
        &self,
        request: PlanGroupColdFlushRequest,
        max_concurrency: usize,
    ) -> Result<usize, RuntimeError> {
        let max_concurrency = max_concurrency.max(1);
        if max_concurrency == 1 {
            return self.flush_cold_all_groups_once_serial(request).await;
        }
        #[cfg(madsim)]
        {
            return self.flush_cold_all_groups_once_serial(request).await;
        }
        #[cfg(not(madsim))]
        {
            let mut flushed = 0;
            let mut next_group_id = 0;
            let group_count = self.shard_map.raft_group_count();
            let mut tasks = JoinSet::new();

            while next_group_id < group_count || !tasks.is_empty() {
                while next_group_id < group_count && tasks.len() < max_concurrency {
                    let runtime = self.clone();
                    let request = request.clone();
                    let group_id = RaftGroupId(next_group_id);
                    next_group_id += 1;
                    tasks.spawn(async move {
                        runtime
                            .flush_cold_group_batch_once(
                                group_id,
                                request,
                                COLD_FLUSH_GROUP_BATCH_MAX_CHUNKS,
                            )
                            .await
                            .map(|responses| responses.len())
                    });
                }
                if let Some(result) = tasks.join_next().await {
                    match result {
                        Ok(Ok(count)) => flushed += count,
                        Ok(Err(err)) => return Err(err),
                        Err(err) => {
                            return Err(RuntimeError::ColdStoreIo {
                                message: format!("cold flush task failed: {err}"),
                            });
                        }
                    }
                }
            }
            Ok(flushed)
        }
    }

    async fn flush_cold_all_groups_once_serial(
        &self,
        request: PlanGroupColdFlushRequest,
    ) -> Result<usize, RuntimeError> {
        let mut flushed = 0;
        for group_id in 0..self.shard_map.raft_group_count() {
            flushed += self
                .flush_cold_group_batch_once(
                    RaftGroupId(group_id),
                    request.clone(),
                    COLD_FLUSH_GROUP_BATCH_MAX_CHUNKS,
                )
                .await?
                .len();
        }
        Ok(flushed)
    }

    /// Rewrites undersized, contiguous objects from the same stream into
    /// target-sized immutable chunks. Discovery reads only cold-index pages.
    pub async fn compact_cold_once(
        &self,
        target_bytes: u64,
        max_bytes: u64,
        max_streams: usize,
        gc_grace_ms: u64,
    ) -> Result<usize, RuntimeError> {
        let Some(cold_store) = self.cold_store.as_ref() else {
            return Ok(0);
        };
        let pages =
            cold_store
                .list_cold_index_pages()
                .await
                .map_err(|err| RuntimeError::ColdStoreIo {
                    message: err.to_string(),
                })?;
        let mut pages_by_stream: HashMap<BucketStreamId, Vec<_>> = HashMap::new();
        for page in pages {
            if page.generation == 0 {
                pages_by_stream
                    .entry(page.stream_id.clone())
                    .or_default()
                    .push(page);
            }
        }
        let store = ColdStoreColdIndexPageStore::new(cold_store.clone());
        let mut compacted = 0;
        for (stream_id, stream_pages) in pages_by_stream {
            if compacted >= max_streams {
                break;
            }
            // Only the local Raft leader may publish a replacement.
            if self
                .require_local_live_read_owner(&stream_id)
                .await
                .is_err()
            {
                continue;
            }
            let chunks = load_cold_chunks_from_pages(&store, &stream_pages)
                .await
                .map_err(|err| RuntimeError::ColdStoreIo {
                    message: err.to_string(),
                })?;
            let Some(old_chunks) = select_cold_chunk_compaction(&chunks, target_bytes, max_bytes)
            else {
                continue;
            };
            let total_bytes = old_chunks
                .iter()
                .try_fold(0_u64, |total, chunk| total.checked_add(chunk.object_size))
                .ok_or_else(|| RuntimeError::ColdStoreIo {
                    message: "cold compaction byte count overflow".to_owned(),
                })?;
            let capacity = usize::try_from(total_bytes).map_err(|_| RuntimeError::ColdStoreIo {
                message: "cold compaction object exceeds addressable memory".to_owned(),
            })?;
            let mut payload = Vec::with_capacity(capacity);
            for chunk in &old_chunks {
                let len =
                    usize::try_from(chunk.object_size).map_err(|_| RuntimeError::ColdStoreIo {
                        message: "cold chunk exceeds addressable memory".to_owned(),
                    })?;
                let bytes = cold_store
                    .read_chunk_range(chunk, chunk.start_offset, len)
                    .await
                    .map_err(|err| RuntimeError::ColdStoreIo {
                        message: err.to_string(),
                    })?;
                payload.extend_from_slice(&bytes);
            }
            let first = old_chunks
                .first()
                .expect("candidate contains at least two chunks");
            let last = old_chunks
                .last()
                .expect("candidate contains at least two chunks");
            let path = new_cold_chunk_path(&stream_id, first.start_offset, last.end_offset);
            let object_size = cold_store
                .write_chunk(&path, &payload)
                .await
                .map_err(|err| RuntimeError::ColdStoreIo {
                    message: err.to_string(),
                })?;
            let replacement = ColdChunkRef {
                start_offset: first.start_offset,
                end_offset: last.end_offset,
                object_size,
                s3_path: path,
            };
            let replacement_path = replacement.s3_path.clone();
            let gc_not_before_ms = unix_time_ms().saturating_add(gc_grace_ms);
            let compact_result = self
                .compact_cold(CompactColdRequest {
                    stream_id: stream_id.clone(),
                    old_chunks,
                    replacement,
                    gc_not_before_ms,
                })
                .await;
            if let Err(err) = compact_result {
                let rollback_safe =
                    err.leader_hint().is_some() || err.stream_error_code().is_some();
                if !rollback_safe {
                    return Err(err);
                }
                if let Err(cleanup_err) = cold_store.delete_chunk(&replacement_path).await {
                    tracing::warn!(
                        stream = %stream_id,
                        path = %replacement_path,
                        error = %cleanup_err,
                        "failed to remove unpublished cold compaction replacement"
                    );
                }
                tracing::warn!(
                    stream = %stream_id,
                    error = %err,
                    "cold compaction publish failed; continuing with remaining streams"
                );
                continue;
            }
            compacted += 1;
        }
        Ok(compacted)
    }

    /// Drains the leader-side cold-GC queue for one group: physically reclaims
    /// each queued target from cold storage, then replicates an ack that pops
    /// the reclaimed entries. Deletions are idempotent, so a crash or leader
    /// change between reclaim and ack simply re-runs them next tick.
    pub async fn run_cold_gc_group_once(
        &self,
        raft_group_id: RaftGroupId,
        max_entries: usize,
    ) -> Result<usize, RuntimeError> {
        let Some(cold_store) = self.cold_store.as_ref() else {
            return Ok(0);
        };
        let entries = self.plan_cold_gc(raft_group_id, max_entries).await?;
        if entries.is_empty() {
            return Ok(0);
        }
        let mut acked_seq = None;
        let mut reclaimed = 0usize;
        // Entries are FIFO by seq; stop at the first failure so the ack never
        // skips past an object that is still present in cold storage.
        for entry in entries {
            if entry.not_before_ms > unix_time_ms() {
                break;
            }
            let result = match &entry.target {
                ColdGcTarget::Stream(stream_id) => {
                    match cold_store.remove_all(&cold_chunk_prefix(stream_id)).await {
                        Ok(()) => cold_store.remove_all(&cold_index_prefix(stream_id)).await,
                        Err(err) => Err(err),
                    }
                }
                ColdGcTarget::Paths(paths) => {
                    let mut outcome = Ok(());
                    for path in paths {
                        if let Err(err) = cold_store.delete_chunk(path).await {
                            outcome = Err(err);
                            break;
                        }
                    }
                    outcome
                }
            };
            match result {
                Ok(()) => {
                    acked_seq = Some(entry.seq);
                    reclaimed += 1;
                }
                Err(err) => {
                    self.metrics.record_cold_gc_error();
                    if acked_seq.is_none() {
                        return Err(RuntimeError::ColdStoreIo {
                            message: err.to_string(),
                        });
                    }
                    break;
                }
            }
        }
        if let Some(up_to_seq) = acked_seq {
            self.ack_cold_gc(raft_group_id, up_to_seq).await?;
            self.metrics
                .record_cold_gc_reclaimed(u64::try_from(reclaimed).expect("reclaimed fits u64"));
        }
        Ok(reclaimed)
    }

    pub async fn run_cold_gc_all_groups_once(
        &self,
        max_entries_per_group: usize,
    ) -> Result<usize, RuntimeError> {
        if self.cold_store.is_none() {
            return Ok(0);
        }
        let mut reclaimed = 0;
        for group_id in 0..self.shard_map.raft_group_count() {
            reclaimed += self
                .run_cold_gc_group_once(RaftGroupId(group_id), max_entries_per_group)
                .await?;
        }
        Ok(reclaimed)
    }

    pub async fn install_group_snapshot(
        &self,
        snapshot: GroupSnapshot,
    ) -> Result<(), RuntimeError> {
        let expected = self.placement_for_group(snapshot.placement.raft_group_id)?;
        if snapshot.placement != expected {
            return Err(RuntimeError::SnapshotPlacementMismatch {
                expected,
                actual: snapshot.placement,
            });
        }
        let (response_tx, response_rx) = oneshot::channel();
        self.group_rpc(
            expected,
            None,
            GroupCommand::InstallGroupSnapshot {
                snapshot,
                response_tx,
            },
            response_rx,
        )
        .await
    }

    #[cfg(madsim)]
    pub async fn shutdown_group_engine_for_simulation(
        &self,
        placement: ShardPlacement,
    ) -> Result<(), RuntimeError> {
        let expected = self.placement_for_group(placement.raft_group_id)?;
        if placement != expected {
            return Err(RuntimeError::SnapshotPlacementMismatch {
                expected,
                actual: placement,
            });
        }
        let mailbox = &self.mailboxes[usize::from(placement.core_id.0)];
        let (response_tx, response_rx) = oneshot::channel();
        self.send_core_command(
            mailbox,
            CoreCommand::ShutdownGroupEngine {
                placement,
                response_tx,
            },
            response_rx,
        )
        .await
    }

    #[cfg(madsim)]
    pub async fn install_group_engine_for_simulation(
        &self,
        placement: ShardPlacement,
        engine: Box<dyn crate::engine::GroupEngine>,
    ) -> Result<(), RuntimeError> {
        let expected = self.placement_for_group(placement.raft_group_id)?;
        if placement != expected {
            return Err(RuntimeError::SnapshotPlacementMismatch {
                expected,
                actual: placement,
            });
        }
        let mailbox = &self.mailboxes[usize::from(placement.core_id.0)];
        let (response_tx, response_rx) = oneshot::channel();
        self.send_core_command(
            mailbox,
            CoreCommand::InstallGroupEngine {
                placement,
                engine,
                response_tx,
            },
            response_rx,
        )
        .await
    }

    pub async fn warm_group(
        &self,
        raft_group_id: RaftGroupId,
    ) -> Result<ShardPlacement, RuntimeError> {
        let placement = self.placement_for_group(raft_group_id)?;
        let mailbox = &self.mailboxes[usize::from(placement.core_id.0)];
        let (response_tx, response_rx) = oneshot::channel();
        self.send_core_command(
            mailbox,
            CoreCommand::WarmGroup {
                placement,
                response_tx,
            },
            response_rx,
        )
        .await
    }

    pub async fn warm_all_groups(&self) -> Result<(), RuntimeError> {
        let mut placements_by_core = vec![Vec::new(); self.mailboxes.len()];
        for raw_group_id in 0..self.shard_map.raft_group_count() {
            let placement = self.placement_for_group(RaftGroupId(raw_group_id))?;
            placements_by_core[usize::from(placement.core_id.0)].push(placement);
        }
        let mut responses = Vec::new();
        for (mailbox, placements) in self.mailboxes.iter().zip(placements_by_core) {
            let (response_tx, response_rx) = oneshot::channel();
            self.enqueue_core_command(mailbox, CoreCommand::WarmGroups {
                placements,
                response_tx,
            })
            .await?;
            responses.push((mailbox.core_id, response_rx));
        }
        for (core_id, response_rx) in responses {
            response_rx
                .await
                .map_err(|_| RuntimeError::ResponseDropped { core_id })??;
        }
        Ok(())
    }

    fn placement_for_group(
        &self,
        raft_group_id: RaftGroupId,
    ) -> Result<ShardPlacement, RuntimeError> {
        if raft_group_id.0 >= self.shard_map.raft_group_count() {
            return Err(RuntimeError::InvalidRaftGroup {
                raft_group_id,
                raft_group_count: self.shard_map.raft_group_count(),
            });
        }
        Ok(ShardPlacement {
            core_id: CoreId(
                (raft_group_id.0 % u32::from(self.shard_map.core_count()))
                    .try_into()
                    .expect("core id fits u16"),
            ),
            shard_id: ShardId(raft_group_id.0),
            raft_group_id,
        })
    }

    /// Routes a group command to its owning core and awaits the reply.
    /// `admission` carries the incoming payload bytes for
    /// raft-uncommitted-backpressure-guarded writes; the check itself runs on
    /// the owning core.
    async fn group_rpc<T>(
        &self,
        placement: ShardPlacement,
        admission: Option<u64>,
        command: GroupCommand,
        response_rx: oneshot::Receiver<Result<T, RuntimeError>>,
    ) -> Result<T, RuntimeError> {
        let mailbox = &self.mailboxes[usize::from(placement.core_id.0)];
        self.send_core_command(
            mailbox,
            CoreCommand::Group {
                placement,
                admission,
                command,
            },
            response_rx,
        )
        .await
    }

    async fn send_core_command<T>(
        &self,
        mailbox: &CoreMailbox,
        command: CoreCommand,
        response_rx: oneshot::Receiver<Result<T, RuntimeError>>,
    ) -> Result<T, RuntimeError> {
        self.enqueue_core_command(mailbox, command).await?;
        response_rx
            .await
            .map_err(|_| RuntimeError::ResponseDropped {
                core_id: mailbox.core_id,
            })?
    }

    async fn enqueue_core_command(
        &self,
        mailbox: &CoreMailbox,
        command: CoreCommand,
    ) -> Result<(), RuntimeError> {
        if mailbox.tx.capacity() == 0 {
            self.metrics.record_mailbox_full(mailbox.core_id);
        }
        let started_at = Instant::now();
        mailbox
            .tx
            .send(Traced::capture(command))
            .await
            .map_err(|_| RuntimeError::MailboxClosed {
                core_id: mailbox.core_id,
            })?;
        self.metrics
            .record_routed_request(mailbox.core_id, elapsed_ns(started_at));
        Ok(())
    }

    pub fn metrics(&self) -> RuntimeMetrics {
        RuntimeMetrics {
            inner: self.metrics.clone(),
        }
    }

    pub fn mailbox_snapshot(&self) -> RuntimeMailboxSnapshot {
        let depths = self
            .mailboxes
            .iter()
            .map(CoreMailbox::depth)
            .collect::<Vec<_>>();
        let capacities = self
            .mailboxes
            .iter()
            .map(CoreMailbox::capacity)
            .collect::<Vec<_>>();
        RuntimeMailboxSnapshot { depths, capacities }
    }
}

#[cfg(not(madsim))]
fn unix_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| u64::try_from(duration.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}

#[cfg(madsim)]
fn unix_time_ms() -> u64 {
    0
}

/// Expands the operation manifest into the uniform `ShardRuntime` client
/// methods: locate the placement (by stream id or raft group id), open the
/// reply channel, and submit the `GroupCommand` through [`ShardRuntime::
/// group_rpc`]. Entries with `client { none }` keep hand-written methods; see
/// the manifest grammar in [`crate::ops`].
macro_rules! shard_runtime_operations {
    // Stream-routed write with admission and an emptiness precheck.
    (@munch
        methods { $($methods:tt)* }
        rest {
            $(#[$attr:meta])*
            op $Variant:ident {
                fields { $req:ident: $Req:ty $(,)? }
                reply { $tx:ident: $Resp:ty }
                guard { $g:ident }
                handle { $($handle:tt)* }
                client {
                    $vis:vis stream fn $method:ident,
                    non_empty: $ne:ident,
                    admit: $incoming:expr
                }
            }
            $($rest:tt)*
        }
    ) => {
        shard_runtime_operations! {
            @munch
            methods {
                $($methods)*
                $(#[$attr])*
                $vis async fn $method(&self, $req: $Req) -> Result<$Resp, RuntimeError> {
                    if $req.$ne.is_empty() {
                        return Err(RuntimeError::EmptyAppend);
                    }
                    let placement = self.shard_map.locate(&$req.stream_id);
                    let incoming_bytes = $incoming;
                    let (response_tx, response_rx) = oneshot::channel();
                    self.group_rpc(
                        placement,
                        Some(incoming_bytes),
                        GroupCommand::$Variant {
                            $req,
                            $tx: response_tx,
                            $g: None,
                        },
                        response_rx,
                    )
                    .await
                }
            }
            rest { $($rest)* }
        }
    };
    // Stream-routed write with admission.
    (@munch
        methods { $($methods:tt)* }
        rest {
            $(#[$attr:meta])*
            op $Variant:ident {
                fields { $req:ident: $Req:ty $(,)? }
                reply { $tx:ident: $Resp:ty }
                guard { $g:ident }
                handle { $($handle:tt)* }
                client { $vis:vis stream fn $method:ident, admit: $incoming:expr }
            }
            $($rest:tt)*
        }
    ) => {
        shard_runtime_operations! {
            @munch
            methods {
                $($methods)*
                $(#[$attr])*
                $vis async fn $method(&self, $req: $Req) -> Result<$Resp, RuntimeError> {
                    let placement = self.shard_map.locate(&$req.stream_id);
                    let incoming_bytes = $incoming;
                    let (response_tx, response_rx) = oneshot::channel();
                    self.group_rpc(
                        placement,
                        Some(incoming_bytes),
                        GroupCommand::$Variant {
                            $req,
                            $tx: response_tx,
                            $g: None,
                        },
                        response_rx,
                    )
                    .await
                }
            }
            rest { $($rest)* }
        }
    };
    // Stream-routed operation without admission.
    (@munch
        methods { $($methods:tt)* }
        rest {
            $(#[$attr:meta])*
            op $Variant:ident {
                fields { $req:ident: $Req:ty $(,)? }
                reply { $tx:ident: $Resp:ty }
                guard { none }
                handle { $($handle:tt)* }
                client { $vis:vis stream fn $method:ident }
            }
            $($rest:tt)*
        }
    ) => {
        shard_runtime_operations! {
            @munch
            methods {
                $($methods)*
                $(#[$attr])*
                $vis async fn $method(&self, $req: $Req) -> Result<$Resp, RuntimeError> {
                    let placement = self.shard_map.locate(&$req.stream_id);
                    let (response_tx, response_rx) = oneshot::channel();
                    self.group_rpc(
                        placement,
                        None,
                        GroupCommand::$Variant {
                            $req,
                            $tx: response_tx,
                        },
                        response_rx,
                    )
                    .await
                }
            }
            rest { $($rest)* }
        }
    };
    // Group-routed operation: an explicit `RaftGroupId` followed by the
    // manifest fields in declaration order.
    (@munch
        methods { $($methods:tt)* }
        rest {
            $(#[$attr:meta])*
            op $Variant:ident {
                fields { $($field:ident: $field_ty:ty),* $(,)? }
                reply { $tx:ident: $Resp:ty }
                guard { none }
                handle { $($handle:tt)* }
                client { $vis:vis group fn $method:ident }
            }
            $($rest:tt)*
        }
    ) => {
        shard_runtime_operations! {
            @munch
            methods {
                $($methods)*
                $(#[$attr])*
                $vis async fn $method(
                    &self,
                    raft_group_id: RaftGroupId,
                    $($field: $field_ty),*
                ) -> Result<$Resp, RuntimeError> {
                    let placement = self.placement_for_group(raft_group_id)?;
                    let (response_tx, response_rx) = oneshot::channel();
                    self.group_rpc(
                        placement,
                        None,
                        GroupCommand::$Variant {
                            $($field,)*
                            $tx: response_tx,
                        },
                        response_rx,
                    )
                    .await
                }
            }
            rest { $($rest)* }
        }
    };
    // Hand-written client method: nothing to generate.
    (@munch
        methods { $($methods:tt)* }
        rest {
            $(#[$attr:meta])*
            op $Variant:ident {
                fields { $($fields:tt)* }
                reply { $($reply:tt)* }
                guard { $($guard:tt)* }
                handle { $($handle:tt)* }
                client { none }
            }
            $($rest:tt)*
        }
    ) => {
        shard_runtime_operations! {
            @munch
            methods { $($methods)* }
            rest { $($rest)* }
        }
    };
    (@munch
        methods { $($methods:tt)* }
        rest {}
    ) => {
        impl ShardRuntime {
            $($methods)*
        }
    };
    ($($manifest:tt)*) => {
        shard_runtime_operations! {
            @munch
            methods {}
            rest { $($manifest)* }
        }
    };
}

crate::ops::runtime_operations!(shard_runtime_operations);

fn spawn_core_worker(threading: RuntimeThreading, worker: CoreWorker) -> Result<(), RuntimeError> {
    match threading {
        RuntimeThreading::HostedTokio => {
            crate::rt::spawn(worker.run());
            Ok(())
        }
        #[cfg(not(madsim))]
        RuntimeThreading::ThreadPerCore => {
            let core_id = worker.core_id;
            std::thread::Builder::new()
                .name(format!("ursula-core-{}", core_id.0))
                .spawn(move || {
                    let runtime = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .expect("build per-core tokio runtime");
                    runtime.block_on(worker.run());
                })
                .map(|_| ())
                .map_err(|err| RuntimeError::SpawnCoreThread {
                    core_id,
                    message: err.to_string(),
                })
        }
    }
}

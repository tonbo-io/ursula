use std::collections::BTreeSet;
use std::collections::HashMap;
use std::io;
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
use ursula_stream::ColdGcPlanEntry;
use ursula_stream::ColdGcTarget;

use crate::admission::RaftUncommittedAdmission;
use crate::admission::RaftUncommittedBytesTracker;
use crate::cold_index::ColdIndexPageKey;
use crate::cold_index::ColdIndexPageStore;
use crate::cold_index::ColdIndexRepairReport;
use crate::cold_index::ColdStoreColdIndexPageStore;
use crate::cold_index::RepairColdIndexRequest;
use crate::cold_index::RepairColdIndexResponse;
use crate::cold_index::cold_index_generation_dir;
use crate::cold_index::load_cold_chunks_from_pages;
use crate::cold_index::parse_cold_index_page_file_name;
use crate::cold_index::select_cold_chunk_compaction;
use crate::cold_refs::ColdOrphanSweepPlan;
use crate::cold_refs::ColdOrphanSweepRequest;
use crate::cold_store::ColdStore;
use crate::cold_store::ColdStoreHandle;
use crate::cold_store::ColdStoreInfo;
use crate::cold_store::cold_chunk_dir;
use crate::cold_store::cold_external_dir;
use crate::cold_store::is_cold_chunk_file_name;
use crate::cold_store::is_external_payload_file_name;
use crate::cold_store::new_cold_chunk_path_in_generation;
use crate::cold_store::new_cold_pack_path;
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
use crate::metrics::elapsed_ns;
use crate::metrics::is_stale_cold_flush_candidate_error;
use crate::request::AckColdGcResponse;
use crate::request::AdvanceRetentionRequest;
use crate::request::AdvanceRetentionResponse;
use crate::request::AppendExternalRequest;
use crate::request::AppendRequest;
use crate::request::AppendResponse;
use crate::request::AppendTransactionRequest;
use crate::request::AppendTransactionResponse;
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
use crate::request::DeferColdGcResponse;
use crate::request::DeleteStreamRequest;
use crate::request::DeleteStreamResponse;
use crate::request::FlushColdRequest;
use crate::request::FlushColdResponse;
use crate::request::HeadStreamRequest;
use crate::request::HeadStreamResponse;
use crate::request::ImportGroupStateRequest;
use crate::request::ImportGroupStateResponse;
use crate::request::PlanColdFlushRequest;
use crate::request::PlanGroupColdFlushRequest;
use crate::request::PublishSnapshotRequest;
use crate::request::PublishSnapshotResponse;
use crate::request::PurgeBucketResponse;
use crate::request::ReadSnapshotRequest;
use crate::request::ReadSnapshotResponse;
use crate::request::ReadStreamRequest;
use crate::request::ReadStreamResponse;
use crate::request::SetFeatureLevelRequest;
use crate::request::SetFeatureLevelResponse;
use crate::request::TidyStreamsRequest;
use crate::request::TidyStreamsResponse;
use crate::rt::sync::Semaphore;
use crate::rt::sync::mpsc;
use crate::rt::sync::oneshot;
use crate::rt::time::Instant;
use crate::trace::Traced;

mod compaction_debt;
mod orphan_sweep;
mod shared_ref_compaction;

use compaction_debt::CompactionDebt;

/// A lone flush candidate below this size goes down the pack path as a pack
/// of one instead of becoming a tiny exclusive object (F14c): its shared ref
/// is compacted by the pack-reference driver, and the flush rewrites no
/// cold-index page.
pub const EXCLUSIVE_FLUSH_MIN_BYTES: usize = 1 << 20;

/// Default size below which an exclusive chunk is compaction debt (F14d);
/// the compaction worker replaces it with its configured target.
const DEFAULT_COMPACTION_DEBT_CHUNK_BYTES: u64 = 8 << 20;

/// Debt pages one compaction pass takes; pages of streams the pass does not
/// reach go back into the debt.
const COMPACTION_DEBT_PAGES_PER_PASS: usize = 4_096;

pub use orphan_sweep::COLD_ORPHAN_SWEEP_GRACE_MS;

/// Backoff before the cold GC retries an entry it deferred after its first
/// failure (bounded-state F14b, feature level 1). Each further deferral
/// doubles it, up to [`COLD_GC_DEFER_MAX_BACKOFF_MS`].
pub const COLD_GC_DEFER_BACKOFF_MS: u64 = 60_000;

/// Longest backoff between retries of a failing cold GC entry (one hour).
pub const COLD_GC_DEFER_MAX_BACKOFF_MS: u64 = 60 * 60 * 1_000;

/// Backoff for an entry that has already been deferred `attempts` times:
/// 1 min, 2 min, 4 min, ... capped at one hour.
pub fn cold_gc_defer_backoff_ms(attempts: u32) -> u64 {
    COLD_GC_DEFER_BACKOFF_MS
        .checked_shl(attempts.min(32))
        .unwrap_or(u64::MAX)
        .min(COLD_GC_DEFER_MAX_BACKOFF_MS)
}

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
    cold_index_repair: Arc<std::sync::Mutex<HashMap<RaftGroupId, ColdIndexRepairCursor>>>,
    /// Node-local cursor of each group's cold orphan sweep (F14h).
    cold_orphan_sweep: Arc<std::sync::Mutex<HashMap<RaftGroupId, Option<BucketStreamId>>>>,
    /// Cold-index pages that may hold compactable small chunks (F14d).
    compaction_debt: Arc<std::sync::Mutex<CompactionDebt>>,
    /// Exclusive chunks below this many bytes are compaction debt (F14d).
    compaction_debt_chunk_bytes: Arc<AtomicU64>,
}

/// Node-local position of one group's cold-index repair cursor.
#[derive(Debug, Clone, Default)]
struct ColdIndexRepairCursor {
    after: Option<BucketStreamId>,
    last_full_cycle_ms: Option<u64>,
}

/// Result of one repair step for one group.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ColdIndexRepairStep {
    pub report: ColdIndexRepairReport,
    /// The step reached the end of the group's streams on its leader.
    pub cycle_completed: bool,
}

/// Cluster-local summary of a bucket purge across all Raft groups.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PurgeBucketReport {
    pub removed_streams: u64,
    pub groups_with_streams: Vec<usize>,
    pub pending_cold_gc_entries: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LegacySharedMigrationReport {
    pub observed_chunks: usize,
    pub migrated_chunks: usize,
    pub pending_chunks: usize,
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
            cold_index_repair: Arc::new(std::sync::Mutex::new(HashMap::new())),
            cold_orphan_sweep: Arc::new(std::sync::Mutex::new(HashMap::new())),
            compaction_debt: Arc::default(),
            compaction_debt_chunk_bytes: Arc::new(AtomicU64::new(
                DEFAULT_COMPACTION_DEBT_CHUNK_BYTES,
            )),
        })
    }

    /// Sets the size below which exclusive chunks are compaction debt; the
    /// compaction worker passes its target (F14d).
    pub fn set_compaction_debt_chunk_bytes(&self, bytes: u64) {
        self.compaction_debt_chunk_bytes
            .store(bytes.max(1), Ordering::Relaxed);
    }

    /// Pages currently held as compaction debt (F14d).
    pub fn compaction_debt_pages(&self) -> usize {
        self.compaction_debt.lock().map_or(0, |debt| debt.len())
    }

    /// Records `[start_offset, end_offset)` of one incarnation as compaction
    /// debt when its exclusive object is below the debt size (F14d).
    pub(crate) fn record_compaction_debt(
        &self,
        stream_id: &BucketStreamId,
        generation: u64,
        start_offset: u64,
        end_offset: u64,
        object_bytes: u64,
    ) {
        if object_bytes >= self.compaction_debt_chunk_bytes.load(Ordering::Relaxed) {
            return;
        }
        if let Ok(mut debt) = self.compaction_debt.lock() {
            debt.record_range(stream_id, generation, start_offset, end_offset);
        }
    }

    fn record_compaction_debt_pages(&self, pages: Vec<ColdIndexPageKey>) {
        if pages.is_empty() {
            return;
        }
        if let Ok(mut debt) = self.compaction_debt.lock() {
            for key in pages {
                debt.record_page(key);
            }
        }
    }

    fn take_compaction_debt(&self, max: usize) -> Vec<ColdIndexPageKey> {
        self.compaction_debt
            .lock()
            .map_or_else(|_| Vec::new(), |mut debt| debt.take(max))
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

    /// Whether the local replica of the stream's group currently leads, by
    /// its own view. Unlike `require_local_live_read_owner` this takes no
    /// quorum round trip; it gates background leader-side work only.
    async fn accepts_local_writes(&self, stream_id: &BucketStreamId) -> Result<bool, RuntimeError> {
        let placement = self.shard_map.locate(stream_id);
        let (response_tx, response_rx) = oneshot::channel();
        self.group_rpc(
            placement,
            None,
            GroupCommand::AcceptsLocalWrites { response_tx },
            response_rx,
        )
        .await
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
        let path = new_cold_chunk_path_in_generation(
            &candidate.stream_id,
            candidate.cold_generation,
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
            object_offset: 0,
            shared_object: false,
            payload_digest: candidate.payload_digest,
        };
        let publish_started_at = Instant::now();
        let debt_stream = candidate.stream_id.clone();
        let publish = self
            .flush_cold(FlushColdRequest {
                stream_id: candidate.stream_id,
                chunk,
                cold_generation: Some(candidate.cold_generation),
            })
            .await;
        match publish {
            Ok(response) => {
                self.metrics
                    .record_cold_publish(object_size, elapsed_ns(publish_started_at));
                self.record_compaction_debt(
                    &debt_stream,
                    candidate.cold_generation,
                    candidate.start_offset,
                    candidate.end_offset,
                    object_size,
                );
                Ok(response)
            }
            Err(err) => {
                // F14e: a typed stream error (a stale candidate) or a
                // redirect before proposal means the flush definitely did not
                // commit, and the engine rolled back or never wrote its page
                // entry, so nothing references the chunk. Any other failure
                // is ambiguous and keeps the chunk.
                if (err.stream_error_code().is_some() || err.is_forward_before_proposal())
                    && let Err(cleanup_err) = cold_store.delete_chunk(&path).await
                {
                    tracing::warn!(
                        path = %path,
                        error = %cleanup_err,
                        "failed to remove the chunk of a rejected cold flush"
                    );
                }
                Err(err)
            }
        }
    }

    pub(crate) async fn flush_cold_candidates_batch(
        &self,
        candidates: Vec<ColdFlushCandidate>,
    ) -> Result<Vec<FlushColdResponse>, RuntimeError> {
        // A bucket is the physical erasure domain. Keep encounter order while
        // partitioning one Raft group's flush plan so no pack can retain bytes
        // for a purged bucket merely because another bucket is still live.
        let mut bucket_batches: Vec<Vec<ColdFlushCandidate>> = Vec::new();
        for candidate in candidates {
            if let Some(batch) = bucket_batches.iter_mut().find(|batch| {
                batch
                    .first()
                    .is_some_and(|first| first.stream_id.bucket_id == candidate.stream_id.bucket_id)
            }) {
                batch.push(candidate);
            } else {
                bucket_batches.push(vec![candidate]);
            }
        }

        let mut responses = Vec::new();
        for mut batch in bucket_batches {
            // F14c: a lone candidate below 1 MiB is packed alone rather
            // than written as a tiny exclusive object.
            let small_single = batch
                .first()
                .is_some_and(|candidate| candidate.payload.len() < EXCLUSIVE_FLUSH_MIN_BYTES);
            if batch.len() > 1 || small_single {
                responses.extend(self.flush_cold_candidates_pack(batch).await?);
                continue;
            }
            let candidate = batch.pop().expect("single-candidate batch");
            match self.flush_cold_candidate(candidate).await {
                Ok(response) => responses.push(response),
                Err(err) if is_stale_cold_flush_candidate_error(&err) => {}
                Err(err) => return Err(err),
            }
        }
        Ok(responses)
    }

    async fn flush_cold_candidates_pack(
        &self,
        candidates: Vec<ColdFlushCandidate>,
    ) -> Result<Vec<FlushColdResponse>, RuntimeError> {
        let Some(cold_store) = self.cold_store.as_ref() else {
            return Err(RuntimeError::ColdStoreConfig {
                message: "cold backend must be configured before flushing cold chunks".to_owned(),
            });
        };
        let first = candidates
            .first()
            .expect("packed cold flush requires at least one candidate");
        let placement = self.shard_map.locate(&first.stream_id);
        if candidates
            .iter()
            .any(|candidate| self.shard_map.locate(&candidate.stream_id) != placement)
        {
            return Err(RuntimeError::ColdStoreConfig {
                message: "packed cold flush candidates must belong to one Raft group".to_owned(),
            });
        }
        if candidates
            .iter()
            .any(|candidate| candidate.stream_id.bucket_id != first.stream_id.bucket_id)
        {
            return Err(RuntimeError::ColdStoreConfig {
                message: "packed cold flush candidates must belong to one bucket erasure domain"
                    .to_owned(),
            });
        }
        let payload_len = candidates.iter().try_fold(0usize, |total, candidate| {
            total.checked_add(candidate.payload.len())
        });
        let Some(payload_len) = payload_len else {
            return Err(RuntimeError::ColdStoreConfig {
                message: "packed cold flush payload size overflow".to_owned(),
            });
        };
        let mut payload = Vec::with_capacity(payload_len);
        let mut object_offsets = Vec::with_capacity(candidates.len());
        for candidate in &candidates {
            object_offsets.push(u64::try_from(payload.len()).expect("pack offset fits u64"));
            payload.extend_from_slice(&candidate.payload);
        }
        let path = new_cold_pack_path(&first.stream_id.bucket_id, placement.raft_group_id.0);
        let upload_started_at = Instant::now();
        let object_size = match cold_store.write_chunk(&path, &payload).await {
            Ok(object_size) => object_size,
            Err(err) => {
                self.metrics.record_cold_flush_write_error();
                return Err(RuntimeError::ColdStoreIo {
                    message: err.to_string(),
                });
            }
        };
        self.metrics
            .record_cold_upload(object_size, elapsed_ns(upload_started_at));
        self.metrics.record_cold_pack(
            object_size,
            u64::try_from(candidates.len()).expect("candidate count fits u64"),
        );

        let mut responses = Vec::with_capacity(candidates.len());
        let mut published = 0usize;
        for (candidate, object_offset) in candidates.into_iter().zip(object_offsets) {
            let logical_size = candidate.end_offset.saturating_sub(candidate.start_offset);
            let publish_started_at = Instant::now();
            let publish = self
                .flush_cold(FlushColdRequest {
                    stream_id: candidate.stream_id,
                    chunk: ColdChunkRef {
                        start_offset: candidate.start_offset,
                        end_offset: candidate.end_offset,
                        s3_path: path.clone(),
                        object_size,
                        object_offset,
                        shared_object: true,
                        payload_digest: candidate.payload_digest,
                    },
                    cold_generation: Some(candidate.cold_generation),
                })
                .await;
            match publish {
                Ok(response) => {
                    published = published.saturating_add(1);
                    self.metrics
                        .record_cold_publish(logical_size, elapsed_ns(publish_started_at));
                    responses.push(response);
                }
                Err(err) if is_stale_cold_flush_candidate_error(&err) => {}
                // A transport or leadership error may happen after the Raft
                // commit became durable. Never delete the pack on an
                // ambiguous response; a later orphan sweep may prove it
                // unreferenced, while eager deletion could corrupt a
                // successfully published slice.
                Err(err) => return Err(err),
            }
        }
        // Every candidate was definitely rejected as stale, so nothing
        // references the pack. Cleanup is best effort, as for an exclusive
        // chunk: a failed delete leaves an orphan for the sweep (F14h), and
        // since F14c this path also carries lone small candidates.
        if published == 0
            && let Err(cleanup_err) = cold_store.delete_chunk(&path).await
        {
            tracing::warn!(
                path = %path,
                error = %cleanup_err,
                "failed to remove the pack of a rejected cold flush"
            );
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

    /// Sums per-bucket committed usage across every Raft group on this node.
    ///
    /// Groups are read serially from their local applied state (leader or
    /// follower); usage export consumers tolerate replication lag, so this
    /// never requires leadership and never blocks on quorum.
    /// Purges one tenant bucket from every Raft group: streams, bucket, and
    /// usage entries. Idempotent — a re-run over an already purged bucket
    /// reports zero removals. Cold objects are reclaimed by the enqueued GC
    /// entries; callers wanting synchronous reclamation run the cold GC pass
    /// afterwards.
    pub async fn purge_bucket_all_groups(
        &self,
        bucket_id: &str,
    ) -> Result<PurgeBucketReport, RuntimeError> {
        let mut report = PurgeBucketReport::default();
        let group_count = self.shard_map.raft_group_count();
        for group_id in 0..group_count {
            let response = self
                .purge_bucket(RaftGroupId(group_id), bucket_id.to_owned())
                .await?;
            if response.removed_streams > 0 {
                report.groups_with_streams.push(group_id as usize);
            }
            report.removed_streams = report
                .removed_streams
                .saturating_add(response.removed_streams);
            report.pending_cold_gc_entries = report
                .pending_cold_gc_entries
                .saturating_add(response.pending_cold_gc_entries);
        }
        Ok(report)
    }

    /// Removes the entire bucket erasure domain, including external payloads
    /// and stage-before-commit orphans, then verifies the authoritative store
    /// no longer lists an object below the prefix. Call only after every
    /// group has durably installed the bucket tombstone and legacy
    /// shared-pack debt has converged to zero.
    pub async fn erase_bucket_cold_prefix_and_prove(
        &self,
        bucket_id: &str,
    ) -> Result<(), RuntimeError> {
        let Some(cold_store) = self.cold_store.as_ref() else {
            return Ok(());
        };
        erase_prefix_and_prove(cold_store, &crate::cold_bucket_prefix(bucket_id)).await
    }

    pub async fn bucket_usage_all_groups(
        &self,
    ) -> Result<Vec<ursula_stream::BucketUsageSnapshot>, RuntimeError> {
        let mut merged: std::collections::HashMap<String, ursula_stream::BucketUsage> =
            std::collections::HashMap::new();
        let group_count = self.shard_map.raft_group_count();
        for group_id in 0..group_count {
            let report = self.bucket_usage(RaftGroupId(group_id)).await?;
            for entry in report {
                let usage = merged.entry(entry.bucket_id).or_default();
                usage.committed_append_bytes = usage
                    .committed_append_bytes
                    .saturating_add(entry.usage.committed_append_bytes);
                usage.committed_records = usage
                    .committed_records
                    .saturating_add(entry.usage.committed_records);
                usage.committed_write_units = usage
                    .committed_write_units
                    .saturating_add(entry.usage.committed_write_units);
                usage.retained_bytes = usage
                    .retained_bytes
                    .saturating_add(entry.usage.retained_bytes);
                usage.stream_count = usage.stream_count.saturating_add(entry.usage.stream_count);
            }
        }
        let mut report = merged
            .into_iter()
            .map(|(bucket_id, usage)| ursula_stream::BucketUsageSnapshot { bucket_id, usage })
            .collect::<Vec<_>>();
        report.sort_by(|left, right| left.bucket_id.cmp(&right.bucket_id));
        Ok(report)
    }

    /// Replicated feature level (C0) of every Raft group as held by this
    /// node's applied replica state. Per-group results, so a group this node
    /// does not host reports its own error instead of hiding the others.
    pub async fn feature_levels_all_groups(&self) -> Vec<(RaftGroupId, Result<u32, RuntimeError>)> {
        let group_count = self.shard_map.raft_group_count();
        let mut levels = Vec::new();
        for group_id in 0..group_count {
            let group = RaftGroupId(group_id);
            levels.push((group, self.feature_level(group).await));
        }
        levels
    }

    /// Bounded-state gauges (`docs/architecture/bounded-stream-state.md`
    /// §7.5) of every Raft group as held by this node's applied replica state.
    /// Groups are asked concurrently; per-group results, like
    /// [`Self::feature_levels_all_groups`].
    pub async fn state_gauges_all_groups(
        &self,
    ) -> Vec<(
        RaftGroupId,
        Result<ursula_stream::GroupStateGauges, RuntimeError>,
    )> {
        let group_count = self.shard_map.raft_group_count();
        let requests = (0..group_count).map(|group_id| {
            let group = RaftGroupId(group_id);
            async move { (group, self.state_gauges(group).await) }
        });
        futures_util::future::join_all(requests).await
    }

    /// Proposes `SetFeatureLevel { level }` to every Raft group (C0), serially
    /// like the other all-group admin sweeps. Each group ends at
    /// `max(current, level)`, so re-running after a partial failure is safe.
    /// Results are per group: on a Raft cluster a group led by another node
    /// fails with a forward-to-leader error, and the operator (`ursulactl
    /// cluster enable-feature`) asks every node so each leader proposes for
    /// its own groups. Callers must ensure every voter and learner supports
    /// `level`.
    pub async fn set_feature_level_all_groups(
        &self,
        level: u32,
    ) -> Vec<(RaftGroupId, Result<SetFeatureLevelResponse, RuntimeError>)> {
        let group_count = self.shard_map.raft_group_count();
        let mut responses = Vec::new();
        for group_id in 0..group_count {
            let group = RaftGroupId(group_id);
            responses.push((
                group,
                self.set_feature_level(group, SetFeatureLevelRequest { level })
                    .await,
            ));
        }
        responses
    }

    /// One leader-side external-locator offload pass (bounded-state F5) in
    /// every group this node leads: each offloads up to
    /// `max_streams_per_group` streams whose state-held external refs are
    /// due. A failing group is logged and skipped, so it cannot stall the
    /// others. Groups below feature level 3 hold no staged refs.
    pub async fn offload_cold_refs_all_groups_once(
        &self,
        max_streams_per_group: usize,
        now_ms: u64,
    ) -> crate::cold_refs::OffloadColdRefsResponse {
        let mut report = crate::cold_refs::OffloadColdRefsResponse::default();
        if self.cold_store.is_none() {
            return report;
        }
        for group_id in 0..self.shard_map.raft_group_count() {
            let request =
                crate::cold_refs::OffloadColdRefsRequest::new(now_ms, max_streams_per_group);
            match self.offload_cold_refs(RaftGroupId(group_id), request).await {
                Ok(step) => report.add(&step),
                Err(err) => tracing::warn!(
                    raft_group_id = group_id,
                    error = %err,
                    "external-locator offload pass failed; continuing with remaining groups"
                ),
            }
        }
        report
    }

    /// One leader-side `TidyStream` pass over every Raft group
    /// (bounded-state F0): each group this node leads proposes `TidyStream`
    /// for at most `max_streams_per_group` streams with normalization debt.
    /// A failing group does not stop the others; the first error is
    /// returned after every group had its pass.
    pub async fn tidy_streams_all_groups_once(
        &self,
        max_streams_per_group: usize,
        now_ms: u64,
    ) -> Result<TidyStreamsResponse, RuntimeError> {
        let mut total = TidyStreamsResponse::default();
        let mut first_error = None;
        for group_id in 0..self.shard_map.raft_group_count() {
            let request = TidyStreamsRequest {
                max_streams: max_streams_per_group,
                now_ms,
            };
            match self.tidy_streams(RaftGroupId(group_id), request).await {
                Ok(report) => {
                    total.tidied = total.tidied.saturating_add(report.tidied);
                    total.debt_remaining =
                        total.debt_remaining.saturating_add(report.debt_remaining);
                }
                Err(err) => {
                    tracing::warn!(
                        raft_group_id = group_id,
                        error = %err,
                        "tidy pass failed; continuing with remaining groups"
                    );
                    first_error.get_or_insert(err);
                }
            }
        }
        match first_error {
            Some(err) => Err(err),
            None => Ok(total),
        }
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
    /// target-sized immutable chunks. Discovery drains the compaction debt
    /// that flushes, compaction outputs and the repair cursor record, and
    /// reads only those cold-index pages, by key: it lists nothing (F14d).
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
        let pages = self.take_compaction_debt(COMPACTION_DEBT_PAGES_PER_PASS);
        // Pages are grouped per stream incarnation (F14g). The engine
        // republishes into the live incarnation's generation, so inputs from
        // a deleted incarnation fail the page match and are skipped.
        let mut pages_by_stream: Vec<((BucketStreamId, u64), Vec<ColdIndexPageKey>)> = Vec::new();
        for page in pages {
            let identity = (page.stream_id.clone(), page.generation);
            match pages_by_stream
                .iter_mut()
                .find(|(existing, _)| *existing == identity)
            {
                Some((_, stream_pages)) => stream_pages.push(page),
                None => pages_by_stream.push((identity, vec![page])),
            }
        }
        let mut pages_by_stream = pages_by_stream.into_iter();
        let result = self
            .compact_cold_debt(
                cold_store,
                &mut pages_by_stream,
                target_bytes,
                max_bytes,
                max_streams,
                gc_grace_ms,
            )
            .await;
        // Streams this pass did not reach stay debt for the next one.
        for (_, stream_pages) in pages_by_stream {
            self.record_compaction_debt_pages(stream_pages);
        }
        result
    }

    async fn compact_cold_debt(
        &self,
        cold_store: &ColdStoreHandle,
        pages_by_stream: &mut impl Iterator<Item = ((BucketStreamId, u64), Vec<ColdIndexPageKey>)>,
        target_bytes: u64,
        max_bytes: u64,
        max_streams: usize,
        gc_grace_ms: u64,
    ) -> Result<usize, RuntimeError> {
        let store = ColdStoreColdIndexPageStore::new(cold_store.clone());
        let mut compacted = 0;
        while compacted < max_streams {
            let Some(((stream_id, generation), stream_pages)) = pages_by_stream.next() else {
                break;
            };
            // Only the local Raft leader may publish a replacement. A plain
            // leadership check: compaction gains no quorum round trip, and the
            // replacement's own commit is what proves leadership.
            if !self.accepts_local_writes(&stream_id).await.unwrap_or(false) {
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
                .try_fold(0_u64, |total, chunk| {
                    total.checked_add(chunk.end_offset.saturating_sub(chunk.start_offset))
                })
                .ok_or_else(|| RuntimeError::ColdStoreIo {
                    message: "cold compaction byte count overflow".to_owned(),
                })?;
            let capacity = usize::try_from(total_bytes).map_err(|_| RuntimeError::ColdStoreIo {
                message: "cold compaction object exceeds addressable memory".to_owned(),
            })?;
            let mut payload = Vec::with_capacity(capacity);
            for chunk in &old_chunks {
                let len = usize::try_from(chunk.end_offset.saturating_sub(chunk.start_offset))
                    .map_err(|_| RuntimeError::ColdStoreIo {
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
            let path = new_cold_chunk_path_in_generation(
                &stream_id,
                generation,
                first.start_offset,
                last.end_offset,
            );
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
                object_offset: 0,
                shared_object: false,
                payload_digest: blake3::hash(&payload).to_hex().to_string(),
            };
            let replacement_path = replacement.s3_path.clone();
            let replacement_range = (
                replacement.start_offset,
                replacement.end_offset,
                replacement.object_size,
            );
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
                    err.is_forward_before_proposal() || err.stream_error_code().is_some();
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
            // A replacement still below the debt size may merge further.
            self.record_compaction_debt(
                &stream_id,
                generation,
                replacement_range.0,
                replacement_range.1,
                replacement_range.2,
            );
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
        let planned = self.plan_cold_gc(raft_group_id, max_entries).await?;
        if planned.is_empty() {
            return Ok(0);
        }
        // F14b (feature level 1): a failing entry is moved to the tail with a
        // backoff, so it no longer blocks every entry behind it. Below level 1
        // the worker stops at the first failure, as before.
        let defer_failures = self
            .feature_level(raft_group_id)
            .await
            .is_ok_and(|level| level >= crate::FEATURE_LEVEL_KEYED_STREAMS);
        let mut acked_seq = None;
        let mut reclaimed = 0usize;
        let mut deferred = 0usize;
        let mut first_error = None;
        // Entries are FIFO by seq; the ack pops a prefix, so it never skips
        // past an object that is still present in cold storage: a failing
        // entry is either deferred (restamped behind the acked prefix) before
        // the ack, or ends the pass.
        for planned_entry in planned {
            let entry = &planned_entry.entry;
            if entry.not_before_ms > unix_time_ms() {
                break;
            }
            let result = match &entry.target {
                ColdGcTarget::Stream(stream_id) => {
                    self.reclaim_stream_incarnation(
                        cold_store,
                        raft_group_id,
                        max_entries,
                        &planned_entry,
                        stream_id,
                    )
                    .await
                }
                ColdGcTarget::Paths(paths) => {
                    let mut outcome = Ok(());
                    for path in paths {
                        // Every path names one object (F14g containment).
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
                    let error = RuntimeError::ColdStoreIo {
                        message: err.to_string(),
                    };
                    if defer_failures {
                        let not_before_ms = unix_time_ms()
                            .saturating_add(cold_gc_defer_backoff_ms(entry.defer_attempts));
                        match self
                            .defer_cold_gc(raft_group_id, entry.seq, not_before_ms)
                            .await
                        {
                            Ok(_) => {
                                tracing::warn!(
                                    raft_group_id = raft_group_id.0,
                                    seq = entry.seq,
                                    error = %err,
                                    "cold GC entry failed; deferred to the tail of the queue"
                                );
                                deferred += 1;
                                first_error.get_or_insert(error);
                                continue;
                            }
                            Err(defer_err) => {
                                tracing::warn!(
                                    raft_group_id = raft_group_id.0,
                                    seq = entry.seq,
                                    error = %defer_err,
                                    "failed to defer a failing cold GC entry"
                                );
                            }
                        }
                    }
                    if acked_seq.is_none() {
                        return Err(error);
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
        // A pass that only deferred entries reports the failure, so the
        // all-groups runner and its callers still see it.
        if reclaimed == 0
            && deferred > 0
            && let Some(error) = first_error
        {
            return Err(error);
        }
        Ok(reclaimed)
    }

    /// Reclaims the cold objects of one removed stream incarnation (F14a,
    /// F14g). The sweep deletes only object names Ursula writes for that
    /// stream, one directory level at a time, so it never reaches another
    /// stream's namespace, such as an affinity stream under a two-segment
    /// stream's name. It never deletes objects of the generation a live
    /// stream of the same name uses, and checks that again before deleting
    /// pages, which are the last objects removed.
    ///
    /// - Legacy entries (no generation, enqueued below level 1) delete
    ///   legacy-format chunk names directly under `{stream}/chunks/` and
    ///   generation-0 pages, and are acknowledged without deleting anything
    ///   while a stream with the name exists again (step 1).
    /// - Entries naming generation `g` (level 1) delete the external
    ///   payloads that generation's pages reference inside
    ///   `{stream}/external/` (F14a), the chunks of that generation (legacy
    ///   names for `g = 0`, `{stream}/chunks/{g:016x}/` otherwise), and its
    ///   pages.
    async fn reclaim_stream_incarnation(
        &self,
        cold_store: &ColdStoreHandle,
        raft_group_id: RaftGroupId,
        max_entries: usize,
        planned: &ColdGcPlanEntry,
        stream_id: &BucketStreamId,
    ) -> io::Result<()> {
        let generation = planned.entry.cold_generation.unwrap_or(0);
        if stream_gc_blocked_by_live_stream(
            planned.entry.cold_generation,
            planned.live_cold_generation,
        ) {
            tracing::debug!(
                stream = %stream_id,
                seq = planned.entry.seq,
                "stream gc entry acknowledged without deletion: the name is live again"
            );
            return Ok(());
        }

        if planned.entry.cold_generation.is_some() {
            let store = ColdStoreColdIndexPageStore::new(cold_store.clone());
            let external_dir = cold_external_dir(stream_id);
            let mut referenced = BTreeSet::new();
            for page_id in list_cold_index_page_ids(cold_store, stream_id, generation).await? {
                let key = ColdIndexPageKey {
                    stream_id: stream_id.clone(),
                    generation,
                    page_id,
                };
                let Some(page) = store.get_page(&key).await? else {
                    continue;
                };
                referenced.extend(
                    page.external_segments
                        .iter()
                        .filter(|object| {
                            object
                                .s3_path
                                .strip_prefix(&external_dir)
                                .is_some_and(is_external_payload_file_name)
                        })
                        .map(|object| object.s3_path.clone()),
                );
            }
            for path in referenced {
                cold_store.delete_chunk(&path).await?;
            }
        }

        let chunk_dir = cold_chunk_dir(stream_id, generation);
        for name in cold_store.list_file_names(&chunk_dir).await? {
            if is_cold_chunk_file_name(&name) {
                cold_store
                    .delete_chunk(&format!("{chunk_dir}{name}"))
                    .await?;
            }
        }

        // The pages are the discovery surface for referenced objects, so
        // they go last, after checking the name once more.
        let live = self
            .plan_cold_gc(raft_group_id, max_entries)
            .await
            .map_err(|err| io::Error::other(err.to_string()))?
            .into_iter()
            .find(|candidate| candidate.entry.seq == planned.entry.seq)
            .and_then(|candidate| candidate.live_cold_generation);
        if stream_gc_blocked_by_live_stream(planned.entry.cold_generation, live) {
            return Ok(());
        }
        let page_dir = cold_index_generation_dir(stream_id, generation);
        for page_id in list_cold_index_page_ids(cold_store, stream_id, generation).await? {
            cold_store
                .delete_chunk(&format!("{page_dir}{page_id:020}.idx"))
                .await?;
        }
        Ok(())
    }

    /// One step of the leader-side cold-index page repair cursor for one
    /// group (bounded-state F19 step 2): repairs the pages of up to
    /// `max_streams` streams after the cursor and advances it. A step that
    /// reaches the end of the group's streams records a completed cycle. On
    /// a follower the step repairs nothing and restarts the cursor.
    pub async fn repair_cold_index_group_once(
        &self,
        raft_group_id: RaftGroupId,
        max_streams: usize,
    ) -> Result<ColdIndexRepairStep, RuntimeError> {
        if self.cold_store.is_none() {
            return Ok(ColdIndexRepairStep::default());
        }
        let after = self
            .cold_index_repair
            .lock()
            .map_err(|_| RuntimeError::ColdStoreConfig {
                message: "cold-index repair cursor lock poisoned".to_owned(),
            })?
            .get(&raft_group_id)
            .and_then(|cursor| cursor.after.clone());
        let response = self
            .repair_cold_index(raft_group_id, RepairColdIndexRequest {
                after,
                max_streams: max_streams.max(1),
                stream: None,
                retention_gc_now_ms: Some(unix_time_ms()),
            })
            .await?;
        let mut cursors =
            self.cold_index_repair
                .lock()
                .map_err(|_| RuntimeError::ColdStoreConfig {
                    message: "cold-index repair cursor lock poisoned".to_owned(),
                })?;
        let cursor = cursors.entry(raft_group_id).or_default();
        cursor.after = response.next_after;
        if response.cycle_completed {
            cursor.last_full_cycle_ms = Some(unix_time_ms());
        }
        drop(cursors);
        self.record_compaction_debt_pages(response.compaction_pages);
        Ok(ColdIndexRepairStep {
            report: response.report,
            cycle_completed: response.cycle_completed,
        })
    }

    /// When this node, as leader of `raft_group_id`, last completed a full
    /// cold-index repair cycle over the group's streams.
    pub fn cold_index_repair_last_full_cycle_ms(&self, raft_group_id: RaftGroupId) -> Option<u64> {
        self.cold_index_repair
            .lock()
            .ok()?
            .get(&raft_group_id)
            .and_then(|cursor| cursor.last_full_cycle_ms)
    }

    /// Whether this node has completed a cold-index page-repair cycle as
    /// leader of `raft_group_id` (F19), which the raise to feature level 2
    /// (F1 sparse marks) requires. Without a cold store no page exists, so
    /// the cycle is vacuously complete.
    pub fn cold_index_repair_completed(&self, raft_group_id: RaftGroupId) -> bool {
        self.cold_store.is_none()
            || self
                .cold_index_repair_last_full_cycle_ms(raft_group_id)
                .is_some()
    }

    /// One repair step in every group. A failing group is logged and
    /// skipped, so it cannot stall the others.
    pub async fn repair_cold_index_all_groups_once(
        &self,
        max_streams_per_group: usize,
    ) -> ColdIndexRepairReport {
        let mut report = ColdIndexRepairReport::default();
        if self.cold_store.is_none() {
            return report;
        }
        for group_id in 0..self.shard_map.raft_group_count() {
            match self
                .repair_cold_index_group_once(RaftGroupId(group_id), max_streams_per_group)
                .await
            {
                Ok(step) => {
                    report.add(&step.report);
                    if step.cycle_completed {
                        tracing::debug!(
                            raft_group_id = group_id,
                            "cold-index repair cycle completed"
                        );
                    }
                }
                Err(err) => tracing::warn!(
                    raft_group_id = group_id,
                    error = %err,
                    "cold-index repair step failed; continuing with remaining groups"
                ),
            }
        }
        report
    }

    pub async fn run_cold_gc_all_groups_once(
        &self,
        max_entries_per_group: usize,
    ) -> Result<usize, RuntimeError> {
        if self.cold_store.is_none() {
            return Ok(0);
        }
        // F14b: one group's failure must not stall reclamation in the groups
        // after it. Every group runs; the first error is reported once all
        // have had their pass.
        let mut reclaimed = 0;
        let mut first_error = None;
        for group_id in 0..self.shard_map.raft_group_count() {
            match self
                .run_cold_gc_group_once(RaftGroupId(group_id), max_entries_per_group)
                .await
            {
                Ok(group_reclaimed) => reclaimed += group_reclaimed,
                Err(err) => {
                    tracing::warn!(
                        raft_group_id = group_id,
                        error = %err,
                        "cold GC pass failed; continuing with remaining groups"
                    );
                    first_error.get_or_insert(err);
                }
            }
        }
        match first_error {
            Some(err) => Err(err),
            None => Ok(reclaimed),
        }
    }

    /// Number of raft groups this runtime is sharded into. Backup tooling
    /// iterates `0..raft_group_count()` to cover the whole keyspace.
    pub fn raft_group_count(&self) -> u32 {
        self.shard_map.raft_group_count()
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

    /// Shut down and remove one hosted group engine, waiting for its durable
    /// resources (including an exclusive WAL lock) to be released.
    pub async fn shutdown_group_engine(
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
    pub async fn shutdown_group_engine_for_simulation(
        &self,
        placement: ShardPlacement,
    ) -> Result<(), RuntimeError> {
        self.shutdown_group_engine(placement).await
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

    /// Atomically appends to affinity-grouped streams in one Raft group.
    ///
    /// This operation deliberately rejects ungrouped or differently grouped
    /// streams: the runtime has no cross-group transaction coordinator.
    pub async fn append_transaction(
        &self,
        request: AppendTransactionRequest,
    ) -> Result<AppendTransactionResponse, RuntimeError> {
        const MAX_OPERATIONS: usize = 64;
        let Some(first) = request.operations.first() else {
            return Err(RuntimeError::InvalidAppendTransaction {
                message: "at least one append operation is required".to_owned(),
            });
        };
        if request.operations.len() > MAX_OPERATIONS {
            return Err(RuntimeError::InvalidAppendTransaction {
                message: format!("at most {MAX_OPERATIONS} append operations are allowed"),
            });
        }
        if request
            .operations
            .iter()
            .any(|operation| operation.payload.is_empty())
        {
            return Err(RuntimeError::InvalidAppendTransaction {
                message: "every append operation must have a non-empty payload".to_owned(),
            });
        }
        let Some(affinity_key) = first.stream_id.affinity_key.as_deref() else {
            return Err(RuntimeError::InvalidAppendTransaction {
                message: "all streams must use an affinity path".to_owned(),
            });
        };
        let placement = self.shard_map.locate(&first.stream_id);
        for operation in &request.operations {
            if operation.stream_id.bucket_id != first.stream_id.bucket_id
                || operation.stream_id.affinity_key.as_deref() != Some(affinity_key)
                || self.shard_map.locate(&operation.stream_id) != placement
            {
                return Err(RuntimeError::InvalidAppendTransaction {
                    message: "all streams must share one bucket and affinity key".to_owned(),
                });
            }
        }
        let incoming_bytes = request.payload_bytes();
        let (response_tx, response_rx) = oneshot::channel();
        self.group_rpc(
            placement,
            Some(incoming_bytes),
            GroupCommand::AppendTransaction {
                request,
                response_tx,
                raft_uncommitted: None,
            },
            response_rx,
        )
        .await
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

/// F14g: a stream GC entry deletes nothing while a live stream of the same
/// name uses its objects. A legacy entry (no generation) shares names with
/// any recreated stream, so it waits for none; an entry naming generation
/// `g` only conflicts with a live incarnation in `g`, which C7 rules out.
fn stream_gc_blocked_by_live_stream(
    entry_generation: Option<u64>,
    live_generation: Option<u64>,
) -> bool {
    match entry_generation {
        None => live_generation.is_some(),
        Some(generation) => live_generation == Some(generation),
    }
}

/// Page ids present in one generation directory of a stream, ignoring any
/// name Ursula does not write there.
async fn list_cold_index_page_ids(
    cold_store: &ColdStoreHandle,
    stream_id: &BucketStreamId,
    generation: u64,
) -> io::Result<Vec<u64>> {
    let dir = cold_index_generation_dir(stream_id, generation);
    Ok(cold_store
        .list_file_names(&dir)
        .await?
        .iter()
        .filter_map(|name| parse_cold_index_page_file_name(name))
        .collect())
}

#[cfg(not(madsim))]
pub(crate) fn unix_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| u64::try_from(duration.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}

#[cfg(madsim)]
pub(crate) fn unix_time_ms() -> u64 {
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

/// Removes every object below `prefix`, then proves the store lists none.
async fn erase_prefix_and_prove(cold_store: &ColdStore, prefix: &str) -> Result<(), RuntimeError> {
    cold_store
        .remove_all(prefix)
        .await
        .map_err(|err| RuntimeError::ColdStoreIo {
            message: err.to_string(),
        })?;
    let absent =
        cold_store
            .prefix_is_empty(prefix)
            .await
            .map_err(|err| RuntimeError::ColdStoreIo {
                message: err.to_string(),
            })?;
    if !absent {
        return Err(RuntimeError::ColdStoreIo {
            message: format!("bucket prefix '{prefix}' still contains objects after erasure"),
        });
    }
    Ok(())
}

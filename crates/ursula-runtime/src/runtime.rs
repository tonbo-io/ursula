use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use bytes::Bytes;
use tokio::sync::{Semaphore, mpsc, oneshot};
use tokio::task::JoinSet;
use ursula_shard::{BucketStreamId, CoreId, RaftGroupId, ShardId, ShardPlacement, StaticShardMap};
use ursula_stream::{ColdChunkRef, ColdFlushCandidate, StreamErrorCode};

use crate::cold_store::{ColdStoreHandle, new_cold_chunk_path};
use crate::command::GroupSnapshot;
use crate::core_worker::{CoreCommand, CoreMailbox, CoreWorker, WaitReadCancel};
use crate::engine::in_memory::InMemoryGroupEngineFactory;
use crate::engine::{GroupEngineError, GroupEngineFactory};
use crate::error::{RuntimeError, map_fork_source_ref_error};
use crate::metrics::{
    COLD_FLUSH_GROUP_BATCH_MAX_CHUNKS, RuntimeMailboxSnapshot, RuntimeMetrics, RuntimeMetricsInner,
    elapsed_ns, is_stale_cold_flush_candidate_error,
};
use crate::request::{
    AppendBatchRequest, AppendBatchResponse, AppendExternalRequest, AppendRequest, AppendResponse,
    BootstrapStreamRequest, BootstrapStreamResponse, CloseStreamRequest, CloseStreamResponse,
    ColdWriteAdmission, CreateStreamExternalRequest, CreateStreamRequest, CreateStreamResponse,
    DeleteSnapshotRequest, DeleteStreamRequest, DeleteStreamResponse, FlushColdRequest,
    FlushColdResponse, ForkRefResponse, HeadStreamRequest, HeadStreamResponse,
    PlanColdFlushRequest, PlanGroupColdFlushRequest, PublishSnapshotRequest,
    PublishSnapshotResponse, ReadSnapshotRequest, ReadSnapshotResponse, ReadStreamRequest,
    ReadStreamResponse,
};

#[derive(Debug, Clone)]
pub struct RuntimeConfig {
    pub core_count: usize,
    pub raft_group_count: usize,
    pub mailbox_capacity: usize,
    pub threading: RuntimeThreading,
    pub cold_max_hot_bytes_per_group: Option<u64>,
    pub live_read_max_waiters_per_core: Option<u64>,
}

impl RuntimeConfig {
    pub fn new(core_count: usize, raft_group_count: usize) -> Self {
        Self {
            core_count,
            raft_group_count,
            mailbox_capacity: 1024,
            threading: RuntimeThreading::ThreadPerCore,
            cold_max_hot_bytes_per_group: None,
            live_read_max_waiters_per_core: Some(65_536),
        }
    }

    pub fn with_cold_max_hot_bytes_per_group(mut self, value: Option<u64>) -> Self {
        self.cold_max_hot_bytes_per_group = value;
        self
    }

    pub fn with_live_read_max_waiters_per_core(mut self, value: Option<u64>) -> Self {
        self.live_read_max_waiters_per_core = value;
        self
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimeThreading {
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

    pub async fn create_stream(
        &self,
        request: CreateStreamRequest,
    ) -> Result<CreateStreamResponse, RuntimeError> {
        if request.forked_from.is_some() {
            return self.create_fork_stream(request).await;
        }
        self.create_stream_on_owner(request).await
    }

    pub async fn create_stream_external(
        &self,
        request: CreateStreamExternalRequest,
    ) -> Result<CreateStreamResponse, RuntimeError> {
        let placement = self.shard_map.locate(&request.stream_id);
        let mailbox = &self.mailboxes[usize::from(placement.core_id.0)];
        let (response_tx, response_rx) = oneshot::channel();
        self.send_core_command(
            mailbox,
            CoreCommand::CreateExternal {
                request,
                placement,
                response_tx,
            },
            response_rx,
        )
        .await
    }

    async fn create_stream_on_owner(
        &self,
        request: CreateStreamRequest,
    ) -> Result<CreateStreamResponse, RuntimeError> {
        let placement = self.shard_map.locate(&request.stream_id);
        let mailbox = &self.mailboxes[usize::from(placement.core_id.0)];
        let (response_tx, response_rx) = oneshot::channel();
        self.send_core_command(
            mailbox,
            CoreCommand::CreateStream {
                request,
                placement,
                response_tx,
            },
            response_rx,
        )
        .await
    }

    async fn create_fork_stream(
        &self,
        mut request: CreateStreamRequest,
    ) -> Result<CreateStreamResponse, RuntimeError> {
        let source_id = request
            .forked_from
            .clone()
            .expect("forked_from checked before create_fork_stream");
        let now_ms = request.now_ms;
        let source_placement = self.shard_map.locate(&source_id);
        let source_head = self
            .head_stream(HeadStreamRequest {
                stream_id: source_id.clone(),
                now_ms,
            })
            .await
            .map_err(|err| map_fork_source_ref_error(err, source_placement))?;

        if request.content_type_explicit {
            if request.content_type != source_head.content_type {
                return Err(RuntimeError::group_engine(
                    source_placement,
                    GroupEngineError::stream(
                        StreamErrorCode::ContentTypeMismatch,
                        format!(
                            "fork content type '{}' does not match source content type '{}'",
                            request.content_type, source_head.content_type
                        ),
                    ),
                ));
            }
        } else {
            request.content_type.clone_from(&source_head.content_type);
        }

        let fork_offset = request.fork_offset.unwrap_or(source_head.tail_offset);
        if fork_offset > source_head.tail_offset {
            return Err(RuntimeError::group_engine(
                source_placement,
                GroupEngineError::stream(
                    StreamErrorCode::InvalidFork,
                    format!(
                        "fork offset {fork_offset} is beyond source stream '{}' tail {}",
                        source_id, source_head.tail_offset
                    ),
                ),
            ));
        }

        let max_len = usize::try_from(fork_offset).map_err(|_| {
            RuntimeError::group_engine(
                source_placement,
                GroupEngineError::stream(
                    StreamErrorCode::InvalidFork,
                    format!("fork offset {fork_offset} cannot fit in memory on this host"),
                ),
            )
        })?;
        request.initial_payload = if fork_offset == 0 {
            Bytes::new()
        } else {
            self.read_stream(ReadStreamRequest {
                stream_id: source_id.clone(),
                offset: 0,
                max_len,
                now_ms,
            })
            .await?
            .payload
            .into()
        };
        self.add_fork_ref_on_owner(source_id.clone(), now_ms)
            .await
            .map_err(|err| map_fork_source_ref_error(err, source_placement))?;
        request.close_after = false;
        request.stream_seq = None;
        request.producer = None;
        if request.stream_ttl_seconds.is_none() && request.stream_expires_at_ms.is_none() {
            request.stream_ttl_seconds = source_head.stream_ttl_seconds;
            request.stream_expires_at_ms = source_head.stream_expires_at_ms;
        }
        request.fork_offset = Some(fork_offset);
        match self.create_stream_on_owner(request).await {
            Ok(response) if response.already_exists => {
                self.release_fork_ref_cascade(source_id).await?;
                Ok(response)
            }
            Ok(response) => Ok(response),
            Err(err) => {
                let _ = self.release_fork_ref_cascade(source_id).await;
                Err(err)
            }
        }
    }

    pub async fn head_stream(
        &self,
        request: HeadStreamRequest,
    ) -> Result<HeadStreamResponse, RuntimeError> {
        let placement = self.shard_map.locate(&request.stream_id);
        let mailbox = &self.mailboxes[usize::from(placement.core_id.0)];
        let (response_tx, response_rx) = oneshot::channel();
        self.send_core_command(
            mailbox,
            CoreCommand::HeadStream {
                request,
                placement,
                response_tx,
            },
            response_rx,
        )
        .await
    }

    pub async fn read_stream(
        &self,
        request: ReadStreamRequest,
    ) -> Result<ReadStreamResponse, RuntimeError> {
        let placement = self.shard_map.locate(&request.stream_id);
        let mailbox = &self.mailboxes[usize::from(placement.core_id.0)];
        let (response_tx, response_rx) = oneshot::channel();
        self.send_core_command(
            mailbox,
            CoreCommand::ReadStream {
                request,
                placement,
                response_tx,
            },
            response_rx,
        )
        .await
    }

    pub async fn publish_snapshot(
        &self,
        request: PublishSnapshotRequest,
    ) -> Result<PublishSnapshotResponse, RuntimeError> {
        let placement = self.shard_map.locate(&request.stream_id);
        let mailbox = &self.mailboxes[usize::from(placement.core_id.0)];
        let (response_tx, response_rx) = oneshot::channel();
        self.send_core_command(
            mailbox,
            CoreCommand::PublishSnapshot {
                request,
                placement,
                response_tx,
            },
            response_rx,
        )
        .await
    }

    pub async fn read_snapshot(
        &self,
        request: ReadSnapshotRequest,
    ) -> Result<ReadSnapshotResponse, RuntimeError> {
        let placement = self.shard_map.locate(&request.stream_id);
        let mailbox = &self.mailboxes[usize::from(placement.core_id.0)];
        let (response_tx, response_rx) = oneshot::channel();
        self.send_core_command(
            mailbox,
            CoreCommand::ReadSnapshot {
                request,
                placement,
                response_tx,
            },
            response_rx,
        )
        .await
    }

    pub async fn delete_snapshot(
        &self,
        request: DeleteSnapshotRequest,
    ) -> Result<(), RuntimeError> {
        let placement = self.shard_map.locate(&request.stream_id);
        let mailbox = &self.mailboxes[usize::from(placement.core_id.0)];
        let (response_tx, response_rx) = oneshot::channel();
        self.send_core_command(
            mailbox,
            CoreCommand::DeleteSnapshot {
                request,
                placement,
                response_tx,
            },
            response_rx,
        )
        .await
    }

    pub async fn bootstrap_stream(
        &self,
        request: BootstrapStreamRequest,
    ) -> Result<BootstrapStreamResponse, RuntimeError> {
        let placement = self.shard_map.locate(&request.stream_id);
        let mailbox = &self.mailboxes[usize::from(placement.core_id.0)];
        let (response_tx, response_rx) = oneshot::channel();
        self.send_core_command(
            mailbox,
            CoreCommand::BootstrapStream {
                request,
                placement,
                response_tx,
            },
            response_rx,
        )
        .await
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
        self.enqueue_core_command(
            mailbox,
            CoreCommand::WaitRead {
                request,
                placement,
                waiter_id,
                response_tx,
            },
        )
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
        let mailbox = &self.mailboxes[usize::from(placement.core_id.0)];
        let (response_tx, response_rx) = oneshot::channel();
        self.send_core_command(
            mailbox,
            CoreCommand::RequireLiveReadOwner {
                placement,
                response_tx,
            },
            response_rx,
        )
        .await
    }

    pub async fn close_stream(
        &self,
        request: CloseStreamRequest,
    ) -> Result<CloseStreamResponse, RuntimeError> {
        let placement = self.shard_map.locate(&request.stream_id);
        let mailbox = &self.mailboxes[usize::from(placement.core_id.0)];
        let (response_tx, response_rx) = oneshot::channel();
        self.send_core_command(
            mailbox,
            CoreCommand::CloseStream {
                request,
                placement,
                response_tx,
            },
            response_rx,
        )
        .await
    }

    pub async fn delete_stream(
        &self,
        request: DeleteStreamRequest,
    ) -> Result<DeleteStreamResponse, RuntimeError> {
        let response = self.delete_stream_on_owner(request).await?;
        if let Some(parent_to_release) = response.parent_to_release.clone() {
            self.release_fork_ref_cascade(parent_to_release).await?;
        }
        Ok(response)
    }

    async fn delete_stream_on_owner(
        &self,
        request: DeleteStreamRequest,
    ) -> Result<DeleteStreamResponse, RuntimeError> {
        let placement = self.shard_map.locate(&request.stream_id);
        let mailbox = &self.mailboxes[usize::from(placement.core_id.0)];
        let (response_tx, response_rx) = oneshot::channel();
        self.send_core_command(
            mailbox,
            CoreCommand::DeleteStream {
                request,
                placement,
                response_tx,
            },
            response_rx,
        )
        .await
    }

    async fn add_fork_ref_on_owner(
        &self,
        stream_id: BucketStreamId,
        now_ms: u64,
    ) -> Result<ForkRefResponse, RuntimeError> {
        let placement = self.shard_map.locate(&stream_id);
        let mailbox = &self.mailboxes[usize::from(placement.core_id.0)];
        let (response_tx, response_rx) = oneshot::channel();
        self.send_core_command(
            mailbox,
            CoreCommand::AddForkRef {
                stream_id,
                now_ms,
                placement,
                response_tx,
            },
            response_rx,
        )
        .await
    }

    async fn release_fork_ref_on_owner(
        &self,
        stream_id: BucketStreamId,
    ) -> Result<ForkRefResponse, RuntimeError> {
        let placement = self.shard_map.locate(&stream_id);
        let mailbox = &self.mailboxes[usize::from(placement.core_id.0)];
        let (response_tx, response_rx) = oneshot::channel();
        self.send_core_command(
            mailbox,
            CoreCommand::ReleaseForkRef {
                stream_id,
                placement,
                response_tx,
            },
            response_rx,
        )
        .await
    }

    async fn release_fork_ref_cascade(
        &self,
        stream_id: BucketStreamId,
    ) -> Result<(), RuntimeError> {
        let mut next = Some(stream_id);
        while let Some(current) = next {
            let response = self.release_fork_ref_on_owner(current).await?;
            next = response.parent_to_release;
        }
        Ok(())
    }

    pub async fn flush_cold(
        &self,
        request: FlushColdRequest,
    ) -> Result<FlushColdResponse, RuntimeError> {
        let placement = self.shard_map.locate(&request.stream_id);
        let mailbox = &self.mailboxes[usize::from(placement.core_id.0)];
        let (response_tx, response_rx) = oneshot::channel();
        self.send_core_command(
            mailbox,
            CoreCommand::FlushCold {
                request,
                placement,
                response_tx,
            },
            response_rx,
        )
        .await
    }

    pub async fn append_external(
        &self,
        request: AppendExternalRequest,
    ) -> Result<AppendResponse, RuntimeError> {
        let placement = self.shard_map.locate(&request.stream_id);
        let mailbox = &self.mailboxes[usize::from(placement.core_id.0)];
        let (response_tx, response_rx) = oneshot::channel();
        self.send_core_command(
            mailbox,
            CoreCommand::AppendExternal {
                request,
                placement,
                response_tx,
            },
            response_rx,
        )
        .await
    }

    pub async fn plan_cold_flush(
        &self,
        request: PlanColdFlushRequest,
    ) -> Result<Option<ColdFlushCandidate>, RuntimeError> {
        let placement = self.shard_map.locate(&request.stream_id);
        let mailbox = &self.mailboxes[usize::from(placement.core_id.0)];
        let (response_tx, response_rx) = oneshot::channel();
        self.send_core_command(
            mailbox,
            CoreCommand::PlanColdFlush {
                request,
                placement,
                response_tx,
            },
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

    pub async fn plan_next_cold_flush(
        &self,
        raft_group_id: RaftGroupId,
        request: PlanGroupColdFlushRequest,
    ) -> Result<Option<ColdFlushCandidate>, RuntimeError> {
        let placement = self.placement_for_group(raft_group_id)?;
        let mailbox = &self.mailboxes[usize::from(placement.core_id.0)];
        let (response_tx, response_rx) = oneshot::channel();
        self.send_core_command(
            mailbox,
            CoreCommand::PlanNextColdFlush {
                request,
                placement,
                response_tx,
            },
            response_rx,
        )
        .await
    }

    pub async fn plan_next_cold_flush_batch(
        &self,
        raft_group_id: RaftGroupId,
        request: PlanGroupColdFlushRequest,
        max_candidates: usize,
    ) -> Result<Vec<ColdFlushCandidate>, RuntimeError> {
        let placement = self.placement_for_group(raft_group_id)?;
        let mailbox = &self.mailboxes[usize::from(placement.core_id.0)];
        let (response_tx, response_rx) = oneshot::channel();
        self.send_core_command(
            mailbox,
            CoreCommand::PlanNextColdFlushBatch {
                request,
                placement,
                max_candidates,
                response_tx,
            },
            response_rx,
        )
        .await
    }

    pub async fn flush_cold_group_once(
        &self,
        raft_group_id: RaftGroupId,
        request: PlanGroupColdFlushRequest,
    ) -> Result<Option<FlushColdResponse>, RuntimeError> {
        let Some(candidate) = self.plan_next_cold_flush(raft_group_id, request).await? else {
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
        match self.flush_cold_candidates_batch(candidates).await {
            Ok(responses) => Ok(responses),
            Err(err) if is_stale_cold_flush_candidate_error(&err) => Ok(Vec::new()),
            Err(err) => Err(err),
        }
    }

    async fn flush_cold_candidate(
        &self,
        candidate: ColdFlushCandidate,
    ) -> Result<FlushColdResponse, RuntimeError> {
        let Some(cold_store) = self.cold_store.as_ref() else {
            return Err(RuntimeError::ColdStoreConfig {
                message: "URSULA_COLD_BACKEND must be configured before flushing cold chunks"
                    .to_owned(),
            });
        };
        let path = new_cold_chunk_path(
            &candidate.stream_id,
            candidate.start_offset,
            candidate.end_offset,
        );
        let upload_started_at = Instant::now();
        let object_size = cold_store
            .write_chunk(&path, &candidate.payload)
            .await
            .map_err(|err| RuntimeError::ColdStoreIo {
                message: err.to_string(),
            })?;
        self.metrics
            .record_cold_upload(object_size, elapsed_ns(upload_started_at));
        let publish_started_at = Instant::now();
        let publish = self
            .flush_cold(FlushColdRequest {
                stream_id: candidate.stream_id,
                chunk: ColdChunkRef {
                    start_offset: candidate.start_offset,
                    end_offset: candidate.end_offset,
                    s3_path: path.clone(),
                    object_size,
                },
            })
            .await;
        match publish {
            Ok(response) => {
                self.metrics
                    .record_cold_publish(object_size, elapsed_ns(publish_started_at));
                Ok(response)
            }
            Err(err) => {
                let cleanup_failed = cold_store.delete_chunk(&path).await.is_err();
                self.metrics
                    .record_cold_orphan_cleanup(object_size, cleanup_failed);
                Err(err)
            }
        }
    }

    pub(crate) async fn flush_cold_candidates_batch(
        &self,
        candidates: Vec<ColdFlushCandidate>,
    ) -> Result<Vec<FlushColdResponse>, RuntimeError> {
        let Some(cold_store) = self.cold_store.as_ref() else {
            return Err(RuntimeError::ColdStoreConfig {
                message: "URSULA_COLD_BACKEND must be configured before flushing cold chunks"
                    .to_owned(),
            });
        };
        let mut requests = Vec::with_capacity(candidates.len());
        let mut uploaded = Vec::with_capacity(candidates.len());
        for candidate in candidates {
            let path = new_cold_chunk_path(
                &candidate.stream_id,
                candidate.start_offset,
                candidate.end_offset,
            );
            let upload_started_at = Instant::now();
            let object_size = cold_store
                .write_chunk(&path, &candidate.payload)
                .await
                .map_err(|err| RuntimeError::ColdStoreIo {
                    message: err.to_string(),
                })?;
            self.metrics
                .record_cold_upload(object_size, elapsed_ns(upload_started_at));
            uploaded.push((path.clone(), object_size));
            requests.push(FlushColdRequest {
                stream_id: candidate.stream_id,
                chunk: ColdChunkRef {
                    start_offset: candidate.start_offset,
                    end_offset: candidate.end_offset,
                    s3_path: path,
                    object_size,
                },
            });
        }

        let placement = self.shard_map.locate(&requests[0].stream_id);
        let mailbox = &self.mailboxes[usize::from(placement.core_id.0)];
        let (response_tx, response_rx) = oneshot::channel();
        let publish_started_at = Instant::now();
        let publish = self
            .send_core_command(
                mailbox,
                CoreCommand::FlushColdBatch {
                    requests,
                    placement,
                    response_tx,
                },
                response_rx,
            )
            .await;
        match publish {
            Ok(responses) => {
                let publish_ns = elapsed_ns(publish_started_at);
                let per_chunk_publish_ns =
                    publish_ns / u64::try_from(uploaded.len()).expect("uploaded len fits u64");
                for (_, object_size) in &uploaded {
                    self.metrics
                        .record_cold_publish(*object_size, per_chunk_publish_ns);
                }
                Ok(responses)
            }
            Err(err) => {
                for (path, object_size) in uploaded {
                    let cleanup_failed = cold_store.delete_chunk(&path).await.is_err();
                    self.metrics
                        .record_cold_orphan_cleanup(object_size, cleanup_failed);
                }
                Err(err)
            }
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

    pub async fn append(&self, request: AppendRequest) -> Result<AppendResponse, RuntimeError> {
        if request.payload.is_empty() {
            return Err(RuntimeError::EmptyAppend);
        }
        let placement = self.shard_map.locate(&request.stream_id);
        let mailbox = &self.mailboxes[usize::from(placement.core_id.0)];
        let (response_tx, response_rx) = oneshot::channel();
        self.send_core_command(
            mailbox,
            CoreCommand::Append {
                request,
                placement,
                response_tx,
            },
            response_rx,
        )
        .await
    }

    pub async fn append_batch(
        &self,
        request: AppendBatchRequest,
    ) -> Result<AppendBatchResponse, RuntimeError> {
        if request.payloads.is_empty() {
            return Err(RuntimeError::EmptyAppend);
        }
        let placement = self.shard_map.locate(&request.stream_id);
        let mailbox = &self.mailboxes[usize::from(placement.core_id.0)];
        let (response_tx, response_rx) = oneshot::channel();
        self.send_core_command(
            mailbox,
            CoreCommand::AppendBatch {
                request,
                placement,
                response_tx,
            },
            response_rx,
        )
        .await
    }

    pub async fn snapshot_group(
        &self,
        raft_group_id: RaftGroupId,
    ) -> Result<GroupSnapshot, RuntimeError> {
        let placement = self.placement_for_group(raft_group_id)?;
        let mailbox = &self.mailboxes[usize::from(placement.core_id.0)];
        let (response_tx, response_rx) = oneshot::channel();
        self.send_core_command(
            mailbox,
            CoreCommand::SnapshotGroup {
                placement,
                response_tx,
            },
            response_rx,
        )
        .await
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
        let mailbox = &self.mailboxes[usize::from(expected.core_id.0)];
        let (response_tx, response_rx) = oneshot::channel();
        self.send_core_command(
            mailbox,
            CoreCommand::InstallGroupSnapshot {
                snapshot,
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
        for raw_group_id in 0..self.shard_map.raft_group_count() {
            self.warm_group(RaftGroupId(raw_group_id)).await?;
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
            .send(command)
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

fn spawn_core_worker(threading: RuntimeThreading, worker: CoreWorker) -> Result<(), RuntimeError> {
    let core_id = worker.core_id;
    match threading {
        RuntimeThreading::HostedTokio => {
            tokio::spawn(worker.run());
            Ok(())
        }
        RuntimeThreading::ThreadPerCore => std::thread::Builder::new()
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
            }),
    }
}

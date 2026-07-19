pub mod in_memory;
pub mod wal;

use std::borrow::Cow;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use serde::Deserialize;
use serde::Serialize;
use ursula_shard::BucketStreamId;
use ursula_shard::ShardPlacement;
use ursula_stream::ColdFlushCandidate;
use ursula_stream::ColdGcEntry;
use ursula_stream::StreamErrorCode;
use ursula_stream::StreamErrorContext;

use crate::command::GroupSnapshot;
use crate::command::GroupWriteCommand;
use crate::metrics::RaftSnapshotBuildSample;
use crate::metrics::RaftWriteManySample;
use crate::metrics::RuntimeMetricsInner;
use crate::request::AckColdGcResponse;
use crate::request::AppendBatchRequest;
use crate::request::AppendExternalRequest;
use crate::request::AppendRequest;
use crate::request::AppendResponse;
use crate::request::BootstrapStreamRequest;
use crate::request::BootstrapStreamResponse;
use crate::request::CloseStreamRequest;
use crate::request::CloseStreamResponse;
use crate::request::ColdHotBacklog;
use crate::request::ColdWriteAdmission;
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
use crate::request::GroupReadStreamParts;
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
use crate::request::TouchStreamAccessResponse;
use crate::request::UpdateStreamAttrsRequest;
use crate::request::UpdateStreamAttrsResponse;

pub type GroupAppendFuture<'a> =
    Pin<Box<dyn Future<Output = Result<AppendResponse, GroupEngineError>> + Send + 'a>>;
pub type GroupAppendBatchFuture<'a> =
    Pin<Box<dyn Future<Output = Result<GroupAppendBatchResponse, GroupEngineError>> + Send + 'a>>;
pub type GroupFlushColdFuture<'a> =
    Pin<Box<dyn Future<Output = Result<FlushColdResponse, GroupEngineError>> + Send + 'a>>;
pub type GroupPlanColdFlushFuture<'a> =
    Pin<Box<dyn Future<Output = Result<Option<ColdFlushCandidate>, GroupEngineError>> + Send + 'a>>;
pub type GroupPlanNextColdFlushFuture<'a> =
    Pin<Box<dyn Future<Output = Result<Option<ColdFlushCandidate>, GroupEngineError>> + Send + 'a>>;
pub type GroupPlanNextColdFlushBatchFuture<'a> =
    Pin<Box<dyn Future<Output = Result<Vec<ColdFlushCandidate>, GroupEngineError>> + Send + 'a>>;
pub type GroupColdHotBacklogFuture<'a> =
    Pin<Box<dyn Future<Output = Result<ColdHotBacklog, GroupEngineError>> + Send + 'a>>;
pub type GroupCreateStreamFuture<'a> =
    Pin<Box<dyn Future<Output = Result<CreateStreamResponse, GroupEngineError>> + Send + 'a>>;
pub type GroupHeadStreamFuture<'a> =
    Pin<Box<dyn Future<Output = Result<HeadStreamResponse, GroupEngineError>> + Send + 'a>>;
pub type GroupGetStreamAttrsFuture<'a> =
    Pin<Box<dyn Future<Output = Result<GetStreamAttrsResponse, GroupEngineError>> + Send + 'a>>;
pub type GroupReadStreamFuture<'a> =
    Pin<Box<dyn Future<Output = Result<ReadStreamResponse, GroupEngineError>> + Send + 'a>>;
pub type GroupReadStreamPartsFuture<'a> =
    Pin<Box<dyn Future<Output = Result<GroupReadStreamParts, GroupEngineError>> + Send + 'a>>;
pub type GroupRequireLiveReadOwnerFuture<'a> =
    Pin<Box<dyn Future<Output = Result<(), GroupEngineError>> + Send + 'a>>;
pub type GroupPublishSnapshotFuture<'a> =
    Pin<Box<dyn Future<Output = Result<PublishSnapshotResponse, GroupEngineError>> + Send + 'a>>;
pub type GroupReadSnapshotFuture<'a> =
    Pin<Box<dyn Future<Output = Result<ReadSnapshotResponse, GroupEngineError>> + Send + 'a>>;
pub type GroupDeleteSnapshotFuture<'a> =
    Pin<Box<dyn Future<Output = Result<(), GroupEngineError>> + Send + 'a>>;
pub type GroupBootstrapStreamFuture<'a> =
    Pin<Box<dyn Future<Output = Result<BootstrapStreamResponse, GroupEngineError>> + Send + 'a>>;
pub type GroupTouchStreamAccessFuture<'a> =
    Pin<Box<dyn Future<Output = Result<TouchStreamAccessResponse, GroupEngineError>> + Send + 'a>>;
pub type GroupUpdateStreamAttrsFuture<'a> =
    Pin<Box<dyn Future<Output = Result<UpdateStreamAttrsResponse, GroupEngineError>> + Send + 'a>>;
pub type GroupCloseStreamFuture<'a> =
    Pin<Box<dyn Future<Output = Result<CloseStreamResponse, GroupEngineError>> + Send + 'a>>;
pub type GroupDeleteStreamFuture<'a> =
    Pin<Box<dyn Future<Output = Result<DeleteStreamResponse, GroupEngineError>> + Send + 'a>>;
pub type GroupAckColdGcFuture<'a> =
    Pin<Box<dyn Future<Output = Result<AckColdGcResponse, GroupEngineError>> + Send + 'a>>;
pub type GroupPlanColdGcFuture<'a> =
    Pin<Box<dyn Future<Output = Result<Vec<ColdGcEntry>, GroupEngineError>> + Send + 'a>>;
pub type GroupSnapshotFuture<'a> =
    Pin<Box<dyn Future<Output = Result<GroupSnapshot, GroupEngineError>> + Send + 'a>>;
pub type GroupInstallSnapshotFuture<'a> =
    Pin<Box<dyn Future<Output = Result<(), GroupEngineError>> + Send + 'a>>;
pub type GroupShutdownFuture<'a> =
    Pin<Box<dyn Future<Output = Result<(), GroupEngineError>> + Send + 'a>>;
pub type GroupWriteBatchFuture<'a> = Pin<
    Box<
        dyn Future<
                Output = Result<
                    Vec<Result<GroupWriteResponse, GroupEngineError>>,
                    GroupEngineError,
                >,
            > + Send
            + 'a,
    >,
>;
pub type GroupEngineCreateFuture<'a> =
    Pin<Box<dyn Future<Output = Result<Box<dyn GroupEngine>, GroupEngineError>> + Send + 'a>>;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GroupAppendBatchResponse {
    pub placement: ShardPlacement,
    pub items: Vec<Result<AppendResponse, GroupEngineError>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum GroupWriteResponse {
    CreateStream(CreateStreamResponse),
    Append(AppendResponse),
    AppendBatch(GroupAppendBatchResponse),
    PublishSnapshot(PublishSnapshotResponse),
    TouchStreamAccess(TouchStreamAccessResponse),
    UpdateStreamAttrs(UpdateStreamAttrsResponse),
    FlushCold(FlushColdResponse),
    CloseStream(CloseStreamResponse),
    DeleteStream(DeleteStreamResponse),
    AckColdGc(AckColdGcResponse),
    Batch(Vec<Result<GroupWriteResponse, GroupEngineError>>),
}

pub trait GroupEngine: Send + 'static {
    fn accepts_local_writes(&self) -> bool {
        true
    }

    fn create_stream<'a>(
        &'a mut self,
        request: CreateStreamRequest,
        placement: ShardPlacement,
    ) -> GroupCreateStreamFuture<'a>;

    fn create_stream_external<'a>(
        &'a mut self,
        request: CreateStreamExternalRequest,
        _placement: ShardPlacement,
    ) -> GroupCreateStreamFuture<'a> {
        Box::pin(async move {
            Err(GroupEngineError::new(format!(
                "external stream create is not supported for stream '{}'",
                request.stream_id
            )))
        })
    }

    fn head_stream<'a>(
        &'a mut self,
        request: HeadStreamRequest,
        placement: ShardPlacement,
    ) -> GroupHeadStreamFuture<'a>;

    fn get_stream_attrs<'a>(
        &'a mut self,
        request: GetStreamAttrsRequest,
        _placement: ShardPlacement,
    ) -> GroupGetStreamAttrsFuture<'a> {
        Box::pin(async move {
            Err(GroupEngineError::new(format!(
                "stream attrs read is not supported for stream '{}'",
                request.stream_id
            )))
        })
    }

    fn read_stream<'a>(
        &'a mut self,
        request: ReadStreamRequest,
        placement: ShardPlacement,
    ) -> GroupReadStreamFuture<'a>;

    fn read_stream_parts<'a>(
        &'a mut self,
        request: ReadStreamRequest,
        placement: ShardPlacement,
    ) -> GroupReadStreamPartsFuture<'a> {
        Box::pin(async move {
            let response = self.read_stream(request, placement).await?;
            Ok(GroupReadStreamParts::from_response(response))
        })
    }

    fn require_local_live_read_owner<'a>(
        &'a mut self,
        _placement: ShardPlacement,
    ) -> GroupRequireLiveReadOwnerFuture<'a> {
        Box::pin(async { Ok(()) })
    }

    fn publish_snapshot<'a>(
        &'a mut self,
        request: PublishSnapshotRequest,
        _placement: ShardPlacement,
    ) -> GroupPublishSnapshotFuture<'a> {
        Box::pin(async move {
            Err(GroupEngineError::new(format!(
                "snapshot publish is not supported for stream '{}'",
                request.stream_id
            )))
        })
    }

    fn read_snapshot<'a>(
        &'a mut self,
        request: ReadSnapshotRequest,
        _placement: ShardPlacement,
    ) -> GroupReadSnapshotFuture<'a> {
        Box::pin(async move {
            Err(GroupEngineError::new(format!(
                "snapshot read is not supported for stream '{}'",
                request.stream_id
            )))
        })
    }

    fn delete_snapshot<'a>(
        &'a mut self,
        request: DeleteSnapshotRequest,
        _placement: ShardPlacement,
    ) -> GroupDeleteSnapshotFuture<'a> {
        Box::pin(async move {
            Err(GroupEngineError::new(format!(
                "snapshot delete is not supported for stream '{}'",
                request.stream_id
            )))
        })
    }

    fn bootstrap_stream<'a>(
        &'a mut self,
        request: BootstrapStreamRequest,
        _placement: ShardPlacement,
    ) -> GroupBootstrapStreamFuture<'a> {
        Box::pin(async move {
            Err(GroupEngineError::new(format!(
                "bootstrap is not supported for stream '{}'",
                request.stream_id
            )))
        })
    }

    fn touch_stream_access<'a>(
        &'a mut self,
        stream_id: BucketStreamId,
        now_ms: u64,
        renew_ttl: bool,
        placement: ShardPlacement,
    ) -> GroupTouchStreamAccessFuture<'a>;

    fn update_stream_attrs<'a>(
        &'a mut self,
        request: UpdateStreamAttrsRequest,
        _placement: ShardPlacement,
    ) -> GroupUpdateStreamAttrsFuture<'a> {
        Box::pin(async move {
            Err(GroupEngineError::new(format!(
                "stream attrs update is not supported for stream '{}'",
                request.stream_id
            )))
        })
    }

    fn close_stream<'a>(
        &'a mut self,
        request: CloseStreamRequest,
        placement: ShardPlacement,
    ) -> GroupCloseStreamFuture<'a>;

    fn delete_stream<'a>(
        &'a mut self,
        request: DeleteStreamRequest,
        placement: ShardPlacement,
    ) -> GroupDeleteStreamFuture<'a>;

    /// Replicated confirmation that cold-GC entries up to `up_to_seq` have been
    /// physically reclaimed; pops them from the queue. Default unsupported.
    fn ack_cold_gc<'a>(
        &'a mut self,
        _up_to_seq: u64,
        _placement: ShardPlacement,
    ) -> GroupAckColdGcFuture<'a> {
        Box::pin(async { Err(GroupEngineError::new("cold GC ack is not supported")) })
    }

    /// Leader-local read of the front of the cold-GC queue for the background
    /// worker to reclaim. Default returns an empty batch.
    fn plan_cold_gc<'a>(
        &'a mut self,
        _max: usize,
        _placement: ShardPlacement,
    ) -> GroupPlanColdGcFuture<'a> {
        Box::pin(async { Ok(Vec::new()) })
    }

    fn append<'a>(
        &'a mut self,
        request: AppendRequest,
        placement: ShardPlacement,
    ) -> GroupAppendFuture<'a>;

    fn append_external<'a>(
        &'a mut self,
        request: AppendExternalRequest,
        _placement: ShardPlacement,
    ) -> GroupAppendFuture<'a> {
        Box::pin(async move {
            Err(GroupEngineError::new(format!(
                "external append is not supported for stream '{}'",
                request.stream_id
            )))
        })
    }

    fn append_batch<'a>(
        &'a mut self,
        request: AppendBatchRequest,
        placement: ShardPlacement,
    ) -> GroupAppendBatchFuture<'a>;

    fn create_stream_with_cold_admission<'a>(
        &'a mut self,
        request: CreateStreamRequest,
        placement: ShardPlacement,
        _admission: ColdWriteAdmission,
    ) -> GroupCreateStreamFuture<'a> {
        self.create_stream(request, placement)
    }

    fn append_with_cold_admission<'a>(
        &'a mut self,
        request: AppendRequest,
        placement: ShardPlacement,
        _admission: ColdWriteAdmission,
    ) -> GroupAppendFuture<'a> {
        self.append(request, placement)
    }

    fn append_batch_with_cold_admission<'a>(
        &'a mut self,
        request: AppendBatchRequest,
        placement: ShardPlacement,
        _admission: ColdWriteAdmission,
    ) -> GroupAppendBatchFuture<'a> {
        self.append_batch(request, placement)
    }

    fn append_batch_many_with_cold_admission<'a>(
        &'a mut self,
        requests: Vec<AppendBatchRequest>,
        placement: ShardPlacement,
        admission: ColdWriteAdmission,
    ) -> GroupWriteBatchFuture<'a> {
        Box::pin(async move {
            let mut responses = Vec::with_capacity(requests.len());
            for request in requests {
                let response = self
                    .append_batch_with_cold_admission(request, placement, admission)
                    .await
                    .map(GroupWriteResponse::AppendBatch);
                responses.push(response);
            }
            Ok(responses)
        })
    }

    fn flush_cold<'a>(
        &'a mut self,
        request: FlushColdRequest,
        _placement: ShardPlacement,
    ) -> GroupFlushColdFuture<'a> {
        Box::pin(async move {
            Err(GroupEngineError::new(format!(
                "cold flush is not supported for stream '{}'",
                request.stream_id
            )))
        })
    }

    fn plan_cold_flush<'a>(
        &'a mut self,
        request: PlanColdFlushRequest,
        _placement: ShardPlacement,
    ) -> GroupPlanColdFlushFuture<'a> {
        Box::pin(async move {
            Err(GroupEngineError::new(format!(
                "cold flush planning is not supported for stream '{}'",
                request.stream_id
            )))
        })
    }

    fn plan_next_cold_flush<'a>(
        &'a mut self,
        _request: PlanGroupColdFlushRequest,
        _placement: ShardPlacement,
    ) -> GroupPlanNextColdFlushFuture<'a> {
        Box::pin(async move {
            Err(GroupEngineError::new(
                "group cold flush planning is not supported",
            ))
        })
    }

    fn plan_next_cold_flush_batch<'a>(
        &'a mut self,
        request: PlanGroupColdFlushRequest,
        placement: ShardPlacement,
        max_candidates: usize,
    ) -> GroupPlanNextColdFlushBatchFuture<'a> {
        Box::pin(async move {
            match self.plan_next_cold_flush(request, placement).await? {
                Some(candidate) if max_candidates > 0 => Ok(vec![candidate]),
                _ => Ok(Vec::new()),
            }
        })
    }

    fn cold_hot_backlog<'a>(
        &'a mut self,
        stream_id: BucketStreamId,
        _placement: ShardPlacement,
    ) -> GroupColdHotBacklogFuture<'a> {
        Box::pin(async move {
            Err(GroupEngineError::new(format!(
                "cold hot backlog is not supported for stream '{stream_id}'"
            )))
        })
    }

    fn snapshot<'a>(&'a mut self, placement: ShardPlacement) -> GroupSnapshotFuture<'a>;

    fn install_snapshot<'a>(
        &'a mut self,
        snapshot: GroupSnapshot,
    ) -> GroupInstallSnapshotFuture<'a>;

    fn shutdown<'a>(&'a mut self) -> GroupShutdownFuture<'a> {
        Box::pin(async { Ok(()) })
    }

    fn write_batch<'a>(
        &'a mut self,
        commands: Vec<GroupWriteCommand>,
        placement: ShardPlacement,
    ) -> GroupWriteBatchFuture<'a> {
        Box::pin(async move {
            let mut responses = Vec::with_capacity(commands.len());
            for command in commands {
                let response = match command {
                    GroupWriteCommand::CreateStream {
                        stream_id,
                        content_type,
                        initial_payload,
                        close_after,
                        stream_seq,
                        producer,
                        stream_ttl_seconds,
                        stream_expires_at_ms,
                        attrs,
                        now_ms,
                    } => self
                        .create_stream(
                            CreateStreamRequest {
                                stream_id,
                                content_type,
                                content_type_explicit: true,
                                initial_payload,
                                close_after,
                                stream_seq,
                                producer,
                                stream_ttl_seconds,
                                stream_expires_at_ms,
                                attrs,
                                now_ms,
                            },
                            placement,
                        )
                        .await
                        .map(GroupWriteResponse::CreateStream),
                    GroupWriteCommand::CreateExternal {
                        stream_id,
                        content_type,
                        initial_payload,
                        record_ends,
                        close_after,
                        stream_seq,
                        producer,
                        stream_ttl_seconds,
                        stream_expires_at_ms,
                        attrs,
                        now_ms,
                    } => self
                        .create_stream_external(
                            CreateStreamExternalRequest {
                                stream_id,
                                content_type,
                                initial_payload,
                                record_ends,
                                close_after,
                                stream_seq,
                                producer,
                                stream_ttl_seconds,
                                stream_expires_at_ms,
                                attrs,
                                now_ms,
                            },
                            placement,
                        )
                        .await
                        .map(GroupWriteResponse::CreateStream),
                    GroupWriteCommand::Append {
                        stream_id,
                        content_type,
                        payload,
                        close_after,
                        stream_seq,
                        producer,
                        now_ms,
                        record_match,
                    } => self
                        .append(
                            AppendRequest {
                                stream_id,
                                content_type,
                                payload,
                                close_after,
                                stream_seq,
                                producer,
                                now_ms,
                                record_match,
                            },
                            placement,
                        )
                        .await
                        .map(GroupWriteResponse::Append),
                    GroupWriteCommand::AppendExternal {
                        stream_id,
                        content_type,
                        payload,
                        record_ends,
                        close_after,
                        stream_seq,
                        producer,
                        now_ms,
                        record_match,
                    } => self
                        .append_external(
                            AppendExternalRequest {
                                stream_id,
                                content_type,
                                payload,
                                record_ends,
                                close_after,
                                stream_seq,
                                producer,
                                now_ms,
                                record_match,
                            },
                            placement,
                        )
                        .await
                        .map(GroupWriteResponse::Append),
                    GroupWriteCommand::AppendBatch {
                        stream_id,
                        content_type,
                        payloads,
                        producer,
                        now_ms,
                    } => self
                        .append_batch(
                            AppendBatchRequest {
                                stream_id,
                                content_type,
                                payloads,
                                producer,
                                now_ms,
                            },
                            placement,
                        )
                        .await
                        .map(GroupWriteResponse::AppendBatch),
                    GroupWriteCommand::PublishSnapshot {
                        stream_id,
                        snapshot_offset,
                        content_type,
                        payload,
                        now_ms,
                    } => self
                        .publish_snapshot(
                            PublishSnapshotRequest {
                                stream_id,
                                snapshot_offset,
                                content_type,
                                payload,
                                now_ms,
                            },
                            placement,
                        )
                        .await
                        .map(GroupWriteResponse::PublishSnapshot),
                    GroupWriteCommand::TouchStreamAccess {
                        stream_id,
                        now_ms,
                        renew_ttl,
                    } => self
                        .touch_stream_access(stream_id, now_ms, renew_ttl, placement)
                        .await
                        .map(GroupWriteResponse::TouchStreamAccess),
                    GroupWriteCommand::UpdateStreamAttrs {
                        stream_id,
                        attrs,
                        now_ms,
                    } => self
                        .update_stream_attrs(
                            UpdateStreamAttrsRequest {
                                stream_id,
                                attrs,
                                now_ms,
                            },
                            placement,
                        )
                        .await
                        .map(GroupWriteResponse::UpdateStreamAttrs),
                    GroupWriteCommand::FlushCold { stream_id, chunk } => self
                        .flush_cold(FlushColdRequest { stream_id, chunk }, placement)
                        .await
                        .map(GroupWriteResponse::FlushCold),
                    GroupWriteCommand::CloseStream {
                        stream_id,
                        stream_seq,
                        producer,
                        now_ms,
                    } => self
                        .close_stream(
                            CloseStreamRequest {
                                stream_id,
                                stream_seq,
                                producer,
                                now_ms,
                            },
                            placement,
                        )
                        .await
                        .map(GroupWriteResponse::CloseStream),
                    GroupWriteCommand::DeleteStream { stream_id } => self
                        .delete_stream(DeleteStreamRequest { stream_id }, placement)
                        .await
                        .map(GroupWriteResponse::DeleteStream),
                    GroupWriteCommand::AckColdGc { up_to_seq } => self
                        .ack_cold_gc(up_to_seq, placement)
                        .await
                        .map(GroupWriteResponse::AckColdGc),
                    GroupWriteCommand::Batch { commands } => self
                        .write_batch(commands, placement)
                        .await
                        .map(GroupWriteResponse::Batch),
                };
                responses.push(response);
            }
            Ok(responses)
        })
    }
}

pub trait GroupEngineFactory: Send + Sync + 'static {
    fn hosts_group(&self, _placement: ShardPlacement) -> bool {
        true
    }

    fn create<'a>(
        &'a self,
        placement: ShardPlacement,
        metrics: GroupEngineMetrics,
    ) -> GroupEngineCreateFuture<'a>;
}

#[derive(Debug, Clone)]
pub struct GroupEngineMetrics {
    pub(crate) inner: Arc<RuntimeMetricsInner>,
}

impl GroupEngineMetrics {
    pub fn record_wal_batch(
        &self,
        placement: ShardPlacement,
        record_count: usize,
        write_ns: u64,
        sync_ns: u64,
    ) {
        self.inner.record_wal_batch(
            placement.core_id,
            placement.raft_group_id,
            u64::try_from(record_count).expect("record count fits u64"),
            write_ns,
            sync_ns,
        );
    }

    pub fn record_raft_write_many(
        &self,
        placement: ShardPlacement,
        command_count: usize,
        logical_command_count: usize,
        response_count: usize,
        submit_ns: u64,
        response_ns: u64,
    ) {
        self.inner.record_raft_write_many(
            placement.core_id,
            placement.raft_group_id,
            RaftWriteManySample {
                command_count: u64::try_from(command_count).expect("command count fits u64"),
                logical_command_count: u64::try_from(logical_command_count)
                    .expect("logical command count fits u64"),
                response_count: u64::try_from(response_count).expect("response count fits u64"),
                submit_ns,
                response_ns,
            },
        );
    }

    pub fn record_raft_apply_batch(
        &self,
        placement: ShardPlacement,
        entry_count: usize,
        apply_ns: u64,
    ) {
        self.inner.record_raft_apply_batch(
            placement.core_id,
            placement.raft_group_id,
            u64::try_from(entry_count).expect("entry count fits u64"),
            apply_ns,
        );
    }

    pub fn record_raft_snapshot_build(
        &self,
        placement: ShardPlacement,
        stream_count: usize,
        body_bytes: usize,
        pointer_bytes: usize,
        build_ns: u64,
        external_upload: bool,
        inline_fallback: bool,
    ) {
        self.inner
            .record_raft_snapshot_build(placement.raft_group_id, RaftSnapshotBuildSample {
                streams: u64::try_from(stream_count).expect("stream count fits u64"),
                body_bytes: u64::try_from(body_bytes).expect("snapshot body bytes fits u64"),
                pointer_bytes: u64::try_from(pointer_bytes)
                    .expect("snapshot pointer bytes fits u64"),
                build_ns,
                external_upload,
                inline_fallback,
            });
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GroupLeaderHint {
    pub node_id: Option<u64>,
    pub address: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamEngineError {
    message: String,
    code: StreamErrorCode,
    next_offset: Option<u64>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    context: Vec<StreamErrorContext>,
}

/// Infra error variants with structured fields render their human message on
/// demand (`message`) instead of storing a denormalized copy alongside the
/// fields. `Internal` is the exception: it carries free-form text with no
/// structured source, so it keeps an owned `message`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum GroupInfraError {
    Internal {
        message: String,
    },
    ProtoDecode {
        field: String,
    },
    ColdBackpressure {
        stream_id: BucketStreamId,
        before_group_hot_bytes: u64,
        after_group_hot_bytes: u64,
        limit: u64,
    },
    RaftUncommittedBackpressure {
        current: u64,
        incoming: u64,
        limit: u64,
    },
}

impl GroupInfraError {
    pub fn internal(message: impl Into<String>) -> Self {
        Self::Internal {
            message: message.into(),
        }
    }

    pub fn proto_decode(field: impl Into<String>) -> Self {
        Self::ProtoDecode {
            field: field.into(),
        }
    }

    pub fn cold_backpressure(
        stream_id: BucketStreamId,
        before_group_hot_bytes: u64,
        after_group_hot_bytes: u64,
        limit: u64,
    ) -> Self {
        Self::ColdBackpressure {
            stream_id,
            before_group_hot_bytes,
            after_group_hot_bytes,
            limit,
        }
    }

    pub fn raft_uncommitted_backpressure(current: u64, incoming: u64, limit: u64) -> Self {
        Self::RaftUncommittedBackpressure {
            current,
            incoming,
            limit,
        }
    }

    pub fn message(&self) -> Cow<'_, str> {
        match self {
            Self::Internal { message } => Cow::Borrowed(message),
            Self::ProtoDecode { field } => Cow::Owned(format!(
                "ProtoDecode: protobuf raft payload missing {field}"
            )),
            Self::ColdBackpressure {
                stream_id,
                before_group_hot_bytes,
                after_group_hot_bytes,
                limit,
            } => Cow::Owned(format!(
                "ColdBackpressure: stream '{stream_id}' would raise group hot bytes from {before_group_hot_bytes} to {after_group_hot_bytes}, above limit {limit}"
            )),
            Self::RaftUncommittedBackpressure {
                current,
                incoming,
                limit,
            } => Cow::Owned(format!(
                "RaftUncommittedBackpressure: group uncommitted bytes {current} plus incoming {incoming} would exceed limit {limit}"
            )),
        }
    }

    pub fn is_cold_backpressure(&self) -> bool {
        matches!(self, Self::ColdBackpressure { .. })
    }

    pub fn is_backpressure(&self) -> bool {
        matches!(
            self,
            Self::ColdBackpressure { .. } | Self::RaftUncommittedBackpressure { .. }
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum GroupEngineError {
    Stream(StreamEngineError),
    Infra(GroupInfraError),
    ForwardToLeader {
        message: String,
        leader_hint: GroupLeaderHint,
    },
}

impl GroupEngineError {
    pub fn new(message: impl Into<String>) -> Self {
        Self::Infra(GroupInfraError::internal(message))
    }

    pub fn cold_backpressure(
        stream_id: BucketStreamId,
        before_group_hot_bytes: u64,
        after_group_hot_bytes: u64,
        limit: u64,
    ) -> Self {
        Self::Infra(GroupInfraError::cold_backpressure(
            stream_id,
            before_group_hot_bytes,
            after_group_hot_bytes,
            limit,
        ))
    }

    pub fn raft_uncommitted_backpressure(current: u64, incoming: u64, limit: u64) -> Self {
        Self::Infra(GroupInfraError::raft_uncommitted_backpressure(
            current, incoming, limit,
        ))
    }

    pub fn stream(code: StreamErrorCode, message: impl Into<String>) -> Self {
        Self::stream_with_next_offset(code, message, None)
    }

    pub fn stream_with_next_offset(
        code: StreamErrorCode,
        message: impl Into<String>,
        next_offset: Option<u64>,
    ) -> Self {
        Self::stream_with_context(code, message, next_offset, vec![])
    }

    pub fn stream_with_context(
        code: StreamErrorCode,
        message: impl Into<String>,
        next_offset: Option<u64>,
        context: Vec<StreamErrorContext>,
    ) -> Self {
        Self::Stream(StreamEngineError {
            message: format!("{code:?}: {}", message.into()),
            code,
            next_offset,
            context,
        })
    }

    pub fn stream_from_replicated(
        message: impl Into<String>,
        code: StreamErrorCode,
        next_offset: Option<u64>,
        context: Vec<StreamErrorContext>,
    ) -> Self {
        Self::Stream(StreamEngineError {
            message: message.into(),
            code,
            next_offset,
            context,
        })
    }

    pub fn forward_to_leader(
        message: impl Into<String>,
        node_id: Option<u64>,
        address: Option<String>,
    ) -> Self {
        Self::ForwardToLeader {
            message: message.into(),
            leader_hint: GroupLeaderHint { node_id, address },
        }
    }

    pub fn message(&self) -> Cow<'_, str> {
        match self {
            Self::Stream(err) => Cow::Borrowed(&err.message),
            Self::Infra(err) => err.message(),
            Self::ForwardToLeader { message, .. } => Cow::Borrowed(message),
        }
    }

    pub fn code(&self) -> Option<StreamErrorCode> {
        match self {
            Self::Stream(err) => Some(err.code),
            Self::Infra(_) | Self::ForwardToLeader { .. } => None,
        }
    }

    pub fn stream_parts(
        &self,
    ) -> Option<(&str, StreamErrorCode, Option<u64>, &[StreamErrorContext])> {
        match self {
            Self::Stream(err) => Some((&err.message, err.code, err.next_offset, &err.context)),
            Self::Infra(_) | Self::ForwardToLeader { .. } => None,
        }
    }

    pub fn next_offset(&self) -> Option<u64> {
        match self {
            Self::Stream(err) => err.next_offset,
            Self::Infra(_) | Self::ForwardToLeader { .. } => None,
        }
    }

    pub fn context(&self) -> &[StreamErrorContext] {
        match self {
            Self::Stream(err) => &err.context,
            Self::Infra(_) | Self::ForwardToLeader { .. } => &[],
        }
    }

    pub fn leader_hint(&self) -> Option<&GroupLeaderHint> {
        match self {
            Self::ForwardToLeader { leader_hint, .. } => Some(leader_hint),
            Self::Stream(_) | Self::Infra(_) => None,
        }
    }

    pub fn infra(&self) -> Option<&GroupInfraError> {
        match self {
            Self::Infra(err) => Some(err),
            Self::Stream(_) | Self::ForwardToLeader { .. } => None,
        }
    }

    pub fn is_cold_backpressure(&self) -> bool {
        self.infra()
            .is_some_and(GroupInfraError::is_cold_backpressure)
    }

    pub fn is_backpressure(&self) -> bool {
        self.infra().is_some_and(GroupInfraError::is_backpressure)
    }
}

impl std::fmt::Display for GroupEngineError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message())
    }
}

impl std::error::Error for GroupEngineError {}

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use ursula_shard::{BucketStreamId, ShardPlacement};
use ursula_stream::{ColdFlushCandidate, StreamErrorCode};

use crate::command::{GroupSnapshot, GroupWriteCommand};
use crate::metrics::{RaftWriteManySample, RuntimeMetricsInner};
use crate::request::{
    AppendBatchRequest, AppendExternalRequest, AppendRequest, AppendResponse,
    BootstrapStreamRequest, BootstrapStreamResponse, CloseStreamRequest, CloseStreamResponse,
    ColdHotBacklog, ColdWriteAdmission, CreateStreamExternalRequest, CreateStreamRequest,
    CreateStreamResponse, DeleteSnapshotRequest, DeleteStreamRequest, DeleteStreamResponse,
    FlushColdRequest, FlushColdResponse, ForkRefResponse, GroupReadStreamParts, HeadStreamRequest,
    HeadStreamResponse, PlanColdFlushRequest, PlanGroupColdFlushRequest, PublishSnapshotRequest,
    PublishSnapshotResponse, ReadSnapshotRequest, ReadSnapshotResponse, ReadStreamRequest,
    ReadStreamResponse, TouchStreamAccessResponse,
};

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
pub type GroupCloseStreamFuture<'a> =
    Pin<Box<dyn Future<Output = Result<CloseStreamResponse, GroupEngineError>> + Send + 'a>>;
pub type GroupDeleteStreamFuture<'a> =
    Pin<Box<dyn Future<Output = Result<DeleteStreamResponse, GroupEngineError>> + Send + 'a>>;
pub type GroupForkRefFuture<'a> =
    Pin<Box<dyn Future<Output = Result<ForkRefResponse, GroupEngineError>> + Send + 'a>>;
pub type GroupSnapshotFuture<'a> =
    Pin<Box<dyn Future<Output = Result<GroupSnapshot, GroupEngineError>> + Send + 'a>>;
pub type GroupInstallSnapshotFuture<'a> =
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
    AddForkRef(ForkRefResponse),
    ReleaseForkRef(ForkRefResponse),
    FlushCold(FlushColdResponse),
    CloseStream(CloseStreamResponse),
    DeleteStream(DeleteStreamResponse),
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

    fn add_fork_ref<'a>(
        &'a mut self,
        stream_id: BucketStreamId,
        now_ms: u64,
        placement: ShardPlacement,
    ) -> GroupForkRefFuture<'a>;

    fn release_fork_ref<'a>(
        &'a mut self,
        stream_id: BucketStreamId,
        placement: ShardPlacement,
    ) -> GroupForkRefFuture<'a>;

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
                        forked_from,
                        fork_offset,
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
                                forked_from,
                                fork_offset,
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
                        close_after,
                        stream_seq,
                        producer,
                        stream_ttl_seconds,
                        stream_expires_at_ms,
                        forked_from,
                        fork_offset,
                        now_ms,
                    } => self
                        .create_stream_external(
                            CreateStreamExternalRequest {
                                stream_id,
                                content_type,
                                initial_payload,
                                close_after,
                                stream_seq,
                                producer,
                                stream_ttl_seconds,
                                stream_expires_at_ms,
                                forked_from,
                                fork_offset,
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
                            },
                            placement,
                        )
                        .await
                        .map(GroupWriteResponse::Append),
                    GroupWriteCommand::AppendExternal {
                        stream_id,
                        content_type,
                        payload,
                        close_after,
                        stream_seq,
                        producer,
                        now_ms,
                    } => self
                        .append_external(
                            AppendExternalRequest {
                                stream_id,
                                content_type,
                                payload,
                                close_after,
                                stream_seq,
                                producer,
                                now_ms,
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
                    GroupWriteCommand::AddForkRef { stream_id, now_ms } => self
                        .add_fork_ref(stream_id, now_ms, placement)
                        .await
                        .map(GroupWriteResponse::AddForkRef),
                    GroupWriteCommand::ReleaseForkRef { stream_id } => self
                        .release_fork_ref(stream_id, placement)
                        .await
                        .map(GroupWriteResponse::ReleaseForkRef),
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
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GroupLeaderHint {
    pub node_id: Option<u64>,
    pub address: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GroupEngineError {
    message: String,
    code: Option<StreamErrorCode>,
    next_offset: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    leader_hint: Option<GroupLeaderHint>,
}

impl GroupEngineError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            code: None,
            next_offset: None,
            leader_hint: None,
        }
    }

    pub fn stream(code: StreamErrorCode, message: impl Into<String>) -> Self {
        Self::stream_with_next_offset(code, message, None)
    }

    pub fn stream_with_next_offset(
        code: StreamErrorCode,
        message: impl Into<String>,
        next_offset: Option<u64>,
    ) -> Self {
        Self {
            message: format!("{code:?}: {}", message.into()),
            code: Some(code),
            next_offset,
            leader_hint: None,
        }
    }

    pub fn forward_to_leader(
        message: impl Into<String>,
        node_id: Option<u64>,
        address: Option<String>,
    ) -> Self {
        Self {
            message: message.into(),
            code: None,
            next_offset: None,
            leader_hint: Some(GroupLeaderHint { node_id, address }),
        }
    }

    pub fn from_replicated_parts(
        message: impl Into<String>,
        code: Option<StreamErrorCode>,
        next_offset: Option<u64>,
        leader_hint: Option<GroupLeaderHint>,
    ) -> Self {
        Self {
            message: message.into(),
            code,
            next_offset,
            leader_hint,
        }
    }

    pub fn message(&self) -> &str {
        &self.message
    }

    pub fn code(&self) -> Option<StreamErrorCode> {
        self.code
    }

    pub fn next_offset(&self) -> Option<u64> {
        self.next_offset
    }

    pub fn leader_hint(&self) -> Option<&GroupLeaderHint> {
        self.leader_hint.as_ref()
    }
}

impl std::fmt::Display for GroupEngineError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for GroupEngineError {}

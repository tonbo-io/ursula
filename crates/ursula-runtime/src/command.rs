use std::fmt;

use serde::Deserialize;
use serde::Serialize;
use ursula_shard::ShardPlacement;
use ursula_stream::StreamCommand;
use ursula_stream::StreamSnapshot;

use crate::request::AppendBatchRequest;
use crate::request::AppendExternalRequest;
use crate::request::AppendRequest;
use crate::request::CloseStreamRequest;
use crate::request::CreateStreamExternalRequest;
use crate::request::CreateStreamRequest;
use crate::request::DeleteStreamRequest;
use crate::request::FlushColdRequest;
use crate::request::PublishSnapshotRequest;
use crate::request::StreamAppendCount;
use crate::request::UpdateStreamAttrsRequest;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GroupSnapshot {
    pub placement: ShardPlacement,
    pub group_commit_index: u64,
    pub stream_snapshot: StreamSnapshot,
    pub stream_append_counts: Vec<StreamAppendCount>,
}

/// Replicated group-level write envelope around the canonical
/// [`StreamCommand`]: either one per-stream command, or an atomic batch of
/// them applied as a single raft entry. This enum (serde-encoded) is the raft
/// log payload; there is no separate wire mirror.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[expect(
    clippy::large_enum_variant,
    reason = "commands are transient write-path values; boxing `Stream` would \
              add a heap allocation to every replicated write"
)]
pub enum GroupWriteCommand {
    Stream(StreamCommand),
    Batch { commands: Vec<StreamCommand> },
}

impl From<StreamCommand> for GroupWriteCommand {
    fn from(command: StreamCommand) -> Self {
        Self::Stream(command)
    }
}

impl From<CreateStreamRequest> for StreamCommand {
    fn from(request: CreateStreamRequest) -> Self {
        Self::CreateStream {
            stream_id: request.stream_id,
            content_type: request.content_type,
            initial_payload: request.initial_payload,
            close_after: request.close_after,
            stream_seq: request.stream_seq,
            producer: request.producer,
            stream_ttl_seconds: request.stream_ttl_seconds,
            stream_expires_at_ms: request.stream_expires_at_ms,
            attrs: request.attrs,
            now_ms: request.now_ms,
        }
    }
}

impl From<CreateStreamExternalRequest> for StreamCommand {
    fn from(request: CreateStreamExternalRequest) -> Self {
        Self::CreateExternal {
            stream_id: request.stream_id,
            content_type: request.content_type,
            initial_payload: request.initial_payload,
            record_ends: request.record_ends,
            close_after: request.close_after,
            stream_seq: request.stream_seq,
            producer: request.producer,
            stream_ttl_seconds: request.stream_ttl_seconds,
            stream_expires_at_ms: request.stream_expires_at_ms,
            attrs: request.attrs,
            now_ms: request.now_ms,
        }
    }
}

impl From<UpdateStreamAttrsRequest> for StreamCommand {
    fn from(request: UpdateStreamAttrsRequest) -> Self {
        Self::UpdateStreamAttrs {
            stream_id: request.stream_id,
            attrs: request.attrs,
            now_ms: request.now_ms,
        }
    }
}

impl From<AppendRequest> for StreamCommand {
    fn from(request: AppendRequest) -> Self {
        Self::Append {
            stream_id: request.stream_id,
            content_type: Some(request.content_type),
            payload: request.payload,
            close_after: request.close_after,
            stream_seq: request.stream_seq,
            producer: request.producer,
            now_ms: request.now_ms,
            record_match: request.record_match,
        }
    }
}

impl From<AppendExternalRequest> for StreamCommand {
    fn from(request: AppendExternalRequest) -> Self {
        Self::AppendExternal {
            stream_id: request.stream_id,
            content_type: Some(request.content_type),
            payload: request.payload,
            record_ends: request.record_ends,
            close_after: request.close_after,
            stream_seq: request.stream_seq,
            producer: request.producer,
            now_ms: request.now_ms,
            record_match: request.record_match,
        }
    }
}

impl From<AppendBatchRequest> for StreamCommand {
    fn from(request: AppendBatchRequest) -> Self {
        Self::AppendBatch {
            stream_id: request.stream_id,
            content_type: Some(request.content_type),
            payloads: request.payloads,
            producer: request.producer,
            now_ms: request.now_ms,
        }
    }
}

impl From<PublishSnapshotRequest> for StreamCommand {
    fn from(request: PublishSnapshotRequest) -> Self {
        Self::PublishSnapshot {
            stream_id: request.stream_id,
            snapshot_offset: request.snapshot_offset,
            content_type: request.content_type,
            payload: request.payload,
            now_ms: request.now_ms,
        }
    }
}

impl From<CloseStreamRequest> for StreamCommand {
    fn from(request: CloseStreamRequest) -> Self {
        Self::Close {
            stream_id: request.stream_id,
            stream_seq: request.stream_seq,
            producer: request.producer,
            now_ms: request.now_ms,
        }
    }
}

impl From<DeleteStreamRequest> for StreamCommand {
    fn from(request: DeleteStreamRequest) -> Self {
        Self::DeleteStream {
            stream_id: request.stream_id,
        }
    }
}

impl From<FlushColdRequest> for StreamCommand {
    fn from(request: FlushColdRequest) -> Self {
        Self::FlushCold {
            stream_id: request.stream_id,
            chunk: request.chunk,
        }
    }
}

macro_rules! group_write_from_request {
    ($($request:ty),+ $(,)?) => {
        $(impl From<$request> for GroupWriteCommand {
            fn from(request: $request) -> Self {
                Self::Stream(StreamCommand::from(request))
            }
        })+
    };
}

group_write_from_request!(
    CreateStreamRequest,
    CreateStreamExternalRequest,
    UpdateStreamAttrsRequest,
    AppendRequest,
    AppendExternalRequest,
    AppendBatchRequest,
    PublishSnapshotRequest,
    CloseStreamRequest,
    DeleteStreamRequest,
    FlushColdRequest,
);

impl fmt::Display for GroupWriteCommand {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Stream(command) => command.fmt(f),
            Self::Batch { commands } => write!(f, "batch:{} commands", commands.len()),
        }
    }
}

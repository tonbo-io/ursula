use std::fmt;

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use ursula_shard::{BucketStreamId, ShardPlacement};
use ursula_stream::{ColdChunkRef, ExternalPayloadRef, ProducerRequest, StreamSnapshot};

use crate::request::{
    AppendBatchRequest, AppendExternalRequest, AppendRequest, CloseStreamRequest,
    CreateStreamExternalRequest, CreateStreamRequest, DeleteStreamRequest, FlushColdRequest,
    PublishSnapshotRequest, StreamAppendCount,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GroupSnapshot {
    pub placement: ShardPlacement,
    pub group_commit_index: u64,
    pub stream_snapshot: StreamSnapshot,
    pub stream_append_counts: Vec<StreamAppendCount>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "command", rename_all = "snake_case")]
pub enum GroupWriteCommand {
    CreateStream {
        stream_id: BucketStreamId,
        content_type: String,
        initial_payload: Bytes,
        close_after: bool,
        stream_seq: Option<String>,
        producer: Option<ProducerRequest>,
        stream_ttl_seconds: Option<u64>,
        stream_expires_at_ms: Option<u64>,
        forked_from: Option<BucketStreamId>,
        fork_offset: Option<u64>,
        now_ms: u64,
    },
    CreateExternal {
        stream_id: BucketStreamId,
        content_type: String,
        initial_payload: ExternalPayloadRef,
        close_after: bool,
        stream_seq: Option<String>,
        producer: Option<ProducerRequest>,
        stream_ttl_seconds: Option<u64>,
        stream_expires_at_ms: Option<u64>,
        forked_from: Option<BucketStreamId>,
        fork_offset: Option<u64>,
        now_ms: u64,
    },
    Append {
        stream_id: BucketStreamId,
        content_type: String,
        payload: Bytes,
        close_after: bool,
        stream_seq: Option<String>,
        producer: Option<ProducerRequest>,
        now_ms: u64,
    },
    AppendExternal {
        stream_id: BucketStreamId,
        content_type: String,
        payload: ExternalPayloadRef,
        close_after: bool,
        stream_seq: Option<String>,
        producer: Option<ProducerRequest>,
        now_ms: u64,
    },
    AppendBatch {
        stream_id: BucketStreamId,
        content_type: String,
        payloads: Vec<Bytes>,
        producer: Option<ProducerRequest>,
        now_ms: u64,
    },
    PublishSnapshot {
        stream_id: BucketStreamId,
        snapshot_offset: u64,
        content_type: String,
        payload: Bytes,
        now_ms: u64,
    },
    TouchStreamAccess {
        stream_id: BucketStreamId,
        now_ms: u64,
        renew_ttl: bool,
    },
    AddForkRef {
        stream_id: BucketStreamId,
        now_ms: u64,
    },
    ReleaseForkRef {
        stream_id: BucketStreamId,
    },
    FlushCold {
        stream_id: BucketStreamId,
        chunk: ColdChunkRef,
    },
    CloseStream {
        stream_id: BucketStreamId,
        stream_seq: Option<String>,
        producer: Option<ProducerRequest>,
        now_ms: u64,
    },
    DeleteStream {
        stream_id: BucketStreamId,
    },
    Batch {
        commands: Vec<GroupWriteCommand>,
    },
}

impl From<CreateStreamRequest> for GroupWriteCommand {
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
            forked_from: request.forked_from,
            fork_offset: request.fork_offset,
            now_ms: request.now_ms,
        }
    }
}

impl From<&CreateStreamRequest> for GroupWriteCommand {
    fn from(request: &CreateStreamRequest) -> Self {
        Self::CreateStream {
            stream_id: request.stream_id.clone(),
            content_type: request.content_type.clone(),
            initial_payload: request.initial_payload.clone(),
            close_after: request.close_after,
            stream_seq: request.stream_seq.clone(),
            producer: request.producer.clone(),
            stream_ttl_seconds: request.stream_ttl_seconds,
            stream_expires_at_ms: request.stream_expires_at_ms,
            forked_from: request.forked_from.clone(),
            fork_offset: request.fork_offset,
            now_ms: request.now_ms,
        }
    }
}

impl From<CreateStreamExternalRequest> for GroupWriteCommand {
    fn from(request: CreateStreamExternalRequest) -> Self {
        Self::CreateExternal {
            stream_id: request.stream_id,
            content_type: request.content_type,
            initial_payload: request.initial_payload,
            close_after: request.close_after,
            stream_seq: request.stream_seq,
            producer: request.producer,
            stream_ttl_seconds: request.stream_ttl_seconds,
            stream_expires_at_ms: request.stream_expires_at_ms,
            forked_from: request.forked_from,
            fork_offset: request.fork_offset,
            now_ms: request.now_ms,
        }
    }
}

impl From<&CreateStreamExternalRequest> for GroupWriteCommand {
    fn from(request: &CreateStreamExternalRequest) -> Self {
        Self::CreateExternal {
            stream_id: request.stream_id.clone(),
            content_type: request.content_type.clone(),
            initial_payload: request.initial_payload.clone(),
            close_after: request.close_after,
            stream_seq: request.stream_seq.clone(),
            producer: request.producer.clone(),
            stream_ttl_seconds: request.stream_ttl_seconds,
            stream_expires_at_ms: request.stream_expires_at_ms,
            forked_from: request.forked_from.clone(),
            fork_offset: request.fork_offset,
            now_ms: request.now_ms,
        }
    }
}

impl From<AppendRequest> for GroupWriteCommand {
    fn from(request: AppendRequest) -> Self {
        Self::Append {
            stream_id: request.stream_id,
            content_type: request.content_type,
            payload: request.payload,
            close_after: request.close_after,
            stream_seq: request.stream_seq,
            producer: request.producer,
            now_ms: request.now_ms,
        }
    }
}

impl From<&AppendRequest> for GroupWriteCommand {
    fn from(request: &AppendRequest) -> Self {
        Self::Append {
            stream_id: request.stream_id.clone(),
            content_type: request.content_type.clone(),
            payload: request.payload.clone(),
            close_after: request.close_after,
            stream_seq: request.stream_seq.clone(),
            producer: request.producer.clone(),
            now_ms: request.now_ms,
        }
    }
}

impl From<AppendExternalRequest> for GroupWriteCommand {
    fn from(request: AppendExternalRequest) -> Self {
        Self::AppendExternal {
            stream_id: request.stream_id,
            content_type: request.content_type,
            payload: request.payload,
            close_after: request.close_after,
            stream_seq: request.stream_seq,
            producer: request.producer,
            now_ms: request.now_ms,
        }
    }
}

impl From<&AppendExternalRequest> for GroupWriteCommand {
    fn from(request: &AppendExternalRequest) -> Self {
        Self::AppendExternal {
            stream_id: request.stream_id.clone(),
            content_type: request.content_type.clone(),
            payload: request.payload.clone(),
            close_after: request.close_after,
            stream_seq: request.stream_seq.clone(),
            producer: request.producer.clone(),
            now_ms: request.now_ms,
        }
    }
}

impl From<AppendBatchRequest> for GroupWriteCommand {
    fn from(request: AppendBatchRequest) -> Self {
        Self::AppendBatch {
            stream_id: request.stream_id,
            content_type: request.content_type,
            payloads: request.payloads,
            producer: request.producer,
            now_ms: request.now_ms,
        }
    }
}

impl From<&AppendBatchRequest> for GroupWriteCommand {
    fn from(request: &AppendBatchRequest) -> Self {
        Self::AppendBatch {
            stream_id: request.stream_id.clone(),
            content_type: request.content_type.clone(),
            payloads: request.payloads.clone(),
            producer: request.producer.clone(),
            now_ms: request.now_ms,
        }
    }
}

impl From<PublishSnapshotRequest> for GroupWriteCommand {
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

impl From<&PublishSnapshotRequest> for GroupWriteCommand {
    fn from(request: &PublishSnapshotRequest) -> Self {
        Self::PublishSnapshot {
            stream_id: request.stream_id.clone(),
            snapshot_offset: request.snapshot_offset,
            content_type: request.content_type.clone(),
            payload: request.payload.clone(),
            now_ms: request.now_ms,
        }
    }
}

impl From<CloseStreamRequest> for GroupWriteCommand {
    fn from(request: CloseStreamRequest) -> Self {
        Self::CloseStream {
            stream_id: request.stream_id,
            stream_seq: request.stream_seq,
            producer: request.producer,
            now_ms: request.now_ms,
        }
    }
}

impl From<&CloseStreamRequest> for GroupWriteCommand {
    fn from(request: &CloseStreamRequest) -> Self {
        Self::CloseStream {
            stream_id: request.stream_id.clone(),
            stream_seq: request.stream_seq.clone(),
            producer: request.producer.clone(),
            now_ms: request.now_ms,
        }
    }
}

impl From<DeleteStreamRequest> for GroupWriteCommand {
    fn from(request: DeleteStreamRequest) -> Self {
        Self::DeleteStream {
            stream_id: request.stream_id,
        }
    }
}

impl From<&DeleteStreamRequest> for GroupWriteCommand {
    fn from(request: &DeleteStreamRequest) -> Self {
        Self::DeleteStream {
            stream_id: request.stream_id.clone(),
        }
    }
}

impl From<FlushColdRequest> for GroupWriteCommand {
    fn from(request: FlushColdRequest) -> Self {
        Self::FlushCold {
            stream_id: request.stream_id,
            chunk: request.chunk,
        }
    }
}

impl From<&FlushColdRequest> for GroupWriteCommand {
    fn from(request: &FlushColdRequest) -> Self {
        Self::FlushCold {
            stream_id: request.stream_id.clone(),
            chunk: request.chunk.clone(),
        }
    }
}

impl fmt::Display for GroupWriteCommand {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CreateStream { stream_id, .. } => {
                write!(f, "create_stream:{stream_id}")
            }
            Self::CreateExternal {
                stream_id,
                initial_payload,
                ..
            } => {
                write!(
                    f,
                    "create_external:{stream_id}:{} bytes",
                    initial_payload.payload_len
                )
            }
            Self::Append {
                stream_id, payload, ..
            } => {
                write!(f, "append:{stream_id}:{} bytes", payload.len())
            }
            Self::AppendExternal {
                stream_id, payload, ..
            } => {
                write!(
                    f,
                    "append_external:{stream_id}:{} bytes",
                    payload.payload_len
                )
            }
            Self::AppendBatch {
                stream_id,
                payloads,
                ..
            } => {
                write!(f, "append_batch:{stream_id}:{} items", payloads.len())
            }
            Self::PublishSnapshot {
                stream_id,
                snapshot_offset,
                payload,
                ..
            } => {
                write!(
                    f,
                    "publish_snapshot:{stream_id}:{snapshot_offset}:{} bytes",
                    payload.len()
                )
            }
            Self::TouchStreamAccess {
                stream_id,
                renew_ttl,
                ..
            } => {
                write!(f, "touch_stream_access:{stream_id}:renew_ttl={renew_ttl}")
            }
            Self::AddForkRef { stream_id, .. } => {
                write!(f, "add_fork_ref:{stream_id}")
            }
            Self::ReleaseForkRef { stream_id } => {
                write!(f, "release_fork_ref:{stream_id}")
            }
            Self::FlushCold { stream_id, chunk } => {
                write!(
                    f,
                    "flush_cold:{stream_id}:{}..{}",
                    chunk.start_offset, chunk.end_offset
                )
            }
            Self::CloseStream { stream_id, .. } => {
                write!(f, "close_stream:{stream_id}")
            }
            Self::DeleteStream { stream_id } => {
                write!(f, "delete_stream:{stream_id}")
            }
            Self::Batch { commands } => {
                write!(f, "batch:{} commands", commands.len())
            }
        }
    }
}

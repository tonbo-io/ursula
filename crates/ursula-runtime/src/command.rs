use std::fmt;

use serde::Deserialize;
use serde::Serialize;
use ursula_shard::ShardPlacement;
use ursula_stream::StreamCommand;
use ursula_stream::StreamSnapshot;

use crate::request::AdvanceRetentionRequest;
use crate::request::AppendExternalRequest;
use crate::request::AppendRequest;
use crate::request::CloseStreamRequest;
use crate::request::CompactColdRequest;
use crate::request::CreateStreamExternalRequest;
use crate::request::CreateStreamRequest;
use crate::request::DeleteStreamRequest;
use crate::request::FlushColdRequest;
use crate::request::PublishSnapshotRequest;
use crate::request::StreamAppendCount;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GroupSnapshot {
    #[serde(default)]
    pub replica_fence_index: u64,
    #[serde(default)]
    pub replica_identities: std::collections::BTreeMap<u64, ursula_proto::admin::ReplicaIdentity>,
    pub placement: ShardPlacement,
    pub group_commit_index: u64,
    pub stream_snapshot: StreamSnapshot,
    pub stream_append_counts: Vec<StreamAppendCount>,
}

/// Replicated group-level write envelope around the canonical
/// [`StreamCommand`]: one command per Raft entry. This enum (serde-encoded) is
/// the Raft log payload; there is no separate wire mirror.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum GroupWriteCommand {
    InstallReplicaIdentity {
        node_id: u64,
        expected: Option<ursula_proto::admin::ReplicaIdentity>,
        replacement: ursula_proto::admin::ReplicaIdentity,
    },
    Stream(StreamCommand),
}

impl GroupWriteCommand {
    /// Approximate Raft-log bytes of this command (bounded-state F12e).
    pub fn log_bytes_estimate(&self) -> u64 {
        match self {
            Self::Stream(command) => command.log_bytes_estimate(),
            Self::InstallReplicaIdentity { .. } => ursula_stream::COMMAND_LOG_OVERHEAD_BYTES,
        }
    }
}

impl From<StreamCommand> for GroupWriteCommand {
    fn from(command: StreamCommand) -> Self {
        Self::Stream(command)
    }
}

impl From<crate::request::ImportGroupStateRequest> for StreamCommand {
    fn from(request: crate::request::ImportGroupStateRequest) -> Self {
        Self::ImportSnapshot {
            snapshot: request.snapshot,
        }
    }
}

/// Wraps `command` in the `Stream-Incarnation` precondition (D12) when the
/// request carried one; a request without it proposes `command` unchanged.
fn if_incarnation(command: StreamCommand, incarnation: Option<u64>) -> StreamCommand {
    match incarnation {
        Some(incarnation) => StreamCommand::IfIncarnation {
            incarnation,
            command: Box::new(command),
        },
        None => command,
    }
}

impl From<CreateStreamRequest> for StreamCommand {
    fn from(request: CreateStreamRequest) -> Self {
        let command = Self::CreateStream {
            stream_id: request.stream_id,
            content_type: request.content_type,
            initial_payload: request.initial_payload,
            close_after: request.close_after,
            stream_seq: request.stream_seq,
            producer: request.producer,
            stream_ttl_seconds: request.stream_ttl_seconds,
            stream_expires_at_ms: request.stream_expires_at_ms,
            now_ms: request.now_ms,
        };
        if_incarnation(command, request.if_incarnation)
    }
}

impl From<CreateStreamExternalRequest> for StreamCommand {
    fn from(request: CreateStreamExternalRequest) -> Self {
        let command = Self::CreateExternal {
            stream_id: request.stream_id,
            content_type: request.content_type,
            initial_payload: request.initial_payload,
            record_ends: request.record_ends,
            close_after: request.close_after,
            stream_seq: request.stream_seq,
            producer: request.producer,
            stream_ttl_seconds: request.stream_ttl_seconds,
            stream_expires_at_ms: request.stream_expires_at_ms,
            now_ms: request.now_ms,
        };
        if_incarnation(command, request.if_incarnation)
    }
}

impl From<AppendRequest> for StreamCommand {
    fn from(request: AppendRequest) -> Self {
        let command = Self::Append {
            stream_id: request.stream_id,
            content_type: Some(request.content_type),
            payload: request.payload,
            close_after: request.close_after,
            stream_seq: request.stream_seq,
            producer: request.producer,
            now_ms: request.now_ms,
        };
        if_incarnation(command, request.if_incarnation)
    }
}

impl From<AppendExternalRequest> for StreamCommand {
    fn from(request: AppendExternalRequest) -> Self {
        let command = Self::AppendExternal {
            stream_id: request.stream_id,
            content_type: Some(request.content_type),
            payload: request.payload,
            record_ends: request.record_ends,
            close_after: request.close_after,
            stream_seq: request.stream_seq,
            producer: request.producer,
            now_ms: request.now_ms,
        };
        if_incarnation(command, request.if_incarnation)
    }
}

impl From<PublishSnapshotRequest> for StreamCommand {
    fn from(request: PublishSnapshotRequest) -> Self {
        let command = match request.cold_body {
            Some(body) => Self::PublishSnapshotExternal {
                stream_id: request.stream_id,
                snapshot_offset: request.snapshot_offset,
                content_type: request.content_type,
                object: body.object,
                digest: body.digest,
                now_ms: request.now_ms,
                expected_incarnation: request.expected_incarnation,
            },
            None => Self::PublishSnapshot {
                stream_id: request.stream_id,
                snapshot_offset: request.snapshot_offset,
                content_type: request.content_type,
                payload: request.payload,
                now_ms: request.now_ms,
                expected_incarnation: request.expected_incarnation,
            },
        };
        if_incarnation(command, request.if_incarnation)
    }
}

impl From<AdvanceRetentionRequest> for StreamCommand {
    fn from(request: AdvanceRetentionRequest) -> Self {
        let command = Self::AdvanceRetention {
            stream_id: request.stream_id,
            retained_offset: request.retained_offset,
            now_ms: request.now_ms,
            expected_incarnation: request.expected_incarnation,
        };
        if_incarnation(command, request.if_incarnation)
    }
}

impl From<CloseStreamRequest> for StreamCommand {
    fn from(request: CloseStreamRequest) -> Self {
        let command = Self::Close {
            stream_id: request.stream_id,
            stream_seq: request.stream_seq,
            producer: request.producer,
            now_ms: request.now_ms,
        };
        if_incarnation(command, request.if_incarnation)
    }
}

impl From<DeleteStreamRequest> for StreamCommand {
    fn from(request: DeleteStreamRequest) -> Self {
        let command = Self::DeleteStream {
            stream_id: request.stream_id,
        };
        if_incarnation(command, request.if_incarnation)
    }
}

impl From<FlushColdRequest> for StreamCommand {
    fn from(request: FlushColdRequest) -> Self {
        Self::FlushCold {
            stream_id: request.stream_id,
            chunk: request.chunk,
            cold_generation: request.cold_generation,
        }
    }
}

impl From<CompactColdRequest> for StreamCommand {
    fn from(request: CompactColdRequest) -> Self {
        Self::CompactCold {
            stream_id: request.stream_id,
            old_chunks: request.old_chunks,
            replacement: request.replacement,
            gc_not_before_ms: request.gc_not_before_ms,
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
    AppendRequest,
    AppendExternalRequest,
    PublishSnapshotRequest,
    AdvanceRetentionRequest,
    CloseStreamRequest,
    DeleteStreamRequest,
    FlushColdRequest,
    CompactColdRequest,
);

impl fmt::Display for GroupWriteCommand {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Stream(command) => command.fmt(f),
            Self::InstallReplicaIdentity { node_id, .. } => {
                write!(f, "install replica identity for node {node_id}")
            }
        }
    }
}

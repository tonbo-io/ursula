use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::fmt;
use std::fmt::Debug;
use std::fs;
use std::fs::File;
use std::fs::OpenOptions;
use std::future::Future;
use std::io;
use std::io::BufRead;
use std::io::BufReader;
use std::io::Cursor;
use std::io::Read;
use std::io::Write;
use std::ops::RangeBounds;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::MutexGuard;
use std::sync::OnceLock;
use std::sync::mpsc;
use std::time::Duration;
use std::time::Instant;

use futures_util::Stream;
use futures_util::TryStreamExt;
use openraft::BasicNode;
use openraft::Config;
use openraft::Entry;
use openraft::EntryPayload;
use openraft::Membership;
use openraft::OptionalSend;
use openraft::Raft;
use openraft::RaftNetworkFactory;
use openraft::RaftNetworkV2;
use openraft::alias::EntryOf;
use openraft::alias::LogIdOf;
use openraft::alias::SnapshotDataOf;
use openraft::alias::SnapshotMetaOf;
use openraft::alias::SnapshotOf;
use openraft::alias::StoredMembershipOf;
use openraft::alias::VoteOf;
use openraft::entry::RaftEntry;
use openraft::error::NetworkError;
use openraft::error::RPCError;
use openraft::error::ReplicationClosed;
use openraft::error::StreamingError;
use openraft::error::Unreachable;
use openraft::network::RPCOption;
use openraft::raft::AppendEntriesRequest;
use openraft::raft::AppendEntriesResponse;
use openraft::raft::SnapshotResponse;
use openraft::raft::VoteRequest;
use openraft::raft::VoteResponse;
use openraft::rt::WatchReceiver;
use openraft::storage::EntryResponder;
use openraft::storage::IOFlushed;
use openraft::storage::LogState;
use openraft::storage::RaftLogReader;
use openraft::storage::RaftLogStorage;
use openraft::storage::RaftSnapshotBuilder;
use openraft::storage::RaftStateMachine;
use openraft::type_config::alias::SnapshotOf as TypeConfigSnapshotOf;
use prost::Message;
use serde::Deserialize;
use serde::Deserializer;
use serde::Serialize;
use serde::Serializer;
use serde::de::DeserializeOwned;
use tonic::transport::Channel;
use tonic::transport::Endpoint;
use ursula_proto as raft_app_proto;
use ursula_runtime::{
    AppendBatchRequest, AppendExternalRequest, AppendRequest, AppendResponse,
    BootstrapStreamRequest, BootstrapStreamResponse, CloseStreamRequest, CloseStreamResponse,
    ColdFlushCandidate, ColdHotBacklog, ColdStoreHandle, ColdWriteAdmission,
    CreateStreamExternalRequest, CreateStreamRequest, CreateStreamResponse, DeleteSnapshotRequest,
    DeleteStreamRequest, DeleteStreamResponse, FlushColdRequest, FlushColdResponse,
    ForkRefResponse, GroupAppendBatchFuture, GroupAppendBatchResponse, GroupAppendFuture,
    GroupBootstrapStreamFuture, GroupCloseStreamFuture, GroupColdHotBacklogFuture,
    GroupCreateStreamFuture, GroupDeleteSnapshotFuture, GroupDeleteStreamFuture, GroupEngine,
    GroupEngineCreateFuture, GroupEngineError, GroupEngineFactory, GroupEngineMetrics,
    GroupFlushColdFuture, GroupForkRefFuture, GroupHeadStreamFuture, GroupInstallSnapshotFuture,
    GroupLeaderHint, GroupPlanColdFlushFuture, GroupPlanNextColdFlushBatchFuture,
    GroupPlanNextColdFlushFuture, GroupPublishSnapshotFuture, GroupReadSnapshotFuture,
    GroupReadStreamFuture, GroupReadStreamParts, GroupReadStreamPartsFuture, GroupSnapshot,
    GroupSnapshotFuture, GroupTouchStreamAccessFuture, GroupWriteBatchFuture, GroupWriteCommand,
    GroupWriteResponse, HeadStreamRequest, HeadStreamResponse, InMemoryGroupEngine,
    PlanColdFlushRequest, PlanGroupColdFlushRequest, PublishSnapshotRequest,
    PublishSnapshotResponse, ReadSnapshotRequest, ReadSnapshotResponse, ReadStreamRequest,
    ReadStreamResponse, StreamErrorCode, TouchStreamAccessResponse,
};
use ursula_shard::BucketStreamId;
use ursula_shard::CoreId;
use ursula_shard::RaftGroupId;
use ursula_shard::ShardId;
use ursula_shard::ShardPlacement;

const CORE_LOG_GROUP_COMMIT_DELAY: Duration = Duration::from_micros(200);
const CORE_LOG_GROUP_COMMIT_MAX_BATCH: usize = 1024;
static GRPC_LEADER_CHANNELS: OnceLock<Mutex<BTreeMap<String, Channel>>> = OnceLock::new();

openraft::declare_raft_types!(
    pub UrsulaRaftTypeConfig:
        D = RaftGroupCommand,
        R = RaftGroupResponse,
        Node = openraft::BasicNode,
        SnapshotData = Cursor<Vec<u8>>,
);

pub type UrsulaAppendEntriesRequest = AppendEntriesRequest<UrsulaRaftTypeConfig>;
pub type UrsulaAppendEntriesResponse = AppendEntriesResponse<UrsulaRaftTypeConfig>;
pub type UrsulaVote = VoteOf<UrsulaRaftTypeConfig>;
pub type UrsulaVoteRequest = VoteRequest<UrsulaRaftTypeConfig>;
pub type UrsulaVoteResponse = VoteResponse<UrsulaRaftTypeConfig>;

#[derive(Debug, Clone, PartialEq)]
pub struct RaftGroupCommand(pub raft_app_proto::RaftGroupCommandV1);

#[derive(Debug, Clone, PartialEq)]
pub struct RaftGroupResponse(pub raft_app_proto::RaftGroupResponseV1);

impl Serialize for RaftGroupCommand {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_bytes(&self.0.encode_to_vec())
    }
}

impl<'de> Deserialize<'de> for RaftGroupCommand {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let bytes = Vec::<u8>::deserialize(deserializer)?;
        let command = raft_app_proto::RaftGroupCommandV1::decode(bytes.as_slice())
            .map_err(serde::de::Error::custom)?;
        Ok(Self(command))
    }
}

impl Serialize for RaftGroupResponse {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_bytes(&self.0.encode_to_vec())
    }
}

impl<'de> Deserialize<'de> for RaftGroupResponse {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let bytes = Vec::<u8>::deserialize(deserializer)?;
        let response = raft_app_proto::RaftGroupResponseV1::decode(bytes.as_slice())
            .map_err(serde::de::Error::custom)?;
        Ok(Self(response))
    }
}

impl fmt::Display for RaftGroupCommand {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match &self.0.command {
            Some(raft_app_proto::raft_group_command_v1::Command::CreateStream(_)) => {
                "create_stream"
            }
            Some(raft_app_proto::raft_group_command_v1::Command::CreateExternal(_)) => {
                "create_external"
            }
            Some(raft_app_proto::raft_group_command_v1::Command::Append(_)) => "append",
            Some(raft_app_proto::raft_group_command_v1::Command::AppendExternal(_)) => {
                "append_external"
            }
            Some(raft_app_proto::raft_group_command_v1::Command::AppendBatch(_)) => "append_batch",
            Some(raft_app_proto::raft_group_command_v1::Command::PublishSnapshot(_)) => {
                "publish_snapshot"
            }
            Some(raft_app_proto::raft_group_command_v1::Command::TouchStreamAccess(_)) => {
                "touch_stream_access"
            }
            Some(raft_app_proto::raft_group_command_v1::Command::AddForkRef(_)) => "add_fork_ref",
            Some(raft_app_proto::raft_group_command_v1::Command::ReleaseForkRef(_)) => {
                "release_fork_ref"
            }
            Some(raft_app_proto::raft_group_command_v1::Command::FlushCold(_)) => "flush_cold",
            Some(raft_app_proto::raft_group_command_v1::Command::CloseStream(_)) => "close_stream",
            Some(raft_app_proto::raft_group_command_v1::Command::DeleteStream(_)) => {
                "delete_stream"
            }
            Some(raft_app_proto::raft_group_command_v1::Command::Batch(_)) => "batch",
            None => "missing",
        };
        f.write_str(name)
    }
}

impl From<GroupWriteCommand> for RaftGroupCommand {
    fn from(command: GroupWriteCommand) -> Self {
        use raft_app_proto::raft_group_command_v1::Command;
        let command = match command {
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
            } => Command::CreateStream(raft_app_proto::CreateStreamCommandV1 {
                stream_id: Some(stream_id.into()),
                content_type,
                initial_payload: initial_payload.to_vec(),
                close_after,
                stream_seq,
                producer,
                stream_ttl_seconds,
                stream_expires_at_ms,
                forked_from: forked_from.map(Into::into),
                fork_offset,
                now_ms,
            }),
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
            } => Command::CreateExternal(raft_app_proto::CreateExternalCommandV1 {
                stream_id: Some(stream_id.into()),
                content_type,
                initial_payload: Some(initial_payload),
                close_after,
                stream_seq,
                producer,
                stream_ttl_seconds,
                stream_expires_at_ms,
                forked_from: forked_from.map(Into::into),
                fork_offset,
                now_ms,
            }),
            GroupWriteCommand::Append {
                stream_id,
                content_type,
                payload,
                close_after,
                stream_seq,
                producer,
                now_ms,
            } => Command::Append(raft_app_proto::AppendCommandV1 {
                stream_id: Some(stream_id.into()),
                content_type,
                payload: payload.to_vec(),
                close_after,
                stream_seq,
                producer,
                now_ms,
            }),
            GroupWriteCommand::AppendExternal {
                stream_id,
                content_type,
                payload,
                close_after,
                stream_seq,
                producer,
                now_ms,
            } => Command::AppendExternal(raft_app_proto::AppendExternalCommandV1 {
                stream_id: Some(stream_id.into()),
                content_type,
                payload: Some(payload),
                close_after,
                stream_seq,
                producer,
                now_ms,
            }),
            GroupWriteCommand::AppendBatch {
                stream_id,
                content_type,
                payloads,
                producer,
                now_ms,
            } => Command::AppendBatch(raft_app_proto::AppendBatchCommandV1 {
                stream_id: Some(stream_id.into()),
                content_type,
                payloads: payloads
                    .into_iter()
                    .map(|payload| payload.to_vec())
                    .collect(),
                producer,
                now_ms,
            }),
            GroupWriteCommand::PublishSnapshot {
                stream_id,
                snapshot_offset,
                content_type,
                payload,
                now_ms,
            } => Command::PublishSnapshot(raft_app_proto::PublishSnapshotCommandV1 {
                stream_id: Some(stream_id.into()),
                snapshot_offset,
                content_type,
                payload: payload.to_vec(),
                now_ms,
            }),
            GroupWriteCommand::TouchStreamAccess {
                stream_id,
                now_ms,
                renew_ttl,
            } => Command::TouchStreamAccess(raft_app_proto::TouchStreamAccessCommandV1 {
                stream_id: Some(stream_id.into()),
                now_ms,
                renew_ttl,
            }),
            GroupWriteCommand::AddForkRef { stream_id, now_ms } => {
                Command::AddForkRef(raft_app_proto::AddForkRefCommandV1 {
                    stream_id: Some(stream_id.into()),
                    now_ms,
                })
            }
            GroupWriteCommand::ReleaseForkRef { stream_id } => {
                Command::ReleaseForkRef(raft_app_proto::ReleaseForkRefCommandV1 {
                    stream_id: Some(stream_id.into()),
                })
            }
            GroupWriteCommand::FlushCold { stream_id, chunk } => {
                Command::FlushCold(raft_app_proto::FlushColdCommandV1 {
                    stream_id: Some(stream_id.into()),
                    chunk: Some(chunk),
                })
            }
            GroupWriteCommand::CloseStream {
                stream_id,
                stream_seq,
                producer,
                now_ms,
            } => Command::CloseStream(raft_app_proto::CloseStreamCommandV1 {
                stream_id: Some(stream_id.into()),
                stream_seq,
                producer,
                now_ms,
            }),
            GroupWriteCommand::DeleteStream { stream_id } => {
                Command::DeleteStream(raft_app_proto::DeleteStreamCommandV1 {
                    stream_id: Some(stream_id.into()),
                })
            }
            GroupWriteCommand::Batch { commands } => {
                Command::Batch(raft_app_proto::BatchCommandV1 {
                    commands: commands
                        .into_iter()
                        .map(|command| RaftGroupCommand::from(command).0)
                        .collect(),
                })
            }
        };
        Self(raft_app_proto::RaftGroupCommandV1 {
            command: Some(command),
        })
    }
}

fn placement_to_proto(placement: ShardPlacement) -> raft_app_proto::ShardPlacementV1 {
    raft_app_proto::ShardPlacementV1 {
        core_id: u32::from(placement.core_id.0),
        shard_id: placement.shard_id.0,
        raft_group_id: placement.raft_group_id.0,
    }
}

fn group_write_command_from_proto(
    command: RaftGroupCommand,
) -> Result<GroupWriteCommand, GroupEngineError> {
    use raft_app_proto::raft_group_command_v1::Command;
    let command = required(command.0.command, "raft group command")?;
    match command {
        Command::CreateStream(command) => Ok(GroupWriteCommand::CreateStream {
            stream_id: stream_id_from_proto(command.stream_id, "create_stream.stream_id")?,
            content_type: command.content_type,
            initial_payload: command.initial_payload.into(),
            close_after: command.close_after,
            stream_seq: command.stream_seq,
            producer: command.producer,
            stream_ttl_seconds: command.stream_ttl_seconds,
            stream_expires_at_ms: command.stream_expires_at_ms,
            forked_from: optional_stream_id_from_proto(command.forked_from)?,
            fork_offset: command.fork_offset,
            now_ms: command.now_ms,
        }),
        Command::CreateExternal(command) => Ok(GroupWriteCommand::CreateExternal {
            stream_id: stream_id_from_proto(command.stream_id, "create_external.stream_id")?,
            content_type: command.content_type,
            initial_payload: required(command.initial_payload, "create_external.initial_payload")?,
            close_after: command.close_after,
            stream_seq: command.stream_seq,
            producer: command.producer,
            stream_ttl_seconds: command.stream_ttl_seconds,
            stream_expires_at_ms: command.stream_expires_at_ms,
            forked_from: optional_stream_id_from_proto(command.forked_from)?,
            fork_offset: command.fork_offset,
            now_ms: command.now_ms,
        }),
        Command::Append(command) => Ok(GroupWriteCommand::Append {
            stream_id: stream_id_from_proto(command.stream_id, "append.stream_id")?,
            content_type: command.content_type,
            payload: command.payload.into(),
            close_after: command.close_after,
            stream_seq: command.stream_seq,
            producer: command.producer,
            now_ms: command.now_ms,
        }),
        Command::AppendExternal(command) => Ok(GroupWriteCommand::AppendExternal {
            stream_id: stream_id_from_proto(command.stream_id, "append_external.stream_id")?,
            content_type: command.content_type,
            payload: required(command.payload, "append_external.payload")?,
            close_after: command.close_after,
            stream_seq: command.stream_seq,
            producer: command.producer,
            now_ms: command.now_ms,
        }),
        Command::AppendBatch(command) => Ok(GroupWriteCommand::AppendBatch {
            stream_id: stream_id_from_proto(command.stream_id, "append_batch.stream_id")?,
            content_type: command.content_type,
            payloads: command
                .payloads
                .into_iter()
                .map(Into::into)
                .collect::<Vec<_>>(),
            producer: command.producer,
            now_ms: command.now_ms,
        }),
        Command::PublishSnapshot(command) => Ok(GroupWriteCommand::PublishSnapshot {
            stream_id: stream_id_from_proto(command.stream_id, "publish_snapshot.stream_id")?,
            snapshot_offset: command.snapshot_offset,
            content_type: command.content_type,
            payload: command.payload.into(),
            now_ms: command.now_ms,
        }),
        Command::TouchStreamAccess(command) => Ok(GroupWriteCommand::TouchStreamAccess {
            stream_id: stream_id_from_proto(command.stream_id, "touch_stream_access.stream_id")?,
            now_ms: command.now_ms,
            renew_ttl: command.renew_ttl,
        }),
        Command::AddForkRef(command) => Ok(GroupWriteCommand::AddForkRef {
            stream_id: stream_id_from_proto(command.stream_id, "add_fork_ref.stream_id")?,
            now_ms: command.now_ms,
        }),
        Command::ReleaseForkRef(command) => Ok(GroupWriteCommand::ReleaseForkRef {
            stream_id: stream_id_from_proto(command.stream_id, "release_fork_ref.stream_id")?,
        }),
        Command::FlushCold(command) => Ok(GroupWriteCommand::FlushCold {
            stream_id: stream_id_from_proto(command.stream_id, "flush_cold.stream_id")?,
            chunk: required(command.chunk, "flush_cold.chunk")?,
        }),
        Command::CloseStream(command) => Ok(GroupWriteCommand::CloseStream {
            stream_id: stream_id_from_proto(command.stream_id, "close_stream.stream_id")?,
            stream_seq: command.stream_seq,
            producer: command.producer,
            now_ms: command.now_ms,
        }),
        Command::DeleteStream(command) => Ok(GroupWriteCommand::DeleteStream {
            stream_id: stream_id_from_proto(command.stream_id, "delete_stream.stream_id")?,
        }),
        Command::Batch(command) => Ok(GroupWriteCommand::Batch {
            commands: command
                .commands
                .into_iter()
                .map(|command| group_write_command_from_proto(RaftGroupCommand(command)))
                .collect::<Result<Vec<_>, _>>()?,
        }),
    }
}

fn stream_id_from_proto(
    stream_id: Option<raft_app_proto::BucketStreamIdV1>,
    field: &str,
) -> Result<BucketStreamId, GroupEngineError> {
    Ok(required(stream_id, field)?.into())
}

fn optional_stream_id_from_proto(
    stream_id: Option<raft_app_proto::BucketStreamIdV1>,
) -> Result<Option<BucketStreamId>, GroupEngineError> {
    Ok(stream_id.map(Into::into))
}

fn placement_from_proto(
    placement: Option<raft_app_proto::ShardPlacementV1>,
    field: &str,
) -> Result<ShardPlacement, GroupEngineError> {
    let placement = required(placement, field)?;
    let core_id = u16::try_from(placement.core_id)
        .map_err(|_| GroupEngineError::new(format!("{field}.core_id does not fit u16")))?;
    Ok(ShardPlacement {
        core_id: CoreId(core_id),
        shard_id: ShardId(placement.shard_id),
        raft_group_id: RaftGroupId(placement.raft_group_id),
    })
}

fn placement_from_parts(
    core_id: u32,
    shard_id: u32,
    raft_group_id: u32,
    field: &str,
) -> Result<ShardPlacement, GroupEngineError> {
    let core_id = u16::try_from(core_id)
        .map_err(|_| GroupEngineError::new(format!("{field}.core_id does not fit u16")))?;
    Ok(ShardPlacement {
        core_id: CoreId(core_id),
        shard_id: ShardId(shard_id),
        raft_group_id: RaftGroupId(raft_group_id),
    })
}

fn required<T>(value: Option<T>, field: &str) -> Result<T, GroupEngineError> {
    value.ok_or_else(|| GroupEngineError::new(format!("protobuf raft payload missing {field}")))
}

fn raft_blank_response() -> RaftGroupResponse {
    RaftGroupResponse(raft_app_proto::RaftGroupResponseV1 {
        response: Some(raft_app_proto::raft_group_response_v1::Response::Blank(
            raft_app_proto::BlankResponseV1 {},
        )),
    })
}

fn raft_membership_response() -> RaftGroupResponse {
    RaftGroupResponse(raft_app_proto::RaftGroupResponseV1 {
        response: Some(
            raft_app_proto::raft_group_response_v1::Response::Membership(
                raft_app_proto::MembershipResponseV1 {},
            ),
        ),
    })
}

fn raft_write_applied_response(response: GroupWriteResponse) -> RaftGroupResponse {
    RaftGroupResponse(raft_app_proto::RaftGroupResponseV1 {
        response: Some(
            raft_app_proto::raft_group_response_v1::Response::WriteApplied(
                write_applied_response_to_proto(response),
            ),
        ),
    })
}

fn raft_write_rejected_response(err: GroupEngineError) -> RaftGroupResponse {
    RaftGroupResponse(raft_app_proto::RaftGroupResponseV1 {
        response: Some(
            raft_app_proto::raft_group_response_v1::Response::WriteRejected(
                group_engine_error_to_proto(err),
            ),
        ),
    })
}

fn write_applied_response_to_proto(
    response: GroupWriteResponse,
) -> raft_app_proto::WriteAppliedResponseV1 {
    use raft_app_proto::write_applied_response_v1::Response;
    let response = match response {
        GroupWriteResponse::CreateStream(response) => {
            Response::CreateStream(create_stream_response_to_proto(response))
        }
        GroupWriteResponse::Append(response) => {
            Response::Append(append_response_to_proto(response))
        }
        GroupWriteResponse::AppendBatch(response) => {
            Response::AppendBatch(append_batch_response_to_proto(response))
        }
        GroupWriteResponse::PublishSnapshot(response) => {
            Response::PublishSnapshot(publish_snapshot_response_to_proto(response))
        }
        GroupWriteResponse::TouchStreamAccess(response) => {
            Response::TouchStreamAccess(touch_stream_access_response_to_proto(response))
        }
        GroupWriteResponse::AddForkRef(response) => {
            Response::AddForkRef(fork_ref_response_to_proto(response))
        }
        GroupWriteResponse::ReleaseForkRef(response) => {
            Response::ReleaseForkRef(fork_ref_response_to_proto(response))
        }
        GroupWriteResponse::FlushCold(response) => {
            Response::FlushCold(flush_cold_response_to_proto(response))
        }
        GroupWriteResponse::CloseStream(response) => {
            Response::CloseStream(close_stream_response_to_proto(response))
        }
        GroupWriteResponse::DeleteStream(response) => {
            Response::DeleteStream(delete_stream_response_to_proto(response))
        }
        GroupWriteResponse::Batch(items) => Response::Batch(raft_app_proto::BatchResponseV1 {
            items: items
                .into_iter()
                .map(group_write_result_to_proto)
                .collect::<Vec<_>>(),
        }),
    };
    raft_app_proto::WriteAppliedResponseV1 {
        response: Some(response),
    }
}

fn create_stream_response_to_proto(
    response: CreateStreamResponse,
) -> raft_app_proto::CreateStreamResponseV1 {
    raft_app_proto::CreateStreamResponseV1 {
        placement: Some(placement_to_proto(response.placement)),
        next_offset: response.next_offset,
        closed: response.closed,
        already_exists: response.already_exists,
        group_commit_index: response.group_commit_index,
    }
}

fn append_response_to_proto(response: AppendResponse) -> raft_app_proto::AppendResponseV1 {
    raft_app_proto::AppendResponseV1 {
        placement: Some(placement_to_proto(response.placement)),
        start_offset: response.start_offset,
        next_offset: response.next_offset,
        stream_append_count: response.stream_append_count,
        group_commit_index: response.group_commit_index,
        closed: response.closed,
        deduplicated: response.deduplicated,
        producer: response.producer,
    }
}

fn append_batch_response_to_proto(
    response: GroupAppendBatchResponse,
) -> raft_app_proto::AppendBatchResponseV1 {
    raft_app_proto::AppendBatchResponseV1 {
        placement: Some(placement_to_proto(response.placement)),
        items: response
            .items
            .into_iter()
            .map(append_result_to_proto)
            .collect::<Vec<_>>(),
    }
}

fn publish_snapshot_response_to_proto(
    response: PublishSnapshotResponse,
) -> raft_app_proto::PublishSnapshotResponseV1 {
    raft_app_proto::PublishSnapshotResponseV1 {
        placement: Some(placement_to_proto(response.placement)),
        snapshot_offset: response.snapshot_offset,
        group_commit_index: response.group_commit_index,
    }
}

fn touch_stream_access_response_to_proto(
    response: TouchStreamAccessResponse,
) -> raft_app_proto::TouchStreamAccessResponseV1 {
    raft_app_proto::TouchStreamAccessResponseV1 {
        placement: Some(placement_to_proto(response.placement)),
        changed: response.changed,
        expired: response.expired,
        group_commit_index: response.group_commit_index,
    }
}

fn fork_ref_response_to_proto(response: ForkRefResponse) -> raft_app_proto::ForkRefResponseV1 {
    raft_app_proto::ForkRefResponseV1 {
        placement: Some(placement_to_proto(response.placement)),
        fork_ref_count: response.fork_ref_count,
        hard_deleted: response.hard_deleted,
        parent_to_release: response.parent_to_release.map(Into::into),
        group_commit_index: response.group_commit_index,
    }
}

fn flush_cold_response_to_proto(
    response: FlushColdResponse,
) -> raft_app_proto::FlushColdResponseV1 {
    raft_app_proto::FlushColdResponseV1 {
        placement: Some(placement_to_proto(response.placement)),
        hot_start_offset: response.hot_start_offset,
        group_commit_index: response.group_commit_index,
    }
}

fn close_stream_response_to_proto(
    response: CloseStreamResponse,
) -> raft_app_proto::CloseStreamResponseV1 {
    raft_app_proto::CloseStreamResponseV1 {
        placement: Some(placement_to_proto(response.placement)),
        next_offset: response.next_offset,
        group_commit_index: response.group_commit_index,
        deduplicated: response.deduplicated,
    }
}

fn delete_stream_response_to_proto(
    response: DeleteStreamResponse,
) -> raft_app_proto::DeleteStreamResponseV1 {
    raft_app_proto::DeleteStreamResponseV1 {
        placement: Some(placement_to_proto(response.placement)),
        group_commit_index: response.group_commit_index,
        hard_deleted: response.hard_deleted,
        parent_to_release: response.parent_to_release.map(Into::into),
    }
}

fn append_result_to_proto(
    result: Result<AppendResponse, GroupEngineError>,
) -> raft_app_proto::AppendResultV1 {
    let result = match result {
        Ok(response) => {
            raft_app_proto::append_result_v1::Result::Ok(append_response_to_proto(response))
        }
        Err(err) => raft_app_proto::append_result_v1::Result::Err(group_engine_error_to_proto(err)),
    };
    raft_app_proto::AppendResultV1 {
        result: Some(result),
    }
}

fn group_write_result_to_proto(
    result: Result<GroupWriteResponse, GroupEngineError>,
) -> raft_app_proto::GroupWriteResultV1 {
    let result = match result {
        Ok(response) => raft_app_proto::group_write_result_v1::Result::Ok(
            write_applied_response_to_proto(response),
        ),
        Err(err) => {
            raft_app_proto::group_write_result_v1::Result::Err(group_engine_error_to_proto(err))
        }
    };
    raft_app_proto::GroupWriteResultV1 {
        result: Some(result),
    }
}

fn group_write_result_from_raft_response(
    response: RaftGroupResponse,
) -> Result<Result<GroupWriteResponse, GroupEngineError>, GroupEngineError> {
    match required(response.0.response, "raft group response")? {
        raft_app_proto::raft_group_response_v1::Response::WriteApplied(response) => {
            Ok(Ok(group_write_response_from_proto(response)?))
        }
        raft_app_proto::raft_group_response_v1::Response::WriteRejected(err) => {
            Ok(Err(group_engine_error_from_proto(err)?))
        }
        other => Err(GroupEngineError::new(format!(
            "unexpected OpenRaft write response: {other:?}"
        ))),
    }
}

fn group_write_response_from_proto(
    response: raft_app_proto::WriteAppliedResponseV1,
) -> Result<GroupWriteResponse, GroupEngineError> {
    use raft_app_proto::write_applied_response_v1::Response;
    match required(response.response, "write_applied.response")? {
        Response::CreateStream(response) => Ok(GroupWriteResponse::CreateStream(
            create_stream_response_from_proto(response)?,
        )),
        Response::Append(response) => Ok(GroupWriteResponse::Append(append_response_from_proto(
            response,
        )?)),
        Response::AppendBatch(response) => Ok(GroupWriteResponse::AppendBatch(
            append_batch_response_from_proto(response)?,
        )),
        Response::PublishSnapshot(response) => Ok(GroupWriteResponse::PublishSnapshot(
            publish_snapshot_response_from_proto(response)?,
        )),
        Response::TouchStreamAccess(response) => Ok(GroupWriteResponse::TouchStreamAccess(
            touch_stream_access_response_from_proto(response)?,
        )),
        Response::AddForkRef(response) => Ok(GroupWriteResponse::AddForkRef(
            fork_ref_response_from_proto(response)?,
        )),
        Response::ReleaseForkRef(response) => Ok(GroupWriteResponse::ReleaseForkRef(
            fork_ref_response_from_proto(response)?,
        )),
        Response::FlushCold(response) => Ok(GroupWriteResponse::FlushCold(
            flush_cold_response_from_proto(response)?,
        )),
        Response::CloseStream(response) => Ok(GroupWriteResponse::CloseStream(
            close_stream_response_from_proto(response)?,
        )),
        Response::DeleteStream(response) => Ok(GroupWriteResponse::DeleteStream(
            delete_stream_response_from_proto(response)?,
        )),
        Response::Batch(response) => Ok(GroupWriteResponse::Batch(
            response
                .items
                .into_iter()
                .map(group_write_result_from_proto)
                .collect::<Result<Vec<_>, _>>()?,
        )),
    }
}

fn create_stream_response_from_proto(
    response: raft_app_proto::CreateStreamResponseV1,
) -> Result<CreateStreamResponse, GroupEngineError> {
    Ok(CreateStreamResponse {
        placement: placement_from_proto(response.placement, "create_stream_response.placement")?,
        next_offset: response.next_offset,
        closed: response.closed,
        already_exists: response.already_exists,
        group_commit_index: response.group_commit_index,
    })
}

fn append_response_from_proto(
    response: raft_app_proto::AppendResponseV1,
) -> Result<AppendResponse, GroupEngineError> {
    Ok(AppendResponse {
        placement: placement_from_proto(response.placement, "append_response.placement")?,
        start_offset: response.start_offset,
        next_offset: response.next_offset,
        stream_append_count: response.stream_append_count,
        group_commit_index: response.group_commit_index,
        closed: response.closed,
        deduplicated: response.deduplicated,
        producer: response.producer,
    })
}

fn append_batch_response_from_proto(
    response: raft_app_proto::AppendBatchResponseV1,
) -> Result<GroupAppendBatchResponse, GroupEngineError> {
    Ok(GroupAppendBatchResponse {
        placement: placement_from_proto(response.placement, "append_batch_response.placement")?,
        items: response
            .items
            .into_iter()
            .map(append_result_from_proto)
            .collect::<Result<Vec<_>, _>>()?,
    })
}

fn publish_snapshot_response_from_proto(
    response: raft_app_proto::PublishSnapshotResponseV1,
) -> Result<PublishSnapshotResponse, GroupEngineError> {
    Ok(PublishSnapshotResponse {
        placement: placement_from_proto(response.placement, "publish_snapshot_response.placement")?,
        snapshot_offset: response.snapshot_offset,
        group_commit_index: response.group_commit_index,
    })
}

fn touch_stream_access_response_from_proto(
    response: raft_app_proto::TouchStreamAccessResponseV1,
) -> Result<TouchStreamAccessResponse, GroupEngineError> {
    Ok(TouchStreamAccessResponse {
        placement: placement_from_proto(
            response.placement,
            "touch_stream_access_response.placement",
        )?,
        changed: response.changed,
        expired: response.expired,
        group_commit_index: response.group_commit_index,
    })
}

fn fork_ref_response_from_proto(
    response: raft_app_proto::ForkRefResponseV1,
) -> Result<ForkRefResponse, GroupEngineError> {
    Ok(ForkRefResponse {
        placement: placement_from_proto(response.placement, "fork_ref_response.placement")?,
        fork_ref_count: response.fork_ref_count,
        hard_deleted: response.hard_deleted,
        parent_to_release: optional_stream_id_from_proto(response.parent_to_release)?,
        group_commit_index: response.group_commit_index,
    })
}

fn flush_cold_response_from_proto(
    response: raft_app_proto::FlushColdResponseV1,
) -> Result<FlushColdResponse, GroupEngineError> {
    Ok(FlushColdResponse {
        placement: placement_from_proto(response.placement, "flush_cold_response.placement")?,
        hot_start_offset: response.hot_start_offset,
        group_commit_index: response.group_commit_index,
    })
}

fn close_stream_response_from_proto(
    response: raft_app_proto::CloseStreamResponseV1,
) -> Result<CloseStreamResponse, GroupEngineError> {
    Ok(CloseStreamResponse {
        placement: placement_from_proto(response.placement, "close_stream_response.placement")?,
        next_offset: response.next_offset,
        group_commit_index: response.group_commit_index,
        deduplicated: response.deduplicated,
    })
}

fn head_stream_response_to_proto(
    response: HeadStreamResponse,
) -> raft_internal_proto::HeadStreamResponsePayloadV1 {
    raft_internal_proto::HeadStreamResponsePayloadV1 {
        core_id: u32::from(response.placement.core_id.0),
        shard_id: response.placement.shard_id.0,
        raft_group_id: response.placement.raft_group_id.0,
        content_type: response.content_type,
        tail_offset: response.tail_offset,
        closed: response.closed,
        stream_ttl_seconds: response.stream_ttl_seconds,
        stream_expires_at_ms: response.stream_expires_at_ms,
        snapshot_offset: response.snapshot_offset,
    }
}

fn head_stream_response_from_proto(
    response: raft_internal_proto::HeadStreamResponsePayloadV1,
) -> Result<HeadStreamResponse, GroupEngineError> {
    Ok(HeadStreamResponse {
        placement: placement_from_parts(
            response.core_id,
            response.shard_id,
            response.raft_group_id,
            "head_stream_response",
        )?,
        content_type: response.content_type,
        tail_offset: response.tail_offset,
        closed: response.closed,
        stream_ttl_seconds: response.stream_ttl_seconds,
        stream_expires_at_ms: response.stream_expires_at_ms,
        snapshot_offset: response.snapshot_offset,
    })
}

fn read_stream_response_to_proto(
    response: ReadStreamResponse,
) -> raft_internal_proto::ReadStreamResponsePayloadV1 {
    raft_internal_proto::ReadStreamResponsePayloadV1 {
        core_id: u32::from(response.placement.core_id.0),
        shard_id: response.placement.shard_id.0,
        raft_group_id: response.placement.raft_group_id.0,
        offset: response.offset,
        next_offset: response.next_offset,
        content_type: response.content_type,
        payload: response.payload,
        up_to_date: response.up_to_date,
        closed: response.closed,
    }
}

fn read_stream_response_from_proto(
    response: raft_internal_proto::ReadStreamResponsePayloadV1,
) -> Result<ReadStreamResponse, GroupEngineError> {
    Ok(ReadStreamResponse {
        placement: placement_from_parts(
            response.core_id,
            response.shard_id,
            response.raft_group_id,
            "read_stream_response",
        )?,
        offset: response.offset,
        next_offset: response.next_offset,
        content_type: response.content_type,
        payload: response.payload,
        up_to_date: response.up_to_date,
        closed: response.closed,
    })
}

fn delete_stream_response_from_proto(
    response: raft_app_proto::DeleteStreamResponseV1,
) -> Result<DeleteStreamResponse, GroupEngineError> {
    Ok(DeleteStreamResponse {
        placement: placement_from_proto(response.placement, "delete_stream_response.placement")?,
        group_commit_index: response.group_commit_index,
        hard_deleted: response.hard_deleted,
        parent_to_release: optional_stream_id_from_proto(response.parent_to_release)?,
    })
}

fn append_result_from_proto(
    result: raft_app_proto::AppendResultV1,
) -> Result<Result<AppendResponse, GroupEngineError>, GroupEngineError> {
    match required(result.result, "append_result.result")? {
        raft_app_proto::append_result_v1::Result::Ok(response) => {
            Ok(Ok(append_response_from_proto(response)?))
        }
        raft_app_proto::append_result_v1::Result::Err(err) => {
            Ok(Err(group_engine_error_from_proto(err)?))
        }
    }
}

fn group_write_result_from_proto(
    result: raft_app_proto::GroupWriteResultV1,
) -> Result<Result<GroupWriteResponse, GroupEngineError>, GroupEngineError> {
    match required(result.result, "group_write_result.result")? {
        raft_app_proto::group_write_result_v1::Result::Ok(response) => {
            Ok(Ok(group_write_response_from_proto(response)?))
        }
        raft_app_proto::group_write_result_v1::Result::Err(err) => {
            Ok(Err(group_engine_error_from_proto(err)?))
        }
    }
}

fn encode_group_write_result(
    result: Result<GroupWriteResponse, GroupEngineError>,
) -> raft_internal_proto::GroupWriteResultV1 {
    match result {
        Ok(response) => raft_internal_proto::GroupWriteResultV1 {
            ok: true,
            payload: write_applied_response_to_proto(response).encode_to_vec(),
        },
        Err(err) => raft_internal_proto::GroupWriteResultV1 {
            ok: false,
            payload: group_engine_error_to_proto(err).encode_to_vec(),
        },
    }
}

fn group_engine_error_to_proto(err: GroupEngineError) -> raft_app_proto::GroupEngineErrorV1 {
    raft_app_proto::GroupEngineErrorV1 {
        message: err.message().to_owned(),
        code: err
            .code()
            .map(stream_error_code_to_proto)
            .map(|code| code as i32),
        next_offset: err.next_offset(),
        leader_hint: err.leader_hint().cloned().map(group_leader_hint_to_proto),
    }
}

fn group_engine_error_from_proto(
    err: raft_app_proto::GroupEngineErrorV1,
) -> Result<GroupEngineError, GroupEngineError> {
    Ok(GroupEngineError::from_replicated_parts(
        err.message,
        err.code.map(stream_error_code_from_proto).transpose()?,
        err.next_offset,
        err.leader_hint.map(group_leader_hint_from_proto),
    ))
}

fn group_leader_hint_to_proto(hint: GroupLeaderHint) -> raft_app_proto::GroupLeaderHintV1 {
    raft_app_proto::GroupLeaderHintV1 {
        node_id: hint.node_id,
        address: hint.address,
    }
}

fn group_leader_hint_from_proto(hint: raft_app_proto::GroupLeaderHintV1) -> GroupLeaderHint {
    GroupLeaderHint {
        node_id: hint.node_id,
        address: hint.address,
    }
}

fn stream_error_code_to_proto(code: StreamErrorCode) -> raft_app_proto::StreamErrorCodeV1 {
    match code {
        StreamErrorCode::InvalidBucketId => {
            raft_app_proto::StreamErrorCodeV1::StreamErrorCodeInvalidBucketId
        }
        StreamErrorCode::InvalidStreamId => {
            raft_app_proto::StreamErrorCodeV1::StreamErrorCodeInvalidStreamId
        }
        StreamErrorCode::BucketNotFound => {
            raft_app_proto::StreamErrorCodeV1::StreamErrorCodeBucketNotFound
        }
        StreamErrorCode::BucketNotEmpty => {
            raft_app_proto::StreamErrorCodeV1::StreamErrorCodeBucketNotEmpty
        }
        StreamErrorCode::StreamNotFound => {
            raft_app_proto::StreamErrorCodeV1::StreamErrorCodeStreamNotFound
        }
        StreamErrorCode::StreamGone => raft_app_proto::StreamErrorCodeV1::StreamErrorCodeStreamGone,
        StreamErrorCode::StreamAlreadyExistsConflict => {
            raft_app_proto::StreamErrorCodeV1::StreamErrorCodeStreamAlreadyExistsConflict
        }
        StreamErrorCode::MissingContentType => {
            raft_app_proto::StreamErrorCodeV1::StreamErrorCodeMissingContentType
        }
        StreamErrorCode::ContentTypeMismatch => {
            raft_app_proto::StreamErrorCodeV1::StreamErrorCodeContentTypeMismatch
        }
        StreamErrorCode::EmptyAppend => {
            raft_app_proto::StreamErrorCodeV1::StreamErrorCodeEmptyAppend
        }
        StreamErrorCode::StreamClosed => {
            raft_app_proto::StreamErrorCodeV1::StreamErrorCodeStreamClosed
        }
        StreamErrorCode::StreamSeqConflict => {
            raft_app_proto::StreamErrorCodeV1::StreamErrorCodeStreamSeqConflict
        }
        StreamErrorCode::InvalidProducer => {
            raft_app_proto::StreamErrorCodeV1::StreamErrorCodeInvalidProducer
        }
        StreamErrorCode::ProducerEpochStale => {
            raft_app_proto::StreamErrorCodeV1::StreamErrorCodeProducerEpochStale
        }
        StreamErrorCode::ProducerSeqConflict => {
            raft_app_proto::StreamErrorCodeV1::StreamErrorCodeProducerSeqConflict
        }
        StreamErrorCode::InvalidRetention => {
            raft_app_proto::StreamErrorCodeV1::StreamErrorCodeInvalidRetention
        }
        StreamErrorCode::InvalidFork => {
            raft_app_proto::StreamErrorCodeV1::StreamErrorCodeInvalidFork
        }
        StreamErrorCode::OffsetOutOfRange => {
            raft_app_proto::StreamErrorCodeV1::StreamErrorCodeOffsetOutOfRange
        }
        StreamErrorCode::InvalidColdFlush => {
            raft_app_proto::StreamErrorCodeV1::StreamErrorCodeInvalidColdFlush
        }
        StreamErrorCode::InvalidSnapshot => {
            raft_app_proto::StreamErrorCodeV1::StreamErrorCodeInvalidSnapshot
        }
        StreamErrorCode::SnapshotNotFound => {
            raft_app_proto::StreamErrorCodeV1::StreamErrorCodeSnapshotNotFound
        }
        StreamErrorCode::SnapshotConflict => {
            raft_app_proto::StreamErrorCodeV1::StreamErrorCodeSnapshotConflict
        }
    }
}

fn stream_error_code_from_proto(code: i32) -> Result<StreamErrorCode, GroupEngineError> {
    let code = raft_app_proto::StreamErrorCodeV1::try_from(code)
        .map_err(|_| GroupEngineError::new(format!("unknown stream error code: {code}")))?;
    match code {
        raft_app_proto::StreamErrorCodeV1::StreamErrorCodeUnspecified => {
            Err(GroupEngineError::new("unspecified stream error code"))
        }
        raft_app_proto::StreamErrorCodeV1::StreamErrorCodeInvalidBucketId => {
            Ok(StreamErrorCode::InvalidBucketId)
        }
        raft_app_proto::StreamErrorCodeV1::StreamErrorCodeInvalidStreamId => {
            Ok(StreamErrorCode::InvalidStreamId)
        }
        raft_app_proto::StreamErrorCodeV1::StreamErrorCodeBucketNotFound => {
            Ok(StreamErrorCode::BucketNotFound)
        }
        raft_app_proto::StreamErrorCodeV1::StreamErrorCodeBucketNotEmpty => {
            Ok(StreamErrorCode::BucketNotEmpty)
        }
        raft_app_proto::StreamErrorCodeV1::StreamErrorCodeStreamNotFound => {
            Ok(StreamErrorCode::StreamNotFound)
        }
        raft_app_proto::StreamErrorCodeV1::StreamErrorCodeStreamGone => {
            Ok(StreamErrorCode::StreamGone)
        }
        raft_app_proto::StreamErrorCodeV1::StreamErrorCodeStreamAlreadyExistsConflict => {
            Ok(StreamErrorCode::StreamAlreadyExistsConflict)
        }
        raft_app_proto::StreamErrorCodeV1::StreamErrorCodeMissingContentType => {
            Ok(StreamErrorCode::MissingContentType)
        }
        raft_app_proto::StreamErrorCodeV1::StreamErrorCodeContentTypeMismatch => {
            Ok(StreamErrorCode::ContentTypeMismatch)
        }
        raft_app_proto::StreamErrorCodeV1::StreamErrorCodeEmptyAppend => {
            Ok(StreamErrorCode::EmptyAppend)
        }
        raft_app_proto::StreamErrorCodeV1::StreamErrorCodeStreamClosed => {
            Ok(StreamErrorCode::StreamClosed)
        }
        raft_app_proto::StreamErrorCodeV1::StreamErrorCodeStreamSeqConflict => {
            Ok(StreamErrorCode::StreamSeqConflict)
        }
        raft_app_proto::StreamErrorCodeV1::StreamErrorCodeInvalidProducer => {
            Ok(StreamErrorCode::InvalidProducer)
        }
        raft_app_proto::StreamErrorCodeV1::StreamErrorCodeProducerEpochStale => {
            Ok(StreamErrorCode::ProducerEpochStale)
        }
        raft_app_proto::StreamErrorCodeV1::StreamErrorCodeProducerSeqConflict => {
            Ok(StreamErrorCode::ProducerSeqConflict)
        }
        raft_app_proto::StreamErrorCodeV1::StreamErrorCodeInvalidRetention => {
            Ok(StreamErrorCode::InvalidRetention)
        }
        raft_app_proto::StreamErrorCodeV1::StreamErrorCodeInvalidFork => {
            Ok(StreamErrorCode::InvalidFork)
        }
        raft_app_proto::StreamErrorCodeV1::StreamErrorCodeOffsetOutOfRange => {
            Ok(StreamErrorCode::OffsetOutOfRange)
        }
        raft_app_proto::StreamErrorCodeV1::StreamErrorCodeInvalidColdFlush => {
            Ok(StreamErrorCode::InvalidColdFlush)
        }
        raft_app_proto::StreamErrorCodeV1::StreamErrorCodeInvalidSnapshot => {
            Ok(StreamErrorCode::InvalidSnapshot)
        }
        raft_app_proto::StreamErrorCodeV1::StreamErrorCodeSnapshotNotFound => {
            Ok(StreamErrorCode::SnapshotNotFound)
        }
        raft_app_proto::StreamErrorCodeV1::StreamErrorCodeSnapshotConflict => {
            Ok(StreamErrorCode::SnapshotConflict)
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct RaftLogProgressSnapshot {
    pub term: u64,
    pub index: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RaftGroupMetricsSnapshot {
    pub raft_group_id: u32,
    pub node_id: u64,
    pub current_term: u64,
    pub current_leader: Option<u64>,
    pub last_log_index: Option<u64>,
    pub committed: Option<RaftLogProgressSnapshot>,
    pub last_applied: Option<RaftLogProgressSnapshot>,
    pub snapshot: Option<RaftLogProgressSnapshot>,
    pub purged: Option<RaftLogProgressSnapshot>,
    pub voter_ids: Vec<u64>,
    pub learner_ids: Vec<u64>,
}

pub mod raft_internal_proto {
    tonic::include_proto!("ursula.raft.v1");
}

pub const RAFT_GRPC_APPEND_PATH: &str = "/ursula.raft.v1.RaftInternal/Append";
pub const RAFT_GRPC_VOTE_PATH: &str = "/ursula.raft.v1.RaftInternal/Vote";
pub const RAFT_GRPC_FULL_SNAPSHOT_PATH: &str = "/ursula.raft.v1.RaftInternal/FullSnapshot";
pub const RAFT_GRPC_FORWARD_HTTP_WRITE_PATH: &str = "/ursula.raft.v1.RaftInternal/ForwardHttpWrite";
pub const RAFT_GRPC_GROUP_WRITE_PATH: &str = "/ursula.raft.v1.RaftInternal/GroupWrite";
pub const RAFT_GRPC_GROUP_READ_PATH: &str = "/ursula.raft.v1.RaftInternal/GroupRead";
pub const RAFT_GRPC_MAX_MESSAGE_BYTES: usize = 256 * 1024 * 1024;
// Protobuf is only the stable gRPC envelope. OpenRaft/domain payload bytes use
// one serde codec so Rust command/state-machine schemas stay authoritative.
pub const RAFT_RPC_PAYLOAD_CODEC: &str = "openraft-rmp-serde-v1";
const RAFT_GRPC_PROTOCOL_VERSION: u32 = 1;

#[derive(Debug)]
struct GrpcRpcError {
    code: tonic::Code,
    message: String,
}

impl GrpcRpcError {
    fn invalid_argument(message: impl Into<String>) -> Self {
        Self {
            code: tonic::Code::InvalidArgument,
            message: message.into(),
        }
    }

    fn failed_precondition(message: impl Into<String>) -> Self {
        Self {
            code: tonic::Code::FailedPrecondition,
            message: message.into(),
        }
    }

    fn not_found(message: impl Into<String>) -> Self {
        Self {
            code: tonic::Code::NotFound,
            message: message.into(),
        }
    }

    fn internal(message: impl Into<String>) -> Self {
        Self {
            code: tonic::Code::Internal,
            message: message.into(),
        }
    }
}

impl From<GrpcRpcError> for tonic::Status {
    fn from(error: GrpcRpcError) -> Self {
        tonic::Status::new(error.code, error.message)
    }
}

#[derive(Debug, Clone)]
pub struct RaftGrpcService {
    registry: RaftGroupHandleRegistry,
    cold_store: Option<ColdStoreHandle>,
}

impl RaftGrpcService {
    pub fn new(registry: RaftGroupHandleRegistry) -> Self {
        Self {
            registry,
            cold_store: None,
        }
    }

    pub fn with_cold_store(mut self, cold_store: Option<ColdStoreHandle>) -> Self {
        self.cold_store = cold_store;
        self
    }
}

pub fn raft_grpc_service(
    registry: RaftGroupHandleRegistry,
) -> raft_internal_proto::raft_internal_server::RaftInternalServer<RaftGrpcService> {
    raft_internal_proto::raft_internal_server::RaftInternalServer::new(RaftGrpcService::new(
        registry,
    ))
    .max_decoding_message_size(RAFT_GRPC_MAX_MESSAGE_BYTES)
    .max_encoding_message_size(RAFT_GRPC_MAX_MESSAGE_BYTES)
}

#[tonic::async_trait]
impl raft_internal_proto::raft_internal_server::RaftInternal for RaftGrpcService {
    async fn append(
        &self,
        request: tonic::Request<raft_internal_proto::RaftRpcEnvelopeV1>,
    ) -> Result<tonic::Response<raft_internal_proto::RaftRpcAckV1>, tonic::Status> {
        let envelope = request.into_inner();
        let raft_group_id = validate_raft_rpc_envelope(&self.registry, &envelope)?;
        let request = decode_grpc_payload::<UrsulaAppendEntriesRequest>(&envelope.payload)?;
        let response = self
            .registry
            .append_entries(raft_group_id, request)
            .await
            .map_err(|err| tonic::Status::internal(err.to_string()))?;
        Ok(tonic::Response::new(raft_internal_proto::RaftRpcAckV1 {
            payload: encode_grpc_payload(&response)?,
        }))
    }

    async fn vote(
        &self,
        request: tonic::Request<raft_internal_proto::RaftRpcEnvelopeV1>,
    ) -> Result<tonic::Response<raft_internal_proto::RaftRpcAckV1>, tonic::Status> {
        let envelope = request.into_inner();
        let raft_group_id = validate_raft_rpc_envelope(&self.registry, &envelope)?;
        let request = decode_grpc_payload::<UrsulaVoteRequest>(&envelope.payload)?;
        let response = self
            .registry
            .vote(raft_group_id, request)
            .await
            .map_err(|err| tonic::Status::internal(err.to_string()))?;
        Ok(tonic::Response::new(raft_internal_proto::RaftRpcAckV1 {
            payload: encode_grpc_payload(&response)?,
        }))
    }

    async fn full_snapshot(
        &self,
        request: tonic::Request<raft_internal_proto::RaftFullSnapshotRequestV1>,
    ) -> Result<tonic::Response<raft_internal_proto::RaftFullSnapshotAckV1>, tonic::Status> {
        let request = request.into_inner();
        let raft_group_id = validate_raft_snapshot_request(&self.registry, &request)?;
        let vote = decode_grpc_payload::<VoteOf<UrsulaRaftTypeConfig>>(&request.vote_payload)?;
        let meta = decode_grpc_payload::<SnapshotMetaOf<UrsulaRaftTypeConfig>>(
            &request.snapshot_meta_payload,
        )?;
        let snapshot = SnapshotOf::<UrsulaRaftTypeConfig> {
            meta,
            snapshot: Cursor::new(request.snapshot_payload),
        };
        let response = self
            .registry
            .install_full_snapshot(raft_group_id, vote, snapshot)
            .await
            .map_err(|err| tonic::Status::internal(err.to_string()))?;
        Ok(tonic::Response::new(
            raft_internal_proto::RaftFullSnapshotAckV1 {
                payload: encode_grpc_payload(&response)?,
            },
        ))
    }

    async fn forward_http_write(
        &self,
        _request: tonic::Request<raft_internal_proto::HttpWriteRequestV1>,
    ) -> Result<tonic::Response<raft_internal_proto::HttpWriteResponseV1>, tonic::Status> {
        Err(tonic::Status::unimplemented(
            "HTTP write forwarding is provided by the HTTP adapter",
        ))
    }

    async fn group_write(
        &self,
        request: tonic::Request<raft_internal_proto::GroupWriteRequestV1>,
    ) -> Result<tonic::Response<raft_internal_proto::GroupWriteResponseV1>, tonic::Status> {
        let request = request.into_inner();
        let placement = placement_from_parts(
            request.core_id,
            request.shard_id,
            request.raft_group_id,
            "group_write_request",
        )
        .map_err(|err| tonic::Status::invalid_argument(err.to_string()))?;
        let raft = self
            .registry
            .get(placement.raft_group_id)
            .ok_or_else(|| tonic::Status::not_found("raft group is not registered"))?;
        let commands = request
            .command_payloads
            .into_iter()
            .map(|payload| {
                let command = raft_app_proto::RaftGroupCommandV1::decode(payload.as_slice())
                    .map_err(|err| GroupEngineError::new(format!("decode group command: {err}")))?;
                group_write_command_from_proto(RaftGroupCommand(command))
            })
            .collect::<Result<Vec<_>, _>>()
            .map_err(|err| tonic::Status::invalid_argument(err.to_string()))?;
        let results = write_commands_on_raft(raft, placement, None, commands)
            .await
            .map_err(|err| tonic::Status::failed_precondition(err.to_string()))?
            .into_iter()
            .map(encode_group_write_result)
            .collect();
        Ok(tonic::Response::new(
            raft_internal_proto::GroupWriteResponseV1 { results },
        ))
    }

    async fn group_read(
        &self,
        request: tonic::Request<raft_internal_proto::GroupReadRequestV1>,
    ) -> Result<tonic::Response<raft_internal_proto::GroupReadResponseV1>, tonic::Status> {
        let request = request.into_inner();
        let placement = placement_from_parts(
            request.core_id,
            request.shard_id,
            request.raft_group_id,
            "group_read_request",
        )
        .map_err(|err| tonic::Status::invalid_argument(err.to_string()))?;
        let raft = self
            .registry
            .get(placement.raft_group_id)
            .ok_or_else(|| tonic::Status::not_found("raft group is not registered"))?;
        let mut engine = RaftGroupEngine {
            raft,
            placement,
            metrics: None,
            cold_store: self.cold_store.clone(),
        };
        let stream_id = BucketStreamId::new(request.bucket_id, request.stream_id);
        let result = match required(request.read, "group_read.read")
            .map_err(|err| tonic::Status::invalid_argument(err.to_string()))?
        {
            raft_internal_proto::group_read_request_v1::Read::Head(_) => engine
                .head_stream(
                    HeadStreamRequest {
                        stream_id,
                        now_ms: request.now_ms,
                    },
                    placement,
                )
                .await
                .map(|response| raft_internal_proto::GroupReadResponseV1 {
                    ok: true,
                    payload: head_stream_response_to_proto(response).encode_to_vec(),
                }),
            raft_internal_proto::group_read_request_v1::Read::ReadStream(read) => {
                let max_len = usize::try_from(read.max_len).map_err(|_| {
                    tonic::Status::invalid_argument("group_read.read_stream.max_len too large")
                })?;
                engine
                    .read_stream(
                        ReadStreamRequest {
                            stream_id,
                            offset: read.offset,
                            max_len,
                            now_ms: request.now_ms,
                        },
                        placement,
                    )
                    .await
                    .map(|response| raft_internal_proto::GroupReadResponseV1 {
                        ok: true,
                        payload: read_stream_response_to_proto(response).encode_to_vec(),
                    })
            }
        };
        let response = match result {
            Ok(response) => response,
            Err(err) => raft_internal_proto::GroupReadResponseV1 {
                ok: false,
                payload: group_engine_error_to_proto(err).encode_to_vec(),
            },
        };
        Ok(tonic::Response::new(response))
    }
}

fn validate_raft_rpc_envelope(
    registry: &RaftGroupHandleRegistry,
    envelope: &raft_internal_proto::RaftRpcEnvelopeV1,
) -> Result<RaftGroupId, GrpcRpcError> {
    validate_grpc_metadata(envelope.protocol_version, &envelope.payload_codec)?;
    let raft_group_id = RaftGroupId(envelope.raft_group_id);
    if !registry.contains_group(raft_group_id) {
        return Err(GrpcRpcError::not_found(format!(
            "raft group {} is not registered on this node",
            raft_group_id.0
        )));
    }
    Ok(raft_group_id)
}

fn validate_raft_snapshot_request(
    registry: &RaftGroupHandleRegistry,
    request: &raft_internal_proto::RaftFullSnapshotRequestV1,
) -> Result<RaftGroupId, GrpcRpcError> {
    validate_grpc_metadata(request.protocol_version, &request.payload_codec)?;
    let raft_group_id = RaftGroupId(request.raft_group_id);
    if !registry.contains_group(raft_group_id) {
        return Err(GrpcRpcError::not_found(format!(
            "raft group {} is not registered on this node",
            raft_group_id.0
        )));
    }
    Ok(raft_group_id)
}

fn validate_grpc_metadata(protocol_version: u32, payload_codec: &str) -> Result<(), GrpcRpcError> {
    if protocol_version != RAFT_GRPC_PROTOCOL_VERSION {
        return Err(GrpcRpcError::failed_precondition(format!(
            "raft grpc protocol mismatch: local={}, remote={protocol_version}",
            RAFT_GRPC_PROTOCOL_VERSION
        )));
    }
    if payload_codec != RAFT_RPC_PAYLOAD_CODEC {
        return Err(GrpcRpcError::invalid_argument(format!(
            "unsupported raft grpc payload codec: {payload_codec}"
        )));
    }
    Ok(())
}

fn encode_grpc_payload<T: Serialize>(value: &T) -> Result<Vec<u8>, GrpcRpcError> {
    rmp_serde::to_vec_named(value)
        .map_err(|err| GrpcRpcError::internal(format!("encode raft grpc payload: {err}")))
}

fn decode_grpc_payload<T: DeserializeOwned>(payload: &[u8]) -> Result<T, GrpcRpcError> {
    rmp_serde::from_slice(payload)
        .map_err(|err| GrpcRpcError::invalid_argument(format!("decode raft grpc payload: {err}")))
}

#[derive(Debug, Clone)]
pub struct GrpcRaftNetworkFactory {
    raft_group_id: RaftGroupId,
}

impl GrpcRaftNetworkFactory {
    pub fn new(raft_group_id: RaftGroupId) -> Self {
        Self { raft_group_id }
    }
}

impl RaftNetworkFactory<UrsulaRaftTypeConfig> for GrpcRaftNetworkFactory {
    type Network = GrpcRaftNetwork;

    async fn new_client(&mut self, target: u64, node: &BasicNode) -> Self::Network {
        GrpcRaftNetwork::new(self.raft_group_id, target, node.addr.clone())
    }
}

#[derive(Clone)]
pub struct GrpcRaftNetwork {
    raft_group_id: RaftGroupId,
    target: u64,
    endpoint: String,
    client: Result<raft_internal_proto::raft_internal_client::RaftInternalClient<Channel>, String>,
}

impl Debug for GrpcRaftNetwork {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GrpcRaftNetwork")
            .field("raft_group_id", &self.raft_group_id)
            .field("target", &self.target)
            .field("endpoint", &self.endpoint)
            .finish()
    }
}

impl GrpcRaftNetwork {
    pub fn new(raft_group_id: RaftGroupId, target: u64, address: impl Into<String>) -> Self {
        let endpoint = normalize_grpc_endpoint(address.into());
        let client = Endpoint::from_shared(endpoint.clone())
            .map(|endpoint| {
                raft_internal_proto::raft_internal_client::RaftInternalClient::new(
                    endpoint.connect_lazy(),
                )
                .max_decoding_message_size(RAFT_GRPC_MAX_MESSAGE_BYTES)
                .max_encoding_message_size(RAFT_GRPC_MAX_MESSAGE_BYTES)
            })
            .map_err(|err| format!("invalid raft gRPC endpoint {endpoint}: {err}"));
        Self {
            raft_group_id,
            target,
            endpoint,
            client,
        }
    }

    fn client(
        &self,
    ) -> Result<
        raft_internal_proto::raft_internal_client::RaftInternalClient<Channel>,
        RPCError<UrsulaRaftTypeConfig>,
    > {
        self.client
            .clone()
            .map_err(|err| RPCError::Unreachable(Unreachable::from_string(err)))
    }

    fn envelope<T: Serialize>(
        &self,
        request: &T,
        route: &str,
    ) -> Result<raft_internal_proto::RaftRpcEnvelopeV1, RPCError<UrsulaRaftTypeConfig>> {
        let payload = rmp_serde::to_vec_named(request)
            .map_err(|err| raft_rpc_network_error(format!("serialize {route}: {err}")))?;
        Ok(raft_internal_proto::RaftRpcEnvelopeV1 {
            raft_group_id: self.raft_group_id.0,
            node_id: self.target,
            protocol_version: RAFT_GRPC_PROTOCOL_VERSION,
            payload_codec: RAFT_RPC_PAYLOAD_CODEC.to_owned(),
            payload,
        })
    }

    fn apply_rpc_timeout<T>(&self, request: &mut tonic::Request<T>, option: RPCOption) {
        request.set_timeout(option.hard_ttl());
    }

    fn map_tonic_status(
        &self,
        route: &str,
        status: tonic::Status,
    ) -> RPCError<UrsulaRaftTypeConfig> {
        let message = format!(
            "{route} to node {} at {} failed: {}",
            self.target, self.endpoint, status
        );
        match status.code() {
            tonic::Code::Unavailable | tonic::Code::Cancelled => {
                RPCError::Unreachable(Unreachable::from_string(message))
            }
            _ => raft_rpc_network_error(message),
        }
    }

    fn decode_ack<T: DeserializeOwned>(
        &self,
        route: &str,
        payload: Vec<u8>,
    ) -> Result<T, RPCError<UrsulaRaftTypeConfig>> {
        rmp_serde::from_slice(&payload).map_err(|err| {
            raft_rpc_network_error(format!(
                "decode {route} response from node {} at {}: {err}",
                self.target, self.endpoint
            ))
        })
    }
}

fn normalize_grpc_endpoint(address: String) -> String {
    let address = address.trim_end_matches('/').to_owned();
    if address.starts_with("http://") || address.starts_with("https://") {
        address
    } else {
        format!("http://{address}")
    }
}

fn raft_rpc_network_error(message: impl ToString) -> RPCError<UrsulaRaftTypeConfig> {
    RPCError::Network(NetworkError::from_string(message))
}

impl RaftNetworkV2<UrsulaRaftTypeConfig> for GrpcRaftNetwork {
    async fn append_entries(
        &mut self,
        rpc: UrsulaAppendEntriesRequest,
        option: RPCOption,
    ) -> Result<UrsulaAppendEntriesResponse, RPCError<UrsulaRaftTypeConfig>> {
        let mut request = tonic::Request::new(self.envelope(&rpc, "Append")?);
        self.apply_rpc_timeout(&mut request, option);
        let response = self
            .client()?
            .append(request)
            .await
            .map_err(|err| self.map_tonic_status("Append", err))?
            .into_inner();
        self.decode_ack("Append", response.payload)
    }

    async fn vote(
        &mut self,
        rpc: UrsulaVoteRequest,
        option: RPCOption,
    ) -> Result<UrsulaVoteResponse, RPCError<UrsulaRaftTypeConfig>> {
        let mut request = tonic::Request::new(self.envelope(&rpc, "Vote")?);
        self.apply_rpc_timeout(&mut request, option);
        let response = self
            .client()?
            .vote(request)
            .await
            .map_err(|err| self.map_tonic_status("Vote", err))?
            .into_inner();
        self.decode_ack("Vote", response.payload)
    }

    async fn full_snapshot(
        &mut self,
        vote: VoteOf<UrsulaRaftTypeConfig>,
        snapshot: SnapshotOf<UrsulaRaftTypeConfig>,
        _cancel: impl Future<Output = ReplicationClosed> + OptionalSend + 'static,
        option: RPCOption,
    ) -> Result<SnapshotResponse<UrsulaRaftTypeConfig>, StreamingError<UrsulaRaftTypeConfig>> {
        let request = raft_internal_proto::RaftFullSnapshotRequestV1 {
            raft_group_id: self.raft_group_id.0,
            node_id: self.target,
            protocol_version: RAFT_GRPC_PROTOCOL_VERSION,
            payload_codec: RAFT_RPC_PAYLOAD_CODEC.to_owned(),
            vote_payload: rmp_serde::to_vec_named(&vote).map_err(|err| {
                StreamingError::from(raft_rpc_network_error(format!(
                    "serialize FullSnapshot vote: {err}"
                )))
            })?,
            snapshot_meta_payload: rmp_serde::to_vec_named(&snapshot.meta).map_err(|err| {
                StreamingError::from(raft_rpc_network_error(format!(
                    "serialize FullSnapshot metadata: {err}"
                )))
            })?,
            snapshot_payload: snapshot.snapshot.into_inner(),
        };
        let mut request = tonic::Request::new(request);
        self.apply_rpc_timeout(&mut request, option);
        let response = self
            .client()
            .map_err(StreamingError::from)?
            .full_snapshot(request)
            .await
            .map_err(|err| StreamingError::from(self.map_tonic_status("FullSnapshot", err)))?
            .into_inner();
        self.decode_ack("FullSnapshot", response.payload)
            .map_err(StreamingError::from)
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct RaftGroupLogStoreInner {
    last_purged_log_id: Option<LogIdOf<UrsulaRaftTypeConfig>>,
    committed: Option<LogIdOf<UrsulaRaftTypeConfig>>,
    entries: BTreeMap<u64, EntryOf<UrsulaRaftTypeConfig>>,
    vote: Option<VoteOf<UrsulaRaftTypeConfig>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "operation", rename_all = "snake_case")]
enum RaftGroupLogRecord {
    SaveVote {
        vote: VoteOf<UrsulaRaftTypeConfig>,
    },
    SaveCommitted {
        committed: Option<LogIdOf<UrsulaRaftTypeConfig>>,
    },
    Append {
        entries: Vec<StoredLogEntry>,
    },
    TruncateAfter {
        last_log_id: Option<LogIdOf<UrsulaRaftTypeConfig>>,
    },
    Purge {
        log_id: LogIdOf<UrsulaRaftTypeConfig>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CoreJournalRecord {
    group_id: u32,
    record: RaftGroupLogRecord,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredLogEntry {
    log_id: LogIdOf<UrsulaRaftTypeConfig>,
    payload: StoredEntryPayload,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "payload", rename_all = "snake_case")]
enum StoredEntryPayload {
    Blank,
    Normal {
        command: Vec<u8>,
    },
    Membership {
        configs: Vec<BTreeSet<u64>>,
        nodes: Vec<(u64, BasicNode)>,
    },
}

impl From<&EntryOf<UrsulaRaftTypeConfig>> for StoredLogEntry {
    fn from(entry: &EntryOf<UrsulaRaftTypeConfig>) -> Self {
        let payload = match &entry.payload {
            EntryPayload::Blank => StoredEntryPayload::Blank,
            EntryPayload::Normal(command) => StoredEntryPayload::Normal {
                command: command.0.encode_to_vec(),
            },
            EntryPayload::Membership(membership) => StoredEntryPayload::Membership {
                configs: membership.get_joint_config().clone(),
                nodes: membership
                    .nodes()
                    .map(|(node_id, node)| (*node_id, node.clone()))
                    .collect(),
            },
        };
        Self {
            log_id: entry.log_id,
            payload,
        }
    }
}

impl StoredLogEntry {
    fn into_entry(self) -> Result<EntryOf<UrsulaRaftTypeConfig>, io::Error> {
        let payload = match self.payload {
            StoredEntryPayload::Blank => EntryPayload::Blank,
            StoredEntryPayload::Normal { command } => {
                let command = raft_app_proto::RaftGroupCommandV1::decode(command.as_slice())
                    .map_err(invalid_data)?;
                EntryPayload::Normal(RaftGroupCommand(command))
            }
            StoredEntryPayload::Membership { configs, nodes } => {
                let nodes = nodes.into_iter().collect::<BTreeMap<_, _>>();
                let membership = Membership::new(configs, nodes).map_err(invalid_data)?;
                EntryPayload::Membership(membership)
            }
        };
        Ok(Entry::new(self.log_id, payload))
    }
}

#[derive(Debug, Default)]
pub struct RaftGroupLogStore {
    inner: Mutex<RaftGroupLogStoreInner>,
}

impl RaftGroupLogStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn shared() -> Arc<Self> {
        Arc::new(Self::new())
    }

    fn lock_inner(&self) -> Result<MutexGuard<'_, RaftGroupLogStoreInner>, io::Error> {
        self.inner
            .lock()
            .map_err(|_| io::Error::other("raft group log store mutex poisoned"))
    }
}

impl RaftLogReader<UrsulaRaftTypeConfig> for Arc<RaftGroupLogStore> {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<EntryOf<UrsulaRaftTypeConfig>>, io::Error> {
        let inner = self.lock_inner()?;
        let entries = inner
            .entries
            .range(range)
            .map(|(_, entry)| entry.clone())
            .collect::<Vec<_>>();

        ensure_consecutive_entries(&entries)?;
        Ok(entries)
    }

    async fn read_vote(&mut self) -> Result<Option<VoteOf<UrsulaRaftTypeConfig>>, io::Error> {
        Ok(self.lock_inner()?.vote)
    }
}

impl RaftLogStorage<UrsulaRaftTypeConfig> for Arc<RaftGroupLogStore> {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<UrsulaRaftTypeConfig>, io::Error> {
        let inner = self.lock_inner()?;
        let last_log_id = inner
            .entries
            .last_key_value()
            .map(|(_, entry)| entry.log_id)
            .or(inner.last_purged_log_id);

        Ok(LogState {
            last_purged_log_id: inner.last_purged_log_id,
            last_log_id,
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &VoteOf<UrsulaRaftTypeConfig>) -> Result<(), io::Error> {
        let mut inner = self.lock_inner()?;
        if inner.vote != Some(*vote) {
            inner.vote = Some(*vote);
        }
        Ok(())
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogIdOf<UrsulaRaftTypeConfig>>,
    ) -> Result<(), io::Error> {
        let mut inner = self.lock_inner()?;
        if inner.committed != committed {
            inner.committed = committed;
        }
        Ok(())
    }

    async fn read_committed(&mut self) -> Result<Option<LogIdOf<UrsulaRaftTypeConfig>>, io::Error> {
        Ok(self.lock_inner()?.committed)
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: IOFlushed<UrsulaRaftTypeConfig>,
    ) -> Result<(), io::Error>
    where
        I: IntoIterator<Item = EntryOf<UrsulaRaftTypeConfig>> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let entries = entries.into_iter().collect::<Vec<_>>();
        ensure_consecutive_entries(&entries)?;

        let mut inner = self.lock_inner()?;
        ensure_log_append_boundary(&inner, &entries)?;
        for entry in entries {
            inner.entries.insert(entry.log_id.index, entry);
        }

        callback.io_completed(Ok(()));
        Ok(())
    }

    async fn truncate_after(
        &mut self,
        last_log_id: Option<LogIdOf<UrsulaRaftTypeConfig>>,
    ) -> Result<(), io::Error> {
        let start_index = last_log_id.map_or(0, |log_id| log_id.index + 1);
        let mut inner = self.lock_inner()?;
        inner.entries.retain(|index, _| *index < start_index);
        Ok(())
    }

    async fn purge(&mut self, log_id: LogIdOf<UrsulaRaftTypeConfig>) -> Result<(), io::Error> {
        let mut inner = self.lock_inner()?;
        if inner.last_purged_log_id > Some(log_id) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "cannot move last purged log id backward from {:?} to {:?}",
                    inner.last_purged_log_id, log_id
                ),
            ));
        }

        inner.last_purged_log_id = Some(log_id);
        inner.entries.retain(|index, _| *index > log_id.index);
        Ok(())
    }
}

#[derive(Debug)]
pub struct RaftGroupFileLogStore {
    path: PathBuf,
    inner: Mutex<RaftGroupLogStoreInner>,
    file: Mutex<RaftGroupFileLogHandle>,
    metrics: Option<RaftGroupFileLogStoreMetrics>,
    core_writer: Option<Arc<CoreFileLogWriter>>,
}

#[derive(Debug, Clone)]
struct RaftGroupFileLogStoreMetrics {
    placement: ShardPlacement,
    metrics: GroupEngineMetrics,
}

#[derive(Debug)]
struct RaftGroupFileLogHandle {
    file: Option<File>,
    parent_needs_sync: bool,
}

#[derive(Debug)]
struct CoreFileLogWriter {
    journal_path: PathBuf,
    tx: mpsc::Sender<CoreFileLogWrite>,
}

#[derive(Debug)]
struct CoreFileLogWrite {
    group_id: u32,
    record: RaftGroupLogRecord,
    response_tx: mpsc::Sender<Result<CoreFileLogWriteTiming, String>>,
}

#[derive(Debug, Clone, Copy)]
struct CoreFileLogWriteTiming {
    write_ns: u64,
    sync_ns: u64,
}

impl RaftGroupFileLogStore {
    pub fn open(path: impl Into<PathBuf>) -> Result<Self, io::Error> {
        Self::open_inner(path.into(), None, None)
    }

    pub fn open_with_metrics(
        path: impl Into<PathBuf>,
        placement: ShardPlacement,
        metrics: GroupEngineMetrics,
    ) -> Result<Self, io::Error> {
        Self::open_inner(
            path.into(),
            Some(RaftGroupFileLogStoreMetrics { placement, metrics }),
            None,
        )
    }

    fn open_with_core_writer(
        path: impl Into<PathBuf>,
        placement: ShardPlacement,
        metrics: GroupEngineMetrics,
        core_writer: Arc<CoreFileLogWriter>,
    ) -> Result<Self, io::Error> {
        Self::open_inner(
            path.into(),
            Some(RaftGroupFileLogStoreMetrics { placement, metrics }),
            Some(core_writer),
        )
    }

    fn open_inner(
        path: PathBuf,
        metrics: Option<RaftGroupFileLogStoreMetrics>,
        core_writer: Option<Arc<CoreFileLogWriter>>,
    ) -> Result<Self, io::Error> {
        let parent_needs_sync = !path.exists();
        let inner = match (&core_writer, &metrics) {
            (Some(writer), Some(metrics)) => {
                load_log_store_inner_from_core_journal(writer.journal_path(), metrics.placement)?
            }
            _ => load_log_store_inner(&path)?,
        };
        Ok(Self {
            path,
            inner: Mutex::new(inner),
            file: Mutex::new(RaftGroupFileLogHandle {
                file: None,
                parent_needs_sync,
            }),
            metrics,
            core_writer,
        })
    }

    pub fn shared(path: impl Into<PathBuf>) -> Result<Arc<Self>, io::Error> {
        Self::open(path).map(Arc::new)
    }

    pub fn shared_with_metrics(
        path: impl Into<PathBuf>,
        placement: ShardPlacement,
        metrics: GroupEngineMetrics,
    ) -> Result<Arc<Self>, io::Error> {
        Self::open_with_metrics(path, placement, metrics).map(Arc::new)
    }

    fn shared_with_core_writer(
        path: impl Into<PathBuf>,
        placement: ShardPlacement,
        metrics: GroupEngineMetrics,
        core_writer: Arc<CoreFileLogWriter>,
    ) -> Result<Arc<Self>, io::Error> {
        Self::open_with_core_writer(path, placement, metrics, core_writer).map(Arc::new)
    }

    fn lock_inner(&self) -> Result<MutexGuard<'_, RaftGroupLogStoreInner>, io::Error> {
        self.inner
            .lock()
            .map_err(|_| io::Error::other("raft group file log store mutex poisoned"))
    }

    fn lock_file(&self) -> Result<MutexGuard<'_, RaftGroupFileLogHandle>, io::Error> {
        self.file
            .lock()
            .map_err(|_| io::Error::other("raft group file log store file mutex poisoned"))
    }

    fn append_record_locked(&self, record: &RaftGroupLogRecord) -> Result<(), io::Error> {
        let (write_ns, sync_ns) = if let Some(core_writer) = &self.core_writer {
            let metrics = self
                .metrics
                .as_ref()
                .expect("core journal writer requires placement metrics");
            core_writer.append(metrics.placement.raft_group_id.0, record.clone())?
        } else {
            let mut file = self.lock_file()?;
            append_log_store_record(&self.path, &mut file, record)?
        };
        if let Some(metrics) = &self.metrics {
            metrics.metrics.record_wal_batch(
                metrics.placement,
                raft_group_log_record_count(record),
                write_ns,
                sync_ns,
            );
        }
        Ok(())
    }
}

impl CoreFileLogWriter {
    fn shared(journal_path: impl Into<PathBuf>) -> Result<Arc<Self>, io::Error> {
        let journal_path = journal_path.into();
        if let Some(parent) = journal_path.parent() {
            fs::create_dir_all(parent)?;
        }
        let (tx, rx) = mpsc::channel();
        let writer = Arc::new(Self {
            journal_path: journal_path.clone(),
            tx,
        });
        std::thread::Builder::new()
            .name("ursula-core-file-log-writer".to_owned())
            .spawn(move || run_core_file_log_writer(journal_path, rx))
            .map_err(|err| io::Error::other(format!("spawn core file log writer: {err}")))?;
        Ok(writer)
    }

    fn journal_path(&self) -> &Path {
        &self.journal_path
    }

    fn append(&self, group_id: u32, record: RaftGroupLogRecord) -> Result<(u64, u64), io::Error> {
        let (response_tx, response_rx) = mpsc::channel();
        self.tx
            .send(CoreFileLogWrite {
                group_id,
                record,
                response_tx,
            })
            .map_err(|_| io::Error::other("core file log writer closed"))?;
        let timing = response_rx
            .recv()
            .map_err(|_| io::Error::other("core file log writer dropped response"))?
            .map_err(io::Error::other)?;
        Ok((timing.write_ns, timing.sync_ns))
    }
}

fn run_core_file_log_writer(journal_path: PathBuf, rx: mpsc::Receiver<CoreFileLogWrite>) {
    let mut journal_handle = RaftGroupFileLogHandle {
        parent_needs_sync: !journal_path.exists(),
        file: None,
    };

    while let Ok(first) = rx.recv() {
        let mut batch = vec![first];
        if let Ok(next) = rx.recv_timeout(CORE_LOG_GROUP_COMMIT_DELAY) {
            batch.push(next);
        }
        while batch.len() < CORE_LOG_GROUP_COMMIT_MAX_BATCH {
            match rx.try_recv() {
                Ok(next) => batch.push(next),
                Err(mpsc::TryRecvError::Empty) => break,
                Err(mpsc::TryRecvError::Disconnected) => break,
            }
        }

        let result = write_core_log_batch(&journal_path, &mut journal_handle, &batch);
        match result {
            Ok(timing) => {
                let count = u64::try_from(batch.len()).expect("batch len fits u64");
                let per_request = CoreFileLogWriteTiming {
                    write_ns: timing.write_ns / count.max(1),
                    sync_ns: timing.sync_ns / count.max(1),
                };
                for request in batch {
                    let _ = request.response_tx.send(Ok(per_request));
                }
            }
            Err(err) => {
                let message = err.to_string();
                for request in batch {
                    let _ = request.response_tx.send(Err(message.clone()));
                }
            }
        }
    }
}

fn write_core_log_batch(
    journal_path: &Path,
    journal_handle: &mut RaftGroupFileLogHandle,
    batch: &[CoreFileLogWrite],
) -> Result<CoreFileLogWriteTiming, io::Error> {
    let write_started_at = Instant::now();
    for request in batch {
        let journal_record = CoreJournalRecord {
            group_id: request.group_id,
            record: request.record.clone(),
        };
        write_binary_frame_to_file(journal_path, journal_handle, &journal_record)?;
    }
    let write_ns = elapsed_ns(write_started_at);

    let sync_started_at = Instant::now();
    sync_file_handle(journal_path, journal_handle)?;
    Ok(CoreFileLogWriteTiming {
        write_ns,
        sync_ns: elapsed_ns(sync_started_at),
    })
}

impl RaftLogReader<UrsulaRaftTypeConfig> for Arc<RaftGroupFileLogStore> {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<EntryOf<UrsulaRaftTypeConfig>>, io::Error> {
        let inner = self.lock_inner()?;
        let entries = inner
            .entries
            .range(range)
            .map(|(_, entry)| entry.clone())
            .collect::<Vec<_>>();

        ensure_consecutive_entries(&entries)?;
        Ok(entries)
    }

    async fn read_vote(&mut self) -> Result<Option<VoteOf<UrsulaRaftTypeConfig>>, io::Error> {
        Ok(self.lock_inner()?.vote)
    }
}

impl RaftLogStorage<UrsulaRaftTypeConfig> for Arc<RaftGroupFileLogStore> {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<UrsulaRaftTypeConfig>, io::Error> {
        let inner = self.lock_inner()?;
        let last_log_id = inner
            .entries
            .last_key_value()
            .map(|(_, entry)| entry.log_id)
            .or(inner.last_purged_log_id);

        Ok(LogState {
            last_purged_log_id: inner.last_purged_log_id,
            last_log_id,
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &VoteOf<UrsulaRaftTypeConfig>) -> Result<(), io::Error> {
        let store = Arc::clone(self);
        let vote = *vote;
        spawn_log_store_blocking(move || {
            let mut inner = store.lock_inner()?;
            if inner.vote == Some(vote) {
                return Ok(());
            }
            let mut next = inner.clone();
            next.vote = Some(vote);
            store.append_record_locked(&RaftGroupLogRecord::SaveVote { vote })?;
            *inner = next;
            Ok(())
        })
        .await
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogIdOf<UrsulaRaftTypeConfig>>,
    ) -> Result<(), io::Error> {
        let store = Arc::clone(self);
        spawn_log_store_blocking(move || {
            let mut inner = store.lock_inner()?;
            if inner.committed == committed {
                return Ok(());
            }
            let mut next = inner.clone();
            next.committed = committed;
            store.append_record_locked(&RaftGroupLogRecord::SaveCommitted { committed })?;
            *inner = next;
            Ok(())
        })
        .await
    }

    async fn read_committed(&mut self) -> Result<Option<LogIdOf<UrsulaRaftTypeConfig>>, io::Error> {
        Ok(self.lock_inner()?.committed)
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: IOFlushed<UrsulaRaftTypeConfig>,
    ) -> Result<(), io::Error>
    where
        I: IntoIterator<Item = EntryOf<UrsulaRaftTypeConfig>> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let entries = entries.into_iter().collect::<Vec<_>>();
        let store = Arc::clone(self);
        spawn_log_store_blocking(move || {
            ensure_consecutive_entries(&entries)?;

            let mut inner = store.lock_inner()?;
            ensure_log_append_boundary(&inner, &entries)?;

            let record = RaftGroupLogRecord::Append {
                entries: entries.iter().map(StoredLogEntry::from).collect(),
            };
            if let Err(err) = store.append_record_locked(&record) {
                callback.io_completed(Err(io::Error::new(err.kind(), err.to_string())));
                return Err(err);
            }
            for entry in entries {
                inner.entries.insert(entry.log_id.index, entry);
            }
            callback.io_completed(Ok(()));
            Ok(())
        })
        .await
    }

    async fn truncate_after(
        &mut self,
        last_log_id: Option<LogIdOf<UrsulaRaftTypeConfig>>,
    ) -> Result<(), io::Error> {
        let store = Arc::clone(self);
        spawn_log_store_blocking(move || {
            let start_index = last_log_id.map_or(0, |log_id| log_id.index + 1);
            let mut inner = store.lock_inner()?;
            let mut next = inner.clone();
            next.entries.retain(|index, _| *index < start_index);
            store.append_record_locked(&RaftGroupLogRecord::TruncateAfter { last_log_id })?;
            *inner = next;
            Ok(())
        })
        .await
    }

    async fn purge(&mut self, log_id: LogIdOf<UrsulaRaftTypeConfig>) -> Result<(), io::Error> {
        let store = Arc::clone(self);
        spawn_log_store_blocking(move || {
            let mut inner = store.lock_inner()?;
            if inner.last_purged_log_id > Some(log_id) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!(
                        "cannot move last purged log id backward from {:?} to {:?}",
                        inner.last_purged_log_id, log_id
                    ),
                ));
            }

            let mut next = inner.clone();
            next.last_purged_log_id = Some(log_id);
            next.entries.retain(|index, _| *index > log_id.index);
            store.append_record_locked(&RaftGroupLogRecord::Purge { log_id })?;
            *inner = next;
            Ok(())
        })
        .await
    }
}

async fn spawn_log_store_blocking<T>(
    f: impl FnOnce() -> Result<T, io::Error> + Send + 'static,
) -> Result<T, io::Error>
where
    T: Send + 'static,
{
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|err| io::Error::other(format!("join OpenRaft file log task: {err}")))?
}

fn load_log_store_inner(path: &Path) -> Result<RaftGroupLogStoreInner, io::Error> {
    if !path.exists() {
        return Ok(RaftGroupLogStoreInner::default());
    }

    let bytes = fs::read(path)?;
    if bytes.is_empty() {
        return Ok(RaftGroupLogStoreInner::default());
    }

    if let Ok(inner) = serde_json::from_slice::<RaftGroupLogStoreInner>(&bytes) {
        return Ok(inner);
    }

    let file = File::open(path)?;
    let reader = BufReader::new(file);
    let mut inner = RaftGroupLogStoreInner::default();
    for (line_index, line) in reader.lines().enumerate() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let record = serde_json::from_str::<RaftGroupLogRecord>(&line).map_err(|err| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "decode OpenRaft log record '{}' line {}: {err}",
                    path.display(),
                    line_index + 1
                ),
            )
        })?;
        apply_log_store_record(&mut inner, record).map_err(|err| {
            io::Error::new(
                err.kind(),
                format!(
                    "replay OpenRaft log record '{}' line {}: {err}",
                    path.display(),
                    line_index + 1
                ),
            )
        })?;
    }
    Ok(inner)
}

fn load_log_store_inner_from_core_journal(
    journal_path: &Path,
    placement: ShardPlacement,
) -> Result<RaftGroupLogStoreInner, io::Error> {
    if !journal_path.exists() {
        return Ok(RaftGroupLogStoreInner::default());
    }

    let bytes = fs::read(journal_path)?;
    if bytes.is_empty() {
        return Ok(RaftGroupLogStoreInner::default());
    }

    let mut inner = RaftGroupLogStoreInner::default();
    let mut cursor = Cursor::new(bytes.as_slice());
    let mut record_index = 0usize;
    while usize::try_from(cursor.position()).expect("cursor position fits usize") < bytes.len() {
        let mut len_bytes = [0_u8; 4];
        cursor.read_exact(&mut len_bytes).map_err(|err| {
            io::Error::new(
                err.kind(),
                format!(
                    "read OpenRaft core journal record length '{}' record {}: {err}",
                    journal_path.display(),
                    record_index + 1
                ),
            )
        })?;
        let len = usize::try_from(u32::from_le_bytes(len_bytes)).expect("u32 fits usize");
        let mut record_bytes = vec![0_u8; len];
        cursor.read_exact(&mut record_bytes).map_err(|err| {
            io::Error::new(
                err.kind(),
                format!(
                    "read OpenRaft core journal record '{}' record {}: {err}",
                    journal_path.display(),
                    record_index + 1
                ),
            )
        })?;
        let record = rmp_serde::from_slice::<CoreJournalRecord>(&record_bytes).map_err(|err| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "decode OpenRaft core journal record '{}' record {}: {err}",
                    journal_path.display(),
                    record_index + 1
                ),
            )
        })?;
        record_index += 1;
        if record.group_id != placement.raft_group_id.0 {
            continue;
        }
        apply_log_store_record(&mut inner, record.record).map_err(|err| {
            io::Error::new(
                err.kind(),
                format!(
                    "replay OpenRaft core journal record '{}' record {}: {err}",
                    journal_path.display(),
                    record_index
                ),
            )
        })?;
    }
    Ok(inner)
}

fn append_log_store_record(
    path: &Path,
    handle: &mut RaftGroupFileLogHandle,
    record: &RaftGroupLogRecord,
) -> Result<(u64, u64), io::Error> {
    let write_started_at = Instant::now();
    write_json_line_to_file(path, handle, record)?;
    let write_ns = elapsed_ns(write_started_at);

    let sync_started_at = Instant::now();
    sync_file_handle(path, handle)?;
    Ok((write_ns, elapsed_ns(sync_started_at)))
}

fn write_json_line_to_file<T: Serialize>(
    path: &Path,
    handle: &mut RaftGroupFileLogHandle,
    value: &T,
) -> Result<(), io::Error> {
    if handle.file.is_none() {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        handle.file = Some(OpenOptions::new().create(true).append(true).open(path)?);
    }
    let file = handle
        .file
        .as_mut()
        .expect("file handle is opened before write");
    serde_json::to_writer(&mut *file, value).map_err(invalid_data)?;
    file.write_all(b"\n")
}

fn write_binary_frame_to_file<T: Serialize>(
    path: &Path,
    handle: &mut RaftGroupFileLogHandle,
    value: &T,
) -> Result<(), io::Error> {
    if handle.file.is_none() {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        handle.file = Some(OpenOptions::new().create(true).append(true).open(path)?);
    }
    let file = handle
        .file
        .as_mut()
        .expect("file handle is opened before write");
    let bytes = rmp_serde::to_vec(value).map_err(invalid_data)?;
    let len = u32::try_from(bytes.len())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "core journal record too large"))?;
    file.write_all(&len.to_le_bytes())?;
    file.write_all(&bytes)
}

fn sync_file_handle(path: &Path, handle: &mut RaftGroupFileLogHandle) -> Result<(), io::Error> {
    let file = handle
        .file
        .as_mut()
        .expect("file handle is opened before sync");
    file.sync_data()?;
    if handle.parent_needs_sync
        && let Some(parent) = path.parent()
        && let Ok(parent_dir) = File::open(parent)
    {
        parent_dir.sync_all()?;
        handle.parent_needs_sync = false;
    }
    Ok(())
}

fn raft_group_log_record_count(record: &RaftGroupLogRecord) -> usize {
    match record {
        RaftGroupLogRecord::Append { entries } => entries.len(),
        RaftGroupLogRecord::SaveVote { .. }
        | RaftGroupLogRecord::SaveCommitted { .. }
        | RaftGroupLogRecord::TruncateAfter { .. }
        | RaftGroupLogRecord::Purge { .. } => 1,
    }
}

fn elapsed_ns(started_at: Instant) -> u64 {
    u64::try_from(started_at.elapsed().as_nanos()).unwrap_or(u64::MAX)
}

fn apply_log_store_record(
    inner: &mut RaftGroupLogStoreInner,
    record: RaftGroupLogRecord,
) -> Result<(), io::Error> {
    match record {
        RaftGroupLogRecord::SaveVote { vote } => {
            inner.vote = Some(vote);
            Ok(())
        }
        RaftGroupLogRecord::SaveCommitted { committed } => {
            inner.committed = committed;
            Ok(())
        }
        RaftGroupLogRecord::Append { entries } => {
            let entries = entries
                .into_iter()
                .map(StoredLogEntry::into_entry)
                .collect::<Result<Vec<_>, _>>()?;
            ensure_consecutive_entries(&entries)?;
            for entry in entries {
                inner.entries.insert(entry.log_id.index, entry);
            }
            ensure_consecutive_log(&inner.entries)
        }
        RaftGroupLogRecord::TruncateAfter { last_log_id } => {
            let start_index = last_log_id.map_or(0, |log_id| log_id.index + 1);
            inner.entries.retain(|index, _| *index < start_index);
            Ok(())
        }
        RaftGroupLogRecord::Purge { log_id } => {
            if inner.last_purged_log_id > Some(log_id) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!(
                        "cannot move last purged log id backward from {:?} to {:?}",
                        inner.last_purged_log_id, log_id
                    ),
                ));
            }
            inner.last_purged_log_id = Some(log_id);
            inner.entries.retain(|index, _| *index > log_id.index);
            Ok(())
        }
    }
}

fn ensure_consecutive_entries(entries: &[EntryOf<UrsulaRaftTypeConfig>]) -> Result<(), io::Error> {
    for pair in entries.windows(2) {
        let current = pair[0].log_id.index;
        let next = pair[1].log_id.index;
        if next != current + 1 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("raft log entries are not consecutive: {current} then {next}"),
            ));
        }
    }
    Ok(())
}

fn ensure_log_append_boundary(
    inner: &RaftGroupLogStoreInner,
    entries: &[EntryOf<UrsulaRaftTypeConfig>],
) -> Result<(), io::Error> {
    let Some(first_entry) = entries.first() else {
        return Ok(());
    };
    let Some(last_existing_index) = inner.entries.keys().next_back().copied() else {
        return Ok(());
    };

    let first_append_index = first_entry.log_id.index;
    if first_append_index > last_existing_index + 1 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("raft log store has a hole: {last_existing_index} then {first_append_index}"),
        ));
    }

    Ok(())
}

fn ensure_consecutive_log(
    entries: &BTreeMap<u64, EntryOf<UrsulaRaftTypeConfig>>,
) -> Result<(), io::Error> {
    let mut previous = None;
    for index in entries.keys().copied() {
        if let Some(previous) = previous
            && index != previous + 1
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("raft log store has a hole: {previous} then {index}"),
            ));
        }
        previous = Some(index);
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, Default)]
pub struct SingleNodeRaftNetworkFactory;

#[derive(Debug, Clone, Copy, Default)]
pub struct SingleNodeRaftNetwork;

impl RaftNetworkFactory<UrsulaRaftTypeConfig> for SingleNodeRaftNetworkFactory {
    type Network = SingleNodeRaftNetwork;

    async fn new_client(&mut self, _target: u64, _node: &BasicNode) -> Self::Network {
        SingleNodeRaftNetwork
    }
}

impl RaftNetworkV2<UrsulaRaftTypeConfig> for SingleNodeRaftNetwork {
    async fn append_entries(
        &mut self,
        _rpc: AppendEntriesRequest<UrsulaRaftTypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<UrsulaRaftTypeConfig>, RPCError<UrsulaRaftTypeConfig>> {
        unreachable!("single-node raft group must not send AppendEntries")
    }

    async fn vote(
        &mut self,
        _rpc: VoteRequest<UrsulaRaftTypeConfig>,
        _option: RPCOption,
    ) -> Result<VoteResponse<UrsulaRaftTypeConfig>, RPCError<UrsulaRaftTypeConfig>> {
        unreachable!("single-node raft group must not send Vote")
    }

    async fn full_snapshot(
        &mut self,
        _vote: VoteOf<UrsulaRaftTypeConfig>,
        _snapshot: TypeConfigSnapshotOf<UrsulaRaftTypeConfig>,
        _cancel: impl Future<Output = ReplicationClosed> + OptionalSend + 'static,
        _option: RPCOption,
    ) -> Result<SnapshotResponse<UrsulaRaftTypeConfig>, StreamingError<UrsulaRaftTypeConfig>> {
        unreachable!("single-node raft group must not send snapshots")
    }
}

#[derive(Debug, Clone, Default)]
pub struct RaftGroupHandleRegistry {
    groups: Arc<Mutex<BTreeMap<u32, Raft<UrsulaRaftTypeConfig, RaftGroupStateMachine>>>>,
}

impl RaftGroupHandleRegistry {
    pub fn register(
        &self,
        placement: ShardPlacement,
        raft: Raft<UrsulaRaftTypeConfig, RaftGroupStateMachine>,
    ) {
        self.groups
            .lock()
            .expect("raft group handle registry mutex")
            .insert(placement.raft_group_id.0, raft);
    }

    pub fn get(
        &self,
        raft_group_id: RaftGroupId,
    ) -> Option<Raft<UrsulaRaftTypeConfig, RaftGroupStateMachine>> {
        self.groups
            .lock()
            .expect("raft group handle registry mutex")
            .get(&raft_group_id.0)
            .cloned()
    }

    pub fn contains_group(&self, raft_group_id: RaftGroupId) -> bool {
        self.groups
            .lock()
            .expect("raft group handle registry mutex")
            .contains_key(&raft_group_id.0)
    }

    pub fn len(&self) -> usize {
        self.groups
            .lock()
            .expect("raft group handle registry mutex")
            .len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn metrics_snapshot(&self) -> Vec<RaftGroupMetricsSnapshot> {
        let groups = self
            .groups
            .lock()
            .expect("raft group handle registry mutex")
            .iter()
            .map(|(raft_group_id, raft)| (*raft_group_id, raft.clone()))
            .collect::<Vec<_>>();

        let mut snapshots = Vec::with_capacity(groups.len());
        for (raft_group_id, raft) in groups {
            let metrics = raft.metrics().borrow_watched().clone();
            let membership = metrics.membership_config.membership();
            snapshots.push(RaftGroupMetricsSnapshot {
                raft_group_id,
                node_id: metrics.id,
                current_term: metrics.current_term,
                current_leader: metrics.current_leader,
                last_log_index: metrics.last_log_index,
                committed: metrics.committed.map(log_progress_snapshot),
                last_applied: metrics.last_applied.map(log_progress_snapshot),
                snapshot: metrics.snapshot.map(log_progress_snapshot),
                purged: metrics.purged.map(log_progress_snapshot),
                voter_ids: membership.voter_ids().collect(),
                learner_ids: membership.learner_ids().collect(),
            });
        }
        snapshots
    }

    pub async fn append_entries(
        &self,
        raft_group_id: RaftGroupId,
        request: AppendEntriesRequest<UrsulaRaftTypeConfig>,
    ) -> Result<AppendEntriesResponse<UrsulaRaftTypeConfig>, GroupEngineError> {
        let raft = self.require_group(raft_group_id)?;
        raft.append_entries(request)
            .await
            .map_err(|err| GroupEngineError::new(format!("OpenRaft AppendEntries: {err}")))
    }

    pub async fn vote(
        &self,
        raft_group_id: RaftGroupId,
        request: VoteRequest<UrsulaRaftTypeConfig>,
    ) -> Result<VoteResponse<UrsulaRaftTypeConfig>, GroupEngineError> {
        let raft = self.require_group(raft_group_id)?;
        raft.vote(request)
            .await
            .map_err(|err| GroupEngineError::new(format!("OpenRaft Vote: {err}")))
    }

    pub async fn install_full_snapshot(
        &self,
        raft_group_id: RaftGroupId,
        vote: VoteOf<UrsulaRaftTypeConfig>,
        snapshot: TypeConfigSnapshotOf<UrsulaRaftTypeConfig>,
    ) -> Result<SnapshotResponse<UrsulaRaftTypeConfig>, GroupEngineError> {
        let raft = self.require_group(raft_group_id)?;
        raft.install_full_snapshot(vote, snapshot)
            .await
            .map_err(|err| GroupEngineError::new(format!("OpenRaft install snapshot: {err}")))
    }

    pub async fn build_snapshot_for_transfer(
        &self,
        raft_group_id: RaftGroupId,
    ) -> Result<TypeConfigSnapshotOf<UrsulaRaftTypeConfig>, GroupEngineError> {
        let raft = self.require_group(raft_group_id)?;
        let snapshot = raft
            .with_state_machine(|state_machine| {
                Box::pin(async move {
                    let mut builder = state_machine.get_snapshot_builder().await;
                    builder.build_snapshot().await
                })
            })
            .await
            .map_err(|err| GroupEngineError::new(format!("OpenRaft build snapshot: {err}")))?
            .map_err(|err| GroupEngineError::new(format!("build OpenRaft snapshot: {err}")))?;
        Ok(snapshot)
    }

    fn require_group(
        &self,
        raft_group_id: RaftGroupId,
    ) -> Result<Raft<UrsulaRaftTypeConfig, RaftGroupStateMachine>, GroupEngineError> {
        self.get(raft_group_id).ok_or_else(|| {
            GroupEngineError::new(format!(
                "raft group {} is not registered on this node",
                raft_group_id.0
            ))
        })
    }
}

fn log_progress_snapshot(log_id: LogIdOf<UrsulaRaftTypeConfig>) -> RaftLogProgressSnapshot {
    RaftLogProgressSnapshot {
        term: log_id.leader_id.term,
        index: log_id.index,
    }
}

#[derive(Debug, Clone)]
struct CurrentSnapshot {
    meta: SnapshotMetaOf<UrsulaRaftTypeConfig>,
    data: Vec<u8>,
}

pub struct RaftGroupStateMachine {
    placement: ShardPlacement,
    engine: InMemoryGroupEngine,
    metrics: Option<GroupEngineMetrics>,
    last_applied_log_id: Option<LogIdOf<UrsulaRaftTypeConfig>>,
    last_membership: StoredMembershipOf<UrsulaRaftTypeConfig>,
    current_snapshot: Arc<Mutex<Option<CurrentSnapshot>>>,
}

impl RaftGroupStateMachine {
    pub fn new(placement: ShardPlacement) -> Self {
        Self::new_with_metrics(placement, None)
    }

    fn new_with_metrics(placement: ShardPlacement, metrics: Option<GroupEngineMetrics>) -> Self {
        Self::new_with_metrics_and_cold_store(placement, metrics, None)
    }

    fn new_with_metrics_and_cold_store(
        placement: ShardPlacement,
        metrics: Option<GroupEngineMetrics>,
        cold_store: Option<ColdStoreHandle>,
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

    fn snapshot_meta(&self) -> SnapshotMetaOf<UrsulaRaftTypeConfig> {
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
            snapshot,
            meta: self.snapshot_meta(),
            current_snapshot: self.current_snapshot.clone(),
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
        let group_snapshot: GroupSnapshot =
            serde_json::from_slice(snapshot.get_ref()).map_err(invalid_data)?;
        self.engine
            .install_snapshot(group_snapshot)
            .await
            .map_err(group_engine_io_error)?;
        self.last_applied_log_id = meta.last_log_id;
        self.last_membership = meta.last_membership.clone();
        *self.current_snapshot.lock().expect("snapshot mutex") = Some(CurrentSnapshot {
            meta: meta.clone(),
            data: snapshot.into_inner(),
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
                snapshot: Cursor::new(snapshot.data.clone()),
            }))
    }
}

pub struct RaftGroupSnapshotBuilder {
    snapshot: GroupSnapshot,
    meta: SnapshotMetaOf<UrsulaRaftTypeConfig>,
    current_snapshot: Arc<Mutex<Option<CurrentSnapshot>>>,
}

impl RaftSnapshotBuilder<UrsulaRaftTypeConfig> for RaftGroupSnapshotBuilder {
    async fn build_snapshot(&mut self) -> Result<SnapshotOf<UrsulaRaftTypeConfig>, io::Error> {
        let data = serde_json::to_vec(&self.snapshot).map_err(invalid_data)?;
        *self.current_snapshot.lock().expect("snapshot mutex") = Some(CurrentSnapshot {
            meta: self.meta.clone(),
            data: data.clone(),
        });
        Ok(SnapshotOf::<UrsulaRaftTypeConfig> {
            meta: self.meta.clone(),
            snapshot: Cursor::new(data),
        })
    }
}

pub struct RaftGroupEngine {
    raft: Raft<UrsulaRaftTypeConfig, RaftGroupStateMachine>,
    placement: ShardPlacement,
    metrics: Option<GroupEngineMetrics>,
    cold_store: Option<ColdStoreHandle>,
}

impl RaftGroupEngine {
    pub async fn new_single_node(placement: ShardPlacement) -> Result<Self, GroupEngineError> {
        Self::new_single_node_with_optional_metrics(placement, None).await
    }

    async fn new_single_node_with_optional_metrics(
        placement: ShardPlacement,
        metrics: Option<GroupEngineMetrics>,
    ) -> Result<Self, GroupEngineError> {
        let config = Arc::new(
            Config {
                cluster_name: format!("ursula-group-{}", placement.raft_group_id.0),
                heartbeat_interval: 10,
                election_timeout_min: 30,
                election_timeout_max: 60,
                ..Default::default()
            }
            .validate()
            .map_err(|err| GroupEngineError::new(format!("invalid OpenRaft config: {err}")))?,
        );
        Self::new_single_node_with_config_and_metrics(
            placement,
            1,
            BasicNode::new("local"),
            config,
            metrics,
        )
        .await
    }

    pub async fn new_single_node_with_file_log(
        placement: ShardPlacement,
        log_path: impl Into<PathBuf>,
    ) -> Result<Self, GroupEngineError> {
        let config = Arc::new(
            Config {
                cluster_name: format!("ursula-group-{}", placement.raft_group_id.0),
                heartbeat_interval: 10,
                election_timeout_min: 30,
                election_timeout_max: 60,
                ..Default::default()
            }
            .validate()
            .map_err(|err| GroupEngineError::new(format!("invalid OpenRaft config: {err}")))?,
        );
        let log_store = RaftGroupFileLogStore::shared(log_path)
            .map_err(|err| GroupEngineError::new(format!("open OpenRaft file log: {err}")))?;
        Self::new_single_node_with_log_store(
            placement,
            1,
            BasicNode::new("local"),
            config,
            log_store,
        )
        .await
    }

    pub async fn new_single_node_with_config(
        placement: ShardPlacement,
        node_id: u64,
        node: BasicNode,
        config: Arc<Config>,
    ) -> Result<Self, GroupEngineError> {
        Self::new_single_node_with_config_and_metrics(placement, node_id, node, config, None).await
    }

    async fn new_single_node_with_config_and_metrics(
        placement: ShardPlacement,
        node_id: u64,
        node: BasicNode,
        config: Arc<Config>,
        metrics: Option<GroupEngineMetrics>,
    ) -> Result<Self, GroupEngineError> {
        Self::new_single_node_with_log_store_and_metrics(
            placement,
            node_id,
            node,
            config,
            RaftGroupLogStore::shared(),
            metrics,
            None,
        )
        .await
    }

    pub async fn new_single_node_with_log_store<LS>(
        placement: ShardPlacement,
        node_id: u64,
        node: BasicNode,
        config: Arc<Config>,
        log_store: LS,
    ) -> Result<Self, GroupEngineError>
    where
        LS: RaftLogStorage<UrsulaRaftTypeConfig>,
    {
        Self::new_single_node_with_log_store_and_metrics(
            placement, node_id, node, config, log_store, None, None,
        )
        .await
    }

    async fn new_single_node_with_log_store_and_metrics<LS>(
        placement: ShardPlacement,
        node_id: u64,
        node: BasicNode,
        config: Arc<Config>,
        log_store: LS,
        metrics: Option<GroupEngineMetrics>,
        cold_store: Option<ColdStoreHandle>,
    ) -> Result<Self, GroupEngineError>
    where
        LS: RaftLogStorage<UrsulaRaftTypeConfig>,
    {
        let engine = Self::new_node_with_log_store_and_network(
            placement,
            node_id,
            config,
            SingleNodeRaftNetworkFactory,
            log_store,
            metrics,
            cold_store,
        )
        .await?;

        let initialized = engine.raft.is_initialized().await.map_err(|err| {
            GroupEngineError::new(format!("check OpenRaft initialization: {err}"))
        })?;
        if !initialized {
            let mut nodes = BTreeMap::new();
            nodes.insert(node_id, node);
            engine.raft.initialize(nodes).await.map_err(|err| {
                GroupEngineError::new(format!("initialize OpenRaft group: {err}"))
            })?;
        }
        engine
            .raft
            .wait(Some(Duration::from_secs(2)))
            .current_leader(node_id, "single-node OpenRaft group should elect itself")
            .await
            .map_err(|err| GroupEngineError::new(format!("wait for OpenRaft leadership: {err}")))?;

        Ok(engine)
    }

    pub async fn new_node_with_log_store_and_network<NF, LS>(
        placement: ShardPlacement,
        node_id: u64,
        config: Arc<Config>,
        network_factory: NF,
        log_store: LS,
        metrics: Option<GroupEngineMetrics>,
        cold_store: Option<ColdStoreHandle>,
    ) -> Result<Self, GroupEngineError>
    where
        NF: RaftNetworkFactory<UrsulaRaftTypeConfig>,
        LS: RaftLogStorage<UrsulaRaftTypeConfig>,
    {
        let raft = Raft::<UrsulaRaftTypeConfig, RaftGroupStateMachine>::new(
            node_id,
            config,
            network_factory,
            log_store,
            RaftGroupStateMachine::new_with_metrics_and_cold_store(
                placement,
                metrics.clone(),
                cold_store.clone(),
            ),
        )
        .await
        .map_err(|err| GroupEngineError::new(format!("create OpenRaft group: {err}")))?;

        Ok(Self {
            raft,
            placement,
            metrics,
            cold_store,
        })
    }

    pub async fn initialize_membership(
        &self,
        nodes: BTreeMap<u64, BasicNode>,
    ) -> Result<(), GroupEngineError> {
        let initialized = self.raft.is_initialized().await.map_err(|err| {
            GroupEngineError::new(format!("check OpenRaft initialization: {err}"))
        })?;
        if initialized {
            return Ok(());
        }
        self.raft
            .initialize(nodes)
            .await
            .map_err(|err| GroupEngineError::new(format!("initialize OpenRaft group: {err}")))
    }

    pub async fn wait_for_current_leader(
        &self,
        node_id: u64,
        timeout: Duration,
    ) -> Result<(), GroupEngineError> {
        self.raft
            .wait(Some(timeout))
            .current_leader(node_id, "OpenRaft group should observe expected leader")
            .await
            .map(|_| ())
            .map_err(|err| GroupEngineError::new(format!("wait for OpenRaft leadership: {err}")))
    }

    pub fn raft_handle(&self) -> Raft<UrsulaRaftTypeConfig, RaftGroupStateMachine> {
        self.raft.clone()
    }

    pub async fn shutdown(&self) -> Result<(), GroupEngineError> {
        self.raft
            .shutdown()
            .await
            .map_err(|err| GroupEngineError::new(format!("shutdown OpenRaft group: {err}")))
    }

    async fn write(
        &self,
        command: GroupWriteCommand,
    ) -> Result<GroupWriteResponse, GroupEngineError> {
        let response = match self.raft.client_write(command.into()).await {
            Ok(response) => response,
            Err(err) => return Err(group_engine_client_write_error(err)),
        };
        group_write_result_from_raft_response(response.data)?
    }

    async fn write_commands(
        &self,
        commands: Vec<GroupWriteCommand>,
    ) -> Result<Vec<Result<GroupWriteResponse, GroupEngineError>>, GroupEngineError> {
        write_commands_on_raft(
            self.raft.clone(),
            self.placement,
            self.metrics.clone(),
            commands,
        )
        .await
    }

    async fn forward_write_to_leader_if_follower(
        &self,
        _command: GroupWriteCommand,
    ) -> Result<Option<GroupWriteResponse>, GroupEngineError> {
        if self.raft.is_leader() {
            return Ok(None);
        }
        let leader_id = self.raft.current_leader().await;
        let leader_node = self.current_leader_node().await;
        Err(group_engine_forward_to_leader_error(
            "OpenRaft group write has to run on the local leader runtime",
            leader_id,
            leader_node.as_ref(),
        ))
    }

    async fn with_state_machine<V>(
        &self,
        f: impl FnOnce(&mut RaftGroupStateMachine) -> openraft::base::BoxFuture<V>
        + OptionalSend
        + 'static,
    ) -> Result<V, GroupEngineError>
    where
        V: OptionalSend + 'static,
    {
        self.raft
            .with_state_machine(f)
            .await
            .map_err(|err| GroupEngineError::new(format!("OpenRaft state-machine access: {err}")))
    }

    async fn access_requires_write(
        &self,
        stream_id: BucketStreamId,
        now_ms: u64,
        renew_ttl: bool,
    ) -> Result<bool, GroupEngineError> {
        self.with_state_machine(move |state_machine| {
            Box::pin(async move {
                state_machine
                    .access_requires_write(&stream_id, now_ms, renew_ttl)
                    .await
            })
        })
        .await?
    }

    async fn ensure_stream_access(
        &self,
        stream_id: BucketStreamId,
        now_ms: u64,
        renew_ttl: bool,
    ) -> Result<Option<TouchStreamAccessResponse>, GroupEngineError> {
        if !self
            .access_requires_write(stream_id.clone(), now_ms, renew_ttl)
            .await?
        {
            return Ok(None);
        }
        let response = match self
            .write(GroupWriteCommand::TouchStreamAccess {
                stream_id: stream_id.clone(),
                now_ms,
                renew_ttl,
            })
            .await?
        {
            GroupWriteResponse::TouchStreamAccess(response) => response,
            other => {
                return Err(GroupEngineError::new(format!(
                    "unexpected touch stream access write response: {other:?}"
                )));
            }
        };
        if response.expired {
            return Err(GroupEngineError::stream(
                StreamErrorCode::StreamNotFound,
                format!("stream '{stream_id}' does not exist"),
            ));
        }
        Ok(Some(response))
    }

    async fn require_local_leader_for_read(&self, operation: &str) -> Result<(), GroupEngineError> {
        if self.raft.is_leader() {
            return Ok(());
        }
        Err(group_engine_forward_to_leader_error(
            format!("OpenRaft {operation} has to forward request to leader"),
            self.raft.current_leader().await,
            None,
        ))
    }

    async fn current_leader_node(&self) -> Option<BasicNode> {
        let leader_id = self.raft.current_leader().await?;
        self.raft
            .metrics()
            .borrow_watched()
            .membership_config
            .membership()
            .get_node(&leader_id)
            .cloned()
    }
}

async fn forward_head_stream_to_leader(
    placement: ShardPlacement,
    leader_node: &BasicNode,
    request: HeadStreamRequest,
) -> Result<HeadStreamResponse, GroupEngineError> {
    let response = forward_group_read_to_leader(
        placement,
        leader_node,
        request.stream_id,
        request.now_ms,
        raft_internal_proto::group_read_request_v1::Read::Head(
            raft_internal_proto::HeadStreamReadV1 {},
        ),
    )
    .await?;
    if response.ok {
        let response =
            raft_internal_proto::HeadStreamResponsePayloadV1::decode(response.payload.as_slice())
                .map_err(|err| {
                GroupEngineError::new(format!("decode forwarded head response: {err}"))
            })?;
        head_stream_response_from_proto(response)
    } else {
        let err = raft_app_proto::GroupEngineErrorV1::decode(response.payload.as_slice())
            .map_err(|err| GroupEngineError::new(format!("decode forwarded head error: {err}")))?;
        Err(group_engine_error_from_proto(err)?)
    }
}

async fn forward_read_stream_to_leader(
    placement: ShardPlacement,
    leader_node: &BasicNode,
    request: ReadStreamRequest,
) -> Result<ReadStreamResponse, GroupEngineError> {
    let max_len = u64::try_from(request.max_len)
        .map_err(|_| GroupEngineError::new("read max_len does not fit u64"))?;
    let response = forward_group_read_to_leader(
        placement,
        leader_node,
        request.stream_id,
        request.now_ms,
        raft_internal_proto::group_read_request_v1::Read::ReadStream(
            raft_internal_proto::ReadStreamReadV1 {
                offset: request.offset,
                max_len,
            },
        ),
    )
    .await?;
    if response.ok {
        let response =
            raft_internal_proto::ReadStreamResponsePayloadV1::decode(response.payload.as_slice())
                .map_err(|err| {
                GroupEngineError::new(format!("decode forwarded read response: {err}"))
            })?;
        read_stream_response_from_proto(response)
    } else {
        let err = raft_app_proto::GroupEngineErrorV1::decode(response.payload.as_slice())
            .map_err(|err| GroupEngineError::new(format!("decode forwarded read error: {err}")))?;
        Err(group_engine_error_from_proto(err)?)
    }
}

async fn forward_group_read_to_leader(
    placement: ShardPlacement,
    leader_node: &BasicNode,
    stream_id: BucketStreamId,
    now_ms: u64,
    read: raft_internal_proto::group_read_request_v1::Read,
) -> Result<raft_internal_proto::GroupReadResponseV1, GroupEngineError> {
    let channel = grpc_leader_channel(&leader_node.addr).await?;
    let mut client = raft_internal_proto::raft_internal_client::RaftInternalClient::new(channel)
        .max_decoding_message_size(RAFT_GRPC_MAX_MESSAGE_BYTES)
        .max_encoding_message_size(RAFT_GRPC_MAX_MESSAGE_BYTES);
    client
        .group_read(raft_internal_proto::GroupReadRequestV1 {
            raft_group_id: placement.raft_group_id.0,
            core_id: u32::from(placement.core_id.0),
            shard_id: placement.shard_id.0,
            bucket_id: stream_id.bucket_id,
            stream_id: stream_id.stream_id,
            now_ms,
            read: Some(read),
        })
        .await
        .map(|response| response.into_inner())
        .map_err(|err| GroupEngineError::new(format!("forward group read to leader: {err}")))
}

async fn grpc_leader_channel(addr: &str) -> Result<Channel, GroupEngineError> {
    let cache = GRPC_LEADER_CHANNELS.get_or_init(|| Mutex::new(BTreeMap::new()));
    if let Some(channel) = cache
        .lock()
        .map_err(|_| GroupEngineError::new("gRPC leader channel cache mutex poisoned"))?
        .get(addr)
        .cloned()
    {
        return Ok(channel);
    }
    let endpoint = Endpoint::from_shared(addr.to_owned())
        .map_err(|err| GroupEngineError::new(format!("invalid gRPC leader endpoint: {err}")))?;
    let channel = endpoint
        .connect()
        .await
        .map_err(|err| GroupEngineError::new(format!("connect gRPC leader: {err}")))?;
    cache
        .lock()
        .map_err(|_| GroupEngineError::new("gRPC leader channel cache mutex poisoned"))?
        .insert(addr.to_owned(), channel.clone());
    Ok(channel)
}

async fn write_commands_on_raft(
    raft: Raft<UrsulaRaftTypeConfig, RaftGroupStateMachine>,
    placement: ShardPlacement,
    metrics: Option<GroupEngineMetrics>,
    commands: Vec<GroupWriteCommand>,
) -> Result<Vec<Result<GroupWriteResponse, GroupEngineError>>, GroupEngineError> {
    if commands.is_empty() {
        return Ok(Vec::new());
    }
    let expected_responses = commands.len();
    let logical_command_count = commands
        .iter()
        .map(logical_group_write_command_count)
        .sum::<usize>();
    let submit_started_at = Instant::now();
    let commands = commands.into_iter().map(Into::into).collect::<Vec<_>>();
    let mut stream = match raft.client_write_many(commands).await {
        Ok(stream) => stream,
        Err(err) => {
            if let Some(metrics) = &metrics {
                metrics.record_raft_write_many(
                    placement,
                    expected_responses,
                    logical_command_count,
                    0,
                    elapsed_ns(submit_started_at),
                    0,
                );
            }
            return Err(GroupEngineError::new(format!(
                "OpenRaft client_write_many: {err}"
            )));
        }
    };
    let submit_ns = elapsed_ns(submit_started_at);
    let response_started_at = Instant::now();
    let mut responses = Vec::with_capacity(expected_responses);
    loop {
        let result = match stream.try_next().await {
            Ok(Some(result)) => result,
            Ok(None) => break,
            Err(err) => {
                if let Some(metrics) = &metrics {
                    metrics.record_raft_write_many(
                        placement,
                        expected_responses,
                        logical_command_count,
                        responses.len(),
                        submit_ns,
                        elapsed_ns(response_started_at),
                    );
                }
                return Err(GroupEngineError::new(format!(
                    "OpenRaft client_write_many response stream: {err}"
                )));
            }
        };
        let response = match result {
            Ok(response) => group_write_result_from_raft_response(response.response)?,
            Err(err) => Err(group_engine_forward_to_leader_error(
                format!("OpenRaft client_write_many forwarded to leader: {err}"),
                err.leader_id,
                err.leader_node.as_ref(),
            )),
        };
        responses.push(response);
    }
    if let Some(metrics) = &metrics {
        metrics.record_raft_write_many(
            placement,
            expected_responses,
            logical_command_count,
            responses.len(),
            submit_ns,
            elapsed_ns(response_started_at),
        );
    }
    if responses.len() != expected_responses {
        return Err(GroupEngineError::new(format!(
            "OpenRaft client_write_many returned {} responses for {} commands",
            responses.len(),
            expected_responses
        )));
    }
    Ok(responses)
}

fn logical_group_write_command_count(command: &GroupWriteCommand) -> usize {
    match command {
        GroupWriteCommand::Batch { commands } => {
            commands.iter().map(logical_group_write_command_count).sum()
        }
        _ => 1,
    }
}

fn group_engine_client_write_error(
    err: openraft::error::RaftError<
        UrsulaRaftTypeConfig,
        openraft::error::ClientWriteError<UrsulaRaftTypeConfig>,
    >,
) -> GroupEngineError {
    if let Some(forward) = err.forward_to_leader() {
        return group_engine_forward_to_leader_error(
            format!("OpenRaft client_write forwarded to leader: {err}"),
            forward.leader_id,
            forward.leader_node.as_ref(),
        );
    }
    GroupEngineError::new(format!("OpenRaft client_write: {err}"))
}

fn group_engine_forward_to_leader_error(
    message: impl Into<String>,
    leader_id: Option<u64>,
    leader_node: Option<&BasicNode>,
) -> GroupEngineError {
    GroupEngineError::forward_to_leader(
        message,
        leader_id,
        leader_node.map(|node| node.addr.clone()),
    )
}

#[derive(Debug, Clone, Copy, Default)]
pub struct RaftGroupEngineFactory;

impl GroupEngineFactory for RaftGroupEngineFactory {
    fn create<'a>(
        &'a self,
        placement: ShardPlacement,
        metrics: GroupEngineMetrics,
    ) -> GroupEngineCreateFuture<'a> {
        Box::pin(async move {
            let engine: Box<dyn GroupEngine> = Box::new(
                RaftGroupEngine::new_single_node_with_optional_metrics(placement, Some(metrics))
                    .await?,
            );
            Ok(engine)
        })
    }
}

#[derive(Debug, Clone)]
pub struct RegisteredRaftGroupEngineFactory {
    registry: RaftGroupHandleRegistry,
}

impl RegisteredRaftGroupEngineFactory {
    pub fn new(registry: RaftGroupHandleRegistry) -> Self {
        Self { registry }
    }

    pub fn registry(&self) -> &RaftGroupHandleRegistry {
        &self.registry
    }
}

impl GroupEngineFactory for RegisteredRaftGroupEngineFactory {
    fn create<'a>(
        &'a self,
        placement: ShardPlacement,
        metrics: GroupEngineMetrics,
    ) -> GroupEngineCreateFuture<'a> {
        Box::pin(async move {
            let engine =
                RaftGroupEngine::new_single_node_with_optional_metrics(placement, Some(metrics))
                    .await?;
            self.registry.register(placement, engine.raft.clone());
            let engine: Box<dyn GroupEngine> = Box::new(engine);
            Ok(engine)
        })
    }
}

#[derive(Debug, Clone)]
pub struct ColdRaftGroupEngineFactory {
    cold_store: ColdStoreHandle,
}

impl ColdRaftGroupEngineFactory {
    pub fn new(cold_store: ColdStoreHandle) -> Self {
        Self { cold_store }
    }
}

impl GroupEngineFactory for ColdRaftGroupEngineFactory {
    fn create<'a>(
        &'a self,
        placement: ShardPlacement,
        metrics: GroupEngineMetrics,
    ) -> GroupEngineCreateFuture<'a> {
        Box::pin(async move {
            let config = Arc::new(
                Config {
                    cluster_name: format!("ursula-group-{}", placement.raft_group_id.0),
                    heartbeat_interval: 10,
                    election_timeout_min: 30,
                    election_timeout_max: 60,
                    ..Default::default()
                }
                .validate()
                .map_err(|err| GroupEngineError::new(format!("invalid OpenRaft config: {err}")))?,
            );
            let engine: Box<dyn GroupEngine> = Box::new(
                RaftGroupEngine::new_single_node_with_log_store_and_metrics(
                    placement,
                    1,
                    BasicNode::new("local"),
                    config,
                    RaftGroupLogStore::shared(),
                    Some(metrics),
                    Some(self.cold_store.clone()),
                )
                .await?,
            );
            Ok(engine)
        })
    }
}

#[derive(Debug, Clone)]
pub struct DurableRaftLogStoreFactory {
    root: PathBuf,
    core_writers: Arc<Mutex<BTreeMap<u16, Arc<CoreFileLogWriter>>>>,
}

impl DurableRaftLogStoreFactory {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            root: root.into(),
            core_writers: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    fn log_path(&self, placement: ShardPlacement) -> PathBuf {
        self.root
            .join(format!("core-{}", placement.core_id.0))
            .join(format!("group-{}.json", placement.raft_group_id.0))
    }

    fn core_journal_path(&self, core_id: CoreId) -> PathBuf {
        self.root
            .join(format!("core-{}", core_id.0))
            .join("journal.bin")
    }

    fn core_writer(&self, core_id: CoreId) -> Result<Arc<CoreFileLogWriter>, GroupEngineError> {
        let mut writers = self
            .core_writers
            .lock()
            .map_err(|_| GroupEngineError::new("core file log writer mutex poisoned"))?;
        if let Some(writer) = writers.get(&core_id.0) {
            return Ok(writer.clone());
        }

        let writer = CoreFileLogWriter::shared(self.core_journal_path(core_id))
            .map_err(|err| GroupEngineError::new(format!("open OpenRaft core journal: {err}")))?;
        writers.insert(core_id.0, writer.clone());
        Ok(writer)
    }

    pub fn open(
        &self,
        placement: ShardPlacement,
        metrics: GroupEngineMetrics,
    ) -> Result<Arc<RaftGroupFileLogStore>, GroupEngineError> {
        RaftGroupFileLogStore::shared_with_core_writer(
            self.log_path(placement),
            placement,
            metrics,
            self.core_writer(placement.core_id)?,
        )
        .map_err(|err| GroupEngineError::new(format!("open OpenRaft file log: {err}")))
    }
}

#[derive(Debug, Clone)]
pub struct DurableRaftGroupEngineFactory {
    log_stores: DurableRaftLogStoreFactory,
    cold_store: Option<ColdStoreHandle>,
}

impl DurableRaftGroupEngineFactory {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            log_stores: DurableRaftLogStoreFactory::new(root),
            cold_store: None,
        }
    }

    pub fn with_cold_store(root: impl Into<PathBuf>, cold_store: Option<ColdStoreHandle>) -> Self {
        Self {
            log_stores: DurableRaftLogStoreFactory::new(root),
            cold_store,
        }
    }
}

impl GroupEngineFactory for DurableRaftGroupEngineFactory {
    fn create<'a>(
        &'a self,
        placement: ShardPlacement,
        metrics: GroupEngineMetrics,
    ) -> GroupEngineCreateFuture<'a> {
        Box::pin(async move {
            let config = Arc::new(
                Config {
                    cluster_name: format!("ursula-group-{}", placement.raft_group_id.0),
                    heartbeat_interval: 10,
                    election_timeout_min: 30,
                    election_timeout_max: 60,
                    ..Default::default()
                }
                .validate()
                .map_err(|err| GroupEngineError::new(format!("invalid OpenRaft config: {err}")))?,
            );
            let log_store = self.log_stores.open(placement, metrics.clone())?;
            let engine: Box<dyn GroupEngine> = Box::new(
                RaftGroupEngine::new_single_node_with_log_store_and_metrics(
                    placement,
                    1,
                    BasicNode::new("local"),
                    config,
                    log_store,
                    Some(metrics),
                    self.cold_store.clone(),
                )
                .await?,
            );
            Ok(engine)
        })
    }
}

impl GroupEngine for RaftGroupEngine {
    fn accepts_local_writes(&self) -> bool {
        self.raft.is_leader()
    }

    fn create_stream<'a>(
        &'a mut self,
        request: CreateStreamRequest,
        _placement: ShardPlacement,
    ) -> GroupCreateStreamFuture<'a> {
        Box::pin(async move {
            match self.write(GroupWriteCommand::from(request)).await? {
                GroupWriteResponse::CreateStream(response) => Ok(response),
                other => Err(GroupEngineError::new(format!(
                    "unexpected create stream write response: {other:?}"
                ))),
            }
        })
    }

    fn create_stream_with_cold_admission<'a>(
        &'a mut self,
        request: CreateStreamRequest,
        placement: ShardPlacement,
        admission: ColdWriteAdmission,
    ) -> GroupCreateStreamFuture<'a> {
        if admission.max_hot_bytes_per_group.is_none() {
            return self.create_stream(request, placement);
        }
        Box::pin(async move {
            let command = GroupWriteCommand::from(request.clone());
            if let Some(response) = self.forward_write_to_leader_if_follower(command).await? {
                return match response {
                    GroupWriteResponse::CreateStream(response) => Ok(response),
                    other => Err(GroupEngineError::new(format!(
                        "unexpected create stream write response: {other:?}"
                    ))),
                };
            }
            self.with_state_machine({
                let request = request.clone();
                move |state_machine| {
                    Box::pin(async move {
                        state_machine
                            .check_create_stream_cold_admission(request, placement, admission)
                            .await
                    })
                }
            })
            .await??;
            match self.write(GroupWriteCommand::from(request)).await? {
                GroupWriteResponse::CreateStream(response) => Ok(response),
                other => Err(GroupEngineError::new(format!(
                    "unexpected create stream write response: {other:?}"
                ))),
            }
        })
    }

    fn create_stream_external<'a>(
        &'a mut self,
        request: CreateStreamExternalRequest,
        _placement: ShardPlacement,
    ) -> GroupCreateStreamFuture<'a> {
        Box::pin(async move {
            match self.write(GroupWriteCommand::from(request)).await? {
                GroupWriteResponse::CreateStream(response) => Ok(response),
                other => Err(GroupEngineError::new(format!(
                    "unexpected external create stream write response: {other:?}"
                ))),
            }
        })
    }

    fn head_stream<'a>(
        &'a mut self,
        request: HeadStreamRequest,
        placement: ShardPlacement,
    ) -> GroupHeadStreamFuture<'a> {
        Box::pin(async move {
            if !self.raft.is_leader()
                && let Some(leader_node) = self.current_leader_node().await
            {
                return forward_head_stream_to_leader(placement, &leader_node, request).await;
            }
            self.require_local_leader_for_read("head_stream").await?;
            self.ensure_stream_access(request.stream_id.clone(), request.now_ms, false)
                .await?;
            self.with_state_machine(move |state_machine| {
                Box::pin(async move { state_machine.head_stream(request, placement).await })
            })
            .await?
        })
    }

    fn read_stream<'a>(
        &'a mut self,
        request: ReadStreamRequest,
        placement: ShardPlacement,
    ) -> GroupReadStreamFuture<'a> {
        Box::pin(async move {
            self.read_stream_parts(request, placement)
                .await?
                .into_response()
                .await
        })
    }

    fn read_stream_parts<'a>(
        &'a mut self,
        request: ReadStreamRequest,
        placement: ShardPlacement,
    ) -> GroupReadStreamPartsFuture<'a> {
        Box::pin(async move {
            if !self.raft.is_leader()
                && let Some(leader_node) = self.current_leader_node().await
            {
                let response =
                    forward_read_stream_to_leader(placement, &leader_node, request).await?;
                return Ok(GroupReadStreamParts::from_response(response));
            }
            let stream_id = request.stream_id.clone();
            self.require_local_leader_for_read("read_stream").await?;
            self.ensure_stream_access(request.stream_id.clone(), request.now_ms, true)
                .await?;
            let plan = self
                .with_state_machine(move |state_machine| {
                    Box::pin(
                        async move { state_machine.engine.read_stream_plan_after_access(&request) },
                    )
                })
                .await??;
            Ok(GroupReadStreamParts::from_plan(
                placement,
                stream_id,
                plan,
                self.cold_store.clone(),
            ))
        })
    }

    fn require_local_live_read_owner<'a>(
        &'a mut self,
        _placement: ShardPlacement,
    ) -> ursula_runtime::GroupRequireLiveReadOwnerFuture<'a> {
        Box::pin(async move { self.require_local_leader_for_read("live_read").await })
    }

    fn publish_snapshot<'a>(
        &'a mut self,
        request: PublishSnapshotRequest,
        _placement: ShardPlacement,
    ) -> GroupPublishSnapshotFuture<'a> {
        Box::pin(async move {
            let command = GroupWriteCommand::from(request.clone());
            if let Some(response) = self.forward_write_to_leader_if_follower(command).await? {
                return match response {
                    GroupWriteResponse::PublishSnapshot(response) => Ok(response),
                    other => Err(GroupEngineError::new(format!(
                        "unexpected publish snapshot write response: {other:?}"
                    ))),
                };
            }
            self.ensure_stream_access(request.stream_id.clone(), request.now_ms, false)
                .await?;
            match self.write(GroupWriteCommand::from(request)).await? {
                GroupWriteResponse::PublishSnapshot(response) => Ok(response),
                other => Err(GroupEngineError::new(format!(
                    "unexpected publish snapshot write response: {other:?}"
                ))),
            }
        })
    }

    fn read_snapshot<'a>(
        &'a mut self,
        request: ReadSnapshotRequest,
        placement: ShardPlacement,
    ) -> GroupReadSnapshotFuture<'a> {
        Box::pin(async move {
            self.require_local_leader_for_read("read_snapshot").await?;
            self.ensure_stream_access(request.stream_id.clone(), request.now_ms, true)
                .await?;
            self.with_state_machine(move |state_machine| {
                Box::pin(async move { state_machine.read_snapshot(request, placement).await })
            })
            .await?
        })
    }

    fn delete_snapshot<'a>(
        &'a mut self,
        request: DeleteSnapshotRequest,
        placement: ShardPlacement,
    ) -> GroupDeleteSnapshotFuture<'a> {
        Box::pin(async move {
            self.ensure_stream_access(request.stream_id.clone(), request.now_ms, false)
                .await?;
            self.with_state_machine(move |state_machine| {
                Box::pin(async move { state_machine.delete_snapshot(request, placement).await })
            })
            .await?
        })
    }

    fn bootstrap_stream<'a>(
        &'a mut self,
        request: BootstrapStreamRequest,
        placement: ShardPlacement,
    ) -> GroupBootstrapStreamFuture<'a> {
        Box::pin(async move {
            self.require_local_leader_for_read("bootstrap_stream")
                .await?;
            self.ensure_stream_access(request.stream_id.clone(), request.now_ms, true)
                .await?;
            self.with_state_machine(move |state_machine| {
                Box::pin(async move { state_machine.bootstrap_stream(request, placement).await })
            })
            .await?
        })
    }

    fn touch_stream_access<'a>(
        &'a mut self,
        stream_id: BucketStreamId,
        now_ms: u64,
        renew_ttl: bool,
        _placement: ShardPlacement,
    ) -> GroupTouchStreamAccessFuture<'a> {
        Box::pin(async move {
            match self
                .write(GroupWriteCommand::TouchStreamAccess {
                    stream_id,
                    now_ms,
                    renew_ttl,
                })
                .await?
            {
                GroupWriteResponse::TouchStreamAccess(response) => Ok(response),
                other => Err(GroupEngineError::new(format!(
                    "unexpected touch stream access write response: {other:?}"
                ))),
            }
        })
    }

    fn add_fork_ref<'a>(
        &'a mut self,
        stream_id: BucketStreamId,
        now_ms: u64,
        _placement: ShardPlacement,
    ) -> GroupForkRefFuture<'a> {
        Box::pin(async move {
            match self
                .write(GroupWriteCommand::AddForkRef { stream_id, now_ms })
                .await?
            {
                GroupWriteResponse::AddForkRef(response) => Ok(response),
                other => Err(GroupEngineError::new(format!(
                    "unexpected add fork ref write response: {other:?}"
                ))),
            }
        })
    }

    fn release_fork_ref<'a>(
        &'a mut self,
        stream_id: BucketStreamId,
        _placement: ShardPlacement,
    ) -> GroupForkRefFuture<'a> {
        Box::pin(async move {
            match self
                .write(GroupWriteCommand::ReleaseForkRef { stream_id })
                .await?
            {
                GroupWriteResponse::ReleaseForkRef(response) => Ok(response),
                other => Err(GroupEngineError::new(format!(
                    "unexpected release fork ref write response: {other:?}"
                ))),
            }
        })
    }

    fn plan_cold_flush<'a>(
        &'a mut self,
        request: PlanColdFlushRequest,
        placement: ShardPlacement,
    ) -> GroupPlanColdFlushFuture<'a> {
        Box::pin(async move {
            self.with_state_machine(move |state_machine| {
                Box::pin(async move { state_machine.plan_cold_flush(request, placement).await })
            })
            .await?
        })
    }

    fn plan_next_cold_flush<'a>(
        &'a mut self,
        request: PlanGroupColdFlushRequest,
        placement: ShardPlacement,
    ) -> GroupPlanNextColdFlushFuture<'a> {
        Box::pin(async move {
            self.with_state_machine(move |state_machine| {
                Box::pin(
                    async move { state_machine.plan_next_cold_flush(request, placement).await },
                )
            })
            .await?
        })
    }

    fn plan_next_cold_flush_batch<'a>(
        &'a mut self,
        request: PlanGroupColdFlushRequest,
        placement: ShardPlacement,
        max_candidates: usize,
    ) -> GroupPlanNextColdFlushBatchFuture<'a> {
        Box::pin(async move {
            self.with_state_machine(move |state_machine| {
                Box::pin(async move {
                    state_machine
                        .plan_next_cold_flush_batch(request, placement, max_candidates)
                        .await
                })
            })
            .await?
        })
    }

    fn cold_hot_backlog<'a>(
        &'a mut self,
        stream_id: BucketStreamId,
        placement: ShardPlacement,
    ) -> GroupColdHotBacklogFuture<'a> {
        Box::pin(async move {
            self.with_state_machine(move |state_machine| {
                Box::pin(async move { state_machine.cold_hot_backlog(stream_id, placement).await })
            })
            .await?
        })
    }

    fn close_stream<'a>(
        &'a mut self,
        request: CloseStreamRequest,
        _placement: ShardPlacement,
    ) -> GroupCloseStreamFuture<'a> {
        Box::pin(async move {
            let command = GroupWriteCommand::from(request.clone());
            if let Some(response) = self.forward_write_to_leader_if_follower(command).await? {
                return match response {
                    GroupWriteResponse::CloseStream(response) => Ok(response),
                    other => Err(GroupEngineError::new(format!(
                        "unexpected close stream write response: {other:?}"
                    ))),
                };
            }
            self.ensure_stream_access(request.stream_id.clone(), request.now_ms, false)
                .await?;
            match self.write(GroupWriteCommand::from(request)).await? {
                GroupWriteResponse::CloseStream(response) => Ok(response),
                other => Err(GroupEngineError::new(format!(
                    "unexpected close stream write response: {other:?}"
                ))),
            }
        })
    }

    fn delete_stream<'a>(
        &'a mut self,
        request: DeleteStreamRequest,
        _placement: ShardPlacement,
    ) -> GroupDeleteStreamFuture<'a> {
        Box::pin(async move {
            match self.write(GroupWriteCommand::from(request)).await? {
                GroupWriteResponse::DeleteStream(response) => Ok(response),
                other => Err(GroupEngineError::new(format!(
                    "unexpected delete stream write response: {other:?}"
                ))),
            }
        })
    }

    fn append<'a>(
        &'a mut self,
        request: AppendRequest,
        _placement: ShardPlacement,
    ) -> GroupAppendFuture<'a> {
        Box::pin(async move {
            let command = GroupWriteCommand::from(request.clone());
            if let Some(response) = self.forward_write_to_leader_if_follower(command).await? {
                return match response {
                    GroupWriteResponse::Append(response) => Ok(response),
                    other => Err(GroupEngineError::new(format!(
                        "unexpected append write response: {other:?}"
                    ))),
                };
            }
            self.ensure_stream_access(request.stream_id.clone(), request.now_ms, false)
                .await?;
            match self.write(GroupWriteCommand::from(request)).await? {
                GroupWriteResponse::Append(response) => Ok(response),
                other => Err(GroupEngineError::new(format!(
                    "unexpected append write response: {other:?}"
                ))),
            }
        })
    }

    fn append_external<'a>(
        &'a mut self,
        request: AppendExternalRequest,
        _placement: ShardPlacement,
    ) -> GroupAppendFuture<'a> {
        Box::pin(async move {
            let command = GroupWriteCommand::from(request.clone());
            if let Some(response) = self.forward_write_to_leader_if_follower(command).await? {
                return match response {
                    GroupWriteResponse::Append(response) => Ok(response),
                    other => Err(GroupEngineError::new(format!(
                        "unexpected external append write response: {other:?}"
                    ))),
                };
            }
            self.ensure_stream_access(request.stream_id.clone(), request.now_ms, false)
                .await?;
            match self.write(GroupWriteCommand::from(request)).await? {
                GroupWriteResponse::Append(response) => Ok(response),
                other => Err(GroupEngineError::new(format!(
                    "unexpected external append write response: {other:?}"
                ))),
            }
        })
    }

    fn append_with_cold_admission<'a>(
        &'a mut self,
        request: AppendRequest,
        placement: ShardPlacement,
        admission: ColdWriteAdmission,
    ) -> GroupAppendFuture<'a> {
        if admission.max_hot_bytes_per_group.is_none() {
            return self.append(request, placement);
        }
        Box::pin(async move {
            let command = GroupWriteCommand::from(request.clone());
            if let Some(response) = self.forward_write_to_leader_if_follower(command).await? {
                return match response {
                    GroupWriteResponse::Append(response) => Ok(response),
                    other => Err(GroupEngineError::new(format!(
                        "unexpected append write response: {other:?}"
                    ))),
                };
            }
            self.ensure_stream_access(request.stream_id.clone(), request.now_ms, false)
                .await?;
            self.with_state_machine({
                let request = request.clone();
                move |state_machine| {
                    Box::pin(async move {
                        state_machine
                            .check_append_cold_admission(request, placement, admission)
                            .await
                    })
                }
            })
            .await??;
            match self.write(GroupWriteCommand::from(request)).await? {
                GroupWriteResponse::Append(response) => Ok(response),
                other => Err(GroupEngineError::new(format!(
                    "unexpected append write response: {other:?}"
                ))),
            }
        })
    }

    fn append_batch<'a>(
        &'a mut self,
        request: AppendBatchRequest,
        _placement: ShardPlacement,
    ) -> GroupAppendBatchFuture<'a> {
        Box::pin(async move {
            let command = GroupWriteCommand::from(request.clone());
            if let Some(response) = self.forward_write_to_leader_if_follower(command).await? {
                return match response {
                    GroupWriteResponse::AppendBatch(response) => Ok(response),
                    other => Err(GroupEngineError::new(format!(
                        "unexpected append batch write response: {other:?}"
                    ))),
                };
            }
            self.ensure_stream_access(request.stream_id.clone(), request.now_ms, false)
                .await?;
            let mut responses = self
                .write_commands(vec![GroupWriteCommand::from(request)])
                .await?;
            let response = responses.pop().ok_or_else(|| {
                GroupEngineError::new("OpenRaft append batch returned no response")
            })?;
            match response? {
                GroupWriteResponse::AppendBatch(response) => Ok(response),
                other => Err(GroupEngineError::new(format!(
                    "unexpected append batch write response: {other:?}"
                ))),
            }
        })
    }

    fn append_batch_with_cold_admission<'a>(
        &'a mut self,
        request: AppendBatchRequest,
        placement: ShardPlacement,
        admission: ColdWriteAdmission,
    ) -> GroupAppendBatchFuture<'a> {
        if admission.max_hot_bytes_per_group.is_none() {
            return self.append_batch(request, placement);
        }
        Box::pin(async move {
            let command = GroupWriteCommand::from(request.clone());
            if let Some(response) = self.forward_write_to_leader_if_follower(command).await? {
                return match response {
                    GroupWriteResponse::AppendBatch(response) => Ok(response),
                    other => Err(GroupEngineError::new(format!(
                        "unexpected append batch write response: {other:?}"
                    ))),
                };
            }
            self.ensure_stream_access(request.stream_id.clone(), request.now_ms, false)
                .await?;
            self.with_state_machine({
                let request = request.clone();
                move |state_machine| {
                    Box::pin(async move {
                        state_machine
                            .check_append_batch_cold_admission(request, placement, admission)
                            .await
                    })
                }
            })
            .await??;
            let mut responses = self
                .write_commands(vec![GroupWriteCommand::from(request)])
                .await?;
            let response = responses.pop().ok_or_else(|| {
                GroupEngineError::new("OpenRaft append batch returned no response")
            })?;
            match response? {
                GroupWriteResponse::AppendBatch(response) => Ok(response),
                other => Err(GroupEngineError::new(format!(
                    "unexpected append batch write response: {other:?}"
                ))),
            }
        })
    }

    fn append_batch_many_with_cold_admission<'a>(
        &'a mut self,
        requests: Vec<AppendBatchRequest>,
        placement: ShardPlacement,
        admission: ColdWriteAdmission,
    ) -> GroupWriteBatchFuture<'a> {
        if admission.max_hot_bytes_per_group.is_none() {
            let commands = requests
                .into_iter()
                .map(GroupWriteCommand::from)
                .collect::<Vec<_>>();
            return self.write_batch(commands, placement);
        }
        Box::pin(async move {
            if requests.is_empty() {
                return Ok(Vec::new());
            }
            let command = GroupWriteCommand::Batch {
                commands: requests
                    .iter()
                    .cloned()
                    .map(GroupWriteCommand::from)
                    .collect(),
            };
            if let Some(response) = self
                .forward_write_to_leader_if_follower(command.clone())
                .await?
            {
                return match response {
                    GroupWriteResponse::Batch(responses) => Ok(responses),
                    other => Err(GroupEngineError::new(format!(
                        "unexpected append batch many write response: {other:?}"
                    ))),
                };
            }
            self.with_state_machine({
                let requests = requests.clone();
                move |state_machine| {
                    Box::pin(async move {
                        state_machine
                            .check_append_batch_many_cold_admission(requests, placement, admission)
                            .await
                    })
                }
            })
            .await??;
            let mut responses = self.write_commands(vec![command]).await?;
            let response = responses.pop().ok_or_else(|| {
                GroupEngineError::new("OpenRaft append batch many returned no response")
            })?;
            match response? {
                GroupWriteResponse::Batch(responses) => Ok(responses),
                other => Err(GroupEngineError::new(format!(
                    "unexpected append batch many write response: {other:?}"
                ))),
            }
        })
    }

    fn flush_cold<'a>(
        &'a mut self,
        request: FlushColdRequest,
        _placement: ShardPlacement,
    ) -> GroupFlushColdFuture<'a> {
        Box::pin(async move {
            match self.write(GroupWriteCommand::from(request)).await? {
                GroupWriteResponse::FlushCold(response) => Ok(response),
                other => Err(GroupEngineError::new(format!(
                    "unexpected flush cold write response: {other:?}"
                ))),
            }
        })
    }

    fn write_batch<'a>(
        &'a mut self,
        commands: Vec<GroupWriteCommand>,
        _placement: ShardPlacement,
    ) -> GroupWriteBatchFuture<'a> {
        Box::pin(async move { self.write_commands(commands).await })
    }

    fn snapshot<'a>(&'a mut self, _placement: ShardPlacement) -> GroupSnapshotFuture<'a> {
        Box::pin(async move {
            self.with_state_machine(move |state_machine| {
                Box::pin(async move {
                    state_machine
                        .group_snapshot()
                        .await
                        .map_err(|err| GroupEngineError::new(err.to_string()))
                })
            })
            .await?
        })
    }

    fn install_snapshot<'a>(
        &'a mut self,
        snapshot: GroupSnapshot,
    ) -> GroupInstallSnapshotFuture<'a> {
        Box::pin(async move {
            self.with_state_machine(move |state_machine| {
                Box::pin(async move { state_machine.install_group_snapshot(snapshot).await })
            })
            .await?
        })
    }
}

fn group_engine_io_error(err: ursula_runtime::GroupEngineError) -> io::Error {
    io::Error::other(err.message().to_owned())
}

fn invalid_data(err: impl std::error::Error + Send + Sync + 'static) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, err)
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::time::Duration;
    use std::time::SystemTime;
    use std::time::UNIX_EPOCH;

    use futures_util::stream;
    use openraft::BasicNode;
    use openraft::Config;
    use openraft::Entry;
    use openraft::EntryPayload;
    use openraft::LogId;
    use openraft::Raft;
    use openraft::RaftSnapshotBuilder;
    use openraft::SnapshotPolicy;
    use openraft::alias::VoteOf;
    use openraft::entry::RaftEntry;
    use openraft::error::NetworkError;
    use openraft::error::Unreachable;
    use openraft::storage::IOFlushed;
    use openraft::storage::RaftLogReader;
    use openraft::storage::RaftLogStorage;
    use openraft::storage::RaftStateMachine;
    use openraft::vote::RaftLeaderId;
    use prost::Message;
    use ursula_runtime::{
        AppendBatchRequest, AppendRequest, ColdWriteAdmission, CreateStreamRequest,
        GroupWriteCommand, GroupWriteResponse, HeadStreamRequest, ProducerRequest,
        ReadStreamRequest, RuntimeConfig, RuntimeThreading, ShardRuntime,
    };
    use ursula_shard::{CoreId, RaftGroupId, ShardId, ShardPlacement};

    use super::*;

    type CommittedLeaderId = <UrsulaRaftTypeConfig as openraft::RaftTypeConfig>::LeaderId;

    fn placement() -> ShardPlacement {
        ShardPlacement {
            core_id: CoreId(0),
            shard_id: ShardId(0),
            raft_group_id: RaftGroupId(0),
        }
    }

    fn log_id(index: u64) -> LogId<CommittedLeaderId> {
        LogId {
            leader_id: CommittedLeaderId::new(1, 1),
            index,
        }
    }

    fn normal_entry(
        index: u64,
        command: GroupWriteCommand,
    ) -> <UrsulaRaftTypeConfig as openraft::RaftTypeConfig>::Entry {
        Entry::new(log_id(index), EntryPayload::Normal(command.into()))
    }

    fn create_stream_command(name: &str) -> GroupWriteCommand {
        GroupWriteCommand::from(CreateStreamRequest::new(
            ursula_shard::BucketStreamId::new("benchcmp", name),
            "application/octet-stream",
        ))
    }

    #[test]
    fn raft_group_command_uses_shared_protobuf_log_schema() {
        let command = GroupWriteCommand::AppendBatch {
            stream_id: ursula_shard::BucketStreamId::new("benchcmp", "shared-proto-log"),
            content_type: "application/octet-stream".to_owned(),
            payloads: vec![b"ab".to_vec().into(), b"cd".to_vec().into()],
            producer: Some(ProducerRequest {
                producer_id: "writer-1".to_owned(),
                producer_epoch: 7,
                producer_seq: 42,
            }),
            now_ms: 123,
        };
        let raft_command = RaftGroupCommand::from(command.clone());

        let mut encoded = Vec::new();
        raft_command
            .0
            .encode(&mut encoded)
            .expect("encode shared proto command");
        let decoded = raft_app_proto::RaftGroupCommandV1::decode(encoded.as_slice())
            .expect("decode shared proto command");

        assert_eq!(decoded, raft_command.0);
        assert_eq!(
            group_write_command_from_proto(RaftGroupCommand(decoded)).expect("domain command"),
            command
        );
    }

    #[test]
    fn raft_group_response_serde_uses_shared_protobuf_log_schema() {
        let response = raft_write_applied_response(GroupWriteResponse::CreateStream(
            ursula_runtime::CreateStreamResponse {
                placement: placement(),
                next_offset: 5,
                closed: false,
                already_exists: false,
                group_commit_index: 11,
            },
        ));

        let encoded_container =
            rmp_serde::to_vec_named(&response).expect("encode response wrapper");
        let decoded_container: RaftGroupResponse =
            rmp_serde::from_slice(&encoded_container).expect("decode response wrapper");

        assert_eq!(decoded_container, response);

        let mut encoded_proto = Vec::new();
        response
            .0
            .encode(&mut encoded_proto)
            .expect("encode shared proto response");
        let decoded_proto = raft_app_proto::RaftGroupResponseV1::decode(encoded_proto.as_slice())
            .expect("decode shared proto response");

        assert_eq!(decoded_proto, response.0);

        match group_write_result_from_raft_response(decoded_container).expect("domain response") {
            Ok(GroupWriteResponse::CreateStream(response)) => {
                assert_eq!(response.next_offset, 5);
                assert_eq!(response.group_commit_index, 11);
            }
            other => panic!("unexpected response: {other:?}"),
        }
    }

    fn temp_log_path(name: &str) -> PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time is after epoch")
            .as_nanos();
        std::env::temp_dir()
            .join("ursula-raft-tests")
            .join(format!("{name}-{}-{nonce}.json", std::process::id()))
    }

    fn non_empty_line_count(path: &Path) -> usize {
        fs::read_to_string(path)
            .expect("read log file")
            .lines()
            .filter(|line| !line.trim().is_empty())
            .count()
    }

    #[derive(Debug, Clone, Default)]
    struct InProcessRaftRegistry {
        nodes: Arc<Mutex<BTreeMap<u64, Raft<UrsulaRaftTypeConfig, RaftGroupStateMachine>>>>,
        full_snapshot_calls: Arc<Mutex<BTreeMap<u64, usize>>>,
    }

    impl InProcessRaftRegistry {
        fn register(&self, node_id: u64, raft: Raft<UrsulaRaftTypeConfig, RaftGroupStateMachine>) {
            self.nodes
                .lock()
                .expect("registry mutex")
                .insert(node_id, raft);
        }

        fn get(&self, node_id: u64) -> Option<Raft<UrsulaRaftTypeConfig, RaftGroupStateMachine>> {
            self.nodes
                .lock()
                .expect("registry mutex")
                .get(&node_id)
                .cloned()
        }

        fn record_full_snapshot(&self, node_id: u64) {
            *self
                .full_snapshot_calls
                .lock()
                .expect("full snapshot calls mutex")
                .entry(node_id)
                .or_insert(0) += 1;
        }

        fn full_snapshot_count(&self, node_id: u64) -> usize {
            self.full_snapshot_calls
                .lock()
                .expect("full snapshot calls mutex")
                .get(&node_id)
                .copied()
                .unwrap_or(0)
        }
    }

    #[derive(Debug, Clone)]
    struct InProcessRaftNetworkFactory {
        registry: InProcessRaftRegistry,
    }

    impl RaftNetworkFactory<UrsulaRaftTypeConfig> for InProcessRaftNetworkFactory {
        type Network = InProcessRaftNetwork;

        async fn new_client(&mut self, target: u64, _node: &BasicNode) -> Self::Network {
            InProcessRaftNetwork {
                target,
                registry: self.registry.clone(),
            }
        }
    }

    #[derive(Debug, Clone)]
    struct InProcessRaftNetwork {
        target: u64,
        registry: InProcessRaftRegistry,
    }

    impl InProcessRaftNetwork {
        fn missing_target_error(&self) -> Unreachable<UrsulaRaftTypeConfig> {
            Unreachable::from_string(format!(
                "in-process raft node {} is not registered",
                self.target
            ))
        }
    }

    impl RaftNetworkV2<UrsulaRaftTypeConfig> for InProcessRaftNetwork {
        async fn append_entries(
            &mut self,
            rpc: AppendEntriesRequest<UrsulaRaftTypeConfig>,
            _option: RPCOption,
        ) -> Result<AppendEntriesResponse<UrsulaRaftTypeConfig>, RPCError<UrsulaRaftTypeConfig>>
        {
            let target = self
                .registry
                .get(self.target)
                .ok_or_else(|| RPCError::Unreachable(self.missing_target_error()))?;
            target.append_entries(rpc).await.map_err(|err| {
                RPCError::Network(NetworkError::from_string(format!(
                    "remote AppendEntries on node {}: {err}",
                    self.target
                )))
            })
        }

        async fn vote(
            &mut self,
            rpc: VoteRequest<UrsulaRaftTypeConfig>,
            _option: RPCOption,
        ) -> Result<VoteResponse<UrsulaRaftTypeConfig>, RPCError<UrsulaRaftTypeConfig>> {
            let target = self
                .registry
                .get(self.target)
                .ok_or_else(|| RPCError::Unreachable(self.missing_target_error()))?;
            target.vote(rpc).await.map_err(|err| {
                RPCError::Network(NetworkError::from_string(format!(
                    "remote Vote on node {}: {err}",
                    self.target
                )))
            })
        }

        async fn full_snapshot(
            &mut self,
            vote: VoteOf<UrsulaRaftTypeConfig>,
            snapshot: TypeConfigSnapshotOf<UrsulaRaftTypeConfig>,
            _cancel: impl Future<Output = ReplicationClosed> + OptionalSend + 'static,
            _option: RPCOption,
        ) -> Result<SnapshotResponse<UrsulaRaftTypeConfig>, StreamingError<UrsulaRaftTypeConfig>>
        {
            self.registry.record_full_snapshot(self.target);
            let target = self
                .registry
                .get(self.target)
                .ok_or_else(|| StreamingError::Unreachable(self.missing_target_error()))?;
            target
                .install_full_snapshot(vote, snapshot)
                .await
                .map_err(|err| {
                    StreamingError::Network(NetworkError::from_string(format!(
                        "remote full snapshot on node {}: {err}",
                        self.target
                    )))
                })
        }
    }

    #[tokio::test]
    async fn raft_log_store_appends_reads_truncates_and_purges() {
        let mut store = RaftGroupLogStore::shared();
        store
            .append(
                vec![
                    normal_entry(1, create_stream_command("log-1")),
                    normal_entry(2, create_stream_command("log-2")),
                    normal_entry(3, create_stream_command("log-3")),
                ],
                IOFlushed::noop(),
            )
            .await
            .expect("append raft log entries");

        let state = store.get_log_state().await.expect("log state");
        assert_eq!(state.last_purged_log_id, None);
        assert_eq!(state.last_log_id, Some(log_id(3)));

        let mut reader = store.get_log_reader().await;
        let entries = reader
            .try_get_log_entries(1..4)
            .await
            .expect("read entries");
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].log_id, log_id(1));
        assert_eq!(entries[2].log_id, log_id(3));

        store
            .truncate_after(Some(log_id(1)))
            .await
            .expect("truncate log");
        assert_eq!(
            store.get_log_state().await.expect("log state").last_log_id,
            Some(log_id(1))
        );

        store
            .append(
                vec![
                    normal_entry(2, create_stream_command("log-2b")),
                    normal_entry(3, create_stream_command("log-3b")),
                ],
                IOFlushed::noop(),
            )
            .await
            .expect("append after truncate");
        store.purge(log_id(2)).await.expect("purge log");

        let state = store.get_log_state().await.expect("log state after purge");
        assert_eq!(state.last_purged_log_id, Some(log_id(2)));
        assert_eq!(state.last_log_id, Some(log_id(3)));

        let entries = reader
            .try_get_log_entries(1..4)
            .await
            .expect("read after purge");
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].log_id, log_id(3));
    }

    #[tokio::test]
    async fn raft_log_store_persists_vote_and_committed_pointer() {
        let mut store = RaftGroupLogStore::shared();
        let vote: VoteOf<UrsulaRaftTypeConfig> = openraft::Vote::new_committed(7, 1);

        store.save_vote(&vote).await.expect("save vote");
        let mut reader = store.get_log_reader().await;
        assert_eq!(reader.read_vote().await.expect("read vote"), Some(vote));

        store
            .save_committed(Some(log_id(9)))
            .await
            .expect("save committed");
        assert_eq!(
            store.read_committed().await.expect("read committed"),
            Some(log_id(9))
        );
    }

    #[tokio::test]
    async fn raft_log_store_rejects_holes() {
        let mut store = RaftGroupLogStore::shared();
        let err = store
            .append(
                vec![
                    normal_entry(1, create_stream_command("hole-1")),
                    normal_entry(3, create_stream_command("hole-3")),
                ],
                IOFlushed::noop(),
            )
            .await
            .expect_err("hole should be rejected");

        assert_eq!(err.kind(), io::ErrorKind::InvalidData);

        store
            .append(
                vec![normal_entry(1, create_stream_command("hole-boundary-1"))],
                IOFlushed::noop(),
            )
            .await
            .expect("append first entry");
        let err = store
            .append(
                vec![normal_entry(3, create_stream_command("hole-boundary-3"))],
                IOFlushed::noop(),
            )
            .await
            .expect_err("cross-append hole should be rejected");

        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    #[tokio::test]
    async fn raft_file_log_store_recovers_vote_committed_and_entries() {
        let path = temp_log_path("recover");
        let vote: VoteOf<UrsulaRaftTypeConfig> = openraft::Vote::new_committed(7, 1);

        {
            let mut store = RaftGroupFileLogStore::shared(&path).expect("open file log store");
            store
                .append(
                    vec![
                        normal_entry(1, create_stream_command("file-log-1")),
                        normal_entry(2, create_stream_command("file-log-2")),
                    ],
                    IOFlushed::noop(),
                )
                .await
                .expect("append file log entries");
            store.save_vote(&vote).await.expect("save vote");
            store
                .save_committed(Some(log_id(2)))
                .await
                .expect("save committed");
        }
        assert_eq!(non_empty_line_count(&path), 3);

        let mut reopened = RaftGroupFileLogStore::shared(&path).expect("reopen file log store");
        let state = reopened.get_log_state().await.expect("log state");
        assert_eq!(state.last_log_id, Some(log_id(2)));
        assert_eq!(
            reopened.read_committed().await.expect("committed"),
            Some(log_id(2))
        );

        let mut reader = reopened.get_log_reader().await;
        assert_eq!(reader.read_vote().await.expect("vote"), Some(vote));
        let entries = reader
            .try_get_log_entries(1..3)
            .await
            .expect("read recovered entries");
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].log_id, log_id(1));
        assert_eq!(entries[1].log_id, log_id(2));

        let _ = fs::remove_file(&path);
    }

    #[tokio::test]
    async fn raft_file_log_store_skips_duplicate_vote_and_committed_records() {
        let path = temp_log_path("duplicate-vote-committed");
        let vote: VoteOf<UrsulaRaftTypeConfig> = openraft::Vote::new_committed(7, 1);

        {
            let mut store = RaftGroupFileLogStore::shared(&path).expect("open file log store");
            store.save_vote(&vote).await.expect("save vote");
            store
                .save_committed(Some(log_id(2)))
                .await
                .expect("save committed");
            store.save_vote(&vote).await.expect("save duplicate vote");
            store
                .save_committed(Some(log_id(2)))
                .await
                .expect("save duplicate committed");
        }
        assert_eq!(non_empty_line_count(&path), 2);

        let mut reopened = RaftGroupFileLogStore::shared(&path).expect("reopen file log store");
        assert_eq!(reopened.read_vote().await.expect("vote"), Some(vote));
        assert_eq!(
            reopened.read_committed().await.expect("committed"),
            Some(log_id(2))
        );

        let _ = fs::remove_file(&path);
    }

    #[tokio::test]
    async fn raft_file_log_store_recovers_truncate_and_purge() {
        let path = temp_log_path("truncate-purge");

        {
            let mut store = RaftGroupFileLogStore::shared(&path).expect("open file log store");
            store
                .append(
                    vec![
                        normal_entry(1, create_stream_command("file-log-1")),
                        normal_entry(2, create_stream_command("file-log-2")),
                        normal_entry(3, create_stream_command("file-log-3")),
                    ],
                    IOFlushed::noop(),
                )
                .await
                .expect("append initial entries");
            store
                .truncate_after(Some(log_id(1)))
                .await
                .expect("truncate file log");
            store
                .append(
                    vec![
                        normal_entry(2, create_stream_command("file-log-2b")),
                        normal_entry(3, create_stream_command("file-log-3b")),
                    ],
                    IOFlushed::noop(),
                )
                .await
                .expect("append after truncate");
            store.purge(log_id(2)).await.expect("purge file log");
        }
        assert_eq!(non_empty_line_count(&path), 4);

        let mut reopened = RaftGroupFileLogStore::shared(&path).expect("reopen file log store");
        let state = reopened.get_log_state().await.expect("log state");
        assert_eq!(state.last_purged_log_id, Some(log_id(2)));
        assert_eq!(state.last_log_id, Some(log_id(3)));

        let mut reader = reopened.get_log_reader().await;
        let entries = reader
            .try_get_log_entries(1..4)
            .await
            .expect("read recovered entries");
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].log_id, log_id(3));

        let _ = fs::remove_file(&path);
    }

    #[tokio::test]
    async fn single_node_openraft_group_applies_client_writes() {
        let config = Arc::new(
            Config {
                cluster_name: "ursula-single-node-test".to_owned(),
                heartbeat_interval: 10,
                election_timeout_min: 30,
                election_timeout_max: 60,
                ..Default::default()
            }
            .validate()
            .expect("valid raft config"),
        );
        let mut log_store = RaftGroupLogStore::shared();
        let state_machine = RaftGroupStateMachine::new(placement());
        let raft = Raft::<UrsulaRaftTypeConfig, RaftGroupStateMachine>::new(
            1,
            config,
            SingleNodeRaftNetworkFactory,
            log_store.clone(),
            state_machine,
        )
        .await
        .expect("create single-node raft group");

        let mut nodes = BTreeMap::new();
        nodes.insert(1, BasicNode::new("local"));
        raft.initialize(nodes)
            .await
            .expect("initialize single-node raft group");
        raft.wait(Some(Duration::from_secs(2)))
            .current_leader(1, "single-node raft group should elect itself")
            .await
            .expect("wait for leadership");

        let stream_id = ursula_shard::BucketStreamId::new("benchcmp", "raft-client-write");
        let created = raft
            .client_write(
                GroupWriteCommand::from(CreateStreamRequest::new(
                    stream_id.clone(),
                    "application/octet-stream",
                ))
                .into(),
            )
            .await
            .expect("create stream through openraft");
        assert!(matches!(
            group_write_result_from_raft_response(created.data).expect("decode create response"),
            Ok(GroupWriteResponse::CreateStream(_))
        ));

        let appended = raft
            .client_write(
                GroupWriteCommand::from(AppendRequest::from_bytes(stream_id, b"payload".to_vec()))
                    .into(),
            )
            .await
            .expect("append through openraft");
        match group_write_result_from_raft_response(appended.data).expect("decode append response")
        {
            Ok(GroupWriteResponse::Append(response)) => {
                assert_eq!(response.start_offset, 0);
                assert_eq!(response.stream_append_count, 1);
            }
            other => panic!("unexpected append response: {other:?}"),
        }

        let state = log_store.get_log_state().await.expect("raft log state");
        assert!(state.last_log_id.is_some());
        raft.shutdown().await.expect("shutdown raft group");
    }

    #[tokio::test]
    async fn three_node_openraft_group_replicates_group_writes() {
        let registry = InProcessRaftRegistry::default();
        let config = Arc::new(
            Config {
                cluster_name: "ursula-three-node-test".to_owned(),
                heartbeat_interval: 10,
                election_timeout_min: 50,
                election_timeout_max: 100,
                ..Default::default()
            }
            .validate()
            .expect("valid raft config"),
        );
        let mut nodes = BTreeMap::new();
        for node_id in 1..=3 {
            nodes.insert(node_id, BasicNode::new(format!("node-{node_id}")));
        }

        let mut engines = Vec::new();
        for node_id in 1..=3 {
            let engine = RaftGroupEngine::new_node_with_log_store_and_network(
                placement(),
                node_id,
                config.clone(),
                InProcessRaftNetworkFactory {
                    registry: registry.clone(),
                },
                RaftGroupLogStore::shared(),
                None,
                None,
            )
            .await
            .expect("create cluster raft group node");
            registry.register(node_id, engine.raft.clone());
            engines.push(engine);
        }

        engines[0]
            .raft
            .initialize(nodes)
            .await
            .expect("initialize three-node raft group");
        let leader_metrics = engines[0]
            .raft
            .wait(Some(Duration::from_secs(5)))
            .metrics(|metrics| metrics.current_leader.is_some(), "leader elected")
            .await
            .expect("wait for leader");
        let leader_id = leader_metrics.current_leader.expect("leader id");
        for engine in &engines {
            engine
                .raft
                .wait(Some(Duration::from_secs(5)))
                .current_leader(leader_id, "all nodes observe the same leader")
                .await
                .expect("wait for shared leader");
        }
        for (index, engine) in engines.iter().enumerate() {
            let node_id = u64::try_from(index + 1).expect("node id fits u64");
            assert_eq!(engine.accepts_local_writes(), node_id == leader_id);
        }

        let leader_index = usize::try_from(leader_id - 1).expect("leader id fits usize");
        let stream_id =
            ursula_shard::BucketStreamId::new("benchcmp", "three-node-raft-group-engine");
        let created = engines[leader_index]
            .raft
            .client_write(
                GroupWriteCommand::from(CreateStreamRequest::new(
                    stream_id.clone(),
                    "application/octet-stream",
                ))
                .into(),
            )
            .await
            .expect("create stream through elected leader");
        assert!(matches!(
            group_write_result_from_raft_response(created.data).expect("decode create response"),
            Ok(GroupWriteResponse::CreateStream(_))
        ));

        let appended = engines[leader_index]
            .raft
            .client_write(
                GroupWriteCommand::from(AppendRequest::from_bytes(
                    stream_id.clone(),
                    b"replicated".to_vec(),
                ))
                .into(),
            )
            .await
            .expect("append through elected leader");
        let appended_log_index = appended.log_id.index();
        match group_write_result_from_raft_response(appended.data).expect("decode append response")
        {
            Ok(GroupWriteResponse::Append(response)) => {
                assert_eq!(response.start_offset, 0);
                assert_eq!(response.next_offset, 10);
            }
            other => panic!("unexpected append response: {other:?}"),
        }

        for engine in &engines {
            engine
                .raft
                .wait(Some(Duration::from_secs(5)))
                .applied_index_at_least(Some(appended_log_index), "append replicated")
                .await
                .expect("wait for append replication");

            let stream_id = stream_id.clone();
            let read = engine
                .with_state_machine(move |state_machine| {
                    Box::pin(async move {
                        state_machine
                            .read_stream(
                                ReadStreamRequest {
                                    stream_id,
                                    offset: 0,
                                    max_len: 16,
                                    now_ms: 0,
                                },
                                placement(),
                            )
                            .await
                    })
                })
                .await
                .expect("read follower state machine")
                .expect("replicated stream is readable");
            assert_eq!(read.payload, b"replicated");
        }

        for engine in &engines {
            engine
                .shutdown()
                .await
                .expect("shutdown cluster raft group node");
        }
    }

    #[tokio::test]
    async fn openraft_installs_snapshot_for_lagging_learner() {
        let registry = InProcessRaftRegistry::default();
        let config = Arc::new(
            Config {
                cluster_name: "ursula-lagging-learner-snapshot-test".to_owned(),
                heartbeat_interval: 10,
                election_timeout_min: 50,
                election_timeout_max: 100,
                max_in_snapshot_log_to_keep: 0,
                purge_batch_size: 1,
                replication_lag_threshold: 0,
                snapshot_policy: SnapshotPolicy::Never,
                ..Default::default()
            }
            .validate()
            .expect("valid raft config"),
        );

        let mut engines = Vec::new();
        for node_id in 1..=3 {
            let engine = RaftGroupEngine::new_node_with_log_store_and_network(
                placement(),
                node_id,
                config.clone(),
                InProcessRaftNetworkFactory {
                    registry: registry.clone(),
                },
                RaftGroupLogStore::shared(),
                None,
                None,
            )
            .await
            .expect("create cluster raft group node");
            if node_id != 3 {
                registry.register(node_id, engine.raft.clone());
            }
            engines.push(engine);
        }

        let mut initial_nodes = BTreeMap::new();
        for node_id in 1..=2 {
            initial_nodes.insert(node_id, BasicNode::new(format!("node-{node_id}")));
        }
        engines[0]
            .raft
            .initialize(initial_nodes)
            .await
            .expect("initialize two-node raft group");
        let leader_metrics = engines[0]
            .raft
            .wait(Some(Duration::from_secs(5)))
            .metrics(|metrics| metrics.current_leader.is_some(), "leader elected")
            .await
            .expect("wait for leader");
        let leader_id = leader_metrics.current_leader.expect("leader id");
        for engine in &engines[..2] {
            engine
                .raft
                .wait(Some(Duration::from_secs(5)))
                .current_leader(leader_id, "initial voters observe the same leader")
                .await
                .expect("wait for shared leader");
        }

        let leader_index = usize::try_from(leader_id - 1).expect("leader id fits usize");
        let stream_id = ursula_shard::BucketStreamId::new("benchcmp", "lagging-learner-snapshot");
        let _created = engines[leader_index]
            .raft
            .client_write(
                GroupWriteCommand::from(CreateStreamRequest::new(
                    stream_id.clone(),
                    "application/octet-stream",
                ))
                .into(),
            )
            .await
            .expect("create stream through elected leader");
        let appended = engines[leader_index]
            .raft
            .client_write(
                GroupWriteCommand::from(AppendRequest::from_bytes(
                    stream_id.clone(),
                    b"snapshot-transfer".to_vec(),
                ))
                .into(),
            )
            .await
            .expect("append through elected leader");
        let appended_log_id = appended.log_id;
        let appended_log_index = appended_log_id.index();
        assert!(matches!(
            group_write_result_from_raft_response(appended.data).expect("decode append response"),
            Ok(GroupWriteResponse::Append(_))
        ));

        for engine in &engines[..2] {
            engine
                .raft
                .wait(Some(Duration::from_secs(5)))
                .applied_index_at_least(Some(appended_log_index), "initial voters applied append")
                .await
                .expect("wait for initial voter apply");
        }

        engines[leader_index]
            .raft
            .trigger()
            .snapshot()
            .await
            .expect("trigger leader snapshot");
        engines[leader_index]
            .raft
            .wait(Some(Duration::from_secs(5)))
            .snapshot(appended_log_id, "leader snapshot includes append")
            .await
            .expect("wait for leader snapshot");
        engines[leader_index]
            .raft
            .trigger()
            .purge_log(appended_log_index)
            .await
            .expect("trigger leader log purge");
        engines[leader_index]
            .raft
            .wait(Some(Duration::from_secs(5)))
            .purged(Some(appended_log_id), "leader purged snapshotted logs")
            .await
            .expect("wait for leader purge");

        registry.register(3, engines[2].raft.clone());
        let learner_added = engines[leader_index]
            .raft
            .add_learner(3, BasicNode::new("node-3"), true)
            .await
            .expect("add lagging learner");
        for _ in 0..50 {
            if registry.full_snapshot_count(3) > 0 {
                break;
            }
            engines[leader_index]
                .raft
                .trigger()
                .heartbeat()
                .await
                .expect("trigger heartbeat while waiting for snapshot replication");
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        assert!(
            registry.full_snapshot_count(3) >= 1,
            "lagging learner should catch up through full_snapshot"
        );

        engines[2]
            .raft
            .wait(Some(Duration::from_secs(5)))
            .applied_index_at_least(
                Some(learner_added.log_id.index()),
                "lagging learner applied learner membership",
            )
            .await
            .expect("wait for lagging learner catch-up");
        let installed_snapshot_log_id = engines[2]
            .with_state_machine(|state_machine| {
                Box::pin(async move {
                    state_machine
                        .current_snapshot
                        .lock()
                        .expect("snapshot mutex")
                        .as_ref()
                        .and_then(|snapshot| snapshot.meta.last_log_id)
                })
            })
            .await
            .expect("inspect lagging learner state machine");
        assert_eq!(installed_snapshot_log_id, Some(appended_log_id));

        let read = engines[2]
            .with_state_machine({
                let stream_id = stream_id.clone();
                move |state_machine| {
                    Box::pin(async move {
                        state_machine
                            .read_stream(
                                ReadStreamRequest {
                                    stream_id,
                                    offset: 0,
                                    max_len: 64,
                                    now_ms: 0,
                                },
                                placement(),
                            )
                            .await
                    })
                }
            })
            .await
            .expect("read lagging learner state machine")
            .expect("stream restored from snapshot is readable");
        assert_eq!(read.payload, b"snapshot-transfer");

        for engine in &engines {
            engine
                .shutdown()
                .await
                .expect("shutdown cluster raft group node");
        }
    }

    #[tokio::test]
    async fn raft_group_engine_implements_runtime_group_engine_over_openraft() {
        let mut engine = RaftGroupEngine::new_single_node(placement())
            .await
            .expect("create raft group engine");
        let stream_id = ursula_shard::BucketStreamId::new("benchcmp", "raft-group-engine");

        let created = engine
            .create_stream(
                CreateStreamRequest::new(stream_id.clone(), "application/octet-stream"),
                placement(),
            )
            .await
            .expect("create through group engine");
        assert_eq!(created.next_offset, 0);
        assert!(!created.already_exists);

        let appended = engine
            .append(
                AppendRequest::from_bytes(stream_id.clone(), b"payload".to_vec()),
                placement(),
            )
            .await
            .expect("append through group engine");
        assert_eq!(appended.start_offset, 0);
        assert_eq!(appended.next_offset, 7);

        let head = engine
            .head_stream(
                HeadStreamRequest {
                    stream_id: stream_id.clone(),
                    now_ms: 0,
                },
                placement(),
            )
            .await
            .expect("head through group engine");
        assert_eq!(head.tail_offset, 7);

        let read = engine
            .read_stream(
                ReadStreamRequest {
                    stream_id,
                    offset: 0,
                    max_len: 16,
                    now_ms: 0,
                },
                placement(),
            )
            .await
            .expect("read through group engine");
        assert_eq!(read.payload, b"payload");

        let snapshot = engine.snapshot(placement()).await.expect("snapshot");
        assert_eq!(snapshot.group_commit_index, 2);
        engine.shutdown().await.expect("shutdown raft group engine");
    }

    #[tokio::test]
    async fn raft_group_engine_applies_batched_runtime_writes() {
        let mut engine = RaftGroupEngine::new_single_node(placement())
            .await
            .expect("create raft group engine");
        let stream_id = ursula_shard::BucketStreamId::new("benchcmp", "raft-group-engine-batch");

        let responses = engine
            .write_batch(
                vec![
                    GroupWriteCommand::from(CreateStreamRequest::new(
                        stream_id.clone(),
                        "application/octet-stream",
                    )),
                    GroupWriteCommand::from(AppendBatchRequest::new(
                        stream_id.clone(),
                        vec![b"ab".to_vec(), b"cd".to_vec()],
                    )),
                ],
                placement(),
            )
            .await
            .expect("write batch through group engine");

        assert_eq!(responses.len(), 2);
        assert!(matches!(
            &responses[0],
            Ok(GroupWriteResponse::CreateStream(response)) if response.group_commit_index == 1
        ));
        match &responses[1] {
            Ok(GroupWriteResponse::AppendBatch(response)) => {
                assert_eq!(response.items.len(), 2);
                assert_eq!(
                    response.items[0].as_ref().expect("first item").start_offset,
                    0
                );
                assert_eq!(
                    response.items[1]
                        .as_ref()
                        .expect("second item")
                        .start_offset,
                    2
                );
            }
            other => panic!("unexpected append batch response: {other:?}"),
        }

        let read = engine
            .read_stream(
                ReadStreamRequest {
                    stream_id,
                    offset: 0,
                    max_len: 16,
                    now_ms: 0,
                },
                placement(),
            )
            .await
            .expect("read batched write");
        assert_eq!(read.payload, b"abcd");
        engine.shutdown().await.expect("shutdown raft group engine");
    }

    #[tokio::test]
    async fn raft_group_engine_cold_admission_coalesces_append_batch_many_into_one_raft_entry() {
        let mut engine = RaftGroupEngine::new_single_node(placement())
            .await
            .expect("create raft group engine");
        let stream_id =
            ursula_shard::BucketStreamId::new("benchcmp", "raft-group-engine-cold-batch-many");

        engine
            .create_stream(
                CreateStreamRequest::new(stream_id.clone(), "application/octet-stream"),
                placement(),
            )
            .await
            .expect("create stream");
        let before_batch_log_index = engine
            .raft_handle()
            .metrics()
            .borrow_watched()
            .last_log_index
            .expect("create stream should append a raft log entry");

        let responses = engine
            .append_batch_many_with_cold_admission(
                vec![
                    AppendBatchRequest::new(stream_id.clone(), vec![b"ab".to_vec()]),
                    AppendBatchRequest::new(stream_id.clone(), vec![b"cd".to_vec()]),
                    AppendBatchRequest::new(stream_id.clone(), vec![b"ef".to_vec()]),
                ],
                placement(),
                ColdWriteAdmission {
                    max_hot_bytes_per_group: Some(1024 * 1024),
                },
            )
            .await
            .expect("append batch many with cold admission");

        assert_eq!(responses.len(), 3);
        for (index, response) in responses.into_iter().enumerate() {
            match response.expect("append batch response") {
                GroupWriteResponse::AppendBatch(response) => {
                    assert_eq!(response.items.len(), 1);
                    let item = response.items[0].as_ref().expect("append batch item");
                    assert_eq!(item.start_offset, u64::try_from(index * 2).unwrap());
                }
                other => panic!("unexpected response: {other:?}"),
            }
        }

        let after_batch_log_index = engine
            .raft_handle()
            .metrics()
            .borrow_watched()
            .last_log_index
            .expect("append batch should append a raft log entry");
        assert_eq!(after_batch_log_index, before_batch_log_index + 1);

        let read = engine
            .read_stream(
                ReadStreamRequest {
                    stream_id,
                    offset: 0,
                    max_len: 16,
                    now_ms: 0,
                },
                placement(),
            )
            .await
            .expect("read coalesced append batches");
        assert_eq!(read.payload, b"abcdef");
        engine.shutdown().await.expect("shutdown raft group engine");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn raft_metrics_count_logical_commands_inside_coalesced_batches() {
        let runtime = ShardRuntime::spawn_with_engine_factory(
            RuntimeConfig::new(1, 1).with_cold_max_hot_bytes_per_group(Some(1024 * 1024)),
            RaftGroupEngineFactory,
        )
        .expect("spawn raft runtime");
        let stream_id =
            ursula_shard::BucketStreamId::new("benchcmp", "raft-logical-command-metrics");

        runtime
            .create_stream(CreateStreamRequest::new(
                stream_id.clone(),
                "application/octet-stream",
            ))
            .await
            .expect("create stream");
        let before = runtime.metrics().snapshot();

        let first = {
            let runtime = runtime.clone();
            let stream_id = stream_id.clone();
            tokio::spawn(async move {
                runtime
                    .append_batch(AppendBatchRequest::new(stream_id, vec![b"ab".to_vec()]))
                    .await
                    .expect("first append batch")
            })
        };
        let second = {
            let runtime = runtime.clone();
            let stream_id = stream_id.clone();
            tokio::spawn(async move {
                runtime
                    .append_batch(AppendBatchRequest::new(stream_id, vec![b"cd".to_vec()]))
                    .await
                    .expect("second append batch")
            })
        };
        let third = {
            let runtime = runtime.clone();
            let stream_id = stream_id.clone();
            tokio::spawn(async move {
                runtime
                    .append_batch(AppendBatchRequest::new(stream_id, vec![b"ef".to_vec()]))
                    .await
                    .expect("third append batch")
            })
        };

        first.await.expect("first task");
        second.await.expect("second task");
        third.await.expect("third task");

        let after = runtime.metrics().snapshot();
        assert_eq!(
            after.raft_write_many_commands - before.raft_write_many_commands,
            after.raft_write_many_batches - before.raft_write_many_batches
        );
        assert_eq!(
            after.raft_write_many_logical_commands - before.raft_write_many_logical_commands,
            3
        );
        assert!(
            after.raft_write_many_logical_commands >= after.raft_write_many_commands,
            "logical command count should include commands nested in Batch"
        );

        let read = runtime
            .read_stream(ReadStreamRequest {
                stream_id,
                offset: 0,
                max_len: 16,
                now_ms: 0,
            })
            .await
            .expect("read appended batches");
        let mut chunks = read
            .payload
            .chunks_exact(2)
            .map(Vec::from)
            .collect::<Vec<_>>();
        chunks.sort();
        assert_eq!(chunks, vec![b"ab".to_vec(), b"cd".to_vec(), b"ef".to_vec()]);
    }

    #[tokio::test]
    async fn raft_group_engine_preserves_stream_error_next_offset() {
        let mut engine = RaftGroupEngine::new_single_node(placement())
            .await
            .expect("create raft group engine");
        let stream_id = ursula_shard::BucketStreamId::new("benchcmp", "raft-stream-error-offset");

        engine
            .create_stream(
                CreateStreamRequest::new(stream_id.clone(), "application/octet-stream"),
                placement(),
            )
            .await
            .expect("create through group engine");
        engine
            .append(
                AppendRequest::from_bytes(stream_id.clone(), b"payload".to_vec()),
                placement(),
            )
            .await
            .expect("append through group engine");
        engine
            .close_stream(
                CloseStreamRequest {
                    stream_id: stream_id.clone(),
                    stream_seq: None,
                    producer: None,
                    now_ms: 0,
                },
                placement(),
            )
            .await
            .expect("close through group engine");

        let err = engine
            .append(
                AppendRequest::from_bytes(stream_id, b"after-close".to_vec()),
                placement(),
            )
            .await
            .expect_err("append to closed stream should fail");
        assert_eq!(err.next_offset(), Some(7));

        engine.shutdown().await.expect("shutdown raft group engine");
    }

    #[tokio::test]
    async fn raft_group_engine_recovers_client_writes_from_file_log() {
        let path = temp_log_path("raft-group-engine-recover");
        let stream_id = ursula_shard::BucketStreamId::new("benchcmp", "raft-engine-recover");

        {
            let mut engine = RaftGroupEngine::new_single_node_with_file_log(placement(), &path)
                .await
                .expect("create durable raft group engine");
            engine
                .create_stream(
                    CreateStreamRequest::new(stream_id.clone(), "application/octet-stream"),
                    placement(),
                )
                .await
                .expect("create through durable raft group engine");
            engine
                .append(
                    AppendRequest::from_bytes(stream_id.clone(), b"payload".to_vec()),
                    placement(),
                )
                .await
                .expect("append through durable raft group engine");
            engine.shutdown().await.expect("shutdown first engine");
        }

        let mut recovered = RaftGroupEngine::new_single_node_with_file_log(placement(), &path)
            .await
            .expect("reopen durable raft group engine");
        let read = recovered
            .read_stream(
                ReadStreamRequest {
                    stream_id,
                    offset: 0,
                    max_len: 16,
                    now_ms: 0,
                },
                placement(),
            )
            .await
            .expect("read recovered payload");
        assert_eq!(read.payload, b"payload");
        recovered
            .shutdown()
            .await
            .expect("shutdown recovered engine");

        let _ = fs::remove_file(&path);
    }

    #[tokio::test]
    async fn shard_runtime_uses_raft_group_engine_factory_for_owned_group() {
        let mut config = RuntimeConfig::new(1, 1);
        config.threading = RuntimeThreading::HostedTokio;
        let runtime = ShardRuntime::spawn_with_engine_factory(config, RaftGroupEngineFactory)
            .expect("spawn runtime with raft group engine factory");
        let stream_id = ursula_shard::BucketStreamId::new("benchcmp", "runtime-raft-engine");

        runtime
            .create_stream(CreateStreamRequest::new(
                stream_id.clone(),
                "application/octet-stream",
            ))
            .await
            .expect("create through runtime-owned raft group");
        runtime
            .append(AppendRequest::from_bytes(
                stream_id.clone(),
                b"payload".to_vec(),
            ))
            .await
            .expect("append through runtime-owned raft group");

        let read = runtime
            .read_stream(ReadStreamRequest {
                stream_id,
                offset: 0,
                max_len: 16,
                now_ms: 0,
            })
            .await
            .expect("read through runtime-owned raft group");
        assert_eq!(read.payload, b"payload");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn warm_group_registers_runtime_owned_raft_handle() {
        let registry = RaftGroupHandleRegistry::default();
        let mut config = RuntimeConfig::new(2, 4);
        config.threading = RuntimeThreading::HostedTokio;
        let runtime = ShardRuntime::spawn_with_engine_factory(
            config,
            RegisteredRaftGroupEngineFactory::new(registry.clone()),
        )
        .expect("spawn runtime with registered raft group engine factory");

        assert!(registry.is_empty());
        let placement = runtime
            .warm_group(RaftGroupId(3))
            .await
            .expect("warm raft group");
        assert_eq!(placement.core_id, CoreId(1));
        assert_eq!(placement.raft_group_id, RaftGroupId(3));
        assert!(registry.contains_group(RaftGroupId(3)));
        assert_eq!(registry.len(), 1);

        let raft = registry
            .get(RaftGroupId(3))
            .expect("registered raft handle");
        raft.wait(Some(Duration::from_secs(2)))
            .current_leader(1, "registered single-node group should elect itself")
            .await
            .expect("wait for registered leader");
    }

    #[tokio::test]
    async fn durable_raft_group_engine_records_file_log_metrics() {
        let root = temp_log_path("raft-file-log-metrics-root").with_extension("");
        let _ = fs::remove_dir_all(&root);

        let mut config = RuntimeConfig::new(1, 1);
        config.threading = RuntimeThreading::HostedTokio;
        let runtime = ShardRuntime::spawn_with_engine_factory(
            config,
            DurableRaftGroupEngineFactory::new(&root),
        )
        .expect("spawn runtime with durable raft group engine factory");
        let placement = placement();
        let stream_id = ursula_shard::BucketStreamId::new("benchcmp", "runtime-raft-file-metrics");

        runtime
            .create_stream(CreateStreamRequest::new(
                stream_id.clone(),
                "application/octet-stream",
            ))
            .await
            .expect("create through durable runtime-owned raft group");
        runtime
            .append(AppendRequest::from_bytes(
                stream_id.clone(),
                b"payload".to_vec(),
            ))
            .await
            .expect("append through durable runtime-owned raft group");

        let metrics = runtime.metrics().snapshot();
        let core_index = usize::from(placement.core_id.0);
        let group_index = usize::try_from(placement.raft_group_id.0).expect("u32 fits usize");
        assert!(metrics.wal_batches >= 2);
        assert!(metrics.wal_records >= 2);
        assert_eq!(
            metrics.wal_batches,
            metrics.per_core_wal_batches[core_index]
        );
        assert_eq!(
            metrics.wal_records,
            metrics.per_group_wal_records[group_index]
        );
        assert!(metrics.wal_write_ns > 0);
        assert!(metrics.wal_sync_ns > 0);

        drop(runtime);
        let _ = fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn durable_raft_group_engine_recovers_from_core_journal() {
        let root = temp_log_path("raft-core-journal-recover-root").with_extension("");
        let _ = fs::remove_dir_all(&root);
        let stream_id = ursula_shard::BucketStreamId::new("benchcmp", "raft-core-journal-recover");

        {
            let mut config = RuntimeConfig::new(1, 1);
            config.threading = RuntimeThreading::HostedTokio;
            let runtime = ShardRuntime::spawn_with_engine_factory(
                config,
                DurableRaftGroupEngineFactory::new(&root),
            )
            .expect("spawn durable runtime");
            runtime
                .create_stream(CreateStreamRequest::new(
                    stream_id.clone(),
                    "application/octet-stream",
                ))
                .await
                .expect("create stream");
            runtime
                .append(AppendRequest::from_bytes(
                    stream_id.clone(),
                    b"journal-payload".to_vec(),
                ))
                .await
                .expect("append stream");
        }

        let journal_path = root.join("core-0").join("journal.bin");
        assert!(journal_path.exists(), "core journal should exist");
        assert!(
            fs::metadata(&journal_path)
                .expect("core journal metadata")
                .len()
                > 0,
            "core journal should contain records"
        );

        {
            let mut config = RuntimeConfig::new(1, 1);
            config.threading = RuntimeThreading::HostedTokio;
            let recovered = ShardRuntime::spawn_with_engine_factory(
                config,
                DurableRaftGroupEngineFactory::new(&root),
            )
            .expect("spawn recovered durable runtime");
            let read = recovered
                .read_stream(ReadStreamRequest {
                    stream_id,
                    offset: 0,
                    max_len: 32,
                    now_ms: 0,
                })
                .await
                .expect("read recovered stream");
            assert_eq!(read.payload, b"journal-payload");
        }

        let _ = fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn openraft_state_machine_applies_group_write_commands() {
        let stream_id = ursula_shard::BucketStreamId::new("benchcmp", "raft-apply");
        let mut sm = RaftGroupStateMachine::new(placement());
        let entries = vec![
            normal_entry(
                1,
                GroupWriteCommand::from(CreateStreamRequest::new(
                    stream_id.clone(),
                    "application/octet-stream",
                )),
            ),
            normal_entry(
                2,
                GroupWriteCommand::from(AppendRequest::from_bytes(
                    stream_id.clone(),
                    b"abc".to_vec(),
                )),
            ),
        ];

        sm.apply(stream::iter(
            entries.into_iter().map(|entry| Ok((entry, None))),
        ))
        .await
        .expect("apply raft entries");

        let snapshot = sm.group_snapshot().await.expect("snapshot");
        assert_eq!(snapshot.group_commit_index, 2);
        assert_eq!(snapshot.stream_append_counts.len(), 1);
        assert_eq!(snapshot.stream_append_counts[0].append_count, 1);
    }

    #[tokio::test]
    async fn openraft_snapshot_round_trips_group_state() {
        let stream_id = ursula_shard::BucketStreamId::new("benchcmp", "raft-snapshot");
        let mut source = RaftGroupStateMachine::new(placement());
        let entries = vec![
            normal_entry(
                1,
                GroupWriteCommand::from(CreateStreamRequest::new(
                    stream_id.clone(),
                    "application/octet-stream",
                )),
            ),
            normal_entry(
                2,
                GroupWriteCommand::from(AppendRequest::from_bytes(stream_id, b"payload".to_vec())),
            ),
        ];
        source
            .apply(stream::iter(
                entries.into_iter().map(|entry| Ok((entry, None))),
            ))
            .await
            .expect("apply source");

        let mut builder = source.get_snapshot_builder().await;
        let snapshot = builder.build_snapshot().await.expect("build snapshot");

        let mut target = RaftGroupStateMachine::new(placement());
        target
            .install_snapshot(&snapshot.meta, snapshot.snapshot)
            .await
            .expect("install snapshot");

        let appended = target
            .engine
            .apply_committed_write(
                GroupWriteCommand::from(AppendRequest::from_bytes(
                    ursula_shard::BucketStreamId::new("benchcmp", "raft-snapshot"),
                    b"-next".to_vec(),
                )),
                placement(),
            )
            .expect("append after install");
        match appended {
            GroupWriteResponse::Append(response) => {
                assert_eq!(response.start_offset, 7);
                assert_eq!(response.stream_append_count, 2);
            }
            other => panic!("unexpected append response: {other:?}"),
        }
    }
}

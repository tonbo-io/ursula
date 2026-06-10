use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::fmt;
use std::io::Cursor;
use std::time::Duration;

use openraft::alias::VoteOf;
use openraft::raft::AppendEntriesRequest;
use openraft::raft::AppendEntriesResponse;
use openraft::raft::VoteRequest;
use openraft::raft::VoteResponse;
use prost::Message;
use serde::Deserialize;
use serde::Deserializer;
use serde::Serialize;
use serde::Serializer;
use ursula_proto as raft_app_proto;
use ursula_runtime::GroupWriteCommand;
use ursula_shard::RaftGroupId;

// Used only by the cfg(not(madsim)) file-log writer thread in
// log_store/file.rs::run_core_file_log_writer.
#[cfg_attr(madsim, allow(dead_code))]
pub(crate) const CORE_LOG_GROUP_COMMIT_DELAY: Duration = Duration::from_micros(200);
#[cfg_attr(madsim, allow(dead_code))]
pub(crate) const CORE_LOG_GROUP_COMMIT_MAX_BATCH: usize = 1024;

#[cfg(madsim)]
type OpenRaftRuntime = crate::sim_runtime::MadsimOpenRaftRuntime;
#[cfg(not(madsim))]
type OpenRaftRuntime = openraft::impls::TokioRuntime;

openraft::declare_raft_types!(
    pub UrsulaRaftTypeConfig:
        D = RaftGroupCommand,
        R = RaftGroupResponse,
        Node = openraft::BasicNode,
        SnapshotData = Cursor<Vec<u8>>,
        AsyncRuntime = OpenRaftRuntime,
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
    where S: Serializer {
        serializer.serialize_bytes(&self.0.encode_to_vec())
    }
}

impl<'de> Deserialize<'de> for RaftGroupCommand {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where D: Deserializer<'de> {
        let bytes = Vec::<u8>::deserialize(deserializer)?;
        let command = raft_app_proto::RaftGroupCommandV1::decode(bytes.as_slice())
            .map_err(serde::de::Error::custom)?;
        Ok(Self(command))
    }
}

impl Serialize for RaftGroupResponse {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where S: Serializer {
        serializer.serialize_bytes(&self.0.encode_to_vec())
    }
}

impl<'de> Deserialize<'de> for RaftGroupResponse {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where D: Deserializer<'de> {
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
            Some(raft_app_proto::raft_group_command_v1::Command::UpdateStreamAttrs(_)) => {
                "update_stream_attrs"
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
            Some(raft_app_proto::raft_group_command_v1::Command::AckColdGc(_)) => "ack_cold_gc",
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
                attrs,
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
                attrs_json: attrs.map(stream_attrs_json),
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
                attrs,
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
                attrs_json: attrs.map(stream_attrs_json),
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
            GroupWriteCommand::UpdateStreamAttrs {
                stream_id,
                attrs,
                now_ms,
            } => Command::UpdateStreamAttrs(raft_app_proto::UpdateStreamAttrsCommandV1 {
                stream_id: Some(stream_id.into()),
                attrs_json: attrs.map(stream_attrs_json),
                now_ms,
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
            GroupWriteCommand::AckColdGc { up_to_seq } => {
                Command::AckColdGc(raft_app_proto::AckColdGcCommandV1 { up_to_seq })
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

fn stream_attrs_json(attrs: ursula_runtime::StreamAttrs) -> Vec<u8> {
    serde_json::to_vec(&attrs).expect("stream attrs serialize to JSON")
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

/// Static gRPC Raft cluster membership configuration.
///
/// Used by the bootstrap layer when constructing a
/// [`StaticGrpcRaftGroupEngineFactory`].
#[derive(Debug, Clone, Default)]
pub struct StaticGrpcRaftMembershipConfig {
    pub initialize_membership_per_group: bool,
    pub per_group_voters: BTreeMap<RaftGroupId, BTreeSet<u64>>,
}

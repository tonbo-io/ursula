mod file;
mod memory;

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::io;

pub(crate) use file::CoreFileLogWriter;
pub use file::RaftGroupFileLogStore;
pub(crate) use file::elapsed_ns;
#[cfg(test)]
pub(crate) use file::read_protobuf_frames;
pub use memory::MemoryRaftLogStore;
pub use memory::MetaRaftLogStore;
pub use memory::RaftGroupLogStore;
use openraft::BasicNode;
use openraft::Entry;
use openraft::EntryPayload;
use openraft::Membership;
use openraft::RaftTypeConfig;
use openraft::StoredMembership;
use openraft::alias::EntryOf;
use openraft::alias::LogIdOf;
use openraft::alias::SnapshotMetaOf;
use openraft::alias::StoredMembershipOf;
use openraft::alias::VoteOf;
use openraft::entry::RaftEntry;
use openraft::raft::AppendEntriesResponse;
use openraft::raft::SnapshotResponse;
use openraft::vote::RaftLeaderId;

use crate::engine::invalid_data;
use crate::grpc::GrpcRpcError;
use crate::raft_internal_proto;
use crate::types::RaftGroupCommand;
use crate::types::UrsulaAppendEntriesRequest;
use crate::types::UrsulaAppendEntriesResponse;
use crate::types::UrsulaRaftTypeConfig;
use crate::types::UrsulaVoteRequest;
use crate::types::UrsulaVoteResponse;

#[derive(Debug, Clone, Default)]
pub(crate) struct MemoryRaftLogStoreInner<C>
where
    C: RaftTypeConfig,
    C::Entry: Clone,
{
    last_purged_log_id: Option<LogIdOf<C>>,
    committed: Option<LogIdOf<C>>,
    entries: BTreeMap<u64, EntryOf<C>>,
    vote: Option<VoteOf<C>>,
}

pub(crate) type RaftGroupLogStoreInner = MemoryRaftLogStoreInner<UrsulaRaftTypeConfig>;

pub(crate) type RaftGroupLogRecord = raft_internal_proto::RaftGroupLogRecordV1;
pub(crate) type CoreJournalRecord = raft_internal_proto::CoreJournalRecordV1;
pub(crate) type StoredLogEntry = raft_internal_proto::StoredLogEntryV1;

impl From<&EntryOf<UrsulaRaftTypeConfig>> for raft_internal_proto::StoredLogEntryV1 {
    fn from(entry: &EntryOf<UrsulaRaftTypeConfig>) -> Self {
        use raft_internal_proto::stored_log_entry_v1::Payload;

        let payload = match &entry.payload {
            EntryPayload::Blank => Payload::Blank(raft_internal_proto::BlankEntryV1 {}),
            EntryPayload::Normal(command) => Payload::Normal(Box::new(command.0.clone())),
            EntryPayload::Membership(membership) => {
                Payload::Membership(raft_internal_proto::MembershipEntryV1 {
                    configs: membership
                        .get_joint_config()
                        .iter()
                        .map(|config| raft_internal_proto::MembershipConfigV1 {
                            node_ids: config.iter().copied().collect(),
                        })
                        .collect(),
                    nodes: membership
                        .nodes()
                        .map(|(node_id, node)| raft_internal_proto::MembershipNodeV1 {
                            node_id: *node_id,
                            node: Some(raft_internal_proto::BasicNodeV1 {
                                addr: node.addr.clone(),
                            }),
                        })
                        .collect(),
                })
            }
        };
        Self {
            log_id: Some(log_id_to_proto(entry.log_id)),
            payload: Some(payload),
        }
    }
}

pub(crate) fn stored_log_entry_into_entry(
    entry: StoredLogEntry,
) -> Result<EntryOf<UrsulaRaftTypeConfig>, io::Error> {
    use raft_internal_proto::stored_log_entry_v1::Payload;

    let log_id = log_id_from_required_proto(entry.log_id, "stored log entry log_id")?;
    let payload = match entry.payload.ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "stored log entry missing payload",
        )
    })? {
        Payload::Blank(_) => EntryPayload::Blank,
        Payload::Normal(command) => EntryPayload::Normal(RaftGroupCommand(*command)),
        Payload::Membership(membership) => {
            let configs = membership
                .configs
                .into_iter()
                .map(|config| config.node_ids.into_iter().collect::<BTreeSet<_>>())
                .collect::<Vec<_>>();
            let nodes = membership
                .nodes
                .into_iter()
                .map(|node| {
                    let basic_node = node.node.ok_or_else(|| {
                        io::Error::new(
                            io::ErrorKind::InvalidData,
                            "membership node missing basic node",
                        )
                    })?;
                    Ok((node.node_id, BasicNode {
                        addr: basic_node.addr,
                    }))
                })
                .collect::<Result<BTreeMap<_, _>, io::Error>>()?;
            let membership = Membership::new(configs, nodes).map_err(invalid_data)?;
            EntryPayload::Membership(membership)
        }
    };
    Ok(Entry::new(log_id, payload))
}

pub(crate) fn log_id_to_proto(
    log_id: LogIdOf<UrsulaRaftTypeConfig>,
) -> raft_internal_proto::LogIdV1 {
    raft_internal_proto::LogIdV1 {
        term: log_id.leader_id.term(),
        node_id: *log_id.leader_id.node_id(),
        index: log_id.index,
    }
}

pub(crate) fn log_id_from_required_proto(
    log_id: Option<raft_internal_proto::LogIdV1>,
    field: &'static str,
) -> Result<LogIdOf<UrsulaRaftTypeConfig>, io::Error> {
    let log_id = log_id
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, format!("{field} is missing")))?;
    let leader_id = <UrsulaRaftTypeConfig as openraft::RaftTypeConfig>::LeaderId::new(
        log_id.term,
        log_id.node_id,
    );
    Ok(LogIdOf::<UrsulaRaftTypeConfig> {
        leader_id,
        index: log_id.index,
    })
}

pub(crate) fn vote_to_proto(vote: VoteOf<UrsulaRaftTypeConfig>) -> raft_internal_proto::VoteV1 {
    raft_internal_proto::VoteV1 {
        term: vote.leader_id.term(),
        node_id: *vote.leader_id.node_id(),
        committed: vote.committed,
    }
}

pub(crate) fn vote_from_required_proto(
    vote: Option<raft_internal_proto::VoteV1>,
) -> Result<VoteOf<UrsulaRaftTypeConfig>, io::Error> {
    let vote = vote.ok_or_else(|| {
        io::Error::new(io::ErrorKind::InvalidData, "save_vote record missing vote")
    })?;
    if vote.committed {
        Ok(VoteOf::<UrsulaRaftTypeConfig>::new_committed(
            vote.term,
            vote.node_id,
        ))
    } else {
        Ok(VoteOf::<UrsulaRaftTypeConfig>::new(vote.term, vote.node_id))
    }
}

pub(crate) fn membership_to_proto(
    membership: &Membership<u64, BasicNode>,
) -> raft_internal_proto::MembershipEntryV1 {
    raft_internal_proto::MembershipEntryV1 {
        configs: membership
            .get_joint_config()
            .iter()
            .map(|config| raft_internal_proto::MembershipConfigV1 {
                node_ids: config.iter().copied().collect(),
            })
            .collect(),
        nodes: membership
            .nodes()
            .map(|(node_id, node)| raft_internal_proto::MembershipNodeV1 {
                node_id: *node_id,
                node: Some(raft_internal_proto::BasicNodeV1 {
                    addr: node.addr.clone(),
                }),
            })
            .collect(),
    }
}

pub(crate) fn membership_from_proto(
    membership: raft_internal_proto::MembershipEntryV1,
) -> Result<Membership<u64, BasicNode>, io::Error> {
    let configs = membership
        .configs
        .into_iter()
        .map(|config| config.node_ids.into_iter().collect::<BTreeSet<_>>())
        .collect::<Vec<_>>();
    let nodes = membership
        .nodes
        .into_iter()
        .map(|node| {
            let basic_node = node.node.ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "membership node missing basic node",
                )
            })?;
            Ok((node.node_id, BasicNode {
                addr: basic_node.addr,
            }))
        })
        .collect::<Result<BTreeMap<_, _>, io::Error>>()?;
    Membership::new(configs, nodes).map_err(invalid_data)
}

pub(crate) fn stored_membership_to_proto(
    membership: StoredMembershipOf<UrsulaRaftTypeConfig>,
) -> raft_internal_proto::StoredMembershipV1 {
    raft_internal_proto::StoredMembershipV1 {
        log_id: membership
            .log_id()
            .as_ref()
            .map(|log_id| log_id_to_proto(*log_id)),
        membership: Some(membership_to_proto(membership.membership())),
    }
}

pub(crate) fn stored_membership_from_required_proto(
    membership: Option<raft_internal_proto::StoredMembershipV1>,
) -> Result<StoredMembershipOf<UrsulaRaftTypeConfig>, io::Error> {
    let membership = membership.ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "snapshot meta missing last_membership",
        )
    })?;
    let log_id = membership
        .log_id
        .map(|log_id| log_id_from_required_proto(Some(log_id), "stored membership log id"))
        .transpose()?;
    let inner = membership.membership.ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "stored membership missing membership",
        )
    })?;
    Ok(StoredMembership::new(log_id, membership_from_proto(inner)?))
}

pub(crate) fn snapshot_meta_to_proto(
    meta: SnapshotMetaOf<UrsulaRaftTypeConfig>,
) -> raft_internal_proto::SnapshotMetaV1 {
    raft_internal_proto::SnapshotMetaV1 {
        last_log_id: meta.last_log_id.map(log_id_to_proto),
        last_membership: Some(stored_membership_to_proto(meta.last_membership)),
        snapshot_id: meta.snapshot_id,
    }
}

pub(crate) fn snapshot_meta_from_required_proto(
    meta: Option<raft_internal_proto::SnapshotMetaV1>,
) -> Result<SnapshotMetaOf<UrsulaRaftTypeConfig>, io::Error> {
    let meta =
        meta.ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "snapshot meta is missing"))?;
    Ok(SnapshotMetaOf::<UrsulaRaftTypeConfig> {
        last_log_id: meta
            .last_log_id
            .map(|log_id| log_id_from_required_proto(Some(log_id), "snapshot last log id"))
            .transpose()?,
        last_membership: stored_membership_from_required_proto(meta.last_membership)?,
        snapshot_id: meta.snapshot_id,
    })
}

pub(crate) fn append_entries_request_to_proto(
    request: UrsulaAppendEntriesRequest,
) -> raft_internal_proto::RaftAppendEntriesRequestV1 {
    raft_internal_proto::RaftAppendEntriesRequestV1 {
        vote: Some(vote_to_proto(request.vote)),
        prev_log_id: request.prev_log_id.map(log_id_to_proto),
        entries: request.entries.iter().map(StoredLogEntry::from).collect(),
        leader_commit: request.leader_commit.map(log_id_to_proto),
    }
}

pub(crate) fn append_entries_request_from_proto(
    request: raft_internal_proto::raft_rpc_envelope_v1::Payload,
) -> Result<UrsulaAppendEntriesRequest, GrpcRpcError> {
    let raft_internal_proto::raft_rpc_envelope_v1::Payload::AppendEntries(request) = request else {
        return Err(GrpcRpcError::invalid_argument(
            "raft append envelope had wrong payload type",
        ));
    };
    let entries = request
        .entries
        .into_iter()
        .map(stored_log_entry_into_entry)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|err| GrpcRpcError::invalid_argument(format!("decode append entries: {err}")))?;
    Ok(UrsulaAppendEntriesRequest {
        vote: vote_from_required_proto(request.vote)
            .map_err(|err| GrpcRpcError::invalid_argument(err.to_string()))?,
        prev_log_id: request
            .prev_log_id
            .map(|log_id| log_id_from_required_proto(Some(log_id), "append prev_log_id"))
            .transpose()
            .map_err(|err| GrpcRpcError::invalid_argument(err.to_string()))?,
        entries,
        leader_commit: request
            .leader_commit
            .map(|log_id| log_id_from_required_proto(Some(log_id), "append leader_commit"))
            .transpose()
            .map_err(|err| GrpcRpcError::invalid_argument(err.to_string()))?,
    })
}

pub(crate) fn append_entries_response_to_proto(
    response: UrsulaAppendEntriesResponse,
) -> raft_internal_proto::RaftAppendEntriesResponseV1 {
    use raft_internal_proto::raft_append_entries_response_v1::Response;

    let response = match response {
        AppendEntriesResponse::Success => {
            Response::Success(raft_internal_proto::RaftAppendSuccessV1 {})
        }
        AppendEntriesResponse::PartialSuccess(log_id) => {
            Response::PartialSuccess(raft_internal_proto::RaftAppendPartialSuccessV1 {
                log_id: log_id.map(log_id_to_proto),
            })
        }
        AppendEntriesResponse::Conflict => {
            Response::Conflict(raft_internal_proto::RaftAppendConflictV1 {})
        }
        AppendEntriesResponse::HigherVote(vote) => {
            Response::HigherVote(raft_internal_proto::RaftAppendHigherVoteV1 {
                vote: Some(vote_to_proto(vote)),
            })
        }
    };
    raft_internal_proto::RaftAppendEntriesResponseV1 {
        response: Some(response),
    }
}

pub(crate) fn append_entries_response_from_proto(
    response: raft_internal_proto::RaftAppendEntriesResponseV1,
) -> Result<UrsulaAppendEntriesResponse, io::Error> {
    use raft_internal_proto::raft_append_entries_response_v1::Response;

    Ok(
        match response.response.ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "append entries response missing response",
            )
        })? {
            Response::Success(_) => AppendEntriesResponse::Success,
            Response::PartialSuccess(response) => AppendEntriesResponse::PartialSuccess(
                response
                    .log_id
                    .map(|log_id| {
                        log_id_from_required_proto(Some(log_id), "partial success log id")
                    })
                    .transpose()?,
            ),
            Response::Conflict(_) => AppendEntriesResponse::Conflict,
            Response::HigherVote(response) => {
                AppendEntriesResponse::HigherVote(vote_from_required_proto(response.vote)?)
            }
        },
    )
}

pub(crate) fn vote_request_to_proto(
    request: UrsulaVoteRequest,
) -> raft_internal_proto::RaftVoteRequestV1 {
    raft_internal_proto::RaftVoteRequestV1 {
        vote: Some(vote_to_proto(request.vote)),
        last_log_id: request.last_log_id.map(log_id_to_proto),
    }
}

pub(crate) fn vote_request_from_proto(
    request: raft_internal_proto::raft_rpc_envelope_v1::Payload,
) -> Result<UrsulaVoteRequest, GrpcRpcError> {
    let raft_internal_proto::raft_rpc_envelope_v1::Payload::Vote(request) = request else {
        return Err(GrpcRpcError::invalid_argument(
            "raft vote envelope had wrong payload type",
        ));
    };
    Ok(UrsulaVoteRequest {
        vote: vote_from_required_proto(request.vote)
            .map_err(|err| GrpcRpcError::invalid_argument(err.to_string()))?,
        last_log_id: request
            .last_log_id
            .map(|log_id| log_id_from_required_proto(Some(log_id), "vote last_log_id"))
            .transpose()
            .map_err(|err| GrpcRpcError::invalid_argument(err.to_string()))?,
    })
}

pub(crate) fn vote_response_to_proto(
    response: UrsulaVoteResponse,
) -> raft_internal_proto::RaftVoteResponseV1 {
    raft_internal_proto::RaftVoteResponseV1 {
        vote: Some(vote_to_proto(response.vote)),
        vote_granted: response.vote_granted,
        last_log_id: response.last_log_id.map(log_id_to_proto),
    }
}

pub(crate) fn vote_response_from_proto(
    response: raft_internal_proto::RaftVoteResponseV1,
) -> Result<UrsulaVoteResponse, io::Error> {
    Ok(UrsulaVoteResponse {
        vote: vote_from_required_proto(response.vote)?,
        vote_granted: response.vote_granted,
        last_log_id: response
            .last_log_id
            .map(|log_id| log_id_from_required_proto(Some(log_id), "vote response last_log_id"))
            .transpose()?,
    })
}

pub(crate) fn snapshot_response_to_proto(
    response: SnapshotResponse<UrsulaRaftTypeConfig>,
) -> raft_internal_proto::RaftSnapshotResponseV1 {
    raft_internal_proto::RaftSnapshotResponseV1 {
        vote: Some(vote_to_proto(response.vote)),
    }
}

pub(crate) fn snapshot_response_from_required_proto(
    response: Option<raft_internal_proto::RaftSnapshotResponseV1>,
) -> Result<SnapshotResponse<UrsulaRaftTypeConfig>, io::Error> {
    let response = response.ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "snapshot response missing response",
        )
    })?;
    Ok(SnapshotResponse::new(vote_from_required_proto(
        response.vote,
    )?))
}

pub(crate) fn save_vote_record(vote: VoteOf<UrsulaRaftTypeConfig>) -> RaftGroupLogRecord {
    use raft_internal_proto::raft_group_log_record_v1::Operation;

    RaftGroupLogRecord {
        operation: Some(Operation::SaveVote(raft_internal_proto::SaveVoteRecordV1 {
            vote: Some(vote_to_proto(vote)),
        })),
    }
}

pub(crate) fn save_committed_record(
    committed: Option<LogIdOf<UrsulaRaftTypeConfig>>,
) -> RaftGroupLogRecord {
    use raft_internal_proto::raft_group_log_record_v1::Operation;

    RaftGroupLogRecord {
        operation: Some(Operation::SaveCommitted(
            raft_internal_proto::SaveCommittedRecordV1 {
                committed: committed.map(log_id_to_proto),
            },
        )),
    }
}

pub(crate) fn append_record(entries: Vec<StoredLogEntry>) -> RaftGroupLogRecord {
    use raft_internal_proto::raft_group_log_record_v1::Operation;

    RaftGroupLogRecord {
        operation: Some(Operation::Append(raft_internal_proto::AppendRecordV1 {
            entries,
        })),
    }
}

pub(crate) fn truncate_after_record(
    last_log_id: Option<LogIdOf<UrsulaRaftTypeConfig>>,
) -> RaftGroupLogRecord {
    use raft_internal_proto::raft_group_log_record_v1::Operation;

    RaftGroupLogRecord {
        operation: Some(Operation::TruncateAfter(
            raft_internal_proto::TruncateAfterRecordV1 {
                last_log_id: last_log_id.map(log_id_to_proto),
            },
        )),
    }
}

pub(crate) fn purge_record(log_id: LogIdOf<UrsulaRaftTypeConfig>) -> RaftGroupLogRecord {
    use raft_internal_proto::raft_group_log_record_v1::Operation;

    RaftGroupLogRecord {
        operation: Some(Operation::Purge(raft_internal_proto::PurgeRecordV1 {
            log_id: Some(log_id_to_proto(log_id)),
        })),
    }
}

pub(crate) fn ensure_consecutive_entries<C>(entries: &[EntryOf<C>]) -> Result<(), io::Error>
where
    C: RaftTypeConfig,
    C::Entry: Clone,
{
    for pair in entries.windows(2) {
        let current = pair[0].log_id().index;
        let next = pair[1].log_id().index;
        if next != current + 1 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("raft log entries are not consecutive: {current} then {next}"),
            ));
        }
    }
    Ok(())
}

pub(crate) fn ensure_log_append_boundary<C>(
    inner: &MemoryRaftLogStoreInner<C>,
    entries: &[EntryOf<C>],
) -> Result<(), io::Error>
where
    C: RaftTypeConfig,
    C::Entry: Clone,
{
    let Some(first_entry) = entries.first() else {
        return Ok(());
    };
    let Some(last_existing_index) = inner.entries.keys().next_back().copied() else {
        return Ok(());
    };

    let first_append_index = first_entry.log_id().index;
    if first_append_index > last_existing_index + 1 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("raft log store has a hole: {last_existing_index} then {first_append_index}"),
        ));
    }

    Ok(())
}

pub(crate) fn ensure_consecutive_log<C>(
    entries: &BTreeMap<u64, EntryOf<C>>,
) -> Result<(), io::Error>
where
    C: RaftTypeConfig,
    C::Entry: Clone,
{
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

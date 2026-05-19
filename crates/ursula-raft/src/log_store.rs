use std::io::Write;
use std::io::Read;
use openraft::vote::RaftLeaderId;
use openraft::entry::RaftEntry;
use std::ops::RangeBounds;
use openraft::storage::RaftLogStorage;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::fmt::Debug;
use std::fs;
use std::fs::File;
use std::fs::OpenOptions;
use std::io;
use std::io::Cursor;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::MutexGuard;
use std::sync::mpsc;
use std::time::Instant;

use openraft::BasicNode;
use openraft::Entry;
use openraft::EntryPayload;
use openraft::Membership;
use openraft::OptionalSend;
use openraft::StoredMembership;
use openraft::alias::EntryOf;
use openraft::alias::LogIdOf;
use openraft::alias::SnapshotMetaOf;
use openraft::alias::StoredMembershipOf;
use openraft::alias::VoteOf;
use openraft::raft::AppendEntriesResponse;
use openraft::raft::SnapshotResponse;
use openraft::storage::IOFlushed;
use openraft::storage::LogState;
use openraft::storage::RaftLogReader;
use prost::Message;
use ursula_runtime::GroupEngineMetrics;
use ursula_shard::ShardPlacement;

use crate::engine::*;
use crate::grpc::*;
use crate::raft_internal_proto;
use crate::types::*;

#[derive(Debug, Clone, Default)]
pub(crate) struct RaftGroupLogStoreInner {
    last_purged_log_id: Option<LogIdOf<UrsulaRaftTypeConfig>>,
    committed: Option<LogIdOf<UrsulaRaftTypeConfig>>,
    entries: BTreeMap<u64, EntryOf<UrsulaRaftTypeConfig>>,
    vote: Option<VoteOf<UrsulaRaftTypeConfig>>,
}

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
                    Ok((
                        node.node_id,
                        BasicNode {
                            addr: basic_node.addr,
                        },
                    ))
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
            Ok((
                node.node_id,
                BasicNode {
                    addr: basic_node.addr,
                },
            ))
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

    pub(crate) fn lock_inner(&self) -> Result<MutexGuard<'_, RaftGroupLogStoreInner>, io::Error> {
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
pub(crate) struct RaftGroupFileLogStoreMetrics {
    placement: ShardPlacement,
    metrics: GroupEngineMetrics,
}

#[derive(Debug)]
pub(crate) struct RaftGroupFileLogHandle {
    file: Option<File>,
    parent_needs_sync: bool,
}

#[derive(Debug)]
pub(crate) struct CoreFileLogWriter {
    journal_path: PathBuf,
    tx: mpsc::Sender<CoreFileLogWrite>,
}

#[derive(Debug)]
pub(crate) struct CoreFileLogWrite {
    group_id: u32,
    record: RaftGroupLogRecord,
    response_tx: mpsc::Sender<Result<CoreFileLogWriteTiming, String>>,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct CoreFileLogWriteTiming {
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

    pub(crate) fn open_with_core_writer(
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

    pub(crate) fn open_inner(
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

    pub(crate) fn shared_with_core_writer(
        path: impl Into<PathBuf>,
        placement: ShardPlacement,
        metrics: GroupEngineMetrics,
        core_writer: Arc<CoreFileLogWriter>,
    ) -> Result<Arc<Self>, io::Error> {
        Self::open_with_core_writer(path, placement, metrics, core_writer).map(Arc::new)
    }

    pub(crate) fn lock_inner(&self) -> Result<MutexGuard<'_, RaftGroupLogStoreInner>, io::Error> {
        self.inner
            .lock()
            .map_err(|_| io::Error::other("raft group file log store mutex poisoned"))
    }

    pub(crate) fn lock_file(&self) -> Result<MutexGuard<'_, RaftGroupFileLogHandle>, io::Error> {
        self.file
            .lock()
            .map_err(|_| io::Error::other("raft group file log store file mutex poisoned"))
    }

    pub(crate) fn append_record_locked(
        &self,
        record: &RaftGroupLogRecord,
    ) -> Result<(), io::Error> {
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
    pub(crate) fn shared(journal_path: impl Into<PathBuf>) -> Result<Arc<Self>, io::Error> {
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

    pub(crate) fn journal_path(&self) -> &Path {
        &self.journal_path
    }

    pub(crate) fn append(
        &self,
        group_id: u32,
        record: RaftGroupLogRecord,
    ) -> Result<(u64, u64), io::Error> {
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

pub(crate) fn run_core_file_log_writer(
    journal_path: PathBuf,
    rx: mpsc::Receiver<CoreFileLogWrite>,
) {
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

pub(crate) fn write_core_log_batch(
    journal_path: &Path,
    journal_handle: &mut RaftGroupFileLogHandle,
    batch: &[CoreFileLogWrite],
) -> Result<CoreFileLogWriteTiming, io::Error> {
    let write_started_at = Instant::now();
    for request in batch {
        let journal_record = CoreJournalRecord {
            group_id: request.group_id,
            record: Some(request.record.clone()),
        };
        write_protobuf_frame_to_file(journal_path, journal_handle, &journal_record)?;
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
            store.append_record_locked(&save_vote_record(vote))?;
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
            store.append_record_locked(&save_committed_record(committed))?;
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

            let record = append_record(entries.iter().map(StoredLogEntry::from).collect());
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
            store.append_record_locked(&truncate_after_record(last_log_id))?;
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
            store.append_record_locked(&purge_record(log_id))?;
            *inner = next;
            Ok(())
        })
        .await
    }
}

pub(crate) async fn spawn_log_store_blocking<T>(
    f: impl FnOnce() -> Result<T, io::Error> + Send + 'static,
) -> Result<T, io::Error>
where
    T: Send + 'static,
{
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|err| io::Error::other(format!("join OpenRaft file log task: {err}")))?
}

pub(crate) fn load_log_store_inner(path: &Path) -> Result<RaftGroupLogStoreInner, io::Error> {
    if !path.exists() {
        return Ok(RaftGroupLogStoreInner::default());
    }

    let bytes = fs::read(path)?;
    let mut inner = RaftGroupLogStoreInner::default();
    for (record_index, record) in read_protobuf_frames::<RaftGroupLogRecord>(&bytes)?
        .into_iter()
        .enumerate()
    {
        apply_log_store_record(&mut inner, record).map_err(|err| {
            io::Error::new(
                err.kind(),
                format!(
                    "replay OpenRaft log record '{}' record {}: {err}",
                    path.display(),
                    record_index + 1
                ),
            )
        })?;
    }
    Ok(inner)
}

pub(crate) fn load_log_store_inner_from_core_journal(
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
    for (record_index, record) in read_protobuf_frames::<CoreJournalRecord>(&bytes)?
        .into_iter()
        .enumerate()
    {
        if record.group_id != placement.raft_group_id.0 {
            continue;
        }
        let record_payload = record.record.ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "OpenRaft core journal record '{}' record {} missing payload",
                    journal_path.display(),
                    record_index + 1
                ),
            )
        })?;
        apply_log_store_record(&mut inner, record_payload).map_err(|err| {
            io::Error::new(
                err.kind(),
                format!(
                    "replay OpenRaft core journal record '{}' record {}: {err}",
                    journal_path.display(),
                    record_index + 1
                ),
            )
        })?;
    }
    Ok(inner)
}

pub(crate) fn append_log_store_record(
    path: &Path,
    handle: &mut RaftGroupFileLogHandle,
    record: &RaftGroupLogRecord,
) -> Result<(u64, u64), io::Error> {
    let write_started_at = Instant::now();
    write_protobuf_frame_to_file(path, handle, record)?;
    let write_ns = elapsed_ns(write_started_at);

    let sync_started_at = Instant::now();
    sync_file_handle(path, handle)?;
    Ok((write_ns, elapsed_ns(sync_started_at)))
}

pub(crate) fn write_protobuf_frame_to_file<M: Message>(
    path: &Path,
    handle: &mut RaftGroupFileLogHandle,
    value: &M,
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
    let bytes = value.encode_to_vec();
    let len = u32::try_from(bytes.len()).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "OpenRaft journal record too large",
        )
    })?;
    file.write_all(&len.to_le_bytes())?;
    file.write_all(&bytes)
}

pub(crate) fn read_protobuf_frames<M: Message + Default>(
    bytes: &[u8],
) -> Result<Vec<M>, io::Error> {
    if bytes.is_empty() {
        return Ok(Vec::new());
    }

    let mut cursor = Cursor::new(bytes);
    let mut records = Vec::new();
    while usize::try_from(cursor.position()).expect("cursor position fits usize") < bytes.len() {
        let mut len_bytes = [0_u8; 4];
        cursor.read_exact(&mut len_bytes)?;
        let len = usize::try_from(u32::from_le_bytes(len_bytes)).expect("u32 fits usize");
        let start = usize::try_from(cursor.position()).expect("cursor position fits usize");
        let end = start.checked_add(len).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "OpenRaft journal frame length overflow",
            )
        })?;
        if end > bytes.len() {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "OpenRaft journal frame extends past end of file",
            ));
        }
        records.push(M::decode(&bytes[start..end]).map_err(invalid_data)?);
        cursor.set_position(u64::try_from(end).expect("frame end fits u64"));
    }
    Ok(records)
}

pub(crate) fn sync_file_handle(
    path: &Path,
    handle: &mut RaftGroupFileLogHandle,
) -> Result<(), io::Error> {
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

pub(crate) fn raft_group_log_record_count(record: &RaftGroupLogRecord) -> usize {
    match &record.operation {
        Some(raft_internal_proto::raft_group_log_record_v1::Operation::Append(append)) => {
            append.entries.len()
        }
        Some(_) => 1,
        None => 0,
    }
}

pub(crate) fn elapsed_ns(started_at: Instant) -> u64 {
    u64::try_from(started_at.elapsed().as_nanos()).unwrap_or(u64::MAX)
}

pub(crate) fn apply_log_store_record(
    inner: &mut RaftGroupLogStoreInner,
    record: RaftGroupLogRecord,
) -> Result<(), io::Error> {
    use raft_internal_proto::raft_group_log_record_v1::Operation;

    match record.operation.ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "OpenRaft log record missing operation",
        )
    })? {
        Operation::SaveVote(record) => {
            inner.vote = Some(vote_from_required_proto(record.vote)?);
            Ok(())
        }
        Operation::SaveCommitted(record) => {
            inner.committed = record
                .committed
                .map(|log_id| log_id_from_required_proto(Some(log_id), "committed log id"))
                .transpose()?;
            Ok(())
        }
        Operation::Append(record) => {
            let entries = record
                .entries
                .into_iter()
                .map(stored_log_entry_into_entry)
                .collect::<Result<Vec<_>, _>>()?;
            ensure_consecutive_entries(&entries)?;
            for entry in entries {
                inner.entries.insert(entry.log_id.index, entry);
            }
            ensure_consecutive_log(&inner.entries)
        }
        Operation::TruncateAfter(record) => {
            let last_log_id = record
                .last_log_id
                .map(|log_id| log_id_from_required_proto(Some(log_id), "truncate_after log id"))
                .transpose()?;
            let start_index = last_log_id.map_or(0, |log_id| log_id.index + 1);
            inner.entries.retain(|index, _| *index < start_index);
            Ok(())
        }
        Operation::Purge(record) => {
            let log_id = log_id_from_required_proto(record.log_id, "purge log id")?;
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

pub(crate) fn ensure_consecutive_entries(
    entries: &[EntryOf<UrsulaRaftTypeConfig>],
) -> Result<(), io::Error> {
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

pub(crate) fn ensure_log_append_boundary(
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

pub(crate) fn ensure_consecutive_log(
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

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::io::Cursor;
use std::time::Duration;

use openraft::alias::VoteOf;
use openraft::raft::AppendEntriesRequest;
use openraft::raft::AppendEntriesResponse;
use openraft::raft::VoteRequest;
use openraft::raft::VoteResponse;
use serde::Deserialize;
use serde::Serialize;
use ursula_runtime::GroupEngineError;
use ursula_runtime::GroupWriteCommand;
use ursula_runtime::GroupWriteResponse;
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
        D = GroupWriteCommand,
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

/// Raft-level response for one applied log entry. Write outcomes carry the
/// canonical [`GroupWriteResponse`]/[`GroupEngineError`] directly; blank and
/// membership entries have no application payload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RaftGroupResponse {
    Blank,
    Membership,
    Write(Result<GroupWriteResponse, GroupEngineError>),
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

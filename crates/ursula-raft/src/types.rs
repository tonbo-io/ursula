use std::time::Duration;

use openraft::alias::SnapshotOf;
use openraft::alias::VoteOf;
use openraft::raft::AppendEntriesRequest;
use openraft::raft::AppendEntriesResponse;
use openraft::raft::VoteRequest;
use openraft::raft::VoteResponse;
use serde::Deserialize;
use serde::Serialize;
use ursula_runtime::GroupEngineError;
use ursula_runtime::GroupWriteResponse;
use ursula_stream::StreamCommand;

use crate::meta::MetaCommand;
use crate::meta::MetaResponse;

/// Under `fsync = always`, how long the per-core journal writer waits for the
/// next request before it writes a batch; each arrival restarts the wait
/// (`log_store/writer.rs::collect_batch`).
pub(crate) const CORE_LOG_GROUP_COMMIT_DELAY: Duration = Duration::from_micros(200);
/// Under `fsync = always`, the longest a batch keeps collecting requests,
/// counted from its first.
pub(crate) const CORE_LOG_GROUP_COMMIT_MAX_DELAY: Duration = Duration::from_millis(1);
/// The most requests the per-core journal writer puts in one batch.
pub(crate) const CORE_LOG_GROUP_COMMIT_MAX_BATCH: usize = 1024;

#[cfg(madsim)]
type OpenRaftRuntime = crate::sim_runtime::MadsimOpenRaftRuntime;
#[cfg(not(madsim))]
type OpenRaftRuntime = openraft::impls::TokioRuntime;

openraft::declare_raft_types!(
    pub UrsulaRaftTypeConfig:
        D = RaftCommand,
        R = RaftGroupResponse,
        Node = openraft::EmptyNode,
        AsyncRuntime = OpenRaftRuntime,
);

/// The command of one Raft entry, on either plane: a data group's
/// [`StreamCommand`] or a meta group's [`MetaCommand`]. One type config
/// serves both planes, so the journal, the gate, the network and the codec
/// are shared, and the plane a request names admits only its own commands
/// (`RaftTarget::admits`).
///
/// A data entry encodes as a one-variant map, `81 a6 "Stream"` followed by
/// its [`StreamCommand`]: the variant is written by its name, so the bytes
/// of a data entry are those of its command under that one key.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RaftCommand {
    Stream(StreamCommand),
    Meta(MetaCommand),
}

impl RaftCommand {
    /// Approximate Raft-log bytes of this command (bounded-state F12e). A
    /// meta command counts as the fixed per-entry overhead.
    pub fn log_bytes_estimate(&self) -> u64 {
        match self {
            Self::Stream(command) => command.log_bytes_estimate(),
            Self::Meta(_) => ursula_stream::COMMAND_LOG_OVERHEAD_BYTES,
        }
    }
}

/// The command's name in logs, as OpenRaft requires of an entry payload.
impl std::fmt::Display for RaftCommand {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Stream(command) => std::fmt::Display::fmt(command, f),
            Self::Meta(command) => std::fmt::Display::fmt(command, f),
        }
    }
}

impl From<StreamCommand> for RaftCommand {
    fn from(command: StreamCommand) -> Self {
        Self::Stream(command)
    }
}

impl From<MetaCommand> for RaftCommand {
    fn from(command: MetaCommand) -> Self {
        Self::Meta(command)
    }
}

/// A group snapshot with its metadata, as the state machine, its snapshot
/// builder and the networks hand it to OpenRaft.
pub type UrsulaSnapshot = SnapshotOf<UrsulaRaftTypeConfig, SnapshotData>;

/// What a group snapshot carries through OpenRaft: the encoded
/// [`ursula_runtime::SnapshotPointer`], and a hold on the object it names
/// for as long as OpenRaft keeps the snapshot. The snapshot a replica hands
/// OpenRaft to send leases its object ([`SnapshotLease`]), and the network
/// keeps the lease until the RPC returns, so the object exists while the
/// receiver reads it. A received snapshot holds nothing on the receiver.
#[derive(Debug, Clone)]
pub struct SnapshotData {
    pointer_bytes: Vec<u8>,
    lease: Option<SnapshotLease>,
}

pub use crate::snapshot_holds::SnapshotLease;

impl SnapshotData {
    /// The snapshot data of `pointer_bytes`, holding nothing.
    pub fn new(pointer_bytes: Vec<u8>) -> Self {
        Self {
            pointer_bytes,
            lease: None,
        }
    }

    /// The snapshot data of `pointer_bytes` that holds the object it names
    /// with `lease`, if it names one.
    pub(crate) fn with_lease(pointer_bytes: Vec<u8>, lease: Option<SnapshotLease>) -> Self {
        Self {
            pointer_bytes,
            lease,
        }
    }

    /// The encoded snapshot pointer.
    pub fn pointer_bytes(&self) -> &[u8] {
        &self.pointer_bytes
    }

    /// The encoded snapshot pointer and the hold on the object it names.
    /// The caller keeps the hold for as long as the pointer is in use, as
    /// until the RPC that sends it returns.
    #[must_use = "the lease keeps the named object while the pointer is in use"]
    pub fn into_parts(self) -> (Vec<u8>, Option<SnapshotLease>) {
        (self.pointer_bytes, self.lease)
    }
}

/// The Raft log bytes an entry is counted as: what the snapshot cadence
/// adds when the entry is applied, what it weighs in the WAL's entry cache,
/// and what it counts toward the limit of a log read, which bounds each
/// Append call to a follower (#507).
pub(crate) fn entry_log_bytes(entry: &openraft::alias::EntryOf<UrsulaRaftTypeConfig>) -> u64 {
    match &entry.payload {
        openraft::EntryPayload::Normal(command) => command.log_bytes_estimate(),
        openraft::EntryPayload::Blank | openraft::EntryPayload::Membership(_) => {
            ursula_stream::COMMAND_LOG_OVERHEAD_BYTES
        }
    }
}

pub type UrsulaAppendEntriesRequest = AppendEntriesRequest<UrsulaRaftTypeConfig>;
pub type UrsulaAppendEntriesResponse = AppendEntriesResponse<UrsulaRaftTypeConfig>;
pub type UrsulaVote = VoteOf<UrsulaRaftTypeConfig>;
pub type UrsulaVoteRequest = VoteRequest<UrsulaRaftTypeConfig>;
pub type UrsulaVoteResponse = VoteResponse<UrsulaRaftTypeConfig>;

/// The round of an election an inbound vote request belongs to. Both carry
/// a [`UrsulaVoteRequest`], travel the same transport and pass the same
/// screens (the node fence, the recovery gate); only what the replica's
/// Raft does with a request that passes differs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VoteRound {
    /// A real vote: a grant saves the candidate's vote.
    Vote,
    /// A pre-vote (`Config::enable_pre_vote`): whether the replica would
    /// grant the vote, answered without saving anything. A candidate
    /// raises its term only once a quorum would grant it, so a replica
    /// that cannot win, one restarted before its leader's first heartbeat
    /// reached it or one cut off from its peers, leaves the term alone.
    PreVote,
}

impl VoteRound {
    /// The RPC's name, in transport errors and logs.
    pub(crate) fn rpc(self) -> &'static str {
        match self {
            Self::Vote => "Vote",
            Self::PreVote => "PreVote",
        }
    }
}

/// Raft-level response for one applied log entry. A data group's write
/// outcomes carry the canonical [`GroupWriteResponse`]/[`GroupEngineError`]
/// directly, the meta group's its [`MetaResponse`]; blank and membership
/// entries have no application payload. Never persisted or sent: an arm
/// reaches the wire only as an `ursula_runtime::ResponseKind`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RaftGroupResponse {
    Blank,
    Membership,
    Write(Result<GroupWriteResponse, GroupEngineError>),
    Meta(MetaResponse),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct RaftLogProgressSnapshot {
    pub term: u64,
    pub index: u64,
    /// The node that led the entry's term (#613): the entry's full id with
    /// the two above.
    pub node_id: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RaftGroupMetricsSnapshot {
    #[serde(default)]
    pub apply_failure: Option<crate::apply_failure::ApplyFailure>,
    pub raft_group_id: u32,
    pub node_id: u64,
    pub current_term: u64,
    /// The leader the replica names ([`crate::leadership::named_leader`]):
    /// itself, or another node only while that node's AppendEntries reach
    /// it. `None` for a stopped replica.
    pub current_leader: Option<u64>,
    /// The replica's last log id, `None` for an empty log.
    pub last_log: Option<RaftLogProgressSnapshot>,
    pub committed: Option<RaftLogProgressSnapshot>,
    pub last_applied: Option<RaftLogProgressSnapshot>,
    pub snapshot: Option<RaftLogProgressSnapshot>,
    pub purged: Option<RaftLogProgressSnapshot>,
    pub voter_ids: Vec<u64>,
    pub voter_configurations: Vec<std::collections::BTreeSet<u64>>,
    /// The voter configurations of the membership the replica keeps when a
    /// recovery plan cuts its log back to its committed entry (#613). A
    /// record from before #613 has none, so the planner finds no replica it
    /// may elect with a cut: only versions of one minor read each other's
    /// records.
    #[serde(default)]
    pub committed_voter_configurations: Vec<std::collections::BTreeSet<u64>>,
    pub learner_ids: Vec<u64>,
    /// Process-local participation and effective-membership status. Missing
    /// fields deserialize closed; an old metrics record is not a health proof.
    #[serde(default)]
    pub maintenance: RaftGroupMaintenanceState,
    /// Log applied since this replica's last snapshot (F12e).
    #[serde(default)]
    pub log: crate::snapshot_cadence::GroupLogProgress,
    /// The history event this replica applied last (#581 section 4.3);
    /// `None` before the group's genesis.
    pub history: Option<ursula_proto::admin::RaftGroupHistory>,
    /// The recovery plan this replica holds until its history event
    /// applies (#581 section 4.5).
    pub recovery: Option<ursula_proto::admin::RaftGroupRecovery>,
    /// The replica's recovery gate for this group.
    pub recovery_gate: ursula_proto::admin::RecoveryGateStatus,
    /// The replica leads with a quorum that acknowledged it lately.
    pub leads_live: bool,
    /// What the replica's membership reconcile shell does, `None` for a
    /// replica that runs none.
    pub reconcile: Option<ursula_proto::placement::ReconcileStatus>,
    /// The replica lost its vote and holds no vote floor yet, so it refuses
    /// every recovery plan.
    pub needs_vote_floor: bool,
}

impl RaftGroupMetricsSnapshot {
    /// The replica's metrics as the admin JSON of `GET /__ursula/raft`
    /// renders them, and as `ursulactl recover-group` reads them.
    pub fn to_admin(&self) -> ursula_proto::admin::RaftGroupMetrics {
        ursula_proto::admin::RaftGroupMetrics {
            raft_group_id: u64::from(self.raft_group_id),
            node_id: self.node_id,
            current_term: self.current_term,
            current_leader: self.current_leader,
            committed_index: self.committed.map(|progress| progress.index),
            last_applied_index: self.last_applied.map(|progress| progress.index),
            voter_ids: self.voter_ids.clone(),
            voter_configs: self.voter_configurations.clone(),
            committed_voter_configs: self.committed_voter_configurations.clone(),
            learner_ids: self.learner_ids.clone(),
            maintenance: self.maintenance.clone(),
            last_log_index: self.last_log.map(|progress| progress.index),
            last_log_term: self.last_log.map(|progress| progress.term),
            committed_term: self.committed.map(|progress| progress.term),
            committed_node: self.committed.map(|progress| progress.node_id),
            last_applied_term: self.last_applied.map(|progress| progress.term),
            snapshot_term: self.snapshot.map(|progress| progress.term),
            snapshot_index: self.snapshot.map(|progress| progress.index),
            purged_term: self.purged.map(|progress| progress.term),
            purged_index: self.purged.map(|progress| progress.index),
            log_bytes_since_snapshot: self.log.log_bytes,
            log_entries_since_snapshot: self.log.log_entries,
            last_snapshot_bytes: self.log.last_snapshot_bytes,
            has_snapshot: self.log.has_snapshot,
            apply_failure: self.apply_failure.clone(),
            history: self.history,
            recovery: self.recovery,
            recovery_gate: self.recovery_gate,
            leads_live: self.leads_live,
            reconcile: self.reconcile.clone(),
            needs_vote_floor: self.needs_vote_floor,
        }
    }
}

pub use ursula_proto::admin::RaftGroupMaintenanceState;

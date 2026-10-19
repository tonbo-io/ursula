//! The group engine's error, [`GroupEngineError`]: a cross-crate,
//! serializable protocol value. It crosses the Raft RPC as named
//! MessagePack in the answer of a forwarded `GroupRead` and `GroupWrite`, so
//! within a minor its wire form, and that of every type it carries, is
//! frozen (`crates/ursula-raft/src/fixtures/group-engine-errors-epoch5.json`
//! shows any change). It carries no free text a caller acts on: every
//! distinction a caller acts on is a variant, and [`GroupEngineError::status`],
//! [`GroupEngineError::write_effect`], [`GroupEngineError::not_leader`] and
//! [`GroupEngineError::peer_backs_off`] classify it once, by exhaustive
//! matches.
//!
//! Native sources (I/O, OpenRaft, tonic) stay in local boundary types; the
//! one conversion of each family into this error logs the source.
//!
//! No type in the frozen set has a catch-all variant, with one exception:
//! `ApplyStopped` carries `ursula_proto::admin::ApplyFailureKind`, whose
//! `#[serde(other)] Unknown` is the admin plane's fallback for another
//! minor's metrics. Its kinds are frozen within a minor like every other
//! type here.
//!
//! Module map:
//!
//! - [`snapshot`]: [`GroupSnapshotError`], the refusals of a snapshot that
//!   crosses the engine boundary.

#[cfg(any(test, feature = "test-support"))]
pub mod samples;
mod snapshot;

use std::fmt;

use serde::Deserialize;
use serde::Serialize;
pub use snapshot::GroupSnapshotError;
pub use snapshot::SnapshotObjectCheck;
pub use snapshot::SnapshotStoreOperation;
use ursula_shard::BucketStreamId;
use ursula_shard::CoreId;
use ursula_shard::RaftGroupId;
use ursula_stream::StreamCommand;
use ursula_stream::StreamErrorCode;
use ursula_stream::StreamErrorContext;
use ursula_stream::StreamResponse;

use crate::error::ErrorStatus;

/// The engine's error: a deterministic refusal of the state machine, a
/// refusal for this replica's leadership, or a failure of the group's
/// infrastructure.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
pub enum GroupEngineError {
    /// The state machine refused the command. Every replica refuses it
    /// alike, and nothing of it applied.
    #[error(transparent)]
    Stream(#[from] StreamRefusal),
    /// This replica does not lead the group, or could not confirm that it
    /// does.
    #[error(transparent)]
    NotLeader(#[from] LeadershipRefusal),
    #[error(transparent)]
    Infra(#[from] GroupInfraError),
}

/// A deterministic refusal of the stream state machine.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
#[error("{code:?}: {text}")]
pub struct StreamRefusal {
    pub code: StreamErrorCode,
    /// What the state machine said about the request. It names nothing but
    /// what the request named, and does not repeat the code.
    pub text: String,
    pub next_offset: Option<u64>,
    pub context: Vec<StreamErrorContext>,
}

/// A refusal for this replica's leadership of the group.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
#[error("leadership refusal ({reason:?}, {proposal:?})")]
pub struct LeadershipRefusal {
    /// The leader to redirect to: known only with the term in which it
    /// leads, read with it from one view. `None` while no leader is known,
    /// and when the refusing replica itself is named (a step-down window).
    pub leader: Option<KnownLeader>,
    pub proposal: Proposal,
    pub reason: LeadershipReason,
}

/// A group leader and the term in which it leads.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KnownLeader {
    pub node_id: u64,
    pub term: u64,
    /// The leader's Raft address, when the refusing replica knew it.
    pub address: Option<String>,
}

/// Whether a refused command may have been proposed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Proposal {
    /// Refused before anything was proposed (RT1): the command did not
    /// commit.
    NotProposed,
    /// Refused after the command was handed to Raft: it may have committed.
    MayHaveCommitted,
}

/// Why a replica refused for its leadership.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum LeadershipReason {
    /// The replica's own leadership check found that it does not lead.
    Follower,
    /// OpenRaft found that the replica no longer leads: it refused the
    /// command before appending it, or discarded it after it took it.
    SteppedDown,
    /// The leader is handing its leadership to another replica. The target
    /// does not lead yet, so the refusal names no leader.
    LeadershipTransfer,
    /// A leader could not confirm its leadership with a quorum, as when no
    /// quorum acknowledged it within its lease.
    QuorumUnconfirmed,
    /// A leader did not apply the read index in time.
    ReadIndexBehind,
    /// The replica could not reach the leader it forwards to.
    LeaderUnreachable,
    /// The leader the replica forwards to is in another cluster.
    LeaderInOtherCluster,
}

/// What a failed write did, which decides whether its effects may be
/// undone and what its answer says.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteEffect {
    /// Refused before its command was proposed: nothing of it applied.
    NotProposed,
    /// The state machine refused it: nothing of it applied on any replica.
    Refused,
    /// It may have been proposed, and may have committed.
    Unknown,
}

/// A command the data-group apply dispatcher cannot execute. The proposal
/// boundary refuses it before anything is written, because once committed it
/// would stop every replica of the group.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum UnproposableCommand {
    /// Stream commands create their bucket inside their own apply.
    CreateBucket,
    /// Only a leader's history driver proposes a history event.
    NewHistory,
}

/// Serializable startup classification. The WAL facade retains the native
/// source chain; the engine boundary logs it before converting to this type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WalOpenFailureKind {
    Io,
    Corrupt,
    IncompatibleFormat,
    Locked,
    DuplicateGroup,
    Stopped,
    InvalidConfiguration,
    InvalidRecord,
    Poisoned,
}

/// A Raft log id as a replica's log store holds it: the term and node of
/// the leader that wrote the entry, and its index.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogPosition {
    pub term: u64,
    pub node_id: u64,
    pub index: u64,
}

/// The kind of an engine response: what a caller expected, and what it got
/// instead ([`GroupInfraError::UnexpectedResponse`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ResponseKind {
    CreateStream,
    Append,
    PublishSnapshot,
    AdvanceRetention,
    TouchStreamAccess,
    FlushCold,
    CompactCold,
    CloseStream,
    DeleteStream,
    AckColdGc,
    PurgeBucket,
    ImportGroupState,
    NewHistory,
    TidyStream,
    DeferColdGc,
    OffloadColdRefs,
    ExpireStreams,
    /// A state-machine answer that no group write answers.
    BucketCreated,
    BucketAlreadyExists,
    StreamError,
    /// OpenRaft's answer to a blank entry.
    RaftBlank,
    /// OpenRaft's answer to a membership entry.
    RaftMembership,
    /// No answer at all.
    Missing,
}

impl ResponseKind {
    /// The kind of response the state machine answers `command` with when
    /// it applies it.
    pub fn answering(command: &StreamCommand) -> Self {
        match command {
            StreamCommand::CreateBucket { .. } => Self::BucketCreated,
            StreamCommand::CreateStream { .. } | StreamCommand::CreateExternal { .. } => {
                Self::CreateStream
            }
            StreamCommand::Append { .. } | StreamCommand::AppendExternal { .. } => Self::Append,
            StreamCommand::PublishSnapshot { .. }
            | StreamCommand::PublishSnapshotExternal { .. } => Self::PublishSnapshot,
            StreamCommand::AdvanceRetention { .. } => Self::AdvanceRetention,
            StreamCommand::TouchStreamAccess { .. } => Self::TouchStreamAccess,
            StreamCommand::FlushCold { .. } => Self::FlushCold,
            StreamCommand::CompactCold { .. } => Self::CompactCold,
            StreamCommand::Close { .. } => Self::CloseStream,
            StreamCommand::DeleteStream { .. } => Self::DeleteStream,
            StreamCommand::AckColdGc { .. } => Self::AckColdGc,
            StreamCommand::DeferColdGc { .. } => Self::DeferColdGc,
            StreamCommand::PurgeBucket { .. } => Self::PurgeBucket,
            StreamCommand::ImportSnapshot { .. } => Self::ImportGroupState,
            StreamCommand::NewHistory { .. } => Self::NewHistory,
            StreamCommand::TidyStream { .. } => Self::TidyStream,
            StreamCommand::OffloadColdRefs { .. } => Self::OffloadColdRefs,
            StreamCommand::ExpireStreams { .. } => Self::ExpireStreams,
            StreamCommand::IfIncarnation { command, .. } => Self::answering(command),
        }
    }
}

impl From<&StreamResponse> for ResponseKind {
    fn from(response: &StreamResponse) -> Self {
        match response {
            StreamResponse::BucketCreated { .. } => Self::BucketCreated,
            StreamResponse::BucketAlreadyExists { .. } => Self::BucketAlreadyExists,
            StreamResponse::Created { .. } | StreamResponse::AlreadyExists { .. } => {
                Self::CreateStream
            }
            StreamResponse::Appended { .. } => Self::Append,
            StreamResponse::Closed { .. } => Self::CloseStream,
            StreamResponse::Deleted { .. } => Self::DeleteStream,
            StreamResponse::ColdFlushed { .. } => Self::FlushCold,
            StreamResponse::ColdCompacted { .. } => Self::CompactCold,
            StreamResponse::SnapshotPublished { .. } => Self::PublishSnapshot,
            StreamResponse::RetentionAdvanced { .. } => Self::AdvanceRetention,
            StreamResponse::Accessed { .. } => Self::TouchStreamAccess,
            StreamResponse::ColdGcAcked { .. } => Self::AckColdGc,
            StreamResponse::ColdGcDeferred { .. } => Self::DeferColdGc,
            StreamResponse::BucketPurged { .. } => Self::PurgeBucket,
            StreamResponse::SnapshotImported { .. } => Self::ImportGroupState,
            StreamResponse::HistoryStarted { .. } => Self::NewHistory,
            StreamResponse::StreamTidied { .. } => Self::TidyStream,
            StreamResponse::ColdRefsOffloaded { .. } => Self::OffloadColdRefs,
            StreamResponse::StreamsExpired { .. } => Self::ExpireStreams,
            StreamResponse::Error { .. } => Self::StreamError,
        }
    }
}

/// An engine operation a [`crate::GroupEngine`] may leave unimplemented.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum EngineOperation {
    CreateStreamExternal,
    AppendExternal,
    PublishSnapshot,
    AdvanceRetention,
    ReadSnapshot,
    BootstrapStream,
    ImportGroupState,
    StateGauges,
    TidyStream,
    AckColdGc,
    DeferColdGc,
    PurgeBucket,
    FlushCold,
    CompactCold,
    PlanColdFlush,
    PlanGroupColdFlush,
    ColdHotBacklog,
}

/// The cold-tier operation that failed ([`GroupInfraError::ColdStore`]).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ColdOperation {
    /// Reading a stream's payload from a cold object.
    ReadPayload { stream_id: BucketStreamId },
    /// Refreshing a cached cold-index page of a stream.
    RefreshIndexPage { stream_id: BucketStreamId },
    /// Indexing an offloaded external object of a stream.
    IndexExternalObject { stream_id: BucketStreamId },
    /// Repairing the cold index of the group's streams.
    RepairIndex,
    /// Indexing a flushed chunk of a stream.
    IndexFlushedChunk { stream_id: BucketStreamId },
    /// Indexing a compaction's replacement chunk of a stream.
    IndexCompactedChunk { stream_id: BucketStreamId },
    /// Rolling back a stream's cold-index pages after its write failed.
    RollbackIndex { stream_id: BucketStreamId },
}

impl fmt::Display for ColdOperation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ReadPayload { stream_id } => write!(f, "payload read of stream {stream_id}"),
            Self::RefreshIndexPage { stream_id } => {
                write!(f, "index page refresh of stream {stream_id}")
            }
            Self::IndexExternalObject { stream_id } => {
                write!(f, "external object indexing of stream {stream_id}")
            }
            Self::RepairIndex => f.write_str("index repair"),
            Self::IndexFlushedChunk { stream_id } => {
                write!(f, "flushed chunk indexing of stream {stream_id}")
            }
            Self::IndexCompactedChunk { stream_id } => {
                write!(f, "compacted chunk indexing of stream {stream_id}")
            }
            Self::RollbackIndex { stream_id } => {
                write!(f, "index rollback of stream {stream_id}")
            }
        }
    }
}

/// The leader RPCs a follower forwards.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ForwardedRpc {
    /// A read or HEAD the leader serves.
    GroupRead,
    /// A bucket purge the leader proposes.
    GroupWrite,
}

impl ForwardedRpc {
    /// The RPC's name on the Raft gRPC plane.
    pub fn route(self) -> &'static str {
        match self {
            Self::GroupRead => "GroupRead",
            Self::GroupWrite => "GroupWrite",
        }
    }
}

impl fmt::Display for ForwardedRpc {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.route())
    }
}

/// Why a forwarded leader RPC got no answer of the leader's group, other
/// than an unreachable leader ([`LeadershipReason::LeaderUnreachable`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ForwardFailure {
    /// The replica runs without the Raft gRPC transport: nothing was sent.
    NoIdentity,
    /// The leader's address is no valid gRPC endpoint: nothing was sent.
    InvalidEndpoint,
    /// The leader refused the call with this gRPC status code.
    Refused { grpc_code: i32 },
}

/// What is wrong with a forwarded RPC's answer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AnswerProblem {
    /// The answer does not decode.
    Undecodable,
    /// A forwarded write answered this many results for its one command.
    ResultCount { results: u64 },
}

/// What is wrong with a group's static Raft membership.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum StaticMembershipProblem {
    /// The per-group voter config does not name the group.
    MissingGroup,
    /// The group's static voter set is empty.
    NoVoters,
    /// A voter of the group has no static peer address.
    VoterWithoutPeer { node_id: u64 },
    /// This node has no static peer address.
    NodeWithoutPeer { node_id: u64 },
}

/// Why OpenRaft refused to initialize a group's membership.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MembershipInitRefusal {
    /// The group's log or vote is not empty.
    NotAllowed,
    /// This node is not a member of the membership it proposed.
    NotInMembers,
}

/// Infra error variants carry structured fields and render their human
/// message on demand through `Display`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
pub enum GroupInfraError {
    #[error(
        "recovery gate node {actual} differs from engine node {expected} for {raft_group_id:?}"
    )]
    RecoveryNodeMismatch {
        raft_group_id: RaftGroupId,
        expected: u64,
        actual: u64,
    },
    #[error("raft group {raft_group_id:?} is not registered on this node")]
    RaftGroupNotRegistered { raft_group_id: RaftGroupId },
    #[error("recovery gate for group {raft_group_id:?} is already bound")]
    RecoveryAlreadyBound { raft_group_id: RaftGroupId },
    #[error("recovery gate group {actual:?} differs from engine group {expected:?}")]
    RecoveryGroupMismatch {
        expected: RaftGroupId,
        actual: RaftGroupId,
    },
    #[error(
        "raft group {raft_group_id:?} stopped after a committed apply failure; retain WAL for corrected-code replay"
    )]
    ApplyStopped {
        raft_group_id: RaftGroupId,
        term: u64,
        index: u64,
        kind: ursula_proto::admin::ApplyFailureKind,
    },
    #[error("command {command:?} cannot be proposed to a data Raft group")]
    InvalidRaftCommand { command: UnproposableCommand },
    #[error("open WAL for raft group {raft_group_id:?} on core {core_id:?}: {failure:?}")]
    WalOpen {
        core_id: CoreId,
        raft_group_id: RaftGroupId,
        failure: WalOpenFailureKind,
    },
    #[error("raft group {raft_group_id:?} has not established its recovery vote floor")]
    RecoveryVoteFloor { raft_group_id: RaftGroupId },
    /// This replica restarts to run a recovery plan's cut (#581 section
    /// 4.5): it takes no replication until it has. Retryable.
    #[error("raft group {raft_group_id:?} restarts to cut its log for a recovery plan")]
    RecoveryCutting { raft_group_id: RaftGroupId },
    /// The sender's entries differ from this replica's at or below the entry
    /// it knows is committed (#613): taking them would overwrite committed
    /// entries. Raft never sends that; a recovery plan led from a replica
    /// that knew less would. The replica takes nothing from the sender.
    #[error(
        "raft group {raft_group_id:?}: the sender's entries differ from this replica's at index \
         {index}, at or below the committed index {committed}"
    )]
    CommittedOverwrite {
        raft_group_id: RaftGroupId,
        committed: u64,
        index: u64,
    },
    /// OpenRaft committed or applied entry `committed`, above the last entry
    /// `held` that this replica's log store holds: the store has not yet
    /// recorded an install OpenRaft admitted (#613). The commit screen cannot
    /// compare anything until it has, so the replica takes nothing from the
    /// sender meanwhile. Retryable.
    #[error(
        "raft group {raft_group_id:?}: committed index {committed} is above the last entry \
         {held:?} its log store holds; replication waits until the store catches up"
    )]
    LogBehindCommit {
        raft_group_id: RaftGroupId,
        committed: u64,
        held: Option<u64>,
    },
    /// At or after its restored snapshot's index, this replica's log holds
    /// an entry, or its commit marker names one, `held`, that is below the
    /// snapshot in log-id order (R1). Such an entry cannot be committed. The
    /// replica refuses to open the group, which stays closed on it until it
    /// rejoins as a wiped node.
    #[error(
        "raft group {raft_group_id:?} on core {core_id:?}: its log holds {held:?} below its \
         restored snapshot {snapshot:?}"
    )]
    LogBelowSnapshot {
        core_id: CoreId,
        raft_group_id: RaftGroupId,
        snapshot: LogPosition,
        held: LogPosition,
    },
    /// A snapshot of history `history`, which replaced history `replaced`,
    /// while this replica's commit marker lies in history `local` (`None`
    /// when its log could not be read): nothing shows that the snapshot
    /// kept this replica's committed entries (#613). The replica takes
    /// nothing from the sender; it rejoins as a wiped node.
    #[error(
        "raft group {raft_group_id:?}: a snapshot of history {history:#x}, which replaced \
         {replaced:#x}, does not continue this replica's committed history {local:?}"
    )]
    UnrelatedHistory {
        raft_group_id: RaftGroupId,
        history: u64,
        replaced: u64,
        local: Option<u64>,
    },
    /// This replica holds a recovery plan and the sender leads at or below
    /// the plan's term: a leader elected before the plan, whose entries it
    /// takes no more (#581 section 4.5). Retryable for the sender, which a
    /// leader under the plan replaces.
    #[error(
        "raft group {raft_group_id:?} holds a recovery plan with plan term {plan_term}; it takes \
         no replication from a leader at or below it"
    )]
    RecoveryPlanFenced {
        raft_group_id: RaftGroupId,
        plan_term: u64,
    },
    /// This replica leads the group, and the group's history is pending: its
    /// genesis, or the history event of a recovery plan, has not applied
    /// (#581 sections 4.3 and 4.5). The leader serves and admits nothing but
    /// the event until it does. Retryable.
    #[error(
        "raft group {raft_group_id:?} waits for its history event; its leader serves nothing before it"
    )]
    RecoveryPending { raft_group_id: RaftGroupId },
    #[error("the Raft owner has stopped")]
    OwnerStopped,
    /// A group engine's shutdown waited `waited_ms` for the work OpenRaft
    /// runs for the engine on tasks of its own (its state-machine worker,
    /// the snapshot builds it spawned, its network clients and log
    /// readers), and some of it still runs (#709). The group does not open
    /// again in this process until that work ended.
    #[error(
        "raft group {raft_group_id:?}: OpenRaft work of the stopped engine still runs after {waited_ms} ms"
    )]
    EngineWorkOutlivedShutdown {
        raft_group_id: RaftGroupId,
        waited_ms: u64,
    },
    /// The OpenRaft work of the group's previous engine on this node still
    /// runs (#709): the group does not open while that work may still change
    /// its state. Refused at once, before any engine starts. Retryable.
    #[error(
        "raft group {raft_group_id:?} does not open: OpenRaft work of its previous engine still runs"
    )]
    PreviousEngineWorkRunning { raft_group_id: RaftGroupId },
    #[error("write outcome is unknown; the proposal may have committed")]
    OutcomeUnknown,
    /// A read or write of the cold tier failed. The store's error is logged
    /// where it is converted ([`cold_store_failure`]).
    #[error("cold tier {operation} failed")]
    ColdStore { operation: ColdOperation },
    #[error(
        "ColdBackpressure: stream '{stream_id}' would raise group hot bytes from {before_group_hot_bytes} to {after_group_hot_bytes}, above limit {limit}"
    )]
    ColdBackpressure {
        stream_id: BucketStreamId,
        before_group_hot_bytes: u64,
        after_group_hot_bytes: u64,
        limit: u64,
    },
    #[error(
        "RaftUncommittedBackpressure: group uncommitted bytes {current} plus incoming {incoming} would exceed limit {limit}"
    )]
    RaftUncommittedBackpressure {
        current: u64,
        incoming: u64,
        limit: u64,
    },
    /// The engine answered a command with a response of another kind: an
    /// invariant of the engine broke.
    #[error("expected a {expected:?} response, got {actual:?}")]
    UnexpectedResponse {
        expected: ResponseKind,
        actual: ResponseKind,
    },
    /// The engine answered a response that names a stream for a command that
    /// names none.
    #[error("{response:?} response for a command without a stream id")]
    MissingStreamId { response: ResponseKind },
    /// The engine does not implement the operation. Nothing was proposed.
    #[error("{operation:?} is not supported by this engine")]
    Unsupported { operation: EngineOperation },
    /// A bootstrap window that should lie wholly in the hot tier does not.
    #[error("bootstrap window [{start}..{end}) for stream '{stream_id}' is not hot")]
    BootstrapWindowNotHot {
        stream_id: BucketStreamId,
        start: u64,
        end: u64,
    },
    /// A compaction's input no longer matches the stream's cold index (a
    /// concurrent change won). Nothing was proposed.
    #[error("cold compaction input of stream '{stream_id}' no longer matches the cold index")]
    ColdCompactionStale { stream_id: BucketStreamId },
    /// A forwarded leader RPC got no answer of the leader's group.
    #[error("forwarded {rpc} failed: {failure:?}")]
    ForwardFailed {
        rpc: ForwardedRpc,
        failure: ForwardFailure,
    },
    /// A forwarded leader RPC's answer is not one this replica can use.
    #[error("forwarded {rpc} answered invalidly: {problem:?}")]
    ForwardedAnswerInvalid {
        rpc: ForwardedRpc,
        problem: AnswerProblem,
    },
    /// The group's Raft timing config does not validate.
    #[error(
        "invalid Raft config: heartbeat {heartbeat_ms} ms, election timeout \
         {election_timeout_min_ms}..{election_timeout_max_ms} ms"
    )]
    InvalidRaftConfig {
        heartbeat_ms: u64,
        election_timeout_min_ms: u64,
        election_timeout_max_ms: u64,
    },
    /// The group's static Raft membership is unusable.
    #[error("static membership of raft group {raft_group_id:?}: {problem:?}")]
    StaticMembership {
        raft_group_id: RaftGroupId,
        problem: StaticMembershipProblem,
    },
    /// The group's recovery gate did not open.
    #[error(
        "open the recovery gate of raft group {raft_group_id:?} on core {core_id:?}: {failure:?}"
    )]
    RecoveryGateOpen {
        core_id: CoreId,
        raft_group_id: RaftGroupId,
        failure: WalOpenFailureKind,
    },
    /// OpenRaft refused to initialize the group's membership.
    #[error("initialize the membership of raft group {raft_group_id:?}: {refusal:?}")]
    MembershipInit {
        raft_group_id: RaftGroupId,
        refusal: MembershipInitRefusal,
    },
    /// The group observed no expected leader in time.
    #[error("raft group {raft_group_id:?} observed no expected leader within {timeout_ms} ms")]
    LeaderWaitTimedOut {
        raft_group_id: RaftGroupId,
        timeout_ms: u64,
    },
    #[error(transparent)]
    Snapshot(#[from] GroupSnapshotError),
}

/// [`GroupEngineError`] fits clippy's `result_large_err` bound, so every
/// `Result` of the engine stays small.
const _: () = assert!(size_of::<GroupEngineError>() <= 128);

/// A cold-tier failure of `operation` as the group's error. The store's
/// error is logged here, once, and stays out of the error.
pub fn cold_store_failure(operation: ColdOperation, source: impl fmt::Display) -> GroupEngineError {
    tracing::warn!(%operation, error = %source, "cold tier operation failed");
    GroupEngineError::Infra(GroupInfraError::ColdStore { operation })
}

impl GroupInfraError {
    pub fn cold_backpressure(
        stream_id: BucketStreamId,
        before_group_hot_bytes: u64,
        after_group_hot_bytes: u64,
        limit: u64,
    ) -> Self {
        Self::ColdBackpressure {
            stream_id,
            before_group_hot_bytes,
            after_group_hot_bytes,
            limit,
        }
    }

    pub fn raft_uncommitted_backpressure(current: u64, incoming: u64, limit: u64) -> Self {
        Self::RaftUncommittedBackpressure {
            current,
            incoming,
            limit,
        }
    }

    pub fn is_cold_backpressure(&self) -> bool {
        matches!(self, Self::ColdBackpressure { .. })
    }

    /// See [`GroupEngineError::status`].
    pub fn status(&self) -> ErrorStatus {
        match self {
            Self::ColdBackpressure { .. }
            | Self::RaftUncommittedBackpressure { .. }
            | Self::RecoveryPending { .. }
            | Self::EngineWorkOutlivedShutdown { .. }
            | Self::PreviousEngineWorkRunning { .. } => ErrorStatus::Temporary,
            Self::Snapshot(error) => error.status(),
            Self::RecoveryNodeMismatch { .. }
            | Self::RaftGroupNotRegistered { .. }
            | Self::RecoveryAlreadyBound { .. }
            | Self::RecoveryGroupMismatch { .. }
            | Self::ApplyStopped { .. }
            | Self::InvalidRaftCommand { .. }
            | Self::WalOpen { .. }
            | Self::LogBelowSnapshot { .. }
            | Self::RecoveryVoteFloor { .. }
            | Self::RecoveryCutting { .. }
            | Self::CommittedOverwrite { .. }
            | Self::LogBehindCommit { .. }
            | Self::UnrelatedHistory { .. }
            | Self::RecoveryPlanFenced { .. }
            | Self::OwnerStopped
            | Self::OutcomeUnknown
            | Self::ColdStore { .. }
            | Self::UnexpectedResponse { .. }
            | Self::MissingStreamId { .. }
            | Self::Unsupported { .. }
            | Self::BootstrapWindowNotHot { .. }
            | Self::ColdCompactionStale { .. }
            | Self::ForwardFailed { .. }
            | Self::ForwardedAnswerInvalid { .. }
            | Self::InvalidRaftConfig { .. }
            | Self::StaticMembership { .. }
            | Self::RecoveryGateOpen { .. }
            | Self::MembershipInit { .. }
            | Self::LeaderWaitTimedOut { .. } => ErrorStatus::Persistent,
        }
    }

    /// See [`GroupEngineError::write_effect`]. The runtime refuses before
    /// proposing when the group's unapplied bytes or hot bytes are over
    /// their limits, when the leader's history is pending, when the engine
    /// lacks the operation, and when a compaction's input went stale.
    pub fn write_effect(&self) -> WriteEffect {
        match self {
            Self::ColdBackpressure { .. }
            | Self::RaftUncommittedBackpressure { .. }
            | Self::RecoveryPending { .. }
            | Self::Unsupported { .. }
            | Self::ColdCompactionStale { .. }
            // No engine of the group runs on the node.
            | Self::EngineWorkOutlivedShutdown { .. }
            | Self::PreviousEngineWorkRunning { .. } => WriteEffect::NotProposed,
            Self::Snapshot(error) => error.write_effect(),
            Self::RecoveryNodeMismatch { .. }
            | Self::RaftGroupNotRegistered { .. }
            | Self::RecoveryAlreadyBound { .. }
            | Self::RecoveryGroupMismatch { .. }
            | Self::ApplyStopped { .. }
            | Self::InvalidRaftCommand { .. }
            | Self::WalOpen { .. }
            | Self::LogBelowSnapshot { .. }
            | Self::RecoveryVoteFloor { .. }
            | Self::RecoveryCutting { .. }
            | Self::CommittedOverwrite { .. }
            | Self::LogBehindCommit { .. }
            | Self::UnrelatedHistory { .. }
            | Self::RecoveryPlanFenced { .. }
            | Self::OwnerStopped
            | Self::OutcomeUnknown
            | Self::ColdStore { .. }
            | Self::UnexpectedResponse { .. }
            | Self::MissingStreamId { .. }
            | Self::BootstrapWindowNotHot { .. }
            | Self::ForwardFailed { .. }
            | Self::ForwardedAnswerInvalid { .. }
            | Self::InvalidRaftConfig { .. }
            | Self::StaticMembership { .. }
            | Self::RecoveryGateOpen { .. }
            | Self::MembershipInit { .. }
            | Self::LeaderWaitTimedOut { .. } => WriteEffect::Unknown,
        }
    }

    /// See [`GroupEngineError::peer_backs_off`].
    pub fn peer_backs_off(&self) -> bool {
        match self {
            Self::RecoveryVoteFloor { .. }
            | Self::RecoveryCutting { .. }
            | Self::CommittedOverwrite { .. }
            | Self::UnrelatedHistory { .. }
            | Self::LogBehindCommit { .. }
            | Self::RecoveryPlanFenced { .. }
            | Self::OwnerStopped
            | Self::ApplyStopped { .. }
            | Self::OutcomeUnknown
            | Self::EngineWorkOutlivedShutdown { .. }
            | Self::PreviousEngineWorkRunning { .. } => true,
            Self::Snapshot(error) => error.peer_backs_off(),
            Self::RecoveryNodeMismatch { .. }
            | Self::RaftGroupNotRegistered { .. }
            | Self::RecoveryAlreadyBound { .. }
            | Self::RecoveryGroupMismatch { .. }
            | Self::InvalidRaftCommand { .. }
            | Self::WalOpen { .. }
            | Self::LogBelowSnapshot { .. }
            | Self::RecoveryPending { .. }
            | Self::ColdStore { .. }
            | Self::ColdBackpressure { .. }
            | Self::RaftUncommittedBackpressure { .. }
            | Self::UnexpectedResponse { .. }
            | Self::MissingStreamId { .. }
            | Self::Unsupported { .. }
            | Self::BootstrapWindowNotHot { .. }
            | Self::ColdCompactionStale { .. }
            | Self::ForwardFailed { .. }
            | Self::ForwardedAnswerInvalid { .. }
            | Self::InvalidRaftConfig { .. }
            | Self::StaticMembership { .. }
            | Self::RecoveryGateOpen { .. }
            | Self::MembershipInit { .. }
            | Self::LeaderWaitTimedOut { .. } => false,
        }
    }
}

impl GroupEngineError {
    pub fn cold_backpressure(
        stream_id: BucketStreamId,
        before_group_hot_bytes: u64,
        after_group_hot_bytes: u64,
        limit: u64,
    ) -> Self {
        Self::Infra(GroupInfraError::cold_backpressure(
            stream_id,
            before_group_hot_bytes,
            after_group_hot_bytes,
            limit,
        ))
    }

    pub fn raft_uncommitted_backpressure(current: u64, incoming: u64, limit: u64) -> Self {
        Self::Infra(GroupInfraError::raft_uncommitted_backpressure(
            current, incoming, limit,
        ))
    }

    pub fn stream(code: StreamErrorCode, text: impl Into<String>) -> Self {
        Self::stream_with_next_offset(code, text, None)
    }

    pub fn stream_with_next_offset(
        code: StreamErrorCode,
        text: impl Into<String>,
        next_offset: Option<u64>,
    ) -> Self {
        Self::stream_with_context(code, text, next_offset, vec![])
    }

    pub fn stream_with_context(
        code: StreamErrorCode,
        text: impl Into<String>,
        next_offset: Option<u64>,
        context: Vec<StreamErrorContext>,
    ) -> Self {
        Self::Stream(StreamRefusal {
            code,
            text: text.into(),
            next_offset,
            context,
        })
    }

    pub fn code(&self) -> Option<StreamErrorCode> {
        match self {
            Self::Stream(err) => Some(err.code),
            Self::Infra(_) | Self::NotLeader(_) => None,
        }
    }

    /// A stream refusal's own text: what the state machine said about the
    /// request, which names nothing but what the request named. `None` for
    /// every other error.
    pub fn refusal_text(&self) -> Option<&str> {
        match self {
            Self::Stream(err) => Some(&err.text),
            Self::Infra(_) | Self::NotLeader(_) => None,
        }
    }

    pub fn next_offset(&self) -> Option<u64> {
        match self {
            Self::Stream(err) => err.next_offset,
            Self::Infra(_) | Self::NotLeader(_) => None,
        }
    }

    pub fn context(&self) -> &[StreamErrorContext] {
        match self {
            Self::Stream(err) => &err.context,
            Self::Infra(_) | Self::NotLeader(_) => &[],
        }
    }

    /// The leadership refusal this error is, if it is one. A read that
    /// gets one redirects to its leader, or answers `NO_LEADER` while none
    /// is known.
    pub fn not_leader(&self) -> Option<&LeadershipRefusal> {
        match self {
            Self::NotLeader(refusal) => Some(refusal),
            Self::Stream(_) | Self::Infra(_) => None,
        }
    }

    pub fn infra(&self) -> Option<&GroupInfraError> {
        match self {
            Self::Infra(err) => Some(err),
            Self::Stream(_) | Self::NotLeader(_) => None,
        }
    }

    pub fn is_cold_backpressure(&self) -> bool {
        self.infra()
            .is_some_and(GroupInfraError::is_cold_backpressure)
    }

    /// Whether a retry soon may succeed (`Retry-After`): a leadership
    /// refusal, a backpressure refusal, a pending history. A stream refusal
    /// is permanent, every other failure persistent.
    pub fn status(&self) -> ErrorStatus {
        match self {
            Self::Stream(refusal) => stream_refusal_status(refusal.code),
            Self::NotLeader(_) => ErrorStatus::Temporary,
            Self::Infra(error) => error.status(),
        }
    }

    /// What a failed write did. Refusals before anything was proposed (the
    /// engine's leadership check, the core worker's check of the group's
    /// unapplied bytes, the cold-tier admission, a pending history, and
    /// OpenRaft's forward before it appends) are [`WriteEffect::NotProposed`].
    /// An entry OpenRaft appended and then discarded may still commit
    /// through another leader (RT1), so it is [`WriteEffect::Unknown`]. It
    /// classifies a write's error only: a read's refusal proposes nothing
    /// either way.
    pub fn write_effect(&self) -> WriteEffect {
        match self {
            Self::Stream(_) => WriteEffect::Refused,
            Self::NotLeader(refusal) => match refusal.proposal {
                Proposal::NotProposed => WriteEffect::NotProposed,
                Proposal::MayHaveCommitted => WriteEffect::Unknown,
            },
            Self::Infra(error) => error.write_effect(),
        }
    }

    /// Whether nothing of the failed write applied, so its staged effects
    /// (a cold chunk, its index pages, an external payload) may be undone.
    pub fn applied_nothing(&self) -> bool {
        match self.write_effect() {
            WriteEffect::NotProposed | WriteEffect::Refused => true,
            WriteEffect::Unknown => false,
        }
    }

    /// Whether the runtime refused the write before it proposed the write's
    /// command, so the command never reached the Raft log. The answer to a
    /// leadership refusal says it: a redirect or `NO_LEADER` only when this
    /// holds.
    pub fn refused_before_proposal(&self) -> bool {
        self.write_effect() == WriteEffect::NotProposed
    }

    /// Whether a Raft peer that sent the refused request should back off
    /// before it sends again: the gRPC plane answers it `UNAVAILABLE`.
    pub fn peer_backs_off(&self) -> bool {
        match self {
            Self::Stream(_) | Self::NotLeader(_) => false,
            Self::Infra(error) => error.peer_backs_off(),
        }
    }
}

/// The status of a stream refusal: permanent, but for a write that raced
/// its group's genesis, which applied nothing and soon succeeds.
fn stream_refusal_status(code: StreamErrorCode) -> ErrorStatus {
    match code {
        StreamErrorCode::HistoryPending => ErrorStatus::Temporary,
        StreamErrorCode::InvalidBucketId
        | StreamErrorCode::InvalidStreamId
        | StreamErrorCode::BucketNotFound
        | StreamErrorCode::BucketErased
        | StreamErrorCode::StreamNotFound
        | StreamErrorCode::StreamGone
        | StreamErrorCode::StreamAlreadyExistsConflict
        | StreamErrorCode::MissingContentType
        | StreamErrorCode::ContentTypeMismatch
        | StreamErrorCode::EmptyAppend
        | StreamErrorCode::StreamClosed
        | StreamErrorCode::StreamSeqConflict
        | StreamErrorCode::InvalidProducer
        | StreamErrorCode::ProducerEpochClosed
        | StreamErrorCode::ProducerExpired
        | StreamErrorCode::ProducerEpochStale
        | StreamErrorCode::ProducerKeyConflict
        | StreamErrorCode::ProducerDigestMismatch
        | StreamErrorCode::ProducerSeqConflict
        | StreamErrorCode::InvalidRetention
        | StreamErrorCode::OffsetOutOfRange
        | StreamErrorCode::InvalidColdFlush
        | StreamErrorCode::InvalidSnapshot
        | StreamErrorCode::SnapshotNotFound
        | StreamErrorCode::SnapshotConflict
        | StreamErrorCode::InvalidRecordBoundaries
        | StreamErrorCode::JsonBoundaryUnverified
        | StreamErrorCode::ImportConflict
        | StreamErrorCode::ImportInvalid
        | StreamErrorCode::ProducerLimit
        | StreamErrorCode::CreatedAtOutOfRange
        | StreamErrorCode::IncarnationMismatch
        | StreamErrorCode::HistoryMismatch => ErrorStatus::Permanent,
    }
}

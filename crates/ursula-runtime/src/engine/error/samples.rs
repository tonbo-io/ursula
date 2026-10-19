//! One sample of every variant of [`GroupEngineError`] and of every type it
//! carries, for the tests that pin its wire form (the byte fixture of
//! `ursula-raft`) and its classification (`status`, `write_effect`, the HTTP
//! answer, the gRPC code). Each list is built behind an exhaustive match
//! with no wildcard ([`every`]), so a new variant does not compile until it
//! has a sample.

use ursula_proto::admin::ApplyFailureKind;
use ursula_shard::BucketStreamId;
use ursula_shard::CoreId;
use ursula_shard::RaftGroupId;
use ursula_stream::ClosedBy;
use ursula_stream::ClosedEpoch;
use ursula_stream::ProducerRefusal;
use ursula_stream::StreamErrorCode;
use ursula_stream::StreamErrorContext;
use ursula_stream::StreamSnapshotError;

use super::AnswerProblem;
use super::ColdOperation;
use super::EngineOperation;
use super::ForwardFailure;
use super::ForwardedRpc;
use super::GroupEngineError;
use super::GroupInfraError;
use super::GroupSnapshotError;
use super::KnownLeader;
use super::LeadershipReason;
use super::LeadershipRefusal;
use super::LogPosition;
use super::MembershipInitRefusal;
use super::Proposal;
use super::ResponseKind;
use super::SnapshotObjectCheck;
use super::SnapshotStoreOperation;
use super::StaticMembershipProblem;
use super::StreamRefusal;
use super::UnproposableCommand;
use super::WalOpenFailureKind;

/// `[value, ...]` for one value per listed pattern, which must cover every
/// variant of `$ty`: the patterns form a match with no wildcard, and each
/// value must match its own pattern.
macro_rules! every {
    ($ty:ty { $($pattern:pat => $value:expr),+ $(,)? }) => {{
        let values: Vec<$ty> = vec![$({
            let value: $ty = $value;
            assert!(matches!(&value, $pattern), "a sample of another variant");
            value
        }),+];
        for value in &values {
            match value {
                $($pattern => {})+
            }
        }
        values
    }};
}

pub(crate) use every;

/// [`every`] for an enum of unit variants.
macro_rules! every_unit {
    ($ty:ident { $($variant:ident),+ $(,)? }) => {
        every!($ty { $($ty::$variant => $ty::$variant),+ })
    };
}

fn stream() -> BucketStreamId {
    BucketStreamId::new("bucket", "stream")
}

const GROUP: RaftGroupId = RaftGroupId(7);

/// Every sample once, in a fixed order.
pub fn group_engine_errors() -> Vec<GroupEngineError> {
    // One witness per top-level variant, each standing for its family.
    let witnesses = every!(GroupEngineError {
        GroupEngineError::Stream(_) => GroupEngineError::stream(StreamErrorCode::StreamClosed, "w"),
        GroupEngineError::NotLeader(_) => GroupEngineError::NotLeader(LeadershipRefusal {
            leader: None,
            proposal: Proposal::NotProposed,
            reason: LeadershipReason::Follower,
        }),
        GroupEngineError::Infra(_) => GroupEngineError::Infra(GroupInfraError::OwnerStopped),
    });
    let mut samples: Vec<GroupEngineError> = Vec::new();
    for sample in witnesses.iter().flat_map(family) {
        if !samples.contains(&sample) {
            samples.push(sample);
        }
    }
    samples
}

/// Every sample of the top-level variant `witness` is.
fn family(witness: &GroupEngineError) -> Vec<GroupEngineError> {
    match witness {
        GroupEngineError::Stream(_) => stream_refusals()
            .into_iter()
            .map(GroupEngineError::Stream)
            .collect(),
        GroupEngineError::NotLeader(_) => leadership_refusals()
            .into_iter()
            .map(GroupEngineError::NotLeader)
            .collect(),
        GroupEngineError::Infra(_) => infra_errors()
            .into_iter()
            .map(GroupEngineError::Infra)
            .collect(),
    }
}

fn stream_refusals() -> Vec<StreamRefusal> {
    let codes = every_unit!(StreamErrorCode {
        InvalidBucketId,
        InvalidStreamId,
        BucketNotFound,
        BucketErased,
        StreamNotFound,
        StreamGone,
        StreamAlreadyExistsConflict,
        MissingContentType,
        ContentTypeMismatch,
        EmptyAppend,
        StreamClosed,
        StreamSeqConflict,
        InvalidProducer,
        ProducerEpochClosed,
        ProducerExpired,
        ProducerEpochStale,
        ProducerKeyConflict,
        ProducerDigestMismatch,
        ProducerSeqConflict,
        InvalidRetention,
        OffsetOutOfRange,
        InvalidColdFlush,
        InvalidSnapshot,
        SnapshotNotFound,
        SnapshotConflict,
        InvalidRecordBoundaries,
        JsonBoundaryUnverified,
        ImportConflict,
        ImportInvalid,
        ProducerLimit,
        CreatedAtOutOfRange,
        IncarnationMismatch,
        HistoryPending,
        HistoryMismatch,
    });
    let producer_refusals = every!(ProducerRefusal {
        ProducerRefusal::EpochClosed { .. } => ProducerRefusal::EpochClosed {
            epoch: 3,
            by: ClosedBy::Claim,
            closed: ClosedEpoch { epoch: 2, seq: 9 },
        },
        ProducerRefusal::Expired { .. } => ProducerRefusal::Expired { epoch: 3 },
        ProducerRefusal::StaleEpoch { .. } => ProducerRefusal::StaleEpoch { epoch: 3 },
        ProducerRefusal::KeyConflict { .. } => ProducerRefusal::KeyConflict { epoch: 3 },
        ProducerRefusal::EpochNotClaimed => ProducerRefusal::EpochNotClaimed,
    });
    let closers = every_unit!(ClosedBy { Claim, History });
    let mut contexts = every!(StreamErrorContext {
        StreamErrorContext::StreamClosed => StreamErrorContext::StreamClosed,
        StreamErrorContext::StaleColdFlushCandidate => StreamErrorContext::StaleColdFlushCandidate,
        StreamErrorContext::Producer(_) => StreamErrorContext::Producer(ProducerRefusal::EpochNotClaimed),
        StreamErrorContext::ProducerSeqConflict { .. } => StreamErrorContext::ProducerSeqConflict {
            expected_seq: 8,
            received_seq: 3,
        },
        StreamErrorContext::StreamIncarnation { .. } => StreamErrorContext::StreamIncarnation {
            incarnation: 0x0123_4567,
        },
        StreamErrorContext::LatestSnapshot { .. } => StreamErrorContext::LatestSnapshot {
            offset: 12,
            digest: "digest".to_owned(),
        },
        StreamErrorContext::LastStreamSeq { .. } => StreamErrorContext::LastStreamSeq {
            seq: "seq".to_owned(),
        },
    });
    contexts.extend(
        producer_refusals
            .into_iter()
            .map(StreamErrorContext::Producer),
    );
    contexts.extend(closers.into_iter().map(|by| {
        StreamErrorContext::Producer(ProducerRefusal::EpochClosed {
            epoch: 4,
            by,
            closed: ClosedEpoch { epoch: 3, seq: 1 },
        })
    }));
    let mut refusals: Vec<StreamRefusal> = codes
        .into_iter()
        .map(|code| StreamRefusal {
            code,
            text: "refused".to_owned(),
            next_offset: None,
            context: Vec::new(),
        })
        .collect();
    refusals.extend(contexts.into_iter().map(|context| StreamRefusal {
        code: StreamErrorCode::StreamSeqConflict,
        text: "refused with context".to_owned(),
        next_offset: Some(5),
        context: vec![context],
    }));
    refusals
}

fn leadership_refusals() -> Vec<LeadershipRefusal> {
    let reasons = every_unit!(LeadershipReason {
        Follower,
        SteppedDown,
        LeadershipTransfer,
        QuorumUnconfirmed,
        ReadIndexBehind,
        LeaderUnreachable,
        LeaderInOtherCluster,
    });
    let proposals = every_unit!(Proposal {
        NotProposed,
        MayHaveCommitted
    });
    let mut refusals: Vec<LeadershipRefusal> = reasons
        .into_iter()
        .map(|reason| LeadershipRefusal {
            leader: None,
            proposal: Proposal::NotProposed,
            reason,
        })
        .collect();
    refusals.extend(proposals.into_iter().map(|proposal| LeadershipRefusal {
        leader: Some(KnownLeader {
            node_id: 2,
            term: 9,
        }),
        proposal,
        reason: LeadershipReason::SteppedDown,
    }));
    refusals
}

fn infra_errors() -> Vec<GroupInfraError> {
    let mut errors = every!(GroupInfraError {
        GroupInfraError::RecoveryNodeMismatch { .. } => GroupInfraError::RecoveryNodeMismatch {
            raft_group_id: GROUP,
            expected: 1,
            actual: 2,
        },
        GroupInfraError::RaftGroupNotRegistered { .. } => GroupInfraError::RaftGroupNotRegistered {
            raft_group_id: GROUP,
        },
        GroupInfraError::RecoveryAlreadyBound { .. } => GroupInfraError::RecoveryAlreadyBound {
            raft_group_id: GROUP,
        },
        GroupInfraError::RecoveryGroupMismatch { .. } => GroupInfraError::RecoveryGroupMismatch {
            expected: GROUP,
            actual: RaftGroupId(8),
        },
        GroupInfraError::ApplyStopped { .. } => GroupInfraError::ApplyStopped {
            raft_group_id: GROUP,
            term: 4,
            index: 19,
            kind: ApplyFailureKind::Panic,
        },
        GroupInfraError::InvalidRaftCommand { .. } => GroupInfraError::InvalidRaftCommand {
            command: UnproposableCommand::CreateBucket,
        },
        GroupInfraError::WalOpen { .. } => GroupInfraError::WalOpen {
            core_id: CoreId(1),
            raft_group_id: GROUP,
            failure: WalOpenFailureKind::Io,
        },
        GroupInfraError::RecoveryVoteFloor { .. } => GroupInfraError::RecoveryVoteFloor {
            raft_group_id: GROUP,
        },
        GroupInfraError::RecoveryCutting { .. } => GroupInfraError::RecoveryCutting {
            raft_group_id: GROUP,
        },
        GroupInfraError::CommittedOverwrite { .. } => GroupInfraError::CommittedOverwrite {
            raft_group_id: GROUP,
            committed: 12,
            index: 10,
        },
        GroupInfraError::LogBehindCommit { .. } => GroupInfraError::LogBehindCommit {
            raft_group_id: GROUP,
            committed: 12,
            held: Some(10),
        },
        GroupInfraError::LogBelowSnapshot { .. } => GroupInfraError::LogBelowSnapshot {
            core_id: CoreId(2),
            raft_group_id: GROUP,
            snapshot: LogPosition {
                term: 2,
                node_id: 4,
                index: 3,
            },
            held: LogPosition {
                term: 1,
                node_id: 6,
                index: 5,
            },
        },
        GroupInfraError::UnrelatedHistory { .. } => GroupInfraError::UnrelatedHistory {
            raft_group_id: GROUP,
            history: 0x0fed_cba9,
            replaced: 0x0123_4567,
            local: Some(0x0123_4568),
        },
        GroupInfraError::RecoveryPlanFenced { .. } => GroupInfraError::RecoveryPlanFenced {
            raft_group_id: GROUP,
            plan_term: 6,
        },
        GroupInfraError::RecoveryPending { .. } => GroupInfraError::RecoveryPending {
            raft_group_id: GROUP,
        },
        GroupInfraError::OwnerStopped => GroupInfraError::OwnerStopped,
        GroupInfraError::EngineWorkOutlivedShutdown { .. } => GroupInfraError::EngineWorkOutlivedShutdown {
            raft_group_id: GROUP,
            waited_ms: 30_000,
        },
        GroupInfraError::PreviousEngineWorkRunning { .. } => GroupInfraError::PreviousEngineWorkRunning {
            raft_group_id: GROUP,
        },
        GroupInfraError::OutcomeUnknown => GroupInfraError::OutcomeUnknown,
        GroupInfraError::ColdStore { .. } => GroupInfraError::ColdStore {
            operation: ColdOperation::RepairIndex,
        },
        GroupInfraError::ColdBackpressure { .. } => GroupInfraError::ColdBackpressure {
            stream_id: stream(),
            before_group_hot_bytes: 10,
            after_group_hot_bytes: 20,
            limit: 15,
        },
        GroupInfraError::RaftUncommittedBackpressure { .. } => {
            GroupInfraError::RaftUncommittedBackpressure {
                current: 10,
                incoming: 20,
                limit: 15,
            }
        },
        GroupInfraError::UnexpectedResponse { .. } => GroupInfraError::UnexpectedResponse {
            expected: ResponseKind::Append,
            actual: ResponseKind::Missing,
        },
        GroupInfraError::MissingStreamId { .. } => GroupInfraError::MissingStreamId {
            response: ResponseKind::CreateStream,
        },
        GroupInfraError::Unsupported { .. } => GroupInfraError::Unsupported {
            operation: EngineOperation::PurgeBucket,
        },
        GroupInfraError::BootstrapWindowNotHot { .. } => GroupInfraError::BootstrapWindowNotHot {
            stream_id: stream(),
            start: 4,
            end: 8,
        },
        GroupInfraError::ColdCompactionStale { .. } => GroupInfraError::ColdCompactionStale {
            stream_id: stream(),
        },
        GroupInfraError::ForwardFailed { .. } => GroupInfraError::ForwardFailed {
            rpc: ForwardedRpc::GroupRead,
            failure: ForwardFailure::NoIdentity,
        },
        GroupInfraError::ForwardedAnswerInvalid { .. } => GroupInfraError::ForwardedAnswerInvalid {
            rpc: ForwardedRpc::GroupWrite,
            problem: AnswerProblem::Undecodable,
        },
        GroupInfraError::InvalidRaftConfig { .. } => GroupInfraError::InvalidRaftConfig {
            heartbeat_ms: 100,
            election_timeout_min_ms: 50,
            election_timeout_max_ms: 40,
        },
        GroupInfraError::StaticMembership { .. } => GroupInfraError::StaticMembership {
            raft_group_id: GROUP,
            problem: StaticMembershipProblem::MissingGroup,
        },
        GroupInfraError::RecoveryGateOpen { .. } => GroupInfraError::RecoveryGateOpen {
            core_id: CoreId(1),
            raft_group_id: GROUP,
            failure: WalOpenFailureKind::Corrupt,
        },
        GroupInfraError::MembershipInit { .. } => GroupInfraError::MembershipInit {
            raft_group_id: GROUP,
            refusal: MembershipInitRefusal::NotAllowed,
        },
        GroupInfraError::LeaderWaitTimedOut { .. } => GroupInfraError::LeaderWaitTimedOut {
            raft_group_id: GROUP,
            timeout_ms: 2000,
        },
        GroupInfraError::Snapshot(_) => GroupInfraError::Snapshot(
            GroupSnapshotError::InvalidContent(StreamSnapshotError::BackupFormat { found: Some(4) }),
        ),
    });
    errors.extend(
        every_unit!(ApplyFailureKind {
            Panic,
            InvariantViolation,
            Unknown
        })
        .into_iter()
        .map(|kind| GroupInfraError::ApplyStopped {
            raft_group_id: GROUP,
            term: 5,
            index: 20,
            kind,
        }),
    );
    errors.extend(
        every_unit!(UnproposableCommand {
            CreateBucket,
            NewHistory
        })
        .into_iter()
        .map(|command| GroupInfraError::InvalidRaftCommand { command }),
    );
    let failures = every_unit!(WalOpenFailureKind {
        Io,
        Corrupt,
        IncompatibleFormat,
        Locked,
        DuplicateGroup,
        Stopped,
        InvalidConfiguration,
        InvalidRecord,
        Poisoned,
    });
    errors.extend(failures.iter().map(|&failure| GroupInfraError::WalOpen {
        core_id: CoreId(2),
        raft_group_id: GROUP,
        failure,
    }));
    errors.extend(
        failures
            .into_iter()
            .map(|failure| GroupInfraError::RecoveryGateOpen {
                core_id: CoreId(2),
                raft_group_id: GROUP,
                failure,
            }),
    );
    errors.extend(
        every!(ColdOperation {
            ColdOperation::ReadPayload { .. } => ColdOperation::ReadPayload { stream_id: stream() },
            ColdOperation::RefreshIndexPage { .. } => ColdOperation::RefreshIndexPage {
                stream_id: stream(),
            },
            ColdOperation::IndexExternalObject { .. } => ColdOperation::IndexExternalObject {
                stream_id: stream(),
            },
            ColdOperation::RepairIndex => ColdOperation::RepairIndex,
            ColdOperation::IndexFlushedChunk { .. } => ColdOperation::IndexFlushedChunk {
                stream_id: stream(),
            },
            ColdOperation::IndexCompactedChunk { .. } => ColdOperation::IndexCompactedChunk {
                stream_id: stream(),
            },
            ColdOperation::RollbackIndex { .. } => ColdOperation::RollbackIndex {
                stream_id: stream(),
            },
        })
        .into_iter()
        .map(|operation| GroupInfraError::ColdStore { operation }),
    );
    errors.extend(
        every_unit!(ResponseKind {
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
            BucketCreated,
            BucketAlreadyExists,
            StreamError,
            RaftBlank,
            RaftMembership,
            Missing,
        })
        .into_iter()
        .map(|actual| GroupInfraError::UnexpectedResponse {
            expected: ResponseKind::TouchStreamAccess,
            actual,
        }),
    );
    errors.extend(
        every_unit!(EngineOperation {
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
        })
        .into_iter()
        .map(|operation| GroupInfraError::Unsupported { operation }),
    );
    let rpcs = every_unit!(ForwardedRpc {
        GroupRead,
        GroupWrite
    });
    errors.extend(
        every!(ForwardFailure {
            ForwardFailure::NoIdentity => ForwardFailure::NoIdentity,
            ForwardFailure::InvalidEndpoint => ForwardFailure::InvalidEndpoint,
            ForwardFailure::Refused { .. } => ForwardFailure::Refused { grpc_code: 9 },
        })
        .into_iter()
        .map(|failure| GroupInfraError::ForwardFailed {
            rpc: ForwardedRpc::GroupWrite,
            failure,
        }),
    );
    errors.extend(
        every!(AnswerProblem {
            AnswerProblem::Undecodable => AnswerProblem::Undecodable,
            AnswerProblem::ResultCount { .. } => AnswerProblem::ResultCount { results: 2 },
        })
        .into_iter()
        .map(|problem| GroupInfraError::ForwardedAnswerInvalid {
            rpc: ForwardedRpc::GroupRead,
            problem,
        }),
    );
    errors.extend(rpcs.into_iter().map(|rpc| GroupInfraError::ForwardFailed {
        rpc,
        failure: ForwardFailure::InvalidEndpoint,
    }));
    errors.extend(
        every!(StaticMembershipProblem {
            StaticMembershipProblem::MissingGroup => StaticMembershipProblem::MissingGroup,
            StaticMembershipProblem::NoVoters => StaticMembershipProblem::NoVoters,
            StaticMembershipProblem::VoterWithoutPeer { .. } => {
                StaticMembershipProblem::VoterWithoutPeer { node_id: 3 }
            },
            StaticMembershipProblem::NodeWithoutPeer { .. } => {
                StaticMembershipProblem::NodeWithoutPeer { node_id: 4 }
            },
        })
        .into_iter()
        .map(|problem| GroupInfraError::StaticMembership {
            raft_group_id: GROUP,
            problem,
        }),
    );
    errors.extend(
        every_unit!(MembershipInitRefusal {
            NotAllowed,
            NotInMembers
        })
        .into_iter()
        .map(|refusal| GroupInfraError::MembershipInit {
            raft_group_id: GROUP,
            refusal,
        }),
    );
    errors.extend(snapshot_errors().into_iter().map(GroupInfraError::Snapshot));
    errors
}

fn snapshot_errors() -> Vec<GroupSnapshotError> {
    let contents = every!(StreamSnapshotError {
        StreamSnapshotError::DuplicateBucket(_) => {
            StreamSnapshotError::DuplicateBucket("bucket".to_owned())
        },
        StreamSnapshotError::DuplicateErasedBucket(_) => {
            StreamSnapshotError::DuplicateErasedBucket("bucket".to_owned())
        },
        StreamSnapshotError::ActiveBucketErased(_) => {
            StreamSnapshotError::ActiveBucketErased("bucket".to_owned())
        },
        StreamSnapshotError::DuplicateStream(_) => StreamSnapshotError::DuplicateStream(stream()),
        StreamSnapshotError::DuplicateProducer { .. } => StreamSnapshotError::DuplicateProducer {
            stream_id: stream(),
            producer_id: "producer".to_owned(),
        },
        StreamSnapshotError::MissingBucket(_) => StreamSnapshotError::MissingBucket(stream()),
        StreamSnapshotError::PayloadLengthMismatch { .. } => {
            StreamSnapshotError::PayloadLengthMismatch {
                stream_id: stream(),
                tail_offset: 9,
                payload_len: 8,
            }
        },
        StreamSnapshotError::SnapshotOffsetOutOfRange { .. } => {
            StreamSnapshotError::SnapshotOffsetOutOfRange {
                stream_id: stream(),
                snapshot_offset: 10,
                tail_offset: 9,
            }
        },
        StreamSnapshotError::BackupFormat { .. } => StreamSnapshotError::BackupFormat { found: None },
        StreamSnapshotError::UnattributedColdGc { .. } => {
            StreamSnapshotError::UnattributedColdGc { seq: 5 }
        },
        StreamSnapshotError::ColdGcWithoutGeneration { .. } => {
            StreamSnapshotError::ColdGcWithoutGeneration { seq: 6 }
        },
    });
    let key = || "snapshots/node-1/group-7/00000000000000000042-00000000000000ab.snap".to_owned();
    let mut errors = every!(GroupSnapshotError {
        GroupSnapshotError::InvalidContent(_) => GroupSnapshotError::InvalidContent(
            StreamSnapshotError::DuplicateBucket("first".to_owned()),
        ),
        GroupSnapshotError::ForeignKey { .. } => GroupSnapshotError::ForeignKey {
            raft_group_id: GROUP,
            key: "snapshots/node-10/group-7/00000000000000000042-00000000000000ab.snap"
                .to_owned(),
            directory: Some("snapshots/node-1/group-7/".to_owned()),
        },
        GroupSnapshotError::ObjectMissing { .. } => GroupSnapshotError::ObjectMissing {
            raft_group_id: GROUP,
            key: key(),
        },
        GroupSnapshotError::ObjectCorrupt { .. } => GroupSnapshotError::ObjectCorrupt {
            raft_group_id: GROUP,
            key: key(),
            check: SnapshotObjectCheck::Decompress,
        },
        GroupSnapshotError::PointerUnreadable { .. } => {
            GroupSnapshotError::PointerUnreadable { raft_group_id: GROUP }
        },
        GroupSnapshotError::StoreUnavailable { .. } => GroupSnapshotError::StoreUnavailable {
            raft_group_id: GROUP,
            key: key(),
            operation: SnapshotStoreOperation::Get,
        },
    });
    errors.extend(contents.into_iter().map(GroupSnapshotError::InvalidContent));
    errors.push(GroupSnapshotError::ForeignKey {
        raft_group_id: GROUP,
        key: key(),
        directory: None,
    });
    errors.extend(
        every!(SnapshotObjectCheck {
            SnapshotObjectCheck::StoredSize { .. } => SnapshotObjectCheck::StoredSize {
                expected: 10,
                actual: 9,
            },
            SnapshotObjectCheck::LogicalSize { .. } => SnapshotObjectCheck::LogicalSize {
                expected: 20,
                actual: 19,
            },
            SnapshotObjectCheck::Decompress => SnapshotObjectCheck::Decompress,
            SnapshotObjectCheck::Decode => SnapshotObjectCheck::Decode,
        })
        .into_iter()
        // The sample above carries this check.
        .filter(|check| *check != SnapshotObjectCheck::Decompress)
        .map(|check| GroupSnapshotError::ObjectCorrupt {
            raft_group_id: GROUP,
            key: key(),
            check,
        }),
    );
    errors.extend(
        every_unit!(SnapshotStoreOperation {
            Get,
            Head,
            Put,
            List,
            Delete
        })
        .into_iter()
        // The sample above carries this operation.
        .filter(|operation| *operation != SnapshotStoreOperation::Get)
        .map(|operation| GroupSnapshotError::StoreUnavailable {
            raft_group_id: GROUP,
            key: key(),
            operation,
        }),
    );
    errors
}

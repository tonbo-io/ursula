//! The classification of every [`GroupEngineError`] sample: its status
//! (`Retry-After`), its write effect and whether a Raft peer backs off. The
//! table is an exhaustive match, so a new variant states its class here.

use ursula_stream::StreamErrorCode;

use super::GroupEngineError;
use super::GroupInfraError;
use super::GroupSnapshotError;
use super::Proposal;
use super::WriteEffect;
use super::group_engine_error_samples;
use crate::error::ErrorStatus;

/// `(status, write_effect, peer_backs_off)` of `error`.
fn expected(error: &GroupEngineError) -> (ErrorStatus, WriteEffect, bool) {
    use ErrorStatus::Permanent;
    use ErrorStatus::Persistent;
    use ErrorStatus::Temporary;
    use WriteEffect::NotProposed;
    use WriteEffect::Refused;
    use WriteEffect::Unknown;
    match error {
        GroupEngineError::Stream(refusal) if refusal.code == StreamErrorCode::HistoryPending => {
            (Temporary, Refused, false)
        }
        GroupEngineError::Stream(_) => (Permanent, Refused, false),
        GroupEngineError::NotLeader(refusal) => match refusal.proposal {
            Proposal::NotProposed => (Temporary, NotProposed, false),
            Proposal::MayHaveCommitted => (Temporary, Unknown, false),
        },
        GroupEngineError::Infra(infra) => match infra {
            GroupInfraError::ColdBackpressure { .. }
            | GroupInfraError::RaftUncommittedBackpressure { .. }
            | GroupInfraError::RecoveryPending { .. } => (Temporary, NotProposed, false),
            // No engine of the group runs on the node; a peer backs off
            // until it does.
            GroupInfraError::EngineWorkOutlivedShutdown { .. }
            | GroupInfraError::PreviousEngineWorkRunning { .. } => (Temporary, NotProposed, true),
            GroupInfraError::Unsupported { .. } | GroupInfraError::ColdCompactionStale { .. } => {
                (Persistent, NotProposed, false)
            }
            GroupInfraError::RecoveryVoteFloor { .. }
            | GroupInfraError::RecoveryCutting { .. }
            | GroupInfraError::CommittedOverwrite { .. }
            | GroupInfraError::UnrelatedHistory { .. }
            | GroupInfraError::LogBehindCommit { .. }
            | GroupInfraError::RecoveryPlanFenced { .. }
            | GroupInfraError::OwnerStopped
            | GroupInfraError::ApplyStopped { .. }
            | GroupInfraError::OutcomeUnknown => (Persistent, Unknown, true),
            GroupInfraError::Snapshot(GroupSnapshotError::InvalidContent(_)) => {
                (Persistent, Unknown, false)
            }
            GroupInfraError::Snapshot(
                GroupSnapshotError::ForeignKey { .. }
                | GroupSnapshotError::ObjectMissing { .. }
                | GroupSnapshotError::ObjectCorrupt { .. }
                | GroupSnapshotError::PointerUnreadable { .. }
                | GroupSnapshotError::StoreUnavailable { .. },
            ) => (Persistent, Unknown, true),
            GroupInfraError::RecoveryNodeMismatch { .. }
            | GroupInfraError::RaftGroupNotRegistered { .. }
            | GroupInfraError::RecoveryAlreadyBound { .. }
            | GroupInfraError::RecoveryGroupMismatch { .. }
            | GroupInfraError::InvalidRaftCommand { .. }
            | GroupInfraError::WalOpen { .. }
            | GroupInfraError::ColdStore { .. }
            | GroupInfraError::Internal { .. }
            | GroupInfraError::ProtoDecode { .. }
            | GroupInfraError::UnexpectedResponse { .. }
            | GroupInfraError::MissingStreamId { .. }
            | GroupInfraError::BootstrapWindowNotHot { .. }
            | GroupInfraError::ForwardFailed { .. }
            | GroupInfraError::ForwardedAnswerInvalid { .. }
            | GroupInfraError::InvalidRaftConfig { .. }
            | GroupInfraError::StaticMembership { .. }
            | GroupInfraError::RecoveryGateOpen { .. }
            | GroupInfraError::MembershipInit { .. }
            | GroupInfraError::LeaderWaitTimedOut { .. } => (Persistent, Unknown, false),
        },
    }
}

#[test]
fn every_sample_classifies_as_its_table_row() {
    for error in group_engine_error_samples() {
        assert_eq!(
            (error.status(), error.write_effect(), error.peer_backs_off()),
            expected(&error),
            "{error:?}"
        );
        assert_eq!(
            error.applied_nothing(),
            error.write_effect() != WriteEffect::Unknown,
            "{error:?}"
        );
        assert_eq!(
            error.refused_before_proposal(),
            error.write_effect() == WriteEffect::NotProposed,
            "{error:?}"
        );
        assert_eq!(
            error.not_leader().is_some(),
            matches!(error, GroupEngineError::NotLeader(_)),
            "{error:?}"
        );
    }
}

/// Every sample has its own `Debug` form, which names it in the wire
/// fixture of `ursula-raft`.
#[test]
fn samples_are_distinct() {
    let samples = group_engine_error_samples();
    let names: std::collections::BTreeSet<String> =
        samples.iter().map(|error| format!("{error:?}")).collect();
    assert_eq!(names.len(), samples.len());
}

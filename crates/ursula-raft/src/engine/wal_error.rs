//! The one conversion from native WAL sources to serializable engine errors.
use ursula_runtime::GroupEngineError;
use ursula_runtime::GroupInfraError;
use ursula_runtime::WalOpenFailureKind;
use ursula_shard::ShardPlacement;

use crate::log_store::CoreJournalError;
use crate::log_store::JournalError;
use crate::log_store::MarkRecoveringError;
use crate::log_store::RaftWalError;
use crate::log_store::StateFileError;

pub(super) fn group_open_error(
    placement: ShardPlacement,
    source: RaftWalError,
) -> GroupEngineError {
    let failure = classify(&source);
    // GroupEngineError is a cross-crate, serializable protocol value. Native
    // non-serializable sources stay intact at the WAL API and are recorded here.
    tracing::error!(core_id = placement.core_id.0, raft_group_id = placement.raft_group_id.0,
        error = %source, source = ?source, "WAL refused to open a group");
    GroupEngineError::Infra(GroupInfraError::WalOpen {
        core_id: placement.core_id,
        raft_group_id: placement.raft_group_id,
        failure,
    })
}

fn classify(error: &RaftWalError) -> WalOpenFailureKind {
    match error {
        RaftWalError::OpenCore { source, .. }
        | RaftWalError::OpenGroup { source, .. }
        | RaftWalError::CloseJournal { source, .. } => core_failure(source),
        RaftWalError::TopologyMismatch { .. } | RaftWalError::MissingTopology { .. } => {
            WalOpenFailureKind::InvalidConfiguration
        }
        RaftWalError::ReadTopology(source)
        | RaftWalError::ReadRunState(source)
        | RaftWalError::ReadCoreMetadata(source) => state_failure(source),
        // Only corrupt or forged core metadata can hold the last epoch.
        RaftWalError::RecoveryEpochExhausted { .. } => WalOpenFailureKind::Corrupt,
        RaftWalError::RecordTopology(source)
        | RaftWalError::RecordRunState(source)
        | RaftWalError::ReadJournal { source, .. }
        | RaftWalError::SyncJournal { source, .. } => journal_failure(source),
        RaftWalError::CreateDir { .. }
        | RaftWalError::Lock { .. }
        | RaftWalError::ListCores { .. } => WalOpenFailureKind::Io,
        RaftWalError::Locked { .. } => WalOpenFailureKind::Locked,
        RaftWalError::MarkRecovering { source, .. } => match source {
            MarkRecoveringError::Read(source) => state_failure(source),
            MarkRecoveringError::Write(source) => journal_failure(source),
        },
        RaftWalError::ShutDown { .. } => WalOpenFailureKind::Stopped,
        RaftWalError::LockPoisoned => WalOpenFailureKind::Poisoned,
    }
}

fn core_failure(error: &CoreJournalError) -> WalOpenFailureKind {
    match error {
        CoreJournalError::Io { .. } => WalOpenFailureKind::Io,
        CoreJournalError::Journal(source) => journal_failure(source),
        CoreJournalError::Metadata(source) => state_failure(source),
        CoreJournalError::Locked { .. } => WalOpenFailureKind::Locked,
        CoreJournalError::GroupAlreadyOpen { .. } => WalOpenFailureKind::DuplicateGroup,
        #[cfg(not(madsim))]
        CoreJournalError::SpawnWriter { .. } => WalOpenFailureKind::Io,
        CoreJournalError::RecordTooLarge(_) | CoreJournalError::InvalidRecord { .. } => {
            WalOpenFailureKind::InvalidRecord
        }
        CoreJournalError::InconsistentLog { .. } => WalOpenFailureKind::Corrupt,
        CoreJournalError::WriterPoisoned { .. } | CoreJournalError::LockPoisoned => {
            WalOpenFailureKind::Poisoned
        }
        CoreJournalError::WriterStopped { .. } => WalOpenFailureKind::Stopped,
        #[cfg(not(madsim))]
        CoreJournalError::ReadStopped { .. } => WalOpenFailureKind::Stopped,
    }
}

fn journal_failure(error: &JournalError) -> WalOpenFailureKind {
    match error {
        JournalError::Io { .. } => WalOpenFailureKind::Io,
        JournalError::NotAJournal { .. } | JournalError::UnsupportedVersion { .. } => {
            WalOpenFailureKind::IncompatibleFormat
        }
        JournalError::RecordTooLarge(_) => WalOpenFailureKind::InvalidRecord,
        JournalError::FrozenArchive { .. }
        | JournalError::CorruptHeader { .. }
        | JournalError::CorruptFrame { .. }
        | JournalError::CorruptFrameAt { .. }
        | JournalError::IncompleteSealedSegment { .. }
        | JournalError::MissingSegment { .. }
        | JournalError::OversizedFrame { .. }
        | JournalError::Undecodable { .. }
        | JournalError::UndecodableAt { .. }
        | JournalError::RejectedAt { .. }
        | JournalError::FrameMismatch { .. }
        | JournalError::Rejected { .. } => WalOpenFailureKind::Corrupt,
    }
}

fn state_failure(error: &StateFileError) -> WalOpenFailureKind {
    match error {
        StateFileError::Io { .. } => WalOpenFailureKind::Io,
        StateFileError::UnsupportedVersion { .. } => WalOpenFailureKind::IncompatibleFormat,
        StateFileError::WrongKind { .. }
        | StateFileError::Corrupt { .. }
        | StateFileError::Undecodable { .. } => WalOpenFailureKind::Corrupt,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use ursula_shard::CoreId;
    use ursula_shard::RaftGroupId;
    use ursula_shard::ShardId;

    use super::*;

    #[test]
    fn factory_conversion_distinguishes_io_corruption_and_format_refusal() {
        let placement = ShardPlacement {
            core_id: CoreId(2),
            shard_id: ShardId(7),
            raft_group_id: RaftGroupId(7),
        };
        let cases = [
            (
                CoreJournalError::Io {
                    path: "journal".into(),
                    source: Arc::new(std::io::Error::from(std::io::ErrorKind::PermissionDenied)),
                },
                WalOpenFailureKind::Io,
            ),
            (
                CoreJournalError::Journal(Arc::new(JournalError::CorruptFrame {
                    path: "journal".into(),
                    frame: 2,
                    offset: 4096,
                    defect: crate::log_store::FrameDefect::HeaderChecksum,
                })),
                WalOpenFailureKind::Corrupt,
            ),
            (
                CoreJournalError::Journal(Arc::new(JournalError::FrozenArchive {
                    path: "frozen-archive".into(),
                    defect: crate::log_store::ArchiveDefect::Checksum,
                })),
                WalOpenFailureKind::Corrupt,
            ),
            (
                CoreJournalError::Journal(Arc::new(JournalError::UnsupportedVersion {
                    path: "journal".into(),
                    version: 0,
                })),
                WalOpenFailureKind::IncompatibleFormat,
            ),
            (
                CoreJournalError::GroupAlreadyOpen {
                    journal: "journal".into(),
                    raft_group_id: placement.raft_group_id,
                },
                WalOpenFailureKind::DuplicateGroup,
            ),
            (
                CoreJournalError::WriterStopped {
                    journal: "journal".into(),
                },
                WalOpenFailureKind::Stopped,
            ),
        ];
        for (source, failure) in cases {
            assert_eq!(
                group_open_error(placement, RaftWalError::OpenCore {
                    core: placement.core_id,
                    source
                }),
                GroupEngineError::Infra(GroupInfraError::WalOpen {
                    core_id: placement.core_id,
                    raft_group_id: placement.raft_group_id,
                    failure
                })
            );
        }
    }
}

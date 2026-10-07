//! Public WAL API, persisted-format value types, and simulation disk adapters.
//! This is the single public import path for opening and inspecting the node WAL.

pub use crate::log_store::BootId;
pub use crate::log_store::CoreJournalError;
pub use crate::log_store::FrameDefect;
pub use crate::log_store::GroupLogState;
pub use crate::log_store::HeaderDefect;
#[cfg(madsim)]
pub use crate::log_store::JournalDisk;
pub use crate::log_store::JournalError;
#[cfg(madsim)]
pub use crate::log_store::JournalFile;
pub use crate::log_store::JournalHistory;
pub use crate::log_store::JournalOp;
pub use crate::log_store::JournalReplayMode;
pub use crate::log_store::JournalTuning;
pub use crate::log_store::LaggingGroups;
#[cfg(madsim)]
pub use crate::log_store::LockAttempt;
pub use crate::log_store::MarkRecoveringError;
pub use crate::log_store::PreviousRun;
pub use crate::log_store::RUN_STATE_FILE;
pub use crate::log_store::RaftGroupFileLogStore;
pub use crate::log_store::RaftWal;
pub use crate::log_store::RaftWalError;
pub use crate::log_store::RecordTooLarge;
pub use crate::log_store::RecoveryReason;
pub use crate::log_store::RecoveryState;
pub use crate::log_store::RunState;
pub use crate::log_store::RunStatus;
#[cfg(madsim)]
pub use crate::log_store::SIM_DISK_PAGE_SIZE;
#[cfg(madsim)]
pub use crate::log_store::SimDisk;
#[cfg(madsim)]
pub use crate::log_store::SimDiskError;
#[cfg(madsim)]
pub use crate::log_store::SimDiskFault;
#[cfg(madsim)]
pub use crate::log_store::SimFile;
#[cfg(madsim)]
pub use crate::log_store::SimJournalLock;
#[cfg(madsim)]
pub use crate::log_store::SimPowerLoss;
pub use crate::log_store::StateFileDefect;
pub use crate::log_store::StateFileError;
pub use crate::log_store::StateFileKind;
pub use crate::log_store::WalOpening;
pub use crate::log_store::journal_segment_path;
pub use crate::log_store::journal_segments;

/// Persisted-format diagnostics and simulation disk adapters.
pub mod diagnostics {
    pub use crate::log_store::BootId;
    pub use crate::log_store::CoreJournalError;
    pub use crate::log_store::FrameDefect;
    pub use crate::log_store::GroupLogState;
    pub use crate::log_store::HeaderDefect;
    #[cfg(madsim)]
    pub use crate::log_store::JournalDisk;
    pub use crate::log_store::JournalError;
    #[cfg(madsim)]
    pub use crate::log_store::JournalFile;
    pub use crate::log_store::JournalHistory;
    pub use crate::log_store::JournalOp;
    pub use crate::log_store::JournalReplayMode;
    pub use crate::log_store::JournalSync;
    #[cfg(madsim)]
    pub use crate::log_store::LockAttempt;
    pub use crate::log_store::MarkRecoveringError;
    pub use crate::log_store::PreviousRun;
    pub use crate::log_store::RUN_STATE_FILE;
    pub use crate::log_store::RecordTooLarge;
    pub use crate::log_store::RecoveryReason;
    pub use crate::log_store::RunState;
    pub use crate::log_store::RunStatus;
    #[cfg(madsim)]
    pub use crate::log_store::SIM_DISK_PAGE_SIZE;
    #[cfg(madsim)]
    pub use crate::log_store::SimDisk;
    #[cfg(madsim)]
    pub use crate::log_store::SimDiskError;
    #[cfg(madsim)]
    pub use crate::log_store::SimDiskFault;
    #[cfg(madsim)]
    pub use crate::log_store::SimFile;
    #[cfg(madsim)]
    pub use crate::log_store::SimJournalLock;
    #[cfg(madsim)]
    pub use crate::log_store::SimPowerLoss;
    pub use crate::log_store::StateFileDefect;
    pub use crate::log_store::StateFileError;
    pub use crate::log_store::StateFileKind;
    pub use crate::log_store::journal_segment_path;
    pub use crate::log_store::journal_segments;
}

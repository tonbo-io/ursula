//! The node's run state, and how the journals open after the run before.
//!
//! The run-state file at the WAL root records the run that last opened the
//! journals: the host's boot id at the time, the run's fsync policy, and how
//! the run ended ([`RunStatus`]). A run reads it first, decides how to read
//! the journals ([`WalOpening::decide`], the design's "Opening the journal"
//! table), and durably records itself as [`RunStatus::Running`] before any
//! journal write. A graceful shutdown records [`RunStatus::Clean`] after every
//! journal is `fsync`ed; an I/O failure that poisons a journal writer records
//! [`RunStatus::Poisoned`] before the process stops.
//!
//! Cores open their journals lazily, so a run can end before it has read
//! every journal the run before it left. The run state therefore also counts
//! recovery epochs: a run that starts after a host crash or an I/O failure
//! begins a new epoch, and each core's metadata file records the epoch in
//! which its journal was last read back to its verified prefix. A journal not
//! read since the epoch began is still read that way, however later runs
//! ended.
//!
//! A run that starts [`RecoveryState::Recovering`] moves every initialized
//! group of every core into recovery in the core's metadata file before it
//! records itself as running, so the groups stay out of elections until
//! their recovery gates open, however later runs end.
//!
//! The policy a run records must hold for every write the journals hold,
//! because a later host crash is read by it. A run that records `always`
//! after a `never` run whose process stopped on this boot therefore first
//! `fsync`s every journal ([`JournalSync`]): that run acknowledged writes the
//! page cache still holds but the disk may not.

use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;

use serde::Deserialize;
use serde::Serialize;
use ursula_config::WalFsync;

use super::core_meta::MarkRecoveringError;
use super::core_meta::core_metadata_path;
use super::core_meta::mark_core_recovering;
use super::disk::Disk;
use super::disk::DiskLock;
use super::disk::JournalDisk;
use super::disk::LockAttempt;
use super::disk::create_dir_all_durable;
use super::journal::JournalError;
use super::journal::JournalReplayMode;
use super::segment;
use super::state_file;
use super::state_file::StateFileError;
use super::state_file::StateFileKind;

/// The run-state file under the WAL root.
pub const RUN_STATE_FILE: &str = "run-state.bin";
/// The lock one node holds on its WAL root while it runs.
const WAL_LOCK_FILE: &str = "wal.lock";
/// The directory of core `N` under the WAL root is `core-N`.
const CORE_DIR_PREFIX: &str = "core-";

/// The journal directory of core `core` under the WAL `root`.
pub(crate) fn core_dir(root: &Path, core: u16) -> PathBuf {
    root.join(format!("{CORE_DIR_PREFIX}{core}"))
}

/// The id the host's kernel gives its current boot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct BootId(String);

impl BootId {
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// How a run that opened the journals ended, as far as it got to record.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunStatus {
    /// The run opened the journals and has not shut down cleanly. A run that
    /// crashed is left in this state.
    Running,
    /// The run stopped its Raft cores and `fsync`ed every journal.
    Clean,
    /// An I/O error stopped one of the run's journal writers.
    Poisoned,
}

/// The run-state file's contents.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunState {
    /// The host's boot when the run started; `None` where the platform
    /// reports none.
    pub boot_id: Option<BootId>,
    /// The run's fsync policy.
    pub fsync: WalFsync,
    pub status: RunStatus,
    /// Runs that started after a host crash or an I/O failure on this WAL
    /// root, including this one.
    pub recovery_epoch: u64,
}

/// Whether the core journals under a WAL root hold records.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JournalHistory {
    /// No core journal holds a record.
    Empty,
    /// Some core journal holds records.
    Records,
}

/// How the previous run that opened the journals ended, read from its run
/// state and the current boot id.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum PreviousRun {
    /// No run state and no journal record: a new node or a new disk.
    Absent,
    /// No run state, but core journals hold records: the run state was
    /// removed or lost, so how the run that wrote them ended is unknown.
    Unrecorded,
    /// The previous run shut down cleanly: every journal was `fsync`ed.
    Clean,
    /// The previous run's process stopped on the host that still runs, so
    /// the page cache kept every write.
    ProcessCrash,
    /// The previous run did not shut down cleanly and the boot id changed, or
    /// is unknown: the host crashed and writeback may have left holes in what
    /// the run wrote without `fsync`.
    HostCrash { fsync: WalFsync },
    /// An I/O error stopped the previous run.
    Poisoned,
}

/// Whether this node's Raft logs may be missing entries it acknowledged.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum RecoveryState {
    /// Every entry the node acknowledged is in its logs.
    Normal,
    /// The node's logs may be missing entries it acknowledged.
    Recovering { reason: RecoveryReason },
}

/// Why a node's logs may be missing acknowledged entries.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RecoveryReason {
    /// The host crashed while the policy was `never`.
    HostCrash,
    /// An I/O error stopped a journal writer.
    Poisoned,
    /// The run state is missing while journals hold records: how the run
    /// that wrote them ended is unknown.
    UnknownHistory,
}

/// Whether a run `fsync`s the journals the previous run left before it
/// records itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum JournalSync {
    /// The policy this run records holds for what the journals hold: the
    /// previous run's writes are on disk, are already treated as possibly
    /// lost, or stay under `never`.
    NotNeeded,
    /// The previous run acknowledged writes under `never` and its process
    /// stopped on this boot, so they may be in the page cache only. This run
    /// records `always`, under which a host crash reads as losing nothing,
    /// so it `fsync`s every core journal first.
    BeforeRecording,
}

/// How this run opens the journals.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct WalOpening {
    pub previous_run: PreviousRun,
    /// How a journal the previous run left is read. A journal not read since
    /// an earlier host crash or I/O failure is read in
    /// [`JournalReplayMode::VerifiedPrefix`] whatever this says.
    pub replay_mode: JournalReplayMode,
    pub recovery: RecoveryState,
    /// This run's recovery epoch (see the module documentation).
    pub recovery_epoch: u64,
    pub journal_sync: JournalSync,
}

impl PreviousRun {
    /// Reads `previous`, the run state the previous run left, given what
    /// the core `journals` hold and the current `boot_id`.
    pub fn interpret(
        previous: Option<&RunState>,
        journals: JournalHistory,
        boot_id: Option<&BootId>,
    ) -> Self {
        let Some(previous) = previous else {
            return match journals {
                JournalHistory::Empty => Self::Absent,
                JournalHistory::Records => Self::Unrecorded,
            };
        };
        match previous.status {
            RunStatus::Clean => Self::Clean,
            RunStatus::Poisoned => Self::Poisoned,
            RunStatus::Running => match (previous.boot_id.as_ref(), boot_id) {
                (Some(before), Some(now)) if before == now => Self::ProcessCrash,
                _ => Self::HostCrash {
                    fsync: previous.fsync,
                },
            },
        }
    }

    /// How to read the journals the previous run left. Even under `always`
    /// a host crash needs the verified prefix: committed and truncate
    /// markers are written without `fsync`, so writeback can leave a hole
    /// before acknowledged frames.
    pub fn replay_mode(self) -> JournalReplayMode {
        match self {
            Self::Absent | Self::Clean | Self::ProcessCrash => JournalReplayMode::Strict,
            Self::Unrecorded | Self::HostCrash { .. } | Self::Poisoned => {
                JournalReplayMode::VerifiedPrefix
            }
        }
    }

    /// Whether the node may have lost entries it acknowledged. Under
    /// `always` every acknowledged append was `fsync`ed before a host crash.
    pub fn recovery_state(self) -> RecoveryState {
        match self {
            Self::Absent
            | Self::Clean
            | Self::ProcessCrash
            | Self::HostCrash {
                fsync: WalFsync::Always,
            } => RecoveryState::Normal,
            Self::HostCrash {
                fsync: WalFsync::Never,
            } => RecoveryState::Recovering {
                reason: RecoveryReason::HostCrash,
            },
            Self::Poisoned => RecoveryState::Recovering {
                reason: RecoveryReason::Poisoned,
            },
            Self::Unrecorded => RecoveryState::Recovering {
                reason: RecoveryReason::UnknownHistory,
            },
        }
    }
}

impl WalOpening {
    /// The "Opening the journal" decision for a run on `boot_id` with the
    /// policy `fsync`, after the run that left `previous` and the core
    /// `journals`.
    pub fn decide(
        previous: Option<&RunState>,
        journals: JournalHistory,
        boot_id: Option<&BootId>,
        fsync: WalFsync,
    ) -> Self {
        let previous_run = PreviousRun::interpret(previous, journals, boot_id);
        let replay_mode = previous_run.replay_mode();
        let prior_epoch = previous.map_or(0, |previous| previous.recovery_epoch);
        let recovery_epoch = match replay_mode {
            JournalReplayMode::Strict => prior_epoch,
            JournalReplayMode::VerifiedPrefix => prior_epoch.saturating_add(1),
        };
        let journal_sync = match (previous_run, previous.map(|previous| previous.fsync), fsync) {
            (PreviousRun::ProcessCrash, Some(WalFsync::Never), WalFsync::Always) => {
                JournalSync::BeforeRecording
            }
            _ => JournalSync::NotNeeded,
        };
        Self {
            previous_run,
            replay_mode,
            recovery: previous_run.recovery_state(),
            recovery_epoch,
            journal_sync,
        }
    }
}

/// How a core journal is read: in [`JournalReplayMode::VerifiedPrefix`] when
/// it has not been read back since the current recovery epoch began.
pub(crate) fn core_replay_mode(verified_epoch: u64, recovery_epoch: u64) -> JournalReplayMode {
    if verified_epoch != recovery_epoch {
        JournalReplayMode::VerifiedPrefix
    } else {
        JournalReplayMode::Strict
    }
}

/// Failure to start or shut down a node's Raft WAL.
#[derive(Debug, thiserror::Error)]
pub enum RaftWalError {
    #[error("WAL topology mismatch at '{}': stored core_count={stored_core_count}, group_count={stored_group_count}, configured core_count={configured_core_count}, group_count={configured_group_count}. Restore the stored counts, or rebuild this replica on an empty WAL from a healthy quorum using the cluster's existing routing counts. See https://ursula.tonbo.io/docs/operations#wal-routing-configuration", .root.display())]
    TopologyMismatch {
        root: PathBuf,
        stored_core_count: u16,
        stored_group_count: u32,
        configured_core_count: u16,
        configured_group_count: u32,
    },
    #[error("WAL topology is missing at '{}' but prior WAL state exists. Refusing to infer core_count and group_count from the current configuration. For replica rebuild or single-node migration, see https://ursula.tonbo.io/docs/operations#wal-routing-configuration", .root.display())]
    MissingTopology { root: PathBuf },
    #[error("read the Raft WAL topology: {0}")]
    ReadTopology(#[source] StateFileError),
    #[error("record the Raft WAL topology: {0}")]
    RecordTopology(#[source] JournalError),
    #[error("create the Raft WAL directory '{}': {source}", .path.display())]
    CreateDir {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("lock the Raft WAL at '{}': {source}", .path.display())]
    Lock {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error(
        "the Raft WAL is already in use: '{}' is locked{}",
        .path.display(),
        .owner.as_deref().map(|owner| format!(" by {owner}")).unwrap_or_default()
    )]
    Locked {
        path: PathBuf,
        owner: Option<String>,
    },
    #[error("read the Raft WAL run state: {0}")]
    ReadRunState(#[source] StateFileError),
    #[error("read core metadata while recovering epochs: {0}")]
    ReadCoreMetadata(#[source] StateFileError),
    #[error("list the core journals under '{}': {source}", .root.display())]
    ListCores {
        root: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("read the core journal '{}': {source}", .path.display())]
    ReadJournal {
        path: PathBuf,
        #[source]
        source: JournalError,
    },
    #[error("move the raft groups of '{}' into recovery: {source}", .path.display())]
    MarkRecovering {
        path: PathBuf,
        #[source]
        source: MarkRecoveringError,
    },
    #[error("sync the core journal '{}' the previous run left: {source}", .path.display())]
    SyncJournal {
        path: PathBuf,
        #[source]
        source: JournalError,
    },
    #[error("record the Raft WAL run state: {0}")]
    RecordRunState(#[source] JournalError),
    #[error("the Raft WAL under '{}' has shut down", .root.display())]
    ShutDown { root: PathBuf },
    #[error("stop the journal of core {core}: {source}")]
    CloseJournal {
        core: u16,
        #[source]
        source: super::CoreJournalError,
    },
    #[error("Raft WAL state mutex poisoned")]
    LockPoisoned,
}

/// The run-state file, and the run state this run writes to it.
#[derive(Debug)]
pub(crate) struct RunStateFile {
    path: PathBuf,
    current: RunState,
}

impl RunStateFile {
    pub(crate) fn new(path: PathBuf, current: RunState) -> Self {
        Self { path, current }
    }

    /// Records that this run is in `status`. `writer` names the caller in
    /// the temporary file, so concurrent writers never share one.
    pub(crate) fn record(&self, status: RunStatus, writer: &str) -> Result<u64, JournalError> {
        let temp = self.path.with_extension(format!("{writer}.tmp"));
        state_file::write(StateFileKind::RunState, &self.path, &temp, &RunState {
            status,
            ..self.current.clone()
        })
    }

    pub(crate) fn current(&self) -> &RunState {
        &self.current
    }
}

/// One node's Raft WAL root while a run holds it.
#[derive(Debug)]
pub(crate) struct NodeWal {
    root: PathBuf,
    opening: WalOpening,
    run_state: Arc<RunStateFile>,
    /// Keeps a second process off the root, so two runs never interleave
    /// their run-state writes. Released by a clean shutdown, or when the
    /// node's WAL is dropped.
    lock: Mutex<Option<DiskLock>>,
}

impl NodeWal {
    /// Starts a run on the WAL under `root`: takes its lock, reads the run
    /// state the previous run left, decides how to open the journals, and
    /// durably records this run before any journal write.
    pub(crate) fn start(
        root: PathBuf,
        fsync: WalFsync,
        topology: &ursula_shard::StaticShardMap,
    ) -> Result<Self, RaftWalError> {
        create_dir_all_durable(&root).map_err(|source| RaftWalError::CreateDir {
            path: root.clone(),
            source,
        })?;
        let lock_path = root.join(WAL_LOCK_FILE);
        let lock = match Disk::try_lock(&lock_path).map_err(|source| RaftWalError::Lock {
            path: lock_path.clone(),
            source,
        })? {
            LockAttempt::Acquired(lock) => lock,
            LockAttempt::Held { owner } => {
                return Err(RaftWalError::Locked {
                    path: lock_path,
                    owner,
                });
            }
        };
        let path = root.join(RUN_STATE_FILE);
        let previous = state_file::read::<RunState>(StateFileKind::RunState, &path)
            .map_err(RaftWalError::ReadRunState)?;
        let cores = core_dirs(&root)?;
        // Even metadata-only cores are prior state: votes must not be forgotten.
        super::topology::check_or_create(&root, topology, previous.is_some() || !cores.is_empty())?;
        let journals = journal_history(&cores)?;
        let boot_id = Disk::boot_id(&root).map(BootId);
        let mut opening = WalOpening::decide(previous.as_ref(), journals, boot_id.as_ref(), fsync);
        if previous.is_none() && !cores.is_empty() {
            let mut greatest_epoch = 0;
            for core in &cores {
                let metadata = super::core_meta::CoreMetadata::load(&core_metadata_path(core))
                    .map_err(RaftWalError::ReadCoreMetadata)?;
                greatest_epoch = greatest_epoch.max(metadata.verified_epoch());
            }
            opening.recovery_epoch = greatest_epoch.saturating_add(1);
        }
        log_opening(&root, previous.as_ref(), boot_id.as_ref(), fsync, &opening);
        // Before this run records itself, so a crash in between marks the
        // cores again on the next start.
        if let RecoveryState::Recovering { .. } = opening.recovery {
            for core in &cores {
                let path = core_metadata_path(core);
                mark_core_recovering(&path)
                    .map_err(|source| RaftWalError::MarkRecovering { path, source })?;
            }
        }
        // Before this run records `always`, so a host crash in between still
        // reads as one under `never`.
        if opening.journal_sync == JournalSync::BeforeRecording {
            for core in &cores {
                segment::sync_journal(core).map_err(|source| RaftWalError::SyncJournal {
                    path: core.clone(),
                    source,
                })?;
            }
        }
        let run_state = RunStateFile::new(path, RunState {
            boot_id,
            fsync,
            status: RunStatus::Running,
            recovery_epoch: opening.recovery_epoch,
        });
        run_state
            .record(RunStatus::Running, "running")
            .map_err(RaftWalError::RecordRunState)?;
        Ok(Self {
            root,
            opening,
            run_state: Arc::new(run_state),
            lock: Mutex::new(Some(lock)),
        })
    }

    pub(crate) fn root(&self) -> &Path {
        &self.root
    }

    pub(crate) fn fsync(&self) -> WalFsync {
        self.run_state.current().fsync
    }

    pub(crate) fn opening(&self) -> WalOpening {
        self.opening
    }

    pub(crate) fn run_state(&self) -> &Arc<RunStateFile> {
        &self.run_state
    }

    /// Records a clean end of this run, once every journal is `fsync`ed,
    /// and lets the next run take the root.
    pub(crate) fn record_clean(&self) -> Result<(), RaftWalError> {
        self.run_state
            .record(RunStatus::Clean, "clean")
            .map_err(RaftWalError::RecordRunState)?;
        drop(
            self.lock
                .lock()
                .map_err(|_poisoned| RaftWalError::LockPoisoned)?
                .take(),
        );
        Ok(())
    }
}

/// The core directories under the WAL `root`, in name order.
fn core_dirs(root: &Path) -> Result<Vec<PathBuf>, RaftWalError> {
    let entries = Disk::read_dir(root).map_err(|source| RaftWalError::ListCores {
        root: root.to_owned(),
        source,
    })?;
    Ok(entries
        .into_iter()
        .filter(|entry| {
            entry
                .file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with(CORE_DIR_PREFIX))
        })
        .collect())
}

/// Whether any of the `cores`' journals holds records.
fn journal_history(cores: &[PathBuf]) -> Result<JournalHistory, RaftWalError> {
    for core in cores {
        if segment::holds_records(core).map_err(|source| RaftWalError::ReadJournal {
            path: core.clone(),
            source,
        })? {
            return Ok(JournalHistory::Records);
        }
    }
    Ok(JournalHistory::Empty)
}

fn log_opening(
    root: &Path,
    previous: Option<&RunState>,
    boot_id: Option<&BootId>,
    fsync: WalFsync,
    opening: &WalOpening,
) {
    tracing::info!(
        root = %root.display(),
        ?fsync,
        boot_id = boot_id.map(BootId::as_str),
        previous_boot_id = previous.and_then(|previous| previous.boot_id.as_ref()).map(BootId::as_str),
        previous_fsync = ?previous.map(|previous| previous.fsync),
        previous_run = ?opening.previous_run,
        replay_mode = ?opening.replay_mode,
        recovery_epoch = opening.recovery_epoch,
        "opening the Raft WAL"
    );
    if let RecoveryState::Recovering { reason } = opening.recovery {
        tracing::warn!(
            root = %root.display(),
            ?reason,
            previous_run = ?opening.previous_run,
            "this node's Raft logs may be missing entries it acknowledged"
        );
    }
    if opening.journal_sync == JournalSync::BeforeRecording {
        tracing::info!(
            root = %root.display(),
            "the previous run acknowledged writes under fsync = never and its process stopped; \
             syncing every journal before this run records fsync = always"
        );
    }
}

#[cfg(test)]
mod tests {
    use ursula_config::WalFsync;

    use super::BootId;
    use super::JournalHistory;
    use super::JournalReplayMode;
    use super::JournalSync;
    use super::PreviousRun;
    use super::RecoveryReason;
    use super::RecoveryState;
    use super::RunState;
    use super::RunStatus;
    use super::WalOpening;
    use super::core_replay_mode;

    fn boot(id: &str) -> BootId {
        BootId::new(id)
    }

    fn previous(boot_id: Option<&str>, fsync: WalFsync, status: RunStatus) -> RunState {
        RunState {
            boot_id: boot_id.map(boot),
            fsync,
            status,
            recovery_epoch: 4,
        }
    }

    /// The previous run's state, what the journals hold, the current boot
    /// id, and what opening decides.
    type Row = (
        Option<RunState>,
        JournalHistory,
        Option<&'static str>,
        PreviousRun,
        JournalReplayMode,
        RecoveryState,
    );

    const NORMAL: RecoveryState = RecoveryState::Normal;
    const HOST_CRASH: RecoveryState = RecoveryState::Recovering {
        reason: RecoveryReason::HostCrash,
    };
    const POISONED: RecoveryState = RecoveryState::Recovering {
        reason: RecoveryReason::Poisoned,
    };
    const UNKNOWN_HISTORY: RecoveryState = RecoveryState::Recovering {
        reason: RecoveryReason::UnknownHistory,
    };
    const STRICT: JournalReplayMode = JournalReplayMode::Strict;
    const PREFIX: JournalReplayMode = JournalReplayMode::VerifiedPrefix;

    /// Every row of the "Opening the journal" table, for both policies and
    /// with the boot id known or not.
    #[test]
    fn opening_the_journal_follows_the_decision_table() {
        use JournalHistory::Empty;
        use JournalHistory::Records;
        use RunStatus::Clean;
        use RunStatus::Poisoned;
        use RunStatus::Running;
        use WalFsync::Always;
        use WalFsync::Never;

        let rows: &[Row] = &[
            // No run state and no journal record: a new node or a new disk.
            (None, Empty, Some("b"), PreviousRun::Absent, STRICT, NORMAL),
            (None, Empty, None, PreviousRun::Absent, STRICT, NORMAL),
            // No run state, but journals hold records: fail safe.
            (
                None,
                Records,
                Some("b"),
                PreviousRun::Unrecorded,
                PREFIX,
                UNKNOWN_HISTORY,
            ),
            (
                None,
                Records,
                None,
                PreviousRun::Unrecorded,
                PREFIX,
                UNKNOWN_HISTORY,
            ),
            // A clean shutdown, under either policy, on any boot.
            (
                Some(previous(Some("a"), Always, Clean)),
                Records,
                Some("b"),
                PreviousRun::Clean,
                STRICT,
                NORMAL,
            ),
            (
                Some(previous(Some("a"), Never, Clean)),
                Records,
                Some("a"),
                PreviousRun::Clean,
                STRICT,
                NORMAL,
            ),
            (
                Some(previous(None, Never, Clean)),
                Records,
                None,
                PreviousRun::Clean,
                STRICT,
                NORMAL,
            ),
            // Same boot: a process crash; the page cache kept every write.
            (
                Some(previous(Some("a"), Never, Running)),
                Records,
                Some("a"),
                PreviousRun::ProcessCrash,
                STRICT,
                NORMAL,
            ),
            (
                Some(previous(Some("a"), Always, Running)),
                Records,
                Some("a"),
                PreviousRun::ProcessCrash,
                STRICT,
                NORMAL,
            ),
            // Another boot: a host crash.
            (
                Some(previous(Some("a"), Never, Running)),
                Records,
                Some("b"),
                PreviousRun::HostCrash { fsync: Never },
                PREFIX,
                HOST_CRASH,
            ),
            (
                Some(previous(Some("a"), Always, Running)),
                Records,
                Some("b"),
                PreviousRun::HostCrash { fsync: Always },
                PREFIX,
                NORMAL,
            ),
            // An unknown boot id without a clean end counts as a host crash.
            (
                Some(previous(None, Never, Running)),
                Records,
                None,
                PreviousRun::HostCrash { fsync: Never },
                PREFIX,
                HOST_CRASH,
            ),
            (
                Some(previous(Some("a"), Never, Running)),
                Records,
                None,
                PreviousRun::HostCrash { fsync: Never },
                PREFIX,
                HOST_CRASH,
            ),
            (
                Some(previous(None, Always, Running)),
                Records,
                Some("a"),
                PreviousRun::HostCrash { fsync: Always },
                PREFIX,
                NORMAL,
            ),
            // An I/O error stopped the previous run, under either policy.
            (
                Some(previous(Some("a"), Always, Poisoned)),
                Records,
                Some("a"),
                PreviousRun::Poisoned,
                PREFIX,
                POISONED,
            ),
            (
                Some(previous(Some("a"), Never, Poisoned)),
                Records,
                Some("b"),
                PreviousRun::Poisoned,
                PREFIX,
                POISONED,
            ),
        ];
        for (previous, journals, boot_id, previous_run, replay_mode, recovery) in rows {
            let boot_id = boot_id.map(boot);
            // The same policy as the previous run: nothing to sync first.
            let fsync = previous
                .as_ref()
                .map_or(WalFsync::Never, |previous| previous.fsync);
            let opening = WalOpening::decide(previous.as_ref(), *journals, boot_id.as_ref(), fsync);
            let prior_epoch = previous
                .as_ref()
                .map_or(0, |previous| previous.recovery_epoch);
            let recovery_epoch = match replay_mode {
                JournalReplayMode::Strict => prior_epoch,
                JournalReplayMode::VerifiedPrefix => prior_epoch.saturating_add(1),
            };
            assert_eq!(
                opening,
                WalOpening {
                    previous_run: *previous_run,
                    replay_mode: *replay_mode,
                    recovery: *recovery,
                    recovery_epoch,
                    journal_sync: JournalSync::NotNeeded,
                },
                "previous {previous:?}, journals {journals:?}, boot {boot_id:?}"
            );
        }
    }

    /// A run that records `always` `fsync`s the journals first exactly when
    /// the previous run acknowledged writes under `never` and its process
    /// stopped on this boot. A host crash, an I/O failure or an unknown
    /// history already gates the groups, a clean end synced everything, and
    /// a run that stays on `never` records the weaker policy.
    #[test]
    fn a_switch_to_always_syncs_what_a_never_run_left_in_the_page_cache() {
        use RunStatus::Clean;
        use RunStatus::Poisoned;
        use RunStatus::Running;
        use WalFsync::Always;
        use WalFsync::Never;

        let cases = [
            (
                Some(previous(Some("a"), Never, Running)),
                Some("a"),
                Always,
                true,
            ),
            (
                Some(previous(Some("a"), Never, Running)),
                Some("a"),
                Never,
                false,
            ),
            (
                Some(previous(Some("a"), Always, Running)),
                Some("a"),
                Always,
                false,
            ),
            (
                Some(previous(Some("a"), Never, Running)),
                Some("b"),
                Always,
                false,
            ),
            (
                Some(previous(Some("a"), Never, Running)),
                None,
                Always,
                false,
            ),
            (
                Some(previous(Some("a"), Never, Clean)),
                Some("a"),
                Always,
                false,
            ),
            (
                Some(previous(Some("a"), Never, Poisoned)),
                Some("a"),
                Always,
                false,
            ),
            (None, Some("a"), Always, false),
        ];
        for (previous, boot_id, fsync, sync) in cases {
            let boot_id = boot_id.map(boot);
            let opening = WalOpening::decide(
                previous.as_ref(),
                JournalHistory::Records,
                boot_id.as_ref(),
                fsync,
            );
            let expected = if sync {
                JournalSync::BeforeRecording
            } else {
                JournalSync::NotNeeded
            };
            assert_eq!(
                opening.journal_sync, expected,
                "previous {previous:?}, boot {boot_id:?}, now {fsync:?}"
            );
        }
    }

    /// A journal not read back since the recovery epoch began is still read
    /// as a verified prefix, however the runs since then ended.
    #[test]
    fn a_core_journal_is_read_strictly_only_once_verified_in_the_current_epoch() {
        assert_eq!(core_replay_mode(0, 0), STRICT);
        assert_eq!(core_replay_mode(3, 3), STRICT);
        assert_eq!(core_replay_mode(2, 3), PREFIX);
        // A missing run state restarts the count. An older verification
        // must not make the new unknown-history epoch read strictly.
        assert_eq!(core_replay_mode(5, 0), PREFIX);
        assert_eq!(core_replay_mode(5, 1), PREFIX);

        // A host crash starts epoch 5; a process crash right after it, before
        // a core was read, keeps epoch 5, so that core still reads its prefix.
        let crashed = previous(Some("a"), WalFsync::Always, RunStatus::Running);
        let after_host_crash = WalOpening::decide(
            Some(&crashed),
            JournalHistory::Records,
            Some(&boot("b")),
            WalFsync::Always,
        );
        assert_eq!(after_host_crash.recovery_epoch, 5);
        let restarted = RunState {
            boot_id: Some(boot("b")),
            recovery_epoch: after_host_crash.recovery_epoch,
            ..crashed
        };
        let after_process_crash = WalOpening::decide(
            Some(&restarted),
            JournalHistory::Records,
            Some(&boot("b")),
            WalFsync::Always,
        );
        assert_eq!(after_process_crash.replay_mode, STRICT);
        assert_eq!(after_process_crash.recovery_epoch, 5);
        assert_eq!(
            core_replay_mode(4, after_process_crash.recovery_epoch),
            PREFIX
        );
        assert_eq!(
            core_replay_mode(5, after_process_crash.recovery_epoch),
            STRICT
        );
    }
}

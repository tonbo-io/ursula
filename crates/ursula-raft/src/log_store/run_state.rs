//! The node's run state, and how the journals open after the run before.
//!
//! The run-state file at the WAL root records the run that last opened the
//! journals: the host's boot id at the time, how the run ended
//! ([`RunStatus`]), and for each class of journal ([`JournalClass`]) the
//! fsync policy it ran and its recovery epoch ([`JournalRun`]). A run reads
//! it first, decides how to read each class's journals
//! ([`WalOpening::decide`], the "Startup and replay" table in
//! docs/architecture/raft-wal.md), and durably records itself as
//! [`RunStatus::Running`] before any journal write. A graceful shutdown
//! records [`RunStatus::Clean`] after every journal is `fsync`ed; an I/O
//! failure that poisons a journal writer records [`RunStatus::Poisoned`]
//! before the process stops.
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
//!
//! The rule is kept per journal class, since the classes run different
//! policies: the data journals (`core-*`) run the configured one, and the
//! meta journal (`meta`) always runs `always`. Each class opens by its own
//! record and its own journals' history, through the same decision table,
//! over the node-wide status and boot id: a host crash under a data policy
//! of `never` gives the data journals a verified prefix and a new epoch, and
//! leaves the meta journal strict with its epoch unmoved.
//!
//! No failure to read or mark the meta journal stops a start: it opens the
//! meta class [`MetaOpening::Damaged`] for the run, which still records the
//! epoch it decided for meta, and the data journals open as they would.
//!
//! The meta journal exists once its metadata file does, or a segment of it
//! holds a record. Its first open puts the metadata file in place, in one
//! atomic replace, before the journal holds anything, so a directory without
//! either is a creation that stopped and holds no journal.

use std::io;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::PoisonError;

use serde::Deserialize;
use serde::Serialize;
use ursula_config::WalFsync;
use ursula_proto::placement::NodeId;
use ursula_proto::placement::ProcessIncarnation;
use ursula_runtime::NodeFence;
use ursula_runtime::node_fence::FenceInstant;
use ursula_runtime::node_fence::FenceStatus;
use ursula_runtime::node_fence::ProbeOutcome;
use ursula_runtime::node_fence::fence_now;

use super::core_meta::CoreMetadata;
use super::core_meta::GroupLogState;
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
/// The meta journal's policy, whatever the data journals run.
pub(crate) const META_FSYNC: WalFsync = WalFsync::Always;
/// The meta journal's directory under the WAL root. It does not start with
/// [`CORE_DIR_PREFIX`], so no data reader of the root lists it as a core.
const META_DIR: &str = "meta";

/// The journal directory of core `core` under the WAL `root`.
pub(crate) fn core_dir(root: &Path, core: u16) -> PathBuf {
    root.join(format!("{CORE_DIR_PREFIX}{core}"))
}

/// The meta journal's directory under the WAL `root`.
pub(crate) fn meta_dir(root: &Path) -> PathBuf {
    root.join(META_DIR)
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

/// A class of journal under a WAL root. Each opens by its own record in the
/// run state ([`RunState::journal`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JournalClass {
    /// The core journals (`core-*`), which hold the data groups' logs and
    /// run the configured fsync policy.
    Data,
    /// The meta group's journal (`meta`), which always runs `fsync =
    /// always`.
    Meta,
}

impl std::fmt::Display for JournalClass {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Data => "data",
            Self::Meta => "meta",
        })
    }
}

/// What the run state records for one class of journal.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct JournalRun {
    /// The fsync policy the class's journals run.
    pub fsync: WalFsync,
    /// Runs on this WAL root that read this class's journals as a verified
    /// prefix, including this one: after a host crash under this class's
    /// policy of `never`, after a lost run state, or after an I/O failure.
    /// The run status is node-wide, so an I/O failure of any journal
    /// advances every class's epoch.
    pub recovery_epoch: u64,
}

/// The run-state file's contents.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunState {
    /// The host's boot when the run started; `None` where the platform
    /// reports none.
    pub boot_id: Option<BootId>,
    pub status: RunStatus,
    /// The data journals' policy and epoch.
    pub data: JournalRun,
    /// The meta journal's policy, always `always`, and epoch.
    pub meta: JournalRun,
    /// The device the WAL root was mounted from when the run started
    /// (`st_dev`, [`super::disk::JournalDisk::device`]): another device on
    /// the same boot is another mount, whose page cache is not this one's.
    pub device: u64,
}

impl RunState {
    /// What this run state records for the journals of `class`.
    pub fn journal(&self, class: JournalClass) -> &JournalRun {
        match class {
            JournalClass::Data => &self.data,
            JournalClass::Meta => &self.meta,
        }
    }
}

/// Whether the journals of one class under a WAL root hold records.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JournalHistory {
    /// No journal of the class holds a record; metadata may retain a
    /// verified epoch.
    Empty { verified_epoch: u64 },
    /// Some journal of the class holds records.
    Records { verified_epoch: u64 },
}

impl JournalHistory {
    /// The history of journals that hold a record or not (`records`), with
    /// the greatest surviving `verified_epoch`.
    fn of(records: bool, verified_epoch: u64) -> Self {
        if records {
            Self::Records { verified_epoch }
        } else {
            Self::Empty { verified_epoch }
        }
    }

    fn verified_epoch(self) -> u64 {
        match self {
            Self::Empty { verified_epoch } | Self::Records { verified_epoch } => verified_epoch,
        }
    }
}

/// How the previous run that opened the journals ended, read from its run
/// state and the current boot id.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum PreviousRun {
    /// No run state, journal record or prior verification: a new node or disk.
    Absent,
    /// No run state, but core journals retain records or verification: the
    /// run state was lost, so how the run that wrote them ended is unknown.
    Unrecorded,
    /// The previous run shut down cleanly: every journal was `fsync`ed.
    Clean,
    /// The previous run's process stopped on the host that still runs, on
    /// the same mount of the root, so the page cache kept every write.
    ProcessCrash,
    /// The previous run did not shut down cleanly, and the root is mounted
    /// from another device on the same boot: the volume moved, and the old
    /// mount's page cache is not this one's, so the run reads as a host
    /// crash under `fsync`, the policy the class's journals ran.
    Remounted { fsync: WalFsync },
    /// The previous run did not shut down cleanly and the boot id changed, or
    /// is unknown: the host crashed and writeback may have left holes in what
    /// the run wrote without `fsync`. `fsync` is the policy the class's
    /// journals ran.
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
    /// The root was mounted from another device after a run that did not
    /// shut down cleanly while the policy was `never`.
    Remounted,
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

/// Whether a process that ran this WAL root before this one may still run,
/// read once at start ([`Predecessor::of`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Predecessor {
    /// A process of this root may still run: nothing proves the run before
    /// ended, or an earlier run left its wait unfinished.
    MayLive,
    /// The run before ended: it recorded its end, or its process held the
    /// root's lock on this mount of this kernel.
    Gone,
}

impl Predecessor {
    /// The predecessor of a run whose root was `empty` when it opened (no
    /// core or meta journal held a record, no core or meta metadata held a
    /// vote, and no snapshot record existed), after `previous_run` and with
    /// `predecessor_wait`, the wait the identity file says is owed. The
    /// first matching row decides:
    ///
    /// - an empty root may live: a fresh root of a node id whose old
    ///   process may still run;
    /// - an owed wait may live: a restart never shortens one;
    /// - `Clean`, `Poisoned` and `ProcessCrash` are gone: the run recorded
    ///   its end, a poisoned writer aborts its process after recording it,
    ///   and a crashed process held the `flock` on this mount of this
    ///   kernel;
    /// - `Remounted`, `HostCrash`, `Unrecorded` and `Absent` may live:
    ///   without a host identity a reboot and a moved volume look alike,
    ///   and a lost run state proves nothing.
    pub fn of(previous_run: PreviousRun, predecessor_wait: bool, empty: bool) -> Self {
        match previous_run {
            _ if empty => Self::MayLive,
            _ if predecessor_wait => Self::MayLive,
            PreviousRun::Clean | PreviousRun::Poisoned | PreviousRun::ProcessCrash => Self::Gone,
            PreviousRun::Remounted { .. }
            | PreviousRun::HostCrash { .. }
            | PreviousRun::Unrecorded
            | PreviousRun::Absent => Self::MayLive,
        }
    }
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

/// How this run opens the meta journal.
#[derive(Debug, Clone)]
pub enum MetaOpening {
    /// The meta journal opens by the meta class's record and its own
    /// history, as the data journals open by theirs.
    Decided(WalOpening),
    /// The start could not read or mark the meta journal. The run records
    /// `opening`, decided as if the journal held records, and opens no meta
    /// journal. Its groups are not marked, so a later run that reads the
    /// journal below the epoch recorded here opens them recovering.
    Damaged {
        opening: WalOpening,
        damage: MetaJournalDamage,
    },
}

impl MetaOpening {
    /// The decision this run records for the meta class, whether or not the
    /// meta journal could be read.
    pub fn opening(&self) -> WalOpening {
        match self {
            Self::Decided(opening) | Self::Damaged { opening, .. } => *opening,
        }
    }
}

/// Why the meta journal does not open in this run. None of them stops the
/// node: the data journals open as they would.
#[derive(Debug, Clone, thiserror::Error)]
pub enum MetaJournalDamage {
    #[error("read the meta journal '{}': {source}", .path.display())]
    ReadJournal {
        path: PathBuf,
        #[source]
        source: Arc<JournalError>,
    },
    #[error("read the meta journal's metadata '{}': {source}", .path.display())]
    ReadMetadata {
        path: PathBuf,
        #[source]
        source: Arc<StateFileError>,
    },
    #[error("record the meta journal's group recovering in '{}': {source}", .path.display())]
    MarkRecovering {
        path: PathBuf,
        #[source]
        source: Arc<MarkRecoveringError>,
    },
    #[error("put the new meta journal's metadata '{}' in place: {source}", .path.display())]
    WriteMetadata {
        path: PathBuf,
        #[source]
        source: Arc<JournalError>,
    },
    #[error("create the meta journal directory '{}': {source}", .path.display())]
    CreateDir {
        path: PathBuf,
        #[source]
        source: Arc<io::Error>,
    },
    /// The meta journal's writer did not open: a sealed segment, a segment
    /// header, a frozen archive or the replayed log is damaged.
    #[error("open the meta journal: {0}")]
    OpenJournal(#[source] super::CoreJournalError),
}

/// Whether the meta journal existed when the run started, before anything
/// of the run touched the WAL root: its metadata file, or a segment holding
/// a record. A directory with neither, which a first open that stopped
/// before its metadata file was in place leaves, is no journal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MetaAtStart {
    Absent,
    Present,
}

/// What the start read of the meta journal.
#[derive(Debug)]
enum MetaRead {
    /// No meta journal ([`MetaAtStart::Absent`]), and whether the meta
    /// group's snapshot record remains without it.
    Absent { snapshot_records: bool },
    /// The meta journal, what it holds, and whether it holds a record, a
    /// vote or a snapshot record (`state`).
    Read {
        history: JournalHistory,
        state: bool,
    },
    /// The meta journal could not be read.
    Unread(MetaJournalDamage),
}

impl MetaRead {
    /// Reads the meta journal in `dir`, if there is one.
    fn of(dir: &Path) -> Self {
        match read_journal(dir) {
            Ok(JournalRead {
                records: false,
                metadata: false,
                snapshot_records,
                ..
            }) => Self::Absent { snapshot_records },
            Ok(read) => Self::Read {
                history: JournalHistory::of(read.records, read.verified_epoch),
                state: read.state(),
            },
            Err(unread) => Self::Unread(unread.into()),
        }
    }

    fn at_start(&self) -> MetaAtStart {
        match self {
            Self::Absent { .. } => MetaAtStart::Absent,
            Self::Read { .. } | Self::Unread(_) => MetaAtStart::Present,
        }
    }

    /// Whether the meta journal holds a record, a vote or a snapshot
    /// record. A journal that could not be read counts as holding them.
    fn holds_state(&self) -> bool {
        match self {
            Self::Absent { snapshot_records } => *snapshot_records,
            Self::Read { state, .. } => *state,
            Self::Unread(_) => true,
        }
    }
}

impl PreviousRun {
    /// Reads `previous`, the run state the previous run left, for the
    /// journals of `class`, given what they hold, the current `boot_id` and
    /// the `device` the root is mounted from now.
    pub fn interpret(
        previous: Option<&RunState>,
        class: JournalClass,
        journals: JournalHistory,
        boot_id: Option<&BootId>,
        device: u64,
    ) -> Self {
        let Some(previous) = previous else {
            return match journals {
                JournalHistory::Empty { verified_epoch: 0 } => Self::Absent,
                // Even an empty journal with prior verification is surviving
                // history, not evidence of a fresh disk.
                JournalHistory::Empty { .. } | JournalHistory::Records { .. } => Self::Unrecorded,
            };
        };
        match previous.status {
            RunStatus::Clean => Self::Clean,
            RunStatus::Poisoned => Self::Poisoned,
            RunStatus::Running => match (previous.boot_id.as_ref(), boot_id) {
                (Some(before), Some(now)) if before == now && previous.device == device => {
                    Self::ProcessCrash
                }
                (Some(before), Some(now)) if before == now => Self::Remounted {
                    fsync: previous.journal(class).fsync,
                },
                _ => Self::HostCrash {
                    fsync: previous.journal(class).fsync,
                },
            },
        }
    }

    /// Whether a possibly lossy run starts a new prefix-verification epoch.
    /// Sealed segments always verify strictly. Newest-segment invalid tails
    /// are repaired only after the core durably closes its recovery gates.
    pub fn replay_mode(self) -> JournalReplayMode {
        match self {
            Self::Absent
            | Self::Clean
            | Self::ProcessCrash
            | Self::HostCrash {
                fsync: WalFsync::Always,
            }
            | Self::Remounted {
                fsync: WalFsync::Always,
            } => JournalReplayMode::Strict,
            Self::Unrecorded
            | Self::HostCrash {
                fsync: WalFsync::Never,
            }
            | Self::Remounted {
                fsync: WalFsync::Never,
            }
            | Self::Poisoned => JournalReplayMode::VerifiedPrefix,
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
            }
            | Self::Remounted {
                fsync: WalFsync::Always,
            } => RecoveryState::Normal,
            Self::HostCrash {
                fsync: WalFsync::Never,
            } => RecoveryState::Recovering {
                reason: RecoveryReason::HostCrash,
            },
            Self::Remounted {
                fsync: WalFsync::Never,
            } => RecoveryState::Recovering {
                reason: RecoveryReason::Remounted,
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
    /// The "Startup and replay" decision for the journals of `class` in a
    /// run on `boot_id`, with the root mounted from `device`, that runs them
    /// with the policy `fsync`, after the run that left `previous` and what
    /// those `journals` hold.
    pub fn decide(
        previous: Option<&RunState>,
        class: JournalClass,
        journals: JournalHistory,
        boot_id: Option<&BootId>,
        device: u64,
        fsync: WalFsync,
    ) -> Result<Self, RaftWalError> {
        let previous_run = PreviousRun::interpret(previous, class, journals, boot_id, device);
        let replay_mode = previous_run.replay_mode();
        let recorded = previous.map(|previous| *previous.journal(class));
        let prior_epoch = recorded
            .map_or(0, |recorded| recorded.recovery_epoch)
            .max(journals.verified_epoch());
        let recovery_epoch = match replay_mode {
            JournalReplayMode::Strict => prior_epoch,
            JournalReplayMode::VerifiedPrefix => {
                prior_epoch
                    .checked_add(1)
                    .ok_or(RaftWalError::RecoveryEpochExhausted {
                        class,
                        recovery_epoch: prior_epoch,
                    })?
            }
        };
        let journal_sync = match (previous_run, recorded.map(|recorded| recorded.fsync), fsync) {
            (PreviousRun::ProcessCrash, Some(WalFsync::Never), WalFsync::Always) => {
                JournalSync::BeforeRecording
            }
            _ => JournalSync::NotNeeded,
        };
        Ok(Self {
            previous_run,
            replay_mode,
            recovery: previous_run.recovery_state(),
            recovery_epoch,
            journal_sync,
        })
    }
}

/// How a journal is read: in [`JournalReplayMode::VerifiedPrefix`] when it
/// has not been read back since its class's current recovery epoch began.
/// `recovery_epoch` is the epoch of the journal's own class.
pub(crate) fn core_replay_mode(verified_epoch: u64, recovery_epoch: u64) -> JournalReplayMode {
    if verified_epoch < recovery_epoch {
        JournalReplayMode::VerifiedPrefix
    } else {
        JournalReplayMode::Strict
    }
}

/// Failure to start, open a group on, or shut down a node's Raft WAL.
#[derive(Debug, thiserror::Error)]
pub enum RaftWalError {
    #[error("open the journal for core {core:?}: {source}")]
    OpenCore {
        core: ursula_shard::CoreId,
        #[source]
        source: super::CoreJournalError,
    },
    #[error("open raft group {:?} on core {:?}: {source}", .placement.raft_group_id, .placement.core_id)]
    OpenGroup {
        placement: ursula_shard::ShardPlacement,
        #[source]
        source: super::CoreJournalError,
    },
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
    #[error("the WAL at '{}' belongs to cluster '{stored}', but server.cluster_id is '{configured}'. A disk does not move between clusters: set server.cluster_id to '{stored}', or start this node on an empty WAL", .root.display())]
    ClusterIdMismatch {
        root: PathBuf,
        stored: ursula_headers::ClusterId,
        configured: ursula_headers::ClusterId,
    },
    #[error("the WAL topology at '{}' holds an invalid cluster id '{stored}'", .root.display())]
    InvalidStoredClusterId { root: PathBuf, stored: String },
    #[error("the WAL at '{}' belongs to node {stored}, but raft.node_id is {configured}. A WAL root opens only under the node id that created it: start node {stored} on it, or start node {configured} on an empty WAL", .root.display())]
    OtherNode {
        root: PathBuf,
        stored: ursula_proto::placement::NodeId,
        configured: ursula_proto::placement::NodeId,
    },
    #[error("the WAL identity file is missing at '{}' but prior WAL state exists. Refusing to count this root's runs again from the start, which would make this process read as an earlier one. Start the node on an empty WAL instead", .root.display())]
    MissingIdentity { root: PathBuf },
    #[error("the WAL at '{}' counted {run} runs, the most a root counts: start the node on an empty WAL", .root.display())]
    RunCounterExhausted { root: PathBuf, run: u32 },
    #[error("read the Raft WAL identity file: {0}")]
    ReadIdentity(#[source] StateFileError),
    #[error("record the Raft WAL identity file: {0}")]
    RecordIdentity(#[source] JournalError),
    #[error("read the device of the Raft WAL root '{}': {source}", .path.display())]
    Device {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("start the disk lease's probe of the Raft WAL root '{}': {source}", .root.display())]
    LeaseProbe {
        root: PathBuf,
        #[source]
        source: std::io::Error,
    },
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
    #[error("Raft WAL recovery epoch {recovery_epoch} of the {class} journals cannot advance")]
    RecoveryEpochExhausted {
        class: JournalClass,
        recovery_epoch: u64,
    },
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
    #[error("stop the journal of core {}: {source}", .core.0)]
    CloseJournal {
        core: ursula_shard::CoreId,
        #[source]
        source: super::CoreJournalError,
    },
    /// A maintenance pass of a core's journal could not be asked for: its
    /// writer stopped or is poisoned.
    #[error("maintain the journal of core {}: {source}", .core.0)]
    MaintainJournal {
        core: ursula_shard::CoreId,
        #[source]
        source: super::CoreJournalError,
    },
    #[error("stop the meta journal: {source}")]
    CloseMetaJournal {
        #[source]
        source: super::CoreJournalError,
    },
    /// A maintenance pass of the meta journal could not be asked for: its
    /// writer stopped or is poisoned.
    #[error("maintain the meta journal: {source}")]
    MaintainMetaJournal {
        #[source]
        source: super::CoreJournalError,
    },
    #[error("Raft WAL state mutex poisoned")]
    LockPoisoned,
    /// The run's node fence does not admit, so the run records nothing
    /// more on its root: its disk lease does not hold, or the run shut
    /// down ([`FenceStatus::Closed`], for good).
    #[error("the Raft WAL root '{}' is fenced ({status:?}): this run writes nothing more to it", .root.display())]
    Fenced { root: PathBuf, status: FenceStatus },
    /// The journal holds no log of the group that `admission` may open: an
    /// `Open` of a group the core's metadata does not name, or any open of
    /// a retired log ([`super::CoreJournalError::NotHeld`]). Nothing was
    /// written.
    #[error(
        "raft group {:?} on core {:?} holds no log here that may open as {:?}",
        .placement.raft_group_id,
        .placement.core_id,
        .admission
    )]
    NotHeld {
        placement: ursula_shard::ShardPlacement,
        admission: ursula_control::Admission,
    },
    /// The OpenRaft work of the group's previous engine still runs (#709).
    #[error(
        "raft group {:?} on core {:?} does not open: OpenRaft work of its previous engine still runs",
        .placement.raft_group_id,
        .placement.core_id
    )]
    PreviousEngineRunning {
        placement: ursula_shard::ShardPlacement,
    },
    /// The last handle of the core's previous journal writer is being
    /// dropped, which joins its worker and then releases the journal's
    /// lock. [`super::RaftWal::open_waiting`] waits on `released`, which
    /// closes once the lock is free, and opens again.
    #[error("the previous journal writer of core {:?} is still stopping", .core)]
    CoreWriterStopping {
        core: ursula_shard::CoreId,
        released: super::writer::WriterExited,
    },
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
    cluster_id: ursula_headers::ClusterId,
    /// How the data journals open.
    opening: WalOpening,
    /// What the data journals held at start.
    data_history: JournalHistory,
    /// How the meta journal opens.
    meta_opening: MetaOpening,
    /// Whether the meta journal existed at start.
    meta_at_start: MetaAtStart,
    run_state: Arc<RunStateFile>,
    /// This process: the root, the run this start counted and its nonce.
    incarnation: ProcessIncarnation,
    /// Whether the process of an earlier run of the root may still run.
    predecessor: Predecessor,
    /// Whether the root held no journal record, vote or snapshot record at
    /// start.
    empty: bool,
    /// The wait this run owes its predecessor, if it owes one.
    wait: Option<WaitFrom>,
    /// This process's fence, whose lease the probe keeps.
    fence: NodeFence,
    /// The probe of a leased fence.
    _probe: Option<super::lease::LeaseProbe>,
    /// Keeps a second process off the root, so two runs never interleave
    /// their run-state writes. Released when the node's WAL is dropped, as
    /// at a crash. A clean end hands it to the process, which holds it
    /// until it exits ([`CleanExit`]), so no successor reads this run's
    /// clean end while this process still runs.
    lock: Mutex<Option<DiskLock>>,
}

/// The root's lock of a run that recorded its clean end
/// ([`super::RaftWal::shutdown`]). The process holds it until it exits
/// ([`Self::hold_until_exit`]), so no successor opens the root, reads the
/// clean end and waits for nothing beside a process that still runs: a
/// task or thread that outlived the shutdown, a group that did not stop.
/// Dropping it stands for the process's exit, as a test that starts the
/// root again in the same process does.
#[derive(Debug)]
pub struct CleanExit {
    _lock: DiskLock,
}

impl CleanExit {
    /// Holds the root's lock until the process exits, when the operating
    /// system releases it.
    pub fn hold_until_exit(self) {
        let _held: &'static mut Self = Box::leak(Box::new(self));
    }
}

/// Where the predecessor wait of a run is measured from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WaitFrom {
    /// The open of the run, for a root that held state.
    Open(FenceInstant),
    /// The first answer of the meta group that binds the root, for an
    /// empty root: a fresh root of a node id whose old process may still
    /// run. The wait is owed from that answer on, and an answer before
    /// Bootstrap settles it at once.
    Binding,
}

impl NodeWal {
    /// Starts node `node`'s run on the WAL under `root`: takes its lock,
    /// reads the run state the previous run left, decides how each class of
    /// journal opens, counts the run in the identity file, and durably
    /// records this run before any journal write. `fsync` is the data
    /// journals' policy.
    pub(crate) fn start(
        root: PathBuf,
        fsync: WalFsync,
        topology: &ursula_shard::StaticShardMap,
        cluster_id: Option<&ursula_headers::ClusterId>,
        node: NodeId,
        fence: NodeFence,
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
        let meta = meta_dir(&root);
        let meta_read = MetaRead::of(&meta);
        let meta_at_start = meta_read.at_start();
        let meta_state = meta_read.holds_state();
        // Even metadata-only cores are prior state: votes must not be
        // forgotten. So is a meta journal.
        let has_prior_state =
            previous.is_some() || !cores.is_empty() || meta_at_start == MetaAtStart::Present;
        let bound =
            super::topology::check_or_create(&root, topology, cluster_id, node, has_prior_state)?;
        let identity = super::identity::read(&root, has_prior_state)?;
        let (journals, data_state) = journal_history(&cores)?;
        let empty = !data_state && !meta_state;
        let boot_id = Disk::boot_id(&root).map(BootId);
        let device = Disk::device(&root).map_err(|source| RaftWalError::Device {
            path: root.clone(),
            source,
        })?;
        let opening = WalOpening::decide(
            previous.as_ref(),
            JournalClass::Data,
            journals,
            boot_id.as_ref(),
            device,
            fsync,
        )?;
        let meta_opening = open_meta_class(
            &meta,
            meta_read,
            previous.as_ref(),
            boot_id.as_ref(),
            device,
        )?;
        log_opening(
            &root,
            previous.as_ref(),
            boot_id.as_ref(),
            JournalClass::Data,
            fsync,
            &opening,
        );
        // Without a meta journal, the meta class's recovery warning would
        // describe a log this node does not have.
        if meta_at_start == MetaAtStart::Present {
            log_opening(
                &root,
                previous.as_ref(),
                boot_id.as_ref(),
                JournalClass::Meta,
                META_FSYNC,
                &meta_opening.opening(),
            );
            if let MetaOpening::Damaged { damage, .. } = &meta_opening {
                tracing::error!(
                    root = %root.display(),
                    error = %damage,
                    "the meta journal could not be read at start; this run opens no meta journal \
                     and the data journals open as they would"
                );
            }
        }
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
        // Before this run records itself, so a crash in between never
        // reads the next start's predecessor as gone without the wait this
        // one owes: the identity file keeps it.
        let predecessor = Predecessor::of(opening.previous_run, identity.predecessor_wait(), empty);
        // The lease's probe file, opened here under the root's lock and
        // never again by path: a probe that reopened it after the disk went
        // away could open a later attachment's file at the same path, and
        // its success would lift a fence that must stay closed.
        let probe_file = fence
            .lease_length()
            .map(|_| super::lease::open(&root))
            .transpose()
            .map_err(|source| RaftWalError::LeaseProbe {
                root: root.clone(),
                source,
            })?;
        let opened = fence_now();
        let (_, incarnation) = super::identity::advance(&root, bound.root, identity, predecessor)?;
        // The synced identity write is the lease's first success.
        fence.record_probe(opened, ProbeOutcome::Synced, fence_now());
        let wait = (predecessor == Predecessor::MayLive && fence.lease_length().is_some())
            .then_some(if empty {
                WaitFrom::Binding
            } else {
                WaitFrom::Open(opened)
            });
        // An empty root owes its wait only once a bootstrapped cluster binds
        // it (`WaitFrom::Binding`), so a forming cluster's meta group still
        // elects.
        if let Some(WaitFrom::Open(_)) = wait {
            fence.begin_wait();
        }
        tracing::info!(
            root = %root.display(),
            %incarnation,
            ?predecessor,
            empty,
            "counted this run of the Raft WAL root"
        );
        let run_state = RunStateFile::new(path, RunState {
            boot_id,
            status: RunStatus::Running,
            data: JournalRun {
                fsync,
                recovery_epoch: opening.recovery_epoch,
            },
            meta: JournalRun {
                fsync: META_FSYNC,
                recovery_epoch: meta_opening.opening().recovery_epoch,
            },
            device,
        });
        run_state
            .record(RunStatus::Running, "running")
            .map_err(RaftWalError::RecordRunState)?;
        let probe = probe_file
            .map(|file| super::lease::spawn(&root, file, fence.clone()))
            .transpose()
            .map_err(|source| RaftWalError::LeaseProbe {
                root: root.clone(),
                source,
            })?;
        Ok(Self {
            root,
            cluster_id: bound.cluster_id,
            opening,
            data_history: journals,
            meta_opening,
            meta_at_start,
            run_state: Arc::new(run_state),
            incarnation,
            predecessor,
            empty,
            wait,
            fence,
            _probe: probe,
            lock: Mutex::new(Some(lock)),
        })
    }

    /// This process's fence.
    pub(crate) fn fence(&self) -> &NodeFence {
        &self.fence
    }

    /// The wait this run owes its predecessor, if it owes one.
    pub(crate) fn wait(&self) -> Option<WaitFrom> {
        self.wait
    }

    /// Records the predecessor wait finished, `fsync`ed, so no later run
    /// owes it again, then lets the fence admit snapshot objects and votes.
    /// Refused while the fence does not admit ([`RaftWalError::Fenced`]):
    /// a run whose lease lapsed writes nothing by path, where another
    /// attachment of the volume may be mounted now, and a run that shut
    /// down writes nothing after its clean end.
    pub(crate) fn finish_wait(&self) -> Result<(), RaftWalError> {
        match self.fence.status(fence_now()) {
            FenceStatus::Admits => {}
            status => {
                return Err(RaftWalError::Fenced {
                    root: self.root.clone(),
                    status,
                });
            }
        }
        super::identity::finish_wait(&self.root, self.incarnation.run)?;
        self.fence.end_wait();
        Ok(())
    }

    /// This process's incarnation: its root, the run this start counted
    /// and the nonce it drew.
    pub(crate) fn incarnation(&self) -> ProcessIncarnation {
        self.incarnation
    }

    /// Whether the process of an earlier run of this root may still run.
    pub(crate) fn predecessor(&self) -> Predecessor {
        self.predecessor
    }

    /// Whether the root held no journal record, vote or snapshot record at
    /// start.
    pub(crate) fn empty(&self) -> bool {
        self.empty
    }

    pub(crate) fn root(&self) -> &Path {
        &self.root
    }

    pub(crate) fn cluster_id(&self) -> &ursula_headers::ClusterId {
        &self.cluster_id
    }

    /// The data journals' fsync policy.
    pub(crate) fn fsync(&self) -> WalFsync {
        self.run_state.current().data.fsync
    }

    /// How the data journals open.
    pub(crate) fn opening(&self) -> WalOpening {
        self.opening
    }

    /// How the meta journal opens.
    pub(crate) fn meta_opening(&self) -> MetaOpening {
        self.meta_opening.clone()
    }

    /// The meta journal's directory.
    pub(crate) fn meta_dir(&self) -> PathBuf {
        meta_dir(&self.root)
    }

    /// Puts the meta journal's metadata file in place before its writer
    /// first opens in this run, when the start found no meta journal
    /// ([`MetaAtStart::Absent`]). When a data journal held records at start,
    /// or the meta group's snapshot record is still there, the file records
    /// the meta journal's group `raft_group_id` recovering.
    ///
    /// A node with data history never opens a new, empty meta log: its gate
    /// then opens closed, and the replica rejoins its meta group as a wiped
    /// voter does, instead of answering as empty and forming a meta group
    /// again beside the one it belonged to. Every node that holds data
    /// creates the meta journal first, so only a discarded or lost journal
    /// gives this. A meta journal that exists opens by what it holds, so an
    /// empty one stays empty. Data history means a segment record. A vote,
    /// the genesis vote floor included, lives only in a core's metadata
    /// file, so a node whose data journals hold votes but no record opens
    /// its lost meta journal empty, as a node without data history does.
    ///
    /// A meta snapshot record is written only once the journal exists, so
    /// one without its journal is meta history whose vote was lost with
    /// the journal: the replica restores the record, and its closed gate
    /// keeps it from voting until it rejoins, as for data history.
    ///
    /// The file appears, mark included, in one atomic replace, before the
    /// journal holds anything. A crash or a failed write before then leaves
    /// no meta journal, and the next start, or the next open of this run,
    /// does this again.
    pub(crate) fn create_meta_journal(
        &self,
        raft_group_id: ursula_shard::RaftGroupId,
    ) -> Result<(), MetaJournalDamage> {
        if self.meta_at_start == MetaAtStart::Present {
            return Ok(());
        }
        let dir = self.meta_dir();
        create_dir_all_durable(&dir).map_err(|source| MetaJournalDamage::CreateDir {
            path: dir.clone(),
            source: Arc::new(source),
        })?;
        let path = core_metadata_path(&dir);
        // An earlier open of this run may have put the file in place.
        let (mut metadata, missing) =
            CoreMetadata::load_with_presence(&path).map_err(|source| {
                MetaJournalDamage::ReadMetadata {
                    path: path.clone(),
                    source: Arc::new(source),
                }
            })?;
        let snapshot_record = dir.join(super::file::snapshot_record_file(raft_group_id.0));
        let lost = held_data_history(self.data_history) || Disk::exists(&snapshot_record);
        let marked = lost && metadata.initialize(raft_group_id.0, GroupLogState::Recovering);
        if missing || marked {
            metadata
                .store(&path)
                .map_err(|source| MetaJournalDamage::WriteMetadata {
                    path,
                    source: Arc::new(source),
                })?;
        }
        Ok(())
    }

    pub(crate) fn run_state(&self) -> &Arc<RunStateFile> {
        &self.run_state
    }

    /// Records a clean end of this run, once every journal is `fsync`ed,
    /// and hands the root's lock to the process ([`CleanExit`]). The fence
    /// closes for good first, so the run answers nothing and writes no
    /// object once a successor could read this end.
    pub(crate) fn record_clean(&self) -> Result<CleanExit, RaftWalError> {
        self.fence.close();
        self.run_state
            .record(RunStatus::Clean, "clean")
            .map_err(RaftWalError::RecordRunState)?;
        let lock = self
            .lock
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
            .ok_or_else(|| RaftWalError::ShutDown {
                root: self.root.clone(),
            })?;
        Ok(CleanExit { _lock: lock })
    }
}

/// Whether the data journals held records at start, so a lost meta journal
/// is meta history that was lost (D9's S1 rule). A negative control turns
/// it off (`Defence::MetaOverDataHistory`).
fn held_data_history(data_history: JournalHistory) -> bool {
    let held = matches!(data_history, JournalHistory::Records { .. });
    if held
        && ursula_stream::sim_controls::off(
            ursula_stream::sim_controls::Defence::MetaOverDataHistory,
        )
    {
        ursula_stream::sim_controls::note_fired(
            ursula_stream::sim_controls::Defence::MetaOverDataHistory,
        );
        return false;
    }
    held
}

/// A run-state file with distinct values in the fields of one type, for the
/// persisted-envelopes fixture. It decodes to the run state it was encoded
/// from and re-encodes to the same bytes.
#[cfg(test)]
pub(crate) fn sample_file() -> Vec<u8> {
    let state = RunState {
        boot_id: Some(BootId::new("0b3f5c1e-9d2a-4e7b-8c61-a1b2c3d4e5f6")),
        status: RunStatus::Poisoned,
        data: JournalRun {
            fsync: WalFsync::Never,
            recovery_epoch: 4,
        },
        meta: JournalRun {
            fsync: WalFsync::Always,
            recovery_epoch: 2,
        },
        device: 0x0001_0305,
    };
    let bytes = state_file::encode(StateFileKind::RunState, &state);
    let decoded: RunState =
        state_file::decode(StateFileKind::RunState, Path::new(RUN_STATE_FILE), &bytes)
            .expect("the sample decodes");
    assert_eq!(decoded, state);
    assert_eq!(state_file::encode(StateFileKind::RunState, &decoded), bytes);
    bytes
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

/// The meta class's opening, by what the start `read` of the meta journal
/// in `dir`: decided on the meta journal's own history, with its groups
/// moved into recovery when the class opens recovering, as the cores' are.
/// Without a meta journal there is nothing to mark. A journal that cannot be
/// read or marked opens the class [`MetaOpening::Damaged`] instead of
/// failing the start, and the run then records the epoch decided as if the
/// journal held records.
fn open_meta_class(
    dir: &Path,
    read: MetaRead,
    previous: Option<&RunState>,
    boot_id: Option<&BootId>,
    device: u64,
) -> Result<MetaOpening, RaftWalError> {
    let decide = |history: JournalHistory| {
        WalOpening::decide(
            previous,
            JournalClass::Meta,
            history,
            boot_id,
            device,
            META_FSYNC,
        )
    };
    let history = match read {
        MetaRead::Absent { .. } => {
            return Ok(MetaOpening::Decided(decide(JournalHistory::Empty {
                verified_epoch: 0,
            })?));
        }
        MetaRead::Unread(damage) => {
            return Ok(MetaOpening::Damaged {
                opening: decide(JournalHistory::Records { verified_epoch: 0 })?,
                damage,
            });
        }
        MetaRead::Read { history, .. } => history,
    };
    let opening = decide(history)?;
    // Before this run records itself, as for the cores. The policy of meta
    // never rises, so nothing is synced first.
    if let RecoveryState::Recovering { .. } = opening.recovery {
        let path = core_metadata_path(dir);
        if let Err(source) = mark_core_recovering(&path) {
            return Ok(MetaOpening::Damaged {
                opening,
                damage: MetaJournalDamage::MarkRecovering {
                    path,
                    source: Arc::new(source),
                },
            });
        }
    }
    Ok(MetaOpening::Decided(opening))
}

/// A journal directory [`journal_history`] could not read.
#[derive(Debug)]
enum HistoryReadError {
    Journal {
        path: PathBuf,
        source: JournalError,
    },
    Metadata {
        path: PathBuf,
        source: StateFileError,
    },
}

impl From<HistoryReadError> for RaftWalError {
    fn from(error: HistoryReadError) -> Self {
        match error {
            HistoryReadError::Journal { path, source } => Self::ReadJournal { path, source },
            HistoryReadError::Metadata { source, .. } => Self::ReadCoreMetadata(source),
        }
    }
}

impl From<HistoryReadError> for MetaJournalDamage {
    fn from(error: HistoryReadError) -> Self {
        match error {
            HistoryReadError::Journal { path, source } => Self::ReadJournal {
                path,
                source: Arc::new(source),
            },
            HistoryReadError::Metadata { path, source } => Self::ReadMetadata {
                path,
                source: Arc::new(source),
            },
        }
    }
}

/// Gather journal records and the greatest surviving verification epoch of
/// the journals in `dirs`. This performs I/O only; `WalOpening::decide` owns
/// the recovery policy.
/// Whether a journal of the class holds a record, a vote or a snapshot record
/// (`state`), with the journals' history.
fn journal_history(dirs: &[PathBuf]) -> Result<(JournalHistory, bool), HistoryReadError> {
    let mut records = false;
    let mut state = false;
    let mut verified_epoch = 0;
    for dir in dirs {
        let read = read_journal(dir)?;
        records |= read.records;
        state |= read.state();
        verified_epoch = verified_epoch.max(read.verified_epoch);
    }
    Ok((JournalHistory::of(records, verified_epoch), state))
}

/// What one journal directory holds, as a start reads it.
#[derive(Debug)]
struct JournalRead {
    /// Whether a segment holds a record.
    records: bool,
    /// Whether the metadata file exists.
    metadata: bool,
    /// The epoch the metadata file records, 0 without one.
    verified_epoch: u64,
    /// Whether the metadata file holds a vote of any group.
    votes: bool,
    /// Whether a group's snapshot record (`group-N.snapshot.json`) exists.
    snapshot_records: bool,
}

impl JournalRead {
    /// Whether the journal holds a record, a vote or a snapshot record: the
    /// state an `empty` root holds none of.
    fn state(&self) -> bool {
        self.records || self.votes || self.snapshot_records
    }
}

/// Whether the core journal in `dir` holds metadata, a segment record or a
/// snapshot record, so a start opens it ([`super::RaftWal::open_held_cores`]).
/// A directory that does not read counts as holding state: its open then
/// reports the failure.
pub(crate) fn core_holds_state(dir: &Path) -> bool {
    match read_journal(dir) {
        Ok(read) => read.records || read.metadata || read.snapshot_records,
        Err(error) => {
            let error = RaftWalError::from(error);
            tracing::warn!(
                path = %dir.display(),
                %error,
                "could not read what the core journal holds; opening it"
            );
            true
        }
    }
}

/// Reads the journal in `dir`. A missing directory holds nothing.
fn read_journal(dir: &Path) -> Result<JournalRead, HistoryReadError> {
    let records = segment::holds_records(dir).map_err(|source| HistoryReadError::Journal {
        path: dir.to_owned(),
        source,
    })?;
    let path = core_metadata_path(dir);
    let (metadata, missing) = CoreMetadata::load_with_presence(&path)
        .map_err(|source| HistoryReadError::Metadata { path, source })?;
    let snapshot_records = Disk::exists(dir)
        && Disk::read_dir(dir)
            .map_err(|source| HistoryReadError::Journal {
                path: dir.to_owned(),
                source: JournalError::io(dir, super::journal::JournalOp::Open, source),
            })?
            .iter()
            .any(|path| super::file::snapshot_record_group(path).is_some());
    Ok(JournalRead {
        records,
        metadata: !missing,
        verified_epoch: metadata.verified_epoch(),
        votes: metadata.groups().any(|(_, group)| group.vote.is_some()),
        snapshot_records,
    })
}

fn log_opening(
    root: &Path,
    previous: Option<&RunState>,
    boot_id: Option<&BootId>,
    class: JournalClass,
    fsync: WalFsync,
    opening: &WalOpening,
) {
    tracing::info!(
        root = %root.display(),
        ?class,
        ?fsync,
        boot_id = boot_id.map(BootId::as_str),
        previous_boot_id = previous.and_then(|previous| previous.boot_id.as_ref()).map(BootId::as_str),
        previous_fsync = ?previous.map(|previous| previous.journal(class).fsync),
        previous_run = ?opening.previous_run,
        replay_mode = ?opening.replay_mode,
        recovery_epoch = opening.recovery_epoch,
        "opening the Raft WAL"
    );
    if let RecoveryState::Recovering { reason } = opening.recovery {
        tracing::warn!(
            root = %root.display(),
            ?class,
            ?reason,
            previous_run = ?opening.previous_run,
            "this node's Raft logs may be missing entries it acknowledged"
        );
    }
    if opening.journal_sync == JournalSync::BeforeRecording {
        tracing::info!(
            root = %root.display(),
            ?class,
            "the previous run acknowledged writes under fsync = never and its process stopped; \
             syncing every journal before this run records fsync = always"
        );
    }
}

#[cfg(test)]
mod tests {
    use ursula_config::WalFsync;

    use super::BootId;
    use super::JournalClass;
    use super::JournalHistory;
    use super::JournalReplayMode;
    use super::JournalRun;
    use super::JournalSync;
    use super::Predecessor;
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

    /// The device the root of every previous run here was mounted from.
    const DEVICE: u64 = 0x0001_0305;

    /// A run records the run state as the persisted-envelopes fixture's
    /// sample holds it ([`super::sample_file`]).
    #[cfg(not(madsim))]
    #[test]
    fn a_recorded_run_state_is_the_fixture_sample() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join(super::RUN_STATE_FILE);
        let current = RunState {
            boot_id: Some(boot("0b3f5c1e-9d2a-4e7b-8c61-a1b2c3d4e5f6")),
            status: RunStatus::Running,
            data: JournalRun {
                fsync: WalFsync::Never,
                recovery_epoch: 4,
            },
            meta: JournalRun {
                fsync: WalFsync::Always,
                recovery_epoch: 2,
            },
            device: DEVICE,
        };
        super::RunStateFile::new(path.clone(), current.clone())
            .record(RunStatus::Poisoned, "sample")
            .expect("record");
        assert_eq!(std::fs::read(&path).expect("read"), super::sample_file());
        let read = super::state_file::read::<RunState>(super::StateFileKind::RunState, &path)
            .expect("read")
            .expect("a run state");
        assert_eq!(read, RunState {
            status: RunStatus::Poisoned,
            ..current
        });
    }

    /// A previous run whose data journals ran `fsync` in epoch 4, and whose
    /// meta journal ran `always` in epoch 2.
    fn previous(boot_id: Option<&str>, fsync: WalFsync, status: RunStatus) -> RunState {
        RunState {
            boot_id: boot_id.map(boot),
            status,
            data: JournalRun {
                fsync,
                recovery_epoch: 4,
            },
            meta: JournalRun {
                fsync: WalFsync::Always,
                recovery_epoch: 2,
            },
            device: DEVICE,
        }
    }

    /// The data decision, as the core journals take it.
    fn decide_data(
        previous: Option<&RunState>,
        journals: JournalHistory,
        boot_id: Option<&BootId>,
        fsync: WalFsync,
    ) -> Result<WalOpening, super::RaftWalError> {
        WalOpening::decide(
            previous,
            JournalClass::Data,
            journals,
            boot_id,
            DEVICE,
            fsync,
        )
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

    /// Every row of the "Startup and replay" table, for both policies and
    /// with the boot id known or not.
    #[test]
    fn opening_the_journal_follows_the_decision_table() {
        const EMPTY: JournalHistory = JournalHistory::Empty { verified_epoch: 0 };
        const RECORDS: JournalHistory = JournalHistory::Records { verified_epoch: 0 };
        use RunStatus::Clean;
        use RunStatus::Poisoned;
        use RunStatus::Running;
        use WalFsync::Always;
        use WalFsync::Never;

        let rows: &[Row] = &[
            // No run state and no journal record: a new node or a new disk.
            (None, EMPTY, Some("b"), PreviousRun::Absent, STRICT, NORMAL),
            (None, EMPTY, None, PreviousRun::Absent, STRICT, NORMAL),
            // No run state, but journals hold records: fail safe.
            (
                None,
                RECORDS,
                Some("b"),
                PreviousRun::Unrecorded,
                PREFIX,
                UNKNOWN_HISTORY,
            ),
            (
                None,
                RECORDS,
                None,
                PreviousRun::Unrecorded,
                PREFIX,
                UNKNOWN_HISTORY,
            ),
            // A clean shutdown, under either policy, on any boot.
            (
                Some(previous(Some("a"), Always, Clean)),
                RECORDS,
                Some("b"),
                PreviousRun::Clean,
                STRICT,
                NORMAL,
            ),
            (
                Some(previous(Some("a"), Never, Clean)),
                RECORDS,
                Some("a"),
                PreviousRun::Clean,
                STRICT,
                NORMAL,
            ),
            (
                Some(previous(None, Never, Clean)),
                RECORDS,
                None,
                PreviousRun::Clean,
                STRICT,
                NORMAL,
            ),
            // Same boot: a process crash; the page cache kept every write.
            (
                Some(previous(Some("a"), Never, Running)),
                RECORDS,
                Some("a"),
                PreviousRun::ProcessCrash,
                STRICT,
                NORMAL,
            ),
            (
                Some(previous(Some("a"), Always, Running)),
                RECORDS,
                Some("a"),
                PreviousRun::ProcessCrash,
                STRICT,
                NORMAL,
            ),
            // Another boot: a host crash.
            (
                Some(previous(Some("a"), Never, Running)),
                RECORDS,
                Some("b"),
                PreviousRun::HostCrash { fsync: Never },
                PREFIX,
                HOST_CRASH,
            ),
            (
                Some(previous(Some("a"), Always, Running)),
                RECORDS,
                Some("b"),
                PreviousRun::HostCrash { fsync: Always },
                STRICT,
                NORMAL,
            ),
            // An unknown boot id without a clean end counts as a host crash.
            (
                Some(previous(None, Never, Running)),
                RECORDS,
                None,
                PreviousRun::HostCrash { fsync: Never },
                PREFIX,
                HOST_CRASH,
            ),
            (
                Some(previous(Some("a"), Never, Running)),
                RECORDS,
                None,
                PreviousRun::HostCrash { fsync: Never },
                PREFIX,
                HOST_CRASH,
            ),
            (
                Some(previous(None, Always, Running)),
                RECORDS,
                Some("a"),
                PreviousRun::HostCrash { fsync: Always },
                STRICT,
                NORMAL,
            ),
            // An I/O error stopped the previous run, under either policy.
            (
                Some(previous(Some("a"), Always, Poisoned)),
                RECORDS,
                Some("a"),
                PreviousRun::Poisoned,
                PREFIX,
                POISONED,
            ),
            (
                Some(previous(Some("a"), Never, Poisoned)),
                RECORDS,
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
                .map_or(WalFsync::Never, |previous| previous.data.fsync);
            let opening = decide_data(previous.as_ref(), *journals, boot_id.as_ref(), fsync)
                .expect("valid recovery epoch");
            let prior_epoch = previous
                .as_ref()
                .map_or(0, |previous| previous.data.recovery_epoch);
            let recovery_epoch = match replay_mode {
                JournalReplayMode::Strict => prior_epoch,
                JournalReplayMode::VerifiedPrefix => {
                    prior_epoch.checked_add(1).expect("test epoch fits")
                }
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

    /// A run that did not end cleanly, on the same boot but with the root
    /// mounted from another device, is `Remounted`: replayed and gated as a
    /// host crash under its policy, with nothing synced first, since the
    /// old mount's page cache is not this one's. The device decides nothing
    /// after a clean or poisoned run, nor without a boot id, which already
    /// reads as a host crash.
    #[test]
    fn another_device_on_the_same_boot_reads_as_remounted() {
        const RECORDS: JournalHistory = JournalHistory::Records { verified_epoch: 0 };
        const OTHER: u64 = 0x0002_0001;
        let opening = |previous: &RunState, boot_id: Option<&str>, device, fsync| {
            WalOpening::decide(
                Some(previous),
                JournalClass::Data,
                RECORDS,
                boot_id.map(boot).as_ref(),
                device,
                fsync,
            )
            .expect("valid recovery epoch")
        };
        for fsync in [WalFsync::Never, WalFsync::Always] {
            let crashed = previous(Some("a"), fsync, RunStatus::Running);
            let moved = opening(&crashed, Some("a"), OTHER, fsync);
            let host_crash = opening(&crashed, Some("b"), DEVICE, fsync);
            assert_eq!(moved.previous_run, PreviousRun::Remounted { fsync });
            assert_eq!(moved.replay_mode, host_crash.replay_mode);
            assert_eq!(moved.recovery_epoch, host_crash.recovery_epoch);
            assert_eq!(moved.recovery, match fsync {
                WalFsync::Never => RecoveryState::Recovering {
                    reason: RecoveryReason::Remounted,
                },
                WalFsync::Always => RecoveryState::Normal,
            });
            // A switch to `always` syncs only a process crash's page cache.
            assert_eq!(
                opening(&crashed, Some("a"), OTHER, WalFsync::Always).journal_sync,
                JournalSync::NotNeeded
            );
            assert_eq!(
                opening(&crashed, Some("a"), DEVICE, fsync).previous_run,
                PreviousRun::ProcessCrash
            );
            assert_eq!(
                opening(&crashed, None, OTHER, fsync).previous_run,
                PreviousRun::HostCrash { fsync }
            );
            for (status, expected) in [
                (RunStatus::Clean, PreviousRun::Clean),
                (RunStatus::Poisoned, PreviousRun::Poisoned),
            ] {
                let ended = previous(Some("a"), fsync, status);
                assert_eq!(
                    opening(&ended, Some("a"), OTHER, fsync).previous_run,
                    expected
                );
            }
        }
    }

    /// One previous run of every variant, with the policy `never`.
    fn every_previous_run() -> [PreviousRun; 7] {
        let runs = [
            PreviousRun::Absent,
            PreviousRun::Unrecorded,
            PreviousRun::Clean,
            PreviousRun::ProcessCrash,
            PreviousRun::Remounted {
                fsync: WalFsync::Never,
            },
            PreviousRun::HostCrash {
                fsync: WalFsync::Never,
            },
            PreviousRun::Poisoned,
        ];
        // A new variant does not compile until it has a row.
        for run in runs {
            match run {
                PreviousRun::Absent
                | PreviousRun::Unrecorded
                | PreviousRun::Clean
                | PreviousRun::ProcessCrash
                | PreviousRun::Remounted { .. }
                | PreviousRun::HostCrash { .. }
                | PreviousRun::Poisoned => {}
            }
        }
        runs
    }

    /// Every row of the predecessor table, first matching row first: an
    /// empty root and an owed wait may live whatever ended, a recorded end,
    /// a poisoned writer and a process crash are gone, and a remount, a
    /// host crash and a lost or absent run state may live. Under either
    /// policy alike.
    #[test]
    fn predecessor_of_follows_its_table_over_every_previous_run() {
        for run in every_previous_run() {
            for (empty, predecessor_wait) in [(true, false), (true, true), (false, true)] {
                assert_eq!(
                    Predecessor::of(run, predecessor_wait, empty),
                    Predecessor::MayLive,
                    "{run:?} empty={empty} wait={predecessor_wait}"
                );
            }
            let expected = match run {
                PreviousRun::Clean | PreviousRun::Poisoned | PreviousRun::ProcessCrash => {
                    Predecessor::Gone
                }
                PreviousRun::Absent
                | PreviousRun::Unrecorded
                | PreviousRun::Remounted { .. }
                | PreviousRun::HostCrash { .. } => Predecessor::MayLive,
            };
            assert_eq!(Predecessor::of(run, false, false), expected, "{run:?}");
        }
        for fsync in [WalFsync::Never, WalFsync::Always] {
            for run in [PreviousRun::Remounted { fsync }, PreviousRun::HostCrash {
                fsync,
            }] {
                assert_eq!(Predecessor::of(run, false, false), Predecessor::MayLive);
            }
        }
        // The admin JSON names.
        assert_eq!(
            serde_json::to_value([Predecessor::MayLive, Predecessor::Gone]).unwrap(),
            serde_json::json!(["may_live", "gone"])
        );
        assert_eq!(
            serde_json::to_value(PreviousRun::Remounted {
                fsync: WalFsync::Never
            })
            .unwrap(),
            serde_json::json!({"kind": "remounted", "fsync": "never"})
        );
    }

    /// Each class of journal opens by its own record through the same
    /// table, over the node-wide status and boot id: a host crash under a
    /// data policy of `never` opens the data journals as a verified prefix
    /// in a new epoch and leaves the meta journal strict, `Normal` and in
    /// its own epoch, while a poisoned or unrecorded run moves both.
    #[test]
    fn each_journal_class_opens_by_its_own_record() {
        use RunStatus::Clean;
        use RunStatus::Poisoned;
        use RunStatus::Running;
        use WalFsync::Always;
        use WalFsync::Never;

        let meta = |previous: Option<&RunState>, journals, boot_id: Option<&str>| {
            WalOpening::decide(
                previous,
                JournalClass::Meta,
                journals,
                boot_id.map(boot).as_ref(),
                DEVICE,
                Always,
            )
            .expect("valid recovery epoch")
        };
        const RECORDS: JournalHistory = JournalHistory::Records { verified_epoch: 0 };

        // A host crash under `never`: the data journals recover, meta does not.
        let crashed = previous(Some("a"), Never, Running);
        let data = decide_data(Some(&crashed), RECORDS, Some(&boot("b")), Never)
            .expect("valid recovery epoch");
        assert_eq!(data.previous_run, PreviousRun::HostCrash { fsync: Never });
        assert_eq!((data.replay_mode, data.recovery), (PREFIX, HOST_CRASH));
        assert_eq!(data.recovery_epoch, 5);
        let opened = meta(Some(&crashed), RECORDS, Some("b"));
        assert_eq!(opened, WalOpening {
            previous_run: PreviousRun::HostCrash { fsync: Always },
            replay_mode: STRICT,
            recovery: NORMAL,
            recovery_epoch: 2,
            journal_sync: JournalSync::NotNeeded,
        });

        // A host crash under `always` moves neither.
        let crashed = previous(Some("a"), Always, Running);
        let data = decide_data(Some(&crashed), RECORDS, Some(&boot("b")), Always)
            .expect("valid recovery epoch");
        assert_eq!((data.replay_mode, data.recovery_epoch), (STRICT, 4));
        assert_eq!(meta(Some(&crashed), RECORDS, Some("b")).recovery_epoch, 2);

        // An I/O failure is node-wide: both classes start a new epoch.
        let poisoned = previous(Some("a"), Never, Poisoned);
        let data = decide_data(Some(&poisoned), RECORDS, Some(&boot("a")), Never)
            .expect("valid recovery epoch");
        assert_eq!((data.replay_mode, data.recovery_epoch), (PREFIX, 5));
        let opened = meta(Some(&poisoned), RECORDS, Some("a"));
        assert_eq!(
            (opened.previous_run, opened.replay_mode),
            (PreviousRun::Poisoned, PREFIX)
        );
        assert_eq!((opened.recovery, opened.recovery_epoch), (POISONED, 3));

        // A lost run state: a class with history recovers past its own
        // surviving epoch, a class without history opens as new.
        let opened = meta(
            None,
            JournalHistory::Records { verified_epoch: 6 },
            Some("b"),
        );
        assert_eq!(
            (opened.previous_run, opened.recovery),
            (PreviousRun::Unrecorded, UNKNOWN_HISTORY)
        );
        assert_eq!(opened.recovery_epoch, 7);
        let opened = meta(None, JournalHistory::Empty { verified_epoch: 0 }, Some("b"));
        assert_eq!(
            (opened.previous_run, opened.recovery_epoch),
            (PreviousRun::Absent, 0)
        );

        // Meta always records `always`, so it never syncs first, while the
        // data journals do when their policy rises after a process crash.
        let stopped = previous(Some("a"), Never, Running);
        let data = decide_data(Some(&stopped), RECORDS, Some(&boot("a")), Always)
            .expect("valid recovery epoch");
        assert_eq!(data.journal_sync, JournalSync::BeforeRecording);
        assert_eq!(
            meta(Some(&stopped), RECORDS, Some("a")).journal_sync,
            JournalSync::NotNeeded
        );
        let clean = previous(Some("a"), Never, Clean);
        assert_eq!(meta(Some(&clean), RECORDS, Some("b")).recovery_epoch, 2);
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
            let opening = decide_data(
                previous.as_ref(),
                JournalHistory::Records { verified_epoch: 0 },
                boot_id.as_ref(),
                fsync,
            )
            .expect("valid recovery epoch");
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

    #[test]
    fn missing_marker_advances_beyond_surviving_verification_even_without_records() {
        for fsync in [WalFsync::Always, WalFsync::Never] {
            for journals in [
                JournalHistory::Records { verified_epoch: 7 },
                JournalHistory::Empty { verified_epoch: 7 },
            ] {
                let opening = decide_data(None, journals, Some(&boot("b")), fsync)
                    .expect("epoch can advance");
                assert_eq!(opening.previous_run, PreviousRun::Unrecorded);
                assert_eq!(opening.recovery, UNKNOWN_HISTORY);
                assert_eq!(opening.replay_mode, PREFIX);
                assert_eq!(opening.recovery_epoch, 8);
                assert_eq!(core_replay_mode(7, opening.recovery_epoch), PREFIX);
            }
            let fresh = decide_data(
                None,
                JournalHistory::Empty { verified_epoch: 0 },
                Some(&boot("b")),
                fsync,
            )
            .expect("fresh disk");
            assert_eq!(fresh.previous_run, PreviousRun::Absent);
            assert_eq!(fresh.replay_mode, STRICT);
            assert_eq!(fresh.recovery_epoch, 0);
        }
    }

    #[test]
    fn epoch_floor_is_the_maximum_of_recorded_and_surviving_history() {
        for (recorded, surviving) in [(4, 7), (9, 2)] {
            for status in [RunStatus::Clean, RunStatus::Poisoned] {
                let mut prior = previous(Some("a"), WalFsync::Never, status);
                prior.data.recovery_epoch = recorded;
                let opening = decide_data(
                    Some(&prior),
                    JournalHistory::Records {
                        verified_epoch: surviving,
                    },
                    Some(&boot("a")),
                    WalFsync::Never,
                )
                .expect("epoch can advance");
                let expected = recorded.max(surviving);
                assert_eq!(
                    opening.recovery_epoch,
                    if status == RunStatus::Clean {
                        expected
                    } else {
                        expected.checked_add(1).unwrap()
                    }
                );
            }
        }
    }

    #[test]
    fn every_prefix_recovery_uses_the_same_checked_epoch_increment() {
        for fsync in [WalFsync::Always, WalFsync::Never] {
            for journals in [
                JournalHistory::Records {
                    verified_epoch: u64::MAX,
                },
                JournalHistory::Empty {
                    verified_epoch: u64::MAX,
                },
            ] {
                assert!(matches!(
                    decide_data(None, journals, Some(&boot("b")), fsync),
                    Err(super::RaftWalError::RecoveryEpochExhausted {
                        class: JournalClass::Data,
                        recovery_epoch: u64::MAX
                    })
                ));
            }
            let mut prior = previous(Some("a"), fsync, RunStatus::Poisoned);
            prior.data.recovery_epoch = u64::MAX;
            let journals = JournalHistory::Records { verified_epoch: 0 };
            assert!(matches!(
                decide_data(Some(&prior), journals, Some(&boot("a")), fsync),
                Err(super::RaftWalError::RecoveryEpochExhausted {
                    class: JournalClass::Data,
                    recovery_epoch: u64::MAX
                })
            ));
            // The meta class's own epoch exhausts on its own decision, which
            // names it.
            let mut meta_prior = previous(Some("a"), fsync, RunStatus::Poisoned);
            meta_prior.meta.recovery_epoch = u64::MAX;
            let meta = WalOpening::decide(
                Some(&meta_prior),
                JournalClass::Meta,
                JournalHistory::Empty { verified_epoch: 0 },
                Some(&boot("a")),
                DEVICE,
                WalFsync::Always,
            );
            assert!(matches!(
                meta,
                Err(super::RaftWalError::RecoveryEpochExhausted {
                    class: JournalClass::Meta,
                    recovery_epoch: u64::MAX
                })
            ));
            prior.status = RunStatus::Clean;
            let clean = decide_data(Some(&prior), journals, Some(&boot("a")), fsync)
                .expect("strict recovery does not advance the epoch");
            assert_eq!(clean.recovery_epoch, u64::MAX);
            assert_eq!(clean.replay_mode, STRICT);
        }
    }

    /// A journal not read back since the recovery epoch began is still read
    /// as a verified prefix, however the runs since then ended.
    #[test]
    fn a_core_journal_is_read_strictly_only_once_verified_in_the_current_epoch() {
        assert_eq!(core_replay_mode(0, 0), STRICT);
        assert_eq!(core_replay_mode(3, 3), STRICT);
        assert_eq!(core_replay_mode(2, 3), PREFIX);
        // The low-level comparison is monotonic; opening a WAL must never
        // supply an epoch lower than surviving metadata.
        assert_eq!(core_replay_mode(5, 0), STRICT);

        // A host crash starts epoch 5; a process crash right after it, before
        // a core was read, keeps epoch 5, so that core still reads its prefix.
        let crashed = previous(Some("a"), WalFsync::Never, RunStatus::Running);
        let after_host_crash = decide_data(
            Some(&crashed),
            JournalHistory::Records { verified_epoch: 0 },
            Some(&boot("b")),
            WalFsync::Always,
        )
        .expect("valid recovery epoch");
        assert_eq!(after_host_crash.recovery_epoch, 5);
        let mut restarted = RunState {
            boot_id: Some(boot("b")),
            ..crashed
        };
        restarted.data.recovery_epoch = after_host_crash.recovery_epoch;
        let after_process_crash = decide_data(
            Some(&restarted),
            JournalHistory::Records { verified_epoch: 0 },
            Some(&boot("b")),
            WalFsync::Always,
        )
        .expect("valid recovery epoch");
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

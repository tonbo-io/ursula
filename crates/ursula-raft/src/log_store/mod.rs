//! OpenRaft log stores.
//!
//! - `wal`: node lifecycle and per-core journal ownership.
//! - `file`: the durable per-group store over the shared per-core journal.
//! - `writer`: the single writer of each core's journal: batches, the fsync
//!   policy, rotation and reclaim.
//! - `group_log`: one group's log in memory: its markers, an index of where
//!   each live entry's frame is, and a bounded cache of recent entries.
//! - `segment`: the segment files of a core journal: naming, recovery across
//!   segments, rotation and deletion.
//! - `reclaim`: which segments a reclaim pass deletes or rewrites, and which
//!   groups it reports lagging.
//! - `journal`: the framed, checksummed format of one segment.
//! - `frozen`: immutable payload archives and validated journal references for stopped groups.
//! - `core_meta`: each core's metadata file: every group's vote and log
//!   state (empty, initialized or recovering).
//! - `topology`: immutable core and group counts of the WAL root.
//! - `run_state`: the node's run-state file and how the journals open after
//!   the previous run (the replay-mode decision and the recovery state).
//! - `state_file`: the atomically replaced, checksummed state-file format.
//! - `disk`: the I/O seam every journal file operation goes through.
//! - `sim_disk`: the simulated disk behind the seam under `cfg(madsim)`.
//! - `meta_test_store`: an in-memory store for the meta Raft's unit tests
//!   only (`cfg(test)`).

mod core_meta;
pub(crate) mod disk;
mod file;
mod frozen;
#[cfg(test)]
mod frozen_tests;
mod group_log;
mod journal;
#[cfg(all(test, not(madsim)))]
mod journal_tests;
#[cfg(test)]
mod meta_test_store;
mod reclaim;
mod run_state;
mod segment;
#[cfg(madsim)]
mod sim_disk;
mod state_file;
mod topology;
mod wal;
mod writer;
use std::io;

pub use core_meta::GroupLogState;
pub use core_meta::MarkRecoveringError;
#[cfg(madsim)]
pub use disk::JournalDisk;
#[cfg(madsim)]
pub use disk::JournalFile;
#[cfg(madsim)]
pub use disk::LockAttempt;
pub use file::RaftGroupFileLogStore;
pub use frozen::ArchiveDefect;
pub use journal::FrameDefect;
pub use journal::HeaderDefect;
pub use journal::JournalError;
pub use journal::JournalOp;
pub use journal::JournalReplayMode;
pub use journal::RecordTooLarge;
#[cfg(test)]
pub(crate) use meta_test_store::MetaTestLogStore;
use openraft::RaftTypeConfig;
use openraft::alias::EntryOf;
use openraft::alias::LogIdOf;
use openraft::entry::RaftEntry;
pub use run_state::BootId;
pub use run_state::JournalHistory;
pub use run_state::JournalSync;
pub(crate) use run_state::NodeWal;
pub use run_state::PreviousRun;
pub use run_state::RUN_STATE_FILE;
pub use run_state::RaftWalError;
pub use run_state::RecoveryReason;
pub use run_state::RecoveryState;
pub use run_state::RunState;
pub use run_state::RunStatus;
pub use run_state::WalOpening;
pub(crate) use run_state::core_dir;
pub use segment::journal_segment_path;
pub use segment::journal_segments;
use serde::Deserialize;
use serde::Serialize;
#[cfg(madsim)]
pub use sim_disk::SIM_DISK_PAGE_SIZE;
#[cfg(madsim)]
pub use sim_disk::SimDisk;
#[cfg(madsim)]
pub use sim_disk::SimDiskError;
#[cfg(madsim)]
pub use sim_disk::SimDiskFault;
#[cfg(madsim)]
pub use sim_disk::SimFile;
#[cfg(madsim)]
pub use sim_disk::SimJournalLock;
#[cfg(madsim)]
pub use sim_disk::SimPowerLoss;
pub use state_file::StateFileDefect;
pub use state_file::StateFileError;
pub use state_file::StateFileKind;
pub use wal::RaftWal;
pub(crate) use writer::CoreFileLogWriter;
pub use writer::CoreJournalError;
pub(crate) use writer::CoreJournalOptions;
pub use writer::JournalTuning;
pub use writer::LaggingGroups;
pub(crate) use writer::WriterExited;
pub(crate) use writer::elapsed_ns;
#[cfg(all(test, not(madsim)))]
pub(crate) use writer::read_wire_frames;

use crate::types::UrsulaRaftTypeConfig;

/// One durable operation appended to a group's raft log journal.
///
/// Journaled as self-describing MessagePack of openraft's own serde-capable
/// types (via [`crate::codec::encode_wire`]) instead of hand-written proto
/// mirrors; the on-disk format is therefore coupled to the Rust type layout,
/// which is acceptable while every deployment upgrades atomically. Votes are
/// not journaled: each core's metadata file holds them.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum RaftGroupLogRecord {
    SaveCommitted(Option<LogIdOf<UrsulaRaftTypeConfig>>),
    Append(Vec<EntryOf<UrsulaRaftTypeConfig>>),
    FrozenAppend(Box<frozen::FrozenAppend>),
    TruncateAfter(Option<LogIdOf<UrsulaRaftTypeConfig>>),
    Purge(LogIdOf<UrsulaRaftTypeConfig>),
}

/// A [`RaftGroupLogRecord`] tagged with its raft group in the shared per-core
/// journal.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct CoreJournalRecord {
    pub(crate) group_id: u32,
    pub(crate) record: RaftGroupLogRecord,
}

pub(crate) fn ensure_consecutive_entries<C>(entries: &[EntryOf<C>]) -> Result<(), io::Error>
where
    C: RaftTypeConfig,
    C::Entry: Clone,
{
    for pair in entries.windows(2) {
        let [current, next] = pair else {
            continue;
        };
        let (current, next) = (current.log_id().index, next.log_id().index);
        if current.checked_add(1) != Some(next) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("raft log entries are not consecutive: {current} then {next}"),
            ));
        }
    }
    Ok(())
}

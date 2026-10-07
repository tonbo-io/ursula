//! OpenRaft log stores.
//!
//! - `file`: the durable per-group store over the shared per-core journal,
//!   and the core writer that applies the fsync policy.
//! - `journal`: the framed, checksummed journal file format.
//! - `core_meta`: each core's metadata file: every group's vote and log
//!   state (empty, initialized or recovering).
//! - `run_state`: the node's run-state file and how the journals open after
//!   the previous run (the replay-mode decision and the recovery state).
//! - `state_file`: the atomically replaced, checksummed format of both.
//! - `disk`: the I/O seam every journal file operation goes through.
//! - `sim_disk`: the simulated disk behind the seam under `cfg(madsim)`.
//! - `meta_test_store`: an in-memory store for the meta Raft's unit tests
//!   only (`cfg(test)`).

mod core_meta;
mod disk;
mod file;
mod journal;
#[cfg(test)]
mod meta_test_store;
mod run_state;
#[cfg(madsim)]
mod sim_disk;
mod state_file;

use std::collections::BTreeMap;
use std::io;

pub use core_meta::GroupLogState;
pub use core_meta::MarkRecoveringError;
#[cfg(madsim)]
pub use disk::JournalDisk;
#[cfg(madsim)]
pub use disk::JournalFile;
#[cfg(madsim)]
pub use disk::LockAttempt;
pub(crate) use file::CoreFileLogWriter;
pub use file::CoreJournalError;
pub(crate) use file::CoreJournalOptions;
pub use file::RaftGroupFileLogStore;
pub(crate) use file::elapsed_ns;
#[cfg(test)]
pub(crate) use file::read_wire_frames;
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
use openraft::alias::VoteOf;
use openraft::entry::RaftEntry;
pub use run_state::BootId;
pub(crate) use run_state::CORE_JOURNAL_FILE;
pub use run_state::JournalHistory;
pub(crate) use run_state::NodeWal;
pub use run_state::PreviousRun;
pub use run_state::RUN_STATE_FILE;
pub use run_state::RaftWalError;
pub use run_state::RecoveryReason;
pub use run_state::RecoveryState;
pub use run_state::RunState;
pub use run_state::RunStatus;
pub use run_state::WalOpening;
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

use crate::types::UrsulaRaftTypeConfig;

/// A group's log as a log store holds it in memory: its entries, vote,
/// committed pointer and purge point.
#[derive(Debug, Clone, Default)]
pub(crate) struct LogStoreInner<C>
where
    C: RaftTypeConfig,
    C::Entry: Clone,
{
    last_purged_log_id: Option<LogIdOf<C>>,
    committed: Option<LogIdOf<C>>,
    entries: BTreeMap<u64, EntryOf<C>>,
    vote: Option<VoteOf<C>>,
}

pub(crate) type RaftGroupLogStoreInner = LogStoreInner<UrsulaRaftTypeConfig>;

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

/// Drops every entry after `last_index`, or all entries when it is `None`.
pub(crate) fn truncate_entries_after<V>(entries: &mut BTreeMap<u64, V>, last_index: Option<u64>) {
    match last_index {
        Some(last_index) => entries.retain(|index, _| *index <= last_index),
        None => entries.clear(),
    }
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

pub(crate) fn ensure_log_append_boundary<C>(
    inner: &LogStoreInner<C>,
    entries: &[EntryOf<C>],
) -> Result<(), io::Error>
where
    C: RaftTypeConfig,
    C::Entry: Clone,
{
    let Some(first_entry) = entries.first() else {
        return Ok(());
    };
    let Some(last_existing_index) = inner.entries.keys().next_back().copied() else {
        return Ok(());
    };

    let first_append_index = first_entry.log_id().index;
    if last_existing_index
        .checked_add(1)
        .is_some_and(|next_index| first_append_index > next_index)
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("raft log store has a hole: {last_existing_index} then {first_append_index}"),
        ));
    }

    Ok(())
}

pub(crate) fn ensure_consecutive_log<C>(
    entries: &BTreeMap<u64, EntryOf<C>>,
) -> Result<(), io::Error>
where
    C: RaftTypeConfig,
    C::Entry: Clone,
{
    let mut previous: Option<u64> = None;
    for index in entries.keys().copied() {
        if let Some(previous) = previous
            && previous.checked_add(1) != Some(index)
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("raft log store has a hole: {previous} then {index}"),
            ));
        }
        previous = Some(index);
    }
    Ok(())
}

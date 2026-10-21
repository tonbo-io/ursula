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
//! - `reclaim`: which segments a reclaim pass deletes or rewrites, for the
//!   journal's size or for how long they held dead frames, and which groups
//!   it reports lagging.
//! - `journal`: the framed, checksummed format of one segment.
//! - `frozen`: immutable payload archives and validated journal references for stopped groups.
//! - `core_meta`: each core's metadata file: every group's vote and log
//!   state (empty, initialized or recovering).
//! - `topology`: immutable core and group counts and cluster id of the WAL
//!   root.
//! - `run_state`: the node's run-state file and how the journals open after
//!   the previous run (the replay-mode decision and the recovery state).
//! - `state_file`: the atomically replaced, checksummed state-file format.
//! - `disk`: the I/O seam every journal file operation goes through.
//! - `sim_disk`: the simulated disk behind the seam under `cfg(madsim)`.

mod core_meta;
#[cfg(test)]
mod dirty_tests;
pub(crate) mod disk;
mod file;
mod frozen;
#[cfg(test)]
mod frozen_tests;
mod group_log;
mod journal;
#[cfg(all(test, not(madsim)))]
mod journal_tests;
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

#[cfg(all(test, not(madsim)))]
pub(crate) use core_meta::CoreMetadata;
pub use core_meta::GroupLogState;
pub use core_meta::LogGeneration;
pub use core_meta::MarkRecoveringError;
pub use core_meta::PlanCut;
pub use core_meta::RecoveryRecord;
#[cfg(all(test, not(madsim)))]
pub(crate) use core_meta::core_metadata_path;
#[cfg(madsim)]
pub use disk::JournalDisk;
#[cfg(madsim)]
pub use disk::JournalFile;
#[cfg(madsim)]
pub use disk::LockAttempt;
pub use file::RaftGroupFileLogStore;
pub use file::RecoveryCutError;
pub use file::SnapshotOrderError;
pub use file::SnapshotRecord;
pub use frozen::ArchiveDefect;
#[cfg(test)]
pub(crate) use frozen::ArchiveId;
#[cfg(test)]
pub(crate) use frozen::ArchiveRef;
#[cfg(test)]
pub(crate) use frozen::FrozenAppend;
pub use journal::FrameDefect;
pub use journal::HeaderDefect;
pub use journal::JournalError;
pub use journal::JournalOp;
pub use journal::JournalReplayMode;
pub use journal::RecordTooLarge;
use openraft::RaftTypeConfig;
use openraft::alias::EntryOf;
use openraft::alias::LogIdOf;
use openraft::entry::RaftEntry;
pub use run_state::BootId;
pub use run_state::JournalClass;
pub use run_state::JournalHistory;
pub use run_state::JournalRun;
pub use run_state::JournalSync;
pub(crate) use run_state::META_FSYNC;
pub use run_state::MetaJournalDamage;
pub use run_state::MetaOpening;
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
pub(crate) use state_file::replace_file;
pub use topology::generate_cluster_id;
pub use wal::MetaOpenError;
pub use wal::RaftWal;
pub use wal::WalMaintenance;
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
/// Journaled as compact MessagePack of openraft's own serde-capable types
/// ([`ursula_stream::msgpack`], as [`crate::codec::encode_wire`] uses it)
/// instead of hand-written proto mirrors. A struct is an array of its fields
/// in declaration order, so the on-disk format is the Rust type layout,
/// OpenRaft's included: a reordered, added or removed field is a format
/// change, which the persisted-envelopes fixture shows. Votes are not
/// journaled: each core's metadata file holds them.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum RaftGroupLogRecord {
    SaveCommitted(Option<LogIdOf<UrsulaRaftTypeConfig>>),
    Append(Vec<EntryOf<UrsulaRaftTypeConfig>>),
    FrozenAppend(Box<frozen::FrozenAppend>),
    TruncateAfter(Option<LogIdOf<UrsulaRaftTypeConfig>>),
    Purge(LogIdOf<UrsulaRaftTypeConfig>),
}

/// One sample of each WAL state file, by name, as the persisted-envelopes
/// fixture holds it: the whole file, its header and checksum included.
#[cfg(test)]
pub(crate) fn state_file_samples() -> [(&'static str, Vec<u8>); 3] {
    [
        ("core_metadata_file", core_meta::sample_file()),
        ("run_state_file", run_state::sample_file()),
        ("topology_file", topology::sample_file()),
    ]
}

/// A [`RaftGroupLogRecord`] tagged with its raft group in the shared per-core
/// journal, and with the generation of the group's log it belongs to: replay
/// keeps only the records of the generation the core's metadata names
/// ([`LogGeneration`]).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct CoreJournalRecord {
    pub(crate) group_id: u32,
    pub(crate) generation: LogGeneration,
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

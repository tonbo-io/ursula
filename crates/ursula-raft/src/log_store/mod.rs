mod file;
mod memory;

use std::collections::BTreeMap;
use std::io;

pub(crate) use file::CoreFileLogWriter;
pub use file::RaftGroupFileLogStore;
pub(crate) use file::elapsed_ns;
#[cfg(test)]
pub(crate) use file::read_wire_frames;
pub use memory::MemoryRaftLogStore;
pub use memory::MetaRaftLogStore;
pub use memory::RaftGroupLogStore;
use openraft::RaftTypeConfig;
use openraft::alias::EntryOf;
use openraft::alias::LogIdOf;
use openraft::alias::VoteOf;
use openraft::entry::RaftEntry;
use serde::Deserialize;
use serde::Serialize;

use crate::types::UrsulaRaftTypeConfig;

#[derive(Debug, Clone, Default)]
pub(crate) struct MemoryRaftLogStoreInner<C>
where
    C: RaftTypeConfig,
    C::Entry: Clone,
{
    last_purged_log_id: Option<LogIdOf<C>>,
    committed: Option<LogIdOf<C>>,
    entries: BTreeMap<u64, EntryOf<C>>,
    vote: Option<VoteOf<C>>,
}

pub(crate) type RaftGroupLogStoreInner = MemoryRaftLogStoreInner<UrsulaRaftTypeConfig>;

/// One durable operation appended to a group's raft log journal.
///
/// Journaled as self-describing MessagePack of openraft's own serde-capable
/// types (via [`crate::codec::encode_wire`]) instead of hand-written proto
/// mirrors; the on-disk format is therefore coupled to the Rust type layout,
/// which is acceptable while every deployment upgrades atomically.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum RaftGroupLogRecord {
    SaveVote(VoteOf<UrsulaRaftTypeConfig>),
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

pub(crate) fn ensure_consecutive_entries<C>(entries: &[EntryOf<C>]) -> Result<(), io::Error>
where
    C: RaftTypeConfig,
    C::Entry: Clone,
{
    for pair in entries.windows(2) {
        let current = pair[0].log_id().index;
        let next = pair[1].log_id().index;
        if next != current + 1 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("raft log entries are not consecutive: {current} then {next}"),
            ));
        }
    }
    Ok(())
}

pub(crate) fn ensure_log_append_boundary<C>(
    inner: &MemoryRaftLogStoreInner<C>,
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
    if first_append_index > last_existing_index + 1 {
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
    let mut previous = None;
    for index in entries.keys().copied() {
        if let Some(previous) = previous
            && index != previous + 1
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

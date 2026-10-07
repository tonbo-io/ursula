//! A test-only in-memory log store for the meta Raft's unit tests.
//!
//! The meta Raft (`crate::meta`) has no production deployment yet, and its
//! tests run it on this store. Data groups always run on the per-core
//! journal; nothing outside `#[cfg(test)]` can reach this module.

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::io;
use std::ops::RangeBounds;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::MutexGuard;

use openraft::OptionalSend;
use openraft::RaftTypeConfig;
use openraft::alias::EntryOf;
use openraft::alias::LogIdOf;
use openraft::alias::VoteOf;
use openraft::entry::RaftEntry;
use openraft::storage::IOFlushed;
use openraft::storage::LogState;
use openraft::storage::RaftLogReader;
use openraft::storage::RaftLogStorage;

use super::ensure_consecutive_entries;
use crate::meta::MetaRaftTypeConfig;

/// The meta log as this store holds it: its entries, vote, committed
/// pointer and purge point.
#[derive(Debug, Clone, Default)]
struct LogStoreInner<C>
where
    C: RaftTypeConfig,
    C::Entry: Clone,
{
    last_purged_log_id: Option<LogIdOf<C>>,
    committed: Option<LogIdOf<C>>,
    entries: BTreeMap<u64, EntryOf<C>>,
    vote: Option<VoteOf<C>>,
}

/// Drops every entry after `last_index`, or all entries when it is `None`.
fn truncate_entries_after<V>(entries: &mut BTreeMap<u64, V>, last_index: Option<u64>) {
    match last_index {
        Some(last_index) => entries.retain(|index, _| *index <= last_index),
        None => entries.clear(),
    }
}

fn ensure_log_append_boundary<C>(
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

/// The meta Raft's test log store: everything in memory, nothing survives
/// the process.
#[derive(Debug, Default)]
pub(crate) struct MetaTestLogStore {
    inner: Mutex<LogStoreInner<MetaRaftTypeConfig>>,
}

impl MetaTestLogStore {
    pub(crate) fn shared() -> Arc<Self> {
        Arc::new(Self::default())
    }

    fn lock_inner(&self) -> Result<MutexGuard<'_, LogStoreInner<MetaRaftTypeConfig>>, io::Error> {
        self.inner
            .lock()
            .map_err(|_poisoned| io::Error::other("meta test log store mutex poisoned"))
    }
}

impl RaftLogReader<MetaRaftTypeConfig> for Arc<MetaTestLogStore> {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<EntryOf<MetaRaftTypeConfig>>, io::Error> {
        let inner = self.lock_inner()?;
        let entries = inner
            .entries
            .range(range)
            .map(|(_, entry)| entry.clone())
            .collect::<Vec<_>>();

        ensure_consecutive_entries::<MetaRaftTypeConfig>(&entries)?;
        Ok(entries)
    }

    async fn read_vote(&mut self) -> Result<Option<VoteOf<MetaRaftTypeConfig>>, io::Error> {
        Ok(self.lock_inner()?.vote)
    }
}

impl RaftLogStorage<MetaRaftTypeConfig> for Arc<MetaTestLogStore> {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<MetaRaftTypeConfig>, io::Error> {
        let inner = self.lock_inner()?;
        let last_log_id = inner
            .entries
            .last_key_value()
            .map(|(_, entry)| entry.log_id())
            .or(inner.last_purged_log_id);

        Ok(LogState {
            last_purged_log_id: inner.last_purged_log_id,
            last_log_id,
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &VoteOf<MetaRaftTypeConfig>) -> Result<(), io::Error> {
        let mut inner = self.lock_inner()?;
        if inner.vote.as_ref() != Some(vote) {
            inner.vote = Some(*vote);
        }
        Ok(())
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogIdOf<MetaRaftTypeConfig>>,
    ) -> Result<(), io::Error> {
        let mut inner = self.lock_inner()?;
        if inner.committed != committed {
            inner.committed = committed;
        }
        Ok(())
    }

    async fn read_committed(&mut self) -> Result<Option<LogIdOf<MetaRaftTypeConfig>>, io::Error> {
        Ok(self.lock_inner()?.committed)
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: IOFlushed<MetaRaftTypeConfig>,
    ) -> Result<(), io::Error>
    where
        I: IntoIterator<Item = EntryOf<MetaRaftTypeConfig>> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let entries = entries.into_iter().collect::<Vec<_>>();
        ensure_consecutive_entries::<MetaRaftTypeConfig>(&entries)?;

        let mut inner = self.lock_inner()?;
        ensure_log_append_boundary::<MetaRaftTypeConfig>(&inner, &entries)?;
        for entry in entries {
            inner.entries.insert(entry.index(), entry);
        }

        callback.io_completed(Ok(()));
        Ok(())
    }

    async fn truncate_after(
        &mut self,
        last_log_id: Option<LogIdOf<MetaRaftTypeConfig>>,
    ) -> Result<(), io::Error> {
        let mut inner = self.lock_inner()?;
        truncate_entries_after(&mut inner.entries, last_log_id.map(|log_id| log_id.index));
        Ok(())
    }

    async fn purge(&mut self, log_id: LogIdOf<MetaRaftTypeConfig>) -> Result<(), io::Error> {
        let mut inner = self.lock_inner()?;
        if inner.last_purged_log_id > Some(log_id) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "cannot move last purged log id backward from {:?} to {:?}",
                    inner.last_purged_log_id, log_id
                ),
            ));
        }

        inner.last_purged_log_id = Some(log_id);
        inner.entries.retain(|index, _| *index > log_id.index);
        Ok(())
    }
}

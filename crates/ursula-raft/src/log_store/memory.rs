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

use super::MemoryRaftLogStoreInner;
use super::ensure_consecutive_entries;
use super::ensure_log_append_boundary;
use crate::meta::MetaRaftTypeConfig;
use crate::types::UrsulaRaftTypeConfig;

#[derive(Debug)]
pub struct MemoryRaftLogStore<C>
where
    C: RaftTypeConfig,
    C::Entry: Clone,
{
    inner: Mutex<MemoryRaftLogStoreInner<C>>,
}

pub type RaftGroupLogStore = MemoryRaftLogStore<UrsulaRaftTypeConfig>;
pub type MetaRaftLogStore = MemoryRaftLogStore<MetaRaftTypeConfig>;

impl<C> Default for MemoryRaftLogStore<C>
where
    C: RaftTypeConfig,
    C::Entry: Clone,
{
    fn default() -> Self {
        Self {
            inner: Mutex::new(MemoryRaftLogStoreInner::default()),
        }
    }
}

impl<C> MemoryRaftLogStore<C>
where
    C: RaftTypeConfig,
    C::Entry: Clone,
{
    pub fn new() -> Self {
        Self::default()
    }

    pub fn shared() -> Arc<Self> {
        Arc::new(Self::new())
    }

    pub(crate) fn lock_inner(
        &self,
    ) -> Result<MutexGuard<'_, MemoryRaftLogStoreInner<C>>, io::Error> {
        self.inner
            .lock()
            .map_err(|_| io::Error::other("raft memory log store mutex poisoned"))
    }
}

macro_rules! impl_memory_log_store {
    ($config:ty) => {
        impl RaftLogReader<$config> for Arc<MemoryRaftLogStore<$config>> {
            async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
                &mut self,
                range: RB,
            ) -> Result<Vec<EntryOf<$config>>, io::Error> {
                let inner = self.lock_inner()?;
                let entries = inner
                    .entries
                    .range(range)
                    .map(|(_, entry)| entry.clone())
                    .collect::<Vec<_>>();

                ensure_consecutive_entries::<$config>(&entries)?;
                Ok(entries)
            }

            async fn read_vote(&mut self) -> Result<Option<VoteOf<$config>>, io::Error> {
                Ok(self.lock_inner()?.vote.clone())
            }
        }

        impl RaftLogStorage<$config> for Arc<MemoryRaftLogStore<$config>> {
            type LogReader = Self;

            async fn get_log_state(&mut self) -> Result<LogState<$config>, io::Error> {
                let inner = self.lock_inner()?;
                let last_log_id = inner
                    .entries
                    .last_key_value()
                    .map(|(_, entry)| entry.log_id())
                    .or_else(|| inner.last_purged_log_id.clone());

                Ok(LogState {
                    last_purged_log_id: inner.last_purged_log_id.clone(),
                    last_log_id,
                })
            }

            async fn get_log_reader(&mut self) -> Self::LogReader {
                self.clone()
            }

            async fn save_vote(&mut self, vote: &VoteOf<$config>) -> Result<(), io::Error> {
                let mut inner = self.lock_inner()?;
                if inner.vote.as_ref() != Some(vote) {
                    inner.vote = Some(vote.clone());
                }
                Ok(())
            }

            async fn save_committed(
                &mut self,
                committed: Option<LogIdOf<$config>>,
            ) -> Result<(), io::Error> {
                let mut inner = self.lock_inner()?;
                if inner.committed != committed {
                    inner.committed = committed;
                }
                Ok(())
            }

            async fn read_committed(&mut self) -> Result<Option<LogIdOf<$config>>, io::Error> {
                Ok(self.lock_inner()?.committed.clone())
            }

            async fn append<I>(
                &mut self,
                entries: I,
                callback: IOFlushed<$config>,
            ) -> Result<(), io::Error>
            where
                I: IntoIterator<Item = EntryOf<$config>> + OptionalSend,
                I::IntoIter: OptionalSend,
            {
                let entries = entries.into_iter().collect::<Vec<_>>();
                ensure_consecutive_entries::<$config>(&entries)?;

                let mut inner = self.lock_inner()?;
                ensure_log_append_boundary::<$config>(&inner, &entries)?;
                for entry in entries {
                    inner.entries.insert(entry.index(), entry);
                }

                callback.io_completed(Ok(()));
                Ok(())
            }

            async fn truncate_after(
                &mut self,
                last_log_id: Option<LogIdOf<$config>>,
            ) -> Result<(), io::Error> {
                let start_index = last_log_id.map_or(0, |log_id| log_id.index + 1);
                let mut inner = self.lock_inner()?;
                inner.entries.retain(|index, _| *index < start_index);
                Ok(())
            }

            async fn purge(&mut self, log_id: LogIdOf<$config>) -> Result<(), io::Error> {
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
    };
}

impl_memory_log_store!(UrsulaRaftTypeConfig);
impl_memory_log_store!(MetaRaftTypeConfig);

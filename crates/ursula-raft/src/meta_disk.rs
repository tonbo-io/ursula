//! Dedicated fsync-always meta log and snapshot files, independent of data topology.

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::fs::File;
use std::io;
use std::io::Write;
use std::ops::RangeBounds;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;

use fs4::fs_std::FileExt;
use openraft::OptionalSend;
use openraft::alias::EntryOf;
use openraft::alias::LogIdOf;
use openraft::alias::VoteOf;
use openraft::entry::RaftEntry;
use openraft::storage::IOFlushed;
use openraft::storage::LogState;
use openraft::storage::RaftLogReader;
use openraft::storage::RaftLogStorage;
use serde::Deserialize;
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::MetaRaftTypeConfig;

#[derive(Serialize, Deserialize)]
struct DurableRecord {
    version: u32,
    checksum: u32,
    payload: Vec<u8>,
}

pub(crate) fn encode_record<T: Serialize>(value: &T) -> Result<Vec<u8>, io::Error> {
    let payload = rmp_serde::to_vec_named(value).map_err(io::Error::other)?;
    rmp_serde::to_vec_named(&DurableRecord {
        version: 1,
        checksum: crc32fast::hash(&payload),
        payload,
    })
    .map_err(io::Error::other)
}
pub(crate) fn decode_record<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, io::Error> {
    let record: DurableRecord = rmp_serde::from_slice(bytes)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    if record.version != 1 || record.checksum != crc32fast::hash(&record.payload) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "meta durable record version or checksum mismatch",
        ));
    }
    rmp_serde::from_slice(&record.payload)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct DiskState {
    vote: Option<VoteOf<MetaRaftTypeConfig>>,
    committed: Option<LogIdOf<MetaRaftTypeConfig>>,
    purged: Option<LogIdOf<MetaRaftTypeConfig>>,
    entries: BTreeMap<u64, EntryOf<MetaRaftTypeConfig>>,
}

#[derive(Debug)]
pub struct MetaDiskLogStore {
    state: Mutex<DiskState>,
    serial: Arc<crate::rt::sync::Mutex<()>>,
    path: PathBuf,
    _lock: File,
}

impl MetaDiskLogStore {
    pub async fn open(root: PathBuf) -> Result<Arc<Self>, io::Error> {
        run_io(move || {
            create_durable_directory(&root)?;
            let lock = File::options()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(root.join("owner.lock"))?;
            lock.try_lock_exclusive()?;
            let path = root.join("meta-log.msgpack");
            let initialized = root.join("initialized");
            let state = match std::fs::read(&path) {
                Ok(bytes) => decode_record(&bytes)?,
                Err(error) if error.kind() == io::ErrorKind::NotFound && !initialized.exists() => {
                    let state = DiskState::default();
                    atomic_write(&path, &encode_record(&state)?)?;
                    state
                }
                Err(error) => return Err(error),
            };
            if !initialized.exists() {
                atomic_write(&initialized, b"ursula-meta-v1\n")?;
            }
            Ok(Arc::new(Self {
                state: Mutex::new(state),
                serial: Arc::default(),
                path,
                _lock: lock,
            }))
        })
        .await
    }

    async fn mutate(
        self: &Arc<Self>,
        change: impl FnOnce(&mut DiskState) -> Result<(), io::Error> + Send + 'static,
    ) -> Result<(), io::Error> {
        let serial = self.serial.clone().lock_owned().await;
        let store = self.clone();
        // The blocking operation owns both serialization and the directory lock.
        // Dropping its waiter cannot let a newer write overtake its rename or
        // release ownership before durable state and in-memory state agree.
        run_io(move || {
            let _serial = serial;
            let mut next = store
                .state
                .lock()
                .map_err(|_poisoned| io::Error::other("meta log lock poisoned"))?
                .clone();
            change(&mut next)?;
            atomic_write(&store.path, &encode_record(&next)?)?;
            *store
                .state
                .lock()
                .map_err(|_poisoned| io::Error::other("meta log lock poisoned"))? = next;
            Ok(())
        })
        .await
    }
}

pub(crate) async fn run_io<T: Send + 'static>(
    work: impl FnOnce() -> Result<T, io::Error> + Send + 'static,
) -> Result<T, io::Error> {
    #[cfg(not(madsim))]
    {
        tokio::task::spawn_blocking(work)
            .await
            .map_err(io::Error::other)?
    }
    #[cfg(madsim)]
    {
        work()
    }
}

fn create_durable_directory(path: &Path) -> Result<(), io::Error> {
    if path.is_dir() {
        return Ok(());
    }
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    create_durable_directory(parent)?;
    match std::fs::create_dir(path) {
        Ok(()) => (),
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists && path.is_dir() => (),
        Err(error) => return Err(error),
    }
    // Sync each newly created directory's parent before publishing any meta
    // log in it; syncing only the leaf misses its ancestors' directory entries.
    File::open(parent)?.sync_all()
}

pub(crate) fn atomic_write(path: &Path, bytes: &[u8]) -> Result<(), io::Error> {
    let temporary = path.with_extension("pending");
    let mut file = File::create(&temporary)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    std::fs::rename(temporary, path)?;
    if let Some(parent) = path.parent() {
        File::open(parent)?.sync_all()?;
    }
    Ok(())
}

impl RaftLogReader<MetaRaftTypeConfig> for Arc<MetaDiskLogStore> {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<EntryOf<MetaRaftTypeConfig>>, io::Error> {
        let entries: Vec<_> = self
            .state
            .lock()
            .map_err(|_poisoned| io::Error::other("meta log lock poisoned"))?
            .entries
            .range(range)
            .map(|(_, entry)| entry.clone())
            .collect();
        crate::log_store::ensure_consecutive_entries::<MetaRaftTypeConfig>(&entries)?;
        Ok(entries)
    }
    async fn read_vote(&mut self) -> Result<Option<VoteOf<MetaRaftTypeConfig>>, io::Error> {
        Ok(self
            .state
            .lock()
            .map_err(|_poisoned| io::Error::other("meta log lock poisoned"))?
            .vote)
    }
}
impl RaftLogStorage<MetaRaftTypeConfig> for Arc<MetaDiskLogStore> {
    type LogReader = Self;
    async fn get_log_state(&mut self) -> Result<LogState<MetaRaftTypeConfig>, io::Error> {
        let state = self
            .state
            .lock()
            .map_err(|_poisoned| io::Error::other("meta log lock poisoned"))?;
        Ok(LogState {
            last_purged_log_id: state.purged,
            last_log_id: state
                .entries
                .last_key_value()
                .map(|(_, entry)| entry.log_id())
                .or(state.purged),
        })
    }
    async fn get_log_reader(&mut self) -> Self {
        self.clone()
    }
    async fn save_vote(&mut self, vote: &VoteOf<MetaRaftTypeConfig>) -> Result<(), io::Error> {
        let vote = *vote;
        self.mutate(move |state| {
            state.vote = Some(vote);
            Ok(())
        })
        .await
    }
    async fn save_committed(
        &mut self,
        committed: Option<LogIdOf<MetaRaftTypeConfig>>,
    ) -> Result<(), io::Error> {
        self.mutate(move |state| {
            state.committed = committed;
            Ok(())
        })
        .await
    }
    async fn read_committed(&mut self) -> Result<Option<LogIdOf<MetaRaftTypeConfig>>, io::Error> {
        Ok(self
            .state
            .lock()
            .map_err(|_poisoned| io::Error::other("meta log lock poisoned"))?
            .committed)
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
        let entries: Vec<_> = entries.into_iter().collect();
        crate::log_store::ensure_consecutive_entries::<MetaRaftTypeConfig>(&entries)?;
        self.mutate(move |state| {
            if let Some(first) = entries.first() {
                let previous = state
                    .entries
                    .last_key_value()
                    .map(|(index, _)| *index)
                    .or(state.purged.map(|id| id.index()));
                if previous
                    .and_then(|index| index.checked_add(1))
                    .is_some_and(|next| first.index() > next)
                {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "meta log append has a hole",
                    ));
                }
            }
            for entry in entries {
                state.entries.insert(entry.index(), entry);
            }
            Ok(())
        })
        .await?;
        callback.io_completed(Ok(()));
        Ok(())
    }
    async fn truncate_after(
        &mut self,
        last: Option<LogIdOf<MetaRaftTypeConfig>>,
    ) -> Result<(), io::Error> {
        self.mutate(move |state| {
            state
                .entries
                .retain(|index, _| last.is_some_and(|last| *index <= last.index()));
            Ok(())
        })
        .await
    }
    async fn purge(&mut self, log_id: LogIdOf<MetaRaftTypeConfig>) -> Result<(), io::Error> {
        self.mutate(move |state| {
            if state.purged > Some(log_id) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "meta purge cannot regress",
                ));
            }
            state.entries.retain(|index, _| *index > log_id.index());
            state.purged = Some(log_id);
            Ok(())
        })
        .await
    }
}

#[cfg(all(test, not(madsim)))]
mod tests {
    use super::*;

    #[tokio::test]
    async fn canceled_meta_write_keeps_serialization_through_durable_publication() {
        let root = tempfile::tempdir().unwrap();
        let store = MetaDiskLogStore::open(root.path().to_path_buf())
            .await
            .unwrap();
        let (entered, started) = tokio::sync::oneshot::channel();
        let (release, blocked) = std::sync::mpsc::channel();
        let first = tokio::spawn({
            let store = store.clone();
            async move {
                store
                    .mutate(move |state| {
                        entered.send(()).unwrap();
                        blocked.recv().unwrap();
                        state.vote = Some(openraft::Vote::new(1, 1));
                        Ok(())
                    })
                    .await
            }
        });
        started.await.unwrap();
        first.abort();
        assert!(first.await.unwrap_err().is_cancelled());
        assert!(
            store.serial.try_lock().is_err(),
            "canceled waiter must not release writer serialization"
        );
        let second = tokio::spawn({
            let store = store.clone();
            async move {
                store
                    .mutate(|state| {
                        assert_eq!(state.vote, Some(openraft::Vote::new(1, 1)));
                        state.vote = Some(openraft::Vote::new(2, 1));
                        Ok(())
                    })
                    .await
            }
        });
        release.send(()).unwrap();
        second.await.unwrap().unwrap();
        let disk: DiskState = decode_record(&std::fs::read(&store.path).unwrap()).unwrap();
        assert_eq!(disk.vote, Some(openraft::Vote::new(2, 1)));
        assert_eq!(store.state.lock().unwrap().vote, disk.vote);
    }
}

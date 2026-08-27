use std::collections::BTreeMap;
use std::fmt::Debug;
use std::fs;
use std::fs::File;
use std::fs::OpenOptions;
use std::io;
use std::io::Read;
use std::io::Seek;
use std::io::Write;
use std::marker::PhantomData;
use std::ops::RangeBounds;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::MutexGuard;
use std::sync::mpsc;
use std::thread::JoinHandle;
use std::time::Duration;
use std::time::Instant as StdInstant;

use fs4::fs_std::FileExt;
use openraft::OptionalSend;
use openraft::alias::EntryOf;
use openraft::alias::LogIdOf;
use openraft::alias::VoteOf;
use openraft::storage::IOFlushed;
use openraft::storage::LogState;
use openraft::storage::RaftLogReader;
use openraft::storage::RaftLogStorage;
use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::sync::Semaphore;
use ursula_runtime::GroupEngineMetrics;
use ursula_runtime::journal;
use ursula_shard::ShardPlacement;

use super::CoreJournalRecord;
use super::RaftGroupLogRecord;
use super::RaftGroupLogStoreInner;
use super::ensure_consecutive_entries;
use super::ensure_log_append_boundary;
use crate::codec::encode_wire;
use crate::engine::invalid_data;
use crate::rt::time::Instant;
use crate::types::CORE_LOG_GROUP_COMMIT_DELAY;
use crate::types::CORE_LOG_GROUP_COMMIT_MAX_BATCH;
use crate::types::UrsulaRaftTypeConfig;

const CORE_LOG_BLOCKING_MAX_CONCURRENCY: usize = 8;
const CORE_LOG_ONLINE_RECLAIM_MIN_BYTES: u64 = 64 * 1024 * 1024;

#[derive(Debug)]
pub struct RaftGroupFileLogStore {
    path: PathBuf,
    inner: Mutex<RaftGroupLogStoreInner>,
    file: Mutex<RaftGroupFileLogHandle>,
    metrics: Option<RaftGroupFileLogStoreMetrics>,
    core_writer: Option<Arc<CoreFileLogWriter>>,
    _lock: Option<JournalLock>,
}

#[derive(Debug, Clone)]
pub(crate) struct RaftGroupFileLogStoreMetrics {
    placement: ShardPlacement,
    metrics: GroupEngineMetrics,
}

/// Raft log writes frame MessagePack records into the shared append-only
/// journal.
type RaftGroupFileLogHandle = journal::JournalWriter;

#[derive(Debug)]
pub(crate) struct CoreFileLogWriter {
    tx: Option<mpsc::Sender<CoreFileLogWrite>>,
    recovered: Mutex<BTreeMap<u32, RaftGroupLogStoreInner>>,
    blocking: Arc<Semaphore>,
    _lock: JournalLock,
    thread: Mutex<Option<JoinHandle<()>>>,
}

#[derive(Debug)]
struct JournalLock {
    _file: File,
    path: PathBuf,
}

impl JournalLock {
    fn acquire(journal_path: &Path) -> Result<Self, io::Error> {
        let mut lock_name = journal_path.as_os_str().to_owned();
        lock_name.push(".lock");
        let path = PathBuf::from(lock_name);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(&path)?;
        if !file.try_lock_exclusive()? {
            let mut owner = String::new();
            file.rewind()?;
            let _ = file.read_to_string(&mut owner);
            let owner = owner.trim();
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                format!(
                    "OpenRaft WAL '{}' is already locked at '{}'{}",
                    journal_path.display(),
                    path.display(),
                    if owner.is_empty() {
                        String::new()
                    } else {
                        format!(" by {owner}")
                    }
                ),
            ));
        }
        file.set_len(0)?;
        file.rewind()?;
        write!(file, "pid={}", std::process::id())?;
        file.sync_data()?;
        Ok(Self { _file: file, path })
    }

    fn acquire_wait(journal_path: &Path, timeout: Duration) -> Result<Self, io::Error> {
        let deadline = StdInstant::now() + timeout;
        loop {
            match Self::acquire(journal_path) {
                Ok(lock) => return Ok(lock),
                Err(err)
                    if err.kind() == io::ErrorKind::AlreadyExists
                        && StdInstant::now() < deadline =>
                {
                    std::thread::sleep(Duration::from_millis(10));
                }
                Err(err) => return Err(err),
            }
        }
    }
}

impl Drop for JournalLock {
    fn drop(&mut self) {
        if let Err(err) = self._file.unlock() {
            tracing::warn!(path = %self.path.display(), %err, "failed to unlock OpenRaft WAL");
        }
    }
}

// File-log writer machinery is only reachable under cfg(not(madsim)) — the
// simulator's `CoreFileLogWriter::shared` panics rather than spawning a
// writer thread (DoD #1). The type still exists under both cfgs because
// `CoreFileLogWriter` holds an `mpsc::Sender<CoreFileLogWrite>` field, but
// no values flow through under cfg(madsim), hence the allow(dead_code).
#[cfg_attr(madsim, allow(dead_code))]
#[derive(Debug)]
pub(crate) struct CoreFileLogWrite {
    group_id: u32,
    record: RaftGroupLogRecord,
    response_tx: mpsc::Sender<Result<CoreFileLogWriteTiming, String>>,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct CoreFileLogWriteTiming {
    write_ns: u64,
    sync_ns: u64,
    fsyncs: u64,
    fsync_records: u64,
    reclaims: u64,
    reclaimed_bytes: u64,
    reclaim_ns: u64,
    physical_bytes: u64,
}

impl RaftGroupFileLogStore {
    pub fn open(path: impl Into<PathBuf>) -> Result<Self, io::Error> {
        Self::open_inner(path.into(), None, None)
    }

    pub fn open_with_metrics(
        path: impl Into<PathBuf>,
        placement: ShardPlacement,
        metrics: GroupEngineMetrics,
    ) -> Result<Self, io::Error> {
        Self::open_inner(
            path.into(),
            Some(RaftGroupFileLogStoreMetrics { placement, metrics }),
            None,
        )
    }

    pub(crate) fn open_with_core_writer(
        path: impl Into<PathBuf>,
        placement: ShardPlacement,
        metrics: GroupEngineMetrics,
        core_writer: Arc<CoreFileLogWriter>,
    ) -> Result<Self, io::Error> {
        Self::open_inner(
            path.into(),
            Some(RaftGroupFileLogStoreMetrics { placement, metrics }),
            Some(core_writer),
        )
    }

    pub(crate) fn open_inner(
        path: PathBuf,
        metrics: Option<RaftGroupFileLogStoreMetrics>,
        core_writer: Option<Arc<CoreFileLogWriter>>,
    ) -> Result<Self, io::Error> {
        let lock = if core_writer.is_none() {
            Some(JournalLock::acquire(&path)?)
        } else {
            None
        };
        if core_writer.is_none() && journal::migrate_legacy::<WireCodec<RaftGroupLogRecord>>(&path)?
        {
            tracing::warn!(
                path = %path.display(),
                backup = %format!("{}.v0.bak", path.display()),
                "migrated legacy OpenRaft group WAL to checksummed format"
            );
        }
        let parent_needs_sync = !path.exists();
        let inner = match (&core_writer, &metrics) {
            (Some(writer), Some(metrics)) => {
                writer.take_recovered(metrics.placement.raft_group_id.0)?
            }
            _ => load_log_store_inner(&path)?,
        };
        Ok(Self {
            path,
            inner: Mutex::new(inner),
            file: Mutex::new(RaftGroupFileLogHandle::new(parent_needs_sync)),
            metrics,
            core_writer,
            _lock: lock,
        })
    }

    pub fn shared(path: impl Into<PathBuf>) -> Result<Arc<Self>, io::Error> {
        Self::open(path).map(Arc::new)
    }

    pub fn shared_with_metrics(
        path: impl Into<PathBuf>,
        placement: ShardPlacement,
        metrics: GroupEngineMetrics,
    ) -> Result<Arc<Self>, io::Error> {
        Self::open_with_metrics(path, placement, metrics).map(Arc::new)
    }

    pub(crate) fn shared_with_core_writer(
        path: impl Into<PathBuf>,
        placement: ShardPlacement,
        metrics: GroupEngineMetrics,
        core_writer: Arc<CoreFileLogWriter>,
    ) -> Result<Arc<Self>, io::Error> {
        Self::open_with_core_writer(path, placement, metrics, core_writer).map(Arc::new)
    }

    pub(crate) fn lock_inner(&self) -> Result<MutexGuard<'_, RaftGroupLogStoreInner>, io::Error> {
        self.inner
            .lock()
            .map_err(|_| io::Error::other("raft group file log store mutex poisoned"))
    }

    pub(crate) fn lock_file(&self) -> Result<MutexGuard<'_, RaftGroupFileLogHandle>, io::Error> {
        self.file
            .lock()
            .map_err(|_| io::Error::other("raft group file log store file mutex poisoned"))
    }

    pub(crate) fn append_record_locked(
        &self,
        record: &RaftGroupLogRecord,
    ) -> Result<(), io::Error> {
        let timing = if let Some(core_writer) = &self.core_writer {
            let metrics = self
                .metrics
                .as_ref()
                .expect("core journal writer requires placement metrics");
            core_writer.append(metrics.placement.raft_group_id.0, record.clone())?
        } else {
            let mut file = self.lock_file()?;
            let (write_ns, sync_ns) = append_log_store_record(&self.path, &mut file, record)?;
            CoreFileLogWriteTiming {
                write_ns,
                sync_ns,
                fsyncs: 1,
                fsync_records: u64::try_from(raft_group_log_record_count(record))
                    .unwrap_or(u64::MAX),
                reclaims: 0,
                reclaimed_bytes: 0,
                reclaim_ns: 0,
                physical_bytes: fs::metadata(&self.path)?.len(),
            }
        };
        if let Some(metrics) = &self.metrics {
            metrics.metrics.record_wal_batch(
                metrics.placement,
                raft_group_log_record_count(record),
                timing.write_ns,
                timing.sync_ns,
            );
            metrics.metrics.record_wal_storage(
                metrics.placement,
                timing.fsyncs,
                timing.fsync_records,
                timing.reclaims,
                timing.reclaimed_bytes,
                timing.reclaim_ns,
                timing.physical_bytes,
            );
        }
        Ok(())
    }
}

impl CoreFileLogWriter {
    #[cfg(not(madsim))]
    pub(crate) fn shared(journal_path: impl Into<PathBuf>) -> Result<Arc<Self>, io::Error> {
        let journal_path = journal_path.into();
        if let Some(parent) = journal_path.parent() {
            fs::create_dir_all(parent)?;
        }
        let (tx, rx) = mpsc::channel();
        // An in-process runtime restart drops its mailboxes synchronously but
        // the core worker may need a short moment to drop the previous writer.
        // Cross-process owners continue to fail after this bounded grace period.
        let lock = JournalLock::acquire_wait(&journal_path, Duration::from_millis(500))?;
        if journal::migrate_legacy::<WireCodec<CoreJournalRecord>>(&journal_path)? {
            tracing::warn!(
                path = %journal_path.display(),
                backup = %format!("{}.v0.bak", journal_path.display()),
                "migrated legacy OpenRaft core WAL to checksummed format"
            );
        }
        let recovered = load_log_store_inners_from_core_journal(&journal_path)?;
        if let Some((before, after)) = compact_core_journal(&journal_path, &recovered)? {
            tracing::info!(
                path = %journal_path.display(),
                before_bytes = before,
                after_bytes = after,
                "compacted recovered OpenRaft core journal"
            );
        }
        let writer = Arc::new(Self {
            tx: Some(tx),
            recovered: Mutex::new(recovered),
            blocking: Arc::new(Semaphore::new(CORE_LOG_BLOCKING_MAX_CONCURRENCY)),
            _lock: lock,
            thread: Mutex::new(None),
        });
        let thread = std::thread::Builder::new()
            .name("ursula-core-file-log-writer".to_owned())
            .spawn(move || run_core_file_log_writer(journal_path, rx))
            .map_err(|err| io::Error::other(format!("spawn core file log writer: {err}")))?;
        *writer
            .thread
            .lock()
            .map_err(|_| io::Error::other("core file log thread mutex poisoned"))? = Some(thread);
        Ok(writer)
    }

    #[cfg(madsim)]
    pub(crate) fn shared(_journal_path: impl Into<PathBuf>) -> Result<Arc<Self>, io::Error> {
        panic!(
            "CoreFileLogWriter::shared spawns an OS thread and is unavailable under cfg(madsim); \
             the simulator must use memory-backed log stores via RaftGroupEngineFactory / \
             RegisteredRaftGroupEngineFactory / MadsimScopedRaftGroupEngineFactory"
        );
    }

    fn take_recovered(&self, group_id: u32) -> Result<RaftGroupLogStoreInner, io::Error> {
        self.recovered
            .lock()
            .map_err(|_| io::Error::other("core file log recovery mutex poisoned"))
            .map(|mut recovered| recovered.remove(&group_id).unwrap_or_default())
    }

    fn blocking_semaphore(&self) -> Arc<Semaphore> {
        Arc::clone(&self.blocking)
    }

    pub(crate) fn append(
        &self,
        group_id: u32,
        record: RaftGroupLogRecord,
    ) -> Result<CoreFileLogWriteTiming, io::Error> {
        let (response_tx, response_rx) = mpsc::channel();
        self.tx
            .as_ref()
            .ok_or_else(|| io::Error::other("core file log writer is shutting down"))?
            .send(CoreFileLogWrite {
                group_id,
                record,
                response_tx,
            })
            .map_err(|_| io::Error::other("core file log writer closed"))?;
        let timing = response_rx
            .recv()
            .map_err(|_| io::Error::other("core file log writer dropped response"))?
            .map_err(io::Error::other)?;
        Ok(timing)
    }
}

impl Drop for CoreFileLogWriter {
    fn drop(&mut self) {
        self.tx.take();
        let Ok(thread) = self.thread.get_mut() else {
            tracing::warn!("core file log thread mutex poisoned during shutdown");
            return;
        };
        if let Some(thread) = thread.take()
            && let Err(payload) = thread.join()
        {
            tracing::warn!(
                ?payload,
                "core file log writer thread panicked during shutdown"
            );
        }
    }
}

#[cfg_attr(madsim, allow(dead_code))]
pub(crate) fn run_core_file_log_writer(
    journal_path: PathBuf,
    rx: mpsc::Receiver<CoreFileLogWrite>,
) {
    let mut journal_handle = RaftGroupFileLogHandle::new(!journal_path.exists());

    while let Ok(first) = rx.recv() {
        let mut batch = vec![first];
        if let Ok(next) = rx.recv_timeout(CORE_LOG_GROUP_COMMIT_DELAY) {
            batch.push(next);
        }
        while batch.len() < CORE_LOG_GROUP_COMMIT_MAX_BATCH {
            match rx.try_recv() {
                Ok(next) => batch.push(next),
                Err(mpsc::TryRecvError::Empty) => break,
                Err(mpsc::TryRecvError::Disconnected) => break,
            }
        }

        let result = write_core_log_batch(&journal_path, &mut journal_handle, &batch);
        match result {
            Ok(timing) => {
                let count = u64::try_from(batch.len()).expect("batch len fits u64");
                for (request_index, request) in batch.into_iter().enumerate() {
                    let owns_batch_sample = request_index == 0;
                    let per_request = CoreFileLogWriteTiming {
                        write_ns: timing.write_ns / count.max(1),
                        sync_ns: timing.sync_ns / count.max(1),
                        fsyncs: u64::from(owns_batch_sample),
                        fsync_records: if owns_batch_sample { count } else { 0 },
                        reclaims: if owns_batch_sample {
                            timing.reclaims
                        } else {
                            0
                        },
                        reclaimed_bytes: if owns_batch_sample {
                            timing.reclaimed_bytes
                        } else {
                            0
                        },
                        reclaim_ns: if owns_batch_sample {
                            timing.reclaim_ns
                        } else {
                            0
                        },
                        physical_bytes: timing.physical_bytes,
                    };
                    let _ = request.response_tx.send(Ok(per_request));
                }
            }
            Err(err) => {
                let message = err.to_string();
                for request in batch {
                    let _ = request.response_tx.send(Err(message.clone()));
                }
            }
        }
    }
}

#[cfg_attr(madsim, allow(dead_code))]
pub(crate) fn write_core_log_batch(
    journal_path: &Path,
    journal_handle: &mut RaftGroupFileLogHandle,
    batch: &[CoreFileLogWrite],
) -> Result<CoreFileLogWriteTiming, io::Error> {
    let write_started_at = Instant::now();
    for request in batch {
        let journal_record = CoreJournalRecord {
            group_id: request.group_id,
            record: request.record.clone(),
        };
        write_wire_frame_to_file(journal_path, journal_handle, &journal_record)?;
    }
    let write_ns = elapsed_ns(write_started_at);

    let sync_started_at = Instant::now();
    sync_file_handle(journal_path, journal_handle)?;
    let sync_ns = elapsed_ns(sync_started_at);
    let reclaim_started_at = Instant::now();
    let mut reclaims = 0;
    let mut reclaimed_bytes = 0;
    if batch.iter().any(|request| {
        matches!(
            &request.record,
            RaftGroupLogRecord::Purge(_) | RaftGroupLogRecord::TruncateAfter(_)
        )
    }) && let Some((before, after)) = reclaim_core_journal_if_needed(
        journal_path,
        journal_handle,
        CORE_LOG_ONLINE_RECLAIM_MIN_BYTES,
    )? {
        reclaims = 1;
        reclaimed_bytes = before.saturating_sub(after);
        tracing::info!(
            path = %journal_path.display(),
            before_bytes = before,
            after_bytes = after,
            reclaimed_bytes = before.saturating_sub(after),
            "reclaimed obsolete OpenRaft core WAL records online"
        );
    }
    let reclaim_ns = if reclaims == 0 {
        0
    } else {
        elapsed_ns(reclaim_started_at)
    };
    Ok(CoreFileLogWriteTiming {
        write_ns,
        sync_ns,
        fsyncs: 1,
        fsync_records: u64::try_from(batch.len()).unwrap_or(u64::MAX),
        reclaims,
        reclaimed_bytes,
        reclaim_ns,
        physical_bytes: fs::metadata(journal_path)?.len(),
    })
}

impl RaftLogReader<UrsulaRaftTypeConfig> for Arc<RaftGroupFileLogStore> {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<EntryOf<UrsulaRaftTypeConfig>>, io::Error> {
        let inner = self.lock_inner()?;
        let entries = inner
            .entries
            .range(range)
            .map(|(_, entry)| entry.clone())
            .collect::<Vec<_>>();

        ensure_consecutive_entries::<UrsulaRaftTypeConfig>(&entries)?;
        Ok(entries)
    }

    async fn read_vote(&mut self) -> Result<Option<VoteOf<UrsulaRaftTypeConfig>>, io::Error> {
        Ok(self.lock_inner()?.vote)
    }
}

impl RaftLogStorage<UrsulaRaftTypeConfig> for Arc<RaftGroupFileLogStore> {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<UrsulaRaftTypeConfig>, io::Error> {
        let inner = self.lock_inner()?;
        let last_log_id = inner
            .entries
            .last_key_value()
            .map(|(_, entry)| entry.log_id)
            .or(inner.last_purged_log_id);

        Ok(LogState {
            last_purged_log_id: inner.last_purged_log_id,
            last_log_id,
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &VoteOf<UrsulaRaftTypeConfig>) -> Result<(), io::Error> {
        let store = Arc::clone(self);
        let vote = *vote;
        let blocking = store
            .core_writer
            .as_ref()
            .map(|writer| writer.blocking_semaphore());
        spawn_log_store_blocking(blocking, move || {
            let mut inner = store.lock_inner()?;
            if inner.vote == Some(vote) {
                return Ok(());
            }
            store.append_record_locked(&RaftGroupLogRecord::SaveVote(vote))?;
            inner.vote = Some(vote);
            Ok(())
        })
        .await
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogIdOf<UrsulaRaftTypeConfig>>,
    ) -> Result<(), io::Error> {
        let store = Arc::clone(self);
        let blocking = store
            .core_writer
            .as_ref()
            .map(|writer| writer.blocking_semaphore());
        spawn_log_store_blocking(blocking, move || {
            let mut inner = store.lock_inner()?;
            if inner.committed == committed {
                return Ok(());
            }
            store.append_record_locked(&RaftGroupLogRecord::SaveCommitted(committed))?;
            inner.committed = committed;
            Ok(())
        })
        .await
    }

    async fn read_committed(&mut self) -> Result<Option<LogIdOf<UrsulaRaftTypeConfig>>, io::Error> {
        Ok(self.lock_inner()?.committed)
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: IOFlushed<UrsulaRaftTypeConfig>,
    ) -> Result<(), io::Error>
    where
        I: IntoIterator<Item = EntryOf<UrsulaRaftTypeConfig>> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let entries = entries.into_iter().collect::<Vec<_>>();
        let store = Arc::clone(self);
        let blocking = store
            .core_writer
            .as_ref()
            .map(|writer| writer.blocking_semaphore());
        spawn_log_store_blocking(blocking, move || {
            ensure_consecutive_entries::<UrsulaRaftTypeConfig>(&entries)?;

            let mut inner = store.lock_inner()?;
            ensure_log_append_boundary::<UrsulaRaftTypeConfig>(&inner, &entries)?;

            let record = RaftGroupLogRecord::Append(entries.clone());
            if let Err(err) = store.append_record_locked(&record) {
                callback.io_completed(Err(io::Error::new(err.kind(), err.to_string())));
                return Err(err);
            }
            for entry in entries {
                inner.entries.insert(entry.log_id.index, entry);
            }
            callback.io_completed(Ok(()));
            Ok(())
        })
        .await
    }

    async fn truncate_after(
        &mut self,
        last_log_id: Option<LogIdOf<UrsulaRaftTypeConfig>>,
    ) -> Result<(), io::Error> {
        let store = Arc::clone(self);
        let blocking = store
            .core_writer
            .as_ref()
            .map(|writer| writer.blocking_semaphore());
        spawn_log_store_blocking(blocking, move || {
            let start_index = last_log_id.map_or(0, |log_id| log_id.index + 1);
            let mut inner = store.lock_inner()?;
            store.append_record_locked(&RaftGroupLogRecord::TruncateAfter(last_log_id))?;
            inner.entries.retain(|index, _| *index < start_index);
            Ok(())
        })
        .await
    }

    async fn purge(&mut self, log_id: LogIdOf<UrsulaRaftTypeConfig>) -> Result<(), io::Error> {
        let store = Arc::clone(self);
        let blocking = store
            .core_writer
            .as_ref()
            .map(|writer| writer.blocking_semaphore());
        spawn_log_store_blocking(blocking, move || {
            let mut inner = store.lock_inner()?;
            if inner.last_purged_log_id > Some(log_id) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!(
                        "cannot move last purged log id backward from {:?} to {:?}",
                        inner.last_purged_log_id, log_id
                    ),
                ));
            }

            store.append_record_locked(&RaftGroupLogRecord::Purge(log_id))?;
            inner.last_purged_log_id = Some(log_id);
            inner.entries.retain(|index, _| *index > log_id.index);
            Ok(())
        })
        .await
    }
}

pub(crate) async fn spawn_log_store_blocking<T>(
    blocking: Option<Arc<Semaphore>>,
    f: impl FnOnce() -> Result<T, io::Error> + Send + 'static,
) -> Result<T, io::Error>
where
    T: Send + 'static,
{
    let permit = match blocking {
        Some(blocking) => Some(
            blocking
                .acquire_owned()
                .await
                .map_err(|_| io::Error::other("OpenRaft file log blocking limiter closed"))?,
        ),
        None => None,
    };
    tokio::task::spawn_blocking(move || {
        let _permit = permit;
        f()
    })
    .await
    .map_err(|err| io::Error::other(format!("join OpenRaft file log task: {err}")))?
}

pub(crate) fn load_log_store_inner(path: &Path) -> Result<RaftGroupLogStoreInner, io::Error> {
    if !path.exists() {
        return Ok(RaftGroupLogStoreInner::default());
    }

    let mut inner = RaftGroupLogStoreInner::default();
    for (record_index, record) in read_wire_frames_from_file::<RaftGroupLogRecord>(path)?
        .into_iter()
        .enumerate()
    {
        apply_log_store_record(&mut inner, record).map_err(|err| {
            io::Error::new(
                err.kind(),
                format!(
                    "replay OpenRaft log record '{}' record {}: {err}",
                    path.display(),
                    record_index + 1
                ),
            )
        })?;
    }
    Ok(inner)
}

#[cfg(test)]
pub(crate) fn load_log_store_inner_from_core_journal(
    journal_path: &Path,
    placement: ShardPlacement,
) -> Result<RaftGroupLogStoreInner, io::Error> {
    Ok(load_log_store_inners_from_core_journal(journal_path)?
        .remove(&placement.raft_group_id.0)
        .unwrap_or_default())
}

fn load_log_store_inners_from_core_journal(
    journal_path: &Path,
) -> Result<BTreeMap<u32, RaftGroupLogStoreInner>, io::Error> {
    let mut inners = BTreeMap::<u32, RaftGroupLogStoreInner>::new();
    let mut record_index = 0_usize;
    journal::replay_each::<WireCodec<CoreJournalRecord>>(journal_path, |record| {
        record_index = record_index.saturating_add(1);
        apply_log_store_record(inners.entry(record.group_id).or_default(), record.record).map_err(
            |err| {
                io::Error::new(
                    err.kind(),
                    format!(
                        "replay OpenRaft core journal record '{}' record {record_index}: {err}",
                        journal_path.display(),
                    ),
                )
            },
        )
    })?;
    Ok(inners)
}

#[cfg(not(madsim))]
fn compact_core_journal(
    journal_path: &Path,
    inners: &BTreeMap<u32, RaftGroupLogStoreInner>,
) -> Result<Option<(u64, u64)>, io::Error> {
    if !journal_path.exists() {
        return Ok(None);
    }
    let before = fs::metadata(journal_path)?.len();
    let compact_path = journal_path.with_extension("compact");
    if compact_path.exists() {
        fs::remove_file(&compact_path)?;
    }

    let mut handle = RaftGroupFileLogHandle::new(true);
    let mut wrote_record = false;
    for (group_id, inner) in inners {
        let mut write = |record| -> Result<(), io::Error> {
            wrote_record = true;
            write_wire_frame_to_file(&compact_path, &mut handle, &CoreJournalRecord {
                group_id: *group_id,
                record,
            })
        };
        if let Some(vote) = inner.vote {
            write(RaftGroupLogRecord::SaveVote(vote))?;
        }
        if let Some(committed) = inner.committed {
            write(RaftGroupLogRecord::SaveCommitted(Some(committed)))?;
        }
        if let Some(purged) = inner.last_purged_log_id {
            write(RaftGroupLogRecord::Purge(purged))?;
        }
        if !inner.entries.is_empty() {
            write(RaftGroupLogRecord::Append(
                inner.entries.values().cloned().collect(),
            ))?;
        }
    }
    if wrote_record {
        sync_file_handle(&compact_path, &mut handle)?;
    } else {
        handle.ensure_created(&compact_path)?;
        sync_file_handle(&compact_path, &mut handle)?;
    }
    drop(handle);

    let after = fs::metadata(&compact_path)?.len();
    if after >= before {
        fs::remove_file(&compact_path)?;
        return Ok(None);
    }
    fs::rename(&compact_path, journal_path)?;
    if let Some(parent) = journal_path.parent() {
        fs::File::open(parent)?.sync_all()?;
    }
    Ok(Some((before, after)))
}

#[cfg(not(madsim))]
fn reclaim_core_journal_if_needed(
    journal_path: &Path,
    journal_handle: &mut RaftGroupFileLogHandle,
    min_physical_bytes: u64,
) -> Result<Option<(u64, u64)>, io::Error> {
    if !journal_path.exists() || fs::metadata(journal_path)?.len() < min_physical_bytes {
        return Ok(None);
    }

    // Close the append descriptor before atomically replacing the path. This
    // avoids continuing to append to the unlinked old inode after `rename` and
    // keeps the replacement portable to filesystems that reject renaming over
    // an open destination.
    let old_handle = std::mem::replace(
        journal_handle,
        RaftGroupFileLogHandle::new(!journal_path.exists()),
    );
    drop(old_handle);

    let inners = load_log_store_inners_from_core_journal(journal_path)?;
    let compacted = compact_core_journal(journal_path, &inners)?;
    *journal_handle = RaftGroupFileLogHandle::new(false);
    Ok(compacted)
}

#[cfg(madsim)]
fn reclaim_core_journal_if_needed(
    _journal_path: &Path,
    _journal_handle: &mut RaftGroupFileLogHandle,
    _min_physical_bytes: u64,
) -> Result<Option<(u64, u64)>, io::Error> {
    Ok(None)
}

/// Frames Raft log records as length-delimited MessagePack for the shared
/// journal (see [`crate::codec::encode_wire`]).
struct WireCodec<T>(PhantomData<T>);

impl<T: Serialize + DeserializeOwned> journal::FrameCodec for WireCodec<T> {
    type Record = T;

    fn encode(record: &T) -> Vec<u8> {
        encode_wire(record).into()
    }

    fn decode(payload: &[u8]) -> Result<T, io::Error> {
        rmp_serde::from_slice(payload).map_err(invalid_data)
    }
}

pub(crate) fn read_wire_frames_from_file<T: Serialize + DeserializeOwned>(
    path: &Path,
) -> Result<Vec<T>, io::Error> {
    journal::replay::<WireCodec<T>>(path)
}

pub(crate) fn append_log_store_record(
    path: &Path,
    handle: &mut RaftGroupFileLogHandle,
    record: &RaftGroupLogRecord,
) -> Result<(u64, u64), io::Error> {
    let write_started_at = Instant::now();
    write_wire_frame_to_file(path, handle, record)?;
    let write_ns = elapsed_ns(write_started_at);

    let sync_started_at = Instant::now();
    sync_file_handle(path, handle)?;
    Ok((write_ns, elapsed_ns(sync_started_at)))
}

pub(crate) fn write_wire_frame_to_file<T: Serialize + DeserializeOwned>(
    path: &Path,
    handle: &mut RaftGroupFileLogHandle,
    value: &T,
) -> Result<(), io::Error> {
    handle.append::<WireCodec<T>>(path, value)
}

#[cfg(test)]
pub(crate) fn read_wire_frames<T: Serialize + DeserializeOwned>(
    bytes: &[u8],
) -> Result<Vec<T>, io::Error> {
    journal::decode_frames::<WireCodec<T>>(bytes).map(|(records, _)| records)
}

pub(crate) fn sync_file_handle(
    path: &Path,
    handle: &mut RaftGroupFileLogHandle,
) -> Result<(), io::Error> {
    handle.sync(path)
}

pub(crate) fn raft_group_log_record_count(record: &RaftGroupLogRecord) -> usize {
    match record {
        RaftGroupLogRecord::Append(entries) => entries.len(),
        _ => 1,
    }
}

pub(crate) fn elapsed_ns(started_at: Instant) -> u64 {
    u64::try_from(started_at.elapsed().as_nanos()).unwrap_or(u64::MAX)
}

pub(crate) fn apply_log_store_record(
    inner: &mut RaftGroupLogStoreInner,
    record: RaftGroupLogRecord,
) -> Result<(), io::Error> {
    match record {
        RaftGroupLogRecord::SaveVote(vote) => {
            inner.vote = Some(vote);
            Ok(())
        }
        RaftGroupLogRecord::SaveCommitted(committed) => {
            inner.committed = committed;
            Ok(())
        }
        RaftGroupLogRecord::Append(entries) => {
            ensure_consecutive_entries::<UrsulaRaftTypeConfig>(&entries)?;
            for entry in entries {
                inner.entries.insert(entry.log_id.index, entry);
            }
            super::ensure_consecutive_log::<UrsulaRaftTypeConfig>(&inner.entries)
        }
        RaftGroupLogRecord::TruncateAfter(last_log_id) => {
            let start_index = last_log_id.map_or(0, |log_id| log_id.index + 1);
            inner.entries.retain(|index, _| *index < start_index);
            Ok(())
        }
        RaftGroupLogRecord::Purge(log_id) => {
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
}

#[cfg(test)]
mod tests {
    use std::fs::OpenOptions;
    use std::io::Write;
    use std::sync::atomic::AtomicU64;
    use std::sync::atomic::Ordering;

    use openraft::EntryPayload;
    use openraft::LogId;
    use openraft::entry::RaftEntry;
    use openraft::vote::RaftLeaderId;
    use openraft::vote::leader_id_adv::CommittedLeaderId;
    use ursula_shard::CoreId;
    use ursula_shard::RaftGroupId;
    use ursula_shard::ShardId;

    use super::*;

    static TEMP_JOURNAL_COUNTER: AtomicU64 = AtomicU64::new(0);

    fn temp_journal_path(name: &str) -> PathBuf {
        let nonce = TEMP_JOURNAL_COUNTER.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir()
            .join("ursula-raft-file-log-tests")
            .join(format!("{name}-{}-{nonce}.bin", std::process::id()));
        let _ = fs::remove_file(&path);
        path
    }

    fn append_torn_frame(path: &Path) {
        let mut file = OpenOptions::new()
            .append(true)
            .open(path)
            .expect("open journal for torn append");
        file.write_all(&128_u32.to_le_bytes())
            .expect("write torn frame length");
        file.write_all(b"torn").expect("write partial torn payload");
        file.sync_data().expect("sync torn tail");
    }

    fn test_log_id(index: u64) -> LogIdOf<UrsulaRaftTypeConfig> {
        LogId {
            leader_id: CommittedLeaderId::new(5, 1),
            index,
        }
    }

    fn blank_entry(index: u64) -> EntryOf<UrsulaRaftTypeConfig> {
        EntryOf::<UrsulaRaftTypeConfig>::new(test_log_id(index), EntryPayload::Blank)
    }

    fn committed_vote() -> VoteOf<UrsulaRaftTypeConfig> {
        openraft::Vote::new_committed(7, 1)
    }

    #[test]
    fn load_log_store_inner_truncates_torn_tail() {
        let path = temp_journal_path("group-log-torn-tail");
        let vote = committed_vote();
        let mut handle = RaftGroupFileLogHandle::new(true);
        append_log_store_record(&path, &mut handle, &RaftGroupLogRecord::SaveVote(vote))
            .expect("write complete vote record");
        drop(handle);
        let valid_len = fs::metadata(&path).expect("journal metadata").len();

        append_torn_frame(&path);
        assert!(
            fs::metadata(&path)
                .expect("journal metadata after torn append")
                .len()
                > valid_len
        );

        let inner = load_log_store_inner(&path).expect("load journal with torn tail");
        assert_eq!(inner.vote, Some(vote));
        assert_eq!(
            fs::metadata(&path)
                .expect("journal metadata after recovery")
                .len(),
            valid_len
        );

        let _ = fs::remove_file(&path);
    }

    #[test]
    fn load_core_journal_truncates_torn_tail() {
        let path = temp_journal_path("core-journal-torn-tail");
        let placement = ShardPlacement {
            core_id: CoreId(0),
            shard_id: ShardId(0),
            raft_group_id: RaftGroupId(3),
        };
        let vote = committed_vote();
        let mut handle = RaftGroupFileLogHandle::new(true);
        write_wire_frame_to_file(&path, &mut handle, &CoreJournalRecord {
            group_id: placement.raft_group_id.0,
            record: RaftGroupLogRecord::SaveVote(vote),
        })
        .expect("write complete core journal record");
        sync_file_handle(&path, &mut handle).expect("sync complete core journal record");
        drop(handle);
        let valid_len = fs::metadata(&path).expect("core journal metadata").len();

        append_torn_frame(&path);
        assert!(
            fs::metadata(&path)
                .expect("core journal metadata after torn append")
                .len()
                > valid_len
        );

        let inner = load_log_store_inner_from_core_journal(&path, placement)
            .expect("load core journal with torn tail");
        assert_eq!(inner.vote, Some(vote));
        assert_eq!(
            fs::metadata(&path)
                .expect("core journal metadata after recovery")
                .len(),
            valid_len
        );

        let _ = fs::remove_file(&path);
    }

    #[test]
    fn load_core_journal_distributes_all_groups_in_one_scan() {
        let path = temp_journal_path("core-journal-groups");
        let first_vote = openraft::Vote::new_committed(3, 1);
        let second_vote = openraft::Vote::new_committed(5, 2);
        let mut handle = RaftGroupFileLogHandle::new(true);
        for (group_id, vote) in [(3, first_vote), (7, second_vote)] {
            write_wire_frame_to_file(&path, &mut handle, &CoreJournalRecord {
                group_id,
                record: RaftGroupLogRecord::SaveVote(vote),
            })
            .expect("write core journal group record");
        }
        sync_file_handle(&path, &mut handle).expect("sync core journal groups");
        drop(handle);

        let inners =
            load_log_store_inners_from_core_journal(&path).expect("load all core journal groups");
        assert_eq!(inners.len(), 2);
        assert_eq!(
            inners.get(&3).and_then(|inner| inner.vote),
            Some(first_vote)
        );
        assert_eq!(
            inners.get(&7).and_then(|inner| inner.vote),
            Some(second_vote)
        );

        let _ = fs::remove_file(&path);
    }

    #[cfg(not(madsim))]
    #[test]
    fn compact_core_journal_keeps_only_recovered_state() {
        let path = temp_journal_path("core-journal-compact");
        let first_vote = openraft::Vote::new_committed(3, 1);
        let latest_vote = openraft::Vote::new_committed(5, 1);
        let mut handle = RaftGroupFileLogHandle::new(true);
        for vote in std::iter::repeat_n(first_vote, 100).chain([latest_vote]) {
            write_wire_frame_to_file(&path, &mut handle, &CoreJournalRecord {
                group_id: 7,
                record: RaftGroupLogRecord::SaveVote(vote),
            })
            .expect("write redundant vote");
        }
        for record in [
            RaftGroupLogRecord::Append((1..=3).map(blank_entry).collect()),
            RaftGroupLogRecord::Purge(test_log_id(2)),
            RaftGroupLogRecord::SaveCommitted(Some(test_log_id(3))),
        ] {
            write_wire_frame_to_file(&path, &mut handle, &CoreJournalRecord {
                group_id: 7,
                record,
            })
            .expect("write retained log state");
        }
        sync_file_handle(&path, &mut handle).expect("sync redundant journal");
        drop(handle);
        let before = fs::metadata(&path).expect("journal metadata").len();
        let inners = load_log_store_inners_from_core_journal(&path).expect("replay journal");

        let compacted = compact_core_journal(&path, &inners)
            .expect("compact journal")
            .expect("redundant journal should shrink");

        assert_eq!(compacted.0, before);
        assert!(compacted.1 < compacted.0);
        let recovered = load_log_store_inners_from_core_journal(&path).expect("replay compacted");
        assert_eq!(
            recovered.get(&7).and_then(|inner| inner.vote),
            Some(latest_vote)
        );
        let recovered = recovered.get(&7).expect("recovered group");
        assert_eq!(recovered.last_purged_log_id, Some(test_log_id(2)));
        assert_eq!(recovered.committed, Some(test_log_id(3)));
        assert_eq!(recovered.entries.keys().copied().collect::<Vec<_>>(), [3]);
        let _ = fs::remove_file(&path);
    }

    #[cfg(not(madsim))]
    #[test]
    fn online_reclaim_reopens_the_replaced_core_journal() {
        let path = temp_journal_path("core-journal-online-reclaim");
        let mut handle = RaftGroupFileLogHandle::new(true);
        for index in 1..=256 {
            write_wire_frame_to_file(&path, &mut handle, &CoreJournalRecord {
                group_id: 7,
                record: RaftGroupLogRecord::Append(vec![blank_entry(index)]),
            })
            .expect("write historical core record");
        }
        write_wire_frame_to_file(&path, &mut handle, &CoreJournalRecord {
            group_id: 7,
            record: RaftGroupLogRecord::Purge(test_log_id(255)),
        })
        .expect("write purge frontier");
        sync_file_handle(&path, &mut handle).expect("sync historical core journal");
        let before = fs::metadata(&path).expect("journal metadata").len();

        let (reclaim_before, reclaim_after) = reclaim_core_journal_if_needed(&path, &mut handle, 0)
            .expect("online reclaim")
            .expect("historical journal should shrink");
        assert_eq!(reclaim_before, before);
        assert!(reclaim_after < reclaim_before);

        write_wire_frame_to_file(&path, &mut handle, &CoreJournalRecord {
            group_id: 7,
            record: RaftGroupLogRecord::Append(vec![blank_entry(257)]),
        })
        .expect("append after atomic replacement");
        sync_file_handle(&path, &mut handle).expect("sync append after reclaim");
        drop(handle);

        let recovered =
            load_log_store_inners_from_core_journal(&path).expect("replay reclaimed WAL");
        let group = recovered.get(&7).expect("recovered group");
        assert_eq!(group.last_purged_log_id, Some(test_log_id(255)));
        assert_eq!(group.entries.keys().copied().collect::<Vec<_>>(), [
            256, 257
        ]);
        let _ = fs::remove_file(&path);
    }

    #[test]
    fn direct_file_log_rejects_a_second_owner() {
        let path = temp_journal_path("direct-exclusive-lock");
        let first = RaftGroupFileLogStore::shared(&path).expect("open first owner");
        let err = RaftGroupFileLogStore::shared(&path).expect_err("second owner must fail");
        assert_eq!(err.kind(), io::ErrorKind::AlreadyExists);
        assert!(err.to_string().contains("already locked"));
        assert!(err.to_string().contains("pid="));
        drop(first);
        let reopened = RaftGroupFileLogStore::shared(&path).expect("lock releases on drop");
        drop(reopened);
        let _ = fs::remove_file(&path);
        let _ = fs::remove_file(format!("{}.lock", path.display()));
    }

    #[cfg(not(madsim))]
    #[test]
    fn core_file_log_rejects_a_second_owner() {
        let path = temp_journal_path("core-exclusive-lock");
        let first = CoreFileLogWriter::shared(&path).expect("open first core owner");
        let err = CoreFileLogWriter::shared(&path).expect_err("second core owner must fail");
        assert_eq!(err.kind(), io::ErrorKind::AlreadyExists);
        assert!(err.to_string().contains("already locked"));
        drop(first);
        let reopened = CoreFileLogWriter::shared(&path).expect("core lock releases on drop");
        drop(reopened);
        let _ = fs::remove_file(&path);
        let _ = fs::remove_file(format!("{}.lock", path.display()));
    }
}

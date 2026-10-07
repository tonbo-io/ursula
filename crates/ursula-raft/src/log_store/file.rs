//! The durable OpenRaft log store: every group's records go to its core's
//! shared journal through one writer per core.
//!
//! The writer is an async loop over the runtime shim's channel. Production
//! runs it on a dedicated OS thread with a current-thread runtime, so blocking
//! file I/O stays off the async workers; `cfg(madsim)` runs it as a simulated
//! task over the simulated disk. Callers await a reply that arrives after the
//! batch is written and, when it needs it, `fsync`ed.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::fmt::Debug;
use std::io;
use std::marker::PhantomData;
use std::ops::RangeBounds;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::MutexGuard;
#[cfg(not(madsim))]
use std::task::Context;
#[cfg(not(madsim))]
use std::task::Poll;
#[cfg(not(madsim))]
use std::task::Wake;
#[cfg(not(madsim))]
use std::task::Waker;
use std::time::Duration;

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
use ursula_runtime::GroupEngineMetrics;
use ursula_shard::RaftGroupId;
use ursula_shard::ShardPlacement;

use super::CoreJournalRecord;
use super::RaftGroupLogRecord;
use super::RaftGroupLogStoreInner;
use super::disk::Disk;
use super::disk::DiskLock;
use super::disk::JournalDisk;
use super::disk::LockAttempt;
use super::ensure_consecutive_entries;
use super::ensure_log_append_boundary;
use super::journal;
use super::journal::JournalWriter;
use super::truncate_entries_after;
use crate::codec::encode_wire;
use crate::engine::invalid_data;
use crate::rt::sync::mpsc;
use crate::rt::sync::oneshot;
use crate::rt::time::Instant;
use crate::types::CORE_LOG_GROUP_COMMIT_DELAY;
use crate::types::CORE_LOG_GROUP_COMMIT_MAX_BATCH;
use crate::types::UrsulaRaftTypeConfig;

/// Journal size at which a purge or truncate rewrites the journal online.
#[cfg(not(madsim))]
const CORE_LOG_ONLINE_RECLAIM_MIN_BYTES: u64 = 64 * 1024 * 1024;
/// Simulated journals stay small, so the simulator reclaims at a lower size to
/// exercise the online rewrite.
#[cfg(madsim)]
const CORE_LOG_ONLINE_RECLAIM_MIN_BYTES: u64 = 16 * 1024;

/// Failure of the per-core journal.
#[derive(Debug, Clone, thiserror::Error)]
pub(crate) enum CoreJournalError {
    #[error("OpenRaft core journal I/O on '{}': {source}", .path.display())]
    Io {
        path: PathBuf,
        #[source]
        source: Arc<io::Error>,
    },
    #[error(
        "OpenRaft WAL '{}' is already locked at '{}'{}",
        .journal.display(),
        .lock.display(),
        .owner.as_deref().map(|owner| format!(" by {owner}")).unwrap_or_default()
    )]
    Locked {
        journal: PathBuf,
        lock: PathBuf,
        owner: Option<String>,
    },
    #[error(
        "raft group {} is already open on OpenRaft core journal '{}'",
        .raft_group_id.0,
        .journal.display()
    )]
    GroupAlreadyOpen {
        journal: PathBuf,
        raft_group_id: RaftGroupId,
    },
    #[cfg(not(madsim))]
    #[error("spawn the OpenRaft core journal writer: {source}")]
    SpawnWriter {
        #[source]
        source: Arc<io::Error>,
    },
    #[error("OpenRaft core journal writer for '{}' has stopped", .journal.display())]
    WriterStopped { journal: PathBuf },
    #[error("OpenRaft core journal state mutex poisoned")]
    Poisoned,
}

impl CoreJournalError {
    fn io(path: &Path, source: io::Error) -> Self {
        Self::Io {
            path: path.to_owned(),
            source: Arc::new(source),
        }
    }
}

/// OpenRaft storage methods report `io::Error`; this is the one conversion.
impl From<CoreJournalError> for io::Error {
    fn from(err: CoreJournalError) -> Self {
        let kind = match &err {
            CoreJournalError::Io { source, .. } => source.kind(),
            #[cfg(not(madsim))]
            CoreJournalError::SpawnWriter { source } => source.kind(),
            CoreJournalError::Locked { .. } | CoreJournalError::GroupAlreadyOpen { .. } => {
                io::ErrorKind::AlreadyExists
            }
            CoreJournalError::WriterStopped { .. } => io::ErrorKind::BrokenPipe,
            CoreJournalError::Poisoned => io::ErrorKind::Other,
        };
        io::Error::new(kind, err)
    }
}

/// One raft group's durable OpenRaft log, stored in its core's shared journal.
#[derive(Debug)]
pub struct RaftGroupFileLogStore {
    placement: ShardPlacement,
    metrics: GroupEngineMetrics,
    inner: Mutex<RaftGroupLogStoreInner>,
    /// Serializes mutations so the journal records them in the same order as
    /// the in-memory state applies them.
    write_order: crate::rt::sync::Mutex<()>,
    core_writer: Arc<CoreFileLogWriter>,
}

/// The single writer of one core's journal.
#[derive(Debug)]
pub(crate) struct CoreFileLogWriter {
    journal_path: PathBuf,
    tx: Option<mpsc::UnboundedSender<CoreFileLogWrite>>,
    groups: Mutex<RecoveredGroups>,
    worker: Option<WriterWorker>,
    /// Released after the worker has stopped (see `Drop`).
    _lock: DiskLock,
}

/// Recovered per-group state, handed out once per group.
#[derive(Debug, Default)]
struct RecoveredGroups {
    recovered: BTreeMap<u32, RaftGroupLogStoreInner>,
    opened: BTreeSet<u32>,
}

#[cfg(not(madsim))]
type WriterWorker = std::thread::JoinHandle<()>;
#[cfg(madsim)]
type WriterWorker = sim_tokio::task::JoinHandle<()>;

#[derive(Debug)]
struct CoreFileLogWrite {
    record: CoreJournalRecord,
    reply: oneshot::Sender<Result<CoreFileLogWriteTiming, CoreJournalError>>,
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
    pub(crate) fn open(
        placement: ShardPlacement,
        metrics: GroupEngineMetrics,
        core_writer: Arc<CoreFileLogWriter>,
    ) -> Result<Arc<Self>, CoreJournalError> {
        let inner = core_writer.take_recovered(placement.raft_group_id)?;
        Ok(Arc::new(Self {
            placement,
            metrics,
            inner: Mutex::new(inner),
            write_order: crate::rt::sync::Mutex::new(()),
            core_writer,
        }))
    }

    fn lock_inner(&self) -> Result<MutexGuard<'_, RaftGroupLogStoreInner>, CoreJournalError> {
        self.inner
            .lock()
            .map_err(|_poisoned| CoreJournalError::Poisoned)
    }

    /// Journals `record` and waits until the writer acknowledges it.
    async fn append_record(&self, record: RaftGroupLogRecord) -> Result<(), CoreJournalError> {
        let record_count = raft_group_log_record_count(&record);
        let timing = self
            .core_writer
            .append(CoreJournalRecord {
                group_id: self.placement.raft_group_id.0,
                record,
            })
            .await?;
        self.metrics.record_wal_batch(
            self.placement,
            record_count,
            timing.write_ns,
            timing.sync_ns,
        );
        self.metrics.record_wal_storage(
            self.placement,
            timing.fsyncs,
            timing.fsync_records,
            timing.reclaims,
            timing.reclaimed_bytes,
            timing.reclaim_ns,
            timing.physical_bytes,
        );
        Ok(())
    }
}

impl CoreFileLogWriter {
    /// Opens the journal at `journal_path`: takes its lock, recovers every
    /// group, compacts the recovered journal and starts the writer.
    pub(crate) fn open(
        journal_path: PathBuf,
        recovery_metrics: Option<(ShardPlacement, GroupEngineMetrics)>,
    ) -> Result<Arc<Self>, CoreJournalError> {
        if let Some(parent) = journal_path.parent() {
            Disk::create_dir_all(parent).map_err(|source| CoreJournalError::io(parent, source))?;
        }
        let lock = acquire_journal_lock(&journal_path)?;
        let recovery_started_at = Instant::now();
        let recovery_bytes = Disk::file_len(&journal_path).unwrap_or(0);
        let (recovered, recovery_records) =
            load_log_store_inners_from_core_journal_with_stats(&journal_path)
                .map_err(|source| CoreJournalError::io(&journal_path, source))?;
        let recovery_ns = elapsed_ns(recovery_started_at);
        let recovery_live_entries = recovered.values().fold(0_u64, |total, inner| {
            total.saturating_add(u64::try_from(inner.entries.len()).unwrap_or(u64::MAX))
        });
        if let Some((placement, metrics)) = &recovery_metrics {
            metrics.record_wal_recovery(
                *placement,
                recovery_ns,
                u64::try_from(recovery_records).unwrap_or(u64::MAX),
                recovery_bytes,
                recovery_live_entries,
            );
        }
        tracing::info!(
            path = %journal_path.display(),
            recovery_ns,
            recovery_records,
            recovery_bytes,
            recovery_live_entries,
            "recovered OpenRaft core journal"
        );
        if let Some((before, after)) = compact_core_journal(&journal_path, &recovered)
            .map_err(|source| CoreJournalError::io(&journal_path, source))?
        {
            tracing::info!(
                path = %journal_path.display(),
                before_bytes = before,
                after_bytes = after,
                "compacted recovered OpenRaft core journal"
            );
        }
        let (tx, rx) = mpsc::unbounded_channel();
        let worker = spawn_core_file_log_writer(journal_path.clone(), rx)?;
        Ok(Arc::new(Self {
            journal_path,
            tx: Some(tx),
            groups: Mutex::new(RecoveredGroups {
                recovered,
                opened: BTreeSet::new(),
            }),
            worker: Some(worker),
            _lock: lock,
        }))
    }

    /// Hands out a group's recovered state. A group opens once per writer:
    /// its state after a reopen would miss what the closed store wrote.
    fn take_recovered(
        &self,
        raft_group_id: RaftGroupId,
    ) -> Result<RaftGroupLogStoreInner, CoreJournalError> {
        let mut groups = self
            .groups
            .lock()
            .map_err(|_poisoned| CoreJournalError::Poisoned)?;
        if !groups.opened.insert(raft_group_id.0) {
            return Err(CoreJournalError::GroupAlreadyOpen {
                journal: self.journal_path.clone(),
                raft_group_id,
            });
        }
        Ok(groups
            .recovered
            .remove(&raft_group_id.0)
            .unwrap_or_default())
    }

    async fn append(
        &self,
        record: CoreJournalRecord,
    ) -> Result<CoreFileLogWriteTiming, CoreJournalError> {
        let stopped = || CoreJournalError::WriterStopped {
            journal: self.journal_path.clone(),
        };
        let (reply, response) = oneshot::channel();
        self.tx
            .as_ref()
            .ok_or_else(stopped)?
            .send(CoreFileLogWrite { record, reply })
            .map_err(|_closed| stopped())?;
        response.await.map_err(|_dropped| stopped())?
    }
}

impl Drop for CoreFileLogWriter {
    fn drop(&mut self) {
        self.tx.take();
        if let Some(worker) = self.worker.take() {
            stop_core_file_log_writer(worker);
        }
    }
}

fn acquire_journal_lock(journal_path: &Path) -> Result<DiskLock, CoreJournalError> {
    let mut lock_name = journal_path.as_os_str().to_owned();
    lock_name.push(".lock");
    let lock_path = PathBuf::from(lock_name);
    match Disk::try_lock(&lock_path).map_err(|source| CoreJournalError::io(&lock_path, source))? {
        LockAttempt::Acquired(lock) => Ok(lock),
        LockAttempt::Held { owner } => Err(CoreJournalError::Locked {
            journal: journal_path.to_owned(),
            lock: lock_path,
            owner,
        }),
    }
}

/// Production runs the writer on its own thread with a current-thread runtime.
#[cfg(not(madsim))]
fn spawn_core_file_log_writer(
    journal_path: PathBuf,
    rx: mpsc::UnboundedReceiver<CoreFileLogWrite>,
) -> Result<WriterWorker, CoreJournalError> {
    let spawn_error = |source| CoreJournalError::SpawnWriter {
        source: Arc::new(source),
    };
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .map_err(spawn_error)?;
    std::thread::Builder::new()
        .name("ursula-core-file-log-writer".to_owned())
        // The writer is the runtime's only task. An exhausted cooperative
        // budget would make the batching window see an empty channel.
        .spawn(move || {
            runtime.block_on(tokio::task::unconstrained(run_core_file_log_writer(
                journal_path,
                rx,
            )))
        })
        .map_err(spawn_error)
}

/// The simulator runs the writer as a simulated task.
#[cfg(madsim)]
fn spawn_core_file_log_writer(
    journal_path: PathBuf,
    rx: mpsc::UnboundedReceiver<CoreFileLogWrite>,
) -> Result<WriterWorker, CoreJournalError> {
    Ok(crate::rt::spawn(run_core_file_log_writer(journal_path, rx)))
}

/// The channel is closed, so the thread finishes its batch and exits.
#[cfg(not(madsim))]
fn stop_core_file_log_writer(worker: WriterWorker) {
    if let Err(payload) = worker.join() {
        tracing::warn!(
            ?payload,
            "core file log writer thread panicked during shutdown"
        );
    }
}

/// Stopping the task is a process stop: a batch it had not written yet is
/// lost, and no caller waits for it any more.
#[cfg(madsim)]
fn stop_core_file_log_writer(worker: WriterWorker) {
    worker.abort();
}

async fn run_core_file_log_writer(
    journal_path: PathBuf,
    mut rx: mpsc::UnboundedReceiver<CoreFileLogWrite>,
) {
    let mut journal = JournalWriter::new(!Disk::exists(&journal_path));
    while let Some(first) = rx.recv().await {
        let mut batch = vec![first];
        if let Some(next) = recv_within(&mut rx, CORE_LOG_GROUP_COMMIT_DELAY).await {
            batch.push(next);
        }
        while batch.len() < CORE_LOG_GROUP_COMMIT_MAX_BATCH {
            let Ok(next) = rx.try_recv() else {
                break;
            };
            batch.push(next);
        }

        let result = write_core_log_batch(&journal_path, &mut journal, &batch)
            .map_err(|source| CoreJournalError::io(&journal_path, source));
        reply_core_log_batch(batch, result);
    }
}

/// Waits up to `window` for the next request.
///
/// Tokio timers tick in whole milliseconds, which would stretch the batching
/// window. The writer owns its thread, so it parks the thread until a request
/// wakes it or the window ends, as a blocking timed receive would.
#[cfg(not(madsim))]
async fn recv_within(
    rx: &mut mpsc::UnboundedReceiver<CoreFileLogWrite>,
    window: Duration,
) -> Option<CoreFileLogWrite> {
    struct UnparkWriter(std::thread::Thread);

    impl Wake for UnparkWriter {
        fn wake(self: Arc<Self>) {
            self.0.unpark();
        }

        fn wake_by_ref(self: &Arc<Self>) {
            self.0.unpark();
        }
    }

    let deadline = Instant::now().checked_add(window);
    let waker = Waker::from(Arc::new(UnparkWriter(std::thread::current())));
    let mut cx = Context::from_waker(&waker);
    loop {
        if let Poll::Ready(request) = rx.poll_recv(&mut cx) {
            return request;
        }
        let remaining = deadline?.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return None;
        }
        std::thread::park_timeout(remaining);
    }
}

/// Simulated time drives the batching window.
#[cfg(madsim)]
async fn recv_within(
    rx: &mut mpsc::UnboundedReceiver<CoreFileLogWrite>,
    window: Duration,
) -> Option<CoreFileLogWrite> {
    crate::rt::time::timeout(window, rx.recv())
        .await
        .ok()
        .flatten()
}

fn reply_core_log_batch(
    batch: Vec<CoreFileLogWrite>,
    result: Result<CoreFileLogWriteTiming, CoreJournalError>,
) {
    let timing = match result {
        Ok(timing) => timing,
        Err(err) => {
            for request in batch {
                if request.reply.send(Err(err.clone())).is_err() {
                    tracing::trace!("raft log append caller stopped waiting");
                }
            }
            return;
        }
    };
    let count = u64::try_from(batch.len()).unwrap_or(u64::MAX);
    for (request_index, request) in batch.into_iter().enumerate() {
        let owns_batch_sample = request_index == 0;
        let batch_sample = |value: u64| if owns_batch_sample { value } else { 0 };
        let per_request = CoreFileLogWriteTiming {
            write_ns: timing
                .write_ns
                .checked_div(count)
                .unwrap_or(timing.write_ns),
            sync_ns: timing.sync_ns.checked_div(count).unwrap_or(timing.sync_ns),
            fsyncs: u64::from(owns_batch_sample),
            fsync_records: batch_sample(count),
            reclaims: batch_sample(timing.reclaims),
            reclaimed_bytes: batch_sample(timing.reclaimed_bytes),
            reclaim_ns: batch_sample(timing.reclaim_ns),
            physical_bytes: timing.physical_bytes,
        };
        if request.reply.send(Ok(per_request)).is_err() {
            tracing::trace!("raft log append caller stopped waiting");
        }
    }
}

fn write_core_log_batch(
    journal_path: &Path,
    journal: &mut JournalWriter,
    batch: &[CoreFileLogWrite],
) -> Result<CoreFileLogWriteTiming, io::Error> {
    let write_started_at = Instant::now();
    for request in batch {
        write_wire_frame_to_file(journal_path, journal, &request.record)?;
    }
    let write_ns = elapsed_ns(write_started_at);

    let requires_sync = batch
        .iter()
        .any(|request| raft_group_log_record_requires_sync(&request.record.record));
    let sync_ns = if requires_sync {
        let sync_started_at = Instant::now();
        journal.sync(journal_path)?;
        elapsed_ns(sync_started_at)
    } else {
        0
    };
    let reclaim_started_at = Instant::now();
    let mut reclaims = 0;
    let mut reclaimed_bytes = 0;
    if batch.iter().any(|request| {
        matches!(
            &request.record.record,
            RaftGroupLogRecord::Purge(_) | RaftGroupLogRecord::TruncateAfter(_)
        )
    }) && let Some((before, after)) =
        reclaim_core_journal_if_needed(journal_path, journal, CORE_LOG_ONLINE_RECLAIM_MIN_BYTES)?
    {
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
        fsyncs: u64::from(requires_sync).saturating_add(reclaims),
        fsync_records: if requires_sync || reclaims != 0 {
            u64::try_from(batch.len()).unwrap_or(u64::MAX)
        } else {
            0
        },
        reclaims,
        reclaimed_bytes,
        reclaim_ns,
        physical_bytes: Disk::file_len(journal_path)?,
    })
}

impl RaftLogReader<UrsulaRaftTypeConfig> for Arc<RaftGroupFileLogStore> {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<EntryOf<UrsulaRaftTypeConfig>>, io::Error> {
        let entries = self
            .lock_inner()?
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
        let vote = *vote;
        let _order = self.write_order.lock().await;
        if self.lock_inner()?.vote == Some(vote) {
            return Ok(());
        }
        self.append_record(RaftGroupLogRecord::SaveVote(vote))
            .await?;
        self.lock_inner()?.vote = Some(vote);
        Ok(())
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogIdOf<UrsulaRaftTypeConfig>>,
    ) -> Result<(), io::Error> {
        let _order = self.write_order.lock().await;
        if self.lock_inner()?.committed == committed {
            return Ok(());
        }
        self.append_record(RaftGroupLogRecord::SaveCommitted(committed))
            .await?;
        self.lock_inner()?.committed = committed;
        Ok(())
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
        ensure_consecutive_entries::<UrsulaRaftTypeConfig>(&entries)?;
        let _order = self.write_order.lock().await;
        ensure_log_append_boundary::<UrsulaRaftTypeConfig>(&*self.lock_inner()?, &entries)?;

        if let Err(err) = self
            .append_record(RaftGroupLogRecord::Append(entries.clone()))
            .await
        {
            callback.io_completed(Err(err.clone().into()));
            return Err(err.into());
        }
        {
            let mut inner = self.lock_inner()?;
            for entry in entries {
                inner.entries.insert(entry.log_id.index, entry);
            }
        }
        callback.io_completed(Ok(()));
        Ok(())
    }

    async fn truncate_after(
        &mut self,
        last_log_id: Option<LogIdOf<UrsulaRaftTypeConfig>>,
    ) -> Result<(), io::Error> {
        let _order = self.write_order.lock().await;
        self.append_record(RaftGroupLogRecord::TruncateAfter(last_log_id))
            .await?;
        truncate_entries_after(
            &mut self.lock_inner()?.entries,
            last_log_id.map(|log_id| log_id.index),
        );
        Ok(())
    }

    async fn purge(&mut self, log_id: LogIdOf<UrsulaRaftTypeConfig>) -> Result<(), io::Error> {
        let _order = self.write_order.lock().await;
        let last_purged_log_id = self.lock_inner()?.last_purged_log_id;
        if last_purged_log_id > Some(log_id) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "cannot move last purged log id backward from {last_purged_log_id:?} to {log_id:?}"
                ),
            ));
        }

        self.append_record(RaftGroupLogRecord::Purge(log_id))
            .await?;
        let mut inner = self.lock_inner()?;
        inner.last_purged_log_id = Some(log_id);
        inner.entries.retain(|index, _| *index > log_id.index);
        Ok(())
    }
}

fn load_log_store_inners_from_core_journal(
    journal_path: &Path,
) -> Result<BTreeMap<u32, RaftGroupLogStoreInner>, io::Error> {
    load_log_store_inners_from_core_journal_with_stats(journal_path).map(|(inners, _)| inners)
}

fn load_log_store_inners_from_core_journal_with_stats(
    journal_path: &Path,
) -> Result<(BTreeMap<u32, RaftGroupLogStoreInner>, usize), io::Error> {
    let mut inners = BTreeMap::<u32, RaftGroupLogStoreInner>::new();
    let mut record_number = 0_usize;
    journal::replay_each::<WireCodec<CoreJournalRecord>>(journal_path, |record| {
        record_number = record_number.saturating_add(1);
        apply_log_store_record(inners.entry(record.group_id).or_default(), record.record).map_err(
            |err| {
                io::Error::new(
                    err.kind(),
                    format!(
                        "replay OpenRaft core journal record '{}' record {record_number}: {err}",
                        journal_path.display(),
                    ),
                )
            },
        )
    })?;
    Ok((inners, record_number))
}

fn compact_core_journal(
    journal_path: &Path,
    inners: &BTreeMap<u32, RaftGroupLogStoreInner>,
) -> Result<Option<(u64, u64)>, io::Error> {
    if !Disk::exists(journal_path) {
        return Ok(None);
    }
    let before = Disk::file_len(journal_path)?;
    let compact_path = journal_path.with_extension("compact");
    if Disk::exists(&compact_path) {
        Disk::remove_file(&compact_path)?;
    }

    let mut handle = JournalWriter::new(true);
    handle.ensure_created(&compact_path)?;
    for (group_id, inner) in inners {
        let mut write = |record| -> Result<(), io::Error> {
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
    handle.sync(&compact_path)?;
    drop(handle);

    let after = Disk::file_len(&compact_path)?;
    if after >= before {
        Disk::remove_file(&compact_path)?;
        return Ok(None);
    }
    Disk::rename(&compact_path, journal_path)?;
    if let Some(parent) = journal_path.parent() {
        Disk::sync_dir(parent)?;
    }
    Ok(Some((before, after)))
}

fn reclaim_core_journal_if_needed(
    journal_path: &Path,
    journal: &mut JournalWriter,
    min_physical_bytes: u64,
) -> Result<Option<(u64, u64)>, io::Error> {
    if !Disk::exists(journal_path) || Disk::file_len(journal_path)? < min_physical_bytes {
        return Ok(None);
    }

    // Close the append handle before atomically replacing the path. This
    // avoids continuing to append to the unlinked old file after `rename` and
    // keeps the replacement portable to filesystems that reject renaming over
    // an open destination.
    drop(std::mem::replace(
        journal,
        JournalWriter::new(!Disk::exists(journal_path)),
    ));

    let inners = load_log_store_inners_from_core_journal(journal_path)?;
    let compacted = compact_core_journal(journal_path, &inners)?;
    *journal = JournalWriter::new(false);
    Ok(compacted)
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

fn write_wire_frame_to_file<T: Serialize + DeserializeOwned>(
    path: &Path,
    journal: &mut JournalWriter,
    value: &T,
) -> Result<(), io::Error> {
    journal.append::<WireCodec<T>>(path, value)
}

#[cfg(test)]
pub(crate) fn read_wire_frames<T: Serialize + DeserializeOwned>(
    bytes: &[u8],
) -> Result<Vec<T>, io::Error> {
    journal::decode_frames::<WireCodec<T>>(bytes).map(|(records, _)| records)
}

pub(crate) fn raft_group_log_record_count(record: &RaftGroupLogRecord) -> usize {
    match record {
        RaftGroupLogRecord::Append(entries) => entries.len(),
        _ => 1,
    }
}

/// Whether OpenRaft requires this record to reach stable storage before the
/// storage method returns.
///
/// Committed and truncate markers are replay optimizations. Losing either in a
/// crash leaves the durable entries intact and OpenRaft re-establishes the
/// marker after restart. Append and vote durability are consensus safety
/// requirements. Purge remains durable because online reclaim may physically
/// discard the entries it covers.
fn raft_group_log_record_requires_sync(record: &RaftGroupLogRecord) -> bool {
    matches!(
        record,
        RaftGroupLogRecord::SaveVote(_)
            | RaftGroupLogRecord::Append(_)
            | RaftGroupLogRecord::Purge(_)
    )
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
            truncate_entries_after(&mut inner.entries, last_log_id.map(|log_id| log_id.index));
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

/// These tests corrupt and inspect real files, so they run against the
/// operating-system disk.
#[cfg(all(test, not(madsim)))]
mod tests {
    use std::fs;
    use std::fs::OpenOptions;
    use std::io::Write;
    use std::sync::atomic::AtomicU64;
    use std::sync::atomic::Ordering;

    use openraft::EntryPayload;
    use openraft::LogId;
    use openraft::entry::RaftEntry;
    use openraft::vote::RaftLeaderId;
    use openraft::vote::leader_id_adv::CommittedLeaderId;
    use ursula_runtime::GroupWriteCommand;
    use ursula_runtime::RuntimeMetrics;
    use ursula_shard::BucketStreamId;
    use ursula_shard::CoreId;
    use ursula_shard::ShardId;
    use ursula_stream::StreamCommand;

    use super::*;

    static TEMP_JOURNAL_COUNTER: AtomicU64 = AtomicU64::new(0);

    fn temp_journal_path(name: &str) -> PathBuf {
        let nonce = TEMP_JOURNAL_COUNTER.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir()
            .join("ursula-raft-file-log-tests")
            .join(format!("{name}-{}-{nonce}.bin", std::process::id()));
        crate::tests::remove_test_path(&path);
        path
    }

    fn placement(raft_group_id: u32) -> ShardPlacement {
        ShardPlacement {
            core_id: CoreId(0),
            shard_id: ShardId(raft_group_id),
            raft_group_id: RaftGroupId(raft_group_id),
        }
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

    fn payload_entry(index: u64, payload_size: usize) -> EntryOf<UrsulaRaftTypeConfig> {
        EntryOf::<UrsulaRaftTypeConfig>::new(
            test_log_id(index),
            EntryPayload::Normal(GroupWriteCommand::Stream(StreamCommand::Append {
                stream_id: BucketStreamId::new("wal-soak", "production-threshold"),
                content_type: Some("application/octet-stream".to_owned()),
                payload: bytes::Bytes::from(vec![7_u8; payload_size]),
                close_after: false,
                stream_seq: None,
                producer: None,
                now_ms: 0,
            })),
        )
    }

    fn committed_vote() -> VoteOf<UrsulaRaftTypeConfig> {
        openraft::Vote::new_committed(7, 1)
    }

    #[test]
    fn fsync_policy_keeps_only_replay_hints_best_effort() {
        assert!(raft_group_log_record_requires_sync(
            &RaftGroupLogRecord::SaveVote(committed_vote())
        ));
        assert!(raft_group_log_record_requires_sync(
            &RaftGroupLogRecord::Append(vec![blank_entry(1)])
        ));
        assert!(raft_group_log_record_requires_sync(
            &RaftGroupLogRecord::Purge(test_log_id(1))
        ));
        assert!(!raft_group_log_record_requires_sync(
            &RaftGroupLogRecord::SaveCommitted(Some(test_log_id(1)))
        ));
        assert!(!raft_group_log_record_requires_sync(
            &RaftGroupLogRecord::TruncateAfter(Some(test_log_id(1)))
        ));
    }

    #[test]
    fn load_core_journal_truncates_torn_tail() {
        let path = temp_journal_path("core-journal-torn-tail");
        let vote = committed_vote();
        let mut handle = JournalWriter::new(true);
        write_wire_frame_to_file(&path, &mut handle, &CoreJournalRecord {
            group_id: 3,
            record: RaftGroupLogRecord::SaveVote(vote),
        })
        .expect("write complete core journal record");
        handle
            .sync(&path)
            .expect("sync complete core journal record");
        drop(handle);
        let valid_len = fs::metadata(&path).expect("core journal metadata").len();

        append_torn_frame(&path);
        assert!(
            fs::metadata(&path)
                .expect("core journal metadata after torn append")
                .len()
                > valid_len
        );

        let inners = load_log_store_inners_from_core_journal(&path)
            .expect("load core journal with torn tail");
        assert_eq!(inners.get(&3).and_then(|inner| inner.vote), Some(vote));
        assert_eq!(
            fs::metadata(&path)
                .expect("core journal metadata after recovery")
                .len(),
            valid_len
        );

        crate::tests::remove_test_path(&path);
    }

    #[test]
    fn load_core_journal_distributes_all_groups_in_one_scan() {
        let path = temp_journal_path("core-journal-groups");
        let first_vote = openraft::Vote::new_committed(3, 1);
        let second_vote = openraft::Vote::new_committed(5, 2);
        let mut handle = JournalWriter::new(true);
        for (group_id, vote) in [(3, first_vote), (7, second_vote)] {
            write_wire_frame_to_file(&path, &mut handle, &CoreJournalRecord {
                group_id,
                record: RaftGroupLogRecord::SaveVote(vote),
            })
            .expect("write core journal group record");
        }
        handle.sync(&path).expect("sync core journal groups");
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

        crate::tests::remove_test_path(&path);
    }

    #[test]
    fn compact_core_journal_keeps_only_recovered_state() {
        let path = temp_journal_path("core-journal-compact");
        let first_vote = openraft::Vote::new_committed(3, 1);
        let latest_vote = openraft::Vote::new_committed(5, 1);
        let mut handle = JournalWriter::new(true);
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
        handle.sync(&path).expect("sync redundant journal");
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
        crate::tests::remove_test_path(&path);
    }

    #[test]
    fn online_reclaim_reopens_the_replaced_core_journal() {
        let path = temp_journal_path("core-journal-online-reclaim");
        let mut handle = JournalWriter::new(true);
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
        handle.sync(&path).expect("sync historical core journal");
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
        handle.sync(&path).expect("sync append after reclaim");
        drop(handle);

        let recovered =
            load_log_store_inners_from_core_journal(&path).expect("replay reclaimed WAL");
        let group = recovered.get(&7).expect("recovered group");
        assert_eq!(group.last_purged_log_id, Some(test_log_id(255)));
        assert_eq!(group.entries.keys().copied().collect::<Vec<_>>(), [
            256, 257
        ]);
        crate::tests::remove_test_path(&path);
    }

    #[test]
    #[ignore = "writes a production-threshold WAL generation; run through scripts/soak_raft_wal.sh"]
    fn online_reclaim_converges_at_production_threshold() {
        let path = temp_journal_path("core-journal-production-reclaim");
        let mut handle = JournalWriter::new(true);
        write_wire_frame_to_file(&path, &mut handle, &CoreJournalRecord {
            group_id: 7,
            record: RaftGroupLogRecord::Append(vec![payload_entry(
                1,
                usize::try_from(CORE_LOG_ONLINE_RECLAIM_MIN_BYTES)
                    .expect("reclaim threshold fits usize")
                    .saturating_add(1024),
            )]),
        })
        .expect("write production-sized historical record");
        write_wire_frame_to_file(&path, &mut handle, &CoreJournalRecord {
            group_id: 7,
            record: RaftGroupLogRecord::Purge(test_log_id(1)),
        })
        .expect("write production-sized purge frontier");
        handle
            .sync(&path)
            .expect("sync production-sized core journal");
        let before = fs::metadata(&path).expect("journal metadata").len();
        assert!(before >= CORE_LOG_ONLINE_RECLAIM_MIN_BYTES);

        let (reclaim_before, reclaim_after) =
            reclaim_core_journal_if_needed(&path, &mut handle, CORE_LOG_ONLINE_RECLAIM_MIN_BYTES)
                .expect("production-threshold online reclaim")
                .expect("production-sized historical journal should shrink");
        assert_eq!(reclaim_before, before);
        assert!(reclaim_after < 1024 * 1024);
        println!(
            "production-threshold reclaim: before_bytes={reclaim_before} after_bytes={reclaim_after}"
        );

        drop(handle);
        let recovered =
            load_log_store_inners_from_core_journal(&path).expect("replay reclaimed WAL");
        let group = recovered.get(&7).expect("recovered group");
        assert_eq!(group.last_purged_log_id, Some(test_log_id(1)));
        assert!(group.entries.is_empty());
        crate::tests::remove_test_path(&path);
    }

    #[test]
    fn core_file_log_rejects_a_second_owner() {
        let path = temp_journal_path("core-exclusive-lock");
        let first = CoreFileLogWriter::open(path.clone(), None).expect("open first core owner");
        let err =
            CoreFileLogWriter::open(path.clone(), None).expect_err("second core owner must fail");
        assert!(
            matches!(&err, CoreJournalError::Locked { owner: Some(owner), .. } if owner.starts_with("pid=")),
            "unexpected error: {err}"
        );
        let err = io::Error::from(err);
        assert_eq!(err.kind(), io::ErrorKind::AlreadyExists);
        assert!(err.to_string().contains("already locked"));
        drop(first);
        let reopened =
            CoreFileLogWriter::open(path.clone(), None).expect("core lock releases on drop");
        drop(reopened);
        crate::tests::remove_test_path(&path);
        crate::tests::remove_test_path(format!("{}.lock", path.display()));
    }

    #[tokio::test]
    async fn a_group_opens_once_per_core_writer() {
        let path = temp_journal_path("core-group-reopen");
        let metrics = RuntimeMetrics::new(1, 2).group_engine_metrics();
        let writer = CoreFileLogWriter::open(path.clone(), None).expect("open core writer");
        let mut store = RaftGroupFileLogStore::open(placement(1), metrics.clone(), writer.clone())
            .expect("open group store");
        store
            .append([blank_entry(1)], IOFlushed::noop())
            .await
            .expect("append through the core writer");
        drop(store);

        let err = RaftGroupFileLogStore::open(placement(1), metrics.clone(), writer.clone())
            .expect_err("a group must not reopen while its core writer lives");
        assert!(matches!(err, CoreJournalError::GroupAlreadyOpen {
            raft_group_id: RaftGroupId(1),
            ..
        }));
        RaftGroupFileLogStore::open(placement(0), metrics.clone(), writer.clone())
            .expect("another group still opens");
        drop(writer);

        let writer = CoreFileLogWriter::open(path.clone(), None).expect("reopen core writer");
        let mut store = RaftGroupFileLogStore::open(placement(1), metrics, writer)
            .expect("a new writer recovers the group");
        let state = store.get_log_state().await.expect("recovered log state");
        assert_eq!(state.last_log_id, Some(test_log_id(1)));
        drop(store);
        crate::tests::remove_test_path(&path);
        crate::tests::remove_test_path(format!("{}.lock", path.display()));
    }
}

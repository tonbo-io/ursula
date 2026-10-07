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
use ursula_runtime::WalStorageSample;
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
use super::journal::FIRST_SEQUENCE;
use super::journal::JournalError;
use super::journal::JournalOp;
use super::journal::JournalReplayMode;
use super::journal::JournalWriter;
use super::journal::RecordTooLarge;
use super::journal::Replayed;
use super::journal::WRITE_BUFFER_BYTES;
use super::truncate_entries_after;
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
pub enum CoreJournalError {
    #[error("OpenRaft core journal I/O on '{}': {source}", .path.display())]
    Io {
        path: PathBuf,
        #[source]
        source: Arc<io::Error>,
    },
    #[error(transparent)]
    Journal(Arc<JournalError>),
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
    #[error(transparent)]
    RecordTooLarge(#[from] RecordTooLarge),
    #[error(
        "OpenRaft core journal '{}' stopped after an I/O failure; only a restart can re-read \
         what is on disk: {cause}",
        .journal.display()
    )]
    WriterPoisoned {
        journal: PathBuf,
        cause: Arc<JournalError>,
    },
    #[error("OpenRaft core journal writer for '{}' has stopped", .journal.display())]
    WriterStopped { journal: PathBuf },
    #[error("OpenRaft core journal state mutex poisoned")]
    LockPoisoned,
}

impl CoreJournalError {
    fn io(path: &Path, source: io::Error) -> Self {
        Self::Io {
            path: path.to_owned(),
            source: Arc::new(source),
        }
    }
}

impl From<JournalError> for CoreJournalError {
    fn from(err: JournalError) -> Self {
        Self::Journal(Arc::new(err))
    }
}

/// OpenRaft storage methods report `io::Error`; this is the one conversion.
impl From<CoreJournalError> for io::Error {
    fn from(err: CoreJournalError) -> Self {
        let kind = match &err {
            CoreJournalError::Io { source, .. } => source.kind(),
            CoreJournalError::Journal(err) => err.kind(),
            #[cfg(not(madsim))]
            CoreJournalError::SpawnWriter { source } => source.kind(),
            CoreJournalError::Locked { .. } | CoreJournalError::GroupAlreadyOpen { .. } => {
                io::ErrorKind::AlreadyExists
            }
            CoreJournalError::RecordTooLarge(_) => io::ErrorKind::InvalidInput,
            CoreJournalError::WriterPoisoned { cause, .. } => cause.kind(),
            CoreJournalError::WriterStopped { .. } => io::ErrorKind::BrokenPipe,
            CoreJournalError::LockPoisoned => io::ErrorKind::Other,
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

/// What one request's write cost, as reported to its group's metrics.
#[derive(Debug, Clone, Copy)]
pub(crate) struct CoreFileLogWriteTiming {
    write_ns: u64,
    sync_ns: u64,
    storage: WalStorageSample,
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
            .map_err(|_poisoned| CoreJournalError::LockPoisoned)
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
        self.metrics
            .record_wal_storage(self.placement, timing.storage);
        Ok(())
    }
}

impl CoreFileLogWriter {
    /// Opens the journal at `journal_path`: takes its lock, recovers every
    /// group in `replay_mode`, compacts the recovered journal and starts the
    /// writer.
    pub(crate) fn open(
        journal_path: PathBuf,
        replay_mode: JournalReplayMode,
        recovery_metrics: Option<(ShardPlacement, GroupEngineMetrics)>,
    ) -> Result<Arc<Self>, CoreJournalError> {
        if let Some(parent) = journal_path.parent() {
            create_dir_all_durable(parent)
                .map_err(|source| CoreJournalError::io(parent, source))?;
        }
        let lock = acquire_journal_lock(&journal_path)?;
        let recovery_started_at = Instant::now();
        let recovered = recover_core_journal(&journal_path, replay_mode)?;
        let recovery_ns = elapsed_ns(recovery_started_at);
        let replayed = recovered.replayed;
        let recovery_bytes = replayed
            .verified_len
            .saturating_add(replayed.dropped_bytes());
        let recovery_live_entries = recovered.groups.values().fold(0_u64, |total, inner| {
            total.saturating_add(u64::try_from(inner.entries.len()).unwrap_or(u64::MAX))
        });
        if let Some((placement, metrics)) = &recovery_metrics {
            metrics.record_wal_recovery(
                *placement,
                recovery_ns,
                replayed.frames,
                recovery_bytes,
                recovery_live_entries,
            );
        }
        tracing::info!(
            path = %journal_path.display(),
            ?replay_mode,
            recovery_ns,
            recovery_records = replayed.frames,
            recovery_bytes,
            recovery_live_entries,
            "recovered OpenRaft core journal"
        );
        if replayed.dropped_bytes() != 0 {
            tracing::warn!(
                path = %journal_path.display(),
                ?replay_mode,
                tail = ?replayed.tail,
                verified_bytes = replayed.verified_len,
                dropped_bytes = replayed.dropped_bytes(),
                "truncated the OpenRaft core journal after its last verified frame"
            );
        }
        if let Some(generation) = compact_core_journal(&journal_path, &recovered.groups, &replayed)?
        {
            tracing::info!(
                path = %journal_path.display(),
                before_bytes = generation.before,
                after_bytes = generation.after,
                "compacted recovered OpenRaft core journal"
            );
        }
        let mut journal = JournalWriter::open(&journal_path, FIRST_SEQUENCE)?;
        if journal.pending_bytes() != 0 {
            // A new journal: make its header and directory entry durable now.
            journal.sync()?;
        }
        let (tx, rx) = mpsc::unbounded_channel();
        let worker = spawn_core_file_log_writer(journal_path.clone(), journal, rx)?;
        Ok(Arc::new(Self {
            journal_path,
            tx: Some(tx),
            groups: Mutex::new(RecoveredGroups {
                recovered: recovered.groups,
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
            .map_err(|_poisoned| CoreJournalError::LockPoisoned)?;
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

/// Creates `path` and `fsync`s the parent of every directory it creates. The
/// journal's first `fsync` makes the journal's own entry durable, but a new
/// directory's entry needs its parent `fsync`ed too, or a power loss can drop
/// the whole directory with every acknowledged write in it.
fn create_dir_all_durable(path: &Path) -> io::Result<()> {
    let created_parents = path
        .ancestors()
        .take_while(|dir| !dir.as_os_str().is_empty() && !Disk::exists(dir))
        .filter_map(Path::parent)
        .filter(|parent| !parent.as_os_str().is_empty())
        .map(Path::to_path_buf)
        .collect::<Vec<_>>();
    Disk::create_dir_all(path)?;
    for parent in created_parents.iter().rev() {
        Disk::sync_dir(parent)?;
    }
    Ok(())
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
    journal: JournalWriter,
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
                journal,
                rx,
            )))
        })
        .map_err(spawn_error)
}

/// The simulator runs the writer as a simulated task.
#[cfg(madsim)]
fn spawn_core_file_log_writer(
    journal_path: PathBuf,
    journal: JournalWriter,
    rx: mpsc::UnboundedReceiver<CoreFileLogWrite>,
) -> Result<WriterWorker, CoreJournalError> {
    Ok(crate::rt::spawn(run_core_file_log_writer(
        journal_path,
        journal,
        rx,
    )))
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

/// The state of a core journal's writer.
enum WriterState {
    /// Appending to the journal.
    Open(JournalWriter),
    /// An I/O failure left the journal in doubt. After a failed write the
    /// file may end in a partial frame, and a failed `fsync` may have dropped
    /// dirty pages that a later `fsync` would report as durable. The writer
    /// never touches the file again: every request fails with the cause.
    Poisoned(Arc<JournalError>),
}

async fn run_core_file_log_writer(
    journal_path: PathBuf,
    journal: JournalWriter,
    mut rx: mpsc::UnboundedReceiver<CoreFileLogWrite>,
) {
    let mut state = WriterState::Open(journal);
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

        state = match state {
            WriterState::Open(journal) => write_core_log_batch(&journal_path, journal, batch),
            WriterState::Poisoned(cause) => {
                refuse_core_log_batch(&journal_path, &cause, batch);
                WriterState::Poisoned(cause)
            }
        };
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

/// Writes one batch: encodes every request, writes the frames, `fsync`s them
/// when a request needs it, reclaims the journal online after a purge or
/// truncate, and replies. Returns the writer's next state.
fn write_core_log_batch(
    journal_path: &Path,
    mut journal: JournalWriter,
    batch: Vec<CoreFileLogWrite>,
) -> WriterState {
    let write_started_at = Instant::now();
    let mut accepted = Vec::with_capacity(batch.len());
    let mut flushed = Ok(());
    for request in batch {
        if flushed.is_ok() {
            if let Err(too_large) = journal.append::<WireCodec<CoreJournalRecord>>(&request.record)
            {
                request.reply(Err(CoreJournalError::RecordTooLarge(too_large)));
                continue;
            }
            if journal.pending_bytes() >= WRITE_BUFFER_BYTES {
                flushed = journal.flush();
            }
        }
        accepted.push(request);
    }
    if accepted.is_empty() {
        return WriterState::Open(journal);
    }
    let requires_sync = accepted
        .iter()
        .any(|request| raft_group_log_record_requires_sync(&request.record.record));
    let written = flushed.and_then(|()| journal.flush()).and_then(|()| {
        let write_ns = elapsed_ns(write_started_at);
        if !requires_sync {
            return Ok((write_ns, 0, 0));
        }
        let sync_started_at = Instant::now();
        let fsyncs = journal.sync()?;
        Ok((write_ns, elapsed_ns(sync_started_at), fsyncs))
    });
    let (write_ns, sync_ns, fsyncs) = match written {
        Ok(written) => written,
        Err(cause) => {
            drop(journal);
            return poison_core_journal(journal_path, cause, accepted);
        }
    };

    let mut storage = WalStorageSample {
        fsyncs,
        fsync_records: if requires_sync {
            u64::try_from(accepted.len()).unwrap_or(u64::MAX)
        } else {
            0
        },
        physical_bytes: journal.len(),
        ..WalStorageSample::default()
    };
    let reclaim_due = journal.len() >= CORE_LOG_ONLINE_RECLAIM_MIN_BYTES
        && accepted.iter().any(|request| {
            matches!(
                &request.record.record,
                RaftGroupLogRecord::Purge(_) | RaftGroupLogRecord::TruncateAfter(_)
            )
        });
    let state = if reclaim_due {
        reclaim_after_batch(journal_path, journal, &mut storage)
    } else {
        WriterState::Open(journal)
    };

    // The batch is durable whatever the reclaim did.
    reply_core_log_batch(accepted, write_ns, sync_ns, storage);
    if let WriterState::Poisoned(cause) = &state {
        on_journal_poisoned(journal_path, cause);
    }
    state
}

/// Rewrites the journal online and records the outcome in `storage`. A
/// failure that leaves the live journal as it was is logged and counted; one
/// that leaves it in doubt poisons the writer.
fn reclaim_after_batch(
    journal_path: &Path,
    journal: JournalWriter,
    storage: &mut WalStorageSample,
) -> WriterState {
    let started_at = Instant::now();
    match reclaim_core_journal(journal_path, journal) {
        Reclaim::Done {
            journal,
            generation,
        } => {
            if let Some(generation) = generation {
                let reclaimed_bytes = generation.before.saturating_sub(generation.after);
                storage.reclaims = 1;
                storage.reclaimed_bytes = reclaimed_bytes;
                storage.reclaim_ns = elapsed_ns(started_at);
                storage.fsyncs = storage.fsyncs.saturating_add(generation.fsyncs);
                storage.physical_bytes = journal.len();
                tracing::info!(
                    path = %journal_path.display(),
                    before_bytes = generation.before,
                    after_bytes = generation.after,
                    reclaimed_bytes,
                    "reclaimed obsolete OpenRaft core WAL records online"
                );
            }
            WriterState::Open(journal)
        }
        Reclaim::Abandoned { journal, error } => {
            storage.reclaim_failures = 1;
            tracing::error!(
                path = %journal_path.display(),
                %error,
                "online reclaim of the OpenRaft core journal failed; the journal is unchanged"
            );
            WriterState::Open(journal)
        }
        Reclaim::Poisoned(cause) => {
            storage.reclaim_failures = 1;
            WriterState::Poisoned(Arc::new(cause))
        }
    }
}

/// Fails `batch` with `cause` and poisons the writer.
fn poison_core_journal(
    journal_path: &Path,
    cause: JournalError,
    batch: Vec<CoreFileLogWrite>,
) -> WriterState {
    let cause = Arc::new(cause);
    refuse_core_log_batch(journal_path, &cause, batch);
    on_journal_poisoned(journal_path, &cause);
    WriterState::Poisoned(cause)
}

/// Fails every request of `batch` because the writer is poisoned by `cause`.
fn refuse_core_log_batch(
    journal_path: &Path,
    cause: &Arc<JournalError>,
    batch: Vec<CoreFileLogWrite>,
) {
    for request in batch {
        request.reply(Err(CoreJournalError::WriterPoisoned {
            journal: journal_path.to_owned(),
            cause: cause.clone(),
        }));
    }
}

/// Runs once, when an I/O failure poisons the writer. This is the one place a
/// journal failure reaches beyond the requests it fails: only a restart can
/// re-read what is really on disk, so production stops the process here.
/// Recording the failure in the node's run state belongs here as well.
fn on_journal_poisoned(journal_path: &Path, cause: &JournalError) {
    tracing::error!(
        path = %journal_path.display(),
        error = %cause,
        "OpenRaft core journal failed; the writer is poisoned and the process stops"
    );
    stop_process_after_journal_failure();
}

/// Aborts: unwinding or a graceful shutdown could write more.
#[cfg(not(any(test, madsim)))]
fn stop_process_after_journal_failure() {
    std::process::abort();
}

/// Unit tests and the simulator keep the process and the poisoned writer, so
/// they can observe it and restart the node themselves.
#[cfg(any(test, madsim))]
fn stop_process_after_journal_failure() {}

/// Replies to every request of a durable batch. The first request carries the
/// batch-wide counters, so each is counted once.
fn reply_core_log_batch(
    batch: Vec<CoreFileLogWrite>,
    write_ns: u64,
    sync_ns: u64,
    storage: WalStorageSample,
) {
    let count = u64::try_from(batch.len()).unwrap_or(u64::MAX);
    for (request_index, request) in batch.into_iter().enumerate() {
        let storage = if request_index == 0 {
            storage
        } else {
            WalStorageSample {
                physical_bytes: storage.physical_bytes,
                ..WalStorageSample::default()
            }
        };
        request.reply(Ok(CoreFileLogWriteTiming {
            write_ns: write_ns.checked_div(count).unwrap_or(write_ns),
            sync_ns: sync_ns.checked_div(count).unwrap_or(sync_ns),
            storage,
        }));
    }
}

impl CoreFileLogWrite {
    fn reply(self, result: Result<CoreFileLogWriteTiming, CoreJournalError>) {
        if self.reply.send(result).is_err() {
            tracing::trace!("raft log append caller stopped waiting");
        }
    }
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

/// Every group's state recovered from a core journal.
#[derive(Debug)]
struct RecoveredJournal {
    groups: BTreeMap<u32, RaftGroupLogStoreInner>,
    replayed: Replayed,
}

/// Replays `journal_path` in `mode` and truncates what follows its verified
/// frames.
fn recover_core_journal(
    journal_path: &Path,
    mode: JournalReplayMode,
) -> Result<RecoveredJournal, JournalError> {
    let mut groups = BTreeMap::<u32, RaftGroupLogStoreInner>::new();
    let replayed =
        journal::recover::<WireCodec<CoreJournalRecord>>(journal_path, mode, |record| {
            apply_log_store_record(groups.entry(record.group_id).or_default(), record.record)
        })?;
    Ok(RecoveredJournal { groups, replayed })
}

/// Reads the journal a running writer appends to, which holds exactly the
/// `written` bytes the writer wrote. Anything else means the file is not what
/// the writer wrote; nothing is truncated.
fn read_live_core_journal(
    journal_path: &Path,
    written: u64,
) -> Result<(RecoveredJournal, u64), JournalError> {
    let mut groups = BTreeMap::<u32, RaftGroupLogStoreInner>::new();
    let replayed = journal::replay::<WireCodec<CoreJournalRecord>>(
        journal_path,
        JournalReplayMode::Strict,
        |record| apply_log_store_record(groups.entry(record.group_id).or_default(), record.record),
    )?;
    replayed.require_written(journal_path, written)?;
    let sequence = replayed
        .sequence
        .ok_or_else(|| JournalError::NotAsWritten {
            path: journal_path.to_owned(),
            written,
            verified: replayed.verified_len,
            unverified: 0,
        })?;
    Ok((RecoveredJournal { groups, replayed }, sequence))
}

/// The next generation of a core journal, written and synced next to it.
#[derive(Debug)]
struct NextGeneration {
    path: PathBuf,
    len: u64,
    fsyncs: u64,
}

/// A generation that replaced the journal.
#[derive(Debug, Clone, Copy)]
struct InstalledGeneration {
    before: u64,
    after: u64,
    fsyncs: u64,
}

/// Writes generation `live_sequence + 1` of `journal_path`, holding only
/// `groups`, to a temporary file and syncs it. The journal is untouched.
fn write_next_generation(
    journal_path: &Path,
    groups: &BTreeMap<u32, RaftGroupLogStoreInner>,
    live_sequence: u64,
) -> Result<NextGeneration, JournalError> {
    let path = journal_path.with_extension("compact");
    if Disk::exists(&path) {
        Disk::remove_file(&path)
            .map_err(|source| JournalError::io(&path, JournalOp::Remove, source))?;
    }
    let mut handle = JournalWriter::open(&path, live_sequence.wrapping_add(1))?;
    for (group_id, inner) in groups {
        let mut write = |record| -> Result<(), JournalError> {
            handle.append::<WireCodec<CoreJournalRecord>>(&CoreJournalRecord {
                group_id: *group_id,
                record,
            })?;
            if handle.pending_bytes() >= WRITE_BUFFER_BYTES {
                handle.flush()?;
            }
            Ok(())
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
    // The rename and the directory `fsync` in `install_generation` publish
    // the file, so its own directory entry needs no `fsync` first.
    let fsyncs = handle.sync_data()?;
    Ok(NextGeneration {
        len: handle.len(),
        path,
        fsyncs,
    })
}

/// Removes a generation that would not shrink the journal.
fn discard_generation(generation: &NextGeneration) -> Result<(), JournalError> {
    Disk::remove_file(&generation.path)
        .map_err(|source| JournalError::io(&generation.path, JournalOp::Remove, source))
}

/// Replaces the journal with `generation` and `fsync`s the directory, so the
/// replacement survives a crash.
fn install_generation(
    journal_path: &Path,
    generation: &NextGeneration,
) -> Result<u64, JournalError> {
    Disk::rename(&generation.path, journal_path)
        .map_err(|source| JournalError::io(journal_path, JournalOp::Rename, source))?;
    let Some(parent) = journal_path.parent() else {
        return Ok(0);
    };
    Disk::sync_dir(parent)
        .map_err(|source| JournalError::io(journal_path, JournalOp::SyncDir, source))?;
    Ok(1)
}

/// Rewrites a recovered journal as its next generation when that is smaller.
fn compact_core_journal(
    journal_path: &Path,
    groups: &BTreeMap<u32, RaftGroupLogStoreInner>,
    replayed: &Replayed,
) -> Result<Option<InstalledGeneration>, JournalError> {
    let Some(live_sequence) = replayed.sequence else {
        return Ok(None);
    };
    let before = replayed.verified_len;
    let generation = write_next_generation(journal_path, groups, live_sequence)?;
    if generation.len >= before {
        discard_generation(&generation)?;
        return Ok(None);
    }
    let fsyncs = install_generation(journal_path, &generation)?;
    Ok(Some(InstalledGeneration {
        before,
        after: generation.len,
        fsyncs: generation.fsyncs.saturating_add(fsyncs),
    }))
}

/// What an online reclaim left behind.
#[derive(Debug)]
enum Reclaim {
    /// Appends continue on `journal`, which is the next generation when one
    /// replaced the old journal.
    Done {
        journal: JournalWriter,
        generation: Option<InstalledGeneration>,
    },
    /// The rewrite failed before it touched the journal, which is unchanged.
    Abandoned {
        journal: JournalWriter,
        error: JournalError,
    },
    /// The journal on disk is not what the writer wrote, or may not be the
    /// generation the writer would append to: it can no longer be trusted.
    Poisoned(JournalError),
}

/// Rewrites the journal online as its next generation, holding only every
/// group's live state.
fn reclaim_core_journal(journal_path: &Path, journal: JournalWriter) -> Reclaim {
    let before = journal.len();
    let (live, live_sequence) = match read_live_core_journal(journal_path, before) {
        Ok(live) => live,
        Err(error) if error.is_io() => return Reclaim::Abandoned { journal, error },
        Err(error) => return Reclaim::Poisoned(error),
    };
    let generation = match write_next_generation(journal_path, &live.groups, live_sequence) {
        Ok(generation) => generation,
        Err(error) => return Reclaim::Abandoned { journal, error },
    };
    if generation.len >= before {
        return match discard_generation(&generation) {
            Ok(()) => Reclaim::Done {
                journal,
                generation: None,
            },
            Err(error) => Reclaim::Abandoned { journal, error },
        };
    }

    // Close the append handle before atomically replacing the path. This
    // avoids continuing to append to the unlinked old file after `rename` and
    // keeps the replacement portable to filesystems that reject renaming over
    // an open destination.
    drop(journal);
    let installed = install_generation(journal_path, &generation)
        .and_then(|fsyncs| Ok((JournalWriter::open(journal_path, FIRST_SEQUENCE)?, fsyncs)));
    match installed {
        Ok((journal, fsyncs)) => Reclaim::Done {
            journal,
            generation: Some(InstalledGeneration {
                before,
                after: generation.len,
                fsyncs: generation.fsyncs.saturating_add(fsyncs),
            }),
        },
        Err(error) => Reclaim::Poisoned(error),
    }
}

/// Frames Raft log records as length-delimited MessagePack for the shared
/// journal (see [`crate::codec::encode_wire`]).
struct WireCodec<T>(PhantomData<T>);

impl<T: Serialize + DeserializeOwned> journal::FrameCodec for WireCodec<T> {
    type Record = T;

    fn encode_into(record: &T, out: &mut Vec<u8>) {
        rmp_serde::encode::write_named(out, record).expect("wire value serializes to MessagePack");
    }

    fn decode(payload: &[u8]) -> Result<T, io::Error> {
        rmp_serde::from_slice(payload).map_err(invalid_data)
    }
}

#[cfg(test)]
pub(crate) fn read_wire_frames<T: Serialize + DeserializeOwned>(
    bytes: &[u8],
) -> Result<Vec<T>, JournalError> {
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
    use std::io::Seek;
    use std::io::SeekFrom;
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

    use super::CORE_LOG_ONLINE_RECLAIM_MIN_BYTES;
    use super::CoreFileLogWriter;
    use super::CoreJournalError;
    use super::CoreJournalRecord;
    use super::EntryOf;
    use super::FIRST_SEQUENCE;
    use super::IOFlushed;
    use super::JournalError;
    use super::JournalReplayMode;
    use super::JournalWriter;
    use super::LogIdOf;
    use super::Path;
    use super::PathBuf;
    use super::RaftGroupFileLogStore;
    use super::RaftGroupId;
    use super::RaftGroupLogRecord;
    use super::RaftLogStorage;
    use super::Reclaim;
    use super::ShardPlacement;
    use super::UrsulaRaftTypeConfig;
    use super::VoteOf;
    use super::WalStorageSample;
    use super::WireCodec;
    use super::WriterState;
    use super::compact_core_journal;
    use super::io;
    use super::journal::ReplayTail;
    use super::raft_group_log_record_requires_sync;
    use super::reclaim_after_batch;
    use super::reclaim_core_journal;
    use super::recover_core_journal;

    static TEMP_JOURNAL_COUNTER: AtomicU64 = AtomicU64::new(0);

    fn temp_journal_path(name: &str) -> PathBuf {
        let nonce = TEMP_JOURNAL_COUNTER.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join("ursula-raft-file-log-tests");
        fs::create_dir_all(&dir).expect("create the test journal directory");
        let path = dir.join(format!("{name}-{}-{nonce}.bin", std::process::id()));
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

    fn record(group_id: u32, record: RaftGroupLogRecord) -> CoreJournalRecord {
        CoreJournalRecord { group_id, record }
    }

    /// Appends `records` to the journal at `path` in one synced batch.
    fn write_records(path: &Path, records: impl IntoIterator<Item = CoreJournalRecord>) {
        let mut writer = JournalWriter::open(path, FIRST_SEQUENCE).expect("open journal");
        for record in records {
            writer
                .append::<WireCodec<CoreJournalRecord>>(&record)
                .expect("append core journal record");
        }
        writer.sync().expect("sync core journal");
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

    fn file_len(path: &Path) -> u64 {
        fs::metadata(path).expect("core journal metadata").len()
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

    fn strict(path: &Path) -> super::RecoveredJournal {
        recover_core_journal(path, JournalReplayMode::Strict).expect("recover core journal")
    }

    fn remove_journal(path: &Path) {
        crate::tests::remove_test_path(path);
        crate::tests::remove_test_path(format!("{}.lock", path.display()));
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
        write_records(&path, [record(3, RaftGroupLogRecord::SaveVote(vote))]);
        let valid_len = file_len(&path);

        append_torn_frame(&path);
        assert!(file_len(&path) > valid_len);

        let recovered = strict(&path);
        assert_eq!(
            recovered.groups.get(&3).and_then(|inner| inner.vote),
            Some(vote)
        );
        assert_eq!(recovered.replayed.tail, ReplayTail::Incomplete { bytes: 8 });
        assert_eq!(file_len(&path), valid_len);
        crate::tests::remove_test_path(&path);
    }

    #[test]
    fn load_core_journal_distributes_all_groups_in_one_scan() {
        let path = temp_journal_path("core-journal-groups");
        let first_vote = openraft::Vote::new_committed(3, 1);
        let second_vote = openraft::Vote::new_committed(5, 2);
        write_records(&path, [
            record(3, RaftGroupLogRecord::SaveVote(first_vote)),
            record(7, RaftGroupLogRecord::SaveVote(second_vote)),
        ]);

        let recovered = strict(&path);
        assert_eq!(recovered.groups.len(), 2);
        assert_eq!(
            recovered.groups.get(&3).and_then(|inner| inner.vote),
            Some(first_vote)
        );
        assert_eq!(
            recovered.groups.get(&7).and_then(|inner| inner.vote),
            Some(second_vote)
        );
        crate::tests::remove_test_path(&path);
    }

    #[test]
    fn compact_core_journal_keeps_only_recovered_state() {
        let path = temp_journal_path("core-journal-compact");
        let first_vote = openraft::Vote::new_committed(3, 1);
        let latest_vote = openraft::Vote::new_committed(5, 1);
        write_records(
            &path,
            std::iter::repeat_n(first_vote, 100)
                .chain([latest_vote])
                .map(|vote| record(7, RaftGroupLogRecord::SaveVote(vote)))
                .chain([
                    record(
                        7,
                        RaftGroupLogRecord::Append((1..=3).map(blank_entry).collect()),
                    ),
                    record(7, RaftGroupLogRecord::Purge(test_log_id(2))),
                    record(7, RaftGroupLogRecord::SaveCommitted(Some(test_log_id(3)))),
                ]),
        );
        let before = file_len(&path);
        let recovered = strict(&path);
        assert_eq!(recovered.replayed.sequence, Some(FIRST_SEQUENCE));

        let compacted = compact_core_journal(&path, &recovered.groups, &recovered.replayed)
            .expect("compact journal")
            .expect("redundant journal should shrink");

        assert_eq!(compacted.before, before);
        assert!(compacted.after < compacted.before);
        assert_eq!(compacted.after, file_len(&path));
        assert_eq!(
            compacted.fsyncs, 2,
            "the new generation's data and the rename"
        );
        let recovered = strict(&path);
        assert_eq!(
            recovered.replayed.sequence,
            Some(FIRST_SEQUENCE + 1),
            "a rewrite is the next generation"
        );
        let group = recovered.groups.get(&7).expect("recovered group");
        assert_eq!(group.vote, Some(latest_vote));
        assert_eq!(group.last_purged_log_id, Some(test_log_id(2)));
        assert_eq!(group.committed, Some(test_log_id(3)));
        assert_eq!(group.entries.keys().copied().collect::<Vec<_>>(), [3]);
        crate::tests::remove_test_path(&path);
    }

    #[test]
    fn online_reclaim_reopens_the_replaced_core_journal() {
        let path = temp_journal_path("core-journal-online-reclaim");
        write_records(
            &path,
            (1..=256)
                .map(|index| record(7, RaftGroupLogRecord::Append(vec![blank_entry(index)])))
                .chain([record(7, RaftGroupLogRecord::Purge(test_log_id(255)))]),
        );
        let before = file_len(&path);
        let journal = JournalWriter::open(&path, FIRST_SEQUENCE).expect("open journal");

        let Reclaim::Done {
            journal: mut writer,
            generation: Some(generation),
        } = reclaim_core_journal(&path, journal)
        else {
            panic!("the historical journal shrinks");
        };
        assert_eq!(generation.before, before);
        assert!(generation.after < generation.before);
        assert_eq!(writer.len(), generation.after);

        writer
            .append::<WireCodec<CoreJournalRecord>>(&record(
                7,
                RaftGroupLogRecord::Append(vec![blank_entry(257)]),
            ))
            .expect("append after atomic replacement");
        writer.sync().expect("sync append after reclaim");
        drop(writer);

        let recovered = strict(&path);
        assert_eq!(recovered.replayed.sequence, Some(FIRST_SEQUENCE + 1));
        let group = recovered.groups.get(&7).expect("recovered group");
        assert_eq!(group.last_purged_log_id, Some(test_log_id(255)));
        assert_eq!(group.entries.keys().copied().collect::<Vec<_>>(), [
            256, 257
        ]);
        crate::tests::remove_test_path(&path);
    }

    /// Online reclaim reads the journal its writer appends to. A frame that
    /// fails verification there poisons the writer and is never truncated
    /// away.
    #[test]
    fn online_reclaim_never_truncates_a_live_journal_that_fails_verification() {
        let path = temp_journal_path("core-journal-reclaim-corrupt");
        write_records(
            &path,
            (1..=4).map(|index| record(7, RaftGroupLogRecord::Append(vec![blank_entry(index)]))),
        );
        let mut file = OpenOptions::new()
            .write(true)
            .open(&path)
            .expect("open journal");
        file.seek(SeekFrom::Start(48)).expect("seek into frame 1");
        file.write_all(b"corrupt").expect("corrupt frame 1");
        file.sync_data().expect("sync corruption");
        let len = file_len(&path);
        let journal = JournalWriter::open(&path, FIRST_SEQUENCE).expect("open journal");

        let mut storage = WalStorageSample::default();
        let WriterState::Poisoned(cause) = reclaim_after_batch(&path, journal, &mut storage) else {
            panic!("a corrupt live journal poisons the writer");
        };
        assert!(
            matches!(*cause, JournalError::CorruptFrame { frame: 1, .. }),
            "unexpected error: {cause}"
        );
        assert_eq!(storage.reclaim_failures, 1);
        assert_eq!(file_len(&path), len, "nothing is truncated");
        crate::tests::remove_test_path(&path);
    }

    /// A rewrite that fails before it touches the journal leaves it as it
    /// was: the failure is counted and appends continue.
    #[test]
    fn a_reclaim_that_fails_before_the_rename_keeps_the_writer_open() {
        let path = temp_journal_path("core-journal-reclaim-abandoned");
        write_records(
            &path,
            (1..=8)
                .map(|index| record(7, RaftGroupLogRecord::Append(vec![blank_entry(index)])))
                .chain([record(7, RaftGroupLogRecord::Purge(test_log_id(7)))]),
        );
        let len = file_len(&path);
        // The next generation cannot be written where a directory stands.
        let blocker = path.with_extension("compact");
        fs::create_dir(&blocker).expect("block the next generation");
        let journal = JournalWriter::open(&path, FIRST_SEQUENCE).expect("open journal");

        let mut storage = WalStorageSample::default();
        let WriterState::Open(mut journal) = reclaim_after_batch(&path, journal, &mut storage)
        else {
            panic!("a failed rewrite leaves the journal trustworthy");
        };
        assert_eq!(storage.reclaim_failures, 1);
        assert_eq!(storage.reclaims, 0);
        assert_eq!(file_len(&path), len, "the journal is unchanged");

        journal
            .append::<WireCodec<CoreJournalRecord>>(&record(
                7,
                RaftGroupLogRecord::Append(vec![blank_entry(9)]),
            ))
            .expect("append after the failed rewrite");
        journal.sync().expect("sync after the failed rewrite");
        drop(journal);
        let mut recovered = strict(&path);
        let group = recovered.groups.remove(&7).expect("recovered group");
        assert_eq!(group.entries.keys().copied().collect::<Vec<_>>(), [8, 9]);
        fs::remove_dir(&blocker).expect("remove the blocker");
        crate::tests::remove_test_path(&path);
    }

    #[test]
    #[ignore = "writes a production-threshold WAL generation; run through scripts/soak_raft_wal.sh"]
    fn online_reclaim_converges_at_production_threshold() {
        let path = temp_journal_path("core-journal-production-reclaim");
        write_records(&path, [
            record(
                7,
                RaftGroupLogRecord::Append(vec![payload_entry(
                    1,
                    usize::try_from(CORE_LOG_ONLINE_RECLAIM_MIN_BYTES)
                        .expect("reclaim threshold fits usize")
                        .saturating_add(1024),
                )]),
            ),
            record(7, RaftGroupLogRecord::Purge(test_log_id(1))),
        ]);
        let before = file_len(&path);
        assert!(before >= CORE_LOG_ONLINE_RECLAIM_MIN_BYTES);
        let journal = JournalWriter::open(&path, FIRST_SEQUENCE).expect("open journal");

        let Reclaim::Done {
            journal,
            generation: Some(generation),
        } = reclaim_core_journal(&path, journal)
        else {
            panic!("the production-sized historical journal shrinks");
        };
        assert_eq!(generation.before, before);
        assert!(generation.after < 1024 * 1024);
        println!(
            "production-threshold reclaim: before_bytes={} after_bytes={}",
            generation.before, generation.after
        );

        drop(journal);
        let recovered = strict(&path);
        let group = recovered.groups.get(&7).expect("recovered group");
        assert_eq!(group.last_purged_log_id, Some(test_log_id(1)));
        assert!(group.entries.is_empty());
        crate::tests::remove_test_path(&path);
    }

    /// Strict recovery refuses a hole before intact frames; the verified
    /// prefix keeps the frames before it.
    #[test]
    fn replay_modes_choose_between_failing_closed_and_the_verified_prefix() {
        let path = temp_journal_path("core-journal-replay-modes");
        let vote = committed_vote();
        write_records(&path, [
            record(3, RaftGroupLogRecord::SaveVote(vote)),
            record(
                3,
                RaftGroupLogRecord::Append((1..=2).map(blank_entry).collect()),
            ),
            record(
                3,
                RaftGroupLogRecord::Append((3..=4).map(blank_entry).collect()),
            ),
        ]);
        let prefix = strict(&path).replayed;
        assert_eq!(prefix.frames, 3);
        write_records(&path, [record(
            3,
            RaftGroupLogRecord::Append((5..=6).map(blank_entry).collect()),
        )]);
        let mut file = OpenOptions::new()
            .write(true)
            .open(&path)
            .expect("open journal");
        file.seek(SeekFrom::Start(prefix.verified_len))
            .expect("seek to frame 4");
        file.write_all(&[0; 16]).expect("zero frame 4");
        file.sync_data().expect("sync hole");
        let len = file_len(&path);

        let err = CoreFileLogWriter::open(path.clone(), JournalReplayMode::Strict, None)
            .expect_err("strict recovery fails closed");
        assert!(
            matches!(&err, CoreJournalError::Journal(err) if matches!(**err, JournalError::CorruptFrame { frame: 4, offset, .. } if offset == prefix.verified_len)),
            "unexpected error: {err}"
        );
        assert_eq!(file_len(&path), len, "strict recovery truncates nothing");

        let writer = CoreFileLogWriter::open(path.clone(), JournalReplayMode::VerifiedPrefix, None)
            .expect("verified-prefix recovery");
        let inner = writer
            .take_recovered(RaftGroupId(3))
            .expect("recovered group");
        assert_eq!(inner.vote, Some(vote));
        assert_eq!(inner.entries.keys().copied().collect::<Vec<_>>(), [
            1, 2, 3, 4
        ]);
        drop(writer);
        remove_journal(&path);
    }

    #[test]
    fn core_file_log_rejects_a_second_owner() {
        let path = temp_journal_path("core-exclusive-lock");
        let first = CoreFileLogWriter::open(path.clone(), JournalReplayMode::Strict, None)
            .expect("open first core owner");
        let err = CoreFileLogWriter::open(path.clone(), JournalReplayMode::Strict, None)
            .expect_err("second core owner must fail");
        assert!(
            matches!(&err, CoreJournalError::Locked { owner: Some(owner), .. } if owner.starts_with("pid=")),
            "unexpected error: {err}"
        );
        let err = io::Error::from(err);
        assert_eq!(err.kind(), io::ErrorKind::AlreadyExists);
        drop(first);
        let reopened = CoreFileLogWriter::open(path.clone(), JournalReplayMode::Strict, None)
            .expect("core lock releases on drop");
        drop(reopened);
        remove_journal(&path);
    }

    /// `wal_fsyncs` counts `fsync` calls, not batches: a batch of replay
    /// hints is written without one.
    #[tokio::test]
    async fn wal_fsyncs_count_fsyncs_not_batches() {
        let path = temp_journal_path("core-fsync-metrics");
        let metrics = RuntimeMetrics::new(1, 2);
        let writer = CoreFileLogWriter::open(path.clone(), JournalReplayMode::Strict, None)
            .expect("open core writer");
        let mut store =
            RaftGroupFileLogStore::open(placement(1), metrics.group_engine_metrics(), writer)
                .expect("open group store");
        store
            .append([blank_entry(1)], IOFlushed::noop())
            .await
            .expect("append an entry");
        store
            .save_committed(Some(test_log_id(1)))
            .await
            .expect("journal a committed marker");

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.wal_batches, 2);
        assert_eq!(snapshot.wal_fsyncs, 1);
        assert_eq!(snapshot.wal_fsync_records, 1);
        assert_eq!(snapshot.wal_physical_bytes, file_len(&path));
        drop(store);
        remove_journal(&path);
    }

    #[tokio::test]
    async fn a_group_opens_once_per_core_writer() {
        let path = temp_journal_path("core-group-reopen");
        let metrics = RuntimeMetrics::new(1, 2).group_engine_metrics();
        let open = || CoreFileLogWriter::open(path.clone(), JournalReplayMode::Strict, None);
        let writer = open().expect("open core writer");
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

        let writer = open().expect("reopen core writer");
        let mut store = RaftGroupFileLogStore::open(placement(1), metrics, writer)
            .expect("a new writer recovers the group");
        let state = store.get_log_state().await.expect("recovered log state");
        assert_eq!(state.last_log_id, Some(test_log_id(1)));
        drop(store);
        remove_journal(&path);
    }
}

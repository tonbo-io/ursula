//! The durable OpenRaft log store: every group's records go to its core's
//! shared journal through one writer per core.
//!
//! The writer is an async loop over the runtime shim's channel. Production
//! runs it on a dedicated OS thread with a current-thread runtime, so blocking
//! file I/O stays off the async workers; `cfg(madsim)` runs it as a simulated
//! task over the simulated disk. Callers await a reply that arrives after the
//! batch is written and, when the fsync policy needs it, `fsync`ed.
//!
//! Each group's vote and `initialized` flag live in the core's metadata file
//! (`core_meta`), which the writer replaces with an `fsync` under either
//! policy. Committed, truncate and purge markers and the entries stay in the
//! journal.
//!
//! The fsync policy shapes each batch:
//!
//! - `always`: a group commit. The writer keeps collecting while requests
//!   keep arriving, each within [`CORE_LOG_GROUP_COMMIT_DELAY`] of the last,
//!   for at most [`CORE_LOG_GROUP_COMMIT_MAX_DELAY`] after the first, and
//!   acknowledges after the `fsync`.
//! - `never`: the batch is what is queued when the writer wakes, and it is
//!   acknowledged once written to the page cache.
//!
//! When the last handle to a writer goes, or on [`CoreFileLogWriter::close`],
//! the writer `fsync`s its journal and stops.

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
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
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
use ursula_config::WalFsync;
use ursula_runtime::GroupEngineMetrics;
use ursula_runtime::WalStorageSample;
use ursula_shard::RaftGroupId;
use ursula_shard::ShardPlacement;

use super::CoreJournalRecord;
use super::RaftGroupLogRecord;
use super::RaftGroupLogStoreInner;
use super::core_meta::CoreMetadata;
use super::core_meta::core_metadata_path;
use super::disk::Disk;
use super::disk::DiskLock;
use super::disk::JournalDisk;
use super::disk::LockAttempt;
use super::disk::create_dir_all_durable;
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
use super::run_state::RunStateFile;
use super::run_state::RunStatus;
use super::run_state::core_replay_mode;
use super::state_file::StateFileError;
use super::truncate_entries_after;
use crate::engine::invalid_data;
use crate::rt::sync::mpsc;
use crate::rt::sync::oneshot;
use crate::rt::time::Instant;
use crate::types::CORE_LOG_GROUP_COMMIT_DELAY;
use crate::types::CORE_LOG_GROUP_COMMIT_MAX_BATCH;
use crate::types::CORE_LOG_GROUP_COMMIT_MAX_DELAY;
use crate::types::UrsulaRaftTypeConfig;

/// Journal size at which a purge or truncate rewrites the journal online.
#[cfg(not(madsim))]
const CORE_LOG_ONLINE_RECLAIM_MIN_BYTES: u64 = 64 * 1024 * 1024;
/// Simulated journals stay small, so the simulator reclaims at a lower size to
/// exercise the online rewrite.
#[cfg(madsim)]
const CORE_LOG_ONLINE_RECLAIM_MIN_BYTES: u64 = 16 * 1024;

/// Target encoded size of the entries in one Append frame of a rewritten
/// journal, far below `MAX_FRAME_PAYLOAD_BYTES`. A single entry larger than
/// this gets a frame of its own, which fits because it once fit in the frame
/// that first journaled it.
#[cfg(not(madsim))]
const COMPACTION_CHUNK_BYTES: usize = 8 * 1024 * 1024;
/// Simulated logs stay small, so the simulator chunks rewrites at a lower
/// size to exercise several Append frames per group.
#[cfg(madsim)]
const COMPACTION_CHUNK_BYTES: usize = 1024;

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
    #[error(transparent)]
    Metadata(Arc<StateFileError>),
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

impl From<StateFileError> for CoreJournalError {
    fn from(err: StateFileError) -> Self {
        Self::Metadata(Arc::new(err))
    }
}

/// OpenRaft storage methods report `io::Error`; this is the one conversion.
impl From<CoreJournalError> for io::Error {
    fn from(err: CoreJournalError) -> Self {
        let kind = match &err {
            CoreJournalError::Io { source, .. } => source.kind(),
            CoreJournalError::Journal(err) => err.kind(),
            CoreJournalError::Metadata(err) => match &**err {
                StateFileError::Io { source, .. } => source.kind(),
                _ => io::ErrorKind::InvalidData,
            },
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

/// How a core journal is opened.
#[derive(Debug, Clone)]
pub(crate) struct CoreJournalOptions {
    pub(crate) fsync: WalFsync,
    /// The run's recovery epoch: a journal last read in full in an earlier
    /// epoch is read as a verified prefix.
    pub(crate) recovery_epoch: u64,
    /// Where a poisoned writer records the failure.
    pub(crate) run_state: Arc<RunStateFile>,
}

/// One raft group's durable OpenRaft log, stored in its core's shared journal.
#[derive(Debug)]
pub struct RaftGroupFileLogStore {
    placement: ShardPlacement,
    metrics: GroupEngineMetrics,
    inner: Mutex<RaftGroupLogStoreInner>,
    /// Mirrors the group's durable `initialized` flag.
    initialized: AtomicBool,
    /// Serializes mutations so the journal records them in the same order as
    /// the in-memory state applies them.
    write_order: crate::rt::sync::Mutex<()>,
    core_writer: Arc<CoreFileLogWriter>,
}

/// The single writer of one core's journal.
#[derive(Debug)]
pub(crate) struct CoreFileLogWriter {
    journal_path: PathBuf,
    replay_mode: JournalReplayMode,
    tx: Option<mpsc::UnboundedSender<CoreWriterRequest>>,
    groups: Mutex<RecoveredGroups>,
    worker: Option<WriterWorker>,
    /// Released after the worker has stopped (see `Drop`).
    _lock: DiskLock,
}

/// What a group recovered from its core's journal and metadata file.
#[derive(Debug, Default)]
struct RecoveredGroup {
    inner: RaftGroupLogStoreInner,
    initialized: bool,
}

/// Recovered per-group state, handed out once per group.
#[derive(Debug, Default)]
struct RecoveredGroups {
    recovered: BTreeMap<u32, RecoveredGroup>,
    opened: BTreeSet<u32>,
}

#[cfg(not(madsim))]
type WriterWorker = std::thread::JoinHandle<()>;
#[cfg(madsim)]
type WriterWorker = sim_tokio::task::JoinHandle<()>;

/// A request to a core journal's writer.
#[derive(Debug)]
enum CoreWriterRequest {
    Write(CoreFileLogWrite),
    /// `fsync` the journal and stop; later requests fail.
    Close(CloseReply),
}

type CloseReply = oneshot::Sender<Result<(), CoreJournalError>>;

#[derive(Debug)]
struct CoreFileLogWrite {
    op: CoreWriteOp,
    reply: oneshot::Sender<Result<CoreFileLogWriteTiming, CoreJournalError>>,
}

#[derive(Debug)]
enum CoreWriteOp {
    /// A record appended to the journal.
    Record(CoreJournalRecord),
    /// A group's vote, kept in the metadata file.
    Vote {
        group_id: u32,
        vote: VoteOf<UrsulaRaftTypeConfig>,
    },
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
        let recovered = core_writer.take_recovered(placement.raft_group_id)?;
        Ok(Arc::new(Self {
            placement,
            metrics,
            inner: Mutex::new(recovered.inner),
            initialized: AtomicBool::new(recovered.initialized),
            write_order: crate::rt::sync::Mutex::new(()),
            core_writer,
        }))
    }

    /// Whether this replica ever persisted an entry or a purge of the group.
    /// The flag is durable and never cleared, even when a crash later costs
    /// the log its entries.
    pub fn initialized(&self) -> bool {
        self.initialized.load(Ordering::Acquire)
    }

    /// How the core journal holding this group was read when it opened.
    pub fn journal_replay_mode(&self) -> JournalReplayMode {
        self.core_writer.replay_mode
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
        self.record_timing(record_count, timing);
        Ok(())
    }

    fn record_timing(&self, record_count: usize, timing: CoreFileLogWriteTiming) {
        self.metrics.record_wal_batch(
            self.placement,
            record_count,
            timing.write_ns,
            timing.sync_ns,
        );
        self.metrics
            .record_wal_storage(self.placement, timing.storage);
    }
}

impl CoreFileLogWriter {
    /// Opens the journal at `journal_path`: takes its lock, reads the core's
    /// metadata file, recovers every group, compacts the recovered journal
    /// and starts the writer.
    pub(crate) fn open(
        journal_path: PathBuf,
        options: CoreJournalOptions,
        recovery_metrics: Option<(ShardPlacement, GroupEngineMetrics)>,
    ) -> Result<Arc<Self>, CoreJournalError> {
        if let Some(parent) = journal_path.parent() {
            create_dir_all_durable(parent)
                .map_err(|source| CoreJournalError::io(parent, source))?;
        }
        let lock = acquire_journal_lock(&journal_path)?;
        let metadata_path = core_metadata_path(&journal_path);
        let mut metadata = CoreMetadata::load(&metadata_path)?;
        let replay_mode = core_replay_mode(metadata.verified_epoch(), options.recovery_epoch);
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
        // A verified prefix may hold frames whose `fsync` failed and that
        // only the page cache still has, so it is always rewritten: every
        // frame it keeps is then on disk.
        let rewrite = match replay_mode {
            JournalReplayMode::Strict => Rewrite::IfSmaller,
            JournalReplayMode::VerifiedPrefix => Rewrite::Always,
        };
        if let Some(generation) =
            compact_core_journal(&journal_path, &recovered.groups, &replayed, rewrite)?
        {
            tracing::info!(
                path = %journal_path.display(),
                before_bytes = generation.before,
                after_bytes = generation.after,
                "compacted recovered OpenRaft core journal"
            );
        }
        // The journal now reads in full in this epoch. A group whose journal
        // holds entries or a purge is initialized, even if a crash came
        // between the journal write and the metadata write.
        let mut metadata_changed = metadata.set_verified_epoch(options.recovery_epoch);
        for (group_id, inner) in &recovered.groups {
            if !inner.entries.is_empty() || inner.last_purged_log_id.is_some() {
                metadata_changed |= metadata.mark_initialized(*group_id);
            }
        }
        if metadata_changed {
            metadata.store(&metadata_path)?;
        }
        let mut groups = recovered
            .groups
            .into_iter()
            .map(|(group_id, inner)| {
                (group_id, RecoveredGroup {
                    inner,
                    initialized: false,
                })
            })
            .collect::<BTreeMap<_, _>>();
        for (group_id, group) in metadata.groups() {
            let recovered = groups.entry(group_id).or_default();
            recovered.inner.vote = group.vote;
            recovered.initialized = group.initialized;
        }

        let mut journal = JournalWriter::open(&journal_path, FIRST_SEQUENCE)?;
        if journal.pending_bytes() != 0 {
            // A new journal: make its header and directory entry durable now.
            journal.sync()?;
        }
        let (tx, rx) = mpsc::unbounded_channel();
        let context = WriterContext {
            name: writer_name(&journal_path),
            journal_path: journal_path.clone(),
            metadata_path,
            fsync: options.fsync,
            run_state: options.run_state,
        };
        let worker = spawn_core_file_log_writer(context, journal, metadata, rx)?;
        Ok(Arc::new(Self {
            journal_path,
            replay_mode,
            tx: Some(tx),
            groups: Mutex::new(RecoveredGroups {
                recovered: groups,
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
    ) -> Result<RecoveredGroup, CoreJournalError> {
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

    fn stopped(&self) -> CoreJournalError {
        CoreJournalError::WriterStopped {
            journal: self.journal_path.clone(),
        }
    }

    fn send(&self, request: CoreWriterRequest) -> Result<(), CoreJournalError> {
        self.tx
            .as_ref()
            .ok_or_else(|| self.stopped())?
            .send(request)
            .map_err(|_closed| self.stopped())
    }

    async fn write(&self, op: CoreWriteOp) -> Result<CoreFileLogWriteTiming, CoreJournalError> {
        let (reply, response) = oneshot::channel();
        self.send(CoreWriterRequest::Write(CoreFileLogWrite { op, reply }))?;
        response.await.map_err(|_dropped| self.stopped())?
    }

    async fn append(
        &self,
        record: CoreJournalRecord,
    ) -> Result<CoreFileLogWriteTiming, CoreJournalError> {
        self.write(CoreWriteOp::Record(record)).await
    }

    async fn save_vote(
        &self,
        group_id: u32,
        vote: VoteOf<UrsulaRaftTypeConfig>,
    ) -> Result<CoreFileLogWriteTiming, CoreJournalError> {
        self.write(CoreWriteOp::Vote { group_id, vote }).await
    }

    /// Writes and `fsync`s everything sent before, then stops the writer.
    /// Every later request fails with [`CoreJournalError::WriterStopped`].
    pub(crate) async fn close(&self) -> Result<(), CoreJournalError> {
        let (reply, response) = oneshot::channel();
        self.send(CoreWriterRequest::Close(reply))?;
        response.await.map_err(|_dropped| self.stopped())?
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

/// Names a core's writer after its journal's directory (`core-0`).
fn writer_name(journal_path: &Path) -> String {
    journal_path.parent().and_then(Path::file_name).map_or_else(
        || "core".to_owned(),
        |name| name.to_string_lossy().into_owned(),
    )
}

/// Production runs the writer on its own thread with a current-thread runtime.
#[cfg(not(madsim))]
fn spawn_core_file_log_writer(
    context: WriterContext,
    journal: JournalWriter,
    metadata: CoreMetadata,
    rx: mpsc::UnboundedReceiver<CoreWriterRequest>,
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
                context, journal, metadata, rx,
            )))
        })
        .map_err(spawn_error)
}

/// The simulator runs the writer as a simulated task.
#[cfg(madsim)]
fn spawn_core_file_log_writer(
    context: WriterContext,
    journal: JournalWriter,
    metadata: CoreMetadata,
    rx: mpsc::UnboundedReceiver<CoreWriterRequest>,
) -> Result<WriterWorker, CoreJournalError> {
    Ok(crate::rt::spawn(run_core_file_log_writer(
        context, journal, metadata, rx,
    )))
}

/// The channel is closed, so the thread finishes its batch, `fsync`s the
/// journal and exits.
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

/// What a core journal's writer works with besides the journal itself.
#[derive(Debug)]
struct WriterContext {
    journal_path: PathBuf,
    metadata_path: PathBuf,
    fsync: WalFsync,
    run_state: Arc<RunStateFile>,
    /// Names the writer in a temporary run-state file.
    name: String,
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
    context: WriterContext,
    journal: JournalWriter,
    mut metadata: CoreMetadata,
    mut rx: mpsc::UnboundedReceiver<CoreWriterRequest>,
) {
    let mut state = WriterState::Open(journal);
    while let Some(first) = rx.recv().await {
        let batch = collect_batch(&mut rx, first, context.fsync).await;
        state = match state {
            WriterState::Open(journal) => {
                write_core_log_batch(&context, journal, &mut metadata, batch.writes)
            }
            WriterState::Poisoned(cause) => {
                refuse_core_log_batch(&context.journal_path, &cause, batch.writes);
                WriterState::Poisoned(cause)
            }
        };
        if let Some(reply) = batch.close {
            if reply.send(close_core_journal(&context, state)).is_err() {
                tracing::trace!("core journal close caller stopped waiting");
            }
            return;
        }
    }
    // Every handle is gone: this core stops. What it wrote becomes durable.
    if let WriterState::Open(journal) = state
        && let Err(cause) = sync_journal(journal)
    {
        on_journal_poisoned(&context, &cause);
    }
}

/// Requests the writer handles together, and a close that ends the batch.
#[derive(Debug, Default)]
struct Batch {
    writes: Vec<CoreFileLogWrite>,
    close: Option<CloseReply>,
}

impl Batch {
    fn push(&mut self, request: CoreWriterRequest) {
        match request {
            CoreWriterRequest::Write(write) => self.writes.push(write),
            CoreWriterRequest::Close(reply) => self.close = Some(reply),
        }
    }

    /// Whether no other request may join.
    fn is_closed(&self) -> bool {
        self.close.is_some() || self.writes.len() >= CORE_LOG_GROUP_COMMIT_MAX_BATCH
    }
}

/// Collects the batch that starts with `first`.
///
/// Under `never` it is what is queued now: nothing waits. Under `always` it
/// is a group commit: the writer keeps collecting while requests keep
/// arriving, each within [`CORE_LOG_GROUP_COMMIT_DELAY`] of the last, for at
/// most [`CORE_LOG_GROUP_COMMIT_MAX_DELAY`] after the first and up to
/// [`CORE_LOG_GROUP_COMMIT_MAX_BATCH`] requests, so one `fsync` covers a burst
/// however its senders are scheduled.
async fn collect_batch(
    rx: &mut mpsc::UnboundedReceiver<CoreWriterRequest>,
    first: CoreWriterRequest,
    fsync: WalFsync,
) -> Batch {
    let started_at = Instant::now();
    let mut batch = Batch::default();
    batch.push(first);
    loop {
        while !batch.is_closed() {
            match rx.try_recv() {
                Ok(request) => batch.push(request),
                Err(_empty_or_closed) => break,
            }
        }
        if batch.is_closed() || fsync == WalFsync::Never {
            return batch;
        }
        let Some(window) = group_commit_wait(started_at.elapsed()) else {
            return batch;
        };
        match recv_within(rx, window).await {
            Some(request) => batch.push(request),
            None => return batch,
        }
    }
}

/// How long a group commit that started `elapsed` ago waits for its next
/// request; `None` once it must be written.
fn group_commit_wait(elapsed: Duration) -> Option<Duration> {
    let wait = CORE_LOG_GROUP_COMMIT_MAX_DELAY
        .saturating_sub(elapsed)
        .min(CORE_LOG_GROUP_COMMIT_DELAY);
    (!wait.is_zero()).then_some(wait)
}

/// Waits up to `window` for the next request.
///
/// Tokio timers tick in whole milliseconds, which would stretch the batching
/// window. The writer owns its thread, so it parks the thread until a request
/// wakes it or the window ends, as a blocking timed receive would.
#[cfg(not(madsim))]
async fn recv_within(
    rx: &mut mpsc::UnboundedReceiver<CoreWriterRequest>,
    window: Duration,
) -> Option<CoreWriterRequest> {
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
    rx: &mut mpsc::UnboundedReceiver<CoreWriterRequest>,
    window: Duration,
) -> Option<CoreWriterRequest> {
    crate::rt::time::timeout(window, rx.recv())
        .await
        .ok()
        .flatten()
}

/// Writes one batch: encodes every record, writes the frames, `fsync`s them
/// when the policy and a record need it, replaces the metadata file when a
/// vote or a newly initialized group changed it, reclaims the journal online
/// after a purge or truncate, and replies. Returns the writer's next state.
fn write_core_log_batch(
    context: &WriterContext,
    mut journal: JournalWriter,
    metadata: &mut CoreMetadata,
    batch: Vec<CoreFileLogWrite>,
) -> WriterState {
    let write_started_at = Instant::now();
    let mut accepted = Vec::with_capacity(batch.len());
    let mut flushed = Ok(());
    let mut journal_records = 0_u64;
    let mut votes = 0_u64;
    let mut requires_sync = false;
    let mut reclaim_due = false;
    let mut metadata_changed = false;
    for request in batch {
        match &request.op {
            CoreWriteOp::Vote { group_id, vote } => {
                votes = votes.saturating_add(1);
                metadata_changed |= metadata.set_vote(*group_id, *vote);
            }
            CoreWriteOp::Record(record) => {
                if flushed.is_ok() {
                    if let Err(too_large) = journal.append::<WireCodec<CoreJournalRecord>>(record) {
                        request.reply(Err(CoreJournalError::RecordTooLarge(too_large)));
                        continue;
                    }
                    if journal.pending_bytes() >= WRITE_BUFFER_BYTES {
                        flushed = journal.flush();
                    }
                }
                journal_records = journal_records.saturating_add(1);
                requires_sync |= raft_group_log_record_requires_sync(&record.record);
                reclaim_due |= matches!(
                    &record.record,
                    RaftGroupLogRecord::Purge(_) | RaftGroupLogRecord::TruncateAfter(_)
                );
                if raft_group_log_record_initializes(&record.record) {
                    metadata_changed |= metadata.mark_initialized(record.group_id);
                }
            }
        }
        accepted.push(request);
    }
    if accepted.is_empty() {
        return WriterState::Open(journal);
    }
    let sync_journal = requires_sync && context.fsync == WalFsync::Always;
    let written = flushed.and_then(|()| journal.flush()).and_then(|()| {
        let write_ns = elapsed_ns(write_started_at);
        if !sync_journal && !metadata_changed {
            return Ok((write_ns, 0, 0));
        }
        let sync_started_at = Instant::now();
        let mut fsyncs = 0_u64;
        if sync_journal {
            fsyncs = fsyncs.saturating_add(journal.sync()?);
        }
        if metadata_changed {
            fsyncs = fsyncs.saturating_add(metadata.store(&context.metadata_path)?);
        }
        Ok((write_ns, elapsed_ns(sync_started_at), fsyncs))
    });
    let (write_ns, sync_ns, fsyncs) = match written {
        Ok(written) => written,
        Err(cause) => {
            drop(journal);
            return poison_core_journal(context, cause, accepted);
        }
    };

    let fsync_records = match (sync_journal, metadata_changed) {
        (true, true) => journal_records.saturating_add(votes),
        (true, false) => journal_records,
        (false, true) => votes,
        (false, false) => 0,
    };
    let mut storage = WalStorageSample {
        fsyncs,
        fsync_records,
        physical_bytes: journal.len(),
        ..WalStorageSample::default()
    };
    let state = if reclaim_due && journal.len() >= CORE_LOG_ONLINE_RECLAIM_MIN_BYTES {
        reclaim_after_batch(&context.journal_path, journal, &mut storage)
    } else {
        WriterState::Open(journal)
    };

    // The batch is durable whatever the reclaim did.
    reply_core_log_batch(accepted, write_ns, sync_ns, storage);
    if let WriterState::Poisoned(cause) = &state {
        on_journal_poisoned(context, cause);
    }
    state
}

/// Writes and `fsync`s whatever the journal holds.
fn sync_journal(mut journal: JournalWriter) -> Result<(), Arc<JournalError>> {
    journal.sync().map(|_fsyncs| ()).map_err(Arc::new)
}

/// Makes everything written durable before the writer stops. A failed
/// `fsync` poisons the journal as it would during a batch.
fn close_core_journal(context: &WriterContext, state: WriterState) -> Result<(), CoreJournalError> {
    let cause = match state {
        WriterState::Open(journal) => match sync_journal(journal) {
            Ok(()) => return Ok(()),
            Err(cause) => {
                on_journal_poisoned(context, &cause);
                cause
            }
        },
        WriterState::Poisoned(cause) => cause,
    };
    Err(CoreJournalError::WriterPoisoned {
        journal: context.journal_path.clone(),
        cause,
    })
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
    context: &WriterContext,
    cause: JournalError,
    batch: Vec<CoreFileLogWrite>,
) -> WriterState {
    let cause = Arc::new(cause);
    refuse_core_log_batch(&context.journal_path, &cause, batch);
    on_journal_poisoned(context, &cause);
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
/// journal failure reaches beyond the requests it fails: it records the
/// failure in the node's run state, so the next start reads every journal as
/// a verified prefix, and then stops the process, because only a restart can
/// re-read what is really on disk.
fn on_journal_poisoned(context: &WriterContext, cause: &JournalError) {
    tracing::error!(
        path = %context.journal_path.display(),
        error = %cause,
        "OpenRaft core journal failed; the writer is poisoned and the process stops"
    );
    // Best effort: the disk that just failed may refuse this write too.
    if let Err(err) = context
        .run_state
        .record(RunStatus::Poisoned, &format!("poisoned-{}", context.name))
    {
        tracing::error!(
            path = %context.journal_path.display(),
            %err,
            "could not record the journal failure in the Raft WAL run state"
        );
    }
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

    /// Votes go to the core's metadata file, which is always `fsync`ed.
    async fn save_vote(&mut self, vote: &VoteOf<UrsulaRaftTypeConfig>) -> Result<(), io::Error> {
        let vote = *vote;
        let _order = self.write_order.lock().await;
        if self.lock_inner()?.vote == Some(vote) {
            return Ok(());
        }
        let timing = self
            .core_writer
            .save_vote(self.placement.raft_group_id.0, vote)
            .await?;
        self.record_timing(1, timing);
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
        if !entries.is_empty() {
            self.initialized.store(true, Ordering::Release);
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
        self.initialized.store(true, Ordering::Release);
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
    append_live_state(&mut handle, groups, COMPACTION_CHUNK_BYTES)?;
    // The rename and the directory `fsync` in `install_generation` publish
    // the file, so its own directory entry needs no `fsync` first.
    let fsyncs = handle.sync_data()?;
    Ok(NextGeneration {
        len: handle.len(),
        path,
        fsyncs,
    })
}

/// Appends every group's live state to `handle`. Entries go in Append frames
/// of about `chunk_bytes` each, so a group's live log of any size fits:
/// one frame per group would exceed the frame limit for a large group. Votes
/// live in the metadata file, not here.
fn append_live_state(
    handle: &mut JournalWriter,
    groups: &BTreeMap<u32, RaftGroupLogStoreInner>,
    chunk_bytes: usize,
) -> Result<(), JournalError> {
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
        if let Some(committed) = inner.committed {
            write(RaftGroupLogRecord::SaveCommitted(Some(committed)))?;
        }
        if let Some(purged) = inner.last_purged_log_id {
            write(RaftGroupLogRecord::Purge(purged))?;
        }
        let mut chunk = Vec::new();
        let mut chunk_len = 0_usize;
        for entry in inner.entries.values() {
            let entry_len = wire_len(entry);
            if !chunk.is_empty() && chunk_len.saturating_add(entry_len) > chunk_bytes {
                write(RaftGroupLogRecord::Append(std::mem::take(&mut chunk)))?;
                chunk_len = 0;
            }
            chunk.push(entry.clone());
            chunk_len = chunk_len.saturating_add(entry_len);
        }
        if !chunk.is_empty() {
            write(RaftGroupLogRecord::Append(chunk))?;
        }
    }
    Ok(())
}

/// The MessagePack size of `value`, measured without allocating it.
fn wire_len<T: Serialize>(value: &T) -> usize {
    struct Counter(usize);

    impl io::Write for Counter {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.0 = self.0.saturating_add(buf.len());
            Ok(buf.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    let mut counter = Counter(0);
    rmp_serde::encode::write_named(&mut counter, value)
        .expect("wire value serializes to MessagePack");
    counter.0
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

/// When a recovered journal is rewritten as its next generation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Rewrite {
    /// Only when the next generation is smaller.
    IfSmaller,
    /// Whatever its size, so every frame kept is on disk.
    Always,
}

/// Rewrites a recovered journal as its next generation, as `rewrite` says.
fn compact_core_journal(
    journal_path: &Path,
    groups: &BTreeMap<u32, RaftGroupLogStoreInner>,
    replayed: &Replayed,
    rewrite: Rewrite,
) -> Result<Option<InstalledGeneration>, JournalError> {
    let Some(live_sequence) = replayed.sequence else {
        return Ok(None);
    };
    let before = replayed.verified_len;
    let generation = write_next_generation(journal_path, groups, live_sequence)?;
    if rewrite == Rewrite::IfSmaller && generation.len >= before {
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
/// storage method returns under `fsync = always`.
///
/// Committed and truncate markers are replay optimizations. Losing either in a
/// crash leaves the durable entries intact and OpenRaft re-establishes the
/// marker after restart. Append durability is a consensus safety
/// requirement. Purge remains durable because online reclaim may physically
/// discard the entries it covers. Votes are not journal records: the
/// metadata file holds them and is always `fsync`ed.
fn raft_group_log_record_requires_sync(record: &RaftGroupLogRecord) -> bool {
    matches!(
        record,
        RaftGroupLogRecord::Append(_) | RaftGroupLogRecord::Purge(_)
    )
}

/// Whether this record shows the group was initialized on this replica: it
/// persists an entry or a purge (which follows a snapshot).
fn raft_group_log_record_initializes(record: &RaftGroupLogRecord) -> bool {
    match record {
        RaftGroupLogRecord::Append(entries) => !entries.is_empty(),
        RaftGroupLogRecord::Purge(_) => true,
        RaftGroupLogRecord::SaveCommitted(_) | RaftGroupLogRecord::TruncateAfter(_) => false,
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

    use super::Arc;
    use super::BTreeMap;
    use super::CORE_LOG_ONLINE_RECLAIM_MIN_BYTES;
    use super::CoreFileLogWriter;
    use super::CoreJournalError;
    use super::CoreJournalOptions;
    use super::CoreJournalRecord;
    use super::CoreMetadata;
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
    use super::RaftGroupLogStoreInner;
    use super::RaftLogReader;
    use super::RaftLogStorage;
    use super::Reclaim;
    use super::RecordTooLarge;
    use super::Rewrite;
    use super::RunStateFile;
    use super::ShardPlacement;
    use super::UrsulaRaftTypeConfig;
    use super::VoteOf;
    use super::WalFsync;
    use super::WalStorageSample;
    use super::WireCodec;
    use super::WriterState;
    use super::append_live_state;
    use super::compact_core_journal;
    use super::core_metadata_path;
    use super::group_commit_wait;
    use super::io;
    use super::journal::ReplayTail;
    use super::raft_group_log_record_initializes;
    use super::raft_group_log_record_requires_sync;
    use super::reclaim_after_batch;
    use super::reclaim_core_journal;
    use super::recover_core_journal;
    use super::wire_len;
    use crate::log_store::RunState;
    use crate::log_store::RunStatus;

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

    /// How a run with `fsync` in recovery epoch `recovery_epoch` opens the
    /// journal at `path`. A journal never read before is read strictly in
    /// epoch 0 and as a verified prefix in any later epoch.
    fn options(path: &Path, fsync: WalFsync, recovery_epoch: u64) -> CoreJournalOptions {
        CoreJournalOptions {
            fsync,
            recovery_epoch,
            run_state: Arc::new(RunStateFile::new(
                path.with_extension("run-state"),
                RunState {
                    boot_id: None,
                    fsync,
                    status: RunStatus::Running,
                    recovery_epoch,
                },
            )),
        }
    }

    fn open_writer(
        path: &Path,
        mode: JournalReplayMode,
    ) -> Result<Arc<CoreFileLogWriter>, CoreJournalError> {
        let recovery_epoch = match mode {
            JournalReplayMode::Strict => 0,
            JournalReplayMode::VerifiedPrefix => 1,
        };
        CoreFileLogWriter::open(
            path.to_owned(),
            options(path, WalFsync::Always, recovery_epoch),
            None,
        )
    }

    fn remove_journal(path: &Path) {
        crate::tests::remove_test_path(path);
        crate::tests::remove_test_path(format!("{}.lock", path.display()));
        crate::tests::remove_test_path(core_metadata_path(path));
        crate::tests::remove_test_path(path.with_extension("run-state"));
    }

    #[test]
    fn fsync_policy_keeps_only_replay_hints_best_effort() {
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

    /// A group commit waits up to 200 µs for each next request, and stops
    /// collecting 1 ms after its first.
    #[test]
    fn the_group_commit_window_extends_up_to_its_limit() {
        let micros = std::time::Duration::from_micros;
        assert_eq!(group_commit_wait(micros(0)), Some(micros(200)));
        assert_eq!(group_commit_wait(micros(700)), Some(micros(200)));
        assert_eq!(group_commit_wait(micros(900)), Some(micros(100)));
        assert_eq!(group_commit_wait(micros(1_000)), None);
        assert_eq!(group_commit_wait(micros(5_000)), None);
    }

    #[test]
    fn entries_and_purges_initialize_a_group() {
        assert!(raft_group_log_record_initializes(
            &RaftGroupLogRecord::Append(vec![blank_entry(1)])
        ));
        assert!(raft_group_log_record_initializes(
            &RaftGroupLogRecord::Purge(test_log_id(1))
        ));
        assert!(!raft_group_log_record_initializes(
            &RaftGroupLogRecord::Append(Vec::new())
        ));
        assert!(!raft_group_log_record_initializes(
            &RaftGroupLogRecord::SaveCommitted(Some(test_log_id(1)))
        ));
        assert!(!raft_group_log_record_initializes(
            &RaftGroupLogRecord::TruncateAfter(None)
        ));
    }

    #[test]
    fn load_core_journal_truncates_torn_tail() {
        let path = temp_journal_path("core-journal-torn-tail");
        write_records(&path, [record(
            3,
            RaftGroupLogRecord::Append(vec![blank_entry(1)]),
        )]);
        let valid_len = file_len(&path);

        append_torn_frame(&path);
        assert!(file_len(&path) > valid_len);

        let recovered = strict(&path);
        assert_eq!(
            recovered
                .groups
                .get(&3)
                .map(|inner| inner.entries.keys().copied().collect::<Vec<_>>()),
            Some(vec![1])
        );
        assert_eq!(recovered.replayed.tail, ReplayTail::Incomplete { bytes: 8 });
        assert_eq!(file_len(&path), valid_len);
        crate::tests::remove_test_path(&path);
    }

    #[test]
    fn load_core_journal_distributes_all_groups_in_one_scan() {
        let path = temp_journal_path("core-journal-groups");
        write_records(&path, [
            record(3, RaftGroupLogRecord::SaveCommitted(Some(test_log_id(2)))),
            record(7, RaftGroupLogRecord::SaveCommitted(Some(test_log_id(4)))),
        ]);

        let recovered = strict(&path);
        assert_eq!(recovered.groups.len(), 2);
        assert_eq!(
            recovered.groups.get(&3).and_then(|inner| inner.committed),
            Some(test_log_id(2))
        );
        assert_eq!(
            recovered.groups.get(&7).and_then(|inner| inner.committed),
            Some(test_log_id(4))
        );
        crate::tests::remove_test_path(&path);
    }

    #[test]
    fn compact_core_journal_keeps_only_recovered_state() {
        let path = temp_journal_path("core-journal-compact");
        write_records(
            &path,
            std::iter::repeat_n(test_log_id(1), 100)
                .map(|committed| record(7, RaftGroupLogRecord::SaveCommitted(Some(committed))))
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

        let compacted = compact_core_journal(
            &path,
            &recovered.groups,
            &recovered.replayed,
            Rewrite::IfSmaller,
        )
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
        assert_eq!(group.vote, None, "votes are not journaled");
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

    /// A rewrite writes a group's live entries in bounded chunks, so a group
    /// whose live log exceeds the frame limit still compacts. One Append
    /// frame per group (an unbounded chunk) fails.
    #[test]
    fn compaction_chunks_a_group_larger_than_the_frame_limit() {
        const FRAME_LIMIT: usize = 4096;
        let path = temp_journal_path("core-journal-chunked");
        let entries = (1..=64)
            .map(|index| payload_entry(index, 512))
            .collect::<Vec<_>>();
        let mut groups = BTreeMap::new();
        let group = groups
            .entry(7)
            .or_insert_with(RaftGroupLogStoreInner::default);
        group.committed = Some(test_log_id(1));
        for entry in &entries {
            group.entries.insert(entry.log_id.index, entry.clone());
        }
        let live_bytes = entries.iter().map(wire_len).sum::<usize>();
        assert!(live_bytes > 8 * FRAME_LIMIT);

        let mut unbounded = JournalWriter::open(&path, FIRST_SEQUENCE)
            .expect("open journal")
            .with_frame_limit(FRAME_LIMIT);
        let err = append_live_state(&mut unbounded, &groups, usize::MAX)
            .expect_err("one frame per group exceeds the limit");
        assert!(matches!(
            err,
            JournalError::RecordTooLarge(RecordTooLarge {
                limit: FRAME_LIMIT,
                ..
            })
        ));
        drop(unbounded);
        crate::tests::remove_test_path(&path);

        let mut chunked = JournalWriter::open(&path, FIRST_SEQUENCE)
            .expect("open journal")
            .with_frame_limit(FRAME_LIMIT);
        append_live_state(&mut chunked, &groups, FRAME_LIMIT / 2)
            .expect("chunks fit the frame limit");
        chunked.sync().expect("sync");
        drop(chunked);

        let recovered = strict(&path);
        assert!(
            recovered.replayed.frames > 8,
            "the entries span several frames: {:?}",
            recovered.replayed
        );
        let group = recovered.groups.get(&7).expect("recovered group");
        assert_eq!(group.committed, Some(test_log_id(1)));
        assert_eq!(
            group.entries.values().cloned().collect::<Vec<_>>(),
            entries,
            "chunking keeps every entry in order"
        );
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
        let committed = test_log_id(2);
        write_records(&path, [
            record(3, RaftGroupLogRecord::SaveCommitted(Some(committed))),
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

        let err = open_writer(&path, JournalReplayMode::Strict)
            .expect_err("strict recovery fails closed");
        assert!(
            matches!(&err, CoreJournalError::Journal(err) if matches!(**err, JournalError::CorruptFrame { frame: 4, offset, .. } if offset == prefix.verified_len)),
            "unexpected error: {err}"
        );
        assert_eq!(file_len(&path), len, "strict recovery truncates nothing");

        let writer = open_writer(&path, JournalReplayMode::VerifiedPrefix)
            .expect("verified-prefix recovery");
        assert_eq!(writer.replay_mode, JournalReplayMode::VerifiedPrefix);
        let recovered = writer
            .take_recovered(RaftGroupId(3))
            .expect("recovered group");
        assert_eq!(recovered.inner.committed, Some(committed));
        assert_eq!(
            recovered.inner.entries.keys().copied().collect::<Vec<_>>(),
            [1, 2, 3, 4]
        );
        assert!(recovered.initialized, "a group with entries is initialized");
        drop(writer);
        assert_eq!(
            strict(&path).replayed.sequence,
            Some(FIRST_SEQUENCE + 1),
            "a verified prefix is rewritten as the next generation"
        );
        let writer = open_writer(&path, JournalReplayMode::VerifiedPrefix).expect("reopen");
        assert_eq!(
            writer.replay_mode,
            JournalReplayMode::Strict,
            "a journal read in full in this epoch is read strictly"
        );
        drop(writer);
        remove_journal(&path);
    }

    #[test]
    fn core_file_log_rejects_a_second_owner() {
        let path = temp_journal_path("core-exclusive-lock");
        let first = open_writer(&path, JournalReplayMode::Strict).expect("open first core owner");
        let err =
            open_writer(&path, JournalReplayMode::Strict).expect_err("second core owner must fail");
        assert!(
            matches!(&err, CoreJournalError::Locked { owner: Some(owner), .. } if owner.starts_with("pid=")),
            "unexpected error: {err}"
        );
        let err = io::Error::from(err);
        assert_eq!(err.kind(), io::ErrorKind::AlreadyExists);
        drop(first);
        let reopened =
            open_writer(&path, JournalReplayMode::Strict).expect("core lock releases on drop");
        drop(reopened);
        remove_journal(&path);
    }

    /// `wal_fsyncs` counts `fsync` calls, not batches: a batch of replay
    /// hints is written without one, and the first entry of a group also
    /// replaces the metadata file (the file and its directory).
    #[tokio::test]
    async fn wal_fsyncs_count_fsyncs_not_batches() {
        let path = temp_journal_path("core-fsync-metrics");
        let metrics = RuntimeMetrics::new(1, 2);
        let writer = open_writer(&path, JournalReplayMode::Strict).expect("open core writer");
        let mut store =
            RaftGroupFileLogStore::open(placement(1), metrics.group_engine_metrics(), writer)
                .expect("open group store");
        store
            .append([blank_entry(1)], IOFlushed::noop())
            .await
            .expect("append an entry");
        store
            .append([blank_entry(2)], IOFlushed::noop())
            .await
            .expect("append another entry");
        store
            .save_committed(Some(test_log_id(1)))
            .await
            .expect("journal a committed marker");

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.wal_batches, 3);
        assert_eq!(snapshot.wal_fsyncs, 1 + 2 + 1);
        assert_eq!(snapshot.wal_fsync_records, 2);
        assert_eq!(snapshot.wal_physical_bytes, file_len(&path));
        drop(store);
        remove_journal(&path);
    }

    /// Votes are kept in the core's metadata file, which is always
    /// `fsync`ed, and never in the journal.
    #[tokio::test]
    async fn votes_live_in_the_metadata_file_not_the_journal() {
        let path = temp_journal_path("core-votes");
        let metrics = RuntimeMetrics::new(1, 2);
        let open = |fsync| {
            let writer = CoreFileLogWriter::open(path.clone(), options(&path, fsync, 0), None)
                .expect("open core writer");
            RaftGroupFileLogStore::open(placement(1), metrics.group_engine_metrics(), writer)
                .expect("open group store")
        };
        let mut store = open(WalFsync::Never);
        let journal_len = file_len(&path);
        let vote = committed_vote();
        store.save_vote(&vote).await.expect("save a vote");
        store.save_vote(&vote).await.expect("the same vote again");
        assert_eq!(file_len(&path), journal_len, "the journal holds no vote");
        assert_eq!(
            metrics.snapshot().wal_fsyncs,
            2,
            "one metadata replacement, even under fsync = never"
        );
        assert!(!store.initialized(), "a vote alone does not initialize");
        assert_eq!(
            CoreMetadata::load(&core_metadata_path(&path))
                .expect("metadata")
                .group(1)
                .vote,
            Some(vote)
        );
        drop(store);

        let mut store = open(WalFsync::Always);
        assert_eq!(store.read_vote().await.expect("read vote"), Some(vote));
        assert!(!store.initialized());
        drop(store);
        remove_journal(&path);
    }

    /// `initialized` becomes durable with a group's first entry and stays
    /// set when the entries are gone.
    #[tokio::test]
    async fn initialized_is_durable_from_the_first_entry_and_never_cleared() {
        let path = temp_journal_path("core-initialized");
        let metrics = RuntimeMetrics::new(1, 2);
        let open = || {
            let writer = open_writer(&path, JournalReplayMode::Strict).expect("open core writer");
            RaftGroupFileLogStore::open(placement(1), metrics.group_engine_metrics(), writer)
                .expect("open group store")
        };
        let mut store = open();
        assert!(!store.initialized());
        store
            .append(Vec::new(), IOFlushed::noop())
            .await
            .expect("an empty append");
        assert!(!store.initialized(), "an empty append initializes nothing");
        store
            .append([blank_entry(1)], IOFlushed::noop())
            .await
            .expect("append an entry");
        assert!(store.initialized());
        drop(store);

        let mut store = open();
        assert!(store.initialized(), "the flag survives a restart");
        store.truncate_after(None).await.expect("drop every entry");
        drop(store);
        let mut store = open();
        assert_eq!(
            store.get_log_state().await.expect("log state").last_log_id,
            None
        );
        assert!(store.initialized(), "the flag is never cleared");
        drop(store);
        remove_journal(&path);
    }

    /// A crash between a group's first journal write and the metadata write
    /// leaves entries without the flag; recovery restores it.
    #[test]
    fn recovery_restores_a_missing_initialized_flag() {
        let path = temp_journal_path("core-initialized-heal");
        write_records(&path, [
            record(7, RaftGroupLogRecord::Append(vec![blank_entry(1)])),
            record(9, RaftGroupLogRecord::SaveCommitted(None)),
        ]);
        assert!(!core_metadata_path(&path).exists());

        let writer = open_writer(&path, JournalReplayMode::Strict).expect("open core writer");
        assert!(
            writer
                .take_recovered(RaftGroupId(7))
                .expect("group 7")
                .initialized
        );
        assert!(
            !writer
                .take_recovered(RaftGroupId(9))
                .expect("group 9")
                .initialized
        );
        let metadata = CoreMetadata::load(&core_metadata_path(&path)).expect("metadata");
        assert!(metadata.group(7).initialized);
        assert!(!metadata.group(9).initialized);
        drop(writer);
        remove_journal(&path);
    }

    /// Under `never` an append is acknowledged without a journal `fsync`;
    /// closing the writer `fsync`s the journal and refuses later writes.
    #[tokio::test]
    async fn fsync_never_acknowledges_from_the_page_cache_and_close_syncs() {
        let path = temp_journal_path("core-fsync-never");
        let metrics = RuntimeMetrics::new(1, 2);
        let writer =
            CoreFileLogWriter::open(path.clone(), options(&path, WalFsync::Never, 0), None)
                .expect("open core writer");
        let mut store = RaftGroupFileLogStore::open(
            placement(1),
            metrics.group_engine_metrics(),
            writer.clone(),
        )
        .expect("open group store");
        store
            .append([blank_entry(1)], IOFlushed::noop())
            .await
            .expect("append an entry");
        assert_eq!(
            metrics.snapshot().wal_fsyncs,
            2,
            "only the metadata file that marks the group initialized"
        );
        store
            .append([blank_entry(2)], IOFlushed::noop())
            .await
            .expect("append another entry");
        assert_eq!(metrics.snapshot().wal_fsyncs, 2, "no journal fsync");

        writer.close().await.expect("close the writer");
        let err = store
            .append([blank_entry(3)], IOFlushed::noop())
            .await
            .expect_err("a closed writer refuses writes");
        assert!(matches!(
            err.get_ref()
                .and_then(|err| err.downcast_ref::<CoreJournalError>()),
            Some(CoreJournalError::WriterStopped { .. })
        ));
        assert!(matches!(
            writer.close().await,
            Err(CoreJournalError::WriterStopped { .. })
        ));
        drop(store);
        drop(writer);
        let recovered = strict(&path);
        assert_eq!(
            recovered.groups[&1]
                .entries
                .keys()
                .copied()
                .collect::<Vec<_>>(),
            [1, 2]
        );
        remove_journal(&path);
    }

    #[tokio::test]
    async fn a_group_opens_once_per_core_writer() {
        let path = temp_journal_path("core-group-reopen");
        let metrics = RuntimeMetrics::new(1, 2).group_engine_metrics();
        let open = || open_writer(&path, JournalReplayMode::Strict);
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

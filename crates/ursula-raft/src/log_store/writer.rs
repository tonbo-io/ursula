//! The single writer of one core's journal.
//!
//! Every group on a core sends its records to the core's writer, an async
//! loop over the runtime shim's channel. Production runs it on a dedicated
//! OS thread with a current-thread runtime, so blocking file I/O stays off
//! the async workers; `cfg(madsim)` runs it as a simulated task over the
//! simulated disk. Callers await a reply that arrives after their batch is
//! written and, when the fsync policy needs it, `fsync`ed, and after the
//! writer applied it to the group's [`GroupLog`].
//!
//! Each group's vote and log state live in the core's metadata file
//! (`core_meta`), which the writer replaces with an `fsync` under either
//! policy. Committed, truncate and purge markers and the entries go to the
//! journal's segments (`segment`).
//!
//! A group's first entry or purge records it initialized in the same batch.
//! While the group's recovery gate is closed and the replica holds nothing
//! of the group, its history is unknown (a new replica or a wiped disk), so
//! that first entry records it recovering instead: a restart before the gate
//! opens comes back gated.
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
//! After it replies, the writer rotates a full segment and reclaims old ones
//! (`reclaim`): it deletes segments no group needs, rewrites the small
//! remainders of quiet groups out of the oldest segment in bounded chunks,
//! and reports the groups that keep it alive with more to [`LaggingGroups`],
//! which the snapshot driver reads.
//!
//! Errors are fail-stop. A failed write or `fsync` of the journal, a
//! rotation, a rewrite or a directory, or a frame that fails verification,
//! poisons the writer: the request that hit it and every later one fail. A
//! reclaim step that leaves the journal as it was (reading an old segment,
//! removing one) is logged and counted instead, and retried by a later pass.
//! Reclaim runs after the batch is acknowledged, so it never fails a batch
//! whose writes are already durable.
//!
//! When the last handle to a writer goes, or on [`CoreFileLogWriter::close`],
//! the writer `fsync`s its journal and stops.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::fmt::Debug;
use std::io;
use std::marker::PhantomData;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
#[cfg(not(madsim))]
use std::task::Context;
#[cfg(not(madsim))]
use std::task::Poll;
#[cfg(not(madsim))]
use std::task::Wake;
#[cfg(not(madsim))]
use std::task::Waker;
use std::time::Duration;

use openraft::alias::EntryOf;
use openraft::alias::VoteOf;
use serde::Serialize;
use serde::de::DeserializeOwned;
use ursula_config::WalFsync;
use ursula_runtime::GroupEngineMetrics;
use ursula_runtime::WalJournalSample;
use ursula_runtime::WalMemorySample;
use ursula_runtime::WalStorageSample;
use ursula_shard::CoreId;
use ursula_shard::RaftGroupId;
use ursula_shard::ShardPlacement;

use super::CoreJournalRecord;
use super::RaftGroupLogRecord;
use super::core_meta::CoreMetadata;
use super::core_meta::GroupLogState;
use super::core_meta::GroupMetadata;
use super::core_meta::core_metadata_path;
use super::disk::Disk;
use super::disk::DiskFile;
use super::disk::DiskLock;
use super::disk::JournalDisk;
use super::disk::LockAttempt;
use super::disk::create_dir_all_durable;
use super::group_log::ApplyMode;
use super::group_log::DiskRead;
use super::group_log::FramePos;
use super::group_log::GroupLog;
use super::group_log::IndexedEntry;
use super::journal;
use super::journal::JournalError;
use super::journal::JournalOp;
use super::journal::JournalReplayMode;
use super::journal::JournalWriter;
use super::journal::RecordTooLarge;
use super::journal::WRITE_BUFFER_BYTES;
use super::reclaim;
use super::reclaim::GroupPin;
use super::reclaim::JournalShape;
use super::reclaim::ReclaimLimits;
use super::run_state::RecoveryState;
use super::run_state::RunStateFile;
use super::run_state::RunStatus;
use super::run_state::core_replay_mode;
use super::segment;
use super::segment::DeleteError;
use super::segment::RecoveryEnd;
use super::segment::SegmentId;
use super::segment::segment_path;
use super::state_file::StateFileError;
use crate::engine::invalid_data;
use crate::rt::sync::Notify;
use crate::rt::sync::mpsc;
use crate::rt::sync::oneshot;
use crate::rt::time::Instant;
use crate::types::CORE_LOG_GROUP_COMMIT_DELAY;
use crate::types::CORE_LOG_GROUP_COMMIT_MAX_BATCH;
use crate::types::CORE_LOG_GROUP_COMMIT_MAX_DELAY;
use crate::types::UrsulaRaftTypeConfig;
use crate::types::entry_log_bytes;

type Entry = EntryOf<UrsulaRaftTypeConfig>;

/// The lock file of a core journal, in its directory.
const CORE_JOURNAL_LOCK_FILE: &str = "journal.lock";

/// How a node's core journals are written and cached.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct JournalTuning {
    pub fsync: WalFsync,
    /// The size at which the writer rotates to a new segment.
    pub segment_bytes: u64,
    /// The recent entries each group keeps in memory, in log bytes.
    pub group_cache_bytes: u64,
}

impl JournalTuning {
    /// The default target segment size.
    #[cfg(not(madsim))]
    pub const DEFAULT_SEGMENT_BYTES: u64 = 64 * 1024 * 1024;
    /// Simulated journals stay small, so the simulator rotates often.
    #[cfg(madsim)]
    pub const DEFAULT_SEGMENT_BYTES: u64 = 4 * 1024;
    /// The default cache of recent entries per group.
    #[cfg(not(madsim))]
    pub const DEFAULT_GROUP_CACHE_BYTES: u64 = 4 * 1024 * 1024;
    /// Simulated groups keep a few entries, so the simulator reads from disk.
    #[cfg(madsim)]
    pub const DEFAULT_GROUP_CACHE_BYTES: u64 = 1024;
    /// The smallest segment size; smaller targets rotate on every batch.
    pub const MIN_SEGMENT_BYTES: u64 = 4 * 1024;

    pub fn new(fsync: WalFsync) -> Self {
        Self {
            fsync,
            segment_bytes: Self::DEFAULT_SEGMENT_BYTES,
            group_cache_bytes: Self::DEFAULT_GROUP_CACHE_BYTES,
        }
    }

    fn segment_bytes(&self) -> u64 {
        self.segment_bytes.max(Self::MIN_SEGMENT_BYTES)
    }

    /// Target encoded size of the entries in one rewritten Append frame.
    fn rewrite_chunk_bytes(&self) -> u64 {
        (self.segment_bytes() / 16).clamp(256, 1024 * 1024)
    }

    /// Live bytes one reclaim pass rewrites at most, so a pass never holds
    /// the writer for long.
    fn rewrite_pass_bytes(&self) -> u64 {
        (self.segment_bytes() / 8).max(1024)
    }
}

/// Groups whose live records keep old journal segments alive with more
/// than a rewrite copies, by core. The snapshot driver snapshots them first,
/// and their purge frees the segments.
#[derive(Debug, Default)]
pub struct LaggingGroups {
    by_core: Mutex<BTreeMap<u16, BTreeSet<u32>>>,
    changed: Notify,
}

impl LaggingGroups {
    /// The groups reported lagging on every core.
    pub fn groups(&self) -> BTreeSet<RaftGroupId> {
        self.by_core
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .values()
            .flatten()
            .map(|group| RaftGroupId(*group))
            .collect()
    }

    /// Waits until the set changes after the last wait returned.
    pub async fn changed(&self) {
        self.changed.notified().await;
    }

    fn set(&self, core: CoreId, groups: &BTreeSet<u32>) {
        let mut by_core = self
            .by_core
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let changed = match by_core.get(&core.0) {
            Some(previous) => previous != groups,
            None => !groups.is_empty(),
        };
        if !changed {
            return;
        }
        if groups.is_empty() {
            by_core.remove(&core.0);
        } else {
            by_core.insert(core.0, groups.clone());
        }
        drop(by_core);
        self.changed.notify_one();
    }
}

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
    /// A record that does not fit the group's log: nothing was written.
    #[error(
        "raft group {} refused a record on OpenRaft core journal '{}': {source}",
        .raft_group_id.0,
        .journal.display()
    )]
    InvalidRecord {
        journal: PathBuf,
        raft_group_id: RaftGroupId,
        #[source]
        source: Arc<io::Error>,
    },
    /// The records replay read for a group do not form a log.
    #[error(
        "raft group {} on OpenRaft core journal '{}' does not replay to a log: {source}",
        .raft_group_id.0,
        .journal.display()
    )]
    InconsistentLog {
        journal: PathBuf,
        raft_group_id: RaftGroupId,
        #[source]
        source: Arc<io::Error>,
    },
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
    #[cfg(not(madsim))]
    #[error("the read of OpenRaft core journal '{}' stopped before it finished", .journal.display())]
    ReadStopped { journal: PathBuf },
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
            CoreJournalError::InvalidRecord { source, .. } => source.kind(),
            CoreJournalError::InconsistentLog { .. } => io::ErrorKind::InvalidData,
            CoreJournalError::WriterPoisoned { cause, .. } => cause.kind(),
            CoreJournalError::WriterStopped { .. } => io::ErrorKind::BrokenPipe,
            #[cfg(not(madsim))]
            CoreJournalError::ReadStopped { .. } => io::ErrorKind::Interrupted,
            CoreJournalError::LockPoisoned => io::ErrorKind::Other,
        };
        io::Error::new(kind, err)
    }
}

/// How a core journal is opened.
#[derive(Debug, Clone)]
pub(crate) struct CoreJournalOptions {
    pub(crate) previous_run: super::run_state::PreviousRun,
    pub(crate) core: CoreId,
    pub(crate) tuning: JournalTuning,
    /// The run's recovery epoch: a journal last read in full in an earlier
    /// epoch is read as a verified prefix.
    pub(crate) recovery_epoch: u64,
    /// Where a poisoned writer records the failure.
    pub(crate) run_state: Arc<RunStateFile>,
    /// Whether the node's logs may be missing entries they acknowledged. A
    /// group whose journal holds entries but whose metadata says it is empty
    /// is recorded recovering in that case, initialized otherwise.
    pub(crate) node_recovery: RecoveryState,
    /// Where the writer reports lagging groups.
    pub(crate) lagging: Arc<LaggingGroups>,
    /// Where the writer records its recovery and its own work.
    pub(crate) metrics: Option<(ShardPlacement, GroupEngineMetrics)>,
}

/// The single writer of one core's journal.
#[derive(Debug)]
pub(crate) struct CoreFileLogWriter {
    #[cfg(madsim)]
    pause: crate::rt::sync::watch::Sender<bool>,
    dir: PathBuf,
    replay_mode: JournalReplayMode,
    pub(crate) previous_run: super::run_state::PreviousRun,
    tx: Option<mpsc::UnboundedSender<CoreWriterRequest>>,
    groups: Arc<Mutex<CoreGroups>>,
    group_cache_bytes: u64,
    worker: Option<WriterWorker>,
    /// Released after the worker has stopped (see `Drop`).
    _lock: DiskLock,
}

/// Every group of a core: its log, and what the metadata file recorded for
/// it, handed out once.
#[derive(Debug, Default)]
struct CoreGroups {
    logs: BTreeMap<u32, Arc<Mutex<GroupLog>>>,
    recorded: BTreeMap<u32, GroupMetadata>,
    opened: BTreeSet<u32>,
}

/// What a group's store opens with.
#[derive(Debug)]
pub(crate) struct OpenedGroup {
    pub(crate) log: Arc<Mutex<GroupLog>>,
    pub(crate) vote: Option<VoteOf<UrsulaRaftTypeConfig>>,
    pub(crate) state: GroupLogState,
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
    reply: WriteReply,
}

/// Where the writer answers one write.
type WriteResult = Result<CoreFileLogWriteTiming, CoreJournalError>;

enum WriteReply {
    Wait(oneshot::Sender<WriteResult>),
    Flush(FlushCompletion),
}

/// Even an aborted writer must release a submitted append with a failure.
struct FlushCompletion {
    callback: Option<Box<dyn FnOnce(WriteResult) + Send>>,
    stopped: CoreJournalError,
}
impl FlushCompletion {
    fn complete(mut self, result: WriteResult) {
        if let Some(callback) = self.callback.take() {
            callback(result);
        }
    }
}
impl Drop for FlushCompletion {
    fn drop(&mut self) {
        if let Some(callback) = self.callback.take() {
            callback(Err(self.stopped.clone()));
        }
    }
}

impl std::fmt::Debug for WriteReply {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("WriteReply")
    }
}

#[derive(Debug)]
pub(crate) enum CoreWriteOp {
    /// A record appended to the journal. When it is the first entry or purge
    /// of an empty group, the metadata file records the group as `first`.
    Record {
        record: CoreJournalRecord,
        first: GroupLogState,
        /// The group's log, which the record joins once it is written.
        log: Arc<Mutex<GroupLog>>,
    },
    /// A group's vote, kept in the metadata file.
    Vote {
        group_id: u32,
        vote: VoteOf<UrsulaRaftTypeConfig>,
    },
    /// An initialized group entering or leaving recovery, kept in the
    /// metadata file.
    LogState { group_id: u32, state: GroupLogState },
}

/// What one request's write cost, as reported to its group's metrics.
#[derive(Debug, Clone, Copy)]
pub(crate) struct CoreFileLogWriteTiming {
    pub(crate) write_ns: u64,
    pub(crate) sync_ns: u64,
    pub(crate) storage: WalStorageSample,
    /// What the group's log holds in memory after a record joined it.
    pub(crate) memory: Option<WalMemorySample>,
}

impl CoreFileLogWriter {
    #[cfg(madsim)]
    pub(crate) fn pause_simulated(&self, paused: bool) {
        self.pause.send_replace(paused);
    }
    #[cfg(madsim)]
    pub(crate) fn abort_simulated(&self) {
        if let Some(worker) = &self.worker {
            worker.abort();
        }
    }

    /// Opens the journal in the core directory `dir`: takes its lock, reads
    /// the core's metadata file, replays every segment into each group's
    /// log and starts the writer.
    pub(crate) fn open(
        dir: PathBuf,
        options: CoreJournalOptions,
    ) -> Result<Arc<Self>, CoreJournalError> {
        create_dir_all_durable(&dir).map_err(|source| CoreJournalError::io(&dir, source))?;
        let lock = acquire_journal_lock(&dir)?;
        let metadata_path = core_metadata_path(&dir);
        let (mut metadata, metadata_missing) = CoreMetadata::load_with_presence(&metadata_path)?;
        let replay_mode = core_replay_mode(metadata.verified_epoch(), options.recovery_epoch);
        let cache_bytes = options.tuning.group_cache_bytes;
        let recovery_started_at = Instant::now();
        let mut logs = BTreeMap::<u32, GroupLog>::new();
        let replayed_groups = std::cell::RefCell::new(std::collections::BTreeSet::new());
        let mut invalid_tail = false;
        let recovered = segment::recover_segments::<WireCodec<CoreJournalRecord>>(
            &dir,
            |segment, loc, record| {
                // Include groups whose first journal append preceded its
                // metadata write in the durable pre-repair gate.
                replayed_groups.borrow_mut().insert(record.group_id);
                logs.entry(record.group_id)
                    .or_insert_with(|| GroupLog::replaying(cache_bytes))
                    .apply(record.record, FramePos { segment, loc }, ApplyMode::Replay)
            },
            || {
                for group in replayed_groups.borrow().iter() {
                    metadata.initialize(*group, GroupLogState::Recovering);
                }
                metadata.mark_recovering();
                metadata.store(&metadata_path)?;
                invalid_tail = true;
                Ok(())
            },
        )?;
        for (group_id, log) in &mut logs {
            log.finish_replay()
                .map_err(|source| CoreJournalError::InconsistentLog {
                    journal: dir.clone(),
                    raft_group_id: RaftGroupId(*group_id),
                    source: Arc::new(source),
                })?;
        }
        let recovery_ns = elapsed_ns(recovery_started_at);
        let recovery_bytes = recovered
            .verified_bytes
            .saturating_add(recovered.dropped_bytes);
        let recovery_live_entries = logs.values().fold(0_u64, |total, log| {
            total.saturating_add(log.indexed_entries())
        });
        if let Some((placement, metrics)) = &options.metrics {
            metrics.record_wal_recovery(
                *placement,
                recovery_ns,
                recovered.frames,
                recovery_bytes,
                recovery_live_entries,
            );
        }
        tracing::info!(
            path = %dir.display(),
            ?replay_mode,
            recovery_ns,
            recovery_records = recovered.frames,
            recovery_bytes,
            recovery_live_entries,
            segments = recovered.segments.len(),
            "recovered OpenRaft core journal"
        );
        if recovered.end != RecoveryEnd::Clean {
            tracing::warn!(
                path = %dir.display(),
                ?replay_mode,
                end = ?recovered.end,
                verified_bytes = recovered.verified_bytes,
                dropped_bytes = recovered.dropped_bytes,
                "cut the OpenRaft core journal after its last verified frame"
            );
        }
        // A verified prefix may hold frames whose `fsync` failed and that
        // only the page cache still has, so every kept segment is written
        // again: every frame it keeps is then on disk.
        if replay_mode == JournalReplayMode::VerifiedPrefix || invalid_tail {
            segment::persist_segments(&dir, &recovered.segments)?;
        }
        // The journal now reads in full in this epoch. A group whose journal
        // holds entries or a purge is initialized, even if a crash came
        // between the journal write and the metadata write. A group that was
        // initialized but whose journal holds nothing of it lost its log (a
        // replaced or wiped journal): it recovers.
        let mut metadata_changed = metadata.set_verified_epoch(options.recovery_epoch);
        // Without metadata, surviving records do not prove the lost vote.
        let damaged = metadata_missing || invalid_tail;
        let repaired = match (options.node_recovery, damaged) {
            (RecoveryState::Normal, false) => GroupLogState::Initialized,
            _ => GroupLogState::Recovering,
        };
        for (group_id, log) in &logs {
            if log.holds_log() {
                metadata_changed |= metadata.initialize(*group_id, repaired);
            }
        }
        if damaged {
            metadata_changed |= metadata.mark_recovering();
        }
        let lost = metadata
            .groups()
            .filter(|(group_id, group)| {
                group.log == GroupLogState::Initialized
                    && !logs.get(group_id).is_some_and(GroupLog::holds_log)
            })
            .map(|(group_id, _)| group_id)
            .collect::<Vec<_>>();
        for group_id in lost {
            tracing::warn!(
                path = %dir.display(),
                raft_group_id = group_id,
                "raft group was initialized on this replica but its journal holds none of its log; \
                 it recovers before it votes again"
            );
            metadata_changed |= metadata.set_log_state(group_id, GroupLogState::Recovering);
        }
        if metadata_changed {
            metadata.store(&metadata_path)?;
        }

        let (active_id, sealed) = match recovered.segments.split_last() {
            Some((active, sealed)) => (
                active.id,
                sealed
                    .iter()
                    .map(|segment| (segment.id, segment.len))
                    .collect::<Vec<_>>(),
            ),
            None => {
                let start = match recovered.end {
                    RecoveryEnd::TornNewest { segment } => segment,
                    RecoveryEnd::Clean | RecoveryEnd::Truncated { .. } => SegmentId::FIRST,
                };
                (start, Vec::new())
            }
        };
        let (active, _fsyncs) = segment::open_segment(&dir, active_id)?;
        let groups = Arc::new(Mutex::new(CoreGroups {
            logs: logs
                .into_iter()
                .map(|(group_id, log)| (group_id, Arc::new(Mutex::new(log))))
                .collect(),
            recorded: metadata.groups().collect(),
            opened: BTreeSet::new(),
        }));
        let (tx, rx) = mpsc::unbounded_channel();
        #[cfg(madsim)]
        let (pause, paused) = crate::rt::sync::watch::channel(false);
        let journal = CoreJournal {
            context: WriterContext {
                #[cfg(madsim)]
                paused,
                name: writer_name(&dir),
                dir: dir.clone(),
                metadata_path,
                core: options.core,
                tuning: options.tuning,
                run_state: options.run_state,
                lagging: options.lagging,
                metrics: options.metrics.map(|(_, metrics)| metrics),
            },
            metadata,
            groups: groups.clone(),
            sealed,
            active,
            read_buf: Vec::new(),
            reclaim_due: true,
            pinned_segments: 0,
            lagging_groups: 0,
        };
        let worker = spawn_core_file_log_writer(Box::new(journal), rx)?;
        Ok(Arc::new(Self {
            #[cfg(madsim)]
            pause,
            dir,
            replay_mode,
            previous_run: options.previous_run,
            tx: Some(tx),
            groups,
            group_cache_bytes: cache_bytes,
            worker: Some(worker),
            _lock: lock,
        }))
    }

    /// The core journal's directory.
    pub(crate) fn dir(&self) -> &Path {
        &self.dir
    }

    /// How the journal was read when it opened.
    pub(crate) fn replay_mode(&self) -> JournalReplayMode {
        self.replay_mode
    }

    /// Hands out a group's log and what the metadata file recorded for it.
    /// A group opens once per writer: a store opened again would miss what
    /// the closed one wrote.
    pub(crate) fn open_group(
        &self,
        raft_group_id: RaftGroupId,
    ) -> Result<OpenedGroup, CoreJournalError> {
        let mut groups = self
            .groups
            .lock()
            .map_err(|_poisoned| CoreJournalError::LockPoisoned)?;
        if !groups.opened.insert(raft_group_id.0) {
            return Err(CoreJournalError::GroupAlreadyOpen {
                journal: self.dir.clone(),
                raft_group_id,
            });
        }
        let cache_bytes = self.group_cache_bytes;
        let log = groups
            .logs
            .entry(raft_group_id.0)
            .or_insert_with(|| Arc::new(Mutex::new(GroupLog::new(cache_bytes))))
            .clone();
        let recorded = groups.recorded.remove(&raft_group_id.0).unwrap_or_default();
        Ok(OpenedGroup {
            log,
            vote: recorded.vote,
            state: recorded.log,
        })
    }

    fn stopped(&self) -> CoreJournalError {
        CoreJournalError::WriterStopped {
            journal: self.dir.clone(),
        }
    }

    fn send(&self, request: CoreWriterRequest) -> Result<(), CoreJournalError> {
        self.tx
            .as_ref()
            .ok_or_else(|| self.stopped())?
            .send(request)
            .map_err(|_closed| self.stopped())
    }

    pub(crate) async fn write(
        &self,
        op: CoreWriteOp,
    ) -> Result<CoreFileLogWriteTiming, CoreJournalError> {
        let (reply, response) = oneshot::channel();
        self.send(CoreWriterRequest::Write(CoreFileLogWrite {
            op,
            reply: WriteReply::Wait(reply),
        }))?;
        response.await.map_err(|_dropped| self.stopped())?
    }

    /// Enqueues an append whose readable memory image is already published.
    /// Completion belongs to the writer, so cancellation of the caller cannot
    /// discard the durability notification or release mutation ordering early.
    pub(crate) fn submit(
        &self,
        op: CoreWriteOp,
        complete: impl FnOnce(WriteResult) + Send + 'static,
    ) {
        let request = CoreWriterRequest::Write(CoreFileLogWrite {
            op,
            reply: WriteReply::Flush(FlushCompletion {
                callback: Some(Box::new(complete)),
                stopped: self.stopped(),
            }),
        });
        match &self.tx {
            Some(tx) => {
                if let Err(error) = tx.send(request)
                    && let CoreWriterRequest::Write(request) = error.0
                {
                    request.reply(Err(self.stopped()));
                }
            }
            None => {
                if let CoreWriterRequest::Write(request) = request {
                    request.reply(Err(self.stopped()));
                }
            }
        }
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

fn acquire_journal_lock(dir: &Path) -> Result<DiskLock, CoreJournalError> {
    let lock_path = dir.join(CORE_JOURNAL_LOCK_FILE);
    match Disk::try_lock(&lock_path).map_err(|source| CoreJournalError::io(&lock_path, source))? {
        LockAttempt::Acquired(lock) => Ok(lock),
        LockAttempt::Held { owner } => Err(CoreJournalError::Locked {
            journal: dir.to_owned(),
            lock: lock_path,
            owner,
        }),
    }
}

/// Names a core's writer after its journal's directory (`core-0`).
fn writer_name(dir: &Path) -> String {
    dir.file_name().map_or_else(
        || "core".to_owned(),
        |name| name.to_string_lossy().into_owned(),
    )
}

/// Production runs the writer on its own thread with a current-thread runtime.
#[cfg(not(madsim))]
fn spawn_core_file_log_writer(
    journal: Box<CoreJournal>,
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
                journal, rx,
            )))
        })
        .map_err(spawn_error)
}

/// The simulator runs the writer as a simulated task.
#[cfg(madsim)]
fn spawn_core_file_log_writer(
    journal: Box<CoreJournal>,
    rx: mpsc::UnboundedReceiver<CoreWriterRequest>,
) -> Result<WriterWorker, CoreJournalError> {
    Ok(crate::rt::spawn(run_core_file_log_writer(journal, rx)))
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
    #[cfg(madsim)]
    paused: crate::rt::sync::watch::Receiver<bool>,
    dir: PathBuf,
    metadata_path: PathBuf,
    core: CoreId,
    tuning: JournalTuning,
    run_state: Arc<RunStateFile>,
    lagging: Arc<LaggingGroups>,
    metrics: Option<GroupEngineMetrics>,
    /// Names the writer in a temporary run-state file.
    name: String,
}

/// The journal an open writer appends to.
#[derive(Debug)]
struct CoreJournal {
    context: WriterContext,
    metadata: CoreMetadata,
    groups: Arc<Mutex<CoreGroups>>,
    /// Sealed segments and their lengths, oldest first.
    sealed: Vec<(SegmentId, u64)>,
    /// The newest segment, which appends go to.
    active: JournalWriter,
    read_buf: Vec<u8>,
    /// Whether the next batch should run a reclaim pass.
    reclaim_due: bool,
    /// What the last reclaim pass found: sealed segments kept only for
    /// lagging groups, and those groups.
    pinned_segments: u64,
    lagging_groups: u64,
}

/// The state of a core journal's writer.
enum WriterState {
    /// Appending to the journal.
    Open(Box<CoreJournal>),
    /// An I/O failure left the journal in doubt. After a failed write the
    /// file may end in a partial frame, and a failed `fsync` may have dropped
    /// dirty pages that a later `fsync` would report as durable. The writer
    /// never touches the file again: every request fails with the cause.
    Poisoned {
        dir: PathBuf,
        cause: Arc<JournalError>,
    },
}

async fn run_core_file_log_writer(
    journal: Box<CoreJournal>,
    mut rx: mpsc::UnboundedReceiver<CoreWriterRequest>,
) {
    // Startup reclaim: segments the recovered groups no longer need go now.
    let mut state = maintain(journal);
    while let Some(first) = rx.recv().await {
        let fsync = match &state {
            WriterState::Open(journal) => journal.context.tuning.fsync,
            WriterState::Poisoned { .. } => WalFsync::Never,
        };
        let batch = collect_batch(&mut rx, first, fsync).await;
        #[cfg(madsim)]
        if let WriterState::Open(journal) = &mut state {
            while *journal.context.paused.borrow() {
                if journal.context.paused.changed().await.is_err() {
                    break;
                }
            }
        }
        state = match state {
            WriterState::Open(journal) => match write_core_log_batch(journal, batch.writes) {
                WriterState::Open(journal) => maintain(journal),
                poisoned @ WriterState::Poisoned { .. } => poisoned,
            },
            WriterState::Poisoned { dir, cause } => {
                refuse_poisoned(&dir, &cause, batch.writes);
                WriterState::Poisoned { dir, cause }
            }
        };
        if let Some(reply) = batch.close {
            if reply.send(close_core_journal(state)).is_err() {
                tracing::trace!("core journal close caller stopped waiting");
            }
            return;
        }
    }
    // Every handle is gone: this core stops. What it wrote becomes durable.
    if let WriterState::Open(mut journal) = state
        && let Err(cause) = journal.active.sync()
    {
        on_journal_poisoned(&journal.context, &cause);
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
pub(crate) fn group_commit_wait(elapsed: Duration) -> Option<Duration> {
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

impl CoreJournal {
    fn active_id(&self) -> SegmentId {
        SegmentId(self.active.sequence())
    }

    /// The journal's size, all segments.
    fn physical_bytes(&self) -> u64 {
        self.sealed
            .iter()
            .fold(self.active.len(), |total, (_, len)| {
                total.saturating_add(*len)
            })
    }

    fn group_log(&self, group_id: u32) -> Option<Arc<Mutex<GroupLog>>> {
        self.groups
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .logs
            .get(&group_id)
            .cloned()
    }

    fn group_logs(&self) -> Vec<(u32, Arc<Mutex<GroupLog>>)> {
        self.groups
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .logs
            .iter()
            .map(|(group_id, log)| (*group_id, log.clone()))
            .collect()
    }

    /// Whether `record` fits its group's `log` as the journal holds it now.
    fn validate(
        &self,
        log: &Mutex<GroupLog>,
        record: &CoreJournalRecord,
    ) -> Result<(), CoreJournalError> {
        lock_log(log)
            .validate(&record.record)
            .map_err(|source| CoreJournalError::InvalidRecord {
                journal: self.context.dir.clone(),
                raft_group_id: RaftGroupId(record.group_id),
                source: Arc::new(source),
            })
    }

    /// Applies a written record to its group's log, and reports what the
    /// log then holds in memory.
    fn apply(
        &self,
        log: &Mutex<GroupLog>,
        record: RaftGroupLogRecord,
        at: FramePos,
    ) -> Result<WalMemorySample, JournalError> {
        let mut log = lock_log(log);
        log.apply(record, at, ApplyMode::Live)
            .map_err(|source| JournalError::RejectedAt {
                path: segment_path(&self.context.dir, at.segment),
                offset: at.loc.offset,
                source,
            })?;
        Ok(WalMemorySample {
            cache_bytes: log.cache_bytes(),
            indexed_entries: log.indexed_entries(),
        })
    }

    /// Seals the active segment and starts the next one. Returns the number
    /// of `fsync`s.
    fn rotate(&mut self) -> Result<u64, JournalError> {
        let (next, fsyncs) = segment::rotate(&self.context.dir, &mut self.active)?;
        let sealed = std::mem::replace(&mut self.active, next);
        self.sealed
            .push((SegmentId(sealed.sequence()), sealed.len()));
        tracing::debug!(
            path = %self.context.dir.display(),
            sealed = sealed.sequence(),
            "rotated the OpenRaft core journal to a new segment"
        );
        Ok(fsyncs)
    }

    /// Rotates once the active segment reached its target size.
    fn rotate_if_full(&mut self, sample: &mut WalJournalSample) -> Result<(), JournalError> {
        if self.active.len() < self.context.tuning.segment_bytes() {
            return Ok(());
        }
        let fsyncs = self.rotate()?;
        sample.rotations = sample.rotations.saturating_add(1);
        sample.fsyncs = sample.fsyncs.saturating_add(fsyncs);
        self.reclaim_due = true;
        Ok(())
    }

    /// One reclaim pass: delete segments no group needs, then rewrite the
    /// small remainders out of the oldest segment when the journal is over
    /// its budget, and report the lagging groups.
    fn reclaim(&mut self, sample: &mut WalJournalSample) -> Reclaim {
        let started_at = Instant::now();
        let active = self.active_id();
        let mut pins = Vec::new();
        let mut live_bytes = 0_u64;
        for (group_id, log) in self.group_logs() {
            let log = lock_log(&log);
            live_bytes = live_bytes.saturating_add(log.live_bytes());
            if let Some(oldest) = log.oldest_segment()
                && oldest < active
            {
                pins.push(GroupPin {
                    group_id,
                    oldest,
                    live_in_oldest: log.live_in(oldest),
                });
            }
        }
        let plan = reclaim::plan(
            JournalShape {
                sealed: &self.sealed,
                active_bytes: self.active.len(),
                live_bytes,
                pins: &pins,
            },
            ReclaimLimits::for_segment_bytes(self.context.tuning.segment_bytes()),
        );
        self.context.lagging.set(self.context.core, &plan.lagging);
        self.lagging_groups = u64::try_from(plan.lagging.len()).unwrap_or(u64::MAX);
        self.pinned_segments = plan.pinned_segments;
        self.reclaim_due = plan.rewrite.is_some();
        let mut outcome = Reclaim::Done;
        if !plan.delete.is_empty() {
            // A segment's deletion becomes durable only after the records
            // that made it unneeded are durable. The purge, truncate or
            // committed record that freed it may still be only in the page
            // cache of the active segment (sealed segments were `fsync`ed
            // when they rotated), and the directory `fsync` below makes the
            // deletion durable. Otherwise a host crash could keep the
            // deletion and lose the purge, leaving a hole in the log.
            match self.active.sync() {
                Ok(fsyncs) => sample.fsyncs = sample.fsyncs.saturating_add(fsyncs),
                Err(error) => return Reclaim::Poisoned(error),
            }
            let (deleted, error) = segment::delete_segments(&self.context.dir, &plan.delete);
            let removed = usize::try_from(deleted.segments).unwrap_or(usize::MAX);
            self.sealed.drain(..removed.min(self.sealed.len()));
            sample.reclaims = sample.reclaims.saturating_add(deleted.segments);
            sample.reclaimed_bytes = sample.reclaimed_bytes.saturating_add(deleted.bytes);
            if deleted.segments != 0 {
                sample.fsyncs = sample.fsyncs.saturating_add(1);
            }
            match error {
                None => {}
                Some(DeleteError::Remove(error)) => {
                    self.reclaim_due = true;
                    outcome = Reclaim::Abandoned(error);
                }
                // A later segment created in this directory relies on its
                // `fsync`, so a failed one stops the writer.
                Some(DeleteError::SyncDir(error)) => return Reclaim::Poisoned(error),
            }
        }
        if let (Reclaim::Done, Some((target, groups))) = (&outcome, plan.rewrite) {
            outcome = self.rewrite(target, &groups, sample);
        }
        sample.reclaim_ns = sample.reclaim_ns.saturating_add(elapsed_ns(started_at));
        outcome
    }

    /// Copies the live records `groups` hold in segment `target` into the
    /// newest segment, at most a pass's worth, then makes the copies
    /// durable and points each group's log at them. A group's entries are
    /// copied in chunks from its newest down, so whatever a pass copied
    /// replays next to entries already present.
    fn rewrite(
        &mut self,
        target: SegmentId,
        groups: &[u32],
        sample: &mut WalJournalSample,
    ) -> Reclaim {
        let path = segment_path(&self.context.dir, target);
        let mut file = match Disk::open_read(&path) {
            Ok(file) => file,
            Err(source) => {
                self.reclaim_due = true;
                return Reclaim::Abandoned(JournalError::io(&path, JournalOp::Open, source));
            }
        };
        let budget = self.context.tuning.rewrite_pass_bytes();
        let chunk_bytes = self.context.tuning.rewrite_chunk_bytes();
        let mut moves = Vec::<Moved>::new();
        let mut copied = 0_u64;
        let mut outcome = Reclaim::Done;
        for group_id in groups {
            if copied >= budget {
                self.reclaim_due = true;
                break;
            }
            let Some(log) = self.group_log(*group_id) else {
                continue;
            };
            let records = lock_log(&log).records_in(target);
            if records.is_empty() {
                continue;
            }
            let reads = disk_reads(&records.entries);
            let entries =
                match read_entries(&mut file, &path, *group_id, &reads, &mut self.read_buf) {
                    Ok(entries) => entries,
                    Err(error) if error.is_io() => {
                        self.reclaim_due = true;
                        outcome = Reclaim::Abandoned(error);
                        break;
                    }
                    Err(error) => return Reclaim::Poisoned(error),
                };
            let positions = records
                .entries
                .iter()
                .map(|entry| (entry.log_id.index, entry.frame))
                .collect::<BTreeMap<_, _>>();
            for chunk in chunk_entries(entries, chunk_bytes).into_iter().rev() {
                if copied >= budget {
                    self.reclaim_due = true;
                    break;
                }
                let indexes = chunk
                    .iter()
                    .map(|entry| entry.log_id.index)
                    .collect::<Vec<_>>();
                let bytes = chunk.iter().fold(0_u64, |total, entry| {
                    total.saturating_add(entry_log_bytes(entry))
                });
                let to = match self.append_rewritten(*group_id, RaftGroupLogRecord::Append(chunk)) {
                    Ok(to) => to,
                    Err(error) => return Reclaim::Poisoned(error),
                };
                for index in indexes {
                    if let Some(from) = positions.get(&index) {
                        moves.push(Moved::Entry {
                            group_id: *group_id,
                            index,
                            from: *from,
                            to,
                        });
                    }
                }
                copied = copied.saturating_add(bytes);
                if let Err(error) = self.rotate_if_full(sample) {
                    return Reclaim::Poisoned(error);
                }
            }
            if let Some(committed) = records.committed {
                match self.append_rewritten(*group_id, RaftGroupLogRecord::SaveCommitted(committed))
                {
                    Ok(to) => moves.push(Moved::Committed {
                        group_id: *group_id,
                        from: target,
                        to: to.segment,
                    }),
                    Err(error) => return Reclaim::Poisoned(error),
                }
            }
            if let Some(purged) = records.purged {
                match self.append_rewritten(*group_id, RaftGroupLogRecord::Purge(purged)) {
                    Ok(to) => moves.push(Moved::Purged {
                        group_id: *group_id,
                        from: target,
                        to: to.segment,
                    }),
                    Err(error) => return Reclaim::Poisoned(error),
                }
            }
        }
        if moves.is_empty() {
            return outcome;
        }
        // The copies must be on disk before anything relies on them: the
        // segment that holds the originals is deleted once nothing points
        // at it.
        match self.active.sync() {
            Ok(fsyncs) => sample.fsyncs = sample.fsyncs.saturating_add(fsyncs),
            Err(error) => return Reclaim::Poisoned(error),
        }
        for moved in moves {
            let Some(log) = self.group_log(moved.group_id()) else {
                continue;
            };
            let mut log = lock_log(&log);
            match moved {
                Moved::Entry {
                    index, from, to, ..
                } => log.relocate(index, from, to),
                Moved::Committed { from, to, .. } => log.relocate_committed(from, to),
                Moved::Purged { from, to, .. } => log.relocate_purged(from, to),
            }
        }
        sample.rewritten_bytes = sample.rewritten_bytes.saturating_add(copied);
        self.reclaim_due = true;
        outcome
    }

    /// Appends a rewritten record of `group_id` to the newest segment.
    fn append_rewritten(
        &mut self,
        group_id: u32,
        record: RaftGroupLogRecord,
    ) -> Result<FramePos, JournalError> {
        let segment = self.active_id();
        let loc = self
            .active
            .append::<WireCodec<CoreJournalRecord>>(&CoreJournalRecord { group_id, record })?;
        if self.active.pending_bytes() >= WRITE_BUFFER_BYTES {
            self.active.flush()?;
        }
        Ok(FramePos { segment, loc })
    }
}

/// A rewritten record whose group's log must point at its copy.
#[derive(Debug, Clone, Copy)]
enum Moved {
    Entry {
        group_id: u32,
        index: u64,
        from: FramePos,
        to: FramePos,
    },
    Committed {
        group_id: u32,
        from: SegmentId,
        to: SegmentId,
    },
    Purged {
        group_id: u32,
        from: SegmentId,
        to: SegmentId,
    },
}

impl Moved {
    fn group_id(&self) -> u32 {
        match self {
            Self::Entry { group_id, .. }
            | Self::Committed { group_id, .. }
            | Self::Purged { group_id, .. } => *group_id,
        }
    }
}

/// How a reclaim pass ended.
#[derive(Debug)]
enum Reclaim {
    Done,
    /// A step failed and left the journal as it was; a later pass retries.
    Abandoned(JournalError),
    /// The journal on disk is in doubt or failed verification.
    Poisoned(JournalError),
}

fn lock_log(log: &Mutex<GroupLog>) -> std::sync::MutexGuard<'_, GroupLog> {
    log.lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// The frames to read for `entries`, a group's live entries in one segment.
fn disk_reads(entries: &[IndexedEntry]) -> Vec<DiskRead> {
    let mut reads: Vec<DiskRead> = Vec::new();
    for entry in entries {
        match reads.last_mut() {
            Some(read) if read.frame == entry.frame => read.log_ids.push(entry.log_id),
            _ => reads.push(DiskRead {
                frame: entry.frame,
                log_ids: vec![entry.log_id],
            }),
        }
    }
    reads
}

/// Reads the entries `reads` names from the segment open as `file`, checking
/// each against the log id the index holds for it.
pub(crate) fn read_entries(
    file: &mut DiskFile,
    path: &Path,
    group_id: u32,
    reads: &[DiskRead],
    buf: &mut Vec<u8>,
) -> Result<Vec<Entry>, JournalError> {
    let mut entries = Vec::new();
    for read in reads {
        let record = journal::read_frame::<WireCodec<CoreJournalRecord>>(
            file,
            path,
            read.frame.segment.0,
            read.frame.loc,
            buf,
        )?;
        let mismatch = |index| JournalError::FrameMismatch {
            path: path.to_owned(),
            offset: read.frame.loc.offset,
            raft_group_id: group_id,
            index,
        };
        let first_wanted = read.log_ids.first().map_or(0, |log_id| log_id.index);
        let RaftGroupLogRecord::Append(frame_entries) = record.record else {
            return Err(mismatch(first_wanted));
        };
        if record.group_id != group_id {
            return Err(mismatch(first_wanted));
        }
        let mut frame_entries = frame_entries.into_iter().peekable();
        for log_id in &read.log_ids {
            let entry = loop {
                match frame_entries.next() {
                    Some(entry) if entry.log_id.index < log_id.index => {}
                    Some(entry) => break Some(entry),
                    None => break None,
                }
            };
            match entry {
                Some(entry) if entry.log_id == *log_id => entries.push(entry),
                _ => return Err(mismatch(log_id.index)),
            }
        }
    }
    Ok(entries)
}

/// Splits consecutive `entries` into runs of about `chunk_bytes` each. An
/// entry larger than that is a run of its own.
fn chunk_entries(entries: Vec<Entry>, chunk_bytes: u64) -> Vec<Vec<Entry>> {
    let mut chunks = Vec::new();
    let mut chunk = Vec::new();
    let mut chunk_len = 0_u64;
    let mut previous: Option<u64> = None;
    for entry in entries {
        let index = entry.log_id.index;
        let bytes = entry_log_bytes(&entry);
        let consecutive = previous.is_none_or(|previous| previous.checked_add(1) == Some(index));
        if !chunk.is_empty() && (!consecutive || chunk_len.saturating_add(bytes) > chunk_bytes) {
            chunks.push(std::mem::take(&mut chunk));
            chunk_len = 0;
        }
        chunk.push(entry);
        chunk_len = chunk_len.saturating_add(bytes);
        previous = Some(index);
    }
    if !chunk.is_empty() {
        chunks.push(chunk);
    }
    chunks
}

/// Writes one batch: encodes every record, writes the frames, `fsync`s them
/// when the policy and a record need it, replaces the metadata file when a
/// vote or a newly initialized group changed it, applies each record to its
/// group's log, and replies. Returns the writer's next state.
fn write_core_log_batch(
    mut journal: Box<CoreJournal>,
    batch: Vec<CoreFileLogWrite>,
) -> WriterState {
    let write_started_at = Instant::now();
    let mut accepted = Vec::with_capacity(batch.len());
    let mut flushed = Ok(());
    let mut journal_records = 0_u64;
    let mut metadata_ops = 0_u64;
    let mut requires_sync = false;
    let mut metadata_changed = false;
    let mut batch = batch.into_iter();
    while let Some(request) = batch.next() {
        let position = match &request.op {
            CoreWriteOp::Vote { group_id, vote } => {
                metadata_ops = metadata_ops.saturating_add(1);
                metadata_changed |= journal.metadata.set_vote(*group_id, *vote);
                None
            }
            CoreWriteOp::LogState { group_id, state } => {
                metadata_ops = metadata_ops.saturating_add(1);
                metadata_changed |= journal.metadata.set_log_state(*group_id, *state);
                None
            }
            CoreWriteOp::Record { record, first, log } => {
                if let Err(error) = journal.validate(log, record) {
                    if matches!(request.reply, WriteReply::Flush(_)) {
                        let cause = JournalError::Io {
                            path: journal.context.dir.clone(),
                            op: JournalOp::Append,
                            source: error.into(),
                        };
                        let mut failed = accepted
                            .into_iter()
                            .map(|(request, _)| request)
                            .collect::<Vec<_>>();
                        failed.push(request);
                        failed.extend(batch);
                        return poison_core_journal(&journal.context, cause, failed);
                    }
                    request.reply(Err(error));
                    continue;
                }
                let mut position = None;
                if flushed.is_ok() {
                    let segment = journal.active_id();
                    match journal
                        .active
                        .append::<WireCodec<CoreJournalRecord>>(record)
                    {
                        Ok(loc) => position = Some((FramePos { segment, loc }, log.clone())),
                        Err(too_large) => {
                            if matches!(request.reply, WriteReply::Flush(_)) {
                                let cause = JournalError::Io {
                                    path: journal.context.dir.clone(),
                                    op: JournalOp::Append,
                                    source: io::Error::new(io::ErrorKind::InvalidInput, too_large),
                                };
                                let mut failed = accepted
                                    .into_iter()
                                    .map(|(request, _)| request)
                                    .collect::<Vec<_>>();
                                failed.push(request);
                                failed.extend(batch);
                                return poison_core_journal(&journal.context, cause, failed);
                            }
                            request.reply(Err(CoreJournalError::RecordTooLarge(too_large)));
                            continue;
                        }
                    }
                    if journal.active.pending_bytes() >= WRITE_BUFFER_BYTES {
                        flushed = journal.active.flush();
                    }
                }
                journal_records = journal_records.saturating_add(1);
                requires_sync |= raft_group_log_record_requires_sync(
                    &record.record,
                    journal.context.tuning.fsync,
                );
                if raft_group_log_record_initializes(&record.record) {
                    metadata_changed |= journal.metadata.initialize(record.group_id, *first);
                }
                position
            }
        };
        accepted.push((request, position));
    }
    if accepted.is_empty() {
        return WriterState::Open(journal);
    }
    // A purge or truncate may leave an old segment without live records.
    journal.reclaim_due |= accepted.iter().any(|(request, _)| {
        matches!(
            &request.op,
            CoreWriteOp::Record { record, .. } if matches!(
                record.record,
                RaftGroupLogRecord::Purge(_) | RaftGroupLogRecord::TruncateAfter(_)
            )
        )
    });
    let written = flushed
        .and_then(|()| journal.active.flush())
        .and_then(|()| {
            let write_ns = elapsed_ns(write_started_at);
            if !requires_sync && !metadata_changed {
                return Ok((write_ns, 0, 0));
            }
            let sync_started_at = Instant::now();
            let mut fsyncs = 0_u64;
            if requires_sync {
                fsyncs = fsyncs.saturating_add(journal.active.sync()?);
            }
            if metadata_changed {
                fsyncs =
                    fsyncs.saturating_add(journal.metadata.store(&journal.context.metadata_path)?);
            }
            Ok((write_ns, elapsed_ns(sync_started_at), fsyncs))
        });
    let (write_ns, sync_ns, fsyncs) = match written {
        Ok(written) => written,
        Err(cause) => {
            let requests = accepted.into_iter().map(|(request, _)| request).collect();
            return poison_core_journal(&journal.context, cause, requests);
        }
    };

    let fsync_records = match (requires_sync, metadata_changed) {
        (true, true) => journal_records.saturating_add(metadata_ops),
        (true, false) => journal_records,
        (false, true) => metadata_ops,
        (false, false) => 0,
    };
    let storage = WalStorageSample {
        fsyncs,
        fsync_records,
        physical_bytes: journal.physical_bytes(),
    };
    // The batch is durable: each record now joins its group's log.
    let mut replies = Vec::with_capacity(accepted.len());
    let mut rejected = None;
    for (CoreFileLogWrite { op, reply }, position) in accepted {
        let applied = match (op, position) {
            (CoreWriteOp::Record { record, .. }, Some((position, log))) => {
                journal.apply(&log, record.record, position).map(Some)
            }
            _ => Ok(None),
        };
        match applied {
            Ok(memory) => replies.push((reply, memory)),
            Err(cause) => {
                let cause = Arc::new(cause);
                send_reply(
                    reply,
                    Err(CoreJournalError::WriterPoisoned {
                        journal: journal.context.dir.clone(),
                        cause: cause.clone(),
                    }),
                );
                rejected = Some(cause);
            }
        }
    }
    reply_core_log_batch(replies, write_ns, sync_ns, storage);
    match rejected {
        // The journal holds a record its group's log refused, so the two
        // disagree: stop.
        Some(cause) => {
            on_journal_poisoned(&journal.context, &cause);
            WriterState::Poisoned {
                dir: journal.context.dir.clone(),
                cause,
            }
        }
        None => WriterState::Open(journal),
    }
}

/// Rotates a full segment and runs a reclaim pass when one is due, after
/// the batch was acknowledged, and records what they did.
fn maintain(mut journal: Box<CoreJournal>) -> WriterState {
    let mut sample = WalJournalSample::default();
    let mut outcome = match journal.rotate_if_full(&mut sample) {
        Ok(()) => Reclaim::Done,
        Err(error) => Reclaim::Poisoned(error),
    };
    if matches!(outcome, Reclaim::Done) && journal.reclaim_due {
        outcome = journal.reclaim(&mut sample);
    }
    sample.physical_bytes = journal.physical_bytes();
    sample.segments = u64::try_from(journal.sealed.len())
        .unwrap_or(u64::MAX)
        .saturating_add(1);
    sample.pinned_segments = journal.pinned_segments;
    sample.lagging_groups = journal.lagging_groups;
    match outcome {
        Reclaim::Done => {}
        Reclaim::Abandoned(error) => {
            sample.reclaim_failures = sample.reclaim_failures.saturating_add(1);
            tracing::error!(
                path = %journal.context.dir.display(),
                %error,
                "reclaim of the OpenRaft core journal stopped; the journal is unchanged and a \
                 later pass retries"
            );
        }
        Reclaim::Poisoned(error) => {
            sample.reclaim_failures = sample.reclaim_failures.saturating_add(1);
            if let Some(metrics) = &journal.context.metrics {
                metrics.record_wal_journal(journal.context.core, sample);
            }
            let cause = Arc::new(error);
            on_journal_poisoned(&journal.context, &cause);
            return WriterState::Poisoned {
                dir: journal.context.dir.clone(),
                cause,
            };
        }
    }
    if let Some(metrics) = &journal.context.metrics {
        metrics.record_wal_journal(journal.context.core, sample);
    }
    WriterState::Open(journal)
}

/// Makes everything written durable before the writer stops. A failed
/// `fsync` poisons the journal as it would during a batch.
fn close_core_journal(state: WriterState) -> Result<(), CoreJournalError> {
    let (journal, cause) = match state {
        WriterState::Open(mut journal) => match journal.active.sync() {
            Ok(_fsyncs) => return Ok(()),
            Err(cause) => {
                let cause = Arc::new(cause);
                on_journal_poisoned(&journal.context, &cause);
                (journal.context.dir.clone(), cause)
            }
        },
        WriterState::Poisoned { dir, cause } => (dir, cause),
    };
    Err(CoreJournalError::WriterPoisoned { journal, cause })
}

/// Fails `batch` with `cause` and poisons the writer.
fn poison_core_journal(
    context: &WriterContext,
    cause: JournalError,
    batch: Vec<CoreFileLogWrite>,
) -> WriterState {
    let cause = Arc::new(cause);
    refuse_poisoned(&context.dir, &cause, batch);
    on_journal_poisoned(context, &cause);
    WriterState::Poisoned {
        dir: context.dir.clone(),
        cause,
    }
}

/// Fails every request of `batch` because the writer is poisoned by `cause`.
fn refuse_poisoned(dir: &Path, cause: &Arc<JournalError>, batch: Vec<CoreFileLogWrite>) {
    for request in batch {
        request.reply(Err(CoreJournalError::WriterPoisoned {
            journal: dir.to_owned(),
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
        path = %context.dir.display(),
        error = %cause,
        "OpenRaft core journal failed; the writer is poisoned and the process stops"
    );
    // Best effort: the disk that just failed may refuse this write too.
    if let Err(err) = context
        .run_state
        .record(RunStatus::Poisoned, &format!("poisoned-{}", context.name))
    {
        tracing::error!(
            path = %context.dir.display(),
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
    replies: Vec<(WriteReply, Option<WalMemorySample>)>,
    write_ns: u64,
    sync_ns: u64,
    storage: WalStorageSample,
) {
    let count = u64::try_from(replies.len()).unwrap_or(u64::MAX);
    for (request_index, (reply, memory)) in replies.into_iter().enumerate() {
        let storage = if request_index == 0 {
            storage
        } else {
            WalStorageSample {
                physical_bytes: storage.physical_bytes,
                ..WalStorageSample::default()
            }
        };
        send_reply(
            reply,
            Ok(CoreFileLogWriteTiming {
                write_ns: write_ns.checked_div(count).unwrap_or(write_ns),
                sync_ns: sync_ns.checked_div(count).unwrap_or(sync_ns),
                storage,
                memory,
            }),
        );
    }
}

fn send_reply(reply: WriteReply, result: Result<CoreFileLogWriteTiming, CoreJournalError>) {
    match reply {
        WriteReply::Wait(reply) => {
            if reply.send(result).is_err() {
                tracing::trace!("raft log append caller stopped waiting");
            }
        }
        WriteReply::Flush(complete) => complete.complete(result),
    }
}

impl CoreFileLogWrite {
    fn reply(self, result: Result<CoreFileLogWriteTiming, CoreJournalError>) {
        send_reply(self.reply, result);
    }
}

/// Frames Raft log records as length-delimited MessagePack for the shared
/// journal (see [`crate::codec::encode_wire`]).
pub(crate) struct WireCodec<T>(PhantomData<T>);

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

/// Whether a journal record must reach stable storage before acknowledgment.
/// Membership batches always sync, including under `never`: losing every
/// bootstrap membership leaves no voter set from which to recover.
/// Ordinary appends and purge follow the configured policy.
///
/// Committed and truncate markers are replay optimizations. Losing either in a
/// crash leaves the durable entries intact and OpenRaft re-establishes the
/// marker after restart. Reclaim syncs any outstanding purge markers before
/// deleting the segments they cover, under both policies. Votes are not journal
/// records: the metadata file holds them and is always `fsync`ed.
pub(crate) fn raft_group_log_record_requires_sync(
    record: &RaftGroupLogRecord,
    fsync: WalFsync,
) -> bool {
    match record {
        RaftGroupLogRecord::Append(entries) => {
            fsync == WalFsync::Always
                || entries
                    .iter()
                    .any(|entry| matches!(entry.payload, openraft::EntryPayload::Membership(_)))
        }
        RaftGroupLogRecord::Purge(_) => fsync == WalFsync::Always,
        RaftGroupLogRecord::SaveCommitted(_) | RaftGroupLogRecord::TruncateAfter(_) => false,
    }
}

/// Whether this record shows the group was initialized on this replica: it
/// persists an entry or a purge (which follows a snapshot).
pub(crate) fn raft_group_log_record_initializes(record: &RaftGroupLogRecord) -> bool {
    match record {
        RaftGroupLogRecord::Append(entries) => !entries.is_empty(),
        RaftGroupLogRecord::Purge(_) => true,
        RaftGroupLogRecord::SaveCommitted(_) | RaftGroupLogRecord::TruncateAfter(_) => false,
    }
}

pub(crate) fn elapsed_ns(started_at: Instant) -> u64 {
    u64::try_from(started_at.elapsed().as_nanos()).unwrap_or(u64::MAX)
}

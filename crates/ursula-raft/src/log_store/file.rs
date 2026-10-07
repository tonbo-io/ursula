//! The durable OpenRaft log store of one raft group, over its core's journal.
//!
//! The store sends every record to the core's writer (`writer`) and returns
//! once the writer acknowledges it, by which time the writer applied it to
//! the group's [`GroupLog`]. Reads come from that log: the log state and the
//! markers from memory, recent entries from its cache, and older entries
//! from disk. A disk read runs on Tokio's blocking pool (inline under
//! `cfg(madsim)`, where the disk is simulated), so a read of a lagging
//! follower's entries never blocks the core's async tasks. A segment a
//! reclaim deleted between planning a read and opening it is planned again:
//! the rewrite that freed it moved the entries first.

use std::fmt::Debug;
use std::io;
use std::ops::Bound;
use std::ops::RangeBounds;
use std::ops::RangeInclusive;
use std::path::Path;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::MutexGuard;

use openraft::OptionalSend;
use openraft::alias::EntryOf;
use openraft::alias::LogIdOf;
use openraft::alias::VoteOf;
use openraft::storage::IOFlushed;
use openraft::storage::LogState;
use openraft::storage::RaftLogReader;
use openraft::storage::RaftLogStorage;
use openraft::vote::RaftLeaderId;
use ursula_runtime::GroupEngineMetrics;
use ursula_runtime::WalReadSample;
use ursula_shard::ShardPlacement;

use super::CoreJournalRecord;
use super::RaftGroupLogRecord;
use super::core_meta::GroupLogState;
use super::disk::Disk;
use super::disk::JournalDisk;
use super::group_log::DiskRead;
use super::group_log::GroupLog;
use super::journal::JournalError;
use super::journal::JournalOp;
use super::journal::JournalReplayMode;
use super::segment::segment_path;
use super::writer::CoreFileLogWriteTiming;
use super::writer::CoreFileLogWriter;
use super::writer::CoreJournalError;
use super::writer::CoreWriteOp;
use super::writer::raft_group_log_record_count;
use super::writer::raft_group_log_record_initializes;
use super::writer::read_entries;
use crate::types::UrsulaRaftTypeConfig;

type Entry = EntryOf<UrsulaRaftTypeConfig>;

/// A read finds a segment a reclaim deleted only after the rewrite that
/// freed it moved the entries, so a second plan finds them; more attempts
/// cover reclaims racing again.
const READ_ATTEMPTS: usize = 4;
/// Bytes of entries a limited read takes from disk, as replication reads
/// a bounded batch at a time.
const LIMITED_READ_DISK_BYTES: u64 = 8 * 1024 * 1024;

/// One raft group's durable OpenRaft log, stored in its core's shared journal.
#[derive(Debug)]
pub struct RaftGroupFileLogStore {
    placement: ShardPlacement,
    metrics: GroupEngineMetrics,
    log: Arc<Mutex<GroupLog>>,
    vote: Mutex<Option<VoteOf<UrsulaRaftTypeConfig>>>,
    /// Mirrors the group's durable log state.
    store_log: Mutex<StoreLog>,
    /// Serializes mutations, so the journal records them in the order
    /// OpenRaft issued them and the writer holds at most one per group.
    write_order: crate::rt::sync::Mutex<()>,
    core_writer: Arc<CoreFileLogWriter>,
}

/// Whether an empty group's history on this replica is known.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EmptyHistory {
    /// The group starts here: its first entry records it initialized. An
    /// empty store starts so; it is a new group, or no recovery gate guards
    /// it, or its gate is open.
    New,
    /// The replica may have held the group before, on a disk it lost: its
    /// first entry records it recovering, until its recovery gate opens.
    Unknown,
}

/// A group's log state as its store last recorded it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StoreLog {
    Empty(EmptyHistory),
    Initialized,
    Recovering,
}

impl StoreLog {
    fn new(log: GroupLogState) -> Self {
        match log {
            GroupLogState::Empty => Self::Empty(EmptyHistory::New),
            GroupLogState::Initialized => Self::Initialized,
            GroupLogState::Recovering => Self::Recovering,
        }
    }

    fn durable(self) -> GroupLogState {
        match self {
            Self::Empty(_) => GroupLogState::Empty,
            Self::Initialized => GroupLogState::Initialized,
            Self::Recovering => GroupLogState::Recovering,
        }
    }

    /// What the group's first entry or purge records.
    fn first_entry(self) -> GroupLogState {
        match self {
            Self::Empty(EmptyHistory::New) | Self::Initialized => GroupLogState::Initialized,
            Self::Empty(EmptyHistory::Unknown) | Self::Recovering => GroupLogState::Recovering,
        }
    }
}

impl RaftGroupFileLogStore {
    pub(crate) fn open(
        placement: ShardPlacement,
        metrics: GroupEngineMetrics,
        core_writer: Arc<CoreFileLogWriter>,
    ) -> Result<Arc<Self>, CoreJournalError> {
        let opened = core_writer.open_group(placement.raft_group_id)?;
        Ok(Arc::new(Self {
            placement,
            metrics,
            log: opened.log,
            vote: Mutex::new(opened.vote),
            store_log: Mutex::new(StoreLog::new(opened.state)),
            write_order: crate::rt::sync::Mutex::new(()),
            core_writer,
        }))
    }

    /// The group's durable log state on this replica.
    pub fn log_state(&self) -> GroupLogState {
        self.store_log().durable()
    }

    /// Whether this replica ever persisted an entry or a purge of the group.
    /// It never becomes false again, even when a crash later costs the log
    /// its entries.
    pub fn initialized(&self) -> bool {
        self.log_state().is_initialized()
    }

    /// The id of the last entry the store holds, or of the last purged one.
    pub(crate) fn last_log_id(&self) -> Option<LogIdOf<UrsulaRaftTypeConfig>> {
        lock(&self.log).last_log_id()
    }

    /// The vote the replica runs with.
    pub(crate) fn vote(&self) -> Option<VoteOf<UrsulaRaftTypeConfig>> {
        *lock(&self.vote)
    }

    fn store_log(&self) -> StoreLog {
        *lock(&self.store_log)
    }

    fn set_store_log(&self, log: StoreLog) {
        *lock(&self.store_log) = log;
    }

    /// The group's recovery gate is closed: while the store is empty, its
    /// first entry records the group recovering, because the replica may
    /// have held the group on a disk it lost.
    pub(crate) fn hold_unknown_history(&self) {
        let mut log = lock(&self.store_log);
        if *log == StoreLog::Empty(EmptyHistory::New) {
            *log = StoreLog::Empty(EmptyHistory::Unknown);
        }
    }

    /// A recovering replica that led its group may have lost entries it
    /// appended as leader while its followers kept them. OpenRaft restores a
    /// replica whose vote is a committed vote for itself as the leader of
    /// that term, which would append new entries under the log ids of those
    /// lost ones and silently fork the group's log. The replica's vote for
    /// itself is therefore replaced by the same vote uncommitted, so it
    /// starts as a follower that already voted in that term.
    ///
    /// The demotion is recorded in the metadata file like any vote, and the
    /// replica runs with it only once it is durable. Call it before the
    /// group's Raft core starts. No later start finds the committed vote
    /// again, whether it follows a clean shutdown or the gate opening. Any
    /// other vote is left as it is, and nothing is written.
    pub(crate) async fn start_as_follower(&self, node_id: u64) -> Result<(), CoreJournalError> {
        let _order = self.write_order.lock().await;
        let Some(current) = self.vote() else {
            return Ok(());
        };
        if !current.is_committed() || *current.leader_id().node_id() != node_id {
            return Ok(());
        }
        self.record_vote(VoteOf::<UrsulaRaftTypeConfig>::new(
            current.leader_id().term(),
            node_id,
        ))
        .await
    }

    /// Records `vote` in the metadata file, which is always `fsync`ed, and
    /// then runs with it. Call with `write_order` held.
    async fn record_vote(
        &self,
        vote: VoteOf<UrsulaRaftTypeConfig>,
    ) -> Result<(), CoreJournalError> {
        if self.vote() == Some(vote) {
            return Ok(());
        }
        let timing = self
            .core_writer
            .write(CoreWriteOp::Vote {
                group_id: self.placement.raft_group_id.0,
                vote,
            })
            .await?;
        self.record_timing(1, timing);
        *lock(&self.vote) = Some(vote);
        Ok(())
    }

    /// The group's recovery gate opened: a recovering group is durably
    /// initialized again, and an empty group's first entry records it
    /// initialized.
    pub(crate) async fn record_recovered(&self) -> Result<(), CoreJournalError> {
        let _order = self.write_order.lock().await;
        match self.store_log() {
            StoreLog::Initialized | StoreLog::Empty(EmptyHistory::New) => Ok(()),
            StoreLog::Empty(EmptyHistory::Unknown) => {
                self.set_store_log(StoreLog::Empty(EmptyHistory::New));
                Ok(())
            }
            StoreLog::Recovering => {
                let timing = self
                    .core_writer
                    .write(CoreWriteOp::LogState {
                        group_id: self.placement.raft_group_id.0,
                        state: GroupLogState::Initialized,
                    })
                    .await?;
                self.record_timing(1, timing);
                self.set_store_log(StoreLog::Initialized);
                Ok(())
            }
        }
    }

    /// How the core journal holding this group was read when it opened.
    pub fn journal_replay_mode(&self) -> JournalReplayMode {
        self.core_writer.replay_mode()
    }

    fn lock_log(&self) -> Result<MutexGuard<'_, GroupLog>, CoreJournalError> {
        self.log
            .lock()
            .map_err(|_poisoned| CoreJournalError::LockPoisoned)
    }

    /// Journals `record` and waits until the writer acknowledges it. Call
    /// with `write_order` held.
    async fn append_record(&self, record: RaftGroupLogRecord) -> Result<(), CoreJournalError> {
        let record_count = raft_group_log_record_count(&record);
        let initializes = raft_group_log_record_initializes(&record);
        let store_log = self.store_log();
        let first = store_log.first_entry();
        let timing = self
            .core_writer
            .write(CoreWriteOp::Record {
                record: CoreJournalRecord {
                    group_id: self.placement.raft_group_id.0,
                    record,
                },
                first,
                log: self.log.clone(),
            })
            .await?;
        if initializes && let StoreLog::Empty(_) = store_log {
            self.set_store_log(StoreLog::new(first));
        }
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
        if let Some(memory) = timing.memory {
            self.metrics.record_wal_memory(self.placement, memory);
        }
    }

    /// Reads the entries of `range`: cached ones from memory, older ones
    /// from disk, at most about `max_disk_bytes` of those when it is set.
    async fn read_entries(
        &self,
        range: (Bound<u64>, Bound<u64>),
        max_disk_bytes: Option<u64>,
    ) -> Result<Vec<Entry>, CoreJournalError> {
        let mut attempt = 0_usize;
        loop {
            attempt = attempt.saturating_add(1);
            let plan = self.lock_log()?.plan_read(range, max_disk_bytes);
            let mut sample = WalReadSample {
                cache_hits: u64::try_from(plan.cached.len()).unwrap_or(u64::MAX),
                ..WalReadSample::default()
            };
            if plan.disk.is_empty() {
                self.metrics.record_wal_read(self.placement, sample);
                return Ok(plan.cached);
            }
            let dir = self.core_writer.dir().to_owned();
            let group_id = self.placement.raft_group_id.0;
            let reads = plan.disk;
            let read = run_blocking(&dir, {
                let dir = dir.clone();
                move || read_from_disk(&dir, group_id, &reads)
            })
            .await?;
            match read {
                Ok(read) => {
                    sample.cache_misses = u64::try_from(read.entries.len()).unwrap_or(u64::MAX);
                    sample.disk_reads = read.frames;
                    sample.disk_read_bytes = read.bytes;
                    self.metrics.record_wal_read(self.placement, sample);
                    let mut entries = read.entries;
                    entries.extend(plan.cached);
                    entries.sort_by_key(|entry| entry.log_id.index);
                    return Ok(entries);
                }
                Err(error) if segment_gone(&error) && attempt < READ_ATTEMPTS => {
                    tracing::debug!(
                        raft_group_id = group_id,
                        %error,
                        "a reclaim removed a journal segment during a read; planning it again"
                    );
                }
                Err(error) => return Err(error.into()),
            }
        }
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// Entries read from disk, and the frames and bytes read for them.
#[derive(Debug)]
struct DiskEntries {
    entries: Vec<Entry>,
    frames: u64,
    bytes: u64,
}

/// Reads `reads`, a group's frames in index order, from the core journal in
/// `dir`.
fn read_from_disk(
    dir: &Path,
    group_id: u32,
    reads: &[DiskRead],
) -> Result<DiskEntries, JournalError> {
    let mut entries = Vec::new();
    let mut buf = Vec::new();
    let mut frames = 0_u64;
    let mut bytes = 0_u64;
    let mut start = 0_usize;
    while let Some(first) = reads.get(start) {
        let segment = first.frame.segment;
        let end = reads
            .iter()
            .skip(start)
            .position(|read| read.frame.segment != segment)
            .map_or(reads.len(), |offset| start.saturating_add(offset));
        let batch = reads.get(start..end).unwrap_or_default();
        let path = segment_path(dir, segment);
        let mut file = Disk::open_read(&path)
            .map_err(|source| JournalError::io(&path, JournalOp::Open, source))?;
        entries.extend(read_entries(&mut file, &path, group_id, batch, &mut buf)?);
        for read in batch {
            frames = frames.saturating_add(1);
            bytes = bytes.saturating_add(read.frame.loc.file_bytes());
        }
        start = end;
    }
    Ok(DiskEntries {
        entries,
        frames,
        bytes,
    })
}

/// Whether a read failed because a reclaim deleted the segment it planned
/// to read.
fn segment_gone(error: &JournalError) -> bool {
    matches!(
        error,
        JournalError::Io { op: JournalOp::Open, source, .. } if source.kind() == io::ErrorKind::NotFound
    )
}

/// Runs blocking disk I/O off the async workers: on Tokio's blocking pool,
/// or inline outside a Tokio runtime.
#[cfg(not(madsim))]
async fn run_blocking<T: Send + 'static>(
    dir: &Path,
    work: impl FnOnce() -> T + Send + 'static,
) -> Result<T, CoreJournalError> {
    match tokio::runtime::Handle::try_current() {
        Ok(runtime) => {
            runtime
                .spawn_blocking(work)
                .await
                .map_err(|_join| CoreJournalError::ReadStopped {
                    journal: dir.to_owned(),
                })
        }
        Err(_no_runtime) => Ok(work()),
    }
}

/// The simulated disk is in memory, so a read runs inline.
#[cfg(madsim)]
async fn run_blocking<T: Send + 'static>(
    _dir: &Path,
    work: impl FnOnce() -> T + Send + 'static,
) -> Result<T, CoreJournalError> {
    Ok(work())
}

fn bounds(range: &impl RangeBounds<u64>) -> (Bound<u64>, Bound<u64>) {
    (range.start_bound().cloned(), range.end_bound().cloned())
}

impl RaftLogReader<UrsulaRaftTypeConfig> for Arc<RaftGroupFileLogStore> {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry>, io::Error> {
        let entries = self.read_entries(bounds(&range), None).await?;
        super::ensure_consecutive_entries::<UrsulaRaftTypeConfig>(&entries)?;
        Ok(entries)
    }

    async fn read_vote(&mut self) -> Result<Option<VoteOf<UrsulaRaftTypeConfig>>, io::Error> {
        Ok(*lock(&self.vote))
    }

    async fn limited_get_log_entries(
        &mut self,
        start: u64,
        end: u64,
    ) -> Result<Vec<Entry>, io::Error> {
        let entries = self
            .read_entries(
                (Bound::Included(start), Bound::Excluded(end)),
                Some(LIMITED_READ_DISK_BYTES),
            )
            .await?;
        super::ensure_consecutive_entries::<UrsulaRaftTypeConfig>(&entries)?;
        Ok(entries)
    }

    /// The index holds every entry's log id, so no entry is read.
    async fn get_key_log_ids(
        &mut self,
        range: RangeInclusive<LogIdOf<UrsulaRaftTypeConfig>>,
    ) -> Result<Vec<LogIdOf<UrsulaRaftTypeConfig>>, io::Error> {
        Ok(self
            .lock_log()?
            .key_log_ids(range.start().index, range.end().index))
    }
}

impl RaftLogStorage<UrsulaRaftTypeConfig> for Arc<RaftGroupFileLogStore> {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<UrsulaRaftTypeConfig>, io::Error> {
        let log = self.lock_log()?;
        Ok(LogState {
            last_purged_log_id: log.last_purged(),
            last_log_id: log.last_log_id(),
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    /// Votes go to the core's metadata file, which is always `fsync`ed.
    async fn save_vote(&mut self, vote: &VoteOf<UrsulaRaftTypeConfig>) -> Result<(), io::Error> {
        let _order = self.write_order.lock().await;
        self.record_vote(*vote).await?;
        Ok(())
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogIdOf<UrsulaRaftTypeConfig>>,
    ) -> Result<(), io::Error> {
        let _order = self.write_order.lock().await;
        if self.lock_log()?.committed() == committed {
            return Ok(());
        }
        self.append_record(RaftGroupLogRecord::SaveCommitted(committed))
            .await?;
        Ok(())
    }

    async fn read_committed(&mut self) -> Result<Option<LogIdOf<UrsulaRaftTypeConfig>>, io::Error> {
        Ok(self.lock_log()?.committed())
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: IOFlushed<UrsulaRaftTypeConfig>,
    ) -> Result<(), io::Error>
    where
        I: IntoIterator<Item = Entry> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let entries = entries.into_iter().collect::<Vec<_>>();
        let _order = self.write_order.lock().await;
        if let Err(err) = self
            .append_record(RaftGroupLogRecord::Append(entries))
            .await
        {
            callback.io_completed(Err(err.clone().into()));
            return Err(err.into());
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
        Ok(())
    }

    async fn purge(&mut self, log_id: LogIdOf<UrsulaRaftTypeConfig>) -> Result<(), io::Error> {
        let _order = self.write_order.lock().await;
        self.append_record(RaftGroupLogRecord::Purge(log_id))
            .await?;
        Ok(())
    }
}

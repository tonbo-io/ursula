use std::fmt::Debug;
use std::fs;
use std::io;
use std::marker::PhantomData;
use std::ops::RangeBounds;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::MutexGuard;
use std::sync::mpsc;

use openraft::OptionalSend;
use openraft::alias::EntryOf;
use openraft::alias::LogIdOf;
use openraft::alias::VoteOf;
use openraft::storage::IOFlushed;
use openraft::storage::LogState;
use openraft::storage::RaftLogReader;
use openraft::storage::RaftLogStorage;
use prost::Message;
use ursula_runtime::GroupEngineMetrics;
use ursula_runtime::journal;
use ursula_shard::ShardPlacement;

use super::CoreJournalRecord;
use super::RaftGroupLogRecord;
use super::RaftGroupLogStoreInner;
use super::StoredLogEntry;
use super::append_record;
use super::ensure_consecutive_entries;
use super::ensure_log_append_boundary;
use super::log_id_from_required_proto;
use super::purge_record;
use super::save_committed_record;
use super::save_vote_record;
use super::stored_log_entry_into_entry;
use super::truncate_after_record;
use super::vote_from_required_proto;
use crate::engine::invalid_data;
use crate::raft_internal_proto;
use crate::rt::time::Instant;
use crate::types::CORE_LOG_GROUP_COMMIT_DELAY;
use crate::types::CORE_LOG_GROUP_COMMIT_MAX_BATCH;
use crate::types::UrsulaRaftTypeConfig;

#[derive(Debug)]
pub struct RaftGroupFileLogStore {
    path: PathBuf,
    inner: Mutex<RaftGroupLogStoreInner>,
    file: Mutex<RaftGroupFileLogHandle>,
    metrics: Option<RaftGroupFileLogStoreMetrics>,
    core_writer: Option<Arc<CoreFileLogWriter>>,
}

#[derive(Debug, Clone)]
pub(crate) struct RaftGroupFileLogStoreMetrics {
    placement: ShardPlacement,
    metrics: GroupEngineMetrics,
}

/// Raft log writes frame protobuf records into the shared append-only journal.
type RaftGroupFileLogHandle = journal::JournalWriter;

#[derive(Debug)]
pub(crate) struct CoreFileLogWriter {
    journal_path: PathBuf,
    tx: mpsc::Sender<CoreFileLogWrite>,
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
        let parent_needs_sync = !path.exists();
        let inner = match (&core_writer, &metrics) {
            (Some(writer), Some(metrics)) => {
                load_log_store_inner_from_core_journal(writer.journal_path(), metrics.placement)?
            }
            _ => load_log_store_inner(&path)?,
        };
        Ok(Self {
            path,
            inner: Mutex::new(inner),
            file: Mutex::new(RaftGroupFileLogHandle::new(parent_needs_sync)),
            metrics,
            core_writer,
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
        let (write_ns, sync_ns) = if let Some(core_writer) = &self.core_writer {
            let metrics = self
                .metrics
                .as_ref()
                .expect("core journal writer requires placement metrics");
            core_writer.append(metrics.placement.raft_group_id.0, record.clone())?
        } else {
            let mut file = self.lock_file()?;
            append_log_store_record(&self.path, &mut file, record)?
        };
        if let Some(metrics) = &self.metrics {
            metrics.metrics.record_wal_batch(
                metrics.placement,
                raft_group_log_record_count(record),
                write_ns,
                sync_ns,
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
        let writer = Arc::new(Self {
            journal_path: journal_path.clone(),
            tx,
        });
        std::thread::Builder::new()
            .name("ursula-core-file-log-writer".to_owned())
            .spawn(move || run_core_file_log_writer(journal_path, rx))
            .map_err(|err| io::Error::other(format!("spawn core file log writer: {err}")))?;
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

    pub(crate) fn journal_path(&self) -> &Path {
        &self.journal_path
    }

    pub(crate) fn append(
        &self,
        group_id: u32,
        record: RaftGroupLogRecord,
    ) -> Result<(u64, u64), io::Error> {
        let (response_tx, response_rx) = mpsc::channel();
        self.tx
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
        Ok((timing.write_ns, timing.sync_ns))
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
                let per_request = CoreFileLogWriteTiming {
                    write_ns: timing.write_ns / count.max(1),
                    sync_ns: timing.sync_ns / count.max(1),
                };
                for request in batch {
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
            record: Some(request.record.clone()),
        };
        write_protobuf_frame_to_file(journal_path, journal_handle, &journal_record)?;
    }
    let write_ns = elapsed_ns(write_started_at);

    let sync_started_at = Instant::now();
    sync_file_handle(journal_path, journal_handle)?;
    Ok(CoreFileLogWriteTiming {
        write_ns,
        sync_ns: elapsed_ns(sync_started_at),
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
        spawn_log_store_blocking(move || {
            let mut inner = store.lock_inner()?;
            if inner.vote == Some(vote) {
                return Ok(());
            }
            let mut next = inner.clone();
            next.vote = Some(vote);
            store.append_record_locked(&save_vote_record(vote))?;
            *inner = next;
            Ok(())
        })
        .await
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogIdOf<UrsulaRaftTypeConfig>>,
    ) -> Result<(), io::Error> {
        let store = Arc::clone(self);
        spawn_log_store_blocking(move || {
            let mut inner = store.lock_inner()?;
            if inner.committed == committed {
                return Ok(());
            }
            let mut next = inner.clone();
            next.committed = committed;
            store.append_record_locked(&save_committed_record(committed))?;
            *inner = next;
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
        spawn_log_store_blocking(move || {
            ensure_consecutive_entries::<UrsulaRaftTypeConfig>(&entries)?;

            let mut inner = store.lock_inner()?;
            ensure_log_append_boundary::<UrsulaRaftTypeConfig>(&inner, &entries)?;

            let record = append_record(entries.iter().map(StoredLogEntry::from).collect());
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
        spawn_log_store_blocking(move || {
            let start_index = last_log_id.map_or(0, |log_id| log_id.index + 1);
            let mut inner = store.lock_inner()?;
            let mut next = inner.clone();
            next.entries.retain(|index, _| *index < start_index);
            store.append_record_locked(&truncate_after_record(last_log_id))?;
            *inner = next;
            Ok(())
        })
        .await
    }

    async fn purge(&mut self, log_id: LogIdOf<UrsulaRaftTypeConfig>) -> Result<(), io::Error> {
        let store = Arc::clone(self);
        spawn_log_store_blocking(move || {
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

            let mut next = inner.clone();
            next.last_purged_log_id = Some(log_id);
            next.entries.retain(|index, _| *index > log_id.index);
            store.append_record_locked(&purge_record(log_id))?;
            *inner = next;
            Ok(())
        })
        .await
    }
}

pub(crate) async fn spawn_log_store_blocking<T>(
    f: impl FnOnce() -> Result<T, io::Error> + Send + 'static,
) -> Result<T, io::Error>
where T: Send + 'static {
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|err| io::Error::other(format!("join OpenRaft file log task: {err}")))?
}

pub(crate) fn load_log_store_inner(path: &Path) -> Result<RaftGroupLogStoreInner, io::Error> {
    if !path.exists() {
        return Ok(RaftGroupLogStoreInner::default());
    }

    let mut inner = RaftGroupLogStoreInner::default();
    for (record_index, record) in read_protobuf_frames_from_file::<RaftGroupLogRecord>(path)?
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

pub(crate) fn load_log_store_inner_from_core_journal(
    journal_path: &Path,
    placement: ShardPlacement,
) -> Result<RaftGroupLogStoreInner, io::Error> {
    if !journal_path.exists() {
        return Ok(RaftGroupLogStoreInner::default());
    }

    let mut inner = RaftGroupLogStoreInner::default();
    for (record_index, record) in read_protobuf_frames_from_file::<CoreJournalRecord>(journal_path)?
        .into_iter()
        .enumerate()
    {
        if record.group_id != placement.raft_group_id.0 {
            continue;
        }
        let record_payload = record.record.ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "OpenRaft core journal record '{}' record {} missing payload",
                    journal_path.display(),
                    record_index + 1
                ),
            )
        })?;
        apply_log_store_record(&mut inner, record_payload).map_err(|err| {
            io::Error::new(
                err.kind(),
                format!(
                    "replay OpenRaft core journal record '{}' record {}: {err}",
                    journal_path.display(),
                    record_index + 1
                ),
            )
        })?;
    }
    Ok(inner)
}

/// Frames Raft log records as length-delimited protobuf for the shared journal.
struct ProtobufCodec<M>(PhantomData<M>);

impl<M: Message + Default> journal::FrameCodec for ProtobufCodec<M> {
    type Record = M;

    fn encode(record: &M) -> Vec<u8> {
        record.encode_to_vec()
    }

    fn decode(payload: &[u8]) -> Result<M, io::Error> {
        M::decode(payload).map_err(invalid_data)
    }
}

pub(crate) fn read_protobuf_frames_from_file<M: Message + Default>(
    path: &Path,
) -> Result<Vec<M>, io::Error> {
    journal::replay::<ProtobufCodec<M>>(path)
}

pub(crate) fn append_log_store_record(
    path: &Path,
    handle: &mut RaftGroupFileLogHandle,
    record: &RaftGroupLogRecord,
) -> Result<(u64, u64), io::Error> {
    let write_started_at = Instant::now();
    write_protobuf_frame_to_file(path, handle, record)?;
    let write_ns = elapsed_ns(write_started_at);

    let sync_started_at = Instant::now();
    sync_file_handle(path, handle)?;
    Ok((write_ns, elapsed_ns(sync_started_at)))
}

pub(crate) fn write_protobuf_frame_to_file<M: Message + Default>(
    path: &Path,
    handle: &mut RaftGroupFileLogHandle,
    value: &M,
) -> Result<(), io::Error> {
    handle.append::<ProtobufCodec<M>>(path, value)
}

#[cfg(test)]
pub(crate) fn read_protobuf_frames<M: Message + Default>(
    bytes: &[u8],
) -> Result<Vec<M>, io::Error> {
    journal::decode_frames::<ProtobufCodec<M>>(bytes).map(|(records, _)| records)
}

pub(crate) fn sync_file_handle(
    path: &Path,
    handle: &mut RaftGroupFileLogHandle,
) -> Result<(), io::Error> {
    handle.sync(path)
}

pub(crate) fn raft_group_log_record_count(record: &RaftGroupLogRecord) -> usize {
    match &record.operation {
        Some(raft_internal_proto::raft_group_log_record_v1::Operation::Append(append)) => {
            append.entries.len()
        }
        Some(_) => 1,
        None => 0,
    }
}

pub(crate) fn elapsed_ns(started_at: Instant) -> u64 {
    u64::try_from(started_at.elapsed().as_nanos()).unwrap_or(u64::MAX)
}

pub(crate) fn apply_log_store_record(
    inner: &mut RaftGroupLogStoreInner,
    record: RaftGroupLogRecord,
) -> Result<(), io::Error> {
    use raft_internal_proto::raft_group_log_record_v1::Operation;

    match record.operation.ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "OpenRaft log record missing operation",
        )
    })? {
        Operation::SaveVote(record) => {
            inner.vote = Some(vote_from_required_proto(record.vote)?);
            Ok(())
        }
        Operation::SaveCommitted(record) => {
            inner.committed = record
                .committed
                .map(|log_id| log_id_from_required_proto(Some(log_id), "committed log id"))
                .transpose()?;
            Ok(())
        }
        Operation::Append(record) => {
            let entries = record
                .entries
                .into_iter()
                .map(stored_log_entry_into_entry)
                .collect::<Result<Vec<_>, _>>()?;
            ensure_consecutive_entries::<UrsulaRaftTypeConfig>(&entries)?;
            for entry in entries {
                inner.entries.insert(entry.log_id.index, entry);
            }
            super::ensure_consecutive_log::<UrsulaRaftTypeConfig>(&inner.entries)
        }
        Operation::TruncateAfter(record) => {
            let last_log_id = record
                .last_log_id
                .map(|log_id| log_id_from_required_proto(Some(log_id), "truncate_after log id"))
                .transpose()?;
            let start_index = last_log_id.map_or(0, |log_id| log_id.index + 1);
            inner.entries.retain(|index, _| *index < start_index);
            Ok(())
        }
        Operation::Purge(record) => {
            let log_id = log_id_from_required_proto(record.log_id, "purge log id")?;
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

    fn committed_vote() -> VoteOf<UrsulaRaftTypeConfig> {
        openraft::Vote::new_committed(7, 1)
    }

    #[test]
    fn load_log_store_inner_truncates_torn_tail() {
        let path = temp_journal_path("group-log-torn-tail");
        let vote = committed_vote();
        let mut handle = RaftGroupFileLogHandle::new(true);
        append_log_store_record(&path, &mut handle, &save_vote_record(vote))
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
        write_protobuf_frame_to_file(&path, &mut handle, &CoreJournalRecord {
            group_id: placement.raft_group_id.0,
            record: Some(save_vote_record(vote)),
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
}

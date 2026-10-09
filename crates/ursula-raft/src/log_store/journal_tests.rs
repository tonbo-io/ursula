//! Tests of the per-core journal on the operating-system disk: the store,
//! the writer, segments, reclaim and recovery. They inspect and corrupt real
//! files.

use std::fs;
use std::fs::OpenOptions;
use std::io::Seek;
use std::io::SeekFrom;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

use bytes::Bytes;
use openraft::EntryPayload;
use openraft::LogId;
use openraft::alias::EntryOf;
use openraft::alias::LogIdOf;
use openraft::alias::VoteOf;
use openraft::entry::RaftEntry;
use openraft::storage::IOFlushed;
use openraft::storage::RaftLogReader;
use openraft::storage::RaftLogStorage;
use openraft::vote::RaftLeaderId;
use openraft::vote::leader_id_adv::CommittedLeaderId;
use ursula_config::WalFsync;
use ursula_runtime::GroupWriteCommand;
use ursula_runtime::RuntimeMetrics;
use ursula_shard::BucketStreamId;
use ursula_shard::CoreId;
use ursula_shard::RaftGroupId;
use ursula_shard::ShardId;
use ursula_shard::ShardPlacement;
use ursula_stream::StreamCommand;

use super::CoreJournalRecord;
use super::GroupLogState;
use super::JournalError;
use super::JournalReplayMode;
use super::RaftGroupFileLogStore;
use super::RaftGroupLogRecord;
use super::RecoveryReason;
use super::RecoveryState;
use super::RunState;
use super::RunStatus;
use super::core_meta::CoreMetadata;
use super::core_meta::MarkRecoveringError;
use super::core_meta::core_metadata_path;
use super::journal::FrameDefect;
use super::journal::JournalWriter;
use super::run_state::PreviousRun;
use super::run_state::RunStateFile;
use super::segment::SegmentId;
use super::segment::list_segments;
use super::segment::segment_path;
use super::writer::CoreFileLogWriter;
use super::writer::CoreJournalError;
use super::writer::CoreJournalOptions;
use super::writer::JournalTuning;
use super::writer::LaggingGroups;
use super::writer::WireCodec;
use super::writer::group_commit_wait;
use super::writer::raft_group_log_record_initializes;
use super::writer::raft_group_log_record_requires_sync;
use crate::types::UrsulaRaftTypeConfig;

type Entry = EntryOf<UrsulaRaftTypeConfig>;

/// The smallest segment, so a few entries rotate.
const SEGMENT_BYTES: u64 = JournalTuning::MIN_SEGMENT_BYTES;

fn placement(raft_group_id: u32) -> ShardPlacement {
    ShardPlacement {
        core_id: CoreId(0),
        shard_id: ShardId(raft_group_id),
        raft_group_id: RaftGroupId(raft_group_id),
    }
}

fn log_id(index: u64) -> LogIdOf<UrsulaRaftTypeConfig> {
    LogId {
        leader_id: CommittedLeaderId::new(5, 1),
        index,
    }
}

fn blank_entry(index: u64) -> Entry {
    Entry::new(log_id(index), EntryPayload::Blank)
}

fn payload_entry(index: u64, payload_size: usize) -> Entry {
    Entry::new(
        log_id(index),
        EntryPayload::Normal(GroupWriteCommand::Stream(StreamCommand::Append {
            stream_id: BucketStreamId::new("wal-test", "segments"),
            content_type: Some("application/octet-stream".to_owned()),
            payload: Bytes::from(vec![u8::try_from(index % 251).unwrap_or(0); payload_size]),
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

/// A core journal under a temporary WAL root and how its runs open it.
struct Core {
    _root: tempfile::TempDir,
    dir: PathBuf,
    metrics: RuntimeMetrics,
    lagging: Arc<LaggingGroups>,
    tuning: JournalTuning,
}

impl Core {
    fn new(tuning: JournalTuning) -> Self {
        let root = tempfile::tempdir().expect("WAL root");
        let dir = root.path().join("core-0");
        Self {
            _root: root,
            dir,
            metrics: RuntimeMetrics::new(1, 8),
            lagging: Arc::new(LaggingGroups::default()),
            tuning,
        }
    }

    /// A core of small segments and caches.
    fn small(fsync: WalFsync) -> Self {
        Self::new(JournalTuning {
            fsync,
            segment_bytes: SEGMENT_BYTES,
            group_cache_bytes: 1024,
        })
    }

    fn options(&self, recovery_epoch: u64, node_recovery: RecoveryState) -> CoreJournalOptions {
        CoreJournalOptions {
            previous_run: PreviousRun::Absent,
            core: CoreId(0),
            tuning: self.tuning,
            recovery_epoch,
            run_state: Arc::new(RunStateFile::new(
                self.dir.with_extension("run-state"),
                RunState {
                    boot_id: None,
                    fsync: self.tuning.fsync,
                    status: RunStatus::Running,
                    recovery_epoch,
                },
            )),
            node_recovery,
            lagging: self.lagging.clone(),
            metrics: Some((placement(0), self.metrics.group_engine_metrics())),
        }
    }

    /// Opens the journal as the run after `previous_run` does, with the
    /// replay mode and the node's recovery state that run derives from it.
    /// A run that reads a verified prefix starts recovery epoch 1, which
    /// this journal was not read in yet. A recovering run first moves the
    /// core's initialized groups into recovery, as the node does.
    fn open_after(
        &self,
        previous_run: PreviousRun,
    ) -> Result<Arc<CoreFileLogWriter>, CoreJournalError> {
        let recovery_epoch = match previous_run.replay_mode() {
            JournalReplayMode::Strict => 0,
            JournalReplayMode::VerifiedPrefix => 1,
        };
        let recovery = previous_run.recovery_state();
        if let RecoveryState::Recovering { .. } = recovery {
            super::core_meta::mark_core_recovering(&core_metadata_path(&self.dir)).map_err(
                |error| match error {
                    MarkRecoveringError::Read(source) => CoreJournalError::from(source),
                    MarkRecoveringError::Write(source) => CoreJournalError::from(source),
                },
            )?;
        }
        let mut options = self.options(recovery_epoch, recovery);
        options.previous_run = previous_run;
        CoreFileLogWriter::open(self.dir.clone(), options)
    }

    fn writer(&self) -> Arc<CoreFileLogWriter> {
        self.open_after(PreviousRun::Absent)
            .expect("open the core writer")
    }

    fn store(&self, writer: &Arc<CoreFileLogWriter>, group: u32) -> Arc<RaftGroupFileLogStore> {
        RaftGroupFileLogStore::open(
            placement(group),
            self.metrics.group_engine_metrics(),
            writer.clone(),
        )
        .expect("open a group store")
    }

    fn segments(&self) -> Vec<u64> {
        list_segments(&self.dir)
            .expect("list segments")
            .into_iter()
            .map(|id| id.0)
            .collect()
    }

    fn segment(&self, id: u64) -> PathBuf {
        segment_path(&self.dir, SegmentId(id))
    }
}

async fn append(store: &mut Arc<RaftGroupFileLogStore>, entries: impl IntoIterator<Item = Entry>) {
    use openraft::type_config::TypeConfigExt;
    let (flushed, result) = UrsulaRaftTypeConfig::oneshot();
    store
        .append(
            entries.into_iter().collect::<Vec<_>>(),
            IOFlushed::signal(flushed),
        )
        .await
        .expect("submit entries");
    result
        .await
        .expect("flush callback")
        .expect("append entries");
}

async fn log_ids(store: &Arc<RaftGroupFileLogStore>) -> Vec<u64> {
    let mut reader = store.clone();
    reader
        .try_get_log_entries(..)
        .await
        .expect("read entries")
        .iter()
        .map(|entry| entry.log_id.index)
        .collect()
}

fn file_len(path: &Path) -> u64 {
    fs::metadata(path).expect("file metadata").len()
}

fn overwrite(path: &Path, offset: u64, bytes: &[u8]) {
    let mut file = OpenOptions::new()
        .write(true)
        .open(path)
        .expect("open segment");
    file.seek(SeekFrom::Start(offset)).expect("seek");
    file.write_all(bytes).expect("overwrite");
    file.sync_data().expect("sync overwrite");
}

fn append_raw(path: &Path, bytes: &[u8]) {
    let mut file = OpenOptions::new()
        .append(true)
        .open(path)
        .expect("open segment");
    file.write_all(bytes).expect("append raw bytes");
    file.sync_data().expect("sync raw bytes");
}

fn journal_error(err: &CoreJournalError) -> Option<&JournalError> {
    match err {
        CoreJournalError::Journal(err) => Some(err),
        _ => None,
    }
}

#[test]
fn fsync_policy_keeps_only_replay_hints_best_effort() {
    assert!(raft_group_log_record_requires_sync(
        &RaftGroupLogRecord::Append(vec![blank_entry(1)]),
        WalFsync::Always
    ));
    assert!(raft_group_log_record_requires_sync(
        &RaftGroupLogRecord::Purge(log_id(1)),
        WalFsync::Always
    ));
    assert!(!raft_group_log_record_requires_sync(
        &RaftGroupLogRecord::SaveCommitted(Some(log_id(1))),
        WalFsync::Always
    ));
    assert!(!raft_group_log_record_requires_sync(
        &RaftGroupLogRecord::TruncateAfter(Some(log_id(1))),
        WalFsync::Always
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
        &RaftGroupLogRecord::Purge(log_id(1))
    ));
    assert!(!raft_group_log_record_initializes(
        &RaftGroupLogRecord::Append(Vec::new())
    ));
    assert!(!raft_group_log_record_initializes(
        &RaftGroupLogRecord::SaveCommitted(Some(log_id(1)))
    ));
    assert!(!raft_group_log_record_initializes(
        &RaftGroupLogRecord::TruncateAfter(None)
    ));
}

/// The journal rotates into numbered segments at its target size, each
/// starting with its own header, and a restart reads them in order.
#[tokio::test]
async fn the_journal_rotates_into_numbered_segments() {
    let core = Core::small(WalFsync::Never);
    let writer = core.writer();
    let mut store = core.store(&writer, 1);
    for index in 1..=40 {
        append(&mut store, [payload_entry(index, 512)]).await;
    }
    // The writer rotates after it replies; closing waits for that.
    writer.close().await.expect("close the writer");
    let segments = core.segments();
    assert!(
        segments.len() >= 4,
        "40 entries of 512 bytes rotate: {segments:?}"
    );
    assert_eq!(
        segments,
        (1..=u64::try_from(segments.len()).expect("fits")).collect::<Vec<_>>(),
        "segment sequences are consecutive from 1"
    );
    for id in segments.iter().take(segments.len().saturating_sub(1)) {
        assert!(
            file_len(&core.segment(*id)) >= SEGMENT_BYTES,
            "a sealed segment reached the target size"
        );
    }
    let snapshot = core.metrics.snapshot();
    assert!(snapshot.wal_rotations >= 3);
    assert_eq!(
        snapshot.wal_segments,
        u64::try_from(segments.len()).expect("fits")
    );
    drop(store);
    drop(writer);

    let writer = core.writer();
    let store = core.store(&writer, 1);
    assert_eq!(log_ids(&store).await, (1..=40).collect::<Vec<_>>());
}

/// Purge deletes every whole segment no group keeps a live record in, and
/// never the segment appends go to.
#[tokio::test]
async fn purge_deletes_the_segments_no_group_needs() {
    let core = Core::small(WalFsync::Always);
    let writer = core.writer();
    let mut store = core.store(&writer, 1);
    for index in 1..=40 {
        append(&mut store, [payload_entry(index, 512)]).await;
    }
    // The writer rotates after it replies; the next write waits for that.
    store
        .save_committed(Some(log_id(39)))
        .await
        .expect("commit");
    let before = core.segments();
    store.purge(log_id(36)).await.expect("purge");
    // The pass runs after the purge's batch; the next write waits for it.
    store
        .save_committed(Some(log_id(40)))
        .await
        .expect("commit");
    let after = core.segments();
    assert!(after.len() < before.len(), "{before:?} then {after:?}");
    assert_eq!(after.last(), before.last(), "the newest segment stays");
    for id in before.iter().filter(|id| !after.contains(id)) {
        assert!(!core.segment(*id).exists());
    }
    let snapshot = core.metrics.snapshot();
    assert_eq!(
        snapshot.wal_reclaims,
        u64::try_from(before.len().saturating_sub(after.len())).expect("fits")
    );
    assert!(snapshot.wal_reclaimed_bytes >= SEGMENT_BYTES);
    assert_eq!(log_ids(&store).await, (37..=40).collect::<Vec<_>>());
    drop(store);
    drop(writer);

    let writer = core.writer();
    let mut store = core.store(&writer, 1);
    assert_eq!(log_ids(&store).await, (37..=40).collect::<Vec<_>>());
    let state = store.get_log_state().await.expect("log state");
    assert_eq!(state.last_purged_log_id, Some(log_id(36)));
    assert_eq!(
        store.read_committed().await.expect("committed"),
        Some(log_id(40))
    );
}

/// A stopped group's retained committed history must not pin the journal's
/// ever-growing suffix, even when it exceeds the normal rewrite threshold.
#[tokio::test]
async fn apply_stopped_payload_is_archived_once_and_references_reclaim() {
    let core = Core::small(WalFsync::Always);
    let writer = core.writer();
    let mut stopped = core.store(&writer, 1);
    let mut healthy = core.store(&writer, 2);
    append(&mut stopped, (1..=8).map(|index| payload_entry(index, 700))).await;
    stopped.save_committed(Some(log_id(8))).await.unwrap();
    stopped
        .apply_stop_signal()
        .store(true, std::sync::atomic::Ordering::Release);
    for index in 1..=400_u64 {
        append(&mut healthy, [payload_entry(index, 512)]).await;
        if index % 8 == 0 {
            healthy
                .purge(log_id(index.saturating_sub(4)))
                .await
                .unwrap();
        }
    }
    healthy.save_committed(Some(log_id(400))).await.unwrap();
    writer.close().await.unwrap();
    let metrics = core.metrics.snapshot();
    assert!(metrics.wal_stopped_rewritten_bytes > 0);
    assert!(metrics.wal_reclaims > 20);
    assert!(core.lagging.groups().is_empty());
    assert!(
        core.segments().len() < 16,
        "bounded retained WAL: {:?}",
        core.segments()
    );
    assert_eq!(log_ids(&stopped).await, (1..=8).collect::<Vec<_>>());
    drop(stopped);
    drop(healthy);
    drop(writer);
    let writer = core.writer();
    let mut stopped = core.store(&writer, 1);
    let healthy = core.store(&writer, 2);
    assert!(
        !stopped
            .apply_stop_signal()
            .load(std::sync::atomic::Ordering::Acquire),
        "replay, not a persistent failure marker, reestablishes stopped state"
    );
    assert_eq!(log_ids(&stopped).await, (1..=8).collect::<Vec<_>>());
    assert_eq!(stopped.read_committed().await.unwrap(), Some(log_id(8)));
    assert_eq!(log_ids(&healthy).await, (397..=400).collect::<Vec<_>>());
    // Replaying faulty code reestablishes the stop signal. Further healthy
    // reclamation copies references, never the already archived payload.
    stopped
        .apply_stop_signal()
        .store(true, std::sync::atomic::Ordering::Release);
    let mut healthy = healthy;
    for index in 401..=800_u64 {
        append(&mut healthy, [payload_entry(index, 512)]).await;
        if index % 8 == 0 {
            healthy
                .purge(log_id(index.saturating_sub(4)))
                .await
                .unwrap();
        }
    }
    writer.close().await.unwrap();
    let after = core.metrics.snapshot();
    assert_eq!(
        after.wal_stopped_rewritten_bytes,
        metrics.wal_stopped_rewritten_bytes
    );
    assert!(
        after.wal_frozen_reference_rewritten_bytes > metrics.wal_frozen_reference_rewritten_bytes
    );
    assert_eq!(log_ids(&stopped).await, (1..=8).collect::<Vec<_>>());
    assert!(core.segments().len() < 16);
}

#[tokio::test]
async fn archived_subset_does_not_resurrect_truncated_or_purged_entries() {
    let core = Core::small(WalFsync::Always);
    let writer = core.writer();
    let mut stopped = core.store(&writer, 1);
    let mut healthy = core.store(&writer, 2);
    append(
        &mut stopped,
        (1..=10).map(|index| payload_entry(index, 700)),
    )
    .await;
    stopped
        .apply_stop_signal()
        .store(true, std::sync::atomic::Ordering::Release);
    for index in 1..=200_u64 {
        append(&mut healthy, [payload_entry(index, 512)]).await;
        if index % 8 == 0 {
            healthy
                .purge(log_id(index.saturating_sub(4)))
                .await
                .unwrap();
        }
    }
    writer.close().await.unwrap();
    assert!(core.metrics.snapshot().wal_stopped_rewritten_bytes > 0);
    drop((stopped, healthy, writer));
    let writer = core.writer();
    let mut stopped = core.store(&writer, 1);
    let mut healthy = core.store(&writer, 2);
    stopped.truncate_after(Some(log_id(6))).await.unwrap();
    let replacement = (7..=8)
        .map(|index| {
            let mut entry = payload_entry(index, 700);
            entry.log_id.leader_id = CommittedLeaderId::new(6, 2);
            entry
        })
        .collect::<Vec<_>>();
    append(&mut stopped, replacement.clone()).await;
    stopped.purge(log_id(2)).await.unwrap();
    stopped
        .apply_stop_signal()
        .store(true, std::sync::atomic::Ordering::Release);
    for index in 201..=400_u64 {
        append(&mut healthy, [payload_entry(index, 512)]).await;
        if index % 8 == 0 {
            healthy
                .purge(log_id(index.saturating_sub(4)))
                .await
                .unwrap();
        }
    }
    writer.close().await.unwrap();
    drop((stopped, healthy, writer));
    let writer = core.writer();
    let mut stopped = core.store(&writer, 1);
    let mut reader = stopped.get_log_reader().await;
    let expected = (3..=6)
        .map(|index| payload_entry(index, 700))
        .chain(replacement)
        .collect::<Vec<_>>();
    assert_eq!(reader.try_get_log_entries(..).await.unwrap(), expected);
    // Corrected code can purge archived records. GC waits until every old
    // physical reference has itself been durably reclaimed.
    stopped
        .purge(LogId {
            leader_id: CommittedLeaderId::new(6, 2),
            index: 8,
        })
        .await
        .unwrap();
    let mut healthy = core.store(&writer, 2);
    for index in 401..=800_u64 {
        append(&mut healthy, [payload_entry(index, 512)]).await;
        if index % 8 == 0 {
            healthy
                .purge(log_id(index.saturating_sub(4)))
                .await
                .unwrap();
        }
    }
    writer.close().await.unwrap();
    assert!(fs::read_dir(&core.dir).unwrap().all(|entry| {
        !entry
            .unwrap()
            .file_name()
            .to_string_lossy()
            .starts_with("frozen-")
    }));
}

#[test]
fn archived_payload_damage_is_a_typed_hard_startup_failure() {
    for missing in [false, true] {
        let core = Core::small(WalFsync::Always);
        fs::create_dir_all(&core.dir).unwrap();
        let entry = payload_entry(1, 700);
        let archive = super::frozen::publish(
            &core.dir,
            super::frozen::ArchiveId {
                group_id: 1,
                segment: 1,
                offset: 32,
                first_index: 1,
                last_index: 1,
                content_hash: [0; 32],
            },
            vec![entry.clone()],
        )
        .unwrap();
        let journal_path = core.segment(1);
        let mut journal = JournalWriter::open(&journal_path, 1).unwrap();
        journal
            .append::<WireCodec<CoreJournalRecord>>(&CoreJournalRecord {
                group_id: 1,
                record: RaftGroupLogRecord::FrozenAppend(Box::new(super::frozen::FrozenAppend {
                    archive: archive.clone(),
                    log_ids: vec![entry.log_id],
                })),
            })
            .unwrap();
        journal.sync_data().unwrap();
        drop(journal);
        let original = fs::read(&journal_path).unwrap();
        let archive_path = super::frozen::path(&core.dir, archive.id);
        if missing {
            fs::remove_file(&archive_path).unwrap();
        } else {
            fs::write(&archive_path, b"corrupt").unwrap();
        }
        for _ in 0..2 {
            let error = core.open_after(PreviousRun::Absent).unwrap_err();
            match error {
                CoreJournalError::Journal(error) if missing => {
                    assert!(matches!(error.as_ref(), JournalError::Io { .. }))
                }
                CoreJournalError::Journal(error) => {
                    assert!(matches!(error.as_ref(), JournalError::FrozenArchive {
                        defect: super::ArchiveDefect::Length,
                        ..
                    }))
                }
                error => panic!("unexpected startup error: {error:?}"),
            }
            assert_eq!(fs::read(&journal_path).unwrap(), original);
        }
    }
}

/// A quiet group's few entries keep the oldest segment alive. Once the
/// journal outgrows twice its live bytes they are rewritten into the newest
/// segment in chunks, with the group's markers, and the old segments go.
#[tokio::test]
async fn a_quiet_groups_remainder_is_rewritten_in_chunks() {
    let core = Core::small(WalFsync::Never);
    let writer = core.writer();
    let mut quiet = core.store(&writer, 1);
    let mut busy = core.store(&writer, 2);
    append(&mut quiet, (1..=3).map(|index| payload_entry(index, 200))).await;
    quiet.save_committed(Some(log_id(3))).await.expect("commit");
    quiet.purge(log_id(1)).await.expect("purge");
    for index in 1..=120 {
        append(&mut busy, [payload_entry(index, 512)]).await;
        if index % 8 == 0 {
            busy.purge(log_id(index.saturating_sub(4)))
                .await
                .expect("purge");
        }
    }
    busy.save_committed(Some(log_id(120)))
        .await
        .expect("commit");
    // Closing waits for the reclaim pass that follows the last batch.
    writer.close().await.expect("close the writer");
    let snapshot = core.metrics.snapshot();
    assert!(
        snapshot.wal_rewritten_bytes > 0,
        "the quiet group was rewritten"
    );
    assert!(
        !core.segment(1).exists(),
        "the oldest segment is gone: {:?}",
        core.segments()
    );
    assert!(
        core.lagging.groups().is_empty(),
        "a small remainder is not lagging"
    );
    assert!(
        snapshot.wal_physical_bytes <= 6 * SEGMENT_BYTES,
        "the journal stays bounded: {} bytes",
        snapshot.wal_physical_bytes
    );
    // Rewritten entries come back in frames of at most a chunk.
    let frames = core
        .segments()
        .iter()
        .flat_map(|id| {
            super::read_wire_frames::<CoreJournalRecord>(
                &fs::read(core.segment(*id)).expect("read"),
            )
            .expect("decode")
        })
        .filter(|record| record.group_id == 1)
        .collect::<Vec<_>>();
    assert!(
        frames.iter().any(|record| matches!(&record.record, RaftGroupLogRecord::Append(entries) if entries.len() == 1)),
        "the rewrite chunked the entries: {frames:?}"
    );
    drop((quiet, busy));
    drop(writer);

    let writer = core.writer();
    let mut quiet = core.store(&writer, 1);
    assert_eq!(log_ids(&quiet).await, [2, 3]);
    assert_eq!(
        quiet.read_committed().await.expect("committed"),
        Some(log_id(3))
    );
    assert_eq!(
        quiet
            .get_log_state()
            .await
            .expect("state")
            .last_purged_log_id,
        Some(log_id(1))
    );
}

/// A group holding much of the oldest segment is reported lagging rather
/// than rewritten; once it purges, the report clears and the segments go.
#[tokio::test]
async fn a_group_holding_much_of_the_oldest_segment_is_reported_lagging() {
    let core = Core::small(WalFsync::Never);
    let writer = core.writer();
    let mut laggard = core.store(&writer, 3);
    let mut busy = core.store(&writer, 2);
    append(
        &mut laggard,
        (1..=10).map(|index| payload_entry(index, 400)),
    )
    .await;
    for index in 1..=120 {
        append(&mut busy, [payload_entry(index, 512)]).await;
        if index % 8 == 0 {
            busy.purge(log_id(index.saturating_sub(4)))
                .await
                .expect("purge");
        }
    }
    busy.save_committed(Some(log_id(120)))
        .await
        .expect("commit");
    assert_eq!(core.lagging.groups().into_iter().collect::<Vec<_>>(), [
        RaftGroupId(3)
    ]);
    let snapshot = core.metrics.snapshot();
    assert_eq!(snapshot.wal_lagging_groups, 1);
    assert!(snapshot.wal_pinned_segments >= 1);
    assert!(core.segment(1).exists());

    laggard
        .purge(log_id(10))
        .await
        .expect("the laggard snapshots and purges");
    busy.save_committed(Some(log_id(119)))
        .await
        .expect("commit");
    assert!(core.lagging.groups().is_empty());
    assert!(!core.segment(1).exists());
    assert_eq!(core.metrics.snapshot().wal_lagging_groups, 0);
}

/// Entries the cache evicted are read back from disk identical to what was
/// appended, and reads report their hits, misses and disk reads.
#[tokio::test]
async fn evicted_entries_read_back_from_disk_identical() {
    let core = Core::small(WalFsync::Never);
    let writer = core.writer();
    let mut store = core.store(&writer, 1);
    let appended = (1..=30)
        .map(|index| payload_entry(index, 300))
        .collect::<Vec<_>>();
    for chunk in appended.chunks(3) {
        append(&mut store, chunk.to_vec()).await;
    }
    let before = core.metrics.snapshot();
    let mut reader = store.clone();
    let read = reader.try_get_log_entries(1..=30).await.expect("read");
    assert_eq!(read, appended);
    let after = core.metrics.snapshot();
    let misses = after
        .wal_cache_misses
        .saturating_sub(before.wal_cache_misses);
    let hits = after.wal_cache_hits.saturating_sub(before.wal_cache_hits);
    assert_eq!(misses.saturating_add(hits), 30);
    assert!(
        misses >= 27,
        "a 1 KiB cache holds at most two 300-byte entries"
    );
    assert_eq!(
        after.wal_disk_reads.saturating_sub(before.wal_disk_reads),
        misses.div_ceil(3),
        "one frame read per append of three"
    );
    assert!(after.wal_cache_bytes <= 1024);
    assert_eq!(after.wal_indexed_entries, 30);
    // A limited read stops after a bounded amount from disk but returns at
    // least one entry, a prefix of the range.
    let limited = reader
        .limited_get_log_entries(1, 31)
        .await
        .expect("limited read");
    assert!(!limited.is_empty());
    assert_eq!(
        limited.as_slice(),
        appended.get(..limited.len()).expect("prefix")
    );
    drop((reader, store));
    drop(writer);

    // A restart reads the same entries.
    let writer = core.writer();
    let mut reader = core.store(&writer, 1);
    assert_eq!(
        reader.try_get_log_entries(..).await.expect("read"),
        appended
    );
}

/// Recovery reads every segment in order and truncates an incomplete final
/// frame only in the newest one, whether a host crash under `always` reads
/// the journal strictly or one under `never` reads its verified prefix.
#[tokio::test]
async fn a_torn_tail_in_the_newest_segment_is_truncated_in_both_modes() {
    for fsync in [WalFsync::Always, WalFsync::Never] {
        let previous_run = PreviousRun::HostCrash { fsync };
        let mode = previous_run.replay_mode();
        let core = Core::small(fsync);
        let writer = core.writer();
        let mut store = core.store(&writer, 1);
        for index in 1..=30 {
            append(&mut store, [payload_entry(index, 512)]).await;
        }
        let mut sibling = core.store(&writer, 2);
        append(&mut sibling, [blank_entry(1)]).await;
        drop(sibling);
        drop(store);
        drop(writer);
        let newest = *core.segments().last().expect("segments");
        let len = file_len(&core.segment(newest));
        append_raw(&core.segment(newest), &[200, 0, 0, 0, 1, 2]);

        let writer = core.open_after(previous_run).expect("recover a torn tail");
        assert_eq!(writer.replay_mode(), mode, "{previous_run:?}");
        let store = core.store(&writer, 1);
        let sibling = core.store(&writer, 2);
        // An unacknowledged partial frame gates no group. Only the host crash
        // under `never` gates them, for what the page cache may have lost.
        let expected = match previous_run.recovery_state() {
            RecoveryState::Normal => GroupLogState::Initialized,
            RecoveryState::Recovering { .. } => GroupLogState::Recovering,
        };
        assert_eq!(store.log_state(), expected, "{mode:?}");
        assert_eq!(
            sibling.log_state(),
            expected,
            "{mode:?}: a sibling sharing the journal"
        );
        assert_eq!(
            log_ids(&store).await,
            (1..=30).collect::<Vec<_>>(),
            "{mode:?}"
        );
        assert_eq!(
            file_len(&core.segment(newest)),
            len,
            "{mode:?}: the torn frame is gone"
        );
    }
}

#[tokio::test]
async fn complete_corruption_under_always_fails_without_changing_any_segment() {
    let core = Core::small(WalFsync::Always);
    let writer = core.writer();
    let mut store = core.store(&writer, 1);
    let entries = 40;
    for index in 1..=entries {
        append(&mut store, [payload_entry(index, 512)]).await;
    }
    drop((store, writer));
    let ids = core.segments();
    let id = ids[0];
    let path = core.segment(id);
    let bytes = fs::read(&path).unwrap();
    let offset = u64::try_from(bytes.len()).unwrap().checked_sub(1).unwrap();
    overwrite(&path, offset, &[bytes.last().unwrap() ^ 0xff]);
    let before = ids
        .iter()
        .map(|id| (*id, fs::read(core.segment(*id)).unwrap()))
        .collect::<Vec<_>>();
    for configured_fsync in [WalFsync::Always, WalFsync::Never] {
        let mut options = core.options(0, RecoveryState::Normal);
        options.tuning.fsync = configured_fsync;
        options.previous_run = PreviousRun::HostCrash {
            fsync: WalFsync::Always,
        };
        let error = CoreFileLogWriter::open(core.dir.clone(), options)
            .expect_err("durable corruption fails closed even after changing fsync policy");
        assert!(
            matches!(
                journal_error(&error),
                Some(JournalError::CorruptFrame {
                    defect: FrameDefect::PayloadChecksum,
                    ..
                })
            ),
            "{error}"
        );
        assert_eq!(core.segments(), ids);
        for (id, bytes) in &before {
            assert_eq!(fs::read(core.segment(*id)).unwrap(), *bytes);
        }
    }
}
#[tokio::test]
async fn durable_corruption_stays_unchanged_across_node_startup_retries() {
    use super::run_state::NodeWal;
    use super::run_state::RUN_STATE_FILE;

    let core = Core::small(WalFsync::Always);
    let root = core._root.path();
    let topology = ursula_shard::StaticShardMap::new(1, 2).unwrap();
    drop(NodeWal::start(root.to_owned(), WalFsync::Always, &topology).unwrap());
    let writer = core.writer();
    let mut store = core.store(&writer, 1);
    for index in 1..=40 {
        append(&mut store, [payload_entry(index, 512)]).await;
    }
    drop((store, writer));
    // Unknown boot identity forces a host crash on the first real startup.
    RunStateFile::new(root.join(RUN_STATE_FILE), RunState {
        boot_id: None,
        fsync: WalFsync::Always,
        status: RunStatus::Running,
        recovery_epoch: 0,
    })
    .record(RunStatus::Running, "test host crash")
    .unwrap();
    let ids = core.segments();
    let path = core.segment(ids[0]);
    let bytes = fs::read(&path).unwrap();
    overwrite(
        &path,
        u64::try_from(bytes.len()).unwrap().checked_sub(1).unwrap(),
        &[bytes.last().unwrap() ^ 0xff],
    );
    let before = ids
        .iter()
        .map(|id| (*id, fs::read(core.segment(*id)).unwrap()))
        .collect::<Vec<_>>();
    for _ in 0..2 {
        let node = NodeWal::start(root.to_owned(), WalFsync::Always, &topology).unwrap();
        let opening = node.opening();
        assert_eq!(opening.recovery_epoch, 0);
        let mut options = core.options(opening.recovery_epoch, opening.recovery);
        options.previous_run = opening.previous_run;
        options.run_state = node.run_state().clone();
        let error = CoreFileLogWriter::open(core.dir.clone(), options).unwrap_err();
        assert!(matches!(
            journal_error(&error),
            Some(JournalError::CorruptFrame {
                defect: FrameDefect::PayloadChecksum,
                ..
            })
        ));
        assert_eq!(core.segments(), ids);
        for (id, bytes) in &before {
            assert_eq!(fs::read(core.segment(*id)).unwrap(), *bytes);
        }
    }
}

#[tokio::test]
async fn newest_tail_is_unchanged_when_persisting_its_recovery_gate_fails() {
    let core = Core::small(WalFsync::Always);
    let writer = core.writer();
    let mut store = core.store(&writer, 1);
    append(&mut store, [payload_entry(1, 512)]).await;
    append(&mut store, [payload_entry(2, 512)]).await;
    drop((store, writer));
    let path = core.segment(*core.segments().last().unwrap());
    let bytes = fs::read(&path).unwrap();
    overwrite(
        &path,
        u64::try_from(bytes.len()).unwrap().checked_sub(1).unwrap(),
        &[bytes.last().unwrap() ^ 0xff],
    );
    let damaged = fs::read(&path).unwrap();
    let metadata_path = core_metadata_path(&core.dir);
    let metadata_before = fs::read(&metadata_path).unwrap();
    // Atomic metadata publication cannot create its temporary file.
    fs::create_dir(core.dir.join("journal.meta.tmp")).unwrap();
    for _ in 0..2 {
        let error = core.open_after(PreviousRun::Absent).unwrap_err();
        assert!(matches!(
            journal_error(&error),
            Some(JournalError::Io { .. })
        ));
        assert_eq!(fs::read(&path).unwrap(), damaged);
        assert_eq!(fs::read(&metadata_path).unwrap(), metadata_before);
    }
}

#[tokio::test]
async fn newest_invalid_tail_is_gated_before_repair_and_across_lazy_startup_retries() {
    use super::run_state::NodeWal;
    use super::run_state::RUN_STATE_FILE;

    for gate_already_persisted in [false, true] {
        let core = Core::small(WalFsync::Always);
        let root = core._root.path();
        let topology = ursula_shard::StaticShardMap::new(1, 3).unwrap();
        drop(NodeWal::start(root.to_owned(), WalFsync::Always, &topology).unwrap());
        let writer = core.writer();
        let mut sibling = core.store(&writer, 2);
        append(&mut sibling, [blank_entry(1)]).await;
        let mut store = core.store(&writer, 1);
        append(&mut store, [payload_entry(1, 512)]).await;
        append(&mut store, [payload_entry(2, 512)]).await;
        drop((store, sibling, writer));
        let path = core.segment(*core.segments().last().unwrap());
        let bytes = fs::read(&path).unwrap();
        overwrite(
            &path,
            u64::try_from(bytes.len()).unwrap().checked_sub(1).unwrap(),
            &[bytes.last().unwrap() ^ 0xff],
        );
        RunStateFile::new(root.join(RUN_STATE_FILE), RunState {
            boot_id: None,
            fsync: WalFsync::Always,
            status: RunStatus::Running,
            recovery_epoch: 0,
        })
        .record(RunStatus::Running, "host crash")
        .unwrap();
        // Startup records Running but crashes before lazy core replay.
        drop(NodeWal::start(root.to_owned(), WalFsync::Always, &topology).unwrap());
        if gate_already_persisted {
            // Also exercise a crash after the durable repair gate, before truncate.
            assert!(
                super::core_meta::mark_core_recovering(&core_metadata_path(&core.dir)).unwrap()
            );
        }
        for _ in 0..2 {
            let node = NodeWal::start(root.to_owned(), WalFsync::Never, &topology).unwrap();
            let opening = node.opening();
            let mut options = core.options(opening.recovery_epoch, opening.recovery);
            options.tuning.fsync = WalFsync::Never;
            options.previous_run = opening.previous_run;
            options.run_state = node.run_state().clone();
            let writer = CoreFileLogWriter::open(core.dir.clone(), options).unwrap();
            let store = core.store(&writer, 1);
            let sibling = core.store(&writer, 2);
            assert_eq!(log_ids(&store).await, [1]);
            assert_eq!(log_ids(&sibling).await, [1]);
            assert_eq!(store.log_state(), GroupLogState::Recovering);
            assert_eq!(sibling.log_state(), GroupLogState::Recovering);
            let durable = CoreMetadata::load(&core_metadata_path(&core.dir)).unwrap();
            assert!(
                durable
                    .groups()
                    .all(|(_, group)| group.log == GroupLogState::Recovering)
            );
            drop((store, sibling, writer));
        }
    }
}

#[tokio::test]
async fn never_host_crash_repair_remains_gated_when_switching_to_always() {
    let core = Core::small(WalFsync::Never);
    let writer = core.writer();
    let mut store = core.store(&writer, 1);
    append(&mut store, [payload_entry(1, 512)]).await;
    append(&mut store, [payload_entry(2, 512)]).await;
    drop((store, writer));
    let path = core.segment(*core.segments().last().unwrap());
    let bytes = fs::read(&path).unwrap();
    let offset = u64::try_from(bytes.len()).unwrap().checked_sub(1).unwrap();
    overwrite(&path, offset, &[bytes.last().unwrap() ^ 0xff]);
    // NodeWal marks existing groups before opening a possibly lossy journal.
    assert!(super::core_meta::mark_core_recovering(&core_metadata_path(&core.dir)).unwrap());
    let mut options = core.options(1, RecoveryState::Recovering {
        reason: RecoveryReason::HostCrash,
    });
    options.previous_run = PreviousRun::HostCrash {
        fsync: WalFsync::Never,
    };
    options.tuning.fsync = WalFsync::Always;
    let writer = CoreFileLogWriter::open(core.dir.clone(), options).unwrap();
    assert_eq!(writer.replay_mode(), JournalReplayMode::VerifiedPrefix);
    let store = core.store(&writer, 1);
    assert_eq!(log_ids(&store).await, [1]);
    assert_eq!(store.log_state(), GroupLogState::Recovering);
}

/// An incomplete frame at the end of a sealed segment cannot be a crash:
/// rotation `fsync`s a segment before the next one starts.
#[tokio::test]
async fn an_incomplete_sealed_segment_fails_strict() {
    let core = Core::small(WalFsync::Always);
    let writer = core.writer();
    let mut store = core.store(&writer, 1);
    for index in 1..=30 {
        append(&mut store, [payload_entry(index, 512)]).await;
    }
    drop(store);
    drop(writer);
    append_raw(&core.segment(1), &[200, 0, 0]);
    let err = core
        .open_after(PreviousRun::Absent)
        .expect_err("strict fails closed");
    assert!(
        matches!(
            journal_error(&err),
            Some(JournalError::IncompleteSealedSegment { bytes: 3, .. })
        ),
        "{err}"
    );
}

/// Rotation fsyncs sealed segments under both policies; never repair their damage.
#[tokio::test]
async fn sealed_corruption_and_missing_segments_fail_without_mutation_under_never() {
    for missing in [false, true] {
        let core = Core::small(WalFsync::Never);
        let writer = core.writer();
        let mut store = core.store(&writer, 1);
        for index in 1..=40 {
            append(&mut store, [payload_entry(index, 512)]).await;
        }
        drop((store, writer));
        if missing {
            fs::remove_file(core.segment(3)).unwrap();
        } else {
            let path = core.segment(2);
            let bytes = fs::read(&path).unwrap();
            overwrite(
                &path,
                u64::try_from(bytes.len()).unwrap().checked_sub(1).unwrap(),
                &[bytes.last().unwrap() ^ 0xff],
            );
        }
        let before = core
            .segments()
            .into_iter()
            .map(|id| (id, fs::read(core.segment(id)).unwrap()))
            .collect::<Vec<_>>();
        for previous_run in [PreviousRun::Absent, PreviousRun::HostCrash {
            fsync: WalFsync::Never,
        }] {
            let error = core.open_after(previous_run).unwrap_err();
            match (missing, journal_error(&error)) {
                (
                    true,
                    Some(JournalError::MissingSegment {
                        previous: 2,
                        found: 4,
                        ..
                    }),
                )
                | (
                    false,
                    Some(JournalError::CorruptFrame {
                        defect: FrameDefect::PayloadChecksum,
                        ..
                    }),
                ) => {}
                other => panic!("unexpected recovery result {other:?}"),
            }
            assert_eq!(
                core.segments(),
                before.iter().map(|(id, _)| *id).collect::<Vec<_>>()
            );
            for (id, bytes) in &before {
                assert_eq!(fs::read(core.segment(*id)).unwrap(), *bytes);
            }
        }
    }
}

#[tokio::test]
async fn a_newest_segment_without_a_whole_header_is_removed() {
    let core = Core::small(WalFsync::Always);
    let writer = core.writer();
    let mut store = core.store(&writer, 1);
    for index in 1..=30 {
        append(&mut store, [payload_entry(index, 512)]).await;
    }
    drop(store);
    drop(writer);
    let newest = *core.segments().last().expect("segments");
    fs::write(core.segment(newest + 1), b"URSJ").expect("a torn rotation");
    let writer = core.writer();
    let store = core.store(&writer, 1);
    assert_eq!(log_ids(&store).await, (1..=30).collect::<Vec<_>>());
    assert_eq!(core.segments().last(), Some(&newest));
}

#[tokio::test]
async fn core_file_log_rejects_a_second_owner() {
    let core = Core::small(WalFsync::Always);
    let first = core.writer();
    let err = core
        .open_after(PreviousRun::Absent)
        .expect_err("second core owner must fail");
    assert!(
        matches!(&err, CoreJournalError::Locked { owner: Some(owner), .. } if owner.starts_with("pid=")),
        "unexpected error: {err}"
    );
    assert_eq!(
        std::io::Error::from(err).kind(),
        std::io::ErrorKind::AlreadyExists
    );
    drop(first);
    drop(core.writer());
}

/// `wal_fsyncs` counts `fsync` calls, not batches: a batch of replay hints
/// is written without one, and the first entry of a group also replaces the
/// metadata file (the file and its directory).
#[tokio::test]
async fn wal_fsyncs_count_fsyncs_not_batches() {
    let core = Core::new(JournalTuning::new(WalFsync::Always));
    let writer = core.writer();
    let fsyncs_at_open = core.metrics.snapshot().wal_fsyncs;
    let mut store = core.store(&writer, 1);
    append(&mut store, [blank_entry(1)]).await;
    append(&mut store, [blank_entry(2)]).await;
    store.save_committed(Some(log_id(1))).await.expect("commit");
    let snapshot = core.metrics.snapshot();
    assert_eq!(snapshot.wal_batches, 3);
    assert_eq!(
        snapshot.wal_fsyncs.saturating_sub(fsyncs_at_open),
        1 + 2 + 1
    );
    assert_eq!(snapshot.wal_fsync_records, 2);
    assert_eq!(snapshot.wal_physical_bytes, file_len(&core.segment(1)));
}

/// Membership changes pay a journal sync under `never`; ordinary data does not.
#[tokio::test]
async fn never_fsync_syncs_membership_without_syncing_each_data_batch() {
    let core = Core::new(JournalTuning::new(WalFsync::Never));
    let writer = core.writer();
    let mut store = core.store(&writer, 1);
    append(&mut store, [blank_entry(1)]).await;
    let before_data = core.metrics.snapshot().wal_fsyncs;
    append(&mut store, [blank_entry(2)]).await;
    assert_eq!(core.metrics.snapshot().wal_fsyncs, before_data);
    let membership = openraft::Membership::new(
        vec![std::collections::BTreeSet::from([1, 2, 3])],
        std::collections::BTreeMap::from([
            (1, openraft::BasicNode::default()),
            (2, openraft::BasicNode::default()),
            (3, openraft::BasicNode::default()),
        ]),
    )
    .unwrap();
    let membership_entry = Entry::new(log_id(3), EntryPayload::Membership(membership));
    assert!(raft_group_log_record_requires_sync(
        &RaftGroupLogRecord::Append(vec![membership_entry.clone()]),
        WalFsync::Never
    ));
    assert!(!raft_group_log_record_requires_sync(
        &RaftGroupLogRecord::Append(vec![blank_entry(4)]),
        WalFsync::Never
    ));
    append(&mut store, [membership_entry]).await;
    let after_membership = core.metrics.snapshot().wal_fsyncs;
    assert_eq!(after_membership.checked_sub(before_data), Some(1));
    append(&mut store, [blank_entry(4)]).await;
    assert_eq!(core.metrics.snapshot().wal_fsyncs, after_membership);
}

/// Votes are kept in the core's metadata file, which is always `fsync`ed,
/// and never in the journal.
#[tokio::test]
async fn votes_live_in_the_metadata_file_not_the_journal() {
    let core = Core::new(JournalTuning::new(WalFsync::Never));
    let writer = core.writer();
    let mut store = core.store(&writer, 1);
    let journal_len = file_len(&core.segment(1));
    let fsyncs = core.metrics.snapshot().wal_fsyncs;
    let vote = committed_vote();
    store.save_vote(&vote).await.expect("save a vote");
    store.save_vote(&vote).await.expect("the same vote again");
    assert_eq!(
        file_len(&core.segment(1)),
        journal_len,
        "the journal holds no vote"
    );
    assert_eq!(
        core.metrics.snapshot().wal_fsyncs.saturating_sub(fsyncs),
        2,
        "one metadata replacement, even under fsync = never"
    );
    assert!(!store.initialized(), "a vote alone does not initialize");
    assert_eq!(
        CoreMetadata::load(&core_metadata_path(&core.dir))
            .expect("metadata")
            .group(1)
            .vote,
        Some(vote)
    );
    drop(store);
    drop(writer);

    let writer = core.writer();
    let mut store = core.store(&writer, 1);
    assert_eq!(store.read_vote().await.expect("read vote"), Some(vote));
    assert!(!store.initialized());
}

/// `initialized` becomes durable with a group's first entry and stays set
/// when the entries are gone.
#[tokio::test]
async fn initialized_is_durable_from_the_first_entry_and_never_cleared() {
    let core = Core::small(WalFsync::Always);
    let open = || {
        let writer = core.writer();
        core.store(&writer, 1)
    };
    let mut store = open();
    assert!(!store.initialized());
    append(&mut store, Vec::new()).await;
    assert!(!store.initialized(), "an empty append initializes nothing");
    append(&mut store, [blank_entry(1)]).await;
    assert!(store.initialized());
    drop(store);

    let mut store = open();
    assert_eq!(store.log_state(), GroupLogState::Initialized);
    store.truncate_after(None).await.expect("drop every entry");
    drop(store);
    let mut store = open();
    assert_eq!(
        store.get_log_state().await.expect("log state").last_log_id,
        None
    );
    assert!(store.initialized(), "the flag is never cleared");
    assert_eq!(
        store.log_state(),
        GroupLogState::Recovering,
        "an initialized group whose journal holds none of its log recovers"
    );
}

/// While a group's recovery gate is closed, an empty store's first entry
/// records the group recovering (its history is unknown). Opening the gate
/// durably records it initialized again.
#[tokio::test]
async fn an_unknown_history_records_recovering_until_the_gate_opens() {
    let core = Core::small(WalFsync::Always);
    let open = || {
        let writer = core.writer();
        core.store(&writer, 1)
    };
    let mut store = open();
    store.hold_unknown_history();
    assert_eq!(store.log_state(), GroupLogState::Empty);
    append(&mut store, [blank_entry(1)]).await;
    assert_eq!(store.log_state(), GroupLogState::Recovering);
    drop(store);

    let store = open();
    assert_eq!(store.log_state(), GroupLogState::Recovering);
    store
        .record_recovered()
        .await
        .expect("record the open gate");
    assert_eq!(store.log_state(), GroupLogState::Initialized);
    drop(store);
    assert_eq!(open().log_state(), GroupLogState::Initialized);
}

/// A recovering replica that led its group starts as a follower: its vote
/// for itself is recorded uncommitted before its Raft core starts. The
/// demotion is durable, so a clean shutdown and a restart that does not
/// demote again still find the vote uncommitted. A vote for another leader,
/// or one already uncommitted, is left as it is and nothing is written.
#[tokio::test]
async fn a_recovering_leaders_demotion_survives_a_clean_restart() {
    for (node_id, recorded, expected) in [
        (
            1,
            openraft::Vote::new_committed(7, 1),
            openraft::Vote::new(7, 1),
        ),
        (
            2,
            openraft::Vote::new_committed(7, 1),
            openraft::Vote::new_committed(7, 1),
        ),
        (1, openraft::Vote::new(7, 1), openraft::Vote::new(7, 1)),
    ] {
        let core = Core::small(WalFsync::Always);
        let metadata = core_metadata_path(&core.dir);
        let writer = core.writer();
        let mut store = core.store(&writer, 1);
        store.hold_unknown_history();
        append(&mut store, [blank_entry(1)]).await;
        store.save_vote(&recorded).await.expect("vote");
        drop(store);
        drop(writer);

        let writer = core.writer();
        let mut store = core.store(&writer, 1);
        assert_eq!(store.log_state(), GroupLogState::Recovering);
        let before = fs::read(&metadata).expect("metadata file");
        store
            .start_as_follower(node_id)
            .await
            .expect("record the demotion");
        assert_eq!(store.read_vote().await.expect("vote"), Some(expected));
        assert_eq!(
            fs::read(&metadata).expect("metadata file") == before,
            expected == recorded,
            "node {node_id}: the metadata file changes exactly when the vote is demoted"
        );
        drop(store);
        writer.close().await.expect("shut the writer down cleanly");
        drop(writer);

        let writer = core.writer();
        let mut store = core.store(&writer, 1);
        assert_eq!(
            store.read_vote().await.expect("vote"),
            Some(expected),
            "node {node_id} recorded {recorded:?}"
        );
        assert_eq!(store.log_state(), GroupLogState::Recovering);
    }
}

/// An initialized group also needs durable demotion after a process crash.
/// A clean stop before any election must not resurrect the old committed vote.
#[tokio::test]
async fn a_crashed_leaders_demotion_survives_an_immediate_clean_restart() {
    for fsync in [WalFsync::Always, WalFsync::Never] {
        for previous in [PreviousRun::ProcessCrash, PreviousRun::Clean] {
            let core = Core::small(fsync);
            let writer = core.writer();
            let mut store = core.store(&writer, 1);
            append(&mut store, [blank_entry(1)]).await;
            store.save_vote(&committed_vote()).await.expect("vote");
            drop(store);
            writer.close().await.expect("persist the initial state");
            drop(writer);

            let open = |previous_run| {
                let mut options = core.options(0, RecoveryState::Normal);
                options.previous_run = previous_run;
                CoreFileLogWriter::open(core.dir.clone(), options).expect("open writer")
            };
            let writer = open(previous);
            let mut store = core.store(&writer, 1);
            assert_eq!(store.log_state(), GroupLogState::Initialized);
            let gate = crate::GroupRejoin::durable(1, RaftGroupId(1), &store)
                .await
                .expect("prepare the group before starting Raft");
            let expected = if previous == PreviousRun::Clean {
                committed_vote()
            } else {
                openraft::Vote::new(7, 1)
            };
            assert_eq!(store.read_vote().await.expect("vote"), Some(expected));
            drop(gate);
            drop(store);
            writer
                .close()
                .await
                .expect("clean stop before any election");
            drop(writer);

            let writer = open(PreviousRun::Clean);
            let mut store = core.store(&writer, 1);
            let _gate = crate::GroupRejoin::durable(1, RaftGroupId(1), &store)
                .await
                .expect("clean restart");
            assert_eq!(store.read_vote().await.expect("vote"), Some(expected));
        }
    }
}

/// A crash between a group's first journal write and the metadata write
/// leaves entries without the flag. Missing metadata also means missing votes,
/// so recovery restores the flag as recovering regardless of the run state.
#[test]
fn recovery_restores_a_missing_initialized_flag() {
    for (node_recovery, expected) in [
        (RecoveryState::Normal, GroupLogState::Recovering),
        (
            RecoveryState::Recovering {
                reason: RecoveryReason::HostCrash,
            },
            GroupLogState::Recovering,
        ),
    ] {
        let core = Core::small(WalFsync::Never);
        fs::create_dir_all(&core.dir).expect("core dir");
        let mut journal = JournalWriter::open(&core.segment(1), 1).expect("segment 1");
        for record in [
            CoreJournalRecord {
                group_id: 7,
                record: RaftGroupLogRecord::Append(vec![blank_entry(1)]),
            },
            CoreJournalRecord {
                group_id: 9,
                record: RaftGroupLogRecord::SaveCommitted(None),
            },
        ] {
            journal
                .append::<WireCodec<CoreJournalRecord>>(&record)
                .expect("append");
        }
        journal.sync().expect("sync");
        drop(journal);
        assert!(!core_metadata_path(&core.dir).exists());

        let writer = CoreFileLogWriter::open(core.dir.clone(), core.options(0, node_recovery))
            .expect("open the core writer");
        assert_eq!(core.store(&writer, 7).log_state(), expected);
        assert_eq!(core.store(&writer, 9).log_state(), GroupLogState::Empty);
        let metadata = CoreMetadata::load(&core_metadata_path(&core.dir)).expect("metadata");
        assert_eq!(metadata.group(7).log, expected);
        assert_eq!(metadata.group(9).log, GroupLogState::Empty);
    }
}

/// Under `never` an append is acknowledged without a journal `fsync`;
/// closing the writer `fsync`s the journal and refuses later writes.
#[tokio::test]
async fn fsync_never_acknowledges_from_the_page_cache_and_close_syncs() {
    let core = Core::new(JournalTuning::new(WalFsync::Never));
    let writer = core.writer();
    let mut store = core.store(&writer, 1);
    let fsyncs = core.metrics.snapshot().wal_fsyncs;
    append(&mut store, [blank_entry(1)]).await;
    assert_eq!(
        core.metrics.snapshot().wal_fsyncs.saturating_sub(fsyncs),
        2,
        "only the metadata file that marks the group initialized"
    );
    append(&mut store, [blank_entry(2)]).await;
    assert_eq!(
        core.metrics.snapshot().wal_fsyncs.saturating_sub(fsyncs),
        2,
        "no journal fsync"
    );

    writer.close().await.expect("close the writer");
    use openraft::type_config::TypeConfigExt;
    let (flushed, result) = UrsulaRaftTypeConfig::oneshot();
    store
        .append([blank_entry(3)], IOFlushed::signal(flushed))
        .await
        .unwrap();
    let err = result
        .await
        .unwrap()
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
    let writer = core.writer();
    assert_eq!(log_ids(&core.store(&writer, 1)).await, [1, 2]);
}

#[tokio::test]
async fn a_group_opens_once_per_core_writer() {
    let core = Core::small(WalFsync::Always);
    let writer = core.writer();
    let mut store = core.store(&writer, 1);
    append(&mut store, [blank_entry(1)]).await;
    drop(store);
    let err = RaftGroupFileLogStore::open(
        placement(1),
        core.metrics.group_engine_metrics(),
        writer.clone(),
    )
    .expect_err("a group must not reopen while its core writer lives");
    assert!(matches!(err, CoreJournalError::GroupAlreadyOpen {
        raft_group_id: RaftGroupId(1),
        ..
    }));
    drop(core.store(&writer, 0));
    drop(writer);

    let writer = core.writer();
    let mut store = core.store(&writer, 1);
    assert_eq!(
        store.get_log_state().await.expect("log state").last_log_id,
        Some(log_id(1))
    );
}

/// A record that does not fit the group's log is refused by the writer and
/// never reaches the journal.
#[tokio::test]
async fn the_writer_refuses_a_record_that_does_not_fit_the_log() {
    let core = Core::small(WalFsync::Always);
    let writer = core.writer();
    let mut store = core.store(&writer, 1);
    append(&mut store, [blank_entry(1), blank_entry(2)]).await;
    let len = file_len(&core.segment(1));
    let err = store
        .append([blank_entry(4)], IOFlushed::noop())
        .await
        .expect_err("a hole");
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
    store.purge(log_id(2)).await.expect("purge");
    let err = store
        .purge(log_id(1))
        .await
        .expect_err("a purge cannot move back");
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
    assert!(
        file_len(&core.segment(1)) > len,
        "only the valid purge was written"
    );
    let records =
        super::read_wire_frames::<CoreJournalRecord>(&fs::read(core.segment(1)).expect("read"))
            .expect("decode");
    assert_eq!(records.len(), 2, "the append and the purge: {records:?}");
}

/// At the production segment size, purge keeps the journal near its live
/// log: a busy group writes several segments' worth while it purges, a quiet
/// group's entries are rewritten out of the oldest segment, and the journal
/// ends at a few segments.
#[tokio::test]
#[ignore = "writes several production-size WAL segments; run through scripts/soak_raft_wal.sh"]
async fn segment_reclaim_converges_at_production_size() {
    let core = Core::new(JournalTuning::new(WalFsync::Never));
    let writer = core.writer();
    let mut quiet = core.store(&writer, 1);
    let mut busy = core.store(&writer, 2);
    append(&mut quiet, (1..=4).map(|index| payload_entry(index, 4096))).await;
    let segment = JournalTuning::DEFAULT_SEGMENT_BYTES;
    let entry = 1024 * 1024;
    let entries = 8 * segment / u64::try_from(entry).expect("fits");
    for index in 1..=entries {
        append(&mut busy, [payload_entry(index, entry)]).await;
        if index % 16 == 0 {
            busy.purge(log_id(index.saturating_sub(8)))
                .await
                .expect("purge");
        }
    }
    writer.close().await.expect("close the writer");
    let snapshot = core.metrics.snapshot();
    println!(
        "production-size reclaim: physical_bytes={} segments={} reclaims={} reclaimed_bytes={} \
         rewritten_bytes={}",
        snapshot.wal_physical_bytes,
        snapshot.wal_segments,
        snapshot.wal_reclaims,
        snapshot.wal_reclaimed_bytes,
        snapshot.wal_rewritten_bytes
    );
    assert!(snapshot.wal_reclaims >= 4, "{snapshot:?}");
    assert!(
        snapshot.wal_rewritten_bytes > 0,
        "the quiet group was rewritten"
    );
    assert!(
        snapshot.wal_physical_bytes <= 6 * segment,
        "the journal stays near its live log: {} bytes",
        snapshot.wal_physical_bytes
    );
    drop((quiet, busy));
    drop(writer);
    let writer = core.writer();
    assert_eq!(log_ids(&core.store(&writer, 1)).await, [1, 2, 3, 4]);
}

/// Hold the real writer between records. Append must publish readable entries
/// without waiting for this writer, but cannot announce durability early.
#[tokio::test]
async fn append_is_readable_before_the_writer_runs_and_flush_orders_truncate() {
    use openraft::type_config::TypeConfigExt;

    use super::writer::CoreWriteOp;

    let core = Core::new(JournalTuning::new(WalFsync::Always));
    let writer = core.writer();
    let mut store = core.store(&writer, 0);
    let (entered, wait_entered) = tokio::sync::oneshot::channel();
    let (release, wait_release) = std::sync::mpsc::channel::<()>();
    // Dropping the sender releases the writer even if an assertion unwinds.
    writer.submit(
        CoreWriteOp::Vote {
            group_id: 0,
            vote: committed_vote(),
        },
        move |result| {
            result.expect("initial vote");
            entered.send(()).expect("test is waiting");
            let _released = wait_release.recv();
        },
    );
    wait_entered.await.expect("writer reached barrier");
    let (flushed, mut flush_result) = UrsulaRaftTypeConfig::oneshot();
    store
        .append([blank_entry(1), blank_entry(2)], IOFlushed::signal(flushed))
        .await
        .expect("submit append");
    assert!(matches!(
        flush_result.try_recv(),
        Err(tokio::sync::oneshot::error::TryRecvError::Empty)
    ));
    assert_eq!(log_ids(&store).await, [1, 2]);
    assert_eq!(
        store
            .try_get_log_entries(2..3)
            .await
            .unwrap()
            .into_iter()
            .map(|entry| entry.log_id.index)
            .collect::<Vec<_>>(),
        [2]
    );
    assert_eq!(
        store.get_key_log_ids(log_id(2)..=log_id(2)).await.unwrap(),
        [log_id(2)]
    );
    assert_eq!(
        store.get_log_state().await.unwrap().last_log_id,
        Some(log_id(2))
    );
    assert_eq!(
        store.get_key_log_ids(log_id(1)..=log_id(2)).await.unwrap(),
        [log_id(2)]
    );
    let mut truncating = store.clone();
    let truncate = tokio::spawn(async move { truncating.truncate_after(Some(log_id(1))).await });
    tokio::task::yield_now().await;
    assert!(!truncate.is_finished());
    drop(release);
    flush_result
        .await
        .expect("callback delivered")
        .expect("durable append");
    truncate.await.unwrap().unwrap();
    assert_eq!(log_ids(&store).await, [1]);
    writer.close().await.unwrap();
}

#[tokio::test]
async fn replication_reaches_followers_while_the_leader_journal_is_paused() {
    use std::collections::BTreeMap;
    use std::time::Duration;

    use openraft::BasicNode;
    use openraft::rt::WatchReceiver;

    use super::writer::CoreWriteOp;
    use crate::InProcessRaftNetworkFactory;
    use crate::InProcessRaftRegistry;
    use crate::RaftGroupEngine;

    let cores = (0..3)
        .map(|_| Core::new(JournalTuning::new(WalFsync::Always)))
        .collect::<Vec<_>>();
    let writers = cores.iter().map(Core::writer).collect::<Vec<_>>();
    let network = InProcessRaftRegistry::default();
    let config = Arc::new(
        openraft::Config {
            enable_tick: false,
            ..Default::default()
        }
        .validate()
        .unwrap(),
    );
    let mut engines = Vec::new();
    let mut members = BTreeMap::new();
    for (index, (core, writer)) in cores.iter().zip(&writers).enumerate() {
        let id = u64::try_from(index).unwrap().saturating_add(1);
        members.insert(id, BasicNode::new(format!("node-{id}")));
        let engine = RaftGroupEngine::new_node_with_log_store_and_network(
            placement(0),
            id,
            config.clone(),
            InProcessRaftNetworkFactory::new(network.clone()).with_source(id),
            core.store(writer, 0),
            None,
            None,
        )
        .await
        .unwrap();
        network.register(id, &engine);
        engines.push(engine);
    }
    let leader = engines[0].raft_handle();
    leader.initialize(members).await.unwrap();
    leader.trigger().elect(false).await.unwrap();
    leader
        .wait(Some(Duration::from_secs(5)))
        .current_leader(1, "leader elected")
        .await
        .unwrap();
    let response = leader
        .client_write(GroupWriteCommand::Stream(StreamCommand::CreateStream {
            stream_id: ursula_shard::BucketStreamId::new("before-pause", "events"),
            content_type: "application/octet-stream".to_owned(),
            initial_payload: bytes::Bytes::new(),
            close_after: false,
            stream_seq: None,
            producer: None,
            stream_ttl_seconds: None,
            stream_expires_at_ms: None,
            now_ms: 0,
        }))
        .await
        .unwrap();
    assert!(matches!(
        response.data,
        crate::RaftGroupResponse::Write(Ok(ursula_runtime::GroupWriteResponse::CreateStream(_)))
    ));
    let before = leader.metrics().borrow_watched().last_log_index;
    let vote = leader.metrics().borrow_watched().vote;
    let (entered, wait_entered) = tokio::sync::oneshot::channel();
    let (release, wait_release) = std::sync::mpsc::channel::<()>();
    writers[0].submit(CoreWriteOp::Vote { group_id: 0, vote }, move |result| {
        result.unwrap();
        entered.send(()).unwrap();
        let _released = wait_release.recv();
    });
    wait_entered.await.unwrap();
    let writing = leader.clone();
    let append = tokio::spawn(async move {
        writing
            .client_write(GroupWriteCommand::Stream(StreamCommand::CreateStream {
                stream_id: ursula_shard::BucketStreamId::new("during-pause", "events"),
                content_type: "application/octet-stream".to_owned(),
                initial_payload: bytes::Bytes::new(),
                close_after: false,
                stream_seq: None,
                producer: None,
                stream_ttl_seconds: None,
                stream_expires_at_ms: None,
                now_ms: 0,
            }))
            .await
    });
    // This times out with the synchronous append implementation: Replicate
    // cannot run until log_store.append returns.
    engines[1]
        .raft_handle()
        .wait(Some(Duration::from_secs(2)))
        .metrics(
            |metrics| metrics.last_applied.map(|id| id.index) > before,
            "follower durable apply overlaps leader WAL I/O",
        )
        .await
        .unwrap();
    drop(release);
    let response = append.await.unwrap().unwrap();
    assert!(matches!(
        response.data,
        crate::RaftGroupResponse::Write(Ok(ursula_runtime::GroupWriteResponse::CreateStream(_)))
    ));
    for engine in engines {
        engine.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn missing_metadata_beside_a_nonempty_journal_closes_the_recovery_gate() {
    let core = Core::small(WalFsync::Always);
    let writer = core.writer();
    let mut store = core.store(&writer, 1);
    append(&mut store, [blank_entry(1)]).await;
    store
        .save_vote(&openraft::Vote::new_committed(5, 2))
        .await
        .expect("vote");
    drop((store, writer));
    fs::remove_file(core.dir.join("journal.meta")).expect("lose metadata");
    let writer = core.writer();
    let mut store = core.store(&writer, 1);
    assert_eq!(store.log_state(), GroupLogState::Recovering);
    assert_eq!(store.read_vote().await.expect("vote"), None);
    let gate = crate::GroupRejoin::durable(1, RaftGroupId(1), &store)
        .await
        .expect("gate");
    assert!(!gate.vote_gate_open());
}
/// Records the run that holds `root` as poisoned, as a journal writer's I/O
/// failure does, so the next run starts a new recovery epoch.
fn record_poisoned(root: &Path) {
    let path = root.join(super::RUN_STATE_FILE);
    let running = super::state_file::read::<RunState>(super::StateFileKind::RunState, &path)
        .expect("read the run state")
        .expect("a run holds the WAL root");
    assert_eq!(running.status, RunStatus::Running);
    RunStateFile::new(path, running)
        .record(RunStatus::Poisoned, "poisoned")
        .expect("record the run as poisoned");
}

fn core_verified_epoch(wal: &super::RaftWal, core: u16) -> u64 {
    CoreMetadata::load(&core_metadata_path(&wal.core_dir(CoreId(core))))
        .expect("read core metadata")
        .verified_epoch()
}

/// Every core's metadata keeps the epoch in which a run last read it. A lost
/// run state must not restart the count below any of them, however often it
/// is lost and whichever core holds the greatest epoch.
#[tokio::test]
async fn missing_run_state_advances_past_every_surviving_core_epoch() {
    for fsync in [WalFsync::Always, WalFsync::Never] {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let map = ursula_shard::StaticShardMap::new(2, 2).unwrap();
        let metrics = RuntimeMetrics::new(2, 2);
        let open = |wal: &super::RaftWal, group: u32| {
            wal.open(
                map.placement(RaftGroupId(group)).unwrap(),
                metrics.group_engine_metrics(),
            )
            .unwrap()
        };
        let wal = super::RaftWal::start(root, fsync, &map).unwrap();
        for group in [0, 1] {
            let mut store = open(&wal, group);
            append(&mut store, [blank_entry(1)]).await;
        }
        record_poisoned(root);
        drop(wal);
        // Each poisoned run starts an epoch and reads only the core it opens:
        // core 0 last in epoch 1, core 1 last in epoch 3.
        for (epoch, group) in [(1, 0), (2, 1), (3, 1)] {
            let wal = super::RaftWal::start(root, fsync, &map).unwrap();
            assert_eq!(wal.opening().recovery_epoch, epoch);
            drop(open(&wal, group));
            assert_eq!(
                core_verified_epoch(&wal, u16::try_from(group).unwrap()),
                epoch
            );
            record_poisoned(root);
        }
        // The greatest surviving epoch is core 1's at the first loss and core
        // 0's at the second.
        for expected in [4, 5] {
            fs::remove_file(root.join(super::RUN_STATE_FILE)).unwrap();
            let wal = super::RaftWal::start(root, fsync, &map).unwrap();
            let opening = wal.opening();
            assert_eq!(opening.previous_run, PreviousRun::Unrecorded);
            assert_eq!(opening.replay_mode, JournalReplayMode::VerifiedPrefix);
            assert_eq!(opening.recovery_epoch, expected);
            let store = open(&wal, 0);
            assert_eq!(log_ids(&store).await, [1]);
            assert_eq!(store.log_state(), GroupLogState::Recovering);
            assert_eq!(core_verified_epoch(&wal, 0), expected);
            assert_eq!(core_verified_epoch(&wal, 1), 3);
            drop((store, wal));
        }
    }
}

/// Without a run state, metadata that cannot be read or an epoch that
/// cannot advance refuses startup before anything is written.
#[tokio::test]
async fn missing_run_state_refuses_unreadable_or_exhausted_core_epochs() {
    for fsync in [WalFsync::Always, WalFsync::Never] {
        for corrupt in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let root = dir.path();
            let map = ursula_shard::StaticShardMap::new(1, 1).unwrap();
            let placement = map.placement(RaftGroupId(0)).unwrap();
            let metrics = RuntimeMetrics::new(1, 1);
            // A poisoned run at the last epoch but one; the next run reads
            // the core in the last epoch.
            drop(super::RaftWal::start(root, fsync, &map).unwrap());
            RunStateFile::new(root.join(super::RUN_STATE_FILE), RunState {
                boot_id: None,
                fsync,
                status: RunStatus::Poisoned,
                recovery_epoch: u64::MAX.checked_sub(1).unwrap(),
            })
            .record(RunStatus::Poisoned, "poisoned")
            .unwrap();
            let wal = super::RaftWal::start(root, fsync, &map).unwrap();
            assert_eq!(wal.opening().recovery_epoch, u64::MAX);
            let mut store = wal.open(placement, metrics.group_engine_metrics()).unwrap();
            append(&mut store, [blank_entry(1)]).await;
            assert_eq!(core_verified_epoch(&wal, 0), u64::MAX);
            drop((store, wal));
            let path = core_metadata_path(&super::core_dir(root, 0));
            if corrupt {
                fs::write(&path, b"broken core metadata").unwrap();
            }
            fs::remove_file(root.join(super::RUN_STATE_FILE)).unwrap();
            let before = fs::read(&path).unwrap();
            for _ in 0..2 {
                let error = super::RaftWal::start(root, fsync, &map).unwrap_err();
                if corrupt {
                    assert!(
                        matches!(error, super::RaftWalError::ReadCoreMetadata(_)),
                        "{error}"
                    );
                } else {
                    assert!(
                        matches!(error, super::RaftWalError::RecoveryEpochExhausted {
                            recovery_epoch: u64::MAX
                        }),
                        "{error}"
                    );
                }
                assert!(!root.join(super::RUN_STATE_FILE).exists());
                assert_eq!(fs::read(&path).unwrap(), before);
            }
        }
    }
}

#[tokio::test]
async fn an_exhausted_recorded_epoch_refuses_a_new_poisoned_recovery() {
    for fsync in [WalFsync::Always, WalFsync::Never] {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let map = ursula_shard::StaticShardMap::new(1, 3).unwrap();
        drop(super::RaftWal::start(root, fsync, &map).unwrap());
        let path = root.join(super::RUN_STATE_FILE);
        RunStateFile::new(path.clone(), RunState {
            boot_id: None,
            fsync,
            status: RunStatus::Poisoned,
            recovery_epoch: u64::MAX,
        })
        .record(RunStatus::Poisoned, "exhausted epoch")
        .unwrap();
        let before = fs::read(&path).unwrap();
        assert!(matches!(
            super::RaftWal::start(root, fsync, &map),
            Err(super::RaftWalError::RecoveryEpochExhausted {
                recovery_epoch: u64::MAX
            })
        ));
        assert_eq!(fs::read(&path).unwrap(), before);
    }
}

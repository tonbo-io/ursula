//! Journal segments that hold dead frames are freed for their age (#669),
//! on the disk of the build: the operating system's under a temporary
//! directory, or the simulated disk under `cfg(madsim)`, where a power loss
//! can also cut a rewrite short.

use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use openraft::EntryPayload;
use openraft::LogId;
use openraft::alias::EntryOf;
use openraft::alias::LogIdOf;
use openraft::entry::RaftEntry;
use openraft::storage::IOFlushed;
use openraft::storage::RaftLogReader;
use openraft::storage::RaftLogStorage;
use openraft::vote::RaftLeaderId;
use openraft::vote::leader_id_adv::CommittedLeaderId;
use ursula_config::WalFsync;
use ursula_runtime::RuntimeMetrics;
use ursula_shard::BucketStreamId;
use ursula_shard::CoreId;
use ursula_shard::RaftGroupId;
use ursula_shard::ShardId;
use ursula_shard::ShardPlacement;
use ursula_stream::StreamCommand;

use super::JournalRun;
use super::RaftGroupFileLogStore;
use super::RecoveryState;
use super::RunState;
use super::RunStatus;
use super::disk::Disk;
use super::disk::JournalDisk;
use super::disk::JournalFile;
use super::run_state::PreviousRun;
use super::run_state::RunStateFile;
use super::writer::CoreFileLogWriter;
use super::writer::CoreJournalOptions;
use super::writer::JournalTuning;
use super::writer::LaggingGroups;
use crate::RaftCommand;
use crate::rt::time::Instant;
use crate::types::UrsulaRaftTypeConfig;

type Entry = EntryOf<UrsulaRaftTypeConfig>;

/// The bytes every record of the removed group carries.
const MARKER: &[u8] = b"dead-frame-marker-669;";
/// The age of dead frames that frees their segment.
const MAX_AGE: Duration = Duration::from_secs(900);
/// More maintenance passes than any test here needs to settle.
const PASSES: usize = 64;
/// The group whose records are removed.
const REMOVED: u32 = 1;
/// A group that writes a few entries and then nothing.
const QUIET: u32 = 2;

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

fn entry(index: u64, payload: &[u8]) -> Entry {
    Entry::new(
        log_id(index),
        EntryPayload::Normal(RaftCommand::Stream(StreamCommand::Append {
            stream_id: BucketStreamId::new("wal-test", "dirty"),
            content_type: Some("application/octet-stream".to_owned()),
            payload: Bytes::copy_from_slice(payload),
            close_after: false,
            stream_seq: None,
            producer: None,
            now_ms: 0,
        })),
    )
}

/// A marked payload of about `bytes`.
fn marked(bytes: usize) -> Vec<u8> {
    MARKER.repeat(bytes.div_ceil(MARKER.len()))
}

/// `by` after `time`.
fn later(time: Instant, by: Duration) -> Instant {
    time.checked_add(by).expect("the clock reaches it")
}

/// A core journal of the smallest segments in a fresh directory.
struct Journal {
    #[cfg(not(madsim))]
    _root: tempfile::TempDir,
    dir: PathBuf,
    metrics: RuntimeMetrics,
    lagging: Arc<LaggingGroups>,
    tuning: JournalTuning,
}

impl Journal {
    fn new(name: &str, fsync: WalFsync) -> Self {
        #[cfg(not(madsim))]
        let (root, dir) = {
            let _ = name;
            let root = tempfile::tempdir().expect("WAL root");
            let dir = root.path().join("core-0");
            (root, dir)
        };
        #[cfg(madsim)]
        let dir = super::SimDisk::provision_dir(name)
            .expect("provision a simulated WAL root")
            .join("core-0");
        Self {
            #[cfg(not(madsim))]
            _root: root,
            dir,
            metrics: RuntimeMetrics::new(1, 8),
            lagging: Arc::new(LaggingGroups::default()),
            tuning: JournalTuning {
                fsync,
                segment_bytes: JournalTuning::MIN_SEGMENT_BYTES,
                group_cache_bytes: 1024,
            },
        }
    }

    fn writer(&self) -> Arc<CoreFileLogWriter> {
        CoreFileLogWriter::open(self.dir.clone(), CoreJournalOptions {
            previous_run: PreviousRun::Absent,
            core: CoreId(0),
            tuning: self.tuning,
            recovery_epoch: 0,
            run_state: Arc::new(RunStateFile::new(
                self.dir.with_extension("run-state"),
                RunState {
                    boot_id: None,
                    status: RunStatus::Running,
                    data: JournalRun {
                        fsync: self.tuning.fsync,
                        recovery_epoch: 0,
                    },
                    meta: JournalRun {
                        fsync: WalFsync::Always,
                        recovery_epoch: 0,
                    },
                },
            )),
            node_recovery: RecoveryState::Normal,
            lagging: self.lagging.clone(),
            metrics: Some((placement(0), self.metrics.group_engine_metrics())),
        })
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

    /// The journal's segment files, oldest first.
    fn segments(&self) -> Vec<PathBuf> {
        super::segment::list_segments(&self.dir)
            .expect("list segments")
            .into_iter()
            .map(|id| super::segment::segment_path(&self.dir, id))
            .collect()
    }

    /// The newest segment, which appends go to.
    fn active(&self) -> PathBuf {
        self.segments().last().cloned().expect("an active segment")
    }

    /// Every file of the journal that holds [`MARKER`].
    fn files_with_marker(&self) -> Vec<PathBuf> {
        Disk::read_dir(&self.dir)
            .expect("list the journal directory")
            .into_iter()
            .filter(|path| holds_marker(path))
            .collect()
    }

    /// Runs maintenance passes as of `now` until no file holds the marker,
    /// and returns how many it took.
    async fn maintain_until_clean(&self, writer: &CoreFileLogWriter, now: Instant) -> usize {
        for pass in 1..=PASSES {
            writer
                .maintain(now, MAX_AGE)
                .await
                .expect("a maintenance pass");
            if self.files_with_marker().is_empty() {
                return pass;
            }
        }
        panic!(
            "files still hold dead frames after {PASSES} passes: {:?}",
            self.files_with_marker()
        );
    }
}

/// Whether the file at `path` holds [`MARKER`]; a directory holds nothing.
fn holds_marker(path: &Path) -> bool {
    let Ok(mut file) = Disk::open_read(path) else {
        return false;
    };
    let Ok(len) = file.file_len() else {
        return false;
    };
    let mut bytes = vec![0; usize::try_from(len).unwrap_or(0)];
    file.read_exact(&mut bytes).is_ok()
        && bytes.windows(MARKER.len()).any(|window| window == MARKER)
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

/// A quiet group writes three entries, then a removed group writes marked
/// entries over several segments, until one fills the segment it went to,
/// and purges them all: every marked entry is in a sealed segment, and the
/// active segment holds the purge alone. The journal stays far below its
/// budget. Maintenance passes at `start` order the writes with the
/// writer's own passes. Returns the stores of the quiet and the removed
/// group.
async fn write_and_remove(
    journal: &Journal,
    writer: &Arc<CoreFileLogWriter>,
    start: Instant,
) -> (Arc<RaftGroupFileLogStore>, Arc<RaftGroupFileLogStore>) {
    let mut quiet = journal.store(writer, QUIET);
    let mut removed = journal.store(writer, REMOVED);
    append(&mut quiet, (1..=3).map(|index| entry(index, b"quiet;"))).await;
    quiet.save_committed(Some(log_id(3))).await.expect("commit");
    let mut last = 0;
    for index in 1..=40 {
        append(&mut removed, [entry(index, &marked(500))]).await;
        writer.maintain(start, MAX_AGE).await.expect("a pass");
        last = index;
        if index >= 8 && journal.segments().len() >= 3 && !holds_marker(&journal.active()) {
            break;
        }
    }
    assert!(
        !holds_marker(&journal.active()),
        "the marked entries are sealed"
    );
    removed.purge(log_id(last)).await.expect("purge");
    writer.maintain(start, MAX_AGE).await.expect("a pass");
    assert_eq!(
        journal.metrics.snapshot().wal_rewritten_bytes,
        0,
        "the journal is within its budget"
    );
    (quiet, removed)
}

/// A removed group's records leave every segment within the age once they
/// are dead, though the journal is far below its budget: the quiet group
/// that pins the oldest segment has its entries copied forward, a pass
/// copies at most its budget, and an idle journal then settles.
async fn dead_frames_leave_every_segment(name: &str) {
    let journal = Journal::new(name, WalFsync::Never);
    let writer = journal.writer();
    let start = Instant::now();
    let (quiet, removed) = write_and_remove(&journal, &writer, start).await;
    let first = journal.segments().first().cloned().expect("a segment");
    // The last pass stamped the deaths; they are not old yet.
    writer
        .maintain(later(start, Duration::from_secs(450)), MAX_AGE)
        .await
        .expect("a pass");
    assert!(
        !journal.files_with_marker().is_empty(),
        "dead frames younger than the age stay"
    );
    assert!(journal.metrics.snapshot().wal_dirty_segments > 0);
    assert_eq!(journal.metrics.snapshot().wal_rewritten_bytes, 0);

    let expired = later(start, MAX_AGE);
    let mut rewritten = 0;
    for _ in 1..=PASSES {
        writer.maintain(expired, MAX_AGE).await.expect("a pass");
        let now = journal.metrics.snapshot().wal_rewritten_bytes;
        // A pass stops copying once it copied its budget, an eighth of a
        // segment, so it copies at most that and one more chunk.
        assert!(
            now.saturating_sub(rewritten) <= 1280,
            "a pass copied {} bytes",
            now.saturating_sub(rewritten)
        );
        rewritten = now;
        if journal.files_with_marker().is_empty() {
            break;
        }
    }
    assert_eq!(journal.files_with_marker(), Vec::<PathBuf>::new());
    assert!(
        rewritten > 0,
        "the quiet group's entries were copied forward"
    );
    assert!(
        !Disk::exists(&first),
        "the oldest segment, which the quiet group pinned, is gone"
    );
    assert_eq!(log_ids(&quiet).await, [1, 2, 3]);
    assert_eq!(log_ids(&removed).await, Vec::<u64>::new());
    // Settled: nothing is dead any more, and further passes copy nothing.
    for _ in 0..4 {
        writer.maintain(expired, MAX_AGE).await.expect("a pass");
    }
    let snapshot = journal.metrics.snapshot();
    assert_eq!(snapshot.wal_dirty_segments, 0);
    assert_eq!(
        snapshot.wal_rewritten_bytes, rewritten,
        "an idle core settles"
    );
    assert!(journal.lagging.groups().is_empty());
    drop((quiet, removed));
    writer.close().await.expect("close the writer");
    drop(writer);

    // The copies replay in place of the originals.
    let writer = journal.writer();
    let quiet = journal.store(&writer, QUIET);
    assert_eq!(log_ids(&quiet).await, [1, 2, 3]);
    assert!(journal.files_with_marker().is_empty());
}

/// After a reopen, replay finds the dead frames again and their age is
/// their segment's modification time: frames that died long ago are freed
/// at the first passes, recent ones only once they are old.
async fn dead_frames_age_from_the_modification_time_after_a_reopen(name: &str) {
    let journal = Journal::new(name, WalFsync::Always);
    let writer = journal.writer();
    let (quiet, removed) = write_and_remove(&journal, &writer, Instant::now()).await;
    drop((quiet, removed));
    writer.close().await.expect("close the writer");
    drop(writer);

    // Just written: the frames are young.
    let writer = journal.writer();
    let quiet = journal.store(&writer, QUIET);
    writer
        .maintain(Instant::now(), MAX_AGE)
        .await
        .expect("a pass");
    assert!(
        !journal.files_with_marker().is_empty(),
        "frames that just died stay"
    );
    assert!(journal.metrics.snapshot().wal_dirty_segments > 0);
    drop(quiet);
    writer.close().await.expect("close the writer");
    drop(writer);

    age_the_journal(&journal, MAX_AGE.saturating_mul(2)).await;
    let writer = journal.writer();
    let quiet = journal.store(&writer, QUIET);
    journal.maintain_until_clean(&writer, Instant::now()).await;
    assert_eq!(log_ids(&quiet).await, [1, 2, 3]);
}

/// A newest segment that holds dead frames is sealed at open, so that a
/// restart never keeps them in a segment that only grows, and the next
/// passes free it like any other.
async fn a_dirty_newest_segment_is_sealed_at_open(name: &str) {
    let journal = Journal::new(name, WalFsync::Always);
    let writer = journal.writer();
    let mut removed = journal.store(&writer, REMOVED);
    append(
        &mut removed,
        (1..=3).map(|index| entry(index, &marked(200))),
    )
    .await;
    removed.purge(log_id(3)).await.expect("purge");
    drop(removed);
    writer.close().await.expect("close the writer");
    drop(writer);
    let before = journal.segments();
    assert_eq!(before.len(), 1, "the dead frames are in the newest segment");

    let writer = journal.writer();
    // The open's own pass ran before this one answers.
    let start = Instant::now();
    writer.maintain(start, MAX_AGE).await.expect("a pass");
    let after = journal.segments();
    assert_eq!(
        after.len(),
        2,
        "the dirty newest segment was sealed: {after:?}"
    );
    assert_eq!(after.first(), before.first());
    assert!(!holds_marker(&journal.active()));
    journal
        .maintain_until_clean(&writer, later(start, MAX_AGE))
        .await;
    let removed = journal.store(&writer, REMOVED);
    assert_eq!(log_ids(&removed).await, Vec::<u64>::new());
}

/// Makes every file of the journal `by` older than now: on the operating
/// system's disk its modification time moves back, on the simulated disk
/// time moves on.
#[cfg(not(madsim))]
async fn age_the_journal(journal: &Journal, by: Duration) {
    let modified = Disk::wall_clock()
        .checked_sub(by)
        .expect("the wall clock reaches back");
    for path in journal.segments() {
        std::fs::File::options()
            .write(true)
            .open(&path)
            .expect("open a segment")
            .set_modified(modified)
            .expect("move its modification time back");
    }
}

#[cfg(madsim)]
async fn age_the_journal(_journal: &Journal, by: Duration) {
    madsim::time::sleep(by).await;
}

#[cfg(not(madsim))]
#[tokio::test]
async fn a_removed_groups_frames_leave_every_segment_on_disk() {
    dead_frames_leave_every_segment("dead-frames").await;
}

#[cfg(not(madsim))]
#[tokio::test]
async fn dead_frames_age_from_the_modification_time_on_disk() {
    dead_frames_age_from_the_modification_time_after_a_reopen("dead-frames-age").await;
}

#[cfg(not(madsim))]
#[tokio::test]
async fn a_dirty_newest_segment_is_sealed_at_open_on_disk() {
    a_dirty_newest_segment_is_sealed_at_open("dirty-newest").await;
}

#[cfg(madsim)]
#[test]
fn a_dirty_newest_segment_is_sealed_at_a_simulated_open() {
    crate::tests::check_madsim_determinism(672, madsim::Config::default(), || async {
        crate::sim_runtime::MadsimOpenRaftRuntime::scope(
            672,
            a_dirty_newest_segment_is_sealed_at_open("dirty-newest"),
        )
        .await;
    });
}

#[cfg(madsim)]
#[test]
fn a_removed_groups_frames_leave_every_simulated_segment() {
    crate::tests::check_madsim_determinism(669, madsim::Config::default(), || async {
        crate::sim_runtime::MadsimOpenRaftRuntime::scope(
            669,
            dead_frames_leave_every_segment("dead-frames"),
        )
        .await;
    });
}

#[cfg(madsim)]
#[test]
fn dead_frames_age_from_the_simulated_modification_time() {
    crate::tests::check_madsim_determinism(670, madsim::Config::default(), || async {
        crate::sim_runtime::MadsimOpenRaftRuntime::scope(
            670,
            dead_frames_age_from_the_modification_time_after_a_reopen("dead-frames-age"),
        )
        .await;
    });
}

/// A power loss in the middle of a rewrite loses no live entry and leaves
/// no hole, under either fsync policy: copies that were not durable yet are
/// lost with the power and the originals stay, copies that were durable
/// stand in for originals that were not deleted yet or whose deletion was,
/// and a deletion whose directory `fsync` failed comes back with the purge
/// that freed it. Under `always` no purged entry comes back. Under `never` a
/// purge that was not synced may be lost, and then the entries it purged
/// come back whole, never with a hole. In every case the next passes free
/// the dead frames.
#[cfg(madsim)]
#[test]
fn a_power_loss_mid_rewrite_loses_no_live_entry() {
    crate::tests::check_madsim_determinism(671, madsim::Config::default(), || async {
        crate::sim_runtime::MadsimOpenRaftRuntime::scope(671, async {
            for fsync in [WalFsync::Always, WalFsync::Never] {
                for cut in [
                    RewriteCut::BeforeCopiesSync,
                    RewriteCut::BeforeOriginalsDeleted,
                    RewriteCut::AfterOriginalsDeleted,
                    RewriteCut::DeletionNotDurable,
                ] {
                    power_loss_mid_rewrite(cut, fsync).await;
                }
            }
        })
        .await;
    });
}

/// Where [`a_power_loss_mid_rewrite_loses_no_live_entry`] cuts the power.
#[cfg(madsim)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RewriteCut {
    /// The `fsync` of the copies fails, which stops the writer.
    BeforeCopiesSync,
    /// After the pass that deleted the originals' segment, losing all that
    /// was not synced: only the synced copies hold the quiet group's
    /// entries.
    AfterOriginalsDeleted,
    /// The `fsync` of the directory after the deletion of the originals'
    /// segment fails, which stops the writer, so the deletion may not be
    /// durable.
    DeletionNotDurable,
    /// After the pass that made the copies durable, before a later pass
    /// deletes the segment of the originals.
    BeforeOriginalsDeleted,
}

#[cfg(madsim)]
async fn power_loss_mid_rewrite(cut: RewriteCut, fsync: WalFsync) {
    use super::SimDisk;
    use super::SimDiskFault;

    let context = format!("{cut:?} under {fsync:?}");
    let journal = Journal::new(&format!("power-loss-mid-rewrite-{cut:?}-{fsync:?}"), fsync);
    let root = journal.dir.parent().expect("a WAL root").to_path_buf();
    let writer = journal.writer();
    let start = Instant::now();
    let (quiet, removed) = write_and_remove(&journal, &writer, start).await;
    // The active segment holds no dead frame, so the next pass seals
    // nothing and copies the quiet group's entries into it.
    let active = journal.active();
    match cut {
        RewriteCut::BeforeCopiesSync => {
            SimDisk::inject_fault(&active, SimDiskFault::Sync).expect("arm the fault");
            assert!(
                writer
                    .maintain(later(start, MAX_AGE), MAX_AGE)
                    .await
                    .is_err(),
                "{context}: the failed fsync of the copies stops the writer"
            );
        }
        RewriteCut::BeforeOriginalsDeleted => {
            writer
                .maintain(later(start, MAX_AGE), MAX_AGE)
                .await
                .expect("a pass");
            assert!(
                journal.metrics.snapshot().wal_rewritten_bytes > 0,
                "{context}: the pass made copies"
            );
            writer.abort_simulated();
        }
        RewriteCut::AfterOriginalsDeleted | RewriteCut::DeletionNotDurable => {
            let first = journal.segments().first().cloned().expect("a segment");
            writer
                .maintain(later(start, MAX_AGE), MAX_AGE)
                .await
                .expect("the pass that copies");
            if cut == RewriteCut::DeletionNotDurable {
                SimDisk::inject_fault(&journal.dir, SimDiskFault::Sync).expect("arm the fault");
            }
            let deleted = writer.maintain(later(start, MAX_AGE), MAX_AGE).await;
            assert!(
                !Disk::exists(&first),
                "{context}: the pass deleted the originals"
            );
            assert_eq!(
                deleted.is_err(),
                cut == RewriteCut::DeletionNotDurable,
                "{context}: {deleted:?}"
            );
            writer.abort_simulated();
        }
    }
    drop((quiet, removed));
    drop(writer);
    match cut {
        RewriteCut::AfterOriginalsDeleted => {
            SimDisk::power_loss_losing_unsynced(&root).expect("cut the power");
        }
        RewriteCut::BeforeCopiesSync
        | RewriteCut::BeforeOriginalsDeleted
        | RewriteCut::DeletionNotDurable => {
            SimDisk::power_loss(&root).expect("cut the power");
        }
    }
    age_the_journal(&journal, MAX_AGE.saturating_mul(2)).await;

    // The journal opens: a hole in any group's log would refuse it.
    let writer = journal.writer();
    let quiet = journal.store(&writer, QUIET);
    let mut removed = journal.store(&writer, REMOVED);
    assert_eq!(log_ids(&quiet).await, [1, 2, 3], "{context}");
    let resurfaced = log_ids(&removed).await;
    match fsync {
        WalFsync::Always => assert_eq!(
            resurfaced,
            Vec::<u64>::new(),
            "{context}: no purged entry comes back"
        ),
        // A lost purge brings its entries back whole: purge them again.
        WalFsync::Never => {
            if let Some(last) = resurfaced.last() {
                removed.purge(log_id(*last)).await.expect("purge again");
            }
        }
    }
    // A purge made again here is new: its frames free their segments once
    // they are old.
    let now = Instant::now();
    writer.maintain(now, MAX_AGE).await.expect("a pass");
    journal
        .maintain_until_clean(&writer, later(now, MAX_AGE))
        .await;
    assert_eq!(log_ids(&quiet).await, [1, 2, 3], "{context}");
    assert_eq!(log_ids(&removed).await, Vec::<u64>::new(), "{context}");
}

/// A group whose entries were too many to copy purges them after the pass
/// that copied the others' out of their segment, and its purge frees the
/// segment. Under `never` nothing else syncs that purge before the segment
/// is deleted: the deletion must not outlive it, or a power loss would keep
/// the deletion, lose the purge and leave the group's log with a hole.
#[cfg(madsim)]
#[test]
fn a_deletion_never_outlives_the_purge_that_freed_its_segment() {
    crate::tests::check_madsim_determinism(673, madsim::Config::default(), || async {
        crate::sim_runtime::MadsimOpenRaftRuntime::scope(673, async {
            for fsync in [WalFsync::Never, WalFsync::Always] {
                purge_after_the_copies(fsync).await;
            }
        })
        .await;
    });
}

/// The group that holds more than a rewrite copies.
#[cfg(madsim)]
const LAGGARD: u32 = 3;

#[cfg(madsim)]
async fn purge_after_the_copies(fsync: WalFsync) {
    use super::SimDisk;

    let journal = Journal::new(&format!("purge-after-the-copies-{fsync:?}"), fsync);
    let root = journal.dir.parent().expect("a WAL root").to_path_buf();
    let writer = journal.writer();
    let start = Instant::now();
    let mut laggard = journal.store(&writer, LAGGARD);
    // More than a quarter of a segment in the first one.
    append(
        &mut laggard,
        (1..=6).map(|index| entry(index, &[b'l'; 300])),
    )
    .await;
    let (quiet, removed) = write_and_remove(&journal, &writer, start).await;
    let first = journal.segments().first().cloned().expect("a segment");
    writer
        .maintain(later(start, MAX_AGE), MAX_AGE)
        .await
        .expect("the pass that copies");
    assert!(journal.metrics.snapshot().wal_rewritten_bytes > 0);
    assert_eq!(
        journal.lagging.groups().into_iter().collect::<Vec<_>>(),
        [RaftGroupId(LAGGARD)],
        "{fsync:?}: the laggard keeps the first segment"
    );
    assert!(Disk::exists(&first));
    // The purge's own pass deletes the segment it freed.
    laggard.purge(log_id(6)).await.expect("purge");
    writer
        .maintain(later(start, MAX_AGE), MAX_AGE)
        .await
        .expect("a pass");
    assert!(
        !Disk::exists(&first),
        "{fsync:?}: the purge freed the segment"
    );
    writer.abort_simulated();
    drop((quiet, removed, laggard));
    drop(writer);
    SimDisk::power_loss_losing_unsynced(&root).expect("cut the power");

    let writer = journal.writer();
    let mut laggard = journal.store(&writer, LAGGARD);
    let purged = laggard
        .get_log_state()
        .await
        .expect("log state")
        .last_purged_log_id
        .map_or(0, |log_id| log_id.index);
    let held = log_ids(&laggard).await;
    assert!(
        purged >= 6 || (1..=6).all(|index| held.contains(&index)),
        "{fsync:?}: the deletion outlived the purge: purged {purged}, entries {held:?}"
    );
}

/// How [`a_snapshot_record_is_published_after_the_journal_syncs`] makes
/// its snapshot.
#[cfg(madsim)]
#[derive(Debug, Clone, Copy)]
enum Publication {
    /// A snapshot a leader sent is installed.
    Install,
    /// The replica builds one from its applied state.
    Build,
}

/// A snapshot record is published only after the core journal synced (R1).
/// Under `fsync = never` a group's entries are durable, its log is truncated
/// after entry 2 without a sync, and a snapshot through entry 2 is
/// installed, or built, with its record. A power loss that drops every
/// unsynced page keeps the record, so it must keep the truncate too: the
/// log reopens ending at 2, not with the truncated tail behind the
/// snapshot.
#[cfg(madsim)]
#[test]
fn a_snapshot_record_is_published_after_the_journal_syncs() {
    crate::tests::check_madsim_determinism(723, madsim::Config::default(), || async {
        crate::sim_runtime::MadsimOpenRaftRuntime::scope(723, async {
            for publication in [Publication::Install, Publication::Build] {
                snapshot_record_after_a_journal_sync(publication).await;
            }
        })
        .await;
    });
}

#[cfg(madsim)]
async fn snapshot_record_after_a_journal_sync(publication: Publication) {
    use openraft::storage::RaftSnapshotBuilder;
    use openraft::storage::RaftStateMachine;

    use super::SimDisk;

    let journal = Journal::new(
        &format!("snapshot-record-after-sync-{publication:?}"),
        WalFsync::Never,
    );
    let root = journal.dir.parent().expect("a WAL root").to_path_buf();
    let writer = journal.writer();
    let mut store = journal.store(&writer, QUIET);
    let record = store.snapshot_record();
    append(&mut store, (1..=5).map(|index| entry(index, b"tail"))).await;
    record.sync().await.expect("make the entries durable");
    store
        .truncate_after(Some(log_id(2)))
        .await
        .expect("truncate the tail");

    let machine = || {
        crate::state_machine::RaftGroupStateMachine::new_with_stores_and_snapshot_install(
            placement(QUIET),
            None,
            None,
            ursula_runtime::default_snapshot_store(),
            crate::state_machine::SnapshotBuildCoordinator::default(),
            crate::state_machine::SnapshotInstallCoordinator::default(),
            crate::state_machine::RecordSite::Store(record.clone()),
        )
    };
    let mut published = machine();
    match publication {
        Publication::Install => {
            let snapshot = ursula_runtime::GroupSnapshot {
                placement: placement(QUIET),
                group_commit_index: 2,
                stream_snapshot: Default::default(),
                stream_append_counts: Vec::new(),
            };
            let bytes = crate::snapshot_codec::group_snapshot_frames(Arc::new(snapshot))
                .collect::<Result<Vec<_>, _>>()
                .expect("snapshot frames")
                .into_iter()
                .flatten()
                .collect();
            let pointer = ursula_runtime::SnapshotPointer {
                snapshot_id: "after-sync".to_owned(),
                location: ursula_runtime::SnapshotLocation::Inline { bytes },
            };
            let meta = openraft::alias::SnapshotMetaOf::<UrsulaRaftTypeConfig> {
                last_log_id: Some(log_id(2)),
                last_membership: Default::default(),
            };
            published
                .install_snapshot(
                    &meta,
                    crate::types::SnapshotData::new(
                        pointer.encode_binary().expect("encode the pointer"),
                    ),
                )
                .await
                .expect("install the snapshot");
        }
        Publication::Build => {
            published
                .apply(futures_util::stream::iter((1..=2).map(|index| {
                    Ok((Entry::new(log_id(index), EntryPayload::Blank), None))
                })))
                .await
                .expect("apply the committed entries");
            let mut builder = published.get_snapshot_builder().await;
            builder.build_snapshot().await.expect("build the snapshot");
        }
    }
    drop(published);
    drop(store);
    writer.abort_simulated();
    drop(writer);
    SimDisk::power_loss_losing_unsynced(&root).expect("cut the power");

    let writer = journal.writer();
    let store = journal.store(&writer, QUIET);
    assert_eq!(
        log_ids(&store).await,
        [1, 2],
        "{publication:?}: the truncate is as durable as the snapshot record"
    );
    let mut restored = machine();
    restored.snapshot_record =
        crate::state_machine::RecordSite::Detached(record.path().to_path_buf());
    restored
        .restore_persisted_snapshot()
        .await
        .expect("restore the record");
    assert_eq!(
        restored.last_applied_log_id,
        Some(log_id(2)),
        "{publication:?}"
    );
}

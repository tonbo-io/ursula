use std::fs;
use std::process::Command;

use openraft::EntryPayload;
use openraft::LogId;
use openraft::Vote;
use openraft::alias::EntryOf;
use openraft::alias::LogIdOf;
use openraft::entry::RaftEntry;
use openraft::storage::IOFlushed;
use openraft::storage::RaftLogReader;
use openraft::storage::RaftLogStorage;
use openraft::vote::RaftLeaderId;
use openraft::vote::leader_id_adv::CommittedLeaderId;
use ursula_runtime::RuntimeMetrics;
use ursula_shard::CoreId;
use ursula_shard::RaftGroupId;
use ursula_shard::ShardId;
use ursula_shard::ShardPlacement;

use super::super::CoreFileLogWriter;
use super::super::CoreJournalRecord;
use super::super::RaftGroupLogRecord;
use super::super::load_log_store_inners_from_core_journal;
use super::super::read_wire_frames;
use crate::engine::DurableRaftLogStoreFactory;
use crate::types::UrsulaRaftTypeConfig;

fn placement(id: u32) -> ShardPlacement {
    ShardPlacement {
        core_id: CoreId(0),
        shard_id: ShardId(id),
        raft_group_id: RaftGroupId(id),
    }
}
fn log(index: u64) -> LogIdOf<UrsulaRaftTypeConfig> {
    LogId {
        leader_id: CommittedLeaderId::new(1, 1),
        index,
    }
}
fn entry(index: u64) -> EntryOf<UrsulaRaftTypeConfig> {
    EntryOf::<UrsulaRaftTypeConfig>::new(log(index), EntryPayload::Blank)
}

#[tokio::test]
async fn reclaim_one_group_preserves_live_neighbors_rejects_old_handles_and_reopens_empty() {
    let root = tempfile::tempdir().unwrap();
    let factory = DurableRaftLogStoreFactory::new(root.path());
    let metrics = RuntimeMetrics::new(1, 3).group_engine_metrics();
    let mut removed = factory.open(placement(0), metrics.clone()).unwrap();
    let mut retained = factory.open(placement(1), metrics.clone()).unwrap();
    assert!(
        factory.open(placement(0), metrics.clone()).is_err(),
        "one live owner per group"
    );
    removed.save_vote(&Vote::new_committed(7, 1)).await.unwrap();
    removed
        .append(vec![entry(0), entry(1)], IOFlushed::noop())
        .await
        .unwrap();
    retained
        .save_vote(&Vote::new_committed(8, 2))
        .await
        .unwrap();
    retained
        .append(vec![entry(0), entry(1), entry(2)], IOFlushed::noop())
        .await
        .unwrap();
    retained.save_committed(Some(log(2))).await.unwrap();
    retained.purge(log(0)).await.unwrap();
    let sizes = factory
        .reclaim_stopped_group_wal(placement(0), metrics.clone())
        .await
        .unwrap();
    assert!(sizes.1 < sizes.0);
    assert!(removed.save_vote(&Vote::new_committed(9, 1)).await.is_err());
    assert!(
        removed
            .append(vec![entry(2)], IOFlushed::noop())
            .await
            .is_err()
    );
    assert!(removed.read_vote().await.is_err());
    let path = factory.core_journal_path(CoreId(0));
    let records: Vec<CoreJournalRecord> = read_wire_frames(&fs::read(&path).unwrap()).unwrap();
    assert!(records.iter().all(|record| record.group_id == 1));
    // The writer must reopen the new inode after reclamation.
    retained
        .append(vec![entry(3)], IOFlushed::noop())
        .await
        .unwrap();
    let reopened = factory.open(placement(0), metrics.clone()).unwrap();
    assert!(reopened.lock_inner().unwrap().entries.is_empty());
    assert!(reopened.lock_inner().unwrap().vote.is_none());
    assert!(
        removed
            .save_vote(&Vote::new_committed(10, 1))
            .await
            .is_err(),
        "new lease never revalidates old handles"
    );
    let writer = factory.core_writer(placement(0), metrics.clone()).unwrap();
    let (reply, received) = std::sync::mpsc::channel();
    writer
        .tx
        .as_ref()
        .unwrap()
        .send(super::CoreFileLogCommand::Append(
            super::super::CoreFileLogWrite {
                group_id: 0,
                lease: removed.core_lease.as_ref().unwrap().clone(),
                record: RaftGroupLogRecord::Append(vec![entry(2)]),
                response_tx: reply,
            },
        ))
        .unwrap();
    assert!(
        received.recv().unwrap().is_err(),
        "writer rejects a delayed old-owner request even after a new owner opens"
    );
    drop(writer);
    drop(reopened);
    drop(removed);
    drop(retained);
    let recovered = load_log_store_inners_from_core_journal(&path).unwrap();
    assert!(!recovered.contains_key(&0));
    assert_eq!(recovered[&1].vote, Some(Vote::new_committed(8, 2)));
    assert_eq!(recovered[&1].committed, Some(log(2)));
    assert_eq!(recovered[&1].last_purged_log_id, Some(log(0)));
    assert_eq!(recovered[&1].entries.keys().copied().collect::<Vec<_>>(), [
        1, 2, 3
    ]);
    // Recovery of a group never opened in this process must also be retired.
    let writer = CoreFileLogWriter::shared(path.clone()).unwrap();
    writer.reclaim_stopped_group(1).unwrap();
    assert!(writer.open_group(1).unwrap().0.entries.is_empty());
    drop(writer);
    assert!(
        load_log_store_inners_from_core_journal(&path)
            .unwrap()
            .is_empty()
    );
    assert!(
        read_wire_frames::<CoreJournalRecord>(&fs::read(path).unwrap())
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn in_process_store_reopen_recovers_latest_writes_and_io_failure_poison_is_closed() {
    let root = tempfile::tempdir().unwrap();
    let factory = DurableRaftLogStoreFactory::new(root.path());
    let metrics = RuntimeMetrics::new(1, 3).group_engine_metrics();
    let mut retained = factory.open(placement(1), metrics.clone()).unwrap();
    let mut group = factory.open(placement(0), metrics.clone()).unwrap();
    group.save_vote(&Vote::new_committed(7, 1)).await.unwrap();
    group
        .append(vec![entry(0), entry(1)], IOFlushed::noop())
        .await
        .unwrap();
    drop(group);
    let group = factory.open(placement(0), metrics.clone()).unwrap();
    assert_eq!(
        group
            .lock_inner()
            .unwrap()
            .entries
            .keys()
            .copied()
            .collect::<Vec<_>>(),
        [0, 1]
    );
    let path = factory.core_journal_path(CoreId(0));
    let before = fs::read(&path).unwrap();
    // A directory at the temp path forces replacement to fail before rename.
    fs::create_dir(path.with_extension("compact")).unwrap();
    assert!(
        factory
            .reclaim_stopped_group_wal(placement(0), metrics.clone())
            .await
            .is_err()
    );
    assert_eq!(fs::read(&path).unwrap(), before);
    assert!(group.lock_inner().is_err());
    assert!(
        retained
            .save_vote(&Vote::new_committed(8, 2))
            .await
            .is_err(),
        "failed writer cannot admit later writes"
    );
    assert!(factory.open(placement(0), metrics.clone()).is_err());
    drop(group);
    drop(retained);
    fs::remove_dir(path.with_extension("compact")).unwrap();
    let mut recovered = factory.open(placement(0), metrics.clone()).unwrap();
    assert_eq!(
        recovered.read_vote().await.unwrap(),
        Some(Vote::new_committed(7, 1))
    );
    assert_eq!(recovered.try_get_log_entries(0..=1).await.unwrap().len(), 2);
    drop(recovered);
    factory
        .reclaim_stopped_group_wal(placement(0), metrics.clone())
        .await
        .unwrap();
    assert!(
        factory
            .open(placement(0), metrics)
            .unwrap()
            .lock_inner()
            .unwrap()
            .entries
            .is_empty()
    );
}

#[test]
#[ignore = "child process entry point invoked by the process-recovery test"]
fn core_reclamation_exit_child() {
    let path =
        std::path::PathBuf::from(std::env::var_os("URSULA_CORE_RECLAIM_CHILD_PATH").unwrap());
    let writer = CoreFileLogWriter::shared(path).unwrap();
    assert_eq!(writer.reclaim_stopped_group(2).unwrap(), (0, 0));
    let (_, first) = writer.open_group(0).unwrap();
    let (_, second) = writer.open_group(1).unwrap();
    writer
        .append(
            0,
            first.clone(),
            RaftGroupLogRecord::Append(vec![entry(0), entry(1)]),
        )
        .unwrap();
    writer
        .append(
            1,
            second.clone(),
            RaftGroupLogRecord::SaveVote(Vote::new_committed(8, 2)),
        )
        .unwrap();
    writer
        .append(
            1,
            second.clone(),
            RaftGroupLogRecord::Append(vec![entry(0)]),
        )
        .unwrap();
    writer.reclaim_stopped_group(0).unwrap();
    writer
        .append(1, second, RaftGroupLogRecord::Append(vec![entry(1)]))
        .unwrap();
    assert!(
        writer
            .append(
                0,
                first,
                RaftGroupLogRecord::SaveVote(Vote::new_committed(9, 1))
            )
            .is_err()
    );
    std::process::exit(0);
}

#[test]
fn reclaimed_core_survives_process_exit_with_later_neighbor_writes() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("journal.bin");
    let output = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "log_store::file::lifecycle::tests::core_reclamation_exit_child",
            "--ignored",
            "--nocapture",
        ])
        .env("URSULA_CORE_RECLAIM_CHILD_PATH", &path)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let recovered = load_log_store_inners_from_core_journal(&path).unwrap();
    assert_eq!(recovered.keys().copied().collect::<Vec<_>>(), [1]);
    assert_eq!(recovered[&1].vote, Some(Vote::new_committed(8, 2)));
    assert_eq!(recovered[&1].entries.keys().copied().collect::<Vec<_>>(), [
        0, 1
    ]);
    let writer = CoreFileLogWriter::shared(path).unwrap();
    let (empty, _) = writer.open_group(0).unwrap();
    assert!(empty.entries.is_empty());
    assert!(empty.vote.is_none());
    assert_eq!(
        writer
            .open_group(1)
            .unwrap()
            .0
            .entries
            .keys()
            .copied()
            .collect::<Vec<_>>(),
        [0, 1]
    );
}

//! Failed poison-marker publication across process and host crashes.

use openraft::storage::IOFlushed;
use openraft::storage::RaftLogReader;
use openraft::storage::RaftLogStorage;
use openraft::storage::RaftLogStorageExt;
use openraft::type_config::TypeConfigExt;
use ursula_raft::UrsulaRaftTypeConfig;
use ursula_raft::wal::diagnostics::CoreJournalError;
use ursula_raft::wal::diagnostics::GroupLogState;
use ursula_raft::wal::diagnostics::PreviousRun;
use ursula_raft::wal::diagnostics::RUN_STATE_FILE;
use ursula_raft::wal::diagnostics::SIM_DISK_PAGE_SIZE;
use ursula_raft::wal::diagnostics::SimDisk;
use ursula_raft::wal::diagnostics::SimDiskFault;

use super::JOURNAL_POWER_LOSS_SEEDS;
use super::SimNodeWal;
use super::active_segments;
use super::blank_entry;
use super::core_journal_error;
use super::group_placement;
use super::run_with_madsim;
use super::seeds_from_env;
use super::sim_log_id;
use super::sim_test_guard;
use super::standalone_wal_metrics;

#[derive(Clone, Copy, Debug)]
enum MarkerFailure {
    Write,
    FileSync,
    Rename,
    DirectorySync,
}

#[test]
fn failed_poison_publication_never_loses_an_acknowledged_prefix() {
    let _guard = sim_test_guard();
    for seed in seeds_from_env("POISON_MARKER_SEEDS", &JOURNAL_POWER_LOSS_SEEDS) {
        for journal_fault in [
            SimDiskFault::Write,
            SimDiskFault::PartialWrite,
            SimDiskFault::Sync,
        ] {
            for marker_fault in [
                MarkerFailure::Write,
                MarkerFailure::FileSync,
                MarkerFailure::Rename,
                MarkerFailure::DirectorySync,
            ] {
                for host_crash in [false, true] {
                    run_with_madsim(seed, async move {
                        let context = format!(
                            "seed {seed} journal {journal_fault:?} marker {marker_fault:?} host {host_crash}"
                        );
                        let wal = SimNodeWal::provision("poison-publication");
                        let placement = group_placement(0);
                        let mut store =
                            wal.open(placement, standalone_wal_metrics(placement)).await;
                        let vote = openraft::Vote::new_committed(7, 1);
                        store.save_vote(&vote).await.expect("durable vote");
                        store
                            .blocking_append([blank_entry(1), blank_entry(2)])
                            .await
                            .expect("acknowledged prefix");
                        let segment = active_segments(wal.root())[0].clone();
                        let before_failure = SimDisk::read(&segment).unwrap().len();
                        SimDisk::inject_fault(&segment, journal_fault).unwrap();
                        let temporary = wal.root().join("run-state.poisoned-core-0.tmp");
                        let (path, fault) = match marker_fault {
                            MarkerFailure::Write => (temporary, SimDiskFault::Write),
                            MarkerFailure::FileSync => (temporary, SimDiskFault::Sync),
                            MarkerFailure::Rename => {
                                (wal.root().join(RUN_STATE_FILE), SimDiskFault::Rename)
                            }
                            MarkerFailure::DirectorySync => {
                                (wal.root().to_owned(), SimDiskFault::Sync)
                            }
                        };
                        SimDisk::inject_fault(&path, fault).unwrap();
                        let (completed, completion) = UrsulaRaftTypeConfig::oneshot();
                        store
                            .append((3..4096).map(blank_entry), IOFlushed::signal(completed))
                            .await
                            .expect("submit failing batch");
                        let error = completion
                            .await
                            .expect("flush completion")
                            .expect_err("journal failure must refuse ACK");
                        assert!(
                            matches!(
                                core_journal_error(&error),
                                Some(CoreJournalError::WriterPoisoned { .. })
                            ),
                            "{context}: {error}"
                        );
                        let after_failure = SimDisk::read(&segment).unwrap().len();
                        if journal_fault == SimDiskFault::Write {
                            assert_eq!(after_failure, before_failure, "{context}");
                        } else {
                            assert!(
                                after_failure > before_failure + 2 * SIM_DISK_PAGE_SIZE,
                                "{context}: the failed batch must span multiple disk pages"
                            );
                        }
                        drop(store);
                        if host_crash {
                            wal.power_loss().await;
                        } else {
                            wal.process_crash().await;
                        }
                        let mut store = wal
                            .try_open(placement, standalone_wal_metrics(placement))
                            .await
                            .unwrap_or_else(|error| {
                                panic!("{context}: valid acknowledged prefix must reopen: {error}")
                            });
                        assert_eq!(store.read_vote().await.unwrap(), Some(vote), "{context}");
                        let entries = store.try_get_log_entries(1..3).await.unwrap();
                        assert_eq!(
                            entries.iter().map(|entry| entry.log_id).collect::<Vec<_>>(),
                            vec![sim_log_id(1), sim_log_id(2)],
                            "{context}"
                        );
                        if wal.opening().previous_run == PreviousRun::Poisoned {
                            assert_eq!(store.log_state(), GroupLogState::Recovering, "{context}");
                        }
                        if !host_crash {
                            let expected = if matches!(marker_fault, MarkerFailure::DirectorySync) {
                                PreviousRun::Poisoned
                            } else {
                                PreviousRun::ProcessCrash
                            };
                            assert_eq!(
                                wal.opening().previous_run,
                                expected,
                                "{context}: marker fault must reach its intended publication stage"
                            );
                        }
                    });
                }
            }
        }
    }
}

/// A failed fsync can leave clean-but-lost pages. If the marker also fails,
/// a same-boot restart sees a valid cached tail. A later append cannot sync
/// those old clean pages. After power loss this must refuse corrupt history,
/// including a retry after that startup refusal, rather than truncate an ACK.
#[test]
fn failed_poison_marker_then_reack_and_power_loss_refuses_corrupt_history() {
    let _guard = sim_test_guard();
    run_with_madsim(7, async {
        let wal = SimNodeWal::provision("failed-fsync-reack");
        let placement = group_placement(0);
        let mut store = wal.open(placement, standalone_wal_metrics(placement)).await;
        store
            .save_vote(&openraft::Vote::new_committed(7, 1))
            .await
            .unwrap();
        let journal = active_segments(wal.root())[0].clone();
        store
            .blocking_append([blank_entry(1), blank_entry(2)])
            .await
            .unwrap();
        SimDisk::inject_fault(&active_segments(wal.root())[0], SimDiskFault::Sync).unwrap();
        SimDisk::inject_fault(
            &wal.root().join("run-state.poisoned-core-0.tmp"),
            SimDiskFault::Write,
        )
        .unwrap();
        let (completed, completion) = UrsulaRaftTypeConfig::oneshot();
        store
            .append((3..4096).map(blank_entry), IOFlushed::signal(completed))
            .await
            .unwrap();
        completion
            .await
            .unwrap()
            .expect_err("failed fsync refuses the original batch");
        drop(store);
        wal.process_crash().await;
        let mut store = wal.open(placement, standalone_wal_metrics(placement)).await;
        assert_eq!(wal.opening().previous_run, PreviousRun::ProcessCrash);
        assert_eq!(store.log_state(), GroupLogState::Initialized);
        store
            .blocking_append([blank_entry(4096)])
            .await
            .expect("later acknowledged append");
        drop(store);
        wal.power_loss_losing_unsynced().await;
        let damaged = SimDisk::read(&journal).unwrap();
        for _ in 0..2 {
            let error = wal
                .try_open(placement, standalone_wal_metrics(placement))
                .await
                .expect_err("corrupt acknowledged history must refuse startup");
            assert!(
                matches!(&error, ursula_raft::RaftWalError::OpenCore {
                core: ursula_shard::CoreId(0),
                source: ursula_raft::wal::diagnostics::CoreJournalError::Journal(source),
            } if matches!(source.as_ref(), ursula_raft::wal::diagnostics::JournalError::CorruptFrame { .. })),
                "startup must preserve the exact journal corruption refusal: {error:?}"
            );
            assert_eq!(
                SimDisk::read(&journal).unwrap(),
                damaged,
                "startup refusal must not truncate an acknowledged suffix"
            );
            wal.process_crash().await;
        }
    });
}

//! Snapshot pointer publication through the production state-machine entry points.

use std::path::Path;
use std::sync::Arc;

use openraft::LogId;
use openraft::storage::RaftStateMachine;
use openraft::vote::RaftLeaderId;
use ursula_runtime::GroupSnapshot;
use ursula_runtime::SnapshotLocation;
use ursula_runtime::SnapshotPointer;
use ursula_shard::CoreId;
use ursula_shard::RaftGroupId;
use ursula_shard::ShardId;
use ursula_shard::ShardPlacement;

use super::RaftGroupStateMachine;
use super::SnapshotBuildCoordinator;
use super::SnapshotInstallCoordinator;
use super::SnapshotMetaOf;
use super::SnapshotRestoreError;
use super::UrsulaRaftTypeConfig;
use super::default_snapshot_store;
use super::group_snapshot_frames;
use crate::log_store::JournalError;
use crate::log_store::JournalOp;
use crate::log_store::SimDisk;
use crate::log_store::SimDiskError;
use crate::log_store::SimDiskFault;
use crate::types::SnapshotData;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PublicationFailure {
    Write,
    FileSync,
    Rename,
    DirectorySync,
}

fn machine(path: &Path) -> RaftGroupStateMachine {
    RaftGroupStateMachine::new_with_stores_and_snapshot_install(
        ShardPlacement {
            core_id: CoreId(0),
            shard_id: ShardId(0),
            raft_group_id: RaftGroupId(7),
        },
        None,
        None,
        default_snapshot_store(),
        SnapshotBuildCoordinator::default(),
        SnapshotInstallCoordinator::default(),
        super::RecordSite::Detached(path.to_owned()),
    )
}

async fn install(machine: &mut RaftGroupStateMachine, index: u64) -> std::io::Result<()> {
    let snapshot = GroupSnapshot {
        placement: machine.placement,
        group_commit_index: index,
        stream_snapshot: Default::default(),
        stream_append_counts: Vec::new(),
    };
    let bytes = group_snapshot_frames(Arc::new(snapshot))
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
        .into_iter()
        .flatten()
        .collect();
    let pointer = SnapshotPointer {
        snapshot_id: format!("snapshot-{index}"),
        location: SnapshotLocation::Inline { bytes },
    };
    let meta = SnapshotMetaOf::<UrsulaRaftTypeConfig> {
        last_log_id: Some(LogId {
            leader_id: <UrsulaRaftTypeConfig as openraft::RaftTypeConfig>::LeaderId::new(1, 1),
            index,
        }),
        last_membership: Default::default(),
    };
    machine
        .install_snapshot(&meta, SnapshotData::new(pointer.encode_binary().unwrap()))
        .await
}

#[test]
fn snapshot_metadata_acknowledged_pointer_survives_power_loss() {
    for seed in [1, 7, 19, 42, 60, 64] {
        crate::sim_runtime::run_seeded(seed, None, move || async move {
            let root = SimDisk::provision_dir("snapshot-ack").unwrap();
            // New parent directories must be durable along with the pointer.
            let path = root.join("new/nested/group-7.snapshot.json");
            let mut state = machine(&path);
            install(&mut state, 1).await.unwrap();
            install(&mut state, 2).await.unwrap();
            drop(state);
            SimDisk::power_loss_losing_unsynced(&root).unwrap();
            let mut restored = machine(&path);
            restored.restore_persisted_snapshot().await.unwrap();
            assert_eq!(restored.last_applied_log_id.unwrap().index, 2);
            assert_eq!(
                restored.group_snapshot().await.unwrap().group_commit_index,
                2
            );
        });
    }
}

/// The injected disk fault behind a failed publication, and the step it failed.
fn injected_fault(error: &std::io::Error) -> Option<(JournalOp, &SimDiskError)> {
    let JournalError::Io { op, source, .. } = error.get_ref()?.downcast_ref::<JournalError>()?
    else {
        return None;
    };
    Some((*op, source.get_ref()?.downcast_ref::<SimDiskError>()?))
}

#[test]
fn snapshot_metadata_failed_publication_restores_a_complete_pointer() {
    for seed in [1, 7, 19, 42, 60, 64] {
        for phase in [
            PublicationFailure::Write,
            PublicationFailure::FileSync,
            PublicationFailure::Rename,
            PublicationFailure::DirectorySync,
        ] {
            for host_crash in [false, true] {
                crate::sim_runtime::run_seeded(seed, None, move || {
                    async move {
                        let root = SimDisk::provision_dir("snapshot-fault").unwrap();
                        let path = root.join("group-7.snapshot.json");
                        let mut state = machine(&path);
                        install(&mut state, 1).await.unwrap();
                        let temporary = path.with_extension("json.tmp");
                        let (fault_path, fault, op) = match phase {
                            PublicationFailure::Write => {
                                (temporary, SimDiskFault::Write, JournalOp::Append)
                            }
                            PublicationFailure::FileSync => {
                                (temporary, SimDiskFault::Sync, JournalOp::Sync)
                            }
                            // Rename faults match the destination.
                            PublicationFailure::Rename => {
                                (path.clone(), SimDiskFault::Rename, JournalOp::Rename)
                            }
                            PublicationFailure::DirectorySync => {
                                (root.clone(), SimDiskFault::Sync, JournalOp::SyncDir)
                            }
                        };
                        SimDisk::inject_fault(&fault_path, fault).unwrap();
                        let error = install(&mut state, 2).await.unwrap_err();
                        assert!(
                            matches!(
                                injected_fault(&error),
                                Some((actual_op, SimDiskError::Injected { path, fault: actual }))
                                    if actual_op == op && path == &fault_path && *actual == fault
                            ),
                            "expected {phase:?} injection: {error}"
                        );
                        drop(state);
                        if host_crash {
                            SimDisk::power_loss_losing_unsynced(&root).unwrap();
                        } else {
                            SimDisk::process_crash(&root).unwrap();
                        }
                        let mut restored = machine(&path);
                        restored.restore_persisted_snapshot().await.unwrap();
                        let expected = if !host_crash && phase == PublicationFailure::DirectorySync
                        {
                            2
                        } else {
                            1
                        };
                        assert_eq!(
                            restored.last_applied_log_id.unwrap().index,
                            expected,
                            "seed {seed} phase {phase:?} host_crash {host_crash}"
                        );
                        assert_eq!(
                            restored.group_snapshot().await.unwrap().group_commit_index,
                            expected
                        );
                        // The next publication replaces whatever temporary file the
                        // failure left, and is durable once acknowledged.
                        install(&mut restored, 3).await.unwrap();
                        drop(restored);
                        SimDisk::power_loss_losing_unsynced(&root).unwrap();
                        let mut republished = machine(&path);
                        republished.restore_persisted_snapshot().await.unwrap();
                        assert_eq!(
                            republished.last_applied_log_id.unwrap().index,
                            3,
                            "seed {seed} phase {phase:?} host_crash {host_crash}"
                        );
                    }
                });
            }
        }
    }
}

/// The object key a pointer names.
fn object(pointer: &SnapshotPointer) -> String {
    let SnapshotLocation::S3 { key, .. } = &pointer.location else {
        panic!("an external pointer: {pointer:?}");
    };
    key.clone()
}

fn external_machine(
    path: &Path,
    store: ursula_runtime::SharedSnapshotStore,
    coordinator: SnapshotInstallCoordinator,
) -> RaftGroupStateMachine {
    RaftGroupStateMachine::new_with_stores_and_snapshot_install(
        ShardPlacement {
            core_id: CoreId(0),
            shard_id: ShardId(0),
            raft_group_id: RaftGroupId(7),
        },
        None,
        None,
        store,
        SnapshotBuildCoordinator::default(),
        coordinator,
        super::RecordSite::Detached(path.to_owned()),
    )
}

fn visible_pointer(path: &Path) -> SnapshotPointer {
    let bytes = SimDisk::read(path).unwrap();
    let persisted: super::PersistedSnapshot = super::decode_snapshot_envelope(&bytes).unwrap();
    SnapshotPointer::decode(&persisted.pointer_bytes).unwrap()
}

/// A metadata publication that failed after its rename may have made its
/// candidate durable, so the candidate stays held with the durable pointer
/// until a restart reads which of them the disk kept. A GC pass meanwhile
/// deletes only the objects of the builds the latch refused (#687). The
/// restart's restore holds only the pointer it found, and its pass deletes
/// the other one; a restart whose directory sync fails holds both.
#[test]
fn a_failed_snapshot_pointer_stays_held_until_a_restart_resolves_it() {
    use openraft::storage::RaftSnapshotBuilder;
    use ursula_runtime::snapshot_harness::MemorySnapshotBucket;
    use ursula_runtime::snapshot_harness::SnapshotStoreFaults;

    for seed in [1, 7, 42] {
        for restore_sync_fails in [false, true] {
            crate::sim_runtime::run_seeded(seed, None, move || async move {
                let root = SimDisk::provision_dir("snapshot-holds").unwrap();
                let path = root.join("group-7.snapshot.json");
                let bucket = MemorySnapshotBucket::new(
                    seed,
                    ursula_runtime::SnapshotPrefix::try_from("snapshots").unwrap(),
                    SnapshotStoreFaults::none(),
                );
                let store: ursula_runtime::SharedSnapshotStore = Arc::new(bucket.store(1));
                let mut state =
                    external_machine(&path, store.clone(), SnapshotInstallCoordinator::default());
                state.restore_persisted_snapshot().await.unwrap();
                let holds = state.holds.clone();
                install(&mut state, 1).await.unwrap();
                state
                    .get_snapshot_builder()
                    .await
                    .build_snapshot()
                    .await
                    .unwrap();
                let durable = visible_pointer(&path);
                SimDisk::inject_fault(&root, SimDiskFault::Sync).unwrap();
                state
                    .get_snapshot_builder()
                    .await
                    .build_snapshot()
                    .await
                    .unwrap_err();
                let candidate = visible_pointer(&path);
                assert_ne!(object(&candidate), object(&durable));
                let current = state.get_current_snapshot().await.unwrap().unwrap();
                assert_eq!(
                    current.snapshot.pointer_bytes(),
                    durable.encode_binary().unwrap().as_slice()
                );
                let expected =
                    std::collections::BTreeSet::from([object(&durable), object(&candidate)]);
                drop(current);
                // A later build still writes an object, but the terminal
                // metadata latch prevents it from overwriting the visible
                // candidate, and the pass deletes what it wrote.
                for _ in 0..3 {
                    let error = state
                        .get_snapshot_builder()
                        .await
                        .build_snapshot()
                        .await
                        .unwrap_err();
                    assert!(matches!(
                        error
                            .get_ref()
                            .unwrap()
                            .downcast_ref::<super::SnapshotMetadataStopped>(),
                        Some(super::SnapshotMetadataStopped::AfterFailure)
                    ));
                    assert_eq!(
                        visible_pointer(&path).encode_binary().unwrap(),
                        candidate.encode_binary().unwrap()
                    );
                    holds
                        .collect_garbage(&store, crate::rt::time::Instant::now())
                        .await
                        .unwrap();
                    assert_eq!(holds.held_keys(), expected);
                    assert_eq!(bucket.keys(), expected);
                }
                drop(state);
                SimDisk::process_crash(&root).unwrap();
                let mut restored =
                    external_machine(&path, store.clone(), SnapshotInstallCoordinator::default());
                if restore_sync_fails {
                    SimDisk::inject_fault(&root, SimDiskFault::Sync).unwrap();
                    let error = restored.restore_persisted_snapshot().await.unwrap_err();
                    let SnapshotRestoreError::Metadata { source, .. } = &error else {
                        panic!("a failed fsync is a metadata failure: {error:?}");
                    };
                    assert!(
                        matches!(source.get_ref().and_then(|error| error.downcast_ref::<SimDiskError>()),
                        Some(SimDiskError::Injected { path, fault: SimDiskFault::Sync }) if path == &root)
                    );
                    // A restore that did not end runs no pass.
                    assert_eq!(
                        restored
                            .holds
                            .collect_garbage(&store, crate::rt::time::Instant::now())
                            .await
                            .unwrap(),
                        crate::snapshot_holds::SnapshotPass::NotRestored
                    );
                    assert_eq!(
                        bucket.keys(),
                        expected,
                        "a failed restart deletes neither object"
                    );
                } else {
                    restored.restore_persisted_snapshot().await.unwrap();
                    restored
                        .holds
                        .collect_garbage(&store, crate::rt::time::Instant::now())
                        .await
                        .unwrap();
                    assert_eq!(
                        bucket.keys(),
                        std::collections::BTreeSet::from([object(&candidate)]),
                        "the restart keeps only the pointer the disk kept"
                    );
                }
                drop(restored);
                SimDisk::power_loss_losing_unsynced(&root).unwrap();
                let mut after_power_loss =
                    external_machine(&path, store.clone(), SnapshotInstallCoordinator::default());
                after_power_loss.restore_persisted_snapshot().await.unwrap();
                let current = after_power_loss
                    .get_current_snapshot()
                    .await
                    .unwrap()
                    .unwrap();
                let expected_pointer = if restore_sync_fails {
                    durable
                } else {
                    candidate
                };
                assert_eq!(
                    current.snapshot.pointer_bytes(),
                    expected_pointer.encode_binary().unwrap().as_slice()
                );
                // I1: the restored pointer's object exists.
                assert!(bucket.keys().contains(&object(&expected_pointer)));
            });
        }
    }
}

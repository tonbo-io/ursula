//! Snapshot pointer publication through the production state-machine entry points.

use std::io::Cursor;
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
use super::UrsulaRaftTypeConfig;
use super::default_snapshot_store;
use super::group_snapshot_frames;
use crate::log_store::JournalError;
use crate::log_store::JournalOp;
use crate::log_store::SimDisk;
use crate::log_store::SimDiskError;
use crate::log_store::SimDiskFault;

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
        Some(path.to_owned()),
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
        snapshot_id: pointer.snapshot_id.clone(),
    };
    machine
        .install_snapshot(&meta, Cursor::new(pointer.encode_binary().unwrap()))
        .await
}

#[test]
fn snapshot_metadata_acknowledged_pointer_survives_power_loss() {
    for seed in [1, 7, 19, 42, 60, 64] {
        madsim::runtime::Runtime::with_seed_and_config(seed, Default::default()).block_on(async {
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
                madsim::runtime::Runtime::with_seed_and_config(seed, Default::default()).block_on(
                    async {
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
                    },
                );
            }
        }
    }
}

#[derive(Debug, Default)]
struct PinnedObjects {
    state: std::sync::Mutex<PinnedObjectState>,
}

#[derive(Debug, Default)]
struct PinnedObjectState {
    objects: std::collections::BTreeMap<String, Vec<u8>>,
    pins: std::collections::BTreeSet<String>,
    current: Option<String>,
    uploads: u64,
}

impl PinnedObjects {
    fn pins(&self) -> std::collections::BTreeSet<String> {
        self.state.lock().unwrap().pins.clone()
    }
    fn collect_unreferenced(&self) {
        let mut state = self.state.lock().unwrap();
        let mut retained = state.pins.clone();
        retained.extend(state.current.clone());
        state.objects.retain(|key, _| retained.contains(key));
    }
}

impl ursula_runtime::SnapshotStore for PinnedObjects {
    fn upload<'a>(
        &'a self,
        key: ursula_runtime::SnapshotKey,
        bytes: bytes::Bytes,
    ) -> ursula_runtime::SnapshotStoreFuture<'a, SnapshotLocation> {
        Box::pin(async move {
            let mut state = self.state.lock().unwrap();
            state.uploads = state.uploads.checked_add(1).unwrap();
            let key = format!("{}-{}", key.snapshot_id, state.uploads);
            let size_bytes = u64::try_from(bytes.len()).unwrap();
            state.objects.insert(key.clone(), bytes.to_vec());
            Ok(SnapshotLocation::S3 {
                key,
                size_bytes,
                stored_size_bytes: size_bytes,
                compression: Default::default(),
                shared_object: true,
            })
        })
    }
    fn download<'a>(
        &'a self,
        location: &'a SnapshotLocation,
    ) -> ursula_runtime::SnapshotStoreFuture<'a, Vec<u8>> {
        Box::pin(async move {
            let SnapshotLocation::S3 { key, .. } = location else {
                panic!("external location");
            };
            self.state
                .lock()
                .unwrap()
                .objects
                .get(key)
                .cloned()
                .ok_or_else(|| {
                    ursula_runtime::SnapshotStoreError::Backend(format!(
                        "object {key} was collected"
                    ))
                })
        })
    }
    fn delete<'a>(
        &'a self,
        location: &'a SnapshotLocation,
    ) -> ursula_runtime::SnapshotStoreFuture<'a, ()> {
        Box::pin(async move {
            if let SnapshotLocation::S3 { key, .. } = location {
                self.state.lock().unwrap().objects.remove(key);
            }
            Ok(())
        })
    }
    fn pin_reference<'a>(
        &'a self,
        _group: u32,
        location: &'a SnapshotLocation,
    ) -> ursula_runtime::SnapshotStoreFuture<'a, ()> {
        Box::pin(async move {
            if let SnapshotLocation::S3 { key, .. } = location {
                self.state.lock().unwrap().pins.insert(key.clone());
            }
            Ok(())
        })
    }
    fn publish_reference<'a>(
        &'a self,
        _group: u32,
        location: &'a SnapshotLocation,
    ) -> ursula_runtime::SnapshotStoreFuture<'a, ()> {
        Box::pin(async move {
            self.state.lock().unwrap().current = match location {
                SnapshotLocation::S3 { key, .. } => Some(key.clone()),
                _ => None,
            };
            Ok(())
        })
    }
    fn reconcile_reference_pins<'a>(
        &'a self,
        _group: u32,
        retained: &'a [SnapshotLocation],
    ) -> ursula_runtime::SnapshotStoreFuture<'a, ()> {
        Box::pin(async move {
            self.state.lock().unwrap().pins.retain(|key| retained.iter().any(|location|
                matches!(location, SnapshotLocation::S3 { key: retained_key, .. } if key == retained_key)));
            Ok(())
        })
    }
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
        Some(path.to_owned()),
    )
}

fn visible_pointer(path: &Path) -> SnapshotPointer {
    let bytes = SimDisk::read(path).unwrap();
    let persisted: super::PersistedSnapshot = super::decode_snapshot_envelope(&bytes).unwrap();
    SnapshotPointer::decode(&persisted.pointer_bytes).unwrap()
}

#[test]
fn failed_snapshot_pointer_keeps_pins_until_restart_resolves_rename() {
    use openraft::storage::RaftSnapshotBuilder;

    for seed in [1, 7, 42] {
        for restore_sync_fails in [false, true] {
            madsim::runtime::Runtime::with_seed_and_config(seed, Default::default()).block_on(async {
                let root = SimDisk::provision_dir("snapshot-pins").unwrap();
                let path = root.join("group-7.snapshot.json");
                let objects = Arc::new(PinnedObjects::default());
                let store: ursula_runtime::SharedSnapshotStore = objects.clone();
                let coordinator = SnapshotInstallCoordinator::default();
                let references = coordinator.references(7);
                let mut state = external_machine(&path, store.clone(), coordinator);
                install(&mut state, 1).await.unwrap();
                state.get_snapshot_builder().await.build_snapshot().await.unwrap();
                let durable = visible_pointer(&path);
                SimDisk::inject_fault(&root, SimDiskFault::Sync).unwrap();
                state.get_snapshot_builder().await.build_snapshot().await.unwrap_err();
                let candidate = visible_pointer(&path);
                assert_ne!(candidate.encode_binary().unwrap(), durable.encode_binary().unwrap());
                let current = state.get_current_snapshot().await.unwrap().unwrap();
                assert_eq!(*current.snapshot.get_ref(), durable.encode_binary().unwrap());
                let expected = [durable.location.clone(), candidate.location.clone()].into_iter().map(|location| {
                    let SnapshotLocation::S3 { key, .. } = location else { panic!("external pointer"); }; key
                }).collect::<std::collections::BTreeSet<_>>();
                // A later build still prepares an object, but the terminal
                // metadata latch must prevent it from overwriting the visible candidate.
                for _ in 0..3 {
                    let error = state.get_snapshot_builder().await.build_snapshot().await.unwrap_err();
                    assert!(error.get_ref().unwrap().is::<super::SnapshotMetadataStopped>());
                    assert_eq!(visible_pointer(&path).encode_binary().unwrap(), candidate.encode_binary().unwrap());
                    references.publish_current(&store, 7).await.unwrap();
                    objects.collect_unreferenced();
                    assert_eq!(objects.pins(), expected);
                }
                drop(state);
                references.publish_current(&store, 7).await.unwrap();
                objects.collect_unreferenced();
                assert_eq!(objects.pins(), expected, "engine drop cannot release an uncertain pointer");
                SimDisk::process_crash(&root).unwrap();
                let mut restored = external_machine(&path, store.clone(), SnapshotInstallCoordinator::default());
                if restore_sync_fails {
                    SimDisk::inject_fault(&root, SimDiskFault::Sync).unwrap();
                    let error = restored.restore_persisted_snapshot().await.unwrap_err();
                    assert!(matches!(error.get_ref().and_then(|error| error.downcast_ref::<SimDiskError>()),
                        Some(SimDiskError::Injected { path, fault: SimDiskFault::Sync }) if path == &root));
                    assert_eq!(objects.pins(), expected, "failed restart fsync must not reconcile either pin");
                } else {
                    restored.restore_persisted_snapshot().await.unwrap();
                    objects.collect_unreferenced();
                }
                drop(restored);
                SimDisk::power_loss_losing_unsynced(&root).unwrap();
                let mut after_power_loss = external_machine(&path, store.clone(), SnapshotInstallCoordinator::default());
                after_power_loss.restore_persisted_snapshot().await.unwrap();
                let current = after_power_loss.get_current_snapshot().await.unwrap().unwrap();
                let expected_pointer = if restore_sync_fails { durable } else { candidate };
                assert_eq!(*current.snapshot.get_ref(), expected_pointer.encode_binary().unwrap());
            });
        }
    }
}

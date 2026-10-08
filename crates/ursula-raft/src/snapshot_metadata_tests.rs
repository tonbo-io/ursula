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
use crate::log_store::SimDisk;
use crate::log_store::SimDiskError;
use crate::log_store::SimDiskFault;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PublicationFailure {
    Write,
    FileSync,
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

#[test]
fn snapshot_metadata_failed_publication_restores_a_complete_pointer() {
    for seed in [1, 7, 19, 42, 60, 64] {
        for phase in [
            PublicationFailure::Write,
            PublicationFailure::FileSync,
            PublicationFailure::DirectorySync,
        ] {
            for host_crash in [false, true] {
                madsim::runtime::Runtime::with_seed_and_config(seed, Default::default()).block_on(
                    async {
                        let root = SimDisk::provision_dir("snapshot-fault").unwrap();
                        let path = root.join("group-7.snapshot.json");
                        let mut state = machine(&path);
                        install(&mut state, 1).await.unwrap();
                        let (fault_path, fault) = match phase {
                            PublicationFailure::Write => {
                                (path.with_extension("json.tmp"), SimDiskFault::Write)
                            }
                            PublicationFailure::FileSync => {
                                (path.with_extension("json.tmp"), SimDiskFault::Sync)
                            }
                            PublicationFailure::DirectorySync => (root.clone(), SimDiskFault::Sync),
                        };
                        SimDisk::inject_fault(&fault_path, fault).unwrap();
                        let error = install(&mut state, 2).await.unwrap_err();
                        assert!(matches!(
                            error.get_ref().and_then(|source| source.downcast_ref::<SimDiskError>()),
                            Some(SimDiskError::Injected { path, fault: actual })
                                if path == &fault_path && *actual == fault
                        ), "expected {phase:?} injection: {error}");
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
                    },
                );
            }
        }
    }
}

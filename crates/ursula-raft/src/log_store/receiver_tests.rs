use std::collections::BTreeMap;
use std::fs;
use std::process::Command;

use ursula_control::ClusterId;
use ursula_control::ClusterIdentity;
use ursula_control::CompletedReceiverMutation;
use ursula_control::MembershipLogId;
use ursula_control::MetaLocalIdentity;
use ursula_control::MigrationToken;
use ursula_control::NodeRegistration;
use ursula_control::PendingReceiverMutation;
use ursula_control::ReceiverFencePhase;
use ursula_control::ReceiverFenceRecord;
use ursula_control::ReceiverLedger;
use ursula_control::ReceiverMutationKind;
use ursula_control::ReceiverProcess;
use ursula_control::ReplicaAssignment;
use ursula_control::ReplicaAssignmentPhase;
use ursula_control::ReplicaMutationResult;
use ursula_control::ReplicaRetirementEvidence;
use ursula_control::RoutingHashVersion;
use ursula_proto::admin::ProcessIncarnation;
use ursula_shard::RaftGroupId;

use super::ManagedReceiverStore;

fn identity() -> MetaLocalIdentity {
    MetaLocalIdentity {
        cluster: ClusterIdentity {
            cluster_id: ClusterId::try_from("receiver-test".to_owned()).unwrap(),
            group_count: 2,
            core_count: 1,
            routing_hash: RoutingHashVersion::Fnv1a64BucketSlashStreamV1,
        },
        node: NodeRegistration {
            node_id: 1,
            client_url: "http://node1:4437".to_owned(),
            cluster_url: "http://node1:4440".to_owned(),
            admin_url: "http://node1:4438".to_owned(),
            labels: BTreeMap::new(),
        },
    }
}
fn token(generation: u64) -> MigrationToken {
    MigrationToken {
        migration_id: 1,
        generation,
        executor: ReceiverProcess {
            node_id: 2,
            incarnation: ProcessIncarnation::from_bits(20),
        },
    }
}

async fn seeded(store: &std::sync::Arc<ManagedReceiverStore>) -> ReceiverLedger {
    let mut ledger = store.snapshot().unwrap();
    ledger.assignments_seeded = true;
    ledger
        .assignments
        .insert(RaftGroupId(0), ReplicaAssignment {
            epoch: 0,
            migration_id: 0,
            generation: 0,
            phase: ReplicaAssignmentPhase::Hosted,
        });
    store.persist(ledger).await.unwrap()
}
async fn active(store: &std::sync::Arc<ManagedReceiverStore>) -> ReceiverLedger {
    let mut ledger = seeded(store).await;
    ledger.high_water_generation = 7;
    ledger.fence = Some(ReceiverFenceRecord {
        token: token(7),
        process: ProcessIncarnation::from_bits(10),
        phase: ReceiverFencePhase::Activating,
    });
    let mut ledger = store.persist(ledger).await.unwrap();
    ledger.fence.as_mut().unwrap().phase = ReceiverFencePhase::Active;
    store.persist(ledger).await.unwrap()
}

#[tokio::test]
async fn receiver_checkpoint_rejects_stale_cas_and_preserves_retirement_tombstones() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("receiver");
    let store = ManagedReceiverStore::open(path.clone(), identity())
        .await
        .unwrap();
    let mut ledger = active(&store).await;
    let stale = ledger.clone();
    ledger.fence.as_mut().unwrap().phase = ReceiverFencePhase::Retiring;
    ledger.assignments.get_mut(&RaftGroupId(0)).unwrap().phase = ReplicaAssignmentPhase::Retiring;
    let mut ledger = store.persist(ledger).await.unwrap();
    assert!(store.persist(stale).await.is_err());
    ledger.fence.as_mut().unwrap().phase = ReceiverFencePhase::Retired;
    let assignment = ledger.assignments.get_mut(&RaftGroupId(0)).unwrap();
    assignment.phase = ReplicaAssignmentPhase::Retired;
    assignment.epoch = 1;
    assignment.generation = 7;
    assignment.migration_id = 1;
    let ledger = store.persist(ledger).await.unwrap();
    assert!(!ledger.may_restore(RaftGroupId(0)));
    assert!(!ledger.may_restore(RaftGroupId(1)));
    let mut reopen = ledger.clone();
    reopen.fence.as_mut().unwrap().phase = ReceiverFencePhase::Active;
    assert!(store.persist(reopen).await.is_err());
    let mut recreate = ledger.clone();
    recreate.assignments.get_mut(&RaftGroupId(0)).unwrap().phase = ReplicaAssignmentPhase::Hosted;
    assert!(store.persist(recreate).await.is_err());
    drop(store);
    let store = ManagedReceiverStore::open(path, identity()).await.unwrap();
    assert_eq!(store.snapshot().unwrap(), ledger);
    let mut replacement = ledger.clone();
    replacement.high_water_generation = 8;
    replacement.fence = Some(ReceiverFenceRecord {
        token: token(8),
        process: ProcessIncarnation::from_bits(11),
        phase: ReceiverFencePhase::Activating,
    });
    replacement.fence.as_mut().unwrap().token.migration_id = 2;
    let mut replacement = store.persist(replacement).await.unwrap();
    replacement.fence.as_mut().unwrap().phase = ReceiverFencePhase::Active;
    let mut replacement = store.persist(replacement).await.unwrap();
    let assignment = replacement.assignments.get_mut(&RaftGroupId(0)).unwrap();
    assignment.phase = ReplicaAssignmentPhase::Preparing;
    assignment.generation = 8;
    assignment.migration_id = 2;
    assert!(
        store
            .persist(replacement)
            .await
            .unwrap()
            .may_restore(RaftGroupId(0))
    );
}

#[tokio::test]
async fn receiver_checkpoint_binding_corruption_missing_history_and_publication_failure_are_closed()
{
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("receiver");
    let store = ManagedReceiverStore::open(path.clone(), identity())
        .await
        .unwrap();
    assert!(
        ManagedReceiverStore::open(path.clone(), identity())
            .await
            .is_err()
    );
    let ledger = active(&store).await;
    let before = fs::read(&path).unwrap();
    fs::create_dir(root.path().join("receiver.tmp")).unwrap();
    let mut next = ledger.clone();
    next.fence.as_mut().unwrap().phase = ReceiverFencePhase::Retiring;
    assert!(store.persist(next).await.is_err());
    assert!(store.snapshot().is_err());
    assert_eq!(fs::read(&path).unwrap(), before);
    drop(store);
    fs::remove_dir(root.path().join("receiver.tmp")).unwrap();
    let mut foreign = identity();
    foreign.node.node_id = 2;
    assert!(
        ManagedReceiverStore::open(path.clone(), foreign)
            .await
            .is_err()
    );
    for len in [0, before.len() / 2, before.len() - 1] {
        fs::write(&path, &before[..len]).unwrap();
        assert!(
            ManagedReceiverStore::open(path.clone(), identity())
                .await
                .is_err()
        );
    }
    fs::write(&path, &before).unwrap();
    fs::remove_file(&path).unwrap();
    assert!(
        ManagedReceiverStore::open(path.clone(), identity())
            .await
            .is_err()
    );
    fs::write(&path, before).unwrap();
    assert_eq!(
        ManagedReceiverStore::open(path, identity())
            .await
            .unwrap()
            .snapshot()
            .unwrap(),
        ledger
    );
}

#[tokio::test]
#[ignore = "subprocess crash entrypoint"]
async fn receiver_crash_child() {
    let path = std::env::var_os("URSULA_RECEIVER_CRASH_PATH").unwrap();
    let mode = std::env::var("URSULA_RECEIVER_CRASH_MODE").unwrap();
    let store = ManagedReceiverStore::open(path.into(), identity())
        .await
        .unwrap();
    let mut ledger = active(&store).await;
    if mode == "pending" {
        ledger.pending = Some(PendingReceiverMutation {
            token: token(7),
            raft_group_id: RaftGroupId(0),
            request_id: "lost-membership-reply".to_owned(),
            process: ProcessIncarnation::from_bits(10),
            operation: ursula_control::ReceiverMutationKind::Membership,
        });
        store.persist(ledger).await.unwrap();
    } else if mode.starts_with("replica-") {
        let prepare = mode.starts_with("replica-prepare-");
        let group = RaftGroupId(u32::from(prepare));
        let operation = if prepare {
            ReceiverMutationKind::PrepareReplica { epoch: 0 }
        } else {
            ReceiverMutationKind::ReleaseReplica {
                epoch: 1,
                membership_log_id: MembershipLogId {
                    term: 2,
                    node_id: 2,
                    index: 14,
                },
            }
        };
        ledger.assignments.insert(group, ReplicaAssignment {
            epoch: u64::from(!prepare),
            migration_id: 1,
            generation: 7,
            phase: if prepare {
                ReplicaAssignmentPhase::Preparing
            } else {
                ReplicaAssignmentPhase::Retiring
            },
        });
        ledger.pending = Some(PendingReceiverMutation {
            token: token(7),
            raft_group_id: group,
            request_id: "stable-replica-action".to_owned(),
            process: ProcessIncarnation::from_bits(10),
            operation,
        });
        let mut ledger = store.persist(ledger).await.unwrap();
        if mode.ends_with("completed") {
            let request = ledger.pending.take().unwrap();
            ledger.assignments.get_mut(&group).unwrap().phase = if prepare {
                ReplicaAssignmentPhase::Hosted
            } else {
                ReplicaAssignmentPhase::Retired
            };
            let process = ReceiverProcess {
                node_id: 1,
                incarnation: request.process.clone(),
            };
            let result = match &request.operation {
                ReceiverMutationKind::PrepareReplica { .. } => {
                    ReplicaMutationResult::Prepared { process }
                }
                ReceiverMutationKind::ReleaseReplica {
                    epoch,
                    membership_log_id,
                } => ReplicaMutationResult::Released {
                    evidence: ReplicaRetirementEvidence {
                        process,
                        placement_epoch: *epoch,
                        membership_log_id: membership_log_id.clone(),
                        work_drained: true,
                        snapshot_references_retired: true,
                        local_records_reclaimed: true,
                    },
                },
                ReceiverMutationKind::Membership => unreachable!(),
            };
            ledger.completed = Some(CompletedReceiverMutation { request, result });
            store.persist(ledger).await.unwrap();
        }
    } else if mode == "retired" {
        ledger.fence.as_mut().unwrap().phase = ReceiverFencePhase::Retiring;
        let mut ledger = store.persist(ledger).await.unwrap();
        ledger.fence.as_mut().unwrap().phase = ReceiverFencePhase::Retired;
        store.persist(ledger).await.unwrap();
    }
    std::process::exit(0);
}

#[tokio::test]
async fn receiver_process_exit_preserves_active_pending_and_retired_authority() {
    for mode in ["active", "pending", "retired"] {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("receiver");
        let status = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "log_store::receiver::tests::receiver_crash_child",
                "--ignored",
                "--nocapture",
            ])
            .env("URSULA_RECEIVER_CRASH_PATH", &path)
            .env("URSULA_RECEIVER_CRASH_MODE", mode)
            .status()
            .unwrap();
        assert!(status.success());
        let store = ManagedReceiverStore::open(path, identity()).await.unwrap();
        let ledger = store.snapshot().unwrap();
        assert_eq!(ledger.high_water_generation, 7);
        assert_eq!(ledger.pending.is_some(), mode == "pending");
        assert_eq!(
            ledger.fence.as_ref().unwrap().phase,
            if mode == "retired" {
                ReceiverFencePhase::Retired
            } else {
                ReceiverFencePhase::Active
            }
        );
        let mut stale = ledger.clone();
        stale.high_water_generation = 6;
        stale.fence.as_mut().unwrap().token.generation = 6;
        assert!(store.persist(stale).await.is_err());
        let mut process = ledger;
        process.fence.as_mut().unwrap().process = ProcessIncarnation::from_bits(11);
        assert!(store.persist(process).await.is_err());
    }
}

// The child exits without destructors after checkpoint publication. Receipts
// here are synthetic storage fixtures; native RF3 tests produce actual cleanup.
#[tokio::test]
async fn receiver_replica_checkpoints_survive_process_exit_without_forgetting_work_or_receipts() {
    for mode in [
        "replica-prepare-pending",
        "replica-prepare-completed",
        "replica-release-pending",
        "replica-release-completed",
    ] {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("receiver");
        let status = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "log_store::receiver::tests::receiver_crash_child",
                "--ignored",
                "--nocapture",
            ])
            .env("URSULA_RECEIVER_CRASH_PATH", &path)
            .env("URSULA_RECEIVER_CRASH_MODE", mode)
            .status()
            .unwrap();
        assert!(status.success());
        let store = ManagedReceiverStore::open(path, identity()).await.unwrap();
        let ledger = store.snapshot().unwrap();
        let completed = mode.ends_with("completed");
        let prepare = mode.starts_with("replica-prepare-");
        let group = RaftGroupId(u32::from(prepare));
        assert_eq!(ledger.pending.is_none(), completed);
        assert_eq!(ledger.completed.is_some(), completed);
        assert_eq!(ledger.may_restore(group), prepare);
        let request = if completed {
            &ledger.completed.as_ref().unwrap().request
        } else {
            ledger.pending.as_ref().unwrap()
        };
        assert_eq!(request.request_id, "stable-replica-action");
        assert_eq!(request.token, token(7));
        if completed {
            let mut forged = ledger.clone();
            match &mut forged.completed.as_mut().unwrap().result {
                ReplicaMutationResult::Prepared { process } => process.node_id = 2,
                ReplicaMutationResult::Released { evidence } => evidence.process.node_id = 2,
            }
            assert!(store.persist(forged).await.is_err());
        } else {
            let mut forgotten = ledger.clone();
            forgotten.pending = None;
            assert!(store.persist(forgotten).await.is_err());
            let mut foreign = ledger.clone();
            let request = foreign.pending.take().unwrap();
            foreign.assignments.get_mut(&group).unwrap().phase = if prepare {
                ReplicaAssignmentPhase::Hosted
            } else {
                ReplicaAssignmentPhase::Retired
            };
            let process = ReceiverProcess {
                node_id: 2,
                incarnation: request.process.clone(),
            };
            let result = match &request.operation {
                ReceiverMutationKind::PrepareReplica { .. } => {
                    ReplicaMutationResult::Prepared { process }
                }
                ReceiverMutationKind::ReleaseReplica {
                    epoch,
                    membership_log_id,
                } => ReplicaMutationResult::Released {
                    evidence: ReplicaRetirementEvidence {
                        process,
                        placement_epoch: *epoch,
                        membership_log_id: membership_log_id.clone(),
                        work_drained: true,
                        snapshot_references_retired: true,
                        local_records_reclaimed: true,
                    },
                },
                ReceiverMutationKind::Membership => unreachable!(),
            };
            foreign.completed = Some(CompletedReceiverMutation { request, result });
            assert!(foreign.validate(2).is_ok());
            assert!(store.persist(foreign).await.is_err());
        }
        assert_eq!(store.snapshot().unwrap(), ledger);
    }
}

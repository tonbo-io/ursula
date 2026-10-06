use std::collections::BTreeMap;
use std::sync::Arc;

use futures_util::FutureExt;
use ursula_control::ClusterId;
use ursula_control::ClusterIdentity;
use ursula_control::CompletedReceiverMutation;
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
use ursula_control::RoutingHashVersion;
use ursula_proto::admin::ProcessIncarnation;
use ursula_shard::RaftGroupId;

use super::ManagedReceiverStore;
use super::SimulatedReceiverDisk;
use super::SimulatedReceiverWriteFault;

fn identity() -> MetaLocalIdentity {
    MetaLocalIdentity {
        cluster: ClusterIdentity {
            cluster_id: ClusterId::try_from("simulated-receiver".to_owned()).unwrap(),
            group_count: 2,
            core_count: 1,
            routing_hash: RoutingHashVersion::Fnv1a64BucketSlashStreamV1,
        },
        node: NodeRegistration {
            node_id: 1,
            client_url: "http://node1:4437".to_owned(),
            cluster_url: "http://node1:4439".to_owned(),
            admin_url: "http://node1:4438".to_owned(),
            labels: BTreeMap::new(),
        },
    }
}

fn persist(
    store: &Arc<ManagedReceiverStore>,
    ledger: ReceiverLedger,
) -> std::io::Result<ReceiverLedger> {
    // The simulated atomic publication has no asynchronous host I/O. A pending
    // future here would accidentally introduce a different durability model.
    store
        .persist(ledger)
        .now_or_never()
        .expect("atomic simulated checkpoint")
}

fn active(disk: &SimulatedReceiverDisk) -> Arc<ManagedReceiverStore> {
    let store = ManagedReceiverStore::open_simulated(disk.clone(), identity()).unwrap();
    let mut ledger = store.snapshot().unwrap();
    ledger.assignments_seeded = true;
    let mut ledger = persist(&store, ledger).unwrap();
    ledger.high_water_generation = 7;
    ledger.fence = Some(ReceiverFenceRecord {
        token: MigrationToken {
            migration_id: 1,
            generation: 7,
            executor: ReceiverProcess {
                node_id: 2,
                incarnation: ProcessIncarnation::from_bits(20),
            },
        },
        process: ProcessIncarnation::from_bits(10),
        phase: ReceiverFencePhase::Activating,
    });
    let mut ledger = persist(&store, ledger).unwrap();
    ledger.fence.as_mut().unwrap().phase = ReceiverFencePhase::Active;
    persist(&store, ledger).unwrap();
    store
}

fn prepare(mut ledger: ReceiverLedger) -> ReceiverLedger {
    let fence = ledger.fence.as_ref().unwrap();
    ledger
        .assignments
        .insert(RaftGroupId(1), ReplicaAssignment {
            epoch: 0,
            migration_id: 1,
            generation: 7,
            phase: ReplicaAssignmentPhase::Preparing,
        });
    ledger.pending = Some(PendingReceiverMutation {
        token: fence.token.clone(),
        raft_group_id: RaftGroupId(1),
        request_id: "prepare-group1".to_owned(),
        process: fence.process.clone(),
        operation: ReceiverMutationKind::PrepareReplica { epoch: 0 },
    });
    ledger
}

#[test]
fn simulated_receiver_disk_is_exclusive_identity_bound_and_isolated() {
    let disk = SimulatedReceiverDisk::default();
    let store = active(&disk);
    let expected = store.snapshot().unwrap();
    assert!(ManagedReceiverStore::open_simulated(disk.clone(), identity()).is_err());
    let retained = store.clone();
    drop(store);
    assert!(ManagedReceiverStore::open_simulated(disk.clone(), identity()).is_err());
    drop(retained);
    let mut foreign = identity();
    foreign.node.node_id = 3;
    assert!(ManagedReceiverStore::open_simulated(disk.clone(), foreign).is_err());
    let reopened = ManagedReceiverStore::open_simulated(disk, identity()).unwrap();
    assert_eq!(reopened.snapshot().unwrap(), expected);
    let separate =
        ManagedReceiverStore::open_simulated(SimulatedReceiverDisk::default(), identity()).unwrap();
    assert_eq!(separate.snapshot().unwrap(), ReceiverLedger::default());
}

#[test]
fn simulated_receiver_publication_failure_recovers_exact_side_of_commit() {
    for fault in [
        SimulatedReceiverWriteFault::BeforeCommit,
        SimulatedReceiverWriteFault::AfterCommit,
    ] {
        let disk = SimulatedReceiverDisk::default();
        let store = active(&disk);
        let before = store.snapshot().unwrap();
        let proposed = prepare(before.clone());
        disk.fail_next_write(fault).unwrap();
        // Idempotent reads/writes and invalid CAS do not use a disk fault.
        assert_eq!(persist(&store, before.clone()).unwrap(), before);
        let mut stale = proposed.clone();
        stale.revision = 0;
        assert!(persist(&store, stale).is_err());
        assert_eq!(store.snapshot().unwrap(), before);
        assert!(persist(&store, proposed.clone()).is_err());
        assert!(store.snapshot().is_err());
        assert!(persist(&store, before.clone()).is_err());
        drop(store);
        let reopened = ManagedReceiverStore::open_simulated(disk, identity()).unwrap();
        let expected = if fault == SimulatedReceiverWriteFault::AfterCommit {
            let mut committed = proposed;
            committed.revision += 1;
            committed
        } else {
            before
        };
        assert_eq!(reopened.snapshot().unwrap(), expected);
    }
}

#[test]
fn simulated_receiver_lost_prepare_receipt_preserves_exact_replay_and_authority() {
    let disk = SimulatedReceiverDisk::default();
    let store = active(&disk);
    let mut ledger = persist(&store, prepare(store.snapshot().unwrap())).unwrap();
    let request = ledger.pending.take().unwrap();
    ledger.assignments.get_mut(&RaftGroupId(1)).unwrap().phase = ReplicaAssignmentPhase::Hosted;
    ledger.completed = Some(CompletedReceiverMutation {
        result: ReplicaMutationResult::Prepared {
            process: ReceiverProcess {
                node_id: 1,
                incarnation: request.process.clone(),
            },
        },
        request,
    });
    let mut foreign = ledger.clone();
    if let ReplicaMutationResult::Prepared { process } =
        &mut foreign.completed.as_mut().unwrap().result
    {
        process.node_id = 3;
    }
    assert!(persist(&store, foreign).is_err());
    disk.fail_next_write(SimulatedReceiverWriteFault::AfterCommit)
        .unwrap();
    assert!(persist(&store, ledger.clone()).is_err());
    ledger.revision += 1;
    drop(store);
    let reopened = ManagedReceiverStore::open_simulated(disk, identity()).unwrap();
    assert_eq!(reopened.snapshot().unwrap(), ledger);
    assert!(ledger.may_restore(RaftGroupId(1)));
    assert_eq!(persist(&reopened, ledger.clone()).unwrap(), ledger);
    let mut forgotten = ledger.clone();
    forgotten.completed = None;
    assert!(persist(&reopened, forgotten).is_err());
}

#[test]
fn simulated_receiver_takeover_keeps_pending_work_and_rejects_old_generation() {
    let disk = SimulatedReceiverDisk::default();
    let store = active(&disk);
    let old = persist(&store, prepare(store.snapshot().unwrap())).unwrap();
    drop(store);
    let reopened = ManagedReceiverStore::open_simulated(disk, identity()).unwrap();
    let mut replacement = old.clone();
    replacement.high_water_generation = 8;
    let fence = replacement.fence.as_mut().unwrap();
    fence.token.generation = 8;
    fence.process = ProcessIncarnation::from_bits(11);
    fence.phase = ReceiverFencePhase::Activating;
    let replacement = persist(&reopened, replacement).unwrap();
    assert_eq!(replacement.pending, old.pending);
    let mut regressed = old;
    regressed.revision = replacement.revision;
    assert!(persist(&reopened, regressed).is_err());
    let mut cleared = replacement.clone();
    cleared.pending = None;
    assert!(persist(&reopened, cleared).is_err());
    let mut prematurely_active = replacement.clone();
    prematurely_active.fence.as_mut().unwrap().phase = ReceiverFencePhase::Active;
    assert!(persist(&reopened, prematurely_active).is_err());
    assert_eq!(reopened.snapshot().unwrap(), replacement);
}

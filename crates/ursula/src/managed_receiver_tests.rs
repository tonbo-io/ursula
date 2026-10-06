use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::http::Request;
use axum::http::StatusCode;
use openraft::BasicNode;
use openraft::Config;
use tokio::net::TcpListener;
use tower::ServiceExt;
use ursula_control::ClusterBootstrap;
use ursula_control::ClusterId;
use ursula_control::ClusterIdentity;
use ursula_control::ControlCommand;
use ursula_control::ControlResponse;
use ursula_control::FinalMembershipEvidence;
use ursula_control::MembershipLogId;
use ursula_control::MetaLocalIdentity;
use ursula_control::MigrationRequest;
use ursula_control::MigrationToken;
use ursula_control::MigrationUpdate;
use ursula_control::NodeRegistration;
use ursula_control::ProjectionCursor;
use ursula_control::ReceiverFencePhase;
use ursula_control::ReceiverProcess;
use ursula_control::ReplicaAppliedEvidence;
use ursula_control::ReplicaAssignment;
use ursula_control::ReplicaAssignmentPhase;
use ursula_control::ReplicaRetirementEvidence;
use ursula_control::RoutingHashVersion;
use ursula_control::VerifiedGroupMembership;
use ursula_proto::admin::PROCESS_INCARNATION_HEADER;
use ursula_proto::admin::ProcessIncarnation;
use ursula_raft::MetaGrpcRaftNetworkFactory;
use ursula_raft::MetaRaftGrpcService;
use ursula_raft::MetaRaftHandle;
use ursula_raft::meta_raft_grpc_service;
use ursula_shard::RaftGroupId;

use super::ManagedReceiver;
use super::ReceiverRequest;
use crate::HttpState;

fn state(receiver: Arc<ManagedReceiver>, identity: &ClusterIdentity) -> HttpState {
    let mut config = ursula_config::UrsulaConfig::default();
    config.runtime.core_count = 1;
    let spawned = crate::bootstrap::spawn_runtime(
        &config,
        crate::bootstrap::Persistence::InMemory,
        crate::bootstrap::Topology::SingleNode {
            raft_group_count: 1,
        },
    )
    .unwrap();
    HttpState::new(spawned.runtime)
        .with_configured_node_id(2)
        .with_managed_projection(Arc::new(std::sync::RwLock::new(
            ProjectionCursor::new(identity.clone()).unwrap(),
        )))
        .with_managed_receiver(receiver)
}

fn lifecycle(state: &HttpState, token: &MigrationToken, action: &str) -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri(format!("/__ursula/control/receiver/{action}"))
        .header(
            PROCESS_INCARNATION_HEADER,
            state.process_incarnation.as_str(),
        )
        .header("content-type", "application/json")
        .body(Body::from(
            serde_json::to_vec(&ReceiverRequest {
                token: token.clone(),
            })
            .unwrap(),
        ))
        .unwrap()
}

fn activation(state: &HttpState, token: &MigrationToken) -> Request<Body> {
    lifecycle(state, token, "activate")
}

async fn update(handle: &MetaRaftHandle, token: &MigrationToken, update: MigrationUpdate) {
    let revision = handle
        .read_state(|state| {
            state
                .active_migration()
                .unwrap()
                .managed
                .as_ref()
                .unwrap()
                .revision
        })
        .await
        .unwrap();
    assert_eq!(
        handle
            .write(ControlCommand::UpdateMigration {
                token: token.clone(),
                expected_revision: revision,
                update,
                now_ms: 6,
            })
            .await
            .unwrap(),
        ControlResponse::Ok
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn receiver_http_requires_fresh_quorums_and_survives_cancelled_activation_and_process_replacement()
 {
    let root = tempfile::tempdir().unwrap();
    let identity = ClusterIdentity {
        cluster_id: ClusterId::try_from("receiver-http".to_owned()).unwrap(),
        group_count: 1,
        core_count: 1,
        routing_hash: RoutingHashVersion::Fnv1a64BucketSlashStreamV1,
    };
    let mut handles = Vec::new();
    let mut servers = Vec::new();
    let mut directory = BTreeMap::new();
    for id in 1..=3 {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let registration = NodeRegistration {
            node_id: id,
            client_url: format!("http://node{id}:4437"),
            cluster_url: format!("http://{}", listener.local_addr().unwrap()),
            admin_url: format!("http://node{id}:4438"),
            labels: BTreeMap::from([("zone".to_owned(), (id - 1).to_string())]),
        };
        directory.insert(id, registration.clone());
        let handle = MetaRaftHandle::new_bound_durable_node_with_network(
            MetaLocalIdentity {
                cluster: identity.clone(),
                node: registration,
            },
            Arc::new(
                Config {
                    heartbeat_interval: 50,
                    election_timeout_min: 150,
                    election_timeout_max: 300,
                    ..Default::default()
                }
                .validate()
                .unwrap(),
            ),
            MetaGrpcRaftNetworkFactory::new_bound(identity.clone()).unwrap(),
            root.path().join(format!("meta{id}")),
        )
        .await
        .unwrap();
        let service = MetaRaftGrpcService::new_bound(&handle).unwrap();
        let mut router = Router::new();
        for path in [
            ursula_raft::META_RAFT_APPEND_PATH,
            ursula_raft::META_RAFT_VOTE_PATH,
            ursula_raft::META_RAFT_FULL_SNAPSHOT_PATH,
            ursula_raft::META_RAFT_TRANSFER_LEADER_PATH,
            ursula_raft::META_RAFT_READ_PROJECTION_PATH,
        ] {
            router = router.route_service(path, meta_raft_grpc_service(service.clone()));
        }
        servers.push(tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        }));
        handles.push(handle);
    }
    handles[0]
        .initialize_membership(
            directory
                .iter()
                .map(|(id, node)| (*id, BasicNode::new(&node.cluster_url)))
                .collect(),
        )
        .await
        .unwrap();
    let leader = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            for (index, handle) in handles.iter().enumerate() {
                if handle.raft_handle().is_leader() {
                    return index;
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    directory.insert(4, NodeRegistration {
        node_id: 4,
        client_url: "http://node4:4437".to_owned(),
        cluster_url: "http://node4:4440".to_owned(),
        admin_url: "http://node4:4438".to_owned(),
        labels: BTreeMap::from([("zone".to_owned(), "1".to_owned())]),
    });
    let recipe = ClusterBootstrap {
        identity: identity.clone(),
        initial_meta_voters: [1, 2, 3].into(),
        nodes: directory.clone(),
        voters: BTreeMap::from([(RaftGroupId(0), [1, 2, 3].into())]),
        placement: Default::default(),
    };
    // Real metadata reads and durable receiver admission; synthetic data
    // certificates intentionally avoid claiming a physical group migration.
    let membership = VerifiedGroupMembership {
        voters: [1, 2, 3].into(),
        learners: BTreeSet::new(),
        log_id: MembershipLogId {
            term: 1,
            node_id: 1,
            index: 0,
        },
    };
    assert_eq!(
        handles[leader]
            .write(ControlCommand::BootstrapCluster {
                bootstrap: recipe.clone(),
                memberships: BTreeMap::from([(RaftGroupId(0), membership.clone())]),
                now_ms: 1
            })
            .await
            .unwrap(),
        ControlResponse::Ok
    );
    assert_eq!(
        handles[leader]
            .write(ControlCommand::SubmitMigration {
                request: MigrationRequest {
                    operation_key: "receiver-move".to_owned(),
                    raft_group_id: RaftGroupId(0),
                    expected_epoch: 0,
                    source_membership: membership,
                    target_voters: [1, 3, 4].into(),
                    target_policy: None
                },
                now_ms: 2
            })
            .await
            .unwrap(),
        ControlResponse::MigrationStarted { migration_id: 1 }
    );
    let executor = ReceiverProcess {
        node_id: 1,
        incarnation: ProcessIncarnation::from_bits(100),
    };
    let ControlResponse::ExecutorClaimed { token } = handles[leader]
        .write(ControlCommand::ClaimMigrationExecutor {
            migration_id: 1,
            expected_generation: 0,
            claim_key: ProcessIncarnation::from_bits(101),
            executor: executor.clone(),
            now_ms: 3,
        })
        .await
        .unwrap()
    else {
        panic!("claim failed");
    };
    let store =
        ursula_raft::ManagedReceiverStore::open(root.path().join("receiver2"), MetaLocalIdentity {
            cluster: identity.clone(),
            node: directory[&2].clone(),
        })
        .await
        .unwrap();
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
    store.persist(ledger).await.unwrap();
    let receiver = Arc::new(ManagedReceiver::new(
        store.clone(),
        recipe,
        handles[1].clone(),
    ));
    let old_state = state(receiver.clone(), &identity);
    let app = crate::admin_router(old_state.clone());
    assert_eq!(
        app.clone()
            .oneshot(activation(&old_state, &token))
            .await
            .unwrap()
            .status(),
        StatusCode::CONFLICT,
        "claim alone cannot admit receiver effects"
    );
    assert_eq!(
        handles[leader]
            .write(ControlCommand::UpdateMigration {
                token: token.clone(),
                expected_revision: 0,
                update: MigrationUpdate::AuthorizeReceivers,
                now_ms: 4
            })
            .await
            .unwrap(),
        ControlResponse::Ok
    );
    let guard = receiver.admit_unmanaged().await.unwrap();
    let request = activation(&old_state, &token);
    let task = tokio::spawn(async move { app.oneshot(request).await.unwrap() });
    tokio::time::timeout(Duration::from_secs(10), async {
        while receiver.gate.try_read().is_ok() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    assert!(
        store.snapshot().unwrap().fence.is_none(),
        "admin work must drain before activation"
    );
    task.abort();
    let _ = task.await;
    drop(guard);
    tokio::time::timeout(Duration::from_secs(10), async {
        while !store
            .snapshot()
            .unwrap()
            .fence
            .as_ref()
            .is_some_and(|f| f.phase == ReceiverFencePhase::Active)
        {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert!(receiver.admit_unmanaged().await.is_err());
    let app = crate::admin_router(old_state.clone());
    assert_eq!(
        app.clone()
            .oneshot(activation(&old_state, &token))
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );
    let maintenance = Request::builder()
        .method("POST")
        .uri("/__ursula/maintenance/fence/activate")
        .header(
            PROCESS_INCARNATION_HEADER,
            old_state.process_incarnation.as_str(),
        )
        .body(Body::empty())
        .unwrap();
    assert_eq!(
        app.oneshot(maintenance).await.unwrap().status(),
        StatusCode::CONFLICT
    );
    let replacement_state = state(receiver.clone(), &identity);
    let app = crate::admin_router(replacement_state.clone());
    assert_eq!(
        app.clone()
            .oneshot(activation(&old_state, &token))
            .await
            .unwrap()
            .status(),
        StatusCode::PRECONDITION_FAILED
    );
    assert_eq!(
        app.clone()
            .oneshot(activation(&replacement_state, &token))
            .await
            .unwrap()
            .status(),
        StatusCode::CONFLICT,
        "a replacement process cannot reuse the previous generation"
    );
    let ControlResponse::ExecutorClaimed { token: replacement } = handles[leader]
        .write(ControlCommand::ClaimMigrationExecutor {
            migration_id: 1,
            expected_generation: token.generation,
            claim_key: ProcessIncarnation::from_bits(201),
            executor: executor.clone(),
            now_ms: 5,
        })
        .await
        .unwrap()
    else {
        panic!("replacement claim failed");
    };
    assert_eq!(
        app.clone()
            .oneshot(activation(&replacement_state, &replacement))
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );
    assert_eq!(
        app.clone()
            .oneshot(activation(&replacement_state, &token))
            .await
            .unwrap()
            .status(),
        StatusCode::CONFLICT
    );
    let processes = (1..=4)
        .map(|id| {
            (
                id,
                if id == 2 {
                    replacement_state.process_incarnation.clone()
                } else {
                    ProcessIncarnation::from_bits(id as u128)
                },
            )
        })
        .collect::<BTreeMap<_, _>>();
    let process = |id| ReceiverProcess {
        node_id: id,
        incarnation: processes[&id].clone(),
    };
    let log = |index| MembershipLogId {
        term: 1,
        node_id: 1,
        index,
    };
    for command in [
        MigrationUpdate::CertifyReceivers {
            processes: processes.clone(),
        },
        MigrationUpdate::RecordPrepared {
            process: process(4),
        },
        MigrationUpdate::CapturePrefix { prefix: log(1) },
        MigrationUpdate::RecordLearner {
            evidence: ReplicaAppliedEvidence {
                process: process(4),
                applied_log_id: log(1),
            },
        },
        MigrationUpdate::AuthorizeMembership,
        MigrationUpdate::VerifyMembership {
            evidence: FinalMembershipEvidence {
                membership: VerifiedGroupMembership {
                    voters: [1, 3, 4].into(),
                    learners: BTreeSet::new(),
                    log_id: log(2),
                },
                committed_prefix: log(3),
                replicas: [1, 3, 4]
                    .into_iter()
                    .map(|id| {
                        (id, ReplicaAppliedEvidence {
                            process: process(id),
                            applied_log_id: log(3),
                        })
                    })
                    .collect(),
            },
        },
        MigrationUpdate::PublishPlacement,
    ] {
        update(&handles[leader], &replacement, command).await;
    }
    assert_eq!(
        app.clone()
            .oneshot(lifecycle(&replacement_state, &replacement, "retire"))
            .await
            .unwrap()
            .status(),
        StatusCode::CONFLICT,
        "publication cannot retire receivers before replica cleanup"
    );
    update(
        &handles[leader],
        &replacement,
        MigrationUpdate::RecordReleased {
            evidence: ReplicaRetirementEvidence {
                process: process(2),
                placement_epoch: 1,
                membership_log_id: log(2),
                work_drained: true,
                snapshot_references_retired: true,
                local_records_reclaimed: true,
            },
        },
    )
    .await;
    assert_eq!(
        app.clone()
            .oneshot(lifecycle(&replacement_state, &replacement, "retire"))
            .await
            .unwrap()
            .status(),
        StatusCode::CONFLICT,
        "global cleanup evidence cannot replace this removed receiver's tombstone"
    );
    let mut ledger = store.snapshot().unwrap();
    let assignment = ledger.assignments.get_mut(&RaftGroupId(0)).unwrap();
    assignment.phase = ReplicaAssignmentPhase::Retiring;
    assignment.migration_id = replacement.migration_id;
    assignment.generation = replacement.generation;
    assignment.epoch = 1;
    let mut ledger = store.persist(ledger).await.unwrap();
    ledger.assignments.get_mut(&RaftGroupId(0)).unwrap().phase = ReplicaAssignmentPhase::Retired;
    store.persist(ledger).await.unwrap();
    assert_eq!(
        app.clone()
            .oneshot(lifecycle(&replacement_state, &replacement, "retire"))
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );
    assert_eq!(
        store.snapshot().unwrap().fence.unwrap().phase,
        ReceiverFencePhase::Retired
    );
    assert!(receiver.admit_unmanaged().await.is_ok());
    assert_eq!(
        app.clone()
            .oneshot(activation(&replacement_state, &replacement))
            .await
            .unwrap()
            .status(),
        StatusCode::CONFLICT,
        "retired authority cannot reopen at the same generation"
    );
    let ControlResponse::ExecutorClaimed { token: replacement } = handles[leader]
        .write(ControlCommand::ClaimMigrationExecutor {
            migration_id: 1,
            expected_generation: replacement.generation,
            claim_key: ProcessIncarnation::from_bits(301),
            executor,
            now_ms: 7,
        })
        .await
        .unwrap()
    else {
        panic!("retirement recovery claim failed");
    };
    assert_eq!(
        app.clone()
            .oneshot(activation(&replacement_state, &replacement))
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );
    let before = store.snapshot().unwrap();
    for index in [0, 2] {
        handles[index].shutdown().await.unwrap();
        servers[index].abort();
    }
    assert_eq!(
        app.oneshot(activation(&replacement_state, &replacement))
            .await
            .unwrap()
            .status(),
        StatusCode::CONFLICT,
        "cached token cannot replace a fresh meta quorum read"
    );
    assert_eq!(store.snapshot().unwrap(), before);
    handles[1].shutdown().await.unwrap();
    servers[1].abort();
}

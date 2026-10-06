//! Native HTTP receiver tests with actual RF3/RF5 data and meta consensus. Each test
//! drives membership directly; the supported fenced membership executor is a
//! separate acceptance gate. HTTP-state replacement is not a binary restart.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::body::to_bytes;
use axum::http::Request;
use axum::http::StatusCode;
use openraft::BasicNode;
use openraft::Config;
use openraft::rt::WatchReceiver;
use openraft::storage::RaftSnapshotBuilder;
use openraft::storage::RaftStateMachine;
use openraft::vote::RaftLeaderId;
use tokio::net::TcpListener;
use tower::ServiceExt;
use ursula_control::ClusterBootstrap;
use ursula_control::ClusterId;
use ursula_control::ClusterIdentity;
use ursula_control::CompletedReceiverMutation;
use ursula_control::ControlCommand;
use ursula_control::ControlResponse;
use ursula_control::FinalMembershipEvidence;
use ursula_control::MembershipLogId;
use ursula_control::MetaLocalIdentity;
use ursula_control::MigrationRequest;
use ursula_control::MigrationToken;
use ursula_control::MigrationUpdate;
use ursula_control::NodeRegistration;
use ursula_control::PendingReceiverMutation;
use ursula_control::ProjectionCursor;
use ursula_control::ReceiverProcess;
use ursula_control::ReplicaAppliedEvidence;
use ursula_control::ReplicaAssignment;
use ursula_control::ReplicaAssignmentPhase;
use ursula_control::ReplicaMutationResult;
use ursula_control::RoutingHashVersion;
use ursula_proto::admin::PROCESS_INCARNATION_HEADER;
use ursula_proto::admin::ProcessIncarnation;
use ursula_raft::ManagedReceiverStore;
use ursula_raft::MetaGrpcRaftNetworkFactory;
use ursula_raft::MetaRaftGrpcService;
use ursula_raft::MetaRaftHandle;
use ursula_raft::RaftGroupHandleRegistry;
use ursula_raft::StaticGrpcRaftGroupEngineFactory;
use ursula_raft::meta_raft_grpc_service;
use ursula_runtime::CreateStreamRequest;
use ursula_runtime::RuntimeConfig;
use ursula_runtime::RuntimeThreading;
use ursula_runtime::ShardRuntime;
use ursula_shard::BucketStreamId;
use ursula_shard::RaftGroupId;

use super::ReceiverMutationKind;
use super::ReplicaRequest;
use crate::HttpState;
use crate::managed_receiver::ManagedReceiver;
use crate::managed_receiver::ReceiverRequest;

const GROUP: RaftGroupId = RaftGroupId(0);

struct Fixture {
    root: tempfile::TempDir,
    recipe: ClusterBootstrap,
    states: Vec<HttpState>,
    receivers: Vec<Arc<ManagedReceiver>>,
    meta: Vec<MetaRaftHandle>,
    servers: Vec<tokio::task::JoinHandle<()>>,
}

impl Fixture {
    async fn new(replicas: usize) -> Self {
        let root = tempfile::tempdir().unwrap();
        let identity = ClusterIdentity {
            cluster_id: ClusterId::try_from("replica-http".to_owned()).unwrap(),
            group_count: 2,
            core_count: 1,
            routing_hash: RoutingHashVersion::Fnv1a64BucketSlashStreamV1,
        };
        let mut listeners = Vec::new();
        let mut nodes = BTreeMap::new();
        for id in 1..=replicas as u64 + 1 {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            nodes.insert(id, NodeRegistration {
                node_id: id,
                client_url: format!("http://node{id}:4437"),
                admin_url: format!("http://node{id}:4438"),
                cluster_url: format!("http://{}", listener.local_addr().unwrap()),
                labels: BTreeMap::from([(
                    "zone".to_owned(),
                    if id == replicas as u64 + 1 {
                        "2".to_owned()
                    } else {
                        match id {
                            1 | 4 => "1",
                            2 | 5 => "2",
                            3 => "3",
                            _ => unreachable!(),
                        }
                        .to_owned()
                    },
                )]),
            });
            listeners.push(listener);
        }
        let source: BTreeSet<_> = (1..=replicas as u64).collect();
        let recipe = ClusterBootstrap {
            identity: identity.clone(),
            initial_meta_voters: source.clone(),
            nodes: nodes.clone(),
            voters: BTreeMap::from([(GROUP, source.clone()), (RaftGroupId(1), source)]),
            placement: ursula_control::PlacementPolicy {
                default_replication_factor: ursula_control::ReplicationFactor::try_from(
                    replicas as u32,
                )
                .unwrap(),
                ..Default::default()
            },
        };
        let mut states = Vec::new();
        let mut receivers = Vec::new();
        let mut meta = Vec::new();
        let mut servers = Vec::new();
        for (index, listener) in listeners.into_iter().enumerate() {
            let id = index as u64 + 1;
            let local = MetaLocalIdentity {
                cluster: identity.clone(),
                node: nodes[&id].clone(),
            };
            let handle = MetaRaftHandle::new_bound_durable_node_with_network(
                local.clone(),
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
            let store =
                ManagedReceiverStore::open(root.path().join(format!("receiver{id}")), local)
                    .await
                    .unwrap();
            let mut ledger = store.snapshot().unwrap();
            ledger.assignments_seeded = true;
            if id <= replicas as u64 {
                for group in [GROUP, RaftGroupId(1)] {
                    ledger.assignments.insert(group, ReplicaAssignment {
                        epoch: 0,
                        migration_id: 0,
                        generation: 0,
                        phase: ReplicaAssignmentPhase::Hosted,
                    });
                }
            }
            store.persist(ledger.clone()).await.unwrap();
            let registry = RaftGroupHandleRegistry::default();
            registry.set_managed_hosting(ledger.assignments.keys().copied().collect());
            let factory = StaticGrpcRaftGroupEngineFactory::new(
                id,
                nodes
                    .iter()
                    .map(|(id, node)| (*id, node.cluster_url.clone())),
                false,
                registry.clone(),
            )
            .with_raft_log_dir(root.path().join(format!("data{id}")));
            let mut config = RuntimeConfig::new(1, 2);
            config.threading = RuntimeThreading::HostedTokio;
            let runtime = ShardRuntime::spawn_with_engine_factory(config, factory).unwrap();
            let receiver = Arc::new(ManagedReceiver::new(store, recipe.clone(), handle.clone()));
            let state = HttpState::with_raft_registry(runtime, registry)
                .with_configured_node_id(id)
                .with_managed_projection(Arc::new(std::sync::RwLock::new(
                    ProjectionCursor::new(identity.clone()).unwrap(),
                )))
                .with_managed_receiver(receiver.clone());
            let service = MetaRaftGrpcService::new_bound(&handle).unwrap();
            let mut router = crate::cluster_router_from_state(state.clone());
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
            states.push(state);
            receivers.push(receiver);
            meta.push(handle);
        }
        for state in states.iter().take(replicas) {
            for group in [GROUP, RaftGroupId(1)] {
                state.runtime.warm_group(group).await.unwrap();
            }
        }
        for group in [GROUP, RaftGroupId(1)] {
            let raft = states[0].raft_registry().unwrap().get(group).unwrap();
            raft.initialize(
                recipe.voters[&group]
                    .iter()
                    .map(|id| (*id, BasicNode::new(&nodes[id].cluster_url)))
                    .collect::<BTreeMap<_, _>>(),
            )
            .await
            .unwrap();
            raft.wait(Some(Duration::from_secs(10)))
                .current_leader(1, "data fixture leader")
                .await
                .unwrap();
        }
        meta[0]
            .initialize_membership(
                recipe
                    .initial_meta_voters
                    .iter()
                    .map(|id| (*id, BasicNode::new(&nodes[id].cluster_url)))
                    .collect(),
            )
            .await
            .unwrap();
        meta[0]
            .raft_handle()
            .wait(Some(Duration::from_secs(10)))
            .current_leader(1, "meta fixture leader")
            .await
            .unwrap();
        let memberships =
            ursula_raft::collect_bootstrap_memberships(&recipe, Duration::from_secs(10))
                .await
                .unwrap();
        assert_eq!(
            meta[0]
                .write(ControlCommand::BootstrapCluster {
                    bootstrap: recipe.clone(),
                    memberships,
                    now_ms: 1
                })
                .await
                .unwrap(),
            ControlResponse::Ok
        );
        Self {
            root,
            recipe,
            states,
            receivers,
            meta,
            servers,
        }
    }

    async fn claim(&self, old: u64) -> MigrationToken {
        let ControlResponse::ExecutorClaimed { token } = self.meta[0]
            .write(ControlCommand::ClaimMigrationExecutor {
                migration_id: 1,
                expected_generation: old,
                claim_key: ProcessIncarnation::from_bits(u128::from(old) + 100),
                executor: self.process(0),
                now_ms: 3,
            })
            .await
            .unwrap()
        else {
            panic!("claim failed");
        };
        token
    }
    fn process(&self, index: usize) -> ReceiverProcess {
        ReceiverProcess {
            node_id: index as u64 + 1,
            incarnation: self.states[index].process_incarnation.clone(),
        }
    }
    async fn update(&self, token: &MigrationToken, update: MigrationUpdate) {
        let revision = self.meta[0]
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
            self.meta[0]
                .write(ControlCommand::UpdateMigration {
                    token: token.clone(),
                    expected_revision: revision,
                    update,
                    now_ms: 4
                })
                .await
                .unwrap(),
            ControlResponse::Ok
        );
    }
    async fn certify(&self, token: &MigrationToken) {
        for state in &self.states {
            let response = crate::admin_router(state.clone())
                .oneshot(lifecycle(state, token, "activate"))
                .await
                .unwrap();
            assert_eq!(
                response.status(),
                StatusCode::OK,
                "{}",
                String::from_utf8_lossy(&to_bytes(response.into_body(), 1 << 20).await.unwrap())
            );
        }
        self.update(token, MigrationUpdate::CertifyReceivers {
            processes: self
                .states
                .iter()
                .enumerate()
                .map(|(index, state)| (index as u64 + 1, state.process_incarnation.clone()))
                .collect(),
        })
        .await;
    }
    async fn stop(self) {
        for state in self.states {
            state
                .raft_registry()
                .unwrap()
                .quiesce_for_restart()
                .await
                .unwrap();
        }
        for handle in self.meta {
            handle.shutdown().await.unwrap();
        }
        for task in self.servers {
            task.abort();
            let _ = task.await;
        }
    }
}

fn lifecycle(state: &HttpState, token: &MigrationToken, action: &str) -> Request<Body> {
    request(state, action, &ReceiverRequest {
        token: token.clone(),
    })
}
fn request<T: serde::Serialize>(state: &HttpState, action: &str, payload: &T) -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri(format!("/__ursula/control/receiver/{action}"))
        .header(
            PROCESS_INCARNATION_HEADER,
            state.process_incarnation.as_str(),
        )
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(payload).unwrap()))
        .unwrap()
}
async fn mutate(
    state: &HttpState,
    request_body: &ReplicaRequest,
    action: &str,
) -> (StatusCode, Vec<u8>) {
    let response = crate::admin_router(state.clone())
        .oneshot(request(state, action, request_body))
        .await
        .unwrap();
    (
        response.status(),
        to_bytes(response.into_body(), 1 << 20)
            .await
            .unwrap()
            .to_vec(),
    )
}

fn applied(state: &HttpState) -> MembershipLogId {
    let raft = state.raft_registry().unwrap().get(GROUP).unwrap();
    let log = raft.metrics().borrow_watched().last_applied.unwrap();
    MembershipLogId {
        term: log.committed_leader_id().term(),
        node_id: *log.committed_leader_id().node_id(),
        index: log.index(),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replica_http_recovers_pre_core_prepare_and_replays_actual_rf3_release_after_cancellation()
{
    replica_scenario(3, false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replica_http_reconciles_release_after_new_generation_and_replaced_process() {
    replica_scenario(3, true).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rf5_replica_http_preserves_acked_prefix_and_replays_release_after_cancellation() {
    replica_scenario(5, false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rf5_replica_http_reconciles_release_across_generation_and_process_replacement() {
    replica_scenario(5, true).await;
}

async fn replica_scenario(replicas: usize, recover_release: bool) {
    let mut fixture = Fixture::new(replicas).await;
    let added = replicas;
    let added_id = added as u64 + 1;
    let target_indices: Vec<_> = (0..=replicas).filter(|index| *index != 1).collect();
    let target_voters: BTreeSet<_> = target_indices
        .iter()
        .map(|index| *index as u64 + 1)
        .collect();
    let source = ursula_raft::confirm_group_membership(
        GROUP,
        1,
        &fixture.recipe.nodes[&1].cluster_url,
        Duration::from_secs(5),
    )
    .await
    .unwrap();
    assert_eq!(
        fixture.meta[0]
            .write(ControlCommand::SubmitMigration {
                request: MigrationRequest {
                    operation_key: "physical-receiver".to_owned(),
                    raft_group_id: GROUP,
                    expected_epoch: 0,
                    source_membership: source.membership,
                    target_voters: target_voters.clone(),
                    target_policy: None,
                },
                now_ms: 2
            })
            .await
            .unwrap(),
        ControlResponse::MigrationStarted { migration_id: 1 }
    );
    let old_token = fixture.claim(0).await;
    fixture
        .update(&old_token, MigrationUpdate::AuthorizeReceivers)
        .await;
    fixture.certify(&old_token).await;
    assert!(
        fixture.states[added]
            .runtime
            .warm_group(GROUP)
            .await
            .is_err()
    );
    // Durable pre-core checkpoint: no actor was created. A replacement HTTP
    // process must obtain a new generation and reconcile that exact action.
    let prepare_id = format!("migration1-node{added_id}-prepare");
    let mut ledger = fixture.receivers[added].store.snapshot().unwrap();
    ledger.assignments.insert(GROUP, ReplicaAssignment {
        epoch: 0,
        migration_id: 1,
        generation: old_token.generation,
        phase: ReplicaAssignmentPhase::Preparing,
    });
    ledger.pending = Some(PendingReceiverMutation {
        token: old_token.clone(),
        raft_group_id: GROUP,
        request_id: prepare_id.clone(),
        process: fixture.states[added].process_incarnation.clone(),
        operation: ReceiverMutationKind::PrepareReplica { epoch: 0 },
    });
    fixture.receivers[added]
        .store
        .persist(ledger)
        .await
        .unwrap();
    let old_state = fixture.states[added].clone();
    fixture.states[added] = HttpState::with_raft_registry(
        old_state.runtime.clone(),
        old_state.raft_registry().unwrap().clone(),
    )
    .with_configured_node_id(added_id)
    .with_managed_projection(old_state.managed_projection.clone().unwrap())
    .with_managed_receiver(fixture.receivers[added].clone());
    assert_eq!(
        crate::admin_router(fixture.states[added].clone())
            .oneshot(lifecycle(&old_state, &old_token, "activate"))
            .await
            .unwrap()
            .status(),
        StatusCode::PRECONDITION_FAILED
    );
    let mut token = fixture.claim(old_token.generation).await;
    fixture.certify(&token).await;
    let prepare = ReplicaRequest {
        token: token.clone(),
        raft_group_id: GROUP,
        request_id: prepare_id,
        operation: ReceiverMutationKind::PrepareReplica { epoch: 0 },
    };
    let (status, ready) = mutate(&fixture.states[added], &prepare, "prepare").await;
    assert_eq!(
        status,
        StatusCode::OK,
        "{}",
        String::from_utf8_lossy(&ready)
    );
    let ready_receipt: CompletedReceiverMutation = serde_json::from_slice(&ready).unwrap();
    assert!(matches!(
        ready_receipt.result,
        ReplicaMutationResult::Prepared { .. }
    ));
    assert_eq!(
        mutate(&fixture.states[added], &prepare, "prepare").await,
        (StatusCode::OK, ready)
    );
    let mut conflicting = prepare.clone();
    conflicting.request_id = "different-key-same-generation".to_owned();
    assert_eq!(
        mutate(&fixture.states[added], &conflicting, "prepare")
            .await
            .0,
        StatusCode::CONFLICT
    );
    conflicting = prepare.clone();
    conflicting.operation = ReceiverMutationKind::PrepareReplica { epoch: 1 };
    assert_eq!(
        mutate(&fixture.states[added], &conflicting, "prepare")
            .await
            .0,
        StatusCode::CONFLICT
    );
    assert_eq!(
        mutate(&fixture.states[0], &prepare, "prepare").await.0,
        StatusCode::CONFLICT
    );
    assert_eq!(
        mutate(&fixture.states[added], &prepare, "release").await.0,
        StatusCode::BAD_REQUEST
    );
    let new_replica = fixture.states[added]
        .raft_registry()
        .unwrap()
        .get(GROUP)
        .unwrap();
    assert!(!new_replica.is_initialized().await.unwrap());
    fixture
        .update(&token, MigrationUpdate::RecordPrepared {
            process: fixture.process(added),
        })
        .await;
    let stream = (0..100)
        .map(|n| BucketStreamId::new("physical", format!("s{n}")))
        .find(|stream| fixture.states[0].runtime.locate(stream).raft_group_id == GROUP)
        .unwrap();
    let mut create = CreateStreamRequest::new(stream.clone(), "text/plain");
    create.initial_payload = b"committed-before-move".to_vec().into();
    fixture.states[0]
        .runtime
        .create_stream(create)
        .await
        .unwrap();
    let prefix = applied(&fixture.states[0]);
    fixture
        .update(&token, MigrationUpdate::CapturePrefix {
            prefix: prefix.clone(),
        })
        .await;
    let leader = fixture.states[0]
        .raft_registry()
        .unwrap()
        .get(GROUP)
        .unwrap();
    leader
        .add_learner(
            added_id,
            BasicNode::new(&fixture.recipe.nodes[&added_id].cluster_url),
            true,
        )
        .await
        .unwrap();
    new_replica
        .wait(Some(Duration::from_secs(10)))
        .applied_index_at_least(Some(prefix.index), "learner prefix")
        .await
        .unwrap();
    fixture
        .update(&token, MigrationUpdate::RecordLearner {
            evidence: ReplicaAppliedEvidence {
                process: fixture.process(added),
                applied_log_id: applied(&fixture.states[added]),
            },
        })
        .await;
    fixture
        .update(&token, MigrationUpdate::AuthorizeMembership)
        .await;
    // Direct native membership drive is fixture setup, not the supported fenced
    // receiver membership endpoint/executor, which remains pending.
    leader
        .change_membership(target_voters, false)
        .await
        .unwrap();
    let target = ursula_raft::confirm_group_membership(
        GROUP,
        1,
        &fixture.recipe.nodes[&1].cluster_url,
        Duration::from_secs(5),
    )
    .await
    .unwrap();
    let committed_prefix = applied(&fixture.states[0]);
    for index in target_indices.iter().copied() {
        fixture.states[index]
            .raft_registry()
            .unwrap()
            .get(GROUP)
            .unwrap()
            .wait(Some(Duration::from_secs(10)))
            .applied_index_at_least(Some(committed_prefix.index), "target prefix")
            .await
            .unwrap();
    }
    fixture
        .update(&token, MigrationUpdate::VerifyMembership {
            evidence: FinalMembershipEvidence {
                membership: target.membership.clone(),
                committed_prefix,
                replicas: target_indices
                    .iter()
                    .copied()
                    .map(|index| {
                        (index as u64 + 1, ReplicaAppliedEvidence {
                            process: fixture.process(index),
                            applied_log_id: applied(&fixture.states[index]),
                        })
                    })
                    .collect(),
            },
        })
        .await;
    fixture
        .update(&token, MigrationUpdate::PublishPlacement)
        .await;
    let mut release = ReplicaRequest {
        token: token.clone(),
        raft_group_id: GROUP,
        request_id: "migration1-node2-release".to_owned(),
        operation: ReceiverMutationKind::ReleaseReplica {
            epoch: 1,
            membership_log_id: target.membership.log_id.clone(),
        },
    };
    let mut wrong_release = release.clone();
    wrong_release.operation = ReceiverMutationKind::ReleaseReplica {
        epoch: 1,
        membership_log_id: MembershipLogId {
            term: target.membership.log_id.term,
            node_id: 1,
            index: target.membership.log_id.index + 1,
        },
    };
    assert_eq!(
        mutate(&fixture.states[1], &wrong_release, "release")
            .await
            .0,
        StatusCode::CONFLICT
    );
    wrong_release.operation = ReceiverMutationKind::ReleaseReplica {
        epoch: 0,
        membership_log_id: target.membership.log_id.clone(),
    };
    assert_eq!(
        mutate(&fixture.states[1], &wrong_release, "release")
            .await
            .0,
        StatusCode::CONFLICT
    );
    assert!(
        fixture.receivers[1]
            .store
            .snapshot()
            .unwrap()
            .pending
            .is_none()
    );
    let moved = fixture.states[added]
        .runtime
        .read_stream(ursula_runtime::ReadStreamRequest {
            stream_id: stream,
            offset: 0,
            max_len: 100,
            now_ms: 0,
            leader_only: false,
            read_index: None,
        })
        .await
        .unwrap();
    assert_eq!(moved.payload, b"committed-before-move");
    let registry = fixture.states[1].raft_registry().unwrap().clone();
    registry.build_snapshot_for_transfer(GROUP).await.unwrap();
    let metadata = fixture
        .root
        .path()
        .join("data2/core-0/group-0.snapshot.json");
    assert!(metadata.exists());
    let removed = registry.get(GROUP).unwrap();
    let mut held_builder = removed
        .with_state_machine(|machine| Box::pin(async move { machine.get_snapshot_builder().await }))
        .await
        .unwrap();
    let release_http_request = request(&fixture.states[1], "release", &release);
    let app = crate::admin_router(fixture.states[1].clone());
    let caller = tokio::spawn(async move { app.oneshot(release_http_request).await.unwrap() });
    tokio::time::timeout(Duration::from_secs(10), async {
        while registry.get(GROUP).is_some() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(metadata.exists());
    assert!(
        fixture.receivers[1]
            .store
            .snapshot()
            .unwrap()
            .pending
            .is_some()
    );
    caller.abort();
    let _ = caller.await;
    assert!(fixture.states[1].runtime.warm_group(GROUP).await.is_err());
    let neighbor = registry.get(RaftGroupId(1)).unwrap();
    let neighbor_stream = (0..100)
        .map(|n| BucketStreamId::new("neighbor", format!("s{n}")))
        .find(|stream| fixture.states[0].runtime.locate(stream).raft_group_id == RaftGroupId(1))
        .unwrap();
    let mut neighbor_create = CreateStreamRequest::new(neighbor_stream.clone(), "text/plain");
    neighbor_create.initial_payload = b"neighbor-kept".to_vec().into();
    let neighbor_leader = fixture.states[0]
        .raft_registry()
        .unwrap()
        .get(RaftGroupId(1))
        .unwrap();
    let written = neighbor_leader
        .client_write(ursula_runtime::GroupWriteCommand::from(neighbor_create))
        .await
        .unwrap();
    neighbor
        .wait(Some(Duration::from_secs(10)))
        .applied_index_at_least(Some(written.log_id.index()), "neighbor stays live")
        .await
        .unwrap();
    let neighbor_read = fixture.states[1]
        .runtime
        .read_stream(ursula_runtime::ReadStreamRequest {
            stream_id: neighbor_stream,
            offset: 0,
            max_len: 100,
            now_ms: 0,
            leader_only: false,
            read_index: None,
        })
        .await
        .unwrap();
    assert_eq!(neighbor_read.payload, b"neighbor-kept");
    if recover_release {
        let previous = fixture.states[1].clone();
        fixture.states[1] = HttpState::with_raft_registry(
            previous.runtime.clone(),
            previous.raft_registry().unwrap().clone(),
        )
        .with_configured_node_id(2)
        .with_managed_projection(previous.managed_projection.clone().unwrap())
        .with_managed_receiver(fixture.receivers[1].clone());
        assert_eq!(
            crate::admin_router(fixture.states[1].clone())
                .oneshot(request(&previous, "release", &release))
                .await
                .unwrap()
                .status(),
            StatusCode::PRECONDITION_FAILED
        );
        token = fixture.claim(token.generation).await;
        held_builder.build_snapshot().await.unwrap();
        // The old detached action finishes its physical cleanup but cannot
        // publish a receipt under the replaced metadata generation. Activation
        // must reconcile its durable intent before certifying this process.
        fixture.certify(&token).await;
        let recovered = fixture.receivers[1].store.snapshot().unwrap();
        let receipt = recovered.completed.as_ref().unwrap();
        assert_eq!(receipt.request.token, token);
        assert_eq!(
            receipt.request.process,
            fixture.states[1].process_incarnation
        );
        assert_eq!(
            recovered.assignments[&GROUP].phase,
            ReplicaAssignmentPhase::Retired
        );
        assert!(!metadata.exists());
        assert_eq!(
            mutate(&fixture.states[1], &release, "release").await.0,
            StatusCode::CONFLICT
        );
        let current = ursula_raft::confirm_group_membership(
            GROUP,
            1,
            &fixture.recipe.nodes[&1].cluster_url,
            Duration::from_secs(5),
        )
        .await
        .unwrap();
        let prefix = applied(&fixture.states[0]);
        for index in target_indices.iter().copied() {
            fixture.states[index]
                .raft_registry()
                .unwrap()
                .get(GROUP)
                .unwrap()
                .wait(Some(Duration::from_secs(10)))
                .applied_index_at_least(Some(prefix.index), "takeover target prefix")
                .await
                .unwrap();
        }
        fixture
            .update(&token, MigrationUpdate::VerifyMembership {
                evidence: FinalMembershipEvidence {
                    membership: current.membership,
                    committed_prefix: prefix,
                    replicas: target_indices
                        .iter()
                        .copied()
                        .map(|index| {
                            (index as u64 + 1, ReplicaAppliedEvidence {
                                process: fixture.process(index),
                                applied_log_id: applied(&fixture.states[index]),
                            })
                        })
                        .collect(),
                },
            })
            .await;
        release.token = token.clone();
    } else {
        held_builder.build_snapshot().await.unwrap();
    }
    tokio::time::timeout(Duration::from_secs(10), async {
        while fixture.receivers[1]
            .store
            .snapshot()
            .unwrap()
            .pending
            .is_some()
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let (status, released) = mutate(&fixture.states[1], &release, "release").await;
    assert_eq!(
        status,
        StatusCode::OK,
        "{}",
        String::from_utf8_lossy(&released)
    );
    assert_eq!(
        mutate(&fixture.states[1], &release, "release").await,
        (StatusCode::OK, released.clone())
    );
    let receipt: CompletedReceiverMutation = serde_json::from_slice(&released).unwrap();
    let ReplicaMutationResult::Released { evidence } = receipt.result else {
        panic!("missing cleanup receipt");
    };
    fixture
        .update(&token, MigrationUpdate::RecordReleased { evidence })
        .await;
    assert!(!metadata.exists());
    assert!(!registry.contains_group(GROUP));
    assert_eq!(
        fixture.receivers[1].store.snapshot().unwrap().assignments[&GROUP].phase,
        ReplicaAssignmentPhase::Retired
    );
    for state in &fixture.states {
        let response = crate::admin_router(state.clone())
            .oneshot(lifecycle(state, &token, "retire"))
            .await
            .unwrap();
        assert_eq!(
            response.status(),
            StatusCode::OK,
            "{}",
            String::from_utf8_lossy(&to_bytes(response.into_body(), 1 << 20).await.unwrap())
        );
    }
    fixture
        .update(&token, MigrationUpdate::RetireReceivers {
            processes: fixture
                .states
                .iter()
                .enumerate()
                .map(|(index, state)| (index as u64 + 1, state.process_incarnation.clone()))
                .collect(),
        })
        .await;
    fixture.update(&token, MigrationUpdate::Finish).await;
    for index in target_indices.iter().copied() {
        assert_eq!(
            fixture.receivers[index]
                .store
                .snapshot()
                .unwrap()
                .assignments[&GROUP]
                .epoch,
            1
        );
    }
    fixture.stop().await;
}

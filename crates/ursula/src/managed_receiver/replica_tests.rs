//! Native HTTP receiver tests with actual RF3/RF5 data and meta consensus. Each test
//! uses fenced membership endpoints; executor cases use actual admin listeners.
//! Joint faults stop a native future; HTTP-state replacement is not OS restart.

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
use ursula_control::CompletedMembershipMutation;
use ursula_control::CompletedReceiverMutation;
use ursula_control::ControlCommand;
use ursula_control::ControlResponse;
use ursula_control::FinalMembershipEvidence;
use ursula_control::MembershipLogId;
use ursula_control::MembershipOutcome;
use ursula_control::MembershipStep;
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

async fn submit_operation(
    fixture: &Fixture,
    request: &crate::managed_operations::OperationRequest,
) -> u64 {
    // Submit through a follower's actual admin listener, then drop the caller.
    let response = reqwest::Client::new()
        .post(format!(
            "{}/__ursula/control/operations",
            fixture.recipe.nodes[&2].admin_url
        ))
        .json(request)
        .send()
        .await
        .unwrap();
    let status = response.status();
    let bytes = response.bytes().await.unwrap();
    assert_eq!(
        status,
        StatusCode::ACCEPTED,
        "{}",
        String::from_utf8_lossy(&bytes)
    );
    let ControlResponse::MigrationStarted { migration_id } =
        serde_json::from_slice(&bytes).unwrap()
    else {
        panic!("missing operation ID")
    };
    migration_id
}

async fn wait_operation(fixture: &Fixture, id: u64) {
    tokio::time::timeout(Duration::from_secs(40), async {
        loop {
            let view = fixture.fresh_projection().await;
            if !view.state.migrations[&id].is_running() {
                assert_eq!(
                    view.state.migrations[&id].phase,
                    ursula_control::MigrationPhase::Succeeded
                );
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!(
            "executor timed out: {:?}",
            fixture
                .receivers
                .iter()
                .map(|r| r.store.snapshot().unwrap())
                .collect::<Vec<_>>()
        )
    });
}

fn executors(fixture: &Fixture) -> Vec<tokio::task::JoinHandle<()>> {
    fixture
        .recipe
        .initial_meta_voters
        .iter()
        .map(|id| {
            tokio::spawn(crate::managed_operations::run(
                fixture.states[*id as usize - 1].clone(),
                Duration::from_millis(20),
            ))
        })
        .collect()
}

async fn stop_executors(tasks: Vec<tokio::task::JoinHandle<()>>) {
    for task in &tasks {
        task.abort();
    }
    for task in tasks {
        let _ = task.await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_operation_api_and_server_executor_resume_rf3_rf5_replacement() {
    for count in [3, 5] {
        let fixture = Fixture::new(count).await;
        let target: BTreeSet<_> = (1..=count as u64 + 1).filter(|id| *id != 2).collect();
        let request = crate::managed_operations::OperationRequest {
            operation_key: format!("automatic-rf{count}"),
            raft_group_id: GROUP,
            expected_epoch: 0,
            target_voters: target.clone(),
            target_policy: None,
        };
        let stream = (0..100)
            .map(|n| BucketStreamId::new("automatic", format!("s{n}")))
            .find(|stream| fixture.states[0].runtime.locate(stream).raft_group_id == GROUP)
            .unwrap();
        let mut create = CreateStreamRequest::new(stream.clone(), "text/plain");
        create.initial_payload = b"acknowledged-before-automatic-move".to_vec().into();
        fixture.states[0]
            .runtime
            .create_stream(create)
            .await
            .unwrap();
        let id = submit_operation(&fixture, &request).await;
        let operation_nodes = fixture
            .recipe
            .nodes
            .values()
            .map(|node| ursula_ctl::NodeInfo {
                id: node.node_id,
                admin_url: node.admin_url.parse().unwrap(),
                host: "127.0.0.1".to_owned(),
                http_url: None,
                metrics_url: None,
                expected_process_incarnation: None,
                expected_maintenance_fence: None,
            })
            .collect::<Vec<_>>();
        let operation_client =
            ursula_ctl::operations::OperationClient::new(Duration::from_secs(2)).unwrap();
        // The CLI client observes accepted intent before any server task starts.
        assert!(
            operation_client
                .wait(
                    &operation_nodes,
                    id,
                    Duration::from_millis(30),
                    Duration::from_millis(5)
                )
                .await
                .is_err()
        );
        assert!(
            operation_client
                .status(&operation_nodes, id)
                .await
                .unwrap()
                .is_running()
        );
        assert!(
            operation_client
                .wait(
                    &operation_nodes,
                    999,
                    Duration::from_secs(10),
                    Duration::from_millis(5)
                )
                .await
                .unwrap_err()
                .to_string()
                .contains("404")
        );
        let executor = executors(&fixture);
        // Lose the executor after receiver activation has become durable.
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let view = fixture.fresh_projection().await;
                if view.state.migrations[&id]
                    .managed
                    .as_ref()
                    .unwrap()
                    .receiver_activation_authorized
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        stop_executors(executor).await;
        let before_takeover = fixture.fresh_projection().await;
        let old_token = before_takeover.state.migrations[&id]
            .managed
            .as_ref()
            .unwrap()
            .executor
            .as_ref()
            .unwrap()
            .token
            .clone();
        let successor = if old_token.executor.node_id == 3 {
            1
        } else {
            3
        };
        fixture.meta[old_token.executor.node_id as usize - 1]
            .raft_handle()
            .trigger()
            .transfer_leader(successor)
            .await
            .unwrap();
        fixture.meta[successor as usize - 1]
            .raft_handle()
            .wait(Some(Duration::from_secs(10)))
            .current_leader(
                successor,
                "executor takeover follows actual meta leadership",
            )
            .await
            .unwrap();
        assert_eq!(submit_operation(&fixture, &request).await, id);
        let executor = executors(&fixture);
        wait_operation(&fixture, id).await;
        stop_executors(executor).await;
        assert_eq!(
            operation_client
                .wait(
                    &operation_nodes,
                    id,
                    Duration::from_secs(2),
                    Duration::from_millis(10)
                )
                .await
                .unwrap()
                .phase,
            ursula_control::MigrationPhase::Succeeded
        );
        assert_eq!(
            operation_client
                .submit(&operation_nodes, &request)
                .await
                .unwrap(),
            id
        );
        operation_client.list(&operation_nodes).await.unwrap();
        assert_eq!(submit_operation(&fixture, &request).await, id);
        let view = fixture.fresh_projection().await;
        assert!(
            view.state.migrations[&id]
                .managed
                .as_ref()
                .unwrap()
                .executor
                .as_ref()
                .unwrap()
                .token
                .generation
                > old_token.generation
        );
        assert_eq!(view.state.placements[&GROUP].voters, target);
        assert_eq!(view.state.placements[&GROUP].epoch, 1);
        assert!(view.state.placements[&GROUP].draining.is_empty());
        assert!(
            fixture.states[1]
                .raft_registry()
                .unwrap()
                .get(GROUP)
                .is_none()
        );
        assert!(
            fixture.states[1]
                .raft_registry()
                .unwrap()
                .get(RaftGroupId(1))
                .is_some()
        );
        let moved = fixture.states[count]
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
        assert_eq!(moved.payload, b"acknowledged-before-automatic-move");
        let mut conflicting = request.clone();
        conflicting.expected_epoch = 1;
        assert_eq!(
            reqwest::Client::new()
                .post(format!(
                    "{}/__ursula/control/operations",
                    fixture.recipe.nodes[&1].admin_url
                ))
                .json(&conflicting)
                .send()
                .await
                .unwrap()
                .status(),
            StatusCode::CONFLICT
        );
        fixture.stop().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_server_executor_changes_rf3_to_rf5_to_rf3_and_hands_off_removed_leader() {
    let fixture = Fixture::new_with_nodes(3, 6).await;
    let executor = executors(&fixture);
    for (index, (rf, target)) in [
        (5, BTreeSet::from([1, 2, 3, 4, 5])),
        (3, BTreeSet::from([2, 4, 6])),
    ]
    .into_iter()
    .enumerate()
    {
        let request = crate::managed_operations::OperationRequest {
            operation_key: format!("automatic-policy-{index}"),
            raft_group_id: GROUP,
            expected_epoch: index as u64,
            target_voters: target.clone(),
            target_policy: Some(ursula_control::GroupPlacementPolicy {
                replication_factor: ursula_control::ReplicationFactor::try_from(rf).unwrap(),
                ..Default::default()
            }),
        };
        let id = submit_operation(&fixture, &request).await;
        wait_operation(&fixture, id).await;
        let view = fixture.fresh_projection().await;
        assert_eq!(view.state.placements[&GROUP].voters, target);
        assert_eq!(
            view.state.managed_placement.as_ref().unwrap().groups[&GROUP].replication_factor,
            ursula_control::ReplicationFactor::try_from(rf).unwrap()
        );
        let leader = *target.first().unwrap();
        let configuration = ursula_raft::confirm_group_configuration(
            GROUP,
            leader,
            &fixture.recipe.nodes[&leader].cluster_url,
            Duration::from_secs(2),
        )
        .await
        .unwrap();
        assert_eq!(configuration.voter_sets, vec![target]);
        assert!(configuration.learners.is_empty());
    }
    stop_executors(executor).await;
    for id in [1, 3, 5] {
        assert!(
            fixture.states[id - 1]
                .raft_registry()
                .unwrap()
                .get(GROUP)
                .is_none()
        );
    }
    fixture.stop().await;
}

#[derive(Debug, Default)]
struct PruningProbe {
    calls: std::sync::Mutex<Vec<(u32, BTreeSet<u64>, bool)>>,
    block: std::sync::atomic::AtomicBool,
    entered: tokio::sync::Notify,
    release: tokio::sync::Notify,
}

impl ursula_runtime::SnapshotStore for PruningProbe {
    fn upload<'a>(
        &'a self,
        key: ursula_runtime::SnapshotKey,
        bytes: axum::body::Bytes,
    ) -> ursula_runtime::SnapshotStoreFuture<'a, ursula_runtime::SnapshotLocation> {
        Box::pin(async move {
            ursula_runtime::default_snapshot_store()
                .upload(key, bytes)
                .await
        })
    }

    fn download<'a>(
        &'a self,
        location: &'a ursula_runtime::SnapshotLocation,
    ) -> ursula_runtime::SnapshotStoreFuture<'a, Vec<u8>> {
        Box::pin(async move {
            ursula_runtime::default_snapshot_store()
                .download(location)
                .await
        })
    }

    fn delete<'a>(
        &'a self,
        location: &'a ursula_runtime::SnapshotLocation,
    ) -> ursula_runtime::SnapshotStoreFuture<'a, ()> {
        Box::pin(async move {
            ursula_runtime::default_snapshot_store()
                .delete(location)
                .await
        })
    }

    fn configure_pruning<'a>(
        &'a self,
        group: u32,
        voters: BTreeSet<u64>,
        enabled: bool,
    ) -> ursula_runtime::SnapshotStoreFuture<'a, ()> {
        Box::pin(async move {
            self.calls.lock().unwrap().push((group, voters, enabled));
            if !enabled && self.block.swap(false, std::sync::atomic::Ordering::SeqCst) {
                self.entered.notify_one();
                self.release.notified().await;
            }
            Ok(())
        })
    }
}

#[tokio::test]
async fn native_receiver_pruning_barrier_survives_executor_takeover_rf3_rf5() {
    for count in [3, 5] {
        let fixture = Fixture::new(count).await;
        let probe = Arc::new(PruningProbe::default());
        probe.block.store(true, std::sync::atomic::Ordering::SeqCst);
        fixture.states[0]
            .raft_registry()
            .unwrap()
            .set_snapshot_store(Some(probe.clone()));
        let target: BTreeSet<_> = (1..=count as u64 + 1).filter(|id| *id != 2).collect();
        let request = crate::managed_operations::OperationRequest {
            operation_key: format!("pruning-barrier-rf{count}"),
            raft_group_id: GROUP,
            expected_epoch: 0,
            target_voters: target.clone(),
            target_policy: None,
        };
        let id = submit_operation(&fixture, &request).await;
        let executor = executors(&fixture);
        tokio::time::timeout(Duration::from_secs(10), probe.entered.notified())
            .await
            .unwrap();
        assert_eq!(
            fixture.receivers[0]
                .store
                .snapshot()
                .unwrap()
                .fence
                .unwrap()
                .phase,
            ursula_control::ReceiverFencePhase::Activating
        );
        let view = fixture.fresh_projection().await;
        let managed = view.state.migrations[&id].managed.as_ref().unwrap();
        assert!(
            managed.receivers.is_empty(),
            "GC drain must precede receiving-process certification"
        );
        assert!(managed.prepared.is_empty());
        let old = managed.executor.as_ref().unwrap().token.clone();
        stop_executors(executor).await;
        let successor = if old.executor.node_id == 3 { 1 } else { 3 };
        fixture.meta[old.executor.node_id as usize - 1]
            .raft_handle()
            .trigger()
            .transfer_leader(successor)
            .await
            .unwrap();
        fixture.meta[successor as usize - 1]
            .raft_handle()
            .wait(Some(Duration::from_secs(10)))
            .current_leader(successor, "take over while source GC drain is blocked")
            .await
            .unwrap();
        let executor = executors(&fixture);
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if fixture.fresh_projection().await.state.migrations[&id]
                    .managed
                    .as_ref()
                    .unwrap()
                    .executor
                    .as_ref()
                    .unwrap()
                    .token
                    .generation
                    > old.generation
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        assert_eq!(
            fixture.receivers[0]
                .store
                .snapshot()
                .unwrap()
                .fence
                .unwrap()
                .phase,
            ursula_control::ReceiverFencePhase::Activating
        );
        probe.release.notify_one();
        wait_operation(&fixture, id).await;
        stop_executors(executor).await;
        assert_eq!(
            fixture.fresh_projection().await.state.placements[&GROUP].voters,
            target
        );
        {
            let calls = probe.calls.lock().unwrap();
            assert!(
                calls
                    .iter()
                    .any(|(group, voters, enabled)| *group == GROUP.0
                        && !enabled
                        && voters.len() == count + 1)
            );
            assert!(
                calls
                    .iter()
                    .any(|(group, voters, enabled)| *group == GROUP.0
                        && *enabled
                        && voters == &target)
            );
        }
        assert_eq!(
            fixture.receivers[0]
                .store
                .snapshot()
                .unwrap()
                .fence
                .unwrap()
                .phase,
            ursula_control::ReceiverFencePhase::Retired
        );
        probe.calls.lock().unwrap().clear();
        let settled = fixture.fresh_projection().await;
        fixture.receivers[0]
            .sync_snapshot_pruning(&fixture.states[0], &settled)
            .await
            .unwrap();
        {
            let calls = probe.calls.lock().unwrap();
            assert!(
                calls
                    .iter()
                    .any(|(group, voters, enabled)| *group == GROUP.0
                        && *enabled
                        && voters == &target)
            );
            assert!(
                calls.iter().any(|(group, voters, enabled)| *group == 1
                    && *enabled
                    && voters.len() == count)
            );
        }
        probe.calls.lock().unwrap().clear();
        fixture.receivers[0]
            .sync_snapshot_pruning(&fixture.states[0], &view)
            .await
            .unwrap();
        assert!(
            probe.calls.lock().unwrap().is_empty(),
            "an older complete view cannot roll pruning policy back"
        );
        let mut inventory = serde_json::to_value(crate::managed_receiver::ReceiverInventory {
            protocol_version: crate::managed_receiver::RECEIVER_PROTOCOL_VERSION,
            identity: fixture.receivers[0].store.identity().clone(),
            process: fixture.states[0].process_incarnation.clone(),
        })
        .unwrap();
        inventory
            .as_object_mut()
            .unwrap()
            .remove("protocol_version");
        let missing =
            serde_json::from_value::<crate::managed_receiver::ReceiverInventory>(inventory)
                .unwrap_err();
        assert!(missing.to_string().contains("protocol_version"));
    }
}

struct Fixture {
    root: tempfile::TempDir,
    recipe: ClusterBootstrap,
    states: Vec<HttpState>,
    receivers: Vec<Arc<ManagedReceiver>>,
    meta: Vec<MetaRaftHandle>,
    servers: Vec<tokio::task::JoinHandle<()>>,
}

impl Fixture {
    async fn fresh_projection(&self) -> ursula_control::ControlProjection {
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                for handle in &self.meta {
                    if let Ok(view) = handle.read_projection(Duration::from_secs(1)).await {
                        return view;
                    }
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("fixture has no fresh meta quorum")
    }
    async fn new(replicas: usize) -> Self {
        Self::new_with_nodes(replicas, replicas + 1).await
    }

    async fn new_with_nodes(replicas: usize, total: usize) -> Self {
        let root = tempfile::tempdir().unwrap();
        let identity = ClusterIdentity {
            cluster_id: ClusterId::try_from("replica-http".to_owned()).unwrap(),
            group_count: 2,
            core_count: 1,
            routing_hash: RoutingHashVersion::Fnv1a64BucketSlashStreamV1,
        };
        let mut listeners = Vec::new();
        let mut admin_listeners = Vec::new();
        let mut nodes = BTreeMap::new();
        for id in 1..=total as u64 {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let admin = TcpListener::bind("127.0.0.1:0").await.unwrap();
            nodes.insert(id, NodeRegistration {
                node_id: id,
                client_url: format!("http://node{id}:4437"),
                admin_url: format!("http://{}", admin.local_addr().unwrap()),
                cluster_url: format!("http://{}", listener.local_addr().unwrap()),
                labels: BTreeMap::from([(
                    "zone".to_owned(),
                    if total > replicas + 1 {
                        ((id - 1) % 3 + 1).to_string()
                    } else if id == replicas as u64 + 1 {
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
            admin_listeners.push(admin);
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
                        election_timeout_min: 500,
                        election_timeout_max: 1000,
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
                ursula_raft::META_RAFT_WRITE_CONTROL_PATH,
            ] {
                router = router.route_service(path, meta_raft_grpc_service(service.clone()));
            }
            servers.push(tokio::spawn(async move {
                axum::serve(listener, router).await.unwrap();
            }));
            let admin = admin_listeners.remove(0);
            let admin_router = crate::admin_router(state.clone());
            servers.push(tokio::spawn(async move {
                axum::serve(admin, admin_router).await.unwrap();
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
    replica_scenario(3, false, false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replica_http_reconciles_release_after_new_generation_and_replaced_process() {
    replica_scenario(3, true, false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rf5_replica_http_preserves_acked_prefix_and_replays_release_after_cancellation() {
    replica_scenario(5, false, false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rf5_replica_http_reconciles_release_across_generation_and_process_replacement() {
    replica_scenario(5, true, false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn membership_http_recovers_an_actual_committed_joint_configuration_without_reverting_source()
{
    replica_scenario(3, false, true).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rf5_membership_http_recovers_committed_joint_configuration() {
    replica_scenario(5, false, true).await;
}

async fn replica_scenario(replicas: usize, recover_release: bool, recover_membership: bool) {
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
    let learner = ReplicaRequest {
        token: token.clone(),
        raft_group_id: GROUP,
        request_id: "add-learner".to_owned(),
        operation: ReceiverMutationKind::ManagedMembership {
            step: MembershipStep::AddLearner {
                epoch: 0,
                node_id: added_id,
                prefix: prefix.clone(),
            },
        },
    };
    let (status, learner_reply) = mutate(&fixture.states[0], &learner, "membership").await;
    assert_eq!(
        status,
        StatusCode::OK,
        "{}",
        String::from_utf8_lossy(&learner_reply)
    );
    assert_eq!(
        mutate(&fixture.states[0], &learner, "membership").await,
        (StatusCode::OK, learner_reply.clone())
    );
    let learner_receipt: CompletedMembershipMutation =
        serde_json::from_slice(&learner_reply).unwrap();
    assert_eq!(learner_receipt.outcome, MembershipOutcome::Applied);
    assert!(learner_receipt.configuration.learners.contains(&added_id));
    new_replica
        .wait(Some(Duration::from_secs(10)))
        .applied_index_at_least(Some(prefix.index), "learner prefix")
        .await
        .unwrap();
    fixture
        .update(&token, MigrationUpdate::RecordLearner {
            evidence: applied_http(&fixture.states[added], &token, &prefix).await,
        })
        .await;
    fixture
        .update(&token, MigrationUpdate::AuthorizeMembership)
        .await;
    let mut change = ReplicaRequest {
        token: token.clone(),
        raft_group_id: GROUP,
        request_id: "change-voters".to_owned(),
        operation: ReceiverMutationKind::ManagedMembership {
            step: MembershipStep::ChangeVoters {
                epoch: 0,
                target_voters: target_voters.clone(),
            },
        },
    };
    if recover_membership {
        // Fault boundary: checkpoint before queueing; stop polling the public
        // OpenRaft future after its first submission, so the joint configuration
        // commits but its second uniform submission never runs.
        let mut ledger = fixture.receivers[0].store.snapshot().unwrap();
        ledger.pending = Some(PendingReceiverMutation {
            token: token.clone(),
            raft_group_id: GROUP,
            request_id: change.request_id.clone(),
            process: fixture.states[0].process_incarnation.clone(),
            operation: change.operation.clone(),
        });
        fixture.receivers[0].store.persist(ledger).await.unwrap();
        let mut interrupted = Box::pin(leader.change_membership(target_voters.clone(), false));
        std::future::poll_fn(|cx| {
            assert!(std::future::Future::poll(interrupted.as_mut(), cx).is_pending());
            std::task::Poll::Ready(())
        })
        .await;
        leader
            .wait(Some(Duration::from_secs(10)))
            .metrics(
                |metrics| {
                    metrics
                        .membership_config
                        .membership()
                        .get_joint_config()
                        .len()
                        == 2
                        && metrics.last_applied.map(|log| log.index())
                            >= metrics.membership_config.log_id().map(|log| log.index())
                },
                "actual committed joint",
            )
            .await
            .unwrap();
        drop(interrupted);
        let joint = ursula_raft::confirm_group_configuration(
            GROUP,
            1,
            &fixture.recipe.nodes[&1].cluster_url,
            Duration::from_secs(5),
        )
        .await
        .unwrap();
        assert_eq!(joint.voter_sets, vec![
            fixture.recipe.voters[&GROUP].clone(),
            target_voters.clone()
        ]);
        assert!(joint.uniform_membership().is_err());
        assert!(
            ursula_raft::confirm_group_membership(
                GROUP,
                1,
                &fixture.recipe.nodes[&1].cluster_url,
                Duration::from_secs(5)
            )
            .await
            .is_err()
        );
        let previous = fixture.states[0].clone();
        fixture.states[0] = HttpState::with_raft_registry(
            previous.runtime.clone(),
            previous.raft_registry().unwrap().clone(),
        )
        .with_configured_node_id(1)
        .with_managed_projection(previous.managed_projection.clone().unwrap())
        .with_managed_receiver(fixture.receivers[0].clone());
        token = fixture.claim(token.generation).await;
        fixture.certify(&token).await;
        let restored = fixture.receivers[0].store.snapshot().unwrap();
        let receipt = &restored.membership_completed[&change.request_id];
        assert_eq!(receipt.outcome, MembershipOutcome::Reconciled);
        assert_eq!(receipt.configuration.voter_sets, joint.voter_sets);
        assert_eq!(receipt.request.token, token);
        let mut prepared = prepare.clone();
        prepared.token = token.clone();
        assert_eq!(
            mutate(&fixture.states[added], &prepared, "prepare").await.0,
            StatusCode::OK
        );
        fixture
            .update(&token, MigrationUpdate::RecordPrepared {
                process: fixture.process(added),
            })
            .await;
        fixture
            .update(&token, MigrationUpdate::RecordLearner {
                evidence: applied_http(&fixture.states[added], &token, &prefix).await,
            })
            .await;
        fixture
            .update(&token, MigrationUpdate::AuthorizeMembership)
            .await;
        change.token = token.clone();
        let (status, recovered) = mutate(&fixture.states[0], &change, "membership").await;
        assert_eq!(
            status,
            StatusCode::OK,
            "{}",
            String::from_utf8_lossy(&recovered)
        );
        assert_eq!(
            serde_json::from_slice::<CompletedMembershipMutation>(&recovered)
                .unwrap()
                .outcome,
            MembershipOutcome::Reconciled
        );
        change.request_id = "change-voters-resume".to_owned();
    }
    let (status, membership_reply) = mutate(&fixture.states[0], &change, "membership").await;
    assert_eq!(
        status,
        StatusCode::OK,
        "{}",
        String::from_utf8_lossy(&membership_reply)
    );
    assert_eq!(
        mutate(&fixture.states[0], &change, "membership").await,
        (StatusCode::OK, membership_reply.clone())
    );
    let changed: CompletedMembershipMutation = serde_json::from_slice(&membership_reply).unwrap();
    assert_eq!(changed.outcome, MembershipOutcome::Applied);
    assert_eq!(
        changed.configuration.uniform_membership().unwrap().voters,
        target_voters
    );
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
    let mut replicas = BTreeMap::new();
    for index in target_indices.iter().copied() {
        let evidence = applied_http(&fixture.states[index], &token, &committed_prefix).await;
        replicas.insert(index as u64 + 1, evidence);
    }
    fixture
        .update(&token, MigrationUpdate::VerifyMembership {
            evidence: FinalMembershipEvidence {
                membership: target.membership.clone(),
                committed_prefix,
                replicas,
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

async fn applied_http(
    state: &HttpState,
    token: &MigrationToken,
    prefix: &MembershipLogId,
) -> ReplicaAppliedEvidence {
    let response = crate::admin_router(state.clone())
        .oneshot(request(
            state,
            "applied",
            &super::super::membership::AppliedRequest {
                token: token.clone(),
                raft_group_id: GROUP,
                prefix: prefix.clone(),
            },
        ))
        .await
        .unwrap();
    let status = response.status();
    let body = to_bytes(response.into_body(), 1 << 20).await.unwrap();
    assert_eq!(status, StatusCode::OK, "{}", String::from_utf8_lossy(&body));
    serde_json::from_slice(&body).unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn membership_http_handoff_uses_fresh_data_quorum_and_replays_the_same_receipt() {
    for count in [3, 5] {
        let fixture = Fixture::new(count).await;
        let source = ursula_raft::confirm_group_membership(
            GROUP,
            1,
            &fixture.recipe.nodes[&1].cluster_url,
            Duration::from_secs(5),
        )
        .await
        .unwrap();
        let target: BTreeSet<_> = (1..=count as u64 + 1).filter(|id| *id != 2).collect();
        assert_eq!(
            fixture.meta[0]
                .write(ControlCommand::SubmitMigration {
                    request: MigrationRequest {
                        operation_key: "handoff-step".to_owned(),
                        raft_group_id: GROUP,
                        expected_epoch: 0,
                        source_membership: source.membership,
                        target_voters: target,
                        target_policy: None,
                    },
                    now_ms: 2,
                })
                .await
                .unwrap(),
            ControlResponse::MigrationStarted { migration_id: 1 }
        );
        let token = fixture.claim(0).await;
        fixture
            .update(&token, MigrationUpdate::AuthorizeReceivers)
            .await;
        fixture.certify(&token).await;
        let mut handoff = ReplicaRequest {
            token: token.clone(),
            raft_group_id: GROUP,
            request_id: "handoff-to-retained-voter".to_owned(),
            operation: ReceiverMutationKind::ManagedMembership {
                step: MembershipStep::TransferLeader {
                    epoch: 0,
                    node_id: 3,
                },
            },
        };
        let mut wrong = handoff.clone();
        wrong.operation = ReceiverMutationKind::ManagedMembership {
            step: MembershipStep::TransferLeader {
                epoch: 0,
                node_id: count as u64 + 1,
            },
        };
        assert_eq!(
            mutate(&fixture.states[0], &wrong, "membership").await.0,
            StatusCode::CONFLICT
        );
        assert!(
            fixture.receivers[0]
                .store
                .snapshot()
                .unwrap()
                .pending
                .is_none()
        );
        let (status, reply) = mutate(&fixture.states[0], &handoff, "membership").await;
        assert_eq!(
            status,
            StatusCode::OK,
            "{}",
            String::from_utf8_lossy(&reply)
        );
        let receipt: CompletedMembershipMutation = serde_json::from_slice(&reply).unwrap();
        assert_eq!(receipt.outcome, MembershipOutcome::Applied);
        assert_eq!(receipt.configuration.leader_id, 3);
        assert_eq!(receipt.configuration.voter_sets, vec![
            fixture.recipe.voters[&GROUP].clone()
        ]);
        assert_eq!(
            mutate(&fixture.states[0], &handoff, "membership").await,
            (StatusCode::OK, reply)
        );
        handoff.operation = ReceiverMutationKind::ManagedMembership {
            step: MembershipStep::TransferLeader {
                epoch: 0,
                node_id: 1,
            },
        };
        assert_eq!(
            mutate(&fixture.states[0], &handoff, "membership").await.0,
            StatusCode::CONFLICT
        );
        fixture.stop().await;
    }
}

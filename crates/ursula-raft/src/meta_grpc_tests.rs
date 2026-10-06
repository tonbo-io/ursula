use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use openraft::BasicNode;
use openraft::Config;
use openraft::ReadPolicy;
use openraft::ServerState;
use openraft::alias::LogIdOf;
use openraft::rt::WatchReceiver;
use openraft::storage::RaftStateMachine;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::TcpListenerStream;
use ursula_control::ClusterBootstrap;
use ursula_control::ClusterId;
use ursula_control::ClusterIdentity;
use ursula_control::ControlCommand;
use ursula_control::ControlPlaneState;
use ursula_control::ControlResponse;
use ursula_control::GroupPolicyOverride;
use ursula_control::MembershipLogId;
use ursula_control::MetaLocalIdentity;
use ursula_control::NodeRegistration;
use ursula_control::NodeState;
use ursula_control::PlacementPolicy;
use ursula_control::ReplicationFactor;
use ursula_control::RoutingHashVersion;
use ursula_control::VerifiedGroupMembership;
use ursula_shard::RaftGroupId;

use crate::meta::MetaRaftHandle;
use crate::meta::MetaRaftTypeConfig;
use crate::meta_grpc::META_RAFT_PROTOCOL_VERSION;
use crate::meta_grpc::MetaGrpcRaftNetworkFactory;
use crate::meta_grpc::MetaRaftGrpcService;
use crate::meta_grpc::meta_raft_grpc_service;
use crate::raft_internal_proto::MetaRaftRpcEnvelopeV1;
use crate::raft_internal_proto::MetaRaftSnapshotRequestV1;
use crate::raft_internal_proto::meta_raft_internal_client::MetaRaftInternalClient;

const CLUSTER: &str = "meta-transport-recovery-test";
const DEADLINE: Duration = Duration::from_secs(10);

struct TestNode {
    handle: MetaRaftHandle,
    server: JoinHandle<()>,
}

async fn start(id: u64, listener: TcpListener, path: &Path) -> TestNode {
    let config = Arc::new(
        Config {
            heartbeat_interval: 50,
            election_timeout_min: 300,
            election_timeout_max: 600,
            max_in_snapshot_log_to_keep: 0,
            ..Config::default()
        }
        .validate()
        .unwrap(),
    );
    let handle = MetaRaftHandle::new_durable_node_with_network(
        id,
        config,
        MetaGrpcRaftNetworkFactory::new(CLUSTER).unwrap(),
        path,
    )
    .await
    .unwrap();
    let service = MetaRaftGrpcService::new(CLUSTER, &handle).unwrap();
    let server = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(meta_raft_grpc_service(service))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    });
    TestNode { handle, server }
}

async fn stop(node: TestNode) {
    node.handle.shutdown().await.unwrap();
    node.server.abort();
    let _ = node.server.await;
    drop(node.handle);
}

fn bound_identity(id: u64, endpoint: String) -> MetaLocalIdentity {
    MetaLocalIdentity {
        cluster: ClusterIdentity {
            cluster_id: ClusterId::try_from(CLUSTER.to_owned()).unwrap(),
            group_count: 1,
            core_count: 1,
            routing_hash: RoutingHashVersion::Fnv1a64BucketSlashStreamV1,
        },
        node: NodeRegistration {
            node_id: id,
            client_url: format!("http://client{id}:4437"),
            cluster_url: endpoint,
            admin_url: format!("http://admin{id}:4438"),
            labels: BTreeMap::from([("zone".to_owned(), ((id - 1) % 3).to_string())]),
        },
    }
}

async fn start_bound(identity: MetaLocalIdentity, listener: TcpListener, path: &Path) -> TestNode {
    let config = Arc::new(
        Config {
            heartbeat_interval: 50,
            election_timeout_min: 300,
            election_timeout_max: 600,
            max_in_snapshot_log_to_keep: 0,
            ..Config::default()
        }
        .validate()
        .unwrap(),
    );
    let factory = MetaGrpcRaftNetworkFactory::new_bound(identity.cluster.clone()).unwrap();
    let handle =
        MetaRaftHandle::new_bound_durable_node_with_network(identity, config, factory, path)
            .await
            .unwrap();
    let service = MetaRaftGrpcService::new_bound(&handle).unwrap();
    let server = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(meta_raft_grpc_service(service))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    });
    TestNode { handle, server }
}

#[tokio::test]
async fn bound_meta_transport_rejects_routing_drift_and_missing_binding_before_decode() {
    let dir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let node = start_bound(
        bound_identity(7, endpoint.clone()),
        listener,
        &dir.path().join("meta.wal"),
    )
    .await;
    assert!(MetaRaftGrpcService::new("other-cluster", &node.handle).is_err());
    let mut client = MetaRaftInternalClient::connect(endpoint).await.unwrap();
    for (groups, cores, hash) in [(2, 1, 1), (1, 2, 1), (1, 1, 2), (0, 0, 0)] {
        let envelope = MetaRaftRpcEnvelopeV1 {
            cluster_id: CLUSTER.to_owned(),
            target_node_id: 7,
            protocol_version: META_RAFT_PROTOCOL_VERSION,
            group_count: groups,
            core_count: cores,
            routing_hash_version: hash,
            payload: vec![0xc1].into(),
        };
        assert_eq!(
            client.append(envelope.clone()).await.unwrap_err().code(),
            tonic::Code::FailedPrecondition
        );
        assert_eq!(
            client.vote(envelope.clone()).await.unwrap_err().code(),
            tonic::Code::FailedPrecondition
        );
        assert_eq!(
            client
                .transfer_leader(envelope.clone())
                .await
                .unwrap_err()
                .code(),
            tonic::Code::FailedPrecondition
        );
        assert_eq!(
            client.status(envelope.clone()).await.unwrap_err().code(),
            tonic::Code::FailedPrecondition
        );
        assert_eq!(
            client
                .read_bootstrap_state(envelope.clone())
                .await
                .unwrap_err()
                .code(),
            tonic::Code::FailedPrecondition
        );
        assert_eq!(
            client
                .read_projection(envelope.clone())
                .await
                .unwrap_err()
                .code(),
            tonic::Code::FailedPrecondition
        );
        assert_eq!(
            client
                .full_snapshot(MetaRaftSnapshotRequestV1 {
                    cluster_id: CLUSTER.to_owned(),
                    target_node_id: 7,
                    protocol_version: META_RAFT_PROTOCOL_VERSION,
                    group_count: groups,
                    core_count: cores,
                    routing_hash_version: hash,
                    vote: vec![0xc1].into(),
                    snapshot_meta: vec![0xc1].into(),
                    snapshot_payload: vec![0xc1].into()
                })
                .await
                .unwrap_err()
                .code(),
            tonic::Code::FailedPrecondition
        );
    }
    let status = crate::read_meta_replica_status(
        &bound_identity(7, String::new()).cluster,
        7,
        &node.handle.local_identity().unwrap().node.cluster_url,
        Duration::from_secs(1),
    )
    .await
    .unwrap();
    assert!(!status.initialized);
    assert!(status.bootstrap_recipe.is_none());
    assert!(status.bootstrap_node_id.is_none());
    assert!(!node.handle.raft_handle().is_initialized().await.unwrap());
    assert!(
        node.handle
            .read_state(|state| state.cluster_bootstrap.is_none())
            .await
            .unwrap()
    );
    stop(node).await;
}

#[tokio::test]
async fn bound_meta_bootstrap_records_replicate_and_survive_compaction_and_full_restart() {
    let dir = tempfile::tempdir().unwrap();
    let mut identities = Vec::new();
    let mut addresses = Vec::new();
    let mut nodes = Vec::new();
    for id in 1..=3 {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let identity = bound_identity(id, format!("http://{address}"));
        nodes.push(Some(
            start_bound(
                identity.clone(),
                listener,
                &dir.path().join(format!("node{id}.wal")),
            )
            .await,
        ));
        identities.push(identity);
        addresses.push(address);
    }
    let meta_membership = identities
        .iter()
        .map(|identity| {
            (
                identity.node.node_id,
                BasicNode::new(&identity.node.cluster_url),
            )
        })
        .collect();
    nodes[0]
        .as_ref()
        .unwrap()
        .handle
        .initialize_membership(meta_membership)
        .await
        .unwrap();
    let elected = leader(&nodes).await;
    let handle = &nodes[elected].as_ref().unwrap().handle;
    let bootstrap = ClusterBootstrap {
        identity: identities[0].cluster.clone(),
        initial_meta_voters: BTreeSet::from([1, 2, 3]),
        nodes: identities
            .iter()
            .map(|identity| (identity.node.node_id, identity.node.clone()))
            .collect(),
        voters: BTreeMap::from([(RaftGroupId(0), BTreeSet::from([1, 2, 3]))]),
        placement: PlacementPolicy::default(),
    };
    // Synthetic data-membership certificate: this test exercises consensus,
    // persistence and bootstrap guards, not data-group discovery or execution.
    let memberships = BTreeMap::from([(RaftGroupId(0), VerifiedGroupMembership {
        voters: BTreeSet::from([1, 2, 3]),
        learners: BTreeSet::new(),
        log_id: MembershipLogId {
            term: 1,
            node_id: 1,
            index: 0,
        },
    })]);
    let mut wrong_meta = bootstrap.clone();
    wrong_meta.initial_meta_voters.insert(4);
    assert!(
        handle
            .write(ControlCommand::BootstrapCluster {
                bootstrap: wrong_meta,
                memberships: memberships.clone(),
                now_ms: 1
            })
            .await
            .unwrap()
            .is_rejected()
    );
    let mut wrong_endpoint = bootstrap.clone();
    wrong_endpoint.nodes.get_mut(&1).unwrap().cluster_url = "http://other-meta:4439".to_owned();
    assert!(
        handle
            .write(ControlCommand::BootstrapCluster {
                bootstrap: wrong_endpoint,
                memberships: memberships.clone(),
                now_ms: 1
            })
            .await
            .unwrap()
            .is_rejected()
    );
    let mut wrong_contract = bootstrap.clone();
    wrong_contract.identity.core_count = 2;
    assert!(
        handle
            .raft_handle()
            .client_write(ControlCommand::BootstrapCluster {
                bootstrap: wrong_contract,
                memberships: memberships.clone(),
                now_ms: 1
            })
            .await
            .unwrap()
            .data
            .is_rejected()
    );
    assert_eq!(
        handle
            .write(ControlCommand::BootstrapCluster {
                bootstrap: bootstrap.clone(),
                memberships: memberships.clone(),
                now_ms: 1
            })
            .await
            .unwrap(),
        ControlResponse::Ok
    );
    assert_eq!(
        handle
            .write(ControlCommand::SetNodeState {
                node_id: 3,
                state: NodeState::Draining,
                now_ms: 2
            })
            .await
            .unwrap(),
        ControlResponse::Ok
    );
    let mut wrong_identity = bootstrap.clone();
    wrong_identity.identity.core_count = 2;
    assert!(
        handle
            .write(ControlCommand::BootstrapCluster {
                bootstrap: wrong_identity,
                memberships: memberships.clone(),
                now_ms: 3
            })
            .await
            .is_err()
    );
    let prefix = applied(handle).await;
    wait_state(&nodes, prefix).await;
    let expected = handle.read_state(Clone::clone).await.unwrap();
    for node in nodes.iter().flatten() {
        snapshot_and_purge(&node.handle).await;
    }
    let source = handle.raft_handle().get_snapshot().await.unwrap().unwrap();
    let mut incompatible = expected.clone();
    incompatible
        .cluster_bootstrap
        .as_mut()
        .unwrap()
        .recipe
        .identity
        .core_count = 2;
    let bytes = serde_json::to_vec(&incompatible).unwrap();
    let mut meta = source.meta;
    let snapshot_path = dir.path().join(format!("node{}.wal.snapshot", elected + 1));
    let before = std::fs::read(&snapshot_path).unwrap();
    let result = handle
        .with_state_machine(move |machine| {
            Box::pin(async move {
                meta.last_log_id = machine.applied_log_id();
                machine
                    .install_snapshot(&meta, std::io::Cursor::new(bytes))
                    .await
            })
        })
        .await
        .unwrap();
    assert_eq!(result.unwrap_err().kind(), std::io::ErrorKind::InvalidData);
    assert_eq!(
        std::fs::read(&snapshot_path).unwrap(),
        before,
        "incompatible snapshot is rejected before durable publication"
    );
    assert_eq!(handle.read_state(Clone::clone).await.unwrap(), expected);
    for node in nodes.iter_mut() {
        stop(node.take().unwrap()).await;
    }
    for (index, identity) in identities.iter().enumerate() {
        let listener = TcpListener::bind(addresses[index]).await.unwrap();
        let node = start_bound(
            identity.clone(),
            listener,
            &dir.path()
                .join(format!("node{}.wal", identity.node.node_id)),
        )
        .await;
        assert!(node.handle.raft_handle().is_initialized().await.unwrap());
        nodes[index] = Some(node);
    }
    let elected = leader(&nodes).await;
    let handle = &nodes[elected].as_ref().unwrap().handle;
    assert_eq!(
        handle
            .write(ControlCommand::BootstrapCluster {
                bootstrap,
                memberships,
                now_ms: 4
            })
            .await
            .unwrap(),
        ControlResponse::Ok
    );
    wait_state(&nodes, applied(handle).await).await;
    for node in nodes.iter().flatten() {
        assert_eq!(
            node.handle.read_state(Clone::clone).await.unwrap(),
            expected
        );
        assert_eq!(
            node.handle.local_identity().unwrap().cluster,
            identities[0].cluster
        );
    }
    for node in nodes.into_iter().flatten() {
        stop(node).await;
    }
}

async fn leader(nodes: &[Option<TestNode>]) -> usize {
    tokio::time::timeout(DEADLINE, async {
        loop {
            for (index, node) in nodes.iter().enumerate() {
                let Some(node) = node else {
                    continue;
                };
                let raft = node.handle.raft_handle();
                if raft.metrics().borrow_watched().state == ServerState::Leader
                    && raft
                        .ensure_linearizable(ReadPolicy::ReadIndex)
                        .await
                        .is_ok()
                {
                    return index;
                }
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("a surviving meta majority elects a usable leader")
}

async fn applied(handle: &MetaRaftHandle) -> LogIdOf<MetaRaftTypeConfig> {
    handle
        .with_state_machine(|sm| Box::pin(async move { sm.applied_log_id().unwrap() }))
        .await
        .unwrap()
}

async fn snapshot_and_purge(handle: &MetaRaftHandle) -> LogIdOf<MetaRaftTypeConfig> {
    let raft = handle.raft_handle();
    let prefix = applied(handle).await;
    raft.trigger().snapshot().await.unwrap();
    // A leadership transfer may apply its new-term blank entry between the
    // prefix read and snapshot build. Require coverage, not exact equality.
    let checkpoint = raft
        .wait(Some(DEADLINE))
        .metrics(
            |metrics| {
                metrics
                    .snapshot
                    .is_some_and(|snapshot| snapshot.index >= prefix.index)
            },
            "persist covering meta checkpoint",
        )
        .await
        .unwrap()
        .snapshot
        .unwrap();
    raft.trigger().purge_log(checkpoint.index).await.unwrap();
    raft.wait(Some(DEADLINE))
        .metrics(
            |metrics| {
                metrics
                    .purged
                    .is_some_and(|purged| purged.index >= checkpoint.index)
            },
            "compact covering meta checkpoint",
        )
        .await
        .unwrap();
    checkpoint
}

async fn wait_state(nodes: &[Option<TestNode>], prefix: LogIdOf<MetaRaftTypeConfig>) {
    for node in nodes.iter().flatten() {
        node.handle
            .raft_handle()
            .wait(Some(DEADLINE))
            .applied_index_at_least(Some(prefix.index), "meta state replicated")
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn meta_transport_rejects_wrong_identity_and_version_before_payload_decode() {
    for invalid in ["", " padded", "bad/cluster", "节点", &"x".repeat(129)] {
        assert!(MetaGrpcRaftNetworkFactory::new(invalid).is_err());
    }
    let dir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let node = start(7, listener, &dir.path().join("meta.wal")).await;
    let mut client = MetaRaftInternalClient::connect(endpoint).await.unwrap();
    let before = node.handle.read_state(Clone::clone).await.unwrap();
    for (cluster_id, target, version) in [
        ("other-cluster", 7, META_RAFT_PROTOCOL_VERSION),
        (CLUSTER, 8, META_RAFT_PROTOCOL_VERSION),
        (CLUSTER, 7, 0),
    ] {
        let envelope = MetaRaftRpcEnvelopeV1 {
            cluster_id: cluster_id.to_owned(),
            target_node_id: target,
            protocol_version: version,
            payload: vec![0xc1].into(),
            ..Default::default()
        };
        assert_eq!(
            client.append(envelope.clone()).await.unwrap_err().code(),
            tonic::Code::FailedPrecondition
        );
        assert_eq!(
            client.vote(envelope.clone()).await.unwrap_err().code(),
            tonic::Code::FailedPrecondition
        );
        assert_eq!(
            client
                .transfer_leader(envelope.clone())
                .await
                .unwrap_err()
                .code(),
            tonic::Code::FailedPrecondition
        );
        assert_eq!(
            client
                .full_snapshot(MetaRaftSnapshotRequestV1 {
                    cluster_id: cluster_id.to_owned(),
                    target_node_id: target,
                    protocol_version: version,
                    vote: vec![0xc1].into(),
                    snapshot_meta: vec![0xc1].into(),
                    snapshot_payload: vec![0xc1].into(),
                    ..Default::default()
                })
                .await
                .unwrap_err()
                .code(),
            tonic::Code::FailedPrecondition
        );
    }
    assert_eq!(
        client
            .vote(MetaRaftRpcEnvelopeV1 {
                cluster_id: CLUSTER.to_owned(),
                target_node_id: 7,
                protocol_version: META_RAFT_PROTOCOL_VERSION,
                payload: vec![0xc1].into(),
                ..Default::default()
            })
            .await
            .unwrap_err()
            .code(),
        tonic::Code::InvalidArgument
    );
    assert_eq!(node.handle.read_state(Clone::clone).await.unwrap(), before);
    assert!(!node.handle.raft_handle().is_initialized().await.unwrap());
    stop(node).await;
}

#[tokio::test]
async fn meta_transport_three_and_five_voters_survive_failures_and_full_restart() {
    for count in [3, 5] {
        let dir = tempfile::tempdir().unwrap();
        let mut addresses = Vec::new();
        let mut nodes = Vec::new();
        for id in 1..=count {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            addresses.push(listener.local_addr().unwrap());
            nodes.push(Some(
                start(id, listener, &dir.path().join(format!("node{id}.wal"))).await,
            ));
        }
        let membership: BTreeMap<_, _> = addresses
            .iter()
            .enumerate()
            .map(|(index, address)| {
                (
                    index as u64 + 1,
                    BasicNode::new(format!("http://{address}")),
                )
            })
            .collect();
        // Initialize exactly once, on one bootstrap node. Peers never initialize
        // themselves, and the full-restart path below does not call initialize.
        nodes[0]
            .as_ref()
            .unwrap()
            .handle
            .initialize_membership(membership)
            .await
            .unwrap();
        let first = leader(&nodes).await;
        let handle = &nodes[first].as_ref().unwrap().handle;
        for id in 1..=5 {
            assert_eq!(
                handle
                    .write(ControlCommand::RegisterNode {
                        node_id: id,
                        client_url: format!("http://data{id}:4437"),
                        cluster_url: format!("http://data{id}:4439"),
                        labels: BTreeMap::from([("zone".to_owned(), ((id - 1) % 3).to_string())]),
                        now_ms: 1,
                    })
                    .await
                    .unwrap(),
                ControlResponse::Ok
            );
        }
        for (group, voters) in [
            (0, BTreeSet::from([1, 2, 3])),
            (1, BTreeSet::from([1, 2, 3, 4, 5])),
        ] {
            assert_eq!(
                handle
                    .write(ControlCommand::SeedPlacement {
                        raft_group_id: RaftGroupId(group),
                        voters,
                        now_ms: 2
                    })
                    .await
                    .unwrap(),
                ControlResponse::Ok
            );
        }
        assert_eq!(
            handle
                .write(ControlCommand::AdoptPlacementPolicy {
                    policy: PlacementPolicy {
                        group_overrides: vec![GroupPolicyOverride {
                            raft_group_id: RaftGroupId(1),
                            replication_factor: ReplicationFactor::Five
                        }],
                        ..PlacementPolicy::default()
                    },
                    group_count: 2,
                    now_ms: 3,
                })
                .await
                .unwrap(),
            ControlResponse::Ok
        );
        let prefix = applied(handle).await;
        wait_state(&nodes, prefix).await;
        let transfer_target = (first + 1) % count as usize;
        handle
            .raft_handle()
            .trigger()
            .transfer_leader(transfer_target as u64 + 1)
            .await
            .unwrap();
        nodes[transfer_target]
            .as_ref()
            .unwrap()
            .handle
            .wait_for_current_leader(transfer_target as u64 + 1, DEADLINE)
            .await
            .unwrap();
        let first = leader(&nodes).await;
        assert_eq!(
            first, transfer_target,
            "meta transfer-leader RPC elects the requested caught-up voter"
        );
        for node in nodes.iter().flatten() {
            snapshot_and_purge(&node.handle).await;
        }
        stop(nodes[first].take().unwrap()).await;
        if count == 5 {
            let second = nodes.iter().position(Option::is_some).unwrap();
            stop(nodes[second].take().unwrap()).await;
        }
        let next = leader(&nodes).await;
        assert_ne!(next, first);
        let handle = &nodes[next].as_ref().unwrap().handle;
        assert_eq!(
            handle
                .write(ControlCommand::RegisterNode {
                    node_id: 101,
                    client_url: "http://data101:4437".to_owned(),
                    cluster_url: "http://data101:4439".to_owned(),
                    labels: BTreeMap::from([("zone".to_owned(), "d".to_owned())]),
                    now_ms: 4,
                })
                .await
                .unwrap(),
            ControlResponse::Ok
        );
        let prefix = snapshot_and_purge(handle).await;
        // A brand-new meta learner must receive the concrete gRPC snapshot;
        // its empty log cannot replay the now-purged control prefix.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let extra_id = count + 1;
        let extra = start(
            extra_id,
            listener,
            &dir.path().join(format!("node{extra_id}.wal")),
        )
        .await;
        handle
            .raft_handle()
            .add_learner(extra_id, BasicNode::new(format!("http://{address}")), true)
            .await
            .unwrap();
        extra
            .handle
            .raft_handle()
            .wait(Some(DEADLINE))
            .metrics(
                |metrics| {
                    metrics
                        .snapshot
                        .is_some_and(|snapshot| snapshot.index >= prefix.index)
                },
                "new learner installed covering meta snapshot",
            )
            .await
            .unwrap();
        let latest = applied(handle).await;
        nodes.push(Some(extra));
        addresses.push(address);
        wait_state(&nodes, latest).await;
        let expected: ControlPlaneState = nodes[next]
            .as_ref()
            .unwrap()
            .handle
            .read_state(Clone::clone)
            .await
            .unwrap();
        for node in nodes.iter_mut() {
            if let Some(node) = node.take() {
                stop(node).await;
            }
        }
        for (index, address) in addresses.iter().enumerate() {
            let id = index as u64 + 1;
            let listener = TcpListener::bind(address).await.unwrap();
            let node = start(id, listener, &dir.path().join(format!("node{id}.wal"))).await;
            assert!(node.handle.raft_handle().is_initialized().await.unwrap());
            nodes[index] = Some(node);
        }
        let elected = leader(&nodes).await;
        let latest = applied(&nodes[elected].as_ref().unwrap().handle).await;
        wait_state(&nodes, latest).await;
        for node in nodes.iter().flatten() {
            assert_eq!(
                node.handle.read_state(Clone::clone).await.unwrap(),
                expected
            );
            let metrics = node.handle.raft_handle().metrics().borrow_watched().clone();
            assert_eq!(
                metrics.membership_config.membership().voter_ids().count(),
                count as usize
            );
        }
        for node in nodes.into_iter().flatten() {
            stop(node).await;
        }
    }
}

#[tokio::test]
async fn complete_projection_rpc_requires_bootstrap_and_a_live_meta_quorum() {
    for count in [3_u64, 5] {
        let dir = tempfile::tempdir().unwrap();
        let mut identities = Vec::new();
        let mut nodes = Vec::new();
        for id in 1..=count {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let identity = bound_identity(id, format!("http://{}", listener.local_addr().unwrap()));
            nodes.push(Some(
                start_bound(
                    identity.clone(),
                    listener,
                    &dir.path().join(format!("meta-{id}.wal")),
                )
                .await,
            ));
            identities.push(identity);
        }
        let membership = identities
            .iter()
            .map(|identity| {
                (
                    identity.node.node_id,
                    BasicNode::new(&identity.node.cluster_url),
                )
            })
            .collect::<BTreeMap<_, _>>();
        nodes[0]
            .as_ref()
            .unwrap()
            .handle
            .initialize_membership(membership)
            .await
            .unwrap();
        let leader_index = leader(&nodes).await;
        let identity = &identities[leader_index];
        assert!(
            crate::read_control_projection(
                &identity.cluster,
                identity.node.node_id,
                &identity.node.cluster_url,
                Duration::from_secs(1)
            )
            .await
            .is_err(),
            "pre-bootstrap state is not a usable managed projection"
        );
        let pending = crate::read_bootstrap_control_state(
            &identity.cluster,
            identity.node.node_id,
            &identity.node.cluster_url,
            Duration::from_secs(1),
        )
        .await
        .unwrap();
        assert!(pending.state.cluster_bootstrap.is_none());
        assert!(pending.state.placements.is_empty());
        pending.validate_bootstrap_state().unwrap();
        assert!(pending.validate().is_err());
        let bootstrap = ClusterBootstrap {
            identity: identity.cluster.clone(),
            initial_meta_voters: (1..=count).collect(),
            nodes: identities
                .iter()
                .map(|id| (id.node.node_id, id.node.clone()))
                .collect(),
            voters: BTreeMap::from([(RaftGroupId(0), (1..=count).collect())]),
            placement: PlacementPolicy {
                default_replication_factor: ReplicationFactor::try_from(count as u32).unwrap(),
                ..Default::default()
            },
        };
        // Data-plane certificate collection is covered by the real data RPC test;
        // this test focuses on the independent meta projection transport.
        let certificates = BTreeMap::from([(RaftGroupId(0), VerifiedGroupMembership {
            voters: (1..=count).collect(),
            learners: BTreeSet::new(),
            log_id: MembershipLogId {
                term: 1,
                node_id: 1,
                index: 1,
            },
        })]);
        let handle = &nodes[leader_index].as_ref().unwrap().handle;
        assert_eq!(
            handle
                .write(ControlCommand::BootstrapCluster {
                    bootstrap,
                    memberships: certificates,
                    now_ms: 1
                })
                .await
                .unwrap(),
            ControlResponse::Ok
        );
        let first = crate::read_control_projection(
            &identity.cluster,
            identity.node.node_id,
            &identity.node.cluster_url,
            Duration::from_secs(1),
        )
        .await
        .unwrap();
        let mut cursor = ursula_control::ProjectionCursor::new(identity.cluster.clone()).unwrap();
        assert_eq!(
            cursor.install(first.clone()).unwrap(),
            ursula_control::ProjectionInstall::Advanced
        );
        assert_eq!(
            handle
                .write(ControlCommand::SetNodeState {
                    node_id: count,
                    state: NodeState::Draining,
                    now_ms: 2
                })
                .await
                .unwrap(),
            ControlResponse::Ok
        );
        let second = crate::read_control_projection(
            &identity.cluster,
            identity.node.node_id,
            &identity.node.cluster_url,
            Duration::from_secs(1),
        )
        .await
        .unwrap();
        assert!(second.applied_log_id.index > first.applied_log_id.index);
        assert_eq!(second.state.nodes[&count].state, NodeState::Draining);
        assert_eq!(
            cursor.install(second.clone()).unwrap(),
            ursula_control::ProjectionInstall::Advanced
        );
        assert_eq!(
            cursor.install(first).unwrap(),
            ursula_control::ProjectionInstall::Stale
        );
        let follower_index = (leader_index + 1) % nodes.len();
        let follower = &identities[follower_index];
        assert!(
            crate::read_control_projection(
                &follower.cluster,
                follower.node.node_id,
                &follower.node.cluster_url,
                Duration::from_secs(1)
            )
            .await
            .is_err()
        );
        let mut wrong_contract = identity.cluster.clone();
        wrong_contract.group_count += 1;
        assert!(
            crate::read_control_projection(
                &wrong_contract,
                identity.node.node_id,
                &identity.node.cluster_url,
                Duration::from_secs(1)
            )
            .await
            .is_err()
        );
        let prefix = applied(handle).await;
        wait_state(&nodes, prefix).await;
        snapshot_and_purge(handle).await;
        let after_compaction = crate::read_control_projection(
            &identity.cluster,
            identity.node.node_id,
            &identity.node.cluster_url,
            Duration::from_secs(1),
        )
        .await
        .unwrap();
        assert_eq!(after_compaction.state, second.state);
        // Keep the sampled leader alive while removing its quorum. Cached state
        // remains usable to existing consumers, but cannot certify a fresh read.
        let remove_count = count / 2 + 1;
        for index in (0..nodes.len())
            .filter(|index| *index != leader_index)
            .take(remove_count as usize)
        {
            stop(nodes[index].take().unwrap()).await;
        }
        assert!(
            crate::read_control_projection(
                &identity.cluster,
                identity.node.node_id,
                &identity.node.cluster_url,
                Duration::from_millis(500)
            )
            .await
            .is_err()
        );
        assert_eq!(cursor.current(), Some(&second));
        for node in nodes.into_iter().flatten() {
            stop(node).await;
        }
    }
}

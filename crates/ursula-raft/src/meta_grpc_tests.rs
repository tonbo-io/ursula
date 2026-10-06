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
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::TcpListenerStream;
use ursula_control::ControlCommand;
use ursula_control::ControlPlaneState;
use ursula_control::ControlResponse;
use ursula_control::GroupPolicyOverride;
use ursula_control::PlacementPolicy;
use ursula_control::ReplicationFactor;
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
            client.transfer_leader(envelope).await.unwrap_err().code(),
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

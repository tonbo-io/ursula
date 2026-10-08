//! Recovery fault schedules over real TCP/gRPC, retaining the actual Raft handlers.

use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

use openraft::rt::WatchReceiver;
use tonic::Request;
use tonic::Response;
use tonic::Status;

use super::*;
use crate::grpc::GrpcRaftNetworkFactory;
use crate::grpc::RaftGrpcService;
use crate::grpc::probe_rejoin_vote_barrier;
use crate::raft_internal_proto as pb;
use crate::raft_internal_proto::raft_internal_server::RaftInternal;
use crate::rejoin::GroupRejoin;

#[derive(Clone)]
struct RecoveryTestService {
    inner: RaftGrpcService,
    pause_replication: Arc<AtomicBool>,
    reject_barrier: Arc<AtomicBool>,
}

#[tonic::async_trait]
impl RaftInternal for RecoveryTestService {
    type AppendStreamStream = <RaftGrpcService as RaftInternal>::AppendStreamStream;

    async fn append(
        &self,
        request: Request<pb::RaftRpcEnvelopeV1>,
    ) -> Result<Response<pb::RaftRpcAckV1>, Status> {
        // Unary Append is used only to deliver the captured, delayed request.
        self.inner.append(request).await
    }

    async fn append_stream(
        &self,
        request: Request<tonic::Streaming<pb::RaftAppendStreamRequest>>,
    ) -> Result<Response<Self::AppendStreamStream>, Status> {
        let responses = stream::unfold(
            (request.into_inner(), self.clone()),
            |(mut requests, service)| async move {
                let frame = match requests.message().await {
                    Ok(Some(frame)) => frame,
                    Ok(None) => return None,
                    Err(err) => return Some((Err(err), (requests, service))),
                };
                let mut items = Vec::new();
                for item in frame.items {
                    let result = if service.pause_replication.load(Ordering::SeqCst) {
                        Err(Status::unavailable("test delayed replication"))
                    } else {
                        service
                            .inner
                            .append(Request::new(item.envelope.expect("append envelope")))
                            .await
                            .map(Response::into_inner)
                    };
                    use pb::raft_append_stream_response_item::Result as WireResult;
                    items.push(pb::RaftAppendStreamResponseItem {
                        request_id: item.request_id,
                        result: Some(match result {
                            Ok(ack) => WireResult::Ack(ack),
                            Err(err) => WireResult::Error(pb::RaftAppendStreamError {
                                code: err.code() as i32,
                                message: err.message().to_owned(),
                            }),
                        }),
                    });
                }
                Some((
                    Ok(pb::RaftAppendStreamResponse { items }),
                    (requests, service),
                ))
            },
        );
        Ok(Response::new(Box::pin(responses)))
    }

    async fn vote(
        &self,
        request: Request<pb::RaftRpcEnvelopeV1>,
    ) -> Result<Response<pb::RaftRpcAckV1>, Status> {
        self.inner.vote(request).await
    }

    async fn full_snapshot(
        &self,
        request: Request<pb::RaftFullSnapshotRequestV1>,
    ) -> Result<Response<pb::RaftFullSnapshotAckV1>, Status> {
        self.inner.full_snapshot(request).await
    }

    async fn group_write(
        &self,
        request: Request<pb::GroupWriteRequestV1>,
    ) -> Result<Response<pb::GroupWriteResponseV1>, Status> {
        self.inner.group_write(request).await
    }

    async fn group_read(
        &self,
        request: Request<pb::GroupReadRequestV1>,
    ) -> Result<Response<pb::GroupReadResponseV1>, Status> {
        self.inner.group_read(request).await
    }

    async fn rejoin_barrier(
        &self,
        request: Request<pb::RejoinBarrierRequestV1>,
    ) -> Result<Response<pb::RejoinBarrierResponseV1>, Status> {
        if self.reject_barrier.load(Ordering::SeqCst) {
            return Err(Status::unimplemented(
                "explicit barrier unavailable in test",
            ));
        }
        self.inner.rejoin_barrier(request).await
    }

    async fn transfer_leader(
        &self,
        request: Request<pb::RaftTransferLeaderRequestV1>,
    ) -> Result<Response<pb::RaftTransferLeaderAckV1>, Status> {
        self.inner.transfer_leader(request).await
    }
}

/// A replica of node `id` on a fresh, empty WAL, as a new node or one that
/// lost its disk starts: its gate is closed with the group's history
/// unknown. The returned directory holds the WAL and must outlive the store.
async fn new_recovery_engine(
    id: u64,
    config: Arc<Config>,
    registry: &RaftGroupHandleRegistry,
) -> (
    RaftGroupEngine,
    Arc<RaftGroupFileLogStore>,
    Arc<GroupRejoin>,
    tempfile::TempDir,
) {
    let wal_root = tempfile::tempdir().expect("WAL root");
    let store = RaftWal::start(
        wal_root.path(),
        WalFsync::Never,
        &ursula_shard::StaticShardMap::new(1, 1).expect("valid topology"),
    )
    .expect("start the WAL")
    .open(
        placement(),
        ursula_runtime::RuntimeMetrics::new(1, 1).group_engine_metrics(),
    )
    .expect("open the log store");
    let gate = Arc::new(
        GroupRejoin::durable(id, placement().raft_group_id, &store)
            .await
            .expect("open the gate"),
    );
    let engine = RaftGroupEngine::new_node_with_log_store_and_network(
        placement(),
        id,
        config,
        GrpcRaftNetworkFactory::new(Arc::default(), placement().raft_group_id)
            .with_rejoin(Some(gate.clone())),
        store.clone(),
        None,
        None,
    )
    .await
    .expect("new engine on an empty WAL");
    engine.publish_recovery(gate.clone(), registry).unwrap();
    (engine, store, gate, wal_root)
}

#[tokio::test]
async fn recovery_publication_keeps_one_gate_bound_to_one_engine() {
    let registry = RaftGroupHandleRegistry::default();
    let config = Arc::new(
        Config {
            enable_tick: false,
            ..Default::default()
        }
        .validate()
        .unwrap(),
    );
    let (engine, store, gate, _root) = new_recovery_engine(1, config.clone(), &registry).await;
    let original = registry.entry(placement().raft_group_id).unwrap();
    engine.publish_recovery(gate.clone(), &registry).unwrap();
    registry.register_engine(&engine);
    assert!(Arc::ptr_eq(
        &registry.rejoin(placement().raft_group_id).unwrap(),
        &gate
    ));
    let alternate = Arc::new(
        GroupRejoin::durable(1, placement().raft_group_id, &store)
            .await
            .unwrap(),
    );
    assert!(matches!(
        engine.publish_recovery(alternate, &registry),
        Err(ursula_runtime::GroupEngineError::Infra(
            ursula_runtime::GroupInfraError::RecoveryAlreadyBound { .. }
        ))
    ));
    let other_registry = RaftGroupHandleRegistry::default();
    let (other, _, other_gate, _other_root) = new_recovery_engine(1, config, &other_registry).await;
    assert!(matches!(
        other.publish_recovery(gate.clone(), &other_registry),
        Err(ursula_runtime::GroupEngineError::Infra(
            ursula_runtime::GroupInfraError::RecoveryAlreadyBound { .. }
        ))
    ));
    registry.register_engine(&other);
    assert!(Arc::ptr_eq(original.recovery.as_ref().unwrap(), &gate));
    assert!(Arc::ptr_eq(
        registry
            .entry(placement().raft_group_id)
            .unwrap()
            .recovery
            .as_ref()
            .unwrap(),
        &other_gate
    ));
    assert!(matches!(
        registry
            .append_entries(
                placement().raft_group_id,
                crate::UrsulaAppendEntriesRequest {
                    vote: crate::UrsulaVote::new_committed(1, 2),
                    prev_log_id: None,
                    entries: Vec::new(),
                    leader_commit: None
                }
            )
            .await,
        Err(ursula_runtime::GroupEngineError::Infra(
            ursula_runtime::GroupInfraError::RecoveryVoteFloor { .. }
        ))
    ));
    // A refused rebind must not disable the unrelated engine's elections.
    other
        .raft
        .initialize(std::collections::BTreeMap::from([(
            1,
            BasicNode::new("local"),
        )]))
        .await
        .unwrap();
    other
        .raft
        .wait(Some(Duration::from_secs(5)))
        .current_leader(1, "singleton leader")
        .await
        .unwrap();
    other
        .raft
        .append_entries(crate::UrsulaAppendEntriesRequest {
            vote: crate::UrsulaVote::new_committed(99, 2),
            prev_log_id: None,
            entries: Vec::new(),
            leader_commit: None,
        })
        .await
        .unwrap();
    other.raft.runtime_config().elect(true);
    assert!(matches!(
        gate.bind(&other.raft_handle()),
        Err(ursula_runtime::GroupEngineError::Infra(
            ursula_runtime::GroupInfraError::RecoveryAlreadyBound { .. }
        ))
    ));
    other.raft.runtime_config().tick(true);
    other
        .raft
        .wait(Some(Duration::from_secs(5)))
        .metrics(
            |metrics| metrics.current_term > 99 && metrics.current_leader == Some(1),
            "failed bind preserves campaigning",
        )
        .await
        .unwrap();
    engine.shutdown().await.unwrap();
    other.shutdown().await.unwrap();
}

const ELECTION_TIMEOUT_MIN_MS: u64 = 300;
const ELECTION_TIMEOUT_MAX_MS: u64 = 600;
/// Longer than any election timeout: a replica that could campaign would
/// have started an election by then.
const LONGER_THAN_AN_ELECTION: Duration =
    Duration::from_millis(ELECTION_TIMEOUT_MAX_MS.saturating_mul(2));

#[tokio::test]
async fn delayed_pre_restart_append_cannot_restore_voting_or_campaigning_over_grpc() {
    let config = Arc::new(
        Config {
            cluster_name: "recovery-delayed-grpc".to_owned(),
            heartbeat_interval: 10,
            // Every vote is `fsync`ed to the core metadata file before it is
            // granted; leave a vote RPC room for that.
            election_timeout_min: ELECTION_TIMEOUT_MIN_MS,
            election_timeout_max: ELECTION_TIMEOUT_MAX_MS,
            enable_tick: false,
            snapshot_policy: SnapshotPolicy::Never,
            ..Default::default()
        }
        .validate()
        .unwrap(),
    );
    let mut registries = Vec::new();
    let mut endpoints = Vec::new();
    let mut services = Vec::new();
    let mut servers = Vec::new();
    let mut engines = Vec::new();
    let mut stores = Vec::new();
    let mut gates = Vec::new();
    let mut wal_roots = Vec::new();
    for id in 1..=3 {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        endpoints.push(format!("http://{}", listener.local_addr().unwrap()));
        let registry = RaftGroupHandleRegistry::default();
        let service = RecoveryTestService {
            inner: RaftGrpcService::new(registry.clone()),
            pause_replication: Arc::new(AtomicBool::new(false)),
            reject_barrier: Arc::new(AtomicBool::new(false)),
        };
        let wire_service = pb::raft_internal_server::RaftInternalServer::new(service.clone())
            .accept_compressed(tonic::codec::CompressionEncoding::Zstd);
        let app = axum::Router::new().fallback_service(wire_service);
        servers.push(tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        }));
        let (engine, store, gate, wal_root) =
            new_recovery_engine(id, config.clone(), &registry).await;
        registries.push(registry);
        services.push(service);
        engines.push(engine);
        stores.push(store);
        gates.push(gate);
        wal_roots.push(wal_root);
    }
    let nodes = endpoints
        .iter()
        .enumerate()
        .map(|(index, endpoint)| (u64::try_from(index).unwrap() + 1, BasicNode::new(endpoint)))
        .collect::<BTreeMap<_, _>>();
    // This fixture represents an initial bootstrap after proving every voter
    // empty.
    for gate in &gates {
        gate.establish_vote_floor(UrsulaVote::new(0, 0))
            .await
            .unwrap();
    }
    gates[1].allow_fresh_bootstrap().await.unwrap();
    registries[1].refresh_group_elections(placement().raft_group_id);
    engines[1].raft.initialize(nodes).await.unwrap();
    engines[1]
        .raft
        .wait(Some(Duration::from_secs(5)))
        .current_leader(2, "initial leader")
        .await
        .unwrap();
    let stream_id = bsid("acked-recovery");
    engines[1]
        .raft
        .client_write(create_command(stream_id.clone()))
        .await
        .unwrap();
    let baseline = engines[1]
        .raft
        .client_write(append_command(stream_id.clone(), b"before"))
        .await
        .unwrap();
    engines[1].raft.trigger().heartbeat().await.unwrap();
    for engine in &engines {
        engine
            .raft
            .wait(Some(Duration::from_secs(5)))
            .applied_index_at_least(Some(baseline.log_id.index()), "baseline applied")
            .await
            .unwrap();
    }
    let proof = probe_rejoin_vote_barrier(
        Arc::default(),
        placement(),
        2,
        &endpoints[1],
        Duration::from_secs(1),
    )
    .await
    .expect("fresh explicit barrier");
    assert!(proof.1 >= baseline.log_id.index());
    for gate in &gates {
        gate.confirm_barrier(proof.0, proof.1);
        assert!(gate.try_open().await.unwrap());
    }
    let mut leader_store = stores[1].clone();
    let delayed = UrsulaAppendEntriesRequest {
        vote: proof.0,
        prev_log_id: None,
        entries: leader_store
            .try_get_log_entries(..=baseline.log_id.index())
            .await
            .unwrap(),
        leader_commit: Some(baseline.log_id),
    };
    services[2].pause_replication.store(true, Ordering::SeqCst);
    let acked = engines[1]
        .raft
        .client_write(append_command(stream_id.clone(), b"-acked"))
        .await
        .unwrap();
    engines[1].raft.trigger().heartbeat().await.unwrap();
    engines[0]
        .raft
        .wait(Some(Duration::from_secs(5)))
        .applied_index_at_least(Some(acked.log_id.index()), "ACK on A")
        .await
        .unwrap();
    // A loses its disk and restarts empty.
    services[0].pause_replication.store(true, Ordering::SeqCst);
    engines[0].shutdown().await.unwrap();
    let (fresh, store, gate, wal_root) =
        new_recovery_engine(1, config.clone(), &registries[0]).await;
    engines[0] = fresh;
    stores[0] = store;
    gates[0] = gate;
    wal_roots[0] = wal_root;
    let channel = tonic::transport::Endpoint::from_shared(endpoints[0].clone())
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut client = pb::raft_internal_client::RaftInternalClient::new(channel);
    let rejected = client
        .append(Request::new(pb::RaftRpcEnvelopeV1 {
            raft_group_id: placement().raft_group_id.0,
            node_id: 1,
            protocol_version: ursula_stream::FORMAT_EPOCH,
            payload: encode_wire(&delayed),
        }))
        .await
        .expect_err("unknown history must not acknowledge a delayed Append");
    assert_eq!(rejected.code(), tonic::Code::Unavailable);
    assert_eq!(
        engines[0].raft.metrics().borrow_watched().last_applied,
        None
    );
    assert!(!gates[0].vote_gate_open());
    assert!(!registries[0].recovery_barriers_ready());
    let term = engines[0].raft.metrics().borrow_watched().current_term;
    engines[0].raft.runtime_config().tick(true);
    tokio::time::sleep(LONGER_THAN_AN_ELECTION).await;
    assert_eq!(
        engines[0].raft.metrics().borrow_watched().current_term,
        term,
        "an empty restart must not campaign on its own"
    );
    let transfer = openraft::raft::TransferLeaderRequest::<UrsulaRaftTypeConfig>::new(
        delayed.vote,
        1,
        Some(baseline.log_id),
    );
    assert!(
        client
            .transfer_leader(Request::new(pb::RaftTransferLeaderRequestV1 {
                raft_group_id: placement().raft_group_id.0,
                node_id: 1,
                protocol_version: ursula_stream::FORMAT_EPOCH,
                request: encode_wire(&transfer),
            }))
            .await
            .is_err(),
        "a leadership transfer must not bypass recovery"
    );
    engines[2].raft.trigger().elect(false).await.unwrap();
    tokio::time::sleep(LONGER_THAN_AN_ELECTION).await;
    assert_ne!(
        engines[2].raft.metrics().borrow_watched().current_leader,
        Some(3),
        "lagging C cannot win using restarted A"
    );
    for reject_barrier in [false, true] {
        services[1]
            .reject_barrier
            .store(reject_barrier, Ordering::SeqCst);
        assert!(
            probe_rejoin_vote_barrier(
                Arc::default(),
                placement(),
                2,
                &endpoints[1],
                Duration::from_millis(150)
            )
            .await
            .is_err(),
            "an unreachable quorum cannot confirm a fresh proof"
        );
        crate::confirm_quorum_prefix(placement(), 2, &endpoints[1], Duration::from_millis(150))
            .await
            .expect_err("a stale or unsupported peer cannot confirm a quorum prefix");
    }
    // Reconnect healthy C first. B still owns the ACKed suffix; A still only
    // has the old prefix, so a successful fresh proof alone cannot open A.
    services[2].pause_replication.store(false, Ordering::SeqCst);
    engines[1].raft.runtime_config().tick(true);
    engines[1].raft.trigger().heartbeat().await.unwrap();
    engines[2]
        .raft
        .wait(Some(Duration::from_secs(5)))
        .applied_index_at_least(Some(acked.log_id.index()), "C repaired suffix")
        .await
        .unwrap();
    // Peers without the explicit protocol fail closed; no HEAD bridge remains.
    services[1].reject_barrier.store(true, Ordering::SeqCst);
    let unsupported = probe_rejoin_vote_barrier(
        Arc::default(),
        placement(),
        2,
        &endpoints[1],
        Duration::from_secs(1),
    )
    .await
    .expect_err("unsupported recovery protocol is refused");
    assert!(
        matches!(unsupported, crate::grpc::RecoveryProbeError::Rpc(status) if status.code() == tonic::Code::Unimplemented)
    );
    services[1].reject_barrier.store(false, Ordering::SeqCst);
    {
        let proof = probe_rejoin_vote_barrier(
            Arc::default(),
            placement(),
            2,
            &endpoints[1],
            Duration::from_secs(1),
        )
        .await
        .expect("fresh recovery proof");
        assert!(proof.1 >= acked.log_id.index());
        let observed =
            crate::confirm_quorum_prefix(placement(), 2, &endpoints[1], Duration::from_secs(1))
                .await
                .expect("fresh maintenance observation through the same probe");
        assert_eq!(observed.leader_id, 2);
        assert_eq!(observed.raft_group_id, placement().raft_group_id.0);
        assert!(observed.required_applied_index >= acked.log_id.index());
        engines[0]
            .raft
            .vote(UrsulaVoteRequest::new(proof.0, None))
            .await
            .unwrap();
        gates[0].establish_vote_floor(proof.0).await.unwrap();
        gates[0].confirm_barrier(proof.0, proof.1);
        assert!(
            !gates[0].try_open().await.unwrap(),
            "proof must also be applied"
        );
    }
    services[0].pause_replication.store(false, Ordering::SeqCst);
    engines[1].raft.trigger().heartbeat().await.unwrap();
    engines[0]
        .raft
        .wait(Some(Duration::from_secs(5)))
        .applied_index_at_least(Some(acked.log_id.index()), "A catches up")
        .await
        .unwrap();
    assert!(gates[0].try_open().await.unwrap());
    registries[0].refresh_group_elections(placement().raft_group_id);
    assert!(registries[0].recovery_barriers_ready());
    registries[0].mark_leadership_shed(LeadershipShedReason::MaintenanceDrain);
    registries[0].refresh_group_elections(placement().raft_group_id);
    services[0].pause_replication.store(true, Ordering::SeqCst);
    let term = engines[0].raft.metrics().borrow_watched().current_term;
    tokio::time::sleep(LONGER_THAN_AN_ELECTION).await;
    assert_eq!(
        engines[0].raft.metrics().borrow_watched().current_term,
        term,
        "recovery cannot clear maintenance and enable campaigning"
    );
    services[0].pause_replication.store(false, Ordering::SeqCst);
    registries[0].clear_leadership_shed(LeadershipShedReason::MaintenanceDrain);

    // A second loss after full repair starts a new closed gate. Changing the
    // healthy leader must neither reuse A's previous proof nor accept a
    // accept the stale peer as proof of the current quorum prefix.
    services[0].pause_replication.store(true, Ordering::SeqCst);
    engines[0].shutdown().await.unwrap();
    let (fresh, store, gate, wal_root) = new_recovery_engine(1, config, &registries[0]).await;
    engines[0] = fresh;
    stores[0] = store;
    gates[0] = gate;
    wal_roots[0] = wal_root;
    let rejected = client
        .append(Request::new(pb::RaftRpcEnvelopeV1 {
            raft_group_id: placement().raft_group_id.0,
            node_id: 1,
            protocol_version: ursula_stream::FORMAT_EPOCH,
            payload: encode_wire(&delayed),
        }))
        .await
        .expect_err("a new lost disk needs a new proven floor");
    assert_eq!(rejected.code(), tonic::Code::Unavailable);
    assert_eq!(
        engines[0].raft.metrics().borrow_watched().last_applied,
        None
    );
    registries[0].mark_leadership_shed(LeadershipShedReason::MaintenanceDrain);
    registries[0].clear_leadership_shed(LeadershipShedReason::MaintenanceDrain);
    engines[0].raft.runtime_config().tick(true);
    let term = engines[0].raft.metrics().borrow_watched().current_term;
    engines[1].raft.runtime_config().tick(false);
    tokio::time::sleep(LONGER_THAN_AN_ELECTION).await;
    assert_eq!(
        engines[0].raft.metrics().borrow_watched().current_term,
        term,
        "undrain cannot bypass a new recovery gate"
    );
    engines[2].raft.trigger().elect(false).await.unwrap();
    engines[2]
        .raft
        .wait(Some(Duration::from_secs(5)))
        .current_leader(3, "healthy C becomes leader")
        .await
        .unwrap();
    engines[2].raft.runtime_config().tick(true);
    services[1].reject_barrier.store(true, Ordering::SeqCst);
    assert!(
        probe_rejoin_vote_barrier(
            Arc::default(),
            placement(),
            2,
            &endpoints[1],
            Duration::from_secs(1)
        )
        .await
        .is_err(),
        "an unsupported peer cannot supply a fresh proof"
    );
    // The leader metric may precede application of the new term's blank entry.
    // A transient ReadIndex refusal must leave the restarted voter ineligible;
    // the production recovery driver also retries fresh outbound proofs.
    let proof_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let proof = loop {
        match probe_rejoin_vote_barrier(
            Arc::default(),
            placement(),
            3,
            &endpoints[2],
            Duration::from_secs(1),
        )
        .await
        {
            Ok(proof) => break proof,
            Err(error) => {
                assert!(
                    !gates[0].vote_gate_open(),
                    "failed proof cannot open the gate"
                );
                assert!(
                    tokio::time::Instant::now() < proof_deadline,
                    "fresh proof from new leader C: {error}; metrics: {:?}",
                    engines[2].raft.metrics().borrow_watched()
                );
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }
    };
    assert!(proof.1 >= acked.log_id.index());
    engines[0]
        .raft
        .vote(UrsulaVoteRequest::new(proof.0, None))
        .await
        .unwrap();
    gates[0].establish_vote_floor(proof.0).await.unwrap();
    gates[0].confirm_barrier(proof.0, proof.1);
    assert!(
        !gates[0].try_open().await.unwrap(),
        "a previous incarnation's proof cannot be reused"
    );
    services[0].pause_replication.store(false, Ordering::SeqCst);
    engines[2].raft.trigger().heartbeat().await.unwrap();
    engines[0]
        .raft
        .wait(Some(Duration::from_secs(5)))
        .applied_index_at_least(Some(proof.1), "second recovery applied")
        .await
        .unwrap();
    assert!(gates[0].try_open().await.unwrap());
    for engine in &engines {
        let payload = read_stream_via_state_machine(engine, stream_id.clone(), 1024)
            .await
            .unwrap()
            .unwrap()
            .payload;
        assert_eq!(payload, b"before-acked");
    }
    shutdown_all(&engines).await;
    for server in servers {
        server.abort();
    }
}

/// ACKs are quorum-replicated, not necessarily applied on the third replica.
/// A catch-up cursor ahead of that replica must be checked at the leader.
#[tokio::test]
async fn lagging_follower_reads_acknowledged_cursor_over_grpc() {
    let config = Arc::new(
        Config {
            cluster_name: "stale-follower-cursor-grpc".to_owned(),
            heartbeat_interval: 10,
            election_timeout_min: 100,
            election_timeout_max: 200,
            enable_tick: false,
            snapshot_policy: SnapshotPolicy::Never,
            ..Default::default()
        }
        .validate()
        .unwrap(),
    );
    let mut endpoints = Vec::new();
    let mut services = Vec::new();
    let mut servers = Vec::new();
    let mut engines = Vec::new();
    let mut gates = Vec::new();
    let mut wal_roots = Vec::new();
    for id in 1..=3 {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        endpoints.push(format!("http://{}", listener.local_addr().unwrap()));
        let registry = RaftGroupHandleRegistry::default();
        let service = RecoveryTestService {
            inner: RaftGrpcService::new(registry.clone()),
            pause_replication: Arc::new(AtomicBool::new(false)),
            reject_barrier: Arc::new(AtomicBool::new(false)),
        };
        let wire = pb::raft_internal_server::RaftInternalServer::new(service.clone())
            .accept_compressed(tonic::codec::CompressionEncoding::Zstd);
        servers.push(tokio::spawn(async move {
            axum::serve(listener, axum::Router::new().fallback_service(wire))
                .await
                .unwrap();
        }));
        let (engine, _, gate, wal_root) = new_recovery_engine(id, config.clone(), &registry).await;
        services.push(service);
        engines.push(engine);
        gates.push(gate);
        wal_roots.push(wal_root);
    }
    let nodes = endpoints
        .iter()
        .enumerate()
        .map(|(index, endpoint)| (u64::try_from(index).unwrap() + 1, BasicNode::new(endpoint)))
        .collect::<BTreeMap<_, _>>();
    for gate in &gates {
        gate.establish_vote_floor(UrsulaVote::new(0, 0))
            .await
            .unwrap();
    }
    gates[1].allow_fresh_bootstrap().await.unwrap();
    engines[1].raft.runtime_config().elect(true);
    engines[1].raft.initialize(nodes).await.unwrap();
    engines[1]
        .raft
        .wait(Some(Duration::from_secs(5)))
        .current_leader(2, "initial leader")
        .await
        .unwrap();
    let stream_id = bsid("ack-cursor");
    create_stream_via_raft(&engines[1], stream_id.clone()).await;
    let initial = engines[1]
        .raft
        .client_write(append_command(stream_id.clone(), b"old-prefix"))
        .await
        .unwrap();
    engines[1].raft.trigger().heartbeat().await.unwrap();
    for engine in &engines {
        engine
            .raft
            .wait(Some(Duration::from_secs(5)))
            .applied_index_at_least(Some(initial.log_id.index()), "initial prefix applied")
            .await
            .unwrap();
    }
    services[0].pause_replication.store(true, Ordering::SeqCst);
    engines[1]
        .raft
        .client_write(append_command(stream_id.clone(), b"-gap"))
        .await
        .unwrap();
    let acked = engines[1]
        .raft
        .client_write(append_command(stream_id.clone(), b"-ACK"))
        .await
        .unwrap();
    match write_result_from_raft_response(acked.data).unwrap() {
        Ok(GroupWriteResponse::Append(response)) => {
            assert_eq!(response.start_offset, 14);
            assert_eq!(response.next_offset, 18);
        }
        other => panic!("unexpected append response: {other:?}"),
    }
    assert_eq!(
        engines[0]
            .raft
            .metrics()
            .borrow_watched()
            .last_applied
            .as_ref()
            .unwrap()
            .index(),
        initial.log_id.index()
    );
    let read = engines[0]
        .read_stream(
            ReadStreamRequest {
                offset: 14,
                ..read_req(stream_id.clone(), 4)
            },
            placement(),
        )
        .await
        .expect("read acknowledged cursor through lagging follower");
    assert_eq!(read.payload, b"-ACK");
    assert_eq!(read.next_offset, 18);
    let invalid = engines[0]
        .read_stream(
            ReadStreamRequest {
                offset: 19,
                ..read_req(stream_id.clone(), 4)
            },
            placement(),
        )
        .await
        .expect_err("cursor beyond authoritative tail stays invalid");
    assert_eq!(invalid.code(), Some(StreamErrorCode::OffsetOutOfRange));
    // A remembered leader without a quorum cannot certify a permanent
    // boundary, even when its own tail currently rejects the cursor.
    services[2].pause_replication.store(true, Ordering::SeqCst);
    let unconfirmed = engines[0]
        .read_stream(
            ReadStreamRequest {
                offset: 19,
                ..read_req(stream_id.clone(), 4)
            },
            placement(),
        )
        .await
        .expect_err("boundary requires a current leader proof");
    assert!(unconfirmed.leader_hint().is_some(), "{unconfirmed:?}");
    assert_ne!(unconfirmed.code(), Some(StreamErrorCode::OffsetOutOfRange));
    services[2].pause_replication.store(false, Ordering::SeqCst);
    services[0].pause_replication.store(false, Ordering::SeqCst);
    engines[1].raft.trigger().heartbeat().await.unwrap();
    for engine in &engines {
        engine
            .raft
            .wait(Some(Duration::from_secs(5)))
            .applied_index_at_least(Some(acked.log_id.index()), "complete ACK prefix repaired")
            .await
            .unwrap();
        assert_eq!(
            read_stream_via_state_machine(engine, stream_id.clone(), 64)
                .await
                .unwrap()
                .unwrap()
                .payload,
            b"old-prefix-gap-ACK"
        );
    }
    shutdown_all(&engines).await;
    for server in servers {
        server.abort();
    }
}

#[tokio::test]
async fn a_lost_vote_refuses_heartbeat_ack_until_the_proven_floor_is_durable() {
    use openraft::storage::RaftLogReader;

    let registry = RaftGroupHandleRegistry::default();
    let config = Arc::new(
        Config {
            enable_tick: false,
            ..Default::default()
        }
        .validate()
        .unwrap(),
    );
    let (engine, mut store, gate, _root) = new_recovery_engine(1, config, &registry).await;
    // Freeze an all-empty observation before the newer proof reaches the owner.
    let (release_genesis, delayed_genesis) = tokio::sync::oneshot::channel();
    let delayed_gate = gate.clone();
    let genesis = tokio::spawn(async move {
        delayed_genesis.await.unwrap();
        delayed_gate
            .establish_vote_floor(UrsulaVote::new(0, 0))
            .await
            .unwrap();
    });
    let proven = UrsulaVote::new_committed(4, 2);
    let snapshot = || openraft::Snapshot {
        meta: Default::default(),
        snapshot: std::io::Cursor::new(Vec::new()),
    };
    // Another install holds the node's only install permit. A refused
    // install answers without queueing for it.
    let busy = registry
        .snapshot_install_coordinator()
        .acquire()
        .await
        .unwrap();
    assert!(matches!(
        tokio::time::timeout(
            Duration::from_secs(1),
            registry.install_full_snapshot(placement().raft_group_id, proven, snapshot()),
        )
        .await
        .expect("a refused install does not wait for the install permit"),
        Err(crate::SnapshotInstallError::Group(
            ursula_runtime::GroupEngineError::Infra(
                ursula_runtime::GroupInfraError::RecoveryVoteFloor { .. }
            )
        ))
    ));
    let heartbeat = |vote| openraft::raft::AppendEntriesRequest {
        vote,
        prev_log_id: None,
        entries: Vec::new(),
        leader_commit: None,
    };
    assert!(matches!(
        registry
            .append_entries(placement().raft_group_id, heartbeat(proven))
            .await,
        Err(ursula_runtime::GroupEngineError::Infra(
            ursula_runtime::GroupInfraError::RecoveryVoteFloor { .. }
        ))
    ));
    assert_eq!(store.read_vote().await.unwrap(), None);
    // This is the same adoption path used only after the driver's fresh quorum proof.
    engine
        .raft
        .vote(UrsulaVoteRequest::new(proven, None))
        .await
        .unwrap();
    assert_eq!(store.read_vote().await.unwrap(), Some(proven));
    assert!(
        !gate.replication_allowed(proven),
        "durable vote alone does not publish admission"
    );
    gate.establish_vote_floor(proven).await.unwrap();
    registry
        .append_entries(placement().raft_group_id, heartbeat(proven))
        .await
        .unwrap();
    assert!(matches!(
        registry.append_entries(placement().raft_group_id, heartbeat(UrsulaVote::new_committed(3, 2))).await.unwrap(),
        openraft::raft::AppendEntriesResponse::HigherVote(vote) if vote == proven
    ));
    let stale_snapshot = tokio::time::timeout(
        Duration::from_secs(1),
        registry.install_full_snapshot(
            placement().raft_group_id,
            UrsulaVote::new_committed(3, 2),
            snapshot(),
        ),
    )
    .await
    .expect("a below-floor install does not wait for the install permit")
    .unwrap();
    drop(busy);
    assert_eq!(stale_snapshot.vote, proven);
    assert_eq!(
        engine.raft.metrics().borrow_watched().snapshot,
        None,
        "reporting the higher vote must not install the stale snapshot"
    );
    // A racing genesis observation must never lower a persisted or published floor.
    release_genesis.send(()).unwrap();
    genesis.await.unwrap();
    assert_eq!(store.read_vote().await.unwrap(), Some(proven));
    assert!(!gate.replication_allowed(UrsulaVote::new_committed(3, 2)));
    engine.shutdown().await.unwrap();
}

#[tokio::test]
async fn genesis_waits_for_the_last_empty_voters_durable_floor() {
    use openraft::storage::RaftLogReader;

    let config = Arc::new(
        Config {
            enable_tick: false,
            ..Default::default()
        }
        .validate()
        .unwrap(),
    );
    let first_registry = RaftGroupHandleRegistry::default();
    let second_registry = RaftGroupHandleRegistry::default();
    let (first, _first_store, first_gate, _first_root) =
        new_recovery_engine(1, config.clone(), &first_registry).await;
    let (second, mut second_store, second_gate, _second_root) =
        new_recovery_engine(2, config, &second_registry).await;
    first_gate
        .establish_vote_floor(UrsulaVote::new(0, 0))
        .await
        .unwrap();
    let (observations, mut observed) = tokio::sync::mpsc::unbounded_channel();
    let peer = second_gate.clone();
    let bootstrap = tokio::spawn(crate::run_group_bootstrap(
        1,
        first.raft_handle(),
        first_gate,
        BTreeMap::from([(1, BasicNode::new("one")), (2, BasicNode::new("two"))]),
        move |_id, _address| {
            let peer = peer.clone();
            let observations = observations.clone();
            async move {
                let response = peer.screen_vote(&crate::bootstrap_probe_vote()).unwrap();
                let state = crate::PeerGroupLog::from_vote_response(&response);
                observations.send(state).unwrap();
                Some(state)
            }
        },
        Duration::from_millis(1),
        Duration::from_secs(5),
    ));
    // Seeing a second probe proves the first Unprepared answer was processed.
    for _ in 0..2 {
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(2), observed.recv())
                .await
                .unwrap(),
            Some(crate::PeerGroupLog::Unprepared)
        );
    }
    assert!(!first.raft.is_initialized().await.unwrap());
    assert_eq!(
        second_store.read_vote().await.unwrap(),
        None,
        "read-only probe cannot create a vote floor"
    );
    second_gate
        .establish_vote_floor(UrsulaVote::new(0, 0))
        .await
        .unwrap();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), bootstrap)
            .await
            .unwrap()
            .unwrap(),
        crate::GroupBootstrap::Initialized
    );
    assert!(first.raft.is_initialized().await.unwrap());
    first.shutdown().await.unwrap();
    second.shutdown().await.unwrap();
}

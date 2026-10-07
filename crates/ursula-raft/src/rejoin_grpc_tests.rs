//! Recovery fault schedules over real TCP/gRPC, retaining the actual Raft handlers.

use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
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
    legacy: Arc<AtomicBool>,
    unknown_rpc_requests: Arc<AtomicUsize>,
    // 1 arms a term change after the legacy HEAD; 2 corrupts only its
    // following Vote response, without changing the actual Raft handlers.
    legacy_vote_change: Arc<AtomicUsize>,
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
        let mut response = self.inner.vote(request).await?;
        if self.legacy.load(Ordering::SeqCst) {
            response
                .metadata_mut()
                .remove(crate::grpc::REJOIN_BARRIER_CAPABILITY);
        }
        if self
            .legacy_vote_change
            .compare_exchange(2, 0, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
        {
            let mut vote: UrsulaVoteResponse =
                decode_wire(&response.get_ref().payload, "test vote").unwrap();
            vote.vote = UrsulaVote::new_committed(
                vote.vote.leader_id().term().saturating_add(1),
                *vote.vote.leader_id().node_id(),
            );
            response.get_mut().payload = encode_wire(&vote);
        }
        Ok(response)
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
        let response = self.inner.group_read(request).await;
        let _armed =
            self.legacy_vote_change
                .compare_exchange(1, 2, Ordering::SeqCst, Ordering::SeqCst);
        response
    }

    async fn rejoin_barrier(
        &self,
        request: Request<pb::RejoinBarrierRequestV1>,
    ) -> Result<Response<pb::RejoinBarrierResponseV1>, Status> {
        if self.legacy.load(Ordering::SeqCst) {
            return Err(Status::unimplemented("0.6.2 has no explicit barrier RPC"));
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

async fn new_recovery_engine(
    id: u64,
    config: Arc<Config>,
    registry: &RaftGroupHandleRegistry,
) -> (RaftGroupEngine, Arc<RaftGroupLogStore>, Arc<GroupRejoin>) {
    let gate = Arc::new(GroupRejoin::volatile(id, placement().raft_group_id));
    let store = RaftGroupLogStore::shared();
    let engine = RaftGroupEngine::new_node_with_log_store_and_network(
        placement(),
        id,
        config,
        GrpcRaftNetworkFactory::new(placement().raft_group_id).with_rejoin(Some(gate.clone())),
        store.clone(),
        None,
        None,
    )
    .await
    .expect("new memory engine");
    gate.bind(&engine.raft_handle());
    registry.register_rejoin(placement().raft_group_id, gate.clone());
    registry.register_read_barrier(placement().raft_group_id, engine.read_barrier.clone());
    registry.register(placement(), engine.raft_handle());
    (engine, store, gate)
}

#[tokio::test]
async fn delayed_pre_restart_append_cannot_restore_voting_or_campaigning_over_grpc() {
    let config = Arc::new(
        Config {
            cluster_name: "recovery-delayed-grpc".to_owned(),
            heartbeat_interval: 10,
            election_timeout_min: 50,
            election_timeout_max: 100,
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
    for id in 1..=3 {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        endpoints.push(format!("http://{}", listener.local_addr().unwrap()));
        let registry = RaftGroupHandleRegistry::default();
        let service = RecoveryTestService {
            inner: RaftGrpcService::new(registry.clone()),
            pause_replication: Arc::new(AtomicBool::new(false)),
            legacy: Arc::new(AtomicBool::new(false)),
            unknown_rpc_requests: Arc::new(AtomicUsize::new(0)),
            legacy_vote_change: Arc::new(AtomicUsize::new(0)),
        };
        let wire_service = pb::raft_internal_server::RaftInternalServer::new(service.clone())
            .accept_compressed(tonic::codec::CompressionEncoding::Zstd);
        let mux_state = service.clone();
        let app =
            axum::Router::new()
                .fallback_service(wire_service)
                .layer(axum::middleware::from_fn(
                    move |request: axum::extract::Request, next: axum::middleware::Next| {
                        let state = mux_state.clone();
                        async move {
                            use axum::response::IntoResponse;
                            // Model the 0.6.2 production mux: an unknown method reaches
                            // the /{bucket}/{stream} HTTP append handler, not tonic's
                            // Unimplemented handler. A successful recovery must never
                            // send this potentially mutating request to that version.
                            if state.legacy.load(Ordering::SeqCst)
                                && request.uri().path()
                                    == "/ursula.raft.v1.RaftInternal/RejoinBarrier"
                            {
                                state.unknown_rpc_requests.fetch_add(1, Ordering::SeqCst);
                                return (axum::http::StatusCode::BAD_REQUEST, "InvalidBucketId")
                                    .into_response();
                            }
                            next.run(request).await
                        }
                    },
                ));
        servers.push(tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        }));
        let (engine, store, gate) = new_recovery_engine(id, config.clone(), &registry).await;
        registries.push(registry);
        services.push(service);
        engines.push(engine);
        stores.push(store);
        gates.push(gate);
    }
    let nodes = endpoints
        .iter()
        .enumerate()
        .map(|(index, endpoint)| (u64::try_from(index).unwrap() + 1, BasicNode::new(endpoint)))
        .collect::<BTreeMap<_, _>>();
    // This fixture represents an initial bootstrap after proving every voter
    // empty and the object-store initialized marker absent.
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
    let proof = probe_rejoin_vote_barrier(placement(), 1, 2, &endpoints[1], Duration::from_secs(1))
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
    services[0].pause_replication.store(true, Ordering::SeqCst);
    engines[0].shutdown().await.unwrap();
    let (fresh, store, gate) = new_recovery_engine(1, config.clone(), &registries[0]).await;
    engines[0] = fresh;
    stores[0] = store;
    gates[0] = gate;
    let channel = tonic::transport::Endpoint::from_shared(endpoints[0].clone())
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut client = pb::raft_internal_client::RaftInternalClient::new(channel);
    client
        .append(Request::new(pb::RaftRpcEnvelopeV1 {
            raft_group_id: placement().raft_group_id.0,
            node_id: 1,
            protocol_version: ursula_stream::FORMAT_EPOCH,
            payload: encode_wire(&delayed),
        }))
        .await
        .expect("delayed valid Append over TCP");
    engines[0]
        .raft
        .wait(Some(Duration::from_secs(5)))
        .applied_index_at_least(Some(baseline.log_id.index()), "delayed prefix applied")
        .await
        .unwrap();
    assert!(!gates[0].vote_gate_open());
    assert!(!registries[0].recovery_barriers_ready());
    let term = engines[0].raft.metrics().borrow_watched().current_term;
    engines[0].raft.runtime_config().tick(true);
    tokio::time::sleep(Duration::from_millis(350)).await;
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
    engines[2].raft.trigger().elect().await.unwrap();
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_ne!(
        engines[2].raft.metrics().borrow_watched().current_leader,
        Some(3),
        "lagging C cannot win using restarted A"
    );
    for legacy in [false, true] {
        services[1].legacy.store(legacy, Ordering::SeqCst);
        assert!(
            probe_rejoin_vote_barrier(placement(), 1, 2, &endpoints[1], Duration::from_millis(150))
                .await
                .is_err(),
            "capability and low-term Vote cannot replace a fresh quorum proof"
        );
        assert_eq!(services[1].unknown_rpc_requests.load(Ordering::SeqCst), 0);
        crate::confirm_quorum_prefix(placement(), 2, &endpoints[1], Duration::from_millis(150))
            .await
            .expect_err("a stale or legacy peer cannot confirm a quorum prefix");
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
    services[1].legacy.store(true, Ordering::SeqCst);
    services[1].legacy_vote_change.store(1, Ordering::SeqCst);
    let changed_vote =
        crate::confirm_quorum_prefix(placement(), 2, &endpoints[1], Duration::from_secs(1))
            .await
            .unwrap_err();
    assert!(changed_vote.contains("changed its vote"), "{changed_vote}");
    for legacy in [false, true, false] {
        services[1].legacy.store(legacy, Ordering::SeqCst);
        if legacy {
            let mut raw =
                pb::raft_internal_client::RaftInternalClient::connect(endpoints[1].clone())
                    .await
                    .unwrap();
            let error = raw
                .rejoin_barrier(pb::RejoinBarrierRequestV1 {
                    raft_group_id: placement().raft_group_id.0,
                    protocol_version: ursula_stream::FORMAT_EPOCH,
                })
                .await
                .unwrap_err();
            assert_eq!(
                error.code(),
                tonic::Code::Internal,
                "legacy mux HTTP 400 is not gRPC Unimplemented"
            );
            assert_eq!(
                services[1].unknown_rpc_requests.swap(0, Ordering::SeqCst),
                1
            );
        }
        let proof =
            probe_rejoin_vote_barrier(placement(), 1, 2, &endpoints[1], Duration::from_secs(1))
                .await
                .expect("fresh recovery proof, including legacy bridge");
        assert!(proof.1 >= acked.log_id.index());
        let observed =
            crate::confirm_quorum_prefix(placement(), 2, &endpoints[1], Duration::from_secs(1))
                .await
                .expect("fresh maintenance observation through the same probe");
        assert_eq!(observed.leader_id, 2);
        assert_eq!(observed.raft_group_id, placement().raft_group_id.0);
        assert!(observed.required_applied_index >= acked.log_id.index());
        gates[0].confirm_barrier(proof.0, proof.1);
        assert!(
            !gates[0].try_open().await.unwrap(),
            "proof must also be applied"
        );
        assert_eq!(services[1].unknown_rpc_requests.load(Ordering::SeqCst), 0);
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
    tokio::time::sleep(Duration::from_millis(350)).await;
    assert_eq!(
        engines[0].raft.metrics().borrow_watched().current_term,
        term,
        "recovery cannot clear maintenance and enable campaigning"
    );
    services[0].pause_replication.store(false, Ordering::SeqCst);
    registries[0].clear_leadership_shed(LeadershipShedReason::MaintenanceDrain);

    // A second loss after full repair starts a new closed gate. Changing the
    // healthy leader must neither reuse A's previous proof nor accept a
    // forwarded legacy HEAD as proof of the stale peer's own log.
    services[0].pause_replication.store(true, Ordering::SeqCst);
    engines[0].shutdown().await.unwrap();
    let (fresh, store, gate) = new_recovery_engine(1, config, &registries[0]).await;
    engines[0] = fresh;
    stores[0] = store;
    gates[0] = gate;
    client
        .append(Request::new(pb::RaftRpcEnvelopeV1 {
            raft_group_id: placement().raft_group_id.0,
            node_id: 1,
            protocol_version: ursula_stream::FORMAT_EPOCH,
            payload: encode_wire(&delayed),
        }))
        .await
        .unwrap();
    engines[0]
        .raft
        .wait(Some(Duration::from_secs(5)))
        .applied_index_at_least(Some(baseline.log_id.index()), "second stale prefix")
        .await
        .unwrap();
    registries[0].mark_leadership_shed(LeadershipShedReason::MaintenanceDrain);
    registries[0].clear_leadership_shed(LeadershipShedReason::MaintenanceDrain);
    engines[0].raft.runtime_config().tick(true);
    let term = engines[0].raft.metrics().borrow_watched().current_term;
    engines[1].raft.runtime_config().tick(false);
    tokio::time::sleep(Duration::from_millis(350)).await;
    assert_eq!(
        engines[0].raft.metrics().borrow_watched().current_term,
        term,
        "undrain cannot bypass a new recovery gate"
    );
    engines[2].raft.trigger().elect().await.unwrap();
    engines[2]
        .raft
        .wait(Some(Duration::from_secs(5)))
        .current_leader(3, "healthy C becomes leader")
        .await
        .unwrap();
    engines[2].raft.runtime_config().tick(true);
    services[1].legacy.store(true, Ordering::SeqCst);
    assert!(
        probe_rejoin_vote_barrier(placement(), 1, 2, &endpoints[1], Duration::from_secs(1))
            .await
            .is_err(),
        "a forwarded HEAD is not evidence of that peer's own prefix"
    );
    // The leader metric may precede application of the new term's blank entry.
    // A transient ReadIndex refusal must leave the restarted voter ineligible;
    // the production recovery driver also retries fresh outbound proofs.
    let proof_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let proof = loop {
        match probe_rejoin_vote_barrier(placement(), 1, 3, &endpoints[2], Duration::from_secs(1))
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
    for id in 1..=3 {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        endpoints.push(format!("http://{}", listener.local_addr().unwrap()));
        let registry = RaftGroupHandleRegistry::default();
        let service = RecoveryTestService {
            inner: RaftGrpcService::new(registry.clone()),
            pause_replication: Arc::new(AtomicBool::new(false)),
            legacy: Arc::new(AtomicBool::new(false)),
            unknown_rpc_requests: Arc::new(AtomicUsize::new(0)),
            legacy_vote_change: Arc::new(AtomicUsize::new(0)),
        };
        let wire = pb::raft_internal_server::RaftInternalServer::new(service.clone())
            .accept_compressed(tonic::codec::CompressionEncoding::Zstd);
        servers.push(tokio::spawn(async move {
            axum::serve(listener, axum::Router::new().fallback_service(wire))
                .await
                .unwrap();
        }));
        let (engine, _, gate) = new_recovery_engine(id, config.clone(), &registry).await;
        services.push(service);
        engines.push(engine);
        gates.push(gate);
    }
    let nodes = endpoints
        .iter()
        .enumerate()
        .map(|(index, endpoint)| (u64::try_from(index).unwrap() + 1, BasicNode::new(endpoint)))
        .collect::<BTreeMap<_, _>>();
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

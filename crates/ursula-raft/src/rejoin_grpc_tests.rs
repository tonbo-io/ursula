//! Recovery fault schedules over real TCP/gRPC, retaining the actual Raft handlers.

use std::sync::Mutex;
use std::sync::PoisonError;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

use openraft::RaftMetrics;
use openraft::rt::Instant as _;
use openraft::rt::WatchReceiver;
use openraft::type_config::alias::WatchReceiverOf;
use tonic::Request;
use tonic::Response;
use tonic::Status;
use ursula_runtime::LeadershipRefusal;
use ursula_runtime::Proposal;

use super::*;
use crate::grpc::GrpcRaftNetworkFactory;
use crate::grpc::RaftGrpcService;
use crate::grpc::RecoveryProbeError;
use crate::grpc::probe_rejoin_vote_barrier;
use crate::raft_internal_proto as pb;
use crate::raft_internal_proto::raft_internal_server::RaftInternal;
use crate::rejoin::GroupRejoin;

#[derive(Clone)]
struct RecoveryTestService {
    inner: RaftGrpcService,
    pause_replication: Arc<AtomicBool>,
    reject_barrier: Arc<AtomicBool>,
    answers: Arc<Mutex<AppendAnswers>>,
}

impl RecoveryTestService {
    fn new(registry: RaftGroupHandleRegistry) -> Self {
        Self {
            inner: RaftGrpcService::new(registry),
            pause_replication: Arc::default(),
            reject_barrier: Arc::default(),
            answers: Arc::default(),
        }
    }
}

/// How a fixture node answered the Append calls it received, recorded when
/// its service produced each answer. OpenRaft's leader metrics do not count
/// the heartbeats of a ReadIndex round, so only this record tells a follower
/// whose answers reach the leader too late from one that does not answer.
#[derive(Default)]
struct AppendAnswers {
    acks: u64,
    last_ack: Option<tokio::time::Instant>,
    refusals: u64,
    last_refusal: Option<tokio::time::Instant>,
}

impl AppendAnswers {
    fn record(answers: &Mutex<Self>, acked: bool) {
        let mut answers = answers.lock().unwrap_or_else(PoisonError::into_inner);
        let now = Some(tokio::time::Instant::now());
        if acked {
            answers.acks = answers.acks.saturating_add(1);
            answers.last_ack = now;
        } else {
            answers.refusals = answers.refusals.saturating_add(1);
            answers.last_refusal = now;
        }
    }
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
                    AppendAnswers::record(&service.answers, result.is_ok());
                    use pb::raft_append_stream_response_item::Result as WireResult;
                    items.push(pb::RaftAppendStreamResponseItem {
                        request_id: item.request_id,
                        result: Some(match result {
                            Ok(ack) => WireResult::Ack(ack),
                            Err(err) => WireResult::Error(crate::grpc::stream_item_error(&err)),
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

    async fn genesis_probe(
        &self,
        request: Request<pb::GenesisProbeRequestV1>,
    ) -> Result<Response<pb::GenesisProbeAckV1>, Status> {
        self.inner.genesis_probe(request).await
    }
}

/// `raft.peers` of a fixture whose node `i + 1` serves at `endpoints[i]`.
fn fixture_peers(endpoints: &[String]) -> crate::PeerDirectory {
    crate::PeerDirectory::new(
        (1_u64..)
            .zip(endpoints)
            .map(|(id, endpoint)| (id, crate::PeerEndpoints::shared(endpoint.as_str()))),
    )
}

/// Lists `endpoints` as the `raft.peers` of every node of `registries`
/// (node `i + 1` holds `registries[i]`).
fn list_fixture_peers(registries: &[RaftGroupHandleRegistry], endpoints: &[String]) {
    for (id, registry) in (1_u64..).zip(registries) {
        registry.peer_cluster().bind(
            crate::peer_cluster::test_identity(id),
            fixture_peers(endpoints),
        );
    }
}

/// A transport that probes the fixture nodes at `endpoints`.
fn probe_transport(endpoints: &[String]) -> Arc<crate::grpc::CoreRaftTransport> {
    Arc::new(crate::grpc::CoreRaftTransport::new(
        crate::peer_cluster::test_identity(0),
        crate::peer_cluster::test_peer_cluster_with(0, fixture_peers(endpoints)),
    ))
}

/// A replica of node `id` on a fresh, empty WAL, as a new node or one that
/// lost its disk starts: its gate is closed with the group's history
/// unknown. The returned directory holds the WAL and must outlive the store.
/// It dials its peers at the `raft.peers` that `registry`'s peer cluster
/// lists ([`list_fixture_peers`]).
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
    let peer_cluster = registry.peer_cluster().clone();
    if peer_cluster.identity().is_none() {
        peer_cluster.bind(
            crate::peer_cluster::test_identity(id),
            crate::PeerDirectory::default(),
        );
    }
    let transport = Arc::new(crate::grpc::CoreRaftTransport::new(
        crate::peer_cluster::test_identity(id),
        peer_cluster.clone(),
    ));
    // The gate goes in as the node's engine factory hands it over, so the
    // group runs with log reversion allowed.
    let engine = RaftGroupEngine::new_node_with_log_store_network_and_options(
        placement(),
        id,
        config,
        GrpcRaftNetworkFactory::new(transport, placement().raft_group_id),
        store.clone(),
        RaftGroupEngineOptions {
            recovery: Some(gate.clone()),
            peer_cluster: Some(peer_cluster),
            ..Default::default()
        },
    )
    .await
    .expect("new engine on an empty WAL");
    engine.publish_recovery(gate.clone(), registry).unwrap();
    (engine, store, gate, wal_root)
}

/// Serves `service` over gRPC on a new loopback listener until the returned
/// task is aborted, and returns the listener's endpoint. It serves through
/// production's [`ursula_observability::serve::serve_until_shutdown`], which
/// sets `TCP_NODELAY` on every socket it accepts: with the fixtures' 10 ms
/// ReadIndex rounds, a delayed reply can otherwise keep every round short of
/// a quorum (#521, #547).
async fn serve_fixture(service: RecoveryTestService) -> (String, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    // Production's server: the cluster-id check runs before the fixture.
    let wire = crate::grpc::raft_grpc_server(service, &crate::peer_cluster::test_peer_cluster(0));
    let server = tokio::spawn(async move {
        ursula_observability::serve::serve_until_shutdown(
            listener,
            axum::Router::new().fallback_service(wire),
            std::future::pending(),
            None,
        )
        .await
        .unwrap();
    });
    (endpoint, server)
}

/// How long a fixture step retries a failure that a production client
/// retries.
const TRANSIENT_RETRY_WINDOW: Duration = Duration::from_secs(5);
/// The pause between two attempts of a fixture step.
const RETRY_PAUSE: Duration = Duration::from_millis(10);
/// The deadline of one fresh quorum probe.
const PROBE_TIMEOUT: Duration = Duration::from_secs(1);

/// How one attempt of a retried fixture step failed.
enum AttemptError {
    /// A failure that a production client retries.
    Transient(String),
    /// Any other failure: the step fails at once.
    Fatal(String),
}

type MetricsWatch = WatchReceiverOf<UrsulaRaftTypeConfig, RaftMetrics<UrsulaRaftTypeConfig>>;

/// What a retried fixture step reports about one replica when it gives up.
struct ReplicaWatch {
    metrics: MetricsWatch,
    answers: Arc<Mutex<AppendAnswers>>,
}

/// Each replica's metrics and Append answers, readable while an attempt
/// holds the replica.
fn replica_watches(
    engines: &[RaftGroupEngine],
    services: &[RecoveryTestService],
) -> Vec<ReplicaWatch> {
    engines
        .iter()
        .zip(services)
        .map(|(engine, service)| ReplicaWatch {
            metrics: engine.raft.metrics(),
            answers: service.answers.clone(),
        })
        .collect()
}

/// Runs `attempt` until it succeeds. `attempt` gets the time left in
/// [`TRANSIENT_RETRY_WINDOW`], and a transient failure is retried until the
/// window closes. A fatal failure, or a transient one once the window has
/// closed, panics with every attempt's error and the state of each replica
/// in `replicas` ([`replica_states`]).
async fn retry_transient<T>(
    step: &str,
    replicas: &[ReplicaWatch],
    mut attempt: impl AsyncFnMut(Duration) -> Result<T, AttemptError>,
) -> T {
    let began = tokio::time::Instant::now();
    let mut failures = Vec::new();
    loop {
        let remaining = TRANSIENT_RETRY_WINDOW.saturating_sub(began.elapsed());
        let (retry, error) = match attempt(remaining).await {
            Ok(value) => return value,
            Err(AttemptError::Transient(error)) => (true, format!("transient: {error}")),
            Err(AttemptError::Fatal(error)) => (false, format!("fatal: {error}")),
        };
        failures.push(format!("  +{:.1?} {error}", began.elapsed()));
        if !retry || began.elapsed().saturating_add(RETRY_PAUSE) >= TRANSIENT_RETRY_WINDOW {
            let attempts = match failures.len() {
                1 => "1 attempt".to_owned(),
                count => format!("{count} attempts"),
            };
            panic!(
                "{step}: no success after {attempts} over {:.1?}\n\
                 attempts, by when each ended:\n{}\n\
                 replica state:\n{}",
                began.elapsed(),
                failures.join("\n"),
                replica_states(replicas),
            );
        }
        tokio::time::sleep(RETRY_PAUSE).await;
    }
}

/// A fresh quorum proof from leader `leader_id` of the fixture nodes at
/// `endpoints`, retried as a recovery driver retries it
/// ([`probe_attempt_error`]).
async fn fresh_proof(
    step: &str,
    replicas: &[ReplicaWatch],
    leader_id: u64,
    endpoints: &[String],
) -> (UrsulaVote, u64) {
    retry_transient(step, replicas, async |remaining| {
        probe_rejoin_vote_barrier(
            probe_transport(endpoints),
            placement(),
            leader_id,
            PROBE_TIMEOUT.min(remaining),
        )
        .await
        .map(|answer| (answer.vote, answer.index))
        .map_err(probe_attempt_error)
    })
    .await
}

/// One line per replica: its role, the leader it knows, its vote, its last
/// log and applied indexes, and its Append answers ([`AppendAnswers`]). On a
/// leader the line adds how long ago a quorum last acknowledged it, and each
/// follower's matched index and last acknowledgement. These times come from
/// OpenRaft's replication and heartbeat acknowledgements. A ReadIndex round
/// does not update them, so a follower whose answers arrive late and one that
/// does not answer look the same there. The Append answers tell them apart:
/// a follower that keeps producing acks while no round reaches a quorum has
/// its answers reach the leader too late.
fn replica_states(replicas: &[ReplicaWatch]) -> String {
    replicas
        .iter()
        .map(|replica| {
            let metrics = replica.metrics.borrow_watched();
            let leader_id = metrics.vote.leader_id();
            let mut line = format!(
                "  node {}: {:?}, leader {:?}, vote term {} node {} committed {}, last log {:?}, \
                 applied {:?}",
                metrics.id,
                metrics.state,
                metrics.current_leader,
                leader_id.term(),
                leader_id.node_id(),
                metrics.vote.is_committed(),
                metrics.last_log_index,
                metrics.last_applied.as_ref().map(|log_id| log_id.index()),
            );
            {
                let answers = replica
                    .answers
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner);
                line.push_str(&format!(
                    ", Append answers: acks {}, refusals {}",
                    answer_count(answers.acks, answers.last_ack),
                    answer_count(answers.refusals, answers.last_refusal),
                ));
            }
            if let Some(replication) = &metrics.replication {
                let followers = replication
                    .iter()
                    .filter(|(node, _)| **node != metrics.id)
                    .map(|(node, matched)| {
                        let acked = metrics
                            .heartbeat
                            .as_ref()
                            .and_then(|heartbeat| heartbeat.get(node))
                            .and_then(Option::as_ref)
                            .map(|acked| acked.elapsed());
                        format!(
                            "{node} matched {:?} acked {}",
                            matched.as_ref().map(|log_id| log_id.index()),
                            ago(acked),
                        )
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                let quorum_acked = metrics
                    .last_quorum_acked
                    .as_ref()
                    .map(|acked| acked.elapsed());
                line.push_str(&format!(
                    ", quorum acked {}, followers [{followers}]",
                    ago(quorum_acked)
                ));
            }
            line
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// How long ago something was last acknowledged, or `never`.
fn ago(elapsed: Option<Duration>) -> String {
    elapsed.map_or_else(
        || "never".to_owned(),
        |elapsed| format!("{elapsed:.1?} ago"),
    )
}

/// A count of answers, with how long ago the last of them was produced.
fn answer_count(count: u64, last: Option<tokio::time::Instant>) -> String {
    match last {
        Some(last) => format!("{count} (last {:.1?} ago)", last.elapsed()),
        None => count.to_string(),
    }
}

/// A failed probe's error, a gRPC error as its code and message.
fn probe_error(error: &RecoveryProbeError) -> String {
    match error {
        RecoveryProbeError::Rpc(status) => format!("{:?}: {}", status.code(), status.message()),
        other => other.to_string(),
    }
}

/// The probe failures a recovery driver retries: a leader whose ReadIndex
/// rounds reached no quorum answers unavailable, and a probe can reach its
/// own deadline first (#521).
fn probe_attempt_error(error: RecoveryProbeError) -> AttemptError {
    let text = probe_error(&error);
    match &error {
        RecoveryProbeError::Rpc(status)
            if matches!(
                status.code(),
                tonic::Code::Unavailable | tonic::Code::Cancelled | tonic::Code::DeadlineExceeded
            ) =>
        {
            AttemptError::Transient(text)
        }
        _ => AttemptError::Fatal(text),
    }
}

/// The read failure a production client retries: the leader-unknown refusal
/// raised before anything was proposed, which the HTTP layer answers with a
/// retryable 503. A leader that could not confirm its leadership with a
/// quorum in time raises it (#547). So do a leader that did not apply the
/// read index in time, an OpenRaft forward that names no leader, and a
/// follower that could not reach the leader. Any other failure is final.
fn read_attempt_error(error: GroupEngineError) -> AttemptError {
    let text = format!("{error:?}");
    match error {
        GroupEngineError::NotLeader(LeadershipRefusal {
            leader: None,
            proposal: Proposal::NotProposed,
            ..
        }) => AttemptError::Transient(text),
        _ => AttemptError::Fatal(text),
    }
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
    other.raft.initialize(BTreeSet::from([1])).await.unwrap();
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
        let registry = RaftGroupHandleRegistry::default();
        let service = RecoveryTestService::new(registry.clone());
        let (endpoint, server) = serve_fixture(service.clone()).await;
        endpoints.push(endpoint);
        servers.push(server);
        let (engine, store, gate, wal_root) =
            new_recovery_engine(id, config.clone(), &registry).await;
        registries.push(registry);
        services.push(service);
        engines.push(engine);
        stores.push(store);
        gates.push(gate);
        wal_roots.push(wal_root);
    }
    list_fixture_peers(&registries, &endpoints);
    let nodes = (1..=u64::try_from(endpoints.len()).unwrap()).collect::<BTreeSet<_>>();
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
    wait_history(&engines[1]).await;
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
    // Establish the fixture with an actual fresh quorum proof. A probe can
    // fail before any fault is injected when none of the leader's ReadIndex
    // rounds reaches a quorum in time: the leader then answers unavailable,
    // or the probe reaches its own deadline first (#521).
    let proof = fresh_proof(
        "fresh explicit barrier",
        &replica_watches(&engines, &services),
        2,
        &endpoints,
    )
    .await;
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
    let mut client = pb::raft_internal_client::RaftInternalClient::with_interceptor(
        channel,
        crate::peer_cluster::test_identity(2),
    );
    let rejected = client
        .append(Request::new(pb::RaftRpcEnvelopeV1 {
            plane: pb::RaftPlaneV1::Data.into(),
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
                plane: pb::RaftPlaneV1::Data.into(),
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
                probe_transport(&endpoints),
                placement(),
                2,
                Duration::from_millis(150)
            )
            .await
            .is_err(),
            "an unreachable quorum cannot confirm a fresh proof"
        );
        crate::confirm_quorum_prefix(
            probe_transport(&endpoints),
            placement(),
            2,
            Duration::from_millis(150),
        )
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
    let unsupported =
        probe_rejoin_vote_barrier(probe_transport(&endpoints), placement(), 2, PROBE_TIMEOUT)
            .await
            .expect_err("unsupported recovery protocol is refused");
    assert!(
        matches!(unsupported, crate::grpc::RecoveryProbeError::Rpc(status) if status.code() == tonic::Code::Unimplemented)
    );
    services[1].reject_barrier.store(false, Ordering::SeqCst);
    {
        // A stays paused, so B's ReadIndex rounds need C's answers for a
        // quorum, as in the fixture probe. These probes retry the same
        // refusals.
        let proof = fresh_proof(
            "fresh recovery proof",
            &replica_watches(&engines, &services),
            2,
            &endpoints,
        )
        .await;
        assert!(proof.1 >= acked.log_id.index());
        let observed = retry_transient(
            "fresh maintenance observation through the same probe",
            &replica_watches(&engines, &services),
            async |remaining| {
                crate::confirm_quorum_prefix(
                    probe_transport(&endpoints),
                    placement(),
                    2,
                    PROBE_TIMEOUT.min(remaining),
                )
                .await
                .map_err(probe_attempt_error)
            },
        )
        .await;
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
            plane: pb::RaftPlaneV1::Data.into(),
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
        probe_rejoin_vote_barrier(probe_transport(&endpoints), placement(), 2, PROBE_TIMEOUT)
            .await
            .is_err(),
        "an unsupported peer cannot supply a fresh proof"
    );
    // The leader metric may precede application of the new term's blank entry.
    // A transient ReadIndex refusal must leave the restarted voter ineligible;
    // the production recovery driver also retries fresh outbound proofs.
    let proof = retry_transient(
        "fresh proof from new leader C",
        &replica_watches(&engines, &services),
        async |remaining| {
            probe_rejoin_vote_barrier(
                probe_transport(&endpoints),
                placement(),
                3,
                PROBE_TIMEOUT.min(remaining),
            )
            .await
            .map(|answer| (answer.vote, answer.index))
            .map_err(|error| {
                assert!(
                    !gates[0].vote_gate_open(),
                    "failed proof cannot open the gate"
                );
                AttemptError::Transient(probe_error(&error))
            })
        },
    )
    .await;
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

/// Three replicas over gRPC, led by the second (`engines[1]`), whose
/// first (`engines[0]`) a test can make lag with its service's
/// `pause_replication`.
struct ThreeReplicas {
    engines: Vec<RaftGroupEngine>,
    services: Vec<RecoveryTestService>,
    servers: Vec<tokio::task::JoinHandle<()>>,
    _gates: Vec<Arc<GroupRejoin>>,
    _wal_roots: Vec<tempfile::TempDir>,
}

impl ThreeReplicas {
    async fn shutdown(self) {
        shutdown_all(&self.engines).await;
        for server in self.servers {
            server.abort();
        }
    }
}

async fn three_replicas_led_by_the_second(cluster_name: &str) -> ThreeReplicas {
    let config = Arc::new(
        Config {
            cluster_name: cluster_name.to_owned(),
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
    let mut registries = Vec::new();
    let mut endpoints = Vec::new();
    let mut services = Vec::new();
    let mut servers = Vec::new();
    let mut engines = Vec::new();
    let mut gates = Vec::new();
    let mut wal_roots = Vec::new();
    for id in 1..=3 {
        let registry = RaftGroupHandleRegistry::default();
        let service = RecoveryTestService::new(registry.clone());
        let (endpoint, server) = serve_fixture(service.clone()).await;
        endpoints.push(endpoint);
        servers.push(server);
        let (engine, _, gate, wal_root) = new_recovery_engine(id, config.clone(), &registry).await;
        registries.push(registry);
        services.push(service);
        engines.push(engine);
        gates.push(gate);
        wal_roots.push(wal_root);
    }
    list_fixture_peers(&registries, &endpoints);
    let nodes = (1..=u64::try_from(endpoints.len()).unwrap()).collect::<BTreeSet<_>>();
    for gate in &gates {
        gate.establish_vote_floor(UrsulaVote::new(0, 0))
            .await
            .unwrap();
    }
    gates[1].allow_fresh_bootstrap().await.unwrap();
    engines[1].raft.runtime_config().elect(true);
    engines[1].raft.initialize(nodes).await.unwrap();
    // Ticks are off, so an election that a loaded host lets time out is not
    // retried on its own: trigger another until the second replica leads.
    let mut attempts = 0_u32;
    while engines[1]
        .raft
        .wait(Some(Duration::from_secs(5)))
        .current_leader(2, "initial leader")
        .await
        .is_err()
    {
        attempts = attempts.saturating_add(1);
        assert!(attempts < 6, "the second replica never won an election");
        engines[1].raft.trigger().elect(false).await.unwrap();
    }
    ThreeReplicas {
        engines,
        services,
        servers,
        _gates: gates,
        _wal_roots: wal_roots,
    }
}

/// ACKs are quorum-replicated, not necessarily applied on the third replica.
/// A catch-up cursor ahead of that replica must be checked at the leader.
#[tokio::test]
async fn lagging_follower_reads_acknowledged_cursor_over_grpc() {
    let ThreeReplicas {
        mut engines,
        services,
        servers,
        _gates,
        _wal_roots,
    } = three_replicas_led_by_the_second("stale-follower-cursor-grpc").await;
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
    match write_result_from_raft_response(acked.data, ursula_runtime::ResponseKind::Append).unwrap()
    {
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
    // The follower forwards the read to the leader, which confirms that it
    // still leads with a ReadIndex round. When no round reaches a quorum in
    // time it refuses, and a client retries that refusal (#547).
    let replicas = replica_watches(&engines, &services);
    let read = retry_transient(
        "read acknowledged cursor through lagging follower",
        &replicas,
        async |_remaining| {
            engines[0]
                .read_stream(
                    ReadStreamRequest {
                        offset: 14,
                        ..read_req(stream_id.clone(), 4)
                    },
                    placement(),
                )
                .await
                .map_err(read_attempt_error)
        },
    )
    .await;
    assert_eq!(read.payload, b"-ACK");
    assert_eq!(read.next_offset, 18);
    // Only a leader that confirmed its leadership certifies the tail, so this
    // read can meet the same refusal and retries it. Any outcome other than
    // that refusal or `OffsetOutOfRange` fails the test.
    retry_transient(
        "cursor beyond authoritative tail stays invalid",
        &replicas,
        async |_remaining| match engines[0]
            .read_stream(
                ReadStreamRequest {
                    offset: 19,
                    ..read_req(stream_id.clone(), 4)
                },
                placement(),
            )
            .await
        {
            Err(error) if error.code() == Some(StreamErrorCode::OffsetOutOfRange) => Ok(()),
            Err(error) => Err(read_attempt_error(error)),
            Ok(read) => Err(AttemptError::Fatal(format!("read succeeded: {read:?}"))),
        },
    )
    .await;
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
    assert!(unconfirmed.not_leader().is_some(), "{unconfirmed:?}");
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

/// A follower answers an immutable-read chunk only when the answer can be
/// marked (#581 section 5.3). A chunk that reaches the lagging follower's own
/// tail goes to the leader, so a client never takes a lagging replica's short
/// or tail-ending answer for the stream's.
#[tokio::test]
async fn lagging_follower_forwards_immutable_chunks_it_cannot_mark() {
    let mut cluster = three_replicas_led_by_the_second("immutable-chunk-follower").await;
    let stream_id = bsid("immutable-chunk");
    create_stream_via_raft(&cluster.engines[1], stream_id.clone()).await;
    let initial = cluster.engines[1]
        .raft
        .client_write(append_command(stream_id.clone(), b"old-prefix"))
        .await
        .unwrap();
    cluster.engines[1].raft.trigger().heartbeat().await.unwrap();
    for engine in &cluster.engines {
        engine
            .raft
            .wait(Some(Duration::from_secs(5)))
            .applied_index_at_least(Some(initial.log_id.index()), "initial prefix applied")
            .await
            .unwrap();
    }
    // The first replica stops at offset 10; the stream grows to 18.
    cluster.services[0]
        .pause_replication
        .store(true, Ordering::SeqCst);
    cluster.engines[1]
        .raft
        .client_write(append_command(stream_id.clone(), b"-gap-ACK"))
        .await
        .unwrap();
    let chunk = |max_len, follower| ReadStreamRequest {
        follower,
        ..read_req(stream_id.clone(), max_len)
    };

    // A base-protocol read is served from the follower's own prefix.
    let prefix = read_through_first(&mut cluster, chunk(12, FollowerRead::Prefix)).await;
    assert_eq!(prefix.payload, b"old-prefix");
    assert!(!prefix.is_immutable(12));

    // The same range as a chunk: the leader answers it, whole and markable.
    let forwarded = read_through_first(&mut cluster, chunk(12, FollowerRead::Immutable)).await;
    assert_eq!(forwarded.payload, b"old-prefix-g");
    assert_eq!(forwarded.next_offset, 12);
    assert!(forwarded.is_immutable(12));

    // A chunk that ends exactly at the follower's tail: the follower cannot
    // tell it from the stream's tail, so the leader answers.
    let at_follower_tail =
        read_through_first(&mut cluster, chunk(10, FollowerRead::Immutable)).await;
    assert_eq!(at_follower_tail.payload, b"old-prefix");
    assert!(
        at_follower_tail.is_immutable(10),
        "the leader's tail is beyond it: {at_follower_tail:?}"
    );

    // A chunk below the follower's tail is the follower's to answer.
    let below = read_through_first(&mut cluster, chunk(4, FollowerRead::Immutable)).await;
    assert_eq!(below.payload, b"old-");
    assert!(below.is_immutable(4));

    cluster.services[0]
        .pause_replication
        .store(false, Ordering::SeqCst);
    cluster.shutdown().await;
}

/// A follower that has not applied a delete and recreate still holds the
/// old incarnation, whose retained offset may be above an offset the new
/// one serves. Its `410` would be for the old stream, so it forwards the
/// read as it forwards a `404` or `416`, and the leader answers for the new
/// one.
#[tokio::test]
async fn lagging_follower_forwards_a_gone_offset_of_a_recreated_stream() {
    let mut cluster = three_replicas_led_by_the_second("gone-after-recreate").await;
    let stream_id = bsid("gone-after-recreate");
    create_stream_via_raft(&cluster.engines[1], stream_id.clone()).await;
    let mut last = None;
    for command in [
        append_command(stream_id.clone(), b"old-prefix"),
        RaftCommand::Stream(StreamCommand::from(
            ursula_runtime::PublishSnapshotRequest {
                stream_id: stream_id.clone(),
                snapshot_offset: 10,
                content_type: "application/octet-stream".to_owned(),
                payload: bytes::Bytes::from_static(b"snapshot"),
                cold_body: None,
                now_ms: 0,
                expected_incarnation: None,
                if_incarnation: None,
            },
        )),
        RaftCommand::Stream(StreamCommand::from(
            ursula_runtime::AdvanceRetentionRequest {
                stream_id: stream_id.clone(),
                retained_offset: 10,
                now_ms: 0,
                expected_incarnation: None,
                if_incarnation: None,
            },
        )),
    ] {
        last = Some(cluster.engines[1].raft.client_write(command).await.unwrap());
    }
    let retained = last.expect("written");
    cluster.engines[1].raft.trigger().heartbeat().await.unwrap();
    for engine in &cluster.engines {
        engine
            .raft
            .wait(Some(Duration::from_secs(5)))
            .applied_index_at_least(Some(retained.log_id.index()), "retention applied")
            .await
            .unwrap();
    }
    let old = read_through_first(&mut cluster, ReadStreamRequest {
        offset: 10,
        ..read_req(stream_id.clone(), 4)
    })
    .await
    .incarnation;

    // The first replica stops before the delete.
    cluster.services[0]
        .pause_replication
        .store(true, Ordering::SeqCst);
    for command in [
        RaftCommand::Stream(StreamCommand::from(ursula_runtime::DeleteStreamRequest {
            stream_id: stream_id.clone(),
            now_ms: 0,
            if_incarnation: None,
        })),
        create_command(stream_id.clone()),
        append_command(stream_id.clone(), b"new"),
    ] {
        cluster.engines[1].raft.client_write(command).await.unwrap();
    }
    for follower in [FollowerRead::Prefix, FollowerRead::Immutable] {
        let read = read_through_first(&mut cluster, ReadStreamRequest {
            follower,
            ..read_req(stream_id.clone(), 3)
        })
        .await;
        assert_eq!(read.payload, b"new", "{follower:?}");
        assert_ne!(read.incarnation, old, "{follower:?}");
    }
    // The leader's `416` comes back through the follower with the token of
    // the stream that decided it.
    let new = read_through_first(&mut cluster, read_req(stream_id.clone(), 3))
        .await
        .incarnation;
    let mut beyond = None;
    for _ in 0..50 {
        match cluster.engines[0]
            .read_stream(
                ReadStreamRequest {
                    offset: 99,
                    ..read_req(stream_id.clone(), 3)
                },
                placement(),
            )
            .await
        {
            Err(error) if error.code() == Some(StreamErrorCode::OffsetOutOfRange) => {
                beyond = Some(error);
                break;
            }
            Err(_transient) => tokio::time::sleep(Duration::from_millis(20)).await,
            Ok(read) => panic!("a read beyond the tail answered {read:?}"),
        }
    }
    let beyond = beyond.expect("the leader's 416");
    assert!(
        beyond
            .context()
            .contains(&ursula_runtime::StreamErrorContext::StreamIncarnation { incarnation: new }),
        "{beyond:?}"
    );
    cluster.services[0]
        .pause_replication
        .store(false, Ordering::SeqCst);
    cluster.shutdown().await;
}

/// A leader answers an immutable-read chunk it cannot mark, and a read that
/// would answer `404`, `410` or `416`, only after a ReadIndex round (#581
/// section 5.3): a leader that cannot reach a quorum may lag the group, as a
/// just-elected one may not have applied a recreate yet, so it refuses
/// rather than answer from that state. A chunk it can mark, and a
/// base-protocol read that succeeds, need no round.
#[tokio::test]
async fn a_leader_linearizes_what_may_hold_only_for_a_lagging_state() {
    let mut cluster = three_replicas_led_by_the_second("immutable-leader-barrier").await;
    let stream_id = bsid("immutable-leader-barrier");
    create_stream_via_raft(&cluster.engines[1], stream_id.clone()).await;
    let mut last = None;
    for command in [
        append_command(stream_id.clone(), b"old-prefix-tail-bytes"),
        RaftCommand::Stream(StreamCommand::from(
            ursula_runtime::PublishSnapshotRequest {
                stream_id: stream_id.clone(),
                snapshot_offset: 10,
                content_type: "application/octet-stream".to_owned(),
                payload: bytes::Bytes::from_static(b"snapshot"),
                cold_body: None,
                now_ms: 0,
                expected_incarnation: None,
                if_incarnation: None,
            },
        )),
        RaftCommand::Stream(StreamCommand::from(
            ursula_runtime::AdvanceRetentionRequest {
                stream_id: stream_id.clone(),
                retained_offset: 10,
                now_ms: 0,
                expected_incarnation: None,
                if_incarnation: None,
            },
        )),
    ] {
        last = Some(cluster.engines[1].raft.client_write(command).await.unwrap());
    }
    let retained = last.expect("written");
    cluster.engines[1].raft.trigger().heartbeat().await.unwrap();
    cluster.engines[1]
        .raft
        .wait(Some(Duration::from_secs(5)))
        .applied_index_at_least(Some(retained.log_id.index()), "retention applied")
        .await
        .unwrap();
    // The leader can no longer reach a quorum.
    for service in [&cluster.services[0], &cluster.services[2]] {
        service.pause_replication.store(true, Ordering::SeqCst);
    }
    let leader = &mut cluster.engines[1];
    let below = leader
        .read_stream(
            ReadStreamRequest {
                offset: 10,
                follower: FollowerRead::Immutable,
                ..read_req(stream_id.clone(), 4)
            },
            placement(),
        )
        .await
        .expect("a markable chunk needs no round");
    assert_eq!(below.payload, b"-tai");
    assert!(below.is_immutable(4));
    let prefix = leader
        .read_stream(
            ReadStreamRequest {
                offset: 10,
                ..read_req(stream_id.clone(), 64)
            },
            placement(),
        )
        .await
        .expect("a base-protocol read needs no round");
    assert_eq!(prefix.payload, b"-tail-bytes");
    for (name, request) in [
        ("an unmarkable chunk", ReadStreamRequest {
            offset: 10,
            follower: FollowerRead::Immutable,
            ..read_req(stream_id.clone(), 64)
        }),
        ("a 416", ReadStreamRequest {
            offset: 99,
            follower: FollowerRead::Immutable,
            ..read_req(stream_id.clone(), 4)
        }),
        ("a 410", read_req(stream_id.clone(), 4)),
        ("a 404", read_req(bsid("never-created"), 4)),
    ] {
        let refused = leader
            .read_stream(request, placement())
            .await
            .expect_err("waits for a quorum");
        assert!(refused.not_leader().is_some(), "{name}: {refused:?}");
        assert!(
            !may_hold_only_for_a_lagging_state(&refused),
            "{name}: {refused:?}"
        );
    }
    for service in [&cluster.services[0], &cluster.services[2]] {
        service.pause_replication.store(false, Ordering::SeqCst);
    }
    // With a quorum again, the same reads answer.
    let mut gone = None;
    for _ in 0..50 {
        match cluster.engines[1]
            .read_stream(read_req(stream_id.clone(), 4), placement())
            .await
        {
            Err(error) if error.code() == Some(StreamErrorCode::StreamGone) => {
                gone = Some(error);
                break;
            }
            Err(_transient) => tokio::time::sleep(Duration::from_millis(20)).await,
            Ok(read) => panic!("a read below the retained offset answered {read:?}"),
        }
    }
    assert!(gone.is_some(), "the leader's 410 once it reaches a quorum");
    cluster.shutdown().await;
}

/// `request` read through the first replica, retried while a leader proof
/// is transiently refused.
async fn read_through_first(
    cluster: &mut ThreeReplicas,
    request: ReadStreamRequest,
) -> ReadStreamResponse {
    let replicas = replica_watches(&cluster.engines, &cluster.services);
    let engine = &mut cluster.engines[0];
    retry_transient("read through the first replica", &replicas, async |_| {
        engine
            .read_stream(request.clone(), placement())
            .await
            .map_err(read_attempt_error)
    })
    .await
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
        snapshot: crate::types::SnapshotData::new(Vec::new()),
    };
    // Another install holds the node's only install permit. A refused
    // install answers without queueing for it.
    let busy = registry
        .snapshot_install_coordinator()
        .acquire(placement().raft_group_id)
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
        BTreeSet::from([1, 2]),
        move |_id| {
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

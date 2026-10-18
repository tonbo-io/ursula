use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::io::Read;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::Instant;

use axum::body::Body;
use axum::body::to_bytes;
use axum::http::Request;
use openraft::RaftNetworkV2;
use openraft::error::ReplicationClosed;
use openraft::network::RPCOption;
use openraft::raft::SnapshotResponse;
use openraft::rt::WatchReceiver;
use serde_json::json;
use tower::ServiceExt;
use ursula_raft::RaftGroupMetricsSnapshot;
use ursula_raft::RaftLogProgressSnapshot;
use ursula_raft::StaticGrpcRaftGroupEngineFactory;
use ursula_raft::StaticGrpcRaftMembershipConfig;
use ursula_raft::UrsulaRaftTypeConfig;
use ursula_runtime::ColdStore;
use ursula_runtime::ColdStoreHandle;
use ursula_runtime::GroupEngineError;
use ursula_runtime::InMemoryGroupEngineFactory;
use ursula_runtime::KnownLeader;
use ursula_runtime::LeadershipReason;
use ursula_runtime::LeadershipRefusal;
use ursula_runtime::PlanGroupColdFlushRequest;
use ursula_runtime::Proposal;
use ursula_runtime::RuntimeConfig;
use ursula_runtime::RuntimeError;
use ursula_runtime::StreamErrorCode;
use ursula_runtime::StreamErrorContext;
use ursula_shard::RaftGroupId;

use super::*;

/// Headers 0.7 set on refusals that applied nothing, removed in 0.8 (#581
/// section 6.6). No response may carry them.
const REMOVED_NOT_APPLIED_HEADERS: [&str; 2] =
    ["x-ursula-not-applied", "x-ursula-not-applied-checked"];

#[track_caller]
fn assert_no_not_applied_headers(headers: &HeaderMap, context: &str) {
    for name in REMOVED_NOT_APPLIED_HEADERS {
        assert!(!headers.contains_key(name), "{context}: {name}");
    }
}

/// Sends one HTTP request through a cloned `Router` and returns the response.
///
/// The status and header expectations stay at the call site; this only folds
/// the `Request::builder()` / `oneshot` / double-`expect` plumbing.
async fn send(
    app: &Router,
    method: &str,
    uri: &str,
    headers: &[(&str, &str)],
    body: Body,
) -> Response {
    let mut request = Request::builder().method(method).uri(uri);
    if uri.starts_with("/__ursula/")
        && !matches!(method, "GET" | "HEAD" | "OPTIONS")
        && !headers
            .iter()
            .any(|(name, _)| *name == PROCESS_INCARNATION_HEADER)
    {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/__ursula/metrics")
                    .body(Body::empty())
                    .expect("metrics request"),
            )
            .await
            .expect("metrics response");
        if response.status().is_success() {
            let metrics: serde_json::Value =
                serde_json::from_slice(&body_bytes(response).await).expect("metrics JSON");
            request = request.header(
                PROCESS_INCARNATION_HEADER,
                metrics["process_incarnation"]
                    .as_str()
                    .expect("process identity"),
            );
        }
    }
    for (name, value) in headers {
        request = request.header(*name, *value);
    }
    app.clone()
        .oneshot(request.body(body).expect("request"))
        .await
        .expect("response")
}

async fn http_get(app: &Router, uri: &str) -> Response {
    send(app, "GET", uri, &[], Body::empty()).await
}

async fn http_head(app: &Router, uri: &str) -> Response {
    send(app, "HEAD", uri, &[], Body::empty()).await
}

async fn http_delete(app: &Router, uri: &str) -> Response {
    send(app, "DELETE", uri, &[], Body::empty()).await
}

async fn http_put(app: &Router, uri: &str, headers: &[(&str, &str)], body: Body) -> Response {
    send(app, "PUT", uri, headers, body).await
}

async fn http_post(app: &Router, uri: &str, headers: &[(&str, &str)], body: Body) -> Response {
    send(app, "POST", uri, headers, body).await
}

/// Returns the named response header as `&str`, panicking when absent.
#[track_caller]
fn header_str<B, K>(response: &axum::http::Response<B>, name: K) -> &str
where K: axum::http::header::AsHeaderName + std::fmt::Display {
    let label = name.to_string();
    response
        .headers()
        .get(name)
        .unwrap_or_else(|| panic!("missing header {label}"))
        .to_str()
        .expect("header value is valid utf-8")
}

async fn body_bytes(response: Response) -> Bytes {
    to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body")
}

fn test_config(core_count: usize, group_count: usize) -> ursula_config::UrsulaConfig {
    let mut config = ursula_config::UrsulaConfig::default();
    config.runtime.core_count = core_count;
    config.raft.group_count = group_count;
    config
}

#[derive(Clone)]
struct TestWallClock {
    now_ms: Arc<AtomicU64>,
}

impl WallClock for TestWallClock {
    fn unix_time_ms(&self) -> u64 {
        self.now_ms.load(Ordering::Relaxed)
    }
}

async fn wait_raft_state_machine_payload(
    registry: &RaftGroupHandleRegistry,
    placement: ursula_shard::ShardPlacement,
    stream_id: &BucketStreamId,
    expected: &[u8],
    context: &str,
) {
    let raft = registry
        .get(placement.raft_group_id)
        .expect("registered raft group");
    let mut last_observed = None;
    let max_len = expected.len().max(64);
    for _ in 0..100 {
        let read = raft
            .with_state_machine({
                let stream_id = stream_id.clone();
                move |state_machine| {
                    Box::pin(async move {
                        state_machine
                            .read_stream(
                                ReadStreamRequest {
                                    stream_id,
                                    offset: 0,
                                    max_len,
                                    now_ms: 0,
                                    leader_only: false,
                                    read_index: None,
                                    follower: FollowerRead::Prefix,
                                },
                                placement,
                            )
                            .await
                    })
                }
            })
            .await;
        match read {
            Ok(Ok(read)) if read.payload == expected => return,
            Ok(Ok(read)) => {
                last_observed = Some(format!(
                    "payload={:?}",
                    String::from_utf8_lossy(&read.payload)
                ));
            }
            Ok(Err(err)) => {
                last_observed = Some(format!("err={err}"));
            }
            Err(err) => {
                last_observed = Some(format!("state-machine err={err}"));
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("{context}: timed out waiting for stream payload; latest={last_observed:?}");
}

fn raft_group_metric_index_at_least(
    body: &str,
    raft_group_id: u64,
    field: &str,
    min_index: u64,
) -> bool {
    let json: serde_json::Value = serde_json::from_str(body).expect("metrics JSON");
    let Some(groups) = json
        .get("raft_groups")
        .and_then(serde_json::Value::as_array)
    else {
        return false;
    };
    let field_name = format!("{field}_index");
    groups
        .iter()
        .find(|group| {
            group
                .get("raft_group_id")
                .and_then(serde_json::Value::as_u64)
                == Some(raft_group_id)
        })
        .and_then(|group| group.get(&field_name))
        .and_then(serde_json::Value::as_u64)
        .is_some_and(|index| index >= min_index)
}

#[test]
fn runtime_error_status_prefers_stream_error_code_over_message_text() {
    let err = RuntimeError::GroupEngine {
        core_id: ursula_shard::CoreId(0),
        raft_group_id: RaftGroupId(0),
        error: GroupEngineError::stream(
            StreamErrorCode::StreamGone,
            "misleading message says NotFound",
        ),
    };

    assert_eq!(
        crate::render::runtime_error_answer(&err).0,
        StatusCode::GONE
    );
}

#[test]
fn runtime_error_status_does_not_parse_infra_message_as_stream_error() {
    let err = RuntimeError::GroupEngine {
        core_id: ursula_shard::CoreId(0),
        raft_group_id: RaftGroupId(0),
        error: GroupEngineError::new("misleading infra message says StreamGone"),
    };

    assert_eq!(
        crate::render::runtime_error_answer(&err).0,
        StatusCode::INTERNAL_SERVER_ERROR
    );
}

#[test]
fn group_engine_leadership_refusal_is_detected_without_matching_message() {
    let err = RuntimeError::GroupEngine {
        core_id: ursula_shard::CoreId(0),
        raft_group_id: RaftGroupId(0),
        error: GroupEngineError::NotLeader(LeadershipRefusal {
            leader: None,
            proposal: Proposal::MayHaveCommitted,
            reason: LeadershipReason::SteppedDown,
        }),
    };

    assert!(super::is_forward_to_leader(&err));
}

// A refusal naming this node (a read bounced back during a leadership
// transfer) must not redirect the client to itself, and one naming no
// leader must not redirect: no 307, so the caller answers the
// leader-unknown 503. A refusal naming a peer in its term redirects there,
// typed.
#[test]
fn leader_redirect_never_points_at_this_node() {
    let router = ClientWriteLeaderRouter::with_static_topology(
        Some(2),
        [
            (1, "http://node-1/".to_owned()),
            (2, "http://node-2".to_owned()),
        ],
        BTreeMap::new(),
    );
    let forward = |leader: Option<KnownLeader>| RuntimeError::GroupEngine {
        core_id: ursula_shard::CoreId(0),
        raft_group_id: RaftGroupId(4),
        error: GroupEngineError::NotLeader(LeadershipRefusal {
            leader,
            proposal: Proposal::NotProposed,
            reason: LeadershipReason::Follower,
        }),
    };
    let known = |node_id| {
        Some(KnownLeader {
            node_id,
            term: 7,
            address: None,
        })
    };

    assert!(
        router
            .redirect_response(&forward(known(2)), "/b/s")
            .is_none()
    );
    assert!(router.redirect_response(&forward(None), "/b/s").is_none());
    let redirect = router
        .redirect_response(&forward(known(1)), "/b/s")
        .expect("a refusal naming a peer in its term redirects");
    assert_eq!(redirect.status(), StatusCode::TEMPORARY_REDIRECT);
    assert_eq!(redirect.headers()[LOCATION], "http://node-1/b/s");
    assert_eq!(
        typed_routing(&redirect),
        ursula_headers::RoutingAnswer::NotLeader {
            leader: ursula_headers::NodeId::from_u64(1),
            group: ursula_headers::RaftGroup::from_u32(4),
            term: ursula_headers::RaftTerm::from_u64(7),
        }
    );
}

// A leader that `[[raft.peers]]` does not name has only its Raft address in
// the refusal. With split planes that address is a cluster listener, so no
// redirect names it, and the leader-router and `leader_url` both give up.
#[test]
fn split_planes_never_redirect_to_a_raft_address() {
    let router = || {
        ClientWriteLeaderRouter::with_static_topology(
            Some(2),
            [
                (1, "http://node-1:4437".to_owned()),
                (2, "http://node-2:4437".to_owned()),
            ],
            BTreeMap::new(),
        )
    };
    let unlisted = RuntimeError::GroupEngine {
        core_id: ursula_shard::CoreId(0),
        raft_group_id: RaftGroupId(4),
        error: GroupEngineError::NotLeader(LeadershipRefusal {
            leader: Some(KnownLeader {
                node_id: 9,
                term: 7,
                address: Some("http://node-9:4439".to_owned()),
            }),
            proposal: Proposal::NotProposed,
            reason: LeadershipReason::Follower,
        }),
    };

    let shared = router()
        .redirect_response(&unlisted, "/b/s")
        .expect("a shared plane redirects to the Raft address");
    assert_eq!(shared.headers()[LOCATION], "http://node-9:4439/b/s");

    let split = router().with_planes(PeerPlanes::Split);
    assert!(split.redirect_response(&unlisted, "/b/s").is_none());
    assert!(
        split
            .leader_url(9, Some("http://node-9:4439"), "/b/s")
            .is_none()
    );
    assert_eq!(
        split.leader_url(1, Some("http://node-1:4439"), "/b/s"),
        Some("http://node-1:4437/b/s".to_owned())
    );
    // Without a redirect the refusal is one of an unknown leader, which the
    // client plane answers with a retryable 503.
    assert!(super::is_forward_to_leader(&unlisted));
}

/// The routing answer of a `307` or a routing `503`, as clients parse it.
/// Every redirect a node sends parses (#581 section 5.7).
#[track_caller]
fn typed_routing<B>(response: &axum::http::Response<B>) -> ursula_headers::RoutingAnswer {
    ursula_headers::RoutingAnswer::parse(
        response.status().as_u16(),
        response
            .headers()
            .iter()
            .map(|(name, value)| (name.as_str(), value.as_bytes())),
    )
    .expect("a well-formed routing answer")
    .expect("a routing answer")
}

#[track_caller]
fn typed_routing_reqwest(response: &reqwest::Response) -> ursula_headers::RoutingAnswer {
    ursula_headers::RoutingAnswer::parse(
        response.status().as_u16(),
        response
            .headers()
            .iter()
            .map(|(name, value)| (name.as_str(), value.as_bytes())),
    )
    .expect("a well-formed routing answer")
    .expect("a routing answer")
}

#[test]
fn runtime_error_response_marks_temporary_errors_retryable() {
    let response = super::runtime_error_response(RuntimeError::LiveReadBackpressure {
        core_id: ursula_shard::CoreId(0),
        current_waiters: 1,
        limit: 1,
    });

    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        response.headers().get(axum::http::header::RETRY_AFTER),
        Some(&axum::http::HeaderValue::from_static("1"))
    );
}

#[test]
fn runtime_error_response_uses_structured_context_for_producer_headers() {
    let response = super::runtime_error_response(RuntimeError::GroupEngine {
        core_id: ursula_shard::CoreId(0),
        raft_group_id: RaftGroupId(0),
        error: GroupEngineError::stream_with_context(
            StreamErrorCode::ProducerSeqConflict,
            "producer conflict without parseable header fields",
            None,
            vec![StreamErrorContext::ProducerSeqConflict {
                expected_seq: 7,
                received_seq: 3,
            }],
        ),
    });

    assert_eq!(
        response.headers().get("producer-expected-seq"),
        Some(&axum::http::HeaderValue::from_static("7"))
    );
    assert_eq!(
        response.headers().get("producer-received-seq"),
        Some(&axum::http::HeaderValue::from_static("3"))
    );
}

#[test]
fn runtime_error_response_uses_structured_context_for_stream_closed_header() {
    let response = super::runtime_error_response(RuntimeError::GroupEngine {
        core_id: ursula_shard::CoreId(0),
        raft_group_id: RaftGroupId(0),
        error: GroupEngineError::stream_with_context(
            StreamErrorCode::StreamClosed,
            "stream unavailable",
            None,
            vec![StreamErrorContext::StreamClosed],
        ),
    });

    assert_eq!(
        response.headers().get(names::STREAM_CLOSED),
        Some(&axum::http::HeaderValue::from_static("true"))
    );
}

#[test]
fn parses_membership_voter_ids() {
    assert_eq!(
        parse_voter_ids("3,1,2").expect("parse voters"),
        BTreeSet::from([1, 2, 3])
    );
    parse_voter_ids("").expect_err("an empty voter list must be rejected");
    parse_voter_ids("1,,2").expect_err("an empty node id between commas must be rejected");
    parse_voter_ids("1,node-2").expect_err("a non-numeric voter id must be rejected");
}

#[test]
fn query_parser_decodes_membership_and_learner_values() {
    let query = parse_query(Some(
        "voters=1%2C2%2C3&addr=http%3A%2F%2Fnode-3%3A4437&blocking=false",
    ))
    .expect("parse encoded admin query");

    assert_eq!(query.get("voters").map(String::as_str), Some("1,2,3"));
    assert_eq!(
        query.get("addr").map(String::as_str),
        Some("http://node-3:4437")
    );
    assert_eq!(query.get("blocking").map(String::as_str), Some("false"));
}

#[test]
fn static_grpc_membership_config_rejects_partial_group_voters() {
    let result = crate::bootstrap::Topology::static_cluster(
        1,
        vec![
            (1, "http://node-1".to_owned()),
            (2, "http://node-2".to_owned()),
            (3, "http://node-3".to_owned()),
        ],
        2,
        true,
        StaticGrpcRaftMembershipConfig {
            initialize_membership_per_group: true,
            per_group_voters: BTreeMap::from([(RaftGroupId(0), BTreeSet::from([1, 2, 3]))]),
        },
    );

    let Err(err) = result else {
        panic!("partial static per-group voter config should be rejected");
    };
    let RuntimeError::StaticMembershipConfig { message } = err else {
        panic!("expected static membership config error, got {err}");
    };
    assert!(message.contains("partial raft_group_voters config is not supported"));
    assert!(message.contains("missing raft group 1"));
}

/// Single-node groups on a fresh WAL under `wal_root`, registered in
/// `registry`, and that WAL. Shut it down with [`shutdown_test_wal`] before
/// `wal_root` goes.
fn registered_durable_factory(
    wal_root: &tempfile::TempDir,
    registry: &RaftGroupHandleRegistry,
) -> (
    ursula_raft::DurableRaftGroupEngineFactory,
    ursula_raft::RaftWal,
) {
    let raft_wal = ursula_raft::RaftWal::start(
        wal_root.path(),
        ursula_config::WalFsync::Never,
        &ursula_shard::StaticShardMap::new(1, 1).expect("valid topology"),
    )
    .expect("start the Raft WAL");
    (
        ursula_raft::DurableRaftGroupEngineFactory::new(raft_wal.clone())
            .with_registry(registry.clone()),
        raft_wal,
    )
}

/// Stops `runtime`'s Raft groups and closes `raft_wal` as the server does,
/// so the test may then remove the WAL directory. Removing it under a live
/// core writer fails the writer's next journal write, which stops the
/// process.
async fn shutdown_test_wal(runtime: &ShardRuntime, raft_wal: &ursula_raft::RaftWal) {
    assert_eq!(
        crate::server::shutdown_raft_wal(runtime, Some(raft_wal)).await,
        crate::server::WalShutdown::Clean,
        "the Raft WAL shuts down cleanly"
    );
}

struct StaticGrpcTestNode {
    runtime: ShardRuntime,
    /// The node's router, for requests sent in process.
    app: Router,
    registry: RaftGroupHandleRegistry,
    shutdown: Option<tokio::sync::oneshot::Sender<()>>,
    server: tokio::task::JoinHandle<()>,
    raft_wal: ursula_raft::RaftWal,
    /// The node's own WAL directory when the test gave it none: a fresh,
    /// empty one per start, as a node that lost its disk restarts.
    /// [`StaticGrpcTestNode::shutdown`] closes the WAL before it goes.
    _wal_root: Option<tempfile::TempDir>,
}

impl StaticGrpcTestNode {
    async fn shutdown(mut self) {
        self.registry.shutdown_transport();
        if let Some(shutdown) = self.shutdown.take() {
            shutdown.send(()).expect("test server is still running");
        }
        if tokio::time::timeout(Duration::from_secs(5), &mut self.server)
            .await
            .is_err()
        {
            self.server.abort();
            if let Err(err) = self.server.await
                && !err.is_cancelled()
            {
                panic!("test server failed: {err}");
            }
        }
        shutdown_test_wal(&self.runtime, &self.raft_wal).await;
    }
}

#[derive(Default)]
struct StaticGrpcTestNodeStorage {
    /// The node's journal directory, kept across restarts; `None` starts
    /// every run on a fresh, empty WAL.
    raft_log_dir: Option<PathBuf>,
    cold_store: Option<ColdStoreHandle>,
    /// Cold admission: the group hot-byte limit it checks inline creates and
    /// appends against (`None`: off, the runtime's default).
    cold_max_hot_bytes_per_group: Option<u64>,
    engine_config: Option<ursula_raft::RaftEngineConfig>,
    per_group_initializers: bool,
    per_group_voters: BTreeMap<RaftGroupId, BTreeSet<u64>>,
    start_maintenance_drained: bool,
    /// Faulty code every replica of one group runs.
    apply_fault: Option<(RaftGroupId, ursula_raft::apply_failure::ApplyFault)>,
    /// Where the node's snapshots go (`None`: inline, the default).
    snapshot_store: Option<ursula_runtime::SharedSnapshotStore>,
    /// Snapshot GC grace G (`None`: the registry's default).
    snapshot_gc_grace: Option<Duration>,
}

/// The cluster every static gRPC test node is in: peers refuse a node of
/// another cluster.
fn static_test_cluster_id() -> ursula_headers::ClusterId {
    ursula_headers::ClusterId::new("static-grpc-test").expect("a valid cluster id")
}

async fn spawn_static_grpc_test_node(
    node_id: u64,
    listener: tokio::net::TcpListener,
    factory_peers: Vec<(u64, String)>,
    router_peers: Vec<(u64, String)>,
    initialize_membership: bool,
    raft_group_count: usize,
    storage: StaticGrpcTestNodeStorage,
) -> StaticGrpcTestNode {
    let registry = match storage.snapshot_gc_grace {
        Some(gc_grace) => RaftGroupHandleRegistry::default().with_snapshot_install(1, gc_grace),
        None => RaftGroupHandleRegistry::default(),
    };
    if storage.start_maintenance_drained {
        registry.mark_leadership_shed(ursula_raft::LeadershipShedReason::MaintenanceDrain);
    }
    let mut config = RuntimeConfig::new(1, raft_group_count)
        .with_cold_max_hot_bytes_per_group(storage.cold_max_hot_bytes_per_group);
    config.threading = ursula_runtime::RuntimeThreading::HostedTokio;
    let (log_stores, wal_root) = match storage.raft_log_dir {
        Some(raft_log_dir) => (
            ursula_raft::RaftWal::start_in_cluster(
                raft_log_dir,
                ursula_raft::JournalTuning::new(ursula_config::WalFsync::Always),
                &ursula_shard::StaticShardMap::new(1, raft_group_count).expect("valid topology"),
                Some(&static_test_cluster_id()),
            )
            .expect("start the Raft WAL"),
            None,
        ),
        None => {
            let wal_root = tempfile::tempdir().expect("WAL root");
            (
                ursula_raft::RaftWal::start_in_cluster(
                    wal_root.path(),
                    ursula_raft::JournalTuning::new(ursula_config::WalFsync::Never),
                    &ursula_shard::StaticShardMap::new(1, raft_group_count)
                        .expect("valid topology"),
                    Some(&static_test_cluster_id()),
                )
                .expect("start the Raft WAL"),
                Some(wal_root),
            )
        }
    };
    let mut factory = StaticGrpcRaftGroupEngineFactory::new(
        node_id,
        factory_peers,
        initialize_membership,
        registry.clone(),
        log_stores.clone(),
    );
    factory = factory.with_per_group_membership_initializers(storage.per_group_initializers);
    factory = factory.with_per_group_voters(storage.per_group_voters.clone());
    factory = factory.with_cold_store(storage.cold_store.clone());
    if let Some(engine_config) = storage.engine_config {
        factory = factory.with_engine_config(engine_config);
    }
    if let Some((group, fault)) = storage.apply_fault {
        factory = factory.with_apply_fault(group, fault);
    }
    if let Some(snapshot_store) = storage.snapshot_store {
        factory = factory.with_snapshot_store(Some(snapshot_store));
    }
    let runtime =
        ShardRuntime::spawn_with_engine_factory_and_cold_store(config, factory, storage.cold_store)
            .expect("runtime");
    // Its answers name the cluster its WAL holds, as a server's do.
    let app = router_with_http_state(
        HttpState::with_static_raft_cluster_topology(
            runtime.clone(),
            registry.clone(),
            Some(node_id),
            router_peers,
            storage.per_group_voters,
        )
        .with_configured_cluster_id(Some(static_test_cluster_id())),
    );
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let served = app.clone();
    let server = tokio::spawn(async move {
        axum::serve(listener, served)
            .with_graceful_shutdown(async move {
                // A dropped sender also means shut down.
                if shutdown_rx.await.is_err() {
                    tracing::debug!("test server shutdown sender dropped");
                }
            })
            .await
            .expect("serve static raft node");
    });
    StaticGrpcTestNode {
        runtime,
        app,
        registry,
        shutdown: Some(shutdown_tx),
        server,
        raft_wal: log_stores,
        _wal_root: wal_root,
    }
}

#[tokio::test]
async fn create_append_read_and_head_match_perf_compare_subset() {
    let app = test_router();

    let response = http_put(
        &app,
        "/benchcmp/stream-1",
        &[(CONTENT_TYPE.as_str(), "text/plain")],
        Body::empty(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::CREATED);
    assert_eq!(header_str(&response, CONTENT_TYPE), "text/plain");

    let response = http_post(
        &app,
        "/benchcmp/stream-1",
        &[(CONTENT_TYPE.as_str(), "text/plain")],
        Body::from("abcdefg"),
    )
    .await;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    assert_eq!(
        header_str(&response, names::STREAM_NEXT_OFFSET),
        "00000000000000000007"
    );

    let response = http_get(&app, "/benchcmp/stream-1?offset=2&max_bytes=3").await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(header_str(&response, CONTENT_TYPE), "text/plain");
    assert_eq!(
        header_str(&response, names::STREAM_NEXT_OFFSET),
        "00000000000000000005"
    );
    let body = body_bytes(response).await;
    assert_eq!(&body[..], b"cde");

    let response = http_head(&app, "/benchcmp/stream-1").await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        header_str(&response, names::STREAM_NEXT_OFFSET),
        "00000000000000000007"
    );
    assert_eq!(
        header_str(&response, HEADER_STREAM_COLD_HOT_START_OFFSET),
        "00000000000000000000"
    );
    assert_eq!(header_str(&response, CONTENT_TYPE), "text/plain");
}

#[tokio::test]
async fn close_only_post_sets_closed_state_and_rejects_later_append() {
    let app = test_router();

    let response = http_put(&app, "/benchcmp/closing", &[], Body::empty()).await;
    let status = response.status();
    let body = body_bytes(response).await;
    assert_eq!(
        status,
        StatusCode::CREATED,
        "create body={}",
        std::str::from_utf8(&body).unwrap_or("<non-utf8>")
    );

    let response = http_post(
        &app,
        "/benchcmp/closing",
        &[(names::STREAM_CLOSED, "true")],
        Body::empty(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    assert_eq!(header_str(&response, names::STREAM_CLOSED), "true");

    let response = http_post(
        &app,
        "/benchcmp/closing",
        &[(CONTENT_TYPE.as_str(), DEFAULT_CONTENT_TYPE)],
        Body::from("x"),
    )
    .await;
    assert_eq!(response.status(), StatusCode::CONFLICT);
}

#[tokio::test]
async fn append_conflict_precedence_reports_closed_header_before_mismatch_or_seq() {
    let app = test_router();

    let response = http_put(
        &app,
        "/benchcmp/closed-precedence",
        &[(CONTENT_TYPE.as_str(), "text/plain")],
        Body::empty(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::CREATED);

    let response = http_post(
        &app,
        "/benchcmp/closed-precedence",
        &[
            (CONTENT_TYPE.as_str(), "text/plain"),
            (names::STREAM_CLOSED, "true"),
        ],
        Body::from("final"),
    )
    .await;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    assert_eq!(
        header_str(&response, names::STREAM_NEXT_OFFSET),
        "00000000000000000005"
    );

    let response = http_post(
        &app,
        "/benchcmp/closed-precedence",
        &[
            (CONTENT_TYPE.as_str(), "application/octet-stream"),
            (HEADER_STREAM_SEQ, "0001"),
        ],
        Body::from("too-late"),
    )
    .await;
    assert_eq!(response.status(), StatusCode::CONFLICT);
    assert_eq!(header_str(&response, names::STREAM_CLOSED), "true");
    assert_eq!(
        header_str(&response, names::STREAM_NEXT_OFFSET),
        "00000000000000000005"
    );
}

// Base-contract pin; see base_contract_tests.rs.
#[tokio::test]
async fn producer_headers_deduplicate_retries_and_fence_stale_epochs() {
    let app = test_router();

    let response = http_put(
        &app,
        "/benchcmp/producer-http",
        &[(CONTENT_TYPE.as_str(), "text/plain")],
        Body::empty(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::CREATED);

    let response = http_post(
        &app,
        "/benchcmp/producer-http",
        &[
            (CONTENT_TYPE.as_str(), "text/plain"),
            (names::PRODUCER_ID, "writer-1"),
            (names::PRODUCER_EPOCH, "0"),
            (names::PRODUCER_SEQ, "0"),
        ],
        Body::from("a"),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(header_str(&response, names::PRODUCER_EPOCH), "0");
    assert_eq!(header_str(&response, names::PRODUCER_SEQ), "0");

    let response = http_post(
        &app,
        "/benchcmp/producer-http",
        &[
            (CONTENT_TYPE.as_str(), "text/plain"),
            (names::PRODUCER_ID, "writer-1"),
            (names::PRODUCER_EPOCH, "0"),
            (names::PRODUCER_SEQ, "0"),
        ],
        Body::from("ignored"),
    )
    .await;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    assert_eq!(
        header_str(&response, names::STREAM_NEXT_OFFSET),
        "00000000000000000001"
    );
    assert_eq!(header_str(&response, names::PRODUCER_EPOCH), "0");
    assert_eq!(header_str(&response, names::PRODUCER_SEQ), "0");

    let response = http_get(&app, "/benchcmp/producer-http?offset=0&max_bytes=16").await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = body_bytes(response).await;
    assert_eq!(&body[..], b"a");

    let response = http_post(
        &app,
        "/benchcmp/producer-http",
        &[
            (CONTENT_TYPE.as_str(), "text/plain"),
            (names::PRODUCER_ID, "writer-1"),
            (names::PRODUCER_EPOCH, "0"),
            (names::PRODUCER_SEQ, "2"),
        ],
        Body::from("gap"),
    )
    .await;
    assert_eq!(response.status(), StatusCode::CONFLICT);
    assert_eq!(header_str(&response, "producer-expected-seq"), "1");
    assert_eq!(header_str(&response, "producer-received-seq"), "2");

    let response = http_post(
        &app,
        "/benchcmp/producer-http",
        &[
            (CONTENT_TYPE.as_str(), "text/plain"),
            (names::PRODUCER_ID, "writer-1"),
            (names::PRODUCER_EPOCH, "1"),
            (names::PRODUCER_SEQ, "0"),
        ],
        Body::from("b"),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(header_str(&response, names::PRODUCER_EPOCH), "1");
    assert_eq!(header_str(&response, names::PRODUCER_SEQ), "0");

    let response = http_post(
        &app,
        "/benchcmp/producer-http",
        &[
            (CONTENT_TYPE.as_str(), "text/plain"),
            (names::PRODUCER_ID, "writer-1"),
            (names::PRODUCER_EPOCH, "0"),
            (names::PRODUCER_SEQ, "1"),
        ],
        Body::from("stale"),
    )
    .await;
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    assert_eq!(header_str(&response, names::PRODUCER_EPOCH), "1");
}

#[tokio::test]
async fn delete_stream_removes_http_visible_state() {
    let app = test_router();

    let response = http_put(
        &app,
        "/benchcmp/delete-http",
        &[(CONTENT_TYPE.as_str(), "text/plain")],
        Body::empty(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::CREATED);

    let response = http_post(
        &app,
        "/benchcmp/delete-http",
        &[(CONTENT_TYPE.as_str(), "text/plain")],
        Body::from("payload"),
    )
    .await;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    let response = http_delete(&app, "/benchcmp/delete-http").await;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    let response = http_head(&app, "/benchcmp/delete-http").await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    let response = http_post(
        &app,
        "/benchcmp/delete-http",
        &[(CONTENT_TYPE.as_str(), "text/plain")],
        Body::from("x"),
    )
    .await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn json_mode_normalizes_appends_and_reads_ndjson() {
    let app = test_router();

    let response = http_put(
        &app,
        "/benchcmp/json-mode",
        &[(CONTENT_TYPE.as_str(), "application/json; charset=utf-8")],
        Body::empty(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::CREATED);

    let response = http_post(
        &app,
        "/benchcmp/json-mode",
        &[(CONTENT_TYPE.as_str(), "application/json; charset=utf-8")],
        Body::from(r#"[[1,2,3]]"#),
    )
    .await;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    let response = http_head(&app, "/benchcmp/json-mode").await;
    assert_eq!(response.status(), StatusCode::OK);

    let response = http_get(&app, "/benchcmp/json-mode").await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(header_str(&response, CONTENT_TYPE), "application/x-ndjson");
    let body = body_bytes(response).await;
    assert_eq!(&body[..], b"[1,2,3]\n");

    let response = http_post(
        &app,
        "/benchcmp/json-mode",
        &[(CONTENT_TYPE.as_str(), "application/json")],
        Body::from("[]"),
    )
    .await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn json_message_text_is_stored_verbatim_minus_whitespace() {
    let app = test_router();
    let create_body = " [ { \"z\" : 1 , \"a\" : 2 } ] ";
    let response = http_put(
        &app,
        "/benchcmp/json-fidelity",
        &[(CONTENT_TYPE.as_str(), "application/json")],
        Body::from(create_body),
    )
    .await;
    assert_eq!(response.status(), StatusCode::CREATED);

    // Member order, duplicate members, number text and escapes (including a
    // lone surrogate) survive; only insignificant whitespace is removed.
    let append_body = "[\n  {\"b\": 1.50e3, \"a\": -0, \"a\": 1e400},\n  \"\\ud800 \\u00e9 \\/ \\\"x\\\"\",\n  [ 1 ,\t2 ]\r\n]";
    let response = http_post(
        &app,
        "/benchcmp/json-fidelity",
        &[(CONTENT_TYPE.as_str(), "application/json")],
        Body::from(append_body),
    )
    .await;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    let response = http_post(
        &app,
        "/benchcmp/json-fidelity",
        &[(CONTENT_TYPE.as_str(), "application/json")],
        Body::from("[ { \"k\" : [ ] } , 7 , { } ]"),
    )
    .await;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    let expected = "{\"z\":1,\"a\":2}\n\
                    {\"b\":1.50e3,\"a\":-0,\"a\":1e400}\n\
                    \"\\ud800 \\u00e9 \\/ \\\"x\\\"\"\n\
                    [1,2]\n\
                    {\"k\":[]}\n\
                    7\n\
                    {}\n";
    let response = http_get(&app, "/benchcmp/json-fidelity").await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = body_bytes(response).await;
    assert_eq!(std::str::from_utf8(&body).unwrap(), expected);

    let response = http_head(&app, "/benchcmp/json-fidelity").await;
    assert_eq!(
        header_str(&response, names::STREAM_NEXT_OFFSET),
        format!("{:020}", expected.len())
    );

    // Close the stream so the SSE response ends at the tail.
    let response = http_post(
        &app,
        "/benchcmp/json-fidelity",
        &[
            (CONTENT_TYPE.as_str(), "application/json"),
            (names::STREAM_CLOSED, "true"),
        ],
        Body::empty(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    let response = http_get(&app, "/benchcmp/json-fidelity?offset=-1&live=sse").await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = body_bytes(response).await;
    let body = std::str::from_utf8(&body).unwrap();
    assert!(
        body.contains("data:{\"b\":1.50e3,\"a\":-0,\"a\":1e400}\n"),
        "{body}"
    );
    assert!(
        body.contains("data:\"\\ud800 \\u00e9 \\/ \\\"x\\\"\"\n"),
        "{body}"
    );
}

#[tokio::test]
async fn invalid_json_bodies_are_refused_without_committing() {
    let app = test_router();
    let response = http_put(
        &app,
        "/benchcmp/json-invalid",
        &[(CONTENT_TYPE.as_str(), "application/json")],
        Body::from("{\"seed\":true}"),
    )
    .await;
    assert_eq!(response.status(), StatusCode::CREATED);

    let deep_object = format!("{}1{}", "{\"k\":".repeat(128), "}".repeat(128));
    let deep_wrapped = format!("[1,{}{}]", "[".repeat(128), "]".repeat(128));
    let bodies: Vec<Vec<u8>> = vec![
        b"{\"a\":1".to_vec(),
        b"[1,2,]".to_vec(),
        b"\"\\x\"".to_vec(),
        b"\"\xff\xfe\"".to_vec(),
        b"[{\"ok\":1}, \"\xc3\"]".to_vec(),
        b"1 2".to_vec(),
        deep_object.into_bytes(),
        deep_wrapped.into_bytes(),
    ];
    for body in bodies {
        let response = http_post(
            &app,
            "/benchcmp/json-invalid",
            &[(CONTENT_TYPE.as_str(), "application/json")],
            Body::from(body.clone()),
        )
        .await;
        assert_eq!(
            response.status(),
            StatusCode::BAD_REQUEST,
            "{}",
            String::from_utf8_lossy(&body)
        );
    }
    let response = http_put(
        &app,
        "/benchcmp/json-invalid-create",
        &[(CONTENT_TYPE.as_str(), "application/json")],
        Body::from("[{\"a\":1},"),
    )
    .await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let response = http_head(&app, "/benchcmp/json-invalid-create").await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    let response = http_head(&app, "/benchcmp/json-invalid").await;
    assert_eq!(
        header_str(&response, names::STREAM_NEXT_OFFSET),
        format!("{:020}", 14)
    );
    let response = http_get(&app, "/benchcmp/json-invalid").await;
    let body = body_bytes(response).await;
    assert_eq!(&body[..], b"{\"seed\":true}\n");
}

#[tokio::test]
async fn json_depth_limit_applies_per_message_after_flattening() {
    let app = test_router();
    let response = http_put(
        &app,
        "/benchcmp/json-depth",
        &[(CONTENT_TYPE.as_str(), "application/json")],
        Body::empty(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::CREATED);

    let object = |depth: usize| format!("{}1{}", "{\"k\":".repeat(depth), "}".repeat(depth));
    let cases = [
        (object(127), StatusCode::NO_CONTENT),
        (object(128), StatusCode::BAD_REQUEST),
        (format!("[{}]", object(127)), StatusCode::NO_CONTENT),
        (format!("[{}]", object(128)), StatusCode::BAD_REQUEST),
        // A 128-deep bare array body is flattened into one 127-deep message.
        (
            format!("{}{}", "[".repeat(128), "]".repeat(128)),
            StatusCode::NO_CONTENT,
        ),
        (
            format!("{}{}", "[".repeat(129), "]".repeat(129)),
            StatusCode::BAD_REQUEST,
        ),
    ];
    for (body, status) in cases {
        let response = http_post(
            &app,
            "/benchcmp/json-depth",
            &[(CONTENT_TYPE.as_str(), "application/json")],
            Body::from(body.clone()),
        )
        .await;
        assert_eq!(
            response.status(),
            status,
            "{}",
            body.get(..16).unwrap_or(&body)
        );
    }
    let response = http_get(&app, "/benchcmp/json-depth?offset=-1").await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = body_bytes(response).await;
    let lines: Vec<&[u8]> = body
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
        .collect();
    assert_eq!(lines.len(), 3);
    assert_eq!(
        lines[2],
        format!("{}{}", "[".repeat(127), "]".repeat(127)).as_bytes()
    );
}

#[tokio::test]
async fn finite_json_reads_negotiate_gzip_without_compressing_sse() {
    let app = test_router();
    let value = "a".repeat(4_096);
    let append_body = format!(r#"[{{"value":"{value}"}}]"#);
    let expected_body = format!("{{\"value\":\"{value}\"}}\n");

    let response = http_put(
        &app,
        "/benchcmp/compressed-json",
        &[(CONTENT_TYPE.as_str(), "application/json")],
        Body::empty(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::CREATED);

    let response = http_post(
        &app,
        "/benchcmp/compressed-json",
        &[(CONTENT_TYPE.as_str(), "application/json")],
        Body::from(append_body),
    )
    .await;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    let response = send(
        &app,
        "GET",
        "/benchcmp/compressed-json",
        &[("accept-encoding", "gzip")],
        Body::empty(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        header_str(&response, axum::http::header::CONTENT_ENCODING),
        "gzip"
    );
    assert!(
        header_str(&response, axum::http::header::VARY)
            .split(',')
            .any(|value| value.trim().eq_ignore_ascii_case("accept-encoding"))
    );
    let compressed = body_bytes(response).await;
    assert!(compressed.len() < expected_body.len() / 4);
    let mut decoder = flate2::read::GzDecoder::new(compressed.as_ref());
    let mut decoded = Vec::new();
    decoder.read_to_end(&mut decoded).expect("decode gzip body");
    assert_eq!(decoded, expected_body.as_bytes());

    let response = http_get(&app, "/benchcmp/compressed-json").await;
    assert_eq!(response.status(), StatusCode::OK);
    assert!(
        response
            .headers()
            .get(axum::http::header::CONTENT_ENCODING)
            .is_none()
    );
    assert_eq!(body_bytes(response).await, expected_body.as_bytes());

    let response = send(
        &app,
        "GET",
        "/benchcmp/compressed-json?offset=now&live=sse",
        &[("accept-encoding", "gzip")],
        Body::empty(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(header_str(&response, CONTENT_TYPE), "text/event-stream");
    assert!(
        response
            .headers()
            .get(axum::http::header::CONTENT_ENCODING)
            .is_none()
    );
}

#[tokio::test]
async fn json_mode_reads_ndjson_bytes_without_message_boundary_projection() {
    let app = test_router();

    let response = http_put(
        &app,
        "/benchcmp/json-window",
        &[(CONTENT_TYPE.as_str(), "application/json")],
        Body::empty(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::CREATED);

    let response = http_post(
        &app,
        "/benchcmp/json-window",
        &[(CONTENT_TYPE.as_str(), "application/json")],
        Body::from(r#"[{"message":"alpha"},{"message":"beta"}]"#),
    )
    .await;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    let response = http_get(&app, "/benchcmp/json-window?offset=-1&max_bytes=5").await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(header_str(&response, CONTENT_TYPE), "application/x-ndjson");
    assert_eq!(
        header_str(&response, names::STREAM_NEXT_OFFSET),
        "00000000000000000005"
    );
    let body = body_bytes(response).await;
    assert_eq!(&body[..], b"{\"mes");
}

#[tokio::test]
async fn metrics_expose_per_core_and_group_append_distribution() {
    let app = test_router();

    let response = http_put(&app, "/benchcmp/metrics-stream", &[], Body::empty()).await;
    assert_eq!(response.status(), StatusCode::CREATED);

    for payload in ["abc", "de"] {
        let response = http_post(
            &app,
            "/benchcmp/metrics-stream",
            &[(CONTENT_TYPE.as_str(), "application/octet-stream")],
            Body::from(payload),
        )
        .await;
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
    }

    let response = http_get(&app, "/__ursula/metrics").await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(header_str(&response, CONTENT_TYPE), "application/json");

    let body = body_bytes(response).await;
    let body = std::str::from_utf8(&body).expect("utf8 body");
    let decoded: ursula_proto::admin::NodeMetrics = serde_json::from_str(body).unwrap();
    assert!(!decoded.maintenance_fence_uncertain);
    assert_eq!(
        decoded.maintenance_fence,
        ursula_proto::admin::MaintenanceFenceState::Unclaimed
    );
    assert!(body.contains("\"accepted_appends\":2"));
    assert!(body.contains("\"applied_mutations\":3"));
    assert!(body.contains("\"active_cores\":1"));
    assert!(body.contains("\"active_groups\":1"));
    assert!(body.contains("\"cold_store\":{\"backend\":\"none\""));
    assert!(body.contains("\"per_core_appends\":["));
    assert!(body.contains("\"per_group_appends\":["));
    assert!(body.contains("\"per_core_applied_mutations\":["));
    assert!(body.contains("\"per_group_applied_mutations\":["));
    assert!(body.contains("\"mutation_apply_ns\":"));
    assert!(body.contains("\"per_core_mutation_apply_ns\":["));
    assert!(body.contains("\"per_group_mutation_apply_ns\":["));
    assert!(body.contains("\"group_lock_wait_ns\":"));
    assert!(body.contains("\"per_core_group_lock_wait_ns\":["));
    assert!(body.contains("\"per_group_group_lock_wait_ns\":["));
    assert!(body.contains("\"group_engine_exec_ns\":"));
    assert!(body.contains("\"per_core_group_engine_exec_ns\":["));
    assert!(body.contains("\"per_group_group_engine_exec_ns\":["));
    assert!(body.contains("\"group_mailbox_depth\":0"));
    assert!(body.contains("\"per_group_group_mailbox_depth\":["));
    assert!(body.contains("\"group_mailbox_max_depth\":"));
    assert!(body.contains("\"per_group_group_mailbox_max_depth\":["));
    assert!(body.contains("\"group_mailbox_full_events\":0"));
    assert!(body.contains("\"per_group_group_mailbox_full_events\":["));
    assert!(body.contains("\"raft_grpc_append_stream_sessions_opened\":"));
    assert!(body.contains("\"raft_grpc_append_stream_session_failures\":"));
    assert!(body.contains("\"raft_grpc_append_stream_requests\":"));
    assert!(body.contains("\"raft_grpc_append_stream_responses\":"));
    assert!(body.contains("\"raft_grpc_append_stream_request_bytes\":"));
    assert!(body.contains("\"raft_grpc_append_stream_response_bytes\":"));
    assert!(body.contains("\"raft_grpc_append_stream_request_frames\":"));
    assert!(body.contains("\"raft_grpc_append_stream_response_frames\":"));
    assert!(body.contains("\"raft_grpc_append_stream_batch_frames\":"));
    assert!(body.contains("\"raft_grpc_append_stream_batch_items_max\":"));
    assert!(body.contains("\"raft_grpc_append_stream_frame_bytes_max\":"));
    assert!(body.contains("\"raft_grpc_append_stream_inflight\":"));
    assert!(body.contains("\"raft_grpc_append_stream_inflight_max\":"));
    assert!(body.contains("\"raft_grpc_append_heartbeat_requests\":"));
    assert!(body.contains("\"raft_grpc_append_heartbeat_request_bytes\":"));
    assert!(body.contains("\"raft_grpc_append_replication_requests\":"));
    assert!(body.contains("\"raft_grpc_append_replication_request_bytes\":"));
    assert!(body.contains("\"raft_grpc_append_replication_entries\":"));
    assert!(body.contains("\"raft_grpc_append_response_bytes\":"));
    assert!(body.contains("\"raft_grpc_vote_requests\":"));
    assert!(body.contains("\"raft_grpc_vote_request_bytes\":"));
    assert!(body.contains("\"raft_grpc_vote_response_bytes\":"));
    assert!(body.contains("\"raft_grpc_snapshot_requests\":"));
    assert!(body.contains("\"raft_grpc_snapshot_request_bytes\":"));
    assert!(body.contains("\"raft_grpc_snapshot_payload_bytes\":"));
    assert!(body.contains("\"raft_grpc_snapshot_response_bytes\":"));
    assert!(body.contains("\"raft_apply_entries\":0"));
    assert!(body.contains("\"per_core_raft_apply_entries\":[0,0]"));
    assert!(body.contains("\"per_group_raft_apply_entries\":["));
    assert!(body.contains("\"raft_apply_ns\":0"));
    assert!(body.contains("\"per_core_raft_apply_ns\":[0,0]"));
    assert!(body.contains("\"per_group_raft_apply_ns\":["));
    assert!(body.contains("\"live_read_waiters\":0"));
    assert!(body.contains("\"per_core_live_read_waiters\":[0,0]"));
    assert!(body.contains("\"live_read_backpressure_events\":0"));
    assert!(body.contains("\"per_core_live_read_backpressure_events\":[0,0]"));
    assert!(body.contains("\"sse_streams_opened\":0"));
    assert!(body.contains("\"sse_read_iterations\":0"));
    assert!(body.contains("\"sse_data_events\":0"));
    assert!(body.contains("\"sse_control_events\":0"));
    assert!(body.contains("\"sse_error_events\":0"));
    // A write is routed from the leadership view, without a trip through
    // its group's core: one request per write.
    assert!(body.contains("\"routed_requests\":3"));
    assert!(body.contains("\"per_core_routed_requests\":["));
    assert!(body.contains("\"mailbox_send_wait_ns\":"));
    assert!(body.contains("\"per_core_mailbox_send_wait_ns\":["));
    assert!(body.contains("\"mailbox_full_events\":0"));
    assert!(body.contains("\"per_core_mailbox_full_events\":[0,0]"));
    assert!(body.contains("\"wal_batches\":0"));
    assert!(body.contains("\"per_core_wal_batches\":[0,0]"));
    assert!(body.contains("\"per_group_wal_batches\":["));
    assert!(body.contains("\"wal_records\":0"));
    assert!(body.contains("\"per_core_wal_records\":[0,0]"));
    assert!(body.contains("\"per_group_wal_records\":["));
    assert!(body.contains("\"wal_write_ns\":0"));
    assert!(body.contains("\"per_core_wal_write_ns\":[0,0]"));
    assert!(body.contains("\"per_group_wal_write_ns\":["));
    assert!(body.contains("\"wal_sync_ns\":0"));
    assert!(body.contains("\"per_core_wal_sync_ns\":[0,0]"));
    assert!(body.contains("\"per_group_wal_sync_ns\":["));
    assert!(body.contains("\"cold_flush_uploads\":0"));
    assert!(body.contains("\"cold_flush_upload_bytes\":0"));
    assert!(body.contains("\"cold_flush_upload_ns\":0"));
    assert!(body.contains("\"cold_flush_publishes\":0"));
    assert!(body.contains("\"cold_flush_publish_bytes\":0"));
    assert!(body.contains("\"cold_flush_publish_ns\":0"));
    assert!(body.contains("\"cold_orphan_cleanup_attempts\":0"));
    assert!(body.contains("\"cold_orphan_cleanup_errors\":0"));
    assert!(body.contains("\"cold_orphan_bytes\":0"));
    assert!(body.contains("\"cold_hot_bytes\":"));
    assert!(body.contains("\"per_group_cold_hot_bytes\":["));
    assert!(body.contains("\"cold_hot_group_bytes_max\":"));
    assert!(body.contains("\"per_group_cold_hot_bytes_max\":["));
    assert!(body.contains("\"cold_hot_stream_bytes_max\":"));
    assert!(body.contains("\"cold_backpressure_events\":0"));
    assert!(body.contains("\"per_core_cold_backpressure_events\":[0,0]"));
    assert!(body.contains("\"per_group_cold_backpressure_events\":["));
    assert!(body.contains("\"cold_backpressure_bytes\":0"));
    assert!(body.contains("\"mailbox_depths\":["));
    assert!(body.contains("\"mailbox_capacities\":[1024,1024]"));
    assert!(body.contains("\"raft_group_count\":0"));
    assert!(body.contains("\"raft_groups\":[]"));
}

#[tokio::test]
async fn long_poll_times_out_with_no_content_and_cleans_waiter() {
    let app = test_router();

    let response = http_put(
        &app,
        "/benchcmp/long-poll-timeout",
        &[(CONTENT_TYPE.as_str(), "text/plain")],
        Body::empty(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::CREATED);

    let response = http_get(
        &app,
        "/benchcmp/long-poll-timeout?offset=now&live=long-poll&timeout_ms=10",
    )
    .await;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    assert_eq!(
        header_str(&response, names::STREAM_NEXT_OFFSET),
        "00000000000000000000"
    );
    assert_eq!(header_str(&response, names::STREAM_UP_TO_DATE), "true");
    assert_eq!(
        header_str(&response, HEADER_STREAM_CURSOR),
        "00000000000000000000"
    );

    // The timed-out waiter is cancelled asynchronously (its drop queues the
    // cancel), so the gauge reaches 0 shortly after the 204.
    let mut cleaned = false;
    for _ in 0..100 {
        let response = http_get(&app, "/__ursula/metrics").await;
        let body = body_bytes(response).await;
        if std::str::from_utf8(&body)
            .expect("utf8 body")
            .contains("\"live_read_waiters\":0")
        {
            cleaned = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert!(cleaned, "the timed-out long-poll waiter is cleaned up");
}

// Base-contract pin; see base_contract_tests.rs.
#[tokio::test]
async fn long_poll_returns_service_unavailable_when_live_waiters_are_full() {
    let runtime =
        ShardRuntime::spawn(RuntimeConfig::new(1, 1).with_live_read_max_waiters_per_core(Some(1)))
            .expect("runtime");
    let app = router(runtime);

    let response = http_put(
        &app,
        "/benchcmp/long-poll-limit",
        &[(CONTENT_TYPE.as_str(), "text/plain")],
        Body::empty(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::CREATED);

    let first = {
        let app = app.clone();
        tokio::spawn(async move {
            http_get(
                &app,
                "/benchcmp/long-poll-limit?offset=now&live=long-poll&timeout_ms=1000",
            )
            .await
        })
    };
    tokio::time::sleep(std::time::Duration::from_millis(10)).await;

    let response = http_get(
        &app,
        "/benchcmp/long-poll-limit?offset=now&live=long-poll&timeout_ms=1000",
    )
    .await;
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        response.headers().get(axum::http::header::RETRY_AFTER),
        Some(&axum::http::HeaderValue::from_static("1"))
    );
    assert_eq!(
        crate::render::error_code_of(response).await,
        ursula_headers::ErrorCode::LiveReadBackpressure
    );

    first.abort();
    if let Err(err) = first.await
        && !err.is_cancelled()
    {
        panic!("first waiter failed: {err}");
    }

    let response = http_get(&app, "/__ursula/metrics").await;
    let body = body_bytes(response).await;
    let body = std::str::from_utf8(&body).expect("utf8 body");
    assert!(body.contains("\"live_read_backpressure_events\":1"));
    assert!(body.contains("\"per_core_live_read_backpressure_events\":[1]"));
}

#[tokio::test]
async fn long_poll_returns_append_from_owner_waiter() {
    let app = test_router();

    let response = http_put(
        &app,
        "/benchcmp/long-poll-wake",
        &[(CONTENT_TYPE.as_str(), "text/plain")],
        Body::empty(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::CREATED);

    let read = {
        let app = app.clone();
        tokio::spawn(async move {
            http_get(
                &app,
                "/benchcmp/long-poll-wake?offset=now&live=long-poll&timeout_ms=1000",
            )
            .await
        })
    };
    tokio::time::sleep(std::time::Duration::from_millis(10)).await;

    let response = http_post(
        &app,
        "/benchcmp/long-poll-wake",
        &[(CONTENT_TYPE.as_str(), "text/plain")],
        Body::from("wake"),
    )
    .await;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    let response = tokio::time::timeout(std::time::Duration::from_secs(1), read)
        .await
        .expect("long poll completed")
        .expect("read task");
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        header_str(&response, names::STREAM_NEXT_OFFSET),
        "00000000000000000004"
    );
    assert_eq!(
        header_str(&response, HEADER_STREAM_CURSOR),
        "00000000000000000004"
    );
    let body = body_bytes(response).await;
    assert_eq!(&body[..], b"wake");
}

#[tokio::test]
async fn sse_live_tail_delivers_appended_text_and_closed_control() {
    let app = test_router();

    let response = http_put(
        &app,
        "/benchcmp/sse-stream",
        &[(CONTENT_TYPE.as_str(), "text/plain")],
        Body::empty(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::CREATED);

    let response = http_get(&app, "/benchcmp/sse-stream?offset=now&live=sse").await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(header_str(&response, CONTENT_TYPE), "text/event-stream");
    assert_eq!(
        header_str(&response, "stream-data-content-type"),
        "text/plain"
    );

    let body_task = tokio::spawn(async move { body_bytes(response).await });

    let response = http_post(
        &app,
        "/benchcmp/sse-stream",
        &[
            (CONTENT_TYPE.as_str(), "text/plain"),
            (names::STREAM_CLOSED, "true"),
        ],
        Body::from("sse-token"),
    )
    .await;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    let body = tokio::time::timeout(std::time::Duration::from_secs(1), body_task)
        .await
        .expect("sse completed")
        .expect("body task");
    let body = std::str::from_utf8(&body).expect("utf8 sse body");
    assert!(body.contains("event: data"));
    assert!(body.contains("data:sse-token"));
    assert!(body.contains("\"streamNextOffset\":\"00000000000000000009\""));
    assert!(body.contains("\"streamClosed\":true"));

    let response = http_get(&app, "/__ursula/metrics").await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = body_bytes(response).await;
    let body = std::str::from_utf8(&body).expect("utf8 metrics body");
    assert!(body.contains("\"sse_streams_opened\":1"));
    assert!(body.contains("\"sse_read_iterations\":"));
    assert!(body.contains("\"sse_data_events\":1"));
    // One control event follows the data. The reader may also have sent
    // one when it first caught up, before the append reached it.
    let metrics: serde_json::Value = serde_json::from_str(body).expect("metrics JSON");
    assert!(
        (1..=2).contains(
            &metrics["sse_control_events"]
                .as_u64()
                .expect("control events")
        ),
        "{}",
        metrics["sse_control_events"]
    );
    assert!(body.contains("\"sse_error_events\":0"));
}

#[tokio::test]
async fn sse_exposes_ndjson_data_content_type_for_json_streams() {
    let app = test_router();

    let response = http_put(
        &app,
        "/benchcmp/sse-json",
        &[(CONTENT_TYPE.as_str(), "application/json")],
        Body::empty(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::CREATED);

    let response = http_post(
        &app,
        "/benchcmp/sse-json",
        &[
            (CONTENT_TYPE.as_str(), "application/json"),
            (names::STREAM_CLOSED, "true"),
        ],
        Body::from(r#"{"event":"done"}"#),
    )
    .await;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    let response = http_get(&app, "/benchcmp/sse-json?offset=-1&live=sse").await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(header_str(&response, CONTENT_TYPE), "text/event-stream");
    assert_eq!(
        header_str(&response, "stream-data-content-type"),
        "application/x-ndjson"
    );
    assert!(
        response
            .headers()
            .get(HEADER_STREAM_SSE_DATA_ENCODING)
            .is_none()
    );
    let body = body_bytes(response).await;
    let body = std::str::from_utf8(&body).expect("utf8 sse body");
    assert!(body.contains("event: data"));
    assert!(body.contains("data:{\"event\":\"done\"}"));
    assert!(body.contains("data:{\"event\":\"done\"}\ndata:\n\n"));
    assert!(body.contains("\"streamClosed\":true"));
}

#[tokio::test]
async fn sse_json_max_bytes_does_not_split_utf8_codepoints() {
    let app = test_router();

    let response = http_put(
        &app,
        "/benchcmp/sse-json-utf8",
        &[(CONTENT_TYPE.as_str(), "application/json")],
        Body::empty(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::CREATED);

    let response = http_post(
        &app,
        "/benchcmp/sse-json-utf8",
        &[
            (CONTENT_TYPE.as_str(), "application/json"),
            (names::STREAM_CLOSED, "true"),
        ],
        // A literal two-byte code point: P1 stores escapes verbatim, so a
        // `\u00e9` escape would stay ASCII and not exercise the split.
        Body::from("{\"m\":\"\u{00e9}\"}"),
    )
    .await;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    let response = http_get(
        &app,
        "/benchcmp/sse-json-utf8?offset=-1&live=sse&max_bytes=7",
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        header_str(&response, "stream-data-content-type"),
        "application/x-ndjson"
    );

    let body = body_bytes(response).await;
    let body = std::str::from_utf8(&body).expect("utf8 sse body");
    assert!(!body.contains('\u{fffd}'), "{body}");
    assert!(body.contains("\"streamNextOffset\":\"00000000000000000006\""));
    assert!(body.contains("data:{\"m\":\""));
    assert!(body.contains("data:\u{00e9}\"}"));
    assert!(body.contains("\"streamClosed\":true"));
}

#[tokio::test]
async fn raft_runtime_serves_http_subset_and_writes_core_journal() {
    let raft_root = std::env::temp_dir().join(format!(
        "ursula-raft-test-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system time after unix epoch")
            .as_nanos()
    ));
    remove_test_path(&raft_root);

    let app = router(
        spawn_runtime(
            &test_config(1, 1),
            Persistence::Raft {
                log_dir: raft_root.clone(),
            },
            Topology::SingleNode {
                raft_group_count: 1,
            },
        )
        .expect("runtime")
        .runtime,
    );
    let response = http_put(
        &app,
        "/benchcmp/raft-http",
        &[(CONTENT_TYPE.as_str(), "text/plain")],
        Body::empty(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::CREATED);

    let response = http_post(
        &app,
        "/benchcmp/raft-http",
        &[(CONTENT_TYPE.as_str(), "text/plain")],
        Body::from("raft-payload"),
    )
    .await;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    let response = http_get(&app, "/benchcmp/raft-http?offset=0&max_bytes=32").await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = body_bytes(response).await;
    assert_eq!(&body[..], b"raft-payload");

    let response = http_put(
        &app,
        "/benchcmp/raft-retention",
        &[(CONTENT_TYPE.as_str(), "application/json")],
        Body::from(r#"[{"id":1},{"id":2}]"#),
    )
    .await;
    assert_eq!(response.status(), StatusCode::CREATED);
    let response = http_put(
        &app,
        "/benchcmp/raft-retention/snapshot/00000000000000000018",
        &[(CONTENT_TYPE.as_str(), "application/json")],
        Body::from(r#"{"count":2}"#),
    )
    .await;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    let response = http_put(
        &app,
        "/benchcmp/raft-retention/retention/00000000000000000018",
        &[],
        Body::empty(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    let response = http_get(&app, "/benchcmp/raft-retention?offset=0").await;
    assert_eq!(response.status(), StatusCode::GONE);
    assert_eq!(
        crate::render::error_code_of(response).await,
        ursula_headers::ErrorCode::StreamGone
    );

    let response = http_get(&app, "/__ursula/metrics").await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = body_bytes(response).await;
    let body = std::str::from_utf8(&body).expect("utf8 body");
    assert!(body.contains("\"wal_batches\":"));
    assert!(!body.contains("\"wal_batches\":0"));
    assert!(body.contains("\"wal_records\":"));
    assert!(!body.contains("\"wal_records\":0"));
    assert!(body.contains("\"wal_write_ns\":"));
    assert!(body.contains("\"wal_sync_ns\":"));

    let journal_len = core_journal_record_bytes(&raft_root.join("core-0"));
    assert!(
        journal_len > 0,
        "expected raft journal records, got {journal_len} bytes"
    );

    std::fs::remove_dir_all(&raft_root).expect("remove raft root");
}

#[tokio::test]
async fn static_grpc_raft_runtime_can_use_core_journal() {
    let raft_root = std::env::temp_dir().join(format!(
        "ursula-static-raft-log-test-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system time after unix epoch")
            .as_nanos()
    ));
    remove_test_path(&raft_root);

    let spawned = spawn_runtime(
        &test_config(1, 1),
        Persistence::Raft {
            log_dir: raft_root.as_path().into(),
        },
        Topology::static_cluster(
            1,
            vec![(1, "http://127.0.0.1:4477".to_owned())],
            1,
            true,
            Default::default(),
        )
        .expect("valid static cluster topology"),
    )
    .expect("runtime");
    let runtime = spawned.runtime;
    let registry = spawned.raft_registry.expect("registry");
    runtime.warm_all_groups().await.expect("warm group");
    let raft = registry
        .get(RaftGroupId(0))
        .expect("registered static raft group");
    raft.wait(Some(Duration::from_secs(5)))
        .current_leader(1, "static durable gRPC Raft group should elect node 1")
        .await
        .expect("wait for leader");
    let app = router_with_static_raft_cluster(runtime, registry, [(
        1,
        "http://127.0.0.1:4477".to_owned(),
    )]);

    let response = http_put(
        &app,
        "/benchcmp/static-raft-log",
        &[(CONTENT_TYPE.as_str(), "text/plain")],
        Body::empty(),
    )
    .await;
    let status = response.status();
    let body = body_bytes(response).await;
    assert_eq!(
        status,
        StatusCode::CREATED,
        "create body={}",
        std::str::from_utf8(&body).unwrap_or("<non-utf8>")
    );

    let response = http_post(
        &app,
        "/benchcmp/static-raft-log",
        &[(CONTENT_TYPE.as_str(), "text/plain")],
        Body::from("static-raft-payload"),
    )
    .await;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    let response = http_get(&app, "/benchcmp/static-raft-log?offset=0&max_bytes=64").await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = body_bytes(response).await;
    assert_eq!(&body[..], b"static-raft-payload");

    let response = http_get(&app, "/__ursula/metrics").await;
    let body = body_bytes(response).await;
    let body = std::str::from_utf8(&body).expect("utf8 body");
    assert!(body.contains("\"wal_batches\":"));
    assert!(body.contains("\"wal_records\":"));

    assert!(
        core_journal_record_bytes(&raft_root.join("core-0")) > 0,
        "core journal should contain records"
    );

    std::fs::remove_dir_all(&raft_root).expect("remove raft root");
}

#[tokio::test]
async fn static_grpc_raft_runtime_recovers_from_core_journal_after_restart() {
    let raft_root = std::env::temp_dir().join(format!(
        "ursula-static-raft-log-restart-test-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system time after unix epoch")
            .as_nanos()
    ));
    remove_test_path(&raft_root);
    let peers = [(1, "http://127.0.0.1:4477".to_owned())];

    {
        let spawned = spawn_runtime(
            &test_config(1, 1),
            Persistence::Raft {
                log_dir: raft_root.as_path().into(),
            },
            Topology::static_cluster(1, peers.to_vec(), 1, true, Default::default())
                .expect("valid static cluster topology"),
        )
        .expect("runtime");
        let runtime = spawned.runtime;
        let registry = spawned.raft_registry.expect("registry");
        let raft_wal = spawned.raft_wal.expect("a durable runtime starts its WAL");
        runtime.warm_all_groups().await.expect("warm group");
        let placement = runtime.locate(&BucketStreamId::new("benchcmp", "static-raft-log-restart"));
        let shutdown_runtime = runtime.clone();
        let raft = registry
            .get(RaftGroupId(0))
            .expect("registered static raft group");
        raft.wait(Some(Duration::from_secs(5)))
            .current_leader(1, "static durable gRPC Raft group should elect node 1")
            .await
            .expect("wait for leader");
        let app = router_with_static_raft_cluster(runtime, registry, peers.clone());

        let response = http_put(
            &app,
            "/benchcmp/static-raft-log-restart",
            &[(CONTENT_TYPE.as_str(), "text/plain")],
            Body::empty(),
        )
        .await;
        assert_eq!(response.status(), StatusCode::CREATED);

        let response = http_post(
            &app,
            "/benchcmp/static-raft-log-restart",
            &[(CONTENT_TYPE.as_str(), "text/plain")],
            Body::from("restart-payload"),
        )
        .await;
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
        drop(app);
        shutdown_runtime
            .shutdown_group_engine(placement)
            .await
            .expect("shut down durable group before restart");
        raft_wal
            .shutdown()
            .await
            .expect("shut down the Raft WAL cleanly");
    }

    assert!(
        core_journal_record_bytes(&raft_root.join("core-0")) > 0,
        "core journal should contain records"
    );

    {
        let spawned = spawn_runtime(
            &test_config(1, 1),
            Persistence::Raft {
                log_dir: raft_root.as_path().into(),
            },
            Topology::static_cluster(1, peers.to_vec(), 1, false, Default::default())
                .expect("valid static cluster topology"),
        )
        .expect("restarted runtime");
        let runtime = spawned.runtime;
        let registry = spawned.raft_registry.expect("registry");
        let opening = spawned.raft_wal.expect("restarted WAL").opening();
        assert_eq!(
            opening.previous_run,
            ursula_raft::wal::diagnostics::PreviousRun::Clean
        );
        assert_eq!(
            opening.replay_mode,
            ursula_raft::wal::diagnostics::JournalReplayMode::Strict
        );
        assert_eq!(opening.recovery, ursula_raft::RecoveryState::Normal);
        assert_eq!(
            registry.wal_opening(),
            Some(opening),
            "the registry publishes how the WAL opened"
        );
        runtime
            .warm_all_groups()
            .await
            .expect("warm restarted group");
        let raft = registry
            .get(RaftGroupId(0))
            .expect("registered restarted static raft group");
        raft.wait(Some(Duration::from_secs(5)))
            .current_leader(1, "restarted durable gRPC Raft group should elect node 1")
            .await
            .expect("wait for restarted leader");
        let placement = runtime.locate(&BucketStreamId::new("benchcmp", "static-raft-log-restart"));
        let shutdown_runtime = runtime.clone();
        let app = router_with_static_raft_cluster(runtime, registry, peers);

        let response = http_get(
            &app,
            "/benchcmp/static-raft-log-restart?offset=0&max_bytes=64",
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        let body = body_bytes(response).await;
        assert_eq!(&body[..], b"restart-payload");
        drop(app);
        shutdown_runtime
            .shutdown_group_engine(placement)
            .await
            .expect("shut down restarted durable group");
    }

    std::fs::remove_dir_all(&raft_root).expect("remove raft root");
}

/// A transport that sends as a node of `registry`'s cluster.
fn peer_transport(registry: &RaftGroupHandleRegistry) -> Arc<ursula_raft::CoreRaftTransport> {
    let identity = registry
        .peer_cluster()
        .identity()
        .expect("the engine factory bound the registry's cluster");
    Arc::new(ursula_raft::CoreRaftTransport::new(
        identity,
        ursula_raft::PeerCluster::default(),
    ))
}

#[tokio::test]
async fn raft_grpc_network_dispatches_to_registered_runtime_owned_group() {
    let wal_root = tempfile::tempdir().expect("WAL root");
    let registry = RaftGroupHandleRegistry::default();
    let mut config = RuntimeConfig::new(1, 1);
    config.threading = ursula_runtime::RuntimeThreading::HostedTokio;
    let (factory, raft_wal) = registered_durable_factory(&wal_root, &registry);
    let runtime = ShardRuntime::spawn_with_engine_factory(config, factory).expect("runtime");
    runtime
        .warm_group(RaftGroupId(0))
        .await
        .expect("warm raft group");
    let app = router_with_raft_registry(runtime.clone(), registry.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind listener");
    let addr = listener.local_addr().expect("local addr");
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let server = tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(async move {
                // A dropped sender also means shut down.
                if shutdown_rx.await.is_err() {
                    tracing::debug!("test server shutdown sender dropped");
                }
            })
            .await
            .expect("serve raft RPC router");
    });

    let metrics_body = reqwest::get(format!("http://{addr}/__ursula/metrics"))
        .await
        .expect("read registered raft metrics")
        .text()
        .await
        .expect("registered raft metrics body");
    assert!(metrics_body.contains("\"raft_group_count\":1"));
    assert!(metrics_body.contains("\"raft_group_id\":0"));
    assert!(metrics_body.contains("\"node_id\":1"));
    assert!(metrics_body.contains("\"voter_ids\":[1]"));

    let client = ursula_ctl::MetricsClient::new(Duration::from_secs(2)).unwrap();
    let node = ursula_ctl::NodeInfo {
        id: 1,
        host: addr.to_string(),
        admin_url: format!("http://{addr}").parse().unwrap(),
        http_url: None,
        metrics_url: None,
        client_url: None,
        expected_process_incarnation: None,
        expected_maintenance_fence: None,
    };
    let observed = client
        .fetch_node(&node)
        .await
        .expect("shared server/client metrics schema");
    assert_eq!(observed.groups.len(), 1);
    assert_eq!(observed.groups[0].voter_ids, vec![1]);
    assert!(observed.groups[0].maintenance.running);
    assert!(!observed.maintenance_fence_uncertain);

    let mut network = ursula_raft::GrpcRaftNetwork::new(
        peer_transport(&registry),
        RaftGroupId(0),
        1,
        format!("http://{addr}"),
    );
    let vote_request =
        ursula_raft::UrsulaVoteRequest::new(ursula_raft::UrsulaVote::new(2, 1), None);
    let _: ursula_raft::UrsulaVoteResponse = network
        .vote(vote_request, RPCOption::new(Duration::from_secs(1)))
        .await
        .expect("send vote over gRPC Raft network");

    let mut missing_group = ursula_raft::GrpcRaftNetwork::new(
        peer_transport(&registry),
        RaftGroupId(1),
        1,
        format!("http://{addr}"),
    );
    let err = missing_group
        .vote(
            ursula_raft::UrsulaVoteRequest::new(ursula_raft::UrsulaVote::new(3, 1), None),
            RPCOption::new(Duration::from_secs(1)),
        )
        .await
        .expect_err("missing group should fail");
    assert!(err.to_string().contains("not registered"), "err={err}");

    shutdown_tx.send(()).expect("server is still running");
    server.await.expect("server task");
    shutdown_test_wal(&runtime, &raft_wal).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn static_grpc_per_group_membership_initializers_distribute_leaders() {
    let mut listeners = Vec::new();
    let mut peers = Vec::new();
    for node_id in 1..=3u64 {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind listener");
        let addr = listener.local_addr().expect("local addr");
        peers.push((node_id, format!("http://{addr}")));
        listeners.push(listener);
    }

    let mut nodes = Vec::new();
    for (index, listener) in listeners.into_iter().enumerate() {
        let node_id = u64::try_from(index + 1).expect("node id fits u64");
        nodes.push(
            spawn_static_grpc_test_node(
                node_id,
                listener,
                peers.clone(),
                peers.clone(),
                true,
                6,
                StaticGrpcTestNodeStorage {
                    per_group_initializers: true,
                    ..Default::default()
                },
            )
            .await,
        );
    }

    for (index, node) in nodes.iter().enumerate() {
        tokio::time::timeout(Duration::from_secs(10), node.runtime.warm_all_groups())
            .await
            .unwrap_or_else(|_| panic!("warm node {} groups timed out", index + 1))
            .expect("warm node groups");
    }

    for raw_group_id in 0u32..6 {
        let expected_leader = u64::from(raw_group_id % 3) + 1;
        let raft_group_id = RaftGroupId(raw_group_id);
        for node in &nodes {
            let raft = node.registry.get(raft_group_id).expect("registered group");
            raft.wait(Some(Duration::from_secs(5)))
                .current_leader(
                    expected_leader,
                    "per-group initializer should become group leader",
                )
                .await
                .expect("wait for distributed leader");
        }
    }

    let manifest = peers
        .iter()
        .map(|(id, endpoint)| ursula_ctl::NodeInfo {
            expected_process_incarnation: None,
            expected_maintenance_fence: None,
            id: *id,
            admin_url: endpoint.parse().unwrap(),
            http_url: Some(endpoint.parse().unwrap()),
            metrics_url: Some(endpoint.parse().unwrap()),
            client_url: None,
            host: endpoint.clone(),
        })
        .collect::<Vec<_>>();
    let client = ursula_ctl::MetricsClient::new(Duration::from_secs(1)).unwrap();
    ursula_ctl::wait_cluster_ready(
        "fresh quorum fixture startup",
        &manifest,
        &client,
        Duration::from_secs(5),
        Duration::from_millis(10),
        16,
    )
    .await
    .unwrap();
    let mut options = ursula_ctl::quorum::QuorumVerificationOptions {
        group_count: 6,
        timeout: Duration::from_secs(5),
        poll_interval: Duration::from_millis(10),
    };
    let proof = ursula_ctl::quorum::verify_quorum(&manifest, &client, &options)
        .await
        .unwrap();
    assert_eq!(proof.process_incarnations.len(), 3);
    assert_eq!(proof.prefixes.len(), 6);
    assert_eq!(proof.applied.len(), 3);
    for (id, prefix) in proof.prefixes {
        assert_eq!(prefix.leader_id, u64::from(id % 3) + 1);
        assert!(
            proof
                .applied
                .values()
                .all(|groups| groups[&id] >= prefix.required_applied_index)
        );
    }
    let mut admitted = client.pin_nodes(&manifest, None).await.unwrap();
    for node in &mut admitted {
        node.expected_maintenance_fence = Some(executor_token(1));
    }
    for node in &admitted {
        client.set_maintenance_fence(node, false).await.unwrap();
    }
    let admitted_proof = ursula_ctl::quorum::verify_quorum(&admitted, &client, &options)
        .await
        .unwrap();
    assert!(admitted_proof.maintenance_executor_certified);
    assert!(!admitted_proof.maintenance_executor_retired_certified);
    assert_eq!(admitted_proof.maintenance_fence, Some(executor_token(1)));
    client
        .set_maintenance_fence(&admitted[0], true)
        .await
        .unwrap();
    let mixed = ursula_ctl::quorum::verify_quorum(&admitted, &client, &options)
        .await
        .unwrap();
    assert!(!mixed.maintenance_executor_certified);
    assert!(!mixed.maintenance_executor_retired_certified);
    assert!(mixed.maintenance_fence.is_none());
    for node in &admitted {
        client.set_maintenance_fence(node, true).await.unwrap();
    }
    let retired_proof = ursula_ctl::quorum::verify_quorum(&admitted, &client, &options)
        .await
        .unwrap();
    assert!(!retired_proof.maintenance_executor_certified);
    assert!(retired_proof.maintenance_executor_retired_certified);
    assert_eq!(retired_proof.prefixes.len(), 6);
    assert_eq!(retired_proof.applied.len(), 3);
    options.group_count = 7;
    let missing = ursula_ctl::quorum::verify_quorum(&manifest, &client, &options)
        .await
        .unwrap_err();
    assert!(
        missing.to_string().contains("inventory differs"),
        "{missing}"
    );

    options.group_count = 6;
    nodes.pop().unwrap().shutdown().await;
    ursula_ctl::quorum::verify_quorum(&manifest, &client, &options)
        .await
        .expect_err("quorum verification must fail while a voter is down");
    let survivor = tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            match ursula_ctl::quorum::verify_surviving_quorum(&manifest, 3, &client, &options).await
            {
                Ok(proof) => break proof,
                Err(_) => tokio::time::sleep(Duration::from_millis(25)).await,
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(survivor.configured_voter_ids, BTreeSet::from([1, 2, 3]));
    assert_eq!(survivor.surviving_voter_ids, BTreeSet::from([1, 2]));
    assert!(!survivor.full_redundancy_restored);
    assert_eq!(survivor.verification.prefixes.len(), 6);
    assert_eq!(survivor.verification.applied.len(), 2);
    nodes.pop().unwrap().shutdown().await;
    ursula_ctl::quorum::verify_surviving_quorum(&manifest, 3, &client, &options)
        .await
        .expect_err("surviving-quorum verification must fail with one voter left");

    for node in nodes {
        node.shutdown().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn static_grpc_per_group_voters_create_distinct_initial_memberships() {
    let mut listeners = Vec::new();
    let mut peers = Vec::new();
    for node_id in 1..=4u64 {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind listener");
        let addr = listener.local_addr().expect("local addr");
        peers.push((node_id, format!("http://{addr}")));
        listeners.push(listener);
    }

    let group_voters = BTreeMap::from([
        (RaftGroupId(0), BTreeSet::from([1, 2, 3])),
        (RaftGroupId(1), BTreeSet::from([2, 3, 4])),
    ]);
    let mut nodes = Vec::new();
    for (index, listener) in listeners.into_iter().enumerate() {
        let node_id = u64::try_from(index + 1).expect("node id fits u64");
        nodes.push(
            spawn_static_grpc_test_node(
                node_id,
                listener,
                peers.clone(),
                peers.clone(),
                true,
                2,
                StaticGrpcTestNodeStorage {
                    per_group_initializers: true,
                    per_group_voters: group_voters.clone(),
                    ..Default::default()
                },
            )
            .await,
        );
    }

    for (raft_group_id, voters) in &group_voters {
        for (index, node) in nodes.iter().enumerate() {
            let node_id = u64::try_from(index + 1).expect("node id fits u64");
            if voters.contains(&node_id) {
                tokio::time::timeout(
                    Duration::from_secs(10),
                    node.runtime.warm_group(*raft_group_id),
                )
                .await
                .unwrap_or_else(|_| {
                    panic!("warm node {node_id} group {} timed out", raft_group_id.0)
                })
                .expect("warm voter group");
            }
        }
    }

    for (raw_group_id, expected_voters, expected_leader) in
        [(0, vec![1, 2, 3], 1), (1, vec![2, 3, 4], 3)]
    {
        let raft_group_id = RaftGroupId(raw_group_id);
        for node_id in &expected_voters {
            let node = &nodes[usize::try_from(*node_id - 1).expect("node id index fits usize")];
            let raft = node.registry.get(raft_group_id).expect("registered group");
            raft.wait(Some(Duration::from_secs(5)))
                .current_leader(
                    expected_leader,
                    "configured group should elect its initializer",
                )
                .await
                .expect("wait for configured group leader");
            let expected_voters_for_wait = expected_voters.clone();
            raft.wait(Some(Duration::from_secs(5)))
                .metrics(
                    move |metrics| {
                        metrics
                            .membership_config
                            .voter_ids()
                            .eq(expected_voters_for_wait.iter().copied())
                    },
                    "configured group should expose its static voter set",
                )
                .await
                .expect("wait for configured group membership");
        }
    }

    for node in nodes {
        node.shutdown().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn static_grpc_non_voter_redirects_request_without_creating_group() {
    let mut listeners = Vec::new();
    let mut peers = Vec::new();
    for node_id in 1..=4u64 {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind listener");
        let addr = listener.local_addr().expect("local addr");
        peers.push((node_id, format!("http://{addr}")));
        listeners.push(listener);
    }

    let group_voters = BTreeMap::from([
        (RaftGroupId(0), BTreeSet::from([1, 2, 3])),
        (RaftGroupId(1), BTreeSet::from([2, 3, 4])),
    ]);
    let mut nodes = Vec::new();
    for (index, listener) in listeners.into_iter().enumerate() {
        let node_id = u64::try_from(index + 1).expect("node id fits u64");
        nodes.push(
            spawn_static_grpc_test_node(
                node_id,
                listener,
                peers.clone(),
                peers.clone(),
                true,
                2,
                StaticGrpcTestNodeStorage {
                    per_group_initializers: true,
                    per_group_voters: group_voters.clone(),
                    ..Default::default()
                },
            )
            .await,
        );
    }

    for (raft_group_id, voters) in &group_voters {
        for (index, node) in nodes.iter().enumerate() {
            let node_id = u64::try_from(index + 1).expect("node id fits u64");
            if voters.contains(&node_id) {
                tokio::time::timeout(
                    Duration::from_secs(10),
                    node.runtime.warm_group(*raft_group_id),
                )
                .await
                .unwrap_or_else(|_| {
                    panic!("warm node {node_id} group {} timed out", raft_group_id.0)
                })
                .expect("warm voter group");
            }
        }
    }

    let stream_id = (0..10_000)
        .map(|index| BucketStreamId::new("benchcmp", format!("non-voter-route-{index}")))
        .find(|stream_id| nodes[0].runtime.locate(stream_id).raft_group_id == RaftGroupId(1))
        .expect("find stream in group hosted by nodes 2, 3, 4");
    assert!(nodes[0].registry.get(RaftGroupId(1)).is_none());

    let no_redirect_client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(Duration::from_secs(5))
        .build()
        .expect("build no-redirect reqwest client");
    let response = no_redirect_client
        .put(format!("{}/{}", peers[0].1, stream_id))
        .header(CONTENT_TYPE, "text/plain")
        .body("must-not-create-on-non-voter")
        .send()
        .await
        .expect("send request to non-voter");

    assert_eq!(response.status(), StatusCode::TEMPORARY_REDIRECT);
    let location = response
        .headers()
        .get(LOCATION)
        .and_then(|value| value.to_str().ok())
        .expect("redirect location");
    assert!(
        peers
            .iter()
            .filter(|(node_id, _)| [2, 3, 4].contains(node_id))
            .any(|(_, peer_url)| location.starts_with(peer_url)),
        "location {location} should target a voter for group 1"
    );
    assert!(location.ends_with(&format!("/{}", stream_id)));
    assert!(nodes[0].registry.get(RaftGroupId(1)).is_none());

    // The node hosts no replica of the group, so it redirects every write
    // before any engine could propose it, a delete included. No answer says
    // so.
    let target = format!("{}/{}", peers[0].1, stream_id);
    let writes = [
        ("create", response),
        (
            "append",
            no_redirect_client
                .post(&target)
                .header(CONTENT_TYPE, "text/plain")
                .body("abc")
                .send()
                .await
                .expect("append on the non-voter"),
        ),
        (
            "close",
            no_redirect_client
                .post(&target)
                .header(names::STREAM_CLOSED, "true")
                .send()
                .await
                .expect("close on the non-voter"),
        ),
        (
            "delete",
            no_redirect_client
                .delete(&target)
                .send()
                .await
                .expect("delete on the non-voter"),
        ),
    ];
    for (write, response) in &writes {
        assert_eq!(response.status(), StatusCode::TEMPORARY_REDIRECT, "{write}");
        assert_no_not_applied_headers(response.headers(), write);
        // The node knows no leader or term of the group: it names the group
        // alone (#581 section 5.7).
        assert_eq!(
            ursula_headers::RoutingAnswer::parse(
                response.status().as_u16(),
                response
                    .headers()
                    .iter()
                    .map(|(name, value)| (name.as_str(), value.as_bytes())),
            ),
            Ok(Some(ursula_headers::RoutingAnswer::NotHosted {
                group: ursula_headers::RaftGroup::from_u32(1)
            })),
            "{write}"
        );
    }
    assert!(nodes[0].registry.get(RaftGroupId(1)).is_none());

    // A HEAD on the node redirects too, and the redirect names the server.
    let head = no_redirect_client
        .head(&target)
        .send()
        .await
        .expect("HEAD on the non-voter");
    assert_eq!(head.status(), StatusCode::TEMPORARY_REDIRECT);
    assert_eq!(
        typed_routing_reqwest(&head),
        ursula_headers::RoutingAnswer::NotHosted {
            group: ursula_headers::RaftGroup::from_u32(1)
        }
    );
    let read = no_redirect_client
        .get(format!("{target}?offset=0"))
        .send()
        .await
        .expect("read on the non-voter");
    assert_eq!(read.status(), StatusCode::TEMPORARY_REDIRECT);
    assert_eq!(
        typed_routing_reqwest(&read),
        ursula_headers::RoutingAnswer::NotHosted {
            group: ursula_headers::RaftGroup::from_u32(1)
        }
    );
    let identity = ursula_headers::ServerIdentity::parse(
        head.headers()
            .iter()
            .map(|(name, value)| (name.as_str(), value.as_bytes())),
    )
    .expect("well-formed identity")
    .expect("the identity on a redirect");
    assert_eq!(identity.protocol, ursula_headers::PROTOCOL_VERSION);

    for node in nodes {
        node.shutdown().await;
    }
}

/// A request body that fails the test if the server polls it.
fn unpollable_body() -> Body {
    Body::from_stream(futures_util::stream::poll_fn(
        |_cx| -> std::task::Poll<Option<Result<Bytes, std::convert::Infallible>>> {
            panic!("the server polled the body of a write it routes before reading it")
        },
    ))
}

/// A write to a follower is routed before its body (#581 section 6.1): a
/// create, an append and a snapshot publish each answer `307` to the leader
/// with the leader, the group and the term, and the follower never polls
/// the body. Pressure comes after routing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn static_grpc_follower_routes_writes_before_their_body() {
    let mut listeners = Vec::new();
    let mut peers = Vec::new();
    for node_id in 1..=3u64 {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind listener");
        let addr = listener.local_addr().expect("local addr");
        peers.push((node_id, format!("http://{addr}")));
        listeners.push(listener);
    }
    let mut nodes = Vec::new();
    for (index, listener) in listeners.into_iter().enumerate() {
        let node_id = u64::try_from(index + 1).expect("node id fits u64");
        nodes.push(
            spawn_static_grpc_test_node(
                node_id,
                listener,
                peers.clone(),
                peers.clone(),
                node_id == 1,
                1,
                StaticGrpcTestNodeStorage::default(),
            )
            .await,
        );
    }
    for node in nodes.iter().skip(1) {
        tokio::time::timeout(Duration::from_secs(10), node.runtime.warm_all_groups())
            .await
            .expect("warm follower groups timed out")
            .expect("warm follower groups");
    }
    tokio::time::timeout(Duration::from_secs(10), nodes[0].runtime.warm_all_groups())
        .await
        .expect("warm initializing leader groups timed out")
        .expect("warm initializing leader groups");
    for node in &nodes {
        node.registry
            .get(RaftGroupId(0))
            .expect("registered group")
            .wait(Some(Duration::from_secs(5)))
            .current_leader(1, "static gRPC Raft cluster should elect node 1")
            .await
            .expect("wait for shared leader");
    }
    let created = send(
        &nodes[0].app,
        "PUT",
        "/benchcmp/routed",
        &[(CONTENT_TYPE.as_str(), "text/plain")],
        Body::empty(),
    )
    .await;
    assert_eq!(created.status(), StatusCode::CREATED);

    let follower = &nodes[1].app;
    for (write, method, target) in [
        ("create", "PUT", "/benchcmp/routed-new"),
        ("append", "POST", "/benchcmp/routed"),
        ("snapshot", "PUT", "/benchcmp/routed/snapshot/0"),
    ] {
        let response = send(
            follower,
            method,
            target,
            // With `Expect: 100-continue` the client sends no body until
            // the server asks for it, so the answer needs none of it. Over
            // HTTP/1.1 without it, the node drains an unread body after its
            // answer (addendum A3).
            &[
                (CONTENT_TYPE.as_str(), "text/plain"),
                (CONTENT_LENGTH.as_str(), "3"),
                (axum::http::header::EXPECT.as_str(), "100-continue"),
            ],
            unpollable_body(),
        )
        .await;
        assert_eq!(response.status(), StatusCode::TEMPORARY_REDIRECT, "{write}");
        assert_eq!(
            response
                .headers()
                .get(LOCATION)
                .and_then(|value| value.to_str().ok()),
            Some(format!("{}{target}", peers[0].1).as_str()),
            "{write}"
        );
        let answer = ursula_headers::RoutingAnswer::parse(
            response.status().as_u16(),
            response
                .headers()
                .iter()
                .map(|(name, value)| (name.as_str(), value.as_bytes())),
        )
        .expect("well-formed routing answer");
        assert!(
            matches!(answer, Some(ursula_headers::RoutingAnswer::NotLeader {
                leader, group, term
            }) if leader.get() == 1 && group.get() == 0 && term.get() > 0),
            "{write}: {answer:?}"
        );
    }

    // A read the follower cannot serve redirects, typed the same way.
    let read = send(
        follower,
        "GET",
        "/benchcmp/routed?offset=now&live=sse",
        &[],
        Body::empty(),
    )
    .await;
    assert_eq!(read.status(), StatusCode::TEMPORARY_REDIRECT);
    assert!(matches!(
        typed_routing(&read),
        ursula_headers::RoutingAnswer::NotLeader { leader, .. } if leader.get() == 1
    ));

    // Pressure comes after routing: a follower whose WAL disk is under
    // pressure redirects a write, and the leader under the same pressure
    // refuses it with a retryable `503`.
    let pressured = |node: &StaticGrpcTestNode, node_id: u64| {
        let monitor = WalDiskMonitor::new(100, 200);
        assert_eq!(
            monitor.observe_available(99),
            crate::wal_disk::WalDiskTransition::EnterPressure
        );
        let state = HttpState::with_static_raft_cluster_topology(
            node.runtime.clone(),
            node.registry.clone(),
            Some(node_id),
            peers.clone(),
            BTreeMap::new(),
        )
        .with_wal_disk_monitor(monitor.clone());
        client_router_with_admission(
            state,
            IngressAdmission::default().with_wal_disk_monitor(monitor),
        )
    };
    let append = |app: Router| async move {
        send(
            &app,
            "POST",
            "/benchcmp/routed",
            &[
                (CONTENT_TYPE.as_str(), "text/plain"),
                (CONTENT_LENGTH.as_str(), "3"),
            ],
            Body::from("abc"),
        )
        .await
    };
    let redirected = append(pressured(&nodes[1], 2)).await;
    assert_eq!(redirected.status(), StatusCode::TEMPORARY_REDIRECT);
    assert!(redirected.headers().contains_key(names::X_URSULA_RAFT_TERM));
    let refused = append(pressured(&nodes[0], 1)).await;
    assert_eq!(refused.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert!(
        refused
            .headers()
            .contains_key(axum::http::header::RETRY_AFTER)
    );
    assert!(!refused.headers().contains_key(names::X_URSULA_RAFT_GROUP));

    // After the body is read a node never redirects. A router without the
    // leadership view thinks this follower leads, reads the body, and its
    // engine refuses the write: a retryable 503, no Location.
    let unaware = Router::new().merge(client_router_with_admission(
        HttpState::new(nodes[1].runtime.clone()),
        IngressAdmission::default(),
    ));
    let late = append(unaware).await;
    assert_eq!(late.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert!(late.headers().contains_key(axum::http::header::RETRY_AFTER));
    assert!(!late.headers().contains_key(LOCATION));
    assert_eq!(
        typed_routing(&late),
        ursula_headers::RoutingAnswer::NoLeader {
            group: ursula_headers::RaftGroup::from_u32(0)
        }
    );

    for node in nodes {
        node.shutdown().await;
    }
}

/// A follower redirects an append and a close after its own leadership
/// check, before it proposes them, and a create and a delete after
/// OpenRaft forwards them. Nothing was proposed, and no redirect says so:
/// 0.8 has no not-applied marker (#581 section 6.6).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn static_grpc_follower_redirects_writes_without_a_not_applied_marker() {
    let mut listeners = Vec::new();
    let mut peers = Vec::new();
    for node_id in 1..=3u64 {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind listener");
        let addr = listener.local_addr().expect("local addr");
        peers.push((node_id, format!("http://{addr}")));
        listeners.push(listener);
    }
    let mut nodes = Vec::new();
    for (index, listener) in listeners.into_iter().enumerate() {
        let node_id = u64::try_from(index + 1).expect("node id fits u64");
        nodes.push(
            spawn_static_grpc_test_node(
                node_id,
                listener,
                peers.clone(),
                peers.clone(),
                node_id == 1,
                1,
                StaticGrpcTestNodeStorage::default(),
            )
            .await,
        );
    }
    for (index, node) in nodes.iter().enumerate().skip(1) {
        tokio::time::timeout(Duration::from_secs(10), node.runtime.warm_all_groups())
            .await
            .unwrap_or_else(|_| panic!("warm follower node {} groups timed out", index + 1))
            .expect("warm follower groups");
    }
    tokio::time::timeout(Duration::from_secs(10), nodes[0].runtime.warm_all_groups())
        .await
        .expect("warm initializing leader groups timed out")
        .expect("warm initializing leader groups");
    for node in &nodes {
        node.registry
            .get(RaftGroupId(0))
            .expect("registered group")
            .wait(Some(Duration::from_secs(5)))
            .current_leader(1, "static gRPC Raft cluster should elect node 1")
            .await
            .expect("wait for shared leader");
    }

    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(Duration::from_secs(5))
        .build()
        .expect("build no-redirect reqwest client");
    let (leader, follower) = (&peers[0].1, &peers[1].1);
    let created = client
        .put(format!("{leader}/benchcmp/marked"))
        .header(CONTENT_TYPE, "text/plain")
        .send()
        .await
        .expect("create on the leader");
    assert_eq!(created.status(), StatusCode::CREATED);

    let appended = client
        .post(format!("{follower}/benchcmp/marked"))
        .header(CONTENT_TYPE, "text/plain")
        .body("abc")
        .send()
        .await
        .expect("append on a follower");
    let closed = client
        .post(format!("{follower}/benchcmp/marked"))
        .header(names::STREAM_CLOSED, "true")
        .send()
        .await
        .expect("close on a follower");
    let leader_target = format!("{leader}/benchcmp/marked");
    for (write, response) in [("append", &appended), ("close", &closed)] {
        assert_eq!(response.status(), StatusCode::TEMPORARY_REDIRECT, "{write}");
        assert_eq!(
            response
                .headers()
                .get(LOCATION)
                .and_then(|value| value.to_str().ok()),
            Some(leader_target.as_str()),
            "{write}"
        );
        assert_no_not_applied_headers(response.headers(), write);
    }

    let create = client
        .put(format!("{follower}/benchcmp/unmarked"))
        .header(CONTENT_TYPE, "text/plain")
        .body("abc")
        .send()
        .await
        .expect("create on a follower");
    let delete = client
        .delete(format!("{follower}/benchcmp/marked"))
        .send()
        .await
        .expect("delete on a follower");
    for (write, response, target) in [
        ("create", &create, format!("{leader}/benchcmp/unmarked")),
        ("delete", &delete, leader_target.clone()),
    ] {
        assert_eq!(response.status(), StatusCode::TEMPORARY_REDIRECT, "{write}");
        assert_eq!(
            response
                .headers()
                .get(LOCATION)
                .and_then(|value| value.to_str().ok()),
            Some(target.as_str()),
            "{write}"
        );
        assert_no_not_applied_headers(response.headers(), write);
    }

    // The follower proposed none of them: the stream is still there, empty
    // and open, and the create made nothing.
    let head = client
        .head(leader_target.as_str())
        .send()
        .await
        .expect("head on the leader");
    assert_eq!(head.status(), StatusCode::OK);
    assert_eq!(
        head.headers()
            .get(names::STREAM_NEXT_OFFSET)
            .and_then(|value| value.to_str().ok()),
        Some("00000000000000000000")
    );
    assert!(!head.headers().contains_key(names::STREAM_CLOSED));
    let head = client
        .head(format!("{leader}/benchcmp/unmarked"))
        .send()
        .await
        .expect("head on the leader");
    assert_eq!(head.status(), StatusCode::NOT_FOUND);

    for node in nodes {
        node.shutdown().await;
    }
}

/// With cold admission on, a follower redirects an inline create after its
/// own leadership check, and an external create (its body goes to the cold
/// tier) after OpenRaft forwards it. Neither carries a not-applied marker.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn static_grpc_follower_redirects_inline_and_external_creates() {
    let mut listeners = Vec::new();
    let mut peers = Vec::new();
    for node_id in 1..=3u64 {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind listener");
        let addr = listener.local_addr().expect("local addr");
        peers.push((node_id, format!("http://{addr}")));
        listeners.push(listener);
    }
    let cold_store = std::sync::Arc::new(ColdStore::memory().expect("memory cold store"));
    let mut nodes = Vec::new();
    for (index, listener) in listeners.into_iter().enumerate() {
        let node_id = u64::try_from(index + 1).expect("node id fits u64");
        nodes.push(
            spawn_static_grpc_test_node(
                node_id,
                listener,
                peers.clone(),
                peers.clone(),
                node_id == 1,
                1,
                StaticGrpcTestNodeStorage {
                    cold_store: Some(cold_store.clone()),
                    cold_max_hot_bytes_per_group: Some(64 << 20),
                    ..Default::default()
                },
            )
            .await,
        );
    }
    for (index, node) in nodes.iter().enumerate().skip(1) {
        tokio::time::timeout(Duration::from_secs(10), node.runtime.warm_all_groups())
            .await
            .unwrap_or_else(|_| panic!("warm follower node {} groups timed out", index + 1))
            .expect("warm follower groups");
    }
    tokio::time::timeout(Duration::from_secs(10), nodes[0].runtime.warm_all_groups())
        .await
        .expect("warm initializing leader groups timed out")
        .expect("warm initializing leader groups");
    for node in &nodes {
        node.registry
            .get(RaftGroupId(0))
            .expect("registered group")
            .wait(Some(Duration::from_secs(5)))
            .current_leader(1, "static gRPC Raft cluster should elect node 1")
            .await
            .expect("wait for shared leader");
    }

    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(Duration::from_secs(5))
        .build()
        .expect("build no-redirect reqwest client");
    let (leader, follower) = (&peers[0].1, &peers[1].1);
    // At or above the 1 MiB external payload minimum, a create's body goes to
    // the cold tier.
    for (name, body) in [("inline", vec![7u8; 16]), ("external", vec![7u8; 1 << 20])] {
        let response = client
            .put(format!("{follower}/benchcmp/{name}"))
            .header(CONTENT_TYPE, "application/octet-stream")
            .body(body)
            .send()
            .await
            .expect("create on a follower");
        assert_eq!(response.status(), StatusCode::TEMPORARY_REDIRECT, "{name}");
        assert_eq!(
            response
                .headers()
                .get(LOCATION)
                .and_then(|value| value.to_str().ok()),
            Some(format!("{leader}/benchcmp/{name}").as_str()),
            "{name}"
        );
        assert_no_not_applied_headers(response.headers(), name);
        let head = client
            .head(format!("{leader}/benchcmp/{name}"))
            .send()
            .await
            .expect("head on the leader");
        assert_eq!(head.status(), StatusCode::NOT_FOUND, "{name}");
    }

    for node in nodes {
        node.shutdown().await;
    }
}

/// Starts three durable nodes with two groups on `addresses`, each node's
/// journals under `root`, and warms every group they host.
async fn start_durable_cluster(
    root: &std::path::Path,
    addresses: &[std::net::SocketAddr],
    initialize: bool,
    apply_fault: Option<(RaftGroupId, ursula_raft::apply_failure::ApplyFault)>,
) -> Vec<StaticGrpcTestNode> {
    let peers = (1..=3u64)
        .zip(addresses)
        .map(|(node_id, address)| (node_id, format!("http://{address}")))
        .collect::<Vec<_>>();
    let mut nodes = Vec::new();
    for (node_id, address) in (1..=3u64).zip(addresses) {
        let listener = tokio::net::TcpListener::bind(address)
            .await
            .expect("bind the node's address");
        nodes.push(
            spawn_static_grpc_test_node(
                node_id,
                listener,
                peers.clone(),
                peers.clone(),
                initialize && node_id == 1,
                2,
                StaticGrpcTestNodeStorage {
                    raft_log_dir: Some(root.join(format!("node-{node_id}"))),
                    apply_fault,
                    ..Default::default()
                },
            )
            .await,
        );
    }
    // The initializer warms last, after its peers can answer.
    for node in nodes.iter().rev() {
        tokio::time::timeout(Duration::from_secs(10), node.runtime.warm_all_groups())
            .await
            .expect("warm the node's groups in time")
            .expect("warm the node's groups");
    }
    nodes
}

/// Polls `url` until it answers 200, and returns the last status.
async fn poll_status(client: &reqwest::Client, url: &str) -> StatusCode {
    let deadline = tokio::time::Instant::now()
        .checked_add(Duration::from_secs(10))
        .expect("deadline");
    loop {
        let status = client.get(url).send().await.expect("request").status();
        if status == StatusCode::OK || tokio::time::Instant::now() >= deadline {
            return status;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Polls a stream's read through `base` until it holds `expected`.
async fn poll_payload(
    client: &reqwest::Client,
    base: &str,
    stream: &BucketStreamId,
    expected: &str,
) {
    let deadline = tokio::time::Instant::now()
        .checked_add(Duration::from_secs(10))
        .expect("deadline");
    loop {
        let response = client
            .get(format!("{base}/{stream}?offset=0"))
            .send()
            .await
            .expect("read the stream");
        let status = response.status();
        let body = response.text().await.expect("read body");
        if status == StatusCode::OK && body == expected {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "{base}/{stream}: {status} {body:?}, expected {expected:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// A deterministic poison record stops its group on every voter: a buggy
/// binary replays the committed record at startup on each replica. Every
/// node stays serving-ready and serves its other group, while maintenance
/// readiness refuses disruption. Corrected code replays the record on every
/// voter without skipping it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_group_stopped_on_every_voter_keeps_nodes_serving_until_corrected_replay() {
    let root = tempfile::tempdir().expect("WAL root");
    let mut addresses = Vec::new();
    for _ in 1..=3 {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("reserve an address");
        addresses.push(listener.local_addr().expect("local addr"));
    }
    let bases = addresses
        .iter()
        .map(|address| format!("http://{address}"))
        .collect::<Vec<_>>();
    let manifest = (1..=3u64)
        .zip(&bases)
        .map(|(id, base)| ursula_ctl::NodeInfo {
            expected_process_incarnation: None,
            expected_maintenance_fence: None,
            id,
            admin_url: base.parse().unwrap(),
            http_url: Some(base.parse().unwrap()),
            metrics_url: Some(base.parse().unwrap()),
            client_url: None,
            host: base.clone(),
        })
        .collect::<Vec<_>>();
    let ctl = ursula_ctl::MetricsClient::new(Duration::from_secs(2)).unwrap();
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap();

    let nodes = start_durable_cluster(root.path(), &addresses, true, None).await;
    let stream_of = |group: u32, name: &str| {
        (0..10_000)
            .map(|index| BucketStreamId::new("poison", format!("{name}-{index}")))
            .find(|stream| nodes[0].runtime.locate(stream).raft_group_id == RaftGroupId(group))
            .expect("a stream of the group")
    };
    let poisoned = stream_of(0, "poisoned");
    let healthy = stream_of(1, "healthy");
    ursula_ctl::observe::wait_ready(
        &ctl,
        &manifest,
        2,
        Duration::from_secs(20),
        Duration::from_millis(50),
    )
    .await
    .expect("a fresh cluster becomes ready");
    for (stream, body) in [(&poisoned, "poison"), (&healthy, "healthy")] {
        let created = client
            .put(format!("{}/{stream}", bases[0]))
            .header(CONTENT_TYPE, "text/plain")
            .body(body)
            .send()
            .await
            .expect("create a stream");
        assert_eq!(created.status(), StatusCode::CREATED, "{stream}");
    }
    let poison = nodes[0]
        .registry
        .get(RaftGroupId(0))
        .expect("group 0")
        .metrics()
        .borrow_watched()
        .last_applied
        .expect("the record applied")
        .index;
    for node in &nodes {
        node.registry
            .get(RaftGroupId(0))
            .expect("group 0")
            .wait(Some(Duration::from_secs(10)))
            .applied_index_at_least(Some(poison), "every voter applied the record")
            .await
            .expect("replicate the record");
    }
    for node in nodes {
        node.shutdown().await;
    }

    // A buggy binary now fails on the committed record, which every replica
    // replays at startup.
    let fault = ursula_raft::apply_failure::ApplyFault::PanicAfterMutation { index: poison };
    let nodes = start_durable_cluster(
        root.path(),
        &addresses,
        false,
        Some((RaftGroupId(0), fault)),
    )
    .await;
    for node in &nodes {
        let groups = node.registry.metrics_snapshot();
        let stopped = groups
            .iter()
            .find(|group| group.raft_group_id == 0)
            .and_then(|group| group.apply_failure.as_ref())
            .expect("group 0 stopped at startup");
        assert_eq!(stopped.index, poison);
    }
    for (base, node_id) in bases.iter().zip(1..=3u64) {
        assert_eq!(
            poll_status(&client, &format!("{base}/__ursula/ready")).await,
            StatusCode::OK,
            "node {node_id} keeps serving its other group"
        );
        let appended = client
            .post(format!("{base}/{healthy}"))
            .header(CONTENT_TYPE, "text/plain")
            .body(format!("-{node_id}"))
            .send()
            .await
            .expect("append to the healthy group");
        assert_eq!(appended.status(), StatusCode::NO_CONTENT);
        let refused = client
            .post(format!("{base}/{poisoned}"))
            .header(CONTENT_TYPE, "text/plain")
            .body("refused")
            .send()
            .await
            .expect("append to the stopped group");
        assert_eq!(refused.status(), StatusCode::SERVICE_UNAVAILABLE);
    }
    for base in &bases {
        poll_payload(&client, base, &healthy, "healthy-1-2-3").await;
    }
    let refusal = ursula_ctl::observe::wait_ready(
        &ctl,
        &manifest,
        2,
        Duration::from_secs(1),
        Duration::from_millis(50),
    )
    .await
    .expect_err("maintenance refuses disruption beside a stopped group");
    match refusal.downcast_ref::<ursula_ctl::observe::ReadinessRefusal>() {
        Some(ursula_ctl::observe::ReadinessRefusal::Maintenance {
            report: Some(report),
            ..
        }) => assert_eq!(
            report.group_issues.get(&0),
            Some(&vec![
                ursula_proto::admin::RaftMaintenanceIssue::ApplyStopped
            ])
        ),
        other => panic!("expected a maintenance refusal: {other:?} ({refusal:#})"),
    }
    for node in nodes {
        node.shutdown().await;
    }

    // Corrected code replays the retained record on every voter.
    let nodes = start_durable_cluster(root.path(), &addresses, false, None).await;
    ursula_ctl::observe::wait_ready(
        &ctl,
        &manifest,
        2,
        Duration::from_secs(20),
        Duration::from_millis(50),
    )
    .await
    .expect("corrected code recovers the group");
    for base in &bases {
        poll_payload(&client, base, &poisoned, "poison").await;
        poll_payload(&client, base, &healthy, "healthy-1-2-3").await;
    }
    for node in nodes {
        node.shutdown().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn static_grpc_follower_serves_replicated_catch_up_read_without_leader_proxy() {
    let mut listeners = Vec::new();
    let mut peers = Vec::new();
    for node_id in 1..=3u64 {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind listener");
        let addr = listener.local_addr().expect("local addr");
        peers.push((node_id, format!("http://{addr}")));
        listeners.push(listener);
    }

    let mut nodes = Vec::new();
    for (index, listener) in listeners.into_iter().enumerate() {
        let node_id = u64::try_from(index + 1).expect("node id fits u64");
        nodes.push(
            spawn_static_grpc_test_node(
                node_id,
                listener,
                peers.clone(),
                peers.clone(),
                node_id == 1,
                1,
                StaticGrpcTestNodeStorage::default(),
            )
            .await,
        );
    }

    for (index, node) in nodes.iter().enumerate().skip(1) {
        tokio::time::timeout(Duration::from_secs(10), node.runtime.warm_all_groups())
            .await
            .unwrap_or_else(|_| panic!("warm follower node {} group timed out", index + 1))
            .expect("warm follower group");
    }
    tokio::time::timeout(Duration::from_secs(10), nodes[0].runtime.warm_all_groups())
        .await
        .expect("warm leader group timed out")
        .expect("warm leader group");

    for node in &nodes {
        let raft = node.registry.get(RaftGroupId(0)).expect("registered group");
        raft.wait(Some(Duration::from_secs(5)))
            .current_leader(1, "static gRPC Raft cluster should elect node 1")
            .await
            .expect("wait for shared leader");
    }

    let http_client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("build reqwest client");
    let stream = BucketStreamId::new("benchcmp", "follower-local-read");
    let leader_base = peers[0].1.as_str();
    let follower_base = peers[1].1.as_str();
    let create = http_client
        .put(format!("{leader_base}/benchcmp/follower-local-read"))
        .header(CONTENT_TYPE, "text/plain")
        .body("read-without-leader")
        .send()
        .await
        .expect("create stream through leader");
    assert_eq!(create.status(), StatusCode::CREATED);

    let placement = nodes[1].runtime.locate(&stream);
    wait_raft_state_machine_payload(
        &nodes[1].registry,
        placement,
        &stream,
        b"read-without-leader",
        "follower replicated stream before local read",
    )
    .await;

    let leader_read = http_client
        .get(format!(
            "{follower_base}/benchcmp/follower-local-read?consistency=leader"
        ))
        .send()
        .await
        .expect("send leader-consistent read through follower");
    assert_eq!(leader_read.status(), StatusCode::OK);
    assert_eq!(
        leader_read
            .headers()
            .get(names::STREAM_UP_TO_DATE)
            .and_then(|value| value.to_str().ok()),
        Some("true"),
        "leader consistency must not be served by follower-local applied state"
    );
    assert_eq!(
        &leader_read
            .bytes()
            .await
            .expect("leader-consistent read body")[..],
        b"read-without-leader"
    );

    let leader = nodes.remove(0);
    leader.shutdown().await;

    let read = http_client
        .get(format!("{follower_base}/benchcmp/follower-local-read"))
        .send()
        .await
        .expect("send follower local read after leader proxy is unavailable");
    assert_eq!(read.status(), StatusCode::OK);
    assert_eq!(
        read.headers()
            .get(names::STREAM_NEXT_OFFSET)
            .and_then(|value| value.to_str().ok()),
        Some("00000000000000000019")
    );
    assert!(
        read.headers().get(names::STREAM_UP_TO_DATE).is_none(),
        "follower local reads must not assert open-tail freshness"
    );
    assert_eq!(
        &read.bytes().await.expect("follower local read body")[..],
        b"read-without-leader"
    );

    for node in nodes {
        node.shutdown().await;
    }
}

/// A follower answers an immutable chunk from its own state only when it can
/// mark it (#581 section 5.3): a chunk below its tail is its own to answer,
/// even with the leader gone, and a chunk that holds the open tail is the
/// leader's.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn static_grpc_follower_answers_only_the_chunks_it_can_mark() {
    const GRID: usize = 4 * 1024 * 1024;
    let mut listeners = Vec::new();
    let mut peers = Vec::new();
    for node_id in 1..=3u64 {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind listener");
        let addr = listener.local_addr().expect("local addr");
        peers.push((node_id, format!("http://{addr}")));
        listeners.push(listener);
    }
    let mut nodes = Vec::new();
    for (index, listener) in listeners.into_iter().enumerate() {
        let node_id = u64::try_from(index + 1).expect("node id fits u64");
        nodes.push(
            spawn_static_grpc_test_node(
                node_id,
                listener,
                peers.clone(),
                peers.clone(),
                node_id == 1,
                1,
                StaticGrpcTestNodeStorage::default(),
            )
            .await,
        );
    }
    for node in nodes.iter().skip(1).chain(nodes.iter().take(1)) {
        tokio::time::timeout(Duration::from_secs(10), node.runtime.warm_all_groups())
            .await
            .expect("warm group timed out")
            .expect("warm group");
    }
    for node in &nodes {
        let raft = node.registry.get(RaftGroupId(0)).expect("registered group");
        raft.wait(Some(Duration::from_secs(5)))
            .current_leader(1, "static gRPC Raft cluster should elect node 1")
            .await
            .expect("wait for shared leader");
    }

    let http_client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .expect("build reqwest client");
    let stream = BucketStreamId::new("benchcmp", "follower-chunks");
    let content: Vec<u8> = (0..GRID + 10)
        .map(|index| u8::try_from(index % 251).expect("byte"))
        .collect();
    let create = http_client
        .put(format!("{}/benchcmp/follower-chunks", peers[0].1))
        .header(CONTENT_TYPE, "application/octet-stream")
        .body(content.clone())
        .send()
        .await
        .expect("create stream through leader");
    assert_eq!(create.status(), StatusCode::CREATED);
    let token = ursula_headers::value::Incarnation::try_from(
        create
            .headers()
            .get(names::STREAM_INCARNATION)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse::<u64>().ok())
            .expect("token"),
    )
    .expect("a token");
    let placement = nodes[1].runtime.locate(&stream);
    wait_raft_state_machine_payload(
        &nodes[1].registry,
        placement,
        &stream,
        &content,
        "follower replicated the stream",
    )
    .await;

    let follower = peers[1].1.clone();
    let chunk_url = |offset: u64| {
        format!(
            "{follower}/benchcmp/follower-chunks{}",
            ursula_headers::ImmutableResource {
                kind: ursula_headers::ResourceKind::Chunk,
                incarnation: token,
                offset: ursula_headers::value::Offset::from_u64(offset),
            }
            .path()
        )
    };
    let chunk = async |offset: u64| {
        let response = http_client
            .get(chunk_url(offset))
            .send()
            .await
            .expect("chunk through follower");
        let status = response.status();
        let read = ursula_headers::ChunkRead::parse(
            response
                .headers()
                .iter()
                .map(|(name, value)| (name.as_str(), value.as_bytes())),
        );
        let body = response.bytes().await.expect("chunk body");
        (status, read, body)
    };

    let (status, read, body) = chunk(0).await;
    assert_eq!(status, StatusCode::OK);
    let read = read.expect("a chunk answer");
    assert_eq!(read.mark(), ursula_headers::ChunkMark::Immutable);
    assert_eq!(&body[..], &content[..GRID]);
    // The tail of the open stream lies in this chunk: the leader answers it,
    // and only the leader can.
    let (status, read, body) = chunk(u64::try_from(GRID).expect("grid")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        read.expect("a chunk answer").mark(),
        ursula_headers::ChunkMark::Unmarked
    );
    assert_eq!(&body[..], &content[GRID..]);

    let leader = nodes.remove(0);
    leader.shutdown().await;
    let (status, read, body) = chunk(0).await;
    assert_eq!(status, StatusCode::OK, "a marked chunk needs no leader");
    assert_eq!(
        read.expect("a chunk answer").mark(),
        ursula_headers::ChunkMark::Immutable
    );
    assert_eq!(&body[..], &content[..GRID]);
    // The follower never answers a chunk it cannot mark: it has no leader
    // to ask (`503`), or, once the survivors elected one, that leader
    // answers the whole tail chunk, unmarked.
    let (status, read, body) = chunk(u64::try_from(GRID).expect("grid")).await;
    match status {
        StatusCode::SERVICE_UNAVAILABLE => {}
        StatusCode::OK => {
            assert_eq!(
                read.expect("a chunk answer").mark(),
                ursula_headers::ChunkMark::Unmarked
            );
            assert_eq!(&body[..], &content[GRID..]);
        }
        other => panic!("the tail chunk without the old leader answered {other}"),
    }

    for node in nodes {
        node.shutdown().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn static_grpc_node_that_lost_its_wal_rejoins_all_groups_through_membership_replacement() {
    let mut listeners = Vec::new();
    let mut peers = Vec::new();
    let mut addrs = Vec::new();
    for node_id in 1..=3u64 {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind listener");
        let addr = listener.local_addr().expect("local addr");
        peers.push((node_id, format!("http://{addr}")));
        addrs.push(addr);
        listeners.push(listener);
    }

    let mut nodes = Vec::new();
    for (index, listener) in listeners.into_iter().enumerate() {
        let node_id = u64::try_from(index + 1).expect("node id fits u64");
        nodes.push(
            spawn_static_grpc_test_node(
                node_id,
                listener,
                peers.clone(),
                peers.clone(),
                true,
                6,
                StaticGrpcTestNodeStorage {
                    per_group_initializers: true,
                    ..Default::default()
                },
            )
            .await,
        );
    }

    for (index, node) in nodes.iter().enumerate().skip(1) {
        tokio::time::timeout(Duration::from_secs(10), node.runtime.warm_all_groups())
            .await
            .unwrap_or_else(|_| panic!("warm follower node {} groups timed out", index + 1))
            .expect("warm follower groups");
    }
    tokio::time::timeout(Duration::from_secs(10), nodes[0].runtime.warm_all_groups())
        .await
        .expect("warm leader groups timed out")
        .expect("warm leader groups");

    for raw_group_id in 0u32..6 {
        let expected_leader = u64::from(raw_group_id % 3) + 1;
        for node in &nodes {
            let raft = node
                .registry
                .get(RaftGroupId(raw_group_id))
                .expect("registered group");
            raft.wait(Some(Duration::from_secs(5)))
                .current_leader(
                    expected_leader,
                    "per-group initializer should become group leader",
                )
                .await
                .expect("wait for distributed leader");
        }
    }

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("build reqwest client");
    let leader_base = peers[0].1.as_str();
    let mut streams_by_group: Vec<Option<BucketStreamId>> = vec![None; 6];
    for candidate in 0..10_000 {
        let stream_id =
            BucketStreamId::new("benchcmp", format!("lost-wal-rejoin-group-{candidate}"));
        let group_index = usize::try_from(nodes[0].runtime.locate(&stream_id).raft_group_id.0)
            .expect("raft group id fits usize");
        if streams_by_group[group_index].is_none() {
            let create = client
                .put(format!("{leader_base}/{}", stream_id))
                .header(CONTENT_TYPE, "application/octet-stream")
                .body(format!("before-{group_index}"))
                .send()
                .await
                .expect("create stream through leader");
            assert_eq!(create.status(), StatusCode::CREATED);
            streams_by_group[group_index] = Some(stream_id);
        }
        if streams_by_group.iter().all(Option::is_some) {
            break;
        }
    }
    let streams_by_group: Vec<BucketStreamId> = streams_by_group
        .into_iter()
        .map(|stream| stream.expect("found stream for raft group"))
        .collect();

    for (group_index, stream_id) in streams_by_group.iter().enumerate() {
        let placement = nodes[2].runtime.locate(stream_id);
        assert_eq!(placement.raft_group_id, RaftGroupId(group_index as u32));
        wait_raft_state_machine_payload(
            &nodes[2].registry,
            placement,
            stream_id,
            format!("before-{group_index}").as_bytes(),
            "node 3 replicated initial payload before shutdown",
        )
        .await;
    }

    // Full redundancy includes the fresh recovery barrier, not just matching
    // payload bytes. Do not inject the next loss during initial recovery.
    let recovery_deadline = Instant::now() + Duration::from_secs(10);
    for node in &nodes {
        while !node.registry.recovery_barriers_ready() {
            assert!(
                Instant::now() < recovery_deadline,
                "initial recovery barriers not applied"
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }
    let stopped_node = nodes.remove(2);
    stopped_node.shutdown().await;

    for (group_index, stream_id) in streams_by_group.iter().enumerate() {
        let raft_group_id = RaftGroupId(group_index as u32);
        let observer_raft = nodes[0]
            .registry
            .get(raft_group_id)
            .expect("observer group");
        observer_raft
            .wait(Some(Duration::from_secs(10)))
            .metrics(
                |metrics| {
                    matches!(
                        metrics.current_leader,
                        Some(leader_id) if leader_id == 1 || leader_id == 2
                    )
                },
                "group should elect a surviving leader after node 3 stops",
            )
            .await
            .expect("wait for surviving leader before write");
        let leader_id = observer_raft
            .metrics()
            .borrow_watched()
            .current_leader
            .expect("surviving leader elected");
        let group_leader_base = peers
            .iter()
            .find(|(node_id, _)| *node_id == leader_id)
            .map(|(_, base_url)| base_url.as_str())
            .expect("leader peer base url");
        let append_while_down = client
            .post(format!("{group_leader_base}/{}", stream_id))
            .header(CONTENT_TYPE, "application/octet-stream")
            .body(format!("-after-stop-{group_index}"))
            .send()
            .await
            .expect("append while node 3 is down");
        assert_eq!(append_while_down.status(), StatusCode::NO_CONTENT);
    }

    for raw_group_id in 0..6 {
        let raft_group_id = RaftGroupId(raw_group_id);
        let observer_raft = nodes[0]
            .registry
            .get(raft_group_id)
            .expect("observer group");
        observer_raft
            .wait(Some(Duration::from_secs(10)))
            .metrics(
                |metrics| {
                    matches!(
                        metrics.current_leader,
                        Some(leader_id) if leader_id == 1 || leader_id == 2
                    )
                },
                "group should elect a surviving leader after node 3 stops",
            )
            .await
            .expect("wait for surviving leader");
        let leader_id = observer_raft
            .metrics()
            .borrow_watched()
            .current_leader
            .expect("surviving leader elected");
        let leader_index = usize::try_from(leader_id - 1).expect("leader id fits usize");
        let leader_node = nodes.get(leader_index).expect("leader node still running");
        let leader_raft = leader_node
            .registry
            .get(raft_group_id)
            .expect("leader group");
        let snapshot_log_id = leader_raft
            .metrics()
            .borrow_watched()
            .last_applied
            .expect("leader applied write while node 3 was down");
        leader_raft
            .trigger()
            .snapshot()
            .await
            .expect("trigger leader snapshot");
        leader_raft
            .wait(Some(Duration::from_secs(5)))
            .snapshot(
                snapshot_log_id,
                "leader snapshot includes quorum-only write",
            )
            .await
            .expect("wait for leader snapshot");
        leader_raft
            .trigger()
            .purge_log(snapshot_log_id.index())
            .await
            .expect("trigger leader purge");
        leader_raft
            .wait(Some(Duration::from_secs(5)))
            .purged(
                Some(snapshot_log_id),
                "leader purged snapshotted quorum-only write",
            )
            .await
            .expect("wait for leader purge");
    }

    let stale_listener = tokio::net::TcpListener::bind(addrs[2])
        .await
        .expect("rebind node 3 listener");
    let stale_replacement = spawn_static_grpc_test_node(
        3,
        stale_listener,
        peers.clone(),
        peers.clone(),
        true,
        6,
        StaticGrpcTestNodeStorage {
            per_group_initializers: true,
            ..Default::default()
        },
    )
    .await;
    stale_replacement
        .runtime
        .warm_all_groups()
        .await
        .expect("warm stale empty node 3 groups");

    // The leaders catch the emptied node 3 up by themselves, from the purged
    // snapshot. It stays a voter throughout.
    for (group_index, stream_id) in streams_by_group.iter().enumerate() {
        wait_raft_state_machine_payload(
            &stale_replacement.registry,
            stale_replacement.runtime.locate(stream_id),
            stream_id,
            format!("before-{group_index}-after-stop-{group_index}").as_bytes(),
            "emptied node healed from the surviving quorum",
        )
        .await;
    }

    stale_replacement.shutdown().await;

    let restarted_listener = tokio::net::TcpListener::bind(addrs[2])
        .await
        .expect("rebind node 3 after its second loss");
    let restarted = spawn_static_grpc_test_node(
        3,
        restarted_listener,
        peers.clone(),
        peers.clone(),
        true,
        6,
        StaticGrpcTestNodeStorage {
            per_group_initializers: true,
            ..Default::default()
        },
    )
    .await;
    restarted
        .runtime
        .warm_all_groups()
        .await
        .expect("warm final empty node 3 groups");

    // Restarted empty a second time, it is caught up again.
    for (group_index, stream_id) in streams_by_group.iter().enumerate() {
        wait_raft_state_machine_payload(
            &restarted.registry,
            restarted.runtime.locate(stream_id),
            stream_id,
            format!("before-{group_index}-after-stop-{group_index}").as_bytes(),
            "restarted empty node caught up from surviving quorum",
        )
        .await;
    }

    nodes.push(restarted);
    for node in nodes {
        node.shutdown().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn static_grpc_raft_group_engine_replicates_between_routers() {
    let mut listeners = Vec::new();
    let mut peers = Vec::new();
    for node_id in 1..=3u64 {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind listener");
        let addr = listener.local_addr().expect("local addr");
        peers.push((node_id, format!("http://{addr}")));
        listeners.push(listener);
    }

    let mut nodes = Vec::new();
    for (index, listener) in listeners.into_iter().enumerate() {
        let node_id = u64::try_from(index + 1).expect("node id fits u64");
        nodes.push(
            spawn_static_grpc_test_node(
                node_id,
                listener,
                peers.clone(),
                peers.clone(),
                node_id == 1,
                4,
                StaticGrpcTestNodeStorage::default(),
            )
            .await,
        );
    }

    for (index, node) in nodes.iter().enumerate().skip(1) {
        tokio::time::timeout(Duration::from_secs(10), node.runtime.warm_all_groups())
            .await
            .unwrap_or_else(|_| panic!("warm follower node {} groups timed out", index + 1))
            .expect("warm follower groups");
    }
    tokio::time::timeout(Duration::from_secs(10), nodes[0].runtime.warm_all_groups())
        .await
        .expect("warm initializing leader groups timed out")
        .expect("warm initializing leader groups");

    for raw_group_id in 0..4 {
        let raft_group_id = RaftGroupId(raw_group_id);
        for node in &nodes {
            let raft = node.registry.get(raft_group_id).expect("registered group");
            raft.wait(Some(Duration::from_secs(5)))
                .current_leader(1, "static gRPC Raft cluster should elect node 1")
                .await
                .expect("wait for shared leader");
        }
    }

    let http_client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("build reqwest client");
    let forwarded_stream = BucketStreamId::new("benchcmp", "follower-forward");
    // A write that lands on a follower is answered with a 307 to the leader;
    // the redirect-following client re-issues the PUT on the leader, which
    // creates the stream through raft.
    let follower_response = http_client
        .put(format!("{}/benchcmp/follower-forward", peers[1].1))
        .header(CONTENT_TYPE, "text/plain")
        .body("created-through-forward")
        .send()
        .await
        .expect("send follower write redirected to leader");
    assert_eq!(follower_response.status(), StatusCode::CREATED);
    assert_eq!(
        follower_response
            .headers()
            .get(names::STREAM_NEXT_OFFSET)
            .and_then(|value| value.to_str().ok()),
        Some("00000000000000000023")
    );

    let no_redirect_client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(Duration::from_secs(5))
        .build()
        .expect("build no-redirect reqwest client");
    let follower_sse = no_redirect_client
        .get(format!(
            "{}/benchcmp/follower-forward?offset=now&live=sse",
            peers[1].1
        ))
        .send()
        .await
        .expect("send follower live read");
    assert_eq!(follower_sse.status(), StatusCode::TEMPORARY_REDIRECT);
    assert!(matches!(
        typed_routing_reqwest(&follower_sse),
        ursula_headers::RoutingAnswer::NotLeader { leader, .. } if leader.get() == 1
    ));
    assert_eq!(
        follower_sse
            .headers()
            .get(names::X_URSULA_RAFT_LEADER_ID)
            .and_then(|value| value.to_str().ok()),
        Some("1")
    );
    assert!(
        follower_sse
            .headers()
            .get(LOCATION)
            .and_then(|value| value.to_str().ok())
            .is_some_and(|location| location.starts_with(&peers[0].1)
                && location.ends_with("/benchcmp/follower-forward?offset=now&live=sse"))
    );
    // A follower's answer to a HEAD, which it serves from the leader,
    // carries the server's identity too.
    let follower_head = no_redirect_client
        .head(format!("{}/benchcmp/follower-forward", peers[1].1))
        .send()
        .await
        .expect("send follower HEAD");
    assert_eq!(follower_head.status(), StatusCode::OK);
    let identity = ursula_headers::ServerIdentity::parse(
        follower_head
            .headers()
            .iter()
            .map(|(name, value)| (name.as_str(), value.as_bytes())),
    )
    .expect("well-formed identity")
    .expect("the identity on a follower's answer");
    assert_eq!(identity.protocol, ursula_headers::PROTOCOL_VERSION);

    let forwarded_placement = nodes[0].runtime.locate(&forwarded_stream);
    for node in &nodes {
        wait_raft_state_machine_payload(
            &node.registry,
            forwarded_placement,
            &forwarded_stream,
            b"created-through-forward",
            "follower forwarded write replicated",
        )
        .await;
    }

    // Once replication has caught up, a normal (non-live) read is served
    // locally by the follower without any redirect to the leader.
    let follower_read = http_client
        .get(format!("{}/benchcmp/follower-forward", peers[1].1))
        .send()
        .await
        .expect("send follower local read after replication");
    assert_eq!(follower_read.status(), StatusCode::OK);
    assert_eq!(
        follower_read
            .headers()
            .get(names::STREAM_NEXT_OFFSET)
            .and_then(|value| value.to_str().ok()),
        Some("00000000000000000023")
    );
    let follower_body = follower_read.bytes().await.expect("follower read body");
    assert_eq!(&follower_body[..], b"created-through-forward");

    let leader_sse = http_client
        .get(format!(
            "{}/benchcmp/follower-forward?offset=now&live=sse",
            peers[0].1
        ))
        .send()
        .await
        .expect("open leader SSE before follower append");
    assert_eq!(leader_sse.status(), StatusCode::OK);
    let sse_token = "wake-through-leader-runtime";
    let sse_task = tokio::spawn(async move {
        let mut response = leader_sse;
        let mut body = Vec::new();
        while let Some(chunk) = response.chunk().await.expect("SSE chunk") {
            body.extend_from_slice(&chunk);
            let body_text = String::from_utf8_lossy(&body);
            if body_text.contains(sse_token) {
                return body_text.into_owned();
            }
        }
        panic!("SSE stream ended before follower append data");
    });
    let follower_append = http_client
        .post(format!("{}/benchcmp/follower-forward", peers[1].1))
        .header(CONTENT_TYPE, "text/plain")
        .body(sse_token)
        .send()
        .await
        .expect("append through follower HTTP");
    assert_eq!(follower_append.status(), StatusCode::NO_CONTENT);
    let sse_body = tokio::time::timeout(Duration::from_secs(5), sse_task)
        .await
        .expect("leader SSE woke after follower append")
        .expect("SSE task");
    assert!(sse_body.contains("event: data"));
    assert!(sse_body.contains(sse_token));

    let (stream_group, stream_id) = (0..10_000)
        .map(|index| {
            let stream_id = BucketStreamId::new("benchcmp", format!("static-grpc-raft-{index}"));
            (nodes[0].runtime.locate(&stream_id).raft_group_id, stream_id)
        })
        .find(|(raft_group_id, _)| raft_group_id.0 == 0)
        .expect("find stream in raft group 0");
    nodes[0]
        .runtime
        .create_stream(CreateStreamRequest::new(
            stream_id.clone(),
            "application/octet-stream",
        ))
        .await
        .expect("create through leader runtime");
    nodes[0]
        .runtime
        .append(AppendRequest::from_bytes(
            stream_id.clone(),
            b"replicated-over-grpc".to_vec(),
        ))
        .await
        .expect("append through leader runtime");
    let stream_placement = nodes[0].runtime.locate(&stream_id);

    for node in &nodes {
        let raft = node.registry.get(stream_group).expect("registered group");
        raft.wait(Some(Duration::from_secs(5)))
            .current_leader(1, "static gRPC Raft group should keep node 1 leader")
            .await
            .expect("wait for stable leader after replicated application");
    }
    for node in &nodes {
        wait_raft_state_machine_payload(
            &node.registry,
            stream_placement,
            &stream_id,
            b"replicated-over-grpc",
            "leader write replicated over gRPC transport",
        )
        .await;
    }

    let mut group_streams = Vec::new();
    let mut seen_groups = BTreeMap::new();
    for index in 0..10_000 {
        let stream_id = BucketStreamId::new("benchcmp", format!("multi-group-{index}"));
        let placement = nodes[0].runtime.locate(&stream_id);
        if seen_groups
            .insert(placement.raft_group_id.0, stream_id.clone())
            .is_none()
        {
            group_streams.push((placement, stream_id));
        }
        if group_streams.len() == 4 {
            break;
        }
    }
    assert_eq!(group_streams.len(), 4);

    for (placement, stream_id) in &group_streams {
        let payload = format!("payload-for-group-{}", placement.raft_group_id.0).into_bytes();
        nodes[0]
            .runtime
            .create_stream(CreateStreamRequest::new(
                stream_id.clone(),
                "application/octet-stream",
            ))
            .await
            .expect("create multi-group stream through leader runtime");
        nodes[0]
            .runtime
            .append(AppendRequest::from_bytes(
                stream_id.clone(),
                payload.clone(),
            ))
            .await
            .expect("append multi-group stream through leader runtime");

        for node in &nodes {
            wait_raft_state_machine_payload(
                &node.registry,
                *placement,
                stream_id,
                &payload,
                "multi-group stream replicated over gRPC transport",
            )
            .await;
        }
    }

    let snapshot = nodes[0]
        .registry
        .build_snapshot_for_transfer(RaftGroupId(0))
        .await
        .expect("build leader snapshot");
    let vote = ursula_raft::UrsulaVote::new(1, 1);
    let mut snapshot_network = ursula_raft::GrpcRaftNetwork::new(
        peer_transport(&nodes[0].registry),
        RaftGroupId(0),
        2,
        peers[1].1.clone(),
    );
    let _: SnapshotResponse<UrsulaRaftTypeConfig> = snapshot_network
        .full_snapshot(
            vote,
            snapshot,
            std::future::pending::<ReplicationClosed>(),
            RPCOption::new(Duration::from_secs(1)),
        )
        .await
        .expect("send full snapshot over gRPC Raft network");

    for node in nodes {
        node.shutdown().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn static_grpc_raft_group_engine_replicates_with_core_journals() {
    let mut listeners = Vec::new();
    let mut peers = Vec::new();
    for node_id in 1..=3u64 {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind listener");
        let addr = listener.local_addr().expect("local addr");
        peers.push((node_id, format!("http://{addr}")));
        listeners.push(listener);
    }

    let raft_root = std::env::temp_dir().join(format!(
        "ursula-static-raft-multinode-log-test-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system time after unix epoch")
            .as_nanos()
    ));
    remove_test_path(&raft_root);

    let mut nodes = Vec::new();
    for (index, listener) in listeners.into_iter().enumerate() {
        let node_id = u64::try_from(index + 1).expect("node id fits u64");
        let node_root = raft_root.join(format!("node-{node_id}"));
        nodes.push(
            spawn_static_grpc_test_node(
                node_id,
                listener,
                peers.clone(),
                peers.clone(),
                node_id == 1,
                2,
                StaticGrpcTestNodeStorage {
                    raft_log_dir: Some(node_root),
                    ..Default::default()
                },
            )
            .await,
        );
    }

    for (index, node) in nodes.iter().enumerate().skip(1) {
        tokio::time::timeout(Duration::from_secs(10), node.runtime.warm_all_groups())
            .await
            .unwrap_or_else(|_| panic!("warm follower node {} groups timed out", index + 1))
            .expect("warm follower groups");
    }
    tokio::time::timeout(Duration::from_secs(10), nodes[0].runtime.warm_all_groups())
        .await
        .expect("warm initializing leader groups timed out")
        .expect("warm initializing leader groups");

    for raw_group_id in 0..2 {
        let raft_group_id = RaftGroupId(raw_group_id);
        for node in &nodes {
            let raft = node.registry.get(raft_group_id).expect("registered group");
            raft.wait(Some(Duration::from_secs(5)))
                .current_leader(1, "durable static gRPC Raft cluster should elect node 1")
                .await
                .expect("wait for shared leader");
        }
    }

    let mut group_streams = Vec::new();
    let mut seen_groups = BTreeMap::new();
    for index in 0..10_000 {
        let stream_id = BucketStreamId::new("benchcmp", format!("durable-multi-{index}"));
        let placement = nodes[0].runtime.locate(&stream_id);
        if seen_groups
            .insert(placement.raft_group_id.0, stream_id.clone())
            .is_none()
        {
            group_streams.push((placement, stream_id));
        }
        if group_streams.len() == 2 {
            break;
        }
    }
    assert_eq!(group_streams.len(), 2);

    for (placement, stream_id) in &group_streams {
        let payload = format!("durable-payload-for-group-{}", placement.raft_group_id.0);
        nodes[0]
            .runtime
            .create_stream(CreateStreamRequest::new(
                stream_id.clone(),
                "application/octet-stream",
            ))
            .await
            .expect("create durable multi-group stream through leader runtime");
        nodes[0]
            .runtime
            .append(AppendRequest::from_bytes(
                stream_id.clone(),
                payload.as_bytes().to_vec(),
            ))
            .await
            .expect("append durable multi-group stream through leader runtime");

        for node in &nodes {
            wait_raft_state_machine_payload(
                &node.registry,
                *placement,
                stream_id,
                payload.as_bytes(),
                "multi-node durable gRPC log replicated stream",
            )
            .await;
        }
    }

    for (node_index, (_, peer_url)) in peers.iter().enumerate() {
        let metrics_body = reqwest::get(format!("{peer_url}/__ursula/metrics"))
            .await
            .unwrap_or_else(|err| panic!("read node {} metrics: {err}", node_index + 1))
            .text()
            .await
            .expect("metrics body");
        assert!(
            !metrics_body.contains("\"wal_records\":0"),
            "node {} should record durable OpenRaft log writes: {metrics_body}",
            node_index + 1
        );

        let core_dir = raft_root
            .join(format!("node-{}", node_index + 1))
            .join("core-0");
        assert!(
            core_journal_record_bytes(&core_dir) > 0,
            "node journal should contain records"
        );
    }

    for node in nodes {
        node.shutdown().await;
    }
    std::fs::remove_dir_all(&raft_root).expect("remove raft root");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn static_grpc_raft_durable_cold_flush_replicates_manifest() {
    let mut listeners = Vec::new();
    let mut peers = Vec::new();
    for node_id in 1..=3u64 {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind listener");
        let addr = listener.local_addr().expect("local addr");
        peers.push((node_id, format!("http://{addr}")));
        listeners.push(listener);
    }

    let raft_root = std::env::temp_dir().join(format!(
        "ursula-static-raft-durable-cold-test-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system time after unix epoch")
            .as_nanos()
    ));
    remove_test_path(&raft_root);
    let cold_store = std::sync::Arc::new(ColdStore::memory().expect("memory cold store"));

    let mut nodes = Vec::new();
    for (index, listener) in listeners.into_iter().enumerate() {
        let node_id = u64::try_from(index + 1).expect("node id fits u64");
        let node_root = raft_root.join(format!("node-{node_id}"));
        nodes.push(
            spawn_static_grpc_test_node(
                node_id,
                listener,
                peers.clone(),
                peers.clone(),
                node_id == 1,
                1,
                StaticGrpcTestNodeStorage {
                    raft_log_dir: Some(node_root),
                    cold_store: Some(cold_store.clone()),
                    ..Default::default()
                },
            )
            .await,
        );
    }

    for (index, node) in nodes.iter().enumerate().skip(1) {
        tokio::time::timeout(Duration::from_secs(10), node.runtime.warm_all_groups())
            .await
            .unwrap_or_else(|_| panic!("warm follower node {} groups timed out", index + 1))
            .expect("warm follower groups");
    }
    tokio::time::timeout(Duration::from_secs(10), nodes[0].runtime.warm_all_groups())
        .await
        .expect("warm leader groups timed out")
        .expect("warm leader groups");

    for node in &nodes {
        let raft = node.registry.get(RaftGroupId(0)).expect("registered group");
        raft.wait(Some(Duration::from_secs(5)))
            .current_leader(1, "durable cold static gRPC cluster should elect node 1")
            .await
            .expect("wait for shared leader");
    }

    let stream_id = BucketStreamId::new("benchcmp", "durable-cold-manifest");
    let placement = nodes[0].runtime.locate(&stream_id);
    let payload = b"durable-cold-replicated-payload".to_vec();
    nodes[0]
        .runtime
        .create_stream(CreateStreamRequest::new(
            stream_id.clone(),
            "application/octet-stream",
        ))
        .await
        .expect("create cold stream through leader runtime");
    nodes[0]
        .runtime
        .append(AppendRequest::from_bytes(
            stream_id.clone(),
            payload.clone(),
        ))
        .await
        .expect("append cold stream through leader runtime");
    let flushed = nodes[0]
        .runtime
        .flush_cold_group_batch_once(
            placement.raft_group_id,
            PlanGroupColdFlushRequest {
                min_hot_bytes: 4,
                max_flush_bytes: 4,
                max_batch_bytes: payload.len(),
                pressure: None,
                max_hot_age: None,
            },
            8,
        )
        .await
        .expect("flush replicated cold manifest");
    assert!(
        flushed.len() >= 2,
        "batch cold flush should publish multiple chunks"
    );
    assert!(
        cold_store
            .list_cold_index_pages()
            .await
            .expect("list cold index pages")
            .is_empty(),
        "shared pack slices must not create per-stream cold-index pages"
    );

    for node in &nodes {
        wait_raft_state_machine_payload(
            &node.registry,
            placement,
            &stream_id,
            &payload,
            "multi-node durable gRPC cold manifest replicated stream",
        )
        .await;
        let raft = node
            .registry
            .get(placement.raft_group_id)
            .expect("registered raft group");
        let mut last_snapshot = None;
        for _ in 0..100 {
            let snapshot = raft
                .with_state_machine(|state_machine| {
                    Box::pin(async move { state_machine.group_snapshot().await })
                })
                .await
                .expect("snapshot node raft state machine")
                .expect("group snapshot");
            let entry = snapshot
                .stream_snapshot
                .streams
                .iter()
                .find(|entry| entry.metadata.stream_id == stream_id)
                .cloned()
                .expect("stream snapshot entry");
            if entry.payload.len() < payload.len() {
                last_snapshot = Some(entry);
                break;
            }
            last_snapshot = Some(entry);
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        let entry = last_snapshot.expect("stream snapshot entry");
        assert!(
            entry.payload.len() < payload.len(),
            "replicated hot payload should shrink after cold flush"
        );
    }

    for (node_index, _) in peers.iter().enumerate() {
        let core_dir = raft_root
            .join(format!("node-{}", node_index + 1))
            .join("core-0");
        assert!(
            core_journal_record_bytes(&core_dir) > 0,
            "node journal should contain records"
        );
    }

    let metrics_body = reqwest::get(format!("{}/__ursula/metrics", peers[0].1))
        .await
        .expect("read leader metrics")
        .text()
        .await
        .expect("metrics body");
    assert!(
        !metrics_body.contains("\"cold_flush_publishes\":0"),
        "leader should report cold metadata publishes: {metrics_body}"
    );
    assert!(
        !metrics_body.contains("\"wal_records\":0"),
        "leader should record durable OpenRaft log writes: {metrics_body}"
    );

    for node in nodes {
        node.shutdown().await;
    }
    std::fs::remove_dir_all(&raft_root).expect("remove raft root");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn static_grpc_raft_installs_snapshot_for_late_learner_over_tcp() {
    run_static_grpc_late_learner_snapshot_over_tcp(None).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn static_grpc_raft_installs_snapshot_for_late_learner_with_core_journals() {
    let raft_root = std::env::temp_dir().join(format!(
        "ursula-static-raft-late-learner-log-test-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system time after unix epoch")
            .as_nanos()
    ));
    remove_test_path(&raft_root);

    run_static_grpc_late_learner_snapshot_over_tcp(Some(raft_root.clone())).await;

    std::fs::remove_dir_all(&raft_root).expect("remove raft root");
}

async fn run_static_grpc_late_learner_snapshot_over_tcp(raft_root: Option<PathBuf>) {
    let listener1 = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind node 1 listener");
    let listener2 = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind node 2 listener");
    let listener3 = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind node 3 listener");
    let peers = vec![
        (
            1,
            format!("http://{}", listener1.local_addr().expect("node 1 addr")),
        ),
        (
            2,
            format!("http://{}", listener2.local_addr().expect("node 2 addr")),
        ),
        (
            3,
            format!("http://{}", listener3.local_addr().expect("node 3 addr")),
        ),
    ];
    let initial_peers = peers[..2].to_vec();
    let mut nodes = vec![
        spawn_static_grpc_test_node(
            1,
            listener1,
            initial_peers.clone(),
            peers.clone(),
            true,
            1,
            StaticGrpcTestNodeStorage {
                raft_log_dir: raft_root.as_ref().map(|root| root.join("node-1")),
                ..Default::default()
            },
        )
        .await,
        spawn_static_grpc_test_node(
            2,
            listener2,
            initial_peers.clone(),
            peers.clone(),
            false,
            1,
            StaticGrpcTestNodeStorage {
                raft_log_dir: raft_root.as_ref().map(|root| root.join("node-2")),
                ..Default::default()
            },
        )
        .await,
    ];

    nodes[1]
        .runtime
        .warm_all_groups()
        .await
        .expect("warm follower group");
    nodes[0]
        .runtime
        .warm_all_groups()
        .await
        .expect("warm leader group");

    for node in &nodes {
        let raft = node.registry.get(RaftGroupId(0)).expect("registered group");
        raft.wait(Some(Duration::from_secs(5)))
            .current_leader(1, "two-node static gRPC cluster should elect node 1")
            .await
            .expect("wait for node 1 leadership");
    }

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("build reqwest client");
    let stream_id = BucketStreamId::new("benchcmp", "late-learner-snapshot");
    let leader_base = peers[0].1.as_str();
    let create = client
        .put(format!("{leader_base}/benchcmp/late-learner-snapshot"))
        .header(CONTENT_TYPE, "application/octet-stream")
        .send()
        .await
        .expect("create stream through leader http");
    assert_eq!(create.status(), StatusCode::CREATED);
    let payload = b"snapshot-over-grpc".to_vec();
    let append = client
        .post(format!("{leader_base}/benchcmp/late-learner-snapshot"))
        .header(CONTENT_TYPE, "application/octet-stream")
        .body(payload.clone())
        .send()
        .await
        .expect("append stream through leader http");
    assert_eq!(append.status(), StatusCode::NO_CONTENT);

    let placement = nodes[0].runtime.locate(&stream_id);
    for node in &nodes {
        wait_raft_state_machine_payload(
            &node.registry,
            placement,
            &stream_id,
            &payload,
            "initial two-node write replicated before snapshot",
        )
        .await;
    }

    let leader_raft = nodes[0].registry.get(RaftGroupId(0)).expect("leader group");
    let leader_metrics = leader_raft.metrics().borrow_watched().clone();
    let snapshot_log_id = leader_metrics
        .last_applied
        .expect("leader applied stream append");
    leader_raft
        .trigger()
        .snapshot()
        .await
        .expect("trigger leader snapshot");
    leader_raft
        .wait(Some(Duration::from_secs(5)))
        .metrics(
            |metrics| {
                metrics
                    .snapshot
                    .as_ref()
                    .is_some_and(|snapshot| snapshot >= &snapshot_log_id)
            },
            format!("leader snapshot includes stream append .snapshot >= {snapshot_log_id}"),
        )
        .await
        .expect("wait for leader snapshot");
    leader_raft
        .trigger()
        .purge_log(snapshot_log_id.index())
        .await
        .expect("trigger leader purge");
    leader_raft
        .wait(Some(Duration::from_secs(5)))
        .purged(Some(snapshot_log_id), "leader purged snapshotted logs")
        .await
        .expect("wait for leader purge");

    nodes.push(
        spawn_static_grpc_test_node(
            3,
            listener3,
            peers.clone(),
            peers.clone(),
            false,
            1,
            StaticGrpcTestNodeStorage {
                raft_log_dir: raft_root.as_ref().map(|root| root.join("node-3")),
                ..Default::default()
            },
        )
        .await,
    );
    nodes[2]
        .runtime
        .warm_all_groups()
        .await
        .expect("warm late learner group");
    let late_learner = nodes[2]
        .registry
        .get(RaftGroupId(0))
        .expect("late learner group");
    let learner_added = leader_raft
        .add_learner(3, BasicNode::new(peers[2].1.clone()), true)
        .await
        .expect("add late learner over gRPC");
    late_learner
        .wait(Some(Duration::from_secs(10)))
        .metrics(
            |metrics| {
                metrics
                    .snapshot
                    .as_ref()
                    .is_some_and(|snapshot| snapshot >= &snapshot_log_id)
            },
            format!("late learner installed gRPC snapshot .snapshot >= {snapshot_log_id}"),
        )
        .await
        .expect("wait for late learner snapshot");
    late_learner
        .wait(Some(Duration::from_secs(10)))
        .applied_index_at_least(
            Some(learner_added.log_id.index()),
            "late learner applied learner membership",
        )
        .await
        .expect("wait for late learner catch-up");

    let promote = admin_test_request(
        &client,
        reqwest::Method::POST,
        format!("{leader_base}/__ursula/raft/0/membership?voters=1,2,3"),
    )
    .await
    .send()
    .await
    .expect("promote late learner");
    assert_eq!(promote.status(), StatusCode::OK);
    let promote_body = promote.text().await.expect("promote body");
    assert!(
        promote_body.contains("\"voter_ids\":[1,2,3]"),
        "promote response should include final voter set: {promote_body}"
    );
    let promote_json: serde_json::Value =
        serde_json::from_str(&promote_body).expect("decode promote response");
    let promote_index = promote_json
        .get("log_index")
        .and_then(serde_json::Value::as_u64)
        .expect("promote log index");
    late_learner
        .wait(Some(Duration::from_secs(10)))
        .applied_index_at_least(Some(promote_index), "late learner applied voter promotion")
        .await
        .expect("wait for late learner promotion");

    wait_raft_state_machine_payload(
        &nodes[2].registry,
        placement,
        &stream_id,
        &payload,
        "late learner restored stream from gRPC snapshot",
    )
    .await;

    let follower_read = client
        .get(format!(
            "{}/benchcmp/late-learner-snapshot?offset=0&max_bytes=64",
            peers[2].1
        ))
        .send()
        .await
        .expect("read stream from late learner http");
    assert_eq!(follower_read.status(), StatusCode::OK);
    let follower_body = follower_read.bytes().await.expect("late learner body");
    assert_eq!(&follower_body[..], &payload[..]);

    let leader_metrics_response = client
        .get(format!("{leader_base}/__ursula/metrics"))
        .send()
        .await
        .expect("read leader metrics");
    assert_eq!(leader_metrics_response.status(), StatusCode::OK);
    let leader_metrics_body = leader_metrics_response
        .text()
        .await
        .expect("leader metrics body");
    assert!(leader_metrics_body.contains("\"raft_group_count\":1"));
    assert!(leader_metrics_body.contains("\"raft_group_id\":0"));
    assert!(
        raft_group_metric_index_at_least(
            &leader_metrics_body,
            0,
            "snapshot",
            snapshot_log_id.index()
        ),
        "leader snapshot should cover append log id {snapshot_log_id}: {leader_metrics_body}"
    );
    assert!(
        raft_group_metric_index_at_least(
            &leader_metrics_body,
            0,
            "purged",
            snapshot_log_id.index()
        ),
        "leader purge should cover append log id {snapshot_log_id}: {leader_metrics_body}"
    );

    let late_metrics_response = client
        .get(format!("{}/__ursula/metrics", peers[2].1))
        .send()
        .await
        .expect("read late learner metrics");
    assert_eq!(late_metrics_response.status(), StatusCode::OK);
    let late_metrics_body = late_metrics_response
        .text()
        .await
        .expect("late learner metrics body");
    assert!(late_metrics_body.contains("\"raft_group_count\":1"));
    assert!(late_metrics_body.contains("\"raft_group_id\":0"));
    assert!(
        raft_group_metric_index_at_least(
            &late_metrics_body,
            0,
            "snapshot",
            snapshot_log_id.index()
        ),
        "late learner snapshot should cover append log id {snapshot_log_id}: {late_metrics_body}"
    );
    assert!(late_metrics_body.contains("\"voter_ids\":[1,2,3]"));
    assert!(late_metrics_body.contains("\"learner_ids\":[]"));

    if let Some(raft_root) = &raft_root {
        for node_id in 1..=3 {
            let core_dir = raft_root.join(format!("node-{node_id}")).join("core-0");
            assert!(
                core_journal_record_bytes(&core_dir) > 0,
                "node journal should contain records"
            );
        }
    }

    for node in nodes {
        node.shutdown().await;
    }
}

#[tokio::test]
async fn flush_cold_endpoint_uploads_and_reads_back_segments() {
    let cold_store = std::sync::Arc::new(ColdStore::memory().expect("memory cold store"));
    let runtime = ShardRuntime::spawn_with_engine_factory_and_cold_store(
        RuntimeConfig::new(1, 1),
        InMemoryGroupEngineFactory::with_cold_store(Some(cold_store.clone())),
        Some(cold_store),
    )
    .expect("runtime");
    let app = router(runtime);

    let response = http_put(
        &app,
        "/benchcmp/http-cold",
        &[(CONTENT_TYPE.as_str(), "text/plain")],
        Body::empty(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::CREATED);
    let response = http_post(
        &app,
        "/benchcmp/http-cold",
        &[(CONTENT_TYPE.as_str(), "text/plain")],
        Body::from("abcdef"),
    )
    .await;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    let response = http_post(
        &app,
        "/__ursula/flush-cold/benchcmp/http-cold?min_hot_bytes=4&max_bytes=4",
        &[],
        Body::empty(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);

    let response = http_get(&app, "/benchcmp/http-cold?offset=0&max_bytes=6").await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = body_bytes(response).await;
    assert_eq!(&body[..], b"abcdef");
}

#[tokio::test]
async fn cold_backpressure_returns_service_unavailable_and_metrics() {
    let cold_store = std::sync::Arc::new(ColdStore::memory().expect("memory cold store"));
    let runtime = ShardRuntime::spawn_with_engine_factory_and_cold_store(
        // F6c: a text/plain stream is charged its payload only.
        RuntimeConfig::new(1, 1).with_cold_max_hot_bytes_per_group(Some(4)),
        InMemoryGroupEngineFactory::with_cold_store(Some(cold_store.clone())),
        Some(cold_store),
    )
    .expect("runtime");
    let app = router(runtime);

    let response = http_put(
        &app,
        "/benchcmp/http-cold-backpressure",
        &[(CONTENT_TYPE.as_str(), "text/plain")],
        Body::empty(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::CREATED);

    let response = http_post(
        &app,
        "/benchcmp/http-cold-backpressure",
        &[(CONTENT_TYPE.as_str(), "text/plain")],
        Body::from("abcd"),
    )
    .await;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    let response = http_post(
        &app,
        "/benchcmp/http-cold-backpressure",
        &[(CONTENT_TYPE.as_str(), "text/plain")],
        Body::from("e"),
    )
    .await;
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        crate::render::error_code_of(response).await,
        ursula_headers::ErrorCode::ColdBackpressure
    );

    let response = http_get(&app, "/__ursula/metrics").await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = body_bytes(response).await;
    let body = std::str::from_utf8(&body).expect("utf8 body");
    assert!(body.contains("\"cold_hot_bytes\":4"));
    assert!(body.contains("\"cold_backpressure_events\":1"));
    assert!(body.contains("\"cold_backpressure_bytes\":1"));
    assert!(body.contains("\"cold_store\":{\"backend\":\"memory\""));
}

#[tokio::test]
async fn snapshot_and_bootstrap_routes_follow_extension_semantics() {
    let app = test_router();
    let stream_uri = "/benchcmp/snapshot-http";

    let response = http_put(
        &app,
        stream_uri,
        &[(CONTENT_TYPE.as_str(), "application/octet-stream")],
        Body::empty(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::CREATED);

    for payload in ["abc", "de"] {
        let response = http_post(
            &app,
            stream_uri,
            &[(CONTENT_TYPE.as_str(), "application/octet-stream")],
            Body::from(payload),
        )
        .await;
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
    }

    let response = http_put(
        &app,
        "/benchcmp/snapshot-http/snapshot/00000000000000000003",
        &[(CONTENT_TYPE.as_str(), "application/json")],
        Body::from(r#"{"state":"abc"}"#),
    )
    .await;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    assert_eq!(
        header_str(&response, names::STREAM_SNAPSHOT_OFFSET),
        "00000000000000000003"
    );

    let response = http_head(&app, stream_uri).await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        header_str(&response, names::STREAM_SNAPSHOT_OFFSET),
        "00000000000000000003"
    );

    let response = http_get(
        &app,
        "/benchcmp/snapshot-http/snapshot/00000000000000000003",
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(header_str(&response, CONTENT_TYPE), "application/json");
    assert_eq!(
        header_str(&response, names::STREAM_NEXT_OFFSET),
        "00000000000000000003"
    );
    let body = body_bytes(response).await;
    assert_eq!(&body[..], br#"{"state":"abc"}"#);

    let response = http_get(&app, "/benchcmp/snapshot-http?offset=0&max_bytes=1").await;
    assert_eq!(response.status(), StatusCode::OK);

    let response = http_put(
        &app,
        "/benchcmp/snapshot-http/retention/00000000000000000003",
        &[],
        Body::empty(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    let response = http_get(&app, "/benchcmp/snapshot-http?offset=0&max_bytes=1").await;
    assert_eq!(response.status(), StatusCode::GONE);
    assert_eq!(
        header_str(&response, names::STREAM_NEXT_OFFSET),
        "00000000000000000003"
    );

    let response = http_get(&app, "/benchcmp/snapshot-http/bootstrap").await;
    assert_eq!(response.status(), StatusCode::OK);
    assert!(
        header_str(&response, CONTENT_TYPE)
            .starts_with("multipart/mixed; boundary=ursula-bootstrap-")
    );
    assert_eq!(
        header_str(&response, names::STREAM_NEXT_OFFSET),
        "00000000000000000005"
    );
    let body = body_bytes(response).await;
    let body = std::str::from_utf8(&body).expect("multipart utf8");
    assert!(body.contains(r#"{"state":"abc"}"#));
    assert!(body.contains("de"));
}

#[tokio::test]
async fn bootstrap_without_snapshot_emits_empty_snapshot_part_and_rejects_live() {
    let app = test_router();
    let stream_uri = "/benchcmp/bootstrap-nosnapshot";

    let response = http_put(
        &app,
        stream_uri,
        &[(CONTENT_TYPE.as_str(), "application/octet-stream")],
        Body::empty(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::CREATED);

    let response = http_post(
        &app,
        stream_uri,
        &[(CONTENT_TYPE.as_str(), "application/octet-stream")],
        Body::from("one"),
    )
    .await;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    let response = http_head(&app, stream_uri).await;
    assert_eq!(response.status(), StatusCode::OK);
    assert!(
        response
            .headers()
            .get(names::STREAM_SNAPSHOT_OFFSET)
            .is_none()
    );

    let response = http_get(&app, "/benchcmp/bootstrap-nosnapshot/bootstrap?live=sse").await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let response = http_get(&app, "/benchcmp/bootstrap-nosnapshot/bootstrap").await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(header_str(&response, names::STREAM_SNAPSHOT_OFFSET), "-1");
    assert_eq!(
        header_str(&response, names::STREAM_NEXT_OFFSET),
        "00000000000000000003"
    );
    let body = body_bytes(response).await;
    let body = std::str::from_utf8(&body).expect("multipart utf8");
    assert!(body.contains("Content-Type: application/octet-stream\r\n\r\n\r\n--"));
    assert!(body.contains("one"));
}

fn cold_test_router() -> Router {
    let cold_store = std::sync::Arc::new(ColdStore::memory().expect("memory cold store"));
    let runtime = ShardRuntime::spawn_with_engine_factory_and_cold_store(
        RuntimeConfig::new(1, 1),
        InMemoryGroupEngineFactory::with_cold_store(Some(cold_store.clone())),
        Some(cold_store),
    )
    .expect("runtime");
    router(runtime)
}

/// Splits a bootstrap multipart body into its parts' payloads.
fn bootstrap_parts(response_content_type: &str, body: &[u8]) -> Vec<String> {
    let boundary = response_content_type
        .split("boundary=")
        .nth(1)
        .expect("multipart boundary");
    let body = std::str::from_utf8(body).expect("multipart utf8");
    let delimiter = format!("--{boundary}");
    body.split(delimiter.as_str())
        .skip(1)
        .filter(|part| !part.starts_with("--"))
        .map(|part| {
            let (_, payload) = part.split_once("\r\n\r\n").expect("part headers");
            payload
                .strip_suffix("\r\n")
                .expect("part terminator")
                .to_owned()
        })
        .collect()
}

async fn bootstrap_get(app: &Router, uri: &str) -> (Response, Vec<String>) {
    let response = http_get(app, uri).await;
    assert_eq!(response.status(), StatusCode::OK);
    let content_type = header_str(&response, CONTENT_TYPE).to_owned();
    let (parts, body) = response.into_parts();
    let bytes = to_bytes(body, usize::MAX).await.expect("body");
    let payloads = bootstrap_parts(&content_type, &bytes);
    (Response::from_parts(parts, Body::empty()), payloads)
}

async fn post_messages(app: &Router, uri: &str, content_type: &str, payloads: &[&str]) {
    for payload in payloads {
        let response = http_post(
            app,
            uri,
            &[(CONTENT_TYPE.as_str(), content_type)],
            Body::from(payload.to_string()),
        )
        .await;
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
    }
}

async fn flush_cold(app: &Router, stream_path: &str, max_bytes: u64) {
    let response = http_post(
        app,
        &format!("/__ursula/flush-cold{stream_path}?min_hot_bytes=1&max_bytes={max_bytes}"),
        &[],
        Body::empty(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
}

/// Regression: a cold flush past the snapshot made bootstrap drop the
/// collapsed messages after the snapshot while still claiming the tail and
/// `Stream-Up-To-Date: true`.
#[tokio::test]
async fn bootstrap_after_cold_flush_past_snapshot_is_honest_partial() {
    let app = cold_test_router();
    let stream_uri = "/benchcmp/bootstrap-cold-partial";
    let response = http_put(
        &app,
        stream_uri,
        &[(CONTENT_TYPE.as_str(), "application/octet-stream")],
        Body::empty(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::CREATED);
    post_messages(&app, stream_uri, "application/octet-stream", &[
        "abc", "de", "fg",
    ])
    .await;
    let response = http_put(
        &app,
        &format!("{stream_uri}/snapshot/00000000000000000003"),
        &[(CONTENT_TYPE.as_str(), "application/json")],
        Body::from(r#"{"state":"abc"}"#),
    )
    .await;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    flush_cold(&app, stream_uri, 5).await;

    let (response, parts) = bootstrap_get(&app, &format!("{stream_uri}/bootstrap")).await;
    assert_eq!(
        header_str(&response, names::STREAM_SNAPSHOT_OFFSET),
        "00000000000000000003"
    );
    assert_eq!(
        header_str(&response, names::STREAM_NEXT_OFFSET),
        "00000000000000000003"
    );
    assert!(response.headers().get(names::STREAM_UP_TO_DATE).is_none());
    assert!(response.headers().get(names::STREAM_CLOSED).is_none());
    assert_eq!(parts, vec![r#"{"state":"abc"}"#.to_owned()]);

    // The client continues with an ordinary read from the next offset.
    let response = http_get(&app, &format!("{stream_uri}?offset=00000000000000000003")).await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(&body_bytes(response).await[..], b"defg");

    // Once the snapshot is at or above the seal point, bootstrap is complete
    // again, with the hot range `[S, tail)` as one part.
    post_messages(&app, stream_uri, "application/octet-stream", &["hi", "jkl"]).await;
    let response = http_put(
        &app,
        &format!("{stream_uri}/snapshot/00000000000000000007"),
        &[(CONTENT_TYPE.as_str(), "application/json")],
        Body::from(r#"{"state":"abcdefg"}"#),
    )
    .await;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    let (response, parts) = bootstrap_get(&app, &format!("{stream_uri}/bootstrap")).await;
    assert_eq!(
        header_str(&response, names::STREAM_NEXT_OFFSET),
        "00000000000000000012"
    );
    assert_eq!(header_str(&response, names::STREAM_UP_TO_DATE), "true");
    assert_eq!(parts, vec![
        r#"{"state":"abcdefg"}"#.to_owned(),
        "hijkl".to_owned(),
    ]);
}

/// A binary stream has no message boundaries: a snapshot at a mid-message
/// offset answers 204 whether or not the bytes around it were flushed, and
/// bootstrap answers the hot range after it as one part.
#[tokio::test]
async fn binary_snapshot_mid_message_is_accepted_and_bootstrap_is_one_part() {
    let app = cold_test_router();
    let stream_uri = "/benchcmp/bootstrap-binary-one-part";
    let response = http_put(
        &app,
        stream_uri,
        &[(CONTENT_TYPE.as_str(), "application/octet-stream")],
        Body::empty(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::CREATED);
    post_messages(&app, stream_uri, "application/octet-stream", &[
        "abc", "de", "fg",
    ])
    .await;
    // Mid-message and hot.
    let response = http_put(
        &app,
        &format!("{stream_uri}/snapshot/00000000000000000001"),
        &[(CONTENT_TYPE.as_str(), "application/octet-stream")],
        Body::from("a"),
    )
    .await;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    let (response, parts) = bootstrap_get(&app, &format!("{stream_uri}/bootstrap")).await;
    assert_eq!(
        header_str(&response, names::STREAM_NEXT_OFFSET),
        "00000000000000000007"
    );
    assert_eq!(header_str(&response, names::STREAM_UP_TO_DATE), "true");
    assert_eq!(parts, vec!["a".to_owned(), "bcdefg".to_owned()]);

    // Mid-message after a flush into the second message.
    flush_cold(&app, stream_uri, 4).await;
    let response = http_put(
        &app,
        &format!("{stream_uri}/snapshot/00000000000000000004"),
        &[(CONTENT_TYPE.as_str(), "application/octet-stream")],
        Body::from("abcd"),
    )
    .await;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    let (response, parts) = bootstrap_get(&app, &format!("{stream_uri}/bootstrap")).await;
    assert_eq!(header_str(&response, names::STREAM_UP_TO_DATE), "true");
    assert_eq!(parts, vec!["abcd".to_owned(), "efg".to_owned()]);
}

/// Regression: without a snapshot (or with one at the retained offset), the
/// collapsed cold prefix came back as a single part holding many messages.
#[tokio::test]
async fn json_bootstrap_never_merges_cold_messages_into_one_part() {
    let app = cold_test_router();
    let stream_uri = "/benchcmp/bootstrap-cold-json";
    let response = http_put(
        &app,
        stream_uri,
        &[(CONTENT_TYPE.as_str(), "application/json")],
        Body::empty(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::CREATED);
    post_messages(&app, stream_uri, "application/json", &[
        r#"{"a":1}"#,
        r#"{"b":2}"#,
    ])
    .await;
    let response = http_head(&app, stream_uri).await;
    let flushed_tail = header_str(&response, names::STREAM_NEXT_OFFSET).to_owned();
    flush_cold(&app, stream_uri, 1024).await;

    let (response, parts) = bootstrap_get(&app, &format!("{stream_uri}/bootstrap")).await;
    assert_eq!(header_str(&response, names::STREAM_SNAPSHOT_OFFSET), "-1");
    assert_eq!(
        header_str(&response, names::STREAM_NEXT_OFFSET),
        "00000000000000000000"
    );
    assert!(response.headers().get(names::STREAM_UP_TO_DATE).is_none());
    assert_eq!(parts, vec![String::new()]);

    let response = http_get(&app, &format!("{stream_uri}?offset=-1")).await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = body_bytes(response).await;
    let body = std::str::from_utf8(&body).expect("json body");
    assert!(body.contains(r#"{"a":1}"#) && body.contains(r#"{"b":2}"#));

    post_messages(&app, stream_uri, "application/json", &[
        r#"{"c":3}"#,
        r#"{"d":4}"#,
    ])
    .await;
    let response = http_head(&app, stream_uri).await;
    let tail = header_str(&response, names::STREAM_NEXT_OFFSET).to_owned();
    // A snapshot at the flush point (a JSON boundary whose preceding byte is
    // cold, so the server verifies it with a read) bootstraps complete, one
    // JSON message per update part...
    let response = http_put(
        &app,
        &format!("{stream_uri}/snapshot/{flushed_tail}"),
        &[(CONTENT_TYPE.as_str(), "application/json")],
        Body::from(r#"{"n":2}"#),
    )
    .await;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    let (response, parts) = bootstrap_get(&app, &format!("{stream_uri}/bootstrap")).await;
    assert_eq!(header_str(&response, names::STREAM_NEXT_OFFSET), tail);
    assert_eq!(header_str(&response, names::STREAM_UP_TO_DATE), "true");
    assert_eq!(parts.len(), 3, "{parts:?}");
    assert_eq!(parts[0], r#"{"n":2}"#);
    assert!(parts[1].contains(r#"{"c":3}"#) && !parts[1].contains(r#"{"d":4}"#));
    assert!(parts[2].contains(r#"{"d":4}"#));

    // ...and so is a snapshot one message later.
    let after_c = format!(
        "{:020}",
        flushed_tail.parse::<u64>().expect("offset") + r#"{"c":3}"#.len() as u64 + 1
    );
    let response = http_put(
        &app,
        &format!("{stream_uri}/snapshot/{after_c}"),
        &[(CONTENT_TYPE.as_str(), "application/json")],
        Body::from(r#"{"n":3}"#),
    )
    .await;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    let (response, parts) = bootstrap_get(&app, &format!("{stream_uri}/bootstrap")).await;
    assert_eq!(header_str(&response, names::STREAM_NEXT_OFFSET), tail);
    assert_eq!(header_str(&response, names::STREAM_UP_TO_DATE), "true");
    assert_eq!(parts.len(), 2);
    assert_eq!(parts[0], r#"{"n":3}"#);
    assert!(parts[1].contains(r#"{"d":4}"#) && !parts[1].contains(r#"{"c":3}"#));
}

/// PR16 LF obligations: a JSON snapshot or retention offset must follow an
/// LF byte. A hot offset is checked at apply; a cold one by a leader read
/// before the server proposes again pinned to the stream incarnation. Both
/// refuse an intra-message offset with 400.
#[tokio::test]
async fn json_snapshot_and_retention_offsets_must_follow_an_lf_hot_and_cold() {
    let app = cold_test_router();
    let stream_uri = "/benchcmp/json-lf";
    let json = [(CONTENT_TYPE.as_str(), "application/json")];
    let response = http_put(&app, stream_uri, &json, Body::empty()).await;
    assert_eq!(response.status(), StatusCode::CREATED);
    // Two 8-byte messages, flushed, then two hot ones: [0, 16) cold, [16, 32) hot.
    post_messages(&app, stream_uri, "application/json", &[
        r#"{"a":1}"#,
        r#"{"b":2}"#,
    ])
    .await;
    flush_cold(&app, stream_uri, 1024).await;
    post_messages(&app, stream_uri, "application/json", &[
        r#"{"c":3}"#,
        r#"{"d":4}"#,
    ])
    .await;
    let put = |path: String| {
        let app = app.clone();
        async move { http_put(&app, &path, &json, Body::from("{}")).await }
    };

    for offset in [3u64, 20] {
        let response = put(format!("{stream_uri}/snapshot/{offset:020}")).await;
        assert_eq!(
            response.status(),
            StatusCode::BAD_REQUEST,
            "offset {offset}"
        );
    }
    let head = http_head(&app, stream_uri).await;
    assert!(head.headers().get(names::STREAM_SNAPSHOT_OFFSET).is_none());

    // Cold boundary (8) and hot boundary (24): accepted; retention is exact.
    for offset in [8u64, 24] {
        let response = put(format!("{stream_uri}/snapshot/{offset:020}")).await;
        assert_eq!(response.status(), StatusCode::NO_CONTENT, "offset {offset}");
    }
    // Intra-message retention offsets: hot (20) and cold (3) both answer 400.
    for offset in [20u64, 3] {
        let response = put(format!("{stream_uri}/retention/{offset:020}")).await;
        assert_eq!(
            response.status(),
            StatusCode::BAD_REQUEST,
            "offset {offset}"
        );
    }
    let response = put(format!("{stream_uri}/retention/{:020}", 8)).await;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    assert_eq!(
        header_str(&response, names::STREAM_RETAINED_OFFSET),
        format!("{:020}", 8)
    );
    let response = put(format!("{stream_uri}/retention/{:020}", 24)).await;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    assert_eq!(
        header_str(&response, names::STREAM_RETAINED_OFFSET),
        format!("{:020}", 24)
    );
}

/// A cold JSON boundary the server cannot read is refused, never accepted
/// unverified: the check fails closed with 503.
#[tokio::test]
async fn json_boundary_lookup_failure_answers_503() {
    let cold_store = Arc::new(
        ColdStore::memory()
            .expect("memory cold store")
            .without_read_cache(),
    );
    let runtime = ShardRuntime::spawn_with_engine_factory_and_cold_store(
        RuntimeConfig::new(1, 1),
        InMemoryGroupEngineFactory::with_cold_store(Some(cold_store.clone())),
        Some(cold_store.clone()),
    )
    .expect("runtime");
    let app = router(runtime);
    let stream_uri = "/benchcmp/json-lf-unreadable";
    let json = [(CONTENT_TYPE.as_str(), "application/json")];
    let response = http_put(&app, stream_uri, &json, Body::empty()).await;
    assert_eq!(response.status(), StatusCode::CREATED);
    post_messages(&app, stream_uri, "application/json", &[
        r#"{"a":1}"#,
        r#"{"b":2}"#,
    ])
    .await;
    flush_cold(&app, stream_uri, 1024).await;
    cold_store.set_fault_policy(|context| {
        (context.operation == ursula_runtime::ColdStoreOperation::ReadObjectRange)
            .then(|| ursula_runtime::ColdStoreFaultEffect::fail("injected read failure"))
    });

    let response = http_put(
        &app,
        &format!("{stream_uri}/snapshot/{:020}", 8),
        &json,
        Body::from("{}"),
    )
    .await;
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let head = http_head(&app, stream_uri).await;
    assert!(head.headers().get(names::STREAM_SNAPSHOT_OFFSET).is_none());
}

#[tokio::test]
async fn snapshot_publish_errors_and_overwrite_follow_extension_statuses() {
    let app = test_router();
    let stream_uri = "/benchcmp/snapshot-errors";

    let response = http_put(
        &app,
        stream_uri,
        &[(CONTENT_TYPE.as_str(), "application/octet-stream")],
        Body::empty(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::CREATED);

    for payload in ["abc", "de"] {
        let response = http_post(
            &app,
            stream_uri,
            &[(CONTENT_TYPE.as_str(), "application/octet-stream")],
            Body::from(payload),
        )
        .await;
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
    }

    let response = http_put(
        &app,
        "/benchcmp/snapshot-errors/snapshot/-1",
        &[],
        Body::empty(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    // A binary stream has no message boundaries: an offset inside the first
    // message is a valid snapshot offset.
    let response = http_put(
        &app,
        "/benchcmp/snapshot-errors/snapshot/00000000000000000002",
        &[],
        Body::from("ab-state"),
    )
    .await;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    let response = http_put(
        &app,
        "/benchcmp/snapshot-errors/snapshot/00000000000000000006",
        &[],
        Body::from("too-far"),
    )
    .await;
    assert_eq!(response.status(), StatusCode::CONFLICT);

    let response = http_put(
        &app,
        "/benchcmp/snapshot-errors/snapshot/00000000000000000003",
        &[],
        Body::from("abc-state"),
    )
    .await;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    let snapshot_digest = header_str(&response, names::STREAM_SNAPSHOT_DIGEST).to_owned();
    assert_eq!(snapshot_digest.len(), 64);

    let response = http_put(
        &app,
        "/benchcmp/snapshot-errors/snapshot/00000000000000000003",
        &[],
        Body::from("abc-state"),
    )
    .await;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    assert_eq!(
        header_str(&response, names::STREAM_SNAPSHOT_DIGEST),
        snapshot_digest
    );

    let response = http_put(
        &app,
        "/benchcmp/snapshot-errors/snapshot/00000000000000000003",
        &[],
        Body::from("different-state"),
    )
    .await;
    assert_eq!(response.status(), StatusCode::CONFLICT);

    let response = http_put(
        &app,
        "/benchcmp/snapshot-errors/snapshot/00000000000000000002",
        &[],
        Body::from("old-state"),
    )
    .await;
    assert_eq!(response.status(), StatusCode::CONFLICT);

    let response = http_put(
        &app,
        "/benchcmp/snapshot-errors/snapshot/00000000000000000005",
        &[],
        Body::from("abcde-state"),
    )
    .await;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    let response = http_get(
        &app,
        "/benchcmp/snapshot-errors/snapshot/00000000000000000003",
    )
    .await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    let response = http_get(&app, "/benchcmp/snapshot-errors?offset=3&max_bytes=2").await;
    assert_eq!(response.status(), StatusCode::OK);

    let response = http_put(
        &app,
        "/benchcmp/snapshot-errors/retention/00000000000000000005",
        &[],
        Body::empty(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    let response = http_get(&app, "/benchcmp/snapshot-errors?offset=3&max_bytes=2").await;
    assert_eq!(response.status(), StatusCode::GONE);

    let response = http_get(&app, "/benchcmp/snapshot-errors/bootstrap").await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        header_str(&response, names::STREAM_SNAPSHOT_OFFSET),
        "00000000000000000005"
    );
    let body = body_bytes(response).await;
    let body = std::str::from_utf8(&body).expect("multipart utf8");
    assert!(body.contains("abcde-state"));
    assert!(!body.contains("abc-state\r\n"));
}

fn test_router() -> Router {
    router(
        spawn_runtime(
            &test_config(2, 8),
            Persistence::InMemory,
            Topology::SingleNode {
                raft_group_count: 8,
            },
        )
        .expect("runtime")
        .runtime,
    )
}

#[tokio::test]
async fn client_router_does_not_serve_cluster_plane_via_grpc_service() {
    // The gRPC path `/ursula.raft.v1.RaftInternal/Append` has the same shape
    // as a client `/{bucket}/{stream}` URL, so axum's wildcard does match it
    // on the client router — but it lands in the regular `append_stream`
    // handler, not the gRPC RaftInternalServer. That handler returns a plain
    // 4xx because Producer headers are missing. The cluster router, by
    // contrast, dispatches it through the gRPC service whose error wire
    // format produces a different status. We assert the cluster router gives
    // a tonic-style response (200/415/501) while the client router stays in
    // HTTP append's error space (4xx, never 200).
    let state = HttpState::new(
        spawn_runtime(
            &test_config(1, 1),
            Persistence::InMemory,
            Topology::SingleNode {
                raft_group_count: 1,
            },
        )
        .expect("runtime")
        .runtime,
    );
    let client = client_router_with_admission(state.clone(), IngressAdmission::default());
    let response = http_post(&client, RAFT_GRPC_APPEND_PATH, &[], Body::empty()).await;
    let status = response.status().as_u16();
    assert!(
        (400..500).contains(&status),
        "cluster-plane gRPC path should land in client wildcard error path, got {status}"
    );
    let ct = response
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    assert!(
        !ct.contains("application/grpc"),
        "client router must not respond with a gRPC content-type, got {ct}"
    );
}

#[tokio::test]
async fn cluster_router_does_not_expose_client_plane_routes() {
    let state = HttpState::new(
        spawn_runtime(
            &test_config(1, 1),
            Persistence::InMemory,
            Topology::SingleNode {
                raft_group_count: 1,
            },
        )
        .expect("runtime")
        .runtime,
    );
    let cluster = cluster_router_from_state(state.clone());
    // A normal client append must be 404 on the cluster router.
    let response = http_post(
        &cluster,
        "/some-bucket/some-stream",
        &[("content-type", "application/octet-stream")],
        Body::from(b"payload".to_vec()),
    )
    .await;
    assert_eq!(
        response.status().as_u16(),
        404,
        "client-plane route leaked into cluster router"
    );
}

// Peers probe each other's egress at their Raft URL, which names the cluster
// listener when a node splits its planes, so the probe is a cluster route.
#[tokio::test]
async fn cluster_router_answers_the_egress_probe() {
    let state = HttpState::new(
        spawn_runtime(
            &test_config(1, 1),
            Persistence::InMemory,
            Topology::SingleNode {
                raft_group_count: 1,
            },
        )
        .expect("runtime")
        .runtime,
    );
    let cluster = cluster_router_from_state(state);
    let response = http_post(
        &cluster,
        crate::CLUSTER_PROBE_PATH,
        &[],
        Body::from(vec![0_u8; 64 * 1024]),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
}

// With one listener the cluster routes merge into the client router after
// its admission layer, so a node out of body budget still answers a peer's
// probe and is not judged to have lost its egress.
#[tokio::test]
async fn a_shared_listener_answers_the_egress_probe_under_write_pressure() {
    let state = HttpState::new(
        spawn_runtime(
            &test_config(1, 1),
            Persistence::InMemory,
            Topology::SingleNode {
                raft_group_count: 1,
            },
        )
        .expect("runtime")
        .runtime,
    );
    let server = ursula_config::ServerConfig {
        http_inflight_body_size: ursula_config::HumanSize::kib(1),
        ..ursula_config::ServerConfig::default()
    };
    let shared = cluster_router_from_state(state.clone()).merge(client_router_with_admission(
        state,
        IngressAdmission::new(&server),
    ));
    // The client plane refuses a 64 KiB write body for the budget.
    let refused = http_put(
        &shared,
        "/pressured-bucket",
        &[("content-length", "65536")],
        Body::from(vec![0_u8; 64 * 1024]),
    )
    .await;
    assert_eq!(refused.status(), StatusCode::SERVICE_UNAVAILABLE);
    let response = http_post(
        &shared,
        crate::CLUSTER_PROBE_PATH,
        &[("content-length", "65536")],
        Body::from(vec![0_u8; 64 * 1024]),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn cluster_router_reports_leadership_shed_policy() {
    let registry = RaftGroupHandleRegistry::default();
    registry.mark_leadership_shed(ursula_raft::LeadershipShedReason::ColdHealth);
    let state = HttpState::with_raft_registry(
        spawn_runtime(
            &test_config(1, 1),
            Persistence::InMemory,
            Topology::SingleNode {
                raft_group_count: 1,
            },
        )
        .expect("runtime")
        .runtime,
        registry,
    );
    let cluster = cluster_router_from_state(state);
    let response = http_get(&cluster, LEADERSHIP_SHED_PATH).await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = body_bytes(response).await;
    let body = std::str::from_utf8(&body).expect("status utf8");
    assert!(body.contains("\"state\":\"cold-health\""), "{body}");
    assert!(body.contains("\"should_accept_transfer\":true"), "{body}");
    assert!(body.contains("\"should_campaign\":true"), "{body}");
    assert!(
        body.contains("\"should_shed_current_leaders\":true"),
        "{body}"
    );
}

#[tokio::test]
async fn maintenance_drain_endpoint_marks_and_clears_leadership_shed() {
    let registry = RaftGroupHandleRegistry::default();
    let state = HttpState::with_raft_registry(
        spawn_runtime(
            &test_config(1, 1),
            Persistence::InMemory,
            Topology::SingleNode {
                raft_group_count: 1,
            },
        )
        .expect("runtime")
        .runtime,
        registry,
    );
    let router = cluster_router_from_state(state.clone())
        .merge(admin_ops_router(state.clone()))
        .merge(client_router_with_admission(
            state,
            IngressAdmission::default(),
        ));

    let response = http_post(
        &router,
        "/__ursula/leadership-shed/maintenance",
        &[],
        Body::empty(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = body_bytes(response).await;
    let body = std::str::from_utf8(&body).expect("status utf8");
    assert!(body.contains("\"state\":\"maintenance-drain\""), "{body}");
    assert!(body.contains("\"should_accept_transfer\":false"), "{body}");
    assert!(body.contains("\"should_campaign\":false"), "{body}");

    let response = http_delete(&router, "/__ursula/leadership-shed/maintenance").await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = body_bytes(response).await;
    let body = std::str::from_utf8(&body).expect("status utf8");
    assert!(body.contains("\"state\":\"none\""), "{body}");
    assert!(body.contains("\"should_accept_transfer\":true"), "{body}");
    assert!(body.contains("\"should_campaign\":true"), "{body}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn startup_maintenance_drain_disables_groups_registered_after_the_fence() {
    let mut listeners = Vec::new();
    let mut peers = Vec::new();
    for node_id in 1..=3u64 {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind listener");
        peers.push((
            node_id,
            format!("http://{}", listener.local_addr().expect("listener addr")),
        ));
        listeners.push(listener);
    }

    let mut nodes = Vec::new();
    for (index, listener) in listeners.into_iter().enumerate() {
        nodes.push(
            spawn_static_grpc_test_node(
                u64::try_from(index + 1).expect("node id fits u64"),
                listener,
                peers.clone(),
                peers.clone(),
                true,
                1,
                StaticGrpcTestNodeStorage {
                    per_group_initializers: true,
                    start_maintenance_drained: index == 2,
                    ..Default::default()
                },
            )
            .await,
        );
    }
    for node in &nodes {
        node.runtime
            .warm_all_groups()
            .await
            .expect("warm raft group");
    }
    for node in &nodes {
        node.registry
            .get(RaftGroupId(0))
            .expect("registered group")
            .wait(Some(Duration::from_secs(5)))
            .current_leader(1, "group 0 initializer elected node 1")
            .await
            .expect("wait for initial leader");
    }

    let follower = nodes[2]
        .registry
        .get(RaftGroupId(0))
        .expect("drained follower group");
    // Do not let the separate recovery gate hide a broken maintenance fence.
    // Every replica must finish recovery while the leader still ticks.
    tokio::time::timeout(Duration::from_secs(10), async {
        while nodes
            .iter()
            .any(|node| !node.registry.recovery_barriers_ready())
        {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("all recovery barriers must open before testing the maintenance fence");
    // Stop both other Raft engines so only the drained follower can campaign.
    // Disabling a peer's election flag can be overwritten by recovery policy;
    // disabling its ticker alone does not stop all outbound replication.
    for node in &nodes[..2] {
        node.registry
            .get(RaftGroupId(0))
            .expect("other peer group")
            .set_tick(false)
            .await
            .expect("disable peer ticker on owner");
    }
    // Capture before shutdown: an unfenced follower can start its election
    // while we are awaiting the two peers' shutdown acknowledgements.
    let term_before = follower.metrics().borrow_watched().current_term;
    for node in &nodes[..2] {
        node.registry
            .get(RaftGroupId(0))
            .expect("other peer group")
            .shutdown()
            .await
            .expect("stop other peer raft engine");
    }
    tokio::time::sleep(Duration::from_secs(15)).await;

    let metrics = follower.metrics().borrow_watched().clone();
    assert_eq!(
        metrics.current_term, term_before,
        "maintenance-drained follower must not campaign after its leader lease expires: {metrics:?}"
    );
    assert_ne!(metrics.current_leader, Some(3));
    assert_ne!(
        metrics.vote.leader_id.node_id, 3,
        "maintenance-drained follower must not cast a self-vote, even without enough peers to become leader: {metrics:?}"
    );

    for node in nodes {
        node.shutdown().await;
    }
}

#[tokio::test]
async fn merged_router_serves_both_planes() {
    // The single-listener router (backwards compat / in-process tests) must
    // still answer both client and cluster routes from one bind.
    let state = HttpState::new(
        spawn_runtime(
            &test_config(1, 1),
            Persistence::InMemory,
            Topology::SingleNode {
                raft_group_count: 1,
            },
        )
        .expect("runtime")
        .runtime,
    );
    let merged = cluster_router_from_state(state.clone())
        .merge(admin_ops_router(state.clone()))
        .merge(client_router_with_admission(
            state,
            IngressAdmission::default(),
        ));

    // Client-plane: HEAD on an unknown stream returns 404 (route mounted).
    let head = http_head(&merged, "/some-bucket/unknown-stream").await;
    assert_ne!(head.status().as_u16(), 405, "client HEAD route missing");

    // Cluster-plane: the gRPC path is reachable (not 404). It will fail later
    // due to missing protobuf headers, but the route itself must be mounted.
    let grpc = http_post(&merged, RAFT_GRPC_APPEND_PATH, &[], Body::empty()).await;
    assert_ne!(
        grpc.status().as_u16(),
        404,
        "merged router missing cluster gRPC path"
    );
}

#[tokio::test]
async fn http_state_wall_clock_drives_protocol_now_ms() {
    let now_ms = Arc::new(AtomicU64::new(1_000));
    let state = HttpState::new(
        spawn_runtime(
            &test_config(1, 1),
            Persistence::InMemory,
            Topology::SingleNode {
                raft_group_count: 1,
            },
        )
        .expect("runtime")
        .runtime,
    )
    .with_wall_clock(TestWallClock {
        now_ms: Arc::clone(&now_ms),
    });
    let app = cluster_router_from_state(state.clone()).merge(client_router_with_admission(
        state,
        IngressAdmission::default(),
    ));

    let response = http_put(
        &app,
        "/benchcmp/clocked-stream",
        &[
            (CONTENT_TYPE.as_str(), "text/plain"),
            (HEADER_STREAM_TTL, "1"),
        ],
        Body::empty(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::CREATED);

    now_ms.store(1_999, Ordering::Relaxed);
    let response = http_head(&app, "/benchcmp/clocked-stream").await;
    assert_eq!(response.status(), StatusCode::OK);

    now_ms.store(2_000, Ordering::Relaxed);
    let response = http_head(&app, "/benchcmp/clocked-stream").await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

/// A live read with a `Stream-Incarnation` precondition ends when its
/// stream is recreated, here after TTL expiry, which deletes nothing a
/// waiter would notice (D12).
#[tokio::test]
async fn pinned_long_poll_ends_when_an_expired_stream_is_recreated() {
    let now_ms = Arc::new(AtomicU64::new(1_000));
    let state = HttpState::new(
        spawn_runtime(
            &test_config(1, 1),
            Persistence::InMemory,
            Topology::SingleNode {
                raft_group_count: 1,
            },
        )
        .expect("runtime")
        .runtime,
    )
    .with_wall_clock(TestWallClock {
        now_ms: Arc::clone(&now_ms),
    });
    let app = cluster_router_from_state(state.clone()).merge(client_router_with_admission(
        state,
        IngressAdmission::default(),
    ));
    let uri = "/benchcmp/pinned-live-read";
    let response = http_put(
        &app,
        uri,
        &[
            (CONTENT_TYPE.as_str(), "text/plain"),
            (HEADER_STREAM_TTL, "1"),
        ],
        Body::empty(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::CREATED);
    let old = header_str(&response, names::STREAM_INCARNATION).to_owned();
    let tail = header_str(&response, names::STREAM_NEXT_OFFSET).to_owned();

    let poll = tokio::spawn({
        let app = app.clone();
        let old = old.clone();
        async move {
            send(
                &app,
                "GET",
                &format!("{uri}?offset={tail}&live=long-poll&timeout_ms=30000"),
                &[(names::STREAM_INCARNATION, old.as_str())],
                Body::empty(),
            )
            .await
        }
    });
    // Let the poll park before the stream expires and is created again.
    tokio::time::sleep(Duration::from_millis(100)).await;
    now_ms.store(2_000, Ordering::Relaxed);
    let response = http_put(
        &app,
        uri,
        &[(CONTENT_TYPE.as_str(), "text/plain")],
        Body::empty(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::CREATED);
    let current = header_str(&response, names::STREAM_INCARNATION).to_owned();
    assert_ne!(current, old);

    let response = tokio::time::timeout(Duration::from_secs(10), poll)
        .await
        .expect("the pinned long-poll ends on recreate")
        .expect("poll task");
    assert_eq!(response.status(), StatusCode::PRECONDITION_FAILED);
    assert_eq!(header_str(&response, names::STREAM_INCARNATION), current);
}

/// A `DELETE` of a stream that expired but is still in its group answers
/// `404`, as for a missing stream, with and without `Stream-Incarnation`,
/// whether or not the expiry sweep ran. It removes the stream like any
/// delete, so a long-poll on the stream ends at once.
#[tokio::test]
async fn delete_of_an_expired_stream_answers_404_and_ends_its_long_poll() {
    let now_ms = Arc::new(AtomicU64::new(1_000));
    let state = HttpState::new(
        spawn_runtime(
            &test_config(1, 1),
            Persistence::InMemory,
            Topology::SingleNode {
                raft_group_count: 1,
            },
        )
        .expect("runtime")
        .runtime,
    )
    .with_wall_clock(TestWallClock {
        now_ms: Arc::clone(&now_ms),
    });
    let app = cluster_router_from_state(state.clone()).merge(client_router_with_admission(
        state,
        IngressAdmission::default(),
    ));
    let plain = "/benchcmp/expired-delete";
    let tokened = "/benchcmp/expired-delete-tokened";
    let mut tails = Vec::new();
    let mut tokens = Vec::new();
    for uri in [plain, tokened] {
        let response = http_put(
            &app,
            uri,
            &[
                (CONTENT_TYPE.as_str(), "text/plain"),
                (HEADER_STREAM_TTL, "1"),
            ],
            Body::empty(),
        )
        .await;
        assert_eq!(response.status(), StatusCode::CREATED);
        tails.push(header_str(&response, names::STREAM_NEXT_OFFSET).to_owned());
        tokens.push(header_str(&response, names::STREAM_INCARNATION).to_owned());
    }

    let poll = tokio::spawn({
        let app = app.clone();
        let tail = tails[0].clone();
        async move {
            send(
                &app,
                "GET",
                &format!("{plain}?offset={tail}&live=long-poll&timeout_ms=30000"),
                &[],
                Body::empty(),
            )
            .await
        }
    });
    // Let the poll park before the stream expires.
    tokio::time::sleep(Duration::from_millis(100)).await;
    now_ms.store(2_000, Ordering::Relaxed);

    let response = send(&app, "DELETE", plain, &[], Body::empty()).await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    let response = tokio::time::timeout(Duration::from_secs(10), poll)
        .await
        .expect("the long-poll ends on the delete")
        .expect("poll task");
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    let response = send(&app, "DELETE", plain, &[], Body::empty()).await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    let response = send(
        &app,
        "DELETE",
        tokened,
        &[(names::STREAM_INCARNATION, tokens[1].as_str())],
        Body::empty(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    let response = http_head(&app, tokened).await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

/// Ingress admission as the server builds it, with an in-flight body budget
/// of `bytes`.
fn admission_with_body_budget(bytes: u64) -> IngressAdmission {
    IngressAdmission::new(&ursula_config::ServerConfig {
        http_inflight_body_size: ursula_config::HumanSize::bytes(bytes),
        ..ursula_config::ServerConfig::default()
    })
}

// Base-contract pin; see base_contract_tests.rs.
#[tokio::test]
async fn ingress_body_budget_rejects_write_when_budget_is_exhausted() {
    let app = Router::new()
        .route(
            "/write",
            post(|_body: Bytes| async { StatusCode::NO_CONTENT }),
        )
        .layer(admission_with_body_budget(4));

    let response = http_post(
        &app,
        "/write",
        &[(CONTENT_LENGTH.as_str(), "5")],
        Body::from("abcde"),
    )
    .await;

    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(header_str(&response, axum::http::header::RETRY_AFTER), "1");
    // Refused before the handler ran: nothing was applied, and nothing says
    // so.
    assert_no_not_applied_headers(response.headers(), "ingress budget");
    assert_eq!(
        crate::render::error_code_of(response).await,
        ursula_headers::ErrorCode::IngressBodyBytesLimitReached
    );
}

#[tokio::test]
async fn ingress_body_budget_holds_credit_until_response_finishes() {
    let entered = Arc::new(tokio::sync::Barrier::new(2));
    let release = Arc::new(tokio::sync::Notify::new());
    let app = Router::new()
        .route(
            "/write",
            post({
                let entered = entered.clone();
                let release = release.clone();
                move |_body: Bytes| {
                    let entered = entered.clone();
                    let release = release.clone();
                    async move {
                        entered.wait().await;
                        release.notified().await;
                        StatusCode::NO_CONTENT
                    }
                }
            }),
        )
        .layer(admission_with_body_budget(4));

    let first = tokio::spawn({
        let app = app.clone();
        async move {
            http_post(
                &app,
                "/write",
                &[(CONTENT_LENGTH.as_str(), "4")],
                Body::from("abcd"),
            )
            .await
        }
    });
    entered.wait().await;

    let second = http_post(
        &app,
        "/write",
        &[(CONTENT_LENGTH.as_str(), "1")],
        Body::from("e"),
    )
    .await;
    assert_eq!(second.status(), StatusCode::SERVICE_UNAVAILABLE);

    release.notify_one();
    assert_eq!(
        first.await.expect("first join").status(),
        StatusCode::NO_CONTENT
    );
}

#[tokio::test]
async fn an_unproven_recovery_cannot_count_as_ready_after_undrain() {
    // A replica on an empty WAL starts gated.
    let wal_root = tempfile::tempdir().expect("WAL root");
    let store = ursula_raft::RaftWal::start(
        wal_root.path(),
        ursula_config::WalFsync::Never,
        &ursula_shard::StaticShardMap::new(1, 1).expect("valid topology"),
    )
    .expect("start the Raft WAL")
    .open(
        ursula_shard::ShardPlacement {
            core_id: ursula_shard::CoreId(0),
            shard_id: ursula_shard::ShardId(0),
            raft_group_id: RaftGroupId(0),
        },
        ursula_runtime::RuntimeMetrics::new(1, 1).group_engine_metrics(),
    )
    .expect("open the log store");
    let placement = ursula_shard::ShardPlacement {
        core_id: ursula_shard::CoreId(0),
        shard_id: ursula_shard::ShardId(0),
        raft_group_id: RaftGroupId(0),
    };
    let gate = Arc::new(
        ursula_raft::GroupRejoin::durable(1, RaftGroupId(0), &store)
            .await
            .expect("open the gate"),
    );
    let engine = ursula_raft::RaftGroupEngine::new_node(
        placement,
        1,
        Arc::new(
            openraft::Config {
                enable_tick: false,
                ..Default::default()
            }
            .validate()
            .unwrap(),
        ),
        ursula_raft::SingleNodeRaftNetworkFactory,
        store,
        gated_engine_options(&gate),
    )
    .await
    .expect("recovering engine");
    let registry = RaftGroupHandleRegistry::default();
    engine.publish_recovery(gate, &registry).unwrap();
    let runtime = spawn_runtime(
        &test_config(1, 1),
        Persistence::InMemory,
        Topology::SingleNode {
            raft_group_count: 1,
        },
    )
    .expect("runtime")
    .runtime;
    let app = client_router_with_admission(
        HttpState::with_raft_registry(runtime, registry.clone()),
        IngressAdmission::default(),
    );
    registry.mark_leadership_shed(ursula_raft::LeadershipShedReason::MaintenanceDrain);
    registry.clear_leadership_shed(ursula_raft::LeadershipShedReason::MaintenanceDrain);
    let ready = http_get(&app, READINESS_PATH).await;
    assert_eq!(ready.status(), StatusCode::SERVICE_UNAVAILABLE);
    let body: serde_json::Value = serde_json::from_slice(&body_bytes(ready).await).unwrap();
    assert_eq!(body["reason"], json!("recovery_gate_closed"));
    assert_eq!(body["recovery_barriers_ready"], json!(false));
    engine.shutdown().await.unwrap();
}

/// `accept-unsynced-loss` is an incarnation-bound admin mutation that takes
/// a recovery plan (#581 section 4.5), only for the log the tool saw in the
/// metrics. A plan electing this replica opens its gate, records the plan
/// durably, reports it in the metrics, and clears the readiness reason the
/// closed gate gave.
#[tokio::test]
async fn a_recovery_plan_opens_the_gate_for_the_observed_log_only() {
    use openraft::entry::RaftEntry;
    use openraft::storage::RaftLogReader;
    use openraft::storage::RaftLogStorage;

    for retained_vote in [false, true] {
        let placement = ursula_shard::ShardPlacement {
            core_id: ursula_shard::CoreId(0),
            shard_id: ursula_shard::ShardId(0),
            raft_group_id: RaftGroupId(0),
        };
        // A missing log with either retained vote metadata or lost vote history.
        // Only the former may accept an unsynced-tail loss.
        let wal_root = tempfile::tempdir().expect("WAL root");
        let mut store = ursula_raft::RaftWal::start(
            wal_root.path(),
            ursula_config::WalFsync::Never,
            &ursula_shard::StaticShardMap::new(1, 1).expect("valid topology"),
        )
        .expect("start the Raft WAL")
        .open(
            placement,
            ursula_runtime::RuntimeMetrics::new(1, 1).group_engine_metrics(),
        )
        .expect("open the log store");
        if retained_vote {
            store
                .save_vote(&ursula_raft::UrsulaVote::new(1, 2))
                .await
                .expect("persist retained vote metadata");
        }
        let gate = Arc::new(
            ursula_raft::GroupRejoin::durable(1, RaftGroupId(0), &store)
                .await
                .expect("open the gate"),
        );
        // The log kept the group's membership: a plan elects one of its
        // voters. The gate is closed, so the entry records it recovering.
        let (flushed, appended) =
            <ursula_raft::UrsulaRaftTypeConfig as openraft::type_config::TypeConfigExt>::oneshot();
        store
            .append(
                [
                    openraft::alias::EntryOf::<ursula_raft::UrsulaRaftTypeConfig>::new(
                        openraft::LogId::new(openraft::vote::RaftLeaderId::new(0, 0), 0),
                        openraft::EntryPayload::Membership(
                            openraft::Membership::new(
                                // Three voters, so that a plan may cut; only
                                // this one runs.
                                vec![std::collections::BTreeSet::from([1, 2, 3])],
                                BTreeMap::from([
                                    (1, openraft::BasicNode::new("local")),
                                    (2, openraft::BasicNode::new("absent")),
                                    (3, openraft::BasicNode::new("absent")),
                                ]),
                            )
                            .expect("membership"),
                        ),
                    ),
                ],
                openraft::storage::IOFlushed::signal(flushed),
            )
            .await
            .expect("submit the membership entry");
        appended
            .await
            .expect("flush callback")
            .expect("append the membership entry");
        let engine = ursula_raft::RaftGroupEngine::new_node(
            placement,
            1,
            Arc::new(
                openraft::Config {
                    enable_tick: false,
                    ..Default::default()
                }
                .validate()
                .unwrap(),
            ),
            ursula_raft::SingleNodeRaftNetworkFactory,
            store.clone(),
            gated_engine_options(&gate),
        )
        .await
        .expect("recovering group");
        if retained_vote {
            engine
                .raft_handle()
                .wait(Some(std::time::Duration::from_secs(2)))
                .metrics(
                    |metrics| metrics.vote == ursula_raft::UrsulaVote::new(1, 2),
                    "retained vote restored before observing the admin CAS",
                )
                .await
                .expect("restore retained vote");
        }
        let registry = RaftGroupHandleRegistry::default();
        engine.publish_recovery(gate.clone(), &registry).unwrap();
        let runtime = spawn_runtime(
            &test_config(1, 1),
            Persistence::InMemory,
            Topology::SingleNode {
                raft_group_count: 1,
            },
        )
        .expect("runtime")
        .runtime;
        let state = HttpState::with_raft_registry(runtime, registry.clone());
        let client = client_router_with_admission(state.clone(), IngressAdmission::default());
        let admin = admin_router(state.clone());
        // Operator tooling reads the same readiness through its admin tunnel.
        for app in [&client, &admin] {
            let ready = http_get(app, READINESS_PATH).await;
            assert_eq!(ready.status(), StatusCode::SERVICE_UNAVAILABLE);
            let body: ursula_proto::admin::ServingReadiness =
                serde_json::from_slice(&body_bytes(ready).await).unwrap();
            assert_eq!(
                body.reason,
                Some(ursula_proto::admin::ServingReadinessReason::RecoveryGateClosed)
            );
        }

        let path = "/__ursula/raft/0/recovery/accept-unsynced-loss";
        let json_body = &[("content-type", "application/json")];
        let metrics: serde_json::Value =
            serde_json::from_slice(&body_bytes(http_get(&admin, "/__ursula/metrics").await).await)
                .expect("metrics JSON");
        let group = &metrics["raft_groups"][0];
        let seen = ursula_proto::admin::ReplicaLogView {
            last_log_index: group["last_log_index"].as_u64(),
            last_log_term: group["last_log_term"].as_u64(),
            current_term: group["current_term"].as_u64().expect("current term"),
        };
        let plan = |expected: ursula_proto::admin::ReplicaLogView| {
            ursula_proto::admin::RecoveryPlanRequest {
                point_entry: None,
                leader: 1,
                point: expected.last_log_index.unwrap_or(0),
                cut: false,
                step: ursula_proto::admin::PlanStep::Lead {
                    highest_term: expected.current_term,
                },
                expected,
            }
        };
        let accept = |expected: ursula_proto::admin::ReplicaLogView| {
            Body::from(serde_json::to_vec(&plan(expected)).expect("request JSON"))
        };

        let unbound = admin
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(path)
                    .header("content-type", "application/json")
                    .body(accept(seen))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(unbound.status(), StatusCode::PRECONDITION_REQUIRED);
        let unnamed = http_post(&admin, path, json_body, Body::from("{}")).await;
        assert_eq!(unnamed.status(), StatusCode::UNPROCESSABLE_ENTITY);
        // A 0.7 body is no plan.
        let old = http_post(
            &admin,
            path,
            json_body,
            Body::from(r#"{"expected_last_log_index": null, "expected_current_term": 0}"#),
        )
        .await;
        assert_eq!(old.status(), StatusCode::UNPROCESSABLE_ENTITY);
        // The gate awaits a barrier: it may still open without losing anything.
        let awaiting = http_post(&admin, path, json_body, accept(seen)).await;
        assert_eq!(awaiting.status(), StatusCode::CONFLICT);
        assert!(!gate.vote_gate_open(), "a refused request opened the gate");

        // No leader confirms a barrier: the production driver reports the group
        // stalled.
        let barrier = tokio::spawn(ursula_raft::run_rejoin_vote_barrier(
            registry.get(RaftGroupId(0)).expect("registered owner"),
            gate.clone(),
            registry.election_policy(),
            BTreeMap::new(),
            |_leader, _address| async {
                Err::<(ursula_raft::UrsulaVote, u64), _>("no leader".to_owned())
            },
            std::time::Duration::from_millis(10),
            std::time::Duration::from_millis(10),
            std::time::Duration::from_millis(50),
        ));
        let deadline = Instant::now() + std::time::Duration::from_secs(10);
        while gate.status() != ursula_raft::RecoveryGateStatus::Stalled {
            assert!(Instant::now() < deadline, "the gate never stalled");
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        if !retained_vote {
            let rejected = http_post(&admin, path, json_body, accept(seen)).await;
            assert_eq!(rejected.status(), StatusCode::CONFLICT);
            assert!(!gate.vote_gate_open());
            assert_eq!(store.read_vote().await.expect("unchanged lost vote"), None);
            assert!(!registry.recovery_barriers_ready());
            barrier.abort();
            assert!(
                barrier
                    .await
                    .expect_err("stopped recovery waiter")
                    .is_cancelled()
            );
            engine.shutdown().await.expect("stop unknown-history group");
            continue;
        }
        let other_log = ursula_proto::admin::ReplicaLogView {
            last_log_index: Some(
                seen.last_log_index
                    .map_or(0, |index| index.saturating_add(1)),
            ),
            ..seen
        };
        let changed = http_post(&admin, path, json_body, accept(other_log)).await;
        assert_eq!(changed.status(), StatusCode::CONFLICT);
        assert!(!gate.vote_gate_open(), "a refused request opened the gate");

        // A refusal after the gate changed, here a cutting plan on a replica
        // that knows no committed entry, puts the gate, the elections and
        // the leader's fence back as they were.
        let before = gate.status();
        let cutting = ursula_proto::admin::RecoveryPlanRequest {
            point_entry: None,
            point: 0,
            cut: true,
            ..plan(seen)
        };
        let refused = http_post(
            &admin,
            path,
            json_body,
            Body::from(serde_json::to_vec(&cutting).expect("request JSON")),
        )
        .await;
        assert_eq!(refused.status(), StatusCode::CONFLICT);
        // m3 (#581 review): the refusal is typed.
        let refused: ursula_proto::admin::RecoveryPlanRefused =
            serde_json::from_slice(&body_bytes(refused).await).expect("typed refusal");
        assert_eq!(
            refused.refusal,
            ursula_proto::admin::RecoveryPlanRefusal::NothingCommitted
        );
        assert_eq!((refused.raft_group_id, refused.node_id), (0, 1));
        assert_eq!(gate.status(), before);
        assert_eq!(engine.history().plan(), None);

        // m1 (#581 review): a request whose client goes away mid-plan does
        // not cancel it. The job runs to its end in a task of its own: here
        // it takes the plan, durably and in memory alike.
        /// A plan without a cut restarts nothing.
        struct NoRestart;
        impl ursula_raft::ReplicaRestart for NoRestart {
            fn restart_replica(
                &mut self,
                raft_group_id: RaftGroupId,
            ) -> futures_util::future::BoxFuture<
                '_,
                Result<Option<Arc<ursula_raft::GroupRejoin>>, ursula_runtime::RuntimeError>,
            > {
                panic!("a plan without a cut restarted group {raft_group_id:?}")
            }
        }
        let pause = gate.pause_plans_after_the_fence();
        {
            let mut no_restart = NoRestart;
            let taking = plan(seen);
            let mut call = std::pin::pin!(registry.accept_recovery_plan(
                RaftGroupId(0),
                &taking,
                &mut no_restart
            ));
            // Poll the caller until the job set the leader's fence and
            // waits, then drop the caller.
            while engine.history().plan().is_none() {
                assert!(
                    futures_util::poll!(call.as_mut()).is_pending(),
                    "the plan answered before its caller went away"
                );
                tokio::task::yield_now().await;
            }
        }
        pause.add_permits(1 << 20);
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            let metrics: ursula_proto::admin::NodeMetrics = serde_json::from_slice(
                &body_bytes(http_get(&admin, "/__ursula/metrics").await).await,
            )
            .expect("metrics JSON");
            // The record on disk, and the leader's fence in memory.
            if let Some(recovery) = metrics.groups[0].recovery
                && engine
                    .history()
                    .plan()
                    .is_some_and(|plan| plan.leader_term == recovery.leader_term)
            {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "a dropped caller cancelled the plan: {:?}",
                gate.status()
            );
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }

        // Item 6 of the #612 review: a `follow` whose plan term is below
        // the vote term the tool read on the replica did not come from the
        // plan's leader, and is refused, typed.
        let low_term = ursula_proto::admin::RecoveryPlanRequest {
            point_entry: None,
            leader: 2,
            step: ursula_proto::admin::PlanStep::Follow {
                leader_term: seen.current_term.saturating_add(4),
            },
            expected: ursula_proto::admin::ReplicaLogView {
                current_term: seen.current_term.saturating_add(5),
                ..seen
            },
            ..plan(seen)
        };
        let refused = http_post(
            &admin,
            path,
            json_body,
            Body::from(serde_json::to_vec(&low_term).expect("request JSON")),
        )
        .await;
        assert_eq!(refused.status(), StatusCode::CONFLICT);
        let refused: ursula_proto::admin::RecoveryPlanRefused =
            serde_json::from_slice(&body_bytes(refused).await).expect("typed refusal");
        assert_eq!(
            refused.refusal,
            ursula_proto::admin::RecoveryPlanRefusal::PlanTermBelowVote
        );

        // m4 (#581 review): one accept job at a time. A second accept while
        // the first one runs is refused, typed, and changes nothing; the
        // first one then takes its plan.
        {
            let pause = gate.pause_plans_after_the_fence();
            let taking = plan(seen);
            let mut first_restart = NoRestart;
            let mut first = std::pin::pin!(registry.accept_recovery_plan(
                RaftGroupId(0),
                &taking,
                &mut first_restart
            ));
            while !gate.plan_job_running() {
                assert!(futures_util::poll!(first.as_mut()).is_pending());
                tokio::task::yield_now().await;
            }
            let mut second_restart = NoRestart;
            let second = registry
                .accept_recovery_plan(RaftGroupId(0), &taking, &mut second_restart)
                .await
                .expect_err("a second accept while one runs");
            assert!(
                matches!(second, ursula_raft::RecoveryGateError::PlanInFlight { .. }),
                "{second:?}"
            );
            assert_eq!(
                second.refusal(),
                Some(ursula_proto::admin::RecoveryPlanRefusal::PlanInFlight)
            );
            pause.add_permits(1 << 20);
            let report = first.await.expect("the first accept takes its plan");
            assert_eq!(
                report.outcome,
                ursula_proto::admin::RecoveryPlanOutcome::Leads
            );
            assert!(!gate.plan_job_running());
        }

        // The plan elects this replica: its gate campaigns but votes for no
        // one else, and it answers its plan term: the highest vote term the
        // tool read, or its own when higher (M1, #581 review). Nothing votes
        // for it (M2), so a repeat of the plan is taken at the same term.
        let mut terms = Vec::new();
        for _ in 0..2 {
            let response = http_post(&admin, path, json_body, accept(seen)).await;
            let status = response.status();
            let response_body = body_bytes(response).await;
            assert_eq!(
                status,
                StatusCode::OK,
                "{}",
                String::from_utf8_lossy(&response_body)
            );
            let report: ursula_proto::admin::RecoveryPlanReport =
                serde_json::from_slice(&response_body).expect("typed report");
            assert_eq!(report.raft_group_id, 0);
            assert_eq!(report.node_id, 1);
            assert_eq!(
                report.outcome,
                ursula_proto::admin::RecoveryPlanOutcome::Leads
            );
            assert_eq!(report.leader, 1);
            terms.push(report.leader_term);
        }
        assert_eq!(terms, [seen.current_term; 2]);
        // It campaigns; with no election here (ticks are off) it soon
        // reports the plan stalled, as a wedged plan does.
        assert!(matches!(
            gate.status(),
            ursula_raft::RecoveryGateStatus::LeadsPlan | ursula_raft::RecoveryGateStatus::Stalled
        ));
        assert!(!gate.vote_gate_open());
        barrier.abort();
        let metrics: ursula_proto::admin::NodeMetrics =
            serde_json::from_slice(&body_bytes(http_get(&admin, "/__ursula/metrics").await).await)
                .expect("metrics JSON");
        assert_eq!(
            metrics.groups[0].recovery,
            Some(ursula_proto::admin::RaftGroupRecovery {
                leader: 1,
                point: seen.last_log_index.unwrap_or(0),
                leader_term: terms[1],
                cut_pending: false,
            })
        );
        assert!(matches!(
            metrics.groups[0].recovery_gate,
            ursula_proto::admin::RecoveryGateStatus::LeadsPlan
                | ursula_proto::admin::RecoveryGateStatus::Stalled
        ));

        let missing = http_post(
            &admin,
            "/__ursula/raft/7/recovery/accept-unsynced-loss",
            json_body,
            accept(seen),
        )
        .await;
        assert_eq!(missing.status(), StatusCode::NOT_FOUND);
        engine.shutdown().await.expect("stop the group");
    }
}
#[tokio::test]
async fn raft_readiness_uses_the_configured_inventory_even_when_every_group_is_missing() {
    let runtime = spawn_runtime(
        &test_config(1, 2),
        Persistence::InMemory,
        Topology::SingleNode {
            raft_group_count: 2,
        },
    )
    .expect("runtime")
    .runtime;
    let state = HttpState::with_static_raft_cluster_topology(
        runtime,
        RaftGroupHandleRegistry::default(),
        1,
        [
            (1, "http://localhost:4437".to_owned()),
            (2, "http://localhost:4438".to_owned()),
            (3, "http://localhost:4439".to_owned()),
        ],
        BTreeMap::new(),
    );
    let app = client_router_with_admission(state, IngressAdmission::default());
    let ready = http_get(&app, READINESS_PATH).await;
    assert_eq!(ready.status(), StatusCode::SERVICE_UNAVAILABLE);
    let typed: ursula_proto::admin::ServingReadiness =
        serde_json::from_slice(&body_bytes(ready).await).unwrap();
    assert_eq!(
        typed.reason,
        Some(ursula_proto::admin::ServingReadinessReason::RaftReplicaUnready)
    );
    let body = serde_json::to_value(typed).unwrap();
    assert_eq!(
        body["raft_maintenance"]["expected_groups"],
        json!({"0": [1, 2, 3], "1": [1, 2, 3]})
    );
    assert_eq!(
        body["raft_maintenance"]["group_issues"],
        json!({"0": ["missing_group"], "1": ["missing_group"]})
    );
    let response = http_get(&app, "/__ursula/metrics").await;
    let body: serde_json::Value = serde_json::from_slice(&body_bytes(response).await).unwrap();
    assert_eq!(body["configured_raft_group_count"], 2);
}

#[tokio::test]
async fn wal_disk_pressure_rejects_writes_and_marks_readiness_unavailable() {
    let monitor = WalDiskMonitor::new(100, 200);
    assert_eq!(
        monitor.observe_available(99),
        crate::wal_disk::WalDiskTransition::EnterPressure
    );
    let state = HttpState::new(
        spawn_runtime(
            &test_config(1, 1),
            Persistence::InMemory,
            Topology::SingleNode {
                raft_group_count: 1,
            },
        )
        .expect("runtime")
        .runtime,
    )
    .with_wal_disk_monitor(monitor.clone());
    let app = client_router_with_admission(
        state,
        IngressAdmission::default().with_wal_disk_monitor(monitor.clone()),
    );

    let ready = http_get(&app, READINESS_PATH).await;
    assert_eq!(ready.status(), StatusCode::SERVICE_UNAVAILABLE);
    let metrics = http_get(&app, "/__ursula/metrics").await;
    assert_eq!(metrics.status(), StatusCode::OK);
    let metrics = body_bytes(metrics).await;
    let metrics: serde_json::Value =
        serde_json::from_slice(&metrics).expect("decode pressure metrics");
    assert_eq!(metrics.get("wal_disk_pressure"), Some(&json!(true)));
    assert_eq!(metrics.get("wal_available_bytes"), Some(&json!(99)));

    let write = http_put(
        &app,
        "/benchcmp/disk-pressure",
        &[(CONTENT_LENGTH.as_str(), "1")],
        Body::from("x"),
    )
    .await;
    assert_eq!(write.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_no_not_applied_headers(write.headers(), "wal disk pressure");
    assert_eq!(
        crate::render::error_code_of(write).await,
        ursula_headers::ErrorCode::WalDiskPressure
    );
    // The marked refusal created nothing. Reads pass admission under pressure.
    assert_eq!(
        http_head(&app, "/benchcmp/disk-pressure").await.status(),
        StatusCode::NOT_FOUND
    );

    assert_eq!(
        monitor.observe_available(200),
        crate::wal_disk::WalDiskTransition::LeavePressure
    );
    assert_eq!(
        http_get(&app, READINESS_PATH).await.status(),
        StatusCode::OK
    );
    // Admitted, the same write creates the stream.
    let write = http_put(
        &app,
        "/benchcmp/disk-pressure",
        &[(CONTENT_LENGTH.as_str(), "1")],
        Body::from("x"),
    )
    .await;
    assert_eq!(write.status(), StatusCode::CREATED);
    assert_no_not_applied_headers(write.headers(), "write");
}

/// Format epoch 2 (E8): once a Raft protocol mismatch is recorded, readiness
/// answers 503 `format_epoch_mismatch` and stays there.
#[tokio::test]
async fn a_recorded_format_epoch_mismatch_makes_readiness_unavailable() {
    let mismatch = ursula_raft::FormatEpochMismatch::default();
    let state = HttpState::new(
        spawn_runtime(
            &test_config(1, 1),
            Persistence::InMemory,
            Topology::SingleNode {
                raft_group_count: 1,
            },
        )
        .expect("runtime")
        .runtime,
    )
    .with_format_epoch_mismatch(mismatch.clone());
    let app = client_router_with_admission(state, IngressAdmission::default());
    assert_eq!(
        http_get(&app, READINESS_PATH).await.status(),
        StatusCode::OK
    );

    mismatch.record();
    let ready = http_get(&app, READINESS_PATH).await;
    assert_eq!(ready.status(), StatusCode::SERVICE_UNAVAILABLE);
    let body: serde_json::Value =
        serde_json::from_slice(&body_bytes(ready).await).expect("readiness json");
    assert_eq!(body["reason"], json!("format_epoch_mismatch"));
    assert_eq!(body["ready"], json!(false));
}

/// Over its hard Raft-log limit a node answers body-carrying writes with 503 +
/// Retry-After (the EKS OOM: the log outgrew memory with no pushback), while
/// bodiless writes such as retention advances still pass.
#[tokio::test]
async fn raft_log_pressure_rejects_body_writes_with_retry_after() {
    let coordinator = ursula_raft::SnapshotBuildCoordinator::new(1);
    let state = HttpState::new(
        spawn_runtime(
            &test_config(1, 1),
            Persistence::InMemory,
            Topology::SingleNode {
                raft_group_count: 1,
            },
        )
        .expect("runtime")
        .runtime,
    );
    let app = client_router_with_admission(
        state,
        IngressAdmission::default().with_raft_log_pressure(Some(coordinator.clone())),
    );
    assert_eq!(coordinator.observe_log_bytes(300, 200, 100), Some(true));

    let write = http_put(
        &app,
        "/benchcmp/log-pressure",
        &[(CONTENT_LENGTH.as_str(), "1")],
        Body::from("x"),
    )
    .await;
    assert_eq!(write.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert!(
        write
            .headers()
            .contains_key(axum::http::header::RETRY_AFTER)
    );
    assert_no_not_applied_headers(write.headers(), "raft log pressure");
    assert_eq!(
        crate::render::error_code_of(write).await,
        ursula_headers::ErrorCode::RaftLogPressure
    );
    // The marked refusal created nothing.
    assert_eq!(
        http_head(&app, "/benchcmp/log-pressure").await.status(),
        StatusCode::NOT_FOUND
    );
    let bodiless = http_put(&app, "/benchcmp", &[], Body::empty()).await;
    assert_ne!(bodiless.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_no_not_applied_headers(bodiless.headers(), "bodiless");

    assert_eq!(coordinator.observe_log_bytes(50, 200, 100), Some(false));
    // Admitted, the same write creates the stream.
    let write = http_put(
        &app,
        "/benchcmp/log-pressure",
        &[(CONTENT_LENGTH.as_str(), "1")],
        Body::from("x"),
    )
    .await;
    assert_eq!(write.status(), StatusCode::CREATED);
    assert_no_not_applied_headers(write.headers(), "write");
}

/// Shared fixture for the governance unit-test modules below: one
/// [`RaftGroupMetricsSnapshot`] builder covering every per-module `snap`
/// variant (term is always 1; `last_applied` mirrors `committed`).
fn raft_metrics_snapshot(
    group_id: u32,
    node_id: u64,
    leader: Option<u64>,
    last_log: Option<u64>,
    committed: Option<u64>,
    snapshot: Option<u64>,
    voters: Vec<u64>,
) -> RaftGroupMetricsSnapshot {
    let progress = |index: u64| RaftLogProgressSnapshot {
        term: 1,
        index,
        node_id: 1,
    };
    RaftGroupMetricsSnapshot {
        apply_failure: None,
        raft_group_id: group_id,
        node_id,
        current_term: 1,
        current_leader: leader,
        last_log: last_log.map(progress),
        committed: committed.map(progress),
        last_applied: committed.map(progress),
        snapshot: snapshot.map(progress),
        purged: None,
        voter_configurations: vec![voters.iter().copied().collect()],
        committed_voter_configurations: vec![voters.iter().copied().collect()],
        voter_ids: voters,
        learner_ids: vec![],
        maintenance: ursula_raft::RaftGroupMaintenanceState::default(),
        log: Default::default(),
        history: None,
        recovery: None,
        recovery_gate: ursula_proto::admin::RecoveryGateStatus::Open,
        leads_live: false,
    }
}

mod cold_health {
    use crate::bootstrap::ColdHealthDecision;
    use crate::bootstrap::ColdHealthSample;
    use crate::bootstrap::ColdHealthTracker;

    fn sample(errors: u64, hot_max: u64) -> ColdHealthSample {
        ColdHealthSample {
            cold_flush_write_errors: errors,
            cold_hot_group_bytes_max: hot_max,
        }
    }

    fn fresh_tracker() -> ColdHealthTracker {
        // unhealthy_ticks=2, heal_ticks=3, hot_high=7MB, hot_low=4MB,
        // errors_per_tick_high=1 → match the chaos defaults but smaller
        // tick counts so tests stay short.
        ColdHealthTracker::new(2, 3, 7 * 1024 * 1024, 4 * 1024 * 1024, 1)
    }

    #[test]
    fn healthy_steady_state_does_nothing() {
        let mut t = fresh_tracker();
        for _ in 0..5 {
            assert_eq!(t.evaluate(sample(0, 0)), ColdHealthDecision::NoChange);
        }
        assert!(!t.yielded());
    }

    #[test]
    fn first_tick_with_accumulated_errors_does_not_immediately_shed() {
        // Process started with 1000 errors already on the counter from a
        // prior run — that's not "+1000/tick", just the baseline.
        let mut t = fresh_tracker();
        assert_eq!(t.evaluate(sample(1000, 0)), ColdHealthDecision::NoChange);
        // Subsequent flat reads must stay quiet too.
        assert_eq!(t.evaluate(sample(1000, 0)), ColdHealthDecision::NoChange);
        assert!(!t.yielded());
    }

    #[test]
    fn hot_above_high_for_unhealthy_ticks_sheds() {
        let mut t = fresh_tracker();
        // tick 1: unhealthy (hot 8MB ≥ 7MB high)
        assert_eq!(
            t.evaluate(sample(0, 8 * 1024 * 1024)),
            ColdHealthDecision::NoChange
        );
        // tick 2: still unhealthy → shed (unhealthy_ticks=2)
        match t.evaluate(sample(0, 8 * 1024 * 1024)) {
            ColdHealthDecision::Shed { reason } => assert!(reason.contains("cold_hot_max")),
            other => panic!("expected Shed, got {other:?}"),
        }
        assert!(t.yielded());
    }

    #[test]
    fn growing_errors_for_unhealthy_ticks_sheds() {
        let mut t = fresh_tracker();
        // baseline
        t.evaluate(sample(100, 0));
        // tick: +5 errors > 1/tick high → unhealthy
        assert_eq!(t.evaluate(sample(105, 0)), ColdHealthDecision::NoChange);
        // tick: +3 errors > 1/tick → still unhealthy → shed
        match t.evaluate(sample(108, 0)) {
            ColdHealthDecision::Shed { reason } => {
                assert!(reason.contains("cold_flush_write_errors"))
            }
            other => panic!("expected Shed, got {other:?}"),
        }
        assert!(t.yielded());
    }

    #[test]
    fn shed_does_not_fire_twice_until_a_heal() {
        let mut t = fresh_tracker();
        t.evaluate(sample(0, 8 * 1024 * 1024));
        let _ = t.evaluate(sample(0, 8 * 1024 * 1024));
        assert!(t.yielded());
        // Stay unhealthy more ticks — must NOT keep emitting Shed, just NoChange.
        for _ in 0..5 {
            assert_eq!(
                t.evaluate(sample(0, 8 * 1024 * 1024)),
                ColdHealthDecision::NoChange,
            );
        }
        assert!(t.yielded());
    }

    #[test]
    fn full_recovery_after_heal_ticks_re_enables() {
        let mut t = fresh_tracker();
        // Get yielded first.
        t.evaluate(sample(0, 8 * 1024 * 1024));
        let _ = t.evaluate(sample(0, 8 * 1024 * 1024));
        assert!(t.yielded());

        // Now healthy: errors flat, hot ≤ LOW. heal_ticks=3.
        assert_eq!(
            t.evaluate(sample(0, 1024 * 1024)),
            ColdHealthDecision::NoChange
        );
        assert_eq!(
            t.evaluate(sample(0, 1024 * 1024)),
            ColdHealthDecision::NoChange
        );
        assert_eq!(t.evaluate(sample(0, 1024 * 1024)), ColdHealthDecision::Heal);
        assert!(!t.yielded());
    }

    #[test]
    fn middle_band_neither_sheds_nor_heals() {
        let mut t = fresh_tracker();
        // Yield first.
        t.evaluate(sample(0, 8 * 1024 * 1024));
        let _ = t.evaluate(sample(0, 8 * 1024 * 1024));
        assert!(t.yielded());

        // Hot 5 MB is between LOW (4) and HIGH (7) — neutral. Must NOT heal
        // even after many ticks; the system needs a real catch-up window
        // (hot ≤ LOW) before we re-elect.
        for _ in 0..10 {
            assert_eq!(
                t.evaluate(sample(0, 5 * 1024 * 1024)),
                ColdHealthDecision::NoChange,
            );
        }
        assert!(t.yielded());
    }

    #[test]
    fn flapping_health_resets_streaks_and_does_not_shed() {
        let mut t = fresh_tracker();
        // alternating: bad, good, bad, good — should never accumulate
        // unhealthy_ticks=2 in a row.
        for _ in 0..5 {
            t.evaluate(sample(0, 8 * 1024 * 1024)); // bad
            t.evaluate(sample(0, 1024 * 1024)); // healthy
        }
        assert!(!t.yielded());
    }
}

mod snapshot_driver {
    use std::collections::BTreeMap;
    use std::collections::BTreeSet;
    use std::sync::Arc;
    use std::time::Duration;

    use axum::Router;
    use axum::body::Body;
    use axum::body::Bytes;
    use axum::http::StatusCode;
    use axum::response::Response;
    use openraft::rt::WatchReceiver;
    use tokio::sync::watch;
    use tokio::task::JoinHandle;
    use tokio::time::Instant;
    use ursula_proto::admin::SnapshotResponse;
    use ursula_raft::RaftEngineConfig;
    use ursula_raft::RaftGroupMetricsSnapshot;
    use ursula_raft::snapshot_cadence::DriveLimits;
    use ursula_raft::snapshot_cadence::GroupLogProgress;
    use ursula_raft::snapshot_cadence::LogPressure;
    use ursula_raft::snapshot_cadence::RemovalProgress;
    use ursula_raft::snapshot_cadence::SnapshotCadence;
    use ursula_runtime::AppendRequest;
    use ursula_runtime::CreateStreamRequest;
    use ursula_runtime::SnapshotKey;
    use ursula_runtime::SnapshotLocation;
    use ursula_runtime::SnapshotStore;
    use ursula_runtime::SnapshotStoreError;
    use ursula_runtime::SnapshotStoreFuture;
    use ursula_shard::BucketStreamId;
    use ursula_shard::RaftGroupId;

    use super::StaticGrpcTestNode;
    use super::StaticGrpcTestNodeStorage;
    use super::body_bytes;
    use super::send;
    use super::spawn_static_grpc_test_node;
    use crate::ADMIN_SNAPSHOT_PUBLISH_WAIT;
    use crate::HttpState;
    use crate::admin_router;
    use crate::bootstrap::LogWriteGate;
    use crate::bootstrap::PeerParticipation;
    use crate::bootstrap::ScrubPurge;
    use crate::bootstrap::StoreGateInputs;
    use crate::bootstrap::Topology;
    use crate::bootstrap::Trigger;
    use crate::bootstrap::deleted_data_sample;
    use crate::bootstrap::group_log_progress;
    use crate::bootstrap::groups_to_drive;
    use crate::bootstrap::plan_lagging_purges;
    use crate::bootstrap::plan_purges;
    use crate::bootstrap::plan_scrub_purges;
    use crate::bootstrap::plan_snapshot_drive;
    use crate::bootstrap::resolve_snapshot_drive_interval_ms;
    use crate::bootstrap::single_node_cadence;
    use crate::bootstrap::spawn_snapshot_driver;
    use crate::bootstrap::trigger_snapshot_build;

    const MIB: u64 = 1 << 20;

    /// One group at a time, as before #519.
    fn limits(max_groups: usize) -> DriveLimits {
        DriveLimits {
            max_groups,
            max_builds: 1,
        }
    }

    fn snap(
        raft_group_id: u32,
        last_applied: Option<u64>,
        snapshot_index: Option<u64>,
        log: GroupLogProgress,
    ) -> RaftGroupMetricsSnapshot {
        let mut snapshot = super::raft_metrics_snapshot(
            raft_group_id,
            1,
            Some(1),
            last_applied,
            last_applied,
            snapshot_index,
            vec![1, 2, 3],
        );
        snapshot.log = log;
        snapshot
    }

    fn log(log_bytes: u64, last_snapshot_bytes: u64) -> GroupLogProgress {
        GroupLogProgress {
            log_bytes,
            log_entries: (log_bytes / 256).saturating_add(1),
            last_snapshot_bytes,
            has_snapshot: last_snapshot_bytes > 0,
            ..GroupLogProgress::default()
        }
    }

    #[test]
    fn snapshot_driver_excludes_apply_stopped_groups_even_under_pressure() {
        let mut stopped = snap(0, Some(9), None, log(16 * MIB, 0));
        stopped.apply_failure = Some(ursula_proto::admin::ApplyFailure {
            term: 1,
            index: 10,
            kind: ursula_proto::admin::ApplyFailureKind::InvariantViolation,
            message: "diagnostic".to_owned(),
        });
        let healthy = snap(1, Some(9), None, log(MIB, 0));
        let groups = [stopped, healthy];
        let (_, selected) = plan_snapshot_drive(
            &groups,
            &SnapshotCadence::new(MIB, 2, 100),
            limits(2),
            &BTreeSet::from([RaftGroupId(0)]),
            &BTreeMap::new(),
            &BTreeSet::new(),
        );
        assert_eq!(
            selected
                .iter()
                .map(|group| group.raft_group_id)
                .collect::<Vec<_>>(),
            vec![1]
        );
    }

    #[test]
    fn snapshot_driver_default_interval_follows_external_store() {
        // F12e: the inline backend runs the byte-based driver too.
        assert_eq!(resolve_snapshot_drive_interval_ms(None, false), 1_000);
        assert_eq!(resolve_snapshot_drive_interval_ms(None, true), 5_000);
        assert_eq!(resolve_snapshot_drive_interval_ms(Some(0), false), 0);
        assert_eq!(resolve_snapshot_drive_interval_ms(Some(0), true), 0);
        assert_eq!(
            resolve_snapshot_drive_interval_ms(Some(15_000), false),
            15_000
        );
        assert_eq!(
            resolve_snapshot_drive_interval_ms(Some(15_000), true),
            15_000
        );
    }

    /// #669: a single node keeps its snapshots inline whatever
    /// `storage.snapshot.backend` says, so its driver ticks on the inline
    /// default, and its entry backstop is no farther than OpenRaft's policy
    /// took it before it ran the driver.
    #[test]
    fn a_single_node_drives_inline_snapshots_with_a_near_entry_backstop() {
        let single = Topology::SingleNode {
            raft_group_count: 4,
        };
        assert!(!single.uses_snapshot_store(true));
        assert!(!single.uses_snapshot_store(false));
        let cluster = Topology::static_cluster(
            1,
            vec![(1, "http://127.0.0.1:4437".to_owned())],
            4,
            true,
            Default::default(),
        )
        .expect("topology");
        assert!(cluster.uses_snapshot_store(true));
        assert!(!cluster.uses_snapshot_store(false));
        assert_eq!(
            resolve_snapshot_drive_interval_ms(None, single.uses_snapshot_store(true)),
            1_000
        );

        let mut raft = ursula_config::RaftConfig::default();
        let cadence = single_node_cadence(&raft, 64);
        assert_eq!(cadence.backstop_entries, raft.snapshot_logs_since_last);
        assert_eq!(cadence.backstop_entries, 5_000);
        // The rest is a cluster node's policy.
        let clustered = SnapshotCadence::new(
            raft.snapshot_log_budget.as_bytes(),
            64,
            raft.snapshot_backstop_logs,
        )
        .with_deleted_data_max_age(raft.effective_deleted_data_max_age());
        assert_eq!(cadence.floor_bytes, clustered.floor_bytes);
        assert_eq!(cadence.floor_bytes, 8 * MIB);
        assert_eq!(cadence.removal_floor, clustered.removal_floor);
        assert_eq!(clustered.backstop_entries, 100_000);
        // A nearer configured backstop stays.
        raft.snapshot_backstop_logs = 1_000;
        assert_eq!(single_node_cadence(&raft, 64).backstop_entries, 1_000);
    }

    #[test]
    fn snapshot_driver_never_snapshots_a_group_without_applied_state() {
        let empty = snap(0, None, None, log(MIB, 0));
        assert_eq!(group_log_progress(&empty), GroupLogProgress::default());
        // OpenRaft's snapshot counts even if the gauge has not seen one.
        let installed = snap(1, Some(9), Some(9), GroupLogProgress {
            log_entries: 1,
            log_bytes: 10,
            ..GroupLogProgress::default()
        });
        assert!(group_log_progress(&installed).has_snapshot);
    }

    fn removal(age_ms: u64) -> GroupLogProgress {
        GroupLogProgress {
            removal: RemovalProgress::Owed { bytes: 10, age_ms },
            ..log(512, MIB)
        }
    }

    /// #669: the driver purges a group's log through the snapshot that
    /// covers its removals once OpenRaft shows that snapshot, and leaves a
    /// group whose Raft stopped alone.
    #[test]
    fn snapshot_driver_purges_through_the_covering_snapshot() {
        let covering = |through| GroupLogProgress {
            scrub_purge_through: through,
            ..log(MIB, MIB)
        };
        let running = |mut snapshot: RaftGroupMetricsSnapshot| {
            snapshot.maintenance.running = true;
            snapshot
        };
        let mut purged = running(snap(3, Some(40), Some(30), covering(30)));
        purged.purged = Some(super::RaftLogProgressSnapshot {
            term: 1,
            index: 30,
            node_id: 1,
        });
        let mut stopped = running(snap(4, Some(40), Some(30), covering(30)));
        stopped.apply_failure = Some(ursula_proto::admin::ApplyFailure {
            term: 1,
            index: 41,
            kind: ursula_proto::admin::ApplyFailureKind::InvariantViolation,
            message: "diagnostic".to_owned(),
        });
        let snapshots = vec![
            running(snap(0, Some(40), Some(30), covering(30))),
            // OpenRaft does not show the covering snapshot yet.
            running(snap(1, Some(40), Some(20), covering(30))),
            // No snapshot covered a removal.
            running(snap(2, Some(40), Some(30), covering(0))),
            purged,
            stopped,
            // Its Raft does not run.
            snap(5, Some(40), Some(30), covering(30)),
        ];
        assert_eq!(plan_scrub_purges(&snapshots), vec![ScrubPurge {
            raft_group_id: 0,
            through: 30,
            published: 30,
        }]);
    }

    /// #669: a group the Raft WAL reports lagging that applied nothing
    /// since its snapshot is purged through that snapshot, so the entries
    /// OpenRaft keeps below it never pin a journal segment. A lagging group
    /// that applied entries since builds instead, and a group that is not
    /// lagging, already purged, stopped or not running is left alone.
    #[test]
    fn snapshot_driver_purges_an_idle_lagging_group_through_its_snapshot() {
        let running = |mut snapshot: RaftGroupMetricsSnapshot| {
            snapshot.maintenance.running = true;
            snapshot
        };
        let idle = GroupLogProgress {
            has_snapshot: true,
            ..GroupLogProgress::default()
        };
        let mut purged = running(snap(3, Some(30), Some(30), idle));
        purged.purged = Some(super::RaftLogProgressSnapshot {
            term: 1,
            index: 30,
            node_id: 1,
        });
        let mut stopped = running(snap(4, Some(30), Some(30), idle));
        stopped.apply_failure = Some(ursula_proto::admin::ApplyFailure {
            term: 1,
            index: 31,
            kind: ursula_proto::admin::ApplyFailureKind::InvariantViolation,
            message: "diagnostic".to_owned(),
        });
        let snapshots = vec![
            running(snap(0, Some(30), Some(30), idle)),
            // It applied entries since its snapshot: the pass builds it.
            running(snap(1, Some(40), Some(30), log(MIB, MIB))),
            // Not lagging.
            running(snap(2, Some(30), Some(30), idle)),
            purged,
            stopped,
            // Its Raft does not run.
            snap(5, Some(30), Some(30), idle),
            // No snapshot yet.
            running(snap(6, Some(30), None, GroupLogProgress::default())),
        ];
        let lagging: BTreeSet<RaftGroupId> =
            [0, 1, 3, 4, 5, 6].map(RaftGroupId).into_iter().collect();
        assert_eq!(plan_lagging_purges(&snapshots, &lagging), vec![
            ScrubPurge {
                raft_group_id: 0,
                through: 30,
                published: 30,
            }
        ]);
    }

    /// #669: a tick purges each group at most once, through the higher of
    /// its scrub purge and its idle lagging purge.
    #[test]
    fn snapshot_driver_purges_each_group_once_through_the_higher_index() {
        let running = |mut snapshot: RaftGroupMetricsSnapshot| {
            snapshot.maintenance.running = true;
            snapshot
        };
        let idle = |through| GroupLogProgress {
            has_snapshot: true,
            scrub_purge_through: through,
            ..GroupLogProgress::default()
        };
        let snapshots = vec![
            // Both: the lagging purge goes further.
            running(snap(0, Some(30), Some(30), idle(20))),
            // A scrub purge only.
            running(snap(1, Some(40), Some(30), GroupLogProgress {
                scrub_purge_through: 30,
                ..log(MIB, MIB)
            })),
            // A lagging purge only.
            running(snap(2, Some(30), Some(30), idle(0))),
        ];
        let lagging: BTreeSet<RaftGroupId> = [0, 2].map(RaftGroupId).into_iter().collect();
        let purge = |raft_group_id, through| ScrubPurge {
            raft_group_id,
            through,
            published: 30,
        };
        assert_eq!(plan_purges(&snapshots, &lagging), vec![
            purge(0, 30),
            purge(1, 30),
            purge(2, 30)
        ]);
        assert_eq!(plan_purges(&snapshots, &BTreeSet::new()), vec![
            purge(0, 20),
            purge(1, 30)
        ]);
    }

    /// #669: a group with an overdue removal leads the pass, and while the
    /// snapshot store is in trouble the driver still snapshots it, and only
    /// it, unless the log is under pressure.
    #[test]
    fn snapshot_driver_drives_overdue_groups_through_store_trouble() {
        let cadence = SnapshotCadence::new(1 << 30, 128, 100_000)
            .with_deleted_data_max_age(Duration::from_secs(60));
        let snapshots = vec![
            snap(0, Some(9), Some(5), log(16 * MIB, MIB)),
            snap(1, Some(9), Some(5), removal(15_000)),
            snap(2, Some(9), Some(5), removal(14_999)),
        ];
        let (plan, selected) = plan_snapshot_drive(
            &snapshots,
            &cadence,
            limits(16),
            &BTreeSet::new(),
            &BTreeMap::new(),
            &BTreeSet::new(),
        );
        assert_eq!(plan.pressure, LogPressure::Calm);
        assert_eq!(plan.overdue, 1);
        assert_eq!(ids(&selected), vec![1, 0]);
        assert_eq!(groups_to_drive(&plan, true), 1);
        assert_eq!(groups_to_drive(&plan, false), 2);
        // A group that builds or is held is not planned, overdue or not.
        let (plan, _) = plan_snapshot_drive(
            &snapshots,
            &cadence,
            limits(16),
            &BTreeSet::new(),
            &BTreeMap::from([(1, 0)]),
            &BTreeSet::new(),
        );
        assert_eq!(groups_to_drive(&plan, true), 0);
        assert_eq!((plan.overdue, plan.groups), (0, vec![0]));
    }

    /// #669: the node gauges report the oldest removal no snapshot covers
    /// and the groups that hold one past the floor.
    #[test]
    fn snapshot_driver_reports_the_deleted_data_its_groups_hold() {
        let cadence = SnapshotCadence::new(1 << 30, 128, 100_000)
            .with_deleted_data_max_age(Duration::from_secs(60));
        let groups = [
            log(MIB, MIB),
            removal(20_000),
            removal(3_000),
            GroupLogProgress {
                removal: RemovalProgress::Replayed {
                    bytes: 1,
                    age_ms: 100,
                },
                ..log(512, MIB)
            },
        ];
        assert_eq!(
            deleted_data_sample(&cadence, &groups),
            ursula_runtime::RaftDeletedDataSample {
                oldest_age_ms: 20_000,
                overdue_groups: 2,
            }
        );
        assert_eq!(
            deleted_data_sample(&cadence, &[log(MIB, MIB)]),
            ursula_runtime::RaftDeletedDataSample::default()
        );
    }

    #[test]
    fn snapshot_driver_follows_log_bytes_not_entry_counts() {
        // 128 groups and a 1 GiB budget: a 4 MiB floor.
        let cadence = SnapshotCadence::new(1 << 30, 128, 100_000);
        let snapshots = vec![
            // First snapshot as soon as there is applied state.
            snap(0, Some(3), None, log(512, 0)),
            // Below max(4 MiB, 2 x 1 MiB).
            snap(1, Some(50_000), Some(10), log(4 * MIB - 1, MIB)),
            // Past max(4 MiB, 2 x 3 MiB) = 6 MiB.
            snap(2, Some(9_000), Some(10), log(6 * MIB, 3 * MIB)),
            // Many entries, few bytes: below the floor and the backstop.
            snap(3, Some(90_000), Some(10), GroupLogProgress {
                log_bytes: MIB,
                log_entries: 89_990,
                last_snapshot_bytes: MIB,
                has_snapshot: true,
                ..GroupLogProgress::default()
            }),
        ];
        let (plan, selected) = plan_snapshot_drive(
            &snapshots,
            &cadence,
            limits(16),
            &BTreeSet::new(),
            &BTreeMap::new(),
            &BTreeSet::new(),
        );
        assert!(!plan.pressure.is_pressure());
        assert_eq!(
            selected
                .iter()
                .map(|snapshot| snapshot.raft_group_id)
                .collect::<Vec<_>>(),
            vec![0, 2]
        );
        let (_, one) = plan_snapshot_drive(
            &snapshots,
            &cadence,
            limits(1),
            &BTreeSet::new(),
            &BTreeMap::new(),
            &BTreeSet::new(),
        );
        assert_eq!(one.len(), 1);

        // A group the Raft WAL reports lagging goes first, though its own
        // cadence does not call for a snapshot yet.
        let lagging = BTreeSet::from([RaftGroupId(1)]);
        let (_, selected) = plan_snapshot_drive(
            &snapshots,
            &cadence,
            limits(16),
            &lagging,
            &BTreeMap::new(),
            &BTreeSet::new(),
        );
        assert_eq!(
            selected
                .iter()
                .map(|snapshot| snapshot.raft_group_id)
                .collect::<Vec<_>>(),
            vec![1, 0, 2]
        );
    }

    #[test]
    fn snapshot_pressure_keeps_unpurged_log_within_the_node_budget() {
        // A 64 MiB budget over 4 groups: floor 8 MiB, pressure from 48 MiB.
        let cadence = SnapshotCadence::new(64 * MIB, 4, 100_000);
        let snapshots = vec![
            snap(0, Some(100), Some(1), log(14 * MIB, 8 * MIB)),
            snap(1, Some(100), Some(1), log(12 * MIB, 16 * MIB)),
            snap(2, Some(100), Some(1), log(14 * MIB, 10 * MIB)),
            snap(3, Some(100), Some(1), log(10 * MIB, 6 * MIB)),
        ];
        let (plan, selected) = plan_snapshot_drive(
            &snapshots,
            &cadence,
            limits(16),
            &BTreeSet::new(),
            &BTreeMap::new(),
            &BTreeSet::new(),
        );
        assert_eq!(plan.pressure, LogPressure::Watermark);
        // The most log first (#519): 14 and 14 MiB, then 22 MiB is at or
        // below half the budget.
        assert_eq!(
            selected
                .iter()
                .map(|snapshot| snapshot.raft_group_id)
                .collect::<Vec<_>>(),
            vec![0, 2]
        );
    }

    #[test]
    fn metrics_export_log_bytes_since_snapshot_and_last_snapshot_size() {
        // Bounded-state gauges: unpurged log bytes and snapshot raw
        // bytes per group, scraped from `/__ursula/metrics`.
        let mut snapshot = snap(3, Some(100), Some(40), GroupLogProgress {
            log_bytes: 5 * MIB,
            log_entries: 60,
            last_snapshot_bytes: 3 * MIB,
            has_snapshot: true,
            ..GroupLogProgress::default()
        });
        // The last log id renders as one term and index, apart from the
        // replica's current term.
        snapshot.last_log = Some(super::RaftLogProgressSnapshot {
            term: 4,
            index: 120,
            node_id: 1,
        });
        let rendered = crate::render::render_raft_group_metrics_array(&[snapshot]);
        let group = &rendered[0];
        assert_eq!(group["raft_group_id"], 3);
        assert_eq!(group["current_term"], 1);
        assert_eq!(group["last_log_index"], 120);
        assert_eq!(group["last_log_term"], 4);
        assert_eq!(group["log_bytes_since_snapshot"], 5 * MIB);
        assert_eq!(group["log_entries_since_snapshot"], 60);
        assert_eq!(group["last_snapshot_bytes"], 3 * MIB);
        assert_eq!(group["has_snapshot"], true);
    }

    fn ids(selected: &[&RaftGroupMetricsSnapshot]) -> Vec<u32> {
        selected
            .iter()
            .map(|snapshot| snapshot.raft_group_id)
            .collect()
    }

    /// #519: past the budget the driver plans the most reclaimable log
    /// first, with more builds at once, and never a group whose build is in
    /// flight, whose log counts as freed.
    #[test]
    fn snapshot_driver_escalates_past_the_budget_around_builds_in_flight() {
        // 70 MiB against a 64 MiB budget: floor 8 MiB, target 32 MiB.
        let cadence = SnapshotCadence::new(64 * MIB, 4, 100_000);
        let snapshots = vec![
            snap(0, Some(100), Some(1), log(30 * MIB, 30 * MIB)),
            snap(1, Some(100), Some(1), log(20 * MIB, MIB)),
            snap(2, Some(100), Some(1), log(16 * MIB, 2 * MIB)),
            snap(3, Some(100), Some(1), log(4 * MIB, 64 << 10)),
        ];
        let four = DriveLimits {
            max_groups: 16,
            max_builds: 4,
        };
        let (plan, selected) = plan_snapshot_drive(
            &snapshots,
            &cadence,
            four,
            &BTreeSet::new(),
            &BTreeMap::new(),
            &BTreeSet::new(),
        );
        // 6 MiB past the budget: the first of three steps to four builds.
        assert_eq!(plan.pressure, LogPressure::Escalated { builds: 2 });
        assert_eq!(ids(&selected), vec![0, 1, 2]);
        let (plan, selected) = plan_snapshot_drive(
            &snapshots,
            &cadence,
            four,
            &BTreeSet::from([RaftGroupId(0)]),
            &BTreeMap::from([(0, 30 * MIB)]),
            &BTreeSet::new(),
        );
        assert_eq!(plan.node_log_bytes, 70 * MIB);
        assert_eq!(ids(&selected), vec![1, 2]);
        // A group held out of the pass after its trigger went unclaimed is
        // not planned either, and none of its log counts as freed, so the
        // pass plans the next group too.
        let (plan, selected) = plan_snapshot_drive(
            &snapshots,
            &cadence,
            four,
            &BTreeSet::new(),
            &BTreeMap::new(),
            &BTreeSet::from([0]),
        );
        assert_eq!(plan.pressure, LogPressure::Escalated { builds: 2 });
        assert_eq!(ids(&selected), vec![1, 2, 3]);
    }

    const GROUPS: u32 = 8;

    fn group_count() -> usize {
        usize::try_from(GROUPS).unwrap()
    }

    /// The uploads a [`GatedStore`] saw.
    #[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
    struct Uploads {
        /// Under way now.
        running: usize,
        /// The most under way at once.
        most: usize,
        /// Every upload that started.
        started: usize,
    }

    /// A snapshot store whose uploads wait until the test lets them finish,
    /// in the order they arrive, and which counts them. Its pruning of
    /// retired snapshots, a build's last housekeeping step, waits while the
    /// test holds it.
    #[derive(Debug)]
    struct GatedStore {
        /// How many uploads, by arrival, may finish.
        allowed: watch::Sender<usize>,
        uploads: watch::Sender<Uploads>,
        /// Whether pruning waits.
        prune_held: watch::Sender<bool>,
        /// How many prunes started.
        prunes: watch::Sender<usize>,
    }

    impl GatedStore {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                allowed: watch::Sender::new(0),
                uploads: watch::Sender::new(Uploads::default()),
                prune_held: watch::Sender::new(false),
                prunes: watch::Sender::new(0),
            })
        }

        /// Holds pruning (`true`) until it is let go (`false`).
        fn hold_pruning(&self, held: bool) {
            self.prune_held.send_replace(held);
        }

        /// Waits until `count` prunes started.
        async fn wait_for_prunes(&self, count: usize, within: Duration) {
            let reached = tokio::time::timeout(
                within,
                self.prunes.subscribe().wait_for(|prunes| *prunes >= count),
            )
            .await
            .map(|reached| reached.map(drop));
            assert!(
                matches!(reached, Ok(Ok(()))),
                "{count} prunes never started within {within:?}"
            );
        }

        fn uploads(&self) -> Uploads {
            *self.uploads.borrow()
        }

        /// Waits until `count` uploads are under way at once.
        async fn wait_for_uploads(&self, count: usize, within: Duration) {
            let reached = tokio::time::timeout(
                within,
                self.uploads
                    .subscribe()
                    .wait_for(|uploads| uploads.running >= count),
            )
            .await
            .map(|reached| reached.map(drop));
            assert!(
                matches!(reached, Ok(Ok(()))),
                "{count} uploads never ran at once within {within:?}: {:?}",
                self.uploads()
            );
        }

        /// Lets the next `count` uploads to arrive finish.
        fn release(&self, count: usize) {
            self.allowed
                .send_modify(|allowed| *allowed = allowed.saturating_add(count));
        }

        /// Lets every upload finish.
        fn open(&self) {
            self.allowed.send_replace(usize::MAX);
        }
    }

    impl SnapshotStore for GatedStore {
        fn upload<'a>(
            &'a self,
            _key: SnapshotKey,
            bytes: Bytes,
        ) -> SnapshotStoreFuture<'a, SnapshotLocation> {
            Box::pin(async move {
                let mut ticket = 0;
                self.uploads.send_modify(|uploads| {
                    ticket = uploads.started;
                    uploads.started = uploads.started.saturating_add(1);
                    uploads.running = uploads.running.saturating_add(1);
                    uploads.most = uploads.most.max(uploads.running);
                });
                let mut allowed = self.allowed.subscribe();
                let released = allowed
                    .wait_for(|allowed| *allowed > ticket)
                    .await
                    .map(drop);
                self.uploads.send_modify(|uploads| {
                    uploads.running = uploads.running.saturating_sub(1);
                });
                released.map_err(|_closed| {
                    ursula_runtime::SnapshotStoreError::backend(
                        ursula_runtime::SnapshotStoreOperation::Put,
                        "test",
                        opendal::Error::new(opendal::ErrorKind::Unexpected, "the gate is gone"),
                    )
                })?;
                Ok(SnapshotLocation::Inline {
                    bytes: bytes.to_vec(),
                })
            })
        }

        fn download<'a>(
            &'a self,
            _location: &'a SnapshotLocation,
        ) -> SnapshotStoreFuture<'a, Vec<u8>> {
            Box::pin(async {
                Err(ursula_runtime::SnapshotStoreError::backend(
                    ursula_runtime::SnapshotStoreOperation::Get,
                    "test",
                    opendal::Error::new(opendal::ErrorKind::Unexpected, "inline only"),
                ))
            })
        }

        fn delete<'a>(&'a self, _location: &'a SnapshotLocation) -> SnapshotStoreFuture<'a, ()> {
            Box::pin(async { Ok(()) })
        }

        fn prune_retired<'a>(
            &'a self,
            _raft_group_id: u32,
            _current: &'a SnapshotLocation,
        ) -> SnapshotStoreFuture<'a, ursula_runtime::SnapshotPrune> {
            Box::pin(async move {
                self.prunes
                    .send_modify(|prunes| *prunes = prunes.saturating_add(1));
                self.prune_held
                    .subscribe()
                    .wait_for(|held| !*held)
                    .await
                    .map(|_released| ursula_runtime::SnapshotPrune::Unmanaged)
                    .map_err(|_closed| {
                        ursula_runtime::SnapshotStoreError::backend(
                            ursula_runtime::SnapshotStoreOperation::Get,
                            "test",
                            opendal::Error::new(opendal::ErrorKind::Unexpected, "the gate is gone"),
                        )
                    })
            })
        }

        ursula_runtime::inline_owned_snapshot_methods!();
    }

    /// The snapshot builds a node runs at once by default, as the server's
    /// default config hands them to its engine.
    fn default_permits() -> usize {
        RaftEngineConfig::from(&ursula_config::RaftConfig::default()).snapshot_build_max_concurrency
    }

    /// One node, the only voter of [`GROUPS`] groups, whose snapshots go to
    /// `store` and which builds at most `permits` of them at once. Group `i`
    /// holds one stream, returned at index `i`, with `appends[i]` appends of
    /// 8 KiB.
    async fn loaded_node(
        store: &Arc<GatedStore>,
        appends: &[usize],
        permits: usize,
    ) -> (StaticGrpcTestNode, Vec<BucketStreamId>) {
        // The driver snapshots, not OpenRaft's entry policy.
        node_with_permits(store, appends, permits, 100).await
    }

    /// [`loaded_node`] whose engine expects a snapshot driver every
    /// `drive_interval_ms` (0: none, so OpenRaft's entry policy runs).
    async fn node_with_permits(
        store: &Arc<GatedStore>,
        appends: &[usize],
        permits: usize,
        drive_interval_ms: u64,
    ) -> (StaticGrpcTestNode, Vec<BucketStreamId>) {
        assert_eq!(appends.len(), group_count());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind listener");
        let peers = vec![(
            1,
            format!("http://{}", listener.local_addr().expect("local addr")),
        )];
        let node = spawn_static_grpc_test_node(
            1,
            listener,
            peers.clone(),
            peers,
            true,
            group_count(),
            StaticGrpcTestNodeStorage {
                engine_config: Some(RaftEngineConfig {
                    snapshot_drive_interval_ms: drive_interval_ms,
                    snapshot_build_max_concurrency: permits,
                    ..RaftEngineConfig::default()
                }),
                snapshot_store: Some(store.clone()),
                ..StaticGrpcTestNodeStorage::default()
            },
        )
        .await;
        tokio::time::timeout(Duration::from_secs(30), node.runtime.warm_all_groups())
            .await
            .expect("warm the groups in time")
            .expect("warm the groups");
        for group in 0..GROUPS {
            node.registry
                .get(RaftGroupId(group))
                .expect("registered group")
                .wait(Some(Duration::from_secs(10)))
                .current_leader(1, "the only voter leads")
                .await
                .expect("the group elects its leader");
        }
        let mut streams = BTreeMap::new();
        for index in 0..10_000 {
            let stream_id = BucketStreamId::new("log-pressure", format!("stream-{index}"));
            streams
                .entry(node.runtime.locate(&stream_id).raft_group_id)
                .or_insert(stream_id);
            if streams.len() == group_count() {
                break;
            }
        }
        assert_eq!(streams.len(), group_count());
        let streams = streams.into_values().collect::<Vec<_>>();
        for (stream_id, count) in streams.iter().zip(appends) {
            node.runtime
                .create_stream(CreateStreamRequest::new(
                    stream_id.clone(),
                    "application/octet-stream",
                ))
                .await
                .expect("create the group's stream");
            append(&node, stream_id, *count).await;
        }
        (node, streams)
    }

    /// Appends `count` records of 8 KiB to `stream_id`.
    async fn append(node: &StaticGrpcTestNode, stream_id: &BucketStreamId, count: usize) {
        for _ in 0..count {
            node.runtime
                .append(AppendRequest::from_bytes(stream_id.clone(), vec![
                    7_u8;
                    8 << 10
                ]))
                .await
                .expect("append to the group's stream");
        }
    }

    /// Runs the production snapshot driver of `node` with `cadence`, a pass
    /// every 100 ms.
    fn drive(node: &StaticGrpcTestNode, store: &Arc<GatedStore>, cadence: SnapshotCadence) {
        drive_every(node, store, cadence, 100);
    }

    /// Runs the production snapshot driver of `node` with `cadence`, a pass
    /// every `interval_ms` unless the driver's pressure re-tick comes first.
    fn drive_every(
        node: &StaticGrpcTestNode,
        store: &Arc<GatedStore>,
        cadence: SnapshotCadence,
        interval_ms: usize,
    ) {
        let participation = PeerParticipation::new(1, &[], reqwest::Client::new());
        spawn_snapshot_driver(
            &node.runtime,
            &node.registry,
            Some((store.clone(), StoreGateInputs {
                s3: None,
                cold_flush_interval: Duration::from_secs(1),
                participation: &participation,
            })),
            LogWriteGate::RefuseAtHardLimit,
            interval_ms,
            cadence,
            None,
            16,
        );
    }

    /// Waits until the node's builds brought its log down to `bytes`.
    async fn wait_for_log_at_most(node: &StaticGrpcTestNode, bytes: u64) {
        let deadline = Instant::now()
            .checked_add(Duration::from_secs(30))
            .expect("deadline");
        let coordinator = node.registry.snapshot_build_coordinator();
        while coordinator.node_log_bytes() > bytes {
            assert!(
                Instant::now() < deadline,
                "the log stays at {} bytes",
                coordinator.node_log_bytes()
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    /// The bytes every record of the stream the deleted-data test removes
    /// carries.
    const DELETED_MARKER: &[u8] = b"deleted-data-marker-669;";

    /// What of the removed stream `raft`'s current snapshot holds, if any:
    /// inline snapshots hold the whole group.
    async fn snapshot_holds_marker(
        raft: &ursula_raft::OwnerRaftHandle,
        stream_id: &BucketStreamId,
    ) -> bool {
        let snapshot = raft
            .get_snapshot()
            .await
            .expect("read the current snapshot")
            .expect("a current snapshot");
        let pointer = ursula_runtime::SnapshotPointer::decode(snapshot.snapshot.pointer_bytes())
            .expect("decode the snapshot pointer");
        let SnapshotLocation::Inline { bytes } = &pointer.location else {
            panic!("an inline snapshot: {:?}", pointer.location);
        };
        ursula_raft::decode_group_snapshot(bytes)
            .expect("decode the snapshot")
            .stream_snapshot
            .streams
            .iter()
            .any(|entry| {
                &entry.metadata.stream_id == stream_id
                    || entry
                        .payload
                        .windows(DELETED_MARKER.len())
                        .any(|window| window == DELETED_MARKER)
            })
    }

    /// The first file under `root` that holds [`DELETED_MARKER`].
    fn file_holding_marker(root: &std::path::Path) -> Option<std::path::PathBuf> {
        let mut dirs = vec![root.to_path_buf()];
        while let Some(dir) = dirs.pop() {
            for entry in std::fs::read_dir(&dir).expect("list a WAL directory") {
                let path = entry.expect("a WAL directory entry").path();
                if path.is_dir() {
                    dirs.push(path);
                } else if std::fs::read(&path).is_ok_and(|bytes| {
                    bytes
                        .windows(DELETED_MARKER.len())
                        .any(|window| window == DELETED_MARKER)
                }) {
                    return Some(path);
                }
            }
        }
        None
    }

    fn wall_ms() -> u64 {
        u64::try_from(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("after the epoch")
                .as_millis(),
        )
        .expect("wall clock fits u64")
    }

    /// A single node with Raft over a fresh WAL root, its snapshot driver at
    /// `drive_interval`, `raft.deleted_data_max_age` of 8 s and OpenRaft
    /// keeping 8 entries below a snapshot.
    fn single_node(
        drive_interval: Option<ursula_config::HumanDuration>,
    ) -> (
        crate::bootstrap::SpawnedRuntime,
        tempfile::TempDir,
        Duration,
    ) {
        let root = tempfile::tempdir().expect("WAL root");
        let mut config = super::test_config(1, 2);
        config.raft.install_snapshot_timeout = ursula_config::HumanDuration::sec(1);
        config.raft.deleted_data_max_age = Some(ursula_config::HumanDuration::sec(8));
        config.raft.max_in_snapshot_log_to_keep = 8;
        config.raft.wal.segment_size = ursula_config::HumanSize::kib(64);
        config.storage.snapshot.drive_interval = drive_interval;
        let spawned = crate::bootstrap::spawn_runtime(
            &config,
            crate::bootstrap::Persistence::Raft {
                log_dir: root.path().to_owned(),
            },
            Topology::SingleNode {
                raft_group_count: 2,
            },
        )
        .expect("spawn a single node");
        (spawned, root, config.raft.effective_deleted_data_max_age())
    }

    /// Creates `stream_id`, expiring at `expires_at_ms` when set, and appends
    /// three marked records to it at the wall clock.
    async fn write_marked(
        runtime: &ursula_runtime::ShardRuntime,
        stream_id: &BucketStreamId,
        expires_at_ms: Option<u64>,
    ) {
        let mut create = CreateStreamRequest::new(stream_id.clone(), "application/octet-stream");
        create.stream_expires_at_ms = expires_at_ms;
        create.now_ms = wall_ms();
        runtime
            .create_stream(create)
            .await
            .expect("create the stream");
        for _ in 0..3 {
            let mut append = AppendRequest::from_bytes(stream_id.clone(), DELETED_MARKER.to_vec());
            append.now_ms = wall_ms();
            runtime
                .append(append)
                .await
                .expect("append a marked record");
        }
    }

    /// Waits until `stream_id`'s group no longer holds it, without a read of
    /// it (which would remove an expired stream itself), and returns the
    /// group's applied index then.
    async fn wait_until_removed(
        runtime: &ursula_runtime::ShardRuntime,
        raft: &ursula_raft::OwnerRaftHandle,
        streams_before: u64,
        group: RaftGroupId,
        within: Duration,
    ) -> u64 {
        let deadline = Instant::now().checked_add(within).expect("deadline");
        loop {
            let streams = runtime
                .state_gauges_all_groups()
                .await
                .into_iter()
                .find(|(id, _)| *id == group)
                .and_then(|(_, gauges)| gauges.ok())
                .map(|gauges| gauges.streams);
            if streams.is_some_and(|streams| streams < streams_before) {
                return raft
                    .metrics()
                    .borrow_watched()
                    .last_applied
                    .expect("applied the removal")
                    .index;
            }
            assert!(Instant::now() < deadline, "the group keeps the stream");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// #669: a single node runs the snapshot driver. A deleted stream and an
    /// expired stream that nothing reads, in groups that take no other write,
    /// leave the node's snapshots, its logs and every file under its WAL root
    /// within `raft.deleted_data_max_age`, its gauges go back to 0, and reads
    /// of them answer 404.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_single_node_scrubs_deleted_and_expired_streams_within_the_bound() {
        let (spawned, root, max_age) = single_node(None);
        let runtime = spawned.runtime.clone();
        let registry = spawned.raft_registry.clone().expect("a Raft registry");
        tokio::time::timeout(Duration::from_secs(30), runtime.warm_all_groups())
            .await
            .expect("warm the groups in time")
            .expect("warm the groups");
        // Two streams in different groups.
        let deleted = BucketStreamId::new("scrub", "deleted");
        let expiring = (0..64)
            .map(|index| BucketStreamId::new("scrub", format!("expiring-{index}")))
            .find(|id| runtime.locate(id).raft_group_id != runtime.locate(&deleted).raft_group_id)
            .expect("a stream in the other group");
        write_marked(&runtime, &deleted, None).await;
        let expires_at_ms = wall_ms().saturating_add(1_500);
        write_marked(&runtime, &expiring, Some(expires_at_ms)).await;
        // The driver runs: its monitor reports the node's log.
        let deadline = Instant::now()
            .checked_add(Duration::from_secs(2))
            .expect("deadline");
        while runtime.metrics().snapshot().raft_log_bytes == 0 {
            assert!(Instant::now() < deadline, "no driver reports the log");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        let deleted_group = runtime.locate(&deleted).raft_group_id;
        let deleted_raft = registry.get(deleted_group).expect("registered group");
        runtime
            .delete_stream(ursula_runtime::DeleteStreamRequest {
                stream_id: deleted.clone(),
                now_ms: 0,
                if_incarnation: None,
            })
            .await
            .expect("delete the stream");
        let deleted_at = Instant::now();
        let deleted_index = deleted_raft
            .metrics()
            .borrow_watched()
            .last_applied
            .expect("applied the deletion")
            .index;

        // The leader's background sweep removes the expired stream within
        // its interval and grace after the expiry.
        let expiring_group = runtime.locate(&expiring).raft_group_id;
        let expiring_raft = registry.get(expiring_group).expect("registered group");
        let interval = ursula_runtime::expiry_sweep::interval(max_age);
        let until_swept = Duration::from_millis(1_500)
            .saturating_add(ursula_runtime::expiry_sweep::CLOCK_GRACE)
            .saturating_add(interval)
            .saturating_add(Duration::from_secs(2));
        let expired_index =
            wait_until_removed(&runtime, &expiring_raft, 1, expiring_group, until_swept).await;
        let expired_at = Instant::now();
        let metrics = runtime.metrics().snapshot();
        assert!(metrics.ttl_expiry_sweeps >= 1);
        assert!(metrics.ttl_expiry_swept_streams >= 1);

        for (raft, removal, removed_at) in [
            (&deleted_raft, deleted_index, deleted_at),
            (&expiring_raft, expired_index, expired_at),
        ] {
            let left = max_age
                .saturating_add(Duration::from_secs(2))
                .saturating_sub(removed_at.elapsed());
            raft.wait(Some(left))
                .metrics(
                    |metrics| {
                        metrics.snapshot.map(|snapshot| snapshot.index) >= Some(removal)
                            && metrics.purged.map(|purged| purged.index)
                                >= metrics.snapshot.map(|snapshot| snapshot.index)
                    },
                    "the log is purged through the snapshot that covers the removal",
                )
                .await
                .expect("scrub the removed stream within the bound");
        }
        assert!(!snapshot_holds_marker(&deleted_raft, &deleted).await);
        assert!(!snapshot_holds_marker(&expiring_raft, &expiring).await);
        // No file under the WAL root holds the records once the bound
        // passed since the later removal.
        let deadline = expired_at
            .checked_add(max_age.saturating_add(Duration::from_secs(2)))
            .expect("deadline");
        while let Some(path) = file_holding_marker(root.path()) {
            assert!(
                Instant::now() < deadline,
                "{} holds the removed records",
                path.display()
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        let deadline = Instant::now()
            .checked_add(Duration::from_secs(2))
            .expect("deadline");
        loop {
            let metrics = runtime.metrics().snapshot();
            if metrics.raft_deleted_data_oldest_age_ms == 0
                && metrics.raft_deleted_data_overdue_groups == 0
            {
                break;
            }
            assert!(Instant::now() < deadline, "the removals are still reported");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let app = crate::router(runtime.clone());
        for stream_id in [&deleted, &expiring] {
            let response = super::http_get(
                &app,
                &format!("/{}/{}?offset=0", stream_id.bucket_id, stream_id.stream_id),
            )
            .await;
            assert_eq!(response.status(), StatusCode::NOT_FOUND, "{stream_id}");
        }
        drop(app);
        drop(spawned);
        drop(root);
    }

    /// #669: a single node never closes admission for its log, as in 0.8.0.
    /// With its driver ticking once an hour, so that no snapshot frees the
    /// log, the log grows past twice a tiny `raft.snapshot_log_budget`, the
    /// driver's monitor reports it, and a write with a body still answers
    /// `204`.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_single_node_admits_writes_past_twice_its_log_budget() {
        let root = tempfile::tempdir().expect("WAL root");
        let budget = 64_u64 << 10;
        let mut config = super::test_config(1, 1);
        config.raft.snapshot_log_budget = ursula_config::HumanSize::bytes(budget);
        config.storage.snapshot.drive_interval = Some(ursula_config::HumanDuration::hour(1));
        let spawned = crate::bootstrap::spawn_runtime(
            &config,
            crate::bootstrap::Persistence::Raft {
                log_dir: root.path().to_owned(),
            },
            Topology::SingleNode {
                raft_group_count: 1,
            },
        )
        .expect("spawn a single node");
        let runtime = spawned.runtime.clone();
        let registry = spawned.raft_registry.clone().expect("a Raft registry");
        tokio::time::timeout(Duration::from_secs(30), runtime.warm_all_groups())
            .await
            .expect("warm the groups in time")
            .expect("warm the groups");
        let coordinator = registry.snapshot_build_coordinator();
        let app = crate::client_router_with_admission(
            HttpState::with_raft_registry(runtime.clone(), registry),
            crate::IngressAdmission::new(&config.server)
                .with_raft_log_pressure(Some(coordinator.clone())),
        );
        let stream = "/budget/stream";
        let created = super::http_put(
            &app,
            stream,
            &[("content-type", "application/octet-stream")],
            Body::empty(),
        )
        .await;
        assert_eq!(created.status(), StatusCode::CREATED);
        let limit = budget.saturating_mul(2);
        let deadline = Instant::now()
            .checked_add(Duration::from_secs(10))
            .expect("deadline");
        while runtime.metrics().snapshot().raft_log_bytes <= limit {
            assert!(
                Instant::now() < deadline,
                "the log stays below twice the budget"
            );
            let appended = super::http_post(
                &app,
                stream,
                &[("content-type", "application/octet-stream")],
                Body::from(vec![7_u8; 8 << 10]),
            )
            .await;
            assert_eq!(appended.status(), StatusCode::NO_CONTENT);
        }
        // Past twice the budget, which closes a cluster node's admission.
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert!(runtime.metrics().snapshot().raft_log_bytes > limit);
        assert!(!coordinator.log_pressured());
        let appended = super::http_post(
            &app,
            stream,
            &[("content-type", "application/octet-stream")],
            Body::from(vec![7_u8; 8 << 10]),
        )
        .await;
        assert_eq!(appended.status(), StatusCode::NO_CONTENT);
        drop(app);
        drop(spawned);
        drop(root);
    }

    /// #669: `storage.snapshot.drive_interval = "0s"` turns a single node's
    /// driver off: nothing reports its log. Its groups still sweep an
    /// expired stream, which needs no driver.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_single_node_without_its_driver_still_sweeps_expired_streams() {
        let (spawned, root, max_age) = single_node(Some(ursula_config::HumanDuration::sec(0)));
        let runtime = spawned.runtime.clone();
        let registry = spawned.raft_registry.clone().expect("a Raft registry");
        tokio::time::timeout(Duration::from_secs(30), runtime.warm_all_groups())
            .await
            .expect("warm the groups in time")
            .expect("warm the groups");
        let expiring = BucketStreamId::new("scrub", "expiring");
        write_marked(&runtime, &expiring, Some(wall_ms().saturating_add(500))).await;
        let group = runtime.locate(&expiring).raft_group_id;
        let raft = registry.get(group).expect("registered group");
        let until_swept = Duration::from_millis(500)
            .saturating_add(ursula_runtime::expiry_sweep::CLOCK_GRACE)
            .saturating_add(ursula_runtime::expiry_sweep::interval(max_age))
            .saturating_add(Duration::from_secs(2));
        wait_until_removed(&runtime, &raft, 1, group, until_swept).await;
        let metrics = runtime.metrics().snapshot();
        assert_eq!(metrics.raft_log_bytes, 0, "no driver runs");
        assert!(metrics.ttl_expiry_sweeps >= 1);
        drop(spawned);
        drop(root);
    }

    /// #669: the production driver snapshots a group a quarter of
    /// `raft.deleted_data_max_age` after it deleted a stream, purges its log
    /// through that snapshot, and reports the deleted data its groups hold
    /// meanwhile.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn the_driver_scrubs_a_deleted_stream_within_the_bound() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind listener");
        let peers = vec![(
            1,
            format!(
                "http://127.0.0.1:{}",
                listener.local_addr().expect("addr").port()
            ),
        )];
        let node = spawn_static_grpc_test_node(
            1,
            listener,
            peers.clone(),
            peers,
            true,
            2,
            StaticGrpcTestNodeStorage {
                engine_config: Some(RaftEngineConfig {
                    snapshot_drive_interval_ms: 100,
                    max_in_snapshot_log_to_keep: 8,
                    ..RaftEngineConfig::default()
                }),
                ..StaticGrpcTestNodeStorage::default()
            },
        )
        .await;
        tokio::time::timeout(Duration::from_secs(30), node.runtime.warm_all_groups())
            .await
            .expect("warm the groups in time")
            .expect("warm the groups");
        let max_age = Duration::from_secs(4);
        spawn_snapshot_driver(
            &node.runtime,
            &node.registry,
            None,
            LogWriteGate::RefuseAtHardLimit,
            100,
            SnapshotCadence::new(1 << 30, 2, 100_000).with_deleted_data_max_age(max_age),
            None,
            16,
        );
        let stream_id = BucketStreamId::new("erased", "marked");
        let group = node.runtime.locate(&stream_id).raft_group_id;
        let raft = node.registry.get(group).expect("registered group");
        raft.wait(Some(Duration::from_secs(10)))
            .current_leader(1, "the only voter leads")
            .await
            .expect("the group elects its leader");
        node.runtime
            .create_stream(CreateStreamRequest::new(
                stream_id.clone(),
                "application/octet-stream",
            ))
            .await
            .expect("create the stream");
        for _ in 0..3 {
            node.runtime
                .append(AppendRequest::from_bytes(
                    stream_id.clone(),
                    DELETED_MARKER.to_vec(),
                ))
                .await
                .expect("append a marked record");
        }
        // A snapshot holds the stream before it goes. OpenRaft drops a
        // trigger while the driver's first build of the group runs.
        let applied = raft.metrics().borrow_watched().last_applied;
        let mut covered = false;
        for _ in 0..20 {
            raft.trigger().snapshot().await.expect("trigger a snapshot");
            covered = raft
                .wait(Some(Duration::from_millis(500)))
                .metrics(
                    |metrics| metrics.snapshot >= applied,
                    "the snapshot covers the marked records",
                )
                .await
                .is_ok();
            if covered {
                break;
            }
        }
        assert!(covered, "no snapshot covers the marked records");
        assert!(snapshot_holds_marker(&raft, &stream_id).await);

        node.runtime
            .delete_stream(ursula_runtime::DeleteStreamRequest {
                stream_id: stream_id.clone(),
                now_ms: 0,
                if_incarnation: None,
            })
            .await
            .expect("delete the stream");
        let deleted_at = Instant::now();
        let removal = raft
            .metrics()
            .borrow_watched()
            .last_applied
            .expect("applied the deletion");
        // The node reports the removal it still holds.
        let deadline = deleted_at
            .checked_add(Duration::from_secs(1))
            .expect("deadline");
        while node
            .runtime
            .metrics()
            .snapshot()
            .raft_deleted_data_oldest_age_ms
            == 0
        {
            assert!(Instant::now() < deadline, "the removal is never reported");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        // Within the bound the snapshot covers the deletion and the log is
        // purged through it.
        let scrubbed = raft
            .wait(Some(max_age))
            .metrics(
                |metrics| {
                    metrics.snapshot >= Some(removal)
                        && metrics.purged.map(|purged| purged.index)
                            >= metrics.snapshot.map(|snapshot| snapshot.index)
                },
                "the log is purged through the snapshot that covers the deletion",
            )
            .await
            .expect("scrub the deleted stream within the bound");
        assert!(deleted_at.elapsed() >= max_age.checked_div(4).expect("a quarter"));
        assert!(!snapshot_holds_marker(&raft, &stream_id).await);
        assert!(scrubbed.purged >= scrubbed.snapshot);
        let deadline = Instant::now()
            .checked_add(Duration::from_secs(1))
            .expect("deadline");
        loop {
            let metrics = node.runtime.metrics().snapshot();
            if metrics.raft_deleted_data_oldest_age_ms == 0
                && metrics.raft_deleted_data_overdue_groups == 0
            {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "the scrubbed removal is still reported"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    /// An S3 snapshot store that remembers every object it uploaded, so a
    /// test can check each one after snapshot GC ran.
    #[derive(Debug)]
    struct RecordingStore {
        inner: ursula_runtime::S3SnapshotStore,
        uploaded: std::sync::Mutex<Vec<SnapshotLocation>>,
    }

    impl RecordingStore {
        fn record(&self, location: &SnapshotLocation) {
            self.uploaded
                .lock()
                .expect("uploads mutex")
                .push(location.clone());
        }

        /// The uploaded objects the store still holds that hold the marker.
        async fn objects_with_marker(&self) -> usize {
            let uploaded = self.uploaded.lock().expect("uploads mutex").clone();
            let mut found = 0_usize;
            for location in &uploaded {
                match self.inner.download(location).await {
                    Ok(bytes) => {
                        if bytes
                            .windows(DELETED_MARKER.len())
                            .any(|window| window == DELETED_MARKER)
                        {
                            found = found.saturating_add(1);
                        }
                    }
                    Err(SnapshotStoreError::NotFound { .. }) => {}
                    Err(err) => panic!("read {location:?}: {err}"),
                }
            }
            found
        }
    }

    impl SnapshotStore for RecordingStore {
        fn upload<'a>(
            &'a self,
            key: SnapshotKey,
            bytes: Bytes,
        ) -> SnapshotStoreFuture<'a, SnapshotLocation> {
            Box::pin(async move {
                let location = self.inner.upload(key, bytes).await?;
                self.record(&location);
                Ok(location)
            })
        }

        fn upload_iter<'a>(
            &'a self,
            key: SnapshotKey,
            chunks: ursula_runtime::SnapshotBytesIterator,
        ) -> SnapshotStoreFuture<'a, SnapshotLocation> {
            Box::pin(async move {
                let location = self.inner.upload_iter(key, chunks).await?;
                self.record(&location);
                Ok(location)
            })
        }

        fn download<'a>(
            &'a self,
            location: &'a SnapshotLocation,
        ) -> SnapshotStoreFuture<'a, Vec<u8>> {
            self.inner.download(location)
        }

        fn delete<'a>(&'a self, location: &'a SnapshotLocation) -> SnapshotStoreFuture<'a, ()> {
            self.inner.delete(location)
        }

        fn prune_retired<'a>(
            &'a self,
            raft_group_id: u32,
            current: &'a SnapshotLocation,
        ) -> SnapshotStoreFuture<'a, ursula_runtime::SnapshotPrune> {
            self.inner.prune_retired(raft_group_id, current)
        }

        fn publish_reference<'a>(
            &'a self,
            raft_group_id: u32,
            location: &'a SnapshotLocation,
        ) -> SnapshotStoreFuture<'a, ()> {
            self.inner.publish_reference(raft_group_id, location)
        }

        fn pin_reference<'a>(
            &'a self,
            raft_group_id: u32,
            location: &'a SnapshotLocation,
        ) -> SnapshotStoreFuture<'a, ()> {
            self.inner.pin_reference(raft_group_id, location)
        }

        fn reconcile_reference_pins<'a>(
            &'a self,
            raft_group_id: u32,
            retained: &'a [SnapshotLocation],
        ) -> SnapshotStoreFuture<'a, ()> {
            self.inner.reconcile_reference_pins(raft_group_id, retained)
        }

        fn verify_uploaded<'a>(
            &'a self,
            location: &'a SnapshotLocation,
        ) -> SnapshotStoreFuture<'a, ()> {
            self.inner.verify_uploaded(location)
        }

        fn namespace(
            &self,
            raft_group_id: ursula_shard::RaftGroupId,
        ) -> ursula_runtime::OwnedNamespace {
            ursula_runtime::SnapshotStore::namespace(&self.inner, raft_group_id)
        }

        fn stage<'a>(
            &'a self,
            raft_group_id: ursula_shard::RaftGroupId,
            index: u64,
            chunks: ursula_runtime::SnapshotBytesIterator,
        ) -> ursula_runtime::SnapshotStoreFuture<'a, ursula_runtime::StagedSnapshot> {
            ursula_runtime::SnapshotStore::stage(&self.inner, raft_group_id, index, chunks)
        }

        fn write_staged<'a>(
            &'a self,
            staged: ursula_runtime::StagedSnapshot,
        ) -> ursula_runtime::SnapshotStoreFuture<'a, ursula_runtime::SnapshotLocation> {
            ursula_runtime::SnapshotStore::write_staged(&self.inner, staged)
        }

        fn list_own<'a>(
            &'a self,
            raft_group_id: ursula_shard::RaftGroupId,
        ) -> ursula_runtime::SnapshotStoreFuture<'a, ursula_runtime::OwnListing> {
            ursula_runtime::SnapshotStore::list_own(&self.inner, raft_group_id)
        }

        fn delete_own<'a>(
            &'a self,
            object: ursula_runtime::OwnedObject,
        ) -> ursula_runtime::SnapshotStoreFuture<'a, ()> {
            ursula_runtime::SnapshotStore::delete_own(&self.inner, object)
        }
    }

    /// How the node's snapshot store treats an object nothing names.
    #[derive(Debug, Clone, Copy)]
    enum ObjectGrace {
        /// Deleted once it is the GC grace old, as on a server.
        GcGrace,
        /// Kept for an hour: snapshot GC deletes nothing within the test.
        Hour,
    }

    /// #669: one node with an S3 snapshot store (on a local directory)
    /// deletes a stream that its snapshot object holds. Returns how many
    /// uploaded objects held the marker right after the covering snapshot
    /// and once `max_age` passed, and the node's metrics at the end.
    async fn delete_with_s3_snapshots(
        object_grace: ObjectGrace,
    ) -> (usize, usize, ursula_runtime::RuntimeMetricsSnapshot) {
        let max_age = Duration::from_secs(8);
        let gc_grace = max_age.checked_div(4).expect("a quarter");
        // A local directory: unlike the in-memory operator, its objects
        // carry the modification time the GC grace reads.
        let objects = tempfile::tempdir().expect("object root");
        let operator = opendal::Operator::new(
            opendal::services::Fs::default()
                .root(objects.path().to_str().expect("a UTF-8 object root")),
        )
        .expect("object store")
        .finish();
        let store = Arc::new(RecordingStore {
            inner: ursula_runtime::S3SnapshotStore::new(
                operator,
                ursula_runtime::SnapshotPrefix::try_from("snapshots").unwrap(),
                1,
            )
            .with_references(ursula_runtime::SnapshotReferenceConfig {
                node_id: 1,
                default_voters: BTreeSet::from([1]),
                per_group_voters: BTreeMap::new(),
            })
            .with_gc_grace(match object_grace {
                ObjectGrace::GcGrace => gc_grace,
                ObjectGrace::Hour => Duration::from_secs(60 * 60),
            }),
            uploaded: std::sync::Mutex::new(Vec::new()),
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind listener");
        let peers = vec![(
            1,
            format!(
                "http://127.0.0.1:{}",
                listener.local_addr().expect("addr").port()
            ),
        )];
        let node = spawn_static_grpc_test_node(
            1,
            listener,
            peers.clone(),
            peers,
            true,
            2,
            StaticGrpcTestNodeStorage {
                engine_config: Some(RaftEngineConfig {
                    snapshot_drive_interval_ms: 100,
                    max_in_snapshot_log_to_keep: 8,
                    ..RaftEngineConfig::default()
                }),
                snapshot_store: Some(store.clone()),
                snapshot_gc_grace: Some(gc_grace),
                ..StaticGrpcTestNodeStorage::default()
            },
        )
        .await;
        tokio::time::timeout(Duration::from_secs(30), node.runtime.warm_all_groups())
            .await
            .expect("warm the groups in time")
            .expect("warm the groups");
        spawn_snapshot_driver(
            &node.runtime,
            &node.registry,
            None,
            LogWriteGate::RefuseAtHardLimit,
            100,
            SnapshotCadence::new(1 << 30, 2, 100_000).with_deleted_data_max_age(max_age),
            None,
            16,
        );
        let stream_id = BucketStreamId::new("erased", "marked-s3");
        let group = node.runtime.locate(&stream_id).raft_group_id;
        let raft = node.registry.get(group).expect("registered group");
        raft.wait(Some(Duration::from_secs(10)))
            .current_leader(1, "the only voter leads")
            .await
            .expect("the group elects its leader");
        node.runtime
            .create_stream(CreateStreamRequest::new(
                stream_id.clone(),
                "application/octet-stream",
            ))
            .await
            .expect("create the stream");
        for _ in 0..3 {
            node.runtime
                .append(AppendRequest::from_bytes(
                    stream_id.clone(),
                    DELETED_MARKER.to_vec(),
                ))
                .await
                .expect("append a marked record");
        }
        // A snapshot object holds the stream before it goes.
        let applied = raft.metrics().borrow_watched().last_applied;
        let mut covered = false;
        for _ in 0..20 {
            raft.trigger().snapshot().await.expect("trigger a snapshot");
            covered = raft
                .wait(Some(Duration::from_millis(500)))
                .metrics(
                    |metrics| metrics.snapshot >= applied,
                    "the snapshot covers the marked records",
                )
                .await
                .is_ok();
            if covered {
                break;
            }
        }
        assert!(covered, "no snapshot covers the marked records");
        assert!(store.objects_with_marker().await > 0);

        node.runtime
            .delete_stream(ursula_runtime::DeleteStreamRequest {
                stream_id: stream_id.clone(),
                now_ms: 0,
                if_incarnation: None,
            })
            .await
            .expect("delete the stream");
        let deleted_at = Instant::now();
        let removal = raft
            .metrics()
            .borrow_watched()
            .last_applied
            .expect("applied the deletion");
        raft.wait(Some(max_age))
            .metrics(
                |metrics| metrics.snapshot >= Some(removal),
                "a snapshot covers the deletion",
            )
            .await
            .expect("snapshot the deletion within the bound");
        // The object the covering snapshot replaced is pinned for the GC
        // grace.
        let pinned = store.objects_with_marker().await;
        let deadline = deleted_at.checked_add(max_age).expect("deadline");
        let mut left = store.objects_with_marker().await;
        while left > 0 && Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(100)).await;
            left = store.objects_with_marker().await;
        }
        let metrics = node.runtime.metrics().snapshot();
        node.shutdown().await;
        (pinned, left, metrics)
    }

    /// #669: within `raft.deleted_data_max_age` of a deletion, snapshot GC
    /// has deleted every snapshot object that held the stream, the one the
    /// covering snapshot replaced included, which stays pinned for the GC
    /// grace first.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn snapshot_gc_deletes_the_replaced_object_within_the_bound() {
        let (pinned, left, metrics) = delete_with_s3_snapshots(ObjectGrace::GcGrace).await;
        assert!(
            pinned > 0,
            "the object the covering snapshot replaced is pinned for the grace"
        );
        assert_eq!(left, 0, "snapshot objects still hold the deleted stream");
        assert!(metrics.snapshot_gc_deleted_objects > 0);
        assert_eq!(metrics.snapshot_gc_skipped_unreferenced_voter, 0);
    }

    /// The negative control: a store that keeps unnamed objects for an hour
    /// still holds the deleted stream once the bound passed, so the check
    /// above sees what snapshot GC deletes.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn without_snapshot_gc_the_replaced_object_outlives_the_bound() {
        let (_pinned, left, metrics) = delete_with_s3_snapshots(ObjectGrace::Hour).await;
        assert!(left > 0, "a snapshot object still holds the deleted stream");
        assert_eq!(metrics.snapshot_gc_deleted_objects, 0);
    }

    /// #519: past the hard limit the driver keeps as many builds in flight
    /// as the node allows, and reports the escalation. The node runs as many
    /// builds at once as the server's default config gives it: two.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn the_driver_uses_every_build_slot_at_the_hard_limit() {
        let store = GatedStore::new();
        let (node, _) = loaded_node(&store, &[4; 8], default_permits()).await;
        let coordinator = node.registry.snapshot_build_coordinator();
        assert_eq!(coordinator.max_concurrency(), 2);
        let log = coordinator.node_log_bytes();
        // A budget of a third of the log puts the node past its hard limit.
        let cadence = SnapshotCadence::new(log / 3, group_count(), 100_000);
        assert_eq!(cadence.pressure(log, 2), LogPressure::Escalated {
            builds: 2
        });
        drive(&node, &store, cadence);
        store.wait_for_uploads(2, Duration::from_secs(30)).await;
        // The node runs two builds at most: the next waits for a slot.
        tokio::time::sleep(Duration::from_millis(500)).await;
        assert_eq!(store.uploads().most, 2);
        let metrics = node.runtime.metrics().snapshot();
        assert_eq!(metrics.raft_snapshot_pressure_level, 2);
        assert_eq!(metrics.raft_snapshot_pressure_builds, 2);
        assert_eq!(metrics.raft_snapshot_builds_in_flight_max, 2);
        assert!(metrics.raft_log_bytes_high_watermark >= log);
        store.open();
        wait_for_log_at_most(&node, cadence.pressure_target_bytes()).await;
        assert_eq!(store.uploads().most, 2);
        node.shutdown().await;
    }

    /// #519: below the budget the driver builds one group at a time, though
    /// the node allows two (the server's default).
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn the_driver_builds_one_group_at_a_time_below_the_budget() {
        let store = GatedStore::new();
        let (node, _) = loaded_node(&store, &[4; 8], default_permits()).await;
        let coordinator = node.registry.snapshot_build_coordinator();
        assert_eq!(coordinator.max_concurrency(), 2);
        let log = coordinator.node_log_bytes();
        // The log at seven eighths of the budget: past the watermark.
        let cadence = SnapshotCadence::new((log / 7).saturating_mul(8), group_count(), 100_000);
        assert_eq!(cadence.pressure(log, 2), LogPressure::Watermark);
        drive(&node, &store, cadence);
        store.wait_for_uploads(1, Duration::from_secs(30)).await;
        // Several driver ticks pass with the first build held at the store.
        tokio::time::sleep(Duration::from_millis(500)).await;
        assert_eq!(store.uploads().most, 1);
        assert_eq!(
            node.runtime
                .metrics()
                .snapshot()
                .raft_snapshot_pressure_level,
            1
        );
        store.open();
        wait_for_log_at_most(&node, cadence.pressure_target_bytes()).await;
        assert_eq!(store.uploads().most, 1);
        node.shutdown().await;
    }

    /// #519: the builds of a pass follow the log down. A pass that started
    /// four builds near the hard limit starts no other while one of them
    /// still runs, once the other three brought the log below the budget.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn an_escalated_pass_keeps_one_build_in_flight_once_the_log_is_below_the_budget() {
        let store = GatedStore::new();
        // Four groups of eight appends, which a pass starts first, and four
        // of one.
        let (node, _) = loaded_node(&store, &[8, 8, 8, 8, 1, 1, 1, 1], 4).await;
        let coordinator = node.registry.snapshot_build_coordinator();
        let log = coordinator.node_log_bytes();
        let largest = coordinator
            .log_progress()
            .values()
            .map(|progress| progress.log_bytes)
            .max()
            .expect("the node's groups");
        // The log at 1.9 budgets: four builds.
        let cadence = SnapshotCadence::new((log / 19).saturating_mul(10), group_count(), 100_000);
        assert_eq!(cadence.pressure(log, 4), LogPressure::Escalated {
            builds: 4
        });
        // Once three of the large groups are snapshotted the log is below
        // the budget, where a pass keeps one build in flight.
        let rest = log.saturating_sub(largest.saturating_mul(3));
        assert!(rest < cadence.node_budget_bytes);
        assert_eq!(cadence.pressure(rest, 4).builds(), 1);
        drive(&node, &store, cadence);
        store.wait_for_uploads(4, Duration::from_secs(30)).await;
        assert_eq!(store.uploads().started, 4);

        store.release(3);
        wait_for_log_at_most(&node, cadence.node_budget_bytes.saturating_sub(1)).await;
        // Driver polls pass with the fourth build held at the store, and the
        // pass, planned for four builds, starts none of its other groups.
        tokio::time::sleep(Duration::from_millis(500)).await;
        let uploads = store.uploads();
        assert_eq!((uploads.started, uploads.running), (4, 1));
        let metrics = node.runtime.metrics().snapshot();
        assert_eq!(metrics.raft_snapshot_pressure_builds, 1);
        assert_eq!(metrics.raft_snapshot_builds_in_flight, 1);
        store.open();
        wait_for_log_at_most(&node, cadence.pressure_target_bytes()).await;
        node.shutdown().await;
    }

    /// #519: a pass that waits for a build slot follows the log up too. Its
    /// one build is held at the store while writes take the log from the
    /// watermark to 1.75 budgets: the pass ends, and the passes after it run
    /// every build slot, well before the minute a pass waits for a slot. The
    /// driver's interval is longer than the test, so only its pressure
    /// re-tick (250 ms) starts the passes after the first.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_pass_waiting_for_a_slot_escalates_with_the_log() {
        let store = GatedStore::new();
        let (node, streams) = loaded_node(&store, &[4; 8], 4).await;
        let coordinator = node.registry.snapshot_build_coordinator();
        let log = coordinator.node_log_bytes();
        // The log at seven eighths of the budget: past the watermark.
        let cadence = SnapshotCadence::new((log / 7).saturating_mul(8), group_count(), 100_000);
        assert_eq!(cadence.pressure(log, 4), LogPressure::Watermark);
        drive_every(&node, &store, cadence, 20_000);
        store.wait_for_uploads(1, Duration::from_secs(30)).await;
        // The pass took group 0 first: write to the others.
        let target = (cadence.node_budget_bytes / 4).saturating_mul(7);
        for stream_id in streams.iter().skip(1).cycle() {
            if coordinator.node_log_bytes() >= target {
                break;
            }
            append(&node, stream_id, 1).await;
        }
        let log = coordinator.node_log_bytes();
        assert_eq!(cadence.pressure(log, 4), LogPressure::Escalated {
            builds: 4
        });
        assert!(log < cadence.hard_limit_bytes());
        store.wait_for_uploads(4, Duration::from_secs(5)).await;
        // Several monitor polls, so the gauges saw the fourth build.
        tokio::time::sleep(Duration::from_millis(500)).await;
        let metrics = node.runtime.metrics().snapshot();
        assert_eq!(metrics.raft_snapshot_pressure_level, 2);
        assert_eq!(metrics.raft_snapshot_builds_in_flight_max, 4);
        assert_eq!(store.uploads().most, 4);
        // The pass that waited ended when the log outgrew it: a pass counts
        // once it ends.
        assert!(metrics.raft_snapshot_pressure_passes >= 1);
        store.open();
        wait_for_log_at_most(&node, cadence.pressure_target_bytes()).await;
        node.shutdown().await;
    }

    /// #519: the driver never triggers a group whose build is in flight,
    /// since OpenRaft drops that trigger and the pass would wait out the
    /// handoff (5 s). A build started outside the driver holds one of four
    /// slots at the hard limit, on the group with the most log, and the
    /// driver fills the other three at once.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn the_driver_skips_a_group_whose_build_is_in_flight() {
        let store = GatedStore::new();
        let (node, _) = loaded_node(&store, &[8, 4, 4, 4, 4, 4, 4, 4], 4).await;
        let coordinator = node.registry.snapshot_build_coordinator();
        node.registry
            .get(RaftGroupId(0))
            .expect("registered group")
            .trigger()
            .snapshot()
            .await
            .expect("trigger group 0's snapshot");
        store.wait_for_uploads(1, Duration::from_secs(30)).await;
        assert_eq!(
            coordinator
                .builds_in_flight()
                .into_keys()
                .collect::<Vec<_>>(),
            vec![0]
        );
        let log = coordinator.node_log_bytes();
        let cadence = SnapshotCadence::new(log / 3, group_count(), 100_000);
        assert_eq!(cadence.pressure(log, 4), LogPressure::Escalated {
            builds: 4
        });
        drive(&node, &store, cadence);
        store.wait_for_uploads(4, Duration::from_secs(4)).await;
        assert_eq!(store.uploads().started, 4);
        store.open();
        wait_for_log_at_most(&node, cadence.pressure_target_bytes()).await;
        node.shutdown().await;
    }

    /// #561: the driver triggers nothing for a busy group, one that builds
    /// or has a permit handed to it, and does not hold it: the group's build
    /// claims no other permit, and OpenRaft would drop the trigger while the
    /// group builds. It sees the group building before it waits for a build
    /// slot, so with one slot, the one the group's build holds, the driver
    /// does not wait for that build to end and then trigger a group just
    /// built.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn the_driver_skips_a_busy_group_without_waiting_for_a_slot() {
        let store = GatedStore::new();
        let (node, _) = node_with_permits(&store, &[1; 8], 3, 0).await;
        let coordinator = node.registry.snapshot_build_coordinator();
        node.registry
            .get(RaftGroupId(0))
            .expect("registered group")
            .trigger()
            .snapshot()
            .await
            .expect("trigger group 0's snapshot");
        store.wait_for_uploads(1, Duration::from_secs(30)).await;
        for slots in [1, 3] {
            let trigger = tokio::time::timeout(
                Duration::from_secs(1),
                trigger_snapshot_build(
                    &node.registry,
                    &coordinator,
                    0,
                    &mut coordinator.subscribe_builds(),
                    || Some(slots),
                ),
            )
            .await;
            assert!(matches!(trigger, Ok(Trigger::Busy)), "{slots}: {trigger:?}");
        }
        assert_eq!(store.uploads().started, 1);
        assert_eq!(coordinator.running_builds(), 1);
        // A group with a permit handed to it is busy too.
        let (handed, hand_off) = tokio::sync::oneshot::channel();
        let pending = tokio::spawn({
            let coordinator = coordinator.clone();
            async move {
                let permit = coordinator.acquire().await.expect("a permit");
                coordinator
                    .hand_off_and_trigger(1, permit, Duration::from_secs(3_600), async move {
                        handed.send(()).expect("the test waits");
                        std::future::pending::<Result<(), ()>>().await
                    })
                    .await
            }
        });
        hand_off.await.expect("the hand-off is pending");
        let trigger = tokio::time::timeout(
            Duration::from_secs(1),
            trigger_snapshot_build(
                &node.registry,
                &coordinator,
                1,
                &mut coordinator.subscribe_builds(),
                || Some(3),
            ),
        )
        .await;
        assert!(matches!(trigger, Ok(Trigger::Busy)), "{trigger:?}");
        assert_eq!(store.uploads().started, 1);
        pending.abort();
        assert!(pending.await.is_err_and(|err| err.is_cancelled()));
        // A group that becomes busy while the driver waits for a slot: the
        // hand-off finds it busy, so nothing is triggered or held.
        let handed = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let mut other = None;
        let pace = || {
            if other.is_none() {
                let coordinator = coordinator.clone();
                let handed = std::sync::Arc::clone(&handed);
                other = Some(tokio::spawn(async move {
                    let permit = coordinator.acquire().await.expect("a permit");
                    coordinator
                        .hand_off_and_trigger(2, permit, Duration::from_secs(3_600), async move {
                            handed.store(true, std::sync::atomic::Ordering::Relaxed);
                            std::future::pending::<Result<(), ()>>().await
                        })
                        .await
                }));
            }
            // No slot below group 0's build until group 2 is busy.
            Some(if handed.load(std::sync::atomic::Ordering::Relaxed) {
                3
            } else {
                1
            })
        };
        let trigger = tokio::time::timeout(
            Duration::from_secs(5),
            trigger_snapshot_build(
                &node.registry,
                &coordinator,
                2,
                &mut coordinator.subscribe_builds(),
                pace,
            ),
        )
        .await;
        assert!(matches!(trigger, Ok(Trigger::Busy)), "{trigger:?}");
        assert_eq!(store.uploads().started, 1);
        if let Some(other) = other {
            other.abort();
        }
        store.open();
        node.shutdown().await;
    }

    /// A node with `permits` build permits and no snapshot driver, so
    /// snapshots run only when an operator asks for them, and its admin
    /// router.
    async fn operator_node(
        store: &Arc<GatedStore>,
        permits: usize,
    ) -> (StaticGrpcTestNode, Router) {
        let (node, _) = node_with_permits(store, &vec![1; group_count()], permits, 0).await;
        let admin = admin_router(HttpState::with_raft_registry(
            node.runtime.clone(),
            node.registry.clone(),
        ));
        (node, admin)
    }

    /// Asks for a snapshot of `group` as an operator does.
    fn operator_snapshot(admin: &Router, group: u32) -> JoinHandle<Response> {
        let admin = admin.clone();
        tokio::spawn(async move {
            send(
                &admin,
                "POST",
                &format!("/__ursula/raft/{group}/snapshot"),
                &[],
                Body::empty(),
            )
            .await
        })
    }

    async fn snapshot_index(response: Response) -> Option<u64> {
        let status = response.status();
        let body = body_bytes(response).await;
        assert_eq!(status, StatusCode::OK, "{}", String::from_utf8_lossy(&body));
        let body: SnapshotResponse = serde_json::from_slice(&body).expect("snapshot response JSON");
        body.snapshot_index
    }

    /// While another group's build holds the node's only permit, an
    /// operator's snapshot waits for it and then builds, where it answered
    /// 500 once its 10 s wait for a refused build ran out (#529).
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn an_operator_snapshot_waits_for_a_build_permit() {
        let store = GatedStore::new();
        let (node, admin) = operator_node(&store, 1).await;
        let first = operator_snapshot(&admin, 0);
        store.wait_for_uploads(1, Duration::from_secs(30)).await;
        let second = operator_snapshot(&admin, 1);
        tokio::time::sleep(Duration::from_millis(500)).await;
        assert_eq!(store.uploads().started, 1, "group 1 waits for the permit");
        store.release(2);
        assert!(
            snapshot_index(first.await.expect("group 0's request"))
                .await
                .is_some()
        );
        assert!(
            snapshot_index(second.await.expect("group 1's request"))
                .await
                .is_some()
        );
        assert_eq!(store.uploads().started, 2);
        node.shutdown().await;
    }

    /// An operator's snapshot that gets no permit in time answers 503 with
    /// `Retry-After`, since a busy permit is transient (#529).
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn an_operator_snapshot_answers_503_while_every_permit_stays_in_use() {
        let store = GatedStore::new();
        let (node, admin) = operator_node(&store, 1).await;
        node.registry
            .get(RaftGroupId(0))
            .expect("registered group")
            .trigger()
            .snapshot()
            .await
            .expect("trigger group 0's snapshot");
        store.wait_for_uploads(1, Duration::from_secs(30)).await;
        let second = operator_snapshot(&admin, 1)
            .await
            .expect("group 1's request");
        assert_eq!(second.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            second
                .headers()
                .get(axum::http::header::RETRY_AFTER)
                .and_then(|value| value.to_str().ok()),
            Some("1")
        );
        assert_eq!(store.uploads().started, 1);
        store.open();
        node.shutdown().await;
    }

    /// #669: an operator's purge follows the hold rule of every purge: while
    /// a build of the group runs it answers 503 with `Retry-After`, and once
    /// the group is idle it purges through the snapshot.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn an_operator_purge_answers_503_while_its_group_builds() {
        let store = GatedStore::new();
        let (node, admin) = operator_node(&store, 2).await;
        store.release(1);
        let snapshot = snapshot_index(operator_snapshot(&admin, 0).await.expect("snapshot"))
            .await
            .expect("a snapshot index");
        let purge = |admin: Router| async move {
            send(
                &admin,
                "POST",
                &format!("/__ursula/raft/0/purge?upto={snapshot}"),
                &[],
                Body::empty(),
            )
            .await
        };
        node.registry
            .get(RaftGroupId(0))
            .expect("registered group")
            .trigger()
            .snapshot()
            .await
            .expect("trigger group 0's snapshot");
        store.wait_for_uploads(1, Duration::from_secs(30)).await;
        let busy = purge(admin.clone()).await;
        assert_eq!(busy.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            busy.headers()
                .get(axum::http::header::RETRY_AFTER)
                .and_then(|value| value.to_str().ok()),
            Some("1")
        );
        store.open();
        node.registry
            .snapshot_build_coordinator()
            .build_ended(0)
            .await;
        let mut purged = None;
        for _ in 0..100 {
            let response = purge(admin.clone()).await;
            if response.status() == StatusCode::OK {
                purged = Some(response);
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        assert!(
            purged.is_some(),
            "the purge never ran once the group was idle"
        );
        node.shutdown().await;
    }

    /// An operator's snapshot of a group with a hand-off pending, as when the
    /// snapshot driver just handed it a permit, waits until that hand-off
    /// settles, here because its caller stopped waiting and gave the permit
    /// back, and then builds (#529).
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn an_operator_snapshot_waits_for_a_pending_handoff() {
        let store = GatedStore::new();
        let (node, admin) = operator_node(&store, 2).await;
        let coordinator = node.registry.snapshot_build_coordinator();
        let (handed, hand_off) = tokio::sync::oneshot::channel();
        let pending = tokio::spawn({
            let coordinator = coordinator.clone();
            async move {
                let permit = coordinator.acquire().await.expect("a permit");
                coordinator
                    .hand_off_and_trigger(0, permit, Duration::from_secs(3_600), async move {
                        handed.send(()).expect("the test waits");
                        std::future::pending::<Result<(), ()>>().await
                    })
                    .await
            }
        });
        hand_off.await.expect("the hand-off is pending");
        let request = operator_snapshot(&admin, 0);
        tokio::time::sleep(Duration::from_millis(500)).await;
        assert!(!request.is_finished(), "the request waits for the hand-off");
        assert_eq!(store.uploads().started, 0);
        pending.abort();
        store.open();
        assert!(
            snapshot_index(request.await.expect("the request"))
                .await
                .is_some()
        );
        assert_eq!(store.uploads().started, 1);
        node.shutdown().await;
    }

    /// #561: an operator's snapshot of a group whose running build covers
    /// what the request asks for, nothing applied since, builds nothing
    /// again once that build ended, though a permit is free for it.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn an_operator_snapshot_covered_by_a_running_build_builds_nothing_again() {
        let store = GatedStore::new();
        let (node, admin) = operator_node(&store, 2).await;
        let first = operator_snapshot(&admin, 0);
        store.wait_for_uploads(1, Duration::from_secs(30)).await;
        let second = operator_snapshot(&admin, 0);
        tokio::time::sleep(Duration::from_millis(500)).await;
        store.open();
        let first = snapshot_index(first.await.expect("the first request")).await;
        let second = snapshot_index(second.await.expect("the second request")).await;
        assert!(first.is_some());
        assert_eq!(second, first);
        assert_eq!(store.uploads().started, 1);
        node.shutdown().await;
    }

    /// #561: an operator's snapshot whose build's housekeeping outlasts
    /// [`ADMIN_SNAPSHOT_PUBLISH_WAIT`] answers 200 once OpenRaft publishes
    /// the snapshot: the request waits for the build to end, bounded on its
    /// own, before it waits for OpenRaft, which publishes the snapshot only
    /// then.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn an_operator_snapshot_outlasts_its_builds_housekeeping() {
        let store = GatedStore::new();
        let (node, admin) = operator_node(&store, 2).await;
        store.open();
        store.hold_pruning(true);
        let request = operator_snapshot(&admin, 0);
        store.wait_for_prunes(1, Duration::from_secs(30)).await;
        tokio::time::sleep(
            ADMIN_SNAPSHOT_PUBLISH_WAIT
                .checked_add(Duration::from_secs(2))
                .expect("a short wait"),
        )
        .await;
        assert!(!request.is_finished(), "the build still prunes");
        store.hold_pruning(false);
        assert!(
            snapshot_index(request.await.expect("the request"))
                .await
                .is_some()
        );
        assert_eq!(store.uploads().started, 1);
        node.shutdown().await;
    }

    /// #561: an operator's snapshot that OpenRaft's published snapshot
    /// already covers answers at once, though another build of the group
    /// runs: it does not wait for that build to end.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn an_operator_snapshot_already_covered_waits_for_no_build() {
        let store = GatedStore::new();
        let (node, admin) = operator_node(&store, 2).await;
        store.open();
        let first = snapshot_index(
            operator_snapshot(&admin, 0)
                .await
                .expect("the first request"),
        )
        .await;
        assert!(first.is_some());
        store.hold_pruning(true);
        let prunes = *store.prunes.borrow();
        node.registry
            .get(RaftGroupId(0))
            .expect("registered group")
            .trigger()
            .snapshot()
            .await
            .expect("trigger group 0's snapshot");
        store
            .wait_for_prunes(prunes.saturating_add(1), Duration::from_secs(30))
            .await;
        assert_eq!(
            node.registry.snapshot_build_coordinator().running_builds(),
            1
        );
        let second = tokio::time::timeout(Duration::from_secs(5), operator_snapshot(&admin, 0))
            .await
            .expect("the request answers at once")
            .expect("the second request");
        assert_eq!(snapshot_index(second).await, first);
        store.hold_pruning(false);
        node.shutdown().await;
    }

    /// An operator's snapshot of a group whose build already runs, with a
    /// permit free for it, builds the group again once that build ended,
    /// which covers less than the request asks for (#529).
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn an_operator_snapshot_builds_again_after_a_running_build() {
        let store = GatedStore::new();
        let (node, admin) = operator_node(&store, 2).await;
        let first = operator_snapshot(&admin, 0);
        store.wait_for_uploads(1, Duration::from_secs(30)).await;
        let stream = (0..10_000)
            .map(|index| BucketStreamId::new("log-pressure", format!("stream-{index}")))
            .find(|stream_id| node.runtime.locate(stream_id).raft_group_id == RaftGroupId(0))
            .expect("a stream of group 0");
        append(&node, &stream, 4).await;
        let applied = node
            .registry
            .get(RaftGroupId(0))
            .expect("registered group")
            .metrics()
            .borrow_watched()
            .last_applied
            .as_ref()
            .map(|log_id| log_id.index)
            .expect("group 0 applied its appends");
        let second = operator_snapshot(&admin, 0);
        tokio::time::sleep(Duration::from_millis(500)).await;
        store.release(2);
        let first = snapshot_index(first.await.expect("the first request"))
            .await
            .expect("the first snapshot");
        let second = snapshot_index(second.await.expect("the second request"))
            .await
            .expect("the second snapshot");
        assert!(first < applied, "{first} {applied}");
        assert!(second >= applied, "{second} {applied}");
        assert_eq!(store.uploads().started, 2);
        node.shutdown().await;
    }
}

mod leadership_balance {
    use std::collections::HashSet;

    use ursula_raft::RaftGroupMetricsSnapshot;

    use crate::bootstrap::leader_counts;
    use crate::bootstrap::plan_leadership_balance;
    use crate::bootstrap::plan_leadership_balance_with_eligible_nodes;
    use crate::bootstrap::prioritized_transfer_targets;

    fn snap(group_id: u32, leader: Option<u64>) -> RaftGroupMetricsSnapshot {
        super::raft_metrics_snapshot(group_id, 1, leader, Some(100), Some(100), None, vec![
            1, 2, 3,
        ])
    }

    #[test]
    fn balanced_cluster_plans_nothing() {
        // 6 groups, 3 voters: each holds 2 — already at fair share.
        let snaps = vec![
            snap(0, Some(1)),
            snap(1, Some(1)),
            snap(2, Some(2)),
            snap(3, Some(2)),
            snap(4, Some(3)),
            snap(5, Some(3)),
        ];
        for me in [1u64, 2, 3] {
            assert!(
                plan_leadership_balance(&snaps, me, 4).is_empty(),
                "node {me} unexpectedly planned a transfer in a balanced cluster",
            );
        }
    }

    #[test]
    fn sole_node_with_everything_sheds_to_fair_share_in_one_tick() {
        // Worst case after a sudden election: one node won every leadership.
        // With max_per_tick=4 (default) we should fully balance in one tick.
        let snaps = vec![
            snap(0, Some(1)),
            snap(1, Some(1)),
            snap(2, Some(1)),
            snap(3, Some(1)),
            snap(4, Some(1)),
            snap(5, Some(1)),
        ];
        let actions = plan_leadership_balance(&snaps, 1, 4);
        // fair = ceil(6/3) = 2. Node 1 has 6, must shed 4. Both other voters
        // should land at 2 each: that's a complete rebalance in a single tick.
        assert_eq!(actions.len(), 4, "actions={actions:?}");
        let mut target_counts = std::collections::HashMap::new();
        for a in &actions {
            *target_counts.entry(a.target).or_insert(0usize) += 1;
        }
        assert_eq!(target_counts.get(&2).copied().unwrap_or(0), 2);
        assert_eq!(target_counts.get(&3).copied().unwrap_or(0), 2);
        // Group ids should be the smallest 4 (deterministic order).
        let mut group_ids: Vec<u32> = actions.iter().map(|a| a.group_id).collect();
        group_ids.sort();
        assert_eq!(group_ids, vec![0, 1, 2, 3]);
    }

    #[test]
    fn max_per_tick_caps_thundering_herd() {
        // Same skew, but tick budget of 1: we shed exactly one and stop.
        let snaps = vec![
            snap(0, Some(1)),
            snap(1, Some(1)),
            snap(2, Some(1)),
            snap(3, Some(1)),
            snap(4, Some(1)),
            snap(5, Some(1)),
        ];
        let actions = plan_leadership_balance(&snaps, 1, 1);
        assert_eq!(actions.len(), 1);
        assert_eq!(actions[0].group_id, 0); // smallest group_id first
    }

    #[test]
    fn only_my_excess_planned_not_peer_excess() {
        // Node 1 at fair share, node 2 over. From node 1's perspective, no
        // action — only node 2's own balancer should shed.
        let snaps = vec![
            snap(0, Some(1)),
            snap(1, Some(1)),
            snap(2, Some(2)),
            snap(3, Some(2)),
            snap(4, Some(2)),
            snap(5, Some(2)),
        ];
        assert!(plan_leadership_balance(&snaps, 1, 4).is_empty());
        let actions_from_2 = plan_leadership_balance(&snaps, 2, 4);
        assert_eq!(actions_from_2.len(), 2);
        // Both transfers must target node 3 (the only under-loaded peer).
        for a in &actions_from_2 {
            assert_eq!(a.target, 3);
        }
    }

    #[test]
    fn picks_least_loaded_target_among_multiple_peers() {
        // Node 1 has 4, node 2 has 1, node 3 has 1, fair = 2. Each of the
        // two transfers should rotate (one to 2, one to 3), not pile up.
        let snaps = vec![
            snap(0, Some(1)),
            snap(1, Some(1)),
            snap(2, Some(1)),
            snap(3, Some(1)),
            snap(4, Some(2)),
            snap(5, Some(3)),
        ];
        let actions = plan_leadership_balance(&snaps, 1, 4);
        let mut target_counts = std::collections::HashMap::new();
        for a in &actions {
            *target_counts.entry(a.target).or_insert(0usize) += 1;
        }
        assert_eq!(actions.len(), 2);
        assert_eq!(target_counts.get(&2).copied().unwrap_or(0), 1);
        assert_eq!(target_counts.get(&3).copied().unwrap_or(0), 1);
    }

    #[test]
    fn group_with_no_eligible_voter_target_is_skipped() {
        // Group 0 has voter set {1} only — nowhere to send. Group 1 normal.
        let mut g0 = snap(0, Some(1));
        g0.voter_ids = vec![1];
        let mut g1 = snap(1, Some(1));
        g1.voter_ids = vec![1, 2, 3];
        let mut g2 = snap(2, Some(1));
        g2.voter_ids = vec![1, 2, 3];
        let snaps = vec![g0, g1, g2];
        // 3 groups / 3 voters → fair = 1. Node 1 has 3, must shed 2.
        let actions = plan_leadership_balance(&snaps, 1, 4);
        // Two transfers, both from groups 1 and 2; group 0 stays put.
        let group_ids: std::collections::BTreeSet<u32> =
            actions.iter().map(|a| a.group_id).collect();
        assert_eq!(group_ids, std::collections::BTreeSet::from([1, 2]));
    }

    #[test]
    fn unbalance_consumer_does_not_evaluate_an_action_for_a_target_we_already_overflowed() {
        // Synthetic: only one peer slot left under fair; second action would
        // pile target above fair, so it should be cut.
        let snaps = vec![
            snap(0, Some(1)),
            snap(1, Some(1)),
            snap(2, Some(1)),
            snap(3, Some(2)),
            snap(4, Some(2)),
            snap(5, Some(3)),
        ];
        // fair = 2. Node 1 has 3, must shed 1. Node 2 is at 2 (fair, no room),
        // node 3 is at 1 (room for 1). Plan should be exactly one transfer to
        // node 3 — not also to node 2.
        let actions = plan_leadership_balance(&snaps, 1, 4);
        assert_eq!(actions.len(), 1);
        assert_eq!(actions[0].target, 3);
    }

    #[test]
    fn campaign_ineligible_peer_is_not_rebalanced_into() {
        // Node 1 has hard-yielded leadership and is not campaign-eligible.
        // With all voters considered eligible, node 2 would see node 1 as
        // under-loaded and push group 0 there. Recomputing fair over only
        // campaign-eligible nodes {2,3} makes node 2 already fair.
        let snaps = vec![
            snap(0, Some(2)),
            snap(1, Some(2)),
            snap(2, Some(2)),
            snap(3, Some(3)),
            snap(4, Some(3)),
            snap(5, Some(3)),
        ];
        let eligible = HashSet::from([2, 3]);
        assert!(plan_leadership_balance_with_eligible_nodes(&snaps, 2, 4, &eligible).is_empty());
    }

    #[test]
    fn rebalances_only_to_campaign_eligible_peers() {
        // If one peer is campaign-ineligible, a deeply overloaded leader
        // should still rebalance across the remaining healthy peer instead of
        // targeting the impaired low-load node.
        let snaps = vec![
            snap(0, Some(2)),
            snap(1, Some(2)),
            snap(2, Some(2)),
            snap(3, Some(2)),
            snap(4, Some(2)),
            snap(5, Some(2)),
        ];
        let eligible = HashSet::from([2, 3]);
        let actions = plan_leadership_balance_with_eligible_nodes(&snaps, 2, 4, &eligible);
        assert_eq!(actions.len(), 3, "actions={actions:?}");
        assert!(actions.iter().all(|action| action.target == 3));
    }

    #[test]
    fn prioritized_transfer_targets_try_least_loaded_peer_first() {
        let snaps = vec![
            snap(0, Some(1)),
            snap(1, Some(2)),
            snap(2, Some(2)),
            snap(3, Some(3)),
        ];
        let counts = leader_counts(&snaps);
        assert_eq!(prioritized_transfer_targets(&snaps[0], 1, &counts), vec![
            3, 2
        ]);
    }
}

mod cluster_egress {
    use std::collections::BTreeMap;
    use std::collections::BTreeSet;
    use std::sync::Arc;
    use std::sync::atomic::AtomicU64;
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    use axum::Router;
    use axum::http::StatusCode;
    use axum::routing::post;
    use ursula_raft::RaftGroupHandleRegistry;
    use ursula_raft::RaftGroupMetricsSnapshot;
    use ursula_shard::RaftGroupId;

    use crate::bootstrap::ClusterEgressProbeScope;
    use crate::bootstrap::PeerParticipation;
    use crate::bootstrap::ShedDrain;
    use crate::bootstrap::ShedHandoff;
    use crate::bootstrap::ShedTargets;
    use crate::bootstrap::cluster_egress_probe_groups;

    /// The egress gate's first plan: every group `node_id` leads, with no
    /// cap, to any voter that may campaign and that `can_take_over` accepts.
    fn plan_egress_shed(
        snaps: &[RaftGroupMetricsSnapshot],
        node_id: u64,
        can_take_over: impl Fn(RaftGroupId, u64) -> bool,
    ) -> crate::bootstrap::ShedPlan {
        ShedDrain::of_groups_led_by(snaps, node_id)
            .tick(
                snaps,
                node_id,
                tokio::time::Instant::now(),
                usize::MAX,
                &[],
                &ShedTargets::for_every_handoff(std::collections::HashSet::from([1, 2, 3, 4])),
                can_take_over,
            )
            .plan
    }

    fn snap(group_id: u32, voters: Vec<u64>) -> RaftGroupMetricsSnapshot {
        snap_with_leader(group_id, 1, Some(1), voters)
    }

    fn snap_with_leader(
        group_id: u32,
        node_id: u64,
        leader: Option<u64>,
        voters: Vec<u64>,
    ) -> RaftGroupMetricsSnapshot {
        super::raft_metrics_snapshot(
            group_id,
            node_id,
            leader,
            Some(100),
            Some(100),
            None,
            voters,
        )
    }

    fn peers() -> Vec<(u64, String)> {
        vec![
            (1, "http://node1".to_owned()),
            (2, "http://node2".to_owned()),
            (3, "http://node3".to_owned()),
            (4, "http://node4".to_owned()),
        ]
    }

    #[test]
    fn per_group_probe_plan_uses_only_that_groups_voters() {
        let per_group_voters = BTreeMap::from([
            (RaftGroupId(0), BTreeSet::from([1, 2, 3])),
            (RaftGroupId(1), BTreeSet::from([2, 3, 4])),
        ]);
        let snaps = vec![snap(0, vec![1, 2, 3]), snap(1, vec![2, 3, 4])];

        let groups = cluster_egress_probe_groups(1, &peers(), &per_group_voters, &snaps);

        assert_eq!(groups.len(), 1);
        assert_eq!(
            groups[0].scope,
            ClusterEgressProbeScope::Group(RaftGroupId(0))
        );
        assert_eq!(groups[0].peer_urls, vec!["http://node2", "http://node3"]);
        assert_eq!(groups[0].needed_peers, 1);
    }

    #[test]
    fn global_probe_plan_is_preserved_without_per_group_voters() {
        let groups = cluster_egress_probe_groups(1, &peers(), &BTreeMap::new(), &[]);

        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].scope, ClusterEgressProbeScope::Global);
        assert_eq!(groups[0].peer_urls, vec![
            "http://node2",
            "http://node3",
            "http://node4"
        ]);
        assert_eq!(groups[0].needed_peers, 2);
    }

    #[test]
    fn egress_shed_skips_recovering_voters_before_assigning_handoffs() {
        let snaps = vec![
            snap_with_leader(0, 3, Some(3), vec![1, 2, 3]),
            snap_with_leader(1, 3, Some(3), vec![1, 3]),
        ];
        let plan = plan_egress_shed(&snaps, 3, |_, target| target != 1);
        assert_eq!(plan.handoffs, vec![ShedHandoff {
            group_id: 0,
            target: 2
        }]);
        assert_eq!(plan.stranded, vec![1]);
    }

    #[test]
    fn egress_shed_spreads_handoffs_across_peer_voters() {
        let snaps = vec![
            snap_with_leader(0, 3, Some(3), vec![1, 2, 3]),
            snap_with_leader(1, 3, Some(3), vec![1, 2, 3]),
            snap_with_leader(2, 3, Some(2), vec![1, 2, 3]),
            snap_with_leader(3, 3, Some(3), vec![1, 2, 3]),
            snap_with_leader(4, 3, Some(3), vec![1, 2, 3]),
            snap_with_leader(5, 3, Some(2), vec![1, 2, 3]),
        ];

        let actions = plan_egress_shed(&snaps, 3, |_, _| true).handoffs;

        assert_eq!(actions, vec![
            ShedHandoff {
                group_id: 0,
                target: 1,
            },
            ShedHandoff {
                group_id: 1,
                target: 1,
            },
            ShedHandoff {
                group_id: 3,
                target: 1,
            },
            ShedHandoff {
                group_id: 4,
                target: 2,
            },
        ]);
    }

    async fn serve_counting_probe_peer(
        counter: Arc<AtomicU64>,
    ) -> (String, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind probe peer");
        let addr = listener.local_addr().expect("probe peer addr");
        let app = Router::new().route(
            crate::CLUSTER_PROBE_PATH,
            post(move |_body: axum::body::Bytes| {
                let counter = counter.clone();
                async move {
                    counter.fetch_add(1, Ordering::SeqCst);
                    StatusCode::NO_CONTENT
                }
            }),
        );
        let handle = tokio::spawn(async move {
            axum::serve(listener, app).await.expect("serve probe peer");
        });
        (format!("http://{addr}"), handle)
    }

    #[tokio::test]
    async fn spawned_gate_uses_per_group_voters_for_probe_targets() {
        let node2_probes = Arc::new(AtomicU64::new(0));
        let node3_probes = Arc::new(AtomicU64::new(0));
        let node4_probes = Arc::new(AtomicU64::new(0));
        let (node2_url, node2_task) = serve_counting_probe_peer(node2_probes.clone()).await;
        let (node3_url, node3_task) = serve_counting_probe_peer(node3_probes.clone()).await;
        let (node4_url, node4_task) = serve_counting_probe_peer(node4_probes.clone()).await;
        let peers = vec![
            (1, "http://node1".to_owned()),
            (2, node2_url),
            (3, node3_url),
            (4, node4_url),
        ];
        let per_group_voters = BTreeMap::from([
            (RaftGroupId(0), BTreeSet::from([1, 2, 3])),
            (RaftGroupId(1), BTreeSet::from([2, 3, 4])),
        ]);
        let registry = RaftGroupHandleRegistry::default();

        crate::bootstrap::spawn_egress_gate(
            &registry,
            &PeerParticipation::new(1, &peers, reqwest::Client::new()),
            &peers,
            per_group_voters,
            &ursula_config::UrsulaConfig::default()
                .governance
                .cluster_probe,
        );
        tokio::time::sleep(Duration::from_millis(2_500)).await;

        assert!(node2_probes.load(Ordering::SeqCst) > 0);
        assert!(node3_probes.load(Ordering::SeqCst) > 0);
        assert_eq!(
            node4_probes.load(Ordering::SeqCst),
            0,
            "node 4 is not a voter for any group hosted by node 1"
        );

        node2_task.abort();
        node3_task.abort();
        node4_task.abort();
    }
}

/// Leadership sheds on a real three-node cluster: twelve groups, each led by
/// node 1 when the test starts.
mod leadership_shed {
    use std::collections::BTreeMap;
    use std::collections::BTreeSet;
    use std::collections::HashSet;
    use std::sync::Arc;
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    use axum::Json;
    use axum::Router;
    use axum::body::Bytes;
    use axum::routing::get;
    use tokio::sync::watch;
    use tokio::time::Instant;
    use ursula_proto::admin::LeadershipShedStatus;
    use ursula_raft::LeadershipShedReason;
    use ursula_raft::RaftGroupHandleRegistry;
    use ursula_raft::snapshot_cadence::SnapshotCadence;
    use ursula_runtime::AppendRequest;
    use ursula_runtime::ColdStore;
    use ursula_runtime::ColdStoreHandle;
    use ursula_runtime::CreateStreamRequest;
    use ursula_runtime::SnapshotKey;
    use ursula_runtime::SnapshotLocation;
    use ursula_runtime::SnapshotStore;
    use ursula_runtime::SnapshotStoreFuture;
    use ursula_shard::BucketStreamId;
    use ursula_shard::RaftGroupId;

    use super::StaticGrpcTestNode;
    use super::StaticGrpcTestNodeStorage;
    use super::spawn_static_grpc_test_node;
    use crate::bootstrap::LeadershipShed;
    use crate::bootstrap::LogWriteGate;
    use crate::bootstrap::PeerParticipation;
    use crate::bootstrap::ShedHandoff;
    use crate::bootstrap::ShedPace;
    use crate::bootstrap::ShedTargets;
    use crate::bootstrap::StoreGateInputs;

    const GROUPS: u32 = 12;
    const PER_TICK: usize = 4;

    struct Cluster {
        nodes: Vec<StaticGrpcTestNode>,
        peers: Vec<(u64, String)>,
    }

    impl Cluster {
        /// Three nodes with [`GROUPS`] groups, every leadership on node 1.
        async fn led_by_node_1() -> Self {
            Self::led_by_node_1_with(None).await
        }

        /// [`Self::led_by_node_1`], every node with `cold_store` as its cold
        /// store.
        async fn led_by_node_1_with(cold_store: Option<ColdStoreHandle>) -> Self {
            let mut listeners = Vec::new();
            let mut peers = Vec::new();
            for node_id in 1..=3_u64 {
                let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                    .await
                    .expect("bind listener");
                let address = listener.local_addr().expect("local addr");
                peers.push((node_id, format!("http://{address}")));
                listeners.push(listener);
            }
            let mut nodes = Vec::new();
            for (node_id, listener) in (1..=3_u64).zip(listeners) {
                nodes.push(
                    spawn_static_grpc_test_node(
                        node_id,
                        listener,
                        peers.clone(),
                        peers.clone(),
                        node_id == 1,
                        usize::try_from(GROUPS).unwrap(),
                        StaticGrpcTestNodeStorage {
                            cold_store: cold_store.clone(),
                            ..StaticGrpcTestNodeStorage::default()
                        },
                    )
                    .await,
                );
            }
            // The initializer warms last, after its peers can answer.
            for node in nodes.iter().rev() {
                tokio::time::timeout(Duration::from_secs(30), node.runtime.warm_all_groups())
                    .await
                    .expect("warm the node's groups in time")
                    .expect("warm the node's groups");
            }
            let cluster = Self { nodes, peers };
            for group in 0..GROUPS {
                cluster.move_leadership_to_node_1(group).await;
            }
            // Every node's recovery gates open, so each may campaign and take
            // over leaderships.
            cluster.wait_until_may_campaign([1, 2, 3]).await;
            cluster
        }

        /// Waits until node 1 sees exactly `nodes` as the nodes that may
        /// campaign, and returns them. One answer can come late on a loaded
        /// host, so a single query is not enough.
        async fn wait_until_may_campaign(
            &self,
            nodes: impl IntoIterator<Item = u64>,
        ) -> HashSet<u64> {
            let expected: HashSet<u64> = nodes.into_iter().collect();
            let deadline = deadline(Duration::from_secs(60));
            let participation = self.participation();
            loop {
                let eligible = participation
                    .campaign_eligible_nodes(&self.node_1().registry)
                    .await;
                if eligible == expected {
                    return eligible;
                }
                assert!(
                    Instant::now() < deadline,
                    "only nodes {eligible:?} may campaign, not {expected:?}"
                );
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }

        /// Each group's term, as node 1 sees it.
        fn terms(&self) -> BTreeMap<u32, u64> {
            self.node_1()
                .registry
                .metrics_snapshot()
                .into_iter()
                .map(|snap| (snap.raft_group_id, snap.current_term))
                .collect()
        }

        /// Checks that node 1 hands off nothing for 8 s while no peer
        /// qualifies. A handoff to a peer that may campaign moves the
        /// leadership. A handoff to a peer that may not is refused without
        /// a word, and Raft abandons it after its 6 s transfer deadline:
        /// node 1 then leads the group in a newer term.
        async fn assert_node_1_hands_nothing_off(&self) {
            let terms = self.terms();
            tokio::time::sleep(Duration::from_secs(8)).await;
            assert_eq!(
                self.led_by(1),
                usize::try_from(GROUPS).unwrap(),
                "a leadership left node 1: leaders {:?}",
                self.leaders()
            );
            assert_eq!(
                self.terms(),
                terms,
                "a handoff went to a peer that may not campaign"
            );
        }

        /// Adds the term of each group node 1 leads to `led_terms`, every
        /// 50 ms until `until` finishes.
        async fn record_node_1_leader_terms(
            &self,
            led_terms: &mut BTreeMap<u32, BTreeSet<u64>>,
            until: impl std::future::Future<Output = ()>,
        ) {
            let mut samples = tokio::time::interval(Duration::from_millis(50));
            tokio::pin!(until);
            loop {
                tokio::select! {
                    () = &mut until => return,
                    _ = samples.tick() => {
                        for snap in self.node_1().registry.metrics_snapshot() {
                            if snap.current_leader == Some(1) {
                                led_terms
                                    .entry(snap.raft_group_id)
                                    .or_default()
                                    .insert(snap.current_term);
                            }
                        }
                    }
                }
            }
        }

        /// Waits until node 1 is shedding for `reason`.
        async fn wait_until_node_1_sheds(&self, reason: LeadershipShedReason) {
            let deadline = deadline(Duration::from_secs(30));
            while !self
                .node_1()
                .registry
                .leadership_shed_state()
                .contains(reason.into())
            {
                assert!(
                    Instant::now() < deadline,
                    "node 1 does not shed for {reason}"
                );
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }

        fn node(&self, node_id: u64) -> &StaticGrpcTestNode {
            &self.nodes[usize::try_from(node_id).unwrap().checked_sub(1).unwrap()]
        }

        fn node_1(&self) -> &StaticGrpcTestNode {
            self.node(1)
        }

        fn participation(&self) -> PeerParticipation {
            PeerParticipation::new(1, &self.peers, peer_client())
        }

        /// Node 1's participation when its leadership-shed requests to node
        /// 2 never get an answer, while Raft still reaches node 2. Each
        /// request gives up after 500 ms. The requests go unanswered as long
        /// as the returned listener lives.
        async fn participation_with_node_2_silent(
            &self,
        ) -> (PeerParticipation, tokio::net::TcpListener) {
            let (silent_2, listener) = silent_url().await;
            let participation = PeerParticipation::new(
                1,
                &[self.peers[0].clone(), (2, silent_2), self.peers[2].clone()],
                reqwest::Client::builder()
                    .timeout(Duration::from_millis(500))
                    .build()
                    .expect("peer client"),
            );
            (participation, listener)
        }

        /// Each group's leader, as node 1 sees it.
        fn leaders(&self) -> BTreeMap<u32, Option<u64>> {
            self.node_1()
                .registry
                .metrics_snapshot()
                .into_iter()
                .map(|snap| (snap.raft_group_id, snap.current_leader))
                .collect()
        }

        fn led_by(&self, node_id: u64) -> usize {
            led_by(&self.node_1().registry, node_id)
        }

        async fn move_leadership_to_node_1(&self, group: u32) {
            let deadline = deadline(Duration::from_secs(60));
            loop {
                let leader = self.leaders().get(&group).copied().flatten();
                match leader {
                    Some(1) => return,
                    Some(other) => {
                        if let Err(err) = self
                            .node(other)
                            .registry
                            .transfer_leader(RaftGroupId(group), 1)
                            .await
                        {
                            tracing::debug!(group, %err, "transfer to node 1 refused");
                        }
                    }
                    None => {}
                }
                assert!(
                    Instant::now() < deadline,
                    "node 1 never leads group {group}"
                );
                if let Err(err) = self
                    .wait_for_leader(group, Duration::from_secs(1), |leader| leader == Some(1))
                    .await
                {
                    tracing::debug!(group, %err, "node 1 does not lead yet");
                }
            }
        }

        /// Waits up to `timeout` for node 1 to see `group`'s leader pass
        /// `accept`.
        async fn wait_for_leader(
            &self,
            group: u32,
            timeout: Duration,
            accept: impl Fn(Option<u64>) -> bool + Send + 'static,
        ) -> Result<(), openraft::metrics::WaitError> {
            self.node_1()
                .registry
                .get(RaftGroupId(group))
                .expect("registered group")
                .wait(Some(timeout))
                .metrics(
                    move |metrics| accept(metrics.current_leader),
                    "the group's leader settles",
                )
                .await
                .map(drop)
        }

        /// Waits until another node leads every group.
        async fn wait_until_node_1_leads_nothing(&self, timeout: Duration) {
            let deadline = deadline(timeout);
            for group in 0..GROUPS {
                let left = deadline.saturating_duration_since(Instant::now());
                self.wait_for_leader(group, left, |leader| {
                    leader.is_some_and(|leader| leader != 1)
                })
                .await
                .unwrap_or_else(|err| {
                    panic!(
                        "group {group} stays on node 1: {err}; leaders {:?}",
                        self.leaders()
                    )
                });
            }
        }

        async fn shutdown(self) {
            for node in self.nodes {
                node.shutdown().await;
            }
        }
    }

    fn deadline(timeout: Duration) -> Instant {
        Instant::now().checked_add(timeout).expect("deadline")
    }

    fn peer_client() -> reqwest::Client {
        reqwest::Client::builder()
            .timeout(Duration::from_secs(2))
            .build()
            .expect("peer client")
    }

    fn led_by(registry: &RaftGroupHandleRegistry, node_id: u64) -> usize {
        registry
            .metrics_snapshot()
            .iter()
            .filter(|snap| snap.current_leader == Some(node_id))
            .count()
    }

    /// A snapshot store whose every health probe fails. Each probe records
    /// how many groups node 1 led and whether it was shedding for the store.
    #[derive(Debug)]
    struct FailingProbeStore {
        registry: RaftGroupHandleRegistry,
        probes: watch::Sender<Vec<(usize, bool)>>,
    }

    impl FailingProbeStore {
        fn new(registry: &RaftGroupHandleRegistry) -> Self {
            Self {
                registry: registry.clone(),
                probes: watch::Sender::new(Vec::new()),
            }
        }

        fn probes(&self) -> Vec<(usize, bool)> {
            self.probes.borrow().clone()
        }

        /// Waits until the store has been probed more than `count` times.
        async fn wait_for_more_probes_than(&self, count: usize) {
            tokio::time::timeout(
                Duration::from_secs(30),
                self.probes
                    .subscribe()
                    .wait_for(|probes| probes.len() > count),
            )
            .await
            .expect("the gate probes the store again")
            .expect("the store keeps its probes");
        }
    }

    impl SnapshotStore for FailingProbeStore {
        fn upload<'a>(
            &'a self,
            _key: SnapshotKey,
            _bytes: Bytes,
        ) -> SnapshotStoreFuture<'a, SnapshotLocation> {
            Box::pin(async {
                Err(ursula_runtime::SnapshotStoreError::backend(
                    ursula_runtime::SnapshotStoreOperation::Put,
                    "test",
                    opendal::Error::new(opendal::ErrorKind::Unexpected, "probe-only store"),
                ))
            })
        }

        fn download<'a>(
            &'a self,
            _location: &'a SnapshotLocation,
        ) -> SnapshotStoreFuture<'a, Vec<u8>> {
            Box::pin(async {
                Err(ursula_runtime::SnapshotStoreError::backend(
                    ursula_runtime::SnapshotStoreOperation::Get,
                    "test",
                    opendal::Error::new(opendal::ErrorKind::Unexpected, "probe-only store"),
                ))
            })
        }

        fn delete<'a>(&'a self, _location: &'a SnapshotLocation) -> SnapshotStoreFuture<'a, ()> {
            Box::pin(async { Ok(()) })
        }

        fn health_check(&self) -> SnapshotStoreFuture<'_, ()> {
            let shedding = self
                .registry
                .leadership_shed_state()
                .contains(LeadershipShedReason::SnapshotDriverS3.into());
            let led = led_by(&self.registry, 1);
            self.probes
                .send_modify(|probes| probes.push((led, shedding)));
            Box::pin(async {
                Err(ursula_runtime::SnapshotStoreError::backend(
                    ursula_runtime::SnapshotStoreOperation::Head,
                    "test",
                    opendal::Error::new(opendal::ErrorKind::Unexpected, "the store is down"),
                ))
            })
        }

        ursula_runtime::inline_owned_snapshot_methods!();
    }

    /// Through the snapshot driver, the production entry point, with the
    /// default `[storage.cold.s3]` settings and 1 s ticks.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn the_store_gate_sheds_after_three_failed_probes_and_drains_four_per_tick() {
        let cluster = Cluster::led_by_node_1().await;
        let node_1 = cluster.node_1();
        let store = Arc::new(FailingProbeStore::new(&node_1.registry));
        // Node 3 drains, so it may not campaign. Node 2 may campaign, but
        // node 1's questions to it get no answer, so the store gate, which
        // asks over a healthy path, must not hand it a leadership.
        cluster
            .node(3)
            .registry
            .mark_leadership_shed(LeadershipShedReason::MaintenanceDrain);
        let (participation, _silent_2) = cluster.participation_with_node_2_silent().await;
        crate::bootstrap::spawn_snapshot_driver(
            &node_1.runtime,
            &node_1.registry,
            Some((store.clone(), StoreGateInputs {
                s3: None,
                cold_flush_interval: Duration::from_secs(1),
                participation: &participation,
            })),
            LogWriteGate::RefuseAtHardLimit,
            1_000,
            SnapshotCadence::new(1 << 30, usize::try_from(GROUPS).unwrap(), 100_000),
            None,
            16,
        );
        cluster
            .wait_until_node_1_sheds(LeadershipShedReason::SnapshotDriverS3)
            .await;
        cluster.assert_node_1_hands_nothing_off().await;
        // Once node 3 may campaign, every leadership goes to it.
        cluster
            .node(3)
            .registry
            .clear_leadership_shed(LeadershipShedReason::MaintenanceDrain);
        let cleared = store.probes().len();
        cluster
            .wait_until_node_1_leads_nothing(Duration::from_secs(60))
            .await;
        // The next probe follows the step that handed off the last
        // leadership.
        store.wait_for_more_probes_than(store.probes().len()).await;

        let probes = store.probes();
        let all = usize::try_from(GROUPS).unwrap();
        assert!(probes.len() > cleared, "{probes:?}");
        assert_eq!(
            probes[..3],
            [(all, false); 3],
            "nothing moves before the third failed probe"
        );
        for (index, (_, shedding)) in probes.iter().enumerate().skip(3) {
            assert!(shedding, "probe {index}: {probes:?}");
        }
        // Each probe from `cleared` on follows the step of the tick that was
        // under way when node 3 cleared its drain, and one step per tick
        // after it. Each step hands off at most four leaderships.
        for (index, (led, _)) in probes.iter().enumerate().skip(cleared) {
            let steps = index.checked_add(1).unwrap().checked_sub(cleared).unwrap();
            assert!(
                all.checked_sub(*led).unwrap() <= PER_TICK.saturating_mul(steps),
                "more than {PER_TICK} leaderships left per tick since probe {cleared}: {probes:?}"
            );
        }
        assert_eq!(
            cluster.led_by(3),
            all,
            "node 3 is the only peer that answered that it may campaign"
        );
        cluster.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn each_step_hands_off_at_most_four_leaderships_to_both_peers() {
        let cluster = Cluster::led_by_node_1().await;
        let node_1 = cluster.node_1();
        let mut shed = LeadershipShed::new(
            &node_1.registry,
            LeadershipShedReason::SnapshotDriverS3,
            1,
            ShedPace::Gradual,
        );
        shed.begin();
        let mut submitted_per_step = Vec::new();
        let mut targets = BTreeMap::<u64, usize>::new();
        let deadline = deadline(Duration::from_secs(60));
        while shed.is_draining() {
            assert!(Instant::now() < deadline, "the drain does not finish");
            // Node 1 sheds, and both peers may campaign.
            let may_campaign = cluster.wait_until_may_campaign([2, 3]).await;
            let step = shed
                .step(&ShedTargets::for_every_handoff(may_campaign), &[])
                .await;
            assert!(step.submitted.len() <= PER_TICK, "{step:?}");
            for handoff in &step.submitted {
                let count = targets.entry(handoff.target).or_default();
                *count = count.saturating_add(1);
                let target = handoff.target;
                cluster
                    .wait_for_leader(handoff.group_id, Duration::from_secs(30), move |leader| {
                        leader == Some(target)
                    })
                    .await
                    .unwrap_or_else(|err| panic!("{handoff:?} does not land: {err}"));
            }
            submitted_per_step.push(step.submitted.len());
        }
        assert_eq!(
            submitted_per_step.first(),
            Some(&PER_TICK),
            "the first step uses its whole cap: {submitted_per_step:?}"
        );
        assert_eq!(
            submitted_per_step.iter().sum::<usize>(),
            usize::try_from(GROUPS).unwrap()
        );
        assert_eq!(cluster.led_by(1), 0);
        assert!(
            targets.get(&2) >= Some(&4) && targets.get(&3) >= Some(&4),
            "{targets:?}"
        );
        drop(shed);
        cluster.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_handoff_that_does_not_move_is_handed_off_again() {
        let cluster = Cluster::led_by_node_1().await;
        let node_1 = cluster.node_1();
        let participation = cluster.participation();
        // Both peers drain: they may not campaign, so each refuses a
        // transfer without telling node 1.
        for node_id in [2, 3] {
            cluster
                .node(node_id)
                .registry
                .mark_leadership_shed(LeadershipShedReason::MaintenanceDrain);
        }
        let mut shed = LeadershipShed::new(
            &node_1.registry,
            LeadershipShedReason::SnapshotDriverS3,
            1,
            ShedPace::Gradual,
        );
        shed.begin();

        // With no peer that may campaign, every leadership stays pending.
        let may_campaign = participation
            .campaign_eligible_nodes(&node_1.registry)
            .await;
        assert!(may_campaign.is_empty(), "{may_campaign:?}");
        let stranded = shed
            .step(&ShedTargets::for_every_handoff(may_campaign), &[])
            .await;
        assert!(stranded.submitted.is_empty(), "{stranded:?}");
        assert_eq!(stranded.stranded.len(), usize::try_from(GROUPS).unwrap());

        // A stale view that still lets both peers campaign: four handoffs
        // go out and none moves the leadership.
        let terms: BTreeMap<u32, u64> = node_1
            .registry
            .metrics_snapshot()
            .into_iter()
            .map(|snap| (snap.raft_group_id, snap.current_term))
            .collect();
        let submitting = Instant::now();
        let first = shed
            .step(&ShedTargets::for_every_handoff(HashSet::from([2, 3])), &[])
            .await;
        assert_eq!(first.submitted.len(), PER_TICK, "{first:?}");
        let settle = node_1
            .registry
            .get(RaftGroupId(first.submitted[0].group_id))
            .expect("registered group")
            .handoff_settle_time();

        // Raft abandons each transfer at its deadline, and node 1, which its
        // draining peers still vote for, leads the group again. The drain
        // calls a handoff stalled only once its settle time has passed,
        // so it does not send a second transfer while Raft may still be
        // completing the first.
        let mut stalled: Vec<ShedHandoff> = Vec::new();
        let stall_deadline = deadline(settle.saturating_add(Duration::from_secs(30)));
        let mut steps = tokio::time::interval(Duration::from_millis(250));
        while stalled.len() < first.submitted.len() {
            assert!(
                Instant::now() < stall_deadline,
                "the handoffs never stall: {stalled:?}"
            );
            steps.tick().await;
            let step = shed.step(&ShedTargets::default(), &[]).await;
            let stepped = Instant::now();
            assert!(step.submitted.is_empty(), "{step:?}");
            if !step.stalled.is_empty() {
                let since = stepped.saturating_duration_since(submitting);
                assert!(
                    since >= settle,
                    "{:?} stalled {since:?} after submission, before the {settle:?} settle time",
                    step.stalled
                );
            }
            stalled.extend(step.stalled);
        }
        for handoff in &first.submitted {
            let raft = node_1
                .registry
                .get(RaftGroupId(handoff.group_id))
                .expect("registered group");
            let term = terms[&handoff.group_id];
            raft.wait(Some(Duration::from_secs(30)))
                .metrics(
                    move |metrics| metrics.current_leader == Some(1) && metrics.current_term > term,
                    "node 1 leads again after the abandoned transfer",
                )
                .await
                .unwrap_or_else(|err| panic!("{handoff:?}: {err}"));
        }

        // Node 2 may campaign again. The drain hands the stalled groups off
        // again, to node 2, alongside the ones it never submitted.
        cluster
            .node(2)
            .registry
            .clear_leadership_shed(LeadershipShedReason::MaintenanceDrain);
        let mut resubmitted: Vec<ShedHandoff> = Vec::new();
        let first_groups: Vec<u32> = first
            .submitted
            .iter()
            .map(|handoff| handoff.group_id)
            .collect();
        let deadline = deadline(Duration::from_secs(60));
        while shed.is_draining() {
            assert!(
                Instant::now() < deadline,
                "the drain does not finish: leaders {:?}",
                cluster.leaders()
            );
            steps.tick().await;
            let may_campaign = participation
                .campaign_eligible_nodes(&node_1.registry)
                .await;
            let step = shed
                .step(&ShedTargets::for_every_handoff(may_campaign), &[])
                .await;
            assert!(step.submitted.len() <= PER_TICK, "{step:?}");
            stalled.extend(step.stalled);
            resubmitted.extend(
                step.submitted
                    .iter()
                    .filter(|handoff| first_groups.contains(&handoff.group_id)),
            );
        }
        stalled.sort_by_key(|handoff| handoff.group_id);
        assert_eq!(
            stalled, first.submitted,
            "every refused handoff stalls once"
        );
        let mut again: Vec<u32> = resubmitted.iter().map(|handoff| handoff.group_id).collect();
        again.sort_unstable();
        again.dedup();
        assert_eq!(
            again, first_groups,
            "every stalled group is handed off again"
        );
        assert!(resubmitted.iter().all(|handoff| handoff.target == 2));
        cluster
            .wait_until_node_1_leads_nothing(Duration::from_secs(30))
            .await;
        assert_eq!(cluster.led_by(2), usize::try_from(GROUPS).unwrap());
        drop(shed);
        cluster.shutdown().await;
    }

    /// An address that accepts connections and never answers, as long as
    /// the returned listener lives.
    async fn silent_url() -> (String, tokio::net::TcpListener) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind listener");
        let address = listener.local_addr().expect("local addr");
        (format!("http://{address}"), listener)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn the_egress_gate_hands_every_leadership_off_on_its_shed_tick() {
        let cluster = Cluster::led_by_node_1().await;
        let node_1 = cluster.node_1();
        // Node 1's egress fails: neither its probes nor its leadership-shed
        // requests to nodes 2 and 3 get an answer, as over an impaired path,
        // while Raft still reaches both.
        let (url_2, _silent_2) = silent_url().await;
        let (url_3, _silent_3) = silent_url().await;
        let peers = vec![cluster.peers[0].clone(), (2, url_2), (3, url_3)];
        let participation = PeerParticipation::new(
            1,
            &peers,
            reqwest::Client::builder()
                .timeout(Duration::from_millis(500))
                .build()
                .expect("peer client"),
        );
        // One bad tick sheds, and the next tick comes 5 s later.
        crate::bootstrap::spawn_egress_gate(
            &node_1.registry,
            &participation,
            &peers,
            BTreeMap::new(),
            &ursula_config::ClusterProbeConfig {
                interval: ursula_config::HumanDuration::sec(5),
                unhealthy_ticks: 1,
                ..ursula_config::ClusterProbeConfig::default()
            },
        );
        cluster
            .wait_until_node_1_sheds(LeadershipShedReason::ClusterEgress)
            .await;
        // Every leadership leaves before the next tick, to peers that did
        // not answer whether they may campaign. A gate that handed off a few
        // per tick would keep some on node 1 for another 5 s.
        cluster
            .wait_until_node_1_leads_nothing(Duration::from_secs(4))
            .await;
        let (to_2, to_3) = (cluster.led_by(2), cluster.led_by(3));
        assert!(to_2 >= 4 && to_3 >= 4, "node 2 leads {to_2}, node 3 {to_3}");
        cluster.shutdown().await;
    }

    /// Answers node 1's leadership-shed requests for one peer, and serves
    /// nothing else, so the egress gate's probe to it gets 404 and fails.
    /// It answers whether the peer may campaign as the test sets it, and
    /// leaves the requests that `dropped` picks by number (the first is 1)
    /// without an answer, as a lossy path would.
    struct ShedStateStub {
        url: String,
        may_campaign: Arc<AtomicBool>,
        requests: watch::Receiver<u64>,
        task: tokio::task::JoinHandle<()>,
    }

    impl ShedStateStub {
        async fn serve(dropped: fn(u64) -> bool) -> Self {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .expect("bind stub");
            let address = listener.local_addr().expect("stub address");
            let may_campaign = Arc::new(AtomicBool::new(false));
            let (count, requests) = watch::channel(0_u64);
            let count = Arc::new(count);
            let answer = may_campaign.clone();
            let app = Router::new().route(
                crate::LEADERSHIP_SHED_PATH,
                get(move || {
                    let answer = answer.clone();
                    let count = count.clone();
                    async move {
                        let mut number = 0;
                        count.send_modify(|count| {
                            *count = count.saturating_add(1);
                            number = *count;
                        });
                        if dropped(number) {
                            // The client gives up at its timeout.
                            std::future::pending::<()>().await;
                        }
                        let should_campaign = answer.load(Ordering::SeqCst);
                        Json(LeadershipShedStatus {
                            cluster_id: crate::tests::static_test_cluster_id(),
                            bits: 0,
                            state: String::new(),
                            should_accept_transfer: should_campaign,
                            should_campaign,
                            recovery_barriers_ready: true,
                            should_shed_current_leaders: false,
                        })
                    }
                }),
            );
            let task = tokio::spawn(async move {
                axum::serve(listener, app).await.expect("serve stub");
            });
            Self {
                url: format!("http://{address}"),
                may_campaign,
                requests,
                task,
            }
        }

        /// Waits until the stub has received `count` requests.
        async fn wait_for_requests(&self, count: u64) {
            self.requests
                .clone()
                .wait_for(|received| *received >= count)
                .await
                .expect("the stub runs");
        }
    }

    impl Drop for ShedStateStub {
        fn drop(&mut self) {
            self.task.abort();
        }
    }

    /// Every node sheds for cluster egress at once, as in a cluster-wide
    /// grey failure, so each peer refuses a transfer without a word. Node
    /// 1's questions to its peers travel the impaired path: they are
    /// answered "may not campaign", and now and then not at all. A peer
    /// that did not answer gets a leadership's first handoff only, so each
    /// leadership stalls at most once while no peer may campaign, and the
    /// gate hands the stalled ones off again on a later tick, once a peer
    /// answers that it may campaign.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_cluster_wide_egress_shed_hands_each_leadership_off_once_until_a_peer_may_campaign() {
        let cluster = Cluster::led_by_node_1().await;
        let node_1 = cluster.node_1();
        for node_id in [2, 3] {
            cluster
                .node(node_id)
                .registry
                .mark_leadership_shed(LeadershipShedReason::ClusterEgress);
        }
        // The gate asks both stubs once per tick while it drains, so a
        // stub's request number is the tick's. On the third tick of every
        // ten only node 3 answers (no), and on the fifth neither answers.
        let stub_2 = ShedStateStub::serve(|request| matches!(request % 10, 3 | 5)).await;
        let stub_3 = ShedStateStub::serve(|request| request % 10 == 5).await;
        let peers = vec![
            cluster.peers[0].clone(),
            (2, stub_2.url.clone()),
            (3, stub_3.url.clone()),
        ];
        let participation = PeerParticipation::new(
            1,
            &peers,
            reqwest::Client::builder()
                .timeout(Duration::from_millis(500))
                .build()
                .expect("peer client"),
        );
        let settle = node_1
            .registry
            .get(RaftGroupId(0))
            .expect("registered group")
            .handoff_settle_time();
        // The terms in which node 1 led each group. Each handoff that does
        // not move a leadership adds one: Raft abandons the transfer, and
        // node 1 steps down and wins the group back in a newer term.
        let mut led_terms: BTreeMap<u32, BTreeSet<u64>> = cluster
            .terms()
            .into_iter()
            .map(|(group, term)| (group, BTreeSet::from([term])))
            .collect();
        // The probes fail, since the stubs serve no probe route. One bad
        // tick sheds, and the gate ticks every second.
        crate::bootstrap::spawn_egress_gate(
            &node_1.registry,
            &participation,
            &peers,
            BTreeMap::new(),
            &ursula_config::ClusterProbeConfig {
                interval: ursula_config::HumanDuration::sec(1),
                unhealthy_ticks: 1,
                ..ursula_config::ClusterProbeConfig::default()
            },
        );
        cluster
            .wait_until_node_1_sheds(LeadershipShedReason::ClusterEgress)
            .await;
        // By the fifteenth tick the leaderships handed to a peer that did
        // not answer on the fifth have stalled, and that peer has gone
        // unanswered twice more. A handoff on the fifteenth tick would
        // stall within the settle time.
        cluster
            .record_node_1_leader_terms(&mut led_terms, async {
                tokio::time::timeout(Duration::from_secs(60), async {
                    stub_2.wait_for_requests(15).await;
                    stub_3.wait_for_requests(15).await;
                })
                .await
                .expect(
                    "the gate asks its peers on every tick while it has leaderships to hand off",
                );
                tokio::time::sleep(settle.saturating_add(Duration::from_secs(1))).await;
            })
            .await;
        for (group, terms) in &led_terms {
            assert!(
                terms.len() <= 2,
                "group {group} was handed off more than once while no peer may campaign: node 1 \
                 led it in terms {terms:?}"
            );
        }
        assert!(
            led_terms.values().any(|terms| terms.len() == 2),
            "no leadership went to a peer that did not answer: {led_terms:?}"
        );

        // Node 3 may campaign again and says so. The gate hands the stalled
        // leaderships off again, all to node 3, on a later tick.
        cluster
            .node(3)
            .registry
            .clear_leadership_shed(LeadershipShedReason::ClusterEgress);
        stub_3.may_campaign.store(true, Ordering::SeqCst);
        cluster
            .wait_until_node_1_leads_nothing(Duration::from_secs(30))
            .await;
        assert_eq!(
            cluster.led_by(3),
            usize::try_from(GROUPS).unwrap(),
            "node 3 is the only peer that may campaign"
        );
        cluster.shutdown().await;
    }

    /// A stream that `group` holds.
    fn stream_in_group(node: &StaticGrpcTestNode, group: u32) -> BucketStreamId {
        (0..10_000)
            .map(|index| BucketStreamId::new("benchcmp", format!("shed-{index}")))
            .find(|stream| node.runtime.locate(stream).raft_group_id == RaftGroupId(group))
            .expect("a stream in the group")
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn the_cold_health_gate_drains_the_hottest_group_first() {
        const HOT_GROUP: u32 = 7;
        let cold_store: ColdStoreHandle = Arc::new(ColdStore::memory().expect("memory cold store"));
        let cluster = Cluster::led_by_node_1_with(Some(cold_store)).await;
        let node_1 = cluster.node_1();
        // No flush worker runs, so the bytes stay hot: group 7 holds more
        // unflushed bytes than any other group, and more than the high
        // watermark.
        let stream = stream_in_group(node_1, HOT_GROUP);
        node_1
            .runtime
            .create_stream(CreateStreamRequest::new(
                stream.clone(),
                "application/octet-stream",
            ))
            .await
            .expect("create the stream");
        node_1
            .runtime
            .append(AppendRequest::from_bytes(stream, vec![7; 8 * 1024]))
            .await
            .expect("append");
        // Node 3 drains, so it may not campaign. Node 2 may campaign, but
        // node 1's questions to it get no answer, so the cold-health gate,
        // which asks over a healthy path, must not hand it a leadership.
        cluster
            .node(3)
            .registry
            .mark_leadership_shed(LeadershipShedReason::MaintenanceDrain);
        let (participation, _silent_2) = cluster.participation_with_node_2_silent().await;
        // One tick over the high watermark sheds, and the gate ticks every
        // 4 s.
        crate::bootstrap::spawn_cold_health_gate(
            &node_1.runtime,
            &node_1.registry,
            &participation,
            &ursula_config::ColdHealthConfig {
                interval: ursula_config::HumanDuration::sec(4),
                unhealthy_ticks: 1,
                hot_size_high: ursula_config::HumanSize::kib(4),
                hot_size_low: ursula_config::HumanSize::kib(2),
                ..ursula_config::ColdHealthConfig::default()
            },
        );
        cluster
            .wait_until_node_1_sheds(LeadershipShedReason::ColdHealth)
            .await;
        cluster.assert_node_1_hands_nothing_off().await;
        // Once node 3 may campaign, the next step hands it four leaderships,
        // the hottest group among them, and node 2 none. A gate that also
        // tried a peer that did not answer would hand the hottest group to
        // node 2, the lower id of two peers that lead nothing.
        cluster
            .node(3)
            .registry
            .clear_leadership_shed(LeadershipShedReason::MaintenanceDrain);
        let deadline = deadline(Duration::from_secs(10));
        while cluster.led_by(3) < PER_TICK {
            assert!(
                Instant::now() < deadline,
                "the first step does not land on node 3: leaders {:?}",
                cluster.leaders()
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let leaders = cluster.leaders();
        assert_eq!(leaders[&HOT_GROUP], Some(3), "{leaders:?}");
        assert_eq!(cluster.led_by(3), PER_TICK, "{leaders:?}");
        assert_eq!(
            cluster.led_by(2),
            0,
            "node 2 did not answer whether it may campaign: {leaders:?}"
        );
        assert_eq!(
            cluster.led_by(1),
            usize::try_from(GROUPS)
                .unwrap()
                .checked_sub(PER_TICK)
                .unwrap(),
            "{leaders:?}"
        );
        // A cold-health shed keeps the node in elections.
        assert!(node_1.registry.should_campaign());
        cluster.shutdown().await;
    }
}

mod commit_stall {
    use std::time::Duration;

    use tokio::time::Instant;
    use ursula_raft::RaftGroupMetricsSnapshot;

    use crate::bootstrap::CommitStallAction;
    use crate::bootstrap::CommitStallTracker;

    fn snap(
        group_id: u32,
        node_id: u64,
        leader: Option<u64>,
        last_log: Option<u64>,
        committed: Option<u64>,
    ) -> RaftGroupMetricsSnapshot {
        super::raft_metrics_snapshot(group_id, node_id, leader, last_log, committed, None, vec![
            1, 2, 3,
        ])
    }

    #[test]
    fn no_gap_emits_no_action_and_tracks_nothing() {
        let mut tracker = CommitStallTracker::default();
        let t0 = Instant::now();
        let snaps = vec![snap(0, 2, Some(2), Some(100), Some(100))];
        let actions = tracker.evaluate(&snaps, 2, t0, Duration::from_secs(15));
        assert!(actions.is_empty());
        // Later tick, still no gap: still nothing.
        let actions = tracker.evaluate(
            &snaps,
            2,
            t0 + Duration::from_secs(60),
            Duration::from_secs(15),
        );
        assert!(actions.is_empty());
    }

    #[test]
    fn gap_below_threshold_silently_baselines() {
        let mut tracker = CommitStallTracker::default();
        let t0 = Instant::now();
        let snaps = vec![snap(0, 2, Some(2), Some(101), Some(100))];
        // First sighting → baselined, no action.
        let actions = tracker.evaluate(&snaps, 2, t0, Duration::from_secs(15));
        assert!(actions.is_empty());
        // 10s later, same indices, still under threshold → still no action.
        let actions = tracker.evaluate(
            &snaps,
            2,
            t0 + Duration::from_secs(10),
            Duration::from_secs(15),
        );
        assert!(actions.is_empty());
    }

    #[test]
    fn gap_persisting_past_threshold_emits_transfer() {
        let mut tracker = CommitStallTracker::default();
        let t0 = Instant::now();
        // Group 3 stalled on N2 (the bug observed in chaos), N1/N3 are voters.
        let snaps = vec![snap(3, 2, Some(2), Some(19172), Some(19171))];
        tracker.evaluate(&snaps, 2, t0, Duration::from_secs(15));
        let actions = tracker.evaluate(
            &snaps,
            2,
            t0 + Duration::from_secs(16),
            Duration::from_secs(15),
        );
        assert_eq!(actions, vec![CommitStallAction {
            group_id: 3,
            // Both peer voters (1 and 3) are equally idle (0 leaderships)
            // and tie-broken by id ascending → [1, 3].
            targets: vec![1, 3],
            stalled_for: Duration::from_secs(16),
            last_log: Some(19172),
            committed: Some(19171),
        }]);
        // After emitting, baseline is cleared → another full threshold wait
        // before re-firing, even though the gap still exists.
        let actions = tracker.evaluate(
            &snaps,
            2,
            t0 + Duration::from_secs(20),
            Duration::from_secs(15),
        );
        assert!(
            actions.is_empty(),
            "must not hammer transfers; got {actions:?}"
        );
    }

    #[test]
    fn forward_progress_resets_the_baseline_and_prevents_trigger() {
        let mut tracker = CommitStallTracker::default();
        let t0 = Instant::now();
        let s1 = vec![snap(0, 2, Some(2), Some(100), Some(99))];
        tracker.evaluate(&s1, 2, t0, Duration::from_secs(15));
        // 10s later: committed catches up by one, gap remains but indices moved.
        let s2 = vec![snap(0, 2, Some(2), Some(101), Some(100))];
        let actions = tracker.evaluate(
            &s2,
            2,
            t0 + Duration::from_secs(10),
            Duration::from_secs(15),
        );
        assert!(actions.is_empty());
        // Another 10s on the same indices (now baselined at t+10) — still
        // under threshold from the most recent re-baseline.
        let actions = tracker.evaluate(
            &s2,
            2,
            t0 + Duration::from_secs(20),
            Duration::from_secs(15),
        );
        assert!(
            actions.is_empty(),
            "10s since reset < 15s threshold; got {actions:?}"
        );
        // 20s past the reset → finally fires.
        let actions = tracker.evaluate(
            &s2,
            2,
            t0 + Duration::from_secs(30),
            Duration::from_secs(15),
        );
        assert_eq!(actions.len(), 1);
    }

    #[test]
    fn m3_followed_by_m1_does_not_double_handoff() {
        // Composite: a stalled leader (node 2) hands off via M3. After M3's
        // transfer lands, the snapshot reflects the new leader. M1 evaluating
        // the SAME post-handoff snapshot must not also plan a redundant
        // transfer of that group, otherwise the two watchdogs would fight on
        // every tick and produce leadership flap.
        let mut tracker = CommitStallTracker::default();
        let t0 = Instant::now();
        let stalled = vec![
            snap(0, 2, Some(2), Some(100), Some(99)), // node 2 stalled
            snap(1, 2, Some(1), Some(50), Some(50)),
            snap(2, 2, Some(3), Some(50), Some(50)),
        ];
        tracker.evaluate(&stalled, 2, t0, Duration::from_secs(15));
        let actions = tracker.evaluate(
            &stalled,
            2,
            t0 + Duration::from_secs(16),
            Duration::from_secs(15),
        );
        assert_eq!(actions.len(), 1);
        let action_target = actions[0].targets[0];

        // Simulate transfer landing: snap now shows new leader and the wedge
        // released (committed caught up).
        let post_transfer = vec![
            snap(0, 2, Some(action_target), Some(100), Some(100)),
            snap(1, 2, Some(1), Some(50), Some(50)),
            snap(2, 2, Some(3), Some(50), Some(50)),
        ];
        let m1_plan = crate::bootstrap::plan_leadership_balance(&post_transfer, 2, 4);
        assert!(
            m1_plan.is_empty(),
            "M1 must not re-balance after M3 handoff; got {m1_plan:?}",
        );

        let m3_followup = tracker.evaluate(
            &post_transfer,
            2,
            t0 + Duration::from_secs(20),
            Duration::from_secs(15),
        );
        assert!(
            m3_followup.is_empty(),
            "M3 followup not empty after handoff: {m3_followup:?}",
        );
    }

    #[test]
    fn target_priority_prefers_least_loaded_peer() {
        // 5 groups total; node 2 leads group 0 (stalled) plus group 4
        // (running fine). Node 1 leads 3 groups, node 3 leads 0. The stall
        // handoff should prefer node 3 (the lightest) first, then node 1.
        let mut tracker = CommitStallTracker::default();
        let t0 = Instant::now();
        let snaps = vec![
            snap(0, 2, Some(2), Some(100), Some(99)), // STALLED, led by us
            snap(1, 2, Some(1), Some(200), Some(200)),
            snap(2, 2, Some(1), Some(300), Some(300)),
            snap(3, 2, Some(1), Some(400), Some(400)),
            snap(4, 2, Some(2), Some(500), Some(500)),
        ];
        tracker.evaluate(&snaps, 2, t0, Duration::from_secs(15));
        let actions = tracker.evaluate(
            &snaps,
            2,
            t0 + Duration::from_secs(16),
            Duration::from_secs(15),
        );
        assert_eq!(actions.len(), 1);
        assert_eq!(
            actions[0].targets,
            vec![3, 1],
            "lightest voter (load 0) first, then the heavy one (load 3); got {:?}",
            actions[0].targets,
        );
    }

    #[test]
    fn losing_leadership_clears_tracking() {
        let mut tracker = CommitStallTracker::default();
        let t0 = Instant::now();
        let stalled = vec![snap(0, 2, Some(2), Some(101), Some(100))];
        tracker.evaluate(&stalled, 2, t0, Duration::from_secs(15));
        // Leadership moves to node 1 (e.g. via M1 transfer); we should drop
        // this group from our tracker — not our problem anymore.
        let new_leader = vec![snap(0, 2, Some(1), Some(101), Some(100))];
        let actions = tracker.evaluate(
            &new_leader,
            2,
            t0 + Duration::from_secs(60),
            Duration::from_secs(15),
        );
        assert!(actions.is_empty());
        // If we win leadership back later, we restart the timer from scratch.
        let re_leader = vec![snap(0, 2, Some(2), Some(101), Some(100))];
        let actions = tracker.evaluate(
            &re_leader,
            2,
            t0 + Duration::from_secs(62),
            Duration::from_secs(15),
        );
        assert!(
            actions.is_empty(),
            "fresh leadership re-baselines; got {actions:?}"
        );
        let actions = tracker.evaluate(
            &re_leader,
            2,
            t0 + Duration::from_secs(80),
            Duration::from_secs(15),
        );
        assert_eq!(actions.len(), 1);
    }
}

// #132: bucket is the top-level tenant namespace. Two tenants using the same
// bucket-local stream name must not observe each other through appends,
// reads, snapshots, or retention.
#[tokio::test]
async fn tenant_buckets_isolate_identical_stream_names() {
    let app = test_router();

    for (bucket, payload) in [
        ("tenant-a", r#"{"who":"a"}"#),
        ("tenant-b", r#"{"who":"b"}"#),
    ] {
        let response = http_put(
            &app,
            &format!("/{bucket}/orders"),
            &[(CONTENT_TYPE.as_str(), "application/json")],
            Body::from(payload),
        )
        .await;
        assert_eq!(response.status(), StatusCode::CREATED);
    }

    // Each tenant reads back only its own record under the shared name.
    for (bucket, expected) in [("tenant-a", "a"), ("tenant-b", "b")] {
        let response = http_get(&app, &format!("/{bucket}/orders?offset=-1")).await;
        assert_eq!(response.status(), StatusCode::OK);
        let body = body_bytes(response).await;
        let body = std::str::from_utf8(&body).expect("utf8 body");
        assert!(
            body.contains(&format!("\"who\":\"{expected}\"")),
            "bucket {bucket} must see only its own record, got: {body}"
        );
    }

    // Snapshot plus retention truncation on tenant-a must not affect
    // tenant-b's readable history for the identically named stream.
    let response = http_put(
        &app,
        "/tenant-a/orders/snapshot/00000000000000000012",
        &[(CONTENT_TYPE.as_str(), "application/json")],
        Body::from(r#"{"count":1}"#),
    )
    .await;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    let response = http_put(
        &app,
        "/tenant-a/orders/retention/00000000000000000012",
        &[],
        Body::empty(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    let response = http_get(&app, "/tenant-a/orders?offset=0").await;
    assert_eq!(response.status(), StatusCode::GONE);

    let response = http_get(&app, "/tenant-b/orders?offset=0").await;
    assert_eq!(response.status(), StatusCode::OK);

    // The snapshot namespace is tenant-scoped as well: tenant-b has none.
    let response = http_head(&app, "/tenant-b/orders").await;
    assert_eq!(response.status(), StatusCode::OK);
    assert!(
        response
            .headers()
            .get(names::STREAM_SNAPSHOT_OFFSET)
            .is_none()
    );
}

// #135 data-plane half: committed usage counters aggregated across groups.
#[tokio::test]
async fn usage_endpoint_reports_per_bucket_committed_counters() {
    let app = test_router();

    for bucket in ["tenant-a", "tenant-b"] {
        let response = http_put(
            &app,
            &format!("/{bucket}/orders"),
            &[(CONTENT_TYPE.as_str(), "application/json")],
            Body::from(r#"{"who":"a"}"#),
        )
        .await;
        assert_eq!(response.status(), StatusCode::CREATED);
    }

    let usage_for = |report: &serde_json::Value, bucket: &str| -> serde_json::Value {
        report["buckets"][bucket].clone()
    };
    let fetch_usage = || async {
        let response = http_get(&app, "/__ursula/usage").await;
        assert_eq!(response.status(), StatusCode::OK);
        let body = body_bytes(response).await;
        serde_json::from_slice::<serde_json::Value>(&body).expect("usage JSON")
    };

    let created = fetch_usage().await;
    let tenant_a_created = usage_for(&created, "tenant-a");
    assert_eq!(tenant_a_created["stream_count"], 1);
    assert_eq!(created["version"], 1);
    assert_eq!(created["write_unit_bytes"], 10 * 1024);
    assert_eq!(tenant_a_created["committed_write_units"], 1);
    assert!(
        tenant_a_created["committed_append_bytes"]
            .as_u64()
            .expect("bytes")
            > 0
    );
    assert_eq!(
        tenant_a_created,
        usage_for(&created, "tenant-b"),
        "identical creations produce identical usage in both tenants"
    );

    // One accepted append, then a deduplicated producer retry of the same
    // logical append: the retry must be invisible to every counter.
    for _ in 0..2 {
        let response = http_post(
            &app,
            "/tenant-a/orders",
            &[
                (CONTENT_TYPE.as_str(), "application/json"),
                ("producer-id", "usage-writer"),
                ("producer-epoch", "0"),
                ("producer-seq", "0"),
            ],
            Body::from(r#"{"n":1}"#),
        )
        .await;
        assert!(response.status().is_success());
    }

    let appended = fetch_usage().await;
    let tenant_a = usage_for(&appended, "tenant-a");
    assert!(
        tenant_a["committed_append_bytes"].as_u64().expect("bytes")
            > tenant_a_created["committed_append_bytes"]
                .as_u64()
                .expect("bytes"),
        "the accepted append grew the committed counter"
    );
    assert_eq!(
        tenant_a["committed_records"].as_u64().expect("records"),
        tenant_a_created["committed_records"]
            .as_u64()
            .expect("records")
            + 1,
        "exactly one record committed despite the duplicate retry"
    );
    assert_eq!(
        tenant_a["committed_write_units"].as_u64().expect("units"),
        tenant_a_created["committed_write_units"]
            .as_u64()
            .expect("units")
            + 1,
        "the duplicate retry does not create another write unit"
    );
    assert_eq!(
        tenant_a["committed_append_bytes"], tenant_a["retained_bytes"],
        "nothing reclaimed yet"
    );
    assert_eq!(
        usage_for(&appended, "tenant-b"),
        tenant_a_created,
        "tenant-b is untouched by tenant-a's appends"
    );

    // Retention truncation shrinks the retained gauge but never the
    // monotonic committed counters.
    let response = http_put(
        &app,
        "/tenant-a/orders/snapshot/00000000000000000020",
        &[(CONTENT_TYPE.as_str(), "application/json")],
        Body::from(r#"{"count":2}"#),
    )
    .await;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    let response = http_put(
        &app,
        "/tenant-a/orders/retention/00000000000000000020",
        &[],
        Body::empty(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    let truncated = fetch_usage().await;
    let tenant_a_truncated = usage_for(&truncated, "tenant-a");
    assert_eq!(
        tenant_a_truncated["committed_append_bytes"], tenant_a["committed_append_bytes"],
        "retention never rewinds the monotonic counter"
    );
    assert_eq!(tenant_a_truncated["retained_bytes"], 0);
}

/// E10: an import is refused with 400 before decoding when its group has no
/// backup format (Ursula 0.5.x) or another one.
#[tokio::test]
async fn backup_import_refuses_groups_of_another_backup_format() {
    let app = test_router();
    let without_format =
        rmp_serde::to_vec_named(&json!({"buckets": ["tenant-a"], "streams": []})).unwrap();
    let other_format = |format_version| {
        rmp_serde::to_vec_named(&ursula_runtime::StreamSnapshot {
            format_version,
            ..ursula_runtime::StreamSnapshot::default()
        })
        .unwrap()
    };
    let older = ursula_runtime::BACKUP_FORMAT_VERSION - 1;
    let newer = ursula_runtime::BACKUP_FORMAT_VERSION + 1;
    for (body, expected) in [
        (without_format, "has no format_epoch field".to_owned()),
        (other_format(older), format!("is backup format {older}")),
        (other_format(newer), format!("is backup format {newer}")),
    ] {
        let digest = blake3::hash(&body).to_hex().to_string();
        let response = http_post(
            &app,
            "/__ursula/backup/group/0/import",
            &[
                (CONTENT_TYPE.as_str(), "application/x-msgpack"),
                ("x-ursula-backup-blake3", digest.as_str()),
            ],
            Body::from(body),
        )
        .await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = body_bytes(response).await;
        let body = std::str::from_utf8(&body).expect("utf8");
        assert!(body.contains(&expected), "{body}");
    }
}

fn cold_runtime_router(cold_store: Option<ColdStoreHandle>) -> Router {
    router(
        ShardRuntime::spawn_with_engine_factory_and_cold_store(
            RuntimeConfig::new(1, 1),
            InMemoryGroupEngineFactory::with_cold_store(cold_store.clone()),
            cold_store,
        )
        .expect("runtime"),
    )
}

async fn backup_cold_check(app: &Router, body: &Bytes) -> ursula_proto::admin::BackupColdCheck {
    let response = http_post(
        app,
        "/__ursula/backup/group/0/cold-check",
        &[(CONTENT_TYPE.as_str(), "application/x-msgpack")],
        Body::from(body.clone()),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    serde_json::from_slice(&body_bytes(response).await).expect("cold check JSON")
}

/// A backup carries cold references, not cold objects. The cold check finds
/// every object a group references in the source cluster's cold store, and
/// names what a target cold store lacks: the index pages when nothing was
/// copied, a chunk when the copy is incomplete. It imports nothing.
#[tokio::test]
async fn backup_cold_check_names_cold_objects_the_target_lacks() {
    let cold_store: ColdStoreHandle = Arc::new(ColdStore::memory().expect("memory cold store"));
    let source = cold_runtime_router(Some(cold_store.clone()));
    let response = http_put(
        &source,
        "/tenant-a/cold",
        &[(CONTENT_TYPE.as_str(), "text/plain")],
        Body::from("abcdef"),
    )
    .await;
    assert_eq!(response.status(), StatusCode::CREATED);
    let response = http_post(
        &source,
        "/__ursula/flush-cold/tenant-a/cold?min_hot_bytes=4&max_bytes=4",
        &[],
        Body::empty(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let response = http_get(&source, "/__ursula/backup/group/0").await;
    assert_eq!(response.status(), StatusCode::OK);
    let export = body_bytes(response).await;

    let complete = backup_cold_check(&source, &export).await;
    assert_eq!(complete.missing_objects, 0, "{complete:?}");
    assert!(
        complete.referenced_objects >= 2,
        "an index page and its chunk: {complete:?}"
    );

    // Another cold store: the index page is missing, so nothing it names can
    // be reached either. Without a cold store at all, the same.
    for target in [
        cold_runtime_router(Some(Arc::new(
            ColdStore::memory().expect("memory cold store"),
        ))),
        cold_runtime_router(None),
    ] {
        let empty = backup_cold_check(&target, &export).await;
        assert!(empty.missing_objects >= 1, "{empty:?}");
        assert_eq!(empty.missing_objects, empty.referenced_objects);
        assert!(
            empty
                .missing_sample
                .iter()
                .all(|key| key.starts_with("tenant-a/cold/cold-index/")),
            "{empty:?}"
        );
        let response = http_get(&target, "/tenant-a/cold?offset=0").await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND, "nothing imported");
    }

    // An incomplete copy: the page is there, one chunk it names is not.
    let pages = cold_store
        .list_cold_index_pages()
        .await
        .expect("list pages");
    let page = ursula_runtime::ColdIndexPageStore::get_page(
        &ursula_runtime::ColdStoreColdIndexPageStore::new(cold_store.clone()),
        &pages[0],
    )
    .await
    .expect("read page")
    .expect("page exists");
    let lost = page.cold_chunks[0].s3_path.clone();
    cold_store.delete_chunk(&lost).await.expect("delete chunk");
    let partial = backup_cold_check(&source, &export).await;
    assert_eq!(partial.missing_objects, 1, "{partial:?}");
    assert_eq!(partial.missing_sample, vec![lost]);
}

// #136: the full recovery drill against the HTTP surface. Build a cluster,
// write two tenants' streams (records, close state, app snapshot, retention
// floor), export every group, destroy the cluster, restore into a fresh one,
// and verify bytes, offsets, closed state, retention, snapshots,
// tenant boundaries, and continued appends -- with no offset drift.
#[tokio::test]
async fn backup_restore_drill_preserves_streams_and_allows_continued_appends() {
    let source = test_router();

    // Tenant A: JSON messages, app snapshot, retention floor after the
    // second message (offset 18).
    let response = http_put(
        &source,
        "/tenant-a/orders",
        &[(CONTENT_TYPE.as_str(), "application/json")],
        Body::from(r#"[{"id":1},{"id":2}]"#),
    )
    .await;
    assert_eq!(response.status(), StatusCode::CREATED);
    let response = http_post(
        &source,
        "/tenant-a/orders",
        &[(CONTENT_TYPE.as_str(), "application/json")],
        Body::from(r#"{"id":3}"#),
    )
    .await;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    let response = http_put(
        &source,
        "/tenant-a/orders/snapshot/00000000000000000018",
        &[(CONTENT_TYPE.as_str(), "application/json")],
        Body::from(r#"{"count":2}"#),
    )
    .await;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    let response = http_put(
        &source,
        "/tenant-a/orders/retention/00000000000000000018",
        &[],
        Body::empty(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    // Tenant B: raw byte stream, closed.
    let response = http_put(
        &source,
        "/tenant-b/journal",
        &[(CONTENT_TYPE.as_str(), "text/plain")],
        Body::from("raw-payload"),
    )
    .await;
    assert_eq!(response.status(), StatusCode::CREATED);
    let response = http_post(
        &source,
        "/tenant-b/journal",
        &[("stream-closed", "true")],
        Body::empty(),
    )
    .await;
    assert!(response.status().is_success());

    // Tenant A: a byte stream of more than one chunk, so its first chunk is
    // marked below the tail.
    let response = http_put(
        &source,
        "/tenant-a/blob",
        &[(CONTENT_TYPE.as_str(), "application/octet-stream")],
        Body::from(vec![7_u8; 4 * 1024 * 1024 + 10]),
    )
    .await;
    assert_eq!(response.status(), StatusCode::CREATED);

    // Record a marked chunk and a closed tail, as a cache would keep them.
    let mut recorded = Vec::new();
    for (stream, expected) in [
        ("/tenant-a/blob", ursula_headers::ChunkMark::Immutable),
        ("/tenant-b/journal", ursula_headers::ChunkMark::ClosedTail),
    ] {
        let response = http_head(&source, stream).await;
        let head = ursula_headers::StreamHead::parse(
            response
                .headers()
                .iter()
                .map(|(name, value)| (name.as_str(), value.as_bytes())),
        )
        .expect("a stream head");
        let chunk = format!(
            "{stream}{}",
            ursula_headers::ImmutableResource {
                kind: ursula_headers::ResourceKind::Chunk,
                incarnation: head.incarnation(),
                offset: ursula_headers::value::Offset::ZERO,
            }
            .path()
        );
        let response = http_get(&source, &chunk).await;
        assert_eq!(response.status(), StatusCode::OK, "{chunk}");
        let read = ursula_headers::ChunkRead::parse(
            response
                .headers()
                .iter()
                .map(|(name, value)| (name.as_str(), value.as_bytes())),
        )
        .expect("a chunk answer");
        assert_eq!(read.mark(), expected, "{chunk}");
        recorded.push((stream, head, chunk));
    }

    // Export every group, checking transfer checksums like ursulactl does.
    let info = http_get(&source, "/__ursula/backup/info").await;
    assert_eq!(info.status(), StatusCode::OK);
    let info: serde_json::Value =
        serde_json::from_slice(&body_bytes(info).await).expect("backup info json");
    assert_eq!(
        info["format_version"],
        ursula_runtime::BACKUP_FORMAT_VERSION
    );
    let group_count = info["raft_group_count"].as_u64().expect("group count");
    let mut exports = Vec::new();
    for group in 0..group_count {
        let response = http_get(&source, &format!("/__ursula/backup/group/{group}")).await;
        assert_eq!(response.status(), StatusCode::OK);
        let declared = header_str(&response, "x-ursula-backup-blake3").to_owned();
        let body = body_bytes(response).await;
        assert_eq!(blake3::hash(&body).to_hex().to_string(), declared);
        exports.push(body);
    }

    // Destroy the source cluster entirely (in-memory: dropping it is total
    // loss) and build a fresh one with its own identity.
    drop(source);
    let restored = test_router();

    for (group, body) in exports.iter().enumerate() {
        let digest = blake3::hash(body).to_hex().to_string();
        let response = http_post(
            &restored,
            &format!("/__ursula/backup/group/{group}/import"),
            &[
                (CONTENT_TYPE.as_str(), "application/x-msgpack"),
                ("x-ursula-backup-blake3", digest.as_str()),
            ],
            Body::from(body.clone()),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK, "import group {group}");
        let imported: ursula_proto::admin::BackupImportResponse =
            serde_json::from_slice(&body_bytes(response).await).expect("typed import answer");
        assert!(!imported.already, "group {group}");
    }

    // Retention floor survived: reads below offset 18 are GONE, the app
    // snapshot is intact, and the tail message is exactly where it was.
    let response = http_get(&restored, "/tenant-a/orders?offset=0").await;
    assert_eq!(response.status(), StatusCode::GONE);
    let response = http_get(&restored, "/tenant-a/orders?offset=18").await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = body_bytes(response).await;
    assert!(
        std::str::from_utf8(&body)
            .expect("utf8")
            .contains("\"id\":3")
    );
    // Read the latest snapshot like a client: its offset comes from HEAD.
    let response = http_head(&restored, "/tenant-a/orders").await;
    assert_eq!(response.status(), StatusCode::OK);
    let snapshot_offset = header_str(&response, names::STREAM_SNAPSHOT_OFFSET).to_owned();
    let response = http_get(
        &restored,
        &format!("/tenant-a/orders/snapshot/{snapshot_offset}"),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = body_bytes(response).await;
    assert_eq!(&body[..], br#"{"count":2}"#);

    // Closed byte stream: bytes identical, close state preserved.
    let response = http_get(&restored, "/tenant-b/journal?offset=0&max_bytes=100").await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(header_str(&response, "stream-closed"), "true");
    let body = body_bytes(response).await;
    assert_eq!(&body[..], b"raw-payload");
    let response = http_post(
        &restored,
        "/tenant-b/journal",
        &[(CONTENT_TYPE.as_str(), "text/plain")],
        Body::from("more"),
    )
    .await;
    assert_eq!(response.status(), StatusCode::CONFLICT);

    // A restore is a history event: the recorded URLs name the source's
    // tokens, so the restored cluster answers them `412`, even after the
    // stream grows again, and HEAD traces the new token to the recorded one
    // (#581 section 5.5). Nothing a cache kept needs purging.
    let response = http_post(
        &restored,
        "/tenant-a/blob",
        &[(CONTENT_TYPE.as_str(), "application/octet-stream")],
        Body::from("regrown"),
    )
    .await;
    assert!(response.status().is_success());
    for (stream, source_head, chunk) in &recorded {
        let response = http_get(&restored, chunk).await;
        assert_eq!(
            response.status(),
            StatusCode::PRECONDITION_FAILED,
            "{chunk}"
        );
        let response = http_head(&restored, stream).await;
        let head = ursula_headers::StreamHead::parse(
            response
                .headers()
                .iter()
                .map(|(name, value)| (name.as_str(), value.as_bytes())),
        )
        .expect("a stream head");
        assert_ne!(head.incarnation(), source_head.incarnation());
        assert_eq!(
            head.inherited(),
            Some(ursula_headers::Inherited {
                previous_incarnation: source_head.incarnation(),
                offset: source_head.next_offset(),
            }),
            "{stream}"
        );
    }

    // Tenant boundary: a bucket that never existed stays absent.
    let response = http_get(&restored, "/tenant-c/orders?offset=0").await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    // Continued append lands at the tail with no drift.
    let response = http_post(
        &restored,
        "/tenant-a/orders",
        &[(CONTENT_TYPE.as_str(), "application/json")],
        Body::from(r#"{"id":4}"#),
    )
    .await;
    assert!(response.status().is_success());
    let response = http_get(&restored, "/tenant-a/orders?offset=27").await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = body_bytes(response).await;
    assert!(
        std::str::from_utf8(&body)
            .expect("utf8")
            .contains("\"id\":4")
    );

    // The import is keyed by the backup's digest: repeating it answers
    // `already`, and another backup into a group that now owns a bucket
    // fails closed.
    let mut conflicted = false;
    for (group, body) in exports.iter().enumerate() {
        let snapshot: ursula_runtime::StreamSnapshot =
            rmp_serde::from_slice(body).expect("decode export");
        if snapshot.buckets.is_empty() {
            continue;
        }
        let digest = blake3::hash(body).to_hex().to_string();
        let response = http_post(
            &restored,
            &format!("/__ursula/backup/group/{group}/import"),
            &[
                (CONTENT_TYPE.as_str(), "application/x-msgpack"),
                ("x-ursula-backup-blake3", digest.as_str()),
            ],
            Body::from(body.clone()),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK, "group {group}");
        let repeat: ursula_proto::admin::BackupImportResponse =
            serde_json::from_slice(&body_bytes(response).await).expect("typed import answer");
        assert!(repeat.already, "group {group}");

        let mut other = snapshot.clone();
        other.buckets.push("tenant-z".to_owned());
        let other = rmp_serde::to_vec_named(&other).expect("encode another backup");
        let digest = blake3::hash(&other).to_hex().to_string();
        let response = http_post(
            &restored,
            &format!("/__ursula/backup/group/{group}/import"),
            &[
                (CONTENT_TYPE.as_str(), "application/x-msgpack"),
                ("x-ursula-backup-blake3", digest.as_str()),
            ],
            Body::from(other),
        )
        .await;
        assert_eq!(response.status(), StatusCode::CONFLICT, "group {group}");
        conflicted = true;
    }
    assert!(conflicted, "expected at least one bucket-owning group");
}

// D10 / AUD §4.8: purge is an admin-plane route. The client listener does not
// serve it; `/__ursula/usage` stays on both listeners for Cloud's meter.
#[tokio::test]
async fn purge_is_served_on_the_admin_listener_only() {
    let state = HttpState::new(
        spawn_runtime(
            &test_config(1, 1),
            Persistence::InMemory,
            Topology::SingleNode {
                raft_group_count: 1,
            },
        )
        .expect("runtime")
        .runtime,
    );
    let client = client_router_with_admission(state.clone(), IngressAdmission::default());
    let admin = admin_router(state);

    let response = http_delete(&client, "/__ursula/purge/tenant-x").await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    let response = http_delete(&admin, "/__ursula/purge/tenant-x").await;
    assert_eq!(response.status(), StatusCode::OK);

    for app in [&client, &admin] {
        let response = http_get(app, "/__ursula/usage").await;
        assert_eq!(response.status(), StatusCode::OK);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cluster_wide_purge_reaches_every_distributed_group_leader() {
    let mut listeners = Vec::new();
    let mut peers = Vec::new();
    for node_id in 1..=3u64 {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind listener");
        let addr = listener.local_addr().expect("local addr");
        peers.push((node_id, format!("http://{addr}")));
        listeners.push(listener);
    }

    let mut nodes = Vec::new();
    for (index, listener) in listeners.into_iter().enumerate() {
        let node_id = u64::try_from(index + 1).expect("node id fits u64");
        nodes.push(
            spawn_static_grpc_test_node(
                node_id,
                listener,
                peers.clone(),
                peers.clone(),
                true,
                6,
                StaticGrpcTestNodeStorage {
                    per_group_initializers: true,
                    ..Default::default()
                },
            )
            .await,
        );
    }
    for node in &nodes {
        tokio::time::timeout(Duration::from_secs(10), node.runtime.warm_all_groups())
            .await
            .expect("warm all groups timed out")
            .expect("warm all groups");
    }
    for raw_group_id in 0u32..6 {
        let expected_leader = u64::from(raw_group_id % 3) + 1;
        for node in &nodes {
            node.registry
                .get(RaftGroupId(raw_group_id))
                .expect("registered group")
                .wait(Some(Duration::from_secs(5)))
                .current_leader(expected_leader, "per-group leader elected")
                .await
                .expect("wait for distributed leader");
        }
    }

    let mut streams_by_group: Vec<Option<BucketStreamId>> = vec![None; 6];
    for candidate in 0..10_000 {
        let stream_id =
            BucketStreamId::new("multi-leader-purge", format!("group-stream-{candidate}"));
        let group_index = usize::try_from(nodes[0].runtime.locate(&stream_id).raft_group_id.0)
            .expect("raft group id fits usize");
        if streams_by_group[group_index].is_none() {
            streams_by_group[group_index] = Some(stream_id);
        }
        if streams_by_group.iter().all(Option::is_some) {
            break;
        }
    }
    let streams_by_group = streams_by_group
        .into_iter()
        .map(|stream| stream.expect("found stream for every group"))
        .collect::<Vec<_>>();
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .expect("build HTTP client");
    for stream_id in &streams_by_group {
        let response = client
            .put(format!("{}/{}", peers[0].1, stream_id))
            .header(CONTENT_TYPE, "text/plain")
            .body("before-purge")
            .send()
            .await
            .expect("create through group leader redirect");
        assert_eq!(response.status(), StatusCode::CREATED);
    }

    let response = admin_test_request(
        &client,
        reqwest::Method::DELETE,
        format!("{}/__ursula/purge/multi-leader-purge", peers[0].1),
    )
    .await
    .send()
    .await
    .expect("cluster-wide purge request");
    assert_eq!(response.status(), StatusCode::OK);
    let report: serde_json::Value = response.json().await.expect("purge report JSON");
    assert_eq!(report["removed_streams"], 6);
    assert_eq!(
        report["groups_with_streams"],
        serde_json::json!([0, 1, 2, 3, 4, 5])
    );
    assert_eq!(report["cold_gc_pending_entries"], 0);
    assert_eq!(report["cold_gc_complete"], true);
    assert_eq!(report["bucket_prefix_absent"], true);

    for stream_id in &streams_by_group {
        let response = client
            .put(format!("{}/{}", peers[0].1, stream_id))
            .header(CONTENT_TYPE, "text/plain")
            .body("must-stay-erased")
            .send()
            .await
            .expect("recreate request reaches group leader");
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    for node in nodes {
        node.shutdown().await;
    }
}

// #150: administrator-triggered tenant purge. Purging tenant A must remove
// its streams and bucket while retaining its aggregate accounting counters;
// tenant B's identically named stream, its offsets, and snapshot stay
// untouched. A re-run converges on the same report shape with zero counts.
#[tokio::test]
async fn purge_endpoint_erases_one_tenant_and_leaves_the_other_intact() {
    let app = test_router();

    for (bucket, payload) in [
        ("tenant-a", r#"{"who":"a"}"#),
        ("tenant-b", r#"{"who":"b"}"#),
    ] {
        let response = http_put(
            &app,
            &format!("/{bucket}/orders"),
            &[(CONTENT_TYPE.as_str(), "application/json")],
            Body::from(payload),
        )
        .await;
        assert_eq!(response.status(), StatusCode::CREATED);
    }
    let response = http_put(
        &app,
        "/tenant-a/orders/snapshot/00000000000000000012",
        &[(CONTENT_TYPE.as_str(), "application/json")],
        Body::from(r#"{"count":1}"#),
    )
    .await;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    let snapshot_offset = header_str(&response, names::STREAM_SNAPSHOT_OFFSET).to_owned();

    let response = send(
        &app,
        "DELETE",
        "/__ursula/purge/tenant-a",
        &[],
        Body::empty(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = body_bytes(response).await;
    let report: serde_json::Value = serde_json::from_slice(&body).expect("purge report JSON");
    assert_eq!(report["bucket"], "tenant-a");
    assert_eq!(report["removed_streams"], 1);
    assert_eq!(report["cold_gc_pending_entries"], 0);
    assert_eq!(report["cold_gc_complete"], true);
    assert!(report["cold_gc_error"].is_null());
    assert!(
        report["groups_with_streams"]
            .as_array()
            .is_some_and(|groups| !groups.is_empty())
    );

    // Tenant A conceals as not-found across streams and snapshots.
    let response = http_get(&app, "/tenant-a/orders?offset=0").await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    let response = http_get(
        &app,
        &format!("/tenant-a/orders/snapshot/{snapshot_offset}"),
    )
    .await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    // Tenant B's identically named stream is untouched.
    let response = http_get(&app, "/tenant-b/orders?offset=-1").await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = body_bytes(response).await;
    let body = std::str::from_utf8(&body).expect("utf8 body");
    assert!(body.contains(r#""who":"b""#));

    // Content gauges reach zero, but monotonic write counters remain visible
    // until the asynchronous biller has observed them.
    let response = http_get(&app, "/__ursula/usage").await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = body_bytes(response).await;
    let usage: serde_json::Value = serde_json::from_slice(&body).expect("usage JSON");
    assert_eq!(usage["buckets"]["tenant-a"]["stream_count"], 0);
    assert_eq!(usage["buckets"]["tenant-a"]["retained_bytes"], 0);
    assert!(
        usage["buckets"]["tenant-a"]["committed_write_units"]
            .as_u64()
            .is_some_and(|units| units > 0)
    );
    assert!(usage["buckets"].get("tenant-b").is_some());

    // Idempotent re-run: same report shape, zero removals.
    let response = send(
        &app,
        "DELETE",
        "/__ursula/purge/tenant-a",
        &[],
        Body::empty(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = body_bytes(response).await;
    let rerun: serde_json::Value = serde_json::from_slice(&body).expect("rerun report JSON");
    assert_eq!(rerun["removed_streams"], 0);
    assert_eq!(rerun["cold_gc_pending_entries"], 0);
    assert_eq!(rerun["cold_gc_complete"], true);

    // The durable erasure fence permanently rejects namespace reuse.
    let response = http_put(
        &app,
        "/tenant-a/orders",
        &[(CONTENT_TYPE.as_str(), "application/json")],
        Body::from(r#"{"who":"a2"}"#),
    )
    .await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn purge_erases_external_payloads_and_uncommitted_stage_orphans() {
    let cold_store = Arc::new(ColdStore::memory().expect("memory cold store"));
    let runtime = ShardRuntime::spawn_with_engine_factory_and_cold_store(
        RuntimeConfig::new(1, 1),
        InMemoryGroupEngineFactory::with_cold_store(Some(cold_store.clone())),
        Some(cold_store.clone()),
    )
    .expect("runtime");
    let app = router(runtime);
    let payload = vec![b'x'; 1024 * 1024 + 1];
    let response = http_put(
        &app,
        "/external-tenant/large",
        &[(CONTENT_TYPE.as_str(), "application/octet-stream")],
        Body::from(payload.clone()),
    )
    .await;
    assert_eq!(response.status(), StatusCode::CREATED);
    let response = http_get(&app, "/external-tenant/large?offset=0&max_bytes=1048577").await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(body_bytes(response).await.len(), payload.len());

    // Simulates a process crash after S3 staging and before the Raft command:
    // no stream state references this object, so only bucket-domain erasure
    // can discover and remove it.
    cold_store
        .write_chunk(
            "external-tenant/orphan/external/staged-before-commit.bin",
            b"orphan",
        )
        .await
        .expect("stage orphan");
    cold_store
        .write_chunk("surviving-tenant/keep/external/object.bin", b"keep")
        .await
        .expect("write other tenant object");
    assert!(
        !cold_store
            .prefix_is_empty("external-tenant/")
            .await
            .expect("list tenant prefix")
    );

    let response = http_delete(&app, "/__ursula/purge/external-tenant").await;
    assert_eq!(response.status(), StatusCode::OK);
    let report: serde_json::Value =
        serde_json::from_slice(&body_bytes(response).await).expect("purge report");
    assert_eq!(report["cold_gc_complete"], true);
    assert_eq!(report["bucket_prefix_absent"], true);
    assert!(
        cold_store
            .prefix_is_empty("external-tenant/")
            .await
            .expect("prove tenant prefix absent")
    );
    assert!(
        !cold_store
            .prefix_is_empty("surviving-tenant/")
            .await
            .expect("list surviving tenant")
    );

    let response = http_put(
        &app,
        "/external-tenant/recreated",
        &[(CONTENT_TYPE.as_str(), "text/plain")],
        Body::from("blocked"),
    )
    .await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

// Bounded-state gauges: per-group state gauges exported in /__ursula/metrics.
#[tokio::test]
async fn metrics_expose_per_group_state_gauges() {
    let app = test_router();
    let response = http_put(
        &app,
        "/benchcmp/gauge-stream",
        &[
            (CONTENT_TYPE.as_str(), "application/json"),
            ("Stream-TTL", "3600"),
        ],
        Body::empty(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::CREATED);
    for seq in 0..3 {
        let seq = seq.to_string();
        let response = http_post(
            &app,
            "/benchcmp/gauge-stream",
            &[
                (CONTENT_TYPE.as_str(), "application/json"),
                ("Producer-Id", "writer-1"),
                ("Producer-Epoch", "0"),
                ("Producer-Seq", seq.as_str()),
            ],
            Body::from(r#"[{"a":1},{"b":2}]"#),
        )
        .await;
        assert!(response.status().is_success(), "{}", response.status());
    }

    let response = http_get(&app, "/__ursula/metrics").await;
    assert_eq!(response.status(), StatusCode::OK);
    let metrics: serde_json::Value =
        serde_json::from_slice(&body_bytes(response).await).expect("metrics json");
    let groups = metrics["group_state_gauges"]
        .as_array()
        .expect("group_state_gauges array");
    assert_eq!(groups.len(), 8, "{metrics}");
    assert!(groups.iter().all(|group| group["hosted"] == true));
    let streams: u64 = groups.iter().filter_map(|g| g["streams"].as_u64()).sum();
    assert_eq!(streams, 1);
    let group = groups
        .iter()
        .find(|group| group["streams"] == 1)
        .expect("group holding the stream");
    assert_eq!(group["producers"], 1, "{group}");
    assert_eq!(group["ttl_streams"], 1);
    assert!(group["ttl_heap_entries"].as_u64().expect("ttl heap") >= 1);
    // F6b: the three contiguous appends share one hot block.
    assert_eq!(group["hot_chunks"], 1);
    for key in [
        "shared_refs",
        "live_packs",
        "staged_external_refs",
        "producer_bytes",
        "hot_payload_bytes",
        "hot_overhead_bytes",
        "pending_cold_gc",
    ] {
        assert!(group[key].is_u64(), "missing {key}: {group}");
    }
}

/// `PUT /{bucket}` is a validated no-op: buckets are implicit namespaces.
#[tokio::test]
async fn create_bucket_validates_the_bucket_id() {
    let app = test_router();
    let longest = "b".repeat(64);
    let too_long = "b".repeat(65);
    for bucket in ["abcd", "tenant-a_1", longest.as_str()] {
        let response = http_put(&app, &format!("/{bucket}"), &[], Body::empty()).await;
        assert_eq!(response.status(), StatusCode::CREATED, "{bucket}");
    }
    for bucket in ["abc", "Tenant", "bad.id", too_long.as_str()] {
        let response = http_put(&app, &format!("/{bucket}"), &[], Body::empty()).await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{bucket}");
    }
}

/// bounded-stream-state F3: `Producer-Id` and `Stream-Seq` are capped at
/// 256 bytes on every HTTP write path, ungated.
#[tokio::test]
async fn producer_id_and_stream_seq_length_caps_reject_with_400() {
    let app = test_router();
    let at_cap = "p".repeat(WRITE_IDENTIFIER_MAX_BYTES);
    let over_cap = "p".repeat(WRITE_IDENTIFIER_MAX_BYTES + 1);
    let producer = |id: &str| -> Vec<(&'static str, String)> {
        vec![
            (names::PRODUCER_ID, id.to_owned()),
            (names::PRODUCER_EPOCH, "0".to_owned()),
            (names::PRODUCER_SEQ, "0".to_owned()),
        ]
    };
    fn as_refs<'a>(headers: &'a [(&'static str, String)]) -> Vec<(&'static str, &'a str)> {
        headers
            .iter()
            .map(|(name, value)| (*name, value.as_str()))
            .collect()
    }

    // Create.
    let mut headers = producer(&over_cap);
    headers.push((CONTENT_TYPE.as_str(), "text/plain".to_owned()));
    let response = http_put(&app, "/benchcmp/caps-a", &as_refs(&headers), Body::empty()).await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let response = http_put(
        &app,
        "/benchcmp/caps-a",
        &[
            (CONTENT_TYPE.as_str(), "text/plain"),
            (HEADER_STREAM_SEQ, over_cap.as_str()),
        ],
        Body::empty(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let response = http_put(
        &app,
        "/benchcmp/caps-a",
        &[(CONTENT_TYPE.as_str(), "text/plain")],
        Body::empty(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::CREATED);

    // Append.
    let mut headers = producer(&over_cap);
    headers.push((CONTENT_TYPE.as_str(), "text/plain".to_owned()));
    let response = http_post(
        &app,
        "/benchcmp/caps-a",
        &as_refs(&headers),
        Body::from("a"),
    )
    .await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let response = http_post(
        &app,
        "/benchcmp/caps-a",
        &[
            (CONTENT_TYPE.as_str(), "text/plain"),
            (HEADER_STREAM_SEQ, over_cap.as_str()),
        ],
        Body::from("a"),
    )
    .await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    // Exactly at the cap is accepted.
    let mut headers = producer(&at_cap);
    headers.push((CONTENT_TYPE.as_str(), "text/plain".to_owned()));
    headers.push((HEADER_STREAM_SEQ, at_cap.clone()));
    let response = http_post(
        &app,
        "/benchcmp/caps-a",
        &as_refs(&headers),
        Body::from("a"),
    )
    .await;
    assert!(response.status().is_success(), "{}", response.status());

    // Close with an empty body.
    let mut headers = producer(&over_cap);
    headers.push((names::STREAM_CLOSED, "true".to_owned()));
    let response = http_post(&app, "/benchcmp/caps-a", &as_refs(&headers), Body::empty()).await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let response = http_post(
        &app,
        "/benchcmp/caps-a",
        &[
            (names::STREAM_CLOSED, "true"),
            (HEADER_STREAM_SEQ, over_cap.as_str()),
        ],
        Body::empty(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

/// One receipt per producer (#581 section 2.7): a repeat of an older write
/// answers `204 receipt-evicted` with the epoch's highest seq and no range,
/// a repeat of the newest write gets its receipt, and neither appends.
#[tokio::test]
async fn an_older_repeat_is_receipt_evicted_and_the_newest_gets_its_receipt() {
    let app = test_router();
    let response = http_put(
        &app,
        "/benchcmp/receipt-window",
        &[(CONTENT_TYPE.as_str(), "application/json")],
        Body::empty(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::CREATED);
    let post = |seq: u64| {
        let app = app.clone();
        async move {
            let seq = seq.to_string();
            http_post(
                &app,
                "/benchcmp/receipt-window",
                &[
                    (CONTENT_TYPE.as_str(), "application/json"),
                    (names::PRODUCER_ID, "writer"),
                    (names::PRODUCER_EPOCH, "0"),
                    (names::PRODUCER_SEQ, seq.as_str()),
                ],
                Body::from(r#"{"a":1}"#),
            )
            .await
        }
    };
    for seq in 0..3 {
        assert_eq!(post(seq).await.status(), StatusCode::OK);
    }
    let tail = http_head(&app, "/benchcmp/receipt-window").await;
    let tail_offset = tail.headers().get(names::STREAM_NEXT_OFFSET).cloned();

    let evicted = post(0).await;
    assert_eq!(evicted.status(), StatusCode::NO_CONTENT);
    assert_eq!(header_str(&evicted, names::PRODUCER_SEQ), "2");
    assert_eq!(header_str(&evicted, "producer-outcome"), "receipt-evicted");
    assert!(evicted.headers().get(names::STREAM_NEXT_OFFSET).is_none());
    assert!(evicted.headers().get("stream-offset").is_none());

    let newest = post(2).await;
    assert_eq!(newest.status(), StatusCode::NO_CONTENT);
    assert_eq!(header_str(&newest, names::PRODUCER_SEQ), "2");
    assert!(newest.headers().get("producer-outcome").is_none());
    assert_eq!(
        newest.headers().get(names::STREAM_NEXT_OFFSET).cloned(),
        tail_offset
    );
    assert!(newest.headers().get("stream-offset").is_some());

    let after = http_head(&app, "/benchcmp/receipt-window").await;
    assert_eq!(
        after.headers().get(names::STREAM_NEXT_OFFSET).cloned(),
        tail_offset
    );
}

/// bounded-stream-state F11: ordinary reads are capped at 8 MiB, like
/// bootstrap. A capped offset read is partial and the continuation from
/// `Stream-Next-Offset` returns the rest. JSON offset reads at the cap are
/// covered by
/// `capped_and_uncapped_offset_reads_continue_exactly`.
#[tokio::test]
async fn reads_are_capped_at_the_server_response_limit() {
    const CAP: usize = 8 * 1024 * 1024;
    let app = test_router();

    // Byte stream: 9 MiB in one append.
    let response = http_put(
        &app,
        "/benchcmp/capped-bytes",
        &[(CONTENT_TYPE.as_str(), "application/octet-stream")],
        Body::empty(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::CREATED);
    let payload = vec![b'x'; CAP + 1024 * 1024];
    let response = http_post(
        &app,
        "/benchcmp/capped-bytes",
        &[(CONTENT_TYPE.as_str(), "application/octet-stream")],
        Body::from(payload.clone()),
    )
    .await;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    let response = http_get(&app, "/benchcmp/capped-bytes?offset=-1").await;
    assert_eq!(response.status(), StatusCode::OK);
    assert!(response.headers().get(names::STREAM_UP_TO_DATE).is_none());
    assert_eq!(
        header_str(&response, names::STREAM_NEXT_OFFSET),
        format!("{CAP:020}")
    );
    assert_eq!(body_bytes(response).await.len(), CAP);
    // A larger client max_bytes is clamped to the cap.
    let response = http_get(&app, "/benchcmp/capped-bytes?offset=-1&max_bytes=99999999").await;
    assert_eq!(body_bytes(response).await.len(), CAP);
    let response = http_get(&app, &format!("/benchcmp/capped-bytes?offset={CAP:020}")).await;
    assert_eq!(header_str(&response, names::STREAM_UP_TO_DATE), "true");
    assert_eq!(body_bytes(response).await.len(), payload.len() - CAP);
}

/// F11: a JSON offset read without `max_bytes`, or with `max_bytes` above
/// 8 MiB, may end inside a message at the cap; the continuation from
/// `Stream-Next-Offset` returns the rest.
#[tokio::test]
async fn a_capped_json_offset_read_may_end_mid_message() {
    const CAP: usize = 8 * 1024 * 1024;
    let app = test_router();
    let response = http_put(
        &app,
        "/benchcmp/capped-big-record",
        &[(CONTENT_TYPE.as_str(), "application/json")],
        Body::empty(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::CREATED);
    let big = format!(r#"{{"v":"{}"}}"#, "b".repeat(CAP + 4096));
    for body in [big.clone(), r#"{"v":"small"}"#.to_owned()] {
        let response = http_post(
            &app,
            "/benchcmp/capped-big-record",
            &[(CONTENT_TYPE.as_str(), "application/json")],
            Body::from(body),
        )
        .await;
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
    }
    let expected = format!("{big}\n{{\"v\":\"small\"}}\n");
    for query in ["", "&max_bytes=99999999"] {
        let response = http_get(
            &app,
            &format!("/benchcmp/capped-big-record?offset=-1{query}"),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        assert!(response.headers().get(names::STREAM_UP_TO_DATE).is_none());
        let next = header_str(&response, names::STREAM_NEXT_OFFSET).to_owned();
        assert_eq!(next, format!("{CAP:020}"));
        let first = body_bytes(response).await;
        assert_eq!(&first[..], &expected.as_bytes()[..CAP], "query={query}");
        let response = http_get(
            &app,
            &format!("/benchcmp/capped-big-record?offset={next}{query}"),
        )
        .await;
        assert_eq!(header_str(&response, names::STREAM_UP_TO_DATE), "true");
        assert_eq!(&body_bytes(response).await[..], &expected.as_bytes()[CAP..]);
    }
}

/// F11: catch-up and long-poll offset reads of a JSON stream that is closed
/// at its tail, uncapped (server cap only) and capped by `max_bytes`: every
/// page fits its cap, `Stream-Next-Offset` continues exactly after the last
/// returned byte, `Stream-Closed` appears only at the tail, and the pages
/// concatenate to the stream.
#[tokio::test]
async fn capped_and_uncapped_offset_reads_continue_exactly() {
    const CAP: usize = 8 * 1024 * 1024;
    let app = test_router();
    let response = http_put(
        &app,
        "/benchcmp/capped-consistent",
        &[(CONTENT_TYPE.as_str(), "application/json")],
        Body::empty(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::CREATED);
    // Records of varying size so the caps land inside a record.
    let records = (0..9_000)
        .map(|index| format!(r#"{{"i":{index},"v":"{}"}}"#, "v".repeat(900 + index % 257)))
        .collect::<Vec<_>>();
    for (index, chunk) in records.chunks(3_000).enumerate() {
        let close = index == 2;
        let mut headers = vec![(CONTENT_TYPE.as_str(), "application/json")];
        if close {
            headers.push((names::STREAM_CLOSED, "true"));
        }
        let response = http_post(
            &app,
            "/benchcmp/capped-consistent",
            &headers,
            Body::from(format!("[{}]", chunk.join(","))),
        )
        .await;
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
    }
    let expected = records
        .iter()
        .map(|record| format!("{record}\n"))
        .collect::<String>();

    for (cap, max_bytes) in [(CAP, ""), (1_000_003, "&max_bytes=1000003")] {
        for live in ["", "&live=long-poll"] {
            let mut offset = 0_u64;
            let mut seen = Vec::new();
            let mut pages = 0;
            loop {
                let response = http_get(
                    &app,
                    &format!("/benchcmp/capped-consistent?offset={offset:020}{max_bytes}{live}"),
                )
                .await;
                assert_eq!(response.status(), StatusCode::OK, "{max_bytes}{live}");
                pages += 1;
                let up_to_date = response.headers().get(names::STREAM_UP_TO_DATE).is_some();
                let closed = response.headers().get(names::STREAM_CLOSED).is_some();
                let next = header_str(&response, names::STREAM_NEXT_OFFSET)
                    .parse::<u64>()
                    .unwrap();
                let body = body_bytes(response).await;
                assert!(body.len() <= cap, "{max_bytes}{live}");
                assert_eq!(
                    next,
                    offset + u64::try_from(body.len()).unwrap(),
                    "{max_bytes}{live}: continuation follows the returned bytes"
                );
                assert_eq!(
                    closed, up_to_date,
                    "{max_bytes}{live}: Stream-Closed only at the tail"
                );
                seen.extend_from_slice(&body);
                offset = next;
                if up_to_date {
                    break;
                }
            }
            assert!(pages > 1, "{max_bytes}{live}: the read was capped");
            assert_eq!(seen, expected.as_bytes(), "{max_bytes}{live}");
        }
    }
}

/// Observe once before each independent test operation; restart races use explicit old pins.
async fn admin_test_request(
    client: &reqwest::Client,
    method: reqwest::Method,
    url: String,
) -> reqwest::RequestBuilder {
    let mut metrics_url = reqwest::Url::parse(&url).expect("admin URL");
    metrics_url.set_path("/__ursula/metrics");
    metrics_url.set_query(None);
    let metrics: serde_json::Value = client
        .get(metrics_url)
        .send()
        .await
        .expect("metrics")
        .json()
        .await
        .expect("JSON");
    client.request(method, url).header(
        PROCESS_INCARNATION_HEADER,
        metrics["process_incarnation"].as_str().expect("identity"),
    )
}

#[tokio::test]
async fn stale_process_admin_requests_cannot_change_a_replacement_or_clear_its_drain() {
    let runtime = spawn_runtime(
        &test_config(1, 1),
        Persistence::InMemory,
        Topology::SingleNode {
            raft_group_count: 1,
        },
    )
    .expect("runtime")
    .runtime;
    let old = HttpState::with_raft_registry(runtime.clone(), RaftGroupHandleRegistry::default());
    let registry = RaftGroupHandleRegistry::default();
    let replacement = HttpState::with_raft_registry(runtime, registry.clone());
    assert_ne!(old.process_incarnation, replacement.process_incarnation);
    assert_eq!(
        replacement.process_incarnation,
        replacement.clone().process_incarnation
    );
    let app = admin_router(replacement.clone());
    for (method, identity, status) in [
        ("POST", None, StatusCode::PRECONDITION_REQUIRED),
        ("POST", Some("malformed"), StatusCode::PRECONDITION_FAILED),
        (
            "POST",
            Some(old.process_incarnation.as_str()),
            StatusCode::PRECONDITION_FAILED,
        ),
    ] {
        let mut request = Request::builder()
            .method(method)
            .uri("/__ursula/leadership-shed/maintenance");
        if let Some(identity) = identity {
            request = request.header(PROCESS_INCARNATION_HEADER, identity);
        }
        let response = app
            .clone()
            .oneshot(request.body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), status);
        assert!(
            !registry.leadership_shed_state().is_shed(),
            "rejected request changed replacement state"
        );
    }
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/__ursula/leadership-shed/maintenance")
                .header(
                    PROCESS_INCARNATION_HEADER,
                    replacement.process_incarnation.as_str(),
                )
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert!(registry.leadership_shed_state().is_shed());
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/__ursula/leadership-shed/maintenance")
                .header(PROCESS_INCARNATION_HEADER, old.process_incarnation.as_str())
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::PRECONDITION_FAILED);
    assert!(
        registry.leadership_shed_state().is_shed(),
        "old executor cleared a replacement fence"
    );
    let response = app
        .oneshot(
            Request::builder()
                .uri("/__ursula/metrics")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let metrics: serde_json::Value = serde_json::from_slice(&body_bytes(response).await).unwrap();
    assert_eq!(
        metrics["process_incarnation"],
        replacement.process_incarnation.as_str()
    );
}

#[tokio::test]
async fn admin_mutation_without_observed_incarnation_is_rejected_before_drain() {
    let registry = RaftGroupHandleRegistry::default();
    let state = HttpState::with_raft_registry(
        spawn_runtime(
            &test_config(1, 1),
            Persistence::InMemory,
            Topology::SingleNode {
                raft_group_count: 1,
            },
        )
        .expect("runtime")
        .runtime,
        registry.clone(),
    );
    let response = admin_router(state)
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/__ursula/leadership-shed/maintenance")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::PRECONDITION_REQUIRED);
    assert!(!registry.leadership_shed_state().is_shed());
}

fn executor_fence_test_state() -> HttpState {
    HttpState::with_raft_registry(
        spawn_runtime(
            &test_config(1, 1),
            Persistence::InMemory,
            Topology::SingleNode {
                raft_group_count: 1,
            },
        )
        .expect("runtime")
        .runtime,
        RaftGroupHandleRegistry::default(),
    )
    .with_configured_node_id(1)
}

fn executor_token(generation: u64) -> MaintenanceFence {
    MaintenanceFence::new(
        format!("{:032x}", 1),
        format!("{generation:032x}"),
        generation,
    )
    .unwrap()
}

async fn executor_lifecycle(
    app: &Router,
    state: &HttpState,
    operation: &str,
    token: &MaintenanceFence,
) -> Response {
    app.clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/__ursula/maintenance/fence/{operation}"))
                .header(
                    PROCESS_INCARNATION_HEADER,
                    state.process_incarnation.as_str(),
                )
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(token).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap()
}

#[tokio::test]
async fn admin_executor_takeover_and_retirement_protect_real_drain_mutations() {
    let state = executor_fence_test_state();
    let registry = state.raft_registry.as_ref().unwrap();
    let app = admin_router(state.clone());
    let old = executor_token(1);
    let new = executor_token(2);
    assert_eq!(
        executor_lifecycle(&app, &state, "activate", &old)
            .await
            .status(),
        StatusCode::OK
    );
    assert_eq!(
        executor_lifecycle(&app, &state, "activate", &new)
            .await
            .status(),
        StatusCode::OK
    );
    for (header, expected) in [
        (None, StatusCode::PRECONDITION_REQUIRED),
        (Some(old.header_value()), StatusCode::PRECONDITION_FAILED),
        (
            Some("malformed".to_owned()),
            StatusCode::PRECONDITION_FAILED,
        ),
        (Some(new.header_value()), StatusCode::OK),
    ] {
        let mut request = Request::builder()
            .method("POST")
            .uri("/__ursula/leadership-shed/maintenance")
            .header(
                PROCESS_INCARNATION_HEADER,
                state.process_incarnation.as_str(),
            );
        if let Some(header) = header {
            request = request.header(MAINTENANCE_FENCE_HEADER, header);
        }
        let response = app
            .clone()
            .oneshot(request.body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), expected);
        assert_eq!(
            registry.leadership_shed_state().is_shed(),
            expected == StatusCode::OK
        );
    }
    assert_eq!(
        executor_lifecycle(&app, &state, "retire", &new)
            .await
            .status(),
        StatusCode::OK
    );
    assert_eq!(
        executor_lifecycle(&app, &state, "activate", &new)
            .await
            .status(),
        StatusCode::PRECONDITION_FAILED
    );
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/__ursula/leadership-shed/maintenance")
                .header(
                    PROCESS_INCARNATION_HEADER,
                    state.process_incarnation.as_str(),
                )
                .header(MAINTENANCE_FENCE_HEADER, new.header_value())
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::PRECONDITION_FAILED);
    assert!(registry.leadership_shed_state().is_shed());
    let response = app
        .oneshot(
            Request::builder()
                .uri("/__ursula/metrics")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body: serde_json::Value = serde_json::from_slice(&body_bytes(response).await).unwrap();
    assert_eq!(body["maintenance_fence"]["state"], "retired");
    assert_eq!(body["maintenance_fence_uncertain"], false);
}

#[tokio::test]
async fn malformed_present_executor_header_cannot_fall_back_to_uncertified_mode() {
    let state = executor_fence_test_state();
    let registry = state.raft_registry.as_ref().unwrap();
    let response = admin_router(state.clone())
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/__ursula/leadership-shed/maintenance")
                .header(
                    PROCESS_INCARNATION_HEADER,
                    state.process_incarnation.as_str(),
                )
                .header(
                    MAINTENANCE_FENCE_HEADER,
                    axum::http::HeaderValue::from_bytes(&[0xff]).unwrap(),
                )
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::PRECONDITION_FAILED);
    assert!(!registry.leadership_shed_state().is_shed());
}

#[tokio::test]
async fn cancelled_http_caller_cannot_release_an_unfinished_executor_mutation() {
    let state = executor_fence_test_state();
    let entered = Arc::new(tokio::sync::Notify::new());
    let finish = Arc::new(tokio::sync::Notify::new());
    let executed = Arc::new(AtomicU64::new(0));
    let handler_entered = entered.clone();
    let handler_finish = finish.clone();
    let handler_executed = executed.clone();
    let app = Router::new()
        .route(
            "/test-mutation",
            post(move || {
                let entered = handler_entered.clone();
                let finish = handler_finish.clone();
                let executed = handler_executed.clone();
                async move {
                    entered.notify_one();
                    finish.notified().await;
                    executed.fetch_add(1, Ordering::SeqCst);
                    StatusCode::OK
                }
            }),
        )
        .route_layer(middleware::from_fn_with_state(
            state.clone(),
            require_admin_incarnation,
        ));
    let identity = state.process_incarnation.clone();
    let caller = tokio::spawn(async move {
        app.oneshot(
            Request::builder()
                .method("POST")
                .uri("/test-mutation")
                .header(PROCESS_INCARNATION_HEADER, identity.as_str())
                .body(Body::empty())
                .unwrap(),
        )
        .await
    });
    entered.notified().await;
    caller.abort();
    assert!(caller.await.unwrap_err().is_cancelled());
    let takeover_state = state.clone();
    let mut takeover = tokio::spawn(async move {
        takeover_state
            .admin_fence
            .activate(executor_token(1), async { Ok(()) })
            .await
    });
    tokio::time::timeout(Duration::from_millis(30), &mut takeover)
        .await
        .expect_err("takeover must wait for the abandoned in-flight mutation");
    finish.notify_one();
    takeover
        .await
        .unwrap()
        .expect("takeover must activate once the in-flight mutation finishes");
    assert_eq!(executed.load(Ordering::SeqCst), 1);
    assert!(!state.admin_fence.is_uncertain());
}

#[test]
fn executor_json_requires_canonical_ids_and_nonzero_generation() {
    for body in [
        json!({"reservation_id": "bad", "executor_id": format!("{:032x}", 1), "generation": 1}),
        json!({"reservation_id": format!("{:032x}", 1), "executor_id": format!("{:032x}", 1), "generation": 0}),
        json!({"reservation_id": format!("{:032x}", 1), "executor_id": format!("{:032x}", 1), "generation": 1, "extra": true}),
    ] {
        serde_json::from_value::<MaintenanceFence>(body)
            .expect_err("a malformed executor token must be rejected");
    }
    let fence = executor_token(1);
    assert_eq!(
        serde_json::from_value::<MaintenanceFence>(serde_json::to_value(&fence).unwrap()).unwrap(),
        fence
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn executor_activation_waits_for_the_actual_raft_api_queue() {
    let wal_root = tempfile::tempdir().expect("WAL root");
    let registry = RaftGroupHandleRegistry::default();
    let mut config = RuntimeConfig::new(1, 1);
    config.threading = ursula_runtime::RuntimeThreading::HostedTokio;
    let (factory, raft_wal) = registered_durable_factory(&wal_root, &registry);
    let runtime = ShardRuntime::spawn_with_engine_factory(config, factory).expect("runtime");
    runtime
        .warm_group(RaftGroupId(0))
        .await
        .expect("warm group");
    let raft = registry.get(RaftGroupId(0)).unwrap();
    let (entered, observed) = tokio::sync::oneshot::channel();
    let (release, hold) = std::sync::mpsc::channel();
    raft.external_request(move |_| {
        entered.send(()).unwrap();
        hold.recv().unwrap();
    })
    .await
    .unwrap();
    observed.await.unwrap();
    let state = HttpState::with_raft_registry(runtime, registry.clone());
    let app = admin_router(state.clone());
    let activation_state = state.clone();
    let activation_app = app.clone();
    let mut activation = tokio::spawn(async move {
        executor_lifecycle(
            &activation_app,
            &activation_state,
            "activate",
            &executor_token(1),
        )
        .await
    });
    tokio::time::timeout(Duration::from_millis(30), &mut activation)
        .await
        .expect_err("activation must wait for the raft API queue");
    release.send(()).unwrap();
    assert_eq!(activation.await.unwrap().status(), StatusCode::OK);
    // Retirement does not depend on a running Raft core.
    raft.shutdown().await.unwrap();
    assert_eq!(
        executor_lifecycle(&app, &state, "retire", &executor_token(1))
            .await
            .status(),
        StatusCode::OK
    );
    shutdown_test_wal(&state.runtime, &raft_wal).await;
}

#[tokio::test]
async fn cli_uses_saved_executor_identity_and_never_refreshes_after_takeover() {
    let state = executor_fence_test_state();
    let registry = state.raft_registry.as_ref().unwrap().clone();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let app = admin_router(state.clone());
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let mut node = ursula_ctl::NodeInfo {
        id: 1,
        host: address.to_string(),
        admin_url: format!("http://{address}").parse().unwrap(),
        http_url: None,
        metrics_url: None,
        client_url: None,
        expected_process_incarnation: Some(state.process_incarnation.clone()),
        expected_maintenance_fence: Some(executor_token(1)),
    };
    let old_client = ursula_ctl::MetricsClient::new(Duration::from_secs(1)).unwrap();
    old_client
        .set_maintenance_fence(&node, false)
        .await
        .unwrap();
    old_client.set_maintenance_drain(&node, true).await.unwrap();
    assert!(registry.leadership_shed_state().is_shed());
    let old_node = node.clone();
    node.expected_maintenance_fence = Some(executor_token(2));
    let new_client = ursula_ctl::MetricsClient::new(Duration::from_secs(1)).unwrap();
    new_client
        .set_maintenance_fence(&node, false)
        .await
        .unwrap();
    assert!(
        old_client
            .set_maintenance_drain(&old_node, false)
            .await
            .unwrap_err()
            .to_string()
            .contains("412")
    );
    old_client
        .fetch_node(&old_node)
        .await
        .expect_err("the replaced executor must not read the node");
    old_client
        .pin_nodes(std::slice::from_ref(&old_node), None)
        .await
        .expect_err("the replaced executor must not pin the node");
    assert!(registry.leadership_shed_state().is_shed());
    new_client
        .set_maintenance_drain(&node, false)
        .await
        .unwrap();
    assert!(!registry.leadership_shed_state().is_shed());
    new_client.set_maintenance_fence(&node, true).await.unwrap();
    // A fresh invocation may retry retirement; it must never activate the
    // retired token or discover a new authority from reported server state.
    let resumed = ursula_ctl::MetricsClient::new(Duration::from_secs(1)).unwrap();
    resumed.set_maintenance_fence(&node, true).await.unwrap();
    resumed
        .set_maintenance_fence(&node, false)
        .await
        .expect_err("a retired token must never be activated again");
    old_client
        .set_maintenance_fence(&old_node, false)
        .await
        .expect_err("the replaced executor must stay rejected after retirement");
    assert!(resumed.set_maintenance_drain(&node, true).await.is_err());
    server.abort();
}

#[tokio::test]
async fn explicit_replacement_binding_preserves_token_and_rejects_another_executor() {
    let state = executor_fence_test_state();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let app = admin_router(state.clone());
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let node = ursula_ctl::NodeInfo {
        id: 1,
        host: address.to_string(),
        admin_url: format!("http://{address}").parse().unwrap(),
        http_url: None,
        metrics_url: None,
        client_url: None,
        expected_process_incarnation: Some(ProcessIncarnation::from_bits(0)),
        expected_maintenance_fence: Some(executor_token(1)),
    };
    let client = ursula_ctl::MetricsClient::new(Duration::from_secs(1)).unwrap();
    client
        .pin_nodes(std::slice::from_ref(&node), None)
        .await
        .expect_err("pinning without an explicit replacement must fail on an incarnation mismatch");
    let client = ursula_ctl::MetricsClient::new(Duration::from_secs(1)).unwrap();
    let pinned = client
        .pin_nodes(std::slice::from_ref(&node), Some(1))
        .await
        .unwrap();
    assert_eq!(
        pinned[0].expected_process_incarnation,
        Some(state.process_incarnation.clone())
    );
    assert_eq!(
        pinned[0].expected_maintenance_fence,
        node.expected_maintenance_fence
    );
    let mut other = pinned[0].clone();
    other.expected_maintenance_fence = Some(executor_token(2));
    client.set_maintenance_fence(&other, false).await.unwrap();
    let client = ursula_ctl::MetricsClient::new(Duration::from_secs(1)).unwrap();
    client
        .pin_nodes(&[node], Some(1))
        .await
        .expect_err("replacement must reject a node bound to another executor");
    server.abort();
}

/// Remove a temporary test file or directory, tolerating its absence.
pub(crate) fn remove_test_path(path: impl AsRef<std::path::Path>) {
    let path = path.as_ref();
    let removed = if path.is_dir() {
        std::fs::remove_dir_all(path)
    } else {
        std::fs::remove_file(path)
    };
    if let Err(err) = removed
        && err.kind() != std::io::ErrorKind::NotFound
    {
        panic!("remove test path {}: {err}", path.display());
    }
}

/// The bytes the segments of the core journal in `core_dir` hold beyond
/// their headers.
fn core_journal_record_bytes(core_dir: &std::path::Path) -> u64 {
    ursula_raft::wal::diagnostics::journal_segments(core_dir)
        .expect("list the core journal segments")
        .iter()
        .map(|(_, path)| {
            std::fs::metadata(path)
                .expect("segment metadata")
                .len()
                .saturating_sub(32)
        })
        .sum()
}

#[tokio::test]
async fn runtime_refuses_persisted_wal_topology_changes() {
    let dir = tempfile::tempdir().expect("WAL root");
    let original = ursula_shard::StaticShardMap::new(4, 8).unwrap();
    let wal = ursula_raft::RaftWal::start(dir.path(), ursula_config::WalFsync::Always, &original)
        .unwrap();
    wal.shutdown().await.unwrap();
    drop(wal);
    for (cores, groups) in [(8, 8), (4, 16)] {
        let error = spawn_runtime(
            &test_config(cores, groups),
            Persistence::Raft {
                log_dir: dir.path().to_owned(),
            },
            Topology::SingleNode {
                raft_group_count: groups,
            },
        )
        .unwrap_err();
        assert!(matches!(
            error,
            crate::SpawnRuntimeError::RaftWal(ursula_raft::RaftWalError::TopologyMismatch { .. })
        ));
    }
}

#[tokio::test]
async fn admin_quorum_proof_is_incarnation_bound_and_uses_registered_read_barrier() {
    let root = tempfile::tempdir().unwrap();
    let spawned = spawn_runtime(
        &test_config(1, 1),
        Persistence::Raft {
            log_dir: root.path().into(),
        },
        Topology::static_cluster(
            1,
            vec![(1, "http://127.0.0.1:4477".to_owned())],
            1,
            true,
            Default::default(),
        )
        .unwrap(),
    )
    .unwrap();
    let runtime = spawned.runtime;
    let registry = spawned.raft_registry.unwrap();
    runtime.warm_all_groups().await.unwrap();
    let raft = registry.get(RaftGroupId(0)).unwrap();
    raft.wait(Some(Duration::from_secs(5)))
        .current_leader(1, "single voter elected")
        .await
        .unwrap();
    let state = HttpState::with_raft_registry(runtime.clone(), registry);
    let identity = state.process_incarnation.clone();
    let admin = admin_router(state);
    let path = "/__ursula/raft/0/quorum";
    let unbound = admin
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(path)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(unbound.status(), StatusCode::PRECONDITION_REQUIRED);
    let response = send(
        &admin,
        "GET",
        path,
        &[(PROCESS_INCARNATION_HEADER, identity.as_str())],
        Body::empty(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let proof: ursula_proto::admin::QuorumPrefix =
        serde_json::from_slice(&body_bytes(response).await).unwrap();
    assert_eq!(proof.raft_group_id, 0);
    assert_eq!(proof.leader_id, 1);
    assert!(proof.leader_term > 0);
    raft.wait(Some(Duration::from_secs(5)))
        .applied_index_at_least(Some(proof.required_applied_index), "proof prefix applied")
        .await
        .unwrap();
    runtime.shutdown_group_engines().await.unwrap();
    spawned.raft_wal.unwrap().shutdown().await.unwrap();
}

/// The leader gains nothing from electing itself again, and OpenRaft would
/// run that election under its old leader state. Self-election on the leader
/// is refused before it reaches OpenRaft, as is one for a stale term, and the
/// leader keeps its vote and commits. A group the node does not host answers
/// 404. A replica whose Raft core stopped while it led keeps the leader's vote
/// in its metrics, and answers 503, not the leader's 409. A follower's
/// self-election is `self_election_elects_a_follower_unless_it_may_not_campaign`.
#[tokio::test]
async fn self_election_on_the_leader_is_refused_and_keeps_its_term() {
    let root = tempfile::tempdir().unwrap();
    let spawned = spawn_runtime(
        &test_config(1, 1),
        Persistence::Raft {
            log_dir: root.path().into(),
        },
        Topology::static_cluster(
            1,
            vec![(1, "http://127.0.0.1:4477".to_owned())],
            1,
            true,
            Default::default(),
        )
        .unwrap(),
    )
    .unwrap();
    let runtime = spawned.runtime;
    let registry = spawned.raft_registry.unwrap();
    runtime.warm_all_groups().await.unwrap();
    let raft = registry.get(RaftGroupId(0)).unwrap();
    raft.wait(Some(Duration::from_secs(5)))
        .current_leader(1, "single voter elected")
        .await
        .unwrap();
    let admin = admin_router(HttpState::with_raft_registry(runtime.clone(), registry));
    let observed = raft.metrics().borrow_watched().clone();
    let self_election = |current_term: u64| {
        http_post(
            &admin,
            "/__ursula/raft/0/self-election",
            &[("content-type", "application/json")],
            Body::from(serde_json::to_vec(&SelfElectionRequest { current_term }).unwrap()),
        )
    };
    assert_eq!(
        self_election(observed.current_term).await.status(),
        StatusCode::CONFLICT
    );
    assert_eq!(
        self_election(observed.current_term.checked_sub(1).unwrap())
            .await
            .status(),
        StatusCode::CONFLICT
    );
    runtime
        .create_stream(CreateStreamRequest::new(
            BucketStreamId::new("benchcmp", "after-self-election"),
            "application/octet-stream",
        ))
        .await
        .expect("the leader keeps committing");
    let after = raft.metrics().borrow_watched().clone();
    assert_eq!(after.vote, observed.vote);
    assert_eq!(after.current_term, observed.current_term);
    assert_eq!(after.current_leader, Some(1));
    let elsewhere = http_post(
        &admin,
        "/__ursula/raft/7/self-election",
        &[("content-type", "application/json")],
        Body::from(serde_json::to_vec(&SelfElectionRequest { current_term: 1 }).unwrap()),
    )
    .await;
    assert_eq!(elsewhere.status(), StatusCode::NOT_FOUND);
    // The core stops under the registered engine, which keeps its handle.
    raft.shutdown().await.unwrap();
    let stopped = raft.metrics().borrow_watched().clone();
    assert!(stopped.running_state.is_err());
    assert_eq!(stopped.vote, observed.vote);
    assert_eq!(
        self_election(observed.current_term).await.status(),
        StatusCode::SERVICE_UNAVAILABLE
    );
    runtime.shutdown_group_engines().await.unwrap();
    spawned.raft_wal.unwrap().shutdown().await.unwrap();
}

/// A follower's self-election is held to the policy of every leadership
/// transfer it receives: refused with 409 while it may not campaign (here a
/// maintenance drain), and once it may, taken with 204, after which it leads.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn self_election_elects_a_follower_unless_it_may_not_campaign() {
    let mut listeners = Vec::new();
    let mut peers = Vec::new();
    for node_id in 1..=3u64 {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind listener");
        let addr = listener.local_addr().expect("local addr");
        peers.push((node_id, format!("http://{addr}")));
        listeners.push(listener);
    }
    let mut nodes = Vec::new();
    for (index, listener) in listeners.into_iter().enumerate() {
        let node_id = u64::try_from(index + 1).expect("node id fits u64");
        nodes.push(
            spawn_static_grpc_test_node(
                node_id,
                listener,
                peers.clone(),
                peers.clone(),
                true,
                1,
                StaticGrpcTestNodeStorage {
                    per_group_initializers: true,
                    ..Default::default()
                },
            )
            .await,
        );
    }
    let group = RaftGroupId(0);
    for node in &nodes {
        tokio::time::timeout(Duration::from_secs(10), node.runtime.warm_all_groups())
            .await
            .expect("warm the groups in time")
            .expect("warm the groups");
    }
    for node in &nodes {
        node.registry
            .get(group)
            .expect("registered group")
            .wait(Some(Duration::from_secs(5)))
            .current_leader(1, "node 1 leads group 0")
            .await
            .expect("node 1 leads group 0");
    }
    // The followers' recovery gates open once they caught up: until then
    // they may not campaign either.
    let manifest = peers
        .iter()
        .map(|(id, endpoint)| ursula_ctl::NodeInfo {
            expected_process_incarnation: None,
            expected_maintenance_fence: None,
            id: *id,
            admin_url: endpoint.parse().unwrap(),
            http_url: Some(endpoint.parse().unwrap()),
            metrics_url: Some(endpoint.parse().unwrap()),
            client_url: None,
            host: endpoint.clone(),
        })
        .collect::<Vec<_>>();
    let client = ursula_ctl::MetricsClient::new(Duration::from_secs(1)).unwrap();
    ursula_ctl::wait_cluster_ready(
        "self-election fixture startup",
        &manifest,
        &client,
        Duration::from_secs(5),
        Duration::from_millis(10),
        1,
    )
    .await
    .unwrap();
    let follower = &nodes[1];
    let raft = follower.registry.get(group).expect("registered group");
    let admin = admin_router(HttpState::with_raft_registry(
        follower.runtime.clone(),
        follower.registry.clone(),
    ));
    let self_election = |current_term: u64| {
        http_post(
            &admin,
            "/__ursula/raft/0/self-election",
            &[("content-type", "application/json")],
            Body::from(serde_json::to_vec(&SelfElectionRequest { current_term }).unwrap()),
        )
    };
    follower
        .registry
        .mark_leadership_shed(ursula_raft::LeadershipShedReason::MaintenanceDrain);
    // The term is read first: a metrics guard held across the request would
    // block the Raft core's metrics update and, behind it, the handler's read.
    let term = raft.metrics().borrow_watched().current_term;
    let refused = self_election(term).await;
    assert_eq!(refused.status(), StatusCode::CONFLICT);
    let reason = String::from_utf8(body_bytes(refused).await.to_vec()).unwrap();
    assert!(reason.contains("may not campaign"), "{reason}");
    assert_eq!(raft.metrics().borrow_watched().current_leader, Some(1));
    follower
        .registry
        .clear_leadership_shed(ursula_raft::LeadershipShedReason::MaintenanceDrain);
    let term = raft.metrics().borrow_watched().current_term;
    let taken = self_election(term).await;
    let status = taken.status();
    let reason = String::from_utf8(body_bytes(taken).await.to_vec()).unwrap();
    assert_eq!(status, StatusCode::NO_CONTENT, "{reason}");
    raft.wait(Some(Duration::from_secs(5)))
        .current_leader(2, "the follower's self-election elects it")
        .await
        .expect("node 2 leads group 0");
    for node in nodes {
        node.shutdown().await;
    }
}

#[tokio::test]
async fn leadership_transfer_http_errors_have_precise_status_and_typed_rejections() {
    use ursula_proto::admin::TransferLeaderResponse;
    use ursula_proto::admin::TransferRejection;
    use ursula_raft::LeadershipTransferError;
    let group = RaftGroupId(0);
    for (error, status, reason) in [
        (
            LeadershipTransferError::NotRegistered { group },
            StatusCode::NOT_FOUND,
            TransferRejection::NotRegistered,
        ),
        (
            LeadershipTransferError::NotLeader { group },
            StatusCode::CONFLICT,
            TransferRejection::NotLeader,
        ),
        (
            LeadershipTransferError::InvalidTarget { group, target: 2 },
            StatusCode::BAD_REQUEST,
            TransferRejection::InvalidTarget,
        ),
        (
            LeadershipTransferError::UnreachableTarget {
                group,
                target: 2,
                ack_age: None,
            },
            StatusCode::CONFLICT,
            TransferRejection::UnreadyTarget,
        ),
        (
            LeadershipTransferError::LaggingTarget {
                group,
                target: 2,
                matched: Some(4),
                committed: Some(5),
            },
            StatusCode::CONFLICT,
            TransferRejection::UnreadyTarget,
        ),
        (
            LeadershipTransferError::Raft {
                group,
                source: openraft::error::Fatal::Stopped,
            },
            StatusCode::INTERNAL_SERVER_ERROR,
            TransferRejection::RaftStopped,
        ),
    ] {
        let response = transfer_raft_error_response(0, 1, 2, Some(1), error);
        assert_eq!(response.status(), status);
        let body: TransferLeaderResponse =
            serde_json::from_slice(&body_bytes(response).await).unwrap();
        assert_eq!(body.rejection, Some(reason));
        assert!(!body.transferred);
    }
}

/// Readiness names the first cause that applies, in a fixed order, and a
/// peer of another cluster is none of them: it is reported beside the
/// reason, so it neither hides a stall or a closed gate nor fails a healthy
/// node.
#[test]
fn readiness_reason_keeps_its_order_and_ignores_peers_of_another_cluster() {
    use ursula_proto::admin::ServingReadinessReason as Reason;

    use crate::ReadinessFacts;
    use crate::readiness_reason;
    let healthy = ReadinessFacts {
        recovery_ready: true,
        raft_ready: true,
        ..ReadinessFacts::default()
    };
    assert_eq!(readiness_reason(healthy), None);
    let mut facts = ReadinessFacts {
        format_epoch_mismatch: true,
        wal_disk_pressure: true,
        recovery_stalled: true,
        recovery_ready: false,
        raft_ready: false,
    };
    let mut order = Vec::new();
    while let Some(reason) = readiness_reason(facts) {
        order.push(reason.clone());
        match reason {
            Reason::FormatEpochMismatch => facts.format_epoch_mismatch = false,
            Reason::WalDiskPressure => facts.wal_disk_pressure = false,
            Reason::RecoveryStalled => facts.recovery_stalled = false,
            Reason::RecoveryGateClosed => facts.recovery_ready = true,
            Reason::RaftReplicaUnready => facts.raft_ready = true,
            Reason::Unknown => panic!("the server never names an unknown reason"),
        }
    }
    assert_eq!(order, vec![
        Reason::FormatEpochMismatch,
        Reason::WalDiskPressure,
        Reason::RecoveryStalled,
        Reason::RecoveryGateClosed,
        Reason::RaftReplicaUnready,
    ]);
}

/// Engine options with the recovery gate `gate`, as a node's engine factory
/// starts a gated replica.
fn gated_engine_options(
    gate: &Arc<ursula_raft::GroupRejoin>,
) -> ursula_raft::RaftGroupEngineOptions {
    let mut options = ursula_raft::RaftGroupEngineOptions::default();
    options.recovery = Some(gate.clone());
    options
}

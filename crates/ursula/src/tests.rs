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
use ursula_runtime::PlanGroupColdFlushRequest;
use ursula_runtime::RuntimeConfig;
use ursula_runtime::RuntimeError;
use ursula_runtime::StreamErrorCode;
use ursula_runtime::StreamErrorContext;
use ursula_shard::RaftGroupId;

use super::*;

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
        error: GroupEngineError::stream_from_replicated(
            "misleading message says NotFound",
            StreamErrorCode::StreamGone,
            None,
            Vec::new(),
        ),
    };

    assert_eq!(crate::render::runtime_error_status(&err), StatusCode::GONE);
}

#[test]
fn runtime_error_status_does_not_parse_infra_message_as_stream_error() {
    let err = RuntimeError::GroupEngine {
        core_id: ursula_shard::CoreId(0),
        raft_group_id: RaftGroupId(0),
        error: GroupEngineError::new("misleading infra message says StreamGone"),
    };

    assert_eq!(
        crate::render::runtime_error_status(&err),
        StatusCode::INTERNAL_SERVER_ERROR
    );
}

#[test]
fn group_engine_leader_hint_is_detected_without_matching_message() {
    let err = RuntimeError::GroupEngine {
        core_id: ursula_shard::CoreId(0),
        raft_group_id: RaftGroupId(0),
        error: GroupEngineError::forward_to_leader("forward request", None, None),
    };

    assert!(super::is_forward_to_leader(&err));
}

// A forward hinting this node (a read bounced back during a leadership
// transfer) must not redirect the client to itself: no 307, so the caller
// answers the leader-unknown 503. A hint naming a peer redirects there.
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
    let forward = |leader| RuntimeError::GroupEngine {
        core_id: ursula_shard::CoreId(0),
        raft_group_id: RaftGroupId(0),
        error: GroupEngineError::forward_to_leader_before_proposal(
            "not leader",
            Some(leader),
            None,
        ),
    };

    assert!(router.redirect_response(&forward(2), "/b/s").is_none());
    let redirect = router
        .redirect_response(&forward(1), "/b/s")
        .expect("a hint naming a peer redirects");
    assert_eq!(redirect.status(), StatusCode::TEMPORARY_REDIRECT);
    assert_eq!(redirect.headers()[LOCATION], "http://node-1/b/s");
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
        response.headers().get(HEADER_STREAM_CLOSED),
        Some(&axum::http::HeaderValue::from_static("true"))
    );
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
    ursula_raft::wal::RaftWal,
) {
    let raft_wal = ursula_raft::wal::RaftWal::start(
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
async fn shutdown_test_wal(runtime: &ShardRuntime, raft_wal: &ursula_raft::wal::RaftWal) {
    assert_eq!(
        crate::server::shutdown_raft_wal(runtime, Some(raft_wal)).await,
        crate::server::WalShutdown::Clean,
        "the Raft WAL shuts down cleanly"
    );
}

struct StaticGrpcTestNode {
    runtime: ShardRuntime,
    registry: RaftGroupHandleRegistry,
    shutdown: Option<tokio::sync::oneshot::Sender<()>>,
    server: tokio::task::JoinHandle<()>,
    raft_wal: ursula_raft::wal::RaftWal,
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
    engine_config: Option<ursula_raft::RaftEngineConfig>,
    per_group_initializers: bool,
    per_group_voters: BTreeMap<RaftGroupId, BTreeSet<u64>>,
    start_maintenance_drained: bool,
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
    let registry = RaftGroupHandleRegistry::default();
    if storage.start_maintenance_drained {
        registry.mark_leadership_shed(ursula_raft::LeadershipShedReason::MaintenanceDrain);
    }
    let mut config = RuntimeConfig::new(1, raft_group_count);
    config.threading = ursula_runtime::RuntimeThreading::HostedTokio;
    let (log_stores, wal_root) = match storage.raft_log_dir {
        Some(raft_log_dir) => (
            ursula_raft::wal::RaftWal::start(
                raft_log_dir,
                ursula_config::WalFsync::Always,
                &ursula_shard::StaticShardMap::new(1, raft_group_count).expect("valid topology"),
            )
            .expect("start the Raft WAL"),
            None,
        ),
        None => {
            let wal_root = tempfile::tempdir().expect("WAL root");
            (
                ursula_raft::wal::RaftWal::start(
                    wal_root.path(),
                    ursula_config::WalFsync::Never,
                    &ursula_shard::StaticShardMap::new(1, raft_group_count)
                        .expect("valid topology"),
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
    let runtime =
        ShardRuntime::spawn_with_engine_factory_and_cold_store(config, factory, storage.cold_store)
            .expect("runtime");
    let app = router_with_static_raft_cluster_topology(
        runtime.clone(),
        registry.clone(),
        node_id,
        router_peers,
        storage.per_group_voters,
    );
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
            .expect("serve static raft node");
    });
    StaticGrpcTestNode {
        runtime,
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
        header_str(&response, HEADER_STREAM_NEXT_OFFSET),
        "00000000000000000007"
    );

    let response = http_get(&app, "/benchcmp/stream-1?offset=2&max_bytes=3").await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(header_str(&response, CONTENT_TYPE), "text/plain");
    assert_eq!(
        header_str(&response, HEADER_STREAM_NEXT_OFFSET),
        "00000000000000000005"
    );
    let body = body_bytes(response).await;
    assert_eq!(&body[..], b"cde");

    let response = http_head(&app, "/benchcmp/stream-1").await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        header_str(&response, HEADER_STREAM_NEXT_OFFSET),
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
        &[(HEADER_STREAM_CLOSED, "true")],
        Body::empty(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    assert_eq!(header_str(&response, HEADER_STREAM_CLOSED), "true");

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
            (HEADER_STREAM_CLOSED, "true"),
        ],
        Body::from("final"),
    )
    .await;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    assert_eq!(
        header_str(&response, HEADER_STREAM_NEXT_OFFSET),
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
    assert_eq!(header_str(&response, HEADER_STREAM_CLOSED), "true");
    assert_eq!(
        header_str(&response, HEADER_STREAM_NEXT_OFFSET),
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
            (HEADER_PRODUCER_ID, "writer-1"),
            (HEADER_PRODUCER_EPOCH, "0"),
            (HEADER_PRODUCER_SEQ, "0"),
        ],
        Body::from("a"),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(header_str(&response, HEADER_PRODUCER_EPOCH), "0");
    assert_eq!(header_str(&response, HEADER_PRODUCER_SEQ), "0");

    let response = http_post(
        &app,
        "/benchcmp/producer-http",
        &[
            (CONTENT_TYPE.as_str(), "text/plain"),
            (HEADER_PRODUCER_ID, "writer-1"),
            (HEADER_PRODUCER_EPOCH, "0"),
            (HEADER_PRODUCER_SEQ, "0"),
        ],
        Body::from("ignored"),
    )
    .await;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    assert_eq!(
        header_str(&response, HEADER_STREAM_NEXT_OFFSET),
        "00000000000000000001"
    );
    assert_eq!(header_str(&response, HEADER_PRODUCER_EPOCH), "0");
    assert_eq!(header_str(&response, HEADER_PRODUCER_SEQ), "0");

    let response = http_get(&app, "/benchcmp/producer-http?offset=0&max_bytes=16").await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = body_bytes(response).await;
    assert_eq!(&body[..], b"a");

    let response = http_post(
        &app,
        "/benchcmp/producer-http",
        &[
            (CONTENT_TYPE.as_str(), "text/plain"),
            (HEADER_PRODUCER_ID, "writer-1"),
            (HEADER_PRODUCER_EPOCH, "0"),
            (HEADER_PRODUCER_SEQ, "2"),
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
            (HEADER_PRODUCER_ID, "writer-1"),
            (HEADER_PRODUCER_EPOCH, "1"),
            (HEADER_PRODUCER_SEQ, "0"),
        ],
        Body::from("b"),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(header_str(&response, HEADER_PRODUCER_EPOCH), "1");
    assert_eq!(header_str(&response, HEADER_PRODUCER_SEQ), "0");

    let response = http_post(
        &app,
        "/benchcmp/producer-http",
        &[
            (CONTENT_TYPE.as_str(), "text/plain"),
            (HEADER_PRODUCER_ID, "writer-1"),
            (HEADER_PRODUCER_EPOCH, "0"),
            (HEADER_PRODUCER_SEQ, "1"),
        ],
        Body::from("stale"),
    )
    .await;
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    assert_eq!(header_str(&response, HEADER_PRODUCER_EPOCH), "1");
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
        header_str(&response, HEADER_STREAM_NEXT_OFFSET),
        format!("{:020}", expected.len())
    );

    // Close the stream so the SSE response ends at the tail.
    let response = http_post(
        &app,
        "/benchcmp/json-fidelity",
        &[
            (CONTENT_TYPE.as_str(), "application/json"),
            (HEADER_STREAM_CLOSED, "true"),
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
        header_str(&response, HEADER_STREAM_NEXT_OFFSET),
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
        header_str(&response, HEADER_STREAM_NEXT_OFFSET),
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
        header_str(&response, HEADER_STREAM_NEXT_OFFSET),
        "00000000000000000000"
    );
    assert_eq!(header_str(&response, HEADER_STREAM_UP_TO_DATE), "true");
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
    let body = body_bytes(response).await;
    assert!(
        std::str::from_utf8(&body)
            .expect("utf8 body")
            .contains("live read waiters")
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
        header_str(&response, HEADER_STREAM_NEXT_OFFSET),
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
            (HEADER_STREAM_CLOSED, "true"),
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
    assert!(body.contains("\"sse_control_events\":1"));
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
            (HEADER_STREAM_CLOSED, "true"),
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
            (HEADER_STREAM_CLOSED, "true"),
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
        shutdown_runtime.stop_owner_services().await;
        shutdown_runtime
            .shutdown_group_engine(placement)
            .await
            .expect("shut down durable group before restart");
        raft_wal
            .shutdown()
            .await
            .expect("shut down the Raft WAL cleanly");
        assert!(shutdown_runtime.shutdown_owners().await.is_empty());
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
        let raft_wal = spawned.raft_wal.expect("restarted WAL");
        let opening = raft_wal.opening();
        assert_eq!(opening.previous_run, ursula_raft::wal::PreviousRun::Clean);
        assert_eq!(
            opening.replay_mode,
            ursula_raft::wal::JournalReplayMode::Strict
        );
        assert_eq!(opening.recovery, ursula_raft::wal::RecoveryState::Normal);
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
        shutdown_runtime.stop_owner_services().await;
        shutdown_runtime
            .shutdown_group_engine(placement)
            .await
            .expect("shut down restarted durable group");
        raft_wal.shutdown().await.expect("stop restarted WAL");
        assert!(shutdown_runtime.shutdown_owners().await.is_empty());
    }

    std::fs::remove_dir_all(&raft_root).expect("remove raft root");
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
    let app = router_with_raft_registry(runtime.clone(), registry);
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

    let mut network = ursula_raft::GrpcRaftNetwork::new(
        Arc::default(),
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
        Arc::default(),
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
            id: *id,
            admin_url: endpoint.parse().unwrap(),
            http_url: Some(endpoint.parse().unwrap()),
            metrics_url: Some(endpoint.parse().unwrap()),
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
    assert!(proof.participation_certified);
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
            .get(HEADER_STREAM_UP_TO_DATE)
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
            .get(HEADER_STREAM_NEXT_OFFSET)
            .and_then(|value| value.to_str().ok()),
        Some("00000000000000000019")
    );
    assert!(
        read.headers().get(HEADER_STREAM_UP_TO_DATE).is_none(),
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn static_grpc_node_that_loses_its_wal_twice_rejoins_without_membership_changes() {
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

    // The leaders rewind and snapshot the emptied node without changing voters.
    for (group_index, stream_id) in streams_by_group.iter().enumerate() {
        let raft_group_id = RaftGroupId(group_index as u32);
        stale_replacement
            .registry
            .get(raft_group_id)
            .expect("stale replacement group")
            .wait(Some(Duration::from_secs(30)))
            .metrics(
                |metrics| metrics.membership_config.voter_ids().any(|id| id == 3),
                format!("emptied node 3 healed back into group {group_index}"),
            )
            .await
            .expect("wait for node 3 self-heal");
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

    // Restarted empty a second time, it is rebuilt again.
    for (group_index, stream_id) in streams_by_group.iter().enumerate() {
        let raft_group_id = RaftGroupId(group_index as u32);
        let restarted_raft = restarted
            .registry
            .get(raft_group_id)
            .expect("restarted group");
        restarted_raft
            .wait(Some(Duration::from_secs(30)))
            .metrics(
                |metrics| metrics.membership_config.voter_ids().any(|id| id == 3),
                format!("restarted node 3 rejoined group {group_index}"),
            )
            .await
            .expect("wait for restarted node membership");
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
            .get(HEADER_STREAM_NEXT_OFFSET)
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
    assert_eq!(
        follower_sse
            .headers()
            .get(HEADER_URSULA_RAFT_LEADER_ID)
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
            .get(HEADER_STREAM_NEXT_OFFSET)
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
    // Election/recovery may have advanced beyond term 1. Snapshot replication
    // must carry the actual committed leader vote, just like AppendEntries.
    let proof = nodes[0]
        .registry
        .confirm_quorum_prefix(RaftGroupId(0))
        .await
        .expect("confirm snapshot sender is the current quorum leader");
    assert_eq!(proof.leader_id, 1);
    let vote = ursula_raft::UrsulaVote::new_committed(proof.leader_term, proof.leader_id);
    let mut snapshot_network =
        ursula_raft::GrpcRaftNetwork::new(Arc::default(), RaftGroupId(0), 2, peers[1].1.clone());
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
        .add_learner(3, openraft::BasicNode::new(peers[2].1.clone()), true)
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

    // This transport fixture drives OpenRaft membership directly. Production
    // HTTP membership changes are covered by the meta operation process drill.
    let promote = leader_raft
        .change_membership(BTreeSet::from([1, 2, 3]), false)
        .await
        .expect("promote caught-up learner");
    let promote_index = promote.log_id.index();
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
    let body = body_bytes(response).await;
    assert!(
        std::str::from_utf8(&body)
            .expect("utf8 body")
            .contains("ColdBackpressure")
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
        header_str(&response, HEADER_STREAM_SNAPSHOT_OFFSET),
        "00000000000000000003"
    );

    let response = http_head(&app, stream_uri).await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        header_str(&response, HEADER_STREAM_SNAPSHOT_OFFSET),
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
        header_str(&response, HEADER_STREAM_NEXT_OFFSET),
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
        header_str(&response, HEADER_STREAM_NEXT_OFFSET),
        "00000000000000000003"
    );

    let response = http_get(&app, "/benchcmp/snapshot-http/bootstrap").await;
    assert_eq!(response.status(), StatusCode::OK);
    assert!(
        header_str(&response, CONTENT_TYPE)
            .starts_with("multipart/mixed; boundary=ursula-bootstrap-")
    );
    assert_eq!(
        header_str(&response, HEADER_STREAM_NEXT_OFFSET),
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
            .get(HEADER_STREAM_SNAPSHOT_OFFSET)
            .is_none()
    );

    let response = http_get(&app, "/benchcmp/bootstrap-nosnapshot/bootstrap?live=sse").await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let response = http_get(&app, "/benchcmp/bootstrap-nosnapshot/bootstrap").await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(header_str(&response, HEADER_STREAM_SNAPSHOT_OFFSET), "-1");
    assert_eq!(
        header_str(&response, HEADER_STREAM_NEXT_OFFSET),
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
        header_str(&response, HEADER_STREAM_SNAPSHOT_OFFSET),
        "00000000000000000003"
    );
    assert_eq!(
        header_str(&response, HEADER_STREAM_NEXT_OFFSET),
        "00000000000000000003"
    );
    assert!(response.headers().get(HEADER_STREAM_UP_TO_DATE).is_none());
    assert!(response.headers().get(HEADER_STREAM_CLOSED).is_none());
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
        header_str(&response, HEADER_STREAM_NEXT_OFFSET),
        "00000000000000000012"
    );
    assert_eq!(header_str(&response, HEADER_STREAM_UP_TO_DATE), "true");
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
        header_str(&response, HEADER_STREAM_NEXT_OFFSET),
        "00000000000000000007"
    );
    assert_eq!(header_str(&response, HEADER_STREAM_UP_TO_DATE), "true");
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
    assert_eq!(header_str(&response, HEADER_STREAM_UP_TO_DATE), "true");
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
    let flushed_tail = header_str(&response, HEADER_STREAM_NEXT_OFFSET).to_owned();
    flush_cold(&app, stream_uri, 1024).await;

    let (response, parts) = bootstrap_get(&app, &format!("{stream_uri}/bootstrap")).await;
    assert_eq!(header_str(&response, HEADER_STREAM_SNAPSHOT_OFFSET), "-1");
    assert_eq!(
        header_str(&response, HEADER_STREAM_NEXT_OFFSET),
        "00000000000000000000"
    );
    assert!(response.headers().get(HEADER_STREAM_UP_TO_DATE).is_none());
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
    let tail = header_str(&response, HEADER_STREAM_NEXT_OFFSET).to_owned();
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
    assert_eq!(header_str(&response, HEADER_STREAM_NEXT_OFFSET), tail);
    assert_eq!(header_str(&response, HEADER_STREAM_UP_TO_DATE), "true");
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
    assert_eq!(header_str(&response, HEADER_STREAM_NEXT_OFFSET), tail);
    assert_eq!(header_str(&response, HEADER_STREAM_UP_TO_DATE), "true");
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
    assert!(head.headers().get(HEADER_STREAM_SNAPSHOT_OFFSET).is_none());

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
        header_str(&response, HEADER_STREAM_RETAINED_OFFSET),
        format!("{:020}", 8)
    );
    let response = put(format!("{stream_uri}/retention/{:020}", 24)).await;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    assert_eq!(
        header_str(&response, HEADER_STREAM_RETAINED_OFFSET),
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
    assert!(head.headers().get(HEADER_STREAM_SNAPSHOT_OFFSET).is_none());
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
    let snapshot_digest = header_str(&response, HEADER_STREAM_SNAPSHOT_DIGEST).to_owned();
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
        header_str(&response, HEADER_STREAM_SNAPSHOT_DIGEST),
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
        header_str(&response, HEADER_STREAM_SNAPSHOT_OFFSET),
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
    let old = header_str(&response, HEADER_STREAM_INCARNATION).to_owned();
    let tail = header_str(&response, HEADER_STREAM_NEXT_OFFSET).to_owned();

    let poll = tokio::spawn({
        let app = app.clone();
        let old = old.clone();
        async move {
            send(
                &app,
                "GET",
                &format!("{uri}?offset={tail}&live=long-poll&timeout_ms=30000"),
                &[(HEADER_STREAM_INCARNATION, old.as_str())],
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
    let current = header_str(&response, HEADER_STREAM_INCARNATION).to_owned();
    assert_ne!(current, old);

    let response = tokio::time::timeout(Duration::from_secs(10), poll)
        .await
        .expect("the pinned long-poll ends on recreate")
        .expect("poll task");
    assert_eq!(response.status(), StatusCode::PRECONDITION_FAILED);
    assert_eq!(header_str(&response, HEADER_STREAM_INCARNATION), current);
}

// Base-contract pin; see base_contract_tests.rs.
#[tokio::test]
async fn ingress_body_budget_rejects_write_when_budget_is_exhausted() {
    let app = Router::new()
        .route(
            "/write",
            post(|_body: Bytes| async { StatusCode::NO_CONTENT }),
        )
        .layer(middleware::from_fn_with_state(
            IngressAdmission {
                body_bytes: Arc::new(tokio::sync::Semaphore::new(4)),
                wal_disk: WalDiskMonitor::default(),
                raft_log: None,
            },
            ingress_admission_middleware,
        ));

    let response = http_post(
        &app,
        "/write",
        &[(CONTENT_LENGTH.as_str(), "5")],
        Body::from("abcde"),
    )
    .await;

    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(header_str(&response, axum::http::header::RETRY_AFTER), "1");
    let body = to_bytes(response.into_body(), MAX_HTTP_BODY_BYTES)
        .await
        .expect("body");
    assert!(
        std::str::from_utf8(&body)
            .expect("utf8 body")
            .contains("IngressBodyBytesLimitReached")
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
        .layer(middleware::from_fn_with_state(
            IngressAdmission {
                body_bytes: Arc::new(tokio::sync::Semaphore::new(4)),
                wal_disk: WalDiskMonitor::default(),
                raft_log: None,
            },
            ingress_admission_middleware,
        ));

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
    let store = ursula_raft::wal::RaftWal::start(
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
    let registry = RaftGroupHandleRegistry::default();
    let gate = Arc::new(
        ursula_raft::GroupRejoin::durable(1, RaftGroupId(0), &store)
            .await
            .expect("open the gate"),
    );
    let engine = ursula_raft::RaftGroupEngine::new_node(
        ursula_shard::ShardPlacement {
            core_id: ursula_shard::CoreId(0),
            shard_id: ursula_shard::ShardId(0),
            raft_group_id: RaftGroupId(0),
        },
        1,
        Arc::new(openraft::Config::default().validate().unwrap()),
        ursula_raft::SingleNodeRaftNetworkFactory,
        store,
        ursula_raft::RaftGroupEngineOptions::default(),
    )
    .await
    .expect("group");
    gate.bind(&engine.raft_handle());
    registry.register_engine(&engine, Some(gate));
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
    // The listener is available before this empty-WAL replica can vote.
    let metrics = http_get(&app, "/__ursula/metrics").await;
    assert_eq!(metrics.status(), StatusCode::OK);
    let metrics: ursula_proto::admin::NodeMetrics =
        serde_json::from_slice(&body_bytes(metrics).await).unwrap();
    assert!(
        metrics
            .diagnostics
            .recovery_gates
            .as_ref()
            .is_some_and(|report| !report.gated.is_empty())
    );
    let ready = http_get(&app, READINESS_PATH).await;
    assert_eq!(ready.status(), StatusCode::SERVICE_UNAVAILABLE);
    let body: serde_json::Value = serde_json::from_slice(&body_bytes(ready).await).unwrap();
    assert_eq!(body["reason"], json!("recovery_gate_closed"));
    assert_eq!(body["recovery_barriers_ready"], json!(false));
}

/// `accept-unsynced-loss` is an incarnation-bound admin mutation: it opens
/// the recovery gate of this node's replica once the gate reports its group
/// stalled, and only for the log the operator saw in the metrics. It
/// reports the replica's log and clears the readiness reason the closed gate
/// gave.
#[tokio::test]
async fn accept_unsynced_loss_opens_a_stalled_gate_for_the_observed_log_only() {
    let placement = ursula_shard::ShardPlacement {
        core_id: ursula_shard::CoreId(0),
        shard_id: ursula_shard::ShardId(0),
        raft_group_id: RaftGroupId(0),
    };
    // A replica on an empty WAL: its gate is closed until a barrier or an
    // operator opens it.
    let wal_root = tempfile::tempdir().expect("WAL root");
    let store = ursula_raft::wal::RaftWal::start(
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
    let gate = Arc::new(
        ursula_raft::GroupRejoin::durable(1, RaftGroupId(0), &store)
            .await
            .expect("open the gate"),
    );
    let engine = ursula_raft::RaftGroupEngine::new_single_node(
        placement,
        1,
        openraft::BasicNode::new("local"),
        Arc::new(openraft::Config::default().validate().unwrap()),
        store,
        ursula_raft::RaftGroupEngineOptions::default(),
    )
    .await
    .expect("single-node group");
    gate.bind(&engine.raft_handle());
    let registry = RaftGroupHandleRegistry::default();
    registry.register_engine(&engine, Some(gate.clone()));
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
    let ready = http_get(&client, READINESS_PATH).await;
    let body: serde_json::Value = serde_json::from_slice(&body_bytes(ready).await).unwrap();
    assert_eq!(body["reason"], json!("recovery_gate_closed"));

    let admin = admin_router(state.clone());
    let path = "/__ursula/raft/0/recovery/accept-unsynced-loss";
    let json_body = &[("content-type", "application/json")];
    let metrics: serde_json::Value =
        serde_json::from_slice(&body_bytes(http_get(&admin, "/__ursula/metrics").await).await)
            .expect("metrics JSON");
    let group = &metrics["raft_groups"][0];
    let seen = ursula_proto::admin::AcceptUnsyncedLossRequest {
        expected_last_log_index: group["last_log_index"].as_u64(),
        expected_current_term: group["current_term"].as_u64().expect("current term"),
    };
    let accept = |expected: ursula_proto::admin::AcceptUnsyncedLossRequest| {
        Body::from(serde_json::to_vec(&expected).expect("request JSON"))
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
    let other_log = ursula_proto::admin::AcceptUnsyncedLossRequest {
        expected_last_log_index: Some(
            seen.expected_last_log_index
                .map_or(0, |index| index.saturating_add(1)),
        ),
        ..seen
    };
    let changed = http_post(&admin, path, json_body, accept(other_log)).await;
    assert_eq!(changed.status(), StatusCode::CONFLICT);
    assert!(!gate.vote_gate_open(), "a refused request opened the gate");

    for expected in [
        ursula_raft::AcceptUnsyncedLossOutcome::GateOpened,
        ursula_raft::AcceptUnsyncedLossOutcome::AlreadyOpen,
    ] {
        let response = http_post(&admin, path, json_body, accept(seen)).await;
        assert_eq!(response.status(), StatusCode::OK);
        let report: ursula_raft::AcceptUnsyncedLossReport =
            serde_json::from_slice(&body_bytes(response).await).expect("typed report");
        assert_eq!(report.raft_group_id, 0);
        assert_eq!(report.node_id, 1);
        assert_eq!(report.outcome, expected);
        assert_eq!(report.last_log_index, seen.expected_last_log_index);
        assert_eq!(report.current_term, seen.expected_current_term);
    }
    assert!(gate.vote_gate_open());
    barrier
        .await
        .expect("the barrier driver ends once the gate opens");
    assert!(registry.recovery_barriers_ready());
    let ready = http_get(&client, READINESS_PATH).await;
    let body: serde_json::Value = serde_json::from_slice(&body_bytes(ready).await).unwrap();
    assert_eq!(body["recovery_barriers_ready"], json!(true));
    assert_eq!(body["recovery_stalled_groups"], json!([]));

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
    let body: serde_json::Value = serde_json::from_slice(&body_bytes(ready).await).unwrap();
    assert_eq!(body["reason"], json!("raft_replica_unready"));
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
    let body = body_bytes(write).await;
    assert!(
        std::str::from_utf8(&body)
            .expect("utf8 response")
            .contains("WalDiskPressure")
    );

    assert_eq!(
        monitor.observe_available(200),
        crate::wal_disk::WalDiskTransition::LeavePressure
    );
    assert_eq!(
        http_get(&app, READINESS_PATH).await.status(),
        StatusCode::OK
    );
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
    let body = body_bytes(write).await;
    assert!(
        std::str::from_utf8(&body)
            .expect("utf8 response")
            .contains("RaftLogPressure")
    );
    let bodiless = http_put(&app, "/benchcmp", &[], Body::empty()).await;
    assert_ne!(bodiless.status(), StatusCode::SERVICE_UNAVAILABLE);

    assert_eq!(coordinator.observe_log_bytes(50, 200, 100), Some(false));
    let write = http_put(
        &app,
        "/benchcmp/log-pressure",
        &[(CONTENT_LENGTH.as_str(), "1")],
        Body::from("x"),
    )
    .await;
    assert_ne!(write.status(), StatusCode::SERVICE_UNAVAILABLE);
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
    let progress = |index: u64| RaftLogProgressSnapshot { term: 1, index };
    RaftGroupMetricsSnapshot {
        installed_replica_identities: Default::default(),
        apply_failure: None,
        raft_group_id: group_id,
        node_id,
        current_term: 1,
        current_leader: leader,
        last_log_index: last_log,
        committed: committed.map(progress),
        last_applied: committed.map(progress),
        snapshot: snapshot.map(progress),
        purged: None,
        voter_ids: voters,
        learner_ids: vec![],
        maintenance: ursula_raft::RaftGroupMaintenanceState::default(),
        log: Default::default(),
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
    use std::collections::BTreeSet;

    use ursula_raft::RaftGroupMetricsSnapshot;
    use ursula_raft::snapshot_cadence::GroupLogProgress;
    use ursula_raft::snapshot_cadence::SnapshotCadence;

    use crate::bootstrap::group_log_progress;
    use crate::bootstrap::plan_snapshot_drive;
    use crate::bootstrap::resolve_snapshot_drive_interval_ms;

    const MIB: u64 = 1 << 20;

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
        }
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
            }),
        ];
        let (plan, selected) = plan_snapshot_drive(&snapshots, &cadence, 16, &BTreeSet::new());
        assert!(!plan.pressure);
        assert_eq!(
            selected
                .iter()
                .map(|snapshot| snapshot.raft_group_id)
                .collect::<Vec<_>>(),
            vec![0, 2]
        );
        let (_, one) = plan_snapshot_drive(&snapshots, &cadence, 1, &BTreeSet::new());
        assert_eq!(one.len(), 1);

        // A group the Raft WAL reports lagging goes first, though its own
        // cadence does not call for a snapshot yet.
        let lagging = BTreeSet::from([ursula_shard::RaftGroupId(1)]);
        let (_, selected) = plan_snapshot_drive(&snapshots, &cadence, 16, &lagging);
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
        let (plan, selected) = plan_snapshot_drive(&snapshots, &cadence, 16, &BTreeSet::new());
        assert!(plan.pressure);
        assert_eq!(
            selected
                .iter()
                .map(|snapshot| snapshot.raft_group_id)
                .collect::<Vec<_>>(),
            vec![0, 3]
        );
    }

    #[test]
    fn metrics_export_log_bytes_since_snapshot_and_last_snapshot_size() {
        // Bounded-state §7.5 soak gauges: unpurged log bytes and snapshot raw
        // bytes per group, scraped from `/__ursula/metrics`.
        let rendered = crate::render::render_raft_group_metrics_array(&[snap(
            3,
            Some(100),
            Some(40),
            GroupLogProgress {
                log_bytes: 5 * MIB,
                log_entries: 60,
                last_snapshot_bytes: 3 * MIB,
                has_snapshot: true,
            },
        )]);
        let group = &rendered[0];
        assert_eq!(group["raft_group_id"], 3);
        assert_eq!(group["log_bytes_since_snapshot"], 5 * MIB);
        assert_eq!(group["log_entries_since_snapshot"], 60);
        assert_eq!(group["last_snapshot_bytes"], 3 * MIB);
        assert_eq!(group["has_snapshot"], true);
    }
}

mod leadership_balance {
    use std::collections::HashSet;

    use ursula_raft::RaftGroupMetricsSnapshot;

    use crate::bootstrap::handoff_target_caught_up;
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
    fn balancer_hands_off_only_to_a_target_that_matched_the_leader_log() {
        // An empty restarted voter has matched nothing; handing it the group
        // leaves the leader muted in transfer while the target cannot win.
        assert!(!handoff_target_caught_up(Some(11), None));
        assert!(!handoff_target_caught_up(Some(11), Some(10)));
        assert!(handoff_target_caught_up(Some(11), Some(11)));
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
    use crate::bootstrap::ClusterEgressShedAction;
    use crate::bootstrap::cluster_egress_probe_groups;
    use crate::bootstrap::plan_cluster_egress_shed;

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
        let actions = plan_cluster_egress_shed(&snaps, 3, |_, target| target != 1);
        assert_eq!(actions, vec![ClusterEgressShedAction {
            group_id: 0,
            target: 2
        }]);
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

        let actions = plan_cluster_egress_shed(&snaps, 3, |_, _| true);

        assert_eq!(actions, vec![
            ClusterEgressShedAction {
                group_id: 0,
                target: 1,
            },
            ClusterEgressShedAction {
                group_id: 1,
                target: 1,
            },
            ClusterEgressShedAction {
                group_id: 3,
                target: 1,
            },
            ClusterEgressShedAction {
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

        let driver_runtime =
            ursula_runtime::ShardRuntime::spawn(ursula_runtime::RuntimeConfig::new(1, 2))
                .expect("driver runtime");
        crate::bootstrap::spawn_egress_gate(
            &driver_runtime,
            &registry,
            1,
            &peers,
            per_group_voters,
            &ursula_config::UrsulaConfig::default()
                .governance
                .cluster_probe,
        );
        tokio::time::sleep(Duration::from_millis(2_500)).await;

        driver_runtime.shutdown_owners().await;
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
            .get(HEADER_STREAM_SNAPSHOT_OFFSET)
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
/// format epoch (Ursula 0.5.x) or another epoch.
#[tokio::test]
async fn backup_import_refuses_groups_without_this_format_epoch() {
    let app = test_router();
    let without_epoch =
        rmp_serde::to_vec_named(&json!({"buckets": ["tenant-a"], "streams": []})).unwrap();
    let other_epoch = rmp_serde::to_vec_named(&ursula_runtime::StreamSnapshot {
        format_epoch: ursula_runtime::FORMAT_EPOCH - 1,
        ..ursula_runtime::StreamSnapshot::default()
    })
    .unwrap();
    for (body, expected) in [
        (without_epoch, "has no format_epoch"),
        (other_epoch, "unsupported format_epoch"),
    ] {
        let response = http_post(
            &app,
            "/__ursula/backup/group/0/import",
            &[(CONTENT_TYPE.as_str(), "application/x-msgpack")],
            Body::from(body),
        )
        .await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = body_bytes(response).await;
        let body = std::str::from_utf8(&body).expect("utf8");
        assert!(body.contains(expected), "{body}");
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

    // Export every group, checking transfer checksums like ursulactl does.
    let info = http_get(&source, "/__ursula/backup/info").await;
    assert_eq!(info.status(), StatusCode::OK);
    let info: serde_json::Value =
        serde_json::from_slice(&body_bytes(info).await).expect("backup info json");
    assert_eq!(info["format_version"], ursula_runtime::FORMAT_EPOCH);
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
        let response = http_post(
            &restored,
            &format!("/__ursula/backup/group/{group}/import"),
            &[(CONTENT_TYPE.as_str(), "application/x-msgpack")],
            Body::from(body.clone()),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK, "import group {group}");
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
    let snapshot_offset = header_str(&response, HEADER_STREAM_SNAPSHOT_OFFSET).to_owned();
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

    // Re-importing into a group that now owns a bucket fails closed.
    let mut conflicted = false;
    for (group, body) in exports.iter().enumerate() {
        let snapshot: ursula_runtime::StreamSnapshot =
            rmp_serde::from_slice(body).expect("decode export");
        if snapshot.buckets.is_empty() {
            continue;
        }
        let response = http_post(
            &restored,
            &format!("/__ursula/backup/group/{group}/import"),
            &[(CONTENT_TYPE.as_str(), "application/x-msgpack")],
            Body::from(body.clone()),
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
    let snapshot_offset = header_str(&response, HEADER_STREAM_SNAPSHOT_OFFSET).to_owned();

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

// Bounded-state §7.5: per-group state gauges exported in /__ursula/metrics.
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
    assert_eq!(group["receipts"], 3);
    assert_eq!(group["ttl_streams"], 1);
    assert!(group["ttl_heap_entries"].as_u64().expect("ttl heap") >= 1);
    // F6b: the three contiguous appends share one hot block.
    assert_eq!(group["hot_chunks"], 1);
    for key in [
        "shared_refs",
        "live_packs",
        "staged_external_refs",
        "receipt_items",
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
            (HEADER_PRODUCER_ID, id.to_owned()),
            (HEADER_PRODUCER_EPOCH, "0".to_owned()),
            (HEADER_PRODUCER_SEQ, "0".to_owned()),
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
    headers.push((HEADER_STREAM_CLOSED, "true".to_owned()));
    let response = http_post(&app, "/benchcmp/caps-a", &as_refs(&headers), Body::empty()).await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let response = http_post(
        &app,
        "/benchcmp/caps-a",
        &[
            (HEADER_STREAM_CLOSED, "true"),
            (HEADER_STREAM_SEQ, over_cap.as_str()),
        ],
        Body::empty(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

/// bounded-stream-state F3: a duplicate whose receipt the stream's receipt
/// window evicted answers `204` with `Producer-Seq` and without a byte
/// range, and never appends.
#[tokio::test]
async fn duplicate_beyond_receipt_window_answers_204_without_ranges() {
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
                    (HEADER_PRODUCER_ID, "writer"),
                    (HEADER_PRODUCER_EPOCH, "0"),
                    (HEADER_PRODUCER_SEQ, seq.as_str()),
                ],
                Body::from(r#"{"a":1}"#),
            )
            .await
        }
    };
    for seq in 0..1_026 {
        assert_eq!(post(seq).await.status(), StatusCode::OK);
    }
    let tail = http_head(&app, "/benchcmp/receipt-window").await;
    let tail_offset = tail.headers().get(HEADER_STREAM_NEXT_OFFSET).cloned();

    let evicted = post(0).await;
    assert_eq!(evicted.status(), StatusCode::NO_CONTENT);
    assert_eq!(
        evicted
            .headers()
            .get(HEADER_PRODUCER_SEQ)
            .and_then(|value| value.to_str().ok()),
        Some("0")
    );
    assert!(evicted.headers().get(HEADER_STREAM_NEXT_OFFSET).is_none());

    let newest = post(1_025).await;
    assert_eq!(newest.status(), StatusCode::NO_CONTENT);
    assert!(newest.headers().get(HEADER_STREAM_NEXT_OFFSET).is_some());

    let after = http_head(&app, "/benchcmp/receipt-window").await;
    assert_eq!(
        after.headers().get(HEADER_STREAM_NEXT_OFFSET).cloned(),
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
    assert!(response.headers().get(HEADER_STREAM_UP_TO_DATE).is_none());
    assert_eq!(
        header_str(&response, HEADER_STREAM_NEXT_OFFSET),
        format!("{CAP:020}")
    );
    assert_eq!(body_bytes(response).await.len(), CAP);
    // A larger client max_bytes is clamped to the cap.
    let response = http_get(&app, "/benchcmp/capped-bytes?offset=-1&max_bytes=99999999").await;
    assert_eq!(body_bytes(response).await.len(), CAP);
    let response = http_get(&app, &format!("/benchcmp/capped-bytes?offset={CAP:020}")).await;
    assert_eq!(header_str(&response, HEADER_STREAM_UP_TO_DATE), "true");
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
        assert!(response.headers().get(HEADER_STREAM_UP_TO_DATE).is_none());
        let next = header_str(&response, HEADER_STREAM_NEXT_OFFSET).to_owned();
        assert_eq!(next, format!("{CAP:020}"));
        let first = body_bytes(response).await;
        assert_eq!(&first[..], &expected.as_bytes()[..CAP], "query={query}");
        let response = http_get(
            &app,
            &format!("/benchcmp/capped-big-record?offset={next}{query}"),
        )
        .await;
        assert_eq!(header_str(&response, HEADER_STREAM_UP_TO_DATE), "true");
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
            headers.push((HEADER_STREAM_CLOSED, "true"));
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
                let up_to_date = response.headers().get(HEADER_STREAM_UP_TO_DATE).is_some();
                let closed = response.headers().get(HEADER_STREAM_CLOSED).is_some();
                let next = header_str(&response, HEADER_STREAM_NEXT_OFFSET)
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

#[tokio::test]
async fn membership_mutations_have_no_legacy_http_authority() {
    let runtime = spawn_runtime(
        &test_config(1, 1),
        Persistence::InMemory,
        Topology::SingleNode {
            raft_group_count: 1,
        },
    )
    .expect("runtime")
    .runtime;
    let state = HttpState::new(runtime);
    let app = admin_router(state.clone());
    for path in [
        "/__ursula/raft/0/membership?voters=1,2",
        "/__ursula/raft/0/learners/2?addr=http://node2",
        "/__ursula/maintenance/fence/activate",
        "/__ursula/maintenance/fence/retire",
    ] {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(path)
                    .header(
                        PROCESS_INCARNATION_HEADER,
                        state.process_incarnation.as_str(),
                    )
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND, "{path}");
    }
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
    ursula_raft::wal::journal_segments(core_dir)
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
    let wal =
        ursula_raft::wal::RaftWal::start(dir.path(), ursula_config::WalFsync::Always, &original)
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
            crate::SpawnRuntimeError::RaftWal(
                ursula_raft::wal::RaftWalError::TopologyMismatch { .. }
            )
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
            LeadershipTransferError::RecoveringTarget { group, target: 2 },
            StatusCode::CONFLICT,
            TransferRejection::RecoveringTarget,
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

#[tokio::test]
async fn maintenance_readiness_is_admin_only_and_honors_local_disk_health() {
    let monitor = WalDiskMonitor::new(100, 200);
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
    let client = client_router_with_admission(state.clone(), IngressAdmission::default());
    let admin = admin_router(state);
    let path = ursula_proto::admin::MAINTENANCE_READINESS_PATH;
    assert_eq!(
        http_get(&client, path).await.status(),
        StatusCode::NOT_FOUND
    );
    assert_eq!(http_get(&admin, path).await.status(), StatusCode::OK);
    monitor.observe_available(99);
    let response = http_get(&admin, path).await;
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let report: ursula_proto::admin::MaintenanceReadiness =
        serde_json::from_slice(&body_bytes(response).await).unwrap();
    assert!(!report.ready);
}

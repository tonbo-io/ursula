use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

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
                                    record: None,
                                    max_records: None,
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
fn parses_membership_voter_ids() {
    assert_eq!(
        parse_voter_ids("3,1,2").expect("parse voters"),
        BTreeSet::from([1, 2, 3])
    );
    assert!(parse_voter_ids("").is_err());
    assert!(parse_voter_ids("1,,2").is_err());
    assert!(parse_voter_ids("1,node-2").is_err());
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

struct StaticGrpcTestNode {
    runtime: ShardRuntime,
    registry: RaftGroupHandleRegistry,
    shutdown: Option<tokio::sync::oneshot::Sender<()>>,
    server: tokio::task::JoinHandle<()>,
}

impl StaticGrpcTestNode {
    async fn shutdown(mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        self.server.abort();
        let _ = self.server.await;
    }
}

#[derive(Default)]
struct StaticGrpcTestNodeStorage {
    raft_log_dir: Option<PathBuf>,
    cold_store: Option<ColdStoreHandle>,
    engine_config: Option<ursula_raft::RaftEngineConfig>,
    per_group_initializers: bool,
    per_group_voters: BTreeMap<RaftGroupId, BTreeSet<u64>>,
}

async fn spawn_static_grpc_test_node(
    node_id: u64,
    listener: tokio::net::TcpListener,
    factory_peers: Vec<(u64, String)>,
    router_peers: Vec<(u64, String)>,
    initialize_membership: bool,
    raft_group_count: usize,
) -> StaticGrpcTestNode {
    spawn_static_grpc_test_node_with_log_dir(
        node_id,
        listener,
        factory_peers,
        router_peers,
        initialize_membership,
        raft_group_count,
        None,
    )
    .await
}

async fn spawn_static_grpc_test_node_with_log_dir(
    node_id: u64,
    listener: tokio::net::TcpListener,
    factory_peers: Vec<(u64, String)>,
    router_peers: Vec<(u64, String)>,
    initialize_membership: bool,
    raft_group_count: usize,
    raft_log_dir: Option<PathBuf>,
) -> StaticGrpcTestNode {
    spawn_static_grpc_test_node_with_storage(
        node_id,
        listener,
        factory_peers,
        router_peers,
        initialize_membership,
        raft_group_count,
        StaticGrpcTestNodeStorage {
            raft_log_dir,
            cold_store: None,
            engine_config: None,
            per_group_initializers: false,
            per_group_voters: BTreeMap::new(),
        },
    )
    .await
}

async fn spawn_static_grpc_test_node_with_per_group_initializers(
    node_id: u64,
    listener: tokio::net::TcpListener,
    factory_peers: Vec<(u64, String)>,
    router_peers: Vec<(u64, String)>,
    initialize_membership: bool,
    raft_group_count: usize,
) -> StaticGrpcTestNode {
    spawn_static_grpc_test_node_with_storage(
        node_id,
        listener,
        factory_peers,
        router_peers,
        initialize_membership,
        raft_group_count,
        StaticGrpcTestNodeStorage {
            raft_log_dir: None,
            cold_store: None,
            engine_config: None,
            per_group_initializers: true,
            per_group_voters: BTreeMap::new(),
        },
    )
    .await
}

async fn spawn_static_grpc_test_node_with_storage(
    node_id: u64,
    listener: tokio::net::TcpListener,
    factory_peers: Vec<(u64, String)>,
    router_peers: Vec<(u64, String)>,
    initialize_membership: bool,
    raft_group_count: usize,
    storage: StaticGrpcTestNodeStorage,
) -> StaticGrpcTestNode {
    let registry = RaftGroupHandleRegistry::default();
    let mut config = RuntimeConfig::new(1, raft_group_count);
    config.threading = ursula_runtime::RuntimeThreading::HostedTokio;
    let mut factory = StaticGrpcRaftGroupEngineFactory::new(
        node_id,
        factory_peers,
        initialize_membership,
        registry.clone(),
    );
    factory = factory.with_per_group_membership_initializers(storage.per_group_initializers);
    factory = factory.with_per_group_voters(storage.per_group_voters.clone());
    factory = factory.with_cold_store(storage.cold_store.clone());
    if let Some(engine_config) = storage.engine_config {
        factory = factory.with_engine_config(engine_config);
    }
    if let Some(raft_log_dir) = storage.raft_log_dir {
        factory = factory.with_raft_log_dir(raft_log_dir);
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
                let _ = shutdown_rx.await;
            })
            .await
            .expect("serve static raft node");
    });
    StaticGrpcTestNode {
        runtime,
        registry,
        shutdown: Some(shutdown_tx),
        server,
    }
}

#[tokio::test]
async fn create_append_read_and_head_match_perf_compare_subset() {
    let app = test_router();

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri("/benchcmp/stream-1")
                .header(CONTENT_TYPE, "text/plain")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::CREATED);
    assert_eq!(response.headers().get(CONTENT_TYPE).unwrap(), "text/plain");

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/benchcmp/stream-1")
                .header(CONTENT_TYPE, "text/plain")
                .body(Body::from("abcdefg"))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    assert_eq!(
        response.headers().get(HEADER_STREAM_NEXT_OFFSET).unwrap(),
        "00000000000000000007"
    );

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/benchcmp/stream-1?offset=2&max_bytes=3")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get(CONTENT_TYPE).unwrap(), "text/plain");
    assert_eq!(
        response.headers().get(HEADER_STREAM_NEXT_OFFSET).unwrap(),
        "00000000000000000005"
    );
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    assert_eq!(&body[..], b"cde");

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("HEAD")
                .uri("/benchcmp/stream-1")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get(HEADER_STREAM_NEXT_OFFSET).unwrap(),
        "00000000000000000007"
    );
    assert_eq!(response.headers().get(CONTENT_TYPE).unwrap(), "text/plain");
    assert_eq!(
        response
            .headers()
            .get(HEADER_STREAM_INTEGRITY_LIVE_RECORDS)
            .unwrap(),
        "1"
    );
    assert_eq!(
        response
            .headers()
            .get(HEADER_STREAM_INTEGRITY_TOTAL_RECORDS)
            .unwrap(),
        "1"
    );
    assert_eq!(
        response
            .headers()
            .get(HEADER_STREAM_INTEGRITY_EVICTED_RECORDS)
            .unwrap(),
        "0"
    );
    assert!(
        response
            .headers()
            .get(HEADER_STREAM_INTEGRITY_LIVE_SETSUM)
            .is_some()
    );
}

#[tokio::test]
async fn stream_attrs_can_be_updated_and_read_over_http() {
    let app = test_router();

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri("/benchcmp/attrs-http")
                .header(CONTENT_TYPE, "text/plain")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::CREATED);

    let attrs = json!({
        "title": "Support session",
        "metadata": {
            "agent": { "id": "agent-1", "version": 2 },
            "purpose": "customer-support"
        }
    });
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri("/benchcmp/attrs-http/attrs")
                .header(CONTENT_TYPE, "application/json")
                .body(Body::from(attrs.to_string()))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/benchcmp/attrs-http/attrs")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get(CONTENT_TYPE).unwrap(),
        "application/json"
    );
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    let actual: serde_json::Value = serde_json::from_slice(&body).expect("attrs json");
    assert_eq!(actual, attrs);

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri("/benchcmp/attrs-http/attrs")
                .header(CONTENT_TYPE, "application/json")
                .body(Body::from("{}"))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/benchcmp/attrs-http/attrs")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    let actual: serde_json::Value = serde_json::from_slice(&body).expect("attrs json");
    assert_eq!(actual, json!({}));

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri("/benchcmp/attrs-http")
                .header(CONTENT_TYPE, "text/plain")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn stream_attrs_can_be_set_when_creating_stream_over_http() {
    let app = test_router();
    let attrs = json!({
        "title": "Created session",
        "metadata": {
            "agent": { "id": "agent-create", "version": 2 },
            "purpose": "create-time"
        }
    });

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri("/benchcmp/attrs-create-http")
                .header(CONTENT_TYPE, "text/plain")
                .header("stream-attrs", attrs.to_string())
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::CREATED);

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/benchcmp/attrs-create-http/attrs")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    let actual: serde_json::Value = serde_json::from_slice(&body).expect("attrs json");
    assert_eq!(actual, attrs);
}

#[tokio::test]
async fn stream_attrs_endpoints_return_not_found_for_missing_stream() {
    let app = test_router();

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/benchcmp/attrs-missing/attrs")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri("/benchcmp/attrs-missing/attrs")
                .header(CONTENT_TYPE, "application/json")
                .body(Body::from(r#"{"title":"missing"}"#))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn create_stream_rejects_invalid_stream_attrs_header() {
    let app = test_router();

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri("/benchcmp/attrs-bad-header")
                .header(CONTENT_TYPE, "text/plain")
                .header("stream-attrs", "{not json")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri("/benchcmp/attrs-bad-header/attrs")
                .header(CONTENT_TYPE, "application/json5")
                .body(Body::from(r#"{"title":"json5"}"#))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn close_only_post_sets_closed_state_and_rejects_later_append() {
    let app = test_router();

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri("/benchcmp/closing")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    let status = response.status();
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    assert_eq!(
        status,
        StatusCode::CREATED,
        "create body={}",
        std::str::from_utf8(&body).unwrap_or("<non-utf8>")
    );

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/benchcmp/closing")
                .header(HEADER_STREAM_CLOSED, "true")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    assert_eq!(
        response.headers().get(HEADER_STREAM_CLOSED).unwrap(),
        "true"
    );

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/benchcmp/closing")
                .header(CONTENT_TYPE, DEFAULT_CONTENT_TYPE)
                .body(Body::from("x"))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::CONFLICT);
}

#[tokio::test]
async fn append_conflict_precedence_reports_closed_header_before_mismatch_or_seq() {
    let app = test_router();

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri("/benchcmp/closed-precedence")
                .header(CONTENT_TYPE, "text/plain")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::CREATED);

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/benchcmp/closed-precedence")
                .header(CONTENT_TYPE, "text/plain")
                .header(HEADER_STREAM_CLOSED, "true")
                .body(Body::from("final"))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    assert_eq!(
        response.headers().get(HEADER_STREAM_NEXT_OFFSET).unwrap(),
        "00000000000000000005"
    );

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/benchcmp/closed-precedence")
                .header(CONTENT_TYPE, "application/octet-stream")
                .header(HEADER_STREAM_SEQ, "0001")
                .body(Body::from("too-late"))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::CONFLICT);
    assert_eq!(
        response.headers().get(HEADER_STREAM_CLOSED).unwrap(),
        "true"
    );
    assert_eq!(
        response.headers().get(HEADER_STREAM_NEXT_OFFSET).unwrap(),
        "00000000000000000005"
    );
}

#[tokio::test]
async fn stream_seq_header_rejects_regressing_appends() {
    let app = test_router();

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri("/benchcmp/seq-stream")
                .header(CONTENT_TYPE, "text/plain")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::CREATED);

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/benchcmp/seq-stream")
                .header(CONTENT_TYPE, "text/plain")
                .header(HEADER_STREAM_SEQ, "0002")
                .body(Body::from("a"))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/benchcmp/seq-stream")
                .header(CONTENT_TYPE, "text/plain")
                .header(HEADER_STREAM_SEQ, "0002")
                .body(Body::from("b"))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::CONFLICT);

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/benchcmp/seq-stream")
                .header(CONTENT_TYPE, "text/plain")
                .header(HEADER_STREAM_SEQ, "0003")
                .body(Body::from("c"))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
}

#[tokio::test]
async fn producer_headers_deduplicate_retries_and_fence_stale_epochs() {
    let app = test_router();

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri("/benchcmp/producer-http")
                .header(CONTENT_TYPE, "text/plain")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::CREATED);

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/benchcmp/producer-http")
                .header(CONTENT_TYPE, "text/plain")
                .header(HEADER_PRODUCER_ID, "writer-1")
                .header(HEADER_PRODUCER_EPOCH, "0")
                .header(HEADER_PRODUCER_SEQ, "0")
                .body(Body::from("a"))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get(HEADER_PRODUCER_EPOCH).unwrap(), "0");
    assert_eq!(response.headers().get(HEADER_PRODUCER_SEQ).unwrap(), "0");

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/benchcmp/producer-http")
                .header(CONTENT_TYPE, "text/plain")
                .header(HEADER_PRODUCER_ID, "writer-1")
                .header(HEADER_PRODUCER_EPOCH, "0")
                .header(HEADER_PRODUCER_SEQ, "0")
                .body(Body::from("ignored"))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    assert_eq!(
        response.headers().get(HEADER_STREAM_NEXT_OFFSET).unwrap(),
        "00000000000000000001"
    );
    assert_eq!(response.headers().get(HEADER_PRODUCER_EPOCH).unwrap(), "0");
    assert_eq!(response.headers().get(HEADER_PRODUCER_SEQ).unwrap(), "0");

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/benchcmp/producer-http?offset=0&max_bytes=16")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    assert_eq!(&body[..], b"a");

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/benchcmp/producer-http")
                .header(CONTENT_TYPE, "text/plain")
                .header(HEADER_PRODUCER_ID, "writer-1")
                .header(HEADER_PRODUCER_EPOCH, "0")
                .header(HEADER_PRODUCER_SEQ, "2")
                .body(Body::from("gap"))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::CONFLICT);
    assert_eq!(
        response.headers().get("producer-expected-seq").unwrap(),
        "1"
    );
    assert_eq!(
        response.headers().get("producer-received-seq").unwrap(),
        "2"
    );

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/benchcmp/producer-http")
                .header(CONTENT_TYPE, "text/plain")
                .header(HEADER_PRODUCER_ID, "writer-1")
                .header(HEADER_PRODUCER_EPOCH, "1")
                .header(HEADER_PRODUCER_SEQ, "0")
                .body(Body::from("b"))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get(HEADER_PRODUCER_EPOCH).unwrap(), "1");
    assert_eq!(response.headers().get(HEADER_PRODUCER_SEQ).unwrap(), "0");

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/benchcmp/producer-http")
                .header(CONTENT_TYPE, "text/plain")
                .header(HEADER_PRODUCER_ID, "writer-1")
                .header(HEADER_PRODUCER_EPOCH, "0")
                .header(HEADER_PRODUCER_SEQ, "1")
                .body(Body::from("stale"))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    assert_eq!(response.headers().get(HEADER_PRODUCER_EPOCH).unwrap(), "1");
}

#[tokio::test]
async fn delete_stream_removes_http_visible_state() {
    let app = test_router();

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri("/benchcmp/delete-http")
                .header(CONTENT_TYPE, "text/plain")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::CREATED);

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/benchcmp/delete-http")
                .header(CONTENT_TYPE, "text/plain")
                .body(Body::from("payload"))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/benchcmp/delete-http")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("HEAD")
                .uri("/benchcmp/delete-http")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/benchcmp/delete-http")
                .header(CONTENT_TYPE, "text/plain")
                .body(Body::from("x"))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn append_batch_matches_perf_compare_frame_format() {
    let app = test_router();

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri("/benchcmp/batch-stream")
                .header(CONTENT_TYPE, "text/plain")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::CREATED);

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/benchcmp/batch-stream/append-batch")
                .header(CONTENT_TYPE, "text/plain")
                .body(Body::from(batch_body(&[
                    b"abc".as_slice(),
                    b"de".as_slice(),
                ])))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    assert_eq!(&body[..], br#"[{"status":204},{"status":204}]"#);

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/benchcmp/batch-stream/append-batch")
                .header(CONTENT_TYPE, "text/plain")
                .body(Body::from(batch_body(&[b"".as_slice(), b"f".as_slice()])))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    assert_eq!(&body[..], br#"[{"status":400},{"status":204}]"#);

    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/benchcmp/batch-stream?offset=0&max_bytes=8")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get(HEADER_STREAM_NEXT_OFFSET).unwrap(),
        "00000000000000000006"
    );
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    assert_eq!(&body[..], b"abcdef");
}

#[tokio::test]
async fn append_batch_minimal_ack_skips_success_body_but_keeps_item_errors() {
    let app = test_router();

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri("/benchcmp/batch-minimal")
                .header(CONTENT_TYPE, "text/plain")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::CREATED);

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/benchcmp/batch-minimal/append-batch")
                .header(CONTENT_TYPE, "text/plain")
                .header(HEADER_PREFER, "return=minimal")
                .body(Body::from(batch_body(&[b"a".as_slice(), b"b".as_slice()])))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    assert!(body.is_empty());

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/benchcmp/batch-minimal/append-batch")
                .header(CONTENT_TYPE, "text/plain")
                .header(HEADER_PREFER, "return=minimal")
                .body(Body::from(batch_body(&[b"".as_slice(), b"c".as_slice()])))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    assert_eq!(&body[..], br#"[{"status":400},{"status":204}]"#);

    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/benchcmp/batch-minimal?offset=0&max_bytes=16")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    assert_eq!(&body[..], b"abc");
}

#[tokio::test]
async fn json_append_batch_returns_per_frame_record_ranges() {
    let app = test_router();
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri("/benchcmp/json-batch-records")
                .header(CONTENT_TYPE, "application/json")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::CREATED);

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/benchcmp/json-batch-records/append-batch")
                .header(CONTENT_TYPE, "application/json")
                .header(HEADER_PREFER, "return=minimal")
                .header(HEADER_PRODUCER_ID, "json-writer")
                .header(HEADER_PRODUCER_EPOCH, "0")
                .header(HEADER_PRODUCER_SEQ, "0")
                .body(Body::from(batch_body(&[
                    br#"[{"id":1},{"id":2}]"#.as_slice(),
                    br#"{"id":3}"#.as_slice(),
                ])))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get(HEADER_STREAM_EXTENSIONS).unwrap(),
        JSON_RECORD_COORDINATES_EXTENSION
    );
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    let body: serde_json::Value = serde_json::from_slice(&body).expect("batch ack JSON");
    assert_eq!(
        body,
        serde_json::json!([
            {"status": 204, "stream_record_start": 0, "stream_record_next": 2},
            {"status": 204, "stream_record_start": 2, "stream_record_next": 3}
        ])
    );

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/benchcmp/json-batch-records/append-batch")
                .header(CONTENT_TYPE, "application/json")
                .header(HEADER_PREFER, "return=minimal")
                .header(HEADER_PRODUCER_ID, "json-writer")
                .header(HEADER_PRODUCER_EPOCH, "0")
                .header(HEADER_PRODUCER_SEQ, "0")
                .body(Body::from(batch_body(&[br#"{"ignored":true}"#.as_slice()])))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get(HEADER_STREAM_EXTENSIONS).unwrap(),
        JSON_RECORD_COORDINATES_EXTENSION
    );
    let retry_body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    let retry_body: serde_json::Value =
        serde_json::from_slice(&retry_body).expect("deduplicated batch ack JSON");
    assert_eq!(retry_body, body);
}

#[tokio::test]
async fn closed_record_long_poll_empty_response_includes_record_headers() {
    let app = test_router();
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri("/benchcmp/json-record-closed-long-poll")
                .header(CONTENT_TYPE, "application/json")
                .header(HEADER_STREAM_CLOSED, "true")
                .body(Body::from(r#"{"id":1}"#))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::CREATED);

    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(
                    "/benchcmp/json-record-closed-long-poll?record=1&live=long-poll&timeout_ms=1000",
                )
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    assert_eq!(
        response.headers().get(HEADER_STREAM_EXTENSIONS).unwrap(),
        JSON_RECORD_COORDINATES_EXTENSION
    );
    assert_eq!(
        response.headers().get(HEADER_STREAM_RECORD_FIRST).unwrap(),
        "0"
    );
    assert_eq!(
        response.headers().get(HEADER_STREAM_RECORD_START).unwrap(),
        "1"
    );
    assert_eq!(
        response.headers().get(HEADER_STREAM_RECORD_NEXT).unwrap(),
        "1"
    );
}

#[tokio::test]
async fn append_batch_producer_headers_deduplicate_retries() {
    let app = test_router();

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri("/benchcmp/batch-producer")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::CREATED);

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/benchcmp/batch-producer/append-batch")
                .header(HEADER_PRODUCER_ID, "writer-1")
                .header(HEADER_PRODUCER_EPOCH, "0")
                .header(HEADER_PRODUCER_SEQ, "0")
                .body(Body::from(batch_body(&[b"ab".as_slice(), b"c".as_slice()])))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get(HEADER_PRODUCER_EPOCH).unwrap(), "0");
    assert_eq!(response.headers().get(HEADER_PRODUCER_SEQ).unwrap(), "0");
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    assert_eq!(&body[..], br#"[{"status":204},{"status":204}]"#);

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/benchcmp/batch-producer/append-batch")
                .header(HEADER_PRODUCER_ID, "writer-1")
                .header(HEADER_PRODUCER_EPOCH, "0")
                .header(HEADER_PRODUCER_SEQ, "0")
                .body(Body::from(batch_body(&[b"ignored".as_slice()])))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    assert_eq!(&body[..], br#"[{"status":204},{"status":204}]"#);

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/benchcmp/batch-producer/append-batch")
                .header(HEADER_PRODUCER_ID, "writer-1")
                .header(HEADER_PRODUCER_EPOCH, "0")
                .header(HEADER_PRODUCER_SEQ, "1")
                .body(Body::from(batch_body(&[b"d".as_slice()])))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    assert_eq!(&body[..], br#"[{"status":204}]"#);

    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/benchcmp/batch-producer?offset=0&max_bytes=16")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    assert_eq!(&body[..], b"abcd");
}

#[tokio::test]
async fn json_mode_normalizes_appends_and_reads_ndjson() {
    let app = test_router();

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri("/v1/stream/json-mode")
                .header(CONTENT_TYPE, "application/json; charset=utf-8")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::CREATED);
    assert_eq!(
        response.headers().get(HEADER_STREAM_EXTENSIONS).unwrap(),
        JSON_RECORD_COORDINATES_EXTENSION
    );
    assert_eq!(
        response.headers().get(HEADER_STREAM_RECORD_START).unwrap(),
        "0"
    );
    assert_eq!(
        response.headers().get(HEADER_STREAM_RECORD_NEXT).unwrap(),
        "0"
    );

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/stream/json-mode")
                .header(CONTENT_TYPE, "application/json; charset=utf-8")
                .body(Body::from(r#"[[1,2,3]]"#))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    assert_eq!(
        response.headers().get(HEADER_STREAM_EXTENSIONS).unwrap(),
        JSON_RECORD_COORDINATES_EXTENSION
    );
    assert_eq!(
        response.headers().get(HEADER_STREAM_RECORD_START).unwrap(),
        "0"
    );
    assert_eq!(
        response.headers().get(HEADER_STREAM_RECORD_NEXT).unwrap(),
        "1"
    );

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("HEAD")
                .uri("/v1/stream/json-mode")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get(HEADER_STREAM_RECORD_FIRST).unwrap(),
        "0"
    );
    assert_eq!(
        response.headers().get(HEADER_STREAM_RECORD_NEXT).unwrap(),
        "1"
    );

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/v1/stream/json-mode")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get(CONTENT_TYPE).unwrap(),
        "application/x-ndjson"
    );
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    assert_eq!(&body[..], b"[1,2,3]\n");

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/stream/json-mode")
                .header(CONTENT_TYPE, "application/json")
                .body(Body::from("[]"))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn json_record_coordinates_read_complete_records() {
    let app = test_router();
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri("/v1/stream/json-record-read")
                .header(CONTENT_TYPE, "application/json")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::CREATED);

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/stream/json-record-read")
                .header(CONTENT_TYPE, "application/json")
                .body(Body::from(r#"[{"id":1},{"id":2},{"id":3}]"#))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    assert_eq!(
        response.headers().get(HEADER_STREAM_RECORD_START).unwrap(),
        "0"
    );
    assert_eq!(
        response.headers().get(HEADER_STREAM_RECORD_NEXT).unwrap(),
        "3"
    );

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/v1/stream/json-record-read?record=1&max_records=1")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get(CONTENT_TYPE).unwrap(),
        "application/x-ndjson"
    );
    assert_eq!(
        response.headers().get(HEADER_STREAM_RECORD_FIRST).unwrap(),
        "0"
    );
    assert_eq!(
        response.headers().get(HEADER_STREAM_RECORD_START).unwrap(),
        "1"
    );
    assert_eq!(
        response.headers().get(HEADER_STREAM_RECORD_NEXT).unwrap(),
        "2"
    );
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    assert_eq!(&body[..], b"{\"id\":2}\n");

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/v1/stream/json-record-read?tail_records=2")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get(HEADER_STREAM_RECORD_START).unwrap(),
        "1"
    );
    assert_eq!(
        response.headers().get(HEADER_STREAM_RECORD_NEXT).unwrap(),
        "3"
    );
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    assert_eq!(&body[..], b"{\"id\":2}\n{\"id\":3}\n");

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/v1/stream/json-record-read?record=1&max_records=1&record_view=envelope")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get(CONTENT_TYPE).unwrap(),
        "application/vnd.durable-stream-records+ndjson"
    );
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    assert_eq!(&body[..], b"{\"record\":1,\"value\":{\"id\":2}}\n");

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/stream/json-record-read")
                .header(CONTENT_TYPE, "application/json")
                .header(HEADER_STREAM_RECORD_MATCH, "3")
                .body(Body::from(r#"{"id":4}"#))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    assert_eq!(
        response.headers().get(HEADER_STREAM_RECORD_START).unwrap(),
        "3"
    );
    assert_eq!(
        response.headers().get(HEADER_STREAM_RECORD_NEXT).unwrap(),
        "4"
    );

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/stream/json-record-read")
                .header(CONTENT_TYPE, "application/json")
                .header(HEADER_STREAM_RECORD_MATCH, "3")
                .body(Body::from(r#"{"id":5}"#))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::PRECONDITION_FAILED);
    assert_eq!(
        response.headers().get(HEADER_STREAM_RECORD_NEXT).unwrap(),
        "4"
    );
    assert!(response.headers().get(HEADER_STREAM_NEXT_OFFSET).is_some());
}

#[tokio::test]
async fn json_record_coordinates_preserve_deduplicated_and_close_only_ranges() {
    let app = test_router();
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri("/v1/stream/json-record-dedup")
                .header(CONTENT_TYPE, "application/json")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::CREATED);

    for payload in [r#"[{"id":1},{"id":2}]"#, r#"{"ignored":true}"#] {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/stream/json-record-dedup")
                    .header(CONTENT_TYPE, "application/json")
                    .header(HEADER_PRODUCER_ID, "browser-tab")
                    .header(HEADER_PRODUCER_EPOCH, "0")
                    .header(HEADER_PRODUCER_SEQ, "0")
                    .body(Body::from(payload))
                    .expect("request"),
            )
            .await
            .expect("response");
        assert!(matches!(
            response.status(),
            StatusCode::OK | StatusCode::NO_CONTENT
        ));
        assert_eq!(
            response.headers().get(HEADER_STREAM_RECORD_START).unwrap(),
            "0"
        );
        assert_eq!(
            response.headers().get(HEADER_STREAM_RECORD_NEXT).unwrap(),
            "2"
        );
    }

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/stream/json-record-dedup")
                .header(CONTENT_TYPE, "application/json")
                .header(HEADER_STREAM_CLOSED, "true")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    assert_eq!(
        response.headers().get(HEADER_STREAM_RECORD_START).unwrap(),
        "2"
    );
    assert_eq!(
        response.headers().get(HEADER_STREAM_RECORD_NEXT).unwrap(),
        "2"
    );
}

#[tokio::test]
async fn json_record_coordinates_concurrent_appends_receive_disjoint_commit_ranges() {
    let app = test_router();
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri("/v1/stream/json-record-concurrent")
                .header(CONTENT_TYPE, "application/json")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::CREATED);

    let mut writes = Vec::new();
    for id in 0..16 {
        let app = app.clone();
        writes.push(tokio::spawn(async move {
            app.oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/stream/json-record-concurrent")
                    .header(CONTENT_TYPE, "application/json")
                    .body(Body::from(format!(r#"{{"id":{id}}}"#)))
                    .expect("request"),
            )
            .await
            .expect("response")
        }));
    }

    let mut ranges = Vec::new();
    for write in writes {
        let response = write.await.expect("write task");
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
        let start = response
            .headers()
            .get(HEADER_STREAM_RECORD_START)
            .expect("record start")
            .to_str()
            .expect("record start text")
            .parse::<u64>()
            .expect("record start integer");
        let next = response
            .headers()
            .get(HEADER_STREAM_RECORD_NEXT)
            .expect("record next")
            .to_str()
            .expect("record next text")
            .parse::<u64>()
            .expect("record next integer");
        ranges.push((start, next));
    }
    ranges.sort_unstable();
    assert_eq!(
        ranges,
        (0..16)
            .map(|record| (record, record + 1))
            .collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn json_record_coordinates_live_reads_resume_by_record() {
    let app = test_router();
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri("/v1/stream/json-record-live")
                .header(CONTENT_TYPE, "application/json")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::CREATED);

    let read = {
        let app = app.clone();
        tokio::spawn(async move {
            app.oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/v1/stream/json-record-live?record=now&live=long-poll&timeout_ms=1000")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response")
        })
    };
    tokio::time::sleep(std::time::Duration::from_millis(10)).await;

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/stream/json-record-live")
                .header(CONTENT_TYPE, "application/json")
                .body(Body::from(r#"[{"id":1},{"id":2}]"#))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    let response = tokio::time::timeout(std::time::Duration::from_secs(1), read)
        .await
        .expect("long poll completed")
        .expect("read task");
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get(HEADER_STREAM_RECORD_START).unwrap(),
        "0"
    );
    assert_eq!(
        response.headers().get(HEADER_STREAM_RECORD_NEXT).unwrap(),
        "2"
    );
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    assert_eq!(&body[..], b"{\"id\":1}\n{\"id\":2}\n");

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/stream/json-record-live")
                .header(CONTENT_TYPE, "application/json")
                .header(HEADER_STREAM_CLOSED, "true")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/v1/stream/json-record-live?record=0&record_view=envelope&live=sse")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get("stream-data-content-type").unwrap(),
        "application/vnd.durable-stream-record+json"
    );
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("sse body");
    let body = std::str::from_utf8(&body).expect("utf8 sse body");
    assert_eq!(body.matches("event: data").count(), 2);
    assert!(body.contains("data:{\"record\":0,\"value\":{\"id\":1}}"));
    assert!(body.contains("data:{\"record\":1,\"value\":{\"id\":2}}"));
    assert!(body.contains("\"streamFirstRecord\":0"));
    assert!(body.contains("\"streamNextRecord\":2"));
}

#[tokio::test]
async fn json_record_coordinates_snapshot_and_bootstrap_headers_are_aligned() {
    let app = test_router();
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri("/benchcmp/json-record-snapshot")
                .header(CONTENT_TYPE, "application/json")
                .body(Body::from(r#"[{"id":1},{"id":2}]"#))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::CREATED);

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri("/benchcmp/json-record-snapshot/snapshot/1")
                .header(CONTENT_TYPE, "application/json")
                .body(Body::from(r#"{"count":0}"#))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/benchcmp/json-record-snapshot?record=0&max_records=1")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    let snapshot_offset = response
        .headers()
        .get(HEADER_STREAM_NEXT_OFFSET)
        .unwrap()
        .to_str()
        .unwrap()
        .to_owned();

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(format!(
                    "/benchcmp/json-record-snapshot/snapshot/{snapshot_offset}"
                ))
                .header(CONTENT_TYPE, "application/json")
                .body(Body::from(r#"{"count":1}"#))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    assert_eq!(
        response.headers().get(HEADER_STREAM_RECORD_FIRST).unwrap(),
        "1"
    );
    assert_eq!(
        response.headers().get(HEADER_STREAM_RECORD_NEXT).unwrap(),
        "2"
    );

    for uri in [
        format!("/benchcmp/json-record-snapshot/snapshot/{snapshot_offset}"),
        "/benchcmp/json-record-snapshot/bootstrap".to_owned(),
    ] {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri(uri)
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get(HEADER_STREAM_RECORD_FIRST).unwrap(),
            "1"
        );
        assert_eq!(
            response.headers().get(HEADER_STREAM_RECORD_NEXT).unwrap(),
            "2"
        );
    }
}

#[tokio::test]
async fn json_mode_reads_ndjson_bytes_without_message_boundary_projection() {
    let app = test_router();

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri("/v1/stream/json-window")
                .header(CONTENT_TYPE, "application/json")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::CREATED);

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/stream/json-window")
                .header(CONTENT_TYPE, "application/json")
                .body(Body::from(r#"[{"message":"alpha"},{"message":"beta"}]"#))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/v1/stream/json-window?offset=-1&max_bytes=5")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get(CONTENT_TYPE).unwrap(),
        "application/x-ndjson"
    );
    assert_eq!(
        response.headers().get(HEADER_STREAM_NEXT_OFFSET).unwrap(),
        "00000000000000000005"
    );
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    assert_eq!(&body[..], b"{\"mes");
}

#[test]
fn append_batch_parser_returns_body_slices() {
    let body = Bytes::from(batch_body(&[b"abc".as_slice(), b"de".as_slice()]));
    let base = body.as_ptr();
    let payloads = parse_append_batch(&body).expect("batch");

    assert_eq!(payloads.len(), 2);
    assert_eq!(&payloads[0][..], b"abc");
    assert_eq!(&payloads[1][..], b"de");
    assert_eq!(payloads[0].as_ptr(), base.wrapping_add(4));
    assert_eq!(payloads[1].as_ptr(), base.wrapping_add(4 + 3 + 4));
}

#[tokio::test]
async fn metrics_expose_per_core_and_group_append_distribution() {
    let app = test_router();

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri("/benchcmp/metrics-stream")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::CREATED);

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/benchcmp/metrics-stream/append-batch")
                .body(Body::from(batch_body(&[
                    b"abc".as_slice(),
                    b"de".as_slice(),
                ])))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::OK);

    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/__ursula/metrics")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get(CONTENT_TYPE).unwrap(),
        "application/json"
    );

    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
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
    assert!(body.contains("\"raft_write_many_batches\":0"));
    assert!(body.contains("\"per_core_raft_write_many_batches\":[0,0]"));
    assert!(body.contains("\"per_group_raft_write_many_batches\":["));
    assert!(body.contains("\"raft_write_many_commands\":0"));
    assert!(body.contains("\"per_core_raft_write_many_commands\":[0,0]"));
    assert!(body.contains("\"per_group_raft_write_many_commands\":["));
    assert!(body.contains("\"raft_write_many_logical_commands\":0"));
    assert!(body.contains("\"per_core_raft_write_many_logical_commands\":[0,0]"));
    assert!(body.contains("\"per_group_raft_write_many_logical_commands\":["));
    assert!(body.contains("\"raft_write_many_responses\":0"));
    assert!(body.contains("\"per_core_raft_write_many_responses\":[0,0]"));
    assert!(body.contains("\"per_group_raft_write_many_responses\":["));
    assert!(body.contains("\"raft_write_many_submit_ns\":0"));
    assert!(body.contains("\"per_core_raft_write_many_submit_ns\":[0,0]"));
    assert!(body.contains("\"per_group_raft_write_many_submit_ns\":["));
    assert!(body.contains("\"raft_write_many_response_ns\":0"));
    assert!(body.contains("\"per_core_raft_write_many_response_ns\":[0,0]"));
    assert!(body.contains("\"per_group_raft_write_many_response_ns\":["));
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
    assert!(body.contains("\"routed_requests\":2"));
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
    assert!(body.contains("\"cold_hot_bytes\":0"));
    assert!(body.contains("\"per_group_cold_hot_bytes\":["));
    assert!(body.contains("\"cold_hot_group_bytes_max\":0"));
    assert!(body.contains("\"per_group_cold_hot_bytes_max\":["));
    assert!(body.contains("\"cold_hot_stream_bytes_max\":0"));
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

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri("/benchcmp/long-poll-timeout")
                .header(CONTENT_TYPE, "text/plain")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::CREATED);

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/benchcmp/long-poll-timeout?offset=now&live=long-poll&timeout_ms=10")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    assert_eq!(
        response.headers().get(HEADER_STREAM_NEXT_OFFSET).unwrap(),
        "00000000000000000000"
    );
    assert_eq!(
        response.headers().get(HEADER_STREAM_UP_TO_DATE).unwrap(),
        "true"
    );
    assert_eq!(
        response.headers().get(HEADER_STREAM_CURSOR).unwrap(),
        "00000000000000000000"
    );

    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/__ursula/metrics")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    let body = std::str::from_utf8(&body).expect("utf8 body");
    assert!(body.contains("\"live_read_waiters\":0"));
}

#[tokio::test]
async fn long_poll_returns_service_unavailable_when_live_waiters_are_full() {
    let runtime =
        ShardRuntime::spawn(RuntimeConfig::new(1, 1).with_live_read_max_waiters_per_core(Some(1)))
            .expect("runtime");
    let app = router(runtime);

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri("/benchcmp/long-poll-limit")
                .header(CONTENT_TYPE, "text/plain")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::CREATED);

    let first = {
        let app = app.clone();
        tokio::spawn(async move {
            app.oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/benchcmp/long-poll-limit?offset=now&live=long-poll&timeout_ms=1000")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response")
        })
    };
    tokio::time::sleep(std::time::Duration::from_millis(10)).await;

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/benchcmp/long-poll-limit?offset=now&live=long-poll&timeout_ms=1000")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        response.headers().get(axum::http::header::RETRY_AFTER),
        Some(&axum::http::HeaderValue::from_static("1"))
    );
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    assert!(
        std::str::from_utf8(&body)
            .expect("utf8 body")
            .contains("live read waiters")
    );

    first.abort();
    let _ = first.await;

    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/__ursula/metrics")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    let body = std::str::from_utf8(&body).expect("utf8 body");
    assert!(body.contains("\"live_read_backpressure_events\":1"));
    assert!(body.contains("\"per_core_live_read_backpressure_events\":[1]"));
}

#[tokio::test]
async fn long_poll_returns_append_from_owner_waiter() {
    let app = test_router();

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri("/benchcmp/long-poll-wake")
                .header(CONTENT_TYPE, "text/plain")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::CREATED);

    let read = {
        let app = app.clone();
        tokio::spawn(async move {
            app.oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/benchcmp/long-poll-wake?offset=now&live=long-poll&timeout_ms=1000")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response")
        })
    };
    tokio::time::sleep(std::time::Duration::from_millis(10)).await;

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/benchcmp/long-poll-wake")
                .header(CONTENT_TYPE, "text/plain")
                .body(Body::from("wake"))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    let response = tokio::time::timeout(std::time::Duration::from_secs(1), read)
        .await
        .expect("long poll completed")
        .expect("read task");
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get(HEADER_STREAM_NEXT_OFFSET).unwrap(),
        "00000000000000000004"
    );
    assert_eq!(
        response.headers().get(HEADER_STREAM_CURSOR).unwrap(),
        "00000000000000000004"
    );
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    assert_eq!(&body[..], b"wake");
}

#[tokio::test]
async fn sse_live_tail_delivers_appended_text_and_closed_control() {
    let app = test_router();

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri("/benchcmp/sse-stream")
                .header(CONTENT_TYPE, "text/plain")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::CREATED);

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/benchcmp/sse-stream?offset=now&live=sse")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get(CONTENT_TYPE).unwrap(),
        "text/event-stream"
    );
    assert_eq!(
        response
            .headers()
            .get("stream-data-content-type")
            .expect("stream data content type"),
        "text/plain"
    );

    let body_task = tokio::spawn(async move {
        to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("sse body")
    });

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/benchcmp/sse-stream")
                .header(CONTENT_TYPE, "text/plain")
                .header(HEADER_STREAM_CLOSED, "true")
                .body(Body::from("sse-token"))
                .expect("request"),
        )
        .await
        .expect("response");
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

    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/__ursula/metrics")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("metrics body");
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

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri("/benchcmp/sse-json")
                .header(CONTENT_TYPE, "application/json")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::CREATED);

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/benchcmp/sse-json")
                .header(CONTENT_TYPE, "application/json")
                .header(HEADER_STREAM_CLOSED, "true")
                .body(Body::from(r#"{"event":"done"}"#))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/benchcmp/sse-json?offset=-1&live=sse")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get(CONTENT_TYPE).unwrap(),
        "text/event-stream"
    );
    assert_eq!(
        response
            .headers()
            .get("stream-data-content-type")
            .expect("stream data content type"),
        "application/x-ndjson"
    );
    assert!(
        response
            .headers()
            .get(HEADER_STREAM_SSE_DATA_ENCODING)
            .is_none()
    );
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("sse body");
    let body = std::str::from_utf8(&body).expect("utf8 sse body");
    assert!(body.contains("event: data"));
    assert!(body.contains("data:{\"event\":\"done\"}"));
    assert!(body.contains("data:{\"event\":\"done\"}\ndata:\n\n"));
    assert!(body.contains("\"streamClosed\":true"));
}

#[tokio::test]
async fn sse_json_max_bytes_does_not_split_utf8_codepoints() {
    let app = test_router();

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri("/benchcmp/sse-json-utf8")
                .header(CONTENT_TYPE, "application/json")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::CREATED);

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/benchcmp/sse-json-utf8")
                .header(CONTENT_TYPE, "application/json")
                .header(HEADER_STREAM_CLOSED, "true")
                .body(Body::from(r#"{"m":"\u00e9"}"#))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/benchcmp/sse-json-utf8?offset=-1&live=sse&max_bytes=7")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response
            .headers()
            .get("stream-data-content-type")
            .expect("stream data content type"),
        "application/x-ndjson"
    );

    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("sse body");
    let body = std::str::from_utf8(&body).expect("utf8 sse body");
    assert!(!body.contains('\u{fffd}'), "{body}");
    assert!(body.contains("\"streamNextOffset\":\"00000000000000000006\""));
    assert!(body.contains("data:{\"m\":\""));
    assert!(body.contains("data:\u{00e9}\"}"));
    assert!(body.contains("\"streamClosed\":true"));
}

#[tokio::test]
async fn wal_runtime_recovers_http_stream_after_restart() {
    let wal_root = std::env::temp_dir().join(format!(
        "ursula-wal-test-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system time after unix epoch")
            .as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&wal_root);

    {
        let app = router(
            spawn_runtime(
                &test_config(2, 8),
                Persistence::Wal {
                    wal_dir: wal_root.clone(),
                },
                Topology::SingleNode {
                    raft_group_count: 8,
                },
            )
            .expect("runtime")
            .runtime,
        );
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/benchcmp/wal-http")
                    .header(CONTENT_TYPE, "text/plain")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::CREATED);

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/benchcmp/wal-http/append-batch")
                    .header(CONTENT_TYPE, "text/plain")
                    .body(Body::from(batch_body(&[
                        b"persisted".as_slice(),
                        b"-batch".as_slice(),
                    ])))
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::OK);

        let response = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/__ursula/metrics")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("metrics body");
        let body = std::str::from_utf8(&body).expect("utf8 body");
        assert!(body.contains("\"wal_batches\":2"));
        assert!(body.contains("\"wal_records\":2"));
        assert!(body.contains("\"wal_write_ns\":"));
        assert!(body.contains("\"wal_sync_ns\":"));
    }

    let app = router(
        spawn_runtime(
            &test_config(2, 8),
            Persistence::Wal {
                wal_dir: wal_root.clone(),
            },
            Topology::SingleNode {
                raft_group_count: 8,
            },
        )
        .expect("recovered runtime")
        .runtime,
    );
    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/benchcmp/wal-http?offset=0&max_bytes=32")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    assert_eq!(&body[..], b"persisted-batch");

    std::fs::remove_dir_all(&wal_root).expect("remove WAL root");
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
    let _ = std::fs::remove_dir_all(&raft_root);

    let app = router(
        spawn_runtime(
            &test_config(1, 1),
            Persistence::Raft {
                log_dir: Some(raft_root.clone()),
            },
            Topology::SingleNode {
                raft_group_count: 1,
            },
        )
        .expect("runtime")
        .runtime,
    );
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri("/benchcmp/raft-http")
                .header(CONTENT_TYPE, "text/plain")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::CREATED);

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/benchcmp/raft-http")
                .header(CONTENT_TYPE, "text/plain")
                .body(Body::from("raft-payload"))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/benchcmp/raft-http?offset=0&max_bytes=32")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    assert_eq!(&body[..], b"raft-payload");

    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/__ursula/metrics")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("metrics body");
    let body = std::str::from_utf8(&body).expect("utf8 body");
    assert!(body.contains("\"wal_batches\":"));
    assert!(!body.contains("\"wal_batches\":0"));
    assert!(body.contains("\"wal_records\":"));
    assert!(!body.contains("\"wal_records\":0"));
    assert!(body.contains("\"wal_write_ns\":"));
    assert!(body.contains("\"wal_sync_ns\":"));

    let journal_path = raft_root.join("core-0").join("journal.bin");
    let journal_len = std::fs::metadata(&journal_path)
        .expect("raft core journal")
        .len();
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
    let _ = std::fs::remove_dir_all(&raft_root);

    let spawned = spawn_runtime(
        &test_config(1, 1),
        Persistence::Raft {
            log_dir: Some(raft_root.as_path().into()),
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

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri("/benchcmp/static-raft-log")
                .header(CONTENT_TYPE, "text/plain")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    let status = response.status();
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    assert_eq!(
        status,
        StatusCode::CREATED,
        "create body={}",
        std::str::from_utf8(&body).unwrap_or("<non-utf8>")
    );

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/benchcmp/static-raft-log")
                .header(CONTENT_TYPE, "text/plain")
                .body(Body::from("static-raft-payload"))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/benchcmp/static-raft-log?offset=0&max_bytes=64")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    assert_eq!(&body[..], b"static-raft-payload");

    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/__ursula/metrics")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("metrics body");
    let body = std::str::from_utf8(&body).expect("utf8 body");
    assert!(body.contains("\"wal_batches\":"));
    assert!(body.contains("\"wal_records\":"));

    let journal_path = raft_root.join("core-0").join("journal.bin");
    assert!(journal_path.exists(), "core journal should exist");
    assert!(
        std::fs::metadata(&journal_path)
            .expect("core journal metadata")
            .len()
            > 0,
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
    let _ = std::fs::remove_dir_all(&raft_root);
    let peers = [(1, "http://127.0.0.1:4477".to_owned())];

    {
        let spawned = spawn_runtime(
            &test_config(1, 1),
            Persistence::Raft {
                log_dir: Some(raft_root.as_path().into()),
            },
            Topology::static_cluster(1, peers.to_vec(), 1, true, Default::default())
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
        let app = router_with_static_raft_cluster(runtime, registry, peers.clone());

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/benchcmp/static-raft-log-restart")
                    .header(CONTENT_TYPE, "text/plain")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::CREATED);

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/benchcmp/static-raft-log-restart")
                    .header(CONTENT_TYPE, "text/plain")
                    .body(Body::from("restart-payload"))
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
    }

    let journal_path = raft_root.join("core-0").join("journal.bin");
    assert!(journal_path.exists(), "core journal should exist");
    assert!(
        std::fs::metadata(&journal_path)
            .expect("core journal metadata")
            .len()
            > 0,
        "core journal should contain records"
    );

    {
        let spawned = spawn_runtime(
            &test_config(1, 1),
            Persistence::Raft {
                log_dir: Some(raft_root.as_path().into()),
            },
            Topology::static_cluster(1, peers.to_vec(), 1, false, Default::default())
                .expect("valid static cluster topology"),
        )
        .expect("restarted runtime");
        let runtime = spawned.runtime;
        let registry = spawned.raft_registry.expect("registry");
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
        let app = router_with_static_raft_cluster(runtime, registry, peers);

        let response = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/benchcmp/static-raft-log-restart?offset=0&max_bytes=64")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        assert_eq!(&body[..], b"restart-payload");
    }

    std::fs::remove_dir_all(&raft_root).expect("remove raft root");
}

#[tokio::test]
async fn raft_memory_runtime_serves_http_subset_without_wal_metrics() {
    let app = router(
        spawn_runtime(
            &test_config(1, 1),
            Persistence::Raft { log_dir: None },
            Topology::SingleNode {
                raft_group_count: 1,
            },
        )
        .expect("runtime")
        .runtime,
    );
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri("/benchcmp/raft-memory-http")
                .header(CONTENT_TYPE, "text/plain")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::CREATED);

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/benchcmp/raft-memory-http")
                .header(CONTENT_TYPE, "text/plain")
                .body(Body::from("raft-memory-payload"))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/benchcmp/raft-memory-http?offset=0&max_bytes=64")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    assert_eq!(&body[..], b"raft-memory-payload");

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri("/benchcmp/raft-memory-http/snapshot/00000000000000000019")
                .header(CONTENT_TYPE, "application/octet-stream")
                .body(Body::from("raft-state"))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/benchcmp/raft-memory-http/bootstrap")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get(HEADER_STREAM_NEXT_OFFSET).unwrap(),
        "00000000000000000019"
    );
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("bootstrap body");
    let body = std::str::from_utf8(&body).expect("utf8 body");
    assert!(body.contains("raft-state"));

    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/__ursula/metrics")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("metrics body");
    let body = std::str::from_utf8(&body).expect("utf8 body");
    assert!(body.contains("\"accepted_appends\":1"));
    assert!(body.contains("\"wal_batches\":0"));
    assert!(body.contains("\"wal_records\":0"));
    assert!(body.contains("\"wal_write_ns\":0"));
    assert!(body.contains("\"wal_sync_ns\":0"));
    assert!(body.contains("\"raft_group_count\":0"));
    assert!(body.contains("\"raft_groups\":[]"));
}

#[tokio::test]
async fn raft_grpc_network_dispatches_to_registered_runtime_owned_group() {
    let registry = RaftGroupHandleRegistry::default();
    let mut config = RuntimeConfig::new(1, 1);
    config.threading = ursula_runtime::RuntimeThreading::HostedTokio;
    let runtime = ShardRuntime::spawn_with_engine_factory(
        config,
        ursula_raft::RegisteredRaftGroupEngineFactory::new(registry.clone()),
    )
    .expect("runtime");
    runtime
        .warm_group(RaftGroupId(0))
        .await
        .expect("warm raft group");
    let app = router_with_raft_registry(runtime, registry);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind listener");
    let addr = listener.local_addr().expect("local addr");
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let server = tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(async move {
                let _ = shutdown_rx.await;
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

    let mut network =
        ursula_raft::GrpcRaftNetwork::new(RaftGroupId(0), 1, format!("http://{addr}"));
    let vote_request =
        ursula_raft::UrsulaVoteRequest::new(ursula_raft::UrsulaVote::new(2, 1), None);
    let _: ursula_raft::UrsulaVoteResponse = network
        .vote(vote_request, RPCOption::new(Duration::from_secs(1)))
        .await
        .expect("send vote over gRPC Raft network");

    let mut missing_group =
        ursula_raft::GrpcRaftNetwork::new(RaftGroupId(1), 1, format!("http://{addr}"));
    let err = missing_group
        .vote(
            ursula_raft::UrsulaVoteRequest::new(ursula_raft::UrsulaVote::new(3, 1), None),
            RPCOption::new(Duration::from_secs(1)),
        )
        .await
        .expect_err("missing group should fail");
    assert!(err.to_string().contains("not registered"), "err={err}");

    let _ = shutdown_tx.send(());
    server.await.expect("server task");
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
            spawn_static_grpc_test_node_with_per_group_initializers(
                node_id,
                listener,
                peers.clone(),
                peers.clone(),
                true,
                6,
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
            spawn_static_grpc_test_node_with_storage(
                node_id,
                listener,
                peers.clone(),
                peers.clone(),
                true,
                2,
                StaticGrpcTestNodeStorage {
                    raft_log_dir: None,
                    cold_store: None,
                    engine_config: None,
                    per_group_initializers: true,
                    per_group_voters: group_voters.clone(),
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
            spawn_static_grpc_test_node_with_storage(
                node_id,
                listener,
                peers.clone(),
                peers.clone(),
                true,
                2,
                StaticGrpcTestNodeStorage {
                    raft_log_dir: None,
                    cold_store: None,
                    engine_config: None,
                    per_group_initializers: true,
                    per_group_voters: group_voters.clone(),
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
async fn static_grpc_memory_node_rejoins_empty_after_allowed_log_revert() {
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
                node_id == 1,
                1,
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

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("build reqwest client");
    let stream_id = BucketStreamId::new("benchcmp", "memory-rejoin-empty");
    let leader_base = peers[0].1.as_str();
    let create = client
        .put(format!("{leader_base}/benchcmp/memory-rejoin-empty"))
        .header(CONTENT_TYPE, "application/octet-stream")
        .body("before-restart")
        .send()
        .await
        .expect("create stream through leader");
    assert_eq!(create.status(), StatusCode::CREATED);

    let placement = nodes[0].runtime.locate(&stream_id);
    wait_raft_state_machine_payload(
        &nodes[2].registry,
        placement,
        &stream_id,
        b"before-restart",
        "node 3 replicated initial payload before shutdown",
    )
    .await;

    let stopped_node = nodes.remove(2);
    stopped_node.shutdown().await;

    let append_while_down = client
        .post(format!("{leader_base}/benchcmp/memory-rejoin-empty"))
        .header(CONTENT_TYPE, "application/octet-stream")
        .body("-while-down")
        .send()
        .await
        .expect("append while node 3 is down");
    assert_eq!(append_while_down.status(), StatusCode::NO_CONTENT);

    let leader_raft = nodes[0].registry.get(RaftGroupId(0)).expect("leader group");
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
    // 15s headroom: x86 CI runners finish openraft's snapshot worker noticeably
    // slower than ARM and laptops; the previous 5s ceiling was tight enough
    // to flake despite the wait being correct.
    leader_raft
        .wait(Some(Duration::from_secs(15)))
        .metrics(
            |metrics| {
                metrics
                    .snapshot
                    .as_ref()
                    .is_some_and(|snapshot| snapshot >= &snapshot_log_id)
            },
            format!("leader snapshot includes quorum-only write .snapshot >= {snapshot_log_id}"),
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

    // A follower must refuse to arm the revert: the raft core drops the
    // trigger on non-leaders, so a 200 here would report an arming that
    // never happened.
    let follower_base = peers[1].1.as_str();
    let follower_reject = client
        .post(format!(
            "{follower_base}/__ursula/raft/0/nodes/3/allow-next-revert"
        ))
        .send()
        .await
        .expect("allow-next-revert request to follower");
    assert_eq!(follower_reject.status(), StatusCode::CONFLICT);

    let allow_revert = client
        .post(format!(
            "{leader_base}/__ursula/raft/0/nodes/3/allow-next-revert"
        ))
        .send()
        .await
        .expect("allow node 3 log reversion through admin endpoint");
    assert_eq!(allow_revert.status(), StatusCode::OK);

    let restarted_listener = tokio::net::TcpListener::bind(addrs[2])
        .await
        .expect("rebind node 3 listener");
    let restarted = spawn_static_grpc_test_node(
        3,
        restarted_listener,
        peers.clone(),
        peers.clone(),
        false,
        1,
    )
    .await;
    restarted
        .runtime
        .warm_all_groups()
        .await
        .expect("warm restarted empty node 3 group");
    let restarted_raft = restarted
        .registry
        .get(RaftGroupId(0))
        .expect("restarted group");
    restarted_raft
        .wait(Some(Duration::from_secs(10)))
        .metrics(
            |metrics| {
                metrics
                    .snapshot
                    .as_ref()
                    .is_some_and(|snapshot| snapshot >= &snapshot_log_id)
            },
            format!(
                "restarted empty node installed leader snapshot .snapshot >= {snapshot_log_id}"
            ),
        )
        .await
        .expect("wait for restarted node snapshot");

    wait_raft_state_machine_payload(
        &restarted.registry,
        placement,
        &stream_id,
        b"before-restart-while-down",
        "restarted empty memory node caught up from surviving quorum",
    )
    .await;

    let rejoined_read = client
        .get(format!(
            "{}/benchcmp/memory-rejoin-empty?offset=0&max_bytes=64",
            peers[2].1
        ))
        .send()
        .await
        .expect("read from restarted empty node");
    assert_eq!(rejoined_read.status(), StatusCode::OK);
    assert_eq!(
        &rejoined_read.bytes().await.expect("rejoined node body")[..],
        b"before-restart-while-down"
    );

    nodes.push(restarted);
    for node in nodes {
        node.shutdown().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn static_grpc_memory_node_rejoins_all_groups_after_allowed_log_revert() {
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
            spawn_static_grpc_test_node_with_per_group_initializers(
                node_id,
                listener,
                peers.clone(),
                peers.clone(),
                true,
                6,
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
        let stream_id = BucketStreamId::new("benchcmp", format!("memory-rejoin-group-{candidate}"));
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

        let leader_base = peers
            .iter()
            .find(|(node_id, _)| *node_id == leader_id)
            .map(|(_, base_url)| base_url.as_str())
            .expect("leader peer base url");
        let allow_revert = client
            .post(format!(
                "{leader_base}/__ursula/raft/{raw_group_id}/nodes/3/allow-next-revert"
            ))
            .send()
            .await
            .expect("allow node 3 log reversion through admin endpoint");
        assert_eq!(allow_revert.status(), StatusCode::OK);
    }

    let restarted_listener = tokio::net::TcpListener::bind(addrs[2])
        .await
        .expect("rebind node 3 listener");
    let restarted = spawn_static_grpc_test_node_with_per_group_initializers(
        3,
        restarted_listener,
        peers.clone(),
        peers.clone(),
        true,
        6,
    )
    .await;
    restarted
        .runtime
        .warm_all_groups()
        .await
        .expect("warm restarted empty node 3 groups");

    for (group_index, stream_id) in streams_by_group.iter().enumerate() {
        let raft_group_id = RaftGroupId(group_index as u32);
        let restarted_raft = restarted
            .registry
            .get(raft_group_id)
            .expect("restarted group");
        restarted_raft
            .wait(Some(Duration::from_secs(10)))
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
            "restarted empty memory node caught up from surviving quorum",
        )
        .await;
    }

    nodes.push(restarted);
    for node in nodes {
        node.shutdown().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn static_grpc_memory_restart_with_bootstrap_marker_fails_fast() {
    let marker_dir = tempfile::tempdir().expect("marker dir");
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

    let engine_config = ursula_raft::RaftEngineConfig {
        memory_bootstrap_marker_dir: Some(marker_dir.path().to_path_buf()),
        rejoin_probe: Duration::from_millis(100),
        bootstrap_peer_probe: Duration::from_millis(100),
        bootstrap_peer_probe_interval: Duration::from_millis(20),
        bootstrap_peer_connect: Duration::from_millis(20),
        ..Default::default()
    };
    let mut nodes = Vec::new();
    for (index, listener) in listeners.into_iter().enumerate() {
        let node_id = u64::try_from(index + 1).expect("node id fits u64");
        nodes.push(
            spawn_static_grpc_test_node_with_storage(
                node_id,
                listener,
                peers.clone(),
                peers.clone(),
                true,
                6,
                StaticGrpcTestNodeStorage {
                    raft_log_dir: None,
                    cold_store: None,
                    engine_config: Some(engine_config.clone()),
                    per_group_initializers: true,
                    per_group_voters: BTreeMap::new(),
                },
            )
            .await,
        );
    }

    for node in &nodes {
        tokio::time::timeout(Duration::from_secs(10), node.runtime.warm_all_groups())
            .await
            .expect("initial warm_all_groups timed out")
            .expect("initial warm_all_groups");
    }
    let group_0_marker = marker_dir.path().join("node-1-group-0.bootstrapped");
    for _ in 0..50 {
        if group_0_marker.exists() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(
        group_0_marker.exists(),
        "bootstrap wrote marker {}",
        group_0_marker.display()
    );
    for node in nodes {
        node.shutdown().await;
    }

    let mut restarted_nodes = Vec::new();
    for (index, addr) in addrs.iter().enumerate() {
        let node_id = u64::try_from(index + 1).expect("node id fits u64");
        let listener = tokio::net::TcpListener::bind(addr)
            .await
            .expect("rebind listener");
        restarted_nodes.push(
            spawn_static_grpc_test_node_with_storage(
                node_id,
                listener,
                peers.clone(),
                peers.clone(),
                true,
                6,
                StaticGrpcTestNodeStorage {
                    raft_log_dir: None,
                    cold_store: None,
                    engine_config: Some(engine_config.clone()),
                    per_group_initializers: true,
                    per_group_voters: BTreeMap::new(),
                },
            )
            .await,
        );
    }
    let restart_error = tokio::time::timeout(
        Duration::from_secs(10),
        restarted_nodes[0].runtime.warm_all_groups(),
    )
    .await
    .expect("restart warm_all_groups timed out")
    .expect_err("marker-backed memory restart must fail instead of reinitializing membership");
    assert!(
        restart_error
            .to_string()
            .contains("already bootstrapped once"),
        "unexpected restart error: {restart_error}"
    );

    for node in restarted_nodes {
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
    let vote = ursula_raft::UrsulaVote::new(1, 1);
    let mut snapshot_network =
        ursula_raft::GrpcRaftNetwork::new(RaftGroupId(0), 2, peers[1].1.clone());
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
    let _ = std::fs::remove_dir_all(&raft_root);

    let mut nodes = Vec::new();
    for (index, listener) in listeners.into_iter().enumerate() {
        let node_id = u64::try_from(index + 1).expect("node id fits u64");
        let node_root = raft_root.join(format!("node-{node_id}"));
        nodes.push(
            spawn_static_grpc_test_node_with_log_dir(
                node_id,
                listener,
                peers.clone(),
                peers.clone(),
                node_id == 1,
                2,
                Some(node_root),
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

        let journal_path = raft_root
            .join(format!("node-{}", node_index + 1))
            .join("core-0")
            .join("journal.bin");
        assert!(journal_path.exists(), "node journal should exist");
        assert!(
            std::fs::metadata(&journal_path)
                .expect("node journal metadata")
                .len()
                > 0,
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
    let _ = std::fs::remove_dir_all(&raft_root);
    let cold_store = std::sync::Arc::new(ColdStore::memory().expect("memory cold store"));

    let mut nodes = Vec::new();
    for (index, listener) in listeners.into_iter().enumerate() {
        let node_id = u64::try_from(index + 1).expect("node id fits u64");
        let node_root = raft_root.join(format!("node-{node_id}"));
        nodes.push(
            spawn_static_grpc_test_node_with_storage(
                node_id,
                listener,
                peers.clone(),
                peers.clone(),
                node_id == 1,
                1,
                StaticGrpcTestNodeStorage {
                    raft_log_dir: Some(node_root),
                    cold_store: Some(cold_store.clone()),
                    engine_config: None,
                    per_group_initializers: false,
                    per_group_voters: BTreeMap::new(),
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
            },
            8,
        )
        .await
        .expect("flush replicated cold manifest");
    assert!(
        flushed.len() >= 2,
        "batch cold flush should publish multiple chunks"
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
            if entry.cold_frontier_offset > 0 && entry.payload.len() < payload.len() {
                last_snapshot = Some(entry);
                break;
            }
            last_snapshot = Some(entry);
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        let entry = last_snapshot.expect("stream snapshot entry");
        assert!(
            entry.cold_frontier_offset > 0,
            "replicated stream should advance cold frontier"
        );
        assert!(
            entry.payload.len() < payload.len(),
            "replicated hot payload should shrink after cold flush"
        );
    }

    for (node_index, _) in peers.iter().enumerate() {
        let journal_path = raft_root
            .join(format!("node-{}", node_index + 1))
            .join("core-0")
            .join("journal.bin");
        assert!(journal_path.exists(), "node journal should exist");
        assert!(
            std::fs::metadata(&journal_path)
                .expect("node journal metadata")
                .len()
                > 0,
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
    let _ = std::fs::remove_dir_all(&raft_root);

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
        spawn_static_grpc_test_node_with_log_dir(
            1,
            listener1,
            initial_peers.clone(),
            peers.clone(),
            true,
            1,
            raft_root.as_ref().map(|root| root.join("node-1")),
        )
        .await,
        spawn_static_grpc_test_node_with_log_dir(
            2,
            listener2,
            initial_peers.clone(),
            peers.clone(),
            false,
            1,
            raft_root.as_ref().map(|root| root.join("node-2")),
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
        spawn_static_grpc_test_node_with_log_dir(
            3,
            listener3,
            peers.clone(),
            peers.clone(),
            false,
            1,
            raft_root.as_ref().map(|root| root.join("node-3")),
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

    let promote = client
        .post(format!(
            "{leader_base}/__ursula/raft/0/membership?voters=1,2,3"
        ))
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
            let journal_path = raft_root
                .join(format!("node-{node_id}"))
                .join("core-0")
                .join("journal.bin");
            assert!(journal_path.exists(), "node journal should exist");
            assert!(
                std::fs::metadata(&journal_path)
                    .expect("node journal metadata")
                    .len()
                    > 0,
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

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri("/benchcmp/http-cold")
                .header(CONTENT_TYPE, "text/plain")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::CREATED);
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/benchcmp/http-cold")
                .header(CONTENT_TYPE, "text/plain")
                .body(Body::from("abcdef"))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/__ursula/flush-cold/benchcmp/http-cold?min_hot_bytes=4&max_bytes=4")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::OK);

    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/benchcmp/http-cold?offset=0&max_bytes=6")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("read body");
    assert_eq!(&body[..], b"abcdef");
}

#[tokio::test]
async fn cold_backpressure_returns_service_unavailable_and_metrics() {
    let cold_store = std::sync::Arc::new(ColdStore::memory().expect("memory cold store"));
    let runtime = ShardRuntime::spawn_with_engine_factory_and_cold_store(
        RuntimeConfig::new(1, 1).with_cold_max_hot_bytes_per_group(Some(4)),
        InMemoryGroupEngineFactory::with_cold_store(Some(cold_store.clone())),
        Some(cold_store),
    )
    .expect("runtime");
    let app = router(runtime);

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri("/benchcmp/http-cold-backpressure")
                .header(CONTENT_TYPE, "text/plain")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::CREATED);

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/benchcmp/http-cold-backpressure")
                .header(CONTENT_TYPE, "text/plain")
                .body(Body::from("abcd"))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/benchcmp/http-cold-backpressure")
                .header(CONTENT_TYPE, "text/plain")
                .body(Body::from("e"))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("error body");
    assert!(
        std::str::from_utf8(&body)
            .expect("utf8 body")
            .contains("ColdBackpressure")
    );

    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/__ursula/metrics")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("metrics body");
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

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(stream_uri)
                .header(CONTENT_TYPE, "application/octet-stream")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::CREATED);

    for payload in ["abc", "de"] {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(stream_uri)
                    .header(CONTENT_TYPE, "application/octet-stream")
                    .body(Body::from(payload))
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
    }

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri("/benchcmp/snapshot-http/snapshot/00000000000000000003")
                .header(CONTENT_TYPE, "application/json")
                .body(Body::from(r#"{"state":"abc"}"#))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    assert_eq!(
        response
            .headers()
            .get(HEADER_STREAM_SNAPSHOT_OFFSET)
            .unwrap(),
        "00000000000000000003"
    );

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("HEAD")
                .uri(stream_uri)
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response
            .headers()
            .get(HEADER_STREAM_SNAPSHOT_OFFSET)
            .unwrap(),
        "00000000000000000003"
    );

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/benchcmp/snapshot-http/snapshot")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::TEMPORARY_REDIRECT);
    assert_eq!(
        response.headers().get(LOCATION).unwrap(),
        "/benchcmp/snapshot-http/snapshot/00000000000000000003"
    );

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/benchcmp/snapshot-http/snapshot/00000000000000000003")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get(CONTENT_TYPE).unwrap(),
        "application/json"
    );
    assert_eq!(
        response.headers().get(HEADER_STREAM_NEXT_OFFSET).unwrap(),
        "00000000000000000003"
    );
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    assert_eq!(&body[..], br#"{"state":"abc"}"#);

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/benchcmp/snapshot-http?offset=0&max_bytes=1")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::GONE);
    assert_eq!(
        response.headers().get(HEADER_STREAM_NEXT_OFFSET).unwrap(),
        "00000000000000000003"
    );

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/benchcmp/snapshot-http/bootstrap")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::OK);
    assert!(
        response
            .headers()
            .get(CONTENT_TYPE)
            .unwrap()
            .to_str()
            .unwrap()
            .starts_with("multipart/mixed; boundary=ursula-bootstrap-")
    );
    assert_eq!(
        response.headers().get(HEADER_STREAM_NEXT_OFFSET).unwrap(),
        "00000000000000000005"
    );
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    let body = std::str::from_utf8(&body).expect("multipart utf8");
    assert!(body.contains(r#"{"state":"abc"}"#));
    assert!(body.contains("de"));

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/benchcmp/snapshot-http/snapshot/00000000000000000003")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::CONFLICT);
}

#[tokio::test]
async fn bootstrap_without_snapshot_emits_empty_snapshot_part_and_rejects_live() {
    let app = test_router();
    let stream_uri = "/benchcmp/bootstrap-nosnapshot";

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(stream_uri)
                .header(CONTENT_TYPE, "application/octet-stream")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::CREATED);

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(stream_uri)
                .header(CONTENT_TYPE, "application/octet-stream")
                .body(Body::from("one"))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/benchcmp/bootstrap-nosnapshot/snapshot")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("HEAD")
                .uri(stream_uri)
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::OK);
    assert!(
        response
            .headers()
            .get(HEADER_STREAM_SNAPSHOT_OFFSET)
            .is_none()
    );

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/benchcmp/bootstrap-nosnapshot/bootstrap?live=sse")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/benchcmp/bootstrap-nosnapshot/bootstrap")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response
            .headers()
            .get(HEADER_STREAM_SNAPSHOT_OFFSET)
            .unwrap(),
        "-1"
    );
    assert_eq!(
        response.headers().get(HEADER_STREAM_NEXT_OFFSET).unwrap(),
        "00000000000000000003"
    );
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    let body = std::str::from_utf8(&body).expect("multipart utf8");
    assert!(body.contains("Content-Type: application/octet-stream\r\n\r\n\r\n--"));
    assert!(body.contains("one"));
}

#[tokio::test]
async fn snapshot_publish_errors_and_overwrite_follow_extension_statuses() {
    let app = test_router();
    let stream_uri = "/benchcmp/snapshot-errors";

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(stream_uri)
                .header(CONTENT_TYPE, "application/octet-stream")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::CREATED);

    for payload in ["abc", "de"] {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(stream_uri)
                    .header(CONTENT_TYPE, "application/octet-stream")
                    .body(Body::from(payload))
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
    }

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri("/benchcmp/snapshot-errors/snapshot/-1")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri("/benchcmp/snapshot-errors/snapshot/00000000000000000002")
                .body(Body::from("ab-state"))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri("/benchcmp/snapshot-errors/snapshot/00000000000000000006")
                .body(Body::from("too-far"))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::CONFLICT);

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri("/benchcmp/snapshot-errors/snapshot/00000000000000000003")
                .body(Body::from("abc-state"))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri("/benchcmp/snapshot-errors/snapshot/00000000000000000002")
                .body(Body::from("old-state"))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::GONE);

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri("/benchcmp/snapshot-errors/snapshot/00000000000000000005")
                .body(Body::from("abcde-state"))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/benchcmp/snapshot-errors/snapshot/00000000000000000003")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/benchcmp/snapshot-errors/snapshot/00000000000000000003")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/benchcmp/snapshot-errors?offset=3&max_bytes=2")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::GONE);

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/benchcmp/snapshot-errors/bootstrap")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response
            .headers()
            .get(HEADER_STREAM_SNAPSHOT_OFFSET)
            .unwrap(),
        "00000000000000000005"
    );
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
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
    let response = client
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(RAFT_GRPC_APPEND_PATH)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
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
    let response = cluster
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/some-bucket/some-stream")
                .header("content-type", "application/octet-stream")
                .body(Body::from(b"payload".to_vec()))
                .unwrap(),
        )
        .await
        .unwrap();
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
    let response = cluster
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(LEADERSHIP_SHED_PATH)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
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

    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/__ursula/leadership-shed/maintenance")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    let body = std::str::from_utf8(&body).expect("status utf8");
    assert!(body.contains("\"state\":\"maintenance-drain\""), "{body}");
    assert!(body.contains("\"should_accept_transfer\":false"), "{body}");
    assert!(body.contains("\"should_campaign\":false"), "{body}");

    let response = router
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/__ursula/leadership-shed/maintenance")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    let body = std::str::from_utf8(&body).expect("status utf8");
    assert!(body.contains("\"state\":\"none\""), "{body}");
    assert!(body.contains("\"should_accept_transfer\":true"), "{body}");
    assert!(body.contains("\"should_campaign\":true"), "{body}");
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
    let merged_for_cluster = merged.clone();

    // Client-plane: HEAD on an unknown stream returns 404 (route mounted).
    let head = merged
        .oneshot(
            Request::builder()
                .method("HEAD")
                .uri("/some-bucket/unknown-stream")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_ne!(head.status().as_u16(), 405, "client HEAD route missing");

    // Cluster-plane: the gRPC path is reachable (not 404). It will fail later
    // due to missing protobuf headers, but the route itself must be mounted.
    let grpc = merged_for_cluster
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(RAFT_GRPC_APPEND_PATH)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
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

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri("/benchcmp/clocked-stream")
                .header(CONTENT_TYPE, "text/plain")
                .header(HEADER_STREAM_TTL, "1")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::CREATED);

    now_ms.store(1_999, Ordering::Relaxed);
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("HEAD")
                .uri("/benchcmp/clocked-stream")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::OK);

    now_ms.store(2_000, Ordering::Relaxed);
    let response = app
        .oneshot(
            Request::builder()
                .method("HEAD")
                .uri("/benchcmp/clocked-stream")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

fn batch_body(payloads: &[&[u8]]) -> Vec<u8> {
    let mut body = Vec::new();
    for payload in payloads {
        body.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        body.extend_from_slice(payload);
    }
    body
}

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
            },
            ingress_admission_middleware,
        ));

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/write")
                .header(CONTENT_LENGTH, "5")
                .body(Body::from("abcde"))
                .expect("request"),
        )
        .await
        .expect("response");

    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        response
            .headers()
            .get(axum::http::header::RETRY_AFTER)
            .expect("retry-after")
            .to_str()
            .expect("ascii retry-after"),
        "1"
    );
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
            },
            ingress_admission_middleware,
        ));

    let first = tokio::spawn({
        let app = app.clone();
        async move {
            app.oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/write")
                    .header(CONTENT_LENGTH, "4")
                    .body(Body::from("abcd"))
                    .expect("request"),
            )
            .await
            .expect("response")
        }
    });
    entered.wait().await;

    let second = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/write")
                .header(CONTENT_LENGTH, "1")
                .body(Body::from("e"))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(second.status(), StatusCode::SERVICE_UNAVAILABLE);

    release.notify_one();
    assert_eq!(
        first.await.expect("first join").status(),
        StatusCode::NO_CONTENT
    );
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
    use ursula_raft::RaftGroupMetricsSnapshot;
    use ursula_raft::RaftLogProgressSnapshot;

    use crate::bootstrap::next_snapshot_to_drive;
    use crate::bootstrap::resolve_snapshot_drive_interval_ms;
    use crate::bootstrap::should_drive_snapshot_for_group;

    fn snap_with_group(
        raft_group_id: u32,
        last_applied: Option<u64>,
        snapshot_index: Option<u64>,
    ) -> RaftGroupMetricsSnapshot {
        RaftGroupMetricsSnapshot {
            raft_group_id,
            node_id: 1,
            current_term: 1,
            current_leader: Some(1),
            last_log_index: last_applied,
            committed: last_applied.map(|index| RaftLogProgressSnapshot { term: 1, index }),
            last_applied: last_applied.map(|index| RaftLogProgressSnapshot { term: 1, index }),
            snapshot: snapshot_index.map(|index| RaftLogProgressSnapshot { term: 1, index }),
            purged: None,
            voter_ids: vec![1, 2, 3],
            learner_ids: vec![],
        }
    }

    fn snap(last_applied: Option<u64>, snapshot_index: Option<u64>) -> RaftGroupMetricsSnapshot {
        snap_with_group(0, last_applied, snapshot_index)
    }

    #[test]
    fn snapshot_driver_only_snapshots_applied_work_past_current_snapshot() {
        assert!(!should_drive_snapshot_for_group(&snap(None, None)));
        assert!(should_drive_snapshot_for_group(&snap(Some(42), None)));
        assert!(should_drive_snapshot_for_group(&snap(Some(42), Some(41))));
        assert!(!should_drive_snapshot_for_group(&snap(Some(42), Some(42))));
        assert!(!should_drive_snapshot_for_group(&snap(Some(42), Some(43))));
    }

    #[test]
    fn snapshot_driver_default_interval_follows_external_store() {
        assert_eq!(resolve_snapshot_drive_interval_ms(None, false), 0);
        assert_eq!(resolve_snapshot_drive_interval_ms(None, true), 60_000);
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
    fn snapshot_driver_picks_one_due_group_round_robin() {
        let snapshots = vec![
            snap_with_group(0, Some(42), Some(42)),
            snap_with_group(1, Some(42), Some(41)),
            snap_with_group(2, Some(42), Some(41)),
        ];

        let first = next_snapshot_to_drive(&snapshots, 0).expect("first due snapshot");
        assert_eq!(first.0, 1);
        assert_eq!(first.1.raft_group_id, 1);

        let second = next_snapshot_to_drive(&snapshots, first.0 + 1).expect("second due snapshot");
        assert_eq!(second.0, 2);
        assert_eq!(second.1.raft_group_id, 2);

        let wrapped = next_snapshot_to_drive(&snapshots, second.0 + 1).expect("wrapped snapshot");
        assert_eq!(wrapped.0, 1);
        assert_eq!(wrapped.1.raft_group_id, 1);
    }
}

mod leadership_balance {
    use std::collections::HashSet;

    use ursula_raft::RaftGroupMetricsSnapshot;
    use ursula_raft::RaftLogProgressSnapshot;

    use crate::bootstrap::leader_counts;
    use crate::bootstrap::plan_leadership_balance;
    use crate::bootstrap::plan_leadership_balance_with_eligible_nodes;
    use crate::bootstrap::prioritized_transfer_targets;

    fn snap(group_id: u32, leader: Option<u64>) -> RaftGroupMetricsSnapshot {
        RaftGroupMetricsSnapshot {
            raft_group_id: group_id,
            node_id: 1,
            current_term: 1,
            current_leader: leader,
            last_log_index: Some(100),
            committed: Some(RaftLogProgressSnapshot {
                term: 1,
                index: 100,
            }),
            last_applied: Some(RaftLogProgressSnapshot {
                term: 1,
                index: 100,
            }),
            snapshot: None,
            purged: None,
            voter_ids: vec![1, 2, 3],
            learner_ids: vec![],
        }
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
    use ursula_raft::RaftLogProgressSnapshot;
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
        RaftGroupMetricsSnapshot {
            raft_group_id: group_id,
            node_id,
            current_term: 1,
            current_leader: leader,
            last_log_index: Some(100),
            committed: Some(RaftLogProgressSnapshot {
                term: 1,
                index: 100,
            }),
            last_applied: Some(RaftLogProgressSnapshot {
                term: 1,
                index: 100,
            }),
            snapshot: None,
            purged: None,
            voter_ids: voters,
            learner_ids: vec![],
        }
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
    fn egress_shed_spreads_handoffs_across_peer_voters() {
        let snaps = vec![
            snap_with_leader(0, 3, Some(3), vec![1, 2, 3]),
            snap_with_leader(1, 3, Some(3), vec![1, 2, 3]),
            snap_with_leader(2, 3, Some(2), vec![1, 2, 3]),
            snap_with_leader(3, 3, Some(3), vec![1, 2, 3]),
            snap_with_leader(4, 3, Some(3), vec![1, 2, 3]),
            snap_with_leader(5, 3, Some(2), vec![1, 2, 3]),
        ];

        let actions = plan_cluster_egress_shed(&snaps, 3);

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

        crate::bootstrap::spawn_egress_gate(
            &registry,
            1,
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

mod commit_stall {
    use std::time::Duration;

    use tokio::time::Instant;
    use ursula_raft::RaftGroupMetricsSnapshot;
    use ursula_raft::RaftLogProgressSnapshot;

    use crate::bootstrap::CommitStallAction;
    use crate::bootstrap::CommitStallTracker;

    fn snap(
        group_id: u32,
        node_id: u64,
        leader: Option<u64>,
        last_log: Option<u64>,
        committed: Option<u64>,
    ) -> RaftGroupMetricsSnapshot {
        RaftGroupMetricsSnapshot {
            raft_group_id: group_id,
            node_id,
            current_term: 1,
            current_leader: leader,
            last_log_index: last_log,
            committed: committed.map(|index| RaftLogProgressSnapshot { term: 1, index }),
            last_applied: committed.map(|index| RaftLogProgressSnapshot { term: 1, index }),
            snapshot: None,
            purged: None,
            voter_ids: vec![1, 2, 3],
            learner_ids: vec![],
        }
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

use std::path::PathBuf;

use axum::body::{Body, to_bytes};
use axum::http::Request;
use openraft::RaftNetworkV2;
use ursula_raft::StaticGrpcRaftGroupEngineFactory;
use ursula_runtime::{
    ColdStore, ColdStoreHandle, InMemoryGroupEngineFactory, PlanGroupColdFlushRequest,
    RuntimeConfig,
};
use openraft::error::ReplicationClosed;
use openraft::network::RPCOption;
use openraft::raft::SnapshotResponse;
use openraft::rt::WatchReceiver;
use tower::ServiceExt;
use ursula_raft::UrsulaRaftTypeConfig;
use ursula_shard::RaftGroupId;

use super::*;

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
    per_group_initializers: bool,
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
            per_group_initializers: false,
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
            per_group_initializers: true,
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
    factory = factory.with_cold_store(storage.cold_store.clone());
    if let Some(raft_log_dir) = storage.raft_log_dir {
        factory = factory.with_raft_log_dir(raft_log_dir);
    }
    let runtime =
        ShardRuntime::spawn_with_engine_factory_and_cold_store(config, factory, storage.cold_store)
            .expect("runtime");
    let app = router_with_static_raft_cluster(runtime.clone(), registry.clone(), router_peers);
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
async fn json_mode_normalizes_appends_and_projects_reads() {
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
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    assert_eq!(&body[..], br#"[[1,2,3]]"#);

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
async fn fork_creation_copies_source_prefix_and_inherits_content_type() {
    let app = test_router();

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri("/v1/stream/fork-copy-source")
                .header(CONTENT_TYPE, "application/json")
                .body(Body::from(r#"[{"from":"source"}]"#))
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
                .uri("/v1/stream/fork-copy-child")
                .header(HEADER_STREAM_FORKED_FROM, "/v1/stream/fork-copy-source")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::CREATED);
    assert_eq!(
        response.headers().get(CONTENT_TYPE).expect("content type"),
        "application/json"
    );

    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/v1/stream/fork-copy-child?offset=-1")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    assert_eq!(&body[..], br#"[{"from":"source"}]"#);
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
        let app = router(spawn_wal_runtime(2, 8, &wal_root).expect("runtime"));
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

    let app = router(spawn_wal_runtime(2, 8, &wal_root).expect("recovered runtime"));
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

    let app = router(spawn_raft_runtime(1, 1, &raft_root).expect("runtime"));
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

    let (runtime, registry) = spawn_static_grpc_raft_runtime(
        1,
        1,
        1,
        [(1, "http://127.0.0.1:4477".to_owned())],
        true,
        &raft_root,
    )
    .expect("runtime");
    runtime.warm_all_groups().await.expect("warm group");
    let raft = registry
        .get(RaftGroupId(0))
        .expect("registered static raft group");
    raft.wait(Some(Duration::from_secs(5)))
        .current_leader(1, "static durable gRPC Raft group should elect node 1")
        .await
        .expect("wait for leader");
    let app = router_with_static_raft_cluster(
        runtime,
        registry,
        [(1, "http://127.0.0.1:4477".to_owned())],
    );

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
        let (runtime, registry) =
            spawn_static_grpc_raft_runtime(1, 1, 1, peers.clone(), true, &raft_root)
                .expect("runtime");
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
        let (runtime, registry) =
            spawn_static_grpc_raft_runtime(1, 1, 1, peers.clone(), false, &raft_root)
                .expect("restarted runtime");
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
    let app = router(spawn_raft_memory_runtime(1, 1).expect("runtime"));
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

    for raw_group_id in 0..6 {
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
    let follower_response = http_client
        .put(format!("{}/benchcmp/follower-forward", peers[1].1))
        .header(CONTENT_TYPE, "text/plain")
        .body("created-through-forward")
        .send()
        .await
        .expect("send follower write through internal gRPC forwarding");
    assert_eq!(follower_response.status(), StatusCode::CREATED);
    assert_eq!(
        follower_response
            .headers()
            .get(HEADER_STREAM_NEXT_OFFSET)
            .and_then(|value| value.to_str().ok()),
        Some("00000000000000000023")
    );
    let follower_read = http_client
        .get(format!("{}/benchcmp/follower-forward", peers[1].1))
        .send()
        .await
        .expect("send immediate follower read");
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
                    per_group_initializers: false,
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
            if !entry.cold_chunks.is_empty() && entry.payload.len() < payload.len() {
                last_snapshot = Some(entry);
                break;
            }
            last_snapshot = Some(entry);
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        let entry = last_snapshot.expect("stream snapshot entry");
        assert!(
            !entry.cold_chunks.is_empty(),
            "replicated stream should have cold manifest chunks"
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
        .snapshot(snapshot_log_id, "leader snapshot includes stream append")
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
        .snapshot(snapshot_log_id, "late learner installed gRPC snapshot")
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
        leader_metrics_body.contains(&format!("\"snapshot_index\":{}", snapshot_log_id.index()))
    );
    assert!(leader_metrics_body.contains(&format!("\"purged_index\":{}", snapshot_log_id.index())));

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
    assert!(late_metrics_body.contains(&format!("\"snapshot_index\":{}", snapshot_log_id.index())));
    assert!(late_metrics_body.contains("\"voter_ids\":[1,2]"));
    assert!(late_metrics_body.contains("\"learner_ids\":[3]"));

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
    router(spawn_default_runtime(2, 8).expect("runtime"))
}

fn batch_body(payloads: &[&[u8]]) -> Vec<u8> {
    let mut body = Vec::new();
    for payload in payloads {
        body.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        body.extend_from_slice(payload);
    }
    body
}

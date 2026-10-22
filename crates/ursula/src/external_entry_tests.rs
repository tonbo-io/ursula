//! Bounded-state F4b: the Raft entry of an external JSON create or append
//! carries the number of messages in its staged payload, never one end
//! offset per message, so its size does not depend on what the body holds.

use std::sync::Arc;

use axum::Router;
use axum::body::Body;
use axum::body::to_bytes;
use axum::http::Request;
use axum::http::StatusCode;
use axum::http::header::CONTENT_TYPE;
use axum::response::Response;
use tower::ServiceExt;
use ursula_runtime::ColdStore;
use ursula_runtime::RuntimeConfig;
use ursula_runtime::ShardRuntime;

use crate::router_with_raft_cluster;

/// A body just under the request limit: `[0,0,...,0]`, whose normalized
/// text is one `0\n` message per value, the most messages a body can hold.
const BODY_BYTES: usize = 33_554_431;
const MESSAGES: u64 = 16_777_215;
/// The bytes of those messages once stored.
const STORED_BYTES: u64 = 33_554_430;
/// Growth of the WAL an entry of the request may cause. One end offset per
/// message made it about 80 MiB.
const WAL_GROWTH_BOUND: u64 = 1024 * 1024;

fn tiny_messages_body() -> Vec<u8> {
    let mut body = Vec::with_capacity(BODY_BYTES);
    body.extend_from_slice(b"[0");
    while body.len() < BODY_BYTES.saturating_sub(1) {
        body.extend_from_slice(b",0");
    }
    body.push(b']');
    assert_eq!(body.len(), BODY_BYTES);
    body
}

async fn send(app: &Router, method: &str, uri: &str, body: Body) -> Response {
    app.clone()
        .oneshot(
            Request::builder()
                .method(method)
                .uri(uri)
                .header(CONTENT_TYPE, "application/json")
                .body(body)
                .expect("request"),
        )
        .await
        .expect("response")
}

async fn committed_records(app: &Router, bucket: &str) -> u64 {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/__ursula/usage")
                .body(Body::empty())
                .expect("usage request"),
        )
        .await
        .expect("usage response");
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("usage body");
    let usage: ursula_proto::admin::UsageReport =
        serde_json::from_slice(&body).expect("typed usage");
    ursula_proto::admin::UsageReport::bucket_usage(&[usage])
        .0
        .get(bucket)
        .map_or(0, |bucket| bucket.committed_records)
}

async fn first_bytes(app: &Router, uri: &str) -> Vec<u8> {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("{uri}?offset=-1&max_bytes=4"))
                .body(Body::empty())
                .expect("read request"),
        )
        .await
        .expect("read response");
    assert_eq!(response.status(), StatusCode::OK);
    to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("read body")
        .to_vec()
}

/// A durable single-node Raft runtime with a memory cold store, so bodies of
/// 1 MiB or more are staged and their writes replicate as external entries.
/// Its groups register in `registry`, whose leadership view the front door
/// routes by and fences the staged uploads to (F14j).
fn durable_runtime_with_cold_store(
    wal_root: &crate::tests::TestWalRoot,
    registry: &ursula_raft::RaftGroupHandleRegistry,
) -> (ShardRuntime, ursula_raft::RaftWal) {
    let raft_wal = ursula_raft::RaftWal::start(
        wal_root.path(),
        ursula_config::WalFsync::Never,
        &ursula_shard::StaticShardMap::new(1, 1).expect("valid topology"),
    )
    .expect("start the Raft WAL");
    let cold_store = Arc::new(ColdStore::memory().expect("memory cold store"));
    let mut config = RuntimeConfig::new(1, 1);
    config.threading = ursula_runtime::RuntimeThreading::HostedTokio;
    let runtime = ShardRuntime::spawn_with_engine_factory_and_cold_store(
        config,
        ursula_raft::PlacedRaftGroupEngineFactory::single_node(
            raft_wal.clone(),
            registry.clone(),
            1,
        )
        .with_cold_store(Some(cold_store.clone())),
        Some(cold_store),
    )
    .expect("runtime");
    (runtime, raft_wal)
}

fn wal_bytes(runtime: &ShardRuntime) -> u64 {
    runtime.metrics().snapshot().wal_physical_bytes
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn external_json_writes_of_tiny_messages_make_small_entries() {
    let wal_root = crate::tests::TestWalRoot::new();
    let registry = ursula_raft::RaftGroupHandleRegistry::default();
    let (runtime, raft_wal) = durable_runtime_with_cold_store(&wal_root, &registry);
    let app = router_with_raft_cluster(runtime.clone(), registry);
    let body = tiny_messages_body();

    // An append to an existing stream.
    let response = send(&app, "PUT", "/benchcmp/tiny-append", Body::empty()).await;
    assert_eq!(response.status(), StatusCode::CREATED);
    let records = committed_records(&app, "benchcmp").await;
    let wal_before = wal_bytes(&runtime);
    let response = send(
        &app,
        "POST",
        "/benchcmp/tiny-append",
        Body::from(body.clone()),
    )
    .await;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    let growth = wal_bytes(&runtime).saturating_sub(wal_before);
    assert!(
        growth > 0 && growth < WAL_GROWTH_BOUND,
        "the append grew the WAL by {growth} bytes"
    );
    assert_eq!(
        committed_records(&app, "benchcmp")
            .await
            .checked_sub(records),
        Some(MESSAGES)
    );
    assert_eq!(first_bytes(&app, "/benchcmp/tiny-append").await, b"0\n0\n");

    // A create whose initial body is the same.
    let records = committed_records(&app, "benchcmp").await;
    let wal_before = wal_bytes(&runtime);
    let response = send(&app, "PUT", "/benchcmp/tiny-create", Body::from(body)).await;
    assert_eq!(response.status(), StatusCode::CREATED);
    let growth = wal_bytes(&runtime).saturating_sub(wal_before);
    assert!(
        growth > 0 && growth < WAL_GROWTH_BOUND,
        "the create grew the WAL by {growth} bytes"
    );
    assert_eq!(
        committed_records(&app, "benchcmp")
            .await
            .checked_sub(records),
        Some(MESSAGES)
    );
    assert_eq!(first_bytes(&app, "/benchcmp/tiny-create").await, b"0\n0\n");

    let head = app
        .clone()
        .oneshot(
            Request::builder()
                .method("HEAD")
                .uri("/benchcmp/tiny-create")
                .body(Body::empty())
                .expect("head request"),
        )
        .await
        .expect("head response");
    assert_eq!(head.status(), StatusCode::OK);
    assert_eq!(
        head.headers()
            .get(ursula_headers::names::STREAM_NEXT_OFFSET)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse::<u64>().ok()),
        Some(STORED_BYTES)
    );

    assert!(
        matches!(
            crate::server::shutdown_raft_wal(&runtime, None, Some(&raft_wal)).await,
            crate::server::WalShutdown::Clean(_)
        ),
        "the Raft WAL shuts down cleanly"
    );
}

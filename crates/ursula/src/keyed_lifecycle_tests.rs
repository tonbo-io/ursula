//! Keyed-streams U23 and U24 over HTTP: bucket purge drains every
//! keyed-state indexer before erasing and proving `{bucket}/` and
//! `.keyed/{bucket}/`, and keyed-state responses are counted in metrics.

use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use axum::body::Body;
use axum::http::Request;
use axum::http::StatusCode;
use tokio::sync::Notify;
use tower::ServiceExt;
use ursula_runtime::ColdStore;
use ursula_runtime::CreateStreamRequest;
use ursula_runtime::FEATURE_LEVEL_KEYED_STREAMS;
use ursula_runtime::HeadStreamRequest;
use ursula_runtime::InMemoryGroupEngineFactory;
use ursula_runtime::RuntimeConfig;
use ursula_runtime::ShardRuntime;
use ursula_shard::BucketStreamId;
use ursula_shard::KEYED_BATCH_CONTENT_TYPE;
use ursula_shard::keyed_namespace::keyed_bucket_prefix;
use ursula_shard::keyed_namespace::keyed_incarnation_prefix;

use crate::HttpState;
use crate::router_with_http_state;

/// A stub indexer pod: records every drain body, then answers `status`
/// once `release` is notified (or at once when `release` is `None`).
#[derive(Clone)]
struct StubIndexer {
    drains: Arc<Mutex<Vec<serde_json::Value>>>,
    received: Arc<Notify>,
    release: Option<Arc<Notify>>,
    status: StatusCode,
}

impl StubIndexer {
    fn new(status: StatusCode, release: Option<Arc<Notify>>) -> Self {
        Self {
            drains: Arc::default(),
            received: Arc::new(Notify::new()),
            release,
            status,
        }
    }

    async fn serve(&self) -> String {
        let stub = self.clone();
        let app = axum::Router::new().route(
            "/v1/keyed/drain",
            axum::routing::post(move |body: axum::body::Bytes| {
                let stub = stub.clone();
                async move {
                    let value = serde_json::from_slice(&body).expect("drain body is JSON");
                    stub.drains.lock().expect("drains lock").push(value);
                    stub.received.notify_one();
                    if let Some(release) = &stub.release {
                        release.notified().await;
                    }
                    stub.status
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind stub indexer");
        let addr = listener.local_addr().expect("stub indexer addr");
        tokio::spawn(async move {
            axum::serve(listener, app)
                .await
                .expect("serve stub indexer");
        });
        format!("http://{addr}")
    }

    fn drains(&self) -> Vec<serde_json::Value> {
        self.drains.lock().expect("drains lock").clone()
    }
}

struct Fixture {
    cold_store: Arc<ColdStore>,
    app: axum::Router,
    keyed_prefix: String,
}

async fn fixture(bucket: &str, indexer_urls: Vec<String>) -> Fixture {
    let cold_store = Arc::new(ColdStore::memory().expect("memory cold store"));
    let runtime = ShardRuntime::spawn_with_engine_factory_and_cold_store(
        RuntimeConfig::new(1, 2),
        InMemoryGroupEngineFactory::with_cold_store(Some(cold_store.clone())),
        Some(cold_store.clone()),
    )
    .expect("runtime");
    for (group, result) in runtime
        .set_feature_level_all_groups(FEATURE_LEVEL_KEYED_STREAMS)
        .await
    {
        result.unwrap_or_else(|err| panic!("raise group {group:?}: {err}"));
    }
    let stream = BucketStreamId::new(bucket, "harness");
    runtime
        .create_stream(CreateStreamRequest::new(
            stream.clone(),
            KEYED_BATCH_CONTENT_TYPE,
        ))
        .await
        .expect("create keyed stream");
    let incarnation = runtime
        .head_stream(HeadStreamRequest {
            stream_id: stream.clone(),
            now_ms: 0,
        })
        .await
        .expect("head keyed stream")
        .created_at_ms
        .expect("incarnation");
    for name in [
        format!(
            "{}v1/CURRENT",
            keyed_incarnation_prefix(&stream, incarnation)
        ),
        // A namespace no stream state tracks any more.
        format!(
            "{}orphan/0000000000000001/v1/CURRENT",
            keyed_bucket_prefix(bucket)
        ),
        format!("{bucket}/orphan/external/staged.bin"),
        "survivor/keep/external/object.bin".to_owned(),
        ".keyed/survivor/keep/0000000000000001/v1/CURRENT".to_owned(),
    ] {
        cold_store
            .write_chunk(&name, b"x")
            .await
            .expect("write object");
    }
    let config = ursula_config::KeyedStateConfig {
        indexer_urls,
        drain_timeout: ursula_config::HumanDuration::sec(5),
    };
    let state = HttpState::new(runtime).with_keyed_state_config(&config);
    Fixture {
        cold_store,
        app: router_with_http_state(state),
        keyed_prefix: keyed_bucket_prefix(bucket),
    }
}

async fn purge(app: &axum::Router, bucket: &str) -> serde_json::Value {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri(format!("/__ursula/purge/{bucket}"))
                .body(Body::empty())
                .expect("purge request"),
        )
        .await
        .expect("purge response");
    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("purge body");
    serde_json::from_slice(&body).expect("purge report JSON")
}

async fn is_empty(cold_store: &ColdStore, prefix: &str) -> bool {
    cold_store
        .prefix_is_empty(prefix)
        .await
        .expect("list prefix")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn u23_purge_waits_for_every_indexer_drain_then_proves_both_prefixes_empty() {
    let release = Arc::new(Notify::new());
    let slow = StubIndexer::new(StatusCode::OK, Some(release.clone()));
    let fast = StubIndexer::new(StatusCode::OK, None);
    let urls = vec![slow.serve().await, format!("{}/", fast.serve().await)];
    let fixture = fixture("erased", urls).await;

    let app = fixture.app.clone();
    let purge_task = tokio::spawn(async move { purge(&app, "erased").await });
    tokio::time::timeout(Duration::from_secs(5), slow.received.notified())
        .await
        .expect("slow indexer receives drain");
    // Until the slow pod acknowledges, nothing below either prefix is erased.
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(!purge_task.is_finished());
    assert!(!is_empty(&fixture.cold_store, &fixture.keyed_prefix).await);
    assert!(!is_empty(&fixture.cold_store, "erased/").await);

    release.notify_one();
    let report = tokio::time::timeout(Duration::from_secs(10), purge_task)
        .await
        .expect("purge finishes after the ack")
        .expect("purge task");
    assert_eq!(report["keyed_drain_complete"], true, "{report}");
    assert_eq!(report["keyed_indexers_drained"], 2);
    assert_eq!(report["keyed_prefix_absent"], true);
    assert_eq!(report["bucket_prefix_absent"], true);
    assert_eq!(report["cold_gc_complete"], true);
    for stub in [&slow, &fast] {
        assert_eq!(stub.drains(), vec![
            serde_json::json!({ "bucket": "erased" })
        ]);
    }
    assert!(is_empty(&fixture.cold_store, &fixture.keyed_prefix).await);
    assert!(is_empty(&fixture.cold_store, "erased/").await);
    assert!(!is_empty(&fixture.cold_store, ".keyed/survivor/").await);
    assert!(!is_empty(&fixture.cold_store, "survivor/").await);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn u23_purge_without_every_drain_ack_erases_nothing() {
    let healthy = StubIndexer::new(StatusCode::OK, None);
    let failing = StubIndexer::new(StatusCode::SERVICE_UNAVAILABLE, None);
    let urls = vec![healthy.serve().await, failing.serve().await];
    let fixture = fixture("erased", urls).await;

    let report = purge(&fixture.app, "erased").await;
    assert_eq!(report["keyed_drain_complete"], false, "{report}");
    assert!(
        report["keyed_drain_error"]
            .as_str()
            .is_some_and(|error| error.contains("503"))
    );
    assert_eq!(report["cold_gc_complete"], false);
    assert_eq!(report["keyed_prefix_absent"], false);
    assert_eq!(report["removed_streams"], 1);
    assert!(!is_empty(&fixture.cold_store, &fixture.keyed_prefix).await);
    assert!(!is_empty(&fixture.cold_store, "erased/").await);

    // An unreachable pod also blocks erasure.
    let unreachable = fixture_with_unreachable_indexer().await;
    let report = purge(&unreachable.app, "erased").await;
    assert_eq!(report["keyed_drain_complete"], false, "{report}");
    assert!(!is_empty(&unreachable.cold_store, &unreachable.keyed_prefix).await);
}

async fn fixture_with_unreachable_indexer() -> Fixture {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("reserve port");
    let addr = listener.local_addr().expect("addr");
    drop(listener);
    fixture("erased", vec![format!("http://{addr}")]).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn u24_metrics_expose_keyed_state_requests_and_group_state_gauges() {
    let fixture = fixture("metrics", Vec::new()).await;
    let response = fixture
        .app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/metrics/harness/keyed-state?key=a")
                .body(Body::empty())
                .expect("keyed-state request"),
        )
        .await
        .expect("keyed-state response");
    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    let response = fixture
        .app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/__ursula/metrics")
                .body(Body::empty())
                .expect("metrics request"),
        )
        .await
        .expect("metrics response");
    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("metrics body");
    let metrics: serde_json::Value = serde_json::from_slice(&body).expect("metrics JSON");
    assert_eq!(
        metrics["keyed_state_requests"]["status_404"], 1,
        "{metrics}"
    );
    assert_eq!(metrics["keyed_state_requests"]["status_200"], 0);
    let groups = metrics["group_state_gauges"]
        .as_array()
        .expect("group gauges array");
    assert!(!groups.is_empty());
    for group in groups {
        assert_eq!(group["feature_level"], FEATURE_LEVEL_KEYED_STREAMS);
        for gauge in [
            "dense_record_entries",
            "record_marks",
            "shared_refs",
            "live_packs",
            "staged_external_refs",
        ] {
            assert!(group[gauge].is_u64(), "{gauge} in {group}");
        }
    }
}

//! The request and snapshot limits over HTTP (#581 section 6.4): HEAD
//! advertises them and every `413` names the one it applied, as the shared
//! header crate parses them.

use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

use axum::Router;
use axum::body::Body;
use axum::body::Bytes;
use axum::http::Request;
use axum::http::StatusCode;
use axum::http::header::CONTENT_LENGTH;
use axum::http::header::CONTENT_TYPE;
use axum::response::Response;
use tower::ServiceExt;
use ursula_headers::SizeLimit;
use ursula_headers::StreamHead;
use ursula_headers::value::Offset;
use ursula_runtime::ColdStore;
use ursula_runtime::InMemoryGroupEngineFactory;
use ursula_runtime::MAX_COLD_SNAPSHOT_BYTES;
use ursula_runtime::RuntimeConfig;
use ursula_runtime::ShardRuntime;

use crate::router;

const OCTET: &str = "application/octet-stream";
const REQUEST_LIMIT: u64 = 32 * 1024 * 1024;

fn app() -> Router {
    let runtime = ShardRuntime::spawn_with_engine_factory(
        RuntimeConfig::new(1, 1),
        InMemoryGroupEngineFactory::default(),
    )
    .expect("runtime");
    router(runtime)
}

fn cold_app() -> Router {
    let cold_store = Arc::new(ColdStore::memory().expect("memory cold store"));
    let runtime = ShardRuntime::spawn_with_engine_factory_and_cold_store(
        RuntimeConfig::new(1, 1),
        InMemoryGroupEngineFactory::with_cold_store(Some(cold_store.clone())),
        Some(cold_store),
    )
    .expect("runtime");
    router(runtime)
}

async fn send(app: &Router, request: Request<Body>) -> Response {
    app.clone().oneshot(request).await.expect("response")
}

async fn create(app: &Router, uri: &str) {
    let response = send(
        app,
        Request::put(uri)
            .header(CONTENT_TYPE, OCTET)
            .body(Body::from("ab"))
            .expect("request"),
    )
    .await;
    assert_eq!(response.status(), StatusCode::CREATED);
}

fn pairs(response: &Response) -> impl Iterator<Item = (&str, &[u8])> {
    response
        .headers()
        .iter()
        .map(|(name, value)| (name.as_str(), value.as_bytes()))
}

fn named_limit(response: &Response) -> Option<SizeLimit> {
    SizeLimit::parse(response.status().as_u16(), pairs(response)).expect("well-formed limit")
}

/// A body that records in `polled` whether the server polled it. The
/// caller declares its length in `Content-Length`.
fn watched(polled: &Arc<AtomicBool>) -> Body {
    let polled = Arc::clone(polled);
    Body::from_stream(futures_util::stream::poll_fn(move |_cx| {
        polled.store(true, Ordering::SeqCst);
        std::task::Poll::Ready(None::<Result<Bytes, std::convert::Infallible>>)
    }))
}

/// A body without `Content-Length` that carries `length` bytes in 1 MiB
/// chunks.
fn chunked(length: usize) -> Body {
    const CHUNK: usize = 1 << 20;
    let mut left = length;
    Body::from_stream(futures_util::stream::poll_fn(move |_cx| {
        if left == 0 {
            return std::task::Poll::Ready(None);
        }
        let take = left.min(CHUNK);
        left = left.saturating_sub(take);
        std::task::Poll::Ready(Some(Ok::<_, std::convert::Infallible>(Bytes::from(
            vec![b'x'; take],
        ))))
    }))
}

#[tokio::test]
async fn head_advertises_the_request_limit_and_no_snapshot_limit_without_a_cold_store() {
    let app = app();
    create(&app, "/limits/s").await;
    let response = send(
        &app,
        Request::head("/limits/s")
            .body(Body::empty())
            .expect("request"),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let head = StreamHead::parse(pairs(&response)).expect("stream head");
    assert_eq!(head.max_request_bytes(), REQUEST_LIMIT);
    assert_eq!(head.max_snapshot_bytes(), None);
    assert_eq!(head.next_offset(), Offset::from_u64(2));
    assert_eq!(head.retained_offset(), Offset::from_u64(0));
    assert!(!head.closed());
    assert_eq!(head.snapshot(), None);
}

#[tokio::test]
async fn head_advertises_the_snapshot_limit_with_a_cold_store() {
    let app = cold_app();
    create(&app, "/limits/s").await;
    let response = send(
        &app,
        Request::put("/limits/s/snapshot/2")
            .header(CONTENT_TYPE, OCTET)
            .body(Body::from("snapshot"))
            .expect("request"),
    )
    .await;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    let response = send(
        &app,
        Request::head("/limits/s")
            .body(Body::empty())
            .expect("request"),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let head = StreamHead::parse(pairs(&response)).expect("stream head");
    assert_eq!(head.max_request_bytes(), REQUEST_LIMIT);
    assert_eq!(head.max_snapshot_bytes(), Some(MAX_COLD_SNAPSHOT_BYTES));
    assert_eq!(
        head.snapshot().map(|snapshot| snapshot.offset),
        Some(Offset::from_u64(2))
    );
}

#[tokio::test]
async fn a_declared_body_over_the_request_limit_is_refused_before_it_is_read() {
    let app = app();
    create(&app, "/limits/s").await;
    for (method, uri) in [("POST", "/limits/s"), ("PUT", "/limits/t")] {
        let polled = Arc::new(AtomicBool::new(false));
        let response = send(
            &app,
            Request::builder()
                .method(method)
                .uri(uri)
                .header(CONTENT_TYPE, OCTET)
                .header(CONTENT_LENGTH, REQUEST_LIMIT + 1)
                .body(watched(&polled))
                .expect("request"),
        )
        .await;
        assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE, "{method}");
        assert_eq!(
            named_limit(&response),
            Some(SizeLimit::Request(REQUEST_LIMIT)),
            "{method}"
        );
        assert!(!polled.load(Ordering::SeqCst), "{method} read the body");
    }
}

#[tokio::test]
async fn a_chunked_body_crossing_the_request_limit_names_it() {
    let app = app();
    create(&app, "/limits/s").await;
    let over = usize::try_from(REQUEST_LIMIT).expect("limit fits usize") + 1;
    // An append counts its own body, and a create's body goes through
    // axum's body limit, whose bare `413` the router types.
    for (method, uri) in [("POST", "/limits/s"), ("PUT", "/limits/t")] {
        let response = send(
            &app,
            Request::builder()
                .method(method)
                .uri(uri)
                .header(CONTENT_TYPE, OCTET)
                .body(chunked(over))
                .expect("request"),
        )
        .await;
        assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE, "{method}");
        assert_eq!(
            named_limit(&response),
            Some(SizeLimit::Request(REQUEST_LIMIT)),
            "{method}"
        );
    }
}

#[tokio::test]
async fn a_snapshot_over_the_request_limit_without_a_cold_store_names_the_request_limit() {
    let app = app();
    create(&app, "/limits/s").await;
    let polled = Arc::new(AtomicBool::new(false));
    let response = send(
        &app,
        Request::put("/limits/s/snapshot/2")
            .header(CONTENT_TYPE, OCTET)
            .header(CONTENT_LENGTH, REQUEST_LIMIT + 1)
            .body(watched(&polled))
            .expect("request"),
    )
    .await;
    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(
        named_limit(&response),
        Some(SizeLimit::Request(REQUEST_LIMIT))
    );
    assert!(!polled.load(Ordering::SeqCst), "read the body");

    let over = usize::try_from(REQUEST_LIMIT).expect("limit fits usize") + 1;
    let response = send(
        &app,
        Request::put("/limits/s/snapshot/2")
            .header(CONTENT_TYPE, OCTET)
            .body(chunked(over))
            .expect("request"),
    )
    .await;
    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(
        named_limit(&response),
        Some(SizeLimit::Request(REQUEST_LIMIT))
    );
}

#[tokio::test]
async fn a_snapshot_over_the_snapshot_limit_names_it() {
    let app = cold_app();
    create(&app, "/limits/s").await;
    let polled = Arc::new(AtomicBool::new(false));
    let response = send(
        &app,
        Request::put("/limits/s/snapshot/2")
            .header(CONTENT_TYPE, OCTET)
            .header(CONTENT_LENGTH, MAX_COLD_SNAPSHOT_BYTES + 1)
            .body(watched(&polled))
            .expect("request"),
    )
    .await;
    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(
        named_limit(&response),
        Some(SizeLimit::Snapshot(MAX_COLD_SNAPSHOT_BYTES))
    );
    assert!(!polled.load(Ordering::SeqCst), "read the body");
}

//! Per-request cost of the client plane's front door (#602): the clone of the
//! shared HTTP state, which the router makes several times for every request,
//! and one tokened producer append of 32 bytes through the whole client
//! router (layers, routing before the body, the handler) on an in-memory
//! node with one core, and one write the admission layer refuses for its
//! declared size, which measures the router alone.

use std::hint::black_box;

use axum::Router;
use axum::body::Body;
use axum::http::Method;
use axum::http::Request;
use axum::http::StatusCode;
use axum::http::header::CONTENT_LENGTH;
use axum::http::header::CONTENT_TYPE;
use criterion::Criterion;
use criterion::criterion_group;
use criterion::criterion_main;
use tower::ServiceExt;
use ursula::HttpState;
use ursula::IngressAdmission;
use ursula::Topology;
use ursula::client_router_with_admission;
use ursula::spawn_runtime;

const STREAM_PATH: &str = "/bench/front-door";

async fn call(router: &Router, request: Request<Body>) -> axum::response::Response {
    router
        .clone()
        .oneshot(request)
        .await
        .expect("the router is infallible")
}

/// A node with one core and one in-memory group, its client router, and
/// the token of a created stream.
async fn node() -> (HttpState, Router, String) {
    let mut config = ursula_config::UrsulaConfig::default();
    config.runtime.core_count = 1;
    config.raft.group_count = Some(1);
    let runtime = spawn_runtime(&config, Topology::Development { group_count: 1 })
        .expect("runtime")
        .runtime;
    let state = HttpState::new(runtime);
    let router = client_router_with_admission(state.clone(), IngressAdmission::default());
    let created = call(
        &router,
        Request::builder()
            .method(Method::PUT)
            .uri(STREAM_PATH)
            .header(CONTENT_TYPE, "application/octet-stream")
            .body(Body::empty())
            .expect("request"),
    )
    .await;
    assert!(
        created.status().is_success(),
        "create: {}",
        created.status()
    );
    let head = call(
        &router,
        Request::builder()
            .method(Method::HEAD)
            .uri(STREAM_PATH)
            .body(Body::empty())
            .expect("request"),
    )
    .await;
    let incarnation = head
        .headers()
        .get("stream-incarnation")
        .and_then(|value| value.to_str().ok())
        .expect("a stream answers its incarnation")
        .to_owned();
    (state, router, incarnation)
}

fn front_door(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .expect("tokio runtime");
    let (state, router, incarnation) = rt.block_on(node());

    c.bench_function("front_door/http_state_clone", |b| {
        b.iter(|| black_box(state.clone()));
    });

    let payload = [0x5a_u8; 32];
    let mut seq: u64 = 0;
    c.bench_function("front_door/tokened_append_32b", |b| {
        b.iter(|| {
            let request = Request::builder()
                .method(Method::POST)
                .uri(STREAM_PATH)
                .header(CONTENT_TYPE, "application/octet-stream")
                .header("producer-id", "bench")
                .header("producer-epoch", "0")
                .header("producer-seq", seq.to_string())
                .header("stream-incarnation", incarnation.as_str())
                .body(Body::from(payload.to_vec()))
                .expect("request");
            seq = seq.wrapping_add(1);
            let response = rt.block_on(call(&router, request));
            assert!(
                response.status().is_success(),
                "append: {}",
                response.status()
            );
        });
    });

    // Refused for its declared size by the admission layer before routing
    // reaches the handler: the router's own per-request cost, without the
    // core thread.
    c.bench_function("front_door/oversize_413", |b| {
        b.iter(|| {
            let request = Request::builder()
                .method(Method::POST)
                .uri(STREAM_PATH)
                .header(CONTENT_TYPE, "application/octet-stream")
                .header(CONTENT_LENGTH, "1099511627776")
                .body(Body::empty())
                .expect("request");
            let response = rt.block_on(call(&router, request));
            assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
        });
    });
}

criterion_group!(benches, front_door);
criterion_main!(benches);

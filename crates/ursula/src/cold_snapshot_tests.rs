//! Bounded-state F16 over HTTP: a snapshot body above the 32 MiB inline cap
//! is staged in the cold tier and streamed back by snapshot reads and
//! `/bootstrap`.

use std::sync::Arc;

use axum::body::Body;
use axum::body::to_bytes;
use axum::http::Request;
use tower::ServiceExt;
use ursula_runtime::ColdStore;
use ursula_runtime::InMemoryGroupEngineFactory;
use ursula_runtime::RuntimeConfig;
use ursula_runtime::ShardRuntime;
use ursula_runtime::SnapshotDigest;
use ursula_runtime::cold_external_dir;
use ursula_shard::BucketStreamId;

use super::*;

const OCTET: &str = "application/octet-stream";

async fn send(app: &Router, method: &str, uri: &str, body: Body) -> Response {
    let request = Request::builder()
        .method(method)
        .uri(uri)
        .header(CONTENT_TYPE, OCTET)
        .body(body)
        .expect("request");
    app.clone().oneshot(request).await.expect("response")
}

async fn body_of(response: Response) -> Vec<u8> {
    to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body")
        .to_vec()
}

#[tokio::test]
async fn snapshot_above_the_inline_cap_round_trips_through_the_cold_tier() {
    let cold_store = Arc::new(ColdStore::memory().expect("memory cold store"));
    let runtime = ShardRuntime::spawn_with_engine_factory_and_cold_store(
        RuntimeConfig::new(1, 1),
        InMemoryGroupEngineFactory::with_cold_store(Some(cold_store.clone())),
        Some(cold_store.clone()),
    )
    .expect("runtime");
    let app = router(runtime.clone());
    let response = send(&app, "PUT", "/cold-snapshots/s", Body::from("ab")).await;
    assert_eq!(response.status(), StatusCode::CREATED);

    let snapshot = (0..MAX_HTTP_BODY_BYTES + (1 << 20))
        .map(|index| (index % 251) as u8)
        .collect::<Vec<_>>();
    let response = send(
        &app,
        "PUT",
        "/cold-snapshots/s/snapshot/2",
        Body::from(snapshot.clone()),
    )
    .await;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    let mut digest = SnapshotDigest::new(OCTET);
    digest.update(&snapshot);
    let digest = digest.finalize();
    assert_eq!(
        response.headers()[HEADER_STREAM_SNAPSHOT_DIGEST],
        digest.as_str(),
        "the staged digest equals the inline one"
    );
    let staged = cold_store
        .list_file_names(&cold_external_dir(&BucketStreamId::new(
            "cold-snapshots",
            "s",
        )))
        .await
        .expect("list external dir");
    assert_eq!(staged.len(), 1, "{staged:?}");

    let response = send(&app, "GET", "/cold-snapshots/s/snapshot/2", Body::empty()).await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers()[HEADER_STREAM_SNAPSHOT_DIGEST],
        digest.as_str()
    );
    assert_eq!(
        response.headers()[CONTENT_LENGTH],
        snapshot.len().to_string().as_str()
    );
    assert!(body_of(response).await == snapshot);

    let response = send(&app, "GET", "/cold-snapshots/s/bootstrap", Body::empty()).await;
    assert_eq!(response.status(), StatusCode::OK);
    let length = response.headers()[CONTENT_LENGTH]
        .to_str()
        .expect("length")
        .parse::<usize>()
        .expect("length");
    let body = body_of(response).await;
    assert_eq!(body.len(), length);
    let start = body
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .expect("part headers")
        + 4;
    assert!(body[start..start + snapshot.len()] == snapshot[..]);
    assert!(body[start + snapshot.len()..].starts_with(b"\r\n--ursula-bootstrap-"));
}

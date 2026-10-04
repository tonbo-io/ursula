//! F1 sparse cold record marks over HTTP (bounded-stream-state §5.2, §6):
//! record reads, SSE, snapshot publish and retention keep exact record
//! coordinates over sealed history (RC-6, RC-7, RC-8, RC-12, RC-13, RC-14).

use std::sync::Arc;

use axum::body::Body;
use axum::body::to_bytes;
use axum::http::Request;
use tower::ServiceExt;
use ursula_runtime::ColdStore;
use ursula_runtime::InMemoryGroupEngineFactory;
use ursula_runtime::PlanColdFlushRequest;
use ursula_runtime::RuntimeConfig;
use ursula_runtime::ShardRuntime;
use ursula_shard::BucketStreamId;
use ursula_shard::RaftGroupId;

use super::*;

const RECORD: u64 = 1_000;

async fn send(
    app: &Router,
    method: &str,
    uri: &str,
    headers: &[(&str, &str)],
    body: Body,
) -> Response {
    let mut request = Request::builder().method(method).uri(uri);
    for (name, value) in headers {
        request = request.header(*name, *value);
    }
    app.clone()
        .oneshot(request.body(body).expect("request"))
        .await
        .expect("response")
}

async fn body_of(response: Response) -> Vec<u8> {
    to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body")
        .to_vec()
}

fn header<'a>(response: &'a Response, name: &str) -> &'a str {
    response
        .headers()
        .get(name)
        .unwrap_or_else(|| panic!("missing header {name}"))
        .to_str()
        .expect("header utf8")
}

fn spawn_with_cold_store() -> (ShardRuntime, Router) {
    let cold_store = Arc::new(ColdStore::memory().expect("memory cold store"));
    let runtime = ShardRuntime::spawn_with_engine_factory_and_cold_store(
        RuntimeConfig::new(1, 1),
        InMemoryGroupEngineFactory::with_cold_store(Some(cold_store.clone())),
        Some(cold_store),
    )
    .expect("runtime");
    let app = router(runtime.clone());
    (runtime, app)
}

async fn complete_repair_cycle(runtime: &ShardRuntime) {
    for _ in 0..16 {
        runtime.repair_cold_index_all_groups_once(64).await;
        if runtime.cold_index_repair_completed(RaftGroupId(0)) {
            return;
        }
    }
    panic!("repair cycle did not complete");
}

/// A JSON stream of `count` records of [`RECORD`] bytes, flushed
/// cold and sealed except for a hot tail of `hot` records.
async fn sealed_stream(count: u64, hot: u64) -> (ShardRuntime, Router, Vec<u8>) {
    let (runtime, app) = spawn_with_cold_store();
    complete_repair_cycle(&runtime).await;
    let record = |index: u64| {
        let mut line = format!("{{\"i\":{index},\"pad\":\"");
        line.push_str(&"p".repeat(usize::try_from(RECORD).unwrap() - line.len() - 3));
        line.push_str("\"}\n");
        line
    };
    let mut bytes = Vec::new();
    for index in 0..count {
        bytes.extend_from_slice(record(index).as_bytes());
    }
    // JSON array bodies; the server stores one compact record per value.
    let array = |from: u64, to: u64| {
        let values = (from..to)
            .map(|index| record(index).trim_end().to_owned())
            .collect::<Vec<_>>();
        format!("[{}]", values.join(","))
    };
    // Above 1 MiB the create body goes external and seals at once.
    let response = send(
        &app,
        "PUT",
        "/sparse-marks/log",
        &[(CONTENT_TYPE.as_str(), "application/json")],
        Body::from(array(0, (count - hot) / 2)),
    )
    .await;
    let status = response.status();
    assert_eq!(
        status,
        StatusCode::CREATED,
        "{:?}",
        String::from_utf8_lossy(&body_of(response).await)
    );
    let response = send(
        &app,
        "POST",
        "/sparse-marks/log",
        &[(CONTENT_TYPE.as_str(), "application/json")],
        Body::from(array((count - hot) / 2, count - hot)),
    )
    .await;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    let stream = BucketStreamId::new("sparse-marks", "log");
    while runtime
        .flush_cold_once(PlanColdFlushRequest {
            stream_id: stream.clone(),
            min_hot_bytes: 1,
            max_flush_bytes: 300_007,
        })
        .await
        .expect("flush")
        .is_some()
    {}
    if hot > 0 {
        let response = send(
            &app,
            "POST",
            "/sparse-marks/log",
            &[(CONTENT_TYPE.as_str(), "application/json")],
            Body::from(array(count - hot, count)),
        )
        .await;
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
    }
    let gauges = runtime.state_gauges(RaftGroupId(0)).await.expect("gauges");
    assert!(gauges.record_marks > 1, "{gauges:?}");
    assert_eq!(gauges.dense_record_entries, hot);
    (runtime, app, bytes)
}

fn slice(bytes: &[u8], first: u64, next: u64) -> &[u8] {
    &bytes[usize::try_from(first * RECORD).unwrap()..usize::try_from(next * RECORD).unwrap()]
}

/// RC-6, RC-7: record reads and `tail_records` over sealed, hot and
/// straddling ranges return the dense layout's bytes and headers.
#[tokio::test]
async fn record_reads_and_tail_records_cross_sealed_history() {
    let (_runtime, app, bytes) = sealed_stream(4_000, 30).await;
    for (query, first, next) in [
        ("record=0&max_records=3", 0, 3),
        ("record=1234&max_records=50", 1_234, 1_284),
        ("record=1234&max_records=50&max_bytes=10000", 1_234, 1_244),
        ("record=3960&max_records=20", 3_960, 3_980),
        ("record=3999&max_records=5", 3_999, 4_000),
        ("tail_records=45", 3_955, 4_000),
    ] {
        let response = send(
            &app,
            "GET",
            &format!("/sparse-marks/log?{query}"),
            &[],
            Body::empty(),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK, "{query}");
        assert_eq!(
            header(&response, HEADER_STREAM_RECORD_START),
            first.to_string(),
            "{query}"
        );
        assert_eq!(
            header(&response, HEADER_STREAM_RECORD_NEXT),
            next.to_string(),
            "{query}"
        );
        assert_eq!(
            header(&response, HEADER_STREAM_NEXT_OFFSET),
            format!("{:020}", next * RECORD),
            "{query}"
        );
        assert_eq!(
            body_of(response).await,
            slice(&bytes, first, next),
            "{query}"
        );
    }
}

/// RC-8: SSE by record over sealed history, one record per event in the
/// envelope view, continues with server-side anchors and returns every
/// record in order.
#[tokio::test]
async fn sse_envelope_reads_every_sealed_record() {
    let (_runtime, app, _bytes) = sealed_stream(2_500, 0).await;
    let response = send(
        &app,
        "POST",
        "/sparse-marks/log",
        &[
            (CONTENT_TYPE.as_str(), "application/json"),
            (HEADER_STREAM_CLOSED, "true"),
        ],
        Body::empty(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    let response = send(
        &app,
        "GET",
        "/sparse-marks/log?record=1040&record_view=envelope&live=sse",
        &[],
        Body::empty(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = String::from_utf8(body_of(response).await).expect("utf8");
    assert_eq!(body.matches("event: data").count(), 1_460);
    let mut expected = 1_040;
    for line in body
        .lines()
        .filter(|line| line.starts_with("data:{\"record\""))
    {
        let envelope: serde_json::Value =
            serde_json::from_str(line.trim_start_matches("data:")).expect("envelope");
        assert_eq!(envelope["record"], expected);
        assert_eq!(envelope["value"]["i"], expected);
        expected += 1;
    }
    assert_eq!(expected, 2_500);
    assert!(body.contains("\"streamNextRecord\":2500"));
}

/// RC-12, RC-13, RC-14: raw-offset routes reject intra-record offsets in
/// sealed history with 400 before proposing; record routes publish exactly
/// and retain onto the mark at or below the target, reporting the effective
/// boundary, and surviving records keep their ordinals.
#[tokio::test]
async fn snapshot_and_retention_respect_sealed_record_boundaries() {
    let (_runtime, app, bytes) = sealed_stream(4_000, 10).await;
    let snapshot = |uri: String| {
        let app = app.clone();
        async move {
            send(
                &app,
                "PUT",
                &uri,
                &[(CONTENT_TYPE.as_str(), "application/json")],
                Body::from(r#"{"state":1}"#),
            )
            .await
        }
    };
    // Intra-record cold offsets: 400 (today's frontier rule accepted them).
    let response = snapshot(format!("/sparse-marks/log/snapshot/{}", 1_500 * RECORD + 7)).await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let response = send(
        &app,
        "PUT",
        &format!("/sparse-marks/log/retention/{}", 1_500 * RECORD + 7),
        &[],
        Body::empty(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    // A sealed record boundary publishes exactly.
    let response = snapshot("/sparse-marks/log/snapshot?record=2500".to_owned()).await;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    assert_eq!(
        header(&response, HEADER_STREAM_SNAPSHOT_OFFSET),
        format!("{:020}", 2_500 * RECORD)
    );
    // Retention to a sealed record lands on its block's mark.
    let response = send(
        &app,
        "PUT",
        "/sparse-marks/log/retention?record=2500",
        &[],
        Body::empty(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    let retained: u64 = header(&response, HEADER_STREAM_RETAINED_OFFSET)
        .parse()
        .expect("retained offset");
    let first: u64 = header(&response, HEADER_STREAM_RECORD_FIRST)
        .parse()
        .expect("first record");
    assert_eq!(retained, first * RECORD);
    assert!(first <= 2_500 && 2_500 * RECORD - retained < ursula_runtime::MARK_BLOCK_BYTES);
    assert_eq!(
        retained / ursula_runtime::MARK_BLOCK_BYTES,
        2_500 * RECORD / ursula_runtime::MARK_BLOCK_BYTES
    );

    let response = send(
        &app,
        "GET",
        &format!("/sparse-marks/log?record={}&max_records=1", first - 1),
        &[],
        Body::empty(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::GONE);
    let response = send(
        &app,
        "GET",
        &format!("/sparse-marks/log?record={first}&max_records=3"),
        &[],
        Body::empty(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(body_of(response).await, slice(&bytes, first, first + 3));
}

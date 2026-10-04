//! Pins of the base protocol contract that the 0.6.0 removals must keep:
//! `Stream-Seq` as a compare-and-set, `Stream-Next-Offset` as the exact
//! resume point, the HEAD snapshot and retention headers, and `Retry-After`
//! on an append's temporary 503. The other error headers are pinned in
//! `tests.rs`: `producer_headers_deduplicate_retries_and_fence_stale_epochs`,
//! `long_poll_returns_service_unavailable_when_live_waiters_are_full` (a
//! read's temporary 503) and
//! `ingress_body_budget_rejects_write_when_budget_is_exhausted`.
//!
//! Offsets are opaque here: an offset is only ever echoed back or compared
//! with another offset, never computed. A failing pin is a finding to
//! triage; a later change may edit an assertion only with a named reason.

use std::sync::Arc;

use axum::body::Body;
use axum::body::to_bytes;
use axum::http::Request;
use tower::ServiceExt;
use ursula_runtime::ColdStore;
use ursula_runtime::InMemoryGroupEngineFactory;
use ursula_runtime::RuntimeConfig;

use super::*;

const OCTET: &str = "application/octet-stream";

fn app() -> Router {
    router(ShardRuntime::spawn(RuntimeConfig::new(1, 1)).expect("runtime"))
}

async fn send(
    app: &Router,
    method: &str,
    uri: &str,
    headers: &[(&str, &str)],
    body: &str,
) -> Response {
    let mut request = Request::builder().method(method).uri(uri);
    for (name, value) in headers {
        request = request.header(*name, *value);
    }
    app.clone()
        .oneshot(request.body(Body::from(body.to_owned())).expect("request"))
        .await
        .expect("response")
}

/// `PUT` of an octet stream.
async fn create(app: &Router, uri: &str, headers: &[(&str, &str)], body: &str) -> Response {
    let mut all = vec![(CONTENT_TYPE.as_str(), OCTET)];
    all.extend_from_slice(headers);
    send(app, "PUT", uri, &all, body).await
}

/// `POST` of an octet-stream body.
async fn append(app: &Router, uri: &str, headers: &[(&str, &str)], body: &str) -> Response {
    let mut all = vec![(CONTENT_TYPE.as_str(), OCTET)];
    all.extend_from_slice(headers);
    send(app, "POST", uri, &all, body).await
}

#[track_caller]
fn header<'a>(response: &'a Response, name: &str) -> &'a str {
    response
        .headers()
        .get(name)
        .unwrap_or_else(|| panic!("missing header {name}"))
        .to_str()
        .expect("utf-8 header")
}

fn next_offset(response: &Response) -> String {
    header(response, HEADER_STREAM_NEXT_OFFSET).to_owned()
}

async fn body_text(response: Response) -> String {
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    String::from_utf8(body.to_vec()).expect("utf-8 body")
}

/// The tail as HEAD reports it.
async fn tail(app: &Router, uri: &str) -> String {
    let response = send(app, "HEAD", uri, &[], "").await;
    assert_eq!(response.status(), StatusCode::OK);
    next_offset(&response)
}

/// Reads the stream from the start in 2-byte pages, each continued from the
/// previous page's `Stream-Next-Offset`, until `Stream-Up-To-Date`.
async fn read_all(app: &Router, uri: &str) -> String {
    let mut offset = "-1".to_owned();
    let mut text = String::new();
    for _ in 0..64 {
        let response = send(
            app,
            "GET",
            &format!("{uri}?offset={offset}&max_bytes=2"),
            &[],
            "",
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        offset = next_offset(&response);
        let up_to_date = response.headers().contains_key(HEADER_STREAM_UP_TO_DATE);
        text.push_str(&body_text(response).await);
        if up_to_date {
            return text;
        }
    }
    panic!("{uri} never reached Stream-Up-To-Date");
}

/// A fixed-width `Stream-Seq`, so bytewise order is numeric order.
fn pad20(n: usize) -> String {
    format!("{n:020}")
}

/// One commit of `count` newline-terminated events.
fn events(prefix: &str, count: usize) -> String {
    (0..count).map(|i| format!("{prefix}{i}\n")).collect()
}

#[tokio::test]
async fn stream_seq_is_a_strict_bytewise_compare_and_set_per_stream() {
    let app = app();
    let a = "/contract/seq-a";
    let b = "/contract/seq-b";

    // PUT seeds the stored value.
    let response = create(&app, a, &[(HEADER_STREAM_SEQ, "0005")], "").await;
    assert_eq!(response.status(), StatusCode::CREATED);
    let response = create(&app, b, &[], "").await;
    assert_eq!(response.status(), StatusCode::CREATED);

    // An equal value answers 409 carrying the tail.
    let response = append(&app, a, &[(HEADER_STREAM_SEQ, "0005")], "x0").await;
    assert_eq!(response.status(), StatusCode::CONFLICT);
    assert_eq!(next_offset(&response), tail(&app, a).await);

    // Scope is per stream: the same value is fresh on each stream.
    let response = append(&app, a, &[(HEADER_STREAM_SEQ, "0006")], "a1").await;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    let response = append(&app, b, &[(HEADER_STREAM_SEQ, "0006")], "b1").await;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    // An append without the header is accepted and leaves the stored value
    // where it was.
    let response = append(&app, a, &[], "a2").await;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    let response = append(&app, a, &[(HEADER_STREAM_SEQ, "0006")], "x1").await;
    assert_eq!(response.status(), StatusCode::CONFLICT);
    assert_eq!(next_offset(&response), tail(&app, a).await);

    // The comparison is bytewise, not numeric: "10" sorts below "9".
    let response = append(&app, a, &[(HEADER_STREAM_SEQ, "9")], "a3").await;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    let response = append(&app, a, &[(HEADER_STREAM_SEQ, "10")], "x2").await;
    assert_eq!(response.status(), StatusCode::CONFLICT);
    assert_eq!(next_offset(&response), tail(&app, a).await);

    // Rejected appends commit nothing.
    assert_eq!(read_all(&app, a).await, "a1a2a3");
    assert_eq!(read_all(&app, b).await, "b1");
}

#[tokio::test]
async fn stream_seq_check_runs_after_producer_dedup() {
    let app = app();
    let uri = "/contract/seq-after-dedup";
    let response = create(&app, uri, &[], "").await;
    assert_eq!(response.status(), StatusCode::CREATED);

    let first_try = [
        (HEADER_PRODUCER_ID, "writer"),
        (HEADER_PRODUCER_EPOCH, "0"),
        (HEADER_PRODUCER_SEQ, "0"),
        (HEADER_STREAM_SEQ, "0001"),
    ];
    let response = append(&app, uri, &first_try, "a").await;
    assert_eq!(response.status(), StatusCode::OK);
    let original_ack = next_offset(&response);
    assert_eq!(original_ack, tail(&app, uri).await);

    let response = append(
        &app,
        uri,
        &[
            (HEADER_PRODUCER_ID, "writer"),
            (HEADER_PRODUCER_EPOCH, "0"),
            (HEADER_PRODUCER_SEQ, "1"),
            (HEADER_STREAM_SEQ, "0002"),
        ],
        "bb",
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);

    // The retry's Stream-Seq is now stale, but the producer duplicate is
    // answered first: 204 with the original ack, not 409, not the tail and
    // not the producer's latest ack.
    let response = append(&app, uri, &first_try, "a").await;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    assert_eq!(next_offset(&response), original_ack);

    assert_eq!(read_all(&app, uri).await, "abb");
}

/// The prefix-count compare-and-set: a writer reads to the tail
/// (`Stream-Up-To-Date`) and appends one commit with `Stream-Seq` = the
/// number of events it read. Every commit holds at least one event, so a
/// count taken at the tail beats the stored value only if no commit landed
/// since. A count taken inside a commit (a read cut by `max_bytes`) can beat
/// it without the writer having seen the whole log.
#[tokio::test]
async fn prefix_count_compare_and_set_admits_one_writer_per_prefix() {
    let app = app();
    let uri = "/contract/prefix-count";
    let response = create(&app, uri, &[], "").await;
    assert_eq!(response.status(), StatusCode::CREATED);

    let base = events("e", 3);
    let response = append(&app, uri, &[(HEADER_STREAM_SEQ, pad20(0).as_str())], &base).await;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    let seen = next_offset(&response);

    // Both writers have seen 3 events and send that count.
    let a_commit = events("a", 1);
    let b_commit = events("b", 2);
    let seq = pad20(3);
    let headers = [(HEADER_STREAM_SEQ, seq.as_str())];
    let (a, b) = tokio::join!(
        append(&app, uri, &headers, &a_commit),
        append(&app, uri, &headers, &b_commit),
    );
    let (loser, winner_commit, loser_commit) = match (a.status(), b.status()) {
        (StatusCode::NO_CONTENT, StatusCode::CONFLICT) => (b, a_commit, b_commit),
        (StatusCode::CONFLICT, StatusCode::NO_CONTENT) => (a, b_commit, a_commit),
        statuses => panic!("exactly one writer must win the prefix: {statuses:?}"),
    };

    // The loser's 409 carries the tail; it reads only the missing suffix.
    let loser_tail = next_offset(&loser);
    assert_eq!(loser_tail, tail(&app, uri).await);
    let response = send(&app, "GET", &format!("{uri}?offset={seen}"), &[], "").await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(next_offset(&response), loser_tail);
    let suffix = body_text(response).await;
    assert_eq!(suffix, winner_commit);

    // It retries at n + k and wins.
    let seq = pad20(3 + suffix.lines().count());
    let response = append(
        &app,
        uri,
        &[(HEADER_STREAM_SEQ, seq.as_str())],
        &loser_commit,
    )
    .await;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    assert_eq!(
        read_all(&app, uri).await,
        format!("{base}{winner_commit}{loser_commit}")
    );
}

/// Counterexample: `Stream-Seq` = the last slot of the append is not a
/// compare-and-set. A writer that missed a commit still sends a larger
/// value whenever its own commit is larger.
#[tokio::test]
async fn last_slot_stream_seq_admits_a_stale_writer() {
    let app = app();
    let uri = "/contract/last-slot";
    let response = create(&app, uri, &[], "").await;
    assert_eq!(response.status(), StatusCode::CREATED);

    let response = append(
        &app,
        uri,
        &[(HEADER_STREAM_SEQ, pad20(3).as_str())],
        &events("e", 3),
    )
    .await;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    // A and B have both seen 3 events. A commits one event (last slot 4).
    let response = append(
        &app,
        uri,
        &[(HEADER_STREAM_SEQ, pad20(4).as_str())],
        &events("a", 1),
    )
    .await;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    // B commits two events (last slot 5) against the stale log, and is
    // accepted too.
    let response = append(
        &app,
        uri,
        &[(HEADER_STREAM_SEQ, pad20(5).as_str())],
        &events("b", 2),
    )
    .await;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    assert_eq!(read_all(&app, uri).await.lines().count(), 6);
}

#[tokio::test]
async fn stream_next_offset_is_the_tail_and_the_next_read_start() {
    let app = app();
    let uri = "/contract/next-offset";

    // PUT 201 and PUT 200 carry the tail.
    let response = create(&app, uri, &[], "abc").await;
    assert_eq!(response.status(), StatusCode::CREATED);
    let created = next_offset(&response);
    assert_eq!(created, tail(&app, uri).await);
    let response = create(&app, uri, &[], "ignored").await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(next_offset(&response), created);

    // A 2xx append's ack is the next HEAD's tail.
    let response = append(&app, uri, &[], "defg").await;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    let first = next_offset(&response);
    assert_eq!(first, tail(&app, uri).await);
    let response = append(&app, uri, &[], "hi").await;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    let second = next_offset(&response);
    assert_eq!(second, tail(&app, uri).await);

    // ... and the start of the next read.
    let response = send(&app, "GET", &format!("{uri}?offset={first}"), &[], "").await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(next_offset(&response), second);
    assert_eq!(body_text(response).await, "hi");

    let response = create(&app, uri, &[], "").await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(next_offset(&response), second);

    // Paged reads continued from it see every byte once.
    assert_eq!(read_all(&app, uri).await, "abcdefghi");
}

#[tokio::test]
async fn head_reports_snapshot_and_retention_after_publish_and_advance() {
    let app = app();
    let uri = "/contract/head";
    let response = create(&app, uri, &[], "").await;
    assert_eq!(response.status(), StatusCode::CREATED);
    let response = append(&app, uri, &[], "abc").await;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    let at = next_offset(&response);

    let response = send(&app, "PUT", &format!("{uri}/snapshot/{at}"), &[], "state").await;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    let digest = header(&response, HEADER_STREAM_SNAPSHOT_DIGEST).to_owned();
    let response = send(&app, "PUT", &format!("{uri}/retention/{at}"), &[], "").await;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    assert_eq!(header(&response, HEADER_STREAM_RETAINED_OFFSET), at);

    let response = send(&app, "HEAD", uri, &[], "").await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(header(&response, HEADER_STREAM_SNAPSHOT_OFFSET), at);
    assert_eq!(header(&response, HEADER_STREAM_SNAPSHOT_DIGEST), digest);
    assert_eq!(header(&response, HEADER_STREAM_RETAINED_OFFSET), at);
}

#[tokio::test]
async fn temporary_unavailable_answers_carry_retry_after() {
    // A one-byte hot cap refuses any append of two or more bytes as a
    // temporary error, whatever the per-record charge.
    let cold_store = Arc::new(ColdStore::memory().expect("memory cold store"));
    let runtime = ShardRuntime::spawn_with_engine_factory_and_cold_store(
        RuntimeConfig::new(1, 1).with_cold_max_hot_bytes_per_group(Some(1)),
        InMemoryGroupEngineFactory::with_cold_store(Some(cold_store.clone())),
        Some(cold_store),
    )
    .expect("runtime");
    let app = router(runtime);
    let uri = "/contract/unavailable";
    let response = create(&app, uri, &[], "").await;
    assert_eq!(response.status(), StatusCode::CREATED);

    let response = append(&app, uri, &[], "ab").await;
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(header(&response, "retry-after"), "1");
    assert!(body_text(response).await.contains("ColdBackpressure"));
}

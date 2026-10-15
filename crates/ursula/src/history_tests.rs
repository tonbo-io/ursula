//! History identity over HTTP (#581 section 4): a restore through the import
//! route starts a new history, every answer is parsed with the shared header
//! crate (`StreamHead`, `IncarnationMismatch`, `ProducerAnswer`), and live
//! reads end when their token is retired.

use axum::Router;
use axum::body::Body;
use axum::body::Bytes;
use axum::body::to_bytes;
use axum::http::Request;
use axum::http::StatusCode;
use axum::http::header::CONTENT_TYPE;
use axum::response::Response;
use tower::ServiceExt;
use ursula_headers::ClosedBy;
use ursula_headers::ClosedEpoch;
use ursula_headers::IncarnationMismatch;
use ursula_headers::Inherited;
use ursula_headers::ProducerAnswer;
use ursula_headers::StreamHead;
use ursula_headers::names;
use ursula_headers::value::Epoch;
use ursula_headers::value::Incarnation;
use ursula_headers::value::Offset;
use ursula_headers::value::Seq;
use ursula_proto::admin::BackupImportResponse;
use ursula_proto::admin::PROCESS_INCARNATION_HEADER;

use crate::Persistence;
use crate::Topology;
use crate::router;
use crate::spawn_runtime;

const URI: &str = "/hist/s";

fn app() -> Router {
    let mut config = ursula_config::UrsulaConfig::default();
    config.runtime.core_count = 1;
    config.raft.group_count = 1;
    router(
        spawn_runtime(&config, Persistence::InMemory, Topology::SingleNode {
            raft_group_count: 1,
        })
        .expect("runtime")
        .runtime,
    )
}

fn pairs(response: &Response) -> impl Iterator<Item = (&str, &[u8])> {
    response
        .headers()
        .iter()
        .map(|(name, value)| (name.as_str(), value.as_bytes()))
}

async fn body_bytes(response: Response) -> Bytes {
    to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body")
}

/// Sends a request. An admin write carries this process's identity, as
/// `ursulactl` does.
async fn send(
    app: &Router,
    method: &str,
    uri: &str,
    headers: &[(&str, &str)],
    body: Body,
) -> Response {
    let mut request = Request::builder().method(method).uri(uri);
    if uri.starts_with("/__ursula/") && method == "POST" {
        let metrics = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/__ursula/metrics")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("metrics");
        let metrics: serde_json::Value =
            serde_json::from_slice(&body_bytes(metrics).await).expect("metrics JSON");
        request = request.header(
            PROCESS_INCARNATION_HEADER,
            metrics["process_incarnation"]
                .as_str()
                .expect("process identity"),
        );
    }
    for (name, value) in headers {
        request = request.header(*name, *value);
    }
    app.clone()
        .oneshot(request.body(body).expect("request"))
        .await
        .expect("response")
}

async fn head(app: &Router) -> StreamHead {
    let response = send(app, "HEAD", URI, &[], Body::empty()).await;
    assert_eq!(response.status(), StatusCode::OK);
    StreamHead::parse(pairs(&response)).expect("a well-formed stream head")
}

/// A producer write of `p` at `(epoch, seq)`, with `token` as its
/// `Stream-Incarnation` when given.
async fn write(
    app: &Router,
    epoch: u64,
    seq: u64,
    body: &'static str,
    token: Option<Incarnation>,
) -> Response {
    let epoch = epoch.to_string();
    let seq = seq.to_string();
    let token = token.map(|token| token.to_string());
    let mut headers = vec![
        (CONTENT_TYPE.as_str(), "text/plain"),
        (names::PRODUCER_ID, "p"),
        (names::PRODUCER_EPOCH, epoch.as_str()),
        (names::PRODUCER_SEQ, seq.as_str()),
    ];
    if let Some(token) = token.as_deref() {
        headers.push((names::STREAM_INCARNATION, token));
    }
    send(app, "POST", URI, &headers, Body::from(body)).await
}

#[track_caller]
fn answer(response: &Response) -> ProducerAnswer {
    ProducerAnswer::parse(response.status().as_u16(), pairs(response))
        .expect("a well-formed producer answer")
        .expect("a producer answer")
}

async fn export(app: &Router) -> Bytes {
    let response = send(app, "GET", "/__ursula/backup/group/0", &[], Body::empty()).await;
    assert_eq!(response.status(), StatusCode::OK);
    body_bytes(response).await
}

async fn import(app: &Router, backup: &Bytes, digest: Option<&str>) -> Response {
    let mut headers = vec![(CONTENT_TYPE.as_str(), "application/x-msgpack")];
    if let Some(digest) = digest {
        headers.push(("x-ursula-backup-blake3", digest));
    }
    send(
        app,
        "POST",
        "/__ursula/backup/group/0/import",
        &headers,
        Body::from(backup.clone()),
    )
    .await
}

async fn imported(response: Response) -> BackupImportResponse {
    assert_eq!(response.status(), StatusCode::OK);
    serde_json::from_slice(&body_bytes(response).await).expect("a typed import answer")
}

/// A source stream with two producer writes, exported.
async fn source_backup() -> (StreamHead, Bytes) {
    let source = app();
    let response = send(
        &source,
        "PUT",
        URI,
        &[(CONTENT_TYPE.as_str(), "text/plain")],
        Body::empty(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::CREATED);
    for (seq, body) in [(0, "a"), (1, "b")] {
        assert_eq!(
            write(&source, 0, seq, body, None).await.status(),
            StatusCode::OK
        );
    }
    let head = head(&source).await;
    assert_eq!(head.inherited(), None);
    (head, export(&source).await)
}

#[tokio::test]
async fn a_restore_starts_a_new_history_that_head_traces_to_the_source() {
    let (source, backup) = source_backup().await;
    let digest = blake3::hash(&backup).to_hex().to_string();
    let target = app();
    let first = imported(import(&target, &backup, Some(&digest)).await).await;
    assert!(!first.already);
    assert_eq!(first.streams, 1);

    let restored = head(&target).await;
    assert_ne!(restored.incarnation(), source.incarnation());
    assert_eq!(restored.next_offset(), source.next_offset());
    assert_eq!(
        restored.inherited(),
        Some(Inherited {
            previous_incarnation: source.incarnation(),
            offset: Offset::from_u64(2),
        })
    );

    // A repeat of the import is the same import: nothing changes.
    let repeat = imported(import(&target, &backup, Some(&digest)).await).await;
    assert!(repeat.already);
    assert_eq!(repeat.history, first.history);
    assert_eq!(head(&target).await, restored);
}

#[tokio::test]
async fn an_import_needs_the_digest_of_its_body() {
    let (_, backup) = source_backup().await;
    let target = app();
    let response = import(&target, &backup, None).await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let other = blake3::hash(b"another backup").to_hex().to_string();
    let response = import(&target, &backup, Some(&other)).await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    // Nothing was imported.
    let response = send(&target, "HEAD", URI, &[], Body::empty()).await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

/// G1: a request that names the source's token answers `412` with the
/// current token, for writes and reads alike.
#[tokio::test]
async fn the_source_token_answers_412_after_a_restore() {
    let (source, backup) = source_backup().await;
    let target = app();
    let digest = blake3::hash(&backup).to_hex().to_string();
    imported(import(&target, &backup, Some(&digest)).await).await;
    let current = head(&target).await.incarnation();

    let response = write(&target, 1, 0, "c", Some(source.incarnation())).await;
    assert_eq!(response.status(), StatusCode::PRECONDITION_FAILED);
    assert_eq!(
        IncarnationMismatch::parse(412, pairs(&response)).expect("a well-formed 412"),
        Some(IncarnationMismatch {
            current: Some(current)
        })
    );

    let token = source.incarnation().to_string();
    let response = send(
        &target,
        "GET",
        &format!("{URI}?offset=0"),
        &[(names::STREAM_INCARNATION, token.as_str())],
        Body::empty(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::PRECONDITION_FAILED);
    assert_eq!(
        IncarnationMismatch::parse(412, pairs(&response)).expect("a well-formed 412"),
        Some(IncarnationMismatch {
            current: Some(current)
        })
    );
    // Nothing applied.
    assert_eq!(head(&target).await.next_offset(), Offset::from_u64(2));
}

/// G2, row 11: every restored producer record is history-fenced. Its epoch
/// is closed at the seq that survived, and a claim of the next epoch reports
/// it.
#[tokio::test]
async fn a_restored_producer_is_fenced_by_the_history() {
    let (_, backup) = source_backup().await;
    let target = app();
    let digest = blake3::hash(&backup).to_hex().to_string();
    imported(import(&target, &backup, Some(&digest)).await).await;
    let token = head(&target).await.incarnation();
    let closed = ClosedEpoch {
        epoch: Epoch::try_from(0).unwrap(),
        seq: Seq::try_from(1).unwrap(),
    };

    for token in [None, Some(token)] {
        let response = write(&target, 0, 2, "c", token).await;
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        match answer(&response) {
            ProducerAnswer::EpochClosed(closure) => {
                assert_eq!(closure.by(), ClosedBy::History);
                assert_eq!(closure.epoch(), closed.epoch);
                assert_eq!(closure.closed_epoch(), closed);
            }
            other => panic!("expected epoch-closed;by=history, got {other:?}"),
        }
    }
    // The newest write applied in the history that survived.
    let response = write(&target, 0, 1, "b", None).await;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    assert!(matches!(answer(&response), ProducerAnswer::Receipt { .. }));

    let response = write(&target, 1, 0, "c", Some(token)).await;
    assert_eq!(response.status(), StatusCode::OK);
    match answer(&response) {
        ProducerAnswer::Receipt { epoch, .. } => {
            assert_eq!(epoch.closed_epoch(), Some(closed));
        }
        other => panic!("expected a receipt, got {other:?}"),
    }
    assert_eq!(head(&target).await.next_offset(), Offset::from_u64(3));
}

/// M4 (#577): an SSE session without a precondition is pinned to the token
/// it announced and ends when the stream is deleted and recreated.
#[tokio::test]
async fn an_unpinned_sse_session_ends_at_a_recreate() {
    let app = app();
    let typed = [(CONTENT_TYPE.as_str(), "text/plain")];
    let create = || send(&app, "PUT", URI, &typed, Body::empty());
    assert_eq!(create().await.status(), StatusCode::CREATED);
    let response = send(
        &app,
        "GET",
        &format!("{URI}?offset=now&live=sse"),
        &[],
        Body::empty(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let session = tokio::spawn(body_bytes(response));

    assert_eq!(
        send(&app, "DELETE", URI, &[], Body::empty()).await.status(),
        StatusCode::NO_CONTENT
    );
    assert_eq!(create().await.status(), StatusCode::CREATED);
    let response = send(
        &app,
        "POST",
        URI,
        &[(CONTENT_TYPE.as_str(), "text/plain")],
        Body::from("new"),
    )
    .await;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    let body = tokio::time::timeout(std::time::Duration::from_secs(5), session)
        .await
        .expect("the session ends")
        .expect("body task");
    let body = std::str::from_utf8(&body).expect("utf8");
    assert!(body.contains("event: error"), "{body}");
    assert!(!body.contains("data:new"), "{body}");
}

/// A `304` never matches across a recreate with equal bytes: the token is
/// part of the read's tag.
#[tokio::test]
async fn a_read_etag_differs_across_a_recreate_with_equal_bytes() {
    let app = app();
    let mut tags = Vec::new();
    for _ in 0..2 {
        let response = send(
            &app,
            "PUT",
            URI,
            &[(CONTENT_TYPE.as_str(), "text/plain")],
            Body::from("same"),
        )
        .await;
        assert_eq!(response.status(), StatusCode::CREATED);
        let response = send(&app, "GET", &format!("{URI}?offset=0"), &[], Body::empty()).await;
        assert_eq!(response.status(), StatusCode::OK);
        tags.push(
            response
                .headers()
                .get("etag")
                .expect("an etag")
                .to_str()
                .expect("ascii")
                .to_owned(),
        );
        assert_eq!(
            send(&app, "DELETE", URI, &[], Body::empty()).await.status(),
            StatusCode::NO_CONTENT
        );
    }
    assert_ne!(tags[0], tags[1]);
}

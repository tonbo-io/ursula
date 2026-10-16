//! Bounded-state F16 over HTTP: a snapshot body above the staging threshold
//! is staged in the cold tier and streamed back by snapshot reads and
//! `/bootstrap`. Snapshot reads honor a single byte range (#581 addendum 7)
//! whether the body is inline or in the cold tier.

use std::sync::Arc;

use axum::body::Body;
use axum::body::to_bytes;
use axum::http::Request;
use tower::ServiceExt;
use ursula_headers::ByteSpan;
use ursula_headers::ContentRange;
use ursula_headers::SnapshotPublishAnswer;
use ursula_headers::SnapshotRead;
use ursula_headers::value::Offset;
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
    send_with(app, method, uri, &[], body).await
}

async fn send_with(
    app: &Router,
    method: &str,
    uri: &str,
    headers: &[(&str, &str)],
    body: Body,
) -> Response {
    let mut request = Request::builder()
        .method(method)
        .uri(uri)
        .header(CONTENT_TYPE, OCTET);
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

fn pairs(response: &Response) -> impl Iterator<Item = (&str, &[u8])> {
    response
        .headers()
        .iter()
        .map(|(name, value)| (name.as_str(), value.as_bytes()))
}

fn cold_app() -> (Router, Arc<ColdStore>) {
    let cold_store = Arc::new(ColdStore::memory().expect("memory cold store"));
    let runtime = ShardRuntime::spawn_with_engine_factory_and_cold_store(
        RuntimeConfig::new(1, 1),
        InMemoryGroupEngineFactory::with_cold_store(Some(cold_store.clone())),
        Some(cold_store.clone()),
    )
    .expect("runtime");
    (router(runtime), cold_store)
}

fn inline_app() -> Router {
    router(ShardRuntime::spawn(RuntimeConfig::new(1, 1)).expect("runtime"))
}

fn digest_of(body: &[u8]) -> String {
    let mut digest = SnapshotDigest::new(OCTET);
    digest.update(body);
    digest.finalize()
}

#[tokio::test]
async fn snapshot_above_the_staging_threshold_round_trips_through_the_cold_tier() {
    let (app, cold_store) = cold_app();
    let response = send(&app, "PUT", "/cold-snapshots/s", Body::from("ab")).await;
    assert_eq!(response.status(), StatusCode::CREATED);

    // Above the 1 MiB staging threshold, below the request limit.
    let snapshot = (0..(3_usize << 20))
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
    let digest = digest_of(&snapshot);
    assert_eq!(
        response.headers()[names::STREAM_SNAPSHOT_DIGEST],
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
        response.headers()[names::STREAM_SNAPSHOT_DIGEST],
        digest.as_str()
    );
    assert_eq!(response.headers()[names::ACCEPT_RANGES], "bytes");
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

/// Addendum 7: one range answers `206` with only its bytes, a range past
/// the end `416`, and several ranges, a malformed one or `If-Range` the
/// whole body. The snapshot headers describe the whole body either way.
#[tokio::test]
async fn a_snapshot_read_honors_one_byte_range_inline_and_in_the_cold_tier() {
    let (cold, _store) = cold_app();
    for (tier, app, len) in [
        ("inline", inline_app(), 4096_usize),
        // Above the staging threshold and above one cold read piece, so a
        // range crosses pieces.
        ("cold", cold, (8 << 20) + 4096),
    ] {
        let snapshot = (0..len)
            .map(|index| (index % 253) as u8)
            .collect::<Vec<_>>();
        let total = u64::try_from(len).unwrap();
        let response = send(&app, "PUT", "/ranges/s", Body::from("abc")).await;
        assert_eq!(response.status(), StatusCode::CREATED, "{tier}");
        let response = send(
            &app,
            "PUT",
            "/ranges/s/snapshot/3",
            Body::from(snapshot.clone()),
        )
        .await;
        assert_eq!(response.status(), StatusCode::NO_CONTENT, "{tier}");
        let whole = SnapshotRead::parse(pairs(
            &send(&app, "GET", "/ranges/s/snapshot/3", Body::empty()).await,
        ))
        .expect("a snapshot read");
        assert_eq!(whole.snapshot.digest.to_string(), digest_of(&snapshot));

        let middle = (8 << 20) - 10;
        for (range, first, last) in [
            ("bytes=0-55".to_owned(), 0, 55),
            (format!("bytes={}-", total - 7), total - 7, total - 1),
            ("bytes=-9".to_owned(), total - 9, total - 1),
            (format!("bytes=100-{}", total + 100), 100, total - 1),
            (
                format!(
                    "bytes={}-{}",
                    middle.min(total - 20),
                    middle.min(total - 20) + 19
                ),
                middle.min(total - 20),
                middle.min(total - 20) + 19,
            ),
        ] {
            let response = send_with(
                &app,
                "GET",
                "/ranges/s/snapshot/3",
                &[("range", &range)],
                Body::empty(),
            )
            .await;
            assert_eq!(
                response.status(),
                StatusCode::PARTIAL_CONTENT,
                "{tier} {range}"
            );
            assert_eq!(
                ContentRange::parse(206, pairs(&response)),
                Ok(Some(ContentRange::Partial(
                    ByteSpan::new(first, last, total).unwrap()
                ))),
                "{tier} {range}"
            );
            assert_eq!(
                SnapshotRead::parse(pairs(&response)),
                Ok(whole),
                "{tier} {range}: the snapshot headers describe the whole body"
            );
            assert_eq!(response.headers()[names::ACCEPT_RANGES], "bytes");
            let first = usize::try_from(first).unwrap();
            let last = usize::try_from(last).unwrap();
            assert_eq!(
                response.headers()[CONTENT_LENGTH],
                (last - first + 1).to_string().as_str()
            );
            assert!(
                body_of(response).await == snapshot[first..=last],
                "{tier} {range}"
            );
        }

        for range in [
            format!("bytes={total}-"),
            format!("bytes={}-{}", total, total + 5),
            "bytes=-0".to_owned(),
        ] {
            let response = send_with(
                &app,
                "GET",
                "/ranges/s/snapshot/3",
                &[("range", &range)],
                Body::empty(),
            )
            .await;
            assert_eq!(
                response.status(),
                StatusCode::RANGE_NOT_SATISFIABLE,
                "{tier} {range}"
            );
            assert_eq!(
                ContentRange::parse(416, pairs(&response)),
                Ok(Some(ContentRange::Unsatisfied { total })),
                "{tier} {range}"
            );
            assert_eq!(
                crate::render::error_code_of(response).await,
                ursula_headers::ErrorCode::RangeNotSatisfiable
            );
        }

        for headers in [
            vec![("range", "bytes=0-1,4-5")],
            vec![("range", "bytes=5-2")],
            vec![("range", "items=0-1")],
            vec![("range", "bytes=0-1"), ("if-range", "\"tag\"")],
        ] {
            let response =
                send_with(&app, "GET", "/ranges/s/snapshot/3", &headers, Body::empty()).await;
            assert_eq!(response.status(), StatusCode::OK, "{tier} {headers:?}");
            assert!(response.headers().get(names::CONTENT_RANGE).is_none());
            assert!(body_of(response).await == snapshot, "{tier} {headers:?}");
        }

        // 404 and 412 are decided before the range.
        let response = send_with(
            &app,
            "GET",
            "/ranges/s/snapshot/2",
            &[("range", "bytes=0-1")],
            Body::empty(),
        )
        .await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND, "{tier}");
        let other = (whole.incarnation.get() ^ 1).to_string();
        let response = send_with(
            &app,
            "GET",
            "/ranges/s/snapshot/3",
            &[("range", "bytes=0-1"), (names::STREAM_INCARNATION, &other)],
            Body::empty(),
        )
        .await;
        assert_eq!(response.status(), StatusCode::PRECONDITION_FAILED, "{tier}");

        // A stream read keeps its offset protocol and ignores `Range`.
        let response = send_with(
            &app,
            "GET",
            "/ranges/s?offset=-1",
            &[("range", "bytes=0-0")],
            Body::empty(),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK, "{tier}");
        assert!(response.headers().get(names::ACCEPT_RANGES).is_none());
        assert_eq!(body_of(response).await, b"abc");
    }
}

/// #581 section 3: a publish is keyed by offset and digest. A repeat of the
/// latest answers its digest; a refused publish names the latest snapshot,
/// so a client tells a superseded publish from a conflicting one.
#[tokio::test]
async fn a_refused_publish_names_the_latest_snapshot() {
    let app = inline_app();
    let response = send(&app, "PUT", "/keyed/s", Body::from("abcdef")).await;
    assert_eq!(response.status(), StatusCode::CREATED);
    let response = send(&app, "PUT", "/keyed/s/snapshot/4", Body::from("four")).await;
    let published = SnapshotPublishAnswer::parse(204, pairs(&response))
        .expect("typed")
        .expect("published");
    let SnapshotPublishAnswer::Published { snapshot, .. } = published else {
        panic!("{published:?}");
    };
    assert_eq!(snapshot.offset, Offset::from_u64(4));
    let response = send(&app, "PUT", "/keyed/s/snapshot/4", Body::from("four")).await;
    assert_eq!(
        SnapshotPublishAnswer::parse(response.status().as_u16(), pairs(&response)),
        Ok(Some(published)),
        "a repeat"
    );
    for (offset, superseded) in [(4, false), (2, true)] {
        let response = send(
            &app,
            "PUT",
            &format!("/keyed/s/snapshot/{offset}"),
            Body::from("other"),
        )
        .await;
        assert_eq!(response.status(), StatusCode::CONFLICT);
        let answer = SnapshotPublishAnswer::parse(409, pairs(&response))
            .expect("typed")
            .expect("a conflict names the latest");
        assert_eq!(answer, SnapshotPublishAnswer::Conflict { latest: snapshot });
        assert_eq!(answer.supersedes(Offset::from_u64(offset)), superseded);
    }

    // Retention is monotonic: a boundary at or below the current one
    // succeeds and reports the current one.
    for (offset, retained) in [(3, 3), (1, 3), (3, 3)] {
        let response = send(
            &app,
            "PUT",
            &format!("/keyed/s/retention/{offset}"),
            Body::empty(),
        )
        .await;
        assert_eq!(response.status(), StatusCode::NO_CONTENT, "{offset}");
        assert_eq!(
            response.headers()[names::STREAM_RETAINED_OFFSET],
            format!("{retained:020}").as_str()
        );
    }
}

/// Addendum 7 edge cases: `HEAD` answers like `GET` without the body, a
/// nonzero suffix of an empty snapshot gets the empty body whole, and a
/// superseded snapshot below retention answers `404` whatever the range. A
/// snapshot read never answers `410`: only the latest snapshot is readable,
/// and retention never passes it.
#[tokio::test]
async fn range_edge_cases_on_snapshot_reads() {
    let app = inline_app();
    let response = send(&app, "PUT", "/edges/s", Body::from("abcdef")).await;
    assert_eq!(response.status(), StatusCode::CREATED);
    let response = send(&app, "PUT", "/edges/s/snapshot/3", Body::from("snapshot")).await;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    let response = send_with(
        &app,
        "HEAD",
        "/edges/s/snapshot/3",
        &[("range", "bytes=0-1")],
        Body::empty(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
    assert_eq!(response.headers()[names::CONTENT_RANGE], "bytes 0-1/8");
    assert!(body_of(response).await.is_empty());

    // An empty snapshot at the tail.
    let response = send(&app, "PUT", "/edges/s/snapshot/6", Body::empty()).await;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    let response = send_with(
        &app,
        "GET",
        "/edges/s/snapshot/6",
        &[("range", "bytes=-5")],
        Body::empty(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()[CONTENT_LENGTH], "0");
    let response = send_with(
        &app,
        "GET",
        "/edges/s/snapshot/6",
        &[("range", "bytes=0-")],
        Body::empty(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::RANGE_NOT_SATISFIABLE);
    assert_eq!(
        crate::render::error_code_of(response).await,
        ursula_headers::ErrorCode::RangeNotSatisfiable
    );

    // Retention past a superseded snapshot's offset.
    let response = send(&app, "PUT", "/edges/s/retention/6", Body::empty()).await;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    let response = send_with(
        &app,
        "GET",
        "/edges/s/snapshot/3",
        &[("range", "bytes=0-1")],
        Body::empty(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

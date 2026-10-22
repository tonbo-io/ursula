//! Immutable reads over HTTP (extensions Section 4, #581 section 5): the
//! chunk and snapshot resources of one incarnation through the client
//! router, every answer parsed with the shared header crate (`ChunkRead`,
//! `ImmutableSnapshotRead`, `IncarnationMismatch`).

use axum::Router;
use axum::body::Body;
use axum::body::Bytes;
use axum::body::to_bytes;
use axum::http::Request;
use axum::http::StatusCode;
use axum::http::header::ACCEPT_ENCODING;
use axum::http::header::CACHE_CONTROL;
use axum::http::header::CONTENT_ENCODING;
use axum::http::header::CONTENT_TYPE;
use axum::http::header::ETAG;
use axum::http::header::IF_NONE_MATCH;
use axum::http::header::RANGE;
use axum::http::header::VARY;
use axum::response::Response;
use tower::ServiceExt;
use ursula_headers::ChunkMark;
use ursula_headers::ChunkRead;
use ursula_headers::ContentRange;
use ursula_headers::ErrorCode;
use ursula_headers::ImmutableResource;
use ursula_headers::ImmutableSnapshotRead;
use ursula_headers::IncarnationMismatch;
use ursula_headers::ResourceKind;
use ursula_headers::SnapshotRead;
use ursula_headers::StreamHead;
use ursula_headers::names;
use ursula_headers::value::Incarnation;
use ursula_headers::value::Offset;

use crate::Topology;
use crate::router;
use crate::spawn_runtime;

/// Ursula's chunk grid, 4 MiB.
const GRID: u64 = 4 * 1024 * 1024;
const URI: &str = "/immutable/s";

fn app() -> Router {
    let mut config = ursula_config::UrsulaConfig::default();
    config.runtime.core_count = 1;
    config.raft.group_count = Some(1);
    router(
        spawn_runtime(&config, Topology::Development { group_count: 1 })
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

fn header<'a>(response: &'a Response, name: &str) -> Option<&'a str> {
    response
        .headers()
        .get(name)
        .and_then(|value| value.to_str().ok())
}

async fn body_bytes(response: Response) -> Bytes {
    to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body")
}

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

async fn create(app: &Router, content_type: &str, body: Vec<u8>) {
    let response = send(
        app,
        "PUT",
        URI,
        &[(CONTENT_TYPE.as_str(), content_type)],
        Body::from(body),
    )
    .await;
    assert_eq!(response.status(), StatusCode::CREATED);
}

async fn head(app: &Router) -> StreamHead {
    let response = send(app, "HEAD", URI, &[], Body::empty()).await;
    assert_eq!(response.status(), StatusCode::OK);
    StreamHead::parse(pairs(&response)).expect("a well-formed stream head")
}

/// `{URI}/incarnation/{token}/{kind}/{offset}`.
fn url(kind: ResourceKind, token: Incarnation, offset: u64) -> String {
    let resource = ImmutableResource {
        kind,
        incarnation: token,
        offset: Offset::from_u64(offset),
    };
    format!("{URI}{}", resource.path())
}

async fn get_chunk(
    app: &Router,
    token: Incarnation,
    offset: u64,
    headers: &[(&str, &str)],
) -> Response {
    send(
        app,
        "GET",
        &url(ResourceKind::Chunk, token, offset),
        headers,
        Body::empty(),
    )
    .await
}

/// A `200` chunk answer: its typed headers and body, checked against what
/// a client verifies (#581 section 5.4).
async fn chunk(app: &Router, token: Incarnation, offset: u64) -> (ChunkRead, String, Bytes) {
    let response = get_chunk(app, token, offset, &[]).await;
    assert_eq!(response.status(), StatusCode::OK);
    let read = ChunkRead::parse(pairs(&response)).expect("a well-formed chunk answer");
    assert!(read.answers(&ImmutableResource {
        kind: ResourceKind::Chunk,
        incarnation: token,
        offset: Offset::from_u64(offset),
    }));
    assert_eq!(header(&response, names::STREAM_UP_TO_DATE), None);
    assert_eq!(header(&response, "stream-cursor"), None);
    assert_eq!(header(&response, CACHE_CONTROL.as_str()), Some("no-store"));
    let etag = header(&response, ETAG.as_str()).expect("ETag").to_owned();
    let body = body_bytes(response).await;
    assert_eq!(u64::try_from(body.len()).unwrap(), read.len());
    (read, etag, body)
}

fn bytes(len: u64) -> Vec<u8> {
    (0..len)
        .map(|index| u8::try_from(index % 251).unwrap())
        .collect()
}

#[tokio::test]
async fn chunks_are_marked_only_where_no_later_answer_differs() {
    let app = app();
    let tail = GRID + 10;
    let content = bytes(tail);
    create(&app, "application/octet-stream", content.clone()).await;
    let token = head(&app).await.incarnation();

    // A whole chunk below the tail: marked.
    let (read, below, body) = chunk(&app, token, 0).await;
    assert_eq!(read.mark(), ChunkMark::Immutable);
    assert_eq!(read.next(), Offset::from_u64(GRID));
    assert_eq!(&body[..], &content[..usize::try_from(GRID).unwrap()]);

    // From inside a chunk, the rest of that chunk: the grid after one answer.
    let (read, _, body) = chunk(&app, token, 5).await;
    assert_eq!(read.mark(), ChunkMark::Immutable);
    assert_eq!(read.next(), Offset::from_u64(GRID));
    assert_eq!(&body[..], &content[5..usize::try_from(GRID).unwrap()]);

    // The tail of an open stream lies within this chunk: not marked.
    let (read, _, body) = chunk(&app, token, GRID).await;
    assert_eq!(read.mark(), ChunkMark::Unmarked);
    assert_eq!(read.next(), Offset::from_u64(tail));
    assert_eq!(&body[..], &content[usize::try_from(GRID).unwrap()..]);
    let (read, _, body) = chunk(&app, token, tail).await;
    assert_eq!(read.mark(), ChunkMark::Unmarked);
    assert!(read.is_empty() && body.is_empty());

    // Closing marks the tail and keeps the tag of every chunk below it.
    let response = send(
        &app,
        "POST",
        URI,
        &[(names::STREAM_CLOSED, "true")],
        Body::empty(),
    )
    .await;
    assert!(response.status().is_success(), "{}", response.status());
    let (read, after_close, _) = chunk(&app, token, 0).await;
    assert_eq!(read.mark(), ChunkMark::Immutable);
    assert_eq!(after_close, below);
    let (read, _, _) = chunk(&app, token, GRID).await;
    assert_eq!(read.mark(), ChunkMark::ClosedTail);
    assert_eq!(read.next(), Offset::from_u64(tail));
    let (read, _, body) = chunk(&app, token, tail).await;
    assert_eq!(read.mark(), ChunkMark::ClosedTail);
    assert!(body.is_empty());
}

#[tokio::test]
async fn a_chunk_that_ends_exactly_at_an_open_tail_is_not_marked() {
    let app = app();
    create(&app, "application/octet-stream", bytes(GRID)).await;
    let token = head(&app).await.incarnation();
    let (read, _, _) = chunk(&app, token, 0).await;
    assert_eq!(read.next(), Offset::from_u64(GRID));
    assert_eq!(
        read.mark(),
        ChunkMark::Unmarked,
        "the stream can still be closed at the chunk's end"
    );
}

#[tokio::test]
async fn a_marked_chunk_revalidates_with_every_header() {
    let app = app();
    create(&app, "application/octet-stream", bytes(GRID + 1)).await;
    let token = head(&app).await.incarnation();
    let (read, etag, _) = chunk(&app, token, 0).await;
    let response = get_chunk(&app, token, 0, &[(IF_NONE_MATCH.as_str(), etag.as_str())]).await;
    assert_eq!(response.status(), StatusCode::NOT_MODIFIED);
    assert_eq!(ChunkRead::parse(pairs(&response)), Ok(read));
    assert_eq!(header(&response, ETAG.as_str()), Some(etag.as_str()));
    assert_eq!(
        header(&response, CONTENT_TYPE.as_str()),
        Some("application/octet-stream")
    );
    assert!(body_bytes(response).await.is_empty());

    let response = get_chunk(&app, token, 0, &[(IF_NONE_MATCH.as_str(), "\"other\"")]).await;
    assert_eq!(response.status(), StatusCode::OK);

    // HEAD answers the headers of the GET.
    let response = send(
        &app,
        "HEAD",
        &url(ResourceKind::Chunk, token, 0),
        &[],
        Body::empty(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(ChunkRead::parse(pairs(&response)), Ok(read));
    assert!(body_bytes(response).await.is_empty());
}

#[tokio::test]
async fn a_json_chunk_is_ndjson_and_never_compressed() {
    let app = app();
    let messages: Vec<String> = (0..64)
        .map(|index| format!(r#"{{"id":{index},"pad":"................"}}"#))
        .collect();
    create(
        &app,
        "application/json",
        format!("[{}]", messages.join(",")).into_bytes(),
    )
    .await;
    let token = head(&app).await.incarnation();
    let response = get_chunk(&app, token, 0, &[(ACCEPT_ENCODING.as_str(), "gzip")]).await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        header(&response, CONTENT_TYPE.as_str()),
        Some("application/x-ndjson")
    );
    assert_eq!(header(&response, CONTENT_ENCODING.as_str()), None);
    assert_eq!(header(&response, VARY.as_str()), None);
    let read = ChunkRead::parse(pairs(&response)).expect("a chunk answer");
    let body = body_bytes(response).await;
    assert!(body.len() > 256);
    assert_eq!(u64::try_from(body.len()).unwrap(), read.len());
    assert_eq!(
        std::str::from_utf8(&body).expect("text"),
        messages
            .iter()
            .map(|message| format!("{message}\n"))
            .collect::<String>()
    );
}

#[tokio::test]
async fn a_recreate_retires_the_urls_and_their_tags() {
    let app = app();
    create(&app, "application/octet-stream", bytes(GRID + 1)).await;
    let first = head(&app).await.incarnation();
    let (_, first_tag, _) = chunk(&app, first, 0).await;

    let response = send(&app, "DELETE", URI, &[], Body::empty()).await;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    create(&app, "application/octet-stream", bytes(GRID + 1)).await;
    let second = head(&app).await.incarnation();
    assert_ne!(second, first);

    let response = get_chunk(&app, first, 0, &[]).await;
    assert_eq!(response.status(), StatusCode::PRECONDITION_FAILED);
    assert_eq!(
        IncarnationMismatch::parse(412, pairs(&response)),
        Ok(Some(IncarnationMismatch {
            current: Some(second)
        }))
    );
    let (read, second_tag, _) = chunk(&app, second, 0).await;
    assert_eq!(read.mark(), ChunkMark::Immutable);
    assert_ne!(second_tag, first_tag, "equal bytes, another token");
}

#[tokio::test]
async fn refusals_follow_the_precondition_order() {
    let app = app();
    let stale = Incarnation::try_from(4_503_599_627_370_497).unwrap();

    // No stream: 404 whatever the token.
    let response = get_chunk(&app, stale, 0, &[]).await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    create(&app, "application/octet-stream", bytes(GRID + 10)).await;
    let token = head(&app).await.incarnation();
    assert_ne!(token, stale);

    // Beyond the tail: 416 before 412.
    let response = get_chunk(&app, stale, GRID + 11, &[]).await;
    assert_eq!(response.status(), StatusCode::RANGE_NOT_SATISFIABLE);

    // Below the retained offset: 410 before 412.
    let response = send(
        &app,
        "PUT",
        &format!("{URI}/snapshot/{}", Offset::from_u64(GRID)),
        &[(CONTENT_TYPE.as_str(), "application/octet-stream")],
        Body::from("snapshot"),
    )
    .await;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    let response = send(
        &app,
        "PUT",
        &format!("{URI}/retention/{}", Offset::from_u64(GRID)),
        &[],
        Body::empty(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    for token in [stale, token] {
        let response = get_chunk(&app, token, 0, &[]).await;
        assert_eq!(response.status(), StatusCode::GONE);
    }

    // Otherwise 412, naming the current token.
    let response = get_chunk(&app, stale, GRID, &[]).await;
    assert_eq!(response.status(), StatusCode::PRECONDITION_FAILED);
    assert_eq!(
        IncarnationMismatch::parse(412, pairs(&response)),
        Ok(Some(IncarnationMismatch {
            current: Some(token)
        }))
    );

    // The request's own header does not override the path.
    let current = token.to_string();
    let response = get_chunk(&app, stale, GRID, &[(
        names::STREAM_INCARNATION,
        current.as_str(),
    )])
    .await;
    assert_eq!(response.status(), StatusCode::PRECONDITION_FAILED);
}

#[tokio::test]
async fn only_the_issued_form_of_a_resource_is_accepted() {
    let app = app();
    create(&app, "application/octet-stream", bytes(16)).await;
    let token = head(&app).await.incarnation();
    let chunk_url = url(ResourceKind::Chunk, token, 0);
    let snapshot_url = url(ResourceKind::Snapshot, token, 0);
    for (uri, code) in [
        (format!("{chunk_url}?offset=0"), ErrorCode::QueryNotAccepted),
        (format!("{chunk_url}?"), ErrorCode::QueryNotAccepted),
        (format!("{snapshot_url}?x=1"), ErrorCode::QueryNotAccepted),
        (
            format!("{URI}/incarnation/{token}/chunk/0"),
            ErrorCode::InvalidResource,
        ),
        (
            format!("{URI}/incarnation/0/chunk/{}", Offset::ZERO),
            ErrorCode::InvalidResource,
        ),
    ] {
        let response = send(&app, "GET", &uri, &[], Body::empty()).await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{uri}");
        assert_eq!(crate::render::error_code_of(response).await, code, "{uri}");
    }
    for uri in [
        format!("{chunk_url}?offset=0"),
        format!("{chunk_url}?"),
        format!("{snapshot_url}?x=1"),
        format!("{URI}/incarnation/{token}/chunk/0"),
        format!("{URI}/incarnation/{token}/chunk/-1"),
        format!("{URI}/incarnation/0{token}/chunk/{}", Offset::ZERO),
        format!("{URI}/incarnation/0/chunk/{}", Offset::ZERO),
        format!("{URI}/incarnation/{token}%20/chunk/{}", Offset::ZERO),
        format!("{URI}/incarnation/{token}/snapshot/16"),
    ] {
        let response = send(&app, "GET", &uri, &[], Body::empty()).await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{uri}");
        assert_eq!(
            header(&response, CACHE_CONTROL.as_str()),
            Some("no-store"),
            "{uri}"
        );
    }
    for uri in [
        format!("{URI}?offset=0&incarnation={token}"),
        format!("{URI}?offset=0&aligned=true"),
        format!("{URI}/snapshot/{}?incarnation={token}", Offset::ZERO),
    ] {
        let response = send(&app, "GET", &uri, &[], Body::empty()).await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{uri}");
        assert_eq!(
            header(&response, CACHE_CONTROL.as_str()),
            Some("no-store"),
            "{uri}"
        );
    }
    let response = send(&app, "POST", &chunk_url, &[], Body::empty()).await;
    assert_eq!(response.status(), StatusCode::METHOD_NOT_ALLOWED);
    assert_eq!(header(&response, CACHE_CONTROL.as_str()), Some("no-store"));
    let response = send(
        &app,
        "GET",
        &format!("{URI}/incarnation/{token}/bytes/{}", Offset::ZERO),
        &[],
        Body::empty(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    assert_eq!(header(&response, CACHE_CONTROL.as_str()), Some("no-store"));
}

#[tokio::test]
async fn a_compressible_read_has_a_weak_tag_and_a_chunk_a_strong_one() {
    let app = app();
    let messages: Vec<String> = (0..64)
        .map(|index| format!(r#"{{"id":{index},"pad":"................"}}"#))
        .collect();
    create(
        &app,
        "application/json",
        format!("[{}]", messages.join(",")).into_bytes(),
    )
    .await;
    let token = head(&app).await.incarnation();
    // An ordinary read may be gzipped for one client and not another.
    let response = send(&app, "GET", &format!("{URI}?offset=0"), &[], Body::empty()).await;
    let weak = header(&response, ETAG.as_str()).expect("ETag").to_owned();
    assert!(weak.starts_with("W/\""), "{weak}");
    for tag in [weak.as_str(), weak.trim_start_matches("W/")] {
        let response = send(
            &app,
            "GET",
            &format!("{URI}?offset=0"),
            &[(IF_NONE_MATCH.as_str(), tag)],
            Body::empty(),
        )
        .await;
        assert_eq!(response.status(), StatusCode::NOT_MODIFIED, "{tag}");
    }
    // A chunk is never compressed: its tag is strong, and a weakened copy
    // still revalidates.
    let (_, strong, _) = chunk(&app, token, 0).await;
    assert!(strong.starts_with('"'), "{strong}");
    let weakened = format!("W/{strong}");
    let response = get_chunk(&app, token, 0, &[(
        IF_NONE_MATCH.as_str(),
        weakened.as_str(),
    )])
    .await;
    assert_eq!(response.status(), StatusCode::NOT_MODIFIED);
}

/// The tag weakens exactly where the compression layer starts to compress:
/// a JSON read of 255 bytes is never gzipped and keeps a strong tag, one of
/// 256 may be and gets a weak one.
#[tokio::test]
async fn the_weak_tag_boundary_is_the_compression_boundary() {
    for (len, weak) in [(255_usize, false), (256, true)] {
        let app = app();
        // `{"p":"…"}` and its LF: 9 bytes around the padding.
        let message = format!(r#"{{"p":"{}"}}"#, "x".repeat(len - 9));
        create(&app, "application/json", message.into_bytes()).await;
        let response = send(
            &app,
            "GET",
            &format!("{URI}?offset=0"),
            &[(ACCEPT_ENCODING.as_str(), "gzip")],
            Body::empty(),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        let tag = header(&response, ETAG.as_str()).expect("ETag").to_owned();
        assert_eq!(tag.starts_with("W/"), weak, "{len}: {tag}");
        assert_eq!(
            header(&response, CONTENT_ENCODING.as_str()) == Some("gzip"),
            weak,
            "{len}"
        );
        if !weak {
            assert_eq!(body_bytes(response).await.len(), len);
        }
    }
}

#[tokio::test]
async fn an_ordinary_revalidation_carries_the_headers_of_its_200() {
    let app = app();
    create(&app, "application/octet-stream", bytes(16)).await;
    let response = send(
        &app,
        "POST",
        URI,
        &[(names::STREAM_CLOSED, "true")],
        Body::empty(),
    )
    .await;
    assert!(response.status().is_success());
    let response = send(&app, "GET", &format!("{URI}?offset=0"), &[], Body::empty()).await;
    let tag = header(&response, ETAG.as_str()).expect("ETag").to_owned();
    let response = send(
        &app,
        "GET",
        &format!("{URI}?offset=0"),
        &[(IF_NONE_MATCH.as_str(), tag.as_str())],
        Body::empty(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::NOT_MODIFIED);
    assert_eq!(header(&response, names::STREAM_UP_TO_DATE), Some("true"));
    assert_eq!(header(&response, names::STREAM_CLOSED), Some("true"));
    assert!(header(&response, names::STREAM_NEXT_OFFSET).is_some());
}

#[tokio::test]
async fn an_immutable_snapshot_is_always_marked_and_never_up_to_date() {
    let app = app();
    create(&app, "application/octet-stream", bytes(16)).await;
    let token = head(&app).await.incarnation();
    let snapshot = b"0123456789abcdef".to_vec();
    let response = send(
        &app,
        "PUT",
        &format!("{URI}/snapshot/{}", Offset::from_u64(16)),
        &[(CONTENT_TYPE.as_str(), "application/x-snapshot")],
        Body::from(snapshot.clone()),
    )
    .await;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    let published = head(&app).await.snapshot().expect("a snapshot");
    let etag = format!("\"{}\"", published.digest);
    let resource = ImmutableResource {
        kind: ResourceKind::Snapshot,
        incarnation: token,
        offset: published.offset,
    };
    let immutable_url = format!("{URI}{}", resource.path());

    // The stream's own snapshot route: up to date at the tail, unmarked.
    let response = send(
        &app,
        "GET",
        &format!("{URI}/snapshot/{}", published.offset),
        &[],
        Body::empty(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        SnapshotRead::parse(pairs(&response)).map(|read| read.up_to_date),
        Ok(true)
    );
    assert_eq!(header(&response, names::STREAM_IMMUTABLE), None);
    assert_eq!(header(&response, ETAG.as_str()), Some(etag.as_str()));

    // The immutable resource: marked, never up to date, tagged by its digest.
    let response = send(&app, "GET", &immutable_url, &[], Body::empty()).await;
    assert_eq!(response.status(), StatusCode::OK);
    let read = ImmutableSnapshotRead::parse(pairs(&response)).expect("a marked snapshot");
    assert!(read.answers(&resource));
    assert_eq!(read.snapshot, published);
    assert_eq!(header(&response, names::STREAM_UP_TO_DATE), None);
    assert_eq!(header(&response, ETAG.as_str()), Some(etag.as_str()));
    assert_eq!(
        header(&response, CONTENT_TYPE.as_str()),
        Some("application/x-snapshot")
    );
    assert_eq!(body_bytes(response).await.to_vec(), snapshot);

    // A range: `206`, marked, with the same tag.
    let response = send(
        &app,
        "GET",
        &immutable_url,
        &[(RANGE.as_str(), "bytes=2-5")],
        Body::empty(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
    assert_eq!(ImmutableSnapshotRead::parse(pairs(&response)), Ok(read));
    assert_eq!(header(&response, ETAG.as_str()), Some(etag.as_str()));
    assert!(matches!(
        ContentRange::parse(206, pairs(&response)),
        Ok(Some(ContentRange::Partial(_)))
    ));
    assert_eq!(&body_bytes(response).await[..], b"2345");

    // A range past the end: `416`, no answer for the URL, so unmarked.
    let response = send(
        &app,
        "GET",
        &immutable_url,
        &[(RANGE.as_str(), "bytes=100-")],
        Body::empty(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::RANGE_NOT_SATISFIABLE);
    assert_eq!(header(&response, names::STREAM_IMMUTABLE), None);

    // A revalidation: `304` with the marker, before any range.
    let response = send(
        &app,
        "GET",
        &immutable_url,
        &[
            (IF_NONE_MATCH.as_str(), etag.as_str()),
            (RANGE.as_str(), "bytes=100-"),
        ],
        Body::empty(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::NOT_MODIFIED);
    assert_eq!(ImmutableSnapshotRead::parse(pairs(&response)), Ok(read));
    assert!(body_bytes(response).await.is_empty());

    // Another token: 412.
    let stale = Incarnation::try_from(4_503_599_627_370_497).unwrap();
    assert_ne!(stale, token);
    let response = send(
        &app,
        "GET",
        &url(ResourceKind::Snapshot, stale, 16),
        &[],
        Body::empty(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::PRECONDITION_FAILED);

    // A superseded snapshot: 404, even under the right token.
    let response = send(
        &app,
        "POST",
        URI,
        &[(CONTENT_TYPE.as_str(), "application/octet-stream")],
        Body::from("more"),
    )
    .await;
    assert!(response.status().is_success());
    let response = send(
        &app,
        "PUT",
        &format!("{URI}/snapshot/{}", Offset::from_u64(20)),
        &[(CONTENT_TYPE.as_str(), "application/x-snapshot")],
        Body::from("later"),
    )
    .await;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    let response = send(&app, "GET", &immutable_url, &[], Body::empty()).await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

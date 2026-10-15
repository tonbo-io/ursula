//! Snapshots uploaded in parts over HTTP (#581 section 3.1, test plan item
//! 6): staging, the commit and its refusals, the snapshot limit, the key
//! checked before the body, and a node without a cold store.

use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

use axum::body::Body;
use axum::body::to_bytes;
use axum::http::Request;
use tower::ServiceExt;
use ursula_headers::PartId;
use ursula_headers::PartStaged;
use ursula_headers::PartsCommit;
use ursula_headers::PartsManifest;
use ursula_headers::SizeLimit;
use ursula_headers::SnapshotPublishAnswer;
use ursula_headers::SnapshotRead;
use ursula_headers::StreamHead;
use ursula_headers::value::SnapshotDigest as DigestHeader;
use ursula_runtime::ColdStore;
use ursula_runtime::InMemoryGroupEngineFactory;
use ursula_runtime::MAX_COLD_SNAPSHOT_BYTES;
use ursula_runtime::RuntimeConfig;
use ursula_runtime::ShardRuntime;
use ursula_runtime::SnapshotDigest;
use ursula_runtime::cold_external_dir;
use ursula_runtime::new_external_payload_path;
use ursula_shard::BucketStreamId;

use super::*;

const OCTET: &str = "application/octet-stream";
const MIB: usize = 1 << 20;

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

async fn send(app: &Router, request: Request<Body>) -> Response {
    app.clone().oneshot(request).await.expect("response")
}

fn pairs(response: &Response) -> impl Iterator<Item = (&str, &[u8])> {
    response
        .headers()
        .iter()
        .map(|(name, value)| (name.as_str(), value.as_bytes()))
}

async fn body_of(response: Response) -> Vec<u8> {
    to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body")
        .to_vec()
}

async fn create(app: &Router, uri: &str) {
    let response = send(
        app,
        Request::put(uri)
            .header(CONTENT_TYPE, OCTET)
            .body(Body::from("abc"))
            .expect("request"),
    )
    .await;
    assert_eq!(response.status(), StatusCode::CREATED, "{uri}");
}

async fn stage(app: &Router, uri: &str, body: Vec<u8>) -> PartId {
    let response = send(
        app,
        Request::post(format!("{uri}/snapshot/3/parts"))
            .body(Body::from(body))
            .expect("request"),
    )
    .await;
    assert_eq!(response.status(), StatusCode::CREATED, "{uri}");
    PartStaged::parse(pairs(&response)).expect("a part id").part
}

fn digest_of(body: &[u8]) -> DigestHeader {
    let mut digest = SnapshotDigest::new(OCTET);
    digest.update(body);
    DigestHeader::parse("digest", &digest.finalize()).expect("hex digest")
}

fn commit_request(uri: &str, parts: &[PartId], digest: DigestHeader) -> Request<Body> {
    let manifest = PartsManifest::new(parts.to_vec()).expect("manifest");
    let mut request = Request::put(format!("{uri}/snapshot/3")).header(CONTENT_TYPE, OCTET);
    let commit = PartsCommit::for_manifest(&manifest, digest);
    let mut headers = Vec::new();
    commit.render(|name, value| headers.push((name, value.to_owned())));
    for (name, value) in headers {
        request = request.header(name, value);
    }
    request
        .body(Body::from(manifest.render()))
        .expect("request")
}

fn bytes(len: usize, seed: u8) -> Vec<u8> {
    (0..len)
        .map(|index| (index % 251) as u8 ^ seed)
        .collect::<Vec<_>>()
}

async fn external_names(cold_store: &ColdStore, stream: &str) -> Vec<String> {
    cold_store
        .list_file_names(&cold_external_dir(&BucketStreamId::new("parts", stream)))
        .await
        .expect("list external dir")
}

/// Three parts that together exceed the request limit are committed,
/// read back whole and by range, and deleted after the commit. A repeat of
/// the commit answers from its key, although its parts are gone.
#[tokio::test]
async fn parts_commit_a_snapshot_above_the_request_limit() {
    let (app, cold_store) = cold_app();
    create(&app, "/parts/s").await;
    let response = send(&app, Request::head("/parts/s").body(Body::empty()).unwrap()).await;
    let head = StreamHead::parse(pairs(&response)).expect("stream head");
    assert_eq!(head.max_snapshot_bytes(), Some(MAX_COLD_SNAPSHOT_BYTES));

    let chunks = [
        bytes(13 * MIB, 1),
        bytes(13 * MIB, 2),
        bytes(7 * MIB + 5, 3),
    ];
    let snapshot = chunks.concat();
    assert!(snapshot.len() > MAX_HTTP_BODY_BYTES);
    let mut parts = Vec::new();
    for chunk in &chunks {
        parts.push(stage(&app, "/parts/s", chunk.clone()).await);
    }
    assert_eq!(external_names(&cold_store, "s").await.len(), 3);

    let digest = digest_of(&snapshot);
    let response = send(&app, commit_request("/parts/s", &parts, digest)).await;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    let published = SnapshotPublishAnswer::parse(204, pairs(&response))
        .expect("typed")
        .expect("published");
    let SnapshotPublishAnswer::Published { snapshot: key, .. } = published else {
        panic!("{published:?}");
    };
    assert_eq!(key.digest, digest);
    let names = external_names(&cold_store, "s").await;
    assert_eq!(names.len(), 1, "the parts are deleted: {names:?}");
    assert!(names[0].ends_with(".bin"), "{names:?}");

    let response = send(
        &app,
        Request::get("/parts/s/snapshot/3")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        SnapshotRead::parse(pairs(&response))
            .expect("read")
            .snapshot,
        key
    );
    assert!(body_of(response).await == snapshot);
    // A range across the boundary of the first two parts.
    let first = 13 * MIB - 3;
    let response = send(
        &app,
        Request::get("/parts/s/snapshot/3")
            .header("range", format!("bytes={first}-{}", first + 9))
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
    assert!(body_of(response).await == snapshot[first..first + 10]);

    // The parts are gone, and the key still answers a repeat.
    let response = send(&app, commit_request("/parts/s", &parts, digest)).await;
    assert_eq!(
        SnapshotPublishAnswer::parse(response.status().as_u16(), pairs(&response)),
        Ok(Some(published))
    );
    // A single-request publish of the same snapshot is refused for its size.
    let response = send(
        &app,
        Request::put("/parts/s/snapshot/3")
            .header(CONTENT_TYPE, OCTET)
            .body(Body::from(snapshot))
            .unwrap(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
}

/// A commit that names a part it cannot use, or whose parts do not
/// assemble to its digest, is refused with `400` and publishes nothing; its
/// parts stay for a corrected commit.
#[tokio::test]
async fn a_commit_is_refused_for_unknown_foreign_or_mismatched_parts() {
    let (app, cold_store) = cold_app();
    create(&app, "/parts/s").await;
    create(&app, "/parts/other").await;
    let a = stage(&app, "/parts/s", b"first".to_vec()).await;
    let b = stage(&app, "/parts/s", b"second".to_vec()).await;
    let foreign = stage(&app, "/parts/other", b"second".to_vec()).await;
    // A staged payload of the stream, which is no part.
    let payload = new_external_payload_path(&BucketStreamId::new("parts", "s"));
    cold_store.write_chunk(&payload, b"second").await.unwrap();
    let payload = PartId::new(payload.rsplit('/').next().unwrap()).unwrap();
    let missing = PartId::new(format!("{:032x}-{:016x}.part", 1, 2)).unwrap();
    let good = digest_of(b"firstsecond");

    for (case, parts, digest) in [
        (
            "digest mismatch",
            vec![a.clone(), b.clone()],
            digest_of(b"other"),
        ),
        ("order", vec![b.clone(), a.clone()], good),
        ("unknown part", vec![a.clone(), missing], good),
        ("another stream's part", vec![a.clone(), foreign], good),
        ("a payload that is no part", vec![a.clone(), payload], good),
    ] {
        let response = send(&app, commit_request("/parts/s", &parts, digest)).await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{case}");
    }
    // A manifest that lists another number of parts than its header.
    let mut request = commit_request("/parts/s", &[a.clone(), b.clone()], good);
    request
        .headers_mut()
        .insert(names::STREAM_SNAPSHOT_PARTS, HeaderValue::from_static("3"));
    assert_eq!(send(&app, request).await.status(), StatusCode::BAD_REQUEST);

    let response = send(&app, Request::head("/parts/s").body(Body::empty()).unwrap()).await;
    assert_eq!(
        StreamHead::parse(pairs(&response))
            .expect("head")
            .snapshot(),
        None,
        "nothing was published"
    );
    let response = send(&app, commit_request("/parts/s", &[a, b], good)).await;
    assert_eq!(
        response.status(),
        StatusCode::NO_CONTENT,
        "the parts stayed"
    );
}

/// An assembled snapshot above the snapshot limit is refused with `413`
/// naming that limit, before anything is copied. One part listed many
/// times reaches it (a manifest may repeat an id).
#[tokio::test]
async fn an_assembled_snapshot_above_the_snapshot_limit_names_it() {
    let (app, _cold_store) = cold_app();
    create(&app, "/parts/s").await;
    let part = stage(&app, "/parts/s", vec![7; MAX_HTTP_BODY_BYTES]).await;
    let copies = usize::try_from(MAX_COLD_SNAPSHOT_BYTES).unwrap() / MAX_HTTP_BODY_BYTES + 1;
    let response = send(
        &app,
        commit_request("/parts/s", &vec![part; copies], digest_of(b"")),
    )
    .await;
    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(
        SizeLimit::parse(413, pairs(&response)),
        Ok(Some(SizeLimit::Snapshot(MAX_COLD_SNAPSHOT_BYTES)))
    );
}

/// A part is a write routed before its body: it needs the stream (and the
/// token a precondition names), and a body above the request limit.
#[tokio::test]
async fn a_part_needs_its_stream_and_fits_one_request() {
    let (app, _cold_store) = cold_app();
    let polled = Arc::new(AtomicBool::new(false));
    let watched = {
        let polled = Arc::clone(&polled);
        Body::from_stream(futures_util::stream::poll_fn(move |_cx| {
            polled.store(true, Ordering::SeqCst);
            std::task::Poll::Ready(None::<Result<Bytes, std::convert::Infallible>>)
        }))
    };
    let response = send(
        &app,
        Request::post("/parts/missing/snapshot/3/parts")
            .header(CONTENT_LENGTH, 4)
            .header("expect", "100-continue")
            .body(watched)
            .unwrap(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    assert!(!polled.load(Ordering::SeqCst), "read the body");

    create(&app, "/parts/s").await;
    let response = send(&app, Request::head("/parts/s").body(Body::empty()).unwrap()).await;
    let token = StreamHead::parse(pairs(&response))
        .expect("head")
        .incarnation();
    let other = (token.get() ^ 1).to_string();
    let response = send(
        &app,
        Request::post("/parts/s/snapshot/3/parts")
            .header(names::STREAM_INCARNATION, other)
            .body(Body::from("part"))
            .unwrap(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::PRECONDITION_FAILED);
    let response = send(
        &app,
        Request::post("/parts/s/snapshot/3/parts")
            .header(names::STREAM_INCARNATION, token.to_string())
            .body(Body::from("part"))
            .unwrap(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::CREATED);

    let over = Body::from_stream(futures_util::stream::iter(
        (0..33).map(|_| Ok::<_, std::convert::Infallible>(Bytes::from(vec![1_u8; MIB]))),
    ));
    let response = send(
        &app,
        Request::post("/parts/s/snapshot/3/parts")
            .body(over)
            .unwrap(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(
        SizeLimit::parse(413, pairs(&response)),
        Ok(Some(SizeLimit::Request(MAX_HTTP_BODY_BYTES as u64)))
    );
}

/// Without a cold store, a part and a commit answer `501`, and HEAD sends
/// no snapshot limit: a snapshot must fit one request.
#[tokio::test]
async fn a_node_without_a_cold_store_takes_no_parts() {
    let app = router(ShardRuntime::spawn(RuntimeConfig::new(1, 1)).expect("runtime"));
    create(&app, "/parts/s").await;
    let response = send(
        &app,
        Request::post("/parts/s/snapshot/3/parts")
            .body(Body::from("part"))
            .unwrap(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::NOT_IMPLEMENTED);
    let part = PartId::new(format!("{:032x}-{:016x}.part", 1, 2)).unwrap();
    let response = send(&app, commit_request("/parts/s", &[part], digest_of(b"x"))).await;
    assert_eq!(response.status(), StatusCode::NOT_IMPLEMENTED);
    let response = send(&app, Request::head("/parts/s").body(Body::empty()).unwrap()).await;
    assert_eq!(
        StreamHead::parse(pairs(&response))
            .expect("head")
            .max_snapshot_bytes(),
        None
    );
}

/// End to end through a gateway that lowers the request limit (#581
/// section 10, end to end): HEAD advertises the gateway's limit and the
/// node's snapshot limit, a snapshot above the gateway's limit is refused in
/// one request and goes up in parts, each under it, and reads back by range.
#[tokio::test]
async fn a_snapshot_above_a_gateways_limit_goes_up_in_parts_through_it() {
    const GATEWAY_LIMIT: usize = 4 * MIB;
    let (app, _cold_store) = cold_app();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind node");
    let node = format!("http://{}", listener.local_addr().expect("node address"));
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.expect("serve node");
    });
    let gateway = ursula_gateway::Gateway::new(ursula_gateway::GatewayConfig {
        listen: "127.0.0.1:0".parse().expect("listen"),
        upstreams: vec![node],
        response_header_timeout: std::time::Duration::from_secs(30),
        connect_timeout: std::time::Duration::from_secs(5),
        upstream_tcp_user_timeout_secs: std::num::NonZeroU64::new(30).expect("non-zero"),
        max_request_body_bytes: GATEWAY_LIMIT,
        raft_group_count: None,
        cors_allowed_origins: Vec::new(),
        usage_chunk_bytes: None,
    });

    let response = gateway
        .handle(
            Request::put("/parts/s")
                .header(CONTENT_TYPE, OCTET)
                .body(Body::from("abc"))
                .unwrap(),
        )
        .await;
    assert_eq!(response.status(), StatusCode::CREATED);
    let response = gateway
        .handle(Request::head("/parts/s").body(Body::empty()).unwrap())
        .await;
    let head = StreamHead::parse(pairs(&response)).expect("stream head");
    assert_eq!(head.max_request_bytes(), GATEWAY_LIMIT as u64);
    assert_eq!(head.max_snapshot_bytes(), Some(MAX_COLD_SNAPSHOT_BYTES));

    let snapshot = bytes(10 * MIB + 17, 9);
    let response = gateway
        .handle(
            Request::put("/parts/s/snapshot/3")
                .header(CONTENT_TYPE, OCTET)
                .header(CONTENT_LENGTH, snapshot.len())
                .body(Body::from(snapshot.clone()))
                .unwrap(),
        )
        .await;
    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(
        SizeLimit::parse(413, pairs(&response)),
        Ok(Some(SizeLimit::Request(GATEWAY_LIMIT as u64)))
    );

    let mut parts = Vec::new();
    for chunk in snapshot.chunks(GATEWAY_LIMIT) {
        let response = gateway
            .handle(
                Request::post("/parts/s/snapshot/3/parts")
                    .header(CONTENT_LENGTH, chunk.len())
                    .body(Body::from(chunk.to_vec()))
                    .unwrap(),
            )
            .await;
        assert_eq!(response.status(), StatusCode::CREATED);
        parts.push(PartStaged::parse(pairs(&response)).expect("a part id").part);
    }
    assert_eq!(parts.len(), 3);
    let digest = digest_of(&snapshot);
    let response = gateway
        .handle(commit_request("/parts/s", &parts, digest))
        .await;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    let response = gateway
        .handle(
            Request::get("/parts/s/snapshot/3")
                .header("range", "bytes=-17")
                .body(Body::empty())
                .unwrap(),
        )
        .await;
    assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
    assert_eq!(
        SnapshotRead::parse(pairs(&response))
            .expect("read")
            .snapshot
            .digest,
        digest
    );
    assert!(body_of(response).await == snapshot[snapshot.len() - 17..]);
    server.abort();
}

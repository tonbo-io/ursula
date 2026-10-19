//! Snapshots uploaded in parts over HTTP (#581 section 3.1, test plan item
//! 6): staging, the commit and its refusals, the snapshot limit, the key
//! checked before the body, and a node without a cold store.

use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;

use axum::body::Body;
use axum::body::to_bytes;
use axum::http::Request;
use axum::http::header::RETRY_AFTER;
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
use ursula_runtime::AppendExternalRequest;
use ursula_runtime::AppendRequest;
use ursula_runtime::CloseStreamRequest;
use ursula_runtime::ColdStore;
use ursula_runtime::ColdWriteAdmission;
use ursula_runtime::CreateStreamExternalRequest;
use ursula_runtime::CreateStreamRequest;
use ursula_runtime::DeleteStreamRequest;
use ursula_runtime::GroupAppendFuture;
use ursula_runtime::GroupBucketUsageFuture;
use ursula_runtime::GroupCloseStreamFuture;
use ursula_runtime::GroupCreateStreamFuture;
use ursula_runtime::GroupDeleteStreamFuture;
use ursula_runtime::GroupEngine;
use ursula_runtime::GroupEngineCreateFuture;
use ursula_runtime::GroupEngineError;
use ursula_runtime::GroupEngineFactory;
use ursula_runtime::GroupEngineMetrics;
use ursula_runtime::GroupHeadStreamFuture;
use ursula_runtime::GroupInstallSnapshotFuture;
use ursula_runtime::GroupPublishSnapshotFuture;
use ursula_runtime::GroupReadStreamFuture;
use ursula_runtime::GroupRouteWriteFuture;
use ursula_runtime::GroupSnapshot;
use ursula_runtime::GroupSnapshotFuture;
use ursula_runtime::GroupTouchStreamAccessFuture;
use ursula_runtime::HeadStreamRequest;
use ursula_runtime::InMemoryGroupEngine;
use ursula_runtime::InMemoryGroupEngineFactory;
use ursula_runtime::LeadershipReason;
use ursula_runtime::LeadershipRefusal;
use ursula_runtime::MAX_COLD_SNAPSHOT_BYTES;
use ursula_runtime::Proposal;
use ursula_runtime::PublishSnapshotRequest;
use ursula_runtime::ReadStreamRequest;
use ursula_runtime::RouteWriteRequest;
use ursula_runtime::RuntimeConfig;
use ursula_runtime::ShardRuntime;
use ursula_runtime::SnapshotDigest;
use ursula_runtime::cold_external_dir;
use ursula_runtime::new_external_payload_path;
use ursula_runtime::snapshot_part_dir;
use ursula_shard::BucketStreamId;
use ursula_shard::ShardPlacement;

use super::*;

const OCTET: &str = "application/octet-stream";
const MIB: usize = 1 << 20;

fn cold_state() -> (HttpState, Arc<ColdStore>) {
    let cold_store = Arc::new(ColdStore::memory().expect("memory cold store"));
    let runtime = ShardRuntime::spawn_with_engine_factory_and_cold_store(
        RuntimeConfig::new(1, 1),
        InMemoryGroupEngineFactory::with_cold_store(Some(cold_store.clone())),
        Some(cold_store.clone()),
    )
    .expect("runtime");
    (HttpState::new(runtime), cold_store)
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
            .header(CONTENT_LENGTH, body.len())
            .body(Body::from(body))
            .expect("request"),
    )
    .await;
    assert_eq!(response.status(), StatusCode::CREATED, "{uri}");
    PartStaged::parse(pairs(&response)).expect("a part id").part
}

pub(crate) fn digest_of(body: &[u8]) -> DigestHeader {
    let mut digest = SnapshotDigest::new(OCTET);
    digest.update(body);
    DigestHeader::parse("digest", &digest.finalize()).expect("hex digest")
}

pub(crate) fn commit_request(uri: &str, parts: &[PartId], digest: DigestHeader) -> Request<Body> {
    commit_request_at(uri, 3, parts, digest)
}

fn commit_request_at(
    uri: &str,
    offset: u64,
    parts: &[PartId],
    digest: DigestHeader,
) -> Request<Body> {
    let manifest = PartsManifest::new(parts.to_vec()).expect("manifest");
    let mut request = Request::put(format!("{uri}/snapshot/{offset}")).header(CONTENT_TYPE, OCTET);
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

/// The staged parts of the test bucket's group 0, the only group.
async fn part_names(cold_store: &ColdStore) -> Vec<String> {
    cold_store
        .list_file_names(&snapshot_part_dir("parts", 0))
        .await
        .expect("list part dir")
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
    assert_eq!(part_names(&cold_store).await.len(), 3);
    assert!(external_names(&cold_store, "s").await.is_empty());

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
    assert!(
        part_names(&cold_store).await.is_empty(),
        "the parts are deleted"
    );
    let names = external_names(&cold_store, "s").await;
    assert_eq!(names.len(), 1, "the assembled snapshot: {names:?}");
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

/// A commit that names a part it cannot use is refused with `400` before
/// anything is copied, and its parts stay for a corrected commit. One whose
/// parts do not assemble to its digest is refused with `400` too, and its
/// copy consumed the parts: they are gone (m1 of the step 4 re-review).
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

    for (case, parts) in [
        ("unknown part", vec![a.clone(), missing]),
        ("another stream's part", vec![a.clone(), foreign]),
        ("a payload that is no part", vec![a.clone(), payload]),
    ] {
        let response = send(&app, commit_request("/parts/s", &parts, good)).await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{case}");
    }
    // A manifest that lists another number of parts than its header.
    let mut request = commit_request("/parts/s", &[a.clone(), b.clone()], good);
    request
        .headers_mut()
        .insert(names::STREAM_SNAPSHOT_PARTS, HeaderValue::from_static("3"));
    assert_eq!(send(&app, request).await.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        part_names(&cold_store).await.len(),
        3,
        "nothing copied, all kept"
    );

    let response = send(
        &app,
        commit_request("/parts/s", &[b.clone(), a.clone()], good),
    )
    .await;
    assert_eq!(
        response.status(),
        StatusCode::BAD_REQUEST,
        "order: a digest mismatch"
    );
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
        StatusCode::BAD_REQUEST,
        "the mismatched copy consumed the parts"
    );
    assert_eq!(
        part_names(&cold_store).await.len(),
        1,
        "only the other stream's"
    );
}

/// A manifest that names one part twice is refused with `400` before
/// anything is copied: a commit costs at most the bytes its client staged.
/// (The snapshot limit is checked from the parts' sizes, in
/// `snapshot_parts::tests`.)
#[tokio::test]
async fn a_manifest_that_repeats_a_part_is_refused() {
    let (app, _cold_store) = cold_app();
    create(&app, "/parts/s").await;
    let part = stage(&app, "/parts/s", b"part".to_vec()).await;
    let response = send(
        &app,
        Request::put("/parts/s/snapshot/3")
            .header(CONTENT_TYPE, OCTET)
            .header(names::STREAM_SNAPSHOT_PARTS, "2")
            .header(
                names::STREAM_SNAPSHOT_DIGEST,
                digest_of(b"partpart").to_string(),
            )
            .body(Body::from(format!("{part}\n{part}\n")))
            .unwrap(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        crate::render::error_code_of(response).await,
        ursula_headers::ErrorCode::InvalidPartsCommit
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
    assert_eq!(
        crate::render::error_code_of(response).await,
        ursula_headers::ErrorCode::RequestTooLarge
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
    assert_eq!(
        crate::render::error_code_of(response).await,
        ursula_headers::ErrorCode::NoColdStore
    );
    let part = PartId::new(format!("{:032x}-{:016x}.part", 1, 2)).unwrap();
    let response = send(&app, commit_request("/parts/s", &[part], digest_of(b"x"))).await;
    assert_eq!(response.status(), StatusCode::NOT_IMPLEMENTED);
    assert_eq!(
        crate::render::error_code_of(response).await,
        ursula_headers::ErrorCode::NoColdStore
    );
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
        uploads: ursula_gateway::uploads::UploadLimits::try_from(
            ursula_gateway::uploads::UploadLimitsSpec {
                max_request_body_bytes: GATEWAY_LIMIT,
                ..ursula_gateway::uploads::UploadLimitsSpec::default()
            },
        )
        .expect("upload limits"),
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

/// An in-memory engine whose first `refusals` snapshot publishes are refused
/// before they are proposed.
struct FlakyPublishEngine {
    inner: InMemoryGroupEngine,
    refusals: Arc<AtomicUsize>,
}

impl GroupEngine for FlakyPublishEngine {
    fn upload_fence(&self) -> Option<ursula_runtime::UploadFence> {
        self.inner.upload_fence()
    }

    fn route_write<'a>(
        &'a mut self,
        request: RouteWriteRequest,
        placement: ShardPlacement,
    ) -> GroupRouteWriteFuture<'a> {
        self.inner.route_write(request, placement)
    }

    fn create_stream<'a>(
        &'a mut self,
        request: CreateStreamRequest,
        placement: ShardPlacement,
        admission: ColdWriteAdmission,
    ) -> GroupCreateStreamFuture<'a> {
        self.inner.create_stream(request, placement, admission)
    }

    fn create_stream_external<'a>(
        &'a mut self,
        request: CreateStreamExternalRequest,
        placement: ShardPlacement,
    ) -> GroupCreateStreamFuture<'a> {
        self.inner.create_stream_external(request, placement)
    }

    fn head_stream<'a>(
        &'a mut self,
        request: HeadStreamRequest,
        placement: ShardPlacement,
    ) -> GroupHeadStreamFuture<'a> {
        self.inner.head_stream(request, placement)
    }

    fn bucket_usage<'a>(&'a mut self, placement: ShardPlacement) -> GroupBucketUsageFuture<'a> {
        self.inner.bucket_usage(placement)
    }

    fn read_stream<'a>(
        &'a mut self,
        request: ReadStreamRequest,
        placement: ShardPlacement,
    ) -> GroupReadStreamFuture<'a> {
        self.inner.read_stream(request, placement)
    }

    fn touch_stream_access<'a>(
        &'a mut self,
        stream_id: BucketStreamId,
        now_ms: u64,
        renew_ttl: bool,
        placement: ShardPlacement,
    ) -> GroupTouchStreamAccessFuture<'a> {
        self.inner
            .touch_stream_access(stream_id, now_ms, renew_ttl, placement)
    }

    fn close_stream<'a>(
        &'a mut self,
        request: CloseStreamRequest,
        placement: ShardPlacement,
    ) -> GroupCloseStreamFuture<'a> {
        self.inner.close_stream(request, placement)
    }

    fn delete_stream<'a>(
        &'a mut self,
        request: DeleteStreamRequest,
        placement: ShardPlacement,
    ) -> GroupDeleteStreamFuture<'a> {
        self.inner.delete_stream(request, placement)
    }

    fn append<'a>(
        &'a mut self,
        request: AppendRequest,
        placement: ShardPlacement,
        admission: ColdWriteAdmission,
    ) -> GroupAppendFuture<'a> {
        self.inner.append(request, placement, admission)
    }

    fn append_external<'a>(
        &'a mut self,
        request: AppendExternalRequest,
        placement: ShardPlacement,
    ) -> GroupAppendFuture<'a> {
        self.inner.append_external(request, placement)
    }

    fn publish_snapshot<'a>(
        &'a mut self,
        request: PublishSnapshotRequest,
        placement: ShardPlacement,
    ) -> GroupPublishSnapshotFuture<'a> {
        // The first publishes are refused before they are proposed, as a
        // leadership change during the copy refuses them: a retryable `503`.
        let refused = self
            .refusals
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |left| {
                left.checked_sub(1)
            })
            .is_ok();
        if refused {
            return Box::pin(async {
                Err(GroupEngineError::NotLeader(LeadershipRefusal {
                    leader: None,
                    proposal: Proposal::NotProposed,
                    reason: LeadershipReason::Follower,
                }))
            });
        }
        self.inner.publish_snapshot(request, placement)
    }

    fn snapshot<'a>(&'a mut self, placement: ShardPlacement) -> GroupSnapshotFuture<'a> {
        self.inner.snapshot(placement)
    }

    fn install_snapshot<'a>(
        &'a mut self,
        snapshot: GroupSnapshot,
    ) -> GroupInstallSnapshotFuture<'a> {
        self.inner.install_snapshot(snapshot)
    }
}

struct FlakyPublishEngineFactory {
    cold_store: Arc<ColdStore>,
    refusals: Arc<AtomicUsize>,
}

impl GroupEngineFactory for FlakyPublishEngineFactory {
    fn create<'a>(
        &'a self,
        placement: ShardPlacement,
        _metrics: GroupEngineMetrics,
    ) -> GroupEngineCreateFuture<'a> {
        let mut inner = InMemoryGroupEngine::with_cold_store(self.cold_store.clone());
        // An engine without Raft starts its group with its genesis.
        inner.start_genesis(placement, 0);
        let refusals = self.refusals.clone();
        Box::pin(async move {
            let engine: Box<dyn GroupEngine> = Box::new(FlakyPublishEngine { inner, refusals });
            Ok(engine)
        })
    }
}

/// B1 of the step 4 review: a commit refused with a retryable `503` after
/// its copy keeps its parts, and its repeat publishes.
#[tokio::test]
async fn a_retryable_refusal_keeps_the_parts_for_the_repeat() {
    let cold_store = Arc::new(ColdStore::memory().expect("memory cold store"));
    let refusals = Arc::new(AtomicUsize::new(1));
    let runtime = ShardRuntime::spawn_with_engine_factory_and_cold_store(
        RuntimeConfig::new(1, 1),
        FlakyPublishEngineFactory {
            cold_store: cold_store.clone(),
            refusals: refusals.clone(),
        },
        Some(cold_store.clone()),
    )
    .expect("runtime");
    let app = router(runtime);
    create(&app, "/parts/s").await;
    let parts = vec![
        stage(&app, "/parts/s", b"first".to_vec()).await,
        stage(&app, "/parts/s", b"second".to_vec()).await,
    ];
    let digest = digest_of(b"firstsecond");
    let response = send(&app, commit_request("/parts/s", &parts, digest)).await;
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert!(response.headers().contains_key(RETRY_AFTER));
    assert_eq!(part_names(&cold_store).await.len(), 2, "the parts stay");
    assert!(
        external_names(&cold_store, "s").await.is_empty(),
        "the copy of a refusal before proposal is deleted (F5)"
    );
    let response = send(&app, commit_request("/parts/s", &parts, digest)).await;
    assert_eq!(
        response.status(),
        StatusCode::NO_CONTENT,
        "the repeat publishes"
    );
    assert!(
        part_names(&cold_store).await.is_empty(),
        "then the parts go"
    );
}

/// M1 of the step 4 review: a repeat of a commit whose snapshot another one
/// superseded, or that another snapshot at its offset beat, answers `409`
/// naming the latest from its key alone, with its parts long gone.
#[tokio::test]
async fn a_superseded_or_conflicting_commit_answers_409_from_its_key() {
    let (app, cold_store) = cold_app();
    create(&app, "/parts/s").await;
    let parts = vec![stage(&app, "/parts/s", b"three".to_vec()).await];
    let digest = digest_of(b"three");
    let response = send(&app, commit_request("/parts/s", &parts, digest)).await;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    assert!(part_names(&cold_store).await.is_empty());

    // Another commit at the same offset, with parts that never existed.
    let unknown = vec![PartId::new(format!("{:032x}-{:016x}.part", 1, 2)).unwrap()];
    let response = send(
        &app,
        commit_request("/parts/s", &unknown, digest_of(b"other")),
    )
    .await;
    assert_eq!(
        response.status(),
        StatusCode::CONFLICT,
        "not 400 unknown part"
    );
    let SnapshotPublishAnswer::Conflict { latest } =
        SnapshotPublishAnswer::parse(409, pairs(&response))
            .expect("typed")
            .expect("names the latest")
    else {
        panic!("a conflict");
    };
    assert_eq!(latest.digest, digest);

    // A later snapshot supersedes the commit; its repeat learns so.
    let response = send(
        &app,
        Request::post("/parts/s")
            .header(CONTENT_TYPE, OCTET)
            .body(Body::from("def"))
            .unwrap(),
    )
    .await;
    assert!(response.status().is_success());
    let response = send(
        &app,
        Request::put("/parts/s/snapshot/6")
            .header(CONTENT_TYPE, OCTET)
            .body(Body::from("six"))
            .unwrap(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    let response = send(&app, commit_request("/parts/s", &parts, digest)).await;
    assert_eq!(response.status(), StatusCode::CONFLICT);
    let answer = SnapshotPublishAnswer::parse(409, pairs(&response))
        .expect("typed")
        .expect("names the latest");
    assert!(answer.supersedes(ursula_headers::value::Offset::from_u64(3)));
}

/// Holds every read of a staged part until [`Gate::open`], counting the
/// reads that wait, so a test acts while a commit copies.
pub(crate) struct Gate {
    permits: Arc<tokio::sync::Semaphore>,
    waiting: Arc<AtomicUsize>,
}

impl Gate {
    pub(crate) fn on_part_reads(cold_store: &ColdStore) -> Self {
        let permits = Arc::new(tokio::sync::Semaphore::new(0));
        let waiting = Arc::new(AtomicUsize::new(0));
        cold_store.set_fault_policy(|context| {
            (context.operation == ursula_runtime::ColdStoreOperation::ReadObjectRange
                && context.path.contains("/_parts/"))
            .then(|| ursula_runtime::ColdStoreFaultEffect::delay(Duration::from_millis(1)))
        });
        let (gate, count) = (Arc::clone(&permits), Arc::clone(&waiting));
        cold_store.set_delay_fn(move |_| {
            let (gate, count) = (Arc::clone(&gate), Arc::clone(&count));
            async move {
                count.fetch_add(1, Ordering::SeqCst);
                if let Ok(permit) = gate.acquire().await {
                    permit.forget();
                }
            }
        });
        Self { permits, waiting }
    }

    /// Waits until `reads` part reads wait at the gate.
    pub(crate) async fn reached(&self, reads: usize) {
        while self.waiting.load(Ordering::SeqCst) < reads {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    }

    pub(crate) fn open(&self) {
        self.permits.add_permits(1 << 20);
    }
}

async fn latest_snapshot(app: &Router, uri: &str) -> Option<ursula_headers::SnapshotRef> {
    let response = send(app, Request::head(uri).body(Body::empty()).unwrap()).await;
    StreamHead::parse(pairs(&response))
        .expect("head")
        .snapshot()
}

/// MB of the step 4 re-review: the node owns a commit, so a client that goes
/// away mid-copy (a gateway timing out) cancels nothing. A repeat while it
/// runs answers `503 SnapshotCommitInProgress`; once it ends, `204` from the
/// key, and its parts are gone.
#[tokio::test]
async fn a_commit_finishes_after_its_client_goes_away() {
    let (app, cold_store) = cold_app();
    create(&app, "/parts/s").await;
    let parts = vec![
        stage(&app, "/parts/s", b"first".to_vec()).await,
        stage(&app, "/parts/s", b"second".to_vec()).await,
    ];
    let digest = digest_of(b"firstsecond");
    let gate = Gate::on_part_reads(&cold_store);
    let gone = tokio::time::timeout(
        Duration::from_millis(50),
        send(&app, commit_request("/parts/s", &parts, digest)),
    )
    .await;
    assert!(gone.is_err(), "the commit waits at the gate");
    gate.reached(1).await;
    let response = send(&app, commit_request("/parts/s", &parts, digest)).await;
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert!(response.headers().contains_key(RETRY_AFTER));
    assert_eq!(
        crate::render::error_code_of(response).await,
        ursula_headers::ErrorCode::SnapshotCommitInProgress
    );

    gate.open();
    while latest_snapshot(&app, "/parts/s").await.is_none() {
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    while !part_names(&cold_store).await.is_empty() {
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    let response = send(&app, commit_request("/parts/s", &parts, digest)).await;
    assert_eq!(response.status(), StatusCode::NO_CONTENT, "the repeat");
}

/// m4: a node copies two commits at once; a third answers `503
/// SnapshotCommitsBusy`, which the metrics count.
#[tokio::test]
async fn a_third_concurrent_commit_is_busy() {
    let (app, cold_store) = cold_app();
    let mut pending = Vec::new();
    for stream in ["a", "b", "c"] {
        create(&app, &format!("/parts/{stream}")).await;
    }
    let commits = {
        let mut commits = Vec::new();
        for stream in ["a", "b", "c"] {
            let uri = format!("/parts/{stream}");
            let part = stage(&app, &uri, stream.as_bytes().to_vec()).await;
            commits.push(commit_request(&uri, &[part], digest_of(stream.as_bytes())));
        }
        commits
    };
    let gate = Gate::on_part_reads(&cold_store);
    let mut commits = commits.into_iter();
    for _ in 0..2 {
        let request = commits.next().unwrap();
        pending.push(tokio::spawn(app.clone().oneshot(request)));
    }
    gate.reached(2).await;
    let response = send(&app, commits.next().unwrap()).await;
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert!(response.headers().contains_key(RETRY_AFTER));
    assert_eq!(
        crate::render::error_code_of(response).await,
        ursula_headers::ErrorCode::SnapshotCommitsBusy
    );
    let metrics = send(
        &app,
        Request::get("/__ursula/metrics")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    let metrics: serde_json::Value = serde_json::from_slice(&body_of(metrics).await).unwrap();
    assert_eq!(metrics["snapshot_commits_busy"], 1, "{metrics}");
    gate.open();
    for commit in pending {
        assert_eq!(
            commit.await.unwrap().unwrap().status(),
            StatusCode::NO_CONTENT
        );
    }
}

/// MA of the step 4 re-review and M2 of the #618 review: on a 64 MiB
/// budget (the tiny and small presets) a copy is charged 32 MiB once it
/// holds its slot, and only while a request's worth stays free beside it.
/// So a second commit answers `503 SnapshotCommitsBusy` instead of taking
/// that room, and a request of the full 32 MiB limit still fits while the
/// first copy runs. A manifest without `Content-Length` is charged its own
/// bound, not the request limit, and gives it back once read.
#[tokio::test]
async fn one_copy_and_one_full_request_fit_the_smallest_budget() {
    let cold_store = Arc::new(ColdStore::memory().expect("memory cold store"));
    let runtime = ShardRuntime::spawn_with_engine_factory_and_cold_store(
        RuntimeConfig::new(1, 1),
        InMemoryGroupEngineFactory::with_cold_store(Some(cold_store.clone())),
        Some(cold_store.clone()),
    )
    .expect("runtime");
    let server = ursula_config::ServerConfig {
        http_inflight_body_size: ursula_config::HumanSize::mib(64),
        ..ursula_config::ServerConfig::default()
    };
    let app = client_router_with_admission(HttpState::new(runtime), IngressAdmission::new(&server));
    create(&app, "/parts/s").await;
    create(&app, "/parts/t").await;
    let first = stage(&app, "/parts/s", b"part".to_vec()).await;
    let second = stage(&app, "/parts/t", b"part".to_vec()).await;
    let gate = Gate::on_part_reads(&cold_store);
    let mut request = commit_request("/parts/s", &[first], digest_of(b"part"));
    // A manifest without `Content-Length`.
    let manifest = std::mem::replace(request.body_mut(), Body::empty());
    *request.body_mut() = Body::from_stream(manifest.into_data_stream());
    let pending = tokio::spawn(app.clone().oneshot(request));
    gate.reached(1).await;

    let response = send(
        &app,
        commit_request("/parts/t", &[second], digest_of(b"part")),
    )
    .await;
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        crate::render::error_code_of(response).await,
        ursula_headers::ErrorCode::SnapshotCommitsBusy
    );

    let response = send(
        &app,
        Request::post("/parts/s")
            .header(CONTENT_TYPE, OCTET)
            .header(CONTENT_LENGTH, MAX_HTTP_BODY_BYTES)
            .body(Body::from(vec![7_u8; MAX_HTTP_BODY_BYTES]))
            .unwrap(),
    )
    .await;
    assert!(response.status().is_success(), "{:?}", response.status());
    gate.open();
    assert_eq!(
        pending.await.unwrap().unwrap().status(),
        StatusCode::NO_CONTENT
    );
}

/// M1 of the #618 review: a repeat whose key check ran while the original
/// still copied, and whose parts the original deleted after it published,
/// answers `204` from its key again, not `400` for an unknown part. The
/// steps are ordered by signals, not by time: the repeat's first poll of its
/// manifest (its key check is behind it), and the original's end.
#[tokio::test]
async fn a_repeat_that_raced_the_original_answers_from_the_key() {
    let (state, cold_store) = cold_state();
    let app = router_with_http_state(state.clone());
    let mut ended = state.snapshot_commits.ended();
    create(&app, "/parts/s").await;
    let parts = vec![stage(&app, "/parts/s", b"part".to_vec()).await];
    let digest = digest_of(b"part");
    let gate = Gate::on_part_reads(&cold_store);
    let original = tokio::spawn(
        app.clone()
            .oneshot(commit_request("/parts/s", &parts, digest)),
    );
    gate.reached(1).await;

    // The repeat passes its key check, then asks for its manifest and waits.
    let mut repeat = commit_request("/parts/s", &parts, digest);
    let manifest = std::mem::replace(repeat.body_mut(), Body::empty());
    let manifest = to_bytes(manifest, usize::MAX).await.unwrap();
    let (asked, asked_for_manifest) = tokio::sync::oneshot::channel::<()>();
    let (send_manifest, manifest_sent) = tokio::sync::oneshot::channel::<Bytes>();
    *repeat.body_mut() = Body::from_stream(futures_util::stream::once(async move {
        // The test awaits this signal, so its receiver is alive.
        asked.send(()).expect("the test waits for the signal");
        Ok::<_, std::convert::Infallible>(manifest_sent.await.unwrap_or_default())
    }));
    let repeat = tokio::spawn(app.clone().oneshot(repeat));
    asked_for_manifest.await.unwrap();

    gate.open();
    assert_eq!(
        original.await.unwrap().unwrap().status(),
        StatusCode::NO_CONTENT
    );
    // The original's task deletes its parts, then ends.
    ended.wait_for(|ended| *ended >= 1).await.unwrap();
    assert!(part_names(&cold_store).await.is_empty());
    send_manifest.send(manifest).unwrap();
    let response = repeat.await.unwrap().unwrap();
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    assert!(matches!(
        SnapshotPublishAnswer::parse(204, pairs(&response)),
        Ok(Some(SnapshotPublishAnswer::Published { .. }))
    ));
}

/// #618 review: a part whose deletion the `DeleteChunk` fault policy
/// refuses stays, the commit's answer does not change, and the orphan sweep
/// reclaims the part later.
#[tokio::test]
async fn a_part_that_fails_to_delete_is_swept_later() {
    let (state, cold_store) = cold_state();
    let app = router_with_http_state(state.clone());
    let mut ended = state.snapshot_commits.ended();
    create(&app, "/parts/s").await;
    let parts = vec![stage(&app, "/parts/s", b"part".to_vec()).await];
    cold_store.set_fault_policy(|context| {
        (context.operation == ursula_runtime::ColdStoreOperation::DeleteChunk
            && context.path.contains("/_parts/"))
        .then(|| ursula_runtime::ColdStoreFaultEffect::fail("injected: delete refused"))
    });
    let events = crate::staging_cleanup_tests::observe(&cold_store);
    let response = send(&app, commit_request("/parts/s", &parts, digest_of(b"part"))).await;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    ended.wait_for(|ended| *ended >= 1).await.unwrap();
    assert_eq!(part_names(&cold_store).await.len(), 1, "the part stays");
    let begun = events
        .lock()
        .unwrap()
        .iter()
        .filter(|event| {
            matches!(event, ursula_runtime::ColdStoreEvent::DeleteChunkBegin { path } if path.contains("/_parts/"))
        })
        .count();
    assert_eq!(begun, 1, "the batch delete goes through the delete hooks");

    cold_store.clear_fault_policy();
    tokio::time::sleep(Duration::from_millis(2)).await;
    let swept = state
        .runtime
        .sweep_cold_orphans_group_once(ursula_shard::RaftGroupId(0), 16, 0)
        .await
        .expect("sweep");
    assert_eq!(swept.orphans_deleted, 1);
    assert!(
        part_names(&cold_store).await.is_empty(),
        "the sweep reclaims it"
    );
}

/// m4: a part that goes away during the copy re-checks the key: a later
/// snapshot published meanwhile answers `409` naming it.
#[tokio::test]
async fn a_part_gone_mid_copy_rechecks_the_key() {
    let (app, cold_store) = cold_app();
    create(&app, "/parts/s").await;
    let parts = vec![
        stage(&app, "/parts/s", b"first".to_vec()).await,
        stage(&app, "/parts/s", b"second".to_vec()).await,
    ];
    let gate = Gate::on_part_reads(&cold_store);
    let pending = tokio::spawn(app.clone().oneshot(commit_request(
        "/parts/s",
        &parts,
        digest_of(b"firstsecond"),
    )));
    gate.reached(2).await;
    let response = send(
        &app,
        Request::post("/parts/s")
            .header(CONTENT_TYPE, OCTET)
            .body(Body::from("def"))
            .unwrap(),
    )
    .await;
    assert!(response.status().is_success());
    let response = send(
        &app,
        Request::put("/parts/s/snapshot/6")
            .header(CONTENT_TYPE, OCTET)
            .body(Body::from("six"))
            .unwrap(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    for name in part_names(&cold_store).await {
        cold_store
            .delete_chunk(&format!("{}{name}", snapshot_part_dir("parts", 0)))
            .await
            .unwrap();
    }
    gate.open();
    let response = pending.await.unwrap().unwrap();
    assert_eq!(response.status(), StatusCode::CONFLICT);
    assert!(matches!(
        SnapshotPublishAnswer::parse(409, pairs(&response)),
        Ok(Some(SnapshotPublishAnswer::Conflict { .. }))
    ));
}

/// m3: a stream recreated during the copy is not published to. A client
/// that named no token gets the typed `400` a commit after the recreate
/// gets; one that named the token gets `412`.
#[tokio::test]
async fn a_stream_recreated_mid_copy_is_not_published_to() {
    for sent_token in [false, true] {
        let (app, cold_store) = cold_app();
        create(&app, "/parts/s").await;
        let response = send(&app, Request::head("/parts/s").body(Body::empty()).unwrap()).await;
        let token = StreamHead::parse(pairs(&response))
            .expect("head")
            .incarnation();
        let part = stage(&app, "/parts/s", b"part".to_vec()).await;
        let gate = Gate::on_part_reads(&cold_store);
        let mut request = commit_request("/parts/s", &[part], digest_of(b"part"));
        if sent_token {
            request.headers_mut().insert(
                names::STREAM_INCARNATION,
                HeaderValue::from_str(&token.to_string()).unwrap(),
            );
        }
        let pending = tokio::spawn(app.clone().oneshot(request));
        gate.reached(1).await;
        let response = send(
            &app,
            Request::delete("/parts/s").body(Body::empty()).unwrap(),
        )
        .await;
        assert!(response.status().is_success());
        create(&app, "/parts/s").await;
        gate.open();
        let response = pending.await.unwrap().unwrap();
        let expected = if sent_token {
            StatusCode::PRECONDITION_FAILED
        } else {
            StatusCode::BAD_REQUEST
        };
        assert_eq!(response.status(), expected, "token sent: {sent_token}");
        assert_eq!(latest_snapshot(&app, "/parts/s").await, None);
    }
}

/// m4: a manifest longer than any valid one is refused with `400`.
#[tokio::test]
async fn an_oversized_manifest_is_refused() {
    let (app, _cold_store) = cold_app();
    create(&app, "/parts/s").await;
    let response = send(
        &app,
        Request::put("/parts/s/snapshot/3")
            .header(CONTENT_TYPE, OCTET)
            .header(names::STREAM_SNAPSHOT_PARTS, "1")
            .header(names::STREAM_SNAPSHOT_DIGEST, digest_of(b"x").to_string())
            .body(Body::from(vec![
                b'a';
                ursula_headers::MAX_MANIFEST_BYTES + 1
            ]))
            .unwrap(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

/// m2: a commit beyond the tail, or below the retained offset, is answered
/// from its key alone, as its first answer was. Below retention the latest
/// snapshot always lies at or above the commit's offset (retention never
/// passes it), so the answer is the superseding `409`, before `410`.
#[tokio::test]
async fn out_of_range_commits_answer_from_their_key() {
    let (app, _cold_store) = cold_app();
    create(&app, "/parts/s").await;
    let unknown = vec![PartId::new(format!("{:032x}-{:016x}.part", 1, 2)).unwrap()];
    let response = send(
        &app,
        commit_request_at("/parts/s", 9, &unknown, digest_of(b"x")),
    )
    .await;
    assert_eq!(response.status(), StatusCode::CONFLICT, "beyond the tail");
    let response = send(
        &app,
        Request::put("/parts/s/snapshot/3")
            .header(CONTENT_TYPE, OCTET)
            .body(Body::from("three"))
            .unwrap(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    let response = send(
        &app,
        Request::put("/parts/s/retention/3")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    let response = send(
        &app,
        commit_request_at("/parts/s", 1, &unknown, digest_of(b"x")),
    )
    .await;
    assert_eq!(
        response.status(),
        StatusCode::CONFLICT,
        "superseded: 409 before 410"
    );
}

/// A node whose streams hold at most `parts` staged parts and `bytes` of
/// them, and lists again at every part it would refuse.
fn bounded_state(parts: u64, bytes: u64) -> (HttpState, Arc<ColdStore>) {
    let (mut state, cold_store) = cold_state();
    state.set_staged_parts(crate::staged_parts::StagedParts::for_test(parts, bytes));
    (state, cold_store)
}

/// A part whose stream is at its limit, sent with `Expect: 100-continue`
/// and a body that panics if the node polls it: the refusal is decided
/// from the head. Returns the limit the `429` names.
async fn refused_part(app: &Router, uri: &str, len: usize) -> ursula_headers::StagedPartsLimit {
    let body = Body::from_stream(futures_util::stream::poll_fn(
        |_cx| -> std::task::Poll<Option<Result<Bytes, std::convert::Infallible>>> {
            panic!("the node polled the body of a refused part")
        },
    ));
    let response = send(
        app,
        Request::post(format!("{uri}/snapshot/3/parts"))
            .header(CONTENT_LENGTH, len)
            .header("expect", "100-continue")
            .body(body)
            .unwrap(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
    assert!(response.headers().get(RETRY_AFTER).is_none());
    assert!(
        response.headers().get(names::STREAM_INCARNATION).is_some(),
        "the 429 names the incarnation whose parts fill the bound"
    );
    let limit = ursula_headers::StagedPartsLimit::parse(429, pairs(&response))
        .expect("typed")
        .expect("a staged-parts limit");
    assert_eq!(
        crate::render::error_code_of(response).await,
        ursula_headers::ErrorCode::StagedPartsLimit
    );
    limit
}

/// #622: a stream holds a bounded number of staged parts and bytes. The
/// part past either limit is refused with `429` naming it before its body
/// is read, and another stream is not affected.
#[tokio::test]
async fn staged_parts_are_bounded_before_the_body() {
    let (state, _cold_store) = bounded_state(2, 100);
    let app = router_with_http_state(state.clone());
    create(&app, "/parts/s").await;
    create(&app, "/parts/other").await;
    stage(&app, "/parts/s", vec![1; 10]).await;
    stage(&app, "/parts/s", vec![2; 10]).await;
    assert_eq!(
        refused_part(&app, "/parts/s", 1).await,
        ursula_headers::StagedPartsLimit::Parts(2)
    );
    assert_eq!(
        state
            .http_metrics
            .snapshot_parts_staged_limit
            .load(Ordering::Relaxed),
        1
    );

    // Bytes: 60 staged, and a part that declares 50 more.
    stage(&app, "/parts/other", vec![3; 60]).await;
    assert_eq!(
        refused_part(&app, "/parts/other", 50).await,
        ursula_headers::StagedPartsLimit::Bytes(100)
    );
    stage(&app, "/parts/other", vec![4; 40]).await;
}

/// #622: a commit releases the parts it spent, and the orphan sweep the
/// parts it reclaimed, so the stream stages again.
#[tokio::test]
async fn a_commit_and_the_sweep_release_staged_parts() {
    let (state, cold_store) = bounded_state(2, 1_000);
    let app = router_with_http_state(state.clone());
    let mut ended = state.snapshot_commits.ended();
    create(&app, "/parts/s").await;
    let parts = vec![
        stage(&app, "/parts/s", b"first".to_vec()).await,
        stage(&app, "/parts/s", b"second".to_vec()).await,
    ];
    refused_part(&app, "/parts/s", 1).await;
    let response = send(
        &app,
        commit_request("/parts/s", &parts, digest_of(b"firstsecond")),
    )
    .await;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    ended.wait_for(|ended| *ended >= 1).await.unwrap();
    assert!(part_names(&cold_store).await.is_empty());

    stage(&app, "/parts/s", b"third".to_vec()).await;
    stage(&app, "/parts/s", b"fourth".to_vec()).await;
    refused_part(&app, "/parts/s", 1).await;
    tokio::time::sleep(Duration::from_millis(2)).await;
    let swept = state
        .runtime
        .sweep_cold_orphans_group_once(ursula_shard::RaftGroupId(0), 16, 0)
        .await
        .expect("sweep");
    assert_eq!(swept.orphans_deleted, 2);
    assert_eq!(
        state
            .staged_parts
            .counted(&BucketStreamId::new("parts", "s")),
        Some((0, 0)),
        "the sweep told the tracker"
    );
    stage(&app, "/parts/s", b"fifth".to_vec()).await;
}

/// #622: a new leader starts with no count. Its first part lists the
/// stream's parts, so it counts the parts the old leader staged, and refuses
/// the part past the limit. Two nodes share the stream and the cold store
/// here: the second stands for the node that leads after a leader change.
#[tokio::test]
async fn a_new_leader_counts_the_parts_the_old_one_staged() {
    let (old_leader, _cold_store) = bounded_state(2, 1_000);
    let mut new_leader = HttpState::new(old_leader.runtime.clone());
    new_leader.set_staged_parts(crate::staged_parts::StagedParts::for_test(2, 1_000));
    let old_app = router_with_http_state(old_leader);
    let new_app = router_with_http_state(new_leader);
    create(&old_app, "/parts/s").await;
    stage(&old_app, "/parts/s", b"first".to_vec()).await;
    stage(&new_app, "/parts/s", b"second".to_vec()).await;
    assert_eq!(
        refused_part(&new_app, "/parts/s", 1).await,
        ursula_headers::StagedPartsLimit::Parts(2)
    );
}

/// B1 of the #622 review: two commits that name the same parts under
/// different wrong digests. The second one, sent while the first copies,
/// answers `503 SnapshotCommitInProgress` and spends nothing, so the parts
/// the first one consumed are given back once, from a listing.
#[tokio::test]
async fn a_second_commit_of_a_stream_waits_for_the_first() {
    let (state, cold_store) = bounded_state(4, 1_000);
    let app = router_with_http_state(state.clone());
    let mut ended = state.snapshot_commits.ended();
    create(&app, "/parts/s").await;
    let parts = vec![
        stage(&app, "/parts/s", b"first".to_vec()).await,
        stage(&app, "/parts/s", b"second".to_vec()).await,
    ];
    stage(&app, "/parts/s", b"kept".to_vec()).await;
    let gate = Gate::on_part_reads(&cold_store);
    let first = tokio::spawn({
        let (app, parts) = (app.clone(), parts.clone());
        async move {
            send(
                &app,
                commit_request("/parts/s", &parts, digest_of(b"wrong")),
            )
            .await
        }
    });
    gate.reached(1).await;
    let response = send(
        &app,
        commit_request("/parts/s", &parts, digest_of(b"also wrong")),
    )
    .await;
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert!(response.headers().get(RETRY_AFTER).is_some());
    assert_eq!(
        crate::render::error_code_of(response).await,
        ursula_headers::ErrorCode::SnapshotCommitInProgress
    );
    gate.open();
    let response = first.await.expect("first commit");
    assert_eq!(
        response.status(),
        StatusCode::BAD_REQUEST,
        "a digest mismatch"
    );
    ended.wait_for(|ended| *ended >= 1).await.unwrap();
    assert_eq!(
        part_names(&cold_store).await.len(),
        1,
        "the mismatch used both"
    );
    assert_eq!(
        state
            .staged_parts
            .counted(&BucketStreamId::new("parts", "s")),
        Some((1, 4)),
        "the count is the listing's: the part kept"
    );
}

/// #622 review M5: `DELETE {stream}/snapshot/parts` discards the stream's
/// staged parts, so a stream at its bound stages again. It honors a
/// `Stream-Incarnation` precondition and answers with the token.
#[tokio::test]
async fn a_client_discards_its_staged_parts() {
    let (state, cold_store) = bounded_state(2, 1_000);
    let app = router_with_http_state(state);
    create(&app, "/parts/s").await;
    create(&app, "/parts/other").await;
    stage(&app, "/parts/s", b"first".to_vec()).await;
    stage(&app, "/parts/s", b"second".to_vec()).await;
    stage(&app, "/parts/other", b"other".to_vec()).await;
    refused_part(&app, "/parts/s", 1).await;
    let response = send(&app, Request::head("/parts/s").body(Body::empty()).unwrap()).await;
    let token = StreamHead::parse(pairs(&response))
        .expect("head")
        .incarnation();

    let response = send(
        &app,
        Request::delete("/parts/s/snapshot/parts")
            .header(names::STREAM_INCARNATION, (token.get() ^ 1).to_string())
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::PRECONDITION_FAILED);
    assert_eq!(part_names(&cold_store).await.len(), 3);

    let response = send(
        &app,
        Request::delete("/parts/s/snapshot/parts")
            .header(names::STREAM_INCARNATION, token.to_string())
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    assert_eq!(
        response.headers()[names::STREAM_INCARNATION],
        token.to_string().as_str()
    );
    assert_eq!(part_names(&cold_store).await.len(), 1, "the other stream's");
    stage(&app, "/parts/s", b"again".to_vec()).await;

    let response = send(
        &app,
        Request::delete("/parts/missing/snapshot/parts")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

/// #622 reviews: a delete of a stream answers first, then deletes the
/// parts of the incarnation it removed, at any age, in a task the node owns,
/// so a recreate starts with none.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn deleting_a_stream_deletes_its_staged_parts_after_the_answer() {
    let (state, cold_store) = bounded_state(2, 1_000);
    let app = router_with_http_state(state.clone());
    let mut cleaned = state.staged_parts.cleaned();
    create(&app, "/parts/s").await;
    stage(&app, "/parts/s", b"first".to_vec()).await;
    stage(&app, "/parts/s", b"second".to_vec()).await;
    let gate = crate::staged_parts::tests::ListingGate::on(&cold_store);
    let response = send(
        &app,
        Request::delete("/parts/s").body(Body::empty()).unwrap(),
    )
    .await;
    assert_eq!(
        response.status(),
        StatusCode::NO_CONTENT,
        "before any listing"
    );
    gate.reached(1).await;
    assert_eq!(part_names(&cold_store).await.len(), 2);
    gate.open();
    cleaned.wait_for(|cleaned| *cleaned >= 1).await.unwrap();
    assert!(part_names(&cold_store).await.is_empty());
    create(&app, "/parts/s").await;
    stage(&app, "/parts/s", b"new".to_vec()).await;
    stage(&app, "/parts/s", b"newer".to_vec()).await;

    // A stream this node counts no parts of is deleted without a cleanup.
    create(&app, "/parts/plain").await;
    let response = send(
        &app,
        Request::delete("/parts/plain").body(Body::empty()).unwrap(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    assert!(
        !state
            .staged_parts
            .tracks(&BucketStreamId::new("parts", "plain"))
            .1
    );
}

/// The node's clock, which a test sets.
struct SetClock(Arc<AtomicU64>);

impl WallClock for SetClock {
    fn unix_time_ms(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }
}

/// A delete of a stream that expired answers `404`, as for a missing
/// stream, but it removed the stream like any delete, so it deletes the
/// stream's staged parts after the answer too.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn deleting_an_expired_stream_deletes_its_staged_parts() {
    let now_ms = Arc::new(AtomicU64::new(1_000));
    let (state, cold_store) = bounded_state(2, 1_000);
    let state = state.with_wall_clock(SetClock(Arc::clone(&now_ms)));
    let app = router_with_http_state(state.clone());
    let mut cleaned = state.staged_parts.cleaned();
    let response = send(
        &app,
        Request::put("/parts/s")
            .header(CONTENT_TYPE, OCTET)
            .header(HEADER_STREAM_TTL, "1")
            .body(Body::from("abc"))
            .expect("request"),
    )
    .await;
    assert_eq!(response.status(), StatusCode::CREATED);
    stage(&app, "/parts/s", b"first".to_vec()).await;
    assert_eq!(part_names(&cold_store).await.len(), 1);

    now_ms.store(2_000, Ordering::Relaxed);
    let response = send(
        &app,
        Request::delete("/parts/s").body(Body::empty()).unwrap(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    cleaned.wait_for(|cleaned| *cleaned >= 1).await.unwrap();
    assert!(part_names(&cold_store).await.is_empty());
}

/// M4 of the #622 re-review: a discard whose delete fails answers `502
/// COLD_STORE_FAILED`, never `204`.
#[tokio::test]
async fn a_discard_whose_delete_fails_answers_502() {
    let (state, cold_store) = bounded_state(2, 1_000);
    let app = router_with_http_state(state);
    create(&app, "/parts/s").await;
    stage(&app, "/parts/s", b"first".to_vec()).await;
    cold_store.set_fault_policy(|context| {
        (context.operation == ursula_runtime::ColdStoreOperation::DeleteChunk
            && context.path.contains("/_parts/"))
        .then(|| ursula_runtime::ColdStoreFaultEffect::fail("injected: delete refused"))
    });
    let response = send(
        &app,
        Request::delete("/parts/s/snapshot/parts")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
    assert_eq!(
        crate::render::error_code_of(response).await,
        ursula_headers::ErrorCode::ColdStoreFailed
    );
    assert_eq!(part_names(&cold_store).await.len(), 1);
}

async fn usage_report(app: &Router) -> ursula_proto::admin::UsageReport {
    let response = send(
        app,
        Request::get("/__ursula/usage").body(Body::empty()).unwrap(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    serde_json::from_slice(&body_of(response).await).expect("typed usage")
}

/// #639: `/__ursula/usage` reports, per group this node leads, the bytes of
/// each bucket's staged parts, seeded from a listing of the group's part
/// directories, so the parts staged before this node started to lead count
/// too. `/__ursula/metrics` reports their sum. Until the listing finishes the
/// group says its counting has not finished.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn usage_reports_staged_part_bytes_per_group() {
    let (state, cold_store) = bounded_state(10, 1_000);
    let app = router_with_http_state(state.clone());
    create(&app, "/parts/s").await;
    create(&app, "/parts/abandoned").await;
    let response = send(
        &app,
        Request::head("/parts/abandoned")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    let token = StreamHead::parse(pairs(&response))
        .expect("head")
        .incarnation()
        .get();
    // Staged under an earlier leader, and never touched since.
    cold_store
        .write_chunk(
            &ursula_runtime::snapshot_part_path(
                &BucketStreamId::new("parts", "abandoned"),
                0,
                token,
                &ursula_runtime::new_snapshot_part_id(),
            ),
            &[0; 9],
        )
        .await
        .expect("an abandoned part");
    assert!(usage_report(&app).await.staged_snapshot_parts.is_empty());

    let gate = crate::staged_parts::tests::ListingGate::on(&cold_store);
    let counting = tokio::spawn({
        let (state, cold_store) = (state.clone(), cold_store.clone());
        async move {
            let cold_store: ursula_runtime::ColdStoreHandle = cold_store;
            state.count_staged_parts_once(&cold_store).await;
        }
    });
    gate.reached(1).await;
    let report = usage_report(&app).await;
    assert_eq!(report.staged_snapshot_parts.len(), 1);
    assert!(!report.staged_snapshot_parts[0].counting_finished);
    gate.open();
    counting.await.expect("counting");
    cold_store.clear_fault_policy();

    stage(&app, "/parts/s", vec![1; 7]).await;
    stage(&app, "/parts/s", vec![2; 5]).await;
    let report = usage_report(&app).await;
    assert_eq!(report.node_id, None, "a node without an id names none");
    assert_eq!(report.buckets["parts"].stream_count, 2);
    let group = &report.staged_snapshot_parts[0];
    assert_eq!((group.raft_group_id, group.term), (0, 0));
    assert!(group.counting_finished);
    assert_eq!(group.bytes.get("parts"), Some(&21));
    let response = send(
        &app,
        Request::get("/__ursula/metrics")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    let metrics: serde_json::Value =
        serde_json::from_slice(&body_of(response).await).expect("metrics");
    assert_eq!(metrics["staged_snapshot_part_bytes"], 21);
}

/// #648 review: reading usage changes nothing a part or a count depends on,
/// a deleted stream's leftover parts and an emptied bucket included.
#[tokio::test]
async fn reading_usage_changes_no_count() {
    let (state, cold_store) = bounded_state(10, 1_000);
    let app = router_with_http_state(state.clone());
    let mut cleaned = state.staged_parts.cleaned();
    let cold_store: ursula_runtime::ColdStoreHandle = cold_store;
    state.count_staged_parts_once(&cold_store).await;
    create(&app, "/parts/s").await;
    create(&app, "/emptied/s").await;
    stage(&app, "/parts/s", vec![1; 7]).await;
    stage(&app, "/emptied/s", vec![2; 5]).await;
    let response = send(
        &app,
        Request::delete("/emptied/s").body(Body::empty()).unwrap(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    cleaned.wait_for(|cleaned| *cleaned >= 1).await.unwrap();
    let streams = [
        BucketStreamId::new("parts", "s"),
        BucketStreamId::new("emptied", "s"),
    ];
    let observe = || {
        streams
            .iter()
            .map(|stream_id| {
                (
                    state.staged_parts.tracks(stream_id),
                    state.staged_parts.counted(stream_id),
                )
            })
            .collect::<Vec<_>>()
    };
    let before = (observe(), state.staged_part_report());
    let first = usage_report(&app).await;
    let second = usage_report(&app).await;
    assert_eq!(first, second);
    assert_eq!((observe(), state.staged_part_report()), before);
}

/// #622 review m1 and m7: the term comes from the node's Raft leadership.
/// A single-node Raft group that elects itself again in a later term lists
/// the stream's parts again, so it counts a part another leader staged in
/// between, here written behind the node's back.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_new_raft_term_lists_the_stream_again() {
    let raft_root = std::env::temp_dir().join(format!(
        "ursula-staged-parts-term-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("after the epoch")
            .as_nanos()
    ));
    crate::tests::remove_test_path(&raft_root);
    let mut config = ursula_config::UrsulaConfig::default();
    config.runtime.core_count = 1;
    config.raft.group_count = 1;
    config.storage.cold.backend = ursula_config::ColdBackend::Memory;
    let spawned = spawn_runtime(
        &config,
        Persistence::Raft {
            log_dir: raft_root.as_path().into(),
        },
        Topology::static_cluster(
            1,
            crate::shared_peers([(1, "http://127.0.0.1:4477".to_owned())]),
            1,
            true,
            Default::default(),
        )
        .expect("topology"),
    )
    .expect("runtime");
    let runtime = spawned.runtime;
    let registry = spawned.raft_registry.expect("registry");
    runtime.warm_all_groups().await.expect("warm");
    let raft = registry.get(RaftGroupId(0)).expect("group 0");
    raft.wait(Some(Duration::from_secs(5)))
        .current_leader(1, "node 1 leads")
        .await
        .expect("leader");
    let cold_store = runtime.cold_store().expect("a memory cold store");
    let mut state = HttpState::with_raft_registry(runtime, registry.clone());
    state.set_staged_parts(crate::staged_parts::StagedParts::for_test(3, 1_000));
    let app = router_with_http_state(state.clone());
    create(&app, "/parts/s").await;
    let response = send(&app, Request::head("/parts/s").body(Body::empty()).unwrap()).await;
    let token = StreamHead::parse(pairs(&response))
        .expect("head")
        .incarnation()
        .get();
    stage(&app, "/parts/s", b"first".to_vec()).await;
    cold_store
        .write_chunk(
            &ursula_runtime::snapshot_part_path(
                &BucketStreamId::new("parts", "s"),
                0,
                token,
                &ursula_runtime::new_snapshot_part_id(),
            ),
            b"elsewhere",
        )
        .await
        .expect("a part staged by another leader");

    let counting = state.spawn_staged_part_counting().expect("a cold store");
    let term = registry
        .leadership(RaftGroupId(0))
        .expect("leadership")
        .term;
    // An election on a leader keeps its leader state (#721): step down to a
    // node outside the single-voter membership first, as production does.
    let stepped_down = raft
        .step_down_and_campaign(2)
        .await
        .expect("step down and campaign");
    assert!(matches!(stepped_down, ursula_raft::StepDown::Campaigned));
    raft.wait(Some(Duration::from_secs(5)))
        .metrics(
            |metrics| metrics.current_term > term && metrics.state == openraft::ServerState::Leader,
            "node 1 leads a later term",
        )
        .await
        .expect("a later term");
    // The new term's counting seeds the group's totals from a listing.
    let new_term = registry
        .leadership(RaftGroupId(0))
        .expect("leadership")
        .term;
    // The counting task, woken by the group's Raft metrics, seeds it.
    let groups = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let groups = state.staged_part_report();
            if groups
                .first()
                .is_some_and(|group| group.term == new_term && group.counting_finished)
            {
                return groups;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the counting task seeds the new term");
    drop(counting);
    assert_eq!(groups.len(), 1);
    assert_eq!(groups[0].term, new_term);
    assert!(groups[0].counting_finished);
    assert_eq!(
        groups[0].buckets.get("parts"),
        Some(&(5 + 9)),
        "the part staged here and the one staged elsewhere"
    );
    stage(&app, "/parts/s", b"third".to_vec()).await;
    assert_eq!(
        refused_part(&app, "/parts/s", 1).await,
        ursula_headers::StagedPartsLimit::Parts(3),
        "the new term counted the part staged elsewhere"
    );
    crate::tests::remove_test_path(&raft_root);
}

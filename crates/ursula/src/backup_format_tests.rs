//! Backup format 4 ([`ursula_runtime::BACKUP_FORMAT_VERSION`]) across
//! releases: a backup that v0.8.2 wrote (its `ursulactl backup-create`
//! objects and the cold objects they reference, in `fixtures/backup-format-4`)
//! verifies, re-encodes byte for byte and restores through `ursulactl
//! restore`. A change to what a backup holds fails here until it also moves
//! `BACKUP_FORMAT_VERSION` and drops this fixture.
//!
//! The source cluster had one group. `tenant-a/orders` holds JSON messages,
//! an app snapshot at 18 and a retention floor at 18, and was written again
//! after an import, so it inherited a history. `tenant-a/cold` was flushed to
//! a cold chunk and an index page. `tenant-a/external` holds an external
//! payload and a snapshot body in the cold tier. `tenant-b/journal` is closed.
//! `tenant-a/keyed` has a producer whose epoch 0 a claim of epoch 1 closed.
//! `tenant-a/ttl` and `tenant-a/expiring` have a TTL and an expiry time.
//! `tenant-c` was purged, and `tenant-a/gone` was deleted after a cold flush.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::body::Bytes;
use axum::body::to_bytes;
use axum::http::Request;
use axum::http::StatusCode;
use axum::http::header::CONTENT_TYPE;
use axum::response::Response;
use tower::ServiceExt;
use ursula_ctl::backup::BackupClient;
use ursula_ctl::backup::BackupError;
use ursula_ctl::backup::BackupManifest;
use ursula_ctl::backup::BackupStore;
use ursula_headers::ClosedBy;
use ursula_headers::ProducerAnswer;
use ursula_headers::StreamHead;
use ursula_headers::names;
use ursula_runtime::BACKUP_FORMAT_VERSION;
use ursula_runtime::ColdStore;
use ursula_runtime::ColdStoreHandle;
use ursula_runtime::InMemoryGroupEngineFactory;
use ursula_runtime::RuntimeConfig;
use ursula_runtime::ShardRuntime;
use ursula_runtime::StreamSnapshot;

use crate::HttpState;
use crate::router_with_http_state;

const FIXTURE: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/src/fixtures/backup-format-4");

fn fixture(name: &str) -> Vec<u8> {
    std::fs::read(format!("{FIXTURE}/{name}")).expect("fixture file")
}

fn fixture_store() -> BackupStore {
    BackupStore::open(FIXTURE).expect("open the fixture backup")
}

/// A one-group cluster of this release on `cold_store`.
fn cluster(cold_store: ColdStoreHandle) -> Router {
    let runtime = ShardRuntime::spawn_with_engine_factory_and_cold_store(
        RuntimeConfig::new(1, 1),
        InMemoryGroupEngineFactory::with_cold_store(Some(cold_store.clone())),
        Some(cold_store),
    )
    .expect("runtime");
    router_with_http_state(HttpState::new(runtime))
}

/// A cold store holding every cold object of the source cluster.
async fn copied_cold_store() -> ColdStoreHandle {
    let cold_store: ColdStoreHandle = Arc::new(ColdStore::memory().expect("memory cold store"));
    let objects: BTreeMap<String, Vec<u8>> =
        serde_json::from_slice(&fixture("cold-objects.json")).expect("cold objects JSON");
    assert!(!objects.is_empty());
    for (key, bytes) in &objects {
        cold_store
            .write_chunk(key, bytes)
            .await
            .expect("copy a cold object");
    }
    cold_store
}

/// Serves `app` on TCP for `ursulactl` and returns its client.
async fn serve(app: &Router) -> (BackupClient, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("local addr");
    let served = app.clone();
    let server = tokio::spawn(async move {
        axum::serve(listener, served).await.expect("serve");
    });
    let client = BackupClient::new(
        ursula_ctl::MetricsClient::new(Duration::from_secs(10)).expect("metrics client"),
        vec![ursula_ctl::NodeInfo {
            id: 1,
            host: addr.to_string(),
            admin_url: format!("http://{addr}").parse().expect("admin url"),
            http_url: None,
            metrics_url: None,
            client_url: None,
            expected_process_incarnation: None,
            expected_maintenance_fence: None,
        }],
    )
    .expect("backup client");
    (client, server)
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

async fn get(app: &Router, uri: &str) -> Response {
    send(app, "GET", uri, &[], Body::empty()).await
}

async fn body_bytes(response: Response) -> Bytes {
    to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body")
}

fn header<'a>(response: &'a Response, name: &str) -> &'a str {
    response
        .headers()
        .get(name)
        .unwrap_or_else(|| panic!("missing header {name}"))
        .to_str()
        .expect("utf-8 header")
}

fn pairs(response: &Response) -> impl Iterator<Item = (&str, &[u8])> {
    response
        .headers()
        .iter()
        .map(|(name, value)| (name.as_str(), value.as_bytes()))
}

/// Reads a byte stream from offset 0 until it holds `len` bytes, following
/// `Stream-Next-Offset` across its cold and hot parts.
async fn read_bytes(app: &Router, stream: &str, len: usize) -> Vec<u8> {
    let mut bytes = Vec::new();
    for _ in 0..8 {
        if bytes.len() >= len {
            break;
        }
        let response = get(app, &format!("{stream}?offset={}", bytes.len())).await;
        assert_eq!(response.status(), StatusCode::OK, "{stream}");
        bytes.extend_from_slice(&body_bytes(response).await);
    }
    bytes
}

/// The app snapshot a client finds through HEAD.
async fn latest_snapshot(app: &Router, stream: &str) -> Bytes {
    let response = send(app, "HEAD", stream, &[], Body::empty()).await;
    assert_eq!(response.status(), StatusCode::OK, "{stream}");
    let offset = header(&response, names::STREAM_SNAPSHOT_OFFSET).to_owned();
    let response = get(app, &format!("{stream}/snapshot/{offset}")).await;
    assert_eq!(response.status(), StatusCode::OK, "{stream}");
    body_bytes(response).await
}

async fn keyed_write(app: &Router, epoch: &str, seq: &str, body: &'static str) -> Response {
    let digest = blake3::hash(body.as_bytes()).to_hex().to_string();
    send(
        app,
        "POST",
        "/tenant-a/keyed",
        &[
            (CONTENT_TYPE.as_str(), "text/plain"),
            (names::PRODUCER_ID, "writer"),
            (names::PRODUCER_EPOCH, epoch),
            (names::PRODUCER_SEQ, seq),
            (names::PRODUCER_DIGEST, digest.as_str()),
        ],
        Body::from(body),
    )
    .await
}

#[tokio::test]
async fn the_backup_format_is_still_0_8s() {
    assert_eq!(BACKUP_FORMAT_VERSION, 4);
    let manifest: BackupManifest =
        serde_json::from_slice(&fixture("manifest.json")).expect("a 0.8.2 manifest decodes");
    assert_eq!(manifest.format_version, BACKUP_FORMAT_VERSION);

    let group = fixture("group-0000.snapshot");
    let snapshot: StreamSnapshot =
        rmp_serde::from_slice(&group).expect("a 0.8.2 group object decodes");
    assert_eq!(snapshot.format_version, BACKUP_FORMAT_VERSION);
    assert_eq!(
        rmp_serde::to_vec_named(&snapshot).expect("encode"),
        group,
        "a 0.8.2 group object re-encodes byte for byte"
    );
    assert!(
        snapshot
            .history
            .as_ref()
            .is_some_and(|history| history.imported.is_some())
    );
    assert_eq!(snapshot.erased_buckets, vec!["tenant-c".to_owned()]);
    assert!(!snapshot.pending_cold_gc.is_empty());

    let report = ursula_ctl::backup::verify(&fixture_store())
        .await
        .expect("a 0.8.2 backup verifies");
    assert_eq!((report.groups, report.buckets, report.streams), (1, 2, 7));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_0_8_2_backup_restores_through_ursulactl() {
    let store = fixture_store();

    // A target without the source's cold objects is refused before the first
    // import.
    let empty = cluster(Arc::new(ColdStore::memory().expect("memory cold store")));
    let (client, server) = serve(&empty).await;
    let err = ursula_ctl::backup::restore(&client, &store)
        .await
        .expect_err("the cold objects are missing");
    assert!(
        matches!(
            &err,
            BackupError::ColdObjectsMissing { missing, referenced, .. }
                if missing == referenced && *missing > 0
        ),
        "{err}"
    );
    server.abort();

    let app = cluster(copied_cold_store().await);
    let (client, server) = serve(&app).await;
    let report = ursula_ctl::backup::restore(&client, &store)
        .await
        .expect("a 0.8.2 backup restores");
    assert_eq!(report.groups, 1);
    assert_eq!((report.buckets, report.streams), (2, 7));
    assert!(report.cold_objects > 0, "{report:?}");
    assert_eq!(report.imports.len(), 1);
    assert!(!report.imports[0].already);

    // Retention floor, messages on both sides of the import, app snapshot.
    let response = get(&app, "/tenant-a/orders?offset=0").await;
    assert_eq!(response.status(), StatusCode::GONE);
    let response = get(&app, "/tenant-a/orders?offset=18").await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = body_bytes(response).await;
    let body = std::str::from_utf8(&body).expect("utf8");
    assert!(
        body.contains(r#"{"id":3}"#) && body.contains(r#"{"id":4}"#),
        "{body}"
    );
    assert_eq!(
        &latest_snapshot(&app, "/tenant-a/orders").await[..],
        br#"{"count":2}"#
    );
    let response = send(&app, "HEAD", "/tenant-a/orders", &[], Body::empty()).await;
    let head = StreamHead::parse(pairs(&response)).expect("a stream head");
    assert!(head.inherited().is_some(), "{head:?}");
    assert!(
        response
            .headers()
            .contains_key(names::STREAM_PREVIOUS_INCARNATION)
    );

    // 0.9 reads the cold chunk and index page, the external payload and the
    // snapshot body that 0.8.2 wrote.
    assert_eq!(read_bytes(&app, "/tenant-a/cold", 6).await, b"abcdef");
    assert_eq!(
        read_bytes(&app, "/tenant-a/external", 40).await,
        b"external-payload-0123456789-abcdefghijkl"
    );
    assert_eq!(
        &latest_snapshot(&app, "/tenant-a/external").await[..],
        b"snapshot-body-0123456789-abcdefg"
    );

    // The closed stream stays closed.
    let response = get(&app, "/tenant-b/journal?offset=0&max_bytes=100").await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(header(&response, names::STREAM_CLOSED), "true");
    assert_eq!(&body_bytes(response).await[..], b"raw-payload");
    let response = send(
        &app,
        "POST",
        "/tenant-b/journal",
        &[(CONTENT_TYPE.as_str(), "text/plain")],
        Body::from("more"),
    )
    .await;
    assert_eq!(response.status(), StatusCode::CONFLICT);

    // The producer's newest write is deduplicated, and its epoch is closed
    // by the restore's history.
    let response = keyed_write(&app, "1", "0", "k2").await;
    assert!(response.status().is_success(), "{:?}", response.status());
    assert!(matches!(
        ProducerAnswer::parse(response.status().as_u16(), pairs(&response))
            .expect("a well-formed producer answer"),
        Some(ProducerAnswer::Receipt { .. })
    ));
    let response = keyed_write(&app, "1", "1", "k3").await;
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    match ProducerAnswer::parse(response.status().as_u16(), pairs(&response))
        .expect("a well-formed producer answer")
    {
        Some(ProducerAnswer::EpochClosed(closure)) => {
            assert_eq!(closure.by(), ClosedBy::History);
        }
        other => panic!("expected epoch-closed;by=history, got {other:?}"),
    }

    // Lifetimes, the erased bucket and the deleted stream.
    for stream in ["/tenant-a/ttl", "/tenant-a/expiring"] {
        let response = send(&app, "HEAD", stream, &[], Body::empty()).await;
        assert_eq!(response.status(), StatusCode::OK, "{stream}");
    }
    let response = send(
        &app,
        "PUT",
        "/tenant-c/again",
        &[(CONTENT_TYPE.as_str(), "text/plain")],
        Body::empty(),
    )
    .await;
    assert!(
        response.status().is_client_error(),
        "an erased bucket refuses writes: {:?}",
        response.status()
    );
    let response = get(&app, "/tenant-a/gone?offset=0").await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    // A second run finds the import applied.
    let again = ursula_ctl::backup::restore(&client, &store)
        .await
        .expect("a repeated restore");
    assert_eq!(again.already(), 1);
    server.abort();
}

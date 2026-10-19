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
//! The TTL (3,000,000,000 s) and the expiry time (2100-01-01) are deliberate:
//! the two streams must stay live whenever this test runs, so do not shorten
//! them.
//!
//! The fixture was generated at tag `v0.8.2` on CI by an ignored test in
//! `crates/ursula/src/tests.rs`, commits `f9c3558d` and `789048bb` (kept on
//! the branch `fixture-gen/backup-format-4`), in the runs `39096527009` and
//! `39096752584` (the second wrote these files).
//!
//! [`a_backup_round_trips_through_ursulactl`] writes the same streams on a
//! cluster of this release and takes them through `ursulactl backup-create`,
//! `backup-verify` and `restore`, so a backup of this release restores
//! whatever its format.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::body::Bytes;
use axum::body::to_bytes;
use axum::http::StatusCode;
use axum::http::header::CONTENT_TYPE;
use axum::response::Response;
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
use crate::tests::send;

const FIXTURE: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/src/fixtures/backup-format-4");

fn fixture(name: &str) -> Vec<u8> {
    std::fs::read(format!("{FIXTURE}/{name}")).expect("fixture file")
}

fn fixture_store() -> BackupStore {
    BackupStore::open(FIXTURE).expect("open the fixture backup")
}

/// A one-group cluster of this release on `cold_store`: node 1, which
/// `ursulactl` finds by its metrics.
fn cluster(cold_store: ColdStoreHandle) -> Router {
    let runtime = ShardRuntime::spawn_with_engine_factory_and_cold_store(
        RuntimeConfig::new(1, 1),
        InMemoryGroupEngineFactory::with_cold_store(Some(cold_store.clone())),
        Some(cold_store),
    )
    .expect("runtime");
    router_with_http_state(HttpState::new(runtime).with_configured_node_id(1))
}

/// A one-group cluster of this release on `cold_store` that stages bodies of
/// 16 bytes or more in the cold tier, so a small stream holds an external
/// payload and a snapshot body there.
fn staging_cluster(cold_store: ColdStoreHandle) -> Router {
    let runtime = ShardRuntime::spawn_with_engine_factory_and_cold_store(
        RuntimeConfig::new(1, 1),
        InMemoryGroupEngineFactory::with_cold_store(Some(cold_store.clone())),
        Some(cold_store),
    )
    .expect("runtime");
    router_with_http_state(
        HttpState::new(runtime)
            .with_configured_node_id(1)
            .with_external_payload_min_bytes(16),
    )
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

/// What a cluster restored from a backup of the source cluster serves: the
/// streams of the module documentation, with their retention floor, app snapshots,
/// cold and external bytes, closure, producer and lifetimes, the erased
/// bucket and the deleted stream.
async fn assert_restored(app: &Router) {
    // Retention floor, messages on both sides of the import, app snapshot.
    let response = get(app, "/tenant-a/orders?offset=0").await;
    assert_eq!(response.status(), StatusCode::GONE);
    let response = get(app, "/tenant-a/orders?offset=18").await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = body_bytes(response).await;
    let body = std::str::from_utf8(&body).expect("utf8");
    assert!(
        body.contains(r#"{"id":3}"#) && body.contains(r#"{"id":4}"#),
        "{body}"
    );
    assert_eq!(
        &latest_snapshot(app, "/tenant-a/orders").await[..],
        br#"{"count":2}"#
    );
    let response = send(app, "HEAD", "/tenant-a/orders", &[], Body::empty()).await;
    let head = StreamHead::parse(pairs(&response)).expect("a stream head");
    assert!(head.inherited().is_some(), "{head:?}");
    assert!(
        response
            .headers()
            .contains_key(names::STREAM_PREVIOUS_INCARNATION)
    );

    // The restored cluster reads the cold chunk and index page, the external
    // payload and the snapshot body the source cluster wrote.
    assert_eq!(read_bytes(app, "/tenant-a/cold", 6).await, b"abcdef");
    assert_eq!(
        read_bytes(app, "/tenant-a/external", 40).await,
        b"external-payload-0123456789-abcdefghijkl"
    );
    assert_eq!(
        &latest_snapshot(app, "/tenant-a/external").await[..],
        b"snapshot-body-0123456789-abcdefg"
    );

    // The closed stream stays closed.
    let response = get(app, "/tenant-b/journal?offset=0&max_bytes=100").await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(header(&response, names::STREAM_CLOSED), "true");
    assert_eq!(&body_bytes(response).await[..], b"raw-payload");
    let response = send(
        app,
        "POST",
        "/tenant-b/journal",
        &[(CONTENT_TYPE.as_str(), "text/plain")],
        Body::from("more"),
    )
    .await;
    assert_eq!(response.status(), StatusCode::CONFLICT);

    // The producer's newest write is deduplicated, and its epoch is closed
    // by the restore's history.
    let response = keyed_write(app, "1", "0", "k2").await;
    assert!(response.status().is_success(), "{:?}", response.status());
    assert!(matches!(
        ProducerAnswer::parse(response.status().as_u16(), pairs(&response))
            .expect("a well-formed producer answer"),
        Some(ProducerAnswer::Receipt { .. })
    ));
    let response = keyed_write(app, "1", "1", "k3").await;
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
        let response = send(app, "HEAD", stream, &[], Body::empty()).await;
        assert_eq!(response.status(), StatusCode::OK, "{stream}");
    }
    let response = send(
        app,
        "PUT",
        "/tenant-c/again",
        &[(CONTENT_TYPE.as_str(), "text/plain")],
        Body::empty(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    assert_eq!(
        ursula_headers::ErrorBody::parse(&body_bytes(response).await)
            .expect("an error body")
            .code,
        ursula_headers::ErrorCode::BucketErased,
        "an erased bucket refuses writes"
    );
    let response = get(app, "/tenant-a/gone?offset=0").await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
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

    assert_restored(&app).await;

    // A second run finds the import applied.
    let again = ursula_ctl::backup::restore(&client, &store)
        .await
        .expect("a repeated restore");
    assert_eq!(again.already(), 1);
    server.abort();
}

/// `response`'s body, which must be a success.
async fn ok(response: Response, context: &str) -> Bytes {
    let status = response.status();
    let body = body_bytes(response).await;
    assert!(
        status.is_success(),
        "{context}: {status} {}",
        String::from_utf8_lossy(&body)
    );
    body
}

async fn put(app: &Router, uri: &str, headers: &[(&str, &str)], body: &'static str) -> Response {
    send(app, "PUT", uri, headers, Body::from(body)).await
}

/// A cluster of this release that holds the streams of the module
/// documentation in `cold_store`. They are written on a first cluster,
/// whose group is then imported into a second one that shares the cold
/// store, and `tenant-a/orders` is written again there, so it inherited a
/// history. Returns the second cluster.
async fn written_cluster(cold_store: &ColdStoreHandle) -> Router {
    let first = staging_cluster(cold_store.clone());
    let json = [(CONTENT_TYPE.as_str(), "application/json")];
    let text = [(CONTENT_TYPE.as_str(), "text/plain")];
    let octets = [(CONTENT_TYPE.as_str(), "application/octet-stream")];

    // tenant-a/orders: JSON messages, an app snapshot and a retention floor
    // at 18.
    ok(
        put(&first, "/tenant-a/orders", &json, r#"[{"id":1},{"id":2}]"#).await,
        "orders create",
    )
    .await;
    ok(
        send(
            &first,
            "POST",
            "/tenant-a/orders",
            &json,
            Body::from(r#"{"id":3}"#),
        )
        .await,
        "orders append",
    )
    .await;
    ok(
        put(
            &first,
            "/tenant-a/orders/snapshot/00000000000000000018",
            &json,
            r#"{"count":2}"#,
        )
        .await,
        "orders snapshot",
    )
    .await;
    ok(
        put(
            &first,
            "/tenant-a/orders/retention/00000000000000000018",
            &[],
            "",
        )
        .await,
        "orders retention",
    )
    .await;

    // tenant-a/cold: flushed to a cold chunk and an index page.
    ok(
        put(&first, "/tenant-a/cold", &text, "abcdef").await,
        "cold create",
    )
    .await;
    ok(
        send(
            &first,
            "POST",
            "/__ursula/flush-cold/tenant-a/cold?min_hot_bytes=4&max_bytes=4",
            &[],
            Body::empty(),
        )
        .await,
        "cold flush",
    )
    .await;

    // tenant-a/external: an external payload and a snapshot body in the
    // cold tier.
    ok(
        put(
            &first,
            "/tenant-a/external",
            &octets,
            "external-payload-0123456789-abcdefghijkl",
        )
        .await,
        "external create",
    )
    .await;
    ok(
        put(
            &first,
            "/tenant-a/external/snapshot/00000000000000000040",
            &octets,
            "snapshot-body-0123456789-abcdefg",
        )
        .await,
        "external snapshot",
    )
    .await;

    // tenant-b/journal: a closed byte stream.
    ok(
        put(&first, "/tenant-b/journal", &text, "raw-payload").await,
        "journal create",
    )
    .await;
    ok(
        send(
            &first,
            "POST",
            "/tenant-b/journal",
            &[(names::STREAM_CLOSED, "true")],
            Body::empty(),
        )
        .await,
        "journal close",
    )
    .await;

    // tenant-a/keyed: a producer with digests, and a claim that closes
    // epoch 0.
    ok(
        put(&first, "/tenant-a/keyed", &text, "").await,
        "keyed create",
    )
    .await;
    for (epoch, seq, body) in [("0", "0", "k0"), ("0", "1", "k1"), ("1", "0", "k2")] {
        ok(
            keyed_write(&first, epoch, seq, body).await,
            &format!("keyed epoch {epoch} seq {seq}"),
        )
        .await;
    }

    // tenant-a/ttl and tenant-a/expiring: a TTL and an expiry time.
    ok(
        put(
            &first,
            "/tenant-a/ttl",
            &[
                (CONTENT_TYPE.as_str(), "text/plain"),
                (crate::HEADER_STREAM_TTL, "3000000000"),
            ],
            "ttl",
        )
        .await,
        "ttl create",
    )
    .await;
    ok(
        put(
            &first,
            "/tenant-a/expiring",
            &[
                (CONTENT_TYPE.as_str(), "text/plain"),
                (crate::HEADER_STREAM_EXPIRES_AT, "2100-01-01T00:00:00Z"),
            ],
            "expiring",
        )
        .await,
        "expiring create",
    )
    .await;

    // tenant-c: purged, so the group keeps an erased bucket.
    ok(
        put(&first, "/tenant-c/doomed", &text, "doomed").await,
        "doomed create",
    )
    .await;
    ok(
        send(
            &first,
            "DELETE",
            "/__ursula/purge/tenant-c",
            &[],
            Body::empty(),
        )
        .await,
        "purge",
    )
    .await;

    // tenant-a/gone: flushed to cold, then deleted, so a cold GC entry
    // remains.
    ok(
        put(&first, "/tenant-a/gone", &text, "ghijkl").await,
        "gone create",
    )
    .await;
    ok(
        send(
            &first,
            "POST",
            "/__ursula/flush-cold/tenant-a/gone?min_hot_bytes=4&max_bytes=4",
            &[],
            Body::empty(),
        )
        .await,
        "gone flush",
    )
    .await;
    ok(
        send(&first, "DELETE", "/tenant-a/gone", &[], Body::empty()).await,
        "gone delete",
    )
    .await;

    // The import into the second cluster, and orders written again there.
    let export = ok(get(&first, "/__ursula/backup/group/0").await, "export").await;
    let second = staging_cluster(cold_store.clone());
    let digest = blake3::hash(&export).to_hex().to_string();
    ok(
        send(
            &second,
            "POST",
            "/__ursula/backup/group/0/import",
            &[
                (CONTENT_TYPE.as_str(), "application/x-msgpack"),
                (crate::HEADER_BACKUP_BLAKE3, digest.as_str()),
            ],
            Body::from(export),
        )
        .await,
        "import",
    )
    .await;
    ok(
        send(
            &second,
            "POST",
            "/tenant-a/orders",
            &json,
            Body::from(r#"{"id":4}"#),
        )
        .await,
        "orders append after the import",
    )
    .await;
    second
}

/// A backup of this release round trips through `ursulactl`:
/// `backup-create` writes it, `backup-verify` accepts it, `restore` refuses a
/// target without its cold objects before the first import and names each
/// of them, and once they are copied restores every stream.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_backup_round_trips_through_ursulactl() {
    let source_cold: ColdStoreHandle = Arc::new(ColdStore::memory().expect("memory cold store"));
    let source = written_cluster(&source_cold).await;
    let dir = tempfile::tempdir().expect("backup directory");
    let store = BackupStore::open(dir.path().to_str().expect("a UTF-8 path"))
        .expect("open the backup store");
    let (client, server) = serve(&source).await;
    let manifest = ursula_ctl::backup::create(&client, &store, 1_760_000_000_000)
        .await
        .expect("backup-create");
    server.abort();
    assert_eq!(manifest.format_version, BACKUP_FORMAT_VERSION);
    assert_eq!(manifest.raft_group_count, 1);
    let report = ursula_ctl::backup::verify(&store)
        .await
        .expect("backup-verify");
    assert_eq!((report.groups, report.buckets, report.streams), (1, 2, 7));

    // A target without the source's cold objects is refused before the first
    // import, and each refusal names every object it found missing. The
    // objects a missing index page references come to light once the page
    // is there, so an operator copies what a refusal names until the
    // restore applies.
    let target_cold: ColdStoreHandle = Arc::new(ColdStore::memory().expect("memory cold store"));
    let target = cluster(target_cold.clone());
    let (client, server) = serve(&target).await;
    let mut copied = std::collections::BTreeSet::new();
    let report = loop {
        let missing = match ursula_ctl::backup::restore(&client, &store).await {
            Ok(report) => break report,
            Err(BackupError::ColdObjectsMissing {
                missing,
                referenced,
                sample,
            }) => {
                assert!(missing > 0 && missing <= referenced);
                assert_eq!(
                    u64::try_from(sample.len()).expect("a count"),
                    missing,
                    "the refusal names every missing object: {sample:?}"
                );
                sample
            }
            Err(other) => panic!("expected missing cold objects, got {other}"),
        };
        assert!(
            copied.len() < 8,
            "the refusals keep naming objects: {copied:?}"
        );
        for key in missing {
            assert!(!copied.contains(&key), "{key} was copied before");
            let size = source_cold
                .object_size(&key)
                .await
                .expect("a source object");
            let bytes = source_cold
                .read_whole_object_range(&key, size, 0, usize::try_from(size).expect("a size"))
                .await
                .expect("read a source object");
            target_cold
                .write_chunk(&key, &bytes)
                .await
                .expect("copy a cold object");
            copied.insert(key);
        }
    };
    assert_eq!(report.groups, 1);
    assert_eq!((report.buckets, report.streams), (2, 7));
    assert_eq!(
        report.cold_objects,
        u64::try_from(copied.len()).expect("a count")
    );
    assert_eq!(report.imports.len(), 1);
    assert!(!report.imports[0].already);

    assert_restored(&target).await;

    // A second run finds the import applied.
    let again = ursula_ctl::backup::restore(&client, &store)
        .await
        .expect("a repeated restore");
    assert_eq!(again.already(), 1);
    server.abort();
}

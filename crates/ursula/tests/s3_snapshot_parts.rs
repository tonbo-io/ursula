//! A large snapshot committed from parts on real S3 (#581 section 3.1,
//! M4 of the step 4 review): 256 MiB in eight 32 MiB parts, staged and
//! committed through the router, then read back by range. It logs how long
//! the commit's copy took, the number a gateway's response timeout must
//! cover. CI runs it in the Integration workflow's real-S3 job; it is
//! skipped without `URSULA_COLD_S3_INTEGRATION=1`.

use std::sync::Arc;
use std::time::Instant;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use axum::body::Body;
use axum::body::to_bytes;
use axum::http::Request;
use axum::http::StatusCode;
use axum::http::header::CONTENT_TYPE;
use tower::ServiceExt;
use ursula_config::ColdBackend;
use ursula_headers::PartStaged;
use ursula_headers::PartsCommit;
use ursula_headers::PartsManifest;
use ursula_headers::names;
use ursula_headers::value::SnapshotDigest as DigestValue;
use ursula_runtime::ColdStore;
use ursula_runtime::InMemoryGroupEngineFactory;
use ursula_runtime::RuntimeConfig;
use ursula_runtime::ShardRuntime;
use ursula_runtime::SnapshotDigest;

const OCTET: &str = "application/octet-stream";
const PART_BYTES: usize = 32 * 1024 * 1024;
const PARTS: usize = 8;

fn cold_config(suffix: u128) -> ursula_runtime::ColdConfig {
    ursula_runtime::ColdConfig {
        backend: ColdBackend::S3,
        // CI scopes each job's credentials to URSULA_S3_PREFIX.
        root: Some(match std::env::var("URSULA_S3_PREFIX") {
            Ok(prefix) => format!("{prefix}/ursula-s3-snapshot-parts-{suffix}"),
            Err(_) => format!("ursula-s3-snapshot-parts-{suffix}"),
        }),
        s3: Some(ursula_config::S3Config {
            bucket: std::env::var("URSULA_COLD_S3_BUCKET").ok(),
            region: std::env::var("URSULA_COLD_S3_REGION").ok(),
            endpoint: std::env::var("URSULA_COLD_S3_ENDPOINT").ok(),
            access_key_id: std::env::var("URSULA_COLD_S3_ACCESS_KEY_ID").ok(),
            secret_access_key: std::env::var("URSULA_COLD_S3_SECRET_ACCESS_KEY").ok(),
            session_token: std::env::var("URSULA_COLD_S3_SESSION_TOKEN").ok(),
            server_side_encryption: match std::env::var("URSULA_COLD_S3_SSE").ok().as_deref() {
                Some("none") => ursula_config::S3ServerSideEncryption::None,
                Some("aws-kms") => ursula_config::S3ServerSideEncryption::AwsKms,
                _ => ursula_config::S3ServerSideEncryption::Aes256,
            },
            ..Default::default()
        }),
        ..Default::default()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn s3_commits_a_256_mib_snapshot_from_parts() {
    if std::env::var("URSULA_COLD_S3_INTEGRATION").ok().as_deref() != Some("1") {
        tracing::warn!(
            "skipping the S3 snapshot parts test; set URSULA_COLD_S3_INTEGRATION=1 and \
             URSULA_COLD_S3_BUCKET"
        );
        return;
    }
    // Straight to stdout, past the harness's capture, so CI's log shows the
    // commit time of a passing run.
    let _subscriber = tracing_subscriber::fmt().with_target(false).try_init();
    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time after unix epoch")
        .as_nanos();
    let cold_store = Arc::new(ColdStore::try_new(&cold_config(suffix)).expect("S3 cold store"));
    let runtime = ShardRuntime::spawn_with_engine_factory_and_cold_store(
        RuntimeConfig::new(1, 1),
        InMemoryGroupEngineFactory::with_cold_store(Some(cold_store.clone())),
        Some(cold_store.clone()),
    )
    .expect("spawn runtime");
    let app = ursula::router(runtime);
    let send = |request: Request<Body>| app.clone().oneshot(request);

    let response = send(
        Request::put("/s3parts/s")
            .header(CONTENT_TYPE, OCTET)
            .body(Body::from("abc"))
            .unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);

    let mut digest = SnapshotDigest::new(OCTET);
    let mut parts = Vec::new();
    let staged = Instant::now();
    for index in 0..PARTS {
        let part = (0..PART_BYTES)
            .map(|byte| u8::try_from((byte ^ index) % 251).unwrap())
            .collect::<Vec<_>>();
        digest.update(&part);
        let response = send(
            Request::post("/s3parts/s/snapshot/3/parts")
                .body(Body::from(part))
                .unwrap(),
        )
        .await
        .unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);
        let pairs = response
            .headers()
            .iter()
            .map(|(name, value)| (name.as_str(), value.as_bytes()));
        parts.push(PartStaged::parse(pairs).expect("a part id").part);
    }
    let staged = staged.elapsed();
    let digest = DigestValue::parse("digest", &digest.finalize()).unwrap();
    let manifest = PartsManifest::new(parts).unwrap();
    let commit = PartsCommit::for_manifest(&manifest, digest);
    let mut request = Request::put("/s3parts/s/snapshot/3").header(CONTENT_TYPE, OCTET);
    let mut headers = Vec::new();
    commit.render(|name, value| headers.push((name, value.to_owned())));
    for (name, value) in headers {
        request = request.header(name, value);
    }
    let committed = Instant::now();
    let response = send(request.body(Body::from(manifest.render())).unwrap())
        .await
        .unwrap();
    let committed = committed.elapsed();
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    assert_eq!(
        response.headers()[names::STREAM_SNAPSHOT_DIGEST],
        digest.to_string().as_str()
    );
    tracing::info!(
        bytes = PART_BYTES * PARTS,
        parts = PARTS,
        staged_ms = staged.as_millis(),
        commit_ms = committed.as_millis(),
        "committed a snapshot from parts on S3"
    );

    let last = PART_BYTES * PARTS - 1;
    let response = send(
        Request::get("/s3parts/s/snapshot/3")
            .header("range", format!("bytes={}-{last}", last - 9))
            .body(Body::empty())
            .unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
    let tail = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let expected = (last - 9..=last)
        .map(|byte| u8::try_from(((byte % PART_BYTES) ^ (PARTS - 1)) % 251).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(tail.as_ref(), expected.as_slice());

    cold_store
        .remove_all("")
        .await
        .expect("clean up the per-run S3 root");
}

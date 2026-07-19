use std::sync::Arc;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use ursula_config::ColdBackend;
use ursula_runtime::AppendRequest;
use ursula_runtime::ColdStore;
use ursula_runtime::CreateStreamRequest;
use ursula_runtime::InMemoryGroupEngineFactory;
use ursula_runtime::PlanColdFlushRequest;
use ursula_runtime::ReadStreamRequest;
use ursula_runtime::RuntimeConfig;
use ursula_runtime::ShardRuntime;
use ursula_shard::BucketStreamId;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s3_cold_path_flushes_reads_and_cleans_up_object() {
    if std::env::var("URSULA_COLD_S3_INTEGRATION").ok().as_deref() != Some("1") {
        tracing::warn!(
            "skipping S3 cold-path integration; set URSULA_COLD_S3_INTEGRATION=1 and URSULA_COLD_S3_BUCKET"
        );
        return;
    }

    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time after unix epoch")
        .as_nanos();
    let cold_cfg = ursula_runtime::ColdConfig {
        backend: ColdBackend::S3,
        root: Some(format!("ursula-runtime-s3-cold-{suffix}")),
        s3: Some(ursula_config::S3Config {
            bucket: std::env::var("URSULA_COLD_S3_BUCKET").ok(),
            region: std::env::var("URSULA_COLD_S3_REGION").ok(),
            endpoint: std::env::var("URSULA_COLD_S3_ENDPOINT").ok(),
            access_key_id: std::env::var("URSULA_COLD_S3_ACCESS_KEY_ID").ok(),
            secret_access_key: std::env::var("URSULA_COLD_S3_SECRET_ACCESS_KEY").ok(),
            session_token: std::env::var("URSULA_COLD_S3_SESSION_TOKEN").ok(),
            ..Default::default()
        }),
        ..Default::default()
    };
    let cold_store = Arc::new(
        ColdStore::try_new(&cold_cfg)
            .unwrap_or_else(|err| panic!("cold store creation failed: {err}")),
    );
    let runtime = ShardRuntime::spawn_with_engine_factory_and_cold_store(
        RuntimeConfig::new(2, 8).with_cold_max_hot_bytes_per_group(Some(8 * 1024 * 1024)),
        InMemoryGroupEngineFactory::with_cold_store(Some(cold_store.clone())),
        Some(cold_store.clone()),
    )
    .expect("spawn runtime");

    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time after unix epoch")
        .as_nanos();
    let stream = BucketStreamId::new("benchcmp", format!("s3-cold-{suffix}"));
    runtime
        .create_stream(CreateStreamRequest::new(
            stream.clone(),
            "application/octet-stream",
        ))
        .await
        .expect("create stream");
    runtime
        .append(AppendRequest::from_bytes(
            stream.clone(),
            b"abcdef".to_vec(),
        ))
        .await
        .expect("append hot bytes");

    runtime
        .flush_cold_once(PlanColdFlushRequest {
            stream_id: stream.clone(),
            min_hot_bytes: 4,
            max_flush_bytes: 4,
        })
        .await
        .expect("flush to S3")
        .expect("candidate flushed");

    let read = runtime
        .read_stream(ReadStreamRequest {
            stream_id: stream.clone(),
            offset: 0,
            max_len: 6,
            now_ms: 0,
            record: None,
            max_records: None,
        })
        .await
        .expect("read cold and hot bytes");
    assert_eq!(read.payload, b"abcdef");
    assert_eq!(read.next_offset, 6);

    let metrics = runtime.metrics().snapshot();
    assert_eq!(metrics.cold_flush_uploads, 1);
    assert_eq!(metrics.cold_flush_upload_bytes, 4);
    assert_eq!(metrics.cold_flush_publishes, 1);
    assert_eq!(metrics.cold_flush_publish_bytes, 4);

    let snapshot = runtime
        .snapshot_group(runtime.locate(&stream).raft_group_id)
        .await
        .expect("snapshot group");
    let chunk_paths = snapshot
        .stream_snapshot
        .streams
        .iter()
        .find(|entry| entry.metadata.stream_id == stream)
        .expect("stream snapshot entry")
        .cold_chunks
        .iter()
        .map(|chunk| chunk.s3_path.clone())
        .collect::<Vec<_>>();
    assert_eq!(chunk_paths.len(), 1);
    for path in chunk_paths {
        cold_store
            .delete_chunk(&path)
            .await
            .expect("cleanup S3 chunk");
    }
}

//! Stream-GC containment and incarnation-scoped cold objects (bounded-state
//! D4, F14a, F14g) on the in-memory engine.

use std::sync::Arc;

use ursula_shard::BucketStreamId;
use ursula_stream::ColdChunkRef;
use ursula_stream::ExternalPayloadRef;

use crate::AppendExternalRequest;
use crate::AppendRequest;
use crate::ColdStore;
use crate::CreateStreamExternalRequest;
use crate::CreateStreamRequest;
use crate::DeleteStreamRequest;
use crate::HeadStreamRequest;
use crate::InMemoryGroupEngineFactory;
use crate::PlanColdFlushRequest;
use crate::ReadStreamRequest;
use crate::RuntimeConfig;
use crate::ShardRuntime;
use crate::cold_index::ColdIndexPageKey;
use crate::cold_index::ColdStoreColdIndexPageStore;
use crate::cold_index::load_cold_chunks_from_pages;
use crate::cold_store::DEFAULT_CONTENT_TYPE;
use crate::new_external_payload_path;

fn spawn(cold_store: Arc<ColdStore>) -> ShardRuntime {
    ShardRuntime::spawn_with_engine_factory_and_cold_store(
        RuntimeConfig::new(2, 8),
        InMemoryGroupEngineFactory::with_cold_store(Some(cold_store.clone())),
        Some(cold_store),
    )
    .expect("spawn runtime")
}

async fn create(runtime: &ShardRuntime, stream: &BucketStreamId) {
    runtime
        .create_stream(CreateStreamRequest::new(
            stream.clone(),
            DEFAULT_CONTENT_TYPE,
        ))
        .await
        .expect("create stream");
}

async fn append(runtime: &ShardRuntime, stream: &BucketStreamId, payload: &[u8]) {
    runtime
        .append(AppendRequest::from_bytes(stream.clone(), payload.to_vec()))
        .await
        .expect("append");
}

async fn delete(runtime: &ShardRuntime, stream: &BucketStreamId) {
    runtime
        .delete_stream(DeleteStreamRequest {
            stream_id: stream.clone(),
        })
        .await
        .expect("delete stream");
}

/// Flushes exactly `len` hot bytes as one exclusive chunk.
async fn flush(runtime: &ShardRuntime, stream: &BucketStreamId, len: usize) {
    runtime
        .flush_cold_once(PlanColdFlushRequest {
            stream_id: stream.clone(),
            min_hot_bytes: len,
            max_flush_bytes: len,
        })
        .await
        .expect("flush cold")
        .expect("flush candidate");
}

async fn read(runtime: &ShardRuntime, stream: &BucketStreamId, len: usize) -> Vec<u8> {
    runtime
        .read_stream(ReadStreamRequest {
            stream_id: stream.clone(),
            offset: 0,
            max_len: len,
            now_ms: 0,
            leader_only: false,
            read_index: None,
        })
        .await
        .expect("read stream")
        .payload
}

async fn generation(runtime: &ShardRuntime, stream: &BucketStreamId) -> u64 {
    let group = runtime.locate(stream).raft_group_id;
    runtime
        .snapshot_group(group)
        .await
        .expect("snapshot group")
        .stream_snapshot
        .streams
        .into_iter()
        .find(|entry| entry.metadata.stream_id == *stream)
        .map(|entry| entry.cold_index_generation)
        .expect("stream in snapshot")
}

async fn page_chunks(
    cold_store: &Arc<ColdStore>,
    stream: &BucketStreamId,
    generation: u64,
) -> Vec<ColdChunkRef> {
    let store = ColdStoreColdIndexPageStore::new(cold_store.clone());
    load_cold_chunks_from_pages(&store, &[ColdIndexPageKey {
        stream_id: stream.clone(),
        generation,
        page_id: 0,
    }])
    .await
    .expect("load page chunks")
}

async fn object_exists(cold_store: &ColdStore, path: &str) -> bool {
    let probe = ColdChunkRef {
        start_offset: 0,
        end_offset: 1,
        s3_path: path.to_owned(),
        object_size: 1,
        object_offset: 0,
        shared_object: false,
        payload_digest: String::new(),
    };
    cold_store.read_chunk_range(&probe, 0, 1).await.is_ok()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn d4_recreate_with_gc_pending_keeps_new_incarnation_objects() {
    let cold_store = Arc::new(ColdStore::memory().expect("memory cold store"));
    let runtime = spawn(cold_store.clone());
    let stream = BucketStreamId::new("d4-bucket", "recreated");

    create(&runtime, &stream).await;
    append(&runtime, &stream, b"OLD!").await;
    flush(&runtime, &stream, 4).await;
    delete(&runtime, &stream).await;

    create(&runtime, &stream).await;
    append(&runtime, &stream, b"new!").await;
    flush(&runtime, &stream, 4).await;
    // C7/F14g: the new incarnation's pages live under its own generation.
    let generation = runtime
        .head_stream(crate::HeadStreamRequest {
            stream_id: stream.clone(),
            now_ms: 0,
            linearizable: false,
            read_index: None,
        })
        .await
        .expect("head")
        .created_at_ms
        .expect("incarnation");
    let new_chunks = page_chunks(&cold_store, &stream, generation).await;
    assert_eq!(new_chunks.len(), 1);

    runtime
        .run_cold_gc_all_groups_once(256)
        .await
        .expect("run stream gc");

    assert!(object_exists(&cold_store, &new_chunks[0].s3_path).await);
    assert_eq!(read(&runtime, &stream, 4).await, b"new!");
    // The entry is acknowledged, not retried forever: the old incarnation's
    // objects become a bounded leak for the orphan sweep (F14h).
    let group = runtime.locate(&stream).raft_group_id;
    let snapshot = runtime.snapshot_group(group).await.expect("snapshot group");
    assert!(snapshot.stream_snapshot.pending_cold_gc.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn incarnation_scoped_recreate_reads_its_own_pages_and_gc_reclaims_the_old_incarnation() {
    let cold_store = Arc::new(ColdStore::memory().expect("memory cold store"));
    let runtime = spawn(cold_store.clone());
    let stream = BucketStreamId::new("d4-bucket", "scoped");

    create(&runtime, &stream).await;
    append(&runtime, &stream, b"old-old-old!").await;
    flush(&runtime, &stream, 12).await;
    let old_generation = generation(&runtime, &stream).await;
    assert_ne!(old_generation, 0);
    let old_chunks = page_chunks(&cold_store, &stream, old_generation).await;
    assert_eq!(old_chunks.len(), 1);
    delete(&runtime, &stream).await;

    // The test clock is frozen at 0, so only C7 separates the incarnations.
    create(&runtime, &stream).await;
    append(&runtime, &stream, b"new!").await;
    flush(&runtime, &stream, 4).await;
    let new_generation = generation(&runtime, &stream).await;
    assert!(new_generation > old_generation);
    // HEAD exposes the unique incarnation (C7).
    let head = runtime
        .head_stream(HeadStreamRequest {
            stream_id: stream.clone(),
            now_ms: 0,
            linearizable: true,
            read_index: None,
        })
        .await
        .expect("head stream");
    assert_eq!(head.created_at_ms, Some(new_generation));
    let new_chunks = page_chunks(&cold_store, &stream, new_generation).await;
    assert_eq!(new_chunks.len(), 1);
    assert!(
        new_chunks[0]
            .s3_path
            .contains(&format!("/chunks/{new_generation:016x}/"))
    );
    assert_eq!(read(&runtime, &stream, 4).await, b"new!");

    runtime
        .run_cold_gc_all_groups_once(256)
        .await
        .expect("run stream gc");

    assert!(!object_exists(&cold_store, &old_chunks[0].s3_path).await);
    assert!(
        page_chunks(&cold_store, &stream, old_generation)
            .await
            .is_empty()
    );
    assert!(object_exists(&cold_store, &new_chunks[0].s3_path).await);
    assert_eq!(read(&runtime, &stream, 4).await, b"new!");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn incarnation_scoped_stream_gc_reclaims_external_payloads() {
    let cold_store = Arc::new(ColdStore::memory().expect("memory cold store"));
    let runtime = spawn(cold_store.clone());
    let stream = BucketStreamId::new("d4-bucket", "external");

    let stage = |payload: &'static [u8]| {
        let cold_store = cold_store.clone();
        let stream = stream.clone();
        async move {
            let s3_path = new_external_payload_path(&stream);
            let object_size = cold_store
                .write_chunk(&s3_path, payload)
                .await
                .expect("stage external payload");
            ExternalPayloadRef {
                s3_path,
                payload_len: u64::try_from(payload.len()).expect("len fits u64"),
                object_size,
            }
        }
    };
    let initial = stage(b"init").await;
    runtime
        .create_stream_external(CreateStreamExternalRequest {
            stream_id: stream.clone(),
            content_type: DEFAULT_CONTENT_TYPE.to_owned(),
            initial_payload: initial.clone(),
            record_ends: Vec::new(),
            close_after: false,
            stream_seq: None,
            producer: None,
            stream_ttl_seconds: None,
            stream_expires_at_ms: None,
            now_ms: 0,
        })
        .await
        .expect("create with external payload");
    let appended = stage(b"more").await;
    runtime
        .append_external(AppendExternalRequest {
            stream_id: stream.clone(),
            content_type: DEFAULT_CONTENT_TYPE.to_owned(),
            payload: appended.clone(),
            record_ends: Vec::new(),
            close_after: false,
            stream_seq: None,
            producer: None,
            now_ms: 0,
        })
        .await
        .expect("append external payload");
    assert_eq!(read(&runtime, &stream, 8).await, b"initmore");

    delete(&runtime, &stream).await;
    runtime
        .run_cold_gc_all_groups_once(256)
        .await
        .expect("run stream gc");

    assert!(!object_exists(&cold_store, &initial.s3_path).await);
    assert!(!object_exists(&cold_store, &appended.s3_path).await);
}

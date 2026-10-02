//! Raft-engine regression tests for `CompactCold` with all-shared inputs
//! (bounded-stream-state F2, Raft branch): direct shared-to-exclusive
//! compaction, legacy pack migration and the bucket purge that depends on it.

use std::sync::Arc;

use ursula_runtime::AppendRequest;
use ursula_runtime::ColdChunkRef;
use ursula_runtime::ColdIndexPageKey;
use ursula_runtime::ColdStore;
use ursula_runtime::ColdStoreColdIndexPageStore;
use ursula_runtime::CompactColdRequest;
use ursula_runtime::CreateStreamRequest;
use ursula_runtime::FlushColdRequest;
use ursula_runtime::ReadStreamRequest;
use ursula_runtime::RuntimeConfig;
use ursula_runtime::ShardRuntime;
use ursula_runtime::load_cold_chunks_from_pages;
use ursula_shard::BucketStreamId;
use ursula_shard::RaftGroupId;

use super::ColdRaftGroupEngineFactory;

fn spawn_raft_with_cold_store(config: RuntimeConfig, cold_store: Arc<ColdStore>) -> ShardRuntime {
    ShardRuntime::spawn_with_engine_factory_and_cold_store(
        config,
        ColdRaftGroupEngineFactory::new(cold_store.clone()),
        Some(cold_store),
    )
    .expect("spawn raft runtime")
}

fn stream_in_bucket_on_group(
    runtime: &ShardRuntime,
    group_id: RaftGroupId,
    bucket_id: &str,
    prefix: &str,
) -> BucketStreamId {
    (0..10_000)
        .map(|index| BucketStreamId::new(bucket_id, format!("{prefix}-{index}")))
        .find(|stream| runtime.locate(stream).raft_group_id == group_id)
        .expect("stream on group")
}

async fn create_and_append(runtime: &ShardRuntime, stream: &BucketStreamId, payload: &[u8]) {
    runtime
        .create_stream(CreateStreamRequest::new(
            stream.clone(),
            "application/octet-stream",
        ))
        .await
        .expect("create stream");
    runtime
        .append(AppendRequest::from_bytes(stream.clone(), payload.to_vec()))
        .await
        .expect("append");
}

fn shared_slice(path: &str, object_offset: u64, payload: &[u8]) -> ColdChunkRef {
    ColdChunkRef {
        start_offset: 0,
        end_offset: u64::try_from(payload.len()).expect("len fits u64"),
        s3_path: path.to_owned(),
        object_size: 8,
        object_offset,
        shared_object: true,
        payload_digest: blake3::hash(payload).to_hex().to_string(),
    }
}

async fn page_chunks(cold_store: &Arc<ColdStore>, stream: &BucketStreamId) -> Vec<ColdChunkRef> {
    let page_store = ColdStoreColdIndexPageStore::new(cold_store.clone());
    load_cold_chunks_from_pages(&page_store, &[ColdIndexPageKey {
        stream_id: stream.clone(),
        generation: 0,
        page_id: 0,
    }])
    .await
    .expect("load cold index page")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn raft_compacts_shared_slice_into_exclusive_chunk() {
    let cold_store = Arc::new(ColdStore::memory().expect("memory cold store"));
    let runtime = spawn_raft_with_cold_store(RuntimeConfig::new(1, 1), cold_store.clone());
    let stream = BucketStreamId::new("raft-compact", "shared");
    create_and_append(&runtime, &stream, b"aaaa").await;

    let pack = "_packs/00000000/shared-compact.bin";
    cold_store
        .write_chunk(pack, b"aaaabbbb")
        .await
        .expect("write pack");
    let slice = shared_slice(pack, 0, b"aaaa");
    runtime
        .flush_cold(FlushColdRequest {
            cold_generation: None,
            stream_id: stream.clone(),
            chunk: slice.clone(),
        })
        .await
        .expect("publish shared slice");

    let replacement_path = "raft-compact/shared/chunks/replacement.bin";
    let object_size = cold_store
        .write_chunk(replacement_path, b"aaaa")
        .await
        .expect("write replacement");
    let replacement = ColdChunkRef {
        start_offset: 0,
        end_offset: 4,
        s3_path: replacement_path.to_owned(),
        object_size,
        object_offset: 0,
        shared_object: false,
        payload_digest: blake3::hash(b"aaaa").to_hex().to_string(),
    };
    runtime
        .compact_cold(CompactColdRequest {
            stream_id: stream.clone(),
            old_chunks: vec![slice],
            replacement: replacement.clone(),
            gc_not_before_ms: 0,
        })
        .await
        .expect("raft engine compacts an all-shared input");

    let indexed = page_chunks(&cold_store, &stream).await;
    assert_eq!(indexed.len(), 1);
    assert_eq!(indexed[0].s3_path, replacement.s3_path);
    assert!(!indexed[0].shared_object);
    let read = runtime
        .read_stream(ReadStreamRequest {
            stream_id: stream,
            offset: 0,
            max_len: 4,
            now_ms: 0,
            record: None,
            max_records: None,
            leader_only: false,
            record_anchor: None,
        })
        .await
        .expect("read compacted stream");
    assert_eq!(read.payload, b"aaaa");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn raft_legacy_cross_bucket_pack_migration_and_bucket_purge() {
    let cold_store = Arc::new(ColdStore::memory().expect("memory cold store"));
    let runtime = spawn_raft_with_cold_store(RuntimeConfig::new(1, 2), cold_store.clone());
    let group_id = RaftGroupId(1);
    let stream_a = stream_in_bucket_on_group(&runtime, group_id, "legacy-raft-a", "shared-a");
    let stream_b = stream_in_bucket_on_group(&runtime, group_id, "legacy-raft-b", "shared-b");
    create_and_append(&runtime, &stream_a, b"aaaa").await;
    create_and_append(&runtime, &stream_b, b"bbbb").await;

    let legacy_path = "_packs/00000001/legacy-cross-bucket.bin";
    cold_store
        .write_chunk(legacy_path, b"aaaabbbb")
        .await
        .expect("write legacy pack");
    for (stream, object_offset, payload) in [
        (&stream_a, 0, b"aaaa".as_slice()),
        (&stream_b, 4, b"bbbb".as_slice()),
    ] {
        runtime
            .flush_cold(FlushColdRequest {
                cold_generation: None,
                stream_id: stream.clone(),
                chunk: shared_slice(legacy_path, object_offset, payload),
            })
            .await
            .expect("publish legacy shared slice");
    }

    let migration = runtime
        .migrate_legacy_shared_cold_once(2, 0)
        .await
        .expect("raft engine migrates legacy shared slices");
    assert_eq!(migration.observed_chunks, 2);
    assert_eq!(migration.migrated_chunks, 2);
    assert_eq!(migration.pending_chunks, 0);
    runtime
        .run_cold_gc_all_groups_once(256)
        .await
        .expect("delete legacy pack");
    let legacy = shared_slice(legacy_path, 0, b"aaaa");
    assert!(cold_store.read_chunk_range(&legacy, 0, 4).await.is_err());

    let a_chunks = page_chunks(&cold_store, &stream_a).await;
    let b_chunks = page_chunks(&cold_store, &stream_b).await;
    assert_eq!(a_chunks.len(), 1);
    assert_eq!(b_chunks.len(), 1);
    assert!(a_chunks.iter().all(|chunk| !chunk.shared_object));
    assert!(a_chunks[0].s3_path.starts_with("legacy-raft-a/"));

    let purge = runtime
        .purge_bucket_all_groups("legacy-raft-a")
        .await
        .expect("purge migrated bucket on raft");
    assert_eq!(purge.removed_streams, 1);
    runtime
        .run_cold_gc_all_groups_once(256)
        .await
        .expect("erase migrated bucket");
    assert!(
        cold_store
            .read_chunk_range(&a_chunks[0], 0, 4)
            .await
            .is_err()
    );
    assert_eq!(
        cold_store
            .read_chunk_range(&b_chunks[0], 0, 4)
            .await
            .expect("other bucket remains readable"),
        b"bbbb"
    );
}

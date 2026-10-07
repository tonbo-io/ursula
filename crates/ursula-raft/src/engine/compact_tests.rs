//! Raft-engine regression test for `CompactCold` with all-shared inputs
//! (bounded-stream-state F2, Raft branch): direct shared-to-exclusive
//! compaction.

use std::sync::Arc;

use ursula_runtime::AppendRequest;
use ursula_runtime::ColdChunkRef;
use ursula_runtime::ColdStore;
use ursula_runtime::ColdStoreColdIndexPageStore;
use ursula_runtime::CompactColdRequest;
use ursula_runtime::CreateStreamRequest;
use ursula_runtime::FlushColdRequest;
use ursula_runtime::HeadStreamRequest;
use ursula_runtime::ReadStreamRequest;
use ursula_runtime::RuntimeConfig;
use ursula_runtime::ShardRuntime;
use ursula_runtime::load_cold_chunks_from_pages;
use ursula_shard::BucketStreamId;

use super::test_support::JournalRuntime;
use super::test_support::spawn_journal_runtime;

fn spawn_raft_with_cold_store(config: RuntimeConfig, cold_store: Arc<ColdStore>) -> JournalRuntime {
    spawn_journal_runtime(config, Some(cold_store))
}

/// C7/F14g: the cold generation of the stream's live incarnation.
async fn cold_generation(runtime: &ShardRuntime, stream: &BucketStreamId) -> u64 {
    runtime
        .head_stream(HeadStreamRequest {
            stream_id: stream.clone(),
            now_ms: 0,
            linearizable: false,
            read_index: None,
        })
        .await
        .expect("head stream")
        .created_at_ms
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
    // C7/F14g: pages live under the stream's incarnation generation.
    let keys = cold_store
        .list_cold_index_pages()
        .await
        .expect("list pages")
        .into_iter()
        .filter(|key| &key.stream_id == stream)
        .collect::<Vec<_>>();
    let page_store = ColdStoreColdIndexPageStore::new(cold_store.clone());
    load_cold_chunks_from_pages(&page_store, &keys)
        .await
        .expect("load cold index page")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn raft_compacts_shared_slice_into_exclusive_chunk() {
    let cold_store = Arc::new(ColdStore::memory().expect("memory cold store"));
    let runtime = spawn_raft_with_cold_store(RuntimeConfig::new(1, 1), cold_store.clone());
    let stream = BucketStreamId::new("raft-compact", "shared");
    create_and_append(&runtime, &stream, b"aaaa").await;

    let pack = "raft-compact/_packs/00000000/shared-compact.bin";
    cold_store
        .write_chunk(pack, b"aaaabbbb")
        .await
        .expect("write pack");
    let slice = shared_slice(pack, 0, b"aaaa");
    runtime
        .flush_cold(FlushColdRequest {
            cold_generation: cold_generation(&runtime, &stream).await,
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
            leader_only: false,
            read_index: None,
        })
        .await
        .expect("read compacted stream");
    assert_eq!(read.payload, b"aaaa");
}

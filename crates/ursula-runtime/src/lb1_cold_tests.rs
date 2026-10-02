//! Bounded-state level Lb1 cold hygiene on the in-memory engine: F14b
//! `DeferColdGc` in the GC worker, F14i retention grace for dropped pack
//! slices, the `FlushCold` incarnation check, and apply-time cold-index page
//! invalidation.

use std::sync::Arc;

use bytes::Bytes;
use ursula_shard::BucketStreamId;
use ursula_shard::RaftGroupId;
use ursula_stream::ColdChunkRef;
use ursula_stream::ColdGcTarget;
use ursula_stream::FEATURE_LEVEL_KEYED_STREAMS;
use ursula_stream::StreamCommand;
use ursula_stream::StreamReadColdIndexSegment;

use crate::AdvanceRetentionRequest;
use crate::AppendRequest;
use crate::ColdStore;
use crate::ColdStoreFaultEffect;
use crate::ColdStoreOperation;
use crate::CreateStreamRequest;
use crate::DeleteStreamRequest;
use crate::GroupWriteCommand;
use crate::InMemoryGroupEngine;
use crate::InMemoryGroupEngineFactory;
use crate::PlanColdFlushRequest;
use crate::PlanGroupColdFlushRequest;
use crate::PublishSnapshotRequest;
use crate::ReadStreamRequest;
use crate::RuntimeConfig;
use crate::ShardRuntime;
use crate::cold_index::ColdIndexPageKey;
use crate::cold_index::ColdStoreColdIndexPageStore;
use crate::cold_index::load_cold_chunks_from_pages;
use crate::cold_index::write_cold_chunk_index_pages_with_rollback_in_generation;
use crate::cold_store::DEFAULT_CONTENT_TYPE;
use crate::cold_store::cold_chunk_dir;

fn spawn(cold_store: Arc<ColdStore>) -> ShardRuntime {
    ShardRuntime::spawn_with_engine_factory_and_cold_store(
        RuntimeConfig::new(1, 4),
        InMemoryGroupEngineFactory::with_cold_store(Some(cold_store.clone())),
        Some(cold_store),
    )
    .expect("spawn runtime")
}

async fn raise(runtime: &ShardRuntime, level: u32) {
    if level == 0 {
        return;
    }
    for (group, result) in runtime.set_feature_level_all_groups(level).await {
        result.unwrap_or_else(|err| panic!("raise group {group:?}: {err}"));
    }
}

fn stream_on_group(runtime: &ShardRuntime, group: RaftGroupId, prefix: &str) -> BucketStreamId {
    (0..10_000)
        .map(|index| BucketStreamId::new("lb1cold", format!("{prefix}-{index}")))
        .find(|stream| runtime.locate(stream).raft_group_id == group)
        .expect("stream on group")
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

async fn flush_exclusive(runtime: &ShardRuntime, stream: &BucketStreamId, len: usize) {
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
            record: None,
            max_records: None,
            leader_only: false,
        })
        .await
        .expect("read stream")
        .payload
}

async fn pending_gc(runtime: &ShardRuntime, group: RaftGroupId) -> Vec<ursula_stream::ColdGcEntry> {
    runtime
        .snapshot_group(group)
        .await
        .expect("snapshot group")
        .stream_snapshot
        .pending_cold_gc
}

async fn object_exists(cold_store: &ColdStore, path: &str) -> bool {
    let (dir, name) = path.rsplit_once('/').expect("object path has a directory");
    cold_store
        .list_file_names(&format!("{dir}/"))
        .await
        .expect("list objects")
        .iter()
        .any(|listed| listed == name)
}

/// Two deleted streams with flushed chunks in one group; the first one's
/// objects cannot be deleted.
async fn gc_with_failing_head(
    level: u32,
) -> (
    Arc<ColdStore>,
    ShardRuntime,
    RaftGroupId,
    [BucketStreamId; 2],
) {
    let cold_store = Arc::new(ColdStore::memory().expect("memory cold store"));
    let runtime = spawn(cold_store.clone());
    raise(&runtime, level).await;
    let group = RaftGroupId(1);
    let streams = [
        stream_on_group(&runtime, group, "failing"),
        stream_on_group(&runtime, group, "healthy"),
    ];
    for stream in &streams {
        create(&runtime, stream).await;
        append(&runtime, stream, b"abcd").await;
        flush_exclusive(&runtime, stream, 4).await;
        delete(&runtime, stream).await;
    }
    let failing = format!("{}/", streams[0]);
    cold_store.set_fault_policy(move |context| {
        (matches!(
            context.operation,
            ColdStoreOperation::DeleteChunk | ColdStoreOperation::RemoveAll
        ) && context.path.starts_with(&failing))
        .then(|| ColdStoreFaultEffect::fail("injected delete failure"))
    });
    (cold_store, runtime, group, streams)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn f14b_failing_gc_head_blocks_the_queue_below_level_one() {
    let (_cold_store, runtime, group, streams) = gc_with_failing_head(0).await;
    let before = pending_gc(&runtime, group).await;
    assert_eq!(before.len(), 2);
    assert!(runtime.run_cold_gc_group_once(group, 16).await.is_err());
    // Nothing behind the failing head was reclaimed.
    let after = pending_gc(&runtime, group).await;
    assert_eq!(after, before);
    assert_eq!(after[1].target, ColdGcTarget::Stream(streams[1].clone()));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn f14b_failing_gc_head_is_deferred_and_the_rest_drains_at_level_one() {
    let (cold_store, runtime, group, streams) =
        gc_with_failing_head(FEATURE_LEVEL_KEYED_STREAMS).await;
    let before = pending_gc(&runtime, group).await;
    assert_eq!(before.len(), 2);
    let started_ms = crate::runtime::unix_time_ms();

    assert_eq!(
        runtime
            .run_cold_gc_group_once(group, 16)
            .await
            .expect("pass reclaims the healthy entry"),
        1
    );
    let after = pending_gc(&runtime, group).await;
    assert_eq!(after.len(), 1, "{after:?}");
    assert_eq!(after[0].target, ColdGcTarget::Stream(streams[0].clone()));
    assert!(after[0].seq > before[1].seq);
    assert!(after[0].not_before_ms >= started_ms + crate::runtime::COLD_GC_DEFER_BACKOFF_MS);
    assert!(runtime.metrics().snapshot().cold_gc_errors >= 1);

    // While the backoff runs the entry is not retried.
    cold_store.clear_fault_policy();
    assert_eq!(
        runtime.run_cold_gc_group_once(group, 16).await.ok(),
        Some(0)
    );
    assert_eq!(pending_gc(&runtime, group).await.len(), 1);
}

/// Wave-1 follow-up: a flush planned from one incarnation is rejected after
/// a delete and recreate whose hot prefix holds the same bytes; its chunk
/// would live under the removed incarnation's generation.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn flush_planned_before_delete_and_recreate_never_publishes_into_the_new_incarnation() {
    let cold_store = Arc::new(ColdStore::memory().expect("memory cold store"));
    let runtime = spawn(cold_store.clone());
    raise(&runtime, FEATURE_LEVEL_KEYED_STREAMS).await;
    let group = RaftGroupId(2);
    let stream = stream_on_group(&runtime, group, "reborn");
    create(&runtime, &stream).await;
    append(&runtime, &stream, b"abcd").await;
    let candidates = runtime
        .plan_next_cold_flush_batch(
            group,
            PlanGroupColdFlushRequest {
                min_hot_bytes: 4,
                max_flush_bytes: 4,
                max_batch_bytes: 4,
                pressure: None,
            },
            1,
        )
        .await
        .expect("plan");
    assert_eq!(candidates.len(), 1);
    let old_generation = candidates[0].cold_generation;

    delete(&runtime, &stream).await;
    create(&runtime, &stream).await;
    append(&runtime, &stream, b"abcd").await;

    assert!(
        runtime
            .flush_cold_candidates_batch(candidates)
            .await
            .expect("stale candidate is classified")
            .is_empty()
    );
    // The rejected chunk was removed, and the new incarnation is untouched.
    assert!(
        cold_store
            .list_file_names(&cold_chunk_dir(&stream, old_generation))
            .await
            .expect("list old generation chunks")
            .is_empty()
    );
    assert_eq!(read(&runtime, &stream, 16).await, b"abcd");

    // Its own flush lands under its own generation.
    flush_exclusive(&runtime, &stream, 4).await;
    let generation = runtime
        .snapshot_group(group)
        .await
        .expect("snapshot")
        .stream_snapshot
        .streams
        .into_iter()
        .find(|entry| entry.metadata.stream_id == stream)
        .expect("stream entry")
        .cold_index_generation;
    assert_ne!(generation, old_generation);
    let store = ColdStoreColdIndexPageStore::new(cold_store.clone());
    let chunks = load_cold_chunks_from_pages(&store, &[ColdIndexPageKey {
        stream_id: stream.clone(),
        generation,
        page_id: 0,
    }])
    .await
    .expect("load pages");
    assert_eq!(chunks.len(), 1);
    assert!(
        chunks[0]
            .s3_path
            .starts_with(&cold_chunk_dir(&stream, generation))
    );
    assert_eq!(read(&runtime, &stream, 16).await, b"abcd");
}

/// Two streams of one bucket share a pack; the first is deleted, then
/// retention drops the last reference to the pack from the second.
async fn retain_past_last_pack_reference(level: u32) -> (Arc<ColdStore>, String) {
    let cold_store = Arc::new(ColdStore::memory().expect("memory cold store"));
    let runtime = spawn(cold_store.clone());
    raise(&runtime, level).await;
    let group = RaftGroupId(3);
    let streams = [
        stream_on_group(&runtime, group, "pack-a"),
        stream_on_group(&runtime, group, "pack-b"),
    ];
    for stream in &streams {
        create(&runtime, stream).await;
        append(&runtime, stream, b"abcd").await;
    }
    let flushed = runtime
        .flush_cold_group_batch_once(
            group,
            PlanGroupColdFlushRequest {
                min_hot_bytes: 4,
                max_flush_bytes: 4,
                max_batch_bytes: 8,
                pressure: None,
            },
            8,
        )
        .await
        .expect("flush pack");
    assert_eq!(flushed.len(), 2);
    let pack_path = runtime
        .snapshot_group(group)
        .await
        .expect("snapshot")
        .stream_snapshot
        .streams
        .iter()
        .find_map(|entry| entry.cold_chunks.first().map(|chunk| chunk.s3_path.clone()))
        .expect("pack chunk");
    delete(&runtime, &streams[0]).await;
    append(&runtime, &streams[1], b"ef").await;
    runtime
        .publish_snapshot(PublishSnapshotRequest {
            stream_id: streams[1].clone(),
            snapshot_offset: 4,
            content_type: DEFAULT_CONTENT_TYPE.to_owned(),
            payload: Bytes::from_static(b"state"),
            expected_digest: None,
            now_ms: crate::runtime::unix_time_ms(),
        })
        .await
        .expect("publish checkpoint");
    runtime
        .advance_retention(AdvanceRetentionRequest {
            stream_id: streams[1].clone(),
            retained_offset: 4,
            now_ms: crate::runtime::unix_time_ms(),
        })
        .await
        .expect("advance retention");
    // The deleted stream's own entry needs no pack; reclaim what is due.
    let _ = runtime.run_cold_gc_group_once(group, 16).await;
    (cold_store, pack_path)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn f14i_retention_deletes_dropped_pack_slices_at_once_below_level_one() {
    let (cold_store, pack_path) = retain_past_last_pack_reference(0).await;
    assert!(!object_exists(&cold_store, &pack_path).await);
}

/// RC-19 / D6: a read planned before a concurrent retention still finds the
/// pack bytes, because GC waits for the grace.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn f14i_retention_keeps_dropped_pack_slices_for_the_grace_at_level_one() {
    let (cold_store, pack_path) =
        retain_past_last_pack_reference(FEATURE_LEVEL_KEYED_STREAMS).await;
    assert!(object_exists(&cold_store, &pack_path).await);
}

/// Wave-1 follow-up: a replica's cached cold-index page is dropped when a
/// `FlushCold` over its range applies, so entries a leader-side clip removed
/// are not served from the cache afterwards.
#[tokio::test]
async fn applying_flush_cold_drops_cached_pages_of_the_flushed_range() {
    let cold_store = Arc::new(ColdStore::memory().expect("memory cold store"));
    let mut engine = InMemoryGroupEngine::with_cold_store(cold_store.clone());
    let placement = ursula_shard::ShardPlacement {
        core_id: ursula_shard::CoreId(0),
        shard_id: ursula_shard::ShardId(0),
        raft_group_id: RaftGroupId(0),
    };
    let stream = BucketStreamId::new("lb1cold", "cached");
    // Creating the stream creates its bucket.
    let create = StreamCommand::CreateStream {
        stream_id: stream.clone(),
        content_type: DEFAULT_CONTENT_TYPE.to_owned(),
        initial_payload: Bytes::from_static(b"abcd"),
        close_after: false,
        stream_seq: None,
        producer: None,
        stream_ttl_seconds: None,
        stream_expires_at_ms: None,
        attrs: None,
        now_ms: 0,
    };
    engine
        .apply_committed_write(GroupWriteCommand::Stream(create), placement)
        .expect("create stream");
    // A stale entry for `[0, 2)` sits in the stream's page and in the cache.
    let store = ColdStoreColdIndexPageStore::new(cold_store.clone());
    let stale = ColdChunkRef {
        start_offset: 0,
        end_offset: 2,
        s3_path: "lb1cold/cached/chunks/stale.bin".to_owned(),
        object_size: 2,
        ..Default::default()
    };
    write_cold_chunk_index_pages_with_rollback_in_generation(&store, &stream, 0, &stale)
        .await
        .expect("write stale entry");
    let cache = engine.cold_index_cache().expect("page cache");
    cache
        .object_segments_for_read(&stream, &StreamReadColdIndexSegment {
            generation: 0,
            page_id: 0,
            read_start_offset: 0,
            len: 2,
        })
        .await
        .expect("cache the page");
    assert_eq!(cache.cached_page_count(), 1);

    // A follower applies the leader's flush of `[0, 4)`.
    engine
        .apply_committed_write(
            GroupWriteCommand::Stream(StreamCommand::FlushCold {
                stream_id: stream.clone(),
                chunk: ColdChunkRef {
                    start_offset: 0,
                    end_offset: 4,
                    s3_path: "lb1cold/cached/chunks/flushed.bin".to_owned(),
                    object_size: 4,
                    ..Default::default()
                },
                cold_generation: None,
            }),
            placement,
        )
        .expect("apply flush");
    assert_eq!(cache.cached_page_count(), 0);
}

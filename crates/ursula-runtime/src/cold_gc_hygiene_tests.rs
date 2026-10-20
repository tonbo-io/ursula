//! Bounded-state cold hygiene on the in-memory engine: F14b
//! `DeferColdGc` in the GC worker, F14i retention grace for dropped pack
//! slices, the `FlushCold` incarnation check, apply-time cold-index page
//! invalidation, and the G1 guard on what a cold GC entry deletes.

use std::sync::Arc;

use bytes::Bytes;
use ursula_shard::BucketStreamId;
use ursula_shard::RaftGroupId;
use ursula_stream::ColdChunkRef;
use ursula_stream::ColdGcEntry;
use ursula_stream::ColdGcTarget;
use ursula_stream::ExternalPayloadRef;
use ursula_stream::StreamCommand;
use ursula_stream::StreamReadColdIndexSegment;

use crate::AdvanceRetentionRequest;
use crate::AppendExternalRequest;
use crate::AppendRequest;
use crate::ColdSnapshotBody;
use crate::ColdStore;
use crate::ColdStoreFaultEffect;
use crate::ColdStoreOperation;
use crate::CreateStreamRequest;
use crate::DeleteStreamRequest;
use crate::FollowerRead;
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
use crate::cold_store::check_cold_gc_path;
use crate::cold_store::cold_chunk_dir;
use crate::cold_store::new_cold_chunk_path_in_generation;
use crate::cold_store::new_cold_pack_path;
use crate::cold_store::new_external_payload_path;

fn spawn(cold_store: Arc<ColdStore>) -> ShardRuntime {
    ShardRuntime::spawn_with_engine_factory_and_cold_store(
        RuntimeConfig::new(1, 4),
        InMemoryGroupEngineFactory::with_cold_store(Some(cold_store.clone())),
        Some(cold_store),
    )
    .expect("spawn runtime")
}

fn stream_on_group(runtime: &ShardRuntime, group: RaftGroupId, prefix: &str) -> BucketStreamId {
    (0..10_000)
        .map(|index| BucketStreamId::new("coldhygiene", format!("{prefix}-{index}")))
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
            now_ms: 0,
            if_incarnation: None,
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
            leader_only: false,
            read_index: None,
            follower: FollowerRead::Prefix,
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

/// G1: every path the group's pending `Paths` entries name passes the
/// guard at the delete site. Returns how many paths it checked.
async fn assert_pending_paths_pass_the_guard(runtime: &ShardRuntime, group: RaftGroupId) -> usize {
    let mut checked = 0_usize;
    for entry in pending_gc(runtime, group).await {
        let ColdGcTarget::Paths(paths) = &entry.target else {
            continue;
        };
        for path in paths {
            let refusal = check_cold_gc_path(group, &entry, path, |stream| {
                runtime.locate(stream).raft_group_id
            })
            .err();
            assert_eq!(refusal, None, "a producer queued a path the guard refuses");
            checked = checked.saturating_add(1);
        }
    }
    checked
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
async fn gc_with_failing_head() -> (
    Arc<ColdStore>,
    ShardRuntime,
    RaftGroupId,
    [BucketStreamId; 2],
) {
    let cold_store = Arc::new(ColdStore::memory().expect("memory cold store"));
    let runtime = spawn(cold_store.clone());
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
async fn f14b_failing_gc_head_is_deferred_and_the_rest_drains() {
    let (cold_store, runtime, group, streams) = gc_with_failing_head().await;
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
    assert_eq!(after[0].defer_attempts, 1, "the deferral counts an attempt");
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
                max_hot_age: None,
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
async fn retain_past_last_pack_reference() -> (Arc<ColdStore>, String) {
    let cold_store = Arc::new(ColdStore::memory().expect("memory cold store"));
    let runtime = spawn(cold_store.clone());
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
                max_hot_age: None,
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
            cold_body: None,
            now_ms: crate::runtime::unix_time_ms(),
            expected_incarnation: None,
            if_incarnation: None,
        })
        .await
        .expect("publish checkpoint");
    runtime
        .advance_retention(AdvanceRetentionRequest {
            stream_id: streams[1].clone(),
            retained_offset: 4,
            now_ms: crate::runtime::unix_time_ms(),
            expected_incarnation: None,
            if_incarnation: None,
        })
        .await
        .expect("advance retention");
    // The released pack's entry (`release_shared_cold_objects`).
    assert!(assert_pending_paths_pass_the_guard(&runtime, group).await >= 1);
    // The deleted stream's own entry needs no pack; reclaim what is due.
    runtime
        .run_cold_gc_group_once(group, 16)
        .await
        .expect("cold gc pass");
    (cold_store, pack_path)
}

/// RC-19 / D6: a read planned before a concurrent retention still finds the
/// pack bytes, because GC waits for the grace.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn f14i_retention_keeps_dropped_pack_slices_for_the_grace() {
    let (cold_store, pack_path) = retain_past_last_pack_reference().await;
    assert!(object_exists(&cold_store, &pack_path).await);
}

/// #475: retention past an external payload still held in state (not offloaded to a page yet)
/// queues its object, and the GC deletes it once the retention grace has passed (retention applies
/// at time 0 here, so the grace is long over). Before, nothing deleted it until the orphan sweep's
/// one-day grace.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn retention_reclaims_external_payloads_still_held_in_state() {
    let cold_store = Arc::new(ColdStore::memory().expect("memory cold store"));
    let runtime = spawn(cold_store.clone());
    let group = RaftGroupId(1);
    let stream = stream_on_group(&runtime, group, "external");
    create(&runtime, &stream).await;
    let [below, above] = [(), ()].map(|()| new_external_payload_path(&stream));
    for path in [&below, &above] {
        cold_store
            .write_chunk(path, b"xyz")
            .await
            .expect("stage external payload");
        runtime
            .append_external(AppendExternalRequest {
                stream_id: stream.clone(),
                content_type: DEFAULT_CONTENT_TYPE.to_owned(),
                payload: ExternalPayloadRef {
                    s3_path: path.clone(),
                    payload_len: 3,
                    object_size: 3,
                },
                message_count: 0,
                close_after: false,
                stream_seq: None,
                producer: None,
                now_ms: 0,
                if_incarnation: None,
                upload: crate::ProposedUpload::new(crate::UploadFence::Unreplicated),
            })
            .await
            .expect("append external");
    }
    runtime
        .publish_snapshot(PublishSnapshotRequest {
            stream_id: stream.clone(),
            snapshot_offset: 3,
            content_type: DEFAULT_CONTENT_TYPE.to_owned(),
            payload: Bytes::from_static(b"state"),
            cold_body: None,
            now_ms: 0,
            expected_incarnation: None,
            if_incarnation: None,
        })
        .await
        .expect("publish checkpoint");
    runtime
        .advance_retention(AdvanceRetentionRequest {
            stream_id: stream.clone(),
            retained_offset: 3,
            now_ms: 0,
            expected_incarnation: None,
            if_incarnation: None,
        })
        .await
        .expect("advance retention");
    assert!(
        pending_gc(&runtime, group)
            .await
            .iter()
            .any(|entry| entry.target == ColdGcTarget::Paths(vec![below.clone()]))
    );
    assert!(assert_pending_paths_pass_the_guard(&runtime, group).await >= 1);
    runtime
        .run_cold_gc_group_once(group, 16)
        .await
        .expect("cold gc pass");
    assert!(!object_exists(&cold_store, &below).await);
    assert!(object_exists(&cold_store, &above).await);
}

/// Wave-1 follow-up: a replica's cached cold-index page is dropped when a
/// `FlushCold` over its range applies, so entries a leader-side clip removed
/// are not served from the cache afterwards.
#[tokio::test]
async fn applying_flush_cold_drops_cached_pages_of_the_flushed_range() {
    let cold_store = Arc::new(ColdStore::memory().expect("memory cold store"));
    let mut engine = InMemoryGroupEngine::started_with_cold_store(cold_store.clone());
    let placement = ursula_shard::ShardPlacement {
        core_id: ursula_shard::CoreId(0),
        shard_id: ursula_shard::ShardId(0),
        raft_group_id: RaftGroupId(0),
    };
    let stream = BucketStreamId::new("coldhygiene", "cached");
    // Creating the stream creates its bucket.
    let create = StreamCommand::CreateStream {
        stream_id: stream.clone(),
        content_type: DEFAULT_CONTENT_TYPE.to_owned(),
        initial_payload: Bytes::from_static(b"abcd"),
        close_after: false,
        stream_seq: None,
        stream_ttl_seconds: None,
        stream_expires_at_ms: None,
        now_ms: 0,
    };
    engine
        .apply_committed_write(create, placement)
        .expect("create stream");
    // C7/F14g: the group's first incarnation is `created_at_ms` 1, which is
    // the generation its pages live under.
    let generation = 1;
    // A stale entry for `[0, 2)` sits in the stream's page and in the cache.
    let store = ColdStoreColdIndexPageStore::new(cold_store.clone());
    let stale = ColdChunkRef {
        start_offset: 0,
        end_offset: 2,
        s3_path: "coldhygiene/cached/chunks/stale.bin".to_owned(),
        object_size: 2,
        ..Default::default()
    };
    write_cold_chunk_index_pages_with_rollback_in_generation(&store, &stream, generation, &stale)
        .await
        .expect("write stale entry");
    let cache = engine.cold_index_cache().expect("page cache");
    cache
        .object_segments_for_read(&stream, &StreamReadColdIndexSegment {
            generation,
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
            StreamCommand::FlushCold {
                stream_id: stream.clone(),
                chunk: ColdChunkRef {
                    start_offset: 0,
                    end_offset: 4,
                    s3_path: "coldhygiene/cached/chunks/flushed.bin".to_owned(),
                    object_size: 4,
                    ..Default::default()
                },
                cold_generation: generation,
            },
            placement,
        )
        .expect("apply flush");
    assert_eq!(cache.cached_page_count(), 0);
}

/// F14b: a failing GC entry backs off exponentially in its replicated
/// attempt counter, from one minute up to one hour.
#[test]
fn f14b_gc_defer_backoff_doubles_per_attempt_up_to_an_hour() {
    use crate::runtime::cold_gc_defer_backoff_ms;
    assert_eq!(cold_gc_defer_backoff_ms(0), 60_000);
    assert_eq!(cold_gc_defer_backoff_ms(1), 120_000);
    assert_eq!(cold_gc_defer_backoff_ms(5), 60_000 * 32);
    assert_eq!(cold_gc_defer_backoff_ms(6), 60 * 60 * 1_000);
    assert_eq!(cold_gc_defer_backoff_ms(u32::MAX), 60 * 60 * 1_000);
}

/// G1: a `Paths` entry as a corrupt or hand-edited backup could restore it,
/// mixing legal paths with a snapshot key, a page, another bucket's object,
/// another group's pack, the object of a stream placed in another group and
/// a malformed path. The worker deletes only the legal ones, counts each
/// refusal and acknowledges the entry, so the queue does not stall.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn g1_cold_gc_deletes_only_the_legal_paths_of_an_entry_and_acks_it() {
    let cold_store = Arc::new(ColdStore::memory().expect("memory cold store"));
    let runtime = spawn(cold_store.clone());
    let group = RaftGroupId(1);
    let stream = stream_on_group(&runtime, group, "guarded");
    let elsewhere = stream_on_group(&runtime, RaftGroupId(2), "elsewhere");
    create(&runtime, &stream).await;
    let legal = [
        new_cold_chunk_path_in_generation(&stream, 1, 0, 4),
        new_cold_pack_path(&stream.bucket_id, group.0),
        new_external_payload_path(&stream),
    ];
    let foreign = [
        format!(
            "snapshots/node-1/group-{}/00000000000000000042-0123456789abcdef.snap",
            group.0
        ),
        ColdIndexPageKey {
            stream_id: stream.clone(),
            generation: 1,
            page_id: 0,
        }
        .path(),
        new_external_payload_path(&BucketStreamId::new(
            "otherbucket",
            stream.stream_id.clone(),
        )),
        new_cold_pack_path(&stream.bucket_id, 2),
        new_external_payload_path(&elsewhere),
        format!("{stream}/chunks/1/not-a-chunk.bin"),
    ];
    for path in legal.iter().chain(&foreign) {
        cold_store
            .write_chunk(path, b"object")
            .await
            .expect("write object");
    }
    let mut snapshot = runtime.snapshot_group(group).await.expect("snapshot group");
    assert!(snapshot.stream_snapshot.pending_cold_gc.is_empty());
    let seq = snapshot.stream_snapshot.next_cold_gc_seq;
    let mut paths = Vec::new();
    for (index, path) in foreign.iter().enumerate() {
        paths.push(path.clone());
        if let Some(legal) = legal.get(index) {
            paths.push(legal.clone());
        }
    }
    snapshot.stream_snapshot.pending_cold_gc.push(ColdGcEntry {
        seq,
        bucket_id: stream.bucket_id.clone(),
        not_before_ms: 0,
        target: ColdGcTarget::Paths(paths),
        cold_generation: None,
        defer_attempts: 0,
    });
    snapshot.stream_snapshot.next_cold_gc_seq = seq.checked_add(1).expect("next seq");
    runtime
        .install_group_snapshot(snapshot)
        .await
        .expect("install the edited snapshot");

    assert_eq!(
        runtime
            .run_cold_gc_group_once(group, 16)
            .await
            .expect("cold gc pass"),
        1
    );
    for path in &legal {
        assert!(!object_exists(&cold_store, path).await, "kept {path}");
    }
    for path in &foreign {
        assert!(object_exists(&cold_store, path).await, "deleted {path}");
    }
    let metrics = runtime.metrics().snapshot();
    assert_eq!(
        metrics.cold_gc_foreign_paths_refused,
        u64::try_from(foreign.len()).expect("count fits u64")
    );
    assert_eq!(metrics.cold_gc_errors, 0);
    assert_eq!(metrics.cold_gc_reclaimed, 1);
    assert!(
        pending_gc(&runtime, group).await.is_empty(),
        "the entry is acknowledged"
    );
}

/// G1: the stream snapshot body producers (an idempotent repeat's unused
/// body, a superseded body, the body of a deleted stream) and a deleted
/// stream's external payloads queue only paths the guard accepts, and the
/// worker deletes them.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn g1_snapshot_body_and_delete_producers_queue_only_accepted_paths() {
    let cold_store = Arc::new(ColdStore::memory().expect("memory cold store"));
    let runtime = spawn(cold_store.clone());
    let group = RaftGroupId(2);
    let stream = stream_on_group(&runtime, group, "bodies");
    create(&runtime, &stream).await;
    let external = new_external_payload_path(&stream);
    cold_store
        .write_chunk(&external, b"xyz")
        .await
        .expect("stage external payload");
    runtime
        .append_external(AppendExternalRequest {
            stream_id: stream.clone(),
            content_type: DEFAULT_CONTENT_TYPE.to_owned(),
            payload: ExternalPayloadRef {
                s3_path: external.clone(),
                payload_len: 3,
                object_size: 3,
            },
            message_count: 0,
            close_after: false,
            stream_seq: None,
            producer: None,
            now_ms: 0,
            if_incarnation: None,
            upload: crate::ProposedUpload::new(crate::UploadFence::Unreplicated),
        })
        .await
        .expect("append external");
    append(&runtime, &stream, b"ab").await;
    let mut bodies = Vec::new();
    // The first body, an idempotent repeat of it with another object, and
    // a later body that supersedes the first.
    for (offset, digest) in [(3, "first"), (3, "first"), (5, "second")] {
        let path = new_external_payload_path(&stream);
        cold_store
            .write_chunk(&path, b"state")
            .await
            .expect("stage snapshot body");
        runtime
            .publish_snapshot(PublishSnapshotRequest {
                stream_id: stream.clone(),
                snapshot_offset: offset,
                content_type: DEFAULT_CONTENT_TYPE.to_owned(),
                payload: Bytes::new(),
                cold_body: Some(ColdSnapshotBody {
                    object: ExternalPayloadRef {
                        s3_path: path.clone(),
                        payload_len: 5,
                        object_size: 5,
                    },
                    digest: digest.to_owned(),
                    upload: crate::ProposedUpload::new(crate::UploadFence::Unreplicated),
                }),
                now_ms: 0,
                expected_incarnation: None,
                if_incarnation: None,
            })
            .await
            .expect("publish snapshot body");
        bodies.push(path);
    }
    delete(&runtime, &stream).await;
    assert!(assert_pending_paths_pass_the_guard(&runtime, group).await >= 4);

    // The retention grace of the repeat and the superseded body started at
    // time 0, so every entry is due.
    runtime
        .run_cold_gc_group_once(group, 16)
        .await
        .expect("cold gc pass");
    for path in bodies.iter().chain([&external]) {
        assert!(!object_exists(&cold_store, path).await, "kept {path}");
    }
    assert_eq!(
        runtime.metrics().snapshot().cold_gc_foreign_paths_refused,
        0
    );
}

/// G1: a `Stream` entry as a corrupt backup could restore it into group 1,
/// naming a stream of group 2 at its live generation, once under the
/// stream's own bucket and once under another bucket. Group 1's state does
/// not hold the stream, so its live generation does not protect it there.
/// The worker refuses both entries, deletes none of the stream's chunks or
/// pages, counts each refusal, and acknowledges the entries without
/// counting them as reclaimed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn g1_cold_gc_refuses_a_stream_entry_for_another_group_or_bucket() {
    let cold_store = Arc::new(ColdStore::memory().expect("memory cold store"));
    let runtime = spawn(cold_store.clone());
    let group = RaftGroupId(1);
    let owner = RaftGroupId(2);
    let victim = stream_on_group(&runtime, owner, "victim");
    create(&runtime, &victim).await;
    append(&runtime, &victim, b"abcd").await;
    flush_exclusive(&runtime, &victim, 4).await;
    let generation = runtime
        .snapshot_group(owner)
        .await
        .expect("snapshot owner")
        .stream_snapshot
        .streams
        .into_iter()
        .find(|entry| entry.metadata.stream_id == victim)
        .expect("victim entry")
        .cold_index_generation;
    let page = ColdIndexPageKey {
        stream_id: victim.clone(),
        generation,
        page_id: 0,
    };
    let store = ColdStoreColdIndexPageStore::new(cold_store.clone());
    let chunks = load_cold_chunks_from_pages(&store, std::slice::from_ref(&page))
        .await
        .expect("load pages");
    assert_eq!(chunks.len(), 1);

    let mut snapshot = runtime.snapshot_group(group).await.expect("snapshot group");
    assert!(snapshot.stream_snapshot.pending_cold_gc.is_empty());
    let first = snapshot.stream_snapshot.next_cold_gc_seq;
    let mut seq = first;
    for bucket_id in [victim.bucket_id.clone(), "otherbucket".to_owned()] {
        snapshot.stream_snapshot.pending_cold_gc.push(ColdGcEntry {
            seq,
            bucket_id,
            not_before_ms: 0,
            target: ColdGcTarget::Stream(victim.clone()),
            cold_generation: Some(generation),
            defer_attempts: 0,
        });
        seq = seq.checked_add(1).expect("next seq");
    }
    snapshot.stream_snapshot.next_cold_gc_seq = seq;
    runtime
        .install_group_snapshot(snapshot)
        .await
        .expect("install the edited snapshot");

    assert_eq!(
        runtime
            .run_cold_gc_group_once(group, 16)
            .await
            .expect("cold gc pass"),
        0
    );
    for chunk in &chunks {
        assert!(
            object_exists(&cold_store, &chunk.s3_path).await,
            "deleted {}",
            chunk.s3_path
        );
    }
    assert!(
        object_exists(&cold_store, &page.path()).await,
        "deleted the page"
    );
    assert_eq!(read(&runtime, &victim, 16).await, b"abcd");
    let metrics = runtime.metrics().snapshot();
    assert_eq!(metrics.cold_gc_foreign_paths_refused, 2);
    assert_eq!(metrics.cold_gc_errors, 0);
    assert_eq!(metrics.cold_gc_reclaimed, 0);
    assert!(
        pending_gc(&runtime, group).await.is_empty(),
        "the entries are acknowledged"
    );
}

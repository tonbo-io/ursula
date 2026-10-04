//! Tests for the leader-side cold-reference drivers on the in-memory engine:
//! the shared pack-reference compaction driver (bounded-stream-state F2) and
//! the cold orphan sweep (F14h). The Raft engine runs the same scenarios in
//! `ursula-raft`'s `engine::compact_tests`.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use ursula_shard::BucketStreamId;
use ursula_shard::RaftGroupId;
use ursula_stream::ExternalPayloadRef;

use crate::AdvanceRetentionRequest;
use crate::AppendExternalRequest;
use crate::AppendRequest;
use crate::ColdStore;
use crate::ColdStoreFaultEffect;
use crate::ColdStoreOperation;
use crate::CreateStreamRequest;
use crate::InMemoryGroupEngineFactory;
use crate::PlanGroupColdFlushRequest;
use crate::PublishSnapshotRequest;
use crate::ReadStreamRequest;
use crate::RuntimeConfig;
use crate::ShardRuntime;
use crate::SharedRefCompactionConfig;
use crate::cold_store::DEFAULT_CONTENT_TYPE;
use crate::cold_store::cold_pack_dir;
use crate::cold_store::new_cold_chunk_path_in_generation;
use crate::cold_store::new_cold_pack_path;
use crate::cold_store::new_external_payload_path;

const GROUP: RaftGroupId = RaftGroupId(0);
const BUCKET: &str = "benchcmp";

fn spawn(cold_store: Arc<ColdStore>) -> ShardRuntime {
    ShardRuntime::spawn_with_engine_factory_and_cold_store(
        RuntimeConfig::new(1, 1),
        InMemoryGroupEngineFactory::with_cold_store(Some(cold_store.clone())),
        Some(cold_store),
    )
    .expect("spawn runtime")
}

fn stream(name: &str) -> BucketStreamId {
    BucketStreamId::new(BUCKET, name)
}

async fn create(runtime: &ShardRuntime, stream_id: &BucketStreamId) {
    runtime
        .create_stream(CreateStreamRequest::new(
            stream_id.clone(),
            DEFAULT_CONTENT_TYPE,
        ))
        .await
        .expect("create stream");
}

async fn append(runtime: &ShardRuntime, stream_id: &BucketStreamId, payload: &[u8]) {
    runtime
        .append(AppendRequest::from_bytes(
            stream_id.clone(),
            payload.to_vec(),
        ))
        .await
        .expect("append");
}

/// One packed flush pass over every hot stream of the group.
async fn pack_flush(runtime: &ShardRuntime) -> usize {
    runtime
        .flush_cold_group_batch_once(
            GROUP,
            PlanGroupColdFlushRequest {
                min_hot_bytes: 1,
                max_flush_bytes: 1 << 20,
                max_batch_bytes: 1 << 20,
                pressure: None,
                max_hot_age: None,
            },
            64,
        )
        .await
        .expect("packed flush")
        .len()
}

async fn read_all(runtime: &ShardRuntime, stream_id: &BucketStreamId) -> Vec<u8> {
    runtime
        .read_stream(ReadStreamRequest {
            stream_id: stream_id.clone(),
            offset: 0,
            max_len: 1 << 20,
            now_ms: 0,
            record: None,
            max_records: None,
            leader_only: false,
            record_anchor: None,
            read_index: None,
        })
        .await
        .expect("read stream")
        .payload
}

async fn exists(cold_store: &ColdStore, path: &str) -> bool {
    cold_store.object_size(path).await.is_ok()
}

fn driver(min_refs: usize, gc_grace_ms: u64) -> SharedRefCompactionConfig {
    SharedRefCompactionConfig {
        min_refs,
        ..SharedRefCompactionConfig::new(16 << 20, 16, gc_grace_ms)
    }
}

/// Two streams trickling into packed flushes accumulate one shared ref per
/// pass each. Below the threshold (and not idle) the driver leaves them;
/// at the threshold it rewrites each stream's run into one exclusive chunk,
/// reads stay byte-identical, and once the grace passes GC deletes every
/// pack the run released.
#[tokio::test]
async fn f2_driver_compacts_runs_at_the_threshold_and_packs_are_gced() {
    let cold_store = Arc::new(ColdStore::memory().expect("memory cold store"));
    let runtime = spawn(cold_store.clone());
    let (a, b) = (stream("trickle-a"), stream("trickle-b"));
    create(&runtime, &a).await;
    create(&runtime, &b).await;
    let mut expected_a = Vec::new();
    let mut expected_b = Vec::new();
    for round in 0..5_u8 {
        let payload_a = [b'a', b'0' + round];
        let payload_b = [b'b', b'0' + round, b'!'];
        append(&runtime, &a, &payload_a).await;
        append(&runtime, &b, &payload_b).await;
        expected_a.extend_from_slice(&payload_a);
        expected_b.extend_from_slice(&payload_b);
        assert_eq!(pack_flush(&runtime).await, 2);
    }
    let gauges = runtime.state_gauges(GROUP).await.expect("gauges");
    assert_eq!(gauges.shared_refs, 10);
    assert_eq!(gauges.live_packs, 5);

    let report = runtime
        .compact_shared_refs_group_once(GROUP, &driver(64, 0))
        .await
        .expect("driver pass below the threshold");
    assert_eq!(report.candidates, 0, "five refs are below T and not idle");

    let report = runtime
        .compact_shared_refs_group_once(GROUP, &driver(5, 0))
        .await
        .expect("driver pass at the threshold");
    assert_eq!(report.compacted_streams, 2);
    assert_eq!(report.compacted_slices, 10);
    let gauges = runtime.state_gauges(GROUP).await.expect("gauges");
    assert_eq!(gauges.shared_refs, 0);
    assert_eq!(gauges.live_packs, 0);
    assert_eq!(gauges.pending_cold_gc, 5, "each released pack is queued");
    assert_eq!(read_all(&runtime, &a).await, expected_a);
    assert_eq!(read_all(&runtime, &b).await, expected_b);

    let pack_dir = cold_pack_dir(BUCKET, GROUP.0);
    assert_eq!(
        cold_store.list_file_names(&pack_dir).await.unwrap().len(),
        5
    );
    runtime
        .run_cold_gc_all_groups_once(256)
        .await
        .expect("cold gc");
    assert!(
        cold_store
            .list_file_names(&pack_dir)
            .await
            .unwrap()
            .is_empty(),
        "packs are deleted once their last ref is compacted away"
    );
    assert_eq!(read_all(&runtime, &a).await, expected_a);
    assert_eq!(read_all(&runtime, &b).await, expected_b);
    assert_eq!(
        runtime.state_gauges(GROUP).await.unwrap().pending_cold_gc,
        0
    );
}

/// A run larger than `compaction_max_size` is compacted in several passes,
/// oldest first, and every pass reads correctly.
#[tokio::test]
async fn f2_driver_caps_each_run_at_the_max_size() {
    let cold_store = Arc::new(ColdStore::memory().expect("memory cold store"));
    let runtime = spawn(cold_store.clone());
    let (a, b) = (stream("cap-a"), stream("cap-b"));
    create(&runtime, &a).await;
    create(&runtime, &b).await;
    for _ in 0..4 {
        append(&runtime, &a, b"0123").await;
        append(&runtime, &b, b"z").await;
        pack_flush(&runtime).await;
    }
    let config = SharedRefCompactionConfig {
        max_run_bytes: 8,
        max_streams: 1,
        ..driver(1, 0)
    };
    let mut passes = 0;
    while runtime
        .state_gauges(GROUP)
        .await
        .unwrap()
        .max_shared_refs_per_stream
        > 1
    {
        let report = runtime
            .compact_shared_refs_group_once(GROUP, &config)
            .await
            .expect("driver pass");
        assert_eq!(report.compacted_streams, 1);
        assert!(report.compacted_bytes <= 8);
        passes += 1;
        assert!(passes < 10, "the driver makes progress");
    }
    assert_eq!(read_all(&runtime, &a).await, b"0123012301230123".to_vec());
}

/// Concurrent retention drops the planned refs before `CompactCold` applies:
/// the compaction is rejected with a typed error, the engine rolls the page
/// entry back, and the driver deletes its replacement chunk.
#[tokio::test]
async fn f2_driver_deletes_the_replacement_of_a_rejected_compaction() {
    let cold_store = Arc::new(ColdStore::memory().expect("memory cold store"));
    let runtime = spawn(cold_store.clone());
    let (a, b) = (stream("reject-a"), stream("reject-b"));
    create(&runtime, &a).await;
    create(&runtime, &b).await;
    for _ in 0..3 {
        append(&runtime, &a, b"aaaa").await;
        append(&runtime, &b, b"bb").await;
        pack_flush(&runtime).await;
    }
    append(&runtime, &a, b"tail").await;

    // While the driver writes `a`'s replacement, retention moves past `a`'s
    // packed history.
    let retention_runtime = runtime.clone();
    let retained = a.clone();
    cold_store.set_delay_fn(move |_| {
        let runtime = retention_runtime.clone();
        let stream_id = retained.clone();
        async move {
            runtime
                .publish_snapshot(PublishSnapshotRequest {
                    stream_id: stream_id.clone(),
                    snapshot_offset: 12,
                    content_type: DEFAULT_CONTENT_TYPE.to_owned(),
                    payload: Bytes::from_static(b"checkpoint"),
                    cold_body: None,
                    now_ms: 0,
                })
                .await
                .expect("publish checkpoint");
            runtime
                .advance_retention(AdvanceRetentionRequest {
                    stream_id,
                    retained_offset: 12,
                    now_ms: 0,
                })
                .await
                .expect("advance retention");
        }
    });
    let chunk_dir = crate::cold_store::cold_chunk_dir(&a, 0);
    let watched_dir = chunk_dir.clone();
    cold_store.set_fault_policy(move |context| {
        (context.operation == ColdStoreOperation::WriteChunk
            && context.path.starts_with(&watched_dir))
        .then(|| ColdStoreFaultEffect::delay(Duration::from_millis(1)))
    });

    let config = SharedRefCompactionConfig {
        max_streams: 1,
        ..driver(3, 0)
    };
    let report = runtime
        .compact_shared_refs_group_once(GROUP, &config)
        .await
        .expect("driver pass");
    cold_store.clear_fault_policy();
    assert_eq!(report.candidates, 1);
    assert_eq!(report.rejected, 1);
    assert_eq!(report.compacted_streams, 0);
    assert!(
        cold_store
            .list_file_names(&chunk_dir)
            .await
            .unwrap()
            .is_empty(),
        "the rejected replacement is deleted"
    );
    let read = runtime
        .read_stream(ReadStreamRequest {
            stream_id: a.clone(),
            offset: 12,
            max_len: 64,
            now_ms: 0,
            record: None,
            max_records: None,
            leader_only: false,
            record_anchor: None,
            read_index: None,
        })
        .await
        .expect("read retained suffix");
    assert_eq!(read.payload, b"tail");
}

/// D3 under F2: an external append that apply rejects leaves a page entry
/// over offsets that a packed trickle later fills. While shared refs cover
/// the range they shadow the stale entry; compaction turns the refs into a
/// page entry, so the driver must repair and clip, and reads must return
/// the trickled bytes, not the rejected object's.
#[tokio::test]
async fn f2_rejected_external_append_under_a_packed_trickle_reads_correctly_after_compaction() {
    let cold_store = Arc::new(ColdStore::memory().expect("memory cold store"));
    let runtime = spawn(cold_store.clone());
    let (s, t) = (stream("d3-trickle"), stream("d3-companion"));
    create(&runtime, &s).await;
    create(&runtime, &t).await;
    let mut request = AppendRequest::from_bytes(s.clone(), b"ab".to_vec());
    request.stream_seq = Some("5".to_owned());
    runtime
        .append(request)
        .await
        .expect("append with stream seq");

    let rejected = new_external_payload_path(&s);
    cold_store
        .write_chunk(&rejected, &[b'#'; 30])
        .await
        .expect("stage rejected payload");
    runtime
        .append_external(AppendExternalRequest {
            stream_id: s.clone(),
            content_type: DEFAULT_CONTENT_TYPE.to_owned(),
            payload: ExternalPayloadRef {
                s3_path: rejected.clone(),
                payload_len: 30,
                object_size: 30,
            },
            record_ends: Vec::new(),
            close_after: false,
            stream_seq: Some("1".to_owned()),
            producer: None,
            now_ms: 0,
            record_match: None,
        })
        .await
        .expect_err("a regressed stream seq rejects the external append");

    let mut expected = b"ab".to_vec();
    for round in 0..6_u8 {
        let payload = [b'c' + round, b'C' + round];
        append(&runtime, &s, &payload).await;
        append(&runtime, &t, b"t").await;
        expected.extend_from_slice(&payload);
        assert_eq!(pack_flush(&runtime).await, 2);
    }
    // A committed external append past the trickle is served from pages,
    // where the rejected entry (which spans it) also lives.
    let committed = new_external_payload_path(&s);
    cold_store
        .write_chunk(&committed, b"WXYZ")
        .await
        .expect("stage committed payload");
    runtime
        .append_external(AppendExternalRequest {
            stream_id: s.clone(),
            content_type: DEFAULT_CONTENT_TYPE.to_owned(),
            payload: ExternalPayloadRef {
                s3_path: committed,
                payload_len: 4,
                object_size: 4,
            },
            record_ends: Vec::new(),
            close_after: false,
            stream_seq: None,
            producer: None,
            now_ms: 0,
            record_match: None,
        })
        .await
        .expect("committed external append");
    expected.extend_from_slice(b"WXYZ");
    append(&runtime, &s, b"hot").await;
    expected.extend_from_slice(b"hot");

    let report = runtime
        .compact_shared_refs_group_once(GROUP, &driver(2, 0))
        .await
        .expect("driver pass");
    assert_eq!(report.compacted_streams, 2);
    assert_eq!(
        runtime.state_gauges(GROUP).await.unwrap().shared_refs,
        0,
        "every ref is now a page entry"
    );
    assert_eq!(read_all(&runtime, &s).await, expected);
    runtime
        .run_cold_gc_all_groups_once(256)
        .await
        .expect("cold gc");
    assert_eq!(read_all(&runtime, &s).await, expected);
}

/// F14h: objects that ambiguous publishes left behind (a pack, a chunk and
/// a staged external payload that nothing references) are deleted once they
/// are older than the grace; live packs, page-referenced chunks and
/// externals, and packs pending GC under their own grace are never touched.
#[tokio::test]
async fn f14h_orphan_sweep_reclaims_only_unreferenced_objects_after_the_grace() {
    let cold_store = Arc::new(ColdStore::memory().expect("memory cold store"));
    let runtime = spawn(cold_store.clone());
    let (a, b) = (stream("sweep-a"), stream("sweep-b"));
    create(&runtime, &a).await;
    create(&runtime, &b).await;

    // Referenced: two packs, one of them released to GC with a long grace by
    // compacting `b`; an exclusive chunk in `a`'s pages; a committed external.
    append(&runtime, &a, b"a1").await;
    append(&runtime, &b, b"b1").await;
    pack_flush(&runtime).await;
    let report = runtime
        .compact_shared_refs_group_once(GROUP, &SharedRefCompactionConfig {
            max_streams: 16,
            ..driver(1, 60 * 60 * 1_000)
        })
        .await
        .expect("compact both streams");
    assert_eq!(report.compacted_streams, 2);
    append(&runtime, &a, b"a2").await;
    append(&runtime, &b, b"b2").await;
    pack_flush(&runtime).await;
    let committed = new_external_payload_path(&a);
    cold_store
        .write_chunk(&committed, b"EXTERNAL")
        .await
        .expect("stage committed payload");
    runtime
        .append_external(AppendExternalRequest {
            stream_id: a.clone(),
            content_type: DEFAULT_CONTENT_TYPE.to_owned(),
            payload: ExternalPayloadRef {
                s3_path: committed.clone(),
                payload_len: 8,
                object_size: 8,
            },
            record_ends: Vec::new(),
            close_after: false,
            stream_seq: None,
            producer: None,
            now_ms: 0,
            record_match: None,
        })
        .await
        .expect("committed external append");

    // Orphans of ambiguous publishes.
    let orphan_pack = new_cold_pack_path(BUCKET, GROUP.0);
    let orphan_chunk = new_cold_chunk_path_in_generation(&a, 0, 0, 2);
    let orphan_external = new_external_payload_path(&b);
    for (path, payload) in [
        (&orphan_pack, b"orphan-pack".as_slice()),
        (&orphan_chunk, b"oc".as_slice()),
        (&orphan_external, b"orphan-external".as_slice()),
    ] {
        cold_store
            .write_chunk(path, payload)
            .await
            .expect("inject orphan");
    }

    let pack_dir = cold_pack_dir(BUCKET, GROUP.0);
    let mut before = cold_store.list_file_names(&pack_dir).await.unwrap();
    before.retain(|name| !orphan_pack.ends_with(name.as_str()));
    let referenced_packs = before
        .iter()
        .map(|name| format!("{pack_dir}{name}"))
        .collect::<Vec<_>>();
    assert_eq!(referenced_packs.len(), 2, "one live pack, one pending GC");
    let chunk_dir = crate::cold_store::cold_chunk_dir(&a, 0);
    let referenced_chunks = cold_store
        .list_file_names(&chunk_dir)
        .await
        .unwrap()
        .into_iter()
        .map(|name| format!("{chunk_dir}{name}"))
        .filter(|path| *path != orphan_chunk)
        .collect::<Vec<_>>();
    assert_eq!(referenced_chunks.len(), 1, "a's compaction replacement");

    let young = runtime
        .sweep_cold_orphans_group_once(GROUP, 16, 60 * 60 * 1_000)
        .await
        .expect("sweep within the grace");
    assert!(young.cycle_completed);
    assert_eq!(young.orphans_deleted, 0, "nothing is older than the grace");
    assert!(young.objects_scanned >= 7);

    tokio::time::sleep(Duration::from_millis(5)).await;
    let swept = runtime
        .sweep_cold_orphans_group_once(GROUP, 16, 0)
        .await
        .expect("sweep after the grace");
    assert_eq!(swept.orphans_deleted, 3);
    assert_eq!(swept.delete_errors, 0);
    assert_eq!(
        swept.orphan_bytes,
        u64::try_from(b"orphan-pack".len() + b"oc".len() + b"orphan-external".len()).unwrap()
    );
    for orphan in [&orphan_pack, &orphan_chunk, &orphan_external] {
        assert!(!exists(&cold_store, orphan).await, "{orphan} is reclaimed");
    }
    for referenced in referenced_packs
        .iter()
        .chain(referenced_chunks.iter())
        .chain(std::iter::once(&committed))
    {
        assert!(
            exists(&cold_store, referenced).await,
            "{referenced} is kept"
        );
    }
    assert_eq!(read_all(&runtime, &a).await, b"a1a2EXTERNAL".to_vec());
    assert_eq!(read_all(&runtime, &b).await, b"b1b2".to_vec());
    let metrics = runtime.metrics().snapshot();
    assert_eq!(metrics.cold_orphan_cleanup_attempts, 3);
    assert_eq!(metrics.cold_orphan_cleanup_errors, 0);
    assert_eq!(metrics.cold_orphan_bytes, swept.orphan_bytes);

    let again = runtime
        .sweep_cold_orphans_group_once(GROUP, 16, 0)
        .await
        .expect("second sweep");
    assert_eq!(again.orphans_deleted, 0, "a second sweep finds nothing");
}

/// RT6: a page read-modify-write by a deposed leader can drop the entry of
/// a committed exclusive chunk. The sweep must not delete that chunk once it
/// is older than the grace: no other referenced object covers its retained
/// range, so it holds the only copy. It is kept and counted as an alert.
#[tokio::test]
async fn rt6_orphan_sweep_keeps_an_unreferenced_chunk_whose_range_nothing_covers() {
    use crate::cold_index::ColdIndexPageKey;
    use crate::cold_index::ColdIndexPageStore;
    use crate::cold_index::ColdStoreColdIndexPageStore;

    let cold_store = Arc::new(ColdStore::memory().expect("memory cold store"));
    let runtime = spawn(cold_store.clone());
    let a = stream("lost-entry");
    create(&runtime, &a).await;
    append(&runtime, &a, b"a1").await;
    pack_flush(&runtime).await;
    runtime
        .compact_shared_refs_group_once(GROUP, &SharedRefCompactionConfig {
            max_streams: 16,
            ..driver(1, 60 * 60 * 1_000)
        })
        .await
        .expect("compact into an exclusive chunk");
    let chunk_dir = crate::cold_store::cold_chunk_dir(&a, 0);
    let chunks = cold_store.list_file_names(&chunk_dir).await.unwrap();
    assert_eq!(chunks.len(), 1, "the compaction replacement");
    let chunk = format!("{chunk_dir}{}", chunks[0]);

    // A deposed leader rewrites the page without the committed entry.
    let pages = ColdStoreColdIndexPageStore::new(cold_store.clone());
    let key = ColdIndexPageKey {
        stream_id: a.clone(),
        generation: 0,
        page_id: 0,
    };
    let mut page = pages.get_page(&key).await.unwrap().expect("page");
    page.cold_chunks.retain(|entry| entry.s3_path != chunk);
    pages.put_page(&key, &page).await.unwrap();

    tokio::time::sleep(Duration::from_millis(5)).await;
    let swept = runtime
        .sweep_cold_orphans_group_once(GROUP, 16, 0)
        .await
        .expect("sweep after the grace");
    assert!(exists(&cold_store, &chunk).await, "the only copy is kept");
    assert_eq!(swept.uncovered_chunks_kept, 1);
    assert_eq!(
        runtime
            .metrics()
            .snapshot()
            .cold_orphan_uncovered_chunks_kept,
        1
    );
}

/// The sweep walks a group's streams in bounded steps and sweeps the pack
/// prefix once per cycle.
#[tokio::test]
async fn f14h_orphan_sweep_walks_streams_in_bounded_steps() {
    let cold_store = Arc::new(ColdStore::memory().expect("memory cold store"));
    let runtime = spawn(cold_store.clone());
    let streams = (0..5)
        .map(|index| stream(&format!("step-{index}")))
        .collect::<Vec<_>>();
    for stream_id in &streams {
        create(&runtime, stream_id).await;
    }
    let orphan = new_external_payload_path(&streams[4]);
    cold_store
        .write_chunk(&orphan, b"x")
        .await
        .expect("inject orphan");
    tokio::time::sleep(Duration::from_millis(5)).await;
    let mut steps = 0;
    let mut deleted = 0;
    loop {
        let step = runtime
            .sweep_cold_orphans_group_once(GROUP, 2, 0)
            .await
            .expect("sweep step");
        steps += 1;
        deleted += step.orphans_deleted;
        assert!(step.streams_scanned <= 2);
        if step.cycle_completed {
            break;
        }
    }
    assert_eq!(steps, 3);
    assert_eq!(deleted, 1);
    assert!(!exists(&cold_store, &orphan).await);
}

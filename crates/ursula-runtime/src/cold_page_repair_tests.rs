//! Regression tests for the bounded-state cold-path correctness defects
//! (D1 regressed cold frontier, D3 stale page entries, F14b GC isolation,
//! F14e stale flushes, F19 page repair).

use std::sync::Arc;

use ursula_shard::BucketStreamId;
use ursula_shard::CoreId;
use ursula_shard::RaftGroupId;
use ursula_shard::ShardId;
use ursula_shard::ShardPlacement;
use ursula_stream::ColdChunkRef;
use ursula_stream::ExternalPayloadRef;
use ursula_stream::ObjectPayloadRef;

use crate::AppendExternalRequest;
use crate::AppendRequest;
use crate::ColdIndexPage;
use crate::ColdIndexPageKey;
use crate::ColdIndexPageStore;
use crate::ColdStore;
use crate::ColdStoreColdIndexPageStore;
use crate::ColdStoreFaultEffect;
use crate::ColdStoreOperation;
use crate::ColdWriteAdmission;
use crate::CreateStreamRequest;
use crate::DeleteStreamRequest;
use crate::FlushColdRequest;
use crate::GroupEngine;
use crate::InMemoryGroupEngine;
use crate::InMemoryGroupEngineFactory;
use crate::PlanColdFlushRequest;
use crate::ReadStreamRequest;
use crate::RuntimeConfig;
use crate::ShardRuntime;
use crate::cold_index::ColdIndexRepairInput;
use crate::cold_index::ColdIndexRepairReport;
use crate::cold_index::repair_cold_index_page;
use crate::cold_store::DEFAULT_CONTENT_TYPE;

fn placement() -> ShardPlacement {
    ShardPlacement {
        core_id: CoreId(0),
        shard_id: ShardId(0),
        raft_group_id: RaftGroupId(0),
    }
}

fn memory_cold_store() -> Arc<ColdStore> {
    Arc::new(ColdStore::memory().expect("memory cold store"))
}

fn read_req(stream_id: BucketStreamId, offset: u64, max_len: usize) -> ReadStreamRequest {
    ReadStreamRequest {
        stream_id,
        offset,
        max_len,
        now_ms: 0,
        leader_only: false,
        read_index: None,
    }
}

fn spawn_with_cold_store(cold_store: Arc<ColdStore>) -> ShardRuntime {
    ShardRuntime::spawn_with_engine_factory_and_cold_store(
        RuntimeConfig::new(1, 1),
        InMemoryGroupEngineFactory::with_cold_store(Some(cold_store.clone())),
        Some(cold_store),
    )
    .expect("spawn runtime")
}

fn append_external_req(
    stream_id: &BucketStreamId,
    s3_path: &str,
    len: u64,
    stream_seq: Option<&str>,
) -> AppendExternalRequest {
    AppendExternalRequest {
        stream_id: stream_id.clone(),
        content_type: DEFAULT_CONTENT_TYPE.to_owned(),
        payload: ExternalPayloadRef {
            s3_path: s3_path.to_owned(),
            payload_len: len,
            object_size: len,
        },
        record_ends: Vec::new(),
        close_after: false,
        stream_seq: stream_seq.map(str::to_owned),
        producer: None,
        now_ms: 0,
    }
}

fn append_req(
    stream_id: &BucketStreamId,
    payload: &[u8],
    stream_seq: Option<&str>,
) -> AppendRequest {
    let mut request = AppendRequest::from_bytes(stream_id.clone(), payload.to_vec());
    request.stream_seq = stream_seq.map(str::to_owned);
    request
}

async fn stage(cold_store: &ColdStore, path: &str, payload: &[u8]) {
    cold_store
        .write_chunk(path, payload)
        .await
        .expect("stage cold object");
}

/// D1 on the in-memory engine through the runtime: hot bytes, then an
/// external append above them, then a flush of the hot prefix regresses the
/// replicated frontier to 2. Reads, snapshot and install must still work.
#[tokio::test]
async fn d1_runtime_reads_and_installs_external_above_a_flushed_hot_prefix() {
    let cold_store = memory_cold_store();
    let runtime = spawn_with_cold_store(cold_store.clone());
    let stream = BucketStreamId::new("benchcmp", "d1-runtime");
    runtime
        .create_stream(CreateStreamRequest::new(
            stream.clone(),
            DEFAULT_CONTENT_TYPE,
        ))
        .await
        .expect("create stream");
    runtime
        .append(append_req(&stream, b"ab", None))
        .await
        .expect("append hot prefix");
    stage(&cold_store, "benchcmp/d1-runtime/external/xyz.bin", b"XYZ").await;
    runtime
        .append_external(append_external_req(
            &stream,
            "benchcmp/d1-runtime/external/xyz.bin",
            3,
            None,
        ))
        .await
        .expect("append external");
    runtime
        .flush_cold_once(PlanColdFlushRequest {
            stream_id: stream.clone(),
            min_hot_bytes: 1,
            max_flush_bytes: 1024,
        })
        .await
        .expect("flush hot prefix")
        .expect("a flush candidate exists");

    let read = runtime
        .read_stream(read_req(stream.clone(), 0, 16))
        .await
        .expect("read across the flushed prefix and the external");
    assert_eq!(read.payload, b"abXYZ");
    assert_eq!(read.next_offset, 5);

    let snapshot = runtime
        .snapshot_group(runtime.locate(&stream).raft_group_id)
        .await
        .expect("snapshot group");
    let target = spawn_with_cold_store(cold_store);
    target
        .install_group_snapshot(snapshot)
        .await
        .expect("install snapshot with a regressed frontier");
    let read = target
        .read_stream(read_req(stream, 2, 16))
        .await
        .expect("read the external after install");
    assert_eq!(read.payload, b"XYZ");
}

/// D3: an external append that apply rejects leaves its pre-proposal page
/// entry behind. A later flush of hot bytes at the same offsets proves that
/// range, so the flush's page write must clip the stale entry; otherwise a
/// later read past the flushed chunk returns the rejected object's bytes.
#[tokio::test]
async fn d3_flush_clips_the_page_entry_of_a_rejected_external_append() {
    let placement = placement();
    let cold_store = memory_cold_store();
    let stream = BucketStreamId::new("benchcmp", "d3-clip");
    let mut engine = InMemoryGroupEngine::with_cold_store(cold_store.clone());
    engine
        .create_stream(
            CreateStreamRequest::new(stream.clone(), DEFAULT_CONTENT_TYPE),
            placement,
            ColdWriteAdmission::default(),
        )
        .await
        .expect("create stream");
    engine
        .append(
            append_req(&stream, b"ab", Some("5")),
            placement,
            ColdWriteAdmission::default(),
        )
        .await
        .expect("append with stream seq");

    stage(
        &cold_store,
        "benchcmp/d3-clip/external/rejected.bin",
        b"0123456789",
    )
    .await;
    engine
        .append_external(
            append_external_req(
                &stream,
                "benchcmp/d3-clip/external/rejected.bin",
                10,
                Some("1"),
            ),
            placement,
        )
        .await
        .expect_err("a regressed stream seq rejects the external append");

    engine
        .append(
            append_req(&stream, b"cdefgh", None),
            placement,
            ColdWriteAdmission::default(),
        )
        .await
        .expect("append hot bytes over the rejected range");
    stage(&cold_store, "benchcmp/d3-clip/chunks/0-8.bin", b"abcdefgh").await;
    engine
        .flush_cold(
            FlushColdRequest {
                cold_generation: engine
                    .state_machine
                    .cold_index_generation(&stream)
                    .expect("live stream"),
                stream_id: stream.clone(),
                chunk: ColdChunkRef {
                    start_offset: 0,
                    end_offset: 8,
                    s3_path: "benchcmp/d3-clip/chunks/0-8.bin".to_owned(),
                    object_size: 8,
                    ..Default::default()
                },
            },
            placement,
        )
        .await
        .expect("flush hot prefix");

    stage(&cold_store, "benchcmp/d3-clip/external/tail.bin", b"WXYZ").await;
    engine
        .append_external(
            append_external_req(&stream, "benchcmp/d3-clip/external/tail.bin", 4, None),
            placement,
        )
        .await
        .expect("append external tail");

    let read = engine
        .read_stream(read_req(stream, 0, 64), placement)
        .await
        .expect("read cold history");
    assert_eq!(read.payload, b"abcdefghWXYZ");
}

/// The repair cursor walks a group in bounded steps and completes a cycle
/// only when it reaches the end of the group's streams.
#[tokio::test]
async fn repair_cursor_walks_streams_in_bounded_steps() {
    let cold_store = memory_cold_store();
    let runtime = spawn_with_cold_store(cold_store);
    for name in ["cursor-a", "cursor-b", "cursor-c"] {
        runtime
            .create_stream(CreateStreamRequest::new(
                BucketStreamId::new("benchcmp", name),
                DEFAULT_CONTENT_TYPE,
            ))
            .await
            .expect("create stream");
    }
    let group = RaftGroupId(0);
    let first = runtime
        .repair_cold_index_group_once(group, 2)
        .await
        .expect("first step");
    assert_eq!(first.report.streams_scanned, 2);
    assert!(!first.cycle_completed);
    let second = runtime
        .repair_cold_index_group_once(group, 2)
        .await
        .expect("second step");
    assert_eq!(second.report.streams_scanned, 1);
    assert!(second.cycle_completed);
}

fn object(start_offset: u64, end_offset: u64, s3_path: &str) -> ObjectPayloadRef {
    ObjectPayloadRef {
        start_offset,
        end_offset,
        s3_path: s3_path.to_owned(),
        object_size: end_offset - start_offset,
        object_offset: 0,
    }
}

/// Each F19 repair rule, with its count.
#[test]
fn page_repair_drops_each_kind_of_stale_external_entry() {
    let stream_id = BucketStreamId::new("benchcmp", "repair-rules");
    let created_at_ms = 10 * 60_000;
    let old_object = format!(
        "benchcmp/repair-rules/external/{:032x}-{:016x}.bin",
        u128::from(created_at_ms - 2 * 60_000) * 1_000_000,
        1
    );
    let fresh_object = format!(
        "benchcmp/repair-rules/external/{:032x}-{:016x}.bin",
        u128::from(created_at_ms) * 1_000_000,
        2
    );
    let mut page = ColdIndexPage {
        start_offset: 0,
        end_offset: ursula_stream::COLD_INDEX_PAGE_SPAN_BYTES,
        cold_chunks: vec![ColdChunkRef {
            start_offset: 0,
            end_offset: 10,
            s3_path: "benchcmp/repair-rules/chunks/0-10.bin".to_owned(),
            object_size: 10,
            ..Default::default()
        }],
        external_segments: vec![
            // Overlaps the chunk entry.
            object(5, 15, "benchcmp/repair-rules/external/a.bin"),
            // Superseded by the later entry at 20.
            object(20, 40, "benchcmp/repair-rules/external/b.bin"),
            object(20, 30, "benchcmp/repair-rules/external/c.bin"),
            // Overlaps another object's state ref at [30, 40).
            object(30, 45, "benchcmp/repair-rules/external/d.bin"),
            // The object a state ref names is kept.
            object(45, 50, "benchcmp/repair-rules/external/e.bin"),
            // Overlaps hot bytes.
            object(50, 55, "benchcmp/repair-rules/external/f.bin"),
            // Predates the stream's creation by more than a minute.
            object(60, 70, &old_object),
            // A staged object from creation time is kept.
            object(70, 80, &fresh_object),
            // Starts at the tail.
            object(100, 110, "benchcmp/repair-rules/external/g.bin"),
        ],
    };
    let input = ColdIndexRepairInput {
        stream_id,
        generation: 0,
        retained_offset: 0,
        tail_offset: 100,
        created_at_ms,
        hot_ranges: vec![(52, 60)],
        state_refs: vec![
            object(30, 40, "benchcmp/_packs/00000000/pack.bin"),
            object(45, 50, "benchcmp/repair-rules/external/e.bin"),
        ],
    };
    let report = repair_cold_index_page(&mut page, &input);
    assert_eq!(report, ColdIndexRepairReport {
        streams_scanned: 0,
        pages_scanned: 1,
        pages_rewritten: 1,
        superseded_entries_dropped: 1,
        beyond_tail_entries_dropped: 1,
        overlapping_entries_dropped: 3,
        predating_entries_dropped: 1,
    });
    let kept = page
        .external_segments
        .iter()
        .map(|entry| entry.start_offset)
        .collect::<Vec<_>>();
    assert_eq!(kept, vec![20, 45, 70]);
    assert_eq!(page.cold_chunks.len(), 1);
}

/// Pages beyond the tail page that a stale entry spans into are repaired.
#[tokio::test]
async fn repair_reaches_pages_past_the_tail_page() {
    let cold_store = memory_cold_store();
    let store = ColdStoreColdIndexPageStore::new(cold_store);
    let stream_id = BucketStreamId::new("benchcmp", "repair-past-tail");
    let span = ursula_stream::COLD_INDEX_PAGE_SPAN_BYTES;
    let stale = object(
        span - 4,
        span + 4,
        "benchcmp/repair-past-tail/external/x.bin",
    );
    for page_id in [0, 1] {
        store
            .put_page(
                &ColdIndexPageKey {
                    stream_id: stream_id.clone(),
                    generation: 0,
                    page_id,
                },
                &ColdIndexPage {
                    start_offset: page_id * span,
                    end_offset: (page_id + 1) * span,
                    cold_chunks: Vec::new(),
                    external_segments: vec![stale.clone()],
                },
            )
            .await
            .expect("store page");
    }
    let report = crate::cold_index::repair_stream_cold_index_pages(&store, &ColdIndexRepairInput {
        stream_id: stream_id.clone(),
        generation: 0,
        retained_offset: 0,
        tail_offset: span - 4,
        created_at_ms: 0,
        hot_ranges: Vec::new(),
        state_refs: Vec::new(),
    })
    .await
    .expect("repair");
    assert_eq!(report.pages_scanned, 2);
    assert_eq!(report.pages_rewritten, 2);
    assert_eq!(report.beyond_tail_entries_dropped, 2);
}

fn stream_in_group(runtime: &ShardRuntime, group: u32, prefix: &str) -> BucketStreamId {
    (0..1024)
        .map(|index| BucketStreamId::new("benchcmp", format!("{prefix}-{index}")))
        .find(|stream| runtime.locate(stream).raft_group_id == RaftGroupId(group))
        .expect("a stream id hashes to the group")
}

/// F14b: one group's GC failure no longer stops the groups after it.
#[tokio::test]
async fn f14b_cold_gc_continues_past_a_failing_group() {
    let cold_store = memory_cold_store();
    let runtime = ShardRuntime::spawn_with_engine_factory_and_cold_store(
        RuntimeConfig::new(1, 2),
        InMemoryGroupEngineFactory::with_cold_store(Some(cold_store.clone())),
        Some(cold_store.clone()),
    )
    .expect("spawn runtime");
    let failing = stream_in_group(&runtime, 0, "gc-failing");
    let healthy = stream_in_group(&runtime, 1, "gc-healthy");
    for stream in [&failing, &healthy] {
        runtime
            .create_stream(CreateStreamRequest::new(
                stream.clone(),
                DEFAULT_CONTENT_TYPE,
            ))
            .await
            .expect("create stream");
        runtime
            .append(append_req(stream, b"abcd", None))
            .await
            .expect("append");
        runtime
            .flush_cold_once(PlanColdFlushRequest {
                stream_id: stream.clone(),
                min_hot_bytes: 1,
                max_flush_bytes: 1024,
            })
            .await
            .expect("flush")
            .expect("a flush candidate exists");
        runtime
            .delete_stream(DeleteStreamRequest {
                stream_id: stream.clone(),
            })
            .await
            .expect("delete stream");
    }
    let failing_prefix = format!("{failing}/");
    cold_store.set_fault_policy(move |context| {
        // Stream GC deletes the names it owns one by one (F14g step 1).
        (context.operation == ColdStoreOperation::DeleteChunk
            && context.path.starts_with(&failing_prefix))
        .then(|| ColdStoreFaultEffect::fail("injected delete failure"))
    });
    runtime
        .run_cold_gc_all_groups_once(256)
        .await
        .expect_err("the failing group's error is reported");
    assert!(
        cold_store
            .prefix_is_empty(&crate::cold_store::cold_chunk_prefix(&healthy))
            .await
            .expect("list healthy prefix"),
        "the healthy group after the failing one is still reclaimed"
    );
    assert!(
        !cold_store
            .prefix_is_empty(&crate::cold_store::cold_chunk_prefix(&failing))
            .await
            .expect("list failing prefix")
    );
    // F14b: the failing entry is deferred with a backoff instead of being
    // retried on the next pass; `cold_gc_hygiene_tests` covers the backoff.
}

/// F14e: the chunk of a definitely rejected (stale) flush is deleted, and
/// its page entry was never left behind.
#[tokio::test]
async fn f14e_stale_flush_deletes_its_chunk() {
    let cold_store = memory_cold_store();
    let runtime = spawn_with_cold_store(cold_store.clone());
    let stream = BucketStreamId::new("benchcmp", "f14e-stale");
    runtime
        .create_stream(CreateStreamRequest::new(
            stream.clone(),
            DEFAULT_CONTENT_TYPE,
        ))
        .await
        .expect("create stream");
    runtime
        .append(append_req(&stream, b"abcd", None))
        .await
        .expect("append");
    let candidate = runtime
        .plan_cold_flush(PlanColdFlushRequest {
            stream_id: stream.clone(),
            min_hot_bytes: 1,
            max_flush_bytes: 1024,
        })
        .await
        .expect("plan")
        .expect("a flush candidate exists");
    let events = Arc::new(std::sync::Mutex::new(Vec::new()));
    let observed = events.clone();
    cold_store.set_observer(move |event| {
        observed.lock().expect("events lock").push(event);
    });
    let published = runtime
        .flush_cold_candidates_batch(vec![candidate.clone()])
        .await
        .expect("first flush publishes");
    assert_eq!(published.len(), 1);
    let stale = runtime
        .flush_cold_candidates_batch(vec![candidate])
        .await
        .expect("a stale candidate is skipped");
    assert!(stale.is_empty());

    let events = events.lock().expect("events lock").clone();
    let written = events
        .iter()
        .filter_map(|event| match event {
            crate::ColdStoreEvent::WriteChunkComplete { path, .. } => Some(path.clone()),
            _ => None,
        })
        .collect::<Vec<_>>();
    let deleted = events
        .iter()
        .filter_map(|event| match event {
            crate::ColdStoreEvent::DeleteChunkComplete { path } => Some(path.clone()),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(written.len(), 2, "{events:?}");
    assert_eq!(deleted, vec![written[1].clone()]);

    let read = runtime
        .read_stream(read_req(stream, 0, 16))
        .await
        .expect("read flushed bytes");
    assert_eq!(read.payload, b"abcd");
}

fn create_external_req(
    stream_id: &BucketStreamId,
    content_type: &str,
    s3_path: &str,
    len: u64,
) -> crate::CreateStreamExternalRequest {
    crate::CreateStreamExternalRequest {
        stream_id: stream_id.clone(),
        content_type: content_type.to_owned(),
        initial_payload: ExternalPayloadRef {
            s3_path: s3_path.to_owned(),
            payload_len: len,
            object_size: len,
        },
        record_ends: Vec::new(),
        close_after: false,
        stream_seq: None,
        producer: None,
        stream_ttl_seconds: None,
        stream_expires_at_ms: None,
        now_ms: 0,
    }
}

/// D3 via the create path: an external create of a stream that already
/// exists (a retry, or a conflicting create) must not write a page entry at
/// offset 0 of the live stream, which used to replace the stream's own entry.
#[tokio::test]
async fn external_create_of_a_live_stream_leaves_its_pages_alone() {
    let cold_store = memory_cold_store();
    let runtime = spawn_with_cold_store(cold_store.clone());
    let stream = BucketStreamId::new("benchcmp", "create-live");
    stage(
        &cold_store,
        "benchcmp/create-live/external/first.bin",
        b"abcd",
    )
    .await;
    runtime
        .create_stream_external(create_external_req(
            &stream,
            DEFAULT_CONTENT_TYPE,
            "benchcmp/create-live/external/first.bin",
            4,
        ))
        .await
        .expect("create external stream");
    stage(
        &cold_store,
        "benchcmp/create-live/external/retry.bin",
        b"WXYZ",
    )
    .await;
    runtime
        .create_stream_external(create_external_req(
            &stream,
            "text/plain",
            "benchcmp/create-live/external/retry.bin",
            4,
        ))
        .await
        .expect_err("a conflicting create is rejected");
    let read = runtime
        .read_stream(read_req(stream, 0, 16))
        .await
        .expect("read initial payload");
    assert_eq!(read.payload, b"abcd");
}

async fn cold_object_exists(cold_store: &ColdStore, path: &str) -> bool {
    let (dir, name) = path.rsplit_once('/').expect("object path has a directory");
    cold_store
        .list_file_names(&format!("{dir}/"))
        .await
        .expect("list objects")
        .iter()
        .any(|listed| listed == name)
}

/// F14f through the engine: the retained offset lies in page 0, the
/// boundary page, which a new leader may be flushing into. After the grace,
/// retention GC leaves that page and every object it names alone, even the
/// chunk wholly below the offset; reads from the offset still work. (Whole
/// pages below the offset are covered in `retention_gc`'s unit tests.)
#[tokio::test]
async fn retention_gc_never_touches_the_boundary_page() {
    let placement = placement();
    let cold_store = memory_cold_store();
    let stream = BucketStreamId::new("benchcmp", "retention-gc");
    let mut engine = InMemoryGroupEngine::with_cold_store(cold_store.clone());
    engine
        .create_stream(
            CreateStreamRequest::new(stream.clone(), DEFAULT_CONTENT_TYPE),
            placement,
            ColdWriteAdmission::default(),
        )
        .await
        .expect("create stream");
    for payload in [b"abcd", b"efgh", b"ijkl"] {
        engine
            .append(
                append_req(&stream, payload, None),
                placement,
                ColdWriteAdmission::default(),
            )
            .await
            .expect("append");
    }
    let below = "benchcmp/retention-gc/chunks/0-4.bin";
    let straddling = "benchcmp/retention-gc/chunks/4-8.bin";
    for (start, end, path, bytes) in [(0, 4, below, b"abcd"), (4, 8, straddling, b"efgh")] {
        stage(&cold_store, path, bytes).await;
        engine
            .flush_cold(
                FlushColdRequest {
                    cold_generation: engine
                        .state_machine
                        .cold_index_generation(&stream)
                        .expect("live stream"),
                    stream_id: stream.clone(),
                    chunk: ColdChunkRef {
                        start_offset: start,
                        end_offset: end,
                        s3_path: path.to_owned(),
                        object_size: 4,
                        ..Default::default()
                    },
                },
                placement,
            )
            .await
            .expect("flush chunk");
    }
    engine
        .publish_snapshot(
            crate::PublishSnapshotRequest {
                stream_id: stream.clone(),
                snapshot_offset: 4,
                content_type: DEFAULT_CONTENT_TYPE.to_owned(),
                payload: bytes::Bytes::from_static(b"state"),
                cold_body: None,
                now_ms: 0,
                expected_incarnation: None,
            },
            placement,
        )
        .await
        .expect("publish checkpoint");
    engine
        .advance_retention(
            crate::AdvanceRetentionRequest {
                stream_id: stream.clone(),
                retained_offset: 4,
                now_ms: 0,
                expected_incarnation: None,
            },
            placement,
        )
        .await
        .expect("advance retention");

    let step = |now_ms| crate::RepairColdIndexRequest {
        after: None,
        max_streams: 16,
        stream: None,
        retention_gc_now_ms: Some(now_ms),
    };
    let t0 = 1_000_000;
    let grace = ursula_stream::RETENTION_COLD_GC_GRACE_MS;
    for now_ms in [t0, t0 + grace, t0 + 2 * grace] {
        engine
            .repair_cold_index(step(now_ms), placement)
            .await
            .expect("repair step");
    }
    assert!(cold_object_exists(&cold_store, below).await);
    assert!(cold_object_exists(&cold_store, straddling).await);
    let page = ColdStoreColdIndexPageStore::new(cold_store.clone())
        .get_page(&ColdIndexPageKey {
            stream_id: stream.clone(),
            // C7/F14g: the group's first incarnation is `created_at_ms` 1.
            generation: 1,
            page_id: 0,
        })
        .await
        .expect("read page")
        .expect("boundary page kept");
    let paths: Vec<_> = page
        .cold_chunks
        .iter()
        .map(|chunk| chunk.s3_path.as_str())
        .collect();
    assert_eq!(paths, [below, straddling]);
    let read = engine
        .read_stream(read_req(stream, 4, 64), placement)
        .await
        .expect("read retained bytes");
    assert_eq!(read.payload, b"efghijkl");
}

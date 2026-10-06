//! Bounded-stream-state F5 on the in-memory engine:
//! external locators committed first and indexed after. No page entry is
//! written before a proposal; the offload pass writes entries only for
//! committed refs, clipping whatever overlapped them (Invariant 11); state
//! keeps at most T_ext staged refs per stream (W3); and the orphan sweep
//! never deletes a staged object that state or pages reference.

use std::sync::Arc;
use std::time::Duration;

use ursula_shard::BucketStreamId;
use ursula_shard::RaftGroupId;
use ursula_stream::ExternalPayloadRef;
use ursula_stream::MAX_STAGED_EXTERNAL_REFS;
use ursula_stream::ObjectPayloadRef;

use crate::AppendExternalRequest;
use crate::AppendRequest;
use crate::ColdIndexPageStore;
use crate::ColdStore;
use crate::ColdStoreColdIndexPageStore;
use crate::CreateStreamRequest;
use crate::InMemoryGroupEngineFactory;
use crate::OffloadColdRefsRequest;
use crate::ReadStreamRequest;
use crate::RuntimeConfig;
use crate::ShardRuntime;
use crate::cold_store::DEFAULT_CONTENT_TYPE;
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

async fn append_with_seq(
    runtime: &ShardRuntime,
    stream_id: &BucketStreamId,
    payload: &[u8],
    seq: &str,
) {
    let mut request = AppendRequest::from_bytes(stream_id.clone(), payload.to_vec());
    request.stream_seq = Some(seq.to_owned());
    runtime.append(request).await.expect("append");
}

/// Stages `payload` like the HTTP layer and proposes `AppendExternal`.
async fn append_external(
    runtime: &ShardRuntime,
    cold_store: &ColdStore,
    stream_id: &BucketStreamId,
    payload: &[u8],
    stream_seq: Option<&str>,
) -> (String, Result<crate::AppendResponse, crate::RuntimeError>) {
    let path = new_external_payload_path(stream_id);
    cold_store
        .write_chunk(&path, payload)
        .await
        .expect("stage payload");
    let len = u64::try_from(payload.len()).expect("len fits u64");
    let result = runtime
        .append_external(AppendExternalRequest {
            stream_id: stream_id.clone(),
            content_type: DEFAULT_CONTENT_TYPE.to_owned(),
            payload: ExternalPayloadRef {
                s3_path: path.clone(),
                payload_len: len,
                object_size: len,
            },
            record_ends: Vec::new(),
            close_after: false,
            stream_seq: stream_seq.map(str::to_owned),
            producer: None,
            now_ms: 0,
            if_incarnation: None,
        })
        .await;
    (path, result)
}

async fn read_all(runtime: &ShardRuntime, stream_id: &BucketStreamId) -> Vec<u8> {
    runtime
        .read_stream(ReadStreamRequest {
            stream_id: stream_id.clone(),
            offset: 0,
            max_len: 1 << 20,
            now_ms: 0,
            leader_only: false,
            read_index: None,
        })
        .await
        .expect("read stream")
        .payload
}

/// Every external entry in the stream's cold-index pages, any generation.
async fn page_external_entries(
    cold_store: &Arc<ColdStore>,
    stream_id: &BucketStreamId,
) -> Vec<ObjectPayloadRef> {
    let store = ColdStoreColdIndexPageStore::new(cold_store.clone());
    let mut entries = Vec::new();
    for key in cold_store
        .list_cold_index_pages()
        .await
        .expect("list pages")
    {
        if &key.stream_id != stream_id {
            continue;
        }
        if let Some(page) = store.get_page(&key).await.expect("get page") {
            entries.extend(page.external_segments);
        }
    }
    entries
}

/// Invariant 11: every page entry serves exactly the acknowledged bytes at
/// its offsets.
async fn assert_no_page_entry_overlaps_differing_bytes(
    cold_store: &Arc<ColdStore>,
    stream_id: &BucketStreamId,
    acknowledged: &[u8],
) {
    for entry in page_external_entries(cold_store, stream_id).await {
        let start = usize::try_from(entry.start_offset).expect("offset fits usize");
        let end = usize::try_from(entry.end_offset).expect("offset fits usize");
        let object = cold_store
            .read_object_range(&entry, entry.start_offset, end.checked_sub(start).unwrap())
            .await
            .expect("page entry names a readable object");
        assert_eq!(
            acknowledged.get(start..end),
            Some(object.as_slice()),
            "page entry {entry:?} serves different bytes"
        );
    }
}

fn offload_now(max_streams: usize) -> OffloadColdRefsRequest {
    OffloadColdRefsRequest {
        min_age_ms: 0,
        ..OffloadColdRefsRequest::new(0, max_streams)
    }
}

/// Neither a rejected nor a committed external append writes a page entry
/// before its proposal (F5); the committed one is served from state until
/// the offload pass indexes it.
#[tokio::test]
async fn no_page_entry_is_written_before_a_proposal() {
    let cold_store = Arc::new(ColdStore::memory().expect("memory cold store"));
    let runtime = spawn(cold_store.clone());
    let s = stream("no-pre-proposal");
    create(&runtime, &s).await;
    append_with_seq(&runtime, &s, b"ab", "5").await;

    let (_, rejected) = append_external(&runtime, &cold_store, &s, &[b'#'; 30], Some("1")).await;
    rejected.expect_err("a regressed stream seq rejects the external append");
    let after_rejection = page_external_entries(&cold_store, &s).await;
    assert!(after_rejection.is_empty(), "{after_rejection:?}");

    let (committed, result) = append_external(&runtime, &cold_store, &s, b"WXYZ", None).await;
    result.expect("committed external append");
    assert!(page_external_entries(&cold_store, &s).await.is_empty());
    let gauges = runtime.state_gauges(GROUP).await.expect("gauges");
    assert_eq!(gauges.staged_external_refs, 1);
    assert_eq!(read_all(&runtime, &s).await, b"abWXYZ".to_vec());

    let report = runtime
        .offload_cold_refs(GROUP, offload_now(16))
        .await
        .expect("offload pass");
    assert_eq!((report.streams, report.refs_offloaded), (1, 1));
    let entries = page_external_entries(&cold_store, &s).await;
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].s3_path, committed);
    assert_eq!((entries[0].start_offset, entries[0].end_offset), (2, 6));
    let gauges = runtime.state_gauges(GROUP).await.expect("gauges");
    assert_eq!(gauges.staged_external_refs, 0);
    assert_eq!(read_all(&runtime, &s).await, b"abWXYZ".to_vec());

    // A second pass finds nothing to do.
    let again = runtime
        .offload_cold_refs(GROUP, offload_now(16))
        .await
        .expect("second offload pass");
    assert_eq!(again.streams, 0);
}

/// W3 on the runtime: an external-only stream keeps at most T_ext staged
/// refs when the offload pass runs between appends, even with no ref old
/// enough for the age trigger.
#[tokio::test]
async fn staged_refs_stay_within_t_ext_on_an_external_only_stream() {
    let cold_store = Arc::new(ColdStore::memory().expect("memory cold store"));
    let runtime = spawn(cold_store.clone());
    let s = stream("w3");
    create(&runtime, &s).await;
    let fresh_only = OffloadColdRefsRequest {
        min_age_ms: u64::MAX,
        ..OffloadColdRefsRequest::new(0, 16)
    };
    let mut acknowledged = Vec::new();
    let mut max_staged = 0;
    for index in 0..50_u8 {
        let payload = [b'a' + index % 26; 7];
        let (_, result) = append_external(&runtime, &cold_store, &s, &payload, None).await;
        result.expect("external append");
        acknowledged.extend_from_slice(&payload);
        runtime
            .offload_cold_refs(GROUP, fresh_only)
            .await
            .expect("offload pass");
        let staged = runtime
            .state_gauges(GROUP)
            .await
            .expect("gauges")
            .staged_external_refs;
        max_staged = max_staged.max(staged);
    }
    assert_eq!(max_staged, u64::try_from(MAX_STAGED_EXTERNAL_REFS).unwrap());
    assert_eq!(read_all(&runtime, &s).await, acknowledged);
    assert_no_page_entry_overlaps_differing_bytes(&cold_store, &s, &acknowledged).await;
}

/// The orphan sweep's reference check covers both homes of a locator: a
/// staged object held only in state, and an offloaded one held only in
/// pages, are kept; a staged object nothing references is deleted after the
/// grace.
#[tokio::test]
async fn orphan_sweep_keeps_state_and_page_referenced_staged_objects() {
    let cold_store = Arc::new(ColdStore::memory().expect("memory cold store"));
    let runtime = spawn(cold_store.clone());
    let s = stream("sweep");
    create(&runtime, &s).await;
    let (offloaded, result) = append_external(&runtime, &cold_store, &s, b"PAGE", None).await;
    result.expect("first external append");
    runtime
        .offload_cold_refs(GROUP, offload_now(16))
        .await
        .expect("offload pass");
    let (staged, result) = append_external(&runtime, &cold_store, &s, b"STATE", None).await;
    result.expect("second external append");
    let orphan = new_external_payload_path(&s);
    cold_store
        .write_chunk(&orphan, b"orphan")
        .await
        .expect("inject orphan");

    tokio::time::sleep(Duration::from_millis(5)).await;
    let swept = runtime
        .sweep_cold_orphans_group_once(GROUP, 16, 0)
        .await
        .expect("sweep");
    assert_eq!(swept.orphans_deleted, 1);
    cold_store
        .object_size(&orphan)
        .await
        .expect_err("the sweep deletes the orphaned object");
    for kept in [&offloaded, &staged] {
        assert!(cold_store.object_size(kept).await.is_ok(), "{kept} is kept");
    }
    assert_eq!(read_all(&runtime, &s).await, b"PAGESTATE".to_vec());
}

/// RT6 for external payloads: once the offload pass has moved a committed
/// external ref into a page, a page read-modify-write by a deposed leader can
/// drop that entry. The payload then looks unreferenced, but it holds the
/// only copy of acknowledged bytes, so the sweep keeps it (and every other
/// unreferenced external payload of the stream) and alerts instead of
/// deleting it after the grace.
#[tokio::test]
async fn orphan_sweep_keeps_an_external_payload_whose_page_entry_was_lost() {
    let cold_store = Arc::new(ColdStore::memory().expect("memory cold store"));
    let runtime = spawn(cold_store.clone());
    let s = stream("lost-external-entry");
    create(&runtime, &s).await;
    let (offloaded, result) = append_external(&runtime, &cold_store, &s, b"PAGE", None).await;
    result.expect("external append");
    runtime
        .offload_cold_refs(GROUP, offload_now(16))
        .await
        .expect("offload pass");
    let orphan = new_external_payload_path(&s);
    cold_store
        .write_chunk(&orphan, b"orphan")
        .await
        .expect("inject orphan");

    // A deposed leader rewrites the page without the committed entry.
    let store = ColdStoreColdIndexPageStore::new(cold_store.clone());
    let mut dropped = 0;
    for key in cold_store
        .list_cold_index_pages()
        .await
        .expect("list pages")
    {
        if key.stream_id != s {
            continue;
        }
        let mut page = store.get_page(&key).await.unwrap().expect("page");
        let before = page.external_segments.len();
        page.external_segments
            .retain(|entry| entry.s3_path != offloaded);
        dropped += before - page.external_segments.len();
        store.put_page(&key, &page).await.unwrap();
    }
    assert_eq!(dropped, 1, "the offloaded entry lived in a page");

    tokio::time::sleep(Duration::from_millis(5)).await;
    let swept = runtime
        .sweep_cold_orphans_group_once(GROUP, 16, 0)
        .await
        .expect("sweep");
    assert_eq!(
        swept.orphans_deleted, 0,
        "nothing of an uncovered stream goes"
    );
    assert_eq!(swept.uncovered_chunks_kept, 2);
    assert_eq!(
        runtime
            .metrics()
            .snapshot()
            .cold_orphan_uncovered_chunks_kept,
        2
    );
    for kept in [&offloaded, &orphan] {
        assert!(cold_store.object_size(kept).await.is_ok(), "{kept} is kept");
    }
}

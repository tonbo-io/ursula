//! Raft-engine wiring for bounded-stream-state F5 at feature level 3:
//! external locators committed first and indexed after (the leader skips its
//! pre-proposal page write; the offload pass indexes the committed append).
//! The contracts (stale-entry clipping, the staged-refs bound T_ext, orphan
//! sweeps) are pinned against the in-memory engine in `ursula-runtime`'s
//! `external_locators_tests`.

use std::sync::Arc;

use ursula_runtime::AppendExternalRequest;
use ursula_runtime::AppendRequest;
use ursula_runtime::ColdIndexPageStore;
use ursula_runtime::ColdStore;
use ursula_runtime::ColdStoreColdIndexPageStore;
use ursula_runtime::CreateStreamRequest;
use ursula_runtime::ExternalPayloadRef;
use ursula_runtime::FEATURE_LEVEL_EXTERNAL_LOCATORS;
use ursula_runtime::FEATURE_LEVEL_KEYED_STREAMS;
use ursula_runtime::OffloadColdRefsRequest;
use ursula_runtime::ReadStreamRequest;
use ursula_runtime::RuntimeConfig;
use ursula_runtime::ShardRuntime;
use ursula_runtime::new_external_payload_path;
use ursula_shard::BucketStreamId;
use ursula_shard::RaftGroupId;
use ursula_stream::ObjectPayloadRef;

use super::ColdRaftGroupEngineFactory;

const GROUP: RaftGroupId = RaftGroupId(0);
const BUCKET: &str = "raft-locators";
const CONTENT_TYPE: &str = "application/octet-stream";

fn spawn(cold_store: Arc<ColdStore>) -> ShardRuntime {
    ShardRuntime::spawn_with_engine_factory_and_cold_store(
        RuntimeConfig::new(1, 1),
        ColdRaftGroupEngineFactory::new(cold_store.clone()),
        Some(cold_store),
    )
    .expect("spawn raft runtime")
}

async fn raise(runtime: &ShardRuntime, level: u32) {
    for (_, result) in runtime.set_feature_level_all_groups(level).await {
        result.expect("raise feature level");
    }
}

async fn setup(level: u32, name: &str) -> (Arc<ColdStore>, ShardRuntime, BucketStreamId) {
    let cold_store = Arc::new(ColdStore::memory().expect("memory cold store"));
    let runtime = spawn(cold_store.clone());
    raise(&runtime, level).await;
    let stream_id = BucketStreamId::new(BUCKET, name);
    runtime
        .create_stream(CreateStreamRequest::new(stream_id.clone(), CONTENT_TYPE))
        .await
        .expect("create stream");
    let mut request = AppendRequest::from_bytes(stream_id.clone(), b"ab".to_vec());
    request.stream_seq = Some("5".to_owned());
    runtime.append(request).await.expect("append with seq");
    (cold_store, runtime, stream_id)
}

async fn append_external(
    runtime: &ShardRuntime,
    cold_store: &ColdStore,
    stream_id: &BucketStreamId,
    payload: &[u8],
    stream_seq: Option<&str>,
) -> (String, bool) {
    let path = new_external_payload_path(stream_id);
    cold_store
        .write_chunk(&path, payload)
        .await
        .expect("stage payload");
    let len = u64::try_from(payload.len()).expect("len fits u64");
    let result = runtime
        .append_external(AppendExternalRequest {
            stream_id: stream_id.clone(),
            content_type: CONTENT_TYPE.to_owned(),
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
            record_match: None,
        })
        .await;
    (path, result.is_ok())
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
        })
        .await
        .expect("read stream")
        .payload
}

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

fn offload_now() -> OffloadColdRefsRequest {
    OffloadColdRefsRequest {
        min_age_ms: 0,
        ..OffloadColdRefsRequest::new(0, 16)
    }
}

#[tokio::test]
async fn level_three_writes_no_page_entry_before_a_proposal() {
    // Level 1 pins the pre-proposal write a rejected append leaves behind.
    let (cold_store, runtime, s) = setup(FEATURE_LEVEL_KEYED_STREAMS, "legacy").await;
    let (_, ok) = append_external(&runtime, &cold_store, &s, &[b'#'; 30], Some("1")).await;
    assert!(!ok);
    assert_eq!(page_external_entries(&cold_store, &s).await.len(), 1);

    let (cold_store, runtime, s) = setup(FEATURE_LEVEL_EXTERNAL_LOCATORS, "lb3").await;
    let (_, ok) = append_external(&runtime, &cold_store, &s, &[b'#'; 30], Some("1")).await;
    assert!(!ok, "a regressed stream seq rejects the external append");
    assert!(page_external_entries(&cold_store, &s).await.is_empty());
    let (committed, ok) = append_external(&runtime, &cold_store, &s, b"WXYZ", None).await;
    assert!(ok);
    assert!(page_external_entries(&cold_store, &s).await.is_empty());
    assert_eq!(
        runtime
            .state_gauges(GROUP)
            .await
            .unwrap()
            .staged_external_refs,
        1
    );
    assert_eq!(read_all(&runtime, &s).await, b"abWXYZ".to_vec());

    let report = runtime
        .offload_cold_refs(GROUP, offload_now())
        .await
        .expect("offload pass");
    assert_eq!((report.streams, report.refs_offloaded), (1, 1));
    let entries = page_external_entries(&cold_store, &s).await;
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].s3_path, committed);
    assert_eq!(
        runtime
            .state_gauges(GROUP)
            .await
            .unwrap()
            .staged_external_refs,
        0
    );
    assert_eq!(read_all(&runtime, &s).await, b"abWXYZ".to_vec());
}

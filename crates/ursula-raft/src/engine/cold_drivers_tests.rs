//! Raft-engine tests for the leader-side cold-reference drivers
//! (bounded-stream-state B2): the shared pack-reference compaction driver
//! (F2) and the cold orphan sweep (F14h): one wiring test per driver. The
//! drivers' contracts are pinned against the in-memory engine in
//! `ursula-runtime`'s `cold_drivers_tests`.

use std::sync::Arc;
use std::time::Duration;

use ursula_runtime::AppendRequest;
use ursula_runtime::ColdStore;
use ursula_runtime::CreateStreamRequest;
use ursula_runtime::HeadStreamRequest;
use ursula_runtime::PlanGroupColdFlushRequest;
use ursula_runtime::ReadStreamRequest;
use ursula_runtime::RuntimeConfig;
use ursula_runtime::ShardRuntime;
use ursula_runtime::SharedRefCompactionConfig;
use ursula_runtime::cold_pack_dir;
use ursula_runtime::new_cold_chunk_path_in_generation;
use ursula_runtime::new_cold_pack_path;
use ursula_runtime::new_external_payload_path;
use ursula_shard::BucketStreamId;
use ursula_shard::RaftGroupId;

use super::test_support::JournalRuntime;
use super::test_support::spawn_journal_runtime;

const GROUP: RaftGroupId = RaftGroupId(0);
const BUCKET: &str = "raft-drivers";
const CONTENT_TYPE: &str = "application/octet-stream";

fn spawn(cold_store: Arc<ColdStore>) -> JournalRuntime {
    spawn_journal_runtime(RuntimeConfig::new(1, 1), Some(cold_store))
}

fn stream(name: &str) -> BucketStreamId {
    BucketStreamId::new(BUCKET, name)
}

async fn create(runtime: &ShardRuntime, stream_id: &BucketStreamId) {
    runtime
        .create_stream(CreateStreamRequest::new(stream_id.clone(), CONTENT_TYPE))
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
            leader_only: false,
            read_index: None,
        })
        .await
        .expect("read stream")
        .payload
}

fn driver(min_refs: usize, gc_grace_ms: u64) -> SharedRefCompactionConfig {
    SharedRefCompactionConfig {
        min_refs,
        ..SharedRefCompactionConfig::new(16 << 20, 16, gc_grace_ms)
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn raft_shared_ref_driver_compacts_and_packs_are_gced() {
    let cold_store = Arc::new(ColdStore::memory().expect("memory cold store"));
    let runtime = spawn(cold_store.clone());
    let (a, b) = (stream("trickle-a"), stream("trickle-b"));
    create(&runtime, &a).await;
    create(&runtime, &b).await;
    let mut expected_a = Vec::new();
    for round in 0..4_u8 {
        let payload = [b'a', b'0' + round];
        append(&runtime, &a, &payload).await;
        append(&runtime, &b, b"b").await;
        expected_a.extend_from_slice(&payload);
        assert_eq!(pack_flush(&runtime).await, 2);
    }
    assert_eq!(runtime.state_gauges(GROUP).await.unwrap().live_packs, 4);

    let report = runtime
        .compact_shared_refs_group_once(GROUP, &driver(4, 0))
        .await
        .expect("driver pass");
    assert_eq!(report.compacted_streams, 2);
    let gauges = runtime.state_gauges(GROUP).await.unwrap();
    assert_eq!(gauges.shared_refs, 0);
    assert_eq!(gauges.live_packs, 0);
    assert_eq!(read_all(&runtime, &a).await, expected_a);
    runtime
        .run_cold_gc_all_groups_once(256)
        .await
        .expect("cold gc");
    assert!(
        cold_store
            .list_file_names(&cold_pack_dir(BUCKET, GROUP.0))
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(read_all(&runtime, &a).await, expected_a);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn raft_orphan_sweep_reclaims_only_unreferenced_objects() {
    let cold_store = Arc::new(ColdStore::memory().expect("memory cold store"));
    let runtime = spawn(cold_store.clone());
    let (a, b) = (stream("sweep-a"), stream("sweep-b"));
    create(&runtime, &a).await;
    create(&runtime, &b).await;
    append(&runtime, &a, b"a1").await;
    append(&runtime, &b, b"b1").await;
    pack_flush(&runtime).await;
    let pack_dir = cold_pack_dir(BUCKET, GROUP.0);
    let live_packs = cold_store.list_file_names(&pack_dir).await.unwrap();
    assert_eq!(live_packs.len(), 1);

    let orphan_pack = new_cold_pack_path(BUCKET, GROUP.0);
    // C7/F14g: chunks live under the stream's incarnation generation, the
    // directory the sweep lists.
    let generation = runtime
        .head_stream(HeadStreamRequest {
            stream_id: a.clone(),
            now_ms: 0,
            linearizable: false,
            read_index: None,
        })
        .await
        .expect("head")
        .created_at_ms;
    let orphan_chunk = new_cold_chunk_path_in_generation(&a, generation, 0, 2);
    let orphan_external = new_external_payload_path(&b);
    for path in [&orphan_pack, &orphan_chunk, &orphan_external] {
        cold_store
            .write_chunk(path, b"orphan")
            .await
            .expect("inject orphan");
    }
    tokio::time::sleep(Duration::from_millis(5)).await;
    let swept = runtime
        .sweep_cold_orphans_group_once(GROUP, 16, 0)
        .await
        .expect("sweep");
    assert!(swept.cycle_completed);
    assert_eq!(swept.orphans_deleted, 3);
    assert_eq!(
        cold_store.list_file_names(&pack_dir).await.unwrap(),
        live_packs,
        "the live pack is kept"
    );
    assert_eq!(read_all(&runtime, &a).await, b"a1");
    assert_eq!(read_all(&runtime, &b).await, b"b1");
}

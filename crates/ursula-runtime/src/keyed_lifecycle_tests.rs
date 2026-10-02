//! Keyed-stream lifecycle (keyed-streams §3.8, U22, U23) on the in-memory
//! engine: stream delete removes the deleted incarnation's projection
//! namespace, and bucket erasure removes and proves `.keyed/{bucket}/`.

use std::sync::Arc;

use ursula_shard::BucketStreamId;
use ursula_shard::KEYED_BATCH_CONTENT_TYPE;
use ursula_shard::keyed_namespace::keyed_bucket_prefix;
use ursula_shard::keyed_namespace::keyed_incarnation_prefix;
use ursula_stream::FEATURE_LEVEL_KEYED_STREAMS;

use crate::ColdStore;
use crate::CreateStreamRequest;
use crate::DeleteStreamRequest;
use crate::HeadStreamRequest;
use crate::InMemoryGroupEngineFactory;
use crate::RuntimeConfig;
use crate::ShardRuntime;

fn spawn(cold_store: Arc<ColdStore>) -> ShardRuntime {
    ShardRuntime::spawn_with_engine_factory_and_cold_store(
        RuntimeConfig::new(2, 8),
        InMemoryGroupEngineFactory::with_cold_store(Some(cold_store.clone())),
        Some(cold_store),
    )
    .expect("spawn runtime")
}

async fn raise_to_keyed_level(runtime: &ShardRuntime) {
    for (group, result) in runtime
        .set_feature_level_all_groups(FEATURE_LEVEL_KEYED_STREAMS)
        .await
    {
        result.unwrap_or_else(|err| panic!("raise group {group:?}: {err}"));
    }
}

async fn create_keyed(runtime: &ShardRuntime, stream: &BucketStreamId) -> u64 {
    runtime
        .create_stream(CreateStreamRequest::new(
            stream.clone(),
            KEYED_BATCH_CONTENT_TYPE,
        ))
        .await
        .expect("create keyed stream");
    runtime
        .head_stream(HeadStreamRequest {
            stream_id: stream.clone(),
            now_ms: 0,
        })
        .await
        .expect("head keyed stream")
        .created_at_ms
        .expect("incarnation at level 1")
}

async fn delete(runtime: &ShardRuntime, stream: &BucketStreamId) {
    runtime
        .delete_stream(DeleteStreamRequest {
            stream_id: stream.clone(),
        })
        .await
        .expect("delete stream");
}

/// Writes the objects an indexer publishes for one namespace.
async fn write_namespace(cold_store: &ColdStore, prefix: &str) -> Vec<String> {
    let names = [
        format!("{prefix}v1/CURRENT"),
        format!("{prefix}v1/manifests/0000000000000001.json"),
        format!("{prefix}v1/parts/0000000000000001.part"),
    ];
    for name in &names {
        cold_store
            .write_chunk(name, b"projection")
            .await
            .expect("write namespace object");
    }
    names.to_vec()
}

async fn prefix_is_empty(cold_store: &ColdStore, prefix: &str) -> bool {
    cold_store
        .prefix_is_empty(prefix)
        .await
        .expect("list namespace prefix")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn u22_delete_removes_the_keyed_namespace_and_recreate_keeps_the_new_one() {
    let cold_store = Arc::new(ColdStore::memory().expect("memory cold store"));
    let runtime = spawn(cold_store.clone());
    raise_to_keyed_level(&runtime).await;
    let stream = BucketStreamId::new("keyed-bucket", "harness");
    // An affinity stream under the same name has a disjoint namespace.
    let neighbour = BucketStreamId::with_affinity("keyed-bucket", "harness", "chunks");

    let old = create_keyed(&runtime, &stream).await;
    let old_prefix = keyed_incarnation_prefix(&stream, old);
    write_namespace(&cold_store, &old_prefix).await;
    let neighbour_incarnation = create_keyed(&runtime, &neighbour).await;
    let neighbour_prefix = keyed_incarnation_prefix(&neighbour, neighbour_incarnation);
    write_namespace(&cold_store, &neighbour_prefix).await;

    delete(&runtime, &stream).await;
    let new = create_keyed(&runtime, &stream).await;
    assert_ne!(old, new);
    let new_prefix = keyed_incarnation_prefix(&stream, new);
    write_namespace(&cold_store, &new_prefix).await;

    runtime
        .run_cold_gc_all_groups_once(256)
        .await
        .expect("run stream gc");

    assert!(prefix_is_empty(&cold_store, &old_prefix).await);
    assert!(!prefix_is_empty(&cold_store, &new_prefix).await);
    assert!(!prefix_is_empty(&cold_store, &neighbour_prefix).await);
    let group = runtime.locate(&stream).raft_group_id;
    let snapshot = runtime.snapshot_group(group).await.expect("snapshot group");
    assert!(snapshot.stream_snapshot.pending_cold_gc.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn u23_bucket_erasure_removes_and_proves_both_prefixes() {
    let cold_store = Arc::new(ColdStore::memory().expect("memory cold store"));
    let runtime = spawn(cold_store.clone());
    raise_to_keyed_level(&runtime).await;
    let stream = BucketStreamId::new("erased", "harness");
    let survivor = BucketStreamId::new("kept", "harness");
    let incarnation = create_keyed(&runtime, &stream).await;
    write_namespace(&cold_store, &keyed_incarnation_prefix(&stream, incarnation)).await;
    // A namespace the node no longer tracks, e.g. one an indexer published
    // after the stream-delete sweep, and a stage-before-commit orphan.
    write_namespace(&cold_store, ".keyed/erased/orphan/0000000000000001/").await;
    cold_store
        .write_chunk("erased/orphan/external/x.bin", b"x")
        .await
        .expect("write orphan");
    let survivor_incarnation = create_keyed(&runtime, &survivor).await;
    let survivor_prefix = keyed_incarnation_prefix(&survivor, survivor_incarnation);
    write_namespace(&cold_store, &survivor_prefix).await;

    runtime
        .purge_bucket_all_groups("erased")
        .await
        .expect("purge bucket");
    runtime
        .run_cold_gc_all_groups_once(256)
        .await
        .expect("run stream gc");
    runtime
        .erase_bucket_cold_prefix_and_prove("erased")
        .await
        .expect("erase and prove");

    assert!(prefix_is_empty(&cold_store, &keyed_bucket_prefix("erased")).await);
    assert!(prefix_is_empty(&cold_store, "erased/").await);
    assert!(!prefix_is_empty(&cold_store, &survivor_prefix).await);
}

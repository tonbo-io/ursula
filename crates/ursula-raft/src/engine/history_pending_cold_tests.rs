//! A cold flush or compaction that a leader with a pending history refuses
//! (#581 section 4.5) was refused before its proposal: nothing of it
//! applied, so the runtime deletes its upload and the cold index keeps what
//! it held.

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::PoisonError;

use ursula_runtime::AppendRequest;
use ursula_runtime::ColdChunkRef;
use ursula_runtime::ColdStore;
use ursula_runtime::ColdStoreColdIndexPageStore;
use ursula_runtime::ColdStoreEvent;
use ursula_runtime::ColdStoreOperation;
use ursula_runtime::CreateStreamRequest;
use ursula_runtime::FlushColdRequest;
use ursula_runtime::GroupEngineError;
use ursula_runtime::GroupInfraError;
use ursula_runtime::PlanColdFlushRequest;
use ursula_runtime::RuntimeConfig;
use ursula_runtime::RuntimeError;
use ursula_runtime::ShardRuntime;
use ursula_runtime::load_cold_chunks_from_pages;
use ursula_shard::BucketStreamId;

use super::test_support::JournalRuntime;
use super::test_support::leader_upload;
use super::test_support::spawn_journal_runtime_published;
use crate::RaftGroupHandleRegistry;
use crate::history::GroupHistory;
use crate::log_store::PlanCut;
use crate::log_store::RecoveryRecord;

/// A plan whose point the single node's history event cannot pass: the
/// group's history stays pending until the plan is dropped.
fn unsatisfiable_plan() -> RecoveryRecord {
    RecoveryRecord {
        leader: 1,
        point: u64::MAX,
        leader_term: 0,
        cut: PlanCut::NoCut,
    }
}

fn spawn(cold_store: &Arc<ColdStore>) -> (JournalRuntime, RaftGroupHandleRegistry) {
    let registry = RaftGroupHandleRegistry::default();
    let runtime = spawn_journal_runtime_published(
        RuntimeConfig::new(1, 1),
        Some(cold_store.clone()),
        Some(registry.clone()),
    );
    (runtime, registry)
}

fn history_of(
    runtime: &ShardRuntime,
    registry: &RaftGroupHandleRegistry,
    stream: &BucketStreamId,
) -> Arc<GroupHistory> {
    registry
        .entry(runtime.locate(stream).raft_group_id)
        .expect("the group published its resources")
        .core
        .history
        .clone()
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

/// C7/F14g: the cold generation of the stream's live incarnation.
async fn cold_generation(runtime: &ShardRuntime, stream: &BucketStreamId) -> u64 {
    runtime
        .snapshot_group(runtime.locate(stream).raft_group_id)
        .await
        .expect("snapshot group")
        .stream_snapshot
        .streams
        .into_iter()
        .find(|entry| entry.metadata.stream_id == *stream)
        .map(|entry| entry.cold_index_generation)
        .expect("stream in snapshot")
}

async fn indexed(cold_store: &Arc<ColdStore>, stream: &BucketStreamId) -> Vec<ColdChunkRef> {
    let keys = cold_store
        .list_cold_index_pages()
        .await
        .expect("list pages")
        .into_iter()
        .filter(|key| &key.stream_id == stream)
        .collect::<Vec<_>>();
    load_cold_chunks_from_pages(&ColdStoreColdIndexPageStore::new(cold_store.clone()), &keys)
        .await
        .expect("load cold index pages")
}

/// The chunk objects written and deleted after this call.
fn record_chunk_writes(cold_store: &Arc<ColdStore>) -> Arc<Mutex<(Vec<String>, Vec<String>)>> {
    let seen = Arc::new(Mutex::new((Vec::new(), Vec::new())));
    let recorder = seen.clone();
    cold_store.set_observer(move |event| {
        let mut seen = recorder.lock().unwrap_or_else(PoisonError::into_inner);
        match event {
            ColdStoreEvent::WriteChunkComplete { path, .. } => seen.0.push(path),
            ColdStoreEvent::DeleteChunkComplete { path } => seen.1.push(path),
            _ => {}
        }
    });
    seen
}

fn is_history_pending(err: &RuntimeError) -> bool {
    matches!(err, RuntimeError::GroupEngine {
        error: GroupEngineError::Infra(GroupInfraError::RecoveryPending { .. }),
        ..
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_flush_refused_for_a_pending_history_deletes_its_upload_and_keeps_the_index() {
    let cold_store = Arc::new(ColdStore::memory().expect("memory cold store"));
    let (runtime, registry) = spawn(&cold_store);
    let stream = BucketStreamId::new("pending-history", "flush");
    create_and_append(&runtime, &stream, b"abcdefgh").await;
    let history = history_of(&runtime, &registry, &stream);
    let seen = record_chunk_writes(&cold_store);
    // The history turns pending once the flush planned its candidate and
    // uploads the chunk, so the publish is refused before its proposal.
    let fenced = history.clone();
    cold_store.set_fault_policy(move |context| {
        if context.operation == ColdStoreOperation::WriteChunk {
            fenced.set_plan(Some(unsatisfiable_plan()));
        }
        None
    });

    let err = runtime
        .flush_cold_once(PlanColdFlushRequest {
            stream_id: stream.clone(),
            min_hot_bytes: 1,
            max_flush_bytes: 1024,
        })
        .await
        .expect_err("a pending history refuses the publish");
    assert!(is_history_pending(&err), "{err:?}");

    let (written, deleted) = seen.lock().unwrap_or_else(PoisonError::into_inner).clone();
    assert_eq!(written.len(), 1, "{written:?}");
    assert_eq!(deleted, written, "the refused flush's upload is deleted");
    assert!(indexed(&cold_store, &stream).await.is_empty());
    history.set_plan(None);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_compaction_refused_for_a_pending_history_deletes_its_upload_and_goes_on() {
    let cold_store = Arc::new(ColdStore::memory().expect("memory cold store"));
    let (runtime, registry) = spawn(&cold_store);
    let streams = [
        BucketStreamId::new("pending-history", "compact-a"),
        BucketStreamId::new("pending-history", "compact-b"),
    ];
    let mut inputs = Vec::new();
    for stream in &streams {
        create_and_append(&runtime, stream, b"abcdefgh").await;
        let mut chunks = Vec::new();
        for (start, end, payload) in [(0, 4, b"abcd".as_slice()), (4, 8, b"efgh".as_slice())] {
            let chunk = ColdChunkRef {
                start_offset: start,
                end_offset: end,
                s3_path: format!("{stream}/chunks/input-{start}.bin"),
                object_size: end - start,
                ..Default::default()
            };
            cold_store
                .write_chunk(&chunk.s3_path, payload)
                .await
                .expect("write input chunk");
            runtime
                .flush_cold(FlushColdRequest {
                    cold_generation: cold_generation(&runtime, stream).await,
                    stream_id: stream.clone(),
                    chunk: chunk.clone(),
                    upload: leader_upload(&runtime, stream).await,
                })
                .await
                .expect("flush input chunk");
            chunks.push(chunk);
        }
        inputs.push(chunks);
    }
    let group = runtime.locate(&streams[0]).raft_group_id;
    runtime
        .repair_cold_index_group_once(group, 16)
        .await
        .expect("repair step");
    assert_eq!(runtime.compaction_debt_pages(), 2);
    let history = history_of(&runtime, &registry, &streams[0]);
    history.set_plan(Some(unsatisfiable_plan()));
    let seen = record_chunk_writes(&cold_store);

    // Each stream's publish is refused before its proposal; the pass goes on
    // with the next stream and answers that it compacted none.
    assert_eq!(
        runtime
            .compact_cold_once(8, 16, 2, 0)
            .await
            .expect("a refused compaction ends no pass"),
        0
    );

    let (written, deleted) = seen.lock().unwrap_or_else(PoisonError::into_inner).clone();
    assert_eq!(written.len(), 2, "one replacement per stream: {written:?}");
    assert_eq!(deleted, written, "every refused replacement is deleted");
    for (stream, chunks) in streams.iter().zip(&inputs) {
        let paths: Vec<String> = indexed(&cold_store, stream)
            .await
            .into_iter()
            .map(|chunk| chunk.s3_path)
            .collect();
        let expected: Vec<String> = chunks.iter().map(|chunk| chunk.s3_path.clone()).collect();
        assert_eq!(paths, expected, "{stream}: the index keeps its inputs");
    }
    history.set_plan(None);
}

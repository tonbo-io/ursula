use super::*;

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use bytes::Bytes;
use tokio::sync::{Notify, Semaphore, oneshot};
use ursula_shard::{BucketStreamId, CoreId, RaftGroupId, ShardId, ShardPlacement};
use ursula_stream::{
    ExternalPayloadRef, ObjectPayloadRef, StreamReadSegment, StreamSnapshot, StreamStateMachine,
};

use crate::cold_store::{ColdReadCacheConfig, DEFAULT_CONTENT_TYPE};
use crate::core_worker::{CoreWorker, ReadWatcher, ReadWatchers};
use crate::engine::wal::group_log_path;
use crate::error::ErrorStatus;
use crate::metrics::RuntimeMetricsInner;

fn runtime(core_count: usize, group_count: usize) -> ShardRuntime {
    ShardRuntime::spawn(RuntimeConfig {
        core_count,
        raft_group_count: group_count,
        mailbox_capacity: 128,
        threading: RuntimeThreading::HostedTokio,
        cold_max_hot_bytes_per_group: None,
        raft_max_uncommitted_bytes_per_group: None,
        live_read_max_waiters_per_core: Some(65_536),
    })
    .expect("spawn runtime")
}

fn stream_on_group(runtime: &ShardRuntime, group_id: RaftGroupId, prefix: &str) -> BucketStreamId {
    for index in 0..10_000 {
        let stream = BucketStreamId::new("benchcmp", format!("{prefix}-{index}"));
        if runtime.locate(&stream).raft_group_id == group_id {
            return stream;
        }
    }
    panic!("could not find stream for group {}", group_id.0);
}

async fn create_stream(runtime: &ShardRuntime, stream: &BucketStreamId) -> CreateStreamResponse {
    runtime
        .create_stream(CreateStreamRequest::new(
            stream.clone(),
            DEFAULT_CONTENT_TYPE,
        ))
        .await
        .expect("create stream")
}

fn producer(id: &str, epoch: u64, seq: u64) -> ProducerRequest {
    ProducerRequest {
        producer_id: id.to_owned(),
        producer_epoch: epoch,
        producer_seq: seq,
    }
}

fn empty_integrity() -> StreamIntegritySnapshot {
    let empty = "00".repeat(32);
    StreamIntegritySnapshot {
        live_setsum: empty.clone(),
        evicted_setsum: empty.clone(),
        total_setsum: empty,
        live_start_offset: 0,
        tail_offset: 0,
        live_records: 0,
        evicted_records: 0,
        total_records: 0,
    }
}

#[test]
fn stream_from_replicated_preserves_wire_message() {
    let err = GroupEngineError::stream_from_replicated(
        "wire message already includes code",
        StreamErrorCode::StreamGone,
        Some(7),
        vec![StreamErrorContext::StreamClosed],
    );

    assert_eq!(err.message(), "wire message already includes code");
    assert_eq!(err.code(), Some(StreamErrorCode::StreamGone));
    assert_eq!(err.next_offset(), Some(7));
    assert_eq!(err.context(), &[StreamErrorContext::StreamClosed]);
}

#[test]
fn stream_parts_returns_stream_fields_only() {
    let err = GroupEngineError::stream_from_replicated(
        "wire stream message",
        StreamErrorCode::ProducerSeqConflict,
        Some(11),
        vec![StreamErrorContext::ProducerSeqConflict {
            expected_seq: 10,
            received_seq: 7,
        }],
    );

    let (message, code, next_offset, context) = err
        .stream_parts()
        .expect("stream error exposes stream parts");
    assert_eq!(message, "wire stream message");
    assert_eq!(code, StreamErrorCode::ProducerSeqConflict);
    assert_eq!(next_offset, Some(11));
    assert_eq!(
        context,
        &[StreamErrorContext::ProducerSeqConflict {
            expected_seq: 10,
            received_seq: 7,
        }]
    );

    assert!(GroupEngineError::new("infra").stream_parts().is_none());
    assert!(
        GroupEngineError::forward_to_leader("forward", None, None)
            .stream_parts()
            .is_none()
    );
}

#[test]
fn runtime_error_status_classifies_retryable_and_permanent_errors() {
    let backpressure = RuntimeError::LiveReadBackpressure {
        core_id: CoreId(0),
        current_waiters: 65_536,
        limit: 65_536,
    };
    assert_eq!(backpressure.status(), ErrorStatus::Temporary);

    let conflict = RuntimeError::GroupEngine {
        core_id: CoreId(0),
        raft_group_id: RaftGroupId(0),
        error: GroupEngineError::stream(
            StreamErrorCode::StreamSeqConflict,
            "expected sequence 1 received 0",
        ),
    };
    assert_eq!(conflict.status(), ErrorStatus::Permanent);

    let internal = RuntimeError::GroupEngine {
        core_id: CoreId(0),
        raft_group_id: RaftGroupId(0),
        error: GroupEngineError::new("OpenRaft client_write: timeout after retries"),
    };
    assert_eq!(internal.status(), ErrorStatus::Persistent);
}

#[test]
fn group_engine_error_variants_separate_stream_infra_and_forwarding() {
    let stream = GroupEngineError::stream_with_context(
        StreamErrorCode::ProducerSeqConflict,
        "producer conflict",
        Some(9),
        vec![StreamErrorContext::ProducerSeqConflict {
            expected_seq: 8,
            received_seq: 3,
        }],
    );
    assert!(matches!(stream, GroupEngineError::Stream(_)));

    let infra = GroupEngineError::new("OpenRaft client_write failed");
    assert!(matches!(
        infra,
        GroupEngineError::Infra(GroupInfraError::Internal { .. })
    ));

    let forward = GroupEngineError::forward_to_leader("forward to leader", Some(2), None);
    assert!(matches!(forward, GroupEngineError::ForwardToLeader { .. }));
}

#[test]
fn stale_cold_flush_candidate_classification_uses_context_not_message_text() {
    let placement = ShardPlacement {
        core_id: CoreId(0),
        shard_id: ShardId(0),
        raft_group_id: RaftGroupId(0),
    };
    let message = "cold chunk end 18 is beyond stream 'benchcmp/stale' tail 17";
    let without_context = RuntimeError::group_engine(
        placement,
        GroupEngineError::stream(StreamErrorCode::InvalidColdFlush, message),
    );
    assert!(
        !crate::metrics::is_stale_cold_flush_candidate_error(&without_context),
        "stale classification must not be inferred from message text"
    );

    let with_context = RuntimeError::group_engine(
        placement,
        GroupEngineError::stream_with_context(
            StreamErrorCode::InvalidColdFlush,
            "cold flush candidate is stale",
            None,
            vec![StreamErrorContext::StaleColdFlushCandidate],
        ),
    );
    assert!(crate::metrics::is_stale_cold_flush_candidate_error(
        &with_context
    ));
}

fn placement() -> ShardPlacement {
    ShardPlacement {
        core_id: CoreId(0),
        shard_id: ShardId(0),
        raft_group_id: RaftGroupId(0),
    }
}

#[test]
fn group_write_command_round_trips_as_log_payload() {
    let command = GroupWriteCommand::AppendBatch {
        stream_id: BucketStreamId::new("benchcmp", "raft-log"),
        content_type: DEFAULT_CONTENT_TYPE.to_owned(),
        payloads: vec![Bytes::from_static(b"ab"), Bytes::from_static(b"cd")],
        producer: Some(producer("writer-1", 7, 42)),
        now_ms: 0,
    };

    let encoded = serde_json::to_vec(&command).expect("encode command");
    let decoded = serde_json::from_slice::<GroupWriteCommand>(&encoded).expect("decode command");

    assert_eq!(decoded, command);
}

#[test]
fn committed_write_command_is_state_machine_apply_boundary() {
    let placement = ShardPlacement {
        core_id: CoreId(0),
        shard_id: ShardId(0),
        raft_group_id: RaftGroupId(0),
    };
    let stream = BucketStreamId::new("benchcmp", "apply-command");
    let mut engine = InMemoryGroupEngine::default();

    let created = engine
        .apply_committed_write(
            GroupWriteCommand::CreateStream {
                stream_id: stream.clone(),
                content_type: DEFAULT_CONTENT_TYPE.to_owned(),
                initial_payload: Bytes::new(),
                close_after: false,
                stream_seq: None,
                producer: None,
                stream_ttl_seconds: None,
                stream_expires_at_ms: None,
                forked_from: None,
                fork_offset: None,
                now_ms: 0,
            },
            placement,
        )
        .expect("create stream");
    assert_eq!(
        created,
        GroupWriteResponse::CreateStream(CreateStreamResponse {
            placement,
            next_offset: 0,
            closed: false,
            already_exists: false,
            group_commit_index: 1,
        })
    );

    let appended = engine
        .apply_committed_write(
            GroupWriteCommand::Append {
                stream_id: stream.clone(),
                content_type: DEFAULT_CONTENT_TYPE.to_owned(),
                payload: Bytes::from_static(b"abc"),
                close_after: false,
                stream_seq: None,
                producer: None,
                now_ms: 0,
            },
            placement,
        )
        .expect("append");
    assert_eq!(
        appended,
        GroupWriteResponse::Append(AppendResponse {
            placement,
            start_offset: 0,
            next_offset: 3,
            stream_append_count: 1,
            group_commit_index: 2,
            closed: false,
            deduplicated: false,
            producer: None,
        })
    );

    let flushed = engine
        .apply_committed_write(
            GroupWriteCommand::FlushCold {
                stream_id: stream.clone(),
                chunk: ColdChunkRef {
                    start_offset: 0,
                    end_offset: 2,
                    s3_path: "s3://bucket/apply-command/000000".to_owned(),
                    object_size: 2,
                },
            },
            placement,
        )
        .expect("flush cold");
    assert_eq!(
        flushed,
        GroupWriteResponse::FlushCold(FlushColdResponse {
            placement,
            hot_start_offset: 2,
            group_commit_index: 3,
        })
    );

    let read = engine
        .state_machine
        .read(&stream, 2, 16)
        .expect("read applied command");
    assert_eq!(read.payload, b"c");
    let plan = engine
        .state_machine
        .read_plan(&stream, 0, 16)
        .expect("read plan");
    assert_eq!(plan.segments.len(), 2);
    assert!(matches!(plan.segments[0], StreamReadSegment::ColdIndex(_)));
    assert_eq!(plan.segments[1], StreamReadSegment::Hot(b"c".to_vec()));
}

#[tokio::test]
async fn cold_store_read_reassembles_cold_and_hot_segments() {
    let placement = placement();
    let stream = BucketStreamId::new("benchcmp", "cold-read");
    let cold_store = Arc::new(ColdStore::memory().expect("memory cold store"));
    cold_store
        .write_chunk("benchcmp/cold-read/chunks/000000.bin", b"abcd")
        .await
        .expect("write cold object");
    let mut engine = InMemoryGroupEngine::with_cold_store(cold_store);

    engine
        .apply_committed_write(
            GroupWriteCommand::CreateStream {
                stream_id: stream.clone(),
                content_type: DEFAULT_CONTENT_TYPE.to_owned(),
                initial_payload: Bytes::new(),
                close_after: false,
                stream_seq: None,
                producer: None,
                stream_ttl_seconds: None,
                stream_expires_at_ms: None,
                forked_from: None,
                fork_offset: None,
                now_ms: 0,
            },
            placement,
        )
        .expect("create stream");
    engine
        .apply_committed_write(
            GroupWriteCommand::Append {
                stream_id: stream.clone(),
                content_type: DEFAULT_CONTENT_TYPE.to_owned(),
                payload: Bytes::from_static(b"abcdef"),
                close_after: false,
                stream_seq: None,
                producer: None,
                now_ms: 0,
            },
            placement,
        )
        .expect("append");
    engine
        .flush_cold(
            FlushColdRequest {
                stream_id: stream.clone(),
                chunk: ColdChunkRef {
                    start_offset: 0,
                    end_offset: 4,
                    s3_path: "benchcmp/cold-read/chunks/000000.bin".to_owned(),
                    object_size: 4,
                },
            },
            placement,
        )
        .await
        .expect("flush cold");

    let read = engine
        .read_stream(
            ReadStreamRequest {
                stream_id: stream,
                offset: 2,
                max_len: 4,
                now_ms: 0,
            },
            placement,
        )
        .await
        .expect("read cold and hot segments");
    assert_eq!(read.payload, b"cdef");
    assert_eq!(read.next_offset, 6);
    assert!(read.up_to_date);
}

#[tokio::test]
async fn stale_cold_flush_rolls_back_index_page_entry() {
    let placement = placement();
    let stream = BucketStreamId::new("benchcmp", "stale-cold-index");
    let live_path = "benchcmp/stale-cold-index/chunks/live.bin";
    let stale_path = "benchcmp/stale-cold-index/chunks/stale.bin";
    let cold_store = Arc::new(ColdStore::memory().expect("memory cold store"));
    cold_store
        .write_chunk(live_path, b"abcd")
        .await
        .expect("write live cold object");
    cold_store
        .write_chunk(stale_path, b"abcd")
        .await
        .expect("write stale cold object");
    let mut engine = InMemoryGroupEngine::with_cold_store(cold_store.clone());

    engine
        .create_stream(
            CreateStreamRequest::new(stream.clone(), DEFAULT_CONTENT_TYPE),
            placement,
        )
        .await
        .expect("create stream");
    engine
        .append(
            AppendRequest::from_bytes(stream.clone(), b"abcdef".to_vec()),
            placement,
        )
        .await
        .expect("append");
    engine
        .flush_cold(
            FlushColdRequest {
                stream_id: stream.clone(),
                chunk: ColdChunkRef {
                    start_offset: 0,
                    end_offset: 4,
                    s3_path: live_path.to_owned(),
                    object_size: 4,
                },
            },
            placement,
        )
        .await
        .expect("flush live cold chunk");

    let stale_flush = engine
        .flush_cold(
            FlushColdRequest {
                stream_id: stream.clone(),
                chunk: ColdChunkRef {
                    start_offset: 0,
                    end_offset: 4,
                    s3_path: stale_path.to_owned(),
                    object_size: 4,
                },
            },
            placement,
        )
        .await
        .expect_err("duplicate cold flush should be stale");
    assert!(
        stale_flush
            .message()
            .contains("must start at the hot prefix"),
        "message={}",
        stale_flush.message()
    );
    cold_store
        .delete_chunk(stale_path)
        .await
        .expect("delete stale cold object");

    let read = engine
        .read_stream(
            ReadStreamRequest {
                stream_id: stream,
                offset: 0,
                max_len: 6,
                now_ms: 0,
            },
            placement,
        )
        .await
        .expect("read should still use live cold index entry");
    assert_eq!(read.payload, b"abcdef");
}

#[tokio::test]
async fn external_payload_index_pages_are_not_kept_in_snapshot_memory() {
    let cold_store = Arc::new(ColdStore::memory().expect("memory cold store"));
    let runtime = ShardRuntime::spawn_with_engine_factory_and_cold_store(
        RuntimeConfig::new(1, 1),
        InMemoryGroupEngineFactory::with_cold_store(Some(cold_store.clone())),
        Some(cold_store.clone()),
    )
    .expect("spawn runtime");
    let stream = BucketStreamId::new("benchcmp", "external-index");

    cold_store
        .write_chunk("benchcmp/external-index/external/initial.bin", b"ab")
        .await
        .expect("write initial external payload");
    runtime
        .create_stream_external(CreateStreamExternalRequest {
            stream_id: stream.clone(),
            content_type: DEFAULT_CONTENT_TYPE.to_owned(),
            initial_payload: ExternalPayloadRef {
                s3_path: "benchcmp/external-index/external/initial.bin".to_owned(),
                payload_len: 2,
                object_size: 2,
            },
            close_after: false,
            stream_seq: None,
            producer: None,
            stream_ttl_seconds: None,
            stream_expires_at_ms: None,
            forked_from: None,
            fork_offset: None,
            now_ms: 0,
        })
        .await
        .expect("create external stream");
    runtime
        .append(AppendRequest::from_bytes(stream.clone(), b"cd".to_vec()))
        .await
        .expect("append hot");
    cold_store
        .write_chunk("benchcmp/external-index/external/tail.bin", b"ef")
        .await
        .expect("write tail external payload");
    runtime
        .append_external(AppendExternalRequest {
            stream_id: stream.clone(),
            content_type: DEFAULT_CONTENT_TYPE.to_owned(),
            payload: ExternalPayloadRef {
                s3_path: "benchcmp/external-index/external/tail.bin".to_owned(),
                payload_len: 2,
                object_size: 2,
            },
            close_after: false,
            stream_seq: None,
            producer: None,
            now_ms: 0,
        })
        .await
        .expect("append external payload");

    let read = runtime
        .read_stream(ReadStreamRequest {
            stream_id: stream.clone(),
            offset: 0,
            max_len: 6,
            now_ms: 0,
        })
        .await
        .expect("read mixed external and hot payload");
    assert_eq!(read.payload, b"abcdef");
    assert_eq!(read.next_offset, 6);

    let snapshot = runtime
        .snapshot_group(runtime.locate(&stream).raft_group_id)
        .await
        .expect("snapshot group");
    let entry = snapshot
        .stream_snapshot
        .streams
        .iter()
        .find(|entry| entry.metadata.stream_id == stream)
        .expect("snapshot entry");
    assert_eq!(entry.cold_frontier_offset, 6);
    assert!(entry.cold_chunks.is_empty());
    assert!(entry.external_segments.is_empty());
}

#[tokio::test]
async fn bootstrap_reads_retained_updates_from_cold_chunk_after_snapshot() {
    let placement = placement();
    let stream = BucketStreamId::new("benchcmp", "cold-bootstrap");
    let cold_store = Arc::new(ColdStore::memory().expect("memory cold store"));
    cold_store
        .write_chunk("benchcmp/cold-bootstrap/chunks/000000.bin", b"abcde")
        .await
        .expect("write cold object");
    let mut engine = InMemoryGroupEngine::with_cold_store(cold_store);

    engine
        .create_stream(
            CreateStreamRequest::new(stream.clone(), DEFAULT_CONTENT_TYPE),
            placement,
        )
        .await
        .expect("create stream");
    engine
        .append(
            AppendRequest::from_bytes(stream.clone(), b"abc".to_vec()),
            placement,
        )
        .await
        .expect("append first message");
    engine
        .append(
            AppendRequest::from_bytes(stream.clone(), b"de".to_vec()),
            placement,
        )
        .await
        .expect("append second message");
    engine
        .flush_cold(
            FlushColdRequest {
                stream_id: stream.clone(),
                chunk: ColdChunkRef {
                    start_offset: 0,
                    end_offset: 5,
                    s3_path: "benchcmp/cold-bootstrap/chunks/000000.bin".to_owned(),
                    object_size: 5,
                },
            },
            placement,
        )
        .await
        .expect("flush all hot bytes");
    engine
        .publish_snapshot(
            PublishSnapshotRequest {
                stream_id: stream.clone(),
                snapshot_offset: 3,
                content_type: DEFAULT_CONTENT_TYPE.to_owned(),
                payload: Bytes::from_static(b"abc-state"),
                now_ms: 0,
            },
            placement,
        )
        .await
        .expect("publish snapshot");

    let read = engine
        .read_stream(
            ReadStreamRequest {
                stream_id: stream.clone(),
                offset: 3,
                max_len: 2,
                now_ms: 0,
            },
            placement,
        )
        .await
        .expect("read retained update from cold chunk");
    assert_eq!(read.payload, b"de");

    let bootstrap = engine
        .bootstrap_stream(
            BootstrapStreamRequest {
                stream_id: stream,
                now_ms: 0,
            },
            placement,
        )
        .await
        .expect("bootstrap");
    assert_eq!(bootstrap.snapshot_offset, Some(3));
    assert_eq!(bootstrap.snapshot_payload, b"abc-state");
    assert_eq!(bootstrap.next_offset, 5);
    assert_eq!(bootstrap.updates.len(), 1);
    assert_eq!(bootstrap.updates[0].start_offset, 3);
    assert_eq!(bootstrap.updates[0].next_offset, 5);
    assert_eq!(bootstrap.updates[0].payload, b"de");
}

#[tokio::test]
async fn cold_store_reads_only_requested_range() {
    let cold_store = ColdStore::memory().expect("memory cold store");
    cold_store
        .write_chunk("benchcmp/cold-range/chunks/000000.bin", b"abcdefgh")
        .await
        .expect("write cold object");
    let bytes = cold_store
        .read_chunk_range(
            &ColdChunkRef {
                start_offset: 10,
                end_offset: 18,
                s3_path: "benchcmp/cold-range/chunks/000000.bin".to_owned(),
                object_size: 8,
            },
            12,
            3,
        )
        .await
        .expect("read range");
    assert_eq!(bytes, b"cde");
}

#[tokio::test]
async fn cold_store_prefetches_sequential_stream_blocks() {
    let cold_store = ColdStore::memory()
        .expect("memory cold store")
        .with_read_cache(ColdReadCacheConfig {
            max_bytes: 32,
            block_bytes: 4,
            max_readahead_blocks: 2,
        });
    let path = "benchcmp/cold-cache/chunks/000000.bin";
    cold_store
        .write_chunk(path, b"abcdefghijklmnop")
        .await
        .expect("write cold object");
    let stream = BucketStreamId::new("benchcmp", "cold-cache");
    let object = ObjectPayloadRef {
        start_offset: 0,
        end_offset: 16,
        s3_path: path.to_owned(),
        object_size: 16,
    };

    let first = cold_store
        .read_object_range_for_stream(&stream, &object, 0, 4)
        .await
        .expect("read first block");
    assert_eq!(first, b"abcd");
    let second = cold_store
        .read_object_range_for_stream(&stream, &object, 4, 4)
        .await
        .expect("read second block");
    assert_eq!(second, b"efgh");

    for _ in 0..20 {
        if cold_store.cached_block_count() >= 3 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(cold_store.cached_block_count() >= 3);

    cold_store
        .delete_chunk(path)
        .await
        .expect("delete cold object");
    assert_eq!(cold_store.cached_block_count(), 0);
}

#[tokio::test]
async fn ttl_read_access_is_committed_and_expiry_removes_stream() {
    let placement = placement();
    let stream = BucketStreamId::new("benchcmp", "runtime-ttl");
    let mut engine = InMemoryGroupEngine::default();

    let mut create = CreateStreamRequest::new(stream.clone(), DEFAULT_CONTENT_TYPE);
    create.initial_payload = Bytes::from_static(b"abc");
    create.stream_ttl_seconds = Some(1);
    create.now_ms = 1_000;
    engine
        .create_stream(create, placement)
        .await
        .expect("create ttl stream");

    let read = engine
        .read_stream(
            ReadStreamRequest {
                stream_id: stream.clone(),
                offset: 0,
                max_len: 16,
                now_ms: 1_500,
            },
            placement,
        )
        .await
        .expect("read renews ttl");
    assert_eq!(read.payload, b"abc");
    assert_eq!(
        engine
            .snapshot(placement)
            .await
            .expect("snapshot")
            .group_commit_index,
        2
    );

    engine
        .head_stream(
            HeadStreamRequest {
                stream_id: stream.clone(),
                now_ms: 2_499,
            },
            placement,
        )
        .await
        .expect("head does not renew but stream is still live");
    assert_eq!(
        engine
            .snapshot(placement)
            .await
            .expect("snapshot")
            .group_commit_index,
        2
    );

    let err = engine
        .read_stream(
            ReadStreamRequest {
                stream_id: stream.clone(),
                offset: 0,
                max_len: 16,
                now_ms: 2_500,
            },
            placement,
        )
        .await
        .expect_err("expired stream read is not found");
    assert_eq!(err.code(), Some(StreamErrorCode::StreamNotFound));
    assert_eq!(
        engine
            .snapshot(placement)
            .await
            .expect("snapshot")
            .group_commit_index,
        3
    );

    let mut recreate = CreateStreamRequest::new(stream, "text/plain");
    recreate.now_ms = 2_501;
    let recreated = engine
        .create_stream(recreate, placement)
        .await
        .expect("recreate expired stream");
    assert!(!recreated.already_exists);
}

#[test]
fn committed_write_batch_preserves_logical_command_responses() {
    let placement = placement();
    let stream = BucketStreamId::new("benchcmp", "apply-command-batch");
    let mut engine = InMemoryGroupEngine::default();

    let response = engine
        .apply_committed_write(
            GroupWriteCommand::Batch {
                commands: vec![
                    GroupWriteCommand::from(CreateStreamRequest::new(
                        stream.clone(),
                        DEFAULT_CONTENT_TYPE,
                    )),
                    GroupWriteCommand::from(AppendBatchRequest::new(
                        stream.clone(),
                        vec![Bytes::from_static(b"ab"), Bytes::from_static(b"cd")],
                    )),
                ],
            },
            placement,
        )
        .expect("apply command batch");

    let GroupWriteResponse::Batch(items) = response else {
        panic!("unexpected batch response: {response:?}");
    };
    assert_eq!(items.len(), 2);
    assert!(matches!(
        &items[0],
        Ok(GroupWriteResponse::CreateStream(CreateStreamResponse {
            group_commit_index: 1,
            ..
        }))
    ));
    match &items[1] {
        Ok(GroupWriteResponse::AppendBatch(response)) => {
            assert_eq!(response.items.len(), 2);
            assert_eq!(
                response.items[0].as_ref().expect("first item").start_offset,
                0
            );
            assert_eq!(
                response.items[1]
                    .as_ref()
                    .expect("second item")
                    .start_offset,
                2
            );
            assert_eq!(
                response.items[1]
                    .as_ref()
                    .expect("second item")
                    .group_commit_index,
                3
            );
        }
        other => panic!("unexpected append batch response: {other:?}"),
    }

    let read = engine
        .state_machine
        .read(&stream, 0, 16)
        .expect("read applied command batch");
    assert_eq!(read.payload, b"abcd");
}

async fn wait_for_live_waiters(runtime: &ShardRuntime, expected: u64) {
    for _ in 0..100 {
        if runtime.metrics().snapshot().live_read_waiters == expected {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    panic!(
        "expected {expected} live waiters, got {}",
        runtime.metrics().snapshot().live_read_waiters
    );
}

async fn wait_for_mailbox_depth(runtime: &ShardRuntime, core_index: usize, expected: usize) {
    for _ in 0..100 {
        if runtime.mailbox_snapshot().depths[core_index] == expected {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    panic!(
        "expected core {core_index} mailbox depth {expected}, got {}",
        runtime.mailbox_snapshot().depths[core_index]
    );
}

async fn wait_for_mailbox_full_events(runtime: &ShardRuntime, expected: u64) {
    for _ in 0..100 {
        if runtime.metrics().snapshot().mailbox_full_events == expected {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    panic!(
        "expected {expected} mailbox full events, got {}",
        runtime.metrics().snapshot().mailbox_full_events
    );
}

async fn wait_for_group_mailbox_full_events(runtime: &ShardRuntime, expected: u64) {
    for _ in 0..100 {
        if runtime.metrics().snapshot().group_mailbox_full_events == expected {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    panic!(
        "expected {expected} group mailbox full events, got {}",
        runtime.metrics().snapshot().group_mailbox_full_events
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn repeated_appends_to_one_stream_are_ordered() {
    let runtime = runtime(4, 32);
    let stream = BucketStreamId::new("benchcmp", "one-stream");
    create_stream(&runtime, &stream).await;
    for index in 0..100 {
        let response = runtime
            .append(AppendRequest::new(stream.clone(), 7))
            .await
            .expect("append");
        assert_eq!(response.start_offset, index * 7);
        assert_eq!(response.next_offset, (index + 1) * 7);
        assert_eq!(response.stream_append_count, index + 1);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn independent_streams_reach_all_cores_and_many_groups() {
    let runtime = runtime(4, 64);
    const STREAM_COUNT: usize = 4096;
    const MAX_IN_FLIGHT: usize = 64;

    for chunk_start in (0..STREAM_COUNT).step_by(MAX_IN_FLIGHT) {
        let chunk_end = (chunk_start + MAX_IN_FLIGHT).min(STREAM_COUNT);
        let mut tasks = Vec::with_capacity(chunk_end - chunk_start);
        for index in chunk_start..chunk_end {
            let runtime = runtime.clone();
            tasks.push(tokio::spawn(async move {
                let stream = BucketStreamId::new("benchcmp", format!("stream-{index}"));
                create_stream(&runtime, &stream).await;
                runtime
                    .append(AppendRequest::new(stream, 1))
                    .await
                    .expect("append")
            }));
        }

        for task in tasks {
            let response = task.await.expect("task");
            assert_eq!(response.start_offset, 0);
            assert_eq!(response.next_offset, 1);
        }
    }

    let snapshot = runtime.metrics().snapshot();
    assert_eq!(snapshot.accepted_appends, STREAM_COUNT as u64);
    assert!(snapshot.per_core_appends.iter().all(|value| *value > 0));
    let active_groups = snapshot
        .per_group_appends
        .iter()
        .filter(|value| **value > 0)
        .count();
    assert!(active_groups > 48, "active_groups={active_groups}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn empty_append_is_rejected_before_routing() {
    let runtime = runtime(2, 8);
    let err = runtime
        .append(AppendRequest::new(BucketStreamId::new("b", "s"), 0))
        .await
        .expect_err("empty append rejected");
    assert_eq!(err, RuntimeError::EmptyAppend);
    assert_eq!(runtime.metrics().snapshot().accepted_appends, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn append_batch_routes_once_and_applies_each_payload_on_owner_core() {
    let runtime = runtime(2, 8);
    let stream = BucketStreamId::new("benchcmp", "batch-runtime");
    let owner_core = usize::from(runtime.locate(&stream).core_id.0);
    let owner_group =
        usize::try_from(runtime.locate(&stream).raft_group_id.0).expect("u32 fits usize");

    create_stream(&runtime, &stream).await;
    let response = runtime
        .append_batch(AppendBatchRequest::new(
            stream.clone(),
            vec![b"ab".to_vec(), b"c".to_vec(), b"def".to_vec()],
        ))
        .await
        .expect("append batch");
    assert_eq!(response.items.len(), 3);
    assert_eq!(response.items[0].as_ref().expect("first").start_offset, 0);
    assert_eq!(response.items[1].as_ref().expect("second").start_offset, 2);
    assert_eq!(response.items[2].as_ref().expect("third").start_offset, 3);

    let read = runtime
        .read_stream(ReadStreamRequest {
            stream_id: stream.clone(),
            offset: 0,
            max_len: 16,
            now_ms: 0,
        })
        .await
        .expect("read");
    assert_eq!(read.payload, b"abcdef");

    let snapshot = runtime.metrics().snapshot();
    assert_eq!(snapshot.accepted_appends, 3);
    assert_eq!(snapshot.applied_mutations, 4);
    assert_eq!(snapshot.routed_requests, 3);
    assert_eq!(snapshot.per_core_appends[owner_core], 3);
    assert_eq!(snapshot.per_group_appends[owner_group], 3);
    assert_eq!(snapshot.per_core_applied_mutations[owner_core], 4);
    assert_eq!(snapshot.per_group_applied_mutations[owner_group], 4);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn append_batch_reports_item_errors_without_stopping_later_payloads() {
    let runtime = runtime(2, 8);
    let stream = BucketStreamId::new("benchcmp", "batch-partial");
    create_stream(&runtime, &stream).await;

    let response = runtime
        .append_batch(AppendBatchRequest::new(
            stream.clone(),
            vec![b"a".to_vec(), Vec::new(), b"b".to_vec()],
        ))
        .await
        .expect("append batch");
    assert!(response.items[0].is_ok());
    assert!(response.items[1].is_err());
    assert!(response.items[2].is_ok());
    assert_eq!(response.items[0].as_ref().expect("first").start_offset, 0);
    assert_eq!(response.items[2].as_ref().expect("third").start_offset, 1);

    let read = runtime
        .read_stream(ReadStreamRequest {
            stream_id: stream,
            offset: 0,
            max_len: 16,
            now_ms: 0,
        })
        .await
        .expect("read");
    assert_eq!(read.payload, b"ab");

    let snapshot = runtime.metrics().snapshot();
    assert_eq!(snapshot.accepted_appends, 2);
    assert_eq!(snapshot.applied_mutations, 3);
    assert_eq!(snapshot.routed_requests, 3);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn producer_duplicate_append_returns_prior_offsets_without_mutating_metrics() {
    let runtime = runtime(2, 8);
    let stream = BucketStreamId::new("benchcmp", "producer-runtime");
    create_stream(&runtime, &stream).await;

    let mut first = AppendRequest::from_bytes(stream.clone(), b"a".to_vec());
    first.producer = Some(producer("writer-1", 0, 0));
    let first = runtime.append(first).await.expect("first append");
    assert_eq!(first.start_offset, 0);
    assert_eq!(first.next_offset, 1);
    assert_eq!(first.stream_append_count, 1);
    assert!(!first.deduplicated);

    let mut duplicate = AppendRequest::from_bytes(stream.clone(), b"ignored".to_vec());
    duplicate.producer = Some(producer("writer-1", 0, 0));
    let duplicate = runtime.append(duplicate).await.expect("duplicate append");
    assert_eq!(duplicate.start_offset, 0);
    assert_eq!(duplicate.next_offset, 1);
    assert_eq!(duplicate.stream_append_count, 1);
    assert!(duplicate.deduplicated);

    let mut next = AppendRequest::from_bytes(stream.clone(), b"b".to_vec());
    next.producer = Some(producer("writer-1", 0, 1));
    let next = runtime.append(next).await.expect("next append");
    assert_eq!(next.start_offset, 1);
    assert_eq!(next.next_offset, 2);
    assert_eq!(next.stream_append_count, 2);
    assert!(!next.deduplicated);

    let read = runtime
        .read_stream(ReadStreamRequest {
            stream_id: stream,
            offset: 0,
            max_len: 16,
            now_ms: 0,
        })
        .await
        .expect("read");
    assert_eq!(read.payload, b"ab");

    let metrics = runtime.metrics().snapshot();
    assert_eq!(metrics.accepted_appends, 2);
    assert_eq!(metrics.applied_mutations, 3);
    assert_eq!(metrics.routed_requests, 5);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn producer_duplicate_append_batch_returns_prior_offsets_without_mutating_metrics() {
    let runtime = runtime(2, 8);
    let stream = BucketStreamId::new("benchcmp", "producer-batch-runtime");
    create_stream(&runtime, &stream).await;

    let mut first = AppendBatchRequest::new(stream.clone(), vec![b"ab".to_vec(), b"c".to_vec()]);
    first.producer = Some(producer("writer-1", 0, 0));
    let first = runtime.append_batch(first).await.expect("first batch");
    assert_eq!(first.items.len(), 2);
    let first_item = first.items[0].as_ref().expect("first item");
    let second_item = first.items[1].as_ref().expect("second item");
    assert_eq!(first_item.start_offset, 0);
    assert_eq!(first_item.next_offset, 2);
    assert_eq!(first_item.stream_append_count, 1);
    assert!(!first_item.deduplicated);
    assert_eq!(second_item.start_offset, 2);
    assert_eq!(second_item.next_offset, 3);
    assert_eq!(second_item.stream_append_count, 2);
    assert!(!second_item.deduplicated);

    let mut duplicate =
        AppendBatchRequest::new(stream.clone(), vec![b"ignored".to_vec(), b"body".to_vec()]);
    duplicate.producer = Some(producer("writer-1", 0, 0));
    let duplicate = runtime
        .append_batch(duplicate)
        .await
        .expect("duplicate batch");
    assert_eq!(duplicate.items.len(), 2);
    assert!(
        duplicate
            .items
            .iter()
            .all(|item| { item.as_ref().expect("deduplicated item").deduplicated })
    );
    assert_eq!(
        duplicate.items[0]
            .as_ref()
            .expect("first duplicate")
            .start_offset,
        0
    );
    assert_eq!(
        duplicate.items[1]
            .as_ref()
            .expect("second duplicate")
            .next_offset,
        3
    );

    let mut next = AppendBatchRequest::new(stream.clone(), vec![b"d".to_vec()]);
    next.producer = Some(producer("writer-1", 0, 1));
    let next = runtime.append_batch(next).await.expect("next batch");
    let next_item = next.items[0].as_ref().expect("next item");
    assert_eq!(next_item.start_offset, 3);
    assert_eq!(next_item.next_offset, 4);
    assert_eq!(next_item.stream_append_count, 3);
    assert!(!next_item.deduplicated);

    let read = runtime
        .read_stream(ReadStreamRequest {
            stream_id: stream,
            offset: 0,
            max_len: 16,
            now_ms: 0,
        })
        .await
        .expect("read");
    assert_eq!(read.payload, b"abcd");

    let metrics = runtime.metrics().snapshot();
    assert_eq!(metrics.accepted_appends, 3);
    assert_eq!(metrics.applied_mutations, 4);
    assert_eq!(metrics.routed_requests, 5);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn snapshot_group_routes_to_owner_core_and_captures_only_group_state() {
    let runtime = runtime(2, 8);
    let first_stream = BucketStreamId::new("benchcmp", "snapshot-first");
    let first_placement = runtime.locate(&first_stream);
    let second_stream = (0..512)
        .map(|index| BucketStreamId::new("benchcmp", format!("snapshot-other-{index}")))
        .find(|stream| runtime.locate(stream).core_id != first_placement.core_id)
        .expect("stream on another core");

    create_stream(&runtime, &first_stream).await;
    runtime
        .append(AppendRequest::from_bytes(
            first_stream.clone(),
            b"first".to_vec(),
        ))
        .await
        .expect("append first stream");
    create_stream(&runtime, &second_stream).await;
    runtime
        .append(AppendRequest::from_bytes(
            second_stream.clone(),
            b"second".to_vec(),
        ))
        .await
        .expect("append second stream");

    let snapshot = runtime
        .snapshot_group(first_placement.raft_group_id)
        .await
        .expect("snapshot group");
    assert_eq!(snapshot.placement, first_placement);
    assert_eq!(snapshot.group_commit_index, 2);
    assert_eq!(snapshot.stream_snapshot.buckets, vec!["benchcmp"]);
    assert_eq!(
        snapshot
            .stream_snapshot
            .streams
            .iter()
            .map(|entry| entry.metadata.stream_id.clone())
            .collect::<Vec<_>>(),
        vec![first_stream.clone()]
    );

    let restored =
        StreamStateMachine::restore(snapshot.stream_snapshot).expect("restore group snapshot");
    let read = restored
        .read(&first_stream, 0, 16)
        .expect("read restored snapshot");
    assert_eq!(read.payload, b"first");
    assert_eq!(read.next_offset, 5);
    assert!(restored.read(&second_stream, 0, 16).is_err());

    let metrics = runtime.metrics().snapshot();
    assert_eq!(metrics.routed_requests, 5);
    assert_eq!(
        metrics.per_core_routed_requests[usize::from(first_placement.core_id.0)],
        3
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn snapshot_group_rejects_out_of_range_group_before_routing() {
    let runtime = runtime(2, 8);
    let err = runtime
        .snapshot_group(RaftGroupId(8))
        .await
        .expect_err("invalid group");
    assert_eq!(
        err,
        RuntimeError::InvalidRaftGroup {
            raft_group_id: RaftGroupId(8),
            raft_group_count: 8,
        }
    );
    assert_eq!(runtime.metrics().snapshot().routed_requests, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn install_group_snapshot_restores_group_state_and_append_counts() {
    let source = runtime(2, 8);
    let stream = BucketStreamId::new("benchcmp", "install-snapshot");
    let placement = source.locate(&stream);
    create_stream(&source, &stream).await;
    source
        .append(AppendRequest::from_bytes(stream.clone(), b"ab".to_vec()))
        .await
        .expect("append first");
    source
        .append(AppendRequest::from_bytes(stream.clone(), b"cd".to_vec()))
        .await
        .expect("append second");

    let snapshot = source
        .snapshot_group(placement.raft_group_id)
        .await
        .expect("snapshot group");
    assert_eq!(snapshot.group_commit_index, 3);
    assert_eq!(
        snapshot.stream_append_counts,
        vec![StreamAppendCount {
            stream_id: stream.clone(),
            append_count: 2,
        }]
    );

    let target = runtime(2, 8);
    target
        .install_group_snapshot(snapshot)
        .await
        .expect("install snapshot");

    let read = target
        .read_stream(ReadStreamRequest {
            stream_id: stream.clone(),
            offset: 0,
            max_len: 16,
            now_ms: 0,
        })
        .await
        .expect("read restored stream");
    assert_eq!(read.placement, placement);
    assert_eq!(read.payload, b"abcd");
    assert_eq!(read.next_offset, 4);

    let appended = target
        .append(AppendRequest::from_bytes(stream, b"ef".to_vec()))
        .await
        .expect("append after restore");
    assert_eq!(appended.start_offset, 4);
    assert_eq!(appended.next_offset, 6);
    assert_eq!(appended.stream_append_count, 3);
    assert_eq!(appended.group_commit_index, 4);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn snapshot_after_stream_delete_installs_without_dangling_append_count() {
    // Regression: a deleted stream must not leave a stale entry in the runtime
    // append-count map. Such an entry would make the snapshot fail every
    // follower's install with "append count references missing snapshot stream",
    // so a lagging node could never catch up and leadership transfer (which
    // catches the target up via a snapshot) could never complete.
    let source = runtime(2, 8);
    let stream = BucketStreamId::new("benchcmp", "churn-delete");
    let placement = source.locate(&stream);
    create_stream(&source, &stream).await;
    source
        .append(AppendRequest::from_bytes(stream.clone(), b"ab".to_vec()))
        .await
        .expect("append");
    source
        .delete_stream(DeleteStreamRequest {
            stream_id: stream.clone(),
        })
        .await
        .expect("delete");

    let snapshot = source
        .snapshot_group(placement.raft_group_id)
        .await
        .expect("snapshot group");
    assert!(
        snapshot
            .stream_append_counts
            .iter()
            .all(|count| count.stream_id != stream),
        "snapshot must not carry an append count for the deleted stream: {:?}",
        snapshot.stream_append_counts
    );

    // Previously this failed the restore consistency check.
    let target = runtime(2, 8);
    target
        .install_group_snapshot(snapshot)
        .await
        .expect("install snapshot after delete");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn install_group_snapshot_rejects_mismatched_placement_before_routing() {
    let runtime = runtime(2, 8);
    let snapshot = GroupSnapshot {
        placement: ShardPlacement {
            core_id: CoreId(1),
            shard_id: ShardId(0),
            raft_group_id: RaftGroupId(0),
        },
        group_commit_index: 0,
        stream_snapshot: StreamSnapshot {
            buckets: Vec::new(),
            streams: Vec::new(),
            pending_cold_gc: Vec::new(),
            next_cold_gc_seq: 0,
        },
        stream_append_counts: Vec::new(),
    };

    let err = runtime
        .install_group_snapshot(snapshot)
        .await
        .expect_err("mismatched placement rejected");
    assert_eq!(
        err,
        RuntimeError::SnapshotPlacementMismatch {
            expected: ShardPlacement {
                core_id: CoreId(0),
                shard_id: ShardId(0),
                raft_group_id: RaftGroupId(0),
            },
            actual: ShardPlacement {
                core_id: CoreId(1),
                shard_id: ShardId(0),
                raft_group_id: RaftGroupId(0),
            },
        }
    );
    assert_eq!(runtime.metrics().snapshot().routed_requests, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mailbox_snapshot_reports_per_core_depths_and_capacities() {
    let runtime = ShardRuntime::spawn(RuntimeConfig {
        core_count: 3,
        raft_group_count: 9,
        mailbox_capacity: 7,
        threading: RuntimeThreading::HostedTokio,
        cold_max_hot_bytes_per_group: None,
        raft_max_uncommitted_bytes_per_group: None,
        live_read_max_waiters_per_core: Some(65_536),
    })
    .expect("spawn runtime");

    let snapshot = runtime.mailbox_snapshot();
    assert_eq!(snapshot.depths, vec![0, 0, 0]);
    assert_eq!(snapshot.capacities, vec![7, 7, 7]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn runtime_metrics_track_owner_core_routing_and_mailbox_wait() {
    let runtime = runtime(2, 8);
    let stream = BucketStreamId::new("benchcmp", "routing-metrics");
    let owner_core = usize::from(runtime.locate(&stream).core_id.0);

    create_stream(&runtime, &stream).await;
    runtime
        .append(AppendRequest::from_bytes(stream.clone(), b"hello".to_vec()))
        .await
        .expect("append");
    runtime
        .read_stream(ReadStreamRequest {
            stream_id: stream.clone(),
            offset: 0,
            max_len: 16,
            now_ms: 0,
        })
        .await
        .expect("read");

    let snapshot = runtime.metrics().snapshot();
    assert_eq!(snapshot.accepted_appends, 1);
    assert_eq!(snapshot.applied_mutations, 2);
    assert_eq!(snapshot.routed_requests, 3);
    assert_eq!(snapshot.per_core_routed_requests.len(), 2);
    assert_eq!(snapshot.per_core_routed_requests[owner_core], 3);
    assert_eq!(snapshot.per_core_applied_mutations[owner_core], 2);
    assert_eq!(
        snapshot.per_group_applied_mutations
            [usize::try_from(runtime.locate(&stream).raft_group_id.0).expect("u32 fits usize")],
        2
    );
    assert_eq!(
        snapshot.mutation_apply_ns,
        snapshot.per_core_mutation_apply_ns.iter().sum::<u64>()
    );
    assert_eq!(
        snapshot.mutation_apply_ns,
        snapshot.per_group_mutation_apply_ns.iter().sum::<u64>()
    );
    assert_eq!(
        snapshot.group_lock_wait_ns,
        snapshot.per_core_group_lock_wait_ns.iter().sum::<u64>()
    );
    assert_eq!(
        snapshot.group_lock_wait_ns,
        snapshot.per_group_group_lock_wait_ns.iter().sum::<u64>()
    );
    assert_eq!(
        snapshot.group_engine_exec_ns,
        snapshot.per_core_group_engine_exec_ns.iter().sum::<u64>()
    );
    assert_eq!(
        snapshot.group_engine_exec_ns,
        snapshot.per_group_group_engine_exec_ns.iter().sum::<u64>()
    );
    assert_eq!(
        snapshot.raft_write_many_batches,
        snapshot
            .per_core_raft_write_many_batches
            .iter()
            .sum::<u64>()
    );
    assert_eq!(
        snapshot.raft_write_many_batches,
        snapshot
            .per_group_raft_write_many_batches
            .iter()
            .sum::<u64>()
    );
    assert_eq!(
        snapshot.raft_write_many_commands,
        snapshot
            .per_core_raft_write_many_commands
            .iter()
            .sum::<u64>()
    );
    assert_eq!(
        snapshot.raft_write_many_commands,
        snapshot
            .per_group_raft_write_many_commands
            .iter()
            .sum::<u64>()
    );
    assert_eq!(
        snapshot.raft_write_many_logical_commands,
        snapshot
            .per_core_raft_write_many_logical_commands
            .iter()
            .sum::<u64>()
    );
    assert_eq!(
        snapshot.raft_write_many_logical_commands,
        snapshot
            .per_group_raft_write_many_logical_commands
            .iter()
            .sum::<u64>()
    );
    assert_eq!(
        snapshot.raft_write_many_responses,
        snapshot
            .per_core_raft_write_many_responses
            .iter()
            .sum::<u64>()
    );
    assert_eq!(
        snapshot.raft_write_many_responses,
        snapshot
            .per_group_raft_write_many_responses
            .iter()
            .sum::<u64>()
    );
    assert_eq!(
        snapshot.raft_write_many_submit_ns,
        snapshot
            .per_core_raft_write_many_submit_ns
            .iter()
            .sum::<u64>()
    );
    assert_eq!(
        snapshot.raft_write_many_submit_ns,
        snapshot
            .per_group_raft_write_many_submit_ns
            .iter()
            .sum::<u64>()
    );
    assert_eq!(
        snapshot.raft_write_many_response_ns,
        snapshot
            .per_core_raft_write_many_response_ns
            .iter()
            .sum::<u64>()
    );
    assert_eq!(
        snapshot.raft_write_many_response_ns,
        snapshot
            .per_group_raft_write_many_response_ns
            .iter()
            .sum::<u64>()
    );
    assert_eq!(
        snapshot.raft_apply_entries,
        snapshot.per_core_raft_apply_entries.iter().sum::<u64>()
    );
    assert_eq!(
        snapshot.raft_apply_entries,
        snapshot.per_group_raft_apply_entries.iter().sum::<u64>()
    );
    assert_eq!(
        snapshot.raft_apply_ns,
        snapshot.per_core_raft_apply_ns.iter().sum::<u64>()
    );
    assert_eq!(
        snapshot.raft_apply_ns,
        snapshot.per_group_raft_apply_ns.iter().sum::<u64>()
    );
    assert_eq!(
        snapshot.mailbox_send_wait_ns,
        snapshot.per_core_mailbox_send_wait_ns.iter().sum::<u64>()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn append_before_stream_setup_uses_stream_state_machine_error() {
    let runtime = runtime(2, 8);
    let stream = BucketStreamId::new("benchcmp", "missing-stream");
    let placement = runtime.locate(&stream);
    let err = runtime
        .append(AppendRequest::new(stream, 1))
        .await
        .expect_err("missing stream rejected");

    match err {
        RuntimeError::GroupEngine {
            core_id,
            raft_group_id,
            error,
            ..
        } => {
            assert_eq!(core_id, placement.core_id);
            assert_eq!(raft_group_id, placement.raft_group_id);
            assert_eq!(error.code(), Some(StreamErrorCode::BucketNotFound));
            let message = error.message();
            assert!(message.contains("BucketNotFound"), "message={message}");
        }
        other => panic!("expected group engine error, got {other:?}"),
    }
    assert_eq!(runtime.metrics().snapshot().accepted_appends, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn group_engine_errors_use_operation_wording_for_non_append_paths() {
    let runtime = runtime(2, 8);
    let stream = BucketStreamId::new("benchcmp", "missing-read-stream");
    let err = runtime
        .read_stream(ReadStreamRequest {
            stream_id: stream,
            offset: 0,
            max_len: 16,
            now_ms: 0,
        })
        .await
        .expect_err("missing stream read rejected");
    let message = err.to_string();
    assert!(message.contains("operation failed"), "message={message}");
    assert!(!message.contains("append failed"), "message={message}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn create_stream_is_routed_and_idempotent_for_matching_metadata() {
    let runtime = runtime(2, 8);
    let stream = BucketStreamId::new("benchcmp", "create-stream");
    let placement = runtime.locate(&stream);

    let created = create_stream(&runtime, &stream).await;
    assert_eq!(created.placement, placement);
    assert_eq!(created.next_offset, 0);
    assert!(!created.closed);
    assert!(!created.already_exists);

    let existing = create_stream(&runtime, &stream).await;
    assert_eq!(existing.placement, placement);
    assert_eq!(existing.next_offset, 0);
    assert!(!existing.closed);
    assert!(existing.already_exists);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn head_stream_reflects_append_and_closed_state_on_owner_group() {
    let runtime = runtime(2, 8);
    let stream = BucketStreamId::new("benchcmp", "head-stream");
    let placement = runtime.locate(&stream);
    runtime
        .create_stream(CreateStreamRequest::new(stream.clone(), "text/plain"))
        .await
        .expect("create stream");

    let mut append = AppendRequest::new(stream.clone(), 3);
    append.content_type = "text/plain".to_owned();
    append.close_after = true;
    let response = runtime.append(append).await.expect("append");
    assert_eq!(response.start_offset, 0);
    assert_eq!(response.next_offset, 3);

    let head = runtime
        .head_stream(HeadStreamRequest {
            stream_id: stream,
            now_ms: 0,
        })
        .await
        .expect("head stream");
    assert_eq!(head.placement, placement);
    assert_eq!(head.content_type, "text/plain");
    assert_eq!(head.tail_offset, 3);
    assert!(head.closed);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn read_stream_returns_payload_slice_from_owner_group() {
    let runtime = runtime(2, 8);
    let stream = BucketStreamId::new("benchcmp", "read-stream");
    let placement = runtime.locate(&stream);
    create_stream(&runtime, &stream).await;
    runtime
        .append(AppendRequest::from_bytes(
            stream.clone(),
            b"abcdefg".to_vec(),
        ))
        .await
        .expect("append");

    let read = runtime
        .read_stream(ReadStreamRequest {
            stream_id: stream.clone(),
            offset: 2,
            max_len: 3,
            now_ms: 0,
        })
        .await
        .expect("read stream");
    assert_eq!(read.placement, placement);
    assert_eq!(read.offset, 2);
    assert_eq!(read.next_offset, 5);
    assert_eq!(read.payload, b"cde");
    assert!(!read.up_to_date);
    assert!(!read.closed);

    let tail = runtime
        .read_stream(ReadStreamRequest {
            stream_id: stream,
            offset: 7,
            max_len: 16,
            now_ms: 0,
        })
        .await
        .expect("tail read");
    assert_eq!(tail.next_offset, 7);
    assert!(tail.payload.is_empty());
    assert!(tail.up_to_date);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn flush_cold_publishes_chunk_metadata_on_owner_group() {
    let runtime = runtime(2, 8);
    let stream = BucketStreamId::new("benchcmp", "cold-runtime");
    let placement = runtime.locate(&stream);
    create_stream(&runtime, &stream).await;
    runtime
        .append(AppendRequest::from_bytes(
            stream.clone(),
            b"abcdef".to_vec(),
        ))
        .await
        .expect("append");

    let flushed = runtime
        .flush_cold(FlushColdRequest {
            stream_id: stream.clone(),
            chunk: ColdChunkRef {
                start_offset: 0,
                end_offset: 4,
                s3_path: "s3://bucket/cold-runtime/000000".to_owned(),
                object_size: 4,
            },
        })
        .await
        .expect("flush cold");
    assert_eq!(flushed.placement, placement);
    assert_eq!(flushed.hot_start_offset, 4);

    let hot = runtime
        .read_stream(ReadStreamRequest {
            stream_id: stream.clone(),
            offset: 4,
            max_len: 16,
            now_ms: 0,
        })
        .await
        .expect("hot read");
    assert_eq!(hot.payload, b"ef");

    let err = runtime
        .read_stream(ReadStreamRequest {
            stream_id: stream,
            offset: 0,
            max_len: 16,
            now_ms: 0,
        })
        .await
        .expect_err("cold read needs store");
    match err {
        RuntimeError::GroupEngine { error, .. } => {
            assert_eq!(error.code(), Some(StreamErrorCode::InvalidColdFlush));
            assert_eq!(error.next_offset(), Some(6));
        }
        other => panic!("expected cold read error, got {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn flush_cold_once_uploads_outside_group_and_reads_back() {
    let cold_store = Arc::new(ColdStore::memory().expect("memory cold store"));
    let runtime = ShardRuntime::spawn_with_engine_factory_and_cold_store(
        RuntimeConfig::new(2, 8),
        InMemoryGroupEngineFactory::with_cold_store(Some(cold_store.clone())),
        Some(cold_store),
    )
    .expect("spawn runtime");
    let stream = BucketStreamId::new("benchcmp", "cold-once");
    create_stream(&runtime, &stream).await;
    runtime
        .append(AppendRequest::from_bytes(
            stream.clone(),
            b"abcdef".to_vec(),
        ))
        .await
        .expect("append");

    let flushed = runtime
        .flush_cold_once(PlanColdFlushRequest {
            stream_id: stream.clone(),
            min_hot_bytes: 4,
            max_flush_bytes: 4,
        })
        .await
        .expect("flush once")
        .expect("candidate flushed");
    assert_eq!(flushed.hot_start_offset, 4);
    let metrics = runtime.metrics().snapshot();
    assert_eq!(metrics.cold_flush_uploads, 1);
    assert_eq!(metrics.cold_flush_upload_bytes, 4);
    assert_eq!(metrics.cold_flush_publishes, 1);
    assert_eq!(metrics.cold_flush_publish_bytes, 4);
    assert_eq!(metrics.cold_orphan_cleanup_attempts, 0);

    let read = runtime
        .read_stream(ReadStreamRequest {
            stream_id: stream,
            offset: 0,
            max_len: 6,
            now_ms: 0,
        })
        .await
        .expect("read cold and hot");
    assert_eq!(read.payload, b"abcdef");
    assert_eq!(read.next_offset, 6);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn flush_cold_group_batch_once_publishes_multiple_chunks() {
    let cold_store = Arc::new(ColdStore::memory().expect("memory cold store"));
    let runtime = ShardRuntime::spawn_with_engine_factory_and_cold_store(
        RuntimeConfig::new(2, 8),
        InMemoryGroupEngineFactory::with_cold_store(Some(cold_store.clone())),
        Some(cold_store),
    )
    .expect("spawn runtime");
    let stream = BucketStreamId::new("benchcmp", "cold-batch");
    let placement = runtime.locate(&stream);
    create_stream(&runtime, &stream).await;
    runtime
        .append(AppendRequest::from_bytes(stream.clone(), b"abcd".to_vec()))
        .await
        .expect("append");

    let flushed = runtime
        .flush_cold_group_batch_once(
            placement.raft_group_id,
            PlanGroupColdFlushRequest {
                min_hot_bytes: 1,
                max_flush_bytes: 1,
            },
            4,
        )
        .await
        .expect("flush batch");
    assert_eq!(flushed.len(), 4);
    assert!(
        flushed
            .iter()
            .all(|response| response.placement == placement)
    );
    assert_eq!(
        flushed
            .iter()
            .map(|response| response.hot_start_offset)
            .collect::<Vec<_>>(),
        vec![1, 2, 3, 4]
    );

    let metrics = runtime.metrics().snapshot();
    assert_eq!(metrics.cold_flush_uploads, 4);
    assert_eq!(metrics.cold_flush_upload_bytes, 4);
    assert_eq!(metrics.cold_flush_publishes, 4);
    assert_eq!(metrics.cold_flush_publish_bytes, 4);
    assert_eq!(metrics.cold_hot_bytes, 0);

    let snapshot = runtime
        .snapshot_group(placement.raft_group_id)
        .await
        .expect("snapshot group");
    let entry = snapshot
        .stream_snapshot
        .streams
        .iter()
        .find(|entry| entry.metadata.stream_id == stream)
        .expect("stream snapshot");
    assert_eq!(entry.cold_frontier_offset, 4);
    assert!(entry.cold_chunks.is_empty());
    assert!(entry.payload.is_empty());

    let read = runtime
        .read_stream(ReadStreamRequest {
            stream_id: stream,
            offset: 0,
            max_len: 4,
            now_ms: 0,
        })
        .await
        .expect("read cold chunks");
    assert_eq!(read.payload, b"abcd");
    assert_eq!(read.next_offset, 4);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cold_gc_worker_physically_reclaims_deleted_stream_chunks() {
    let cold_store = Arc::new(ColdStore::memory().expect("memory cold store"));
    let runtime = ShardRuntime::spawn_with_engine_factory_and_cold_store(
        RuntimeConfig::new(2, 8),
        InMemoryGroupEngineFactory::with_cold_store(Some(cold_store.clone())),
        Some(cold_store.clone()),
    )
    .expect("spawn runtime");
    let stream = BucketStreamId::new("benchcmp", "cold-gc");
    create_stream(&runtime, &stream).await;
    runtime
        .append(AppendRequest::from_bytes(stream.clone(), b"abcd".to_vec()))
        .await
        .expect("append");
    let chunk = ColdChunkRef {
        start_offset: 0,
        end_offset: 4,
        s3_path: "benchcmp/cold-gc/chunks/000000.bin".to_owned(),
        object_size: 4,
    };
    cold_store
        .write_chunk(&chunk.s3_path, b"abcd")
        .await
        .expect("write cold chunk");
    runtime
        .flush_cold(FlushColdRequest {
            stream_id: stream.clone(),
            chunk: chunk.clone(),
        })
        .await
        .expect("flush cold");

    assert!(
        cold_store
            .read_chunk_range(&chunk, chunk.start_offset, 4)
            .await
            .is_ok(),
        "chunk must exist before GC"
    );

    runtime
        .delete_stream(DeleteStreamRequest {
            stream_id: stream.clone(),
        })
        .await
        .expect("delete stream");

    // A GC tick reclaims exactly the one queued prefix and drains the queue.
    let reclaimed = runtime
        .run_cold_gc_all_groups_once(256)
        .await
        .expect("run cold gc");
    assert_eq!(reclaimed, 1);
    assert_eq!(runtime.metrics().snapshot().cold_gc_reclaimed, 1);

    // The physical object is gone, and a second tick finds nothing to do.
    assert!(
        cold_store
            .read_chunk_range(&chunk, chunk.start_offset, 4)
            .await
            .is_err(),
        "chunk must be physically reclaimed after GC"
    );
    assert_eq!(
        runtime
            .run_cold_gc_all_groups_once(256)
            .await
            .expect("idempotent gc tick"),
        0
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stale_cold_flush_batch_after_delete_recreate_is_classified_for_cleanup() {
    let cold_store = Arc::new(ColdStore::memory().expect("memory cold store"));
    let runtime = ShardRuntime::spawn_with_engine_factory_and_cold_store(
        RuntimeConfig::new(2, 8),
        InMemoryGroupEngineFactory::with_cold_store(Some(cold_store.clone())),
        Some(cold_store),
    )
    .expect("spawn runtime");
    let stream = BucketStreamId::new("benchcmp", "stale-cold-runtime");
    let placement = runtime.locate(&stream);
    create_stream(&runtime, &stream).await;
    runtime
        .append(AppendRequest::from_bytes(
            stream.clone(),
            b"abcdefghijklmnopqr".to_vec(),
        ))
        .await
        .expect("append old stream");
    let candidates = runtime
        .plan_next_cold_flush_batch(
            placement.raft_group_id,
            PlanGroupColdFlushRequest {
                min_hot_bytes: 18,
                max_flush_bytes: 18,
            },
            1,
        )
        .await
        .expect("plan candidate");
    assert_eq!(candidates.len(), 1);

    runtime
        .delete_stream(DeleteStreamRequest {
            stream_id: stream.clone(),
        })
        .await
        .expect("delete old stream");
    create_stream(&runtime, &stream).await;
    runtime
        .append(AppendRequest::from_bytes(
            stream.clone(),
            b"abcdefghijklmnopq".to_vec(),
        ))
        .await
        .expect("append recreated stream");

    let flushed = runtime
        .flush_cold_candidates_batch(candidates)
        .await
        .expect("stale candidate should be skipped");
    assert!(flushed.is_empty());
    let metrics = runtime.metrics().snapshot();
    assert_eq!(metrics.cold_flush_uploads, 1);
    assert_eq!(metrics.cold_flush_publishes, 0);
    assert_eq!(metrics.cold_orphan_cleanup_attempts, 0);
    assert_eq!(metrics.cold_orphan_cleanup_errors, 0);

    let read = runtime
        .read_stream(ReadStreamRequest {
            stream_id: stream,
            offset: 0,
            max_len: 32,
            now_ms: 0,
        })
        .await
        .expect("read recreated stream");
    assert_eq!(read.payload, b"abcdefghijklmnopq");
    assert_eq!(read.next_offset, 17);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cold_write_admission_rejects_new_bytes_until_flush_catches_up() {
    let cold_store = Arc::new(ColdStore::memory().expect("memory cold store"));
    let runtime = ShardRuntime::spawn_with_engine_factory_and_cold_store(
        RuntimeConfig::new(2, 8).with_cold_max_hot_bytes_per_group(Some(4)),
        InMemoryGroupEngineFactory::with_cold_store(Some(cold_store.clone())),
        Some(cold_store),
    )
    .expect("spawn runtime");
    let stream = BucketStreamId::new("benchcmp", "cold-admission");
    create_stream(&runtime, &stream).await;
    runtime
        .append(AppendRequest::from_bytes(stream.clone(), b"abcd".to_vec()))
        .await
        .expect("append below limit");

    let err = runtime
        .append(AppendRequest::from_bytes(stream.clone(), b"e".to_vec()))
        .await
        .expect_err("append should be backpressured");
    match err {
        RuntimeError::GroupEngine {
            error:
                GroupEngineError::Infra(GroupInfraError::ColdBackpressure {
                    stream_id,
                    before_group_hot_bytes,
                    after_group_hot_bytes,
                    limit,
                    ..
                }),
            ..
        } => {
            assert_eq!(stream_id, stream);
            assert_eq!(before_group_hot_bytes, 4);
            assert_eq!(after_group_hot_bytes, 5);
            assert_eq!(limit, 4);
        }
        other => panic!("expected cold backpressure, got {other:?}"),
    }
    let metrics = runtime.metrics().snapshot();
    let group_index = usize::try_from(runtime.locate(&stream).raft_group_id.0).unwrap();
    assert_eq!(metrics.accepted_appends, 1);
    assert_eq!(metrics.cold_hot_bytes, 4);
    assert_eq!(metrics.per_group_cold_hot_bytes[group_index], 4);
    assert_eq!(metrics.cold_hot_group_bytes_max, 4);
    assert_eq!(metrics.cold_hot_stream_bytes_max, 4);
    assert_eq!(metrics.cold_backpressure_events, 1);
    assert_eq!(metrics.per_group_cold_backpressure_events[group_index], 1);
    assert_eq!(metrics.cold_backpressure_bytes, 1);

    runtime
        .flush_cold_once(PlanColdFlushRequest {
            stream_id: stream.clone(),
            min_hot_bytes: 4,
            max_flush_bytes: 4,
        })
        .await
        .expect("flush once")
        .expect("candidate flushed");
    assert_eq!(runtime.metrics().snapshot().cold_hot_bytes, 0);

    runtime
        .append(AppendRequest::from_bytes(stream.clone(), b"e".to_vec()))
        .await
        .expect("append after flush");
    let read = runtime
        .read_stream(ReadStreamRequest {
            stream_id: stream,
            offset: 0,
            max_len: 5,
            now_ms: 0,
        })
        .await
        .expect("read cold and hot");
    assert_eq!(read.payload, b"abcde");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cold_write_admission_allows_deduplicated_append_retry_at_hot_limit() {
    let cold_store = Arc::new(ColdStore::memory().expect("memory cold store"));
    let runtime = ShardRuntime::spawn_with_engine_factory_and_cold_store(
        RuntimeConfig::new(2, 8).with_cold_max_hot_bytes_per_group(Some(4)),
        InMemoryGroupEngineFactory::with_cold_store(Some(cold_store.clone())),
        Some(cold_store),
    )
    .expect("spawn runtime");
    let stream = BucketStreamId::new("benchcmp", "cold-admission-dedup-append");
    create_stream(&runtime, &stream).await;

    let mut first = AppendRequest::from_bytes(stream.clone(), b"abcd".to_vec());
    first.producer = Some(producer("writer", 0, 0));
    runtime.append(first).await.expect("append at hot limit");

    let mut retry = AppendRequest::from_bytes(stream.clone(), b"ignored".to_vec());
    retry.producer = Some(producer("writer", 0, 0));
    let retry = runtime
        .append(retry)
        .await
        .expect("deduplicated retry should bypass cold admission");
    assert!(retry.deduplicated);
    assert_eq!(retry.start_offset, 0);
    assert_eq!(retry.next_offset, 4);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cold_write_admission_allows_deduplicated_append_batch_retry_at_hot_limit() {
    let cold_store = Arc::new(ColdStore::memory().expect("memory cold store"));
    let runtime = ShardRuntime::spawn_with_engine_factory_and_cold_store(
        RuntimeConfig::new(2, 8).with_cold_max_hot_bytes_per_group(Some(4)),
        InMemoryGroupEngineFactory::with_cold_store(Some(cold_store.clone())),
        Some(cold_store),
    )
    .expect("spawn runtime");
    let stream = BucketStreamId::new("benchcmp", "cold-admission-dedup-batch");
    create_stream(&runtime, &stream).await;

    let mut first = AppendBatchRequest::new(
        stream.clone(),
        vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec(), b"d".to_vec()],
    );
    first.producer = Some(producer("writer", 0, 0));
    runtime
        .append_batch(first)
        .await
        .expect("batch at hot limit");

    let mut retry = AppendBatchRequest::new(stream.clone(), vec![b"ignored".to_vec()]);
    retry.producer = Some(producer("writer", 0, 0));
    let retry = runtime
        .append_batch(retry)
        .await
        .expect("deduplicated batch retry should bypass cold admission");
    assert!(
        retry
            .items
            .iter()
            .all(|item| item.as_ref().expect("deduplicated item").deduplicated)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cold_write_admission_allows_existing_create_retry_at_hot_limit() {
    let cold_store = Arc::new(ColdStore::memory().expect("memory cold store"));
    let runtime = ShardRuntime::spawn_with_engine_factory_and_cold_store(
        RuntimeConfig::new(2, 8).with_cold_max_hot_bytes_per_group(Some(4)),
        InMemoryGroupEngineFactory::with_cold_store(Some(cold_store.clone())),
        Some(cold_store),
    )
    .expect("spawn runtime");
    let stream = BucketStreamId::new("benchcmp", "cold-admission-existing-create");
    let mut request = CreateStreamRequest::new(stream.clone(), DEFAULT_CONTENT_TYPE);
    request.initial_payload = Bytes::from_static(b"abcd");

    runtime
        .create_stream(request.clone())
        .await
        .expect("create at hot limit");
    let retry = runtime
        .create_stream(request)
        .await
        .expect("existing create should bypass cold admission");
    assert!(retry.already_exists);
    assert_eq!(retry.next_offset, 4);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn raft_uncommitted_admission_disabled_by_default_lets_writes_through() {
    // Disabled admission must not interfere with the existing accept path
    // (acceptance criterion 6: existing cold behaviour unchanged when the
    // raft admission is disabled).
    let runtime = ShardRuntime::spawn_with_engine_factory(
        RuntimeConfig::new(2, 4).with_raft_max_uncommitted_bytes_per_group(None),
        InMemoryGroupEngineFactory::default(),
    )
    .expect("spawn runtime");
    let stream = BucketStreamId::new("benchcmp", "raft-uncommitted-disabled");
    create_stream(&runtime, &stream).await;
    for _ in 0..4 {
        runtime
            .append(AppendRequest::from_bytes(
                stream.clone(),
                b"payload".to_vec(),
            ))
            .await
            .expect("append succeeds with admission disabled");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn raft_uncommitted_admission_rejects_when_incoming_would_exceed_limit() {
    // With a 4-byte cap and a 5-byte append on an empty stream, the
    // CoreWorker-level admission rejects the request before reaching the
    // actor since `current (0) + incoming (5) > limit (4)`.
    let runtime = ShardRuntime::spawn_with_engine_factory(
        RuntimeConfig::new(2, 4).with_raft_max_uncommitted_bytes_per_group(Some(4)),
        InMemoryGroupEngineFactory::default(),
    )
    .expect("spawn runtime");
    let stream = BucketStreamId::new("benchcmp", "raft-uncommitted-trip");
    create_stream(&runtime, &stream).await;

    let err = runtime
        .append(AppendRequest::from_bytes(stream.clone(), vec![b'x'; 5]))
        .await
        .expect_err("oversized append should trip raft uncommitted admission");
    match err {
        RuntimeError::GroupEngine {
            error:
                GroupEngineError::Infra(GroupInfraError::RaftUncommittedBackpressure {
                    current,
                    incoming,
                    limit,
                    ..
                }),
            ..
        } => {
            assert_eq!(current, 0);
            assert_eq!(incoming, 5);
            assert_eq!(limit, 4);
        }
        other => panic!("expected raft uncommitted backpressure, got {other:?}"),
    }

    // A within-budget append still succeeds.
    runtime
        .append(AppendRequest::from_bytes(stream.clone(), b"abcd".to_vec()))
        .await
        .expect("append at the limit succeeds");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cold_write_admission_rejects_append_batch_without_partial_mutation() {
    let cold_store = Arc::new(ColdStore::memory().expect("memory cold store"));
    let runtime = ShardRuntime::spawn_with_engine_factory_and_cold_store(
        RuntimeConfig::new(2, 8).with_cold_max_hot_bytes_per_group(Some(4)),
        InMemoryGroupEngineFactory::with_cold_store(Some(cold_store.clone())),
        Some(cold_store),
    )
    .expect("spawn runtime");
    let stream = BucketStreamId::new("benchcmp", "cold-admission-batch");
    create_stream(&runtime, &stream).await;
    runtime
        .append(AppendRequest::from_bytes(stream.clone(), b"abc".to_vec()))
        .await
        .expect("append below limit");

    let err = runtime
        .append_batch(AppendBatchRequest::new(
            stream.clone(),
            vec![b"d".to_vec(), b"e".to_vec()],
        ))
        .await
        .expect_err("batch should be backpressured");
    match err {
        RuntimeError::GroupEngine {
            error:
                GroupEngineError::Infra(GroupInfraError::ColdBackpressure {
                    stream_id,
                    before_group_hot_bytes,
                    after_group_hot_bytes,
                    limit,
                    ..
                }),
            ..
        } => {
            assert_eq!(stream_id, stream);
            assert_eq!(before_group_hot_bytes, 3);
            assert_eq!(after_group_hot_bytes, 5);
            assert_eq!(limit, 4);
        }
        other => panic!("expected cold backpressure, got {other:?}"),
    }
    let read = runtime
        .read_stream(ReadStreamRequest {
            stream_id: stream.clone(),
            offset: 0,
            max_len: 8,
            now_ms: 0,
        })
        .await
        .expect("read");
    assert_eq!(read.payload, b"abc");
    let metrics = runtime.metrics().snapshot();
    assert_eq!(metrics.accepted_appends, 1);
    assert_eq!(metrics.cold_backpressure_events, 1);
    assert_eq!(metrics.cold_backpressure_bytes, 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cold_write_admission_does_not_preempt_non_local_write_engine() {
    let factory = RecordingFactory::without_local_writes().with_cold_hot_bytes(8);
    let runtime = ShardRuntime::spawn_with_engine_factory(
        RuntimeConfig::new(2, 8).with_cold_max_hot_bytes_per_group(Some(4)),
        factory,
    )
    .expect("spawn runtime");
    let stream = BucketStreamId::new("benchcmp", "cold-admission-non-local");

    runtime
        .create_stream(CreateStreamRequest::new(
            stream.clone(),
            DEFAULT_CONTENT_TYPE,
        ))
        .await
        .expect("create reaches non-local engine");
    assert_eq!(runtime.metrics().snapshot().cold_hot_bytes, 8);

    runtime
        .append(AppendRequest::from_bytes(stream, b"x".to_vec()))
        .await
        .expect("append reaches non-local engine despite local cold backlog");

    let metrics = runtime.metrics().snapshot();
    assert_eq!(metrics.accepted_appends, 1);
    assert_eq!(metrics.cold_backpressure_events, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn flush_cold_group_once_selects_stream_inside_owner_group() {
    let cold_store = Arc::new(ColdStore::memory().expect("memory cold store"));
    let runtime = ShardRuntime::spawn_with_engine_factory_and_cold_store(
        RuntimeConfig::new(2, 8),
        InMemoryGroupEngineFactory::with_cold_store(Some(cold_store.clone())),
        Some(cold_store),
    )
    .expect("spawn runtime");
    let group_id = RaftGroupId(3);
    let stream = stream_on_group(&runtime, group_id, "cold-group");
    create_stream(&runtime, &stream).await;
    runtime
        .append(AppendRequest::from_bytes(
            stream.clone(),
            b"abcdef".to_vec(),
        ))
        .await
        .expect("append");

    let flushed = runtime
        .flush_cold_group_once(
            group_id,
            PlanGroupColdFlushRequest {
                min_hot_bytes: 4,
                max_flush_bytes: 4,
            },
        )
        .await
        .expect("flush group")
        .expect("candidate flushed");
    assert_eq!(flushed.hot_start_offset, 4);

    let read = runtime
        .read_stream(ReadStreamRequest {
            stream_id: stream,
            offset: 0,
            max_len: 6,
            now_ms: 0,
        })
        .await
        .expect("read cold and hot");
    assert_eq!(read.payload, b"abcdef");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn flush_cold_all_groups_once_bounded_flushes_multiple_groups() {
    let cold_store = Arc::new(ColdStore::memory().expect("memory cold store"));
    let runtime = ShardRuntime::spawn_with_engine_factory_and_cold_store(
        RuntimeConfig::new(2, 8),
        InMemoryGroupEngineFactory::with_cold_store(Some(cold_store.clone())),
        Some(cold_store),
    )
    .expect("spawn runtime");
    let first = stream_on_group(&runtime, RaftGroupId(1), "cold-bounded-a");
    let second = stream_on_group(&runtime, RaftGroupId(6), "cold-bounded-b");
    for stream in [&first, &second] {
        create_stream(&runtime, stream).await;
        runtime
            .append(AppendRequest::from_bytes(
                stream.clone(),
                b"abcdef".to_vec(),
            ))
            .await
            .expect("append");
    }

    let flushed = runtime
        .flush_cold_all_groups_once_bounded(
            PlanGroupColdFlushRequest {
                min_hot_bytes: 4,
                max_flush_bytes: 4,
            },
            2,
        )
        .await
        .expect("flush all bounded");
    assert_eq!(flushed, 2);
    let metrics = runtime.metrics().snapshot();
    assert_eq!(metrics.cold_flush_uploads, 2);
    assert_eq!(metrics.cold_flush_upload_bytes, 8);
    assert_eq!(metrics.cold_flush_publishes, 2);
    assert_eq!(metrics.cold_flush_publish_bytes, 8);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn repeated_cold_flush_keeps_hot_bytes_bounded_while_writes_continue() {
    let cold_store = Arc::new(ColdStore::memory().expect("memory cold store"));
    let runtime = ShardRuntime::spawn_with_engine_factory_and_cold_store(
        RuntimeConfig::new(2, 8).with_cold_max_hot_bytes_per_group(Some(16)),
        InMemoryGroupEngineFactory::with_cold_store(Some(cold_store.clone())),
        Some(cold_store),
    )
    .expect("spawn runtime");
    let streams = [
        stream_on_group(&runtime, RaftGroupId(0), "cold-steady-a"),
        stream_on_group(&runtime, RaftGroupId(3), "cold-steady-b"),
        stream_on_group(&runtime, RaftGroupId(5), "cold-steady-c"),
        stream_on_group(&runtime, RaftGroupId(7), "cold-steady-d"),
    ];
    for stream in &streams {
        create_stream(&runtime, stream).await;
    }

    let mut expected = Vec::new();
    for round in 0..8u8 {
        let payload = vec![b'a' + round; 4];
        expected.extend_from_slice(&payload);
        for stream in &streams {
            runtime
                .append(AppendRequest::from_bytes(stream.clone(), payload.clone()))
                .await
                .expect("append while cold worker keeps up");
        }

        let metrics_before_flush = runtime.metrics().snapshot();
        assert!(
            metrics_before_flush.cold_hot_bytes <= 64,
            "hot bytes should stay within one unflushed batch per group before flush: {}",
            metrics_before_flush.cold_hot_bytes
        );

        let flushed = runtime
            .flush_cold_all_groups_once_bounded(
                PlanGroupColdFlushRequest {
                    min_hot_bytes: 4,
                    max_flush_bytes: 4,
                },
                streams.len(),
            )
            .await
            .expect("flush all bounded");
        assert_eq!(flushed, streams.len());
        let metrics_after_flush = runtime.metrics().snapshot();
        assert_eq!(
            metrics_after_flush.cold_hot_bytes, 0,
            "all newly appended bytes should be offloaded after round {round}"
        );
        assert_eq!(
            metrics_after_flush.cold_flush_uploads,
            u64::try_from((usize::from(round) + 1) * streams.len()).expect("count fits u64")
        );
        assert_eq!(metrics_after_flush.cold_orphan_cleanup_attempts, 0);
        assert_eq!(metrics_after_flush.cold_backpressure_events, 0);
    }

    for stream in streams {
        let read = runtime
            .read_stream(ReadStreamRequest {
                stream_id: stream,
                offset: 0,
                max_len: expected.len(),
                now_ms: 0,
            })
            .await
            .expect("read cold-backed stream");
        assert_eq!(read.payload, expected);
        assert_eq!(read.next_offset, u64::try_from(expected.len()).unwrap());
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wait_read_stream_completes_after_owner_append() {
    let runtime = runtime(2, 8);
    let stream = BucketStreamId::new("benchcmp", "wait-read");
    create_stream(&runtime, &stream).await;

    let wait = {
        let runtime = runtime.clone();
        let stream = stream.clone();
        tokio::spawn(async move {
            runtime
                .wait_read_stream(ReadStreamRequest {
                    stream_id: stream,
                    offset: 0,
                    max_len: 16,
                    now_ms: 0,
                })
                .await
                .expect("wait read")
        })
    };
    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    runtime
        .append(AppendRequest::from_bytes(stream.clone(), b"hello".to_vec()))
        .await
        .expect("append");

    let read = tokio::time::timeout(std::time::Duration::from_secs(1), wait)
        .await
        .expect("wait read timeout")
        .expect("wait task");
    assert_eq!(read.payload, b"hello");
    assert_eq!(read.next_offset, 5);
    assert!(read.up_to_date);
    assert!(!read.closed);
    assert_eq!(runtime.metrics().snapshot().live_read_waiters, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wait_read_stream_completes_on_close_at_tail() {
    let runtime = runtime(2, 8);
    let stream = BucketStreamId::new("benchcmp", "wait-close");
    create_stream(&runtime, &stream).await;

    let wait = {
        let runtime = runtime.clone();
        let stream = stream.clone();
        tokio::spawn(async move {
            runtime
                .wait_read_stream(ReadStreamRequest {
                    stream_id: stream,
                    offset: 0,
                    max_len: 16,
                    now_ms: 0,
                })
                .await
                .expect("wait read")
        })
    };
    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    runtime
        .close_stream(CloseStreamRequest {
            stream_id: stream,
            stream_seq: None,
            producer: None,
            now_ms: 0,
        })
        .await
        .expect("close stream");

    let read = tokio::time::timeout(std::time::Duration::from_secs(1), wait)
        .await
        .expect("wait read timeout")
        .expect("wait task");
    assert!(read.payload.is_empty());
    assert_eq!(read.next_offset, 0);
    assert!(read.up_to_date);
    assert!(read.closed);
    assert_eq!(runtime.metrics().snapshot().live_read_waiters, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn canceled_wait_read_stream_removes_owner_waiter() {
    let runtime = runtime(2, 8);
    let stream = BucketStreamId::new("benchcmp", "wait-cancel");
    create_stream(&runtime, &stream).await;

    let wait = {
        let runtime = runtime.clone();
        let stream = stream.clone();
        tokio::spawn(async move {
            runtime
                .wait_read_stream(ReadStreamRequest {
                    stream_id: stream,
                    offset: 0,
                    max_len: 16,
                    now_ms: 0,
                })
                .await
        })
    };
    wait_for_live_waiters(&runtime, 1).await;
    wait.abort();
    let _ = wait.await;
    wait_for_live_waiters(&runtime, 0).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn live_read_waiter_limit_rejects_excess_waiters_on_owner_core() {
    let runtime =
        ShardRuntime::spawn(RuntimeConfig::new(1, 1).with_live_read_max_waiters_per_core(Some(1)))
            .expect("spawn runtime");
    let stream = BucketStreamId::new("benchcmp", "wait-limit");
    create_stream(&runtime, &stream).await;

    let first = {
        let runtime = runtime.clone();
        let stream = stream.clone();
        tokio::spawn(async move {
            runtime
                .wait_read_stream(ReadStreamRequest {
                    stream_id: stream,
                    offset: 0,
                    max_len: 16,
                    now_ms: 0,
                })
                .await
        })
    };
    wait_for_live_waiters(&runtime, 1).await;

    let err = runtime
        .wait_read_stream(ReadStreamRequest {
            stream_id: stream.clone(),
            offset: 0,
            max_len: 16,
            now_ms: 0,
        })
        .await
        .expect_err("second waiter should hit owner-core limit");
    assert_eq!(
        err,
        RuntimeError::LiveReadBackpressure {
            core_id: CoreId(0),
            current_waiters: 1,
            limit: 1,
        }
    );
    let snapshot = runtime.metrics().snapshot();
    assert_eq!(snapshot.live_read_waiters, 1);
    assert_eq!(snapshot.live_read_backpressure_events, 1);
    assert_eq!(snapshot.per_core_live_read_backpressure_events, vec![1]);

    first.abort();
    let _ = first.await;
    wait_for_live_waiters(&runtime, 0).await;
}

#[test]
fn cancel_read_watcher_removes_group_local_waiter() {
    let stream = BucketStreamId::new("benchcmp", "watcher-cancel-local");
    let mut read_watchers = ReadWatchers::new();
    let (first_tx, _first_rx) = oneshot::channel();
    let (second_tx, _second_rx) = oneshot::channel();
    read_watchers.insert(
        stream.clone(),
        vec![
            ReadWatcher {
                waiter_id: 1,
                request: ReadStreamRequest {
                    stream_id: stream.clone(),
                    offset: 0,
                    max_len: 16,
                    now_ms: 0,
                },
                response_tx: first_tx,
            },
            ReadWatcher {
                waiter_id: 2,
                request: ReadStreamRequest {
                    stream_id: stream.clone(),
                    offset: 0,
                    max_len: 16,
                    now_ms: 0,
                },
                response_tx: second_tx,
            },
        ],
    );

    let metrics = Arc::new(RuntimeMetricsInner::new(1, 1));
    metrics.record_read_watchers_added(CoreId(0), 2);
    CoreWorker::cancel_read_watcher(
        &mut read_watchers,
        metrics.clone(),
        CoreId(0),
        stream.clone(),
        1,
    );

    let watcher_ids = read_watchers
        .get(&stream)
        .expect("one watcher remains")
        .iter()
        .map(|watcher| watcher.waiter_id)
        .collect::<Vec<_>>();
    assert_eq!(watcher_ids, vec![2]);
    assert_eq!(metrics.per_core_live_read_waiters[0].load_relaxed(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn notify_read_watchers_shares_identical_reads_across_watchers() {
    let factory = BlockingReadFactory::default();
    let runtime = ShardRuntime::spawn_with_engine_factory(
        RuntimeConfig {
            core_count: 1,
            raft_group_count: 1,
            mailbox_capacity: 8,
            threading: RuntimeThreading::HostedTokio,
            cold_max_hot_bytes_per_group: None,
            raft_max_uncommitted_bytes_per_group: None,
            live_read_max_waiters_per_core: Some(65_536),
        },
        factory.clone(),
    )
    .expect("spawn runtime");
    let stream = BucketStreamId::new("benchcmp", "watcher-shared-read");
    let placement = runtime.locate(&stream);
    let request = ReadStreamRequest {
        stream_id: stream.clone(),
        offset: 0,
        max_len: 16,
        now_ms: 0,
    };
    let mut read_watchers = ReadWatchers::new();
    let (first_tx, _first_rx) = oneshot::channel();
    let (second_tx, _second_rx) = oneshot::channel();
    read_watchers.insert(
        stream.clone(),
        vec![
            ReadWatcher {
                waiter_id: 1,
                request: request.clone(),
                response_tx: first_tx,
            },
            ReadWatcher {
                waiter_id: 2,
                request,
                response_tx: second_tx,
            },
        ],
    );

    let metrics = Arc::new(RuntimeMetricsInner::new(1, 1));
    let mut engine = factory
        .create(
            placement,
            GroupEngineMetrics {
                inner: metrics.clone(),
            },
        )
        .await
        .expect("create engine");
    let notify = {
        let stream = stream.clone();
        tokio::spawn(async move {
            CoreWorker::notify_read_watchers(
                &mut engine,
                metrics,
                Arc::new(Semaphore::new(8)),
                &mut read_watchers,
                &stream,
                placement,
            )
            .await;
            read_watchers
        })
    };
    tokio::time::timeout(
        std::time::Duration::from_secs(1),
        factory.entered.notified(),
    )
    .await
    .expect("notify issued one grouped read");
    factory.release.notify_one();
    let read_watchers = tokio::time::timeout(std::time::Duration::from_secs(1), notify)
        .await
        .expect("notify should finish after one read")
        .expect("notify task");

    let watcher_ids = read_watchers
        .get(&stream)
        .expect("pending watchers reinserted")
        .iter()
        .map(|watcher| watcher.waiter_id)
        .collect::<Vec<_>>();
    assert_eq!(watcher_ids, vec![1, 2]);
    assert_eq!(factory.read_count.load(Ordering::Relaxed), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn close_stream_allows_close_only_and_rejects_later_appends() {
    let runtime = runtime(2, 8);
    let stream = BucketStreamId::new("benchcmp", "close-only");
    let placement = runtime.locate(&stream);
    create_stream(&runtime, &stream).await;

    let closed = runtime
        .close_stream(CloseStreamRequest {
            stream_id: stream.clone(),
            stream_seq: None,
            producer: None,
            now_ms: 0,
        })
        .await
        .expect("close stream");
    assert_eq!(closed.placement, placement);
    assert_eq!(closed.next_offset, 0);

    let err = runtime
        .append(AppendRequest::new(stream.clone(), 1))
        .await
        .expect_err("append after close rejected");
    match err {
        RuntimeError::GroupEngine { error, .. } => {
            let message = error.message();
            assert!(message.contains("StreamClosed"), "message={message}");
        }
        other => panic!("expected group engine error, got {other:?}"),
    }

    let head = runtime
        .head_stream(HeadStreamRequest {
            stream_id: stream,
            now_ms: 0,
        })
        .await
        .expect("head stream");
    assert_eq!(head.tail_offset, 0);
    assert!(head.closed);
    assert_eq!(runtime.metrics().snapshot().accepted_appends, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delete_stream_removes_state_on_owner_group() {
    let runtime = runtime(2, 8);
    let stream = BucketStreamId::new("benchcmp", "delete-stream");
    let placement = runtime.locate(&stream);
    create_stream(&runtime, &stream).await;
    runtime
        .append(AppendRequest::from_bytes(
            stream.clone(),
            b"payload".to_vec(),
        ))
        .await
        .expect("append");

    let deleted = runtime
        .delete_stream(DeleteStreamRequest {
            stream_id: stream.clone(),
        })
        .await
        .expect("delete stream");
    assert_eq!(deleted.placement, placement);

    let err = runtime
        .head_stream(HeadStreamRequest {
            stream_id: stream.clone(),
            now_ms: 0,
        })
        .await
        .expect_err("head after delete rejected");
    match err {
        RuntimeError::GroupEngine { error, .. } => {
            let message = error.message();
            assert!(message.contains("StreamNotFound"), "message={message}");
        }
        other => panic!("expected group engine error, got {other:?}"),
    }

    let err = runtime
        .append(AppendRequest::new(stream, 1))
        .await
        .expect_err("append after delete rejected");
    match err {
        RuntimeError::GroupEngine { error, .. } => {
            let message = error.message();
            assert!(message.contains("StreamNotFound"), "message={message}");
        }
        other => panic!("expected group engine error, got {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fork_ref_keeps_deleted_source_gone_until_last_fork_delete() {
    let runtime = runtime(2, 8);
    let source = BucketStreamId::new("benchcmp", "fork-ref-source");
    let fork = BucketStreamId::new("benchcmp", "fork-ref-child");
    let mut source_create = CreateStreamRequest::new(source.clone(), DEFAULT_CONTENT_TYPE);
    source_create.initial_payload = Bytes::from_static(b"abc");
    runtime
        .create_stream(source_create)
        .await
        .expect("create source");

    let mut fork_create = CreateStreamRequest::new(fork.clone(), DEFAULT_CONTENT_TYPE);
    fork_create.forked_from = Some(source.clone());
    runtime
        .create_stream(fork_create)
        .await
        .expect("create fork");

    runtime
        .delete_stream(DeleteStreamRequest {
            stream_id: source.clone(),
        })
        .await
        .expect("delete source");
    let err = runtime
        .head_stream(HeadStreamRequest {
            stream_id: source.clone(),
            now_ms: 0,
        })
        .await
        .expect_err("soft-deleted source is gone");
    match err {
        RuntimeError::GroupEngine { error, .. } => {
            let message = error.message();
            assert!(message.contains("StreamGone"), "message={message}");
        }
        other => panic!("expected group engine error, got {other:?}"),
    }

    let fork_read = runtime
        .read_stream(ReadStreamRequest {
            stream_id: fork.clone(),
            offset: 0,
            max_len: 16,
            now_ms: 0,
        })
        .await
        .expect("fork remains readable");
    assert_eq!(fork_read.payload, b"abc");

    runtime
        .delete_stream(DeleteStreamRequest { stream_id: fork })
        .await
        .expect("delete fork");
    let err = runtime
        .head_stream(HeadStreamRequest {
            stream_id: source,
            now_ms: 0,
        })
        .await
        .expect_err("source is hard-deleted after last fork");
    match err {
        RuntimeError::GroupEngine { error, .. } => {
            let message = error.message();
            assert!(message.contains("StreamNotFound"), "message={message}");
        }
        other => panic!("expected group engine error, got {other:?}"),
    }
}

#[cfg(not(madsim))]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn thread_per_core_runtime_reaches_all_configured_cores() {
    let mut config = RuntimeConfig::new(4, 32);
    config.mailbox_capacity = 128;
    assert_eq!(config.threading, RuntimeThreading::ThreadPerCore);
    let runtime = ShardRuntime::spawn(config).expect("spawn runtime");

    let mut tasks = Vec::new();
    for index in 0..1024 {
        let runtime = runtime.clone();
        tasks.push(tokio::spawn(async move {
            let stream = BucketStreamId::new("benchcmp", format!("thread-core-{index}"));
            create_stream(&runtime, &stream).await;
            runtime
                .append(AppendRequest::new(stream, 1))
                .await
                .expect("append");
        }));
    }

    for task in tasks {
        task.await.expect("task");
    }

    let snapshot = runtime.metrics().snapshot();
    assert_eq!(snapshot.accepted_appends, 1024);
    assert!(snapshot.per_core_appends.iter().all(|value| *value > 0));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn custom_group_engine_is_created_once_per_touched_group_on_owner_core() {
    let factory = RecordingFactory::default();
    let runtime = ShardRuntime::spawn_with_engine_factory(
        RuntimeConfig {
            core_count: 4,
            raft_group_count: 32,
            mailbox_capacity: 128,
            threading: RuntimeThreading::HostedTokio,
            cold_max_hot_bytes_per_group: None,
            raft_max_uncommitted_bytes_per_group: None,
            live_read_max_waiters_per_core: Some(65_536),
        },
        factory.clone(),
    )
    .expect("spawn runtime");

    let mut touched_groups = HashSet::new();
    for index in 0..4096 {
        let stream = BucketStreamId::new("benchcmp", format!("engine-{index}"));
        let placement = runtime.locate(&stream);
        runtime
            .create_stream(CreateStreamRequest::new(stream, DEFAULT_CONTENT_TYPE))
            .await
            .expect("create stream");
        touched_groups.insert(placement.raft_group_id);
        if touched_groups.len() == 16 {
            break;
        }
    }

    let created = factory.created();
    let created_groups = created
        .iter()
        .map(|placement| placement.raft_group_id)
        .collect::<HashSet<_>>();
    assert_eq!(created_groups, touched_groups);
    for placement in created {
        assert_eq!(
            u32::from(placement.core_id.0),
            placement.raft_group_id.0 % 4
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn background_cold_flush_skips_groups_that_cannot_accept_local_writes() {
    let factory = RecordingFactory::without_local_writes();
    let cold_store = Arc::new(ColdStore::memory().expect("memory cold store"));
    let runtime = ShardRuntime::spawn_with_engine_factory_and_cold_store(
        RuntimeConfig {
            core_count: 2,
            raft_group_count: 4,
            mailbox_capacity: 128,
            threading: RuntimeThreading::HostedTokio,
            cold_max_hot_bytes_per_group: None,
            raft_max_uncommitted_bytes_per_group: None,
            live_read_max_waiters_per_core: Some(65_536),
        },
        factory.clone(),
        Some(cold_store),
    )
    .expect("spawn runtime");

    let flushed = runtime
        .flush_cold_all_groups_once_bounded(
            PlanGroupColdFlushRequest {
                min_hot_bytes: 1,
                max_flush_bytes: 1,
            },
            4,
        )
        .await
        .expect("flush all groups");

    assert_eq!(flushed, 0);
    assert_eq!(factory.created().len(), 4);
    let metrics = runtime.metrics().snapshot();
    assert_eq!(metrics.cold_flush_uploads, 0);
    assert_eq!(metrics.cold_flush_publishes, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn warm_group_instantiates_engine_on_owner_core_without_stream_mutation() {
    let factory = RecordingFactory::default();
    let runtime = ShardRuntime::spawn_with_engine_factory(
        RuntimeConfig {
            core_count: 2,
            raft_group_count: 4,
            mailbox_capacity: 128,
            threading: RuntimeThreading::HostedTokio,
            cold_max_hot_bytes_per_group: None,
            raft_max_uncommitted_bytes_per_group: None,
            live_read_max_waiters_per_core: Some(65_536),
        },
        factory.clone(),
    )
    .expect("spawn runtime");

    let warmed = runtime
        .warm_group(RaftGroupId(3))
        .await
        .expect("warm group");
    assert_eq!(warmed.core_id, CoreId(1));
    assert_eq!(warmed.raft_group_id, RaftGroupId(3));

    runtime
        .warm_group(RaftGroupId(3))
        .await
        .expect("second warm is idempotent");

    let created = factory.created();
    assert_eq!(created, vec![warmed]);

    runtime.warm_all_groups().await.expect("warm all groups");
    let created_groups = factory
        .created()
        .into_iter()
        .map(|placement| placement.raft_group_id)
        .collect::<HashSet<_>>();
    assert_eq!(
        created_groups,
        [
            RaftGroupId(0),
            RaftGroupId(1),
            RaftGroupId(2),
            RaftGroupId(3)
        ]
        .into_iter()
        .collect()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn core_worker_dispatches_other_groups_while_one_group_waits() {
    let factory = BlockingFirstCreateEngineFactory::default();
    let runtime = ShardRuntime::spawn_with_engine_factory(
        RuntimeConfig {
            core_count: 1,
            raft_group_count: 2,
            mailbox_capacity: 128,
            threading: RuntimeThreading::HostedTokio,
            cold_max_hot_bytes_per_group: None,
            raft_max_uncommitted_bytes_per_group: None,
            live_read_max_waiters_per_core: Some(65_536),
        },
        factory.clone(),
    )
    .expect("spawn runtime");

    let blocked_stream = stream_on_group(&runtime, RaftGroupId(0), "blocked-group");
    let free_stream = stream_on_group(&runtime, RaftGroupId(1), "free-group");
    let entered_wait = factory.entered.notified();
    let blocked_runtime = runtime.clone();
    let blocked =
        tokio::spawn(async move { create_stream(&blocked_runtime, &blocked_stream).await });

    tokio::time::timeout(std::time::Duration::from_secs(1), entered_wait)
        .await
        .expect("first group entered blocking create");

    let completed = tokio::time::timeout(
        std::time::Duration::from_secs(1),
        create_stream(&runtime, &free_stream),
    )
    .await
    .expect("other group should complete while first group is blocked");
    assert_eq!(completed.placement.raft_group_id, RaftGroupId(1));

    factory.release.notify_one();
    blocked.await.expect("blocked task");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn runtime_read_uses_group_read_parts_fast_path() {
    let factory = BlockingReadFactory::default();
    let runtime = ShardRuntime::spawn_with_engine_factory(
        RuntimeConfig {
            core_count: 1,
            raft_group_count: 1,
            mailbox_capacity: 128,
            threading: RuntimeThreading::HostedTokio,
            cold_max_hot_bytes_per_group: None,
            raft_max_uncommitted_bytes_per_group: None,
            live_read_max_waiters_per_core: Some(65_536),
        },
        factory.clone(),
    )
    .expect("spawn runtime");
    let stream = BucketStreamId::new("benchcmp", "read-offload");
    create_stream(&runtime, &stream).await;

    let read = tokio::time::timeout(
        std::time::Duration::from_secs(1),
        runtime.read_stream(ReadStreamRequest {
            stream_id: stream.clone(),
            offset: 0,
            max_len: 16,
            now_ms: 0,
        }),
    )
    .await
    .expect("runtime read should not use blocking legacy read_stream")
    .expect("read stream");
    assert_eq!(read.placement.raft_group_id, RaftGroupId(0));
    assert_eq!(factory.read_count.load(Ordering::Relaxed), 1);

    let head = runtime
        .head_stream(HeadStreamRequest {
            stream_id: stream,
            now_ms: 0,
        })
        .await
        .expect("head stream");
    assert_eq!(head.placement.raft_group_id, RaftGroupId(0));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn read_materialization_is_bounded_without_blocking_group_actor() {
    let factory = BlockingReadFactory::block_materialization();
    let mut config = RuntimeConfig::new(1, 1);
    config.mailbox_capacity = 1;
    config.threading = RuntimeThreading::HostedTokio;
    let runtime =
        ShardRuntime::spawn_with_engine_factory(config, factory.clone()).expect("spawn runtime");
    let first_stream = BucketStreamId::new("benchcmp", "materialize-bound-1");
    let second_stream = BucketStreamId::new("benchcmp", "materialize-bound-2");
    create_stream(&runtime, &first_stream).await;
    create_stream(&runtime, &second_stream).await;

    let first_runtime = runtime.clone();
    let first_stream_for_read = first_stream.clone();
    let first_read = tokio::spawn(async move {
        first_runtime
            .read_stream(ReadStreamRequest {
                stream_id: first_stream_for_read,
                offset: 0,
                max_len: 16,
                now_ms: 0,
            })
            .await
    });
    tokio::time::timeout(
        std::time::Duration::from_secs(1),
        factory.materialized.notified(),
    )
    .await
    .expect("first materialization acquired the only permit");

    let second_runtime = runtime.clone();
    let second_stream_for_read = second_stream.clone();
    let second_read = tokio::spawn(async move {
        second_runtime
            .read_stream(ReadStreamRequest {
                stream_id: second_stream_for_read,
                offset: 0,
                max_len: 16,
                now_ms: 0,
            })
            .await
    });

    let head = tokio::time::timeout(
        std::time::Duration::from_secs(1),
        runtime.head_stream(HeadStreamRequest {
            stream_id: first_stream,
            now_ms: 0,
        }),
    )
    .await
    .expect("group actor should keep serving metadata while materialization waits")
    .expect("head stream");
    assert_eq!(head.placement.raft_group_id, RaftGroupId(0));
    assert!(!second_read.is_finished());

    factory.release.notify_one();
    let first = first_read
        .await
        .expect("first read task")
        .expect("first read");
    assert_eq!(first.payload, b"ready");
    tokio::time::timeout(
        std::time::Duration::from_secs(1),
        factory.materialized.notified(),
    )
    .await
    .expect("second materialization acquired permit after first released it");
    factory.release.notify_one();
    let second = second_read
        .await
        .expect("second read task")
        .expect("second read");
    assert_eq!(second.payload, b"ready");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn group_engine_errors_include_group_context_and_do_not_record_success_metrics() {
    let runtime = ShardRuntime::spawn_with_engine_factory(
        RuntimeConfig {
            core_count: 2,
            raft_group_count: 8,
            mailbox_capacity: 128,
            threading: RuntimeThreading::HostedTokio,
            cold_max_hot_bytes_per_group: None,
            raft_max_uncommitted_bytes_per_group: None,
            live_read_max_waiters_per_core: Some(65_536),
        },
        FailingFactory,
    )
    .expect("spawn runtime");

    let stream = BucketStreamId::new("benchcmp", "failing-stream");
    let placement = runtime.locate(&stream);
    let err = runtime
        .append(AppendRequest::new(stream, 1))
        .await
        .expect_err("engine failure");

    assert_eq!(
        err,
        RuntimeError::GroupEngine {
            core_id: placement.core_id,
            raft_group_id: placement.raft_group_id,
            error: GroupEngineError::new("proposal rejected"),
        }
    );
    assert_eq!(runtime.metrics().snapshot().accepted_appends, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mailbox_full_events_record_owner_core_backpressure() {
    let factory = BlockingOnceFactory::default();
    let runtime = ShardRuntime::spawn_with_engine_factory(
        RuntimeConfig {
            core_count: 1,
            raft_group_count: 1,
            mailbox_capacity: 1,
            threading: RuntimeThreading::HostedTokio,
            cold_max_hot_bytes_per_group: None,
            raft_max_uncommitted_bytes_per_group: None,
            live_read_max_waiters_per_core: Some(65_536),
        },
        factory.clone(),
    )
    .expect("spawn runtime");

    let entered = factory.entered.clone();
    let entered_wait = entered.notified();
    let first_runtime = runtime.clone();
    let first = tokio::spawn(async move {
        create_stream(
            &first_runtime,
            &BucketStreamId::new("benchcmp", "backpressure-1"),
        )
        .await
    });
    tokio::time::timeout(std::time::Duration::from_secs(1), entered_wait)
        .await
        .expect("first create entered blocking engine factory");

    let second_runtime = runtime.clone();
    let second = tokio::spawn(async move {
        create_stream(
            &second_runtime,
            &BucketStreamId::new("benchcmp", "backpressure-2"),
        )
        .await
    });
    wait_for_mailbox_depth(&runtime, 0, 1).await;

    let third_runtime = runtime.clone();
    let third = tokio::spawn(async move {
        create_stream(
            &third_runtime,
            &BucketStreamId::new("benchcmp", "backpressure-3"),
        )
        .await
    });
    wait_for_mailbox_full_events(&runtime, 1).await;
    assert_eq!(
        runtime.metrics().snapshot().per_core_mailbox_full_events[0],
        1
    );

    factory.release.notify_one();
    first.await.expect("first task");
    second.await.expect("second task");
    third.await.expect("third task");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn group_mailbox_full_events_record_inner_actor_backpressure() {
    let factory = BlockingFirstCreateEngineFactory::default();
    let runtime = ShardRuntime::spawn_with_engine_factory(
        RuntimeConfig {
            core_count: 1,
            raft_group_count: 1,
            mailbox_capacity: 1,
            threading: RuntimeThreading::HostedTokio,
            cold_max_hot_bytes_per_group: None,
            raft_max_uncommitted_bytes_per_group: None,
            live_read_max_waiters_per_core: Some(65_536),
        },
        factory.clone(),
    )
    .expect("spawn runtime");

    let first_runtime = runtime.clone();
    let first = tokio::spawn(async move {
        create_stream(
            &first_runtime,
            &BucketStreamId::new("benchcmp", "group-backpressure-1"),
        )
        .await
    });
    tokio::time::timeout(
        std::time::Duration::from_secs(1),
        factory.entered.notified(),
    )
    .await
    .expect("first append entered blocking group engine");

    let second_runtime = runtime.clone();
    let second = tokio::spawn(async move {
        create_stream(
            &second_runtime,
            &BucketStreamId::new("benchcmp", "group-backpressure-2"),
        )
        .await
    });
    for _ in 0..100 {
        if runtime.metrics().snapshot().group_mailbox_depth == 1 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }

    let third_runtime = runtime.clone();
    let third = tokio::spawn(async move {
        create_stream(
            &third_runtime,
            &BucketStreamId::new("benchcmp", "group-backpressure-3"),
        )
        .await
    });
    wait_for_group_mailbox_full_events(&runtime, 1).await;
    assert_eq!(
        runtime
            .metrics()
            .snapshot()
            .per_group_group_mailbox_full_events[0],
        1
    );

    factory.release.notify_one();
    first.await.expect("first task");
    second.await.expect("second task");
    third.await.expect("third task");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wal_group_engine_recovers_multiple_groups_from_per_group_logs() {
    let wal_root = std::env::temp_dir().join(format!(
        "ursula-wal-test-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system time after unix epoch")
            .as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&wal_root);
    let config = RuntimeConfig {
        core_count: 2,
        raft_group_count: 8,
        mailbox_capacity: 128,
        threading: RuntimeThreading::HostedTokio,
        cold_max_hot_bytes_per_group: None,
        raft_max_uncommitted_bytes_per_group: None,
        live_read_max_waiters_per_core: Some(65_536),
    };

    let (first_stream, second_stream) = {
        let runtime = ShardRuntime::spawn_with_engine_factory(
            config.clone(),
            WalGroupEngineFactory::new(&wal_root),
        )
        .expect("spawn runtime");

        let mut seen_groups = HashSet::new();
        let mut streams = Vec::new();
        for index in 0..256 {
            let stream = BucketStreamId::new("benchcmp", format!("wal-{index}"));
            if seen_groups.insert(runtime.locate(&stream).raft_group_id) {
                streams.push(stream);
            }
            if streams.len() == 2 {
                break;
            }
        }
        assert_eq!(streams.len(), 2, "expected streams on two groups");
        let first_stream = streams[0].clone();
        let second_stream = streams[1].clone();

        create_stream(&runtime, &first_stream).await;
        runtime
            .append(AppendRequest::from_bytes(
                first_stream.clone(),
                b"first-payload".to_vec(),
            ))
            .await
            .expect("append first stream");

        create_stream(&runtime, &second_stream).await;
        let mut append_second =
            AppendRequest::from_bytes(second_stream.clone(), b"second-payload".to_vec());
        append_second.close_after = true;
        runtime
            .append(append_second)
            .await
            .expect("append second stream");

        (first_stream, second_stream)
    };

    let recovered =
        ShardRuntime::spawn_with_engine_factory(config, WalGroupEngineFactory::new(&wal_root))
            .expect("spawn recovered runtime");

    let first_read = recovered
        .read_stream(ReadStreamRequest {
            stream_id: first_stream.clone(),
            offset: 0,
            max_len: 128,
            now_ms: 0,
        })
        .await
        .expect("read recovered first stream");
    assert_eq!(first_read.payload, b"first-payload");
    assert!(!first_read.closed);

    let second_read = recovered
        .read_stream(ReadStreamRequest {
            stream_id: second_stream.clone(),
            offset: 0,
            max_len: 128,
            now_ms: 0,
        })
        .await
        .expect("read recovered second stream");
    assert_eq!(second_read.payload, b"second-payload");
    assert!(second_read.closed);

    let mut wal_file_count = 0;
    for core_entry in std::fs::read_dir(&wal_root).expect("read WAL root") {
        let core_entry = core_entry.expect("read core WAL dir");
        for group_entry in std::fs::read_dir(core_entry.path()).expect("read group WAL dir") {
            let group_entry = group_entry.expect("read group WAL file");
            if group_entry
                .path()
                .extension()
                .is_some_and(|ext| ext == "jsonl")
            {
                wal_file_count += 1;
            }
        }
    }
    assert_eq!(wal_file_count, 2);

    drop(recovered);
    std::fs::remove_dir_all(&wal_root).expect("remove WAL root");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wal_group_engine_batches_append_records_and_recovers() {
    let wal_root = std::env::temp_dir().join(format!(
        "ursula-wal-batch-test-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system time after unix epoch")
            .as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&wal_root);
    let config = RuntimeConfig {
        core_count: 2,
        raft_group_count: 8,
        mailbox_capacity: 128,
        threading: RuntimeThreading::HostedTokio,
        cold_max_hot_bytes_per_group: None,
        raft_max_uncommitted_bytes_per_group: None,
        live_read_max_waiters_per_core: Some(65_536),
    };
    let stream = BucketStreamId::new("benchcmp", "wal-batch");
    let placement;

    {
        let runtime = ShardRuntime::spawn_with_engine_factory(
            config.clone(),
            WalGroupEngineFactory::new(&wal_root),
        )
        .expect("spawn runtime");
        placement = runtime.locate(&stream);
        create_stream(&runtime, &stream).await;
        let response = runtime
            .append_batch(AppendBatchRequest::new(
                stream.clone(),
                vec![b"ab".to_vec(), b"cd".to_vec(), b"ef".to_vec()],
            ))
            .await
            .expect("append batch");
        assert_eq!(response.items.len(), 3);
        assert!(response.items.iter().all(Result::is_ok));

        let read = runtime
            .read_stream(ReadStreamRequest {
                stream_id: stream.clone(),
                offset: 0,
                max_len: 16,
                now_ms: 0,
            })
            .await
            .expect("read");
        assert_eq!(read.payload, b"abcdef");

        let snapshot = runtime.metrics().snapshot();
        let core_index = usize::from(placement.core_id.0);
        let group_index = usize::try_from(placement.raft_group_id.0).expect("u32 fits usize");
        assert_eq!(snapshot.wal_batches, 2);
        assert_eq!(snapshot.wal_records, 2);
        assert_eq!(snapshot.per_core_wal_batches[core_index], 2);
        assert_eq!(snapshot.per_group_wal_batches[group_index], 2);
        assert_eq!(snapshot.per_core_wal_records[core_index], 2);
        assert_eq!(snapshot.per_group_wal_records[group_index], 2);
        assert!(snapshot.wal_write_ns > 0);
        assert!(snapshot.wal_sync_ns > 0);
        assert_eq!(
            snapshot.wal_write_ns,
            snapshot.per_core_wal_write_ns.iter().sum::<u64>()
        );
        assert_eq!(
            snapshot.wal_sync_ns,
            snapshot.per_group_wal_sync_ns.iter().sum::<u64>()
        );
    }

    let log_path = group_log_path(&wal_root, placement);
    let line_count = std::fs::read_to_string(&log_path)
        .expect("read WAL log")
        .lines()
        .count();
    assert_eq!(line_count, 2);

    let recovered =
        ShardRuntime::spawn_with_engine_factory(config, WalGroupEngineFactory::new(&wal_root))
            .expect("spawn recovered runtime");
    let read = recovered
        .read_stream(ReadStreamRequest {
            stream_id: stream,
            offset: 0,
            max_len: 16,
            now_ms: 0,
        })
        .await
        .expect("read recovered batch");
    assert_eq!(read.payload, b"abcdef");

    drop(recovered);
    std::fs::remove_dir_all(&wal_root).expect("remove WAL root");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wal_group_engine_persists_installed_snapshot() {
    let wal_root = std::env::temp_dir().join(format!(
        "ursula-wal-install-snapshot-test-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system time after unix epoch")
            .as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&wal_root);
    let config = RuntimeConfig {
        core_count: 2,
        raft_group_count: 8,
        mailbox_capacity: 128,
        threading: RuntimeThreading::HostedTokio,
        cold_max_hot_bytes_per_group: None,
        raft_max_uncommitted_bytes_per_group: None,
        live_read_max_waiters_per_core: Some(65_536),
    };
    let stream = BucketStreamId::new("benchcmp", "wal-installed-snapshot");
    let source = runtime(2, 8);
    let placement = source.locate(&stream);
    create_stream(&source, &stream).await;
    source
        .append(AppendRequest::from_bytes(
            stream.clone(),
            b"snapshot-payload".to_vec(),
        ))
        .await
        .expect("append source");
    let snapshot = source
        .snapshot_group(placement.raft_group_id)
        .await
        .expect("snapshot source");

    {
        let target = ShardRuntime::spawn_with_engine_factory(
            config.clone(),
            WalGroupEngineFactory::new(&wal_root),
        )
        .expect("spawn WAL runtime");
        target
            .install_group_snapshot(snapshot)
            .await
            .expect("install snapshot");
    }

    let recovered =
        ShardRuntime::spawn_with_engine_factory(config, WalGroupEngineFactory::new(&wal_root))
            .expect("spawn recovered WAL runtime");
    let read = recovered
        .read_stream(ReadStreamRequest {
            stream_id: stream.clone(),
            offset: 0,
            max_len: 32,
            now_ms: 0,
        })
        .await
        .expect("read recovered snapshot");
    assert_eq!(read.payload, b"snapshot-payload");

    let appended = recovered
        .append(AppendRequest::from_bytes(stream, b"-next".to_vec()))
        .await
        .expect("append after recovered snapshot");
    assert_eq!(appended.start_offset, 16);
    assert_eq!(appended.stream_append_count, 2);

    drop(recovered);
    std::fs::remove_dir_all(&wal_root).expect("remove WAL root");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wal_group_engine_recovers_producer_dedup_state() {
    let wal_root = std::env::temp_dir().join(format!(
        "ursula-wal-producer-test-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system time after unix epoch")
            .as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&wal_root);
    let config = RuntimeConfig {
        core_count: 2,
        raft_group_count: 8,
        mailbox_capacity: 128,
        threading: RuntimeThreading::HostedTokio,
        cold_max_hot_bytes_per_group: None,
        raft_max_uncommitted_bytes_per_group: None,
        live_read_max_waiters_per_core: Some(65_536),
    };
    let stream = BucketStreamId::new("benchcmp", "wal-producer");

    {
        let runtime = ShardRuntime::spawn_with_engine_factory(
            config.clone(),
            WalGroupEngineFactory::new(&wal_root),
        )
        .expect("spawn WAL runtime");
        create_stream(&runtime, &stream).await;
        let mut append = AppendRequest::from_bytes(stream.clone(), b"a".to_vec());
        append.producer = Some(producer("writer-1", 0, 0));
        runtime.append(append).await.expect("append");
    }

    let recovered =
        ShardRuntime::spawn_with_engine_factory(config, WalGroupEngineFactory::new(&wal_root))
            .expect("spawn recovered runtime");
    let mut duplicate = AppendRequest::from_bytes(stream.clone(), b"ignored".to_vec());
    duplicate.producer = Some(producer("writer-1", 0, 0));
    let duplicate = recovered
        .append(duplicate)
        .await
        .expect("deduplicated retry");
    assert!(duplicate.deduplicated);
    assert_eq!(duplicate.start_offset, 0);
    assert_eq!(duplicate.next_offset, 1);
    assert_eq!(duplicate.stream_append_count, 1);

    let mut next = AppendRequest::from_bytes(stream.clone(), b"b".to_vec());
    next.producer = Some(producer("writer-1", 0, 1));
    let next = recovered.append(next).await.expect("next append");
    assert_eq!(next.start_offset, 1);
    assert_eq!(next.next_offset, 2);
    assert_eq!(next.stream_append_count, 2);

    let read = recovered
        .read_stream(ReadStreamRequest {
            stream_id: stream,
            offset: 0,
            max_len: 16,
            now_ms: 0,
        })
        .await
        .expect("read");
    assert_eq!(read.payload, b"ab");

    drop(recovered);
    std::fs::remove_dir_all(&wal_root).expect("remove WAL root");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wal_group_engine_recovers_producer_append_batch_dedup_state() {
    let wal_root = std::env::temp_dir().join(format!(
        "ursula-wal-producer-batch-test-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system time after unix epoch")
            .as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&wal_root);
    let config = RuntimeConfig {
        core_count: 2,
        raft_group_count: 8,
        mailbox_capacity: 128,
        threading: RuntimeThreading::HostedTokio,
        cold_max_hot_bytes_per_group: None,
        raft_max_uncommitted_bytes_per_group: None,
        live_read_max_waiters_per_core: Some(65_536),
    };
    let stream = BucketStreamId::new("benchcmp", "wal-producer-batch");
    let placement;

    {
        let runtime = ShardRuntime::spawn_with_engine_factory(
            config.clone(),
            WalGroupEngineFactory::new(&wal_root),
        )
        .expect("spawn WAL runtime");
        placement = runtime.locate(&stream);
        create_stream(&runtime, &stream).await;

        let mut first = AppendBatchRequest::new(stream.clone(), vec![b"a".to_vec(), b"b".to_vec()]);
        first.producer = Some(producer("writer-1", 0, 0));
        let first = runtime.append_batch(first).await.expect("first batch");
        assert!(first.items.iter().all(Result::is_ok));

        let mut duplicate = AppendBatchRequest::new(stream.clone(), vec![b"ignored".to_vec()]);
        duplicate.producer = Some(producer("writer-1", 0, 0));
        let duplicate = runtime
            .append_batch(duplicate)
            .await
            .expect("duplicate batch");
        assert!(
            duplicate
                .items
                .iter()
                .all(|item| { item.as_ref().expect("deduplicated item").deduplicated })
        );
    }

    let log_path = group_log_path(&wal_root, placement);
    let line_count = std::fs::read_to_string(&log_path)
        .expect("read WAL log")
        .lines()
        .count();
    assert_eq!(line_count, 2);

    let recovered =
        ShardRuntime::spawn_with_engine_factory(config, WalGroupEngineFactory::new(&wal_root))
            .expect("spawn recovered runtime");
    let mut duplicate = AppendBatchRequest::new(stream.clone(), vec![b"retry".to_vec()]);
    duplicate.producer = Some(producer("writer-1", 0, 0));
    let duplicate = recovered
        .append_batch(duplicate)
        .await
        .expect("deduplicated retry");
    assert_eq!(duplicate.items.len(), 2);
    assert!(
        duplicate
            .items
            .iter()
            .all(|item| { item.as_ref().expect("deduplicated item").deduplicated })
    );

    let mut next = AppendBatchRequest::new(stream.clone(), vec![b"c".to_vec()]);
    next.producer = Some(producer("writer-1", 0, 1));
    let next = recovered.append_batch(next).await.expect("next batch");
    assert_eq!(next.items[0].as_ref().expect("next item").start_offset, 2);

    let read = recovered
        .read_stream(ReadStreamRequest {
            stream_id: stream,
            offset: 0,
            max_len: 16,
            now_ms: 0,
        })
        .await
        .expect("read");
    assert_eq!(read.payload, b"abc");

    drop(recovered);
    std::fs::remove_dir_all(&wal_root).expect("remove WAL root");
}

#[derive(Debug, Clone)]
struct RecordingFactory {
    created: Arc<Mutex<Vec<ShardPlacement>>>,
    accepts_local_writes: bool,
    cold_hot_bytes: u64,
}

impl Default for RecordingFactory {
    fn default() -> Self {
        Self {
            created: Arc::default(),
            accepts_local_writes: true,
            cold_hot_bytes: 0,
        }
    }
}

impl RecordingFactory {
    fn without_local_writes() -> Self {
        Self {
            accepts_local_writes: false,
            ..Self::default()
        }
    }

    fn with_cold_hot_bytes(mut self, bytes: u64) -> Self {
        self.cold_hot_bytes = bytes;
        self
    }

    fn created(&self) -> Vec<ShardPlacement> {
        self.created.lock().expect("lock created groups").clone()
    }
}

impl GroupEngineFactory for RecordingFactory {
    fn create<'a>(
        &'a self,
        placement: ShardPlacement,
        _metrics: GroupEngineMetrics,
    ) -> GroupEngineCreateFuture<'a> {
        Box::pin(async move {
            self.created
                .lock()
                .expect("lock created groups")
                .push(placement);
            let engine: Box<dyn GroupEngine> = Box::new(RecordingEngine {
                placement,
                commit_index: 0,
                accepts_local_writes: self.accepts_local_writes,
                cold_hot_bytes: self.cold_hot_bytes,
            });
            Ok(engine)
        })
    }
}

struct RecordingEngine {
    placement: ShardPlacement,
    commit_index: u64,
    accepts_local_writes: bool,
    cold_hot_bytes: u64,
}

#[derive(Clone)]
struct BlockingReadFactory {
    entered: Arc<Notify>,
    materialized: Arc<Notify>,
    release: Arc<Notify>,
    read_count: Arc<AtomicU64>,
    block_parts: bool,
}

impl Default for BlockingReadFactory {
    fn default() -> Self {
        Self {
            entered: Arc::new(Notify::new()),
            materialized: Arc::new(Notify::new()),
            release: Arc::new(Notify::new()),
            read_count: Arc::new(AtomicU64::new(0)),
            block_parts: false,
        }
    }
}

impl BlockingReadFactory {
    fn block_materialization() -> Self {
        Self {
            block_parts: true,
            ..Self::default()
        }
    }
}

impl GroupEngineFactory for BlockingReadFactory {
    fn create<'a>(
        &'a self,
        placement: ShardPlacement,
        _metrics: GroupEngineMetrics,
    ) -> GroupEngineCreateFuture<'a> {
        Box::pin(async move {
            let engine: Box<dyn GroupEngine> = Box::new(BlockingReadEngine {
                inner: InMemoryGroupEngine::default(),
                placement,
                entered: self.entered.clone(),
                materialized: self.materialized.clone(),
                release: self.release.clone(),
                read_count: self.read_count.clone(),
                block_parts: self.block_parts,
            });
            Ok(engine)
        })
    }
}

struct BlockingReadEngine {
    inner: InMemoryGroupEngine,
    placement: ShardPlacement,
    entered: Arc<Notify>,
    materialized: Arc<Notify>,
    release: Arc<Notify>,
    read_count: Arc<AtomicU64>,
    block_parts: bool,
}

impl GroupEngine for BlockingReadEngine {
    fn create_stream<'a>(
        &'a mut self,
        request: CreateStreamRequest,
        placement: ShardPlacement,
    ) -> GroupCreateStreamFuture<'a> {
        self.inner.create_stream(request, placement)
    }

    fn head_stream<'a>(
        &'a mut self,
        request: HeadStreamRequest,
        placement: ShardPlacement,
    ) -> GroupHeadStreamFuture<'a> {
        self.inner.head_stream(request, placement)
    }

    fn read_stream<'a>(
        &'a mut self,
        request: ReadStreamRequest,
        placement: ShardPlacement,
    ) -> GroupReadStreamFuture<'a> {
        let entered = self.entered.clone();
        let release = self.release.clone();
        let read_count = self.read_count.clone();
        Box::pin(async move {
            assert_eq!(placement, self.placement);
            read_count.fetch_add(1, Ordering::Relaxed);
            entered.notify_one();
            release.notified().await;
            Ok(ReadStreamResponse {
                placement,
                offset: request.offset,
                next_offset: request.offset,
                content_type: DEFAULT_CONTENT_TYPE.to_owned(),
                payload: Vec::new(),
                up_to_date: true,
                closed: false,
            })
        })
    }

    fn read_stream_parts<'a>(
        &'a mut self,
        request: ReadStreamRequest,
        placement: ShardPlacement,
    ) -> GroupReadStreamPartsFuture<'a> {
        let entered = self.entered.clone();
        let read_count = self.read_count.clone();
        Box::pin(async move {
            assert_eq!(placement, self.placement);
            read_count.fetch_add(1, Ordering::Relaxed);
            entered.notify_one();
            if self.block_parts {
                return Ok(GroupReadStreamParts {
                    placement,
                    offset: request.offset,
                    next_offset: request.offset
                        + u64::try_from(b"ready".len()).expect("payload len fits u64"),
                    content_type: DEFAULT_CONTENT_TYPE.to_owned(),
                    up_to_date: true,
                    closed: false,
                    body: GroupReadStreamBody::Blocking {
                        entered: self.entered.clone(),
                        materialized: self.materialized.clone(),
                        release: self.release.clone(),
                        payload: b"ready".to_vec(),
                    },
                });
            }
            let response = ReadStreamResponse {
                placement,
                offset: request.offset,
                next_offset: request.offset,
                content_type: DEFAULT_CONTENT_TYPE.to_owned(),
                payload: Vec::new(),
                up_to_date: true,
                closed: false,
            };
            Ok(GroupReadStreamParts::from_response(response))
        })
    }

    fn touch_stream_access<'a>(
        &'a mut self,
        stream_id: BucketStreamId,
        now_ms: u64,
        renew_ttl: bool,
        placement: ShardPlacement,
    ) -> GroupTouchStreamAccessFuture<'a> {
        self.inner
            .touch_stream_access(stream_id, now_ms, renew_ttl, placement)
    }

    fn add_fork_ref<'a>(
        &'a mut self,
        stream_id: BucketStreamId,
        now_ms: u64,
        placement: ShardPlacement,
    ) -> GroupForkRefFuture<'a> {
        self.inner.add_fork_ref(stream_id, now_ms, placement)
    }

    fn release_fork_ref<'a>(
        &'a mut self,
        stream_id: BucketStreamId,
        placement: ShardPlacement,
    ) -> GroupForkRefFuture<'a> {
        self.inner.release_fork_ref(stream_id, placement)
    }

    fn close_stream<'a>(
        &'a mut self,
        request: CloseStreamRequest,
        placement: ShardPlacement,
    ) -> GroupCloseStreamFuture<'a> {
        self.inner.close_stream(request, placement)
    }

    fn delete_stream<'a>(
        &'a mut self,
        request: DeleteStreamRequest,
        placement: ShardPlacement,
    ) -> GroupDeleteStreamFuture<'a> {
        self.inner.delete_stream(request, placement)
    }

    fn append<'a>(
        &'a mut self,
        request: AppendRequest,
        placement: ShardPlacement,
    ) -> GroupAppendFuture<'a> {
        self.inner.append(request, placement)
    }

    fn append_batch<'a>(
        &'a mut self,
        request: AppendBatchRequest,
        placement: ShardPlacement,
    ) -> GroupAppendBatchFuture<'a> {
        self.inner.append_batch(request, placement)
    }

    fn snapshot<'a>(&'a mut self, placement: ShardPlacement) -> GroupSnapshotFuture<'a> {
        Box::pin(async move {
            Ok(GroupSnapshot {
                placement,
                group_commit_index: 0,
                stream_snapshot: StreamSnapshot {
                    buckets: Vec::new(),
                    streams: Vec::new(),
                    pending_cold_gc: Vec::new(),
                    next_cold_gc_seq: 0,
                },
                stream_append_counts: Vec::new(),
            })
        })
    }

    fn install_snapshot<'a>(
        &'a mut self,
        _snapshot: GroupSnapshot,
    ) -> GroupInstallSnapshotFuture<'a> {
        Box::pin(async { Ok(()) })
    }
}

impl GroupEngine for RecordingEngine {
    fn accepts_local_writes(&self) -> bool {
        self.accepts_local_writes
    }

    fn create_stream<'a>(
        &'a mut self,
        request: CreateStreamRequest,
        placement: ShardPlacement,
    ) -> GroupCreateStreamFuture<'a> {
        Box::pin(async move {
            assert_eq!(placement, self.placement);
            self.commit_index += 1;
            Ok(CreateStreamResponse {
                placement,
                next_offset: u64::try_from(request.initial_payload.len())
                    .expect("payload len fits u64"),
                closed: request.close_after,
                already_exists: false,
                group_commit_index: self.commit_index,
            })
        })
    }

    fn head_stream<'a>(
        &'a mut self,
        request: HeadStreamRequest,
        placement: ShardPlacement,
    ) -> GroupHeadStreamFuture<'a> {
        Box::pin(async move {
            assert_eq!(placement, self.placement);
            Ok(HeadStreamResponse {
                placement,
                content_type: DEFAULT_CONTENT_TYPE.to_owned(),
                tail_offset: request.stream_id.stream_id.len() as u64,
                cold_hot_start_offset: 0,
                closed: false,
                stream_ttl_seconds: None,
                stream_expires_at_ms: None,
                snapshot_offset: None,
                integrity: empty_integrity(),
            })
        })
    }

    fn read_stream<'a>(
        &'a mut self,
        request: ReadStreamRequest,
        placement: ShardPlacement,
    ) -> GroupReadStreamFuture<'a> {
        Box::pin(async move {
            assert_eq!(placement, self.placement);
            Ok(ReadStreamResponse {
                placement,
                offset: request.offset,
                next_offset: request.offset,
                content_type: DEFAULT_CONTENT_TYPE.to_owned(),
                payload: Vec::new(),
                up_to_date: true,
                closed: false,
            })
        })
    }

    fn touch_stream_access<'a>(
        &'a mut self,
        _stream_id: BucketStreamId,
        _now_ms: u64,
        _renew_ttl: bool,
        placement: ShardPlacement,
    ) -> GroupTouchStreamAccessFuture<'a> {
        Box::pin(async move {
            assert_eq!(placement, self.placement);
            Ok(TouchStreamAccessResponse {
                placement,
                changed: false,
                expired: false,
                group_commit_index: self.commit_index,
            })
        })
    }

    fn add_fork_ref<'a>(
        &'a mut self,
        _stream_id: BucketStreamId,
        _now_ms: u64,
        placement: ShardPlacement,
    ) -> GroupForkRefFuture<'a> {
        Box::pin(async move {
            assert_eq!(placement, self.placement);
            self.commit_index += 1;
            Ok(ForkRefResponse {
                placement,
                fork_ref_count: 1,
                hard_deleted: false,
                parent_to_release: None,
                group_commit_index: self.commit_index,
            })
        })
    }

    fn release_fork_ref<'a>(
        &'a mut self,
        _stream_id: BucketStreamId,
        placement: ShardPlacement,
    ) -> GroupForkRefFuture<'a> {
        Box::pin(async move {
            assert_eq!(placement, self.placement);
            self.commit_index += 1;
            Ok(ForkRefResponse {
                placement,
                fork_ref_count: 0,
                hard_deleted: false,
                parent_to_release: None,
                group_commit_index: self.commit_index,
            })
        })
    }

    fn close_stream<'a>(
        &'a mut self,
        _request: CloseStreamRequest,
        placement: ShardPlacement,
    ) -> GroupCloseStreamFuture<'a> {
        Box::pin(async move {
            assert_eq!(placement, self.placement);
            self.commit_index += 1;
            Ok(CloseStreamResponse {
                placement,
                next_offset: self.commit_index,
                group_commit_index: self.commit_index,
                deduplicated: false,
            })
        })
    }

    fn delete_stream<'a>(
        &'a mut self,
        _request: DeleteStreamRequest,
        placement: ShardPlacement,
    ) -> GroupDeleteStreamFuture<'a> {
        Box::pin(async move {
            assert_eq!(placement, self.placement);
            self.commit_index += 1;
            Ok(DeleteStreamResponse {
                placement,
                group_commit_index: self.commit_index,
                hard_deleted: true,
                parent_to_release: None,
            })
        })
    }

    fn append<'a>(
        &'a mut self,
        request: AppendRequest,
        placement: ShardPlacement,
    ) -> GroupAppendFuture<'a> {
        Box::pin(async move {
            assert_eq!(placement, self.placement);
            let start_offset = self.commit_index;
            let next_offset = start_offset + request.payload_len();
            self.commit_index += 1;
            Ok(AppendResponse {
                placement,
                start_offset,
                next_offset,
                stream_append_count: self.commit_index,
                group_commit_index: self.commit_index,
                closed: request.close_after,
                deduplicated: false,
                producer: request.producer,
            })
        })
    }

    fn append_batch<'a>(
        &'a mut self,
        request: AppendBatchRequest,
        placement: ShardPlacement,
    ) -> GroupAppendBatchFuture<'a> {
        Box::pin(async move {
            assert_eq!(placement, self.placement);
            let AppendBatchRequest {
                stream_id: _,
                content_type: _,
                payloads,
                producer: _,
                ..
            } = request;
            let mut items = Vec::with_capacity(payloads.len());
            for payload in payloads {
                let start_offset = self.commit_index;
                let next_offset =
                    start_offset + u64::try_from(payload.len()).expect("payload len fits u64");
                self.commit_index += 1;
                items.push(Ok(AppendResponse {
                    placement,
                    start_offset,
                    next_offset,
                    stream_append_count: self.commit_index,
                    group_commit_index: self.commit_index,
                    closed: false,
                    deduplicated: false,
                    producer: None,
                }));
            }
            Ok(GroupAppendBatchResponse { placement, items })
        })
    }

    fn cold_hot_backlog<'a>(
        &'a mut self,
        stream_id: BucketStreamId,
        placement: ShardPlacement,
    ) -> GroupColdHotBacklogFuture<'a> {
        Box::pin(async move {
            assert_eq!(placement, self.placement);
            Ok(ColdHotBacklog {
                stream_id,
                stream_hot_bytes: self.cold_hot_bytes,
                group_hot_bytes: self.cold_hot_bytes,
            })
        })
    }

    fn snapshot<'a>(&'a mut self, placement: ShardPlacement) -> GroupSnapshotFuture<'a> {
        Box::pin(async move {
            assert_eq!(placement, self.placement);
            Ok(GroupSnapshot {
                placement,
                group_commit_index: self.commit_index,
                stream_snapshot: StreamSnapshot {
                    buckets: Vec::new(),
                    streams: Vec::new(),
                    pending_cold_gc: Vec::new(),
                    next_cold_gc_seq: 0,
                },
                stream_append_counts: Vec::new(),
            })
        })
    }

    fn install_snapshot<'a>(
        &'a mut self,
        snapshot: GroupSnapshot,
    ) -> GroupInstallSnapshotFuture<'a> {
        Box::pin(async move {
            assert_eq!(snapshot.placement, self.placement);
            self.commit_index = snapshot.group_commit_index;
            Ok(())
        })
    }
}

#[derive(Debug, Clone)]
struct BlockingFirstCreateEngineFactory {
    first_create_blocks: Arc<AtomicBool>,
    entered: Arc<Notify>,
    release: Arc<Notify>,
}

impl Default for BlockingFirstCreateEngineFactory {
    fn default() -> Self {
        Self {
            first_create_blocks: Arc::new(AtomicBool::new(true)),
            entered: Arc::new(Notify::new()),
            release: Arc::new(Notify::new()),
        }
    }
}

impl GroupEngineFactory for BlockingFirstCreateEngineFactory {
    fn create<'a>(
        &'a self,
        _placement: ShardPlacement,
        _metrics: GroupEngineMetrics,
    ) -> GroupEngineCreateFuture<'a> {
        Box::pin(async move {
            let engine: Box<dyn GroupEngine> = Box::new(BlockingFirstCreateEngine {
                inner: InMemoryGroupEngine::default(),
                first_create_blocks: self.first_create_blocks.clone(),
                entered: self.entered.clone(),
                release: self.release.clone(),
            });
            Ok(engine)
        })
    }
}

struct BlockingFirstCreateEngine {
    inner: InMemoryGroupEngine,
    first_create_blocks: Arc<AtomicBool>,
    entered: Arc<Notify>,
    release: Arc<Notify>,
}

impl GroupEngine for BlockingFirstCreateEngine {
    fn create_stream<'a>(
        &'a mut self,
        request: CreateStreamRequest,
        placement: ShardPlacement,
    ) -> GroupCreateStreamFuture<'a> {
        let should_block = self.first_create_blocks.swap(false, Ordering::SeqCst);
        let entered = self.entered.clone();
        let release = self.release.clone();
        Box::pin(async move {
            if should_block {
                entered.notify_one();
                release.notified().await;
            }
            self.inner.create_stream(request, placement).await
        })
    }

    fn head_stream<'a>(
        &'a mut self,
        request: HeadStreamRequest,
        placement: ShardPlacement,
    ) -> GroupHeadStreamFuture<'a> {
        self.inner.head_stream(request, placement)
    }

    fn read_stream<'a>(
        &'a mut self,
        request: ReadStreamRequest,
        placement: ShardPlacement,
    ) -> GroupReadStreamFuture<'a> {
        self.inner.read_stream(request, placement)
    }

    fn touch_stream_access<'a>(
        &'a mut self,
        stream_id: BucketStreamId,
        now_ms: u64,
        renew_ttl: bool,
        placement: ShardPlacement,
    ) -> GroupTouchStreamAccessFuture<'a> {
        self.inner
            .touch_stream_access(stream_id, now_ms, renew_ttl, placement)
    }

    fn add_fork_ref<'a>(
        &'a mut self,
        stream_id: BucketStreamId,
        now_ms: u64,
        placement: ShardPlacement,
    ) -> GroupForkRefFuture<'a> {
        self.inner.add_fork_ref(stream_id, now_ms, placement)
    }

    fn release_fork_ref<'a>(
        &'a mut self,
        stream_id: BucketStreamId,
        placement: ShardPlacement,
    ) -> GroupForkRefFuture<'a> {
        self.inner.release_fork_ref(stream_id, placement)
    }

    fn close_stream<'a>(
        &'a mut self,
        request: CloseStreamRequest,
        placement: ShardPlacement,
    ) -> GroupCloseStreamFuture<'a> {
        self.inner.close_stream(request, placement)
    }

    fn delete_stream<'a>(
        &'a mut self,
        request: DeleteStreamRequest,
        placement: ShardPlacement,
    ) -> GroupDeleteStreamFuture<'a> {
        self.inner.delete_stream(request, placement)
    }

    fn append<'a>(
        &'a mut self,
        request: AppendRequest,
        placement: ShardPlacement,
    ) -> GroupAppendFuture<'a> {
        self.inner.append(request, placement)
    }

    fn append_batch<'a>(
        &'a mut self,
        request: AppendBatchRequest,
        placement: ShardPlacement,
    ) -> GroupAppendBatchFuture<'a> {
        self.inner.append_batch(request, placement)
    }

    fn snapshot<'a>(&'a mut self, placement: ShardPlacement) -> GroupSnapshotFuture<'a> {
        self.inner.snapshot(placement)
    }

    fn install_snapshot<'a>(
        &'a mut self,
        snapshot: GroupSnapshot,
    ) -> GroupInstallSnapshotFuture<'a> {
        self.inner.install_snapshot(snapshot)
    }
}

#[derive(Debug, Clone)]
struct BlockingOnceFactory {
    first_create_blocks: Arc<AtomicBool>,
    entered: Arc<Notify>,
    release: Arc<Notify>,
}

impl Default for BlockingOnceFactory {
    fn default() -> Self {
        Self {
            first_create_blocks: Arc::new(AtomicBool::new(true)),
            entered: Arc::new(Notify::new()),
            release: Arc::new(Notify::new()),
        }
    }
}

impl GroupEngineFactory for BlockingOnceFactory {
    fn create<'a>(
        &'a self,
        _placement: ShardPlacement,
        _metrics: GroupEngineMetrics,
    ) -> GroupEngineCreateFuture<'a> {
        Box::pin(async move {
            if self.first_create_blocks.swap(false, Ordering::SeqCst) {
                self.entered.notify_one();
                self.release.notified().await;
            }
            let engine: Box<dyn GroupEngine> = Box::new(InMemoryGroupEngine::default());
            Ok(engine)
        })
    }
}

#[derive(Debug, Clone, Copy)]
struct FailingFactory;

impl GroupEngineFactory for FailingFactory {
    fn create<'a>(
        &'a self,
        _placement: ShardPlacement,
        _metrics: GroupEngineMetrics,
    ) -> GroupEngineCreateFuture<'a> {
        Box::pin(async {
            let engine: Box<dyn GroupEngine> = Box::new(FailingEngine);
            Ok(engine)
        })
    }
}

struct FailingEngine;

impl GroupEngine for FailingEngine {
    fn create_stream<'a>(
        &'a mut self,
        _request: CreateStreamRequest,
        _placement: ShardPlacement,
    ) -> GroupCreateStreamFuture<'a> {
        Box::pin(async { Err(GroupEngineError::new("proposal rejected")) })
    }

    fn head_stream<'a>(
        &'a mut self,
        _request: HeadStreamRequest,
        _placement: ShardPlacement,
    ) -> GroupHeadStreamFuture<'a> {
        Box::pin(async { Err(GroupEngineError::new("proposal rejected")) })
    }

    fn read_stream<'a>(
        &'a mut self,
        _request: ReadStreamRequest,
        _placement: ShardPlacement,
    ) -> GroupReadStreamFuture<'a> {
        Box::pin(async { Err(GroupEngineError::new("proposal rejected")) })
    }

    fn touch_stream_access<'a>(
        &'a mut self,
        _stream_id: BucketStreamId,
        _now_ms: u64,
        _renew_ttl: bool,
        _placement: ShardPlacement,
    ) -> GroupTouchStreamAccessFuture<'a> {
        Box::pin(async { Err(GroupEngineError::new("proposal rejected")) })
    }

    fn add_fork_ref<'a>(
        &'a mut self,
        _stream_id: BucketStreamId,
        _now_ms: u64,
        _placement: ShardPlacement,
    ) -> GroupForkRefFuture<'a> {
        Box::pin(async { Err(GroupEngineError::new("proposal rejected")) })
    }

    fn release_fork_ref<'a>(
        &'a mut self,
        _stream_id: BucketStreamId,
        _placement: ShardPlacement,
    ) -> GroupForkRefFuture<'a> {
        Box::pin(async { Err(GroupEngineError::new("proposal rejected")) })
    }

    fn close_stream<'a>(
        &'a mut self,
        _request: CloseStreamRequest,
        _placement: ShardPlacement,
    ) -> GroupCloseStreamFuture<'a> {
        Box::pin(async { Err(GroupEngineError::new("proposal rejected")) })
    }

    fn delete_stream<'a>(
        &'a mut self,
        _request: DeleteStreamRequest,
        _placement: ShardPlacement,
    ) -> GroupDeleteStreamFuture<'a> {
        Box::pin(async { Err(GroupEngineError::new("proposal rejected")) })
    }

    fn append<'a>(
        &'a mut self,
        _request: AppendRequest,
        _placement: ShardPlacement,
    ) -> GroupAppendFuture<'a> {
        Box::pin(async { Err(GroupEngineError::new("proposal rejected")) })
    }

    fn append_batch<'a>(
        &'a mut self,
        _request: AppendBatchRequest,
        _placement: ShardPlacement,
    ) -> GroupAppendBatchFuture<'a> {
        Box::pin(async { Err(GroupEngineError::new("proposal rejected")) })
    }

    fn snapshot<'a>(&'a mut self, _placement: ShardPlacement) -> GroupSnapshotFuture<'a> {
        Box::pin(async { Err(GroupEngineError::new("proposal rejected")) })
    }

    fn install_snapshot<'a>(
        &'a mut self,
        _snapshot: GroupSnapshot,
    ) -> GroupInstallSnapshotFuture<'a> {
        Box::pin(async { Err(GroupEngineError::new("proposal rejected")) })
    }
}

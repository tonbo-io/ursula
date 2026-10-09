//! Admission must not clone the existing group payload. Run alone so dhat's
//! process-wide counters cannot include allocations from concurrent tests.

use bytes::Bytes;
use ursula_runtime::AppendRequest;
use ursula_runtime::ColdWriteAdmission;
use ursula_runtime::CreateStreamRequest;
use ursula_runtime::GroupEngine;
use ursula_runtime::InMemoryGroupEngine;
use ursula_shard::BucketStreamId;
use ursula_shard::CoreId;
use ursula_shard::RaftGroupId;
use ursula_shard::ShardId;
use ursula_shard::ShardPlacement;

#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

fn allocated_by<T>(work: impl FnOnce() -> T) -> (T, u64) {
    let before = dhat::HeapStats::get().total_bytes;
    let value = work();
    (
        value,
        dhat::HeapStats::get().total_bytes.saturating_sub(before),
    )
}

fn placement() -> ShardPlacement {
    ShardPlacement {
        core_id: CoreId(0),
        shard_id: ShardId(0),
        raft_group_id: RaftGroupId(0),
    }
}

#[test]
fn admission_preview_does_not_copy_group_state() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("runtime");
    let _profiler = dhat::Profiler::builder().testing().build();
    // Measured before F9: every create, append and batch cloned the whole
    // engine to preview admission (17.7 s for 30k appends).
    let stream_id = BucketStreamId::new("hygiene", "admission");
    let mut engine = InMemoryGroupEngine::default();
    let mut request = CreateStreamRequest::new(stream_id.clone(), "application/octet-stream");
    request.initial_payload = Bytes::from(vec![7u8; 4 << 20]);
    runtime
        .block_on(engine.create_stream(request, placement(), ColdWriteAdmission::default()))
        .expect("create");
    let admission = ColdWriteAdmission {
        max_hot_bytes_per_group: Some(1 << 30),
    };
    let request = AppendRequest::from_bytes(stream_id.clone(), b"x".to_vec());
    let (response, bytes) =
        allocated_by(|| runtime.block_on(engine.append(request, placement(), admission)));
    assert_eq!(response.expect("append").next_offset, (4 << 20) + 1);
    dhat::assert!(bytes < 64 * 1024, "admitted append allocated {bytes} bytes");

    let mut create = CreateStreamRequest::new(
        BucketStreamId::new("hygiene", "second"),
        "application/octet-stream",
    );
    create.initial_payload = Bytes::from_static(b"y");
    let (response, bytes) =
        allocated_by(|| runtime.block_on(engine.create_stream(create, placement(), admission)));
    response.expect("create");
    dhat::assert!(bytes < 64 * 1024, "admitted create allocated {bytes} bytes");
}

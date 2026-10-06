//! Engine bookkeeping (F9): per-stream append counts die with the stream on
//! every removal path, and the admission preview does not copy group state.

use std::alloc::GlobalAlloc;
use std::alloc::Layout;
use std::alloc::System;
use std::cell::Cell;

use bytes::Bytes;
use ursula_shard::BucketStreamId;
use ursula_shard::CoreId;
use ursula_shard::RaftGroupId;
use ursula_shard::ShardId;
use ursula_shard::ShardPlacement;
use ursula_stream::StreamCommand;

use super::in_memory::InMemoryGroupEngine;
use crate::command::GroupWriteCommand;
use crate::request::AppendRequest;
use crate::request::ColdWriteAdmission;
use crate::request::CreateStreamRequest;

/// Passes through to the system allocator and counts bytes allocated by the
/// current thread, so a test can measure one call without seeing other tests.
struct ThreadCountingAllocator;

thread_local! {
    static ALLOCATED: Cell<usize> = const { Cell::new(0) };
}

#[expect(
    unsafe_code,
    reason = "a counting global allocator must implement the unsafe GlobalAlloc trait"
)]
// SAFETY: every call forwards to `System` unchanged; the thread-local counter
// is const-initialized and never allocates.
unsafe impl GlobalAlloc for ThreadCountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        #[expect(
            clippy::let_underscore_must_use,
            reason = "the allocator cannot report a thread-local torn down at thread exit"
        )]
        let _ = ALLOCATED.try_with(|bytes| bytes.set(bytes.get().wrapping_add(layout.size())));
        // SAFETY: forwarded verbatim.
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: forwarded verbatim.
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        #[expect(
            clippy::let_underscore_must_use,
            reason = "the allocator cannot report a thread-local torn down at thread exit"
        )]
        let _ = ALLOCATED.try_with(|bytes| bytes.set(bytes.get().wrapping_add(new_size)));
        // SAFETY: forwarded verbatim.
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static GLOBAL: ThreadCountingAllocator = ThreadCountingAllocator;

fn allocated_by<T>(work: impl FnOnce() -> T) -> (T, usize) {
    let before = ALLOCATED.with(Cell::get);
    let value = work();
    let after = ALLOCATED.with(Cell::get);
    (value, after.wrapping_sub(before))
}

fn placement() -> ShardPlacement {
    ShardPlacement {
        core_id: CoreId(0),
        shard_id: ShardId(0),
        raft_group_id: RaftGroupId(0),
    }
}

fn create(
    engine: &mut InMemoryGroupEngine,
    stream_id: &BucketStreamId,
    ttl: Option<u64>,
    now_ms: u64,
) {
    let mut request = CreateStreamRequest::new(stream_id.clone(), "application/octet-stream");
    request.stream_ttl_seconds = ttl;
    request.now_ms = now_ms;
    engine
        .apply_committed_write(GroupWriteCommand::from(request), placement())
        .expect("create stream");
}

fn append(engine: &mut InMemoryGroupEngine, stream_id: &BucketStreamId, now_ms: u64) -> u64 {
    let mut request = AppendRequest::from_bytes(stream_id.clone(), b"x".to_vec());
    request.now_ms = now_ms;
    engine
        .append_with_admission_inner(request, placement(), ColdWriteAdmission::default())
        .expect("append")
        .stream_append_count
}

#[test]
fn ttl_expiry_drops_append_count_and_recreate_matches_installed_replica() {
    let stream_id = BucketStreamId::new("hygiene", "ttl");
    let mut long_lived = InMemoryGroupEngine::default();
    create(&mut long_lived, &stream_id, Some(1), 0);
    assert_eq!(append(&mut long_lived, &stream_id, 1), 1);
    assert_eq!(append(&mut long_lived, &stream_id, 2), 2);
    long_lived
        .apply_committed_write(
            GroupWriteCommand::Stream(StreamCommand::TouchStreamAccess {
                stream_id: stream_id.clone(),
                now_ms: 10_000,
                renew_ttl: true,
            }),
            placement(),
        )
        .expect("expire stream");

    let mut installed = InMemoryGroupEngine::default();
    installed
        .install_snapshot_inner(long_lived.build_snapshot(placement()))
        .expect("install snapshot");

    create(&mut long_lived, &stream_id, None, 10_001);
    create(&mut installed, &stream_id, None, 10_001);
    // Before F9 the long-lived replica continued the expired incarnation's
    // count (3) while the installed replica started over (1).
    assert_eq!(append(&mut long_lived, &stream_id, 10_002), 1);
    assert_eq!(append(&mut installed, &stream_id, 10_002), 1);
}

#[test]
fn purge_bucket_drops_append_counts() {
    let mut engine = InMemoryGroupEngine::default();
    for index in 0..8 {
        let stream_id = BucketStreamId::new("purged", format!("s{index}"));
        create(&mut engine, &stream_id, None, 0);
        append(&mut engine, &stream_id, 1);
    }
    engine
        .apply_committed_write(
            GroupWriteCommand::Stream(StreamCommand::PurgeBucket {
                bucket_id: "purged".to_owned(),
            }),
            placement(),
        )
        .expect("purge bucket");
    assert_eq!(engine.tracked_append_count_entries(), 0);
    assert!(
        engine
            .build_snapshot(placement())
            .stream_append_counts
            .is_empty()
    );
}

#[test]
fn admission_preview_does_not_copy_group_state() {
    // Measured before F9: every create, append and batch cloned the whole
    // engine to preview admission (17.7 s for 30k appends).
    let stream_id = BucketStreamId::new("hygiene", "admission");
    let mut engine = InMemoryGroupEngine::default();
    let mut request = CreateStreamRequest::new(stream_id.clone(), "application/octet-stream");
    request.initial_payload = Bytes::from(vec![7u8; 4 << 20]);
    engine
        .apply_committed_write(GroupWriteCommand::from(request), placement())
        .expect("create");
    let admission = ColdWriteAdmission {
        max_hot_bytes_per_group: Some(1 << 30),
    };
    let request = AppendRequest::from_bytes(stream_id.clone(), b"x".to_vec());
    let (response, bytes) =
        allocated_by(|| engine.append_with_admission_inner(request, placement(), admission));
    assert_eq!(response.expect("append").next_offset, (4 << 20) + 1);
    assert!(bytes < 64 * 1024, "admitted append allocated {bytes} bytes");

    let mut create = CreateStreamRequest::new(
        BucketStreamId::new("hygiene", "second"),
        "application/octet-stream",
    );
    create.initial_payload = Bytes::from_static(b"y");
    let (response, bytes) =
        allocated_by(|| engine.create_stream_with_admission_inner(create, placement(), admission));
    response.expect("create");
    assert!(bytes < 64 * 1024, "admitted create allocated {bytes} bytes");
}

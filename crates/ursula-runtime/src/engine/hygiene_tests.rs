//! Engine bookkeeping (F9): per-stream append counts, which apply keeps in
//! each version's usage, die with the stream on every removal path, and the
//! admission preview does not copy group state.

use ursula_shard::BucketStreamId;
use ursula_shard::CoreId;
use ursula_shard::RaftGroupId;
use ursula_shard::ShardId;
use ursula_shard::ShardPlacement;
use ursula_stream::StreamCommand;

use super::GroupWriteResponse;
use super::in_memory::InMemoryGroupEngine;
use crate::request::AppendRequest;
use crate::request::ColdWriteAdmission;
use crate::request::CreateStreamRequest;
use crate::request::PurgeBucketResponse;

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
        .apply_committed_write(StreamCommand::from(request), placement())
        .expect("create stream");
}

/// Appends one byte and answers the version's applied appends (F9).
fn append(engine: &mut InMemoryGroupEngine, stream_id: &BucketStreamId, now_ms: u64) -> u64 {
    let mut request = AppendRequest::from_bytes(stream_id.clone(), b"x".to_vec());
    request.now_ms = now_ms;
    engine
        .append_with_admission_inner(request, placement(), ColdWriteAdmission::default())
        .expect("append");
    engine
        .state_machine
        .version_usage(stream_id)
        .map_or(0, |usage| usage.counters.appends)
}

#[test]
fn ttl_expiry_drops_append_count_and_recreate_matches_installed_replica() {
    let stream_id = BucketStreamId::new("hygiene", "ttl");
    let mut long_lived = InMemoryGroupEngine::started();
    create(&mut long_lived, &stream_id, Some(1), 0);
    assert_eq!(append(&mut long_lived, &stream_id, 1), 1);
    assert_eq!(append(&mut long_lived, &stream_id, 2), 2);
    long_lived
        .apply_committed_write(
            StreamCommand::TouchStreamAccess {
                stream_id: stream_id.clone(),
                now_ms: 10_000,
                renew_ttl: true,
            },
            placement(),
        )
        .expect("expire stream");

    let mut installed = InMemoryGroupEngine::started();
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
    let mut engine = InMemoryGroupEngine::started();
    for index in 0..8 {
        let stream_id = BucketStreamId::new("purged", format!("s{index}"));
        create(&mut engine, &stream_id, None, 0);
        append(&mut engine, &stream_id, 1);
    }
    engine
        .apply_committed_write(
            StreamCommand::PurgeBucket {
                bucket_id: "purged".to_owned(),
                now_ms: 2,
            },
            placement(),
        )
        .expect("purge bucket");
    // Every count left with its slot, into the stream's end record (F20).
    let usage = engine.build_snapshot(placement()).usage;
    assert!(usage.streams.is_empty());
    assert_eq!(usage.ends.len(), 8);
    assert!(usage.ends.iter().all(|record| record.counters.appends == 1));
}

#[test]
fn purge_bucket_answers_its_log_index() {
    let mut engine = InMemoryGroupEngine::started();
    let mut request = CreateStreamRequest::new(
        BucketStreamId::new("purged", "s0"),
        "application/octet-stream",
    );
    request.now_ms = 1;
    // The indexes skipped stand for blank and membership entries, which never
    // reach the engine.
    engine
        .apply_committed_entry(StreamCommand::from(request), 4, placement())
        .expect("create stream");
    let purged = engine
        .apply_committed_entry(
            StreamCommand::PurgeBucket {
                bucket_id: "purged".to_owned(),
                now_ms: 2,
            },
            6,
            placement(),
        )
        .expect("purge bucket");
    // Rule 12 compares the answer with the export's `applied.index`, a log
    // index, not with the count of applied writes.
    assert!(matches!(
        purged,
        GroupWriteResponse::PurgeBucket(PurgeBucketResponse {
            removed_streams: 1,
            entry_index: 6,
            ..
        })
    ));
}

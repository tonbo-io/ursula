//! Engine bookkeeping (F9): per-stream append counts die with the stream on
//! every removal path, and the admission preview does not copy group state.

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

//! Native disk-WAL drill: isolate poison application and replay the intact record.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use openraft::rt::WatchReceiver;
use openraft::storage::RaftLogReader;
use ursula_runtime::GroupWriteCommand;
use ursula_shard::CoreId;
use ursula_shard::RaftGroupId;
use ursula_shard::ShardId;
use ursula_shard::ShardPlacement;

use crate::RaftGroupEngine;
use crate::RaftGroupEngineOptions;
use crate::RaftGroupHandleRegistry;

fn placement(group: u32) -> ShardPlacement {
    ShardPlacement {
        core_id: CoreId(0),
        shard_id: ShardId(group),
        raft_group_id: RaftGroupId(group),
    }
}

fn create_stream(name: &str) -> GroupWriteCommand {
    GroupWriteCommand::Stream(ursula_stream::StreamCommand::CreateStream {
        stream_id: ursula_shard::BucketStreamId::new(name, "events"),
        content_type: "application/octet-stream".to_owned(),
        initial_payload: bytes::Bytes::copy_from_slice(name.as_bytes()),
        close_after: false,
        stream_seq: None,
        producer: None,
        stream_ttl_seconds: None,
        stream_expires_at_ms: None,
        now_ms: 0,
    })
}

#[tokio::test]
async fn poison_apply_isolates_one_group_and_corrected_code_replays_the_intact_wal() {
    let directory = tempfile::tempdir().unwrap();
    let topology = ursula_shard::StaticShardMap::new(1, 2).unwrap();
    let metrics = ursula_runtime::RuntimeMetrics::new(1, 2);
    let config = Arc::new(
        openraft::Config {
            snapshot_policy: openraft::SnapshotPolicy::Never,
            ..Default::default()
        }
        .validate()
        .unwrap(),
    );
    let wal = crate::log_store::RaftWal::start(
        directory.path(),
        ursula_config::WalFsync::Always,
        &topology,
    )
    .unwrap();
    let mut poisoned_log = wal
        .open(placement(0), metrics.group_engine_metrics())
        .unwrap();
    let poisoned = RaftGroupEngine::new_single_node(
        placement(0),
        1,
        openraft::BasicNode::new("local"),
        config.clone(),
        poisoned_log.clone(),
        RaftGroupEngineOptions::default(),
    )
    .await
    .unwrap();
    let healthy = RaftGroupEngine::new_single_node(
        placement(1),
        1,
        openraft::BasicNode::new("local"),
        config.clone(),
        wal.open(placement(1), metrics.group_engine_metrics())
            .unwrap(),
        RaftGroupEngineOptions::default(),
    )
    .await
    .unwrap();
    let registry = RaftGroupHandleRegistry::default();
    registry.register_engine(&poisoned, None);
    registry.register_engine(&healthy, None);
    poisoned
        .raft
        .client_write(create_stream("before-poison"))
        .await
        .unwrap();
    let previous = poisoned
        .raft
        .metrics()
        .borrow_watched()
        .last_applied
        .unwrap();
    let poison_index = previous.index().checked_add(1).unwrap();
    poisoned
        .with_state_machine(move |state| {
            Box::pin(async move {
                state.fail_apply_at = Some(poison_index);
            })
        })
        .await
        .unwrap();
    let result = crate::rt::time::timeout(
        Duration::from_secs(5),
        poisoned.raft.client_write(create_stream("poison-record")),
    )
    .await
    .unwrap();
    result.expect_err("poison command must not receive a success response");
    healthy
        .raft
        .client_write(create_stream("healthy-after-failure"))
        .await
        .unwrap();
    let groups = registry.metrics_snapshot();
    let stopped = groups
        .iter()
        .find(|group| group.raft_group_id == 0)
        .unwrap();
    assert_eq!(stopped.apply_failure.as_ref().unwrap().index, poison_index);
    assert_eq!(
        stopped.apply_failure.as_ref().unwrap().kind,
        ursula_proto::admin::RaftApplyFailureKind::Panic
    );
    assert!(!stopped.maintenance.running);
    assert!(
        stopped
            .last_applied
            .is_none_or(|applied| applied.index < poison_index)
    );
    assert!(
        groups
            .iter()
            .find(|group| group.raft_group_id == 1)
            .unwrap()
            .maintenance
            .running
    );
    let report = crate::check_raft_maintenance(
        &groups,
        1,
        BTreeMap::from([(0, BTreeSet::from([1])), (1, BTreeSet::from([1]))]),
        0,
    );
    assert!(!report.ready());
    let retained = poisoned_log
        .try_get_log_entries(poison_index..=poison_index)
        .await
        .unwrap();
    assert_eq!(
        retained.len(),
        1,
        "failed committed record stays in the WAL"
    );
    poisoned.raft.shutdown().await.unwrap();
    healthy.raft.shutdown().await.unwrap();
    wal.shutdown().await.unwrap();
    drop(registry);
    drop(poisoned);
    drop(healthy);
    drop(poisoned_log);
    drop(wal);

    // Equivalent to deploying corrected code and restarting with the original
    // WAL directory. No skip marker, truncation, or snapshot substitution.
    let wal = crate::log_store::RaftWal::start(
        directory.path(),
        ursula_config::WalFsync::Always,
        &topology,
    )
    .unwrap();
    let recovered = RaftGroupEngine::new_single_node(
        placement(0),
        1,
        openraft::BasicNode::new("local"),
        config,
        wal.open(placement(0), metrics.group_engine_metrics())
            .unwrap(),
        RaftGroupEngineOptions::default(),
    )
    .await
    .unwrap();
    recovered
        .raft
        .client_write(create_stream("after-recovery"))
        .await
        .unwrap();
    let snapshot = recovered
        .with_state_machine(|state| Box::pin(async move { state.group_snapshot().await }))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(snapshot.stream_snapshot.buckets, vec![
        "after-recovery",
        "before-poison",
        "poison-record"
    ]);
    for name in ["before-poison", "poison-record", "after-recovery"] {
        let stream = snapshot
            .stream_snapshot
            .streams
            .iter()
            .find(|stream| {
                stream.metadata.stream_id == ursula_shard::BucketStreamId::new(name, "events")
            })
            .unwrap();
        assert_eq!(
            stream.payload,
            name.as_bytes(),
            "replay preserves every payload"
        );
    }
    assert!(recovered.apply_failure.lock().unwrap().is_none());
    recovered.raft.shutdown().await.unwrap();
    wal.shutdown().await.unwrap();
}

use std::collections::BTreeMap;
use std::fs;
use std::time::Duration;

use openraft::BasicNode;
use openraft::storage::RaftSnapshotBuilder;
use openraft::storage::RaftStateMachine;
use ursula_runtime::ShardRuntime;
use ursula_shard::RaftGroupId;

use super::create_stream_command;
use super::hosted_config;
use crate::StaticGrpcRaftGroupEngineFactory;
use crate::registry::RaftGroupHandleRegistry;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn owning_core_retirement_drains_work_cleans_disk_and_registry_and_keeps_neighbors_running() {
    let root = tempfile::tempdir().unwrap();
    let registry = RaftGroupHandleRegistry::default();
    registry.set_managed_hosting([RaftGroupId(0), RaftGroupId(1)].into());
    let factory = StaticGrpcRaftGroupEngineFactory::new(
        1,
        [(1, "http://node1:4440".to_owned())],
        false,
        registry.clone(),
    )
    .with_raft_log_dir(root.path());
    let runtime = ShardRuntime::spawn_with_engine_factory(hosted_config(1, 2), factory).unwrap();
    for group in [RaftGroupId(0), RaftGroupId(1)] {
        runtime.warm_group(group).await.unwrap();
    }
    // Single-voter native-kernel fixture; this is physical cleanup evidence,
    // not the supported RF3/RF5 migration acceptance.
    let removed = registry.get(RaftGroupId(0)).unwrap();
    let neighbor = registry.get(RaftGroupId(1)).unwrap();
    for raft in [&removed, &neighbor] {
        raft.initialize(BTreeMap::from([(1, BasicNode::new("http://node1:4440"))]))
            .await
            .unwrap();
        raft.wait(Some(Duration::from_secs(10)))
            .current_leader(1, "kernel fixture leader")
            .await
            .unwrap();
    }
    removed
        .client_write(create_stream_command("removed"))
        .await
        .unwrap();
    neighbor
        .client_write(create_stream_command("neighbor"))
        .await
        .unwrap();
    registry
        .build_snapshot_for_transfer(RaftGroupId(0))
        .await
        .unwrap();
    let metadata = root.path().join("core-0/group-0.snapshot.json");
    assert!(metadata.exists());
    assert!(
        runtime.retire_group_engine(RaftGroupId(0)).await.is_err(),
        "live hosting cannot be retired"
    );
    let old_references = registry.snapshot_install_coordinator().references(0);
    let mut queued_builder = removed
        .with_state_machine(|sm| Box::pin(async move { sm.get_snapshot_builder().await }))
        .await
        .unwrap();
    registry.set_managed_hosting([RaftGroupId(1)].into());
    let caller_runtime = runtime.clone();
    let cancelled =
        tokio::spawn(async move { caller_runtime.retire_group_engine(RaftGroupId(0)).await });
    tokio::time::timeout(Duration::from_secs(10), async {
        while !old_references.activity.is_closed() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let retry_runtime = runtime.clone();
    let retry =
        tokio::spawn(async move { retry_runtime.retire_group_engine(RaftGroupId(0)).await });
    cancelled.abort();
    let _ = cancelled.await;
    assert!(registry.get(RaftGroupId(0)).is_none());
    assert!(registry.read_barrier(RaftGroupId(0)).is_none());
    assert!(
        registry
            .build_snapshot_for_transfer(RaftGroupId(0))
            .await
            .is_err()
    );
    assert!(runtime.warm_group(RaftGroupId(0)).await.is_err());
    assert!(
        removed
            .client_write(create_stream_command("stopped"))
            .await
            .is_err()
    );
    tokio::time::timeout(Duration::from_secs(2), runtime.warm_group(RaftGroupId(1)))
        .await
        .unwrap()
        .unwrap();
    neighbor
        .client_write(create_stream_command("neighbor-after"))
        .await
        .unwrap();
    assert!(
        !retry.is_finished(),
        "duplicate retirement must wait for the same drain"
    );
    assert!(
        metadata.exists(),
        "metadata survives until snapshot work drains"
    );
    queued_builder.build_snapshot().await.unwrap();
    tokio::time::timeout(Duration::from_secs(10), retry)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(!metadata.exists());
    assert!(!metadata.with_extension("json.tmp").exists());
    assert!(!registry.contains_group(RaftGroupId(0)));
    assert!(registry.cold_index_cache(RaftGroupId(0)).is_none());
    assert!(
        !registry
            .snapshot_build_coordinator()
            .log_progress()
            .contains_key(&0)
    );
    let records = crate::log_store::read_wire_frames::<crate::log_store::CoreJournalRecord>(
        &fs::read(root.path().join("core-0/journal.bin")).unwrap(),
    )
    .unwrap();
    assert!(records.iter().all(|record| record.group_id == 1));
    let snapshot = neighbor
        .with_state_machine(|sm| Box::pin(async move { sm.group_snapshot().await }))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(snapshot.stream_snapshot.streams.len(), 2);
    registry.set_managed_hosting([RaftGroupId(0), RaftGroupId(1)].into());
    runtime.warm_group(RaftGroupId(0)).await.unwrap();
    let new = registry.get(RaftGroupId(0)).unwrap();
    assert!(
        !new.is_initialized().await.unwrap(),
        "prepare must not initialize membership"
    );
    assert!(
        queued_builder.build_snapshot().await.is_err(),
        "delayed old builder cannot recreate retired metadata"
    );
    assert!(!metadata.exists());
    assert!(old_references.activity.enter().is_err());
    assert!(
        registry
            .snapshot_install_coordinator()
            .references(0)
            .activity
            .enter()
            .is_ok()
    );
    assert!(
        removed
            .client_write(create_stream_command("old-owner"))
            .await
            .is_err()
    );
    registry.quiesce_for_restart().await.unwrap();
}

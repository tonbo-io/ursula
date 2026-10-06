use std::collections::BTreeMap;
use std::fs;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;

use openraft::BasicNode;
use openraft::storage::RaftSnapshotBuilder;
use openraft::storage::RaftStateMachine;
use ursula_runtime::ColdStore;
use ursula_runtime::ColdStoreEvent;
use ursula_runtime::ColdStoreFaultEffect;
use ursula_runtime::ColdStoreOperation;
use ursula_runtime::CreateStreamExternalRequest;
use ursula_runtime::CreateStreamRequest;
use ursula_runtime::DeleteStreamRequest;
use ursula_runtime::ExternalPayloadRef;
use ursula_runtime::PlanColdFlushRequest;
use ursula_runtime::ShardRuntime;
use ursula_shard::BucketStreamId;
use ursula_shard::RaftGroupId;

use super::create_stream_command;
use super::hosted_config;
use crate::StaticGrpcRaftGroupEngineFactory;
use crate::registry::RaftGroupHandleRegistry;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn retirement_waits_for_cancelled_cold_flush_after_planning() {
    cancelled_cold_work_retirement(ColdStoreOperation::WriteChunk).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn retirement_waits_for_cancelled_cold_gc_after_planning() {
    cancelled_cold_work_retirement(ColdStoreOperation::DeleteChunk).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn retirement_waits_for_cancelled_cold_read_materialization() {
    cancelled_cold_work_retirement(ColdStoreOperation::ReadObjectRange).await;
}

async fn cancelled_cold_work_retirement(operation: ColdStoreOperation) {
    let root = tempfile::tempdir().unwrap();
    let registry = RaftGroupHandleRegistry::default();
    registry.set_managed_hosting([RaftGroupId(0), RaftGroupId(1)].into());
    let cold = Arc::new(ColdStore::memory().unwrap());
    let factory = StaticGrpcRaftGroupEngineFactory::new(
        1,
        [(1, "http://node1:4440".to_owned())],
        false,
        registry.clone(),
    )
    .with_raft_log_dir(root.path())
    .with_cold_store(Some(cold.clone()));
    let runtime = ShardRuntime::spawn_with_engine_factory_and_cold_store(
        hosted_config(1, 2),
        factory,
        Some(cold.clone()),
    )
    .unwrap();
    for group in [RaftGroupId(0), RaftGroupId(1)] {
        runtime.warm_group(group).await.unwrap();
        let raft = registry.get(group).unwrap();
        raft.initialize(BTreeMap::from([(1, BasicNode::new("http://node1:4440"))]))
            .await
            .unwrap();
        raft.wait(Some(Duration::from_secs(10)))
            .current_leader(1, "cold kernel fixture")
            .await
            .unwrap();
    }
    let stream = (0..100)
        .map(|n| BucketStreamId::new("cold-retirement", format!("s{n}")))
        .find(|stream| runtime.locate(stream).raft_group_id == RaftGroupId(0))
        .unwrap();
    let mut create = CreateStreamRequest::new(stream.clone(), "application/octet-stream");
    create.initial_payload = vec![7; 32].into();
    if operation == ColdStoreOperation::ReadObjectRange {
        let path = ursula_runtime::new_external_payload_path(&stream);
        cold.write_chunk(&path, &[7; 32]).await.unwrap();
        runtime
            .create_stream_external(CreateStreamExternalRequest::from_create_request(
                create,
                ExternalPayloadRef {
                    s3_path: path,
                    payload_len: 32,
                    object_size: 32,
                },
                vec![],
            ))
            .await
            .unwrap();
    } else {
        runtime.create_stream(create).await.unwrap();
    }
    let flush = PlanColdFlushRequest {
        stream_id: stream.clone(),
        min_hot_bytes: 1,
        max_flush_bytes: 32,
    };
    if operation == ColdStoreOperation::DeleteChunk {
        runtime
            .flush_cold_once(flush.clone())
            .await
            .unwrap()
            .unwrap();
        runtime
            .delete_stream(DeleteStreamRequest {
                stream_id: stream.clone(),
                if_incarnation: None,
            })
            .await
            .unwrap();
    }
    registry
        .build_snapshot_for_transfer(RaftGroupId(0))
        .await
        .unwrap();
    let metadata = root.path().join("core-0/group-0.snapshot.json");
    let entered = Arc::new(tokio::sync::Semaphore::new(0));
    let resume = Arc::new(tokio::sync::Semaphore::new(0));
    let completed = Arc::new(AtomicUsize::new(0));
    let observe = completed.clone();
    cold.set_observer(move |event| {
        if matches!(
            (operation, event),
            (
                ColdStoreOperation::WriteChunk,
                ColdStoreEvent::WriteChunkComplete { .. }
            ) | (
                ColdStoreOperation::DeleteChunk,
                ColdStoreEvent::DeleteChunkComplete { .. }
            ) | (
                ColdStoreOperation::ReadObjectRange,
                ColdStoreEvent::ReadObjectRangeComplete { .. }
            )
        ) {
            observe.fetch_add(1, Ordering::SeqCst);
        }
    });
    let pauses = AtomicUsize::new(0);
    cold.set_fault_policy(move |context| {
        (context.operation == operation && pauses.fetch_add(1, Ordering::SeqCst) == 0)
            .then(|| ColdStoreFaultEffect::delay(Duration::from_secs(1)))
    });
    let enter = entered.clone();
    let release = resume.clone();
    cold.set_delay_fn(move |_| {
        let enter = enter.clone();
        let release = release.clone();
        async move {
            enter.add_permits(1);
            release.acquire().await.unwrap().forget();
        }
    });
    let work_runtime = runtime.clone();
    let caller = tokio::spawn(async move {
        if operation == ColdStoreOperation::DeleteChunk {
            work_runtime
                .run_cold_gc_group_once(RaftGroupId(0), 10)
                .await
                .map(|_| ())
        } else if operation == ColdStoreOperation::WriteChunk {
            work_runtime.flush_cold_once(flush).await.map(|_| ())
        } else {
            work_runtime
                .read_stream(super::read_req(stream, 32))
                .await
                .map(|_| ())
        }
    });
    tokio::time::timeout(Duration::from_secs(10), entered.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    let old_references = registry.snapshot_install_coordinator().references(0);
    registry.set_managed_hosting([RaftGroupId(1)].into());
    caller.abort();
    let _ = caller.await;
    let retire_runtime = runtime.clone();
    let retiring =
        tokio::spawn(async move { retire_runtime.retire_group_engine(RaftGroupId(0)).await });
    tokio::time::timeout(Duration::from_secs(10), async {
        while !old_references.activity.is_closed() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(
        !retiring.is_finished(),
        "cancelled cold I/O remains in the drain"
    );
    assert!(metadata.exists());
    assert_eq!(completed.load(Ordering::SeqCst), 0);
    assert!(runtime.enter_group_work(RaftGroupId(0)).is_err());
    registry
        .get(RaftGroupId(1))
        .unwrap()
        .client_write(create_stream_command("neighbor-during-cold-drain"))
        .await
        .unwrap();
    resume.add_permits(1);
    tokio::time::timeout(Duration::from_secs(10), retiring)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(completed.load(Ordering::SeqCst) >= 1);
    assert!(!metadata.exists());
    assert!(!registry.contains_group(RaftGroupId(0)));
    assert!(old_references.activity.enter().is_err());
    registry.quiesce_for_restart().await.unwrap();
}

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

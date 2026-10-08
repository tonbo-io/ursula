//! Native disk-WAL drill: isolate poison application and replay the intact record.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use openraft::rt::WatchReceiver;
use openraft::storage::RaftLogReader;
use openraft::vote::RaftLeaderId;
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
    crate::rt::time::timeout(
        Duration::from_secs(30),
        poison_apply_isolates_one_group_and_corrected_code_replays_the_intact_wal_drill(),
    )
    .await
    .expect("bounded apply recovery drill");
}

async fn poison_apply_isolates_one_group_and_corrected_code_replays_the_intact_wal_drill() {
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
        RaftGroupEngineOptions {
            apply_stop_signal: Some(poisoned_log.apply_stop_signal()),
            ..Default::default()
        },
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
    registry.register_engine(&poisoned);
    registry.register_engine(&healthy);
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
    let rejected = poisoned
        .write(GroupWriteCommand::Stream(
            ursula_stream::StreamCommand::CreateBucket {
                bucket_id: "not-a-data-command".to_owned(),
            },
        ))
        .await
        .unwrap_err();
    assert!(matches!(
        rejected,
        ursula_runtime::GroupEngineError::Infra(
            ursula_runtime::GroupInfraError::InvalidRaftCommand {
                command: ursula_runtime::UnproposableCommand::CreateBucket
            }
        )
    ));
    assert_eq!(
        poisoned.raft.metrics().borrow_watched().last_applied,
        Some(previous)
    );

    poisoned
        .with_state_machine(move |state| {
            Box::pin(async move {
                state.apply_fault = Some(crate::apply_failure::ApplyFault::PanicAfterMutation {
                    index: poison_index,
                });
            })
        })
        .await
        .unwrap();
    let result = crate::rt::time::timeout(
        Duration::from_secs(5),
        poisoned.write(create_stream("poison-record")),
    )
    .await
    .unwrap();
    // The stopped command is committed and applies after repair.
    assert!(matches!(
        result,
        Err(ursula_runtime::GroupEngineError::Infra(
            ursula_runtime::GroupInfraError::OutcomeUnknown
        ))
    ));
    // A later command is refused before proposal and names the failure.
    assert!(matches!(
        poisoned.write(create_stream("after-stop")).await,
        Err(ursula_runtime::GroupEngineError::Infra(
            ursula_runtime::GroupInfraError::ApplyStopped { index, .. }
        )) if index == poison_index
    ));
    assert!(
        poisoned_log
            .apply_stop_signal()
            .load(std::sync::atomic::Ordering::Acquire)
    );
    healthy
        .raft
        .client_write(create_stream("healthy-after-failure"))
        .await
        .unwrap();
    let healthy_snapshot = healthy
        .with_state_machine(|state| Box::pin(async move { state.group_snapshot().await }))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        healthy_snapshot.stream_snapshot.streams[0].payload,
        b"healthy-after-failure"
    );
    assert!(matches!(
        poisoned
            .with_state_machine(|state| Box::pin(async move { state.group_snapshot().await }))
            .await
            .unwrap_err(),
        ursula_runtime::GroupEngineError::Infra(
            ursula_runtime::GroupInfraError::ApplyStopped { .. }
        )
    ));
    let groups = registry.metrics_snapshot();
    let stopped = groups
        .iter()
        .find(|group| group.raft_group_id == 0)
        .unwrap();
    assert_eq!(stopped.apply_failure.as_ref().unwrap().index, poison_index);
    assert_eq!(
        stopped.apply_failure.as_ref().unwrap().kind,
        crate::apply_failure::ApplyFailureKind::Panic
    );
    assert!(!stopped.maintenance.running);
    // Its last observed leadership is stale: balancing and stall recovery
    // must not count it.
    assert_eq!(stopped.current_leader, None);
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

    // Restarting the same faulty binary replays and stops on the SAME retained entry.
    let wal = crate::log_store::RaftWal::start(
        directory.path(),
        ursula_config::WalFsync::Always,
        &topology,
    )
    .unwrap();
    let repeated = RaftGroupEngine::new_node(
        placement(0),
        1,
        config.clone(),
        crate::registry::SingleNodeRaftNetworkFactory,
        wal.open(placement(0), metrics.group_engine_metrics())
            .unwrap(),
        RaftGroupEngineOptions {
            apply_fault: Some(crate::apply_failure::ApplyFault::PanicAfterMutation {
                index: poison_index,
            }),
            ..Default::default()
        },
    )
    .await;
    assert!(matches!(
        repeated,
        Err(ursula_runtime::GroupEngineError::Infra(
            ursula_runtime::GroupInfraError::ApplyStopped { .. }
        ))
    ));
    let retained_again = wal
        .open(placement(0), metrics.group_engine_metrics())
        .unwrap()
        .try_get_log_entries(poison_index..=poison_index)
        .await
        .unwrap();
    assert_eq!(retained_again.len(), 1);
    wal.shutdown().await.unwrap();
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
    assert!(recovered.apply_health.failure().is_none());
    recovered.raft.shutdown().await.unwrap();
    wal.shutdown().await.unwrap();
}

#[tokio::test]
async fn partially_mutated_apply_does_not_advance_and_cannot_build_a_snapshot() {
    use openraft::storage::RaftSnapshotBuilder;
    use openraft::storage::RaftStateMachine;
    use ursula_runtime::GroupEngine;
    use ursula_runtime::GroupEngineError;
    use ursula_runtime::GroupInfraError;
    let mut state = crate::RaftGroupStateMachine::new(placement(0));
    state.apply_fault = Some(crate::apply_failure::ApplyFault::PanicAfterMutation { index: 1 });
    let entry = openraft::alias::EntryOf::<crate::UrsulaRaftTypeConfig> {
        log_id: openraft::LogId::new(
            openraft::vote::leader_id_adv::CommittedLeaderId::new(1, 1),
            1,
        ),
        payload: openraft::EntryPayload::Normal(create_stream("partially-mutated")),
    };
    let error = state
        .apply(futures_util::stream::iter([Ok((entry.clone(), None))]))
        .await
        .unwrap_err();
    assert!(
        error
            .get_ref()
            .unwrap()
            .downcast_ref::<crate::apply_failure::ApplyError>()
            .is_some()
    );
    // The injection follows real mutation; the test does not model an atomic failure.
    assert_eq!(
        state
            .engine
            .snapshot(placement(0))
            .await
            .unwrap()
            .stream_snapshot
            .streams
            .len(),
        1
    );
    assert_eq!(state.last_applied_log_id, None);
    let mut builder = state.get_snapshot_builder().await;
    assert!(matches!(
        builder
            .build_snapshot()
            .await
            .unwrap_err()
            .get_ref()
            .unwrap()
            .downcast_ref::<GroupEngineError>(),
        Some(GroupEngineError::Infra(
            GroupInfraError::ApplyStopped { .. }
        ))
    ));
    assert!(state.current_snapshot.lock().unwrap().is_none());
}

#[tokio::test]
async fn invariant_failure_is_fatal_but_business_rejection_is_applied() {
    use openraft::storage::RaftStateMachine;
    let mut state = crate::RaftGroupStateMachine::new(placement(0));
    let entry = |index, command| openraft::alias::EntryOf::<crate::UrsulaRaftTypeConfig> {
        log_id: openraft::LogId::new(
            openraft::vote::leader_id_adv::CommittedLeaderId::new(1, 1),
            index,
        ),
        payload: openraft::EntryPayload::Normal(command),
    };
    let rejected = GroupWriteCommand::Stream(ursula_stream::StreamCommand::DeleteStream {
        stream_id: ursula_shard::BucketStreamId::new("absent", "absent"),
    });
    let result = state
        .engine
        .apply_committed_write(rejected.clone(), placement(0));
    assert!(matches!(
        result,
        Err(ursula_runtime::GroupEngineError::Stream(_))
    ));
    state
        .apply(futures_util::stream::iter([Ok((entry(1, rejected), None))]))
        .await
        .unwrap();
    assert_eq!(state.last_applied_log_id.unwrap().index(), 1);
    assert!(state.apply_health.failure().is_none());
    state.apply_fault = Some(crate::apply_failure::ApplyFault::InvariantAfterMutation { index: 2 });
    let error = state
        .apply(futures_util::stream::iter([Ok((
            entry(2, create_stream("infra-poison")),
            None,
        ))]))
        .await
        .unwrap_err();
    assert!(matches!(
        error
            .get_ref()
            .unwrap()
            .downcast_ref::<crate::apply_failure::ApplyError>(),
        Some(crate::apply_failure::ApplyError::InvariantViolation(
            ursula_runtime::GroupEngineError::Infra(
                ursula_runtime::GroupInfraError::ProtoDecode { .. }
            )
        ))
    ));
    assert_eq!(state.last_applied_log_id.unwrap().index(), 1);
    assert_eq!(
        state.apply_health.failure().unwrap().kind,
        crate::apply_failure::ApplyFailureKind::InvariantViolation
    );
}

#[tokio::test]
async fn three_replica_poison_drill_replays_every_payload_without_skipping() {
    crate::rt::time::timeout(
        Duration::from_secs(30),
        three_replica_poison_drill_replays_every_payload_without_skipping_drill(),
    )
    .await
    .expect("bounded apply recovery drill");
}

async fn three_replica_poison_drill_replays_every_payload_without_skipping_drill() {
    let roots = [
        tempfile::tempdir().unwrap(),
        tempfile::tempdir().unwrap(),
        tempfile::tempdir().unwrap(),
    ];
    let topology = ursula_shard::StaticShardMap::new(1, 1).unwrap();
    let metrics = ursula_runtime::RuntimeMetrics::new(1, 1);
    let config = Arc::new(
        openraft::Config {
            snapshot_policy: openraft::SnapshotPolicy::Never,
            ..Default::default()
        }
        .validate()
        .unwrap(),
    );
    let mut poison_index = None;
    for corrected in [false, true] {
        let network = crate::in_process::InProcessRaftRegistry::default();
        let mut wals = Vec::new();
        let mut engines = Vec::new();
        let mut logs = Vec::new();
        for (offset, root) in roots.iter().enumerate() {
            let id = u64::try_from(offset).unwrap().checked_add(1).unwrap();
            let wal = crate::log_store::RaftWal::start(
                root.path(),
                ursula_config::WalFsync::Always,
                &topology,
            )
            .unwrap();
            let log = wal
                .open(placement(0), metrics.group_engine_metrics())
                .unwrap();
            let engine = RaftGroupEngine::new_node(
                placement(0),
                id,
                config.clone(),
                crate::in_process::InProcessRaftNetworkFactory::new(network.clone())
                    .with_source(id),
                log.clone(),
                RaftGroupEngineOptions::default(),
            )
            .await
            .unwrap();
            network.register(id, &engine);
            logs.push(log);
            wals.push(wal);
            engines.push(engine);
        }
        if !corrected {
            engines[0]
                .initialize_membership(BTreeMap::from([
                    (1, openraft::BasicNode::new("one")),
                    (2, openraft::BasicNode::new("two")),
                    (3, openraft::BasicNode::new("three")),
                ]))
                .await
                .unwrap();
        }
        let index = crate::rt::time::timeout(Duration::from_secs(5), async {
            let mut changes = engines[0].raft.metrics();
            loop {
                for (index, engine) in engines.iter().enumerate() {
                    if engine.raft.metrics().borrow_watched().state != openraft::ServerState::Leader
                    {
                        continue;
                    }
                    match engine
                        .raft
                        .ensure_linearizable(openraft::ReadPolicy::ReadIndex)
                        .await
                    {
                        Ok(_) => return index,
                        Err(openraft::error::RaftError::APIError(
                            openraft::error::LinearizableReadError::QuorumNotEnough(_)
                            | openraft::error::LinearizableReadError::ForwardToLeader(_),
                        )) => {}
                        Err(error) => panic!("unexpected readiness failure: {error}"),
                    }
                }
                changes.changed().await.unwrap();
            }
        })
        .await
        .expect("elected leader establishes a fresh quorum before the fault");
        let leader = &engines[index];
        if !corrected {
            leader
                .raft
                .client_write(create_stream("acknowledged-before-poison"))
                .await
                .unwrap();
            let previous = leader.raft.metrics().borrow_watched().last_applied.unwrap();
            let next = previous.index().checked_add(1).unwrap();
            poison_index = Some(next);
            for engine in &engines {
                engine
                    .with_state_machine(move |state| {
                        Box::pin(async move {
                            state.apply_fault =
                                Some(crate::apply_failure::ApplyFault::PanicAfterMutation {
                                    index: next,
                                });
                        })
                    })
                    .await
                    .unwrap();
            }
            let result = crate::rt::time::timeout(
                Duration::from_secs(5),
                leader.raft.client_write(create_stream("poison-payload")),
            )
            .await
            .unwrap();
            assert!(matches!(result, Err(openraft::error::RaftError::Fatal(_))));
            // A surviving follower must elect a new leader and learn the
            // committed poison through Raft replication, not a test-delivered ACK.
            let waits = engines
                .iter()
                .enumerate()
                .filter(|(other, _)| *other != index)
                .map(|(_, engine)| {
                    Box::pin(async move {
                        engine
                            .raft
                            .wait(Some(Duration::from_secs(5)))
                            .metrics(
                                |m| m.running_state.is_err(),
                                "re-election applies committed poison",
                            )
                            .await
                    })
                })
                .collect::<Vec<_>>();
            futures_util::future::select_all(waits).await.0.unwrap();
            assert!(
                engines
                    .iter()
                    .filter(|engine| engine.apply_health.failure().is_some())
                    .count()
                    >= 2
            );
            for engine in &engines {
                if let Some(failure) = engine.apply_health.failure() {
                    assert_eq!(failure.index, next);
                    assert!(
                        engine
                            .raft
                            .metrics()
                            .borrow_watched()
                            .last_applied
                            .is_none_or(|id| id.index() < next)
                    );
                }
            }
        } else {
            let response = leader
                .raft
                .client_write(create_stream("acknowledged-after-repair"))
                .await
                .unwrap();
            for engine in &engines {
                engine
                    .raft
                    .wait(Some(Duration::from_secs(5)))
                    .metrics(
                        |m| {
                            m.last_applied
                                .is_some_and(|id| id.index() >= response.log_id.index())
                        },
                        "corrected replay and catch-up",
                    )
                    .await
                    .unwrap();
                let snapshot = engine
                    .with_state_machine(|state| {
                        Box::pin(async move { state.group_snapshot().await })
                    })
                    .await
                    .unwrap()
                    .unwrap();
                for name in [
                    "acknowledged-before-poison",
                    "poison-payload",
                    "acknowledged-after-repair",
                ] {
                    let stream = snapshot
                        .stream_snapshot
                        .streams
                        .iter()
                        .find(|stream| {
                            stream.metadata.stream_id
                                == ursula_shard::BucketStreamId::new(name, "events")
                        })
                        .unwrap();
                    assert_eq!(stream.payload, name.as_bytes());
                }
                assert!(engine.apply_health.failure().is_none());
                assert!(
                    engine
                        .raft
                        .metrics()
                        .borrow_watched()
                        .last_applied
                        .unwrap()
                        .index()
                        > poison_index.unwrap()
                );
            }
        }
        for engine in &engines {
            engine.shutdown().await.unwrap();
        }
        for wal in &wals {
            wal.shutdown().await.unwrap();
        }
        drop(network);
        drop(engines);
        drop(logs);
        drop(wals);
    }
}

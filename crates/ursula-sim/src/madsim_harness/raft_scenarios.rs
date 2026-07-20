//! Raft-level scenarios (no-fault baseline, partition/heal, snapshot,
//! restart-follower, leader-failover) extracted from `madsim_harness/mod.rs`
//! (DoD #3 modularity refactor — workloads axis).

#[cfg(test)]
use openraft::storage::RaftLogStorage;

use super::AppendRequest;
use super::BasicNode;
use super::ColdWriteAdmission;
use super::CreateStreamRequest;
use super::Duration;
use super::GroupEngine;
#[cfg(test)]
use super::GroupWriteCommand;
use super::InProcessRaftFaultAction;
use super::InProcessRaftFaultScript;
use super::InProcessRaftNetworkFactory;
use super::RaftGroupEngine;
use super::SimEvent;
use super::SimTrace;
use super::ThreeNodeRaftSimConfig;
use super::ThreeNodeRaftSimOutcome;
use super::build_lagging_learner_snapshot_cluster;
use super::build_restartable_three_node_cluster;
use super::build_three_node_cluster;
#[cfg(test)]
use super::build_three_node_snapshot_purge_cluster;
use super::placement;
use super::read_local_payload_eventually;
use super::seeded_follower_id;
use super::sim_network_policy;
use super::verify_all_nodes_can_read;
use super::wait_all_nodes_applied;

pub(super) async fn run_no_fault_inner(config: ThreeNodeRaftSimConfig) -> ThreeNodeRaftSimOutcome {
    let mut trace = SimTrace::default();
    let policy = sim_network_policy();
    let (_registry, mut engines, leader_id) = build_three_node_cluster(policy).await;
    trace.push(SimEvent::ClusterBuilt { seed: config.seed });
    trace.push(SimEvent::LeaderElected { leader_id });

    let leader_index = usize::try_from(leader_id - 1).expect("leader id fits usize");

    engines[leader_index]
        .create_stream(
            CreateStreamRequest::new(config.stream.clone(), "application/octet-stream"),
            placement(),
            ColdWriteAdmission::default(),
        )
        .await
        .expect("create stream through simulated leader");
    trace.push(SimEvent::StreamCreated {
        stream: config.stream.clone(),
    });

    let appended = engines[leader_index]
        .append(
            AppendRequest::from_bytes(config.stream.clone(), b"simulated".to_vec()),
            placement(),
            ColdWriteAdmission::default(),
        )
        .await
        .expect("append through simulated leader");
    let appended_log_index = appended.group_commit_index;
    trace.push(SimEvent::AppendCommitted {
        stream: config.stream.clone(),
        log_index: appended_log_index,
    });

    wait_all_nodes_applied(&engines, appended_log_index, "append applied on all nodes").await;
    trace.push(SimEvent::AllNodesApplied {
        log_index: appended_log_index,
    });

    verify_all_nodes_can_read(&mut engines, &config.stream).await;
    trace.push(SimEvent::AllNodesReadVerified {
        stream: config.stream,
    });

    ThreeNodeRaftSimOutcome {
        seed: config.seed,
        leader_id,
        target_node_id: None,
        appended_log_index,
        trace,
    }
}

pub(super) async fn run_partition_heal_inner(
    config: ThreeNodeRaftSimConfig,
    partition_before_append: bool,
    heal_after_lag: bool,
) -> ThreeNodeRaftSimOutcome {
    let mut trace = SimTrace::default();
    let policy = sim_network_policy();
    let (_registry, mut engines, leader_id) = build_three_node_cluster(policy.clone()).await;
    trace.push(SimEvent::ClusterBuilt { seed: config.seed });
    trace.push(SimEvent::LeaderElected { leader_id });

    let leader_index = usize::try_from(leader_id - 1).expect("leader id fits usize");
    let isolated_id = seeded_follower_id(config.seed, leader_id);
    let connected_id = (1..=3)
        .find(|node_id| *node_id != leader_id && *node_id != isolated_id)
        .expect("connected follower exists");
    let isolated_index = usize::try_from(isolated_id - 1).expect("node id fits usize");
    let connected_index = usize::try_from(connected_id - 1).expect("node id fits usize");

    let mut script = InProcessRaftFaultScript::new(config.seed);
    script.push(
        "before_append",
        InProcessRaftFaultAction::PartitionBidirectional {
            a: leader_id,
            b: isolated_id,
        },
    );
    script.push(
        "after_isolated_lag",
        InProcessRaftFaultAction::HealBidirectional {
            a: leader_id,
            b: isolated_id,
        },
    );

    if partition_before_append {
        script.apply_phase("before_append", &policy);
        trace.push(SimEvent::FaultApplied {
            phase: "before_append".to_owned(),
        });
    }

    engines[leader_index]
        .create_stream(
            CreateStreamRequest::new(config.stream.clone(), "application/octet-stream"),
            placement(),
            ColdWriteAdmission::default(),
        )
        .await
        .expect("create stream through simulated leader");
    trace.push(SimEvent::StreamCreated {
        stream: config.stream.clone(),
    });

    let appended = engines[leader_index]
        .append(
            AppendRequest::from_bytes(config.stream.clone(), b"simulated".to_vec()),
            placement(),
            ColdWriteAdmission::default(),
        )
        .await
        .expect("append through simulated leader");
    let appended_log_index = appended.group_commit_index;
    trace.push(SimEvent::AppendCommitted {
        stream: config.stream.clone(),
        log_index: appended_log_index,
    });

    for index in [leader_index, connected_index] {
        engines[index]
            .raft_handle()
            .wait(Some(Duration::from_secs(5)))
            .applied_index_at_least(Some(appended_log_index), "append applied on majority")
            .await
            .expect("wait for majority apply");
    }
    trace.push(SimEvent::MajorityApplied {
        log_index: appended_log_index,
    });

    if partition_before_append {
        let isolated_wait = engines[isolated_index]
            .raft_handle()
            .wait(Some(Duration::from_millis(50)))
            .applied_index_at_least(Some(appended_log_index), "isolated follower should lag")
            .await;
        assert!(
            isolated_wait.is_err(),
            "isolated follower should not apply before heal"
        );
        trace.push(SimEvent::IsolatedFollowerLagged {
            node_id: isolated_id,
            log_index: appended_log_index,
        });
        if !heal_after_lag {
            let message = format!(
                "node {isolated_id} remained partitioned at log index {appended_log_index}"
            );
            SimTrace::record(SimEvent::InvariantFailed {
                invariant: "raft_partition_follower_catchup".to_owned(),
                after_event: "isolated_follower_lagged".to_owned(),
                message: message.clone(),
            });
            panic!(
                "invariant `raft_partition_follower_catchup` failed after `isolated_follower_lagged`: {message}"
            );
        }
    }

    if heal_after_lag {
        script.apply_phase("after_isolated_lag", &policy);
        trace.push(SimEvent::FaultApplied {
            phase: "after_isolated_lag".to_owned(),
        });
    }
    engines[isolated_index]
        .raft_handle()
        .wait(Some(Duration::from_secs(5)))
        .applied_index_at_least(Some(appended_log_index), "healed follower catches up")
        .await
        .expect("wait for healed follower apply");
    trace.push(SimEvent::FollowerCaughtUp {
        node_id: isolated_id,
        log_index: appended_log_index,
    });

    read_local_payload_eventually(
        &engines[isolated_index],
        isolated_id,
        &config.stream,
        0,
        16,
        b"simulated",
        "read healed follower",
    )
    .await;
    trace.push(SimEvent::FollowerReadVerified {
        node_id: isolated_id,
        stream: config.stream,
    });

    ThreeNodeRaftSimOutcome {
        seed: script.seed(),
        leader_id,
        target_node_id: Some(isolated_id),
        appended_log_index,
        trace,
    }
}

pub(super) async fn run_snapshot_catch_up_inner(
    config: ThreeNodeRaftSimConfig,
) -> ThreeNodeRaftSimOutcome {
    let mut trace = SimTrace::default();
    let policy = sim_network_policy();
    let (registry, mut engines, leader_id) = build_lagging_learner_snapshot_cluster(policy).await;
    trace.push(SimEvent::ClusterBuilt { seed: config.seed });
    trace.push(SimEvent::LeaderElected { leader_id });

    let leader_index = usize::try_from(leader_id - 1).expect("leader id fits usize");
    let learner_id = 3;
    let learner_index = usize::try_from(learner_id - 1).expect("learner id fits usize");

    engines[leader_index]
        .create_stream(
            CreateStreamRequest::new(config.stream.clone(), "application/octet-stream"),
            placement(),
            ColdWriteAdmission::default(),
        )
        .await
        .expect("create stream through simulated leader");
    trace.push(SimEvent::StreamCreated {
        stream: config.stream.clone(),
    });

    let appended = engines[leader_index]
        .append(
            AppendRequest::from_bytes(config.stream.clone(), b"snapshot-transfer".to_vec()),
            placement(),
            ColdWriteAdmission::default(),
        )
        .await
        .expect("append through simulated leader");
    let appended_log_index = appended.group_commit_index;
    trace.push(SimEvent::AppendCommitted {
        stream: config.stream.clone(),
        log_index: appended_log_index,
    });

    for engine in &engines[..2] {
        engine
            .raft_handle()
            .wait(Some(Duration::from_secs(5)))
            .applied_index_at_least(Some(appended_log_index), "initial voters applied append")
            .await
            .expect("wait for initial voter apply");
    }
    trace.push(SimEvent::MajorityApplied {
        log_index: appended_log_index,
    });

    let leader = engines[leader_index].raft_handle();
    leader
        .trigger()
        .snapshot()
        .await
        .expect("trigger leader snapshot");
    leader
        .wait(Some(Duration::from_secs(5)))
        .metrics(
            |metrics| {
                metrics
                    .snapshot
                    .as_ref()
                    .is_some_and(|log_id| log_id.index() >= appended_log_index)
            },
            "leader snapshot includes append",
        )
        .await
        .expect("wait for leader snapshot");
    trace.push(SimEvent::SnapshotCreated {
        log_index: appended_log_index,
    });

    leader
        .trigger()
        .purge_log(appended_log_index)
        .await
        .expect("trigger leader log purge");
    leader
        .wait(Some(Duration::from_secs(5)))
        .metrics(
            |metrics| {
                metrics
                    .purged
                    .as_ref()
                    .is_some_and(|log_id| log_id.index() >= appended_log_index)
            },
            "leader purged snapshotted logs",
        )
        .await
        .expect("wait for leader purge");
    trace.push(SimEvent::LogPurged {
        log_index: appended_log_index,
    });

    registry.register(learner_id, engines[learner_index].raft_handle());
    let learner_added = leader
        .add_learner(learner_id, BasicNode::new("node-3"), true)
        .await
        .expect("add lagging learner");
    trace.push(SimEvent::LearnerAdded {
        node_id: learner_id,
        log_index: learner_added.log_id.index(),
    });

    for attempt in 0..50 {
        if registry.full_snapshot_count(learner_id) > 0 {
            break;
        }
        leader
            .trigger()
            .heartbeat()
            .await
            .expect("trigger heartbeat while waiting for snapshot replication");
        SimTrace::record(SimEvent::HeartbeatTriggered {
            node_id: leader_id,
            reason: "waiting for snapshot replication".to_owned(),
            attempt,
        });
        madsim::time::sleep(Duration::from_millis(100)).await;
    }
    let full_snapshot_count = registry.full_snapshot_count(learner_id);
    assert!(
        full_snapshot_count >= 1,
        "lagging learner should catch up through full_snapshot"
    );
    trace.push(SimEvent::FullSnapshotTransferred {
        node_id: learner_id,
        count: full_snapshot_count,
    });

    engines[learner_index]
        .raft_handle()
        .wait(Some(Duration::from_secs(5)))
        .applied_index_at_least(
            Some(learner_added.log_id.index()),
            "lagging learner applied learner membership",
        )
        .await
        .expect("wait for lagging learner catch-up");
    trace.push(SimEvent::FollowerCaughtUp {
        node_id: learner_id,
        log_index: learner_added.log_id.index(),
    });

    read_local_payload_eventually(
        &engines[learner_index],
        learner_id,
        &config.stream,
        0,
        64,
        b"snapshot-transfer",
        "read lagging learner after snapshot catch-up",
    )
    .await;
    trace.push(SimEvent::SnapshotCatchUpReadVerified {
        node_id: learner_id,
        stream: config.stream,
    });

    ThreeNodeRaftSimOutcome {
        seed: config.seed,
        leader_id,
        target_node_id: Some(learner_id),
        appended_log_index,
        trace,
    }
}

#[cfg(test)]
pub(super) async fn run_isolated_leader_pending_write_snapshot_purge_inner(
    config: ThreeNodeRaftSimConfig,
) -> ThreeNodeRaftSimOutcome {
    let mut trace = SimTrace::default();
    let policy = sim_network_policy();
    let (registry, mut engines, log_stores, old_leader_id) =
        build_three_node_snapshot_purge_cluster(policy.clone()).await;
    trace.push(SimEvent::ClusterBuilt { seed: config.seed });
    trace.push(SimEvent::LeaderElected {
        leader_id: old_leader_id,
    });

    let old_leader_index = usize::try_from(old_leader_id - 1).expect("leader id fits usize");
    engines[old_leader_index]
        .create_stream(
            CreateStreamRequest::new(config.stream.clone(), "application/octet-stream"),
            placement(),
            ColdWriteAdmission::default(),
        )
        .await
        .expect("create stream through old leader");
    trace.push(SimEvent::StreamCreated {
        stream: config.stream.clone(),
    });

    let baseline = engines[old_leader_index]
        .append(
            AppendRequest::from_bytes(config.stream.clone(), b"baseline;".to_vec()),
            placement(),
            ColdWriteAdmission::default(),
        )
        .await
        .expect("append baseline through old leader");
    let baseline_log_index = baseline.group_commit_index;
    trace.push(SimEvent::AppendCommitted {
        stream: config.stream.clone(),
        log_index: baseline_log_index,
    });
    wait_all_nodes_applied(
        &engines,
        baseline_log_index,
        "baseline applied on all nodes",
    )
    .await;
    trace.push(SimEvent::AllNodesApplied {
        log_index: baseline_log_index,
    });

    let connected_ids = (1..=3)
        .filter(|node_id| *node_id != old_leader_id)
        .collect::<Vec<_>>();
    for peer in &connected_ids {
        policy.partition_bidirectional(old_leader_id, *peer);
    }
    trace.push(SimEvent::FaultApplied {
        phase: "isolate_old_leader".to_owned(),
    });

    let old_leader_raft = engines[old_leader_index].raft_handle();
    let pending_write_count = 32_u64;
    let mut pending_writes = Vec::new();
    for pending_id in 0..pending_write_count {
        let pending_stream = config.stream.clone();
        pending_writes.push(madsim::task::spawn({
            let old_leader_raft = old_leader_raft.clone();
            async move {
                old_leader_raft
                    .client_write(
                        GroupWriteCommand::from(AppendRequest::from_bytes(
                            pending_stream,
                            format!("pending-old-leader-{pending_id};").into_bytes(),
                        ))
                        .into(),
                    )
                    .await
            }
        }));
    }

    let mut old_leader_log_store = log_stores[old_leader_index].clone();
    let mut pending_log_index = None;
    for _ in 0..100 {
        let log_state = old_leader_log_store
            .get_log_state()
            .await
            .expect("read old leader log state");
        if let Some(index) = log_state.last_log_id.map(|log_id| log_id.index)
            && index >= baseline_log_index + pending_write_count
        {
            pending_log_index = Some(index);
            break;
        }
        madsim::time::sleep(Duration::from_millis(10)).await;
    }
    let pending_log_index =
        pending_log_index.expect("old leader should append pending uncommitted client write");
    trace.push(SimEvent::IsolatedFollowerLagged {
        node_id: old_leader_id,
        log_index: pending_log_index,
    });

    let observer_id = connected_ids[0];
    let observer_index = usize::try_from(observer_id - 1).expect("observer id fits usize");
    let observer_raft = engines[observer_index].raft_handle();
    let new_leader_metrics = observer_raft
        .wait(Some(Duration::from_secs(5)))
        .metrics(
            |metrics| {
                metrics
                    .current_leader
                    .is_some_and(|leader_id| leader_id != old_leader_id)
            },
            "connected voters elect replacement leader",
        )
        .await
        .expect("wait for replacement leader");
    let new_leader_id = new_leader_metrics
        .current_leader
        .expect("replacement leader id");
    let new_leader_index = usize::try_from(new_leader_id - 1).expect("new leader id fits usize");
    trace.push(SimEvent::LeaderElected {
        leader_id: new_leader_id,
    });

    let mut expected_payload = b"baseline;".to_vec();
    let mut latest_log_index = baseline_log_index;
    for append_id in 0..8 {
        let payload = format!("new-{append_id};").into_bytes();
        let appended = engines[new_leader_index]
            .append(
                AppendRequest::from_bytes(config.stream.clone(), payload.clone()),
                placement(),
                ColdWriteAdmission::default(),
            )
            .await
            .expect("append through replacement leader");
        expected_payload.extend(payload);
        latest_log_index = appended.group_commit_index;
        trace.push(SimEvent::AppendCommitted {
            stream: config.stream.clone(),
            log_index: latest_log_index,
        });
    }
    for node_id in &connected_ids {
        let index = usize::try_from(*node_id - 1).expect("node id fits usize");
        engines[index]
            .raft_handle()
            .wait(Some(Duration::from_secs(5)))
            .applied_index_at_least(
                Some(latest_log_index),
                "replacement majority applied new leader appends",
            )
            .await
            .expect("wait for replacement majority apply");
    }
    trace.push(SimEvent::MajorityApplied {
        log_index: latest_log_index,
    });

    let new_leader_raft = engines[new_leader_index].raft_handle();
    new_leader_raft
        .trigger()
        .snapshot()
        .await
        .expect("trigger replacement leader snapshot");
    new_leader_raft
        .wait(Some(Duration::from_secs(5)))
        .metrics(
            |metrics| {
                metrics
                    .snapshot
                    .as_ref()
                    .is_some_and(|log_id| log_id.index() >= latest_log_index)
            },
            "replacement leader snapshot includes new appends",
        )
        .await
        .expect("wait for replacement leader snapshot");
    trace.push(SimEvent::SnapshotCreated {
        log_index: latest_log_index,
    });

    new_leader_raft
        .trigger()
        .purge_log(latest_log_index)
        .await
        .expect("trigger replacement leader log purge");
    new_leader_raft
        .wait(Some(Duration::from_secs(5)))
        .metrics(
            |metrics| {
                metrics
                    .purged
                    .as_ref()
                    .is_some_and(|log_id| log_id.index() >= latest_log_index)
            },
            "replacement leader purged snapshotted logs",
        )
        .await
        .expect("wait for replacement leader purge");
    trace.push(SimEvent::LogPurged {
        log_index: latest_log_index,
    });

    policy.set_delay(Some(Duration::from_millis(125)));
    trace.push(SimEvent::FaultApplied {
        phase: "delay_before_heal_old_leader".to_owned(),
    });
    for peer in &connected_ids {
        policy.heal_bidirectional(old_leader_id, *peer);
    }
    trace.push(SimEvent::FaultApplied {
        phase: "heal_old_leader".to_owned(),
    });

    for attempt in 0..50 {
        if old_leader_raft
            .wait(Some(Duration::from_millis(100)))
            .applied_index_at_least(Some(latest_log_index), "old leader catches up after purge")
            .await
            .is_ok()
        {
            break;
        }
        new_leader_raft
            .trigger()
            .heartbeat()
            .await
            .expect("trigger heartbeat while waiting for old leader catch-up");
        SimTrace::record(SimEvent::HeartbeatTriggered {
            node_id: new_leader_id,
            reason: "waiting for old leader catch-up after purge".to_owned(),
            attempt,
        });
        madsim::time::sleep(Duration::from_millis(100)).await;
    }
    old_leader_raft
        .wait(Some(Duration::from_secs(5)))
        .applied_index_at_least(Some(latest_log_index), "old leader catches up after purge")
        .await
        .expect("wait for old leader catch-up after purge");
    trace.push(SimEvent::FullSnapshotTransferred {
        node_id: old_leader_id,
        count: registry.full_snapshot_count(old_leader_id),
    });
    trace.push(SimEvent::FollowerCaughtUp {
        node_id: old_leader_id,
        log_index: latest_log_index,
    });

    read_local_payload_eventually(
        &engines[old_leader_index],
        old_leader_id,
        &config.stream,
        0,
        expected_payload.len(),
        &expected_payload,
        "read old leader after snapshot/purge catch-up",
    )
    .await;
    trace.push(SimEvent::FollowerReadVerified {
        node_id: old_leader_id,
        stream: config.stream.clone(),
    });

    for pending_write in pending_writes {
        if let Ok(joined) = madsim::time::timeout(Duration::from_millis(100), pending_write).await {
            let _ = joined.expect("pending old-leader client write task panicked");
        }
    }

    ThreeNodeRaftSimOutcome {
        seed: config.seed,
        leader_id: old_leader_id,
        target_node_id: Some(old_leader_id),
        appended_log_index: latest_log_index,
        trace,
    }
}

pub(super) async fn run_restart_follower_inner(
    config: ThreeNodeRaftSimConfig,
) -> ThreeNodeRaftSimOutcome {
    let mut trace = SimTrace::default();
    let policy = sim_network_policy();
    let (registry, mut engines, log_stores, raft_config, leader_id) =
        build_restartable_three_node_cluster(policy.clone()).await;
    trace.push(SimEvent::ClusterBuilt { seed: config.seed });
    trace.push(SimEvent::LeaderElected { leader_id });

    let leader_index = usize::try_from(leader_id - 1).expect("leader id fits usize");
    let restarted_id = seeded_follower_id(config.seed, leader_id);
    let restarted_index = usize::try_from(restarted_id - 1).expect("node id fits usize");
    let connected_id = (1..=3)
        .find(|node_id| *node_id != leader_id && *node_id != restarted_id)
        .expect("connected follower exists");
    let connected_index = usize::try_from(connected_id - 1).expect("node id fits usize");

    registry.unregister(restarted_id);
    engines[restarted_index]
        .shutdown()
        .await
        .expect("shutdown restarted follower before offline append");
    trace.push(SimEvent::NodeStopped {
        node_id: restarted_id,
    });

    engines[leader_index]
        .create_stream(
            CreateStreamRequest::new(config.stream.clone(), "application/octet-stream"),
            placement(),
            ColdWriteAdmission::default(),
        )
        .await
        .expect("create stream while follower is stopped");
    trace.push(SimEvent::StreamCreated {
        stream: config.stream.clone(),
    });

    let appended = engines[leader_index]
        .append(
            AppendRequest::from_bytes(config.stream.clone(), b"restart-transfer".to_vec()),
            placement(),
            ColdWriteAdmission::default(),
        )
        .await
        .expect("append while follower is stopped");
    let appended_log_index = appended.group_commit_index;
    trace.push(SimEvent::AppendCommitted {
        stream: config.stream.clone(),
        log_index: appended_log_index,
    });

    for index in [leader_index, connected_index] {
        engines[index]
            .raft_handle()
            .wait(Some(Duration::from_secs(5)))
            .applied_index_at_least(
                Some(appended_log_index),
                "majority applied append while follower stopped",
            )
            .await
            .expect("wait for majority apply while follower stopped");
    }
    trace.push(SimEvent::MajorityApplied {
        log_index: appended_log_index,
    });

    let restarted = RaftGroupEngine::new_node_with_log_store_and_network(
        placement(),
        restarted_id,
        raft_config,
        InProcessRaftNetworkFactory::new(registry.clone())
            .with_source(restarted_id)
            .with_policy(policy),
        log_stores[restarted_index].clone(),
        None,
        None,
    )
    .await
    .expect("restart follower with the same log store");
    registry.register(restarted_id, restarted.raft_handle());
    engines[restarted_index] = restarted;
    trace.push(SimEvent::NodeRestarted {
        node_id: restarted_id,
    });

    let leader = engines[leader_index].raft_handle();
    for attempt in 0..50 {
        if engines[restarted_index]
            .raft_handle()
            .wait(Some(Duration::from_millis(100)))
            .applied_index_at_least(Some(appended_log_index), "restarted follower catches up")
            .await
            .is_ok()
        {
            break;
        }
        leader
            .trigger()
            .heartbeat()
            .await
            .expect("trigger heartbeat while waiting for restarted follower");
        SimTrace::record(SimEvent::HeartbeatTriggered {
            node_id: leader_id,
            reason: "waiting for restarted follower".to_owned(),
            attempt,
        });
        madsim::time::sleep(Duration::from_millis(100)).await;
    }
    engines[restarted_index]
        .raft_handle()
        .wait(Some(Duration::from_secs(5)))
        .applied_index_at_least(Some(appended_log_index), "restarted follower caught up")
        .await
        .expect("wait for restarted follower catch-up");
    trace.push(SimEvent::FollowerCaughtUp {
        node_id: restarted_id,
        log_index: appended_log_index,
    });

    read_local_payload_eventually(
        &engines[restarted_index],
        restarted_id,
        &config.stream,
        0,
        64,
        b"restart-transfer",
        "read restarted follower after catch-up",
    )
    .await;
    trace.push(SimEvent::RestartedNodeReadVerified {
        node_id: restarted_id,
        stream: config.stream,
    });

    ThreeNodeRaftSimOutcome {
        seed: config.seed,
        leader_id,
        target_node_id: Some(restarted_id),
        appended_log_index,
        trace,
    }
}

pub(super) async fn run_leader_failover_inner(
    config: ThreeNodeRaftSimConfig,
) -> ThreeNodeRaftSimOutcome {
    let mut trace = SimTrace::default();
    let policy = sim_network_policy();
    let (registry, mut engines, log_stores, raft_config, old_leader_id) =
        build_restartable_three_node_cluster(policy.clone()).await;
    trace.push(SimEvent::ClusterBuilt { seed: config.seed });
    trace.push(SimEvent::LeaderElected {
        leader_id: old_leader_id,
    });

    let old_leader_index = usize::try_from(old_leader_id - 1).expect("leader id fits usize");

    engines[old_leader_index]
        .create_stream(
            CreateStreamRequest::new(config.stream.clone(), "application/octet-stream"),
            placement(),
            ColdWriteAdmission::default(),
        )
        .await
        .expect("create stream before leader failover");
    trace.push(SimEvent::StreamCreated {
        stream: config.stream.clone(),
    });

    let before = engines[old_leader_index]
        .append(
            AppendRequest::from_bytes(config.stream.clone(), b"before-".to_vec()),
            placement(),
            ColdWriteAdmission::default(),
        )
        .await
        .expect("append before leader failover");
    trace.push(SimEvent::AppendCommitted {
        stream: config.stream.clone(),
        log_index: before.group_commit_index,
    });
    for node_id in 1..=3 {
        let index = usize::try_from(node_id - 1).expect("node id fits usize");
        engines[index]
            .raft_handle()
            .wait(Some(Duration::from_secs(5)))
            .applied_index_at_least(
                Some(before.group_commit_index),
                "all nodes apply initial append before leader failover",
            )
            .await
            .expect("wait for initial append on all nodes before leader failover");
    }
    trace.push(SimEvent::AllNodesApplied {
        log_index: before.group_commit_index,
    });

    registry.unregister(old_leader_id);
    engines[old_leader_index]
        .shutdown()
        .await
        .expect("shutdown current leader before failover");
    trace.push(SimEvent::FaultApplied {
        phase: "after_initial_append".to_owned(),
    });
    trace.push(SimEvent::NodeStopped {
        node_id: old_leader_id,
    });

    let wait_node_id = (1..=3)
        .find(|node_id| *node_id != old_leader_id)
        .expect("remaining voter exists");
    let wait_index = usize::try_from(wait_node_id - 1).expect("node id fits usize");
    let new_leader_metrics = engines[wait_index]
        .raft_handle()
        .wait(Some(Duration::from_secs(5)))
        .metrics(
            |metrics| {
                metrics
                    .current_leader
                    .is_some_and(|leader_id| leader_id != old_leader_id)
            },
            "new leader elected after old leader stop",
        )
        .await
        .expect("wait for new leader after old leader stop");
    let new_leader_id = new_leader_metrics
        .current_leader
        .expect("new leader id after old leader stop");
    let new_leader_index = usize::try_from(new_leader_id - 1).expect("new leader id fits usize");
    trace.push(SimEvent::LeaderElected {
        leader_id: new_leader_id,
    });

    let after = engines[new_leader_index]
        .append(
            AppendRequest::from_bytes(config.stream.clone(), b"after".to_vec()),
            placement(),
            ColdWriteAdmission::default(),
        )
        .await
        .expect("append through new leader after failover");
    trace.push(SimEvent::AppendCommitted {
        stream: config.stream.clone(),
        log_index: after.group_commit_index,
    });
    trace.push(SimEvent::LeaderFailoverAppendVerified {
        old_leader_id,
        new_leader_id,
        stream: config.stream.clone(),
        first_next_offset: before.next_offset,
        second_next_offset: after.next_offset,
        log_index: after.group_commit_index,
    });

    for node_id in 1..=3 {
        if node_id == old_leader_id {
            continue;
        }
        let index = usize::try_from(node_id - 1).expect("node id fits usize");
        engines[index]
            .raft_handle()
            .wait(Some(Duration::from_secs(5)))
            .applied_index_at_least(
                Some(after.group_commit_index),
                "remaining majority applies post-failover append",
            )
            .await
            .expect("wait for post-failover append on remaining majority");
    }
    trace.push(SimEvent::MajorityApplied {
        log_index: after.group_commit_index,
    });

    let restarted = RaftGroupEngine::new_node_with_log_store_and_network(
        placement(),
        old_leader_id,
        raft_config,
        InProcessRaftNetworkFactory::new(registry.clone())
            .with_source(old_leader_id)
            .with_policy(policy),
        log_stores[old_leader_index].clone(),
        None,
        None,
    )
    .await
    .expect("restart old leader with the same log store");
    registry.register(old_leader_id, restarted.raft_handle());
    engines[old_leader_index] = restarted;
    trace.push(SimEvent::FaultApplied {
        phase: "after_failover_append".to_owned(),
    });
    trace.push(SimEvent::NodeRestarted {
        node_id: old_leader_id,
    });

    let new_leader = engines[new_leader_index].raft_handle();
    for attempt in 0..50 {
        if engines[old_leader_index]
            .raft_handle()
            .wait(Some(Duration::from_millis(100)))
            .applied_index_at_least(
                Some(after.group_commit_index),
                "old leader catches up after restart",
            )
            .await
            .is_ok()
        {
            break;
        }
        new_leader
            .trigger()
            .heartbeat()
            .await
            .expect("trigger heartbeat while waiting for old leader catch-up");
        SimTrace::record(SimEvent::HeartbeatTriggered {
            node_id: new_leader_id,
            reason: "waiting for restarted old leader after failover".to_owned(),
            attempt,
        });
        madsim::time::sleep(Duration::from_millis(100)).await;
    }
    engines[old_leader_index]
        .raft_handle()
        .wait(Some(Duration::from_secs(5)))
        .applied_index_at_least(
            Some(after.group_commit_index),
            "old leader caught up after restart",
        )
        .await
        .expect("wait for old leader catch-up after restart");
    trace.push(SimEvent::FollowerCaughtUp {
        node_id: old_leader_id,
        log_index: after.group_commit_index,
    });

    for node_id in 1..=3 {
        let index = usize::try_from(node_id - 1).expect("node id fits usize");
        let read = read_local_payload_eventually(
            &engines[index],
            node_id,
            &config.stream,
            0,
            64,
            b"before-after",
            "read all nodes after leader failover and old leader restart",
        )
        .await;
        if read.next_offset != after.next_offset {
            let message = format!(
                "node {node_id} returned next_offset {}, expected {}",
                read.next_offset, after.next_offset
            );
            SimTrace::record(SimEvent::InvariantFailed {
                invariant: "leader_failover_no_loss_or_dup".to_owned(),
                after_event: "leader_failover_read_verified".to_owned(),
                message: message.clone(),
            });
            panic!(
                "invariant `leader_failover_no_loss_or_dup` failed after `leader_failover_read_verified`: {message}"
            );
        }
        trace.push(SimEvent::FollowerReadVerified {
            node_id,
            stream: config.stream.clone(),
        });
    }
    trace.push(SimEvent::LeaderFailoverReadVerified {
        stream: config.stream.clone(),
        next_offset: after.next_offset,
        node_count: 3,
    });

    ThreeNodeRaftSimOutcome {
        seed: config.seed,
        leader_id: new_leader_id,
        target_node_id: Some(old_leader_id),
        appended_log_index: after.group_commit_index,
        trace,
    }
}

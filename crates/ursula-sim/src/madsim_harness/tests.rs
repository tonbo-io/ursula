//! Extracted from madsim_harness.rs (DoD #3 modularity refactor).
//! See crates/ursula-sim/src/madsim_harness/mod.rs for the rest.

use std::collections::BTreeSet;
use std::sync::Mutex;
use std::sync::MutexGuard;

use super::*;

static SIM_TEST_LOCK: Mutex<()> = Mutex::new(());

fn sim_test_guard() -> MutexGuard<'static, ()> {
    SIM_TEST_LOCK
        .lock()
        .unwrap_or_else(|poison| poison.into_inner())
}

#[test]
#[ignore = "diagnostic: madsim process-global state makes scenario tests safer to run individually"]
fn partition_heal_workload_replays_with_same_seed_and_trace() {
    let _guard = sim_test_guard();
    let first = ThreeNodeRaftSim::run_partition_heal_report(ThreeNodeRaftSimConfig::new(
        11,
        "ursula-sim-partition-heal",
    ));
    let second = ThreeNodeRaftSim::run_partition_heal_report(ThreeNodeRaftSimConfig::new(
        11,
        "ursula-sim-partition-heal",
    ));

    assert_eq!(first, second);
    assert_eq!(first.scenario, SimScenario::PartitionHeal);
    assert_eq!(first.outcome.seed, 11);
    assert!(first.outcome.target_node_id.is_some());
    assert!(first.outcome.appended_log_index > 0);
    assert!(
        first
            .outcome
            .trace
            .events
            .iter()
            .any(|event| matches!(event, SimEvent::IsolatedFollowerLagged { .. }))
    );
    assert!(
        first
            .outcome
            .trace
            .events
            .iter()
            .any(|event| matches!(event, SimEvent::FollowerReadVerified { .. }))
    );
}

#[test]
#[ignore = "diagnostic: covered by smoke_corpus_replays in the default madsim suite"]
fn no_fault_workload_replays_with_same_seed_and_trace() {
    let _guard = sim_test_guard();
    let first = ThreeNodeRaftSim::run_no_fault_report(ThreeNodeRaftSimConfig::new(
        29,
        "ursula-sim-no-fault",
    ));
    let second = ThreeNodeRaftSim::run_no_fault_report(ThreeNodeRaftSimConfig::new(
        29,
        "ursula-sim-no-fault",
    ));

    assert_eq!(first, second);
    assert_eq!(first.scenario, SimScenario::NoFaultBaseline);
    assert_eq!(first.outcome.seed, 29);
    assert_eq!(first.outcome.target_node_id, None);
    assert!(first.outcome.appended_log_index > 0);
    assert!(
        first
            .outcome
            .trace
            .events
            .iter()
            .any(|event| matches!(event, SimEvent::AllNodesApplied { .. }))
    );
    assert!(
        first
            .outcome
            .trace
            .events
            .iter()
            .any(|event| matches!(event, SimEvent::AllNodesReadVerified { .. }))
    );
}

#[test]
#[ignore = "diagnostic: madsim process-global state makes scenario tests safer to run individually"]
fn snapshot_catch_up_workload_replays_with_same_seed_and_trace() {
    let _guard = sim_test_guard();
    let first = ThreeNodeRaftSim::run_snapshot_catch_up_report(ThreeNodeRaftSimConfig::new(
        43,
        "ursula-sim-snapshot-catch-up",
    ));
    let second = ThreeNodeRaftSim::run_snapshot_catch_up_report(ThreeNodeRaftSimConfig::new(
        43,
        "ursula-sim-snapshot-catch-up",
    ));

    assert_eq!(first, second);
    assert_eq!(first.scenario, SimScenario::SnapshotCatchUp);
    assert_eq!(first.outcome.seed, 43);
    assert_eq!(first.outcome.target_node_id, Some(3));
    assert!(
        first
            .outcome
            .trace
            .events
            .iter()
            .any(|event| matches!(event, SimEvent::FullSnapshotTransferred { .. }))
    );
    assert!(
        first
            .outcome
            .trace
            .events
            .iter()
            .any(|event| matches!(event, SimEvent::SnapshotCatchUpReadVerified { .. }))
    );
}

#[test]
#[ignore = "diagnostic: targets pending OpenRaft responders during snapshot/purge catch-up"]
fn isolated_leader_pending_write_snapshot_purge_probe() {
    let _guard = sim_test_guard();
    for seed in 901..=916 {
        let outcome = run_with_madsim(seed, async move {
            run_isolated_leader_pending_write_snapshot_purge_inner(ThreeNodeRaftSimConfig::new(
                seed,
                format!("ursula-sim-isolated-leader-pending-write-snapshot-purge-{seed}"),
            ))
            .await
        });

        assert_eq!(outcome.seed, seed);
        assert!(outcome.target_node_id.is_some());
        assert!(outcome.appended_log_index > 0);
        assert!(
            outcome
                .trace
                .events
                .iter()
                .any(|event| matches!(event, SimEvent::LogPurged { .. }))
        );
        assert!(outcome.trace.events.iter().any(|event| matches!(
            event,
            SimEvent::FullSnapshotTransferred { count, .. } if *count > 0
        )));
        assert!(
            outcome
                .trace
                .events
                .iter()
                .any(|event| matches!(event, SimEvent::FollowerCaughtUp { .. }))
        );
        assert!(
            outcome
                .trace
                .events
                .iter()
                .any(|event| matches!(event, SimEvent::FollowerReadVerified { .. }))
        );
    }
}

#[test]
#[ignore = "diagnostic: madsim process-global state makes scenario tests safer to run individually"]
fn restart_follower_workload_replays_with_same_seed_and_trace() {
    let _guard = sim_test_guard();
    let first = ThreeNodeRaftSim::run_restart_follower_report(ThreeNodeRaftSimConfig::new(
        47,
        "ursula-sim-restart-follower",
    ));
    let second = ThreeNodeRaftSim::run_restart_follower_report(ThreeNodeRaftSimConfig::new(
        47,
        "ursula-sim-restart-follower",
    ));

    assert_eq!(first, second);
    assert_eq!(first.scenario, SimScenario::RestartFollower);
    assert_eq!(first.outcome.seed, 47);
    assert!(first.outcome.target_node_id.is_some());
    assert!(
        first
            .outcome
            .trace
            .events
            .iter()
            .any(|event| matches!(event, SimEvent::NodeStopped { .. }))
    );
    assert!(
        first
            .outcome
            .trace
            .events
            .iter()
            .any(|event| matches!(event, SimEvent::RestartedNodeReadVerified { .. }))
    );
}

#[test]
#[ignore = "diagnostic: madsim process-global state makes scenario tests safer to run individually"]
fn cold_live_read_workload_replays_with_same_seed_and_trace() {
    let _guard = sim_test_guard();
    let first = ThreeNodeRaftSim::run_cold_live_read_report(ThreeNodeRaftSimConfig::new(
        53,
        "ursula-sim-cold-live",
    ));
    let second = ThreeNodeRaftSim::run_cold_live_read_report(ThreeNodeRaftSimConfig::new(
        53,
        "ursula-sim-cold-live",
    ));

    assert_eq!(first, second);
    assert_eq!(first.scenario, SimScenario::ColdLiveRead);
    assert_eq!(first.outcome.seed, 53);
    assert_eq!(first.outcome.target_node_id, None);
    assert!(
        first
            .outcome
            .trace
            .events
            .iter()
            .any(|event| matches!(event, SimEvent::ColdFlushed { .. }))
    );
    assert_eq!(
        first
            .outcome
            .trace
            .events
            .iter()
            .filter(|event| matches!(event, SimEvent::ColdLiveReadVerified { .. }))
            .count(),
        3
    );
}

#[test]
#[ignore = "diagnostic: madsim process-global state makes scenario tests safer to run individually"]
fn cold_read_fault_workload_replays_with_same_seed_and_trace() {
    let _guard = sim_test_guard();
    let first = ThreeNodeRaftSim::run_cold_read_fault_report(ThreeNodeRaftSimConfig::new(
        65,
        "ursula-sim-cold-read-fault",
    ));
    let second = ThreeNodeRaftSim::run_cold_read_fault_report(ThreeNodeRaftSimConfig::new(
        65,
        "ursula-sim-cold-read-fault",
    ));

    assert_eq!(first, second);
    assert_eq!(first.scenario, SimScenario::ColdReadFault);
    assert_eq!(first.outcome.seed, 65);
    assert!(
        first
            .outcome
            .trace
            .events
            .iter()
            .any(|event| matches!(event, SimEvent::ColdReadFaultObserved { .. }))
    );
}

#[test]
#[ignore = "diagnostic: madsim process-global state makes scenario tests safer to run individually"]
fn cold_write_fault_workload_replays_with_same_seed_and_trace() {
    let _guard = sim_test_guard();
    let first = ThreeNodeRaftSim::run_cold_write_fault_report(ThreeNodeRaftSimConfig::new(
        66,
        "ursula-sim-cold-write-fault",
    ));
    let second = ThreeNodeRaftSim::run_cold_write_fault_report(ThreeNodeRaftSimConfig::new(
        66,
        "ursula-sim-cold-write-fault",
    ));

    assert_eq!(first, second);
    assert_eq!(first.scenario, SimScenario::ColdWriteFault);
    assert_eq!(first.outcome.seed, 66);
    assert!(
        first
            .outcome
            .trace
            .events
            .iter()
            .any(|event| matches!(event, SimEvent::ColdWriteFaultObserved { .. }))
    );
    assert!(
        first
            .outcome
            .trace
            .events
            .iter()
            .any(|event| matches!(event, SimEvent::HotReadAfterColdWriteFailureVerified { .. }))
    );
}

#[test]
#[ignore = "diagnostic: madsim process-global state makes scenario tests safer to run individually"]
fn cold_write_delay_workload_replays_with_same_seed_and_trace() {
    let _guard = sim_test_guard();
    let first = ThreeNodeRaftSim::run_cold_write_delay_report(ThreeNodeRaftSimConfig::new(
        59,
        "ursula-sim-cold-write-delay",
    ));
    let second = ThreeNodeRaftSim::run_cold_write_delay_report(ThreeNodeRaftSimConfig::new(
        59,
        "ursula-sim-cold-write-delay",
    ));

    assert_eq!(first, second);
    assert_eq!(first.scenario, SimScenario::ColdWriteDelay);
    assert_eq!(first.outcome.seed, 59);
    assert!(
        first
            .outcome
            .trace
            .events
            .iter()
            .any(|event| matches!(event, SimEvent::ColdWriteDelayVerified { .. }))
    );
    assert!(
        first
            .outcome
            .trace
            .events
            .iter()
            .any(|event| matches!(event, SimEvent::ColdFlushed { .. }))
    );
}

#[test]
#[ignore = "diagnostic: madsim process-global state makes scenario tests safer to run individually"]
fn cold_delete_fault_workload_replays_with_same_seed_and_trace() {
    let _guard = sim_test_guard();
    let first = ThreeNodeRaftSim::run_cold_delete_fault_report(ThreeNodeRaftSimConfig::new(
        58,
        "ursula-sim-cold-delete-fault",
    ));
    let second = ThreeNodeRaftSim::run_cold_delete_fault_report(ThreeNodeRaftSimConfig::new(
        58,
        "ursula-sim-cold-delete-fault",
    ));

    assert_eq!(first, second);
    assert_eq!(first.scenario, SimScenario::ColdDeleteFault);
    assert_eq!(first.outcome.seed, 58);
    assert!(first.outcome.trace.events.iter().any(|event| matches!(
        event,
        SimEvent::FaultApplied { phase } if phase == "before_cold_cleanup"
    )));
    assert!(
        !first
            .outcome
            .trace
            .events
            .iter()
            .any(|event| matches!(event, SimEvent::ColdDeleteFaultObserved { .. }))
    );
}

#[test]
#[ignore = "diagnostic: madsim process-global state makes scenario tests safer to run individually"]
fn http_producer_protocol_surface_workload_replays_with_same_seed_and_trace() {
    let _guard = sim_test_guard();
    let first = ThreeNodeRaftSim::run_http_producer_protocol_surface_report(
        ThreeNodeRaftSimConfig::new(54, "ursula-sim-http-producer-protocol"),
    );
    let second = ThreeNodeRaftSim::run_http_producer_protocol_surface_report(
        ThreeNodeRaftSimConfig::new(54, "ursula-sim-http-producer-protocol"),
    );

    assert_eq!(first, second);
    assert_eq!(first.scenario, SimScenario::HttpProducerProtocolSurface);
    assert_eq!(first.outcome.seed, 54);
    assert!(first.outcome.trace.events.iter().any(|event| matches!(
        event,
        SimEvent::HttpProducerProtocolSurfaceVerified {
            producer_count: 2,
            final_next_offset: 6,
            gap_expected_seq: 1,
            stale_epoch: 0,
            ..
        }
    )));
}

#[test]
#[ignore = "diagnostic: madsim process-global state makes scenario tests safer to run individually"]
fn http_live_limit_protocol_surface_workload_replays_with_same_seed_and_trace() {
    let _guard = sim_test_guard();
    let first = ThreeNodeRaftSim::run_http_live_limit_protocol_surface_report(
        ThreeNodeRaftSimConfig::new(55, "ursula-sim-http-live-limit-protocol"),
    );
    let second = ThreeNodeRaftSim::run_http_live_limit_protocol_surface_report(
        ThreeNodeRaftSimConfig::new(55, "ursula-sim-http-live-limit-protocol"),
    );

    assert_eq!(first, second);
    assert_eq!(first.scenario, SimScenario::HttpLiveLimitProtocolSurface);
    assert_eq!(first.outcome.seed, 55);
    assert!(first.outcome.trace.events.iter().any(|event| matches!(
        event,
        SimEvent::HttpLiveLimitProtocolSurfaceVerified {
            timeout_next_offset: 0,
            backpressure_events: 1,
            ..
        }
    )));
}

#[test]
#[ignore = "diagnostic: madsim process-global state makes scenario tests safer to run individually"]
fn http_live_protocol_surface_workload_replays_with_same_seed_and_trace() {
    let _guard = sim_test_guard();
    let first = ThreeNodeRaftSim::run_http_live_protocol_surface_report(
        ThreeNodeRaftSimConfig::new(56, "ursula-sim-http-live-protocol"),
    );
    let second = ThreeNodeRaftSim::run_http_live_protocol_surface_report(
        ThreeNodeRaftSimConfig::new(56, "ursula-sim-http-live-protocol"),
    );

    assert_eq!(first, second);
    assert_eq!(first.scenario, SimScenario::HttpLiveProtocolSurface);
    assert_eq!(first.outcome.seed, 56);
    assert!(first.outcome.trace.events.iter().any(|event| matches!(
        event,
        SimEvent::HttpLiveProtocolSurfaceVerified {
            long_poll_next_offset: 4,
            sse_next_offset: 7,
            ..
        }
    )));
}

#[test]
#[ignore = "diagnostic: madsim process-global state makes scenario tests safer to run individually"]
fn http_protocol_surface_workload_replays_with_same_seed_and_trace() {
    let _guard = sim_test_guard();
    let first = ThreeNodeRaftSim::run_http_protocol_surface_report(ThreeNodeRaftSimConfig::new(
        57,
        "ursula-sim-http-protocol",
    ));
    let second = ThreeNodeRaftSim::run_http_protocol_surface_report(ThreeNodeRaftSimConfig::new(
        57,
        "ursula-sim-http-protocol",
    ));

    assert_eq!(first, second);
    assert_eq!(first.scenario, SimScenario::HttpProtocolSurface);
    assert_eq!(first.outcome.seed, 57);
    assert!(first.outcome.trace.events.iter().any(|event| matches!(
        event,
        SimEvent::HttpProtocolSurfaceVerified {
            next_offset: 2,
            expired_at_ms: 2_000,
            ..
        }
    )));
}

#[test]
#[ignore = "diagnostic: madsim process-global state makes scenario tests safer to run individually"]
fn http_protocol_surface_randomized_workload_replays_with_same_seed_and_trace() {
    let _guard = sim_test_guard();
    let first = ThreeNodeRaftSim::run_http_protocol_surface_randomized_report(
        ThreeNodeRaftSimConfig::new(277, "ursula-sim-http-randomized-protocol"),
    );
    let second = ThreeNodeRaftSim::run_http_protocol_surface_randomized_report(
        ThreeNodeRaftSimConfig::new(277, "ursula-sim-http-randomized-protocol"),
    );

    assert_eq!(first, second);
    assert_eq!(first.scenario, SimScenario::HttpProtocolSurfaceRandomized);
    assert_eq!(first.outcome.seed, 277);
    assert!(first.outcome.trace.events.iter().any(|event| matches!(
        event,
        SimEvent::HttpProtocolSurfaceRandomizedVerified {
            final_next_offset: 4,
            ttl_checked: true,
            long_poll: true,
            ..
        }
    )));
}

#[test]
#[ignore = "diagnostic: madsim process-global state makes scenario tests safer to run individually"]
fn cold_read_delay_workload_replays_with_same_seed_and_trace() {
    let _guard = sim_test_guard();
    let first = ThreeNodeRaftSim::run_cold_read_delay_report(ThreeNodeRaftSimConfig::new(
        67,
        "ursula-sim-cold-read-delay",
    ));
    let second = ThreeNodeRaftSim::run_cold_read_delay_report(ThreeNodeRaftSimConfig::new(
        67,
        "ursula-sim-cold-read-delay",
    ));

    assert_eq!(first, second);
    assert_eq!(first.scenario, SimScenario::ColdReadDelay);
    assert_eq!(first.outcome.seed, 67);
    assert!(
        first
            .outcome
            .trace
            .events
            .iter()
            .any(|event| matches!(event, SimEvent::ColdReadDelayVerified { .. }))
    );
}

#[test]
#[ignore = "diagnostic: madsim process-global state makes scenario tests safer to run individually"]
fn cold_read_truncate_workload_replays_with_same_seed_and_trace() {
    let _guard = sim_test_guard();
    let first = ThreeNodeRaftSim::run_cold_read_truncate_report(ThreeNodeRaftSimConfig::new(
        68,
        "ursula-sim-cold-read-truncate",
    ));
    let second = ThreeNodeRaftSim::run_cold_read_truncate_report(ThreeNodeRaftSimConfig::new(
        68,
        "ursula-sim-cold-read-truncate",
    ));

    assert_eq!(first, second);
    assert_eq!(first.scenario, SimScenario::ColdReadTruncate);
    assert_eq!(first.outcome.seed, 68);
    assert!(
        first
            .outcome
            .trace
            .events
            .iter()
            .any(|event| matches!(event, SimEvent::ColdReadTruncateObserved { .. }))
    );
}

#[test]
#[ignore = "diagnostic: madsim process-global state makes scenario tests safer to run individually"]
fn runtime_actor_scheduling_workload_replays_with_same_seed_and_trace() {
    let _guard = sim_test_guard();
    let first = ThreeNodeRaftSim::run_runtime_actor_scheduling_report(ThreeNodeRaftSimConfig::new(
        69,
        "ursula-sim-runtime-actor-scheduling",
    ));
    let second = ThreeNodeRaftSim::run_runtime_actor_scheduling_report(
        ThreeNodeRaftSimConfig::new(69, "ursula-sim-runtime-actor-scheduling"),
    );

    assert_eq!(first, second);
    assert_eq!(first.scenario, SimScenario::RuntimeActorScheduling);
    assert_eq!(first.outcome.seed, 69);
    assert!(
        first
            .outcome
            .trace
            .events
            .iter()
            .any(|event| matches!(event, SimEvent::RuntimeWaitReadSatisfied { .. }))
    );
    assert!(
        first
            .outcome
            .trace
            .events
            .iter()
            .any(|event| matches!(event, SimEvent::RuntimeReadVerified { .. }))
    );
}

#[test]
#[ignore = "diagnostic: madsim process-global state makes scenario tests safer to run individually"]
fn runtime_multi_client_actor_workload_replays_with_same_seed_and_trace() {
    let _guard = sim_test_guard();
    let first = ThreeNodeRaftSim::run_runtime_multi_client_actors_report(
        ThreeNodeRaftSimConfig::new(70, "ursula-sim-runtime-multi-client-actors"),
    );
    let second = ThreeNodeRaftSim::run_runtime_multi_client_actors_report(
        ThreeNodeRaftSimConfig::new(70, "ursula-sim-runtime-multi-client-actors"),
    );

    assert_eq!(first, second);
    assert_eq!(first.scenario, SimScenario::RuntimeMultiClientActors);
    assert_eq!(first.outcome.seed, 70);
    assert!(
        first
            .outcome
            .trace
            .events
            .iter()
            .any(|event| matches!(event, SimEvent::RuntimeMultiClientVerified { .. }))
    );
}

#[test]
#[ignore = "diagnostic: madsim process-global state makes scenario tests safer to run individually"]
fn runtime_cold_flush_worker_workload_replays_with_same_seed_and_trace() {
    let _guard = sim_test_guard();
    let first = ThreeNodeRaftSim::run_runtime_cold_flush_worker_report(
        ThreeNodeRaftSimConfig::new(71, "ursula-sim-runtime-cold-flush-worker"),
    );
    let second = ThreeNodeRaftSim::run_runtime_cold_flush_worker_report(
        ThreeNodeRaftSimConfig::new(71, "ursula-sim-runtime-cold-flush-worker"),
    );

    assert_eq!(first, second);
    assert_eq!(first.scenario, SimScenario::RuntimeColdFlushWorker);
    assert_eq!(first.outcome.seed, 71);
    assert!(
        first
            .outcome
            .trace
            .events
            .iter()
            .any(|event| matches!(event, SimEvent::RuntimeColdFlushCompleted { .. }))
    );
    assert_eq!(
        first
            .outcome
            .trace
            .events
            .iter()
            .filter(|event| matches!(event, SimEvent::RuntimeColdLiveReadVerified { .. }))
            .count(),
        2
    );
}

#[test]
#[ignore = "diagnostic: madsim process-global state makes scenario tests safer to run individually"]
fn runtime_seeded_interleaving_workload_replays_with_same_seed_and_trace() {
    let _guard = sim_test_guard();
    let first = ThreeNodeRaftSim::run_runtime_seeded_interleaving_report(
        ThreeNodeRaftSimConfig::new(72, "ursula-sim-runtime-seeded-interleaving"),
    );
    let second = ThreeNodeRaftSim::run_runtime_seeded_interleaving_report(
        ThreeNodeRaftSimConfig::new(72, "ursula-sim-runtime-seeded-interleaving"),
    );

    assert_eq!(first, second);
    assert_eq!(first.scenario, SimScenario::RuntimeSeededInterleaving);
    assert_eq!(first.outcome.seed, 72);
    assert!(
        first
            .outcome
            .trace
            .events
            .iter()
            .any(|event| matches!(event, SimEvent::RuntimeInterleavingFlushCompleted { .. }))
    );
    assert!(
        first
            .outcome
            .trace
            .events
            .iter()
            .any(|event| matches!(event, SimEvent::RuntimeInterleavingVerified { .. }))
    );
}

#[test]
#[ignore = "diagnostic: madsim process-global state makes scenario tests safer to run individually"]
fn runtime_raft_engine_workload_replays_with_same_seed_and_trace() {
    let _guard = sim_test_guard();
    let first = ThreeNodeRaftSim::run_runtime_raft_engine_report(ThreeNodeRaftSimConfig::new(
        97,
        "ursula-sim-runtime-raft-engine",
    ));
    let second = ThreeNodeRaftSim::run_runtime_raft_engine_report(ThreeNodeRaftSimConfig::new(
        97,
        "ursula-sim-runtime-raft-engine",
    ));

    assert_eq!(first, second);
    assert_eq!(first.scenario, SimScenario::RuntimeRaftEngine);
    assert_eq!(first.outcome.seed, 97);
    assert_eq!(first.outcome.leader_id, 1);
    assert!(first.outcome.appended_log_index > 0);
    assert!(
        first
            .outcome
            .trace
            .events
            .iter()
            .any(|event| matches!(event, SimEvent::RuntimeRaftEngineBuilt { .. }))
    );
    assert!(
        first
            .outcome
            .trace
            .events
            .iter()
            .any(|event| matches!(event, SimEvent::RuntimeRaftEngineReadVerified { .. }))
    );
}

#[test]
#[ignore = "diagnostic: madsim process-global state makes scenario tests safer to run individually"]
fn runtime_raft_network_workload_replays_with_same_seed_and_trace() {
    let _guard = sim_test_guard();
    let first = ThreeNodeRaftSim::run_runtime_raft_network_report(ThreeNodeRaftSimConfig::new(
        102,
        "ursula-sim-runtime-raft-network",
    ));
    let second = ThreeNodeRaftSim::run_runtime_raft_network_report(ThreeNodeRaftSimConfig::new(
        102,
        "ursula-sim-runtime-raft-network",
    ));

    assert_eq!(first, second);
    assert_eq!(first.scenario, SimScenario::RuntimeRaftNetwork);
    assert_eq!(first.outcome.seed, 102);
    assert!(first.outcome.appended_log_index > 0);
    assert!(
        first
            .outcome
            .trace
            .events
            .iter()
            .any(|event| matches!(event, SimEvent::RuntimeRaftNetworkBuilt { .. }))
    );
    assert!(first.outcome.trace.events.iter().any(|event| matches!(
        event,
        SimEvent::RuntimeRaftNetworkReadVerified {
            delivered_rpc_count,
            ..
        } if *delivered_rpc_count > 0
    )));
}

#[test]
#[ignore = "diagnostic: netem-delay style runtime Raft snapshot/purge stress"]
fn runtime_raft_network_delay_snapshot_purge_probe() {
    let _guard = sim_test_guard();
    for seed in 917..=924 {
        for (delay_index, delay_ms) in [25_u64, 75, 125].into_iter().enumerate() {
            let sim_seed = seed * 10 + u64::try_from(delay_index).expect("delay index fits u64");
            run_with_madsim(sim_seed, async move {
                let policy = sim_network_policy();
                let factory = MadsimRuntimeRaftNetworkFactory::new(sim_seed, policy.clone())
                    .with_aggressive_snapshot_purge();
                let mut runtime_config = RuntimeConfig::new(1, 1);
                runtime_config.threading = RuntimeThreading::HostedTokio;
                let runtime =
                    ShardRuntime::spawn_with_engine_factory(runtime_config, factory.clone())
                        .expect("spawn diagnostic runtime raft network");
                let stream = BucketStreamId::new(
                    "benchcmp",
                    format!("runtime-raft-delay-snapshot-purge-{sim_seed}-{delay_ms}"),
                );
                runtime
                    .create_stream(CreateStreamRequest::new(
                        stream.clone(),
                        "application/octet-stream",
                    ))
                    .await
                    .expect("create diagnostic runtime raft stream");
                let placement = runtime.locate(&stream);
                let initial_leader_id = factory
                    .leader_id(placement.raft_group_id)
                    .expect("diagnostic runtime raft initial leader");
                let isolated_id = seeded_follower_id(sim_seed, initial_leader_id);
                policy.partition_bidirectional(initial_leader_id, isolated_id);

                policy.set_delay(Some(Duration::from_millis(delay_ms)));
                let mut tasks = Vec::new();
                for append_id in 0..96_u64 {
                    let runtime = runtime.clone();
                    let stream = stream.clone();
                    tasks.push(madsim::task::spawn(async move {
                        if append_id % 8 != 0 {
                            madsim::time::sleep(Duration::from_millis((append_id % 8) * 3)).await;
                        }
                        runtime
                            .append_batch(AppendBatchRequest::new(stream, vec![
                                format!("delay-{append_id};").into_bytes(),
                            ]))
                            .await
                    }));
                }

                for _ in 0..24 {
                    madsim::time::sleep(Duration::from_millis(25)).await;
                    for node_id in 1..=3 {
                        let Some(raft) = factory.raft_handle(node_id) else {
                            continue;
                        };
                        let _ = raft.trigger().snapshot().await;
                        if let Some(last_log_index) =
                            factory.log_store_last_log_index(node_id).await
                        {
                            let _ = raft.trigger().purge_log(last_log_index).await;
                        }
                    }
                }

                policy.heal_bidirectional(initial_leader_id, isolated_id);
                for _ in 0..16 {
                    madsim::time::sleep(Duration::from_millis(50)).await;
                    for node_id in 1..=3 {
                        if let Some(raft) = factory.raft_handle(node_id) {
                            let _ = raft.trigger().heartbeat().await;
                            let _ = raft.trigger().snapshot().await;
                        }
                    }
                }
                policy.clear();
                madsim::time::sleep(Duration::from_millis(500)).await;

                let mut successful_items = 0usize;
                let mut nonfatal_errors = 0usize;
                for task in tasks {
                    let joined = madsim::time::timeout(Duration::from_secs(10), task)
                        .await
                        .expect("diagnostic delayed append task timed out")
                        .expect("diagnostic delayed append task panicked");
                    match joined {
                        Ok(batch) => match batch.items.into_iter().collect::<Result<Vec<_>, _>>() {
                            Ok(items) => successful_items += items.len(),
                            Err(err) => {
                                let err = format!("{err:?}");
                                assert!(
                                    !err.contains("panicked"),
                                    "OpenRaft panicked during delayed append item: {err}"
                                );
                                nonfatal_errors += 1;
                            }
                        },
                        Err(err) => {
                            let err = format!("{err:?}");
                            assert!(
                                !err.contains("panicked"),
                                "OpenRaft panicked during delayed append: {err}"
                            );
                            nonfatal_errors += 1;
                        }
                    }
                }
                assert!(
                    successful_items > 0,
                    "seed {sim_seed} delay {delay_ms}ms should complete at least one delayed append; nonfatal_errors={nonfatal_errors}"
                );

                let observer = factory.raft_handle(1).expect("diagnostic raft node 1");
                let current_leader = observer
                    .wait(Some(Duration::from_secs(5)))
                    .metrics(
                        |metrics| metrics.current_leader.is_some(),
                        "observe leader after clearing raft delay",
                    )
                    .await
                    .expect("observe leader after clearing raft delay")
                    .current_leader
                    .expect("current leader after clearing raft delay");
                let leader_raft = factory
                    .raft_handle(current_leader)
                    .expect("diagnostic current leader raft handle");
                let probe_payload = format!("after-delay-{sim_seed}-{delay_ms};").into_bytes();
                let probe = leader_raft
                    .client_write(
                        GroupWriteCommand::from(AppendBatchRequest::new(stream, vec![
                            probe_payload,
                        ]))
                        .into(),
                    )
                    .await;
                if let Err(err) = probe {
                    let err = format!("{err:?}");
                    assert!(
                        !err.contains("panicked"),
                        "OpenRaft panicked during post-delay leader probe: {err}"
                    );
                }
                let trace = SimTrace::last_recorded();
                let full_snapshot_decisions = trace
                    .events
                    .iter()
                    .filter(|event| {
                        matches!(
                            event,
                            SimEvent::NetworkRpcDecision { kind, .. } if kind == "full_snapshot"
                        )
                    })
                    .count();
                assert!(
                    full_snapshot_decisions > 0,
                    "seed {sim_seed} delay {delay_ms}ms should attempt at least one full_snapshot"
                );
            });
        }
    }
}

#[test]
#[ignore = "diagnostic: madsim process-global state makes scenario tests safer to run individually"]
fn runtime_raft_snapshot_install_workload_replays_with_same_seed_and_trace() {
    let _guard = sim_test_guard();
    let first = ThreeNodeRaftSim::run_runtime_raft_snapshot_install_report(
        ThreeNodeRaftSimConfig::new(132, "ursula-sim-runtime-raft-snapshot-install"),
    );
    let second = ThreeNodeRaftSim::run_runtime_raft_snapshot_install_report(
        ThreeNodeRaftSimConfig::new(132, "ursula-sim-runtime-raft-snapshot-install"),
    );

    assert_eq!(first, second);
    assert_eq!(first.scenario, SimScenario::RuntimeRaftSnapshotInstall);
    assert_eq!(first.outcome.seed, 132);
    assert!(first.outcome.appended_log_index > 0);
    assert!(
        first
            .outcome
            .trace
            .events
            .iter()
            .any(|event| matches!(event, SimEvent::RuntimeRaftSnapshotCaptured { .. }))
    );
    assert!(
        first
            .outcome
            .trace
            .events
            .iter()
            .any(|event| matches!(event, SimEvent::RuntimeRaftSnapshotInstalledVerified { .. }))
    );
}

#[test]
#[ignore = "diagnostic: madsim process-global state makes scenario tests safer to run individually"]
fn leader_failover_workload_replays_with_same_seed_and_trace() {
    let _guard = sim_test_guard();
    let first = ThreeNodeRaftSim::run_leader_failover_report(ThreeNodeRaftSimConfig::new(
        122,
        "ursula-sim-leader-failover",
    ));
    let second = ThreeNodeRaftSim::run_leader_failover_report(ThreeNodeRaftSimConfig::new(
        122,
        "ursula-sim-leader-failover",
    ));

    assert_eq!(first, second);
    assert_eq!(first.scenario, SimScenario::LeaderFailover);
    assert_eq!(first.outcome.seed, 122);
    assert!(first.outcome.target_node_id.is_some());
    assert!(first.outcome.appended_log_index > 0);
    assert!(
        first
            .outcome
            .trace
            .events
            .iter()
            .any(|event| matches!(event, SimEvent::LeaderFailoverAppendVerified { .. }))
    );
    assert!(
        first
            .outcome
            .trace
            .events
            .iter()
            .any(|event| matches!(event, SimEvent::LeaderFailoverReadVerified { .. }))
    );
    assert!(
        first
            .outcome
            .trace
            .events
            .iter()
            .any(|event| matches!(event, SimEvent::NodeStopped { .. }))
    );
    assert!(
        first
            .outcome
            .trace
            .events
            .iter()
            .any(|event| matches!(event, SimEvent::NodeRestarted { .. }))
    );
}

#[test]
#[ignore = "diagnostic: madsim process-global state makes scenario tests safer to run individually"]
fn runtime_raft_network_recovery_workload_replays_with_same_seed_and_trace() {
    let _guard = sim_test_guard();
    let first = ThreeNodeRaftSim::run_runtime_raft_network_with_options_report(
        ThreeNodeRaftSimConfig::new(107, "ursula-sim-runtime-raft-network-recovery"),
        RuntimeRaftNetworkOptions {
            partition_before_append: true,
            heal_after_lag: true,
            ..Default::default()
        },
    );
    let second = ThreeNodeRaftSim::run_runtime_raft_network_with_options_report(
        ThreeNodeRaftSimConfig::new(107, "ursula-sim-runtime-raft-network-recovery"),
        RuntimeRaftNetworkOptions {
            partition_before_append: true,
            heal_after_lag: true,
            ..Default::default()
        },
    );

    assert_eq!(first, second);
    assert_eq!(first.scenario, SimScenario::RuntimeRaftNetwork);
    assert_eq!(first.outcome.seed, 107);
    assert!(first.outcome.target_node_id.is_some());
    assert!(
        first
            .outcome
            .trace
            .events
            .iter()
            .any(|event| matches!(event, SimEvent::IsolatedFollowerLagged { .. }))
    );
    assert!(
        first
            .outcome
            .trace
            .events
            .iter()
            .any(|event| matches!(event, SimEvent::FollowerCaughtUp { .. }))
    );
    assert!(
        first
            .outcome
            .trace
            .events
            .iter()
            .any(|event| matches!(event, SimEvent::RuntimeRaftNetworkReadVerified { .. }))
    );
    assert_eq!(
        first
            .outcome
            .trace
            .events
            .iter()
            .filter(|event| matches!(event, SimEvent::FaultApplied { .. }))
            .count(),
        2
    );
}

#[test]
#[ignore = "diagnostic: madsim process-global state makes scenario tests safer to run individually"]
fn runtime_raft_network_leader_failover_workload_replays_with_same_seed_and_trace() {
    let _guard = sim_test_guard();
    let first = ThreeNodeRaftSim::run_runtime_raft_network_with_options_report(
        ThreeNodeRaftSimConfig::new(127, "ursula-sim-runtime-raft-network-leader-failover"),
        RuntimeRaftNetworkOptions {
            leader_failover_after_read: true,
            ..Default::default()
        },
    );
    let second = ThreeNodeRaftSim::run_runtime_raft_network_with_options_report(
        ThreeNodeRaftSimConfig::new(127, "ursula-sim-runtime-raft-network-leader-failover"),
        RuntimeRaftNetworkOptions {
            leader_failover_after_read: true,
            ..Default::default()
        },
    );

    assert_eq!(first, second);
    assert_eq!(first.scenario, SimScenario::RuntimeRaftNetwork);
    assert_eq!(first.outcome.seed, 127);
    assert!(first.outcome.target_node_id.is_some());
    assert!(first.outcome.trace.events.iter().any(|event| matches!(
        event,
        SimEvent::RuntimeRaftNetworkLeaderFailoverVerified { .. }
    )));
    assert!(first.outcome.trace.events.iter().any(|event| matches!(
        event,
        SimEvent::RuntimeRaftNetworkLeaderFailoverReadVerified { .. }
    )));
}

#[test]
#[ignore = "diagnostic: madsim process-global state makes scenario tests safer to run individually"]
fn runtime_raft_network_cold_live_recovery_workload_replays_with_same_seed_and_trace() {
    let _guard = sim_test_guard();
    let first = ThreeNodeRaftSim::run_runtime_raft_network_with_options_report(
        ThreeNodeRaftSimConfig::new(112, "ursula-sim-runtime-raft-network-cold-live-recovery"),
        RuntimeRaftNetworkOptions {
            partition_before_append: true,
            heal_after_lag: true,
            verify_cold_live_read: true,
            ..Default::default()
        },
    );
    let second = ThreeNodeRaftSim::run_runtime_raft_network_with_options_report(
        ThreeNodeRaftSimConfig::new(112, "ursula-sim-runtime-raft-network-cold-live-recovery"),
        RuntimeRaftNetworkOptions {
            partition_before_append: true,
            heal_after_lag: true,
            verify_cold_live_read: true,
            ..Default::default()
        },
    );

    assert_eq!(first, second);
    assert_eq!(first.scenario, SimScenario::RuntimeRaftNetwork);
    assert_eq!(first.outcome.seed, 112);
    assert!(first.outcome.target_node_id.is_some());
    assert!(
        first
            .outcome
            .trace
            .events
            .iter()
            .any(|event| matches!(event, SimEvent::FollowerCaughtUp { .. }))
    );
    assert!(first.outcome.trace.events.iter().any(|event| {
        matches!(
            event,
            SimEvent::RuntimeRaftNetworkColdLiveReadVerified { .. }
        )
    }));
}

#[test]
#[ignore = "diagnostic: covered by smoke_corpus_replays in the default madsim suite"]
fn regression_record_round_trips_and_replays() {
    let _guard = sim_test_guard();
    let config = ThreeNodeRaftSimConfig::new(41, "ursula-sim-record");
    let report = ThreeNodeRaftSim::run_no_fault_report(config.clone());
    let record = SimRegressionRecord::new(&config, report);

    let encoded = serde_json::to_string_pretty(&record).expect("serialize sim record");
    let decoded =
        serde_json::from_str::<SimRegressionRecord>(&encoded).expect("deserialize sim record");

    assert_eq!(decoded, record);
    decoded.assert_replays();
}

#[test]
#[ignore = "diagnostic: covered by smoke_corpus_replays in the default madsim suite"]
fn scheduled_record_round_trips_and_replays() {
    let _guard = sim_test_guard();
    let schedule = SimSchedule::for_scenario(64, SimScenario::ColdLiveRead);
    let record = SimScheduledRecord::new(schedule.clone(), schedule.run());

    let encoded = serde_json::to_string_pretty(&record).expect("serialize schedule record");
    let decoded =
        serde_json::from_str::<SimScheduledRecord>(&encoded).expect("deserialize schedule record");

    assert_eq!(decoded, record);
    decoded.assert_replays();
    assert_eq!(
        decoded.schedule.fault_plan,
        SimFaultPlan::for_scenario(SimScenario::ColdLiveRead)
    );
}

#[test]
fn runtime_raft_randomized_seed_sets_cover_key_branches() {
    assert_runtime_raft_randomized_seed_sets_cover_key_branches();
}

fn assert_runtime_raft_randomized_seed_sets_cover_key_branches() {
    let pr_schedules = (137..=140)
        .map(SimSchedule::generate_runtime_raft_network_randomized)
        .collect::<Vec<_>>();

    assert!(
        pr_schedules.iter().any(has_partition_heal),
        "PR runtime/Raft randomized seeds should cover partition/heal"
    );
    assert!(
        pr_schedules.iter().any(has_leader_failover),
        "PR runtime/Raft randomized seeds should cover leader failover"
    );
    assert!(
        pr_schedules
            .iter()
            .map(runtime_raft_network_workload_plan)
            .any(|plan| plan.stream_count > 1),
        "PR runtime/Raft randomized seeds should cover multi-stream workloads"
    );
    assert!(
        pr_schedules
            .iter()
            .map(runtime_raft_network_workload_plan)
            .any(|plan| plan.producer_sessions),
        "PR runtime/Raft randomized seeds should cover producer sessions"
    );
    assert!(
        pr_schedules
            .iter()
            .map(runtime_raft_network_workload_plan)
            .any(|plan| plan.concurrent_producers),
        "PR runtime/Raft randomized seeds should cover concurrent producers"
    );
    assert!(
        pr_schedules
            .iter()
            .map(runtime_raft_network_workload_plan)
            .any(|plan| plan.partial_reads),
        "PR runtime/Raft randomized seeds should cover partial reads"
    );
    assert!(
        pr_schedules
            .iter()
            .map(runtime_raft_network_workload_plan)
            .any(|plan| plan.tail_reads),
        "PR runtime/Raft randomized seeds should cover tail reads"
    );
    assert!(
        pr_schedules
            .iter()
            .map(runtime_raft_network_workload_plan)
            .any(|plan| plan.close_streams),
        "PR runtime/Raft randomized seeds should cover close streams"
    );
    assert!(
        pr_schedules
            .iter()
            .map(runtime_raft_network_workload_plan)
            .any(|plan| plan.publish_snapshots),
        "PR runtime/Raft randomized seeds should cover snapshot publish/read"
    );
    assert!(
        pr_schedules.iter().any(has_cold_write_retry),
        "PR runtime/Raft randomized seeds should cover cold-write retry"
    );

    let retry_cold_read_pr_seeds = retry_cold_read_seeds(137..=140);
    assert_eq!(retry_cold_read_pr_seeds, vec![140]);

    let nightly_schedules = (137..=156)
        .map(SimSchedule::generate_runtime_raft_network_randomized)
        .collect::<Vec<_>>();
    assert!(
        nightly_schedules
            .iter()
            .map(runtime_raft_network_workload_plan)
            .any(|plan| plan.producer_epoch_bumps),
        "nightly runtime/Raft randomized seeds should cover producer epoch bumps"
    );
    assert_eq!(retry_cold_read_seeds(137..=156), vec![140, 150, 155]);
    assert_eq!(cold_write_delay_seeds(137..=156), vec![146]);
    assert_eq!(cold_read_delay_seeds(137..=156), vec![147]);
}

fn runtime_raft_network_workload_plan(schedule: &SimSchedule) -> &RuntimeRaftNetworkWorkloadPlan {
    schedule
        .fault_plan
        .steps
        .iter()
        .find_map(|step| match &step.action {
            SimFaultAction::RunRuntimeRaftNetworkWorkload { plan } => Some(plan),
            _ => None,
        })
        .expect("runtime/Raft network workload plan")
}

fn has_partition_heal(schedule: &SimSchedule) -> bool {
    has_action(schedule, |action| {
        matches!(action, SimFaultAction::PartitionSeededFollower)
    }) && has_action(schedule, |action| {
        matches!(action, SimFaultAction::HealSeededFollower)
    })
}

fn has_leader_failover(schedule: &SimSchedule) -> bool {
    has_action(schedule, |action| {
        matches!(action, SimFaultAction::StopCurrentLeader)
    }) && has_action(schedule, |action| {
        matches!(action, SimFaultAction::RestartStoppedLeader)
    })
}

fn has_cold_write_retry(schedule: &SimSchedule) -> bool {
    has_action(schedule, |action| {
        matches!(action, SimFaultAction::FailNextColdWrite)
    }) && has_action(schedule, |action| {
        matches!(action, SimFaultAction::RetryColdWriteAfterFailure)
    })
}

fn retry_cold_read_seeds(seeds: std::ops::RangeInclusive<u64>) -> Vec<u64> {
    seeds
        .filter(|seed| {
            let schedule = SimSchedule::generate_runtime_raft_network_randomized(*seed);
            has_action(&schedule, |action| {
                matches!(action, SimFaultAction::TruncateNextColdRead {
                    returned_len: 0
                })
            }) && has_action(&schedule, |action| {
                matches!(action, SimFaultAction::RetryColdReadAfterFailure)
            })
        })
        .collect()
}

fn cold_read_delay_seeds(seeds: std::ops::RangeInclusive<u64>) -> Vec<u64> {
    seeds
        .filter(|seed| {
            let schedule = SimSchedule::generate_runtime_raft_network_randomized(*seed);
            has_action(&schedule, |action| {
                matches!(action, SimFaultAction::DelayNextColdRead { delay_ms: 125 })
            })
        })
        .collect()
}

fn cold_write_delay_seeds(seeds: std::ops::RangeInclusive<u64>) -> Vec<u64> {
    seeds
        .filter(|seed| {
            let schedule = SimSchedule::generate_runtime_raft_network_randomized(*seed);
            has_action(&schedule, |action| {
                matches!(action, SimFaultAction::DelayNextColdWrite { delay_ms: 125 })
            })
        })
        .collect()
}

fn has_action(
    schedule: &SimSchedule,
    mut matches_action: impl FnMut(&SimFaultAction) -> bool,
) -> bool {
    schedule
        .fault_plan
        .steps
        .iter()
        .any(|step| matches_action(&step.action))
}

#[test]
fn failure_corpus_covers_expected_invariants_and_seeds() {
    assert_failure_corpus_covers_expected_invariants_and_seeds();
}

fn assert_failure_corpus_covers_expected_invariants_and_seeds() {
    let failure_corpus = include_str!("../../corpus/failure-smoke.json");
    let failure_records = serde_json::from_str::<Vec<SimFailureRegressionRecord>>(failure_corpus)
        .expect("parse failure smoke corpus");
    let actual = failure_records
        .iter()
        .map(|record| (record.seed, record.invariant.as_str()))
        .collect::<BTreeSet<_>>();
    let expected = BTreeSet::from([
        (192, "runtime_interleaving_cold_write_integrity"),
        (222, "runtime_raft_network_cold_live_read_integrity"),
        (232, "runtime_raft_snapshot_install_integrity"),
        (244, "runtime_raft_network_read_your_write"),
        (248, "runtime_raft_network_partial_read_integrity"),
        (253, "runtime_raft_network_leader_failover_no_loss_or_dup"),
        (262, "http_producer_retry_idempotence"),
        (267, "http_live_sse_delivery"),
        (272, "http_live_waiter_backpressure"),
        (297, "http_protocol_randomized_read_your_write"),
        (302, "http_protocol_randomized_sse_delivery"),
        (307, "http_protocol_randomized_live_waiter_backpressure"),
        (312, "runtime_raft_network_cold_live_write_integrity"),
        (322, "runtime_raft_network_cold_live_read_integrity"),
        (332, "http_snapshot_protocol_surface_read"),
        (337, "runtime_raft_network_tail_read_empty"),
        (342, "runtime_raft_network_close_state"),
        (347, "runtime_raft_network_snapshot_publish_read"),
    ]);

    assert_eq!(actual, expected);

    assert_failure_record_actions(&failure_records, 192, |actions| {
        assert_eq!(actions.len(), 1);
        let plan = match actions[0] {
            SimFaultAction::RunRuntimeSeededInterleaving { plan } => plan,
            other => panic!("seed 192 should run runtime interleaving, got {other:?}"),
        };
        assert_eq!(plan.clients.len(), 1);
        assert_eq!(plan.clients[0].client_id, 0);
        assert_eq!(plan.clients[0].stream_index, 0);
        assert_eq!(plan.clients[0].first_append_delay_ms, 0);
        assert_eq!(plan.clients[0].second_append_delay_ms, 0);
        assert_eq!(plan.flush_delay_ms, 0);
        assert_eq!(plan.read_verify_delay_ms, 0);
        assert_eq!(plan.flush_group_limit, 1);
        assert_eq!(
            plan.runtime_cold_write_failure.as_deref(),
            Some("seeded runtime cold write fault for seed 192")
        );
        assert!(plan.panic_after.is_none());
        assert!(plan.corrupt_read_client_id.is_none());
        assert!(plan.runtime_cold_read_delay_ms.is_none());
        assert!(plan.runtime_cold_read_truncate_len.is_none());
    });
    assert_failure_record_actions(&failure_records, 222, |actions| {
        assert_eq!(actions.len(), 2);
        assert!(matches!(
            actions[0],
            SimFaultAction::VerifyRuntimeColdLiveReads
        ));
        assert!(matches!(actions[1], SimFaultAction::TruncateNextColdRead {
            returned_len: 0
        }));
    });
    assert_failure_record_actions(&failure_records, 232, |actions| {
        assert_eq!(actions.len(), 1);
        assert!(matches!(
            actions[0],
            SimFaultAction::CorruptRuntimeRaftSnapshotAppendCounts
        ));
    });
    assert_failure_record_actions(&failure_records, 244, |actions| {
        assert_eq!(actions.len(), 1);
        let plan = expect_runtime_raft_network_workload(actions[0], 244);
        assert_minimized_runtime_raft_workload_plan(plan);
        assert!(plan.corrupt_read_expectation);
        assert!(!plan.partial_reads);
        assert!(!plan.corrupt_partial_read_expectation);
        assert!(!plan.tail_reads);
        assert!(!plan.corrupt_tail_read_expectation);
        assert!(!plan.close_streams);
        assert!(!plan.corrupt_close_state_expectation);
        assert!(!plan.publish_snapshots);
        assert!(!plan.corrupt_snapshot_expectation);
        assert!(!plan.corrupt_leader_failover_read_expectation);
    });
    assert_failure_record_actions(&failure_records, 248, |actions| {
        assert_eq!(actions.len(), 1);
        let plan = expect_runtime_raft_network_workload(actions[0], 248);
        assert_minimized_runtime_raft_workload_plan(plan);
        assert!(!plan.corrupt_read_expectation);
        assert!(plan.partial_reads);
        assert!(plan.corrupt_partial_read_expectation);
        assert!(!plan.tail_reads);
        assert!(!plan.corrupt_tail_read_expectation);
        assert!(!plan.close_streams);
        assert!(!plan.corrupt_close_state_expectation);
        assert!(!plan.publish_snapshots);
        assert!(!plan.corrupt_snapshot_expectation);
        assert!(!plan.corrupt_leader_failover_read_expectation);
    });
    assert_failure_record_actions(&failure_records, 337, |actions| {
        assert_eq!(actions.len(), 1);
        let plan = expect_runtime_raft_network_workload(actions[0], 337);
        assert_minimized_runtime_raft_workload_plan(plan);
        assert!(!plan.corrupt_read_expectation);
        assert!(!plan.partial_reads);
        assert!(!plan.corrupt_partial_read_expectation);
        assert!(plan.tail_reads);
        assert!(plan.corrupt_tail_read_expectation);
        assert!(!plan.close_streams);
        assert!(!plan.corrupt_close_state_expectation);
        assert!(!plan.publish_snapshots);
        assert!(!plan.corrupt_snapshot_expectation);
        assert!(!plan.corrupt_leader_failover_read_expectation);
    });
    assert_failure_record_actions(&failure_records, 342, |actions| {
        assert_eq!(actions.len(), 1);
        let plan = expect_runtime_raft_network_workload(actions[0], 342);
        assert_minimized_runtime_raft_workload_plan(plan);
        assert!(!plan.corrupt_read_expectation);
        assert!(!plan.partial_reads);
        assert!(!plan.corrupt_partial_read_expectation);
        assert!(!plan.tail_reads);
        assert!(!plan.corrupt_tail_read_expectation);
        assert!(plan.close_streams);
        assert!(plan.corrupt_close_state_expectation);
        assert!(!plan.publish_snapshots);
        assert!(!plan.corrupt_snapshot_expectation);
        assert!(!plan.corrupt_leader_failover_read_expectation);
    });
    assert_failure_record_actions(&failure_records, 347, |actions| {
        assert_eq!(actions.len(), 1);
        let plan = expect_runtime_raft_network_workload(actions[0], 347);
        assert_minimized_runtime_raft_workload_plan(plan);
        assert!(!plan.corrupt_read_expectation);
        assert!(!plan.partial_reads);
        assert!(!plan.corrupt_partial_read_expectation);
        assert!(!plan.tail_reads);
        assert!(!plan.corrupt_tail_read_expectation);
        assert!(!plan.close_streams);
        assert!(!plan.corrupt_close_state_expectation);
        assert!(plan.publish_snapshots);
        assert!(plan.corrupt_snapshot_expectation);
        assert!(!plan.corrupt_leader_failover_read_expectation);
    });
    assert_failure_record_actions(&failure_records, 253, |actions| {
        assert_eq!(actions.len(), 3);
        assert!(matches!(actions[0], SimFaultAction::StopCurrentLeader));
        assert!(matches!(actions[1], SimFaultAction::RestartStoppedLeader));
        let plan = expect_runtime_raft_network_workload(actions[2], 253);
        assert_minimized_runtime_raft_workload_plan(plan);
        assert!(!plan.corrupt_read_expectation);
        assert!(!plan.partial_reads);
        assert!(!plan.corrupt_partial_read_expectation);
        assert!(!plan.tail_reads);
        assert!(!plan.corrupt_tail_read_expectation);
        assert!(!plan.close_streams);
        assert!(!plan.corrupt_close_state_expectation);
        assert!(!plan.publish_snapshots);
        assert!(!plan.corrupt_snapshot_expectation);
        assert!(plan.corrupt_leader_failover_read_expectation);
    });
    assert_failure_record_actions(&failure_records, 262, |actions| {
        assert_eq!(actions.len(), 1);
        assert!(matches!(
            actions[0],
            SimFaultAction::CorruptHttpProducerDuplicateExpectation
        ));
    });
    assert_failure_record_actions(&failure_records, 267, |actions| {
        assert_eq!(actions.len(), 1);
        assert!(matches!(
            actions[0],
            SimFaultAction::CorruptHttpLiveSseNextOffsetExpectation
        ));
    });
    assert_failure_record_actions(&failure_records, 272, |actions| {
        assert_eq!(actions.len(), 1);
        assert!(matches!(
            actions[0],
            SimFaultAction::CorruptHttpLiveLimitBackpressureExpectation
        ));
    });
    assert_failure_record_actions(&failure_records, 332, |actions| {
        assert_eq!(actions.len(), 1);
        assert!(matches!(
            actions[0],
            SimFaultAction::CorruptHttpSnapshotBodyExpectation
        ));
    });
    assert_failure_record_actions(&failure_records, 297, |actions| {
        assert_eq!(actions.len(), 1);
        let plan = expect_http_protocol_surface_workload(actions[0], 297);
        assert_minimized_http_protocol_surface_plan(plan);
        assert!(plan.corrupt_final_read_expectation);
        assert!(!plan.corrupt_sse_next_offset_expectation);
        assert!(!plan.corrupt_live_limit_backpressure_expectation);
    });
    assert_failure_record_actions(&failure_records, 302, |actions| {
        assert_eq!(actions.len(), 1);
        let plan = expect_http_protocol_surface_workload(actions[0], 302);
        assert_minimized_http_protocol_surface_plan(plan);
        assert!(!plan.corrupt_final_read_expectation);
        assert!(plan.sse_close);
        assert!(plan.corrupt_sse_next_offset_expectation);
        assert!(!plan.corrupt_live_limit_backpressure_expectation);
    });
    assert_failure_record_actions(&failure_records, 307, |actions| {
        assert_eq!(actions.len(), 1);
        let plan = expect_http_protocol_surface_workload(actions[0], 307);
        assert_minimized_http_protocol_surface_plan(plan);
        assert!(!plan.corrupt_final_read_expectation);
        assert!(!plan.corrupt_sse_next_offset_expectation);
        assert!(plan.live_limit);
        assert!(plan.corrupt_live_limit_backpressure_expectation);
    });
    assert_failure_record_actions(&failure_records, 312, |actions| {
        assert_eq!(actions.len(), 2);
        assert!(matches!(
            actions[0],
            SimFaultAction::VerifyRuntimeColdLiveReads
        ));
        assert!(matches!(actions[1], SimFaultAction::FailNextColdWrite));
    });
    assert_failure_record_actions(&failure_records, 322, |actions| {
        assert_eq!(actions.len(), 3);
        assert!(matches!(
            actions[0],
            SimFaultAction::RunRuntimeRaftNetworkWorkload { .. }
        ));
        assert!(matches!(
            actions[1],
            SimFaultAction::VerifyRuntimeColdLiveReads
        ));
        assert!(matches!(actions[2], SimFaultAction::TruncateNextColdRead {
            returned_len: 0
        }));
    });
}

fn expect_runtime_raft_network_workload(
    action: &SimFaultAction,
    seed: u64,
) -> &RuntimeRaftNetworkWorkloadPlan {
    match action {
        SimFaultAction::RunRuntimeRaftNetworkWorkload { plan } => plan,
        other => panic!("seed {seed} should run runtime/Raft network workload, got {other:?}"),
    }
}

fn assert_minimized_runtime_raft_workload_plan(plan: &RuntimeRaftNetworkWorkloadPlan) {
    assert_eq!(plan.stream_count, 1);
    assert_eq!(plan.append_batch_lens, vec![1]);
    assert_eq!(plan.failover_batch_lens, vec![1]);
    assert!(!plan.producer_sessions);
    assert!(!plan.producer_epoch_bumps);
    assert!(!plan.concurrent_producers);
}

fn expect_http_protocol_surface_workload(
    action: &SimFaultAction,
    seed: u64,
) -> &HttpProtocolSurfacePlan {
    match action {
        SimFaultAction::RunHttpProtocolSurfaceWorkload { plan } => plan,
        other => panic!("seed {seed} should run HTTP protocol-surface workload, got {other:?}"),
    }
}

fn assert_minimized_http_protocol_surface_plan(plan: &HttpProtocolSurfacePlan) {
    assert!(!plan.ttl);
    assert!(!plan.producer_sessions);
    assert!(!plan.producer_sequence_gap);
    assert!(!plan.producer_epoch_bump);
    assert!(!plan.concurrent_producers);
    assert!(!plan.long_poll);
    assert!(!plan.live_timeout);
    assert!(!plan.partial_reads);
}

fn assert_failure_record_actions(
    records: &[SimFailureRegressionRecord],
    seed: u64,
    assert_actions: impl FnOnce(Vec<&SimFaultAction>),
) {
    let record = records
        .iter()
        .find(|record| record.seed == seed)
        .unwrap_or_else(|| panic!("failure corpus should include seed {seed}"));
    let actions = record
        .schedule
        .fault_plan
        .steps
        .iter()
        .map(|step| &step.action)
        .collect::<Vec<_>>();
    assert_actions(actions);
}

#[test]
fn schedule_corpus_covers_expected_scenarios_and_seeds() {
    assert_schedule_corpus_covers_expected_scenarios_and_seeds();
}

fn assert_schedule_corpus_covers_expected_scenarios_and_seeds() {
    let schedule_corpus = include_str!("../../corpus/schedule-smoke.json");
    let schedule_records = serde_json::from_str::<Vec<SimScheduledRecord>>(schedule_corpus)
        .expect("deserialize schedule corpus");
    let mut actual = schedule_records
        .iter()
        .map(|record| (record.schedule.seed, record.schedule.scenario))
        .collect::<Vec<_>>();
    actual.sort_by_key(|(seed, _scenario)| *seed);
    let expected = vec![
        (54, SimScenario::HttpProducerProtocolSurface),
        (55, SimScenario::HttpLiveLimitProtocolSurface),
        (56, SimScenario::HttpLiveProtocolSurface),
        (57, SimScenario::HttpProtocolSurface),
        (58, SimScenario::ColdDeleteFault),
        (59, SimScenario::ColdWriteDelay),
        (60, SimScenario::NoFaultBaseline),
        (61, SimScenario::PartitionHeal),
        (62, SimScenario::SnapshotCatchUp),
        (63, SimScenario::RestartFollower),
        (64, SimScenario::ColdLiveRead),
        (65, SimScenario::ColdReadFault),
        (66, SimScenario::ColdWriteFault),
        (67, SimScenario::ColdReadDelay),
        (68, SimScenario::ColdReadTruncate),
        (69, SimScenario::RuntimeActorScheduling),
        (70, SimScenario::RuntimeMultiClientActors),
        (71, SimScenario::RuntimeColdFlushWorker),
        (72, SimScenario::RuntimeSeededInterleaving),
        (137, SimScenario::RuntimeRaftNetwork),
        (146, SimScenario::RuntimeRaftNetwork),
        (147, SimScenario::RuntimeRaftNetwork),
        (155, SimScenario::RuntimeRaftNetwork),
        (277, SimScenario::HttpProtocolSurfaceRandomized),
        (281, SimScenario::HttpProtocolSurfaceRandomized),
        (285, SimScenario::HttpProtocolSurfaceRandomized),
        (317, SimScenario::RuntimeRaftNetwork),
    ];
    assert_eq!(actual, expected);

    let seed_146 = schedule_records
        .iter()
        .find(|record| record.schedule.seed == 146)
        .expect("schedule corpus seed 146");
    assert!(has_action(&seed_146.schedule, |action| matches!(
        action,
        SimFaultAction::DelayNextColdWrite { delay_ms: 125 }
    )));
    assert_eq!(
        seed_146
            .outcome
            .trace
            .events
            .iter()
            .filter(
                |event| matches!(event, SimEvent::RuntimeRaftNetworkColdWriteDelayVerified {
                    delay_ms: 125,
                    ..
                })
            )
            .count(),
        1
    );

    let seed_147 = schedule_records
        .iter()
        .find(|record| record.schedule.seed == 147)
        .expect("schedule corpus seed 147");
    assert!(has_action(&seed_147.schedule, |action| matches!(
        action,
        SimFaultAction::DelayNextColdRead { delay_ms: 125 }
    )));
    assert_eq!(
        seed_147
            .outcome
            .trace
            .events
            .iter()
            .filter(
                |event| matches!(event, SimEvent::RuntimeRaftNetworkColdReadDelayVerified {
                    delay_ms: 125,
                    ..
                })
            )
            .count(),
        1
    );

    let seed_155 = schedule_records
        .iter()
        .find(|record| record.schedule.seed == 155)
        .expect("schedule corpus seed 155");
    assert!(has_leader_failover(&seed_155.schedule));
    assert!(has_cold_write_retry(&seed_155.schedule));
    assert!(has_action(&seed_155.schedule, |action| matches!(
        action,
        SimFaultAction::TruncateNextColdRead { returned_len: 0 }
    )));
    assert!(has_action(&seed_155.schedule, |action| matches!(
        action,
        SimFaultAction::RetryColdReadAfterFailure
    )));
    assert_eq!(
        seed_155
            .outcome
            .trace
            .events
            .iter()
            .filter(|event| matches!(
                event,
                SimEvent::RuntimeRaftNetworkLeaderFailoverColdLiveReadVerified {
                    stream_count: 3,
                    flushed_count: 12,
                    ..
                }
            ))
            .count(),
        1
    );
    let seed_155_stages = seed_155
        .outcome
        .trace
        .events
        .iter()
        .filter_map(|event| match event {
            SimEvent::RuntimeRaftNetworkLeaderFailoverStageReached { stage, .. } => {
                Some(stage.as_str())
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(seed_155_stages, vec![
        "old_leader_stopped",
        "replacement_leader_installed",
        "failover_appends_applied",
        "old_leader_caught_up",
        "cold_flush_started_after_failover",
        "cold_flush_applied_after_failover",
    ]);

    let seed_317 = schedule_records
        .iter()
        .find(|record| record.schedule.seed == 317)
        .expect("schedule corpus seed 317");
    assert!(has_partition_heal(&seed_317.schedule));
    assert!(has_cold_write_retry(&seed_317.schedule));
    assert!(has_action(&seed_317.schedule, |action| matches!(
        action,
        SimFaultAction::VerifyRuntimeColdLiveReads
    )));
}

#[test]
fn smoke_corpus_replays() {
    let _guard = sim_test_guard();
    assert_runtime_raft_randomized_seed_sets_cover_key_branches();
    assert_failure_corpus_covers_expected_invariants_and_seeds();
    assert_schedule_corpus_covers_expected_scenarios_and_seeds();

    let corpus = include_str!("../../corpus/smoke.json");
    let records =
        serde_json::from_str::<Vec<SimRegressionRecord>>(corpus).expect("parse smoke corpus");

    assert_eq!(records.len(), 5);
    for record in records {
        record.assert_replays();
    }

    let schedule_corpus = include_str!("../../corpus/schedule-smoke.json");
    let schedule_records = serde_json::from_str::<Vec<SimScheduledRecord>>(schedule_corpus)
        .expect("deserialize schedule corpus");
    assert_eq!(schedule_records.len(), 27);
    for record in schedule_records {
        assert_eq!(record.schedule, SimSchedule::generate(record.schedule.seed));
        record.assert_replays();
    }

    let failure_corpus = include_str!("../../corpus/failure-smoke.json");
    let failure_records = serde_json::from_str::<Vec<SimFailureRegressionRecord>>(failure_corpus)
        .expect("parse failure smoke corpus");
    assert_eq!(failure_records.len(), 18);
    for record in failure_records {
        record.assert_replays();
    }
}

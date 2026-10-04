//! Extracted from madsim_harness.rs (DoD #3 modularity refactor).
//! See crates/ursula-sim/src/madsim_harness/mod.rs for the rest.

use std::collections::BTreeSet;
use std::sync::Mutex;
use std::sync::MutexGuard;

use super::raft_scenarios::apply_barrier;
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
                            .append(AppendRequest::from_bytes(
                                stream,
                                format!("delay-{append_id};").into_bytes(),
                            ))
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
                        Ok(_) => successful_items += 1,
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
                    .client_write(GroupWriteCommand::from(AppendRequest::from_bytes(
                        stream,
                        probe_payload,
                    )))
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
    assert_eq!(plan.append_payload_counts, vec![1]);
    assert_eq!(plan.failover_payload_counts, vec![1]);
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

/// Bounded-state Invariant 12 (§7.4): after the same log prefix every
/// replica holds the same producers, receipt windows and newest
/// acknowledgements, whether it replayed the log or installed a snapshot.
/// A learner installs a snapshot taken mid-stream (receipt window
/// already evicting), then every replica applies the same suffix.
#[test]
fn producer_state_matches_after_snapshot_install_mid_stream() {
    let _guard = sim_test_guard();
    let producer_states = run_with_madsim(1_212, async {
        let policy = sim_network_policy();
        let (registry, mut engines, leader_id) =
            build_lagging_learner_snapshot_cluster(policy).await;
        let leader_index = usize::try_from(leader_id - 1).expect("leader id fits usize");
        let learner_id = 3;
        let learner_index = usize::try_from(learner_id - 1).expect("learner id fits usize");
        let stream = BucketStreamId::new("simulated", "producers");

        engines[leader_index]
            .create_stream(
                CreateStreamRequest::new(stream.clone(), "application/octet-stream"),
                placement(),
                ColdWriteAdmission::default(),
            )
            .await
            .expect("create stream");
        let append = |seq: u64, producer_id: &str, now_ms: u64| {
            let mut request = AppendRequest::from_bytes(stream.clone(), b"rec".to_vec());
            request.producer = Some(ProducerRequest {
                producer_id: producer_id.to_owned(),
                producer_epoch: 1,
                producer_seq: seq,
            });
            request.now_ms = now_ms;
            request
        };
        // Prefix: "quiet" writes twice, "busy" fills and overflows the
        // window, so the snapshot carries evicted state.
        for seq in 0..2 {
            engines[leader_index]
                .append(
                    append(seq, "quiet", 5),
                    placement(),
                    ColdWriteAdmission::default(),
                )
                .await
                .expect("quiet append");
        }
        for seq in 0..1_100 {
            engines[leader_index]
                .append(
                    append(seq, "busy", 10 + seq),
                    placement(),
                    ColdWriteAdmission::default(),
                )
                .await
                .expect("busy append");
        }
        // A lower bound on the prefix's Raft log index; it only sizes the
        // snapshot and the purge.
        let last_index = leader_applied_index(&engines[leader_index]);
        let leader = engines[leader_index].raft_handle();
        for engine in &engines[..2] {
            engine
                .raft_handle()
                .wait(Some(Duration::from_secs(10)))
                .applied_index_at_least(Some(last_index), "voters applied prefix")
                .await
                .expect("wait for voter apply");
        }
        leader.trigger().snapshot().await.expect("trigger snapshot");
        leader
            .wait(Some(Duration::from_secs(10)))
            .metrics(
                |metrics| {
                    metrics
                        .snapshot
                        .as_ref()
                        .is_some_and(|log_id| log_id.index() >= last_index)
                },
                "leader snapshot covers prefix",
            )
            .await
            .expect("wait for snapshot");
        leader
            .trigger()
            .purge_log(last_index)
            .await
            .expect("purge log");
        leader
            .wait(Some(Duration::from_secs(10)))
            .metrics(
                |metrics| {
                    metrics
                        .purged
                        .as_ref()
                        .is_some_and(|log_id| log_id.index() >= last_index)
                },
                "leader purged prefix",
            )
            .await
            .expect("wait for purge");

        registry.register(learner_id, engines[learner_index].raft_handle());
        let added = leader
            .add_learner(learner_id, BasicNode::new("node-3"), true)
            .await
            .expect("add learner");
        assert!(added.log_id.index() >= last_index);
        assert!(
            registry.full_snapshot_count(learner_id) >= 1,
            "learner must catch up through a full snapshot"
        );

        // Suffix after the install: more window churn, a retry of the
        // newest sequence, a duplicate beyond the window and an idle
        // expiry; every replica applies it from its own state.
        for seq in 1_100..1_300 {
            engines[leader_index]
                .append(
                    append(seq, "busy", 10 + seq),
                    placement(),
                    ColdWriteAdmission::default(),
                )
                .await
                .expect("busy suffix append");
        }
        let newest = engines[leader_index]
            .append(
                append(1, "quiet", 2_000),
                placement(),
                ColdWriteAdmission::default(),
            )
            .await
            .expect("quiet retry");
        assert!(newest.deduplicated && !newest.receipt_evicted);
        let evicted = engines[leader_index]
            .append(
                append(3, "busy", 2_000),
                placement(),
                ColdWriteAdmission::default(),
            )
            .await
            .expect("evicted retry");
        assert!(evicted.deduplicated && evicted.receipt_evicted);
        let idle_at = 5 + 7 * 24 * 60 * 60 * 1_000;
        engines[leader_index]
            .append(
                append(0, "quiet", idle_at),
                placement(),
                ColdWriteAdmission::default(),
            )
            .await
            .expect("quiet after idle expiry");
        apply_barrier(&engines, leader_index, "every replica applied suffix").await;
        let mut producer_states = Vec::new();
        for engine in &engines {
            let snapshot = engine
                .sim_local_group_snapshot()
                .await
                .expect("local group snapshot");
            producer_states.push(
                snapshot
                    .stream_snapshot
                    .streams
                    .into_iter()
                    .map(|entry| entry.producer_states)
                    .collect::<Vec<_>>(),
            );
        }
        producer_states
    });
    assert_eq!(producer_states.len(), 3);
    assert_eq!(producer_states[0], producer_states[1]);
    assert_eq!(producer_states[0], producer_states[2]);
    let stream = &producer_states[0][0];
    let busy = stream
        .iter()
        .find(|producer| producer.producer_id == "busy")
        .expect("busy producer");
    let items: usize = stream
        .iter()
        .flat_map(|producer| producer.receipts.iter())
        .map(|receipt| receipt.items.len().max(1))
        .sum();
    assert!(items <= 1_024, "receipt items {items}");
    assert_eq!(busy.receipts.last().map(|r| r.producer_seq), Some(1_299));
    let quiet = stream
        .iter()
        .find(|producer| producer.producer_id == "quiet")
        .expect("quiet producer");
    assert_eq!(quiet.receipts.len(), 1, "quiet restarted after idle expiry");
    assert_eq!(quiet.last_seen_ms, 5 + 7 * 24 * 60 * 60 * 1_000);
}

/// Seeds for a `#[test]` seed family: the PR default set, or the inclusive
/// range `start..=end` from the environment variable `var` (the nightly
/// sweep sets larger ranges, e.g. `SPARSE_MARKS_SEEDS=2000..=2039`).
fn seeds_from_env(var: &str, default: &[u64]) -> Vec<u64> {
    std::env::var(var)
        .ok()
        .and_then(|range| {
            let (start, end) = range.split_once("..=")?;
            Some((start.parse().ok()?..=end.parse().ok()?).collect())
        })
        .unwrap_or_else(|| default.to_vec())
}

/// Bounded-state F1 seed family (§7.4): cold-path record reads across flush
/// and seal boundaries, retention by record into sealed history, and a
/// learner that installs a snapshot with marks mid-stream.
///
/// - Invariant 9: every replica's marks stay within `ceil(cold MiB) + 2`
///   and its dense part within the unflushed records plus one.
/// - Invariant 10: every replica's record reads, the acknowledgements and the
///   retention result equal the client-side model (RC-2, RC-6, RC-12).
/// - Invariant 11: every replica's readable bytes equal the acknowledged
///   bytes.
/// - Invariant 12 / RC-17: every replica holds identical marks and dense
///   offsets after the same log prefix, replayed or installed.
#[test]
fn sparse_marks_survive_cold_reads_retention_and_snapshot_install() {
    let _guard = sim_test_guard();
    for seed in seeds_from_env("SPARSE_MARKS_SEEDS", &[2_021, 2_022, 2_023]) {
        run_with_madsim(seed, sparse_marks_scenario(seed));
    }
}

async fn sparse_marks_scenario(seed: u64) {
    const RECORD: u64 = SPARSE_MARKS_RECORD;
    let cold_store = Arc::new(sim_cold_store());
    let policy = sim_network_policy();
    let (registry, mut engines, leader_id) =
        build_lagging_learner_snapshot_cluster_with_cold_store(policy, Some(cold_store.clone()))
            .await;
    let leader_index = usize::try_from(leader_id - 1).expect("leader id fits usize");
    let learner_id = 3;
    let learner_index = usize::try_from(learner_id - 1).expect("learner id fits usize");
    let stream = BucketStreamId::new("simulated", "marks");
    engines[leader_index]
        .create_stream(
            CreateStreamRequest::new(stream.clone(), "application/json"),
            placement(),
            ColdWriteAdmission::default(),
        )
        .await
        .expect("create stream");

    let body = sparse_marks_body;
    let mut next = 0_u64;
    let mut chunk = 0_u64;

    // Prefix: 3 MiB of records flushed in mid-record cuts, then a hot tail.
    let leader = &mut engines[leader_index];
    sparse_marks_append(leader, &stream, &mut next, 2_000).await;
    sparse_marks_append(leader, &stream, &mut next, 1_200).await;
    let max = 700_001 + usize::try_from(seed % 7).unwrap() * 1_000;
    sparse_marks_flush(leader, &cold_store, &stream, max, format!("{seed}-{chunk}")).await;
    chunk += 1;
    sparse_marks_append(leader, &stream, &mut next, 40).await;

    // Retention by record into sealed history lands on the block's mark.
    let target = 1_500 + seed % 100;
    let offset = sparse_marks_read(&engines[leader_index], &stream, target, 1)
        .await
        .expect("resolve record")
        .offset;
    assert_eq!(offset, target * RECORD);
    engines[leader_index]
        .publish_snapshot(
            ursula_runtime::PublishSnapshotRequest {
                stream_id: stream.clone(),
                snapshot_offset: offset,
                content_type: "application/json".to_owned(),
                payload: b"{}".to_vec().into(),
                cold_body: None,
                now_ms: 1,
            },
            placement(),
        )
        .await
        .expect("publish snapshot");
    let retained = engines[leader_index]
        .advance_retention(
            ursula_runtime::AdvanceRetentionRequest {
                stream_id: stream.clone(),
                retained_offset: offset,
                now_ms: 1,
            },
            placement(),
        )
        .await
        .expect("advance retention");
    let first = retained.record_range.expect("record range").first_record;
    assert_eq!(retained.retained_offset, first * RECORD);
    assert!(first < target, "a sealed target lands on its mark");
    assert_eq!(
        first * RECORD / ursula_runtime::MARK_BLOCK_BYTES,
        target * RECORD / ursula_runtime::MARK_BLOCK_BYTES
    );

    // Snapshot the prefix, purge it, and install it on the learner.
    let leader = engines[leader_index].raft_handle();
    let last_index = openraft::rt::WatchReceiver::borrow_watched(&leader.metrics())
        .last_applied
        .map(|log_id| log_id.index)
        .expect("leader applied index");
    for engine in &engines[..2] {
        engine
            .raft_handle()
            .wait(Some(Duration::from_secs(10)))
            .applied_index_at_least(Some(last_index), "voters applied prefix")
            .await
            .expect("wait for voter apply");
    }
    leader.trigger().snapshot().await.expect("trigger snapshot");
    leader
        .wait(Some(Duration::from_secs(10)))
        .metrics(
            |metrics| {
                metrics
                    .snapshot
                    .as_ref()
                    .is_some_and(|log_id| log_id.index() >= last_index)
            },
            "leader snapshot covers prefix",
        )
        .await
        .expect("wait for snapshot");
    leader
        .trigger()
        .purge_log(last_index)
        .await
        .expect("purge log");
    leader
        .wait(Some(Duration::from_secs(10)))
        .metrics(
            |metrics| {
                metrics
                    .purged
                    .as_ref()
                    .is_some_and(|log_id| log_id.index() >= last_index)
            },
            "leader purged prefix",
        )
        .await
        .expect("wait for purge");
    registry.register(learner_id, engines[learner_index].raft_handle());
    leader
        .add_learner(learner_id, BasicNode::new("node-3"), true)
        .await
        .expect("add learner");
    assert!(registry.full_snapshot_count(learner_id) >= 1);

    // Suffix: more records and a flush that seals them on every replica.
    let leader_engine = &mut engines[leader_index];
    sparse_marks_append(leader_engine, &stream, &mut next, 900).await;
    sparse_marks_flush(
        leader_engine,
        &cold_store,
        &stream,
        1 << 20,
        format!("{seed}-{chunk}"),
    )
    .await;
    sparse_marks_append(leader_engine, &stream, &mut next, 25).await;
    apply_barrier(&engines, leader_index, "every replica applied suffix").await;

    let model = body(0, next);
    let mut indexes = Vec::new();
    for engine in &engines {
        // Invariant 10: record reads equal the model on every replica.
        for (record, count) in [(first, 3), (target, 7), (2_999, 400), (next - 30, 30)] {
            let response = sparse_marks_read(engine, &stream, record, count)
                .await
                .expect("record read");
            let end = (record + count).min(next);
            assert_eq!(response.offset, record * RECORD);
            assert_eq!(response.next_offset, end * RECORD);
            assert_eq!(
                response.payload,
                &model[usize::try_from(record * RECORD).unwrap()
                    ..usize::try_from(end * RECORD).unwrap()]
            );
        }
        // Invariant 11: readable bytes equal acknowledged bytes.
        let all = engine
            .sim_read_local_stream(
                ReadStreamRequest {
                    stream_id: stream.clone(),
                    offset: first * RECORD,
                    max_len: usize::MAX,
                    now_ms: 0,
                    record: None,
                    max_records: None,
                    leader_only: false,
                    record_anchor: None,
                    read_index: None,
                },
                placement(),
            )
            .await
            .expect("offset read");
        assert_eq!(
            all.payload,
            &model[usize::try_from(first * RECORD).unwrap()..]
        );
        let snapshot = engine
            .sim_local_group_snapshot()
            .await
            .expect("local group snapshot");
        let entry = &snapshot.stream_snapshot.streams[0];
        let index = entry.record_index.clone().expect("record index");
        // Invariant 9.
        let tail = next * RECORD;
        let seal_point = if entry.payload.is_empty() {
            tail
        } else {
            entry.hot_start_offset
        };
        let cold_mib = (seal_point - first * RECORD).div_ceil(ursula_runtime::MARK_BLOCK_BYTES);
        assert!(index.marks().len() as u64 <= cold_mib + 2);
        let unflushed = next - seal_point.div_ceil(RECORD);
        assert!(index.dense_len() as u64 <= unflushed + 1);
        assert!(index.marks().len() > 1);
        indexes.push(index);
    }
    // Invariant 12 / RC-17: identical marks on every replica.
    assert_eq!(indexes[0], indexes[1]);
    assert_eq!(indexes[0], indexes[2]);
}

/// Bytes per record in [`sparse_marks_scenario`]: record `r` starts at
/// `r * SPARSE_MARKS_RECORD`.
const SPARSE_MARKS_RECORD: u64 = 1_000;

fn sparse_marks_body(from: u64, to: u64) -> Vec<u8> {
    let mut bytes = Vec::new();
    for index in from..to {
        let mut line = format!("{{\"i\":{index},\"p\":\"");
        line.push_str(&"p".repeat(usize::try_from(SPARSE_MARKS_RECORD).unwrap() - line.len() - 3));
        line.push_str("\"}\n");
        bytes.extend_from_slice(line.as_bytes());
    }
    bytes
}

/// Appends `count` records through the leader and checks the
/// acknowledgement range comes from apply (Invariant 10).
async fn sparse_marks_append(
    leader: &mut RaftGroupEngine,
    stream: &BucketStreamId,
    next: &mut u64,
    count: u64,
) {
    let from = *next;
    *next += count;
    let mut request = AppendRequest::from_bytes(stream.clone(), sparse_marks_body(from, *next));
    request.content_type = "application/json".to_owned();
    let response = leader
        .append(request, placement(), ColdWriteAdmission::default())
        .await
        .expect("append");
    assert_eq!(
        response.record_range,
        Some(ursula_runtime::StreamRecordRange {
            first_record: from,
            next_record: *next,
        })
    );
}

/// Flushes the whole hot prefix in cuts of at most `max` bytes.
async fn sparse_marks_flush(
    leader: &mut RaftGroupEngine,
    cold_store: &ColdStore,
    stream: &BucketStreamId,
    max: usize,
    tag: String,
) {
    while let Some(candidate) = leader
        .plan_cold_flush(
            PlanColdFlushRequest {
                stream_id: stream.clone(),
                min_hot_bytes: 1,
                max_flush_bytes: max,
            },
            placement(),
        )
        .await
        .expect("plan flush")
    {
        let path = format!(
            "simulated/marks/chunks/seed-{tag}-{}.bin",
            candidate.start_offset
        );
        let object_size = cold_store
            .write_chunk(&path, &candidate.payload)
            .await
            .expect("write chunk");
        leader
            .flush_cold(
                FlushColdRequest {
                    cold_generation: candidate.cold_generation,
                    stream_id: stream.clone(),
                    chunk: ursula_runtime::ColdChunkRef {
                        start_offset: candidate.start_offset,
                        end_offset: candidate.end_offset,
                        s3_path: path,
                        object_size,
                        ..Default::default()
                    },
                },
                placement(),
            )
            .await
            .expect("flush cold");
    }
}

async fn sparse_marks_read(
    engine: &RaftGroupEngine,
    stream: &BucketStreamId,
    record: u64,
    max_records: u64,
) -> Result<ursula_runtime::ReadStreamResponse, GroupEngineError> {
    engine
        .sim_read_local_stream(
            ReadStreamRequest {
                stream_id: stream.clone(),
                offset: 0,
                max_len: usize::MAX,
                now_ms: 0,
                record: Some(record),
                max_records: Some(max_records),
                leader_only: false,
                record_anchor: None,
                read_index: None,
            },
            placement(),
        )
        .await
}

/// Seeds of the bounded-state F5 ambiguous-commit family (§7.4): each runs
/// the variant `seed % 3` of [`external_locator_ambiguity`].
const EXTERNAL_LOCATOR_AMBIGUITY_SEEDS: [u64; 9] = [3, 17, 41, 58, 77, 96, 112, 131, 150];

/// How an external append or its offload ends ambiguously for its caller.
#[derive(Debug, Clone, Copy)]
enum LocatorAmbiguity {
    /// The leader proposes `AppendExternal` while isolated; the caller gives
    /// up, a new leader takes over, and the entry never commits.
    UncommittedOnIsolatedLeader,
    /// The append commits, but its caller never sees the response.
    LostResponse,
    /// The append commits; the leader writes the page entries for the
    /// offload while isolated, and its `OffloadColdRefs` never commits. The
    /// new leader's offload rewrites the same entries and commits.
    AmbiguousOffload,
}

fn engine_index(node_id: u64) -> usize {
    usize::try_from(node_id - 1).expect("node id fits usize")
}

fn leader_applied_index(engine: &RaftGroupEngine) -> u64 {
    openraft::rt::WatchReceiver::borrow_watched(&engine.raft_handle().metrics())
        .last_applied
        .map(|log_id| log_id.index)
        .expect("leader applied index")
}

fn external_append_request(stream: &BucketStreamId, path: &str, len: u64) -> AppendExternalRequest {
    AppendExternalRequest {
        stream_id: stream.clone(),
        content_type: "application/octet-stream".to_owned(),
        payload: ursula_runtime::ExternalPayloadRef {
            s3_path: path.to_owned(),
            payload_len: len,
            object_size: len,
        },
        record_ends: Vec::new(),
        close_after: false,
        stream_seq: None,
        producer: None,
        now_ms: 0,
        record_match: None,
    }
}

/// The object path of every chunk page entry of `stream`.
async fn page_chunk_paths(cold_store: &Arc<ColdStore>, stream: &BucketStreamId) -> Vec<String> {
    use ursula_runtime::ColdIndexPageStore;

    let store = ursula_runtime::ColdStoreColdIndexPageStore::new(cold_store.clone());
    let mut paths = Vec::new();
    for key in cold_store
        .list_cold_index_pages()
        .await
        .expect("list cold index pages")
    {
        if &key.stream_id != stream {
            continue;
        }
        if let Some(page) = store.get_page(&key).await.expect("get page") {
            paths.extend(page.cold_chunks.iter().map(|chunk| chunk.s3_path.clone()));
        }
    }
    paths
}

/// Every external page entry of `stream` as `(start, end, path)`.
async fn page_external_entries(
    cold_store: &Arc<ColdStore>,
    stream: &BucketStreamId,
) -> Vec<(u64, u64, String)> {
    use ursula_runtime::ColdIndexPageStore;

    let store = ursula_runtime::ColdStoreColdIndexPageStore::new(cold_store.clone());
    let mut entries = Vec::new();
    for key in cold_store
        .list_cold_index_pages()
        .await
        .expect("list cold index pages")
    {
        if &key.stream_id != stream {
            continue;
        }
        if let Some(page) = store.get_page(&key).await.expect("get page") {
            entries.extend(page.external_segments.iter().map(|object| {
                (
                    object.start_offset,
                    object.end_offset,
                    object.s3_path.clone(),
                )
            }));
        }
    }
    entries
}

fn isolate(policy: &InProcessRaftNetworkPolicy, leader_id: u64) {
    for peer in (1..=3).filter(|peer| *peer != leader_id) {
        policy.partition_bidirectional(leader_id, peer);
    }
}

/// Waits for the voters other than the isolated `leader_id` to elect
/// another leader; returns its id.
async fn replacement_leader(engines: &[RaftGroupEngine], leader_id: u64) -> u64 {
    let observer = (1..=3)
        .find(|peer| *peer != leader_id)
        .expect("a connected voter");
    engines[engine_index(observer)]
        .raft_handle()
        .wait(Some(Duration::from_secs(5)))
        .metrics(
            |metrics| {
                metrics
                    .current_leader
                    .is_some_and(|current| current != leader_id)
            },
            "connected voters elect a replacement leader",
        )
        .await
        .expect("replacement leader")
        .current_leader
        .expect("replacement leader id")
}

/// One run of the F5 ambiguous-commit family, checking
/// Invariant 11: after the ambiguity, readable bytes equal acknowledged bytes
/// on every replica, no page entry overlaps differing bytes, no page entry
/// names an object that never committed, and the ambiguous staged object was
/// not deleted.
async fn external_locator_ambiguity(variant: LocatorAmbiguity) {
    let cold_store: Arc<ColdStore> = Arc::new(sim_cold_store());
    let policy = sim_network_policy();
    let (_registry, mut engines, mut leader_id) =
        build_three_node_cluster_with_cold_store(policy.clone(), Some(cold_store.clone())).await;
    let stream = BucketStreamId::new("simulated", "external-locators");
    let leader = engine_index(leader_id);
    engines[leader]
        .create_stream(
            CreateStreamRequest::new(stream.clone(), "application/octet-stream"),
            placement(),
            ColdWriteAdmission::default(),
        )
        .await
        .expect("create stream");
    engines[leader]
        .append(
            AppendRequest::from_bytes(stream.clone(), b"base;".to_vec()),
            placement(),
            ColdWriteAdmission::default(),
        )
        .await
        .expect("base append");
    let offload_now = ursula_runtime::OffloadColdRefsRequest {
        min_age_ms: 0,
        ..ursula_runtime::OffloadColdRefsRequest::new(0, 16)
    };

    let ambiguous = vec![b'A'; 64];
    let ambiguous_path = ursula_runtime::new_external_payload_path(&stream);
    cold_store
        .write_chunk(&ambiguous_path, &ambiguous)
        .await
        .expect("stage ambiguous payload");
    let ambiguous_request = external_append_request(&stream, &ambiguous_path, 64);
    let ambiguous_committed = match variant {
        LocatorAmbiguity::UncommittedOnIsolatedLeader => {
            isolate(&policy, leader_id);
            let outcome = madsim::time::timeout(
                Duration::from_millis(300),
                engines[leader].append_external(ambiguous_request, placement()),
            )
            .await;
            assert!(
                !matches!(outcome, Ok(Ok(_))),
                "an isolated leader cannot commit: {outcome:?}"
            );
            assert!(
                page_external_entries(&cold_store, &stream).await.is_empty(),
                "no page entry is written before or for an uncommitted proposal"
            );
            leader_id = replacement_leader(&engines, leader_id).await;
            policy.clear();
            false
        }
        LocatorAmbiguity::LostResponse => {
            let raft = engines[leader].raft_handle();
            // The caller's response is never observed.
            drop(madsim::task::spawn(async move {
                let _ = raft
                    .client_write(GroupWriteCommand::from(ambiguous_request))
                    .await;
            }));
            // Let it commit before the next append claims the tail.
            madsim::time::sleep(Duration::from_millis(200)).await;
            true
        }
        LocatorAmbiguity::AmbiguousOffload => {
            engines[leader]
                .append_external(ambiguous_request, placement())
                .await
                .expect("committed external append");
            isolate(&policy, leader_id);
            let outcome = madsim::time::timeout(
                Duration::from_millis(300),
                engines[leader].offload_cold_refs(offload_now, placement()),
            )
            .await;
            assert!(
                !matches!(outcome, Ok(Ok(ref report)) if report.streams > 0),
                "an isolated leader cannot commit its offload: {outcome:?}"
            );
            assert!(
                !page_external_entries(&cold_store, &stream).await.is_empty(),
                "the isolated leader indexed the committed ref before proposing"
            );
            leader_id = replacement_leader(&engines, leader_id).await;
            policy.clear();
            true
        }
    };

    // A committed append at the same offsets the ambiguous one would take.
    let leader = engine_index(leader_id);
    let committed = vec![b'B'; 40];
    let committed_path = ursula_runtime::new_external_payload_path(&stream);
    cold_store
        .write_chunk(&committed_path, &committed)
        .await
        .expect("stage committed payload");
    let mut appended = None;
    for _ in 0..50 {
        match engines[leader]
            .append_external(
                external_append_request(&stream, &committed_path, 40),
                placement(),
            )
            .await
        {
            Ok(response) => {
                appended = Some(response);
                break;
            }
            Err(_) => madsim::time::sleep(Duration::from_millis(50)).await,
        }
    }
    appended.expect("committed external append on the current leader");
    for _ in 0..50 {
        let report = engines[leader]
            .offload_cold_refs(offload_now, placement())
            .await
            .expect("offload pass");
        if report.streams == 0 {
            break;
        }
    }

    apply_barrier(&engines, leader, "every replica applied the offloads").await;
    let mut acknowledged = b"base;".to_vec();
    if ambiguous_committed {
        acknowledged.extend_from_slice(&ambiguous);
    }
    acknowledged.extend_from_slice(&committed);
    let tail = acknowledged.len();
    for (index, engine) in engines.iter_mut().enumerate() {
        let node_id = u64::try_from(index + 1).expect("node id fits u64");
        read_local_payload_eventually(
            engine,
            node_id,
            &stream,
            0,
            tail + 16,
            &acknowledged,
            "replica reads the acknowledged bytes",
        )
        .await;
        let gauges = engine
            .state_gauges(placement())
            .await
            .expect("state gauges");
        assert_eq!(gauges.staged_external_refs, 0, "node {node_id}");
    }
    let entries = page_external_entries(&cold_store, &stream).await;
    assert!(
        !entries.is_empty(),
        "the offload indexed the committed refs"
    );
    for (start, end, path) in entries {
        assert!(
            ambiguous_committed || path != ambiguous_path,
            "a page entry names an object that never committed"
        );
        let start = usize::try_from(start).expect("offset fits usize");
        let end = usize::try_from(end).expect("offset fits usize");
        let object = if path == ambiguous_path {
            &ambiguous
        } else {
            assert_eq!(path, committed_path, "unexpected page entry {path}");
            &committed
        };
        assert_eq!(
            acknowledged.get(start..end),
            object.get(..end - start),
            "page entry [{start}, {end}) {path} overlaps differing bytes"
        );
    }
    assert!(
        cold_store.object_size(&ambiguous_path).await.is_ok(),
        "the ambiguous staged object is kept for the orphan sweep"
    );
}

/// Bounded-state F5 ambiguous-commit seeds with Invariant 11 (§7.4, B5).
#[test]
fn external_locators_survive_ambiguous_commits() {
    let _guard = sim_test_guard();
    for seed in seeds_from_env(
        "EXTERNAL_LOCATOR_AMBIGUITY_SEEDS",
        &EXTERNAL_LOCATOR_AMBIGUITY_SEEDS,
    ) {
        let variant = match seed % 3 {
            0 => LocatorAmbiguity::UncommittedOnIsolatedLeader,
            1 => LocatorAmbiguity::LostResponse,
            _ => LocatorAmbiguity::AmbiguousOffload,
        };
        run_with_madsim(seed, external_locator_ambiguity(variant));
    }
}

/// Seeds of the F5 snapshot-install family: a learner installs a
/// group snapshot that holds staged external refs (not yet offloaded).
const EXTERNAL_LOCATOR_SNAPSHOT_SEEDS: [u64; 2] = [5, 23];

/// Bounded-state F5 follow-up (§7.4, Invariants 11 and 12): a learner that
/// installs a snapshot holding staged external refs reads the
/// acknowledged bytes, holds the same refs as the voters, and converges with
/// them after the offload commits.
#[test]
fn external_locators_survive_snapshot_install_with_staged_refs() {
    let _guard = sim_test_guard();
    for seed in seeds_from_env(
        "EXTERNAL_LOCATOR_SNAPSHOT_SEEDS",
        &EXTERNAL_LOCATOR_SNAPSHOT_SEEDS,
    ) {
        run_with_madsim(seed, external_locator_snapshot_install(seed));
    }
}

async fn external_locator_snapshot_install(seed: u64) {
    let cold_store: Arc<ColdStore> = Arc::new(sim_cold_store());
    let policy = sim_network_policy();
    let (registry, mut engines, leader_id) =
        build_lagging_learner_snapshot_cluster_with_cold_store(policy, Some(cold_store.clone()))
            .await;
    let leader_index = engine_index(leader_id);
    let learner_id = 3;
    let stream = BucketStreamId::new("simulated", "locator-install");
    engines[leader_index]
        .create_stream(
            CreateStreamRequest::new(stream.clone(), "application/octet-stream"),
            placement(),
            ColdWriteAdmission::default(),
        )
        .await
        .expect("create stream");
    let mut acknowledged = Vec::new();
    let externals = 2 + usize::try_from(seed % 3).expect("small");
    for index in 0..externals {
        let hot = format!("hot-{index};").into_bytes();
        engines[leader_index]
            .append(
                AppendRequest::from_bytes(stream.clone(), hot.clone()),
                placement(),
                ColdWriteAdmission::default(),
            )
            .await
            .expect("hot append");
        acknowledged.extend_from_slice(&hot);
        let payload = vec![b'a' + u8::try_from(index).expect("small"); 32 + index];
        let path = ursula_runtime::new_external_payload_path(&stream);
        cold_store
            .write_chunk(&path, &payload)
            .await
            .expect("stage external payload");
        let len = u64::try_from(payload.len()).expect("len fits u64");
        engines[leader_index]
            .append_external(external_append_request(&stream, &path, len), placement())
            .await
            .expect("external append");
        acknowledged.extend_from_slice(&payload);
    }
    let staged = engines[leader_index]
        .state_gauges(placement())
        .await
        .expect("leader gauges")
        .staged_external_refs;
    assert!(staged > 0, "the snapshot holds staged refs");

    // Snapshot the prefix, purge it, and install it on the learner.
    let leader = engines[leader_index].raft_handle();
    let last_index = leader_applied_index(&engines[leader_index]);
    for engine in &engines[..2] {
        engine
            .raft_handle()
            .wait(Some(Duration::from_secs(10)))
            .applied_index_at_least(Some(last_index), "voters applied prefix")
            .await
            .expect("wait for voter apply");
    }
    leader.trigger().snapshot().await.expect("trigger snapshot");
    leader
        .wait(Some(Duration::from_secs(10)))
        .metrics(
            |metrics| {
                metrics
                    .snapshot
                    .as_ref()
                    .is_some_and(|log_id| log_id.index() >= last_index)
            },
            "leader snapshot covers prefix",
        )
        .await
        .expect("wait for snapshot");
    leader
        .trigger()
        .purge_log(last_index)
        .await
        .expect("purge log");
    leader
        .wait(Some(Duration::from_secs(10)))
        .metrics(
            |metrics| {
                metrics
                    .purged
                    .as_ref()
                    .is_some_and(|log_id| log_id.index() >= last_index)
            },
            "leader purged prefix",
        )
        .await
        .expect("wait for purge");
    registry.register(learner_id, engines[engine_index(learner_id)].raft_handle());
    leader
        .add_learner(learner_id, BasicNode::new("node-3"), true)
        .await
        .expect("add learner");
    assert!(registry.full_snapshot_count(learner_id) >= 1);
    apply_barrier(&engines, leader_index, "barrier-install").await;

    // Invariants 11 and 12 with the refs still staged.
    let tail = acknowledged.len();
    let mut entries = Vec::new();
    for (index, engine) in engines.iter_mut().enumerate() {
        let node_id = u64::try_from(index + 1).expect("node id fits u64");
        read_local_payload_eventually(
            engine,
            node_id,
            &stream,
            0,
            tail + 16,
            &acknowledged,
            "replica reads the acknowledged bytes before the offload",
        )
        .await;
        let gauges = engine.state_gauges(placement()).await.expect("gauges");
        assert_eq!(gauges.staged_external_refs, staged, "node {node_id}");
        let snapshot = engine
            .sim_local_group_snapshot()
            .await
            .expect("local group snapshot");
        let entry = snapshot
            .stream_snapshot
            .streams
            .iter()
            .find(|entry| entry.metadata.stream_id == stream)
            .expect("stream entry")
            .clone();
        entries.push((entry.external_segments, entry.cold_chunks));
    }
    assert_eq!(entries[0], entries[1]);
    assert_eq!(
        entries[0], entries[2],
        "the installed refs equal the voters'"
    );

    // The offload commits on every replica, the learner included.
    let offload_now = ursula_runtime::OffloadColdRefsRequest {
        min_age_ms: 0,
        ..ursula_runtime::OffloadColdRefsRequest::new(0, 16)
    };
    for _ in 0..50 {
        let report = engines[leader_index]
            .offload_cold_refs(offload_now, placement())
            .await
            .expect("offload pass");
        if report.streams == 0 {
            break;
        }
    }
    apply_barrier(&engines, leader_index, "barrier-offload").await;
    for (index, engine) in engines.iter_mut().enumerate() {
        let node_id = u64::try_from(index + 1).expect("node id fits u64");
        read_local_payload_eventually(
            engine,
            node_id,
            &stream,
            0,
            tail + 16,
            &acknowledged,
            "replica reads the acknowledged bytes after the offload",
        )
        .await;
        let gauges = engine.state_gauges(placement()).await.expect("gauges");
        assert_eq!(gauges.staged_external_refs, 0, "node {node_id}");
    }
}

/// Seeds of the F5 ambiguous `CompactCold` family: `seed % 2` picks
/// whether the compaction commits.
const AMBIGUOUS_COMPACTION_SEEDS: [u64; 2] = [8, 13];

/// Bounded-state F5/F14 follow-up (§7.4, Invariant 11): a `CompactCold` whose
/// outcome is ambiguous to its caller (the leader rewrote the page entries,
/// then lost quorum) never changes readable bytes: every replica reads the
/// acknowledged bytes, around an offloaded external ref, before and after a
/// new leader keeps writing, and the compaction inputs are not deleted when
/// the compaction never committed.
#[test]
fn external_locators_survive_ambiguous_compaction() {
    let _guard = sim_test_guard();
    for seed in seeds_from_env("AMBIGUOUS_COMPACTION_SEEDS", &AMBIGUOUS_COMPACTION_SEEDS) {
        run_with_madsim(seed, ambiguous_compaction(seed % 2 == 0));
    }
}

async fn ambiguous_compaction(commits: bool) {
    let cold_store: Arc<ColdStore> = Arc::new(sim_cold_store());
    let policy = sim_network_policy();
    let (_registry, mut engines, mut leader_id) =
        build_three_node_cluster_with_cold_store(policy.clone(), Some(cold_store.clone())).await;
    let stream = BucketStreamId::new("simulated", "ambiguous-compaction");
    let leader = engine_index(leader_id);
    engines[leader]
        .create_stream(
            CreateStreamRequest::new(stream.clone(), "application/octet-stream"),
            placement(),
            ColdWriteAdmission::default(),
        )
        .await
        .expect("create stream");

    // Two flushed chunks, then an external append offloaded to the pages.
    let mut acknowledged = Vec::new();
    let mut chunks = Vec::new();
    for part in [b"first-chunk;".to_vec(), b"second-chunk;".to_vec()] {
        engines[leader]
            .append(
                AppendRequest::from_bytes(stream.clone(), part.clone()),
                placement(),
                ColdWriteAdmission::default(),
            )
            .await
            .expect("append");
        acknowledged.extend_from_slice(&part);
        let candidate = engines[leader]
            .plan_cold_flush(
                PlanColdFlushRequest {
                    stream_id: stream.clone(),
                    min_hot_bytes: 1,
                    max_flush_bytes: usize::MAX,
                },
                placement(),
            )
            .await
            .expect("plan flush")
            .expect("a flush candidate");
        let path = format!(
            "simulated/ambiguous-compaction/chunks/{}.bin",
            candidate.start_offset
        );
        let object_size = cold_store
            .write_chunk(&path, &candidate.payload)
            .await
            .expect("write chunk");
        let chunk = ursula_runtime::ColdChunkRef {
            start_offset: candidate.start_offset,
            end_offset: candidate.end_offset,
            s3_path: path,
            object_size,
            ..Default::default()
        };
        engines[leader]
            .flush_cold(
                FlushColdRequest {
                    cold_generation: candidate.cold_generation,
                    stream_id: stream.clone(),
                    chunk: chunk.clone(),
                },
                placement(),
            )
            .await
            .expect("flush cold");
        chunks.push(chunk);
    }
    let external = vec![b'E'; 48];
    let external_path = ursula_runtime::new_external_payload_path(&stream);
    cold_store
        .write_chunk(&external_path, &external)
        .await
        .expect("stage external payload");
    engines[leader]
        .append_external(
            external_append_request(&stream, &external_path, 48),
            placement(),
        )
        .await
        .expect("external append");
    acknowledged.extend_from_slice(&external);
    let offload_now = ursula_runtime::OffloadColdRefsRequest {
        min_age_ms: 0,
        ..ursula_runtime::OffloadColdRefsRequest::new(0, 16)
    };
    for _ in 0..50 {
        let report = engines[leader]
            .offload_cold_refs(offload_now, placement())
            .await
            .expect("offload pass");
        if report.streams == 0 {
            break;
        }
    }

    // The compaction replacement holds the same bytes as its inputs.
    let replacement_bytes = acknowledged
        .get(..usize::try_from(chunks[1].end_offset).expect("offset fits usize"))
        .expect("replacement range")
        .to_vec();
    let replacement_path = "simulated/ambiguous-compaction/chunks/compacted.bin".to_owned();
    let replacement_size = cold_store
        .write_chunk(&replacement_path, &replacement_bytes)
        .await
        .expect("write replacement");
    let request = ursula_runtime::CompactColdRequest {
        stream_id: stream.clone(),
        old_chunks: chunks.clone(),
        replacement: ursula_runtime::ColdChunkRef {
            start_offset: chunks[0].start_offset,
            end_offset: chunks[1].end_offset,
            s3_path: replacement_path.clone(),
            object_size: replacement_size,
            ..Default::default()
        },
        gc_not_before_ms: u64::MAX,
    };
    apply_barrier(&engines, leader, "barrier-prefix").await;
    if commits {
        // The compaction commits; then its leader loses quorum.
        engines[leader]
            .compact_cold(request, placement())
            .await
            .expect("committed compaction");
        isolate(&policy, leader_id);
    } else {
        // The leader rewrites the page entries, then loses quorum before
        // the command commits; the caller cannot roll the pages back.
        isolate(&policy, leader_id);
        let outcome = madsim::time::timeout(
            Duration::from_millis(300),
            engines[leader].compact_cold(request, placement()),
        )
        .await;
        assert!(
            !matches!(outcome, Ok(Ok(_))),
            "an isolated leader cannot commit its compaction: {outcome:?}"
        );
    }
    leader_id = replacement_leader(&engines, leader_id).await;
    policy.clear();
    // Either way the pages already name the replacement, which holds the
    // inputs' bytes.
    let chunk_paths = page_chunk_paths(&cold_store, &stream).await;
    assert!(
        !chunk_paths.is_empty() && chunk_paths.iter().all(|path| *path == replacement_path),
        "pages index the replacement: {chunk_paths:?}"
    );

    // The current leader keeps writing past the ambiguity.
    let leader = engine_index(leader_id);
    let tail_part = b"after-compaction;".to_vec();
    let mut appended = false;
    for _ in 0..50 {
        if engines[leader]
            .append(
                AppendRequest::from_bytes(stream.clone(), tail_part.clone()),
                placement(),
                ColdWriteAdmission::default(),
            )
            .await
            .is_ok()
        {
            appended = true;
            break;
        }
        madsim::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(appended, "the current leader accepts appends");
    acknowledged.extend_from_slice(&tail_part);
    apply_barrier(&engines, leader, "barrier-suffix").await;

    // Invariant 11 on every replica.
    let tail = acknowledged.len();
    for (index, engine) in engines.iter_mut().enumerate() {
        let node_id = u64::try_from(index + 1).expect("node id fits u64");
        read_local_payload_eventually(
            engine,
            node_id,
            &stream,
            0,
            tail + 16,
            &acknowledged,
            "replica reads the acknowledged bytes after the compaction",
        )
        .await;
    }
    for (start, end, path) in page_external_entries(&cold_store, &stream).await {
        assert_eq!(path, external_path, "unexpected external page entry {path}");
        let start = usize::try_from(start).expect("offset fits usize");
        let end = usize::try_from(end).expect("offset fits usize");
        assert_eq!(
            acknowledged.get(start..end),
            external.get(..end - start),
            "external page entry [{start}, {end}) overlaps differing bytes"
        );
    }
    if !commits {
        for chunk in &chunks {
            assert!(
                cold_store.object_size(&chunk.s3_path).await.is_ok(),
                "an uncommitted compaction deletes none of its inputs"
            );
        }
    }
}

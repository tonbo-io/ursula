//! `ThreeNodeRaftSim::*` dispatch glue extracted from `madsim_harness/mod.rs`
//! (DoD #3 modularity refactor). The dispatch layer routes a SimSchedule to
//! the right `run_*_inner` scenario function and wraps the outcome into a
//! `SimReport`.

use super::{
    HttpProtocolSurfacePlan, RuntimeInterleavingPlan, RuntimeRaftNetworkOptions, SimReport,
    SimScenario, SimSchedule, ThreeNodeRaftSim, ThreeNodeRaftSimConfig, ThreeNodeRaftSimOutcome,
    cold_read_delay_ms_from_fault_plan, cold_read_truncate_len_from_fault_plan,
    cold_write_delay_ms_from_fault_plan, corrupt_cold_live_read_node_from_fault_plan,
    has_cold_delete_fault_in_fault_plan, has_cold_read_fault_in_fault_plan,
    has_cold_write_fault_in_fault_plan,
    has_corrupt_http_live_limit_backpressure_expectation_in_fault_plan,
    has_corrupt_http_live_sse_next_offset_expectation_in_fault_plan,
    has_corrupt_http_producer_duplicate_expectation_in_fault_plan,
    has_corrupt_http_snapshot_body_expectation_in_fault_plan,
    has_corrupt_runtime_raft_snapshot_append_counts_in_fault_plan,
    has_heal_seeded_follower_in_fault_plan, has_partition_seeded_follower_in_fault_plan,
    has_restart_stopped_follower_in_fault_plan, has_restart_stopped_leader_in_fault_plan,
    has_retry_cold_read_after_failure_in_fault_plan,
    has_retry_cold_write_after_failure_in_fault_plan, has_stop_current_leader_in_fault_plan,
    has_stop_seeded_follower_in_fault_plan, has_verify_runtime_cold_live_reads_in_fault_plan,
    http_protocol_surface_plan_from_fault_plan, run_cold_delete_fault_inner,
    run_cold_live_read_inner, run_cold_read_delay_inner, run_cold_read_fault_inner,
    run_cold_read_truncate_inner, run_cold_write_delay_inner, run_cold_write_fault_inner,
    run_http_live_limit_protocol_surface_inner, run_http_live_protocol_surface_inner,
    run_http_producer_protocol_surface_inner, run_http_protocol_surface_inner,
    run_http_protocol_surface_randomized_inner, run_leader_failover_inner, run_no_fault_inner,
    run_partition_heal_inner, run_restart_follower_inner, run_runtime_actor_scheduling_inner,
    run_runtime_cold_flush_worker_inner, run_runtime_multi_client_actors_inner,
    run_runtime_raft_engine_inner, run_runtime_raft_network_inner,
    run_runtime_raft_snapshot_install_inner, run_runtime_seeded_interleaving_inner,
    run_snapshot_catch_up_inner, run_with_madsim, runtime_interleaving_plan_from_fault_plan,
    runtime_raft_network_workload_plan_from_fault_plan,
};

impl ThreeNodeRaftSim {
    pub fn run_schedule(schedule: SimSchedule) -> SimReport {
        let config = schedule.config();
        match schedule.scenario {
            SimScenario::RuntimeSeededInterleaving => {
                let plan = runtime_interleaving_plan_from_fault_plan(&schedule.fault_plan)
                    .unwrap_or_else(|| RuntimeInterleavingPlan::from_seed(schedule.seed));
                Self::run_runtime_seeded_interleaving_with_plan_report(config, plan)
            }
            SimScenario::HttpProtocolSurfaceRandomized => {
                let plan = http_protocol_surface_plan_from_fault_plan(&schedule.fault_plan)
                    .unwrap_or_else(|| HttpProtocolSurfacePlan::from_seed(schedule.seed));
                Self::run_http_protocol_surface_randomized_with_plan_report(config, plan)
            }
            SimScenario::RuntimeRaftNetwork => Self::run_runtime_raft_network_with_options_report(
                config,
                RuntimeRaftNetworkOptions {
                    partition_before_append: has_partition_seeded_follower_in_fault_plan(
                        &schedule.fault_plan,
                    ),
                    heal_after_lag: has_heal_seeded_follower_in_fault_plan(&schedule.fault_plan),
                    verify_cold_live_read: has_verify_runtime_cold_live_reads_in_fault_plan(
                        &schedule.fault_plan,
                    ),
                    delay_cold_write_ms: cold_write_delay_ms_from_fault_plan(&schedule.fault_plan),
                    delay_cold_read_ms: cold_read_delay_ms_from_fault_plan(&schedule.fault_plan),
                    truncate_cold_read_len: cold_read_truncate_len_from_fault_plan(
                        &schedule.fault_plan,
                    ),
                    fail_cold_write: has_cold_write_fault_in_fault_plan(&schedule.fault_plan),
                    retry_cold_write_after_failure:
                        has_retry_cold_write_after_failure_in_fault_plan(&schedule.fault_plan),
                    retry_cold_read_after_truncate: has_retry_cold_read_after_failure_in_fault_plan(
                        &schedule.fault_plan,
                    ),
                    restart_during_cold_flush: has_stop_seeded_follower_in_fault_plan(
                        &schedule.fault_plan,
                    ) && has_restart_stopped_follower_in_fault_plan(
                        &schedule.fault_plan,
                    ),
                    leader_failover_after_read: has_stop_current_leader_in_fault_plan(
                        &schedule.fault_plan,
                    ) && has_restart_stopped_leader_in_fault_plan(
                        &schedule.fault_plan,
                    ),
                    workload_plan: runtime_raft_network_workload_plan_from_fault_plan(
                        &schedule.fault_plan,
                    )
                    .unwrap_or_default(),
                },
            ),
            SimScenario::PartitionHeal => Self::run_partition_heal_with_options_report(
                config,
                has_partition_seeded_follower_in_fault_plan(&schedule.fault_plan),
                has_heal_seeded_follower_in_fault_plan(&schedule.fault_plan),
            ),
            SimScenario::ColdLiveRead => Self::run_cold_live_read_with_options_report(
                config,
                corrupt_cold_live_read_node_from_fault_plan(&schedule.fault_plan),
            ),
            SimScenario::ColdReadTruncate => Self::run_cold_read_truncate_with_options_report(
                config,
                cold_read_truncate_len_from_fault_plan(&schedule.fault_plan),
            ),
            SimScenario::ColdReadDelay => Self::run_cold_read_delay_with_options_report(
                config,
                cold_read_delay_ms_from_fault_plan(&schedule.fault_plan),
            ),
            SimScenario::ColdReadFault => Self::run_cold_read_fault_with_options_report(
                config,
                has_cold_read_fault_in_fault_plan(&schedule.fault_plan),
            ),
            SimScenario::ColdWriteFault => Self::run_cold_write_fault_with_options_report(
                config,
                has_cold_write_fault_in_fault_plan(&schedule.fault_plan),
            ),
            SimScenario::ColdWriteDelay => Self::run_cold_write_delay_with_options_report(
                config,
                cold_write_delay_ms_from_fault_plan(&schedule.fault_plan),
            ),
            SimScenario::ColdDeleteFault => Self::run_cold_delete_fault_with_options_report(
                config,
                has_cold_delete_fault_in_fault_plan(&schedule.fault_plan),
            ),
            SimScenario::HttpLiveLimitProtocolSurface => {
                Self::run_http_live_limit_protocol_surface_with_options_report(
                    config,
                    has_corrupt_http_live_limit_backpressure_expectation_in_fault_plan(
                        &schedule.fault_plan,
                    ),
                )
            }
            SimScenario::HttpLiveProtocolSurface => {
                Self::run_http_live_protocol_surface_with_options_report(
                    config,
                    has_corrupt_http_live_sse_next_offset_expectation_in_fault_plan(
                        &schedule.fault_plan,
                    ),
                )
            }
            SimScenario::HttpProducerProtocolSurface => {
                Self::run_http_producer_protocol_surface_with_options_report(
                    config,
                    has_corrupt_http_producer_duplicate_expectation_in_fault_plan(
                        &schedule.fault_plan,
                    ),
                )
            }
            SimScenario::HttpProtocolSurface => {
                Self::run_http_protocol_surface_with_options_report(
                    config,
                    has_corrupt_http_snapshot_body_expectation_in_fault_plan(&schedule.fault_plan),
                )
            }
            SimScenario::RuntimeRaftSnapshotInstall => {
                Self::run_runtime_raft_snapshot_install_with_options_report(
                    config,
                    has_corrupt_runtime_raft_snapshot_append_counts_in_fault_plan(
                        &schedule.fault_plan,
                    ),
                )
            }
            scenario => Self::run_report(scenario, config),
        }
    }

    pub fn run_report(scenario: SimScenario, config: ThreeNodeRaftSimConfig) -> SimReport {
        match scenario {
            SimScenario::NoFaultBaseline => Self::run_no_fault_report(config),
            SimScenario::PartitionHeal => Self::run_partition_heal_report(config),
            SimScenario::LeaderFailover => Self::run_leader_failover_report(config),
            SimScenario::SnapshotCatchUp => Self::run_snapshot_catch_up_report(config),
            SimScenario::RestartFollower => Self::run_restart_follower_report(config),
            SimScenario::ColdLiveRead => Self::run_cold_live_read_report(config),
            SimScenario::ColdReadFault => Self::run_cold_read_fault_report(config),
            SimScenario::ColdWriteFault => Self::run_cold_write_fault_report(config),
            SimScenario::ColdWriteDelay => Self::run_cold_write_delay_report(config),
            SimScenario::ColdDeleteFault => Self::run_cold_delete_fault_report(config),
            SimScenario::ColdReadDelay => Self::run_cold_read_delay_report(config),
            SimScenario::ColdReadTruncate => Self::run_cold_read_truncate_report(config),
            SimScenario::HttpLiveLimitProtocolSurface => {
                Self::run_http_live_limit_protocol_surface_report(config)
            }
            SimScenario::HttpLiveProtocolSurface => {
                Self::run_http_live_protocol_surface_report(config)
            }
            SimScenario::HttpProducerProtocolSurface => {
                Self::run_http_producer_protocol_surface_report(config)
            }
            SimScenario::HttpProtocolSurface => Self::run_http_protocol_surface_report(config),
            SimScenario::HttpProtocolSurfaceRandomized => {
                Self::run_http_protocol_surface_randomized_report(config)
            }
            SimScenario::RuntimeActorScheduling => {
                Self::run_runtime_actor_scheduling_report(config)
            }
            SimScenario::RuntimeMultiClientActors => {
                Self::run_runtime_multi_client_actors_report(config)
            }
            SimScenario::RuntimeColdFlushWorker => {
                Self::run_runtime_cold_flush_worker_report(config)
            }
            SimScenario::RuntimeSeededInterleaving => {
                Self::run_runtime_seeded_interleaving_report(config)
            }
            SimScenario::RuntimeRaftEngine => Self::run_runtime_raft_engine_report(config),
            SimScenario::RuntimeRaftNetwork => Self::run_runtime_raft_network_report(config),
            SimScenario::RuntimeRaftSnapshotInstall => {
                Self::run_runtime_raft_snapshot_install_report(config)
            }
        }
    }

    pub fn run_partition_heal(config: ThreeNodeRaftSimConfig) -> ThreeNodeRaftSimOutcome {
        Self::run_partition_heal_report(config).outcome
    }

    pub fn run_partition_heal_report(config: ThreeNodeRaftSimConfig) -> SimReport {
        Self::run_partition_heal_with_options_report(config, true, true)
    }

    pub fn run_partition_heal_with_options_report(
        config: ThreeNodeRaftSimConfig,
        partition_before_append: bool,
        heal_after_lag: bool,
    ) -> SimReport {
        run_with_madsim(config.seed, async move {
            SimReport {
                scenario: SimScenario::PartitionHeal,
                outcome: run_partition_heal_inner(config, partition_before_append, heal_after_lag)
                    .await,
            }
        })
    }

    pub fn run_leader_failover(config: ThreeNodeRaftSimConfig) -> ThreeNodeRaftSimOutcome {
        Self::run_leader_failover_report(config).outcome
    }

    pub fn run_leader_failover_report(config: ThreeNodeRaftSimConfig) -> SimReport {
        run_with_madsim(config.seed, async move {
            SimReport {
                scenario: SimScenario::LeaderFailover,
                outcome: run_leader_failover_inner(config).await,
            }
        })
    }

    pub fn run_no_fault(config: ThreeNodeRaftSimConfig) -> ThreeNodeRaftSimOutcome {
        Self::run_no_fault_report(config).outcome
    }

    pub fn run_no_fault_report(config: ThreeNodeRaftSimConfig) -> SimReport {
        run_with_madsim(config.seed, async move {
            SimReport {
                scenario: SimScenario::NoFaultBaseline,
                outcome: run_no_fault_inner(config).await,
            }
        })
    }

    pub fn run_snapshot_catch_up(config: ThreeNodeRaftSimConfig) -> ThreeNodeRaftSimOutcome {
        Self::run_snapshot_catch_up_report(config).outcome
    }

    pub fn run_snapshot_catch_up_report(config: ThreeNodeRaftSimConfig) -> SimReport {
        run_with_madsim(config.seed, async move {
            SimReport {
                scenario: SimScenario::SnapshotCatchUp,
                outcome: run_snapshot_catch_up_inner(config).await,
            }
        })
    }

    pub fn run_restart_follower(config: ThreeNodeRaftSimConfig) -> ThreeNodeRaftSimOutcome {
        Self::run_restart_follower_report(config).outcome
    }

    pub fn run_restart_follower_report(config: ThreeNodeRaftSimConfig) -> SimReport {
        run_with_madsim(config.seed, async move {
            SimReport {
                scenario: SimScenario::RestartFollower,
                outcome: run_restart_follower_inner(config).await,
            }
        })
    }

    pub fn run_cold_live_read(config: ThreeNodeRaftSimConfig) -> ThreeNodeRaftSimOutcome {
        Self::run_cold_live_read_report(config).outcome
    }

    pub fn run_cold_live_read_report(config: ThreeNodeRaftSimConfig) -> SimReport {
        Self::run_cold_live_read_with_options_report(config, None)
    }

    pub fn run_cold_live_read_with_options_report(
        config: ThreeNodeRaftSimConfig,
        corrupt_expected_node_id: Option<u64>,
    ) -> SimReport {
        run_with_madsim(config.seed, async move {
            SimReport {
                scenario: SimScenario::ColdLiveRead,
                outcome: run_cold_live_read_inner(config, corrupt_expected_node_id).await,
            }
        })
    }

    pub fn run_cold_read_fault(config: ThreeNodeRaftSimConfig) -> ThreeNodeRaftSimOutcome {
        Self::run_cold_read_fault_report(config).outcome
    }

    pub fn run_cold_read_fault_report(config: ThreeNodeRaftSimConfig) -> SimReport {
        Self::run_cold_read_fault_with_options_report(config, true)
    }

    pub fn run_cold_read_fault_with_options_report(
        config: ThreeNodeRaftSimConfig,
        inject_read_fault: bool,
    ) -> SimReport {
        run_with_madsim(config.seed, async move {
            SimReport {
                scenario: SimScenario::ColdReadFault,
                outcome: run_cold_read_fault_inner(config, inject_read_fault).await,
            }
        })
    }

    pub fn run_cold_write_fault(config: ThreeNodeRaftSimConfig) -> ThreeNodeRaftSimOutcome {
        Self::run_cold_write_fault_report(config).outcome
    }

    pub fn run_cold_write_fault_report(config: ThreeNodeRaftSimConfig) -> SimReport {
        Self::run_cold_write_fault_with_options_report(config, true)
    }

    pub fn run_cold_write_fault_with_options_report(
        config: ThreeNodeRaftSimConfig,
        inject_write_fault: bool,
    ) -> SimReport {
        run_with_madsim(config.seed, async move {
            SimReport {
                scenario: SimScenario::ColdWriteFault,
                outcome: run_cold_write_fault_inner(config, inject_write_fault).await,
            }
        })
    }

    pub fn run_cold_write_delay(config: ThreeNodeRaftSimConfig) -> ThreeNodeRaftSimOutcome {
        Self::run_cold_write_delay_report(config).outcome
    }

    pub fn run_cold_write_delay_report(config: ThreeNodeRaftSimConfig) -> SimReport {
        Self::run_cold_write_delay_with_options_report(config, Some(250))
    }

    pub fn run_cold_write_delay_with_options_report(
        config: ThreeNodeRaftSimConfig,
        delay_ms: Option<u64>,
    ) -> SimReport {
        run_with_madsim(config.seed, async move {
            SimReport {
                scenario: SimScenario::ColdWriteDelay,
                outcome: run_cold_write_delay_inner(config, delay_ms).await,
            }
        })
    }

    pub fn run_cold_delete_fault(config: ThreeNodeRaftSimConfig) -> ThreeNodeRaftSimOutcome {
        Self::run_cold_delete_fault_report(config).outcome
    }

    pub fn run_cold_delete_fault_report(config: ThreeNodeRaftSimConfig) -> SimReport {
        Self::run_cold_delete_fault_with_options_report(config, true)
    }

    pub fn run_cold_delete_fault_with_options_report(
        config: ThreeNodeRaftSimConfig,
        inject_delete_fault: bool,
    ) -> SimReport {
        run_with_madsim(config.seed, async move {
            SimReport {
                scenario: SimScenario::ColdDeleteFault,
                outcome: run_cold_delete_fault_inner(config, inject_delete_fault).await,
            }
        })
    }

    pub fn run_cold_read_delay(config: ThreeNodeRaftSimConfig) -> ThreeNodeRaftSimOutcome {
        Self::run_cold_read_delay_report(config).outcome
    }

    pub fn run_cold_read_delay_report(config: ThreeNodeRaftSimConfig) -> SimReport {
        Self::run_cold_read_delay_with_options_report(config, Some(250))
    }

    pub fn run_cold_read_delay_with_options_report(
        config: ThreeNodeRaftSimConfig,
        delay_ms: Option<u64>,
    ) -> SimReport {
        run_with_madsim(config.seed, async move {
            SimReport {
                scenario: SimScenario::ColdReadDelay,
                outcome: run_cold_read_delay_inner(config, delay_ms).await,
            }
        })
    }

    pub fn run_cold_read_truncate(config: ThreeNodeRaftSimConfig) -> ThreeNodeRaftSimOutcome {
        Self::run_cold_read_truncate_report(config).outcome
    }

    pub fn run_cold_read_truncate_report(config: ThreeNodeRaftSimConfig) -> SimReport {
        Self::run_cold_read_truncate_with_options_report(config, Some(2))
    }

    pub fn run_cold_read_truncate_with_options_report(
        config: ThreeNodeRaftSimConfig,
        truncate_returned_len: Option<usize>,
    ) -> SimReport {
        run_with_madsim(config.seed, async move {
            SimReport {
                scenario: SimScenario::ColdReadTruncate,
                outcome: run_cold_read_truncate_inner(config, truncate_returned_len).await,
            }
        })
    }

    pub fn run_http_protocol_surface(config: ThreeNodeRaftSimConfig) -> ThreeNodeRaftSimOutcome {
        Self::run_http_protocol_surface_report(config).outcome
    }

    pub fn run_http_protocol_surface_report(config: ThreeNodeRaftSimConfig) -> SimReport {
        Self::run_http_protocol_surface_with_options_report(config, false)
    }

    pub fn run_http_protocol_surface_with_options_report(
        config: ThreeNodeRaftSimConfig,
        corrupt_snapshot_body_expectation: bool,
    ) -> SimReport {
        run_with_madsim(config.seed, async move {
            SimReport {
                scenario: SimScenario::HttpProtocolSurface,
                outcome: run_http_protocol_surface_inner(config, corrupt_snapshot_body_expectation)
                    .await,
            }
        })
    }

    pub fn run_http_protocol_surface_randomized(
        config: ThreeNodeRaftSimConfig,
    ) -> ThreeNodeRaftSimOutcome {
        Self::run_http_protocol_surface_randomized_report(config).outcome
    }

    pub fn run_http_protocol_surface_randomized_report(
        config: ThreeNodeRaftSimConfig,
    ) -> SimReport {
        let plan = HttpProtocolSurfacePlan::from_seed(config.seed);
        Self::run_http_protocol_surface_randomized_with_plan_report(config, plan)
    }

    pub fn run_http_protocol_surface_randomized_with_plan_report(
        config: ThreeNodeRaftSimConfig,
        plan: HttpProtocolSurfacePlan,
    ) -> SimReport {
        run_with_madsim(config.seed, async move {
            SimReport {
                scenario: SimScenario::HttpProtocolSurfaceRandomized,
                outcome: run_http_protocol_surface_randomized_inner(config, plan).await,
            }
        })
    }

    pub fn run_http_live_protocol_surface(
        config: ThreeNodeRaftSimConfig,
    ) -> ThreeNodeRaftSimOutcome {
        Self::run_http_live_protocol_surface_report(config).outcome
    }

    pub fn run_http_live_protocol_surface_report(config: ThreeNodeRaftSimConfig) -> SimReport {
        Self::run_http_live_protocol_surface_with_options_report(config, false)
    }

    pub fn run_http_live_protocol_surface_with_options_report(
        config: ThreeNodeRaftSimConfig,
        corrupt_sse_next_offset_expectation: bool,
    ) -> SimReport {
        run_with_madsim(config.seed, async move {
            SimReport {
                scenario: SimScenario::HttpLiveProtocolSurface,
                outcome: run_http_live_protocol_surface_inner(
                    config,
                    corrupt_sse_next_offset_expectation,
                )
                .await,
            }
        })
    }

    pub fn run_http_live_limit_protocol_surface(
        config: ThreeNodeRaftSimConfig,
    ) -> ThreeNodeRaftSimOutcome {
        Self::run_http_live_limit_protocol_surface_report(config).outcome
    }

    pub fn run_http_live_limit_protocol_surface_report(
        config: ThreeNodeRaftSimConfig,
    ) -> SimReport {
        Self::run_http_live_limit_protocol_surface_with_options_report(config, false)
    }

    pub fn run_http_live_limit_protocol_surface_with_options_report(
        config: ThreeNodeRaftSimConfig,
        corrupt_backpressure_expectation: bool,
    ) -> SimReport {
        run_with_madsim(config.seed, async move {
            SimReport {
                scenario: SimScenario::HttpLiveLimitProtocolSurface,
                outcome: run_http_live_limit_protocol_surface_inner(
                    config,
                    corrupt_backpressure_expectation,
                )
                .await,
            }
        })
    }

    pub fn run_http_producer_protocol_surface(
        config: ThreeNodeRaftSimConfig,
    ) -> ThreeNodeRaftSimOutcome {
        Self::run_http_producer_protocol_surface_report(config).outcome
    }

    pub fn run_http_producer_protocol_surface_report(config: ThreeNodeRaftSimConfig) -> SimReport {
        Self::run_http_producer_protocol_surface_with_options_report(config, false)
    }

    pub fn run_http_producer_protocol_surface_with_options_report(
        config: ThreeNodeRaftSimConfig,
        corrupt_duplicate_expectation: bool,
    ) -> SimReport {
        run_with_madsim(config.seed, async move {
            SimReport {
                scenario: SimScenario::HttpProducerProtocolSurface,
                outcome: run_http_producer_protocol_surface_inner(
                    config,
                    corrupt_duplicate_expectation,
                )
                .await,
            }
        })
    }

    pub fn run_runtime_actor_scheduling(config: ThreeNodeRaftSimConfig) -> ThreeNodeRaftSimOutcome {
        Self::run_runtime_actor_scheduling_report(config).outcome
    }

    pub fn run_runtime_actor_scheduling_report(config: ThreeNodeRaftSimConfig) -> SimReport {
        run_with_madsim(config.seed, async move {
            SimReport {
                scenario: SimScenario::RuntimeActorScheduling,
                outcome: run_runtime_actor_scheduling_inner(config).await,
            }
        })
    }

    pub fn run_runtime_multi_client_actors(
        config: ThreeNodeRaftSimConfig,
    ) -> ThreeNodeRaftSimOutcome {
        Self::run_runtime_multi_client_actors_report(config).outcome
    }

    pub fn run_runtime_multi_client_actors_report(config: ThreeNodeRaftSimConfig) -> SimReport {
        run_with_madsim(config.seed, async move {
            SimReport {
                scenario: SimScenario::RuntimeMultiClientActors,
                outcome: run_runtime_multi_client_actors_inner(config).await,
            }
        })
    }

    pub fn run_runtime_cold_flush_worker(
        config: ThreeNodeRaftSimConfig,
    ) -> ThreeNodeRaftSimOutcome {
        Self::run_runtime_cold_flush_worker_report(config).outcome
    }

    pub fn run_runtime_cold_flush_worker_report(config: ThreeNodeRaftSimConfig) -> SimReport {
        run_with_madsim(config.seed, async move {
            SimReport {
                scenario: SimScenario::RuntimeColdFlushWorker,
                outcome: run_runtime_cold_flush_worker_inner(config).await,
            }
        })
    }

    pub fn run_runtime_seeded_interleaving(
        config: ThreeNodeRaftSimConfig,
    ) -> ThreeNodeRaftSimOutcome {
        Self::run_runtime_seeded_interleaving_report(config).outcome
    }

    pub fn run_runtime_seeded_interleaving_report(config: ThreeNodeRaftSimConfig) -> SimReport {
        Self::run_runtime_seeded_interleaving_with_plan_report(
            config.clone(),
            RuntimeInterleavingPlan::from_seed(config.seed),
        )
    }

    pub fn run_runtime_seeded_interleaving_with_plan_report(
        config: ThreeNodeRaftSimConfig,
        plan: RuntimeInterleavingPlan,
    ) -> SimReport {
        run_with_madsim(config.seed, async move {
            SimReport {
                scenario: SimScenario::RuntimeSeededInterleaving,
                outcome: run_runtime_seeded_interleaving_inner(config, plan).await,
            }
        })
    }

    pub fn run_runtime_raft_engine(config: ThreeNodeRaftSimConfig) -> ThreeNodeRaftSimOutcome {
        Self::run_runtime_raft_engine_report(config).outcome
    }

    pub fn run_runtime_raft_engine_report(config: ThreeNodeRaftSimConfig) -> SimReport {
        run_with_madsim(config.seed, async move {
            SimReport {
                scenario: SimScenario::RuntimeRaftEngine,
                outcome: run_runtime_raft_engine_inner(config).await,
            }
        })
    }

    pub fn run_runtime_raft_snapshot_install(
        config: ThreeNodeRaftSimConfig,
    ) -> ThreeNodeRaftSimOutcome {
        Self::run_runtime_raft_snapshot_install_report(config).outcome
    }

    pub fn run_runtime_raft_snapshot_install_report(config: ThreeNodeRaftSimConfig) -> SimReport {
        Self::run_runtime_raft_snapshot_install_with_options_report(config, false)
    }

    pub fn run_runtime_raft_snapshot_install_with_options_report(
        config: ThreeNodeRaftSimConfig,
        corrupt_append_counts: bool,
    ) -> SimReport {
        run_with_madsim(config.seed, async move {
            SimReport {
                scenario: SimScenario::RuntimeRaftSnapshotInstall,
                outcome: run_runtime_raft_snapshot_install_inner(config, corrupt_append_counts)
                    .await,
            }
        })
    }

    pub fn run_runtime_raft_network(config: ThreeNodeRaftSimConfig) -> ThreeNodeRaftSimOutcome {
        Self::run_runtime_raft_network_report(config).outcome
    }

    pub fn run_runtime_raft_network_report(config: ThreeNodeRaftSimConfig) -> SimReport {
        Self::run_runtime_raft_network_with_options_report(
            config,
            RuntimeRaftNetworkOptions::default(),
        )
    }

    pub(super) fn run_runtime_raft_network_with_options_report(
        config: ThreeNodeRaftSimConfig,
        options: RuntimeRaftNetworkOptions,
    ) -> SimReport {
        run_with_madsim(config.seed, async move {
            SimReport {
                scenario: SimScenario::RuntimeRaftNetwork,
                outcome: run_runtime_raft_network_inner(config, options).await,
            }
        })
    }
}

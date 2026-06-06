//! `SimSchedule::generate_*` factory methods extracted from
//! `madsim_harness/mod.rs` (DoD #3 modularity refactor — scenarios axis).
//! Each method maps a u64 seed to a SimSchedule for a specific scenario.

use super::BucketStreamId;
use super::HttpProtocolSurfacePlan;
use super::RuntimeRaftNetworkWorkloadPlan;
use super::SimFaultAction;
use super::SimFaultPlan;
use super::SimFaultStep;
use super::SimScenario;
use super::SimSchedule;
use super::SplitMix64;
use super::has_stop_current_leader_in_fault_plan;
use super::has_verify_runtime_cold_live_reads_in_fault_plan;
use super::runtime_cold_read_truncate_len;
use super::runtime_corrupt_read_client_id;

impl SimSchedule {
    pub fn generate_runtime_interleaving_failure(seed: u64) -> Self {
        let mut schedule = Self::for_scenario(seed, SimScenario::RuntimeSeededInterleaving);
        for step in &mut schedule.fault_plan.steps {
            if let SimFaultAction::RunRuntimeSeededInterleaving { plan } = &mut step.action {
                plan.corrupt_read_client_id = Some(runtime_corrupt_read_client_id(seed, plan));
            }
        }
        schedule
    }

    pub fn generate_runtime_interleaving_truncate_failure(seed: u64) -> Self {
        let mut schedule = Self::for_scenario(seed, SimScenario::RuntimeSeededInterleaving);
        for step in &mut schedule.fault_plan.steps {
            if let SimFaultAction::RunRuntimeSeededInterleaving { plan } = &mut step.action {
                plan.runtime_cold_read_truncate_len = Some(runtime_cold_read_truncate_len(seed));
            }
        }
        schedule
    }

    pub fn generate_runtime_interleaving_write_failure(seed: u64) -> Self {
        let mut schedule = Self::for_scenario(seed, SimScenario::RuntimeSeededInterleaving);
        for step in &mut schedule.fault_plan.steps {
            if let SimFaultAction::RunRuntimeSeededInterleaving { plan } = &mut step.action {
                plan.runtime_cold_write_failure =
                    Some(format!("seeded runtime cold write fault for seed {seed}"));
            }
        }
        schedule
    }

    pub fn generate_http_producer_protocol_surface_failure(seed: u64) -> Self {
        let mut schedule = Self::for_scenario(seed, SimScenario::HttpProducerProtocolSurface);
        schedule.stream = BucketStreamId::new(
            "benchcmp",
            format!("ursula-sim-schedule-{seed}-http-producer-protocol-surface-failure"),
        );
        schedule.fault_plan.steps.push(SimFaultStep {
            phase: "after_http_duplicate_retry".to_owned(),
            action: SimFaultAction::CorruptHttpProducerDuplicateExpectation,
        });
        schedule
    }

    pub fn generate_http_live_protocol_surface_failure(seed: u64) -> Self {
        let mut schedule = Self::for_scenario(seed, SimScenario::HttpLiveProtocolSurface);
        schedule.stream = BucketStreamId::new(
            "benchcmp",
            format!("ursula-sim-schedule-{seed}-http-live-protocol-surface-failure"),
        );
        schedule.fault_plan.steps.push(SimFaultStep {
            phase: "after_http_sse_body".to_owned(),
            action: SimFaultAction::CorruptHttpLiveSseNextOffsetExpectation,
        });
        schedule
    }

    pub fn generate_http_live_limit_protocol_surface_failure(seed: u64) -> Self {
        let mut schedule = Self::for_scenario(seed, SimScenario::HttpLiveLimitProtocolSurface);
        schedule.stream = BucketStreamId::new(
            "benchcmp",
            format!("ursula-sim-schedule-{seed}-http-live-limit-protocol-surface-failure"),
        );
        schedule.fault_plan.steps.push(SimFaultStep {
            phase: "after_http_live_limit_metrics".to_owned(),
            action: SimFaultAction::CorruptHttpLiveLimitBackpressureExpectation,
        });
        schedule
    }

    pub fn generate_http_snapshot_protocol_surface_failure(seed: u64) -> Self {
        let mut schedule = Self::for_scenario(seed, SimScenario::HttpProtocolSurface);
        schedule.stream = BucketStreamId::new(
            "benchcmp",
            format!("ursula-sim-schedule-{seed}-http-snapshot-protocol-surface-failure"),
        );
        schedule.fault_plan.steps.push(SimFaultStep {
            phase: "after_http_snapshot_read".to_owned(),
            action: SimFaultAction::CorruptHttpSnapshotBodyExpectation,
        });
        schedule
    }

    pub fn generate_http_protocol_surface_randomized(seed: u64) -> Self {
        let stream = BucketStreamId::new(
            "benchcmp",
            format!("ursula-sim-schedule-{seed}-http-protocol-surface-randomized"),
        );
        Self {
            seed,
            scenario: SimScenario::HttpProtocolSurfaceRandomized,
            stream,
            fault_plan: SimFaultPlan {
                steps: vec![SimFaultStep {
                    phase: "http_protocol_surface_workload".to_owned(),
                    action: SimFaultAction::RunHttpProtocolSurfaceWorkload {
                        plan: HttpProtocolSurfacePlan::from_seed(seed),
                    },
                }],
            },
        }
    }

    pub fn generate_http_protocol_surface_randomized_failure(seed: u64) -> Self {
        let mut schedule = Self::generate_http_protocol_surface_randomized(seed);
        schedule.stream = BucketStreamId::new(
            "benchcmp",
            format!("ursula-sim-schedule-{seed}-http-protocol-surface-randomized-failure"),
        );
        for step in &mut schedule.fault_plan.steps {
            if let SimFaultAction::RunHttpProtocolSurfaceWorkload { plan } = &mut step.action {
                plan.corrupt_final_read_expectation = true;
            }
        }
        schedule
    }

    pub fn generate_http_protocol_surface_randomized_sse_failure(seed: u64) -> Self {
        let mut schedule = Self::generate_http_protocol_surface_randomized(seed);
        schedule.stream = BucketStreamId::new(
            "benchcmp",
            format!("ursula-sim-schedule-{seed}-http-protocol-surface-randomized-sse-failure"),
        );
        for step in &mut schedule.fault_plan.steps {
            if let SimFaultAction::RunHttpProtocolSurfaceWorkload { plan } = &mut step.action {
                plan.sse_close = true;
                plan.corrupt_sse_next_offset_expectation = true;
            }
        }
        schedule
    }

    pub fn generate_http_protocol_surface_randomized_backpressure_failure(seed: u64) -> Self {
        let mut schedule = Self::generate_http_protocol_surface_randomized(seed);
        schedule.stream = BucketStreamId::new(
            "benchcmp",
            format!(
                "ursula-sim-schedule-{seed}-http-protocol-surface-randomized-backpressure-failure"
            ),
        );
        for step in &mut schedule.fault_plan.steps {
            if let SimFaultAction::RunHttpProtocolSurfaceWorkload { plan } = &mut step.action {
                plan.live_limit = true;
                plan.corrupt_live_limit_backpressure_expectation = true;
            }
        }
        schedule
    }

    pub fn generate_raft_partition_failure(seed: u64) -> Self {
        let stream = BucketStreamId::new(
            "benchcmp",
            format!("ursula-sim-schedule-{seed}-raft-partition-failure"),
        );
        Self {
            seed,
            scenario: SimScenario::PartitionHeal,
            stream,
            fault_plan: SimFaultPlan {
                steps: vec![SimFaultStep {
                    phase: "before_append".to_owned(),
                    action: SimFaultAction::PartitionSeededFollower,
                }],
            },
        }
    }

    pub fn generate_runtime_raft_network_partition_failure(seed: u64) -> Self {
        let stream = BucketStreamId::new(
            "benchcmp",
            format!("ursula-sim-schedule-{seed}-runtime-raft-network-partition-failure"),
        );
        Self {
            seed,
            scenario: SimScenario::RuntimeRaftNetwork,
            stream,
            fault_plan: SimFaultPlan {
                steps: vec![SimFaultStep {
                    phase: "before_append".to_owned(),
                    action: SimFaultAction::PartitionSeededFollower,
                }],
            },
        }
    }

    pub fn generate_runtime_raft_network_recovery(seed: u64) -> Self {
        let stream = BucketStreamId::new(
            "benchcmp",
            format!("ursula-sim-schedule-{seed}-runtime-raft-network-recovery"),
        );
        Self {
            seed,
            scenario: SimScenario::RuntimeRaftNetwork,
            stream,
            fault_plan: SimFaultPlan {
                steps: vec![
                    SimFaultStep {
                        phase: "before_append".to_owned(),
                        action: SimFaultAction::PartitionSeededFollower,
                    },
                    SimFaultStep {
                        phase: "after_isolated_lag".to_owned(),
                        action: SimFaultAction::HealSeededFollower,
                    },
                ],
            },
        }
    }

    pub fn generate_runtime_raft_network_cold_live_recovery(seed: u64) -> Self {
        let stream = BucketStreamId::new(
            "benchcmp",
            format!("ursula-sim-schedule-{seed}-runtime-raft-network-cold-live-recovery"),
        );
        Self {
            seed,
            scenario: SimScenario::RuntimeRaftNetwork,
            stream,
            fault_plan: SimFaultPlan {
                steps: vec![
                    SimFaultStep {
                        phase: "before_append".to_owned(),
                        action: SimFaultAction::PartitionSeededFollower,
                    },
                    SimFaultStep {
                        phase: "after_isolated_lag".to_owned(),
                        action: SimFaultAction::HealSeededFollower,
                    },
                    SimFaultStep {
                        phase: "after_recovery_read".to_owned(),
                        action: SimFaultAction::VerifyRuntimeColdLiveReads,
                    },
                ],
            },
        }
    }

    pub fn generate_runtime_raft_network_cold_live_restart(seed: u64) -> Self {
        let stream = BucketStreamId::new(
            "benchcmp",
            format!("ursula-sim-schedule-{seed}-runtime-raft-network-cold-live-restart"),
        );
        Self {
            seed,
            scenario: SimScenario::RuntimeRaftNetwork,
            stream,
            fault_plan: SimFaultPlan {
                steps: vec![
                    SimFaultStep {
                        phase: "before_append".to_owned(),
                        action: SimFaultAction::PartitionSeededFollower,
                    },
                    SimFaultStep {
                        phase: "after_isolated_lag".to_owned(),
                        action: SimFaultAction::HealSeededFollower,
                    },
                    SimFaultStep {
                        phase: "after_recovery_read".to_owned(),
                        action: SimFaultAction::VerifyRuntimeColdLiveReads,
                    },
                    SimFaultStep {
                        phase: "before_cold_flush".to_owned(),
                        action: SimFaultAction::StopSeededFollower,
                    },
                    SimFaultStep {
                        phase: "after_cold_flush".to_owned(),
                        action: SimFaultAction::RestartStoppedFollower,
                    },
                ],
            },
        }
    }

    pub fn generate_runtime_raft_network_cold_live_write_recovery(seed: u64) -> Self {
        let stream = BucketStreamId::new(
            "benchcmp",
            format!("ursula-sim-schedule-{seed}-runtime-raft-network-cold-live-write-recovery"),
        );
        Self {
            seed,
            scenario: SimScenario::RuntimeRaftNetwork,
            stream,
            fault_plan: SimFaultPlan {
                steps: vec![
                    SimFaultStep {
                        phase: "before_append".to_owned(),
                        action: SimFaultAction::PartitionSeededFollower,
                    },
                    SimFaultStep {
                        phase: "after_isolated_lag".to_owned(),
                        action: SimFaultAction::HealSeededFollower,
                    },
                    SimFaultStep {
                        phase: "after_recovery_read".to_owned(),
                        action: SimFaultAction::VerifyRuntimeColdLiveReads,
                    },
                    SimFaultStep {
                        phase: "before_cold_write".to_owned(),
                        action: SimFaultAction::FailNextColdWrite,
                    },
                    SimFaultStep {
                        phase: "after_cold_write_failure".to_owned(),
                        action: SimFaultAction::RetryColdWriteAfterFailure,
                    },
                ],
            },
        }
    }

    pub fn generate_runtime_raft_network_leader_failover(seed: u64) -> Self {
        let stream = BucketStreamId::new(
            "benchcmp",
            format!("ursula-sim-schedule-{seed}-runtime-raft-network-leader-failover"),
        );
        Self {
            seed,
            scenario: SimScenario::RuntimeRaftNetwork,
            stream,
            fault_plan: SimFaultPlan {
                steps: vec![
                    SimFaultStep {
                        phase: "after_runtime_read".to_owned(),
                        action: SimFaultAction::StopCurrentLeader,
                    },
                    SimFaultStep {
                        phase: "after_failover_append".to_owned(),
                        action: SimFaultAction::RestartStoppedLeader,
                    },
                ],
            },
        }
    }

    pub fn generate_runtime_raft_network_randomized(seed: u64) -> Self {
        let stream = BucketStreamId::new(
            "benchcmp",
            format!("ursula-sim-schedule-{seed}-runtime-raft-network-randomized"),
        );
        let mut rng = SplitMix64::new(seed ^ 0x7274_7261_6674_6e65);
        let workload_plan = RuntimeRaftNetworkWorkloadPlan::from_seed(seed);
        let partition_and_heal = rng.next_bounded(2) == 0;
        let verify_cold_live = rng.next_bounded(4) != 0;
        let restart_during_cold_flush = verify_cold_live && rng.next_bounded(3) == 0;
        let leader_failover = rng.next_bounded(2) == 0;
        let retry_cold_write_after_failure = verify_cold_live && rng.next_bounded(4) == 0;
        let delay_cold_write_ms =
            if verify_cold_live && !retry_cold_write_after_failure && seed % 17 == 10 {
                Some(125)
            } else {
                None
            };
        let retry_cold_read_after_truncate = verify_cold_live && seed.is_multiple_of(5);
        let delay_cold_read_ms =
            if verify_cold_live && !retry_cold_read_after_truncate && seed % 11 == 4 {
                Some(125)
            } else {
                None
            };
        let mut steps = Vec::new();
        if partition_and_heal {
            steps.push(SimFaultStep {
                phase: "before_append".to_owned(),
                action: SimFaultAction::PartitionSeededFollower,
            });
            steps.push(SimFaultStep {
                phase: "after_isolated_lag".to_owned(),
                action: SimFaultAction::HealSeededFollower,
            });
        }
        if leader_failover {
            steps.push(SimFaultStep {
                phase: "after_runtime_read".to_owned(),
                action: SimFaultAction::StopCurrentLeader,
            });
            steps.push(SimFaultStep {
                phase: "after_failover_append".to_owned(),
                action: SimFaultAction::RestartStoppedLeader,
            });
        }
        steps.push(SimFaultStep {
            phase: "runtime_raft_network_workload".to_owned(),
            action: SimFaultAction::RunRuntimeRaftNetworkWorkload {
                plan: workload_plan,
            },
        });
        if verify_cold_live {
            steps.push(SimFaultStep {
                phase: "after_recovery_read".to_owned(),
                action: SimFaultAction::VerifyRuntimeColdLiveReads,
            });
            if retry_cold_write_after_failure {
                steps.push(SimFaultStep {
                    phase: "before_cold_write".to_owned(),
                    action: SimFaultAction::FailNextColdWrite,
                });
                steps.push(SimFaultStep {
                    phase: "after_cold_write_failure".to_owned(),
                    action: SimFaultAction::RetryColdWriteAfterFailure,
                });
            }
            if let Some(delay_ms) = delay_cold_write_ms {
                steps.push(SimFaultStep {
                    phase: "before_cold_write".to_owned(),
                    action: SimFaultAction::DelayNextColdWrite { delay_ms },
                });
            }
            if restart_during_cold_flush {
                steps.push(SimFaultStep {
                    phase: "before_cold_flush".to_owned(),
                    action: SimFaultAction::StopSeededFollower,
                });
                steps.push(SimFaultStep {
                    phase: "after_cold_flush".to_owned(),
                    action: SimFaultAction::RestartStoppedFollower,
                });
            }
            if let Some(delay_ms) = delay_cold_read_ms {
                steps.push(SimFaultStep {
                    phase: "before_cold_read".to_owned(),
                    action: SimFaultAction::DelayNextColdRead { delay_ms },
                });
            }
            if retry_cold_read_after_truncate {
                steps.push(SimFaultStep {
                    phase: "before_cold_read".to_owned(),
                    action: SimFaultAction::TruncateNextColdRead { returned_len: 0 },
                });
                steps.push(SimFaultStep {
                    phase: "after_cold_read_failure".to_owned(),
                    action: SimFaultAction::RetryColdReadAfterFailure,
                });
            }
        }
        if steps.is_empty() {
            steps.push(SimFaultStep {
                phase: "after_recovery_read".to_owned(),
                action: SimFaultAction::VerifyRuntimeColdLiveReads,
            });
        }
        Self {
            seed,
            scenario: SimScenario::RuntimeRaftNetwork,
            stream,
            fault_plan: SimFaultPlan { steps },
        }
    }

    pub fn generate_runtime_raft_network_randomized_failure(seed: u64) -> Self {
        let mut schedule = Self::generate_runtime_raft_network_randomized(seed);
        schedule.stream = BucketStreamId::new(
            "benchcmp",
            format!("ursula-sim-schedule-{seed}-runtime-raft-network-randomized-failure"),
        );
        for step in &mut schedule.fault_plan.steps {
            if let SimFaultAction::RunRuntimeRaftNetworkWorkload { plan } = &mut step.action {
                plan.corrupt_read_expectation = true;
            }
        }
        schedule
    }

    pub fn generate_runtime_raft_network_partial_read_failure(seed: u64) -> Self {
        let mut schedule = Self::generate_runtime_raft_network_randomized(seed);
        schedule.stream = BucketStreamId::new(
            "benchcmp",
            format!("ursula-sim-schedule-{seed}-runtime-raft-network-partial-read-failure"),
        );
        for step in &mut schedule.fault_plan.steps {
            if let SimFaultAction::RunRuntimeRaftNetworkWorkload { plan } = &mut step.action {
                plan.partial_reads = true;
                plan.corrupt_partial_read_expectation = true;
            }
        }
        schedule
    }

    pub fn generate_runtime_raft_network_tail_read_failure(seed: u64) -> Self {
        let mut schedule = Self::generate_runtime_raft_network_randomized(seed);
        schedule.stream = BucketStreamId::new(
            "benchcmp",
            format!("ursula-sim-schedule-{seed}-runtime-raft-network-tail-read-failure"),
        );
        for step in &mut schedule.fault_plan.steps {
            if let SimFaultAction::RunRuntimeRaftNetworkWorkload { plan } = &mut step.action {
                plan.tail_reads = true;
                plan.corrupt_tail_read_expectation = true;
            }
        }
        schedule
    }

    pub fn generate_runtime_raft_network_close_failure(seed: u64) -> Self {
        let mut schedule = Self::generate_runtime_raft_network_randomized(seed);
        schedule.stream = BucketStreamId::new(
            "benchcmp",
            format!("ursula-sim-schedule-{seed}-runtime-raft-network-close-failure"),
        );
        for step in &mut schedule.fault_plan.steps {
            if let SimFaultAction::RunRuntimeRaftNetworkWorkload { plan } = &mut step.action {
                plan.close_streams = true;
                plan.corrupt_close_state_expectation = true;
            }
        }
        schedule
    }

    pub fn generate_runtime_raft_network_snapshot_failure(seed: u64) -> Self {
        let mut schedule = Self::generate_runtime_raft_network_randomized(seed);
        schedule.stream = BucketStreamId::new(
            "benchcmp",
            format!("ursula-sim-schedule-{seed}-runtime-raft-network-snapshot-failure"),
        );
        for step in &mut schedule.fault_plan.steps {
            if let SimFaultAction::RunRuntimeRaftNetworkWorkload { plan } = &mut step.action {
                plan.publish_snapshots = true;
                plan.corrupt_snapshot_expectation = true;
            }
        }
        schedule
    }

    pub fn generate_runtime_raft_network_leader_failover_read_failure(seed: u64) -> Self {
        let mut schedule = Self::generate_runtime_raft_network_randomized(seed);
        schedule.stream = BucketStreamId::new(
            "benchcmp",
            format!("ursula-sim-schedule-{seed}-runtime-raft-network-leader-failover-read-failure"),
        );
        if !has_stop_current_leader_in_fault_plan(&schedule.fault_plan) {
            schedule.fault_plan.steps.push(SimFaultStep {
                phase: "after_runtime_read".to_owned(),
                action: SimFaultAction::StopCurrentLeader,
            });
            schedule.fault_plan.steps.push(SimFaultStep {
                phase: "after_failover_append".to_owned(),
                action: SimFaultAction::RestartStoppedLeader,
            });
        }
        for step in &mut schedule.fault_plan.steps {
            if let SimFaultAction::RunRuntimeRaftNetworkWorkload { plan } = &mut step.action {
                plan.corrupt_leader_failover_read_expectation = true;
            }
        }
        schedule
    }

    pub fn generate_runtime_raft_network_leader_failover_cold_live_read_failure(seed: u64) -> Self {
        let stream = BucketStreamId::new(
            "benchcmp",
            format!(
                "ursula-sim-schedule-{seed}-runtime-raft-network-leader-failover-cold-live-read-failure"
            ),
        );
        Self {
            seed,
            scenario: SimScenario::RuntimeRaftNetwork,
            stream,
            fault_plan: SimFaultPlan {
                steps: vec![
                    SimFaultStep {
                        phase: "after_runtime_read".to_owned(),
                        action: SimFaultAction::StopCurrentLeader,
                    },
                    SimFaultStep {
                        phase: "after_failover_append".to_owned(),
                        action: SimFaultAction::RestartStoppedLeader,
                    },
                    SimFaultStep {
                        phase: "runtime_raft_network_workload".to_owned(),
                        action: SimFaultAction::RunRuntimeRaftNetworkWorkload {
                            plan: RuntimeRaftNetworkWorkloadPlan {
                                stream_count: 1,
                                append_batch_lens: vec![1],
                                failover_batch_lens: vec![1],
                                producer_sessions: false,
                                producer_epoch_bumps: false,
                                concurrent_producers: false,
                                partial_reads: false,
                                tail_reads: false,
                                close_streams: false,
                                publish_snapshots: false,
                                corrupt_read_expectation: false,
                                corrupt_partial_read_expectation: false,
                                corrupt_tail_read_expectation: false,
                                corrupt_close_state_expectation: false,
                                corrupt_snapshot_expectation: false,
                                corrupt_leader_failover_read_expectation: false,
                            },
                        },
                    },
                    SimFaultStep {
                        phase: "after_recovery_read".to_owned(),
                        action: SimFaultAction::VerifyRuntimeColdLiveReads,
                    },
                    SimFaultStep {
                        phase: "before_cold_read".to_owned(),
                        action: SimFaultAction::TruncateNextColdRead { returned_len: 0 },
                    },
                ],
            },
        }
    }

    pub fn generate_runtime_raft_network_randomized_cold_read_failure(seed: u64) -> Self {
        let mut schedule = Self::generate_runtime_raft_network_randomized(seed);
        schedule.stream = BucketStreamId::new(
            "benchcmp",
            format!("ursula-sim-schedule-{seed}-runtime-raft-network-randomized-cold-read-failure"),
        );
        if !has_verify_runtime_cold_live_reads_in_fault_plan(&schedule.fault_plan) {
            schedule.fault_plan.steps.push(SimFaultStep {
                phase: "after_recovery_read".to_owned(),
                action: SimFaultAction::VerifyRuntimeColdLiveReads,
            });
        }
        schedule.fault_plan.steps.retain(|step| {
            !matches!(
                step.action,
                SimFaultAction::RetryColdReadAfterFailure
                    | SimFaultAction::TruncateNextColdRead { .. }
            )
        });
        schedule.fault_plan.steps.push(SimFaultStep {
            phase: "before_cold_read".to_owned(),
            action: SimFaultAction::TruncateNextColdRead { returned_len: 0 },
        });
        schedule
    }

    pub fn generate_runtime_raft_network_cold_live_truncate_failure(seed: u64) -> Self {
        let stream = BucketStreamId::new(
            "benchcmp",
            format!("ursula-sim-schedule-{seed}-runtime-raft-network-cold-live-truncate-failure"),
        );
        Self {
            seed,
            scenario: SimScenario::RuntimeRaftNetwork,
            stream,
            fault_plan: SimFaultPlan {
                steps: vec![
                    SimFaultStep {
                        phase: "before_append".to_owned(),
                        action: SimFaultAction::PartitionSeededFollower,
                    },
                    SimFaultStep {
                        phase: "after_isolated_lag".to_owned(),
                        action: SimFaultAction::HealSeededFollower,
                    },
                    SimFaultStep {
                        phase: "after_recovery_read".to_owned(),
                        action: SimFaultAction::VerifyRuntimeColdLiveReads,
                    },
                    SimFaultStep {
                        phase: "before_cold_read".to_owned(),
                        action: SimFaultAction::TruncateNextColdRead { returned_len: 2 },
                    },
                ],
            },
        }
    }

    pub fn generate_runtime_raft_network_cold_live_write_failure(seed: u64) -> Self {
        let stream = BucketStreamId::new(
            "benchcmp",
            format!("ursula-sim-schedule-{seed}-runtime-raft-network-cold-live-write-failure"),
        );
        Self {
            seed,
            scenario: SimScenario::RuntimeRaftNetwork,
            stream,
            fault_plan: SimFaultPlan {
                steps: vec![
                    SimFaultStep {
                        phase: "before_append".to_owned(),
                        action: SimFaultAction::PartitionSeededFollower,
                    },
                    SimFaultStep {
                        phase: "after_isolated_lag".to_owned(),
                        action: SimFaultAction::HealSeededFollower,
                    },
                    SimFaultStep {
                        phase: "after_recovery_read".to_owned(),
                        action: SimFaultAction::VerifyRuntimeColdLiveReads,
                    },
                    SimFaultStep {
                        phase: "before_cold_write".to_owned(),
                        action: SimFaultAction::FailNextColdWrite,
                    },
                ],
            },
        }
    }

    pub fn generate_runtime_raft_snapshot_install_failure(seed: u64) -> Self {
        let stream = BucketStreamId::new(
            "benchcmp",
            format!("ursula-sim-schedule-{seed}-runtime-raft-snapshot-install-failure"),
        );
        Self {
            seed,
            scenario: SimScenario::RuntimeRaftSnapshotInstall,
            stream,
            fault_plan: SimFaultPlan {
                steps: vec![SimFaultStep {
                    phase: "after_snapshot_capture".to_owned(),
                    action: SimFaultAction::CorruptRuntimeRaftSnapshotAppendCounts,
                }],
            },
        }
    }
}

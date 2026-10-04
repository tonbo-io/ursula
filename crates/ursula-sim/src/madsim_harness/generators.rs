//! `SimSchedule::generate_*` factory methods extracted from
//! `madsim_harness/mod.rs` (DoD #3 modularity refactor — scenarios axis).
//! Each method maps a u64 seed to a SimSchedule for a specific scenario.

use super::BucketStreamId;
use super::HttpProtocolSurfacePlan;
use super::RuntimeInterleavingPlan;
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

/// Stream id shared by every generated schedule:
/// `benchcmp/ursula-sim-schedule-{seed}-{suffix}`.
fn seeded_stream(seed: u64, suffix: &str) -> BucketStreamId {
    BucketStreamId::new("benchcmp", format!("ursula-sim-schedule-{seed}-{suffix}"))
}

fn step(phase: &str, action: SimFaultAction) -> SimFaultStep {
    SimFaultStep {
        phase: phase.to_owned(),
        action,
    }
}

impl SimSchedule {
    /// Fixed-step schedule: seed, scenario, suffixed stream name, and a
    /// literal fault-step list.
    fn from_steps(
        seed: u64,
        scenario: SimScenario,
        suffix: &str,
        steps: Vec<SimFaultStep>,
    ) -> Self {
        Self {
            seed,
            scenario,
            stream: seeded_stream(seed, suffix),
            fault_plan: SimFaultPlan { steps },
        }
    }

    /// Seeded-scenario schedule with a suffixed stream name and one extra
    /// corrupt-expectation step (the HTTP protocol-surface failure family).
    fn seeded_with_failure_step(
        seed: u64,
        scenario: SimScenario,
        suffix: &str,
        phase: &str,
        action: SimFaultAction,
    ) -> Self {
        let mut schedule = Self::for_scenario(seed, scenario);
        schedule.stream = seeded_stream(seed, suffix);
        schedule.fault_plan.steps.push(step(phase, action));
        schedule
    }

    /// Seeded interleaving schedule with its workload plan mutated in place.
    fn interleaving_with(seed: u64, mutate: impl Fn(&mut RuntimeInterleavingPlan)) -> Self {
        let mut schedule = Self::for_scenario(seed, SimScenario::RuntimeSeededInterleaving);
        for step in &mut schedule.fault_plan.steps {
            if let SimFaultAction::RunRuntimeSeededInterleaving { plan } = &mut step.action {
                mutate(plan);
            }
        }
        schedule
    }

    /// Randomized raft-network schedule with a suffixed stream name and its
    /// workload plan mutated in place.
    fn raft_network_randomized_with(
        seed: u64,
        suffix: &str,
        mutate: impl Fn(&mut RuntimeRaftNetworkWorkloadPlan),
    ) -> Self {
        let mut schedule = Self::generate_runtime_raft_network_randomized(seed);
        schedule.stream = seeded_stream(seed, suffix);
        for step in &mut schedule.fault_plan.steps {
            if let SimFaultAction::RunRuntimeRaftNetworkWorkload { plan } = &mut step.action {
                mutate(plan);
            }
        }
        schedule
    }

    /// Randomized HTTP protocol-surface schedule with a suffixed stream name
    /// and its surface plan mutated in place.
    fn http_randomized_with(
        seed: u64,
        suffix: &str,
        mutate: impl Fn(&mut HttpProtocolSurfacePlan),
    ) -> Self {
        let mut schedule = Self::generate_http_protocol_surface_randomized(seed);
        schedule.stream = seeded_stream(seed, suffix);
        for step in &mut schedule.fault_plan.steps {
            if let SimFaultAction::RunHttpProtocolSurfaceWorkload { plan } = &mut step.action {
                mutate(plan);
            }
        }
        schedule
    }

    pub fn generate_runtime_interleaving_failure(seed: u64) -> Self {
        Self::interleaving_with(seed, |plan| {
            plan.corrupt_read_client_id = Some(runtime_corrupt_read_client_id(seed, plan));
        })
    }

    pub fn generate_runtime_interleaving_truncate_failure(seed: u64) -> Self {
        Self::interleaving_with(seed, |plan| {
            plan.runtime_cold_read_truncate_len = Some(runtime_cold_read_truncate_len(seed));
        })
    }

    pub fn generate_runtime_interleaving_write_failure(seed: u64) -> Self {
        Self::interleaving_with(seed, |plan| {
            plan.runtime_cold_write_failure =
                Some(format!("seeded runtime cold write fault for seed {seed}"));
        })
    }

    pub fn generate_http_producer_protocol_surface_failure(seed: u64) -> Self {
        Self::seeded_with_failure_step(
            seed,
            SimScenario::HttpProducerProtocolSurface,
            "http-producer-protocol-surface-failure",
            "after_http_duplicate_retry",
            SimFaultAction::CorruptHttpProducerDuplicateExpectation,
        )
    }

    pub fn generate_http_live_protocol_surface_failure(seed: u64) -> Self {
        Self::seeded_with_failure_step(
            seed,
            SimScenario::HttpLiveProtocolSurface,
            "http-live-protocol-surface-failure",
            "after_http_sse_body",
            SimFaultAction::CorruptHttpLiveSseNextOffsetExpectation,
        )
    }

    pub fn generate_http_live_limit_protocol_surface_failure(seed: u64) -> Self {
        Self::seeded_with_failure_step(
            seed,
            SimScenario::HttpLiveLimitProtocolSurface,
            "http-live-limit-protocol-surface-failure",
            "after_http_live_limit_metrics",
            SimFaultAction::CorruptHttpLiveLimitBackpressureExpectation,
        )
    }

    pub fn generate_http_snapshot_protocol_surface_failure(seed: u64) -> Self {
        Self::seeded_with_failure_step(
            seed,
            SimScenario::HttpProtocolSurface,
            "http-snapshot-protocol-surface-failure",
            "after_http_snapshot_read",
            SimFaultAction::CorruptHttpSnapshotBodyExpectation,
        )
    }

    pub fn generate_http_protocol_surface_randomized(seed: u64) -> Self {
        Self::from_steps(
            seed,
            SimScenario::HttpProtocolSurfaceRandomized,
            "http-protocol-surface-randomized",
            vec![step(
                "http_protocol_surface_workload",
                SimFaultAction::RunHttpProtocolSurfaceWorkload {
                    plan: HttpProtocolSurfacePlan::from_seed(seed),
                },
            )],
        )
    }

    pub fn generate_http_protocol_surface_randomized_failure(seed: u64) -> Self {
        Self::http_randomized_with(seed, "http-protocol-surface-randomized-failure", |plan| {
            plan.corrupt_final_read_expectation = true;
        })
    }

    pub fn generate_http_protocol_surface_randomized_sse_failure(seed: u64) -> Self {
        Self::http_randomized_with(
            seed,
            "http-protocol-surface-randomized-sse-failure",
            |plan| {
                plan.sse_close = true;
                plan.corrupt_sse_next_offset_expectation = true;
            },
        )
    }

    pub fn generate_http_protocol_surface_randomized_backpressure_failure(seed: u64) -> Self {
        Self::http_randomized_with(
            seed,
            "http-protocol-surface-randomized-backpressure-failure",
            |plan| {
                plan.live_limit = true;
                plan.corrupt_live_limit_backpressure_expectation = true;
            },
        )
    }

    pub fn generate_raft_partition_failure(seed: u64) -> Self {
        Self::from_steps(
            seed,
            SimScenario::PartitionHeal,
            "raft-partition-failure",
            vec![step(
                "before_append",
                SimFaultAction::PartitionSeededFollower,
            )],
        )
    }

    pub fn generate_runtime_raft_network_partition_failure(seed: u64) -> Self {
        Self::from_steps(
            seed,
            SimScenario::RuntimeRaftNetwork,
            "runtime-raft-network-partition-failure",
            vec![step(
                "before_append",
                SimFaultAction::PartitionSeededFollower,
            )],
        )
    }

    pub fn generate_runtime_raft_network_recovery(seed: u64) -> Self {
        Self::from_steps(
            seed,
            SimScenario::RuntimeRaftNetwork,
            "runtime-raft-network-recovery",
            vec![
                step("before_append", SimFaultAction::PartitionSeededFollower),
                step("after_isolated_lag", SimFaultAction::HealSeededFollower),
            ],
        )
    }

    pub fn generate_runtime_raft_network_cold_live_recovery(seed: u64) -> Self {
        Self::from_steps(
            seed,
            SimScenario::RuntimeRaftNetwork,
            "runtime-raft-network-cold-live-recovery",
            vec![
                step("before_append", SimFaultAction::PartitionSeededFollower),
                step("after_isolated_lag", SimFaultAction::HealSeededFollower),
                step(
                    "after_recovery_read",
                    SimFaultAction::VerifyRuntimeColdLiveReads,
                ),
            ],
        )
    }

    pub fn generate_runtime_raft_network_cold_live_restart(seed: u64) -> Self {
        Self::from_steps(
            seed,
            SimScenario::RuntimeRaftNetwork,
            "runtime-raft-network-cold-live-restart",
            vec![
                step("before_append", SimFaultAction::PartitionSeededFollower),
                step("after_isolated_lag", SimFaultAction::HealSeededFollower),
                step(
                    "after_recovery_read",
                    SimFaultAction::VerifyRuntimeColdLiveReads,
                ),
                step("before_cold_flush", SimFaultAction::StopSeededFollower),
                step("after_cold_flush", SimFaultAction::RestartStoppedFollower),
            ],
        )
    }

    pub fn generate_runtime_raft_network_cold_live_write_recovery(seed: u64) -> Self {
        Self::from_steps(
            seed,
            SimScenario::RuntimeRaftNetwork,
            "runtime-raft-network-cold-live-write-recovery",
            vec![
                step("before_append", SimFaultAction::PartitionSeededFollower),
                step("after_isolated_lag", SimFaultAction::HealSeededFollower),
                step(
                    "after_recovery_read",
                    SimFaultAction::VerifyRuntimeColdLiveReads,
                ),
                step("before_cold_write", SimFaultAction::FailNextColdWrite),
                step(
                    "after_cold_write_failure",
                    SimFaultAction::RetryColdWriteAfterFailure,
                ),
            ],
        )
    }

    pub fn generate_runtime_raft_network_leader_failover(seed: u64) -> Self {
        Self::from_steps(
            seed,
            SimScenario::RuntimeRaftNetwork,
            "runtime-raft-network-leader-failover",
            vec![
                step("after_runtime_read", SimFaultAction::StopCurrentLeader),
                step(
                    "after_failover_append",
                    SimFaultAction::RestartStoppedLeader,
                ),
            ],
        )
    }

    pub fn generate_runtime_raft_network_randomized(seed: u64) -> Self {
        let stream = seeded_stream(seed, "runtime-raft-network-randomized");
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
            steps.push(step(
                "before_append",
                SimFaultAction::PartitionSeededFollower,
            ));
            steps.push(step(
                "after_isolated_lag",
                SimFaultAction::HealSeededFollower,
            ));
        }
        if leader_failover {
            steps.push(step(
                "after_runtime_read",
                SimFaultAction::StopCurrentLeader,
            ));
            steps.push(step(
                "after_failover_append",
                SimFaultAction::RestartStoppedLeader,
            ));
        }
        steps.push(step(
            "runtime_raft_network_workload",
            SimFaultAction::RunRuntimeRaftNetworkWorkload {
                plan: workload_plan,
            },
        ));
        if verify_cold_live {
            steps.push(step(
                "after_recovery_read",
                SimFaultAction::VerifyRuntimeColdLiveReads,
            ));
            if retry_cold_write_after_failure {
                steps.push(step("before_cold_write", SimFaultAction::FailNextColdWrite));
                steps.push(step(
                    "after_cold_write_failure",
                    SimFaultAction::RetryColdWriteAfterFailure,
                ));
            }
            if let Some(delay_ms) = delay_cold_write_ms {
                steps.push(step(
                    "before_cold_write",
                    SimFaultAction::DelayNextColdWrite { delay_ms },
                ));
            }
            if restart_during_cold_flush {
                steps.push(step(
                    "before_cold_flush",
                    SimFaultAction::StopSeededFollower,
                ));
                steps.push(step(
                    "after_cold_flush",
                    SimFaultAction::RestartStoppedFollower,
                ));
            }
            if let Some(delay_ms) = delay_cold_read_ms {
                steps.push(step(
                    "before_cold_read",
                    SimFaultAction::DelayNextColdRead { delay_ms },
                ));
            }
            if retry_cold_read_after_truncate {
                steps.push(step(
                    "before_cold_read",
                    SimFaultAction::TruncateNextColdRead { returned_len: 0 },
                ));
                steps.push(step(
                    "after_cold_read_failure",
                    SimFaultAction::RetryColdReadAfterFailure,
                ));
            }
        }
        if steps.is_empty() {
            steps.push(step(
                "after_recovery_read",
                SimFaultAction::VerifyRuntimeColdLiveReads,
            ));
        }
        Self {
            seed,
            scenario: SimScenario::RuntimeRaftNetwork,
            stream,
            fault_plan: SimFaultPlan { steps },
        }
    }

    pub fn generate_runtime_raft_network_randomized_failure(seed: u64) -> Self {
        Self::raft_network_randomized_with(
            seed,
            "runtime-raft-network-randomized-failure",
            |plan| {
                plan.corrupt_read_expectation = true;
            },
        )
    }

    pub fn generate_runtime_raft_network_partial_read_failure(seed: u64) -> Self {
        Self::raft_network_randomized_with(
            seed,
            "runtime-raft-network-partial-read-failure",
            |plan| {
                plan.partial_reads = true;
                plan.corrupt_partial_read_expectation = true;
            },
        )
    }

    pub fn generate_runtime_raft_network_tail_read_failure(seed: u64) -> Self {
        Self::raft_network_randomized_with(seed, "runtime-raft-network-tail-read-failure", |plan| {
            plan.tail_reads = true;
            plan.corrupt_tail_read_expectation = true;
        })
    }

    pub fn generate_runtime_raft_network_close_failure(seed: u64) -> Self {
        Self::raft_network_randomized_with(seed, "runtime-raft-network-close-failure", |plan| {
            plan.close_streams = true;
            plan.corrupt_close_state_expectation = true;
        })
    }

    pub fn generate_runtime_raft_network_snapshot_failure(seed: u64) -> Self {
        Self::raft_network_randomized_with(seed, "runtime-raft-network-snapshot-failure", |plan| {
            plan.publish_snapshots = true;
            plan.corrupt_snapshot_expectation = true;
        })
    }

    pub fn generate_runtime_raft_network_leader_failover_read_failure(seed: u64) -> Self {
        let mut schedule = Self::raft_network_randomized_with(
            seed,
            "runtime-raft-network-leader-failover-read-failure",
            |plan| {
                plan.corrupt_leader_failover_read_expectation = true;
            },
        );
        if !has_stop_current_leader_in_fault_plan(&schedule.fault_plan) {
            schedule.fault_plan.steps.push(step(
                "after_runtime_read",
                SimFaultAction::StopCurrentLeader,
            ));
            schedule.fault_plan.steps.push(step(
                "after_failover_append",
                SimFaultAction::RestartStoppedLeader,
            ));
        }
        schedule
    }

    pub fn generate_runtime_raft_network_leader_failover_cold_live_read_failure(seed: u64) -> Self {
        Self::from_steps(
            seed,
            SimScenario::RuntimeRaftNetwork,
            "runtime-raft-network-leader-failover-cold-live-read-failure",
            vec![
                step("after_runtime_read", SimFaultAction::StopCurrentLeader),
                step(
                    "after_failover_append",
                    SimFaultAction::RestartStoppedLeader,
                ),
                step(
                    "runtime_raft_network_workload",
                    SimFaultAction::RunRuntimeRaftNetworkWorkload {
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
                ),
                step(
                    "after_recovery_read",
                    SimFaultAction::VerifyRuntimeColdLiveReads,
                ),
                step("before_cold_read", SimFaultAction::TruncateNextColdRead {
                    returned_len: 0,
                }),
            ],
        )
    }

    pub fn generate_runtime_raft_network_randomized_cold_read_failure(seed: u64) -> Self {
        let mut schedule = Self::generate_runtime_raft_network_randomized(seed);
        schedule.stream = seeded_stream(seed, "runtime-raft-network-randomized-cold-read-failure");
        if !has_verify_runtime_cold_live_reads_in_fault_plan(&schedule.fault_plan) {
            schedule.fault_plan.steps.push(step(
                "after_recovery_read",
                SimFaultAction::VerifyRuntimeColdLiveReads,
            ));
        }
        schedule.fault_plan.steps.retain(|step| {
            !matches!(
                step.action,
                SimFaultAction::RetryColdReadAfterFailure
                    | SimFaultAction::TruncateNextColdRead { .. }
            )
        });
        schedule.fault_plan.steps.push(step(
            "before_cold_read",
            SimFaultAction::TruncateNextColdRead { returned_len: 0 },
        ));
        schedule
    }

    pub fn generate_runtime_raft_network_cold_live_truncate_failure(seed: u64) -> Self {
        Self::from_steps(
            seed,
            SimScenario::RuntimeRaftNetwork,
            "runtime-raft-network-cold-live-truncate-failure",
            vec![
                step("before_append", SimFaultAction::PartitionSeededFollower),
                step("after_isolated_lag", SimFaultAction::HealSeededFollower),
                step(
                    "after_recovery_read",
                    SimFaultAction::VerifyRuntimeColdLiveReads,
                ),
                step("before_cold_read", SimFaultAction::TruncateNextColdRead {
                    returned_len: 2,
                }),
            ],
        )
    }

    pub fn generate_runtime_raft_network_cold_live_write_failure(seed: u64) -> Self {
        Self::from_steps(
            seed,
            SimScenario::RuntimeRaftNetwork,
            "runtime-raft-network-cold-live-write-failure",
            vec![
                step("before_append", SimFaultAction::PartitionSeededFollower),
                step("after_isolated_lag", SimFaultAction::HealSeededFollower),
                step(
                    "after_recovery_read",
                    SimFaultAction::VerifyRuntimeColdLiveReads,
                ),
                step("before_cold_write", SimFaultAction::FailNextColdWrite),
            ],
        )
    }

    pub fn generate_runtime_raft_snapshot_install_failure(seed: u64) -> Self {
        Self::from_steps(
            seed,
            SimScenario::RuntimeRaftSnapshotInstall,
            "runtime-raft-snapshot-install-failure",
            vec![step(
                "after_snapshot_capture",
                SimFaultAction::CorruptRuntimeRaftSnapshotAppendCounts,
            )],
        )
    }
}

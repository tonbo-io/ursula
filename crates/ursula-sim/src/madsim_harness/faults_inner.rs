//! Fault types extracted from `madsim_harness/mod.rs` (DoD #3 modularity
//! refactor — faults axis). Re-exported from the parent module so callers
//! that import via `crate::madsim_harness::*` or via lib.rs's
//! `pub use madsim_harness::{SimFaultAction, ...}` continue to compile.

use super::{
    Deserialize, Serialize, SimScenario, SplitMix64, default_runtime_flush_group_limit,
    is_default_runtime_flush_group_limit, is_false, runtime_cold_read_delay_ms_from_seed,
};

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SimFaultPlan {
    pub steps: Vec<SimFaultStep>,
}

impl SimFaultPlan {
    pub(super) fn for_scenario(scenario: SimScenario) -> Self {
        let steps = match scenario {
            SimScenario::NoFaultBaseline => Vec::new(),
            SimScenario::PartitionHeal => vec![
                SimFaultStep {
                    phase: "before_append".to_owned(),
                    action: SimFaultAction::PartitionSeededFollower,
                },
                SimFaultStep {
                    phase: "after_isolated_lag".to_owned(),
                    action: SimFaultAction::HealSeededFollower,
                },
            ],
            SimScenario::LeaderFailover => vec![
                SimFaultStep {
                    phase: "after_initial_append".to_owned(),
                    action: SimFaultAction::StopCurrentLeader,
                },
                SimFaultStep {
                    phase: "after_failover_append".to_owned(),
                    action: SimFaultAction::RestartStoppedLeader,
                },
            ],
            SimScenario::SnapshotCatchUp => vec![
                SimFaultStep {
                    phase: "after_append".to_owned(),
                    action: SimFaultAction::CreateLeaderSnapshot,
                },
                SimFaultStep {
                    phase: "after_snapshot".to_owned(),
                    action: SimFaultAction::PurgeLeaderLog,
                },
                SimFaultStep {
                    phase: "after_purge".to_owned(),
                    action: SimFaultAction::AddLaggingLearner { node_id: 3 },
                },
            ],
            SimScenario::RestartFollower => vec![
                SimFaultStep {
                    phase: "before_append".to_owned(),
                    action: SimFaultAction::StopSeededFollower,
                },
                SimFaultStep {
                    phase: "after_majority_commit".to_owned(),
                    action: SimFaultAction::RestartStoppedFollower,
                },
            ],
            SimScenario::ColdLiveRead => vec![
                SimFaultStep {
                    phase: "after_append".to_owned(),
                    action: SimFaultAction::WriteColdChunk {
                        start_offset: 0,
                        end_offset: 4,
                    },
                },
                SimFaultStep {
                    phase: "after_cold_write".to_owned(),
                    action: SimFaultAction::PublishColdFlush {
                        start_offset: 0,
                        end_offset: 4,
                    },
                },
            ],
            SimScenario::ColdReadFault => vec![
                SimFaultStep {
                    phase: "after_append".to_owned(),
                    action: SimFaultAction::WriteColdChunk {
                        start_offset: 0,
                        end_offset: 4,
                    },
                },
                SimFaultStep {
                    phase: "after_cold_write".to_owned(),
                    action: SimFaultAction::PublishColdFlush {
                        start_offset: 0,
                        end_offset: 4,
                    },
                },
                SimFaultStep {
                    phase: "before_cold_read".to_owned(),
                    action: SimFaultAction::FailNextColdRead,
                },
            ],
            SimScenario::ColdWriteFault => vec![
                SimFaultStep {
                    phase: "before_cold_write".to_owned(),
                    action: SimFaultAction::FailNextColdWrite,
                },
                SimFaultStep {
                    phase: "after_cold_write_failure".to_owned(),
                    action: SimFaultAction::VerifyHotReadAfterColdWriteFailure,
                },
            ],
            SimScenario::ColdWriteDelay => vec![SimFaultStep {
                phase: "before_cold_write".to_owned(),
                action: SimFaultAction::DelayNextColdWrite { delay_ms: 250 },
            }],
            SimScenario::ColdDeleteFault => vec![SimFaultStep {
                phase: "before_cold_cleanup".to_owned(),
                action: SimFaultAction::FailNextColdDelete,
            }],
            SimScenario::HttpLiveLimitProtocolSurface => Vec::new(),
            SimScenario::HttpLiveProtocolSurface => Vec::new(),
            SimScenario::HttpProducerProtocolSurface => Vec::new(),
            SimScenario::HttpProtocolSurface => Vec::new(),
            SimScenario::HttpProtocolSurfaceRandomized => vec![SimFaultStep {
                phase: "http_protocol_surface_workload".to_owned(),
                action: SimFaultAction::RunHttpProtocolSurfaceWorkload {
                    plan: HttpProtocolSurfacePlan::from_seed(0),
                },
            }],
            SimScenario::ColdReadDelay => vec![
                SimFaultStep {
                    phase: "after_append".to_owned(),
                    action: SimFaultAction::WriteColdChunk {
                        start_offset: 0,
                        end_offset: 4,
                    },
                },
                SimFaultStep {
                    phase: "after_cold_write".to_owned(),
                    action: SimFaultAction::PublishColdFlush {
                        start_offset: 0,
                        end_offset: 4,
                    },
                },
                SimFaultStep {
                    phase: "before_cold_read".to_owned(),
                    action: SimFaultAction::DelayNextColdRead { delay_ms: 250 },
                },
            ],
            SimScenario::ColdReadTruncate => vec![
                SimFaultStep {
                    phase: "after_append".to_owned(),
                    action: SimFaultAction::WriteColdChunk {
                        start_offset: 0,
                        end_offset: 4,
                    },
                },
                SimFaultStep {
                    phase: "after_cold_write".to_owned(),
                    action: SimFaultAction::PublishColdFlush {
                        start_offset: 0,
                        end_offset: 4,
                    },
                },
                SimFaultStep {
                    phase: "before_cold_read".to_owned(),
                    action: SimFaultAction::TruncateNextColdRead { returned_len: 2 },
                },
            ],
            SimScenario::RuntimeActorScheduling => vec![
                SimFaultStep {
                    phase: "before_append".to_owned(),
                    action: SimFaultAction::StartRuntimeWaitRead,
                },
                SimFaultStep {
                    phase: "before_append".to_owned(),
                    action: SimFaultAction::DelayRuntimeAppend { delay_ms: 50 },
                },
            ],
            SimScenario::RuntimeMultiClientActors => vec![
                SimFaultStep {
                    phase: "before_concurrent_clients".to_owned(),
                    action: SimFaultAction::StartRuntimeConcurrentClients { client_count: 4 },
                },
                SimFaultStep {
                    phase: "during_concurrent_clients".to_owned(),
                    action: SimFaultAction::DelayRuntimeClientAppends { base_delay_ms: 10 },
                },
            ],
            SimScenario::RuntimeColdFlushWorker => vec![
                SimFaultStep {
                    phase: "after_appends".to_owned(),
                    action: SimFaultAction::RunRuntimeColdFlushAllGroups {
                        min_hot_bytes: 4,
                        max_flush_bytes: 4,
                    },
                },
                SimFaultStep {
                    phase: "after_flush".to_owned(),
                    action: SimFaultAction::VerifyRuntimeColdLiveReads,
                },
            ],
            SimScenario::RuntimeSeededInterleaving => vec![SimFaultStep {
                phase: "seeded_runtime_interleaving".to_owned(),
                action: SimFaultAction::RunRuntimeSeededInterleaving {
                    plan: RuntimeInterleavingPlan::from_seed(0),
                },
            }],
            SimScenario::RuntimeRaftEngine => Vec::new(),
            SimScenario::RuntimeRaftNetwork => Vec::new(),
            SimScenario::RuntimeRaftSnapshotInstall => Vec::new(),
        };
        Self { steps }
    }
}

impl SimFaultPlan {
    pub(super) fn for_seeded_scenario(seed: u64, scenario: SimScenario) -> Self {
        match scenario {
            SimScenario::RuntimeSeededInterleaving => Self {
                steps: vec![SimFaultStep {
                    phase: "seeded_runtime_interleaving".to_owned(),
                    action: SimFaultAction::RunRuntimeSeededInterleaving {
                        plan: RuntimeInterleavingPlan::from_seed(seed),
                    },
                }],
            },
            SimScenario::HttpProtocolSurfaceRandomized => Self {
                steps: vec![SimFaultStep {
                    phase: "http_protocol_surface_workload".to_owned(),
                    action: SimFaultAction::RunHttpProtocolSurfaceWorkload {
                        plan: HttpProtocolSurfacePlan::from_seed(seed),
                    },
                }],
            },
            _ => Self::for_scenario(scenario),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SimFaultStep {
    pub phase: String,
    pub action: SimFaultAction,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case")]
pub enum SimFaultAction {
    PartitionSeededFollower,
    HealSeededFollower,
    CreateLeaderSnapshot,
    PurgeLeaderLog,
    AddLaggingLearner {
        node_id: u64,
    },
    StopSeededFollower,
    RestartStoppedFollower,
    StopCurrentLeader,
    RestartStoppedLeader,
    CorruptRuntimeRaftSnapshotAppendCounts,
    CorruptHttpProducerDuplicateExpectation,
    CorruptHttpLiveSseNextOffsetExpectation,
    CorruptHttpLiveLimitBackpressureExpectation,
    CorruptHttpSnapshotBodyExpectation,
    WriteColdChunk {
        start_offset: u64,
        end_offset: u64,
    },
    PublishColdFlush {
        start_offset: u64,
        end_offset: u64,
    },
    FailNextColdRead,
    FailNextColdWrite,
    RetryColdWriteAfterFailure,
    RetryColdReadAfterFailure,
    DelayNextColdWrite {
        delay_ms: u64,
    },
    FailNextColdDelete,
    VerifyHotReadAfterColdWriteFailure,
    CorruptColdLiveReadExpectation {
        node_id: u64,
    },
    DelayNextColdRead {
        delay_ms: u64,
    },
    TruncateNextColdRead {
        returned_len: usize,
    },
    StartRuntimeWaitRead,
    DelayRuntimeAppend {
        delay_ms: u64,
    },
    StartRuntimeConcurrentClients {
        client_count: usize,
    },
    DelayRuntimeClientAppends {
        base_delay_ms: u64,
    },
    RunRuntimeColdFlushAllGroups {
        min_hot_bytes: usize,
        max_flush_bytes: usize,
    },
    VerifyRuntimeColdLiveReads,
    RunRuntimeSeededInterleaving {
        plan: RuntimeInterleavingPlan,
    },
    RunRuntimeRaftNetworkWorkload {
        plan: RuntimeRaftNetworkWorkloadPlan,
    },
    RunHttpProtocolSurfaceWorkload {
        plan: HttpProtocolSurfacePlan,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HttpProtocolSurfacePlan {
    #[serde(default, skip_serializing_if = "is_false")]
    pub ttl: bool,
    #[serde(default, skip_serializing_if = "is_false")]
    pub producer_sessions: bool,
    #[serde(default, skip_serializing_if = "is_false")]
    pub producer_sequence_gap: bool,
    #[serde(default, skip_serializing_if = "is_false")]
    pub producer_epoch_bump: bool,
    #[serde(default, skip_serializing_if = "is_false")]
    pub concurrent_producers: bool,
    #[serde(default, skip_serializing_if = "is_false")]
    pub long_poll: bool,
    #[serde(default, skip_serializing_if = "is_false")]
    pub sse_close: bool,
    #[serde(default, skip_serializing_if = "is_false")]
    pub live_limit: bool,
    #[serde(default, skip_serializing_if = "is_false")]
    pub live_timeout: bool,
    #[serde(default, skip_serializing_if = "is_false")]
    pub partial_reads: bool,
    #[serde(default, skip_serializing_if = "is_false")]
    pub corrupt_sse_next_offset_expectation: bool,
    #[serde(default, skip_serializing_if = "is_false")]
    pub corrupt_live_limit_backpressure_expectation: bool,
    #[serde(default, skip_serializing_if = "is_false")]
    pub corrupt_final_read_expectation: bool,
}

impl HttpProtocolSurfacePlan {
    pub(super) fn from_seed(seed: u64) -> Self {
        let mut rng = SplitMix64::new(seed ^ 0x6874_7470_5f70_6c61);
        let producer_sessions = rng.next_bounded(3) != 0;
        let ttl = rng.next_bounded(2) == 0;
        let producer_epoch_bump = producer_sessions && rng.next_bounded(2) == 0;
        let long_poll = rng.next_bounded(2) == 0;
        let sse_close = rng.next_bounded(2) == 0;
        let live_limit = rng.next_bounded(2) == 0;
        Self {
            ttl,
            producer_sessions,
            producer_sequence_gap: producer_sessions && rng.next_bounded(2) == 0,
            producer_epoch_bump,
            concurrent_producers: producer_sessions && rng.next_bounded(2) == 0,
            long_poll,
            sse_close,
            live_limit,
            partial_reads: rng.next_bounded(2) == 0,
            live_timeout: rng.next_bounded(2) == 0,
            corrupt_final_read_expectation: false,
            corrupt_sse_next_offset_expectation: false,
            corrupt_live_limit_backpressure_expectation: false,
        }
    }
}

impl Default for HttpProtocolSurfacePlan {
    fn default() -> Self {
        Self::from_seed(0)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeInterleavingPlan {
    pub clients: Vec<RuntimeInterleavingClient>,
    pub flush_delay_ms: u64,
    pub read_verify_delay_ms: u64,
    #[serde(
        default = "default_runtime_flush_group_limit",
        skip_serializing_if = "is_default_runtime_flush_group_limit"
    )]
    pub flush_group_limit: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub panic_after: Option<RuntimeInterleavingPanic>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub corrupt_read_client_id: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub runtime_cold_read_delay_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub runtime_cold_read_truncate_len: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub runtime_cold_write_failure: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeRaftNetworkWorkloadPlan {
    pub stream_count: usize,
    pub append_batch_lens: Vec<usize>,
    pub failover_batch_lens: Vec<usize>,
    #[serde(default, skip_serializing_if = "is_false")]
    pub producer_sessions: bool,
    #[serde(default, skip_serializing_if = "is_false")]
    pub producer_epoch_bumps: bool,
    #[serde(default, skip_serializing_if = "is_false")]
    pub concurrent_producers: bool,
    #[serde(default, skip_serializing_if = "is_false")]
    pub partial_reads: bool,
    #[serde(default, skip_serializing_if = "is_false")]
    pub tail_reads: bool,
    #[serde(default, skip_serializing_if = "is_false")]
    pub close_streams: bool,
    #[serde(default, skip_serializing_if = "is_false")]
    pub publish_snapshots: bool,
    #[serde(default, skip_serializing_if = "is_false")]
    pub corrupt_read_expectation: bool,
    #[serde(default, skip_serializing_if = "is_false")]
    pub corrupt_partial_read_expectation: bool,
    #[serde(default, skip_serializing_if = "is_false")]
    pub corrupt_tail_read_expectation: bool,
    #[serde(default, skip_serializing_if = "is_false")]
    pub corrupt_close_state_expectation: bool,
    #[serde(default, skip_serializing_if = "is_false")]
    pub corrupt_snapshot_expectation: bool,
    #[serde(default, skip_serializing_if = "is_false")]
    pub corrupt_leader_failover_read_expectation: bool,
}

impl RuntimeRaftNetworkWorkloadPlan {
    fn single_stream_default() -> Self {
        Self {
            stream_count: 1,
            append_batch_lens: vec![2],
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
        }
    }

    pub(super) fn from_seed(seed: u64) -> Self {
        let mut rng = SplitMix64::new(seed ^ 0x7274_776f_726b_6c64);
        let stream_count = 1 + rng.next_bounded(3) as usize;
        let mut append_batch_lens = Vec::with_capacity(stream_count);
        let mut failover_batch_lens = Vec::with_capacity(stream_count);
        for _ in 0..stream_count {
            append_batch_lens.push(1 + rng.next_bounded(3) as usize);
            failover_batch_lens.push(1 + rng.next_bounded(2) as usize);
        }
        let producer_sessions = rng.next_bounded(2) == 0;
        Self {
            stream_count,
            append_batch_lens,
            failover_batch_lens,
            producer_sessions,
            producer_epoch_bumps: producer_sessions && rng.next_bounded(2) == 0,
            concurrent_producers: producer_sessions && rng.next_bounded(2) == 0,
            partial_reads: rng.next_bounded(2) == 0,
            tail_reads: seed.is_multiple_of(3),
            close_streams: seed % 7 == 5,
            publish_snapshots: seed % 11 == 7,
            corrupt_read_expectation: false,
            corrupt_partial_read_expectation: false,
            corrupt_tail_read_expectation: false,
            corrupt_close_state_expectation: false,
            corrupt_snapshot_expectation: false,
            corrupt_leader_failover_read_expectation: false,
        }
    }

    pub(super) fn append_batch_len(&self, stream_index: usize) -> usize {
        self.append_batch_lens
            .get(stream_index)
            .copied()
            .unwrap_or(1)
            .max(1)
    }

    pub(super) fn failover_batch_len(&self, stream_index: usize) -> usize {
        self.failover_batch_lens
            .get(stream_index)
            .copied()
            .unwrap_or(1)
            .max(1)
    }
}

impl Default for RuntimeRaftNetworkWorkloadPlan {
    fn default() -> Self {
        Self::single_stream_default()
    }
}

#[derive(Debug, Clone, Default)]
pub(super) struct RuntimeRaftNetworkOptions {
    pub(super) partition_before_append: bool,
    pub(super) heal_after_lag: bool,
    pub(super) verify_cold_live_read: bool,
    pub(super) delay_cold_write_ms: Option<u64>,
    pub(super) delay_cold_read_ms: Option<u64>,
    pub(super) truncate_cold_read_len: Option<usize>,
    pub(super) fail_cold_write: bool,
    pub(super) retry_cold_write_after_failure: bool,
    pub(super) retry_cold_read_after_truncate: bool,
    pub(super) restart_during_cold_flush: bool,
    pub(super) leader_failover_after_read: bool,
    pub(super) workload_plan: RuntimeRaftNetworkWorkloadPlan,
}

impl RuntimeInterleavingPlan {
    pub fn from_seed(seed: u64) -> Self {
        let mut rng = SplitMix64::new(seed ^ 0x7a65_6465_645f_7274);
        if seed == 72 {
            return Self::legacy_three_client_plan(&mut rng);
        }

        let client_count = 2 + rng.next_bounded(4) as usize;
        let mut clients = Vec::with_capacity(client_count);
        for client_id in 0..client_count {
            let base_delay_ms = 5 + (client_id as u64 * 9);
            clients.push(RuntimeInterleavingClient {
                client_id,
                stream_index: client_id,
                first_append_delay_ms: base_delay_ms + rng.next_bounded(9),
                second_append_delay_ms: 12 + rng.next_bounded(12),
            });
        }
        let flush_delay_ms = 18 + rng.next_bounded(18);
        let max_flush_group_limit = client_count.min(default_runtime_flush_group_limit() + 2);
        let flush_group_limit = 1 + rng.next_bounded(max_flush_group_limit as u64) as usize;
        Self {
            clients,
            flush_delay_ms,
            read_verify_delay_ms: 6 + rng.next_bounded(12),
            flush_group_limit,
            panic_after: None,
            corrupt_read_client_id: None,
            runtime_cold_read_delay_ms: runtime_cold_read_delay_ms_from_seed(seed),
            runtime_cold_read_truncate_len: None,
            runtime_cold_write_failure: None,
        }
    }

    fn legacy_three_client_plan(rng: &mut SplitMix64) -> Self {
        let first_a = 5 + rng.next_bounded(6);
        let first_b = 14 + rng.next_bounded(7);
        let first_c = 36 + rng.next_bounded(8);
        let flush_delay_ms = 26 + rng.next_bounded(6);
        Self {
            clients: vec![
                RuntimeInterleavingClient {
                    client_id: 0,
                    stream_index: 0,
                    first_append_delay_ms: first_a,
                    second_append_delay_ms: 20 + rng.next_bounded(8),
                },
                RuntimeInterleavingClient {
                    client_id: 1,
                    stream_index: 1,
                    first_append_delay_ms: first_b,
                    second_append_delay_ms: 18 + rng.next_bounded(8),
                },
                RuntimeInterleavingClient {
                    client_id: 2,
                    stream_index: 2,
                    first_append_delay_ms: first_c,
                    second_append_delay_ms: 12 + rng.next_bounded(8),
                },
            ],
            flush_delay_ms,
            read_verify_delay_ms: 8 + rng.next_bounded(8),
            flush_group_limit: default_runtime_flush_group_limit(),
            panic_after: None,
            corrupt_read_client_id: None,
            runtime_cold_read_delay_ms: None,
            runtime_cold_read_truncate_len: None,
            runtime_cold_write_failure: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeInterleavingPanic {
    pub after_event: String,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub invariant: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeInterleavingClient {
    pub client_id: usize,
    pub stream_index: usize,
    pub first_append_delay_ms: u64,
    pub second_append_delay_ms: u64,
}

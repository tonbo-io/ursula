use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::future::Future;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::Duration;

use axum::body::Body;
use axum::body::to_bytes;
use axum::http::HeaderValue;
use axum::http::Request as HttpRequest;
use axum::http::StatusCode;
use openraft::BasicNode;
use openraft::Config;
use openraft::SnapshotPolicy;
use openraft::storage::RaftLogStorage;
use serde::Deserialize;
use serde::Serialize;
use tower::ServiceExt;
use ursula::HttpState;
use ursula::WallClock;
use ursula::router_with_http_state;
use ursula_raft::InProcessRaftFaultAction;
use ursula_raft::InProcessRaftFaultScript;
use ursula_raft::InProcessRaftNetworkEvent;
use ursula_raft::InProcessRaftNetworkFactory;
use ursula_raft::InProcessRaftNetworkPolicy;
use ursula_raft::InProcessRaftNetworkPolicyEvent;
use ursula_raft::InProcessRaftRegistry;
use ursula_raft::InProcessRaftRpcKind;
use ursula_raft::MadsimOpenRaftRuntime;
use ursula_raft::RaftGroupEngine;
use ursula_raft::RaftGroupEngineFactory;
use ursula_raft::RaftGroupLogStore;
use ursula_raft::RaftGroupStateMachine;
use ursula_raft::UrsulaRaftTypeConfig;
use ursula_runtime::AppendBatchRequest;
use ursula_runtime::AppendExternalRequest;
use ursula_runtime::AppendRequest;
use ursula_runtime::AppendResponse;
use ursula_runtime::BootstrapStreamRequest;
use ursula_runtime::CloseStreamRequest;
use ursula_runtime::CloseStreamResponse;
use ursula_runtime::ColdStore;
use ursula_runtime::ColdStoreEvent;
use ursula_runtime::ColdStoreFaultEffect;
use ursula_runtime::ColdStoreHandle;
use ursula_runtime::ColdStoreOperation;
use ursula_runtime::ColdWriteAdmission;
use ursula_runtime::CreateStreamExternalRequest;
use ursula_runtime::CreateStreamRequest;
use ursula_runtime::DeleteSnapshotRequest;
use ursula_runtime::DeleteStreamRequest;
use ursula_runtime::FlushColdRequest;
use ursula_runtime::GroupAppendBatchFuture;
use ursula_runtime::GroupAppendFuture;
use ursula_runtime::GroupBootstrapStreamFuture;
use ursula_runtime::GroupCloseStreamFuture;
use ursula_runtime::GroupColdHotBacklogFuture;
use ursula_runtime::GroupCreateStreamFuture;
use ursula_runtime::GroupDeleteSnapshotFuture;
use ursula_runtime::GroupDeleteStreamFuture;
use ursula_runtime::GroupEngine;
use ursula_runtime::GroupEngineCreateFuture;
use ursula_runtime::GroupEngineError;
use ursula_runtime::GroupEngineFactory;
use ursula_runtime::GroupEngineMetrics;
use ursula_runtime::GroupFlushColdFuture;
use ursula_runtime::GroupForkRefFuture;
use ursula_runtime::GroupHeadStreamFuture;
use ursula_runtime::GroupInstallSnapshotFuture;
use ursula_runtime::GroupPlanColdFlushFuture;
use ursula_runtime::GroupPlanNextColdFlushBatchFuture;
use ursula_runtime::GroupPlanNextColdFlushFuture;
use ursula_runtime::GroupPublishSnapshotFuture;
use ursula_runtime::GroupReadSnapshotFuture;
use ursula_runtime::GroupReadStreamFuture;
use ursula_runtime::GroupReadStreamPartsFuture;
use ursula_runtime::GroupRequireLiveReadOwnerFuture;
use ursula_runtime::GroupShutdownFuture;
use ursula_runtime::GroupSnapshot;
use ursula_runtime::GroupSnapshotFuture;
use ursula_runtime::GroupTouchStreamAccessFuture;
use ursula_runtime::GroupWriteBatchFuture;
use ursula_runtime::GroupWriteCommand;
use ursula_runtime::HeadStreamRequest;
use ursula_runtime::InMemoryGroupEngineFactory;
use ursula_runtime::PlanColdFlushRequest;
use ursula_runtime::PlanGroupColdFlushRequest;
use ursula_runtime::ProducerRequest;
use ursula_runtime::PublishSnapshotRequest;
use ursula_runtime::ReadSnapshotRequest;
use ursula_runtime::ReadStreamRequest;
use ursula_runtime::RuntimeConfig;
use ursula_runtime::RuntimeError;
use ursula_runtime::RuntimeThreading;
use ursula_runtime::ShardRuntime;
use ursula_shard::BucketStreamId;
use ursula_shard::CoreId;
use ursula_shard::RaftGroupId;
use ursula_shard::ShardId;
use ursula_shard::ShardPlacement;

pub const SIM_REGRESSION_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ThreeNodeRaftSimConfig {
    pub seed: u64,
    pub stream: BucketStreamId,
}

impl ThreeNodeRaftSimConfig {
    pub fn new(seed: u64, stream_name: impl Into<String>) -> Self {
        Self {
            seed,
            stream: BucketStreamId::new("benchcmp", stream_name.into()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ThreeNodeRaftSimOutcome {
    pub seed: u64,
    pub leader_id: u64,
    pub target_node_id: Option<u64>,
    pub appended_log_index: u64,
    pub trace: SimTrace,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SimScenario {
    NoFaultBaseline,
    PartitionHeal,
    LeaderFailover,
    SnapshotCatchUp,
    RestartFollower,
    ColdLiveRead,
    ColdReadFault,
    ColdWriteFault,
    ColdWriteDelay,
    ColdDeleteFault,
    ColdReadDelay,
    ColdReadTruncate,
    RuntimeActorScheduling,
    RuntimeMultiClientActors,
    RuntimeColdFlushWorker,
    RuntimeSeededInterleaving,
    RuntimeRaftEngine,
    RuntimeRaftNetwork,
    RuntimeRaftSnapshotInstall,
    HttpProtocolSurfaceRandomized,
    HttpLiveLimitProtocolSurface,
    HttpLiveProtocolSurface,
    HttpProducerProtocolSurface,
    HttpProtocolSurface,
}

#[derive(Clone)]
struct SimHttpWallClock {
    now_ms: Arc<AtomicU64>,
}

impl WallClock for SimHttpWallClock {
    fn unix_time_ms(&self) -> u64 {
        self.now_ms.load(Ordering::Relaxed)
    }
}

impl SimScenario {
    fn slug(self) -> &'static str {
        match self {
            Self::NoFaultBaseline => "no-fault",
            Self::PartitionHeal => "partition-heal",
            Self::LeaderFailover => "leader-failover",
            Self::SnapshotCatchUp => "snapshot-catch-up",
            Self::RestartFollower => "restart-follower",
            Self::ColdLiveRead => "cold-live-read",
            Self::ColdReadFault => "cold-read-fault",
            Self::ColdWriteFault => "cold-write-fault",
            Self::ColdWriteDelay => "cold-write-delay",
            Self::ColdDeleteFault => "cold-delete-fault",
            Self::ColdReadDelay => "cold-read-delay",
            Self::ColdReadTruncate => "cold-read-truncate",
            Self::RuntimeActorScheduling => "runtime-actor-scheduling",
            Self::RuntimeMultiClientActors => "runtime-multi-client-actors",
            Self::RuntimeColdFlushWorker => "runtime-cold-flush-worker",
            Self::RuntimeSeededInterleaving => "runtime-seeded-interleaving",
            Self::RuntimeRaftEngine => "runtime-raft-engine",
            Self::RuntimeRaftNetwork => "runtime-raft-network",
            Self::RuntimeRaftSnapshotInstall => "runtime-raft-snapshot-install",
            Self::HttpProtocolSurfaceRandomized => "http-protocol-surface-randomized",
            Self::HttpLiveLimitProtocolSurface => "http-live-limit-protocol-surface",
            Self::HttpLiveProtocolSurface => "http-live-protocol-surface",
            Self::HttpProducerProtocolSurface => "http-producer-protocol-surface",
            Self::HttpProtocolSurface => "http-protocol-surface",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SimReport {
    pub scenario: SimScenario,
    pub outcome: ThreeNodeRaftSimOutcome,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SimSchedule {
    pub seed: u64,
    pub scenario: SimScenario,
    pub stream: BucketStreamId,
    pub fault_plan: SimFaultPlan,
}

impl SimSchedule {
    pub fn generate(seed: u64) -> Self {
        if seed == 54 {
            return Self::for_scenario(seed, SimScenario::HttpProducerProtocolSurface);
        }
        if seed == 55 {
            return Self::for_scenario(seed, SimScenario::HttpLiveLimitProtocolSurface);
        }
        if seed == 56 {
            return Self::for_scenario(seed, SimScenario::HttpLiveProtocolSurface);
        }
        if seed == 57 {
            return Self::for_scenario(seed, SimScenario::HttpProtocolSurface);
        }
        if seed == 58 {
            return Self::for_scenario(seed, SimScenario::ColdDeleteFault);
        }
        if seed == 59 {
            return Self::for_scenario(seed, SimScenario::ColdWriteDelay);
        }
        if seed == 68 {
            return Self::for_scenario(seed, SimScenario::ColdReadTruncate);
        }
        if seed == 69 {
            return Self::for_scenario(seed, SimScenario::RuntimeActorScheduling);
        }
        if seed == 70 {
            return Self::for_scenario(seed, SimScenario::RuntimeMultiClientActors);
        }
        if seed == 71 {
            return Self::for_scenario(seed, SimScenario::RuntimeColdFlushWorker);
        }
        if RUNTIME_INTERLEAVING_SEEDS.contains(&seed) {
            return Self::for_scenario(seed, SimScenario::RuntimeSeededInterleaving);
        }
        if RUNTIME_RAFT_ENGINE_SEEDS.contains(&seed) {
            return Self::for_scenario(seed, SimScenario::RuntimeRaftEngine);
        }
        if RUNTIME_RAFT_NETWORK_SEEDS.contains(&seed) {
            return Self::for_scenario(seed, SimScenario::RuntimeRaftNetwork);
        }
        if RUNTIME_RAFT_NETWORK_RANDOMIZED_SEEDS.contains(&seed) {
            return Self::generate_runtime_raft_network_randomized(seed);
        }
        if RUNTIME_RAFT_NETWORK_COLD_LIVE_WRITE_RECOVERY_SEEDS.contains(&seed) {
            return Self::generate_runtime_raft_network_cold_live_write_recovery(seed);
        }
        if LEADER_FAILOVER_SEEDS.contains(&seed) {
            return Self::for_scenario(seed, SimScenario::LeaderFailover);
        }
        if RUNTIME_RAFT_SNAPSHOT_INSTALL_SEEDS.contains(&seed) {
            return Self::for_scenario(seed, SimScenario::RuntimeRaftSnapshotInstall);
        }
        if HTTP_PROTOCOL_SURFACE_RANDOMIZED_SEEDS.contains(&seed) {
            return Self::generate_http_protocol_surface_randomized(seed);
        }
        let scenario = match seed % 12 {
            0 => SimScenario::NoFaultBaseline,
            1 => SimScenario::PartitionHeal,
            2 => SimScenario::SnapshotCatchUp,
            3 => SimScenario::RestartFollower,
            4 => SimScenario::ColdLiveRead,
            5 => SimScenario::ColdReadFault,
            6 => SimScenario::ColdWriteFault,
            7 => SimScenario::ColdReadDelay,
            8 => SimScenario::PartitionHeal,
            9 => SimScenario::SnapshotCatchUp,
            10 => SimScenario::RestartFollower,
            _ => SimScenario::ColdLiveRead,
        };
        Self::for_scenario(seed, scenario)
    }

    pub fn for_scenario(seed: u64, scenario: SimScenario) -> Self {
        let stream = BucketStreamId::new(
            "benchcmp",
            format!("ursula-sim-schedule-{seed}-{}", scenario.slug()),
        );
        Self {
            seed,
            scenario,
            stream,
            fault_plan: SimFaultPlan::for_seeded_scenario(seed, scenario),
        }
    }

    pub fn config(&self) -> ThreeNodeRaftSimConfig {
        ThreeNodeRaftSimConfig {
            seed: self.seed,
            stream: self.stream.clone(),
        }
    }

    pub fn run(&self) -> SimReport {
        ThreeNodeRaftSim::run_schedule(self.clone())
    }
}

pub const RUNTIME_INTERLEAVING_SEEDS: std::ops::RangeInclusive<u64> = 72..=96;
pub const RUNTIME_INTERLEAVING_FAILURE_SEEDS: std::ops::RangeInclusive<u64> = 172..=176;
pub const RUNTIME_INTERLEAVING_TRUNCATE_FAILURE_SEEDS: std::ops::RangeInclusive<u64> = 182..=186;
pub const RUNTIME_INTERLEAVING_WRITE_FAILURE_SEEDS: std::ops::RangeInclusive<u64> = 192..=196;
pub const RAFT_PARTITION_FAILURE_SEEDS: std::ops::RangeInclusive<u64> = 202..=206;
pub const LEADER_FAILOVER_SEEDS: std::ops::RangeInclusive<u64> = 122..=126;
pub const RUNTIME_RAFT_ENGINE_SEEDS: std::ops::RangeInclusive<u64> = 97..=101;
pub const RUNTIME_RAFT_NETWORK_SEEDS: std::ops::RangeInclusive<u64> = 102..=106;
pub const RUNTIME_RAFT_NETWORK_RECOVERY_SEEDS: std::ops::RangeInclusive<u64> = 107..=111;
pub const RUNTIME_RAFT_NETWORK_COLD_LIVE_RECOVERY_SEEDS: std::ops::RangeInclusive<u64> = 112..=116;
pub const RUNTIME_RAFT_NETWORK_COLD_LIVE_RESTART_SEEDS: std::ops::RangeInclusive<u64> = 117..=121;
pub const RUNTIME_RAFT_NETWORK_COLD_LIVE_WRITE_RECOVERY_SEEDS: std::ops::RangeInclusive<u64> =
    317..=321;
pub const RUNTIME_RAFT_NETWORK_LEADER_FAILOVER_SEEDS: std::ops::RangeInclusive<u64> = 127..=131;
pub const RUNTIME_RAFT_SNAPSHOT_INSTALL_SEEDS: std::ops::RangeInclusive<u64> = 132..=136;
pub const RUNTIME_RAFT_NETWORK_RANDOMIZED_SEEDS: std::ops::RangeInclusive<u64> = 137..=156;
pub const HTTP_PROTOCOL_SURFACE_RANDOMIZED_SEEDS: std::ops::RangeInclusive<u64> = 277..=296;
pub const RUNTIME_RAFT_NETWORK_PARTITION_FAILURE_SEEDS: std::ops::RangeInclusive<u64> = 212..=216;
pub const RUNTIME_RAFT_NETWORK_RANDOMIZED_COLD_READ_FAILURE_SEEDS: std::ops::RangeInclusive<u64> =
    322..=326;
pub const RUNTIME_RAFT_NETWORK_COLD_LIVE_TRUNCATE_FAILURE_SEEDS: std::ops::RangeInclusive<u64> =
    222..=226;
pub const RUNTIME_RAFT_SNAPSHOT_INSTALL_FAILURE_SEEDS: std::ops::RangeInclusive<u64> = 232..=236;

fn is_false(value: &bool) -> bool {
    !*value
}

fn default_runtime_flush_group_limit() -> usize {
    2
}

fn is_default_runtime_flush_group_limit(value: &usize) -> bool {
    *value == default_runtime_flush_group_limit()
}

fn runtime_cold_read_delay_ms_from_seed(seed: u64) -> Option<u64> {
    if seed >= 73 && seed.is_multiple_of(5) {
        Some(40 + (seed % 4) * 10)
    } else {
        None
    }
}

fn runtime_corrupt_read_client_id(seed: u64, plan: &RuntimeInterleavingPlan) -> usize {
    let client_count = plan.clients.len().max(1);
    usize::try_from(seed % client_count as u64).expect("client id fits usize")
}

fn runtime_cold_read_truncate_len(seed: u64) -> usize {
    usize::try_from(seed % 3).expect("truncate len fits usize")
}

#[derive(Debug, Clone)]
struct SplitMix64 {
    state: u64,
}

impl SplitMix64 {
    fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    fn next(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut value = self.state;
        value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        value ^ (value >> 31)
    }

    fn next_bounded(&mut self, upper: u64) -> u64 {
        debug_assert!(upper > 0);
        self.next() % upper
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SimRegressionRecord {
    pub schema_version: u32,
    pub scenario: SimScenario,
    pub seed: u64,
    pub stream: BucketStreamId,
    pub outcome: ThreeNodeRaftSimOutcome,
}

impl SimRegressionRecord {
    pub fn new(config: &ThreeNodeRaftSimConfig, report: SimReport) -> Self {
        Self {
            schema_version: SIM_REGRESSION_SCHEMA_VERSION,
            scenario: report.scenario,
            seed: config.seed,
            stream: config.stream.clone(),
            outcome: report.outcome,
        }
    }

    pub fn config(&self) -> ThreeNodeRaftSimConfig {
        ThreeNodeRaftSimConfig {
            seed: self.seed,
            stream: self.stream.clone(),
        }
    }

    pub fn replay(&self) -> SimReport {
        assert_eq!(
            self.schema_version, SIM_REGRESSION_SCHEMA_VERSION,
            "unsupported sim regression schema version"
        );
        ThreeNodeRaftSim::run_report(self.scenario, self.config())
    }

    pub fn assert_replays(&self) {
        let replayed = self.replay();
        assert_eq!(replayed.scenario, self.scenario);
        assert_eq!(replayed.outcome, self.outcome);
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SimScheduledRecord {
    pub schema_version: u32,
    pub schedule: SimSchedule,
    pub outcome: ThreeNodeRaftSimOutcome,
}

impl SimScheduledRecord {
    pub fn new(schedule: SimSchedule, report: SimReport) -> Self {
        assert_eq!(schedule.scenario, report.scenario);
        Self {
            schema_version: SIM_REGRESSION_SCHEMA_VERSION,
            schedule,
            outcome: report.outcome,
        }
    }

    pub fn from_seed(seed: u64) -> Self {
        let schedule = SimSchedule::generate(seed);
        let report = schedule.run();
        Self::new(schedule, report)
    }

    pub fn replay(&self) -> SimReport {
        assert_eq!(
            self.schema_version, SIM_REGRESSION_SCHEMA_VERSION,
            "unsupported sim schedule schema version"
        );
        self.schedule.run()
    }

    pub fn assert_replays(&self) {
        let replayed = self.replay();
        assert_eq!(replayed.scenario, self.schedule.scenario);
        assert_eq!(
            stable_replay_outcome(replayed.outcome),
            stable_replay_outcome(self.outcome.clone())
        );
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SimFailureRegressionRecord {
    pub schema_version: u32,
    pub seed: u64,
    pub schedule: SimSchedule,
    pub invariant: String,
    pub panic: String,
    #[serde(default)]
    pub panic_contains: Vec<String>,
}

impl SimFailureRegressionRecord {
    pub fn assert_replays(&self) {
        assert_eq!(
            self.schema_version, SIM_REGRESSION_SCHEMA_VERSION,
            "unsupported sim failure regression schema version"
        );
        assert_eq!(self.seed, self.schedule.seed);
        let previous_hook = std::panic::take_hook();
        std::panic::set_hook(Box::new(|_| {}));
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            self.schedule.run();
        }));
        std::panic::set_hook(previous_hook);
        let panic = result
            .map(|_| "schedule completed successfully".to_owned())
            .unwrap_or_else(panic_payload_to_string);
        if self.panic_contains.is_empty() {
            assert_eq!(panic, self.panic);
        } else {
            for expected in &self.panic_contains {
                assert!(
                    panic.contains(expected),
                    "failure regression panic did not contain `{expected}`; panic was `{panic}`"
                );
            }
        }
        assert!(
            invariant_failed(&SimTrace::last_recorded(), &self.invariant),
            "failure regression did not record invariant `{}`",
            self.invariant
        );
    }
}

mod trace;
pub use self::trace::SimEvent;
pub use self::trace::SimTrace;

mod cold_path;
mod generators;
use cold_path::run_cold_delete_fault_inner;
use cold_path::run_cold_live_read_inner;
use cold_path::run_cold_read_delay_inner;
use cold_path::run_cold_read_fault_inner;
use cold_path::run_cold_read_truncate_inner;
use cold_path::run_cold_write_delay_inner;
use cold_path::run_cold_write_fault_inner;
mod http;
use http::run_http_live_limit_protocol_surface_inner;
use http::run_http_live_protocol_surface_inner;
use http::run_http_producer_protocol_surface_inner;
use http::run_http_protocol_surface_inner;
use http::run_http_protocol_surface_randomized_inner;
mod runtime_scenarios;
use runtime_scenarios::run_runtime_actor_scheduling_inner;
use runtime_scenarios::run_runtime_cold_flush_worker_inner;
use runtime_scenarios::run_runtime_multi_client_actors_inner;
use runtime_scenarios::run_runtime_raft_engine_inner;
use runtime_scenarios::run_runtime_raft_network_inner;
use runtime_scenarios::run_runtime_raft_snapshot_install_inner;
use runtime_scenarios::run_runtime_seeded_interleaving_inner;
mod raft_scenarios;
#[cfg(test)]
use raft_scenarios::run_isolated_leader_pending_write_snapshot_purge_inner;
use raft_scenarios::run_leader_failover_inner;
use raft_scenarios::run_no_fault_inner;
use raft_scenarios::run_partition_heal_inner;
use raft_scenarios::run_restart_follower_inner;
use raft_scenarios::run_snapshot_catch_up_inner;
mod faults_inner;
pub use faults_inner::*;
mod dispatch;
mod introspect;
use introspect::cold_read_delay_ms_from_fault_plan;
use introspect::cold_read_truncate_len_from_fault_plan;
use introspect::cold_write_delay_ms_from_fault_plan;
use introspect::corrupt_cold_live_read_node_from_fault_plan;
use introspect::has_cold_delete_fault_in_fault_plan;
use introspect::has_cold_read_fault_in_fault_plan;
use introspect::has_cold_write_fault_in_fault_plan;
use introspect::has_corrupt_http_live_limit_backpressure_expectation_in_fault_plan;
use introspect::has_corrupt_http_live_sse_next_offset_expectation_in_fault_plan;
use introspect::has_corrupt_http_producer_duplicate_expectation_in_fault_plan;
use introspect::has_corrupt_http_snapshot_body_expectation_in_fault_plan;
use introspect::has_corrupt_runtime_raft_snapshot_append_counts_in_fault_plan;
use introspect::has_heal_seeded_follower_in_fault_plan;
use introspect::has_partition_seeded_follower_in_fault_plan;
use introspect::has_restart_stopped_follower_in_fault_plan;
use introspect::has_restart_stopped_leader_in_fault_plan;
use introspect::has_retry_cold_read_after_failure_in_fault_plan;
use introspect::has_retry_cold_write_after_failure_in_fault_plan;
use introspect::has_stop_current_leader_in_fault_plan;
use introspect::has_stop_seeded_follower_in_fault_plan;
use introspect::has_verify_runtime_cold_live_reads_in_fault_plan;
use introspect::http_protocol_surface_plan_from_fault_plan;
use introspect::invariant_failed;
use introspect::panic_payload_to_string;
use introspect::runtime_interleaving_plan_from_fault_plan;
use introspect::runtime_raft_network_workload_plan_from_fault_plan;
use introspect::sim_event_from_cold_store_event;
use introspect::sim_event_from_network_event;
#[cfg(test)]
mod rolling_restart;

pub fn stable_replay_outcome(mut outcome: ThreeNodeRaftSimOutcome) -> ThreeNodeRaftSimOutcome {
    outcome.trace = outcome.trace.stable_replay();
    outcome
}

pub struct ThreeNodeRaftSim;

pub(super) fn http_offset(offset: u64) -> String {
    format!("{offset:020}")
}

pub(super) fn parse_http_offset(value: &HeaderValue) -> u64 {
    value
        .to_str()
        .expect("http offset header should be utf8")
        .parse::<u64>()
        .expect("http offset header should parse as u64")
}

pub(super) fn assert_http_protocol_surface_randomized_final_read(
    trace: &mut SimTrace,
    stream: &BucketStreamId,
    actual: &[u8],
    expected: &[u8],
    expected_next_offset: u64,
) {
    if actual != expected {
        let message = format!(
            "final HTTP read for stream {stream} returned {} bytes, expected {} bytes/next_offset {}",
            actual.len(),
            expected.len(),
            expected_next_offset
        );
        trace.push(SimEvent::InvariantFailed {
            invariant: "http_protocol_randomized_read_your_write".to_owned(),
            after_event: "http_protocol_surface_randomized_final_read".to_owned(),
            message: message.clone(),
        });
        SimTrace::record(trace.events.last().expect("invariant event").clone());
        panic!(
            "invariant `http_protocol_randomized_read_your_write` failed after `http_protocol_surface_randomized_final_read`: {message}"
        );
    }
}

pub(super) fn assert_http_protocol_surface_randomized_sse_next_offset(
    trace: &mut SimTrace,
    stream: &BucketStreamId,
    body: &str,
    expected_next_offset: u64,
) {
    let expected = format!(
        "\"streamNextOffset\":\"{}\"",
        http_offset(expected_next_offset)
    );
    if !body.contains(&expected) {
        let message = format!(
            "SSE body for stream {stream} did not contain expected next offset {}",
            http_offset(expected_next_offset)
        );
        trace.push(SimEvent::InvariantFailed {
            invariant: "http_protocol_randomized_sse_delivery".to_owned(),
            after_event: "http_protocol_surface_randomized_sse_body".to_owned(),
            message: message.clone(),
        });
        SimTrace::record(trace.events.last().expect("invariant event").clone());
        panic!(
            "invariant `http_protocol_randomized_sse_delivery` failed after `http_protocol_surface_randomized_sse_body`: {message}"
        );
    }
}

pub(super) fn assert_http_protocol_surface_randomized_live_backpressure(
    trace: &mut SimTrace,
    stream: &BucketStreamId,
    metrics_body: &str,
    expected_backpressure_events: u64,
) {
    let expected = format!("\"live_read_backpressure_events\":{expected_backpressure_events}");
    if !metrics_body.contains(&expected) {
        let message = format!(
            "live-read backpressure metrics for stream {stream} did not contain expected event count {expected}: {metrics_body}"
        );
        trace.push(SimEvent::InvariantFailed {
            invariant: "http_protocol_randomized_live_waiter_backpressure".to_owned(),
            after_event: "http_protocol_surface_randomized_live_limit_metrics".to_owned(),
            message: message.clone(),
        });
        SimTrace::record(trace.events.last().expect("invariant event").clone());
        panic!(
            "invariant `http_protocol_randomized_live_waiter_backpressure` failed after `http_protocol_surface_randomized_live_limit_metrics`: {message}"
        );
    }
}

fn run_with_madsim<T>(seed: u64, workload: impl Future<Output = T>) -> T {
    SimTrace::clear_recorded();
    let mut runtime =
        madsim::runtime::Runtime::with_seed_and_config(seed, madsim::Config::default());
    runtime.set_time_limit(Duration::from_secs(30));
    runtime.block_on(MadsimOpenRaftRuntime::scope(seed, workload))
}

pub(super) fn sim_network_policy() -> InProcessRaftNetworkPolicy {
    let policy = InProcessRaftNetworkPolicy::default();
    policy.set_observer(|event| {
        SimTrace::record(sim_event_from_network_event(event));
    });
    policy
}

pub(super) fn sim_cold_store() -> ColdStore {
    let cold_store = ColdStore::memory()
        .expect("memory cold store")
        .without_read_cache();
    cold_store.set_delay_fn(madsim::time::sleep);
    cold_store.set_observer(|event| {
        SimTrace::record(sim_event_from_cold_store_event(event));
    });
    cold_store
}

#[derive(Debug, Clone, Copy)]
struct MadsimScopedRaftGroupEngineFactory {
    seed: u64,
}

impl MadsimScopedRaftGroupEngineFactory {
    fn new(seed: u64) -> Self {
        Self { seed }
    }
}

impl GroupEngineFactory for MadsimScopedRaftGroupEngineFactory {
    fn create<'a>(
        &'a self,
        placement: ShardPlacement,
        metrics: GroupEngineMetrics,
    ) -> GroupEngineCreateFuture<'a> {
        let seed = self.seed ^ u64::from(placement.raft_group_id.0);
        Box::pin(MadsimOpenRaftRuntime::scope(seed, async move {
            let inner = RaftGroupEngineFactory.create(placement, metrics).await?;
            let engine: Box<dyn GroupEngine> = Box::new(MadsimScopedGroupEngine { seed, inner });
            Ok(engine)
        }))
    }
}

#[derive(Clone)]
struct MadsimRuntimeRaftNetworkFactory {
    seed: u64,
    policy: InProcessRaftNetworkPolicy,
    cold_store: Option<ColdStoreHandle>,
    aggressive_snapshot_purge: bool,
    followers: Arc<Mutex<Vec<(u64, RaftGroupEngine)>>>,
    leaders: Arc<Mutex<BTreeMap<u32, u64>>>,
    groups: Arc<Mutex<BTreeMap<u32, RuntimeRaftNetworkGroupControl>>>,
}

#[derive(Clone)]
struct RuntimeRaftNetworkGroupControl {
    placement: ShardPlacement,
    config: Arc<Config>,
    registry: InProcessRaftRegistry,
    metrics: GroupEngineMetrics,
    log_stores: BTreeMap<u64, Arc<RaftGroupLogStore>>,
}

impl MadsimRuntimeRaftNetworkFactory {
    fn new(seed: u64, policy: InProcessRaftNetworkPolicy) -> Self {
        Self::with_cold_store(seed, policy, None)
    }

    fn with_cold_store(
        seed: u64,
        policy: InProcessRaftNetworkPolicy,
        cold_store: Option<ColdStoreHandle>,
    ) -> Self {
        Self {
            seed,
            policy,
            cold_store,
            aggressive_snapshot_purge: false,
            followers: Arc::new(Mutex::new(Vec::new())),
            leaders: Arc::new(Mutex::new(BTreeMap::new())),
            groups: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    #[cfg(test)]
    fn with_aggressive_snapshot_purge(mut self) -> Self {
        self.aggressive_snapshot_purge = true;
        self
    }

    fn leader_id(&self, raft_group_id: RaftGroupId) -> Option<u64> {
        self.leaders
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .get(&raft_group_id.0)
            .copied()
    }

    fn unregister_current_leader(
        &self,
        raft_group_id: RaftGroupId,
    ) -> Result<u64, GroupEngineError> {
        let leader_id = self.leader_id(raft_group_id).ok_or_else(|| {
            GroupEngineError::new(format!(
                "runtime raft leader for group {} not found",
                raft_group_id.0
            ))
        })?;
        let groups = self
            .groups
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        let control = groups.get(&raft_group_id.0).ok_or_else(|| {
            GroupEngineError::new(format!(
                "runtime raft group control for group {} not found",
                raft_group_id.0
            ))
        })?;
        control.registry.unregister(leader_id);
        Ok(leader_id)
    }

    fn follower_raft_handle(
        &self,
        node_id: u64,
    ) -> Option<openraft::Raft<UrsulaRaftTypeConfig, RaftGroupStateMachine>> {
        self.followers
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .iter()
            .find_map(|(candidate, engine)| (*candidate == node_id).then(|| engine.raft_handle()))
    }

    fn raft_handle(
        &self,
        node_id: u64,
    ) -> Option<openraft::Raft<UrsulaRaftTypeConfig, RaftGroupStateMachine>> {
        self.groups
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .values()
            .find_map(|control| control.registry.get(node_id))
    }

    async fn log_store_last_log_index(&self, node_id: u64) -> Option<u64> {
        let log_store = self
            .groups
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .values()
            .find_map(|control| control.log_stores.get(&node_id).cloned())?;
        let mut log_store = log_store;
        log_store
            .get_log_state()
            .await
            .ok()
            .and_then(|state| state.last_log_id.map(|log_id| log_id.index))
    }

    fn retained_follower_id_prefer(&self, preferred_node_id: u64) -> Option<u64> {
        let followers = self
            .followers
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        followers
            .iter()
            .find_map(|(candidate, _)| (*candidate == preferred_node_id).then_some(*candidate))
            .or_else(|| followers.iter().map(|(candidate, _)| *candidate).min())
    }

    async fn take_current_leader_engine(
        &self,
        raft_group_id: RaftGroupId,
    ) -> Result<(u64, Box<dyn GroupEngine>), GroupEngineError> {
        let old_leader_id = self.leader_id(raft_group_id).ok_or_else(|| {
            GroupEngineError::new(format!(
                "runtime raft leader for group {} not found",
                raft_group_id.0
            ))
        })?;
        let handles = self
            .followers
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .iter()
            .map(|(node_id, engine)| (*node_id, engine.raft_handle()))
            .collect::<Vec<_>>();
        let wait_handle = handles
            .iter()
            .find(|(node_id, _)| *node_id != old_leader_id)
            .map(|(_, raft)| raft.clone())
            .ok_or_else(|| GroupEngineError::new("runtime raft replacement follower not found"))?;
        let metrics = wait_handle
            .wait(Some(Duration::from_secs(5)))
            .metrics(
                |metrics| {
                    metrics
                        .current_leader
                        .is_some_and(|leader_id| leader_id != old_leader_id)
                },
                "runtime raft new leader elected after old leader shutdown",
            )
            .await
            .map_err(|err| {
                GroupEngineError::new(format!("wait for runtime raft replacement leader: {err}"))
            })?;
        let new_leader_id = metrics
            .current_leader
            .ok_or_else(|| GroupEngineError::new("runtime raft replacement leader missing"))?;
        let engine = {
            let mut followers = self
                .followers
                .lock()
                .unwrap_or_else(|poison| poison.into_inner());
            let Some(index) = followers
                .iter()
                .position(|(candidate, _)| *candidate == new_leader_id)
            else {
                return Err(GroupEngineError::new(format!(
                    "runtime raft replacement leader {new_leader_id} not retained as follower"
                )));
            };
            followers.swap_remove(index).1
        };
        self.leaders
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .insert(raft_group_id.0, new_leader_id);
        let seed = self.seed ^ 0x7274_6c66_6169_6c6f ^ u64::from(raft_group_id.0);
        let engine: Box<dyn GroupEngine> = Box::new(MadsimScopedGroupEngine {
            seed,
            inner: Box::new(engine),
        });
        Ok((new_leader_id, engine))
    }

    async fn stop_follower(&self, node_id: u64) -> Result<(), GroupEngineError> {
        let engine = {
            let mut followers = self
                .followers
                .lock()
                .unwrap_or_else(|poison| poison.into_inner());
            let Some(index) = followers
                .iter()
                .position(|(candidate, _)| *candidate == node_id)
            else {
                return Err(GroupEngineError::new(format!(
                    "runtime raft follower {node_id} not found"
                )));
            };
            followers.swap_remove(index).1
        };
        {
            let groups = self
                .groups
                .lock()
                .unwrap_or_else(|poison| poison.into_inner());
            for control in groups.values() {
                control.registry.unregister(node_id);
            }
        }
        engine.shutdown().await
    }

    async fn restart_follower(&self, node_id: u64) -> Result<(), GroupEngineError> {
        let control = {
            let groups = self
                .groups
                .lock()
                .unwrap_or_else(|poison| poison.into_inner());
            groups
                .values()
                .next()
                .cloned()
                .ok_or_else(|| GroupEngineError::new("runtime raft group control not found"))?
        };
        let log_store = control.log_stores.get(&node_id).cloned().ok_or_else(|| {
            GroupEngineError::new(format!(
                "runtime raft log store for node {node_id} not found"
            ))
        })?;
        let engine = RaftGroupEngine::new_node_with_log_store_and_network(
            control.placement,
            node_id,
            control.config,
            InProcessRaftNetworkFactory::new(control.registry.clone())
                .with_source(node_id)
                .with_policy(self.policy.clone()),
            log_store,
            Some(control.metrics),
            self.cold_store.clone(),
        )
        .await?;
        control.registry.register(node_id, engine.raft_handle());
        self.followers
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .push((node_id, engine));
        Ok(())
    }
}

impl GroupEngineFactory for MadsimRuntimeRaftNetworkFactory {
    fn create<'a>(
        &'a self,
        placement: ShardPlacement,
        metrics: GroupEngineMetrics,
    ) -> GroupEngineCreateFuture<'a> {
        let seed = self.seed ^ 0x7274_6e65_7477_6f72 ^ u64::from(placement.raft_group_id.0);
        let policy = self.policy.clone();
        let cold_store = self.cold_store.clone();
        let aggressive_snapshot_purge = self.aggressive_snapshot_purge;
        let followers = self.followers.clone();
        let leaders = self.leaders.clone();
        let groups = self.groups.clone();
        Box::pin(MadsimOpenRaftRuntime::scope(seed, async move {
            let registry = InProcessRaftRegistry::default();
            let mut config = Config {
                cluster_name: format!("ursula-sim-runtime-group-{}", placement.raft_group_id.0),
                heartbeat_interval: 10,
                election_timeout_min: 50,
                election_timeout_max: 100,
                ..Default::default()
            };
            if aggressive_snapshot_purge {
                config.max_in_snapshot_log_to_keep = 0;
                config.purge_batch_size = 1;
                config.replication_lag_threshold = 0;
                config.snapshot_policy = SnapshotPolicy::LogsSinceLast(4);
            }
            let config =
                Arc::new(config.validate().map_err(|err| {
                    GroupEngineError::new(format!("invalid OpenRaft config: {err}"))
                })?);
            let mut nodes = BTreeMap::new();
            for node_id in 1..=3 {
                nodes.insert(
                    node_id,
                    BasicNode::new(format!("runtime-raft-node-{node_id}")),
                );
            }

            let mut engines = Vec::new();
            let mut log_stores = BTreeMap::new();
            for node_id in 1..=3 {
                let log_store = RaftGroupLogStore::shared();
                log_stores.insert(node_id, log_store.clone());
                let engine = RaftGroupEngine::new_node_with_log_store_and_network(
                    placement,
                    node_id,
                    config.clone(),
                    InProcessRaftNetworkFactory::new(registry.clone())
                        .with_source(node_id)
                        .with_policy(policy.clone()),
                    log_store,
                    Some(metrics.clone()),
                    cold_store.clone(),
                )
                .await?;
                registry.register(node_id, engine.raft_handle());
                engines.push((node_id, engine));
            }

            engines[0].1.initialize_membership(nodes).await?;
            let leader_metrics = engines[0]
                .1
                .raft_handle()
                .wait(Some(Duration::from_secs(5)))
                .metrics(|metrics| metrics.current_leader.is_some(), "leader elected")
                .await
                .map_err(|err| GroupEngineError::new(format!("wait for leader: {err}")))?;
            let leader_id = leader_metrics.current_leader.expect("leader id");
            leaders
                .lock()
                .unwrap_or_else(|poison| poison.into_inner())
                .insert(placement.raft_group_id.0, leader_id);
            groups
                .lock()
                .unwrap_or_else(|poison| poison.into_inner())
                .insert(placement.raft_group_id.0, RuntimeRaftNetworkGroupControl {
                    placement,
                    config: config.clone(),
                    registry: registry.clone(),
                    metrics: metrics.clone(),
                    log_stores,
                });

            let leader_index = engines
                .iter()
                .position(|(node_id, _)| *node_id == leader_id)
                .expect("leader engine exists");
            let (_leader_id, leader) = engines.swap_remove(leader_index);
            followers
                .lock()
                .unwrap_or_else(|poison| poison.into_inner())
                .extend(engines);

            let engine: Box<dyn GroupEngine> = Box::new(MadsimScopedGroupEngine {
                seed,
                inner: Box::new(leader),
            });
            Ok(engine)
        }))
    }
}

struct MadsimScopedGroupEngine {
    seed: u64,
    inner: Box<dyn GroupEngine>,
}

impl GroupEngine for MadsimScopedGroupEngine {
    fn accepts_local_writes(&self) -> bool {
        self.inner.accepts_local_writes()
    }

    fn create_stream<'a>(
        &'a mut self,
        request: CreateStreamRequest,
        placement: ShardPlacement,
    ) -> GroupCreateStreamFuture<'a> {
        Box::pin(MadsimOpenRaftRuntime::scope(self.seed, async move {
            self.inner.create_stream(request, placement).await
        }))
    }

    fn create_stream_external<'a>(
        &'a mut self,
        request: CreateStreamExternalRequest,
        placement: ShardPlacement,
    ) -> GroupCreateStreamFuture<'a> {
        Box::pin(MadsimOpenRaftRuntime::scope(self.seed, async move {
            self.inner.create_stream_external(request, placement).await
        }))
    }

    fn head_stream<'a>(
        &'a mut self,
        request: ursula_runtime::HeadStreamRequest,
        placement: ShardPlacement,
    ) -> GroupHeadStreamFuture<'a> {
        Box::pin(MadsimOpenRaftRuntime::scope(self.seed, async move {
            self.inner.head_stream(request, placement).await
        }))
    }

    fn read_stream<'a>(
        &'a mut self,
        request: ReadStreamRequest,
        placement: ShardPlacement,
    ) -> GroupReadStreamFuture<'a> {
        Box::pin(MadsimOpenRaftRuntime::scope(self.seed, async move {
            self.inner.read_stream(request, placement).await
        }))
    }

    fn read_stream_parts<'a>(
        &'a mut self,
        request: ReadStreamRequest,
        placement: ShardPlacement,
    ) -> GroupReadStreamPartsFuture<'a> {
        Box::pin(MadsimOpenRaftRuntime::scope(self.seed, async move {
            self.inner.read_stream_parts(request, placement).await
        }))
    }

    fn require_local_live_read_owner<'a>(
        &'a mut self,
        placement: ShardPlacement,
    ) -> GroupRequireLiveReadOwnerFuture<'a> {
        Box::pin(MadsimOpenRaftRuntime::scope(self.seed, async move {
            self.inner.require_local_live_read_owner(placement).await
        }))
    }

    fn publish_snapshot<'a>(
        &'a mut self,
        request: PublishSnapshotRequest,
        placement: ShardPlacement,
    ) -> GroupPublishSnapshotFuture<'a> {
        Box::pin(MadsimOpenRaftRuntime::scope(self.seed, async move {
            self.inner.publish_snapshot(request, placement).await
        }))
    }

    fn read_snapshot<'a>(
        &'a mut self,
        request: ReadSnapshotRequest,
        placement: ShardPlacement,
    ) -> GroupReadSnapshotFuture<'a> {
        Box::pin(MadsimOpenRaftRuntime::scope(self.seed, async move {
            self.inner.read_snapshot(request, placement).await
        }))
    }

    fn delete_snapshot<'a>(
        &'a mut self,
        request: DeleteSnapshotRequest,
        placement: ShardPlacement,
    ) -> GroupDeleteSnapshotFuture<'a> {
        Box::pin(MadsimOpenRaftRuntime::scope(self.seed, async move {
            self.inner.delete_snapshot(request, placement).await
        }))
    }

    fn bootstrap_stream<'a>(
        &'a mut self,
        request: BootstrapStreamRequest,
        placement: ShardPlacement,
    ) -> GroupBootstrapStreamFuture<'a> {
        Box::pin(MadsimOpenRaftRuntime::scope(self.seed, async move {
            self.inner.bootstrap_stream(request, placement).await
        }))
    }

    fn touch_stream_access<'a>(
        &'a mut self,
        stream_id: BucketStreamId,
        now_ms: u64,
        renew_ttl: bool,
        placement: ShardPlacement,
    ) -> GroupTouchStreamAccessFuture<'a> {
        Box::pin(MadsimOpenRaftRuntime::scope(self.seed, async move {
            self.inner
                .touch_stream_access(stream_id, now_ms, renew_ttl, placement)
                .await
        }))
    }

    fn add_fork_ref<'a>(
        &'a mut self,
        stream_id: BucketStreamId,
        now_ms: u64,
        placement: ShardPlacement,
    ) -> GroupForkRefFuture<'a> {
        Box::pin(MadsimOpenRaftRuntime::scope(self.seed, async move {
            self.inner.add_fork_ref(stream_id, now_ms, placement).await
        }))
    }

    fn release_fork_ref<'a>(
        &'a mut self,
        stream_id: BucketStreamId,
        placement: ShardPlacement,
    ) -> GroupForkRefFuture<'a> {
        Box::pin(MadsimOpenRaftRuntime::scope(self.seed, async move {
            self.inner.release_fork_ref(stream_id, placement).await
        }))
    }

    fn close_stream<'a>(
        &'a mut self,
        request: CloseStreamRequest,
        placement: ShardPlacement,
    ) -> GroupCloseStreamFuture<'a> {
        Box::pin(MadsimOpenRaftRuntime::scope(self.seed, async move {
            self.inner.close_stream(request, placement).await
        }))
    }

    fn delete_stream<'a>(
        &'a mut self,
        request: DeleteStreamRequest,
        placement: ShardPlacement,
    ) -> GroupDeleteStreamFuture<'a> {
        Box::pin(MadsimOpenRaftRuntime::scope(self.seed, async move {
            self.inner.delete_stream(request, placement).await
        }))
    }

    fn append<'a>(
        &'a mut self,
        request: AppendRequest,
        placement: ShardPlacement,
    ) -> GroupAppendFuture<'a> {
        Box::pin(MadsimOpenRaftRuntime::scope(self.seed, async move {
            self.inner.append(request, placement).await
        }))
    }

    fn append_external<'a>(
        &'a mut self,
        request: AppendExternalRequest,
        placement: ShardPlacement,
    ) -> GroupAppendFuture<'a> {
        Box::pin(MadsimOpenRaftRuntime::scope(self.seed, async move {
            self.inner.append_external(request, placement).await
        }))
    }

    fn append_batch<'a>(
        &'a mut self,
        request: AppendBatchRequest,
        placement: ShardPlacement,
    ) -> GroupAppendBatchFuture<'a> {
        Box::pin(MadsimOpenRaftRuntime::scope(self.seed, async move {
            self.inner.append_batch(request, placement).await
        }))
    }

    fn create_stream_with_cold_admission<'a>(
        &'a mut self,
        request: CreateStreamRequest,
        placement: ShardPlacement,
        admission: ColdWriteAdmission,
    ) -> GroupCreateStreamFuture<'a> {
        Box::pin(MadsimOpenRaftRuntime::scope(self.seed, async move {
            self.inner
                .create_stream_with_cold_admission(request, placement, admission)
                .await
        }))
    }

    fn append_with_cold_admission<'a>(
        &'a mut self,
        request: AppendRequest,
        placement: ShardPlacement,
        admission: ColdWriteAdmission,
    ) -> GroupAppendFuture<'a> {
        Box::pin(MadsimOpenRaftRuntime::scope(self.seed, async move {
            self.inner
                .append_with_cold_admission(request, placement, admission)
                .await
        }))
    }

    fn append_batch_with_cold_admission<'a>(
        &'a mut self,
        request: AppendBatchRequest,
        placement: ShardPlacement,
        admission: ColdWriteAdmission,
    ) -> GroupAppendBatchFuture<'a> {
        Box::pin(MadsimOpenRaftRuntime::scope(self.seed, async move {
            self.inner
                .append_batch_with_cold_admission(request, placement, admission)
                .await
        }))
    }

    fn append_batch_many_with_cold_admission<'a>(
        &'a mut self,
        requests: Vec<AppendBatchRequest>,
        placement: ShardPlacement,
        admission: ColdWriteAdmission,
    ) -> GroupWriteBatchFuture<'a> {
        Box::pin(MadsimOpenRaftRuntime::scope(self.seed, async move {
            self.inner
                .append_batch_many_with_cold_admission(requests, placement, admission)
                .await
        }))
    }

    fn flush_cold<'a>(
        &'a mut self,
        request: FlushColdRequest,
        placement: ShardPlacement,
    ) -> GroupFlushColdFuture<'a> {
        Box::pin(MadsimOpenRaftRuntime::scope(self.seed, async move {
            self.inner.flush_cold(request, placement).await
        }))
    }

    fn plan_cold_flush<'a>(
        &'a mut self,
        request: PlanColdFlushRequest,
        placement: ShardPlacement,
    ) -> GroupPlanColdFlushFuture<'a> {
        Box::pin(MadsimOpenRaftRuntime::scope(self.seed, async move {
            self.inner.plan_cold_flush(request, placement).await
        }))
    }

    fn plan_next_cold_flush<'a>(
        &'a mut self,
        request: PlanGroupColdFlushRequest,
        placement: ShardPlacement,
    ) -> GroupPlanNextColdFlushFuture<'a> {
        Box::pin(MadsimOpenRaftRuntime::scope(self.seed, async move {
            self.inner.plan_next_cold_flush(request, placement).await
        }))
    }

    fn plan_next_cold_flush_batch<'a>(
        &'a mut self,
        request: PlanGroupColdFlushRequest,
        placement: ShardPlacement,
        max_candidates: usize,
    ) -> GroupPlanNextColdFlushBatchFuture<'a> {
        Box::pin(MadsimOpenRaftRuntime::scope(self.seed, async move {
            self.inner
                .plan_next_cold_flush_batch(request, placement, max_candidates)
                .await
        }))
    }

    fn cold_hot_backlog<'a>(
        &'a mut self,
        stream_id: BucketStreamId,
        placement: ShardPlacement,
    ) -> GroupColdHotBacklogFuture<'a> {
        Box::pin(MadsimOpenRaftRuntime::scope(self.seed, async move {
            self.inner.cold_hot_backlog(stream_id, placement).await
        }))
    }

    fn snapshot<'a>(&'a mut self, placement: ShardPlacement) -> GroupSnapshotFuture<'a> {
        Box::pin(MadsimOpenRaftRuntime::scope(self.seed, async move {
            self.inner.snapshot(placement).await
        }))
    }

    fn install_snapshot<'a>(
        &'a mut self,
        snapshot: GroupSnapshot,
    ) -> GroupInstallSnapshotFuture<'a> {
        Box::pin(MadsimOpenRaftRuntime::scope(self.seed, async move {
            self.inner.install_snapshot(snapshot).await
        }))
    }

    fn shutdown<'a>(&'a mut self) -> GroupShutdownFuture<'a> {
        Box::pin(MadsimOpenRaftRuntime::scope(self.seed, async move {
            self.inner.shutdown().await
        }))
    }

    fn write_batch<'a>(
        &'a mut self,
        commands: Vec<GroupWriteCommand>,
        placement: ShardPlacement,
    ) -> GroupWriteBatchFuture<'a> {
        Box::pin(MadsimOpenRaftRuntime::scope(self.seed, async move {
            self.inner.write_batch(commands, placement).await
        }))
    }
}

fn network_rpc_kind_name(kind: InProcessRaftRpcKind) -> &'static str {
    match kind {
        InProcessRaftRpcKind::AppendEntries => "append_entries",
        InProcessRaftRpcKind::Vote => "vote",
        InProcessRaftRpcKind::FullSnapshot => "full_snapshot",
        InProcessRaftRpcKind::TransferLeader => "transfer_leader",
    }
}

pub(super) fn duration_ms(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

pub(super) fn assert_runtime_interleaving_read_your_write(
    client_id: usize,
    stream: &BucketStreamId,
    actual: &[u8],
    expected: &[u8],
    after_event: &'static str,
) {
    if actual != expected {
        let message = format!(
            "client {client_id} stream {stream} expected {} bytes, read {} bytes",
            expected.len(),
            actual.len()
        );
        SimTrace::record(SimEvent::InvariantFailed {
            invariant: "runtime_interleaving_read_your_write".to_owned(),
            after_event: after_event.to_owned(),
            message: message.clone(),
        });
        panic!(
            "invariant `runtime_interleaving_read_your_write` failed after `{after_event}`: {message}"
        );
    }
}

pub(super) fn assert_cold_live_read_consistency(
    node_id: u64,
    stream: &BucketStreamId,
    actual: &[u8],
    expected: &[u8],
    actual_next_offset: u64,
    expected_next_offset: u64,
    after_event: &'static str,
) {
    if actual != expected || actual_next_offset != expected_next_offset {
        let message = format!(
            "node {node_id} stream {stream} expected {} bytes/next_offset {}, read {} bytes/next_offset {}",
            expected.len(),
            expected_next_offset,
            actual.len(),
            actual_next_offset
        );
        SimTrace::record(SimEvent::InvariantFailed {
            invariant: "cold_live_read_consistency".to_owned(),
            after_event: after_event.to_owned(),
            message: message.clone(),
        });
        panic!("invariant `cold_live_read_consistency` failed after `{after_event}`: {message}");
    }
}

pub(super) fn assert_runtime_raft_read_consistency(
    stream: &BucketStreamId,
    actual: &[u8],
    expected: &[u8],
    actual_next_offset: u64,
    expected_next_offset: u64,
    after_event: &'static str,
) {
    if actual != expected || actual_next_offset != expected_next_offset {
        let message = format!(
            "stream {stream} expected {} bytes/next_offset {}, read {} bytes/next_offset {}",
            expected.len(),
            expected_next_offset,
            actual.len(),
            actual_next_offset
        );
        SimTrace::record(SimEvent::InvariantFailed {
            invariant: "runtime_raft_network_read_your_write".to_owned(),
            after_event: after_event.to_owned(),
            message: message.clone(),
        });
        panic!(
            "invariant `runtime_raft_network_read_your_write` failed after `{after_event}`: {message}"
        );
    }
}

pub(super) fn assert_runtime_raft_leader_failover_read_consistency(
    stream: &BucketStreamId,
    actual: &[u8],
    expected: &[u8],
    actual_next_offset: u64,
    expected_next_offset: u64,
) {
    if actual != expected || actual_next_offset != expected_next_offset {
        let message = format!(
            "stream {stream} leader failover read returned {} bytes/next_offset {}, expected {} bytes/next_offset {}",
            actual.len(),
            actual_next_offset,
            expected.len(),
            expected_next_offset
        );
        SimTrace::record(SimEvent::InvariantFailed {
            invariant: "runtime_raft_network_leader_failover_no_loss_or_dup".to_owned(),
            after_event: "runtime_raft_network_leader_failover_verified".to_owned(),
            message: message.clone(),
        });
        panic!(
            "invariant `runtime_raft_network_leader_failover_no_loss_or_dup` failed after `runtime_raft_network_leader_failover_verified`: {message}"
        );
    }
}

pub(super) fn runtime_raft_network_streams(
    base: &BucketStreamId,
    stream_count: usize,
) -> Vec<BucketStreamId> {
    let stream_count = stream_count.max(1);
    (0..stream_count)
        .map(|stream_index| {
            if stream_index == 0 {
                base.clone()
            } else {
                BucketStreamId::new(
                    base.bucket_id.clone(),
                    format!("{}-s{stream_index}", base.stream_id),
                )
            }
        })
        .collect()
}

pub(super) fn runtime_raft_network_batch_payloads(
    stream_index: usize,
    batch_index: usize,
    batch_len: usize,
) -> Vec<Vec<u8>> {
    (0..batch_len.max(1))
        .map(|item_index| format!("s{stream_index}:b{batch_index}:i{item_index};").into_bytes())
        .collect()
}

pub(super) fn runtime_raft_network_duplicate_payloads(
    stream_index: usize,
    batch_index: usize,
    batch_len: usize,
) -> Vec<Vec<u8>> {
    (0..batch_len.max(1))
        .map(|item_index| {
            format!("duplicate:s{stream_index}:b{batch_index}:i{item_index};").into_bytes()
        })
        .collect()
}

pub(super) async fn verify_runtime_raft_partial_read(
    runtime: &ShardRuntime,
    stream: &BucketStreamId,
    expected_payload: &[u8],
    after_event: &str,
    corrupt_expectation: bool,
    trace: &mut SimTrace,
) {
    if expected_payload.len() < 4 {
        return;
    }
    let offset = (expected_payload.len() / 3).max(1);
    let max_len = (expected_payload.len() - offset).clamp(1, 7);
    let mut expected_slice = expected_payload[offset..offset + max_len].to_vec();
    if corrupt_expectation {
        expected_slice.push(b'!');
    }
    let read = runtime
        .read_stream(ReadStreamRequest {
            stream_id: stream.clone(),
            offset: offset as u64,
            max_len,
            now_ms: 0,
        })
        .await
        .expect("runtime raft partial read");
    if read.payload != expected_slice {
        let message = format!(
            "partial read for stream {stream} at offset {offset} returned {:?}, expected {:?}",
            read.payload, expected_slice
        );
        SimTrace::record(SimEvent::InvariantFailed {
            invariant: "runtime_raft_network_partial_read_integrity".to_owned(),
            after_event: after_event.to_owned(),
            message: message.clone(),
        });
        panic!(
            "invariant `runtime_raft_network_partial_read_integrity` failed after `{after_event}`: {message}"
        );
    }
    assert_eq!(
        read.next_offset,
        (offset + expected_slice.len()) as u64,
        "partial read for stream {stream} returned unexpected next offset"
    );
    trace.push(SimEvent::RuntimeRaftNetworkPartialReadVerified {
        stream: stream.clone(),
        after_event: after_event.to_owned(),
        offset: offset as u64,
        max_len,
        next_offset: read.next_offset,
    });
}

pub(super) async fn verify_runtime_raft_tail_read(
    runtime: &ShardRuntime,
    stream: &BucketStreamId,
    expected_next_offset: u64,
    after_event: &str,
    corrupt_expectation: bool,
    trace: &mut SimTrace,
) {
    let request_offset = expected_next_offset;
    let read = runtime
        .read_stream(ReadStreamRequest {
            stream_id: stream.clone(),
            offset: request_offset,
            max_len: 8,
            now_ms: 0,
        })
        .await
        .expect("runtime raft tail read");
    let expected_next_offset = expected_next_offset + u64::from(corrupt_expectation);
    if !read.payload.is_empty() || read.next_offset != expected_next_offset {
        let message = format!(
            "tail read for stream {stream} at offset {request_offset} returned {} bytes/next_offset {}, expected empty payload/next_offset {expected_next_offset}",
            read.payload.len(),
            read.next_offset
        );
        SimTrace::record(SimEvent::InvariantFailed {
            invariant: "runtime_raft_network_tail_read_empty".to_owned(),
            after_event: after_event.to_owned(),
            message: message.clone(),
        });
        panic!(
            "invariant `runtime_raft_network_tail_read_empty` failed after `{after_event}`: {message}"
        );
    }
    trace.push(SimEvent::RuntimeRaftNetworkTailReadVerified {
        stream: stream.clone(),
        after_event: after_event.to_owned(),
        offset: expected_next_offset,
        next_offset: read.next_offset,
    });
}

pub(super) async fn verify_runtime_raft_close_stream(
    runtime: &ShardRuntime,
    stream: &BucketStreamId,
    expected_payload: &[u8],
    expected_next_offset: u64,
    after_event: &str,
    corrupt_expectation: bool,
    trace: &mut SimTrace,
) -> CloseStreamResponse {
    let close = runtime
        .close_stream(CloseStreamRequest {
            stream_id: stream.clone(),
            stream_seq: None,
            producer: None,
            now_ms: 0,
        })
        .await
        .expect("close runtime raft stream");
    if close.next_offset != expected_next_offset {
        let message = format!(
            "close for stream {stream} returned next_offset {}, expected {expected_next_offset}",
            close.next_offset
        );
        SimTrace::record(SimEvent::InvariantFailed {
            invariant: "runtime_raft_network_close_state".to_owned(),
            after_event: after_event.to_owned(),
            message: message.clone(),
        });
        panic!(
            "invariant `runtime_raft_network_close_state` failed after `{after_event}`: {message}"
        );
    }

    let read = runtime
        .read_stream(ReadStreamRequest {
            stream_id: stream.clone(),
            offset: 0,
            max_len: expected_payload.len().max(64),
            now_ms: 0,
        })
        .await
        .expect("read closed runtime raft stream");
    let expected_closed = !corrupt_expectation;
    if read.payload != expected_payload
        || read.next_offset != expected_next_offset
        || read.closed != expected_closed
    {
        let message = format!(
            "closed read for stream {stream} returned {} bytes/next_offset {}/closed {}, expected {} bytes/next_offset {expected_next_offset}/closed {expected_closed}",
            read.payload.len(),
            read.next_offset,
            read.closed,
            expected_payload.len()
        );
        SimTrace::record(SimEvent::InvariantFailed {
            invariant: "runtime_raft_network_close_state".to_owned(),
            after_event: after_event.to_owned(),
            message: message.clone(),
        });
        panic!(
            "invariant `runtime_raft_network_close_state` failed after `{after_event}`: {message}"
        );
    }

    let append_after_close = runtime
        .append(AppendRequest::from_bytes(
            stream.clone(),
            b"append-after-close;".to_vec(),
        ))
        .await;
    let append_rejected = match append_after_close {
        Ok(response) => {
            let message = format!(
                "append after close for stream {stream} unexpectedly committed next_offset {}",
                response.next_offset
            );
            SimTrace::record(SimEvent::InvariantFailed {
                invariant: "runtime_raft_network_close_state".to_owned(),
                after_event: after_event.to_owned(),
                message: message.clone(),
            });
            panic!(
                "invariant `runtime_raft_network_close_state` failed after `{after_event}`: {message}"
            );
        }
        Err(err) => {
            let message = format!("{err:?}");
            if !message.contains("StreamClosed") {
                let message = format!(
                    "append after close for stream {stream} returned unexpected error {message}"
                );
                SimTrace::record(SimEvent::InvariantFailed {
                    invariant: "runtime_raft_network_close_state".to_owned(),
                    after_event: after_event.to_owned(),
                    message: message.clone(),
                });
                panic!(
                    "invariant `runtime_raft_network_close_state` failed after `{after_event}`: {message}"
                );
            }
            true
        }
    };

    trace.push(SimEvent::RuntimeRaftNetworkCloseVerified {
        stream: stream.clone(),
        after_event: after_event.to_owned(),
        next_offset: close.next_offset,
        group_commit_index: close.group_commit_index,
        append_rejected,
    });
    close
}

pub(super) async fn verify_runtime_raft_snapshot_publish(
    runtime: &ShardRuntime,
    stream_index: usize,
    stream: &BucketStreamId,
    expected_next_offset: u64,
    after_event: &str,
    corrupt_expectation: bool,
    trace: &mut SimTrace,
) -> ursula_runtime::PublishSnapshotResponse {
    let snapshot_offset = expected_next_offset;
    let content_type = "application/octet-stream".to_owned();
    let snapshot_payload =
        format!("runtime-raft-snapshot:{stream_index}:{snapshot_offset};").into_bytes();
    let mut expected_snapshot_payload = snapshot_payload.clone();
    if corrupt_expectation {
        expected_snapshot_payload.push(b'!');
    }
    let publish = runtime
        .publish_snapshot(PublishSnapshotRequest {
            stream_id: stream.clone(),
            snapshot_offset,
            content_type: content_type.clone(),
            payload: snapshot_payload.clone().into(),
            now_ms: 0,
        })
        .await
        .expect("publish runtime raft snapshot");
    if publish.snapshot_offset != snapshot_offset {
        let message = format!(
            "publish snapshot for stream {stream} returned offset {}, expected {snapshot_offset}",
            publish.snapshot_offset
        );
        SimTrace::record(SimEvent::InvariantFailed {
            invariant: "runtime_raft_network_snapshot_publish_read".to_owned(),
            after_event: after_event.to_owned(),
            message: message.clone(),
        });
        panic!(
            "invariant `runtime_raft_network_snapshot_publish_read` failed after `{after_event}`: {message}"
        );
    }

    for requested_offset in [None, Some(snapshot_offset)] {
        let read = runtime
            .read_snapshot(ReadSnapshotRequest {
                stream_id: stream.clone(),
                snapshot_offset: requested_offset,
                now_ms: 0,
            })
            .await
            .expect("read runtime raft snapshot");
        if read.snapshot_offset != snapshot_offset
            || read.next_offset != expected_next_offset
            || read.content_type != content_type
            || read.payload != expected_snapshot_payload
            || !read.up_to_date
        {
            let message = format!(
                "read snapshot for stream {stream} at {:?} returned offset {}/next_offset {}/content_type {}/{} bytes/up_to_date {}, expected offset {snapshot_offset}/next_offset {expected_next_offset}/content_type {content_type}/{} bytes/up_to_date true",
                requested_offset,
                read.snapshot_offset,
                read.next_offset,
                read.content_type,
                read.payload.len(),
                read.up_to_date,
                expected_snapshot_payload.len()
            );
            SimTrace::record(SimEvent::InvariantFailed {
                invariant: "runtime_raft_network_snapshot_publish_read".to_owned(),
                after_event: after_event.to_owned(),
                message: message.clone(),
            });
            panic!(
                "invariant `runtime_raft_network_snapshot_publish_read` failed after `{after_event}`: {message}"
            );
        }
    }

    let bootstrap = runtime
        .bootstrap_stream(BootstrapStreamRequest {
            stream_id: stream.clone(),
            now_ms: 0,
        })
        .await
        .expect("bootstrap runtime raft stream after snapshot publish");
    if bootstrap.snapshot_offset != Some(snapshot_offset)
        || bootstrap.snapshot_content_type != content_type
        || bootstrap.snapshot_payload != expected_snapshot_payload
        || bootstrap.next_offset != expected_next_offset
        || !bootstrap.up_to_date
    {
        let message = format!(
            "bootstrap after snapshot for stream {stream} returned snapshot {:?}/next_offset {}/{} snapshot bytes/up_to_date {}, expected snapshot {snapshot_offset}/next_offset {expected_next_offset}/{} snapshot bytes/up_to_date true",
            bootstrap.snapshot_offset,
            bootstrap.next_offset,
            bootstrap.snapshot_payload.len(),
            bootstrap.up_to_date,
            expected_snapshot_payload.len()
        );
        SimTrace::record(SimEvent::InvariantFailed {
            invariant: "runtime_raft_network_snapshot_publish_read".to_owned(),
            after_event: after_event.to_owned(),
            message: message.clone(),
        });
        panic!(
            "invariant `runtime_raft_network_snapshot_publish_read` failed after `{after_event}`: {message}"
        );
    }

    trace.push(SimEvent::RuntimeRaftNetworkSnapshotPublishedVerified {
        stream: stream.clone(),
        after_event: after_event.to_owned(),
        snapshot_offset,
        snapshot_len: snapshot_payload.len(),
        next_offset: expected_next_offset,
        group_commit_index: publish.group_commit_index,
    });
    publish
}

pub(super) fn runtime_raft_network_producer(
    seed: u64,
    stream_index: usize,
    epoch_delta: u64,
    seq: u64,
) -> ProducerRequest {
    runtime_raft_network_producer_with_lane(seed, stream_index, 0, epoch_delta, seq)
}

pub(super) fn runtime_raft_network_producer_with_lane(
    seed: u64,
    stream_index: usize,
    producer_lane: usize,
    epoch_delta: u64,
    seq: u64,
) -> ProducerRequest {
    let producer_id = if producer_lane == 0 {
        format!("sim-writer-{seed}-{stream_index}")
    } else {
        format!("sim-writer-{seed}-{stream_index}-concurrent-{producer_lane}")
    };
    ProducerRequest {
        producer_id,
        producer_epoch: (seed % 7) + epoch_delta,
        producer_seq: seq,
    }
}

pub(super) fn assert_runtime_raft_producer_duplicate(
    stream: &BucketStreamId,
    original: &[AppendResponse],
    duplicate: &[AppendResponse],
    producer_seq: u64,
) {
    assert_eq!(
        duplicate.len(),
        original.len(),
        "runtime raft producer duplicate item count mismatch for {stream} seq {producer_seq}"
    );
    for (item_index, (original, duplicate)) in original.iter().zip(duplicate).enumerate() {
        assert!(
            duplicate.deduplicated,
            "runtime raft producer duplicate item {item_index} for {stream} seq {producer_seq} was not marked deduplicated"
        );
        assert_eq!(
            duplicate.start_offset, original.start_offset,
            "runtime raft producer duplicate item {item_index} for {stream} seq {producer_seq} changed start offset"
        );
        assert_eq!(
            duplicate.next_offset, original.next_offset,
            "runtime raft producer duplicate item {item_index} for {stream} seq {producer_seq} changed next offset"
        );
    }
}

pub(super) fn assert_runtime_raft_producer_stale_epoch(
    stream: &BucketStreamId,
    result: Result<ursula_runtime::AppendBatchResponse, RuntimeError>,
) {
    match result {
        Ok(batch) => {
            assert!(
                !batch.items.is_empty(),
                "runtime raft stale producer epoch returned an empty batch for {stream}"
            );
            for (item_index, item) in batch.items.into_iter().enumerate() {
                match item {
                    Ok(response) => panic!(
                        "runtime raft stale producer epoch item {item_index} for {stream} unexpectedly appended at offset {}",
                        response.start_offset
                    ),
                    Err(err) => assert_runtime_raft_stale_epoch_error(stream, &err),
                }
            }
        }
        Err(err) => assert_runtime_raft_stale_epoch_error(stream, &err),
    }
}

fn assert_runtime_raft_stale_epoch_error(stream: &BucketStreamId, err: &RuntimeError) {
    assert!(
        err.to_string().contains("ProducerEpochStale"),
        "runtime raft stale producer epoch for {stream} returned unexpected error: {err}"
    );
}

pub(super) async fn wait_raft_applied_index_at_least(
    raft: &openraft::Raft<UrsulaRaftTypeConfig, RaftGroupStateMachine>,
    log_index: u64,
    description: &'static str,
) {
    raft.wait(Some(Duration::from_secs(5)))
        .applied_index_at_least(Some(log_index), description)
        .await
        .expect("wait for raft applied index");
}

pub(super) fn maybe_panic_after_runtime_interleaving_event(
    plan: &RuntimeInterleavingPlan,
    event: &'static str,
) {
    if let Some(panic_after) = &plan.panic_after
        && panic_after.after_event == event
    {
        if let Some(invariant) = &panic_after.invariant {
            SimTrace::record(SimEvent::InvariantFailed {
                invariant: invariant.clone(),
                after_event: event.to_owned(),
                message: panic_after.message.clone(),
            });
            panic!(
                "invariant `{}` failed after `{}`: {}",
                invariant, event, panic_after.message
            );
        }
        panic!("{}", panic_after.message);
    }
}

pub(super) fn runtime_interleaving_payload(client_id: usize, append_index: usize) -> Vec<u8> {
    format!("c{client_id}{append_index}!").into_bytes()
}

pub(super) async fn wait_all_nodes_applied(
    engines: &[RaftGroupEngine],
    log_index: u64,
    description: &'static str,
) {
    for (index, engine) in engines.iter().enumerate() {
        let node_id = u64::try_from(index + 1).expect("node index fits u64");
        SimTrace::record(SimEvent::WaitAppliedBegin {
            node_id,
            log_index,
            description: description.to_owned(),
        });
        engine
            .raft_handle()
            .wait(Some(Duration::from_secs(5)))
            .applied_index_at_least(Some(log_index), description)
            .await
            .expect("wait for all nodes apply");
        SimTrace::record(SimEvent::WaitAppliedComplete {
            node_id,
            log_index,
            description: description.to_owned(),
        });
    }
}

pub(super) async fn verify_all_nodes_can_read(
    engines: &mut [RaftGroupEngine],
    stream: &BucketStreamId,
) {
    verify_all_nodes_can_read_payload(engines, stream, b"simulated").await;
}

pub(super) async fn verify_all_nodes_can_read_payload(
    engines: &mut [RaftGroupEngine],
    stream: &BucketStreamId,
    expected: &[u8],
) {
    for (index, engine) in engines.iter().enumerate() {
        let node_id = u64::try_from(index + 1).expect("node index fits u64");
        read_local_payload_eventually(
            engine,
            node_id,
            stream,
            0,
            16,
            expected,
            "read stream from simulated node",
        )
        .await;
    }
}

pub(super) async fn read_local_payload_eventually(
    engine: &RaftGroupEngine,
    node_id: u64,
    stream: &BucketStreamId,
    offset: u64,
    max_len: usize,
    expected: &[u8],
    description: &'static str,
) -> ursula_runtime::ReadStreamResponse {
    let mut last_payload = Vec::new();
    for attempt in 0..50 {
        let read = engine
            .sim_read_local_stream(
                ReadStreamRequest {
                    stream_id: stream.clone(),
                    offset,
                    max_len,
                    now_ms: 0,
                },
                placement(),
            )
            .await
            .unwrap_or_else(|err| panic!("{description}: {err}"));
        SimTrace::record(SimEvent::ReadAttempt {
            node_id,
            stream: stream.clone(),
            offset,
            max_len,
            attempt,
            payload_len: read.payload.len(),
        });
        if read.payload == expected {
            SimTrace::record(SimEvent::ReadSatisfied {
                node_id,
                stream: stream.clone(),
                offset,
                max_len,
                attempt,
            });
            return read;
        }
        last_payload = read.payload;
        madsim::time::sleep(Duration::from_millis(100)).await;
    }
    panic!(
        "{description}: expected payload {:?}, last payload {:?}",
        expected, last_payload
    );
}

pub(super) async fn build_three_node_cluster(
    policy: InProcessRaftNetworkPolicy,
) -> (InProcessRaftRegistry, Vec<RaftGroupEngine>, u64) {
    build_three_node_cluster_with_cold_store(policy, None).await
}

pub(super) async fn build_three_node_cluster_with_cold_store(
    policy: InProcessRaftNetworkPolicy,
    cold_store: Option<ColdStoreHandle>,
) -> (InProcessRaftRegistry, Vec<RaftGroupEngine>, u64) {
    let (registry, engines, _log_stores, _config, leader_id) =
        build_restartable_three_node_cluster_with_cold_store(policy, cold_store).await;
    (registry, engines, leader_id)
}

pub(super) async fn build_restartable_three_node_cluster(
    policy: InProcessRaftNetworkPolicy,
) -> (
    InProcessRaftRegistry,
    Vec<RaftGroupEngine>,
    Vec<Arc<RaftGroupLogStore>>,
    Arc<Config>,
    u64,
) {
    build_restartable_three_node_cluster_with_cold_store(policy, None).await
}

async fn build_restartable_three_node_cluster_with_cold_store(
    policy: InProcessRaftNetworkPolicy,
    cold_store: Option<ColdStoreHandle>,
) -> (
    InProcessRaftRegistry,
    Vec<RaftGroupEngine>,
    Vec<Arc<RaftGroupLogStore>>,
    Arc<Config>,
    u64,
) {
    let registry = InProcessRaftRegistry::default();
    let config = Arc::new(
        Config {
            cluster_name: "ursula-sim-three-node".to_owned(),
            heartbeat_interval: 10,
            election_timeout_min: 50,
            election_timeout_max: 100,
            ..Default::default()
        }
        .validate()
        .expect("valid raft config"),
    );
    let mut nodes = BTreeMap::new();
    for node_id in 1..=3 {
        nodes.insert(node_id, BasicNode::new(format!("node-{node_id}")));
    }

    let mut engines = Vec::new();
    let mut log_stores = Vec::new();
    for node_id in 1..=3 {
        let log_store = RaftGroupLogStore::shared();
        let engine = RaftGroupEngine::new_node_with_log_store_and_network(
            placement(),
            node_id,
            config.clone(),
            InProcessRaftNetworkFactory::new(registry.clone())
                .with_source(node_id)
                .with_policy(policy.clone()),
            log_store.clone(),
            None,
            cold_store.clone(),
        )
        .await
        .expect("create simulated raft group node");
        registry.register(node_id, engine.raft_handle());
        engines.push(engine);
        log_stores.push(log_store);
    }

    engines[0]
        .initialize_membership(nodes)
        .await
        .expect("initialize simulated raft group");
    let leader_metrics = engines[0]
        .raft_handle()
        .wait(Some(Duration::from_secs(5)))
        .metrics(|metrics| metrics.current_leader.is_some(), "leader elected")
        .await
        .expect("wait for simulated leader");
    let leader_id = leader_metrics.current_leader.expect("leader id");
    (registry, engines, log_stores, config, leader_id)
}

pub(super) async fn build_lagging_learner_snapshot_cluster(
    policy: InProcessRaftNetworkPolicy,
) -> (InProcessRaftRegistry, Vec<RaftGroupEngine>, u64) {
    let registry = InProcessRaftRegistry::default();
    let config = Arc::new(
        Config {
            cluster_name: "ursula-sim-lagging-learner-snapshot".to_owned(),
            heartbeat_interval: 10,
            election_timeout_min: 50,
            election_timeout_max: 100,
            max_in_snapshot_log_to_keep: 0,
            purge_batch_size: 1,
            replication_lag_threshold: 0,
            snapshot_policy: SnapshotPolicy::Never,
            ..Default::default()
        }
        .validate()
        .expect("valid raft config"),
    );

    let mut engines = Vec::new();
    for node_id in 1..=3 {
        let engine = RaftGroupEngine::new_node_with_log_store_and_network(
            placement(),
            node_id,
            config.clone(),
            InProcessRaftNetworkFactory::new(registry.clone())
                .with_source(node_id)
                .with_policy(policy.clone()),
            RaftGroupLogStore::shared(),
            None,
            None,
        )
        .await
        .expect("create simulated raft group node");
        if node_id != 3 {
            registry.register(node_id, engine.raft_handle());
        }
        engines.push(engine);
    }

    let mut initial_nodes = BTreeMap::new();
    for node_id in 1..=2 {
        initial_nodes.insert(node_id, BasicNode::new(format!("node-{node_id}")));
    }
    engines[0]
        .initialize_membership(initial_nodes)
        .await
        .expect("initialize simulated two-voter raft group");
    let leader_metrics = engines[0]
        .raft_handle()
        .wait(Some(Duration::from_secs(5)))
        .metrics(|metrics| metrics.current_leader.is_some(), "leader elected")
        .await
        .expect("wait for simulated leader");
    let leader_id = leader_metrics.current_leader.expect("leader id");
    for engine in &engines[..2] {
        engine
            .raft_handle()
            .wait(Some(Duration::from_secs(5)))
            .current_leader(leader_id, "initial voters observe the same leader")
            .await
            .expect("wait for shared leader");
    }
    (registry, engines, leader_id)
}

#[cfg(test)]
pub(super) async fn build_three_node_snapshot_purge_cluster(
    policy: InProcessRaftNetworkPolicy,
) -> (
    InProcessRaftRegistry,
    Vec<RaftGroupEngine>,
    Vec<Arc<RaftGroupLogStore>>,
    u64,
) {
    let registry = InProcessRaftRegistry::default();
    let config = Arc::new(
        Config {
            cluster_name: "ursula-sim-three-node-snapshot-purge".to_owned(),
            heartbeat_interval: 10,
            election_timeout_min: 50,
            election_timeout_max: 100,
            max_in_snapshot_log_to_keep: 0,
            purge_batch_size: 1,
            replication_lag_threshold: 0,
            snapshot_policy: SnapshotPolicy::Never,
            ..Default::default()
        }
        .validate()
        .expect("valid raft config"),
    );
    let mut nodes = BTreeMap::new();
    for node_id in 1..=3 {
        nodes.insert(node_id, BasicNode::new(format!("node-{node_id}")));
    }

    let mut engines = Vec::new();
    let mut log_stores = Vec::new();
    for node_id in 1..=3 {
        let log_store = RaftGroupLogStore::shared();
        let engine = RaftGroupEngine::new_node_with_log_store_and_network(
            placement(),
            node_id,
            config.clone(),
            InProcessRaftNetworkFactory::new(registry.clone())
                .with_source(node_id)
                .with_policy(policy.clone()),
            log_store.clone(),
            None,
            None,
        )
        .await
        .expect("create snapshot-purge simulated raft group node");
        registry.register(node_id, engine.raft_handle());
        engines.push(engine);
        log_stores.push(log_store);
    }

    engines[0]
        .initialize_membership(nodes)
        .await
        .expect("initialize snapshot-purge simulated raft group");
    let leader_metrics = engines[0]
        .raft_handle()
        .wait(Some(Duration::from_secs(5)))
        .metrics(|metrics| metrics.current_leader.is_some(), "leader elected")
        .await
        .expect("wait for snapshot-purge simulated leader");
    let leader_id = leader_metrics.current_leader.expect("leader id");
    (registry, engines, log_stores, leader_id)
}

pub(super) fn placement() -> ShardPlacement {
    ShardPlacement {
        core_id: CoreId(0),
        shard_id: ShardId(0),
        raft_group_id: RaftGroupId(0),
    }
}

pub(super) fn choose_runtime_streams_spanning_placement(
    runtime: &ShardRuntime,
    base: &BucketStreamId,
    count: usize,
) -> Vec<BucketStreamId> {
    let mut candidates = Vec::new();
    for index in 0..256 {
        let stream = BucketStreamId::new(
            base.bucket_id.clone(),
            format!("{}-client-{index}", base.stream_id),
        );
        let placement = runtime.locate(&stream);
        candidates.push((stream, placement));
    }

    let mut selected = Vec::with_capacity(count);
    let mut cores = BTreeSet::new();
    let mut groups = BTreeSet::new();
    for (stream, placement) in &candidates {
        let adds_core = cores.insert(placement.core_id.0);
        let adds_group = groups.insert(placement.raft_group_id.0);
        if selected.len() < count && (adds_core || adds_group || selected.len() < 2) {
            selected.push(stream.clone());
        }
        if selected.len() == count {
            break;
        }
    }

    for (stream, _) in &candidates {
        if selected.len() == count {
            break;
        }
        if !selected.iter().any(|selected| selected == stream) {
            selected.push(stream.clone());
        }
    }

    assert!(
        selected.len() >= count,
        "could not select enough runtime streams across placement"
    );
    selected.truncate(count);
    let mut selected_cores = BTreeSet::new();
    let mut selected_groups = BTreeSet::new();
    for stream in &selected {
        let placement = runtime.locate(stream);
        selected_cores.insert(placement.core_id.0);
        selected_groups.insert(placement.raft_group_id.0);
    }
    assert!(
        count < 2 || selected_cores.len() >= 2,
        "runtime streams should span at least two cores"
    );
    assert!(
        count < 2 || selected_groups.len() >= 2,
        "runtime streams should span at least two raft groups"
    );
    selected
}

pub(super) fn seeded_follower_id(seed: u64, leader_id: u64) -> u64 {
    let mut followers: Vec<u64> = (1..=3).filter(|node_id| *node_id != leader_id).collect();
    followers.sort_unstable();
    let mixed = seed ^ seed.rotate_left(17) ^ 0x9e37_79b9_7f4a_7c15;
    followers[usize::try_from(mixed % followers.len() as u64).expect("index fits usize")]
}

#[cfg(test)]
mod tests;

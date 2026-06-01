//! Ursula HTTP server: axum router, request handlers, response rendering,
//! plus the env-driven `ShardRuntime` constructors used by the `ursula` binary.
//!
//! Module map:
//!
//! - [`render`]: response builders, header helpers, SSE/multipart rendering.
//! - [`bootstrap`]: env-driven `spawn_*_runtime` constructors and cold-flush worker.

mod bootstrap;
mod http_time {
    #[cfg(madsim)]
    pub use madsim::time::timeout;
    #[cfg(not(madsim))]
    pub use tokio::time::timeout;
}
mod render;

pub use bootstrap::{
    StaticGrpcRaftMembershipConfig, spawn_cold_flush_worker_if_configured,
    spawn_cold_gc_worker_if_configured, spawn_default_runtime, spawn_raft_memory_runtime,
    spawn_raft_runtime, spawn_static_grpc_raft_memory_runtime,
    spawn_static_grpc_raft_memory_runtime_with_membership_config,
    spawn_static_grpc_raft_memory_runtime_with_per_group_initializers,
    spawn_static_grpc_raft_runtime, spawn_static_grpc_raft_runtime_with_membership_config,
    spawn_static_grpc_raft_runtime_with_per_group_initializers, spawn_wal_runtime,
};

use std::collections::hash_map::DefaultHasher;
use std::collections::{BTreeMap, HashMap};
use std::convert::Infallible;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
#[cfg(not(madsim))]
use std::time::{SystemTime, UNIX_EPOCH};

use axum::Router;
use axum::body::{Body, Bytes};
use axum::extract::{DefaultBodyLimit, OriginalUri, Path, RawQuery, State};
use axum::http::header::{CACHE_CONTROL, CONTENT_TYPE, ETAG, IF_NONE_MATCH, LOCATION};
use axum::http::{HeaderMap, HeaderValue, Method, Request, StatusCode, Uri};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post, put};

use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use chrono::{DateTime, SecondsFormat, Utc};
use futures_util::stream;
use openraft::BasicNode;
use openraft::rt::WatchReceiver;
use ursula_raft::{
    LeadershipShedFlag, RAFT_GRPC_APPEND_PATH, RAFT_GRPC_FULL_SNAPSHOT_PATH,
    RAFT_GRPC_GROUP_READ_PATH, RAFT_GRPC_GROUP_WRITE_PATH, RAFT_GRPC_MAX_MESSAGE_BYTES,
    RAFT_GRPC_TRANSFER_LEADER_PATH, RAFT_GRPC_VOTE_PATH, RaftGroupHandleRegistry, RaftGrpcService,
    RaftLogProgressSnapshot, raft_internal_proto,
};
use ursula_runtime::{
    AppendBatchRequest, AppendExternalRequest, AppendRequest, AppendResponse,
    BootstrapStreamRequest, CloseStreamRequest, CreateStreamExternalRequest, CreateStreamRequest,
    CreateStreamResponse, DeleteSnapshotRequest, DeleteStreamRequest, ExternalPayloadRef,
    HeadStreamRequest, PlanColdFlushRequest, ProducerRequest, PublishSnapshotRequest,
    ReadSnapshotRequest, ReadStreamRequest, RuntimeError, ShardRuntime, new_external_payload_path,
};
use ursula_shard::{BucketStreamId, RaftGroupId};

use crate::bootstrap::env_usize;
use crate::render::*;

type BoxResponse = Box<Response>;

const DEFAULT_CONTENT_TYPE: &str = "application/octet-stream";
const HEADER_STREAM_CLOSED: &str = "stream-closed";
const HEADER_STREAM_CURSOR: &str = "stream-cursor";
const HEADER_STREAM_EXPIRES_AT: &str = "stream-expires-at";
const HEADER_STREAM_FORK_OFFSET: &str = "stream-fork-offset";
const HEADER_STREAM_FORKED_FROM: &str = "stream-forked-from";
const HEADER_STREAM_INTEGRITY_EVICTED_RECORDS: &str = "stream-integrity-evicted-records";
const HEADER_STREAM_INTEGRITY_EVICTED_SETSUM: &str = "stream-integrity-evicted-setsum";
const HEADER_STREAM_INTEGRITY_LIVE_RECORDS: &str = "stream-integrity-live-records";
const HEADER_STREAM_INTEGRITY_LIVE_SETSUM: &str = "stream-integrity-live-setsum";
const HEADER_STREAM_INTEGRITY_LIVE_START_OFFSET: &str = "stream-integrity-live-start-offset";
const HEADER_STREAM_INTEGRITY_TOTAL_RECORDS: &str = "stream-integrity-total-records";
const HEADER_STREAM_INTEGRITY_TOTAL_SETSUM: &str = "stream-integrity-total-setsum";
const HEADER_STREAM_COLD_HOT_START_OFFSET: &str = "stream-cold-hot-start-offset";
const HEADER_STREAM_NEXT_OFFSET: &str = "stream-next-offset";
const HEADER_STREAM_SNAPSHOT_OFFSET: &str = "stream-snapshot-offset";
const HEADER_STREAM_SSE_DATA_ENCODING: &str = "stream-sse-data-encoding";
const HEADER_STREAM_SEQ: &str = "stream-seq";
const HEADER_STREAM_TTL: &str = "stream-ttl";
const HEADER_STREAM_UP_TO_DATE: &str = "stream-up-to-date";
const HEADER_PRODUCER_ID: &str = "producer-id";
const HEADER_PRODUCER_EPOCH: &str = "producer-epoch";
const HEADER_PRODUCER_SEQ: &str = "producer-seq";
const HEADER_PREFER: &str = "prefer";
const HEADER_X_CONTENT_TYPE_OPTIONS: &str = "x-content-type-options";
const HEADER_CROSS_ORIGIN_RESOURCE_POLICY: &str = "cross-origin-resource-policy";
const HEADER_URSULA_RAFT_LEADER_ID: &str = "x-ursula-raft-leader-id";
const APPEND_BATCH_MAX_ITEMS: usize = 512;
const APPEND_BATCH_MAX_BYTES: usize = 32 * 1024 * 1024;
const MAX_HTTP_BODY_BYTES: usize = 32 * 1024 * 1024;
const DEFAULT_LONG_POLL_TIMEOUT_MS: u64 = 1_000;
const MAX_LONG_POLL_TIMEOUT_MS: u64 = 60_000;
const V1_DEFAULT_BUCKET: &str = "_default";

struct CreateStreamHttpResponseInput<'a> {
    response: CreateStreamResponse,
    stream_id: &'a BucketStreamId,
    content_type: &'a str,
    stream_ttl_seconds: Option<u64>,
    stream_expires_at_ms: Option<u64>,
    producer: Option<&'a ProducerRequest>,
    public_path: Option<&'a str>,
    request_headers: &'a HeaderMap,
}

pub trait WallClock: Send + Sync + 'static {
    fn unix_time_ms(&self) -> u64;
}

#[derive(Debug, Default)]
pub struct SystemWallClock;

impl WallClock for SystemWallClock {
    fn unix_time_ms(&self) -> u64 {
        unix_time_ms()
    }
}

#[derive(Clone)]
pub struct HttpState {
    runtime: ShardRuntime,
    raft_registry: Option<RaftGroupHandleRegistry>,
    client_write_router: Option<ClientWriteLeaderRouter>,
    http_metrics: Arc<HttpMetrics>,
    wall_clock: Arc<dyn WallClock>,
    node_memory_admission: NodeMemoryAdmission,
    leadership_shed: LeadershipShedFlag,
}

impl HttpState {
    pub fn new(runtime: ShardRuntime) -> Self {
        Self {
            runtime,
            raft_registry: None,
            client_write_router: None,
            http_metrics: Arc::new(HttpMetrics::default()),
            wall_clock: Arc::new(SystemWallClock),
            node_memory_admission: NodeMemoryAdmission::from_env(),
            leadership_shed: Arc::new(std::sync::atomic::AtomicU8::new(0)),
        }
    }

    pub fn with_raft_registry(
        runtime: ShardRuntime,
        raft_registry: RaftGroupHandleRegistry,
    ) -> Self {
        let leadership_shed = raft_registry.leadership_shed_flag();
        Self {
            runtime,
            raft_registry: Some(raft_registry),
            client_write_router: None,
            http_metrics: Arc::new(HttpMetrics::default()),
            wall_clock: Arc::new(SystemWallClock),
            node_memory_admission: NodeMemoryAdmission::from_env(),
            leadership_shed,
        }
    }

    pub fn with_static_raft_cluster(
        runtime: ShardRuntime,
        raft_registry: RaftGroupHandleRegistry,
        peers: impl IntoIterator<Item = (u64, String)>,
    ) -> Self {
        let leadership_shed = raft_registry.leadership_shed_flag();
        Self {
            runtime,
            raft_registry: Some(raft_registry),
            client_write_router: Some(ClientWriteLeaderRouter::new(peers)),
            http_metrics: Arc::new(HttpMetrics::default()),
            wall_clock: Arc::new(SystemWallClock),
            node_memory_admission: NodeMemoryAdmission::from_env(),
            leadership_shed,
        }
    }

    pub fn leadership_shed_flag(&self) -> LeadershipShedFlag {
        self.leadership_shed.clone()
    }

    /// Replace the leadership-shed flag with one shared with the bootstrap
    /// health gates. They set per-gate bits on shed and clear their own bit on
    /// heal; the raft registry policy decides separately whether the node may
    /// campaign, shed current leaders, or accept inbound leadership transfer.
    pub fn with_leadership_shed_flag(mut self, flag: LeadershipShedFlag) -> Self {
        self.leadership_shed = flag;
        self
    }

    pub fn with_wall_clock(mut self, wall_clock: impl WallClock) -> Self {
        self.wall_clock = Arc::new(wall_clock);
        self
    }

    pub fn with_wall_clock_handle(mut self, wall_clock: Arc<dyn WallClock>) -> Self {
        self.wall_clock = wall_clock;
        self
    }

    pub fn runtime(&self) -> &ShardRuntime {
        &self.runtime
    }

    pub fn raft_registry(&self) -> Option<&RaftGroupHandleRegistry> {
        self.raft_registry.as_ref()
    }

    pub fn client_write_router(&self) -> Option<&ClientWriteLeaderRouter> {
        self.client_write_router.as_ref()
    }

    pub fn unix_time_ms(&self) -> u64 {
        self.wall_clock.unix_time_ms()
    }
}

#[derive(Debug, Default)]
struct HttpMetrics {
    sse_streams_opened: AtomicU64,
    sse_read_iterations: AtomicU64,
    sse_data_events: AtomicU64,
    sse_control_events: AtomicU64,
    sse_error_events: AtomicU64,
}

impl HttpMetrics {
    fn snapshot(&self) -> HttpMetricsSnapshot {
        HttpMetricsSnapshot {
            sse_streams_opened: self.sse_streams_opened.load(Ordering::Relaxed),
            sse_read_iterations: self.sse_read_iterations.load(Ordering::Relaxed),
            sse_data_events: self.sse_data_events.load(Ordering::Relaxed),
            sse_control_events: self.sse_control_events.load(Ordering::Relaxed),
            sse_error_events: self.sse_error_events.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Clone, Copy, Default)]
struct HttpMetricsSnapshot {
    sse_streams_opened: u64,
    sse_read_iterations: u64,
    sse_data_events: u64,
    sse_control_events: u64,
    sse_error_events: u64,
}

/// Resolves the current leader of a raft group to a client-reachable base URL
/// so a write/read that lands on a non-leader can be answered with a 307
/// redirect. `peers` maps raft node id to that node's listen address; gRPC and
/// the client API share one listener, so a single address per peer is both the
/// replication endpoint and the redirect target.
#[derive(Clone, Debug)]
pub struct ClientWriteLeaderRouter {
    peers: Arc<BTreeMap<u64, String>>,
}

impl ClientWriteLeaderRouter {
    pub fn new(peers: impl IntoIterator<Item = (u64, String)>) -> Self {
        Self {
            peers: Arc::new(
                peers
                    .into_iter()
                    .map(|(node_id, url)| (node_id, url.trim_end_matches('/').to_owned()))
                    .collect(),
            ),
        }
    }

    fn leader_base(&self, err: &RuntimeError) -> Option<(u64, String)> {
        let RuntimeError::GroupEngine {
            leader_hint: Some(leader_hint),
            ..
        } = err
        else {
            return None;
        };
        let leader_id = leader_hint.node_id?;
        let leader_base = self
            .peers
            .get(&leader_id)
            .or(leader_hint.address.as_ref())?;
        Some((leader_id, leader_base.trim_end_matches('/').to_owned()))
    }

    fn redirect_response(&self, err: &RuntimeError, request_target: &str) -> Option<Response> {
        let (leader_id, leader_base) = self.leader_base(err)?;
        let mut headers = HeaderMap::new();
        insert_default_response_headers(&mut headers);
        let leader_url = format!("{}{}", leader_base.trim_end_matches('/'), request_target);
        if let Ok(value) = HeaderValue::from_str(&leader_url) {
            headers.insert(LOCATION, value);
        } else {
            return None;
        }
        insert_u64_header(&mut headers, HEADER_URSULA_RAFT_LEADER_ID, leader_id);
        Some((StatusCode::TEMPORARY_REDIRECT, headers, err.to_string()).into_response())
    }
}

/// Process-wide RSS soft-cap admission. Independent from cold/raft/forward
/// admissions; this one sheds writes when the entire process is approaching
/// OOM regardless of which downstream is misbehaving. Read-only routes are
/// not gated by this admission.
///
/// Shedding is *probabilistic* between `soft_cap` and `hard_cap`: at soft_cap
/// 0% of writes are rejected, at hard_cap 100% are rejected, linear in
/// between. Going off a cliff at soft_cap turned an s3_unavailable transient
/// into a complete write outage — even partial throughput keeps producers
/// alive while cold flush drains the live region back under soft_cap, and
/// shedding ramps in / out smoothly tracks the RSS trajectory rather than
/// snapping at a single threshold.
#[derive(Clone)]
pub struct NodeMemoryAdmission {
    soft_cap_bytes: Option<u64>,
    hard_cap_bytes: Option<u64>,
    last_rss_bytes: Arc<AtomicU64>,
}

impl std::fmt::Debug for NodeMemoryAdmission {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NodeMemoryAdmission")
            .field("soft_cap_bytes", &self.soft_cap_bytes)
            .field("hard_cap_bytes", &self.hard_cap_bytes)
            .field(
                "last_rss_bytes",
                &self.last_rss_bytes.load(Ordering::Relaxed),
            )
            .finish()
    }
}

impl NodeMemoryAdmission {
    pub fn disabled() -> Self {
        Self {
            soft_cap_bytes: None,
            hard_cap_bytes: None,
            last_rss_bytes: Arc::new(AtomicU64::new(0)),
        }
    }

    pub fn from_env() -> Self {
        let admission = Self {
            soft_cap_bytes: env_u64_optional("URSULA_NODE_MEMORY_SOFT_CAP_BYTES"),
            hard_cap_bytes: env_u64_optional("URSULA_NODE_MEMORY_HARD_CAP_BYTES"),
            last_rss_bytes: Arc::new(AtomicU64::new(0)),
        };
        // Always sample RSS so the /__ursula/metrics endpoint can report
        // process memory even when the admission itself is disabled. The
        // sampler is the only path that observes OOM trajectories under
        // chaos faults — when SSM exec dies post-OOM, this remains the
        // sole real-time signal available via the status.json pipeline.
        admission.spawn_rss_sampler();
        admission
    }

    pub fn is_enabled(&self) -> bool {
        self.soft_cap_bytes.is_some()
    }

    pub fn last_rss_bytes(&self) -> u64 {
        self.last_rss_bytes.load(Ordering::Relaxed)
    }

    pub fn soft_cap_bytes(&self) -> Option<u64> {
        self.soft_cap_bytes
    }

    /// Effective hard cap: explicit env override, or `soft + soft/4` when only
    /// the soft cap is configured. Returns None iff the admission is disabled.
    pub fn effective_hard_cap_bytes(&self) -> Option<u64> {
        let soft = self.soft_cap_bytes?;
        let hard = self
            .hard_cap_bytes
            .unwrap_or_else(|| soft.saturating_add(soft / 4));
        Some(hard.max(soft))
    }

    #[cfg(madsim)]
    fn spawn_rss_sampler(&self) {
        // Deterministic simulation: never report RSS. The admission stays
        // installed for plumbing parity but never trips.
    }

    #[cfg(not(madsim))]
    fn spawn_rss_sampler(&self) {
        let last_rss_bytes = self.last_rss_bytes.clone();
        let abort_cap = env_u64_optional("URSULA_NODE_MEMORY_ABORT_CAP_BYTES");
        let soft_cap = self.soft_cap_bytes;
        let hard_cap = self.effective_hard_cap_bytes();
        // Optional sticky path: if set we also write the same breadcrumb to a
        // file so it survives systemd journald rotation and is trivially
        // pulled later via SSH. Path is set, not derived, so deployments can
        // route it into a long-lived disk (e.g. /var/log/ursula-abort.json).
        let abort_log_path = std::env::var("URSULA_NODE_MEMORY_ABORT_LOG_PATH").ok();
        tokio::spawn(async move {
            loop {
                if let Some(rss) = read_proc_self_status_vm_rss_bytes() {
                    last_rss_bytes.store(rss, Ordering::Relaxed);
                    if let Some(cap) = abort_cap
                        && rss > cap
                    {
                        // Last-resort safety net: ingress admissions (cold,
                        // raft uncommitted, forward, concurrency) should bound
                        // memory under normal load. If we still overshoot, the
                        // unaccounted growth (allocator slack, framework
                        // overhead) is unbounded — process::abort lets systemd
                        // restart from a clean state instead of riding the
                        // OOM-killer into a kernel-level shutdown that also
                        // takes the SSM agent / cloud-init credentials with it.
                        //
                        // Before aborting, emit a structured breadcrumb to
                        // stderr (so journald captures it) AND optionally to a
                        // sticky file. The previous bare eprintln was hard to
                        // grep across many nodes and left no machine-readable
                        // trail — under chaos load when this fires across the
                        // fleet we want to correlate aborts to the same minute.
                        let host = std::env::var("HOSTNAME")
                            .ok()
                            .or_else(|| std::fs::read_to_string("/proc/sys/kernel/hostname").ok())
                            .map(|s| s.trim().to_string())
                            .unwrap_or_default();
                        let now_ms = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .map(|d| d.as_millis())
                            .unwrap_or(0);
                        let breadcrumb = format!(
                            "{{\"event\":\"memory_admission_abort\",\"ts_ms\":{now_ms},\"host\":\"{host}\",\"rss_bytes\":{rss},\"soft_cap_bytes\":{soft},\"hard_cap_bytes\":{hard},\"abort_cap_bytes\":{cap}}}",
                            soft = soft_cap
                                .map(|v| v.to_string())
                                .unwrap_or_else(|| "null".into()),
                            hard = hard_cap
                                .map(|v| v.to_string())
                                .unwrap_or_else(|| "null".into()),
                        );
                        eprintln!("{breadcrumb}");
                        if let Some(path) = &abort_log_path {
                            // Best-effort: filesystem write may fail under
                            // memory pressure but the stderr line above is
                            // already in flight. Don't bother retrying.
                            let _ = std::fs::write(path, format!("{breadcrumb}\n"));
                        }
                        std::process::abort();
                    }
                }
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        });
    }
}

/// Returns Some(503-response) when admission decides to shed this write.
/// Below soft_cap: never shed. Above hard_cap: always shed. In between, the
/// shed probability ramps linearly so producers keep partial throughput while
/// the live region drains back. Read paths bypass this entirely.
fn node_memory_admission_response(admission: &NodeMemoryAdmission) -> Option<Response> {
    let soft = admission.soft_cap_bytes?;
    let hard = admission.effective_hard_cap_bytes()?;
    let rss = admission.last_rss_bytes.load(Ordering::Relaxed);
    if rss <= soft {
        return None;
    }
    let reject_prob: f64 = if rss >= hard || hard == soft {
        1.0
    } else {
        (rss - soft) as f64 / (hard - soft) as f64
    };
    if reject_prob < 1.0 && admission_sample() >= reject_prob {
        return None;
    }
    let mut headers = HeaderMap::new();
    insert_default_response_headers(&mut headers);
    headers.insert(
        axum::http::header::RETRY_AFTER,
        HeaderValue::from_static("1"),
    );
    headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    let body = format!(
        "{{\"error\":\"NodeMemoryBackpressure\",\"rss_bytes\":{rss},\"soft_cap_bytes\":{soft},\"hard_cap_bytes\":{hard}}}"
    );
    Some((StatusCode::SERVICE_UNAVAILABLE, headers, body).into_response())
}

/// Cheap process-local pseudo-random sample in [0, 1). Xorshift64 over a
/// shared atomic seed; race-induced bias is irrelevant for load shedding and
/// avoids pulling in a `rand` dependency just for this path.
fn admission_sample() -> f64 {
    static SEED: AtomicU64 = AtomicU64::new(0xfeed_face_cafe_beef);
    let mut s = SEED.load(Ordering::Relaxed);
    if s == 0 {
        s = 0xdead_beef;
    }
    s ^= s << 13;
    s ^= s >> 7;
    s ^= s << 17;
    SEED.store(s, Ordering::Relaxed);
    // Map the high 53 bits into [0, 1) so the result fits in an f64 mantissa.
    ((s >> 11) as f64) / ((1u64 << 53) as f64)
}

#[cfg(not(madsim))]
fn read_proc_self_status_vm_rss_bytes() -> Option<u64> {
    // Linux-only: parse `VmRSS:    NNN kB` from /proc/self/status.
    let raw = std::fs::read_to_string("/proc/self/status").ok()?;
    for line in raw.lines() {
        if let Some(rest) = line.strip_prefix("VmRSS:") {
            let kb: u64 = rest.split_whitespace().next()?.parse().ok()?;
            return Some(kb.saturating_mul(1024));
        }
    }
    None
}

fn env_u64_optional(name: &str) -> Option<u64> {
    std::env::var(name)
        .ok()
        .and_then(|raw| raw.parse::<u64>().ok())
}

#[derive(Clone)]

struct HttpRaftGrpcService {
    raft: RaftGrpcService,
}

impl HttpRaftGrpcService {
    fn new(registry: RaftGroupHandleRegistry, state: HttpState) -> Self {
        let cold_store = state.runtime().cold_store();
        let leadership_shed = state.leadership_shed_flag();
        Self {
            raft: RaftGrpcService::new(registry)
                .with_cold_store(cold_store)
                .with_leadership_shed_flag(leadership_shed),
        }
    }
}

#[tonic::async_trait]
impl raft_internal_proto::raft_internal_server::RaftInternal for HttpRaftGrpcService {
    async fn append(
        &self,
        request: tonic::Request<raft_internal_proto::RaftRpcEnvelopeV1>,
    ) -> Result<tonic::Response<raft_internal_proto::RaftRpcAckV1>, tonic::Status> {
        raft_internal_proto::raft_internal_server::RaftInternal::append(&self.raft, request).await
    }

    async fn vote(
        &self,
        request: tonic::Request<raft_internal_proto::RaftRpcEnvelopeV1>,
    ) -> Result<tonic::Response<raft_internal_proto::RaftRpcAckV1>, tonic::Status> {
        raft_internal_proto::raft_internal_server::RaftInternal::vote(&self.raft, request).await
    }

    async fn full_snapshot(
        &self,
        request: tonic::Request<raft_internal_proto::RaftFullSnapshotRequestV1>,
    ) -> Result<tonic::Response<raft_internal_proto::RaftFullSnapshotAckV1>, tonic::Status> {
        raft_internal_proto::raft_internal_server::RaftInternal::full_snapshot(&self.raft, request)
            .await
    }

    async fn group_write(
        &self,
        request: tonic::Request<raft_internal_proto::GroupWriteRequestV1>,
    ) -> Result<tonic::Response<raft_internal_proto::GroupWriteResponseV1>, tonic::Status> {
        raft_internal_proto::raft_internal_server::RaftInternal::group_write(&self.raft, request)
            .await
    }

    async fn group_read(
        &self,
        request: tonic::Request<raft_internal_proto::GroupReadRequestV1>,
    ) -> Result<tonic::Response<raft_internal_proto::GroupReadResponseV1>, tonic::Status> {
        raft_internal_proto::raft_internal_server::RaftInternal::group_read(&self.raft, request)
            .await
    }

    async fn transfer_leader(
        &self,
        request: tonic::Request<raft_internal_proto::RaftTransferLeaderRequestV1>,
    ) -> Result<tonic::Response<raft_internal_proto::RaftTransferLeaderAckV1>, tonic::Status> {
        raft_internal_proto::raft_internal_server::RaftInternal::transfer_leader(
            &self.raft, request,
        )
        .await
    }
}

fn raft_grpc_service(
    state: HttpState,
    registry: RaftGroupHandleRegistry,
) -> raft_internal_proto::raft_internal_server::RaftInternalServer<HttpRaftGrpcService> {
    raft_internal_proto::raft_internal_server::RaftInternalServer::new(HttpRaftGrpcService::new(
        registry, state,
    ))
    .max_decoding_message_size(RAFT_GRPC_MAX_MESSAGE_BYTES)
    .max_encoding_message_size(RAFT_GRPC_MAX_MESSAGE_BYTES)
}

pub fn router(runtime: ShardRuntime) -> Router {
    router_from_state(HttpState::new(runtime))
}

pub fn router_with_raft_registry(
    runtime: ShardRuntime,
    raft_registry: RaftGroupHandleRegistry,
) -> Router {
    router_from_state(HttpState::with_raft_registry(runtime, raft_registry))
}

pub fn router_with_static_raft_cluster(
    runtime: ShardRuntime,
    raft_registry: RaftGroupHandleRegistry,
    peers: impl IntoIterator<Item = (u64, String)>,
) -> Router {
    router_from_state(HttpState::with_static_raft_cluster(
        runtime,
        raft_registry,
        peers,
    ))
}

pub fn router_with_http_state(state: HttpState) -> Router {
    router_from_state(state)
}

/// Cluster-plane routes: inter-node gRPC carrying Raft RPCs, snapshot
/// transfer, and HTTP-write forwarding to the leader. In a dual-listener
/// deployment these bind to the private (VPC) interface so chaos applied to
/// the public face never disrupts consensus.
pub fn cluster_router_from_state(state: HttpState) -> Router {
    let raft_registry = state.raft_registry.clone().unwrap_or_default();
    Router::new()
        .route_service(
            RAFT_GRPC_APPEND_PATH,
            raft_grpc_service(state.clone(), raft_registry.clone()),
        )
        .route_service(
            RAFT_GRPC_VOTE_PATH,
            raft_grpc_service(state.clone(), raft_registry.clone()),
        )
        .route_service(
            RAFT_GRPC_FULL_SNAPSHOT_PATH,
            raft_grpc_service(state.clone(), raft_registry.clone()),
        )
        .route_service(
            RAFT_GRPC_GROUP_WRITE_PATH,
            raft_grpc_service(state.clone(), raft_registry.clone()),
        )
        .route_service(
            RAFT_GRPC_GROUP_READ_PATH,
            raft_grpc_service(state.clone(), raft_registry.clone()),
        )
        .route_service(
            RAFT_GRPC_TRANSFER_LEADER_PATH,
            raft_grpc_service(state.clone(), raft_registry),
        )
        .route(LEADERSHIP_SHED_PATH, get(leadership_shed_status))
        .layer(DefaultBodyLimit::max(MAX_HTTP_BODY_BYTES))
        .with_state(state)
}

/// Client-plane routes: HTTP append/read/admin endpoints used by external
/// callers (producers, readers, operators). In a dual-listener deployment
/// these bind to the public interface; failure injection against the public
/// face only affects this plane.
pub fn client_router_from_state(state: HttpState) -> Router {
    let memory_admission = state.node_memory_admission.clone();
    Router::new()
        .route("/__ursula/metrics", get(metrics))
        .route(CLUSTER_PROBE_PATH, post(cluster_probe))
        .route(
            "/__ursula/flush-cold/{bucket}/{stream}",
            post(flush_cold_stream),
        )
        .route(
            "/__ursula/raft/{raft_group_id}/snapshot",
            post(trigger_raft_snapshot),
        )
        .route(
            "/__ursula/raft/{raft_group_id}/purge",
            post(trigger_raft_purge),
        )
        .route(
            "/__ursula/raft/{raft_group_id}/learners/{node_id}",
            post(add_raft_learner),
        )
        .route(
            "/__ursula/raft/{raft_group_id}/nodes/{node_id}/allow-next-revert",
            post(allow_raft_node_next_revert),
        )
        .route(
            "/__ursula/raft/{raft_group_id}/leader/transfer/{node_id}",
            post(transfer_raft_leader),
        )
        .route(
            "/v1/stream/{*path}",
            put(create_stream_v1)
                .post(append_stream_v1)
                .get(read_stream_v1)
                .delete(delete_stream_v1)
                .head(head_stream_v1),
        )
        .route("/{bucket}", put(create_bucket))
        .route("/{bucket}/{stream}/snapshot", get(read_latest_snapshot))
        .route(
            "/{bucket}/{stream}/snapshot/{snapshot_offset}",
            put(publish_snapshot)
                .get(read_snapshot)
                .delete(delete_snapshot),
        )
        .route("/{bucket}/{stream}/bootstrap", get(bootstrap_stream))
        .route(
            "/{bucket}/{stream}",
            put(create_stream)
                .post(append_stream)
                .get(read_stream)
                .delete(delete_stream)
                .head(head_stream),
        )
        .route("/{bucket}/{stream}/append-batch", post(append_batch))
        .layer(DefaultBodyLimit::max(MAX_HTTP_BODY_BYTES))
        .layer(middleware::from_fn_with_state(
            memory_admission,
            node_memory_admission_middleware,
        ))
        .layer(middleware::from_fn_with_state(
            ConcurrencyLimiter::from_env(),
            concurrency_limit_middleware,
        ))
        .with_state(state)
}

/// Synchronous concurrent-request cap on the client plane. Bounds the worst-
/// case in-flight ingress memory: `concurrency × max_body_bytes` is the hard
/// upper bound on bytes held in axum/hyper accept buffers at any moment.
/// Bytes per request are not tracked separately — we conservatively count
/// every accepted request against the same budget.
///
/// Disabled when `URSULA_MAX_CONCURRENT_REQUESTS` is unset. When the cap is
/// reached, new requests are rejected with 503 + Retry-After: 1 immediately,
/// no queueing (the queue itself would be unbounded ingress memory).
#[derive(Clone)]
struct ConcurrencyLimiter {
    semaphore: Option<Arc<tokio::sync::Semaphore>>,
}

impl ConcurrencyLimiter {
    fn from_env() -> Self {
        let limit = env_u64_optional("URSULA_MAX_CONCURRENT_REQUESTS")
            .and_then(|n| usize::try_from(n).ok())
            .filter(|n| *n > 0);
        Self {
            semaphore: limit.map(|n| Arc::new(tokio::sync::Semaphore::new(n))),
        }
    }
}

async fn concurrency_limit_middleware(
    State(limiter): State<ConcurrencyLimiter>,
    request: Request<Body>,
    next: Next,
) -> Response {
    let Some(sem) = limiter.semaphore.as_ref() else {
        return next.run(request).await;
    };
    match sem.clone().try_acquire_owned() {
        Ok(_permit) => next.run(request).await,
        Err(_) => {
            let mut headers = HeaderMap::new();
            insert_default_response_headers(&mut headers);
            headers.insert(
                axum::http::header::RETRY_AFTER,
                HeaderValue::from_static("1"),
            );
            headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
            let body = "{\"error\":\"ConcurrencyLimitReached\"}";
            (StatusCode::SERVICE_UNAVAILABLE, headers, body).into_response()
        }
    }
}

/// Path of the cluster egress-health probe (M2). Peers POST a payload here over
/// the cluster plane; the round-trip time exposes loss/delay on the sender's
/// egress, which a small heartbeat-sized request would mask.
pub(crate) const CLUSTER_PROBE_PATH: &str = "/__ursula/cluster-probe";
pub(crate) const LEADERSHIP_SHED_PATH: &str = "/__ursula/leadership-shed";

/// Probe target: drain the body (so the sender's full egress traverses the
/// cluster plane) and answer 200. Bypasses memory admission.
async fn cluster_probe(_body: Bytes) -> StatusCode {
    StatusCode::OK
}

async fn leadership_shed_status(State(state): State<HttpState>) -> Response {
    let shed_state = state
        .raft_registry()
        .map(RaftGroupHandleRegistry::leadership_shed_state)
        .unwrap_or_default();
    let mut body = String::from("{\"bits\":");
    body.push_str(&shed_state.bits().to_string());
    body.push_str(",\"state\":");
    push_json_string(&mut body, &shed_state.to_string());
    body.push_str(",\"should_accept_transfer\":");
    body.push_str(bool_json(shed_state.should_accept_transfer()));
    body.push_str(",\"should_campaign\":");
    body.push_str(bool_json(shed_state.should_campaign()));
    body.push_str(",\"should_shed_current_leaders\":");
    body.push_str(bool_json(shed_state.should_shed_current_leaders()));
    body.push('}');
    let mut headers = HeaderMap::new();
    insert_content_type(&mut headers, "application/json");
    (StatusCode::OK, headers, body).into_response()
}

fn bool_json(value: bool) -> &'static str {
    if value { "true" } else { "false" }
}

/// axum middleware that short-circuits write-method requests (PUT/POST/PATCH/
/// DELETE) with a 503 + Retry-After when the per-process RSS is over the
/// configured soft cap. Reads (GET/HEAD/OPTIONS) bypass the check so a
/// memory-pressured node still serves cached data and metrics.
async fn node_memory_admission_middleware(
    State(admission): State<NodeMemoryAdmission>,
    request: Request<Body>,
    next: Next,
) -> Response {
    if !admission.is_enabled() {
        return next.run(request).await;
    }
    // The cluster egress-health probe is a POST but carries no durable work; it
    // must bypass admission so RSS pressure is never mistaken for an egress
    // fault (which would wrongly trigger a leadership yield).
    if request.uri().path() == CLUSTER_PROBE_PATH {
        return next.run(request).await;
    }
    let method = request.method();
    let is_write = matches!(
        *method,
        Method::POST | Method::PUT | Method::PATCH | Method::DELETE
    );
    if is_write && let Some(rejected) = node_memory_admission_response(&admission) {
        return rejected;
    }
    next.run(request).await
}

/// Single-listener router that serves both planes from one bind. Kept for
/// in-process tests and backward-compatible deployments that don't separate
/// the public and cluster network paths.
fn router_from_state(state: HttpState) -> Router {
    cluster_router_from_state(state.clone()).merge(client_router_from_state(state))
}

pub(crate) fn should_externalize_payload(
    state: &HttpState,
    payload_len: usize,
    allowed: bool,
) -> bool {
    allowed
        && payload_len > 0
        && state.runtime.has_cold_store()
        && payload_len >= env_usize("URSULA_EXTERNAL_PAYLOAD_MIN_BYTES", 1024 * 1024)
}

pub(crate) async fn stage_external_payload(
    state: &HttpState,
    stream_id: &BucketStreamId,
    payload: &[u8],
) -> Result<ExternalPayloadRef, Response> {
    let Some(cold_store) = state.runtime.cold_store() else {
        return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            "URSULA_COLD_BACKEND must be configured before externalizing payloads",
        )
            .into_response());
    };
    let s3_path = new_external_payload_path(stream_id);
    let object_size = cold_store
        .write_chunk(&s3_path, payload)
        .await
        .map_err(|err| {
            (
                StatusCode::BAD_GATEWAY,
                format!("write external payload object: {err}"),
            )
                .into_response()
        })?;
    Ok(ExternalPayloadRef {
        s3_path,
        payload_len: u64::try_from(payload.len()).expect("payload len fits u64"),
        object_size,
    })
}

pub(crate) async fn cleanup_external_payload(state: &HttpState, s3_path: &str) {
    let Some(cold_store) = state.runtime.cold_store() else {
        return;
    };
    let _ = cold_store.delete_chunk(s3_path).await;
}

pub(crate) fn create_stream_http_response(input: CreateStreamHttpResponseInput<'_>) -> Response {
    let CreateStreamHttpResponseInput {
        response,
        stream_id,
        content_type,
        stream_ttl_seconds,
        stream_expires_at_ms,
        producer,
        public_path,
        request_headers,
    } = input;
    let mut headers = HeaderMap::new();
    insert_default_response_headers(&mut headers);
    insert_content_type(&mut headers, content_type);
    insert_offset(&mut headers, response.next_offset);
    if let Some(public_path) = public_path {
        insert_public_location(&mut headers, request_headers, public_path);
    } else {
        insert_location(&mut headers, stream_id);
    }
    insert_lifetime_headers(&mut headers, stream_ttl_seconds, stream_expires_at_ms);
    insert_producer_ack(&mut headers, producer);
    if response.closed {
        insert_static(&mut headers, HEADER_STREAM_CLOSED, "true");
    }
    let status = if response.already_exists {
        StatusCode::OK
    } else {
        StatusCode::CREATED
    };
    (status, headers).into_response()
}

pub(crate) fn append_http_response(response: AppendResponse) -> Response {
    let mut headers = HeaderMap::new();
    insert_default_response_headers(&mut headers);
    insert_offset(&mut headers, response.next_offset);
    insert_producer_ack(&mut headers, response.producer.as_ref());
    if response.closed {
        insert_static(&mut headers, HEADER_STREAM_CLOSED, "true");
    }
    let status = if response.producer.is_some() && !response.deduplicated {
        StatusCode::OK
    } else {
        StatusCode::NO_CONTENT
    };
    (status, headers).into_response()
}

pub(crate) async fn create_bucket(Path(_bucket): Path<String>) -> Response {
    StatusCode::CREATED.into_response()
}

pub(crate) async fn metrics(State(state): State<HttpState>) -> Response {
    let mut headers = HeaderMap::new();
    insert_content_type(&mut headers, "application/json");
    let raft_groups = state
        .raft_registry()
        .map(RaftGroupHandleRegistry::metrics_snapshot)
        .unwrap_or_default();
    let mut body = render_metrics(
        state.runtime.metrics().snapshot(),
        state.runtime.mailbox_snapshot(),
        state.http_metrics.snapshot(),
        &raft_groups,
        state.runtime.cold_store_info().as_ref(),
    );
    // Splice process-level memory observability onto the metrics JSON so
    // a chaos node's RSS trajectory is visible from any HTTP client (the
    // status-publishing pipeline survives SSM exec failures).
    let rss = state.node_memory_admission.last_rss_bytes();
    let cap = state
        .node_memory_admission
        .soft_cap_bytes()
        .unwrap_or_default();
    if body.ends_with('}') {
        body.truncate(body.len() - 1);
        body.push_str(",\"process_rss_bytes\":");
        body.push_str(&rss.to_string());
        body.push_str(",\"node_memory_soft_cap_bytes\":");
        body.push_str(&cap.to_string());
        body.push('}');
    }
    (StatusCode::OK, headers, body).into_response()
}

pub(crate) async fn flush_cold_stream(
    State(state): State<HttpState>,
    OriginalUri(uri): OriginalUri,
    Path((bucket, stream)): Path<(String, String)>,
    RawQuery(raw_query): RawQuery,
) -> Response {
    let query = match parse_query(raw_query.as_deref()) {
        Ok(query) => query,
        Err(response) => return *response,
    };
    let min_hot_bytes = query
        .get("min_hot_bytes")
        .and_then(|raw| raw.parse::<usize>().ok())
        .unwrap_or(1);
    let max_flush_bytes = query
        .get("max_bytes")
        .and_then(|raw| raw.parse::<usize>().ok())
        .unwrap_or(8 * 1024 * 1024);
    let stream_id = BucketStreamId::new(bucket, stream);
    match state
        .runtime
        .flush_cold_once(PlanColdFlushRequest {
            stream_id,
            min_hot_bytes,
            max_flush_bytes,
        })
        .await
    {
        Ok(Some(response)) => {
            let mut headers = HeaderMap::new();
            insert_content_type(&mut headers, "application/json");
            (
                StatusCode::OK,
                headers,
                format!(
                    "{{\"hot_start_offset\":{},\"group_commit_index\":{}}}",
                    response.hot_start_offset, response.group_commit_index
                ),
            )
                .into_response()
        }
        Ok(None) => StatusCode::NO_CONTENT.into_response(),
        Err(err) => {
            runtime_error_or_leader_redirect_async(&state, err, &request_target(&uri)).await
        }
    }
}

pub(crate) async fn trigger_raft_snapshot(
    State(state): State<HttpState>,
    Path(raft_group_id): Path<u64>,
) -> Response {
    let Some(registry) = state.raft_registry() else {
        return (
            StatusCode::BAD_REQUEST,
            "raft registry is not configured for this server",
        )
            .into_response();
    };
    let Ok(raft_group_id) = parse_raft_group_id(raft_group_id) else {
        return (StatusCode::BAD_REQUEST, "invalid raft group id").into_response();
    };
    let Some(raft) = registry.get(raft_group_id) else {
        return (StatusCode::NOT_FOUND, "raft group is not registered").into_response();
    };
    let snapshot_log_id = raft.metrics().borrow_watched().last_applied;
    if let Err(err) = raft.trigger().snapshot().await {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("trigger raft snapshot: {err}"),
        )
            .into_response();
    }
    if let Some(snapshot_log_id) = snapshot_log_id
        && let Err(err) = raft
            .wait(Some(Duration::from_secs(10)))
            .snapshot(snapshot_log_id, "admin snapshot trigger")
            .await
    {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("wait for raft snapshot: {err}"),
        )
            .into_response();
    }

    let metrics = raft.metrics().borrow_watched().clone();
    (
        StatusCode::OK,
        [("content-type", "application/json")],
        format!(
            "{{\"raft_group_id\":{},\"snapshot_index\":{}}}",
            raft_group_id.0,
            optional_u64_json(metrics.snapshot.map(|log_id| log_id.index))
        ),
    )
        .into_response()
}

pub(crate) async fn trigger_raft_purge(
    State(state): State<HttpState>,
    Path(raft_group_id): Path<u64>,
    RawQuery(raw_query): RawQuery,
) -> Response {
    let query = match parse_query(raw_query.as_deref()) {
        Ok(query) => query,
        Err(response) => return *response,
    };
    let Some(upto) = query
        .get("upto")
        .and_then(|value| value.parse::<u64>().ok())
    else {
        return (StatusCode::BAD_REQUEST, "upto query parameter is required").into_response();
    };
    let Some(registry) = state.raft_registry() else {
        return (
            StatusCode::BAD_REQUEST,
            "raft registry is not configured for this server",
        )
            .into_response();
    };
    let Ok(raft_group_id) = parse_raft_group_id(raft_group_id) else {
        return (StatusCode::BAD_REQUEST, "invalid raft group id").into_response();
    };
    let Some(raft) = registry.get(raft_group_id) else {
        return (StatusCode::NOT_FOUND, "raft group is not registered").into_response();
    };
    if let Err(err) = raft.trigger().purge_log(upto).await {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("trigger raft purge: {err}"),
        )
            .into_response();
    }
    if let Err(err) = raft
        .wait(Some(Duration::from_secs(10)))
        .metrics(
            |metrics| metrics.purged.map(|log_id| log_id.index) >= Some(upto),
            format!("admin purge to index {upto}"),
        )
        .await
    {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("wait for raft purge: {err}"),
        )
            .into_response();
    }
    let metrics = raft.metrics().borrow_watched().clone();
    (
        StatusCode::OK,
        [("content-type", "application/json")],
        format!(
            "{{\"raft_group_id\":{},\"purged_index\":{}}}",
            raft_group_id.0,
            optional_u64_json(metrics.purged.map(|log_id| log_id.index))
        ),
    )
        .into_response()
}

pub(crate) async fn add_raft_learner(
    State(state): State<HttpState>,
    Path((raft_group_id, node_id)): Path<(u64, u64)>,
    RawQuery(raw_query): RawQuery,
) -> Response {
    let query = match parse_query(raw_query.as_deref()) {
        Ok(query) => query,
        Err(response) => return *response,
    };
    let Some(address) = query.get("addr").filter(|value| !value.trim().is_empty()) else {
        return (StatusCode::BAD_REQUEST, "addr query parameter is required").into_response();
    };
    let Some(registry) = state.raft_registry() else {
        return (
            StatusCode::BAD_REQUEST,
            "raft registry is not configured for this server",
        )
            .into_response();
    };
    let Ok(raft_group_id) = parse_raft_group_id(raft_group_id) else {
        return (StatusCode::BAD_REQUEST, "invalid raft group id").into_response();
    };
    let Some(raft) = registry.get(raft_group_id) else {
        return (StatusCode::NOT_FOUND, "raft group is not registered").into_response();
    };
    match raft
        .add_learner(node_id, BasicNode::new(address.clone()), true)
        .await
    {
        Ok(response) => (
            StatusCode::OK,
            [("content-type", "application/json")],
            format!(
                "{{\"raft_group_id\":{},\"node_id\":{},\"log_index\":{}}}",
                raft_group_id.0,
                node_id,
                response.log_id.index()
            ),
        )
            .into_response(),
        Err(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("add raft learner: {err}"),
        )
            .into_response(),
    }
}

pub(crate) async fn allow_raft_node_next_revert(
    State(state): State<HttpState>,
    Path((raft_group_id, node_id)): Path<(u64, u64)>,
) -> Response {
    let Some(registry) = state.raft_registry() else {
        return (
            StatusCode::BAD_REQUEST,
            "raft registry is not configured for this server",
        )
            .into_response();
    };
    let Ok(raft_group_id) = parse_raft_group_id(raft_group_id) else {
        return (StatusCode::BAD_REQUEST, "invalid raft group id").into_response();
    };
    let Some(raft) = registry.get(raft_group_id) else {
        return (StatusCode::NOT_FOUND, "raft group is not registered").into_response();
    };
    match raft.trigger().allow_next_revert(&node_id, true).await {
        Ok(Ok(())) => (
            StatusCode::OK,
            [("content-type", "application/json")],
            format!(
                "{{\"raft_group_id\":{},\"node_id\":{},\"allow_next_revert\":true}}",
                raft_group_id.0, node_id
            ),
        )
            .into_response(),
        Ok(Err(err)) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("allow raft node next revert: {err}"),
        )
            .into_response(),
        Err(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("allow raft node next revert: {err}"),
        )
            .into_response(),
    }
}

pub(crate) async fn transfer_raft_leader(
    State(state): State<HttpState>,
    Path((raft_group_id, node_id)): Path<(u64, u64)>,
) -> Response {
    let Some(registry) = state.raft_registry() else {
        return (
            StatusCode::BAD_REQUEST,
            "raft registry is not configured for this server",
        )
            .into_response();
    };
    let Ok(raft_group_id) = parse_raft_group_id(raft_group_id) else {
        return (StatusCode::BAD_REQUEST, "invalid raft group id").into_response();
    };
    let Some(raft) = registry.get(raft_group_id) else {
        return (StatusCode::NOT_FOUND, "raft group is not registered").into_response();
    };
    let metrics_before = raft.metrics().borrow_watched().clone();
    let current_leader = metrics_before.current_leader;
    let self_id = metrics_before.id;
    if current_leader != Some(self_id) {
        return (
            StatusCode::CONFLICT,
            [("content-type", "application/json")],
            format!(
                "{{\"raft_group_id\":{},\"current_leader\":{},\"transferred\":false,\"reason\":\"not leader\"}}",
                raft_group_id.0,
                optional_u64_json(current_leader)
            ),
        )
            .into_response();
    }
    if node_id == self_id {
        return (
            StatusCode::BAD_REQUEST,
            "target node_id is the current leader",
        )
            .into_response();
    }
    if !metrics_before
        .membership_config
        .voter_ids()
        .any(|voter| voter == node_id)
    {
        return (
            StatusCode::BAD_REQUEST,
            "target node_id is not a voter in this raft group",
        )
            .into_response();
    }
    if let Err(err) = raft.trigger().transfer_leader(node_id).await {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("trigger raft transfer leader: {err}"),
        )
            .into_response();
    }
    (
        StatusCode::OK,
        [("content-type", "application/json")],
        format!(
            "{{\"raft_group_id\":{},\"from\":{},\"to\":{},\"transferred\":true}}",
            raft_group_id.0, self_id, node_id
        ),
    )
        .into_response()
}

pub(crate) fn parse_raft_group_id(raw: u64) -> Result<RaftGroupId, std::num::TryFromIntError> {
    u32::try_from(raw).map(RaftGroupId)
}

pub(crate) fn optional_u64_json(value: Option<u64>) -> String {
    value
        .map(|value| value.to_string())
        .unwrap_or_else(|| "null".to_owned())
}

pub(crate) async fn create_stream(
    State(state): State<HttpState>,
    OriginalUri(uri): OriginalUri,
    Path((bucket, stream)): Path<(String, String)>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let stream_id = BucketStreamId::new(bucket, stream);
    create_stream_by_id(state, request_target(&uri), stream_id, None, headers, body).await
}

pub(crate) async fn create_stream_v1(
    State(state): State<HttpState>,
    OriginalUri(uri): OriginalUri,
    Path(path): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let stream_id = match v1_stream_id(&path) {
        Ok(stream_id) => stream_id,
        Err(response) => return *response,
    };
    create_stream_by_id(
        state,
        request_target(&uri),
        stream_id,
        Some(format!("/v1/stream/{path}")),
        headers,
        body,
    )
    .await
}

pub(crate) async fn create_stream_by_id(
    state: HttpState,
    request_target: String,
    stream_id: BucketStreamId,
    public_path: Option<String>,
    request_headers: HeaderMap,
    body: Bytes,
) -> Response {
    let content_type_explicit = has_content_type(&request_headers);
    let forked_from = match stream_forked_from(&request_headers) {
        Ok(forked_from) => forked_from,
        Err(response) => return *response,
    };
    let fork_offset = match stream_fork_offset(&request_headers) {
        Ok(fork_offset) => fork_offset,
        Err(response) => return *response,
    };
    let mut content_type = request_content_type(&request_headers);
    if let Some(source_id) = forked_from.as_ref()
        && !content_type_explicit
    {
        match state
            .runtime
            .head_stream(HeadStreamRequest {
                stream_id: source_id.clone(),
                now_ms: state.unix_time_ms(),
            })
            .await
        {
            Ok(source) => content_type = source.content_type,
            Err(err) => return runtime_error_response(err),
        }
    }
    let (stream_ttl_seconds, stream_expires_at_ms) = match stream_lifetime(&request_headers) {
        Ok(lifetime) => lifetime,
        Err(response) => return *response,
    };
    let mut request = CreateStreamRequest::new(stream_id.clone(), content_type.clone());
    request.content_type_explicit = content_type_explicit;
    request.now_ms = state.unix_time_ms();
    request.initial_payload = match normalize_http_write_payload(&content_type, body.clone(), true)
    {
        Ok(payload) => payload,
        Err(message) => return (StatusCode::BAD_REQUEST, message).into_response(),
    };
    request.close_after = stream_closed(&request_headers);
    request.stream_seq = stream_seq(&request_headers);
    request.stream_ttl_seconds = stream_ttl_seconds;
    request.stream_expires_at_ms = stream_expires_at_ms;
    request.forked_from = forked_from;
    request.fork_offset = fork_offset;
    let producer = match producer_request(&request_headers) {
        Ok(producer) => producer,
        Err(message) => return (StatusCode::BAD_REQUEST, message).into_response(),
    };
    request.producer = producer.clone();

    if should_externalize_payload(
        &state,
        request.initial_payload.len(),
        request.forked_from.is_none(),
    ) {
        return create_stream_external_by_id(
            state,
            request_target,
            request,
            public_path,
            request_headers,
            producer,
        )
        .await;
    }

    match state.runtime.create_stream(request).await {
        Ok(response) => create_stream_http_response(CreateStreamHttpResponseInput {
            response,
            stream_id: &stream_id,
            content_type: &content_type,
            stream_ttl_seconds,
            stream_expires_at_ms,
            producer: producer.as_ref(),
            public_path: public_path.as_deref(),
            request_headers: &request_headers,
        }),
        Err(err) => runtime_error_or_leader_redirect_async(&state, err, &request_target).await,
    }
}

pub(crate) async fn create_stream_external_by_id(
    state: HttpState,
    request_target: String,
    mut request: CreateStreamRequest,
    public_path: Option<String>,
    request_headers: HeaderMap,
    producer: Option<ProducerRequest>,
) -> Response {
    let stream_id = request.stream_id.clone();
    let content_type = request.content_type.clone();
    let stream_ttl_seconds = request.stream_ttl_seconds;
    let stream_expires_at_ms = request.stream_expires_at_ms;
    let payload = std::mem::take(&mut request.initial_payload);
    let external_payload = match stage_external_payload(&state, &stream_id, &payload).await {
        Ok(payload) => payload,
        Err(response) => return response,
    };
    let external_path = external_payload.s3_path.clone();
    let external_request =
        CreateStreamExternalRequest::from_create_request(request, external_payload);

    match state.runtime.create_stream_external(external_request).await {
        Ok(response) => create_stream_http_response(CreateStreamHttpResponseInput {
            response,
            stream_id: &stream_id,
            content_type: &content_type,
            stream_ttl_seconds,
            stream_expires_at_ms,
            producer: producer.as_ref(),
            public_path: public_path.as_deref(),
            request_headers: &request_headers,
        }),
        Err(err) => {
            cleanup_external_payload(&state, &external_path).await;
            runtime_error_or_leader_redirect_async(&state, err, &request_target).await
        }
    }
}

pub(crate) async fn append_stream(
    State(state): State<HttpState>,
    OriginalUri(uri): OriginalUri,
    Path((bucket, stream)): Path<(String, String)>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let stream_id = BucketStreamId::new(bucket, stream);
    append_stream_by_id(state, request_target(&uri), stream_id, headers, body).await
}

pub(crate) async fn append_stream_v1(
    State(state): State<HttpState>,
    OriginalUri(uri): OriginalUri,
    Path(path): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let stream_id = match v1_stream_id(&path) {
        Ok(stream_id) => stream_id,
        Err(response) => return *response,
    };
    append_stream_by_id(state, request_target(&uri), stream_id, headers, body).await
}

pub(crate) async fn append_stream_by_id(
    state: HttpState,
    request_target: String,
    stream_id: BucketStreamId,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let close_after = stream_closed(&headers);

    if body.is_empty() && close_after {
        let producer = match producer_request(&headers) {
            Ok(producer) => producer,
            Err(message) => return (StatusCode::BAD_REQUEST, message).into_response(),
        };
        return match state
            .runtime
            .close_stream(CloseStreamRequest {
                stream_id,
                stream_seq: stream_seq(&headers),
                producer: producer.clone(),
                now_ms: state.unix_time_ms(),
            })
            .await
        {
            Ok(response) => {
                let mut headers = HeaderMap::new();
                insert_default_response_headers(&mut headers);
                insert_offset(&mut headers, response.next_offset);
                insert_producer_ack(&mut headers, producer.as_ref());
                insert_static(&mut headers, HEADER_STREAM_CLOSED, "true");
                (StatusCode::NO_CONTENT, headers).into_response()
            }
            Err(err) => runtime_error_or_leader_redirect_async(&state, err, &request_target).await,
        };
    }
    if !body.is_empty() && !has_content_type(&headers) {
        return (
            StatusCode::BAD_REQUEST,
            "append with a body must include content type",
        )
            .into_response();
    }

    let content_type = request_content_type(&headers);
    let payload = match normalize_http_write_payload(&content_type, body.clone(), false) {
        Ok(payload) => payload,
        Err(message) => return (StatusCode::BAD_REQUEST, message).into_response(),
    };
    let mut request = AppendRequest::from_bytes(stream_id, payload);
    request.content_type = content_type;
    request.close_after = close_after;
    request.stream_seq = stream_seq(&headers);
    request.now_ms = state.unix_time_ms();
    let producer = match producer_request(&headers) {
        Ok(producer) => producer,
        Err(message) => return (StatusCode::BAD_REQUEST, message).into_response(),
    };
    request.producer = producer.clone();

    if should_externalize_payload(&state, request.payload.len(), true) {
        return append_stream_external_by_id(state, request_target, request).await;
    }

    match state.runtime.append(request).await {
        Ok(response) => append_http_response(response),
        Err(err) => runtime_error_or_leader_redirect_async(&state, err, &request_target).await,
    }
}

pub(crate) async fn append_stream_external_by_id(
    state: HttpState,
    request_target: String,
    mut request: AppendRequest,
) -> Response {
    let stream_id = request.stream_id.clone();
    let payload = std::mem::take(&mut request.payload);
    let external_payload = match stage_external_payload(&state, &stream_id, &payload).await {
        Ok(payload) => payload,
        Err(response) => return response,
    };
    let external_path = external_payload.s3_path.clone();
    let external_request = AppendExternalRequest::from_append_request(request, external_payload);
    match state.runtime.append_external(external_request).await {
        Ok(response) => append_http_response(response),
        Err(err) => {
            cleanup_external_payload(&state, &external_path).await;
            runtime_error_or_leader_redirect_async(&state, err, &request_target).await
        }
    }
}

pub(crate) async fn append_batch(
    State(state): State<HttpState>,
    OriginalUri(uri): OriginalUri,
    Path((bucket, stream)): Path<(String, String)>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    if body.len() > APPEND_BATCH_MAX_BYTES {
        return (StatusCode::PAYLOAD_TOO_LARGE, "append batch is too large").into_response();
    }
    let producer = match producer_request(&headers) {
        Ok(producer) => producer,
        Err(message) => return (StatusCode::BAD_REQUEST, message).into_response(),
    };
    let minimal_ack = prefers_minimal_response(&headers);
    let payloads = match parse_append_batch(&body) {
        Ok(payloads) => payloads,
        Err(message) => return (StatusCode::BAD_REQUEST, message).into_response(),
    };
    if payloads.len() > APPEND_BATCH_MAX_ITEMS {
        return (
            StatusCode::PAYLOAD_TOO_LARGE,
            "append batch contains too many items",
        )
            .into_response();
    }

    let stream_id = BucketStreamId::new(bucket, stream);
    let content_type = request_content_type(&headers);
    let mut request = AppendBatchRequest::new(stream_id, payloads);
    request.content_type = content_type;
    request.producer = producer.clone();
    request.now_ms = state.unix_time_ms();
    let response = match state.runtime.append_batch(request).await {
        Ok(response) => response,
        Err(err) => {
            return runtime_error_or_leader_redirect_async(&state, err, &request_target(&uri))
                .await;
        }
    };

    let mut headers = HeaderMap::new();
    insert_default_response_headers(&mut headers);
    insert_producer_ack(&mut headers, producer.as_ref());
    if minimal_ack && response.items.iter().all(Result::is_ok) {
        return (StatusCode::NO_CONTENT, headers).into_response();
    }

    insert_content_type(&mut headers, "application/json");
    let body = render_batch_results(&response.items);
    (StatusCode::OK, headers, body).into_response()
}

pub(crate) async fn delete_stream(
    State(state): State<HttpState>,
    OriginalUri(uri): OriginalUri,
    Path((bucket, stream)): Path<(String, String)>,
) -> Response {
    let stream_id = BucketStreamId::new(bucket, stream);
    delete_stream_by_id(state, request_target(&uri), stream_id).await
}

pub(crate) async fn delete_stream_v1(
    State(state): State<HttpState>,
    OriginalUri(uri): OriginalUri,
    Path(path): Path<String>,
) -> Response {
    let stream_id = match v1_stream_id(&path) {
        Ok(stream_id) => stream_id,
        Err(response) => return *response,
    };
    delete_stream_by_id(state, request_target(&uri), stream_id).await
}

pub(crate) async fn delete_stream_by_id(
    state: HttpState,
    request_target: String,
    stream_id: BucketStreamId,
) -> Response {
    match state
        .runtime
        .delete_stream(DeleteStreamRequest { stream_id })
        .await
    {
        Ok(_) => {
            let mut headers = HeaderMap::new();
            insert_default_response_headers(&mut headers);
            (StatusCode::NO_CONTENT, headers).into_response()
        }
        Err(err) => runtime_error_or_leader_redirect_async(&state, err, &request_target).await,
    }
}

pub(crate) async fn head_stream(
    State(state): State<HttpState>,
    OriginalUri(uri): OriginalUri,
    Path((bucket, stream)): Path<(String, String)>,
) -> Response {
    let stream_id = BucketStreamId::new(bucket, stream);
    head_stream_by_id(state, request_target(&uri), stream_id).await
}

pub(crate) async fn head_stream_v1(
    State(state): State<HttpState>,
    OriginalUri(uri): OriginalUri,
    Path(path): Path<String>,
) -> Response {
    let stream_id = match v1_stream_id(&path) {
        Ok(stream_id) => stream_id,
        Err(response) => return *response,
    };
    head_stream_by_id(state, request_target(&uri), stream_id).await
}

pub(crate) async fn head_stream_by_id(
    state: HttpState,
    request_target: String,
    stream_id: BucketStreamId,
) -> Response {
    match state
        .runtime
        .head_stream(HeadStreamRequest {
            stream_id,
            now_ms: state.unix_time_ms(),
        })
        .await
    {
        Ok(response) => {
            let mut headers = HeaderMap::new();
            insert_default_response_headers(&mut headers);
            insert_content_type(&mut headers, &response.content_type);
            insert_offset(&mut headers, response.tail_offset);
            insert_u64_header(
                &mut headers,
                HEADER_STREAM_COLD_HOT_START_OFFSET,
                response.cold_hot_start_offset,
            );
            insert_static(&mut headers, HEADER_STREAM_UP_TO_DATE, "true");
            insert_cache_control(&mut headers, "no-store");
            insert_lifetime_headers(
                &mut headers,
                response.stream_ttl_seconds,
                response.stream_expires_at_ms,
            );
            insert_header_str(
                &mut headers,
                HEADER_STREAM_INTEGRITY_LIVE_SETSUM,
                &response.integrity.live_setsum,
            );
            insert_header_str(
                &mut headers,
                HEADER_STREAM_INTEGRITY_EVICTED_SETSUM,
                &response.integrity.evicted_setsum,
            );
            insert_header_str(
                &mut headers,
                HEADER_STREAM_INTEGRITY_TOTAL_SETSUM,
                &response.integrity.total_setsum,
            );
            insert_u64_header(
                &mut headers,
                HEADER_STREAM_INTEGRITY_LIVE_START_OFFSET,
                response.integrity.live_start_offset,
            );
            insert_u64_header(
                &mut headers,
                HEADER_STREAM_INTEGRITY_LIVE_RECORDS,
                response.integrity.live_records,
            );
            insert_u64_header(
                &mut headers,
                HEADER_STREAM_INTEGRITY_EVICTED_RECORDS,
                response.integrity.evicted_records,
            );
            insert_u64_header(
                &mut headers,
                HEADER_STREAM_INTEGRITY_TOTAL_RECORDS,
                response.integrity.total_records,
            );
            if let Some(snapshot_offset) = response.snapshot_offset {
                insert_snapshot_offset(&mut headers, snapshot_offset);
            }
            if response.closed {
                insert_static(&mut headers, HEADER_STREAM_CLOSED, "true");
            }
            (StatusCode::OK, headers).into_response()
        }
        Err(err) => runtime_error_or_leader_redirect_async(&state, err, &request_target).await,
    }
}

pub(crate) async fn read_stream(
    State(state): State<HttpState>,
    OriginalUri(uri): OriginalUri,
    Path((bucket, stream)): Path<(String, String)>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> Response {
    let stream_id = BucketStreamId::new(bucket, stream);
    read_stream_by_id(state, request_target(&uri), stream_id, headers, raw_query).await
}

pub(crate) async fn read_stream_v1(
    State(state): State<HttpState>,
    OriginalUri(uri): OriginalUri,
    Path(path): Path<String>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> Response {
    let stream_id = match v1_stream_id(&path) {
        Ok(stream_id) => stream_id,
        Err(response) => return *response,
    };
    read_stream_by_id(state, request_target(&uri), stream_id, headers, raw_query).await
}

pub(crate) async fn read_stream_by_id(
    state: HttpState,
    request_target: String,
    stream_id: BucketStreamId,
    headers: HeaderMap,
    raw_query: Option<String>,
) -> Response {
    let query = match parse_query(raw_query.as_deref()) {
        Ok(query) => query,
        Err(response) => return *response,
    };
    let live_mode = query.get("live").map(String::as_str);
    let offset_is_now = query.get("offset").is_some_and(|offset| offset == "now");
    if live_mode.is_some() && !query.contains_key("offset") {
        return (StatusCode::BAD_REQUEST, "live reads require offset").into_response();
    }
    if matches!(live_mode, Some("sse" | "long-poll"))
        && let Err(err) = state
            .runtime
            .require_local_live_read_owner(&stream_id)
            .await
    {
        return runtime_error_or_leader_redirect_async(&state, err, &request_target).await;
    }
    let offset = match read_offset(
        &state,
        &stream_id,
        query.get("offset").map(String::as_str),
        &request_target,
    )
    .await
    {
        Ok(offset) => offset,
        Err(response) => return *response,
    };
    let max_len = query
        .get("max_bytes")
        .and_then(|raw| raw.parse::<usize>().ok())
        .unwrap_or(usize::MAX);

    match live_mode {
        Some("sse") => {
            return sse_stream(state, request_target, stream_id, offset, max_len, &query).await;
        }
        Some("long-poll") => {
            return long_poll_stream(
                state,
                request_target,
                stream_id,
                offset,
                max_len,
                &query,
                headers,
            )
            .await;
        }
        Some(_) => return (StatusCode::BAD_REQUEST, "invalid live mode").into_response(),
        None => {}
    }

    match state
        .runtime
        .read_stream(ReadStreamRequest {
            stream_id,
            offset,
            max_len,
            now_ms: state.unix_time_ms(),
        })
        .await
    {
        Ok(response) if offset_is_now => offset_now_response(response),
        Ok(response) => read_response(response, &headers, None),
        Err(err) => runtime_error_or_leader_redirect_async(&state, err, &request_target).await,
    }
}

pub(crate) async fn publish_snapshot(
    State(state): State<HttpState>,
    OriginalUri(uri): OriginalUri,
    Path((bucket, stream, snapshot_offset)): Path<(String, String, String)>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let snapshot_offset = match parse_snapshot_offset(&snapshot_offset) {
        Ok(offset) => offset,
        Err(response) => return *response,
    };
    let stream_id = BucketStreamId::new(bucket, stream);
    let request = PublishSnapshotRequest {
        stream_id,
        snapshot_offset,
        content_type: request_content_type(&headers),
        payload: body.clone(),
        now_ms: state.unix_time_ms(),
    };
    match state.runtime.publish_snapshot(request).await {
        Ok(response) => {
            let mut headers = HeaderMap::new();
            insert_default_response_headers(&mut headers);
            insert_snapshot_offset(&mut headers, response.snapshot_offset);
            (StatusCode::NO_CONTENT, headers).into_response()
        }
        Err(err) => {
            runtime_error_or_leader_redirect_async(&state, err, &request_target(&uri)).await
        }
    }
}

pub(crate) async fn read_latest_snapshot(
    State(state): State<HttpState>,
    OriginalUri(uri): OriginalUri,
    Path((bucket, stream)): Path<(String, String)>,
    headers: HeaderMap,
) -> Response {
    let stream_id = BucketStreamId::new(bucket.clone(), stream.clone());
    let head = match state
        .runtime
        .head_stream(HeadStreamRequest {
            stream_id,
            now_ms: state.unix_time_ms(),
        })
        .await
    {
        Ok(head) => head,
        Err(err) => {
            return runtime_error_or_leader_redirect_async(&state, err, &request_target(&uri))
                .await;
        }
    };
    let Some(snapshot_offset) = head.snapshot_offset else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let mut response_headers = HeaderMap::new();
    insert_default_response_headers(&mut response_headers);
    insert_snapshot_offset(&mut response_headers, snapshot_offset);
    let path = format!("/{bucket}/{stream}/snapshot/{snapshot_offset:020}");
    insert_public_location(&mut response_headers, &headers, &path);
    (StatusCode::TEMPORARY_REDIRECT, response_headers).into_response()
}

pub(crate) async fn read_snapshot(
    State(state): State<HttpState>,
    OriginalUri(uri): OriginalUri,
    Path((bucket, stream, snapshot_offset)): Path<(String, String, String)>,
) -> Response {
    let snapshot_offset = match parse_snapshot_offset(&snapshot_offset) {
        Ok(offset) => offset,
        Err(response) => return *response,
    };
    let stream_id = BucketStreamId::new(bucket, stream);
    match state
        .runtime
        .read_snapshot(ReadSnapshotRequest {
            stream_id,
            snapshot_offset: Some(snapshot_offset),
            now_ms: state.unix_time_ms(),
        })
        .await
    {
        Ok(response) => snapshot_response(response),
        Err(err) => {
            runtime_error_or_leader_redirect_async(&state, err, &request_target(&uri)).await
        }
    }
}

pub(crate) async fn delete_snapshot(
    State(state): State<HttpState>,
    OriginalUri(uri): OriginalUri,
    Path((bucket, stream, snapshot_offset)): Path<(String, String, String)>,
) -> Response {
    let snapshot_offset = match parse_snapshot_offset(&snapshot_offset) {
        Ok(offset) => offset,
        Err(response) => return *response,
    };
    let stream_id = BucketStreamId::new(bucket, stream);
    match state
        .runtime
        .delete_snapshot(DeleteSnapshotRequest {
            stream_id,
            snapshot_offset,
            now_ms: state.unix_time_ms(),
        })
        .await
    {
        Ok(()) => {
            let mut headers = HeaderMap::new();
            insert_default_response_headers(&mut headers);
            (StatusCode::NO_CONTENT, headers).into_response()
        }
        Err(err) => {
            runtime_error_or_leader_redirect_async(&state, err, &request_target(&uri)).await
        }
    }
}

pub(crate) async fn bootstrap_stream(
    State(state): State<HttpState>,
    OriginalUri(uri): OriginalUri,
    Path((bucket, stream)): Path<(String, String)>,
    RawQuery(raw_query): RawQuery,
) -> Response {
    let query = match parse_query(raw_query.as_deref()) {
        Ok(query) => query,
        Err(response) => return *response,
    };
    if query.contains_key("live") {
        return (
            StatusCode::BAD_REQUEST,
            "bootstrap does not support live reads",
        )
            .into_response();
    }
    let stream_id = BucketStreamId::new(bucket, stream);
    match state
        .runtime
        .bootstrap_stream(BootstrapStreamRequest {
            stream_id,
            now_ms: state.unix_time_ms(),
        })
        .await
    {
        Ok(response) => bootstrap_response(response),
        Err(err) => {
            runtime_error_or_leader_redirect_async(&state, err, &request_target(&uri)).await
        }
    }
}

fn parse_snapshot_offset(raw: &str) -> Result<u64, BoxResponse> {
    if raw == "-1" {
        return Err(Box::new(
            (StatusCode::BAD_REQUEST, "invalid snapshot offset").into_response(),
        ));
    }
    raw.parse::<u64>()
        .map_err(|_| Box::new((StatusCode::BAD_REQUEST, "invalid snapshot offset").into_response()))
}

pub(crate) async fn read_offset(
    state: &HttpState,
    stream_id: &BucketStreamId,
    raw: Option<&str>,
    request_target: &str,
) -> Result<u64, BoxResponse> {
    match raw {
        Some("-1") => Ok(0),
        Some("now") => match state
            .runtime
            .head_stream(HeadStreamRequest {
                stream_id: stream_id.clone(),
                now_ms: state.unix_time_ms(),
            })
            .await
        {
            Ok(head) => Ok(head.tail_offset),
            Err(err) => {
                let response =
                    runtime_error_or_leader_redirect_async(state, err, request_target).await;
                Err(Box::new(response))
            }
        },
        Some(raw) => raw
            .parse::<u64>()
            .map_err(|_| Box::new((StatusCode::BAD_REQUEST, "invalid offset").into_response())),
        None => Ok(0),
    }
}

pub(crate) async fn long_poll_stream(
    state: HttpState,
    request_target: String,
    stream_id: BucketStreamId,
    offset: u64,
    max_len: usize,
    query: &HashMap<String, String>,
    headers: HeaderMap,
) -> Response {
    let timeout_ms = long_poll_timeout_ms(query);
    let read = state.runtime.wait_read_stream(ReadStreamRequest {
        stream_id: stream_id.clone(),
        offset,
        max_len: max_len.max(1),
        now_ms: state.unix_time_ms(),
    });
    match http_time::timeout(Duration::from_millis(timeout_ms), read).await {
        Ok(Ok(response)) if response.payload.is_empty() && response.up_to_date => {
            long_poll_no_content_response(&response, query.get("cursor").map(String::as_str))
        }
        Ok(Ok(response)) => read_response(
            response,
            &headers,
            Some(query.get("cursor").map(String::as_str).unwrap_or("")),
        ),
        Ok(Err(err)) => runtime_error_or_leader_redirect_async(&state, err, &request_target).await,
        Err(_) => match state
            .runtime
            .head_stream(HeadStreamRequest {
                stream_id: stream_id.clone(),
                now_ms: state.unix_time_ms(),
            })
            .await
        {
            Ok(head) => {
                let mut headers = HeaderMap::new();
                insert_default_response_headers(&mut headers);
                insert_offset(&mut headers, head.tail_offset);
                insert_static(&mut headers, HEADER_STREAM_UP_TO_DATE, "true");
                if head.closed {
                    insert_static(&mut headers, HEADER_STREAM_CLOSED, "true");
                } else {
                    insert_cursor(
                        &mut headers,
                        response_cursor(head.tail_offset, query.get("cursor").map(String::as_str)),
                    );
                }
                (StatusCode::NO_CONTENT, headers).into_response()
            }
            Err(err) => runtime_error_or_leader_redirect_async(&state, err, &request_target).await,
        },
    }
}

#[derive(Clone)]
struct SseState {
    runtime: ShardRuntime,
    http_metrics: Arc<HttpMetrics>,
    wall_clock: Arc<dyn WallClock>,
    stream_id: BucketStreamId,
    offset: u64,
    max_len: usize,
    encode_base64: bool,
    cursor: Option<String>,
    initial_read: bool,
}

pub(crate) async fn sse_stream(
    state: HttpState,
    request_target: String,
    stream_id: BucketStreamId,
    offset: u64,
    max_len: usize,
    query: &HashMap<String, String>,
) -> Response {
    let head = match state
        .runtime
        .head_stream(HeadStreamRequest {
            stream_id: stream_id.clone(),
            now_ms: state.unix_time_ms(),
        })
        .await
    {
        Ok(head) => head,
        Err(err) => {
            return runtime_error_or_leader_redirect_async(&state, err, &request_target).await;
        }
    };

    let encode_base64 = should_base64_encode_sse_data(&head.content_type);
    state
        .http_metrics
        .sse_streams_opened
        .fetch_add(1, Ordering::Relaxed);
    let sse_state = SseState {
        runtime: state.runtime,
        http_metrics: state.http_metrics,
        wall_clock: state.wall_clock,
        stream_id,
        offset,
        max_len: max_len.max(1),
        encode_base64,
        cursor: query.get("cursor").cloned(),
        initial_read: true,
    };
    let body_stream = stream::unfold(Some(sse_state), |state| async move {
        let mut state = match state {
            Some(state) => state,
            None => return None,
        };
        state
            .http_metrics
            .sse_read_iterations
            .fetch_add(1, Ordering::Relaxed);
        let read_request = ReadStreamRequest {
            stream_id: state.stream_id.clone(),
            offset: state.offset,
            max_len: state.max_len,
            now_ms: state.wall_clock.unix_time_ms(),
        };
        let read = if state.initial_read {
            state.initial_read = false;
            state.runtime.read_stream(read_request).await
        } else {
            state.runtime.wait_read_stream(read_request).await
        };
        let read = match read {
            Ok(read) => read,
            Err(err) => {
                state
                    .http_metrics
                    .sse_error_events
                    .fetch_add(1, Ordering::Relaxed);
                let event = format!("event: error\ndata:{}\n\n", sse_safe_line(&err.to_string()));
                return Some((Ok::<Bytes, Infallible>(Bytes::from(event)), None));
            }
        };

        state.offset = read.next_offset;
        let done = read.closed && read.up_to_date;
        if !read.payload.is_empty() {
            state
                .http_metrics
                .sse_data_events
                .fetch_add(1, Ordering::Relaxed);
        }
        state
            .http_metrics
            .sse_control_events
            .fetch_add(1, Ordering::Relaxed);
        let event = render_sse_read(&read, state.encode_base64, state.cursor.as_deref());
        let next = if done { None } else { Some(state) };
        Some((Ok::<Bytes, Infallible>(Bytes::from(event)), next))
    });

    let mut headers = HeaderMap::new();
    insert_default_response_headers(&mut headers);
    insert_content_type(&mut headers, "text/event-stream");
    insert_cache_control(&mut headers, "no-cache");
    if encode_base64 {
        insert_static(&mut headers, HEADER_STREAM_SSE_DATA_ENCODING, "base64");
    }
    (StatusCode::OK, headers, Body::from_stream(body_stream)).into_response()
}

pub(crate) fn long_poll_timeout_ms(query: &HashMap<String, String>) -> u64 {
    query
        .get("timeout_ms")
        .and_then(|raw| raw.parse::<u64>().ok())
        .unwrap_or(DEFAULT_LONG_POLL_TIMEOUT_MS)
        .clamp(1, MAX_LONG_POLL_TIMEOUT_MS)
}

#[cfg(not(madsim))]
pub(crate) fn unix_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| u64::try_from(duration.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}

#[cfg(madsim)]
pub(crate) fn unix_time_ms() -> u64 {
    panic!(
        "unix_time_ms() / SystemWallClock is non-deterministic under cfg(madsim); \
         inject a deterministic WallClock via HttpState::with_wall_clock (or _handle)"
    );
}

pub(crate) fn v1_stream_id(path: &str) -> Result<BucketStreamId, BoxResponse> {
    if path.is_empty() {
        return Err(Box::new(
            (StatusCode::BAD_REQUEST, "stream path must not be empty").into_response(),
        ));
    }
    if path.contains('\0')
        || path
            .split('/')
            .any(|segment| segment == ".." || segment.is_empty())
    {
        return Err(Box::new(
            (
                StatusCode::BAD_REQUEST,
                "stream path contains invalid characters",
            )
                .into_response(),
        ));
    }
    let (bucket, stream) = path.split_once('/').unwrap_or((V1_DEFAULT_BUCKET, path));
    Ok(BucketStreamId::new(bucket, stream))
}

pub(crate) fn parse_query(raw: Option<&str>) -> Result<HashMap<String, String>, BoxResponse> {
    let mut query = HashMap::new();
    let Some(raw) = raw else {
        return Ok(query);
    };
    for pair in raw.split('&') {
        if pair.is_empty() {
            continue;
        }
        let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
        if key == "offset" && query.contains_key("offset") {
            return Err(Box::new(
                (StatusCode::BAD_REQUEST, "multiple offset parameters").into_response(),
            ));
        }
        query.insert(key.to_owned(), value.to_owned());
    }
    Ok(query)
}

pub(crate) fn request_content_type(headers: &HeaderMap) -> String {
    headers
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .filter(|value| !value.trim().is_empty())
        .map(normalize_content_type)
        .unwrap_or_else(|| DEFAULT_CONTENT_TYPE.to_owned())
}

pub(crate) fn stream_forked_from(
    headers: &HeaderMap,
) -> Result<Option<BucketStreamId>, BoxResponse> {
    let Some(raw) = header_value(headers, HEADER_STREAM_FORKED_FROM) else {
        return Ok(None);
    };
    let path = raw
        .strip_prefix("/v1/stream/")
        .or_else(|| raw.strip_prefix("v1/stream/"))
        .unwrap_or(raw)
        .trim_start_matches('/');
    v1_stream_id(path).map(Some).map_err(|_| {
        Box::new((StatusCode::BAD_REQUEST, "invalid stream-forked-from").into_response())
    })
}

pub(crate) fn stream_fork_offset(headers: &HeaderMap) -> Result<Option<u64>, BoxResponse> {
    let Some(raw) = header_value(headers, HEADER_STREAM_FORK_OFFSET) else {
        return Ok(None);
    };
    let normalized = raw.replace('_', "");
    normalized.parse::<u64>().map(Some).map_err(|_| {
        Box::new((StatusCode::BAD_REQUEST, "invalid stream-fork-offset").into_response())
    })
}

pub(crate) fn has_content_type(headers: &HeaderMap) -> bool {
    headers
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| !value.trim().is_empty())
}

pub(crate) fn normalize_content_type(value: &str) -> String {
    value
        .split(';')
        .map(str::trim)
        .filter(|part| !part.is_empty())
        .map(str::to_ascii_lowercase)
        .collect::<Vec<_>>()
        .join("; ")
}

pub(crate) fn stream_lifetime(
    headers: &HeaderMap,
) -> Result<(Option<u64>, Option<u64>), BoxResponse> {
    let ttl = header_value(headers, HEADER_STREAM_TTL)
        .map(parse_stream_ttl)
        .transpose()
        .map_err(|message| Box::new((StatusCode::BAD_REQUEST, message).into_response()))?;
    let expires_at = header_value(headers, HEADER_STREAM_EXPIRES_AT)
        .map(parse_stream_expires_at)
        .transpose()
        .map_err(|message| Box::new((StatusCode::BAD_REQUEST, message).into_response()))?;
    if ttl.is_some() && expires_at.is_some() {
        return Err(Box::new(
            (
                StatusCode::BAD_REQUEST,
                "stream-ttl and stream-expires-at cannot be provided together",
            )
                .into_response(),
        ));
    }
    Ok((ttl, expires_at))
}

pub(crate) fn parse_stream_ttl(raw: &str) -> Result<u64, String> {
    if raw.is_empty() {
        return Err("stream-ttl must not be empty".to_owned());
    }
    if raw.len() > 1 && raw.starts_with('0') {
        return Err("stream-ttl must not contain leading zeros".to_owned());
    }
    if !raw.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err("stream-ttl must be a non-negative decimal integer".to_owned());
    }
    raw.parse::<u64>()
        .map_err(|_| "stream-ttl is too large".to_owned())
}

pub(crate) fn parse_stream_expires_at(raw: &str) -> Result<u64, String> {
    let expires_at = DateTime::parse_from_rfc3339(raw)
        .map_err(|_| "stream-expires-at must be an RFC3339 timestamp".to_owned())?;
    u64::try_from(expires_at.timestamp_millis())
        .map_err(|_| "stream-expires-at must not be before the Unix epoch".to_owned())
}

pub(crate) fn stream_closed(headers: &HeaderMap) -> bool {
    headers
        .get(HEADER_STREAM_CLOSED)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.eq_ignore_ascii_case("true"))
}

pub(crate) fn stream_seq(headers: &HeaderMap) -> Option<String> {
    headers
        .get(HEADER_STREAM_SEQ)
        .and_then(|value| value.to_str().ok())
        .filter(|value| !value.trim().is_empty())
        .map(str::to_owned)
}

pub(crate) fn producer_request(headers: &HeaderMap) -> Result<Option<ProducerRequest>, String> {
    let producer_id = header_value(headers, HEADER_PRODUCER_ID);
    let producer_epoch = header_value(headers, HEADER_PRODUCER_EPOCH);
    let producer_seq = header_value(headers, HEADER_PRODUCER_SEQ);
    let present = [
        producer_id.is_some(),
        producer_epoch.is_some(),
        producer_seq.is_some(),
    ];
    if present.iter().all(|value| !*value) {
        return Ok(None);
    }
    if !present.iter().all(|value| *value) {
        return Err(
            "producer-id, producer-epoch, and producer-seq must be provided together".to_owned(),
        );
    }

    let producer_id = producer_id.expect("checked present");
    if producer_id.trim().is_empty() {
        return Err("producer-id must not be empty".to_owned());
    }
    Ok(Some(ProducerRequest {
        producer_id: producer_id.to_owned(),
        producer_epoch: parse_producer_integer(
            HEADER_PRODUCER_EPOCH,
            producer_epoch.expect("checked present"),
        )?,
        producer_seq: parse_producer_integer(
            HEADER_PRODUCER_SEQ,
            producer_seq.expect("checked present"),
        )?,
    }))
}

pub(crate) fn prefers_minimal_response(headers: &HeaderMap) -> bool {
    headers
        .get(HEADER_PREFER)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| {
            value
                .split(',')
                .any(|part| part.trim().eq_ignore_ascii_case("return=minimal"))
        })
}

pub(crate) fn header_value<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
}

pub(crate) fn parse_producer_integer(name: &str, raw: &str) -> Result<u64, String> {
    const MAX_JS_SAFE_INTEGER: u64 = 9_007_199_254_740_991;
    let value = raw
        .parse::<u64>()
        .map_err(|_| format!("{name} must be a non-negative integer"))?;
    if value > MAX_JS_SAFE_INTEGER {
        return Err(format!("{name} must be <= {MAX_JS_SAFE_INTEGER}"));
    }
    Ok(value)
}

fn runtime_error_response(err: RuntimeError) -> Response {
    let status = runtime_error_status(&err);
    let mut headers = HeaderMap::new();
    insert_default_response_headers(&mut headers);
    insert_producer_error_headers(&mut headers, &err);
    insert_stream_error_headers(&mut headers, &err);
    insert_stream_error_offset(&mut headers, &err);
    (status, headers, err.to_string()).into_response()
}

pub(crate) async fn runtime_error_or_leader_redirect_async(
    state: &HttpState,
    err: RuntimeError,
    request_target: &str,
) -> Response {
    let Some(router) = state.client_write_router() else {
        return runtime_error_response(err);
    };
    // gRPC and the client API share one listener, so the leader's address is
    // a valid redirect target for reads and writes alike. 307 preserves the
    // method and body, so a forwarded POST/PUT re-runs as a write on the
    // leader. Writes go through the leader's raft client_write exactly as a
    // local write would; redirecting only moves the leader hop to the client.
    if let Some(redirect) = router.redirect_response(&err, request_target) {
        return redirect;
    }
    // Forward-to-leader error whose leader is currently unknown (election in
    // progress): tell the client to retry rather than failing hard.
    if is_forward_to_leader(&err) {
        return leader_unknown_retry_response(err);
    }
    runtime_error_response(err)
}

/// True when `err` is a group-engine error asking the caller to forward to the
/// leader (carries a leader hint), regardless of whether the leader is yet
/// known.
fn is_forward_to_leader(err: &RuntimeError) -> bool {
    matches!(
        err,
        RuntimeError::GroupEngine {
            leader_hint: Some(_),
            ..
        }
    )
}

/// 503 + `Retry-After: 1` for a write that hit a non-leader while the group has
/// no known leader. Retryable: a new leader should be elected shortly.
fn leader_unknown_retry_response(err: RuntimeError) -> Response {
    let mut headers = HeaderMap::new();
    insert_default_response_headers(&mut headers);
    headers.insert(
        axum::http::header::RETRY_AFTER,
        HeaderValue::from_static("1"),
    );
    (StatusCode::SERVICE_UNAVAILABLE, headers, err.to_string()).into_response()
}

fn request_target(uri: &Uri) -> String {
    uri.path_and_query()
        .map(|path_and_query| path_and_query.as_str().to_owned())
        .unwrap_or_else(|| uri.path().to_owned())
}

#[cfg(test)]
mod tests;

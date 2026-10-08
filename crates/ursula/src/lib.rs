//! Ursula HTTP server: axum router, request handlers, response rendering,
//! plus the typed-config bootstrap constructors used by the `ursula` binary.
//!
//! Module map:
//!
//! - [`admin_fence`]: process-local executor ordering for administrative mutations.
//! - [`json_text`]: JSON Message Text (P1): validation, flattening and lexical
//!   minification of `application/json` write bodies.
//! - [`render`]: response builders, header helpers, SSE/multipart rendering.
//! - [`bootstrap`]: typed-config `spawn_*_runtime` constructors and cold-flush worker.
//! - [`server`]: command arguments and the long-running server service entrypoint.

mod admin_fence;
mod bootstrap;
mod cold_snapshot;
pub mod json_text;
mod otel_metrics;
pub mod server;
mod http_time {
    #[cfg(madsim)]
    pub use madsim::time::timeout;
    #[cfg(not(madsim))]
    pub use tokio::time::timeout;
}
mod removed_surface;
mod render;
mod wal_disk;

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::convert::Infallible;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::Duration;
#[cfg(not(madsim))]
use std::time::SystemTime;
#[cfg(not(madsim))]
use std::time::UNIX_EPOCH;

use axum::Json;
use axum::Router;
use axum::body::Body;
use axum::body::Bytes;
use axum::body::HttpBody;
use axum::extract::DefaultBodyLimit;
use axum::extract::OriginalUri;
use axum::extract::Path;
use axum::extract::RawQuery;
use axum::extract::State;
use axum::http::HeaderMap;
use axum::http::HeaderValue;
use axum::http::Method;
use axum::http::Request;
use axum::http::StatusCode;
use axum::http::Uri;
use axum::http::Version;
#[cfg(feature = "jemalloc-prof")]
use axum::http::header::CONTENT_DISPOSITION;
use axum::http::header::CONTENT_LENGTH;
use axum::http::header::CONTENT_TYPE;
use axum::http::header::LOCATION;
use axum::middleware::Next;
use axum::middleware::{self};
use axum::response::IntoResponse;
use axum::response::Response;
use axum::routing::get;
use axum::routing::post;
use axum::routing::put;
pub use bootstrap::Persistence;
pub use bootstrap::SpawnRuntimeError;
pub use bootstrap::SpawnedRuntime;
pub use bootstrap::Topology;
pub use bootstrap::spawn_runtime;
use chrono::DateTime;
use futures_util::stream;
use openraft::BasicNode;
use openraft::rt::WatchReceiver;
use tower_http::compression::CompressionLayer;
use tower_http::compression::CompressionLevel;
use tower_http::compression::predicate::Predicate;
use tower_http::compression::predicate::SizeAbove;
use ursula_proto::admin::MAINTENANCE_FENCE_HEADER;
use ursula_proto::admin::MaintenanceFence;
use ursula_proto::admin::PROCESS_INCARNATION_HEADER;
use ursula_proto::admin::ProcessIncarnation;
use ursula_raft::LeadershipShedReason;
use ursula_raft::OwnerRaftHandle;
use ursula_raft::RAFT_GRPC_APPEND_PATH;
use ursula_raft::RAFT_GRPC_APPEND_STREAM_PATH;
use ursula_raft::RAFT_GRPC_FULL_SNAPSHOT_PATH;
use ursula_raft::RAFT_GRPC_GROUP_READ_PATH;
use ursula_raft::RAFT_GRPC_GROUP_WRITE_PATH;
use ursula_raft::RAFT_GRPC_MAX_MESSAGE_BYTES;
use ursula_raft::RAFT_GRPC_REJOIN_BARRIER_PATH;
use ursula_raft::RAFT_GRPC_TRANSFER_LEADER_PATH;
use ursula_raft::RAFT_GRPC_VOTE_PATH;
use ursula_raft::RaftGroupHandleRegistry;
use ursula_raft::RaftGrpcService;
use ursula_raft::raft_internal_proto;
use ursula_runtime::AdvanceRetentionRequest;
use ursula_runtime::AppendExternalRequest;
use ursula_runtime::AppendRequest;
use ursula_runtime::AppendResponse;
use ursula_runtime::BootstrapStreamRequest;
use ursula_runtime::CloseStreamRequest;
use ursula_runtime::CreateStreamExternalRequest;
use ursula_runtime::CreateStreamRequest;
use ursula_runtime::CreateStreamResponse;
use ursula_runtime::DeleteStreamRequest;
use ursula_runtime::ErrorStatus;
use ursula_runtime::ExternalPayloadRef;
use ursula_runtime::GroupEngineError;
use ursula_runtime::HeadStreamRequest;
use ursula_runtime::ImportGroupStateRequest;
use ursula_runtime::LiveReadOwner;
use ursula_runtime::PlanColdFlushRequest;
use ursula_runtime::ProducerRequest;
use ursula_runtime::PublishSnapshotRequest;
use ursula_runtime::ReadSnapshotRequest;
use ursula_runtime::ReadStreamRequest;
use ursula_runtime::RuntimeError;
use ursula_runtime::ShardRuntime;
use ursula_runtime::StreamErrorCode;
use ursula_runtime::StreamErrorContext;
use ursula_runtime::new_external_payload_path;
use ursula_shard::BucketStreamId;
use ursula_shard::RaftGroupId;
use wal_disk::WalDiskMonitor;

use crate::render::bootstrap_response;
use crate::render::clamp_sse_text_read;
use crate::render::http_read_content_type;
use crate::render::insert_cache_control;
use crate::render::insert_content_type;
use crate::render::insert_cursor;
use crate::render::insert_default_response_headers;
use crate::render::insert_header_str;
use crate::render::insert_incarnation;
use crate::render::insert_lifetime_headers;
use crate::render::insert_location;
use crate::render::insert_offset;
use crate::render::insert_padded_offset;
use crate::render::insert_producer_ack;
use crate::render::insert_producer_error_headers;
use crate::render::insert_snapshot_digest;
use crate::render::insert_snapshot_offset;
use crate::render::insert_static;
use crate::render::insert_stream_error_headers;
use crate::render::insert_stream_error_offset;
use crate::render::insert_u64_header;
use crate::render::long_poll_no_content_response;
use crate::render::normalize_http_write_payload;
use crate::render::offset_now_response;
use crate::render::read_response;
use crate::render::render_metrics;
use crate::render::render_sse_read;
use crate::render::response_cursor;
use crate::render::runtime_error_status;
use crate::render::should_base64_encode_sse_data;
use crate::render::snapshot_response;
use crate::render::sse_safe_line;

type BoxResponse = Box<Response>;

const DEFAULT_CONTENT_TYPE: &str = "application/octet-stream";
const HEADER_STREAM_CLOSED: &str = "stream-closed";
const HEADER_STREAM_CURSOR: &str = "stream-cursor";
const HEADER_STREAM_EXPIRES_AT: &str = "stream-expires-at";
const HEADER_STREAM_INCARNATION: &str = "stream-incarnation";
const HEADER_STREAM_COLD_HOT_START_OFFSET: &str = "stream-cold-hot-start-offset";
const HEADER_STREAM_DATA_CONTENT_TYPE: &str = "stream-data-content-type";
const HEADER_STREAM_NEXT_OFFSET: &str = "stream-next-offset";
const HEADER_STREAM_SNAPSHOT_OFFSET: &str = "stream-snapshot-offset";
const HEADER_STREAM_SNAPSHOT_DIGEST: &str = "stream-snapshot-digest";
const HEADER_STREAM_RETAINED_OFFSET: &str = "stream-retained-offset";
const HEADER_STREAM_SSE_DATA_ENCODING: &str = "stream-sse-data-encoding";
const HEADER_STREAM_SEQ: &str = "stream-seq";
const HEADER_STREAM_TTL: &str = "stream-ttl";
const HEADER_STREAM_UP_TO_DATE: &str = "stream-up-to-date";
const HEADER_PRODUCER_ID: &str = "producer-id";
const HEADER_PRODUCER_EPOCH: &str = "producer-epoch";
const HEADER_PRODUCER_SEQ: &str = "producer-seq";
const HEADER_X_CONTENT_TYPE_OPTIONS: &str = "x-content-type-options";
const HEADER_CROSS_ORIGIN_RESOURCE_POLICY: &str = "cross-origin-resource-policy";
const HEADER_URSULA_RAFT_LEADER_ID: &str = "x-ursula-raft-leader-id";
#[cfg(feature = "jemalloc-prof")]
const HEADER_URSULA_DEBUG_TOKEN: &str = "x-ursula-debug-token";
// tikv-jemalloc-sys forces the `_rjem_` symbol prefix on Apple targets, so
// jemalloc reads `_RJEM_MALLOC_CONF` there instead of `MALLOC_CONF`.
#[cfg(feature = "jemalloc-prof")]
const MALLOC_CONF_ENV_VAR: &str = if cfg!(target_vendor = "apple") {
    "_RJEM_MALLOC_CONF"
} else {
    "MALLOC_CONF"
};
const MAX_HTTP_BODY_BYTES: usize = 32 * 1024 * 1024;
/// Server-side cap on one read response (bounded-stream-state F11), the same
/// 8 MiB that caps bootstrap updates. A request's `max_bytes` is clamped to
/// it; a capped response is partial (`Stream-Up-To-Date` absent), may end
/// inside a message of any content type, and the client continues from
/// `Stream-Next-Offset`.
const READ_MAX_RESPONSE_BYTES: usize = 8 * 1024 * 1024;
const DEFAULT_HTTP_INFLIGHT_BODY_BYTES: usize = MAX_HTTP_BODY_BYTES * 8;
const DEFAULT_LONG_POLL_TIMEOUT_MS: u64 = 1_000;
const MAX_LONG_POLL_TIMEOUT_MS: u64 = 60_000;

#[derive(Debug, serde::Deserialize)]
pub(crate) struct StreamPath {
    bucket: String,
    stream: String,
}

impl StreamPath {
    fn into_stream_id(self) -> BucketStreamId {
        BucketStreamId::new(self.bucket, self.stream)
    }
}

#[derive(Debug, serde::Deserialize)]
pub(crate) struct SnapshotPath {
    bucket: String,
    stream: String,
    snapshot_offset: String,
}

impl SnapshotPath {
    fn into_parts(self) -> (BucketStreamId, String) {
        let stream_id = BucketStreamId::new(self.bucket, self.stream);
        (stream_id, self.snapshot_offset)
    }
}

#[derive(Debug, serde::Deserialize)]
pub(crate) struct RetentionPath {
    bucket: String,
    stream: String,
    retained_offset: String,
}

impl RetentionPath {
    fn into_parts(self) -> (BucketStreamId, String) {
        let stream_id = BucketStreamId::new(self.bucket, self.stream);
        (stream_id, self.retained_offset)
    }
}

struct CreateStreamHttpResponseInput<'a> {
    response: CreateStreamResponse,
    stream_id: &'a BucketStreamId,
    content_type: &'a str,
    stream_ttl_seconds: Option<u64>,
    stream_expires_at_ms: Option<u64>,
    producer: Option<&'a ProducerRequest>,
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
    process_incarnation: ProcessIncarnation,
    admin_fence: admin_fence::AdminMutationFence,
    configured_node_id: Option<u64>,
    runtime: ShardRuntime,
    raft_registry: Option<RaftGroupHandleRegistry>,
    client_write_router: Option<ClientWriteLeaderRouter>,
    http_metrics: Arc<HttpMetrics>,
    wall_clock: Arc<dyn WallClock>,
    pub node_memory: NodeMemoryMonitor,
    external_payload_min_bytes: usize,
    wal_disk: WalDiskMonitor,
    /// The node's Raft WAL when it runs Raft: how it opened, and the
    /// clean shutdown at exit.
    raft_wal: Option<ursula_raft::RaftWal>,
    /// A Raft protocol (format-epoch) mismatch seen since start: readiness
    /// answers 503 `format_epoch_mismatch` until restart.
    format_epoch_mismatch: ursula_raft::FormatEpochMismatch,
}

impl HttpState {
    /// The static topology is the expected inventory. Observed metrics cannot
    /// establish which groups or voters are missing after a restart.
    fn raft_maintenance_report(&self) -> Option<ursula_raft::RaftMaintenanceReport> {
        let registry = self.raft_registry()?;
        let topology = self.client_write_router.as_ref()?;
        let snapshots = registry.metrics_snapshot();
        let node_id = topology
            .node_id
            .or_else(|| snapshots.first().map(|group| group.node_id))?;
        let all_voters = topology.peers.keys().copied().collect::<BTreeSet<_>>();
        let expected = (0..self.runtime.raft_group_count())
            .filter_map(|id| {
                let voters = topology
                    .per_group_voters
                    .get(&RaftGroupId(id))
                    .unwrap_or(&all_voters);
                voters.contains(&node_id).then(|| (id, voters.clone()))
            })
            .collect();
        Some(ursula_raft::check_raft_maintenance(
            &snapshots, node_id, expected, 16,
        ))
    }

    /// Bridge the runtime's metrics to the global OTLP meter (export-time
    /// observable instruments; no hot-path cost). Inert when no OTLP meter
    /// provider is installed.
    pub fn register_otel_metrics(&self) {
        otel_metrics::register(&self.runtime.metrics());
        if let Some(raft_wal) = &self.raft_wal {
            otel_metrics::register_wal_recovery(raft_wal.recovery_state());
        }
        if let Some(registry) = &self.raft_registry {
            otel_metrics::register_recovery_gates(registry.clone());
        }
    }

    pub(crate) fn with_process_incarnation(mut self, boot: ProcessIncarnation) -> Self {
        self.process_incarnation = boot;
        self
    }

    pub(crate) fn with_startup_maintenance_fence(
        mut self,
        fence: ursula_proto::admin::MaintenanceFenceState,
    ) -> Self {
        self.admin_fence = admin_fence::AdminMutationFence::from_startup(fence);
        self
    }

    pub(crate) fn with_configured_node_id(mut self, node_id: u64) -> Self {
        self.configured_node_id = Some(node_id);
        self
    }

    pub fn new(runtime: ShardRuntime) -> Self {
        Self {
            process_incarnation: ProcessIncarnation::from_bits(rand::random()),
            admin_fence: admin_fence::AdminMutationFence::default(),
            configured_node_id: None,
            runtime,
            raft_registry: None,
            client_write_router: None,
            http_metrics: Arc::new(HttpMetrics::default()),
            wall_clock: Arc::new(SystemWallClock),
            node_memory: NodeMemoryMonitor::default(),
            external_payload_min_bytes: 1024 * 1024,
            wal_disk: WalDiskMonitor::default(),
            raft_wal: None,
            format_epoch_mismatch: ursula_raft::FormatEpochMismatch::global(),
        }
    }

    pub fn with_raft_registry(
        runtime: ShardRuntime,
        raft_registry: RaftGroupHandleRegistry,
    ) -> Self {
        Self {
            process_incarnation: ProcessIncarnation::from_bits(rand::random()),
            admin_fence: admin_fence::AdminMutationFence::default(),
            configured_node_id: None,
            runtime,
            raft_registry: Some(raft_registry),
            client_write_router: None,
            http_metrics: Arc::new(HttpMetrics::default()),
            wall_clock: Arc::new(SystemWallClock),
            node_memory: NodeMemoryMonitor::default(),
            external_payload_min_bytes: 1024 * 1024,
            wal_disk: WalDiskMonitor::default(),
            raft_wal: None,
            format_epoch_mismatch: ursula_raft::FormatEpochMismatch::global(),
        }
    }

    pub fn with_static_raft_cluster(
        runtime: ShardRuntime,
        raft_registry: RaftGroupHandleRegistry,
        peers: impl IntoIterator<Item = (u64, String)>,
    ) -> Self {
        Self::with_static_raft_cluster_topology(
            runtime,
            raft_registry,
            None,
            peers,
            BTreeMap::new(),
        )
    }

    pub fn with_static_raft_cluster_topology(
        runtime: ShardRuntime,
        raft_registry: RaftGroupHandleRegistry,
        node_id: impl Into<Option<u64>>,
        peers: impl IntoIterator<Item = (u64, String)>,
        per_group_voters: BTreeMap<RaftGroupId, BTreeSet<u64>>,
    ) -> Self {
        Self {
            runtime,
            raft_registry: Some(raft_registry),
            client_write_router: Some(ClientWriteLeaderRouter::with_static_topology(
                node_id,
                peers,
                per_group_voters,
            )),
            process_incarnation: ProcessIncarnation::from_bits(rand::random()),
            admin_fence: admin_fence::AdminMutationFence::default(),
            configured_node_id: None,
            http_metrics: Arc::new(HttpMetrics::default()),
            wall_clock: Arc::new(SystemWallClock),
            node_memory: NodeMemoryMonitor::default(),
            external_payload_min_bytes: 1024 * 1024,
            wal_disk: WalDiskMonitor::default(),
            raft_wal: None,
            format_epoch_mismatch: ursula_raft::FormatEpochMismatch::global(),
        }
    }

    pub fn with_wall_clock(mut self, wall_clock: impl WallClock) -> Self {
        self.wall_clock = Arc::new(wall_clock);
        self
    }

    pub fn with_wall_clock_handle(mut self, wall_clock: Arc<dyn WallClock>) -> Self {
        self.wall_clock = wall_clock;
        self
    }

    pub fn with_external_payload_min_bytes(mut self, min_bytes: usize) -> Self {
        self.external_payload_min_bytes = min_bytes;
        self
    }

    /// Record the raft WAL backend so it appears in the metrics JSON.
    pub(crate) fn with_wal_disk_monitor(mut self, monitor: WalDiskMonitor) -> Self {
        self.wal_disk = monitor;
        self
    }

    /// Record the node's Raft WAL, so the metrics JSON reports how it opened
    /// and the server shuts it down cleanly.
    pub fn with_raft_wal(mut self, raft_wal: Option<ursula_raft::RaftWal>) -> Self {
        self.raft_wal = raft_wal;
        self
    }

    pub(crate) fn raft_wal(&self) -> Option<&ursula_raft::RaftWal> {
        self.raft_wal.as_ref()
    }

    #[cfg(test)]
    pub(crate) fn with_format_epoch_mismatch(
        mut self,
        mismatch: ursula_raft::FormatEpochMismatch,
    ) -> Self {
        self.format_epoch_mismatch = mismatch;
        self
    }

    pub(crate) fn wal_disk_monitor(&self) -> WalDiskMonitor {
        self.wal_disk.clone()
    }

    /// Apply runtime-level config (memory monitor, payload threshold) derived
    /// from the typed configuration.  Replaces the hard-coded defaults set by
    /// the constructors.
    pub fn with_runtime_config(mut self, config: &ursula_config::RuntimeConfig) -> Self {
        self.node_memory = NodeMemoryMonitor::new(config);
        if let Some(min_size) = &config.external_payload_min_size {
            self.external_payload_min_bytes = usize::try_from(min_size.as_bytes())
                .expect("config validation ensures payload size fits usize");
        }
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

pub use ursula_proto::telemetry::HttpMetricsSnapshot;

/// Resolves the current leader of a raft group to a client-reachable base URL
/// so a write/read that lands on a non-leader can be answered with a 307
/// redirect. `peers` maps raft node id to that node's configured peer URL:
/// `server.listen` when `server.cluster_listen` is unset, or the separate
/// `server.cluster_listen` address when it is set. Peer URLs are also used as
/// HTTP leader-redirect targets, so clients and gateways must be able to reach
/// them too.
#[derive(Clone, Debug)]
pub struct ClientWriteLeaderRouter {
    peers: Arc<BTreeMap<u64, String>>,
    node_id: Option<u64>,
    per_group_voters: Arc<BTreeMap<RaftGroupId, BTreeSet<u64>>>,
}

impl ClientWriteLeaderRouter {
    pub fn new(peers: impl IntoIterator<Item = (u64, String)>) -> Self {
        Self::with_static_topology(None, peers, BTreeMap::new())
    }

    pub fn with_static_topology(
        node_id: impl Into<Option<u64>>,
        peers: impl IntoIterator<Item = (u64, String)>,
        per_group_voters: BTreeMap<RaftGroupId, BTreeSet<u64>>,
    ) -> Self {
        Self {
            peers: Arc::new(
                peers
                    .into_iter()
                    .map(|(node_id, url)| (node_id, url.trim_end_matches('/').to_owned()))
                    .collect(),
            ),
            node_id: node_id.into(),
            per_group_voters: Arc::new(per_group_voters),
        }
    }

    /// The hinted leader's base URL. A hint naming this node (a follower's
    /// read bounced back during a leadership transfer) yields `None`, so the
    /// client gets the leader-unknown 503 instead of a redirect to itself.
    fn leader_base(&self, err: &RuntimeError) -> Option<(u64, String)> {
        let leader_hint = err.leader_hint()?;
        let leader_id = leader_hint.node_id?;
        if Some(leader_id) == self.node_id {
            return None;
        }
        let leader_base = self
            .peers
            .get(&leader_id)
            .or(leader_hint.address.as_ref())?;
        Some((leader_id, leader_base.trim_end_matches('/').to_owned()))
    }

    fn hosted_group_base(&self, err: &RuntimeError) -> Option<(u64, String)> {
        let RuntimeError::GroupNotHosted { raft_group_id, .. } = err else {
            return None;
        };
        let voters = self.per_group_voters.get(raft_group_id)?;
        voters
            .iter()
            .copied()
            .filter(|node_id| Some(*node_id) != self.node_id)
            .find_map(|node_id| {
                self.peers
                    .get(&node_id)
                    .map(|base| (node_id, base.trim_end_matches('/').to_owned()))
            })
    }

    fn redirect_response(&self, err: &RuntimeError, request_target: &str) -> Option<Response> {
        let (leader_id, leader_base) = self
            .leader_base(err)
            .or_else(|| self.hosted_group_base(err))?;
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

/// Process-wide RSS monitor. It reports RSS in `/__ursula/metrics` and exits
/// when the last-resort abort cap is exceeded. It does not reject writes;
/// ingress byte admission is the write-path memory control.
#[derive(Clone)]
pub struct NodeMemoryMonitor {
    abort_cap_bytes: Option<u64>,
    last_rss_bytes: Arc<AtomicU64>,
}

impl std::fmt::Debug for NodeMemoryMonitor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NodeMemoryMonitor")
            .field("abort_cap_bytes", &self.abort_cap_bytes)
            .field(
                "last_rss_bytes",
                &self.last_rss_bytes.load(Ordering::Relaxed),
            )
            .finish()
    }
}

impl Default for NodeMemoryMonitor {
    fn default() -> Self {
        Self {
            abort_cap_bytes: None,
            last_rss_bytes: Arc::new(AtomicU64::new(0)),
        }
    }
}

impl NodeMemoryMonitor {
    pub fn new(cfg: &ursula_config::RuntimeConfig) -> Self {
        let monitor = Self {
            abort_cap_bytes: cfg
                .node_memory_abort_cap_size
                .as_ref()
                .map(|s| s.as_bytes()),
            last_rss_bytes: Arc::new(AtomicU64::new(0)),
        };
        monitor.spawn_rss_sampler();
        monitor
    }

    pub fn last_rss_bytes(&self) -> u64 {
        self.last_rss_bytes.load(Ordering::Relaxed)
    }

    pub fn abort_cap_bytes(&self) -> Option<u64> {
        self.abort_cap_bytes
    }

    #[cfg(madsim)]
    fn spawn_rss_sampler(&self) {
        // Deterministic simulation: never report RSS.
    }

    #[cfg(not(madsim))]
    fn spawn_rss_sampler(&self) {
        let last_rss_bytes = self.last_rss_bytes.clone();
        let abort_cap = self.abort_cap_bytes;
        tokio::spawn(async move {
            loop {
                if let Some(rss) = read_proc_self_status_vm_rss_bytes() {
                    last_rss_bytes.store(rss, Ordering::Relaxed);
                    if let Some(cap) = abort_cap
                        && rss > cap
                    {
                        let host = std::env::var("HOSTNAME")
                            .ok()
                            .or_else(|| std::fs::read_to_string("/proc/sys/kernel/hostname").ok())
                            .map(|s| s.trim().to_string())
                            .unwrap_or_default();
                        let now_ms = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .map(|d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
                            .unwrap_or(0);
                        let breadcrumb = serde_json::json!({
                            "event": "memory_abort_cap_exit",
                            "ts_ms": now_ms,
                            "host": host,
                            "rss_bytes": rss,
                            "abort_cap_bytes": cap,
                        })
                        .to_string();
                        tracing::error!("{breadcrumb}");
                        use std::io::Write as _;
                        #[expect(
                            clippy::let_underscore_must_use,
                            reason = "best-effort flush immediately before abort; nothing can handle the error"
                        )]
                        let _ = std::io::stderr().flush();
                        std::process::abort();
                    }
                }
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        });
    }
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

#[derive(Clone)]
struct HttpRaftGrpcService {
    raft: RaftGrpcService,
}

impl HttpRaftGrpcService {
    fn new(registry: RaftGroupHandleRegistry, state: HttpState) -> Self {
        let cold_store = state.runtime().cold_store();
        Self {
            raft: RaftGrpcService::new(registry).with_cold_store(cold_store),
        }
    }
}

#[tonic::async_trait]
impl raft_internal_proto::raft_internal_server::RaftInternal for HttpRaftGrpcService {
    type AppendStreamStream =
        <RaftGrpcService as raft_internal_proto::raft_internal_server::RaftInternal>::AppendStreamStream;

    async fn append(
        &self,
        request: tonic::Request<raft_internal_proto::RaftRpcEnvelopeV1>,
    ) -> Result<tonic::Response<raft_internal_proto::RaftRpcAckV1>, tonic::Status> {
        raft_internal_proto::raft_internal_server::RaftInternal::append(&self.raft, request).await
    }

    async fn append_stream(
        &self,
        request: tonic::Request<tonic::Streaming<raft_internal_proto::RaftAppendStreamRequest>>,
    ) -> Result<tonic::Response<Self::AppendStreamStream>, tonic::Status> {
        raft_internal_proto::raft_internal_server::RaftInternal::append_stream(&self.raft, request)
            .await
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

    async fn rejoin_barrier(
        &self,
        request: tonic::Request<raft_internal_proto::RejoinBarrierRequestV1>,
    ) -> Result<tonic::Response<raft_internal_proto::RejoinBarrierResponseV1>, tonic::Status> {
        raft_internal_proto::raft_internal_server::RaftInternal::rejoin_barrier(&self.raft, request)
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
    .accept_compressed(tonic::codec::CompressionEncoding::Zstd)
    .max_decoding_message_size(RAFT_GRPC_MAX_MESSAGE_BYTES)
    .max_encoding_message_size(RAFT_GRPC_MAX_MESSAGE_BYTES)
}

pub fn router(runtime: ShardRuntime) -> Router {
    let state = HttpState::new(runtime);
    cluster_router_from_state(state.clone())
        .merge(admin_ops_router(state.clone()))
        .merge(client_router_with_admission(
            state,
            IngressAdmission::default(),
        ))
}

pub fn router_with_raft_registry(
    runtime: ShardRuntime,
    raft_registry: RaftGroupHandleRegistry,
) -> Router {
    let state = HttpState::with_raft_registry(runtime, raft_registry);
    cluster_router_from_state(state.clone()).merge(client_router_with_admission(
        state,
        IngressAdmission::default(),
    ))
}

pub fn router_with_static_raft_cluster(
    runtime: ShardRuntime,
    raft_registry: RaftGroupHandleRegistry,
    peers: impl IntoIterator<Item = (u64, String)>,
) -> Router {
    let state = HttpState::with_static_raft_cluster(runtime, raft_registry, peers);
    cluster_router_from_state(state.clone()).merge(client_router_with_admission(
        state,
        IngressAdmission::default(),
    ))
}

pub fn router_with_static_raft_cluster_topology(
    runtime: ShardRuntime,
    raft_registry: RaftGroupHandleRegistry,
    node_id: u64,
    peers: impl IntoIterator<Item = (u64, String)>,
    per_group_voters: BTreeMap<RaftGroupId, BTreeSet<u64>>,
) -> Router {
    let state = HttpState::with_static_raft_cluster_topology(
        runtime,
        raft_registry,
        Some(node_id),
        peers,
        per_group_voters,
    );
    cluster_router_from_state(state.clone())
        .merge(admin_ops_router(state.clone()))
        .merge(client_router_with_admission(
            state,
            IngressAdmission::default(),
        ))
}

/// Convenience wrapper that merges the client, cluster, and admin planes into
/// a single router.  Used by in-process tests and the madsim harness, where
/// per-plane listeners would only add noise.
pub fn router_with_http_state(state: HttpState) -> Router {
    cluster_router_from_state(state.clone())
        .merge(admin_ops_router(state.clone()))
        .merge(client_router_with_admission(
            state,
            IngressAdmission::default(),
        ))
}

/// Admin-plane routes: the mutating operator surface (raft group operations,
/// maintenance drain, cold-flush trigger, bucket purge) plus read-only metrics
/// and usage so operator tooling works over a single tunnel. Production binds this to
/// `server.admin_listen` (loopback by default) — nodes expose no
/// cluster-mutation endpoints on the client or cluster planes.
pub fn admin_router(state: HttpState) -> Router {
    admin_ops_router(state.clone()).merge(
        Router::new()
            .route("/__ursula/metrics", get(metrics))
            .route("/__ursula/usage", get(bucket_usage))
            .with_state(state),
    )
}

/// The mutating admin routes without the metrics and usage aliases. The
/// single-router convenience mergers use this directly because the client
/// plane already serves `/__ursula/metrics` and `/__ursula/usage`.
fn admin_ops_router(state: HttpState) -> Router {
    let router = Router::new()
        .route(
            "/__ursula/maintenance/fence/activate",
            post(activate_admin_fence),
        )
        .route(
            "/__ursula/maintenance/fence/retire",
            post(retire_admin_fence),
        )
        .route(
            "/__ursula/flush-cold/{bucket}/{stream}",
            post(flush_cold_stream),
        )
        .route(
            "/__ursula/purge/{bucket}",
            axum::routing::delete(purge_bucket),
        )
        .route("/__ursula/backup/info", get(backup_info))
        .route(
            "/__ursula/backup/group/{raft_group_id}",
            get(export_backup_group),
        )
        .route(
            "/__ursula/backup/group/{raft_group_id}/import",
            post(import_backup_group),
        )
        .route(
            "/__ursula/backup/group/{raft_group_id}/cold-check",
            post(check_backup_group_cold_objects),
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
            "/__ursula/raft/{raft_group_id}/membership",
            post(change_raft_membership),
        )
        .route(
            "/__ursula/raft/{raft_group_id}/learners/{node_id}",
            post(add_raft_learner),
        )
        .route(
            "/__ursula/raft/{raft_group_id}/leader/transfer/{node_id}",
            post(transfer_raft_leader),
        )
        .route(
            "/__ursula/raft/{raft_group_id}/quorum",
            get(confirm_raft_quorum),
        )
        .route(
            "/__ursula/raft/{raft_group_id}/self-election",
            post(request_raft_self_election),
        )
        .route(
            "/__ursula/raft/{raft_group_id}/recovery/accept-unsynced-loss",
            post(accept_unsynced_loss),
        )
        .route(
            "/__ursula/leadership-shed/maintenance",
            post(mark_maintenance_drain).delete(clear_maintenance_drain),
        );
    #[cfg(feature = "jemalloc-prof")]
    let router = router.route("/__ursula/debug/heap-profile", get(heap_profile));
    router
        .route_layer(middleware::from_fn_with_state(
            state.clone(),
            require_admin_incarnation,
        ))
        .layer(DefaultBodyLimit::max(MAX_HTTP_BODY_BYTES))
        .with_state(state)
}

fn reject_admin_incarnation(
    state: &HttpState,
    headers: &axum::http::HeaderMap,
) -> Option<Response> {
    let Some(identity) = headers.get(PROCESS_INCARNATION_HEADER) else {
        return Some(
            (
                StatusCode::PRECONDITION_REQUIRED,
                "admin operation requires its observed process incarnation",
            )
                .into_response(),
        );
    };
    if identity.to_str().ok() != Some(state.process_incarnation.as_str()) {
        return Some(
            (
                StatusCode::PRECONDITION_FAILED,
                "admin target process incarnation changed; stop the current maintenance plan",
            )
                .into_response(),
        );
    }
    None
}

async fn require_admin_incarnation(
    State(state): State<HttpState>,
    request: Request<Body>,
    next: Next,
) -> Response {
    if !matches!(
        *request.method(),
        Method::GET | Method::HEAD | Method::OPTIONS
    ) {
        if let Some(response) = reject_admin_incarnation(&state, request.headers()) {
            return response;
        }
        // Lifecycle handlers take the write guard themselves. They remain
        // process-bound and validate the supplied immutable executor token.
        if *request.method() == Method::POST
            && matches!(
                request.uri().path(),
                "/__ursula/maintenance/fence/activate" | "/__ursula/maintenance/fence/retire"
            )
        {
            return next.run(request).await;
        }
        let fence_header = match request.headers().get(MAINTENANCE_FENCE_HEADER) {
            Some(header) => match header.to_str() {
                Ok(value) => Some(value),
                Err(_) => return admin_fence::FenceRejection::Changed.response(),
            },
            None => None,
        };
        let guard = match state.admin_fence.admit_mutation(fence_header).await {
            Ok(guard) => guard,
            Err(rejection) => return rejection.response(),
        };
        // Dropping the caller's response future must not cancel an admitted
        // actor/Core mutation and release its guard before its reply arrives.
        return match tokio::spawn(async move {
            let response = next.run(request).await;
            guard.complete();
            response
        })
        .await
        {
            Ok(response) => response,
            Err(error) => {
                tracing::error!(%error, "admitted admin mutation task failed");
                admin_fence::FenceRejection::Uncertain.response()
            }
        };
    }
    next.run(request).await
}

async fn confirm_admin_command_submission(state: &HttpState) -> Result<(), String> {
    if let Some(registry) = &state.raft_registry {
        registry.confirm_admin_command_submission().await?;
    }
    Ok(())
}

async fn activate_admin_fence(
    State(state): State<HttpState>,
    Json(fence): Json<MaintenanceFence>,
) -> Response {
    match state
        .admin_fence
        .activate(fence, confirm_admin_command_submission(&state))
        .await
    {
        Ok(snapshot) => Json(snapshot).into_response(),
        Err(rejection) => rejection.response(),
    }
}

async fn retire_admin_fence(
    State(state): State<HttpState>,
    Json(fence): Json<MaintenanceFence>,
) -> Response {
    match state
        .admin_fence
        .retire(fence, confirm_admin_command_submission(&state))
        .await
    {
        Ok(snapshot) => Json(snapshot).into_response(),
        Err(rejection) => rejection.response(),
    }
}

/// Cluster-plane routes: inter-node gRPC carrying Raft RPCs, snapshot
/// transfer, and leader-read checks. In a dual-listener deployment these bind
/// to the private (VPC) interface so chaos applied to the public face never
/// disrupts consensus.
pub fn cluster_router_from_state(state: HttpState) -> Router {
    let raft_registry = state.raft_registry.clone().unwrap_or_default();
    Router::new()
        .route_service(
            RAFT_GRPC_APPEND_PATH,
            raft_grpc_service(state.clone(), raft_registry.clone()),
        )
        .route_service(
            RAFT_GRPC_APPEND_STREAM_PATH,
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
            RAFT_GRPC_REJOIN_BARRIER_PATH,
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

/// Client-plane ingress admission. The primary control is a process-wide
/// in-flight write-body byte budget: a request must reserve its body bytes
/// before axum drains the body into `Bytes`, and the reservation lives until
/// the response is produced. This is the layer that turns memory pressure into
/// backpressure at the HTTP edge.
///
/// Configurable via `ServerConfig.http_inflight_body_size`.
#[derive(Clone)]
pub struct IngressAdmission {
    body_bytes: Arc<tokio::sync::Semaphore>,
    wal_disk: WalDiskMonitor,
    /// The node's Raft log-pressure flag (set by the snapshot driver's
    /// monitor while the unsnapshotted log is over its hard limit).
    raft_log: Option<ursula_raft::SnapshotBuildCoordinator>,
}

impl Default for IngressAdmission {
    fn default() -> Self {
        Self {
            body_bytes: Arc::new(tokio::sync::Semaphore::new(
                DEFAULT_HTTP_INFLIGHT_BODY_BYTES,
            )),
            wal_disk: WalDiskMonitor::default(),
            raft_log: None,
        }
    }
}

impl IngressAdmission {
    pub fn new(cfg: &ursula_config::ServerConfig) -> Self {
        let body_budget = cfg.http_inflight_body_size.as_bytes() as usize;
        Self {
            body_bytes: Arc::new(tokio::sync::Semaphore::new(body_budget)),
            wal_disk: WalDiskMonitor::default(),
            raft_log: None,
        }
    }

    pub fn disabled() -> Self {
        Self {
            body_bytes: Arc::new(tokio::sync::Semaphore::new(usize::MAX)),
            wal_disk: WalDiskMonitor::default(),
            raft_log: None,
        }
    }

    pub(crate) fn with_wal_disk_monitor(mut self, monitor: WalDiskMonitor) -> Self {
        self.wal_disk = monitor;
        self
    }

    /// Refuse client writes while the node's Raft log is over its hard limit.
    pub fn with_raft_log_pressure(
        mut self,
        coordinator: Option<ursula_raft::SnapshotBuildCoordinator>,
    ) -> Self {
        self.raft_log = coordinator;
        self
    }
}

async fn ingress_admission_middleware(
    State(admission): State<IngressAdmission>,
    request: Request<Body>,
    next: Next,
) -> Response {
    if request.uri().path() == CLUSTER_PROBE_PATH {
        return next.run(request).await;
    }
    let Some(body_bytes) = request_write_body_bytes(&request) else {
        return next.run(request).await;
    };
    if body_bytes > cold_snapshot::max_admitted_body_bytes(request.method(), request.uri()) {
        return (StatusCode::PAYLOAD_TOO_LARGE, "request body is too large").into_response();
    }
    if admission.wal_disk.is_pressured() {
        return retry_after_json("WalDiskPressure");
    }
    // Every write that carries a body grows the Raft log; bodiless ones
    // (retention advances, deletes) only shrink state.
    if body_bytes > 0
        && admission
            .raft_log
            .as_ref()
            .is_some_and(ursula_raft::SnapshotBuildCoordinator::log_pressured)
    {
        return retry_after_json("RaftLogPressure");
    }
    // A snapshot body above the inline cap streams to the cold store in
    // bounded parts (F16), so it holds at most the inline cap in memory.
    let body_bytes =
        body_bytes.min(u64::try_from(MAX_HTTP_BODY_BYTES).expect("max body bytes fits u64"));

    let _body_permits = if body_bytes > 0 {
        let Ok(permits) = u32::try_from(body_bytes) else {
            return (StatusCode::PAYLOAD_TOO_LARGE, "request body is too large").into_response();
        };
        match admission.body_bytes.clone().try_acquire_many_owned(permits) {
            Ok(permit) => Some(permit),
            Err(_) => return retry_after_json("IngressBodyBytesLimitReached"),
        }
    } else {
        None
    };

    next.run(request).await
}

fn request_write_body_bytes(request: &Request<Body>) -> Option<u64> {
    if !is_write_method(request.method()) {
        return None;
    }
    if let Some(content_length) = request.headers().get(CONTENT_LENGTH)
        && let Ok(content_length) = content_length.to_str()
        && let Ok(parsed) = content_length.parse::<u64>()
    {
        return Some(parsed);
    }
    let size_hint = request.body().size_hint();
    size_hint.exact().or_else(|| size_hint.upper()).or(Some(
        u64::try_from(MAX_HTTP_BODY_BYTES).expect("max body bytes fits u64"),
    ))
}

fn is_write_method(method: &Method) -> bool {
    matches!(
        *method,
        Method::POST | Method::PUT | Method::PATCH | Method::DELETE
    )
}

fn retry_after_json(error: &'static str) -> Response {
    let mut headers = HeaderMap::new();
    insert_default_response_headers(&mut headers);
    headers.insert(
        axum::http::header::RETRY_AFTER,
        HeaderValue::from_static("1"),
    );
    headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    (
        StatusCode::SERVICE_UNAVAILABLE,
        headers,
        serde_json::json!({ "error": error }).to_string(),
    )
        .into_response()
}

/// Builds a `200`-style JSON response with the `application/json` content type.
/// Centralizes the content-type wiring so handlers returning a JSON body share
/// one consistent style.
fn json_response(status: StatusCode, body: String) -> Response {
    (status, [(CONTENT_TYPE, "application/json")], body).into_response()
}

/// Path of the cluster egress-health probe (M2). Peers POST a payload here over
/// the cluster plane; the round-trip time exposes loss/delay on the sender's
/// egress, which a small heartbeat-sized request would mask.
pub(crate) const CLUSTER_PROBE_PATH: &str = "/__ursula/cluster-probe";
pub(crate) const LEADERSHIP_SHED_PATH: &str = "/__ursula/leadership-shed";
pub(crate) const READINESS_PATH: &str = "/__ursula/ready";

/// Probe target: drain the body (so the sender's full egress traverses the
/// cluster plane) and answer 200. Bypasses ingress admission.
async fn cluster_probe(_body: Bytes) -> StatusCode {
    StatusCode::OK
}

async fn readiness(State(state): State<HttpState>) -> Response {
    let disk = state.wal_disk.snapshot();
    // A node that saw a peer on another format epoch never becomes Ready
    // again, so a rolling update stops at it (format epoch 2, E8).
    let format_epoch_mismatch = state.format_epoch_mismatch.recorded();
    let recovery_ready = state
        .raft_registry()
        .is_none_or(RaftGroupHandleRegistry::recovery_barriers_ready);
    let raft_maintenance = state.raft_maintenance_report();
    // Non-Raft dev mode has no static voter role. A registry without its
    // topology cannot certify a complete maintenance inventory.
    let raft_ready = state.raft_registry().is_none()
        || raft_maintenance
            .as_ref()
            .is_some_and(ursula_raft::RaftMaintenanceReport::ready);
    let ready = !disk.pressure && !format_epoch_mismatch && recovery_ready && raft_ready;
    // Groups whose gated replica here got no leader barrier and applied
    // nothing for a while: a majority of their voters may be gated, and they
    // refuse writes until an operator accepts the loss of the unsynced tail.
    let stalled_groups = state
        .raft_registry()
        .map(RaftGroupHandleRegistry::stalled_recovery_groups)
        .unwrap_or_default();
    let status = if ready {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    json_response(
        status,
        serde_json::json!({
            "ready": ready,
            "reason": if format_epoch_mismatch {
                Some("format_epoch_mismatch")
            } else if disk.pressure {
                Some("wal_disk_pressure")
            } else if !stalled_groups.is_empty() {
                Some("recovery_stalled")
            } else if !recovery_ready {
                Some("recovery_gate_closed")
            } else if !raft_ready {
                Some("raft_maintenance_unready")
            } else {
                None
            },
            "format_epoch_mismatch": format_epoch_mismatch,
            "recovery_barriers_ready": recovery_ready,
            "raft_maintenance": raft_maintenance,
            "recovery_stalled_groups": stalled_groups,
            "wal_disk_pressure": disk.pressure,
            "wal_available_bytes": disk.available_bytes,
            "wal_min_available_bytes": disk.min_available_bytes,
            "wal_resume_available_bytes": disk.resume_available_bytes,
            "wal_disk_stat_errors": disk.stat_errors,
        })
        .to_string(),
    )
}

async fn leadership_shed_status(State(state): State<HttpState>) -> Response {
    axum::Json(
        state
            .raft_registry()
            .cloned()
            .unwrap_or_default()
            .participation_status(),
    )
    .into_response()
}

async fn mark_maintenance_drain(State(state): State<HttpState>) -> Response {
    let Some(registry) = state.raft_registry() else {
        return (
            StatusCode::BAD_REQUEST,
            "raft registry is not configured for this server",
        )
            .into_response();
    };
    registry.mark_leadership_shed(LeadershipShedReason::MaintenanceDrain);
    leadership_shed_status(State(state)).await
}

async fn clear_maintenance_drain(State(state): State<HttpState>) -> Response {
    let Some(registry) = state.raft_registry() else {
        return (
            StatusCode::BAD_REQUEST,
            "raft registry is not configured for this server",
        )
            .into_response();
    };
    registry.clear_leadership_shed(LeadershipShedReason::MaintenanceDrain);
    leadership_shed_status(State(state)).await
}

pub fn client_router_with_admission(state: HttpState, admission: IngressAdmission) -> Router {
    let finite_record_response =
        |_status: StatusCode,
         _version: Version,
         headers: &HeaderMap,
         _extensions: &axum::http::Extensions| {
            headers
                .get(CONTENT_TYPE)
                .and_then(|value| value.to_str().ok())
                .is_some_and(|content_type| {
                    let media_type = content_type
                        .split(';')
                        .next()
                        .unwrap_or(content_type)
                        .trim();
                    media_type == "application/json" || media_type == "application/x-ndjson"
                })
        };
    let response_compression = CompressionLayer::new()
        .gzip(true)
        .quality(CompressionLevel::Fastest)
        .compress_when(SizeAbove::new(256).and(finite_record_response));

    Router::new()
        .route("/__ursula/metrics", get(metrics))
        .route(READINESS_PATH, get(readiness))
        .route("/__ursula/usage", get(bucket_usage))
        .route(CLUSTER_PROBE_PATH, post(cluster_probe))
        .route("/{bucket}", put(create_bucket))
        // The bare path (the removed latest-snapshot redirect and the removed
        // record-addressed publish) answers 405 to every method, not a 404
        // that Loro's client would read as "no snapshot".
        .route(
            "/{bucket}/{stream}/snapshot",
            axum::routing::any(removed_latest_snapshot),
        )
        .route(
            "/{bucket}/{stream}/snapshot/{snapshot_offset}",
            put(publish_snapshot).get(read_snapshot),
        )
        .route(
            "/{bucket}/{stream}/retention/{retained_offset}",
            put(advance_retention),
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
        .layer(DefaultBodyLimit::max(MAX_HTTP_BODY_BYTES))
        .layer(middleware::from_fn_with_state(
            admission,
            ingress_admission_middleware,
        ))
        .layer(response_compression)
        .with_state(state)
}

pub(crate) fn should_externalize_payload(
    state: &HttpState,
    payload_len: usize,
    allowed: bool,
) -> bool {
    allowed
        && payload_len > 0
        && state.runtime.has_cold_store()
        && payload_len >= state.external_payload_min_bytes
}

pub(crate) async fn stage_external_payload(
    state: &HttpState,
    stream_id: &BucketStreamId,
    payload: &[u8],
) -> Result<ExternalPayloadRef, Response> {
    let Some(cold_store) = state.runtime.cold_store() else {
        return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            "cold backend must be configured before externalizing payloads",
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

/// Bounded-state F5 cleanup rule (ungated): a staged external object may be
/// deleted only when its append or create definitely did not commit. That is
/// a typed stream error (apply, or a pre-proposal check, rejected it on every
/// replica alike), a redirect raised by the local pre-proposal leadership
/// check or a backpressure rejection before proposal, or a
/// request the runtime refused before dispatch. Every other failure (a lost
/// response, a forward-to-leader reported by OpenRaft after `client_write`
/// (RT1), a transport or storage error, an untyped engine error) may
/// follow a committed proposal that references the object, so the object is
/// kept; an orphan sweep or stream GC reclaims it if nothing does.
pub(crate) fn staged_external_definitely_unreferenced(err: &RuntimeError) -> bool {
    match err {
        RuntimeError::GroupEngine { error, .. } => {
            error.code().is_some() || error.is_forward_before_proposal() || error.is_backpressure()
        }
        RuntimeError::EmptyAppend
        | RuntimeError::InvalidRaftGroup { .. }
        | RuntimeError::GroupNotHosted { .. } => true,
        RuntimeError::InvalidConfig(_)
        | RuntimeError::SnapshotPlacementMismatch { .. }
        | RuntimeError::ColdStoreConfig { .. }
        | RuntimeError::StaticMembershipConfig { .. }
        | RuntimeError::ColdStoreIo { .. }
        | RuntimeError::LiveReadBackpressure { .. }
        | RuntimeError::MailboxClosed { .. }
        | RuntimeError::ResponseDropped { .. }
        | RuntimeError::SpawnCoreThread { .. } => false,
    }
}

/// Deletes a staged external object after a failed append or create, but
/// only when [`staged_external_definitely_unreferenced`] allows it.
pub(crate) async fn cleanup_external_payload(state: &HttpState, s3_path: &str, err: &RuntimeError) {
    if !staged_external_definitely_unreferenced(err) {
        tracing::warn!(
            path = %s3_path,
            error = %err,
            "keeping staged external payload after an ambiguous failure"
        );
        return;
    }
    delete_unreferenced_staged_payload(state, s3_path).await;
}

/// Deletes a staged external object that no committed command references:
/// one whose write was definitely rejected, or whose write was answered
/// without applying it (a deduplicated append, a create of a live stream).
pub(crate) async fn delete_unreferenced_staged_payload(state: &HttpState, s3_path: &str) {
    let Some(cold_store) = state.runtime.cold_store() else {
        return;
    };
    if let Err(cleanup_err) = cold_store.delete_chunk(s3_path).await {
        tracing::warn!(
            path = %s3_path,
            error = %cleanup_err,
            "failed to remove an unreferenced staged external payload"
        );
    }
}

pub(crate) fn create_stream_http_response(input: CreateStreamHttpResponseInput<'_>) -> Response {
    let CreateStreamHttpResponseInput {
        response,
        stream_id,
        content_type,
        stream_ttl_seconds,
        stream_expires_at_ms,
        producer,
    } = input;
    let mut headers = HeaderMap::new();
    insert_default_response_headers(&mut headers);
    insert_content_type(&mut headers, content_type);
    insert_offset(&mut headers, response.next_offset);
    insert_location(&mut headers, stream_id);
    insert_incarnation(&mut headers, response.incarnation);
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
    // A duplicate beyond the receipt window (bounded-state F3) is answered
    // `204` with `Producer-Seq` and without a byte range.
    if !response.receipt_evicted {
        insert_offset(&mut headers, response.next_offset);
    }
    insert_incarnation(&mut headers, response.incarnation);
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

/// Administrator-triggered tenant offboarding (#150): purges the bucket from
/// every Raft group, then runs one cold-GC pass so the enqueued cold-object
/// prefixes are reclaimed before the report returns, and finally erases and
/// proves empty `{bucket}/`. Idempotent — purging an
/// absent bucket returns the same report shape with zero counts, and a
/// crashed purge converges on re-run because cold reclamation is
/// list-then-delete over object prefixes.
pub(crate) async fn purge_bucket(
    State(state): State<HttpState>,
    Path(bucket): Path<String>,
) -> Response {
    let report = match state.runtime.purge_bucket_all_groups(&bucket).await {
        Ok(report) => report,
        Err(err) => return purge_error_response(err),
    };
    // Reclaim the just-enqueued cold prefixes now instead of waiting for the
    // background worker's next pass. Failures leave entries queued for the
    // worker; the purge itself is already durable.
    let (cold_gc_reclaimed, mut cold_gc_error) = match state
        .runtime
        .run_cold_gc_all_groups_once(COLD_GC_PURGE_BATCH_MAX_ENTRIES)
        .await
    {
        Ok(reclaimed) => (reclaimed, None),
        Err(err) => {
            tracing::warn!(
                bucket = %bucket,
                error = %err,
                "cold GC pass after purge failed; background worker will finish reclamation"
            );
            (0, Some(err.to_string()))
        }
    };
    // A second idempotent purge is a linearized read of every group's durable
    // queue after reclamation. The number reclaimed by one pass is only
    // diagnostic: delayed entries, a partial failure, or another leader's
    // background worker can all make it zero without proving cold absence.
    let proof = match state.runtime.purge_bucket_all_groups(&bucket).await {
        Ok(report) => report,
        Err(err) => return purge_error_response(err),
    };
    let bucket_prefix_absent = if proof.pending_cold_gc_entries == 0 && cold_gc_error.is_none() {
        match state
            .runtime
            .erase_bucket_cold_prefix_and_prove(&bucket)
            .await
        {
            Ok(()) => true,
            Err(err) => {
                cold_gc_error = Some(err.to_string());
                false
            }
        }
    } else {
        false
    };
    let cold_gc_complete =
        proof.pending_cold_gc_entries == 0 && cold_gc_error.is_none() && bucket_prefix_absent;
    axum::Json(serde_json::json!({
        "bucket": bucket,
        "removed_streams": report.removed_streams,
        "groups_with_streams": report.groups_with_streams,
        "cold_gc_entries_reclaimed": cold_gc_reclaimed,
        "cold_gc_pending_entries": proof.pending_cold_gc_entries,
        "cold_gc_complete": cold_gc_complete,
        "cold_gc_error": cold_gc_error,
        "bucket_prefix_absent": bucket_prefix_absent,
    }))
    .into_response()
}

/// Purge runs on the admin listener, so it never redirects to a peer's client
/// URL. A group whose leader is unknown or moving answers `503` with
/// `Retry-After`; purge is idempotent, so the caller retries the whole request.
fn purge_error_response(err: RuntimeError) -> Response {
    if is_forward_to_leader(&err) {
        return leader_unknown_retry_response(err);
    }
    runtime_error_response(err)
}

const COLD_GC_PURGE_BATCH_MAX_ENTRIES: usize = 4096;

/// `PUT /{bucket}`: buckets are implicit namespaces, created on a group by
/// the first stream create there, so this only validates the bucket ID and
/// answers 201. It stays for clients that create the bucket first.
pub(crate) async fn create_bucket(Path(bucket): Path<String>) -> Response {
    match ursula_runtime::validate_bucket_id(&bucket) {
        Ok(()) => StatusCode::CREATED.into_response(),
        Err(message) => (StatusCode::BAD_REQUEST, message).into_response(),
    }
}

/// Versioned, self-described per-bucket usage summed across this node's Raft
/// groups. Served from local replica state; consumers tolerate replication
/// lag and validate the contract before interpreting derived counters.
pub(crate) async fn bucket_usage(State(state): State<HttpState>) -> Response {
    match state.runtime.bucket_usage_all_groups().await {
        Ok(report) => {
            let buckets = report
                .into_iter()
                .map(|entry| {
                    (
                        entry.bucket_id,
                        serde_json::json!({
                            "committed_append_bytes": entry.usage.committed_append_bytes,
                            "committed_records": entry.usage.committed_records,
                            "committed_write_units": entry.usage.committed_write_units,
                            "retained_bytes": entry.usage.retained_bytes,
                            "stream_count": entry.usage.stream_count,
                        }),
                    )
                })
                .collect::<serde_json::Map<_, _>>();
            axum::Json(serde_json::json!({
                "version": 1,
                "write_unit_bytes": ursula_runtime::COMMITTED_WRITE_UNIT_BYTES,
                "buckets": buckets,
            }))
            .into_response()
        }
        Err(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("bucket usage read failed: {err}"),
        )
            .into_response(),
    }
}

pub(crate) async fn metrics(State(state): State<HttpState>) -> Response {
    let raft_groups = state
        .raft_registry()
        .map(RaftGroupHandleRegistry::metrics_snapshot)
        .unwrap_or_default();
    let mut diagnostics = render_metrics(
        state.runtime.metrics().snapshot(),
        state.runtime.mailbox_snapshot(),
        state.http_metrics.snapshot(),
        &raft_groups,
        state.runtime.cold_store_info().as_ref(),
    );
    diagnostics.configured_raft_group_count = state.runtime.raft_group_count();
    diagnostics.group_state_gauges = group_state_gauges_json(&state).await;
    diagnostics.process_rss_bytes = state.node_memory.last_rss_bytes();
    diagnostics.node_memory_abort_cap_bytes =
        state.node_memory.abort_cap_bytes().unwrap_or_default();
    diagnostics.wal_recovery = state.raft_wal.as_ref().map(render::wal_recovery_metrics);
    diagnostics.recovery_gates = state
        .raft_registry()
        .map(RaftGroupHandleRegistry::recovery_report);
    let wal_disk = state.wal_disk.snapshot();
    diagnostics.wal_available_bytes = wal_disk.available_bytes;
    diagnostics.wal_min_available_bytes = wal_disk.min_available_bytes;
    diagnostics.wal_resume_available_bytes = wal_disk.resume_available_bytes;
    diagnostics.wal_disk_pressure = wal_disk.pressure;
    diagnostics.wal_disk_stat_errors = wal_disk.stat_errors;
    axum::Json(ursula_proto::admin::NodeMetrics {
        maintenance_fence: Some(state.admin_fence.snapshot().await),
        maintenance_fence_uncertain: state.admin_fence.is_uncertain(),
        process_incarnation: state.process_incarnation.clone(),
        process_node_id: state
            .configured_node_id
            .or_else(|| {
                state
                    .client_write_router
                    .as_ref()
                    .and_then(|topology| topology.node_id)
            })
            .or_else(|| raft_groups.first().map(|group| group.node_id)),
        raft_groups: raft_groups.iter().map(render::raft_group_metrics).collect(),
        raft_maintenance: state.raft_maintenance_report(),
        diagnostics,
    })
    .into_response()
}

/// Upper bound on how long a metrics scrape waits for the per-group
/// bounded-state gauges; a busy or wedged group must not stall the scrape.
const GROUP_STATE_GAUGES_TIMEOUT: Duration = Duration::from_secs(2);

/// Per-group bounded-state gauges (`docs/architecture/bounded-stream-state.md`
/// §7.5) for `/__ursula/metrics`: one object per Raft group with the group id,
/// whether this node hosts it, and either the gauges or an error.
async fn group_state_gauges_json(
    state: &HttpState,
) -> ursula_proto::telemetry::GroupGaugeCollection {
    use ursula_proto::telemetry::GroupGaugeCollection;
    use ursula_proto::telemetry::GroupGaugeMetrics;
    let Ok(groups) = http_time::timeout(
        GROUP_STATE_GAUGES_TIMEOUT,
        state.runtime.state_gauges_all_groups(),
    )
    .await
    else {
        return GroupGaugeCollection::Error {
            error: "timed out collecting group state gauges".to_owned(),
        };
    };
    GroupGaugeCollection::Groups(
        groups
            .into_iter()
            .map(|(group, result)| match result {
                Ok(gauges) => GroupGaugeMetrics {
                    raft_group_id: group.0,
                    hosted: true,
                    gauges: Some(gauges),
                    error: None,
                },
                Err(RuntimeError::GroupNotHosted { .. }) => GroupGaugeMetrics {
                    raft_group_id: group.0,
                    hosted: false,
                    gauges: None,
                    error: None,
                },
                Err(error) => GroupGaugeMetrics {
                    raft_group_id: group.0,
                    hosted: true,
                    gauges: None,
                    error: Some(error.to_string()),
                },
            })
            .collect(),
    )
}

#[cfg(feature = "jemalloc-prof")]
pub(crate) async fn heap_profile(headers: HeaderMap) -> Response {
    if let Err(response) = authorize_debug_endpoint(&headers) {
        return *response;
    }

    let profile = tokio::task::spawn_blocking(dump_jemalloc_heap_profile).await;
    let bytes = match profile {
        Ok(Ok(bytes)) => bytes,
        Ok(Err(HeapProfileError::Disabled(message))) => {
            return json_response(
                StatusCode::CONFLICT,
                serde_json::json!({
                    "error": "heap_profile_unavailable",
                    "message": message,
                    "required_build_feature": "jemalloc-prof",
                    "required_malloc_conf":
                        format!("{MALLOC_CONF_ENV_VAR}=prof:true,prof_active:true,lg_prof_sample:19"),
                })
                .to_string(),
            );
        }
        Ok(Err(HeapProfileError::Io(message))) => {
            return json_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                serde_json::json!({
                    "error": "heap_profile_io_failed",
                    "message": message,
                })
                .to_string(),
            );
        }
        Err(err) => {
            return json_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                serde_json::json!({
                    "error": "heap_profile_task_failed",
                    "message": err.to_string(),
                })
                .to_string(),
            );
        }
    };

    let mut response_headers = HeaderMap::new();
    insert_default_response_headers(&mut response_headers);
    response_headers.insert(
        CONTENT_TYPE,
        HeaderValue::from_static("application/octet-stream"),
    );
    response_headers.insert(
        CONTENT_DISPOSITION,
        HeaderValue::from_static("attachment; filename=\"ursula-heap.heap\""),
    );
    (StatusCode::OK, response_headers, bytes).into_response()
}

#[cfg(feature = "jemalloc-prof")]
fn authorize_debug_endpoint(headers: &HeaderMap) -> Result<(), BoxResponse> {
    // Any failure answers with the router's plain 404 so unauthenticated
    // probes cannot tell this endpoint apart from an unknown path.
    let expected = match std::env::var("URSULA_DEBUG_TOKEN") {
        Ok(token) if !token.is_empty() => token,
        _ => return Err(Box::new(StatusCode::NOT_FOUND.into_response())),
    };

    let authorized = headers
        .get(HEADER_URSULA_DEBUG_TOKEN)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|actual| constant_time_str_eq(actual, &expected));
    if authorized {
        Ok(())
    } else {
        Err(Box::new(StatusCode::NOT_FOUND.into_response()))
    }
}

// Token comparison must not short-circuit on the first mismatching byte;
// this route is reachable through the gateway, so response timing is
// attacker-observable. Only the length may leak.
#[cfg(feature = "jemalloc-prof")]
fn constant_time_str_eq(a: &str, b: &str) -> bool {
    let a = a.as_bytes();
    let b = b.as_bytes();
    if a.len() != b.len() {
        return false;
    }
    a.iter()
        .zip(b.iter())
        .fold(0u8, |acc, (x, y)| acc | (x ^ y))
        == 0
}

#[cfg(feature = "jemalloc-prof")]
enum HeapProfileError {
    /// Profiling cannot produce data under the current build or runtime
    /// configuration; the response carries remediation hints.
    Disabled(String),
    /// The dump itself failed for reasons unrelated to configuration.
    Io(String),
}

#[cfg(feature = "jemalloc-prof")]
fn dump_jemalloc_heap_profile() -> Result<Vec<u8>, HeapProfileError> {
    let profiling_enabled = tikv_jemalloc_ctl::profiling::prof::read()
        .map_err(|err| HeapProfileError::Io(format!("read jemalloc opt.prof: {err}")))?;
    if !profiling_enabled {
        return Err(HeapProfileError::Disabled(format!(
            "jemalloc profiling is disabled; restart with \
             {MALLOC_CONF_ENV_VAR}=prof:true,prof_active:true"
        )));
    }

    // Dump into a fresh mode-0700 temp directory: a fixed path in
    // world-writable /tmp would be symlink-attackable, readable by other
    // local users, and shared between co-located nodes.
    let dump_dir = tempfile::tempdir()
        .map_err(|err| HeapProfileError::Io(format!("create heap profile dir: {err}")))?;
    let dump_path = dump_dir.path().join("ursula-heap.heap");
    let dump_path = dump_path
        .to_str()
        .ok_or_else(|| HeapProfileError::Io("heap profile path is not UTF-8".to_owned()))?;
    // `raw::write_str` only accepts a `'static` value; leaking the short
    // path string on each authorized dump is the price of staying on the
    // safe mallctl API.
    let dump_path_nul: &'static [u8] =
        Box::leak(format!("{dump_path}\0").into_bytes().into_boxed_slice());
    tikv_jemalloc_ctl::raw::write_str(b"prof.dump\0", dump_path_nul).map_err(|err| {
        HeapProfileError::Io(format!("dump jemalloc heap profile to {dump_path}: {err}"))
    })?;
    let bytes = std::fs::read(dump_path).map_err(|err| {
        HeapProfileError::Io(format!("read jemalloc heap profile {dump_path}: {err}"))
    })?;
    if !heap_profile_has_samples(&bytes) {
        return Err(HeapProfileError::Disabled(format!(
            "heap profile contains no samples; ensure {MALLOC_CONF_ENV_VAR} sets \
             prof_active:true and that jemalloc is this process's allocator"
        )));
    }
    Ok(bytes)
}

// A jemalloc `heap_v2` dump aggregates its totals on the first `t*:` line as
// `t*: <live count>: <live bytes> [...]`. Zero live samples means sampling is
// not actually recording (prof_active:false, or jemalloc is linked but not
// the process's global allocator), which must surface as an error instead of
// an empty-but-200 profile.
#[cfg(feature = "jemalloc-prof")]
fn heap_profile_has_samples(profile: &[u8]) -> bool {
    let text = String::from_utf8_lossy(profile);
    for line in text.lines() {
        let Some(totals) = line.trim_start().strip_prefix("t*:") else {
            continue;
        };
        return totals
            .split(':')
            .next()
            .and_then(|count| count.trim().parse::<u64>().ok())
            .is_none_or(|count| count > 0);
    }
    true
}

pub(crate) async fn flush_cold_stream(
    State(state): State<HttpState>,
    OriginalUri(uri): OriginalUri,
    Path(path): Path<StreamPath>,
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
    let stream_id = path.into_stream_id();
    match state
        .runtime
        .flush_cold_once(PlanColdFlushRequest {
            stream_id,
            min_hot_bytes,
            max_flush_bytes,
        })
        .await
    {
        Ok(Some(response)) => json_response(
            StatusCode::OK,
            serde_json::json!({
                "hot_start_offset": response.hot_start_offset,
                "group_commit_index": response.group_commit_index,
            })
            .to_string(),
        ),
        Ok(None) => StatusCode::NO_CONTENT.into_response(),
        Err(err) => {
            runtime_error_or_leader_redirect_async(&state, err, &request_target(&uri)).await
        }
    }
}

/// The backup format is the format epoch: an ursulactl of another epoch
/// refuses this version with its own check, and this server refuses groups of
/// another epoch (E10).
pub(crate) const BACKUP_FORMAT_VERSION: u32 = ursula_runtime::FORMAT_EPOCH;
pub(crate) const HEADER_BACKUP_FORMAT: &str = "x-ursula-backup-format";
pub(crate) const HEADER_BACKUP_BLAKE3: &str = "x-ursula-backup-blake3";
pub(crate) const HEADER_BACKUP_COMMIT_INDEX: &str = "x-ursula-backup-commit-index";

/// Cluster shape a backup client needs before iterating groups.
pub(crate) async fn backup_info(State(state): State<HttpState>) -> Response {
    (
        StatusCode::OK,
        axum::Json(ursula_proto::admin::BackupInfo {
            format_version: BACKUP_FORMAT_VERSION,
            raft_group_count: state.runtime.raft_group_count(),
        }),
    )
        .into_response()
}

/// Exports one group's complete stream state as a MessagePack document.
///
/// The export is the same deterministic `StreamSnapshot` the raft snapshot
/// path persists, so it is internally consistent per group while writes
/// continue; cross-group consistency is intentionally not promised (the
/// recovery boundary is per stream).
pub(crate) async fn export_backup_group(
    State(state): State<HttpState>,
    OriginalUri(uri): OriginalUri,
    Path(raft_group_id): Path<u64>,
) -> Response {
    let group_count = u64::from(state.runtime.raft_group_count());
    if raft_group_id >= group_count {
        return (
            StatusCode::BAD_REQUEST,
            format!("raft group {raft_group_id} out of range 0..{group_count}"),
        )
            .into_response();
    }
    let Ok(raft_group_id) = parse_raft_group_id(raft_group_id) else {
        return (StatusCode::BAD_REQUEST, "invalid raft group id").into_response();
    };
    let snapshot = match state.runtime.snapshot_group(raft_group_id).await {
        Ok(snapshot) => snapshot,
        Err(err) => {
            return runtime_error_or_leader_redirect_async(&state, err, &request_target(&uri))
                .await;
        }
    };
    let body = match rmp_serde::to_vec_named(&snapshot.stream_snapshot) {
        Ok(body) => body,
        Err(err) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("encode backup snapshot: {err}"),
            )
                .into_response();
        }
    };
    let checksum = blake3::hash(&body).to_hex().to_string();
    let mut response = (StatusCode::OK, body).into_response();
    let headers = response.headers_mut();
    headers.insert(
        HEADER_BACKUP_FORMAT,
        HeaderValue::from(BACKUP_FORMAT_VERSION),
    );
    if let Ok(value) = HeaderValue::from_str(&checksum) {
        headers.insert(HEADER_BACKUP_BLAKE3, value);
    }
    if let Ok(value) = HeaderValue::from_str(&snapshot.group_commit_index.to_string()) {
        headers.insert(HEADER_BACKUP_COMMIT_INDEX, value);
    }
    headers.insert(
        axum::http::header::CONTENT_TYPE,
        HeaderValue::from_static("application/x-msgpack"),
    );
    response
}

/// The only field an import reads before it trusts the payload (E10).
#[derive(serde::Deserialize)]
struct BackupEpochProbe {
    #[serde(default)]
    format_epoch: Option<u32>,
}

/// Reads one backup group object addressed to group `raft_group_id`: the
/// group must exist here and the body must be a stream snapshot of this
/// server's format epoch (E10). A refusal is the `400` to answer.
fn decode_backup_group(
    state: &HttpState,
    raft_group_id: u64,
    body: &[u8],
) -> Result<(RaftGroupId, ursula_runtime::StreamSnapshot), Box<Response>> {
    let bad_request =
        |message: String| Box::new((StatusCode::BAD_REQUEST, message).into_response());
    let group_count = u64::from(state.runtime.raft_group_count());
    if raft_group_id >= group_count {
        return Err(bad_request(format!(
            "raft group {raft_group_id} out of range 0..{group_count}"
        )));
    }
    let Ok(raft_group_id) = parse_raft_group_id(raft_group_id) else {
        return Err(bad_request("invalid raft group id".to_owned()));
    };
    // E10: read only the epoch first, so a group of another epoch answers
    // 400 instead of being decoded under this epoch's rules.
    match rmp_serde::from_slice::<BackupEpochProbe>(body) {
        Ok(BackupEpochProbe {
            format_epoch: Some(epoch),
        }) if epoch == ursula_runtime::FORMAT_EPOCH => {}
        Ok(BackupEpochProbe { format_epoch }) => {
            let found = format_epoch.map_or_else(
                || "no format_epoch".to_owned(),
                |epoch| format!("an unsupported format_epoch ({epoch})"),
            );
            return Err(bad_request(format!(
                "backup group has {found}; this server imports format epoch {} only",
                ursula_runtime::FORMAT_EPOCH
            )));
        }
        Err(err) => return Err(bad_request(format!("decode backup snapshot: {err}"))),
    }
    let snapshot = rmp_serde::from_slice(body)
        .map_err(|err| bad_request(format!("decode backup snapshot: {err}")))?;
    Ok((raft_group_id, snapshot))
}

/// Imports one group's backup snapshot into an empty group as a replicated
/// write. Non-empty groups fail closed with `409`; invalid payloads with
/// `400`. The restored cluster keeps its own raft identity and membership.
pub(crate) async fn import_backup_group(
    State(state): State<HttpState>,
    OriginalUri(uri): OriginalUri,
    Path(raft_group_id): Path<u64>,
    body: axum::body::Bytes,
) -> Response {
    let (raft_group_id, snapshot) = match decode_backup_group(&state, raft_group_id, &body) {
        Ok(decoded) => decoded,
        Err(response) => return *response,
    };
    match state
        .runtime
        .import_group_state(raft_group_id, ImportGroupStateRequest {
            snapshot: Box::new(snapshot),
        })
        .await
    {
        Ok(response) => json_response(
            StatusCode::OK,
            serde_json::json!({
                "buckets": response.buckets,
                "streams": response.streams,
                "group_commit_index": response.group_commit_index,
            })
            .to_string(),
        ),
        Err(err) => {
            runtime_error_or_leader_redirect_async(&state, err, &request_target(&uri)).await
        }
    }
}

/// Checks that this node's cold store holds every cold object one backup
/// group references, without importing anything. A backup carries the
/// references, not the objects, so `ursulactl restore` asks this for every
/// group before its first import. Any node answers: they share one cold
/// store.
pub(crate) async fn check_backup_group_cold_objects(
    State(state): State<HttpState>,
    Path(raft_group_id): Path<u64>,
    body: axum::body::Bytes,
) -> Response {
    let (raft_group_id, snapshot) = match decode_backup_group(&state, raft_group_id, &body) {
        Ok(decoded) => decoded,
        Err(response) => return *response,
    };
    let cold_store = state.runtime.cold_store();
    match ursula_runtime::check_cold_references(cold_store.as_ref(), snapshot).await {
        Ok(report) => (
            StatusCode::OK,
            axum::Json(ursula_proto::admin::BackupColdCheck {
                raft_group_id: raft_group_id.0,
                referenced_objects: report.referenced,
                missing_objects: report.missing,
                missing_sample: report.missing_sample,
            }),
        )
            .into_response(),
        Err(err) => cold_reference_error_response(&err),
    }
}

fn cold_reference_error_response(err: &ursula_runtime::ColdReferenceError) -> Response {
    let status = match err {
        ursula_runtime::ColdReferenceError::InvalidSnapshot(_)
        | ursula_runtime::ColdReferenceError::Unplannable { .. } => StatusCode::BAD_REQUEST,
        ursula_runtime::ColdReferenceError::ColdStore { .. } => StatusCode::SERVICE_UNAVAILABLE,
    };
    let message = std::iter::successors(Some(err as &(dyn std::error::Error + 'static)), |error| {
        error.source()
    })
    .map(ToString::to_string)
    .collect::<Vec<_>>()
    .join(": ");
    (status, message).into_response()
}

pub(crate) async fn trigger_raft_snapshot(
    State(state): State<HttpState>,
    Path(raft_group_id): Path<u64>,
) -> Response {
    let (raft_group_id, raft) = match resolve_raft_group(&state, raft_group_id) {
        Ok(resolved) => resolved,
        Err(response) => return *response,
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
            .metrics(
                |metrics| {
                    metrics
                        .snapshot
                        .as_ref()
                        .is_some_and(|snapshot| snapshot >= &snapshot_log_id)
                },
                format!("admin snapshot trigger .snapshot >= {snapshot_log_id}"),
            )
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
        axum::Json(ursula_proto::admin::SnapshotResponse {
            raft_group_id: raft_group_id.0,
            snapshot_index: metrics.snapshot.map(|log_id| log_id.index),
        }),
    )
        .into_response()
}

pub(crate) async fn trigger_raft_purge(
    State(state): State<HttpState>,
    Path(raft_group_id): Path<u64>,
    axum::extract::Query(query): axum::extract::Query<ursula_proto::admin::PurgeQuery>,
) -> Response {
    let upto = query.upto;
    let (raft_group_id, raft) = match resolve_raft_group(&state, raft_group_id) {
        Ok(resolved) => resolved,
        Err(response) => return *response,
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
        axum::Json(ursula_proto::admin::PurgeResponse {
            raft_group_id: raft_group_id.0,
            purged_index: metrics.purged.map(|log_id| log_id.index),
        }),
    )
        .into_response()
}

pub(crate) async fn add_raft_learner(
    State(state): State<HttpState>,
    Path((raft_group_id, node_id)): Path<(u64, u64)>,
    axum::extract::Query(query): axum::extract::Query<ursula_proto::admin::AddLearnerQuery>,
) -> Response {
    if query.addr.trim().is_empty() {
        return (StatusCode::BAD_REQUEST, "addr query parameter is required").into_response();
    }
    let address = query.addr;
    let blocking = query.blocking.unwrap_or(true);
    let (raft_group_id, raft) = match resolve_raft_group(&state, raft_group_id) {
        Ok(resolved) => resolved,
        Err(response) => return *response,
    };
    match raft
        .add_learner(node_id, BasicNode::new(address.clone()), blocking)
        .await
    {
        Ok(response) => (
            StatusCode::OK,
            axum::Json(ursula_proto::admin::AddLearnerResponse {
                raft_group_id: raft_group_id.0,
                node_id,
                log_index: response.log_id.index(),
            }),
        )
            .into_response(),
        Err(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("add raft learner: {err}"),
        )
            .into_response(),
    }
}

pub(crate) async fn change_raft_membership(
    State(state): State<HttpState>,
    Path(raft_group_id): Path<u64>,
    axum::extract::Query(query): axum::extract::Query<ursula_proto::admin::MembershipQuery>,
) -> Response {
    let voters = match parse_voter_ids(&query.voters) {
        Ok(voters) => voters,
        Err(message) => return (StatusCode::BAD_REQUEST, message).into_response(),
    };
    let (raft_group_id, raft) = match resolve_raft_group(&state, raft_group_id) {
        Ok(resolved) => resolved,
        Err(response) => return *response,
    };
    let metrics = raft.metrics().borrow_watched().clone();
    if metrics.current_leader != Some(metrics.id) {
        return (
            StatusCode::CONFLICT,
            axum::Json(ursula_proto::admin::MembershipResponse {
                raft_group_id: raft_group_id.0,
                current_leader: metrics.current_leader,
                changed: false,
                reason: Some("not leader".to_owned()),
                voter_ids: BTreeSet::new(),
                log_index: None,
            }),
        )
            .into_response();
    }

    match raft.change_membership(voters.clone(), false).await {
        Ok(response) => (
            StatusCode::OK,
            axum::Json(ursula_proto::admin::MembershipResponse {
                raft_group_id: raft_group_id.0,
                voter_ids: voters,
                log_index: Some(response.log_id.index()),
                changed: true,
                current_leader: None,
                reason: None,
            }),
        )
            .into_response(),
        Err(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("change raft membership: {err}"),
        )
            .into_response(),
    }
}

pub(crate) fn parse_voter_ids(raw: &str) -> Result<BTreeSet<u64>, String> {
    let mut voters = BTreeSet::new();
    for part in raw.split(',') {
        let part = part.trim();
        if part.is_empty() {
            return Err("voters contains an empty node id".to_owned());
        }
        let node_id = part
            .parse::<u64>()
            .map_err(|err| format!("invalid voter id '{part}': {err}"))?;
        voters.insert(node_id);
    }
    if voters.is_empty() {
        return Err("voters must not be empty".to_owned());
    }
    Ok(voters)
}

pub(crate) async fn transfer_raft_leader(
    State(state): State<HttpState>,
    Path((raft_group_id, node_id)): Path<(u64, u64)>,
) -> Response {
    use ursula_proto::admin::TransferLeaderResponse;

    let (group, raft) = match resolve_raft_group(&state, raft_group_id) {
        Ok(resolved) => resolved,
        Err(response) => return *response,
    };
    let Some(registry) = state.raft_registry() else {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    };
    let metrics = raft.metrics().borrow_watched().clone();
    match registry.transfer_leader(group, node_id).await {
        Ok(()) => axum::Json(TransferLeaderResponse {
            raft_group_id,
            from: Some(metrics.id),
            to: Some(node_id),
            current_leader: None,
            transferred: true,
            rejection: None,
            reason: None,
        })
        .into_response(),
        Err(error) => transfer_raft_error_response(
            raft_group_id,
            metrics.id,
            node_id,
            metrics.current_leader,
            error,
        ),
    }
}

fn transfer_raft_error_response(
    group: u64,
    from: u64,
    to: u64,
    leader: Option<u64>,
    error: ursula_raft::LeadershipTransferError,
) -> Response {
    use ursula_proto::admin::TransferLeaderResponse;
    use ursula_proto::admin::TransferRejection;
    use ursula_raft::LeadershipTransferError;
    let (status, rejection) = match &error {
        LeadershipTransferError::NotRegistered { .. } => {
            (StatusCode::NOT_FOUND, TransferRejection::NotRegistered)
        }
        LeadershipTransferError::NotLeader { .. } => {
            (StatusCode::CONFLICT, TransferRejection::NotLeader)
        }
        LeadershipTransferError::RecoveringTarget { .. } => {
            (StatusCode::CONFLICT, TransferRejection::RecoveringTarget)
        }
        LeadershipTransferError::UnreachableTarget { .. }
        | LeadershipTransferError::LaggingTarget { .. } => {
            (StatusCode::CONFLICT, TransferRejection::UnreadyTarget)
        }
        LeadershipTransferError::InvalidTarget { .. } => {
            (StatusCode::BAD_REQUEST, TransferRejection::InvalidTarget)
        }
        LeadershipTransferError::Raft { .. } => (
            StatusCode::INTERNAL_SERVER_ERROR,
            TransferRejection::RaftStopped,
        ),
    };
    (
        status,
        axum::Json(TransferLeaderResponse {
            raft_group_id: group,
            from: Some(from),
            to: Some(to),
            current_leader: leader,
            transferred: false,
            rejection: Some(rejection),
            reason: Some(error.to_string()),
        }),
    )
        .into_response()
}

async fn confirm_raft_quorum(
    State(state): State<HttpState>,
    Path(group): Path<u64>,
    headers: axum::http::HeaderMap,
) -> Response {
    if let Some(response) = reject_admin_incarnation(&state, &headers) {
        return response;
    }
    let (group, _) = match resolve_raft_group(&state, group) {
        Ok(resolved) => resolved,
        Err(response) => return *response,
    };
    let Some(registry) = state.raft_registry() else {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    };
    match http_time::timeout(
        Duration::from_secs(10),
        registry.confirm_quorum_prefix(group),
    )
    .await
    {
        Ok(Ok(proof)) => axum::Json(proof).into_response(),
        Ok(Err(error)) => {
            let status = match &error {
                ursula_raft::QuorumProofError::NotRegistered { .. } => StatusCode::NOT_FOUND,
                ursula_raft::QuorumProofError::Read { .. } => StatusCode::SERVICE_UNAVAILABLE,
                _ => StatusCode::CONFLICT,
            };
            (status, error.to_string()).into_response()
        }
        Err(_) => (StatusCode::GATEWAY_TIMEOUT, "quorum confirmation timed out").into_response(),
    }
}

use ursula_proto::admin::SelfElectionRequest;

async fn request_raft_self_election(
    State(state): State<HttpState>,
    Path(group_id): Path<u64>,
    axum::Json(request): axum::Json<SelfElectionRequest>,
) -> Response {
    let (group_id, raft) = match resolve_raft_group(&state, group_id) {
        Ok(resolved) => resolved,
        Err(response) => return *response,
    };
    let observed = raft.metrics().borrow_watched().clone();
    if observed.current_term != request.current_term {
        return (
            StatusCode::CONFLICT,
            "self-election term changed; observe again",
        )
            .into_response();
    }
    let Some(registry) = state.raft_registry() else {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    };
    // OpenRaft checks the persisted vote before accepting the transfer. A
    // synthetic vote for this node is not the vote it actually observed.
    let transfer = openraft::raft::TransferLeaderRequest::new(observed.vote, observed.id, None);
    match registry.handle_transfer_leader(group_id, transfer).await {
        Ok(Ok(())) => StatusCode::NO_CONTENT.into_response(),
        Ok(Err(error)) => (
            StatusCode::CONFLICT,
            format!("self-election refused: {error}"),
        )
            .into_response(),
        Err(error) => (
            StatusCode::CONFLICT,
            format!("self-election refused: {error}"),
        )
            .into_response(),
    }
}

pub(crate) fn parse_raft_group_id(raw: u64) -> Result<RaftGroupId, std::num::TryFromIntError> {
    u32::try_from(raw).map(RaftGroupId)
}

/// Operator recovery when a majority of a group's voters are gated after a
/// crash may have cost them their unsynced tail: open this node's stalled
/// recovery gate for the group, accepting that its replica may be missing
/// entries it acknowledged, so it votes and campaigns with the log it holds.
/// The body names the replica's log as the operator saw it in the metrics;
/// a gate that is not stalled, or a replica whose log changed since, is
/// refused with `409 Conflict`. Run it on the gated replicas with the longest
/// logs until a leader is elected.
pub(crate) async fn accept_unsynced_loss(
    State(state): State<HttpState>,
    Path(raft_group_id): Path<u64>,
    axum::Json(expected): axum::Json<ursula_proto::admin::AcceptUnsyncedLossRequest>,
) -> Response {
    let (raft_group_id, _raft) = match resolve_raft_group(&state, raft_group_id) {
        Ok(resolved) => resolved,
        Err(response) => return *response,
    };
    let Some(registry) = state.raft_registry() else {
        return (
            StatusCode::BAD_REQUEST,
            "raft registry is not configured for this server",
        )
            .into_response();
    };
    match registry
        .accept_unsynced_loss(raft_group_id, &expected)
        .await
    {
        Ok(report) => (StatusCode::OK, axum::Json(report)).into_response(),
        Err(err @ ursula_raft::RecoveryGateError::NotRegistered { .. }) => {
            (StatusCode::NOT_FOUND, err.to_string()).into_response()
        }
        Err(
            err @ (ursula_raft::RecoveryGateError::StoreClosed { .. }
            | ursula_raft::RecoveryGateError::NotStalled { .. }
            | ursula_raft::RecoveryGateError::MissingVoteFloor { .. }
            | ursula_raft::RecoveryGateError::ReplicaChanged { .. }),
        ) => (StatusCode::CONFLICT, err.to_string()).into_response(),
        Err(
            err @ (ursula_raft::RecoveryGateError::Record { .. }
            | ursula_raft::RecoveryGateError::StartAsFollower { .. }),
        ) => (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()).into_response(),
        Err(err @ ursula_raft::RecoveryGateError::OwnerStopped { .. }) => {
            (StatusCode::SERVICE_UNAVAILABLE, err.to_string()).into_response()
        }
    }
}

/// Resolves the raft registry, parses the group id, and looks up the live group
/// handle — the preamble shared by every raft admin endpoint. Returns the
/// appropriate error response (`400`/`404`) when any step fails, so handlers can
/// `?`-style early-return and focus on their actual operation.
fn resolve_raft_group(
    state: &HttpState,
    raft_group_id: u64,
) -> Result<(RaftGroupId, OwnerRaftHandle), Box<Response>> {
    let Some(registry) = state.raft_registry() else {
        return Err(Box::new(
            (
                StatusCode::BAD_REQUEST,
                "raft registry is not configured for this server",
            )
                .into_response(),
        ));
    };
    let Ok(raft_group_id) = parse_raft_group_id(raft_group_id) else {
        return Err(Box::new(
            (StatusCode::BAD_REQUEST, "invalid raft group id").into_response(),
        ));
    };
    let Some(raft) = registry.get(raft_group_id) else {
        return Err(Box::new(
            (StatusCode::NOT_FOUND, "raft group is not registered").into_response(),
        ));
    };
    Ok((raft_group_id, raft))
}

pub(crate) async fn create_stream(
    State(state): State<HttpState>,
    OriginalUri(uri): OriginalUri,
    Path(path): Path<StreamPath>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let stream_id = path.into_stream_id();
    create_stream_by_id(state, request_target(&uri), stream_id, headers, body).await
}

pub(crate) async fn create_stream_by_id(
    state: HttpState,
    request_target: String,
    stream_id: BucketStreamId,
    request_headers: HeaderMap,
    body: Bytes,
) -> Response {
    let content_type_explicit = has_content_type(&request_headers);
    let content_type = request_content_type(&request_headers);
    let (stream_ttl_seconds, stream_expires_at_ms) = match stream_lifetime(&request_headers) {
        Ok(lifetime) => lifetime,
        Err(response) => return *response,
    };
    let if_incarnation = match incarnation_precondition(&request_headers) {
        Ok(if_incarnation) => if_incarnation,
        Err(response) => return *response,
    };
    let mut request = CreateStreamRequest::new(stream_id.clone(), content_type.clone());
    request.if_incarnation = if_incarnation;
    request.content_type_explicit = content_type_explicit;
    request.now_ms = state.unix_time_ms();
    request.initial_payload = match normalize_http_write_payload(&content_type, body.clone(), true)
    {
        Ok(payload) => payload,
        Err(message) => return (StatusCode::BAD_REQUEST, message).into_response(),
    };
    request.close_after = stream_closed(&request_headers);
    request.stream_seq = match stream_seq(&request_headers) {
        Ok(stream_seq) => stream_seq,
        Err(message) => return (StatusCode::BAD_REQUEST, message).into_response(),
    };
    request.stream_ttl_seconds = stream_ttl_seconds;
    request.stream_expires_at_ms = stream_expires_at_ms;
    let producer = match producer_request(&request_headers) {
        Ok(producer) => producer,
        Err(message) => return (StatusCode::BAD_REQUEST, message).into_response(),
    };
    request.producer = producer.clone();
    if should_externalize_payload(&state, request.initial_payload.len(), true) {
        return create_stream_external_by_id(state, request_target, request, producer).await;
    }

    match state.runtime.create_stream(request).await {
        Ok(response) => create_stream_http_response(CreateStreamHttpResponseInput {
            response,
            stream_id: &stream_id,
            content_type: &content_type,
            stream_ttl_seconds,
            stream_expires_at_ms,
            producer: producer.as_ref(),
        }),
        Err(err) => runtime_error_or_leader_redirect_async(&state, err, &request_target).await,
    }
}

pub(crate) async fn create_stream_external_by_id(
    state: HttpState,
    request_target: String,
    mut request: CreateStreamRequest,
    producer: Option<ProducerRequest>,
) -> Response {
    let stream_id = request.stream_id.clone();
    let content_type = request.content_type.clone();
    let stream_ttl_seconds = request.stream_ttl_seconds;
    let stream_expires_at_ms = request.stream_expires_at_ms;
    let record_ends = request.canonical_record_ends();
    let payload = std::mem::take(&mut request.initial_payload);
    let external_payload = match stage_external_payload(&state, &stream_id, &payload).await {
        Ok(payload) => payload,
        Err(response) => return response,
    };
    let external_path = external_payload.s3_path.clone();
    let external_request =
        CreateStreamExternalRequest::from_create_request(request, external_payload, record_ends);

    match state.runtime.create_stream_external(external_request).await {
        Ok(response) => {
            if response.already_exists {
                // The live stream was not replaced; its initial payload is
                // another object (F5 cleanup rule).
                delete_unreferenced_staged_payload(&state, &external_path).await;
            }
            create_stream_http_response(CreateStreamHttpResponseInput {
                response,
                stream_id: &stream_id,
                content_type: &content_type,
                stream_ttl_seconds,
                stream_expires_at_ms,
                producer: producer.as_ref(),
            })
        }
        Err(err) => {
            cleanup_external_payload(&state, &external_path, &err).await;
            runtime_error_or_leader_redirect_async(&state, err, &request_target).await
        }
    }
}

pub(crate) async fn append_stream(
    State(state): State<HttpState>,
    OriginalUri(uri): OriginalUri,
    Path(path): Path<StreamPath>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let stream_id = path.into_stream_id();
    append_stream_by_id(state, request_target(&uri), stream_id, headers, body).await
}

#[tracing::instrument(
    name = "http.append",
    skip_all,
    fields(bucket = %stream_id.bucket_id, stream = %stream_id.stream_id),
)]
pub(crate) async fn append_stream_by_id(
    state: HttpState,
    request_target: String,
    stream_id: BucketStreamId,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    if let Err(response) = removed_surface::reject_removed_append_headers(&headers) {
        return *response;
    }
    let if_incarnation = match incarnation_precondition(&headers) {
        Ok(if_incarnation) => if_incarnation,
        Err(response) => return *response,
    };
    let close_after = stream_closed(&headers);

    if body.is_empty() && close_after {
        let producer = match producer_request(&headers) {
            Ok(producer) => producer,
            Err(message) => return (StatusCode::BAD_REQUEST, message).into_response(),
        };
        let stream_seq = match stream_seq(&headers) {
            Ok(stream_seq) => stream_seq,
            Err(message) => return (StatusCode::BAD_REQUEST, message).into_response(),
        };
        return match state
            .runtime
            .close_stream(CloseStreamRequest {
                stream_id,
                stream_seq,
                producer: producer.clone(),
                now_ms: state.unix_time_ms(),
                if_incarnation,
            })
            .await
        {
            Ok(response) => {
                let mut headers = HeaderMap::new();
                insert_default_response_headers(&mut headers);
                insert_offset(&mut headers, response.next_offset);
                insert_incarnation(&mut headers, response.incarnation);
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
    request.if_incarnation = if_incarnation;
    request.close_after = close_after;
    request.stream_seq = match stream_seq(&headers) {
        Ok(stream_seq) => stream_seq,
        Err(message) => return (StatusCode::BAD_REQUEST, message).into_response(),
    };
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
    let record_ends = request.canonical_record_ends();
    let payload = std::mem::take(&mut request.payload);
    let external_payload = match stage_external_payload(&state, &stream_id, &payload).await {
        Ok(payload) => payload,
        Err(response) => return response,
    };
    let external_path = external_payload.s3_path.clone();
    let external_request =
        AppendExternalRequest::from_append_request(request, external_payload, record_ends);
    match state.runtime.append_external(external_request).await {
        Ok(response) => {
            if response.deduplicated {
                // The original append committed with its own object; this
                // retry's object is referenced by nothing (F5 cleanup rule).
                delete_unreferenced_staged_payload(&state, &external_path).await;
            }
            append_http_response(response)
        }
        Err(err) => {
            cleanup_external_payload(&state, &external_path, &err).await;
            runtime_error_or_leader_redirect_async(&state, err, &request_target).await
        }
    }
}

pub(crate) async fn delete_stream(
    State(state): State<HttpState>,
    OriginalUri(uri): OriginalUri,
    Path(path): Path<StreamPath>,
    headers: HeaderMap,
) -> Response {
    let stream_id = path.into_stream_id();
    delete_stream_by_id(state, request_target(&uri), stream_id, headers).await
}

pub(crate) async fn delete_stream_by_id(
    state: HttpState,
    request_target: String,
    stream_id: BucketStreamId,
    headers: HeaderMap,
) -> Response {
    let if_incarnation = match incarnation_precondition(&headers) {
        Ok(if_incarnation) => if_incarnation,
        Err(response) => return *response,
    };
    match state
        .runtime
        .delete_stream(DeleteStreamRequest {
            stream_id,
            if_incarnation,
        })
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
    Path(path): Path<StreamPath>,
    headers: HeaderMap,
) -> Response {
    let stream_id = path.into_stream_id();
    head_stream_by_id(state, request_target(&uri), stream_id, headers).await
}

#[tracing::instrument(
    name = "http.head",
    skip_all,
    fields(bucket = %stream_id.bucket_id, stream = %stream_id.stream_id),
)]
pub(crate) async fn head_stream_by_id(
    state: HttpState,
    request_target: String,
    stream_id: BucketStreamId,
    request_headers: HeaderMap,
) -> Response {
    let if_incarnation = match incarnation_precondition(&request_headers) {
        Ok(if_incarnation) => if_incarnation,
        Err(response) => return *response,
    };
    match state
        .runtime
        .head_stream(HeadStreamRequest {
            stream_id,
            now_ms: state.unix_time_ms(),
            linearizable: true,
            read_index: None,
        })
        .await
    {
        Ok(response) => {
            if let Some(refused) = read_precondition_failed(if_incarnation, response.created_at_ms)
            {
                return refused;
            }
            let mut headers = HeaderMap::new();
            insert_default_response_headers(&mut headers);
            insert_content_type(&mut headers, &response.content_type);
            insert_offset(&mut headers, response.tail_offset);
            insert_padded_offset(
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
            if let Some(snapshot_offset) = response.snapshot_offset {
                insert_snapshot_offset(&mut headers, snapshot_offset);
            }
            if let Some(snapshot_digest) = response.snapshot_digest {
                insert_snapshot_digest(&mut headers, &snapshot_digest);
            }
            insert_padded_offset(
                &mut headers,
                HEADER_STREAM_RETAINED_OFFSET,
                response.retained_offset,
            );
            insert_incarnation(&mut headers, response.created_at_ms);
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
    Path(path): Path<StreamPath>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> Response {
    let stream_id = path.into_stream_id();
    read_stream_by_id(state, request_target(&uri), stream_id, headers, raw_query).await
}

#[tracing::instrument(
    name = "http.read",
    skip_all,
    fields(bucket = %stream_id.bucket_id, stream = %stream_id.stream_id),
)]
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
    if let Err(response) = removed_surface::reject_removed_read_parameters(&query) {
        return *response;
    }
    let if_incarnation = match incarnation_precondition(&headers) {
        Ok(if_incarnation) => if_incarnation,
        Err(response) => return *response,
    };
    let live_mode = query.get("live").map(String::as_str);
    let leader_only = match query.get("consistency").map(String::as_str) {
        None | Some("local") => false,
        Some("leader") => true,
        Some(_) => return (StatusCode::BAD_REQUEST, "invalid consistency").into_response(),
    };
    if leader_only && live_mode.is_some() {
        return (
            StatusCode::BAD_REQUEST,
            "leader consistency is available only for catch-up reads",
        )
            .into_response();
    }
    let offset_is_now = query.get("offset").is_some_and(|offset| offset == "now");
    if live_mode.is_some() && !query.contains_key("offset") {
        return (
            StatusCode::BAD_REQUEST,
            "live reads require a start position",
        )
            .into_response();
    }
    // A live read takes every lookup from its owner check (D10): `offset=now`,
    // existence, content type and incarnation come from state this replica
    // read at or after the read index it confirmed, never from a later HEAD
    // that a just-elected leader could answer from a lagging state.
    let (live_owner, offset) = if matches!(live_mode, Some("sse" | "long-poll")) {
        let start = if offset_is_now {
            None
        } else {
            match parse_read_offset(query.get("offset").map(String::as_str)) {
                Ok(offset) => Some(offset),
                Err(response) => return *response,
            }
        };
        let owner = match state
            .runtime
            .open_live_read(stream_id.clone(), state.unix_time_ms())
            .await
        {
            Ok(owner) => owner,
            Err(err) => {
                return runtime_error_or_leader_redirect_async(&state, err, &request_target).await;
            }
        };
        let offset = start.unwrap_or(owner.head.tail_offset);
        (Some(owner), offset)
    } else {
        match read_offset(
            &state,
            &stream_id,
            query.get("offset").map(String::as_str),
            &request_target,
            leader_only,
        )
        .await
        {
            Ok(offset) => (None, offset),
            Err(response) => return *response,
        }
    };
    // F11: every read is capped at READ_MAX_RESPONSE_BYTES; a capped read may
    // end inside a message. `max_bytes` keeps the base protocol's lenient
    // parsing.
    let max_len = query
        .get("max_bytes")
        .map_or(usize::MAX, |raw| raw.parse::<usize>().unwrap_or(usize::MAX))
        .min(READ_MAX_RESPONSE_BYTES);

    match (live_mode, live_owner) {
        (Some("sse"), Some(owner)) => {
            return sse_stream(
                state,
                stream_id,
                owner,
                offset,
                max_len,
                &query,
                if_incarnation,
            );
        }
        (Some("long-poll"), Some(owner)) => {
            return long_poll_stream(
                state,
                request_target,
                stream_id,
                owner,
                offset,
                max_len,
                &query,
                headers,
                if_incarnation,
            )
            .await;
        }
        (Some(_), _) => return (StatusCode::BAD_REQUEST, "invalid live mode").into_response(),
        (None, _) => {}
    }

    let read = state
        .runtime
        .read_stream(ReadStreamRequest {
            stream_id: stream_id.clone(),
            offset,
            max_len,
            now_ms: state.unix_time_ms(),
            leader_only,
            read_index: None,
        })
        .await;
    match read {
        Ok(response) => {
            if let Some(refused) = read_precondition_failed(if_incarnation, response.incarnation) {
                return refused;
            }
            if offset_is_now {
                offset_now_response(response)
            } else {
                read_response(response, &headers, None)
            }
        }
        Err(err) => runtime_error_or_leader_redirect_async(&state, err, &request_target).await,
    }
}

#[tracing::instrument(
    name = "http.snapshot_publish",
    skip_all,
    fields(bucket = %path.bucket, stream = %path.stream, snapshot_offset = %path.snapshot_offset),
)]
pub(crate) async fn publish_snapshot(
    State(state): State<HttpState>,
    OriginalUri(uri): OriginalUri,
    Path(path): Path<SnapshotPath>,
    headers: HeaderMap,
    body: Body,
) -> Response {
    if let Err(response) = removed_surface::reject_removed_snapshot_headers(&headers) {
        return *response;
    }
    let (stream_id, snapshot_offset) = path.into_parts();
    let snapshot_offset = match parse_snapshot_offset(&snapshot_offset) {
        Ok(offset) => offset,
        Err(response) => return *response,
    };
    let request_target = request_target(&uri);
    publish_snapshot_by_offset(
        state,
        request_target,
        stream_id,
        snapshot_offset,
        headers,
        body,
    )
    .await
}

/// The bare `{stream}/snapshot` path (the removed latest-snapshot redirect
/// and record-addressed publish) answers 405 with an empty `Allow`, not 404:
/// Loro's streams client reads a 404 as "no snapshot" (see
/// `removed_surface`).
pub(crate) async fn removed_latest_snapshot() -> Response {
    (
        StatusCode::METHOD_NOT_ALLOWED,
        [(axum::http::header::ALLOW, "")],
        "the latest-snapshot redirect was removed; read Stream-Snapshot-Offset from HEAD",
    )
        .into_response()
}

/// The stream incarnation an apply refusal names when a JSON snapshot or
/// retention offset needs a proposer-verified boundary (the byte before it
/// is not hot).
fn json_boundary_unverified(err: &RuntimeError) -> Option<u64> {
    if err.stream_error_code() != Some(StreamErrorCode::JsonBoundaryUnverified) {
        return None;
    }
    err.stream_error_context()
        .iter()
        .find_map(|context| match context {
            StreamErrorContext::StreamIncarnation { incarnation } => Some(*incarnation),
            _ => None,
        })
}

/// The JSON LF obligation, proposer side (AUD §2). Apply checks a JSON
/// snapshot or retention offset whose preceding byte is hot; for any other
/// offset it refuses with the stream's incarnation, and this reads that
/// byte on the leader (`consistency=leader`). LF passes, so the caller
/// proposes again pinned to the incarnation; any other byte answers 400; a
/// failed read answers 503 (fail closed), except that a read refused for
/// leadership gets the usual leader redirect or retry response. A delete and
/// recreate between this read and the second apply changes the incarnation,
/// so apply refuses it. Non-JSON streams and hot boundaries never reach this.
async fn verify_json_boundary(
    state: &HttpState,
    stream_id: &BucketStreamId,
    offset: u64,
    request_target: &str,
) -> Result<(), Response> {
    let read = state
        .runtime
        .read_stream(ReadStreamRequest {
            stream_id: stream_id.clone(),
            offset: offset.saturating_sub(1),
            max_len: 1,
            now_ms: state.unix_time_ms(),
            leader_only: true,
            read_index: None,
        })
        .await;
    let byte = match read {
        Ok(read) => read.payload.first().copied(),
        Err(err) if is_forward_to_leader(&err) => {
            return Err(runtime_error_or_leader_redirect_async(state, err, request_target).await);
        }
        Err(err) => {
            tracing::warn!(
                bucket = %stream_id.bucket_id,
                stream = %stream_id.stream_id,
                offset,
                error = %err,
                "JSON message boundary check failed"
            );
            None
        }
    };
    match byte {
        Some(b'\n') => Ok(()),
        Some(_) => Err((
            StatusCode::BAD_REQUEST,
            format!("offset {offset} is not a JSON message boundary"),
        )
            .into_response()),
        None => Err((
            StatusCode::SERVICE_UNAVAILABLE,
            format!("could not verify that offset {offset} is a JSON message boundary"),
        )
            .into_response()),
    }
}

async fn publish_snapshot_by_offset(
    state: HttpState,
    request_target: String,
    stream_id: BucketStreamId,
    snapshot_offset: u64,
    headers: HeaderMap,
    body: Body,
) -> Response {
    let if_incarnation = match incarnation_precondition(&headers) {
        Ok(if_incarnation) => if_incarnation,
        Err(response) => return *response,
    };
    let content_type = request_content_type(&headers);
    let (payload, cold_body) =
        match cold_snapshot::receive_snapshot_body(&state, &stream_id, &content_type, body).await {
            Ok(cold_snapshot::SnapshotUpload::Inline(payload)) => (payload, None),
            Ok(cold_snapshot::SnapshotUpload::Cold(cold_body)) => (Bytes::new(), Some(cold_body)),
            Err(response) => return response,
        };
    let staged_path = cold_body
        .as_ref()
        .map(|cold_body| cold_body.object.s3_path.clone());
    let mut request = PublishSnapshotRequest {
        stream_id: stream_id.clone(),
        snapshot_offset,
        content_type,
        payload,
        cold_body,
        now_ms: state.unix_time_ms(),
        expected_incarnation: None,
        if_incarnation,
    };
    let mut result = state.runtime.publish_snapshot(request.clone()).await;
    if let Some(incarnation) = result.as_ref().err().and_then(json_boundary_unverified) {
        if let Err(response) =
            verify_json_boundary(&state, &stream_id, snapshot_offset, &request_target).await
        {
            // Nothing references the staged body: apply refused it.
            if let Some(staged_path) = staged_path.as_deref() {
                delete_unreferenced_staged_payload(&state, staged_path).await;
            }
            return response;
        }
        request.expected_incarnation = Some(incarnation);
        request.now_ms = state.unix_time_ms();
        result = state.runtime.publish_snapshot(request).await;
    }
    if let (Err(err), Some(staged_path)) = (&result, staged_path.as_deref()) {
        // F5 cleanup rule: delete the staged body only after a definite
        // rejection; the orphan sweep reclaims it otherwise.
        cleanup_external_payload(&state, staged_path, err).await;
    }
    match result {
        Ok(response) => {
            let mut headers = HeaderMap::new();
            insert_default_response_headers(&mut headers);
            insert_snapshot_offset(&mut headers, response.snapshot_offset);
            insert_snapshot_digest(&mut headers, &response.snapshot_digest);
            insert_incarnation(&mut headers, response.incarnation);
            (StatusCode::NO_CONTENT, headers).into_response()
        }
        Err(err) => runtime_error_or_leader_redirect_async(&state, err, &request_target).await,
    }
}

pub(crate) async fn advance_retention(
    State(state): State<HttpState>,
    OriginalUri(uri): OriginalUri,
    Path(path): Path<RetentionPath>,
    headers: HeaderMap,
) -> Response {
    let (stream_id, retained_offset) = path.into_parts();
    let retained_offset = match parse_snapshot_offset(&retained_offset) {
        Ok(offset) => offset,
        Err(response) => return *response,
    };
    let if_incarnation = match incarnation_precondition(&headers) {
        Ok(if_incarnation) => if_incarnation,
        Err(response) => return *response,
    };
    let request_target = request_target(&uri);
    let mut request = AdvanceRetentionRequest {
        stream_id: stream_id.clone(),
        retained_offset,
        now_ms: state.unix_time_ms(),
        expected_incarnation: None,
        if_incarnation,
    };
    let mut result = state.runtime.advance_retention(request.clone()).await;
    if let Some(incarnation) = result.as_ref().err().and_then(json_boundary_unverified) {
        if let Err(response) =
            verify_json_boundary(&state, &stream_id, retained_offset, &request_target).await
        {
            return response;
        }
        request.expected_incarnation = Some(incarnation);
        request.now_ms = state.unix_time_ms();
        result = state.runtime.advance_retention(request).await;
    }
    match result {
        Ok(response) => {
            let mut headers = HeaderMap::new();
            insert_default_response_headers(&mut headers);
            insert_padded_offset(
                &mut headers,
                HEADER_STREAM_RETAINED_OFFSET,
                response.retained_offset,
            );
            insert_incarnation(&mut headers, response.incarnation);
            (StatusCode::NO_CONTENT, headers).into_response()
        }
        Err(err) => runtime_error_or_leader_redirect_async(&state, err, &request_target).await,
    }
}

#[tracing::instrument(
    name = "http.snapshot_read",
    skip_all,
    fields(bucket = %path.bucket, stream = %path.stream, snapshot_offset = %path.snapshot_offset),
)]
pub(crate) async fn read_snapshot(
    State(state): State<HttpState>,
    OriginalUri(uri): OriginalUri,
    Path(path): Path<SnapshotPath>,
    headers: HeaderMap,
) -> Response {
    let (stream_id, snapshot_offset) = path.into_parts();
    let snapshot_offset = match parse_snapshot_offset(&snapshot_offset) {
        Ok(offset) => offset,
        Err(response) => return *response,
    };
    let if_incarnation = match incarnation_precondition(&headers) {
        Ok(if_incarnation) => if_incarnation,
        Err(response) => return *response,
    };
    match state
        .runtime
        .read_snapshot(ReadSnapshotRequest {
            stream_id: stream_id.clone(),
            snapshot_offset: Some(snapshot_offset),
            now_ms: state.unix_time_ms(),
            read_index: None,
        })
        .await
    {
        Ok(mut response) => {
            if let Some(refused) = read_precondition_failed(if_incarnation, response.incarnation) {
                return refused;
            }
            let object = response.object.take();
            let mut rendered = snapshot_response(response);
            if let Some(object) = object {
                let len = object.payload_len;
                match cold_snapshot::cold_snapshot_body(&state, object).await {
                    Ok(body) => {
                        *rendered.body_mut() = body;
                        rendered
                            .headers_mut()
                            .insert(CONTENT_LENGTH, HeaderValue::from(len));
                    }
                    Err(response) => return response,
                }
            }
            rendered
        }
        Err(err) => {
            runtime_error_or_leader_redirect_async(&state, err, &request_target(&uri)).await
        }
    }
}

pub(crate) async fn bootstrap_stream(
    State(state): State<HttpState>,
    OriginalUri(uri): OriginalUri,
    Path(path): Path<StreamPath>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> Response {
    let query = match parse_query(raw_query.as_deref()) {
        Ok(query) => query,
        Err(response) => return *response,
    };
    let if_incarnation = match incarnation_precondition(&headers) {
        Ok(if_incarnation) => if_incarnation,
        Err(response) => return *response,
    };
    if query.contains_key("live") {
        return (
            StatusCode::BAD_REQUEST,
            "bootstrap does not support live reads",
        )
            .into_response();
    }
    let stream_id = path.into_stream_id();
    match state
        .runtime
        .bootstrap_stream(BootstrapStreamRequest {
            stream_id,
            now_ms: state.unix_time_ms(),
            read_index: None,
        })
        .await
    {
        Ok(response) => {
            if let Some(refused) = read_precondition_failed(if_incarnation, response.incarnation) {
                return refused;
            }
            match response.snapshot_object.clone() {
                None => bootstrap_response(response),
                Some(object) => cold_snapshot::bootstrap_response(&state, response, object).await,
            }
        }
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
    raw.parse::<u64>().map_err(|_invalid| {
        Box::new((StatusCode::BAD_REQUEST, "invalid snapshot offset").into_response())
    })
}

/// A catch-up read's start position. Its `offset=now` HEAD is linearizable
/// for `consistency=leader`; for `consistency=local` it reads the leader's
/// applied state, which a just-elected leader may not have brought up to
/// the committed tail, so it can resolve to a lagging tail.
pub(crate) async fn read_offset(
    state: &HttpState,
    stream_id: &BucketStreamId,
    raw: Option<&str>,
    request_target: &str,
    linearizable: bool,
) -> Result<u64, BoxResponse> {
    match raw {
        Some("now") => match state
            .runtime
            .head_stream(HeadStreamRequest {
                stream_id: stream_id.clone(),
                now_ms: state.unix_time_ms(),
                linearizable,
                read_index: None,
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
        raw => parse_read_offset(raw),
    }
}

/// A read's numeric start position: `-1` and an omitted offset are `0`.
/// `now` is resolved by the caller.
pub(crate) fn parse_read_offset(raw: Option<&str>) -> Result<u64, BoxResponse> {
    match raw {
        Some("-1") | None => Ok(0),
        Some(raw) => raw.parse::<u64>().map_err(|_invalid| {
            Box::new((StatusCode::BAD_REQUEST, "invalid offset").into_response())
        }),
    }
}

/// A long-poll with a `Stream-Incarnation` precondition (`if_incarnation`)
/// waits on that incarnation only: it ends with 412 when the stream it was
/// opened against is recreated, and with 404 when it is deleted (D12).
///
/// Every lookup comes from `owner` (D10): the waiter is pinned to the
/// incarnation the owner read and to its read index, an offset beyond the
/// owner's tail answers 416, and the timeout 204 answers from the waiter's
/// own state rather than a later HEAD.
pub(crate) async fn long_poll_stream(
    state: HttpState,
    request_target: String,
    stream_id: BucketStreamId,
    owner: LiveReadOwner,
    offset: u64,
    max_len: usize,
    query: &HashMap<String, String>,
    headers: HeaderMap,
    if_incarnation: Option<u64>,
) -> Response {
    let head = owner.head;
    if offset > head.tail_offset {
        let err = RuntimeError::GroupEngine {
            core_id: head.placement.core_id,
            raft_group_id: head.placement.raft_group_id,
            error: GroupEngineError::stream_with_next_offset(
                StreamErrorCode::OffsetOutOfRange,
                format!(
                    "offset {offset} is beyond stream '{stream_id}' tail {}",
                    head.tail_offset
                ),
                Some(head.tail_offset),
            ),
        };
        return runtime_error_or_leader_redirect_async(&state, err, &request_target).await;
    }
    let incarnation = if_incarnation.or((head.created_at_ms != 0).then_some(head.created_at_ms));
    let timeout_ms = long_poll_timeout_ms(query);
    let read = state.runtime.wait_read_stream_pinned(
        ReadStreamRequest {
            stream_id: stream_id.clone(),
            offset,
            max_len: max_len.max(1),
            now_ms: state.unix_time_ms(),
            leader_only: false,
            read_index: owner.read_index,
        },
        incarnation,
    );
    match http_time::timeout(Duration::from_millis(timeout_ms), read).await {
        Ok(Ok(response)) => {
            if let Some(refused) = read_precondition_failed(if_incarnation, response.incarnation) {
                return refused;
            }
            if response.payload.is_empty() && response.up_to_date {
                long_poll_no_content_response(&response, query.get("cursor").map(String::as_str))
            } else {
                read_response(
                    response,
                    &headers,
                    Some(query.get("cursor").map(String::as_str).unwrap_or("")),
                )
            }
        }
        Ok(Err(err)) => runtime_error_or_leader_redirect_async(&state, err, &request_target).await,
        // The requested offset (the 416 check above keeps it within the
        // owner's tail) and the incarnation the waiter is pinned to.
        // `Stream-Up-To-Date` and `Stream-Closed` hold as the owner read
        // them only when that offset is the owner's tail.
        Err(_) => {
            if let Some(refused) = read_precondition_failed(if_incarnation, head.created_at_ms) {
                return refused;
            }
            let at_tail = offset == head.tail_offset;
            let mut headers = HeaderMap::new();
            insert_default_response_headers(&mut headers);
            insert_offset(&mut headers, offset);
            insert_incarnation(&mut headers, head.created_at_ms);
            if at_tail {
                insert_static(&mut headers, HEADER_STREAM_UP_TO_DATE, "true");
            }
            if at_tail && head.closed {
                insert_static(&mut headers, HEADER_STREAM_CLOSED, "true");
            } else {
                insert_cursor(
                    &mut headers,
                    response_cursor(offset, query.get("cursor").map(String::as_str)),
                );
            }
            (StatusCode::NO_CONTENT, headers).into_response()
        }
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
    /// The incarnation a session with a `Stream-Incarnation` precondition
    /// was opened against (D12): a read of any other one ends it.
    incarnation: Option<u64>,
    /// The owner's confirmed read index, pinning the session's first read
    /// to the owner (D10); later reads are plain live reads.
    owner_read_index: Option<u64>,
}

/// An SSE session with a `Stream-Incarnation` precondition (`if_incarnation`)
/// answers 412 when the stream is another incarnation, and ends with an
/// `error` event when the stream it was opened against is deleted or
/// recreated (D12).
///
/// The session's content type and incarnation come from `owner` (D10), and
/// its first read is pinned to the owner's read index.
pub(crate) fn sse_stream(
    state: HttpState,
    stream_id: BucketStreamId,
    owner: LiveReadOwner,
    offset: u64,
    max_len: usize,
    query: &HashMap<String, String>,
    if_incarnation: Option<u64>,
) -> Response {
    let head = owner.head;
    if let Some(refused) = read_precondition_failed(if_incarnation, head.created_at_ms) {
        return refused;
    }

    let encode_base64 = should_base64_encode_sse_data(&head.content_type);
    state
        .http_metrics
        .sse_streams_opened
        .fetch_add(1, Ordering::Relaxed);
    // A text read needs room for one complete UTF-8 code point.
    let sse_max_len = if encode_base64 {
        max_len.max(1)
    } else {
        max_len.max(4)
    };
    let sse_state = SseState {
        runtime: state.runtime,
        http_metrics: state.http_metrics,
        wall_clock: state.wall_clock,
        stream_id,
        offset,
        max_len: sse_max_len,
        encode_base64,
        cursor: query.get("cursor").cloned(),
        initial_read: true,
        incarnation: if_incarnation,
        owner_read_index: owner.read_index,
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
            leader_only: false,
            read_index: if state.initial_read {
                state.owner_read_index
            } else {
                None
            },
        };
        let read = if state.initial_read {
            state.initial_read = false;
            state.runtime.read_stream(read_request).await
        } else {
            state
                .runtime
                .wait_read_stream_pinned(read_request, state.incarnation)
                .await
        };
        let mut read = match read {
            Ok(read)
                if state
                    .incarnation
                    .is_some_and(|incarnation| incarnation != read.incarnation) =>
            {
                state
                    .http_metrics
                    .sse_error_events
                    .fetch_add(1, Ordering::Relaxed);
                let event = format!(
                    "event: error\ndata:{}\n\n",
                    sse_safe_line("the stream was deleted and recreated (Stream-Incarnation)")
                );
                return Some((Ok::<Bytes, Infallible>(Bytes::from(event)), None));
            }
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
        clamp_sse_text_read(&mut read, state.encode_base64);

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
    insert_header_str(
        &mut headers,
        HEADER_STREAM_DATA_CONTENT_TYPE,
        http_read_content_type(&head.content_type),
    );
    insert_cache_control(&mut headers, "no-cache");
    insert_incarnation(&mut headers, head.created_at_ms);
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

pub(crate) fn parse_query(raw: Option<&str>) -> Result<HashMap<String, String>, BoxResponse> {
    let mut query = HashMap::new();
    let Some(raw) = raw else {
        return Ok(query);
    };
    for (key, value) in url::form_urlencoded::parse(raw.as_bytes()) {
        if key == "offset" && query.contains_key("offset") {
            return Err(Box::new(
                (StatusCode::BAD_REQUEST, "multiple offset parameters").into_response(),
            ));
        }
        query.insert(key.into_owned(), value.into_owned());
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

pub(crate) fn has_content_type(headers: &HeaderMap) -> bool {
    headers
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| !value.trim().is_empty())
}

pub(crate) fn normalize_content_type(value: &str) -> String {
    ursula_shard::normalize_content_type(value)
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
        .map_err(|_overflow| "stream-ttl is too large".to_owned())
}

pub(crate) fn parse_stream_expires_at(raw: &str) -> Result<u64, String> {
    let expires_at = DateTime::parse_from_rfc3339(raw)
        .map_err(|_invalid| "stream-expires-at must be an RFC3339 timestamp".to_owned())?;
    u64::try_from(expires_at.timestamp_millis())
        .map_err(|_before_epoch| "stream-expires-at must not be before the Unix epoch".to_owned())
}

/// The `Stream-Incarnation` request precondition (D12): `None` without the
/// header. The value is compared for equality only; anything that is not a
/// token the server could have issued (one canonical decimal integer) is
/// malformed and answers 400, as do repeated values.
pub(crate) fn incarnation_precondition(headers: &HeaderMap) -> Result<Option<u64>, BoxResponse> {
    let mut values = headers.get_all(HEADER_STREAM_INCARNATION).iter();
    let Some(value) = values.next() else {
        return Ok(None);
    };
    let malformed =
        || Box::new((StatusCode::BAD_REQUEST, "malformed Stream-Incarnation").into_response());
    if values.next().is_some() {
        return Err(malformed());
    }
    let Some(value) = value.to_str().ok().map(str::trim) else {
        return Err(malformed());
    };
    match value.parse::<u64>() {
        Ok(incarnation) if incarnation.to_string() == value => Ok(Some(incarnation)),
        _ => Err(malformed()),
    }
}

/// A read's `Stream-Incarnation` precondition (D12), checked against the
/// incarnation of the stream state that served it: `Some(412)` on a
/// mismatch, naming the current incarnation.
fn read_precondition_failed(expected: Option<u64>, served: u64) -> Option<Response> {
    let expected = expected?;
    (expected != served).then(|| {
        let mut headers = HeaderMap::new();
        insert_default_response_headers(&mut headers);
        insert_incarnation(&mut headers, served);
        (
            StatusCode::PRECONDITION_FAILED,
            headers,
            format!("Stream-Incarnation {expected} does not match the stream"),
        )
            .into_response()
    })
}

pub(crate) fn stream_closed(headers: &HeaderMap) -> bool {
    headers
        .get(HEADER_STREAM_CLOSED)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.eq_ignore_ascii_case("true"))
}

/// Maximum length in bytes of a `Producer-Id` or `Stream-Seq` value on every
/// HTTP write path (bounded-stream-state F3).
/// Both values are kept in replicated per-stream state, so their length must
/// be bounded at the edge.
pub(crate) const WRITE_IDENTIFIER_MAX_BYTES: usize = 256;

fn check_write_identifier_len(name: &str, value: &str) -> Result<(), String> {
    if value.len() > WRITE_IDENTIFIER_MAX_BYTES {
        return Err(format!(
            "{name} must be at most {WRITE_IDENTIFIER_MAX_BYTES} bytes"
        ));
    }
    Ok(())
}

pub(crate) fn stream_seq(headers: &HeaderMap) -> Result<Option<String>, String> {
    let Some(value) = headers
        .get(HEADER_STREAM_SEQ)
        .and_then(|value| value.to_str().ok())
        .filter(|value| !value.trim().is_empty())
    else {
        return Ok(None);
    };
    check_write_identifier_len(HEADER_STREAM_SEQ, value)?;
    Ok(Some(value.to_owned()))
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
    check_write_identifier_len(HEADER_PRODUCER_ID, producer_id)?;
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
        .map_err(|_invalid| format!("{name} must be a non-negative integer"))?;
    if value > MAX_JS_SAFE_INTEGER {
        return Err(format!("{name} must be <= {MAX_JS_SAFE_INTEGER}"));
    }
    Ok(value)
}

fn runtime_error_response(err: RuntimeError) -> Response {
    let status = runtime_error_status(&err);
    if status.is_server_error() {
        tracing::warn!(%status, error = %err, "runtime request failed");
    }
    let mut headers = HeaderMap::new();
    insert_default_response_headers(&mut headers);
    insert_retry_after_for_temporary(&mut headers, &err);
    insert_producer_error_headers(&mut headers, &err);
    insert_stream_error_headers(&mut headers, &err);
    insert_stream_error_offset(&mut headers, &err);
    (status, headers, err.to_string()).into_response()
}

fn insert_retry_after_for_temporary(headers: &mut HeaderMap, err: &RuntimeError) {
    if err.status() == ErrorStatus::Temporary {
        headers.insert(
            axum::http::header::RETRY_AFTER,
            HeaderValue::from_static("1"),
        );
    }
}

pub(crate) async fn runtime_error_or_leader_redirect_async(
    state: &HttpState,
    err: RuntimeError,
    request_target: &str,
) -> Response {
    let Some(router) = state.client_write_router() else {
        return runtime_error_response(err);
    };
    // Peer URLs are the configured client-reachable leader addresses
    // (`server.listen` when shared, or `server.cluster_listen` when split), so
    // they are valid redirect targets for reads and writes alike. 307 preserves
    // the method and body, so a redirected POST/PUT re-runs as a write on the
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
    err.leader_hint().is_some()
}

/// 503 + `Retry-After: 1` for a request that must reach the group leader but
/// cannot be redirected (purge) or has no known leader. Retryable.
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

#[cfg(test)]
mod base_contract_tests;
#[cfg(test)]
mod cold_snapshot_tests;
#[cfg(test)]
mod staging_cleanup_tests;

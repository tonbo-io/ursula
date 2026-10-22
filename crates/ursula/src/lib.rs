//! Ursula HTTP server: axum router, request handlers, response rendering,
//! plus the typed-config bootstrap constructors used by the `ursula` binary.
//!
//! Module map:
//!
//! - [`admission`]: client-plane ingress admission: a write's size before its
//!   handler, and its pressure checks, which a write routed before its body
//!   meets after routing.
//! - [`front_door`]: what a write's head decides before its body is read:
//!   routing, early answers and deferred admission.
//! - [`immutable_reads`]: the chunk and snapshot resources of one
//!   incarnation that a shared cache can serve (extensions Section 4).
//! - [`json_text`]: JSON Message Text (P1): validation, flattening and lexical
//!   minification of `application/json` write bodies.
//! - [`limits`]: the request and snapshot limits HEAD advertises and every
//!   `413` names.
//! - [`cold_snapshot`]: snapshot bodies in the cold tier: staging a publish
//!   body, and streaming a read's body or byte range.
//! - [`snapshot_parts`]: snapshots uploaded in parts: staging a part, and
//!   the commit that assembles and publishes them.
//! - [`staged_parts`]: the bound on a stream's staged, unused snapshot
//!   parts, counted by the leader.
//! - [`response_metadata`]: `Ursula-Protocol` and `Ursula-Cluster-Id` on
//!   every HEAD answer, and the request limit on a `413` that names none.
//! - [`placement_status`]: `GET /__ursula/placement`, this node's placement
//!   status, and the one answer of every placement refusal.
//! - [`render`]: response builders, header helpers, SSE/multipart rendering.
//! - [`bootstrap`]: typed-config `spawn_*_runtime` constructors and cold-flush worker.
//! - [`server`]: command arguments and the long-running server service entrypoint.

mod admission;
mod bootstrap;
mod cold_snapshot;
mod front_door;
mod immutable_reads;
pub mod json_text;
mod limits;
mod otel_metrics;
mod placement_status;
pub mod server;
mod http_time {
    #[cfg(madsim)]
    pub use madsim::time::timeout;
    #[cfg(not(madsim))]
    pub use tokio::time::timeout;
}
mod removed_surface;
mod render;
mod response_metadata;
mod snapshot_parts;
mod staged_parts;
mod wal_disk;

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::convert::Infallible;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;
#[cfg(not(madsim))]
use std::time::SystemTime;
#[cfg(not(madsim))]
use std::time::UNIX_EPOCH;

pub use admission::IngressAdmission;
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
pub use bootstrap::ClusterWal;
pub use bootstrap::SnapshotDriveSettings;
pub use bootstrap::SnapshotDriver;
pub use bootstrap::SpawnRuntimeError;
pub use bootstrap::SpawnedHosting;
pub use bootstrap::SpawnedMeta;
pub use bootstrap::SpawnedRuntime;
pub use bootstrap::Topology;
pub use bootstrap::spawn_runtime;
use chrono::DateTime;
use futures_util::stream;
use openraft::error::Fatal;
use openraft::rt::WatchReceiver;
use tower::ServiceBuilder;
use tower_http::compression::CompressionLayer;
use tower_http::compression::CompressionLevel;
use tower_http::compression::predicate::Predicate;
use tower_http::compression::predicate::SizeAbove;
use ursula_headers::ByteRange;
use ursula_headers::ClusterId;
use ursula_headers::ContentRange;
use ursula_headers::ErrorBody;
use ursula_headers::ErrorCode;
use ursula_headers::HeaderError as ProducerHeaderError;
use ursula_headers::IncarnationMismatch;
use ursula_headers::IncarnationPrecondition;
use ursula_headers::ProducerAnswer;
use ursula_headers::ProducerRequestHeaders;
use ursula_headers::ProducerWrite;
use ursula_headers::RoutingAnswer;
use ursula_headers::STREAM_SEQ_MAX_BYTES;
use ursula_headers::StreamSeq;
use ursula_headers::names;
use ursula_headers::value::Epoch;
use ursula_headers::value::Incarnation;
use ursula_proto::ContentFaultV1;
use ursula_proto::admin::PROCESS_INCARNATION_HEADER;
use ursula_proto::placement::ProcessIncarnation;
use ursula_raft::IncomingTransferError;
use ursula_raft::LeadershipShedReason;
use ursula_raft::OwnerRaftHandle;
use ursula_raft::RAFT_GRPC_APPEND_PATH;
use ursula_raft::RAFT_GRPC_APPEND_STREAM_PATH;
use ursula_raft::RAFT_GRPC_FULL_SNAPSHOT_PATH;
use ursula_raft::RAFT_GRPC_GENESIS_PROBE_PATH;
use ursula_raft::RAFT_GRPC_GROUP_READ_PATH;
use ursula_raft::RAFT_GRPC_GROUP_WRITE_PATH;
use ursula_raft::RAFT_GRPC_REJOIN_BARRIER_PATH;
use ursula_raft::RAFT_GRPC_TRANSFER_LEADER_PATH;
use ursula_raft::RAFT_GRPC_VOTE_PATH;
use ursula_raft::RaftGroupHandleRegistry;
use ursula_raft::RaftGroupMetricsSnapshot;
use ursula_raft::RaftGrpcService;
use ursula_raft::SelfElectionError;
use ursula_raft::SnapshotStartError;
use ursula_raft::UrsulaRaftTypeConfig;
use ursula_raft::raft_internal_proto;
use ursula_runtime::AdvanceRetentionRequest;
use ursula_runtime::AppendExternalRequest;
use ursula_runtime::AppendRequest;
use ursula_runtime::AppendResponse;
use ursula_runtime::BootstrapStreamRequest;
use ursula_runtime::CloseStreamRequest;
use ursula_runtime::CloseStreamResponse;
use ursula_runtime::CreateStreamExternalRequest;
use ursula_runtime::CreateStreamRequest;
use ursula_runtime::CreateStreamResponse;
use ursula_runtime::DeleteStreamRequest;
use ursula_runtime::EarlyAnswerQuery;
use ursula_runtime::ErrorStatus;
use ursula_runtime::ExternalPayloadRef;
use ursula_runtime::FollowerRead;
use ursula_runtime::GroupEngineError;
use ursula_runtime::HeadStreamRequest;
use ursula_runtime::ImportGroupStateRequest;
use ursula_runtime::LiveReadOwner;
use ursula_runtime::PlanColdFlushRequest;
use ursula_runtime::ProducerEarlyAnswer;
use ursula_runtime::ProducerRequest;
use ursula_runtime::ProposedUpload;
use ursula_runtime::PublishSnapshotRequest;
use ursula_runtime::ReadEnd;
use ursula_runtime::ReadSnapshotRequest;
use ursula_runtime::ReadStreamRequest;
use ursula_runtime::ReadStreamResponse;
use ursula_runtime::RuntimeError;
use ursula_runtime::ShardRuntime;
use ursula_runtime::StreamErrorCode;
use ursula_runtime::StreamErrorContext;
use ursula_runtime::UploadFence;
use ursula_runtime::new_external_payload_path;
use ursula_shard::BucketStreamId;
use ursula_shard::RaftGroupId;
use wal_disk::WalDiskMonitor;

use crate::admission::PendingAdmission;
use crate::front_door::BodyState;
use crate::front_door::HeadDecision;
use crate::render::COMPRESS_MIN_BYTES;
use crate::render::Uncompressed;
use crate::render::bootstrap_response;
use crate::render::clamp_sse_text_read;
use crate::render::compressible_media;
use crate::render::error_answer;
use crate::render::error_response;
use crate::render::fixed_error_answer;
use crate::render::fixed_error_response;
use crate::render::http_read_content_type;
use crate::render::if_none_match;
use crate::render::insert_cache_control;
use crate::render::insert_content_type;
use crate::render::insert_cursor;
use crate::render::insert_default_response_headers;
use crate::render::insert_header_str;
use crate::render::insert_incarnation;
use crate::render::insert_incarnation_mismatch;
use crate::render::insert_lifetime_headers;
use crate::render::insert_location;
use crate::render::insert_offset;
use crate::render::insert_padded_offset;
use crate::render::insert_producer_answer;
use crate::render::insert_producer_error_headers;
use crate::render::insert_static;
use crate::render::insert_stream_error_headers;
use crate::render::insert_stream_error_offset;
use crate::render::insert_stream_head;
use crate::render::long_poll_no_content_response;
use crate::render::normalize_http_write_payload;
use crate::render::offset_now_response;
use crate::render::producer_ack_answer;
use crate::render::producer_answer_failed;
use crate::render::read_response;
use crate::render::render_metrics;
use crate::render::render_sse_read;
use crate::render::response_cursor;
use crate::render::runtime_error_body;
use crate::render::should_base64_encode_sse_data;
use crate::render::snapshot_etag;
use crate::render::snapshot_head;
use crate::render::snapshot_published_response;
use crate::render::sse_error_event;
use crate::render::stream_head;
use crate::render::stream_seq_conflict;
use crate::render::uncompressed;
use crate::response_metadata::IdentityHeaders;
use crate::response_metadata::ResponseMetadataLayer;

type BoxResponse = Box<Response>;

const DEFAULT_CONTENT_TYPE: &str = "application/octet-stream";
const HEADER_STREAM_CURSOR: &str = "stream-cursor";
const HEADER_STREAM_EXPIRES_AT: &str = "stream-expires-at";
const HEADER_STREAM_COLD_HOT_START_OFFSET: &str = "stream-cold-hot-start-offset";
const HEADER_STREAM_DATA_CONTENT_TYPE: &str = "stream-data-content-type";
const HEADER_STREAM_SSE_DATA_ENCODING: &str = "stream-sse-data-encoding";
const HEADER_STREAM_SEQ: &str = "stream-seq";
const HEADER_STREAM_TTL: &str = "stream-ttl";
const HEADER_X_CONTENT_TYPE_OPTIONS: &str = "x-content-type-options";
const HEADER_CROSS_ORIGIN_RESOURCE_POLICY: &str = "cross-origin-resource-policy";
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
/// The protocol's request limit ([`ursula_headers::MAX_REQUEST_BYTES`]).
const MAX_HTTP_BODY_BYTES: usize = ursula_headers::MAX_REQUEST_BYTES as usize;
/// Server-side cap on one read response (bounded-stream-state F11), the same
/// 8 MiB that caps bootstrap updates. A request's `max_bytes` is clamped to
/// it; a capped response is partial (`Stream-Up-To-Date` absent), may end
/// inside a message of any content type, and the client continues from
/// `Stream-Next-Offset`.
const READ_MAX_RESPONSE_BYTES: usize = 8 * 1024 * 1024;
const DEFAULT_LONG_POLL_TIMEOUT_MS: u64 = 1_000;
const MAX_LONG_POLL_TIMEOUT_MS: u64 = 60_000;

#[derive(Debug, serde::Deserialize)]
pub(crate) struct StreamPath {
    bucket: String,
    stream: String,
}

impl StreamPath {
    pub(crate) fn into_stream_id(self) -> BucketStreamId {
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

/// The node's HTTP state, shared by every handler. Cloning it is one
/// reference count: the router clones it for every request (#602), so its
/// fields live behind one `Arc` rather than as many reference counts as it
/// has fields.
#[derive(Clone)]
pub struct HttpState(Arc<HttpStateFields>);

use http_state_fields::HttpStateFields;

impl std::ops::Deref for HttpState {
    type Target = HttpStateFields;

    fn deref(&self) -> &HttpStateFields {
        &self.0
    }
}

impl From<HttpStateFields> for HttpState {
    fn from(fields: HttpStateFields) -> Self {
        Self(Arc::new(fields))
    }
}

/// The fields of [`HttpState`], in a private module: the type is public
/// only so the state can deref to it, and no caller outside the crate can
/// name it.
mod http_state_fields {
    use std::sync::Arc;

    use ursula_headers::ClusterId;
    use ursula_proto::placement::ProcessIncarnation;
    use ursula_raft::RaftGroupHandleRegistry;
    use ursula_runtime::ShardRuntime;

    use crate::ClientWriteLeaderRouter;
    use crate::HttpMetrics;
    use crate::NodeMemoryMonitor;
    use crate::WallClock;
    use crate::snapshot_parts;
    use crate::staged_parts;
    use crate::wal_disk::WalDiskMonitor;

    /// The fields of [`HttpState`](crate::HttpState).
    #[derive(Clone)]
    pub struct HttpStateFields {
        pub(crate) process_incarnation: ProcessIncarnation,
        pub(crate) configured_node_id: Option<u64>,
        pub(crate) runtime: ShardRuntime,
        pub(crate) raft_registry: Option<RaftGroupHandleRegistry>,
        pub(crate) client_write_router: Option<ClientWriteLeaderRouter>,
        pub(crate) http_metrics: Arc<HttpMetrics>,
        pub(crate) wall_clock: Arc<dyn WallClock>,
        pub node_memory: NodeMemoryMonitor,
        pub(crate) external_payload_min_bytes: usize,
        pub(crate) wal_disk: WalDiskMonitor,
        /// The node's Raft WAL when it runs Raft: how it opened, and the
        /// clean shutdown at exit.
        pub(crate) raft_wal: Option<ursula_raft::RaftWal>,
        /// The node's meta runtime when it runs Raft, until shutdown takes
        /// it.
        pub(crate) meta: crate::bootstrap::MetaSlot,
        /// A Raft protocol (format-epoch) mismatch seen since start: readiness
        /// answers 503 `format_epoch_mismatch` until restart.
        pub(crate) format_epoch_mismatch: ursula_raft::FormatEpochMismatch,
        /// `Ursula-Cluster-Id`: the id bound to the node's WAL root, or one
        /// drawn for this process when the node runs without a WAL.
        pub(crate) cluster_id: Arc<ClusterId>,
        /// The parts commits this node runs (#581 section 3.1).
        pub(crate) snapshot_commits: snapshot_parts::SnapshotCommits,
        /// The staged, unused snapshot parts of the streams this node leads.
        pub(crate) staged_parts: staged_parts::StagedParts,
    }
}

impl HttpState {
    /// The fields, while the state is built: before the router holds it,
    /// the state is its only owner and this clones nothing.
    fn fields_mut(&mut self) -> &mut HttpStateFields {
        Arc::make_mut(&mut self.0)
    }

    /// This node's maintenance report (report v2, design 10): its data
    /// replicas against its placement view, its meta replica's status and
    /// its hosting driver's, and each replica's effective membership. A
    /// Raft node that knows no node id of its own reports none.
    fn raft_maintenance_report(&self) -> Option<ursula_raft::RaftMaintenanceReport> {
        let registry = self.raft_registry()?;
        let node_id = self.node_id()?;
        let reader = self.meta.status_reader();
        let hosting = self.meta.hosting_status();
        Some(ursula_raft::raft_maintenance_report(
            registry,
            node_id,
            reader.as_ref(),
            hosting.as_deref(),
        ))
    }

    /// Bridge the runtime's metrics to the global OTLP meter (export-time
    /// observable instruments; no hot-path cost). Inert when no OTLP meter
    /// provider is installed.
    pub fn register_otel_metrics(&self) {
        otel_metrics::register(&self.runtime.metrics());
        otel_metrics::register_cluster(self.cluster_id(), self.raft_registry.clone());
        if let Some(raft_wal) = &self.raft_wal {
            otel_metrics::register_wal_recovery(raft_wal.recovery_state());
        }
        if let Some(registry) = &self.raft_registry {
            otel_metrics::register_recovery_gates(registry.clone());
            otel_metrics::register_meta(registry.clone(), self.meta.wal_metrics());
        }
        otel_metrics::register_staged_snapshot_parts(self.clone());
    }

    /// Replaces the staged-parts tracker, which the orphan sweep then tells
    /// of the parts it deletes.
    #[cfg(test)]
    pub(crate) fn set_staged_parts(&mut self, staged_parts: staged_parts::StagedParts) {
        staged_parts.lead_by(self.raft_registry.clone());
        self.runtime
            .observe_swept_parts(Arc::new(staged_parts.clone()));
        self.fields_mut().staged_parts = staged_parts;
    }

    /// This node's id, when it has one: configured, or the one its peer
    /// cluster binds.
    pub(crate) fn node_id(&self) -> Option<u64> {
        self.configured_node_id
            .or_else(|| {
                self.client_write_router
                    .as_ref()
                    .and_then(ClientWriteLeaderRouter::node_id)
            })
            .or_else(|| {
                self.raft_registry().and_then(|registry| {
                    registry
                        .metrics_snapshot()
                        .first()
                        .map(|group| group.node_id)
                })
            })
    }

    /// The staged-part totals of the groups this node leads, in the term
    /// it leads each in now.
    pub(crate) fn staged_part_report(&self) -> Vec<staged_parts::GroupReport> {
        self.staged_parts.report()
    }

    /// The bytes of staged parts in the groups this node leads.
    pub(crate) fn staged_part_bytes_total(&self) -> u64 {
        self.staged_part_report()
            .iter()
            .flat_map(|group| group.buckets.values())
            .fold(0_u64, |total, bytes| total.saturating_add(*bytes))
    }

    /// Counts the staged parts of the groups this node leads, off the
    /// request path: seeds a group's totals when the node starts to lead
    /// it, woken by its Raft metrics. `None` without a cold store. Dropping
    /// the guard ends the task.
    pub(crate) fn spawn_staged_part_counting(&self) -> Option<AbortOnDrop> {
        let cold_store = self.runtime.cold_store()?;
        let state = self.clone();
        Some(AbortOnDrop(tokio::spawn(async move {
            loop {
                state.count_staged_parts_once(&cold_store).await;
                state.counting_wakeup().await;
            }
        })))
    }

    /// One round of the staged-part counting.
    pub(crate) async fn count_staged_parts_once(
        &self,
        cold_store: &ursula_runtime::ColdStoreHandle,
    ) {
        self.staged_parts
            .count_led_groups(&self.runtime, cold_store)
            .await;
    }

    /// Waits until this node's leadership of a registered group changes
    /// (its role or term, from the group's Raft metrics watch), an update
    /// starts a group's staged-part totals, or [`STAGED_PART_COUNTING_RETRY`]
    /// passes, after which a group registered meanwhile is watched and a
    /// failed seed runs again.
    async fn counting_wakeup(&self) {
        use openraft::rt::WatchReceiver;

        let mut changes = Vec::new();
        if let Some(registry) = self.raft_registry() {
            for group in 0..self.runtime.raft_group_count() {
                let Some(raft) = registry.get(RaftGroupId(group)) else {
                    continue;
                };
                let seen = raft.leadership();
                let mut metrics = raft.metrics();
                let fence = raft.fence().clone();
                changes.push(Box::pin(async move {
                    while metrics.changed().await.is_ok() {
                        if ursula_raft::GroupLeadership::of(&metrics.borrow_watched(), &fence)
                            != seen
                        {
                            return;
                        }
                    }
                    std::future::pending::<()>().await;
                }));
            }
        }
        let leadership = async {
            if changes.is_empty() {
                std::future::pending::<()>().await;
            } else {
                futures_util::future::select_all(changes).await;
            }
        };
        tokio::select! {
            biased;
            () = leadership => {}
            () = self.staged_parts.count_started() => {}
            () = tokio::time::sleep(STAGED_PART_COUNTING_RETRY) => {}
        }
    }

    pub(crate) fn with_configured_node_id(mut self, node_id: u64) -> Self {
        self.fields_mut().configured_node_id = Some(node_id);
        self
    }

    pub fn new(runtime: ShardRuntime) -> Self {
        let staged_parts = staged_parts::StagedParts::observing(&runtime, None);
        HttpStateFields {
            process_incarnation: unpersisted_incarnation(),
            configured_node_id: None,
            runtime,
            raft_registry: None,
            client_write_router: None,
            http_metrics: Arc::new(HttpMetrics::default()),
            wall_clock: Arc::new(SystemWallClock),
            node_memory: NodeMemoryMonitor::default(),
            external_payload_min_bytes: 1024 * 1024,
            snapshot_commits: snapshot_parts::SnapshotCommits::default(),
            staged_parts,
            wal_disk: WalDiskMonitor::default(),
            raft_wal: None,
            meta: crate::bootstrap::MetaSlot::default(),
            format_epoch_mismatch: ursula_raft::FormatEpochMismatch::global(),
            cluster_id: Arc::new(ursula_raft::generate_cluster_id()),
        }
        .into()
    }

    /// A Raft node of a cluster. Its redirects read the peers
    /// `raft_registry`'s peer cluster binds, and its `NotHosted` hints the
    /// registry's placement views ([`ClientWriteLeaderRouter`]). Its
    /// maintenance inventory reads the node's meta runtime, which the server
    /// sets: without one the node reports its placement unknown.
    pub fn with_raft_cluster(
        runtime: ShardRuntime,
        raft_registry: RaftGroupHandleRegistry,
        placement: Arc<dyn ursula_raft::PlacementInputs>,
    ) -> Self {
        let staged_parts =
            staged_parts::StagedParts::observing(&runtime, Some(raft_registry.clone()));
        let router = ClientWriteLeaderRouter::new(raft_registry.clone(), placement);
        HttpStateFields {
            process_incarnation: unpersisted_incarnation(),
            configured_node_id: None,
            runtime,
            raft_registry: Some(raft_registry),
            client_write_router: Some(router),
            http_metrics: Arc::new(HttpMetrics::default()),
            wall_clock: Arc::new(SystemWallClock),
            node_memory: NodeMemoryMonitor::default(),
            external_payload_min_bytes: 1024 * 1024,
            snapshot_commits: snapshot_parts::SnapshotCommits::default(),
            staged_parts,
            wal_disk: WalDiskMonitor::default(),
            raft_wal: None,
            meta: crate::bootstrap::MetaSlot::default(),
            format_epoch_mismatch: ursula_raft::FormatEpochMismatch::global(),
            cluster_id: Arc::new(ursula_raft::generate_cluster_id()),
        }
        .into()
    }

    pub fn with_wall_clock(mut self, wall_clock: impl WallClock) -> Self {
        self.fields_mut().wall_clock = Arc::new(wall_clock);
        self
    }

    pub fn with_wall_clock_handle(mut self, wall_clock: Arc<dyn WallClock>) -> Self {
        self.fields_mut().wall_clock = wall_clock;
        self
    }

    pub fn with_external_payload_min_bytes(mut self, min_bytes: usize) -> Self {
        self.fields_mut().external_payload_min_bytes = min_bytes;
        self
    }

    /// Record the raft WAL backend so it appears in the metrics JSON.
    pub(crate) fn with_wal_disk_monitor(mut self, monitor: WalDiskMonitor) -> Self {
        self.fields_mut().wal_disk = monitor;
        self
    }

    /// Records the node's Raft WAL: the metrics JSON reports how it opened,
    /// the server shuts it down cleanly, its root holds the cluster id, and
    /// the run it counted names this process
    /// ([`ursula_raft::RaftWal::incarnation`]) to the admin precondition.
    pub fn with_raft_wal(mut self, raft_wal: Option<ursula_raft::RaftWal>) -> Self {
        if let Some(wal) = &raft_wal {
            self.fields_mut().cluster_id = Arc::new(wal.cluster_id().clone());
            self.fields_mut().process_incarnation = wal.incarnation();
        }
        self.fields_mut().raft_wal = raft_wal;
        self
    }

    /// Records the node's meta runtime, which the server stops at shutdown.
    pub(crate) fn with_meta(mut self, meta: crate::bootstrap::MetaSlot) -> Self {
        self.fields_mut().meta = meta;
        self
    }

    /// The node's meta runtime, while it runs.
    pub(crate) fn meta(&self) -> &crate::bootstrap::MetaSlot {
        &self.meta
    }

    /// Sets the configured `server.cluster_id` of a node without a WAL. A
    /// node with one takes the id its WAL root holds ([`Self::with_raft_wal`]),
    /// which is the configured id when one is set.
    pub fn with_configured_cluster_id(mut self, cluster_id: Option<ClusterId>) -> Self {
        if let Some(cluster_id) = cluster_id {
            self.fields_mut().cluster_id = Arc::new(cluster_id);
        }
        self
    }

    /// The id this node sends as `Ursula-Cluster-Id`.
    pub fn cluster_id(&self) -> &ClusterId {
        &self.cluster_id
    }

    /// Peers refused on the Raft gRPC plane for another cluster id, in the
    /// shape readiness and metrics report them.
    fn peer_cluster_mismatches(&self) -> Vec<ursula_proto::admin::PeerClusterMismatch> {
        self.raft_registry()
            .map(|registry| registry.peer_cluster().mismatches())
            .unwrap_or_default()
    }

    /// Raft RPCs refused for a cluster-id mismatch since start, both ways.
    fn peer_cluster_refusals(&self) -> u64 {
        self.raft_registry()
            .map_or(0, |registry| registry.peer_cluster().refusals())
    }

    /// Inbound Raft RPCs refused since start because they named no cluster
    /// id.
    fn unidentified_peer_refusals(&self) -> u64 {
        self.raft_registry().map_or(0, |registry| {
            registry.peer_cluster().unidentified_refusals()
        })
    }

    pub(crate) fn raft_wal(&self) -> Option<&ursula_raft::RaftWal> {
        self.raft_wal.as_ref()
    }

    #[cfg(test)]
    pub(crate) fn with_format_epoch_mismatch(
        mut self,
        mismatch: ursula_raft::FormatEpochMismatch,
    ) -> Self {
        self.fields_mut().format_epoch_mismatch = mismatch;
        self
    }

    pub(crate) fn wal_disk_monitor(&self) -> WalDiskMonitor {
        self.wal_disk.clone()
    }

    /// Apply runtime-level config (memory monitor, payload threshold) derived
    /// from the typed configuration.  Replaces the hard-coded defaults set by
    /// the constructors.
    pub fn with_runtime_config(mut self, config: &ursula_config::RuntimeConfig) -> Self {
        self.fields_mut().node_memory = NodeMemoryMonitor::new(config);
        if let Some(min_size) = &config.external_payload_min_size {
            self.fields_mut().external_payload_min_bytes = usize::try_from(min_size.as_bytes())
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
    /// Producer writes answered `producer-expired` (the stream's epoch
    /// floor refused them).
    producer_expired_answers: AtomicU64,
    /// Parts commits answered `503 SnapshotCommitsBusy`: every copy slot
    /// was taken.
    snapshot_commits_busy: AtomicU64,
    /// Repeats of a running parts commit, answered `503
    /// SnapshotCommitInProgress`.
    snapshot_commits_in_progress: AtomicU64,
    /// Snapshot parts answered `429 STAGED_PARTS_LIMIT`: their stream held
    /// as many staged parts as it may.
    snapshot_parts_staged_limit: AtomicU64,
}

impl HttpMetrics {
    fn snapshot(&self) -> HttpMetricsSnapshot {
        HttpMetricsSnapshot {
            sse_streams_opened: self.sse_streams_opened.load(Ordering::Relaxed),
            sse_read_iterations: self.sse_read_iterations.load(Ordering::Relaxed),
            sse_data_events: self.sse_data_events.load(Ordering::Relaxed),
            sse_control_events: self.sse_control_events.load(Ordering::Relaxed),
            sse_error_events: self.sse_error_events.load(Ordering::Relaxed),
            producer_expired_answers: self.producer_expired_answers.load(Ordering::Relaxed),
            snapshot_commits_busy: self.snapshot_commits_busy.load(Ordering::Relaxed),
            snapshot_commits_in_progress: self.snapshot_commits_in_progress.load(Ordering::Relaxed),
            snapshot_parts_staged_limit: self.snapshot_parts_staged_limit.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Clone, Copy, Default, serde::Serialize)]
struct HttpMetricsSnapshot {
    sse_streams_opened: u64,
    sse_read_iterations: u64,
    sse_data_events: u64,
    sse_control_events: u64,
    sse_error_events: u64,
    producer_expired_answers: u64,
    snapshot_commits_busy: u64,
    snapshot_commits_in_progress: u64,
    snapshot_parts_staged_limit: u64,
}

/// The longest the staged-part counting waits for a leadership change
/// before it looks at every group again.
const STAGED_PART_COUNTING_RETRY: Duration = Duration::from_secs(60);

/// A task that ends when its guard drops.
pub(crate) struct AbortOnDrop(tokio::task::JoinHandle<()>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// Resolves where a request for a group this node cannot serve goes, as a
/// client-reachable base URL for a `307` (design 9):
///
/// - a leadership refusal that names a leader goes to that leader
///   ([`RoutingAnswer::NotLeader`]);
/// - a node that hosts no replica of the group, and a replica that names
///   no leader while it does not count as a voter (S10), go to a node of
///   the first tier of `ursula_control::leader_candidates` in this node's
///   latest placement view, rotating per answer
///   ([`RoutingAnswer::NotHosted`]). Without a view that tier is every
///   other `raft.peers` node;
/// - a replica that counts as a voter and names no leader gets no redirect:
///   its "no leader" holds for the group, and the caller answers a
///   retryable `503`.
///
/// Every URL is the one the bound `raft.peers` lists, read at each answer
/// ([`ursula_raft::PeerCluster::client_url`]): a peer's `client_url`, or
/// its Raft `url` when it serves both planes on `server.listen`. A node
/// `raft.peers` does not list is never a redirect target. This node's id is
/// the one its peer cluster binds.
#[derive(Clone, Debug)]
pub struct ClientWriteLeaderRouter {
    registry: RaftGroupHandleRegistry,
    placement: Arc<dyn ursula_raft::PlacementInputs>,
    /// The `NotHosted` answers given, which rotate the node each names.
    not_hosted_turns: Arc<AtomicUsize>,
}

impl ClientWriteLeaderRouter {
    /// Routes over `registry`'s peers, placement views and voter
    /// predicate. `placement`, the node's placement inputs, gives the
    /// maintenance inventory.
    pub fn new(
        registry: RaftGroupHandleRegistry,
        placement: Arc<dyn ursula_raft::PlacementInputs>,
    ) -> Self {
        Self {
            registry,
            placement,
            not_hosted_turns: Arc::default(),
        }
    }

    fn peers(&self) -> &ursula_raft::PeerCluster {
        self.registry.peer_cluster()
    }

    /// This node's id, as its peer cluster binds it.
    fn node_id(&self) -> Option<u64> {
        self.peers().identity().map(|identity| identity.node_id())
    }

    /// The voters this node's placement gives `group`.
    fn placed_voters(&self, group: RaftGroupId) -> BTreeSet<u64> {
        self.placement.nodes(group).voters()
    }

    /// The client URL `raft.peers` lists for `leader_id`. `None` for this
    /// node itself, and for a leader that `raft.peers` does not list, which
    /// the caller answers with a retryable `503`.
    fn peer_base(&self, leader_id: u64) -> Option<String> {
        if Some(leader_id) == self.node_id() {
            return None;
        }
        self.peers().client_url(leader_id)
    }

    /// The URL of `request_target` on `leader_id`, the target of a routing
    /// redirect (#581 section 6.1).
    pub(crate) fn leader_url(&self, leader_id: u64, request_target: &str) -> Option<String> {
        self.peer_base(leader_id)
            .map(|base| format!("{base}{request_target}"))
    }

    /// The one answer of a node that cannot name `group`'s leader: the
    /// base URL a `NotHosted` redirect names, or `None` for a retryable
    /// `503`. `None` when `replica` counts as a voter, and on the admin
    /// plane, whose redirect would name a client-plane URL.
    fn unhosted_answer(
        &self,
        group: RaftGroupId,
        replica: Replica,
        plane: Plane,
    ) -> Option<String> {
        if plane == Plane::Admin {
            return None;
        }
        if replica == Replica::Hosted
            && self.registry.counts_as_voter(group) == ursula_control::Counts::Voter
        {
            return None;
        }
        let me = ursula_proto::placement::NodeId::new(self.node_id()?);
        let peers = self.peers().directory();
        let ids = peers
            .node_ids()
            .map(ursula_proto::placement::NodeId::new)
            .collect();
        let view = self.registry.placement_watch().current();
        let candidates = ursula_control::leader_candidates(
            me,
            ursula_control::CandidatesOf::Group(group),
            &view,
            None,
            &ids,
        );
        let turn = self.not_hosted_turns.fetch_add(1, Ordering::Relaxed);
        let hint = candidates.not_hosted_hint(turn, |node| peers.contains(node.get()))?;
        peers.client_url(hint.get()).map(str::to_owned)
    }

    /// The `307` for `group` that names no leader, of a replica this node
    /// hosts: [`Self::unhosted_answer`] on the client plane.
    pub(crate) fn no_leader_redirect(
        &self,
        group: RaftGroupId,
        request_target: &str,
    ) -> Option<Response> {
        let base = self.unhosted_answer(group, Replica::Hosted, Plane::Client)?;
        redirect(
            &base,
            RoutingAnswer::NotHosted {
                group: ursula_headers::RaftGroup::from_u32(group.0),
            },
            request_target,
        )
    }

    /// The redirect for `err`, typed through `ursula-headers`, as the type
    /// docs route it. `None` for a refusal that gets no redirect, or one
    /// with no address: the caller answers a retryable `503` instead, so no
    /// redirect lacks the group, or the leader and its term.
    fn redirect_response(
        &self,
        err: &RuntimeError,
        request_target: &str,
        plane: Plane,
    ) -> Option<Response> {
        let (base, answer) = match err {
            RuntimeError::GroupNotHosted { raft_group_id, .. } => (
                self.unhosted_answer(*raft_group_id, Replica::Absent, plane)?,
                RoutingAnswer::NotHosted {
                    group: ursula_headers::RaftGroup::from_u32(raft_group_id.0),
                },
            ),
            RuntimeError::GroupEngine {
                raft_group_id,
                error,
                ..
            } => match &error.not_leader()?.leader {
                Some(leader) => (self.peer_base(leader.node_id)?, RoutingAnswer::NotLeader {
                    leader: ursula_headers::NodeId::from_u64(leader.node_id),
                    group: ursula_headers::RaftGroup::from_u32(raft_group_id.0),
                    term: ursula_headers::RaftTerm::from_u64(leader.term),
                }),
                None => (
                    self.unhosted_answer(*raft_group_id, Replica::Hosted, plane)?,
                    RoutingAnswer::NotHosted {
                        group: ursula_headers::RaftGroup::from_u32(raft_group_id.0),
                    },
                ),
            },
            _ => return None,
        };
        redirect(&base, answer, request_target)
    }
}

/// Whether the node that answers holds a replica of the group.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Replica {
    /// It hosts none ([`RuntimeError::GroupNotHosted`]).
    Absent,
    /// Its replica refused: the voter predicate decides its answer.
    Hosted,
}

/// The plane a request came in on. A `Location` always names a
/// client-plane URL, so only the client plane answers `NotHosted`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Plane {
    Client,
    Admin,
}

/// The `307` of `answer` to `request_target` on `base`.
fn redirect(base: &str, answer: RoutingAnswer, request_target: &str) -> Option<Response> {
    let mut headers = HeaderMap::new();
    insert_default_response_headers(&mut headers);
    let location = HeaderValue::from_str(&format!("{base}{request_target}")).ok()?;
    headers.insert(LOCATION, location);
    answer.render(|name, value| insert_header_str(&mut headers, name, value));
    Some(
        (
            StatusCode::TEMPORARY_REDIRECT,
            headers,
            "this node does not lead the stream's group",
        )
            .into_response(),
    )
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

    async fn genesis_probe(
        &self,
        request: tonic::Request<raft_internal_proto::GenesisProbeRequestV1>,
    ) -> Result<tonic::Response<raft_internal_proto::GenesisProbeAckV1>, tonic::Status> {
        raft_internal_proto::raft_internal_server::RaftInternal::genesis_probe(&self.raft, request)
            .await
    }
}

fn raft_grpc_service(
    state: HttpState,
    registry: RaftGroupHandleRegistry,
) -> ursula_raft::RaftGrpcServer<HttpRaftGrpcService> {
    let peer_cluster = registry.peer_cluster().clone();
    ursula_raft::raft_grpc_server(HttpRaftGrpcService::new(registry, state), &peer_cluster)
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

pub fn router_with_raft_cluster(
    runtime: ShardRuntime,
    raft_registry: RaftGroupHandleRegistry,
    placement: Arc<dyn ursula_raft::PlacementInputs>,
) -> Router {
    let state = HttpState::with_raft_cluster(runtime, raft_registry, placement);
    cluster_router_from_state(state.clone()).merge(client_router_with_admission(
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
/// maintenance drain, cold-flush trigger, bucket purge) plus read-only metrics,
/// readiness and usage so operator tooling works over a single tunnel. Production binds this to
/// `server.admin_listen` (loopback by default) — nodes expose no
/// cluster-mutation endpoints on the client or cluster planes.
pub fn admin_router(state: HttpState) -> Router {
    admin_ops_router(state.clone()).merge(
        Router::new()
            .route("/__ursula/metrics", get(metrics))
            .route(
                placement_status::PLACEMENT_PATH,
                get(placement_status::admin_placement_status),
            )
            .route(READINESS_PATH, get(readiness))
            .route("/__ursula/usage", get(bucket_usage))
            .with_state(state),
    )
}

/// The mutating admin routes without the read-only aliases. The single-router
/// convenience mergers use this directly because the client plane already
/// serves `/__ursula/metrics`, `/__ursula/ready` and `/__ursula/usage`.
fn admin_ops_router(state: HttpState) -> Router {
    let router = Router::new()
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
            "/__ursula/raft/{target}/snapshot",
            post(trigger_raft_snapshot),
        )
        .route("/__ursula/raft/{target}/purge", post(trigger_raft_purge))
        .route(
            "/__ursula/raft/{target}/membership",
            post(change_raft_membership),
        )
        .route(
            "/__ursula/raft/{target}/leader/transfer/{node_id}",
            post(transfer_raft_leader),
        )
        .route("/__ursula/raft/{target}/quorum", get(confirm_raft_quorum))
        .route(
            "/__ursula/raft/{target}/self-election",
            post(request_raft_self_election),
        )
        .route(
            "/__ursula/raft/{target}/recovery/accept-unsynced-loss",
            post(accept_unsynced_loss),
        )
        .route(
            "/__ursula/leadership-shed/maintenance",
            post(mark_maintenance_drain).delete(clear_maintenance_drain),
        )
        .route(
            placement_status::REPLACE_DISK_ROUTE,
            post(placement_status::replace_disk),
        );
    #[cfg(feature = "jemalloc-prof")]
    let router = router.route("/__ursula/debug/heap-profile", get(heap_profile));
    router
        .route_layer(middleware::from_fn_with_state(
            state.clone(),
            require_admin_incarnation,
        ))
        .layer(DefaultBodyLimit::max(MAX_HTTP_BODY_BYTES))
        .layer(ResponseMetadataLayer::admin())
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
    if identity.to_str().ok() != Some(state.process_incarnation.to_string().as_str()) {
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
        if let Err(response) = removed_surface::reject_removed_admin_headers(request.headers()) {
            return *response;
        }
        // A fenced node changes nothing: it leads no group and sends no
        // Raft request.
        if let Some(response) = fenced_answer(&state) {
            return response;
        }
        if let Some(response) = reject_admin_incarnation(&state, request.headers()) {
            return response;
        }
    }
    next.run(request).await
}

/// The answer to a request at a node whose fence does not admit (its disk
/// lease lapsed, or it shuts down): `503` with `Retry-After` and
/// `NO_LEADER`, as for a group without a known leader, since the node
/// proposes nothing for it. `None` while the fence admits.
fn fenced_answer(state: &HttpState) -> Option<Response> {
    if state.raft_wal.as_ref()?.fence().admits_now() {
        return None;
    }
    let mut headers = HeaderMap::new();
    headers.insert(
        axum::http::header::RETRY_AFTER,
        HeaderValue::from_static("1"),
    );
    Some(render::fixed_error_answer(
        StatusCode::SERVICE_UNAVAILABLE,
        ErrorCode::NoLeader,
        headers,
    ))
}

/// The front door's fence: every client request at a fenced node, local
/// reads included, gets [`fenced_answer`].
async fn refuse_while_fenced(
    State(state): State<HttpState>,
    request: Request<Body>,
    next: Next,
) -> Response {
    match fenced_answer(&state) {
        Some(answer) => answer,
        None => next.run(request).await,
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
            raft_grpc_service(state.clone(), raft_registry.clone()),
        )
        .route_service(
            RAFT_GRPC_GENESIS_PROBE_PATH,
            raft_grpc_service(state.clone(), raft_registry),
        )
        .route(LEADERSHIP_SHED_PATH, get(leadership_shed_status))
        .route(CLUSTER_PROBE_PATH, post(cluster_probe))
        .layer(DefaultBodyLimit::max(MAX_HTTP_BODY_BYTES))
        .layer(axum::middleware::map_request(with_peer_address))
        .with_state(state)
}

/// Hands a Raft RPC's peer address to tonic, whose cluster-id check logs
/// the address of a refused sender (`tonic::Request::remote_addr`). Axum
/// records it as `ConnectInfo`, where tonic does not look.
async fn with_peer_address<B>(mut request: axum::http::Request<B>) -> axum::http::Request<B> {
    if let Some(axum::extract::ConnectInfo(remote)) = request
        .extensions()
        .get::<axum::extract::ConnectInfo<std::net::SocketAddr>>()
        .copied()
    {
        request
            .extensions_mut()
            .insert(tonic::transport::server::TcpConnectInfo {
                local_addr: None,
                remote_addr: Some(remote),
            });
    }
    request
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
/// cluster plane) and answer 200. The cluster plane runs no ingress
/// admission, so a node under write pressure still answers its peers.
async fn cluster_probe(_body: Bytes) -> StatusCode {
    StatusCode::OK
}

/// What readiness decides on, besides the reports it carries.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct ReadinessFacts {
    pub(crate) superseded: bool,
    pub(crate) disk_lease_lost: bool,
    pub(crate) root_refused: bool,
    pub(crate) predecessor_wait: bool,
    pub(crate) format_epoch_mismatch: bool,
    pub(crate) wal_disk_pressure: bool,
    pub(crate) recovery_stalled: bool,
    pub(crate) recovery_ready: bool,
    pub(crate) raft_ready: bool,
}

/// The reason readiness names: the first of the causes that apply, in this
/// order. Peers of another cluster are never a reason.
pub(crate) fn readiness_reason(
    facts: ReadinessFacts,
) -> Option<ursula_proto::admin::ServingReadinessReason> {
    use ursula_proto::admin::ServingReadinessReason as Reason;
    if facts.superseded {
        Some(Reason::Superseded)
    } else if facts.disk_lease_lost {
        Some(Reason::DiskLeaseLost)
    } else if facts.root_refused {
        Some(Reason::RootRefused)
    } else if facts.predecessor_wait {
        Some(Reason::PredecessorWait)
    } else if facts.format_epoch_mismatch {
        Some(Reason::FormatEpochMismatch)
    } else if facts.wal_disk_pressure {
        Some(Reason::WalDiskPressure)
    } else if facts.recovery_stalled {
        Some(Reason::RecoveryStalled)
    } else if !facts.recovery_ready {
        Some(Reason::RecoveryGateClosed)
    } else if !facts.raft_ready {
        Some(Reason::RaftReplicaUnready)
    } else {
        None
    }
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
    // Non-Raft dev mode has no voter role. A Raft node that knows no node
    // id of its own cannot certify its local replica inventory.
    let raft_ready = match (state.raft_registry(), raft_maintenance.as_ref()) {
        (None, _) => true,
        (Some(_), Some(report)) => report.serving_ready(),
        (Some(_), None) => false,
    };
    // A node whose disk lease lapsed, or that was superseded, takes part in
    // no Raft traffic, and one that owes its predecessor wait grants no vote.
    let fence = state.raft_wal.as_ref().map(|wal| wal.fence());
    let fence_status = fence
        .as_ref()
        .map(|fence| fence.status(ursula_runtime::node_fence::fence_now()));
    let superseded = fence_status == Some(ursula_runtime::node_fence::FenceStatus::Superseded);
    let disk_lease_lost =
        fence_status == Some(ursula_runtime::node_fence::FenceStatus::DiskLeaseLost);
    let predecessor_wait = fence
        .as_ref()
        .is_some_and(ursula_runtime::NodeFence::owes_wait);
    // An unbound root under a node id that meta binds to another root.
    let root_refused = state.meta().status_reader().is_some_and(|reader| {
        matches!(
            reader.registration(),
            ursula_control::RegistrationStatus::RootRefused { .. }
        )
    });
    let ready = !superseded
        && !disk_lease_lost
        && !root_refused
        && !predecessor_wait
        && !disk.pressure
        && !format_epoch_mismatch
        && recovery_ready
        && raft_ready;
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
    (
        status,
        axum::Json(ursula_proto::admin::ServingReadiness {
            ready,
            reason: readiness_reason(ReadinessFacts {
                superseded,
                disk_lease_lost,
                root_refused,
                predecessor_wait,
                format_epoch_mismatch,
                wal_disk_pressure: disk.pressure,
                recovery_stalled: !stalled_groups.is_empty(),
                recovery_ready,
                raft_ready,
            }),
            cluster_id: state.cluster_id().clone(),
            // Configured peers refused for another cluster id. Reported
            // beside the reason, never as one: one misconfigured node must
            // not take its healthy peers out of service, nor hide why a node
            // is unready.
            peer_cluster_mismatches: state.peer_cluster_mismatches(),
            format_epoch_mismatch,
            recovery_barriers_ready: recovery_ready,
            raft_maintenance,
            recovery_stalled_groups: stalled_groups,
            wal_disk_pressure: disk.pressure,
            wal_available_bytes: disk.available_bytes,
            wal_min_available_bytes: disk.min_available_bytes,
            wal_resume_available_bytes: disk.resume_available_bytes,
            wal_disk_stat_errors: disk.stat_errors,
        }),
    )
        .into_response()
}

async fn leadership_shed_status(State(state): State<HttpState>) -> Response {
    axum::Json(
        state
            .raft_registry()
            .cloned()
            .unwrap_or_default()
            .participation_status(state.cluster_id().clone()),
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

/// Whether the compression layer may compress an answer: a finite JSON or
/// NDJSON body, never a snapshot answer, which offers byte ranges of its
/// identity body, and never an error answer ([`Uncompressed`]).
pub(crate) fn finite_record_response(
    _status: StatusCode,
    _version: Version,
    headers: &HeaderMap,
    extensions: &axum::http::Extensions,
) -> bool {
    headers
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .is_some_and(compressible_media)
        && !headers.contains_key(names::ACCEPT_RANGES)
        && extensions.get::<Uncompressed>().is_none()
}

pub fn client_router_with_admission(state: HttpState, admission: IngressAdmission) -> Router {
    let response_compression = CompressionLayer::new()
        .gzip(true)
        .quality(CompressionLevel::Fastest)
        .compress_when(SizeAbove::new(COMPRESS_MIN_BYTES).and(finite_record_response));

    Router::new()
        .route("/__ursula/metrics", get(metrics))
        .route(READINESS_PATH, get(readiness))
        .route("/__ursula/usage", get(bucket_usage))
        .route(
            placement_status::PLACEMENT_PATH,
            get(placement_status::placement_status),
        )
        .route("/{bucket}", put(create_bucket))
        // The bare path (the removed latest-snapshot redirect and the removed
        // record-addressed publish) answers 405 to every method, not a 404
        // that Loro's client would read as "no snapshot".
        .route(
            "/{bucket}/{stream}/snapshot",
            axum::routing::any(removed_latest_snapshot),
        )
        .route(
            front_door::SNAPSHOT_ROUTE,
            put(publish_snapshot).get(read_snapshot),
        )
        .route(
            front_door::SNAPSHOT_PARTS_ROUTE,
            post(snapshot_parts::stage_snapshot_part),
        )
        .route(
            immutable_reads::CHUNK_ROUTE,
            get(immutable_reads::read_chunk),
        )
        .route(
            immutable_reads::SNAPSHOT_ROUTE,
            get(immutable_reads::read_immutable_snapshot),
        )
        .route(
            front_door::STAGED_PARTS_ROUTE,
            axum::routing::delete(snapshot_parts::discard_snapshot_parts),
        )
        .route(
            "/{bucket}/{stream}/retention/{retained_offset}",
            put(advance_retention),
        )
        .route("/{bucket}/{stream}/bootstrap", get(bootstrap_stream))
        .route(
            front_door::STREAM_ROUTE,
            put(create_stream)
                .post(append_stream)
                .get(read_stream)
                .delete(delete_stream)
                .head(head_stream),
        )
        // One layer of the router, so a request crosses one boxed route
        // rather than one per layer: the router clones a boxed route, and
        // every route below it, for every request it calls (#602). Outermost
        // first.
        .layer(
            ServiceBuilder::new()
                .layer(response_compression)
                // An unknown path gets its error body from this layer only
                // because the client router's fallback is the one a merged
                // router keeps: the node merges the client router last, after
                // the cluster and admin routers (`error_body_tests` pins it on
                // that composition).
                .layer(ResponseMetadataLayer::client(IdentityHeaders::new(
                    state.cluster_id(),
                )))
                .layer(middleware::from_fn_with_state(
                    state.clone(),
                    refuse_while_fenced,
                ))
                .layer(admission)
                .layer(DefaultBodyLimit::max(MAX_HTTP_BODY_BYTES)),
        )
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
        return Err(fixed_error_response(
            StatusCode::NOT_IMPLEMENTED,
            ErrorCode::NoColdStore,
        ));
    };
    let s3_path = new_external_payload_path(stream_id);
    let object_size = cold_store
        .write_chunk(&s3_path, payload)
        .await
        .map_err(|err| {
            tracing::warn!(error = %err, "write external payload object");
            error_response(
                StatusCode::BAD_GATEWAY,
                ErrorCode::ColdStoreFailed,
                "writing the external payload object failed",
            )
        })?;
    Ok(ExternalPayloadRef {
        s3_path,
        payload_len: u64::try_from(payload.len()).expect("payload len fits u64"),
        object_size,
    })
}

/// Bounded-state F5 cleanup rule (ungated): a staged external object may be
/// deleted only when its append or create definitely did not commit. That is
/// a write the group engine applied nothing of
/// ([`GroupEngineError::applied_nothing`]: a typed stream error, which apply,
/// or a pre-proposal check, rejected on every replica alike, or a refusal
/// before proposal), a request the runtime refused before dispatch, or a
/// group this node does not host ([`refused_before_proposal`]). Every other
/// failure (a lost response, an entry OpenRaft appended and then discarded
/// (RT1), a transport or storage error, an untyped engine
/// error) may follow a committed proposal that references the object, so the
/// object is kept. An orphan sweep or stream GC reclaims it if nothing does.
///
/// [`GroupEngineError::applied_nothing`]: ursula_runtime::GroupEngineError::applied_nothing
pub(crate) fn staged_external_definitely_unreferenced(err: &RuntimeError) -> bool {
    match err {
        RuntimeError::GroupEngine { error, .. } => error.applied_nothing(),
        RuntimeError::InvalidRaftGroup { .. } => true,
        other => refused_before_proposal(other),
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
    } = input;
    let mut headers = HeaderMap::new();
    insert_default_response_headers(&mut headers);
    insert_content_type(&mut headers, content_type);
    insert_offset(&mut headers, response.next_offset);
    insert_location(&mut headers, stream_id);
    insert_incarnation(&mut headers, response.incarnation);
    insert_lifetime_headers(&mut headers, stream_ttl_seconds, stream_expires_at_ms);
    if response.closed {
        insert_static(&mut headers, names::STREAM_CLOSED, "true");
    }
    let status = if response.already_exists {
        StatusCode::OK
    } else {
        StatusCode::CREATED
    };
    (status, headers).into_response()
}

pub(crate) fn append_http_response(response: AppendResponse) -> Response {
    if let Some(ack) = response.producer.as_ref() {
        return producer_answer_response(
            producer_ack_answer(ack, response.incarnation),
            AnswerTo::Append,
        );
    }
    let mut headers = HeaderMap::new();
    insert_default_response_headers(&mut headers);
    insert_incarnation(&mut headers, response.incarnation);
    insert_offset(&mut headers, response.next_offset);
    if response.closed {
        insert_static(&mut headers, names::STREAM_CLOSED, "true");
    }
    (StatusCode::NO_CONTENT, headers).into_response()
}

/// The answer to a close-only write: `204` with the stream's tail and
/// `Stream-Closed`, or, for a producer close, the producer answer alone.
pub(crate) fn close_http_response(response: &CloseStreamResponse) -> Response {
    if let Some(ack) = response.producer.as_ref() {
        return producer_answer_response(
            producer_ack_answer(ack, response.incarnation),
            AnswerTo::CloseOnly,
        );
    }
    let mut headers = HeaderMap::new();
    insert_default_response_headers(&mut headers);
    insert_offset(&mut headers, response.next_offset);
    insert_incarnation(&mut headers, response.incarnation);
    insert_static(&mut headers, names::STREAM_CLOSED, "true");
    (StatusCode::NO_CONTENT, headers).into_response()
}

/// What a producer answer responds to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AnswerTo {
    /// A write with a body, or one whose body was not read.
    Append,
    /// An empty write with `Stream-Closed: true`. The base protocol answers
    /// it `204` even when it applied now.
    CloseOnly,
}

/// The one renderer of a producer answer, for the early and the late path:
/// the answer's own headers (`Stream-Incarnation` and the range on a
/// receipt, `Stream-Closed` only on a receipt of a write that closed the
/// stream), nothing else. A value outside the protocol's range answers
/// `500`.
fn producer_answer_response(
    answer: Result<ProducerAnswer, ProducerHeaderError>,
    to: AnswerTo,
) -> Response {
    let answer = match answer {
        Ok(answer) => answer,
        Err(err) => return producer_answer_failed(&err),
    };
    let mut headers = HeaderMap::new();
    insert_default_response_headers(&mut headers);
    // Only a DST negative control answers an applied write with a refusal,
    // to show the simulation catches a refusal that is not true; every
    // other build has no switch.
    if matches!(answer, ProducerAnswer::Receipt {
        delivery: ursula_headers::Delivery::Applied,
        ..
    }) && ursula_stream::sim_controls::off(
        ursula_stream::sim_controls::Defence::RefusalMeansUnapplied,
    ) {
        headers.insert(
            axum::http::header::RETRY_AFTER,
            HeaderValue::from_static("1"),
        );
        return fixed_error_answer(
            StatusCode::SERVICE_UNAVAILABLE,
            ErrorCode::NoLeader,
            headers,
        );
    }
    insert_producer_answer(&mut headers, &answer);
    let status = match (to, &answer) {
        (AnswerTo::CloseOnly, ProducerAnswer::Receipt { .. }) => StatusCode::NO_CONTENT,
        _ => StatusCode::from_u16(answer.status()).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
    };
    match answer.error_code() {
        Some(code) => error_answer(status, code, producer_refusal_message(&answer), headers),
        None => (status, headers).into_response(),
    }
}

/// The message of a producer refusal answered before the body, from its
/// typed answer: the late path's message names the producer too.
fn producer_refusal_message(answer: &ProducerAnswer) -> String {
    match answer {
        ProducerAnswer::Receipt { .. } | ProducerAnswer::ReceiptEvicted { .. } => String::new(),
        ProducerAnswer::EpochClosed(_) => "the producer's epoch is closed".to_owned(),
        ProducerAnswer::KeyConflict { epoch } => {
            format!("producer epoch {epoch} holds a write with another identity under this key")
        }
        ProducerAnswer::ProducerExpired { epoch } => {
            format!("the producer's epoch is not remembered; claim an epoch above {epoch}")
        }
        ProducerAnswer::StaleEpoch { epoch } => {
            format!("the producer's epoch is stale; current epoch is {epoch}")
        }
        ProducerAnswer::SeqGap { expected, received } => {
            format!("producer sequence {received} is out of order; expected {expected}")
        }
        ProducerAnswer::EpochNotClaimed => {
            "a new producer epoch must start at sequence 0".to_owned()
        }
    }
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
    // The purge erased the bucket's staged parts with its prefix.
    state.staged_parts.purged(&bucket);
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
        return leader_unknown_retry_response(err, Access::Write);
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
        Err(message) => {
            error_response(StatusCode::BAD_REQUEST, ErrorCode::InvalidBucketId, message)
        }
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
                    (entry.bucket_id, ursula_proto::admin::BucketUsageReport {
                        committed_append_bytes: entry.usage.committed_append_bytes,
                        committed_records: entry.usage.committed_records,
                        committed_write_units: entry.usage.committed_write_units,
                        retained_bytes: entry.usage.retained_bytes,
                        stream_count: entry.usage.stream_count,
                    })
                })
                .collect::<BTreeMap<_, _>>();
            axum::Json(ursula_proto::admin::UsageReport {
                version: ursula_proto::admin::SchemaVersion,
                node_id: state.node_id(),
                write_unit_bytes: ursula_runtime::COMMITTED_WRITE_UNIT_BYTES,
                buckets,
                raft_group_count: state.runtime.raft_group_count(),
                staged_snapshot_parts: state
                    .staged_part_report()
                    .into_iter()
                    .map(|group| ursula_proto::admin::GroupStagedParts {
                        raft_group_id: group.raft_group_id,
                        term: group.term,
                        counting_finished: group.counting_finished,
                        bytes: group.buckets,
                    })
                    .collect(),
            })
            .into_response()
        }
        Err(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("bucket usage read failed: {err}"),
        )
            .into_response(),
    }
}

#[derive(Debug, serde::Serialize)]
struct WalRecoveryReport {
    fsync: ursula_config::WalFsync,
    #[serde(flatten)]
    opening: ursula_raft::WalOpening,
    /// How the meta journal opened, by its own run-state class.
    meta: MetaWalRecoveryReport,
    /// Whether a process of an earlier run of the root may still run.
    predecessor: ursula_raft::wal::diagnostics::Predecessor,
}

/// How the meta journal opened, and why it could not be read, if it could
/// not.
#[derive(Debug, serde::Serialize)]
struct MetaWalRecoveryReport {
    #[serde(flatten)]
    opening: ursula_raft::WalOpening,
    damage: Option<String>,
}

impl WalRecoveryReport {
    fn new(raft_wal: &ursula_raft::RaftWal) -> Self {
        let meta = raft_wal.meta_opening();
        Self {
            fsync: raft_wal.fsync(),
            opening: raft_wal.opening(),
            meta: MetaWalRecoveryReport {
                opening: meta.opening(),
                damage: match &meta {
                    ursula_raft::MetaOpening::Decided(_) => None,
                    ursula_raft::MetaOpening::Damaged { damage, .. } => {
                        Some(ursula_runtime::ErrorChain(damage).to_string())
                    }
                },
            },
            predecessor: raft_wal.predecessor(),
        }
    }
}

/// The incarnation of a process without a WAL: a root drawn for this
/// start, run 0. It serves the admin precondition only.
fn unpersisted_incarnation() -> ProcessIncarnation {
    ProcessIncarnation {
        root: ursula_proto::placement::RootId::new(rand::random()),
        run: 0,
        nonce: rand::random(),
    }
}

pub(crate) async fn metrics(State(state): State<HttpState>) -> Response {
    let raft_groups = state
        .raft_registry()
        .map(RaftGroupHandleRegistry::metrics_snapshot)
        .unwrap_or_default();
    let wal_disk = state.wal_disk.snapshot();
    axum::Json(render::MetricsResponse {
        node: ursula_proto::admin::NodeMetrics {
            cluster_id: state.cluster_id().clone(),
            process_incarnation: state.process_incarnation,
            process_node_id: state
                .node_id()
                .or_else(|| raft_groups.first().map(|group| group.node_id)),
            groups: raft_groups
                .iter()
                .map(RaftGroupMetricsSnapshot::to_admin)
                .collect(),
            meta: state
                .raft_registry()
                .and_then(RaftGroupHandleRegistry::meta_metrics_snapshot)
                .as_ref()
                .map(RaftGroupMetricsSnapshot::to_admin),
            raft_maintenance: state.raft_maintenance_report(),
        },
        runtime: render_metrics(
            state.runtime.metrics().snapshot(),
            state.runtime.mailbox_snapshot(),
            state.http_metrics.snapshot(),
            &raft_groups,
            state.runtime.cold_store_info().as_ref(),
        ),
        configured_raft_group_count: state.runtime.raft_group_count(),
        group_state_gauges: group_state_gauges_json(&state).await,
        process_rss_bytes: state.node_memory.last_rss_bytes(),
        node_memory_abort_cap_bytes: state.node_memory.abort_cap_bytes().unwrap_or_default(),
        wal_recovery: state.raft_wal.as_ref().map(WalRecoveryReport::new),
        recovery_gates: state
            .raft_registry()
            .map(RaftGroupHandleRegistry::recovery_report),
        wal_available_bytes: wal_disk.available_bytes,
        wal_min_available_bytes: wal_disk.min_available_bytes,
        wal_resume_available_bytes: wal_disk.resume_available_bytes,
        wal_disk_pressure: wal_disk.pressure,
        wal_disk_stat_errors: wal_disk.stat_errors,
        raft_grpc_peer_cluster_refusals: state.peer_cluster_refusals(),
        raft_grpc_unidentified_peer_refusals: state.unidentified_peer_refusals(),
        peer_cluster_mismatches: state.peer_cluster_mismatches(),
        staged_snapshot_part_bytes: state.staged_part_bytes_total(),
        deleted_data: render::deleted_data(&raft_groups),
    })
    .into_response()
}

/// Upper bound on how long a metrics scrape waits for the per-group
/// bounded-state gauges; a busy or wedged group must not stall the scrape.
const GROUP_STATE_GAUGES_TIMEOUT: Duration = Duration::from_secs(2);

/// Per-group bounded-state gauges (`docs/architecture/bounded-stream-state.md`
/// Gauges) for `/__ursula/metrics`: one object per Raft group with the group id,
/// whether this node hosts it, and either the gauges or an error.
async fn group_state_gauges_json(state: &HttpState) -> render::GroupGaugeCollection {
    use crate::render::GroupGaugeCollection;
    use crate::render::GroupGaugeMetrics;
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
                Ok(gauges) => GroupGaugeMetrics::Hosted {
                    raft_group_id: group.0,
                    gauges,
                },
                Err(RuntimeError::GroupNotHosted { .. }) => GroupGaugeMetrics::NotHosted {
                    raft_group_id: group.0,
                },
                Err(error) => GroupGaugeMetrics::Failed {
                    raft_group_id: group.0,
                    error: error.to_string(),
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
            admin_error_or_leader_redirect(&state, err, &request_target(&uri), Access::Write)
        }
    }
}

pub(crate) const HEADER_BACKUP_FORMAT: &str = "x-ursula-backup-format";
pub(crate) const HEADER_BACKUP_BLAKE3: &str = "x-ursula-backup-blake3";
pub(crate) const HEADER_BACKUP_COMMIT_INDEX: &str = "x-ursula-backup-commit-index";

/// Cluster shape a backup client needs before iterating groups.
pub(crate) async fn backup_info(State(state): State<HttpState>) -> Response {
    (
        StatusCode::OK,
        axum::Json(ursula_proto::admin::BackupInfo {
            format_version: ursula_runtime::BACKUP_FORMAT_VERSION,
            raft_group_count: state.runtime.raft_group_count(),
        }),
    )
        .into_response()
}

/// Exports one group's complete stream state as a compact MessagePack
/// document of this binary's backup format.
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
            return admin_error_or_leader_redirect(
                &state,
                err,
                &request_target(&uri),
                Access::Read,
            );
        }
    };
    let body = match ursula_stream::msgpack::encode(&snapshot.stream_snapshot) {
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
        HeaderValue::from(ursula_runtime::BACKUP_FORMAT_VERSION),
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

/// Reads one backup group object addressed to group `raft_group_id`: the
/// group must exist here and the body must be a stream snapshot of this
/// server's backup format (E10). A refusal is the `400` to answer.
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
    // E10: the backup format is read first, so a group of another format
    // answers 400 instead of being decoded under this format's rules.
    let snapshot =
        ursula_stream::decode_backup_snapshot(body).map_err(|err| bad_request(err.to_string()))?;
    Ok((raft_group_id, snapshot))
}

/// The backup's BLAKE3 digest from the required `x-ursula-backup-blake3`
/// header, checked against the body: the import's key (#581 section 4.4).
/// A refusal is the `400` to answer.
fn backup_digest(
    headers: &HeaderMap,
    body: &[u8],
) -> Result<ursula_runtime::BackupDigest, Box<Response>> {
    let bad_request =
        |message: String| Box::new((StatusCode::BAD_REQUEST, message).into_response());
    let mut values = headers.get_all(HEADER_BACKUP_BLAKE3).iter();
    let (Some(value), None) = (values.next(), values.next()) else {
        return Err(bad_request(format!(
            "an import needs exactly one {HEADER_BACKUP_BLAKE3} header"
        )));
    };
    let claimed = blake3::Hash::from_hex(value.as_bytes())
        .map_err(|err| bad_request(format!("malformed {HEADER_BACKUP_BLAKE3}: {err}")))?;
    let actual = blake3::hash(body);
    if claimed != actual {
        return Err(bad_request(format!(
            "{HEADER_BACKUP_BLAKE3} {} does not match the body's BLAKE3 {}",
            claimed.to_hex(),
            actual.to_hex()
        )));
    }
    Ok(*actual.as_bytes())
}

/// Imports one group's backup snapshot into an empty group as a replicated
/// write that starts a new history (#581 section 4.4), keyed by the
/// backup's digest: a repeat answers `already: true`. Non-empty groups fail
/// closed with `409`; invalid payloads with `400`. The restored cluster
/// keeps its own raft identity and membership.
pub(crate) async fn import_backup_group(
    State(state): State<HttpState>,
    OriginalUri(uri): OriginalUri,
    Path(raft_group_id): Path<u64>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Response {
    let backup_digest = match backup_digest(&headers, &body) {
        Ok(digest) => digest,
        Err(response) => return *response,
    };
    let (raft_group_id, snapshot) = match decode_backup_group(&state, raft_group_id, &body) {
        Ok(decoded) => decoded,
        Err(response) => return *response,
    };
    match state
        .runtime
        .import_group_state(raft_group_id, ImportGroupStateRequest {
            snapshot: Box::new(snapshot),
            history: ursula_runtime::draw_history_id(),
            backup_digest,
            now_ms: state.unix_time_ms(),
        })
        .await
    {
        Ok(response) => (
            StatusCode::OK,
            axum::Json(ursula_proto::admin::BackupImportResponse {
                buckets: response.buckets,
                streams: response.streams,
                group_commit_index: response.group_commit_index,
                history: response.history,
                already: response.already,
            }),
        )
            .into_response(),
        Err(err) => {
            admin_error_or_leader_redirect(&state, err, &request_target(&uri), Access::Write)
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

/// How long an operator's snapshot request waits for its build to start.
const ADMIN_SNAPSHOT_START_WAIT: Duration = Duration::from_secs(10);
/// How long an operator's snapshot request waits for the group's running
/// build to end, and then, separately, for OpenRaft to publish the snapshot
/// that covers it.
const ADMIN_SNAPSHOT_PUBLISH_WAIT: Duration = Duration::from_secs(10);

pub(crate) async fn trigger_raft_snapshot(
    State(state): State<HttpState>,
    Path(target): Path<String>,
) -> Response {
    let target = match parse_raft_target(&target) {
        Ok(target) => target,
        Err(response) => return *response,
    };
    let Some(registry) = state.raft_registry() else {
        return *no_raft_registry();
    };
    let raft_group_id = match target {
        ursula_raft::RaftTarget::Meta => return trigger_meta_snapshot(registry).await,
        ursula_raft::RaftTarget::Data(group) => group,
    };
    let (raft_group_id, raft) = match resolve_registered_raft_group(registry, raft_group_id) {
        Ok(resolved) => resolved,
        Err(response) => return *response,
    };
    let coordinator = registry.snapshot_build_coordinator();
    let snapshot_log_id = raft.metrics().borrow_watched().last_applied;
    // Whether a snapshot OpenRaft published covers what the group had
    // applied when the request arrived, such as one a build that began
    // since then made. The coordinator also weighs the snapshot the group's
    // latest build made, which OpenRaft publishes only after that build
    // released its permit (#561).
    let published = |metrics: &openraft::RaftMetrics<UrsulaRaftTypeConfig>| {
        snapshot_log_id.as_ref().is_some_and(|wanted| {
            metrics
                .snapshot
                .as_ref()
                .is_some_and(|snapshot| snapshot >= wanted)
        })
    };
    let covering = async {
        let waited = raft
            .wait(Some(ADMIN_SNAPSHOT_START_WAIT))
            .metrics(published, "admin snapshot start covered")
            .await;
        if waited.is_err() {
            // The start's own wait ends the request.
            std::future::pending::<()>().await;
        }
    };
    if let Err(err) = coordinator
        .start_snapshot(
            raft_group_id.0,
            snapshot_log_id,
            ADMIN_SNAPSHOT_START_WAIT,
            || published(&raft.metrics().borrow_watched()),
            covering,
            || async { raft.trigger().snapshot().await },
        )
        .await
    {
        return snapshot_not_started(&err);
    }
    if let Some(snapshot_log_id) = &snapshot_log_id {
        // The covering snapshot may come from a build that still publishes
        // its reference or prunes retired snapshots, and OpenRaft publishes
        // it only once that build ended, so the wait for OpenRaft starts
        // then. A request OpenRaft's snapshot already covers waits for no
        // build. The build wait is bounded like the wait for OpenRaft, so a
        // build held up by its snapshot store (OpenDAL retries, then an
        // inline fallback) adds at most ADMIN_SNAPSHOT_PUBLISH_WAIT, and a
        // request whose build outlasts it waits for OpenRaft as before.
        if !published(&raft.metrics().borrow_watched()) {
            let build_ended = coordinator.build_ended(raft_group_id.0);
            if tokio::time::timeout(ADMIN_SNAPSHOT_PUBLISH_WAIT, build_ended)
                .await
                .is_err()
            {
                tracing::warn!(
                    raft_group_id = raft_group_id.0,
                    "admin snapshot: the group's build did not end within \
                     {ADMIN_SNAPSHOT_PUBLISH_WAIT:?}; waiting for its snapshot all the same"
                );
            }
        }
        if let Err(err) = raft
            .wait(Some(ADMIN_SNAPSHOT_PUBLISH_WAIT))
            .metrics(
                published,
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

/// An operator's snapshot of the meta group: its inline snapshot, which
/// takes no build permit of the data groups, covering what meta had
/// applied when the request arrived.
async fn trigger_meta_snapshot(registry: &RaftGroupHandleRegistry) -> Response {
    let Some(raft) = registry.meta_raft() else {
        return *no_meta_replica();
    };
    let snapshot_log_id = raft.metrics().borrow_watched().last_applied;
    if let Err(err) = raft.trigger().snapshot().await {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("trigger meta snapshot: {err}"),
        )
            .into_response();
    }
    if let Some(wanted) = snapshot_log_id
        && let Err(err) = raft
            .wait(Some(ADMIN_SNAPSHOT_PUBLISH_WAIT))
            .metrics(
                |metrics| {
                    metrics
                        .snapshot
                        .as_ref()
                        .is_some_and(|snapshot| snapshot >= &wanted)
                },
                format!("admin meta snapshot .snapshot >= {wanted}"),
            )
            .await
    {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("wait for meta snapshot: {err}"),
        )
            .into_response();
    }
    let metrics = raft.metrics().borrow_watched().clone();
    (
        StatusCode::OK,
        axum::Json(ursula_proto::admin::SnapshotResponse {
            raft_group_id: ursula_raft::meta::state_machine::META_GROUP.0,
            snapshot_index: metrics.snapshot.map(|log_id| log_id.index),
        }),
    )
        .into_response()
}

pub(crate) async fn trigger_raft_purge(
    State(state): State<HttpState>,
    Path(target): Path<String>,
    axum::extract::Query(query): axum::extract::Query<ursula_proto::admin::PurgeQuery>,
) -> Response {
    let raft_group_id = match data_group_target(&target, "purge") {
        Ok(group) => group,
        Err(response) => return *response,
    };
    let upto = query.upto;
    let Some(registry) = state.raft_registry() else {
        return *no_raft_registry();
    };
    let (raft_group_id, raft) = match resolve_registered_raft_group(registry, raft_group_id) {
        Ok(resolved) => resolved,
        Err(response) => return *response,
    };
    // The purge follows the hold rule of every purge (#669): it starts only
    // while no build of the group runs or waits for OpenRaft to publish it,
    // and holds the group's builds until it lands. OpenRaft purges at most
    // through its snapshot, so the hold is for that much.
    let coordinator = registry.snapshot_build_coordinator();
    let published = raft
        .metrics()
        .borrow_watched()
        .snapshot
        .map(|log_id| log_id.index);
    // A request dropped before its purge was triggered drops the guard,
    // which lifts the hold.
    let guard = match published.map(|published| {
        coordinator.hold_for_purge(
            raft_group_id.0,
            Some(published),
            upto.min(published),
            raft.metrics(),
        )
    }) {
        None => None,
        Some(Ok(guard)) => Some(guard),
        Some(Err(busy)) => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                [(axum::http::header::RETRY_AFTER, "1")],
                format!("{busy}; retry"),
            )
                .into_response();
        }
    };
    // A failed trigger drops the guard here, which lifts the hold.
    if let Err(err) = raft.trigger().purge_log(upto).await {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("trigger raft purge: {err}"),
        )
            .into_response();
    }
    if let Some(guard) = guard {
        guard.keep();
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

/// The answer to a membership request OpenRaft refused before appending it
/// while no leader is known: `503` with `Retry-After`, as while a new leader
/// waits for its quorum.
fn membership_no_leader_response(operation: &str) -> Response {
    (
        StatusCode::SERVICE_UNAVAILABLE,
        [(axum::http::header::RETRY_AFTER, "1")],
        format!("{operation}: no leader with a live quorum; retry"),
    )
        .into_response()
}

pub(crate) async fn change_raft_membership(
    State(state): State<HttpState>,
    Path(target): Path<String>,
    axum::extract::Query(query): axum::extract::Query<ursula_proto::admin::MembershipQuery>,
) -> Response {
    let raft_group_id = match data_group_target(&target, "membership") {
        Ok(group) => group,
        Err(response) => return *response,
    };
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

    match raft.replace_voters(voters.clone()).await {
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
        Err(err) => match err.api_error() {
            // Refused before anything was appended: retry on the leader.
            Some(openraft::error::ClientWriteError::ForwardToLeader(forward)) => {
                match forward.leader_id {
                    Some(leader) => (
                        StatusCode::CONFLICT,
                        axum::Json(ursula_proto::admin::MembershipResponse {
                            raft_group_id: raft_group_id.0,
                            current_leader: Some(leader),
                            changed: false,
                            reason: Some("not leader".to_owned()),
                            voter_ids: BTreeSet::new(),
                            log_index: None,
                        }),
                    )
                        .into_response(),
                    None => membership_no_leader_response("change raft membership"),
                }
            }
            _ => (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("change raft membership: {err}"),
            )
                .into_response(),
        },
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
    Path((target, node_id)): Path<(String, u64)>,
) -> Response {
    use ursula_proto::admin::TransferLeaderResponse;

    let target = match parse_raft_target(&target) {
        Ok(target) => target,
        Err(response) => return *response,
    };
    let Some(registry) = state.raft_registry() else {
        return *no_raft_registry();
    };
    let (raft_group_id, metrics) = match target {
        ursula_raft::RaftTarget::Meta => match registry.meta_raft() {
            Some(raft) => (
                ursula_raft::meta::state_machine::META_GROUP,
                raft.metrics().borrow_watched().clone(),
            ),
            None => return *no_meta_replica(),
        },
        ursula_raft::RaftTarget::Data(group) => {
            match resolve_registered_raft_group(registry, group) {
                Ok((group, raft)) => (group, raft.metrics().borrow_watched().clone()),
                Err(response) => return *response,
            }
        }
    };
    let transferred = match target {
        ursula_raft::RaftTarget::Meta => registry.transfer_meta_leader(node_id).await,
        ursula_raft::RaftTarget::Data(group) => registry.transfer_leader(group, node_id).await,
    };
    let raft_group_id = u64::from(raft_group_id.0);
    match transferred {
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
    Path(target): Path<String>,
    headers: axum::http::HeaderMap,
) -> Response {
    if let Some(response) = reject_admin_incarnation(&state, &headers) {
        return response;
    }
    let target = match parse_raft_target(&target) {
        Ok(target) => target,
        Err(response) => return *response,
    };
    let Some(registry) = state.raft_registry() else {
        return *no_raft_registry();
    };
    let confirmed = match target {
        ursula_raft::RaftTarget::Meta => {
            futures_util::FutureExt::boxed(registry.confirm_meta_quorum_prefix())
        }
        ursula_raft::RaftTarget::Data(group) => {
            if let Err(response) = resolve_registered_raft_group(registry, group) {
                return *response;
            }
            futures_util::FutureExt::boxed(registry.confirm_quorum_prefix(group))
        }
    };
    match http_time::timeout(Duration::from_secs(10), confirmed).await {
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

/// Starts an election on this node's replica of the group, accepted only
/// while the term is still the one in the body
/// ([`ursula_raft::RaftGroupHandleRegistry::request_self_election`]). A
/// replica that already leads the group or may not campaign, or whose term
/// or vote changed, is refused with `409 Conflict`. A replica whose Raft
/// core or apply loop has stopped answers `503 Service Unavailable` (a core
/// that panicked is not seen: its metrics keep their last values). A group
/// this node does not host, or one retired because its replay stopped at
/// startup, answers `404 Not Found`.
async fn request_raft_self_election(
    State(state): State<HttpState>,
    Path(target): Path<String>,
    axum::Json(request): axum::Json<SelfElectionRequest>,
) -> Response {
    let target = match parse_raft_target(&target) {
        Ok(target) => target,
        Err(response) => return *response,
    };
    let Some(registry) = state.raft_registry() else {
        return *no_raft_registry();
    };
    let elected = match target {
        ursula_raft::RaftTarget::Meta => {
            registry
                .request_meta_self_election(request.current_term)
                .await
        }
        ursula_raft::RaftTarget::Data(group) => {
            if let Err(response) = resolve_registered_raft_group(registry, group) {
                return *response;
            }
            registry
                .request_self_election(group, request.current_term)
                .await
        }
    };
    let Err(error) = elected else {
        return StatusCode::NO_CONTENT.into_response();
    };
    let status = match &error {
        SelfElectionError::TermChanged { .. }
        | SelfElectionError::Rejected { .. }
        | SelfElectionError::Transfer(
            IncomingTransferError::OwnCommittedVote { .. }
            | IncomingTransferError::MayNotCampaign { .. },
        ) => StatusCode::CONFLICT,
        SelfElectionError::Transfer(IncomingTransferError::Group(group))
            if matches!(
                group.infra(),
                Some(ursula_runtime::GroupInfraError::RaftGroupNotRegistered { .. })
            ) =>
        {
            StatusCode::NOT_FOUND
        }
        SelfElectionError::Transfer(
            IncomingTransferError::Group(_) | IncomingTransferError::Raft { .. },
        ) => StatusCode::SERVICE_UNAVAILABLE,
    };
    (status, format!("self-election refused: {error}")).into_response()
}

pub(crate) fn parse_raft_group_id(raw: u64) -> Result<RaftGroupId, std::num::TryFromIntError> {
    u32::try_from(raw).map(RaftGroupId)
}

/// Operator recovery when a majority of a group's voters are gated after a
/// crash may have cost them their unsynced tail: this replica takes a
/// recovery plan (#581 section 4.5), of a data group or of the meta group
/// (`/__ursula/raft/meta/...`), whose `floor` must be of the target's
/// plane. The plan names its leader, the point the leader's log reaches,
/// whether every member cuts its log to what it knew was committed, and
/// this replica's log as the tool read it. A
/// replica that follows a live leader, holds another log, or committed above
/// the point refuses with `409 Conflict` and changes nothing; a 0.7 body is
/// refused with `422`. A plan that cuts the logs restarts the group's
/// replica to run the cut before it answers. The operator computes the plan
/// and sends it to the plan's leader first, then to every other reachable
/// member with the leader's plan term (operations docs, "Recovering after a
/// host crash").
pub(crate) async fn accept_unsynced_loss(
    State(state): State<HttpState>,
    Path(target): Path<String>,
    axum::Json(plan): axum::Json<ursula_proto::admin::RecoveryPlanRequest>,
) -> Response {
    let target = match parse_raft_target(&target) {
        Ok(target) => target,
        Err(response) => return *response,
    };
    let Some(registry) = state.raft_registry() else {
        return *no_raft_registry();
    };
    let raft_group_id = match target {
        ursula_raft::RaftTarget::Meta => ursula_raft::meta::state_machine::META_GROUP,
        ursula_raft::RaftTarget::Data(group) => {
            if let Err(response) = resolve_registered_raft_group(registry, group) {
                return *response;
            }
            group
        }
    };
    // The node's own id: the one its config names, or its replica's.
    let Some(node_id) = state.configured_node_id.or_else(|| match target {
        ursula_raft::RaftTarget::Meta => registry
            .meta_raft()
            .map(|raft| raft.metrics().borrow_watched().id),
        ursula_raft::RaftTarget::Data(group) => {
            registry.rejoin(group).map(|rejoin| rejoin.node_id())
        }
    }) else {
        let err = ursula_raft::RecoveryGateError::NotRegistered { group: target };
        return (StatusCode::NOT_FOUND, err.to_string()).into_response();
    };
    let plans = match target {
        ursula_raft::RaftTarget::Meta => match state.meta().plans() {
            Some(plans) => Some(plans),
            None => return *no_meta_replica(),
        },
        ursula_raft::RaftTarget::Data(_) => None,
    };
    // The plan runs in a task of its own, so a client that goes away
    // cancels neither the plan nor the restart of its cut, and the registry
    // runs the plan's job in its own task too (m1, #581 review).
    let job = {
        let runtime = state.runtime.clone();
        let registry = registry.clone();
        tokio::spawn(async move {
            match plans {
                Some(plans) => plans.accept_recovery_plan(&plan).await,
                None => {
                    let mut restart = RuntimeReplicaRestart {
                        runtime,
                        registry: registry.clone(),
                    };
                    registry
                        .accept_recovery_plan(raft_group_id, &plan, &mut restart)
                        .await
                }
            }
        })
    };
    let accepted = job
        .await
        .map_err(|_aborted| ursula_raft::RecoveryGateError::JobAborted { group: target })
        .and_then(|accepted| accepted);
    let err = match accepted {
        Ok(report) => return (StatusCode::OK, axum::Json(report)).into_response(),
        Err(err) => err,
    };
    match err.answer() {
        // m3 (#581 review): a refusal answers its typed reason.
        ursula_raft::PlanAnswer::Refused(refusal) => (
            StatusCode::CONFLICT,
            axum::Json(ursula_proto::admin::RecoveryPlanRefused {
                raft_group_id: u64::from(raft_group_id.0),
                node_id,
                refusal,
                message: err.to_string(),
            }),
        )
            .into_response(),
        ursula_raft::PlanAnswer::NotRegistered => {
            (StatusCode::NOT_FOUND, err.to_string()).into_response()
        }
        ursula_raft::PlanAnswer::Failed => {
            (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()).into_response()
        }
        ursula_raft::PlanAnswer::Unavailable => {
            (StatusCode::SERVICE_UNAVAILABLE, err.to_string()).into_response()
        }
    }
}

/// Restarts a group's replica through the node's runtime, for a recovery
/// plan's cut, and finds its new recovery gate in the node's registry.
struct RuntimeReplicaRestart {
    runtime: ShardRuntime,
    registry: RaftGroupHandleRegistry,
}

impl ursula_raft::ReplicaRestart for RuntimeReplicaRestart {
    fn restart_replica(
        &mut self,
        raft_group_id: RaftGroupId,
    ) -> futures_util::future::BoxFuture<
        '_,
        Result<Option<Arc<ursula_raft::GroupRejoin>>, ursula_runtime::RuntimeError>,
    > {
        Box::pin(async move {
            self.runtime.restart_group_engine(raft_group_id).await?;
            Ok(self.registry.rejoin(raft_group_id))
        })
    }
}

/// The answer to an operator's snapshot whose build did not start: `503`
/// with `Retry-After` when no build of the group could start in time, since
/// builds end, and `500` when the permits closed or the trigger failed.
fn snapshot_not_started(err: &SnapshotStartError<Fatal<UrsulaRaftTypeConfig>>) -> Response {
    match err {
        SnapshotStartError::NotStarted { .. } => (
            StatusCode::SERVICE_UNAVAILABLE,
            [(axum::http::header::RETRY_AFTER, "1")],
            format!("{err}; retry"),
        )
            .into_response(),
        SnapshotStartError::Permit(source) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("{err}: {source}"),
        )
            .into_response(),
        SnapshotStartError::Trigger { source, .. } => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("{err}: {source}"),
        )
            .into_response(),
    }
}

/// The `{target}` of a `/__ursula/raft/{target}/...` route: `meta`, the
/// meta group, or a data group's id. `400` for anything else.
fn parse_raft_target(raw: &str) -> Result<ursula_raft::RaftTarget, Box<Response>> {
    if raw == "meta" {
        return Ok(ursula_raft::RaftTarget::Meta);
    }
    raw.parse::<u32>()
        .map(|group| ursula_raft::RaftTarget::Data(RaftGroupId(group)))
        .map_err(|_invalid| {
            Box::new(
                (
                    StatusCode::BAD_REQUEST,
                    "invalid raft target: `meta` or a raft group id",
                )
                    .into_response(),
            )
        })
}

/// The data group of a route that serves data groups only: `400` for
/// `meta`, as for any invalid target.
fn data_group_target(raw: &str, route: &str) -> Result<RaftGroupId, Box<Response>> {
    match parse_raft_target(raw)? {
        ursula_raft::RaftTarget::Data(group) => Ok(group),
        ursula_raft::RaftTarget::Meta => Err(Box::new(
            (
                StatusCode::BAD_REQUEST,
                format!("the {route} route serves data groups only, not `meta`"),
            )
                .into_response(),
        )),
    }
}

/// Resolves the raft registry and looks up the live group handle, the
/// preamble of the data-group raft admin routes: `400` without a registry
/// and `404` for a group the node does not run.
fn resolve_raft_group(
    state: &HttpState,
    raft_group_id: RaftGroupId,
) -> Result<(RaftGroupId, OwnerRaftHandle), Box<Response>> {
    let Some(registry) = state.raft_registry() else {
        return Err(no_raft_registry());
    };
    resolve_registered_raft_group(registry, raft_group_id)
}

/// The answer of a meta route on a node that runs no meta replica: the
/// zero-config in-memory node, a damaged one, or one that shuts down.
fn no_meta_replica() -> Box<Response> {
    Box::new((StatusCode::NOT_FOUND, "this node runs no meta replica").into_response())
}

fn no_raft_registry() -> Box<Response> {
    Box::new(
        (
            StatusCode::BAD_REQUEST,
            "raft registry is not configured for this server",
        )
            .into_response(),
    )
}

/// The group `raft_group_id` of `registry`: `404` for a group the node
/// does not run.
fn resolve_registered_raft_group(
    registry: &RaftGroupHandleRegistry,
    raft_group_id: RaftGroupId,
) -> Result<(RaftGroupId, OwnerRaftHandle), Box<Response>> {
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
    version: Version,
    Path(path): Path<StreamPath>,
    headers: HeaderMap,
    admission: PendingAdmission,
    body: Body,
) -> Response {
    let stream_id = path.into_stream_id();
    let request_target = request_target(&uri);
    let head = match CreateHead::parse(&headers) {
        Ok(head) => head,
        Err(response) => return answer_before_body(*response, version, &headers, body).await,
    };
    let decision =
        front_door::route_before_body(&state, &stream_id, &request_target, None, admission.0).await;
    let (_permit, fence) = match decision {
        HeadDecision::Answer(answer) => {
            return answer_before_body(answer, version, &headers, body).await;
        }
        HeadDecision::Read { permit, fence } => (permit, fence),
    };
    let body = match axum::body::to_bytes(body, MAX_HTTP_BODY_BYTES).await {
        Ok(body) => body,
        Err(err) => return unreadable_body_response(&err),
    };
    create_stream_by_id(state, stream_id, headers, head, body, fence).await
}

/// A create's head, parsed once before routing (the checks that answer
/// `400`, #581 section 6.1), and carried to the late path.
pub(crate) struct CreateHead {
    stream_ttl_seconds: Option<u64>,
    stream_expires_at_ms: Option<u64>,
    if_incarnation: Option<u64>,
    stream_seq: Option<String>,
}

impl CreateHead {
    fn parse(headers: &HeaderMap) -> Result<Self, Box<Response>> {
        let (stream_ttl_seconds, stream_expires_at_ms) = stream_lifetime(headers)?;
        let if_incarnation = incarnation_precondition(headers)?;
        let stream_seq = stream_seq(headers).map_err(invalid_header)?;
        refuse_create_producer_headers(headers)?;
        Ok(Self {
            stream_ttl_seconds,
            stream_expires_at_ms,
            if_incarnation,
            stream_seq,
        })
    }
}

/// A create carries no producer headers (#581 section 3): keyed bytes
/// enter a stream only through claims and appends.
fn refuse_create_producer_headers(headers: &HeaderMap) -> Result<(), Box<Response>> {
    match parse_producer_headers(headers) {
        Ok(ProducerRequestHeaders { producer: None, .. }) => Ok(()),
        Ok(ProducerRequestHeaders {
            producer: Some(_), ..
        }) => Err(Box::new(error_response(
            StatusCode::BAD_REQUEST,
            ErrorCode::InvalidProducer,
            "a create carries no producer headers: create the stream, then claim an epoch",
        ))),
        Err(err) => Err(invalid_producer(&err)),
    }
}

/// Creates the stream the head `head` and the body `body` describe. A body
/// staged in the cold tier is fenced to `fence`, the leadership that
/// admitted the create (F14j).
pub(crate) async fn create_stream_by_id(
    state: HttpState,
    stream_id: BucketStreamId,
    request_headers: HeaderMap,
    head: CreateHead,
    body: Bytes,
    fence: UploadFence,
) -> Response {
    let CreateHead {
        stream_ttl_seconds,
        stream_expires_at_ms,
        if_incarnation,
        stream_seq,
    } = head;
    let content_type_explicit = has_content_type(&request_headers);
    let content_type = request_content_type(&request_headers);
    let mut request = CreateStreamRequest::new(stream_id.clone(), content_type.clone());
    request.if_incarnation = if_incarnation;
    request.content_type_explicit = content_type_explicit;
    request.now_ms = state.unix_time_ms();
    request.initial_payload = match normalize_http_write_payload(&content_type, body.clone(), true)
    {
        Ok(payload) => payload,
        Err(message) => {
            return error_response(
                StatusCode::BAD_REQUEST,
                ErrorCode::InvalidRecordBoundaries,
                message,
            );
        }
    };
    request.close_after = stream_closed(&request_headers);
    request.stream_seq = stream_seq;
    request.stream_ttl_seconds = stream_ttl_seconds;
    request.stream_expires_at_ms = stream_expires_at_ms;
    if should_externalize_payload(&state, request.initial_payload.len(), true) {
        return create_stream_external_by_id(state, request, fence).await;
    }

    match state.runtime.create_stream(request).await {
        Ok(response) => create_stream_http_response(CreateStreamHttpResponseInput {
            response,
            stream_id: &stream_id,
            content_type: &content_type,
            stream_ttl_seconds,
            stream_expires_at_ms,
        }),
        Err(err) => runtime_error_after_body(&state, err),
    }
}

pub(crate) async fn create_stream_external_by_id(
    state: HttpState,
    mut request: CreateStreamRequest,
    fence: UploadFence,
) -> Response {
    let stream_id = request.stream_id.clone();
    let content_type = request.content_type.clone();
    let stream_ttl_seconds = request.stream_ttl_seconds;
    let stream_expires_at_ms = request.stream_expires_at_ms;
    let message_count = request.canonical_message_count();
    let payload = std::mem::take(&mut request.initial_payload);
    let external_payload = match stage_external_payload(&state, &stream_id, &payload).await {
        Ok(payload) => payload,
        Err(response) => return response,
    };
    let external_path = external_payload.s3_path.clone();
    let external_request = CreateStreamExternalRequest::from_create_request(
        request,
        external_payload,
        message_count,
        ProposedUpload::new(fence),
    );

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
            })
        }
        Err(err) => {
            cleanup_external_payload(&state, &external_path, &err).await;
            runtime_error_after_body(&state, err)
        }
    }
}

pub(crate) async fn append_stream(
    State(state): State<HttpState>,
    OriginalUri(uri): OriginalUri,
    version: Version,
    Path(path): Path<StreamPath>,
    headers: HeaderMap,
    admission: PendingAdmission,
    body: Body,
) -> Response {
    let stream_id = path.into_stream_id();
    let request_target = request_target(&uri);
    let head = match AppendHead::parse(&headers) {
        Ok(head) => head,
        Err(response) => return answer_before_body(*response, version, &headers, body).await,
    };
    let early_answer = early_answer_may_help(&headers)
        .then(|| head.early_answer_query(state.unix_time_ms()))
        .flatten();
    let decision = front_door::route_before_body(
        &state,
        &stream_id,
        &request_target,
        early_answer,
        admission.0,
    )
    .await;
    let (_permit, fence) = match decision {
        HeadDecision::Answer(answer) => {
            return answer_before_body(answer, version, &headers, body).await;
        }
        HeadDecision::Read { permit, fence } => (permit, fence),
    };
    let body = match axum::body::to_bytes(body, MAX_HTTP_BODY_BYTES).await {
        Ok(body) => body,
        Err(err) => return unreadable_body_response(&err),
    };
    append_stream_by_id(state, stream_id, headers, head, body, fence).await
}

/// An append's head, parsed once before routing (step 1 of #581 section
/// 5.3, which answers `400`), and carried to the late path.
pub(crate) struct AppendHead {
    if_incarnation: Option<u64>,
    stream_seq: Option<String>,
    producer: Option<ProducerRequest>,
}

impl AppendHead {
    fn parse(headers: &HeaderMap) -> Result<Self, Box<Response>> {
        removed_surface::reject_removed_append_headers(headers)?;
        let if_incarnation = incarnation_precondition(headers)?;
        let producer = producer_write(headers)?;
        let stream_seq = stream_seq(headers).map_err(invalid_header)?;
        Ok(Self {
            if_incarnation,
            stream_seq,
            producer,
        })
    }

    /// The key of a tokened producer write, whose answer the group's leader
    /// may decide from the head alone (#581 section 2.5, W7). `None` for
    /// every other write, which is read, proposed and decided at apply.
    fn early_answer_query(&self, now_ms: u64) -> Option<EarlyAnswerQuery> {
        Some(EarlyAnswerQuery {
            producer: self.producer.clone()?,
            incarnation: self.if_incarnation?,
            now_ms,
        })
    }
}

/// The early answer to a tokened producer write: a repeat of a write that
/// applied under the request's `Stream-Incarnation` (`204` with its
/// receipt, or `receipt-evicted`), or a key conflict (`403`).
pub(crate) fn early_answer_response(answer: ProducerEarlyAnswer, incarnation: u64) -> Response {
    let answer = match answer {
        ProducerEarlyAnswer::Repeat(ack) => producer_ack_answer(&ack, incarnation),
        ProducerEarlyAnswer::KeyConflict { epoch } => {
            Epoch::try_from(epoch).map(|epoch| ProducerAnswer::KeyConflict { epoch })
        }
    };
    // A repeat is `204` whether the request is a close or an append, so the
    // renderer needs no more than this.
    producer_answer_response(answer, AnswerTo::Append)
}

/// The smallest declared body for which an early answer is worth its lookup
/// without `Expect: 100-continue`, on any HTTP version: below it the lookup
/// ends the group's append batch for less than it saves. On HTTP/2 the
/// answer saves the rest of the upload; on HTTP/1.1 the body is still read
/// and dropped, so the answer saves the staging and the proposal.
const EARLY_ANSWER_MIN_BODY_BYTES: u64 = 64 * 1024;

/// Whether to look for an early answer: only after `Expect: 100-continue`,
/// which always saves the upload, or for a declared body of at least
/// [`EARLY_ANSWER_MIN_BODY_BYTES`]. The caller has run the request-shape
/// checks of step 1 (#581 section 5.3) already ([`AppendHead::parse`]). The
/// content checks of step 5 come after the producer rows on both paths, so
/// a repeat or a key conflict is answered whatever the body is.
fn early_answer_may_help(headers: &HeaderMap) -> bool {
    let expects_continue = headers
        .get(axum::http::header::EXPECT)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.eq_ignore_ascii_case("100-continue"));
    let content_length = headers
        .get(axum::http::header::CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok());
    expects_continue || content_length.is_some_and(|len| len >= EARLY_ANSWER_MIN_BODY_BYTES)
}

/// Sends an answer decided before the body was read (addendum A3 on #581).
/// On HTTP/2 the answer goes first and the server ends the request stream
/// (`RST_STREAM(NO_ERROR)`). A client that sent `Expect: 100-continue` gets
/// no `100 Continue` and sends no body. On HTTP/1.1 otherwise the body is
/// read and dropped within the request limit and the gateway's drain
/// bounds, so the connection carries the next request; past them the
/// connection closes after the answer.
async fn answer_before_body(
    mut response: Response,
    version: Version,
    headers: &HeaderMap,
    body: Body,
) -> Response {
    let expects_continue = headers
        .get(axum::http::header::EXPECT)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.eq_ignore_ascii_case("100-continue"));
    if version >= Version::HTTP_2 || expects_continue || body.is_end_stream() {
        return response;
    }
    if ursula_gateway::drain(body, MAX_HTTP_BODY_BYTES).await != ursula_gateway::ReadEnd::Whole {
        response.headers_mut().insert(
            axum::http::header::CONNECTION,
            HeaderValue::from_static("close"),
        );
    }
    response
}

/// `413` for a body over the request limit, `400` for one that failed.
fn unreadable_body_response(err: &axum::Error) -> Response {
    let over_limit = std::error::Error::source(err)
        .is_some_and(|source| source.is::<http_body_util::LengthLimitError>());
    if over_limit {
        return limits::size_refusal(limits::REQUEST_LIMIT);
    }
    error_response(
        StatusCode::BAD_REQUEST,
        ErrorCode::BodyUnreadable,
        format!("request body unreadable: {err}"),
    )
}

#[tracing::instrument(
    name = "http.append",
    skip_all,
    fields(bucket = %stream_id.bucket_id, stream = %stream_id.stream_id),
)]
pub(crate) async fn append_stream_by_id(
    state: HttpState,
    stream_id: BucketStreamId,
    headers: HeaderMap,
    head: AppendHead,
    body: Bytes,
    fence: UploadFence,
) -> Response {
    let AppendHead {
        if_incarnation,
        stream_seq: stream_seq_header,
        mut producer,
    } = head;
    let close_after = stream_closed(&headers);

    let content_type = request_content_type(&headers);
    let close_only = body.is_empty() && close_after;
    let payload = match content_checks(&headers, &content_type, body, producer.as_mut()) {
        Ok(payload) => payload,
        Err(response) => return *response,
    };
    // A write without a key has no producer rows, so its empty body is
    // refused here, with the other content faults, instead of in the log.
    if producer.is_none() && payload.is_empty() && !close_after {
        return error_response(
            StatusCode::BAD_REQUEST,
            ErrorCode::EmptyAppend,
            "append payload must be non-empty unless closing the stream",
        );
    }
    if close_only {
        return match state
            .runtime
            .close_stream(CloseStreamRequest {
                stream_id,
                stream_seq: stream_seq_header,
                producer: producer.clone(),
                now_ms: state.unix_time_ms(),
                if_incarnation,
            })
            .await
        {
            Ok(response) => close_http_response(&response),
            Err(err) => runtime_error_after_body(&state, err),
        };
    }
    let mut request = AppendRequest::from_bytes(stream_id, payload);
    request.content_type = content_type;
    request.if_incarnation = if_incarnation;
    request.close_after = close_after;
    request.stream_seq = stream_seq_header;
    request.now_ms = state.unix_time_ms();
    request.producer = producer;

    if should_externalize_payload(&state, request.payload.len(), true) {
        return append_stream_external_by_id(state, request, fence).await;
    }

    match state.runtime.append(request).await {
        Ok(response) => append_http_response(response),
        Err(err) => runtime_error_after_body(&state, err),
    }
}

/// Appends `request` with its payload staged in the cold tier, fenced to
/// `fence`, the leadership that admitted the append (F14j).
pub(crate) async fn append_stream_external_by_id(
    state: HttpState,
    mut request: AppendRequest,
    fence: UploadFence,
) -> Response {
    let stream_id = request.stream_id.clone();
    let message_count = request.canonical_message_count();
    let payload = std::mem::take(&mut request.payload);
    let external_payload = match stage_external_payload(&state, &stream_id, &payload).await {
        Ok(payload) => payload,
        Err(response) => return response,
    };
    let external_path = external_payload.s3_path.clone();
    let external_request = AppendExternalRequest::from_append_request(
        request,
        external_payload,
        message_count,
        ProposedUpload::new(fence),
    );
    match state.runtime.append_external(external_request).await {
        Ok(response) => {
            if response.deduplicated() {
                // The original append committed with its own object; this
                // retry's object is referenced by nothing (F5 cleanup rule).
                delete_unreferenced_staged_payload(&state, &external_path).await;
            }
            append_http_response(response)
        }
        Err(err) => {
            cleanup_external_payload(&state, &external_path, &err).await;
            runtime_error_after_body(&state, err)
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
    // Read before the delete: every incarnation known then is dead after it.
    let started_ms = state.unix_time_ms();
    let known_parts = state.staged_parts.before_delete(&stream_id);
    match state
        .runtime
        .delete_stream(DeleteStreamRequest {
            stream_id: stream_id.clone(),
            now_ms: started_ms,
            if_incarnation,
        })
        .await
    {
        Ok(response) => {
            // The stream had expired, so it was already gone: the delete
            // removed it and answers as for a missing stream.
            let expired = response.expired.then(|| RuntimeError::GroupEngine {
                core_id: response.placement.core_id,
                raft_group_id: response.placement.raft_group_id,
                error: GroupEngineError::stream(
                    StreamErrorCode::StreamNotFound,
                    format!("stream '{stream_id}' does not exist"),
                ),
            });
            // The stream's staged parts can no longer be committed. The
            // cleanup runs after the answer, and only for a stream this
            // node counts parts of.
            if let (Some(known), Some(cold_store)) = (known_parts, state.runtime.cold_store()) {
                let raft_group_id = state.runtime.locate(&stream_id).raft_group_id.0;
                state.staged_parts.deleted(
                    cold_store,
                    stream_id,
                    raft_group_id,
                    known,
                    if_incarnation,
                    started_ms,
                );
            }
            if let Some(err) = expired {
                return write_error_or_leader_redirect_async(&state, err, &request_target).await;
            }
            let mut headers = HeaderMap::new();
            insert_default_response_headers(&mut headers);
            (StatusCode::NO_CONTENT, headers).into_response()
        }
        Err(err) => write_error_or_leader_redirect_async(&state, err, &request_target).await,
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
            if let Some(refused) = read_precondition_failed(if_incarnation, response.incarnation) {
                return refused;
            }
            let head = match stream_head(
                &response,
                limits::max_snapshot_bytes(state.runtime.has_cold_store()),
            ) {
                Ok(head) => head,
                Err(err) => {
                    tracing::error!(error = %err, "a stream's HEAD is outside the protocol's range");
                    return error_response(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        ErrorCode::Internal,
                        "stream metadata outside the protocol's range",
                    );
                }
            };
            let mut headers = HeaderMap::new();
            insert_default_response_headers(&mut headers);
            insert_content_type(&mut headers, &response.content_type);
            insert_stream_head(&mut headers, &head);
            insert_padded_offset(
                &mut headers,
                HEADER_STREAM_COLD_HOT_START_OFFSET,
                response.cold_hot_start_offset,
            );
            insert_static(&mut headers, names::STREAM_UP_TO_DATE, "true");
            insert_cache_control(&mut headers, "no-store");
            insert_lifetime_headers(
                &mut headers,
                response.stream_ttl_seconds,
                response.stream_expires_at_ms,
            );
            (StatusCode::OK, headers).into_response()
        }
        Err(err) => read_error_or_leader_redirect_async(&state, err, &request_target).await,
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
        Some(_) => {
            return error_response(
                StatusCode::BAD_REQUEST,
                ErrorCode::InvalidQuery,
                "invalid consistency",
            );
        }
    };
    if leader_only && live_mode.is_some() {
        return error_response(
            StatusCode::BAD_REQUEST,
            ErrorCode::InvalidQuery,
            "leader consistency is available only for catch-up reads",
        );
    }
    let offset_is_now = query.get("offset").is_some_and(|offset| offset == "now");
    if live_mode.is_some() && !query.contains_key("offset") {
        return error_response(
            StatusCode::BAD_REQUEST,
            ErrorCode::InvalidQuery,
            "live reads require a start position",
        );
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
                return read_error_or_leader_redirect_async(&state, err, &request_target).await;
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
        (Some(_), _) => {
            return error_response(
                StatusCode::BAD_REQUEST,
                ErrorCode::InvalidQuery,
                "invalid live mode",
            );
        }
        (None, _) => {}
    }

    let read = read_at_token(
        &state,
        ReadStreamRequest {
            stream_id: stream_id.clone(),
            offset,
            max_len,
            now_ms: state.unix_time_ms(),
            leader_only,
            read_index: None,
            follower: FollowerRead::Prefix,
        },
        if_incarnation,
    )
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
        Err(err) => read_error_or_leader_redirect_async(&state, err, &request_target).await,
    }
}

/// A catch-up read checked against the token the request expects. A
/// follower never answers `412` from a state that may lag (#581 section
/// 4.7): when its token differs from `expected`, the read goes to the
/// stream's leader, whose linearized state decides.
pub(crate) async fn read_at_token(
    state: &HttpState,
    request: ReadStreamRequest,
    expected: Option<u64>,
) -> Result<ReadStreamResponse, RuntimeError> {
    let leader_only = request.leader_only;
    let read = state.runtime.read_stream(request.clone()).await;
    match (expected, &read) {
        (Some(expected), Ok(response)) if !leader_only && response.incarnation != expected => {
            state
                .runtime
                .read_stream(ReadStreamRequest {
                    leader_only: true,
                    ..request
                })
                .await
        }
        _ => read,
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
    version: Version,
    Path(path): Path<SnapshotPath>,
    headers: HeaderMap,
    admission: PendingAdmission,
    body: Body,
) -> Response {
    if let Err(response) = removed_surface::reject_removed_snapshot_headers(&headers) {
        return answer_before_body(*response, version, &headers, body).await;
    }
    let (stream_id, snapshot_offset) = path.into_parts();
    let snapshot_offset = match parse_snapshot_offset(&snapshot_offset) {
        Ok(offset) => offset,
        Err(response) => return answer_before_body(*response, version, &headers, body).await,
    };
    let if_incarnation = match incarnation_precondition(&headers) {
        Ok(if_incarnation) => if_incarnation,
        Err(response) => return answer_before_body(*response, version, &headers, body).await,
    };
    // A publish with `Stream-Snapshot-Parts` commits a snapshot uploaded in
    // parts: its body lists the parts (#581 section 3.1).
    let parts = match snapshot_parts::parts_commit(&state, &headers) {
        Ok(parts) => parts,
        Err(refusal) => {
            return answer_before_body(refusal.into_response(), version, &headers, body).await;
        }
    };
    let request_target = request_target(&uri);
    let snapshot = PublishTarget {
        state,
        request_target,
        stream_id,
        snapshot_offset,
        content_type: request_content_type(&headers),
        if_incarnation,
    };
    let Some(commit) = parts else {
        let decision = front_door::route_before_body(
            &snapshot.state,
            &snapshot.stream_id,
            &snapshot.request_target,
            None,
            admission.0,
        )
        .await;
        let (_permit, fence) = match decision {
            HeadDecision::Answer(answer) => {
                return answer_before_body(answer, version, &headers, body).await;
            }
            HeadDecision::Read { permit, fence } => (permit, fence),
        };
        return match cold_snapshot::receive_snapshot_body(
            &snapshot.state,
            &snapshot.stream_id,
            &snapshot.content_type,
            body,
            fence,
        )
        .await
        {
            Ok(upload) => snapshot.publish(upload).await,
            Err(response) => response,
        };
    };
    // A commit checks its key before its body: a repeat of a published
    // snapshot answers from the key alone, even after its parts are gone
    // (A2 of the #581 addendum on the VFS client asks).
    let decision = front_door::route_before_body(
        &snapshot.state,
        &snapshot.stream_id,
        &snapshot.request_target,
        None,
        None,
    )
    .await;
    if let HeadDecision::Answer(answer) = decision {
        return answer_before_body(answer, version, &headers, body).await;
    }
    let incarnation = match snapshot_parts::key_check(&snapshot, &commit, BodyState::Unread).await {
        snapshot_parts::KeyCheck::Answered(answer) => {
            return answer_before_body(answer, version, &headers, body).await;
        }
        snapshot_parts::KeyCheck::Open { incarnation } => incarnation,
    };
    // The commit's copy is fenced to the leadership this node leads in
    // after its key check (F14j). A node that lost the lead since routing
    // answers as the routing does now, before the body.
    let raft_group_id = snapshot
        .state
        .runtime
        .locate(&snapshot.stream_id)
        .raft_group_id;
    let Some(fence) = front_door::upload_fence(&snapshot.state, raft_group_id) else {
        let answer = front_door::not_leading(
            &snapshot.state,
            &snapshot.stream_id,
            &snapshot.request_target,
        )
        .await;
        return answer_before_body(answer, version, &headers, body).await;
    };
    // The manifest is charged at most its own bound; the copy is charged
    // once it holds a copy slot.
    let budget = admission
        .0
        .as_ref()
        .map(admission::DeferredAdmission::budget);
    let manifest_bytes = u64::try_from(ursula_headers::MAX_MANIFEST_BYTES).unwrap_or(u64::MAX);
    let admission = admission
        .0
        .map(|admission| admission.capped(manifest_bytes));
    let manifest_permit = match front_door::admit(admission, fence) {
        HeadDecision::Answer(answer) => {
            return answer_before_body(answer, version, &headers, body).await;
        }
        HeadDecision::Read { permit, .. } => permit,
    };
    snapshot_parts::commit(
        snapshot,
        commit,
        incarnation,
        budget,
        manifest_permit,
        body,
        fence,
    )
    .await
}

/// The bare `{stream}/snapshot` path (the removed latest-snapshot redirect
/// and record-addressed publish) answers 405 with an empty `Allow`, not 404:
/// Loro's streams client reads a 404 as "no snapshot" (see
/// `removed_surface`).
pub(crate) async fn removed_latest_snapshot() -> Response {
    let mut headers = HeaderMap::new();
    headers.insert(axum::http::header::ALLOW, HeaderValue::from_static(""));
    error_answer(
        StatusCode::METHOD_NOT_ALLOWED,
        ErrorCode::Unsupported,
        "the latest-snapshot redirect was removed; read Stream-Snapshot-Offset from HEAD",
        headers,
    )
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
    body: BodyState,
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
            follower: FollowerRead::Prefix,
        })
        .await;
    let byte = match read {
        Ok(read) => read.payload.first().copied(),
        Err(err) if is_forward_to_leader(&err) => {
            return Err(front_door::write_error_response(state, err, request_target, body).await);
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
        Some(_) => Err(error_response(
            StatusCode::BAD_REQUEST,
            ErrorCode::NotJsonBoundary,
            format!("offset {offset} is not a JSON message boundary"),
        )),
        None => {
            let mut headers = HeaderMap::new();
            headers.insert(
                axum::http::header::RETRY_AFTER,
                HeaderValue::from_static("1"),
            );
            Err(error_answer(
                StatusCode::SERVICE_UNAVAILABLE,
                ErrorCode::JsonBoundaryUnverified,
                format!("could not verify that offset {offset} is a JSON message boundary"),
                headers,
            ))
        }
    }
}

/// A snapshot publish: where it goes and what it names.
pub(crate) struct PublishTarget {
    pub(crate) state: HttpState,
    pub(crate) request_target: String,
    pub(crate) stream_id: BucketStreamId,
    pub(crate) snapshot_offset: u64,
    pub(crate) content_type: String,
    pub(crate) if_incarnation: Option<u64>,
}

impl PublishTarget {
    /// Publishes a received body: proposes it, verifies a JSON boundary the
    /// apply could not, and deletes a staged object after a definite
    /// refusal (F5).
    pub(crate) async fn publish(&self, upload: cold_snapshot::SnapshotUpload) -> Response {
        let state = &self.state;
        let (payload, cold_body) = match upload {
            cold_snapshot::SnapshotUpload::Inline(payload) => (payload, None),
            cold_snapshot::SnapshotUpload::Cold(cold_body) => (Bytes::new(), Some(cold_body)),
        };
        let staged_path = cold_body
            .as_ref()
            .map(|cold_body| cold_body.object.s3_path.clone());
        let mut request = PublishSnapshotRequest {
            stream_id: self.stream_id.clone(),
            snapshot_offset: self.snapshot_offset,
            content_type: self.content_type.clone(),
            payload,
            cold_body,
            now_ms: state.unix_time_ms(),
            expected_incarnation: None,
            if_incarnation: self.if_incarnation,
        };
        let mut result = state.runtime.publish_snapshot(request.clone()).await;
        if let Some(incarnation) = result.as_ref().err().and_then(json_boundary_unverified) {
            if let Err(response) = verify_json_boundary(
                state,
                &self.stream_id,
                self.snapshot_offset,
                &self.request_target,
                BodyState::Read,
            )
            .await
            {
                // Nothing references the staged body: apply refused it.
                if let Some(staged_path) = staged_path.as_deref() {
                    delete_unreferenced_staged_payload(state, staged_path).await;
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
            cleanup_external_payload(state, staged_path, err).await;
        }
        match result {
            Ok(response) => snapshot_published_response(
                response.snapshot_offset,
                &response.snapshot_digest,
                response.incarnation,
            ),
            Err(err) => runtime_error_after_body(state, err),
        }
    }
}

pub(crate) async fn advance_retention(
    State(state): State<HttpState>,
    OriginalUri(uri): OriginalUri,
    Path(path): Path<RetentionPath>,
    headers: HeaderMap,
) -> Response {
    let (stream_id, retained_offset) = path.into_parts();
    let retained_offset = match parse_path_offset(&retained_offset, "invalid retention offset") {
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
        if let Err(response) = verify_json_boundary(
            &state,
            &stream_id,
            retained_offset,
            &request_target,
            BodyState::Unread,
        )
        .await
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
                names::STREAM_RETAINED_OFFSET,
                response.retained_offset,
            );
            insert_incarnation(&mut headers, response.incarnation);
            (StatusCode::NO_CONTENT, headers).into_response()
        }
        Err(err) => write_error_or_leader_redirect_async(&state, err, &request_target).await,
    }
}

#[tracing::instrument(
    name = "http.snapshot_read",
    skip_all,
    fields(bucket = %path.bucket, stream = %path.stream, snapshot_offset = %path.snapshot_offset),
)]
pub(crate) async fn read_snapshot(
    State(state): State<HttpState>,
    method: Method,
    OriginalUri(uri): OriginalUri,
    Path(path): Path<SnapshotPath>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> Response {
    if let Err(response) = parse_query(raw_query.as_deref())
        .and_then(|query| removed_surface::reject_removed_snapshot_read_parameters(&query))
    {
        return *response;
    }
    let (stream_id, snapshot_offset) = path.into_parts();
    let snapshot_offset = match parse_snapshot_offset(&snapshot_offset) {
        Ok(offset) => offset,
        Err(response) => return *response,
    };
    let if_incarnation = match incarnation_precondition(&headers) {
        Ok(if_incarnation) => if_incarnation,
        Err(response) => return *response,
    };
    snapshot_answer(
        &state,
        SnapshotAnswer {
            stream_id,
            snapshot_offset,
            if_incarnation,
            resource: SnapshotResource::Stream,
            body: SnapshotBody::of(&method),
        },
        &headers,
        &request_target(&uri),
    )
    .await
}

/// Which resource a snapshot read answers for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SnapshotResource {
    /// `{stream}/snapshot/{offset}` (extensions Section 2.5): it says
    /// whether the snapshot is up to date.
    Stream,
    /// `{stream}/incarnation/{token}/snapshot/{offset}` (extensions Section
    /// 4.1): always marked immutable, never `Stream-Up-To-Date`.
    Immutable,
}

/// What a snapshot read asks for.
pub(crate) struct SnapshotAnswer {
    pub(crate) stream_id: BucketStreamId,
    pub(crate) snapshot_offset: u64,
    pub(crate) if_incarnation: Option<u64>,
    pub(crate) resource: SnapshotResource,
    pub(crate) body: SnapshotBody,
}

/// Whether a snapshot answer carries its body.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SnapshotBody {
    /// A `GET`: the body, or the bytes of its range.
    Sent,
    /// A `HEAD`: the headers of the `GET` and no body, so a cold snapshot
    /// is not read.
    Omitted,
}

impl SnapshotBody {
    pub(crate) fn of(method: &Method) -> Self {
        if *method == Method::HEAD {
            Self::Omitted
        } else {
            Self::Sent
        }
    }
}

/// The answer of a snapshot read, for both snapshot resources, in the
/// order of extensions Sections 2.4 and 2.5: `404` and `410` (the runtime's
/// errors), then `416` for a range past the end, then the `Stream-Incarnation`
/// precondition (`412`), then a revalidation (`304`), then the body or its
/// range. A revalidation ignores the range, so it never answers `416`.
pub(crate) async fn snapshot_answer(
    state: &HttpState,
    read: SnapshotAnswer,
    headers: &HeaderMap,
    request_target: &str,
) -> Response {
    match state
        .runtime
        .read_snapshot(ReadSnapshotRequest {
            stream_id: read.stream_id,
            snapshot_offset: Some(read.snapshot_offset),
            now_ms: state.unix_time_ms(),
            read_index: None,
        })
        .await
    {
        Ok(mut response) => {
            let object = response.object.take();
            let payload = Bytes::from(std::mem::take(&mut response.payload));
            let total = object.as_ref().map_or_else(
                || u64::try_from(payload.len()).unwrap_or(u64::MAX),
                |object| object.payload_len,
            );
            let revalidated = if_none_match(headers, &snapshot_etag(&response.snapshot_digest));
            let range = if revalidated {
                None
            } else {
                ByteRange::parse(
                    headers
                        .iter()
                        .map(|(name, value)| (name.as_str(), value.as_bytes())),
                )
                .and_then(|range| range.resolve(total))
            };
            let (status, mut response_headers) = match snapshot_head(
                &response,
                range,
                read.resource,
            ) {
                Ok(head) => head,
                Err(err) => {
                    tracing::error!(error = %err, "a snapshot answer outside the protocol's range");
                    return error_response(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        ErrorCode::Internal,
                        "snapshot answer outside the protocol's range",
                    );
                }
            };
            if let Some(ContentRange::Unsatisfied { .. }) = range {
                return error_answer(
                    status,
                    ErrorCode::RangeNotSatisfiable,
                    "the range lies outside the snapshot",
                    response_headers,
                );
            }
            if let Some(refused) =
                read_precondition_failed(read.if_incarnation, response.incarnation)
            {
                return refused;
            }
            // The headers of the `200` it stands for (extensions Section 4.3).
            if revalidated {
                return uncompressed((StatusCode::NOT_MODIFIED, response_headers).into_response());
            }
            let (start, len) = match range {
                None => (0, total),
                Some(ContentRange::Partial(span)) => (span.first(), span.len()),
                Some(ContentRange::Unsatisfied { .. }) => (0, 0),
            };
            if read.body == SnapshotBody::Omitted {
                response_headers.insert(CONTENT_LENGTH, HeaderValue::from(len));
                return uncompressed((status, response_headers).into_response());
            }
            let body = match object {
                Some(object) if len > 0 => {
                    match cold_snapshot::cold_snapshot_body(state, object, start, len).await {
                        Ok(body) => body,
                        Err(response) => return response,
                    }
                }
                _ => {
                    let slice = usize::try_from(start)
                        .ok()
                        .zip(usize::try_from(start.saturating_add(len)).ok());
                    match slice.and_then(|(start, end)| payload.get(start..end)) {
                        Some(bytes) => Body::from(payload.slice_ref(bytes)),
                        // A range resolved against this body lies inside it.
                        None => {
                            tracing::error!(start, len, total, "a snapshot range outside its body");
                            return error_response(
                                StatusCode::INTERNAL_SERVER_ERROR,
                                ErrorCode::Internal,
                                "snapshot range outside its body",
                            );
                        }
                    }
                }
            };
            response_headers.insert(CONTENT_LENGTH, HeaderValue::from(len));
            uncompressed((status, response_headers, body).into_response())
        }
        Err(err) => read_error_or_leader_redirect_async(state, err, request_target).await,
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
        return error_response(
            StatusCode::BAD_REQUEST,
            ErrorCode::InvalidQuery,
            "bootstrap does not support live reads",
        );
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
        Err(err) => read_error_or_leader_redirect_async(&state, err, &request_target(&uri)).await,
    }
}

fn parse_snapshot_offset(raw: &str) -> Result<u64, BoxResponse> {
    parse_path_offset(raw, "invalid snapshot offset")
}

/// An offset in a request path: a decimal, never `-1`. `message` names it.
fn parse_path_offset(raw: &str, message: &'static str) -> Result<u64, BoxResponse> {
    let invalid = || {
        Box::new(error_response(
            StatusCode::BAD_REQUEST,
            ErrorCode::InvalidOffset,
            message,
        ))
    };
    if raw == "-1" {
        return Err(invalid());
    }
    raw.parse::<u64>().map_err(|_invalid| invalid())
}

/// A catch-up read's start position. Its `offset=now` HEAD is linearizable
/// for `consistency=leader`; for `consistency=local` it reads the leader's
/// applied state, which a just-elected leader may not have brought up to
/// the committed tail, so it can resolve to a lagging tail. A missing stream
/// is confirmed by a linearizable HEAD before it answers `404`.
pub(crate) async fn read_offset(
    state: &HttpState,
    stream_id: &BucketStreamId,
    raw: Option<&str>,
    request_target: &str,
    linearizable: bool,
) -> Result<u64, BoxResponse> {
    let head = |linearizable| HeadStreamRequest {
        stream_id: stream_id.clone(),
        now_ms: state.unix_time_ms(),
        linearizable,
        read_index: None,
    };
    match raw {
        Some("now") => match match state.runtime.head_stream(head(linearizable)).await {
            // The leader's applied state may not hold a stream that was just
            // created: a `404` is answered from a linearized HEAD only, as
            // every read's `404` is (#581 section 5.3).
            Err(err)
                if !linearizable
                    && err.stream_error_code() == Some(StreamErrorCode::StreamNotFound) =>
            {
                state.runtime.head_stream(head(true)).await
            }
            head => head,
        } {
            Ok(head) => Ok(head.tail_offset),
            Err(err) => {
                let response =
                    read_error_or_leader_redirect_async(state, err, request_target).await;
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
            Box::new(error_response(
                StatusCode::BAD_REQUEST,
                ErrorCode::InvalidOffset,
                "invalid offset",
            ))
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
            error: GroupEngineError::stream_with_context(
                StreamErrorCode::OffsetOutOfRange,
                format!(
                    "offset {offset} is beyond stream '{stream_id}' tail {}",
                    head.tail_offset
                ),
                Some(head.tail_offset),
                vec![StreamErrorContext::StreamIncarnation {
                    incarnation: head.incarnation,
                }],
            ),
        };
        return read_error_or_leader_redirect_async(&state, err, &request_target).await;
    }
    let incarnation = if_incarnation.or((head.incarnation != 0).then_some(head.incarnation));
    let timeout_ms = long_poll_timeout_ms(query);
    let read = state.runtime.wait_read_stream_pinned(
        ReadStreamRequest {
            stream_id: stream_id.clone(),
            offset,
            max_len: max_len.max(1),
            now_ms: state.unix_time_ms(),
            leader_only: false,
            read_index: owner.read_index,
            follower: FollowerRead::Prefix,
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
        Ok(Err(err)) => read_error_or_leader_redirect_async(&state, err, &request_target).await,
        // The requested offset (the 416 check above keeps it within the
        // owner's tail) and the incarnation the waiter is pinned to.
        // `Stream-Up-To-Date` and `Stream-Closed` hold as the owner read
        // them only when that offset is the owner's tail.
        Err(_) => {
            if let Some(refused) = read_precondition_failed(if_incarnation, head.incarnation) {
                return refused;
            }
            let at_tail = offset == head.tail_offset;
            let mut headers = HeaderMap::new();
            insert_default_response_headers(&mut headers);
            insert_offset(&mut headers, offset);
            insert_incarnation(&mut headers, head.incarnation);
            if at_tail {
                insert_static(&mut headers, names::STREAM_UP_TO_DATE, "true");
            }
            if at_tail && head.closed {
                insert_static(&mut headers, names::STREAM_CLOSED, "true");
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
    if let Some(refused) = read_precondition_failed(if_incarnation, head.incarnation) {
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
        runtime: state.runtime.clone(),
        http_metrics: state.http_metrics.clone(),
        wall_clock: state.wall_clock.clone(),
        stream_id,
        offset,
        max_len: sse_max_len,
        encode_base64,
        cursor: query.get("cursor").cloned(),
        initial_read: true,
        // Pinned to the announced token, as a long-poll is (#581 section
        // 4.7): a session without a precondition ends at a recreate or a
        // history event too.
        incarnation: if_incarnation.or((head.incarnation != 0).then_some(head.incarnation)),
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
            follower: FollowerRead::Prefix,
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
                let event = sse_error_event(&ErrorBody::new(
                    ErrorCode::IncarnationMismatch,
                    "the stream was deleted and recreated, or its history was rewritten \
                     (Stream-Incarnation)",
                ));
                return Some((Ok::<Bytes, Infallible>(Bytes::from(event)), None));
            }
            Ok(read) => read,
            Err(err) => {
                state
                    .http_metrics
                    .sse_error_events
                    .fetch_add(1, Ordering::Relaxed);
                let event = sse_error_event(&runtime_error_body(&err).1);
                return Some((Ok::<Bytes, Infallible>(Bytes::from(event)), None));
            }
        };
        clamp_sse_text_read(&mut read, state.encode_base64);

        state.offset = read.next_offset;
        let done = read.end() == ReadEnd::ClosedTail;
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
    // The base protocol asks an SSE answer for `no-cache`, and no cache may
    // store a node's answer, so both.
    insert_cache_control(&mut headers, "no-cache, no-store");
    insert_incarnation(&mut headers, head.incarnation);
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
            return Err(Box::new(error_response(
                StatusCode::BAD_REQUEST,
                ErrorCode::InvalidQuery,
                "multiple offset parameters",
            )));
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
        .map_err(invalid_header)?;
    let expires_at = header_value(headers, HEADER_STREAM_EXPIRES_AT)
        .map(parse_stream_expires_at)
        .transpose()
        .map_err(invalid_header)?;
    if ttl.is_some() && expires_at.is_some() {
        return Err(invalid_header(
            "stream-ttl and stream-expires-at cannot be provided together".to_owned(),
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
    IncarnationPrecondition::parse(
        headers
            .iter()
            .map(|(name, value)| (name.as_str(), value.as_bytes())),
    )
    .map(|precondition| precondition.map(|precondition| precondition.incarnation.get()))
    .map_err(|err| invalid_header(format!("malformed Stream-Incarnation: {err}")))
}

/// `400 InvalidHeader`, for a header value that `message` describes.
fn invalid_header(message: String) -> BoxResponse {
    Box::new(error_response(
        StatusCode::BAD_REQUEST,
        ErrorCode::InvalidHeader,
        message,
    ))
}

/// `400 InvalidProducer`, for malformed producer headers.
fn invalid_producer(err: &ProducerHeaderError) -> BoxResponse {
    Box::new(error_response(
        StatusCode::BAD_REQUEST,
        ErrorCode::InvalidProducer,
        err.to_string(),
    ))
}

/// A read's `Stream-Incarnation` precondition (D12), checked against the
/// incarnation of the stream state that served it: `Some(412)` on a
/// mismatch, naming the current incarnation.
fn read_precondition_failed(expected: Option<u64>, served: u64) -> Option<Response> {
    let expected = expected?;
    (expected != served).then(|| {
        let mut headers = HeaderMap::new();
        insert_incarnation_mismatch(&mut headers, IncarnationMismatch {
            current: Incarnation::try_from(served).ok(),
        });
        error_answer(
            StatusCode::PRECONDITION_FAILED,
            ErrorCode::IncarnationMismatch,
            format!("Stream-Incarnation {expected} does not match the stream"),
            headers,
        )
    })
}

pub(crate) fn stream_closed(headers: &HeaderMap) -> bool {
    headers
        .get(names::STREAM_CLOSED)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.eq_ignore_ascii_case("true"))
}

/// Maximum length in bytes of a `Producer-Id` or `Stream-Seq` value on every
/// HTTP write path (bounded-stream-state F3), both checked by the header
/// crate. Both values are kept in replicated per-stream state, so their
/// length is bounded at the edge.
#[cfg(test)]
pub(crate) const WRITE_IDENTIFIER_MAX_BYTES: usize = STREAM_SEQ_MAX_BYTES;

/// A request's `Stream-Seq`, checked by the header crate ([`StreamSeq`]),
/// the one check of every seq the server stores and later echoes in
/// `Stream-Last-Seq`. A value that is not visible ASCII text is ignored, as
/// the base protocol's servers do; a blank one is absent.
pub(crate) fn stream_seq(headers: &HeaderMap) -> Result<Option<String>, String> {
    let Some(value) = headers
        .get(HEADER_STREAM_SEQ)
        .and_then(|value| value.to_str().ok())
        .map(|value| value.trim_matches([' ', '\t']))
        .filter(|value| !value.is_empty())
    else {
        return Ok(None);
    };
    StreamSeq::new(value)
        .map(|seq| Some(seq.as_str().to_owned()))
        .map_err(|_invalid| {
            format!(
                "{HEADER_STREAM_SEQ} must be at most {STREAM_SEQ_MAX_BYTES} bytes of visible ASCII"
            )
        })
}

/// The write-key headers of a request (#581 section 2.2), parsed by the
/// shared header crate so the server and its clients cannot drift.
pub(crate) fn parse_producer_headers(
    headers: &HeaderMap,
) -> Result<ProducerRequestHeaders, ProducerHeaderError> {
    ProducerRequestHeaders::parse(
        headers
            .iter()
            .map(|(name, value)| (name.as_str(), value.as_bytes())),
    )
}

/// The replicated form of a parsed producer key.
fn producer_request(write: ProducerWrite) -> ProducerRequest {
    ProducerRequest {
        producer_id: write.id.as_str().to_owned(),
        producer_epoch: write.epoch.get(),
        producer_seq: write.seq.get(),
        digest: write
            .digest
            .map(|digest| Bytes::copy_from_slice(digest.as_bytes())),
        content_fault: ContentFaultV1::ContentFaultNone.into(),
    }
}

/// A write's producer key from its headers (step 1 of #581 section 5.3):
/// `400` for malformed producer headers. `None` for a write without a key.
pub(crate) fn producer_write(
    headers: &HeaderMap,
) -> Result<Option<ProducerRequest>, Box<Response>> {
    let parsed = parse_producer_headers(headers).map_err(|err| invalid_producer(&err))?;
    Ok(parsed.producer.map(producer_request))
}

/// The content checks of a write's body (step 5 of #581 section 5.3) and
/// the payload to propose. A keyed write carries its fault into apply,
/// which answers it only when the write would apply, after the producer
/// rows and the closed check. Apply never needs the bytes of a faulty
/// write, so it is proposed with an empty payload: one small log entry,
/// and nothing in the hot ring. A write without a key has no rows, so its
/// fault is answered here.
fn content_checks(
    headers: &HeaderMap,
    content_type: &str,
    body: Bytes,
    producer: Option<&mut ProducerRequest>,
) -> Result<Bytes, Box<Response>> {
    let digest_mismatch = producer
        .as_ref()
        .and_then(|producer| producer.digest.as_ref())
        .is_some_and(|digest| blake3::hash(&body).as_bytes().as_slice() != digest.as_ref());
    let missing_content_type = !body.is_empty() && !has_content_type(headers);
    let (payload, malformed) = match normalize_http_write_payload(content_type, body.clone(), false)
    {
        Ok(payload) => (payload, None),
        Err(message) => (body, Some(message)),
    };
    let fault = if digest_mismatch {
        Some(ContentFaultV1::ContentFaultDigestMismatch)
    } else if missing_content_type {
        Some(ContentFaultV1::ContentFaultMissingContentType)
    } else if malformed.is_some() {
        Some(ContentFaultV1::ContentFaultMalformedJson)
    } else {
        None
    };
    match (producer, fault) {
        (_, None) => Ok(payload),
        (Some(producer), Some(fault)) => {
            producer.content_fault = fault.into();
            Ok(Bytes::new())
        }
        // The codes apply gives the same faults of a keyed write.
        (None, Some(fault)) => {
            let (code, message) = match fault {
                ContentFaultV1::ContentFaultMissingContentType => (
                    ErrorCode::MissingContentType,
                    "append with a body must include content type".to_owned(),
                ),
                ContentFaultV1::ContentFaultMalformedJson => (
                    ErrorCode::InvalidRecordBoundaries,
                    malformed.unwrap_or_default(),
                ),
                ContentFaultV1::ContentFaultDigestMismatch | ContentFaultV1::ContentFaultNone => (
                    ErrorCode::ProducerDigestMismatch,
                    "producer-digest does not match the request body".to_owned(),
                ),
            };
            Err(Box::new(error_response(
                StatusCode::BAD_REQUEST,
                code,
                message,
            )))
        }
    }
}

pub(crate) fn header_value<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
}

fn runtime_error_response(err: RuntimeError) -> Response {
    let (status, body) = runtime_error_body(&err);
    let mut headers = HeaderMap::new();
    insert_retry_after_for_temporary(&mut headers, &err);
    if let Err(render_err) = insert_producer_error_headers(&mut headers, &err) {
        tracing::error!(refusal = %err, "producer refusal outside the protocol's range");
        return producer_answer_failed(&render_err);
    }
    match stream_seq_conflict(&err) {
        Some(Ok(conflict)) => {
            conflict.render(|name, value| insert_header_str(&mut headers, name, value));
        }
        Some(Err(render_err)) => {
            tracing::error!(refusal = %err, error = %render_err, "a Stream-Seq conflict without its last seq");
            return fixed_error_response(StatusCode::INTERNAL_SERVER_ERROR, ErrorCode::Internal);
        }
        None => {}
    }
    insert_stream_error_headers(&mut headers, &err);
    insert_stream_error_offset(&mut headers, &err);
    error_answer(status, body.code, body.message, headers)
}

fn insert_retry_after_for_temporary(headers: &mut HeaderMap, err: &RuntimeError) {
    if err.status() == ErrorStatus::Temporary {
        headers.insert(
            axum::http::header::RETRY_AFTER,
            HeaderValue::from_static("1"),
        );
    }
}

/// Counts the refusals operators watch: `producer-expired` answers.
fn count_refusal(state: &HttpState, err: &RuntimeError) {
    if err.stream_error_code() == Some(StreamErrorCode::ProducerExpired) {
        state
            .http_metrics
            .producer_expired_answers
            .fetch_add(1, Ordering::Relaxed);
    }
}

/// Whether a request reads or writes, which decides what a leadership
/// refusal of it answers ([`leader_unknown_retry_response`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Access {
    /// A read, which proposes nothing: a forward with a leader redirects,
    /// and one without answers `NO_LEADER`.
    Read,
    /// A write: only one refused before its proposal redirects or answers
    /// `NO_LEADER`, and any other answers `OUTCOME_UNKNOWN`.
    Write,
}

/// The answer to a failed read, which proposes nothing: a redirect to the
/// leader its error names, else the runtime's answer. A write never uses it,
/// since a write's forward after its proposal must answer
/// `OUTCOME_UNKNOWN` ([`write_error_or_leader_redirect_async`]).
pub(crate) async fn read_error_or_leader_redirect_async(
    state: &HttpState,
    err: RuntimeError,
    request_target: &str,
) -> Response {
    leadership_or_runtime_answer(state, err, request_target, Access::Read, Plane::Client)
}

/// The answer to a failed write before its body was read: a redirect only
/// for a write refused before its proposal (extensions Section 5.7), else
/// the runtime's answer.
pub(crate) async fn write_error_or_leader_redirect_async(
    state: &HttpState,
    err: RuntimeError,
    request_target: &str,
) -> Response {
    leadership_or_runtime_answer(state, err, request_target, Access::Write, Plane::Client)
}

/// The answer of an admin route to a failed `access`: the client plane's,
/// without `NotHosted`. A node that hosts no replica of the group, or whose
/// replica names no leader, answers a retryable `503` with no `Location`,
/// so an operator tool, which follows no redirect, asks its next node
/// (design 9).
pub(crate) fn admin_error_or_leader_redirect(
    state: &HttpState,
    err: RuntimeError,
    request_target: &str,
    access: Access,
) -> Response {
    leadership_or_runtime_answer(state, err, request_target, access, Plane::Admin)
}

fn leadership_or_runtime_answer(
    state: &HttpState,
    err: RuntimeError,
    request_target: &str,
    access: Access,
    plane: Plane,
) -> Response {
    count_refusal(state, &err);
    let Some(router) = state.client_write_router() else {
        return runtime_error_response(err);
    };
    // The router holds each peer's client-plane URL (its `client_url`, or its
    // Raft `url` when the planes share `server.listen`), so they are valid
    // redirect targets for reads and writes alike. 307 preserves
    // the method and body, so a redirected POST/PUT re-runs as a write on the
    // leader. Writes go through the leader's raft client_write exactly as a
    // local write would; redirecting only moves the leader hop to the client.
    // Only a write refused before its proposal redirects: a redirect tells
    // the client that this node applied nothing (extensions Section 5.7).
    let redirects = match access {
        Access::Read => true,
        Access::Write => refused_before_proposal(&err),
    };
    if redirects && let Some(redirect) = router.redirect_response(&err, request_target, plane) {
        return redirect;
    }
    // Forward-to-leader error whose leader or term is currently unknown
    // (election in progress), or a group no known node hosts: tell the
    // client to retry rather than failing hard.
    if is_leadership_refusal(&err) {
        return leader_unknown_retry_response(err, access);
    }
    runtime_error_response(err)
}

/// The answer to a write that failed after its body was read: never a
/// redirect (#581 section 6.1). A leadership refusal, of a node that lost
/// the group's leadership while it read the body, answers `503` with
/// `Retry-After` ([`leader_unknown_retry_response`]): `NO_LEADER` when the
/// node refused the write before proposing it, else `OUTCOME_UNKNOWN`, and
/// the write's key resolves the outcome.
pub(crate) fn runtime_error_after_body(state: &HttpState, err: RuntimeError) -> Response {
    count_refusal(state, &err);
    // Only a DST negative control turns the rule off, to show the
    // simulation catches a redirect after the body (its `Location` names
    // the leader's base URL); every other build has no switch.
    if ursula_stream::sim_controls::off(ursula_stream::sim_controls::Defence::NoRedirectAfterBody)
        && let Some(redirect) = state
            .client_write_router()
            .and_then(|router| router.redirect_response(&err, "", Plane::Client))
    {
        return redirect;
    }
    if is_leadership_refusal(&err) {
        return leader_unknown_retry_response(err, Access::Write);
    }
    runtime_error_response(err)
}

/// Whether `err` refuses a request for this node's leadership: a forward to
/// the leader, known or not, or a group this node hosts no replica of.
fn is_leadership_refusal(err: &RuntimeError) -> bool {
    is_forward_to_leader(err) || matches!(err, RuntimeError::GroupNotHosted { .. })
}

/// Whether the runtime refused `err`'s write before it proposed the write's
/// command, so the command never reached the Raft log: a group this node does
/// not host, or what the engine refused before proposing
/// ([`GroupEngineError::refused_before_proposal`]). Every other failure can
/// follow a proposal (a forward or a lost response after `client_write`, a
/// group that stopped applying, a storage error) or is no refusal to retry.
/// The F5 staged-payload cleanup asks, and the answer to a leadership
/// refusal says it: a redirect or `NO_LEADER` only when this holds
/// ([`leader_unknown_retry_response`]).
///
/// [`GroupEngineError::refused_before_proposal`]: ursula_runtime::GroupEngineError::refused_before_proposal
fn refused_before_proposal(err: &RuntimeError) -> bool {
    match err {
        RuntimeError::GroupNotHosted { .. } | RuntimeError::GroupOpening { .. } => true,
        RuntimeError::GroupEngine { error, .. } => error.refused_before_proposal(),
        RuntimeError::InvalidConfig(_)
        | RuntimeError::InvalidRaftGroup { .. }
        | RuntimeError::SnapshotPlacementMismatch { .. }
        | RuntimeError::ColdStoreConfig { .. }
        | RuntimeError::ColdStoreIo { .. }
        | RuntimeError::LiveReadBackpressure { .. }
        | RuntimeError::MailboxClosed { .. }
        | RuntimeError::ResponseDropped { .. }
        | RuntimeError::GroupRestartAborted { .. }
        | RuntimeError::SpawnCoreThread { .. }
        | RuntimeError::GroupAlreadyInstalled { .. } => false,
    }
}

/// True when `err` is a group engine's leadership refusal, whether or not it
/// names the leader.
fn is_forward_to_leader(err: &RuntimeError) -> bool {
    err.not_leader().is_some()
}

/// The `503` with `Retry-After: 1` of a leadership refusal that does not
/// redirect: a purge, which cannot, or a leader that is unknown, or a body
/// already read. The one place that tells the two leadership `503`s apart
/// (extensions Section 5.7):
///
/// - `NO_LEADER` with `x-ursula-raft-group`, for a read, and for a write
///   this node refused before proposing it ([`refused_before_proposal`]):
///   nothing applied.
/// - `OUTCOME_UNKNOWN`, for a write's forward reported after it was handed
///   to Raft, which may follow a committed entry: the write may have
///   applied, and its key resolves it. A read never gets it.
fn leader_unknown_retry_response(err: RuntimeError, access: Access) -> Response {
    // The error stays in the log, at debug, since every election answers so.
    tracing::debug!(error = %err, "no leader of the group is known");
    let mut headers = HeaderMap::new();
    headers.insert(
        axum::http::header::RETRY_AFTER,
        HeaderValue::from_static("1"),
    );
    if access == Access::Write && !refused_before_proposal(&err) {
        return fixed_error_answer(
            StatusCode::SERVICE_UNAVAILABLE,
            ErrorCode::OutcomeUnknown,
            headers,
        );
    }
    let group = match &err {
        RuntimeError::GroupEngine { raft_group_id, .. }
        | RuntimeError::GroupNotHosted { raft_group_id, .. } => Some(*raft_group_id),
        _ => None,
    };
    if let Some(group) = group {
        RoutingAnswer::NoLeader {
            group: ursula_headers::RaftGroup::from_u32(group.0),
        }
        .render(|name, value| insert_header_str(&mut headers, name, value));
    }
    fixed_error_answer(
        StatusCode::SERVICE_UNAVAILABLE,
        ErrorCode::NoLeader,
        headers,
    )
}

fn request_target(uri: &Uri) -> String {
    uri.path_and_query()
        .map(|path_and_query| path_and_query.as_str().to_owned())
        .unwrap_or_else(|| uri.path().to_owned())
}

/// The directory of peers that each serve both planes at one URL, for
/// tests.
#[cfg(test)]
pub(crate) fn shared_peers(
    peers: impl IntoIterator<Item = (u64, String)>,
) -> ursula_raft::PeerDirectory {
    ursula_raft::PeerDirectory::new(
        peers
            .into_iter()
            .map(|(node_id, url)| (node_id, ursula_raft::PeerEndpoints::shared(url))),
    )
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod backup_format_tests;
#[cfg(test)]
mod base_contract_tests;
#[cfg(test)]
mod cold_snapshot_tests;
#[cfg(test)]
mod error_body_tests;
#[cfg(test)]
mod external_entry_tests;
#[cfg(test)]
mod history_tests;
#[cfg(test)]
mod immutable_reads_tests;
#[cfg(test)]
mod server_metadata_tests;
#[cfg(test)]
mod snapshot_parts_tests;
#[cfg(test)]
mod staging_cleanup_tests;
#[cfg(test)]
mod upload_fence_tests;
#[cfg(test)]
mod write_keys_tests;
#[cfg(test)]
mod write_outcome_tests;

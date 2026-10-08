//! Shared telemetry schema; counter ownership remains in the runtime or transport.
//! The manifest drives both the wire snapshot and runtime counter implementation.

use serde::Deserialize;
use serde::Serialize;

#[macro_export]
macro_rules! runtime_metrics_manifest {
    ($callback:ident) => { $callback! {

    sum accepted_appends: core per_core_appends, group per_group_appends;
    sum applied_mutations:
        core per_core_applied_mutations, group per_group_applied_mutations;
    sum mutation_apply_ns:
        core per_core_mutation_apply_ns, group per_group_mutation_apply_ns;
    sum append_post_commit_ns:
        core per_core_append_post_commit_ns, group per_group_append_post_commit_ns;
    sum read_watcher_notify_calls:
        core per_core_read_watcher_notify_calls, group per_group_read_watcher_notify_calls;
    sum read_watcher_notify_ns:
        core per_core_read_watcher_notify_ns, group per_group_read_watcher_notify_ns;
    sum read_watcher_replans:
        core per_core_read_watcher_replans, group per_group_read_watcher_replans;
    sum group_lock_wait_ns:
        core per_core_group_lock_wait_ns, group per_group_group_lock_wait_ns;
    sum group_engine_exec_ns:
        core per_core_group_engine_exec_ns, group per_group_group_engine_exec_ns;
    sum group_mailbox_depth: group per_group_group_mailbox_depth;
    max group_mailbox_max_depth: group per_group_group_mailbox_max_depth;
    sum group_mailbox_full_events: group per_group_group_mailbox_full_events;
    sum raft_apply_entries: core per_core_raft_apply_entries, group per_group_raft_apply_entries;
    sum raft_apply_ns: core per_core_raft_apply_ns, group per_group_raft_apply_ns;
    sum raft_snapshot_builds: group per_group_raft_snapshot_builds;
    sum raft_snapshot_build_ns: group per_group_raft_snapshot_build_ns;
    summax raft_snapshot_body_bytes, raft_snapshot_body_bytes_max:
        group per_group_raft_snapshot_body_bytes;
    summax raft_snapshot_pointer_bytes, raft_snapshot_pointer_bytes_max:
        group per_group_raft_snapshot_pointer_bytes;
    summax raft_snapshot_streams, raft_snapshot_streams_max:
        group per_group_raft_snapshot_streams;
    sum raft_snapshot_external_uploads: group per_group_raft_snapshot_external_uploads;
    sum raft_snapshot_inline_fallbacks: group per_group_raft_snapshot_inline_fallbacks;
    sum live_read_waiters: core per_core_live_read_waiters;
    sum live_read_backpressure_events: core per_core_live_read_backpressure_events;
    sum routed_requests: core per_core_routed_requests;
    sum mailbox_send_wait_ns: core per_core_mailbox_send_wait_ns;
    sum mailbox_full_events: core per_core_mailbox_full_events;
    sum wal_batches: core per_core_wal_batches, group per_group_wal_batches;
    sum wal_records: core per_core_wal_records, group per_group_wal_records;
    sum wal_write_ns: core per_core_wal_write_ns, group per_group_wal_write_ns;
    sum wal_sync_ns: core per_core_wal_sync_ns, group per_group_wal_sync_ns;
    sum wal_fsyncs: core per_core_wal_fsyncs, group per_group_wal_fsyncs;
    sum wal_fsync_records:
        core per_core_wal_fsync_records, group per_group_wal_fsync_records;
    sum wal_reclaims: core per_core_wal_reclaims;
    sum wal_reclaimed_bytes: core per_core_wal_reclaimed_bytes;
    sum wal_reclaim_ns: core per_core_wal_reclaim_ns;
    sum wal_reclaim_failures: core per_core_wal_reclaim_failures;
    sum wal_rewritten_bytes: core per_core_wal_rewritten_bytes;
    sum wal_rotations: core per_core_wal_rotations;
    sum wal_physical_bytes: core per_core_wal_physical_bytes;
    sum wal_segments: core per_core_wal_segments;
    sum wal_pinned_segments: core per_core_wal_pinned_segments;
    sum wal_lagging_groups: core per_core_wal_lagging_groups;
    sum wal_cache_hits: core per_core_wal_cache_hits, group per_group_wal_cache_hits;
    sum wal_cache_misses: core per_core_wal_cache_misses, group per_group_wal_cache_misses;
    sum wal_disk_reads: core per_core_wal_disk_reads, group per_group_wal_disk_reads;
    sum wal_disk_read_bytes:
        core per_core_wal_disk_read_bytes, group per_group_wal_disk_read_bytes;
    sum wal_cache_bytes: group per_group_wal_cache_bytes;
    sum wal_indexed_entries: group per_group_wal_indexed_entries;
    sum wal_recovery_ns: core per_core_wal_recovery_ns;
    sum wal_recovery_records: core per_core_wal_recovery_records;
    sum wal_recovery_bytes: core per_core_wal_recovery_bytes;
    sum wal_recovery_live_entries: core per_core_wal_recovery_live_entries;
    counter cold_flush_uploads;
    counter cold_flush_upload_bytes;
    counter cold_flush_upload_ns;
    counter cold_pack_uploads;
    counter cold_pack_bytes;
    counter cold_pack_slices;
    counter cold_flush_publishes;
    counter cold_flush_publish_bytes;
    counter cold_flush_publish_ns;
    counter cold_orphan_cleanup_attempts;
    counter cold_orphan_cleanup_errors;
    counter cold_orphan_uncovered_chunks_kept;
    counter cold_orphan_bytes;
    counter cold_gc_reclaimed;
    counter cold_gc_errors;
    counter cold_flush_write_errors;
    counter cold_pressure_flush_passes;
    counter cold_pressure_flush_candidates;
    counter raft_snapshot_pressure_passes;
    counter raft_snapshot_pressure_groups;
    sum cold_hot_bytes: group per_group_cold_hot_bytes;
    // Current largest per-group backlog. Cold-health consumes this gauge and
    // must be able to recover after a flush. The separate per-group `*_max`
    // series remains the lifetime high-water mark for diagnostics.
    max cold_hot_group_bytes_max: group per_group_cold_hot_bytes_current_max;
    max cold_hot_group_bytes_high_watermark: group per_group_cold_hot_bytes_max;
    max cold_hot_stream_bytes_max: group per_group_cold_hot_stream_bytes_max;
    sum cold_backpressure_events:
        core per_core_cold_backpressure_events, group per_group_cold_backpressure_events;
    counter cold_backpressure_bytes;
    } };
}

macro_rules! snapshot_schema {
    (@fields { $($fields:tt)* }) => {
        #[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
        #[serde(default)]
        pub struct RuntimeMetricsSnapshot { $($fields)* }
    };
    (@fields { $($fields:tt)* } sum $global:ident: core $core:ident, group $group:ident; $($rest:tt)*) => {
        snapshot_schema!(@fields { $($fields)* pub $global: u64, pub $core: Vec<u64>, pub $group: Vec<u64>, } $($rest)*);
    };
    (@fields { $($fields:tt)* } sum $global:ident: core $core:ident; $($rest:tt)*) => {
        snapshot_schema!(@fields { $($fields)* pub $global: u64, pub $core: Vec<u64>, } $($rest)*);
    };
    (@fields { $($fields:tt)* } sum $global:ident: group $group:ident; $($rest:tt)*) => {
        snapshot_schema!(@fields { $($fields)* pub $global: u64, pub $group: Vec<u64>, } $($rest)*);
    };
    (@fields { $($fields:tt)* } max $global:ident: group $group:ident; $($rest:tt)*) => {
        snapshot_schema!(@fields { $($fields)* pub $global: u64, pub $group: Vec<u64>, } $($rest)*);
    };
    (@fields { $($fields:tt)* } summax $sum:ident, $max:ident: group $group:ident; $($rest:tt)*) => {
        snapshot_schema!(@fields { $($fields)* pub $sum: u64, pub $max: u64, pub $group: Vec<u64>, } $($rest)*);
    };
    (@fields { $($fields:tt)* } counter $global:ident; $($rest:tt)*) => {
        snapshot_schema!(@fields { $($fields)* pub $global: u64, } $($rest)*);
    };
    ($($manifest:tt)*) => { snapshot_schema!(@fields {} $($manifest)*); };
}
runtime_metrics_manifest!(snapshot_schema);

/// What one journal write reports beyond its own latency.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct WalStorageSample {
    /// `fsync` calls the write issued, on files and directories.
    pub fsyncs: u64,
    /// Records made durable by those `fsync`s.
    pub fsync_records: u64,
    /// The current size of the core's journal, all segments.
    pub physical_bytes: u64,
}

/// What a core journal's writer did besides writing batches: rotating
/// segments and reclaiming old ones. Counters add up; gauges replace.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct WalJournalSample {
    /// `fsync` calls of rotations and reclaim passes, on files and
    /// directories.
    pub fsyncs: u64,
    pub rotations: u64,
    /// Segments deleted, and their bytes.
    pub reclaims: u64,
    pub reclaimed_bytes: u64,
    pub reclaim_ns: u64,
    /// Reclaim passes that stopped on an error and left the journal correct.
    pub reclaim_failures: u64,
    /// Live entry bytes copied out of old segments.
    pub rewritten_bytes: u64,
    /// Gauge: the journal's size, all segments.
    pub physical_bytes: u64,
    /// Gauge: the journal's segments.
    pub segments: u64,
    /// Gauge: sealed segments kept only for lagging groups.
    pub pinned_segments: u64,
    /// Gauge: groups reported lagging to the snapshot driver.
    pub lagging_groups: u64,
}

/// What a read of a group's log cost.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct WalReadSample {
    /// Entries served from the group's cache.
    pub cache_hits: u64,
    /// Entries read from disk.
    pub cache_misses: u64,
    /// Frames read from disk, and their bytes.
    pub disk_reads: u64,
    pub disk_read_bytes: u64,
}

/// The size of a group's log in memory.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct WalMemorySample {
    /// Bytes of cached entries.
    pub cache_bytes: u64,
    /// Entries the group's index holds.
    pub indexed_entries: u64,
}

#[derive(Debug, Clone, Copy, Default, serde::Serialize, serde::Deserialize)]
#[serde(default)]
pub struct HttpMetricsSnapshot {
    pub sse_streams_opened: u64,
    pub sse_read_iterations: u64,
    pub sse_data_events: u64,
    pub sse_control_events: u64,
    pub sse_error_events: u64,
}

#[derive(Debug, Clone, Copy, Default, serde::Serialize, serde::Deserialize)]
#[serde(default)]
pub struct RaftGrpcMetricsSnapshot {
    pub raft_grpc_append_stream_sessions_opened: u64,
    pub raft_grpc_append_stream_session_failures: u64,
    pub raft_grpc_append_stream_requests: u64,
    pub raft_grpc_append_stream_responses: u64,
    pub raft_grpc_append_stream_request_bytes: u64,
    pub raft_grpc_append_stream_response_bytes: u64,
    pub raft_grpc_append_stream_request_frames: u64,
    pub raft_grpc_append_stream_response_frames: u64,
    pub raft_grpc_append_stream_batch_frames: u64,
    pub raft_grpc_append_stream_batch_items_max: u64,
    pub raft_grpc_append_stream_inflight: u64,
    pub raft_grpc_append_stream_inflight_max: u64,
    /// Leader side: bytes of Append calls queued for peers but not yet taken by the HTTP/2
    /// encoder, bounded per peer by `RAFT_GRPC_APPEND_STREAM_MAX_QUEUED_BYTES`.
    pub raft_grpc_append_stream_queued_bytes: u64,
    pub raft_grpc_append_stream_queued_bytes_max: u64,
    /// Append calls refused because the peer's queue was full (the replication
    /// stream backs off instead of piling up more copies of its entries).
    pub raft_grpc_append_stream_backpressure_rejections: u64,
    /// Queued Append calls dropped unsent because their caller had already timed out.
    pub raft_grpc_append_stream_expired_unsent: u64,
    /// Append sessions closed because the peer stopped answering.
    pub raft_grpc_append_stream_stalls: u64,
    /// Follower side: decoded inbound Append frames not yet answered.
    pub raft_grpc_append_stream_server_buffered_bytes: u64,
    pub raft_grpc_append_stream_server_buffered_bytes_max: u64,
    /// Logical protobuf bytes before tonic's optional ZSTD compression and
    /// HTTP/2 framing. Compare these counters with VPC/CUR bytes to calculate
    /// transport and billing amplification.
    pub raft_grpc_append_heartbeat_requests: u64,
    pub raft_grpc_append_heartbeat_request_bytes: u64,
    pub raft_grpc_append_replication_requests: u64,
    pub raft_grpc_append_replication_request_bytes: u64,
    pub raft_grpc_append_replication_entries: u64,
    pub raft_grpc_append_response_bytes: u64,
    pub raft_grpc_vote_requests: u64,
    pub raft_grpc_vote_request_bytes: u64,
    pub raft_grpc_vote_response_bytes: u64,
    pub raft_grpc_snapshot_requests: u64,
    pub raft_grpc_snapshot_request_bytes: u64,
    pub raft_grpc_snapshot_payload_bytes: u64,
    pub raft_grpc_snapshot_response_bytes: u64,
}

/// Snapshot of one group's bounded-state gauges.
///
/// Counts are exact. Byte figures are length-based estimates (element counts
/// times in-memory element sizes, without allocator slack), which is what the
/// §7.2 formula checks compare against.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct GroupStateGauges {
    /// Live streams in the group.
    pub streams: u64,
    /// Shared pack-slice references held in stream state (F2).
    pub shared_refs: u64,
    /// Largest shared-reference list held by one stream.
    pub max_shared_refs_per_stream: u64,
    /// Distinct live shared pack objects in the group maps (F2).
    pub live_packs: u64,
    /// External payload locators held in stream state (F5).
    pub staged_external_refs: u64,
    /// Producer ids across streams (F3).
    pub producers: u64,
    /// Largest producer map held by one stream.
    pub max_producers_per_stream: u64,
    /// Producer receipts across streams (F3).
    pub receipts: u64,
    /// Receipts counted against the F3 window (one item per receipt; target:
    /// 1,024 per stream beyond each producer's newest).
    pub receipt_items: u64,
    /// Largest receipt count held by one stream.
    pub max_receipt_items_per_stream: u64,
    /// Length-based producer state bytes across streams (F3 `Prod(s)`).
    pub producer_bytes: u64,
    /// Largest length-based producer state of one stream.
    pub max_producer_bytes_per_stream: u64,
    /// Live streams with a TTL or absolute expiry.
    pub ttl_streams: u64,
    /// Entries in the node-local TTL heap, stale ones included (F8 target:
    /// at most two per TTL stream).
    pub ttl_heap_entries: u64,
    /// Unflushed payload bytes (the group hot gauge).
    pub hot_payload_bytes: u64,
    /// Hot blocks of up to 64 KiB (F6b; one per append before it).
    pub hot_chunks: u64,
    /// Hot-window block headers beyond payload (F6b).
    pub hot_overhead_bytes: u64,
    /// Pending cold-GC queue entries (F14).
    pub pending_cold_gc: u64,
    /// Per-bucket usage rows (F15, by design O(buckets ever written)).
    pub bucket_usage_rows: u64,
    /// Tenant-erasure fences (F15, by design O(buckets ever purged)).
    pub erased_buckets: u64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct ColdStoreMetrics {
    pub backend: String,
    pub root: Option<String>,
    pub bucket: Option<String>,
    pub region: Option<String>,
    pub endpoint: Option<String>,
    pub encryption: Option<String>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum GroupGaugeCollection {
    Groups(Vec<GroupGaugeMetrics>),
    Error { error: String },
}
impl Default for GroupGaugeCollection {
    fn default() -> Self {
        Self::Groups(Vec::new())
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroupGaugeMetrics {
    pub raft_group_id: u32,
    pub hosted: bool,
    #[serde(flatten)]
    pub gauges: Option<GroupStateGauges>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WalSyncPolicy {
    Never,
    Always,
}
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum PreviousWalRun {
    Absent,
    Unrecorded,
    Clean,
    ProcessCrash,
    HostCrash { fsync: WalSyncPolicy },
    Poisoned,
}
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WalReplayMode {
    Strict,
    VerifiedPrefix,
}
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WalRecoveryReason {
    HostCrash,
    Poisoned,
    UnknownHistory,
}
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum WalRecoveryState {
    Normal,
    Recovering { reason: WalRecoveryReason },
}
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WalJournalSync {
    NotNeeded,
    BeforeRecording,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalRecoveryMetrics {
    pub fsync: WalSyncPolicy,
    pub previous_run: PreviousWalRun,
    pub replay_mode: WalReplayMode,
    pub recovery: WalRecoveryState,
    pub recovery_epoch: u64,
    pub journal_sync: Option<WalJournalSync>,
}
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct NodeDiagnostics {
    #[serde(flatten)]
    pub runtime: RuntimeMetricsSnapshot,
    #[serde(flatten)]
    pub http: HttpMetricsSnapshot,
    #[serde(flatten)]
    pub raft_grpc: RaftGrpcMetricsSnapshot,
    pub active_cores: usize,
    pub active_groups: usize,
    pub mailbox_depths: Vec<usize>,
    pub mailbox_capacities: Vec<usize>,
    pub cold_store: ColdStoreMetrics,
    pub raft_group_count: usize,
    pub configured_raft_group_count: u32,
    pub group_state_gauges: GroupGaugeCollection,
    pub process_rss_bytes: u64,
    pub node_memory_abort_cap_bytes: u64,
    pub wal_recovery: Option<WalRecoveryMetrics>,
    pub recovery_gates: Option<crate::admin::RecoveryGatesReport>,
    pub wal_available_bytes: u64,
    pub wal_min_available_bytes: u64,
    pub wal_resume_available_bytes: u64,
    pub wal_disk_pressure: bool,
    pub wal_disk_stat_errors: u64,
}

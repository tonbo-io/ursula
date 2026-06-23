use std::path::PathBuf;

use serde::Deserialize;
use serde::Serialize;

use crate::human::HumanDuration;
use crate::human::HumanSize;

/// Cold-storage backend selector.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum ColdBackend {
    #[default]
    #[serde(alias = "disabled", alias = "off")]
    None,
    #[serde(alias = "mem", alias = "inmem")]
    Memory,
    S3,
}

/// Raft WAL persistence backend selector.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum WalBackend {
    #[default]
    Memory,
    Disk,
}

/// Raft snapshot store backend selector.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum RaftSnapshotBackend {
    #[default]
    #[serde(alias = "default", alias = "")]
    Inline,
    Local,
    S3,
}

/// Top-level Ursula server configuration.
///
/// Populated from a config file (TOML/JSON/YAML), an optional preset, and
/// CLI overrides.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct UrsulaConfig {
    pub server: ServerConfig,
    pub runtime: RuntimeConfig,
    pub raft: RaftConfig,
    pub storage: StorageConfig,
    pub governance: GovernanceConfig,
    pub observability: ObservabilityConfig,
}

/// HTTP server binding and admission settings.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct ServerConfig {
    /// Public HTTP client API bind address.
    pub listen: String,
    /// Optional separate bind for the cluster / Raft gRPC plane.
    /// When omitted, both planes share `listen`.
    pub cluster_listen: Option<String>,
    /// Process-wide cap on accepted write body bytes held by the HTTP layer.
    pub http_inflight_body_size: HumanSize,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            listen: "127.0.0.1:4437".to_string(),
            cluster_listen: None,
            http_inflight_body_size: HumanSize::mib(256),
        }
    }
}

/// Per-core runtime sizing and admission controls.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct RuntimeConfig {
    /// Number of CPU cores / tokio worker threads to use.
    pub core_count: usize,
    /// Soft RSS cap. When the process RSS exceeds this value, new writes are
    /// rejected with HTTP 503. `None` disables the monitor.
    pub node_memory_abort_cap_size: Option<HumanSize>,
    /// Minimum payload size that triggers external cold-store staging instead
    /// of inline hot-ring storage. `None` uses the default (1 MiB).
    pub external_payload_min_size: Option<HumanSize>,
    /// Max live-read waiters per core. `None` or `0` disables the limit.
    pub live_read_max_waiters_per_core: Option<usize>,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            core_count: std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(4),
            node_memory_abort_cap_size: None,
            external_payload_min_size: None,
            live_read_max_waiters_per_core: Some(65_536),
        }
    }
}

/// Raft consensus and static-cluster networking configuration.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct RaftConfig {
    /// Unique node ID within the static gRPC Raft cluster.
    /// Must be present in `peers` and must be non-zero.
    pub node_id: u64,
    /// Number of Raft groups (shards). Defaults to `core_count * 16`.
    pub group_count: usize,
    /// Per-group cap on raft-submitted-but-not-yet-applied payload bytes.
    /// `None` or `0` disables the admission. Catches raft replication lag before
    /// in-memory queues grow unbounded.
    pub max_uncommitted_size_per_group: Option<HumanSize>,
    /// Bootstrap the initial Raft membership once on startup.
    pub init_membership: bool,
    /// Bootstrap per-group Raft membership on startup.
    pub init_membership_per_group: bool,
    /// Raft WAL configuration.
    pub wal: WalConfig,
    /// Static gRPC Raft peers. Each entry maps a `node_id` to its gRPC URL.
    pub peers: Vec<RaftPeerConfig>,
    /// Optional per-group voter assignments.
    ///
    /// When empty (the default), every Raft group uses all peers as voters.
    /// When supplied, every group in `0..group_count` must have an entry and
    /// each entry's voters must be a non-empty subset of `peers`.
    #[serde(default)]
    pub groups: Vec<RaftGroupConfig>,
    /// How long a restarting node waits to observe an already-established
    /// (or freshly re-elected) leader before deciding the group is truly new
    /// and bootstrapping it. Must exceed the election window.
    pub rejoin_probe: HumanDuration,
    /// Timeout for probing static peers during bootstrap before logging a
    /// warning. Continues retrying indefinitely.
    pub bootstrap_peer_probe: HumanDuration,
    /// Interval between static-peer reachability probes during bootstrap.
    pub bootstrap_peer_probe_interval: HumanDuration,
    /// gRPC connect timeout when probing static peers.
    pub bootstrap_peer_connect: HumanDuration,
    /// OpenRaft's `install_snapshot_timeout` covers the whole FullSnapshot RPC.
    /// The receiver downloads and installs the referenced object before
    /// replying, so this must be comfortably above the S3 per-attempt timeout
    /// plus retries.
    pub install_snapshot_timeout: HumanDuration,
    /// Directory for memory-bootstrap marker files. When set, each group
    /// writes a marker after successful membership initialization. On restart,
    /// a marked memory group rejoins an observed leader or reinitializes
    /// volatile membership if no leader exists.
    pub memory_bootstrap_marker_dir: Option<PathBuf>,
    /// Consecutive gRPC RPC failures before forcing a transport reconnect.
    pub grpc_reconnect_after_failures: usize,
    /// Max concurrent snapshot installs across all groups on this node.
    pub snapshot_install_max_concurrency: usize,
}

impl Default for RaftConfig {
    fn default() -> Self {
        Self {
            node_id: 0,
            group_count: std::thread::available_parallelism()
                .map(|n| n.get().saturating_mul(16).max(1))
                .unwrap_or(16),
            max_uncommitted_size_per_group: None,
            init_membership: false,
            init_membership_per_group: false,
            wal: WalConfig::default(),
            peers: Vec::new(),
            groups: Vec::new(),
            rejoin_probe: HumanDuration::sec(6),
            bootstrap_peer_probe: HumanDuration::sec(60),
            bootstrap_peer_probe_interval: HumanDuration::milli(250),
            bootstrap_peer_connect: HumanDuration::milli(500),
            install_snapshot_timeout: HumanDuration::sec(120),
            memory_bootstrap_marker_dir: None,
            grpc_reconnect_after_failures: 8,
            snapshot_install_max_concurrency: 1,
        }
    }
}

/// Raft write-ahead log configuration.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct WalConfig {
    /// WAL persistence backend.
    pub backend: WalBackend,
    /// Directory for on-disk WAL files. Required when `backend` is `Disk`.
    pub path: Option<PathBuf>,
}

impl WalConfig {
    /// Resolved on-disk log directory for the Raft WAL.
    ///
    /// When `backend` is `Disk` and `path` is set, appends the legacy
    /// `raft-log` subdirectory so that existing data directories continue
    /// to work after the config refactor.
    pub fn resolved_path(&self) -> Option<PathBuf> {
        match self.backend {
            WalBackend::Memory => None,
            WalBackend::Disk => self.path.as_ref().map(|p| p.join("raft-log")),
        }
    }
}

impl Default for WalConfig {
    fn default() -> Self {
        Self {
            backend: WalBackend::Memory,
            path: None,
        }
    }
}

/// A single static gRPC Raft peer.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RaftPeerConfig {
    /// Peer node ID.
    pub node_id: u64,
    /// Peer gRPC URL.
    pub url: String,
}

/// Per-group voter assignment for heterogeneous static clusters.
///
/// When omitted (the default), every group uses all peers as voters.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RaftGroupConfig {
    /// Raft group ID.
    pub raft_group_id: u32,
    /// Node IDs that are voters for this group.
    pub voters: Vec<u64>,
}

/// Storage tier configuration.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct StorageConfig {
    /// Cold-tier (opendal-backed object store) configuration.
    pub cold: ColdConfig,
    /// Raft snapshot store configuration.
    pub snapshot: RaftSnapshotConfig,
}

/// Cold-tier flush, GC, and cache configuration.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct ColdConfig {
    /// Cold-storage backend.
    pub backend: ColdBackend,
    /// Root prefix for cold-storage objects (e.g. S3 prefix or local dir).
    pub root: Option<String>,
    /// S3-specific connection and credential settings.
    /// Required when `backend` is `S3`.
    pub s3: Option<S3Config>,
    /// Optional cold-read cache.
    pub cache: Option<ColdCacheConfig>,
    /// Interval between periodic cold-flush passes. Must be non-zero.
    pub flush_interval: HumanDuration,
    /// Target number of hot bytes to flush per group per pass.
    pub flush_size: HumanSize,
    /// Minimum hot bytes a group must have before it is eligible for flush.
    /// Falls back to [`flush_size`](Self::flush_size) when unset.
    pub flush_min_hot_size: Option<HumanSize>,
    /// Upper bound on bytes flushed per group per pass.
    /// Falls back to [`flush_size`](Self::flush_size) when unset.
    pub flush_max_size: Option<HumanSize>,
    /// Max groups flushed concurrently.
    pub flush_max_concurrency: usize,
    /// Per-group hot-size cap. When a group's hot bytes exceed this, new
    /// writes are rejected with HTTP 503. `None` or `0` disables the admission.
    pub max_hot_size_per_group: Option<HumanSize>,
    /// Interval between periodic cold-gc passes. Must be non-zero.
    pub gc_interval: HumanDuration,
    /// Max GC entries to process per group per pass.
    pub gc_max_entries: usize,
}

impl ColdConfig {
    /// Minimum hot bytes a group must have before it is eligible for flush.
    ///
    /// Falls back to [`flush_size`](Self::flush_size) when the user does not
    /// supply an explicit value.
    pub fn flush_min_hot_size(&self) -> HumanSize {
        self.flush_min_hot_size.unwrap_or(self.flush_size)
    }

    /// Upper bound on bytes flushed per group per pass.
    ///
    /// Falls back to [`flush_size`](Self::flush_size) when the user does not
    /// supply an explicit value.
    pub fn flush_max_size(&self) -> HumanSize {
        self.flush_max_size.unwrap_or(self.flush_size)
    }
}

impl Default for ColdConfig {
    fn default() -> Self {
        Self {
            backend: ColdBackend::None,
            root: None,
            s3: None,
            cache: None,
            flush_interval: HumanDuration::sec(1),
            flush_size: HumanSize::mib(8),
            flush_min_hot_size: None,
            flush_max_size: None,
            flush_max_concurrency: 4,
            max_hot_size_per_group: Some(HumanSize::mib(64)),
            gc_interval: HumanDuration::sec(5),
            gc_max_entries: 256,
        }
    }
}

/// S3 connection and credential settings.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct S3Config {
    /// S3 bucket name.
    pub bucket: Option<String>,
    /// S3 region.
    pub region: Option<String>,
    /// Custom S3 endpoint (for MinIO, etc.).
    pub endpoint: Option<String>,
    /// S3 access key ID.
    pub access_key_id: Option<String>,
    /// S3 secret access key.
    pub secret_access_key: Option<String>,
    /// Optional S3 session token.
    pub session_token: Option<String>,
    /// Per-S3-operation timeout.
    pub timeout: HumanDuration,
    /// Max retries per S3 operation.
    pub max_retries: usize,
    /// Timeout for S3 health probes.
    pub probe_timeout: HumanDuration,
    /// Consecutive probe failures before marking S3 unhealthy.
    pub unhealthy_ticks: usize,
    /// Consecutive probe successes before marking S3 healthy again.
    pub heal_ticks: usize,
}

impl Default for S3Config {
    fn default() -> Self {
        Self {
            bucket: None,
            region: None,
            endpoint: None,
            access_key_id: None,
            secret_access_key: None,
            session_token: None,
            timeout: HumanDuration::sec(10),
            max_retries: 3,
            probe_timeout: HumanDuration::sec(2),
            unhealthy_ticks: 1,
            heal_ticks: 2,
        }
    }
}

/// Cold-read cache sizing.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct ColdCacheConfig {
    /// Max cache size in bytes.
    pub max_size: HumanSize,
    /// Cache block size in bytes.
    pub block_size: HumanSize,
    /// Number of blocks to read ahead on cache miss.
    pub readahead_blocks: usize,
}

impl Default for ColdCacheConfig {
    fn default() -> Self {
        Self {
            max_size: HumanSize::mib(256),
            block_size: HumanSize::mib(1),
            readahead_blocks: 4,
        }
    }
}

/// Raft snapshot store configuration.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct RaftSnapshotConfig {
    /// Snapshot store backend.
    pub backend: RaftSnapshotBackend,
    /// Root directory for local snapshot storage.
    /// Required when `backend` is `Local`.
    pub local_root: Option<PathBuf>,
    /// S3 prefix for snapshot objects. Used only when `backend` is `S3`.
    pub s3_prefix: Option<String>,
    /// Interval for the manual snapshot driver.
    ///
    /// When omitted, inline snapshot stores keep the manual driver disabled and
    /// external snapshot stores use a 60s manual-driver default. Explicit `0s`
    /// disables the manual driver and keeps openraft's default auto-policy.
    pub drive_interval: Option<HumanDuration>,
    /// Max concurrent snapshot flushes.
    pub drive_flush_concurrency: usize,
}

impl Default for RaftSnapshotConfig {
    fn default() -> Self {
        Self {
            backend: RaftSnapshotBackend::Inline,
            local_root: None,
            s3_prefix: None,
            drive_interval: None,
            drive_flush_concurrency: 4,
        }
    }
}

/// Cluster governance and health-gate configuration.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct GovernanceConfig {
    /// Leadership balancing configuration.
    pub leadership_balance: LeadershipBalanceConfig,
    /// Cluster egress probe configuration.
    pub cluster_probe: ClusterProbeConfig,
    /// Commit-stall watchdog configuration.
    pub commit_stall: CommitStallConfig,
    /// Cold-storage health gate configuration.
    pub cold_health: ColdHealthConfig,
}

/// Leadership balancer tuning.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct LeadershipBalanceConfig {
    /// Tick interval for the leadership balancer.
    pub interval: HumanDuration,
    /// Max leader handoffs to attempt per tick.
    pub max_per_tick: usize,
    /// Timeout when querying peer shed state.
    pub peer_timeout: HumanDuration,
}

impl Default for LeadershipBalanceConfig {
    fn default() -> Self {
        Self {
            interval: HumanDuration::sec(5),
            max_per_tick: 4,
            peer_timeout: HumanDuration::milli(500),
        }
    }
}

/// Cluster egress probe tuning.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct ClusterProbeConfig {
    /// Tick interval for egress probes.
    pub interval: HumanDuration,
    /// Payload size for egress probe messages.
    pub probe_size: HumanSize,
    /// Timeout for individual egress probes.
    pub timeout: HumanDuration,
    /// Consecutive failed ticks before marking egress unhealthy.
    pub unhealthy_ticks: usize,
    /// Consecutive healthy ticks before clearing egress unhealthy.
    pub heal_ticks: usize,
}

impl Default for ClusterProbeConfig {
    fn default() -> Self {
        Self {
            interval: HumanDuration::milli(500),
            probe_size: HumanSize::kib(64),
            timeout: HumanDuration::milli(200),
            unhealthy_ticks: 2,
            heal_ticks: 6,
        }
    }
}

/// Commit-stall watchdog tuning.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct CommitStallConfig {
    /// Tick interval for the commit-stall watchdog.
    pub interval: HumanDuration,
    /// Duration a group must be stalled (`last_log_index > committed_index`)
    /// before triggering a leader transfer.
    pub threshold: HumanDuration,
}

impl Default for CommitStallConfig {
    fn default() -> Self {
        Self {
            interval: HumanDuration::sec(2),
            threshold: HumanDuration::sec(15),
        }
    }
}

/// Cold-storage health gate tuning.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct ColdHealthConfig {
    /// Tick interval for the cold-health gate.
    pub interval: HumanDuration,
    /// Consecutive unhealthy ticks before shedding leadership.
    pub unhealthy_ticks: usize,
    /// Consecutive healthy ticks before re-allowing leadership.
    pub heal_ticks: usize,
    /// High watermark for per-group hot bytes. Exceeding this contributes to
    /// unhealthy.
    pub hot_size_high: HumanSize,
    /// Low watermark for per-group hot bytes. Dropping below this contributes
    /// to healthy.
    pub hot_size_low: HumanSize,
    /// Error-count threshold per tick that marks cold as unhealthy.
    pub errors_per_tick_high: usize,
}

impl Default for ColdHealthConfig {
    fn default() -> Self {
        Self {
            interval: HumanDuration::sec(2),
            unhealthy_ticks: 3,
            heal_ticks: 5,
            hot_size_high: HumanSize::mib(7),
            hot_size_low: HumanSize::mib(4),
            errors_per_tick_high: 1,
        }
    }
}

/// Observability and debugging features.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct ObservabilityConfig {
    /// Enable tokio-console integration.
    pub tokio_console: bool,
}

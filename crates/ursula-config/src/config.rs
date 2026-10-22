use std::path::PathBuf;
use std::time::Duration;

use serde::Deserialize;
use serde::Serialize;
use ursula_control::MoveLimit;
use ursula_headers::ClusterId;
use ursula_proto::placement::ReplicationFactor;
use ursula_proto::placement::VoterSet;

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

/// When appends to the Raft WAL journal reach stable storage.
///
/// Votes, the per-group log states and the node's run state are always
/// written with `fsync`, whatever this policy says.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum WalFsync {
    /// Acknowledge a batch of appends only after it is `fsync`ed.
    Always,
    /// Acknowledge appends once they reach the page cache. A process crash
    /// loses nothing. A host crash can lose the replica's unsynced tail: the
    /// replica then stays out of elections until it has caught up from the
    /// group's leader, and a group whose majority lost it stops until an
    /// operator accepts the loss.
    Never,
}

/// Raft snapshot store backend selector.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum RaftSnapshotBackend {
    /// S3 whenever an S3 cold store is configured, inline otherwise
    /// (bounded-stream-state F12b).
    #[default]
    Auto,
    #[serde(alias = "default", alias = "")]
    Inline,
    S3,
}

impl RaftSnapshotBackend {
    /// The concrete backend for a node whose cold store uses `cold_backend`:
    /// [`Self::Auto`] picks S3 for an S3 cold store and inline otherwise.
    pub fn resolve(self, cold_backend: ColdBackend) -> Self {
        match self {
            Self::Auto if cold_backend == ColdBackend::S3 => Self::S3,
            Self::Auto => Self::Inline,
            other => other,
        }
    }
}

/// Top-level Ursula server configuration.
///
/// Populated from a TOML config file, an optional preset, and CLI overrides.
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
    /// Optional separate bind for the cluster plane (Raft gRPC, the
    /// peers' egress probe and leadership-shed state). When omitted, both
    /// planes share `listen`. When set, every `[[raft.peers]]` `url` names a
    /// peer's cluster listener and every peer needs a `client_url`.
    pub cluster_listen: Option<String>,
    /// Admin-plane bind for mutating operator endpoints (raft ops,
    /// maintenance drain, cold-flush trigger). Loopback by default so nodes
    /// expose no cluster-mutation surface on the network; operator tooling
    /// reaches it through an SSH/SSM/port-forward tunnel.
    pub admin_listen: String,
    /// Process-wide cap on accepted write body bytes held by the HTTP layer.
    pub http_inflight_body_size: HumanSize,
    /// The id of the deployment, sent as `Ursula-Cluster-Id` on every HEAD:
    /// 1 to 64 bytes of `[A-Za-z0-9._-]`. Required whenever `raft.peers` is
    /// set, the same on every node and different for every deployment one
    /// client can reach. A single node without it generates one at its first
    /// start and keeps it in its WAL. The WAL records the id, and a node whose
    /// configured id differs from its WAL's refuses to start.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cluster_id: Option<ClusterId>,
}

impl ServerConfig {
    /// The smallest `http_inflight_body_size` a node with a cold backend
    /// accepts: one request of the 32 MiB limit, plus the copy of one
    /// snapshot commit at its least read-ahead (32 MiB), so that a commit
    /// is never refused forever for the budget.
    pub const MIN_INFLIGHT_BODY_SIZE_WITH_COLD: u64 = 64 * 1024 * 1024;
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            listen: "127.0.0.1:4437".to_string(),
            cluster_listen: None,
            admin_listen: "127.0.0.1:4438".to_string(),
            http_inflight_body_size: HumanSize::mib(256),
            cluster_id: None,
        }
    }
}

/// Per-core runtime sizing and admission controls.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct RuntimeConfig {
    /// Number of CPU cores / tokio worker threads to use.
    pub core_count: usize,
    /// CPU affinity: None pins only when the allowed cpuset equals core_count.
    /// true forces pinning; false disables it.
    pub cpu_affinity: Option<bool>,
    /// Emergency RSS abort threshold. Exceeding it aborts the process; it does
    /// not reject writes with HTTP 503 or perform a graceful leadership handoff.
    /// `None` disables the monitor.
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
            cpu_affinity: None,
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
    /// Node-wide append transport bytes per direction, shared by all cores/peers.
    pub append_transport_budget_bytes: usize,
    /// Unique node ID within the static gRPC Raft cluster.
    /// Must be present in `peers` and must be non-zero.
    pub node_id: u64,
    /// Number of Raft groups (shards), at most
    /// [`ursula_proto::placement::MAX_GROUP_COUNT`]. Defaults to
    /// `core_count * 16` within that bound.
    pub group_count: usize,
    /// Per-group cap on raft-submitted-but-not-yet-applied payload bytes.
    /// `None` or `0` disables the admission. Catches raft replication lag before
    /// in-memory queues grow unbounded.
    pub max_uncommitted_size_per_group: Option<HumanSize>,
    /// Raft WAL configuration.
    pub wal: WalConfig,
    /// Static gRPC Raft peers. Each entry maps a `node_id` to its gRPC URL.
    pub peers: Vec<RaftPeerConfig>,
    /// The meta group's voters at its genesis: ids of `raft.peers`, or this
    /// node's id when `raft.peers` is empty. Unset, the smallest
    /// `min(3, N)` ids of the address book ([`RaftConfig::address_book`]).
    /// Read at genesis only: the meta group's committed voters rule after
    /// it. A repeated id counts once, and an empty list is refused.
    pub meta_voters: Option<VoterSet>,
    /// How the meta group places the data groups.
    pub placement: PlacementConfig,
    /// How often a group's initializer that holds nothing of the group logs
    /// that it still waits for every voter to report an empty group before
    /// it initializes the group. It keeps probing indefinitely.
    pub bootstrap_peer_probe: HumanDuration,
    /// Interval between bootstrap probes of the other voters.
    pub bootstrap_peer_probe_interval: HumanDuration,
    /// Timeout of one bootstrap probe of another voter.
    pub bootstrap_peer_connect: HumanDuration,
    /// OpenRaft's `install_snapshot_timeout` covers the whole FullSnapshot RPC.
    /// The receiver downloads the referenced object, writes its own copy of
    /// it and checks the copy before it installs and replies, so on a
    /// cluster node with S3 snapshots it must cover those requests under
    /// `storage.snapshot.s3_timeout` and `s3_max_retries`
    /// ([`RaftSnapshotConfig::install_copy_budget`]), or the config is
    /// refused. A node also runs a further snapshot GC pass this long after
    /// a write of its own that failed, for a PUT that lands late.
    pub install_snapshot_timeout: HumanDuration,
    /// Consecutive gRPC RPC failures before forcing a transport reconnect.
    pub grpc_reconnect_after_failures: usize,
    /// Max concurrent snapshot builds across all groups on this node. Each
    /// build holds a copy of its group's state and runs its snapshot-store
    /// requests one after another. The snapshot driver starts a build only
    /// while fewer run than the node's log calls for: one while the log is
    /// below `snapshot_log_budget`, and past it more, in even steps up to
    /// this many before the hard limit (twice the budget). Builds it started
    /// past the budget finish after the log falls below it. Between 1 and
    /// [`RaftConfig::MAX_SNAPSHOT_BUILD_CONCURRENCY`].
    pub snapshot_build_max_concurrency: usize,
    /// Max concurrent snapshot installs across all groups on this node.
    pub snapshot_install_max_concurrency: usize,
    /// Maximum groups snapshotted by one driver tick, including pressure
    /// passes, while the node's log is below `snapshot_log_budget`. Past it
    /// a pass starts this many for each build it keeps in flight, so it
    /// lasts about as long.
    pub snapshot_pressure_max_groups_per_tick: usize,
    /// Node log-byte budget of the snapshot driver (bounded-stream-state
    /// F12e). A group snapshots once the log it applied since its last
    /// snapshot reaches `max(F, 2 × that snapshot's size)`, where the floor
    /// `F` is this budget divided by twice the group count, at most 16 MiB.
    /// Once the node's groups hold three quarters of the budget, a pressure
    /// pass snapshots the groups with the most log first, and past the
    /// budget it runs more builds at once (`snapshot_build_max_concurrency`).
    /// Over twice the budget, the hard limit, the node refuses client writes
    /// with a body until its log is below one and a half budgets.
    pub snapshot_log_budget: HumanSize,
    /// Far backstop of the snapshot driver: applied entries since a group's
    /// last snapshot after which it snapshots whatever its log bytes.
    pub snapshot_backstop_logs: u64,
    /// Maximum number of payload-bearing Raft log entries retained per group
    /// after they are covered by a snapshot.
    pub max_in_snapshot_log_to_keep: u64,
    /// How long a replica keeps a deleted, purged or expired stream in its
    /// current Raft snapshot and its Raft log after it applies the removal
    /// (#669). The snapshot driver snapshots a group a quarter of this
    /// after its oldest removal no snapshot covers, sooner when the removed
    /// bytes reach the group's snapshot threshold, and then purges the
    /// group's log through that snapshot. Unset, it is the larger of one
    /// hour and eight times `install_snapshot_timeout`
    /// ([`RaftConfig::effective_deleted_data_max_age`]). A set value must be
    /// at least eight times `install_snapshot_timeout`.
    pub deleted_data_max_age: Option<HumanDuration>,
}

impl RaftConfig {
    /// The most snapshot builds a node may run at once
    /// (`snapshot_build_max_concurrency`). Each build holds a copy of its
    /// group's state and, while it uploads, its compressed snapshot, so 16
    /// builds of large groups already hold gigabytes. A larger value is
    /// taken for a mistake.
    pub const MAX_SNAPSHOT_BUILD_CONCURRENCY: usize = 16;
    /// `deleted_data_max_age` is at least this many `install_snapshot_timeout`s
    /// (#669). With `A` the age, `I` the install timeout and `D` the snapshot
    /// drive interval, a replica snapshots a group `A/4` after a removal, and
    /// deletes the object that snapshot replaced once no send of it is in
    /// flight, which a send's timeout bounds by `I`, at its next GC pass,
    /// within `D`. So the removal is gone by `A/4 + I + D`, within the age
    /// whenever `A >= 4(I + D)/3`. Eight is a margin chosen on top of that,
    /// not derived from it. A GC pass that fails runs again after a back-off
    /// of its own, 30 seconds doubled per failure up to 15 minutes, which
    /// this margin covers only for the first retries.
    pub const DELETED_DATA_MAX_AGE_INSTALLS: u32 = 8;
    /// `deleted_data_max_age` when it is unset and the install timeout
    /// allows it.
    pub const DEFAULT_DELETED_DATA_MAX_AGE: std::time::Duration =
        std::time::Duration::from_secs(60 * 60);

    /// This node's address book: `raft.peers`, or, when it is empty, this
    /// node alone at `self_url`, so every Raft node can run the meta group.
    /// The data topology does not read it. Removed in S2 (#768), where
    /// every node with `raft.wal.path` lists itself in `raft.peers`.
    pub fn address_book(&self, self_url: &str) -> Vec<RaftPeerConfig> {
        if !self.peers.is_empty() {
            return self.peers.clone();
        }
        vec![RaftPeerConfig {
            node_id: self.node_id,
            url: self_url.to_owned(),
            client_url: None,
        }]
    }

    /// The journal policy the node runs: `raft.wal.fsync`, or, unset,
    /// `always` when the node's groups have one or two voters and `never`
    /// otherwise. A group of one or two voters has no quorum without a
    /// replica that a host crash under `never` gates, so only `always`
    /// lets it restart unattended after one. The voter count is
    /// `min(raft.placement.replication_factor, N)` over the N nodes of the
    /// address book: `raft.peers`, or this node alone when it is empty.
    pub fn wal_fsync(&self) -> WalFsync {
        if let Some(fsync) = self.wal.fsync {
            return fsync;
        }
        let nodes = self.peers.len().max(1);
        let voters = nodes.min(usize::from(self.placement.replication_factor.get()));
        if voters <= 2 {
            WalFsync::Always
        } else {
            WalFsync::Never
        }
    }

    /// The shortest `deleted_data_max_age` this config allows.
    pub fn min_deleted_data_max_age(&self) -> std::time::Duration {
        self.install_snapshot_timeout
            .as_duration()
            .saturating_mul(Self::DELETED_DATA_MAX_AGE_INSTALLS)
    }

    /// The bound on deleted data the node keeps: the configured
    /// `deleted_data_max_age`, or, unset, the larger of one hour and
    /// [`RaftConfig::min_deleted_data_max_age`], so a config without it
    /// stays valid whatever its install timeout.
    pub fn effective_deleted_data_max_age(&self) -> std::time::Duration {
        self.deleted_data_max_age.map_or_else(
            || Self::DEFAULT_DELETED_DATA_MAX_AGE.max(self.min_deleted_data_max_age()),
            |configured| configured.as_duration(),
        )
    }
}

impl Default for RaftConfig {
    fn default() -> Self {
        Self {
            node_id: 0,
            // At most the bound `validate` checks, so a large host's
            // default stays valid.
            group_count: std::thread::available_parallelism()
                .map(|n| n.get().saturating_mul(16).max(1))
                .unwrap_or(16)
                .min(
                    usize::try_from(ursula_proto::placement::MAX_GROUP_COUNT).unwrap_or(usize::MAX),
                ),
            max_uncommitted_size_per_group: None,
            wal: WalConfig::default(),
            peers: Vec::new(),
            meta_voters: None,
            placement: PlacementConfig::default(),
            bootstrap_peer_probe: HumanDuration::sec(60),
            bootstrap_peer_probe_interval: HumanDuration::milli(250),
            bootstrap_peer_connect: HumanDuration::milli(500),
            install_snapshot_timeout: HumanDuration::sec(120),
            append_transport_budget_bytes: 128 * 1024 * 1024,
            grpc_reconnect_after_failures: 8,
            snapshot_build_max_concurrency: 2,
            snapshot_install_max_concurrency: 1,
            snapshot_pressure_max_groups_per_tick: 16,
            snapshot_log_budget: HumanSize::gib(1),
            snapshot_backstop_logs: 100_000,
            max_in_snapshot_log_to_keep: 64,
            deleted_data_max_age: None,
        }
    }
}

/// Raft write-ahead log configuration.
///
/// The WAL is always the per-core disk journal.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct WalConfig {
    /// Directory for the WAL files; the journals live in its `raft-log`
    /// subdirectory. Required whenever `raft.peers` is set. A single node
    /// without a path runs its WAL in a fresh temporary directory that is
    /// removed on clean shutdown.
    pub path: Option<PathBuf>,
    /// When journal appends reach stable storage. Unset, it resolves from
    /// the replication factor ([`RaftConfig::wal_fsync`]).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fsync: Option<WalFsync>,
    /// The size at which each core's journal starts a new segment. Purge
    /// deletes whole segments, so it is also the granularity at which disk
    /// space comes back.
    pub segment_size: HumanSize,
    /// Recent Raft entries kept in memory across the node, split evenly
    /// between its Raft groups. Older entries are read from disk.
    pub cache_size: HumanSize,
    /// Reject writes and mark the node unready below this many available bytes.
    /// Zero disables disk-pressure admission.
    pub min_available_size: HumanSize,
    /// Clear disk-pressure admission only after free space reaches this value.
    /// Must exceed `min_available_size` when the guard is enabled.
    pub resume_available_size: HumanSize,
    /// The disk lease: with no synced probe of the WAL disk for this long,
    /// the node stops all Raft traffic and object writes until a probe
    /// succeeds again. From [`Self::MIN_LEASE`] to [`Self::MAX_LEASE`].
    pub lease: HumanDuration,
}

impl WalConfig {
    /// The subdirectory of a WAL directory that holds the journals.
    pub const LOG_SUBDIR: &str = "raft-log";
    /// The smallest `segment_size`.
    pub const MIN_SEGMENT_SIZE: u64 = 4 * 1024;
    /// The shortest `lease`: a probe every second must have room to miss.
    pub const MIN_LEASE: Duration = Duration::from_secs(2);
    /// The longest `lease`, which every successor's predecessor wait
    /// covers.
    pub const MAX_LEASE: Duration = Duration::from_secs(10);

    /// The entry cache of each group when the node hosts `groups` groups.
    pub fn group_cache_bytes(&self, groups: usize) -> u64 {
        let groups = u64::try_from(groups.max(1)).unwrap_or(u64::MAX);
        self.cache_size.as_bytes().checked_div(groups).unwrap_or(0)
    }

    /// The configured journal directory: `path`'s `raft-log`
    /// subdirectory. `None` when no path is configured.
    pub fn resolved_path(&self) -> Option<PathBuf> {
        self.path.as_ref().map(|path| path.join(Self::LOG_SUBDIR))
    }
}

impl Default for WalConfig {
    fn default() -> Self {
        Self {
            path: None,
            fsync: None,
            segment_size: HumanSize::mib(64),
            cache_size: HumanSize::mib(256),
            min_available_size: HumanSize::mib(512),
            resume_available_size: HumanSize::gib(1),
            lease: HumanDuration::sec(5),
        }
    }
}

/// A single static gRPC Raft peer.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RaftPeerConfig {
    /// Peer node ID.
    pub node_id: u64,
    /// URL peers dial for the cluster plane: the peer's `server.listen`, or
    /// its `server.cluster_listen` when it splits the cluster plane off.
    pub url: String,
    /// URL clients reach the peer's client plane at, the target of leader
    /// redirects. Required when `server.cluster_listen` is set, because `url`
    /// then serves no client route. Defaults to `url` otherwise.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_url: Option<String>,
}

impl RaftPeerConfig {
    /// The base URL of the peer's client plane: `client_url`, else `url`.
    pub fn client_base_url(&self) -> &str {
        self.client_url.as_deref().unwrap_or(&self.url)
    }
}

/// `[raft.placement]`: what the meta group places the data groups with.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct PlacementConfig {
    /// Voters of each data group, at least 1. The cluster runs with
    /// `min(value, N)` over its N nodes, and the genesis claim carries that
    /// effective value.
    pub replication_factor: ReplicationFactor,
    /// How many groups may move at once, recorded at `Bootstrap` only.
    /// Zero stops every move.
    pub max_concurrent_moves: MoveLimit,
}

impl PlacementConfig {
    pub const DEFAULT_REPLICATION_FACTOR: u8 = 3;
    pub const DEFAULT_MAX_CONCURRENT_MOVES: u32 = 4;
}

impl Default for PlacementConfig {
    fn default() -> Self {
        Self {
            replication_factor: ReplicationFactor::new(
                std::num::NonZeroU8::new(Self::DEFAULT_REPLICATION_FACTOR)
                    .unwrap_or(std::num::NonZeroU8::MIN),
            ),
            max_concurrent_moves: MoveLimit::new(Self::DEFAULT_MAX_CONCURRENT_MOVES),
        }
    }
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
    /// Aggregate hot bytes across locally led groups that activate a pressure
    /// flush. A pressure pass allows undersized groups to flush without
    /// changing their independent state ownership. `0` disables the fallback.
    pub flush_pressure_hot_size: HumanSize,
    /// Upper bound on bytes flushed per group per pass.
    /// Falls back to [`flush_size`](Self::flush_size) when unset.
    pub flush_max_size: Option<HumanSize>,
    /// Maximum hot age: a stream's hot tail older than this is flushed even
    /// when its group is below the flush threshold, which bounds how long a
    /// quiet stream's records stay hot (bounded-stream-state F10). `0`
    /// disables it.
    pub flush_max_hot_age: HumanDuration,
    /// Max groups flushed concurrently.
    pub flush_max_concurrency: usize,
    /// Enable background cold compaction (on by default, bounded-stream-state
    /// F14d): the same-stream chunk compactor, which drains compaction debt
    /// recorded by small flushes and the cold-index repair cursor and lists
    /// no objects, and the shared pack-reference driver, which rewrites a
    /// stream's packed slices into one exclusive chunk once it holds 64 of
    /// them or its tail has been idle for an hour (F2).
    pub compaction_enabled: bool,
    /// Interval between cold chunk compaction discovery passes.
    pub compaction_interval: HumanDuration,
    /// Preferred compacted object size.
    pub compaction_target_size: HumanSize,
    /// Hard maximum compacted object size.
    pub compaction_max_size: HumanSize,
    /// Maximum streams compacted per pass.
    pub compaction_max_streams_per_pass: usize,
    /// Grace period before compacted input objects are physically deleted.
    pub compaction_gc_grace: HumanDuration,
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
            flush_pressure_hot_size: HumanSize::mib(128),
            flush_max_size: None,
            flush_max_hot_age: HumanDuration::min(5),
            flush_max_concurrency: 4,
            compaction_enabled: true,
            compaction_interval: HumanDuration::sec(30),
            compaction_target_size: HumanSize::mib(8),
            compaction_max_size: HumanSize::mib(16),
            compaction_max_streams_per_pass: 16,
            compaction_gc_grace: HumanDuration::sec(300),
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
    /// Server-side encryption requested on every cold-tier object write.
    ///
    /// Defaults to `aes256` (SSE-S3): free on AWS S3 and the baseline for any
    /// shared deployment. MinIO only honors it when a KMS/KES is configured —
    /// set `none` explicitly for a MinIO deployment without one. `aws-kms`
    /// uses the AWS-managed KMS key unless `kms_key_id` names a customer
    /// managed key.
    pub server_side_encryption: S3ServerSideEncryption,
    /// Customer managed KMS key for `server_side_encryption = "aws-kms"`.
    pub kms_key_id: Option<String>,
    /// Per-S3-operation timeout.
    pub timeout: HumanDuration,
    /// Max retries per S3 operation.
    pub max_retries: usize,
    /// Deadline of one snapshot-store health probe, retries of the S3 client
    /// included. The default leaves room for one retry of a probe whose first
    /// attempt fails fast, since the client's first backoff is 1 to 2
    /// seconds. An attempt that hangs runs into this deadline instead. Must
    /// be non-zero.
    pub probe_timeout: HumanDuration,
    /// Bad health ticks, with no good tick between them, before the node
    /// sheds its leadership. A tick is bad when the probe fails or times out,
    /// or when a cold-flush write failed within the flush window and no write
    /// has succeeded since. The window spans as long as the S3 client can
    /// take to give up on a write (`timeout` and `max_retries`). A tick that
    /// records both failed and successful writes is inconclusive: it never
    /// ends a bad streak and never lengthens one, as in the cold-health gate.
    /// Must be non-zero.
    pub unhealthy_ticks: usize,
    /// Good health ticks in a row before a node that shed may lead again.
    /// After failed writes, the good ticks start with the first write that
    /// succeeds after the last failed one, or once the last failed one has
    /// left the flush window. Must be non-zero.
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
            server_side_encryption: S3ServerSideEncryption::default(),
            kms_key_id: None,
            timeout: HumanDuration::sec(10),
            max_retries: 3,
            // One failed probe the client is still retrying must not move
            // every leadership on the node (#508).
            probe_timeout: HumanDuration::sec(5),
            unhealthy_ticks: 3,
            heal_ticks: 2,
        }
    }
}

/// Backoff before the first retry of an S3 request. Each later retry waits
/// twice as long, up to [`S3_RETRY_MAX_DELAY`], and the jitter adds a random
/// share of up to the same delay again.
pub const S3_RETRY_MIN_DELAY: Duration = Duration::from_secs(1);
/// The longest backoff before a retry of an S3 request, before jitter.
pub const S3_RETRY_MAX_DELAY: Duration = Duration::from_secs(60);

/// The longest one S3 request can take before its caller sees it fail, with
/// a per-attempt `timeout` and `max_retries` retries: each of its
/// `max_retries + 1` attempts runs into `timeout`, and each backoff before a
/// retry runs to its jittered maximum, twice its delay.
pub fn s3_operation_budget(timeout: Duration, max_retries: usize) -> Duration {
    let attempts = u32::try_from(max_retries)
        .unwrap_or(u32::MAX)
        .saturating_add(1);
    let mut backoff = Duration::ZERO;
    let mut delay = S3_RETRY_MIN_DELAY;
    let mut retries_left = max_retries;
    while retries_left > 0 && delay < S3_RETRY_MAX_DELAY {
        backoff = backoff.saturating_add(delay.saturating_mul(2));
        delay = delay.saturating_mul(2).min(S3_RETRY_MAX_DELAY);
        retries_left = retries_left.saturating_sub(1);
    }
    // The delay has reached its cap: every retry left waits that long.
    let capped_retries = u32::try_from(retries_left).unwrap_or(u32::MAX);
    backoff = backoff.saturating_add(delay.saturating_mul(2).saturating_mul(capped_retries));
    timeout.saturating_mul(attempts).saturating_add(backoff)
}

/// Server-side encryption mode for cold-tier S3 writes.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum S3ServerSideEncryption {
    /// SSE-S3 (`x-amz-server-side-encryption: AES256`). The default.
    #[default]
    #[serde(rename = "aes256")]
    Aes256,
    /// SSE-KMS; uses the AWS managed key unless `kms_key_id` is set.
    AwsKms,
    /// No server-side encryption header. Required for object stores that
    /// reject the header (for example MinIO without a configured KMS).
    None,
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
    /// S3 namespace for snapshot objects, relative to the cold-storage root
    /// ([`SnapshotPrefix`], `snapshots` when unset). Used only when `backend`
    /// is `S3`.
    pub s3_prefix: Option<SnapshotPrefix>,
    /// Interval of the snapshot driver, which alone takes each group's
    /// snapshots, by Raft log bytes (bounded-stream-state F12e).
    ///
    /// When omitted, external snapshot stores use a 5s interval and inline
    /// snapshot stores 1s; only external stores gate the driver on store
    /// health. The driver always runs, so a zero interval is refused
    /// ([`crate::validate::ValidationError::ZeroSnapshotDriveInterval`]).
    pub drive_interval: Option<HumanDuration>,
    /// Per-attempt timeout of an S3 snapshot-store request. The snapshot
    /// store has a request budget of its own, apart from the cold store's
    /// (`storage.cold.s3.timeout`), so that a follower's copy of a snapshot
    /// it installs fits inside `raft.install_snapshot_timeout`
    /// ([`RaftSnapshotConfig::install_copy_budget`]). A PUT is one attempt
    /// of the whole object, so this also covers a healthy GET or PUT of the
    /// largest snapshot.
    pub s3_timeout: HumanDuration,
    /// Retries of an S3 snapshot-store request.
    pub s3_max_retries: usize,
}

impl RaftSnapshotConfig {
    /// The snapshot-store requests a follower's install copy can send one
    /// after another: the GET of the leader's object, the PUT of its own
    /// copy, a second PUT under a new name when the first one's name is
    /// taken, and the HEAD that checks the copy.
    pub const INSTALL_COPY_REQUESTS: u32 = 4;

    /// The snapshot prefix this config names, `snapshots` when unset.
    pub fn snapshot_prefix(&self) -> SnapshotPrefix {
        self.s3_prefix.clone().unwrap_or_default()
    }

    /// The longest one snapshot-store request can take
    /// ([`s3_operation_budget`]).
    pub fn request_budget(&self) -> Duration {
        s3_operation_budget(self.s3_timeout.as_duration(), self.s3_max_retries)
    }

    /// The longest a follower's install copy can take: its
    /// [`Self::INSTALL_COPY_REQUESTS`] requests, each to the end of
    /// [`Self::request_budget`]. It must fit inside
    /// `raft.install_snapshot_timeout`, or the leader's send ends first and
    /// the follower answers it only once the leader sends again.
    pub fn install_copy_budget(&self) -> Duration {
        self.request_budget()
            .saturating_mul(Self::INSTALL_COPY_REQUESTS)
    }
}

impl Default for RaftSnapshotConfig {
    fn default() -> Self {
        Self {
            backend: RaftSnapshotBackend::Auto,
            s3_prefix: None,
            drive_interval: None,
            // 22 s per request with its retry and backoff, so a copy takes
            // at most 88 s of the default 120 s install timeout.
            s3_timeout: HumanDuration::sec(10),
            s3_max_retries: 1,
        }
    }
}

/// The prefix of a node's snapshot objects (`storage.snapshot.s3_prefix`),
/// parsed once, where the config is read, whatever the snapshot backend.
/// Every leading and trailing `/` of the configured text is trimmed. What
/// is left must be one or more components separated by single `/`s, each
/// of ASCII letters, digits, `-`, `_` and `.` and none of them `.` or `..`,
/// so no object store rewrites a key under it. Any other character,
/// whitespace included, is refused before the trimming.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct SnapshotPrefix(String);

/// The prefix a node uses when `storage.snapshot.s3_prefix` is unset.
pub const DEFAULT_SNAPSHOT_PREFIX: &str = "snapshots";

/// Why a configured snapshot prefix is refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SnapshotPrefixProblem {
    /// Nothing is left once the leading and trailing `/` are trimmed.
    Empty,
    /// A component is empty, as in `a//b`.
    EmptyComponent,
    /// A component is `.` or `..`.
    DotComponent,
    /// The prefix holds a character outside the allowed set.
    Character { found: char },
}

/// A refused `storage.snapshot.s3_prefix`.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("storage.snapshot.s3_prefix {prefix:?} is not a snapshot prefix: {problem:?}")]
pub struct SnapshotPrefixError {
    pub prefix: String,
    pub problem: SnapshotPrefixProblem,
}

impl SnapshotPrefix {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Default for SnapshotPrefix {
    fn default() -> Self {
        Self(DEFAULT_SNAPSHOT_PREFIX.to_owned())
    }
}

impl std::fmt::Display for SnapshotPrefix {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl TryFrom<&str> for SnapshotPrefix {
    type Error = SnapshotPrefixError;

    fn try_from(prefix: &str) -> Result<Self, Self::Error> {
        let refuse = |problem| SnapshotPrefixError {
            prefix: prefix.to_owned(),
            problem,
        };
        if let Some(found) = prefix
            .chars()
            .find(|c| !(c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.' | '/')))
        {
            return Err(refuse(SnapshotPrefixProblem::Character { found }));
        }
        let trimmed = prefix.trim_matches('/');
        if trimmed.is_empty() {
            return Err(refuse(SnapshotPrefixProblem::Empty));
        }
        for component in trimmed.split('/') {
            match component {
                "" => return Err(refuse(SnapshotPrefixProblem::EmptyComponent)),
                "." | ".." => return Err(refuse(SnapshotPrefixProblem::DotComponent)),
                _ => {}
            }
        }
        Ok(Self(trimmed.to_owned()))
    }
}

impl TryFrom<String> for SnapshotPrefix {
    type Error = SnapshotPrefixError;

    fn try_from(prefix: String) -> Result<Self, Self::Error> {
        Self::try_from(prefix.as_str())
    }
}

impl From<SnapshotPrefix> for String {
    fn from(prefix: SnapshotPrefix) -> Self {
        prefix.0
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
    /// How long the leadership balancer and the leadership health gates wait
    /// for a peer's leadership-shed state. A peer that does not answer in
    /// time is treated as unable to campaign, except that the cluster-egress
    /// gate may give it a leadership's first handoff in a shed, on a tick
    /// when no peer answered that it may not campaign, and leaves that
    /// handoff to the Raft handoff check. Must be non-zero.
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
            // Leave the normal 8 MiB cold-flush threshold enough room to run.
            // The old 7 MiB watermark forced leadership shedding before a
            // group became eligible for its first flush.
            hot_size_high: HumanSize::mib(48),
            hot_size_low: HumanSize::mib(32),
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

#[cfg(test)]
mod s3_budget_tests {
    use std::time::Duration;

    use super::s3_operation_budget;

    #[test]
    fn s3_operation_budget_covers_every_attempt_and_jittered_backoff() {
        let secs = Duration::from_secs;
        assert_eq!(s3_operation_budget(secs(10), 0), secs(10));
        // The cold defaults: four attempts of 10 s, backoffs of at most 2, 4
        // and 8 s.
        assert_eq!(s3_operation_budget(secs(10), 3), secs(54));
        // The snapshot defaults: two attempts of 10 s and one backoff of at
        // most 2 s.
        assert_eq!(s3_operation_budget(secs(10), 1), secs(22));
        // Nine attempts of 1 s, backoffs that double from 1 s to the 60 s
        // cap (2+4+8+16+32+64 s), then 120 s for each of the two retries
        // past the cap: 9 + 126 + 240 s.
        assert_eq!(s3_operation_budget(secs(1), 8), secs(375));
        assert_eq!(
            s3_operation_budget(Duration::MAX, 3),
            Duration::MAX,
            "saturates instead of overflowing"
        );
    }
}

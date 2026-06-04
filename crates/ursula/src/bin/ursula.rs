use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use axum::Router;
use clap::Parser;
use serde::Deserialize;

use ursula::{
    HttpState, StaticGrpcRaftMembershipConfig, client_router_from_state, cluster_router_from_state,
    spawn_cold_flush_worker_if_configured, spawn_cold_gc_worker_if_configured,
    spawn_default_runtime, spawn_raft_memory_runtime, spawn_raft_runtime,
    spawn_static_grpc_raft_memory_runtime_with_membership_config,
    spawn_static_grpc_raft_runtime_with_membership_config, spawn_wal_runtime,
};
use ursula_shard::RaftGroupId;

// glibc malloc held ~1 GB of cached arena chunks under the chaos workload
// (16+ per-thread arenas of 64 MB each, freed memory never returned to the OS).
// mimalloc trims aggressively and uses one segment-per-thread instead, which
// keeps steady-state RSS proportional to live working set.
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let tokio_console = init_tokio_console_if_enabled();
    if !tokio_console.installed() {
        init_tracing();
    }
    tokio_console.warn_if_needed();

    let raw = RawArgs::parse();
    let args = Args::try_from(raw)?;
    apply_admission_env_overrides(&args);

    let state = if args.static_grpc_raft_configured() {
        init_static_grpc_state(&args).await?
    } else {
        init_local_runtime_state(&args)?
    };

    serve(state, &args).await
}

async fn init_static_grpc_state(args: &Args) -> Result<HttpState, Box<dyn std::error::Error>> {
    let node_id = args
        .raft_node_id
        .expect("static gRPC Raft validation required node id");
    let raft_peers = args.raft_peers.clone();
    let raft_group_voters = args.raft_group_voters.clone();
    let membership_config = StaticGrpcRaftMembershipConfig {
        initialize_membership_per_group: args.raft_init_membership_per_group,
        per_group_voters: raft_group_voters,
    };
    let (runtime, registry) = if let Some(raft_log_dir) = args.raft_log_dir.clone() {
        spawn_static_grpc_raft_runtime_with_membership_config(
            args.core_count,
            args.raft_group_count,
            node_id,
            raft_peers,
            args.raft_init_membership,
            membership_config,
            raft_log_dir,
        )?
    } else {
        spawn_static_grpc_raft_memory_runtime_with_membership_config(
            args.core_count,
            args.raft_group_count,
            node_id,
            raft_peers,
            args.raft_init_membership,
            membership_config,
        )?
    };
    warm_static_grpc_groups(
        &runtime,
        args.raft_group_count,
        node_id,
        &args.raft_group_voters,
    )
    .await?;
    spawn_cold_flush_worker_if_configured(&runtime);
    spawn_cold_gc_worker_if_configured(&runtime);
    Ok(HttpState::with_static_raft_cluster_topology(
        runtime,
        registry,
        node_id,
        args.raft_peers.clone(),
        args.raft_group_voters.clone(),
    ))
}

fn init_local_runtime_state(args: &Args) -> Result<HttpState, Box<dyn std::error::Error>> {
    let runtime = match (
        args.wal_dir.clone(),
        args.raft_log_dir.clone(),
        args.raft_memory,
    ) {
        (Some(wal_dir), None, false) => {
            spawn_wal_runtime(args.core_count, args.raft_group_count, wal_dir)?
        }
        (None, Some(raft_log_dir), false) => {
            spawn_raft_runtime(args.core_count, args.raft_group_count, raft_log_dir)?
        }
        (None, None, true) => spawn_raft_memory_runtime(args.core_count, args.raft_group_count)?,
        (None, None, false) => spawn_default_runtime(args.core_count, args.raft_group_count)?,
        _ => unreachable!("storage mode exclusivity is checked above"),
    };
    Ok(HttpState::new(runtime))
}

/// Start the HTTP server(s).
///
/// When `args.cluster_listen` is set we bind two separate sockets so that
/// client traffic and Raft / inter-node gRPC traffic are isolated.
async fn serve(state: HttpState, args: &Args) -> Result<(), Box<dyn std::error::Error>> {
    if let Some(cluster_addr) = args.cluster_listen {
        let client_app = client_router_from_state(state.clone());
        let cluster_app = cluster_router_from_state(state);
        let client_listener = tokio::net::TcpListener::bind(args.listen).await?;
        let cluster_listener = tokio::net::TcpListener::bind(cluster_addr).await?;
        let client_task =
            tokio::spawn(async move { axum::serve(client_listener, client_app).await });
        let cluster_task =
            tokio::spawn(async move { axum::serve(cluster_listener, cluster_app).await });
        tokio::select! {
            res = client_task => res??,
            res = cluster_task => res??,
        }
    } else {
        let app: Router = ursula::router_with_http_state(state);
        let listener = tokio::net::TcpListener::bind(args.listen).await?;
        axum::serve(listener, app).await?;
    }
    Ok(())
}

/// Bridges CLI admission flags to env vars so the runtime/HTTP layers see one
/// configuration source. Explicit CLI values take precedence over an existing
/// env var only if the CLI value was supplied (the parser only sets `Some`
/// when the flag was passed).
fn apply_admission_env_overrides(args: &Args) {
    if let Some(value) = args.raft_max_uncommitted_bytes_per_group {
        // SAFETY: env mutation happens before any threads spawn that read
        // these vars; the runtime is spawned later in `main`.
        unsafe {
            std::env::set_var(
                "URSULA_RAFT_MAX_UNCOMMITTED_BYTES_PER_GROUP",
                value.to_string(),
            );
        }
    }
    if let Some(value) = args.http_inflight_body_bytes {
        unsafe {
            std::env::set_var("URSULA_HTTP_INFLIGHT_BODY_BYTES", value.to_string());
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum TokioConsoleInit {
    Disabled,
    #[cfg(all(feature = "tokio-console", tokio_unstable))]
    Installed,
    #[cfg(not(feature = "tokio-console"))]
    MissingFeature,
    #[cfg(all(feature = "tokio-console", not(tokio_unstable)))]
    MissingTokioUnstable,
}

impl TokioConsoleInit {
    fn installed(self) -> bool {
        #[cfg(all(feature = "tokio-console", tokio_unstable))]
        {
            matches!(self, Self::Installed)
        }
        #[cfg(not(all(feature = "tokio-console", tokio_unstable)))]
        {
            let _ = self;
            false
        }
    }

    fn warn_if_needed(self) {
        match self {
            Self::Disabled => {}
            #[cfg(all(feature = "tokio-console", tokio_unstable))]
            Self::Installed => {}
            #[cfg(all(feature = "tokio-console", not(tokio_unstable)))]
            Self::MissingTokioUnstable => tracing::warn!(
                "URSULA_TOKIO_CONSOLE is set, but tokio-console requires building with RUSTFLAGS=\"--cfg tokio_unstable\""
            ),
            #[cfg(not(feature = "tokio-console"))]
            Self::MissingFeature => tracing::warn!(
                "URSULA_TOKIO_CONSOLE is set, but ursula was built without tokio-console feature"
            ),
        }
    }
}

fn init_tokio_console_if_enabled() -> TokioConsoleInit {
    if std::env::var_os("URSULA_TOKIO_CONSOLE").is_none() {
        return TokioConsoleInit::Disabled;
    }

    #[cfg(all(feature = "tokio-console", tokio_unstable))]
    {
        console_subscriber::ConsoleLayer::builder()
            .with_default_env()
            .init();
        TokioConsoleInit::Installed
    }

    #[cfg(all(feature = "tokio-console", not(tokio_unstable)))]
    {
        TokioConsoleInit::MissingTokioUnstable
    }

    #[cfg(not(feature = "tokio-console"))]
    {
        TokioConsoleInit::MissingFeature
    }
}

/// Install a stderr tracing subscriber filtered by `RUST_LOG` (default `info`).
/// `try_init` is a no-op if a global subscriber was already installed.
fn init_tracing() {
    use tracing_subscriber::EnvFilter;

    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .with_target(true)
        .try_init();
}

// CLI argument schema for clap derive.
//
// Several invariants (duplicate-peer detection, cross-field defaults, config
// file merge) cannot be expressed declaratively with `#[derive(Parser)]`.
// Post-processing happens in [`TryFrom<RawArgs> for Args`].
#[derive(Parser, Debug)]
#[command(
    version,
    about = "Ursula durable-stream server",
    group = clap::ArgGroup::new("storage").multiple(false).required(false),
)]
struct RawArgs {
    /// Public HTTP client API bind address.
    #[arg(long, default_value = "127.0.0.1:4437")]
    listen: SocketAddr,

    /// Optional separate bind for the cluster / Raft gRPC plane.
    /// When omitted, both planes share `--listen`.
    #[arg(long)]
    cluster_listen: Option<SocketAddr>,

    /// Number of CPU cores / tokio worker threads to use.
    #[arg(
        long,
        default_value_t = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(4)
    )]
    core_count: usize,

    /// Number of Raft groups (shards). Defaults to `core_count * 16`.
    #[arg(long)]
    raft_group_count: Option<usize>,

    /// Persist write-ahead log to the given directory.
    #[arg(long, group = "storage")]
    wal_dir: Option<PathBuf>,

    /// Persist Raft log entries to the given directory.
    #[arg(long, group = "storage")]
    raft_log_dir: Option<PathBuf>,

    /// Run Raft entirely in-memory (no disk persistence).
    #[arg(long, group = "storage")]
    raft_memory: bool,

    /// Static gRPC Raft cluster JSON config file path.
    #[arg(long)]
    raft_cluster_config: Option<PathBuf>,

    /// Unique node ID within the static gRPC Raft cluster.
    #[arg(long)]
    raft_node_id: Option<u64>,

    /// Static gRPC Raft peer, repeated for each member. Format: `NODE_ID=URL`.
    #[arg(long, value_parser = parse_raft_peer)]
    raft_peer: Vec<(u64, String)>,

    /// Bootstrap the initial Raft membership once on startup.
    #[arg(long)]
    raft_init_membership: bool,

    /// Bootstrap per-group Raft membership on startup.
    #[arg(long)]
    raft_init_membership_per_group: bool,

    /// Per-group cap on raft-submitted-but-not-yet-applied payload bytes; `None`
    /// disables. Catches raft replication lag before in-memory queues grow
    /// unbounded.
    #[arg(long)]
    raft_max_uncommitted_bytes_per_group: Option<u64>,

    /// Process-wide cap on accepted write body bytes held by the HTTP layer.
    #[arg(long)]
    http_inflight_body_bytes: Option<u64>,
}

impl TryFrom<RawArgs> for Args {
    type Error = String;

    /// Convert parsed clap flags into the resolved [`Args`] used by `main` and
    /// tests.
    ///
    /// This is where logic that cannot be expressed declaratively in
    /// `#[derive(Parser)]` lives: duplicate-peer detection, runtime defaults
    /// that reference other fields (`raft_group_count` from `core_count`),
    /// and config-file loading / merging.  Cross-field validation is delegated
    /// to [`Args::validate`] after construction is complete.
    fn try_from(raw: RawArgs) -> Result<Self, Self::Error> {
        let mut raft_peers = BTreeMap::new();
        for (node_id, url) in raw.raft_peer {
            if raft_peers.insert(node_id, url).is_some() {
                return Err(format!("duplicate --raft-peer for node id {node_id}"));
            }
        }

        let raft_init_membership = raw.raft_init_membership || raw.raft_init_membership_per_group;

        let mut args = Args {
            listen: raw.listen,
            cluster_listen: raw.cluster_listen,
            core_count: raw.core_count,
            raft_group_count: raw
                .raft_group_count
                .unwrap_or_else(|| raw.core_count.saturating_mul(16).max(1)),
            wal_dir: raw.wal_dir,
            raft_log_dir: raw.raft_log_dir,
            raft_memory: raw.raft_memory,
            raft_cluster_config: raw.raft_cluster_config.clone(),
            raft_node_id: raw.raft_node_id,
            raft_peers,
            raft_group_voters: BTreeMap::new(),
            raft_init_membership,
            raft_init_membership_per_group: raw.raft_init_membership_per_group,
            raft_max_uncommitted_bytes_per_group: raw.raft_max_uncommitted_bytes_per_group,
            http_inflight_body_bytes: raw.http_inflight_body_bytes,
        };

        if let Some(path) = raw.raft_cluster_config {
            let config = load_raft_cluster_config(&path)?;
            merge_raft_cluster_config(&path, config, &mut args)?;
        }

        args.validate()?;
        Ok(args)
    }
}

/// Fully resolved CLI arguments ready for consumption by `main` and tests.
///
/// Intentionally kept separate from [`RawArgs`] (the clap derive layer) so
/// the rest of the codebase can work with normal Rust types (`BTreeMap`,
/// `usize`, etc.) without leaking CLI-parsing details.  See [`RawArgs`] for
/// the rationale behind the two-layer design.
#[derive(Debug)]
struct Args {
    listen: SocketAddr,
    cluster_listen: Option<SocketAddr>,
    core_count: usize,
    raft_group_count: usize,
    wal_dir: Option<PathBuf>,
    raft_log_dir: Option<PathBuf>,
    raft_memory: bool,
    raft_cluster_config: Option<PathBuf>,
    raft_node_id: Option<u64>,
    raft_peers: BTreeMap<u64, String>,
    raft_group_voters: BTreeMap<RaftGroupId, BTreeSet<u64>>,
    raft_init_membership: bool,
    raft_init_membership_per_group: bool,
    raft_max_uncommitted_bytes_per_group: Option<u64>,
    http_inflight_body_bytes: Option<u64>,
}

impl Args {
    /// Test helper that delegates to clap and then runs post-processing via
    /// [`TryFrom<RawArgs>`].
    ///
    /// Existing tests call `Args::parse_from(["--flag", ...])` (without a
    /// leading binary name). We prepend `"ursula"` before handing the
    /// iterator to clap so those tests require zero changes.
    #[cfg(test)]
    fn parse_from<I>(args: I) -> Result<Self, String>
    where
        I: IntoIterator,
        I::Item: Into<std::ffi::OsString>,
    {
        let args = std::iter::once(std::ffi::OsString::from("ursula"))
            .chain(args.into_iter().map(Into::into));
        let raw = RawArgs::try_parse_from(args).map_err(|e| e.to_string())?;
        Args::try_from(raw)
    }

    fn static_grpc_raft_configured(&self) -> bool {
        self.raft_cluster_config.is_some()
            || self.raft_node_id.is_some()
            || !self.raft_peers.is_empty()
            || !self.raft_group_voters.is_empty()
            || self.raft_init_membership
            || self.raft_init_membership_per_group
    }

    /// Validate cross-field invariants that can only be checked after the
    /// struct is fully built and optional configuration-file merge is done.
    ///
    /// Errors returned here correspond to the `Err(...)` branches that used to
    /// live directly in `TryFrom::try_from` and in `main`. Keeping them in one
    /// place makes the validation surface easy to spot and extend.
    fn validate(&self) -> Result<(), String> {
        if let Some(cluster) = self.cluster_listen
            && cluster == self.listen
        {
            return Err("--cluster-listen and --listen must use distinct addresses".to_owned());
        }

        if !self.static_grpc_raft_configured() {
            return Ok(());
        }
        if self.wal_dir.is_some() {
            return Err("static gRPC Raft does not support --wal-dir".into());
        }
        if !self.raft_memory && self.raft_log_dir.is_none() {
            return Err("static gRPC Raft requires --raft-memory or --raft-log-dir".into());
        }
        if self.raft_peers.is_empty() {
            return Err("static gRPC Raft requires at least one --raft-peer NODE_ID=URL".into());
        }
        let Some(node_id) = self.raft_node_id else {
            return Err("static gRPC Raft requires --raft-node-id".into());
        };
        if !self.raft_peers.contains_key(&node_id) {
            return Err(format!("--raft-peer must include this node id {node_id}"));
        }
        self.validate_raft_group_voters()?;
        Ok(())
    }

    fn validate_raft_group_voters(&self) -> Result<(), String> {
        if self.raft_group_voters.is_empty() {
            return Ok(());
        }

        let raft_group_count = u32::try_from(self.raft_group_count).map_err(|_| {
            format!(
                "--raft-group-count {} exceeds u32::MAX",
                self.raft_group_count
            )
        })?;

        for (raft_group_id, voters) in &self.raft_group_voters {
            if raft_group_id.0 >= raft_group_count {
                return Err(format!(
                    "raft group {} is outside configured --raft-group-count {}",
                    raft_group_id.0, self.raft_group_count
                ));
            }
            if voters.is_empty() {
                return Err(format!("raft group {} has no voters", raft_group_id.0));
            }
            for voter in voters {
                if !self.raft_peers.contains_key(voter) {
                    return Err(format!(
                        "raft group {} voter {} is not present in static peer config",
                        raft_group_id.0, voter
                    ));
                }
            }
        }

        for raw_group_id in 0..raft_group_count {
            if !self
                .raft_group_voters
                .contains_key(&RaftGroupId(raw_group_id))
            {
                return Err(format!(
                    "partial raft_group_voters config is not supported; missing raft group {} of {}",
                    raw_group_id, self.raft_group_count
                ));
            }
        }

        Ok(())
    }
}

async fn warm_static_grpc_groups(
    runtime: &ursula_runtime::ShardRuntime,
    raft_group_count: usize,
    node_id: u64,
    raft_group_voters: &BTreeMap<RaftGroupId, BTreeSet<u64>>,
) -> Result<(), ursula_runtime::RuntimeError> {
    if raft_group_voters.is_empty() {
        return runtime.warm_all_groups().await;
    }

    for raw_group_id in 0..raft_group_count {
        let raft_group_id =
            u32::try_from(raw_group_id).expect("runtime config validates raft group ids fit u32");
        if static_grpc_node_hosts_group(node_id, raft_group_id, raft_group_voters) {
            runtime.warm_group(RaftGroupId(raft_group_id)).await?;
        }
    }
    Ok(())
}

fn static_grpc_node_hosts_group(
    node_id: u64,
    raft_group_id: u32,
    raft_group_voters: &BTreeMap<RaftGroupId, BTreeSet<u64>>,
) -> bool {
    if raft_group_voters.is_empty() {
        return true;
    }
    raft_group_voters
        .get(&RaftGroupId(raft_group_id))
        .is_some_and(|voters| voters.contains(&node_id))
}

#[derive(Debug, Deserialize)]
struct RaftClusterConfigFile {
    node_id: Option<u64>,
    #[serde(default)]
    peers: Vec<RaftPeerConfigFile>,
    #[serde(default)]
    groups: Vec<RaftGroupConfigFile>,
    #[serde(default)]
    init_membership: bool,
    #[serde(default)]
    init_membership_per_group: bool,
}

#[derive(Debug, Deserialize)]
struct RaftPeerConfigFile {
    node_id: u64,
    url: String,
}

#[derive(Debug, Deserialize)]
struct RaftGroupConfigFile {
    raft_group_id: u32,
    voters: Vec<u64>,
}

fn load_raft_cluster_config(path: &Path) -> Result<RaftClusterConfigFile, String> {
    let bytes = fs::read(path)
        .map_err(|err| format!("read --raft-cluster-config '{}': {err}", path.display()))?;
    serde_json::from_slice(&bytes)
        .map_err(|err| format!("parse --raft-cluster-config '{}': {err}", path.display()))
}

fn merge_raft_cluster_config(
    path: &Path,
    config: RaftClusterConfigFile,
    args: &mut Args,
) -> Result<(), String> {
    if let Some(config_node_id) = config.node_id {
        match args.raft_node_id {
            Some(existing) if existing != config_node_id => {
                return Err(format!(
                    "--raft-cluster-config '{}' node_id {} conflicts with --raft-node-id {}",
                    path.display(),
                    config_node_id,
                    existing
                ));
            }
            Some(_) => {}
            None => args.raft_node_id = Some(config_node_id),
        }
    }

    for peer in config.peers {
        let (node_id, url) = parse_raft_peer(&format!("{}={}", peer.node_id, peer.url))?;
        if args.raft_peers.insert(node_id, url).is_some() {
            return Err(format!(
                "--raft-cluster-config '{}' duplicates raft peer node id {}",
                path.display(),
                node_id
            ));
        }
    }

    for group in config.groups {
        if group.voters.is_empty() {
            return Err(format!(
                "--raft-cluster-config '{}' raft group {} has no voters",
                path.display(),
                group.raft_group_id
            ));
        }
        let mut voters = BTreeSet::new();
        for voter in group.voters {
            if !voters.insert(voter) {
                return Err(format!(
                    "--raft-cluster-config '{}' raft group {} duplicates voter node id {}",
                    path.display(),
                    group.raft_group_id,
                    voter
                ));
            }
        }
        if args
            .raft_group_voters
            .insert(RaftGroupId(group.raft_group_id), voters)
            .is_some()
        {
            return Err(format!(
                "--raft-cluster-config '{}' duplicates raft group {}",
                path.display(),
                group.raft_group_id
            ));
        }
    }

    args.raft_init_membership |= config.init_membership;
    if config.init_membership_per_group {
        args.raft_init_membership = true;
        args.raft_init_membership_per_group = true;
    }
    Ok(())
}

/// Custom value parser used by clap for `--raft-peer NODE_ID=URL`.
///
/// Validates that the URL has an `http://` or `https://` scheme and strips
/// a trailing slash so peer comparisons are normalised.
fn parse_raft_peer(raw: &str) -> Result<(u64, String), String> {
    let (raw_node_id, raw_url) = raw
        .split_once('=')
        .ok_or_else(|| format!("invalid --raft-peer '{raw}': expected NODE_ID=URL"))?;
    let node_id = raw_node_id
        .parse::<u64>()
        .map_err(|err| format!("invalid --raft-peer node id '{raw_node_id}': {err}"))?;
    let url = raw_url.trim();
    if url.is_empty() {
        return Err(format!(
            "invalid --raft-peer '{raw}': URL must not be empty"
        ));
    }
    if !(url.starts_with("http://") || url.starts_with("https://")) {
        return Err(format!(
            "invalid --raft-peer '{raw}': URL must start with http:// or https://"
        ));
    }
    Ok((node_id, url.trim_end_matches('/').to_owned()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_config_path(prefix: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "ursula-{prefix}-{}-{}.json",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system time after unix epoch")
                .as_nanos()
        ))
    }

    #[test]
    fn parses_static_grpc_raft_cluster_args() {
        let args = Args::parse_from([
            "--listen",
            "127.0.0.1:4437",
            "--core-count",
            "4",
            "--raft-group-count",
            "16",
            "--raft-memory",
            "--raft-node-id",
            "2",
            "--raft-peer",
            "1=http://10.0.0.1:4437",
            "--raft-peer",
            "2=http://10.0.0.2:4437/",
            "--raft-init-membership",
            "--http-inflight-body-bytes",
            "67108864",
        ])
        .expect("static gRPC Raft args should parse");

        assert!(args.static_grpc_raft_configured());
        assert_eq!(args.listen, "127.0.0.1:4437".parse().unwrap());
        assert_eq!(args.core_count, 4);
        assert_eq!(args.raft_group_count, 16);
        assert!(args.raft_memory);
        assert_eq!(args.raft_node_id, Some(2));
        assert_eq!(
            args.raft_peers.get(&1).map(String::as_str),
            Some("http://10.0.0.1:4437")
        );
        assert_eq!(
            args.raft_peers.get(&2).map(String::as_str),
            Some("http://10.0.0.2:4437")
        );
        assert!(args.raft_init_membership);
        assert!(!args.raft_init_membership_per_group);
        assert_eq!(args.http_inflight_body_bytes, Some(67_108_864));
    }

    #[test]
    fn parses_static_grpc_per_group_membership_initializers() {
        let args = Args::parse_from([
            "--raft-memory",
            "--raft-node-id",
            "2",
            "--raft-peer",
            "1=http://10.0.0.1:4437",
            "--raft-peer",
            "2=http://10.0.0.2:4437",
            "--raft-init-membership-per-group",
        ])
        .expect("static gRPC Raft args should parse");

        assert!(args.static_grpc_raft_configured());
        assert!(args.raft_init_membership);
        assert!(args.raft_init_membership_per_group);
    }

    #[test]
    fn parses_static_grpc_raft_cluster_config_file() {
        let path = temp_config_path("raft-cluster-config");
        std::fs::write(
            &path,
            r#"{
                "node_id": 2,
                "init_membership": true,
                "peers": [
                    {"node_id": 1, "url": "http://10.0.0.1:4437"},
                    {"node_id": 2, "url": "http://10.0.0.2:4437/"}
                ]
            }"#,
        )
        .expect("write cluster config");

        let args = Args::parse_from([
            "--raft-memory",
            "--raft-group-count",
            "2",
            "--raft-cluster-config",
            path.to_str().expect("utf8 path"),
        ])
        .expect("static gRPC Raft config should parse");

        assert!(args.static_grpc_raft_configured());
        assert_eq!(args.raft_cluster_config.as_deref(), Some(path.as_path()));
        assert_eq!(args.raft_node_id, Some(2));
        assert_eq!(
            args.raft_peers.get(&1).map(String::as_str),
            Some("http://10.0.0.1:4437")
        );
        assert_eq!(
            args.raft_peers.get(&2).map(String::as_str),
            Some("http://10.0.0.2:4437")
        );
        assert!(args.raft_init_membership);
        assert!(!args.raft_init_membership_per_group);

        std::fs::remove_file(path).expect("remove cluster config");
    }

    #[test]
    fn parses_static_grpc_per_group_membership_initializers_from_config_file() {
        let path = temp_config_path("raft-cluster-config-per-group");
        std::fs::write(
            &path,
            r#"{
                "node_id": 2,
                "init_membership_per_group": true,
                "peers": [
                    {"node_id": 1, "url": "http://10.0.0.1:4437"},
                    {"node_id": 2, "url": "http://10.0.0.2:4437/"}
                ]
            }"#,
        )
        .expect("write cluster config");

        let args = Args::parse_from([
            "--raft-memory",
            "--raft-group-count",
            "2",
            "--raft-cluster-config",
            path.to_str().expect("utf8 path"),
        ])
        .expect("static gRPC Raft config should parse");

        assert!(args.raft_init_membership);
        assert!(args.raft_init_membership_per_group);

        std::fs::remove_file(path).expect("remove cluster config");
    }

    #[test]
    fn parses_static_grpc_per_group_voters_from_config_file() {
        let path = temp_config_path("raft-cluster-config-group-voters");
        std::fs::write(
            &path,
            r#"{
                "node_id": 2,
                "init_membership_per_group": true,
                "peers": [
                    {"node_id": 1, "url": "http://10.0.0.1:4437"},
                    {"node_id": 2, "url": "http://10.0.0.2:4437"},
                    {"node_id": 3, "url": "http://10.0.0.3:4437"},
                    {"node_id": 4, "url": "http://10.0.0.4:4437"}
                ],
                "groups": [
                    {"raft_group_id": 0, "voters": [1, 2, 3]},
                    {"raft_group_id": 1, "voters": [2, 3, 4]}
                ]
            }"#,
        )
        .expect("write cluster config");

        let args = Args::parse_from([
            "--raft-memory",
            "--raft-group-count",
            "2",
            "--raft-cluster-config",
            path.to_str().expect("utf8 path"),
        ])
        .expect("static gRPC Raft config should parse");

        assert_eq!(
            args.raft_group_voters.get(&RaftGroupId(0)),
            Some(&BTreeSet::from([1, 2, 3]))
        );
        assert_eq!(
            args.raft_group_voters.get(&RaftGroupId(1)),
            Some(&BTreeSet::from([2, 3, 4]))
        );

        fs::remove_file(path).expect("remove cluster config");
    }

    #[test]
    fn rejects_partial_static_grpc_per_group_voters_from_config_file() {
        let path = temp_config_path("raft-cluster-config-partial-group-voters");
        std::fs::write(
            &path,
            r#"{
                "node_id": 2,
                "peers": [
                    {"node_id": 1, "url": "http://10.0.0.1:4437"},
                    {"node_id": 2, "url": "http://10.0.0.2:4437"},
                    {"node_id": 3, "url": "http://10.0.0.3:4437"}
                ],
                "groups": [
                    {"raft_group_id": 0, "voters": [1, 2, 3]}
                ]
            }"#,
        )
        .expect("write cluster config");

        let err = Args::parse_from([
            "--raft-memory",
            "--raft-group-count",
            "2",
            "--raft-cluster-config",
            path.to_str().expect("utf8 path"),
        ])
        .expect_err("partial static per-group voter config should be rejected");

        assert!(err.contains("partial raft_group_voters config is not supported"));
        assert!(err.contains("missing raft group 1"));

        fs::remove_file(path).expect("remove cluster config");
    }

    #[test]
    fn configured_group_voters_limit_startup_warmup_to_member_nodes() {
        let group_voters = BTreeMap::from([
            (RaftGroupId(0), BTreeSet::from([1, 2, 3])),
            (RaftGroupId(1), BTreeSet::from([2, 3, 4])),
        ]);

        assert!(static_grpc_node_hosts_group(1, 0, &group_voters));
        assert!(!static_grpc_node_hosts_group(1, 1, &group_voters));
        assert!(!static_grpc_node_hosts_group(4, 0, &group_voters));
        assert!(static_grpc_node_hosts_group(4, 1, &group_voters));
        assert!(!static_grpc_node_hosts_group(4, 2, &group_voters));
    }

    #[test]
    fn parses_static_grpc_raft_cluster_with_durable_log_dir() {
        let args = Args::parse_from([
            "--raft-log-dir",
            "/tmp/ursula-raft-log",
            "--raft-node-id",
            "1",
            "--raft-peer",
            "1=http://127.0.0.1:4477",
            "--raft-init-membership",
        ])
        .expect("static durable gRPC Raft args should parse");

        assert!(args.static_grpc_raft_configured());
        assert_eq!(
            args.raft_log_dir.as_deref(),
            Some(Path::new("/tmp/ursula-raft-log"))
        );
        assert!(!args.raft_memory);
        assert_eq!(args.raft_node_id, Some(1));
        assert_eq!(
            args.raft_peers.get(&1).map(String::as_str),
            Some("http://127.0.0.1:4477")
        );
        assert!(args.raft_init_membership);
    }

    #[test]
    fn rejects_conflicting_raft_node_id_from_config_file() {
        let path = temp_config_path("raft-cluster-config-conflict");
        std::fs::write(&path, r#"{"node_id": 2, "peers": []}"#).expect("write cluster config");

        let err = Args::parse_from([
            "--raft-node-id",
            "1",
            "--raft-cluster-config",
            path.to_str().expect("utf8 path"),
        ])
        .expect_err("conflicting node id should be rejected");

        assert!(err.contains("conflicts with --raft-node-id 1"));
        std::fs::remove_file(path).expect("remove cluster config");
    }

    #[test]
    fn rejects_duplicate_raft_peer() {
        let err = Args::parse_from([
            "--raft-peer",
            "1=http://10.0.0.1:4437",
            "--raft-peer",
            "1=http://10.0.0.2:4437",
        ])
        .expect_err("duplicate raft peer should be rejected");

        assert!(err.contains("duplicate --raft-peer for node id 1"));
    }

    #[test]
    fn rejects_raft_peer_without_http_scheme() {
        let err = Args::parse_from(["--raft-peer", "1=10.0.0.1:4437"])
            .expect_err("raft peer URL without scheme should be rejected");

        assert!(err.contains("URL must start with http:// or https://"));
    }

    #[test]
    fn parses_separate_cluster_listen() {
        let args = Args::parse_from([
            "--listen",
            "0.0.0.0:4491",
            "--cluster-listen",
            "10.0.0.1:4495",
        ])
        .expect("dual listener args should parse");
        assert_eq!(args.listen.to_string(), "0.0.0.0:4491");
        assert_eq!(
            args.cluster_listen.map(|a| a.to_string()),
            Some("10.0.0.1:4495".to_owned())
        );
    }

    #[test]
    fn defaults_cluster_listen_to_none() {
        let args = Args::parse_from(["--listen", "0.0.0.0:4491"])
            .expect("single listener args should parse");
        assert!(args.cluster_listen.is_none());
    }

    #[test]
    fn rejects_cluster_listen_equal_to_listen() {
        let err = Args::parse_from([
            "--listen",
            "127.0.0.1:4437",
            "--cluster-listen",
            "127.0.0.1:4437",
        ])
        .expect_err("identical listen and cluster-listen must be rejected");
        assert!(err.contains("distinct addresses"), "got: {err}");
    }

    // --- static gRPC Raft conditional validation ---

    #[test]
    fn rejects_static_grpc_with_wal_dir() {
        let err = Args::parse_from([
            "--wal-dir",
            "/tmp/ursula-wal",
            "--raft-node-id",
            "1",
            "--raft-peer",
            "1=http://127.0.0.1:4437",
            "--raft-init-membership",
        ])
        .expect_err("static gRPC with --wal-dir should be rejected");

        assert!(
            err.contains("static gRPC Raft does not support --wal-dir"),
            "got: {err}"
        );
    }

    #[test]
    fn rejects_static_grpc_without_storage() {
        let err = Args::parse_from([
            "--raft-node-id",
            "1",
            "--raft-peer",
            "1=http://127.0.0.1:4437",
            "--raft-init-membership",
        ])
        .expect_err("static gRPC without --raft-memory or --raft-log-dir should be rejected");

        assert!(
            err.contains("static gRPC Raft requires --raft-memory or --raft-log-dir"),
            "got: {err}"
        );
    }

    #[test]
    fn rejects_static_grpc_without_peers() {
        let err = Args::parse_from([
            "--raft-memory",
            "--raft-node-id",
            "1",
            "--raft-init-membership",
        ])
        .expect_err("static gRPC without raft peers should be rejected");

        assert!(
            err.contains("static gRPC Raft requires at least one --raft-peer"),
            "got: {err}"
        );
    }

    #[test]
    fn rejects_static_grpc_without_node_id() {
        let err = Args::parse_from([
            "--raft-memory",
            "--raft-peer",
            "1=http://127.0.0.1:4437",
            "--raft-init-membership",
        ])
        .expect_err("static gRPC without raft-node-id should be rejected");

        assert!(
            err.contains("static gRPC Raft requires --raft-node-id"),
            "got: {err}"
        );
    }

    #[test]
    fn rejects_static_grpc_when_node_id_not_in_peers() {
        let err = Args::parse_from([
            "--raft-memory",
            "--raft-node-id",
            "2",
            "--raft-peer",
            "1=http://127.0.0.1:4437",
            "--raft-init-membership",
        ])
        .expect_err("static gRPC with node id absent from peers should be rejected");

        assert!(
            err.contains("--raft-peer must include this node id 2"),
            "got: {err}"
        );
    }
}

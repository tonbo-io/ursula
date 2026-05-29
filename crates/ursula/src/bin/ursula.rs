use std::collections::BTreeMap;
use std::fs;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use axum::Router;
use serde::Deserialize;
use ursula::{
    HttpState, client_router_from_state, cluster_router_from_state,
    spawn_cold_flush_worker_if_configured, spawn_cold_gc_worker_if_configured,
    spawn_default_runtime, spawn_raft_memory_runtime, spawn_raft_runtime,
    spawn_static_grpc_raft_memory_runtime,
    spawn_static_grpc_raft_memory_runtime_with_per_group_initializers,
    spawn_static_grpc_raft_runtime, spawn_static_grpc_raft_runtime_with_per_group_initializers,
    spawn_wal_runtime,
};

// glibc malloc held ~1 GB of cached arena chunks under the chaos workload
// (16+ per-thread arenas of 64 MB each, freed memory never returned to the OS).
// mimalloc trims aggressively and uses one segment-per-thread instead, which
// keeps steady-state RSS proportional to live working set.
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    init_tokio_console_if_enabled();
    init_tracing();

    let args = Args::parse()?;
    apply_admission_env_overrides(&args);
    let selected_storage_modes =
        usize::from(args.wal_dir.is_some()) + usize::from(args.raft_log_dir.is_some());
    if selected_storage_modes > 1 {
        return Err("use only one of --wal-dir or --raft-log-dir".into());
    }
    if args.raft_memory && selected_storage_modes > 0 {
        return Err("use --raft-memory without --wal-dir or --raft-log-dir".into());
    }
    let static_grpc_raft = args.static_grpc_raft_configured();
    if static_grpc_raft {
        if args.wal_dir.is_some() {
            return Err("static gRPC Raft does not support --wal-dir".into());
        }
        if !args.raft_memory && args.raft_log_dir.is_none() {
            return Err("static gRPC Raft requires --raft-memory or --raft-log-dir".into());
        }
        if args.raft_memory && args.raft_log_dir.is_some() {
            return Err("use --raft-memory without --raft-log-dir".into());
        }
        if args.raft_peers.is_empty() {
            return Err("static gRPC Raft requires at least one --raft-peer NODE_ID=URL".into());
        }
        let Some(node_id) = args.raft_node_id else {
            return Err("static gRPC Raft requires --raft-node-id".into());
        };
        if !args.raft_peers.contains_key(&node_id) {
            return Err(format!("--raft-peer must include this node id {node_id}").into());
        }
    }

    let state: HttpState = if static_grpc_raft {
        let node_id = args
            .raft_node_id
            .expect("static gRPC Raft validation required node id");
        let (runtime, registry) = if let Some(raft_log_dir) = args.raft_log_dir {
            if args.raft_init_membership_per_group {
                spawn_static_grpc_raft_runtime_with_per_group_initializers(
                    args.core_count,
                    args.raft_group_count,
                    node_id,
                    args.raft_peers.clone(),
                    args.raft_init_membership,
                    raft_log_dir,
                )?
            } else {
                spawn_static_grpc_raft_runtime(
                    args.core_count,
                    args.raft_group_count,
                    node_id,
                    args.raft_peers.clone(),
                    args.raft_init_membership,
                    raft_log_dir,
                )?
            }
        } else if args.raft_init_membership_per_group {
            spawn_static_grpc_raft_memory_runtime_with_per_group_initializers(
                args.core_count,
                args.raft_group_count,
                node_id,
                args.raft_peers.clone(),
                args.raft_init_membership,
            )?
        } else {
            spawn_static_grpc_raft_memory_runtime(
                args.core_count,
                args.raft_group_count,
                node_id,
                args.raft_peers.clone(),
                args.raft_init_membership,
            )?
        };
        runtime.warm_all_groups().await?;
        spawn_cold_flush_worker_if_configured(&runtime);
        spawn_cold_gc_worker_if_configured(&runtime);
        HttpState::with_static_raft_cluster(runtime, registry, args.raft_peers.clone())
    } else {
        let runtime = match (args.wal_dir, args.raft_log_dir, args.raft_memory) {
            (Some(wal_dir), None, false) => {
                spawn_wal_runtime(args.core_count, args.raft_group_count, wal_dir)?
            }
            (None, Some(raft_log_dir), false) => {
                spawn_raft_runtime(args.core_count, args.raft_group_count, raft_log_dir)?
            }
            (None, None, true) => {
                spawn_raft_memory_runtime(args.core_count, args.raft_group_count)?
            }
            (None, None, false) => spawn_default_runtime(args.core_count, args.raft_group_count)?,
            _ => unreachable!("storage mode exclusivity is checked above"),
        };
        HttpState::new(runtime)
    };

    if let Some(cluster_addr) = args.cluster_listen {
        // Dual-listener mode: client API on `--listen`, Raft / inter-node
        // gRPC on `--cluster-listen`. Failures applied to the public face
        // never disturb consensus traffic on the cluster face.
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
    if let Some(value) = args.node_memory_soft_cap_bytes {
        unsafe {
            std::env::set_var("URSULA_NODE_MEMORY_SOFT_CAP_BYTES", value.to_string());
        }
    }
}

fn init_tokio_console_if_enabled() {
    if std::env::var_os("URSULA_TOKIO_CONSOLE").is_none() {
        return;
    }

    #[cfg(feature = "tokio-console")]
    console_subscriber::ConsoleLayer::builder()
        .with_default_env()
        .init();

    #[cfg(not(feature = "tokio-console"))]
    eprintln!("URSULA_TOKIO_CONSOLE is set, but ursula was built without tokio-console feature");
}

/// Install a stderr tracing subscriber filtered by `RUST_LOG` (default `info`).
/// `try_init` is a no-op if a global subscriber was already installed (e.g. the
/// tokio-console layer), so this never conflicts with that path.
fn init_tracing() {
    use tracing_subscriber::EnvFilter;
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .with_target(true)
        .try_init();
}

#[derive(Debug)]
struct Args {
    listen: SocketAddr,
    /// Optional separate bind for the cluster plane (Raft / inter-node gRPC).
    /// In production this should point at a private VPC interface so chaos /
    /// load / abuse on the public client plane never disrupts consensus.
    /// When None, both planes share `listen`.
    cluster_listen: Option<SocketAddr>,
    core_count: usize,
    raft_group_count: usize,
    wal_dir: Option<PathBuf>,
    raft_log_dir: Option<PathBuf>,
    raft_memory: bool,
    raft_cluster_config: Option<PathBuf>,
    raft_node_id: Option<u64>,
    raft_peers: BTreeMap<u64, String>,
    raft_init_membership: bool,
    raft_init_membership_per_group: bool,
    /// Per-group cap on raft-submitted-but-not-yet-applied payload bytes; `None`
    /// disables. Catches raft replication lag before in-memory queues grow
    /// unbounded.
    raft_max_uncommitted_bytes_per_group: Option<u64>,
    /// Process-wide RSS soft cap. When exceeded, write endpoints return 503
    /// with `Retry-After: 1`. `None` disables. Linux-only.
    node_memory_soft_cap_bytes: Option<u64>,
}

impl Args {
    fn parse() -> Result<Self, String> {
        Self::parse_from(std::env::args().skip(1))
    }

    fn parse_from(args: impl IntoIterator<Item = impl Into<String>>) -> Result<Self, String> {
        let mut listen = "127.0.0.1:4437"
            .parse::<SocketAddr>()
            .expect("default listen addr is valid");
        let mut cluster_listen: Option<SocketAddr> = None;
        let mut core_count = std::thread::available_parallelism()
            .map(usize::from)
            .unwrap_or(4);
        let mut raft_group_count = core_count.saturating_mul(16).max(1);
        let mut wal_dir = None;
        let mut raft_log_dir = None;
        let mut raft_memory = false;
        let mut raft_cluster_config = None;
        let mut raft_node_id = None;
        let mut raft_peers = BTreeMap::new();
        let mut raft_init_membership = false;
        let mut raft_init_membership_per_group = false;
        let mut raft_max_uncommitted_bytes_per_group: Option<u64> = None;
        let mut node_memory_soft_cap_bytes: Option<u64> = None;

        let mut args = args.into_iter().map(Into::into);
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--listen" => {
                    let raw = args
                        .next()
                        .ok_or_else(|| "--listen requires an address".to_owned())?;
                    listen = raw
                        .parse()
                        .map_err(|err| format!("invalid --listen address '{raw}': {err}"))?;
                }
                "--cluster-listen" => {
                    let raw = args
                        .next()
                        .ok_or_else(|| "--cluster-listen requires an address".to_owned())?;
                    cluster_listen = Some(raw.parse().map_err(|err| {
                        format!("invalid --cluster-listen address '{raw}': {err}")
                    })?);
                }
                "--core-count" => {
                    let raw = args
                        .next()
                        .ok_or_else(|| "--core-count requires a value".to_owned())?;
                    core_count = raw
                        .parse()
                        .map_err(|err| format!("invalid --core-count '{raw}': {err}"))?;
                }
                "--raft-group-count" => {
                    let raw = args
                        .next()
                        .ok_or_else(|| "--raft-group-count requires a value".to_owned())?;
                    raft_group_count = raw
                        .parse()
                        .map_err(|err| format!("invalid --raft-group-count '{raw}': {err}"))?;
                }
                "--wal-dir" => {
                    let raw = args
                        .next()
                        .ok_or_else(|| "--wal-dir requires a directory".to_owned())?;
                    wal_dir = Some(PathBuf::from(raw));
                }
                "--raft-log-dir" => {
                    let raw = args
                        .next()
                        .ok_or_else(|| "--raft-log-dir requires a directory".to_owned())?;
                    raft_log_dir = Some(PathBuf::from(raw));
                }
                "--raft-memory" => {
                    raft_memory = true;
                }
                "--raft-cluster-config" => {
                    let raw = args
                        .next()
                        .ok_or_else(|| "--raft-cluster-config requires a JSON file".to_owned())?;
                    let path = PathBuf::from(raw);
                    let config = load_raft_cluster_config(&path)?;
                    merge_raft_cluster_config(
                        &path,
                        config,
                        &mut raft_node_id,
                        &mut raft_peers,
                        &mut raft_init_membership,
                        &mut raft_init_membership_per_group,
                    )?;
                    raft_cluster_config = Some(path);
                }
                "--raft-node-id" => {
                    let raw = args
                        .next()
                        .ok_or_else(|| "--raft-node-id requires a value".to_owned())?;
                    let node_id = raw
                        .parse()
                        .map_err(|err| format!("invalid --raft-node-id '{raw}': {err}"))?;
                    if let Some(existing) = raft_node_id
                        && existing != node_id
                    {
                        return Err(format!(
                            "--raft-node-id {node_id} conflicts with configured node id {existing}"
                        ));
                    }
                    raft_node_id = Some(node_id);
                }
                "--raft-peer" => {
                    let raw = args
                        .next()
                        .ok_or_else(|| "--raft-peer requires NODE_ID=URL".to_owned())?;
                    let (node_id, url) = parse_raft_peer(&raw)?;
                    if raft_peers.insert(node_id, url).is_some() {
                        return Err(format!("duplicate --raft-peer for node id {node_id}"));
                    }
                }
                "--raft-init-membership" => {
                    raft_init_membership = true;
                }
                "--raft-init-membership-per-group" => {
                    raft_init_membership = true;
                    raft_init_membership_per_group = true;
                }
                "--raft-max-uncommitted-bytes-per-group" => {
                    let raw = args.next().ok_or_else(|| {
                        "--raft-max-uncommitted-bytes-per-group requires a value".to_owned()
                    })?;
                    raft_max_uncommitted_bytes_per_group = Some(raw.parse().map_err(|err| {
                        format!("invalid --raft-max-uncommitted-bytes-per-group '{raw}': {err}")
                    })?);
                }
                "--node-memory-soft-cap-bytes" => {
                    let raw = args.next().ok_or_else(|| {
                        "--node-memory-soft-cap-bytes requires a value".to_owned()
                    })?;
                    node_memory_soft_cap_bytes = Some(raw.parse().map_err(|err| {
                        format!("invalid --node-memory-soft-cap-bytes '{raw}': {err}")
                    })?);
                }
                "--help" | "-h" => return Err(help()),
                other => return Err(format!("unknown argument '{other}'\n\n{}", help())),
            }
        }

        if let Some(cluster) = cluster_listen
            && cluster == listen
        {
            return Err("--cluster-listen and --listen must use distinct addresses".to_owned());
        }

        Ok(Self {
            listen,
            cluster_listen,
            core_count,
            raft_group_count,
            wal_dir,
            raft_log_dir,
            raft_memory,
            raft_cluster_config,
            raft_node_id,
            raft_peers,
            raft_init_membership,
            raft_init_membership_per_group,
            raft_max_uncommitted_bytes_per_group,
            node_memory_soft_cap_bytes,
        })
    }

    fn static_grpc_raft_configured(&self) -> bool {
        self.raft_cluster_config.is_some()
            || self.raft_node_id.is_some()
            || !self.raft_peers.is_empty()
            || self.raft_init_membership
            || self.raft_init_membership_per_group
    }
}

fn help() -> String {
    "usage: ursula [--listen ADDR] [--cluster-listen ADDR] [--core-count N] [--raft-group-count N] [--raft-memory | --wal-dir DIR | --raft-log-dir DIR] [--raft-cluster-config FILE | --raft-node-id ID --raft-peer ID=URL ... --raft-init-membership | --raft-init-membership-per-group] [--raft-max-uncommitted-bytes-per-group N] [--node-memory-soft-cap-bytes N]"
        .to_owned()
}

#[derive(Debug, Deserialize)]
struct RaftClusterConfigFile {
    node_id: Option<u64>,
    #[serde(default)]
    peers: Vec<RaftPeerConfigFile>,
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

fn load_raft_cluster_config(path: &Path) -> Result<RaftClusterConfigFile, String> {
    let bytes = fs::read(path)
        .map_err(|err| format!("read --raft-cluster-config '{}': {err}", path.display()))?;
    serde_json::from_slice(&bytes)
        .map_err(|err| format!("parse --raft-cluster-config '{}': {err}", path.display()))
}

fn merge_raft_cluster_config(
    path: &Path,
    config: RaftClusterConfigFile,
    raft_node_id: &mut Option<u64>,
    raft_peers: &mut BTreeMap<u64, String>,
    raft_init_membership: &mut bool,
    raft_init_membership_per_group: &mut bool,
) -> Result<(), String> {
    if let Some(config_node_id) = config.node_id {
        match *raft_node_id {
            Some(existing) if existing != config_node_id => {
                return Err(format!(
                    "--raft-cluster-config '{}' node_id {} conflicts with --raft-node-id {}",
                    path.display(),
                    config_node_id,
                    existing
                ));
            }
            Some(_) => {}
            None => *raft_node_id = Some(config_node_id),
        }
    }

    for peer in config.peers {
        let (node_id, url) = parse_raft_peer(&format!("{}={}", peer.node_id, peer.url))?;
        if raft_peers.insert(node_id, url).is_some() {
            return Err(format!(
                "--raft-cluster-config '{}' duplicates raft peer node id {}",
                path.display(),
                node_id
            ));
        }
    }

    *raft_init_membership |= config.init_membership;
    if config.init_membership_per_group {
        *raft_init_membership = true;
        *raft_init_membership_per_group = true;
    }
    Ok(())
}

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
        let path = std::env::temp_dir().join(format!(
            "ursula-raft-cluster-config-{}-{}.json",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system time after unix epoch")
                .as_nanos()
        ));
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
        let path = std::env::temp_dir().join(format!(
            "ursula-raft-cluster-config-per-group-{}-{}.json",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system time after unix epoch")
                .as_nanos()
        ));
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
            "--raft-cluster-config",
            path.to_str().expect("utf8 path"),
        ])
        .expect("static gRPC Raft config should parse");

        assert!(args.raft_init_membership);
        assert!(args.raft_init_membership_per_group);

        std::fs::remove_file(path).expect("remove cluster config");
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
        let path = std::env::temp_dir().join(format!(
            "ursula-raft-cluster-config-conflict-{}-{}.json",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system time after unix epoch")
                .as_nanos()
        ));
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
}

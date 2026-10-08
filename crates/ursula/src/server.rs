use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::ffi::OsStr;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use clap::Args;
use tokio::sync::watch;
use ursula_config::Preset;
use ursula_config::find_default_config;
use ursula_config::load_config;
use ursula_observability::serve::serve_until_shutdown;
use ursula_observability::serve::shutdown_signal;
use ursula_shard::CoreId;
use ursula_shard::RaftGroupId;

use crate::HttpState;
use crate::Persistence;
use crate::Topology;
use crate::bootstrap::spawn_runtime_with_maintenance_drain;
use crate::client_router_with_admission;
use crate::cluster_router_from_state;

#[derive(Args, Debug, Default)]
pub struct ServerArgs {
    /// Path to the TOML configuration file.
    #[arg(short, long)]
    config: Option<PathBuf>,

    /// Resource preset (default, tiny, small, standard, large).
    #[arg(long)]
    #[clap(value_enum)]
    preset: Option<Preset>,

    /// Raft node identity.  Must be unique per node in a cluster.
    #[arg(long)]
    node_id: Option<u64>,
}

pub async fn run(args: ServerArgs) -> Result<(), Box<dyn std::error::Error>> {
    let config_path = args.config.or_else(find_default_config);

    let mut preset = args.preset;

    // When no config file and no explicit preset are given, fall back to the
    // default single-node development preset: the in-memory engine without
    // Raft, node-id = 1.
    if config_path.is_none() && preset.is_none() {
        preset = Some(Preset::Default);
    }

    let config = load_config(config_path.as_deref(), preset, args.node_id)?;

    let tokio_console =
        config.observability.tokio_console || std::env::var_os("URSULA_TOKIO_CONSOLE").is_some();

    #[cfg(feature = "tokio-console")]
    let _telemetry: Option<ursula_observability::ObservabilityGuard> = if tokio_console {
        console_subscriber::init();
        None
    } else {
        Some(init_telemetry(&config))
    };

    #[cfg(not(feature = "tokio-console"))]
    let _telemetry: Option<ursula_observability::ObservabilityGuard> = {
        let _ = tokio_console;
        Some(init_telemetry(&config))
    };

    tracing::info!(
        "loaded config from {} (preset={})",
        config_path
            .as_deref()
            .map_or_else(|| "(none)".into(), |p| p.display().to_string()),
        preset.unwrap_or(Preset::Default)
    );

    let start_maintenance_drained = parse_start_maintenance_drained(
        std::env::var_os("URSULA_START_MAINTENANCE_DRAINED").as_deref(),
    )?;
    let boot = ursula_proto::admin::ProcessIncarnation::from_bits(rand::random());
    let wal_dir = if runs_raft(&config, preset) {
        Some(RaftWalDir::resolve(&config.raft.wal)?)
    } else {
        None
    };
    let persistence = match &wal_dir {
        Some(wal_dir) => Persistence::Raft {
            log_dir: wal_dir.log_dir(),
        },
        None => Persistence::InMemory,
    };
    // Meta startup persists its identity inside the journal directory. Refuse
    // incompatible storage before that write, and stamp a fresh directory
    // before its first identity record makes it nonempty.
    crate::bootstrap::check_and_stamp_format_epoch(&config, persistence.log_dir()).await?;
    let meta_authority = if config.raft.uses_meta_authority() {
        let root = config.raft.wal.path.as_ref().ok_or_else(|| {
            std::io::Error::other("meta authority requires a persistent WAL path")
        })?;
        Some(
            crate::bootstrap::meta::start_meta_authority(
                &config,
                root.join("meta-raft"),
                boot.clone(),
            )
            .await?,
        )
    } else {
        None
    };
    if let Some(authority) = &meta_authority {
        let topology = authority.handle.committed_state();
        let mut applied = topology.clone();
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if applied
                    .borrow()
                    .operations
                    .accepts_process(config.raft.node_id, &authority.process)
                {
                    break;
                }
                applied.changed().await.map_err(std::io::Error::other)?;
            }
            Ok::<(), std::io::Error>(())
        })
        .await??;
    }
    let mut state = init_state(
        &config,
        persistence,
        start_maintenance_drained,
        meta_authority.as_ref(),
    )
    .await?
    .with_process_incarnation(boot);
    let live_cleanup = if let Some(authority) = &meta_authority {
        let topology = authority.handle.committed_state();
        state = state
            .with_meta_control(authority.handle.clone())
            .with_live_topology(topology.clone());
        let registry = state
            .raft_registry()
            .ok_or_else(|| std::io::Error::other("meta authority requires data Raft registry"))?
            .clone();
        registry.set_replica_authority(config.raft.node_id, authority.replica.clone());
        registry.set_replica_genesis(authority.replica_genesis.clone());
        registry.set_replica_genesis_prefixes(authority.replica_genesis_prefixes.clone());
        registry.set_process_authority(
            config.raft.node_id,
            authority.process.clone(),
            authority.handle.clone(),
        );
        Some(crate::bootstrap::spawn_live_topology_cleanup(
            state.runtime.clone(),
            registry,
            config.raft.node_id,
            ursula_shard::StaticShardMap::new(config.runtime.core_count, config.raft.group_count)?,
            topology,
        )?)
    } else {
        None
    };
    state.register_otel_metrics();
    let cleanup_runtime = state.runtime.clone();
    let cleanup_wal = state.raft_wal().cloned();
    let served = serve(state, &config).await;
    cleanup_runtime.stop_owner_services().await;
    if served.is_err() {
        shutdown_raft_wal(&cleanup_runtime, cleanup_wal.as_ref()).await;
        cleanup_runtime.shutdown_owners().await;
    }
    if let Some(cleanup) = live_cleanup {
        let _cancelled = cleanup.await;
    }
    if let Some(authority) = meta_authority {
        authority.shutdown().await?;
    }
    let wal_shutdown = served?;
    if let Some(wal_dir) = wal_dir {
        wal_dir.close(wal_shutdown);
    }
    Ok(())
}

/// Whether the server runs Raft. Only the zero-config development mode (the
/// `default` preset on a single node) runs the in-memory engine without it.
fn runs_raft(config: &ursula_config::UrsulaConfig, preset: Option<Preset>) -> bool {
    preset != Some(Preset::Default)
        || !config.raft.peers.is_empty()
        || config.raft.wal.path.is_some()
}

/// Where this process keeps its Raft WAL.
#[derive(Debug)]
enum RaftWalDir {
    /// `raft.wal.path`, kept across runs.
    Configured(PathBuf),
    /// No path is configured, which only a single node allows: a fresh
    /// temporary directory for this run. Only [`RaftWalDir::close`] after a
    /// clean WAL shutdown removes it. Dropping it, as an error return does,
    /// leaves it, because a core writer may still write to it, and a journal
    /// write that fails stops the process.
    Temporary(PathBuf),
}

impl RaftWalDir {
    fn resolve(wal: &ursula_config::WalConfig) -> std::io::Result<Self> {
        match &wal.path {
            Some(path) => Ok(Self::Configured(path.clone())),
            None => {
                let dir = tempfile::Builder::new()
                    .prefix("ursula-wal-")
                    .tempdir()?
                    .keep();
                tracing::info!(
                    path = %dir.display(),
                    "raft.wal.path is not set: the Raft WAL runs in a temporary directory that \
                     is removed after a clean shutdown"
                );
                Ok(Self::Temporary(dir))
            }
        }
    }

    /// The journal directory: the WAL directory's `raft-log` subdirectory.
    fn log_dir(&self) -> PathBuf {
        let (Self::Configured(root) | Self::Temporary(root)) = self;
        root.join(ursula_config::WalConfig::LOG_SUBDIR)
    }

    /// After the server stopped: removes a temporary WAL directory once its
    /// WAL shut down cleanly. Only then has every core writer closed, so
    /// nothing writes to the directory any more.
    fn close(self, shutdown: WalShutdown) {
        let Self::Temporary(path) = self else {
            return;
        };
        if shutdown != WalShutdown::Clean {
            tracing::warn!(
                path = %path.display(),
                ?shutdown,
                "kept the temporary Raft WAL directory: the WAL did not shut down cleanly"
            );
            return;
        }
        match std::fs::remove_dir_all(&path) {
            Ok(()) => tracing::info!(
                path = %path.display(),
                "removed the temporary Raft WAL directory"
            ),
            Err(err) => tracing::warn!(
                path = %path.display(),
                %err,
                "could not remove the temporary Raft WAL directory"
            ),
        }
    }
}

fn init_telemetry(
    config: &ursula_config::UrsulaConfig,
) -> ursula_observability::ObservabilityGuard {
    let mut options = ursula_observability::InitOptions::new("ursula");
    options = options.with_resource("service.instance.id", config.raft.node_id.to_string());
    ursula_observability::init(options)
}

async fn init_state(
    config: &ursula_config::UrsulaConfig,
    persistence: Persistence,
    start_maintenance_drained: bool,
    meta_authority: Option<&crate::bootstrap::meta::MetaAuthority>,
) -> Result<HttpState, Box<dyn std::error::Error>> {
    let implicit_single_meta = config.raft.uses_implicit_single_node_meta();
    let mut raft_peers: Vec<(u64, String)> = config
        .raft
        .peers
        .iter()
        .map(|p| (p.node_id, p.url.clone()))
        .collect();
    // Durable standalone meta uses the same registered group engines as a
    // distributed node, preserving the configured node ID and WAL identity.
    if implicit_single_meta {
        raft_peers.push((
            config.raft.node_id,
            format!("http://{}", config.server.listen),
        ));
    }
    if start_maintenance_drained && raft_peers.is_empty() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "URSULA_START_MAINTENANCE_DRAINED requires a static Raft cluster",
        )
        .into());
    }

    let per_group_voters: BTreeMap<RaftGroupId, BTreeSet<u64>> = config
        .raft
        .groups
        .iter()
        .map(|g| {
            (
                RaftGroupId(g.raft_group_id),
                g.voters.iter().cloned().collect(),
            )
        })
        .collect();

    let topology = if raft_peers.is_empty() {
        Topology::SingleNode {
            raft_group_count: config.raft.group_count,
        }
    } else {
        Topology::static_cluster(
            config.raft.node_id,
            raft_peers.clone(),
            config.raft.group_count,
            config.raft.init_membership || implicit_single_meta,
            ursula_raft::StaticGrpcRaftMembershipConfig {
                initialize_membership_per_group: config.raft.init_membership_per_group
                    || implicit_single_meta,
                per_group_voters: per_group_voters.clone(),
            },
        )?
    };

    // Format epoch: refuse data and peers of another epoch before anything is
    // written.
    let log_dir = persistence.log_dir().map(std::path::Path::to_path_buf);
    crate::bootstrap::check_and_stamp_format_epoch(config, log_dir.as_deref()).await?;

    let spawned = spawn_runtime_with_maintenance_drain(
        config,
        persistence,
        topology,
        start_maintenance_drained,
    )?;
    let runtime = spawned.runtime;
    let raft_wal = spawned.raft_wal;
    // Attach committed placement and certified identities before opening any
    // group. Only an original replica's first boot may initialize genesis;
    // stale static configuration must not initialize a replacement or newcomer.
    if let Some(authority) = meta_authority {
        let registry = spawned
            .raft_registry
            .as_ref()
            .ok_or_else(|| std::io::Error::other("meta authority requires data Raft registry"))?;
        registry.set_replica_authority(config.raft.node_id, authority.replica.clone());
        registry.set_replica_genesis(authority.replica_genesis.clone());
        registry.set_replica_genesis_prefixes(authority.replica_genesis_prefixes.clone());
        let topology = authority.handle.committed_state();
        let genesis_groups = if authority.process.epoch == 1 && authority.replica.generation == 1 {
            topology
                .borrow()
                .placements
                .iter()
                .filter(|(group, placement)| {
                    placement.voters.contains(&config.raft.node_id)
                        && !authority.replica_genesis_prefixes.contains_key(*group)
                })
                .map(|(group, _)| *group)
                .collect()
        } else {
            std::collections::BTreeSet::new()
        };
        registry.set_genesis_initialization_groups(genesis_groups);
        registry.set_control_topology(topology);
        registry.set_process_authority(
            config.raft.node_id,
            authority.process.clone(),
            authority.handle.clone(),
        );
    }

    if !raft_peers.is_empty() {
        if let Some(registry) = spawned
            .raft_registry
            .as_ref()
            .filter(|_| meta_authority.is_some())
        {
            for raw_group_id in 0..config.raft.group_count {
                let group = RaftGroupId(
                    u32::try_from(raw_group_id)
                        .expect("runtime config validates raft group ids fit u32"),
                );
                if registry.control_can_open_group(group, config.raft.node_id) == Some(true) {
                    runtime.warm_group(group).await?;
                }
            }
        } else if per_group_voters.is_empty() {
            runtime.warm_all_groups().await?;
        } else {
            for raw_group_id in 0..config.raft.group_count {
                let raft_group_id = u32::try_from(raw_group_id)
                    .expect("runtime config validates raft group ids fit u32");
                if static_grpc_node_hosts_group(
                    config.raft.node_id,
                    raft_group_id,
                    &per_group_voters,
                ) {
                    runtime.warm_group(RaftGroupId(raft_group_id)).await?;
                }
            }
        }
    }

    let state = if raft_peers.is_empty() {
        HttpState::new(runtime)
    } else {
        let registry = spawned
            .raft_registry
            .expect("static grpc topology returns registry");
        HttpState::with_static_raft_cluster_topology(
            runtime,
            registry,
            config.raft.node_id,
            raft_peers,
            per_group_voters,
        )
    };
    let mut state = state
        .with_configured_node_id(config.raft.node_id)
        .with_runtime_config(&config.runtime)
        .with_raft_wal(raft_wal);
    if let Some(wal_path) = log_dir {
        let monitor = crate::bootstrap::initialize_wal_disk_monitor(
            &wal_path,
            config.raft.wal.min_available_size.as_bytes(),
            config.raft.wal.resume_available_size.as_bytes(),
        )?;
        state = state.with_wal_disk_monitor(monitor.clone());
        crate::bootstrap::spawn_wal_disk_gate(
            &state.runtime,
            wal_path,
            monitor,
            state.raft_registry().cloned(),
            config.raft.node_id,
        );
    }
    Ok(state)
}

fn parse_start_maintenance_drained(value: Option<&OsStr>) -> Result<bool, std::io::Error> {
    match value.and_then(OsStr::to_str) {
        None if value.is_none() => Ok(false),
        Some("true") => Ok(true),
        Some("false") => Ok(false),
        _ => Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "URSULA_START_MAINTENANCE_DRAINED must be exactly true or false",
        )),
    }
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

/// Serves until a shutdown signal, then stops the Raft groups and the WAL.
async fn serve(
    state: HttpState,
    config: &ursula_config::UrsulaConfig,
) -> Result<WalShutdown, Box<dyn std::error::Error>> {
    let listen: SocketAddr = config.server.listen.parse()?;
    let cluster_listen = config
        .server
        .cluster_listen
        .as_ref()
        .map(|s| s.parse::<SocketAddr>())
        .transpose()?;
    let admin_listen: SocketAddr = config.server.admin_listen.parse()?;
    let runtime = state.runtime.clone();
    let raft_wal = state.raft_wal().cloned();

    let (shutdown, shutdown_rx) = watch::channel(false);
    let signal_task = spawn_shutdown_signal_task(
        shutdown,
        state.raft_registry().cloned(),
        config.raft.node_id,
        config
            .raft
            .peers
            .iter()
            .map(|peer| (peer.node_id, peer.url.clone()))
            .collect(),
    );
    let admission = crate::IngressAdmission::new(&config.server)
        .with_wal_disk_monitor(state.wal_disk_monitor())
        .with_raft_log_pressure(
            state
                .raft_registry()
                .map(ursula_raft::RaftGroupHandleRegistry::snapshot_build_coordinator),
        );
    let mut endpoints = vec![(admin_listen, crate::admin_router(state.clone()))];
    if let Some(cluster_addr) = cluster_listen {
        endpoints.push((
            listen,
            client_router_with_admission(state.clone(), admission),
        ));
        endpoints.push((cluster_addr, cluster_router_from_state(state)));
    } else {
        endpoints.push((
            listen,
            cluster_router_from_state(state.clone())
                .merge(client_router_with_admission(state, admission)),
        ));
    }
    // Bind every socket before starting a service, so a bind error cannot
    // leave a partially serving node. Accept, parsing and handler execution
    // then run on the owners; Linux distributes connections with SO_REUSEPORT.
    let mut listeners = Vec::new();
    for (addr, app) in endpoints {
        for owner in 0..config.runtime.core_count {
            let core_id = CoreId(u16::try_from(owner)?);
            listeners.push((core_id, bind_owner_listener(addr)?, app.clone()));
        }
    }
    let mut servers = Vec::new();
    for (core_id, listener, app) in listeners {
        let stop = shutdown_rx.clone();
        servers.push(runtime.spawn_on_owner(core_id, async move {
            let listener = tokio::net::TcpListener::from_std(listener)?;
            serve_until_shutdown(listener, app, owner_shutdown(stop), None).await
        })?);
    }
    for result in futures_util::future::join_all(servers).await {
        result??;
    }
    tracing::info!("all listeners drained; stopping the Raft groups");
    runtime.stop_owner_services().await;
    let wal_shutdown = shutdown_raft_wal(&runtime, raft_wal.as_ref()).await;
    let failed_owners = runtime.shutdown_owners().await;
    if !failed_owners.is_empty() {
        tracing::warn!(?failed_owners, "owner workers stopped abnormally");
    }
    signal_task.abort();
    tracing::info!("exiting");
    Ok(wal_shutdown)
}

/// How [`shutdown_raft_wal`] ended the node's Raft WAL.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WalShutdown {
    /// The node runs no Raft WAL.
    NoWal,
    /// Every core writer closed and `fsync`ed its journal, and the run is
    /// recorded as clean. Nothing writes to the WAL any more.
    Clean,
    /// A writer did not close, or the clean run was not recorded. A writer
    /// may still be open.
    Unclean,
}

/// The end of a graceful shutdown, after the leadership handoff and the
/// listener drain: stops every Raft group, then the core journal writers,
/// each of which `fsync`s its journal, and only then records a clean
/// shutdown in the run state. A failure is logged and leaves the run
/// unclean, so the next start reads it as a crash. The shutdown grace period
/// still bounds this: when it expires first the process exits without
/// recording a clean shutdown.
pub(crate) async fn shutdown_raft_wal(
    runtime: &ursula_runtime::ShardRuntime,
    raft_wal: Option<&ursula_raft::wal::RaftWal>,
) -> WalShutdown {
    let Some(raft_wal) = raft_wal else {
        return WalShutdown::NoWal;
    };
    // A group that failed to stop can write no more once its core writer
    // has closed, so the WAL still shuts down cleanly.
    if let Err(err) = runtime.shutdown_group_engines().await {
        tracing::warn!(%err, "failed to stop every Raft group before closing the WAL");
    }
    match raft_wal.shutdown().await {
        Ok(()) => {
            tracing::info!("Raft WAL synced and recorded as cleanly shut down");
            WalShutdown::Clean
        }
        Err(err) => {
            tracing::warn!(
                %err,
                "Raft WAL did not shut down cleanly; the next start treats this run as a crash"
            );
            WalShutdown::Unclean
        }
    }
}

/// Build a nonblocking listener before registering it with the owner's reactor.
fn bind_owner_listener(addr: SocketAddr) -> std::io::Result<std::net::TcpListener> {
    let socket = socket2::Socket::new(
        socket2::Domain::for_address(addr),
        socket2::Type::STREAM,
        Some(socket2::Protocol::TCP),
    )?;
    socket.set_reuse_address(true)?;
    #[cfg(unix)]
    socket.set_reuse_port(true)?;
    socket.set_nonblocking(true)?;
    socket.bind(&addr.into())?;
    socket.listen(1024)?;
    Ok(socket.into())
}

async fn owner_shutdown(mut shutdown: watch::Receiver<bool>) {
    let _closed = shutdown.wait_for(|stopped| *stopped).await;
}

/// Grace period between the first shutdown signal and a forced exit, so a hung
/// in-flight request (or a long live-read poll) cannot block termination past
/// what systemd/Kubernetes allot before SIGKILL.
const SHUTDOWN_GRACE: Duration = Duration::from_secs(20);
/// Leave most of the overall grace period for draining HTTP requests.
const SHUTDOWN_HANDOFF_GRACE: Duration = Duration::from_secs(5);

/// Translate SIGTERM (systemd stop, Kubernetes pod termination) and Ctrl-C
/// into a bounded leadership handoff followed by listener draining, after
/// which `serve` stops the Raft groups and shuts the WAL down cleanly. A
/// second signal, or the overall grace deadline expiring, exits immediately
/// without recording a clean shutdown, so the next start treats the run as a
/// crash.
fn spawn_shutdown_signal_task(
    shutdown: watch::Sender<bool>,
    raft_registry: Option<ursula_raft::RaftGroupHandleRegistry>,
    node_id: u64,
    peers: Vec<(u64, String)>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        shutdown_signal().await;
        tracing::info!(
            "received shutdown signal; handing off leadership before draining listeners (forced exit after {SHUTDOWN_GRACE:?})"
        );
        tokio::select! {
            () = async {
                if let Some(registry) = raft_registry {
                    let result = tokio::time::timeout(
                        SHUTDOWN_HANDOFF_GRACE,
                        crate::bootstrap::handoff_shutdown_leadership(&registry, node_id, &peers),
                    ).await;
                    let remaining_leaders = registry.metrics_snapshot().iter()
                        .filter(|snapshot| snapshot.current_leader == Some(node_id)).count();
                    if matches!(result, Ok(0)) {
                        tracing::info!(remaining_leaders, "shutdown leadership handoff complete");
                    } else {
                        tracing::warn!(remaining_leaders, timed_out = result.is_err(),
                            "shutdown leadership handoff incomplete; continuing bounded shutdown");
                    }
                    registry.shutdown_transport();
                }
                shutdown.send_replace(true);
                std::future::pending::<()>().await;
            } => {}
            () = shutdown_signal() => {
                tracing::warn!(
                    "second shutdown signal; exiting immediately without a clean WAL shutdown"
                );
            }
            () = tokio::time::sleep(SHUTDOWN_GRACE) => {
                tracing::warn!(
                    "shutdown grace period expired; exiting with drains incomplete and without a \
                     clean WAL shutdown"
                );
            }
        }
        std::process::exit(0);
    })
}

#[cfg(test)]
mod tests {
    use std::ffi::OsStr;
    use std::io::Write;

    use super::parse_start_maintenance_drained;

    #[cfg(not(madsim))]
    #[tokio::test]
    async fn http_accept_and_handlers_execute_on_an_owner() {
        let mut config = ursula_runtime::RuntimeConfig::new(2, 2);
        config.threading = ursula_runtime::RuntimeThreading::ThreadPerCore;
        let runtime = ursula_runtime::ShardRuntime::spawn(config).expect("owners");
        let first = super::bind_owner_listener("127.0.0.1:0".parse().expect("addr"))
            .expect("first listener");
        let addr = first.local_addr().expect("bound addr");
        let second = super::bind_owner_listener(addr).expect("shared port");
        let (shutdown, _) = tokio::sync::watch::channel(false);
        let mut servers = Vec::new();
        for (core, listener) in [(0, first), (1, second)] {
            let stopped = shutdown.subscribe();
            servers.push(
                runtime
                    .spawn_on_owner(ursula_shard::CoreId(core), async move {
                        let app = axum::Router::new().route(
                            "/",
                            axum::routing::get(|| async {
                                std::thread::current()
                                    .name()
                                    .expect("owner name")
                                    .to_owned()
                            }),
                        );
                        ursula_observability::serve::serve_until_shutdown(
                            tokio::net::TcpListener::from_std(listener).expect("owner reactor"),
                            app,
                            super::owner_shutdown(stopped),
                            None,
                        )
                        .await
                    })
                    .expect("serve"),
            );
        }
        let client = reqwest::Client::new();
        for _ in 0..16 {
            let owner = client
                .get(format!("http://{addr}/"))
                .header("Connection", "close")
                .send()
                .await
                .expect("http")
                .text()
                .await
                .expect("body");
            assert!(matches!(owner.as_str(), "ursula-core-0" | "ursula-core-1"));
        }
        shutdown.send_replace(true);
        for server in servers {
            server.await.expect("owner alive").expect("drained");
        }
        assert!(runtime.shutdown_owners().await.is_empty());
    }

    #[test]
    fn startup_maintenance_drain_is_strict_and_opt_in() {
        assert!(!parse_start_maintenance_drained(None).unwrap());
        assert!(parse_start_maintenance_drained(Some(OsStr::new("true"))).unwrap());
        assert!(!parse_start_maintenance_drained(Some(OsStr::new("false"))).unwrap());
        parse_start_maintenance_drained(Some(OsStr::new("1")))
            .expect_err("a startup drain flag other than true or false must be rejected");
    }

    #[tokio::test]
    async fn standalone_boot_publishes_configured_node_identity_before_groups_exist() {
        use axum::body::Body;
        use axum::body::to_bytes;
        use axum::http::Request;
        use tower::ServiceExt;
        let mut config = ursula_config::UrsulaConfig::default();
        config.runtime.core_count = 1;
        config.raft.group_count = 1;
        config.raft.node_id = 7;
        assert!(config.raft.peers.is_empty());
        let wal = tempfile::tempdir().unwrap();
        let persistence = crate::Persistence::Raft {
            log_dir: wal.path().join("raft-log"),
        };
        let state = super::init_state(&config, persistence, false, None)
            .await
            .unwrap();
        let response = crate::admin_router(state)
            .oneshot(
                Request::builder()
                    .uri("/__ursula/metrics")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert!(response.status().is_success());
        let metrics: serde_json::Value =
            serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap())
                .unwrap();
        assert_eq!(metrics["process_node_id"], 7);
        assert_eq!(metrics["process_incarnation"].as_str().unwrap().len(), 32);
    }

    /// A node reports how the WAL opened: a new WAL root is read strictly
    /// and its logs are complete.
    #[tokio::test]
    async fn boot_reports_how_the_wal_opened() {
        use axum::body::Body;
        use axum::body::to_bytes;
        use axum::http::Request;
        use tower::ServiceExt;
        let dir = tempfile::tempdir().unwrap();
        let mut config = ursula_config::UrsulaConfig::default();
        config.runtime.core_count = 1;
        config.raft.group_count = 1;
        config.raft.node_id = 7;
        config.raft.wal.path = Some(dir.path().to_owned());
        config.raft.wal.fsync = ursula_config::WalFsync::Never;
        config.raft.wal.min_available_size = ursula_config::HumanSize::bytes(0);
        let wal_dir = super::RaftWalDir::resolve(&config.raft.wal).unwrap();
        assert!(matches!(wal_dir, super::RaftWalDir::Configured(_)));
        let persistence = crate::Persistence::Raft {
            log_dir: wal_dir.log_dir(),
        };
        let state = super::init_state(&config, persistence, false, None)
            .await
            .unwrap();
        let raft_wal = state.raft_wal().cloned().expect("a disk WAL starts");
        let response = crate::admin_router(state.clone())
            .oneshot(
                Request::builder()
                    .uri("/__ursula/metrics")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert!(response.status().is_success());
        let metrics: serde_json::Value =
            serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap())
                .unwrap();
        assert_eq!(
            metrics["wal_recovery"],
            serde_json::json!({
                "fsync": "never",
                "previous_run": {"kind": "absent"},
                "replay_mode": "strict",
                "recovery": {"state": "normal"},
                "recovery_epoch": 0,
                "journal_sync": "not_needed",
            })
        );
        assert_eq!(
            super::shutdown_raft_wal(&state.runtime, Some(&raft_wal)).await,
            super::WalShutdown::Clean
        );
        assert!(
            matches!(
                raft_wal.shutdown().await,
                Err(ursula_raft::wal::RaftWalError::ShutDown { .. })
            ),
            "the server shut the WAL down"
        );
    }

    /// Without `raft.wal.path` a single node runs its WAL in a fresh
    /// temporary directory, removed once the server shut down cleanly and
    /// kept otherwise. Only the zero-config default runs without Raft.
    #[tokio::test]
    async fn a_single_node_without_a_wal_path_runs_in_a_temporary_directory() {
        let mut config = ursula_config::UrsulaConfig::default();
        config.runtime.core_count = 1;
        config.raft.group_count = 1;
        config.raft.node_id = 1;
        config.raft.wal.min_available_size = ursula_config::HumanSize::bytes(0);
        assert!(!super::runs_raft(
            &config,
            Some(ursula_config::Preset::Default)
        ));
        assert!(super::runs_raft(&config, Some(ursula_config::Preset::Tiny)));
        assert!(super::runs_raft(&config, None));
        config.raft.wal.path = Some(std::path::PathBuf::from("/tmp/explicit-wal"));
        assert!(super::runs_raft(
            &config,
            Some(ursula_config::Preset::Default)
        ));
        config.raft.wal.path = None;

        let wal_dir = super::RaftWalDir::resolve(&config.raft.wal).unwrap();
        let super::RaftWalDir::Temporary(root) = &wal_dir else {
            panic!("no path configured: {wal_dir:?}");
        };
        let root = root.clone();
        let log_dir = wal_dir.log_dir();
        assert_eq!(log_dir, root.join("raft-log"));
        let state = super::init_state(
            &config,
            crate::Persistence::Raft {
                log_dir: log_dir.clone(),
            },
            false,
            None,
        )
        .await
        .unwrap();
        let raft_wal = state.raft_wal().cloned().expect("the node runs Raft");
        assert_eq!(raft_wal.root(), log_dir.as_path());
        let shutdown = super::shutdown_raft_wal(&state.runtime, Some(&raft_wal)).await;
        assert_eq!(shutdown, super::WalShutdown::Clean);
        assert!(root.exists());
        wal_dir.close(shutdown);
        assert!(!root.exists(), "a clean shutdown removes the temporary WAL");
    }

    /// A temporary WAL directory stays unless its WAL shut down cleanly: a
    /// core writer may still write to it.
    #[test]
    fn a_temporary_wal_directory_stays_without_a_clean_shutdown() {
        let config = ursula_config::UrsulaConfig::default();
        let resolve = || {
            let wal_dir = super::RaftWalDir::resolve(&config.raft.wal).unwrap();
            let super::RaftWalDir::Temporary(root) = &wal_dir else {
                panic!("no path configured: {wal_dir:?}");
            };
            let root = root.clone();
            (wal_dir, root)
        };
        let (wal_dir, unclean) = resolve();
        wal_dir.close(super::WalShutdown::Unclean);
        assert!(unclean.exists(), "an unclean shutdown keeps the directory");
        let (wal_dir, dropped) = resolve();
        drop(wal_dir);
        assert!(dropped.exists(), "an error return keeps the directory");
        for root in [unclean, dropped] {
            std::fs::remove_dir_all(root).unwrap();
        }
    }

    #[test]
    fn loads_minimal_toml_config() {
        let mut tmp = tempfile::NamedTempFile::with_suffix(".toml").unwrap();
        write!(
            tmp,
            r#"
[server]
listen = "127.0.0.1:4437"

[runtime]
core_count = 4

[raft]
group_count = 16
"#
        )
        .unwrap();
        let config = ursula_config::load_config(Some(tmp.path()), None, Some(1)).unwrap();
        assert_eq!(config.runtime.core_count, 4);
        assert_eq!(config.raft.node_id, 1);
    }
}

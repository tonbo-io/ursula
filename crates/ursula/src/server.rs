use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use clap::Args;
use tokio::sync::Notify;
use ursula_config::Preset;
use ursula_config::find_default_config;
use ursula_config::load_config;
use ursula_observability::serve::serve_until_shutdown;
use ursula_observability::serve::shutdown_signal;
use ursula_shard::RaftGroupId;

use crate::HttpState;
use crate::Persistence;
use crate::Topology;
use crate::bootstrap::spawn_runtime;
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
    let state = init_state(&config, persistence).await?;
    state.register_otel_metrics();
    let wal_shutdown = serve(state, &config).await?;
    if let Some(wal_dir) = wal_dir {
        wal_dir.close(&wal_shutdown);
    }
    // A clean run's root lock stays held until the process exits, so no
    // successor reads the clean end beside a task of this run that is still
    // running.
    if let WalShutdown::Clean(exit) = wal_shutdown {
        exit.hold_until_exit();
    }
    Ok(())
}

/// Whether the server runs Raft. Only the zero-config development mode (the
/// `default` preset on a single node) runs the in-memory engine without it.
fn runs_raft(config: &ursula_config::UrsulaConfig, preset: Option<Preset>) -> bool {
    preset != Some(Preset::Default) || !config.raft.peers.is_empty()
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
    fn close(self, shutdown: &WalShutdown) {
        let Self::Temporary(path) = self else {
            return;
        };
        if !matches!(shutdown, WalShutdown::Clean(_)) {
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
) -> Result<HttpState, Box<dyn std::error::Error>> {
    // The one source of every configured peer address.
    let raft_peers = ursula_raft::PeerDirectory::from_config(&config.raft.peers);

    let topology = if raft_peers.is_empty() {
        Topology::SingleNode {
            raft_group_count: config.raft.group_count,
        }
    } else {
        Topology::static_cluster(
            config.raft.node_id,
            raft_peers.clone(),
            config.raft.group_count,
            config.raft.placement.replication_factor,
        )
    };
    // Interim, removed in S2(h): the seed table decides what this node
    // warms, where it routes a group it does not host, and what its
    // maintenance inventory and egress gate expect.
    let seeds = topology.config_seeds().cloned().unwrap_or_default();

    // Format epoch: refuse data and peers of another epoch before anything is
    // written.
    let log_dir = persistence.log_dir().map(std::path::Path::to_path_buf);
    crate::bootstrap::check_and_stamp_format_epoch(config, &raft_peers, log_dir.as_deref()).await?;

    let spawned = spawn_runtime(config, persistence, topology)?;
    let runtime = spawned.runtime;
    let raft_wal = spawned.raft_wal;
    // Meta's Raft starts before the warm-up, on the journal the spawn path
    // opened before the runtime started.
    let meta = match spawned.meta {
        Some(meta) => meta.start().await,
        None => crate::bootstrap::MetaSlot::default(),
    };

    if !raft_peers.is_empty() {
        // The hosted groups open concurrently, as `warm_all_groups` opens
        // every group, so startup waits for the slowest open, not their sum.
        let hosted: Vec<RaftGroupId> = (0..config.raft.group_count)
            .map(|raw_group_id| {
                RaftGroupId(
                    u32::try_from(raw_group_id)
                        .expect("runtime config validates raft group ids fit u32"),
                )
            })
            .filter(|raft_group_id| seeds.hosts(config.raft.node_id, *raft_group_id))
            .collect();
        let results = futures_util::future::join_all(
            hosted
                .iter()
                .map(|raft_group_id| runtime.warm_group(*raft_group_id)),
        )
        .await;
        for (RaftGroupId(raft_group_id), result) in hosted.into_iter().zip(results) {
            let Err(error) = result else {
                continue;
            };
            // A group whose replay stopped, or whose snapshot store did not
            // answer its restore, stays closed on its core, which reopens the
            // latter after a back-off.
            if !error.leaves_group_closed() {
                return Err(error.into());
            }
            if matches!(&error, ursula_runtime::RuntimeError::GroupEngine {
                error: ursula_runtime::GroupEngineError::Infra(
                    ursula_runtime::GroupInfraError::ApplyStopped { .. }
                ),
                ..
            }) {
                tracing::error!(raft_group_id, %error, "group replay stopped; other groups remain available");
            } else {
                tracing::warn!(raft_group_id, %error, "the snapshot store did not answer the group's restore; other groups remain available");
            }
        }
    }

    let state = if raft_peers.is_empty() {
        match spawned.raft_registry {
            Some(registry) => HttpState::with_raft_registry(runtime, registry),
            None => HttpState::new(runtime),
        }
    } else {
        let registry = spawned
            .raft_registry
            .expect("static grpc topology returns registry");
        // Leader redirects send clients to a peer's client plane, which is
        // not its Raft `url` when the cluster plane listens apart.
        HttpState::with_static_raft_cluster_topology(
            runtime,
            registry,
            config.raft.node_id,
            raft_peers,
            seeds.voter_table(),
        )
    };
    let mut state = state
        .with_configured_node_id(config.raft.node_id)
        .with_runtime_config(&config.runtime)
        .with_configured_cluster_id(config.server.cluster_id.clone())
        .with_raft_wal(raft_wal)
        .with_meta(meta);
    if let Some(wal_path) = log_dir {
        let monitor = crate::bootstrap::initialize_wal_disk_monitor(
            &wal_path,
            config.raft.wal.min_available_size.as_bytes(),
            config.raft.wal.resume_available_size.as_bytes(),
        )?;
        state = state.with_wal_disk_monitor(monitor.clone());
        crate::bootstrap::spawn_wal_disk_gate(
            wal_path,
            monitor,
            state.raft_registry().cloned(),
            config.raft.node_id,
        );
    }
    Ok(state)
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
    let meta = state.meta().clone();

    let shutdown = Arc::new(Notify::new());
    spawn_shutdown_signal_task(
        shutdown.clone(),
        state.raft_registry().cloned(),
        config.raft.node_id,
    );

    // Counts the staged snapshot parts of the groups this node leads, for
    // usage. Its guard ends it on every way out of this function.
    let staged_part_counting = state.spawn_staged_part_counting();
    let admin_app = crate::admin_router(state.clone());
    let admin_listener = tokio::net::TcpListener::bind(admin_listen).await?;
    let admin_task = tokio::spawn(serve_until_shutdown(
        admin_listener,
        admin_app,
        notified(shutdown.clone()),
        Some(SHUTDOWN_DRAIN_GRACE),
    ));

    if let Some(cluster_addr) = cluster_listen {
        let client_app = client_router_with_admission(
            state.clone(),
            crate::IngressAdmission::new(&config.server)
                .with_wal_disk_monitor(state.wal_disk_monitor())
                .with_raft_log_pressure(
                    state
                        .raft_registry()
                        .map(ursula_raft::RaftGroupHandleRegistry::snapshot_build_coordinator),
                ),
        );
        let cluster_app = cluster_router_from_state(state);
        let client_listener = tokio::net::TcpListener::bind(listen).await?;
        let cluster_listener = tokio::net::TcpListener::bind(cluster_addr).await?;
        let client_task = tokio::spawn(serve_until_shutdown(
            client_listener,
            client_app,
            notified(shutdown.clone()),
            Some(SHUTDOWN_DRAIN_GRACE),
        ));
        let cluster_task = tokio::spawn(serve_until_shutdown(
            cluster_listener,
            cluster_app,
            notified(shutdown),
            Some(SHUTDOWN_DRAIN_GRACE),
        ));
        let (client_res, cluster_res, admin_res) =
            tokio::try_join!(client_task, cluster_task, admin_task)?;
        client_res?;
        cluster_res?;
        admin_res?;
    } else {
        let admission = crate::IngressAdmission::new(&config.server)
            .with_wal_disk_monitor(state.wal_disk_monitor())
            .with_raft_log_pressure(
                state
                    .raft_registry()
                    .map(ursula_raft::RaftGroupHandleRegistry::snapshot_build_coordinator),
            );
        let app = cluster_router_from_state(state.clone())
            .merge(client_router_with_admission(state, admission));
        let listener = tokio::net::TcpListener::bind(listen).await?;
        let serve_task = tokio::spawn(serve_until_shutdown(
            listener,
            app,
            notified(shutdown),
            Some(SHUTDOWN_DRAIN_GRACE),
        ));
        let (serve_res, admin_res) = tokio::try_join!(serve_task, admin_task)?;
        serve_res?;
        admin_res?;
    }
    drop(staged_part_counting);
    tracing::info!("listeners closed; stopping the Raft groups");
    let wal_shutdown = shutdown_raft_wal(&runtime, meta.take(), raft_wal.as_ref()).await;
    tracing::info!("exiting");
    Ok(wal_shutdown)
}

/// How [`shutdown_raft_wal`] ended the node's Raft WAL.
#[derive(Debug)]
pub(crate) enum WalShutdown {
    /// The node runs no Raft WAL.
    NoWal,
    /// Every core writer closed and `fsync`ed its journal, the node fence
    /// closed for good, and the run is recorded as clean. Nothing writes to
    /// the WAL any more. The process holds the root's lock until it exits;
    /// a test that drops it stands for that exit.
    Clean(ursula_raft::CleanExit),
    /// A writer did not close, or the clean run was not recorded. A writer
    /// may still be open.
    Unclean,
}

/// The end of a graceful shutdown, after the leadership handoff and the
/// listener drain: stops every data group, then the meta runtime, then the
/// journal writers, each of which `fsync`s its journal, and only then
/// records a clean shutdown in the run state. A failure is logged and leaves the run
/// unclean, so the next start reads it as a crash. The shutdown grace period
/// still bounds this: when it expires first the process exits without
/// recording a clean shutdown.
pub(crate) async fn shutdown_raft_wal(
    runtime: &ursula_runtime::ShardRuntime,
    meta: Option<ursula_raft::meta::runtime::GrpcMetaRuntime>,
    raft_wal: Option<&ursula_raft::RaftWal>,
) -> WalShutdown {
    let Some(raft_wal) = raft_wal else {
        return WalShutdown::NoWal;
    };
    // A group that failed to stop writes no more journal records once its
    // core writer has closed, so the WAL still shuts down. Its snapshot
    // pointer (`group-N.snapshot.json`) bypasses the core writer: while that
    // group's engine is still inside its shutdown, a snapshot build can
    // write the pointer until the engine closes its snapshot publication
    // (#489).
    if let Err(err) = runtime.shutdown_group_engines().await {
        tracing::warn!(%err, "failed to stop every Raft group before closing the WAL");
    }
    // Meta after the data groups: a data group's shutdown never waits on
    // meta, and meta's replica, like a group, writes no more journal records
    // once its writer has closed.
    if let Some(meta) = meta
        && let Err(err) = meta.shutdown().await
    {
        tracing::warn!(%err, "failed to stop the meta replica before closing the WAL");
    }
    match raft_wal.shutdown().await {
        Ok(exit) => {
            tracing::info!("Raft WAL synced and recorded as cleanly shut down");
            WalShutdown::Clean(exit)
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

/// Adapt the shared shutdown [`Notify`] into an owned future for
/// [`serve_until_shutdown`].
async fn notified(shutdown: Arc<Notify>) {
    shutdown.notified().await;
}

/// Grace period between the first shutdown signal and a forced exit, so a hung
/// in-flight request (or a long live-read poll) cannot block termination past
/// what systemd/Kubernetes allot before SIGKILL.
const SHUTDOWN_GRACE: Duration = Duration::from_secs(20);
/// Leave most of the overall grace period for draining HTTP requests.
const SHUTDOWN_HANDOFF_GRACE: Duration = Duration::from_secs(5);
/// Bound on the listener drain, so stopping the groups and closing the WAL
/// keep what is left of [`SHUTDOWN_GRACE`]. A connection from a peer that lost
/// power never closes, even after transport shutdown ends its inbound append
/// streams, so an unbounded drain waited for the forced exit and the run was
/// recorded as a crash.
const SHUTDOWN_DRAIN_GRACE: Duration = Duration::from_secs(10);

/// Translate SIGTERM (systemd stop, Kubernetes pod termination) and Ctrl-C
/// into a bounded leadership handoff followed by listener draining, after
/// which `serve` stops the Raft groups and shuts the WAL down cleanly. A
/// second signal, or the overall grace deadline expiring, exits immediately
/// without recording a clean shutdown, so the next start treats the run as a
/// crash.
fn spawn_shutdown_signal_task(
    shutdown: Arc<Notify>,
    raft_registry: Option<ursula_raft::RaftGroupHandleRegistry>,
    node_id: u64,
) {
    tokio::spawn(async move {
        shutdown_signal().await;
        tracing::info!(
            "received shutdown signal; handing off leadership before draining listeners (forced exit after {SHUTDOWN_GRACE:?})"
        );
        tokio::select! {
            () = async {
                if let Some(registry) = raft_registry {
                    let peers = registry.peer_cluster().directory();
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
                shutdown.notify_waiters();
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
    });
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    #[test]
    fn bounded_shutdown_phases_leave_time_to_close_the_wal() {
        let phases = super::SHUTDOWN_HANDOFF_GRACE.saturating_add(super::SHUTDOWN_DRAIN_GRACE);
        assert!(phases < super::SHUTDOWN_GRACE, "{phases:?}");
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
        let state = super::init_state(&config, persistence).await.unwrap();
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
        assert_eq!(metrics["process_node_id"], 7);
        assert_eq!(metrics["process_incarnation"].as_str().unwrap().len(), 32);
        // The WAL shuts down before its directory goes: a journal that
        // first syncs after the directory was removed cannot sync it,
        // poisons and aborts the test process.
        assert!(matches!(
            super::shutdown_raft_wal(&state.runtime, state.meta().take(), Some(&raft_wal)).await,
            super::WalShutdown::Clean(_)
        ));
    }

    /// A node without a WAL (in-memory mode) names the configured cluster
    /// on every HEAD answer, as one with a WAL does.
    #[tokio::test]
    async fn an_in_memory_node_names_the_configured_cluster() {
        use axum::body::Body;
        use axum::http::Request;
        use tower::ServiceExt;
        let mut config = ursula_config::UrsulaConfig::default();
        config.runtime.core_count = 1;
        config.raft.group_count = 1;
        config.server.cluster_id = Some(ursula_headers::ClusterId::new("dev-local").unwrap());
        let state = super::init_state(&config, crate::Persistence::InMemory)
            .await
            .unwrap();
        let response =
            crate::client_router_with_admission(state, crate::IngressAdmission::default())
                .oneshot(
                    Request::head("/benchcmp/missing")
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
        assert_eq!(response.status(), axum::http::StatusCode::NOT_FOUND);
        assert_eq!(response.headers()["ursula-cluster-id"], "dev-local");
    }

    /// `GET uri` on `router`, with its status and its JSON body.
    async fn get_json(
        router: &axum::Router,
        uri: &str,
    ) -> (axum::http::StatusCode, serde_json::Value) {
        use tower::ServiceExt;
        let response = router
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .uri(uri)
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = response.status();
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        (
            status,
            serde_json::from_slice(&body).unwrap_or(serde_json::Value::Null),
        )
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
        let state = super::init_state(&config, persistence).await.unwrap();
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
        let mut wal_recovery = metrics["wal_recovery"].clone();
        let meta = wal_recovery
            .as_object_mut()
            .unwrap()
            .remove("meta")
            .expect("the report carries the meta class");
        // The meta class opens by its own record: a fresh node's meta
        // journal opens normally, unread by nothing.
        assert_eq!(meta["previous_run"], serde_json::json!({"kind": "absent"}));
        assert_eq!(meta["recovery"], serde_json::json!({"state": "normal"}));
        assert_eq!(meta["damage"], serde_json::Value::Null);
        // Meta's replica reports apart from the data groups.
        assert_eq!(metrics["meta"]["raft_group_id"], 0);
        assert_eq!(metrics["meta"]["node_id"], 7);
        assert_eq!(
            wal_recovery,
            serde_json::json!({
                "fsync": "never",
                "previous_run": {"kind": "absent"},
                "replay_mode": "strict",
                "recovery": {"state": "normal"},
                "recovery_epoch": 0,
                "journal_sync": "not_needed",
                "predecessor": "may_live",
            })
        );
        assert!(matches!(
            super::shutdown_raft_wal(&state.runtime, state.meta().take(), Some(&raft_wal)).await,
            super::WalShutdown::Clean(_)
        ));
        assert!(
            matches!(
                raft_wal.shutdown().await,
                Err(ursula_raft::RaftWalError::ShutDown { .. })
            ),
            "the server shut the WAL down"
        );
    }

    /// A single-node durable server has no static inventory. It is ready
    /// before its first group exists and while its groups serve.
    #[tokio::test]
    async fn a_single_node_durable_server_becomes_ready() {
        use axum::body::Body;
        use axum::http::Request;
        use axum::http::StatusCode;
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
        let persistence = crate::Persistence::Raft {
            log_dir: wal_dir.log_dir(),
        };
        let state = super::init_state(&config, persistence).await.unwrap();
        let raft_wal = state.raft_wal().cloned().expect("a disk WAL starts");
        let registry = state.raft_registry().cloned().expect("a Raft node");
        // The spawn path opened the meta journal before any data group: the
        // first one opens on the create below.
        assert!(wal_dir.log_dir().join("meta").is_dir());
        assert!(registry.get(super::RaftGroupId(0)).is_none());
        // Meta forms on this node alone: its voters and nodes are this node,
        // and the default replication factor of 3 runs as 1.
        let mut placement = registry.placement_watch();
        let view = tokio::time::timeout(std::time::Duration::from_secs(30), async {
            loop {
                let view = placement.current();
                if view.cluster().is_some() {
                    return view;
                }
                assert!(placement.changed().await, "the meta runtime runs");
            }
        })
        .await
        .expect("meta bootstraps the cluster");
        let me = ursula_proto::placement::NodeId::new(7);
        let cluster = view.cluster().expect("bootstrapped");
        assert_eq!(
            cluster.params().meta_voters,
            ursula_proto::placement::VoterSet::single(me)
        );
        assert_eq!(cluster.params().replication_factor.get(), 1);
        assert_eq!(cluster.nodes().keys().copied().collect::<Vec<_>>(), vec![
            me
        ]);
        let client =
            crate::client_router_with_admission(state.clone(), crate::IngressAdmission::default());
        let admin = crate::admin_router(state.clone());
        // The placement status shows the bootstrapped cluster with this node
        // the meta voter that leads, on both listeners. Only the admin
        // listener confirms it by a meta read barrier.
        let (status, placement) = get_json(&client, "/__ursula/placement").await;
        assert_eq!(status, StatusCode::OK);
        let placement: ursula_control::PlacementStatus = serde_json::from_value(placement).unwrap();
        assert_eq!(placement.node, me);
        assert_eq!(
            placement.view,
            ursula_control::ClusterView::Bootstrapped(cluster.clone())
        );
        assert_eq!(placement.meta, ursula_control::MetaReplicaStatus::Running {
            role: ursula_control::MetaRole::Voter,
            leader: Some(me),
        });
        assert!(matches!(
            placement.registration,
            ursula_control::RegistrationStatus::Answered(_)
        ));
        let (status, _) = get_json(&client, "/__ursula/placement?confirmed=true").await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        let (status, confirmed) = get_json(&admin, "/__ursula/placement?confirmed=true").await;
        assert_eq!(status, StatusCode::OK);
        let confirmed: ursula_control::PlacementStatus = serde_json::from_value(confirmed).unwrap();
        assert_eq!(confirmed.view, placement.view);
        let (_, metrics) = get_json(&admin, "/__ursula/metrics").await;
        assert_eq!(metrics["meta"]["current_leader"], 7);
        let ready = |client: axum::Router| async move {
            client
                .oneshot(
                    Request::builder()
                        .uri(crate::READINESS_PATH)
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap()
                .status()
        };
        assert_eq!(ready(client.clone()).await, StatusCode::OK);
        let created = client
            .clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/ready/events")
                    .header("content-type", "text/plain")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(created.status(), StatusCode::CREATED);
        // Readiness reads the group's Raft metrics, which OpenRaft updates
        // just after the state machine answered the create (#503).
        state
            .raft_registry()
            .and_then(|registry| registry.get(super::RaftGroupId(0)))
            .expect("the create started group 0")
            .wait(Some(std::time::Duration::from_secs(10)))
            .metrics(
                |metrics| {
                    metrics.last_applied.is_some()
                        && metrics.last_applied.as_ref().map(|log_id| log_id.index)
                            >= metrics.local_committed.as_ref().map(|log_id| log_id.index)
                },
                "the group's metrics show the create applied",
            )
            .await
            .expect("the create applies");
        assert_eq!(ready(client).await, StatusCode::OK);
        assert!(matches!(
            super::shutdown_raft_wal(&state.runtime, state.meta().take(), Some(&raft_wal)).await,
            super::WalShutdown::Clean(_)
        ));
    }

    /// The admin recovery route takes `meta` as its target and hands the
    /// plan to the meta replica, and each plane refuses a plan floor of the
    /// other one before the plan runs.
    #[tokio::test]
    async fn the_admin_recovery_route_plans_meta_and_refuses_a_floor_of_the_other_plane() {
        use axum::body::Body;
        use axum::http::Request;
        use axum::http::StatusCode;
        use tower::ServiceExt;
        use ursula_proto::admin::PlanFloor;
        use ursula_proto::admin::PlanStep;
        use ursula_proto::admin::RecoveryPlanRefusal;
        use ursula_proto::admin::RecoveryPlanRefused;
        use ursula_proto::admin::RecoveryPlanRequest;
        use ursula_proto::admin::ReplicaLogView;
        let dir = tempfile::tempdir().unwrap();
        let mut config = ursula_config::UrsulaConfig::default();
        config.runtime.core_count = 1;
        config.raft.group_count = 1;
        config.raft.node_id = 7;
        config.raft.wal.path = Some(dir.path().to_owned());
        config.raft.wal.fsync = ursula_config::WalFsync::Never;
        config.raft.wal.min_available_size = ursula_config::HumanSize::bytes(0);
        let wal_dir = super::RaftWalDir::resolve(&config.raft.wal).unwrap();
        let state = super::init_state(&config, crate::Persistence::Raft {
            log_dir: wal_dir.log_dir(),
        })
        .await
        .unwrap();
        let raft_wal = state.raft_wal().cloned().expect("a disk WAL starts");
        let registry = state.raft_registry().cloned().expect("a Raft node");
        let mut placement = registry.placement_watch();
        tokio::time::timeout(std::time::Duration::from_secs(30), async {
            while placement.current().cluster().is_none() {
                assert!(placement.changed().await, "the meta runtime runs");
            }
        })
        .await
        .expect("meta bootstraps the cluster");
        let admin = crate::admin_router(state.clone());
        let incarnation = state.process_incarnation.to_string();
        let post = |uri: String, plan: RecoveryPlanRequest| {
            let admin = admin.clone();
            let incarnation = incarnation.clone();
            async move {
                let response = admin
                    .oneshot(
                        Request::builder()
                            .method("POST")
                            .uri(uri)
                            .header(ursula_proto::admin::PROCESS_INCARNATION_HEADER, incarnation)
                            .header("content-type", "application/json")
                            .body(Body::from(serde_json::to_vec(&plan).unwrap()))
                            .unwrap(),
                    )
                    .await
                    .unwrap();
                let status = response.status();
                let body = axum::body::to_bytes(response.into_body(), usize::MAX)
                    .await
                    .unwrap();
                (status, body)
            }
        };
        let refused = |(status, body): (StatusCode, axum::body::Bytes)| {
            assert_eq!(status, StatusCode::CONFLICT, "{body:?}");
            serde_json::from_slice::<RecoveryPlanRefused>(&body).expect("a typed refusal")
        };
        // A `follow` to the plan's own leader is refused inside the plan's
        // job, after the replica was found and before anything changed.
        let (_, metrics) = get_json(&admin, "/__ursula/metrics").await;
        let view = |replica: &serde_json::Value| ReplicaLogView {
            last_log_index: replica["last_log_index"].as_u64(),
            last_log_term: replica["last_log_term"].as_u64(),
            current_term: replica["current_term"].as_u64().expect("current term"),
        };
        let mismatched = |expected: ReplicaLogView, floor| RecoveryPlanRequest {
            leader: 7,
            point: expected.last_log_index.unwrap_or(0),
            cut: false,
            step: PlanStep::Follow {
                leader_term: expected.current_term,
            },
            floor,
            point_entry: None,
            expected,
        };
        let meta_url = "/__ursula/raft/meta/recovery/accept-unsynced-loss".to_owned();
        let meta = view(&metrics["meta"]);
        // `step_mismatch` comes from inside a plan's job, and a data group
        // would refuse the `meta` floor first: the plan reached the meta
        // replica.
        let answer = refused(post(meta_url.clone(), mismatched(meta, PlanFloor::Meta)).await);
        assert_eq!(answer.refusal, RecoveryPlanRefusal::StepMismatch);
        assert_eq!((answer.raft_group_id, answer.node_id), (0, 7));
        let answer = refused(post(meta_url, mismatched(meta, PlanFloor::Seeded)).await);
        assert_eq!(answer.refusal, RecoveryPlanRefusal::FloorOtherPlane);

        // A committed write in group 0 settles its leader and term before
        // the test reads the view it plans against.
        let created = create_a_stream(&state).await;
        assert_eq!(created, StatusCode::CREATED);
        let (_, metrics) = get_json(&admin, "/__ursula/metrics").await;
        let data = view(&metrics["raft_groups"][0]);
        let data_url = "/__ursula/raft/0/recovery/accept-unsynced-loss".to_owned();
        // A single node's data group runs no recovery gate, so only the
        // floor check, which comes before the replica lookup, answers here.
        let answer = refused(post(data_url, mismatched(data, PlanFloor::Meta)).await);
        assert_eq!(answer.refusal, RecoveryPlanRefusal::FloorOtherPlane);
        assert!(matches!(
            super::shutdown_raft_wal(&state.runtime, state.meta().take(), Some(&raft_wal)).await,
            super::WalShutdown::Clean(_)
        ));
    }

    /// Creates a stream through the client router, a write in group 0.
    async fn create_a_stream(state: &crate::HttpState) -> axum::http::StatusCode {
        use tower::ServiceExt;
        crate::client_router_with_admission(state.clone(), crate::IngressAdmission::default())
            .oneshot(
                axum::http::Request::builder()
                    .method("PUT")
                    .uri("/plans/events")
                    .header("content-type", "text/plain")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap()
            .status()
    }

    /// A meta journal that does not read leaves the node without a meta
    /// replica, and the node still starts, is ready and serves writes.
    #[tokio::test]
    async fn a_node_with_a_damaged_meta_journal_starts_and_serves() {
        use axum::body::Body;
        use axum::http::Request;
        use axum::http::StatusCode;
        use tower::ServiceExt;
        let dir = tempfile::tempdir().unwrap();
        let mut config = ursula_config::UrsulaConfig::default();
        config.runtime.core_count = 1;
        config.raft.group_count = 1;
        config.raft.node_id = 7;
        config.raft.wal.path = Some(dir.path().to_owned());
        config.raft.wal.fsync = ursula_config::WalFsync::Never;
        config.raft.wal.min_available_size = ursula_config::HumanSize::bytes(0);
        let log_dir = super::RaftWalDir::resolve(&config.raft.wal)
            .unwrap()
            .log_dir();
        let start = || async {
            super::init_state(&config, crate::Persistence::Raft {
                log_dir: log_dir.clone(),
            })
            .await
            .unwrap()
        };
        let stop = |state: crate::HttpState| async move {
            let raft_wal = state.raft_wal().cloned().expect("a disk WAL starts");
            assert!(matches!(
                super::shutdown_raft_wal(&state.runtime, state.meta().take(), Some(&raft_wal))
                    .await,
                super::WalShutdown::Clean(_)
            ));
        };
        // The first run creates the meta journal.
        let state = start().await;
        assert_eq!(
            state
                .meta()
                .with(ursula_raft::meta::runtime::MetaRuntime::state),
            Some(ursula_raft::meta::runtime::MetaRuntimeState::Running)
        );
        stop(state).await;
        std::fs::write(
            log_dir.join("meta").join("journal.meta"),
            b"broken meta metadata",
        )
        .unwrap();

        let state = start().await;
        assert!(matches!(
            state
                .meta()
                .with(ursula_raft::meta::runtime::MetaRuntime::state),
            Some(ursula_raft::meta::runtime::MetaRuntimeState::Damaged {
                kind: ursula_control::MetaDamageKind::Journal,
                ..
            })
        ));
        let client =
            crate::client_router_with_admission(state.clone(), crate::IngressAdmission::default());
        // Its status names the damage, and the node takes no leadership
        // transfer while it may still campaign.
        let (status, placement) = get_json(&client, "/__ursula/placement").await;
        assert_eq!(status, StatusCode::OK);
        let placement: ursula_control::PlacementStatus = serde_json::from_value(placement).unwrap();
        assert!(matches!(
            placement.meta,
            ursula_control::MetaReplicaStatus::Damaged {
                kind: ursula_control::MetaDamageKind::Journal,
                ..
            }
        ));
        assert_eq!(placement.applied, None);
        // The first run started on an empty root and may stop before the
        // meta group answers its registration, which leaves its predecessor
        // wait owed: this run then waits it out from its open, without
        // meta, and campaigns only after it. The wait's end clears the
        // fence's shed in the same step.
        let fence = state.raft_wal().expect("a disk WAL starts").fence();
        tokio::time::timeout(
            ursula_raft::predecessor_wait::PREDECESSOR_WAIT.saturating_mul(2),
            fence.waited(),
        )
        .await
        .expect("the predecessor wait ends without meta");
        let registry = state.raft_registry().cloned().expect("a Raft node");
        let shed = registry.leadership_shed_state();
        assert!(shed.contains(ursula_raft::LeadershipShedState::PLACEMENT_UNAVAILABLE));
        assert!(shed.should_campaign());
        assert!(!shed.should_accept_transfer());
        // A barrier has no replica to run on.
        let (status, refusal) = get_json(
            &crate::admin_router(state.clone()),
            "/__ursula/placement?confirmed=true",
        )
        .await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        let refusal: ursula_control::PlacementApiError = serde_json::from_value(refusal).unwrap();
        assert_eq!(refusal.reason, ursula_control::PlacementApiReason::Stopped);
        let ready = client
            .clone()
            .oneshot(
                Request::builder()
                    .uri(crate::READINESS_PATH)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(ready.status(), StatusCode::OK);
        let created = client
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/damaged-meta/events")
                    .header("content-type", "text/plain")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(created.status(), StatusCode::CREATED);
        stop(state).await;
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

        let wal_dir = super::RaftWalDir::resolve(&config.raft.wal).unwrap();
        let super::RaftWalDir::Temporary(root) = &wal_dir else {
            panic!("no path configured: {wal_dir:?}");
        };
        let root = root.clone();
        let log_dir = wal_dir.log_dir();
        assert_eq!(log_dir, root.join("raft-log"));
        let state = super::init_state(&config, crate::Persistence::Raft {
            log_dir: log_dir.clone(),
        })
        .await
        .unwrap();
        let raft_wal = state.raft_wal().cloned().expect("the node runs Raft");
        assert_eq!(raft_wal.root(), log_dir.as_path());
        let shutdown =
            super::shutdown_raft_wal(&state.runtime, state.meta().take(), Some(&raft_wal)).await;
        assert!(matches!(shutdown, super::WalShutdown::Clean(_)));
        assert!(root.exists());
        wal_dir.close(&shutdown);
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
        wal_dir.close(&super::WalShutdown::Unclean);
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

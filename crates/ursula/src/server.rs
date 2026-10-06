use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::ffi::OsStr;
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
    // default single-node development preset (memory WAL, node-id = 1).
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

    let mut start_maintenance_drained = parse_start_maintenance_drained(
        std::env::var_os("URSULA_START_MAINTENANCE_DRAINED").as_deref(),
    )?;
    let boot = ursula_proto::admin::ProcessIncarnation::from_bits(rand::random());
    let mut startup_admission = None;
    match std::env::var("URSULA_STARTUP_RESERVATION") {
        Ok(value) if value == "true" => {
            let helper = std::env::current_exe()?.with_file_name("ursulactl");
            let mut helper_command = tokio::process::Command::new(helper);
            helper_command
                .args([
                    "startup-admit",
                    "--node-id",
                    &config.raft.node_id.to_string(),
                    "--group-count",
                    &config.raft.group_count.to_string(),
                    "--core-count",
                    &config.runtime.core_count.to_string(),
                    "--process-incarnation",
                    boot.as_str(),
                ])
                .kill_on_drop(true);
            let output =
                tokio::time::timeout(Duration::from_secs(65), helper_command.output()).await??;
            if !output.status.success() {
                return Err(std::io::Error::other(format!(
                    "startup reservation refused: {}",
                    String::from_utf8_lossy(&output.stderr)
                ))
                .into());
            }
            let admission: ursula_proto::admin::StartupAdmission =
                serde_json::from_slice(&output.stdout)?;
            admission.validate().map_err(std::io::Error::other)?;
            if admission.process_incarnation != boot {
                return Err(std::io::Error::other(
                    "startup helper changed the fresh process incarnation",
                )
                .into());
            }
            start_maintenance_drained |= admission.start_maintenance_drained();
            startup_admission = Some(admission);
        }
        Ok(value) if value == "false" => (),
        Err(std::env::VarError::NotPresent) => (),
        _ => {
            return Err(std::io::Error::other(
                "URSULA_STARTUP_RESERVATION must be exactly true or false",
            )
            .into());
        }
    }
    // No format stamps, Raft actors, transport or listeners exist before admission.
    let mut state = init_state(&config, preset, start_maintenance_drained)
        .await?
        .with_process_incarnation(boot);
    if let Some(admission) = startup_admission {
        // No admin requests can enter the Raft API queue before this gate is
        // installed: all listeners are opened by serve.
        state = state.with_startup_maintenance_fence(admission.maintenance_fence);
    }
    state.register_otel_metrics();
    serve(state, &config).await
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
    preset: Option<Preset>,
    start_maintenance_drained: bool,
) -> Result<HttpState, Box<dyn std::error::Error>> {
    let raft_peers: Vec<(u64, String)> = config
        .raft
        .peers
        .iter()
        .map(|p| (p.node_id, p.url.clone()))
        .collect();
    if start_maintenance_drained && raft_peers.is_empty() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "URSULA_START_MAINTENANCE_DRAINED requires a static Raft cluster",
        )
        .into());
    }

    let persistence = if preset == Some(Preset::Default) && raft_peers.is_empty() {
        // Default single-node dev mode: use the simple InMemory engine (no
        // Raft overhead).  This matches the old default profile behaviour.
        Persistence::InMemory
    } else {
        Persistence::Raft {
            log_dir: config.raft.wal.resolved_path(),
        }
    };

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
            config.raft.init_membership,
            ursula_raft::StaticGrpcRaftMembershipConfig {
                initialize_membership_per_group: config.raft.init_membership_per_group,
                per_group_voters: per_group_voters.clone(),
            },
        )?
    };

    // Format epoch 2: refuse 0.5.x data and peers before anything is written.
    crate::bootstrap::check_and_stamp_format_epoch(config).await?;

    let spawned = spawn_runtime_with_maintenance_drain(
        config,
        persistence,
        topology,
        start_maintenance_drained,
    )?;
    let runtime = spawned.runtime;

    if !raft_peers.is_empty() {
        if per_group_voters.is_empty() {
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
    let wal_backend = match config.raft.wal.backend {
        ursula_config::WalBackend::Memory => "memory",
        ursula_config::WalBackend::Disk => "disk",
    };
    let mut state = state
        .with_configured_node_id(config.raft.node_id)
        .with_runtime_config(&config.runtime)
        .with_wal_backend(wal_backend);
    if let Some(wal_path) = config.raft.wal.resolved_path() {
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

async fn serve(
    state: HttpState,
    config: &ursula_config::UrsulaConfig,
) -> Result<(), Box<dyn std::error::Error>> {
    let listen: SocketAddr = config.server.listen.parse()?;
    let cluster_listen = config
        .server
        .cluster_listen
        .as_ref()
        .map(|s| s.parse::<SocketAddr>())
        .transpose()?;
    let admin_listen: SocketAddr = config.server.admin_listen.parse()?;

    let shutdown = Arc::new(Notify::new());
    spawn_shutdown_signal_task(
        shutdown.clone(),
        state.raft_registry().cloned(),
        config.raft.node_id,
        config
            .raft
            .peers
            .iter()
            .map(|peer| (peer.node_id, peer.url.clone()))
            .collect(),
    );

    let admin_app = crate::admin_router(state.clone());
    let admin_listener = tokio::net::TcpListener::bind(admin_listen).await?;
    let admin_task = tokio::spawn(serve_until_shutdown(
        admin_listener,
        admin_app,
        notified(shutdown.clone()),
        None,
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
            None,
        ));
        let cluster_task = tokio::spawn(serve_until_shutdown(
            cluster_listener,
            cluster_app,
            notified(shutdown),
            None,
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
            None,
        ));
        let (serve_res, admin_res) = tokio::try_join!(serve_task, admin_task)?;
        serve_res?;
        admin_res?;
    }
    tracing::info!("all listeners drained; exiting");
    Ok(())
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

/// Translate SIGTERM (systemd stop, Kubernetes pod termination) and Ctrl-C
/// into a bounded leadership handoff followed by listener draining. A second
/// signal, or the overall grace deadline expiring, exits immediately. Memory
/// WAL still loses all local state on exit; this is an availability optimization,
/// not protection against overlapping voter losses.
fn spawn_shutdown_signal_task(
    shutdown: Arc<Notify>,
    raft_registry: Option<ursula_raft::RaftGroupHandleRegistry>,
    node_id: u64,
    peers: Vec<(u64, String)>,
) {
    tokio::spawn(async move {
        shutdown_signal().await;
        tracing::info!(
            "received shutdown signal; handing off leadership before draining listeners (forced exit after {SHUTDOWN_GRACE:?})"
        );
        let deadline = tokio::time::Instant::now() + SHUTDOWN_GRACE;
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
                shutdown.notify_waiters();
                std::future::pending::<()>().await;
            } => {}
            () = shutdown_signal() => {
                tracing::warn!("second shutdown signal; exiting immediately");
            }
            () = tokio::time::sleep_until(deadline) => {
                tracing::warn!("shutdown grace period expired; exiting with drains incomplete");
            }
        }
        std::process::exit(0);
    });
}

#[cfg(test)]
mod tests {
    use std::ffi::OsStr;
    use std::io::Write;

    use super::parse_start_maintenance_drained;

    #[test]
    fn startup_maintenance_drain_is_strict_and_opt_in() {
        assert!(!parse_start_maintenance_drained(None).unwrap());
        assert!(parse_start_maintenance_drained(Some(OsStr::new("true"))).unwrap());
        assert!(!parse_start_maintenance_drained(Some(OsStr::new("false"))).unwrap());
        assert!(parse_start_maintenance_drained(Some(OsStr::new("1"))).is_err());
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
        let state = super::init_state(&config, None, false).await.unwrap();
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

[raft.wal]
backend = "memory"
"#
        )
        .unwrap();
        let config = ursula_config::load_config(Some(tmp.path()), None, Some(1)).unwrap();
        assert_eq!(config.runtime.core_count, 4);
        assert_eq!(config.raft.node_id, 1);
    }
}

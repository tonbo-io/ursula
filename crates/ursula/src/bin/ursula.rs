use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::net::SocketAddr;
use std::path::PathBuf;

use clap::Parser;
use ursula::HttpState;
use ursula::Persistence;
use ursula::Topology;
use ursula::client_router_with_admission;
use ursula::cluster_router_from_state;
use ursula::spawn_runtime;
use ursula_config::Preset;
use ursula_config::find_default_config;
use ursula_config::load_config;
use ursula_shard::RaftGroupId;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[derive(Parser, Debug)]
#[command(version, about = "Ursula durable-stream server")]
struct Cli {
    /// Path to the TOML, JSON, or YAML configuration file.
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

#[tokio::main]
async fn main() {
    if let Err(e) = run().await {
        eprintln!("Error: {e}"); // epirntln is fine here since we haven't initialized logging yet,
        // and the error is likely related to config loading or telemetry initialization
        std::process::exit(1);
    }
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    let config_path = cli.config.or_else(find_default_config);

    let mut preset = cli.preset;

    // When no config file and no explicit preset are given, fall back to the
    // default single-node development preset (memory WAL, node-id = 1).
    if config_path.is_none() && preset.is_none() {
        preset = Some(Preset::Default);
    }

    let config = load_config(config_path.as_deref(), preset, cli.node_id)?;

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

    let state = init_state(&config, preset).await?;
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
) -> Result<HttpState, Box<dyn std::error::Error>> {
    let raft_peers: Vec<(u64, String)> = config
        .raft
        .peers
        .iter()
        .map(|p| (p.node_id, p.url.clone()))
        .collect();

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

    let spawned = spawn_runtime(config, persistence, topology)?;
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
    let state = state.with_runtime_config(&config.runtime);
    Ok(state)
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

    if let Some(cluster_addr) = cluster_listen {
        let client_app = client_router_with_admission(
            state.clone(),
            ursula::IngressAdmission::new(&config.server),
        );
        let cluster_app = cluster_router_from_state(state);
        let client_listener = tokio::net::TcpListener::bind(listen).await?;
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
        let app = cluster_router_from_state(state.clone()).merge(client_router_with_admission(
            state,
            ursula::IngressAdmission::new(&config.server),
        ));
        let listener = tokio::net::TcpListener::bind(listen).await?;
        axum::serve(listener, app).await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::io::Write;

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

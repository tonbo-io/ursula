use std::path::PathBuf;
use std::time::Duration;

use anyhow::Context;
use anyhow::Result;
use anyhow::bail;
use clap::Args;
use clap::Parser;
use clap::Subcommand;
use ursula_ctl::MetricsClient;
use ursula_ctl::NodeInfo;
use ursula_ctl::NodeProvider;
use ursula_ctl::StaticNodeProvider;
use ursula_ctl::observe::collect_status;
use ursula_ctl::wait_ready;
use ursula_ctl::write_status;

#[derive(Parser, Debug)]
#[command(
    name = "ursulactl",
    about = "Logical cluster management for Ursula over the admin and metrics HTTP APIs",
    version
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Print per-node raft group count and leadership distribution from /__ursula/metrics.
    Status(ObserveArgs),
    /// Block until every node reports the expected number of raft groups and initialized groups have leaders.
    WaitReady(WaitReadyArgs),
    /// Mark one node as draining and transfer away every leadership it holds.
    /// The mark persists until `undrain` so the node does not re-acquire
    /// groups while the platform restarts it.
    Drain(DrainArgs),
    /// Clear a node's maintenance-drain mark so it can hold leaderships again.
    Undrain(NodeArgs),
    /// Block until one node is back as a voter in every group and caught up.
    /// Progress-gated: a node that keeps advancing is never timed out.
    Wait(WaitArgs),
    /// Arm one empty-log rejoin per group for a raft-memory node that lost its
    /// volatile log. Refused on disk-backed clusters.
    AllowRejoin(NodeArgs),
}

#[derive(Args, Debug)]
struct ObserveArgs {
    /// Cluster manifest (TOML/JSON/YAML by extension, `-` for stdin).
    #[arg(long, value_name = "PATH")]
    config: PathBuf,
    #[arg(long, default_value_t = 10)]
    http_timeout_secs: u64,
}

#[derive(Args, Debug)]
struct WaitReadyArgs {
    /// Cluster manifest (TOML/JSON/YAML by extension, `-` for stdin).
    #[arg(long, value_name = "PATH")]
    config: PathBuf,
    /// Number of raft groups each node must report
    /// (the cluster's `raft.group_count`).
    #[arg(long)]
    expected_groups: usize,
    #[arg(long, default_value_t = 120)]
    timeout_secs: u64,
    #[arg(long, default_value_t = 1)]
    poll_interval_secs: u64,
    #[arg(long, default_value_t = 5)]
    http_timeout_secs: u64,
}

#[derive(Args, Debug)]
struct NodeArgs {
    /// Cluster manifest (TOML/JSON/YAML by extension, `-` for stdin).
    #[arg(long, value_name = "PATH")]
    config: PathBuf,
    /// Target node id from the manifest.
    #[arg(long)]
    node: u64,
    #[arg(long, default_value_t = 10)]
    http_timeout_secs: u64,
}

#[derive(Args, Debug)]
struct DrainArgs {
    /// Cluster manifest (TOML/JSON/YAML by extension, `-` for stdin).
    #[arg(long, value_name = "PATH")]
    config: PathBuf,
    /// Target node id from the manifest.
    #[arg(long)]
    node: u64,
    /// Seconds to wait for the target to relinquish all leaderships before aborting.
    #[arg(long, default_value_t = 60)]
    drain_timeout_secs: u64,
    /// Budget for the surrounding whole-cluster readiness waits.
    #[arg(long, default_value_t = 120)]
    ready_timeout_secs: u64,
    #[arg(long, default_value_t = 2)]
    poll_interval_secs: u64,
    #[arg(long, default_value_t = 10)]
    http_timeout_secs: u64,
    /// Allowed gap (in log indices) between applied and committed for readiness.
    #[arg(long, default_value_t = 16)]
    lag_tolerance: u64,
    /// Print the transfer plan and stop before mutating anything.
    #[arg(long, default_value_t = false)]
    dry_run: bool,
}

#[derive(Args, Debug)]
struct WaitArgs {
    /// Cluster manifest (TOML/JSON/YAML by extension, `-` for stdin).
    #[arg(long, value_name = "PATH")]
    config: PathBuf,
    /// Target node id from the manifest.
    #[arg(long)]
    node: u64,
    /// Abort when the target makes no catch-up progress for this long.
    #[arg(long, default_value_t = 90)]
    stall_timeout_secs: u64,
    /// Absolute backstop above the stall detector.
    #[arg(long, default_value_t = 1800)]
    ready_timeout_secs: u64,
    #[arg(long, default_value_t = 2)]
    poll_interval_secs: u64,
    #[arg(long, default_value_t = 10)]
    http_timeout_secs: u64,
    /// Allowed gap (in log indices) between applied and committed for readiness.
    #[arg(long, default_value_t = 16)]
    lag_tolerance: u64,
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> Result<()> {
    let _telemetry =
        ursula_observability::init(ursula_observability::InitOptions::new("ursulactl"));

    let cli = Cli::parse();
    match cli.command {
        Command::Status(args) => run_status_subcommand(args).await,
        Command::WaitReady(args) => run_wait_ready_subcommand(args).await,
        Command::Drain(args) => run_drain_subcommand(args).await,
        Command::Undrain(args) => run_undrain_subcommand(args).await,
        Command::Wait(args) => run_wait_subcommand(args).await,
        Command::AllowRejoin(args) => run_allow_rejoin_subcommand(args).await,
    }
}

/// Load the manifest and return its node list.
async fn load_nodes(config: &std::path::Path) -> Result<Vec<NodeInfo>> {
    let manifest = StaticNodeProvider::from_path(config)
        .with_context(|| format!("load node config {}", config.display()))?;
    let nodes = manifest.list_nodes().await?;
    if nodes.is_empty() {
        bail!("node config {} contains no nodes", config.display());
    }
    Ok(nodes)
}

/// Find one node by id in the manifest.
fn find_node(nodes: &[NodeInfo], id: u64) -> Result<&NodeInfo> {
    nodes
        .iter()
        .find(|n| n.id == id)
        .ok_or_else(|| anyhow::anyhow!("node id {id} not present in the manifest"))
}

async fn run_status_subcommand(args: ObserveArgs) -> Result<()> {
    let nodes = load_nodes(&args.config).await?;
    let client = MetricsClient::new(Duration::from_secs(args.http_timeout_secs))?;
    let report = collect_status(&client, &nodes).await;
    let mut stdout = std::io::stdout().lock();
    write_status(&mut stdout, &report)?;
    Ok(())
}

async fn run_wait_ready_subcommand(args: WaitReadyArgs) -> Result<()> {
    if args.expected_groups == 0 {
        bail!("--expected-groups must be positive");
    }
    let nodes = load_nodes(&args.config).await?;
    let client = MetricsClient::new(Duration::from_secs(args.http_timeout_secs))?;
    let snapshot = wait_ready(
        &client,
        &nodes,
        args.expected_groups,
        Duration::from_secs(args.timeout_secs),
        Duration::from_secs(args.poll_interval_secs),
    )
    .await?;
    println!(
        "ready: {} node(s), {} groups each",
        snapshot.per_node.len(),
        args.expected_groups
    );
    Ok(())
}

async fn run_drain_subcommand(args: DrainArgs) -> Result<()> {
    let nodes = load_nodes(&args.config).await?;
    let client = MetricsClient::new(Duration::from_secs(args.http_timeout_secs))?;
    let target = find_node(&nodes, args.node)?;
    let options = ursula_ctl::DrainOptions {
        drain_timeout: Duration::from_secs(args.drain_timeout_secs),
        ready_timeout: Duration::from_secs(args.ready_timeout_secs),
        poll_interval: Duration::from_secs(args.poll_interval_secs),
        lag_tolerance: args.lag_tolerance,
        dry_run: args.dry_run,
    };
    match ursula_ctl::drain_node(&nodes, target, &client, &options).await? {
        ursula_ctl::DrainOutcome::Drained => {
            println!(
                "node {}: drained (mark stays set; run `undrain` after maintenance)",
                target.id
            );
            Ok(())
        }
        ursula_ctl::DrainOutcome::DryRun(plan) => {
            if plan.transfers.is_empty() {
                println!("node {}: leads no groups, nothing to transfer", target.id);
            } else {
                for transfer in &plan.transfers {
                    println!(
                        "group {}: transfer to node {}",
                        transfer.raft_group_id, transfer.preferred_successor
                    );
                }
            }
            Ok(())
        }
        ursula_ctl::DrainOutcome::Aborted { reason } => {
            eprintln!("node {}: ABORTED ({reason})", target.id);
            std::process::exit(2);
        }
    }
}

async fn run_undrain_subcommand(args: NodeArgs) -> Result<()> {
    let nodes = load_nodes(&args.config).await?;
    let client = MetricsClient::new(Duration::from_secs(args.http_timeout_secs))?;
    let target = find_node(&nodes, args.node)?;
    ursula_ctl::undrain_node(&client, target).await?;
    println!("node {}: drain mark cleared", target.id);
    Ok(())
}

async fn run_wait_subcommand(args: WaitArgs) -> Result<()> {
    let nodes = load_nodes(&args.config).await?;
    let client = MetricsClient::new(Duration::from_secs(args.http_timeout_secs))?;
    let target = find_node(&nodes, args.node)?;
    let options = ursula_ctl::CatchUpOptions {
        stall_timeout: Duration::from_secs(args.stall_timeout_secs),
        ready_timeout: Duration::from_secs(args.ready_timeout_secs),
        poll_interval: Duration::from_secs(args.poll_interval_secs),
        lag_tolerance: args.lag_tolerance,
    };
    match ursula_ctl::wait_node_ready(&nodes, target, &client, &options, false).await? {
        ursula_ctl::CatchUpOutcome::Ready => {
            println!("node {}: caught up", target.id);
            Ok(())
        }
        ursula_ctl::CatchUpOutcome::Stalled { reason } => {
            eprintln!("node {}: NOT READY ({reason})", target.id);
            std::process::exit(2);
        }
    }
}

async fn run_allow_rejoin_subcommand(args: NodeArgs) -> Result<()> {
    let nodes = load_nodes(&args.config).await?;
    let client = MetricsClient::new(Duration::from_secs(args.http_timeout_secs))?;
    let target = find_node(&nodes, args.node)?;
    // Refuses on all-disk clusters: an empty rejoin there means a wiped node.
    let allowed = ursula_ctl::resolve_empty_rejoin_policy(&client, &nodes, true).await?;
    if !allowed {
        bail!("empty-log rejoin is not applicable to this cluster");
    }
    let snap = client.fetch_cluster(&nodes).await?;
    ursula_ctl::arm_empty_rejoin(&nodes, target, &client, &snap).await?;
    println!(
        "node {}: empty-log rejoin armed on every group leader",
        target.id
    );
    Ok(())
}

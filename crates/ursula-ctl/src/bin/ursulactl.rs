use std::path::PathBuf;
use std::time::Duration;

use anyhow::Context;
use anyhow::Result;
use anyhow::bail;
use clap::Args;
use clap::Parser;
use clap::Subcommand;
use ursula_ctl::MetricsClient;
use ursula_ctl::NodeProvider;
use ursula_ctl::RestartOptions;
use ursula_ctl::RestartOutcome;
use ursula_ctl::StaticNodeProvider;
use ursula_ctl::observe::collect_status;
use ursula_ctl::run_restart;
use ursula_ctl::wait_ready;
use ursula_ctl::write_status;

#[derive(Parser, Debug)]
#[command(
    name = "ursulactl",
    about = "Safe operational CLI for Ursula clusters",
    version
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Rolling restart with raft-aware leadership drain and applied_index catch-up checks.
    Restart(RestartArgs),
    /// Print per-node raft group count and leadership distribution from /__ursula/metrics.
    Status(ObserveArgs),
    /// Block until every node reports the expected number of raft groups and initialized groups have leaders.
    WaitReady(WaitReadyArgs),
}

#[derive(Args, Debug)]
struct ObserveArgs {
    #[arg(long, value_name = "PATH")]
    config: PathBuf,
    #[arg(long, default_value_t = 10)]
    http_timeout_secs: u64,
}

#[derive(Args, Debug)]
struct WaitReadyArgs {
    #[arg(long, value_name = "PATH")]
    config: PathBuf,
    /// Number of raft groups each node must report. Matches
    /// `ClusterConfig.raft_group_count` from scripts/ursula_ec2.py.
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
struct RestartArgs {
    /// Path to the node config JSON (compatible with scripts/ursula_ec2.py's nodes.json).
    #[arg(long, value_name = "PATH")]
    config: PathBuf,
    /// Shell command template to restart a single node. Supported placeholders:
    /// `{node_id}`, `{host}`, `{http_url}`, `{name}`. Example:
    /// `ssh ec2-user@{host} sudo systemctl restart ursula-chaos.service`
    #[arg(long, value_name = "CMD")]
    restart_cmd: Option<String>,
    /// Restrict the rollout to these node ids (in the supplied order).
    /// Default: every node from the provider, in config order.
    #[arg(long = "only", value_delimiter = ',')]
    only: Option<Vec<u64>>,
    /// Seconds to wait for a target to relinquish all leaderships before aborting.
    #[arg(long, default_value_t = 60)]
    drain_timeout_secs: u64,
    /// Seconds to wait for a target to come back as voter + caught-up before aborting.
    #[arg(long, default_value_t = 180)]
    ready_timeout_secs: u64,
    /// Poll interval between metrics fetches.
    #[arg(long, default_value_t = 2)]
    poll_interval_secs: u64,
    /// Per-group HTTP timeout for a single metrics or transfer request.
    #[arg(long, default_value_t = 10)]
    http_timeout_secs: u64,
    /// Allowed gap (in log indices) between target last_applied and peer max committed
    /// for the target to be considered ready.
    #[arg(long, default_value_t = 16)]
    lag_tolerance: u64,
    /// Permit one empty-log rejoin per group before restarting a node.
    /// Use only for volatile --raft-memory clusters.
    #[arg(long, default_value_t = false)]
    allow_empty_raft_rejoin: bool,
    /// Print the drain plan and stop before issuing transfers or restart commands.
    #[arg(long, default_value_t = false)]
    dry_run: bool,
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> Result<()> {
    let _telemetry =
        ursula_observability::init(ursula_observability::InitOptions::new("ursulactl"));

    let cli = Cli::parse();
    match cli.command {
        Command::Restart(args) => run_restart_subcommand(args).await,
        Command::Status(args) => run_status_subcommand(args).await,
        Command::WaitReady(args) => run_wait_ready_subcommand(args).await,
    }
}

async fn run_status_subcommand(args: ObserveArgs) -> Result<()> {
    let provider = StaticNodeProvider::from_path(&args.config)?;
    let nodes = provider.list_nodes().await?;
    let client = MetricsClient::new(Duration::from_secs(args.http_timeout_secs))?;
    let report = collect_status(&client, &nodes).await;
    let mut stdout = std::io::stdout().lock();
    write_status(&mut stdout, &report)?;
    Ok(())
}

async fn run_wait_ready_subcommand(args: WaitReadyArgs) -> Result<()> {
    let provider = StaticNodeProvider::from_path(&args.config)?;
    let nodes = provider.list_nodes().await?;
    if args.expected_groups == 0 {
        bail!("--expected-groups must be positive");
    }
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

async fn run_restart_subcommand(args: RestartArgs) -> Result<()> {
    let provider = StaticNodeProvider::from_path(&args.config)
        .with_context(|| format!("load node config {}", args.config.display()))?;
    let nodes = provider.list_nodes().await?;
    if nodes.is_empty() {
        bail!("node config {} contains no nodes", args.config.display());
    }
    let client = MetricsClient::new(Duration::from_secs(args.http_timeout_secs))?;
    let options = RestartOptions {
        restart_cmd: args.restart_cmd.unwrap_or_default(),
        drain_timeout: Duration::from_secs(args.drain_timeout_secs),
        ready_timeout: Duration::from_secs(args.ready_timeout_secs),
        poll_interval: Duration::from_secs(args.poll_interval_secs),
        lag_tolerance: args.lag_tolerance,
        allow_empty_raft_rejoin: args.allow_empty_raft_rejoin,
        only: args.only,
        dry_run: args.dry_run,
    };
    let report = run_restart(&nodes, &client, &options).await?;
    for (id, outcome) in &report.per_node {
        match outcome {
            RestartOutcome::Restarted => println!("node {id}: restarted"),
            RestartOutcome::Skipped { reason } => println!("node {id}: skipped ({reason})"),
            RestartOutcome::Aborted { reason } => println!("node {id}: ABORTED — {reason}"),
        }
    }
    if !report.all_succeeded() {
        std::process::exit(2);
    }
    Ok(())
}

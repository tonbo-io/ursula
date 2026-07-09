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
use ursula_ctl::OperationProvider;
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

/// How ursulactl reaches each node's loopback-bound admin plane. A named
/// provider builds its own forward/restart commands; the manifest's optional
/// `[provider]` block supplies defaults and these flags override it.
#[derive(Args, Debug)]
struct ProviderArgs {
    /// Transport: `direct` (admin reachable, observe-only), `ssh`, `eice`
    /// (ssh over AWS EC2 Instance Connect), or `command` (raw templates).
    /// Overrides the manifest `[provider] kind`.
    #[arg(long)]
    provider: Option<String>,
    /// AWS region (eice).
    #[arg(long)]
    region: Option<String>,
    /// SSH login user (ssh, eice).
    #[arg(long)]
    ssh_user: Option<String>,
    /// SSH private key path (ssh, eice); its `.pub` is sent for eice.
    #[arg(long)]
    ssh_key: Option<String>,
    /// systemd unit to restart (ssh, eice).
    #[arg(long)]
    restart_unit: Option<String>,
    /// Raw port-forward command for `--provider command`. Placeholders:
    /// `{local_port}` `{admin_port}` `{admin_host}` `{host}` `{instance_id}`
    /// `{node_id}` `{name}`.
    #[arg(long, value_name = "CMD")]
    forward_cmd: Option<String>,
    /// Raw restart command for `--provider command`. Same placeholders minus
    /// the port ones.
    #[arg(long, value_name = "CMD")]
    restart_cmd: Option<String>,
    /// Seconds to wait for a forwarded local port to accept connections.
    #[arg(long, default_value_t = 20)]
    forward_ready_secs: u64,
}

impl ProviderArgs {
    /// Merge manifest `[provider]` defaults with these flag overrides into a
    /// resolved [`OperationProvider`].
    fn resolve(&self, manifest: Option<&ursula_ctl::RawProvider>) -> Result<OperationProvider> {
        let m = manifest;
        let kind_str = self
            .provider
            .clone()
            .or_else(|| m.and_then(|p| p.kind.clone()))
            .unwrap_or_else(|| "direct".to_owned());
        let kind = ursula_ctl::ProviderKind::parse(&kind_str)?;
        Ok(OperationProvider {
            kind,
            region: self
                .region
                .clone()
                .or_else(|| m.and_then(|p| p.region.clone())),
            ssh_user: self
                .ssh_user
                .clone()
                .or_else(|| m.and_then(|p| p.ssh_user.clone())),
            ssh_key: self
                .ssh_key
                .clone()
                .or_else(|| m.and_then(|p| p.ssh_key.clone())),
            restart_unit: self
                .restart_unit
                .clone()
                .or_else(|| m.and_then(|p| p.restart_unit.clone())),
            forward_cmd: self
                .forward_cmd
                .clone()
                .or_else(|| m.and_then(|p| p.forward_cmd.clone())),
            restart_cmd: self
                .restart_cmd
                .clone()
                .or_else(|| m.and_then(|p| p.restart_cmd.clone())),
            forward_ready: Duration::from_secs(self.forward_ready_secs),
        })
    }
}

#[derive(Args, Debug)]
struct ObserveArgs {
    #[arg(long, value_name = "PATH")]
    config: PathBuf,
    #[arg(long, default_value_t = 10)]
    http_timeout_secs: u64,
    #[command(flatten)]
    provider: ProviderArgs,
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
    #[command(flatten)]
    provider: ProviderArgs,
}

#[derive(Args, Debug)]
struct RestartArgs {
    /// Path to the node config JSON (compatible with scripts/ursula_ec2.py's nodes.json).
    #[arg(long, value_name = "PATH")]
    config: PathBuf,
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
    /// Force empty-log rejoin on. Normally auto-derived from each node's
    /// reported `wal_backend` (memory enables it, disk refuses it); this flag
    /// only matters for servers too old to report the backend. Setting it on a
    /// disk-backed cluster is refused.
    #[arg(long, default_value_t = false)]
    allow_empty_raft_rejoin: bool,
    /// Print the drain plan and stop before issuing transfers or restart commands.
    #[arg(long, default_value_t = false)]
    dry_run: bool,
    #[command(flatten)]
    provider: ProviderArgs,
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

/// Load the manifest, resolve the provider (manifest `[provider]` block merged
/// with CLI flags), and open admin access to every node. The returned
/// `AdminAccess` must stay in scope for the whole operation — dropping it tears
/// down any tunnels — so callers bind it and read `.nodes` from it. The
/// resolved provider is returned alongside so `restart` can build restart
/// commands from it.
async fn connect_nodes(
    config: &std::path::Path,
    args: &ProviderArgs,
    restart_needed: bool,
) -> Result<(OperationProvider, ursula_ctl::AdminAccess)> {
    let manifest = StaticNodeProvider::from_path(config)
        .with_context(|| format!("load node config {}", config.display()))?;
    let nodes = manifest.list_nodes().await?;
    if nodes.is_empty() {
        bail!("node config {} contains no nodes", config.display());
    }
    let provider = args.resolve(manifest.provider_config())?;
    provider.validate(restart_needed)?;
    let access = provider.connect(&nodes).await?;
    Ok((provider, access))
}

async fn run_status_subcommand(args: ObserveArgs) -> Result<()> {
    let (_provider, access) = connect_nodes(&args.config, &args.provider, false).await?;
    let client = MetricsClient::new(Duration::from_secs(args.http_timeout_secs))?;
    let report = collect_status(&client, &access.nodes).await;
    let mut stdout = std::io::stdout().lock();
    write_status(&mut stdout, &report)?;
    Ok(())
}

async fn run_wait_ready_subcommand(args: WaitReadyArgs) -> Result<()> {
    if args.expected_groups == 0 {
        bail!("--expected-groups must be positive");
    }
    let (_provider, access) = connect_nodes(&args.config, &args.provider, false).await?;
    let client = MetricsClient::new(Duration::from_secs(args.http_timeout_secs))?;
    let snapshot = wait_ready(
        &client,
        &access.nodes,
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
    // dry-run only prints the plan, so it needs no restart channel.
    let restart_needed = !args.dry_run;
    let (provider, access) = connect_nodes(&args.config, &args.provider, restart_needed).await?;
    let client = MetricsClient::new(Duration::from_secs(args.http_timeout_secs))?;
    let options = RestartOptions {
        drain_timeout: Duration::from_secs(args.drain_timeout_secs),
        ready_timeout: Duration::from_secs(args.ready_timeout_secs),
        poll_interval: Duration::from_secs(args.poll_interval_secs),
        lag_tolerance: args.lag_tolerance,
        force_allow_empty: args.allow_empty_raft_rejoin,
        only: args.only,
        dry_run: args.dry_run,
    };
    let report = run_restart(&access.nodes, &client, &provider, &options).await?;
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

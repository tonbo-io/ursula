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
    /// Rolling restart for hosts with no platform controller: composes drain,
    /// a provider restart command, and the catch-up wait per node.
    Restart(RestartArgs),
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

/// How ursulactl reaches each node's loopback-bound admin plane. The
/// manifest's optional `[provider]` block supplies defaults and these flags
/// override it.
#[derive(Args, Debug)]
struct ProviderArgs {
    /// Transport: `direct` (admin reachable, observe-only) or `command`
    /// (raw templates). Overrides the manifest `[provider] kind`.
    #[arg(long)]
    provider: Option<String>,
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
    /// Cluster manifest (TOML/JSON/YAML by extension, `-` for stdin).
    #[arg(long, value_name = "PATH")]
    config: PathBuf,
    #[arg(long, default_value_t = 10)]
    http_timeout_secs: u64,
    #[command(flatten)]
    provider: ProviderArgs,
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
    #[command(flatten)]
    provider: ProviderArgs,
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
    #[command(flatten)]
    provider: ProviderArgs,
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
    #[command(flatten)]
    provider: ProviderArgs,
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
    #[command(flatten)]
    provider: ProviderArgs,
}

#[derive(Args, Debug)]
struct RestartArgs {
    /// Cluster manifest (TOML/JSON/YAML by extension, `-` for stdin).
    #[arg(long, value_name = "PATH")]
    config: PathBuf,
    /// Restrict the rollout to these node ids (in the supplied order).
    /// Default: every node from the provider, in config order.
    #[arg(long = "only", value_delimiter = ',')]
    only: Option<Vec<u64>>,
    /// Seconds to wait for a target to relinquish all leaderships before aborting.
    #[arg(long, default_value_t = 60)]
    drain_timeout_secs: u64,
    /// Abort the post-restart wait if the target makes no catch-up progress for
    /// this long (no new applied entries or voter memberships). This is the
    /// real control; a steadily rebuilding node is never timed out.
    #[arg(long, default_value_t = 90)]
    stall_timeout_secs: u64,
    /// Absolute backstop on the post-restart wait, above the stall detector.
    /// A healthy rollout finishes as soon as the target catches up regardless,
    /// so this only bounds pathological cases; size it above a full rebuild.
    #[arg(long, default_value_t = 1800)]
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
        Command::Drain(args) => run_drain_subcommand(args).await,
        Command::Undrain(args) => run_undrain_subcommand(args).await,
        Command::Wait(args) => run_wait_subcommand(args).await,
        Command::AllowRejoin(args) => run_allow_rejoin_subcommand(args).await,
    }
}

/// Find one node by id in the connected manifest.
fn find_node(nodes: &[ursula_ctl::NodeInfo], id: u64) -> Result<&ursula_ctl::NodeInfo> {
    nodes
        .iter()
        .find(|n| n.id == id)
        .ok_or_else(|| anyhow::anyhow!("node id {id} not present in the manifest"))
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

async fn run_drain_subcommand(args: DrainArgs) -> Result<()> {
    let (_provider, access) = connect_nodes(&args.config, &args.provider, false).await?;
    let client = MetricsClient::new(Duration::from_secs(args.http_timeout_secs))?;
    let target = find_node(&access.nodes, args.node)?;
    let options = ursula_ctl::DrainOptions {
        drain_timeout: Duration::from_secs(args.drain_timeout_secs),
        ready_timeout: Duration::from_secs(args.ready_timeout_secs),
        poll_interval: Duration::from_secs(args.poll_interval_secs),
        lag_tolerance: args.lag_tolerance,
        dry_run: args.dry_run,
    };
    match ursula_ctl::drain_node(&access.nodes, target, &client, &options).await? {
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
    let (_provider, access) = connect_nodes(&args.config, &args.provider, false).await?;
    let client = MetricsClient::new(Duration::from_secs(args.http_timeout_secs))?;
    let target = find_node(&access.nodes, args.node)?;
    ursula_ctl::undrain_node(&client, target).await?;
    println!("node {}: drain mark cleared", target.id);
    Ok(())
}

async fn run_wait_subcommand(args: WaitArgs) -> Result<()> {
    let (_provider, access) = connect_nodes(&args.config, &args.provider, false).await?;
    let client = MetricsClient::new(Duration::from_secs(args.http_timeout_secs))?;
    let target = find_node(&access.nodes, args.node)?;
    let options = ursula_ctl::CatchUpOptions {
        stall_timeout: Duration::from_secs(args.stall_timeout_secs),
        ready_timeout: Duration::from_secs(args.ready_timeout_secs),
        poll_interval: Duration::from_secs(args.poll_interval_secs),
        lag_tolerance: args.lag_tolerance,
    };
    match ursula_ctl::wait_node_ready(&access.nodes, target, &client, &options, false).await? {
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
    let (_provider, access) = connect_nodes(&args.config, &args.provider, false).await?;
    let client = MetricsClient::new(Duration::from_secs(args.http_timeout_secs))?;
    let target = find_node(&access.nodes, args.node)?;
    // Refuses on all-disk clusters: an empty rejoin there means a wiped node.
    let allowed = ursula_ctl::resolve_empty_rejoin_policy(&client, &access.nodes, true).await?;
    if !allowed {
        bail!("empty-log rejoin is not applicable to this cluster");
    }
    let snap = client.fetch_cluster(&access.nodes).await?;
    ursula_ctl::arm_empty_rejoin(&access.nodes, target, &client, &snap).await?;
    println!(
        "node {}: empty-log rejoin armed on every group leader",
        target.id
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
        stall_timeout: Duration::from_secs(args.stall_timeout_secs),
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

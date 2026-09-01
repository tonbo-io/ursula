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
use ursula_ctl::backup;
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
    /// Quiesce one drained node for an immediate platform restart and pin
    /// survivor leadership to one anchor for durable membership repair.
    PrepareRestart(NodeArgs),
    /// Print whether the target exposes restart quiescence without mutating it.
    /// The legacy-unavailable result exists only for the 0.4.8 upgrade bridge.
    RestartQuiesceCapability(NodeArgs),
    /// Release the target and survivor maintenance fences after the prepared
    /// replacement has caught up.
    FinishPreparedRestart(NodeArgs),
    /// Release only survivor fences after a prepared replacement failed. The
    /// uncertain target remains maintenance-drained.
    AbortPreparedRestart(NodeArgs),
    /// Recover the uniquely identifiable memory-WAL voter that is missing
    /// whole Raft groups. Drains and quiesces it for platform replacement.
    PrepareAmnesiacRestart(RestartTargetArgs),
    /// Rebuild an unready replacement by detaching it from each affected Raft
    /// group, attaching it as a blocking learner, and promoting it after
    /// catch-up. Safe to resume after a partially completed repair.
    RepairRestartedVoter(RestartTargetArgs),
    /// Drain a partial replacement that predates restart quiescence so the
    /// platform can replace it with a binary that supports membership repair.
    PrepareRecoveryHandoff(RestartTargetArgs),
    /// Print the unique safely recoverable amnesiac voter id, or `none` when
    /// every voter is ready. Refuses every other unready cluster shape.
    ClassifyAmnesiac(AmnesiacClassifyArgs),
    /// Strictly verify that every configured node is a voter in every group,
    /// caught up, and observes a usable leader.
    VerifyCluster(VerifyClusterArgs),
    /// Create a verifiable backup of every raft group into a local directory
    /// or `s3://bucket/prefix`.
    #[command(name = "backup-create")]
    BackupCreate(BackupCreateArgs),
    /// Verify a backup's manifest, checksums, and snapshot validity without
    /// touching any cluster.
    #[command(name = "backup-verify")]
    BackupVerify(BackupLocationArgs),
    /// Restore a verified backup into a fresh, empty cluster with the same
    /// raft group count.
    Restore(BackupCreateArgs),
}

#[derive(Args, Debug)]
struct BackupCreateArgs {
    /// Cluster manifest (TOML/JSON/YAML by extension, `-` for stdin).
    #[arg(long, value_name = "PATH")]
    config: PathBuf,
    /// Backup location: local directory or `s3://bucket/prefix`.
    #[arg(long, value_name = "LOCATION")]
    location: String,
    /// Manifest creation timestamp override (unix milliseconds); defaults to
    /// the current wall clock.
    #[arg(long)]
    created_unix_ms: Option<u64>,
    #[arg(long, default_value_t = 30)]
    http_timeout_secs: u64,
}

#[derive(Args, Debug)]
struct BackupLocationArgs {
    /// Backup location: local directory or `s3://bucket/prefix`.
    #[arg(long, value_name = "LOCATION")]
    location: String,
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
struct RestartTargetArgs {
    /// Cluster manifest (TOML/JSON/YAML by extension, `-` for stdin).
    #[arg(long, value_name = "PATH")]
    config: PathBuf,
    /// Target node id from the manifest.
    #[arg(long)]
    node: u64,
    /// Seconds to transfer the target's remaining leaderships before aborting.
    #[arg(long, default_value_t = 300)]
    drain_timeout_secs: u64,
    #[arg(long, default_value_t = 2)]
    poll_interval_secs: u64,
    #[arg(long, default_value_t = 10)]
    http_timeout_secs: u64,
    /// Allowed replication gap on every surviving peer.
    #[arg(long, default_value_t = 16)]
    lag_tolerance: u64,
}

#[derive(Args, Debug)]
struct AmnesiacClassifyArgs {
    /// Cluster manifest (TOML/JSON/YAML by extension, `-` for stdin).
    #[arg(long, value_name = "PATH")]
    config: PathBuf,
    #[arg(long, default_value_t = 10)]
    http_timeout_secs: u64,
    /// Allowed replication gap on every surviving peer.
    #[arg(long, default_value_t = 16)]
    lag_tolerance: u64,
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

#[derive(Args, Debug)]
struct VerifyClusterArgs {
    /// Cluster manifest (TOML/JSON/YAML by extension, `-` for stdin).
    #[arg(long, value_name = "PATH")]
    config: PathBuf,
    /// Seconds to wait for two consecutive strict-ready samples.
    #[arg(long, default_value_t = 120)]
    timeout_secs: u64,
    #[arg(long, default_value_t = 2)]
    poll_interval_secs: u64,
    #[arg(long, default_value_t = 10)]
    http_timeout_secs: u64,
    /// Allowed gap (in log indices) between applied and committed.
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
        Command::PrepareRestart(args) => run_prepare_restart_subcommand(args).await,
        Command::RestartQuiesceCapability(args) => {
            run_restart_quiesce_capability_subcommand(args).await
        }
        Command::FinishPreparedRestart(args) => run_finish_prepared_restart_subcommand(args).await,
        Command::AbortPreparedRestart(args) => run_abort_prepared_restart_subcommand(args).await,
        Command::PrepareAmnesiacRestart(args) => {
            run_prepare_amnesiac_restart_subcommand(args).await
        }
        Command::RepairRestartedVoter(args) => run_repair_restarted_voter_subcommand(args).await,
        Command::PrepareRecoveryHandoff(args) => {
            run_prepare_recovery_handoff_subcommand(args).await
        }
        Command::ClassifyAmnesiac(args) => run_classify_amnesiac_subcommand(args).await,
        Command::VerifyCluster(args) => run_verify_cluster_subcommand(args).await,
        Command::BackupCreate(args) => run_backup_create_subcommand(args).await,
        Command::BackupVerify(args) => run_backup_verify_subcommand(args).await,
        Command::Restore(args) => run_restore_subcommand(args).await,
    }
}

fn backup_client(nodes: &[NodeInfo], http_timeout_secs: u64) -> Result<backup::BackupClient> {
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(http_timeout_secs))
        .build()
        .context("build backup HTTP client")?;
    let urls = nodes
        .iter()
        .map(|node| node.admin_url.as_str().trim_end_matches('/').to_owned())
        .collect::<Vec<_>>();
    backup::BackupClient::new(http, urls)
}

fn wall_clock_unix_ms() -> u64 {
    // Operator-CLI wall clock: manifests are billing/ops artifacts, not
    // simulation-visible state.
    use std::time::SystemTime;
    use std::time::UNIX_EPOCH;
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}

async fn run_backup_create_subcommand(args: BackupCreateArgs) -> Result<()> {
    let nodes = load_nodes(&args.config).await?;
    let client = backup_client(&nodes, args.http_timeout_secs)?;
    let store = backup::BackupStore::open(&args.location)?;
    let created_unix_ms = args.created_unix_ms.unwrap_or_else(wall_clock_unix_ms);
    let manifest = backup::create(&client, &store, created_unix_ms).await?;
    let (buckets, streams) = manifest.groups.iter().fold((0u64, 0u64), |acc, group| {
        (
            acc.0.saturating_add(group.buckets),
            acc.1.saturating_add(group.streams),
        )
    });
    println!(
        "backup created: {} groups, {buckets} buckets, {streams} streams -> {}",
        manifest.raft_group_count, args.location
    );
    Ok(())
}

async fn run_backup_verify_subcommand(args: BackupLocationArgs) -> Result<()> {
    let store = backup::BackupStore::open(&args.location)?;
    let report = backup::verify(&store).await?;
    println!(
        "backup verified: {} groups, {} buckets, {} streams",
        report.groups, report.buckets, report.streams
    );
    Ok(())
}

async fn run_restore_subcommand(args: BackupCreateArgs) -> Result<()> {
    let nodes = load_nodes(&args.config).await?;
    let client = backup_client(&nodes, args.http_timeout_secs)?;
    let store = backup::BackupStore::open(&args.location)?;
    let report = backup::restore(&client, &store).await?;
    println!(
        "restore complete: {} groups, {} buckets, {} streams",
        report.groups, report.buckets, report.streams
    );
    Ok(())
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
    match ursula_ctl::wait_node_ready(&nodes, target, &client, &options).await? {
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

async fn run_prepare_restart_subcommand(args: NodeArgs) -> Result<()> {
    let nodes = load_nodes(&args.config).await?;
    let client = MetricsClient::new(Duration::from_secs(args.http_timeout_secs))?;
    let target = find_node(&nodes, args.node)?;
    let preparation =
        ursula_ctl::prepare_restart(&nodes, target, &client, &ursula_ctl::DrainOptions {
            ready_timeout: Duration::ZERO,
            dry_run: false,
            ..ursula_ctl::DrainOptions::default()
        })
        .await?;
    println!(
        "node {}: quiesced with restart leaders pinned to node {}; fenced survivors={:?}",
        target.id, preparation.leader_anchor, preparation.fenced_node_ids
    );
    Ok(())
}

async fn run_restart_quiesce_capability_subcommand(args: NodeArgs) -> Result<()> {
    let nodes = load_nodes(&args.config).await?;
    let client = MetricsClient::new(Duration::from_secs(args.http_timeout_secs))?;
    let target = find_node(&nodes, args.node)?;
    match client.restart_quiesce_capability(target).await? {
        ursula_ctl::RestartQuiesceCapability::Supported => println!("supported"),
        ursula_ctl::RestartQuiesceCapability::LegacyUnavailable => {
            println!("legacy-unavailable")
        }
    }
    Ok(())
}

async fn run_finish_prepared_restart_subcommand(args: NodeArgs) -> Result<()> {
    let nodes = load_nodes(&args.config).await?;
    let client = MetricsClient::new(Duration::from_secs(args.http_timeout_secs))?;
    let target = find_node(&nodes, args.node)?;
    ursula_ctl::finish_prepared_restart(&nodes, target, &client).await?;
    println!("node {}: prepared restart fences cleared", target.id);
    Ok(())
}

async fn run_abort_prepared_restart_subcommand(args: NodeArgs) -> Result<()> {
    let nodes = load_nodes(&args.config).await?;
    let client = MetricsClient::new(Duration::from_secs(args.http_timeout_secs))?;
    let target = find_node(&nodes, args.node)?;
    ursula_ctl::abort_prepared_restart(&nodes, target, &client).await?;
    println!(
        "node {}: survivor restart fences cleared; target remains drained",
        target.id
    );
    Ok(())
}

async fn run_prepare_amnesiac_restart_subcommand(args: RestartTargetArgs) -> Result<()> {
    let nodes = load_nodes(&args.config).await?;
    let client = MetricsClient::new(Duration::from_secs(args.http_timeout_secs))?;
    let target = find_node(&nodes, args.node)?;
    let preparation =
        ursula_ctl::prepare_amnesiac_restart(&nodes, target, &client, &ursula_ctl::DrainOptions {
            drain_timeout: Duration::from_secs(args.drain_timeout_secs),
            ready_timeout: Duration::ZERO,
            poll_interval: Duration::from_secs(args.poll_interval_secs),
            lag_tolerance: args.lag_tolerance,
            dry_run: false,
        })
        .await?;
    println!(
        "node {}: prepared amnesiac restart for {} missing group(s); leaders pinned to node {}; fenced survivors={:?}; restart immediately",
        target.id,
        preparation.missing_group_count,
        preparation.leader_anchor,
        preparation.fenced_node_ids
    );
    Ok(())
}

async fn run_repair_restarted_voter_subcommand(args: RestartTargetArgs) -> Result<()> {
    let nodes = load_nodes(&args.config).await?;
    let client = MetricsClient::new(Duration::from_secs(args.http_timeout_secs))?;
    let target = find_node(&nodes, args.node)?;
    let preparation = ursula_ctl::repair_restarted_voter(
        &nodes,
        target,
        &client,
        &ursula_ctl::DrainOptions {
            drain_timeout: Duration::from_secs(args.drain_timeout_secs),
            ready_timeout: Duration::ZERO,
            poll_interval: Duration::from_secs(args.poll_interval_secs),
            lag_tolerance: args.lag_tolerance,
            dry_run: false,
        },
        &ursula_ctl::MembershipRepairOptions::default(),
    )
    .await?;
    println!(
        "node {}: repaired {} unready group(s) through learner catch-up; leaders pinned to node {}; fenced survivors={:?}",
        target.id,
        preparation.missing_group_count,
        preparation.leader_anchor,
        preparation.fenced_node_ids
    );
    Ok(())
}

async fn run_prepare_recovery_handoff_subcommand(args: RestartTargetArgs) -> Result<()> {
    let nodes = load_nodes(&args.config).await?;
    let client = MetricsClient::new(Duration::from_secs(args.http_timeout_secs))?;
    let target = find_node(&nodes, args.node)?;
    let missing_groups =
        ursula_ctl::prepare_recovery_handoff(&nodes, target, &client, &ursula_ctl::DrainOptions {
            drain_timeout: Duration::from_secs(args.drain_timeout_secs),
            ready_timeout: Duration::ZERO,
            poll_interval: Duration::from_secs(args.poll_interval_secs),
            lag_tolerance: args.lag_tolerance,
            dry_run: false,
        })
        .await?;
    println!(
        "node {}: recovery handoff drained; {} group(s) are wholly missing; replace with a restart-quiesce-capable binary",
        target.id, missing_groups
    );
    Ok(())
}

async fn run_classify_amnesiac_subcommand(args: AmnesiacClassifyArgs) -> Result<()> {
    let nodes = load_nodes(&args.config).await?;
    let client = MetricsClient::new(Duration::from_secs(args.http_timeout_secs))?;
    let snapshot = client.fetch_cluster(&nodes).await?;
    let configured_node_ids = nodes.iter().map(|node| node.id).collect::<Vec<_>>();
    match ursula_ctl::classify_amnesiac_voter(&snapshot, &configured_node_ids, args.lag_tolerance)
        .map_err(anyhow::Error::msg)?
    {
        Some(candidate) => println!("{}", candidate.node_id),
        None => println!("none"),
    }
    Ok(())
}

async fn run_verify_cluster_subcommand(args: VerifyClusterArgs) -> Result<()> {
    let nodes = load_nodes(&args.config).await?;
    let client = MetricsClient::new(Duration::from_secs(args.http_timeout_secs))?;
    ursula_ctl::wait_cluster_ready(
        "strict cluster verification",
        &nodes,
        &client,
        Duration::from_secs(args.timeout_secs),
        Duration::from_secs(args.poll_interval_secs),
        args.lag_tolerance,
    )
    .await?;
    println!("cluster verified: {} node(s) fully ready", nodes.len());
    Ok(())
}

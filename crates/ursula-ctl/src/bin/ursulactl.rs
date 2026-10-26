use std::io::Write;
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
use ursula_shard::RaftGroupId;

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
    /// Block until every node is ready and serves, and the meta leader's
    /// placement view shows the cluster bootstrapped with every group live,
    /// no move, and running on exactly its target voters with a leader.
    WaitReady(WaitReadyArgs),
    /// Mark one node as draining and transfer away every leadership it holds.
    /// The mark persists until `undrain` so the node does not re-acquire
    /// groups while the platform restarts it.
    Drain(DrainArgs),
    /// Clear a node's maintenance-drain mark so it can hold leaderships again.
    Undrain(NodeArgs),
    /// Block until one node is a caught-up voter again in every group the
    /// meta leader's placement view places it among and every group its own
    /// report counts it a voter of. Progress-gated: a node that keeps
    /// advancing is never timed out. A node that lost entries is caught up
    /// by its groups' leaders meanwhile.
    Wait(WaitArgs),
    /// Strictly verify, in two samples in a row, that every group of the meta
    /// leader's placement view is live and runs on exactly its target
    /// voters, each caught up, with a usable leader.
    VerifyCluster(VerifyClusterArgs),
    /// Capture fresh quorum prefixes and verify every target voter of every
    /// group of the meta leader's placement view.
    VerifyQuorum(VerifyQuorumArgs),
    /// Verify fresh prefixes on every target voter but one excluded node,
    /// without claiming restored redundancy.
    VerifySurvivors(VerifySurvivorsArgs),
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
    /// Accept the unsynced loss of one stalled group, the meta group or a
    /// data group: plan it from every reachable replica, send the plan to
    /// its leader first and then to the other members, and wait until its
    /// history event applied. A plan that wedges is planned again. Recover
    /// meta first: a data group's plan needs a confirmed meta view.
    RecoverGroup(RecoverGroupArgs),
    /// Operations on one node id in the meta group.
    #[command(subcommand)]
    Node(NodeCommand),
    /// Where groups live and how their moves go.
    #[command(subcommand)]
    Placement(PlacementCommand),
}

#[derive(Subcommand, Debug)]
enum PlacementCommand {
    /// Print a node's placement view, and for each moving group (or the
    /// group asked for) what its leader's reconcile shell does.
    Status(PlacementStatusArgs),
    /// Move groups to other voters. Groups queue under the move limit, a
    /// group already stable at or moving to its target counts as done, so a
    /// rerun finishes an interrupted batch, and other refusals are reported
    /// per group. Exits 2 when a group was refused or timed out.
    Move(MoveArgs),
    /// Move a moving group back to its source. Exits 2 when it was refused
    /// or timed out.
    Cancel(CancelArgs),
    /// Set how many groups may move at once. Exits 2 when the limit was
    /// refused or did not take effect.
    SetMoveLimit(SetMoveLimitArgs),
}

#[derive(Args, Debug)]
struct PlacementStatusArgs {
    /// Cluster manifest (TOML/JSON/YAML by extension, `-` for stdin).
    #[arg(long, value_name = "PATH")]
    config: PathBuf,
    /// List every group this node has a role in, not only moving ones.
    #[arg(long, conflicts_with = "group")]
    node: Option<u64>,
    /// List this group and ask for its leader, moving or not.
    #[arg(long)]
    group: Option<u32>,
    /// Read a view a meta read barrier confirmed, on an admin plane.
    #[arg(long, default_value_t = false)]
    confirmed: bool,
    /// Print the report as JSON.
    #[arg(long, default_value_t = false)]
    json: bool,
    #[arg(long, default_value_t = 10)]
    http_timeout_secs: u64,
}

#[derive(Args, Debug)]
struct MoveWaitArgs {
    /// Wait until every group is stable at its target, printing each
    /// leader's state as it changes.
    #[arg(long, default_value_t = false)]
    wait: bool,
    /// Bounds the command: groups queued under the move limit and, with
    /// `--wait`, the moves.
    #[arg(long, default_value_t = 600)]
    timeout_secs: u64,
    #[arg(long, default_value_t = 2)]
    poll_interval_secs: u64,
    #[arg(long, default_value_t = 10)]
    http_timeout_secs: u64,
}

#[derive(Args, Debug)]
#[command(group(clap::ArgGroup::new("target").required(true).args(["to", "replace"])))]
struct MoveArgs {
    /// Cluster manifest (TOML/JSON/YAML by extension, `-` for stdin).
    #[arg(long, value_name = "PATH")]
    config: PathBuf,
    /// A group to move. Repeat it for a batch.
    #[arg(long = "group", required = true)]
    groups: Vec<u32>,
    /// The voters to move to, as many as the group has: `2,3,4`.
    #[arg(long, value_delimiter = ',')]
    to: Vec<u64>,
    /// Replace node OLD by node NEW in each group's target: `3=4`.
    #[arg(long, value_name = "OLD=NEW")]
    replace: Option<ursula_ctl::placement::moves::Replacement>,
    /// The placement key to move from, `term-leader-index` as the status
    /// prints it, in place of the confirmed view's. One group only. A
    /// moving group named by its key takes the new target.
    #[arg(long, value_name = "SINCE", conflicts_with = "retarget")]
    expect: Option<ursula_proto::placement::PlacementEpoch>,
    /// Let a moving group take the new target.
    #[arg(long, default_value_t = false)]
    retarget: bool,
    /// Move onto nodes the meta leader's report does not list live, or with
    /// no report reachable. A group whose new voter is down waits until it
    /// is back.
    #[arg(long, default_value_t = false)]
    allow_not_live: bool,
    #[command(flatten)]
    wait: MoveWaitArgs,
}

#[derive(Args, Debug)]
struct CancelArgs {
    /// Cluster manifest (TOML/JSON/YAML by extension, `-` for stdin).
    #[arg(long, value_name = "PATH")]
    config: PathBuf,
    #[arg(long)]
    group: u32,
    /// The placement key of the move, `term-leader-index` as the status
    /// prints it, in place of the confirmed view's.
    #[arg(long, value_name = "SINCE")]
    expect: Option<ursula_proto::placement::PlacementEpoch>,
    #[command(flatten)]
    wait: MoveWaitArgs,
}

#[derive(Args, Debug)]
struct SetMoveLimitArgs {
    /// Cluster manifest (TOML/JSON/YAML by extension, `-` for stdin).
    #[arg(long, value_name = "PATH")]
    config: PathBuf,
    /// The most groups that may move at once. 0 stops new moves.
    #[arg(long)]
    limit: u32,
    #[arg(long, default_value_t = 10)]
    http_timeout_secs: u64,
}

#[derive(Subcommand, Debug)]
enum NodeCommand {
    /// Release the root (the WAL disk) a node id is bound to, so the empty
    /// disk the node runs on now binds with no restart. Run it once the
    /// old disk is revoked: the release cannot be undone. The release goes
    /// to another node's admin plane.
    ReplaceDisk(ReplaceDiskArgs),
    /// Cordon nodes: they keep their replicas and take no new one. Exits 2
    /// when a node was refused or the timeout passed.
    Cordon(LifecycleArgs),
    /// Return cordoned or decommissioning nodes to active, which aborts a
    /// decommission while the node still has a role. Exits 2 when a node was
    /// refused or the timeout passed.
    Uncordon(LifecycleArgs),
    /// Decommission nodes: checks every named node first (a meta voter is
    /// refused with nothing sent), cordons every active one, then
    /// decommissions each. The meta leader moves their groups away, and a
    /// node is removed for good once it has no role. With `--wait` it waits
    /// until every node is removed. Exits 2 when a node was refused or the
    /// timeout passed.
    Decommission(DecommissionArgs),
    /// Verify that every named node is removed, vacant or never registered,
    /// so it may leave. Reads the meta leader's view over the read URLs.
    /// Exits 2 when a node is in a life.
    VerifyRemoved(VerifyRemovalArgs),
    /// Verify that every named node may start: it never registered, it is
    /// vacant (an empty disk registers as a new node), or it is in a life
    /// (its own disk comes back). Waits, up to the timeout, while a node is
    /// removed and its removal has not completed, printing what it waits
    /// for. Exits 2 when a removal has not completed by the timeout.
    VerifyJoinable(VerifyRemovalArgs),
    /// Verify that every named node is vacant, so a disk left under its id
    /// holds a removed life's state and may be deleted. Waits like
    /// `verify-joinable` for a pending removal. Exits 3 when a node is in a
    /// life (its disk is its own: keep it), and 2 for anything else.
    VerifyVacant(VerifyRemovalArgs),
}

#[derive(Args, Debug)]
struct LifecycleArgs {
    /// Cluster manifest (TOML/JSON/YAML by extension, `-` for stdin).
    #[arg(long, value_name = "PATH")]
    config: PathBuf,
    /// A node id. Repeat it for several nodes.
    #[arg(long = "node", required = true)]
    nodes: Vec<u64>,
    /// Bounds the command.
    #[arg(long, default_value_t = 60)]
    timeout_secs: u64,
    #[arg(long, default_value_t = 2)]
    poll_interval_secs: u64,
    #[arg(long, default_value_t = 10)]
    http_timeout_secs: u64,
}

#[derive(Args, Debug)]
struct DecommissionArgs {
    /// Cluster manifest (TOML/JSON/YAML by extension, `-` for stdin).
    #[arg(long, value_name = "PATH")]
    config: PathBuf,
    /// A node id. Repeat it for several nodes.
    #[arg(long = "node", required = true)]
    nodes: Vec<u64>,
    /// Wait until every node is removed, printing how many groups each
    /// still has a role in.
    #[arg(long, default_value_t = false)]
    wait: bool,
    /// Bounds the command, with `--wait` the moves of every group away.
    #[arg(long, default_value_t = 3600)]
    timeout_secs: u64,
    #[arg(long, default_value_t = 2)]
    poll_interval_secs: u64,
    #[arg(long, default_value_t = 10)]
    http_timeout_secs: u64,
}

#[derive(Args, Debug)]
struct VerifyRemovalArgs {
    /// Cluster manifest (TOML/JSON/YAML by extension, `-` for stdin).
    #[arg(long, value_name = "PATH")]
    config: PathBuf,
    /// A node id. Repeat it for several nodes.
    #[arg(long = "node", required = true)]
    nodes: Vec<u64>,
    /// How long to read again while no node names a meta leader, or a
    /// node's removal has not completed.
    #[arg(long, default_value_t = 60)]
    timeout_secs: u64,
    #[arg(long, default_value_t = 1)]
    poll_interval_secs: u64,
    #[arg(long, default_value_t = 5)]
    http_timeout_secs: u64,
}

#[derive(Args, Debug)]
struct ReplaceDiskArgs {
    /// Cluster manifest (TOML/JSON/YAML by extension, `-` for stdin).
    #[arg(long, value_name = "PATH")]
    config: PathBuf,
    /// The node id whose disk was replaced.
    #[arg(long)]
    node: u64,
    /// The root the node id is bound to now, in 16 lowercase hex digits, as
    /// the placement status, `root_refused` and the first 16 digits of a
    /// process incarnation print it. The release applies only while the
    /// slot still names it.
    #[arg(long)]
    expect_root: ursula_proto::placement::RootId,
    /// Release even when the node still runs on the root or reports ready.
    #[arg(long, default_value_t = false)]
    force: bool,
    #[arg(long, default_value_t = 10)]
    http_timeout_secs: u64,
}

#[derive(Args, Debug)]
struct RecoverGroupArgs {
    /// Cluster manifest (TOML/JSON/YAML by extension, `-` for stdin).
    #[arg(long, value_name = "PATH")]
    config: PathBuf,
    /// The stalled group: `meta`, or a data group's id.
    #[arg(long)]
    target: ursula_ctl::recovery::RecoverTarget,
    /// Plans to send at most, the first included.
    #[arg(long, default_value_t = 3)]
    max_plans: usize,
    /// Seconds one plan may take to be satisfied.
    #[arg(long, default_value_t = 120)]
    settle_timeout_secs: u64,
    #[arg(long, default_value_t = 2)]
    poll_interval_secs: u64,
    #[arg(long, default_value_t = 10)]
    http_timeout_secs: u64,
    /// Seconds one `accept-unsynced-loss` may take: a cut or a demotion
    /// restarts the group before the answer.
    #[arg(long, default_value_t = 120)]
    accept_timeout_secs: u64,
    /// Send the plan, accepting the loss of every write acknowledged after
    /// the last `fsync` of the replicas it keeps. Without it the command
    /// explains the plan it would send and sends nothing.
    #[arg(long, default_value_t = false)]
    accept_loss: bool,
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

#[derive(Args, Debug)]
struct VerifyQuorumArgs {
    /// Cluster manifest (TOML/JSON/YAML by extension, `-` for stdin).
    #[arg(long, value_name = "PATH")]
    config: PathBuf,
    #[arg(long, default_value_t = 120)]
    timeout_secs: u64,
    #[arg(long, default_value_t = 1)]
    poll_interval_secs: u64,
    #[arg(long, default_value_t = 10)]
    http_timeout_secs: u64,
}

impl VerifyQuorumArgs {
    fn options(&self) -> ursula_ctl::quorum::QuorumVerificationOptions {
        ursula_ctl::quorum::QuorumVerificationOptions {
            timeout: Duration::from_secs(self.timeout_secs),
            poll_interval: Duration::from_secs(self.poll_interval_secs),
        }
    }
}

#[derive(Args, Debug)]
struct VerifySurvivorsArgs {
    #[command(flatten)]
    quorum: VerifyQuorumArgs,
    /// The one node of the manifest to omit from observation. This option
    /// does not establish that its old process or host cannot return.
    #[arg(long)]
    excluded_node_id: u64,
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> Result<()> {
    let _telemetry =
        ursula_observability::init(ursula_observability::InitOptions::new("ursulactl"));

    let cli = Cli::parse();
    match cli.command {
        Command::Status(args) => run_status_subcommand(args).await,
        Command::WaitReady(args) => run_wait_ready_subcommand(args).await,
        Command::RecoverGroup(args) => run_recover_group_subcommand(args).await,
        Command::Node(command) => run_lifecycle_subcommand(command).await,
        Command::Placement(command) => run_placement_subcommand(command).await,
        Command::Drain(args) => run_drain_subcommand(args).await,
        Command::Undrain(args) => run_undrain_subcommand(args).await,
        Command::Wait(args) => run_wait_subcommand(args).await,
        Command::VerifyCluster(args) => run_verify_cluster_subcommand(args).await,
        Command::VerifyQuorum(args) => {
            let started_ms = wall_clock_unix_ms();
            let nodes = load_nodes(&args.config).await?;
            let client = MetricsClient::new(Duration::from_secs(args.http_timeout_secs))?;
            let report =
                ursula_ctl::quorum::verify_quorum(&nodes, &client, &args.options()).await?;
            println!(
                "{}",
                serde_json::json!({
                    "started_ms": started_ms,
                    "completed_ms": wall_clock_unix_ms(),
                    "verification": report,
                })
            );
            Ok(())
        }
        Command::VerifySurvivors(args) => {
            let started_ms = wall_clock_unix_ms();
            let nodes = load_nodes(&args.quorum.config).await?;
            let client = MetricsClient::new(Duration::from_secs(args.quorum.http_timeout_secs))?;
            let report = ursula_ctl::quorum::verify_surviving_quorum(
                &nodes,
                args.excluded_node_id,
                &client,
                &args.quorum.options(),
            )
            .await?;
            println!(
                "{}",
                serde_json::json!({
                    "started_ms": started_ms,
                    "completed_ms": wall_clock_unix_ms(),
                    "surviving_quorum": report,
                })
            );
            Ok(())
        }
        Command::BackupCreate(args) => run_backup_create_subcommand(args).await,
        Command::BackupVerify(args) => run_backup_verify_subcommand(args).await,
        Command::Restore(args) => run_restore_subcommand(args).await,
    }
}

fn backup_client(nodes: &[NodeInfo], http_timeout_secs: u64) -> Result<backup::BackupClient> {
    Ok(backup::BackupClient::new(
        MetricsClient::new(Duration::from_secs(http_timeout_secs))?,
        nodes.to_vec(),
    )?)
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
        "restore complete: {} groups ({} imported by an earlier run), {} buckets, {} streams, \
         {} cold objects present; every restored stream answers with a new Stream-Incarnation",
        report.groups,
        report.already(),
        report.buckets,
        report.streams,
        report.cold_objects
    );
    for import in &report.imports {
        println!(
            "group {}: history {}{}",
            import.raft_group_id,
            import.history,
            if import.already {
                " (imported by an earlier run)"
            } else {
                ""
            }
        );
    }
    Ok(())
}

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
    let nodes = load_nodes(&args.config).await?;
    let client = MetricsClient::new(Duration::from_secs(args.http_timeout_secs))?;
    let (snapshot, view) = wait_ready(
        &client,
        &nodes,
        Duration::from_secs(args.timeout_secs),
        Duration::from_secs(args.poll_interval_secs),
    )
    .await?;
    println!(
        "ready: {} node(s), {} group(s) live on their target voters (meta leader: node {})",
        snapshot.per_node.len(),
        view.cluster.groups().len(),
        view.leader
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

async fn run_recover_group_subcommand(args: RecoverGroupArgs) -> Result<()> {
    let nodes = load_nodes(&args.config).await?;
    let client = MetricsClient::new(Duration::from_secs(args.http_timeout_secs))?;
    let options = ursula_ctl::recovery::RecoverOptions {
        max_plans: args.max_plans,
        settle_timeout: Duration::from_secs(args.settle_timeout_secs),
        poll_interval: Duration::from_secs(args.poll_interval_secs),
        accept_timeout: Duration::from_secs(args.accept_timeout_secs),
        accept_loss: args.accept_loss,
    };
    let target = args.target;
    match ursula_ctl::recovery::recover_group(&client, &nodes, target, &options).await? {
        ursula_ctl::recovery::RecoverOutcome::Healthy => {
            println!("{target}: it has a leader and no replica holds a plan");
        }
        ursula_ctl::recovery::RecoverOutcome::DryRun(explained) => {
            print_plan_explanation(target, &explained);
        }
        ursula_ctl::recovery::RecoverOutcome::Satisfied {
            plan,
            plans_sent,
            outside,
            stopped,
        } => {
            println!(
                "{target}: recovered with leader {} at point {} (cut: {}, plans sent: {plans_sent})",
                plan.leader, plan.point, plan.cut
            );
            if !outside.is_empty() {
                let readmission = match target {
                    ursula_ctl::recovery::RecoverTarget::Meta => {
                        "the meta leader adds a registered node back as a learner"
                    }
                    ursula_ctl::recovery::RecoverTarget::Group(_) => {
                        "the group's leader does not add a node back"
                    }
                };
                println!(
                    "{target}: nodes {outside:?} are outside the leader's membership: they answer reads \
                     of the group with 503 while they hold the plan; {readmission}, and one whose log \
                     is ahead of the plan's point must rejoin as a wiped node (operations guide, \
                     Rejoining a wiped node)"
                );
            }
            if !stopped.is_empty() {
                println!(
                    "{target}: nodes {stopped:?} report the group stopped and were not waited for"
                );
            }
        }
    }
    Ok(())
}

/// What `recover-group` would send, before `--accept-loss`.
fn print_plan_explanation(
    target: ursula_ctl::recovery::RecoverTarget,
    explained: &ursula_ctl::recovery::PlanExplanation,
) {
    let plan = explained.plan;
    println!(
        "{target}: would plan leader {} at point {} (cut: {}); nothing was sent",
        plan.leader, plan.point, plan.cut
    );
    let longest = explained
        .longest_last_log_term
        .map_or("an empty log".to_owned(), |term| {
            format!("last log term {term}")
        });
    if plan.cut {
        println!(
            "  cut: the longest voter log ends at {longest}, below the term floor {}, the highest \
             vote term a restricted replica holds, or its replica is no voter of its own \
             membership; every replica keeps only what it knew was committed",
            plan.floor
        );
    } else {
        println!(
            "  no cut: the longest voter log ends at {longest}, not below the term floor {}",
            plan.floor
        );
    }
    println!(
        "  plan term at least {}, the highest vote term read",
        plan.highest_term
    );
    for member in &explained.members {
        println!(
            "  node {}: gate {:?}, expected last log index {:?}, last log term {:?}, current term {}, committed {:?}, voters {:?}, voters after a cut {:?}",
            member.node_id,
            member.gate,
            member.log.last_log_index,
            member.log.last_log_term,
            member.log.current_term,
            member.committed,
            member.voter_configs,
            member.committed_voter_configs
        );
    }
    match explained.floor {
        ursula_proto::admin::PlanFloor::Meta => println!("  plan floor: meta"),
        ursula_proto::admin::PlanFloor::Seeded => println!("  plan floor: seeded"),
        ursula_proto::admin::PlanFloor::Certified(entry) => println!(
            "  plan floor: certified {entry}, the membership entry the group's last convergence \
             certified; every replica that holds it keeps it, and the plan's leader holds it"
        ),
    }
    if !explained.without_vote_floor.is_empty() {
        println!(
            "  replicas without a vote floor, which the plan neither counts nor is sent to: {:?}; \
             they rejoin through a live leader's barrier",
            explained.without_vote_floor
        );
    }
    if explained.unreached_voters.is_empty() {
        println!("  every voter answered");
    } else {
        println!(
            "  voters that did not answer, which the plan goes ahead without: {:?}",
            explained.unreached_voters
        );
    }
    println!(
        "  to accept the loss of writes acknowledged after the last fsync and send it, run again \
         with --accept-loss"
    );
}

async fn run_replace_disk_subcommand(args: ReplaceDiskArgs) -> Result<()> {
    use ursula_ctl::placement;
    let nodes = load_nodes(&args.config).await?;
    let client = MetricsClient::new(Duration::from_secs(args.http_timeout_secs))?;
    let before = placement::confirmed_slot(&client, &nodes, args.node).await?;
    println!(
        "node {}: slot {} (confirmed by node {})",
        args.node, before.slot, before.via.id
    );
    if !args.force {
        placement::check_replaceable(&client, &nodes, args.node, args.expect_root).await?;
    }
    println!("{}", placement::RELEASE_RULE);
    let record = placement::release_root(&client, &before.via, args.node, args.expect_root).await?;
    println!(
        "node {}: slot {}, released through node {}",
        args.node, record.root, before.via.id
    );
    Ok(())
}

async fn run_placement_subcommand(command: PlacementCommand) -> Result<()> {
    use ursula_ctl::placement::moves;
    use ursula_ctl::placement::status;
    match command {
        PlacementCommand::Status(args) => {
            let nodes = load_nodes(&args.config).await?;
            let client = MetricsClient::new(Duration::from_secs(args.http_timeout_secs))?;
            let filter = match (args.node, args.group) {
                (Some(node), _) => status::StatusFilter::Node(node.into()),
                (None, Some(group)) => status::StatusFilter::Group(RaftGroupId(group)),
                (None, None) => status::StatusFilter::Moving,
            };
            let report = status::placement_status(&client, &nodes, filter, args.confirmed).await?;
            let mut stdout = std::io::stdout().lock();
            if args.json {
                serde_json::to_writer_pretty(&mut stdout, &report)?;
                writeln!(stdout)?;
            } else {
                status::write_report(&mut stdout, &report)?;
            }
            Ok(())
        }
        PlacementCommand::Move(args) => {
            let intent = match args.replace {
                Some(replacement) => moves::Intent::Replace(replacement),
                None => moves::Intent::To(
                    ursula_proto::placement::VoterSet::new(
                        args.to.iter().copied().map(Into::into).collect(),
                    )
                    .context("--to names at least one node")?,
                ),
            };
            let groups = args.groups.iter().copied().map(RaftGroupId).collect();
            run_moves(
                &args.config,
                groups,
                intent,
                args.expect,
                args.retarget,
                args.allow_not_live,
                &args.wait,
            )
            .await
        }
        PlacementCommand::Cancel(args) => {
            let groups = vec![RaftGroupId(args.group)];
            run_moves(
                &args.config,
                groups,
                moves::Intent::Cancel,
                args.expect,
                false,
                false,
                &args.wait,
            )
            .await
        }
        PlacementCommand::SetMoveLimit(args) => {
            let nodes = load_nodes(&args.config).await?;
            let client = MetricsClient::new(Duration::from_secs(args.http_timeout_secs))?;
            let limit = ursula_control::MoveLimit::new(args.limit);
            let end = moves::set_move_limit(&client, &nodes, limit).await?;
            match &end {
                moves::MoveLimitEnd::Applied(applied) => {
                    println!("move limit: {}", applied.get());
                }
                moves::MoveLimitEnd::Resolved { current, .. } => {
                    let took = if end.took_effect() {
                        "took effect"
                    } else {
                        "did not take effect, send it again"
                    };
                    println!(
                        "the confirmed view shows move limit {}: the request {took}",
                        current.get()
                    );
                }
                moves::MoveLimitEnd::Refused(refusal) => println!("{refusal}"),
            }
            if !end.took_effect() {
                std::process::exit(2);
            }
            Ok(())
        }
    }
}

async fn run_moves(
    config: &std::path::Path,
    groups: Vec<RaftGroupId>,
    intent: ursula_ctl::placement::moves::Intent,
    expect: Option<ursula_proto::placement::PlacementEpoch>,
    retarget: bool,
    allow_not_live: bool,
    wait: &MoveWaitArgs,
) -> Result<()> {
    use ursula_ctl::placement::moves;
    let nodes = load_nodes(config).await?;
    let client = MetricsClient::new(Duration::from_secs(wait.http_timeout_secs))?;
    let options = moves::MoveOptions {
        groups,
        intent,
        expect,
        retarget,
        allow_not_live,
        wait: wait.wait,
        timeout: Duration::from_secs(wait.timeout_secs),
        poll_interval: Duration::from_secs(wait.poll_interval_secs),
    };
    let mut stdout = std::io::stdout().lock();
    let ends = moves::run_moves(&client, &nodes, &options, &mut stdout).await?;
    let failed: Vec<u32> = ends
        .iter()
        .filter(|(_, end)| end.is_failure())
        .map(|(group, _)| group.0)
        .collect();
    for (group, end) in &ends {
        if let moves::GroupEnd::TimedOut { waiting } = end {
            writeln!(stdout, "group {}: timed out, waiting {waiting:?}", group.0)?;
        }
    }
    if !failed.is_empty() {
        writeln!(
            stdout,
            "{} of {} groups did not move: {failed:?}",
            failed.len(),
            ends.len()
        )?;
        stdout.flush()?;
        std::process::exit(2);
    }
    Ok(())
}

fn lifecycle_options(
    nodes: &[u64],
    wait: bool,
    timeout_secs: u64,
    poll_interval_secs: u64,
) -> ursula_ctl::placement::lifecycle::LifecycleOptions {
    ursula_ctl::placement::lifecycle::LifecycleOptions {
        nodes: nodes
            .iter()
            .copied()
            .map(ursula_proto::placement::NodeId::new)
            .collect(),
        wait,
        timeout: Duration::from_secs(timeout_secs),
        poll_interval: Duration::from_secs(poll_interval_secs),
    }
}

/// `node cordon`, `uncordon`, `decommission`, `verify-removed`,
/// `verify-joinable` and `verify-vacant`. A refusal or a check that fails
/// exits 2, and `verify-vacant` of a node in a life exits
/// [`IN_LIFE_EXIT`].
async fn run_lifecycle_subcommand(command: NodeCommand) -> Result<()> {
    use ursula_control::LifecycleChange;
    use ursula_ctl::placement::lifecycle;
    let ended = match command {
        NodeCommand::ReplaceDisk(args) => return run_replace_disk_subcommand(args).await,
        NodeCommand::Cordon(args) => run_set_lifecycle(&args, LifecycleChange::Cordon).await,
        NodeCommand::Uncordon(args) => run_set_lifecycle(&args, LifecycleChange::Uncordon).await,
        NodeCommand::Decommission(args) => {
            let nodes = load_nodes(&args.config).await?;
            let client = MetricsClient::new(Duration::from_secs(args.http_timeout_secs))?;
            let options = lifecycle_options(
                &args.nodes,
                args.wait,
                args.timeout_secs,
                args.poll_interval_secs,
            );
            let mut stdout = std::io::stdout().lock();
            lifecycle::decommission(&client, &nodes, &options, &mut stdout).await
        }
        NodeCommand::VerifyRemoved(args) => {
            run_verify_removal(&args, lifecycle::Removal::Leaving).await
        }
        NodeCommand::VerifyJoinable(args) => {
            run_verify_removal(&args, lifecycle::Removal::Joining).await
        }
        NodeCommand::VerifyVacant(args) => {
            run_verify_removal(&args, lifecycle::Removal::Vacant).await
        }
    };
    let refused = |error: &anyhow::Error| {
        error
            .downcast_ref::<lifecycle::LifecycleRefusal>()
            .is_some()
            || error.downcast_ref::<lifecycle::RemovalRefusal>().is_some()
    };
    match ended {
        Ok(lifecycle::LifecycleEnd::Done) => Ok(()),
        Ok(lifecycle::LifecycleEnd::Leaving(leaving)) => {
            println!(
                "nodes {leaving:?} are decommissioning: the meta leader moves their groups away, \
                 `node verify-removed` passes once they are removed, and `node verify-joinable` \
                 once their removal completed and their ids are vacant"
            );
            Ok(())
        }
        Ok(lifecycle::LifecycleEnd::TimedOut(open)) => {
            println!("timed out, nodes {open:?} are not there yet");
            std::process::exit(2);
        }
        Err(error) if refused(&error) => {
            println!("refused: {error}");
            let in_life = matches!(
                error.downcast_ref::<lifecycle::RemovalRefusal>(),
                Some(lifecycle::RemovalRefusal::InLife { .. })
            );
            std::process::exit(if in_life { IN_LIFE_EXIT } else { 2 });
        }
        Err(error) => Err(error),
    }
}

/// The exit status of `node verify-vacant` for a node in a life, whose
/// disk is its own: the chart's scale-check keeps such a volume.
const IN_LIFE_EXIT: i32 = 3;

async fn run_set_lifecycle(
    args: &LifecycleArgs,
    change: ursula_control::LifecycleChange,
) -> Result<ursula_ctl::placement::lifecycle::LifecycleEnd> {
    let nodes = load_nodes(&args.config).await?;
    let client = MetricsClient::new(Duration::from_secs(args.http_timeout_secs))?;
    let options = lifecycle_options(
        &args.nodes,
        false,
        args.timeout_secs,
        args.poll_interval_secs,
    );
    let mut stdout = std::io::stdout().lock();
    ursula_ctl::placement::lifecycle::set_lifecycles(&client, &nodes, change, &options, &mut stdout)
        .await
}

async fn run_verify_removal(
    args: &VerifyRemovalArgs,
    removal: ursula_ctl::placement::lifecycle::Removal,
) -> Result<ursula_ctl::placement::lifecycle::LifecycleEnd> {
    let nodes = load_nodes(&args.config).await?;
    let client = MetricsClient::new(Duration::from_secs(args.http_timeout_secs))?;
    let ids = args
        .nodes
        .iter()
        .copied()
        .map(ursula_proto::placement::NodeId::new)
        .collect();
    let mut stdout = std::io::stdout().lock();
    ursula_ctl::placement::lifecycle::verify_removal(
        &client,
        &nodes,
        &ids,
        removal,
        Duration::from_secs(args.timeout_secs),
        Duration::from_secs(args.poll_interval_secs),
        &mut stdout,
    )
    .await?;
    let verified = match removal {
        ursula_ctl::placement::lifecycle::Removal::Leaving => "may leave",
        ursula_ctl::placement::lifecycle::Removal::Joining => "may start",
        ursula_ctl::placement::lifecycle::Removal::Vacant => "are vacant",
    };
    println!("verified: nodes {:?} {verified}", args.nodes);
    Ok(ursula_ctl::placement::lifecycle::LifecycleEnd::Done)
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
    println!(
        "cluster verified: {} node(s) ready, every group on its target voters",
        nodes.len()
    );
    Ok(())
}

#[cfg(test)]
mod cli_tests {
    use clap::Parser;

    /// Item 2 of the #612 review: `recover-group` sends nothing unless the
    /// operator accepts the loss.
    #[test]
    fn recover_group_sends_only_with_accept_loss() {
        let parse = |extra: &[&str]| {
            let mut args = vec![
                "ursulactl",
                "recover-group",
                "--config",
                "cluster.toml",
                "--target",
                "4",
            ];
            args.extend_from_slice(extra);
            match super::Cli::try_parse_from(args).expect("parses").command {
                super::Command::RecoverGroup(args) => args,
                other => panic!("{other:?}"),
            }
        };
        assert!(!parse(&[]).accept_loss);
        assert!(parse(&["--accept-loss"]).accept_loss);
        assert_eq!(
            parse(&[]).target,
            ursula_ctl::recovery::RecoverTarget::Group(4)
        );
        let removed = super::Cli::try_parse_from([
            "ursulactl",
            "recover-group",
            "--config",
            "cluster.toml",
            "--target",
            "4",
            "--dry-run",
        ])
        .expect_err("--dry-run is gone: a run without --accept-loss is the dry run");
        assert_eq!(removed.kind(), clap::error::ErrorKind::UnknownArgument);
    }

    /// The groups come from the meta leader's view, so no verb takes a
    /// group count.
    #[test]
    fn no_verb_takes_an_expected_group_count() {
        for verb in [&["wait-ready"][..], &["verify-quorum"], &[
            "verify-survivors",
            "--excluded-node-id",
            "3",
        ]] {
            let mut args = vec!["ursulactl"];
            args.extend_from_slice(verb);
            args.extend_from_slice(&["--config", "cluster.toml"]);
            super::Cli::try_parse_from(&args).unwrap_or_else(|error| panic!("{verb:?}: {error}"));
            args.extend_from_slice(&["--expected-groups", "4"]);
            let removed = super::Cli::try_parse_from(&args).expect_err("--expected-groups is gone");
            assert_eq!(
                removed.kind(),
                clap::error::ErrorKind::UnknownArgument,
                "{verb:?}"
            );
        }
    }
}

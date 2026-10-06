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
    /// Admit one fresh server boot against its live Kubernetes identities.
    StartupAdmit(StartupAdmitArgs),
    /// Produce a read-only manifest with fixed server-instance identities.
    PinIncarnations(PinIncarnationsArgs),
    /// Activate an already-admitted reservation token on every pinned process.
    /// Does not acquire a shared cell reservation.
    ActivateMaintenanceFence(ObserveArgs),
    /// Retire the same token on every pinned process before external release.
    RetireMaintenanceFence(ObserveArgs),
    /// Produce a whole-object reservation CAS proposal without contacting Kubernetes.
    ReservationPropose(ReservationProposeArgs),
    /// Capture immutable three-voter cell identity from complete API objects.
    ReservationCell(ReservationCellArgs),
    /// Capture a selected Pod/Node identity using its pinned process plan.
    ReservationSource(ReservationSourceArgs),
    /// Read one validated reservation snapshot without acquiring authority.
    ReservationRead(ReservationReadArgs),
    /// Emit an explicit new-cell store for reviewed create-only bootstrap.
    /// Never use this to recover a missing/deleted store for an existing cell.
    ReservationBootstrap(ReservationBootstrapArgs),
    /// Build a typed request from bounded JSON files, without acquiring authority.
    ReservationRequest(ReservationRequestArgs),
    /// Verify the exact original proposal's API receipt and emit its persistent state.
    /// Ownership alone never grants disruption or release.
    ReservationAcknowledge(ReservationAcknowledgeArgs),
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
    /// Capture fresh quorum prefixes and verify every configured replica.
    /// This observation does not reserve permission to disrupt a voter.
    VerifyQuorum(VerifyQuorumArgs),
    /// Verify fresh prefixes on two survivors without claiming restored
    /// redundancy or authorizing an unfenced physical replacement.
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
}

#[derive(Args, Debug)]
struct StartupAdmitArgs {
    #[arg(long)]
    node_id: u64,
    #[arg(long)]
    group_count: u32,
    #[arg(long)]
    core_count: u16,
    #[arg(long)]
    process_incarnation: String,
}

#[derive(Args, Debug)]
struct ReservationCellArgs {
    #[arg(long)]
    namespace_object: PathBuf,
    #[arg(long)]
    statefulset_object: PathBuf,
    #[arg(long)]
    group_count: u32,
    #[arg(long)]
    core_count: u16,
}

#[derive(Args, Debug)]
struct ReservationSourceArgs {
    #[arg(long)]
    cell: PathBuf,
    #[arg(long)]
    pod_object: PathBuf,
    #[arg(long)]
    node_object: PathBuf,
    #[arg(long)]
    config: PathBuf,
    #[arg(long)]
    node_id: u64,
    #[arg(long, value_enum, default_value = "source")]
    field: ReservationSourceField,
}

#[derive(clap::ValueEnum, Clone, Debug)]
enum ReservationSourceField {
    Source,
    PodUid,
}

#[derive(clap::ValueEnum, Clone, Debug)]
enum ReservationField {
    State,
    Hosts,
    OperationKind,
    HostRecovery,
    SourceNodeUid,
    SourceProviderInstance,
    SourceNodeName,
    Manifest,
    Stage,
    SourceNodeId,
    SourcePodUid,
    ReplacementPodUid,
    OperationId,
    Fence,
}

#[derive(Args, Debug)]
struct ReservationReadArgs {
    #[arg(long)]
    cell: PathBuf,
    #[arg(long)]
    snapshot: PathBuf,
    #[arg(long, value_enum, default_value = "state")]
    field: ReservationField,
    /// Project the two saved survivors only; valid exclusively for manifest.
    #[arg(long)]
    exclude_source: bool,
}

#[derive(Args, Debug)]
struct ReservationBootstrapArgs {
    #[arg(long)]
    cell: PathBuf,
    #[arg(long, required = true)]
    confirm_new_cell: bool,
}

#[derive(Args, Debug)]
struct ReservationRequestArgs {
    #[command(subcommand)]
    action: ReservationRequestAction,
}

#[derive(Subcommand, Debug)]
enum ReservationRequestAction {
    ReserveHostRecovery {
        #[arg(long)]
        operation_id: String,
        #[arg(long)]
        executor_id: String,
        #[arg(long)]
        node_id: u64,
        #[arg(long)]
        config: PathBuf,
    },
    AdmitHostTermination(ReservationObservationArgs),
    RecordHostTermination(ReservationObservationArgs),
    AdmitReplacementTermination(ReplacementObservationArgs),
    RecordReplacementTermination(ReplacementObservationArgs),
    RestageHostReplacement {
        #[arg(long)]
        fence: PathBuf,
        #[arg(long)]
        candidate: PathBuf,
    },
    CompleteHostReplacement(ReservationObservationArgs),
    AdmitFencedPodRetirement {
        #[arg(long)]
        fence: PathBuf,
        #[arg(long)]
        pod_object: Option<PathBuf>,
        #[arg(long)]
        node_object: Option<PathBuf>,
    },
    BindHostReplacement {
        #[arg(long)]
        fence: PathBuf,
        #[arg(long)]
        pod_object: PathBuf,
        #[arg(long)]
        node_object: PathBuf,
        #[arg(long)]
        config: PathBuf,
    },
    PublishHostInventory {
        #[arg(long)]
        pods: PathBuf,
        #[arg(long)]
        nodes: PathBuf,
        #[arg(long)]
        config: PathBuf,
        #[arg(long)]
        observation: PathBuf,
    },
    Reserve {
        #[arg(long)]
        operation_id: String,
        #[arg(long)]
        executor_id: String,
        #[arg(long)]
        source: PathBuf,
        #[arg(long)]
        config: PathBuf,
    },
    Takeover {
        #[arg(long)]
        operation_id: String,
        #[arg(long)]
        executor_id: String,
    },
    AdmitPodDeletion(ReservationObservationArgs),
    CompletePodReplacement(ReservationObservationArgs),
    BindPodReplacement {
        #[arg(long)]
        fence: PathBuf,
        #[arg(long)]
        pod_object: PathBuf,
        #[arg(long)]
        node_object: PathBuf,
        #[arg(long)]
        config: PathBuf,
    },
}

#[derive(Args, Debug)]
struct ReplacementObservationArgs {
    #[command(flatten)]
    observation: ReservationObservationArgs,
    #[arg(long)]
    candidate: PathBuf,
}

#[derive(Args, Debug)]
struct ReservationObservationArgs {
    #[arg(long)]
    fence: PathBuf,
    #[arg(long)]
    observation: PathBuf,
}

#[derive(Args, Debug)]
struct ReservationProposeArgs {
    /// Expected immutable cell identity, as JSON.
    #[arg(long)]
    cell: PathBuf,
    /// One complete ConfigMap GET response, including UID and resourceVersion.
    #[arg(long)]
    snapshot: PathBuf,
    /// Explicit ownership or Pod-replacement progress request, as JSON.
    #[arg(long)]
    request: PathBuf,
}

#[derive(Args, Debug)]
struct ReservationAcknowledgeArgs {
    #[command(flatten)]
    proposal: ReservationProposeArgs,
    /// Successful API update response; a proposal/dry-run/conflict is not a receipt.
    #[arg(long)]
    response: PathBuf,
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
struct PinIncarnationsArgs {
    #[arg(long, value_name = "PATH")]
    config: PathBuf,
    /// Bind only this explicitly admitted replacement to its new identity;
    /// every other voter must still match the saved manifest.
    #[arg(long)]
    replace_node: Option<u64>,
    /// Migration diagnostic for deployed 0.6.2 and earlier sources only.
    /// Missing identities remain explicitly uncertified in the output.
    #[arg(long)]
    allow_legacy_incarnation: bool,
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

#[derive(Args, Debug)]
struct VerifyQuorumArgs {
    #[arg(long, value_name = "PATH")]
    config: PathBuf,
    #[arg(long)]
    expected_groups: u32,
    #[arg(long)]
    core_count: u16,
    #[arg(long, default_value_t = 120)]
    timeout_secs: u64,
    #[arg(long, default_value_t = 1)]
    poll_interval_secs: u64,
    #[arg(long, default_value_t = 10)]
    http_timeout_secs: u64,
    /// Only for diagnostic measurements of the pinned 0.6.2 baseline;
    /// output explicitly reports participation_certified=false.
    #[arg(long)]
    allow_legacy_eligibility: bool,
}

impl VerifyQuorumArgs {
    fn options(&self) -> ursula_ctl::quorum::QuorumVerificationOptions {
        ursula_ctl::quorum::QuorumVerificationOptions {
            group_count: self.expected_groups,
            core_count: self.core_count,
            timeout: Duration::from_secs(self.timeout_secs),
            poll_interval: Duration::from_secs(self.poll_interval_secs),
            allow_legacy_eligibility: self.allow_legacy_eligibility,
        }
    }
}

#[derive(Args, Debug)]
struct VerifySurvivorsArgs {
    #[command(flatten)]
    quorum: VerifyQuorumArgs,
    /// Exactly one configured voter to omit from observation. This option
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
        Command::StartupAdmit(args) => {
            let boot = ursula_proto::admin::ProcessIncarnation::try_from(args.process_incarnation)
                .map_err(anyhow::Error::msg)?;
            let identity = ursula_ctl::startup::StartupIdentity::from_environment(
                args.node_id,
                args.group_count,
                args.core_count,
                boot,
            )?;
            println!(
                "{}",
                serde_json::to_string(&ursula_ctl::startup::admit_in_cluster(identity).await?)?
            );
            Ok(())
        }
        Command::Status(args) => run_status_subcommand(args).await,
        Command::PinIncarnations(args) => {
            let nodes = load_nodes(&args.config).await?;
            let client = MetricsClient::new(Duration::from_secs(args.http_timeout_secs))?;
            let pinned = client
                .pin_nodes(&nodes, args.replace_node, args.allow_legacy_incarnation)
                .await?;
            println!(
                "{}",
                serde_json::json!({"process_incarnations_certified": pinned.iter().all(|node| node.expected_process_incarnation.is_some()), "nodes": pinned})
            );
            Ok(())
        }
        Command::ActivateMaintenanceFence(args) => {
            run_maintenance_fence_subcommand(args, false).await
        }
        Command::RetireMaintenanceFence(args) => run_maintenance_fence_subcommand(args, true).await,
        Command::ReservationCell(args) => {
            let namespace = read_reservation_json(&args.namespace_object)?;
            let statefulset = read_reservation_json(&args.statefulset_object)?;
            let cell = ursula_ctl::reservation::CellIdentity::capture(
                &namespace,
                &statefulset,
                args.group_count,
                args.core_count,
            )?;
            println!("{}", serde_json::to_string(&cell)?);
            Ok(())
        }
        Command::ReservationSource(args) => {
            let cell = read_reservation_json(&args.cell)?;
            let pod = read_reservation_json(&args.pod_object)?;
            let node = read_reservation_json(&args.node_object)?;
            let plan = load_nodes(&args.config).await?;
            let source = ursula_ctl::reservation::SourceIdentity::capture(
                &cell,
                args.node_id,
                &pod,
                &node,
                &plan,
            )?;
            match args.field {
                ReservationSourceField::Source => println!("{}", serde_json::to_string(&source)?),
                ReservationSourceField::PodUid => println!("{}", source.pod_uid),
            }
            Ok(())
        }
        Command::ReservationRead(args) => {
            if args.exclude_source && !matches!(args.field, ReservationField::Manifest) {
                bail!("exclude-source is only valid for the manifest projection");
            }
            let cell = read_reservation_json(&args.cell)?;
            let document = read_reservation_json(&args.snapshot)?;
            let snapshot = ursula_ctl::reservation::ConfigMapSnapshot::parse(document, &cell)?;
            let state = snapshot.state();
            if matches!(args.field, ReservationField::State) {
                println!("{}", serde_json::to_string(state)?);
                return Ok(());
            }
            if matches!(args.field, ReservationField::Hosts) {
                println!(
                    "{}",
                    serde_json::to_string(
                        state
                            .hosts()
                            .context("pre-fault host inventory has not been published")?
                    )?
                );
                return Ok(());
            }
            if matches!(args.field, ReservationField::Stage) {
                let stage = match state.operation() {
                    None => "idle",
                    Some(operation) if operation.host.is_some() => operation
                        .host
                        .as_ref()
                        .context("missing host recovery")?
                        .stage(),
                    Some(operation) if operation.replacement.is_some() => "replacement-bound",
                    Some(operation) if operation.admission.is_some() => "deletion-admitted",
                    Some(_) => "reserved",
                };
                println!("{stage}");
                return Ok(());
            }
            if matches!(args.field, ReservationField::OperationKind) {
                println!("{}", match state.operation() {
                    None => "idle",
                    Some(operation) if operation.host.is_some() => "host-recovery",
                    Some(_) => "pod-replacement",
                });
                return Ok(());
            }
            let operation = state.operation().context("no active source reservation")?;
            match args.field {
                ReservationField::Manifest => {
                    let nodes = operation
                        .process_plan
                        .iter()
                        .filter(|node| !args.exclude_source || node.id != operation.source.node_id)
                        .collect::<Vec<_>>();
                    println!("{}", serde_json::json!({"nodes": nodes}))
                }
                ReservationField::SourceNodeId => println!("{}", operation.source.node_id),
                ReservationField::SourceNodeUid => println!("{}", operation.source.node_uid),
                ReservationField::SourceProviderInstance => {
                    println!("{}", operation.source.provider_instance)
                }
                ReservationField::SourceNodeName => println!(
                    "{}",
                    operation
                        .host
                        .as_ref()
                        .context("not a host recovery")?
                        .source_host
                        .node_name
                ),
                ReservationField::HostRecovery => println!(
                    "{}",
                    serde_json::to_string(operation.host.as_ref().context("not a host recovery")?)?
                ),
                ReservationField::SourcePodUid => println!("{}", operation.source.pod_uid),
                ReservationField::ReplacementPodUid => println!(
                    "{}",
                    operation
                        .replacement
                        .as_ref()
                        .context("replacement not bound")?
                        .pod_uid
                ),
                ReservationField::OperationId => println!("{}", operation.fence.reservation_id()),
                ReservationField::Fence => println!("{}", serde_json::to_string(&operation.fence)?),
                ReservationField::State
                | ReservationField::Stage
                | ReservationField::Hosts
                | ReservationField::OperationKind => {
                    bail!("unexpected reservation projection")
                }
            }
            Ok(())
        }
        Command::ReservationBootstrap(args) => {
            if !args.confirm_new_cell {
                bail!("new-cell bootstrap requires explicit confirmation");
            }
            let cell: ursula_ctl::reservation::CellIdentity = read_reservation_json(&args.cell)?;
            let state = ursula_ctl::reservation::Reservation::initial(cell.clone())?;
            println!(
                "{}",
                serde_json::json!({
                    "apiVersion": "v1", "kind": "ConfigMap",
                    "metadata": {"namespace": cell.namespace,
                        "name": format!("{}-maintenance", cell.statefulset)},
                    "data": {"reservation": serde_json::to_string(&state)?},
                })
            );
            Ok(())
        }
        Command::ReservationRequest(args) => run_reservation_request(args).await,
        Command::ReservationPropose(args) => {
            let (_, proposal) = reservation_proposal(&args)?;
            println!("{}", serde_json::to_string(proposal.document())?);
            Ok(())
        }
        Command::ReservationAcknowledge(args) => {
            let (snapshot, proposal) = reservation_proposal(&args.proposal)?;
            let response = read_reservation_json(&args.response)?;
            let acknowledged = snapshot.acknowledge(&proposal, response)?;
            let state = acknowledged.state();
            let operation = state.operation();
            println!(
                "{}",
                serde_json::json!({
                    "nodes": operation.map(|operation| &operation.process_plan),
                    "reservation": state,
                    "reservation_proposal_acknowledged": true,
                    "disruption_authorized": false,
                    "physical_hosts_fenced": false,
                })
            );
            Ok(())
        }
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
                survivor_observation_json(started_ms, wall_clock_unix_ms(), report)?
            );
            Ok(())
        }
        Command::BackupCreate(args) => run_backup_create_subcommand(args).await,
        Command::BackupVerify(args) => run_backup_verify_subcommand(args).await,
        Command::Restore(args) => run_restore_subcommand(args).await,
    }
}

fn survivor_observation_json(
    started_ms: u64,
    completed_ms: u64,
    verification: ursula_ctl::quorum::SurvivingQuorumVerification,
) -> Result<String> {
    Ok(serde_json::to_string(
        &ursula_ctl::reservation::SurvivingPrefixObservation {
            started_ms,
            completed_ms,
            verification,
        },
    )?)
}

fn backup_client(nodes: &[NodeInfo], http_timeout_secs: u64) -> Result<backup::BackupClient> {
    backup::BackupClient::new(
        MetricsClient::new(Duration::from_secs(http_timeout_secs))?,
        nodes.to_vec(),
    )
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

/// Read one bounded JSON input without invoking any external operation.
fn read_reservation_json<T: serde::de::DeserializeOwned>(path: &std::path::Path) -> Result<T> {
    use std::io::Read;
    let file = std::fs::File::open(path)
        .with_context(|| format!("open reservation JSON {}", path.display()))?;
    let mut bytes = Vec::new();
    file.take(2_097_153).read_to_end(&mut bytes)?;
    if bytes.len() > 2_097_152 {
        bail!("reservation JSON exceeds the 2 MiB transport bound");
    }
    serde_json::from_slice(&bytes)
        .with_context(|| format!("parse reservation JSON {}", path.display()))
}

async fn run_reservation_request(args: ReservationRequestArgs) -> Result<()> {
    use ursula_ctl::reservation::HostRequest;
    use ursula_ctl::reservation::OwnershipRequest;
    use ursula_ctl::reservation::ProgressRequest;
    use ursula_ctl::reservation::ReservationRequest;
    let now_ms = wall_clock_unix_ms();
    let request = match args.action {
        ReservationRequestAction::ReserveHostRecovery {
            operation_id,
            executor_id,
            node_id,
            config,
        } => ReservationRequest::Host(HostRequest::ReserveHostRecovery {
            operation_id,
            executor_id,
            node_id,
            process_plan: load_nodes(&config).await?,
            now_ms,
        }),
        ReservationRequestAction::AdmitHostTermination(args) => {
            ReservationRequest::Host(HostRequest::AdmitHostTermination {
                fence: read_reservation_json(&args.fence)?,
                now_ms,
                observation: read_reservation_json(&args.observation)?,
            })
        }
        ReservationRequestAction::RecordHostTermination(args) => {
            ReservationRequest::Host(HostRequest::RecordHostTermination {
                fence: read_reservation_json(&args.fence)?,
                now_ms,
                observation: read_reservation_json(&args.observation)?,
            })
        }
        ReservationRequestAction::AdmitReplacementTermination(args) => {
            ReservationRequest::Host(HostRequest::AdmitReplacementTermination {
                fence: read_reservation_json(&args.observation.fence)?,
                candidate: read_reservation_json(&args.candidate)?,
                now_ms,
                observation: read_reservation_json(&args.observation.observation)?,
            })
        }
        ReservationRequestAction::RecordReplacementTermination(args) => {
            ReservationRequest::Host(HostRequest::RecordReplacementTermination {
                fence: read_reservation_json(&args.observation.fence)?,
                candidate: read_reservation_json(&args.candidate)?,
                now_ms,
                observation: read_reservation_json(&args.observation.observation)?,
            })
        }
        ReservationRequestAction::RestageHostReplacement { fence, candidate } => {
            ReservationRequest::Host(HostRequest::RestageHostReplacement {
                fence: read_reservation_json(&fence)?,
                candidate: read_reservation_json(&candidate)?,
                now_ms,
            })
        }
        ReservationRequestAction::CompleteHostReplacement(args) => {
            ReservationRequest::Host(HostRequest::CompleteHostReplacement {
                fence: read_reservation_json(&args.fence)?,
                now_ms,
                observation: read_reservation_json(&args.observation)?,
            })
        }
        ReservationRequestAction::AdmitFencedPodRetirement {
            fence,
            pod_object,
            node_object,
        } => ReservationRequest::Host(HostRequest::AdmitFencedPodRetirement {
            fence: read_reservation_json(&fence)?,
            pod: pod_object
                .as_deref()
                .map(read_reservation_json)
                .transpose()?,
            node: node_object
                .as_deref()
                .map(read_reservation_json)
                .transpose()?,
        }),
        ReservationRequestAction::BindHostReplacement {
            fence,
            pod_object,
            node_object,
            config,
        } => ReservationRequest::Host(HostRequest::BindHostReplacement {
            fence: read_reservation_json(&fence)?,
            pod: read_reservation_json(&pod_object)?,
            node: read_reservation_json(&node_object)?,
            process_plan: load_nodes(&config).await?,
        }),
        ReservationRequestAction::PublishHostInventory {
            pods,
            nodes,
            config,
            observation,
        } => {
            let objects = |path: &std::path::Path| -> Result<Vec<serde_json::Value>> {
                let value: serde_json::Value = read_reservation_json(path)?;
                value
                    .get("items")
                    .and_then(serde_json::Value::as_array)
                    .cloned()
                    .context("inventory input must be a Kubernetes List with complete objects")
            };
            ReservationRequest::Inventory(ursula_ctl::reservation::PublishHostInventory {
                now_ms,
                pods: objects(&pods)?,
                nodes: objects(&nodes)?,
                process_plan: load_nodes(&config).await?,
                observation: read_reservation_json(&observation)?,
            })
        }
        ReservationRequestAction::Reserve {
            operation_id,
            executor_id,
            source,
            config,
        } => ReservationRequest::Ownership(OwnershipRequest::Reserve {
            operation_id,
            executor_id,
            source: read_reservation_json(&source)?,
            process_plan: load_nodes(&config).await?,
            now_ms,
        }),
        ReservationRequestAction::Takeover {
            operation_id,
            executor_id,
        } => ReservationRequest::Ownership(OwnershipRequest::Takeover {
            operation_id,
            executor_id,
            now_ms,
        }),
        ReservationRequestAction::AdmitPodDeletion(args) => {
            ReservationRequest::Progress(ProgressRequest::AdmitPodDeletion {
                fence: read_reservation_json(&args.fence)?,
                now_ms,
                observation: read_reservation_json(&args.observation)?,
            })
        }
        ReservationRequestAction::CompletePodReplacement(args) => {
            ReservationRequest::Progress(ProgressRequest::CompletePodReplacement {
                fence: read_reservation_json(&args.fence)?,
                now_ms,
                observation: read_reservation_json(&args.observation)?,
            })
        }
        ReservationRequestAction::BindPodReplacement {
            fence,
            pod_object,
            node_object,
            config,
        } => ReservationRequest::Progress(ProgressRequest::BindPodReplacement {
            fence: read_reservation_json(&fence)?,
            pod: read_reservation_json(&pod_object)?,
            node: read_reservation_json(&node_object)?,
            process_plan: load_nodes(&config).await?,
        }),
    };
    println!("{}", serde_json::to_string(&request)?);
    Ok(())
}

fn reservation_proposal(
    args: &ReservationProposeArgs,
) -> Result<(
    ursula_ctl::reservation::ConfigMapSnapshot,
    ursula_ctl::reservation::CasProposal,
)> {
    let cell = read_reservation_json(&args.cell)?;
    let document = read_reservation_json(&args.snapshot)?;
    let request = read_reservation_json(&args.request)?;
    let snapshot = ursula_ctl::reservation::ConfigMapSnapshot::parse(document, &cell)?;
    let proposal = snapshot.transition(request)?;
    Ok((snapshot, proposal))
}

async fn run_maintenance_fence_subcommand(args: ObserveArgs, retire: bool) -> Result<()> {
    let nodes = load_nodes(&args.config).await?;
    if nodes.is_empty()
        || nodes.iter().any(|node| {
            node.expected_maintenance_fence.is_none() || node.expected_process_incarnation.is_none()
        })
    {
        bail!(
            "maintenance lifecycle requires a token and process identity for every configured voter"
        );
    }
    let client = MetricsClient::new(Duration::from_secs(args.http_timeout_secs))?;
    let mut acknowledged = std::collections::BTreeMap::new();
    for node in &nodes {
        let state = client.set_maintenance_fence(node, retire).await?;
        acknowledged.insert(node.id, state);
    }
    println!(
        "{}",
        serde_json::json!({"nodes": acknowledged, "cell_reservation_acquired": false, "physical_hosts_fenced": false})
    );
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

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use clap::Parser;

    use super::Cli;
    use super::Command;
    use super::run_reservation_request;
    use super::survivor_observation_json;

    #[tokio::test]
    async fn survivor_output_feeds_both_native_admission_builders_without_rewriting() {
        let directory = tempfile::tempdir().unwrap();
        let fence = ursula_proto::admin::MaintenanceFence::new(
            format!("{:032x}", 1),
            format!("{:032x}", 2),
            1,
        )
        .unwrap();
        let verification = ursula_ctl::quorum::QuorumVerification {
            version: 3,
            participation_certified: true,
            process_incarnations_certified: true,
            process_incarnations: (2..=3)
                .map(|id| {
                    (
                        id,
                        ursula_proto::admin::ProcessIncarnation::from_bits(u128::from(id)),
                    )
                })
                .collect(),
            maintenance_executor_certified: true,
            maintenance_executor_retired_certified: false,
            maintenance_fence: Some(fence.clone()),
            prefixes: BTreeMap::from([(0, ursula_raft::QuorumPrefix {
                raft_group_id: 0,
                leader_id: 2,
                leader_term: 7,
                required_applied_index: 20,
            })]),
            applied: (2..=3)
                .map(|id| (id, BTreeMap::from([(0, 20)])))
                .collect(),
        };
        let encoded = survivor_observation_json(
            100,
            101,
            ursula_ctl::quorum::SurvivingQuorumVerification {
                excluded_voter_id: 1,
                configured_voter_ids: [1, 2, 3].into_iter().collect(),
                surviving_voter_ids: [2, 3].into_iter().collect(),
                full_redundancy_restored: false,
                verification,
            },
        )
        .unwrap();
        let output: serde_json::Value = serde_json::from_str(&encoded).unwrap();
        assert_eq!(output["verification"]["excluded_voter_id"], 1);
        assert_eq!(
            output["verification"]["surviving_voter_ids"],
            serde_json::json!([2, 3])
        );
        assert_eq!(output["verification"]["full_redundancy_restored"], false);
        assert!(
            serde_json::from_value::<ursula_ctl::reservation::PrefixObservation>(output).is_err()
        );
        let observation_path = directory.path().join("observation.json");
        std::fs::write(&observation_path, encoded).unwrap();
        let fence_path = directory.path().join("fence.json");
        std::fs::write(&fence_path, serde_json::to_vec(&fence).unwrap()).unwrap();
        let candidate_path = directory.path().join("candidate.json");
        let candidate = ursula_ctl::reservation::SourceIdentity {
            node_id: 1,
            pod_name: "voters-0".into(),
            pod_uid: "candidate-pod".into(),
            node_uid: "candidate-node".into(),
            provider_instance: "candidate-provider".into(),
            process_incarnation: ursula_proto::admin::ProcessIncarnation::from_bits(11),
        };
        std::fs::write(&candidate_path, serde_json::to_vec(&candidate).unwrap()).unwrap();
        for action in ["admit-host-termination", "admit-replacement-termination"] {
            let mut arguments = vec![
                "ursulactl".to_owned(),
                "reservation-request".to_owned(),
                action.to_owned(),
                "--fence".to_owned(),
                fence_path.display().to_string(),
                "--observation".to_owned(),
                observation_path.display().to_string(),
            ];
            if action == "admit-replacement-termination" {
                arguments.extend([
                    "--candidate".to_owned(),
                    candidate_path.display().to_string(),
                ]);
            }
            let Command::ReservationRequest(args) = Cli::try_parse_from(arguments).unwrap().command
            else {
                panic!("expected native reservation builder");
            };
            run_reservation_request(args).await.unwrap();
        }
    }
}

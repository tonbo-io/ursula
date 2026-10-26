//! `ursula-sim smoke`: corpus replays plus a seed sweep with failure
//! artifacts. `--shard INDEX/COUNT` runs one share of the sweep. The sweep
//! runs every seed of its share and then fails listing each failed seed.

use std::collections::BTreeSet;
use std::error::Error;
use std::fmt;
use std::fs;
use std::num::NonZeroUsize;
use std::num::ParseIntError;
use std::panic;
use std::path::Path;
use std::path::PathBuf;

use ursula_sim::SIM_REGRESSION_SCHEMA_VERSION;
use ursula_sim::SimFailureRegressionRecord;
use ursula_sim::SimRegressionRecord;
use ursula_sim::SimScenario;
use ursula_sim::SimSchedule;
use ursula_sim::SimScheduledRecord;
use ursula_sim::SimTrace;
use ursula_sim::artifact::FailedSeedArtifact;
use ursula_sim::artifact::RawEventLogArtifact;
use ursula_sim::artifact::StableTraceArtifact;
use ursula_sim::artifact::panic_payload_to_string;

pub fn run(args: Vec<String>) -> Result<(), Box<dyn Error>> {
    let args = Args::parse(args)?;
    let shard = args.shard.unwrap_or(Shard::WHOLE);

    // The corpora replay once per sweep: in its first share.
    if shard.index == 0 {
        replay_corpora()?;
    }

    let failure_dir = args
        .failure_dir
        .unwrap_or_else(|| PathBuf::from("target/ursula-sim-failures"));
    fs::create_dir_all(&failure_dir)?;

    let listed = args.seeds.len();
    // Taken from the whole list, so every share of one command names a
    // run's artifacts alike.
    let shared = seeds_under_several_generators(&args.seeds);
    let seeds = shard.select(args.seeds);
    if args.shard.is_some() {
        println!("shard {shard}: {} of {listed} seeds", seeds.len());
    }

    // Every seed runs: a failure is recorded and the sweep goes on, so one
    // failing seed cannot hide the verdict of the seeds after it.
    let mut failures = Vec::new();
    for schedule_seed in seeds {
        let seed = schedule_seed.seed;
        let stem = schedule_seed.artifact_stem(&shared);
        let mut schedule = schedule_seed.generate_schedule();
        apply_runtime_panic_after(
            &mut schedule,
            args.runtime_panic_after.as_ref(),
            args.panic_message.as_ref(),
            args.runtime_invariant.as_ref(),
        )?;
        apply_runtime_corrupt_read_client(&mut schedule, args.runtime_corrupt_read_client)?;
        apply_cold_corrupt_read_node(&mut schedule, args.cold_corrupt_read_node)?;
        let result = panic::catch_unwind(|| {
            let report = schedule.run();
            let record = SimScheduledRecord::new(schedule.clone(), report);
            if args.inject_panic_seed == Some(seed) {
                panic!("injected sim smoke panic for seed {seed}");
            }
            record
        });
        match result {
            Ok(record) => {
                if args.write_artifacts {
                    write_success_artifacts(&failure_dir, &stem, &record)?;
                }
            }
            Err(payload) => {
                let failure = write_failure_artifacts(
                    &failure_dir,
                    &stem,
                    schedule_seed,
                    schedule,
                    panic_payload_to_string(payload),
                )?;
                println!("sim {failure}");
                failures.push(failure);
            }
        }
    }

    Ok(sweep_verdict(failures, args.expect_failures)?)
}

/// A seed the sweep saw fail and the artifacts it wrote for it.
#[derive(Debug)]
struct FailedSeed {
    seed: u64,
    kind: &'static str,
    summary_path: PathBuf,
    stable_path: PathBuf,
    raw_path: PathBuf,
}

impl fmt::Display for FailedSeed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "seed {} ({}) failed; wrote {}, {}, {}",
            self.seed,
            self.kind,
            self.summary_path.display(),
            self.stable_path.display(),
            self.raw_path.display()
        )
    }
}

/// Why a sweep that ran every seed of its share fails.
#[derive(Debug, thiserror::Error)]
enum SweepError {
    #[error("{}", seeds_failed_message(.failures))]
    SeedsFailed { failures: Vec<FailedSeed> },
    #[error("--expect-failures was set but no seed failed")]
    NoSeedFailed,
}

fn seeds_failed_message(failures: &[FailedSeed]) -> String {
    let mut message = format!("{} sim seeds failed:", failures.len());
    for failure in failures {
        message.push_str("\n  ");
        message.push_str(&failure.to_string());
    }
    message
}

/// The verdict of a sweep over the failures of all its seeds: without
/// `--expect-failures` any failure fails it, with the flag none does.
fn sweep_verdict(failures: Vec<FailedSeed>, expect_failures: bool) -> Result<(), SweepError> {
    match (expect_failures, failures.is_empty()) {
        (false, true) | (true, false) => Ok(()),
        (false, false) => Err(SweepError::SeedsFailed { failures }),
        (true, true) => Err(SweepError::NoSeedFailed),
    }
}

/// The seed numbers that the sorted, deduplicated list names under more
/// than one generator.
fn seeds_under_several_generators(seeds: &[ScheduleSeed]) -> BTreeSet<u64> {
    seeds
        .windows(2)
        .filter_map(|pair| match pair {
            [first, second] if first.seed == second.seed => Some(first.seed),
            _ => None,
        })
        .collect()
}

fn write_failure_artifacts(
    failure_dir: &Path,
    stem: &str,
    schedule_seed: ScheduleSeed,
    schedule: SimSchedule,
    panic: String,
) -> Result<FailedSeed, Box<dyn Error>> {
    let seed = schedule_seed.seed;
    let raw_event_log = SimTrace::last_recorded().events;
    let stable_trace = SimTrace {
        events: raw_event_log.clone(),
    }
    .stable_replay();
    let stable_artifact = StableTraceArtifact {
        schema_version: SIM_REGRESSION_SCHEMA_VERSION,
        schedule: schedule.clone(),
        stable_trace,
    };
    let stable_path = failure_dir.join(format!("{stem}-stable-trace.json"));
    let raw_path = failure_dir.join(format!("{stem}-raw-events.json"));
    let summary_path = failure_dir.join(format!("{stem}-failure.json"));

    let mut stable_body = serde_json::to_string_pretty(&stable_artifact)?;
    stable_body.push('\n');
    fs::write(&stable_path, stable_body)?;

    let mut raw_body = serde_json::to_string_pretty(&RawEventLogArtifact {
        schema_version: SIM_REGRESSION_SCHEMA_VERSION,
        seed,
        events: raw_event_log,
    })?;
    raw_body.push('\n');
    fs::write(&raw_path, raw_body)?;

    let artifact = FailedSeedArtifact {
        schema_version: SIM_REGRESSION_SCHEMA_VERSION,
        seed,
        schedule,
        stable_trace_path: stable_path.display().to_string(),
        raw_event_log_path: raw_path.display().to_string(),
        panic,
    };
    let mut body = serde_json::to_string_pretty(&artifact)?;
    body.push('\n');
    fs::write(&summary_path, body)?;
    Ok(FailedSeed {
        seed,
        kind: schedule_seed.kind,
        summary_path,
        stable_path,
        raw_path,
    })
}

/// The three checked-in corpora.
fn replay_corpora() -> Result<(), Box<dyn Error>> {
    let regression_corpus = include_str!("../../../corpus/smoke.json");
    let regression_records = serde_json::from_str::<Vec<SimRegressionRecord>>(regression_corpus)?;
    for record in regression_records {
        record.assert_replays();
    }

    let schedule_corpus = include_str!("../../../corpus/schedule-smoke.json");
    let schedule_records = serde_json::from_str::<Vec<SimScheduledRecord>>(schedule_corpus)?;
    for record in schedule_records {
        assert_eq!(record.schedule, SimSchedule::generate(record.schedule.seed));
        record.assert_replays();
    }

    let failure_corpus = include_str!("../../../corpus/failure-smoke.json");
    let failure_records = serde_json::from_str::<Vec<SimFailureRegressionRecord>>(failure_corpus)?;
    for record in failure_records {
        record.assert_replays();
    }
    Ok(())
}

/// One share of a sweep: the seeds at positions INDEX, INDEX + COUNT, ...
/// of its sorted, deduplicated list. The COUNT shares of one command run
/// each of its seeds once.
#[derive(Clone, Copy)]
struct Shard {
    index: usize,
    count: NonZeroUsize,
}

impl Shard {
    const WHOLE: Self = Self {
        index: 0,
        count: NonZeroUsize::MIN,
    };

    fn parse(text: &str) -> Result<Self, ShardError> {
        let (index, count) = text.split_once('/').ok_or_else(|| ShardError::Shape {
            text: text.to_owned(),
        })?;
        let index = index.parse::<usize>().map_err(|source| ShardError::Index {
            text: text.to_owned(),
            source,
        })?;
        let count = count
            .parse::<NonZeroUsize>()
            .map_err(|source| ShardError::Count {
                text: text.to_owned(),
                source,
            })?;
        if index >= count.get() {
            return Err(ShardError::IndexNotBelowCount { index, count });
        }
        Ok(Self { index, count })
    }

    fn select(self, seeds: Vec<ScheduleSeed>) -> Vec<ScheduleSeed> {
        seeds
            .into_iter()
            .skip(self.index)
            .step_by(self.count.get())
            .collect()
    }
}

/// Why `--shard` refuses its value.
#[derive(Debug, thiserror::Error)]
enum ShardError {
    #[error("--shard takes INDEX/COUNT, got `{text}`")]
    Shape { text: String },
    #[error("--shard INDEX in `{text}` is not a number")]
    Index {
        text: String,
        #[source]
        source: ParseIntError,
    },
    #[error("--shard COUNT in `{text}` is not a positive number")]
    Count {
        text: String,
        #[source]
        source: ParseIntError,
    },
    #[error("--shard index {index} is not below its count {count}")]
    IndexNotBelowCount { index: usize, count: NonZeroUsize },
}

impl fmt::Display for Shard {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}", self.index, self.count)
    }
}

struct Args {
    seeds: Vec<ScheduleSeed>,
    shard: Option<Shard>,
    failure_dir: Option<PathBuf>,
    inject_panic_seed: Option<u64>,
    runtime_panic_after: Option<String>,
    panic_message: Option<String>,
    runtime_invariant: Option<String>,
    runtime_corrupt_read_client: Option<usize>,
    cold_corrupt_read_node: Option<u64>,
    write_artifacts: bool,
    expect_failures: bool,
}

impl Args {
    fn parse(args: Vec<String>) -> Result<Self, Box<dyn Error>> {
        let mut seeds = Vec::new();
        let mut shard = None;
        let mut failure_dir = None;
        let mut inject_panic_seed = None;
        let mut runtime_panic_after = None;
        let mut panic_message = None;
        let mut runtime_invariant = None;
        let mut runtime_corrupt_read_client = None;
        let mut cold_corrupt_read_node = None;
        let mut write_artifacts = false;
        let mut expect_failures = false;
        let mut args = args.into_iter();

        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--seed" => {
                    let seed = args
                        .next()
                        .ok_or_else(|| format!("usage: {}", usage()))?
                        .parse::<u64>()?;
                    seeds.push(ScheduleSeed::normal(seed));
                }
                "--seed-range" => {
                    let range = args.next().ok_or_else(|| format!("usage: {}", usage()))?;
                    seeds.extend(
                        parse_seed_range(&range)?
                            .into_iter()
                            .map(ScheduleSeed::normal),
                    );
                }
                "--seed-family" => {
                    let family = args.next().ok_or_else(|| format!("usage: {}", usage()))?;
                    seeds.extend(parse_seed_family(&family)?);
                }
                "--shard" => {
                    let text = args.next().ok_or_else(|| format!("usage: {}", usage()))?;
                    shard = Some(Shard::parse(&text)?);
                }
                "--failure-dir" => {
                    let dir = args.next().ok_or_else(|| format!("usage: {}", usage()))?;
                    failure_dir = Some(PathBuf::from(dir));
                }
                "--inject-panic-seed" => {
                    let seed = args
                        .next()
                        .ok_or_else(|| format!("usage: {}", usage()))?
                        .parse::<u64>()?;
                    inject_panic_seed = Some(seed);
                }
                "--runtime-panic-after" => {
                    runtime_panic_after =
                        Some(args.next().ok_or_else(|| format!("usage: {}", usage()))?);
                }
                "--panic-message" => {
                    panic_message = Some(args.next().ok_or_else(|| format!("usage: {}", usage()))?);
                }
                "--runtime-invariant" => {
                    runtime_invariant =
                        Some(args.next().ok_or_else(|| format!("usage: {}", usage()))?);
                }
                "--runtime-corrupt-read-client" => {
                    runtime_corrupt_read_client = Some(
                        args.next()
                            .ok_or_else(|| format!("usage: {}", usage()))?
                            .parse::<usize>()?,
                    );
                }
                "--cold-corrupt-read-node" => {
                    cold_corrupt_read_node = Some(
                        args.next()
                            .ok_or_else(|| format!("usage: {}", usage()))?
                            .parse::<u64>()?,
                    );
                }
                "--write-artifacts" => {
                    write_artifacts = true;
                }
                "--expect-failures" => {
                    expect_failures = true;
                }
                "--help" | "-h" => {
                    println!("{}", usage());
                    std::process::exit(0);
                }
                _ => return Err(format!("unknown argument `{arg}`\nusage: {}", usage()).into()),
            }
        }

        if seeds.is_empty() {
            seeds.extend((60..=64).map(ScheduleSeed::normal));
        }
        seeds.sort_unstable_by_key(|seed| (seed.seed, seed.kind));
        seeds.dedup_by_key(|seed| (seed.seed, seed.kind));
        if panic_message.is_some() && runtime_panic_after.is_none() {
            return Err(format!(
                "--panic-message requires --runtime-panic-after\nusage: {}",
                usage()
            )
            .into());
        }
        if runtime_invariant.is_some() && runtime_panic_after.is_none() {
            return Err(format!(
                "--runtime-invariant requires --runtime-panic-after\nusage: {}",
                usage()
            )
            .into());
        }
        if shard.is_some() && expect_failures {
            return Err(format!(
                "--shard does not combine with --expect-failures, which needs a failure in each invocation\nusage: {}",
                usage()
            )
            .into());
        }

        Ok(Self {
            seeds,
            shard,
            failure_dir,
            inject_panic_seed,
            runtime_panic_after,
            panic_message,
            runtime_invariant,
            runtime_corrupt_read_client,
            cold_corrupt_read_node,
            write_artifacts,
            expect_failures,
        })
    }
}

/// Deterministic schedule generator invoked per seed.
type Generator = fn(u64) -> SimSchedule;

#[derive(Clone, Copy)]
struct ScheduleSeed {
    seed: u64,
    /// Stable generator identifier; part of the sort/dedup key so the same
    /// seed can run under different generators.
    kind: &'static str,
    generate: Generator,
}

impl ScheduleSeed {
    fn normal(seed: u64) -> Self {
        Self {
            seed,
            kind: "normal",
            generate: SimSchedule::generate,
        }
    }

    fn generate_schedule(self) -> SimSchedule {
        (self.generate)(self.seed)
    }

    /// The file-name stem of this run's artifacts: `seed-N`, or
    /// `seed-N-KIND` when the command runs seed N under several generators,
    /// so no run of the command overwrites another's artifacts.
    fn artifact_stem(self, shared: &BTreeSet<u64>) -> String {
        if shared.contains(&self.seed) {
            format!("seed-{}-{}", self.seed, self.kind)
        } else {
            format!("seed-{}", self.seed)
        }
    }
}

fn generate_leader_failover(seed: u64) -> SimSchedule {
    SimSchedule::for_scenario(seed, SimScenario::LeaderFailover)
}

fn generate_leader_read_linearizability(seed: u64) -> SimSchedule {
    SimSchedule::for_scenario(seed, SimScenario::LeaderReadLinearizability)
}

fn generate_producer_repeats_exactly_once(seed: u64) -> SimSchedule {
    SimSchedule::for_scenario(seed, SimScenario::ProducerRepeatsExactlyOnce)
}

fn generate_idle_expiry_cannot_reopen_an_epoch(seed: u64) -> SimSchedule {
    SimSchedule::for_scenario(seed, SimScenario::IdleExpiryCannotReopenAnEpoch)
}

fn generate_history_rewind_under_leader_churn(seed: u64) -> SimSchedule {
    SimSchedule::for_scenario(seed, SimScenario::HistoryRewindUnderLeaderChurn)
}

fn generate_stale_writers_after_restore(seed: u64) -> SimSchedule {
    SimSchedule::for_scenario(seed, SimScenario::StaleWritersAfterRestore)
}

fn generate_recovery_plan_cut_under_restarts(seed: u64) -> SimSchedule {
    SimSchedule::for_scenario(seed, SimScenario::RecoveryPlanCutUnderRestarts)
}

fn generate_recovery_plan_survivor_campaigns(seed: u64) -> SimSchedule {
    SimSchedule::for_scenario(seed, SimScenario::RecoveryPlanSurvivorCampaigns)
}

fn generate_routing_before_body_under_leader_churn(seed: u64) -> SimSchedule {
    SimSchedule::for_scenario(seed, SimScenario::RoutingBeforeBodyUnderLeaderChurn)
}

fn generate_marked_chunks_never_contradict(seed: u64) -> SimSchedule {
    SimSchedule::for_scenario(seed, SimScenario::MarkedChunksNeverContradict)
}

fn generate_recovery_plan_membership_rollback(seed: u64) -> SimSchedule {
    SimSchedule::for_scenario(seed, SimScenario::RecoveryPlanMembershipRollback)
}

fn generate_deleted_data_leaves_within_bound(seed: u64) -> SimSchedule {
    SimSchedule::for_scenario(seed, SimScenario::DeletedDataLeavesWithinBound)
}

fn generate_engine_restart_leaves_no_work(seed: u64) -> SimSchedule {
    SimSchedule::for_scenario(seed, SimScenario::EngineRestartLeavesNoWork)
}

fn generate_snapshot_objects_stay_owned(seed: u64) -> SimSchedule {
    SimSchedule::for_scenario(seed, SimScenario::SnapshotObjectsStayOwned)
}

/// The placement-move family's seeds: the run draws its shape, its meta
/// faults and its moves from the seed.
fn generate_placement_move(seed: u64) -> SimSchedule {
    SimSchedule::for_scenario(seed, SimScenario::PlacementMove)
}

/// The identity-fence family: the run draws its scenario from the seed.
fn generate_identity_fence(seed: u64) -> SimSchedule {
    SimSchedule::for_scenario(seed, SimScenario::IdentityFence)
}

/// The per-stream usage family: the run draws its script from the seed.
fn generate_stream_usage_bills_exactly(seed: u64) -> SimSchedule {
    SimSchedule::for_scenario(seed, SimScenario::StreamUsageBillsExactly)
}

/// A named contiguous seed range bound to one schedule generator.
struct SeedFamily {
    name: &'static str,
    start: u64,
    end: u64,
    kind: &'static str,
    generate: Generator,
}

/// Seed families accepted by `--seed-family`.
///
/// NOTE: `scripts/dst/audits.py` parses this table (the `name`, `start`, and
/// `end` fields) for the seed-inventory audit; keep the field layout intact.
const SEED_FAMILIES: &[SeedFamily] = &[
    SeedFamily {
        name: "routing-before-body-under-leader-churn",
        start: 500,
        end: 502,
        kind: "routing-before-body-under-leader-churn",
        generate: generate_routing_before_body_under_leader_churn,
    },
    SeedFamily {
        name: "routing-before-body-under-leader-churn-extended",
        start: 950,
        end: 979,
        kind: "routing-before-body-under-leader-churn",
        generate: generate_routing_before_body_under_leader_churn,
    },
    SeedFamily {
        name: "marked-chunks-never-contradict",
        start: 395,
        end: 399,
        kind: "marked-chunks-never-contradict",
        generate: generate_marked_chunks_never_contradict,
    },
    SeedFamily {
        name: "marked-chunks-never-contradict-extended",
        start: 920,
        end: 949,
        kind: "marked-chunks-never-contradict",
        generate: generate_marked_chunks_never_contradict,
    },
    SeedFamily {
        name: "history-rewind-under-leader-churn",
        start: 375,
        end: 379,
        kind: "history-rewind-under-leader-churn",
        generate: generate_history_rewind_under_leader_churn,
    },
    SeedFamily {
        name: "stale-writers-after-restore",
        start: 380,
        end: 384,
        kind: "stale-writers-after-restore",
        generate: generate_stale_writers_after_restore,
    },
    SeedFamily {
        name: "recovery-plan-cut-under-restarts",
        start: 385,
        end: 389,
        kind: "recovery-plan-cut-under-restarts",
        generate: generate_recovery_plan_cut_under_restarts,
    },
    SeedFamily {
        name: "recovery-plan-cut-under-restarts-extended",
        start: 880,
        end: 899,
        kind: "recovery-plan-cut-under-restarts",
        generate: generate_recovery_plan_cut_under_restarts,
    },
    SeedFamily {
        name: "recovery-plan-survivor-campaigns",
        start: 390,
        end: 394,
        kind: "recovery-plan-survivor-campaigns",
        generate: generate_recovery_plan_survivor_campaigns,
    },
    SeedFamily {
        name: "recovery-plan-survivor-campaigns-extended",
        start: 900,
        end: 919,
        kind: "recovery-plan-survivor-campaigns",
        generate: generate_recovery_plan_survivor_campaigns,
    },
    SeedFamily {
        name: "recovery-plan-membership-rollback",
        start: 505,
        end: 513,
        kind: "recovery-plan-membership-rollback",
        generate: generate_recovery_plan_membership_rollback,
    },
    SeedFamily {
        name: "recovery-plan-membership-rollback-extended",
        start: 980,
        end: 999,
        kind: "recovery-plan-membership-rollback",
        generate: generate_recovery_plan_membership_rollback,
    },
    SeedFamily {
        name: "deleted-data-leaves-within-bound",
        start: 520,
        end: 527,
        kind: "deleted-data-leaves-within-bound",
        generate: generate_deleted_data_leaves_within_bound,
    },
    SeedFamily {
        name: "deleted-data-leaves-within-bound-extended",
        start: 600,
        end: 629,
        kind: "deleted-data-leaves-within-bound",
        generate: generate_deleted_data_leaves_within_bound,
    },
    SeedFamily {
        name: "deleted-data-across-segments",
        start: 528,
        end: 531,
        kind: "deleted-data-leaves-within-bound",
        generate: generate_deleted_data_leaves_within_bound,
    },
    SeedFamily {
        name: "deleted-data-across-segments-extended",
        start: 630,
        end: 649,
        kind: "deleted-data-leaves-within-bound",
        generate: generate_deleted_data_leaves_within_bound,
    },
    SeedFamily {
        name: "engine-restart-leaves-no-work",
        start: 540,
        end: 545,
        kind: "engine-restart-leaves-no-work",
        generate: generate_engine_restart_leaves_no_work,
    },
    SeedFamily {
        name: "engine-restart-leaves-no-work-extended",
        start: 1000,
        end: 1029,
        kind: "engine-restart-leaves-no-work",
        generate: generate_engine_restart_leaves_no_work,
    },
    SeedFamily {
        name: "snapshot-objects-stay-owned",
        start: 546,
        end: 549,
        kind: "snapshot-objects-stay-owned",
        generate: generate_snapshot_objects_stay_owned,
    },
    SeedFamily {
        name: "snapshot-objects-stay-owned-extended",
        start: 1030,
        end: 1061,
        kind: "snapshot-objects-stay-owned",
        generate: generate_snapshot_objects_stay_owned,
    },
    SeedFamily {
        name: "placement-move",
        start: 1100,
        end: 1109,
        kind: "placement-move",
        generate: generate_placement_move,
    },
    SeedFamily {
        name: "identity-fence",
        start: 1110,
        end: 1119,
        kind: "identity-fence",
        generate: generate_identity_fence,
    },
    SeedFamily {
        name: "placement-move-extended",
        start: 1120,
        end: 1159,
        kind: "placement-move",
        generate: generate_placement_move,
    },
    SeedFamily {
        name: "identity-fence-extended",
        start: 1160,
        end: 1163,
        kind: "identity-fence",
        generate: generate_identity_fence,
    },
    SeedFamily {
        name: "stream-usage-bills-exactly",
        start: 1170,
        end: 1177,
        kind: "stream-usage-bills-exactly",
        generate: generate_stream_usage_bills_exactly,
    },
    SeedFamily {
        name: "stream-usage-bills-exactly-extended",
        start: 1180,
        end: 1211,
        kind: "stream-usage-bills-exactly",
        generate: generate_stream_usage_bills_exactly,
    },
    SeedFamily {
        name: "deleted-data-after-expiry",
        start: 532,
        end: 539,
        kind: "deleted-data-leaves-within-bound",
        generate: generate_deleted_data_leaves_within_bound,
    },
    SeedFamily {
        name: "deleted-data-after-expiry-extended",
        start: 650,
        end: 679,
        kind: "deleted-data-leaves-within-bound",
        generate: generate_deleted_data_leaves_within_bound,
    },
    SeedFamily {
        name: "history-rewind-under-leader-churn-extended",
        start: 850,
        end: 879,
        kind: "history-rewind-under-leader-churn",
        generate: generate_history_rewind_under_leader_churn,
    },
    SeedFamily {
        name: "producer-repeats-exactly-once",
        start: 365,
        end: 369,
        kind: "producer-repeats-exactly-once",
        generate: generate_producer_repeats_exactly_once,
    },
    SeedFamily {
        name: "idle-expiry-cannot-reopen-an-epoch",
        start: 370,
        end: 374,
        kind: "idle-expiry-cannot-reopen-an-epoch",
        generate: generate_idle_expiry_cannot_reopen_an_epoch,
    },
    SeedFamily {
        name: "producer-repeats-exactly-once-extended",
        start: 800,
        end: 849,
        kind: "producer-repeats-exactly-once",
        generate: generate_producer_repeats_exactly_once,
    },
    SeedFamily {
        name: "leader-read-linearizability",
        start: 360,
        end: 364,
        kind: "leader-read-linearizability",
        generate: generate_leader_read_linearizability,
    },
    SeedFamily {
        name: "leader-read-linearizability-extended",
        start: 700,
        end: 799,
        kind: "leader-read-linearizability",
        generate: generate_leader_read_linearizability,
    },
    SeedFamily {
        name: "runtime-interleaving",
        start: 72,
        end: 96,
        kind: "normal",
        generate: SimSchedule::generate,
    },
    SeedFamily {
        name: "runtime-raft-engine",
        start: 97,
        end: 101,
        kind: "normal",
        generate: SimSchedule::generate,
    },
    SeedFamily {
        name: "runtime-raft-network",
        start: 102,
        end: 106,
        kind: "normal",
        generate: SimSchedule::generate,
    },
    SeedFamily {
        name: "runtime-raft-network-recovery",
        start: 107,
        end: 111,
        kind: "runtime-raft-network-recovery",
        generate: SimSchedule::generate_runtime_raft_network_recovery,
    },
    SeedFamily {
        name: "runtime-raft-network-cold-live-recovery",
        start: 112,
        end: 116,
        kind: "runtime-raft-network-cold-live-recovery",
        generate: SimSchedule::generate_runtime_raft_network_cold_live_recovery,
    },
    SeedFamily {
        name: "runtime-raft-network-cold-live-restart",
        start: 117,
        end: 121,
        kind: "runtime-raft-network-cold-live-restart",
        generate: SimSchedule::generate_runtime_raft_network_cold_live_restart,
    },
    SeedFamily {
        name: "runtime-raft-network-cold-live-write-recovery",
        start: 317,
        end: 321,
        kind: "runtime-raft-network-cold-live-write-recovery",
        generate: SimSchedule::generate_runtime_raft_network_cold_live_write_recovery,
    },
    SeedFamily {
        name: "leader-failover",
        start: 122,
        end: 126,
        kind: "leader-failover",
        generate: generate_leader_failover,
    },
    SeedFamily {
        name: "runtime-raft-network-leader-failover",
        start: 127,
        end: 131,
        kind: "runtime-raft-network-leader-failover",
        generate: SimSchedule::generate_runtime_raft_network_leader_failover,
    },
    SeedFamily {
        name: "runtime-raft-snapshot-install",
        start: 132,
        end: 136,
        kind: "normal",
        generate: SimSchedule::generate,
    },
    SeedFamily {
        name: "runtime-raft-network-randomized",
        start: 137,
        end: 156,
        kind: "runtime-raft-network-randomized",
        generate: SimSchedule::generate_runtime_raft_network_randomized,
    },
    SeedFamily {
        name: "runtime-raft-network-randomized-extended",
        start: 400,
        end: 499,
        kind: "runtime-raft-network-randomized",
        generate: SimSchedule::generate_runtime_raft_network_randomized,
    },
    SeedFamily {
        name: "http-protocol-surface-randomized",
        start: 277,
        end: 296,
        kind: "http-protocol-surface-randomized",
        generate: SimSchedule::generate_http_protocol_surface_randomized,
    },
    SeedFamily {
        name: "pipeline-smoke-http-protocol-surface-randomized-corruption",
        start: 297,
        end: 301,
        kind: "http-protocol-surface-randomized-failure",
        generate: SimSchedule::generate_http_protocol_surface_randomized_failure,
    },
    SeedFamily {
        name: "pipeline-smoke-http-protocol-surface-randomized-sse-corruption",
        start: 302,
        end: 306,
        kind: "http-protocol-surface-randomized-sse-failure",
        generate: SimSchedule::generate_http_protocol_surface_randomized_sse_failure,
    },
    SeedFamily {
        name: "pipeline-smoke-http-protocol-surface-randomized-backpressure-corruption",
        start: 307,
        end: 311,
        kind: "http-protocol-surface-randomized-backpressure-failure",
        generate: SimSchedule::generate_http_protocol_surface_randomized_backpressure_failure,
    },
    SeedFamily {
        name: "pipeline-smoke-runtime-raft-network-randomized-read-corruption",
        start: 242,
        end: 246,
        kind: "runtime-raft-network-randomized-failure",
        generate: SimSchedule::generate_runtime_raft_network_randomized_failure,
    },
    SeedFamily {
        name: "pipeline-smoke-runtime-raft-network-partial-read-corruption",
        start: 247,
        end: 251,
        kind: "runtime-raft-network-partial-read-failure",
        generate: SimSchedule::generate_runtime_raft_network_partial_read_failure,
    },
    SeedFamily {
        name: "pipeline-smoke-runtime-raft-network-tail-read-corruption",
        start: 337,
        end: 341,
        kind: "runtime-raft-network-tail-read-failure",
        generate: SimSchedule::generate_runtime_raft_network_tail_read_failure,
    },
    SeedFamily {
        name: "pipeline-smoke-runtime-raft-network-close-state-corruption",
        start: 342,
        end: 346,
        kind: "runtime-raft-network-close-failure",
        generate: SimSchedule::generate_runtime_raft_network_close_failure,
    },
    SeedFamily {
        name: "pipeline-smoke-runtime-raft-network-snapshot-corruption",
        start: 347,
        end: 351,
        kind: "runtime-raft-network-snapshot-failure",
        generate: SimSchedule::generate_runtime_raft_network_snapshot_failure,
    },
    SeedFamily {
        name: "pipeline-smoke-runtime-raft-network-leader-failover-read-corruption",
        start: 252,
        end: 256,
        kind: "runtime-raft-network-leader-failover-read-failure",
        generate: SimSchedule::generate_runtime_raft_network_leader_failover_read_failure,
    },
    SeedFamily {
        name: "runtime-raft-network-leader-failover-cold-live-read-failures",
        start: 327,
        end: 331,
        kind: "runtime-raft-network-leader-failover-cold-live-read-failure",
        generate: SimSchedule::generate_runtime_raft_network_leader_failover_cold_live_read_failure,
    },
    SeedFamily {
        name: "runtime-raft-network-randomized-cold-read-failures",
        start: 322,
        end: 326,
        kind: "runtime-raft-network-randomized-cold-read-failure",
        generate: SimSchedule::generate_runtime_raft_network_randomized_cold_read_failure,
    },
    SeedFamily {
        name: "runtime-raft-snapshot-install-failures",
        start: 232,
        end: 236,
        kind: "runtime-raft-snapshot-install-failure",
        generate: SimSchedule::generate_runtime_raft_snapshot_install_failure,
    },
    SeedFamily {
        name: "runtime-raft-network-cold-live-truncate-failures",
        start: 222,
        end: 226,
        kind: "runtime-raft-network-cold-live-truncate-failure",
        generate: SimSchedule::generate_runtime_raft_network_cold_live_truncate_failure,
    },
    SeedFamily {
        name: "runtime-raft-network-cold-live-write-failures",
        start: 312,
        end: 316,
        kind: "runtime-raft-network-cold-live-write-failure",
        generate: SimSchedule::generate_runtime_raft_network_cold_live_write_failure,
    },
    SeedFamily {
        name: "runtime-raft-network-partition-failures",
        start: 212,
        end: 216,
        kind: "runtime-raft-network-partition-failure",
        generate: SimSchedule::generate_runtime_raft_network_partition_failure,
    },
    SeedFamily {
        name: "pipeline-smoke-runtime-interleaving-read-corruption",
        start: 172,
        end: 176,
        kind: "runtime-interleaving-failure",
        generate: SimSchedule::generate_runtime_interleaving_failure,
    },
    SeedFamily {
        name: "runtime-interleaving-truncate-failures",
        start: 182,
        end: 186,
        kind: "runtime-interleaving-truncate-failure",
        generate: SimSchedule::generate_runtime_interleaving_truncate_failure,
    },
    SeedFamily {
        name: "runtime-interleaving-write-failures",
        start: 192,
        end: 196,
        kind: "runtime-interleaving-write-failure",
        generate: SimSchedule::generate_runtime_interleaving_write_failure,
    },
    SeedFamily {
        name: "pipeline-smoke-http-producer-retry-corruption",
        start: 262,
        end: 266,
        kind: "http-producer-protocol-surface-failure",
        generate: SimSchedule::generate_http_producer_protocol_surface_failure,
    },
    SeedFamily {
        name: "pipeline-smoke-http-live-sse-corruption",
        start: 267,
        end: 271,
        kind: "http-live-protocol-surface-failure",
        generate: SimSchedule::generate_http_live_protocol_surface_failure,
    },
    SeedFamily {
        name: "pipeline-smoke-http-live-waiter-corruption",
        start: 272,
        end: 276,
        kind: "http-live-limit-protocol-surface-failure",
        generate: SimSchedule::generate_http_live_limit_protocol_surface_failure,
    },
    SeedFamily {
        name: "pipeline-smoke-http-snapshot-body-corruption",
        start: 332,
        end: 336,
        kind: "http-snapshot-protocol-surface-failure",
        generate: SimSchedule::generate_http_snapshot_protocol_surface_failure,
    },
    SeedFamily {
        name: "raft-partition-failures",
        start: 202,
        end: 206,
        kind: "raft-partition-failure",
        generate: SimSchedule::generate_raft_partition_failure,
    },
];

fn parse_seed_range(range: &str) -> Result<Vec<u64>, Box<dyn Error>> {
    let (start, end) = range
        .split_once("..=")
        .ok_or_else(|| format!("seed range must use START..=END, got `{range}`"))?;
    let start = start.parse::<u64>()?;
    let end = end.parse::<u64>()?;
    if start > end {
        return Err(format!("seed range start {start} is greater than end {end}").into());
    }
    Ok((start..=end).collect())
}

fn parse_seed_family(family: &str) -> Result<Vec<ScheduleSeed>, Box<dyn Error>> {
    let entry = SEED_FAMILIES
        .iter()
        .find(|candidate| candidate.name == family)
        .ok_or_else(|| format!("unknown seed family `{family}`"))?;
    Ok((entry.start..=entry.end)
        .map(|seed| ScheduleSeed {
            seed,
            kind: entry.kind,
            generate: entry.generate,
        })
        .collect())
}

fn usage() -> String {
    let families = SEED_FAMILIES
        .iter()
        .map(|family| family.name)
        .collect::<Vec<_>>()
        .join("|");
    format!(
        "ursula-sim smoke [--seed N]... [--seed-range START..=END] [--seed-family {families}] [--failure-dir DIR] [--inject-panic-seed N] [--runtime-panic-after EVENT --panic-message TEXT --runtime-invariant NAME] [--runtime-corrupt-read-client CLIENT_ID] [--cold-corrupt-read-node NODE_ID] [--write-artifacts] [--expect-failures] [--shard INDEX/COUNT]"
    )
}

fn apply_runtime_panic_after(
    schedule: &mut SimSchedule,
    after_event: Option<&String>,
    message: Option<&String>,
    invariant: Option<&String>,
) -> Result<(), Box<dyn Error>> {
    let Some(after_event) = after_event else {
        return Ok(());
    };
    if schedule.scenario != SimScenario::RuntimeSeededInterleaving {
        return Err(format!(
            "--runtime-panic-after requires runtime_seeded_interleaving, got {:?} for seed {}",
            schedule.scenario, schedule.seed
        )
        .into());
    }
    let message = message
        .cloned()
        .unwrap_or_else(|| format!("injected schedule panic after {after_event}"));
    for step in &mut schedule.fault_plan.steps {
        if let ursula_sim::SimFaultAction::RunRuntimeSeededInterleaving { plan } = &mut step.action
        {
            plan.panic_after = Some(ursula_sim::RuntimeInterleavingPanic {
                after_event: after_event.clone(),
                message,
                invariant: invariant.cloned(),
            });
            return Ok(());
        }
    }
    Err("runtime_seeded_interleaving schedule does not contain an interleaving plan".into())
}

fn apply_runtime_corrupt_read_client(
    schedule: &mut SimSchedule,
    client_id: Option<usize>,
) -> Result<(), Box<dyn Error>> {
    let Some(client_id) = client_id else {
        return Ok(());
    };
    if schedule.scenario != SimScenario::RuntimeSeededInterleaving {
        return Err(format!(
            "--runtime-corrupt-read-client requires runtime_seeded_interleaving, got {:?} for seed {}",
            schedule.scenario, schedule.seed
        )
        .into());
    }
    for step in &mut schedule.fault_plan.steps {
        if let ursula_sim::SimFaultAction::RunRuntimeSeededInterleaving { plan } = &mut step.action
        {
            plan.corrupt_read_client_id = Some(client_id);
            return Ok(());
        }
    }
    Err("runtime_seeded_interleaving schedule does not contain an interleaving plan".into())
}

fn apply_cold_corrupt_read_node(
    schedule: &mut SimSchedule,
    node_id: Option<u64>,
) -> Result<(), Box<dyn Error>> {
    let Some(node_id) = node_id else {
        return Ok(());
    };
    if schedule.scenario != SimScenario::ColdLiveRead {
        return Err(format!(
            "--cold-corrupt-read-node requires cold_live_read, got {:?} for seed {}",
            schedule.scenario, schedule.seed
        )
        .into());
    }
    schedule.fault_plan.steps.push(ursula_sim::SimFaultStep {
        phase: "cold_live_read_verify".to_owned(),
        action: ursula_sim::SimFaultAction::CorruptColdLiveReadExpectation { node_id },
    });
    Ok(())
}

fn write_success_artifacts(
    failure_dir: &Path,
    stem: &str,
    record: &SimScheduledRecord,
) -> Result<(), Box<dyn Error>> {
    let stable_path = failure_dir.join(format!("{stem}-replay.json"));
    let raw_path = failure_dir.join(format!("{stem}-raw-events.json"));
    let mut stable_body = serde_json::to_string_pretty(record)?;
    stable_body.push('\n');
    fs::write(stable_path, stable_body)?;

    let mut raw_body = serde_json::to_string_pretty(&RawEventLogArtifact {
        schema_version: SIM_REGRESSION_SCHEMA_VERSION,
        seed: record.schedule.seed,
        events: SimTrace::last_recorded().events,
    })?;
    raw_body.push('\n');
    fs::write(raw_path, raw_body)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::path::PathBuf;

    use super::FailedSeed;
    use super::ScheduleSeed;
    use super::SweepError;
    use super::generate_leader_failover;
    use super::seeds_under_several_generators;
    use super::sweep_verdict;

    fn leader_failover(seed: u64) -> ScheduleSeed {
        ScheduleSeed {
            seed,
            kind: "leader-failover",
            generate: generate_leader_failover,
        }
    }

    fn failed(seed: u64) -> FailedSeed {
        FailedSeed {
            seed,
            kind: "normal",
            summary_path: PathBuf::from(format!("dir/seed-{seed}-failure.json")),
            stable_path: PathBuf::from(format!("dir/seed-{seed}-stable-trace.json")),
            raw_path: PathBuf::from(format!("dir/seed-{seed}-raw-events.json")),
        }
    }

    #[test]
    fn a_sweep_with_failures_fails_naming_every_failed_seed() {
        let err = sweep_verdict(vec![failed(61), failed(64)], false).unwrap_err();
        let message = err.to_string();
        assert!(
            matches!(&err, SweepError::SeedsFailed { failures } if failures.len() == 2),
            "{err:?}"
        );
        assert!(message.contains("2 sim seeds failed"), "{message}");
        assert!(message.contains("dir/seed-61-failure.json"), "{message}");
        assert!(message.contains("dir/seed-64-failure.json"), "{message}");
    }

    #[test]
    fn a_sweep_verdict_follows_expect_failures() {
        assert!(matches!(sweep_verdict(Vec::new(), false), Ok(())));
        assert!(matches!(sweep_verdict(vec![failed(61)], true), Ok(())));
        assert!(matches!(
            sweep_verdict(Vec::new(), true),
            Err(SweepError::NoSeedFailed)
        ));
    }

    #[test]
    fn a_seed_under_several_generators_names_its_artifacts_by_generator() {
        let seeds = vec![
            ScheduleSeed::normal(121),
            ScheduleSeed::normal(122),
            leader_failover(122),
            leader_failover(123),
        ];
        let shared = seeds_under_several_generators(&seeds);
        assert_eq!(shared, BTreeSet::from([122]));

        let stems = seeds
            .iter()
            .map(|seed| seed.artifact_stem(&shared))
            .collect::<Vec<_>>();
        assert_eq!(stems, [
            "seed-121",
            "seed-122-normal",
            "seed-122-leader-failover",
            "seed-123",
        ]);
    }
}

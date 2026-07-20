//! `ursula-sim smoke`: corpus replays plus a seed sweep with failure
//! artifacts.

use std::error::Error;
use std::fs;
use std::panic;
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

    let failure_dir = args
        .failure_dir
        .unwrap_or_else(|| PathBuf::from("target/ursula-sim-failures"));
    fs::create_dir_all(&failure_dir)?;

    let mut observed_failures = 0usize;
    for schedule_seed in args.seeds {
        let seed = schedule_seed.seed;
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
                    write_success_artifacts(&failure_dir, &record)?;
                }
            }
            Err(payload) => {
                observed_failures += 1;
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
                let stable_path = failure_dir.join(format!("seed-{seed}-stable-trace.json"));
                let raw_path = failure_dir.join(format!("seed-{seed}-raw-events.json"));
                let summary_path = failure_dir.join(format!("seed-{seed}-failure.json"));

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

                let panic = panic_payload_to_string(payload);
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
                if !args.expect_failures {
                    return Err(format!(
                        "sim seed {seed} failed; wrote {}, {}, {}",
                        summary_path.display(),
                        stable_path.display(),
                        raw_path.display()
                    )
                    .into());
                }
            }
        }
    }

    if args.expect_failures && observed_failures == 0 {
        return Err("--expect-failures was set but no seed failed".into());
    }

    Ok(())
}

struct Args {
    seeds: Vec<ScheduleSeed>,
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

        Ok(Self {
            seeds,
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
}

fn generate_leader_failover(seed: u64) -> SimSchedule {
    SimSchedule::for_scenario(seed, SimScenario::LeaderFailover)
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
        "ursula-sim smoke [--seed N]... [--seed-range START..=END] [--seed-family {families}] [--failure-dir DIR] [--inject-panic-seed N] [--runtime-panic-after EVENT --panic-message TEXT --runtime-invariant NAME] [--runtime-corrupt-read-client CLIENT_ID] [--cold-corrupt-read-node NODE_ID] [--write-artifacts] [--expect-failures]"
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
    failure_dir: &std::path::Path,
    record: &SimScheduledRecord,
) -> Result<(), Box<dyn Error>> {
    let stable_path = failure_dir.join(format!("seed-{}-replay.json", record.schedule.seed));
    let raw_path = failure_dir.join(format!("seed-{}-raw-events.json", record.schedule.seed));
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

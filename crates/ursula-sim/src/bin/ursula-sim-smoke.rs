use std::env;
use std::error::Error;
use std::path::PathBuf;

#[cfg(madsim)]
fn main() -> Result<(), Box<dyn Error>> {
    use std::fs;
    use std::panic;

    use ursula_sim::SIM_REGRESSION_SCHEMA_VERSION;
    use ursula_sim::SimFailureRegressionRecord;
    use ursula_sim::SimRegressionRecord;
    use ursula_sim::SimSchedule;
    use ursula_sim::SimScheduledRecord;
    use ursula_sim::SimTrace;

    let args = Args::parse()?;

    let regression_corpus = include_str!("../../corpus/smoke.json");
    let regression_records = serde_json::from_str::<Vec<SimRegressionRecord>>(regression_corpus)?;
    for record in regression_records {
        record.assert_replays();
    }

    let schedule_corpus = include_str!("../../corpus/schedule-smoke.json");
    let schedule_records = serde_json::from_str::<Vec<SimScheduledRecord>>(schedule_corpus)?;
    for record in schedule_records {
        assert_eq!(record.schedule, SimSchedule::generate(record.schedule.seed));
        record.assert_replays();
    }

    let failure_corpus = include_str!("../../corpus/failure-smoke.json");
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

#[cfg(not(madsim))]
fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse()?;
    let _ = (
        args.seeds,
        args.failure_dir,
        args.inject_panic_seed,
        args.runtime_panic_after,
        args.panic_message,
        args.runtime_invariant,
        args.runtime_corrupt_read_client,
        args.cold_corrupt_read_node,
        args.write_artifacts,
        args.expect_failures,
    );
    Err("ursula-sim-smoke must run with RUSTFLAGS=\"--cfg madsim\"".into())
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
    fn parse() -> Result<Self, Box<dyn Error>> {
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
        let mut args = env::args().skip(1);

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
        seeds.sort_unstable();
        seeds.dedup();
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum ScheduleKind {
    Normal,
    RuntimeInterleavingFailure,
    RuntimeInterleavingTruncateFailure,
    RuntimeInterleavingWriteFailure,
    HttpProducerProtocolSurfaceFailure,
    HttpLiveProtocolSurfaceFailure,
    HttpLiveLimitProtocolSurfaceFailure,
    HttpSnapshotProtocolSurfaceFailure,
    RaftPartitionFailure,
    LeaderFailover,
    RuntimeRaftNetworkRecovery,
    RuntimeRaftNetworkColdLiveRecovery,
    RuntimeRaftNetworkColdLiveRestart,
    RuntimeRaftNetworkColdLiveWriteRecovery,
    RuntimeRaftNetworkLeaderFailover,
    RuntimeRaftNetworkRandomized,
    HttpProtocolSurfaceRandomized,
    HttpProtocolSurfaceRandomizedFailure,
    HttpProtocolSurfaceRandomizedSseFailure,
    HttpProtocolSurfaceRandomizedBackpressureFailure,
    RuntimeRaftNetworkRandomizedFailure,
    RuntimeRaftNetworkPartialReadFailure,
    RuntimeRaftNetworkTailReadFailure,
    RuntimeRaftNetworkCloseFailure,
    RuntimeRaftNetworkSnapshotFailure,
    RuntimeRaftNetworkLeaderFailoverReadFailure,
    RuntimeRaftNetworkLeaderFailoverColdLiveReadFailure,
    RuntimeRaftNetworkRandomizedColdReadFailure,
    RuntimeRaftSnapshotInstallFailure,
    RuntimeRaftNetworkColdLiveTruncateFailure,
    RuntimeRaftNetworkColdLiveWriteFailure,
    RuntimeRaftNetworkPartitionFailure,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct ScheduleSeed {
    seed: u64,
    kind: ScheduleKind,
}

impl ScheduleSeed {
    fn normal(seed: u64) -> Self {
        Self {
            seed,
            kind: ScheduleKind::Normal,
        }
    }

    fn runtime_interleaving_failure(seed: u64) -> Self {
        Self {
            seed,
            kind: ScheduleKind::RuntimeInterleavingFailure,
        }
    }

    fn runtime_interleaving_truncate_failure(seed: u64) -> Self {
        Self {
            seed,
            kind: ScheduleKind::RuntimeInterleavingTruncateFailure,
        }
    }

    fn runtime_interleaving_write_failure(seed: u64) -> Self {
        Self {
            seed,
            kind: ScheduleKind::RuntimeInterleavingWriteFailure,
        }
    }

    fn http_producer_protocol_surface_failure(seed: u64) -> Self {
        Self {
            seed,
            kind: ScheduleKind::HttpProducerProtocolSurfaceFailure,
        }
    }

    fn http_live_protocol_surface_failure(seed: u64) -> Self {
        Self {
            seed,
            kind: ScheduleKind::HttpLiveProtocolSurfaceFailure,
        }
    }

    fn http_live_limit_protocol_surface_failure(seed: u64) -> Self {
        Self {
            seed,
            kind: ScheduleKind::HttpLiveLimitProtocolSurfaceFailure,
        }
    }

    fn http_snapshot_protocol_surface_failure(seed: u64) -> Self {
        Self {
            seed,
            kind: ScheduleKind::HttpSnapshotProtocolSurfaceFailure,
        }
    }

    fn raft_partition_failure(seed: u64) -> Self {
        Self {
            seed,
            kind: ScheduleKind::RaftPartitionFailure,
        }
    }

    fn leader_failover(seed: u64) -> Self {
        Self {
            seed,
            kind: ScheduleKind::LeaderFailover,
        }
    }

    fn runtime_raft_network_recovery(seed: u64) -> Self {
        Self {
            seed,
            kind: ScheduleKind::RuntimeRaftNetworkRecovery,
        }
    }

    fn runtime_raft_network_cold_live_recovery(seed: u64) -> Self {
        Self {
            seed,
            kind: ScheduleKind::RuntimeRaftNetworkColdLiveRecovery,
        }
    }

    fn runtime_raft_network_cold_live_restart(seed: u64) -> Self {
        Self {
            seed,
            kind: ScheduleKind::RuntimeRaftNetworkColdLiveRestart,
        }
    }

    fn runtime_raft_network_cold_live_write_recovery(seed: u64) -> Self {
        Self {
            seed,
            kind: ScheduleKind::RuntimeRaftNetworkColdLiveWriteRecovery,
        }
    }

    fn runtime_raft_network_leader_failover(seed: u64) -> Self {
        Self {
            seed,
            kind: ScheduleKind::RuntimeRaftNetworkLeaderFailover,
        }
    }

    fn runtime_raft_network_randomized(seed: u64) -> Self {
        Self {
            seed,
            kind: ScheduleKind::RuntimeRaftNetworkRandomized,
        }
    }

    fn http_protocol_surface_randomized(seed: u64) -> Self {
        Self {
            seed,
            kind: ScheduleKind::HttpProtocolSurfaceRandomized,
        }
    }

    fn http_protocol_surface_randomized_failure(seed: u64) -> Self {
        Self {
            seed,
            kind: ScheduleKind::HttpProtocolSurfaceRandomizedFailure,
        }
    }

    fn http_protocol_surface_randomized_sse_failure(seed: u64) -> Self {
        Self {
            seed,
            kind: ScheduleKind::HttpProtocolSurfaceRandomizedSseFailure,
        }
    }

    fn http_protocol_surface_randomized_backpressure_failure(seed: u64) -> Self {
        Self {
            seed,
            kind: ScheduleKind::HttpProtocolSurfaceRandomizedBackpressureFailure,
        }
    }

    fn runtime_raft_network_randomized_failure(seed: u64) -> Self {
        Self {
            seed,
            kind: ScheduleKind::RuntimeRaftNetworkRandomizedFailure,
        }
    }

    fn runtime_raft_network_partial_read_failure(seed: u64) -> Self {
        Self {
            seed,
            kind: ScheduleKind::RuntimeRaftNetworkPartialReadFailure,
        }
    }

    fn runtime_raft_network_tail_read_failure(seed: u64) -> Self {
        Self {
            seed,
            kind: ScheduleKind::RuntimeRaftNetworkTailReadFailure,
        }
    }

    fn runtime_raft_network_close_failure(seed: u64) -> Self {
        Self {
            seed,
            kind: ScheduleKind::RuntimeRaftNetworkCloseFailure,
        }
    }

    fn runtime_raft_network_snapshot_failure(seed: u64) -> Self {
        Self {
            seed,
            kind: ScheduleKind::RuntimeRaftNetworkSnapshotFailure,
        }
    }

    fn runtime_raft_network_leader_failover_read_failure(seed: u64) -> Self {
        Self {
            seed,
            kind: ScheduleKind::RuntimeRaftNetworkLeaderFailoverReadFailure,
        }
    }

    fn runtime_raft_network_leader_failover_cold_live_read_failure(seed: u64) -> Self {
        Self {
            seed,
            kind: ScheduleKind::RuntimeRaftNetworkLeaderFailoverColdLiveReadFailure,
        }
    }

    fn runtime_raft_network_randomized_cold_read_failure(seed: u64) -> Self {
        Self {
            seed,
            kind: ScheduleKind::RuntimeRaftNetworkRandomizedColdReadFailure,
        }
    }

    fn runtime_raft_network_cold_live_truncate_failure(seed: u64) -> Self {
        Self {
            seed,
            kind: ScheduleKind::RuntimeRaftNetworkColdLiveTruncateFailure,
        }
    }

    fn runtime_raft_network_cold_live_write_failure(seed: u64) -> Self {
        Self {
            seed,
            kind: ScheduleKind::RuntimeRaftNetworkColdLiveWriteFailure,
        }
    }

    fn runtime_raft_snapshot_install_failure(seed: u64) -> Self {
        Self {
            seed,
            kind: ScheduleKind::RuntimeRaftSnapshotInstallFailure,
        }
    }

    fn runtime_raft_network_partition_failure(seed: u64) -> Self {
        Self {
            seed,
            kind: ScheduleKind::RuntimeRaftNetworkPartitionFailure,
        }
    }

    #[cfg(madsim)]
    fn generate_schedule(self) -> ursula_sim::SimSchedule {
        match self.kind {
            ScheduleKind::Normal => ursula_sim::SimSchedule::generate(self.seed),
            ScheduleKind::RuntimeInterleavingFailure => {
                ursula_sim::SimSchedule::generate_runtime_interleaving_failure(self.seed)
            }
            ScheduleKind::RuntimeInterleavingTruncateFailure => {
                ursula_sim::SimSchedule::generate_runtime_interleaving_truncate_failure(self.seed)
            }
            ScheduleKind::RuntimeInterleavingWriteFailure => {
                ursula_sim::SimSchedule::generate_runtime_interleaving_write_failure(self.seed)
            }
            ScheduleKind::HttpProducerProtocolSurfaceFailure => {
                ursula_sim::SimSchedule::generate_http_producer_protocol_surface_failure(self.seed)
            }
            ScheduleKind::HttpLiveProtocolSurfaceFailure => {
                ursula_sim::SimSchedule::generate_http_live_protocol_surface_failure(self.seed)
            }
            ScheduleKind::HttpLiveLimitProtocolSurfaceFailure => {
                ursula_sim::SimSchedule::generate_http_live_limit_protocol_surface_failure(
                    self.seed,
                )
            }
            ScheduleKind::RaftPartitionFailure => {
                ursula_sim::SimSchedule::generate_raft_partition_failure(self.seed)
            }
            ScheduleKind::LeaderFailover => ursula_sim::SimSchedule::for_scenario(
                self.seed,
                ursula_sim::SimScenario::LeaderFailover,
            ),
            ScheduleKind::RuntimeRaftNetworkRecovery => {
                ursula_sim::SimSchedule::generate_runtime_raft_network_recovery(self.seed)
            }
            ScheduleKind::RuntimeRaftNetworkColdLiveRecovery => {
                ursula_sim::SimSchedule::generate_runtime_raft_network_cold_live_recovery(self.seed)
            }
            ScheduleKind::RuntimeRaftNetworkColdLiveRestart => {
                ursula_sim::SimSchedule::generate_runtime_raft_network_cold_live_restart(self.seed)
            }
            ScheduleKind::RuntimeRaftNetworkColdLiveWriteRecovery => {
                ursula_sim::SimSchedule::generate_runtime_raft_network_cold_live_write_recovery(
                    self.seed,
                )
            }
            ScheduleKind::RuntimeRaftNetworkLeaderFailover => {
                ursula_sim::SimSchedule::generate_runtime_raft_network_leader_failover(self.seed)
            }
            ScheduleKind::RuntimeRaftNetworkRandomized => {
                ursula_sim::SimSchedule::generate_runtime_raft_network_randomized(self.seed)
            }
            ScheduleKind::HttpProtocolSurfaceRandomized => {
                ursula_sim::SimSchedule::generate_http_protocol_surface_randomized(self.seed)
            }
            ScheduleKind::HttpProtocolSurfaceRandomizedFailure => {
                ursula_sim::SimSchedule::generate_http_protocol_surface_randomized_failure(
                    self.seed,
                )
            }
            ScheduleKind::HttpProtocolSurfaceRandomizedSseFailure => {
                ursula_sim::SimSchedule::generate_http_protocol_surface_randomized_sse_failure(
                    self.seed,
                )
            }
            ScheduleKind::HttpProtocolSurfaceRandomizedBackpressureFailure => {
                ursula_sim::SimSchedule::generate_http_protocol_surface_randomized_backpressure_failure(
                    self.seed,
                )
            }
            ScheduleKind::HttpSnapshotProtocolSurfaceFailure => {
                ursula_sim::SimSchedule::generate_http_snapshot_protocol_surface_failure(self.seed)
            }
            ScheduleKind::RuntimeRaftNetworkRandomizedFailure => {
                ursula_sim::SimSchedule::generate_runtime_raft_network_randomized_failure(self.seed)
            }
            ScheduleKind::RuntimeRaftNetworkPartialReadFailure => {
                ursula_sim::SimSchedule::generate_runtime_raft_network_partial_read_failure(
                    self.seed,
                )
            }
            ScheduleKind::RuntimeRaftNetworkTailReadFailure => {
                ursula_sim::SimSchedule::generate_runtime_raft_network_tail_read_failure(self.seed)
            }
            ScheduleKind::RuntimeRaftNetworkCloseFailure => {
                ursula_sim::SimSchedule::generate_runtime_raft_network_close_failure(self.seed)
            }
            ScheduleKind::RuntimeRaftNetworkSnapshotFailure => {
                ursula_sim::SimSchedule::generate_runtime_raft_network_snapshot_failure(self.seed)
            }
            ScheduleKind::RuntimeRaftNetworkLeaderFailoverReadFailure => {
                ursula_sim::SimSchedule::generate_runtime_raft_network_leader_failover_read_failure(
                    self.seed,
                )
            }
            ScheduleKind::RuntimeRaftNetworkLeaderFailoverColdLiveReadFailure => {
                ursula_sim::SimSchedule::generate_runtime_raft_network_leader_failover_cold_live_read_failure(
                    self.seed,
                )
            }
            ScheduleKind::RuntimeRaftNetworkRandomizedColdReadFailure => {
                ursula_sim::SimSchedule::generate_runtime_raft_network_randomized_cold_read_failure(
                    self.seed,
                )
            }
            ScheduleKind::RuntimeRaftSnapshotInstallFailure => {
                ursula_sim::SimSchedule::generate_runtime_raft_snapshot_install_failure(self.seed)
            }
            ScheduleKind::RuntimeRaftNetworkColdLiveTruncateFailure => {
                ursula_sim::SimSchedule::generate_runtime_raft_network_cold_live_truncate_failure(
                    self.seed,
                )
            }
            ScheduleKind::RuntimeRaftNetworkColdLiveWriteFailure => {
                ursula_sim::SimSchedule::generate_runtime_raft_network_cold_live_write_failure(
                    self.seed,
                )
            }
            ScheduleKind::RuntimeRaftNetworkPartitionFailure => {
                ursula_sim::SimSchedule::generate_runtime_raft_network_partition_failure(self.seed)
            }
        }
    }
}

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
    match family {
        "runtime-interleaving" => Ok((72..=96).map(ScheduleSeed::normal).collect()),
        "runtime-raft-engine" => Ok((97..=101).map(ScheduleSeed::normal).collect()),
        "runtime-raft-network" => Ok((102..=106).map(ScheduleSeed::normal).collect()),
        "runtime-raft-network-recovery" => Ok((107..=111)
            .map(ScheduleSeed::runtime_raft_network_recovery)
            .collect()),
        "runtime-raft-network-cold-live-recovery" => Ok((112..=116)
            .map(ScheduleSeed::runtime_raft_network_cold_live_recovery)
            .collect()),
        "runtime-raft-network-cold-live-restart" => Ok((117..=121)
            .map(ScheduleSeed::runtime_raft_network_cold_live_restart)
            .collect()),
        "runtime-raft-network-cold-live-write-recovery" => Ok((317..=321)
            .map(ScheduleSeed::runtime_raft_network_cold_live_write_recovery)
            .collect()),
        "leader-failover" => Ok((122..=126).map(ScheduleSeed::leader_failover).collect()),
        "runtime-raft-network-leader-failover" => Ok((127..=131)
            .map(ScheduleSeed::runtime_raft_network_leader_failover)
            .collect()),
        "runtime-raft-snapshot-install" => Ok((132..=136).map(ScheduleSeed::normal).collect()),
        "runtime-raft-network-randomized" => Ok((137..=156)
            .map(ScheduleSeed::runtime_raft_network_randomized)
            .collect()),
        "runtime-raft-network-randomized-extended" => Ok((400..=499)
            .map(ScheduleSeed::runtime_raft_network_randomized)
            .collect()),
        "http-protocol-surface-randomized" => Ok((277..=296)
            .map(ScheduleSeed::http_protocol_surface_randomized)
            .collect()),
        "pipeline-smoke-http-protocol-surface-randomized-corruption" => Ok((297..=301)
            .map(ScheduleSeed::http_protocol_surface_randomized_failure)
            .collect()),
        "pipeline-smoke-http-protocol-surface-randomized-sse-corruption" => Ok((302..=306)
            .map(ScheduleSeed::http_protocol_surface_randomized_sse_failure)
            .collect()),
        "pipeline-smoke-http-protocol-surface-randomized-backpressure-corruption" => Ok((307
            ..=311)
            .map(ScheduleSeed::http_protocol_surface_randomized_backpressure_failure)
            .collect()),
        "pipeline-smoke-runtime-raft-network-randomized-read-corruption" => Ok((242..=246)
            .map(ScheduleSeed::runtime_raft_network_randomized_failure)
            .collect()),
        "pipeline-smoke-runtime-raft-network-partial-read-corruption" => Ok((247..=251)
            .map(ScheduleSeed::runtime_raft_network_partial_read_failure)
            .collect()),
        "pipeline-smoke-runtime-raft-network-tail-read-corruption" => Ok((337..=341)
            .map(ScheduleSeed::runtime_raft_network_tail_read_failure)
            .collect()),
        "pipeline-smoke-runtime-raft-network-close-state-corruption" => Ok((342..=346)
            .map(ScheduleSeed::runtime_raft_network_close_failure)
            .collect()),
        "pipeline-smoke-runtime-raft-network-snapshot-corruption" => Ok((347..=351)
            .map(ScheduleSeed::runtime_raft_network_snapshot_failure)
            .collect()),
        "pipeline-smoke-runtime-raft-network-leader-failover-read-corruption" => Ok((252..=256)
            .map(ScheduleSeed::runtime_raft_network_leader_failover_read_failure)
            .collect()),
        "runtime-raft-network-leader-failover-cold-live-read-failures" => Ok((327..=331)
            .map(ScheduleSeed::runtime_raft_network_leader_failover_cold_live_read_failure)
            .collect()),
        "runtime-raft-network-randomized-cold-read-failures" => Ok((322..=326)
            .map(ScheduleSeed::runtime_raft_network_randomized_cold_read_failure)
            .collect()),
        "runtime-raft-snapshot-install-failures" => Ok((232..=236)
            .map(ScheduleSeed::runtime_raft_snapshot_install_failure)
            .collect()),
        "runtime-raft-network-cold-live-truncate-failures" => Ok((222..=226)
            .map(ScheduleSeed::runtime_raft_network_cold_live_truncate_failure)
            .collect()),
        "runtime-raft-network-cold-live-write-failures" => Ok((312..=316)
            .map(ScheduleSeed::runtime_raft_network_cold_live_write_failure)
            .collect()),
        "runtime-raft-network-partition-failures" => Ok((212..=216)
            .map(ScheduleSeed::runtime_raft_network_partition_failure)
            .collect()),
        "pipeline-smoke-runtime-interleaving-read-corruption" => Ok((172..=176)
            .map(ScheduleSeed::runtime_interleaving_failure)
            .collect()),
        "runtime-interleaving-truncate-failures" => Ok((182..=186)
            .map(ScheduleSeed::runtime_interleaving_truncate_failure)
            .collect()),
        "runtime-interleaving-write-failures" => Ok((192..=196)
            .map(ScheduleSeed::runtime_interleaving_write_failure)
            .collect()),
        "pipeline-smoke-http-producer-retry-corruption" => Ok((262..=266)
            .map(ScheduleSeed::http_producer_protocol_surface_failure)
            .collect()),
        "pipeline-smoke-http-live-sse-corruption" => Ok((267..=271)
            .map(ScheduleSeed::http_live_protocol_surface_failure)
            .collect()),
        "pipeline-smoke-http-live-waiter-corruption" => Ok((272..=276)
            .map(ScheduleSeed::http_live_limit_protocol_surface_failure)
            .collect()),
        "pipeline-smoke-http-snapshot-body-corruption" => Ok((332..=336)
            .map(ScheduleSeed::http_snapshot_protocol_surface_failure)
            .collect()),
        "raft-partition-failures" => Ok((202..=206)
            .map(ScheduleSeed::raft_partition_failure)
            .collect()),
        _ => Err(format!("unknown seed family `{family}`").into()),
    }
}

fn usage() -> String {
    format!(
        "{} [--seed N]... [--seed-range START..=END] [--seed-family runtime-interleaving|runtime-raft-engine|runtime-raft-network|runtime-raft-network-recovery|runtime-raft-network-cold-live-recovery|runtime-raft-network-cold-live-restart|runtime-raft-network-cold-live-write-recovery|leader-failover|runtime-raft-network-leader-failover|runtime-raft-snapshot-install|runtime-raft-network-randomized|runtime-raft-network-randomized-extended|http-protocol-surface-randomized|pipeline-smoke-http-protocol-surface-randomized-corruption|pipeline-smoke-http-protocol-surface-randomized-sse-corruption|pipeline-smoke-http-protocol-surface-randomized-backpressure-corruption|pipeline-smoke-runtime-raft-network-randomized-read-corruption|pipeline-smoke-runtime-raft-network-partial-read-corruption|pipeline-smoke-runtime-raft-network-tail-read-corruption|pipeline-smoke-runtime-raft-network-close-state-corruption|pipeline-smoke-runtime-raft-network-snapshot-corruption|pipeline-smoke-runtime-raft-network-leader-failover-read-corruption|runtime-raft-network-randomized-cold-read-failures|runtime-raft-snapshot-install-failures|runtime-raft-network-cold-live-truncate-failures|runtime-raft-network-cold-live-write-failures|runtime-raft-network-partition-failures|pipeline-smoke-runtime-interleaving-read-corruption|runtime-interleaving-truncate-failures|runtime-interleaving-write-failures|pipeline-smoke-http-producer-retry-corruption|pipeline-smoke-http-live-sse-corruption|pipeline-smoke-http-live-waiter-corruption|pipeline-smoke-http-snapshot-body-corruption|raft-partition-failures] [--failure-dir DIR] [--inject-panic-seed N] [--runtime-panic-after EVENT --panic-message TEXT --runtime-invariant NAME] [--runtime-corrupt-read-client CLIENT_ID] [--cold-corrupt-read-node NODE_ID] [--write-artifacts] [--expect-failures]",
        bin_name()
    )
}

fn bin_name() -> String {
    env::args()
        .next()
        .unwrap_or_else(|| "ursula-sim-smoke".to_owned())
}

#[cfg(madsim)]
#[derive(serde::Serialize)]
struct FailedSeedArtifact {
    schema_version: u32,
    seed: u64,
    schedule: ursula_sim::SimSchedule,
    stable_trace_path: String,
    raw_event_log_path: String,
    panic: String,
}

#[cfg(madsim)]
#[derive(serde::Serialize)]
struct StableTraceArtifact {
    schema_version: u32,
    schedule: ursula_sim::SimSchedule,
    stable_trace: ursula_sim::SimTrace,
}

#[cfg(madsim)]
#[derive(serde::Serialize)]
struct RawEventLogArtifact {
    schema_version: u32,
    seed: u64,
    events: Vec<ursula_sim::SimEvent>,
}

#[cfg(madsim)]
fn apply_runtime_panic_after(
    schedule: &mut ursula_sim::SimSchedule,
    after_event: Option<&String>,
    message: Option<&String>,
    invariant: Option<&String>,
) -> Result<(), Box<dyn Error>> {
    let Some(after_event) = after_event else {
        return Ok(());
    };
    if schedule.scenario != ursula_sim::SimScenario::RuntimeSeededInterleaving {
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

#[cfg(madsim)]
fn apply_runtime_corrupt_read_client(
    schedule: &mut ursula_sim::SimSchedule,
    client_id: Option<usize>,
) -> Result<(), Box<dyn Error>> {
    let Some(client_id) = client_id else {
        return Ok(());
    };
    if schedule.scenario != ursula_sim::SimScenario::RuntimeSeededInterleaving {
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

#[cfg(madsim)]
fn apply_cold_corrupt_read_node(
    schedule: &mut ursula_sim::SimSchedule,
    node_id: Option<u64>,
) -> Result<(), Box<dyn Error>> {
    let Some(node_id) = node_id else {
        return Ok(());
    };
    if schedule.scenario != ursula_sim::SimScenario::ColdLiveRead {
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

#[cfg(madsim)]
fn write_success_artifacts(
    failure_dir: &std::path::Path,
    record: &ursula_sim::SimScheduledRecord,
) -> Result<(), Box<dyn Error>> {
    use std::fs;

    let stable_path = failure_dir.join(format!("seed-{}-replay.json", record.schedule.seed));
    let raw_path = failure_dir.join(format!("seed-{}-raw-events.json", record.schedule.seed));
    let mut stable_body = serde_json::to_string_pretty(record)?;
    stable_body.push('\n');
    fs::write(stable_path, stable_body)?;

    let mut raw_body = serde_json::to_string_pretty(&RawEventLogArtifact {
        schema_version: ursula_sim::SIM_REGRESSION_SCHEMA_VERSION,
        seed: record.schedule.seed,
        events: ursula_sim::SimTrace::last_recorded().events,
    })?;
    raw_body.push('\n');
    fs::write(raw_path, raw_body)?;
    Ok(())
}

#[cfg(madsim)]
fn panic_payload_to_string(payload: Box<dyn std::any::Any + Send>) -> String {
    if let Some(message) = payload.downcast_ref::<&str>() {
        (*message).to_owned()
    } else if let Some(message) = payload.downcast_ref::<String>() {
        message.clone()
    } else {
        "non-string panic payload".to_owned()
    }
}

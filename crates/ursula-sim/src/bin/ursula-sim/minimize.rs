//! `ursula-sim minimize`: shrink a failing schedule while preserving a
//! target predicate.
//!
//! One generic greedy shrink loop and one generic minimize driver serve every
//! scenario; the scenario-specific knowledge lives entirely in the
//! candidate-schedule (mutation operator) functions.

use std::env;
use std::error::Error;
use std::path::PathBuf;

use ursula_sim::artifact::FailedSeedArtifact;
use ursula_sim::artifact::StableTraceArtifact;
use ursula_sim::artifact::invariant_failed;
use ursula_sim::artifact::run_schedule_capturing_panic;
use ursula_sim::artifact::stable_trace;

pub fn run(args: Vec<String>) -> Result<(), Box<dyn Error>> {
    crate::init_stderr_tracing();
    let args = Args::parse(args)?;
    let request = MinimizeRequest::from_artifact(args.artifact, args.target_overrides)?;
    let encoded = if args.list_candidates {
        serde_json::to_string_pretty(&list_candidate_schedules(&request)?)?
    } else if let Some(mutation) = args.probe_mutation {
        serde_json::to_string_pretty(&probe_candidate_schedule(request, &mutation)?)?
    } else if args.shrink_only {
        serde_json::to_string_pretty(&minimize_schedule_shrink_only(request)?)?
    } else {
        serde_json::to_string_pretty(&minimize_schedule(request)?)?
    };
    let mut encoded = encoded;
    encoded.push('\n');
    match args.output {
        Some(path) => std::fs::write(path, encoded)?,
        None => print!("{encoded}"),
    }
    Ok(())
}

struct Args {
    artifact: PathBuf,
    output: Option<PathBuf>,
    list_candidates: bool,
    probe_mutation: Option<String>,
    shrink_only: bool,
    target_overrides: TargetOverrides,
}

impl Args {
    fn parse(args: Vec<String>) -> Result<Self, Box<dyn Error>> {
        let mut artifact = None;
        let mut output = None;
        let mut list_candidates = false;
        let mut probe_mutation = None;
        let mut shrink_only = false;
        let mut target_overrides = TargetOverrides::default();
        let mut args = args.into_iter();

        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--artifact" => {
                    artifact = Some(PathBuf::from(
                        args.next().ok_or_else(|| format!("usage: {USAGE}"))?,
                    ));
                }
                "--output" => {
                    output = Some(PathBuf::from(
                        args.next().ok_or_else(|| format!("usage: {USAGE}"))?,
                    ));
                }
                "--list-candidates" => {
                    list_candidates = true;
                }
                "--shrink-only" => {
                    shrink_only = true;
                }
                "--probe-mutation" => {
                    probe_mutation = Some(args.next().ok_or_else(|| format!("usage: {USAGE}"))?);
                }
                "--panic-contains" => {
                    target_overrides.panic_contains =
                        Some(args.next().ok_or_else(|| format!("usage: {USAGE}"))?);
                }
                "--event" => {
                    target_overrides.event_name =
                        Some(args.next().ok_or_else(|| format!("usage: {USAGE}"))?);
                }
                "--event-min-count" => {
                    target_overrides.event_min_count = Some(
                        args.next()
                            .ok_or_else(|| format!("usage: {USAGE}"))?
                            .parse::<usize>()?,
                    );
                }
                "--invariant" => {
                    target_overrides.invariant =
                        Some(args.next().ok_or_else(|| format!("usage: {USAGE}"))?);
                }
                "--stable-prefix" => {
                    target_overrides.stable_prefix_len = Some(
                        args.next()
                            .ok_or_else(|| format!("usage: {USAGE}"))?
                            .parse::<usize>()?,
                    );
                }
                "--stable-exact" => {
                    target_overrides.stable_exact = true;
                }
                "--help" | "-h" => {
                    println!("{USAGE}");
                    std::process::exit(0);
                }
                _ => return Err(format!("unknown argument `{arg}`\nusage: {USAGE}").into()),
            }
        }

        target_overrides.validate()?;
        Ok(Self {
            artifact: artifact.ok_or_else(|| format!("usage: {USAGE}"))?,
            output,
            list_candidates,
            probe_mutation,
            shrink_only,
            target_overrides,
        })
    }
}

#[derive(Default)]
struct TargetOverrides {
    panic_contains: Option<String>,
    event_name: Option<String>,
    event_min_count: Option<usize>,
    invariant: Option<String>,
    stable_prefix_len: Option<usize>,
    stable_exact: bool,
}

impl TargetOverrides {
    fn validate(&self) -> Result<(), Box<dyn Error>> {
        let selected = usize::from(self.panic_contains.is_some())
            + usize::from(self.event_name.is_some())
            + usize::from(self.invariant.is_some())
            + usize::from(self.stable_prefix_len.is_some())
            + usize::from(self.stable_exact);
        if selected > 1 {
            return Err(format!("choose only one minimization target\nusage: {USAGE}").into());
        }
        if self.event_min_count.is_some() && self.event_name.is_none() {
            return Err(format!("--event-min-count requires --event\nusage: {USAGE}").into());
        }
        Ok(())
    }
}

const USAGE: &str = "ursula-sim minimize --artifact PATH [--output output.json] [--list-candidates] [--probe-mutation NAME] [--shrink-only] [--panic-contains TEXT] [--event EVENT --event-min-count N] [--invariant NAME] [--stable-prefix N] [--stable-exact]";

struct MinimizeRequest {
    schedule: ursula_sim::SimSchedule,
    target: TargetPredicate,
}

impl MinimizeRequest {
    fn from_artifact(path: PathBuf, overrides: TargetOverrides) -> Result<Self, Box<dyn Error>> {
        let body = std::fs::read_to_string(&path)?;
        if let Ok(record) = serde_json::from_str::<ursula_sim::SimScheduledRecord>(&body) {
            let stable_trace = stable_trace(record.outcome.trace);
            return Ok(Self {
                schedule: record.schedule,
                target: target_from_stable_trace(stable_trace, overrides),
            });
        }
        if let Ok(artifact) = serde_json::from_str::<FailedSeedArtifact>(&body) {
            return Ok(Self {
                schedule: artifact.schedule,
                target: target_from_failed_artifact(artifact.panic, overrides),
            });
        }
        if let Ok(artifact) = serde_json::from_str::<StableTraceArtifact>(&body) {
            return Ok(Self {
                schedule: artifact.schedule,
                target: target_from_stable_trace(artifact.stable_trace, overrides),
            });
        }
        Err(format!("unsupported minimization artifact `{}`", path.display()).into())
    }
}

fn target_from_stable_trace(
    trace: ursula_sim::SimTrace,
    overrides: TargetOverrides,
) -> TargetPredicate {
    if let Some(value) = overrides.panic_contains {
        return TargetPredicate::PanicContains { value };
    }
    if let Some(event) = overrides.event_name {
        return TargetPredicate::EventCountAtLeast {
            event,
            min_count: overrides.event_min_count.unwrap_or(1),
        };
    }
    if let Some(invariant) = overrides.invariant {
        return TargetPredicate::InvariantFailed { invariant };
    }
    if let Some(prefix_len) = overrides.stable_prefix_len {
        let mut prefix = trace;
        prefix.events.truncate(prefix_len);
        return TargetPredicate::StableTracePrefix { trace: prefix };
    }
    if overrides.stable_exact {
        return TargetPredicate::StableTraceExact { trace };
    }
    TargetPredicate::StableTraceExact { trace }
}

fn target_from_failed_artifact(panic: String, overrides: TargetOverrides) -> TargetPredicate {
    if let Some(value) = overrides.panic_contains {
        return TargetPredicate::PanicContains { value };
    }
    if let Some(event) = overrides.event_name {
        return TargetPredicate::EventCountAtLeast {
            event,
            min_count: overrides.event_min_count.unwrap_or(1),
        };
    }
    if let Some(invariant) = overrides.invariant {
        return TargetPredicate::InvariantFailed { invariant };
    }
    TargetPredicate::PanicContains { value: panic }
}

#[derive(Clone, serde::Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum TargetPredicate {
    PanicContains { value: String },
    EventCountAtLeast { event: String, min_count: usize },
    InvariantFailed { invariant: String },
    StableTraceExact { trace: ursula_sim::SimTrace },
    StableTracePrefix { trace: ursula_sim::SimTrace },
}

#[derive(serde::Serialize)]
struct MinimizeReport {
    schema_version: u32,
    seed: u64,
    scenario: ursula_sim::SimScenario,
    target: MinimizeTarget,
    candidates: Vec<MinimizeCandidate>,
    accepted_reductions: Vec<MinimizeCandidate>,
    minimized: MinimizedSchedule,
}

#[derive(serde::Serialize)]
struct MinimizeTarget {
    predicate: TargetPredicate,
    original_schedule: ursula_sim::SimSchedule,
}

#[derive(serde::Serialize)]
struct CandidateListReport {
    schema_version: u32,
    seed: u64,
    scenario: ursula_sim::SimScenario,
    candidates: Vec<CandidateSchedule>,
}

#[derive(serde::Serialize)]
struct CandidateProbeReport {
    schema_version: u32,
    seed: u64,
    scenario: ursula_sim::SimScenario,
    target: TargetPredicate,
    candidate: MinimizeCandidate,
    record: Option<ursula_sim::SimScheduledRecord>,
    failure: Option<MinimizedFailureArtifact>,
}

#[derive(serde::Serialize)]
struct CandidateSchedule {
    mutation: String,
    schedule: ursula_sim::SimSchedule,
}

#[derive(serde::Serialize)]
struct MinimizeCandidate {
    mutation: String,
    schedule: ursula_sim::SimSchedule,
    outcome: CandidateOutcome,
}

#[derive(serde::Serialize)]
struct MinimizedSchedule {
    schedule: ursula_sim::SimSchedule,
    outcome: CandidateOutcome,
    record: Option<ursula_sim::SimScheduledRecord>,
    failure: Option<MinimizedFailureArtifact>,
}

#[derive(serde::Serialize)]
struct MinimizedFailureArtifact {
    schema_version: u32,
    seed: u64,
    schedule: ursula_sim::SimSchedule,
    panic: String,
    stable_trace: ursula_sim::SimTrace,
    raw_event_log: Vec<ursula_sim::SimEvent>,
}

#[derive(serde::Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
enum CandidateOutcome {
    Passed {
        target_preserved: bool,
    },
    ReproducedPanic {
        panic: String,
        target_preserved: bool,
    },
}

impl CandidateOutcome {
    fn target_preserved(&self) -> bool {
        match self {
            Self::Passed { target_preserved }
            | Self::ReproducedPanic {
                target_preserved, ..
            } => *target_preserved,
        }
    }
}

fn list_candidate_schedules(
    request: &MinimizeRequest,
) -> Result<CandidateListReport, Box<dyn Error>> {
    Ok(CandidateListReport {
        schema_version: ursula_sim::SIM_REGRESSION_SCHEMA_VERSION,
        seed: request.schedule.seed,
        scenario: request.schedule.scenario,
        candidates: candidate_schedules_for(&request.schedule)?
            .into_iter()
            .map(|(mutation, schedule)| CandidateSchedule { mutation, schedule })
            .collect(),
    })
}

fn probe_candidate_schedule(
    request: MinimizeRequest,
    mutation_name: &str,
) -> Result<CandidateProbeReport, Box<dyn Error>> {
    let (mutation, schedule) = candidate_schedules_for(&request.schedule)?
        .into_iter()
        .find(|(mutation, _)| mutation == mutation_name)
        .ok_or_else(|| format!("candidate mutation `{mutation_name}` not found"))?;
    let (outcome, record, failure) = run_minimized_schedule(&schedule, &request.target);
    Ok(CandidateProbeReport {
        schema_version: ursula_sim::SIM_REGRESSION_SCHEMA_VERSION,
        seed: request.schedule.seed,
        scenario: request.schedule.scenario,
        target: request.target,
        candidate: MinimizeCandidate {
            mutation,
            schedule,
            outcome,
        },
        record,
        failure,
    })
}

fn candidate_schedules_for(
    schedule: &ursula_sim::SimSchedule,
) -> Result<Vec<(String, ursula_sim::SimSchedule)>, Box<dyn Error>> {
    let candidates = match schedule.scenario {
        ursula_sim::SimScenario::RuntimeSeededInterleaving => {
            let original_plan = runtime_interleaving_plan(schedule)
                .ok_or("runtime interleaving schedule does not contain a plan")?;
            candidate_plans(&original_plan)
                .into_iter()
                .map(|(mutation, plan)| (mutation, schedule_with_plan(schedule, plan)))
                .collect()
        }
        ursula_sim::SimScenario::PartitionHeal => raft_partition_candidate_schedules(schedule),
        ursula_sim::SimScenario::RuntimeRaftNetwork => {
            if let Some(fault) = cold_path_fault(schedule) {
                cold_path_candidate_schedules(schedule, &fault)
            } else {
                runtime_raft_network_candidate_schedules(schedule)
            }
        }
        ursula_sim::SimScenario::RuntimeRaftSnapshotInstall => {
            runtime_raft_snapshot_install_candidate_schedules(schedule)
        }
        ursula_sim::SimScenario::HttpProducerProtocolSurface => {
            http_producer_protocol_surface_candidate_schedules(schedule)
        }
        ursula_sim::SimScenario::HttpProtocolSurface => {
            http_snapshot_protocol_surface_candidate_schedules(schedule)
        }
        ursula_sim::SimScenario::HttpLiveLimitProtocolSurface => {
            http_live_limit_protocol_surface_candidate_schedules(schedule)
        }
        ursula_sim::SimScenario::HttpLiveProtocolSurface => {
            http_live_protocol_surface_candidate_schedules(schedule)
        }
        ursula_sim::SimScenario::HttpProtocolSurfaceRandomized => {
            http_protocol_surface_randomized_candidate_schedules(schedule)
        }
        ursula_sim::SimScenario::ColdLiveRead
        | ursula_sim::SimScenario::ColdReadFault
        | ursula_sim::SimScenario::ColdWriteFault
        | ursula_sim::SimScenario::ColdWriteDelay
        | ursula_sim::SimScenario::ColdDeleteFault
        | ursula_sim::SimScenario::ColdReadDelay
        | ursula_sim::SimScenario::ColdReadTruncate => {
            let fault = cold_path_fault(schedule)
                .ok_or("schedule does not contain a reducible cold path fault")?;
            cold_path_candidate_schedules(schedule, &fault)
        }
        _ => {
            return Err(
                format!("{:?} schedules cannot be minimized yet", schedule.scenario).into(),
            );
        }
    };
    Ok(candidates)
}

/// Like [`candidate_schedules_for`], but errors when the scenario's reducible
/// corruption fault is absent — full minimization requires one, while
/// `--list-candidates` and `--shrink-only` tolerate its absence.
fn checked_candidate_schedules(
    schedule: &ursula_sim::SimSchedule,
) -> Result<Vec<(String, ursula_sim::SimSchedule)>, Box<dyn Error>> {
    let missing_corruption = match schedule.scenario {
        ursula_sim::SimScenario::HttpProducerProtocolSurface
            if !http_producer_protocol_surface_has_corruption(schedule) =>
        {
            Some(
                "http producer protocol surface schedule does not contain a reducible duplicate expectation corruption fault",
            )
        }
        ursula_sim::SimScenario::HttpProtocolSurface
            if !http_snapshot_protocol_surface_has_corruption(schedule) =>
        {
            Some(
                "http snapshot protocol surface schedule does not contain a reducible snapshot body expectation corruption fault",
            )
        }
        ursula_sim::SimScenario::HttpLiveProtocolSurface
            if !http_live_protocol_surface_has_corruption(schedule) =>
        {
            Some(
                "http live protocol surface schedule does not contain a reducible SSE next-offset expectation corruption fault",
            )
        }
        ursula_sim::SimScenario::HttpLiveLimitProtocolSurface
            if !http_live_limit_protocol_surface_has_corruption(schedule) =>
        {
            Some(
                "http live-limit protocol surface schedule does not contain a reducible backpressure expectation corruption fault",
            )
        }
        ursula_sim::SimScenario::HttpProtocolSurfaceRandomized
            if !http_protocol_surface_randomized_has_corruption(schedule) =>
        {
            Some(
                "randomized http protocol surface schedule does not contain a reducible final-read expectation corruption fault",
            )
        }
        ursula_sim::SimScenario::RuntimeRaftSnapshotInstall
            if !runtime_raft_snapshot_install_has_corruption(schedule) =>
        {
            Some(
                "runtime raft snapshot install schedule does not contain a reducible snapshot corruption fault",
            )
        }
        _ => None,
    };
    if let Some(message) = missing_corruption {
        return Err(message.into());
    }
    candidate_schedules_for(schedule)
}

/// Generic minimize driver: probe every first-generation candidate, then run
/// the greedy shrink loop.
fn minimize_schedule(request: MinimizeRequest) -> Result<MinimizeReport, Box<dyn Error>> {
    let mut candidates = Vec::new();
    for (mutation, schedule) in checked_candidate_schedules(&request.schedule)? {
        candidates.push(MinimizeCandidate {
            mutation,
            outcome: run_candidate(&schedule, &request.target),
            schedule,
        });
    }
    let (accepted_reductions, minimized) = shrink_schedule(&request.schedule, &request.target);

    Ok(MinimizeReport {
        schema_version: ursula_sim::SIM_REGRESSION_SCHEMA_VERSION,
        seed: request.schedule.seed,
        scenario: request.schedule.scenario,
        target: MinimizeTarget {
            predicate: request.target,
            original_schedule: request.schedule,
        },
        candidates,
        accepted_reductions,
        minimized,
    })
}

fn minimize_schedule_shrink_only(
    request: MinimizeRequest,
) -> Result<MinimizeReport, Box<dyn Error>> {
    let MinimizeRequest { schedule, target } = request;
    // Surface unsupported-scenario and missing-plan/fault errors up front;
    // the shrink loop itself treats them as "no candidates left".
    candidate_schedules_for(&schedule)?;
    let (accepted_reductions, minimized) = shrink_schedule(&schedule, &target);

    Ok(MinimizeReport {
        schema_version: ursula_sim::SIM_REGRESSION_SCHEMA_VERSION,
        seed: schedule.seed,
        scenario: schedule.scenario,
        target: MinimizeTarget {
            predicate: target,
            original_schedule: schedule,
        },
        candidates: Vec::new(),
        accepted_reductions,
        minimized,
    })
}

/// Generic greedy shrink loop: repeatedly accept the first candidate mutation
/// that preserves the target, re-deriving the candidate set from the current
/// schedule, until no candidate preserves it.
fn shrink_schedule(
    original: &ursula_sim::SimSchedule,
    target: &TargetPredicate,
) -> (Vec<MinimizeCandidate>, MinimizedSchedule) {
    let mut current_schedule = original.clone();
    let mut accepted = Vec::new();

    while let Some(next) = candidate_schedules_for(&current_schedule)
        .unwrap_or_default()
        .into_iter()
        .find_map(|(mutation, schedule)| {
            trace_minimize_candidate(&mutation);
            let outcome = run_candidate(&schedule, target);
            outcome.target_preserved().then_some(MinimizeCandidate {
                mutation,
                schedule,
                outcome,
            })
        })
    {
        current_schedule = next.schedule.clone();
        accepted.push(next);
    }

    let (outcome, record, failure) = run_minimized_schedule(&current_schedule, target);
    (accepted, MinimizedSchedule {
        schedule: current_schedule,
        outcome,
        record,
        failure,
    })
}

fn trace_minimize_candidate(mutation: &str) {
    if env::var_os("URSULA_SIM_MINIMIZE_TRACE").is_some() {
        tracing::info!("ursula-sim minimize: running candidate `{mutation}`");
    }
}

fn candidate_plans(
    plan: &ursula_sim::RuntimeInterleavingPlan,
) -> Vec<(String, ursula_sim::RuntimeInterleavingPlan)> {
    let mut candidates = Vec::new();
    if plan.clients.len() > 1 {
        for index in 0..plan.clients.len() {
            let reduced = remove_runtime_client(plan, index);
            candidates.push((format!("remove_client_{index}"), reduced));
        }
    }
    for index in 0..plan.clients.len() {
        if plan.clients[index].first_append_delay_ms != 0 {
            let mut reduced = plan.clone();
            reduced.clients[index].first_append_delay_ms = 0;
            candidates.push((format!("zero_client_{index}_first_append_delay"), reduced));
        }

        if plan.clients[index].second_append_delay_ms != 0 {
            let mut reduced = plan.clone();
            reduced.clients[index].second_append_delay_ms = 0;
            candidates.push((format!("zero_client_{index}_second_append_delay"), reduced));
        }
    }
    if plan.flush_delay_ms != 0 {
        let mut reduced = plan.clone();
        reduced.flush_delay_ms = 0;
        candidates.push(("zero_flush_delay".to_owned(), reduced));
    }

    if plan.read_verify_delay_ms != 0 {
        let mut reduced = plan.clone();
        reduced.read_verify_delay_ms = 0;
        candidates.push(("zero_read_verify_delay".to_owned(), reduced));
    }

    if plan.flush_group_limit > 1 {
        for limit in 1..plan.flush_group_limit {
            let mut reduced = plan.clone();
            reduced.flush_group_limit = limit;
            candidates.push((format!("set_flush_group_limit_{limit}"), reduced));
        }
    }

    if let Some(delay_ms) = plan.runtime_cold_read_delay_ms {
        let mut reduced = plan.clone();
        reduced.runtime_cold_read_delay_ms = None;
        candidates.push(("remove_runtime_cold_read_delay".to_owned(), reduced));

        for candidate_delay_ms in smaller_delay_candidates(delay_ms) {
            let mut reduced = plan.clone();
            reduced.runtime_cold_read_delay_ms = Some(candidate_delay_ms);
            candidates.push((
                format!("set_runtime_cold_read_delay_ms_{candidate_delay_ms}"),
                reduced,
            ));
        }
    }

    if let Some(returned_len) = plan.runtime_cold_read_truncate_len {
        let mut reduced = plan.clone();
        reduced.runtime_cold_read_truncate_len = None;
        candidates.push(("remove_runtime_cold_read_truncate".to_owned(), reduced));

        for candidate_len in 0..returned_len {
            let mut reduced = plan.clone();
            reduced.runtime_cold_read_truncate_len = Some(candidate_len);
            candidates.push((
                format!("set_runtime_cold_read_truncate_len_{candidate_len}"),
                reduced,
            ));
        }
    }

    if plan.runtime_cold_write_failure.is_some() {
        let mut reduced = plan.clone();
        reduced.runtime_cold_write_failure = None;
        candidates.push(("remove_runtime_cold_write_failure".to_owned(), reduced));
    }

    if let Some(corrupt_client_id) = plan.corrupt_read_client_id {
        let mut reduced = plan.clone();
        reduced.corrupt_read_client_id = None;
        candidates.push(("remove_corrupt_read_client".to_owned(), reduced));

        for client_id in plan
            .clients
            .iter()
            .map(|client| client.client_id)
            .filter(|candidate| *candidate < corrupt_client_id)
        {
            let mut reduced = plan.clone();
            reduced.corrupt_read_client_id = Some(client_id);
            candidates.push((format!("set_corrupt_read_client_{client_id}"), reduced));
        }
    }

    if let Some(panic_after) = &plan.panic_after {
        let mut reduced = plan.clone();
        reduced.panic_after = None;
        candidates.push(("remove_runtime_panic_after".to_owned(), reduced));

        if panic_after.after_event == "runtime_interleaving_verified" {
            let mut reduced = plan.clone();
            if let Some(panic_after) = &mut reduced.panic_after {
                panic_after.after_event = "runtime_interleaving_flush_completed".to_owned();
            }
            candidates.push((
                "move_runtime_panic_after_to_flush_completed".to_owned(),
                reduced,
            ));
        }
    }

    if plan
        .panic_after
        .as_ref()
        .is_some_and(|panic_after| panic_after.invariant.is_some())
    {
        let mut reduced = plan.clone();
        if let Some(panic_after) = &mut reduced.panic_after {
            panic_after.invariant = None;
        }
        candidates.push(("remove_runtime_panic_invariant".to_owned(), reduced));
    }
    candidates
}

fn cold_path_candidate_schedules(
    original: &ursula_sim::SimSchedule,
    current_fault: &ColdPathFault,
) -> Vec<(String, ursula_sim::SimSchedule)> {
    let mut candidates = vec![(
        format!("remove_{}", current_fault.mutation_name()),
        schedule_with_cold_path_fault(original, None),
    )];
    match current_fault {
        ColdPathFault::CorruptReadExpectation {
            node_id: current_node,
        } => {
            for node_id in 1..*current_node {
                candidates.push((
                    format!("set_corrupt_cold_live_read_node_{node_id}"),
                    schedule_with_cold_path_fault(
                        original,
                        Some(ColdPathFault::CorruptReadExpectation { node_id }),
                    ),
                ));
            }
        }
        ColdPathFault::FailNextColdRead => {}
        ColdPathFault::FailNextColdWrite => {}
        ColdPathFault::FailNextColdDelete => {}
        ColdPathFault::DelayNextColdWrite {
            delay_ms: current_delay_ms,
        } => {
            for delay_ms in smaller_delay_candidates(*current_delay_ms) {
                candidates.push((
                    format!("set_delay_next_cold_write_ms_{delay_ms}"),
                    schedule_with_cold_path_fault(
                        original,
                        Some(ColdPathFault::DelayNextColdWrite { delay_ms }),
                    ),
                ));
            }
        }
        ColdPathFault::TruncateNextColdRead {
            returned_len: current_len,
        } => {
            for returned_len in 0..*current_len {
                candidates.push((
                    format!("set_truncate_next_cold_read_returned_len_{returned_len}"),
                    schedule_with_cold_path_fault(
                        original,
                        Some(ColdPathFault::TruncateNextColdRead { returned_len }),
                    ),
                ));
            }
        }
        ColdPathFault::DelayNextColdRead {
            delay_ms: current_delay_ms,
        } => {
            for delay_ms in smaller_delay_candidates(*current_delay_ms) {
                candidates.push((
                    format!("set_delay_next_cold_read_ms_{delay_ms}"),
                    schedule_with_cold_path_fault(
                        original,
                        Some(ColdPathFault::DelayNextColdRead { delay_ms }),
                    ),
                ));
            }
        }
    }
    if original.scenario == ursula_sim::SimScenario::RuntimeRaftNetwork {
        candidates.extend(
            runtime_raft_network_candidate_schedules(original)
                .into_iter()
                .filter(|(mutation, _)| mutation != "remove_runtime_raft_cold_live_read"),
        );
    }
    candidates
}

fn smaller_delay_candidates(delay_ms: u64) -> Vec<u64> {
    [1, 10, 50, 100]
        .into_iter()
        .filter(|candidate| *candidate < delay_ms)
        .collect()
}

fn remove_runtime_client(
    plan: &ursula_sim::RuntimeInterleavingPlan,
    remove_index: usize,
) -> ursula_sim::RuntimeInterleavingPlan {
    let mut reduced = plan.clone();
    let removed_client_id = reduced.clients[remove_index].client_id;
    reduced.clients.remove(remove_index);

    let old_corrupt_client_id = reduced.corrupt_read_client_id;
    reduced.corrupt_read_client_id = None;
    for (new_index, client) in reduced.clients.iter_mut().enumerate() {
        let old_client_id = client.client_id;
        client.client_id = new_index;
        client.stream_index = new_index;
        if old_corrupt_client_id == Some(old_client_id) {
            reduced.corrupt_read_client_id = Some(new_index);
        }
    }
    if old_corrupt_client_id == Some(removed_client_id) {
        reduced.corrupt_read_client_id = None;
    }
    reduced
}

fn schedule_with_plan(
    original: &ursula_sim::SimSchedule,
    plan: ursula_sim::RuntimeInterleavingPlan,
) -> ursula_sim::SimSchedule {
    ursula_sim::SimSchedule {
        seed: original.seed,
        scenario: original.scenario,
        stream: original.stream.clone(),
        fault_plan: ursula_sim::SimFaultPlan {
            steps: vec![ursula_sim::SimFaultStep {
                phase: "seeded_runtime_interleaving".to_owned(),
                action: ursula_sim::SimFaultAction::RunRuntimeSeededInterleaving { plan },
            }],
        },
    }
}

fn raft_partition_candidate_schedules(
    original: &ursula_sim::SimSchedule,
) -> Vec<(String, ursula_sim::SimSchedule)> {
    let has_partition = original.fault_plan.steps.iter().any(|step| {
        matches!(
            step.action,
            ursula_sim::SimFaultAction::PartitionSeededFollower
        )
    });
    let has_heal = original
        .fault_plan
        .steps
        .iter()
        .any(|step| matches!(step.action, ursula_sim::SimFaultAction::HealSeededFollower));
    let mut candidates = Vec::new();
    if has_partition {
        let mut schedule = original.clone();
        schedule.fault_plan.steps.retain(|step| {
            !matches!(
                step.action,
                ursula_sim::SimFaultAction::PartitionSeededFollower
            )
        });
        candidates.push(("remove_partition_seeded_follower".to_owned(), schedule));
    }
    if has_heal {
        let mut schedule = original.clone();
        schedule
            .fault_plan
            .steps
            .retain(|step| !matches!(step.action, ursula_sim::SimFaultAction::HealSeededFollower));
        candidates.push(("remove_heal_seeded_follower".to_owned(), schedule));
    }
    if has_partition && !has_heal {
        let mut schedule = original.clone();
        schedule.fault_plan.steps.push(ursula_sim::SimFaultStep {
            phase: "after_isolated_lag".to_owned(),
            action: ursula_sim::SimFaultAction::HealSeededFollower,
        });
        candidates.push(("add_heal_seeded_follower".to_owned(), schedule));
    }
    candidates
}

fn runtime_raft_network_candidate_schedules(
    original: &ursula_sim::SimSchedule,
) -> Vec<(String, ursula_sim::SimSchedule)> {
    let mut candidates = raft_partition_candidate_schedules(original);
    candidates.extend(orphan_cold_retry_candidate_schedules(original));
    let has_runtime_cold_live_read = original.fault_plan.steps.iter().any(|step| {
        matches!(
            step.action,
            ursula_sim::SimFaultAction::VerifyRuntimeColdLiveReads
        )
    });
    let has_runtime_cold_live_restart = original.fault_plan.steps.iter().any(|step| {
        matches!(
            step.action,
            ursula_sim::SimFaultAction::StopSeededFollower
                | ursula_sim::SimFaultAction::RestartStoppedFollower
        )
    });
    if has_runtime_cold_live_restart {
        let mut schedule = original.clone();
        schedule.fault_plan.steps.retain(|step| {
            !matches!(
                step.action,
                ursula_sim::SimFaultAction::StopSeededFollower
                    | ursula_sim::SimFaultAction::RestartStoppedFollower
            )
        });
        candidates.push(("remove_runtime_raft_cold_live_restart".to_owned(), schedule));
    }
    if has_runtime_cold_live_read {
        let mut schedule = original.clone();
        schedule.fault_plan.steps.retain(|step| {
            !matches!(
                step.action,
                ursula_sim::SimFaultAction::VerifyRuntimeColdLiveReads
                    | ursula_sim::SimFaultAction::StopSeededFollower
                    | ursula_sim::SimFaultAction::RestartStoppedFollower
                    | ursula_sim::SimFaultAction::FailNextColdWrite
                    | ursula_sim::SimFaultAction::RetryColdWriteAfterFailure
                    | ursula_sim::SimFaultAction::TruncateNextColdRead { .. }
                    | ursula_sim::SimFaultAction::RetryColdReadAfterFailure
            )
        });
        candidates.push(("remove_runtime_raft_cold_live_read".to_owned(), schedule));
    }
    let has_stop_current_leader = original
        .fault_plan
        .steps
        .iter()
        .any(|step| matches!(step.action, ursula_sim::SimFaultAction::StopCurrentLeader));
    let has_restart_stopped_leader = original.fault_plan.steps.iter().any(|step| {
        matches!(
            step.action,
            ursula_sim::SimFaultAction::RestartStoppedLeader
        )
    });
    if has_stop_current_leader || has_restart_stopped_leader {
        let mut schedule = original.clone();
        schedule.fault_plan.steps.retain(|step| {
            !matches!(
                step.action,
                ursula_sim::SimFaultAction::StopCurrentLeader
                    | ursula_sim::SimFaultAction::RestartStoppedLeader
            )
        });
        candidates.push(("remove_runtime_raft_leader_failover".to_owned(), schedule));
    }
    candidates.extend(runtime_raft_network_workload_candidate_schedules(original));
    if runtime_raft_network_workload_is_multistream(original) {
        candidates.push((
            "shrink_runtime_raft_workload_to_single_stream".to_owned(),
            runtime_raft_network_single_stream_schedule(original),
        ));
    }
    candidates
}

fn orphan_cold_retry_candidate_schedules(
    original: &ursula_sim::SimSchedule,
) -> Vec<(String, ursula_sim::SimSchedule)> {
    let has_cold_write_fault = original
        .fault_plan
        .steps
        .iter()
        .any(|step| matches!(step.action, ursula_sim::SimFaultAction::FailNextColdWrite));
    let has_cold_read_fault = original.fault_plan.steps.iter().any(|step| {
        matches!(
            step.action,
            ursula_sim::SimFaultAction::FailNextColdRead
                | ursula_sim::SimFaultAction::TruncateNextColdRead { .. }
        )
    });
    let mut candidates = Vec::new();
    if !has_cold_write_fault
        && original.fault_plan.steps.iter().any(|step| {
            matches!(
                step.action,
                ursula_sim::SimFaultAction::RetryColdWriteAfterFailure
            )
        })
    {
        let mut schedule = original.clone();
        schedule.fault_plan.steps.retain(|step| {
            !matches!(
                step.action,
                ursula_sim::SimFaultAction::RetryColdWriteAfterFailure
            )
        });
        candidates.push(("remove_orphan_cold_write_retry".to_owned(), schedule));
    }
    if !has_cold_read_fault
        && original.fault_plan.steps.iter().any(|step| {
            matches!(
                step.action,
                ursula_sim::SimFaultAction::RetryColdReadAfterFailure
            )
        })
    {
        let mut schedule = original.clone();
        schedule.fault_plan.steps.retain(|step| {
            !matches!(
                step.action,
                ursula_sim::SimFaultAction::RetryColdReadAfterFailure
            )
        });
        candidates.push(("remove_orphan_cold_read_retry".to_owned(), schedule));
    }
    candidates
}

fn runtime_raft_network_single_stream_schedule(
    original: &ursula_sim::SimSchedule,
) -> ursula_sim::SimSchedule {
    let mut schedule = original.clone();
    for step in &mut schedule.fault_plan.steps {
        if let ursula_sim::SimFaultAction::RunRuntimeRaftNetworkWorkload { plan } = &mut step.action
        {
            plan.stream_count = 1;
            plan.append_batch_lens.truncate(1);
            plan.failover_batch_lens.truncate(1);
            plan.producer_sessions = false;
            plan.producer_epoch_bumps = false;
            plan.concurrent_producers = false;
            if plan.append_batch_lens.is_empty() {
                plan.append_batch_lens.push(2);
            }
            if plan.failover_batch_lens.is_empty() {
                plan.failover_batch_lens.push(1);
            }
        }
    }
    schedule
}

fn runtime_raft_network_workload_candidate_schedules(
    original: &ursula_sim::SimSchedule,
) -> Vec<(String, ursula_sim::SimSchedule)> {
    let mut candidates = Vec::new();
    for step in &original.fault_plan.steps {
        let ursula_sim::SimFaultAction::RunRuntimeRaftNetworkWorkload { plan } = &step.action
        else {
            continue;
        };
        for (mutation, plan) in runtime_raft_network_workload_candidate_plans(plan) {
            candidates.push((
                mutation,
                schedule_with_runtime_raft_workload_plan(original, plan),
            ));
        }
    }
    candidates
}

fn runtime_raft_network_workload_candidate_plans(
    plan: &ursula_sim::RuntimeRaftNetworkWorkloadPlan,
) -> Vec<(String, ursula_sim::RuntimeRaftNetworkWorkloadPlan)> {
    let mut candidates = Vec::new();
    if plan.corrupt_read_expectation {
        let mut reduced = plan.clone();
        reduced.corrupt_read_expectation = false;
        candidates.push((
            "disable_runtime_raft_corrupt_read_expectation".to_owned(),
            reduced,
        ));
    }
    if plan.corrupt_partial_read_expectation {
        let mut reduced = plan.clone();
        reduced.corrupt_partial_read_expectation = false;
        candidates.push((
            "disable_runtime_raft_corrupt_partial_read_expectation".to_owned(),
            reduced,
        ));
    }
    if plan.corrupt_tail_read_expectation {
        let mut reduced = plan.clone();
        reduced.corrupt_tail_read_expectation = false;
        candidates.push((
            "disable_runtime_raft_corrupt_tail_read_expectation".to_owned(),
            reduced,
        ));
    }
    if plan.corrupt_close_state_expectation {
        let mut reduced = plan.clone();
        reduced.corrupt_close_state_expectation = false;
        candidates.push((
            "disable_runtime_raft_corrupt_close_state_expectation".to_owned(),
            reduced,
        ));
    }
    if plan.corrupt_snapshot_expectation {
        let mut reduced = plan.clone();
        reduced.corrupt_snapshot_expectation = false;
        candidates.push((
            "disable_runtime_raft_corrupt_snapshot_expectation".to_owned(),
            reduced,
        ));
    }
    if plan.corrupt_leader_failover_read_expectation {
        let mut reduced = plan.clone();
        reduced.corrupt_leader_failover_read_expectation = false;
        candidates.push((
            "disable_runtime_raft_corrupt_leader_failover_read_expectation".to_owned(),
            reduced,
        ));
    }
    if plan.partial_reads {
        let mut reduced = plan.clone();
        reduced.partial_reads = false;
        candidates.push(("disable_runtime_raft_partial_reads".to_owned(), reduced));
    }
    if plan.tail_reads {
        let mut reduced = plan.clone();
        reduced.tail_reads = false;
        candidates.push(("disable_runtime_raft_tail_reads".to_owned(), reduced));
    }
    if plan.close_streams {
        let mut reduced = plan.clone();
        reduced.close_streams = false;
        candidates.push(("disable_runtime_raft_close_streams".to_owned(), reduced));
    }
    if plan.publish_snapshots {
        let mut reduced = plan.clone();
        reduced.publish_snapshots = false;
        candidates.push(("disable_runtime_raft_publish_snapshots".to_owned(), reduced));
    }
    if plan.concurrent_producers {
        let mut reduced = plan.clone();
        reduced.concurrent_producers = false;
        candidates.push((
            "disable_runtime_raft_concurrent_producers".to_owned(),
            reduced,
        ));
    }
    if plan.producer_epoch_bumps {
        let mut reduced = plan.clone();
        reduced.producer_epoch_bumps = false;
        candidates.push((
            "disable_runtime_raft_producer_epoch_bumps".to_owned(),
            reduced,
        ));
    }
    if plan.producer_sessions {
        let mut reduced = plan.clone();
        reduced.producer_sessions = false;
        reduced.producer_epoch_bumps = false;
        reduced.concurrent_producers = false;
        candidates.push(("disable_runtime_raft_producer_sessions".to_owned(), reduced));
    }
    if plan.append_batch_lens.iter().any(|len| *len > 1) {
        let mut reduced = plan.clone();
        for len in &mut reduced.append_batch_lens {
            *len = 1;
        }
        candidates.push((
            "shrink_runtime_raft_append_batches_to_one".to_owned(),
            reduced,
        ));
    }
    if plan.failover_batch_lens.iter().any(|len| *len > 1) {
        let mut reduced = plan.clone();
        for len in &mut reduced.failover_batch_lens {
            *len = 1;
        }
        candidates.push((
            "shrink_runtime_raft_failover_batches_to_one".to_owned(),
            reduced,
        ));
    }
    candidates
}

fn schedule_with_runtime_raft_workload_plan(
    original: &ursula_sim::SimSchedule,
    plan: ursula_sim::RuntimeRaftNetworkWorkloadPlan,
) -> ursula_sim::SimSchedule {
    let mut schedule = original.clone();
    for step in &mut schedule.fault_plan.steps {
        if let ursula_sim::SimFaultAction::RunRuntimeRaftNetworkWorkload {
            plan: existing_plan,
        } = &mut step.action
        {
            *existing_plan = plan;
            break;
        }
    }
    schedule
}

fn runtime_raft_network_workload_is_multistream(schedule: &ursula_sim::SimSchedule) -> bool {
    schedule.fault_plan.steps.iter().any(|step| {
        matches!(
            &step.action,
            ursula_sim::SimFaultAction::RunRuntimeRaftNetworkWorkload { plan }
                if plan.stream_count > 1
        )
    })
}

fn runtime_raft_snapshot_install_has_corruption(schedule: &ursula_sim::SimSchedule) -> bool {
    schedule.fault_plan.steps.iter().any(|step| {
        matches!(
            step.action,
            ursula_sim::SimFaultAction::CorruptRuntimeRaftSnapshotAppendCounts
        )
    })
}

fn runtime_raft_snapshot_install_candidate_schedules(
    original: &ursula_sim::SimSchedule,
) -> Vec<(String, ursula_sim::SimSchedule)> {
    if !runtime_raft_snapshot_install_has_corruption(original) {
        return Vec::new();
    }
    let mut schedule = original.clone();
    schedule.fault_plan.steps.retain(|step| {
        !matches!(
            step.action,
            ursula_sim::SimFaultAction::CorruptRuntimeRaftSnapshotAppendCounts
        )
    });
    vec![(
        "remove_corrupt_runtime_raft_snapshot_append_counts".to_owned(),
        schedule,
    )]
}

fn http_producer_protocol_surface_has_corruption(schedule: &ursula_sim::SimSchedule) -> bool {
    schedule.fault_plan.steps.iter().any(|step| {
        matches!(
            step.action,
            ursula_sim::SimFaultAction::CorruptHttpProducerDuplicateExpectation
        )
    })
}

fn http_producer_protocol_surface_candidate_schedules(
    original: &ursula_sim::SimSchedule,
) -> Vec<(String, ursula_sim::SimSchedule)> {
    if !http_producer_protocol_surface_has_corruption(original) {
        return Vec::new();
    }
    let mut schedule = original.clone();
    schedule.fault_plan.steps.retain(|step| {
        !matches!(
            step.action,
            ursula_sim::SimFaultAction::CorruptHttpProducerDuplicateExpectation
        )
    });
    vec![(
        "remove_corrupt_http_producer_duplicate_expectation".to_owned(),
        schedule,
    )]
}

fn http_snapshot_protocol_surface_has_corruption(schedule: &ursula_sim::SimSchedule) -> bool {
    schedule.fault_plan.steps.iter().any(|step| {
        matches!(
            step.action,
            ursula_sim::SimFaultAction::CorruptHttpSnapshotBodyExpectation
        )
    })
}

fn http_snapshot_protocol_surface_candidate_schedules(
    original: &ursula_sim::SimSchedule,
) -> Vec<(String, ursula_sim::SimSchedule)> {
    if !http_snapshot_protocol_surface_has_corruption(original) {
        return Vec::new();
    }
    let mut schedule = original.clone();
    schedule.fault_plan.steps.retain(|step| {
        !matches!(
            step.action,
            ursula_sim::SimFaultAction::CorruptHttpSnapshotBodyExpectation
        )
    });
    vec![(
        "remove_corrupt_http_snapshot_body_expectation".to_owned(),
        schedule,
    )]
}

fn http_live_protocol_surface_has_corruption(schedule: &ursula_sim::SimSchedule) -> bool {
    schedule.fault_plan.steps.iter().any(|step| {
        matches!(
            step.action,
            ursula_sim::SimFaultAction::CorruptHttpLiveSseNextOffsetExpectation
        )
    })
}

fn http_live_protocol_surface_candidate_schedules(
    original: &ursula_sim::SimSchedule,
) -> Vec<(String, ursula_sim::SimSchedule)> {
    if !http_live_protocol_surface_has_corruption(original) {
        return Vec::new();
    }
    let mut schedule = original.clone();
    schedule.fault_plan.steps.retain(|step| {
        !matches!(
            step.action,
            ursula_sim::SimFaultAction::CorruptHttpLiveSseNextOffsetExpectation
        )
    });
    vec![(
        "remove_corrupt_http_live_sse_next_offset_expectation".to_owned(),
        schedule,
    )]
}

fn http_live_limit_protocol_surface_has_corruption(schedule: &ursula_sim::SimSchedule) -> bool {
    schedule.fault_plan.steps.iter().any(|step| {
        matches!(
            step.action,
            ursula_sim::SimFaultAction::CorruptHttpLiveLimitBackpressureExpectation
        )
    })
}

fn http_live_limit_protocol_surface_candidate_schedules(
    original: &ursula_sim::SimSchedule,
) -> Vec<(String, ursula_sim::SimSchedule)> {
    if !http_live_limit_protocol_surface_has_corruption(original) {
        return Vec::new();
    }
    let mut schedule = original.clone();
    schedule.fault_plan.steps.retain(|step| {
        !matches!(
            step.action,
            ursula_sim::SimFaultAction::CorruptHttpLiveLimitBackpressureExpectation
        )
    });
    vec![(
        "remove_corrupt_http_live_limit_backpressure_expectation".to_owned(),
        schedule,
    )]
}

fn http_protocol_surface_randomized_has_corruption(schedule: &ursula_sim::SimSchedule) -> bool {
    http_protocol_surface_plan(schedule)
        .map(|plan| {
            plan.corrupt_final_read_expectation
                || plan.corrupt_sse_next_offset_expectation
                || plan.corrupt_live_limit_backpressure_expectation
        })
        .unwrap_or(false)
}

fn http_protocol_surface_randomized_candidate_schedules(
    original: &ursula_sim::SimSchedule,
) -> Vec<(String, ursula_sim::SimSchedule)> {
    let Some(plan) = http_protocol_surface_plan(original) else {
        return Vec::new();
    };
    if !plan.corrupt_final_read_expectation
        && !plan.corrupt_sse_next_offset_expectation
        && !plan.corrupt_live_limit_backpressure_expectation
    {
        return Vec::new();
    }

    let mut candidates = Vec::new();
    if plan.corrupt_final_read_expectation {
        let mut without_corruption = plan.clone();
        without_corruption.corrupt_final_read_expectation = false;
        candidates.push((
            "remove_corrupt_http_protocol_final_read_expectation".to_owned(),
            schedule_with_http_protocol_surface_plan(original, without_corruption),
        ));
    }
    if plan.corrupt_sse_next_offset_expectation {
        let mut without_corruption = plan.clone();
        without_corruption.corrupt_sse_next_offset_expectation = false;
        candidates.push((
            "remove_corrupt_http_protocol_sse_next_offset_expectation".to_owned(),
            schedule_with_http_protocol_surface_plan(original, without_corruption),
        ));
    }
    if plan.corrupt_live_limit_backpressure_expectation {
        let mut without_corruption = plan.clone();
        without_corruption.corrupt_live_limit_backpressure_expectation = false;
        candidates.push((
            "remove_corrupt_http_protocol_live_limit_backpressure_expectation".to_owned(),
            schedule_with_http_protocol_surface_plan(original, without_corruption),
        ));
    }

    for (field, enabled) in [
        ("ttl", plan.ttl),
        ("producer_sessions", plan.producer_sessions),
        ("producer_sequence_gap", plan.producer_sequence_gap),
        ("producer_epoch_bump", plan.producer_epoch_bump),
        ("concurrent_producers", plan.concurrent_producers),
        ("long_poll", plan.long_poll),
        ("sse_close", plan.sse_close),
        ("live_limit", plan.live_limit),
        ("live_timeout", plan.live_timeout),
        ("partial_reads", plan.partial_reads),
    ] {
        if !enabled {
            continue;
        }
        let mut reduced = plan.clone();
        match field {
            "ttl" => reduced.ttl = false,
            "producer_sessions" => {
                reduced.producer_sessions = false;
                reduced.producer_sequence_gap = false;
                reduced.producer_epoch_bump = false;
                reduced.concurrent_producers = false;
            }
            "producer_sequence_gap" => reduced.producer_sequence_gap = false,
            "producer_epoch_bump" => reduced.producer_epoch_bump = false,
            "concurrent_producers" => reduced.concurrent_producers = false,
            "long_poll" => reduced.long_poll = false,
            "sse_close" => reduced.sse_close = false,
            "live_limit" => reduced.live_limit = false,
            "live_timeout" => reduced.live_timeout = false,
            "partial_reads" => reduced.partial_reads = false,
            _ => unreachable!(),
        }
        candidates.push((
            format!("disable_http_protocol_{field}"),
            schedule_with_http_protocol_surface_plan(original, reduced),
        ));
    }

    candidates
}

fn http_protocol_surface_plan(
    schedule: &ursula_sim::SimSchedule,
) -> Option<ursula_sim::HttpProtocolSurfacePlan> {
    schedule
        .fault_plan
        .steps
        .iter()
        .find_map(|step| match &step.action {
            ursula_sim::SimFaultAction::RunHttpProtocolSurfaceWorkload { plan } => {
                Some(plan.clone())
            }
            _ => None,
        })
}

fn schedule_with_http_protocol_surface_plan(
    original: &ursula_sim::SimSchedule,
    plan: ursula_sim::HttpProtocolSurfacePlan,
) -> ursula_sim::SimSchedule {
    let mut schedule = original.clone();
    for step in &mut schedule.fault_plan.steps {
        if let ursula_sim::SimFaultAction::RunHttpProtocolSurfaceWorkload {
            plan: existing_plan,
        } = &mut step.action
        {
            *existing_plan = plan;
            break;
        }
    }
    schedule
}

#[derive(Clone)]
enum ColdPathFault {
    CorruptReadExpectation { node_id: u64 },
    FailNextColdRead,
    FailNextColdWrite,
    DelayNextColdWrite { delay_ms: u64 },
    FailNextColdDelete,
    DelayNextColdRead { delay_ms: u64 },
    TruncateNextColdRead { returned_len: usize },
}

impl ColdPathFault {
    fn mutation_name(&self) -> &'static str {
        match self {
            Self::CorruptReadExpectation { .. } => "corrupt_cold_live_read_expectation",
            Self::FailNextColdRead => "fail_next_cold_read",
            Self::FailNextColdWrite => "fail_next_cold_write",
            Self::DelayNextColdWrite { .. } => "delay_next_cold_write",
            Self::FailNextColdDelete => "fail_next_cold_delete",
            Self::DelayNextColdRead { .. } => "delay_next_cold_read",
            Self::TruncateNextColdRead { .. } => "truncate_next_cold_read",
        }
    }
}

fn schedule_with_cold_path_fault(
    original: &ursula_sim::SimSchedule,
    fault: Option<ColdPathFault>,
) -> ursula_sim::SimSchedule {
    let mut schedule = original.clone();
    schedule.fault_plan.steps.retain(|step| {
        !matches!(
            step.action,
            ursula_sim::SimFaultAction::CorruptColdLiveReadExpectation { .. }
                | ursula_sim::SimFaultAction::FailNextColdRead
                | ursula_sim::SimFaultAction::FailNextColdWrite
                | ursula_sim::SimFaultAction::DelayNextColdWrite { .. }
                | ursula_sim::SimFaultAction::FailNextColdDelete
                | ursula_sim::SimFaultAction::DelayNextColdRead { .. }
                | ursula_sim::SimFaultAction::TruncateNextColdRead { .. }
        )
    });
    if let Some(fault) = fault {
        match fault {
            ColdPathFault::CorruptReadExpectation { node_id } => {
                schedule.fault_plan.steps.push(ursula_sim::SimFaultStep {
                    phase: "cold_live_read_verify".to_owned(),
                    action: ursula_sim::SimFaultAction::CorruptColdLiveReadExpectation { node_id },
                });
            }
            ColdPathFault::FailNextColdRead => {
                schedule.fault_plan.steps.push(ursula_sim::SimFaultStep {
                    phase: "before_cold_read".to_owned(),
                    action: ursula_sim::SimFaultAction::FailNextColdRead,
                });
            }
            ColdPathFault::FailNextColdWrite => {
                schedule.fault_plan.steps.push(ursula_sim::SimFaultStep {
                    phase: "before_cold_write".to_owned(),
                    action: ursula_sim::SimFaultAction::FailNextColdWrite,
                });
            }
            ColdPathFault::DelayNextColdWrite { delay_ms } => {
                schedule.fault_plan.steps.push(ursula_sim::SimFaultStep {
                    phase: "before_cold_write".to_owned(),
                    action: ursula_sim::SimFaultAction::DelayNextColdWrite { delay_ms },
                });
            }
            ColdPathFault::FailNextColdDelete => {
                schedule.fault_plan.steps.push(ursula_sim::SimFaultStep {
                    phase: "before_cold_cleanup".to_owned(),
                    action: ursula_sim::SimFaultAction::FailNextColdDelete,
                });
            }
            ColdPathFault::DelayNextColdRead { delay_ms } => {
                schedule.fault_plan.steps.push(ursula_sim::SimFaultStep {
                    phase: "before_cold_read".to_owned(),
                    action: ursula_sim::SimFaultAction::DelayNextColdRead { delay_ms },
                });
            }
            ColdPathFault::TruncateNextColdRead { returned_len } => {
                schedule.fault_plan.steps.push(ursula_sim::SimFaultStep {
                    phase: "before_cold_read".to_owned(),
                    action: ursula_sim::SimFaultAction::TruncateNextColdRead { returned_len },
                });
            }
        }
    }
    schedule
}

fn run_candidate(schedule: &ursula_sim::SimSchedule, target: &TargetPredicate) -> CandidateOutcome {
    match run_schedule_capturing_panic(schedule) {
        Ok(report) => CandidateOutcome::Passed {
            target_preserved: target_matches_pass(target, stable_trace(report.outcome.trace)),
        },
        Err(panic) => {
            let trace = stable_trace(ursula_sim::SimTrace::last_recorded());
            CandidateOutcome::ReproducedPanic {
                target_preserved: target_matches_failure(target, &panic, trace),
                panic,
            }
        }
    }
}

fn run_minimized_schedule(
    schedule: &ursula_sim::SimSchedule,
    target: &TargetPredicate,
) -> (
    CandidateOutcome,
    Option<ursula_sim::SimScheduledRecord>,
    Option<MinimizedFailureArtifact>,
) {
    match run_schedule_capturing_panic(schedule) {
        Ok(report) => {
            let target_preserved =
                target_matches_pass(target, stable_trace(report.outcome.clone().trace));
            let record = target_preserved
                .then(|| ursula_sim::SimScheduledRecord::new(schedule.clone(), report));
            (CandidateOutcome::Passed { target_preserved }, record, None)
        }
        Err(panic) => {
            let raw_trace = ursula_sim::SimTrace::last_recorded();
            let trace = stable_trace(raw_trace.clone());
            let target_preserved = target_matches_failure(target, &panic, trace.clone());
            let failure = target_preserved.then(|| MinimizedFailureArtifact {
                schema_version: ursula_sim::SIM_REGRESSION_SCHEMA_VERSION,
                seed: schedule.seed,
                schedule: schedule.clone(),
                panic: panic.clone(),
                stable_trace: trace,
                raw_event_log: raw_trace.events,
            });
            (
                CandidateOutcome::ReproducedPanic {
                    target_preserved,
                    panic,
                },
                None,
                failure,
            )
        }
    }
}

fn target_matches_pass(target: &TargetPredicate, trace: ursula_sim::SimTrace) -> bool {
    match target {
        TargetPredicate::PanicContains { .. } => false,
        TargetPredicate::EventCountAtLeast { event, min_count } => {
            event_count(&trace, event) >= *min_count
        }
        TargetPredicate::InvariantFailed { invariant } => invariant_failed(&trace, invariant),
        TargetPredicate::StableTraceExact { trace: expected } => trace == *expected,
        TargetPredicate::StableTracePrefix {
            trace: expected_prefix,
        } => {
            trace.events.get(..expected_prefix.events.len())
                == Some(expected_prefix.events.as_slice())
        }
    }
}

fn target_matches_failure(
    target: &TargetPredicate,
    panic: &str,
    trace: ursula_sim::SimTrace,
) -> bool {
    match target {
        TargetPredicate::PanicContains { value } => panic.contains(value),
        TargetPredicate::EventCountAtLeast { event, min_count } => {
            event_count(&trace, event) >= *min_count
        }
        TargetPredicate::InvariantFailed { invariant } => invariant_failed(&trace, invariant),
        TargetPredicate::StableTraceExact { .. } | TargetPredicate::StableTracePrefix { .. } => {
            false
        }
    }
}

fn event_count(trace: &ursula_sim::SimTrace, event: &str) -> usize {
    trace
        .events
        .iter()
        .filter(|candidate| event_name(candidate).as_deref() == Some(event))
        .count()
}

fn event_name(event: &ursula_sim::SimEvent) -> Option<String> {
    let value = serde_json::to_value(event).ok()?;
    value.get("event")?.as_str().map(str::to_owned)
}

fn runtime_interleaving_plan(
    schedule: &ursula_sim::SimSchedule,
) -> Option<ursula_sim::RuntimeInterleavingPlan> {
    schedule
        .fault_plan
        .steps
        .iter()
        .find_map(|step| match &step.action {
            ursula_sim::SimFaultAction::RunRuntimeSeededInterleaving { plan } => Some(plan.clone()),
            _ => None,
        })
}

fn cold_path_fault(schedule: &ursula_sim::SimSchedule) -> Option<ColdPathFault> {
    schedule
        .fault_plan
        .steps
        .iter()
        .find_map(|step| match &step.action {
            ursula_sim::SimFaultAction::CorruptColdLiveReadExpectation { node_id } => {
                Some(ColdPathFault::CorruptReadExpectation { node_id: *node_id })
            }
            ursula_sim::SimFaultAction::FailNextColdRead => Some(ColdPathFault::FailNextColdRead),
            ursula_sim::SimFaultAction::FailNextColdWrite => Some(ColdPathFault::FailNextColdWrite),
            ursula_sim::SimFaultAction::DelayNextColdWrite { delay_ms } => {
                Some(ColdPathFault::DelayNextColdWrite {
                    delay_ms: *delay_ms,
                })
            }
            ursula_sim::SimFaultAction::FailNextColdDelete => {
                Some(ColdPathFault::FailNextColdDelete)
            }
            ursula_sim::SimFaultAction::DelayNextColdRead { delay_ms } => {
                Some(ColdPathFault::DelayNextColdRead {
                    delay_ms: *delay_ms,
                })
            }
            ursula_sim::SimFaultAction::TruncateNextColdRead { returned_len } => {
                Some(ColdPathFault::TruncateNextColdRead {
                    returned_len: *returned_len,
                })
            }
            _ => None,
        })
}

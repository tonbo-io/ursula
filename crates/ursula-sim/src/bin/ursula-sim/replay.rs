//! `ursula-sim replay`: re-run a seed or recorded artifact and check its
//! outcome.

use std::error::Error;
use std::path::PathBuf;

use ursula_sim::artifact::FailedSeedArtifact;
use ursula_sim::artifact::StableTraceArtifact;
use ursula_sim::artifact::invariant_failed;
use ursula_sim::artifact::run_schedule_capturing_panic;
use ursula_sim::artifact::stable_trace;

pub fn run(args: Vec<String>) -> Result<(), Box<dyn Error>> {
    let args = Args::parse(args)?;
    let replay = match args.input {
        ReplayInput::Seed(seed) => ReplayRequest {
            schedule: ursula_sim::SimSchedule::generate(seed),
            expected_outcome: None,
            expected_stable_trace: None,
            artifact_panic: None,
        },
        ReplayInput::Artifact(path) => ReplayRequest::from_artifact(path)?,
    };

    let expected_panic = match args.expected_panic {
        Some(ExpectedPanic::Artifact) => {
            let panic = replay.artifact_panic.clone().ok_or(
                "--expect-artifact-panic requires an artifact produced from a failed seed",
            )?;
            Some(ExpectedPanic::Contains(panic))
        }
        other => other,
    };

    if let Some(expected_panic) = expected_panic {
        let Err(panic) = run_schedule_capturing_panic(&replay.schedule) else {
            return Err("replay completed successfully, expected panic".into());
        };
        let current_stable_trace = stable_trace(ursula_sim::SimTrace::last_recorded());
        expected_panic.assert_matches(&panic, current_stable_trace.clone())?;
        if let Some(expected) = replay.expected_stable_trace {
            assert_eq!(current_stable_trace, expected);
        }
        println!("reproduced expected panic: {panic}");
        return Ok(());
    }

    let report = replay.schedule.run();
    if let Some(expected) = replay.expected_outcome {
        assert_eq!(
            ursula_sim::stable_replay_outcome(report.outcome.clone()),
            ursula_sim::stable_replay_outcome(expected)
        );
    }
    if let Some(expected) = replay.expected_stable_trace {
        assert_eq!(stable_trace(report.outcome.trace.clone()), expected);
    }

    let record = ursula_sim::SimScheduledRecord::new(replay.schedule, report);
    let mut encoded = serde_json::to_string_pretty(&record)?;
    encoded.push('\n');
    match args.output {
        Some(path) => std::fs::write(path, encoded)?,
        None => print!("{encoded}"),
    }
    Ok(())
}

struct Args {
    input: ReplayInput,
    output: Option<PathBuf>,
    expected_panic: Option<ExpectedPanic>,
}

#[derive(Debug)]
enum ExpectedPanic {
    Contains(String),
    Invariant(String),
    Artifact,
}

enum ReplayInput {
    Seed(u64),
    Artifact(PathBuf),
}

impl Args {
    fn parse(args: Vec<String>) -> Result<Self, Box<dyn Error>> {
        let mut input = None;
        let mut output = None;
        let mut expected_panic = None;
        let mut args = args.into_iter();

        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--seed" => {
                    let seed = args
                        .next()
                        .ok_or_else(|| format!("usage: {USAGE}"))?
                        .parse::<u64>()?;
                    set_input(&mut input, ReplayInput::Seed(seed))?;
                }
                "--artifact" => {
                    let path = args.next().ok_or_else(|| format!("usage: {USAGE}"))?;
                    set_input(&mut input, ReplayInput::Artifact(PathBuf::from(path)))?;
                }
                "--output" => {
                    let path = args.next().ok_or_else(|| format!("usage: {USAGE}"))?;
                    output = Some(PathBuf::from(path));
                }
                "--expect-panic-contains" => {
                    set_expected_panic(
                        &mut expected_panic,
                        ExpectedPanic::Contains(
                            args.next().ok_or_else(|| format!("usage: {USAGE}"))?,
                        ),
                    )?;
                }
                "--expect-invariant" => {
                    set_expected_panic(
                        &mut expected_panic,
                        ExpectedPanic::Invariant(
                            args.next().ok_or_else(|| format!("usage: {USAGE}"))?,
                        ),
                    )?;
                }
                "--expect-artifact-panic" => {
                    set_expected_panic(&mut expected_panic, ExpectedPanic::Artifact)?;
                }
                "--help" | "-h" => {
                    println!("{USAGE}");
                    std::process::exit(0);
                }
                _ => return Err(format!("unknown argument `{arg}`\nusage: {USAGE}").into()),
            }
        }

        let input = input.ok_or_else(|| format!("usage: {USAGE}"))?;
        if expected_panic.is_some() && output.is_some() {
            return Err("--output cannot be used with expected-panic replay".into());
        }
        Ok(Self {
            input,
            output,
            expected_panic,
        })
    }
}

fn set_input(slot: &mut Option<ReplayInput>, value: ReplayInput) -> Result<(), Box<dyn Error>> {
    if slot.is_some() {
        return Err("exactly one of --seed or --artifact is allowed".into());
    }
    *slot = Some(value);
    Ok(())
}

fn set_expected_panic(
    slot: &mut Option<ExpectedPanic>,
    value: ExpectedPanic,
) -> Result<(), Box<dyn Error>> {
    if slot.is_some() {
        return Err(
            "only one of --expect-panic-contains, --expect-invariant, or --expect-artifact-panic is allowed"
                .into(),
        );
    }
    *slot = Some(value);
    Ok(())
}

const USAGE: &str = "ursula-sim replay (--seed N | --artifact PATH) [--output output.json] [--expect-panic-contains TEXT | --expect-invariant NAME | --expect-artifact-panic]";

struct ReplayRequest {
    schedule: ursula_sim::SimSchedule,
    expected_outcome: Option<ursula_sim::ThreeNodeRaftSimOutcome>,
    expected_stable_trace: Option<ursula_sim::SimTrace>,
    artifact_panic: Option<String>,
}

impl ReplayRequest {
    fn from_artifact(path: PathBuf) -> Result<Self, Box<dyn Error>> {
        let body = std::fs::read_to_string(&path)?;
        if let Ok(record) = serde_json::from_str::<ursula_sim::SimScheduledRecord>(&body) {
            return Ok(Self {
                schedule: record.schedule,
                expected_outcome: Some(record.outcome),
                expected_stable_trace: None,
                artifact_panic: None,
            });
        }
        if let Ok(artifact) = serde_json::from_str::<FailedSeedArtifact>(&body) {
            return Ok(Self {
                schedule: artifact.schedule,
                expected_outcome: None,
                expected_stable_trace: None,
                artifact_panic: Some(artifact.panic),
            });
        }
        if let Ok(artifact) = serde_json::from_str::<StableTraceArtifact>(&body) {
            return Ok(Self {
                schedule: artifact.schedule,
                expected_outcome: None,
                expected_stable_trace: Some(artifact.stable_trace),
                artifact_panic: None,
            });
        }
        Err(format!(
            "unsupported replay artifact `{}`; expected scheduled record, stable trace artifact, or failure summary",
            path.display()
        )
        .into())
    }
}

impl ExpectedPanic {
    fn assert_matches(
        self,
        panic: &str,
        trace: ursula_sim::SimTrace,
    ) -> Result<(), Box<dyn Error>> {
        match self {
            Self::Contains(value) => {
                if panic.contains(&value) {
                    Ok(())
                } else {
                    Err(format!("panic did not contain `{value}`: {panic}").into())
                }
            }
            Self::Invariant(invariant) => {
                if invariant_failed(&trace, &invariant) {
                    Ok(())
                } else {
                    Err(format!(
                        "panic replay did not record invariant `{invariant}`; panic was: {panic}"
                    )
                    .into())
                }
            }
            Self::Artifact => unreachable!("artifact panic expectation is resolved before replay"),
        }
    }
}

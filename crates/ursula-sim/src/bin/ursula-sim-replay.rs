use std::env;
use std::error::Error;
use std::path::PathBuf;

#[cfg(madsim)]
fn main() -> Result<(), Box<dyn Error>> {
    use std::panic;

    let args = Args::parse()?;
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
        let previous_hook = panic::take_hook();
        panic::set_hook(Box::new(|_| {}));
        let result = panic::catch_unwind(panic::AssertUnwindSafe(|| replay.schedule.run()));
        panic::set_hook(previous_hook);
        let Err(payload) = result else {
            return Err("replay completed successfully, expected panic".into());
        };
        let panic = panic_payload_to_string(payload);
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

#[cfg(not(madsim))]
fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse()?;
    match args.input {
        ReplayInput::Seed(seed) => {
            let _ = seed;
        }
        ReplayInput::Artifact(path) => {
            let _ = path;
        }
    }
    let _ = args.output;
    if let Some(expected_panic) = args.expected_panic {
        match expected_panic {
            ExpectedPanic::Contains(value) | ExpectedPanic::Invariant(value) => {
                let _ = value;
            }
            ExpectedPanic::Artifact => {}
        }
    }
    Err("ursula-sim-replay must run with RUSTFLAGS=\"--cfg madsim\"".into())
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
    fn parse() -> Result<Self, Box<dyn Error>> {
        let mut input = None;
        let mut output = None;
        let mut expected_panic = None;
        let mut args = env::args().skip(1);

        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--seed" => {
                    let seed = args
                        .next()
                        .ok_or_else(|| format!("usage: {}", usage()))?
                        .parse::<u64>()?;
                    set_input(&mut input, ReplayInput::Seed(seed))?;
                }
                "--artifact" => {
                    let path = args.next().ok_or_else(|| format!("usage: {}", usage()))?;
                    set_input(&mut input, ReplayInput::Artifact(PathBuf::from(path)))?;
                }
                "--output" => {
                    let path = args.next().ok_or_else(|| format!("usage: {}", usage()))?;
                    output = Some(PathBuf::from(path));
                }
                "--expect-panic-contains" => {
                    set_expected_panic(
                        &mut expected_panic,
                        ExpectedPanic::Contains(
                            args.next().ok_or_else(|| format!("usage: {}", usage()))?,
                        ),
                    )?;
                }
                "--expect-invariant" => {
                    set_expected_panic(
                        &mut expected_panic,
                        ExpectedPanic::Invariant(
                            args.next().ok_or_else(|| format!("usage: {}", usage()))?,
                        ),
                    )?;
                }
                "--expect-artifact-panic" => {
                    set_expected_panic(&mut expected_panic, ExpectedPanic::Artifact)?;
                }
                "--help" | "-h" => {
                    println!("{}", usage());
                    std::process::exit(0);
                }
                _ => return Err(format!("unknown argument `{arg}`\nusage: {}", usage()).into()),
            }
        }

        let input = input.ok_or_else(|| format!("usage: {}", usage()))?;
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

fn usage() -> String {
    format!(
        "{} (--seed N | --artifact PATH) [--output output.json] [--expect-panic-contains TEXT | --expect-invariant NAME | --expect-artifact-panic]",
        bin_name()
    )
}

fn bin_name() -> String {
    env::args()
        .next()
        .unwrap_or_else(|| "ursula-sim-replay".to_owned())
}

#[cfg(madsim)]
struct ReplayRequest {
    schedule: ursula_sim::SimSchedule,
    expected_outcome: Option<ursula_sim::ThreeNodeRaftSimOutcome>,
    expected_stable_trace: Option<ursula_sim::SimTrace>,
    artifact_panic: Option<String>,
}

#[cfg(madsim)]
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

#[cfg(madsim)]
#[derive(serde::Deserialize)]
struct StableTraceArtifact {
    schedule: ursula_sim::SimSchedule,
    stable_trace: ursula_sim::SimTrace,
}

#[cfg(madsim)]
#[derive(serde::Deserialize)]
struct FailedSeedArtifact {
    schedule: ursula_sim::SimSchedule,
    panic: String,
}

#[cfg(madsim)]
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

#[cfg(madsim)]
fn invariant_failed(trace: &ursula_sim::SimTrace, invariant: &str) -> bool {
    trace.events.iter().any(|event| {
        matches!(
            event,
            ursula_sim::SimEvent::InvariantFailed {
                invariant: candidate,
                ..
            } if candidate == invariant
        )
    })
}

#[cfg(madsim)]
fn panic_payload_to_string(payload: Box<dyn std::any::Any + Send>) -> String {
    if let Some(message) = payload.downcast_ref::<String>() {
        message.clone()
    } else if let Some(message) = payload.downcast_ref::<&'static str>() {
        (*message).to_owned()
    } else {
        "non-string panic payload".to_owned()
    }
}

#[cfg(madsim)]
fn stable_trace(trace: ursula_sim::SimTrace) -> ursula_sim::SimTrace {
    trace.stable_replay()
}

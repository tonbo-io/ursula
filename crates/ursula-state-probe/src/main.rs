//! `ursula-state-probe`: run one bounded-state workload, a whole suite, or the
//! CI ratchet gate. See the crate docs and
//! `docs/architecture/bounded-stream-state.md` §7.

use std::path::PathBuf;
use std::process::ExitCode;

use anyhow::Result;
use clap::Parser;
use clap::Subcommand;
use clap::ValueEnum;
use ursula_state_probe::alloc::Counting;
use ursula_state_probe::gate;
use ursula_state_probe::workloads::Workload;
use ursula_state_probe::workloads::l2::L2Args;
use ursula_state_probe::workloads::planner::PlannerArgs;
use ursula_state_probe::workloads::w1::W1Args;
use ursula_state_probe::workloads::w2::W2Args;
use ursula_state_probe::workloads::w3::W3Args;
use ursula_state_probe::workloads::w4::W4Args;
use ursula_state_probe::workloads::w5::W5Args;

#[global_allocator]
static GLOBAL: Counting = Counting;

#[derive(Debug, Parser)]
#[command(
    name = "ursula-state-probe",
    about = "Bounded per-stream state measurement harness"
)]
struct Cli {
    /// Directory for `<workload>.jsonl` results and `gate.json`.
    #[arg(long, global = true, default_value = "target/state-probe")]
    out_dir: PathBuf,
    /// Do not echo JSONL rows to stdout.
    #[arg(long, global = true)]
    quiet: bool,
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum SuiteName {
    Pr,
    Nightly,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// W1 (and W6 with `--retain-every`): one JSON stream of inline appends.
    W1(W1Args),
    /// W2 (and W6 with `--retain-every-sec`): slow streams sharing packs.
    W2(W2Args),
    /// W3 (and W6): external-only appends.
    W3(W3Args),
    /// W4 (and W6): producer headers.
    W4(W4Args),
    /// W5: churn (delete, TTL expiry, TTL heap, bucket purge).
    W5(W5Args),
    /// Flush-planner cost and starvation counters.
    Planner(PlannerArgs),
    /// L2: the real ShardRuntime on the in-memory and Raft engines.
    L2(L2Args),
    /// Run a whole suite without checking it.
    Suite {
        #[arg(long, value_enum, default_value = "pr")]
        suite: SuiteName,
    },
    /// Run a suite and compare it with the ratchet file (CI gate).
    Gate {
        #[arg(long, value_enum, default_value = "pr")]
        suite: SuiteName,
        #[arg(long, default_value = "crates/ursula-state-probe/ratchet.json")]
        ratchet: PathBuf,
        /// Rewrite the ratchet file from this run instead of comparing.
        #[arg(long)]
        write_ratchet: bool,
    },
}

fn suite(name: SuiteName) -> Result<Vec<gate::Job>> {
    match name {
        SuiteName::Pr => gate::pr_suite(),
        SuiteName::Nightly => gate::nightly_suite(),
    }
}

fn run(cli: Cli) -> Result<()> {
    let echo = !cli.quiet;
    let single = match cli.command {
        Command::W1(args) => Workload::W1(args),
        Command::W2(args) => Workload::W2(args),
        Command::W3(args) => Workload::W3(args),
        Command::W4(args) => Workload::W4(args),
        Command::W5(args) => Workload::W5(args),
        Command::Planner(args) => Workload::Planner(args),
        Command::L2(args) => Workload::L2(args),
        Command::Suite { suite: name } => {
            let outcome = gate::run_suite(&suite(name)?, &cli.out_dir, echo)?;
            std::fs::write(
                cli.out_dir.join("outcome.json"),
                serde_json::to_string_pretty(&outcome)?,
            )?;
            return Ok(());
        }
        Command::Gate {
            suite: name,
            ratchet,
            write_ratchet,
        } => {
            let outcome = gate::run_suite(&suite(name)?, &cli.out_dir, echo)?;
            if write_ratchet {
                let fresh = gate::Ratchet::from_outcome(&outcome);
                std::fs::write(&ratchet, serde_json::to_string_pretty(&fresh)? + "\n")?;
                println!("wrote {}", ratchet.display());
                return Ok(());
            }
            let findings = gate::compare(&outcome, &gate::Ratchet::load(&ratchet)?);
            return gate::report(&outcome, &findings, &cli.out_dir);
        }
    };
    let outcome = single.run(&cli.out_dir, echo)?;
    for check in &outcome.checks {
        let status = if check.met { "met" } else { "not met" };
        eprintln!(
            "check {} {status}: {} (bound {})",
            check.name, check.value, check.bound
        );
    }
    Ok(())
}

fn main() -> ExitCode {
    match run(Cli::parse()) {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("ursula-state-probe: {err:#}");
            ExitCode::FAILURE
        }
    }
}

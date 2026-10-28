//! `ursula-vfs-host`: a host's tool for the roots on its disk (design: Hosts), for a host that
//! snapshots or moves the process, or the VM it runs in. It never loads SQLite. `inspect` writes
//! no file and takes no lock, `hold` locks the host directory's `hold` file, and `drain` is a
//! process of the extension: it locks roots, writes their logs and claims, so a host that tells
//! its helpers apart by executable must tell a drain apart by its first argument.
//!
//! `ursula-vfs-host inspect [--dir D] [--scan S]... [--pid P]... [--until-clean MS] [--budget-ms MS]`
//! reads every root of the directories the host directory records and of each `--scan S`, checks
//! that every root lock a live process holds belongs to a root it read, and prints the result as
//! one line of JSON ([`Inspection::to_json`]).
//!
//! - `--dir D`: the host directory, the compiled-in `/var/lib/sqlite-ursula/host` by default.
//! - `--scan S`: a directory to read besides the recorded ones, such as a default cache root.
//! - `--pid P`: the lock check reads only these processes, every process without one, which
//!   needs root.
//! - `--until-clean MS`: read again every 50 ms until the result is clean or incomplete, or MS
//!   have passed, then print the last result.
//! - `--budget-ms MS`: a pass that runs longer stops, incomplete (`budget`).
//!
//! `ursula-vfs-host hold [--dir D]` takes the host directory's `hold` file exclusively, waiting
//! for it as long as another process holds it, prints `held`, and holds it until its stdin closes:
//! meanwhile write transactions of the extension wait before they start ([`hold::take`]). It never
//! creates the file, which the host creates with the directory.
//!
//! `ursula-vfs-host drain --origin URL --budget-ms MS [--scan S]... [--concurrency N]` reads every
//! root of the host directory the extension records into and of each `--scan S` as an inspection
//! does, drains each one its effective user owns that holds commits the stream has not
//! acknowledged, and prints one line of JSON within MS + 250 ms ([`Drained::to_json`]). It reads
//! the extension's settings from its environment, `URSULA_VFS_TOKEN_FILE` included.
//!
//! - `--origin URL`: where the deployment answers: `http://` or `https://`, a host and an optional
//!   port. A root's stream URL is the origin followed by its stream path.
//! - `--budget-ms MS`: the drain prints once every root has an outcome, and at MS at the latest.
//! - `--scan S`: a directory to read besides the recorded ones.
//! - `--concurrency N`: the streams that drain at once, 1 to 16, 4 by default.
//!
//! It exits 0 when it printed a result (or held and released the hold), 2 on a usage error, and 1
//! when it could not take the hold or write.

use std::ffi::OsString;
use std::io::Read;
use std::io::Write;
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Duration;

use sqlite_ursula_vfs::drain::Concurrency;
use sqlite_ursula_vfs::drain::ConcurrencyError;
use sqlite_ursula_vfs::drain::Drain;
use sqlite_ursula_vfs::drain::Drained;
use sqlite_ursula_vfs::drain::Origin;
use sqlite_ursula_vfs::drain::OriginError;
use sqlite_ursula_vfs::drain::drain;
use sqlite_ursula_vfs::hold;
use sqlite_ursula_vfs::inspection::Inspect;
use sqlite_ursula_vfs::inspection::Inspection;
use sqlite_ursula_vfs::inspection::inspect;

const USAGE: &str = "usage: ursula-vfs-host inspect [--dir D] [--scan S]... [--pid P]... \
                     [--until-clean MS] [--budget-ms MS]\n       ursula-vfs-host hold [--dir D]\n       \
                     ursula-vfs-host drain --origin URL --budget-ms MS [--scan S]... [--concurrency N]";

/// What the command line asks for.
enum Command {
    Inspect(Inspect),
    Hold { dir: Option<PathBuf> },
    Drain(Drain),
}

/// A command line that is not one `ursula-vfs-host` takes.
#[derive(Debug, thiserror::Error)]
enum Usage {
    #[error("no command")]
    NoCommand,
    #[error("unknown command {0:?}")]
    Command(String),
    #[error("unknown option {0:?}")]
    Option(String),
    #[error("{0} needs a value")]
    Missing(&'static str),
    #[error("{flag} takes a whole number, not {value:?}")]
    Number { flag: &'static str, value: String },
    #[error("--budget-ms takes a whole number from 1, not 0")]
    NoBudget,
    #[error("--concurrency: {0}")]
    Concurrency(#[from] ConcurrencyError),
    #[error("--origin: {0}")]
    Origin(#[from] OriginError),
    #[error("--origin takes UTF-8, not {0:?}")]
    OriginBytes(String),
    #[error("drain needs {0}")]
    Required(&'static str),
}

fn number<T: std::str::FromStr>(flag: &'static str, value: OsString) -> Result<T, Usage> {
    let usage = |value: OsString| Usage::Number {
        flag,
        value: value.to_string_lossy().into_owned(),
    };
    match value.to_str().map(str::parse) {
        Some(Ok(n)) => Ok(n),
        Some(Err(_not_a_number)) => Err(usage(value)),
        None => Err(usage(value)),
    }
}

/// The command the arguments `args` ask for. A path may be any bytes, as a home directory taken
/// from the password database may be.
fn parse(mut args: impl Iterator<Item = OsString>) -> Result<Command, Usage> {
    match args.next() {
        Some(command) if command == "inspect" => parse_inspect(args).map(Command::Inspect),
        Some(command) if command == "hold" => parse_hold(args),
        Some(command) if command == "drain" => parse_drain(args).map(Command::Drain),
        Some(other) => Err(Usage::Command(other.to_string_lossy().into_owned())),
        None => Err(Usage::NoCommand),
    }
}

fn parse_hold(mut args: impl Iterator<Item = OsString>) -> Result<Command, Usage> {
    let mut dir = None;
    while let Some(flag) = args.next() {
        match flag.to_str().unwrap_or_default() {
            "--dir" => dir = Some(PathBuf::from(args.next().ok_or(Usage::Missing("--dir"))?)),
            _ => return Err(Usage::Option(flag.to_string_lossy().into_owned())),
        }
    }
    Ok(Command::Hold { dir })
}

fn parse_inspect(mut args: impl Iterator<Item = OsString>) -> Result<Inspect, Usage> {
    let mut request = Inspect::default();
    while let Some(flag) = args.next() {
        let mut value = |name: &'static str| args.next().ok_or(Usage::Missing(name));
        match flag.to_str().unwrap_or_default() {
            "--dir" => request.dir = Some(PathBuf::from(value("--dir")?)),
            "--scan" => request.scans.push(PathBuf::from(value("--scan")?)),
            "--pid" => {
                let pid = number("--pid", value("--pid")?)?;
                request.pids.get_or_insert_with(Vec::new).push(pid);
            }
            "--until-clean" => {
                let ms = number("--until-clean", value("--until-clean")?)?;
                request.until_clean = Some(Duration::from_millis(ms));
            }
            "--budget-ms" => {
                let ms = number("--budget-ms", value("--budget-ms")?)?;
                request.budget = Some(Duration::from_millis(ms));
            }
            _ => return Err(Usage::Option(flag.to_string_lossy().into_owned())),
        }
    }
    Ok(request)
}

fn parse_drain(mut args: impl Iterator<Item = OsString>) -> Result<Drain, Usage> {
    let (mut origin, mut budget, mut scans) = (None, None, Vec::new());
    let mut concurrency = Concurrency::default();
    while let Some(flag) = args.next() {
        let mut value = |name: &'static str| args.next().ok_or(Usage::Missing(name));
        match flag.to_str().unwrap_or_default() {
            "--origin" => {
                let given = value("--origin")?;
                let text = given
                    .to_str()
                    .ok_or_else(|| Usage::OriginBytes(given.to_string_lossy().into_owned()))?;
                origin = Some(Origin::try_from(text)?);
            }
            "--budget-ms" => match number::<u64>("--budget-ms", value("--budget-ms")?)? {
                0 => return Err(Usage::NoBudget),
                ms => budget = Some(Duration::from_millis(ms)),
            },
            "--scan" => scans.push(PathBuf::from(value("--scan")?)),
            "--concurrency" => {
                let n = number::<u64>("--concurrency", value("--concurrency")?)?;
                concurrency = Concurrency::try_from(n)?;
            }
            _ => return Err(Usage::Option(flag.to_string_lossy().into_owned())),
        }
    }
    Ok(Drain {
        origin: origin.ok_or(Usage::Required("--origin"))?,
        budget: budget.ok_or(Usage::Required("--budget-ms"))?,
        scans,
        concurrency,
    })
}

fn print_drained(drained: &Drained) -> std::io::Result<()> {
    let mut stdout = std::io::stdout().lock();
    writeln!(stdout, "{}", drained.to_json())?;
    stdout.flush()
}

fn print(inspection: &Inspection) -> std::io::Result<()> {
    let mut stdout = std::io::stdout().lock();
    writeln!(stdout, "{}", inspection.to_json())?;
    stdout.flush()
}

/// Why `hold` failed.
#[derive(Debug, thiserror::Error)]
enum HoldFailed {
    #[error(transparent)]
    Take(#[from] hold::HoldError),
    #[error("stdout: {0}")]
    Stdout(#[source] std::io::Error),
    #[error("stdin: {0}")]
    Stdin(#[source] std::io::Error),
}

/// Takes the hold, prints `held`, and holds it until stdin closes.
fn hold_until_stdin_closes(dir: Option<PathBuf>) -> Result<(), HoldFailed> {
    let held = hold::take(dir.as_deref())?;
    let mut stdout = std::io::stdout().lock();
    writeln!(stdout, "held")
        .and_then(|()| stdout.flush())
        .map_err(HoldFailed::Stdout)?;
    let mut sink = [0u8; 4096];
    let mut stdin = std::io::stdin().lock();
    loop {
        match stdin.read(&mut sink) {
            Ok(0) => break,
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
            Err(e) => return Err(HoldFailed::Stdin(e)),
        }
    }
    drop(held);
    Ok(())
}

fn main() -> ExitCode {
    let command = match parse(std::env::args_os().skip(1)) {
        Ok(command) => command,
        Err(usage) => {
            return match writeln!(std::io::stderr(), "ursula-vfs-host: {usage}\n{USAGE}") {
                Ok(()) => ExitCode::from(2),
                Err(_closed) => ExitCode::FAILURE,
            };
        }
    };
    match command {
        Command::Inspect(request) => match print(&inspect(&request)) {
            Ok(()) => ExitCode::SUCCESS,
            Err(_closed) => ExitCode::FAILURE,
        },
        Command::Drain(request) => match print_drained(&drain(&request)) {
            Ok(()) => ExitCode::SUCCESS,
            Err(_closed) => ExitCode::FAILURE,
        },
        Command::Hold { dir } => match hold_until_stdin_closes(dir) {
            Ok(()) => ExitCode::SUCCESS,
            Err(e) => {
                if let Err(_closed) = writeln!(std::io::stderr(), "ursula-vfs-host hold: {e}") {}
                ExitCode::FAILURE
            }
        },
    }
}

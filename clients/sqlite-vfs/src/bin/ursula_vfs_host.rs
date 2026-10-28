//! `ursula-vfs-host`: a host's tool for the roots on its disk (design: Hosts), for a host that
//! snapshots or moves the process, or the VM it runs in. It never loads SQLite or the extension,
//! and it never writes or locks anything.
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
//! It exits 0 when it printed a result and 2 on a usage error (1 when it could not write either).

use std::ffi::OsString;
use std::io::Write;
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Duration;

use sqlite_ursula_vfs::inspection::Inspect;
use sqlite_ursula_vfs::inspection::Inspection;
use sqlite_ursula_vfs::inspection::inspect;

const USAGE: &str = "usage: ursula-vfs-host inspect [--dir D] [--scan S]... [--pid P]... \
                     [--until-clean MS] [--budget-ms MS]";

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

/// The inspection the arguments `args` ask for. A path may be any bytes, as a home directory
/// taken from the password database may be.
fn parse(mut args: impl Iterator<Item = OsString>) -> Result<Inspect, Usage> {
    match args.next() {
        Some(command) if command == "inspect" => {}
        Some(other) => return Err(Usage::Command(other.to_string_lossy().into_owned())),
        None => return Err(Usage::NoCommand),
    }
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

fn print(inspection: &Inspection) -> std::io::Result<()> {
    let mut stdout = std::io::stdout().lock();
    writeln!(stdout, "{}", inspection.to_json())?;
    stdout.flush()
}

fn main() -> ExitCode {
    let request = match parse(std::env::args_os().skip(1)) {
        Ok(request) => request,
        Err(usage) => {
            return match writeln!(std::io::stderr(), "ursula-vfs-host: {usage}\n{USAGE}") {
                Ok(()) => ExitCode::from(2),
                Err(_closed) => ExitCode::FAILURE,
            };
        }
    };
    match print(&inspect(&request)) {
        Ok(()) => ExitCode::SUCCESS,
        Err(_closed) => ExitCode::FAILURE,
    }
}

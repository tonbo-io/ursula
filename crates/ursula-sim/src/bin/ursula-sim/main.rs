//! `ursula-sim` — unified CLI for the deterministic simulation harness.
//!
//! Subcommands:
//!
//! - `smoke`: replay the checked-in corpora, then sweep seeds and write
//!   failure artifacts.
//! - `replay`: re-run a seed or recorded artifact and check its outcome.
//! - `minimize`: shrink a failing schedule while preserving a target
//!   predicate.
//! - `record`: write the scheduled-record JSON for a single seed.
//! - `assert-shape`: typed shape assertions for CI-generated minimize
//!   artifacts.
//!
//! Every subcommand requires `RUSTFLAGS="--cfg madsim"`.

use std::error::Error;

#[cfg(madsim)]
mod assert_shape;
#[cfg(madsim)]
mod minimize;
#[cfg(madsim)]
mod record;
#[cfg(madsim)]
mod replay;
#[cfg(madsim)]
mod smoke;

const USAGE: &str = "ursula-sim <smoke|replay|minimize|record|assert-shape> [args]";

#[cfg(madsim)]
fn main() -> Result<(), Box<dyn Error>> {
    let mut args = std::env::args().skip(1);
    let Some(subcommand) = args.next() else {
        return Err(format!("usage: {USAGE}").into());
    };
    let rest: Vec<String> = args.collect();
    match subcommand.as_str() {
        "smoke" => smoke::run(rest),
        "replay" => replay::run(rest),
        "minimize" => minimize::run(rest),
        "record" => record::run(rest),
        "assert-shape" => assert_shape::run(rest),
        "--help" | "-h" => {
            println!("{USAGE}");
            Ok(())
        }
        other => Err(format!("unknown subcommand `{other}`\nusage: {USAGE}").into()),
    }
}

#[cfg(not(madsim))]
fn main() -> Result<(), Box<dyn Error>> {
    Err(format!("ursula-sim must run with RUSTFLAGS=\"--cfg madsim\"\nusage: {USAGE}").into())
}

#[cfg(madsim)]
fn init_stderr_tracing() {
    // fmt-only: this crate pulls observability without the `otlp` feature, so
    // the returned guard is inert and can be dropped immediately.
    let _ = ursula_observability::init(ursula_observability::InitOptions::new("ursula-sim"));
}

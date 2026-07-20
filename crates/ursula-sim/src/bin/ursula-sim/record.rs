//! `ursula-sim record`: write the scheduled-record JSON for a single seed.

use std::error::Error;
use std::fs;
use std::path::PathBuf;

pub fn run(args: Vec<String>) -> Result<(), Box<dyn Error>> {
    let args = Args::parse(args)?;
    let record = ursula_sim::SimScheduledRecord::from_seed(args.seed);
    let encoded = serde_json::to_string_pretty(&record)?;
    write_output(args.output, encoded)?;
    Ok(())
}

const USAGE: &str = "ursula-sim record <seed> [output.json]";

struct Args {
    seed: u64,
    output: Option<PathBuf>,
}

impl Args {
    fn parse(args: Vec<String>) -> Result<Self, Box<dyn Error>> {
        let mut args = args.into_iter();
        let seed = args
            .next()
            .ok_or_else(|| format!("usage: {USAGE}"))?
            .parse::<u64>()?;
        let output = args.next().map(PathBuf::from);
        if args.next().is_some() {
            return Err(format!("usage: {USAGE}").into());
        }
        Ok(Self { seed, output })
    }
}

fn write_output(output: Option<PathBuf>, encoded: String) -> Result<(), Box<dyn Error>> {
    match output {
        Some(path) => {
            let mut body = encoded;
            body.push('\n');
            fs::write(path, body)?;
        }
        None => {
            println!("{encoded}");
        }
    }
    Ok(())
}

use std::env;
use std::error::Error;
#[cfg(madsim)]
use std::fs;
use std::path::PathBuf;

#[cfg(madsim)]
fn main() -> Result<(), Box<dyn Error>> {
    use ursula_sim::SimScheduledRecord;

    let args = Args::parse()?;
    let record = SimScheduledRecord::from_seed(args.seed);
    let encoded = serde_json::to_string_pretty(&record)?;
    write_output(args.output, encoded)?;
    Ok(())
}

#[cfg(not(madsim))]
fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse()?;
    let _ = (args.seed, args.output);
    Err("ursula-sim-record must run with RUSTFLAGS=\"--cfg madsim\"".into())
}

struct Args {
    seed: u64,
    output: Option<PathBuf>,
}

impl Args {
    fn parse() -> Result<Self, Box<dyn Error>> {
        let mut args = env::args().skip(1);
        let seed = args
            .next()
            .ok_or_else(|| format!("usage: {} <seed> [output.json]", bin_name()))?
            .parse::<u64>()?;
        let output = args.next().map(PathBuf::from);
        if args.next().is_some() {
            return Err(format!("usage: {} <seed> [output.json]", bin_name()).into());
        }
        Ok(Self { seed, output })
    }
}

#[cfg(madsim)]
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

fn bin_name() -> String {
    env::args()
        .next()
        .unwrap_or_else(|| "ursula-sim-record".to_owned())
}

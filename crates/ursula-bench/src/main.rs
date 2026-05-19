mod backend;
mod bootstrap;
mod common;
mod fanout;
mod multi_stream;

use anyhow::Result;
use clap::{Parser, Subcommand};

#[derive(Parser, Debug)]
#[command(
    name = "ursula-bench",
    version,
    about = "Ursula real-world workload benchmark client"
)]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand, Debug)]
enum Cmd {
    /// Multi-stream concurrent write - proves multi-Raft sharding scales with stream count.
    MultiStream(multi_stream::MultiStreamArgs),
    /// SSE fan-out - single stream, many subscribers, measure per-event end-to-end latency.
    FanOut(fanout::FanOutArgs),
    /// Bootstrap stampede - N clients hit /bootstrap simultaneously after a snapshot.
    Bootstrap(bootstrap::BootstrapArgs),
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .with_writer(std::io::stderr)
        .init();

    let cli = Cli::parse();
    let json = match cli.cmd {
        Cmd::MultiStream(args) => serde_json::to_string_pretty(&multi_stream::run(args).await?)?,
        Cmd::FanOut(args) => serde_json::to_string_pretty(&fanout::run(args).await?)?,
        Cmd::Bootstrap(args) => serde_json::to_string_pretty(&bootstrap::run(args).await?)?,
    };
    println!("{json}");
    Ok(())
}

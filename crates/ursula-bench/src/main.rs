//! `ursula-bench`: workload benchmark client.
//!
//! Module map:
//! - `backend`: target API styles (Ursula, Durable Streams reference) and request builders.
//! - `common`: HTTP client, HDR histogram helpers, payload generation.
//! - `multi_stream`: concurrent writers, one stream each (producer-header octet appends).
//! - `fanout`: SSE fan-out latency.
//! - `bootstrap`: `/bootstrap` stampede.
//! - `record_match`: serialized single-writer `Stream-Record-Match` JSON appends.

mod backend;
mod bootstrap;
mod common;
mod fanout;
mod multi_stream;
mod record_match;

use anyhow::Result;
use clap::Parser;
use clap::Subcommand;

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
    /// Serialized single-writer `Stream-Record-Match` JSON appends (commit latency).
    RecordMatch(record_match::RecordMatchArgs),
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let _telemetry =
        ursula_observability::init(ursula_observability::InitOptions::new("ursula-bench"));

    let cli = Cli::parse();
    let json = match cli.cmd {
        Cmd::MultiStream(args) => serde_json::to_string_pretty(&multi_stream::run(args).await?)?,
        Cmd::FanOut(args) => serde_json::to_string_pretty(&fanout::run(args).await?)?,
        Cmd::Bootstrap(args) => serde_json::to_string_pretty(&bootstrap::run(args).await?)?,
        Cmd::RecordMatch(args) => serde_json::to_string_pretty(&record_match::run(args).await?)?,
    };
    println!("{json}");
    Ok(())
}

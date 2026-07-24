use std::ffi::OsString;

use clap::Parser;
use clap::Subcommand;
use ursula::server::ServerArgs;
use ursula_gateway::service::GatewayArgs;
use ursula_index::service::IndexerArgs;

#[cfg(not(feature = "jemalloc-prof"))]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[cfg(feature = "jemalloc-prof")]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[derive(Parser, Debug)]
#[command(version, about = "Ursula durable-stream services")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Run a stateful Ursula Durable Streams server node.
    Server(ServerArgs),
    /// Run the stateless public HTTP/SSE gateway.
    Gateway(GatewayArgs),
    /// Run the rebuildable event-time indexer worker pool.
    Indexer(Box<IndexerArgs>),
}

#[derive(Parser, Debug)]
struct LegacyServerCli {
    #[command(flatten)]
    server: ServerArgs,
}

#[tokio::main]
async fn main() {
    let result = match parse_command() {
        Command::Server(args) => ursula::server::run(args).await,
        Command::Gateway(args) => ursula_gateway::service::run(args).await,
        Command::Indexer(args) => ursula_index::service::run(*args).await.map_err(Into::into),
    };
    if let Err(error) = result {
        clap::Error::raw(clap::error::ErrorKind::Io, error.to_string()).exit();
    }
}

fn parse_command() -> Command {
    parse_command_from(std::env::args_os()).unwrap_or_else(|error| error.exit())
}

fn parse_command_from(args: impl IntoIterator<Item = OsString>) -> Result<Command, clap::Error> {
    let args = args.into_iter().collect::<Vec<_>>();
    let first = args.get(1).and_then(|value| value.to_str());
    match first {
        None => Ok(Command::Server(ServerArgs::default())),
        Some("server" | "gateway" | "indexer" | "help")
        | Some("-h" | "--help" | "-V" | "--version") => {
            Cli::try_parse_from(args).map(|cli| cli.command)
        }
        Some(value) if value.starts_with('-') => {
            LegacyServerCli::try_parse_from(args).map(|cli| Command::Server(cli.server))
        }
        Some(_) => Cli::try_parse_from(args).map(|cli| cli.command),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(args: &[&str]) -> Result<Command, clap::Error> {
        parse_command_from(args.iter().map(OsString::from))
    }

    #[test]
    fn no_subcommand_keeps_zero_config_server_mode() {
        assert!(matches!(
            parse(&["ursula"]).expect("default command"),
            Command::Server(_)
        ));
    }

    #[test]
    fn legacy_server_flags_remain_accepted() {
        assert!(matches!(
            parse(&["ursula", "--preset", "tiny"]).expect("legacy server command"),
            Command::Server(_)
        ));
    }

    #[test]
    fn deployment_roles_are_explicit_subcommands() {
        assert!(matches!(
            parse(&["ursula", "server", "--preset", "tiny"]).expect("server command"),
            Command::Server(_)
        ));
        assert!(matches!(
            parse(&["ursula", "gateway", "--upstream", "http://127.0.0.1:4437"])
                .expect("gateway command"),
            Command::Gateway(_)
        ));
        assert!(matches!(
            parse(&[
                "ursula",
                "indexer",
                "--object-dir",
                "/tmp/index",
                "--cache-dir",
                "/tmp/cache"
            ])
            .expect("indexer command"),
            Command::Indexer(_)
        ));
    }
}

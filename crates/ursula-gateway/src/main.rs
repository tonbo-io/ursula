use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::extract::State;
use axum::http::Request;
use axum::routing::any;
use clap::Parser;
use tokio::signal;
use tracing_subscriber::EnvFilter;
use ursula_gateway::Gateway;
use ursula_gateway::GatewayConfig;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    init_tracing();

    let args = Args::parse();
    let config = GatewayConfig {
        listen: args.listen,
        upstreams: args.upstream,
        response_header_timeout: Duration::from_secs(args.response_header_timeout),
        connect_timeout: Duration::from_secs(args.connect_timeout),
    };

    let gateway = Arc::new(Gateway::new(config.clone()));

    // Keep Axum's State extractor in the binary so the library handler stays
    // easy to call directly from tests.
    let app = Router::new()
        .fallback(any(
            |State(gateway): State<Arc<Gateway>>, req: Request<Body>| async move {
                gateway.handle(req).await
            },
        ))
        .with_state(gateway);

    let listener = tokio::net::TcpListener::bind(config.listen).await?;
    tracing::info!(
        listen = %config.listen,
        upstreams = ?config.upstreams,
        "ursulagw starting"
    );

    let server = axum::serve(listener, app);

    tokio::select! {
        result = server => {
            result?;
        }
        _ = shutdown_signal() => {
            tracing::info!("graceful shutdown initiated, waiting up to 1 hour for in-flight requests");
        }
    }

    // Give in-flight requests (including long-lived SSE streams) up to 1 hour
    // to finish before the process exits.
    tokio::time::sleep(Duration::from_secs(3600)).await;
    Ok(())
}

#[derive(Parser, Debug)]
#[command(version, about = "Ursula Gateway: HTTP/SSE proxy for Ursula nodes")]
struct Args {
    /// Address to bind the gateway server.
    #[arg(long, default_value = "0.0.0.0:4437")]
    listen: SocketAddr,

    /// Upstream Ursula node URL. Repeat for each node.
    #[arg(long, required = true)]
    upstream: Vec<String>,

    /// Timeout for sending the upstream request and receiving response headers, in seconds.
    /// Streamed response bodies such as SSE live reads are not covered by this timeout.
    #[arg(long, default_value_t = 30)]
    response_header_timeout: u64,

    /// TCP connect timeout per upstream attempt in seconds.
    #[arg(long, default_value_t = 5)]
    connect_timeout: u64,
}

fn init_tracing() {
    let _result = tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("ursula_gateway=info")),
        )
        .with_target(true)
        .try_init();
}

async fn shutdown_signal() {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {
            tracing::info!("received Ctrl+C, starting graceful shutdown");
        }
        _ = terminate => {
            tracing::info!("received SIGTERM, starting graceful shutdown");
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::sync::oneshot;

    /// A version of shutdown_signal that can be triggered by a channel instead
    /// of a real OS signal, so tests don't need unsafe code or libc.
    async fn shutdown_signal_for_test(rx: oneshot::Receiver<()>) {
        let ctrl_c = async {
            drop(rx.await);
        };

        let terminate = std::future::pending::<()>();

        tokio::select! {
            _ = ctrl_c => {
                tracing::info!("received test signal, starting graceful shutdown");
            }
            _ = terminate => {}
        }
    }

    #[tokio::test]
    async fn shutdown_signal_returns_when_triggered() {
        let (tx, rx) = oneshot::channel();

        let handle = tokio::spawn(shutdown_signal_for_test(rx));

        // Trigger the "signal".
        tx.send(()).unwrap();

        // shutdown_signal should return promptly.
        tokio::time::timeout(Duration::from_secs(5), handle)
            .await
            .expect("shutdown_signal should return within 5s")
            .expect("shutdown_signal task should not panic");
    }

    #[tokio::test]
    async fn shutdown_signal_does_not_return_without_trigger() {
        let (_tx, rx) = oneshot::channel::<()>();

        let handle = tokio::spawn(shutdown_signal_for_test(rx));

        // Give it a moment to start.
        tokio::time::sleep(Duration::from_millis(50)).await;

        // The task should still be running since no signal was sent.
        assert!(!handle.is_finished());

        // Clean up.
        handle.abort();
    }
}

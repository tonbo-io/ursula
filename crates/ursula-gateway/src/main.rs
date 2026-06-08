use std::future::Future;
use std::io;
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
use tokio::sync::oneshot;
use tracing_subscriber::EnvFilter;
use ursula_gateway::DEFAULT_MAX_REQUEST_BODY_BYTES;
use ursula_gateway::Gateway;
use ursula_gateway::GatewayConfig;

const DEFAULT_GRACEFUL_SHUTDOWN_TIMEOUT_SECS: u64 = 3600;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    init_tracing();

    let args = Args::parse();
    let config = GatewayConfig {
        listen: args.listen,
        upstreams: args.upstream,
        response_header_timeout: Duration::from_secs(args.response_header_timeout),
        connect_timeout: Duration::from_secs(args.connect_timeout),
        max_request_body_bytes: args.max_request_body_bytes,
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

    serve_with_shutdown(
        listener,
        app,
        shutdown_signal(),
        Duration::from_secs(args.graceful_shutdown_timeout),
    )
    .await?;

    Ok(())
}

async fn serve_with_shutdown(
    listener: tokio::net::TcpListener,
    app: Router,
    shutdown: impl Future<Output = ()> + Send + 'static,
    drain_timeout: Duration,
) -> io::Result<()> {
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let server = axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            drop(shutdown_rx.await);
        })
        .into_future();
    tokio::pin!(server);

    tokio::select! {
        result = &mut server => result,
        _ = shutdown => {
            tracing::info!(
                drain_timeout_secs = drain_timeout.as_secs(),
                "graceful shutdown initiated"
            );
            if shutdown_tx.send(()).is_err() {
                tracing::debug!("graceful shutdown receiver already dropped");
            }
            match tokio::time::timeout(drain_timeout, &mut server).await {
                Ok(result) => result,
                Err(_) => {
                    tracing::warn!(
                        drain_timeout_secs = drain_timeout.as_secs(),
                        "graceful shutdown timeout elapsed; exiting"
                    );
                    Ok(())
                }
            }
        }
    }
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

    /// Maximum request body bytes buffered for leader-redirect replay.
    #[arg(long, default_value_t = DEFAULT_MAX_REQUEST_BODY_BYTES)]
    max_request_body_bytes: usize,

    /// Maximum graceful shutdown drain time after SIGTERM/CTRL-C, in seconds.
    #[arg(long, default_value_t = DEFAULT_GRACEFUL_SHUTDOWN_TIMEOUT_SECS)]
    graceful_shutdown_timeout: u64,
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

    use axum::Router;
    use tokio::sync::oneshot;

    use super::serve_with_shutdown;

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

    #[tokio::test]
    async fn serve_with_shutdown_does_not_return_before_shutdown_signal() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind test listener");
        let app = Router::new();
        let (_tx, rx) = oneshot::channel::<()>();

        let handle = tokio::spawn(serve_with_shutdown(
            listener,
            app,
            async move {
                drop(rx.await);
            },
            Duration::from_secs(60),
        ));

        // Give the server a moment to start.
        tokio::time::sleep(Duration::from_millis(50)).await;

        // The task should still be running since no shutdown signal was sent.
        assert!(!handle.is_finished());

        // Clean up.
        handle.abort();
    }

    #[tokio::test]
    async fn serve_with_shutdown_returns_promptly_after_shutdown_signal() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind test listener");
        let app = Router::new();
        let (tx, rx) = oneshot::channel();

        let handle = tokio::spawn(serve_with_shutdown(
            listener,
            app,
            async move {
                drop(rx.await);
            },
            Duration::from_secs(60),
        ));

        tx.send(()).expect("send shutdown signal");

        tokio::time::timeout(Duration::from_secs(1), handle)
            .await
            .expect("server should return promptly after shutdown signal")
            .expect("server task should not panic")
            .expect("server should stop cleanly");
    }

    #[tokio::test]
    async fn serve_with_shutdown_returns_ok_when_drain_timeout_elapses() {
        use axum::routing::get;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind test listener");
        let addr = listener.local_addr().expect("listener local addr");

        // Handler that keeps the connection occupied for longer than the drain timeout.
        let app = Router::new().route(
            "/slow",
            get(|| async {
                tokio::time::sleep(Duration::from_secs(3600)).await;
                "done"
            }),
        );

        let (shutdown_tx, shutdown_rx) = oneshot::channel();

        let handle = tokio::spawn(serve_with_shutdown(
            listener,
            app,
            async move {
                drop(shutdown_rx.await);
            },
            Duration::from_millis(100), // short drain timeout
        ));

        // Start a slow request so the server has an active connection.
        let client = reqwest::Client::new();
        let client_handle = tokio::spawn(async move {
            drop(client.get(format!("http://{addr}/slow")).send().await);
        });

        // Wait for the request to reach the handler.
        tokio::time::sleep(Duration::from_millis(50)).await;

        shutdown_tx.send(()).expect("send shutdown signal");

        // Server should return after drain_timeout despite the slow request.
        let result = tokio::time::timeout(Duration::from_secs(2), handle)
            .await
            .expect("server should return within 2s")
            .expect("server task should not panic");

        assert!(
            result.is_ok(),
            "server should return Ok when drain timeout elapses"
        );

        client_handle.abort();
    }
}

//! Shared shutdown-signal handling and graceful HTTP serving for Ursula
//! binaries.
//!
//! Every Ursula binary translates SIGTERM (systemd stop, Kubernetes pod
//! termination) and Ctrl-C into the same graceful-drain dance; this module
//! keeps that plumbing in one place. Binaries that need extra policy (forced
//! exit after a grace period, multiple listeners) compose these primitives
//! instead of re-rolling signal handling.

use std::future::Future;
use std::future::IntoFuture;
use std::io;
use std::time::Duration;

/// Wait for a termination signal: SIGTERM or Ctrl-C (Ctrl-C only on
/// non-unix). If the SIGTERM handler cannot be installed, falls back to
/// Ctrl-C alone instead of failing.
pub async fn shutdown_signal() {
    #[cfg(unix)]
    {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut sigterm) => {
                tokio::select! {
                    _ = tokio::signal::ctrl_c() => {
                        tracing::info!("received Ctrl+C, starting graceful shutdown");
                    }
                    _ = sigterm.recv() => {
                        tracing::info!("received SIGTERM, starting graceful shutdown");
                    }
                }
            }
            Err(err) => {
                tracing::error!("install SIGTERM handler: {err}; falling back to Ctrl-C only");
                let _ = tokio::signal::ctrl_c().await;
                tracing::info!("received Ctrl+C, starting graceful shutdown");
            }
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
        tracing::info!("received Ctrl+C, starting graceful shutdown");
    }
}

/// Serve `listener` with `app` until `shutdown` completes, then stop accepting
/// new connections and drain in-flight requests before returning.
///
/// `drain_timeout` bounds the drain: with `Some(t)` the server returns `Ok`
/// after `t` even if connections are still open; with `None` the drain is
/// unbounded (callers enforce their own deadline, e.g. via a forced-exit
/// watchdog).
pub async fn serve_until_shutdown(
    listener: tokio::net::TcpListener,
    app: axum::Router,
    shutdown: impl Future<Output = ()> + Send + 'static,
    drain_timeout: Option<Duration>,
) -> io::Result<()> {
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let server = axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            drop(shutdown_rx.await);
        })
        .into_future();
    tokio::pin!(server);

    tokio::select! {
        result = &mut server => result,
        () = shutdown => {
            if let Some(timeout) = drain_timeout {
                tracing::info!(
                    drain_timeout_secs = timeout.as_secs(),
                    "graceful shutdown initiated"
                );
            }
            if shutdown_tx.send(()).is_err() {
                tracing::debug!("graceful shutdown receiver already dropped");
            }
            match drain_timeout {
                None => server.await,
                Some(timeout) => match tokio::time::timeout(timeout, &mut server).await {
                    Ok(result) => result,
                    Err(_) => {
                        tracing::warn!(
                            drain_timeout_secs = timeout.as_secs(),
                            "graceful shutdown timeout elapsed; exiting"
                        );
                        Ok(())
                    }
                },
            }
        }
    }
}

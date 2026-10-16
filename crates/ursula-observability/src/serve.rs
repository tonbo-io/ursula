//! Shared shutdown-signal handling and graceful HTTP serving for Ursula
//! binaries.
//!
//! Every Ursula binary translates SIGTERM (systemd stop, Kubernetes pod
//! termination) and Ctrl-C into the same graceful-drain dance; this module
//! keeps that plumbing in one place. Binaries that need extra policy (forced
//! exit after a grace period, multiple listeners) compose these primitives
//! instead of re-rolling signal handling.

use std::fmt::Debug;
use std::future::Future;
use std::future::IntoFuture;
use std::io;
use std::time::Duration;

use axum::serve::Listener;
use axum::serve::ListenerExt;
use axum::serve::TapIo;
use tokio::net::TcpStream;

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
                if let Err(err) = tokio::signal::ctrl_c().await {
                    // Without any signal handler, only process exit stops the server.
                    tracing::error!(
                        "install Ctrl-C handler: {err}; graceful shutdown is unavailable"
                    );
                    std::future::pending::<()>().await;
                }
                tracing::info!("received Ctrl+C, starting graceful shutdown");
            }
        }
    }
    #[cfg(not(unix))]
    {
        if let Err(err) = tokio::signal::ctrl_c().await {
            // Without any signal handler, only process exit stops the server.
            tracing::error!("install Ctrl-C handler: {err}; graceful shutdown is unavailable");
            std::future::pending::<()>().await;
        }
        tracing::info!("received Ctrl+C, starting graceful shutdown");
    }
}

/// Wrap `listener` so that every socket it accepts sets `TCP_NODELAY`.
///
/// Without it, Nagle's algorithm holds a reply while an earlier reply on the
/// same connection is unacknowledged, and the peer delays that
/// acknowledgement (at least 40 ms on Linux) until its next request. A Raft
/// follower that writes two replies in a row then answers each ReadIndex
/// round one round late (#521, #547).
fn nodelay_listener<L>(listener: L) -> TapIo<L, fn(&mut TcpStream)>
where L: Listener<Io = TcpStream> {
    listener.tap_io(set_nodelay as fn(&mut TcpStream))
}

fn set_nodelay(stream: &mut TcpStream) {
    if let Err(err) = stream.set_nodelay(true) {
        tracing::warn!("set TCP_NODELAY on accepted socket: {err}");
    }
}

/// Serve `listener` with `app` until `shutdown` completes, then stop accepting
/// new connections and drain in-flight requests before returning.
///
/// Every connection it accepts sets `TCP_NODELAY`: without it, Nagle's
/// algorithm and the peer's delayed acknowledgement hold back a second reply
/// on the same connection by 40 ms or more. Each request carries its
/// connection's peer address as `axum::extract::ConnectInfo<L::Addr>`, a
/// `SocketAddr` for a TCP listener.
///
/// `drain_timeout` bounds the drain: with `Some(t)` the server returns `Ok`
/// after `t` even if connections are still open; with `None` the drain is
/// unbounded (callers enforce their own deadline, e.g. via a forced-exit
/// watchdog).
pub async fn serve_until_shutdown<L>(
    listener: L,
    app: axum::Router,
    shutdown: impl Future<Output = ()> + Send + 'static,
    drain_timeout: Option<Duration>,
) -> io::Result<()>
where
    L: Listener<Io = TcpStream>,
    L::Addr: Debug + Clone + Sync + 'static,
{
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    // Handlers see each connection's peer address as
    // `axum::extract::ConnectInfo<L::Addr>`.
    let app = app.into_make_service_with_connect_info::<L::Addr>();
    let server = axum::serve(nodelay_listener(listener), app)
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
                            "graceful drain timed out; abandoning the connections still open"
                        );
                        Ok(())
                    }
                },
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;
    use std::sync::Arc;
    use std::sync::Mutex;

    use axum::serve::Listener;
    use tokio::io::AsyncReadExt;
    use tokio::io::AsyncWriteExt;
    use tokio::net::TcpListener;
    use tokio::net::TcpStream;

    use super::serve_until_shutdown;

    /// A [`TcpListener`] that keeps a duplicate of every socket it accepts,
    /// so that a test can read the socket options the server sets on it.
    struct DuplicatingListener {
        inner: TcpListener,
        accepted: Arc<Mutex<Vec<std::net::TcpStream>>>,
    }

    impl Listener for DuplicatingListener {
        type Addr = SocketAddr;
        type Io = TcpStream;

        async fn accept(&mut self) -> (TcpStream, SocketAddr) {
            let (stream, peer) = self.inner.accept().await.unwrap();
            let stream = stream.into_std().unwrap();
            self.accepted
                .lock()
                .unwrap()
                .push(stream.try_clone().unwrap());
            (TcpStream::from_std(stream).unwrap(), peer)
        }

        fn local_addr(&self) -> std::io::Result<SocketAddr> {
            self.inner.local_addr()
        }
    }

    /// `serve_until_shutdown` sets `TCP_NODELAY` on every connection it
    /// accepts.
    #[tokio::test]
    async fn serve_until_shutdown_sets_nodelay_on_accepted_connections() {
        let inner = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = inner.local_addr().unwrap();
        let accepted = Arc::new(Mutex::new(Vec::new()));
        let listener = DuplicatingListener {
            inner,
            accepted: Arc::clone(&accepted),
        };
        let app = axum::Router::new().route("/", axum::routing::get(|| async { "ok" }));
        let server = tokio::spawn(serve_until_shutdown(
            listener,
            app,
            std::future::pending(),
            None,
        ));

        let mut client = TcpStream::connect(address).await.unwrap();
        client
            .write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
            .await
            .unwrap();
        let mut response = String::new();
        client.read_to_string(&mut response).await.unwrap();
        assert!(response.starts_with("HTTP/1.1 200"), "{response}");

        let accepted = accepted.lock().unwrap();
        assert_eq!(accepted.len(), 1);
        assert!(accepted[0].nodelay().unwrap());
        drop(accepted);
        server.abort();
    }
}

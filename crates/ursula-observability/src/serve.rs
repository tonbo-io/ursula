//! Shared shutdown-signal handling and graceful HTTP serving for Ursula
//! binaries.
//!
//! Every Ursula binary translates SIGTERM (systemd stop, Kubernetes pod
//! termination) and Ctrl-C into the same graceful-drain dance; this module
//! keeps that plumbing in one place. Binaries that need extra policy (forced
//! exit after a grace period, multiple listeners) compose these primitives
//! instead of re-rolling signal handling.
//!
//! A drain answers a request a client sends on a pooled HTTP/1 connection
//! that was idle when the drain began, within the bounds and with the gaps
//! that [`serve_until_shutdown`] names. See also [`CLIENT_IDLE_BOUND`].

use std::fmt::Debug;
use std::future::Future;
use std::future::IntoFuture;
use std::io;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;
use std::time::Duration;

use axum::extract::Request;
use axum::extract::State;
use axum::http::HeaderValue;
use axum::http::StatusCode;
use axum::http::Version;
use axum::http::header::CONNECTION;
use axum::middleware::Next;
use axum::response::Response;
use axum::serve::Listener;
use axum::serve::ListenerExt;
use axum::serve::TapIo;
use tokio::io::AsyncRead;
use tokio::io::AsyncWrite;
use tokio::io::ReadBuf;
use tokio::net::TcpStream;
use tokio::sync::watch;

/// How long a drain keeps the connections it accepted open, at most, before
/// it closes the ones still idle: a client's pooled connection may carry a
/// request the server has not read yet, which a close then loses, and an
/// HTTP/1 client cannot tell that request from one the server read and lost.
/// See [`CLIENT_IDLE_BOUND`].
pub const CLOSE_NOTICE: Duration = Duration::from_secs(1);

/// The longest an HTTP/1 client may keep a pooled connection to an Ursula
/// server idle and still send a request on it, so that a drain answers a
/// request the client sends on a connection that was idle when the drain
/// began: the server keeps that connection open for [`CLOSE_NOTICE`], and the
/// rest of the notice covers a request sent at this bound on its way to the
/// server. [`serve_until_shutdown`] names the closes this does not cover. The
/// gateway's upstream pool keeps to it.
pub const CLIENT_IDLE_BOUND: Duration = Duration::from_millis(500);

const _: () = assert!(CLIENT_IDLE_BOUND.as_millis() < CLOSE_NOTICE.as_millis());

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
fn nodelay_listener<L>(
    listener: DrainingListener<L>,
) -> TapIo<DrainingListener<L>, fn(&mut Accepted<TcpStream>)>
where L: Listener<Io = TcpStream> {
    listener.tap_io(set_nodelay as fn(&mut Accepted<TcpStream>))
}

fn set_nodelay(stream: &mut Accepted<TcpStream>) {
    if let Err(err) = stream.io.set_nodelay(true) {
        tracing::warn!("set TCP_NODELAY on accepted socket: {err}");
    }
}

/// A listener that closes once its drain begins, so that a client's new
/// connection is refused, which tells the client that it sent nothing. Every
/// connection it accepted holds a receiver of `open` until it closes, so the
/// drain sees when the last one is gone.
struct DrainingListener<L> {
    /// `None` once closed.
    listener: Option<L>,
    /// The receiver each accepted connection clones. `None` once closed.
    open: Option<watch::Receiver<()>>,
    draining: watch::Receiver<bool>,
}

impl<L: Listener> Listener for DrainingListener<L> {
    type Addr = L::Addr;
    type Io = Accepted<L::Io>;

    async fn accept(&mut self) -> (Self::Io, Self::Addr) {
        let Self {
            listener,
            open,
            draining,
        } = self;
        if let (Some(inner), Some(open)) = (listener.as_mut(), open.as_ref()) {
            tokio::select! {
                biased;
                // An error means the server is gone, so its drain began too.
                _began = draining.wait_for(|draining| *draining) => {}
                (io, addr) = inner.accept() => {
                    return (Accepted { io, _open: open.clone() }, addr);
                }
            }
        }
        *listener = None;
        *open = None;
        std::future::pending().await
    }

    fn local_addr(&self) -> io::Result<Self::Addr> {
        self.listener
            .as_ref()
            .map_or_else(|| Err(io::ErrorKind::NotConnected.into()), L::local_addr)
    }
}

/// A connection a [`DrainingListener`] accepted. It holds a receiver of the
/// drain's `open` channel until the connection closes.
struct Accepted<Io> {
    io: Io,
    _open: watch::Receiver<()>,
}

impl<Io: AsyncRead + Unpin> AsyncRead for Accepted<Io> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().io).poll_read(cx, buf)
    }
}

impl<Io: AsyncWrite + Unpin> AsyncWrite for Accepted<Io> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().io).poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().io).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().io).poll_shutdown(cx)
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().io).poll_write_vectored(cx, bufs)
    }

    fn is_write_vectored(&self) -> bool {
        self.io.is_write_vectored()
    }
}

/// Once the drain began, marks an HTTP/1 answer `Connection: close`, so that
/// the server closes its connection after the answer and the client sends
/// nothing more on it. HTTP/2 announces its close with `GOAWAY` instead, and a
/// `101` keeps its upgrade.
async fn announce_close(
    State(draining): State<watch::Receiver<bool>>,
    request: Request,
    next: Next,
) -> Response {
    let http1 = matches!(
        request.version(),
        Version::HTTP_09 | Version::HTTP_10 | Version::HTTP_11
    );
    let mut response = next.run(request).await;
    if http1 && *draining.borrow() && response.status() != StatusCode::SWITCHING_PROTOCOLS {
        response
            .headers_mut()
            .insert(CONNECTION, HeaderValue::from_static("close"));
    }
    response
}

/// Serve `listener` with `app` until `shutdown` completes, then drain:
///
/// 1. The listener closes, so a new connection is refused, which tells its
///    client that it sent nothing. A connection the kernel completed but the
///    server had not accepted yet is reset instead, the one close of the
///    drain that its client cannot tell from a lost request.
/// 2. Every HTTP/1 answer the server starts from then on carries
///    `Connection: close`, and its connection closes after it. A connection
///    its client keeps idle stays open, so a request the client sends on it
///    is answered.
/// 3. Once every connection has closed, or after [`CLOSE_NOTICE`], the
///    connections still open close as each finishes its request in flight:
///    an idle HTTP/1 connection at once, an HTTP/2 connection after `GOAWAY`.
///
/// So a request a client sends on a pooled connection that went idle at most
/// [`CLIENT_IDLE_BOUND`] earlier is answered, if the connection was idle when
/// the drain began or its last answer started after that, the request reaches
/// the server within the rest of the notice (0.5 s), and `drain_timeout` is
/// at least [`CLOSE_NOTICE`]. Two closes are not covered:
///
/// - A connection the kernel completed but the server had not accepted when
///   the listener closed is reset, after its client may have written a
///   request on it.
/// - An answer the server started before the drain began carries no
///   `Connection: close`. Its connection closes after it, at once if it ends
///   after [`CLOSE_NOTICE`] and at the end of the notice otherwise, so its
///   client may send its next request into that close.
///
/// An HTTP/1 client cannot tell a request lost to either from one the server
/// read and lost, so a proxy answers it `502` and must not send it again.
///
/// Every connection it accepts sets `TCP_NODELAY`: without it, Nagle's
/// algorithm and the peer's delayed acknowledgement hold back a second reply
/// on the same connection by 40 ms or more. Each request carries its
/// connection's peer address as `axum::extract::ConnectInfo<L::Addr>`, a
/// `SocketAddr` for a TCP listener.
///
/// `drain_timeout` bounds the drain, [`CLOSE_NOTICE`] included: with `Some(t)`
/// the server returns `Ok` after `t` even if connections are still open; with
/// `None` the drain is unbounded (callers enforce their own deadline, e.g. via
/// a forced-exit watchdog).
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
    let (draining_tx, draining) = watch::channel(false);
    let (open_tx, open) = watch::channel(());
    let (close_tx, close_rx) = tokio::sync::oneshot::channel::<()>();
    let listener = DrainingListener {
        listener: Some(listener),
        open: Some(open),
        draining: draining.clone(),
    };
    // Handlers see each connection's peer address as
    // `axum::extract::ConnectInfo<L::Addr>`.
    let app = app
        .layer(axum::middleware::from_fn_with_state(
            draining,
            announce_close,
        ))
        .into_make_service_with_connect_info::<L::Addr>();
    let server = axum::serve(nodelay_listener(listener), app)
        .with_graceful_shutdown(async move {
            drop(close_rx.await);
        })
        .into_future();
    tokio::pin!(server);

    tokio::select! {
        result = &mut server => result,
        () = shutdown => {
            let began = tokio::time::Instant::now();
            if let Some(timeout) = drain_timeout {
                tracing::info!(
                    drain_timeout_secs = timeout.as_secs(),
                    "graceful shutdown initiated"
                );
            }
            draining_tx.send_replace(true);
            let notice = drain_timeout.map_or(CLOSE_NOTICE, |timeout| timeout.min(CLOSE_NOTICE));
            // The server goes on accepting nothing and answering requests on
            // the connections it has, until their clients close them.
            tokio::select! {
                result = &mut server => return result,
                closed = tokio::time::timeout(notice, open_tx.closed()) => {
                    if closed.is_err() {
                        tracing::debug!("connections still open after the close notice");
                    }
                }
            }
            if close_tx.send(()).is_err() {
                tracing::debug!("graceful shutdown receiver already dropped");
            }
            match drain_timeout {
                None => server.await,
                Some(timeout) => {
                    let left = timeout.saturating_sub(began.elapsed());
                    match tokio::time::timeout(left, &mut server).await {
                        Ok(result) => result,
                        Err(_elapsed) => {
                            tracing::warn!(
                                drain_timeout_secs = timeout.as_secs(),
                                "graceful drain timed out; abandoning the connections still open"
                            );
                            Ok(())
                        }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::time::Duration;

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

    /// Sends `GET /` on `client`, a kept-alive connection, and reads its
    /// answer, the head and the `Content-Length` body, lowercased. An error
    /// when the connection closes before the whole answer.
    async fn exchange(client: &mut TcpStream) -> std::io::Result<String> {
        client
            .write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .await?;
        let mut answer = Vec::new();
        loop {
            if let Some(head) = answer.windows(4).position(|window| window == b"\r\n\r\n") {
                let text = String::from_utf8_lossy(&answer).to_ascii_lowercase();
                let body = text
                    .lines()
                    .find_map(|line| line.strip_prefix("content-length:"))
                    .map_or(0, |len| len.trim().parse::<usize>().unwrap());
                if answer.len() >= head.saturating_add(4).saturating_add(body) {
                    return Ok(text);
                }
            }
            let mut chunk = [0_u8; 1024];
            let read = client.read(&mut chunk).await?;
            if read == 0 {
                return Err(std::io::ErrorKind::UnexpectedEof.into());
            }
            answer.extend_from_slice(&chunk[..read]);
        }
    }

    /// A drain answers a request that arrives on a kept-alive connection
    /// after the drain began, marked `Connection: close`, and closes the
    /// connection after the answer. A drain that closed the idle connection at
    /// once lost such a request, and a client that had sent it, such as the
    /// gateway, could only answer `502`.
    #[tokio::test]
    async fn a_drain_answers_a_request_on_a_kept_alive_connection_before_closing_it() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let app = axum::Router::new().route("/", axum::routing::get(|| async { "ok" }));
        let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
        let server = tokio::spawn(serve_until_shutdown(
            listener,
            app,
            async move {
                drop(stopped.await);
            },
            Some(Duration::from_secs(30)),
        ));
        let mut client = TcpStream::connect(address).await.unwrap();
        let before = exchange(&mut client).await.unwrap();
        assert!(before.starts_with("http/1.1 200"), "{before}");
        assert!(!before.contains("connection: close"), "{before}");

        stop.send(()).unwrap();
        // The drain began once the listener refuses a new connection. The
        // request follows a moment later, as a pooled client's next one does.
        while TcpStream::connect(address).await.is_ok() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;

        let during = exchange(&mut client)
            .await
            .expect("the drain answers a request on a kept-alive connection");
        assert!(during.starts_with("http/1.1 200"), "{during}");
        assert!(during.contains("connection: close"), "{during}");
        let mut rest = Vec::new();
        client.read_to_end(&mut rest).await.unwrap();
        assert!(rest.is_empty(), "{rest:?}");
        tokio::time::timeout(Duration::from_secs(10), server)
            .await
            .expect("the drain ends")
            .unwrap()
            .unwrap();
    }
}

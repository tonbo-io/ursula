//! The HTTP transport: one attempt of a request over blocking HTTP/1.1 (ureq), and the agents,
//! TLS, bearer token and failure classes the 0.7 client shares with it.
//!
//! [`HttpTransport`] sends a [`Request`] once and hands back what came of it. It never retries and
//! never follows a redirect: the retry policy belongs to the replicator. Every request with a body
//! carries `Content-Length`, so a node can answer `413` or an early answer before the body
//! (#581 §2.5, §6.1). A failure is [`NoAnswer::NotSent`] only when `net` proves nothing went out
//! (the resolver or the connector failed, `net::NotSent`): decided by where it failed, never by
//! its kind. Any other failure is [`NoAnswer::Lost`].
//!
//! Each attempt's time limits scale with its size (design §4.12, #566): connecting 5 s, sending
//! the request head 5 s, sending the body `5 s + n / rate` (`URSULA_VFS_MIN_UPLOAD_KBPS`, 256 KiB/s by default), the answer's
//! head 15 s, and its body `10 s + m / 256 KiB/s` for the `m` bytes the caller expects. A timeout
//! is only another lost attempt.

use std::fs;
use std::io::Read;
use std::net::IpAddr;
use std::sync::OnceLock;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::time::Duration;

use ureq::http::Uri;
use ureq::tls::Certificate;
use ureq::tls::PemItem;
use ureq::tls::RootCerts;
use ureq::tls::TlsConfig;

use crate::auth;
use crate::config::ca_file;
use crate::config::idle_age;
use crate::config::min_upload_kbps;
use crate::log;
use crate::log::Level;
use crate::net;
use crate::transport::Attempt;
use crate::transport::Transport;
use crate::wire::Exchange;
use crate::wire::Method;
use crate::wire::NoAnswer;
use crate::wire::Request;
use crate::wire::Target;
use crate::wire::parse_answer;

/// The largest answer body read: a read's chunk or a snapshot. An answer with a larger body is
/// lost (the attempt is repeated), never cut short.
#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "the client side is unwired until the 0.8 driver (PR5) sends through it"
    )
)]
const ANSWER_BODY_LIMIT: u64 = 2 << 30;

/// One stream over blocking HTTP/1.1. Snapshot transfers go on the bulk agent, every other request
/// on the agent of appends and reads.
#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "the client side is unwired until the 0.8 driver (PR5) sends through it"
    )
)]
#[derive(Debug, Clone)]
pub(crate) struct HttpTransport {
    url: String,
}

#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "the client side is unwired until the 0.8 driver (PR5) sends through it"
    )
)]
impl HttpTransport {
    /// The transport of the stream at `url`.
    pub(crate) fn new(url: impl Into<String>) -> Self {
        Self { url: url.into() }
    }
}

impl Transport for HttpTransport {
    fn send(&self, request: &Request, attempt: Attempt) -> Exchange {
        let url = format!("{}{}", self.url, request.target.suffix());
        let agent = match request.target {
            Target::Snapshot { .. } => bulk_agent(),
            Target::Stream | Target::Read { .. } | Target::Retention { .. } => agent(),
        };
        let resend = Resend {
            refresh: attempt.refresh_token,
            fresh: attempt.fresh_connection,
        };
        let limits = Timeouts::new(
            u64::try_from(request.body.len()).unwrap_or(u64::MAX),
            request.answer_bytes,
            min_upload_kbps(),
        );
        // A body always goes out from a slice, so ureq sets `Content-Length`, an empty body's
        // included, and never chunks it.
        let sent = match request.method {
            Method::Head => limits
                .apply(with_headers(prepared(agent.head(&url), resend), request))
                .call(),
            Method::Get => limits
                .apply(with_headers(prepared(agent.get(&url), resend), request))
                .call(),
            Method::Put => limits
                .apply(with_headers(prepared(agent.put(&url), resend), request))
                .send(&request.body[..]),
            Method::Post => limits
                .apply(with_headers(prepared(agent.post(&url), resend), request))
                .send(&request.body[..]),
        };
        let mut response = match sent {
            Ok(response) => response,
            Err(e) => return Exchange::NoAnswer(no_answer_from(e)),
        };
        let body = match response
            .body_mut()
            .with_config()
            .limit(ANSWER_BODY_LIMIT)
            .read_to_vec()
        {
            Ok(body) => body,
            Err(e) => {
                return Exchange::NoAnswer(NoAnswer::Lost {
                    reason: format!("the answer's body broke off: {e}"),
                });
            }
        };
        let status = response.status().as_u16();
        let headers: Vec<(&str, &[u8])> = response
            .headers()
            .iter()
            .map(|(name, value)| (name.as_str(), value.as_bytes()))
            .collect();
        Exchange::Answered(Box::new(parse_answer(
            request,
            status,
            headers.iter().copied(),
            body,
        )))
    }
}

/// What an attempt that failed with `e` before its answer means: nothing sent only when `net`
/// proves it, whatever the error's kind (an `InvalidData` or a refused connection on a connection
/// that carried the request is lost, not unsent).
#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "the client side is unwired until the 0.8 driver (PR5) sends through it"
    )
)]
fn no_answer_from(e: ureq::Error) -> NoAnswer {
    let (e, sent_nothing) = net::sent_nothing(e);
    let reason = e.to_string();
    if sent_nothing {
        NoAnswer::NotSent { reason }
    } else {
        NoAnswer::Lost { reason }
    }
}

/// The rate the answer's body time limit assumes, in KiB/s.
#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "the client side is unwired until the 0.8 driver (PR5) sends through it"
    )
)]
const MIN_DOWNLOAD_KBPS: u64 = 256;

/// One attempt's time limits (design §4.12).
#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "the client side is unwired until the 0.8 driver (PR5) sends through it"
    )
)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Timeouts {
    connect: Duration,
    send_request: Duration,
    send_body: Duration,
    recv_response: Duration,
    recv_body: Duration,
}

#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "the client side is unwired until the 0.8 driver (PR5) sends through it"
    )
)]
impl Timeouts {
    /// The limits of an attempt that sends `body` bytes and expects an answer body of up to
    /// `answer` bytes, uploading at `upload_kbps` KiB/s at least.
    fn new(body: u64, answer: u64, upload_kbps: u64) -> Self {
        Self {
            connect: Duration::from_secs(5),
            send_request: Duration::from_secs(5),
            send_body: scaled(Duration::from_secs(5), body, upload_kbps),
            recv_response: Duration::from_secs(15),
            recv_body: scaled(Duration::from_secs(10), answer, MIN_DOWNLOAD_KBPS),
        }
    }

    /// `req` under these limits in place of the agent's overall timeout.
    fn apply<B>(self, req: ureq::RequestBuilder<B>) -> ureq::RequestBuilder<B> {
        req.config()
            .timeout_global(None)
            .timeout_connect(Some(self.connect))
            .timeout_send_request(Some(self.send_request))
            .timeout_send_body(Some(self.send_body))
            .timeout_recv_response(Some(self.recv_response))
            .timeout_recv_body(Some(self.recv_body))
            .build()
    }
}

/// `base` plus the time `bytes` take at `kbps` KiB/s.
#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "the client side is unwired until the 0.8 driver (PR5) sends through it"
    )
)]
fn scaled(base: Duration, bytes: u64, kbps: u64) -> Duration {
    let per_second = kbps.max(1).saturating_mul(1024);
    let millis = bytes
        .saturating_mul(1000)
        .checked_div(per_second)
        .unwrap_or(0);
    base.saturating_add(Duration::from_millis(millis))
}

/// `req` with the request's own headers.
#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "the client side is unwired until the 0.8 driver (PR5) sends through it"
    )
)]
fn with_headers<B>(mut req: ureq::RequestBuilder<B>, request: &Request) -> ureq::RequestBuilder<B> {
    let mut headers = Vec::new();
    request.render_headers(|name, value| headers.push((name, value.to_owned())));
    for (name, value) in headers {
        req = req.header(name, value);
    }
    req
}

/// `User-Agent` of every request (a CONNECT proxy's too): servers and gateways tell versions apart.
pub(crate) const USER_AGENT: &str = concat!("sqlite-ursula-vfs/", env!("CARGO_PKG_VERSION"));

/// Appends, claims, reads and HEADs. It keeps a connection for `idle_age()` (90 s) after its last
/// request: with ureq's 15 s, a writer that commits less often opened a new TCP and TLS connection
/// for almost every commit, which behind a reverse proxy or a CDN sometimes costs about a second
/// more.
///
/// Before it reuses a pooled connection, ureq reads its socket once without blocking (ureq 3.4.2,
/// `Pool::get` and `probe_tcp_stream`, under TLS too). A connection the server or a proxy closed
/// while it sat idle (its FIN or RST arrived meanwhile) is dropped, and the request goes out on
/// another one or a new one. One closed just as the request goes out, and one that a NAT, a load
/// balancer or a firewall dropped without telling either end, still look open. A request sent on
/// it fails, at once when the device answers with a reset and after its whole timeout when nothing
/// answers (10 s), and ureq never sends it again: the retries here do, on a new connection
/// (`Resend::fresh`). Many such devices drop a connection idle for about 4 minutes, so the age
/// stays well below that.
pub(crate) fn agent() -> &'static ureq::Agent {
    static A: OnceLock<ureq::Agent> = OnceLock::new();
    A.get_or_init(|| new_agent(Duration::from_secs(10), idle_age()))
}

/// Snapshot transfers move whole databases: a longer timeout than appends. One snapshot's requests
/// follow each other within seconds, so the agent keeps ureq's 15 s idle age (or `idle_age()`,
/// when shorter): a connection a device dropped would hold a transfer for the whole timeout,
/// longer than the retry budget.
pub(crate) fn bulk_agent() -> &'static ureq::Agent {
    static A: OnceLock<ureq::Agent> = OnceLock::new();
    A.get_or_init(|| {
        new_agent(
            Duration::from_secs(120),
            idle_age().min(Duration::from_secs(15)),
        )
    })
}

/// How long resolving a host, and connecting to it (the TLS handshake included), may take, each:
/// well below either agent's timeout. A host that does not answer fails the request as
/// `Timeout::Resolve` or `Timeout::Connect`, before anything of it was sent (`before_connection`),
/// rather than holding it for the whole timeout (ureq reports the earliest deadline, and a
/// connection's starts when the host resolved).
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// An agent whose requests time out after `timeout` and that keeps a connection for
/// `max_idle_age` after its last request. Resolving and connecting time out after
/// `CONNECT_TIMEOUT` each. The pool keeps ureq's bounds: at most 3 idle connections per host and
/// 10 in all. Hosts resolve through the shared cache of `net::Resolver`, and a failure to resolve
/// or to connect proves the request never went out (`net`). ureq's proxy from the environment
/// applies, as it always did (`ALL_PROXY`, `HTTPS_PROXY`, `HTTP_PROXY`, and `NO_PROXY`): a CONNECT
/// proxy's host resolves through the same cache, and a failure to reach the proxy, or to have it
/// open the tunnel, proves nothing went out either.
pub(crate) fn new_agent(timeout: Duration, max_idle_age: Duration) -> ureq::Agent {
    new_agent_with(timeout, max_idle_age, net::Resolver::shared())
}

/// `new_agent` resolving through `resolver`.
pub(crate) fn new_agent_with<R: ureq::unversioned::resolver::Resolver>(
    timeout: Duration,
    max_idle_age: Duration,
    resolver: net::Resolver<R>,
) -> ureq::Agent {
    let config = ureq::Agent::config_builder()
        .timeout_global(Some(timeout))
        .timeout_resolve(Some(CONNECT_TIMEOUT))
        .timeout_connect(Some(CONNECT_TIMEOUT))
        .http_status_as_error(false)
        // Every redirect comes back to the caller, which sends a 307 again to the same URL and
        // fails on any other (`redirect_retried`). Followed, a redirect would go out without
        // the token (ureq drops it on every redirect, to the same host too), and a 301, 302 or 303
        // would turn an append into a GET.
        .max_redirects(0)
        .max_idle_age(max_idle_age)
        .tls_config(tls())
        .user_agent(USER_AGENT)
        .build();
    ureq::Agent::with_parts(config, net::Connector::new(resolver.clone()), resolver)
}

/// TLS for `https://` stream URLs: the bundled Mozilla roots, or only the certificate authorities
/// in `URSULA_VFS_CA_FILE`. A CA file without a usable certificate trusts nothing (every TLS
/// request then fails), and says why once.
fn tls() -> TlsConfig {
    static T: OnceLock<TlsConfig> = OnceLock::new();
    T.get_or_init(|| {
        let Some(path) = ca_file() else {
            return TlsConfig::builder().build();
        };
        let certs: Vec<Certificate<'static>> = match fs::read(path) {
            Ok(pem) => ureq::tls::parse_pem(&pem)
                .filter_map(|item| match item {
                    Ok(PemItem::Certificate(cert)) => Some(cert),
                    _ => None,
                })
                .collect(),
            Err(e) => {
                log::emit(Level::Error, "ca_file_unreadable", &[
                    ("path", &path),
                    ("error", &e),
                ]);
                Vec::new()
            }
        };
        if certs.is_empty() {
            log::emit(Level::Error, "ca_file_without_certificates", &[(
                "path", &path,
            )]);
        }
        TlsConfig::builder()
            .root_certs(RootCerts::new_with_certs(&certs))
            .build()
    })
    .clone()
}

/// How the next attempt of a request goes out, after the one before it (`send_retrying`,
/// `append`, `put_idempotent`).
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct Resend {
    /// The attempt's token was refused: the next one reads the token again (`authorized`).
    pub(crate) refresh: bool,
    /// The attempt got no answer: the next one takes a new connection (`RETRY_IDLE_AGE`).
    pub(crate) fresh: bool,
}

/// How long a pooled connection may have been idle for an attempt after one that got no answer.
/// ureq checks a request's own idle age when it takes a connection from the pool, and drops one
/// too old for it: the attempt goes out on a new connection (or one used a moment ago), and the
/// stale ones are gone. A NAT or a load balancer that drops idle connections without telling
/// either end leaves every pooled connection to the host dead (up to 3), and ureq takes the oldest
/// first: each would cost its whole timeout.
const RETRY_IDLE_AGE: Duration = Duration::from_secs(1);

/// `req` for an attempt: with the token (`authorized`), on a new connection after an attempt that
/// got no answer (`Resend`).
pub(crate) fn prepared<B>(req: ureq::RequestBuilder<B>, resend: Resend) -> ureq::RequestBuilder<B> {
    let req = authorized(req, resend.refresh);
    if resend.fresh {
        req.config().max_idle_age(RETRY_IDLE_AGE).build()
    } else {
        req
    }
}

/// A request that got no answer for a reason another attempt on a new connection may not meet:
/// the connection was reset, aborted or closed under it (a server closing it mid-answer included,
/// an early EOF, and on macOS a reset reported as `EINVAL`: `reset_on_macos`), the request timed
/// out (a pooled connection closed as the request went out, or dropped by a device without telling
/// either end), or a signal interrupted it. Not a refused connection (nothing listens there, so a
/// server still restarting fails the read), a failed TLS handshake (`InvalidData`), a protocol
/// error (an answer that is not HTTP/1, such as from a gRPC port, is the same every time), nor
/// anything about the request or the setup.
pub(crate) fn no_answer(e: &ureq::Error) -> bool {
    match net::unwrapped(e) {
        ureq::Error::Io(e) => {
            matches!(
                e.kind(),
                std::io::ErrorKind::ConnectionReset
                    | std::io::ErrorKind::ConnectionAborted
                    | std::io::ErrorKind::BrokenPipe
                    | std::io::ErrorKind::UnexpectedEof
                    | std::io::ErrorKind::TimedOut
                    | std::io::ErrorKind::Interrupted
            ) || reset_on_macos(e)
        }
        ureq::Error::Timeout(_) => true,
        _ => false,
    }
}

/// Whether `e` is a reset as macOS reports it when the reset came in between two calls on the
/// socket (#542): `EINVAL` from the OS (`Invalid argument (os error 22)`), where Linux reports
/// `EPIPE` or `ECONNRESET`.
///
/// A reset shuts a socket down both ways, and macOS then fails every `setsockopt` on it with
/// `EINVAL` (XNU's `sosetoptlock`). ureq 3.4.2 sets the socket's write timeout before it writes
/// each buffer of a request (128 KiB) and its read timeout before each read (`TcpTransport`, each
/// to the time left, which differs every time), and `TCP_NODELAY` once connected (`TcpConnector`).
/// So a reset that comes in as a request goes out or as its answer starts (a pooled connection the
/// server closed just before, or a body a gateway stopped reading, #540) can fail the request this
/// way. Under TLS too: rustls hands the socket's error back unchanged.
///
/// No error about the request or the setup reaches here. ureq and ureq-proto make no
/// `InvalidInput` of their own, and neither does rustls on ureq's path (its one is for TLS early
/// data, which ureq never sends). A bad header or URL fails as `Error::Http` or `Error::BadUri`.
/// The `InvalidInput` that std makes up for an argument it refuses (a host and port that do not
/// parse, a NUL in a host, a zero timeout, none of which ureq passes) carries no OS error code, and
/// would stay final. A request that meets any other `EINVAL` from the OS on macOS is sent again
/// until the retry budget runs out, and then fails as before. Other systems keep `EINVAL` final:
/// Linux reports a reset as a reset, and its `connect` fails with `EINVAL` for a wrong URL (an
/// IPv6 link-local address without a zone).
fn reset_on_macos(e: &std::io::Error) -> bool {
    cfg!(target_os = "macos") && e.raw_os_error() == Some(libc::EINVAL)
}

/// A transport failure before any connection carried the request: the host did not resolve, the
/// connection was refused or failed, the TLS handshake failed, or resolving or connecting (TLS
/// included) timed out. Nothing of the request was sent. A reset is none of these, whatever error
/// reports it (`EINVAL` on macOS among them, `reset_on_macos`): it came on an open connection,
/// which may have carried part of the request or the whole of it.
pub(crate) fn before_connection(e: &ureq::Error) -> bool {
    if net::is_not_sent(e) {
        return true;
    }
    match e {
        ureq::Error::HostNotFound
        | ureq::Error::ConnectionFailed
        | ureq::Error::Tls(_)
        | ureq::Error::Rustls(_)
        | ureq::Error::Timeout(ureq::Timeout::Resolve | ureq::Timeout::Connect) => true,
        ureq::Error::Io(e) => matches!(
            e.kind(),
            std::io::ErrorKind::ConnectionRefused
                | std::io::ErrorKind::HostUnreachable
                | std::io::ErrorKind::NetworkUnreachable
                | std::io::ErrorKind::AddrNotAvailable
                | std::io::ErrorKind::InvalidData
        ),
        _ => false,
    }
}

/// A token went out in clear (`authorized`), and the log said so.
static IN_CLEAR_LOGGED: AtomicBool = AtomicBool::new(false);

/// `req` with the bearer token, when there is one; `refresh` reads the token file again. A token
/// sent over plain HTTP to another host crosses the network in clear: logged once per process.
pub(crate) fn authorized<B>(
    req: ureq::RequestBuilder<B>,
    refresh: bool,
) -> ureq::RequestBuilder<B> {
    let Some(token) = auth::token(refresh) else {
        return req;
    };
    if let Some(uri) = req.uri_ref().filter(|uri| in_clear(uri))
        && !IN_CLEAR_LOGGED.swap(true, Ordering::Relaxed)
    {
        log::emit(Level::Warn, "token_in_clear", &[("url", uri)]);
    }
    req.header("authorization", format!("Bearer {token}"))
}

/// A plain `http://` URL whose host is not a loopback address (or `localhost`).
pub(crate) fn in_clear(uri: &Uri) -> bool {
    let loopback = |host: &str| {
        host.eq_ignore_ascii_case("localhost")
            || host
                .trim_start_matches('[')
                .trim_end_matches(']')
                .parse::<IpAddr>()
                .is_ok_and(|ip| ip.is_loopback())
    };
    uri.scheme_str() == Some("http") && !uri.host().is_some_and(loopback)
}

/// The most of a response body an error message or a log line shows.
pub(crate) const BODY_SHOWN: usize = 256;

/// The start of a response's body as text, for an error message or a log line: at most
/// `BODY_SHOWN` bytes, followed by `...` when there is more (a gateway's error page, say), and
/// what could be read when the body breaks off.
pub(crate) fn body_text(r: &mut ureq::http::Response<ureq::Body>) -> String {
    let mut bytes = Vec::new();
    let limit = u64::try_from(BODY_SHOWN.saturating_add(1)).unwrap_or(u64::MAX);
    if let Err(_broken) = r.body_mut().as_reader().take(limit).read_to_end(&mut bytes) {
        // Keep what arrived before the break.
    }
    let more = bytes.len() > BODY_SHOWN;
    bytes.truncate(BODY_SHOWN);
    let mut text = String::from_utf8_lossy(&bytes).into_owned();
    if more {
        text.push_str("...");
    }
    text
}

#[cfg(test)]
mod tests;

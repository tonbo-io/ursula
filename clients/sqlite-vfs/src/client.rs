//! The stream, over blocking HTTP: appends with the idempotent producer, reads, `HEAD`,
//! snapshot and retention requests, and their retries.

use std::fs;
use std::io::Read;
use std::net::IpAddr;
use std::sync::OnceLock;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::time::Instant;

use ureq::http::Uri;
use ureq::tls::Certificate;
use ureq::tls::PemItem;
use ureq::tls::RootCerts;
use ureq::tls::TlsConfig;

use crate::auth;
use crate::config::ca_file;
use crate::config::idle_age;
use crate::config::max_frame_bytes;
use crate::config::retry_budget;
use crate::error::Attempt;
use crate::error::Error;
use crate::error::FrameLimit;
use crate::error::Gone;
use crate::error::WriteRefusal;
use crate::log;
use crate::log::Level;
use crate::net;

/// `Producer-Id` prefix; the stream incarnation follows (`producer_id`).
const PRODUCER: &str = "sqlite-ursula-vfs";

const CONTENT_TYPE: &str = "application/octet-stream";

/// `User-Agent` of every request (a CONNECT proxy's too): servers and gateways tell versions apart.
const USER_AGENT: &str = concat!("sqlite-ursula-vfs/", env!("CARGO_PKG_VERSION"));

/// The protocol's offset for the beginning of a stream, and "none" for an offset that may be absent
/// (a snapshot, retention, the local state): it sorts before every offset the server mints.
pub(crate) const START: &str = "-1";

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
fn agent() -> &'static ureq::Agent {
    static A: OnceLock<ureq::Agent> = OnceLock::new();
    A.get_or_init(|| new_agent(Duration::from_secs(10), idle_age()))
}

/// Snapshot transfers move whole databases: a longer timeout than appends. One snapshot's requests
/// follow each other within seconds, so the agent keeps ureq's 15 s idle age (or `idle_age()`,
/// when shorter): a connection a device dropped would hold a transfer for the whole timeout,
/// longer than the retry budget.
fn bulk_agent() -> &'static ureq::Agent {
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
fn new_agent(timeout: Duration, max_idle_age: Duration) -> ureq::Agent {
    new_agent_with(timeout, max_idle_age, net::Resolver::shared())
}

/// `new_agent` resolving through `resolver`.
fn new_agent_with<R: ureq::unversioned::resolver::Resolver>(
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
struct Resend {
    /// The attempt's token was refused: the next one reads the token again (`authorized`).
    refresh: bool,
    /// The attempt got no answer: the next one takes a new connection (`RETRY_IDLE_AGE`).
    fresh: bool,
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
fn prepared<B>(req: ureq::RequestBuilder<B>, resend: Resend) -> ureq::RequestBuilder<B> {
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
fn no_answer(e: &ureq::Error) -> bool {
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

/// Whether the stream at `url` answers a `HEAD` with a status below 500 that is no redirect, in one
/// attempt, on a new connection or one used within the last second (`Resend::fresh`): the layers
/// in front of the stream pass a request without a body, and the server behind them is up (a
/// gateway whose servers are down answers 502 or 503). A redirect is no answer, as everywhere
/// else in the client. The snapshot thread asks after a large body failed, to tell a body too
/// large for a layer in front of the stream from an outage (`snapshotter::FailureKind`).
pub(crate) fn answers(url: &str) -> bool {
    let resend = Resend {
        fresh: true,
        ..Resend::default()
    };
    prepared(agent().head(url), resend).call().is_ok_and(|r| {
        let status = r.status().as_u16();
        status < 500 && !(300..400).contains(&status)
    })
}

/// A token went out in clear (`authorized`), and the log said so.
static IN_CLEAR_LOGGED: AtomicBool = AtomicBool::new(false);

/// `req` with the bearer token, when there is one; `refresh` reads the token file again. A token
/// sent over plain HTTP to another host crosses the network in clear: logged once per process.
fn authorized<B>(req: ureq::RequestBuilder<B>, refresh: bool) -> ureq::RequestBuilder<B> {
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
fn in_clear(uri: &Uri) -> bool {
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

/// How an answer refuses a request before it reaches the stream, or fences its owner: the one
/// place a fence is told from a refusal in front of the stream.
enum Refused {
    /// A 401: an authorizer in front of the stream refused the token. Sent again, with the token
    /// read again, within the retry budget.
    Token,
    /// A 402: a layer in front of the stream (an authorizer, a quota or billing service) refuses
    /// the request until something is paid or settled. Final for the request, never a fence, and
    /// nothing of it reached the stream (unless an earlier attempt of an append did: `append`).
    PaymentRequired,
    /// A 403 without `Producer-Epoch`: an authorizer denied the request. Final, and never a fence.
    /// Nothing of it reached the stream only when the answer proves it (`applied_nothing`: marked
    /// `x-ursula-not-applied`, with a gateway's vouch): a filtering proxy can answer 403 in place
    /// of the stream's own answer (see `append`).
    Denied,
    /// A 403 with `Producer-Epoch`: the server's fence, which names the epoch that owns the stream
    /// (only an append can meet it).
    Fenced { current: Option<u64> },
}

fn refused(r: &ureq::http::Response<ureq::Body>) -> Option<Refused> {
    match r.status().as_u16() {
        401 => Some(Refused::Token),
        402 => Some(Refused::PaymentRequired),
        403 if r.headers().contains_key("producer-epoch") => Some(Refused::Fenced {
            current: header_u64(r, "producer-epoch"),
        }),
        403 => Some(Refused::Denied),
        _ => None,
    }
}

/// The response header with which a layer (the server, at its ingress admission or before it
/// proposes a write, a gateway, a proxy in front of either) marks its refusal (a 4xx or 5xx) of a
/// request it refused before passing it on: the stream applied nothing of it. The value is a short
/// token naming why (`RaftLogPressure`, say). Older layers never send it. It counts only on a
/// refusal status and with a gateway's vouch (`NOT_APPLIED_CHECKED`): see `applied_nothing`.
const NOT_APPLIED: &str = "x-ursula-not-applied";

/// The response header with which a gateway vouches for `NOT_APPLIED`. Ursula's gateway sets it
/// (to `1`) from 0.7.3 on, on every answer it returns: the answers it forwards, after it applied
/// its own rule to their marker (an answer it got by following a leader redirect keeps the marker
/// only if the redirect carried it too), and its own refusals and errors. It removes any copy that
/// comes from upstream before it adds its own, and a node never sets it. A gateway before 0.7.3
/// relays the marker of an answer it got by following a redirect, which a node can send after it
/// proposed the write, and it never vouches: through it, and without a gateway, a marker proves
/// nothing.
const NOT_APPLIED_CHECKED: &str = "x-ursula-not-applied-checked";

/// How an answer proves that the stream applied nothing of the request it answers (see
/// `applied_nothing`).
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Proof {
    /// Its status: 401, 402 or 429.
    Status,
    /// A refusal status (4xx or 5xx) with `x-ursula-not-applied` and a gateway's vouch: the
    /// marker's token, cut to `BODY_SHOWN` bytes like a body. A marker with an empty value still
    /// marks.
    Marked(String),
}

impl Proof {
    /// The marker's token, empty for a proof by status.
    fn token(self) -> String {
        match self {
            Proof::Marked(token) => token,
            Proof::Status => String::new(),
        }
    }

    /// The refusal that an answer with `status` and `body`, proven this way, stands for: a 401,
    /// 402, 403 or 429 keeps its own kind, marked or not, and a vouched marker names any other
    /// (`NotApplied`, with the marker's token). A kind of its own carries the layer's explanation:
    /// its body, or the marker's token when the body is empty (Ursula's gateway answers its
    /// refusals without a body).
    pub(crate) fn refusal(self, status: u16, body: String) -> WriteRefusal {
        let token = self.token();
        let explained = if body.trim().is_empty() {
            token.clone()
        } else {
            body
        };
        match status {
            401 => WriteRefusal::Unauthorized { body: explained },
            402 => WriteRefusal::PaymentRequired { body: explained },
            403 => WriteRefusal::Forbidden { body: explained },
            429 => WriteRefusal::RateLimited { body: explained },
            _ => WriteRefusal::NotApplied {
                status,
                reason: token,
            },
        }
    }
}

/// Whether an answer proves that the stream applied nothing of the request it answers, and how: the
/// one place that decides it (design doc §2). It does when its status is 401 or 402 (a layer in
/// front of the stream refused the request before passing it on) or 429 (the same, or the server's
/// limit on a stream's producers, which refuses only the first append of a producer the stream does
/// not hold: an attempt sent after an earlier one was applied is answered as its duplicate), or
/// when a refusal status (4xx or 5xx) carries both `x-ursula-not-applied` and the vouch
/// `x-ursula-not-applied-checked`. Any other answer proves nothing: a 403, 413 or 503 with only one
/// of the two headers, and a redirect or a success whatever headers it carries (`None`). It reads
/// the status and the headers only, never the body.
pub(crate) fn applied_nothing<B>(r: &ureq::http::Response<B>) -> Option<Proof> {
    let status = r.status();
    let headers = r.headers();
    if let Some(marker) = headers.get(NOT_APPLIED).filter(|_| {
        headers.contains_key(NOT_APPLIED_CHECKED)
            && (status.is_client_error() || status.is_server_error())
    }) {
        let value = marker.as_bytes();
        let shown = value.get(..BODY_SHOWN).unwrap_or(value);
        return Some(Proof::Marked(
            String::from_utf8_lossy(shown).trim().to_owned(),
        ));
    }
    matches!(status.as_u16(), 401 | 402 | 429).then_some(Proof::Status)
}

/// The refusal a 402 stands for (`Proof::refusal`): its status proves that nothing was applied,
/// and a vouched marker gives the reason when the body is empty.
fn payment_refusal(r: &mut ureq::http::Response<ureq::Body>) -> WriteRefusal {
    let proof = applied_nothing(r).unwrap_or(Proof::Status);
    proof.refusal(402, body_text(r))
}

/// The error of request `op` to `url` that a layer in front of the stream refused (a refused
/// token, a denial, or a 402), never a fence; `None` for any other answer.
fn refusal(op: &'static str, url: &str, r: &mut ureq::http::Response<ureq::Body>) -> Option<Error> {
    let status = r.status().as_u16();
    match refused(r)? {
        Refused::Token | Refused::Denied => Some(unauthorized(op, url, status, body_text(r))),
        Refused::PaymentRequired => Some(refused_request(op, url, payment_refusal(r))),
        Refused::Fenced { .. } => None,
    }
}

fn unauthorized(op: &'static str, url: &str, status: u16, body: String) -> Error {
    Error::Unauthorized {
        op,
        url: url.to_owned(),
        status,
        body,
    }
}

fn refused_request(op: &'static str, url: &str, refusal: WriteRefusal) -> Error {
    Error::Refused {
        op,
        url: url.to_owned(),
        refusal,
    }
}

/// A redirect the extension retries: a 307, which keeps the method and body and says that the
/// request belongs elsewhere for now (a gateway passing on the server's redirect to the leader of
/// the stream's group, which moved). Never followed (`new_agent`): the request is sent again to the
/// same URL, like a 503, so it reaches the leader through the same gateway, with the token. An
/// append so answered has an unknown outcome: a leader that loses its leadership answers so, and
/// what it proposed may still be committed. A 308 is not retried: neither the server nor the
/// gateway sends one, and a proxy that redirects `http://` to `https://` does (ingress-nginx and
/// Caddy by default), so it fails the request at once with its Location (`redirected`).
fn redirect_retried(status: u16) -> bool {
    status == 307
}

/// The error of request `op` to `url` answered with a redirect the extension does not retry (any
/// 3xx but 307: a 301, 302 or 303 would turn an append into a GET, and a 308 comes from a proxy in
/// front of the stream); `None` for any other answer.
fn redirected(op: &'static str, url: &str, r: &ureq::http::Response<ureq::Body>) -> Option<Error> {
    let status = r.status().as_u16();
    ((300..400).contains(&status) && !redirect_retried(status)).then(|| Error::Redirected {
        op,
        url: url.to_owned(),
        status,
        location: r
            .headers()
            .get("location")
            .and_then(|v| v.to_str().ok())
            .map(str::to_owned),
    })
}

fn header_u64(r: &ureq::http::Response<ureq::Body>, name: &str) -> Option<u64> {
    r.headers().get(name)?.to_str().ok()?.trim().parse().ok()
}

/// An offset as the server wrote it: opaque, kept verbatim and compared only lexicographically
/// (never parsed or computed with). `None` unless it is usable in a URL path or query and a
/// sidecar as it is: 1 to 64 unreserved URL characters.
pub(crate) fn offset_token(v: &str) -> Option<String> {
    let ok = |b: u8| b.is_ascii_alphanumeric() || b"-._~".contains(&b);
    (!v.is_empty() && v.len() <= 64 && v.bytes().all(ok)).then(|| v.to_owned())
}

pub(crate) fn header_offset(r: &ureq::http::Response<ureq::Body>, name: &str) -> Option<String> {
    offset_token(r.headers().get(name)?.to_str().ok()?.trim())
}

/// The wait before the next retry: no sooner than `retry_after`, and at least `backoff`. `None`
/// when the wait would end past `deadline` (or overflow, for an absurd Retry-After): no retry.
fn next_wait(
    retry_after: Option<Duration>,
    backoff: Duration,
    deadline: Instant,
) -> Option<Duration> {
    let wait = retry_after.map_or(backoff, |after| after.max(backoff));
    Instant::now()
        .checked_add(wait)
        .is_some_and(|end| end <= deadline)
        .then_some(wait)
}

/// Sleeps `wait` (from `next_wait`), then doubles `backoff` (up to 1 s).
fn back_off(wait: Duration, backoff: &mut Duration) {
    std::thread::sleep(wait);
    *backoff = backoff.saturating_mul(2).min(Duration::from_secs(1));
}

/// Waits before the next retry (`next_wait`, then `back_off`). Returns false, without waiting,
/// when there is no retry.
fn pause(retry_after: Option<Duration>, backoff: &mut Duration, deadline: Instant) -> bool {
    let Some(wait) = next_wait(retry_after, *backoff, deadline) else {
        return false;
    };
    back_off(wait, backoff);
    true
}

/// When the retries of a request end: `retry_budget()` from now (now, for a budget past what
/// `Instant` can hold).
fn retry_deadline() -> Instant {
    let now = Instant::now();
    now.checked_add(retry_budget()).unwrap_or(now)
}

/// Sends a request that is safe to repeat (a read, or a create), retrying 429 (rate limiting), 503
/// (overload, or a `consistency=leader` read, HEAD or snapshot read the leader could not confirm
/// with a quorum in time), a 307 (`redirect_retried`: sent again to the same URL, never followed)
/// and a refused token (`Refused::Token`: sent again with the token read again) the way
/// `append` does: no sooner than Retry-After (seconds), with backoff, within `retry_budget()`.
/// A request that got no answer (`no_answer`) is sent again too, on a new connection, as after a
/// 503 without Retry-After. Returns the first other answer or error, or the last retried one once
/// the budget is spent or `stopped` (a re-attach is waiting for the snapshot thread). `send` builds
/// the request for an attempt (`prepared`).
fn send_retrying(
    send: impl Fn(Resend) -> Result<ureq::http::Response<ureq::Body>, ureq::Error>,
    stopped: &dyn Fn() -> bool,
) -> Result<ureq::http::Response<ureq::Body>, ureq::Error> {
    let deadline = retry_deadline();
    let mut backoff = Duration::from_millis(20);
    let mut resend = Resend::default();
    loop {
        let r = match send(std::mem::take(&mut resend)) {
            Ok(r) => r,
            Err(e) if no_answer(&e) => {
                if stopped() || !pause(None, &mut backoff, deadline) {
                    return Err(e);
                }
                resend.fresh = true;
                continue;
            }
            Err(e) => return Err(e),
        };
        resend.refresh = matches!(refused(&r), Some(Refused::Token));
        let status = r.status().as_u16();
        if !resend.refresh && !matches!(status, 429 | 503) && !redirect_retried(status) {
            return Ok(r);
        }
        let retry_after = header_u64(&r, "retry-after").map(Duration::from_secs);
        if stopped() || !pause(retry_after, &mut backoff, deadline) {
            return Ok(r);
        }
    }
}

pub(crate) enum Append {
    /// Applied, or answered as a duplicate of an earlier attempt of this append (or, for a claim,
    /// of any append at its epoch and sequence 0, verified separately); the stream offset after it
    /// when known. `duplicate`: the answer was a duplicate's (204), which a commit gets only after
    /// an attempt that may have reached the stream (see `append`).
    Acked {
        next: Option<String>,
        attempts: u32,
        duplicate: bool,
    },
    /// A commit's attempt answered 204, the server's answer to a duplicate, while no earlier
    /// attempt of it can have reached the stream (see `append`): the stream held an append at this
    /// epoch and sequence before that attempt reached it, which no attempt of this append put
    /// there as far as their answers tell. This owner alone writes its producer at its epoch, so
    /// that append is its own: an earlier commit at this sequence that was refused as never
    /// applied ([`Append::Refused`]) but was applied after all, or this commit, applied by an
    /// exchange whose answer said otherwise (an earlier attempt refused falsely, or an exchange
    /// that a layer in front of the stream made and answered for itself, such as a gateway that
    /// followed a leader redirect the leader sent after it proposed the commit). `next` is that
    /// append's receipt. `Db::append_commit` tells them apart by whether a commit at this sequence
    /// was refused.
    UnexplainedDuplicate { next: Option<String>, attempts: u32 },
    /// 403 with `Producer-Epoch`: a newer epoch owns the stream.
    Fenced { current: Option<u64> },
    /// 412: the stream is no longer the incarnation the append was sent to (deleted and
    /// recreated); nothing was appended.
    Recreated,
    /// 409 expecting sequence 0: the server does not know this producer: it expired it after 7
    /// idle days (a recreated stream answers 412 first).
    ProducerExpired,
    /// 409 refusing the commit's `Stream-Seq` (not above the stream's last one): another writer
    /// appended with a higher one (see `stream_seq`). `body` is the server's explanation.
    SeqConflict { body: String },
    /// Refused before it reached the stream, and so was every earlier attempt of it (see
    /// `append`): nothing was appended, and the producer sequence is still free.
    Refused(WriteRefusal),
    /// Refused by the extension itself and never sent: the frame is over the frame limit
    /// (`max_frame_bytes`). No exchange took place, so unlike [`Append::Refused`] it says nothing
    /// about the layers in front of the stream, and no exchange can have applied it.
    Unsent(WriteRefusal),
    /// A definite rejection, or no answer within the retry budget.
    Failed(Error),
}

/// The `Producer-Id` of every owner of one stream incarnation: owners of the same incarnation fence
/// each other by epoch (a recreated stream refuses an owner of the deleted one with 412, see
/// `append`).
pub(crate) fn producer_id(incarnation: &str) -> String {
    format!("{PRODUCER}/{incarnation}")
}

/// One idempotent append by `producer` to stream incarnation `incarnation` (sent as the
/// `Stream-Incarnation` precondition): retried with the same producer sequence until the outcome
/// is known.
///
/// A duplicate answer is taken as proof of *our* earlier append only for commits (seq >= 1), and
/// only with its receipt (`Stream-Next-Offset`, checked by `Db::append_commit`): they are sent
/// after a verified claim (see `claim_once`), which makes this owner the only writer of its
/// incarnation's producer at its epoch, so whatever holds (epoch, seq) there is ours: an earlier
/// attempt of this append, or an earlier commit at this sequence that was refused as never applied
/// (a refusal leaves the sequence to the next commit) but was applied after all. So a duplicate's
/// answer (204) while no earlier attempt of this append can have reached the stream ends the
/// append as [`Append::UnexplainedDuplicate`], which `Db::append_commit` takes for such a refused
/// commit when one was refused at this sequence. After an attempt that may have reached the
/// stream, a duplicate's answer is taken for that attempt's ([`Append::Acked`], `duplicate`), so a
/// refused commit applied after all goes unchecked when an attempt of the next commit got no
/// answer, or an answer that proves nothing (a 307, a 5xx without a vouched marker), before the
/// duplicate's (design doc §2): `Db::append_commit` only logs it. A claim's answer is verified
/// separately. Commits also carry their `Stream-Seq` (see `stream_seq`).
///
/// An attempt whose answer proves that it applied nothing (`applied_nothing`: a 401, 402 or 429, or
/// a refusal marked `x-ursula-not-applied` that a gateway vouched for with
/// `x-ursula-not-applied-checked`) is taken to have applied nothing, which every layer in front of
/// the stream must ensure (design doc §2): one that passed the append on and then answered so would
/// have the next commit, sent with the same sequence, answered as this one's duplicate. That poisons
/// the next commit (see above) instead of acknowledging it without its pages, unless one of its
/// attempts got no answer, or an answer that proves nothing, before the duplicate's. A marker
/// without the vouch proves nothing: a gateway before 0.7.3 relays the marker of an answer it got
/// by following a leader redirect, which a node can send after it proposed the append. A 403
/// without `Producer-Epoch` counts only when proven: a filtering proxy can answer 403 in place of
/// the stream's own answer, after the stream applied the append, so any other fails the append, as
/// before 0.7.3. While no attempt of the append can have reached the stream, a 402 or a proven
/// final answer (any other 4xx, an authorizer's 403 among them) ends it as [`Append::Refused`] at
/// once, and the retried ones (401, 429, a proven 5xx) end it so once the budget runs out. After
/// an attempt whose outcome is unknown (no answer, a 307, or a 5xx that proves nothing), the append
/// may be in the stream, and the next commit, sent with the same sequence, would be answered as its
/// duplicate. Its refusals then prove nothing: a 402 is retried like an unknown outcome, until an
/// answer settles it or the budget runs out ([`Error::AppendUnknown`]), and a 403 fails it.
///
/// A 307 is an unknown outcome, sent again to the same URL with the same sequence
/// (`redirect_retried`); any other redirect (a 308 among them) fails the append
/// ([`Error::Redirected`]).
///
/// An attempt that failed before any byte of it went out (`net::NotSent`: its host did not
/// resolve, or no connection to it was made, the TLS handshake included) applied nothing, as a
/// proven refusal did. It is retried like an unknown outcome, but leaves the append's outcome as it
/// was: an append whose every attempt failed so, or was refused, ends as [`Append::Refused`] once
/// the budget runs out, with the last attempt's refusal, or [`WriteRefusal::Unreachable`] when the
/// last attempt failed before it went out. So a server that cannot be reached fails the commit
/// alone instead of poisoning the database. A failure on a connection that was made, a pooled one
/// included, is an unknown outcome.
///
/// A frame over `max_frame_bytes()` is never sent: it ends as [`Append::Unsent`] at once, with
/// [`WriteRefusal::TooLarge`]. A 413 that proves it applied nothing (`applied_nothing`: marked and
/// vouched for) ends as [`Append::Refused`] with the same kind, while no attempt can have reached
/// the stream (any other is a rejection, like any other 4xx that proves nothing). The same frame
/// can never pass, so neither is retried.
pub(crate) fn append(
    url: &str,
    incarnation: &str,
    producer: &str,
    body: &[u8],
    epoch: u64,
    seq: u64,
) -> Append {
    let retrying = Retrying {
        agent: agent(),
        deadline: retry_deadline(),
    };
    append_on(retrying, url, incarnation, producer, body, epoch, seq)
}

/// What `append_on` sends its attempts with: the agent, and when the retry budget runs out.
struct Retrying<'a> {
    agent: &'a ureq::Agent,
    deadline: Instant,
}

/// `append`'s attempts, on `retrying.agent` until `retrying.deadline`.
fn append_on(
    retrying: Retrying<'_>,
    url: &str,
    incarnation: &str,
    producer: &str,
    body: &[u8],
    epoch: u64,
    seq: u64,
) -> Append {
    // A frame the server cannot take is never sent, so nothing of it reaches the stream and the
    // commit fails alone (design doc §7). Sent, it would be refused with a 413 that may be
    // unmarked, or with the connection closed before the body was read, which is no answer.
    let limit = max_frame_bytes();
    if body.len() > limit {
        return Append::Unsent(WriteRefusal::frame_over_limit(body.len(), limit));
    }
    let started = Instant::now();
    let Retrying { agent, deadline } = retrying;
    let mut backoff = Duration::from_millis(20);
    let mut attempts: u32 = 0;
    // How the next attempt goes out, after a refused token or no answer.
    let mut resend = Resend::default();
    // An earlier attempt may have reached the stream: its outcome is unknown.
    let mut reached = false;
    loop {
        attempts = attempts.saturating_add(1);
        let mut req = prepared(agent.post(url), std::mem::take(&mut resend))
            .header("content-type", CONTENT_TYPE)
            .header("stream-incarnation", incarnation)
            .header("producer-id", producer)
            .header("producer-epoch", epoch.to_string())
            .header("producer-seq", seq.to_string());
        if seq > 0 {
            req = req.header("stream-seq", stream_seq((epoch, seq)));
        }
        let sent = req.send(body);
        let mut retry_after = None;
        // The attempt retried (for the log and the error), and why it applied nothing: `None`
        // when it may have reached the stream.
        let (unknown, refusal) = match sent {
            Ok(mut r) => {
                let status = r.status().as_u16();
                // Rate limiting (429, e.g. ursulagw), overload (503) and a redirect for the leader
                // (307) are transient: retried with the same producer sequence, no sooner than
                // Retry-After (seconds).
                retry_after = header_u64(&r, "retry-after").map(Duration::from_secs);
                // Whether the attempt applied nothing: every branch below takes it from here.
                let proof = applied_nothing(&r);
                match refused(&r) {
                    Some(Refused::Fenced { current }) => return Append::Fenced { current },
                    // Unproven, the denial may have replaced the stream's answer (see above).
                    Some(Refused::Denied) => {
                        return match proof {
                            Some(proof) if !reached => {
                                Append::Refused(proof.refusal(status, body_text(&mut r)))
                            }
                            _ => Append::Failed(unauthorized(
                                "append",
                                url,
                                status,
                                body_text(&mut r),
                            )),
                        };
                    }
                    Some(Refused::PaymentRequired) => {
                        let body = body_text(&mut r);
                        match proof.map(|proof| proof.refusal(status, body.clone())) {
                            Some(refusal) if !reached => return Append::Refused(refusal),
                            // An earlier attempt may be in the stream: the outcome is still
                            // unknown.
                            refusal => (Attempt::Status { status, body }, refusal),
                        }
                    }
                    Some(Refused::Token) => {
                        resend.refresh = true;
                        let body = body_text(&mut r);
                        let refusal = proof.map(|proof| proof.refusal(status, body.clone()));
                        (Attempt::Status { status, body }, refusal)
                    }
                    None => match status {
                        // A duplicate's answer that no attempt of this commit explains (see
                        // above): the server answers a fresh producer append 200.
                        204 if seq > 0 && !reached => {
                            return Append::UnexplainedDuplicate {
                                next: header_offset(&r, "stream-next-offset"),
                                attempts,
                            };
                        }
                        200..=299 => {
                            return Append::Acked {
                                next: header_offset(&r, "stream-next-offset"),
                                attempts,
                                duplicate: status == 204,
                            };
                        }
                        // Never followed (`new_agent`): a 307 is sent again, its outcome unknown
                        // (`redirect_retried`: a redirect proves nothing, whatever headers it
                        // carries), and any other redirect fails the append.
                        300..=399 => match redirected("append", url, &r) {
                            Some(e) => return Append::Failed(e),
                            None => {
                                let body = body_text(&mut r);
                                let refusal =
                                    proof.map(|proof| proof.refusal(status, body.clone()));
                                (Attempt::Status { status, body }, refusal)
                            }
                        },
                        // The stream was deleted and recreated: nothing was appended.
                        412 => return Append::Recreated,
                        409 if seq > 0 && header_u64(&r, "producer-expected-seq") == Some(0) => {
                            return Append::ProducerExpired;
                        }
                        // Neither a producer sequence conflict nor a closed stream: the
                        // `Stream-Seq`.
                        409 if seq > 0
                            && !r.headers().contains_key("producer-expected-seq")
                            && !r.headers().contains_key("stream-closed") =>
                        {
                            return Append::SeqConflict {
                                body: body_text(&mut r),
                            };
                        }
                        // Proven (only a vouched marker proves a 413): refused for its size before
                        // anything handled it, by a limit in front that is lower than the
                        // extension's own. Final, like any proven 4xx, but named for what the
                        // application can do about it: a smaller commit.
                        413 if !reached && proof.is_some() => {
                            return Append::Refused(WriteRefusal::TooLarge {
                                bytes: body.len(),
                                over: FrameLimit::InFront {
                                    token: proof.map(Proof::token).unwrap_or_default(),
                                },
                            });
                        }
                        // A definite rejection (429 is retried, below), final: the append fails
                        // alone when the answer proves it applied nothing and no earlier attempt
                        // can have reached the stream.
                        400..=499 if status != 429 => {
                            let body = body_text(&mut r);
                            return match proof {
                                Some(proof) if !reached => {
                                    Append::Refused(proof.refusal(status, body))
                                }
                                _ => Append::Failed(Error::AppendRejected { status, body }),
                            };
                        }
                        _ => {
                            let body = body_text(&mut r);
                            let refusal = proof.map(|proof| proof.refusal(status, body.clone()));
                            (Attempt::Status { status, body }, refusal)
                        }
                    },
                }
            }
            Err(e) => {
                resend.fresh = true;
                // One that failed before any byte of it went out (`net`) applied nothing.
                let (e, sent_nothing) = net::sent_nothing(e);
                let refusal = sent_nothing.then(|| WriteRefusal::Unreachable {
                    reason: e.to_string(),
                });
                (Attempt::Transport(Box::new(e)), refusal)
            }
        };
        // An answer that does not prove the attempt applied nothing (`refusal` is set exactly
        // when `applied_nothing` proved it: never for a 307), or none, may come after the stream
        // applied the append (see above).
        reached |= refusal.is_none();
        // A retried commit is a stall the application sees: say why, attempt by attempt, and
        // whether another attempt follows.
        let wait = next_wait(retry_after, backoff, deadline);
        let event = if wait.is_some() {
            "append_retry"
        } else {
            "append_gave_up"
        };
        log::emit(Level::Warn, event, &[
            ("stream", &url),
            ("attempt", &attempts),
            ("elapsed_ms", &started.elapsed().as_millis()),
            ("reason", &unknown),
        ]);
        let Some(wait) = wait else {
            return match refusal {
                // Every attempt was refused before it reached the stream: nothing was appended.
                Some(refusal) if !reached => Append::Refused(refusal),
                _ if resend.refresh => {
                    Append::Failed(unauthorized("append", url, 401, String::new()))
                }
                _ => Append::Failed(Error::AppendUnknown {
                    attempts,
                    last: unknown,
                }),
            };
        };
        back_off(wait, &mut backoff);
    }
}

/// Creates the stream at `url` (idempotent: an existing stream of the same content type answers
/// 200).
pub(crate) fn create_stream(url: &str) -> Result<(), Error> {
    let mut r = send_retrying(
        |resend| {
            prepared(agent().put(url), resend)
                .header("content-type", CONTENT_TYPE)
                .send_empty()
        },
        &|| false,
    )
    .map_err(|e| http_error("create", url, e))?;
    if let Some(e) = refusal("create", url, &mut r).or_else(|| redirected("create", url, &r)) {
        return Err(e);
    }
    let status = r.status().as_u16();
    let body = body_text(&mut r);
    if (200..300).contains(&status) {
        Ok(())
    } else {
        Err(Error::Status {
            op: "create",
            url: url.to_owned(),
            status,
            body,
        })
    }
}

/// A request that got no answer.
fn http_error(op: &'static str, url: &str, e: ureq::Error) -> Error {
    let (e, _sent_nothing) = net::sent_nothing(e);
    Error::Http {
        op,
        url: url.to_owned(),
        source: Box::new(e),
    }
}

/// The most of a response body an error message or a log line shows.
const BODY_SHOWN: usize = 256;

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

/// One read from `offset` of stream incarnation `incarnation` (a 412 when it is not, see
/// `recreated`): the bytes and the server's offset after them (empty at the tail). Reads
/// the leader's applied state: a follower may lag behind an acknowledged append (a claim, a
/// commit). A 200 without a usable `Stream-Next-Offset` is an error: offsets are never computed.
pub(crate) fn read_from(
    url: &str,
    incarnation: &str,
    offset: &str,
) -> Result<(Vec<u8>, String), Error> {
    let request = format!("{url}?offset={offset}&consistency=leader");
    let mut r = send_retrying(
        |resend| {
            prepared(agent().get(request.as_str()), resend)
                .header("stream-incarnation", incarnation)
                .call()
        },
        &|| false,
    )
    .map_err(|e| http_error("read", &request, e))?;
    if let Some(e) = refusal("read", &request, &mut r).or_else(|| redirected("read", &request, &r))
    {
        return Err(e);
    }
    let status = r.status().as_u16();
    if status == 412 {
        return Err(recreated(url, incarnation));
    }
    let next = header_offset(&r, "stream-next-offset");
    if status == 204 {
        return Ok((Vec::new(), next.unwrap_or_else(|| offset.to_owned())));
    }
    let body = r
        .body_mut()
        .with_config()
        .limit(1 << 30)
        .read_to_vec()
        .map_err(|e| http_error("read the body of", &request, e))?;
    if status == 410 {
        return Err(Error::Gone(Gone::BelowRetention {
            url: url.to_owned(),
            offset: offset.to_owned(),
        }));
    }
    if status == 416 {
        // The server answers 416, not 412, for a stream recreated shorter than `offset` (RFC 9110
        // §13.2.1): only a HEAD tells the two apart, and a recreate rebuilds.
        if head(url, &|| false).is_ok_and(|h| h.incarnation.as_deref() != Some(incarnation)) {
            return Err(recreated(url, incarnation));
        }
        return Err(Error::LostData {
            url: url.to_owned(),
            offset: offset.to_owned(),
        });
    }
    if status != 200 {
        return Err(Error::Status {
            op: "read",
            url: request,
            status,
            body: String::from_utf8_lossy(&body).into_owned(),
        });
    }
    let Some(next) = next else {
        return Err(Error::NoNextOffset {
            op: "read",
            url: request,
        });
    };
    Ok((body, next))
}

/// A read loop's step: `next`, answered by a read at `at` that returned bytes, must sort past `at`.
/// Checked in the loops only, where both are server offsets (or `START`): an older sidecar's
/// unpadded offset, read once to probe for a 416, compares meaninglessly.
pub(crate) fn advanced(url: &str, at: &str, len: usize, next: &str) -> Result<(), Error> {
    if next <= at {
        return Err(Error::OffsetNotAdvanced {
            url: url.to_owned(),
            at: at.to_owned(),
            len,
            next: next.to_owned(),
        });
    }
    Ok(())
}

pub(crate) struct Head {
    /// `START` when absent.
    pub(crate) retained: String,
    /// `None` when absent or `-1` (no snapshot).
    pub(crate) snapshot: Option<String>,
    /// `Stream-Incarnation`: opaque, changes when the stream is deleted and recreated; compared
    /// for equality only. `None` when absent (or unusable in a sidecar or `Producer-Id`): attach
    /// refuses.
    pub(crate) incarnation: Option<String>,
}

/// `e`, a `HEAD`'s refusal of `url`, with the explanation a `HEAD`'s answer cannot carry (it has
/// no body): the body of one `GET` of the stream from its beginning, when that `GET` is refused
/// with a 4xx too, such as the server's `400` that names the bucket or stream id it refuses. A
/// `404` gets none: attach creates the stream then, or names the missing stream itself. Any other
/// outcome of the `GET` leaves `e` as it was.
fn with_reason(e: Error, url: &str) -> Error {
    let missing = |status: u16| (400..500).contains(&status) && status != 404;
    match e {
        Error::Status {
            op,
            url: u,
            status,
            body,
        } if body.is_empty() && missing(status) => Error::Status {
            op,
            url: u,
            status,
            body: reason_of(url),
        },
        Error::Unauthorized {
            op,
            url: u,
            status,
            body,
        } if body.is_empty() => Error::Unauthorized {
            op,
            url: u,
            status,
            body: reason_of(url),
        },
        e => e,
    }
}

/// The body of a `GET` of `url` from the stream's beginning, sent once, when it is refused with a
/// 4xx (cut short: `body_text`); empty otherwise.
fn reason_of(url: &str) -> String {
    let request = format!("{url}?offset={START}");
    match prepared(agent().get(request.as_str()), Resend::default()).call() {
        Ok(mut r) if (400..500).contains(&r.status().as_u16()) => {
            body_text(&mut r).trim().to_owned()
        }
        _ => String::new(),
    }
}

/// `stopped` ends the retries early (`send_retrying` lists the answers it retries).
pub(crate) fn head(url: &str, stopped: &dyn Fn() -> bool) -> Result<Head, Error> {
    let mut r = send_retrying(|resend| prepared(agent().head(url), resend).call(), stopped)
        .map_err(|e| http_error("head", url, e))?;
    if let Some(e) = refusal("head", url, &mut r).or_else(|| redirected("head", url, &r)) {
        return Err(with_reason(e, url));
    }
    let status = r.status().as_u16();
    if status != 200 {
        return Err(with_reason(
            Error::Status {
                op: "head",
                url: url.to_owned(),
                status,
                body: String::new(),
            },
            url,
        ));
    }
    Ok(Head {
        retained: header_offset(&r, "stream-retained-offset").unwrap_or_else(|| START.into()),
        snapshot: header_offset(&r, "stream-snapshot-offset").filter(|s| s != START),
        incarnation: r
            .headers()
            .get("stream-incarnation")
            .and_then(|v| v.to_str().ok())
            .map(str::trim)
            .filter(|v| !v.is_empty() && !v.contains(char::is_whitespace))
            .map(str::to_owned),
    })
}

/// The snapshot at `offset`; `None` when it does not exist (superseded, or not yet visible here).
/// `stopped` ends the retries early (`send_retrying` lists the answers it retries).
pub(crate) fn get_snapshot(
    url: &str,
    incarnation: &str,
    offset: &str,
    stopped: &dyn Fn() -> bool,
) -> Result<Option<Vec<u8>>, Error> {
    let request = format!("{url}/snapshot/{offset}");
    let mut r = send_retrying(
        |resend| {
            prepared(bulk_agent().get(request.as_str()), resend)
                .header("stream-incarnation", incarnation)
                .call()
        },
        stopped,
    )
    .map_err(|e| http_error("get snapshot", &request, e))?;
    if let Some(e) = refusal("get snapshot", &request, &mut r)
        .or_else(|| redirected("get snapshot", &request, &r))
    {
        return Err(e);
    }
    let status = r.status().as_u16();
    if status == 412 {
        return Err(recreated(url, incarnation));
    }
    // A body cut short: the snapshot was superseded and its cold object deleted (after its grace)
    // while it was streaming, or the connection dropped. Either way, start again from `HEAD`.
    let Ok(body) = r.body_mut().with_config().limit(2 << 30).read_to_vec() else {
        return Ok(None);
    };
    match status {
        200 => Ok(Some(body)),
        404 | 410 => Ok(None),
        _ => Err(Error::Status {
            op: "get snapshot",
            url: request,
            status,
            body: String::from_utf8_lossy(&body).into_owned(),
        }),
    }
}

/// How many times `put_idempotent` sends a body larger than [`RESENT_UP_TO`] (a snapshot's) whose
/// outcome stays unknown (no answer, a 5xx or a 307): once, and once again. Such a body can be hundreds
/// of MiB, and one that a layer in front of the stream cuts off for its size (a gateway stops
/// reading at its limit and closes the connection, which the client sees as a failed write), or
/// that a proxy times out, fails the same way every time. So the snapshot thread backs off before
/// its next attempt instead (`snapshotter::SnapshotFailing`).
const UNKNOWN_SENDS: u32 = 2;

/// The largest body `put_idempotent` sends again until the retry budget runs out, as before 0.7.3.
pub(crate) const RESENT_UP_TO: usize = 1 << 20;

/// `PUT` to stream incarnation `incarnation` (a 412 when it is not) with retries while the outcome
/// is unknown (transport errors, 5xx, a 307: `redirect_retried`), no sooner than Retry-After
/// (seconds): both publishing a snapshot and advancing retention are idempotent, so a 402 fails
/// the request alone ([`Error::Refused`]), whatever an earlier attempt did, and so does any other
/// redirect ([`Error::Redirected`]). No other answer refuses it, whatever headers it carries: no
/// answer here decides a producer sequence, so none needs `applied_nothing`, and a marked 413 for
/// a snapshot body over a gateway's limit stays a snapshot failure, which the snapshot thread
/// counts. A body larger than [`RESENT_UP_TO`] is sent [`UNKNOWN_SENDS`] times at most while its
/// outcome stays unknown (a refused token, sent again with the token read again, does not count),
/// a smaller one (a small snapshot, a retention move) until the retry budget runs out. The count
/// is checked before the pause, so the last send of a large body ends the call without waiting
/// for its Retry-After, which the error keeps when a 5xx put the request off (`Attempt::Deferred`,
/// `Error::retry_after`). Returns the status and response. Gives up once `stopped` (a re-attach is
/// waiting for the snapshot thread).
pub(crate) fn put_idempotent(
    url: &str,
    incarnation: &str,
    body: &[u8],
    stopped: &dyn Fn() -> bool,
) -> Result<(u16, ureq::http::Response<ureq::Body>), Error> {
    let deadline = retry_deadline();
    let mut backoff = Duration::from_millis(50);
    // How the next attempt goes out, after a refused token or no answer.
    let mut resend = Resend::default();
    // Sends whose outcome is unknown, for a body larger than `RESENT_UP_TO`.
    let mut unknown_sends: u32 = 0;
    loop {
        let mut retry_after = None;
        let unknown = match prepared(bulk_agent().put(url), std::mem::take(&mut resend))
            .header("content-type", CONTENT_TYPE)
            .header("stream-incarnation", incarnation)
            .send(body)
        {
            Ok(mut r) => {
                retry_after = header_u64(&r, "retry-after").map(Duration::from_secs);
                match (refused(&r), r.status().as_u16()) {
                    (Some(Refused::Token), status) => {
                        resend.refresh = true;
                        Attempt::Status {
                            status,
                            body: body_text(&mut r),
                        }
                    }
                    (Some(Refused::Denied), status) => {
                        return Err(unauthorized("put", url, status, body_text(&mut r)));
                    }
                    (Some(Refused::PaymentRequired), _) => {
                        return Err(refused_request("put", url, payment_refusal(&mut r)));
                    }
                    (_, status) if status < 500 && !redirect_retried(status) => {
                        return match redirected("put", url, &r) {
                            Some(e) => Err(e),
                            None => Ok((status, r)),
                        };
                    }
                    // A 5xx that asks for a later attempt or says it applied nothing: put off,
                    // not about the body's size (`Attempt::Deferred`).
                    (_, status)
                        if status >= 500
                            && (retry_after.is_some() || r.headers().contains_key(NOT_APPLIED)) =>
                    {
                        Attempt::Deferred {
                            status,
                            body: body_text(&mut r),
                            retry_after,
                        }
                    }
                    (_, status) => Attempt::Status {
                        status,
                        body: body_text(&mut r),
                    },
                }
            }
            Err(e) => {
                resend.fresh = true;
                Attempt::Transport(Box::new(net::sent_nothing(e).0))
            }
        };
        if !resend.refresh {
            unknown_sends = unknown_sends.saturating_add(1);
        }
        if (body.len() > RESENT_UP_TO && unknown_sends >= UNKNOWN_SENDS)
            || stopped()
            || !pause(retry_after, &mut backoff, deadline)
        {
            return Err(if resend.refresh {
                unauthorized("put", url, 401, String::new())
            } else {
                Error::PutUnknown {
                    url: url.to_owned(),
                    last: unknown,
                }
            });
        }
    }
}

/// The error for a 412: `url` is no longer stream incarnation `incarnation`.
pub(crate) fn recreated(url: &str, incarnation: &str) -> Error {
    Error::Recreated {
        url: url.to_owned(),
        incarnation: incarnation.to_owned(),
    }
}

/// A commit's `Stream-Seq`: the owner's (epoch, producer sequence), each zero-padded to 20 digits,
/// so it sorts like the pair. Owners of one incarnation claim ever higher epochs and number commits
/// upwards within one, so every commit's is above every earlier commit's: the server's check
/// (refused unless above the stream's last `Stream-Seq`) then fences a writer outside this protocol
/// that appended with a higher one since this owner's last commit (one with a lower or equal
/// `Stream-Seq` is itself refused). Writers without `Stream-Seq` go unnoticed: offsets are opaque,
/// so the VFS does not check where its frame landed.
pub(crate) fn stream_seq((epoch, seq): (u64, u64)) -> String {
    format!("{epoch:020}{seq:020}")
}

/// A stand-in for the stream in tests: canned answers, in order.
#[cfg(test)]
pub(crate) mod mock {
    use std::io::ErrorKind;
    use std::io::Read as _;
    use std::io::Write as _;
    use std::net::TcpListener;
    use std::net::TcpStream;
    use std::time::Duration;
    use std::time::Instant;

    /// Ends an append's retry budget with the answer that carries it: a Retry-After too large to
    /// wait for (or even add to now).
    pub(crate) const LAST: &str = "retry-after: 18446744073709551615";

    /// A loopback port nothing listens on: connects to it are refused while the [`Refusing`]
    /// lives.
    pub(crate) fn refusing() -> (std::net::SocketAddr, Refusing) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let connection = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
        let (accepted, _peer) = listener.accept().unwrap();
        // The listener's port, which bind chose: Linux's connect never hands a port bind chose to
        // another connect, so no test's own connect can take it as its source port and reach
        // itself. The accepted end keeps holding it once the listener is dropped here.
        (accepted.local_addr().unwrap(), Refusing {
            _connection: (connection, accepted),
        })
    }

    /// Holds the port of [`refusing`]: an end of an open loopback connection, so `bind(0)` never
    /// returns it and nothing listens on it (a connect to it is refused).
    /// A socket that is only bound would hold the port too, but macOS drops a connect to it
    /// instead of refusing it, so the connect waits for its timeout.
    pub(crate) struct Refusing {
        _connection: (TcpStream, TcpStream),
    }

    /// Answers one connection per answer, in order, and returns each request's head. An empty
    /// answer closes the connection without one: the attempt's outcome is unknown. The request's
    /// body is read whole before the answer: a connection closed with unread bytes is reset, which
    /// can drop the answer before the client reads it. A connection it waits for over a minute
    /// fails it, so a client that sends fewer requests than answers fails the test's `join`
    /// instead of hanging it.
    pub(crate) fn serve(answers: Vec<String>) -> (String, std::thread::JoinHandle<Vec<String>>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let url = format!("http://{}/b/s", listener.local_addr().unwrap());
        let server = std::thread::spawn(move || {
            answers
                .into_iter()
                .enumerate()
                .map(|(i, answer)| {
                    let mut conn = accepted(&listener, i);
                    let mut request = Vec::new();
                    let mut byte = [0u8; 1];
                    while !request.ends_with(b"\r\n\r\n") {
                        conn.read_exact(&mut byte).unwrap();
                        request.push(byte[0]);
                    }
                    let head = String::from_utf8_lossy(&request).into_owned();
                    let len =
                        request_header(&head, "content-length").map_or(0, |n| n.parse().unwrap());
                    conn.read_exact(&mut vec![0u8; len]).unwrap();
                    conn.write_all(answer.as_bytes()).unwrap();
                    head
                })
                .collect()
        });
        (url, server)
    }

    /// The `i`-th connection on `listener`, which does not block, within a minute.
    pub(crate) fn accepted(listener: &TcpListener, i: usize) -> TcpStream {
        let start = Instant::now();
        loop {
            match listener.accept() {
                Ok((conn, _)) => {
                    conn.set_nonblocking(false).unwrap();
                    return conn;
                }
                Err(e)
                    if e.kind() == ErrorKind::WouldBlock
                        && start.elapsed() < Duration::from_secs(60) =>
                {
                    std::thread::sleep(Duration::from_millis(5));
                }
                Err(e) => panic!("no request {i}: {e}"),
            }
        }
    }

    /// An answer without a body: `head` is the status and the headers.
    pub(crate) fn answer(head: &str) -> String {
        format!("HTTP/1.1 {head}\r\nconnection: close\r\ncontent-length: 0\r\n\r\n")
    }

    pub(crate) fn answer_with_body(head: &str, body: &str) -> String {
        format!(
            "HTTP/1.1 {head}\r\nconnection: close\r\ncontent-length: {}\r\n\r\n{body}",
            body.len()
        )
    }

    /// A gateway's vouch for the marker, as a header line of an answer: alone, it proves nothing.
    pub(crate) const VOUCH: &str = "x-ursula-not-applied-checked: 1";

    /// The header line that marks an answer as applying nothing, with `token`, as a node or a
    /// gateway before 0.7.3 sends it: alone, it proves nothing.
    pub(crate) fn marker(token: &str) -> String {
        format!("x-ursula-not-applied: {token}")
    }

    /// The header lines of an answer marked as applying nothing, with `token`, and vouched for by a
    /// gateway from 0.7.3 on: the marker counts.
    pub(crate) fn vouched(token: &str) -> String {
        format!("{}\r\n{VOUCH}", marker(token))
    }

    /// The value of header `name` in a request head `serve` returned.
    pub(crate) fn request_header<'a>(request: &'a str, name: &str) -> Option<&'a str> {
        request.split("\r\n").find_map(|line| {
            let (key, value) = line.split_once(':')?;
            key.eq_ignore_ascii_case(name).then(|| value.trim())
        })
    }
}

#[cfg(test)]
mod tests {
    use std::io::ErrorKind;
    use std::io::Read as _;
    use std::io::Write as _;
    use std::net::TcpListener;
    use std::net::TcpStream;
    use std::net::ToSocketAddrs as _;
    use std::sync::Arc;
    use std::sync::Barrier;
    use std::sync::Mutex;
    use std::sync::PoisonError;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::time::Duration;
    use std::time::Instant;

    use super::Append;
    use super::Proof;
    use super::Retrying;
    use super::agent;
    use super::answers;
    use super::append;
    use super::append_on;
    use super::applied_nothing;
    use super::before_connection;
    use super::bulk_agent;
    use super::create_stream;
    use super::get_snapshot;
    use super::head;
    use super::in_clear;
    use super::mock::LAST;
    use super::mock::VOUCH;
    use super::mock::accepted;
    use super::mock::answer;
    use super::mock::answer_with_body;
    use super::mock::marker;
    use super::mock::refusing;
    use super::mock::request_header;
    use super::mock::serve;
    use super::mock::vouched;
    use super::new_agent;
    use super::new_agent_with;
    use super::no_answer;
    use super::put_idempotent;
    use super::read_from;
    use crate::auth::set_token;
    use crate::error::Attempt;
    use crate::error::Error;
    use crate::error::ErrorKind as Kind;
    use crate::error::FrameLimit;
    use crate::error::WriteRefusal;
    use crate::net;

    /// Held by each test that sets the process-wide token (`set_token`): tests run in parallel.
    static TOKEN: Mutex<()> = Mutex::new(());

    // A leader read the server could not confirm with a quorum in time answers 503 (Retry-After),
    // and a gateway may rate-limit with 429: catch-up reads and HEAD retry both, as appends do,
    // so attach, the claim check and reclaim never fail on them.
    #[test]
    fn reads_retry_transient_unavailability() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("http://{}/b/s", listener.local_addr().unwrap());
        let answer = |head: &str, body: &str| {
            format!(
                "HTTP/1.1 {head}\r\nconnection: close\r\ncontent-length: {}\r\n\r\n{body}",
                body.len()
            )
        };
        let unavailable = answer(
            "503 Service Unavailable\r\nretry-after: 0",
            "leader unknown",
        );
        let answers = [
            unavailable.clone(),
            answer("429 Too Many Requests", ""),
            answer("200 OK\r\nstream-next-offset: 7", "abc"),
            unavailable.clone(),
            answer("200 OK\r\nstream-retained-offset: 2", ""),
            // A Retry-After too large to wait for (or even add to now) ends the retries at once.
            answer(
                "503 Service Unavailable\r\nretry-after: 18446744073709551615",
                "leader unknown",
            ),
            // So does the stop flag (a re-attach waiting for the snapshot thread).
            unavailable,
        ];
        let server = std::thread::spawn(move || {
            answers
                .into_iter()
                .map(|answer| {
                    let (mut conn, _) = listener.accept().unwrap();
                    let mut request = Vec::new();
                    let mut byte = [0u8; 1];
                    while !request.ends_with(b"\r\n\r\n") {
                        conn.read_exact(&mut byte).unwrap();
                        request.push(byte[0]);
                    }
                    conn.write_all(answer.as_bytes()).unwrap();
                    let request = String::from_utf8_lossy(&request);
                    request.split(" HTTP/").next().unwrap().to_owned()
                })
                .collect::<Vec<_>>()
        });
        let (bytes, next) = read_from(&url, "1", "4").unwrap();
        assert_eq!((&bytes[..], next.as_str()), (&b"abc"[..], "7"));
        assert_eq!(head(&url, &|| false).unwrap().retained, "2");
        let refused = get_snapshot(&url, "1", "9", &|| false).unwrap_err();
        assert!(
            matches!(refused, Error::Status { status: 503, .. }),
            "{refused}"
        );
        let refused = head(&url, &|| true).err().unwrap();
        assert!(
            matches!(refused, Error::Status { status: 503, .. }),
            "{refused}"
        );
        let read = "GET /b/s?offset=4&consistency=leader";
        assert_eq!(server.join().unwrap(), [
            read,
            read,
            read,
            "HEAD /b/s",
            "HEAD /b/s",
            "GET /b/s/snapshot/9",
            "HEAD /b/s"
        ]);
    }

    // Every request names the extension and its version, so servers and gateways tell versions
    // apart: a read, an append and a HEAD.
    #[test]
    fn requests_carry_the_user_agent() {
        let (url, server) = serve(vec![
            answer_with_body("200 OK\r\nstream-next-offset: 7", "abc"),
            answer("200 OK"),
            answer("200 OK\r\nstream-retained-offset: 2"),
        ]);
        read_from(&url, "1", "4").unwrap();
        assert!(matches!(
            append(&url, "i1", "p", b"frame", 1, 1),
            Append::Acked { attempts: 1, .. }
        ));
        head(&url, &|| false).unwrap();
        let requests = server.join().unwrap();
        let expected = concat!("sqlite-ursula-vfs/", env!("CARGO_PKG_VERSION"));
        for (request, method) in requests.iter().zip(["GET ", "POST ", "HEAD "]) {
            assert!(request.starts_with(method), "{request}");
            assert_eq!(
                request_header(request, "user-agent"),
                Some(expected),
                "{request}"
            );
        }
    }

    // Every request carries the token. A 401 is an authorizer refusing it: the request is sent
    // again (with the token read again) within the retry budget, a create too. A 403 is a fence
    // only with the epoch that owns the stream (the server's); without it, an authorizer's final
    // denial, never taken for a fence. Marked `x-ursula-not-applied` with a gateway's vouch, it
    // refuses an append none of whose attempts can have reached the stream (nothing was appended).
    // Unmarked (a filtering proxy may have answered it after the stream applied the append), or
    // after an attempt whose outcome is unknown, it fails the append.
    #[test]
    fn auth_refusals_are_retried_or_final_never_fences() {
        let _token = TOKEN.lock().unwrap_or_else(PoisonError::into_inner);
        set_token(Some("t1"));
        let denied =
            |headers: &str| answer_with_body(&format!("403 Forbidden{headers}"), "read only");
        let marked = format!("\r\n{}", vouched("Forbidden"));
        let (url, server) = serve(vec![
            answer("401 Unauthorized\r\nwww-authenticate: Bearer error=\"invalid_token\""),
            answer("200 OK\r\nstream-next-offset: 9"),
            answer("401 Unauthorized"),
            answer("200 OK\r\nstream-incarnation: i1"),
            answer("403 Forbidden\r\nproducer-epoch: 4"),
            denied(&marked),
            denied(""),
            answer("503 Service Unavailable\r\nretry-after: 0"),
            denied(&marked),
            answer("401 Unauthorized"),
            answer("201 Created"),
        ]);
        let acked = append(&url, "i1", "p", b"x", 1, 1);
        assert!(matches!(acked, Append::Acked { attempts: 2, .. }));
        assert_eq!(
            head(&url, &|| false).unwrap().incarnation.as_deref(),
            Some("i1")
        );
        let fenced = append(&url, "i1", "p", b"x", 1, 2);
        assert!(matches!(fenced, Append::Fenced { current: Some(4) }));
        let denied = append(&url, "i1", "p", b"x", 1, 2);
        assert!(
            matches!(&denied, Append::Refused(WriteRefusal::Forbidden { body }) if body == "read only")
        );
        let unmarked = append(&url, "i1", "p", b"x", 1, 2);
        assert!(matches!(
            unmarked,
            Append::Failed(Error::Unauthorized { status: 403, .. })
        ));
        let after_unknown = append(&url, "i1", "p", b"x", 1, 2);
        assert!(matches!(
            after_unknown,
            Append::Failed(Error::Unauthorized { status: 403, .. })
        ));
        create_stream(&url).unwrap();
        set_token(None);
        let requests = server.join().unwrap();
        assert_eq!(requests.len(), 11);
        for request in &requests {
            assert!(
                request
                    .to_ascii_lowercase()
                    .contains("authorization: bearer t1\r\n"),
                "{request}"
            );
        }
    }

    // `applied_nothing` alone decides whether an answer proves that the stream applied nothing: a
    // 401, 402 or 429 by its status, and a refusal (a 4xx or 5xx) by the marker with a gateway's
    // vouch, which then names it. A redirect or a success proves nothing whatever headers it
    // carries, and neither does the marker or the vouch alone.
    #[test]
    fn only_a_refusal_proves_that_nothing_was_applied() {
        let response = |status: u16, headers: &[(&str, &str)]| {
            let mut r = ureq::http::Response::builder().status(status);
            for &(name, value) in headers {
                r = r.header(name, value);
            }
            r.body(()).unwrap()
        };
        let marker = ("x-ursula-not-applied", "RaftLogPressure");
        let vouch = ("x-ursula-not-applied-checked", "1");
        let marked = Some(Proof::Marked("RaftLogPressure".to_owned()));
        for status in [503, 413, 403, 404, 502, 504, 401, 402, 429] {
            let proof = applied_nothing(&response(status, &[marker, vouch]));
            assert_eq!(proof, marked, "{status}");
        }
        for status in [307, 308, 301, 303, 200, 201, 204] {
            let proof = applied_nothing(&response(status, &[marker, vouch]));
            assert_eq!(proof, None, "{status}");
        }
        for status in [401, 402, 429] {
            for headers in [&[][..], &[marker], &[vouch]] {
                let proof = applied_nothing(&response(status, headers));
                assert_eq!(proof, Some(Proof::Status), "{status} {headers:?}");
            }
        }
        for status in [503, 413, 403, 404, 400, 500] {
            for headers in [&[][..], &[marker], &[vouch]] {
                let proof = applied_nothing(&response(status, headers));
                assert_eq!(proof, None, "{status} {headers:?}");
            }
        }
        // An empty marker still marks, and a long one is cut like a body.
        let empty = applied_nothing(&response(503, &[("x-ursula-not-applied", ""), vouch]));
        assert_eq!(empty, Some(Proof::Marked(String::new())));
        let long = "x".repeat(300);
        let cut = applied_nothing(&response(503, &[("x-ursula-not-applied", &long), vouch]));
        assert_eq!(cut, Some(Proof::Marked("x".repeat(256))));
    }

    // A token crosses the network in clear only over plain HTTP to a host other than this one.
    #[test]
    fn tokens_go_in_clear_over_http_to_other_hosts() {
        let clear = |url: &str| in_clear(&url.parse().unwrap());
        assert!(clear("http://ursula.example.com/b/s"));
        assert!(clear("http://10.0.0.7:4437/b/s"));
        assert!(!clear("https://ursula.example.com/b/s"));
        assert!(!clear("http://127.0.0.1:4437/b/s"));
        assert!(!clear("http://localhost:4437/b/s"));
        assert!(!clear("http://[::1]:4437/b/s"));
    }

    // An error message or a log line shows the start of a response body only: a gateway's error
    // page is not repeated whole on every retried attempt.
    #[test]
    fn response_bodies_are_cut_for_messages() {
        let long = "x".repeat(4_000);
        let (url, server) = serve(vec![format!(
            "HTTP/1.1 400 Bad Request\r\nconnection: close\r\ncontent-length: {}\r\n\r\n{long}",
            long.len()
        )]);
        let rejected = append(&url, "i1", "p", b"x", 1, 1);
        let Append::Failed(Error::AppendRejected { status, body }) = rejected else {
            panic!("not rejected");
        };
        assert_eq!(status, 400);
        assert_eq!(body, format!("{}...", "x".repeat(256)));
        server.join().unwrap();
    }

    // A 402 is a layer in front of the stream refusing the request: it fails that request alone,
    // with the layer's explanation, and is never retried, except by an append one of whose earlier
    // attempts may have reached the stream (no answer, a 5xx or a 307): its outcome is still
    // unknown, so it is sent again until an answer settles it. Refusals in front of the stream
    // (401, 402) and rate limiting (429) leave the outcome known.
    #[test]
    fn payment_refusals_fail_the_request_alone() {
        let reason = "{\"detail\":\"quota exhausted\"}";
        let refused = || answer_with_body("402 Payment Required", reason);
        let payment = |r: &Error| match r.refusal() {
            Some(WriteRefusal::PaymentRequired { body }) => body == reason,
            _ => false,
        };
        let (url, server) = serve(vec![
            refused(),
            answer("429 Too Many Requests"),
            refused(),
            answer("401 Unauthorized"),
            refused(),
            // No answer at all: the attempt may have reached the stream.
            String::new(),
            refused(),
            answer("503 Service Unavailable"),
            refused(),
            answer("200 OK\r\nstream-next-offset: 9"),
            refused(),
            refused(),
            refused(),
        ]);
        let first = append(&url, "i1", "p", b"x", 1, 1);
        assert!(
            matches!(&first, Append::Refused(WriteRefusal::PaymentRequired { body }) if body == reason)
        );
        let after_429 = append(&url, "i1", "p", b"x", 1, 1);
        assert!(matches!(
            after_429,
            Append::Refused(WriteRefusal::PaymentRequired { .. })
        ));
        let after_401 = append(&url, "i1", "p", b"x", 1, 1);
        assert!(matches!(
            after_401,
            Append::Refused(WriteRefusal::PaymentRequired { .. })
        ));
        let settled = append(&url, "i1", "p", b"x", 1, 1);
        assert!(
            matches!(settled, Append::Acked { next: Some(ref n), attempts: 5, duplicate: false } if n == "9"),
            "a 402 after an unknown outcome is retried"
        );
        let create = create_stream(&url).unwrap_err();
        assert!(matches!(create, Error::Refused { op: "create", .. }) && payment(&create));
        // A HEAD answer has no body: no explanation.
        let head = head(&url, &|| false).err().unwrap();
        assert!(matches!(head, Error::Refused {
            op: "head",
            refusal: WriteRefusal::PaymentRequired { .. },
            ..
        }));
        let put = put_idempotent(&url, "i1", b"", &|| false).err().unwrap();
        assert!(matches!(put, Error::Refused { op: "put", .. }) && payment(&put));
        let requests = server.join().unwrap();
        let methods: Vec<&str> = requests
            .iter()
            .map(|r| r.split(' ').next().unwrap())
            .collect();
        assert_eq!(methods, [
            "POST", "POST", "POST", "POST", "POST", "POST", "POST", "POST", "POST", "POST", "PUT",
            "HEAD", "PUT"
        ]);
    }

    // An append whose outcome stays unknown to the end (a 402 after an attempt that may have
    // reached the stream) fails as unknown, never as refused: the caller must not take the
    // producer sequence for free.
    #[test]
    fn a_payment_refusal_after_an_unknown_outcome_stays_unknown() {
        let (url, server) = serve(vec![
            answer("503 Service Unavailable"),
            answer_with_body(
                &format!("402 Payment Required\r\n{LAST}"),
                "quota exhausted",
            ),
        ]);
        let unknown = append(&url, "i1", "p", b"x", 1, 1);
        let Append::Failed(Error::AppendUnknown { attempts, last }) = unknown else {
            panic!("not unknown");
        };
        assert_eq!(attempts, 2);
        assert!(
            matches!(last, Attempt::Status { status: 402, ref body } if body == "quota exhausted")
        );
        assert_eq!(server.join().unwrap().len(), 2);
    }

    // Answers that applied nothing (401, 429, or a refusal marked `x-ursula-not-applied` with a
    // gateway's vouch) leave the outcome known: when the budget runs out on them, the append is
    // refused, with the last answer's reason (the marker's token when there is one, cut like a
    // body, and never the body beside it; an empty marker still marks), and the producer sequence
    // is free. A marked final answer refuses it at once.
    #[test]
    fn refusals_that_applied_nothing_refuse_the_append_when_the_budget_runs_out() {
        let marked = |status: &str, token: &str, last: &str| {
            answer(&format!("{status}\r\n{}\r\n{last}", vouched(token)))
        };
        let (url, server) = serve(vec![
            // The token refused to the end, read again before every attempt.
            answer("401 Unauthorized\r\nretry-after: 0"),
            answer_with_body(&format!("401 Unauthorized\r\n{LAST}"), "expired"),
            // The server under pressure, to the end.
            marked(
                "503 Service Unavailable",
                "RaftLogPressure",
                "retry-after: 0",
            ),
            marked("503 Service Unavailable", "RaftLogPressure", LAST),
            // Kinds mixed: the last answer names the refusal.
            marked(
                "503 Service Unavailable",
                "AuthorizationUnavailable",
                "retry-after: 0",
            ),
            answer("429 Too Many Requests\r\nretry-after: 0"),
            answer_with_body(&format!("429 Too Many Requests\r\n{LAST}"), "slow down"),
            // A marked 429 stays rate limiting, with the marker's token for the body it lacks.
            marked("429 Too Many Requests", "TenantRateLimited", LAST),
            // A vouched 413: refused at once, as too large. Unmarked: rejected (poisons).
            answer(&format!(
                "413 Payload Too Large\r\n{}",
                vouched("RequestBodyTooLarge")
            )),
            answer("413 Payload Too Large"),
            // A marker without a token, and one with a token too long to show whole.
            marked("503 Service Unavailable", "", LAST),
            marked("503 Service Unavailable", &"x".repeat(300), LAST),
            // A body besides the marker: the reason is the marker's token all the same.
            answer_with_body(
                &format!(
                    "503 Service Unavailable\r\n{}\r\n{LAST}",
                    vouched("RaftLogPressure")
                ),
                "{\"error\":\"RaftLogPressure\"}",
            ),
        ]);
        let unauthorized = append(&url, "i1", "p", b"x", 1, 1);
        assert!(
            matches!(&unauthorized, Append::Refused(WriteRefusal::Unauthorized { body }) if body == "expired")
        );
        let pressure = append(&url, "i1", "p", b"x", 1, 1);
        assert!(matches!(
            &pressure,
            Append::Refused(WriteRefusal::NotApplied { status: 503, reason }) if reason == "RaftLogPressure"
        ));
        let limited = append(&url, "i1", "p", b"x", 1, 1);
        assert!(
            matches!(&limited, Append::Refused(WriteRefusal::RateLimited { body }) if body == "slow down")
        );
        let marked_429 = append(&url, "i1", "p", b"x", 1, 1);
        assert!(matches!(
            &marked_429,
            Append::Refused(WriteRefusal::RateLimited { body }) if body == "TenantRateLimited"
        ));
        let too_large = append(&url, "i1", "p", b"x", 1, 1);
        assert!(matches!(
            &too_large,
            Append::Refused(WriteRefusal::TooLarge { bytes: 1, over: FrameLimit::InFront { token } }) if token == "RequestBodyTooLarge"
        ));
        let rejected = append(&url, "i1", "p", b"x", 1, 1);
        assert!(matches!(
            rejected,
            Append::Failed(Error::AppendRejected { status: 413, .. })
        ));
        let Append::Refused(empty) = append(&url, "i1", "p", b"x", 1, 1) else {
            panic!("an empty marker does not mark");
        };
        assert_eq!(empty.to_string(), "503 (not applied)");
        assert!(
            matches!(empty, WriteRefusal::NotApplied { status: 503, .. })
                && empty.reason().is_none()
        );
        let long = append(&url, "i1", "p", b"x", 1, 1);
        assert!(matches!(
            &long,
            Append::Refused(WriteRefusal::NotApplied { status: 503, reason }) if *reason == "x".repeat(256)
        ));
        let with_body = append(&url, "i1", "p", b"x", 1, 1);
        assert!(matches!(
            &with_body,
            Append::Refused(WriteRefusal::NotApplied { status: 503, reason }) if reason == "RaftLogPressure"
        ));
        assert_eq!(server.join().unwrap().len(), 13);
    }

    // A 401, 402, 403 or 429 keeps its own kind, marked or not, with the layer's explanation, or
    // the marker's token when the answer has no body. Only a 401, 402 or 429 proves anything by its
    // status: a 502 or a 504 with only the vouch is an unknown outcome, and a 404 with only the
    // marker a rejection. A snapshot publish decides no producer sequence, so a vouched 413 stays
    // its status there, and a vouched 402 without a body is refused with the marker's token.
    #[test]
    fn refusals_keep_their_kind_and_no_other_status_proves_anything() {
        let (url, server) = serve(vec![
            answer(&format!(
                "401 Unauthorized\r\n{}\r\n{LAST}",
                vouched("InvalidCredential")
            )),
            answer_with_body(
                &format!("402 Payment Required\r\n{}", vouched("QuotaExhausted")),
                "quota",
            ),
            answer(&format!(
                "402 Payment Required\r\n{}",
                vouched("QuotaExhausted")
            )),
            answer(&format!("403 Forbidden\r\n{}", vouched("Forbidden"))),
            answer(&format!(
                "429 Too Many Requests\r\n{}\r\n{LAST}",
                vouched("TenantRateLimited")
            )),
            answer(&format!("502 Bad Gateway\r\n{VOUCH}\r\n{LAST}")),
            answer(&format!("504 Gateway Timeout\r\n{VOUCH}\r\n{LAST}")),
            answer(&format!("404 Not Found\r\n{}", marker("NotFound"))),
            answer(&format!(
                "413 Payload Too Large\r\n{}",
                vouched("RequestBodyTooLarge")
            )),
            answer(&format!(
                "402 Payment Required\r\n{}",
                vouched("QuotaExhausted")
            )),
        ]);
        let refused = |expected: WriteRefusal| {
            let outcome = append(&url, "i1", "p", b"x", 1, 1);
            assert!(
                matches!(&outcome, Append::Refused(refusal) if *refusal == expected),
                "{expected:?}"
            );
        };
        refused(WriteRefusal::Unauthorized {
            body: "InvalidCredential".into(),
        });
        refused(WriteRefusal::PaymentRequired {
            body: "quota".into(),
        });
        refused(WriteRefusal::PaymentRequired {
            body: "QuotaExhausted".into(),
        });
        refused(WriteRefusal::Forbidden {
            body: "Forbidden".into(),
        });
        refused(WriteRefusal::RateLimited {
            body: "TenantRateLimited".into(),
        });
        for status in [502, 504] {
            let unknown = append(&url, "i1", "p", b"x", 1, 1);
            assert!(
                matches!(
                    unknown,
                    Append::Failed(Error::AppendUnknown {
                        attempts: 1,
                        last: Attempt::Status { status: s, .. }
                    }) if s == status
                ),
                "{status}"
            );
        }
        let rejected = append(&url, "i1", "p", b"x", 1, 1);
        assert!(matches!(
            rejected,
            Append::Failed(Error::AppendRejected { status: 404, .. })
        ));
        let (status, _) = put_idempotent(&url, "i1", b"x", &|| false).unwrap();
        assert_eq!(status, 413);
        let put = put_idempotent(&url, "i1", b"x", &|| false).err().unwrap();
        assert!(matches!(put, Error::Refused {
            op: "put",
            refusal: WriteRefusal::PaymentRequired { ref body },
            ..
        } if body == "QuotaExhausted"));
        assert_eq!(server.join().unwrap().len(), 10);
    }

    // A frame over the extension's limit is never sent: the append fails alone at once as too
    // large, and nothing reaches the server (with no answer to give, it would refuse a connection).
    // A frame at the limit is sent.
    #[test]
    fn a_frame_over_the_limit_is_never_sent() {
        use crate::config::DEFAULT_MAX_FRAME_BYTES;

        // A listener that never accepts: a connection would still be queued on it.
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("http://{}/b/s", listener.local_addr().unwrap());
        let over = vec![0_u8; DEFAULT_MAX_FRAME_BYTES + 1];
        let refused = append(&url, "i1", "p", &over, 1, 1);
        assert!(matches!(
            refused,
            Append::Unsent(WriteRefusal::TooLarge { bytes, over: FrameLimit::Extension { limit } })
                if bytes == DEFAULT_MAX_FRAME_BYTES + 1 && limit == DEFAULT_MAX_FRAME_BYTES
        ));
        listener.set_nonblocking(true).unwrap();
        assert!(listener.accept().is_err(), "nothing connected");

        let (url, server) = serve(vec![answer("200 OK")]);
        let at = vec![0_u8; DEFAULT_MAX_FRAME_BYTES];
        assert!(matches!(
            append(&url, "i1", "p", &at, 1, 1),
            Append::Acked { attempts: 1, .. }
        ));
        assert_eq!(server.join().unwrap().len(), 1);
    }

    // The probe after a large snapshot body failed: the stream answers it only with a status below
    // 500 that is no redirect, on the first attempt. A redirect, a 5xx (a gateway whose servers are
    // down), no answer or nothing listening is none.
    #[test]
    fn the_stream_answers_a_probe_only_below_500() {
        for (status, answered) in [
            ("200 OK", true),
            ("404 Not Found", true),
            ("401 Unauthorized", true),
            (
                "307 Temporary Redirect\r\nlocation: http://127.0.0.1:9/b/s",
                false,
            ),
            ("500 Internal Server Error", false),
            ("502 Bad Gateway", false),
            ("503 Service Unavailable", false),
            ("504 Gateway Timeout", false),
            ("", false),
        ] {
            let (url, server) = serve(vec![if status.is_empty() {
                String::new()
            } else {
                answer(status)
            }]);
            assert_eq!(answers(&url), answered, "{status:?}");
            let requests = server.join().unwrap();
            assert!(requests[0].starts_with("HEAD /b/s "), "{requests:?}");
        }
        let (nothing, _refusing) = refusing();
        let url = format!("http://{nothing}/b/s");
        assert!(!answers(&url), "nothing listening");
    }

    // A 5xx that asks for a later attempt (Retry-After) or says it applied nothing puts the request
    // off (`Attempt::Deferred`), as the server's ingress admission does under pressure. The second
    // send of a large body ends the call at once, without waiting the 30 s it asked for: the error
    // keeps the Retry-After, for the snapshot thread's wait.
    #[test]
    fn a_large_body_put_off_ends_the_call_and_keeps_the_retry_after() {
        let large = vec![7u8; (1 << 20) + 1];
        let (url, server) = serve(vec![
            String::new(),
            answer("503 Service Unavailable\r\nretry-after: 30"),
        ]);
        let started = Instant::now();
        let put = put_idempotent(&format!("{url}/snapshot/7"), "i1", &large, &|| false);
        assert!(started.elapsed() < Duration::from_secs(5));
        assert!(matches!(put, Err(Error::PutUnknown {
            last: Attempt::Deferred {
                status: 503,
                retry_after: Some(after),
                ..
            },
            ..
        }) if after == Duration::from_secs(30)));
        assert_eq!(server.join().unwrap().len(), 2);

        let marked = || {
            answer(&format!(
                "503 Service Unavailable\r\n{}",
                marker("IngressBodyBytesLimitReached")
            ))
        };
        let (url, server) = serve(vec![marked(), marked()]);
        let put = put_idempotent(&format!("{url}/snapshot/7"), "i1", &large, &|| false);
        assert!(matches!(
            put,
            Err(Error::PutUnknown {
                last: Attempt::Deferred {
                    status: 503,
                    retry_after: None,
                    ..
                },
                ..
            })
        ));
        assert_eq!(server.join().unwrap().len(), 2);
    }

    // A body over 1 MiB whose outcome stays unknown is sent twice at most: a large snapshot that a
    // layer in front cuts off for its size, or a proxy times out, fails the same way again, and the
    // snapshot thread backs off instead. A refused token, sent again with the token read again,
    // does not count, and a 307 counts like a 503. A smaller body (a small snapshot, a retention
    // move) is retried within the budget, as before. Each stand-in answers exactly the requests
    // expected: one more would find nothing listening.
    #[test]
    fn a_large_body_is_sent_twice_at_most_while_its_outcome_stays_unknown() {
        let large = vec![7u8; (1 << 20) + 1];
        let sent = |answers: Vec<String>, body: &[u8]| {
            let (url, server) = serve(answers);
            let put = put_idempotent(&format!("{url}/snapshot/7"), "i1", body, &|| false);
            (put, server)
        };

        // Its token refused, then no answer, then a 503.
        let (put, server) = sent(
            vec![
                answer("401 Unauthorized"),
                String::new(),
                answer("503 Service Unavailable"),
            ],
            &large,
        );
        assert!(matches!(
            put,
            Err(Error::PutUnknown {
                last: Attempt::Status { status: 503, .. },
                ..
            })
        ));
        assert_eq!(server.join().unwrap().len(), 3);

        // A redirect, which is never followed, then a 503.
        let (put, server) = sent(
            vec![
                answer("307 Temporary Redirect\r\nlocation: http://127.0.0.1:9/b/s/snapshot/7"),
                answer("503 Service Unavailable"),
            ],
            &large,
        );
        assert!(matches!(
            put,
            Err(Error::PutUnknown {
                last: Attempt::Status { status: 503, .. },
                ..
            })
        ));
        assert_eq!(server.join().unwrap().len(), 2);

        // A small snapshot: three answers with an unknown outcome, then accepted.
        let (put, server) = sent(
            vec![
                answer("503 Service Unavailable"),
                String::new(),
                answer("502 Bad Gateway"),
                answer("204 No Content"),
            ],
            &[7u8; 4096],
        );
        assert!(matches!(put, Ok((204, _))));
        assert_eq!(server.join().unwrap().len(), 4);
    }

    // One attempt whose outcome is unknown (no answer, an unmarked 5xx) and the append may be in
    // the stream: refusals before or after it prove nothing about it, vouched for or not, so a
    // budget that runs out fails the append as unknown (or as unauthorized, after a refused token),
    // and a marked final answer is a rejection.
    #[test]
    fn one_unknown_outcome_among_refusals_keeps_the_append_unknown() {
        let pressure = |last: &str| {
            answer(&format!(
                "503 Service Unavailable\r\n{}\r\n{last}",
                vouched("RaftLogPressure")
            ))
        };
        let (url, server) = serve(vec![
            // No answer, then refusals to the end.
            String::new(),
            pressure(LAST),
            // Refusals, then an unmarked 503 to the end.
            pressure("retry-after: 0"),
            answer(&format!("503 Service Unavailable\r\n{LAST}")),
            // An unmarked 503, then a refused token to the end.
            answer("503 Service Unavailable\r\nretry-after: 0"),
            answer(&format!("401 Unauthorized\r\n{LAST}")),
            // An unmarked 503, then a marked final answer.
            answer("503 Service Unavailable\r\nretry-after: 0"),
            answer(&format!(
                "413 Payload Too Large\r\n{}",
                vouched("RequestBodyTooLarge")
            )),
        ]);
        let unknown = append(&url, "i1", "p", b"x", 1, 1);
        assert!(matches!(
            unknown,
            Append::Failed(Error::AppendUnknown {
                attempts: 2,
                last: Attempt::Status { status: 503, .. }
            })
        ));
        let unknown = append(&url, "i1", "p", b"x", 1, 1);
        assert!(matches!(
            unknown,
            Append::Failed(Error::AppendUnknown { attempts: 2, .. })
        ));
        let unauthorized = append(&url, "i1", "p", b"x", 1, 1);
        assert!(matches!(
            unauthorized,
            Append::Failed(Error::Unauthorized { status: 401, .. })
        ));
        let rejected = append(&url, "i1", "p", b"x", 1, 1);
        assert!(matches!(
            rejected,
            Append::Failed(Error::AppendRejected { status: 413, .. })
        ));
        assert_eq!(server.join().unwrap().len(), 8);
    }

    // A commit's attempt answered 204, the server's answer to a duplicate, while no earlier attempt
    // of it can have reached the stream is a duplicate that no attempt of it explains: a commit
    // refused before it at the same sequence may have been applied after all (`Db::append_commit`
    // decides). A refused attempt of the same append does not explain it either. A 200 is an
    // acknowledgement, a 204 after an attempt that may have reached the stream (no answer, or a
    // 307, marked and vouched for or not: a redirect proves nothing) is that attempt's duplicate,
    // and a claim's 204 is verified separately. A 307 to the end of the budget leaves the outcome
    // unknown.
    #[test]
    fn a_duplicate_that_no_attempt_explains_is_told_apart() {
        let pressure = |last: &str| {
            answer(&format!(
                "503 Service Unavailable\r\n{}\r\n{last}",
                vouched("RaftLogPressure")
            ))
        };
        let duplicate = || answer("204 No Content\r\nstream-next-offset: 12");
        let redirect = |last: &str| {
            answer(&format!(
                "307 Temporary Redirect\r\nlocation: /b/s\r\n{}\r\n{last}",
                vouched("RaftLogPressure")
            ))
        };
        let (url, server) = serve(vec![
            // Refused to the end, then a duplicate's answer to the next commit at its sequence.
            pressure(LAST),
            duplicate(),
            // Refused to the end, then a fresh append's answer.
            pressure(LAST),
            answer("200 OK\r\nstream-next-offset: 12"),
            // A refused attempt, then a duplicate's answer, within one append.
            pressure("retry-after: 0"),
            duplicate(),
            // No answer, then a duplicate's answer: the lost attempt's.
            String::new(),
            duplicate(),
            // A redirect, marked and vouched for, then a duplicate's answer: the redirected
            // attempt's.
            redirect("retry-after: 0"),
            duplicate(),
            // The same redirect to the end of the budget.
            redirect(LAST),
            // A claim's duplicate.
            duplicate(),
        ]);
        let refused = append(&url, "i1", "p", b"x", 1, 5);
        assert!(matches!(
            refused,
            Append::Refused(WriteRefusal::NotApplied { status: 503, .. })
        ));
        let unexplained = append(&url, "i1", "p", b"x", 1, 5);
        assert!(matches!(
            unexplained,
            Append::UnexplainedDuplicate { next: Some(ref n), attempts: 1 } if n == "12"
        ));
        assert!(matches!(
            append(&url, "i1", "p", b"x", 1, 5),
            Append::Refused(_)
        ));
        let fresh = append(&url, "i1", "p", b"x", 1, 5);
        assert!(
            matches!(fresh, Append::Acked { next: Some(ref n), attempts: 1, duplicate: false } if n == "12")
        );
        let within = append(&url, "i1", "p", b"x", 1, 5);
        assert!(matches!(within, Append::UnexplainedDuplicate {
            attempts: 2,
            ..
        }));
        let retried = append(&url, "i1", "p", b"x", 1, 5);
        assert!(
            matches!(retried, Append::Acked { next: Some(ref n), attempts: 2, duplicate: true } if n == "12")
        );
        let redirected = append(&url, "i1", "p", b"x", 1, 5);
        assert!(
            matches!(redirected, Append::Acked { next: Some(ref n), attempts: 2, duplicate: true } if n == "12")
        );
        let unknown = append(&url, "i1", "p", b"x", 1, 5);
        assert!(matches!(
            unknown,
            Append::Failed(Error::AppendUnknown {
                attempts: 1,
                last: Attempt::Status { status: 307, .. }
            })
        ));
        let claim = append(&url, "i1", "p", b"x", 1, 0);
        assert!(matches!(claim, Append::Acked { attempts: 1, .. }));
        assert_eq!(server.join().unwrap().len(), 12);
    }

    // A marker counts only with a gateway's vouch (`applied_nothing`), and the vouch alone proves
    // nothing: an answer with one of the two headers is read by its status alone. A 503 is then an
    // unknown outcome, so a budget that runs out on it fails the append as unknown (a poison), and
    // a 403 or a 413 fails it. A 401, 402 or 429 proves by its status that it applied nothing, with
    // either header or none (a 429 marked alone is rate limiting). One attempt marked alone among
    // vouched refusals leaves the append unknown.
    #[test]
    fn a_marker_proves_nothing_without_the_vouch() {
        let unavailable = |headers: &str, last: &str| {
            answer(&format!("503 Service Unavailable\r\n{headers}\r\n{last}"))
        };
        let (url, server) = serve(vec![
            // The marker alone, and the vouch alone, to the end.
            unavailable(&marker("RaftLogPressure"), LAST),
            unavailable(VOUCH, LAST),
            // A denial and a request too large, each with one of the two headers.
            answer(&format!("403 Forbidden\r\n{}", marker("Forbidden"))),
            answer(&format!("403 Forbidden\r\n{VOUCH}")),
            answer(&format!(
                "413 Payload Too Large\r\n{}",
                marker("RequestBodyTooLarge")
            )),
            answer(&format!("413 Payload Too Large\r\n{VOUCH}")),
            // A refused token, a 402 and a 429, marked alone: their status proves it.
            answer(&format!(
                "401 Unauthorized\r\n{}\r\n{LAST}",
                marker("TokenExpired")
            )),
            answer_with_body(
                &format!("402 Payment Required\r\n{}", marker("QuotaExhausted")),
                "quota",
            ),
            answer_with_body(
                &format!(
                    "429 Too Many Requests\r\n{}\r\n{LAST}",
                    marker("TenantRateLimited")
                ),
                "slow down",
            ),
            // Vouched refusals around one marked alone.
            unavailable(&vouched("RaftLogPressure"), "retry-after: 0"),
            unavailable(&marker("RaftLogPressure"), "retry-after: 0"),
            unavailable(&vouched("RaftLogPressure"), LAST),
        ]);
        for _ in 0..2 {
            let unknown = append(&url, "i1", "p", b"x", 1, 1);
            assert!(matches!(
                unknown,
                Append::Failed(Error::AppendUnknown {
                    attempts: 1,
                    last: Attempt::Status { status: 503, .. }
                })
            ));
        }
        for _ in 0..2 {
            let denied = append(&url, "i1", "p", b"x", 1, 1);
            assert!(matches!(
                denied,
                Append::Failed(Error::Unauthorized { status: 403, .. })
            ));
        }
        for _ in 0..2 {
            let rejected = append(&url, "i1", "p", b"x", 1, 1);
            assert!(matches!(
                rejected,
                Append::Failed(Error::AppendRejected { status: 413, .. })
            ));
        }
        let token = append(&url, "i1", "p", b"x", 1, 1);
        assert!(matches!(
            token,
            Append::Refused(WriteRefusal::Unauthorized { .. })
        ));
        let payment = append(&url, "i1", "p", b"x", 1, 1);
        assert!(
            matches!(&payment, Append::Refused(WriteRefusal::PaymentRequired { body }) if body == "quota")
        );
        let limited = append(&url, "i1", "p", b"x", 1, 1);
        assert!(
            matches!(&limited, Append::Refused(WriteRefusal::RateLimited { body }) if body == "slow down")
        );
        let mixed = append(&url, "i1", "p", b"x", 1, 1);
        assert!(matches!(
            mixed,
            Append::Failed(Error::AppendUnknown { attempts: 3, .. })
        ));
        assert_eq!(server.join().unwrap().len(), 12);
    }

    // Redirects are never followed: followed, a request would go out without the token (ureq
    // drops it on every redirect), and a 301, 302 or 303 would turn an append into a GET. A 307 (a
    // gateway passing on the server's redirect to a leader that moved) is sent again to the same
    // URL, with the same method, sequence and token: reads, creates and PUTs retry it like a 503,
    // an append as an unknown outcome. Any other redirect fails the request at once with its
    // Location, a 308 among them (a proxy that redirects `http://` to `https://`).
    #[test]
    fn redirects_are_sent_again_in_place_or_fail_never_followed() {
        let _token = TOKEN.lock().unwrap_or_else(PoisonError::into_inner);
        set_token(Some("t2"));
        let redirect = |status: &str| answer(&format!("{status}\r\nlocation: /elsewhere"));
        let (url, server) = serve(vec![
            redirect("307 Temporary Redirect"),
            answer_with_body("200 OK\r\nstream-next-offset: 7", "abc"),
            redirect("307 Temporary Redirect"),
            answer("200 OK\r\nstream-incarnation: i1"),
            redirect("307 Temporary Redirect"),
            answer_with_body("200 OK", "snapshot"),
            redirect("307 Temporary Redirect"),
            answer("201 Created"),
            redirect("307 Temporary Redirect"),
            answer("204 No Content"),
            redirect("307 Temporary Redirect"),
            redirect("307 Temporary Redirect"),
            answer("200 OK\r\nstream-next-offset: 9"),
            redirect("301 Moved Permanently"),
            redirect("302 Found"),
            redirect("303 See Other"),
            redirect("301 Moved Permanently"),
            redirect("302 Found"),
            redirect("301 Moved Permanently"),
            redirect("308 Permanent Redirect"),
            redirect("308 Permanent Redirect"),
        ]);
        let (bytes, next) = read_from(&url, "i1", "4").unwrap();
        assert_eq!((&bytes[..], next.as_str()), (&b"abc"[..], "7"));
        let head_answer = head(&url, &|| false).unwrap();
        assert_eq!(head_answer.incarnation.as_deref(), Some("i1"));
        let snapshot = get_snapshot(&url, "i1", "9", &|| false).unwrap();
        assert_eq!(snapshot.as_deref(), Some(&b"snapshot"[..]));
        create_stream(&url).unwrap();
        let retention = format!("{url}/retention/5");
        let (status, _) = put_idempotent(&retention, "i1", b"", &|| false).unwrap();
        assert_eq!(status, 204);
        let acked = append(&url, "i1", "p", b"commit", 1, 1);
        assert!(
            matches!(acked, Append::Acked { next: Some(ref n), attempts: 3, duplicate: false } if n == "9")
        );
        let moved = append(&url, "i1", "p", b"commit", 1, 2);
        assert!(matches!(
            moved,
            Append::Failed(Error::Redirected { op: "append", status: 301, location: Some(ref l), .. })
                if l == "/elsewhere"
        ));
        let found = head(&url, &|| false).err().unwrap();
        assert!(matches!(found, Error::Redirected {
            op: "head",
            status: 302,
            ..
        }));
        let other = read_from(&url, "i1", "4").unwrap_err();
        assert!(matches!(other, Error::Redirected {
            op: "read",
            status: 303,
            ..
        }));
        let put = put_idempotent(&url, "i1", b"", &|| false).err().unwrap();
        assert!(matches!(put, Error::Redirected {
            op: "put",
            status: 301,
            ..
        }));
        let create = create_stream(&url).unwrap_err();
        assert!(matches!(create, Error::Redirected {
            op: "create",
            status: 302,
            ..
        }));
        assert!(
            create.to_string().contains("302 redirect to /elsewhere"),
            "{create}"
        );
        let snapshot = get_snapshot(&url, "i1", "9", &|| false).unwrap_err();
        assert!(matches!(snapshot, Error::Redirected {
            op: "get snapshot",
            status: 301,
            ..
        }));
        let proxied = append(&url, "i1", "p", b"commit", 1, 2);
        assert!(matches!(
            proxied,
            Append::Failed(Error::Redirected { op: "append", status: 308, location: Some(ref l), .. })
                if l == "/elsewhere"
        ));
        let proxied = head(&url, &|| false).err().unwrap();
        assert!(matches!(proxied, Error::Redirected {
            op: "head",
            status: 308,
            ..
        }));
        set_token(None);
        let requests = server.join().unwrap();
        let lines: Vec<&str> = requests
            .iter()
            .map(|r| r.split(" HTTP/").next().unwrap())
            .collect();
        let read = "GET /b/s?offset=4&consistency=leader";
        assert_eq!(lines, [
            read,
            read,
            "HEAD /b/s",
            "HEAD /b/s",
            "GET /b/s/snapshot/9",
            "GET /b/s/snapshot/9",
            "PUT /b/s",
            "PUT /b/s",
            "PUT /b/s/retention/5",
            "PUT /b/s/retention/5",
            "POST /b/s",
            "POST /b/s",
            "POST /b/s",
            "POST /b/s",
            "HEAD /b/s",
            read,
            "PUT /b/s",
            "PUT /b/s",
            "GET /b/s/snapshot/9",
            "POST /b/s",
            "HEAD /b/s"
        ]);
        for request in &requests {
            let request = request.to_ascii_lowercase();
            assert!(
                request.contains("authorization: bearer t2\r\n"),
                "{request}"
            );
        }
        for request in &requests[10..13] {
            let request = request.to_ascii_lowercase();
            assert!(request.contains("producer-seq: 1\r\n"), "{request}");
            assert!(request.contains("content-length: 6\r\n"), "{request}");
        }
    }

    // A 307 that lasts the whole retry budget leaves an append's or a PUT's outcome unknown (a
    // leader that loses its leadership answers so, and what it proposed may still be committed),
    // and fails a HEAD, a read, a snapshot GET or a create with its status: attach never takes it
    // for a missing stream (a 404), nor a snapshot GET for a superseded snapshot.
    #[test]
    fn a_redirect_to_the_end_of_the_budget_is_an_unknown_outcome() {
        // The budget ends once a wait would pass it: a Retry-After too large to wait for. The
        // server's redirect explains itself in its body, which the gateway passes on.
        let redirect = |body: &str| {
            answer_with_body(
                "307 Temporary Redirect\r\nlocation: /b/s\r\nretry-after: 18446744073709551615",
                body,
            )
        };
        let (url, server) = serve(vec![
            redirect(""),
            redirect(""),
            redirect(""),
            redirect("not the leader"),
            redirect(""),
            redirect("not the leader"),
            redirect("not the leader"),
        ]);
        let unknown = append(&url, "i1", "p", b"x", 1, 1);
        let Append::Failed(Error::AppendUnknown { attempts, last }) = unknown else {
            panic!("not unknown");
        };
        assert_eq!(attempts, 1);
        assert!(matches!(last, Attempt::Status { status: 307, .. }));
        let put = put_idempotent(&url, "i1", b"", &|| false).err().unwrap();
        assert!(
            matches!(put, Error::PutUnknown {
                last: Attempt::Status { status: 307, .. },
                ..
            }),
            "{put}"
        );
        let head = head(&url, &|| false).err().unwrap();
        assert!(
            matches!(head, Error::Status {
                op: "head",
                status: 307,
                ..
            }),
            "{head}"
        );
        let read = read_from(&url, "i1", "4").unwrap_err();
        assert!(
            matches!(read, Error::Status {
                op: "read",
                status: 307,
                ..
            }),
            "{read}"
        );
        // Without a body and with one.
        for _ in 0..2 {
            let snapshot = get_snapshot(&url, "i1", "9", &|| false).unwrap_err();
            assert!(
                matches!(snapshot, Error::Status {
                    op: "get snapshot",
                    status: 307,
                    ..
                }),
                "{snapshot}"
            );
        }
        let create = create_stream(&url).unwrap_err();
        assert!(
            matches!(create, Error::Status {
                op: "create",
                status: 307,
                ..
            }),
            "{create}"
        );
        assert_eq!(server.join().unwrap().len(), 7);
    }

    // A 307 leaves an append's outcome unknown (a leader that loses its leadership answers so, and
    // what it proposed may still be committed): a 402 after it, and every later one, is retried as
    // after a 503, until an answer settles the outcome or the budget runs out. Taken to have
    // applied nothing, it would end the append as refused with its sequence free, and the next
    // commit, sent with that sequence, could be acknowledged as this one's duplicate, its pages
    // never appended.
    #[test]
    fn a_payment_refusal_after_a_redirect_is_retried() {
        let refused = |head: &str| answer_with_body(head, "quota exhausted");
        let (url, server) = serve(vec![
            answer("307 Temporary Redirect\r\nlocation: /b/s"),
            refused("402 Payment Required"),
            answer("200 OK\r\nstream-next-offset: 9"),
            answer("307 Temporary Redirect\r\nlocation: /b/s"),
            refused("402 Payment Required"),
            // The budget ends once a wait would pass it: a Retry-After too large to wait for.
            refused("402 Payment Required\r\nretry-after: 18446744073709551615"),
        ]);
        let settled = append(&url, "i1", "p", b"x", 1, 1);
        assert!(
            matches!(settled, Append::Acked { next: Some(ref n), attempts: 3, duplicate: false } if n == "9"),
            "a 402 after a 307 was taken to have applied nothing"
        );
        let unknown = append(&url, "i1", "p", b"x", 1, 2);
        let Append::Failed(Error::AppendUnknown { attempts, last }) = unknown else {
            panic!("a second 402 after a 307 was taken to have applied nothing");
        };
        assert_eq!(attempts, 3);
        assert!(
            matches!(last, Attempt::Status { status: 402, ref body } if body == "quota exhausted")
        );
        assert_eq!(server.join().unwrap().len(), 6);
    }

    /// Answers `answers` in order, each on the connection its request came on, and returns the
    /// number (from 1) of that connection for each request. A connection stays open for the next
    /// request until the client closes it, or until the server closes it the given time after the
    /// answer (as a server or a proxy that drops idle connections does).
    fn serve_pooled(
        answers: Vec<(&'static str, Option<Duration>)>,
    ) -> (String, std::thread::JoinHandle<Vec<usize>>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("http://{}/b/s", listener.local_addr().unwrap());
        let server = std::thread::spawn(move || {
            let mut accepted: usize = 0;
            let mut open: Option<TcpStream> = None;
            let mut on = Vec::new();
            for (answer, close) in answers {
                let mut conn = loop {
                    let mut conn = match open.take() {
                        Some(conn) => conn,
                        None => {
                            accepted = accepted.saturating_add(1);
                            listener.accept().unwrap().0
                        }
                    };
                    // Unless the client closed it: then the request comes on a new one.
                    if read_head(&mut conn) {
                        break conn;
                    }
                };
                conn.write_all(answer.as_bytes()).unwrap();
                on.push(accepted);
                match close {
                    Some(after) => {
                        std::thread::sleep(after);
                        drop(conn);
                    }
                    None => open = Some(conn),
                }
            }
            on
        });
        (url, server)
    }

    /// Reads a request head from `conn`: false when the client closed the connection instead.
    fn read_head(conn: &mut TcpStream) -> bool {
        let mut head = Vec::new();
        let mut byte = [0u8; 1];
        while !head.ends_with(b"\r\n\r\n") {
            match conn.read(&mut byte) {
                Ok(1) => head.push(byte[0]),
                _ => return false,
            }
        }
        true
    }

    /// An answer that leaves the connection open for the next request.
    const KEPT: &str = "HTTP/1.1 200 OK\r\ncontent-length: 0\r\n\r\n";

    // The agent keeps a connection for 90 s after its last request, not ureq's 15 s: a writer that
    // commits every 15 to 90 s reuses its connection instead of opening a new TCP and TLS
    // connection for almost every commit. Longer, a NAT or a load balancer may have dropped it
    // without telling either end. The bulk agent keeps ureq's 15 s: one snapshot's requests follow
    // each other within seconds. The pool keeps ureq's bounds.
    #[test]
    fn agents_keep_idle_connections_for_90_s_and_15_s() {
        for (agent, age) in [(agent(), 90), (bulk_agent(), 15)] {
            let config = agent.config();
            assert_eq!(config.max_idle_age(), Duration::from_secs(age));
            assert_eq!(config.max_idle_connections_per_host(), 3);
            assert_eq!(config.max_idle_connections(), 10);
        }
    }

    // Both agents resolve and connect (the TLS handshake included) within 5 s each, well below
    // their timeouts: a host that does not answer fails as `Timeout::Resolve` or `Timeout::Connect`
    // (`before_connection`), not as the whole request's timeout.
    #[test]
    fn agents_resolve_and_connect_within_5_s() {
        for agent in [agent(), bulk_agent()] {
            let timeouts = agent.config().timeouts();
            assert_eq!(timeouts.resolve, Some(Duration::from_secs(5)));
            assert_eq!(timeouts.connect, Some(Duration::from_secs(5)));
            assert!(timeouts.global > Some(Duration::from_secs(5)));
        }
    }

    // A connection idle for less than the agent's idle age carries the next request, and one idle
    // for longer is closed and replaced. 2 s here, well below ureq's 15 s, to keep the test short
    // (`agents_keep_idle_connections_for_90_s_and_15_s` pins the agents' age).
    #[test]
    fn connections_are_reused_until_their_idle_age() {
        let (url, server) = serve_pooled(vec![(KEPT, None), (KEPT, None), (KEPT, None)]);
        let agent = new_agent(Duration::from_secs(10), Duration::from_secs(2));
        let get = || agent.get(url.as_str()).call().unwrap().status().as_u16();
        assert_eq!(get(), 200);
        std::thread::sleep(Duration::from_millis(500));
        assert_eq!(get(), 200);
        std::thread::sleep(Duration::from_millis(2_500));
        assert_eq!(get(), 200);
        assert_eq!(server.join().unwrap(), [1, 1, 2]);
    }

    // ureq reads a pooled connection's socket before it reuses it: one the server closed while it
    // sat idle is dropped, and the request goes out on a new connection instead of failing. Sent
    // with a bare agent, without the retries here, so a request on the closed connection would
    // fail.
    #[test]
    fn a_connection_closed_while_idle_is_not_reused() {
        let (url, server) =
            serve_pooled(vec![(KEPT, Some(Duration::from_millis(100))), (KEPT, None)]);
        let agent = new_agent(Duration::from_secs(10), Duration::from_secs(90));
        let head = || agent.head(url.as_str()).call().unwrap().status().as_u16();
        assert_eq!(head(), 200);
        std::thread::sleep(Duration::from_millis(300));
        assert_eq!(head(), 200);
        assert_eq!(server.join().unwrap(), [1, 2]);
    }

    // A read whose pooled connection was closed just as it went out (a proxy dropping idle
    // connections, a gateway shutting down) gets no answer: it is sent again on a new connection
    // within the budget, as after a 503, instead of failing the attach. A refused connection
    // (nothing listens there) fails at once.
    #[test]
    fn a_read_that_got_no_answer_is_sent_again_on_a_new_connection() {
        let (url, server) = serve(vec![
            String::new(),
            answer("200 OK\r\nstream-incarnation: i1"),
        ]);
        let head_ok = head(&url, &|| false).unwrap();
        assert_eq!(head_ok.incarnation.as_deref(), Some("i1"));
        assert_eq!(server.join().unwrap().len(), 2);
        let started = Instant::now();
        let refused = head("http://127.0.0.1:1/b/s", &|| false).err().unwrap();
        assert!(
            matches!(&refused, Error::Http { source, .. } if matches!(**source, ureq::Error::Io(_))),
            "{refused}"
        );
        assert!(started.elapsed() < Duration::from_secs(5));
    }

    // On macOS a reset that comes in between two calls on the socket fails the next one with
    // `EINVAL` (`reset_on_macos`, #542): no answer there, as a reset is everywhere, and final on
    // other systems. Like a reset, it is no failure before the connection, so a large snapshot body
    // whose write met it may be too large (`snapshotter::FailureKind::may_be_size`). std reports
    // `EINVAL` as `InvalidInput`, as it does the `InvalidInput` it makes up for an argument it
    // refuses (a host and port that do not parse, a NUL in a host). That one carries no OS error
    // code and stays final everywhere.
    #[test]
    fn einval_from_the_os_is_no_answer_on_macos_only() {
        let einval = std::io::Error::from_raw_os_error(libc::EINVAL);
        assert_eq!(einval.kind(), ErrorKind::InvalidInput);
        let einval = ureq::Error::Io(einval);
        assert_eq!(no_answer(&einval), cfg!(target_os = "macos"));
        assert!(!before_connection(&einval));
        for reset in [libc::EPIPE, libc::ECONNRESET] {
            let reset = ureq::Error::Io(std::io::Error::from_raw_os_error(reset));
            assert!(no_answer(&reset) && !before_connection(&reset), "{reset}");
        }
        let made_up = [
            "ursula.invalid".to_socket_addrs().err().unwrap(),
            ("ursula\0.invalid", 80).to_socket_addrs().err().unwrap(),
        ];
        for e in made_up {
            assert_eq!(
                (e.kind(), e.raw_os_error()),
                (ErrorKind::InvalidInput, None),
                "{e}"
            );
            let e = ureq::Error::Io(e);
            assert!(!no_answer(&e), "{e}");
        }
    }

    /// How `serve_resetting` resets a connection: it drops it with bytes of the request unread,
    /// which resets it (an RST, on Linux and on macOS alike, as `SO_LINGER` 0 would).
    #[derive(Clone, Copy)]
    enum Reset {
        /// As the answer starts: after the request's first byte, and with the first line of an
        /// answer sent, which the client reads before it meets the reset.
        AsTheAnswerStarts,
        /// While the body goes out: once the head and `after` bytes of the body were read, as
        /// fast as they came, so the client is still writing.
        WhileTheBodyGoesOut { after: usize },
    }

    /// A stand-in that resets its first `resets` connections (`Reset`), then answers the next one
    /// with `answer`, if any, after it read the request whole. Returns the URL, and the number of
    /// connections it accepted.
    fn serve_resetting(
        reset: Reset,
        resets: usize,
        answer: Option<String>,
    ) -> (String, std::thread::JoinHandle<usize>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let url = format!("http://{}/b/s", listener.local_addr().unwrap());
        let server = std::thread::spawn(move || {
            for i in 0..resets {
                let mut conn = accepted(&listener, i);
                match reset {
                    Reset::AsTheAnswerStarts => {
                        conn.read_exact(&mut [0u8; 1]).unwrap();
                        conn.write_all(b"HTTP/1.1 200 OK\r\n").unwrap();
                    }
                    Reset::WhileTheBodyGoesOut { after } => {
                        assert!(read_head(&mut conn));
                        conn.read_exact(&mut vec![0u8; after]).unwrap();
                    }
                }
                // Bytes of the request unread: a reset.
                drop(conn);
            }
            let Some(answer) = answer else {
                return resets;
            };
            let mut conn = accepted(&listener, resets);
            assert!(read_request(&mut conn));
            conn.write_all(answer.as_bytes()).unwrap();
            resets.saturating_add(1)
        });
        (url, server)
    }

    /// Whether `e`, a reset, is reported as `EINVAL`: after asserting that it is no answer, after
    /// the connection carried the request, as a reset is whatever error reports it.
    fn met_as_einval(e: &ureq::Error) -> bool {
        assert!(no_answer(e) && !before_connection(e), "{e}");
        matches!(e, ureq::Error::Io(io) if io.raw_os_error() == Some(libc::EINVAL))
    }

    // A reset that comes in between two calls on the socket fails the next one with `EINVAL` on
    // macOS, where Linux reports `ECONNRESET` or `EPIPE` (#542). A stand-in that resets the
    // connection as a `HEAD`'s answer starts makes the client meet the reset at its read after
    // that first line, while it is between two calls: on macOS as `EINVAL` almost every time, and
    // on Linux never (there as `ECONNRESET`). Each `HEAD` is sent once, with no retry to hide the
    // error.
    #[test]
    fn a_reset_as_the_answer_starts_is_met_as_einval_on_macos_only() {
        let mut einval: usize = 0;
        for _ in 0..10 {
            let (url, server) = serve_resetting(Reset::AsTheAnswerStarts, 1, None);
            if met_as_einval(&agent().head(url.as_str()).call().unwrap_err()) {
                einval = einval.saturating_add(1);
            }
            assert_eq!(server.join().unwrap(), 1);
        }
        if cfg!(target_os = "macos") {
            assert!(einval > 0, "no reset met as EINVAL");
        } else {
            assert_eq!(einval, 0);
        }
    }

    // A reset while a large snapshot body goes out (as a gateway that stopped reading a body over
    // its limit does, #540) reaches the writer as `EINVAL` on macOS when it was between two writes
    // (about a third of the time), and otherwise, as on Linux, as `EPIPE` or `ECONNRESET`. Either
    // way it is no answer, after the connection carried part of the body, so the failure may be
    // about the size (`snapshotter::FailureKind::may_be_size`). `put_idempotent` sends the body
    // twice and keeps the last error.
    #[test]
    fn a_reset_while_a_large_body_goes_out_is_no_answer() {
        let large = vec![7u8; 16 << 20];
        for _ in 0..5 {
            let (url, server) =
                serve_resetting(Reset::WhileTheBodyGoesOut { after: 1 << 20 }, 2, None);
            let put = put_idempotent(&format!("{url}/snapshot/7"), "i1", &large, &|| false);
            let Err(Error::PutUnknown {
                last: Attempt::Transport(e),
                ..
            }) = put
            else {
                panic!("not an unknown outcome");
            };
            assert!(!met_as_einval(&e) || cfg!(target_os = "macos"), "{e}");
            assert_eq!(server.join().unwrap(), 2);
        }
    }

    // A read whose connection is reset as its answer starts is sent again on a new connection,
    // within the budget, whatever error reports the reset: on macOS it is usually `EINVAL`
    // (`a_reset_as_the_answer_starts_is_met_as_einval_on_macos_only`), which failed the attach
    // there before this fix (0.7.3 and earlier).
    #[test]
    fn a_read_reset_as_its_answer_starts_is_sent_again_on_a_new_connection() {
        let (url, server) = serve_resetting(
            Reset::AsTheAnswerStarts,
            3,
            Some(answer("200 OK\r\nstream-incarnation: i1")),
        );
        let head_ok = head(&url, &|| false).unwrap();
        assert_eq!(head_ok.incarnation.as_deref(), Some("i1"));
        assert_eq!(server.join().unwrap(), 4);
        let (url, server) = serve_resetting(
            Reset::AsTheAnswerStarts,
            3,
            Some(answer_with_body("200 OK\r\nstream-next-offset: 7", "abc")),
        );
        let (bytes, next) = read_from(&url, "i1", "4").unwrap();
        assert_eq!((&bytes[..], next.as_str()), (&b"abc"[..], "7"));
        assert_eq!(server.join().unwrap(), 4);
    }

    /// A server whose connections answer every request (200 ms later, so that two requests sent at
    /// once take two connections), until `dead` passes their number: from then on they take a
    /// request and close without an answer, as connections a device dropped without telling either
    /// end. Returns the URL, `dead`, and the connections accepted and requests received so far.
    fn serve_until_dead() -> (String, Arc<AtomicUsize>, Arc<AtomicUsize>, Arc<AtomicUsize>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("http://{}/b/s", listener.local_addr().unwrap());
        let dead = Arc::new(AtomicUsize::new(0));
        let accepted = Arc::new(AtomicUsize::new(0));
        let requests = Arc::new(AtomicUsize::new(0));
        {
            let (dead, accepted, requests) = (dead.clone(), accepted.clone(), requests.clone());
            std::thread::spawn(move || {
                for conn in listener.incoming() {
                    let mut conn = conn.unwrap();
                    let n = accepted.fetch_add(1, Ordering::SeqCst);
                    let (dead, requests) = (dead.clone(), requests.clone());
                    std::thread::spawn(move || {
                        while read_request(&mut conn) {
                            requests.fetch_add(1, Ordering::SeqCst);
                            if n < dead.load(Ordering::SeqCst) {
                                return;
                            }
                            std::thread::sleep(Duration::from_millis(200));
                            let ok = "HTTP/1.1 200 OK\r\nstream-incarnation: i1\r\n\
                                      stream-next-offset: 9\r\ncontent-length: 0\r\n\r\n";
                            conn.write_all(ok.as_bytes()).unwrap();
                        }
                    });
                }
            });
        }
        (url, dead, accepted, requests)
    }

    /// Two connections to a `serve_until_dead` server pooled by the agent `open` sends with (two
    /// requests sent at once), idle past `RETRY_IDLE_AGE` and dead. Returns the URL, and the
    /// connections accepted and requests received from then on (the count of connections goes on).
    fn two_dead_pooled_connections(open: fn(&str)) -> (String, Arc<AtomicUsize>, Arc<AtomicUsize>) {
        let (url, dead, accepted, requests) = serve_until_dead();
        let start = Arc::new(Barrier::new(2));
        let sends: Vec<_> = (0..2)
            .map(|_| {
                let (url, start) = (url.clone(), start.clone());
                std::thread::spawn(move || {
                    start.wait();
                    open(&url);
                })
            })
            .collect();
        for send in sends {
            send.join().unwrap();
        }
        assert_eq!(accepted.load(Ordering::SeqCst), 2);
        std::thread::sleep(Duration::from_millis(1_200));
        dead.store(2, Ordering::SeqCst);
        requests.store(0, Ordering::SeqCst);
        (url, accepted, requests)
    }

    // A device that drops idle connections without telling either end leaves every pooled
    // connection to the host dead (here they take a request and close without an answer). After
    // the first attempt, on the oldest one, gets no answer, the next one goes out on a new
    // connection and the other stale one is dropped, instead of trying each pooled connection in
    // turn: where nothing answers, each would cost the whole timeout, and three the whole budget.
    // So the next append goes out on the new connection at once: ureq drops a pooled connection too
    // old for a request (3.4.2 does, although its docs say it keeps it), which this pins.
    #[test]
    fn an_append_after_no_answer_goes_out_on_a_new_connection() {
        let (url, accepted, requests) = two_dead_pooled_connections(|url| {
            head(url, &|| false).unwrap();
        });
        let appended = append(&url, "i1", "p", b"x", 1, 1);
        assert!(
            matches!(appended, Append::Acked { next: Some(ref n), attempts: 2, duplicate: false } if n == "9"),
            "one attempt on the oldest pooled connection, then one on a new connection"
        );
        assert_eq!(
            (
                accepted.load(Ordering::SeqCst),
                requests.load(Ordering::SeqCst)
            ),
            (3, 2)
        );
        let next = append(&url, "i1", "p", b"x", 1, 2);
        assert!(matches!(next, Append::Acked { attempts: 1, .. }));
        assert_eq!(
            accepted.load(Ordering::SeqCst),
            3,
            "the stale connection is gone"
        );
    }

    // The same for a read: attach's first `HEAD` and its catch-up reads would otherwise try each
    // dead pooled connection in turn.
    #[test]
    fn a_read_after_no_answer_goes_out_on_a_new_connection() {
        let (url, accepted, requests) = two_dead_pooled_connections(|url| {
            head(url, &|| false).unwrap();
        });
        assert_eq!(
            head(&url, &|| false).unwrap().incarnation.as_deref(),
            Some("i1")
        );
        assert_eq!(
            (
                accepted.load(Ordering::SeqCst),
                requests.load(Ordering::SeqCst)
            ),
            (3, 2)
        );
    }

    // The same for a snapshot `PUT` or a retention move, on the snapshot agent's own pool.
    #[test]
    fn a_put_after_no_answer_goes_out_on_a_new_connection() {
        let put = |url: &str| {
            let retention = format!("{url}/retention/7");
            put_idempotent(&retention, "i1", b"", &|| false).unwrap();
        };
        let (url, accepted, requests) = two_dead_pooled_connections(put);
        put(&url);
        assert_eq!(
            (
                accepted.load(Ordering::SeqCst),
                requests.load(Ordering::SeqCst)
            ),
            (3, 2)
        );
    }

    /// Reads a request (its head, and a body of its `content-length`): false when the client
    /// closed the connection instead.
    fn read_request(conn: &mut TcpStream) -> bool {
        let mut head = Vec::new();
        let mut byte = [0u8; 1];
        while !head.ends_with(b"\r\n\r\n") {
            match conn.read(&mut byte) {
                Ok(1) => head.push(byte[0]),
                _ => return false,
            }
        }
        let head = String::from_utf8_lossy(&head).to_ascii_lowercase();
        let len = head
            .split("\r\n")
            .find_map(|line| line.strip_prefix("content-length:"))
            .map_or(0, |n| n.trim().parse().unwrap());
        conn.read_exact(&mut vec![0u8; len]).is_ok()
    }

    /// A resolver whose lookups answer from its answers in order, the last one again once they
    /// run out, and which counts them. Clones share the count.
    #[derive(Clone, Debug)]
    struct Lookups(Arc<Script>);

    #[derive(Debug)]
    struct Script {
        made: AtomicUsize,
        answers: Vec<Result<std::net::SocketAddr, ureq::Timeout>>,
    }

    impl Lookups {
        fn new(answers: Vec<Result<std::net::SocketAddr, ureq::Timeout>>) -> Self {
            Lookups(Arc::new(Script {
                made: AtomicUsize::new(0),
                answers,
            }))
        }

        fn made(&self) -> usize {
            self.0.made.load(Ordering::Relaxed)
        }
    }

    impl ureq::unversioned::resolver::Resolver for Lookups {
        fn resolve(
            &self,
            _uri: &ureq::http::Uri,
            _config: &ureq::config::Config,
            _timeout: ureq::unversioned::transport::NextTimeout,
        ) -> Result<ureq::unversioned::resolver::ResolvedSocketAddrs, ureq::Error> {
            let made = self.0.made.fetch_add(1, Ordering::Relaxed);
            let answer = self.0.answers[made.min(self.0.answers.len().saturating_sub(1))];
            let mut addrs = self.empty();
            addrs.push(answer.map_err(ureq::Error::Timeout)?);
            Ok(addrs)
        }
    }

    /// An agent like `agent()` whose hosts resolve through the cache over `lookups`.
    fn agent_over(lookups: &Lookups) -> ureq::Agent {
        new_agent_with(
            Duration::from_secs(10),
            Duration::from_secs(90),
            net::Resolver::new(lookups.clone()),
        )
    }

    /// The address `serve` listens on, from its URL.
    fn served(url: &str) -> std::net::SocketAddr {
        url.trim_start_matches("http://")
            .split('/')
            .next()
            .unwrap()
            .parse()
            .unwrap()
    }

    fn within(retry_budget: Duration) -> Instant {
        Instant::now().checked_add(retry_budget).unwrap()
    }

    // Requests to a host go out with one lookup while its addresses are fresh (`net::DNS_FRESH`),
    // where ureq looked the host up before every request, a pooled connection's included.
    #[test]
    fn requests_to_a_host_share_one_lookup() {
        let (url, server) = serve(vec![answer("200 OK"); 3]);
        let lookups = Lookups::new(vec![Ok(served(&url))]);
        let agent = agent_over(&lookups);
        for _ in 0..3 {
            let r = agent.head("http://gw.test/b/s").call().unwrap();
            assert_eq!(r.status().as_u16(), 200);
        }
        assert_eq!(server.join().unwrap().len(), 3);
        assert_eq!(lookups.made(), 1);
    }

    // A connection that fails makes its host's lookup due, no sooner than 5 s after the last
    // lookup: the next request then looks the host up again and reaches the endpoint at its new
    // address, where the cached address kept failing.
    #[test]
    fn a_failed_connection_looks_the_host_up_again() {
        let (moved_from, _refusing) = refusing();
        let (url, server) = serve(vec![answer("200 OK")]);
        let lookups = Lookups::new(vec![Ok(moved_from), Ok(served(&url))]);
        let resolver = net::Resolver::new(lookups.clone());
        let agent = new_agent_with(
            Duration::from_secs(10),
            Duration::from_secs(90),
            resolver.clone(),
        );
        let e = agent.head("http://gw.test/b/s").call().unwrap_err();
        assert!(net::is_not_sent(&e), "{e}");
        resolver.backdate(net::DNS_RETRY);
        let r = agent.head("http://gw.test/b/s").call().unwrap();
        assert_eq!(r.status().as_u16(), 200);
        assert_eq!(server.join().unwrap().len(), 1);
        assert_eq!(lookups.made(), 2);
    }

    // Through a CONNECT proxy (ureq's proxy from the environment), the tunnel goes through the
    // marking resolver and connector, and then the connector's own marking: a proxy whose host
    // does not resolve still fails as never sent, once, so the failure is no answer, and a read is
    // sent again, as before the marking.
    #[test]
    fn a_proxy_that_does_not_resolve_fails_as_never_sent_once() {
        let lookups = Lookups::new(vec![Err(ureq::Timeout::Resolve)]);
        let resolver = net::Resolver::new(lookups.clone());
        let config = ureq::Agent::config_builder()
            .proxy(Some(ureq::Proxy::new("http://proxy.test:3128").unwrap()))
            .build();
        let agent =
            ureq::Agent::with_parts(config, net::Connector::new(resolver.clone()), resolver);
        let e = agent.get("http://gw.test/b/s").call().unwrap_err();
        assert!(no_answer(&e) && before_connection(&e), "{e}");
        assert!(matches!(
            net::sent_nothing(e),
            (ureq::Error::Timeout(ureq::Timeout::Resolve), true)
        ));
        assert_eq!(lookups.made(), 1);
    }

    // ureq completes the TLS handshake in its connector, before any byte of the request: a
    // handshake that fails (an endpoint that answers plain HTTP) is marked as never sent, which
    // the proof that such a failure applied nothing relies on (`net`).
    #[test]
    fn a_failed_tls_handshake_sends_nothing() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("https://{}/b/s", listener.local_addr().unwrap());
        let server = std::thread::spawn(move || {
            use std::io::Write as _;
            let (mut conn, _) = listener.accept().unwrap();
            conn.write_all(b"HTTP/1.1 400 Bad Request\r\ncontent-length: 0\r\n\r\n")
                .unwrap();
        });
        let e = agent().get(&url).call().unwrap_err();
        server.join().unwrap();
        assert!(net::is_not_sent(&e), "{e}");
    }

    // A commit that cannot reach its endpoint, every attempt refused by the host before a
    // connection was made, applied nothing: it fails alone, with the producer sequence still free,
    // and the database is not poisoned. It used to end as an unknown outcome.
    #[test]
    fn an_append_whose_every_attempt_fails_before_sending_fails_alone() {
        let (closed, _refusing) = refusing();
        let url = format!("http://{closed}/b/s");
        let retrying = Retrying {
            agent: agent(),
            deadline: within(Duration::from_millis(500)),
        };
        let unreachable = append_on(retrying, &url, "i1", "p", b"x", 1, 1);
        assert!(
            matches!(&unreachable, Append::Refused(WriteRefusal::Unreachable { reason }) if reason.starts_with("io: ")),
            "{}",
            outcome(&unreachable)
        );
    }

    // A lookup that times out (`Timeout::Resolve`, a DNS server that does not answer) with no
    // address to stand in fails the attempt before anything went out: the commit fails alone.
    #[test]
    fn an_append_whose_lookups_time_out_fails_alone() {
        let lookups = Lookups::new(vec![Err(ureq::Timeout::Resolve)]);
        let agent = agent_over(&lookups);
        let retrying = Retrying {
            agent: &agent,
            deadline: within(Duration::from_millis(500)),
        };
        let unreachable = append_on(retrying, "http://gw.test/b/s", "i1", "p", b"x", 1, 1);
        assert!(
            matches!(&unreachable, Append::Refused(WriteRefusal::Unreachable { reason }) if reason == "timeout: resolve"),
            "{}",
            outcome(&unreachable)
        );
        // The first lookup's failure holds the next one off for `DNS_RETRY` (5 s), so the
        // budget's other attempts fail as it did, with no lookup.
        assert_eq!(lookups.made(), 1);
    }

    // An attempt that failed before it went out leaves the append's outcome as it was: after it,
    // a proven refusal still fails the commit alone, where it used to be unknown.
    #[test]
    fn a_failure_before_sending_leaves_a_later_refusal_proven() {
        let (url, server) = serve(vec![answer(&format!(
            "503 Service Unavailable\r\n{}\r\n{LAST}",
            vouched("RaftLogPressure")
        ))]);
        let lookups = Lookups::new(vec![Err(ureq::Timeout::Resolve), Ok(served(&url))]);
        let agent = agent_over(&lookups);
        let retrying = Retrying {
            agent: &agent,
            deadline: within(Duration::from_secs(30)),
        };
        let refused = append_on(retrying, "http://gw.test/b/s", "i1", "p", b"x", 1, 1);
        assert!(
            matches!(
                &refused,
                Append::Refused(WriteRefusal::NotApplied { status: 503, .. })
            ),
            "{}",
            outcome(&refused)
        );
        assert_eq!(server.join().unwrap().len(), 1);
    }

    // A failure on a connection that was made may come after the request went out: an attempt that
    // got no answer keeps the append unknown, whatever failed before sending after it.
    #[test]
    fn a_failure_after_connecting_keeps_the_append_unknown() {
        let (url, server) = serve(vec![String::new()]);
        let retrying = Retrying {
            agent: agent(),
            deadline: within(Duration::from_millis(500)),
        };
        let unknown = append_on(retrying, &url, "i1", "p", b"x", 1, 1);
        assert!(
            matches!(&unknown, Append::Failed(Error::AppendUnknown { .. })),
            "{}",
            outcome(&unknown)
        );
        assert_eq!(server.join().unwrap().len(), 1);
    }

    // A HEAD's answer has no body, so a refused HEAD fetches the server's explanation with one GET
    // of the stream, refused too: a 400 names the bucket or stream id it refuses (`invalid_name`),
    // a 403 the denial. A 404 sends none (attach creates the stream then), and a GET that
    // succeeds adds nothing.
    #[test]
    fn a_refused_head_carries_the_reason_a_get_gives() {
        let reason = "bucket_id must be 4 to 64 bytes, got 2 bytes";
        let (url, server) = serve(vec![
            answer("400 Bad Request"),
            answer_with_body("400 Bad Request", reason),
        ]);
        let refused = head(&url, &|| false).err().unwrap();
        let requests = server.join().unwrap();
        assert!(
            requests[1].starts_with("GET /b/s?offset=-1 "),
            "{}",
            requests[1]
        );
        assert!(
            matches!(&refused, Error::Status { op: "head", status: 400, body, .. } if body == reason)
        );
        assert_eq!(refused.kind(), Kind::InvalidName);
        assert!(refused.to_string().ends_with(&format!(": 400 {reason}")));

        let (url, server) = serve(vec![
            answer("403 Forbidden"),
            answer_with_body("403 Forbidden", "no access to bucket sqlite-e2e"),
        ]);
        let refused = head(&url, &|| false).err().unwrap();
        server.join().unwrap();
        assert!(
            matches!(&refused, Error::Unauthorized { status: 403, body, .. } if body == "no access to bucket sqlite-e2e")
        );
        assert_eq!(refused.kind(), Kind::Unauthorized);

        let (url, server) = serve(vec![answer("404 Not Found")]);
        let missing = head(&url, &|| false).err().unwrap();
        assert_eq!(server.join().unwrap().len(), 1);
        assert!(matches!(&missing, Error::Status { status: 404, body, .. } if body.is_empty()));

        let (url, server) = serve(vec![
            answer("403 Forbidden"),
            answer_with_body("200 OK", "stream bytes"),
        ]);
        let refused = head(&url, &|| false).err().unwrap();
        server.join().unwrap();
        assert!(
            matches!(&refused, Error::Unauthorized { status: 403, body, .. } if body.is_empty())
        );
    }

    fn outcome(append: &Append) -> String {
        match append {
            Append::Refused(refusal) => format!("refused: {} ({})", refusal.kind(), refusal),
            Append::Failed(e) => format!("failed: {e}"),
            Append::Acked { .. } => "acked".to_owned(),
            _ => "another outcome".to_owned(),
        }
    }
}

//! The network under the HTTP transport's agents: a cache of the hosts' addresses, and the failures
//! that prove a request never went out.
//!
//! ureq resolves a request's host before it looks for a pooled connection (ureq 3.4.2,
//! `run::connect`), so without the cache every request, on a pooled connection too, asked the
//! system's resolver. In Kubernetes (`ndots:5`, a short name) that is several DNS queries per
//! request: 128 owners sent CoreDNS about 1,300 queries a second, and a lookup that stalled failed
//! its request after the connect timeout (`transport::http::new_agent`) with a pooled connection ready. With
//! the cache, a host is looked up at most once every [`DNS_FRESH`] (every [`DNS_RETRY`] while
//! lookups fail), by one request at a time: while it waits for the lookup, up to the resolve
//! timeout (5 s), the host's other requests go on with its last addresses, or fail as its last
//! lookup did when that left none (`settle`). Only the first requests to a host, before a lookup
//! of it ended, look it up each. A connection that fails makes its host's lookup due, no sooner
//! than [`DNS_RETRY`] after the last one ended, so an endpoint that moved is looked up again at the
//! next attempt.
//!
//! The resolver and the connector here wrap each failure of theirs in [`NotSent`]: a host that did
//! not resolve, and a connection that was not made. Nothing of such a request went out, so an
//! request that failed this way applied nothing (`wire::NoAnswer::NotSent`). A failure on a
//! connection that was made, a pooled one included, is never wrapped: part of the request, or all
//! of it, may have gone out. The wrapping is by where the failure happened, not by its kind: the
//! same I/O error kinds (a reset, `InvalidData` from rustls, an unreachable host) also come from
//! connections that carried a request.
//!
//! That proof relies on how ureq 3.4.2 sends a request as these agents use it (no redirects
//! followed, no middleware): it resolves the host and makes the connection (the TCP connect, a
//! CONNECT proxy's tunnel, and the TLS handshake, which ureq's rustls connector completes) before
//! it writes any byte of the request, it sends a request once and never again on another
//! connection, and it sends no TLS early data.
//! These use ureq's
//! `unversioned` resolver and transport types, which its semver promises do not cover: an update
//! of ureq that changes them fails the build here.

use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::OnceLock;
use std::sync::PoisonError;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::time::Instant;

use ureq::config::Config;
use ureq::http::Uri;
use ureq::unversioned::resolver::DefaultResolver;
use ureq::unversioned::resolver::ResolvedSocketAddrs;
use ureq::unversioned::transport::ConnectionDetails;
use ureq::unversioned::transport::DefaultConnector;
use ureq::unversioned::transport::NextTimeout;
use ureq::unversioned::transport::Transport;

use crate::log;
use crate::log::Level;

/// How long a host's addresses serve requests after a lookup, without another one.
pub(crate) const DNS_FRESH: Duration = Duration::from_secs(30);

/// How long after a host's last successful lookup its addresses still serve requests while new
/// lookups fail. Past it, a request whose lookup fails fails with it. A host no request needed for
/// that long still uses them while one request looks it up again, rather than every request
/// looking it up.
pub(crate) const DNS_STALE_LIMIT: Duration = Duration::from_secs(600);

/// How long after a failed lookup the host's last addresses serve requests without another
/// lookup.
pub(crate) const DNS_RETRY: Duration = Duration::from_secs(5);

/// A failure of a request before any byte of it went out (see the module): its host did not
/// resolve, or no connection to it was made. It shows as the failure it wraps.
#[derive(Debug, thiserror::Error)]
#[error(transparent)]
pub(crate) struct NotSent(ureq::Error);

/// `e` wrapped in [`NotSent`], once: a CONNECT proxy's tunnel is made through the same resolver
/// and connector, whose failure the connector would otherwise wrap again.
fn not_sent(e: ureq::Error) -> ureq::Error {
    if is_not_sent(&e) {
        return e;
    }
    ureq::Error::Other(Box::new(NotSent(e)))
}

/// The failure as ureq reported it, without the [`NotSent`] wrapping, and whether it proves that
/// nothing of the request went out.
pub(crate) fn sent_nothing(e: ureq::Error) -> (ureq::Error, bool) {
    match e {
        ureq::Error::Other(other) => match other.downcast::<NotSent>() {
            Ok(not_sent) => (not_sent.0, true),
            Err(other) => (ureq::Error::Other(other), false),
        },
        e => (e, false),
    }
}

/// The failure as ureq reported it, without the [`NotSent`] wrapping.
pub(crate) fn unwrapped(e: &ureq::Error) -> &ureq::Error {
    match e {
        ureq::Error::Other(other) => other
            .downcast_ref::<NotSent>()
            .map_or(e, |not_sent| &not_sent.0),
        e => e,
    }
}

/// Whether `e` proves that nothing of the request went out (see the module).
pub(crate) fn is_not_sent(e: &ureq::Error) -> bool {
    matches!(e, ureq::Error::Other(other) if other.is::<NotSent>())
}

/// A host's last successful lookup: its addresses, and when the lookup ended.
#[derive(Clone, Debug)]
struct Good {
    addrs: ResolvedSocketAddrs,
    at: Instant,
}

/// How a host's last lookup failed, kept to fail the host's requests the same way until the next
/// lookup (a ureq error is not `Clone`): the same variant and text, a timeout's kind (a CONNECT
/// proxy's host is looked up within the connect timeout), and an I/O error's kind with its OS error
/// code, or with its message (`getaddrinfo`'s failures are `Uncategorized` with one).
#[derive(Clone, Debug)]
enum Failure {
    TimedOut(ureq::Timeout),
    NotFound,
    BadUri(String),
    Os(i32),
    Io(std::io::ErrorKind, String),
}

impl Failure {
    /// How `e` failed, unless the cache cannot make the same failure again (no failure of ureq's
    /// own resolver), which is then not kept: the host's next request looks it up again.
    fn of(e: &ureq::Error) -> Option<Failure> {
        match unwrapped(e) {
            ureq::Error::Timeout(timeout) => Some(Failure::TimedOut(*timeout)),
            ureq::Error::HostNotFound => Some(Failure::NotFound),
            ureq::Error::BadUri(uri) => Some(Failure::BadUri(uri.clone())),
            ureq::Error::Io(e) => Some(match e.raw_os_error() {
                Some(code) => Failure::Os(code),
                None => Failure::Io(e.kind(), e.to_string()),
            }),
            _ => None,
        }
    }

    /// The failure again.
    fn error(&self) -> ureq::Error {
        match self {
            Failure::TimedOut(timeout) => ureq::Error::Timeout(*timeout),
            Failure::NotFound => ureq::Error::HostNotFound,
            Failure::BadUri(uri) => ureq::Error::BadUri(uri.clone()),
            Failure::Os(code) => ureq::Error::Io(std::io::Error::from_raw_os_error(*code)),
            Failure::Io(kind, text) => ureq::Error::Io(std::io::Error::new(*kind, text.clone())),
        }
    }
}

/// What the cache knows of a host.
#[derive(Clone, Debug)]
struct Entry {
    /// The last successful lookup.
    good: Option<Good>,
    /// When the last lookup ended, successful or not.
    ended: Instant,
    /// How the last lookup failed, when it did (and the cache keeps the failure, `Failure::of`).
    failed: Option<Failure>,
    /// When the next lookup is due.
    next: Instant,
    /// The lookup a request runs now, by its number.
    looking: Option<u64>,
}

impl Entry {
    /// The last successful lookup's addresses, while they stand in for a lookup at `now`: within
    /// [`DNS_STALE_LIMIT`] of it.
    fn stand_in(&self, now: Instant) -> Option<&Good> {
        self.good
            .as_ref()
            .filter(|good| now.saturating_duration_since(good.at) < DNS_STALE_LIMIT)
    }
}

/// What a request does about its host's addresses.
#[derive(Debug)]
#[expect(
    clippy::large_enum_variant,
    reason = "a plan lives from `plan` to its match in `resolve`, and boxing the addresses would \
              allocate for every request the cache serves"
)]
enum Plan {
    /// Uses them: they are fresh, or they stand in for a failed lookup until the next is due, or
    /// while another request looks the host up, past the stale limit too when no lookup since the
    /// last successful one failed.
    Use(ResolvedSocketAddrs),
    /// Fails as the last lookup did, which left no addresses to stand in, until the next lookup is
    /// due, or while another request looks the host up.
    Fail(Failure),
    /// Looks the host up.
    LookUp,
}

/// The plan of a request at `now`, given what the cache knows of its host, which the request
/// marks as being looked up (as lookup `id`) when it is to look the host up.
fn plan(entry: &mut Entry, now: Instant, id: u64) -> Plan {
    let waits = now < entry.next || entry.looking.is_some();
    match (entry.stand_in(now), &entry.failed, &entry.good) {
        (Some(good), _, _) if waits => Plan::Use(good.addrs.clone()),
        (None, Some(failure), _) if waits => Plan::Fail(failure.clone()),
        // Past the stale limit with no failure kept: no request needed the host that long.
        (None, None, Some(good)) if entry.looking.is_some() => Plan::Use(good.addrs.clone()),
        _ => {
            entry.looking = Some(id);
            Plan::LookUp
        }
    }
}

/// What a request gets from its lookup ([`settle`]).
#[derive(Debug)]
enum Settled {
    /// Addresses: its lookup's, or those of a lookup that succeeded while it ran.
    Found(ResolvedSocketAddrs),
    /// The host's last addresses, standing in for its lookup, which failed so.
    Stale(Good, ureq::Error),
    /// Its lookup's failure, with no addresses to stand in.
    Failed(ureq::Error),
}

/// Settles in `entry` a request's lookup of the host, which started at `started` and ended at
/// `ended` with `looked`, and says what the request gets. `entry` is what the cache knows of the
/// host at `ended`: other requests' lookups may have settled since `started`, but none after
/// `ended` (`resolve` reads it under the cache's lock).
///
/// A success is the host's newest addresses, due again after [`DNS_FRESH`]. A failure gives way
/// to a success that ended since `started`: the request gets its addresses, and the entry stays as
/// that success left it, but for the next lookup, which a failed connection may have made due
/// since: no sooner than [`DNS_RETRY`] after `ended`. Otherwise the failure is kept, and the
/// addresses that stand in at `ended`
/// serve the request, with the next lookup due after [`DNS_RETRY`], or at their stale limit if that
/// comes first. With none, the request fails, and so do the host's requests until the next lookup,
/// after [`DNS_RETRY`]. Either way, the next lookup is due after `ended`.
fn settle(
    entry: &mut Entry,
    started: Instant,
    ended: Instant,
    looked: Result<ResolvedSocketAddrs, ureq::Error>,
) -> Settled {
    entry.ended = ended;
    let e = match looked {
        Ok(addrs) => {
            entry.good = Some(Good {
                addrs: addrs.clone(),
                at: ended,
            });
            entry.failed = None;
            entry.next = later(ended, DNS_FRESH);
            return Settled::Found(addrs);
        }
        Err(e) => e,
    };
    if let Some(good) = entry.good.as_ref().filter(|good| good.at >= started) {
        entry.next = entry.next.max(later(ended, DNS_RETRY));
        return Settled::Found(good.addrs.clone());
    }
    entry.failed = Failure::of(&e);
    match entry.stand_in(ended).cloned() {
        Some(good) => {
            entry.next = retry_at(&good, ended);
            Settled::Stale(good, e)
        }
        None => {
            entry.next = later(ended, DNS_RETRY);
            Settled::Failed(e)
        }
    }
}

/// `at` moved `by` into the future, or `at` if that cannot be told.
fn later(at: Instant, by: Duration) -> Instant {
    at.checked_add(by).unwrap_or(at)
}

/// When the next lookup is due after one that failed at `ended` with `good` standing in: after
/// [`DNS_RETRY`], or when `good` passes the stale limit if that comes first, which is after
/// `ended` since `good` stands in then.
fn retry_at(good: &Good, ended: Instant) -> Instant {
    later(ended, DNS_RETRY).min(later(good.at, DNS_STALE_LIMIT))
}

/// When the next lookup is due after a connection to the host's addresses failed at `now`: at
/// once, but no sooner than [`DNS_RETRY`] after the last lookup ended, and no later than it was.
fn due_at(entry: &Entry, now: Instant) -> Instant {
    entry.next.min(now.max(later(entry.ended, DNS_RETRY)))
}

/// The agents' resolver: ureq's own, behind a cache of each host's addresses ([`DNS_FRESH`]),
/// whose last addresses also stand in for failed lookups ([`DNS_STALE_LIMIT`], [`DNS_RETRY`]), with
/// one lookup at a time per host (see the module). A lookup that failed with none to stand in fails
/// the host's requests the same way until the next one, after [`DNS_RETRY`], ends. An IP address
/// needs no lookup and is never cached. Every failure it returns is [`NotSent`]. Clones share one
/// cache.
#[derive(Debug)]
pub(crate) struct Resolver<R = DefaultResolver>(Arc<Cache<R>>);

#[derive(Debug)]
struct Cache<R> {
    inner: R,
    /// What the cache knows of each host, by host and port.
    hosts: Mutex<HashMap<String, Entry>>,
    /// Numbers the lookups.
    lookups: AtomicU64,
}

impl<R> Clone for Resolver<R> {
    fn clone(&self) -> Self {
        Resolver(Arc::clone(&self.0))
    }
}

impl Resolver {
    /// The resolver both agents share, so that a host looked up for one serves the other.
    pub(crate) fn shared() -> Resolver {
        static R: OnceLock<Resolver> = OnceLock::new();
        R.get_or_init(|| Resolver::new(DefaultResolver::default()))
            .clone()
    }
}

impl<R> Resolver<R> {
    pub(crate) fn new(inner: R) -> Self {
        Resolver(Arc::new(Cache {
            inner,
            hosts: Mutex::new(HashMap::new()),
            lookups: AtomicU64::new(0),
        }))
    }

    fn hosts(&self) -> std::sync::MutexGuard<'_, HashMap<String, Entry>> {
        self.0.hosts.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Moves every host's lookups `by` into the past, for tests of what follows a lookup's retry.
    #[cfg(test)]
    pub(crate) fn backdate(&self, by: Duration) {
        let back = |at: Instant| at.checked_sub(by).unwrap_or(at);
        for entry in self.hosts().values_mut() {
            entry.ended = back(entry.ended);
            entry.next = back(entry.next);
            if let Some(good) = entry.good.as_mut() {
                good.at = back(good.at);
            }
        }
    }

    /// Makes the lookup of `uri`'s host due (`due_at`) and keeps its addresses: a connection to
    /// them failed, so the endpoint may have moved.
    fn due(&self, uri: &Uri) {
        let Some(host) = host(uri) else {
            return;
        };
        if let Some(entry) = self.hosts().get_mut(&host) {
            entry.next = due_at(entry, Instant::now());
        }
    }
}

/// A request's lookup of `host`, as lookup `id`: ends the mark in the cache however the lookup
/// ends, unless another request looks the host up since.
struct Looking<'a, R> {
    resolver: &'a Resolver<R>,
    host: &'a str,
    id: u64,
}

impl<R> Drop for Looking<'_, R> {
    fn drop(&mut self) {
        if let Some(entry) = self.resolver.hosts().get_mut(self.host)
            && entry.looking == Some(self.id)
        {
            entry.looking = None;
        }
    }
}

/// The host and port `uri` connects to, unless its host is an IP address, which needs no lookup.
fn host(uri: &Uri) -> Option<String> {
    let scheme = uri.scheme()?;
    let authority = uri.authority()?;
    if authority
        .host()
        .trim_matches(['[', ']'])
        .parse::<IpAddr>()
        .is_ok()
    {
        return None;
    }
    DefaultResolver::host_and_port(scheme, authority)
}

impl<R: ureq::unversioned::resolver::Resolver> ureq::unversioned::resolver::Resolver
    for Resolver<R>
{
    fn resolve(
        &self,
        uri: &Uri,
        config: &Config,
        timeout: NextTimeout,
    ) -> Result<ResolvedSocketAddrs, ureq::Error> {
        let Some(host) = host(uri) else {
            return self.0.inner.resolve(uri, config, timeout).map_err(not_sent);
        };
        let started = Instant::now();
        let id = self.0.lookups.fetch_add(1, Ordering::Relaxed);
        {
            let mut hosts = self.hosts();
            let entry = hosts.entry(host.clone()).or_insert(Entry {
                good: None,
                ended: started,
                failed: None,
                next: started,
                looking: None,
            });
            match plan(entry, started, id) {
                Plan::Use(addrs) => return Ok(addrs),
                Plan::Fail(failure) => return Err(not_sent(failure.error())),
                Plan::LookUp => {}
            }
        }
        let _looking = Looking {
            resolver: self,
            host: &host,
            id,
        };
        let looked = self.0.inner.resolve(uri, config, timeout);
        let mut hosts = self.hosts();
        let ended = Instant::now();
        let Some(entry) = hosts.get_mut(&host) else {
            return looked.map_err(not_sent);
        };
        let settled = settle(entry, started, ended, looked);
        drop(hosts);
        match settled {
            Settled::Found(addrs) => Ok(addrs),
            Settled::Stale(good, e) => {
                log::emit(Level::Warn, "dns_stale", &[
                    ("host", &host),
                    (
                        "age_ms",
                        &ended.saturating_duration_since(good.at).as_millis(),
                    ),
                    ("reason", &e),
                ]);
                Ok(good.addrs)
            }
            Settled::Failed(e) => Err(not_sent(e)),
        }
    }
}

/// The agents' connector: ureq's own (a CONNECT proxy's tunnel, TCP, then TLS for `https://`),
/// whose every failure is [`NotSent`]: no connection was made, so nothing of the request went
/// out. A failure also makes the lookup of the request's host due ([`Resolver`]).
#[derive(Debug)]
pub(crate) struct Connector<R = DefaultResolver> {
    inner: DefaultConnector,
    resolver: Resolver<R>,
}

impl<R> Connector<R> {
    /// A connector that makes `resolver`'s lookups due when a connection fails.
    pub(crate) fn new(resolver: Resolver<R>) -> Self {
        Connector {
            inner: DefaultConnector::default(),
            resolver,
        }
    }
}

impl<R: std::fmt::Debug + Send + Sync + 'static> ureq::unversioned::transport::Connector
    for Connector<R>
{
    type Out = Box<dyn Transport>;

    fn connect(
        &self,
        details: &ConnectionDetails,
        chained: Option<()>,
    ) -> Result<Option<Self::Out>, ureq::Error> {
        let e = match self.inner.connect(details, chained) {
            Ok(Some(transport)) => return Ok(Some(transport)),
            Ok(None) => ureq::Error::ConnectionFailed,
            Err(e) => e,
        };
        self.resolver.due(details.uri);
        Err(not_sent(e))
    }
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;
    use std::sync::Mutex;
    use std::sync::PoisonError;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::sync::mpsc;
    use std::time::Duration;
    use std::time::Instant;

    use ureq::config::Config;
    use ureq::http::Uri;
    use ureq::unversioned::resolver::DefaultResolver;
    use ureq::unversioned::resolver::ResolvedSocketAddrs;
    use ureq::unversioned::resolver::Resolver as _;
    use ureq::unversioned::transport::NextTimeout;

    use super::DNS_FRESH;
    use super::DNS_RETRY;
    use super::DNS_STALE_LIMIT;
    use super::Entry;
    use super::Failure;
    use super::Good;
    use super::Resolver;
    use super::Settled;
    use super::is_not_sent;
    use super::not_sent;
    use super::sent_nothing;
    use super::settle;
    use super::unwrapped;

    const URL: &str = "http://gw.test/b/s";
    const HOST: &str = "gw.test:80";

    /// How long a test waits for a lookup it expects to start: it fails, rather than hangs, when
    /// the lookup does not.
    const STARTS: Duration = Duration::from_secs(10);

    /// One lookup's answer: addresses, a failure, or addresses once the test lets the lookup end
    /// (`gated`).
    #[derive(Debug)]
    enum Answer {
        Addr(SocketAddr),
        Fail(ureq::Error),
        Gated {
            addr: SocketAddr,
            started: mpsc::Sender<()>,
            end: mpsc::Receiver<()>,
        },
    }

    /// An answer of `addr` whose lookup says on the first receiver that it started, and ends when
    /// the sender sends.
    fn gated(addr: SocketAddr) -> (Answer, mpsc::Receiver<()>, mpsc::Sender<()>) {
        let (started, starts) = mpsc::channel();
        let (ends, end) = mpsc::channel();
        (Answer::Gated { addr, started, end }, starts, ends)
    }

    /// A resolver whose lookups answer from its answers in order, and which counts them.
    #[derive(Debug)]
    struct Scripted {
        lookups: AtomicUsize,
        answers: Mutex<Vec<Answer>>,
    }

    impl Scripted {
        fn new(answers: Vec<Answer>) -> Self {
            Scripted {
                lookups: AtomicUsize::new(0),
                answers: Mutex::new(answers),
            }
        }
    }

    impl ureq::unversioned::resolver::Resolver for Scripted {
        fn resolve(
            &self,
            _uri: &Uri,
            _config: &Config,
            _timeout: NextTimeout,
        ) -> Result<ResolvedSocketAddrs, ureq::Error> {
            self.lookups.fetch_add(1, Ordering::Relaxed);
            let answer = self
                .answers
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .remove(0);
            let addr = match answer {
                Answer::Addr(addr) => addr,
                Answer::Fail(e) => return Err(e),
                Answer::Gated { addr, started, end } => {
                    started
                        .send(())
                        .map_err(|_closed| ureq::Error::ConnectionFailed)?;
                    end.recv()
                        .map_err(|_closed| ureq::Error::ConnectionFailed)?;
                    addr
                }
            };
            let mut addrs = self.empty();
            addrs.push(addr);
            Ok(addrs)
        }
    }

    fn lookups(resolver: &Resolver<Scripted>) -> usize {
        resolver.0.inner.lookups.load(Ordering::Relaxed)
    }

    fn resolve(resolver: &Resolver<Scripted>, uri: &str) -> Result<SocketAddr, ureq::Error> {
        let uri: Uri = uri
            .parse()
            .map_err(|e: ureq::http::uri::InvalidUri| ureq::Error::BadUri(e.to_string()))?;
        let timeout = NextTimeout {
            after: Duration::from_secs(5).into(),
            reason: ureq::Timeout::Resolve,
        };
        resolver
            .resolve(&uri, &Config::default(), timeout)
            .map(|addrs| addrs[0])
    }

    /// A request resolving [`URL`] in a thread of its own.
    fn in_thread(
        resolver: &Resolver<Scripted>,
    ) -> std::thread::JoinHandle<Result<SocketAddr, ureq::Error>> {
        let resolver = resolver.clone();
        std::thread::spawn(move || resolve(&resolver, URL))
    }

    fn entry(resolver: &Resolver<Scripted>) -> Entry {
        resolver.hosts()[HOST].clone()
    }

    /// Moves the host's last lookup `ago` into the past, and its next one due now.
    fn age(resolver: &Resolver<Scripted>, ago: Duration) {
        let mut hosts = resolver.hosts();
        let entry = hosts.get_mut(HOST).unwrap();
        let at = Instant::now().checked_sub(ago).unwrap();
        entry.good.as_mut().unwrap().at = at;
        entry.ended = at;
        entry.next = Instant::now();
    }

    /// Makes the host's next lookup due now, and nothing else.
    fn due_now(resolver: &Resolver<Scripted>) {
        resolver.hosts().get_mut(HOST).unwrap().next = Instant::now();
    }

    fn addr(port: u16) -> SocketAddr {
        SocketAddr::from(([10, 0, 0, 1], port))
    }

    // Requests to a host within 30 s of its lookup make no other.
    #[test]
    fn requests_within_30_s_of_a_lookup_make_none() {
        let resolver = Resolver::new(Scripted::new(vec![
            Answer::Addr(addr(80)),
            Answer::Addr(addr(81)),
        ]));
        for _ in 0..3 {
            assert_eq!(resolve(&resolver, URL).unwrap(), addr(80));
        }
        assert_eq!(lookups(&resolver), 1);
        age(&resolver, DNS_FRESH);
        assert_eq!(resolve(&resolver, URL).unwrap(), addr(81));
        assert_eq!(lookups(&resolver), 2);
    }

    // While one request looks a host up, the host's other requests use its last addresses instead
    // of looking it up too.
    #[test]
    fn one_request_at_a_time_looks_a_host_up() {
        let (lookup, started, end) = gated(addr(81));
        let resolver = Resolver::new(Scripted::new(vec![
            Answer::Addr(addr(80)),
            lookup,
            Answer::Addr(addr(99)),
        ]));
        assert_eq!(resolve(&resolver, URL).unwrap(), addr(80));
        age(&resolver, DNS_FRESH);
        let looking = in_thread(&resolver);
        started.recv_timeout(STARTS).unwrap();
        assert_eq!(resolve(&resolver, URL).unwrap(), addr(80));
        assert_eq!(lookups(&resolver), 2);
        end.send(()).unwrap();
        assert_eq!(looking.join().unwrap().unwrap(), addr(81));
        assert_eq!(resolve(&resolver, URL).unwrap(), addr(81));
        assert_eq!(lookups(&resolver), 2);
    }

    // Requests that find no addresses look the host up each. When one of their lookups ends while
    // another runs, the host stays marked as looked up by the other, so a request when the next
    // lookup is due uses the addresses found instead of looking it up a third time.
    #[test]
    fn a_lookup_that_ends_leaves_the_host_marked_while_another_runs() {
        let (first, first_started, first_end) = gated(addr(81));
        let (second, second_started, second_end) = gated(addr(82));
        let resolver = Resolver::new(Scripted::new(vec![first, second, Answer::Addr(addr(99))]));
        let first_request = in_thread(&resolver);
        first_started.recv_timeout(STARTS).unwrap();
        let second_request = in_thread(&resolver);
        second_started.recv_timeout(STARTS).unwrap();
        first_end.send(()).unwrap();
        assert_eq!(first_request.join().unwrap().unwrap(), addr(81));
        due_now(&resolver);
        assert_eq!(resolve(&resolver, URL).unwrap(), addr(81));
        assert_eq!(lookups(&resolver), 2);
        second_end.send(()).unwrap();
        assert_eq!(second_request.join().unwrap().unwrap(), addr(82));
        due_now(&resolver);
        assert_eq!(resolve(&resolver, URL).unwrap(), addr(99));
        assert_eq!(lookups(&resolver), 3);
    }

    // A lookup that fails while the host's last addresses stand in serves them, with no other
    // lookup until the retry is due, and then the host is looked up again.
    #[test]
    fn a_failed_lookup_uses_the_last_addresses_until_the_retry() {
        let resolver = Resolver::new(Scripted::new(vec![
            Answer::Addr(addr(80)),
            Answer::Fail(ureq::Error::Timeout(ureq::Timeout::Resolve)),
            Answer::Addr(addr(81)),
        ]));
        assert_eq!(resolve(&resolver, URL).unwrap(), addr(80));
        age(&resolver, DNS_FRESH);
        assert_eq!(resolve(&resolver, URL).unwrap(), addr(80));
        assert_eq!(resolve(&resolver, URL).unwrap(), addr(80));
        assert_eq!(lookups(&resolver), 2);
        let next = entry(&resolver).next;
        assert!(next > Instant::now() && next <= Instant::now().checked_add(DNS_RETRY).unwrap());
        due_now(&resolver);
        assert_eq!(resolve(&resolver, URL).unwrap(), addr(81));
        assert_eq!(lookups(&resolver), 3);
    }

    // Near the stale limit, a failed lookup holds the next one off no longer than the limit.
    #[test]
    fn a_failed_lookup_near_the_stale_limit_holds_the_next_off_no_longer() {
        let resolver = Resolver::new(Scripted::new(vec![
            Answer::Addr(addr(80)),
            Answer::Fail(ureq::Error::Timeout(ureq::Timeout::Resolve)),
        ]));
        assert_eq!(resolve(&resolver, URL).unwrap(), addr(80));
        age(
            &resolver,
            DNS_STALE_LIMIT.saturating_sub(Duration::from_secs(1)),
        );
        assert_eq!(resolve(&resolver, URL).unwrap(), addr(80));
        let entry = entry(&resolver);
        let limit = entry
            .good
            .as_ref()
            .unwrap()
            .at
            .checked_add(DNS_STALE_LIMIT)
            .unwrap();
        assert!(entry.next <= limit, "{:?} past {:?}", entry.next, limit);
    }

    // Past the stale limit, a failed lookup fails as never sent.
    #[test]
    fn a_failed_lookup_past_the_stale_limit_fails_as_never_sent() {
        let resolver = Resolver::new(Scripted::new(vec![
            Answer::Addr(addr(80)),
            Answer::Fail(ureq::Error::HostNotFound),
        ]));
        assert_eq!(resolve(&resolver, URL).unwrap(), addr(80));
        age(&resolver, DNS_STALE_LIMIT);
        let e = resolve(&resolver, URL).unwrap_err();
        assert!(matches!(sent_nothing(e), (ureq::Error::HostNotFound, true)));
    }

    // Past the stale limit, with no failure since the last successful lookup (no request needed the
    // host that long), the last addresses serve while one request looks the host up again, rather
    // than every request looking it up.
    #[test]
    fn past_the_stale_limit_one_request_at_a_time_looks_a_host_up() {
        let (lookup, started, end) = gated(addr(81));
        let resolver = Resolver::new(Scripted::new(vec![
            Answer::Addr(addr(80)),
            lookup,
            Answer::Addr(addr(99)),
        ]));
        assert_eq!(resolve(&resolver, URL).unwrap(), addr(80));
        age(&resolver, DNS_STALE_LIMIT);
        let looking = in_thread(&resolver);
        started.recv_timeout(STARTS).unwrap();
        assert_eq!(resolve(&resolver, URL).unwrap(), addr(80));
        assert_eq!(lookups(&resolver), 2);
        end.send(()).unwrap();
        assert_eq!(looking.join().unwrap().unwrap(), addr(81));
        assert_eq!(resolve(&resolver, URL).unwrap(), addr(81));
        assert_eq!(lookups(&resolver), 2);
    }

    // A lookup that fails with no addresses to stand in fails the host's requests as it did until
    // the next lookup is due, with no lookup of theirs.
    #[test]
    fn a_failed_lookup_with_no_addresses_fails_the_host_until_the_retry() {
        let resolver = Resolver::new(Scripted::new(vec![
            Answer::Fail(ureq::Error::Timeout(ureq::Timeout::Resolve)),
            Answer::Fail(ureq::Error::HostNotFound),
            Answer::Addr(addr(80)),
        ]));
        for _ in 0..2 {
            let e = resolve(&resolver, URL).unwrap_err();
            assert!(is_not_sent(&e), "{e}");
            assert!(matches!(
                sent_nothing(e),
                (ureq::Error::Timeout(ureq::Timeout::Resolve), true)
            ));
        }
        assert_eq!(lookups(&resolver), 1);
        due_now(&resolver);
        let e = resolve(&resolver, URL).unwrap_err();
        assert!(matches!(sent_nothing(e), (ureq::Error::HostNotFound, true)));
        due_now(&resolver);
        assert_eq!(resolve(&resolver, URL).unwrap(), addr(80));
        assert_eq!(lookups(&resolver), 3);
    }

    // A failed connection makes the host's lookup due, but no sooner than 5 s after the last
    // lookup ended (a lookup's end, not the first request's, starts those 5 s), and keeps its
    // addresses to stand in for a lookup that fails.
    #[test]
    fn a_failed_connection_makes_the_lookup_due_after_the_retry_and_keeps_the_addresses() {
        let resolver = Resolver::new(Scripted::new(vec![
            Answer::Addr(addr(80)),
            Answer::Addr(addr(81)),
            Answer::Fail(ureq::Error::Timeout(ureq::Timeout::Resolve)),
        ]));
        let uri: Uri = URL.parse().unwrap();
        assert_eq!(resolve(&resolver, URL).unwrap(), addr(80));
        age(&resolver, DNS_FRESH);
        assert_eq!(resolve(&resolver, URL).unwrap(), addr(81));
        resolver.due(&uri);
        assert_eq!(resolve(&resolver, URL).unwrap(), addr(81));
        assert_eq!(lookups(&resolver), 2);
        resolver.backdate(DNS_RETRY);
        resolver.due(&uri);
        assert_eq!(resolve(&resolver, URL).unwrap(), addr(81));
        assert_eq!(lookups(&resolver), 3);
    }

    // With no addresses to stand in, while one request looks the host up again after a failed
    // lookup, the host's other requests fail as that lookup did, with no lookup of theirs.
    #[test]
    fn one_request_at_a_time_looks_up_a_host_with_no_addresses() {
        let (lookup, started, end) = gated(addr(81));
        let resolver = Resolver::new(Scripted::new(vec![
            Answer::Fail(ureq::Error::HostNotFound),
            lookup,
            Answer::Addr(addr(99)),
        ]));
        let e = resolve(&resolver, URL).unwrap_err();
        assert!(matches!(sent_nothing(e), (ureq::Error::HostNotFound, true)));
        due_now(&resolver);
        let looking = in_thread(&resolver);
        started.recv_timeout(STARTS).unwrap();
        let e = resolve(&resolver, URL).unwrap_err();
        assert!(matches!(sent_nothing(e), (ureq::Error::HostNotFound, true)));
        assert_eq!(lookups(&resolver), 2);
        end.send(()).unwrap();
        assert_eq!(looking.join().unwrap().unwrap(), addr(81));
        assert_eq!(resolve(&resolver, URL).unwrap(), addr(81));
        assert_eq!(lookups(&resolver), 2);
    }

    // A failure the cache repeats, until the next lookup, prints as the lookup's own did, and is
    // the same error: an I/O error's message (getaddrinfo's failures are `Uncategorized` with one)
    // or OS error code, and a timeout's kind (a CONNECT proxy's host is looked up within the
    // connect timeout).
    #[test]
    fn a_kept_failure_prints_as_the_lookup_did() {
        // No system uses this OS error code, so std takes its kind as `Uncategorized`.
        let uncategorized = std::io::Error::from_raw_os_error(9999).kind();
        let failures: [fn(std::io::ErrorKind) -> ureq::Error; 5] = [
            |kind| {
                ureq::Error::Io(std::io::Error::new(
                    kind,
                    "failed to lookup address information: Name or service not known",
                ))
            },
            |_| ureq::Error::Io(std::io::Error::from_raw_os_error(libc::ECONNREFUSED)),
            |_| ureq::Error::Timeout(ureq::Timeout::Connect),
            |_| ureq::Error::HostNotFound,
            |_| ureq::Error::BadUri("gw.test is missing scheme".to_string()),
        ];
        for failure in failures {
            let resolver = Resolver::new(Scripted::new(vec![Answer::Fail(failure(uncategorized))]));
            let (first, sent) = sent_nothing(resolve(&resolver, URL).unwrap_err());
            assert!(sent);
            let (kept, sent) = sent_nothing(resolve(&resolver, URL).unwrap_err());
            assert!(sent);
            assert_eq!(lookups(&resolver), 1);
            assert_eq!(kept.to_string(), first.to_string());
            assert_eq!(format!("{kept:?}"), format!("{first:?}"));
        }
    }

    /// `t` moved `secs` seconds on.
    fn at(t: Instant, secs: u64) -> Instant {
        t.checked_add(Duration::from_secs(secs)).unwrap()
    }

    fn addrs(port: u16) -> ResolvedSocketAddrs {
        let mut addrs = DefaultResolver::default().empty();
        addrs.push(addr(port));
        addrs
    }

    /// A host whose last lookup succeeded (port 80) and ended at `good`, due again at `next`.
    fn looked_up(good: Instant, next: Instant) -> Entry {
        Entry {
            good: Some(Good {
                addrs: addrs(80),
                at: good,
            }),
            ended: good,
            failed: None,
            next,
            looking: None,
        }
    }

    // A lookup that succeeds settles as the host's newest addresses, due again 30 s after it ended,
    // which is when the host's last lookup ended.
    #[test]
    fn a_lookup_that_succeeds_settles_as_the_newest_addresses() {
        let t = Instant::now();
        let mut entry = looked_up(t, at(t, 30));
        entry.failed = Some(Failure::NotFound);
        let settled = settle(&mut entry, at(t, 40), at(t, 41), Ok(addrs(81)));
        assert!(matches!(settled, Settled::Found(found) if found[..] == [addr(81)]));
        let good = entry.good.unwrap();
        assert_eq!((&good.addrs[..], good.at), (&[addr(81)][..], at(t, 41)));
        assert!(entry.failed.is_none());
        assert_eq!((entry.ended, entry.next), (at(t, 41), at(t, 71)));
    }

    // A lookup that fails gives way to one that succeeded since it started: its request gets that
    // lookup's addresses, and the host stays as that lookup left it, with no failure kept and the
    // next lookup not brought forward.
    #[test]
    fn a_lookup_that_fails_gives_way_to_a_success_since_it_started() {
        let t = Instant::now();
        let mut entry = looked_up(at(t, 2), at(t, 32));
        let settled = settle(
            &mut entry,
            t,
            at(t, 5),
            Err(ureq::Error::Timeout(ureq::Timeout::Resolve)),
        );
        assert!(matches!(settled, Settled::Found(found) if found[..] == [addr(80)]));
        assert!(entry.failed.is_none());
        assert_eq!((entry.ended, entry.next), (at(t, 5), at(t, 32)));
        // A failed connection made the next lookup due 5 s after that success: it still comes no
        // sooner than 5 s after this lookup ended.
        let mut entry = looked_up(at(t, 2), at(t, 7));
        let settled = settle(&mut entry, t, at(t, 5), Err(ureq::Error::HostNotFound));
        assert!(matches!(settled, Settled::Found(found) if found[..] == [addr(80)]));
        assert_eq!((entry.ended, entry.next), (at(t, 5), at(t, 10)));
    }

    // A lookup that fails settles on the addresses that stand in when it ended: the next lookup is
    // due 5 s after it ended, or at the stale limit if that comes first, and never before it
    // ended. Addresses that pass the stale limit while the lookup runs stand in for nothing.
    #[test]
    fn a_lookup_that_fails_settles_on_what_stands_in_when_it_ended() {
        let t = Instant::now();
        let limit = DNS_STALE_LIMIT.as_secs();
        let timeout = || Err(ureq::Error::Timeout(ureq::Timeout::Resolve));
        let mut entry = looked_up(t, at(t, 30));
        let settled = settle(&mut entry, at(t, 60), at(t, 61), timeout());
        assert!(matches!(&settled, Settled::Stale(good, _) if good.at == t));
        assert!(matches!(
            entry.failed,
            Some(Failure::TimedOut(ureq::Timeout::Resolve))
        ));
        assert_eq!((entry.ended, entry.next), (at(t, 61), at(t, 66)));
        let mut entry = looked_up(t, at(t, 30));
        let settled = settle(&mut entry, at(t, limit - 4), at(t, limit - 2), timeout());
        assert!(matches!(settled, Settled::Stale(..)));
        assert_eq!((entry.ended, entry.next), (at(t, limit - 2), at(t, limit)));
        let mut entry = looked_up(t, at(t, 30));
        let settled = settle(&mut entry, at(t, limit - 1), at(t, limit + 1), timeout());
        assert!(matches!(settled, Settled::Failed(_)));
        assert_eq!(
            (entry.ended, entry.next),
            (at(t, limit + 1), at(t, limit + 6))
        );
    }

    // A lookup that fails with no addresses to stand in fails its request and keeps how it failed
    // until the next lookup, 5 s after it ended. A failure the cache cannot make again is not kept.
    #[test]
    fn a_lookup_that_fails_with_no_addresses_keeps_its_failure_until_the_retry() {
        let t = Instant::now();
        let mut entry = looked_up(t, t);
        entry.good = None;
        let settled = settle(&mut entry, t, at(t, 5), Err(ureq::Error::HostNotFound));
        assert!(matches!(
            settled,
            Settled::Failed(ureq::Error::HostNotFound)
        ));
        assert!(matches!(entry.failed, Some(Failure::NotFound)));
        assert_eq!((entry.ended, entry.next), (at(t, 5), at(t, 10)));
        let settled = settle(
            &mut entry,
            at(t, 10),
            at(t, 11),
            Err(ureq::Error::ConnectionFailed),
        );
        assert!(matches!(
            settled,
            Settled::Failed(ureq::Error::ConnectionFailed)
        ));
        assert!(entry.failed.is_none());
        assert_eq!((entry.ended, entry.next), (at(t, 11), at(t, 16)));
    }

    // A failure is wrapped once, however many times it passes a marking resolver or connector (a
    // CONNECT proxy's tunnel goes through both, and then the connector's own wrapping).
    #[test]
    fn a_failure_is_marked_once() {
        let twice = not_sent(not_sent(ureq::Error::Timeout(ureq::Timeout::Connect)));
        assert!(matches!(
            unwrapped(&twice),
            ureq::Error::Timeout(ureq::Timeout::Connect)
        ));
        assert!(matches!(
            sent_nothing(twice),
            (ureq::Error::Timeout(ureq::Timeout::Connect), true)
        ));
    }

    // An IP address is never cached, and each scheme's port of a host is its own entry: `https://`
    // without a port is port 443, not 80.
    #[test]
    fn ip_addresses_pass_through_and_ports_are_separate_hosts() {
        let resolver = Resolver::new(Scripted::new(vec![
            Answer::Addr(addr(1)),
            Answer::Addr(addr(2)),
            Answer::Addr(addr(3)),
            Answer::Addr(addr(4)),
            Answer::Addr(addr(5)),
        ]));
        assert_eq!(
            resolve(&resolver, "http://127.0.0.1:9/b/s").unwrap(),
            addr(1)
        );
        assert_eq!(resolve(&resolver, "http://[::1]:9/b/s").unwrap(), addr(2));
        assert_eq!(resolve(&resolver, URL).unwrap(), addr(3));
        assert_eq!(resolve(&resolver, "https://gw.test/b/s").unwrap(), addr(4));
        assert_eq!(
            resolve(&resolver, "http://gw.test:8080/b/s").unwrap(),
            addr(5)
        );
        assert_eq!(resolve(&resolver, URL).unwrap(), addr(3));
        assert_eq!(
            resolve(&resolver, "https://gw.test:443/b/s").unwrap(),
            addr(4)
        );
        let mut hosts = resolver.hosts().keys().cloned().collect::<Vec<_>>();
        hosts.sort();
        assert_eq!(hosts, ["gw.test:443", "gw.test:80", "gw.test:8080"]);
    }
}

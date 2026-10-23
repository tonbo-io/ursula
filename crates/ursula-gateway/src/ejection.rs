//! Ejection of upstreams that do not answer (#454).
//!
//! The probes (`probe`) are the one judge of whether an upstream answers:
//! an upstream leaves selection when a probe of it fails at the transport or
//! gets no answer within the probe timeout, and when a request to it fails
//! at the transport. It comes back with its next answer, to a probe or to a
//! request, whatever its status. The gateway probes every upstream several
//! times a second, so an upstream that stops answering leaves selection
//! within a probe interval and a probe timeout, whether requests reach it or
//! not, and one that answers again is back within a probe interval.
//!
//! Each ejection has a cause ([`Ejection`]). An upstream that refused a
//! connection, or closed one, stopped accepting requests: a draining node
//! closes its listener and still answers every request it accepted. One
//! whose probe got no answer in time stopped answering: a stopped process or
//! a frozen host answers nothing, the requests it accepted included. Both
//! leave selection, but only an upstream that stopped answering ends the
//! wait of a request in flight to it ([`Ejections::stopped_answering`]).
//!
//! A request that gets no response headers within the response header
//! timeout ejects nothing. The probes eject a silent node sooner, and a live
//! node can hold one request longer than the timeout (a write on a leader
//! elected over a slow link, a read forwarded to an unreachable leader, a
//! snapshot commit of many parts) while it answers every probe: ejecting it
//! would refuse every redirect to it until its next probe.
//!
//! Requests skip an ejected upstream: along a cached route, at random, and
//! on every hop after a redirect, a hop that follows a leader redirect
//! included. When every upstream is ejected, none is: requests choose among
//! all of them, as if nothing were ejected, rather than among none. A
//! request waiting on an upstream can stop waiting once requests skip it
//! because it stopped answering ([`Ejections::until`]).
//!
//! [`Ejections`] holds the ejected upstreams and their causes under a lock
//! shared by every request, with a count that lets requests skip the lock
//! while nothing is ejected.

use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Mutex;
use std::sync::PoisonError;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use rand::Rng;
use rand::seq::IndexedRandom;
use rand::seq::IteratorRandom;
use tokio::sync::watch;

use crate::GatewayError;

/// Why an upstream is out of selection. The later variant is the stronger
/// evidence: an upstream ejected for both keeps the later until it answers.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum Ejection {
    /// It failed at the transport without a timeout: it refused a
    /// connection, as a draining node does once its listener closed, or it
    /// closed one. It may still answer the requests it accepted, so a request
    /// in flight to it waits for its answer.
    StoppedAccepting,
    /// It sent nothing in time: a probe of it got no response headers within
    /// the probe timeout, or the transport timed out a connect to it or data
    /// sent to it. It answers nothing, as a stopped process or a frozen host,
    /// so a request in flight to it whose repeat is safe goes on at once.
    StoppedAnswering,
}

impl Ejection {
    /// The cause a transport failure, `source`, gives: a timeout says that
    /// the upstream sent nothing, any other failure that it refused or closed
    /// the connection.
    pub(crate) fn of_transport(source: &reqwest::Error) -> Self {
        if source.is_timeout() {
            Self::StoppedAnswering
        } else {
            Self::StoppedAccepting
        }
    }
}

/// What [`Ejections::failed`] changed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum EjectionChange {
    /// The upstream was in selection, and is out of it now.
    Ejected,
    /// The upstream was out of selection for a weaker cause, and is out for
    /// this one now.
    Escalated,
    /// Nothing: the upstream was out of selection for this cause or a
    /// stronger one.
    Unchanged,
}

/// Where a request goes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Pick<'a> {
    pub(crate) upstream: &'a str,
    /// Whether `upstream` is the cached route the caller passed in.
    pub(crate) cached: bool,
}

/// The upstream a request goes to: the cached route unless `skipped` says
/// otherwise, else one at random among those not skipped. When every
/// upstream is skipped, the cached route or any upstream, rather than none.
fn choose<'a>(
    cached: Option<&'a str>,
    upstreams: &'a [String],
    skipped: impl Fn(&str) -> bool,
    rng: &mut impl Rng,
) -> Option<Pick<'a>> {
    if let Some(cached) = cached
        && !skipped(cached)
    {
        return Some(Pick {
            upstream: cached,
            cached: true,
        });
    }
    if let Some(open) = upstreams
        .iter()
        .map(String::as_str)
        .filter(|upstream| !skipped(upstream))
        .choose(rng)
    {
        return Some(Pick {
            upstream: open,
            cached: false,
        });
    }
    match cached {
        Some(cached) => Some(Pick {
            upstream: cached,
            cached: true,
        }),
        None => upstreams.choose(rng).map(|upstream| Pick {
            upstream: upstream.as_str(),
            cached: false,
        }),
    }
}

/// The ejected upstreams, shared by every request and by the probes.
#[derive(Debug)]
pub(crate) struct Ejections {
    /// Distinct configured upstreams: with as many ejected, none is skipped.
    upstreams: usize,
    ejected: Mutex<HashMap<String, Ejection>>,
    /// Entries in `ejected`, stored under its lock after every change. While
    /// it is 0, requests skip the lock. A request that reads it during a
    /// change acts as one that started before it.
    entries: AtomicUsize,
    /// Times [`Ejections::eject`] ejected an upstream that was in selection.
    ejections: AtomicU64,
    /// Marked changed whenever an upstream is ejected or readmitted, after
    /// the change, so that [`Ejections::until`] looks again.
    changes: watch::Sender<()>,
}

impl Ejections {
    /// No upstream of `upstreams` ejected.
    pub(crate) fn new(upstreams: &[String]) -> Self {
        Self {
            upstreams: upstreams.iter().collect::<HashSet<_>>().len(),
            ejected: Mutex::new(HashMap::new()),
            entries: AtomicUsize::new(0),
            ejections: AtomicU64::new(0),
            changes: watch::Sender::new(()),
        }
    }

    fn with_ejected<T>(&self, f: impl FnOnce(&mut HashMap<String, Ejection>) -> T) -> T {
        let mut ejected = self.ejected.lock().unwrap_or_else(PoisonError::into_inner);
        let out = f(&mut ejected);
        self.entries.store(ejected.len(), Ordering::Release);
        out
    }

    /// Whether requests skip ejected upstreams now: some, but not all of
    /// them, are ejected.
    fn skipping(&self) -> bool {
        let entries = self.entries.load(Ordering::Acquire);
        entries > 0 && entries < self.upstreams
    }

    /// Ejects `upstream` for `cause`, unless it is out of selection for that
    /// cause or a stronger one already, and answers what changed.
    pub(crate) fn failed(&self, upstream: &str, cause: Ejection) -> EjectionChange {
        let change = self.with_ejected(|ejected| match ejected.get_mut(upstream) {
            None => {
                ejected.insert(upstream.to_owned(), cause);
                EjectionChange::Ejected
            }
            Some(held) if *held < cause => {
                *held = cause;
                EjectionChange::Escalated
            }
            Some(_held) => EjectionChange::Unchanged,
        });
        if change != EjectionChange::Unchanged {
            self.changes.send_replace(());
        }
        change
    }

    /// Readmits `upstream`, which answered. Returns whether it was ejected.
    pub(crate) fn answered(&self, upstream: &str) -> bool {
        let readmitted = self.entries.load(Ordering::Acquire) > 0
            && self.with_ejected(|ejected| ejected.remove(upstream).is_some());
        if readmitted {
            self.changes.send_replace(());
        }
        readmitted
    }

    /// Ejects `upstream` for `cause`, which `error` shows: a probe's or a
    /// request's transport failure, or a probe's timeout. Counts the ejection
    /// if the upstream was in selection, and logs it if its cause changed.
    pub(crate) fn eject(&self, upstream: &str, cause: Ejection, error: &GatewayError) {
        let change = self.failed(upstream, cause);
        if change == EjectionChange::Ejected {
            self.ejections.fetch_add(1, Ordering::Relaxed);
        }
        match (change, cause) {
            (EjectionChange::Unchanged, _) => {}
            (EjectionChange::Ejected | EjectionChange::Escalated, Ejection::StoppedAccepting) => {
                tracing::warn!(
                    upstream,
                    %error,
                    "upstream refused or closed a connection; leaving it out of selection until it answers, its requests in flight still wait for their answers"
                );
            }
            (EjectionChange::Ejected | EjectionChange::Escalated, Ejection::StoppedAnswering) => {
                tracing::warn!(
                    upstream,
                    %error,
                    "upstream sent nothing in time; leaving it out of selection until it answers, its safe requests in flight go on elsewhere"
                );
            }
        }
    }

    /// Readmits `upstream`, which answered a request or a probe. Logged at
    /// debug: an upstream whose new connections take about the probe
    /// timeout goes out and back in on every such connection, and its
    /// ejection is logged already.
    pub(crate) fn readmit(&self, upstream: &str) {
        if self.answered(upstream) {
            tracing::debug!(upstream, "upstream answers again; back in selection");
        }
    }

    /// Resolves once `holds`, a condition on the ejected upstreams, does:
    /// at once if it holds now, else at the first ejection or readmission
    /// after which it does.
    pub(crate) async fn until(&self, holds: impl Fn() -> bool) {
        // Subscribed before the first look, so a change after it wakes the
        // wait.
        let mut changes = self.changes.subscribe();
        while !holds() {
            if changes.changed().await.is_err() {
                // Unreachable: `self` holds the sender while this waits.
                std::future::pending::<()>().await;
            }
        }
    }

    /// Chooses where a request goes: `cached`, the leader route the gateway
    /// learned for it, unless it is skipped, else an upstream that is not
    /// skipped at random.
    pub(crate) fn pick<'a>(
        &self,
        cached: Option<&'a str>,
        upstreams: &'a [String],
        rng: &mut impl Rng,
    ) -> Option<Pick<'a>> {
        if !self.skipping() {
            return choose(cached, upstreams, |_| false, rng);
        }
        self.with_ejected(|ejected| {
            choose(
                cached,
                upstreams,
                |upstream| ejected.contains_key(upstream),
                rng,
            )
        })
    }

    /// Whether a hop may go to `upstream`: it is not ejected, or every
    /// upstream is.
    pub(crate) fn admits(&self, upstream: &str) -> bool {
        !self.skipping() || self.with_ejected(|ejected| !ejected.contains_key(upstream))
    }

    /// Whether requests skip `upstream` because it stopped answering
    /// ([`Ejection::StoppedAnswering`]): a request in flight to it gets no
    /// answer. False while every upstream is ejected, as nothing is skipped
    /// then.
    pub(crate) fn stopped_answering(&self, upstream: &str) -> bool {
        self.skipping()
            && self
                .with_ejected(|ejected| ejected.get(upstream) == Some(&Ejection::StoppedAnswering))
    }

    /// Upstreams ejected now. Requests skip them unless every upstream is
    /// ejected.
    pub(crate) fn ejected(&self) -> usize {
        self.entries.load(Ordering::Acquire)
    }

    /// Times [`Ejections::eject`] ejected an upstream that was in selection.
    pub(crate) fn ejections(&self) -> u64 {
        self.ejections.load(Ordering::Relaxed)
    }
}

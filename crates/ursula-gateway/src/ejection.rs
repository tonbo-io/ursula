//! Ejection of upstreams that do not answer (#454).
//!
//! An upstream leaves selection when a request to it fails at the transport
//! or gets no response headers within the response header timeout, and when
//! a probe of it fails or gets no answer within the probe timeout (`probe`).
//! It comes back with its next answer, to a probe or to a request, whatever
//! its status. The gateway probes every upstream several times a second, so
//! an upstream that stops answering leaves selection within a probe interval
//! and a probe timeout, whether requests reach it or not, and one that
//! answers again is back within a probe interval.
//!
//! Requests skip an ejected upstream: along a cached route, at random, and
//! on every hop after a redirect, a hop that follows a leader redirect
//! included. When every upstream is ejected, none is: requests choose among
//! all of them, as if nothing were ejected, rather than among none.
//!
//! [`Ejections`] holds the ejected upstreams under a lock shared by every
//! request, with a count that lets requests skip the lock while nothing is
//! ejected.

use std::collections::HashSet;
use std::sync::Mutex;
use std::sync::PoisonError;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use rand::Rng;
use rand::seq::IndexedRandom;
use rand::seq::IteratorRandom;

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
    ejected: Mutex<HashSet<String>>,
    /// Entries in `ejected`, stored under its lock after every change. While
    /// it is 0, requests skip the lock. A request that reads it during a
    /// change acts as one that started before it.
    entries: AtomicUsize,
}

impl Ejections {
    /// No upstream of `upstreams` ejected.
    pub(crate) fn new(upstreams: &[String]) -> Self {
        Self {
            upstreams: upstreams.iter().collect::<HashSet<_>>().len(),
            ejected: Mutex::new(HashSet::new()),
            entries: AtomicUsize::new(0),
        }
    }

    fn with_ejected<T>(&self, f: impl FnOnce(&mut HashSet<String>) -> T) -> T {
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

    /// Ejects `upstream`, which failed or did not answer in time. Returns
    /// whether it was in selection before.
    pub(crate) fn failed(&self, upstream: &str) -> bool {
        self.with_ejected(|ejected| ejected.insert(upstream.to_owned()))
    }

    /// Readmits `upstream`, which answered. Returns whether it was ejected.
    pub(crate) fn answered(&self, upstream: &str) -> bool {
        self.entries.load(Ordering::Acquire) > 0
            && self.with_ejected(|ejected| ejected.remove(upstream))
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
                |upstream| ejected.contains(upstream),
                rng,
            )
        })
    }

    /// Whether a hop may go to `upstream`: it is not ejected, or every
    /// upstream is.
    pub(crate) fn admits(&self, upstream: &str) -> bool {
        !self.skipping() || self.with_ejected(|ejected| !ejected.contains(upstream))
    }

    /// Upstreams ejected now. Requests skip them unless every upstream is
    /// ejected.
    pub(crate) fn ejected(&self) -> usize {
        self.entries.load(Ordering::Acquire)
    }
}

//! Passive ejection of upstreams that fail at the transport (#454).
//!
//! A voter whose host is gone (powered off, terminated, partitioned) answers
//! no connection attempt, and a pooled connection to it fails only after the
//! upstream client's bounds. Once a request to an upstream fails at the
//! transport, the gateway leaves that upstream out of selection for a window:
//! 10 s, doubled for each failure after a window up to 60 s.
//!
//! When the window expires, one request probes the upstream: the first one
//! that selects it, through a cached route or at random. Until that probe
//! answers or fails, every other request keeps skipping the upstream. A probe
//! that does neither within the probe deadline (a slow long-poll, say) no
//! longer holds the others back, and the next request that selects the
//! upstream probes it again. Any response from an upstream, whatever its
//! status, clears its ejection. If every upstream is ejected or being probed,
//! selection falls back to all of them rather than to none, and to the cached
//! route first.
//!
//! Failures of requests already in flight when an upstream was ejected do not
//! lengthen its window: only a failure after the window can.
//!
//! [`Table`] holds the decisions, with time and randomness passed in.
//! [`Ejections`] wraps it in a lock shared by every request, with a count of
//! entries that lets requests skip the lock while nothing is ejected.

use std::collections::HashMap;
use std::sync::Mutex;
use std::sync::PoisonError;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;

use rand::Rng;
use rand::seq::IndexedRandom;
use rand::seq::IteratorRandom;
use tokio::time::Instant;

/// The first window of an upstream that fails, and the longest one.
pub(crate) const EJECTION_BASE: Duration = Duration::from_secs(10);
pub(crate) const EJECTION_MAX: Duration = Duration::from_secs(60);

/// The window of the `failures`-th failure in a row: [`EJECTION_BASE`],
/// doubling up to [`EJECTION_MAX`].
fn window(failures: u32) -> Duration {
    let doubling = 1_u32
        .checked_shl(failures.saturating_sub(1))
        .unwrap_or(u32::MAX);
    EJECTION_BASE.saturating_mul(doubling).min(EJECTION_MAX)
}

/// An upstream that failed at the transport and has not answered since.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Ejection {
    /// Out of selection until `until`. After that, the next request that
    /// selects it probes it.
    Out { until: Instant, failures: u32 },
    /// One request has been probing it since `since`.
    Probing { failures: u32, since: Instant },
}

impl Ejection {
    /// Failed windows in a row.
    fn failures(self) -> u32 {
        match self {
            Self::Out { failures, .. } | Self::Probing { failures, .. } => failures,
        }
    }

    /// Whether requests at `now` skip the upstream: inside its window, or
    /// while a probe sent less than `probe_deadline` ago is unresolved.
    fn active(self, now: Instant, probe_deadline: Duration) -> bool {
        match self {
            Self::Out { until, .. } => now < until,
            Self::Probing { since, .. } => now.saturating_duration_since(since) < probe_deadline,
        }
    }
}

/// What a transport failure did to an upstream's ejection.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Failed {
    /// Ejected for `window`, the `failures`-th window in a row.
    Ejected { window: Duration, failures: u32 },
    /// Already inside its window: a request that was in flight before the
    /// ejection.
    AlreadyEjected,
}

/// Where a request goes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Pick<'a> {
    pub(crate) upstream: &'a str,
    /// Whether `upstream` is the cached route the caller passed in.
    pub(crate) cached: bool,
    /// Whether this request claimed the probe of `upstream`.
    pub(crate) probe: bool,
}

/// The upstream a request goes to: the cached route unless `skipped` says
/// otherwise, else one at random among those not skipped. When every
/// upstream is skipped, the cached route or any upstream, rather than none.
fn choose<'a>(
    cached: Option<&'a str>,
    upstreams: &'a [String],
    skipped: impl Fn(&str) -> bool,
    rng: &mut impl Rng,
) -> Option<(&'a str, bool)> {
    if let Some(cached) = cached
        && !skipped(cached)
    {
        return Some((cached, true));
    }
    if let Some(open) = upstreams
        .iter()
        .map(String::as_str)
        .filter(|upstream| !skipped(upstream))
        .choose(rng)
    {
        return Some((open, false));
    }
    match cached {
        Some(cached) => Some((cached, true)),
        None => upstreams
            .choose(rng)
            .map(|upstream| (upstream.as_str(), false)),
    }
}

/// The ejections of every upstream, keyed by upstream URL.
#[derive(Debug, Default)]
struct Table(HashMap<String, Ejection>);

impl Table {
    /// Records a transport failure of `upstream` at `now`.
    fn failed(&mut self, upstream: &str, now: Instant, probe_deadline: Duration) -> Failed {
        let previous = self.0.get(upstream).copied();
        if let Some(previous @ Ejection::Out { .. }) = previous
            && previous.active(now, probe_deadline)
        {
            return Failed::AlreadyEjected;
        }
        let failures = previous.map_or(1, |e| e.failures().saturating_add(1));
        let window = window(failures);
        self.0.insert(upstream.to_owned(), Ejection::Out {
            until: now.checked_add(window).unwrap_or(now),
            failures,
        });
        Failed::Ejected { window, failures }
    }

    /// Records a response from `upstream`. Returns whether it was ejected.
    fn answered(&mut self, upstream: &str) -> bool {
        self.0.remove(upstream).is_some()
    }

    /// Chooses where a request at `now` goes (see [`choose`]), skipping
    /// active ejections. A chosen upstream whose ejection is no longer
    /// active moves to [`Ejection::Probing`]: this request is its probe.
    fn pick<'a>(
        &mut self,
        cached: Option<&'a str>,
        upstreams: &'a [String],
        now: Instant,
        probe_deadline: Duration,
        rng: &mut impl Rng,
    ) -> Option<Pick<'a>> {
        let (upstream, cached) = choose(
            cached,
            upstreams,
            |upstream| {
                self.0
                    .get(upstream)
                    .is_some_and(|e| e.active(now, probe_deadline))
            },
            rng,
        )?;
        let probe = match self.0.get_mut(upstream) {
            Some(e) if !e.active(now, probe_deadline) => {
                *e = Ejection::Probing {
                    failures: e.failures(),
                    since: now,
                };
                true
            }
            _ => false,
        };
        Some(Pick {
            upstream,
            cached,
            probe,
        })
    }

    /// Upstreams requests skip at `now`.
    fn active(&self, now: Instant, probe_deadline: Duration) -> usize {
        self.0
            .values()
            .filter(|e| e.active(now, probe_deadline))
            .count()
    }
}

/// The ejection [`Table`] shared by every request.
#[derive(Debug)]
pub(crate) struct Ejections {
    /// How long a probe holds other requests back: the longest a request to
    /// a dead upstream takes to fail at the transport.
    probe_deadline: Duration,
    table: Mutex<Table>,
    /// Entries in `table`, stored under its lock after every change. While it
    /// is 0, requests skip the lock. A request that reads it during a change
    /// acts as one that was in flight before it.
    entries: AtomicUsize,
}

impl Ejections {
    pub(crate) fn new(probe_deadline: Duration) -> Self {
        Self {
            probe_deadline,
            table: Mutex::new(Table::default()),
            entries: AtomicUsize::new(0),
        }
    }

    fn nothing_ejected(&self) -> bool {
        self.entries.load(Ordering::Acquire) == 0
    }

    fn with_table<T>(&self, f: impl FnOnce(&mut Table) -> T) -> T {
        let mut table = self.table.lock().unwrap_or_else(PoisonError::into_inner);
        let out = f(&mut table);
        self.entries.store(table.0.len(), Ordering::Release);
        out
    }

    /// Records a transport failure of `upstream` at `now`.
    pub(crate) fn failed(&self, upstream: &str, now: Instant) -> Failed {
        self.with_table(|table| table.failed(upstream, now, self.probe_deadline))
    }

    /// Records a response from `upstream`. Returns whether it was ejected.
    pub(crate) fn answered(&self, upstream: &str) -> bool {
        !self.nothing_ejected() && self.with_table(|table| table.answered(upstream))
    }

    /// Chooses where a request at `now` goes: `cached`, the leader route the
    /// gateway learned for it, unless it is ejected, else an upstream that is
    /// not ejected at random.
    pub(crate) fn pick<'a>(
        &self,
        cached: Option<&'a str>,
        upstreams: &'a [String],
        now: Instant,
        rng: &mut impl Rng,
    ) -> Option<Pick<'a>> {
        if self.nothing_ejected() {
            let (upstream, cached) = choose(cached, upstreams, |_| false, rng)?;
            return Some(Pick {
                upstream,
                cached,
                probe: false,
            });
        }
        self.with_table(|table| table.pick(cached, upstreams, now, self.probe_deadline, rng))
    }

    /// Upstreams requests skip at `now`.
    pub(crate) fn active(&self, now: Instant) -> usize {
        if self.nothing_ejected() {
            return 0;
        }
        self.with_table(|table| table.active(now, self.probe_deadline))
    }
}

#[cfg(test)]
#[path = "ejection_tests.rs"]
mod tests;

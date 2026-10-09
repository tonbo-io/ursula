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
mod tests {
    use std::sync::Arc;
    use std::sync::Barrier;
    use std::time::Duration;

    use tokio::time::Instant;

    use super::Ejection;
    use super::Ejections;
    use super::Failed;
    use super::Pick;
    use super::Table;

    /// The gateway's default probe deadline: 5 s connect plus 5 s user
    /// timeout.
    const PROBE_DEADLINE: Duration = Duration::from_secs(10);

    fn secs(secs: u64) -> Duration {
        Duration::from_secs(secs)
    }

    fn after(now: Instant, by: Duration) -> Instant {
        now.checked_add(by).unwrap()
    }

    fn upstreams() -> [String; 2] {
        ["a", "b"].map(String::from)
    }

    fn pick<'a>(
        table: &mut Table,
        cached: Option<&'a str>,
        upstreams: &'a [String],
        now: Instant,
    ) -> Pick<'a> {
        table
            .pick(cached, upstreams, now, PROBE_DEADLINE, &mut rand::rng())
            .unwrap()
    }

    /// `upstream` ejected once at `now`, by itself in the table.
    fn ejected_at(upstream: &str, now: Instant) -> Table {
        let mut table = Table::default();
        table.failed(upstream, now, PROBE_DEADLINE);
        table
    }

    #[test]
    fn a_first_failure_ejects_for_the_base_window() {
        let mut table = Table::default();
        let now = Instant::now();
        assert_eq!(table.failed("a", now, PROBE_DEADLINE), Failed::Ejected {
            window: secs(10),
            failures: 1,
        });
        assert_eq!(
            table.0.get("a"),
            Some(&Ejection::Out {
                until: after(now, secs(10)),
                failures: 1,
            })
        );
    }

    // Requests that were in flight before the ejection fail inside the
    // window and do not lengthen it.
    #[test]
    fn failures_inside_the_window_change_nothing() {
        let now = Instant::now();
        let mut table = ejected_at("a", now);
        let before = table.0.get("a").copied();
        for at in [secs(0), secs(1), secs(9)] {
            assert_eq!(
                table.failed("a", after(now, at), PROBE_DEADLINE),
                Failed::AlreadyEjected
            );
        }
        assert_eq!(table.0.get("a").copied(), before);
    }

    #[test]
    fn an_active_window_is_skipped_by_cached_routes_and_random_picks() {
        let now = Instant::now();
        let mut table = ejected_at("a", now);
        let upstreams = upstreams();
        for at in [secs(0), secs(9)] {
            let at = after(now, at);
            assert_eq!(pick(&mut table, Some("a"), &upstreams, at), Pick {
                upstream: "b",
                cached: false,
                probe: false,
            });
            assert_eq!(pick(&mut table, None, &upstreams, at).upstream, "b");
        }
        assert_eq!(table.active(after(now, secs(9)), PROBE_DEADLINE), 1);
    }

    #[test]
    fn the_first_pick_after_the_window_claims_the_probe() {
        let now = Instant::now();
        let mut table = ejected_at("a", now);
        let upstreams = upstreams();
        let expired = after(now, secs(10));
        assert_eq!(table.active(expired, PROBE_DEADLINE), 0);

        assert_eq!(pick(&mut table, Some("a"), &upstreams, expired), Pick {
            upstream: "a",
            cached: true,
            probe: true,
        });
        assert_eq!(
            table.0.get("a"),
            Some(&Ejection::Probing {
                failures: 1,
                since: expired,
            })
        );
    }

    // Two callers at the same instant, both routed to the expired upstream:
    // only the first probes it. The second goes elsewhere, and so does every
    // request until the probe resolves or its deadline passes.
    #[test]
    fn two_callers_send_one_probe() {
        let now = Instant::now();
        let mut table = ejected_at("a", now);
        let upstreams = upstreams();
        let expired = after(now, secs(10));

        let first = pick(&mut table, Some("a"), &upstreams, expired);
        let second = pick(&mut table, Some("a"), &upstreams, expired);

        assert_eq!((first.upstream, first.probe), ("a", true));
        assert_eq!(second, Pick {
            upstream: "b",
            cached: false,
            probe: false,
        });
        let before_deadline = after(expired, PROBE_DEADLINE.saturating_sub(secs(1)));
        assert_eq!(
            pick(&mut table, None, &upstreams, before_deadline).upstream,
            "b"
        );
        assert_eq!(table.active(before_deadline, PROBE_DEADLINE), 1);
    }

    // A probe that neither answered nor failed by its deadline no longer
    // holds the others back: the next request to select the upstream probes
    // it again.
    #[test]
    fn a_probe_past_its_deadline_is_claimed_again() {
        let now = Instant::now();
        let mut table = ejected_at("a", now);
        let upstreams = upstreams();
        let expired = after(now, secs(10));
        pick(&mut table, Some("a"), &upstreams, expired);

        let stale = after(expired, PROBE_DEADLINE);
        assert_eq!(table.active(stale, PROBE_DEADLINE), 0);
        assert_eq!(pick(&mut table, Some("a"), &upstreams, stale), Pick {
            upstream: "a",
            cached: true,
            probe: true,
        });
        assert_eq!(
            table.0.get("a"),
            Some(&Ejection::Probing {
                failures: 1,
                since: stale,
            })
        );
    }

    // Each failure after the first comes from the probe of the window before.
    #[test]
    fn a_failed_probe_ejects_for_a_doubled_window_up_to_the_cap() {
        let mut now = Instant::now();
        let mut table = Table::default();
        let upstreams = upstreams();
        for (failures, window) in [(1, 10), (2, 20), (3, 40), (4, 60), (5, 60)] {
            assert_eq!(table.failed("a", now, PROBE_DEADLINE), Failed::Ejected {
                window: secs(window),
                failures,
            });
            assert_eq!(
                table.0.get("a"),
                Some(&Ejection::Out {
                    until: after(now, secs(window)),
                    failures,
                })
            );
            now = after(now, secs(window));
            assert!(pick(&mut table, Some("a"), &upstreams, now).probe);
            now = after(now, secs(1));
        }
    }

    // A request that did not go through selection, such as one that follows
    // a redirect, can fail after the window too.
    #[test]
    fn a_failure_after_an_unclaimed_window_doubles_it() {
        let now = Instant::now();
        let mut table = ejected_at("a", now);
        assert_eq!(
            table.failed("a", after(now, secs(10)), PROBE_DEADLINE),
            Failed::Ejected {
                window: secs(20),
                failures: 2,
            }
        );
    }

    #[test]
    fn any_answer_clears_the_ejection() {
        let now = Instant::now();
        let upstreams = upstreams();

        let mut out = ejected_at("a", now);
        assert!(out.answered("a"));
        assert!(out.0.is_empty());

        let mut probing = ejected_at("a", now);
        pick(&mut probing, Some("a"), &upstreams, after(now, secs(10)));
        assert!(probing.answered("a"));
        assert!(probing.0.is_empty());

        assert!(!probing.answered("a"));
        assert_eq!(probing.failed("a", now, PROBE_DEADLINE), Failed::Ejected {
            window: secs(10),
            failures: 1,
        });
    }

    // With every upstream ejected or being probed, requests still go out:
    // along the cached route, which is kept, or to any upstream. None of
    // them claims a probe.
    #[test]
    fn selection_falls_back_to_every_upstream_and_keeps_the_cached_route() {
        let now = Instant::now();
        let upstreams = upstreams();
        let mut table = ejected_at("a", now);
        table.failed("b", after(now, secs(5)), PROBE_DEADLINE);
        let probing_a = after(now, secs(10));
        pick(&mut table, Some("a"), &upstreams, probing_a);
        let before = table.0.clone();

        assert_eq!(pick(&mut table, Some("b"), &upstreams, probing_a), Pick {
            upstream: "b",
            cached: true,
            probe: false,
        });
        for _ in 0..20 {
            assert!(!pick(&mut table, None, &upstreams, probing_a).probe);
        }
        assert_eq!(table.0, before);
        assert_eq!(table.active(probing_a, PROBE_DEADLINE), 2);
    }

    #[test]
    fn the_entry_count_follows_the_table() {
        let ejections = Ejections::new(PROBE_DEADLINE);
        let upstreams = upstreams();
        let now = Instant::now();
        assert_eq!(ejections.active(now), 0);
        assert!(!ejections.answered("a"));
        assert_eq!(
            ejections
                .pick(Some("a"), &upstreams, now, &mut rand::rng())
                .unwrap(),
            Pick {
                upstream: "a",
                cached: true,
                probe: false,
            }
        );

        ejections.failed("a", now);
        assert_eq!(ejections.active(now), 1);
        assert_eq!(
            ejections
                .pick(Some("a"), &upstreams, now, &mut rand::rng())
                .unwrap()
                .upstream,
            "b"
        );
        assert!(ejections.answered("a"));
        assert_eq!(ejections.active(now), 0);
        assert_eq!(
            ejections
                .pick(Some("a"), &upstreams, now, &mut rand::rng())
                .unwrap()
                .upstream,
            "a"
        );
    }

    // Callers on different threads at the same instant, all routed to the
    // expired upstream: exactly one probes it.
    #[test]
    fn concurrent_callers_send_one_probe() {
        const CALLERS: usize = 16;
        let ejections = Arc::new(Ejections::new(PROBE_DEADLINE));
        let now = Instant::now();
        ejections.failed("a", now);
        let expired = after(now, secs(10));
        let start = Arc::new(Barrier::new(CALLERS));

        let callers: Vec<_> = (0..CALLERS)
            .map(|_| {
                let ejections = Arc::clone(&ejections);
                let start = Arc::clone(&start);
                std::thread::spawn(move || {
                    let upstreams = upstreams();
                    start.wait();
                    let pick = ejections
                        .pick(Some("a"), &upstreams, expired, &mut rand::rng())
                        .unwrap();
                    (pick.upstream.to_owned(), pick.probe)
                })
            })
            .collect();
        let picks: Vec<_> = callers.into_iter().map(|c| c.join().unwrap()).collect();

        assert_eq!(picks.iter().filter(|(_, probe)| *probe).count(), 1);
        assert_eq!(
            picks.iter().filter(|(upstream, _)| upstream == "a").count(),
            1
        );
    }
}

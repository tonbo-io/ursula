//! Passive ejection of upstreams that fail at the transport (#454).
//!
//! A voter whose host is gone (powered off, terminated, partitioned) answers
//! no connection attempt, and a pooled connection to it fails only after the
//! upstream client's bounds. Once a request to an upstream fails at the
//! transport, the gateway leaves that upstream out of selection for a window:
//! 10 s, doubled for each failed probe up to 60 s. When the window expires,
//! the next request that picks the upstream probes it. Any response from an
//! upstream, whatever its status, clears its ejection. If every upstream is
//! ejected, selection falls back to all of them rather than to none.
//!
//! Failures of requests already in flight when an upstream was ejected do not
//! lengthen its window: only a probe after the window can.

use std::collections::HashMap;
use std::sync::Mutex;
use std::sync::PoisonError;
use std::time::Duration;

use tokio::time::Instant;

/// The first window of an upstream that fails, and the longest one.
pub(crate) const EJECTION_BASE: Duration = Duration::from_secs(10);
pub(crate) const EJECTION_MAX: Duration = Duration::from_secs(60);

#[derive(Debug)]
pub(crate) struct Ejections {
    base: Duration,
    max: Duration,
    ejected: Mutex<HashMap<String, Ejection>>,
}

#[derive(Clone, Copy, Debug)]
struct Ejection {
    until: Instant,
    /// Failed windows since the upstream last answered.
    failures: u32,
}

/// What a transport failure did to an upstream's ejection.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Failed {
    /// Ejected for `window`, the `failures`-th window in a row.
    Ejected { window: Duration, failures: u32 },
    /// Already ejected: a request that was in flight before the ejection.
    AlreadyEjected,
}

impl Ejections {
    pub(crate) fn new(base: Duration, max: Duration) -> Self {
        Self {
            base,
            max,
            ejected: Mutex::new(HashMap::new()),
        }
    }

    /// Records a transport failure of `upstream` at `now`.
    pub(crate) fn failed(&self, upstream: &str, now: Instant) -> Failed {
        let mut ejected = self.ejected.lock().unwrap_or_else(PoisonError::into_inner);
        let previous = ejected.get(upstream).copied();
        if previous.is_some_and(|e| now < e.until) {
            return Failed::AlreadyEjected;
        }
        let failures = previous.map_or(1, |e| e.failures.saturating_add(1));
        let doubling = 1_u32
            .checked_shl(failures.saturating_sub(1))
            .unwrap_or(u32::MAX);
        let window = self.base.saturating_mul(doubling).min(self.max);
        ejected.insert(upstream.to_owned(), Ejection {
            until: now.checked_add(window).unwrap_or(now),
            failures,
        });
        Failed::Ejected { window, failures }
    }

    /// Records a response from `upstream`. Returns whether it was ejected.
    pub(crate) fn answered(&self, upstream: &str) -> bool {
        self.ejected
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(upstream)
            .is_some()
    }

    pub(crate) fn is_ejected(&self, upstream: &str, now: Instant) -> bool {
        self.ejected
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(upstream)
            .is_some_and(|e| now < e.until)
    }

    /// The upstreams a request may pick at `now`: those not ejected, or all
    /// of them when every one is.
    pub(crate) fn eligible<'a>(&self, upstreams: &'a [String], now: Instant) -> Vec<&'a String> {
        let ejected = self.ejected.lock().unwrap_or_else(PoisonError::into_inner);
        let live: Vec<&String> = upstreams
            .iter()
            .filter(|u| !ejected.get(u.as_str()).is_some_and(|e| now < e.until))
            .collect();
        if live.is_empty() {
            upstreams.iter().collect()
        } else {
            live
        }
    }

    /// Upstreams ejected at `now`.
    pub(crate) fn count(&self, now: Instant) -> usize {
        self.ejected
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .values()
            .filter(|e| now < e.until)
            .count()
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::time::Instant;

    use super::Ejections;
    use super::Failed;

    fn ejections() -> Ejections {
        Ejections::new(Duration::from_secs(10), Duration::from_secs(60))
    }

    fn after(now: Instant, secs: u64) -> Instant {
        now.checked_add(Duration::from_secs(secs)).unwrap()
    }

    #[test]
    fn windows_double_up_to_the_cap_and_an_answer_clears_them() {
        let e = ejections();
        let mut now = Instant::now();
        for (failures, window) in [(1, 10), (2, 20), (3, 40), (4, 60), (5, 60)] {
            assert_eq!(e.failed("a", now), Failed::Ejected {
                window: Duration::from_secs(window),
                failures
            });
            assert!(e.is_ejected("a", now));
            now = after(now, window);
            assert!(!e.is_ejected("a", now));
        }
        assert!(e.answered("a"));
        assert!(!e.answered("a"));
        assert_eq!(e.failed("a", now), Failed::Ejected {
            window: Duration::from_secs(10),
            failures: 1
        });
    }

    #[test]
    fn failures_inside_a_window_do_not_lengthen_it() {
        let e = ejections();
        let now = Instant::now();
        assert!(matches!(e.failed("a", now), Failed::Ejected { .. }));
        for _ in 0..20 {
            assert_eq!(e.failed("a", after(now, 1)), Failed::AlreadyEjected);
        }
        assert!(!e.is_ejected("a", after(now, 10)));
        assert_eq!(e.failed("a", after(now, 10)), Failed::Ejected {
            window: Duration::from_secs(20),
            failures: 2
        });
    }

    #[test]
    fn selection_skips_ejected_upstreams_but_never_empties() {
        let e = ejections();
        let upstreams = ["a", "b", "c"].map(String::from);
        let now = Instant::now();
        assert_eq!(e.eligible(&upstreams, now).len(), 3);
        e.failed("b", now);
        assert_eq!(e.eligible(&upstreams, now), [&upstreams[0], &upstreams[2]]);
        assert_eq!(e.count(now), 1);
        e.failed("a", now);
        e.failed("c", now);
        assert_eq!(e.eligible(&upstreams, now).len(), 3);
        assert_eq!(e.count(now), 3);
        assert_eq!(e.eligible(&upstreams, after(now, 10)).len(), 3);
        assert_eq!(e.count(after(now, 10)), 0);
    }
}

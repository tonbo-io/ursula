//! Passive ejection of upstreams that fail at the transport (#454).
//!
//! A voter whose host is gone (powered off, terminated, partitioned) answers
//! no connection attempt, and a pooled connection to it fails only after the
//! upstream client's bounds. Once a request to an upstream fails at the
//! transport, the gateway leaves that upstream out of selection for a window:
//! 10 s, doubled for each failed probe up to 60 s.
//!
//! When the window expires, one request may probe the upstream: the first one
//! that selects it, at random or through a cached route. Until that probe
//! answers or fails, every other request keeps skipping the upstream. A probe
//! that does neither within a base window (its client went away, say) no
//! longer holds the others back, and another request may probe. Any response
//! from an upstream, whatever its status, clears its ejection. If every
//! upstream is ejected or being probed, selection falls back to all of them
//! rather than to none.
//!
//! Failures of requests already in flight when an upstream was ejected do not
//! lengthen its window: only a failure after the window can.

use std::collections::HashMap;
use std::sync::Mutex;
use std::sync::PoisonError;
use std::time::Duration;

use rand::seq::IndexedRandom;
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
    /// When the request probing the upstream after `until` was sent.
    probe: Option<Instant>,
}

impl Ejection {
    /// Whether requests at `now` skip the upstream: inside its window, or
    /// while a probe sent less than `base` ago is outstanding.
    fn excludes(&self, now: Instant, base: Duration) -> bool {
        now < self.until
            || self
                .probe
                .is_some_and(|sent| now < sent.checked_add(base).unwrap_or(sent))
    }
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
            probe: None,
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

    /// Whether a request at `now` may go to `upstream`: it is not ejected, or
    /// its window expired and nobody is probing it, in which case this
    /// request becomes the probe.
    pub(crate) fn admit(&self, upstream: &str, now: Instant) -> bool {
        let mut ejected = self.ejected.lock().unwrap_or_else(PoisonError::into_inner);
        match ejected.get_mut(upstream) {
            None => true,
            Some(e) if e.excludes(now, self.base) => false,
            Some(e) => {
                e.probe = Some(now);
                true
            }
        }
    }

    /// Picks an upstream for a request at `now`, at random among those it
    /// may go to (claiming the probe of an expired one it picks), or among
    /// all of them when every one is excluded.
    pub(crate) fn pick<'a>(
        &self,
        upstreams: &'a [String],
        now: Instant,
        rng: &mut impl rand::Rng,
    ) -> Option<&'a String> {
        let mut ejected = self.ejected.lock().unwrap_or_else(PoisonError::into_inner);
        let open: Vec<&String> = upstreams
            .iter()
            .filter(|u| {
                !ejected
                    .get(u.as_str())
                    .is_some_and(|e| e.excludes(now, self.base))
            })
            .collect();
        let Some(&picked) = open.choose(rng) else {
            return upstreams.choose(rng);
        };
        if let Some(e) = ejected.get_mut(picked.as_str()) {
            e.probe = Some(now);
        }
        Some(picked)
    }

    /// Upstreams requests skip at `now`.
    pub(crate) fn count(&self, now: Instant) -> usize {
        self.ejected
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .values()
            .filter(|e| e.excludes(now, self.base))
            .count()
    }
}

/// The time ejection windows run on: the runtime's clock, or in tests one
/// that only moves when told to.
#[derive(Clone, Debug, Default)]
pub(crate) struct Clock {
    #[cfg(test)]
    manual: Option<std::sync::Arc<Mutex<Instant>>>,
}

impl Clock {
    #[cfg(not(test))]
    pub(crate) fn now(&self) -> Instant {
        Instant::now()
    }

    #[cfg(test)]
    pub(crate) fn now(&self) -> Instant {
        self.manual.as_ref().map_or_else(Instant::now, |manual| {
            *manual.lock().unwrap_or_else(PoisonError::into_inner)
        })
    }

    /// A clock that starts now and moves only by [`Clock::advance`].
    #[cfg(test)]
    pub(crate) fn manual() -> Self {
        Self {
            manual: Some(std::sync::Arc::new(Mutex::new(Instant::now()))),
        }
    }

    #[cfg(test)]
    pub(crate) fn advance(&self, by: Duration) {
        if let Some(manual) = &self.manual {
            let mut now = manual.lock().unwrap_or_else(PoisonError::into_inner);
            *now = now.checked_add(by).unwrap();
        }
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
            assert!(!e.admit("a", now));
            now = after(now, window);
            assert!(e.admit("a", now));
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
        assert_eq!(e.failed("a", after(now, 10)), Failed::Ejected {
            window: Duration::from_secs(20),
            failures: 2
        });
    }

    // After the window one request probes; the rest skip the upstream until
    // the probe resolves, or until it has been outstanding for a base window.
    #[test]
    fn one_request_probes_an_expired_upstream_at_a_time() {
        let e = ejections();
        let now = Instant::now();
        e.failed("a", now);
        let expired = after(now, 10);
        assert!(e.admit("a", expired));
        assert!(!e.admit("a", expired));
        assert!(!e.admit("a", after(now, 19)));
        assert_eq!(e.count(after(now, 19)), 1);
        assert!(e.admit("a", after(now, 20)));
        assert!(!e.admit("a", after(now, 20)));

        let upstreams = ["a", "b"].map(String::from);
        let mut rng = rand::rng();
        for _ in 0..50 {
            assert_eq!(
                e.pick(&upstreams, after(now, 21), &mut rng),
                Some(&upstreams[1])
            );
        }
        assert!(e.answered("a"));
        assert!(e.admit("a", after(now, 21)));
    }

    #[test]
    fn selection_skips_excluded_upstreams_but_never_empties() {
        let e = ejections();
        let upstreams = ["a", "b", "c"].map(String::from);
        let now = Instant::now();
        let mut rng = rand::rng();
        e.failed("b", now);
        for _ in 0..50 {
            assert_ne!(e.pick(&upstreams, now, &mut rng), Some(&upstreams[1]));
        }
        assert_eq!(e.count(now), 1);
        e.failed("a", now);
        e.failed("c", now);
        assert_eq!(e.count(now), 3);
        assert!(e.pick(&upstreams, now, &mut rng).is_some());
        assert_eq!(e.count(after(now, 10)), 0);
    }
}

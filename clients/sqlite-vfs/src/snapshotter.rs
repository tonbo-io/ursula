//! What the snapshot thread of a bound database keeps from 0.7 for PR7: its wake-up ([`Snapper`])
//! and its backoff after failed attempts ([`SnapshotFailing`]). The 0.7 attempt and its read-back
//! are gone; PR7 publishes snapshots keyed by offset and digest (design §4.11).

use std::collections::hash_map::RandomState;
use std::hash::BuildHasher;
use std::hash::Hasher;
use std::sync::Condvar;
use std::sync::Mutex;
use std::sync::PoisonError;
use std::time::Duration;
use std::time::Instant;

/// Wakes a bound database's snapshot thread.
#[derive(Debug, Default)]
pub(crate) struct Snapper {
    /// (requested, stopped)
    state: Mutex<(bool, bool)>,
    cv: Condvar,
}

impl Snapper {
    pub(crate) fn request(&self) {
        self.state.lock().unwrap_or_else(PoisonError::into_inner).0 = true;
        self.cv.notify_one();
    }

    pub(crate) fn stopped(&self) -> bool {
        self.state.lock().unwrap_or_else(PoisonError::into_inner).1
    }

    pub(crate) fn stop(&self) {
        self.state.lock().unwrap_or_else(PoisonError::into_inner).1 = true;
        self.cv.notify_one();
    }

    /// Waits for a request (true) or the stop (false).
    pub(crate) fn wait(&self) -> bool {
        let mut s = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        loop {
            if s.1 {
                return false;
            }
            if std::mem::take(&mut s.0) {
                return true;
            }
            s = self.cv.wait(s).unwrap_or_else(PoisonError::into_inner);
        }
    }

    /// Waits for a request or `deadline` (true), or the stop (false), whichever comes first.
    pub(crate) fn wait_until(&self, deadline: Instant) -> bool {
        let mut s = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        loop {
            if s.1 {
                return false;
            }
            if std::mem::take(&mut s.0) {
                return true;
            }
            let left = deadline.saturating_duration_since(Instant::now());
            if left.is_zero() {
                return true;
            }
            s = self
                .cv
                .wait_timeout(s, left)
                .unwrap_or_else(PoisonError::into_inner)
                .0;
        }
    }
}

/// The wait after a first failed attempt.
const FIRST_RETRY: Duration = Duration::from_millis(100);

/// The longest wait between failed attempts: an hour while the body is too large for a layer in
/// front of the stream, which fails the same way until the database shrinks, and five minutes
/// after any other failure, so that a snapshot follows soon after its cause goes away.
const SIZE_RETRY_MAX: Duration = Duration::from_secs(3600);
const RETRY_MAX: Duration = Duration::from_secs(300);

/// What a failed snapshot attempt failed on, for the wait before the next one.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum FailureKind {
    /// The body is over a size limit in front of the stream.
    Size,
    /// No answer, or a 5xx: the server or a layer in front of it is unavailable.
    Outage,
    /// Anything else.
    Other,
}

impl FailureKind {
    fn retry_max(self) -> Duration {
        match self {
            FailureKind::Size => SIZE_RETRY_MAX,
            FailureKind::Outage | FailureKind::Other => RETRY_MAX,
        }
    }

    /// What the `snapshot_failed` log calls it.
    pub(crate) fn name(self) -> &'static str {
        match self {
            FailureKind::Size => "size",
            FailureKind::Outage => "outage",
            FailureKind::Other => "other",
        }
    }
}

/// Snapshot attempts that failed in a row, since the last one that published.
///
/// Each failure doubles the wait before the next attempt, from 100 ms up to the kind's limit, give
/// or take a fifth, so the owners a cluster-wide outage failed together do not try again together.
/// An attempt does not wait once the database is below three quarters of the image that last
/// failed. After an outage, the first acknowledged commit brings the next attempt forward, once a
/// run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SnapshotFailing {
    /// How many failed in a row.
    pub(crate) count: u32,
    /// Why the last one failed.
    pub(crate) error: String,
    pub(crate) kind: FailureKind,
    /// The size, in pages, of the image the last failure took.
    pub(crate) pages: u32,
    /// The wait after the last failure, before the jitter.
    pub(crate) wait: Duration,
    /// The next attempt starts no sooner, unless the database shrank (`holds_back`).
    pub(crate) until: Instant,
    /// A commit brought the next attempt forward in this run already.
    pub(crate) brought_forward: bool,
}

impl SnapshotFailing {
    /// The run after one more failure at `now`, after `last` (`None`: the first of a run). It
    /// waits at least `retry_after`, what the stream asked for.
    pub(crate) fn failed(
        last: Option<&SnapshotFailing>,
        kind: FailureKind,
        error: String,
        pages: u32,
        retry_after: Option<Duration>,
        now: Instant,
    ) -> SnapshotFailing {
        let first = FIRST_RETRY.min(kind.retry_max());
        let (count, wait, brought_forward) = match last {
            Some(last) => (
                last.count.saturating_add(1),
                last.wait.saturating_mul(2).clamp(first, kind.retry_max()),
                last.brought_forward,
            ),
            None => (1, first, false),
        };
        let until = [Some(jittered(wait)), retry_after]
            .into_iter()
            .flatten()
            .max()
            .and_then(|after| now.checked_add(after))
            .unwrap_or(now);
        SnapshotFailing {
            count,
            error,
            kind,
            pages,
            wait,
            until,
            brought_forward,
        }
    }

    /// Whether an attempt at `now`, with the database at `pages`, waits: until `until`, unless it
    /// is below three quarters of the image that last failed.
    pub(crate) fn holds_back(&self, pages: u32, now: Instant) -> bool {
        let shrank = u64::from(pages).saturating_mul(4) < u64::from(self.pages).saturating_mul(3);
        now < self.until && !shrank
    }

    /// A commit was acknowledged at `now`: after an outage the next attempt moves to a random
    /// point within `BROUGHT_FORWARD_SPREAD`, once a run. Whether it moved.
    pub(crate) fn commit_acknowledged(&mut self, now: Instant) -> bool {
        if self.kind != FailureKind::Outage || self.brought_forward || now >= self.until {
            return false;
        }
        let spread = self
            .until
            .saturating_duration_since(now)
            .min(BROUGHT_FORWARD_SPREAD);
        self.until = now
            .checked_add(spread.mul_f64(random_fraction()))
            .unwrap_or(now);
        self.brought_forward = true;
        true
    }
}

/// How far after the commit that brings it forward an attempt may start.
const BROUGHT_FORWARD_SPREAD: Duration = Duration::from_secs(30);

/// A random number from 0 to 1, from the standard library's randomly keyed hasher.
fn random_fraction() -> f64 {
    RandomState::new().build_hasher().finish() as f64 / u64::MAX as f64
}

/// `wait` give or take a fifth.
fn jittered(wait: Duration) -> Duration {
    wait.mul_f64(0.8 + 0.4 * random_fraction())
}

#[cfg(test)]
mod tests {
    use std::time::Duration;
    use std::time::Instant;

    use super::BROUGHT_FORWARD_SPREAD;
    use super::FailureKind;
    use super::RETRY_MAX;
    use super::SIZE_RETRY_MAX;
    use super::Snapper;
    use super::SnapshotFailing;
    use super::jittered;

    fn run(kind: FailureKind, n: usize, pages: u32, now: Instant) -> SnapshotFailing {
        let mut last: Option<SnapshotFailing> = None;
        for _ in 0..n {
            last = Some(SnapshotFailing::failed(
                last.as_ref(),
                kind,
                "e".into(),
                pages,
                None,
                now,
            ));
        }
        last.unwrap()
    }

    // Each failure doubles the wait, up to an hour for a body too large and five minutes
    // otherwise, and never less than the stream asked for.
    #[test]
    fn failures_double_the_wait_up_to_their_kinds_limit() {
        let now = Instant::now();
        assert_eq!(
            run(FailureKind::Other, 1, 0, now).wait,
            Duration::from_millis(100)
        );
        assert_eq!(
            run(FailureKind::Other, 3, 0, now).wait,
            Duration::from_millis(400)
        );
        assert_eq!(run(FailureKind::Other, 30, 0, now).wait, RETRY_MAX);
        assert_eq!(run(FailureKind::Size, 30, 0, now).wait, SIZE_RETRY_MAX);
        let asked = SnapshotFailing::failed(
            None,
            FailureKind::Outage,
            "e".into(),
            0,
            Some(Duration::from_secs(9)),
            now,
        );
        assert!(asked.until >= now + Duration::from_secs(9));
        assert_eq!(
            [FailureKind::Size, FailureKind::Outage, FailureKind::Other].map(FailureKind::name),
            ["size", "outage", "other"]
        );
    }

    #[test]
    fn a_failure_holds_the_next_attempt_back_until_its_wait_ends_or_the_database_shrinks() {
        let now = Instant::now();
        let failing = run(FailureKind::Size, 1, 400, now);
        for pages in [400, 401, 4000, 300] {
            assert!(failing.holds_back(pages, now), "{pages}");
        }
        assert!(!failing.holds_back(299, now));
        assert!(!failing.holds_back(400, failing.until));
    }

    // After an outage, the first commit acknowledged brings the next attempt forward, once a run,
    // and not after a failure about the body's size or anything else.
    #[test]
    fn a_commit_after_an_outage_brings_the_next_attempt_forward_once() {
        let now = Instant::now();
        let mut failing = run(FailureKind::Outage, 12, 0, now);
        assert!(failing.until.duration_since(now) > BROUGHT_FORWARD_SPREAD);
        assert!(failing.commit_acknowledged(now));
        assert!(failing.until.duration_since(now) <= BROUGHT_FORWARD_SPREAD);
        assert!(!failing.commit_acknowledged(now));
        let next = SnapshotFailing::failed(
            Some(&failing),
            FailureKind::Outage,
            "e".into(),
            0,
            None,
            now,
        );
        assert!(next.brought_forward);
        assert!(!run(FailureKind::Size, 1, 0, now).commit_acknowledged(now));
        assert!(!run(FailureKind::Other, 1, 0, now).commit_acknowledged(now));
    }

    #[test]
    fn waits_are_spread_by_a_fifth() {
        let wait = Duration::from_secs(10);
        let spread: Vec<Duration> = (0..200).map(|_| jittered(wait)).collect();
        assert!(
            spread
                .iter()
                .all(|d| *d >= wait.mul_f64(0.8) && *d <= wait.mul_f64(1.2))
        );
        assert!(spread.iter().any(|d| *d < wait.mul_f64(0.95)));
        assert!(spread.iter().any(|d| *d > wait.mul_f64(1.05)));
    }

    #[test]
    fn a_wait_until_ends_at_a_request_its_deadline_or_the_stop() {
        let snapper = Snapper::default();
        let later = Instant::now() + Duration::from_secs(3600);
        snapper.request();
        assert!(snapper.wait_until(later));
        let started = Instant::now();
        assert!(snapper.wait_until(started + Duration::from_millis(20)));
        assert!(started.elapsed() >= Duration::from_millis(20));
        snapper.stop();
        assert!(!snapper.wait_until(later));
        assert!(!snapper.wait());
        assert!(snapper.stopped());
    }
}

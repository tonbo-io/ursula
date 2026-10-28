//! The schedule of an API key's exchanges (design: Transport): when the next exchange goes out,
//! and what a token, a failure or a `401` changes. Pure: time comes in as `now_ms` and randomness
//! as a [`Jitter`], and the refresher thread of `auth` carries the schedule out.
//!
//! - A token is refreshed after 70 to 80% of its lifetime, and at least [`MIN_REFRESH_MS`] after
//!   it arrived. A token that lives shorter than that counts as a failed exchange.
//! - A failed exchange is retried after `min(30 s, 1 s * 2^(n-1))`, jittered down to half, for the
//!   `n`th failure in a row, and never sooner than a `429`'s `Retry-After` (up to
//!   [`RETRY_AFTER_CAP_MS`]). The last token is kept meanwhile, expired or not: a request with a
//!   stale token is refused with a `401` and retried, one without a token would be taken for a
//!   missing stream. A host waiting for a token does not cut the backoff short.
//! - A failure another try cannot fix (the key refused, an answer the exchange cannot use) stops
//!   the schedule: no exchange goes out until a host uses the key again ([`Schedule::wanted`]).
//! - A `401` to the token held brings its refresh forward to at once, still at least
//!   [`MIN_REFRESH_MS`] after it arrived, and the token no longer counts as valid. Tokens refused
//!   one after another back off as failed exchanges do, and a token that served until its refresh
//!   ends the run. A `401` while an exchange is out, a retry is scheduled or the schedule stopped
//!   changes nothing: one exchange at a time.
//! - Exchanges go out only while the source is active (`auth`: an open, a bound database or an
//!   import may send requests with the key's token, or a host waits for it). An inactive source
//!   keeps its schedule, and a refresh that came due meanwhile goes out once it is active again.

use super::exchange::ExchangeError;
use super::exchange::Exchanged;

/// The earliest refresh after a token arrived, and so the shortest lifetime a token may have.
pub(crate) const MIN_REFRESH_MS: u64 = 1_000;
/// The first retry after a failed exchange, doubled for each failure in a row up to
/// [`MAX_RETRY_MS`], then jittered down to half.
const FIRST_RETRY_MS: u64 = 1_000;
const MAX_RETRY_MS: u64 = 30_000;
/// The longest `Retry-After` a retry waits for.
const RETRY_AFTER_CAP_MS: u64 = 60_000;

/// A random draw, in thousandths: `0..=1000`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Jitter(u64);

impl Jitter {
    pub(crate) const MAX: u64 = 1000;

    /// `permille` in thousandths, at most [`Jitter::MAX`].
    pub(crate) fn new(permille: u64) -> Self {
        Self(permille.min(Self::MAX))
    }
}

/// When a token of `lifetime_ms` is refreshed after it arrived: at 70% of its lifetime, plus up to
/// 10% more with `jitter`, and at least [`MIN_REFRESH_MS`].
fn refresh_after(lifetime_ms: u64, jitter: Jitter) -> u64 {
    let permille = 700_u64.saturating_add(jitter.0 / 10);
    let at = lifetime_ms.saturating_mul(permille) / 1000;
    at.max(MIN_REFRESH_MS)
}

/// The wait before the next exchange after `failures` failures in a row: `min(30 s, 1 s *
/// 2^(n-1))`, jittered down to half.
fn retry_after(failures: u32, jitter: Jitter) -> u64 {
    let doublings = failures.saturating_sub(1).min(16);
    let span = FIRST_RETRY_MS
        .saturating_mul(1_u64 << doublings)
        .min(MAX_RETRY_MS);
    let permille = 500_u64.saturating_add(jitter.0 / 2);
    span.saturating_mul(permille) / 1000
}

/// The token the source holds.
#[derive(Clone, PartialEq, Eq)]
struct Held {
    token: String,
    arrived_ms: u64,
    expires_ms: u64,
    /// A request was refused `401` under it.
    refused: bool,
}

impl std::fmt::Debug for Held {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Held")
            .field("arrived_ms", &self.arrived_ms)
            .field("expires_ms", &self.expires_ms)
            .field("refused", &self.refused)
            .finish_non_exhaustive()
    }
}

/// What the source does next.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    /// Exchange at `at_ms`, once active.
    Due { at_ms: u64 },
    /// An exchange is out.
    Exchanging,
    /// The last exchange failed in a way another try cannot fix: none goes out until a host uses
    /// the key again.
    Stopped,
}

/// Exchanges failed in a row, and the last failure.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Failing {
    count: u32,
    last: ExchangeError,
}

/// One key's schedule.
#[derive(Debug, Clone)]
pub(crate) struct Schedule {
    held: Option<Held>,
    phase: Phase,
    failing: Option<Failing>,
    /// Tokens refused `401` one after another, each before its refresh came due.
    refusals: u32,
    /// Exchanges that ended, with a token or without.
    ended: u64,
}

/// Where the source stands, for `ursula_status`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum KeyState {
    /// No token yet, and no exchange out.
    Pending,
    Exchanging,
    /// A token that has not expired, and no failure since it arrived.
    Fresh,
    /// The token held expired while the source was inactive.
    Expired,
    /// The last exchange failed: its retry is scheduled, and the last token (if any) serves.
    Failing,
    /// The last exchange failed in a way another try cannot fix: no exchange goes out until a
    /// host uses the key again, and the last token (if any) serves.
    Stopped,
}

impl KeyState {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            KeyState::Pending => "pending",
            KeyState::Exchanging => "exchanging",
            KeyState::Fresh => "fresh",
            KeyState::Expired => "expired",
            KeyState::Failing => "failing",
            KeyState::Stopped => "stopped",
        }
    }
}

/// A schedule as `ursula_status` shows it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct KeyView {
    pub(crate) state: KeyState,
    /// Until the token held expires (0 once it has), `None` without one.
    pub(crate) expires_in_ms: Option<u64>,
    /// Until the next exchange is due (0 once it is), `None` while one is out or the schedule
    /// stopped.
    pub(crate) refresh_in_ms: Option<u64>,
    /// Exchanges failed in a row.
    pub(crate) failures: u32,
    /// The last failure, until an exchange succeeds.
    pub(crate) error: Option<ExchangeError>,
}

impl Schedule {
    /// A new source: its first exchange is due at once.
    pub(crate) fn new(now_ms: u64) -> Self {
        Self {
            held: None,
            phase: Phase::Due { at_ms: now_ms },
            failing: None,
            refusals: 0,
            ended: 0,
        }
    }

    /// The token requests carry: the last one handed out, kept while exchanges fail.
    pub(crate) fn token(&self) -> Option<&str> {
        self.held.as_ref().map(|h| h.token.as_str())
    }

    /// Whether a token is held that has not expired and was not refused `401`.
    pub(crate) fn valid(&self, now_ms: u64) -> bool {
        self.held
            .as_ref()
            .is_some_and(|h| h.expires_ms > now_ms && !h.refused)
    }

    /// Whether requests can carry a token of the key: one is held, and the key's exchanges did not
    /// fail for good.
    pub(crate) fn serves(&self) -> bool {
        self.held.is_some() && self.phase != Phase::Stopped
    }

    /// Exchanges that ended so far.
    pub(crate) fn ended(&self) -> u64 {
        self.ended
    }

    /// The last failure, until an exchange succeeds.
    pub(crate) fn last_failure(&self) -> Option<&ExchangeError> {
        self.failing.as_ref().map(|f| &f.last)
    }

    /// When the next exchange goes out, while `active`.
    pub(crate) fn deadline(&self, active: bool) -> Option<u64> {
        match self.phase {
            Phase::Due { at_ms } if active => Some(at_ms),
            Phase::Due { .. } | Phase::Exchanging | Phase::Stopped => None,
        }
    }

    /// Starts the exchange that is due, while `active`: true when it goes out now, and then no
    /// other until it [`Schedule::exchanged`].
    pub(crate) fn start(&mut self, now_ms: u64, active: bool) -> bool {
        match self.phase {
            Phase::Due { at_ms } if active && at_ms <= now_ms => {
                // A token that served until its refresh without a 401 ends a run of refusals.
                if self.held.as_ref().is_some_and(|h| !h.refused) {
                    self.refusals = 0;
                }
                self.phase = Phase::Exchanging;
                true
            }
            Phase::Due { .. } | Phase::Exchanging | Phase::Stopped => false,
        }
    }

    /// The exchange out ended with `outcome`. Returns whether the token held changed.
    pub(crate) fn exchanged(
        &mut self,
        outcome: Result<Exchanged, ExchangeError>,
        now_ms: u64,
        jitter: Jitter,
    ) -> bool {
        self.ended = self.ended.saturating_add(1);
        let outcome = outcome.and_then(|got| {
            if got.lifetime_ms < MIN_REFRESH_MS {
                Err(ExchangeError::ShortLived {
                    lifetime_ms: got.lifetime_ms,
                    min_ms: MIN_REFRESH_MS,
                })
            } else {
                Ok(got)
            }
        });
        match outcome {
            Ok(Exchanged { token, lifetime_ms }) => {
                let changed = self.token() != Some(token.as_str());
                self.held = Some(Held {
                    token,
                    arrived_ms: now_ms,
                    expires_ms: now_ms.saturating_add(lifetime_ms),
                    refused: false,
                });
                self.failing = None;
                self.phase = Phase::Due {
                    at_ms: now_ms.saturating_add(refresh_after(lifetime_ms, jitter)),
                };
                changed
            }
            Err(last) => {
                let count = self
                    .failing
                    .as_ref()
                    .map_or(1, |f| f.count.saturating_add(1));
                self.phase = if last.permanent() {
                    Phase::Stopped
                } else {
                    let asked = last
                        .retry_after_ms()
                        .map_or(0, |ms| ms.min(RETRY_AFTER_CAP_MS));
                    Phase::Due {
                        at_ms: now_ms.saturating_add(retry_after(count, jitter).max(asked)),
                    }
                };
                self.failing = Some(Failing { count, last });
                false
            }
        }
    }

    /// A request carrying `token` was refused `401`. Returns whether the next exchange moved
    /// earlier.
    pub(crate) fn refused(&mut self, token: &str, now_ms: u64, jitter: Jitter) -> bool {
        // A retry is scheduled, an exchange is out, or the schedule stopped: the first two bring
        // the next token, and a host's next use restarts the last.
        if self.failing.is_some() {
            return false;
        }
        let Phase::Due { at_ms } = self.phase else {
            return false;
        };
        let Some(held) = self
            .held
            .as_mut()
            .filter(|h| h.token == token && !h.refused)
        else {
            return false;
        };
        held.refused = true;
        self.refusals = self.refusals.saturating_add(1);
        let earliest = now_ms.max(held.arrived_ms.saturating_add(MIN_REFRESH_MS));
        let backoff = match self.refusals {
            0 | 1 => 0,
            n => retry_after(n.saturating_sub(1), jitter),
        };
        let at = earliest.saturating_add(backoff);
        if at < at_ms {
            self.phase = Phase::Due { at_ms: at };
            true
        } else {
            false
        }
    }

    /// A host uses the key: a schedule that stopped on a failure another try could not fix
    /// starts again at once, since the host may have fixed it. A retry after a passing failure
    /// keeps its time, so hosts that use the key over and over do not cut the backoff short.
    pub(crate) fn wanted(&mut self, now_ms: u64) {
        if self.phase == Phase::Stopped {
            self.phase = Phase::Due { at_ms: now_ms };
        }
    }

    pub(crate) fn view(&self, now_ms: u64) -> KeyView {
        let state = match (&self.phase, &self.failing, &self.held) {
            (Phase::Exchanging, ..) => KeyState::Exchanging,
            (Phase::Stopped, ..) => KeyState::Stopped,
            (_, Some(_), _) => KeyState::Failing,
            (_, None, None) => KeyState::Pending,
            (_, None, Some(h)) if h.expires_ms > now_ms => KeyState::Fresh,
            (_, None, Some(_)) => KeyState::Expired,
        };
        KeyView {
            state,
            expires_in_ms: self
                .held
                .as_ref()
                .map(|h| h.expires_ms.saturating_sub(now_ms)),
            refresh_in_ms: match self.phase {
                Phase::Due { at_ms } => Some(at_ms.saturating_sub(now_ms)),
                Phase::Exchanging | Phase::Stopped => None,
            },
            failures: self.failing.as_ref().map_or(0, |f| f.count),
            error: self.last_failure().cloned(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::ExchangeError;
    use super::Exchanged;
    use super::Jitter;
    use super::KeyState;
    use super::MIN_REFRESH_MS;
    use super::Schedule;
    use super::refresh_after;
    use super::retry_after;

    const LOW: Jitter = Jitter(0);
    const HIGH: Jitter = Jitter(Jitter::MAX);

    fn token(token: &str, lifetime_ms: u64) -> Result<Exchanged, ExchangeError> {
        Ok(Exchanged {
            token: token.to_owned(),
            lifetime_ms,
        })
    }

    fn down() -> Result<Exchanged, ExchangeError> {
        Err(ExchangeError::Unavailable { status: 503 })
    }

    // A refresh is due after 70 to 80% of the lifetime, at least 1 s after the token arrived.
    #[test]
    fn a_refresh_is_due_at_70_to_80_percent_of_the_lifetime() {
        assert_eq!(refresh_after(100_000, LOW), 70_000);
        assert_eq!(refresh_after(100_000, HIGH), 80_000);
        assert_eq!(refresh_after(100_000, Jitter::new(500)), 75_000);
        assert_eq!(refresh_after(1_000, HIGH), MIN_REFRESH_MS);
        assert_eq!(refresh_after(u64::MAX, HIGH), u64::MAX / 1000);
        assert_eq!(Jitter::new(5_000), HIGH);
    }

    // Failures back off from 1 s to 30 s, doubling, jittered down to half.
    #[test]
    fn failures_back_off_from_1_s_to_30_s_jittered_down_to_half() {
        let spans: Vec<u64> = (1..=7).map(|n| retry_after(n, HIGH)).collect();
        assert_eq!(spans, [1_000, 2_000, 4_000, 8_000, 16_000, 30_000, 30_000]);
        assert_eq!(retry_after(1, LOW), 500);
        assert_eq!(retry_after(6, LOW), 15_000);
        assert_eq!(retry_after(u32::MAX, HIGH), 30_000);
    }

    // The first exchange goes out at once once active, one at a time, and its token is
    // refreshed on schedule.
    #[test]
    fn exchanges_go_out_one_at_a_time_while_active_and_refresh_on_schedule() {
        let mut s = Schedule::new(0);
        assert_eq!(s.view(0).state, KeyState::Pending);
        assert_eq!(s.deadline(false), None, "inactive: nothing goes out");
        assert!(!s.start(10, false));
        assert_eq!(s.deadline(true), Some(0));
        assert!(s.start(10, true));
        assert!(!s.start(10, true), "one exchange at a time");
        assert_eq!(s.deadline(true), None);
        assert_eq!(s.view(10).state, KeyState::Exchanging);

        assert!(s.exchanged(token("t1", 100_000), 20, LOW));
        assert_eq!(s.token(), Some("t1"));
        assert!(s.valid(20));
        assert_eq!(s.deadline(true), Some(70_020));
        let view = s.view(20);
        assert_eq!(view.state, KeyState::Fresh);
        assert_eq!(view.expires_in_ms, Some(100_000));
        assert_eq!(view.refresh_in_ms, Some(70_000));
        assert!(!s.start(70_019, true));
        assert!(s.start(70_020, true));
        assert!(
            !s.exchanged(token("t1", 100_000), 70_030, LOW),
            "the same token"
        );
        assert!(s.start(140_030, true));
        assert!(s.exchanged(token("t2", 100_000), 140_040, LOW));
        assert_eq!(s.token(), Some("t2"));
    }

    // A failed exchange keeps the last token, expired or not, and is retried with backoff until
    // one succeeds.
    #[test]
    fn a_failed_exchange_keeps_the_last_token_and_backs_off() {
        let mut s = Schedule::new(0);
        assert!(s.start(0, true));
        assert!(!s.exchanged(down(), 0, HIGH));
        assert_eq!(s.token(), None);
        assert_eq!(s.deadline(true), Some(1_000));
        assert!(s.start(1_000, true));
        assert!(s.exchanged(token("t1", 10_000), 1_000, LOW));
        assert!(s.start(8_000, true));
        assert!(!s.exchanged(down(), 8_000, HIGH));
        assert_eq!(s.deadline(true), Some(9_000), "the first failure in a row");
        assert!(s.start(9_000, true));
        let limited = ExchangeError::RateLimited {
            retry_after_ms: None,
        };
        assert!(!s.exchanged(Err(limited), 9_000, HIGH));
        assert_eq!(s.deadline(true), Some(11_000));
        assert!(s.start(11_000, true));
        assert!(!s.exchanged(down(), 11_000, HIGH));
        assert_eq!(s.token(), Some("t1"), "kept, though it expired at 11 000");
        assert!(!s.valid(11_000));
        let view = s.view(11_000);
        assert_eq!(view.state, KeyState::Failing);
        assert_eq!(view.failures, 3);
        assert_eq!(view.expires_in_ms, Some(0));
        assert_eq!(view.error, Some(ExchangeError::Unavailable { status: 503 }));
        assert_eq!(s.ended(), 5);
        assert!(s.start(15_000, true));
        assert!(s.exchanged(token("t2", 10_000), 15_000, LOW));
        assert_eq!(s.view(15_000).failures, 0);
        assert_eq!(s.last_failure(), None);
    }

    // A 429's Retry-After puts the retry later than the backoff would, up to a minute, never
    // sooner.
    #[test]
    fn a_429_is_retried_no_sooner_than_its_retry_after() {
        let limited = |retry_after_ms| {
            Err(ExchangeError::RateLimited {
                retry_after_ms: Some(retry_after_ms),
            })
        };
        let mut s = Schedule::new(0);
        assert!(s.start(0, true));
        assert!(!s.exchanged(limited(7_000), 0, HIGH));
        assert_eq!(s.deadline(true), Some(7_000), "longer than the backoff");
        assert!(s.start(7_000, true));
        assert!(!s.exchanged(limited(100), 7_000, HIGH));
        assert_eq!(s.deadline(true), Some(9_000), "shorter: the backoff");
        assert!(s.start(9_000, true));
        assert!(!s.exchanged(limited(3_600_000), 9_000, HIGH));
        assert_eq!(s.deadline(true), Some(69_000), "capped at a minute");
    }

    // A token that lives shorter than the earliest refresh counts as a failed exchange.
    #[test]
    fn a_token_shorter_lived_than_a_refresh_is_a_failure() {
        let mut s = Schedule::new(0);
        assert!(s.start(0, true));
        assert!(!s.exchanged(token("t1", 999), 0, HIGH));
        assert_eq!(s.token(), None);
        assert_eq!(
            s.last_failure(),
            Some(&ExchangeError::ShortLived {
                lifetime_ms: 999,
                min_ms: MIN_REFRESH_MS
            })
        );
        assert_eq!(s.deadline(true), Some(1_000));
    }

    // A 401 to the token held refreshes at once, but never sooner than 1 s after it arrived, and
    // only once per token. A 401 to another token, while an exchange is out or while a retry is
    // scheduled, changes nothing.
    #[test]
    fn a_401_refreshes_at_once_but_not_within_1_s_of_arrival() {
        let mut s = Schedule::new(0);
        assert!(s.start(0, true));
        assert!(s.exchanged(token("t1", 100_000), 0, LOW));
        assert!(!s.refused("other", 5_000, LOW), "another token");
        assert!(s.valid(5_000));
        assert!(s.refused("t1", 5_000, LOW));
        assert!(!s.valid(5_000), "a token refused 401 is no longer valid");
        assert_eq!(s.token(), Some("t1"), "but serves until the next one");
        assert_eq!(s.deadline(true), Some(5_000));
        assert!(!s.refused("t1", 5_000, LOW), "once per token");
        assert!(s.start(5_000, true));
        assert!(!s.refused("t1", 5_000, LOW), "an exchange is out");
        assert!(s.exchanged(token("t2", 100_000), 5_100, LOW));
        assert!(s.refused("t2", 5_200, LOW));
        assert_eq!(
            s.deadline(true),
            Some(6_600),
            "1 s after it arrived, backing off as the second refused in a row"
        );

        let mut failing = Schedule::new(0);
        assert!(failing.start(0, true));
        assert!(failing.exchanged(token("t1", 100_000), 0, LOW));
        assert!(failing.start(70_000, true));
        assert!(!failing.exchanged(down(), 70_000, HIGH));
        assert!(
            !failing.refused("t1", 70_500, LOW),
            "its retry is scheduled"
        );
        assert_eq!(failing.deadline(true), Some(71_000));
    }

    // Tokens refused one after another back off as failures do, and a token that serves until
    // its refresh ends the run.
    #[test]
    fn tokens_refused_in_a_row_back_off_until_one_serves_its_time() {
        let mut s = Schedule::new(0);
        let mut now = 0;
        let mut waits = Vec::new();
        for n in 0..5 {
            assert!(s.start(now, true));
            assert!(s.exchanged(token(&format!("t{n}"), 100_000), now, HIGH));
            assert!(s.refused(&format!("t{n}"), now, HIGH));
            let at = s.deadline(true).unwrap();
            waits.push(at.saturating_sub(now));
            now = at;
        }
        assert_eq!(waits, [1_000, 2_000, 3_000, 5_000, 9_000]);
        // This one serves until its refresh: the run ends.
        assert!(s.start(now, true));
        assert!(s.exchanged(token("ok", 100_000), now, HIGH));
        now = s.deadline(true).unwrap();
        assert!(s.start(now, true));
        assert!(s.exchanged(token("next", 100_000), now, HIGH));
        let later = now.saturating_add(5_000);
        assert!(s.refused("next", later, HIGH));
        assert_eq!(s.deadline(true), Some(later));
    }

    // An inactive source keeps its schedule: a refresh that came due goes out once active, and a
    // token that expired meanwhile shows as expired.
    #[test]
    fn an_inactive_source_refreshes_once_active_again() {
        let mut s = Schedule::new(0);
        assert!(s.start(0, true));
        assert!(s.exchanged(token("t1", 10_000), 0, LOW));
        assert!(!s.start(50_000, false));
        assert_eq!(s.view(50_000).state, KeyState::Expired);
        assert_eq!(s.view(50_000).refresh_in_ms, Some(0));
        assert!(s.start(50_000, true));
        assert!(s.exchanged(token("t2", 10_000), 50_000, LOW));
        assert_eq!(s.view(50_000).state, KeyState::Fresh);
    }

    // A failure another try cannot fix stops the schedule, until a host uses the key again. A
    // host's use leaves a passing failure's retry and a healthy schedule to their times.
    #[test]
    fn a_refused_key_stops_until_a_host_uses_it_again() {
        let mut s = Schedule::new(0);
        assert!(s.start(0, true));
        assert!(s.exchanged(token("t1", 100_000), 0, LOW));
        assert!(s.start(70_000, true));
        assert!(!s.exchanged(Err(ExchangeError::Refused { status: 401 }), 70_000, HIGH));
        assert_eq!(s.deadline(true), None, "stopped");
        assert!(!s.start(500_000, true));
        assert!(!s.refused("t1", 500_000, LOW));
        assert!(!s.serves(), "its token is no longer one requests can carry");
        let view = s.view(500_000);
        assert_eq!(view.state, KeyState::Stopped);
        assert_eq!(view.refresh_in_ms, None);
        assert_eq!(s.token(), Some("t1"), "kept");
        s.wanted(500_000);
        assert_eq!(s.deadline(true), Some(500_000));
        assert!(s.start(500_000, true));
        assert!(s.exchanged(token("t2", 100_000), 500_100, LOW));
        assert!(s.serves());
        s.wanted(500_200);
        assert_eq!(
            s.deadline(true),
            Some(570_100),
            "a healthy schedule keeps its time"
        );

        let mut passing = Schedule::new(0);
        assert!(passing.start(0, true));
        assert!(!passing.exchanged(down(), 0, HIGH));
        assert_eq!(passing.deadline(true), Some(1_000));
        passing.wanted(200);
        assert_eq!(passing.deadline(true), Some(1_000), "the backoff holds");
    }
}

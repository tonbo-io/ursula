//! The resolves in progress (design: Tonbo) as one pure table: which call waits for which resolve,
//! and which call takes its outcome. [`Table`] does no I/O, holds no lock and takes the time from
//! its caller. `tonbo` keeps it under a mutex and carries out what each transition answers:
//! starting a resolve's thread, waking the calls that wait, cancelling a resolve no call wants.
//!
//! A resolve is kept by the digest of its call's arguments, with the calls it owes an outcome: the
//! blocking calls waiting for it now (`waiting`), and whether a call that returned at once comes
//! back for it (`owed`), as the opens in progress count theirs (`db`).
//!
//! | resolve | call | answer |
//! | --- | --- | --- |
//! | none | any | start one |
//! | running | blocking | wait for it |
//! | running | returning at once | come back for it |
//! | done, a blocking call still to take it | any | its outcome |
//! | done, owed to a call that returned at once | returning at once | its outcome, owed no more |
//! | done, owed to a call that returned at once | blocking | start one, still owed to that call: a blocking call never comes back |
//!
//! - A resolve that ends with no call owed its outcome leaves the table. One whose last call took
//!   it leaves too, so an outcome goes only to calls made before it arrived, with two exceptions.
//!   A call made while blocking calls are still to take it takes it with them, and one that
//!   returns at once is then owed it no more. A call that returned at once and never came back nor
//!   cancelled leaves its outcome to the next call that returns at once, until [`UNTAKEN`] after
//!   it arrived ([`Table::sweep`]).
//! - A blocking call whose time runs out leaves the resolve to the other calls it owes, and gives
//!   it up only when it was the last ([`Table::give_up`]).
//! - `ursula_cancel(request)` is a call that returned at once and gives up: the resolve goes on
//!   while a blocking call waits for it, and otherwise it is given up ([`Table::cancel`]). A call
//!   that comes back for a resolve given up meanwhile starts one.

use std::collections::HashMap;
use std::time::Duration;
use std::time::Instant;

/// How long an outcome owed to a call that returned at once is kept for it.
pub(crate) const UNTAKEN: Duration = Duration::from_secs(60);

/// The digest of a call's arguments.
pub(crate) type Id = [u8; 32];

/// The resolves in progress and their outcomes still owed, for outcomes `O` and resolves `R` (what
/// the driver keeps of one: its cancel, its last condition).
pub(crate) struct Table<O, R> {
    entries: HashMap<Id, Entry<O, R>>,
}

struct Entry<O, R> {
    request: u64,
    run: R,
    /// Blocking calls waiting for it now.
    waiting: usize,
    /// A call returned at once and comes back for the outcome.
    owed: bool,
    phase: Phase<O>,
}

enum Phase<O> {
    Running,
    Done { outcome: O, at: Instant },
}

impl<O, R> Entry<O, R> {
    /// Whether no call is owed the outcome any more.
    fn unclaimed(&self) -> bool {
        self.waiting == 0 && !self.owed
    }
}

/// Where a call goes ([`Table::arrive`]).
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Arrival<O, R> {
    /// No resolve of these arguments is owed to anyone: start one ([`Table::start`]).
    Start,
    /// Wait for resolve `request` (`R`), which goes on for the call's time too: [`Table::poll`]
    /// once it ends, [`Table::give_up`] when the call's time runs out.
    Wait(u64, R),
    /// Return at once: the call comes back for resolve `request` (`R`), which goes on for the
    /// call's time too.
    Pending(u64, R),
    /// The outcome.
    Take(O),
}

/// What a blocking call finds of its resolve ([`Table::poll`]).
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Poll<O> {
    Wait,
    Take(O),
    /// It left the table: the call starts again.
    Gone,
}

/// What a blocking call whose time ran out does ([`Table::give_up`]).
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum GiveUp<O, R> {
    /// The outcome arrived as the time ran out.
    Take(O),
    /// Other calls are owed the resolve: it goes on for them.
    Leave(R),
    /// It was the last call: the resolve is given up, and left the table.
    Abandon(R),
}

/// What `ursula_cancel` of a resolve comes to ([`Table::cancel`]).
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Cancelled<O, R> {
    /// It had ended: its outcome.
    Ended(O),
    /// A blocking call waits for it: it goes on for that call.
    Left(R),
    /// No call is owed it any more: it is given up, and left the table.
    Abandoned(R),
}

impl<O, R> Default for Table<O, R> {
    fn default() -> Self {
        Self {
            entries: HashMap::new(),
        }
    }
}

impl<O: Clone, R: Clone> Table<O, R> {
    /// Drops the outcomes owed only to calls that returned at once and did not come back within
    /// [`UNTAKEN`] of `now`.
    pub(crate) fn sweep(&mut self, now: Instant) {
        self.entries.retain(|_, e| match &e.phase {
            Phase::Done { at, .. } => e.waiting > 0 || now.saturating_duration_since(*at) < UNTAKEN,
            Phase::Running => true,
        });
    }

    /// A call with the arguments `id`: `blocking` when it waits for the outcome.
    pub(crate) fn arrive(&mut self, id: Id, blocking: bool) -> Arrival<O, R> {
        let Some(entry) = self.entries.get_mut(&id) else {
            return Arrival::Start;
        };
        match &entry.phase {
            Phase::Running if blocking => {
                entry.waiting = entry.waiting.saturating_add(1);
                Arrival::Wait(entry.request, entry.run.clone())
            }
            Phase::Running => {
                entry.owed = true;
                Arrival::Pending(entry.request, entry.run.clone())
            }
            // Blocking calls about to take it: this one takes it with them, and a call that
            // returns at once is owed it no more, so it stays only for them.
            Phase::Done { outcome, .. } if entry.waiting > 0 => {
                let outcome = outcome.clone();
                if !blocking {
                    entry.owed = false;
                }
                Arrival::Take(outcome)
            }
            Phase::Done { .. } if blocking => Arrival::Start,
            Phase::Done { outcome, .. } => {
                let outcome = outcome.clone();
                entry.owed = false;
                if entry.unclaimed() {
                    self.entries.remove(&id);
                }
                Arrival::Take(outcome)
            }
        }
    }

    /// Resolve `request` (`run`) started for a call with the arguments `id` that [`Self::arrive`]
    /// told to start one, in place of an outcome owed to a call that returned at once: that call
    /// is owed the new resolve's outcome instead.
    pub(crate) fn start(&mut self, id: Id, request: u64, run: R, blocking: bool) {
        let owed = !blocking || self.entries.get(&id).is_some_and(|e| e.owed);
        self.entries.insert(id, Entry {
            request,
            run,
            waiting: usize::from(blocking),
            owed,
            phase: Phase::Running,
        });
    }

    /// Resolve `request` ended with `outcome` at `now`: kept for the calls owed it, or dropped
    /// when there are none (or it was given up). The driver says so with a `resolved` event either
    /// way.
    pub(crate) fn finish(&mut self, id: Id, request: u64, outcome: O, now: Instant) {
        let Some(entry) = self
            .entries
            .get_mut(&id)
            .filter(|e| e.request == request && matches!(e.phase, Phase::Running))
        else {
            return;
        };
        if entry.unclaimed() {
            self.entries.remove(&id);
        } else {
            entry.phase = Phase::Done { outcome, at: now };
        }
    }

    /// A blocking call waiting for resolve `request` looks again.
    pub(crate) fn poll(&mut self, id: Id, request: u64) -> Poll<O> {
        let Some(entry) = self.entries.get_mut(&id).filter(|e| e.request == request) else {
            return Poll::Gone;
        };
        match &entry.phase {
            Phase::Running => Poll::Wait,
            Phase::Done { outcome, .. } => {
                let outcome = outcome.clone();
                self.leave(id);
                Poll::Take(outcome)
            }
        }
    }

    /// A blocking call waiting for resolve `request` gives up: its time ran out.
    pub(crate) fn give_up(&mut self, id: Id, request: u64) -> Option<GiveUp<O, R>> {
        let entry = self.entries.get(&id).filter(|e| e.request == request)?;
        let run = entry.run.clone();
        if let Phase::Done { outcome, .. } = &entry.phase {
            let outcome = outcome.clone();
            self.leave(id);
            return Some(GiveUp::Take(outcome));
        }
        Some(if self.leave(id) {
            GiveUp::Abandon(run)
        } else {
            GiveUp::Leave(run)
        })
    }

    /// `ursula_cancel(request)`: `None` when `request` names no resolve here.
    pub(crate) fn cancel(&mut self, request: u64) -> Option<Cancelled<O, R>> {
        let (&id, entry) = self
            .entries
            .iter_mut()
            .find(|(_, e)| e.request == request)?;
        entry.owed = false;
        let unclaimed = entry.unclaimed();
        let step = match &entry.phase {
            Phase::Done { outcome, .. } => Cancelled::Ended(outcome.clone()),
            Phase::Running if unclaimed => Cancelled::Abandoned(entry.run.clone()),
            Phase::Running => Cancelled::Left(entry.run.clone()),
        };
        if unclaimed {
            self.entries.remove(&id);
        }
        Some(step)
    }

    /// One blocking call of `id` leaves: whether the entry left with it, the last.
    fn leave(&mut self, id: Id) -> bool {
        let Some(entry) = self.entries.get_mut(&id) else {
            return false;
        };
        entry.waiting = entry.waiting.saturating_sub(1);
        let last = entry.unclaimed();
        if last {
            self.entries.remove(&id);
        }
        last
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.entries.len()
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;
    use std::time::Instant;

    use super::Arrival;
    use super::Cancelled;
    use super::GiveUp;
    use super::Id;
    use super::Poll;
    use super::Table;
    use super::UNTAKEN;

    const A: Id = [1; 32];
    const B: Id = [2; 32];

    /// A table with resolve 7 of `A` started by a call, `blocking` or not.
    fn started(blocking: bool) -> Table<&'static str, &'static str> {
        let mut t = Table::default();
        assert_eq!(t.arrive(A, blocking), Arrival::Start);
        t.start(A, 7, "run", blocking);
        t
    }

    // Blocking calls with the same arguments join one resolve, and each takes its outcome: the
    // last one's take empties the table.
    #[test]
    fn every_blocking_call_takes_the_one_outcome() {
        let mut t = started(true);
        assert_eq!(t.arrive(A, true), Arrival::Wait(7, "run"));
        assert_eq!(t.arrive(B, true), Arrival::Start, "other arguments");
        assert_eq!(t.poll(A, 7), Poll::Wait);
        t.finish(A, 7, "url", Instant::now());
        assert_eq!(t.poll(A, 7), Poll::Take("url"));
        assert_eq!(t.len(), 1);
        assert_eq!(t.poll(A, 7), Poll::Take("url"));
        assert_eq!(t.len(), 0);
        assert_eq!(
            t.arrive(A, true),
            Arrival::Start,
            "the next call starts anew"
        );
    }

    // A blocking call whose time runs out leaves the resolve to the call that returned at once,
    // which takes its outcome. The last call to give up gives the resolve up.
    #[test]
    fn a_blocking_timeout_gives_up_only_a_resolve_no_other_call_wants() {
        let mut t = started(false);
        assert_eq!(t.arrive(A, true), Arrival::Wait(7, "run"));
        assert_eq!(t.give_up(A, 7), Some(GiveUp::Leave("run")));
        t.finish(A, 7, "url", Instant::now());
        assert_eq!(t.arrive(A, false), Arrival::Take("url"));
        assert_eq!(t.len(), 0);

        let mut t = started(true);
        assert_eq!(t.arrive(A, true), Arrival::Wait(7, "run"));
        assert_eq!(t.give_up(A, 7), Some(GiveUp::Leave("run")));
        assert_eq!(t.give_up(A, 7), Some(GiveUp::Abandon("run")));
        assert_eq!(t.len(), 0);
        t.finish(A, 7, "url", Instant::now());
        assert_eq!(t.len(), 0, "an abandoned resolve's outcome is dropped");

        // The outcome that arrived as the time ran out is taken.
        let mut t = started(true);
        t.finish(A, 7, "url", Instant::now());
        assert_eq!(t.give_up(A, 7), Some(GiveUp::Take("url")));
        assert_eq!(t.give_up(A, 7), None);
    }

    // A cancel gives a resolve up unless a blocking call waits for it, and reports the outcome of
    // one that ended. A call that comes back for a resolve given up starts one.
    #[test]
    fn a_cancel_leaves_a_resolve_a_blocking_call_waits_for() {
        let mut t = started(false);
        assert_eq!(t.arrive(A, true), Arrival::Wait(7, "run"));
        assert_eq!(t.cancel(7), Some(Cancelled::Left("run")));
        assert_eq!(t.cancel(8), None, "no such request");
        t.finish(A, 7, "url", Instant::now());
        assert_eq!(t.poll(A, 7), Poll::Take("url"));
        assert_eq!(t.len(), 0);

        let mut t = started(false);
        assert_eq!(
            t.arrive(A, false),
            Arrival::Pending(7, "run"),
            "a call joins"
        );
        assert_eq!(t.cancel(7), Some(Cancelled::Abandoned("run")));
        assert_eq!(t.cancel(7), None);
        assert_eq!(t.arrive(A, false), Arrival::Start);

        let mut t = started(false);
        t.finish(A, 7, "failed", Instant::now());
        assert_eq!(t.cancel(7), Some(Cancelled::Ended("failed")));
        assert_eq!(t.len(), 0);
    }

    // An outcome goes only to calls owed it: no call owed it drops it, a blocking call never
    // comes back for one, and one owed to a call that returned at once goes once.
    #[test]
    fn an_outcome_goes_only_to_calls_owed_it() {
        let mut t = started(true);
        assert_eq!(t.give_up(A, 7), Some(GiveUp::Abandon("run")));
        t.finish(A, 7, "not_found", Instant::now());
        assert_eq!(t.arrive(A, false), Arrival::Start);

        // The call that returned at once is owed the new resolve's outcome instead.
        let mut t = started(false);
        t.finish(A, 7, "not_found", Instant::now());
        assert_eq!(
            t.arrive(A, true),
            Arrival::Start,
            "a blocking call is a new one"
        );
        t.start(A, 9, "again", true);
        t.finish(A, 9, "url", Instant::now());
        assert_eq!(t.poll(A, 9), Poll::Take("url"));
        assert_eq!(t.len(), 1);
        assert_eq!(t.arrive(A, false), Arrival::Take("url"));
        assert_eq!(t.len(), 0);

        // A call that returns at once and takes an outcome blocking calls are still to take is
        // owed it no more: the next such call starts anew.
        let mut t = started(false);
        assert_eq!(t.arrive(A, true), Arrival::Wait(7, "run"));
        t.finish(A, 7, "not_found", Instant::now());
        assert_eq!(t.arrive(A, false), Arrival::Take("not_found"));
        assert_eq!(t.poll(A, 7), Poll::Take("not_found"));
        assert_eq!(t.len(), 0);
        assert_eq!(t.arrive(A, false), Arrival::Start);

        let mut t = started(false);
        assert_eq!(
            t.arrive(A, false),
            Arrival::Pending(7, "run"),
            "polled again"
        );
        t.finish(A, 7, "url", Instant::now());
        assert_eq!(t.arrive(A, false), Arrival::Take("url"));
        assert_eq!(t.arrive(A, false), Arrival::Start, "owed once");
    }

    // An outcome owed to a call that never came back goes a minute after it arrived. One a
    // blocking call is still to take stays.
    #[test]
    fn an_outcome_owed_to_no_call_that_comes_back_goes_after_a_minute() {
        let at = Instant::now();
        let mut t = started(false);
        t.finish(A, 7, "url", at);
        let later = |by: Duration| at.checked_add(by).unwrap();
        t.sweep(later(
            UNTAKEN.checked_sub(Duration::from_millis(1)).unwrap(),
        ));
        assert_eq!(t.len(), 1);
        t.sweep(later(UNTAKEN));
        assert_eq!(t.len(), 0);

        let mut t = started(true);
        t.finish(A, 7, "url", at);
        t.sweep(later(UNTAKEN));
        assert_eq!(t.poll(A, 7), Poll::Take("url"));
        assert_eq!(t.poll(A, 7), Poll::Gone);
    }
}

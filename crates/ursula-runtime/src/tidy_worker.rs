//! Leader-side tidy driver (bounded-stream-state F0).
//!
//! Expired streams, end records and idle streams with debt (receipts beyond
//! the window, idle producers, producers over the cap) keep their state
//! until something normalizes them. Every [`crate::expiry_sweep::interval`]
//! of `raft.deleted_data_max_age`, every group this node hosts samples its
//! retirement clock, and each group this node leads removes the streams
//! that expired in it and folds its due end records with `Sweep` entries,
//! at most [`crate::expiry_sweep::MAX_COMMANDS_PER_GROUP`] of them, for at most
//! [`crate::expiry_sweep::GROUP_BUDGET`]. A pass that stops a group at that
//! cap or budget is followed by another [`crate::expiry_sweep::MIN_INTERVAL`]
//! later, with the shorter [`crate::expiry_sweep::RERUN_BUDGET`], until the
//! backlog is gone. Every [`TIDY_INTERVAL`] it also proposes `TidyStream`
//! for at most [`TIDY_MAX_STREAMS_PER_GROUP`] streams with debt. Both repeat
//! on later passes until nothing remains. Each command does bounded work, so
//! the driver never stalls a group's apply.

use std::time::Duration;

use tokio::time::Instant;

use crate::ShardRuntime;
use crate::expiry_sweep;
use crate::request::TidyStreamsRequest;

/// Pause between `TidyStream` passes.
pub const TIDY_INTERVAL: Duration = Duration::from_secs(60);
/// `TidyStream` commands one pass proposes per group.
pub const TIDY_MAX_STREAMS_PER_GROUP: usize = 64;

/// Which group sweep a tick runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Sweep {
    /// The sweep every interval.
    Scheduled,
    /// The sweep a second after a pass that left a group capped, which
    /// drains its backlog.
    Rerun,
}

impl Sweep {
    /// How long the sweep may spend in one group.
    fn budget(self) -> Duration {
        match self {
            Self::Scheduled => expiry_sweep::GROUP_BUDGET,
            Self::Rerun => expiry_sweep::RERUN_BUDGET,
        }
    }
}

/// What one tick of the driver runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Tick {
    sweep: Option<Sweep>,
    tidy: bool,
}

/// When the driver's two passes are next due.
#[derive(Debug, Clone, Copy)]
struct Schedule {
    sweep_interval: Duration,
    next_sweep: Instant,
    /// The next sweep drains a capped group.
    rerun: bool,
    next_tidy: Instant,
}

impl Schedule {
    fn new(sweep_interval: Duration, now: Instant) -> Self {
        Self {
            sweep_interval,
            next_sweep: later(now, sweep_interval),
            rerun: false,
            next_tidy: later(now, TIDY_INTERVAL),
        }
    }

    /// When the next tick runs.
    fn next(&self) -> Instant {
        self.next_sweep.min(self.next_tidy)
    }

    /// The passes due at `now`, and the schedule after them.
    fn take(&mut self, now: Instant) -> Tick {
        let sweep = (now >= self.next_sweep).then_some(if self.rerun {
            Sweep::Rerun
        } else {
            Sweep::Scheduled
        });
        let tick = Tick {
            sweep,
            tidy: now >= self.next_tidy,
        };
        if tick.sweep.is_some() {
            self.next_sweep = later(now, self.sweep_interval);
            self.rerun = false;
        }
        if tick.tidy {
            self.next_tidy = later(now, TIDY_INTERVAL);
        }
        tick
    }

    /// Reschedules the sweep after a pass that ended at `now`: a pass that
    /// left a group `capped` (at its entry cap or time budget, with expired
    /// streams left) is followed by a re-run after
    /// [`expiry_sweep::MIN_INTERVAL`], every other one by the scheduled
    /// sweep after the interval.
    fn swept(&mut self, now: Instant, capped: bool) {
        let pause = if capped {
            expiry_sweep::MIN_INTERVAL
        } else {
            self.sweep_interval
        };
        self.next_sweep = later(now, pause);
        self.rerun = capped;
    }
}

/// `pause` after `now`; `now` itself past the clock's end, which no
/// interval here reaches.
fn later(now: Instant, pause: Duration) -> Instant {
    now.checked_add(pause).unwrap_or(now)
}

/// Start the periodic tidy driver, which sweeps every
/// [`expiry_sweep::interval`] of `deleted_data_max_age` and folds end
/// records its `RaftConfig::end_record_retention` after their removal.
pub fn spawn_tidy_worker(runtime: &ShardRuntime, deleted_data_max_age: Duration) {
    let runtime = runtime.clone();
    let mut schedule = Schedule::new(expiry_sweep::interval(deleted_data_max_age), Instant::now());
    tokio::spawn(async move {
        loop {
            tokio::time::sleep_until(schedule.next()).await;
            let tick = schedule.take(Instant::now());
            let pass = runtime
                .tidy_streams_all_groups_once(TidyStreamsRequest {
                    max_streams: if tick.tidy {
                        TIDY_MAX_STREAMS_PER_GROUP
                    } else {
                        0
                    },
                    max_expiry_sweeps: if tick.sweep.is_some() {
                        expiry_sweep::MAX_COMMANDS_PER_GROUP
                    } else {
                        0
                    },
                    expiry_sweep_budget: tick.sweep.map_or(Duration::ZERO, Sweep::budget),
                    deleted_data_max_age,
                    now_ms: crate::runtime::unix_time_ms(),
                })
                .await;
            let report = pass.report;
            if tick.sweep.is_some() {
                // A group whose `TidyStream` step failed still reports its
                // sweep, so a capped group re-runs. A sweep that stopped on
                // an error is not capped and keeps the usual interval, so a
                // group that keeps failing does not log every second.
                schedule.swept(Instant::now(), report.expiry_capped_groups > 0);
            }
            if report.tidied > 0 {
                tracing::info!(
                    tidied = report.tidied,
                    debt_remaining = report.debt_remaining,
                    "tidy pass completed"
                );
            }
            if report.expiry_sweeps > 0 {
                tracing::info!(
                    sweeps = report.expiry_sweeps,
                    swept_streams = report.expiry_swept_streams,
                    retired_end_records = report.end_records_retired,
                    due_groups = report.expiry_due_groups,
                    capped_groups = report.expiry_capped_groups,
                    "group sweep completed"
                );
            }
            if let Some(err) = pass.error {
                tracing::debug!("tidy pass incomplete: {err}");
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::time::Instant;

    use super::Schedule;
    use super::Sweep;
    use super::TIDY_INTERVAL;
    use super::Tick;
    use crate::expiry_sweep;

    fn tick(sweep: Option<Sweep>, tidy: bool) -> Tick {
        Tick { sweep, tidy }
    }

    #[test]
    fn the_sweep_keeps_its_interval_and_tidy_keeps_its_own() {
        let start = Instant::now();
        let mut schedule = Schedule::new(Duration::from_secs(45), start);
        let at = |secs| super::later(start, Duration::from_secs(secs));
        assert_eq!(schedule.next(), at(45));
        assert_eq!(schedule.take(at(45)), tick(Some(Sweep::Scheduled), false));
        assert_eq!(schedule.next(), super::later(start, TIDY_INTERVAL));
        assert_eq!(schedule.take(at(60)), tick(None, true));
        assert_eq!(schedule.next(), at(90));
        assert_eq!(schedule.take(at(90)), tick(Some(Sweep::Scheduled), false));
        assert_eq!(schedule.next(), at(120));
        assert_eq!(schedule.take(at(120)), tick(None, true));
        // At the default bound both run every minute, together.
        let mut schedule = Schedule::new(Duration::from_secs(60), start);
        assert_eq!(schedule.take(at(60)), tick(Some(Sweep::Scheduled), true));
        assert_eq!(schedule.next(), at(120));
    }

    #[test]
    fn a_capped_sweep_runs_again_a_second_later_with_a_shorter_budget() {
        assert!(Sweep::Rerun.budget() < Sweep::Scheduled.budget());
        assert_eq!(Sweep::Scheduled.budget(), expiry_sweep::GROUP_BUDGET);
        let start = Instant::now();
        let mut schedule = Schedule::new(Duration::from_secs(45), start);
        let at = |secs| super::later(start, Duration::from_secs(secs));
        assert_eq!(schedule.take(at(45)), tick(Some(Sweep::Scheduled), false));
        // The pass ended a second in, with a group capped.
        schedule.swept(at(46), true);
        assert_eq!(schedule.next(), at(47));
        assert_eq!(schedule.take(at(47)), tick(Some(Sweep::Rerun), false));
        schedule.swept(at(47), true);
        assert_eq!(schedule.take(at(48)), tick(Some(Sweep::Rerun), false));
        // Nothing capped, or the sweep failed: the usual interval again.
        schedule.swept(at(48), false);
        assert_eq!(schedule.next(), super::later(start, TIDY_INTERVAL));
        assert_eq!(schedule.take(at(60)), tick(None, true));
        assert_eq!(schedule.next(), at(93));
        // Tidy keeps its own minute whatever the sweep does.
        assert_eq!(schedule.take(at(93)), tick(Some(Sweep::Scheduled), false));
        schedule.swept(at(93), true);
        assert_eq!(schedule.next(), at(94));
        assert_eq!(schedule.take(at(94)), tick(Some(Sweep::Rerun), false));
        schedule.swept(at(94), false);
        assert_eq!(schedule.next(), at(120));
    }
}

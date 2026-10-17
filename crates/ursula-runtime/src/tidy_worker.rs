//! Leader-side tidy driver (bounded-stream-state F0).
//!
//! Expired streams and idle streams with debt (receipts beyond the window,
//! idle producers, producers over the cap) keep their state until something
//! normalizes them. Every [`crate::expiry_sweep::interval`] of
//! `raft.deleted_data_max_age`, each group this node leads sweeps the
//! streams that expired in it, with at most
//! [`crate::expiry_sweep::MAX_COMMANDS_PER_GROUP`] commands. Every
//! [`TIDY_INTERVAL`] it also proposes `TidyStream` for at most
//! [`TIDY_MAX_STREAMS_PER_GROUP`] streams with debt. Both repeat on later
//! passes until nothing remains. Each command does bounded work, so the
//! driver never stalls a group's apply.

use std::time::Duration;

use tokio::time::Instant;

use crate::ShardRuntime;
use crate::expiry_sweep;

/// Pause between `TidyStream` passes.
pub const TIDY_INTERVAL: Duration = Duration::from_secs(60);
/// `TidyStream` commands one pass proposes per group.
pub const TIDY_MAX_STREAMS_PER_GROUP: usize = 64;

/// What one tick of the driver runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Tick {
    sweep: bool,
    tidy: bool,
}

/// When the driver's two passes are next due.
#[derive(Debug, Clone, Copy)]
struct Schedule {
    sweep_interval: Duration,
    next_sweep: Instant,
    next_tidy: Instant,
}

impl Schedule {
    fn new(sweep_interval: Duration, now: Instant) -> Self {
        Self {
            sweep_interval,
            next_sweep: later(now, sweep_interval),
            next_tidy: later(now, TIDY_INTERVAL),
        }
    }

    /// When the next tick runs.
    fn next(&self) -> Instant {
        self.next_sweep.min(self.next_tidy)
    }

    /// The passes due at `now`, and the schedule after them.
    fn take(&mut self, now: Instant) -> Tick {
        let tick = Tick {
            sweep: now >= self.next_sweep,
            tidy: now >= self.next_tidy,
        };
        if tick.sweep {
            self.next_sweep = later(now, self.sweep_interval);
        }
        if tick.tidy {
            self.next_tidy = later(now, TIDY_INTERVAL);
        }
        tick
    }
}

/// `pause` after `now`; `now` itself past the clock's end, which no
/// interval here reaches.
fn later(now: Instant, pause: Duration) -> Instant {
    now.checked_add(pause).unwrap_or(now)
}

/// Start the periodic tidy driver, which sweeps expired streams every
/// [`expiry_sweep::interval`] of `deleted_data_max_age`.
pub fn spawn_tidy_worker(runtime: &ShardRuntime, deleted_data_max_age: Duration) {
    let runtime = runtime.clone();
    let mut schedule = Schedule::new(expiry_sweep::interval(deleted_data_max_age), Instant::now());
    tokio::spawn(async move {
        loop {
            tokio::time::sleep_until(schedule.next()).await;
            let tick = schedule.take(Instant::now());
            match runtime
                .tidy_streams_all_groups_once(
                    if tick.tidy {
                        TIDY_MAX_STREAMS_PER_GROUP
                    } else {
                        0
                    },
                    if tick.sweep {
                        expiry_sweep::MAX_COMMANDS_PER_GROUP
                    } else {
                        0
                    },
                    crate::runtime::unix_time_ms(),
                )
                .await
            {
                Ok(report) => {
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
                            due_groups = report.expiry_due_groups,
                            "expiry sweep completed"
                        );
                    }
                }
                Err(err) => tracing::debug!("tidy pass incomplete: {err}"),
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::time::Instant;

    use super::Schedule;
    use super::TIDY_INTERVAL;
    use super::Tick;

    #[test]
    fn the_sweep_keeps_its_interval_and_tidy_keeps_its_own() {
        let start = Instant::now();
        let mut schedule = Schedule::new(Duration::from_secs(45), start);
        let at = |secs| super::later(start, Duration::from_secs(secs));
        assert_eq!(schedule.next(), at(45));
        assert_eq!(schedule.take(at(45)), Tick {
            sweep: true,
            tidy: false,
        });
        assert_eq!(schedule.next(), super::later(start, TIDY_INTERVAL));
        assert_eq!(schedule.take(at(60)), Tick {
            sweep: false,
            tidy: true,
        });
        assert_eq!(schedule.next(), at(90));
        assert_eq!(schedule.take(at(90)), Tick {
            sweep: true,
            tidy: false,
        });
        assert_eq!(schedule.next(), at(120));
        assert_eq!(schedule.take(at(120)), Tick {
            sweep: false,
            tidy: true,
        });
        // At the default bound both run every minute, together.
        let mut schedule = Schedule::new(Duration::from_secs(60), start);
        assert_eq!(schedule.take(at(60)), Tick {
            sweep: true,
            tidy: true,
        });
        assert_eq!(schedule.next(), at(120));
    }
}

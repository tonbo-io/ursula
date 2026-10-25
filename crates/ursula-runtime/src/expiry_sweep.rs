//! Leader-side background group sweep (bounded-stream-state F0, F20,
//! #669).
//!
//! An expired stream reads as gone at once: reads, `HEAD`, writes and
//! deletes check expiry at their `now_ms`, and a command that names an
//! expired stream removes it. No command removes a stream it does not name,
//! so an expired stream that no request reaches leaves its group only
//! through this sweep. Every removal leaves an end record (F20), which the
//! same sweep folds into its bucket's usage once it is due.
//!
//! In the tidy pass ([`crate::tidy_worker`]) each group's leader proposes
//! the dedicated `Sweep { now_ms, limit, retire_through }` command while
//! the group is due ([`ursula_stream::StreamStateMachine::sweep_due`]): its
//! TTL front is due, or an end record is at or below `retire_through`.
//! Every replica applies it the same way: it removes up to
//! [`STREAMS_PER_COMMAND`] streams expired at the entry's `now_ms`, earliest
//! expiry first, then by bucket and stream id, and then folds up to as many
//! end records at or below `retire_through`, lowest seq first. The leader
//! only picks the moment, the `now_ms` and `retire_through`. A stream
//! renewed after the leader looked is live at the entry's `now_ms`, so it
//! stays.
//!
//! The leader sweeps at its own clock less [`CLOCK_GRACE`], so that a
//! renewal a node whose clock is up to that much behind sends at the
//! deadline is not beaten by the sweep. `retire_through` comes from the
//! group's [`RetirementClock`] on the leader: the usage seq it saw at least
//! the end-record retention ago, by its monotonic clock, so no proposer's
//! wall clock decides a fold. Per group and pass it proposes at most
//! [`MAX_COMMANDS_PER_GROUP`] entries and, on an engine with a log, starts
//! none after [`GROUP_BUDGET`]. It stops once the group is no longer due,
//! which an entry that removed and folded fewer than its limit guarantees.
//! Only a pass that stops at that cap or budget leaves the group capped,
//! and the tidy driver then runs the next sweep [`MIN_INTERVAL`] later
//! instead of a whole [`interval`], with the shorter [`RERUN_BUDGET`], so a
//! backlog drains at up to `MAX_COMMANDS_PER_GROUP * STREAMS_PER_COMMAND`
//! streams and as many end records a second. A sweep an error stopped
//! keeps the interval.

use std::collections::VecDeque;
use std::num::NonZeroU32;
use std::time::Duration;

use ursula_config::RaftConfig;
use ursula_shard::RaftGroupId;
use ursula_stream::StreamCommand;
use ursula_stream::sim_controls::Defence;

use crate::engine::GroupEngineError;
use crate::engine::GroupWriteResponse;
use crate::request::SweepResponse;
use crate::request::TidyStreamsResponse;
use crate::rt::time::Instant;

/// `Sweep` entries one pass proposes per group.
pub const MAX_COMMANDS_PER_GROUP: usize = 16;
/// Expired streams one `Sweep` entry removes at most, and end records it
/// folds at most: its `limit`.
pub const STREAMS_PER_COMMAND: NonZeroU32 = NonZeroU32::new(256).expect("256 is not zero");
/// After this long in one group's scheduled sweep, on an engine with a log,
/// the pass starts no more entries there.
pub const GROUP_BUDGET: Duration = Duration::from_millis(250);
/// The [`GROUP_BUDGET`] of a re-run that drains a capped group. It is
/// shorter, so a draining group's actor does not hold client writes past
/// the write latency goal.
pub const RERUN_BUDGET: Duration = Duration::from_millis(50);
/// How far past a stream's expiry, by the leader's clock, the sweep removes
/// it.
pub const CLOCK_GRACE: Duration = Duration::from_secs(1);
/// The longest pause between sweeps.
pub const MAX_INTERVAL: Duration = RaftConfig::SWEEP_MAX_INTERVAL;
/// The shortest pause between sweeps, and the pause before the next sweep
/// after a pass that left a group capped.
pub const MIN_INTERVAL: Duration = RaftConfig::SWEEP_MIN_INTERVAL;

/// The pause between sweeps for `raft.deleted_data_max_age`
/// ([`RaftConfig::sweep_interval`]): a sixteenth of it, between
/// [`MIN_INTERVAL`] and [`MAX_INTERVAL`]. The config's floor of the bound
/// assumes this interval.
pub fn interval(deleted_data_max_age: Duration) -> Duration {
    RaftConfig::sweep_interval(deleted_data_max_age)
}

/// The `now_ms` a sweep at the leader's `now_ms` proposes.
pub fn sweep_now_ms(now_ms: u64) -> u64 {
    let grace_ms = u64::try_from(CLOCK_GRACE.as_millis()).unwrap_or(u64::MAX);
    now_ms.saturating_sub(grace_ms)
}

/// Whether the sweep runs: always, except in a simulation's negative control.
pub fn enabled() -> bool {
    !ursula_stream::sim_controls::off(Defence::ExpirySweep)
}

/// The entry a sweep proposes at `now_ms`, the [`sweep_now_ms`] of its
/// pass, folding through `retire_through`, the
/// [`RetirementClock::retire_through`] of its pass. Records that the
/// defence fired.
pub fn command(now_ms: u64, retire_through: u64) -> StreamCommand {
    ursula_stream::sim_controls::note_fired(Defence::ExpirySweep);
    StreamCommand::Sweep {
        now_ms,
        limit: STREAMS_PER_COMMAND,
        retire_through,
    }
}

/// What became of one `Sweep` entry once it applied: the streams it removed
/// and the end records it folded. The command refuses nothing, so every
/// error is returned, a response of another kind as
/// [`crate::GroupInfraError::UnexpectedResponse`], and the next pass sweeps
/// again.
pub fn settle(
    result: Result<GroupWriteResponse, GroupEngineError>,
) -> Result<SweepResponse, GroupEngineError> {
    result.and_then(SweepResponse::try_from)
}

/// Counts one applied entry.
pub fn applied(report: &mut TidyStreamsResponse, response: &SweepResponse) {
    report.expiry_sweeps = report.expiry_sweeps.saturating_add(1);
    report.expiry_swept_streams = report.expiry_swept_streams.saturating_add(response.expired);
    report.end_records_retired = report.end_records_retired.saturating_add(response.retired);
}

/// A sweep that stopped at its entry cap or its time budget while the group
/// was still due: the group is left capped, and the tidy driver sweeps
/// again after [`MIN_INTERVAL`].
pub fn capped(report: &mut TidyStreamsResponse) {
    report.expiry_due_groups = 1;
    report.expiry_capped_groups = 1;
}

/// A sweep that `err` ended before its group was done: logged, and the
/// group is left due for the next pass, which sweeps again at the usual
/// interval. The entries committed so far stay in `report`, and the pass
/// goes on to `TidyStream`.
pub fn interrupted(group: RaftGroupId, err: &GroupEngineError, report: &mut TidyStreamsResponse) {
    tracing::warn!(
        raft_group_id = group.0,
        sweeps = report.expiry_sweeps,
        error = %err,
        "group sweep stopped early; the next pass sweeps again"
    );
    report.expiry_due_groups = 1;
}

/// When a group's end records are due on this replica (F20): samples of
/// the group's usage seq by this node's monotonic clock, one per tidy pass
/// at most every half sweep interval, on every replica, so a new leader
/// retires on time. The sweep folds through the newest sample at least the
/// retention old. Never replicated: the clock picks the watermark, and
/// apply folds what the entry names.
///
/// The clock seeds at its first sample, after its replica's state was
/// restored (the engine's start, which builds a new clock, and a snapshot
/// install, which resets it), and after a history change: seqs of an
/// earlier lineage say nothing about the new one. The seed is
/// [`ursula_stream::StreamStateMachine::end_records_ended_by`] of the
/// pass's wall clock less the retention, so a restart or an install keeps
/// each end record's age when the nodes' clocks agree. An end record dated
/// ahead holds only the seed: the samples after it fold it within the
/// retention.
#[derive(Debug, Clone, Default)]
pub struct RetirementClock {
    /// `None` until the first sample after the clock was built or reset.
    samples: Option<RetirementSamples>,
}

/// A seeded [`RetirementClock`].
#[derive(Debug, Clone)]
struct RetirementSamples {
    /// The history the samples belong to.
    history: u64,
    /// End records at or below it are due already: the seed, or a sample
    /// past the retention that was dropped.
    due_through: u64,
    /// `(when, usage_seq)`, oldest first, one per distinct `usage_seq`,
    /// trimmed to the newest sample at least the retention old and the ones
    /// after it: at most twice the retention over the sweep interval, plus
    /// two.
    samples: VecDeque<(Instant, u64)>,
}

/// What one tidy pass reads of its group for the clock, in one read of
/// the applied state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetirementSample {
    /// The group's history id.
    pub history: u64,
    /// The group's latest usage seq.
    pub usage_seq: u64,
}

impl RetirementClock {
    /// Records `sample` at `now`, `seed` giving the seed when the clock
    /// seeds, and returns the usage seq through which end records are due:
    /// the larger of the seed or a dropped sample and the seq of the newest
    /// sample at least `retain` old. 0, which no seq is, when none is.
    pub fn sample(
        &mut self,
        now: Instant,
        sample: RetirementSample,
        retain: Duration,
        seed: impl FnOnce() -> u64,
    ) -> u64 {
        let samples = match &mut self.samples {
            Some(samples) if samples.history == sample.history => samples,
            stale => stale.insert(RetirementSamples {
                history: sample.history,
                due_through: seed(),
                samples: VecDeque::new(),
            }),
        };
        let spacing = interval(retain.saturating_mul(2))
            .checked_div(2)
            .unwrap_or(Duration::ZERO);
        let record = samples.samples.back().is_none_or(|&(when, usage_seq)| {
            usage_seq != sample.usage_seq && now.saturating_duration_since(when) >= spacing
        });
        if record {
            samples.samples.push_back((now, sample.usage_seq));
        }
        // Keep the newest sample at least `retain` old and the ones after
        // it. What an older one covered is due already.
        while samples
            .samples
            .get(1)
            .is_some_and(|&(when, _)| now.saturating_duration_since(when) >= retain)
        {
            if let Some((_, usage_seq)) = samples.samples.pop_front() {
                samples.due_through = samples.due_through.max(usage_seq);
            }
        }
        let aged = samples
            .samples
            .front()
            .filter(|&&(when, _)| now.saturating_duration_since(when) >= retain)
            .map_or(0, |&(_, usage_seq)| usage_seq);
        samples.due_through.max(aged)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use ursula_shard::CoreId;
    use ursula_shard::RaftGroupId;
    use ursula_shard::ShardId;
    use ursula_shard::ShardPlacement;
    use ursula_stream::StreamErrorCode;

    use super::MAX_INTERVAL;
    use super::MIN_INTERVAL;
    use super::RetirementClock;
    use super::RetirementSample;
    use super::interval;
    use super::settle;
    use super::sweep_now_ms;
    use crate::engine::GroupEngineError;
    use crate::engine::GroupInfraError;
    use crate::engine::GroupWriteResponse;
    use crate::engine::ResponseKind;
    use crate::request::SweepResponse;
    use crate::request::TidyStreamResponse;
    use crate::rt::time::Instant;

    #[test]
    fn the_interval_is_a_sixteenth_of_the_bound_within_its_limits() {
        assert_eq!(interval(Duration::from_secs(3_600)), MAX_INTERVAL);
        assert_eq!(interval(Duration::from_secs(960)), MAX_INTERVAL);
        assert_eq!(interval(Duration::from_secs(480)), Duration::from_secs(30));
        assert_eq!(interval(Duration::from_secs(48)), Duration::from_secs(3));
        assert_eq!(interval(Duration::from_secs(8)), MIN_INTERVAL);
        assert_eq!(interval(Duration::ZERO), MIN_INTERVAL);
    }

    #[test]
    fn the_sweep_runs_a_second_behind_the_leader_clock() {
        assert_eq!(sweep_now_ms(10_000), 9_000);
        assert_eq!(sweep_now_ms(500), 0);
    }

    #[test]
    fn settle_returns_the_answer_and_every_error() {
        let placement = ShardPlacement {
            core_id: CoreId(0),
            shard_id: ShardId(0),
            raft_group_id: RaftGroupId(0),
        };
        let swept = SweepResponse {
            placement,
            expired: 7,
            retired: 5,
            group_commit_index: 3,
        };
        assert_eq!(
            settle(Ok(GroupWriteResponse::Sweep(swept.clone()))).expect("applied"),
            swept
        );
        let unexpected = settle(Ok(GroupWriteResponse::TidyStream(TidyStreamResponse {
            placement,
            debt_remaining: false,
            group_commit_index: 3,
        })));
        assert!(
            matches!(
                unexpected,
                Err(GroupEngineError::Infra(
                    GroupInfraError::UnexpectedResponse {
                        expected: ResponseKind::Sweep,
                        actual: ResponseKind::TidyStream,
                    }
                ))
            ),
            "{unexpected:?}"
        );
        // The command refuses nothing, so even a stream error is not
        // swallowed.
        let refused = settle(Err(GroupEngineError::stream(
            StreamErrorCode::StreamNotFound,
            "gone",
        )));
        assert!(matches!(
            refused,
            Err(err) if err.code() == Some(StreamErrorCode::StreamNotFound)
        ));
    }

    const RETAIN: Duration = Duration::from_secs(1_800);
    /// The sweep interval of the bound `RETAIN` is half of.
    const PASS: Duration = Duration::from_secs(60);

    fn at(history: u64, usage_seq: u64) -> RetirementSample {
        RetirementSample { history, usage_seq }
    }

    /// `start` plus `secs` seconds.
    fn after(start: Instant, secs: u64) -> Instant {
        start
            .checked_add(Duration::from_secs(secs))
            .expect("within the clock")
    }

    fn unseeded() -> u64 {
        panic!("the clock seeds only at its first sample or a history change")
    }

    fn held(clock: &RetirementClock) -> usize {
        clock
            .samples
            .as_ref()
            .map_or(0, |samples| samples.samples.len())
    }

    /// A first sample seeds the clock, and a sample counts once it is the
    /// retention old: every pass after that folds through it.
    #[test]
    fn a_sample_is_due_once_it_is_the_retention_old() {
        let start = Instant::now();
        let mut clock = RetirementClock::default();
        assert_eq!(clock.sample(start, at(7, 10), RETAIN, || 4), 4);
        // A pass a minute, three new seqs each, up to a minute short of the
        // retention: only the seed is due.
        for pass in 1_u64..30 {
            let now = after(start, pass.saturating_mul(PASS.as_secs()));
            let seq = pass.saturating_mul(3).saturating_add(10);
            assert_eq!(clock.sample(now, at(7, seq), RETAIN, unseeded), 4, "{pass}");
        }
        assert_eq!(
            clock.sample(after(start, 1_800), at(7, 100), RETAIN, unseeded),
            10
        );
        assert_eq!(
            clock.sample(after(start, 1_860), at(7, 101), RETAIN, unseeded),
            13
        );
    }

    /// Samples of one seq keep the first instant it was seen at, and a pass
    /// sooner than half an interval after the last sample records none, so
    /// the clock holds a bounded number of samples.
    #[test]
    fn samples_keep_the_oldest_instant_of_a_seq_and_are_bounded() {
        let start = Instant::now();
        let mut clock = RetirementClock::default();
        clock.sample(start, at(1, 5), RETAIN, || 0);
        // The same seq for the whole retention: its first instant counts.
        assert_eq!(
            clock.sample(after(start, 1_800), at(1, 5), RETAIN, unseeded),
            5
        );
        // A pass a second: only one sample per half interval is kept.
        for seq in 6_u64..=600 {
            clock.sample(
                after(start, seq.saturating_add(1_800)),
                at(1, seq),
                RETAIN,
                unseeded,
            );
        }
        // Twice the retention over the interval, plus two.
        assert!(held(&clock) <= 62, "{} samples", held(&clock));
        assert!(held(&clock) >= 2, "{} samples", held(&clock));
    }

    /// Samples past the retention are dropped, and what they covered stays
    /// due.
    #[test]
    fn a_dropped_sample_keeps_its_seq_due() {
        let start = Instant::now();
        let mut clock = RetirementClock::default();
        clock.sample(start, at(1, 3), RETAIN, || 0);
        clock.sample(after(start, 60), at(1, 8), RETAIN, unseeded);
        // Both samples are past the retention: only the newer one stays.
        assert_eq!(
            clock.sample(after(start, 1_920), at(1, 9), RETAIN, unseeded),
            8
        );
        assert_eq!(held(&clock), 2);
    }

    /// Another history seeds the clock again: the old lineage's seqs say
    /// nothing about the new one.
    #[test]
    fn a_history_change_seeds_the_clock_again() {
        let start = Instant::now();
        let mut clock = RetirementClock::default();
        clock.sample(start, at(1, 50), RETAIN, || 0);
        assert_eq!(
            clock.sample(after(start, 1_800), at(1, 60), RETAIN, unseeded),
            50
        );
        // The new history's seed, never the old samples.
        assert_eq!(
            clock.sample(after(start, 1_860), at(2, 40), RETAIN, || 12),
            12
        );
        assert_eq!(
            clock.sample(after(start, 1_920), at(2, 45), RETAIN, unseeded),
            12
        );
    }

    /// A reset clock, as an install leaves it, seeds at its next sample.
    #[test]
    fn a_reset_clock_seeds_at_its_next_sample() {
        let start = Instant::now();
        let mut clock = RetirementClock::default();
        clock.sample(start, at(1, 50), RETAIN, || 0);
        clock = RetirementClock::default();
        assert_eq!(clock.sample(after(start, 60), at(1, 70), RETAIN, || 30), 30);
    }
}

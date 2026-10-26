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
//! wall clock decides the fold of an end record the leader saw removed.
//! Only the end records a replica held at its start or brought in with a
//! snapshot install are dated by its wall clock. Per group and pass it proposes at most
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
/// The clock seeds at its first sample after it was built (the engine's
/// start) and after a history change, since seqs of an earlier lineage say
/// nothing about the new one. A snapshot install of the same history keeps
/// the samples and seeds again the end records it brought. The end records
/// a replica held or installed before it sampled them, those at or below
/// the seed's usage seq, are dated by the wall clock: every pass folds
/// through those that ended at least the retention before the pass's wall
/// clock ([`ursula_stream::StreamStateMachine::end_records_ended_by`]),
/// until the seed's own sample is the retention old. So a restart or an
/// install keeps each end record's age when the nodes' clocks agree. An end
/// record dated ahead holds the ones after it only until that sample is
/// due. End records removed after the seed are dated by the samples alone.
#[derive(Debug, Clone, Default)]
pub struct RetirementClock {
    /// `None` until the first sample after the clock was built.
    samples: Option<RetirementSamples>,
}

/// A seeded [`RetirementClock`].
#[derive(Debug, Clone)]
struct RetirementSamples {
    /// The history the samples belong to.
    history: u64,
    /// The group's usage seq at the latest seed. The wall clock dates the
    /// end records at or below it while `due_through` is below it.
    seeded_through: u64,
    /// The replica installed a snapshot since the last sample: the next
    /// sample seeds again at its usage seq.
    installed: bool,
    /// End records at or below it are due already: those the wall clock
    /// dated past the retention, or a sample past the retention that was
    /// dropped.
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
    /// The replica installed a snapshot: its next sample seeds the end
    /// records the install brought, which this replica never sampled. A
    /// clock not yet seeded seeds at its first sample anyway.
    pub fn installed(&mut self) {
        if let Some(samples) = &mut self.samples {
            samples.installed = true;
        }
    }

    /// Records `sample` at `now`, the pass's monotonic clock, and returns
    /// the usage seq through which end records are due under
    /// `deleted_data_max_age`, whose [`RaftConfig::end_record_retention`]
    /// they wait: the largest of what the wall clock dated, a dropped
    /// sample and the seq of the newest sample at least the retention old.
    /// 0, which no seq is, when none is. `wall_ms` is the pass's wall clock,
    /// and `ended_by(at_ms, through)` the end records at or below `through`
    /// that ended by `at_ms`
    /// ([`ursula_stream::StreamStateMachine::end_records_ended_by`]), read
    /// only while the seeded end records are not all due.
    pub fn sample(
        &mut self,
        now: Instant,
        wall_ms: u64,
        sample: RetirementSample,
        deleted_data_max_age: Duration,
        ended_by: impl FnOnce(u64, u64) -> u64,
    ) -> u64 {
        let retain = RaftConfig::end_record_retention(deleted_data_max_age);
        let spacing = interval(deleted_data_max_age)
            .checked_div(2)
            .unwrap_or(Duration::ZERO);
        let samples = match &mut self.samples {
            Some(samples) if samples.history == sample.history => {
                if samples.installed {
                    samples.installed = false;
                    samples.seeded_through = samples.seeded_through.max(sample.usage_seq);
                }
                samples
            }
            stale => stale.insert(RetirementSamples {
                history: sample.history,
                seeded_through: sample.usage_seq,
                installed: false,
                due_through: 0,
                samples: VecDeque::new(),
            }),
        };
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
        if let Some(&(when, usage_seq)) = samples.samples.front()
            && now.saturating_duration_since(when) >= retain
        {
            samples.due_through = samples.due_through.max(usage_seq);
        }
        // Until the seed's sample is the retention old, the wall clock dates
        // the end records the seed held.
        if samples.due_through < samples.seeded_through {
            let retain_ms = u64::try_from(retain.as_millis()).unwrap_or(u64::MAX);
            let dated = ended_by(wall_ms.saturating_sub(retain_ms), samples.seeded_through);
            samples.due_through = samples.due_through.max(dated.min(samples.seeded_through));
        }
        samples.due_through
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

    /// The default bound, whose retention is 30 minutes and whose sweep
    /// runs every minute.
    const BOUND: Duration = Duration::from_secs(3_600);
    /// The sweep interval of `BOUND`.
    const PASS: Duration = Duration::from_secs(60);
    /// The wall clock of the first pass of each test, in ms.
    const WALL: u64 = 1_000_000_000;
    /// `BOUND`'s retention in ms.
    const RETAIN_MS: u64 = 1_800_000;

    fn at(history: u64, usage_seq: u64) -> RetirementSample {
        RetirementSample { history, usage_seq }
    }

    /// `start` plus `secs` seconds.
    fn after(start: Instant, secs: u64) -> Instant {
        start
            .checked_add(Duration::from_secs(secs))
            .expect("within the clock")
    }

    /// `WALL` plus `secs` seconds.
    fn wall_after(secs: u64) -> u64 {
        WALL.saturating_add(secs.saturating_mul(1_000))
    }

    /// The end records a group holds, as `(seq, ended_at_ms)` in seq order:
    /// what [`ursula_stream::StreamStateMachine::end_records_ended_by`]
    /// answers over them, with nothing folded.
    fn ended_by(records: &[(u64, u64)]) -> impl Fn(u64, u64) -> u64 + '_ {
        move |at_ms, through| {
            records
                .iter()
                .take_while(|&&(seq, ended_at_ms)| seq <= through && ended_at_ms <= at_ms)
                .last()
                .map_or(0, |&(seq, _)| seq)
        }
    }

    /// The clock dates nothing by the wall clock: every end record it is
    /// asked about was removed after its seed.
    fn undated(_at_ms: u64, _through: u64) -> u64 {
        panic!("the wall clock dates only the end records held at the seed")
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
        // Seqs 1 to 4 ended long before, and the held end records above
        // them later than the retention before the pass.
        let records = [(4, 0), (9, wall_after(0))];
        assert_eq!(
            clock.sample(start, WALL, at(7, 10), BOUND, ended_by(&records)),
            4
        );
        // A pass a minute, three new seqs each, up to a minute short of the
        // retention: only what the wall clock dated is due, until the
        // record at 9 is the retention old by it.
        for pass in 1_u64..30 {
            let now = after(start, pass.saturating_mul(PASS.as_secs()));
            let seq = pass.saturating_mul(3).saturating_add(10);
            assert_eq!(
                clock.sample(
                    now,
                    wall_after(pass.saturating_mul(60)),
                    at(7, seq),
                    BOUND,
                    ended_by(&[(4, 0)])
                ),
                4,
                "{pass}"
            );
        }
        assert_eq!(
            clock.sample(
                after(start, 1_800),
                wall_after(1_800),
                at(7, 100),
                BOUND,
                undated
            ),
            10
        );
        assert_eq!(
            clock.sample(
                after(start, 1_860),
                wall_after(1_860),
                at(7, 101),
                BOUND,
                undated
            ),
            13
        );
    }

    /// An end record the replica held when its clock seeded keeps its age:
    /// one removed 29 minutes before the reset is due about a minute after
    /// it, by the wall clock, not a whole retention later.
    #[test]
    fn an_end_record_held_at_the_seed_keeps_its_age() {
        let start = Instant::now();
        let mut clock = RetirementClock::default();
        let removed_at = WALL.saturating_sub(1_740_000);
        let records = [(9, removed_at)];
        assert_eq!(
            clock.sample(start, WALL, at(1, 9), BOUND, ended_by(&records)),
            0
        );
        assert_eq!(
            clock.sample(
                after(start, 30),
                wall_after(30),
                at(1, 9),
                BOUND,
                ended_by(&records)
            ),
            0
        );
        assert_eq!(
            clock.sample(
                after(start, 60),
                wall_after(60),
                at(1, 9),
                BOUND,
                ended_by(&records)
            ),
            9
        );
        // Half a retention before the reset: due half a retention after it.
        let mut clock = RetirementClock::default();
        let records = [(3, WALL.saturating_sub(900_000))];
        assert_eq!(
            clock.sample(start, WALL, at(1, 3), BOUND, ended_by(&records)),
            0
        );
        assert_eq!(
            clock.sample(
                after(start, 899),
                wall_after(899),
                at(1, 3),
                BOUND,
                ended_by(&records)
            ),
            0
        );
        assert_eq!(
            clock.sample(
                after(start, 900),
                wall_after(900),
                at(1, 3),
                BOUND,
                ended_by(&records)
            ),
            3
        );
    }

    /// The wall clock dates only the end records held at the seed: one
    /// removed after it waits for the samples, whatever the wall clock says.
    /// An end record dated ahead holds the ones after it until the seed's
    /// sample is the retention old.
    #[test]
    fn the_wall_clock_dates_only_what_the_seed_held() {
        let start = Instant::now();
        let mut clock = RetirementClock::default();
        // 5 was removed on a node whose clock was a day ahead.
        let records = [(5, wall_after(86_400)), (6, 0), (12, 0)];
        assert_eq!(
            clock.sample(start, WALL, at(1, 6), BOUND, ended_by(&records)),
            0
        );
        // 12 ended after the seed: the scan never reaches it.
        assert_eq!(
            clock.sample(
                after(start, 60),
                wall_after(7_200),
                at(1, 12),
                BOUND,
                ended_by(&records)
            ),
            0
        );
        // The seed's sample is the retention old: 5 and 6 are due, 12 is not.
        assert_eq!(
            clock.sample(
                after(start, 1_800),
                wall_after(9_000),
                at(1, 12),
                BOUND,
                undated
            ),
            6
        );
        assert_eq!(
            clock.sample(
                after(start, 1_860),
                wall_after(9_060),
                at(1, 12),
                BOUND,
                undated
            ),
            12
        );
    }

    /// Samples of one seq keep the first instant it was seen at, and a pass
    /// sooner than half an interval after the last sample records none, so
    /// the clock holds a bounded number of samples.
    #[test]
    fn samples_keep_the_oldest_instant_of_a_seq_and_are_bounded() {
        let start = Instant::now();
        let mut clock = RetirementClock::default();
        clock.sample(start, WALL, at(1, 5), BOUND, ended_by(&[]));
        // The same seq for the whole retention: its first instant counts.
        assert_eq!(
            clock.sample(after(start, 1_800), WALL, at(1, 5), BOUND, ended_by(&[])),
            5
        );
        // A pass a second: only one sample per half interval is kept.
        for seq in 6_u64..=600 {
            clock.sample(
                after(start, seq.saturating_add(1_800)),
                WALL,
                at(1, seq),
                BOUND,
                undated,
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
        clock.sample(start, WALL, at(1, 3), BOUND, ended_by(&[]));
        clock.sample(after(start, 60), WALL, at(1, 8), BOUND, ended_by(&[]));
        // Both samples are past the retention: only the newer one stays.
        assert_eq!(
            clock.sample(after(start, 1_920), WALL, at(1, 9), BOUND, undated),
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
        clock.sample(start, WALL, at(1, 50), BOUND, ended_by(&[]));
        assert_eq!(
            clock.sample(after(start, 1_800), WALL, at(1, 60), BOUND, undated),
            50
        );
        // The new history's wall-clock dating, never the old samples.
        let records = [(12, 0), (30, wall_after(1_860))];
        assert_eq!(
            clock.sample(
                after(start, 1_860),
                wall_after(1_860),
                at(2, 40),
                BOUND,
                ended_by(&records)
            ),
            12
        );
        assert_eq!(
            clock.sample(
                after(start, 1_920),
                wall_after(1_920),
                at(2, 45),
                BOUND,
                ended_by(&records)
            ),
            12
        );
    }

    /// An install of the same history keeps the samples, and its next
    /// sample dates the end records it brought by the wall clock. A clock
    /// built anew, as at a start, seeds at its first sample.
    #[test]
    fn an_install_keeps_the_samples_and_dates_what_it_brought() {
        let start = Instant::now();
        let mut clock = RetirementClock::default();
        clock.sample(start, WALL, at(1, 50), BOUND, ended_by(&[]));
        clock.sample(
            after(start, 600),
            wall_after(600),
            at(1, 55),
            BOUND,
            ended_by(&[]),
        );
        clock.installed();
        // The install brought seqs up to 90: 70 ended a retention before
        // the pass, 80 later.
        let records = [
            (70, wall_after(700).saturating_sub(RETAIN_MS)),
            (80, wall_after(700)),
        ];
        assert_eq!(
            clock.sample(
                after(start, 700),
                wall_after(700),
                at(1, 90),
                BOUND,
                ended_by(&records)
            ),
            70
        );
        // The sample from before the install still counts at its own age.
        assert_eq!(
            clock.sample(
                after(start, 1_800),
                wall_after(1_800),
                at(1, 90),
                BOUND,
                ended_by(&records[..1])
            ),
            70
        );
        assert_eq!(
            clock.sample(
                after(start, 2_400),
                wall_after(2_400),
                at(1, 91),
                BOUND,
                ended_by(&records[..1])
            ),
            70
        );
        // 80 ended at the install's pass: a retention later it is due.
        assert_eq!(
            clock.sample(
                after(start, 2_500),
                wall_after(2_500),
                at(1, 91),
                BOUND,
                ended_by(&records)
            ),
            90
        );

        let mut clock = RetirementClock::default();
        clock.installed();
        assert_eq!(
            clock.sample(
                after(start, 60),
                WALL,
                at(1, 70),
                BOUND,
                ended_by(&[(30, 0)])
            ),
            30
        );
    }
}

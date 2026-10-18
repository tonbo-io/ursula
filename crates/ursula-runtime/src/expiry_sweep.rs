//! Leader-side background expiry sweep (bounded-stream-state F0, #669).
//!
//! An expired stream reads as gone at once: reads, `HEAD`, writes and
//! deletes check expiry at their `now_ms`, and a command that names an
//! expired stream removes it. No command removes a stream it does not name,
//! so an expired stream that no request reaches leaves its group only
//! through this sweep.
//!
//! In the tidy pass ([`crate::tidy_worker`]) each group's leader proposes
//! the dedicated `ExpireStreams { now_ms, limit }` command while the group's
//! TTL front is due ([`ursula_stream::StreamStateMachine::expiry_due`]).
//! Every replica applies it the same way: it removes up to
//! [`STREAMS_PER_COMMAND`] streams expired at the entry's `now_ms`, earliest
//! expiry first, then by bucket and stream id. The leader only picks the
//! moment and the `now_ms`. A stream renewed after the leader looked is
//! live at the entry's `now_ms`, so it stays.
//!
//! The leader sweeps at its own clock less [`CLOCK_GRACE`], so that a
//! renewal a node whose clock is up to that much behind sends at the
//! deadline is not beaten by the sweep. Per group and pass it proposes at
//! most [`MAX_COMMANDS_PER_GROUP`] entries and, on an engine with a log,
//! starts none after [`GROUP_BUDGET`]. It stops once the front is no longer
//! due, which an entry that removed fewer than its limit guarantees. Only a
//! pass that stops at that cap or budget leaves the group capped, and the
//! tidy driver then runs the next sweep [`MIN_INTERVAL`] later instead of a
//! whole [`interval`], with the shorter [`RERUN_BUDGET`], so a backlog drains
//! at up to `MAX_COMMANDS_PER_GROUP * STREAMS_PER_COMMAND` streams a second.
//! A sweep an error stopped keeps the interval.

use std::num::NonZeroU32;
use std::time::Duration;

use ursula_shard::RaftGroupId;
use ursula_stream::StreamCommand;
use ursula_stream::sim_controls::Defence;

use crate::command::GroupWriteCommand;
use crate::engine::GroupEngineError;
use crate::engine::GroupWriteResponse;
use crate::request::ExpireStreamsResponse;
use crate::request::TidyStreamsResponse;

/// `ExpireStreams` entries one pass proposes per group.
pub const MAX_COMMANDS_PER_GROUP: usize = 16;
/// Expired streams one `ExpireStreams` entry removes at most: its `limit`.
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
pub const MAX_INTERVAL: Duration = Duration::from_secs(60);
/// The shortest pause between sweeps, and the pause before the next sweep
/// after a pass that left a group capped.
pub const MIN_INTERVAL: Duration = Duration::from_secs(1);

/// The pause between sweeps for `raft.deleted_data_max_age`: a quarter of
/// it, between [`MIN_INTERVAL`] and [`MAX_INTERVAL`].
pub fn interval(deleted_data_max_age: Duration) -> Duration {
    deleted_data_max_age
        .checked_div(4)
        .unwrap_or(MAX_INTERVAL)
        .clamp(MIN_INTERVAL, MAX_INTERVAL)
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
/// pass. Records that the defence fired.
pub fn command(now_ms: u64) -> GroupWriteCommand {
    ursula_stream::sim_controls::note_fired(Defence::ExpirySweep);
    GroupWriteCommand::from(StreamCommand::ExpireStreams {
        now_ms,
        limit: STREAMS_PER_COMMAND,
    })
}

/// What became of one `ExpireStreams` entry: the streams it removed once it
/// applied. The command refuses nothing, so every error is returned, a
/// response of another kind as [`crate::GroupInfraError::UnexpectedResponse`],
/// and the next pass sweeps again.
pub fn settle(
    result: Result<GroupWriteResponse, GroupEngineError>,
) -> Result<u64, GroupEngineError> {
    result
        .and_then(ExpireStreamsResponse::try_from)
        .map(|response| response.removed)
}

/// Counts one applied entry that removed `removed` streams.
pub fn applied(report: &mut TidyStreamsResponse, removed: u64) {
    report.expiry_sweeps = report.expiry_sweeps.saturating_add(1);
    report.expiry_swept_streams = report.expiry_swept_streams.saturating_add(removed);
}

/// A sweep that stopped at its entry cap or its time budget while streams
/// were still due: the group is left capped, and the tidy driver sweeps
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
        "expiry sweep stopped early; the next pass sweeps again"
    );
    report.expiry_due_groups = 1;
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
    use super::interval;
    use super::settle;
    use super::sweep_now_ms;
    use crate::engine::GroupEngineError;
    use crate::engine::GroupInfraError;
    use crate::engine::GroupWriteResponse;
    use crate::engine::ResponseKind;
    use crate::request::ExpireStreamsResponse;
    use crate::request::TidyStreamResponse;

    #[test]
    fn the_interval_is_a_quarter_of_the_bound_within_its_limits() {
        assert_eq!(interval(Duration::from_secs(3_600)), MAX_INTERVAL);
        assert_eq!(interval(Duration::from_secs(240)), MAX_INTERVAL);
        assert_eq!(interval(Duration::from_secs(120)), Duration::from_secs(30));
        assert_eq!(interval(Duration::from_secs(12)), Duration::from_secs(3));
        assert_eq!(interval(Duration::from_secs(2)), MIN_INTERVAL);
        assert_eq!(interval(Duration::ZERO), MIN_INTERVAL);
    }

    #[test]
    fn the_sweep_runs_a_second_behind_the_leader_clock() {
        assert_eq!(sweep_now_ms(10_000), 9_000);
        assert_eq!(sweep_now_ms(500), 0);
    }

    #[test]
    fn settle_returns_the_removed_count_and_every_error() {
        let placement = ShardPlacement {
            core_id: CoreId(0),
            shard_id: ShardId(0),
            raft_group_id: RaftGroupId(0),
        };
        assert_eq!(
            settle(Ok(GroupWriteResponse::ExpireStreams(
                ExpireStreamsResponse {
                    placement,
                    removed: 7,
                    group_commit_index: 3,
                }
            )))
            .expect("applied"),
            7
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
                        expected: ResponseKind::ExpireStreams,
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
}

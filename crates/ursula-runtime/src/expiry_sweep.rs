//! Leader-side background expiry sweep (bounded-stream-state F0, #669).
//!
//! An expired stream reads as gone at once, but it leaves its group's state
//! only when a command sweeps it: a create, an external append, a snapshot
//! publish, a retention advance or a close sweeps up to 256 expired streams
//! at its `now_ms` (a plain append, a deletion, a bucket purge and the cold,
//! tidy and history commands do not), and a read of an expired
//! stream proposes `TouchStreamAccess`, which removes it. A group that takes
//! only plain appends, or no writes at all, kept its expired streams.
//!
//! The tidy pass ([`crate::tidy_worker`]) closes that gap with no new
//! replicated command. Each group's leader proposes the
//! `TouchStreamAccess { renew_ttl: false }` a read of an expired stream
//! already proposes, naming the stream at the front of the group's TTL index
//! ([`ursula_stream::StreamStateMachine::expiry_sweep_target`]). Every
//! replica applies it the same way: it removes the named stream if that
//! stream expired at the command's `now_ms`, and then sweeps up to 256 more.
//! The leader only picks the moment, the `now_ms` and the name. A stream
//! renewed after the leader picked it is live at the entry's `now_ms`, so it
//! stays.
//!
//! The leader sweeps at its own clock less [`CLOCK_GRACE`], so that a
//! renewal a node whose clock is up to that much behind sends at the
//! deadline is not beaten by the sweep. Per group and pass it proposes at
//! most [`MAX_COMMANDS_PER_GROUP`] commands and, on an engine with a log,
//! starts none after [`GROUP_BUDGET`], so a large backlog drains over a few
//! passes.

use std::time::Duration;

use ursula_shard::BucketStreamId;
use ursula_shard::RaftGroupId;
use ursula_stream::StreamCommand;
use ursula_stream::StreamErrorCode;
use ursula_stream::sim_controls::Defence;

use crate::command::GroupWriteCommand;
use crate::engine::GroupEngineError;
use crate::engine::GroupWriteResponse;
use crate::request::TidyStreamsResponse;

/// Sweep commands one pass proposes per group, each removing the named
/// stream and up to 256 more.
pub const MAX_COMMANDS_PER_GROUP: usize = 16;
/// After this long in one group's sweep, on an engine with a log, the pass
/// starts no more commands there.
pub const GROUP_BUDGET: Duration = Duration::from_millis(250);
/// How far past a stream's expiry, by the leader's clock, the sweep removes
/// it.
pub const CLOCK_GRACE: Duration = Duration::from_secs(1);
/// The longest pause between sweeps.
pub const MAX_INTERVAL: Duration = Duration::from_secs(60);
/// The shortest pause between sweeps.
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

/// The command a sweep proposes for `stream_id` at `now_ms`, the
/// [`sweep_now_ms`] of its pass. Records that the defence fired.
pub fn command(stream_id: BucketStreamId, now_ms: u64) -> GroupWriteCommand {
    ursula_stream::sim_controls::note_fired(Defence::ExpirySweep);
    GroupWriteCommand::from(StreamCommand::TouchStreamAccess {
        stream_id,
        now_ms,
        renew_ttl: false,
    })
}

/// What became of one sweep command: `Ok` once it applied. A stream that
/// went away, or whose bucket did, after the leader named it is refused by
/// the command, but the command applied and swept all the same. Every other
/// error is returned, and the next pass sweeps again.
pub fn settle(
    result: Result<GroupWriteResponse, GroupEngineError>,
) -> Result<(), GroupEngineError> {
    match result {
        Ok(GroupWriteResponse::TouchStreamAccess(_)) => Ok(()),
        Ok(other) => Err(GroupEngineError::new(format!(
            "unexpected expiry sweep write response: {other:?}"
        ))),
        Err(err)
            if matches!(
                err.code(),
                Some(
                    StreamErrorCode::StreamNotFound
                        | StreamErrorCode::BucketNotFound
                        | StreamErrorCode::BucketErased
                )
            ) =>
        {
            Ok(())
        }
        Err(err) => Err(err),
    }
}

/// A sweep that `err` ended before its group was done: logged, and the
/// group is left due for the next pass, which sweeps again. The commands
/// committed so far stay in `report`, and the pass goes on to `TidyStream`.
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

    use super::MAX_INTERVAL;
    use super::MIN_INTERVAL;
    use super::interval;
    use super::sweep_now_ms;

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
}

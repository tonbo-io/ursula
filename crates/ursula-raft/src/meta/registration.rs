//! This node's registration with the meta group (design 3.6).
//!
//! A node registers itself at every start, and again after
//! [`REREGISTER_AFTER`] without a meta leader, naming the root it runs on
//! and whether that root was empty when it opened. It retries a refusal
//! that may pass later and every failure to reach the leader with a capped
//! back-off, and stops at a permanent refusal
//! (`ursula_control::ControlError::requeue`). Each answer is published as
//! the node's [`RegistrationStatus`], which status reports read.
//!
//! `OtherRoot` is judged by how the run stands on its root
//! ([`super::root::RootStanding`], [`judge_answer`]): an unbound empty root,
//! whose data replicas the node fence holds out until it is bound, is held
//! out of meta votes too and registers until an operator releases the
//! slot, so it binds within [`RETRY_MAX`] of the release with no restart,
//! and a bound root, one that holds state, fences itself for good.
//! `NodeRemoved` makes the node treat itself as removed until it restarts.

use std::sync::Arc;
use std::sync::Weak;
use std::time::Duration;

use openraft::rt::WatchReceiver;
use ursula_control::ControlApplied;
use ursula_control::ControlCommand;
use ursula_control::ControlError;
use ursula_control::RegistrationStatus;
use ursula_control::Requeue;
use ursula_proto::placement::NodeId;
use ursula_proto::placement::Registered;
use ursula_proto::placement::RootId;
use ursula_proto::placement::RootSlot;

use super::handle::MetaHandle;
use super::handle::MetaWriteError;
use super::pause::MetaPausePoint;
use super::pause::MetaPauses;
use super::replica::ActiveMeta;
use super::root::RootGuard;
use super::root::RootStanding;
use super::root::Supersession;
use crate::RaftGroupHandleRegistry;
use crate::RecoveryTransport;
use crate::rt::sync::watch;

/// How long a registered node goes without a meta leader before it
/// registers again.
pub const REREGISTER_AFTER: Duration = Duration::from_secs(30);
/// The first pause before a registration is sent again.
pub(crate) const RETRY_MIN: Duration = Duration::from_millis(200);
/// The longest pause before a registration is sent again.
pub(crate) const RETRY_MAX: Duration = Duration::from_secs(5);

/// The next pause of a capped back-off.
pub(crate) fn back_off(pause: Duration) -> Duration {
    pause.saturating_mul(2).min(RETRY_MAX)
}

/// What this node registers: its id, the root it runs on, and whether the
/// root held no journal record, vote or snapshot record when it opened.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Registrant {
    pub(crate) node: NodeId,
    pub(crate) root: RootId,
    pub(crate) empty: bool,
}

/// What the registration does with one answer.
#[derive(Debug)]
pub(crate) enum Verdict {
    /// Publish the answer, and register again after [`REREGISTER_AFTER`]
    /// without a meta leader.
    Registered(Registered),
    /// An unbound root refused for the slot: hold it out and register
    /// again.
    RootRefused(RootSlot),
    /// A bound root refused for the slot: fence the process for good.
    Superseded(RootSlot),
    /// The node is removed: it treats itself so until it restarts.
    Removed,
    /// Refused for good.
    Refused(ControlError),
    /// The meta group answered another command.
    Unexpected(ControlApplied),
    /// Not answered, or refused for now: register again.
    Retry(MetaWriteError),
}

/// What a run standing on its root as `standing` does with `answer` (see
/// the module docs).
pub(crate) fn judge_answer(
    standing: RootStanding,
    answer: Result<ControlApplied, MetaWriteError>,
) -> Verdict {
    match answer {
        Ok(ControlApplied::NodeRegistered(registered)) => Verdict::Registered(registered),
        Ok(other) => Verdict::Unexpected(other),
        Err(MetaWriteError::Refused(ControlError::NodeRemoved { .. })) => Verdict::Removed,
        Err(MetaWriteError::Refused(ControlError::OtherRoot { slot, .. })) => match standing {
            RootStanding::Unbound => Verdict::RootRefused(slot),
            RootStanding::Bound => Verdict::Superseded(slot),
        },
        Err(MetaWriteError::Refused(refusal)) if refusal.requeue() == Requeue::Permanent => {
            Verdict::Refused(refusal)
        }
        Err(error) => Verdict::Retry(error),
    }
}

/// Registers `registrant` until the meta group answers for good,
/// publishing each answer on `status` and judging it by `guard`'s
/// standing. Ends at a permanent refusal, once the process is superseded,
/// or once `stopped` is true.
pub(crate) async fn run_registration<T: RecoveryTransport>(
    handle: MetaHandle<T>,
    registrant: Registrant,
    guard: RootGuard,
    registry: RaftGroupHandleRegistry,
    status: watch::Sender<RegistrationStatus>,
    pauses: MetaPauses,
    stopped: impl Fn() -> bool,
) {
    let me = registrant.node;
    let mut pause = RETRY_MIN;
    while !stopped() {
        pauses.at(MetaPausePoint::Registration).await;
        let command = ControlCommand::RegisterNode {
            node: me,
            root: registrant.root,
            empty: registrant.empty,
            now_ms: ursula_runtime::unix_time_ms(),
        };
        match judge_answer(guard.standing(), handle.submit(command).await) {
            Verdict::Registered(registered) => {
                guard.bind();
                status.send_replace(RegistrationStatus::Answered(registered));
                pause = RETRY_MIN;
                wait_without_leader(&registry, REREGISTER_AFTER).await;
                continue;
            }
            Verdict::RootRefused(slot) => {
                tracing::warn!(
                    node = %me,
                    root = %registrant.root,
                    %slot,
                    "meta registration: this node id is bound to another root. Its data replicas \
                     stay held out and it registers again until an operator releases that root \
                     with `ursulactl node replace-disk`"
                );
                guard.hold_out();
                status.send_replace(RegistrationStatus::RootRefused { slot });
            }
            Verdict::Superseded(slot) => {
                guard.supersede(Supersession::OtherRoot { slot });
                status.send_replace(RegistrationStatus::Refused(ControlError::OtherRoot {
                    node: me,
                    slot,
                }));
                return;
            }
            Verdict::Removed => {
                tracing::warn!(node = %me, "meta registration: this node is removed");
                status.send_replace(RegistrationStatus::Removed);
                return;
            }
            Verdict::Refused(refusal) => {
                tracing::error!(%refusal, "meta registration: refused for good");
                status.send_replace(RegistrationStatus::Refused(refusal));
                return;
            }
            Verdict::Unexpected(other) => {
                tracing::error!(
                    ?other,
                    "meta registration: the meta group answered another command"
                );
                return;
            }
            Verdict::Retry(error) => {
                tracing::debug!(%error, "meta registration: not answered, retrying");
            }
        }
        crate::rt::time::sleep(pause).await;
        pause = back_off(pause);
    }
}

/// Waits until this node's meta replica has gone `window` without a
/// leader. A node without a meta replica waits `window`, and so does one
/// whose replica stopped while it is still published, whose metrics close
/// at once: the wait then always awaits something that pends.
async fn wait_without_leader(registry: &RaftGroupHandleRegistry, window: Duration) {
    // The replica whose metrics closed, held weakly so the wait keeps no
    // replica alive and its address cannot name another.
    let mut stopped: Option<Weak<ActiveMeta>> = None;
    loop {
        let Some(active) = registry.meta() else {
            crate::rt::time::sleep(window).await;
            return;
        };
        let current = Arc::downgrade(&active);
        if stopped
            .as_ref()
            .is_some_and(|stopped| Weak::ptr_eq(stopped, &current))
        {
            // The replica whose metrics closed is still the published one.
            // It names no leader, so the node goes `window` without one.
            drop(active);
            crate::rt::time::sleep(window).await;
            return;
        }
        let mut metrics = active.core.owner().metrics();
        drop(active);
        let mut leaderless_since = None;
        loop {
            if metrics.borrow_watched().current_leader.is_some() {
                leaderless_since = None;
                if metrics.changed().await.is_err() {
                    break;
                }
                continue;
            }
            let since = *leaderless_since.get_or_insert_with(crate::rt::time::Instant::now);
            let remaining = window.saturating_sub(since.elapsed());
            if remaining.is_zero() {
                return;
            }
            match crate::rt::time::timeout(remaining, metrics.changed()).await {
                Err(_elapsed) => return,
                Ok(Err(_closed)) => break,
                Ok(Ok(())) => {}
            }
        }
        // The replica stopped: watch the one that replaced it, if one did.
        stopped = Some(current);
    }
}

#[cfg(test)]
mod tests {
    use ursula_control::ControlApplied;
    use ursula_control::ControlError;
    use ursula_control::samples;
    use ursula_proto::placement::Registered;
    use ursula_proto::placement::RootSlot;

    use super::MetaWriteError;
    use super::RootStanding;
    use super::Verdict;
    use super::judge_answer;

    /// Each answer, to each standing on the root: only `OtherRoot` depends
    /// on it, holding an unbound empty root out and superseding a bound
    /// one, whichever slot it names. A transient refusal is retried and a
    /// permanent one ends the registration, for both.
    #[test]
    fn an_other_root_answer_holds_an_empty_root_out_and_supersedes_a_bound_one() {
        let slots = [
            RootSlot::Bound(samples::root(6)),
            RootSlot::Released(samples::root(5)),
            RootSlot::Released(samples::root(7)),
        ];
        for standing in [RootStanding::Unbound, RootStanding::Bound] {
            for slot in slots {
                let refused = Err(MetaWriteError::Refused(ControlError::OtherRoot {
                    node: samples::node(3),
                    slot,
                }));
                match (standing, judge_answer(standing, refused)) {
                    (RootStanding::Unbound, Verdict::RootRefused(answered))
                    | (RootStanding::Bound, Verdict::Superseded(answered)) => {
                        assert_eq!(answered, slot);
                    }
                    (standing, verdict) => panic!("{standing:?} {slot:?}: {verdict:?}"),
                }
            }
            assert!(matches!(
                judge_answer(
                    standing,
                    Ok(ControlApplied::NodeRegistered(Registered::Forming))
                ),
                Verdict::Registered(Registered::Forming)
            ));
            assert!(matches!(
                judge_answer(
                    standing,
                    Err(MetaWriteError::Refused(ControlError::NodeRemoved {
                        node: samples::node(3)
                    }))
                ),
                Verdict::Removed
            ));
            assert!(matches!(
                judge_answer(
                    standing,
                    Err(MetaWriteError::Refused(ControlError::FormationCut))
                ),
                Verdict::Refused(ControlError::FormationCut)
            ));
            assert!(matches!(
                judge_answer(standing, Err(MetaWriteError::NoLeader)),
                Verdict::Retry(MetaWriteError::NoLeader)
            ));
            assert!(matches!(
                judge_answer(
                    standing,
                    Err(MetaWriteError::Refused(ControlError::NotBootstrapped))
                ),
                Verdict::Retry(MetaWriteError::Refused(ControlError::NotBootstrapped))
            ));
            assert!(matches!(
                judge_answer(standing, Ok(ControlApplied::TornDown)),
                Verdict::Unexpected(ControlApplied::TornDown)
            ));
        }
    }
}

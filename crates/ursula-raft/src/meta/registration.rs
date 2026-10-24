//! This node's registration with the meta group (design 3.6).
//!
//! A node registers itself at every start, and again after
//! [`REREGISTER_AFTER`] without a live meta leader, naming the root it runs on
//! and, as `empty`, whether the meta group never bound that root
//! (`RootStanding::Unbound`). The first answer is recorded as the root's
//! binding, `fsync`ed, before it is published ([`RootGuard::bind`]). It
//! retries a refusal that may pass later and every failure to reach the
//! leader with a capped back-off, and stops at a permanent refusal
//! (`ursula_control::ControlError::requeue`). Each answer is published as
//! the node's [`RegistrationStatus`], which status reports read.
//!
//! `OtherRoot` is judged by how the run stands on its root
//! ([`RootStanding`], [`judge_answer`]): an unbound root, whose data
//! replicas the node fence holds out until it is bound, is held out of meta
//! votes too and registers until an operator releases the slot, so it
//! binds within [`RETRY_MAX`] of the release with no restart, and a bound
//! root, one whose replicas may hold state, fences itself for good.
//! `NodeRemoved` is judged by the standing too: a bound root is the removed
//! life's own process, which treats itself as removed until it restarts,
//! and an unbound root is a new disk started under the id before its
//! removal completed, which is held out like a refused root and registers
//! again with a back-off capped at [`REMOVAL_PENDING_RETRY_MAX`], so it
//! binds with no restart once the id is vacant.
//!
//! The first answer of a run also lets the run delete snapshot objects
//! (`NodeFence::registered`), and a removal stops it for good
//! (`NodeFence::removed`).

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
use ursula_runtime::node_fence::FenceStatus;

use super::handle::MetaHandle;
use super::handle::MetaWriteError;
use super::pause::MetaPausePoint;
use super::pause::MetaPauses;
use super::replica::ActiveMeta;
use super::root::RootGuard;
use crate::RaftGroupHandleRegistry;
use crate::RaftWalError;
use crate::RecoveryTransport;
use crate::log_store::RootStanding;
use crate::peer_cluster::Supersession;
use crate::rt::sync::watch;
use crate::rt::time::Instant;

/// How long a registered node goes without a meta leader before it
/// registers again.
pub const REREGISTER_AFTER: Duration = Duration::from_secs(30);
/// The first pause before a registration is sent again.
pub(crate) const RETRY_MIN: Duration = Duration::from_millis(200);
/// The longest pause before a registration is sent again.
pub(crate) const RETRY_MAX: Duration = Duration::from_secs(5);
/// The longest pause before a registration refused for a pending removal
/// is sent again: the meta leader completes a removal on a pass every
/// `meta::sweep::REMOVAL_RETRY`, and each refused registration is a meta
/// entry.
pub(crate) const REMOVAL_PENDING_RETRY_MAX: Duration = Duration::from_secs(30);

/// The next pause of a capped back-off.
pub(crate) fn back_off(pause: Duration) -> Duration {
    pause.saturating_mul(2).min(RETRY_MAX)
}

/// What this node registers: its id and the root it runs on. Whether the
/// root is `empty` is the guard's standing at each send.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Registrant {
    pub(crate) node: NodeId,
    pub(crate) root: RootId,
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
    /// An unbound root under a removed id whose removal has not completed:
    /// hold it out and register again.
    RemovalPending,
    /// A bound root refused for the slot: fence the process for good.
    Superseded(RootSlot),
    /// The bound root of a removed node: it treats itself so until it
    /// restarts.
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
        Err(MetaWriteError::Refused(ControlError::NodeRemoved { .. })) => match standing {
            RootStanding::Unbound => Verdict::RemovalPending,
            RootStanding::Bound => Verdict::Removed,
        },
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
    mut leaderless: watch::Receiver<MetaLeaderless>,
    status: watch::Sender<RegistrationStatus>,
    pauses: MetaPauses,
    stopped: impl Fn() -> bool,
) {
    let me = registrant.node;
    let mut pause = RETRY_MIN;
    while !stopped() {
        pauses.at(MetaPausePoint::Registration).await;
        let standing = guard.standing();
        let command = ControlCommand::RegisterNode {
            node: me,
            root: registrant.root,
            empty: standing == RootStanding::Unbound,
            now_ms: ursula_runtime::unix_time_ms(),
        };
        match judge_answer(standing, handle.submit(command).await) {
            Verdict::Registered(registered) => {
                pauses.at(MetaPausePoint::RegistrationAnswered).await;
                // Durable before the answer releases the data replicas.
                match guard.bind().await {
                    Ok(()) => {}
                    Err(RaftWalError::Fenced {
                        status: FenceStatus::Closed | FenceStatus::Superseded,
                        ..
                    }) => return,
                    Err(error) => {
                        tracing::error!(
                            error = %ursula_runtime::ErrorChain(&error),
                            "meta registration: could not record this root bound; its data \
                             replicas stay held out and it registers again"
                        );
                        crate::rt::time::sleep(pause).await;
                        pause = back_off(pause);
                        continue;
                    }
                }
                guard.registered();
                status.send_replace(RegistrationStatus::Answered(registered));
                pause = RETRY_MIN;
                wait_without_leader(&mut leaderless, REREGISTER_AFTER).await;
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
            Verdict::RemovalPending => {
                tracing::warn!(
                    node = %me,
                    root = %registrant.root,
                    "meta registration: this node id is removed and its removal has not \
                     completed. Its data replicas stay held out and it registers again until \
                     the meta leader marks the id vacant"
                );
                guard.hold_out();
                status.send_replace(RegistrationStatus::RemovalPending);
                crate::rt::time::sleep(pause).await;
                pause = pause.saturating_mul(2).min(REMOVAL_PENDING_RETRY_MAX);
                continue;
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
                guard.removed();
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

/// Since when this node has known no live meta leader, `None` while its
/// meta replica knows one, as the meta leader's tracker publishes it.
pub type MetaLeaderless = Option<Instant>;

/// How often the meta leader's tracker reads its replica's leader again
/// while its metrics stay unchanged: a leader goes from live to lapsed
/// with no change of the metrics.
const META_LEADER_RECHECK: Duration = Duration::from_millis(500);

/// Follows this node's running meta replica and publishes on `leaderless`
/// since when it has known no live meta leader
/// ([`crate::leadership::knows_live_leader`]): one it leads with a recent
/// quorum, or follows and heard from recently. A follower that hears its
/// leader no more keeps naming it in OpenRaft's metrics, since with
/// pre-vote it raises no term of its own, so the tracker reads the
/// leader's liveness, again every [`META_LEADER_RECHECK`]. A node without
/// a running meta replica knows none, and so does one whose replica
/// stopped while it is still published, whose metrics close at once. Runs
/// until its task is aborted.
pub(crate) async fn track_meta_leader(
    registry: RaftGroupHandleRegistry,
    leaderless: watch::Sender<MetaLeaderless>,
) {
    let mark_leaderless = || {
        leaderless.send_if_modified(|since| {
            let unset = since.is_none();
            if unset {
                *since = Some(Instant::now());
            }
            unset
        });
    };
    let mut changes = registry.subscribe_meta_changes();
    // The replica whose metrics closed, held weakly so the tracker keeps
    // no replica alive and its address cannot name another.
    let mut stopped: Option<Weak<ActiveMeta>> = None;
    loop {
        changes.borrow_and_update();
        let running = registry.meta().filter(|active| {
            !stopped
                .as_ref()
                .is_some_and(|stopped| Weak::ptr_eq(stopped, &Arc::downgrade(active)))
        });
        let Some(active) = running else {
            mark_leaderless();
            if changes.changed().await.is_err() {
                return;
            }
            continue;
        };
        let current = Arc::downgrade(&active);
        let mut metrics = active.core.owner().metrics();
        let contact = active.core.owner().contact().clone();
        drop(active);
        loop {
            let live = crate::leadership::knows_live_leader(&metrics.borrow_watched(), &contact);
            if live {
                leaderless.send_if_modified(|since| since.take().is_some());
            } else {
                mark_leaderless();
            }
            crate::rt::select! {
                biased;
                changed = metrics.changed() => {
                    if changed.is_err() {
                        stopped = Some(current);
                        break;
                    }
                }
                changed = changes.changed() => {
                    if changed.is_err() {
                        return;
                    }
                    // Another replica may run now: follow the one published.
                    break;
                }
                () = crate::rt::time::sleep(META_LEADER_RECHECK) => {}
            }
        }
    }
}

/// Waits until this node has gone `window` without a meta leader, counted
/// from the call at the earliest, as [`track_meta_leader`] publishes it on
/// `leaderless`. If the tracker stopped, waits `window`.
async fn wait_without_leader(leaderless: &mut watch::Receiver<MetaLeaderless>, window: Duration) {
    let called = Instant::now();
    loop {
        let since = *leaderless.borrow_and_update();
        let changed = match since {
            None => leaderless.changed().await,
            Some(since) => {
                let remaining = window.saturating_sub(since.max(called).elapsed());
                if remaining.is_zero() {
                    return;
                }
                match crate::rt::time::timeout(remaining, leaderless.changed()).await {
                    Err(_elapsed) => return,
                    Ok(changed) => changed,
                }
            }
        };
        if changed.is_err() {
            crate::rt::time::sleep(window).await;
            return;
        }
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

    /// Each answer, to each standing on the root: only `OtherRoot` and
    /// `NodeRemoved` depend on it, holding an unbound root out and
    /// superseding a bound one, whichever slot it names, and holding an
    /// unbound root out while a removal is pending, where a bound root is
    /// the removed life's. A transient refusal is retried and a
    /// permanent one ends the registration, for both.
    #[test]
    fn an_other_root_answer_holds_an_unbound_root_out_and_supersedes_a_bound_one() {
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
            // A removal is the removed life's own for a bound root, and a
            // pending one for a new disk.
            match (
                standing,
                judge_answer(
                    standing,
                    Err(MetaWriteError::Refused(ControlError::NodeRemoved {
                        node: samples::node(3),
                    })),
                ),
            ) {
                (RootStanding::Unbound, Verdict::RemovalPending)
                | (RootStanding::Bound, Verdict::Removed) => {}
                (standing, verdict) => panic!("{standing:?} removed: {verdict:?}"),
            }
            assert!(matches!(
                judge_answer(
                    standing,
                    Err(MetaWriteError::Refused(ControlError::NodeVacant {
                        node: samples::node(3)
                    }))
                ),
                Verdict::Refused(ControlError::NodeVacant { .. })
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

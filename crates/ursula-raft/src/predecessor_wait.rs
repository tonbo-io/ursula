//! The predecessor wait, and the leadership shed of the node fence.
//!
//! A run that reads a predecessor that may still run
//! (`Predecessor::MayLive`) owes a wait of
//! [`PREDECESSOR_WAIT`] before it grants a vote, takes a transfer, opens a
//! recovery gate or writes a snapshot object. No successor can read its
//! predecessor's lease, so each waits out the longest one allowed, with
//! room for clock rates that differ by [`CLOCK_RATE_BOUND`] and a lease
//! check that precedes its write by [`CHECK_MARGIN`]. A root that the
//! meta group bound waits from its open. A root its identity file records
//! unbound (`RootStanding::Unbound`) owes the wait from the meta group's
//! first answer to its registration on, once a bootstrapped cluster binds
//! it, and an answer before Bootstrap settles it at once: no data group
//! holds data before Bootstrap, and a forming cluster's meta group must
//! elect. The registration records that answer's binding, `fsync`ed,
//! before it publishes the answer. At its end the run records the wait
//! finished, `fsync`ed, so no later run owes it again, and a restart during
//! it waits again.
//!
//! An unbound root's data replicas are held out from its open until that
//! answer and then through the wait ([`fence_holds`]): a new disk under a
//! live id grants no data vote and opens no data gate while meta refuses
//! it, across restarts, and its floors come from barrier rounds after its
//! predecessor stopped. A run whose meta replica does not open gets no
//! answer, so an unbound root's data stays held for that whole run. Its
//! meta replica waits only for an owed wait, so a forming cluster's meta
//! group elects before any registration is answered.
//!
//! While the fence does not admit Raft traffic or a wait is owed, the node
//! sheds leadership for [`LeadershipShedReason::Fence`]: none of its groups
//! campaigns or takes a transfer.

use std::time::Duration;

use ursula_control::RegistrationStatus;
use ursula_proto::placement::Registered;
use ursula_runtime::NodeFence;
use ursula_runtime::node_fence::FenceStatus;
use ursula_runtime::node_fence::fence_now;

use crate::LeadershipShedReason;
use crate::PredecessorWait;
use crate::RaftGroupHandleRegistry;
use crate::RaftTarget;
use crate::RaftWalError;
use crate::WaitFrom;
use crate::rt::sync::watch;

/// The longest `raft.wal.lease` allowed, which every wait covers.
pub const LONGEST_LEASE: Duration = ursula_config::config::WalConfig::MAX_LEASE;
/// How far apart the monotonic clock rates of two nodes may be, as for any
/// leader lease: a factor of 1.1, in tenths.
pub const CLOCK_RATE_BOUND: (u32, u32) = (11, 10);
/// How long a lease check may precede the write it guards.
pub const CHECK_MARGIN: Duration = Duration::from_secs(1);
/// How long a successor waits out a predecessor that may still run:
/// [`LONGEST_LEASE`] stretched by [`CLOCK_RATE_BOUND`], plus
/// [`CHECK_MARGIN`].
pub const PREDECESSOR_WAIT: Duration = Duration::from_secs(12);
/// How long the wait pauses before it records its end again after a
/// failed record.
const RECORD_RETRY: Duration = Duration::from_secs(1);

/// Whether `fence` holds a replica of `target` out now: no vote but the
/// bootstrap probe's, no gate driver and no vote floor. The one policy the
/// vote screen, the gate drivers and the barrier read.
pub(crate) fn fence_holds(fence: &NodeFence, target: RaftTarget) -> bool {
    match target {
        RaftTarget::Meta => fence.owes_wait(),
        RaftTarget::Data(_) => fence.holds_data(),
    }
}

/// Returns once `fence` no longer holds a replica of `target` out
/// ([`fence_holds`]).
pub(crate) async fn fence_released(fence: &NodeFence, target: RaftTarget) {
    match target {
        RaftTarget::Meta => fence.waited().await,
        RaftTarget::Data(_) => fence.bound_and_waited().await,
    }
}

/// Whether `fence` holds the barrier rounds of a replica of `target` and
/// the floor they write ([`fence_holds`]). A negative control turns it off
/// (`Defence::GateAfterWait`), and they then run during the hold.
pub(crate) fn fence_holds_barrier(fence: &NodeFence, target: RaftTarget) -> bool {
    use ursula_stream::sim_controls::Defence;
    if !fence_holds(fence, target) {
        return false;
    }
    if ursula_stream::sim_controls::off(Defence::GateAfterWait) {
        ursula_stream::sim_controls::note_fired(Defence::GateAfterWait);
        return false;
    }
    true
}

/// Returns once `fence` no longer holds the barrier rounds of a replica of
/// `target` ([`fence_holds_barrier`]).
pub(crate) async fn barrier_released(fence: &NodeFence, target: RaftTarget) {
    if fence_holds_barrier(fence, target) {
        fence_released(fence, target).await;
    }
}

/// Marks [`LeadershipShedReason::Fence`] while `fence` does not admit or a
/// wait is owed, and clears it otherwise. Returns whether it is marked.
pub fn sync_fence_shed(registry: &RaftGroupHandleRegistry, fence: &NodeFence) -> bool {
    let fenced = fence.owes_wait() || !fence.admits_now();
    if fenced {
        registry.mark_leadership_shed(LeadershipShedReason::Fence);
    } else {
        registry.clear_leadership_shed(LeadershipShedReason::Fence);
    }
    fenced
}

/// Keeps [`LeadershipShedReason::Fence`] in step with a leased `fence`,
/// checking every [`crate::PROBE_INTERVAL`], and logs each change of the
/// fence's status. Ends at once for an unleased fence.
pub async fn run_fence_gate(registry: RaftGroupHandleRegistry, fence: NodeFence) {
    if fence.lease_length().is_none() {
        return;
    }
    let mut last = FenceStatus::Admits;
    loop {
        sync_fence_shed(&registry, &fence);
        let status = fence.status(fence_now());
        if status != last {
            match status {
                FenceStatus::DiskLeaseLost => tracing::error!(
                    lease = ?fence.lease_length(),
                    "disk lease lost: this node stops all Raft traffic and object writes, leads \
                     no group and answers clients 503 until a probe of its WAL disk succeeds"
                ),
                FenceStatus::Admits => {
                    tracing::warn!("disk lease holds again: this node takes part in Raft again");
                }
                FenceStatus::Closed => tracing::info!("node fence closed for the shutdown"),
                FenceStatus::Superseded => tracing::error!(
                    "superseded: another process may answer as this node, so this process takes \
                     part in nothing again until it exits"
                ),
            }
            last = status;
        }
        crate::rt::time::sleep(crate::PROBE_INTERVAL).await;
    }
}

/// Waits out the predecessor of a run that owes it a wait
/// ([`crate::RaftWal::predecessor_wait`]), then records the wait finished
/// and lifts the leadership shed. An unbound root's wait starts with the
/// first answer `registration` publishes, which the registration publishes
/// only once it recorded the root bound. The shutdown ends this task
/// before it closes the WAL, and a record after the fence closed is
/// refused, so no end of a wait is written once a successor may run.
pub async fn run_predecessor_wait(
    wait: Option<PredecessorWait>,
    registry: RaftGroupHandleRegistry,
    mut registration: watch::Receiver<RegistrationStatus>,
) {
    let Some(wait) = wait else {
        return;
    };
    let from = wait.from();
    let fence = wait.fence().clone();
    sync_fence_shed(&registry, &fence);
    let started = match from {
        WaitFrom::Open(opened) => Some(opened),
        WaitFrom::Binding => loop {
            match &*registration.borrow() {
                RegistrationStatus::Answered(Registered::Forming) => {
                    // The binding is on disk already.
                    fence.bind(false);
                    break None;
                }
                RegistrationStatus::Answered(_) => {
                    // The binding is on disk already, and the wait begins
                    // before the data hold ends.
                    fence.bind(true);
                    break Some(fence_now());
                }
                RegistrationStatus::Pending
                | RegistrationStatus::RootRefused { .. }
                | RegistrationStatus::Refused(_)
                | RegistrationStatus::Removed => {}
            }
            if registration.changed().await.is_err() {
                return;
            }
        },
    };
    if let Some(started) = started {
        sync_fence_shed(&registry, &fence);
        tracing::warn!(
            wait = ?PREDECESSOR_WAIT,
            ?from,
            "a process of an earlier run of this WAL root may still run: this node grants no \
             vote, takes no transfer, opens no recovery gate and writes no snapshot object until \
             the wait ends"
        );
        let waited = fence_now().saturating_duration_since(started);
        crate::rt::time::sleep(PREDECESSOR_WAIT.saturating_sub(waited)).await;
    }
    while let Err(error) = wait.finish().await {
        if let RaftWalError::Fenced {
            status: FenceStatus::Closed | FenceStatus::Superseded,
            ..
        } = error
        {
            tracing::info!(
                "the run shut down or was superseded before its predecessor wait was recorded \
                 finished"
            );
            return;
        }
        tracing::error!(
            error = %ursula_runtime::ErrorChain(&error),
            "could not record the predecessor wait finished; trying again"
        );
        crate::rt::time::sleep(RECORD_RETRY).await;
    }
    sync_fence_shed(&registry, &fence);
    tracing::info!("predecessor wait finished");
}

#[cfg(test)]
mod tests {
    use super::CHECK_MARGIN;
    use super::CLOCK_RATE_BOUND;
    use super::LONGEST_LEASE;
    use super::PREDECESSOR_WAIT;

    /// The wait covers the longest lease on a clock running the bound
    /// slower, plus the check-to-write margin, so a predecessor's last
    /// write happened before a successor that waited it out takes part.
    #[test]
    fn the_wait_covers_the_longest_lease_at_the_clock_bound_plus_the_margin() {
        let (num, den) = CLOCK_RATE_BOUND;
        let stretched = LONGEST_LEASE
            .checked_mul(num)
            .and_then(|lease| lease.checked_div(den))
            .unwrap();
        assert!(PREDECESSOR_WAIT >= stretched.checked_add(CHECK_MARGIN).unwrap());
        assert!(
            LONGEST_LEASE
                >= ursula_config::config::WalConfig::default()
                    .lease
                    .as_duration()
        );
    }
}

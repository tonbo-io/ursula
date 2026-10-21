//! This node's registration with the meta group (design 3.6).
//!
//! A node registers itself at every start, and again after
//! [`REREGISTER_AFTER`] without a meta leader. It retries a refusal that
//! may pass later and every failure to reach the leader with a capped
//! back-off, and stops at a permanent refusal
//! (`ursula_control::ControlError::requeue`). `NodeRemoved` makes the node
//! treat itself as removed until it restarts. Each answer is published as
//! the node's [`RegistrationStatus`], which status reports read.

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

use super::handle::MetaHandle;
use super::handle::MetaWriteError;
use super::pause::MetaPausePoint;
use super::pause::MetaPauses;
use super::replica::ActiveMeta;
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

/// Registers node `me` until the meta group answers for good, publishing
/// each answer on `status`. Ends at a permanent refusal, or once `stopped`
/// is true.
pub(crate) async fn run_registration<T: RecoveryTransport>(
    handle: MetaHandle<T>,
    me: NodeId,
    registry: RaftGroupHandleRegistry,
    status: watch::Sender<RegistrationStatus>,
    pauses: MetaPauses,
    stopped: impl Fn() -> bool,
) {
    let mut pause = RETRY_MIN;
    while !stopped() {
        pauses.at(MetaPausePoint::Registration).await;
        let command = ControlCommand::RegisterNode {
            node: me,
            now_ms: ursula_runtime::unix_time_ms(),
        };
        match handle.submit(command).await {
            Ok(ControlApplied::NodeRegistered(registered)) => {
                status.send_replace(RegistrationStatus::Answered(registered));
                pause = RETRY_MIN;
                wait_without_leader(&registry, REREGISTER_AFTER).await;
                continue;
            }
            Ok(other) => {
                tracing::error!(
                    ?other,
                    "meta registration: the meta group answered another command"
                );
                return;
            }
            Err(MetaWriteError::Refused(ControlError::NodeRemoved { .. })) => {
                tracing::warn!(node = %me, "meta registration: this node is removed");
                status.send_replace(RegistrationStatus::Removed);
                return;
            }
            Err(MetaWriteError::Refused(refusal)) if refusal.requeue() == Requeue::Permanent => {
                tracing::error!(%refusal, "meta registration: refused for good");
                status.send_replace(RegistrationStatus::Refused(refusal));
                return;
            }
            Err(error) => {
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

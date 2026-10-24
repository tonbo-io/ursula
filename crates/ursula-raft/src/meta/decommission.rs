//! The meta leader's decommission driver and its report (design 8.2,
//! 11.1).
//!
//! While this node's meta replica leads under a committed vote with a
//! settled history, the driver runs a pass every [`DECOMMISSION_INTERVAL`]
//! and on each new view. A pass reads each node's eligibility
//! ([`super::liveness`]), plans with `plan_decommission` over the view, and
//! submits the planned moves as `RequestMove` in order. The first refusal
//! ends the pass, since the later moves were planned with it: a refusal
//! whose class is `ConfirmView` (`ControlError::requeue`) confirms the view
//! and plans again at once, any other plans again on the next pass. The
//! driver only proposes: the kernel decides each move against the state it
//! applies on, and its placement CAS refuses a plan from an old state.
//!
//! Each pass publishes the [`MetaLeaderReport`]: the eligibility it planned
//! with, the groups the plan could not move, the age of the oldest move,
//! the last refusal of this node's `Bootstrap` and what each removal still
//! waits for ([`super::sweep`]). A node that does not lead publishes none.

use std::collections::BTreeMap;
use std::time::Duration;

use openraft::rt::WatchReceiver;
use ursula_control::ControlCommand;
use ursula_control::ControlError;
use ursula_control::DecommissionMove;
use ursula_control::MetaLeaderReport;
use ursula_control::Requeue;
use ursula_control::plan_decommission;
use ursula_proto::placement::NodeId;

use super::handle::MetaHandle;
use super::handle::MetaWriteError;
use super::liveness::Liveness;
use super::sweep::RemovalPending;
use crate::RaftGroupHandleRegistry;
use crate::RecoveryTransport;
use crate::rt::sync::watch;

/// How long the driver waits for a new view before its next pass.
pub const DECOMMISSION_INTERVAL: Duration = Duration::from_secs(1);

/// What the decommission driver of node `me` runs with.
pub(crate) struct DecommissionDriver<T> {
    pub(crate) me: NodeId,
    pub(crate) handle: MetaHandle<T>,
    pub(crate) registry: RaftGroupHandleRegistry,
    pub(crate) liveness: Liveness,
    /// The last refusal of this node's `Bootstrap`, which the report
    /// carries.
    pub(crate) bootstrap_refusal: watch::Receiver<Option<ControlError>>,
    /// What each removal still waits for ([`super::sweep`]), which the
    /// report carries.
    pub(crate) removal_pending: watch::Receiver<RemovalPending>,
    pub(crate) report: watch::Sender<Option<MetaLeaderReport>>,
}

/// This node's meta leadership, as a pass reads it.
pub(super) struct Leading {
    pub(super) term: u64,
    /// The age of each node's last acknowledgement of this leader.
    acked: BTreeMap<NodeId, Duration>,
}

/// How the last pass ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PassEnd {
    /// Wait for a view or the interval.
    Wait,
    /// A refusal asked for a confirmed view, which the pass took: plan
    /// again at once.
    PlanAgain,
}

impl<T: RecoveryTransport> DecommissionDriver<T> {
    /// Runs passes until `stopped` is true (see the module docs).
    pub(crate) async fn run(mut self, stopped: impl Fn() -> bool) {
        let mut views = self.registry.placement_watch();
        // The term this node leads in, and since when (display only).
        let mut leadership: Option<(u64, u64)> = None;
        let mut end = PassEnd::Wait;
        while !stopped() {
            // A pass plans again at once only after a wait, so refusals
            // never spin.
            let again = end == PassEnd::PlanAgain;
            end = PassEnd::Wait;
            if !again {
                match crate::rt::time::timeout(DECOMMISSION_INTERVAL, views.changed()).await {
                    Ok(true) | Err(_) => {}
                    // The views outlive the driver: wait out the interval.
                    Ok(false) => crate::rt::time::sleep(DECOMMISSION_INTERVAL).await,
                }
            }
            let Some(leading) = leading(&self.registry, self.me) else {
                leadership = None;
                self.report
                    .send_if_modified(|report| report.take().is_some());
                continue;
            };
            let now_ms = ursula_runtime::unix_time_ms();
            let since_ms = match leadership {
                Some((term, since_ms)) if term == leading.term => since_ms,
                Some(_) | None => now_ms,
            };
            leadership = Some((leading.term, since_ms));
            let eligibility = self.liveness.eligibility(&leading.acked).await;
            let view = views.current();
            let (blocked, oldest_move_since_ms) = match view.cluster() {
                Some(cluster) => {
                    let plan = plan_decommission(cluster, &eligibility);
                    let submitted = self.submit(plan.moves).await;
                    if !again {
                        end = submitted;
                    }
                    (plan.blocked, cluster.oldest_move_since_ms())
                }
                None => (Vec::new(), None),
            };
            self.report.send_replace(Some(MetaLeaderReport {
                leader: self.me,
                leader_since_ms: since_ms,
                eligibility,
                decommission_blocked: blocked,
                removal_pending: self.removal_pending.borrow().clone(),
                last_bootstrap_refusal: self.bootstrap_refusal.borrow().clone(),
                oldest_move_age_ms: oldest_move_since_ms
                    .map(|since_ms| now_ms.saturating_sub(since_ms)),
            }));
        }
    }

    /// Submits `moves` in order, up to the first that does not apply.
    async fn submit(&self, moves: Vec<DecommissionMove>) -> PassEnd {
        for planned in moves {
            let DecommissionMove {
                group,
                expected,
                to,
            } = planned;
            let command = ControlCommand::RequestMove {
                group,
                expected,
                to,
                now_ms: ursula_runtime::unix_time_ms(),
            };
            match self.handle.submit(command).await {
                Ok(_accepted) => {
                    tracing::info!(group = group.0, "decommission: moving the group away");
                }
                Err(MetaWriteError::Refused(error)) => {
                    tracing::info!(group = group.0, %error, "decommission: the move was refused; planning again");
                    return match error.requeue() {
                        Requeue::ConfirmView => match self.handle.confirm_view().await {
                            Ok(_confirmed) => PassEnd::PlanAgain,
                            Err(error) => {
                                tracing::debug!(%error, "decommission: no confirmed view");
                                PassEnd::Wait
                            }
                        },
                        Requeue::Retry | Requeue::Permanent => PassEnd::Wait,
                    };
                }
                Err(error) => {
                    tracing::debug!(group = group.0, %error, "decommission: the move was not answered");
                    return PassEnd::Wait;
                }
            }
        }
        PassEnd::Wait
    }
}

/// Node `me`'s meta leadership, when its meta replica leads under a
/// committed vote with a settled history.
pub(super) fn leading(registry: &RaftGroupHandleRegistry, me: NodeId) -> Option<Leading> {
    let active = registry.meta()?;
    if active.core.history.pending() {
        return None;
    }
    let watched = active.core.owner().metrics();
    let metrics = watched.borrow_watched();
    if metrics.current_leader != Some(me.get()) || !metrics.vote.is_committed() {
        return None;
    }
    let acked = metrics
        .heartbeat
        .iter()
        .flatten()
        .filter_map(|(node, acked)| {
            let acked = acked.as_ref()?;
            Some((NodeId::new(*node), openraft::Instant::elapsed(&**acked)))
        })
        .collect();
    Some(Leading {
        term: metrics.current_term,
        acked,
    })
}

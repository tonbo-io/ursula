//! The meta leader's `Bootstrap` of the cluster (design 3.6).
//!
//! Once this node leads the meta group, its view applied the genesis
//! history and shows the cluster `Forming`, and every `raft.peers` id
//! registered, it submits `Bootstrap` with meta's committed voters, the
//! claimed group count, the effective replication factor over the nodes,
//! and the node's move limit and maintenance authority. It keeps the last
//! refusal for status reports, retries by the refusal's class (confirming
//! its view first when the refusal says the view is stale), and stops at a
//! permanent refusal, once bootstrapped, or once a meta plan cut the
//! formation.

use std::time::Duration;

use openraft::rt::WatchReceiver;
use ursula_control::ClusterParams;
use ursula_control::ControlCommand;
use ursula_control::ControlError;
use ursula_control::ControlPlaneState;
use ursula_control::MaintenanceAuthority;
use ursula_control::MembershipConfig;
use ursula_control::MoveLimit;
use ursula_control::Requeue;
use ursula_control::effective_replication_factor;
use ursula_proto::placement::GenesisClaim;
use ursula_proto::placement::NodeId;
use ursula_proto::placement::VoterSet;

use super::handle::MetaHandle;
use super::handle::MetaWriteError;
use super::registration::RETRY_MIN;
use super::registration::back_off;
use crate::RaftGroupHandleRegistry;
use crate::RecoveryTransport;
use crate::memberships::MembershipKind;
use crate::memberships::membership_config;
use crate::rt::sync::watch;

/// How long the formation waits for a view change before it looks at the
/// meta leadership again.
const FORMATION_RECHECK: Duration = Duration::from_secs(1);

/// What this node bootstraps the cluster with, beside its genesis claim.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FormationParams {
    pub max_concurrent_moves: MoveLimit,
    /// `Reservation` exactly under the startup reservation.
    pub maintenance: MaintenanceAuthority,
}

/// Bootstraps the cluster from node `me`'s meta leadership (see the module
/// docs), publishing the last refusal on `refusal`. Ends once `stopped` is
/// true.
pub(crate) async fn run_formation<T: RecoveryTransport>(
    handle: MetaHandle<T>,
    me: NodeId,
    claim: GenesisClaim,
    params: FormationParams,
    registry: RaftGroupHandleRegistry,
    refusal: watch::Sender<Option<ControlError>>,
    stopped: impl Fn() -> bool,
) {
    let mut watch = registry.placement_watch();
    let mut pause = RETRY_MIN;
    while !stopped() {
        let view = watch.current();
        let ready = match &*view.state {
            ControlPlaneState::Bootstrapped(_) | ControlPlaneState::FormationCut => return,
            ControlPlaneState::Forming { registered } => {
                view.applied().is_some()
                    && claim
                        .peers
                        .iter()
                        .all(|peer| registered.contains_key(&peer))
            }
        };
        let voters = if ready {
            leading_voters(&registry, me)
        } else {
            None
        };
        let Some(meta_voters) = voters else {
            // A wait bounded by the recheck, since the meta leadership can
            // change without a new view.
            let _changed = crate::rt::time::timeout(FORMATION_RECHECK, watch.changed()).await;
            continue;
        };
        let command = ControlCommand::Bootstrap {
            params: ClusterParams {
                group_count: claim.group_count,
                replication_factor: effective_replication_factor(
                    claim.replication_factor,
                    &claim.peers,
                ),
                meta_voters,
                max_concurrent_moves: params.max_concurrent_moves,
                maintenance: params.maintenance,
            },
            nodes: claim.peers.clone(),
            now_ms: ursula_runtime::unix_time_ms(),
        };
        match handle.submit(command).await {
            Ok(_bootstrapped) => return,
            Err(MetaWriteError::Refused(error)) => {
                tracing::warn!(%error, "meta formation: Bootstrap refused");
                let requeue = error.requeue();
                refusal.send_replace(Some(error));
                match requeue {
                    Requeue::Permanent => return,
                    Requeue::ConfirmView => {
                        if let Err(error) = handle.confirm_view().await {
                            tracing::debug!(%error, "meta formation: no confirmed view");
                        }
                    }
                    Requeue::Retry => {}
                }
            }
            Err(error) => tracing::debug!(%error, "meta formation: Bootstrap not answered"),
        }
        crate::rt::time::sleep(pause).await;
        pause = back_off(pause);
    }
}

/// Meta's committed voters, when node `me` leads the meta group under a
/// committed vote with a uniform committed membership.
fn leading_voters(registry: &RaftGroupHandleRegistry, me: NodeId) -> Option<VoterSet> {
    let active = registry.meta()?;
    let watched = active.core.owner().metrics();
    let metrics = watched.borrow_watched();
    if metrics.current_leader != Some(me.get()) || !metrics.vote.is_committed() {
        return None;
    }
    match membership_config(
        &metrics.committed_membership_config,
        MembershipKind::Committed,
    )
    .ok()?
    {
        MembershipConfig::Uniform { voters, .. } => Some(voters),
        MembershipConfig::Joint { .. } => None,
    }
}

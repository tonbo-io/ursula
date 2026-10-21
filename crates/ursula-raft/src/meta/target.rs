//! The meta group's membership target (design 5.1), which the membership
//! reconcile shell pursues on the meta leader.
//!
//! The target keeps meta's committed voters, so the shell never changes a
//! meta voter, and adds every registered node, every node that is not
//! `Removed` once bootstrapped, as a learner. Its stage is always
//! `Settled`, and meta has no peer gates, since meta never promotes. An
//! empty view and a cut formation give no target: a meta recovery plan's
//! cut leaves the membership as the plan made it.

use futures_util::future::BoxFuture;
use openraft::rt::WatchReceiver;
use ursula_control::ConfirmedStamp;
use ursula_control::ControlPlaneState;
use ursula_control::MembershipConfig;
use ursula_control::MembershipTarget;
use ursula_control::PlacementView;
use ursula_control::TargetStage;
use ursula_proto::placement::NodeId;
use ursula_proto::placement::NodeLifecycle;

use super::confirm::MetaBarrierError;
use super::handle::MetaHandle;
use super::state_machine::MetaStateMachine;
use super::watch::PlacementWatch;
use crate::RecoveryTransport;
use crate::memberships::MembershipKind;
use crate::memberships::membership_config;
use crate::owner::OwnerRaftHandle;
use crate::rejoin::MembershipTargetSource;
use crate::rejoin::TargetRead;

/// The meta group's target source: the local meta replica's committed
/// membership and placement view, and meta read barriers through the
/// node's [`MetaHandle`].
pub(crate) struct MetaTargetSource<T> {
    pub(crate) owner: OwnerRaftHandle<MetaStateMachine>,
    pub(crate) watch: PlacementWatch,
    pub(crate) handle: MetaHandle<T>,
}

impl<T: RecoveryTransport> MembershipTargetSource for MetaTargetSource<T> {
    fn target(&self) -> Option<TargetRead> {
        let committed = {
            let watched = self.owner.metrics();
            let metrics = watched.borrow_watched();
            membership_config(
                &metrics.committed_membership_config,
                MembershipKind::Committed,
            )
            .ok()?
        };
        meta_target(&self.watch.current(), &committed)
    }

    fn confirm(&self) -> BoxFuture<'_, Result<ConfirmedStamp, MetaBarrierError>> {
        Box::pin(async move {
            self.handle
                .confirm_view()
                .await
                .map(|confirmed| confirmed.stamp())
        })
    }
}

/// The meta target of `view` over the committed meta membership
/// `committed` (see the module docs). A joint membership targets its new
/// voters.
pub(crate) fn meta_target(
    view: &PlacementView,
    committed: &MembershipConfig,
) -> Option<TargetRead> {
    let voters = match committed {
        MembershipConfig::Uniform { voters, .. } | MembershipConfig::Joint { new: voters, .. } => {
            voters.clone()
        }
    };
    let nodes: Vec<NodeId> = match &*view.state {
        _ if view.applied().is_none() => return None,
        ControlPlaneState::FormationCut => return None,
        ControlPlaneState::Forming { registered } => registered.keys().copied().collect(),
        ControlPlaneState::Bootstrapped(cluster) => cluster
            .nodes()
            .iter()
            .filter(|(_, record)| record.lifecycle != NodeLifecycle::Removed)
            .map(|(node, _)| *node)
            .collect(),
    };
    let learners = nodes
        .into_iter()
        .filter(|node| !voters.contains(*node))
        .collect();
    Some(TargetRead {
        target: MembershipTarget {
            voters,
            learners,
            stage: TargetStage::Settled,
        },
        view: view.stage,
    })
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use ursula_control::ControlPlaneState;
    use ursula_control::MembershipConfig;
    use ursula_control::PlacementView;
    use ursula_control::Registration;
    use ursula_control::TargetStage;
    use ursula_control::samples;
    use ursula_proto::placement::ViewStage;

    use super::meta_target;

    fn applied(state: ControlPlaneState) -> PlacementView {
        PlacementView {
            stage: ViewStage::Applied {
                applied: samples::meta(2, 9),
                history: samples::meta(2, 1),
            },
            state: Arc::new(state),
        }
    }

    /// Meta's target keeps the committed voters and adds the registered
    /// nodes, then every node not removed, as learners, always settled. An
    /// empty view and a cut formation give none. A source that read the
    /// view's meta voters instead of the committed ones, kept a removed
    /// node, or listed a voter as a learner, gives another target.
    #[test]
    fn the_meta_target_keeps_the_voters_and_adds_every_live_node_as_a_learner() {
        let committed = MembershipConfig::Uniform {
            voters: samples::voters(&[1, 2]),
            learners: samples::ids(&[9]),
        };
        assert_eq!(meta_target(&PlacementView::empty(), &committed), None);
        assert_eq!(
            meta_target(&applied(ControlPlaneState::FormationCut), &committed),
            None
        );

        let registered = [1, 3, 4]
            .map(|node| (samples::node(node), Registration { since_ms: node }))
            .into_iter()
            .collect::<BTreeMap<_, _>>();
        let forming = applied(ControlPlaneState::Forming { registered });
        let read = meta_target(&forming, &committed).unwrap();
        assert_eq!(read.view, forming.stage);
        assert_eq!(read.target.voters, samples::voters(&[1, 2]));
        assert_eq!(read.target.learners, samples::ids(&[3, 4]));
        assert_eq!(read.target.stage, TargetStage::Settled);

        // The sample cluster: 1 active, 2 cordoned, 3 decommissioning, 4
        // removed and 5 active, with meta voters {1,2} in its params. The
        // committed voters {1,5} differ from those, and stay the target's:
        // read from the view, the target would be voters {1,2} with
        // learners {3,5}. The removed node is no learner.
        let committed = MembershipConfig::Uniform {
            voters: samples::voters(&[1, 5]),
            learners: samples::ids(&[2]),
        };
        let bootstrapped = applied(samples::sample_state());
        let read = meta_target(&bootstrapped, &committed).unwrap();
        assert_eq!(read.target.voters, samples::voters(&[1, 5]));
        assert_eq!(read.target.learners, samples::ids(&[2, 3]));
    }
}

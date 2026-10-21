//! This node's placement status (`GET /__ursula/placement`, design 11.1).
//!
//! [`MetaStatusReader`] reads the meta runtime's watches and the registry's
//! meta replica and placement view at once, and derives the view's
//! [`ClusterView`] stage ([`cluster_view`]). A confirmed status
//! ([`MetaStatusReader::confirmed_placement_status`]) derives it from a view
//! a meta read barrier confirmed current.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use openraft::rt::WatchReceiver;
use ursula_control::ClusterView;
use ursula_control::ConfirmedView;
use ursula_control::ControlPlaneState;
use ursula_control::MetaReplicaStatus;
use ursula_control::MetaRole;
use ursula_control::PlacementStatus;
use ursula_control::PlacementView;
use ursula_proto::placement::GenesisClaim;
use ursula_proto::placement::NodeId;

use super::confirm::MetaBarrierError;
use super::genesis_driver::GenesisStatus;
use super::handle::MetaHandle;
use super::runtime::MetaRuntimeState;
use super::runtime::MetaRuntimeStatus;
use crate::RaftGroupHandleRegistry;
use crate::RecoveryTransport;

type Confirmation<'a> =
    Pin<Box<dyn Future<Output = Result<ConfirmedView, MetaBarrierError>> + Send + 'a>>;

/// A meta read barrier on this node's view, whatever the transport.
trait ConfirmView: Send + Sync {
    fn confirm_view(&self) -> Confirmation<'_>;
}

impl<T: RecoveryTransport> ConfirmView for MetaHandle<T> {
    fn confirm_view(&self) -> Confirmation<'_> {
        Box::pin(MetaHandle::confirm_view(self))
    }
}

/// What a placement status reads of this node's meta runtime. Cheap to
/// clone, so a reader takes it out of any lock before it awaits.
#[derive(Clone)]
pub struct MetaStatusReader {
    node: NodeId,
    claim: GenesisClaim,
    status: MetaRuntimeStatus,
    registry: RaftGroupHandleRegistry,
    confirm: Arc<dyn ConfirmView>,
}

impl std::fmt::Debug for MetaStatusReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MetaStatusReader")
            .field("node", &self.node)
            .finish_non_exhaustive()
    }
}

impl MetaStatusReader {
    pub(crate) fn new<T: RecoveryTransport>(
        node: NodeId,
        claim: GenesisClaim,
        status: MetaRuntimeStatus,
        registry: RaftGroupHandleRegistry,
        handle: MetaHandle<T>,
    ) -> Self {
        Self {
            node,
            claim,
            status,
            registry,
            confirm: Arc::new(handle),
        }
    }

    /// Whether this node runs a meta replica now.
    pub fn state(&self) -> MetaRuntimeState {
        self.status.state.borrow().clone()
    }

    /// This node's placement status, from its current view.
    pub fn placement_status(&self) -> PlacementStatus {
        self.status_of(&self.registry.placement_watch().current())
    }

    /// This node's placement status, from a view a meta read barrier
    /// confirmed current.
    pub async fn confirmed_placement_status(&self) -> Result<PlacementStatus, MetaBarrierError> {
        let confirmed = self.confirm.confirm_view().await?;
        Ok(self.status_of(confirmed.view()))
    }

    fn status_of(&self, view: &PlacementView) -> PlacementStatus {
        let state = self.status.state.borrow().clone();
        let (meta, formed_elsewhere) = match (state, self.registry.meta()) {
            (MetaRuntimeState::Damaged { kind, message }, _) => {
                (MetaReplicaStatus::Damaged { kind, message }, false)
            }
            (MetaRuntimeState::Running, None) => (MetaReplicaStatus::Absent, false),
            (MetaRuntimeState::Running, Some(active)) => {
                let watched = active.core.owner().metrics();
                let metrics = watched.borrow_watched();
                let membership = metrics.membership_config.membership();
                let me = self.node.get();
                let role = if membership.voter_ids().any(|id| id == me) {
                    MetaRole::Voter
                } else if membership.learner_ids().any(|id| id == me) {
                    MetaRole::Learner
                } else {
                    MetaRole::NotMember
                };
                let leader = metrics.current_leader.map(NodeId::new);
                let formed = leader.is_some() || metrics.current_term > 0;
                (MetaReplicaStatus::Running { role, leader }, formed)
            }
        };
        PlacementStatus {
            node: self.node,
            view: cluster_view(
                view,
                &self.claim,
                self.node,
                &self.status.genesis.borrow(),
                formed_elsewhere,
            ),
            applied: view.applied(),
            meta,
            registration: self.status.registration.borrow().clone(),
            last_bootstrap_refusal: self.status.bootstrap_refusal.borrow().clone(),
        }
    }
}

/// The stage of `view` on node `me`, which claims `claim`.
///
/// An applied view gives its state's stage: `Forming` gives the registered
/// ids and the claimed peers not registered yet. An empty view gives
/// `MetaCatchingUp` when the replica sees a leader or a vote above term 0
/// (`formed_elsewhere`), else `MetaGenesis`: from `genesis` on the genesis
/// node ([`GenesisClaim::is_genesis_node`]), and with every other peer
/// unanswered elsewhere.
pub(crate) fn cluster_view(
    view: &PlacementView,
    claim: &GenesisClaim,
    me: NodeId,
    genesis: &GenesisStatus,
    formed_elsewhere: bool,
) -> ClusterView {
    if view.applied().is_some() {
        return match &*view.state {
            ControlPlaneState::Forming { registered } => ClusterView::Registering {
                registered: registered.keys().copied().collect(),
                waiting_for: claim
                    .peers
                    .iter()
                    .filter(|peer| !registered.contains_key(peer))
                    .collect(),
            },
            ControlPlaneState::FormationCut => ClusterView::FormationCut,
            ControlPlaneState::Bootstrapped(cluster) => ClusterView::Bootstrapped(cluster.clone()),
        };
    }
    if formed_elsewhere {
        return ClusterView::MetaCatchingUp;
    }
    let unanswered = if claim.is_genesis_node(me) {
        genesis.unanswered.clone()
    } else {
        claim.peers.iter().filter(|peer| *peer != me).collect()
    };
    ClusterView::MetaGenesis {
        unanswered,
        genesis_mismatch: genesis.mismatch.clone(),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use ursula_control::ClusterView;
    use ursula_control::ControlPlaneState;
    use ursula_control::PlacementView;
    use ursula_control::Registration;
    use ursula_control::samples;
    use ursula_proto::placement::GenesisClaim;
    use ursula_proto::placement::ViewStage;

    use super::GenesisStatus;
    use super::cluster_view;

    fn claim() -> GenesisClaim {
        GenesisClaim {
            meta_voters: samples::voters(&[2, 3]),
            peers: samples::node_set(&[2, 3, 4]),
            ..samples::sample_genesis_claim()
        }
    }

    fn applied(state: ControlPlaneState) -> PlacementView {
        PlacementView {
            stage: ViewStage::Applied {
                applied: samples::meta(2, 9),
                history: samples::meta(1, 1),
            },
            state: Arc::new(state),
        }
    }

    /// Each stage of the status, and the one rule that picks it: an
    /// applied view by its state, an empty view by whether meta formed
    /// elsewhere and whether this node is the genesis node.
    #[test]
    fn every_view_stage_is_derived_from_the_view_and_the_replica() {
        let genesis = GenesisStatus {
            unanswered: samples::ids(&[4]),
            mismatch: BTreeMap::from([(samples::node(3), samples::sample_genesis_claim())]),
        };
        let (me, other) = (samples::node(2), samples::node(4));
        let forming = applied(ControlPlaneState::Forming {
            registered: BTreeMap::from([(samples::node(3), Registration { since_ms: 5 })]),
        });
        assert_eq!(
            cluster_view(&forming, &claim(), me, &genesis, true),
            ClusterView::Registering {
                registered: samples::ids(&[3]),
                waiting_for: samples::ids(&[2, 4]),
            }
        );
        assert_eq!(
            cluster_view(
                &applied(ControlPlaneState::FormationCut),
                &claim(),
                me,
                &genesis,
                false
            ),
            ClusterView::FormationCut
        );
        let ControlPlaneState::Bootstrapped(cluster) = samples::sample_state() else {
            panic!("the sample state is bootstrapped");
        };
        assert_eq!(
            cluster_view(
                &applied(ControlPlaneState::Bootstrapped(cluster.clone())),
                &claim(),
                other,
                &genesis,
                false
            ),
            ClusterView::Bootstrapped(cluster)
        );
        let empty = PlacementView::empty();
        assert_eq!(
            cluster_view(&empty, &claim(), other, &genesis, true),
            ClusterView::MetaCatchingUp
        );
        // The genesis node reports what its probes heard.
        assert_eq!(
            cluster_view(&empty, &claim(), me, &genesis, false),
            ClusterView::MetaGenesis {
                unanswered: samples::ids(&[4]),
                genesis_mismatch: genesis.mismatch.clone(),
            }
        );
        // Any other node counts every other peer unanswered.
        assert_eq!(
            cluster_view(&empty, &claim(), other, &genesis, false),
            ClusterView::MetaGenesis {
                unanswered: samples::ids(&[2, 3]),
                genesis_mismatch: genesis.mismatch.clone(),
            }
        );
    }
}

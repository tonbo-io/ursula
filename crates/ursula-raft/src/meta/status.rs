//! This node's placement status (`GET /__ursula/placement`, design 11.1).
//!
//! [`MetaStatusReader`] reads the meta runtime's watches and the registry's
//! meta replica and placement view at once, and derives the view's
//! [`ClusterView`] stage ([`cluster_view`]). A confirmed status
//! ([`MetaStatusReader::confirmed_placement_status`]) derives it from a view
//! a meta read barrier confirmed current. The reader also carries the
//! placement routes' writes ([`MetaStatusReader::release_root`]) and the
//! data group leaders' reports ([`MetaStatusReader::report_converged`]) to
//! the meta group.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use openraft::rt::WatchReceiver;
use ursula_control::ClusterView;
use ursula_control::ConfirmedView;
use ursula_control::ControlApplied;
use ursula_control::ControlCommand;
use ursula_control::ControlPlaneState;
use ursula_control::MetaReplicaStatus;
use ursula_control::MetaRole;
use ursula_control::PlacementStatus;
use ursula_control::PlacementView;
use ursula_control::RegistrationStatus;
use ursula_proto::placement::GenesisClaim;
use ursula_proto::placement::NodeId;
use ursula_proto::placement::RootId;
use ursula_shard::RaftGroupId;

use super::confirm::MetaBarrierError;
use super::genesis_driver::GenesisStatus;
use super::handle::MetaHandle;
use super::handle::MetaWriteError;
use super::runtime::MetaRuntimeState;
use super::runtime::MetaRuntimeStatus;
use crate::RaftGroupHandleRegistry;
use crate::RecoveryTransport;
use crate::rejoin::Convergence;

pub(crate) type Confirmation<'a> =
    Pin<Box<dyn Future<Output = Result<ConfirmedView, MetaBarrierError>> + Send + 'a>>;
type Submission<'a> =
    Pin<Box<dyn Future<Output = Result<ControlApplied, MetaWriteError>> + Send + 'a>>;

/// A meta read barrier on this node's view, and a write to the meta group,
/// whatever the transport.
trait MetaAccess: Send + Sync {
    fn confirm_view(&self) -> Confirmation<'_>;
    fn submit(&self, command: ControlCommand) -> Submission<'_>;
}

impl<T: RecoveryTransport> MetaAccess for MetaHandle<T> {
    fn confirm_view(&self) -> Confirmation<'_> {
        Box::pin(MetaHandle::confirm_view(self))
    }

    fn submit(&self, command: ControlCommand) -> Submission<'_> {
        Box::pin(MetaHandle::submit(self, command))
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
    meta: Arc<dyn MetaAccess>,
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
            meta: Arc::new(handle),
        }
    }

    /// Whether this node runs a meta replica now.
    pub fn state(&self) -> MetaRuntimeState {
        self.status.state.borrow().clone()
    }

    /// The node the status is of.
    pub(crate) fn node(&self) -> NodeId {
        self.node
    }

    pub(crate) fn registry(&self) -> &RaftGroupHandleRegistry {
        &self.registry
    }

    /// The meta group's answers to this node's registration.
    pub(crate) fn registration_answers(
        &self,
    ) -> crate::rt::sync::watch::Receiver<RegistrationStatus> {
        self.status.registration.clone()
    }

    /// This node's view, confirmed by a meta read barrier
    /// ([`MetaHandle::confirm_view`]).
    pub(crate) fn confirm_view(&self) -> Confirmation<'_> {
        self.meta.confirm_view()
    }

    /// This node's placement status, from its current view.
    pub fn placement_status(&self) -> PlacementStatus {
        self.status_of(&self.registry.placement_watch().current())
    }

    /// This node's placement status, from a view a meta read barrier
    /// confirmed current.
    pub async fn confirmed_placement_status(&self) -> Result<PlacementStatus, MetaBarrierError> {
        let confirmed = self.meta.confirm_view().await?;
        Ok(self.status_of(confirmed.view()))
    }

    /// What this node claims at genesis: its configured group count and
    /// effective replication factor among them.
    pub fn claim(&self) -> &GenesisClaim {
        &self.claim
    }

    /// How long this node has known no meta leader, `None` while its meta
    /// replica knows one.
    pub fn meta_leaderless_for(&self) -> Option<std::time::Duration> {
        self.status.leaderless.borrow().map(|since| since.elapsed())
    }

    /// Where this node's registration stands.
    pub fn registration(&self) -> RegistrationStatus {
        self.status.registration.borrow().clone()
    }

    /// Releases `root`, the root node `node` is bound to, through the meta
    /// leader (`ControlCommand::ReleaseRoot`).
    pub async fn release_root(
        &self,
        node: NodeId,
        root: RootId,
    ) -> Result<ControlApplied, MetaWriteError> {
        self.meta
            .submit(ControlCommand::ReleaseRoot { node, root })
            .await
    }

    /// Reports through the meta leader that `group`'s leader, proven,
    /// reached `convergence` (`ControlCommand::ReportConverged`). The
    /// command's time is this node's clock.
    pub async fn report_converged(
        &self,
        group: RaftGroupId,
        convergence: Convergence,
    ) -> Result<ControlApplied, MetaWriteError> {
        let Convergence {
            since,
            voters,
            membership,
        } = convergence;
        self.meta
            .submit(ControlCommand::ReportConverged {
                group,
                since,
                voters,
                membership,
                now_ms: ursula_runtime::unix_time_ms(),
            })
            .await
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
                registered: registered
                    .iter()
                    .map(|(node, registration)| (*node, registration.root))
                    .collect(),
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
            registered: BTreeMap::from([(samples::node(3), Registration {
                since_ms: 5,
                root: ursula_proto::placement::RootSlot::Bound(samples::root(3)),
            })]),
        });
        assert_eq!(
            cluster_view(&forming, &claim(), me, &genesis, true),
            ClusterView::Registering {
                registered: BTreeMap::from([(
                    samples::node(3),
                    ursula_proto::placement::RootSlot::Bound(samples::root(3))
                )]),
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

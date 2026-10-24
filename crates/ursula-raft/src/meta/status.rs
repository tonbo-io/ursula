//! This node's placement status (`GET /__ursula/placement`, design 11.1).
//!
//! [`MetaStatusReader`] reads the meta runtime's watches and the registry's
//! meta replica and placement view at once, and derives the view's
//! [`ClusterView`] stage ([`cluster_view`]). A confirmed status
//! ([`MetaStatusReader::confirmed_placement_status`]) derives it from a view
//! a meta read barrier confirmed current, and a group's status
//! ([`MetaStatusReader::group_status`]) joins its placement with this node's
//! replica. On the meta leader the status carries the leader's report,
//! which the decommission driver publishes ([`super::decommission`]). The
//! reader also carries the placement routes' writes
//! ([`MetaStatusReader::release_root`], [`MetaStatusReader::request_move`],
//! [`MetaStatusReader::set_lifecycle`], [`MetaStatusReader::set_move_limit`])
//! and the data group leaders' reports
//! ([`MetaStatusReader::report_converged`]) and this node's teardowns
//! (`MetaStatusReader::report_torn_down`) to the meta group, and the
//! barriers that ask a group's leader about its memberships
//! (`MetaStatusReader::barrier`).

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use openraft::rt::WatchReceiver;
use ursula_control::ClusterView;
use ursula_control::ConfirmedView;
use ursula_control::ControlApplied;
use ursula_control::ControlCommand;
use ursula_control::ControlError;
use ursula_control::ControlPlaneState;
use ursula_control::LifecycleChange;
use ursula_control::MetaReplicaStatus;
use ursula_control::MetaRole;
use ursula_control::MoveLimit;
use ursula_control::PlacementGroupStatus;
use ursula_control::PlacementStatus;
use ursula_control::PlacementView;
use ursula_control::RegistrationStatus;
use ursula_proto::placement::GenesisClaim;
use ursula_proto::placement::MetaLogId;
use ursula_proto::placement::NodeId;
use ursula_proto::placement::PlacementEpoch;
use ursula_proto::placement::RootId;
use ursula_proto::placement::VoterSet;
use ursula_shard::RaftGroupId;
use ursula_shard::RaftTarget;

use super::confirm::BarrierFailure;
use super::confirm::MetaBarrierError;
use super::genesis::GenesisProbe;
use super::genesis_driver::GenesisStatus;
use super::handle::MetaHandle;
use super::handle::MetaWriteError;
use super::runtime::MetaRuntimeState;
use super::runtime::MetaRuntimeStatus;
use crate::BarrierAnswer;
use crate::RaftGroupHandleRegistry;
use crate::RecoveryTransport;
use crate::rejoin::Convergence;

pub(crate) type Confirmation<'a> =
    Pin<Box<dyn Future<Output = Result<ConfirmedView, MetaBarrierError>> + Send + 'a>>;
type Submission<'a> =
    Pin<Box<dyn Future<Output = Result<ControlApplied, MetaWriteError>> + Send + 'a>>;
type Barrier<'a> = Pin<Box<dyn Future<Output = Result<BarrierAnswer, BarrierFailure>> + Send + 'a>>;
type Probe<'a> = Pin<Box<dyn Future<Output = Option<crate::rejoin::PeerGroupLog>> + Send + 'a>>;

/// Why [`MetaStatusReader::group_status`] answered with no group status.
#[derive(Debug, thiserror::Error)]
pub enum GroupStatusError {
    /// A meta read barrier confirmed no view.
    #[error(transparent)]
    Unconfirmed(#[from] MetaBarrierError),
    /// The view refuses the read as the meta state machine would refuse a
    /// command: no bootstrapped cluster, or no such group.
    #[error("this node's placement view refuses the read")]
    Refused(#[source] ControlError),
}

/// A meta read barrier on this node's view, a write to the meta group, a
/// barrier of any group and a genesis probe, whatever the transport.
trait MetaAccess: Send + Sync {
    fn confirm_view(&self) -> Confirmation<'_>;
    fn submit(&self, command: ControlCommand) -> Submission<'_>;
    fn barrier(&self, leader: NodeId, target: RaftTarget) -> Barrier<'_>;
    fn genesis_probe<'a>(&'a self, peer: NodeId, probe: &'a GenesisProbe) -> Probe<'a>;
}

impl<T: RecoveryTransport> MetaAccess for MetaHandle<T> {
    fn confirm_view(&self) -> Confirmation<'_> {
        Box::pin(MetaHandle::confirm_view(self))
    }

    fn submit(&self, command: ControlCommand) -> Submission<'_> {
        Box::pin(MetaHandle::submit(self, command))
    }

    fn barrier(&self, leader: NodeId, target: RaftTarget) -> Barrier<'_> {
        Box::pin(MetaHandle::barrier(self, leader, target))
    }

    fn genesis_probe<'a>(&'a self, peer: NodeId, probe: &'a GenesisProbe) -> Probe<'a> {
        Box::pin(MetaHandle::genesis_probe(self, peer, probe))
    }
}

/// What a placement status reads of this node's meta runtime. Cheap to
/// clone, so a reader takes it out of any lock before it awaits.
#[derive(Clone)]
pub struct MetaStatusReader {
    node: NodeId,
    /// The WAL root this run of the node runs on.
    root: RootId,
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
        root: RootId,
        claim: GenesisClaim,
        status: MetaRuntimeStatus,
        registry: RaftGroupHandleRegistry,
        handle: MetaHandle<T>,
    ) -> Self {
        Self {
            node,
            root,
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

    /// The WAL root this run of the node runs on: the view's record of the
    /// node is this run's only when its slot names it
    /// (`ursula_control::Cluster::own_record`).
    pub fn root(&self) -> RootId {
        self.root
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
    pub async fn confirm_view(&self) -> Result<ConfirmedView, MetaBarrierError> {
        self.meta.confirm_view().await
    }

    /// Asks `leader` for a fresh quorum barrier of `target`, answered with
    /// the memberships of the leader view it confirmed, over this node's one
    /// recovery transport. Its answer passed the check every seam runs
    /// ([`BarrierAnswer::accept`]).
    pub(crate) fn barrier(&self, leader: NodeId, target: RaftTarget) -> Barrier<'_> {
        self.meta.barrier(leader, target)
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

    /// Starts, retargets or cancels a move of `group` to `to` through the
    /// meta leader, while its placement is at `expected`
    /// (`ControlCommand::RequestMove`). The command's time is this node's
    /// clock.
    pub async fn request_move(
        &self,
        group: RaftGroupId,
        expected: PlacementEpoch,
        to: VoterSet,
    ) -> Result<ControlApplied, MetaWriteError> {
        self.meta
            .submit(ControlCommand::RequestMove {
                group,
                expected,
                to,
                now_ms: ursula_runtime::unix_time_ms(),
            })
            .await
    }

    /// Cordons, uncordons or decommissions `node` through the meta leader
    /// (`ControlCommand::SetNodeLifecycle`). The command's time is this
    /// node's clock.
    pub async fn set_lifecycle(
        &self,
        node: NodeId,
        change: LifecycleChange,
    ) -> Result<ControlApplied, MetaWriteError> {
        self.meta
            .submit(ControlCommand::SetNodeLifecycle {
                node,
                change,
                now_ms: ursula_runtime::unix_time_ms(),
            })
            .await
    }

    /// Sets the move limit through the meta leader
    /// (`ControlCommand::SetMoveLimit`).
    pub async fn set_move_limit(&self, limit: MoveLimit) -> Result<ControlApplied, MetaWriteError> {
        self.meta
            .submit(ControlCommand::SetMoveLimit { limit })
            .await
    }

    /// `group`'s placement in this node's view, confirmed by a meta read
    /// barrier first when `confirmed`, and this node's replica of it. A view
    /// that shows no bootstrapped cluster or no such group refuses as the
    /// meta state machine would.
    pub async fn group_status(
        &self,
        group: RaftGroupId,
        confirmed: bool,
    ) -> Result<PlacementGroupStatus, GroupStatusError> {
        let view = if confirmed {
            Arc::new(self.meta.confirm_view().await?.view().clone())
        } else {
            self.registry.placement_watch().current()
        };
        let cluster = match &*view.state {
            ControlPlaneState::Bootstrapped(cluster) => cluster,
            ControlPlaneState::Forming { .. } => {
                return Err(GroupStatusError::Refused(ControlError::NotBootstrapped));
            }
            ControlPlaneState::FormationCut => {
                return Err(GroupStatusError::Refused(ControlError::FormationCut));
            }
        };
        let placement = cluster
            .group(group)
            .ok_or(GroupStatusError::Refused(ControlError::UnknownGroup {
                group,
            }))?
            .clone();
        let replica = self
            .registry
            .metrics_snapshot()
            .iter()
            .find(|metrics| metrics.raft_group_id == group.0)
            .map(crate::RaftGroupMetricsSnapshot::to_admin);
        Ok(PlacementGroupStatus {
            node: self.node,
            placement,
            replica,
        })
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

    /// Reports through the meta leader that this node tore down its
    /// replica of `group`, which the retirement created at `created_at`
    /// named (`ControlCommand::ReportTornDown`).
    pub(crate) async fn report_torn_down(
        &self,
        group: RaftGroupId,
        created_at: MetaLogId,
    ) -> Result<ControlApplied, MetaWriteError> {
        self.meta
            .submit(ControlCommand::ReportTornDown {
                group,
                node: self.node,
                created_at,
            })
            .await
    }

    /// Whether a process of the root `node` is bound to in this node's view
    /// answers at `node`'s `raft.peers` address: it answered this node's
    /// genesis probe, the one read-only meta-plane request, and its answer
    /// passed the responder check, which counts only an answer stamped by a
    /// process of the root the view binds the id to
    /// (`crate::PeerCluster::accept_responder`). A refusal, no answer, or an
    /// answer of another root is no process of it.
    pub(crate) async fn bound_process_answers(&self, node: NodeId) -> bool {
        let probe = GenesisProbe {
            claim: self.claim.clone(),
            vote: crate::rejoin::bootstrap_probe_vote(),
        };
        self.meta.genesis_probe(node, &probe).await.is_some()
    }

    /// Reports through the meta leader that the removal of `node`'s life on
    /// `root` completed (`ControlCommand::ReportRemovalComplete`).
    pub(crate) async fn report_removal_complete(
        &self,
        node: NodeId,
        root: RootId,
    ) -> Result<ControlApplied, MetaWriteError> {
        self.meta
            .submit(ControlCommand::ReportRemovalComplete {
                node,
                root,
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
                let owner = active.core.owner();
                let watched = owner.metrics();
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
                let leader =
                    crate::leadership::named_leader(&metrics, owner.contact()).map(NodeId::new);
                let formed = leader.is_some() || metrics.current_term > 0;
                (MetaReplicaStatus::Running { role, leader }, formed)
            }
        };
        // Only while the replica still names this node the leader: a
        // deposed leader's last report is not the meta leader's.
        let leader_report = match &meta {
            MetaReplicaStatus::Running {
                leader: Some(leader),
                ..
            } if *leader == self.node => self.status.leader_report.borrow().clone(),
            MetaReplicaStatus::Running { .. }
            | MetaReplicaStatus::Damaged { .. }
            | MetaReplicaStatus::Absent => None,
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
            leader_report,
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

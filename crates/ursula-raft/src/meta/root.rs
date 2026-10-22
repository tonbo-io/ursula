//! This run's root as the meta group sees it, and the checks that end the
//! process's part for good when meta binds its node id elsewhere.
//!
//! Each node id's slot in meta names the root (the WAL disk) it is bound to
//! (`ursula_proto::placement::RootSlot`). A run stands on its root in one
//! of two ways ([`RootStanding`]), which the root's identity file records:
//! a root stands `Unbound` from its creation until the meta group first
//! answers a registration of it, and `Bound` from then on, across restarts.
//! The guard records the binding, `fsync`ed, before the answer is published
//! ([`RootGuard::bind`]). An unbound root never fences itself: a new disk
//! under a live id registers until an operator releases the old root, and a
//! restart while it is refused changes nothing, whatever its meta replica
//! wrote. Its data replicas are held out from its open until the binding
//! and then through the predecessor wait (`NodeFence::awaits_binding`), and
//! a refusal holds its meta replica's votes too ([`RootGuard::hold_out`]).
//! A bound root fences itself for good ([`RootGuard::supersede`]) on a
//! registration refused for another root, on any view that shows its root
//! released, and on a view a meta read barrier confirmed that binds its id
//! to another root. An unconfirmed view may show the slot of a replica
//! still applying history, before this root's binding, so only a release
//! counts there: a released root never binds again.

use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

use ursula_control::ControlPlaneState;
use ursula_control::PlacementView;
use ursula_proto::placement::NodeId;
use ursula_proto::placement::RootId;
use ursula_proto::placement::RootSlot;
use ursula_runtime::NodeFence;

use super::watch::PlacementWatch;
use crate::RaftGroupHandleRegistry;
use crate::RaftWalError;
use crate::log_store::BindingRecord;
use crate::log_store::RootStanding;
use crate::peer_cluster::Supersession;

/// What a view's slot `slot` of this node says to a run standing on
/// `root` (see the module docs). `confirmed` says that a meta read barrier
/// confirmed the view current.
pub(crate) fn judge_slot(
    standing: RootStanding,
    root: RootId,
    slot: Option<RootSlot>,
    confirmed: bool,
) -> Option<Supersession> {
    match (standing, slot) {
        (RootStanding::Unbound, _) | (RootStanding::Bound, None) => None,
        (RootStanding::Bound, Some(RootSlot::Released(released))) if released == root => {
            Some(Supersession::Released)
        }
        (RootStanding::Bound, Some(RootSlot::Bound(bound))) if confirmed && bound != root => {
            Some(Supersession::BoundElsewhere { root: bound })
        }
        (RootStanding::Bound, Some(RootSlot::Bound(_) | RootSlot::Released(_))) => None,
    }
}

/// Node `node`'s slot in `state`, `None` before it registered.
pub(crate) fn slot_of(state: &ControlPlaneState, node: NodeId) -> Option<RootSlot> {
    match state {
        ControlPlaneState::Forming { registered } => {
            registered.get(&node).map(|registration| registration.root)
        }
        ControlPlaneState::Bootstrapped(cluster) => cluster.node(node).map(|record| record.root),
        ControlPlaneState::FormationCut => None,
    }
}

#[derive(Debug)]
struct GuardState {
    node: NodeId,
    root: RootId,
    /// Whether the identity file records the root bound.
    bound: AtomicBool,
    record: BindingRecord,
    fence: NodeFence,
    registry: RaftGroupHandleRegistry,
}

/// This run's root and its standing, shared by the registration, the view
/// watch and the meta read barrier. Cloning shares it.
#[derive(Debug, Clone)]
pub(crate) struct RootGuard(Arc<GuardState>);

impl RootGuard {
    /// The guard of node `node` running on `root`, standing as `record`
    /// says the identity file recorded at the root's open.
    pub(crate) fn new(
        node: NodeId,
        root: RootId,
        record: BindingRecord,
        fence: NodeFence,
        registry: RaftGroupHandleRegistry,
    ) -> Self {
        Self(Arc::new(GuardState {
            node,
            root,
            bound: AtomicBool::new(record.standing_at_open() == RootStanding::Bound),
            record,
            fence,
            registry,
        }))
    }

    pub(crate) fn standing(&self) -> RootStanding {
        if self.0.bound.load(Ordering::Acquire) {
            RootStanding::Bound
        } else {
            RootStanding::Unbound
        }
    }

    /// The meta group answered a registration of this root: records it
    /// bound in the identity file, `fsync`ed, the first time, and only then
    /// stands bound. The caller publishes the answer, which releases the
    /// binding's hold on the data replicas, only after this returned `Ok`.
    pub(crate) async fn bind(&self) -> Result<(), RaftWalError> {
        if self.standing() == RootStanding::Unbound {
            self.0.record.record_bound().await?;
            self.0.bound.store(true, Ordering::Release);
        }
        Ok(())
    }

    /// Holds an unbound root out while meta binds its id to another root.
    /// Its data replicas are held from its open until its binding
    /// (`NodeFence::awaits_binding`). The refusal also makes it owe the
    /// predecessor wait, so its meta replica grants no vote and takes no
    /// transfer from now on, and the node sheds every leadership. Its meta
    /// barrier writes no floor while the wait is owed, but a meta floor
    /// written before the refusal stays. The wait runs once meta binds the
    /// root.
    pub(crate) fn hold_out(&self) {
        self.0.fence.begin_wait();
        crate::predecessor_wait::sync_fence_shed(&self.0.registry, &self.0.fence);
    }

    /// Checks node's slot in `view` (see the module docs).
    pub(crate) fn check_view(&self, view: &PlacementView, confirmed: bool) {
        let slot = slot_of(&view.state, self.0.node);
        if let Some(why) = judge_slot(self.standing(), self.0.root, slot, confirmed) {
            self.supersede(why);
        }
    }

    /// Fences this process for good: another process may answer as this
    /// node.
    pub(crate) fn supersede(&self, why: Supersession) {
        if crate::peer_cluster::supersede(&self.0.fence, self.0.node.get(), why) {
            crate::predecessor_wait::sync_fence_shed(&self.0.registry, &self.0.fence);
        }
    }
}

/// Checks every view this node applies against `guard` until the process
/// is superseded or the views end.
pub(crate) async fn watch_views(guard: RootGuard, mut watch: PlacementWatch) {
    loop {
        guard.check_view(&watch.current(), false);
        if guard.0.fence.is_closed() || !watch.changed().await {
            return;
        }
    }
}

#[cfg(test)]
mod tests {
    use ursula_control::samples;
    use ursula_proto::placement::RootSlot;

    use super::RootStanding;
    use super::judge_slot;
    use crate::peer_cluster::Supersession;

    /// Each slot a view can show, to each standing: an unbound root never
    /// fences itself, a bound one on its release in any view and on
    /// another root's binding only in a confirmed view.
    #[test]
    fn a_view_supersedes_only_a_bound_root_and_another_binding_only_when_confirmed() {
        let (mine, other) = (samples::root(5), samples::root(6));
        let released = Some(Supersession::Released);
        let elsewhere = Some(Supersession::BoundElsewhere { root: other });
        // (slot, bound unconfirmed, bound confirmed)
        let table = [
            (None, None, None),
            (Some(RootSlot::Bound(mine)), None, None),
            (Some(RootSlot::Bound(other)), None, elsewhere),
            (Some(RootSlot::Released(mine)), released, released),
            (Some(RootSlot::Released(other)), None, None),
        ];
        for (slot, unconfirmed, confirmed) in table {
            for (is_confirmed, expected) in [(false, unconfirmed), (true, confirmed)] {
                assert_eq!(
                    judge_slot(RootStanding::Unbound, mine, slot, is_confirmed),
                    None,
                    "{slot:?} {is_confirmed}"
                );
                assert_eq!(
                    judge_slot(RootStanding::Bound, mine, slot, is_confirmed),
                    expected,
                    "{slot:?} {is_confirmed}"
                );
            }
        }
    }
}

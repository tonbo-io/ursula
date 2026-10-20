//! Placement views: what a node's meta replica applied, and the proof that
//! a view is current.
//!
//! Every meta replica publishes a [`PlacementView`] per applied batch,
//! install and startup restore. No reader takes one as current without a
//! confirmation: a [`ConfirmedView`] pairs a view with a [`ConfirmedStamp`]
//! from a meta read barrier taken after its inputs were read, and only such
//! a view creates a replica, starts a teardown or adds a voter. Views are not
//! monotonic across restarts, so an unconfirmed one only delays work.

use std::sync::Arc;

use ursula_proto::placement::MetaLogId;
use ursula_proto::placement::ViewStage;

use super::state::Cluster;
use super::state::ControlPlaneState;

/// A meta replica's applied state and how far it reaches.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlacementView {
    pub stage: ViewStage,
    pub state: Arc<ControlPlaneState>,
}

impl PlacementView {
    /// The view of a replica that applied no history event.
    pub fn empty() -> Self {
        Self {
            stage: ViewStage::Empty,
            state: Arc::new(ControlPlaneState::default()),
        }
    }

    /// The bootstrapped cluster the view shows, if any.
    pub fn cluster(&self) -> Option<&Cluster> {
        self.state.cluster()
    }

    /// The last meta entry the view includes, `None` for an empty view.
    pub fn applied(&self) -> Option<MetaLogId> {
        match self.stage {
            ViewStage::Empty => None,
            ViewStage::Applied { applied, .. } => Some(applied),
        }
    }
}

/// What a meta read barrier proved: the meta history at the barrier, and
/// the entry the meta log had reached, which a confirmed view includes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ConfirmedStamp {
    pub history: MetaLogId,
    pub applied: MetaLogId,
}

/// A view a meta read barrier confirmed current.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfirmedView {
    view: Arc<PlacementView>,
    stamp: ConfirmedStamp,
}

/// Why a view does not cover a confirmation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum UnconfirmedView {
    #[error("the view applied no meta history event")]
    Empty,
    #[error("the view is in meta history {view}, the barrier in {stamp}")]
    OtherHistory { view: MetaLogId, stamp: MetaLogId },
    #[error("the view applied up to {applied}, below the barrier's {required}")]
    Behind {
        applied: MetaLogId,
        required: MetaLogId,
    },
}

impl ConfirmedView {
    /// Confirms `view` by `stamp`, when the view is in the stamp's meta
    /// history and applied at least the stamp's entry, compared term first.
    pub fn new(view: Arc<PlacementView>, stamp: ConfirmedStamp) -> Result<Self, UnconfirmedView> {
        let stage = view.stage;
        match stage {
            ViewStage::Empty => Err(UnconfirmedView::Empty),
            ViewStage::Applied { history, .. } if history != stamp.history => {
                Err(UnconfirmedView::OtherHistory {
                    view: history,
                    stamp: stamp.history,
                })
            }
            ViewStage::Applied { applied, .. } if applied < stamp.applied => {
                Err(UnconfirmedView::Behind {
                    applied,
                    required: stamp.applied,
                })
            }
            ViewStage::Applied { .. } => Ok(Self { view, stamp }),
        }
    }

    pub fn view(&self) -> &PlacementView {
        &self.view
    }

    pub fn stamp(&self) -> ConfirmedStamp {
        self.stamp
    }

    /// Whether a later `view` still carries this confirmation: the same meta
    /// history, and an applied entry not below the stamp's.
    pub fn still_covers(&self, view: &PlacementView) -> bool {
        match view.stage {
            ViewStage::Empty => false,
            ViewStage::Applied { applied, history } => {
                history == self.stamp.history && applied >= self.stamp.applied
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use ursula_proto::placement::MetaLogId;
    use ursula_proto::placement::NodeId;
    use ursula_proto::placement::RaftLogId;
    use ursula_proto::placement::ViewStage;

    use super::ConfirmedStamp;
    use super::ConfirmedView;
    use super::PlacementView;
    use super::UnconfirmedView;
    use crate::placement::state::ControlPlaneState;

    fn id(term: u64, index: u64) -> MetaLogId {
        MetaLogId::new(RaftLogId {
            term,
            leader: NodeId::new(1),
            index,
        })
    }

    fn view(applied: MetaLogId, history: MetaLogId) -> Arc<PlacementView> {
        Arc::new(PlacementView {
            stage: ViewStage::Applied { applied, history },
            state: Arc::new(ControlPlaneState::default()),
        })
    }

    #[test]
    fn a_view_confirms_only_within_the_barriers_history_and_at_its_entry() {
        let stamp = ConfirmedStamp {
            history: id(1, 1),
            applied: id(2, 10),
        };
        assert!(matches!(
            ConfirmedView::new(Arc::new(PlacementView::empty()), stamp),
            Err(UnconfirmedView::Empty)
        ));
        assert!(matches!(
            ConfirmedView::new(view(id(2, 10), id(2, 5)), stamp),
            Err(UnconfirmedView::OtherHistory { .. })
        ));
        // A learner that missed a meta plan can hold a higher index of an
        // older term: it does not confirm.
        assert!(matches!(
            ConfirmedView::new(view(id(1, 50), id(1, 1)), stamp),
            Err(UnconfirmedView::Behind { .. })
        ));
        let confirmed = ConfirmedView::new(view(id(2, 11), id(1, 1)), stamp).unwrap();
        assert!(confirmed.still_covers(&view(id(2, 12), id(1, 1))));
        assert!(!confirmed.still_covers(&view(id(3, 1), id(3, 1))));
        assert!(!confirmed.still_covers(&PlacementView::empty()));
    }
}

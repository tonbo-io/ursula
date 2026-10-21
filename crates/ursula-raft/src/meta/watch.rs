//! Placement views as this node's meta replica publishes them.
//!
//! The registry owns one [`PlacementPublisher`] for the life of the node,
//! so a reader's [`PlacementWatch`] outlives every restart of the meta
//! replica. The replica publishes a view per applied batch, per snapshot
//! install and per startup restore. A view is never current by itself: a
//! reader takes it as current only with a confirmation
//! (`ursula_control::ConfirmedView`). The channel is the runtime shim's
//! `watch`, so a simulation wakes its receivers in a replayable order.

use std::sync::Arc;

use ursula_control::GroupPlacement;
use ursula_control::PlacementView;
use ursula_shard::RaftGroupId;

use crate::rt::sync::watch;

/// The sending side of this node's placement views, and the peer cluster
/// whose process fence takes the roots each view binds
/// ([`crate::PeerCluster::observe_view`]).
#[derive(Debug, Clone)]
pub struct PlacementPublisher {
    sender: Arc<watch::Sender<Arc<PlacementView>>>,
    peers: Option<crate::PeerCluster>,
}

impl Default for PlacementPublisher {
    /// A publisher holding the empty view, as a node holds before its meta
    /// replica restored or applied anything.
    fn default() -> Self {
        let (sender, _receiver) = watch::channel(Arc::new(PlacementView::empty()));
        Self {
            sender: Arc::new(sender),
            peers: None,
        }
    }
}

impl PlacementPublisher {
    /// A publisher whose views also reach `peers`' process fence.
    pub(crate) fn for_peers(peers: crate::PeerCluster) -> Self {
        Self {
            peers: Some(peers),
            ..Self::default()
        }
    }

    /// Publishes `view` as the meta replica's latest.
    pub(crate) fn publish(&self, view: PlacementView) {
        if let Some(peers) = &self.peers {
            peers.observe_view(&view);
        }
        self.sender.send_replace(Arc::new(view));
    }

    /// A watch of the views published from now on, starting at the latest.
    pub fn subscribe(&self) -> PlacementWatch {
        PlacementWatch(self.sender.subscribe())
    }
}

/// A reader of this node's placement views.
#[derive(Debug, Clone)]
pub struct PlacementWatch(watch::Receiver<Arc<PlacementView>>);

impl PlacementWatch {
    /// The latest view published.
    pub fn current(&self) -> Arc<PlacementView> {
        Arc::clone(&self.0.borrow())
    }

    /// Waits until a view is published after the one this watch last saw.
    /// `false` once the publisher is gone.
    pub async fn changed(&mut self) -> bool {
        self.0.changed().await.is_ok()
    }
}

/// A reader of one data group's placement in this node's views.
#[derive(Debug, Clone)]
pub struct GroupPlacementWatch {
    watch: PlacementWatch,
    group: RaftGroupId,
}

impl GroupPlacementWatch {
    pub fn new(watch: PlacementWatch, group: RaftGroupId) -> Self {
        Self { watch, group }
    }

    pub fn group(&self) -> RaftGroupId {
        self.group
    }

    /// The latest view published, which a reader takes as current only
    /// with a confirmation.
    pub fn view(&self) -> Arc<PlacementView> {
        self.watch.current()
    }

    /// The group's placement in the latest view, `None` before Bootstrap.
    pub fn current(&self) -> Option<GroupPlacement> {
        self.watch
            .current()
            .cluster()
            .and_then(|cluster| cluster.group(self.group))
            .cloned()
    }

    /// Waits until a view is published after the one this watch last saw.
    /// `false` once the publisher is gone.
    pub async fn changed(&mut self) -> bool {
        self.watch.changed().await
    }
}

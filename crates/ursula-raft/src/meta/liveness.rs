//! Which nodes the meta leader takes as live move targets (design 8.2).
//!
//! A node is `Eligible` when it acknowledged the meta leader within
//! [`LIVE_WITHIN`], by the leader's own heartbeat acknowledgements
//! (OpenRaft's `RaftMetrics::heartbeat`, every meta voter and learner), and
//! does not shed leadership for WAL disk pressure. A node that acknowledged
//! in time and sheds for its WAL disk is `DiskPressure`. Every other node is
//! absent from the map: not live. The leader counts itself acknowledged.
//!
//! The WAL-disk sheds of the peers come from one injected source
//! ([`WalDiskSheds`]): the participation answers on a server, the peer
//! registries in the simulation. The leader's own shed comes from its
//! registry. A peer whose shed the source does not know is judged by its
//! acknowledgements alone.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use ursula_control::NodeEligibility;
use ursula_proto::placement::NodeId;

use crate::LeadershipShedState;
use crate::RaftGroupHandleRegistry;
use crate::rt::time::Instant;

/// How recently a node acknowledged the meta leader to count as live.
pub const LIVE_WITHIN: Duration = Duration::from_secs(10);

/// How long the peers' sheds are reused before the source is asked again.
const SHEDS_REFRESH: Duration = Duration::from_secs(5);

/// The answer of [`WalDiskSheds::shedding`].
pub type Shedding<'a> = Pin<Box<dyn Future<Output = BTreeSet<NodeId>> + Send + 'a>>;

/// Where the meta leader learns which peers shed leadership for WAL disk
/// pressure.
pub trait WalDiskSheds: Send + Sync + std::fmt::Debug {
    /// The peers known to shed leadership for WAL disk pressure now. A peer
    /// whose state is unknown is left out.
    fn shedding(&self) -> Shedding<'_>;
}

/// The meta leader's liveness of every node, read on each decommission
/// pass.
#[derive(Debug)]
pub(crate) struct Liveness {
    me: NodeId,
    registry: RaftGroupHandleRegistry,
    sheds: Arc<dyn WalDiskSheds>,
    /// The peers' sheds and when the source answered them.
    cached: Option<(Instant, BTreeSet<NodeId>)>,
}

impl Liveness {
    pub(crate) fn new(
        me: NodeId,
        registry: RaftGroupHandleRegistry,
        sheds: Arc<dyn WalDiskSheds>,
    ) -> Self {
        Self {
            me,
            registry,
            sheds,
            cached: None,
        }
    }

    /// Every live node's eligibility, from `acked`, the age of each node's
    /// last acknowledgement of this leader (see the module docs).
    pub(crate) async fn eligibility(
        &mut self,
        acked: &BTreeMap<NodeId, Duration>,
    ) -> BTreeMap<NodeId, NodeEligibility> {
        let mut shedding = match &self.cached {
            Some((at, sheds)) if at.elapsed() < SHEDS_REFRESH => sheds.clone(),
            Some(_) | None => {
                let sheds = self.sheds.shedding().await;
                self.cached = Some((Instant::now(), sheds.clone()));
                sheds
            }
        };
        let own = self.registry.leadership_shed_state();
        if own.contains(LeadershipShedState::WAL_DISK_PRESSURE) {
            shedding.insert(self.me);
        } else {
            shedding.remove(&self.me);
        }
        eligibility(self.me, acked, &shedding)
    }
}

/// The eligibility of each node `acked` lists as acknowledged within
/// [`LIVE_WITHIN`], and of the leader `me`, by whether `shedding` names it.
fn eligibility(
    me: NodeId,
    acked: &BTreeMap<NodeId, Duration>,
    shedding: &BTreeSet<NodeId>,
) -> BTreeMap<NodeId, NodeEligibility> {
    acked
        .iter()
        .filter(|(_, age)| **age <= LIVE_WITHIN)
        .map(|(node, _)| *node)
        .chain(std::iter::once(me))
        .map(|node| {
            let eligibility = if shedding.contains(&node) {
                NodeEligibility::DiskPressure
            } else {
                NodeEligibility::Eligible
            };
            (node, eligibility)
        })
        .collect()
}

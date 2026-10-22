//! Which nodes the meta leader takes as live move targets (design 8.2).
//!
//! A node is `Eligible` when it acknowledged the meta leader within
//! [`LIVE_WITHIN`], by the leader's own heartbeat acknowledgements
//! (OpenRaft's `RaftMetrics::heartbeat`, every meta voter and learner), and
//! is known not to shed leadership for WAL disk pressure. A node that
//! acknowledged in time and sheds for its WAL disk is `DiskPressure`. Every
//! other node is absent from the map, and so is a node whose WAL-disk state
//! is unknown: unknown is not live. The leader counts itself acknowledged.
//!
//! The WAL-disk states of the peers come from one injected source
//! ([`WalDiskSheds`]): the participation answers on a server, the peer
//! registries in the simulation. The leader's own state comes from its
//! registry.

use std::collections::BTreeMap;
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

/// The answer of [`WalDiskSheds::shedding`]: each peer whose state is
/// known, `true` when it sheds leadership for WAL disk pressure.
pub type Shedding<'a> = Pin<Box<dyn Future<Output = BTreeMap<NodeId, bool>> + Send + 'a>>;

/// Where the meta leader learns whether each peer sheds leadership for WAL
/// disk pressure.
pub trait WalDiskSheds: Send + Sync + std::fmt::Debug {
    /// Each peer whose WAL-disk state is known now, `true` when it sheds
    /// leadership for WAL disk pressure. A peer whose state is unknown (no
    /// answer in time, or an answer for another cluster) is left out.
    fn shedding(&self) -> Shedding<'_>;
}

/// The meta leader's liveness of every node, read on each decommission
/// pass.
#[derive(Debug)]
pub(crate) struct Liveness {
    me: NodeId,
    registry: RaftGroupHandleRegistry,
    sheds: Arc<dyn WalDiskSheds>,
    /// The peers' known WAL-disk states and when the source answered them.
    cached: Option<(Instant, BTreeMap<NodeId, bool>)>,
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
        shedding.insert(
            self.me,
            own.contains(LeadershipShedState::WAL_DISK_PRESSURE),
        );
        eligibility(self.me, acked, &shedding)
    }
}

/// The eligibility of each node `acked` lists as acknowledged within
/// [`LIVE_WITHIN`], and of the leader `me`, by its WAL-disk state in
/// `shedding`. A node with no known state is left out.
fn eligibility(
    me: NodeId,
    acked: &BTreeMap<NodeId, Duration>,
    shedding: &BTreeMap<NodeId, bool>,
) -> BTreeMap<NodeId, NodeEligibility> {
    acked
        .iter()
        .filter(|(_, age)| **age <= LIVE_WITHIN)
        .map(|(node, _)| *node)
        .chain(std::iter::once(me))
        .filter_map(|node| {
            let eligibility = if *shedding.get(&node)? {
                NodeEligibility::DiskPressure
            } else {
                NodeEligibility::Eligible
            };
            Some((node, eligibility))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::time::Duration;

    use ursula_control::NodeEligibility;
    use ursula_proto::placement::NodeId;

    use super::LIVE_WITHIN;
    use super::eligibility;

    /// Bugs caught: a node whose WAL-disk state is unknown (its
    /// participation answer did not arrive) counting as `Eligible` on its
    /// acknowledgements alone, and a stale acknowledgement counting as live.
    #[test]
    fn a_node_is_listed_only_when_it_acknowledged_in_time_and_its_wal_disk_state_is_known() {
        let fresh = Duration::from_secs(1);
        let stale = LIVE_WITHIN + Duration::from_millis(1);
        let acked = BTreeMap::from([
            (NodeId::new(2), fresh),
            (NodeId::new(3), fresh),
            (NodeId::new(4), fresh),
            (NodeId::new(5), stale),
        ]);
        let shedding = BTreeMap::from([
            (NodeId::new(1), false),
            (NodeId::new(2), false),
            (NodeId::new(3), true),
            (NodeId::new(5), false),
        ]);

        assert_eq!(
            eligibility(NodeId::new(1), &acked, &shedding),
            BTreeMap::from([
                (NodeId::new(1), NodeEligibility::Eligible),
                (NodeId::new(2), NodeEligibility::Eligible),
                (NodeId::new(3), NodeEligibility::DiskPressure),
            ])
        );
    }
}

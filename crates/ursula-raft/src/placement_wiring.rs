//! The placement inputs of every data replica's open (design 4.6).
//!
//! Every engine input that placement decides reaches the engine factory and
//! the simulation through one trait, [`PlacementInputs`]:
//!
//! - **admission**: whether the node hosts a group, and how its store may
//!   open ([`Admission`]). The factory's `hosts_group` answers it, and the
//!   store open takes it as its intent.
//! - **seed role** ([`SeedRole`]): whether the replica initializes its group
//!   or waits for the initializer. The bootstrap driver confirms it right
//!   before it opens its gate for a fresh group, the vote floor task right
//!   before it writes the floor.
//! - **node sets** ([`GroupNodes`]): whom the gate drivers ask, whom an
//!   initializer initializes and which voters the heal driver restores,
//!   each read at its use, so a placement that changes reaches a running
//!   replica.
//!
//! [`MetaPlacement`] answers from meta: admission from the hosting driver's
//! [`HostingSet`], the seed role and the node sets from this node's
//! placement views. [`ConfigSeeds`] is production's interim
//! implementation, the seed rule over `raft.peers`. [`FixedPlacement`]
//! gives tests and the simulation one fixed voter set. The meta group keeps
//! its genesis driver and passes its claim through [`FixedNodes`] and
//! [`FixedRole`].

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::sync::Arc;

use futures_util::future::BoxFuture;
use ursula_control::Admission;
use ursula_control::SeedRoleOf;
use ursula_shard::RaftGroupId;

use crate::hosting::HostingSet;
use crate::meta::confirm::MetaBarrierError;
use crate::meta::status::MetaStatusReader;
use crate::meta::watch::GroupPlacementWatch;

/// Every placement input of a data replica's open, by group.
pub trait PlacementInputs: Send + Sync + 'static {
    /// How the node may open `group`, `None` when it does not host it.
    fn admission(&self, group: RaftGroupId) -> Option<Admission>;
    /// The node's seed role in `group`.
    fn seed_role(&self, group: RaftGroupId) -> Arc<dyn SeedRole>;
    /// The node sets of `group` the replica's drivers read.
    fn nodes(&self, group: RaftGroupId) -> Arc<dyn GroupNodes>;
}

/// A replica's seed role in its group.
pub trait SeedRole: Send + Sync + 'static {
    /// The role the local placement shows, unconfirmed: a replica runs the
    /// bootstrap driver only while it shows `Initializer`.
    fn local_role(&self) -> Option<SeedRoleOf>;
    /// The role from a confirmed placement. `Ok(None)` stops the driver
    /// that asks, and `Err` makes it retry.
    fn confirmed_role(&self) -> BoxFuture<'_, Result<Option<SeedRoleOf>, MetaBarrierError>>;
}

/// The node sets of one group that its replica's recovery drivers read at
/// each use.
pub trait GroupNodes: Send + Sync + 'static {
    /// The nodes the barrier driver asks for a leader's barrier, and the
    /// vote floor task and the bootstrap driver probe.
    fn candidates(&self) -> BTreeSet<u64>;
    /// The voters an initializer initializes, which the heal driver also
    /// restores.
    fn voters(&self) -> BTreeSet<u64>;
}

/// A role that needs no confirmation: configuration, or a test, decided it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FixedRole(pub Option<SeedRoleOf>);

impl SeedRole for FixedRole {
    fn local_role(&self) -> Option<SeedRoleOf> {
        self.0
    }

    fn confirmed_role(&self) -> BoxFuture<'_, Result<Option<SeedRoleOf>, MetaBarrierError>> {
        Box::pin(std::future::ready(Ok(self.0)))
    }
}

/// Node sets that never change.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FixedNodes {
    pub candidates: BTreeSet<u64>,
    pub voters: BTreeSet<u64>,
}

impl FixedNodes {
    /// `voters` as both the candidates and the voters, as a data group has
    /// them.
    pub fn from_voters(voters: BTreeSet<u64>) -> Self {
        Self {
            candidates: voters.clone(),
            voters,
        }
    }
}

impl GroupNodes for FixedNodes {
    fn candidates(&self) -> BTreeSet<u64> {
        self.candidates.clone()
    }

    fn voters(&self) -> BTreeSet<u64> {
        self.voters.clone()
    }
}

/// Production's placement inputs from meta (design 4.6):
///
/// - admission from the [`HostingSet`], which only the hosting driver
///   writes ([`crate::HostingDriver`]);
/// - the seed role from this node's view, confirmed by a meta read barrier
///   ([`crate::meta::handle::MetaHandle::confirm_view`]) for the role a
///   driver acts on ([`ursula_control::seed_role`]);
/// - the node sets from the group's placement in the latest view
///   ([`GroupPlacementWatch`]): every voter of its current and target
///   voters as candidates, and the target voters as the voters an
///   initializer initializes and the heal driver restores. A group without
///   a placement has neither, so its replica asks only the members of its
///   own membership and heals nothing.
#[derive(Debug, Clone)]
pub struct MetaPlacement {
    meta: MetaStatusReader,
    hosting: HostingSet,
}

impl MetaPlacement {
    /// The inputs of the node `meta` reports on, admitting what `hosting`
    /// holds.
    pub fn new(meta: MetaStatusReader, hosting: HostingSet) -> Self {
        Self { meta, hosting }
    }
}

impl PlacementInputs for MetaPlacement {
    fn admission(&self, group: RaftGroupId) -> Option<Admission> {
        self.hosting.admission(group)
    }

    fn seed_role(&self, group: RaftGroupId) -> Arc<dyn SeedRole> {
        Arc::new(MetaSeedRole {
            meta: self.meta.clone(),
            group,
        })
    }

    fn nodes(&self, group: RaftGroupId) -> Arc<dyn GroupNodes> {
        Arc::new(PlacedNodes(GroupPlacementWatch::new(
            self.meta.registry().placement_watch(),
            group,
        )))
    }
}

/// A seed role read from this node's placement views.
struct MetaSeedRole {
    meta: MetaStatusReader,
    group: RaftGroupId,
}

impl SeedRole for MetaSeedRole {
    fn local_role(&self) -> Option<SeedRoleOf> {
        ursula_control::shown_seed_role(
            &self.meta.registry().placement_watch().current(),
            self.meta.node(),
            self.group,
        )
    }

    fn confirmed_role(&self) -> BoxFuture<'_, Result<Option<SeedRoleOf>, MetaBarrierError>> {
        Box::pin(async move {
            let confirmed = self.meta.confirm_view().await?;
            Ok(ursula_control::seed_role(
                &confirmed,
                self.meta.node(),
                self.group,
            ))
        })
    }
}

/// One group's node sets from its placement in the latest view.
struct PlacedNodes(GroupPlacementWatch);

impl GroupNodes for PlacedNodes {
    fn candidates(&self) -> BTreeSet<u64> {
        self.0
            .current()
            .map(|placement| {
                placement
                    .current_voters()
                    .iter()
                    .chain(placement.target_voters().iter())
                    .map(ursula_proto::placement::NodeId::get)
                    .collect()
            })
            .unwrap_or_default()
    }

    fn voters(&self) -> BTreeSet<u64> {
        self.0
            .current()
            .map(|placement| {
                placement
                    .target_voters()
                    .iter()
                    .map(ursula_proto::placement::NodeId::get)
                    .collect()
            })
            .unwrap_or_default()
    }
}

/// One voter set for every group, for tests and the simulation: `node`
/// hosts every group and may create it, `initializer` initializes it and
/// every other node waits for it, without asking anyone.
#[cfg(any(test, madsim, feature = "test-support"))]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FixedPlacement {
    /// The node the inputs are for.
    pub node: u64,
    pub voters: BTreeSet<u64>,
    pub initializer: u64,
}

#[cfg(any(test, madsim, feature = "test-support"))]
impl FixedPlacement {
    /// A single node, the only voter and the initializer of every group.
    pub fn single(node: u64) -> Self {
        Self {
            node,
            voters: BTreeSet::from([node]),
            initializer: node,
        }
    }
}

#[cfg(any(test, madsim, feature = "test-support"))]
impl PlacementInputs for FixedPlacement {
    fn admission(&self, _group: RaftGroupId) -> Option<Admission> {
        Some(Admission::Create)
    }

    fn seed_role(&self, _group: RaftGroupId) -> Arc<dyn SeedRole> {
        Arc::new(FixedRole(Some(if self.node == self.initializer {
            SeedRoleOf::Initializer
        } else {
            SeedRoleOf::Voter
        })))
    }

    fn nodes(&self, _group: RaftGroupId) -> Arc<dyn GroupNodes> {
        Arc::new(FixedNodes::from_voters(self.voters.clone()))
    }
}

/// The voters and the initializer of every data group of a static cluster:
/// the kernel's seed rule ([`ursula_control::seed_voters`] and
/// [`ursula_control::initializer`]) over the `raft.peers` ids, at the
/// configured replication factor, at most the node count.
///
/// Interim: removed in S2(h), where meta's seeds decide what a node hosts.
/// Until then [`ConfigSeeds::for_node`] gives production's placement inputs.
/// Meta's `Bootstrap` seeds the same rule over the genesis claim's peers,
/// which are the `raft.peers` ids at genesis. The two agree only while
/// `raft.peers` keeps those ids, so `raft.peers` may not grow until then.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ConfigSeeds {
    groups: BTreeMap<RaftGroupId, GroupSeed>,
}

/// One data group's seed: its voters and the voter that initializes it.
#[derive(Debug, Clone, PartialEq, Eq)]
struct GroupSeed {
    voters: BTreeSet<u64>,
    initializer: u64,
}

impl ConfigSeeds {
    /// The seeds of groups `0..group_count` over the node ids of `peers`.
    /// No peers seed nothing.
    pub fn new(
        peers: &crate::PeerDirectory,
        group_count: usize,
        replication_factor: ursula_proto::placement::ReplicationFactor,
    ) -> Self {
        let ids = peers
            .node_ids()
            .map(ursula_proto::placement::NodeId::new)
            .collect();
        let Ok(nodes) = ursula_proto::placement::NodeSet::new(ids) else {
            return Self::default();
        };
        let replication_factor =
            ursula_control::effective_replication_factor(replication_factor, &nodes);
        let groups = (0..group_count)
            .map_while(|group| u32::try_from(group).ok())
            .map(|group| {
                let group = RaftGroupId(group);
                let seed = GroupSeed {
                    voters: ursula_control::seed_voters(group, replication_factor, &nodes)
                        .iter()
                        .map(ursula_proto::placement::NodeId::get)
                        .collect(),
                    initializer: ursula_control::initializer(group, &nodes).get(),
                };
                (group, seed)
            })
            .collect();
        Self { groups }
    }

    /// The voters `group` is seeded on.
    pub fn voters(&self, group: RaftGroupId) -> Option<&BTreeSet<u64>> {
        self.groups.get(&group).map(|seed| &seed.voters)
    }

    /// The node that initializes `group`.
    pub fn initializer(&self, group: RaftGroupId) -> Option<u64> {
        self.groups.get(&group).map(|seed| seed.initializer)
    }

    /// Whether `node_id` is one of `group`'s seed voters.
    pub fn hosts(&self, node_id: u64, group: RaftGroupId) -> bool {
        self.voters(group)
            .is_some_and(|voters| voters.contains(&node_id))
    }

    /// The seed voters of every group.
    pub fn voter_table(&self) -> BTreeMap<RaftGroupId, BTreeSet<u64>> {
        self.groups
            .iter()
            .map(|(group, seed)| (*group, seed.voters.clone()))
            .collect()
    }
}

/// [`ConfigSeeds`] as one node's placement inputs: the node hosts, and may
/// create, the groups it is a seed voter of, and confirms its seed role
/// without asking, since configuration decided it.
///
/// Interim: removed in S2(h), with [`ConfigSeeds`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigSeedInputs {
    node: u64,
    seeds: ConfigSeeds,
}

impl ConfigSeeds {
    /// The placement inputs of node `node`.
    pub fn for_node(self, node: u64) -> ConfigSeedInputs {
        ConfigSeedInputs { node, seeds: self }
    }
}

impl ConfigSeedInputs {
    /// The node's role in `group`: its initializer, another seed voter, or
    /// none.
    fn role(&self, group: RaftGroupId) -> Option<SeedRoleOf> {
        if self.seeds.initializer(group) == Some(self.node) {
            Some(SeedRoleOf::Initializer)
        } else if self.seeds.hosts(self.node, group) {
            Some(SeedRoleOf::Voter)
        } else {
            None
        }
    }
}

impl PlacementInputs for ConfigSeedInputs {
    fn admission(&self, group: RaftGroupId) -> Option<Admission> {
        self.seeds
            .hosts(self.node, group)
            .then_some(Admission::Create)
    }

    fn seed_role(&self, group: RaftGroupId) -> Arc<dyn SeedRole> {
        Arc::new(FixedRole(self.role(group)))
    }

    fn nodes(&self, group: RaftGroupId) -> Arc<dyn GroupNodes> {
        Arc::new(FixedNodes::from_voters(
            self.seeds.voters(group).cloned().unwrap_or_default(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use ursula_shard::RaftGroupId;

    use super::Admission;
    use super::ConfigSeeds;
    use super::PlacementInputs;
    use super::SeedRoleOf;

    /// The seed rule's inputs over four nodes at the default replication
    /// factor: group `g`'s voters are three consecutive ids from index
    /// `g mod 4` and its initializer is the first of them. A node hosts only
    /// the groups it is a seed voter of, and a group without a seed is
    /// hosted nowhere, never run on every peer.
    #[test]
    fn config_seeds_admit_the_seed_voters_and_name_one_initializer() {
        let peers = crate::PeerDirectory::new((1..=4).map(|node_id| {
            (
                node_id,
                crate::PeerEndpoints::shared(format!("http://node-{node_id}")),
            )
        }));
        let seeds = ConfigSeeds::new(
            &peers,
            2,
            ursula_config::PlacementConfig::default().replication_factor,
        );
        let nodes: Vec<_> = (1..=4).map(|node| seeds.clone().for_node(node)).collect();

        let voters = |group| nodes[0].nodes(RaftGroupId(group)).voters();
        assert_eq!(voters(0), BTreeSet::from([1, 2, 3]));
        assert_eq!(voters(1), BTreeSet::from([2, 3, 4]));
        assert_eq!(voters(2), BTreeSet::new());
        let hosted = |group| {
            nodes
                .iter()
                .filter(|node| node.admission(RaftGroupId(group)) == Some(Admission::Create))
                .map(|node| node.node)
                .collect::<Vec<_>>()
        };
        assert_eq!(hosted(0), vec![1, 2, 3]);
        assert_eq!(hosted(1), vec![2, 3, 4]);
        assert_eq!(hosted(2), Vec::<u64>::new());

        let roles = |group| {
            nodes
                .iter()
                .map(|node| node.seed_role(RaftGroupId(group)).local_role())
                .collect::<Vec<_>>()
        };
        let (init, voter) = (Some(SeedRoleOf::Initializer), Some(SeedRoleOf::Voter));
        assert_eq!(roles(0), vec![init, voter, voter, None]);
        assert_eq!(roles(1), vec![None, init, voter, voter]);
    }
}

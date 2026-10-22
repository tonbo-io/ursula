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
//! placement views. Every Raft node of a server runs it. [`FixedPlacement`]
//! gives tests and the simulation one fixed voter set. The meta group keeps
//! its genesis driver and passes its claim through [`FixedNodes`] and
//! [`FixedRole`].

use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::OnceLock;

use futures_util::future::BoxFuture;
use ursula_control::Admission;
use ursula_control::SeedRoleOf;
use ursula_proto::placement::NodeId;
use ursula_proto::placement::ReplicaRole;
use ursula_shard::RaftGroupId;

use crate::RaftGroupHandleRegistry;
use crate::hosting::HostingSet;
use crate::meta::confirm::MetaBarrierError;
use crate::meta::status::MetaStatusReader;
use crate::meta::watch::GroupPlacementWatch;

/// Every placement input of a data replica's open, by group.
pub trait PlacementInputs: std::fmt::Debug + Send + Sync + 'static {
    /// How the node may open `group`, `None` when it does not host it.
    fn admission(&self, group: RaftGroupId) -> Option<Admission>;
    /// The node's seed role in `group`.
    fn seed_role(&self, group: RaftGroupId) -> Arc<dyn SeedRole>;
    /// The node sets of `group` the replica's drivers read.
    fn nodes(&self, group: RaftGroupId) -> Arc<dyn GroupNodes>;
}

/// A replica's seed role in its group.
pub trait SeedRole: Send + Sync + 'static {
    /// The role the local placement shows, unconfirmed. The bootstrap
    /// driver probes and initializes only while it shows `Initializer`
    /// ([`Self::may_change`] says when the driver runs).
    fn local_role(&self) -> Option<SeedRoleOf>;
    /// Whether [`Self::local_role`] may change while the replica runs. A
    /// replica whose role may change runs the bootstrap driver until its
    /// group holds history, and the driver probes only while the role shows
    /// `Initializer`. A fixed role decides once, when the replica opens.
    fn may_change(&self) -> bool;
    /// The role from a confirmed placement. `Ok(None)` stops the driver
    /// that asks, and `Err` makes it retry.
    fn confirmed_role(&self) -> BoxFuture<'_, Result<Option<SeedRoleOf>, MetaBarrierError>>;
    /// The node's role in the group in a placement it holds confirmed now,
    /// without asking: the first term of the voter predicate
    /// ([`ursula_control::counts_as_voter`]). `None` without a confirmation
    /// or without a role.
    fn placed_role(&self) -> Option<ReplicaRole>;
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

    /// Whether `node` is the group's only voter, so no other node could
    /// take its leadership. Never while the voters are unknown (empty).
    fn is_only_voter(&self, node: u64) -> bool {
        let voters = self.voters();
        voters.len() == 1 && voters.contains(&node)
    }
}

/// A role that needs no confirmation: configuration, or a test, decided it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FixedRole(pub Option<SeedRoleOf>);

impl SeedRole for FixedRole {
    fn local_role(&self) -> Option<SeedRoleOf> {
        self.0
    }

    fn may_change(&self) -> bool {
        false
    }

    fn confirmed_role(&self) -> BoxFuture<'_, Result<Option<SeedRoleOf>, MetaBarrierError>> {
        Box::pin(std::future::ready(Ok(self.0)))
    }

    /// A seed role, decided without asking, is a seed voter's: the role
    /// `Voter` of a seeded group.
    fn placed_role(&self) -> Option<ReplicaRole> {
        self.0.map(|_role| ReplicaRole::Voter)
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
///
/// The engine factory takes the inputs before the meta runtime starts, so
/// the meta read barrier binds later ([`Self::bind_meta`]). Until then the
/// hosting set is empty, so no replica opens, and a confirmation fails as
/// it does on a node that runs no meta replica
/// ([`MetaBarrierError::Stopped`]). Cloning shares the binding.
#[derive(Debug, Clone)]
pub struct MetaPlacement {
    node: NodeId,
    registry: RaftGroupHandleRegistry,
    hosting: HostingSet,
    meta: Arc<OnceLock<MetaStatusReader>>,
}

impl MetaPlacement {
    /// The inputs of node `node`, read from `registry`'s placement views,
    /// admitting what `hosting` holds.
    pub fn new(node: NodeId, registry: RaftGroupHandleRegistry, hosting: HostingSet) -> Self {
        Self {
            node,
            registry,
            hosting,
            meta: Arc::default(),
        }
    }

    /// Confirms views through `meta`, the meta runtime of this node and
    /// registry, from now on. The first binding holds.
    pub fn bind_meta(&self, meta: MetaStatusReader) {
        if self.meta.set(meta).is_err() {
            tracing::warn!("placement inputs: meta was bound already; the first binding stays");
        }
    }
}

impl PlacementInputs for MetaPlacement {
    fn admission(&self, group: RaftGroupId) -> Option<Admission> {
        self.hosting.admission(group)
    }

    fn seed_role(&self, group: RaftGroupId) -> Arc<dyn SeedRole> {
        Arc::new(MetaSeedRole {
            placement: self.clone(),
            group,
        })
    }

    fn nodes(&self, group: RaftGroupId) -> Arc<dyn GroupNodes> {
        Arc::new(PlacedNodes(GroupPlacementWatch::new(
            self.registry.placement_watch(),
            group,
        )))
    }
}

/// A seed role read from this node's placement views.
struct MetaSeedRole {
    placement: MetaPlacement,
    group: RaftGroupId,
}

/// Whether a negative control turned off the rule that the seed role comes
/// from the placement view (`Defence::InitializeOnlySeeded`): the role is
/// then `Initializer` for any view.
fn any_view_initializes() -> bool {
    let off = ursula_stream::sim_controls::off(
        ursula_stream::sim_controls::Defence::InitializeOnlySeeded,
    );
    if off {
        ursula_stream::sim_controls::note_fired(
            ursula_stream::sim_controls::Defence::InitializeOnlySeeded,
        );
    }
    off
}

impl SeedRole for MetaSeedRole {
    fn local_role(&self) -> Option<SeedRoleOf> {
        if any_view_initializes() {
            return Some(SeedRoleOf::Initializer);
        }
        ursula_control::shown_seed_role(
            &self.placement.registry.placement_watch().current(),
            self.placement.node,
            self.group,
        )
    }

    /// A view that regressed or has not reached the bootstrap yet shows no
    /// role, and a later one shows it.
    fn may_change(&self) -> bool {
        true
    }

    fn confirmed_role(&self) -> BoxFuture<'_, Result<Option<SeedRoleOf>, MetaBarrierError>> {
        Box::pin(async move {
            if any_view_initializes() {
                return Ok(Some(SeedRoleOf::Initializer));
            }
            let meta = self.placement.meta.get().ok_or(MetaBarrierError::Stopped)?;
            let confirmed = meta.confirm_view().await?;
            Ok(ursula_control::seed_role(
                &confirmed,
                self.placement.node,
                self.group,
            ))
        })
    }

    /// The role in this node's latest view while the last meta read
    /// barrier's stamp still covers it
    /// ([`crate::RaftGroupHandleRegistry::confirmed_view`]).
    fn placed_role(&self) -> Option<ReplicaRole> {
        let confirmed = self.placement.registry.confirmed_view()?;
        confirmed
            .view()
            .cluster()?
            .role(self.group, self.placement.node)
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

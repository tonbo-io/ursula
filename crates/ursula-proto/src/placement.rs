//! Placement vocabulary shared by the meta group's kernel (`ursula-control`),
//! the journal metadata, the maintenance report, group metrics and
//! `ursulactl`.
//!
//! The serde form of these types is persisted in the meta log and its
//! snapshots and sent between nodes, and every encode and decode goes through
//! `ursula_stream::msgpack`, which writes a struct as an array of its fields.
//! Field order is therefore format: never reorder, add or remove a field of a
//! type here outside a format epoch. No type here uses `default`,
//! `skip_serializing_if`, `flatten` or an untagged enum. Newtypes are
//! transparent, and a set that is never empty decodes through a validating
//! `try_from`.
//!
//! Group ids are `RaftGroupId` in the kernel's state and plain `u32` in admin
//! JSON.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::fmt;

use serde::Deserialize;
use serde::Serialize;

/// The id of a node, as `raft.peers` names it and OpenRaft's membership
/// carries it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct NodeId(u64);

impl NodeId {
    pub const fn new(id: u64) -> Self {
        Self(id)
    }

    pub const fn get(self) -> u64 {
        self.0
    }
}

impl From<u64> for NodeId {
    fn from(id: u64) -> Self {
        Self(id)
    }
}

impl From<NodeId> for u64 {
    fn from(id: NodeId) -> Self {
        id.0
    }
}

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// The id of an entry of a Raft log: its leader's `(term, leader)`, then its
/// index, compared in that order.
///
/// After a recovery plan cut a log, the next leader's electing majority meets
/// every earlier leader's (plan members keep their votes), so its term
/// exceeds every earlier term and its entries compare above every entry
/// written before the cut. Comparing by index alone would not.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct RaftLogId {
    pub term: u64,
    pub leader: NodeId,
    pub index: u64,
}

impl fmt::Display for RaftLogId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}-{}-{}", self.term, self.leader, self.index)
    }
}

/// An entry of the meta group's log.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct MetaLogId(RaftLogId);

impl MetaLogId {
    pub const fn new(id: RaftLogId) -> Self {
        Self(id)
    }

    pub const fn get(self) -> RaftLogId {
        self.0
    }
}

impl fmt::Display for MetaLogId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

/// A membership entry of a data group's log.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct MembershipLogId(RaftLogId);

impl MembershipLogId {
    pub const fn new(id: RaftLogId) -> Self {
        Self(id)
    }

    pub const fn get(self) -> RaftLogId {
        self.0
    }
}

impl fmt::Display for MembershipLogId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

/// A group's placement key: the meta entry that set its placement, or the
/// last meta history event, whichever came later. Only those two write it,
/// and every command that acts on a placement names the one it read.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct PlacementEpoch(MetaLogId);

impl PlacementEpoch {
    pub const fn new(at: MetaLogId) -> Self {
        Self(at)
    }

    pub const fn log_id(self) -> MetaLogId {
        self.0
    }
}

impl fmt::Display for PlacementEpoch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

/// Where a node is in its one life. `Removed` is terminal: a removed id is
/// never placed again.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NodeLifecycle {
    /// Takes new replicas and keeps its own.
    Active,
    /// Keeps its replicas and takes no new one.
    Cordoned,
    /// Its groups move away. It becomes `Removed` once it has no role.
    Decommissioning,
    /// Has no role for good.
    Removed,
}

/// A registered node's lifecycle and when it entered it (display only).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct NodeRecord {
    pub lifecycle: NodeLifecycle,
    pub since_ms: u64,
}

/// A node set with no id: [`VoterSet`] and [`NodeSet`] hold at least one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("a node set holds at least one node id")]
pub struct EmptyNodeSet;

// A set of node ids that is never empty, encoded as the plain set. The
// smallest id is held apart, so `min` needs no fallback.
macro_rules! non_empty_node_set {
    ($(#[$attr:meta])* $name:ident) => {
        $(#[$attr])*
        #[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
        #[serde(try_from = "BTreeSet<NodeId>", into = "BTreeSet<NodeId>")]
        pub struct $name {
            min: NodeId,
            rest: BTreeSet<NodeId>,
        }

        impl $name {
            /// Parses a set, refusing an empty one.
            pub fn new(ids: BTreeSet<NodeId>) -> Result<Self, EmptyNodeSet> {
                let mut rest = ids;
                let min = rest.pop_first().ok_or(EmptyNodeSet)?;
                Ok(Self { min, rest })
            }

            /// The set of one id.
            pub fn single(id: NodeId) -> Self {
                Self {
                    min: id,
                    rest: BTreeSet::new(),
                }
            }

            /// The set of `first` and `others`, which may repeat it.
            pub fn from_first(first: NodeId, others: impl IntoIterator<Item = NodeId>) -> Self {
                let mut rest: BTreeSet<NodeId> = others.into_iter().collect();
                rest.insert(first);
                let min = rest.pop_first().unwrap_or(first);
                Self { min, rest }
            }

            /// The smallest id.
            pub fn min(&self) -> NodeId {
                self.min
            }

            pub fn contains(&self, id: NodeId) -> bool {
                id == self.min || self.rest.contains(&id)
            }

            /// How many ids the set holds, at least one.
            pub fn len(&self) -> usize {
                self.rest.len().saturating_add(1)
            }

            /// Never: the set holds at least one id.
            pub fn is_empty(&self) -> bool {
                false
            }

            /// The ids in ascending order.
            pub fn iter(&self) -> impl Iterator<Item = NodeId> + '_ {
                std::iter::once(self.min).chain(self.rest.iter().copied())
            }

            /// The ids as a plain set.
            pub fn to_set(&self) -> BTreeSet<NodeId> {
                self.iter().collect()
            }
        }

        impl TryFrom<BTreeSet<NodeId>> for $name {
            type Error = EmptyNodeSet;

            fn try_from(ids: BTreeSet<NodeId>) -> Result<Self, Self::Error> {
                Self::new(ids)
            }
        }

        impl From<$name> for BTreeSet<NodeId> {
            fn from(set: $name) -> Self {
                let mut ids = set.rest;
                ids.insert(set.min);
                ids
            }
        }
    };
}

non_empty_node_set!(
    /// The voters of a group or of the meta group: never empty.
    VoterSet
);

non_empty_node_set!(
    /// A set of nodes, such as the ones a cluster bootstraps over: never
    /// empty.
    NodeSet
);

/// A node's role in one group's placement.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReplicaRole {
    /// A seed or stable voter, or a voter of both sides of a move.
    Voter,
    /// In a move's target and not in its source.
    Incoming,
    /// In a move's source and not in its target.
    Outgoing,
    /// An earlier target of a move that is in neither its source nor its
    /// target.
    Abandoned,
}

/// What the meta group answered a node's registration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Registered {
    /// The cluster has not bootstrapped: the node waits for its placement.
    Forming,
    /// The node has a role in at least one group.
    Referenced(NodeRecord),
    /// The node has no role in any group.
    Unreferenced(NodeRecord),
}

/// How far a node's placement view reaches.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ViewStage {
    /// The local meta replica applied no history event yet.
    Empty,
    /// The local meta replica applied up to `applied`, in the history that
    /// the event at `history` began.
    Applied {
        applied: MetaLogId,
        history: MetaLogId,
    },
}

/// A live group leader's answer about one node, from its committed and its
/// effective memberships.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MembershipAnswer {
    /// A voter of a config of either membership.
    Voter,
    /// A member of either membership and a voter of neither.
    Learner,
    /// A member of neither membership.
    NotMember,
}

/// How far a replica's teardown went.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TeardownStage {
    /// The node asks the group whether it is a member. Nothing is fenced.
    Ask,
    /// The replica is fenced, stopped, and retired once the group answers
    /// again.
    Fence,
}

/// Why a group leader's reconciler takes no membership step.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WaitReason {
    NotLeader,
    TransferPending,
    StepPending,
    MembershipUncommitted,
    NotAMember,
    NoLiveQuorum,
    TargetSettled,
    /// Learners whose matched index is behind the committed one.
    CatchingUp {
        committed: u64,
        behind: BTreeMap<NodeId, Option<u64>>,
    },
    /// Targets the leader's address book does not list.
    PeerNotListed {
        nodes: BTreeSet<NodeId>,
    },
    /// Caught-up learners whose recovery gate is closed.
    GateClosed {
        nodes: BTreeSet<NodeId>,
    },
    /// Caught-up learners whose gate did not answer.
    GateUnknown {
        nodes: BTreeSet<NodeId>,
    },
    NoHandoffTarget,
}

/// A membership step a reconciler takes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StepKind {
    AddLearners,
    ChangeVoters,
    RemoveLearners,
    TransferLeadership,
    ConfirmView,
    ReportConverged,
}

/// Why a reconciler's step failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StepFailureKind {
    /// The membership moved since the step was planned.
    PreconditionFailed,
    /// Another membership change was in progress.
    InProgress,
    /// The node no longer leads the group.
    NotLeader,
    /// The meta group did not answer.
    MetaUnavailable,
    /// The node could not prove its leadership before a report.
    LeadershipProofFailed,
    /// A leadership transfer did not complete.
    TransferAbandoned,
}

/// The last failed step of a reconciler.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct StepFailure {
    pub step: StepKind,
    pub kind: StepFailureKind,
    pub at_ms: u64,
}

/// What a group leader's reconciler is doing.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReconcileState {
    Stepping(StepKind),
    Waiting(WaitReason),
    Reporting,
    /// A report named a placement the meta group had moved past: the
    /// leader confirms its view before it plans again.
    StaleView {
        reported: PlacementEpoch,
        current: PlacementEpoch,
    },
    Converged,
}

/// A group leader's reconcile status, published in its group metrics.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ReconcileStatus {
    pub state: ReconcileState,
    pub since_ms: u64,
    pub last_failure: Option<StepFailure>,
    pub attempts: u32,
    /// The placement the leader pursues. `None` for a settled target, which
    /// has no placement key.
    pub target_since: Option<PlacementEpoch>,
    /// The meta entry the leader's view includes. `None` for an empty view.
    pub view_applied: Option<MetaLogId>,
}

/// Whether a refused or failed admin write may have taken effect.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WriteEffect {
    /// The write did not apply.
    NotApplied,
    /// The write may have applied: read a confirmed view to learn whether.
    MayHaveApplied,
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::EmptyNodeSet;
    use super::MetaLogId;
    use super::NodeId;
    use super::RaftLogId;
    use super::VoterSet;

    fn ids(values: &[u64]) -> BTreeSet<NodeId> {
        values.iter().copied().map(NodeId::new).collect()
    }

    #[test]
    fn a_voter_set_is_never_empty_and_encodes_as_the_plain_set() {
        assert!(matches!(VoterSet::new(BTreeSet::new()), Err(EmptyNodeSet)));
        let voters = VoterSet::new(ids(&[3, 1, 2])).unwrap();
        assert_eq!(voters.min(), NodeId::new(1));
        assert_eq!(voters.len(), 3);
        assert!(voters.contains(NodeId::new(3)));
        assert!(!voters.contains(NodeId::new(4)));
        assert_eq!(voters.iter().collect::<Vec<_>>(), vec![
            NodeId::new(1),
            NodeId::new(2),
            NodeId::new(3)
        ]);
        let json = serde_json::to_string(&voters).unwrap();
        assert_eq!(json, "[1,2,3]");
        assert_eq!(serde_json::from_str::<VoterSet>(&json).unwrap(), voters);
        serde_json::from_str::<VoterSet>("[]").expect_err("an empty set does not decode");
        assert_eq!(
            VoterSet::from_first(NodeId::new(5), [NodeId::new(2), NodeId::new(5)]),
            VoterSet::new(ids(&[2, 5])).unwrap()
        );
    }

    #[test]
    fn log_ids_compare_term_first() {
        let id = |term, leader, index| {
            MetaLogId::new(RaftLogId {
                term,
                leader: NodeId::new(leader),
                index,
            })
        };
        assert!(id(2, 1, 1) > id(1, 3, 100));
        assert!(id(2, 2, 1) > id(2, 1, 100));
        assert!(id(2, 2, 5) > id(2, 2, 4));
    }
}

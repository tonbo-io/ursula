//! Placement vocabulary shared by the meta group's kernel (`ursula-control`),
//! the journal metadata, the maintenance report, group metrics and
//! `ursulactl`.
//!
//! Each type here is in one of three classes, and its class says what may
//! change it:
//!
//! - **Format**: persisted in the meta log and its snapshots, replicated, or
//!   sent between nodes. [`NodeId`], [`RaftLogId`], [`MetaLogId`],
//!   [`MembershipLogId`], [`PlacementEpoch`], [`NodeLifecycle`],
//!   [`RootId`] (also in the WAL's `topology.bin`), [`RootSlot`],
//!   [`NodeRecord`], [`VoterSet`], [`NodeSet`], [`GroupCount`],
//!   [`ReplicationFactor`], [`GenesisClaim`] (the genesis probe) and
//!   [`Registered`] (in the kernel's answers). Every encode and decode goes
//!   through `ursula_stream::msgpack`, which writes a struct as an array of
//!   its fields, so field order is format: never reorder, add or remove a
//!   field outside a format epoch. These types get wire fixtures with the
//!   meta group's roots. No type in the replicated, persisted or RPC
//!   MessagePack closure uses `#[serde(other)]`: an unknown variant decoding
//!   as another would hide a format mismatch.
//! - **Admin JSON**: the admin protocol, compatible within a minor and never
//!   in the meta log or between nodes. [`ReplicaRole`], [`MetaRole`],
//!   [`ClusterStage`], [`ExpectedReplica`], [`UnplacedReplica`],
//!   [`TeardownStage`], [`WaitReason`], [`StepKind`], [`StepFailureKind`],
//!   [`StepFailure`], [`ReconcileState`], [`ReconcileStatus`] and
//!   [`WriteEffect`].
//! - **Header and admin JSON**: [`ProcessIncarnation`], one fixed grammar
//!   of 32 hex digits, carried as a header or gRPC metadata value and in
//!   admin JSON, in no MessagePack type.
//! - **Never encoded**: [`ViewStage`], [`MembershipAnswer`] and
//!   [`Relation`], which stay inside one process and derive no serde.
//!
//! In both encoded classes a variant is written by its `snake_case` name, so
//! variant names are format too: renaming one is a format change, or an
//! admin protocol change. The `snake_case` names are deliberate, and so is
//! the different casing of the Raft-layer envelopes beside them
//! (`StreamCommand`, `RaftCommand`, `MetaCommand`, `MetaResponse` and
//! `GenesisAnswer`), which keep serde's default `PascalCase` names as 0.9's
//! `StreamCommand` does. The types here and those of `ursula_control` are
//! `snake_case` because one serde form serves both the admin JSON and the
//! MessagePack closure. Each casing is frozen with format epoch 6, and
//! neither is converted to the other. No type here uses `default`,
//! `skip_serializing_if`, `flatten` or an untagged enum. Newtypes are
//! transparent but for [`RootId`], which writes its hex string, and a set
//! that is never empty decodes through a validating `try_from`.
//!
//! **Admin JSON enums tolerate unknown variants.** The planners and the
//! reconciler decide only what is proposed, so a patch of the minor may add
//! a reason or a step, and a same-minor `ursulactl` must still decode the
//! answer around it. Every admin JSON enum a planner, a driver or the
//! reconciler produces ([`TeardownStage`], [`WaitReason`], [`StepKind`],
//! [`StepFailureKind`], [`ReconcileState`], and `ursula-control`'s
//! `NodeEligibility` and `BlockReason`) ends in a unit `Unknown` variant
//! marked `#[serde(other)]`, which an unknown name decodes to and nothing
//! here produces. An enum whose variants carry data is internally tagged
//! (`reason`, `state`), so its discriminant is a plain string beside the
//! details and an unknown one decodes to `Unknown`, its details ignored. Such
//! an enum's details hold no map with number keys, since serde buffers an
//! internally tagged value and a buffered JSON key stays a string: a list
//! of structs takes its place. [`ReplicaRole`], [`MetaRole`],
//! [`ClusterStage`] and [`WriteEffect`] stay closed: a role and a stage are
//! the kernel's, fixed with the format epoch, and a write effect is a yes or
//! no a client must not guess. An `Option` field of an
//! admin JSON struct is required, also when `null`
//! ([`crate::admin::required_option`]).
//!
//! Group ids are `RaftGroupId` in the kernel's state and plain `u32` in admin
//! JSON.

use std::cmp::Ordering;
use std::collections::BTreeSet;
use std::fmt;
use std::num::NonZeroU8;
use std::num::NonZeroU32;
use std::num::NonZeroU64;

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
/// Under OpenRaft's `leader_id_adv` two leaders can share a term, so the
/// term alone does not order leaderships: the `(term, leader)` pair does,
/// as a node votes only for a pair not below the one it last voted for.
/// After a
/// recovery plan cut a log, the next leader's electing majority meets every
/// earlier leader's (plan members keep their votes), so its `(term, leader)`
/// exceeds every earlier one and its entries compare above every entry
/// written before the cut. Comparing by index alone would not.
///
/// The order is written by hand, not derived: field order is the encoding,
/// and the order must not change with it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RaftLogId {
    pub term: u64,
    pub leader: NodeId,
    pub index: u64,
}

impl Ord for RaftLogId {
    fn cmp(&self, other: &Self) -> Ordering {
        self.term
            .cmp(&other.term)
            .then_with(|| self.leader.cmp(&other.leader))
            .then_with(|| self.index.cmp(&other.index))
    }
}

impl PartialOrd for RaftLogId {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl fmt::Display for RaftLogId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}-{}-{}", self.term, self.leader, self.index)
    }
}

/// A log id that is not `term-leader-index` in decimal, as [`RaftLogId`]
/// displays it.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("a log id is term-leader-index in decimal, such as 3-1-57, not '{found}'")]
pub struct InvalidRaftLogId {
    pub found: String,
}

impl std::str::FromStr for RaftLogId {
    type Err = InvalidRaftLogId;

    /// Parses what [`fmt::Display`] writes, so an operator pastes a log id
    /// a status printed.
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let invalid = || InvalidRaftLogId {
            found: value.to_owned(),
        };
        let mut parts = value.split('-').map(|part| {
            if !part.is_empty() && part.bytes().all(|byte| byte.is_ascii_digit()) {
                part.parse::<u64>().ok()
            } else {
                None
            }
        });
        match (parts.next(), parts.next(), parts.next(), parts.next()) {
            (Some(Some(term)), Some(Some(leader)), Some(Some(index)), None) => Ok(Self {
                term,
                leader: NodeId(leader),
                index,
            }),
            _ => Err(invalid()),
        }
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

impl std::str::FromStr for PlacementEpoch {
    type Err = InvalidRaftLogId;

    /// Parses what [`fmt::Display`] writes, `term-leader-index`.
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        value.parse().map(|id| Self(MetaLogId(id)))
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

impl fmt::Display for NodeLifecycle {
    /// The lifecycle's name as its encodings write it.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Active => "active",
            Self::Cordoned => "cordoned",
            Self::Decommissioning => "decommissioning",
            Self::Removed => "removed",
        })
    }
}

/// The id of a node's Raft WAL root, drawn nonzero when the root's
/// `topology.bin` is created and kept for the root's life. A new disk is a
/// new root.
///
/// Encoded as 16 lowercase hex digits in every encoding, admin JSON and
/// MessagePack alike, never as a number: a random id above 2^53 does not
/// survive a JSON reader that parses numbers as doubles. [`FromStr`] parses
/// the same grammar, so an operator pastes what a status printed.
///
/// [`FromStr`]: std::str::FromStr
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct RootId(NonZeroU64);

impl RootId {
    pub const fn new(id: NonZeroU64) -> Self {
        Self(id)
    }

    pub const fn get(self) -> u64 {
        self.0.get()
    }
}

/// A root id that is not 16 lowercase hex digits naming a nonzero id.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("a root id is 16 lowercase hexadecimal digits and not zero, not '{found}'")]
pub struct InvalidRootId {
    pub found: String,
}

impl fmt::Display for RootId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:016x}", self.0)
    }
}

impl std::str::FromStr for RootId {
    type Err = InvalidRootId;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let invalid = || InvalidRootId {
            found: value.to_owned(),
        };
        if !lower_hex(value, 16) {
            return Err(invalid());
        }
        u64::from_str_radix(value, 16)
            .ok()
            .and_then(NonZeroU64::new)
            .map(Self)
            .ok_or_else(invalid)
    }
}

impl TryFrom<String> for RootId {
    type Error = InvalidRootId;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        value.parse()
    }
}

impl From<RootId> for String {
    fn from(id: RootId) -> Self {
        id.to_string()
    }
}

/// Whether `value` is exactly `digits` lowercase hex digits.
fn lower_hex(value: &str, digits: usize) -> bool {
    value.len() == digits
        && value
            .bytes()
            .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
}

/// One process of one node id: the WAL root it runs on, the run of that
/// root it is (`run` counts the runs that opened the root, 0 for a node
/// without a WAL), and a `nonce` drawn at start that tells apart two
/// processes of one run counter.
///
/// Encoded as 32 lowercase hex digits, the root's 16 as [`RootId`] writes
/// them, then `run` and `nonce` with 8 each: the value of the
/// `x-ursula-process-incarnation` header ([`crate::admin`]) and of admin
/// JSON. No MessagePack type carries one. Two incarnations are compared only
/// by [`ProcessIncarnation::relation`], never by a derived order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct ProcessIncarnation {
    pub root: RootId,
    pub run: u32,
    pub nonce: u32,
}

/// How one process incarnation stands to another
/// ([`ProcessIncarnation::relation`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Relation {
    /// The same process.
    Same,
    /// An earlier run of the same root.
    Older,
    /// A later run of the same root.
    Newer,
    /// Another process of the same run of the same root: two processes
    /// that read one run counter.
    Duplicate,
    /// A process of another root, which only the placement view orders.
    OtherRoot,
}

impl ProcessIncarnation {
    /// How `other` stands to `self`: [`Relation::Newer`] when `other` is a
    /// later run of `self`'s root. Another root is [`Relation::OtherRoot`],
    /// and on one root the run decides, an equal run with another nonce
    /// being [`Relation::Duplicate`].
    pub fn relation(&self, other: &Self) -> Relation {
        if self.root != other.root {
            return Relation::OtherRoot;
        }
        match other.run.cmp(&self.run) {
            Ordering::Less => Relation::Older,
            Ordering::Greater => Relation::Newer,
            Ordering::Equal if other.nonce == self.nonce => Relation::Same,
            Ordering::Equal => Relation::Duplicate,
        }
    }
}

/// A process incarnation that is not 32 lowercase hex digits whose first 16
/// name a nonzero root.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error(
    "a process incarnation is 32 lowercase hexadecimal digits, the first 16 a nonzero root id, \
     not '{found}'"
)]
pub struct InvalidProcessIncarnation {
    pub found: String,
}

impl fmt::Display for ProcessIncarnation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}{:08x}{:08x}", self.root, self.run, self.nonce)
    }
}

impl std::str::FromStr for ProcessIncarnation {
    type Err = InvalidProcessIncarnation;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let invalid = || InvalidProcessIncarnation {
            found: value.to_owned(),
        };
        if !lower_hex(value, 32) {
            return Err(invalid());
        }
        let (root, rest) = value.split_at_checked(16).ok_or_else(invalid)?;
        let (run, nonce) = rest.split_at_checked(8).ok_or_else(invalid)?;
        Ok(Self {
            root: root.parse().map_err(|_zero| invalid())?,
            run: u32::from_str_radix(run, 16).map_err(|_digits| invalid())?,
            nonce: u32::from_str_radix(nonce, 16).map_err(|_digits| invalid())?,
        })
    }
}

impl TryFrom<String> for ProcessIncarnation {
    type Error = InvalidProcessIncarnation;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        value.parse()
    }
}

impl From<ProcessIncarnation> for String {
    fn from(incarnation: ProcessIncarnation) -> Self {
        incarnation.to_string()
    }
}

/// The root a node id is bound to in the meta group.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RootSlot {
    /// The node id runs on this root.
    Bound(RootId),
    /// An operator released this root (`replace-disk`): it never binds
    /// again while the slot names it, and a root of the node id that meta
    /// never bound (`empty`) takes the slot. The slot remembers only the
    /// latest release: a root released earlier registers as not `empty`
    /// once its data replicas took part, and the release rule (a root is
    /// released only once its disk is revoked) keeps it out otherwise.
    Released(RootId),
}

impl RootSlot {
    /// The root the slot names, bound or released.
    pub fn root(self) -> RootId {
        match self {
            Self::Bound(root) | Self::Released(root) => root,
        }
    }
}

impl fmt::Display for RootSlot {
    /// `bound <root>` or `released <root>`, the root as [`RootId`] writes it.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Bound(root) => write!(f, "bound {root}"),
            Self::Released(root) => write!(f, "released {root}"),
        }
    }
}

/// A registered node's lifecycle, when it entered it (display only), and
/// the root its id is bound to. A slot change leaves `since_ms`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct NodeRecord {
    pub lifecycle: NodeLifecycle,
    pub since_ms: u64,
    pub root: RootSlot,
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

/// The most data groups a cluster may have.
///
/// The meta group's snapshot is always `Inline`, so the whole meta state is
/// one value: held in memory behind every placement view, rewritten whole
/// into the meta snapshot record at every meta snapshot and sent whole in
/// one install. One placement per group dominates it, about 100 bytes at
/// RF 3 in compact MessagePack (two log ids, the voter sets of a move and
/// their framing), so 4,096 groups keep it near 400 KiB, and every group
/// moving with full-width ids and owing retirements stays within a few MiB.
/// 4,096 is 16 groups per core, the default, on a 256-core host, and eight
/// times the largest preset. Node records grow with the ids ever
/// registered, not with the group count.
pub const MAX_GROUP_COUNT: u32 = 4_096;

/// Whether a cluster may have `count` data groups: at most
/// [`MAX_GROUP_COUNT`]. `Bootstrap` and the configuration's
/// `raft.group_count` both check it.
pub fn group_count_within_bound(count: u64) -> bool {
    count <= u64::from(MAX_GROUP_COUNT)
}

/// How many data groups a cluster has, at least one. Group ids are
/// `0..group_count`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct GroupCount(NonZeroU32);

impl GroupCount {
    pub const fn new(count: NonZeroU32) -> Self {
        Self(count)
    }

    pub const fn get(self) -> u32 {
        self.0.get()
    }
}

/// How many voters each data group has, at least one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ReplicationFactor(NonZeroU8);

impl ReplicationFactor {
    pub const fn new(factor: NonZeroU8) -> Self {
        Self(factor)
    }

    pub const fn get(self) -> u8 {
        self.0.get()
    }
}

/// The values a node proposes the meta group's genesis with. Every node must
/// claim the same before meta initializes.
///
/// The claim lists peer ids, not a digest of `raft.peers`: genesis rests on
/// the id set a node probes, not on how it writes addresses, and a mismatch
/// names the ids that differ.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct GenesisClaim {
    /// The configured meta voters.
    pub meta_voters: VoterSet,
    /// The ids of `raft.peers`.
    pub peers: NodeSet,
    pub group_count: GroupCount,
    /// The effective replication factor.
    pub replication_factor: ReplicationFactor,
}

impl GenesisClaim {
    /// Whether node `node` is the genesis node, the one that initializes
    /// the meta group: the smallest claimed meta voter.
    pub fn is_genesis_node(&self, node: NodeId) -> bool {
        self.meta_voters.min() == node
    }
}

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

impl ReplicaRole {
    /// Whether a replica of this role may take over its group's leadership:
    /// a voter's or an incoming replica's may, an outgoing or abandoned one
    /// is about to leave the group (design 5.9). Every leadership handoff
    /// planner asks this one rule.
    pub fn takes_leadership(self) -> bool {
        match self {
            Self::Voter | Self::Incoming => true,
            Self::Outgoing | Self::Abandoned => false,
        }
    }
}

/// A node's role in meta's effective membership.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MetaRole {
    Voter,
    Learner,
    NotMember,
}

/// The stage of a node's placement view: `ursula_control`'s `ClusterView`
/// without its details.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ClusterStage {
    /// Meta has not formed.
    MetaGenesis,
    /// Meta formed elsewhere and the node's replica applied nothing yet.
    MetaCatchingUp,
    /// Meta formed and nodes register.
    Registering,
    /// A meta recovery plan ended the formation.
    FormationCut,
    Bootstrapped,
}

/// A group in which the node's bootstrapped view gives it a role, as the
/// maintenance report lists it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExpectedReplica {
    pub role: ReplicaRole,
    /// The group's placement key.
    pub since: PlacementEpoch,
    /// The voters the group should end on: its seed or stable voters, or
    /// its move's target.
    pub target: VoterSet,
    /// Whether the node's replica counts as a voter, by the one voter
    /// predicate, also while it is closed.
    pub counts_as_voter: bool,
}

/// A replica the node holds or runs while its bootstrapped view gives it
/// no role in the group, as the maintenance report lists it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct UnplacedReplica {
    /// Whether the replica counts as a voter, by the one voter predicate.
    pub counts_as_voter: bool,
    /// What a live leader of the group last answered the node's membership
    /// asker about it in this run, `null` before an answer.
    #[serde(deserialize_with = "crate::admin::required_option")]
    pub last_answer: Option<MembershipAnswer>,
    /// How far the node's teardown of the replica went, `null` while none
    /// runs.
    #[serde(deserialize_with = "crate::admin::required_option")]
    pub stage: Option<TeardownStage>,
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
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
/// effective memberships. Admin JSON in a maintenance report's
/// [`UnplacedReplica::last_answer`]. No reply names it: every asker
/// classifies the memberships a barrier answers with.
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
    /// A stage a newer patch of the minor reports.
    #[serde(other)]
    Unknown,
}

/// A learner the leader waits for, with its matched log index (`None` when
/// it matches nothing).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct LearnerLag {
    pub node: NodeId,
    #[serde(deserialize_with = "crate::admin::required_option")]
    pub matched: Option<u64>,
}

/// Why a group leader's reconciler takes no membership step. Encoded with
/// its name in `reason` beside its details.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(tag = "reason", rename_all = "snake_case")]
pub enum WaitReason {
    NotLeader,
    TransferPending,
    StepPending,
    MembershipUncommitted,
    NotAMember,
    NoLiveQuorum,
    TargetSettled,
    /// Learners whose matched index is behind the committed one, by
    /// ascending node id.
    CatchingUp {
        committed: u64,
        behind: Vec<LearnerLag>,
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
    /// A reason a newer patch of the minor reports, its details dropped.
    #[serde(other)]
    Unknown,
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
    /// A step a newer patch of the minor reports.
    #[serde(other)]
    Unknown,
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
    /// A failure a newer patch of the minor reports.
    #[serde(other)]
    Unknown,
}

/// The last failed step of a reconciler.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct StepFailure {
    pub step: StepKind,
    pub kind: StepFailureKind,
    pub at_ms: u64,
}

/// What a group leader's reconciler is doing. Encoded with its name in
/// `state` beside its details: a wait's `reason` and details sit beside it
/// too.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum ReconcileState {
    Stepping {
        step: StepKind,
    },
    Waiting(WaitReason),
    Reporting,
    /// A report named a placement the meta group had moved past: the
    /// leader confirms its view before it plans again.
    StaleView {
        reported: PlacementEpoch,
        current: PlacementEpoch,
    },
    Converged,
    /// A state a newer patch of the minor reports, its details dropped.
    #[serde(other)]
    Unknown,
}

/// A group leader's reconcile status, published in its group metrics.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ReconcileStatus {
    pub state: ReconcileState,
    /// When `state` last changed.
    pub since_ms: u64,
    /// When the reconciler last made progress: `since_ms`, or later while
    /// a `catching_up` learner's gap shrinks below its smallest since then
    /// or a snapshot is being sent to one of those learners (design 11.4).
    /// A moving group's leader whose progress is older than five minutes
    /// reports `move_stalled`.
    pub progress_ms: u64,
    #[serde(deserialize_with = "crate::admin::required_option")]
    pub last_failure: Option<StepFailure>,
    pub attempts: u32,
    /// The placement the leader pursues. `None` for a settled target, which
    /// has no placement key.
    #[serde(deserialize_with = "crate::admin::required_option")]
    pub target_since: Option<PlacementEpoch>,
    /// The meta entry the leader's view includes. `None` for an empty view.
    #[serde(deserialize_with = "crate::admin::required_option")]
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
    use std::num::NonZeroU8;
    use std::num::NonZeroU32;

    use super::EmptyNodeSet;
    use super::GenesisClaim;
    use super::GroupCount;
    use super::InvalidProcessIncarnation;
    use super::InvalidRaftLogId;
    use super::InvalidRootId;
    use super::LearnerLag;
    use super::MetaLogId;
    use super::NodeId;
    use super::NodeLifecycle;
    use super::NodeRecord;
    use super::NodeSet;
    use super::PlacementEpoch;
    use super::ProcessIncarnation;
    use super::RaftLogId;
    use super::ReconcileState;
    use super::ReconcileStatus;
    use super::Relation;
    use super::ReplicationFactor;
    use super::RootId;
    use super::RootSlot;
    use super::StepFailure;
    use super::StepFailureKind;
    use super::StepKind;
    use super::TeardownStage;
    use super::VoterSet;
    use super::WaitReason;

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
    fn a_lifecycle_displays_as_its_encodings_name() {
        for lifecycle in [
            NodeLifecycle::Active,
            NodeLifecycle::Cordoned,
            NodeLifecycle::Decommissioning,
            NodeLifecycle::Removed,
        ] {
            let json = serde_json::to_string(&lifecycle).unwrap();
            assert_eq!(json, format!("\"{lifecycle}\""));
        }
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
        assert_eq!(id(2, 2, 5).cmp(&id(2, 2, 5)), std::cmp::Ordering::Equal);
    }

    #[test]
    fn a_genesis_claim_lists_peer_ids() {
        let claim = GenesisClaim {
            meta_voters: VoterSet::new(ids(&[2, 1])).unwrap(),
            peers: NodeSet::new(ids(&[3, 1, 2, 4])).unwrap(),
            group_count: GroupCount::new(NonZeroU32::new(16).unwrap()),
            replication_factor: ReplicationFactor::new(NonZeroU8::new(3).unwrap()),
        };
        let json = serde_json::to_string(&claim).unwrap();
        assert_eq!(
            json,
            r#"{"meta_voters":[1,2],"peers":[1,2,3,4],"group_count":16,"replication_factor":3}"#
        );
        assert_eq!(serde_json::from_str::<GenesisClaim>(&json).unwrap(), claim);
    }

    fn json_round_trip<T>(value: &T, expected: &str)
    where T: serde::Serialize + serde::de::DeserializeOwned + PartialEq + std::fmt::Debug {
        let json = serde_json::to_string(value).unwrap();
        assert_eq!(json, expected);
        assert_eq!(&serde_json::from_str::<T>(&json).unwrap(), value);
    }

    #[test]
    fn a_reconcile_state_names_its_state_and_reason_beside_their_details() {
        json_round_trip(
            &ReconcileState::Stepping {
                step: StepKind::AddLearners,
            },
            r#"{"state":"stepping","step":"add_learners"}"#,
        );
        json_round_trip(
            &ReconcileState::Waiting(WaitReason::NotLeader),
            r#"{"state":"waiting","reason":"not_leader"}"#,
        );
        json_round_trip(
            &ReconcileState::Waiting(WaitReason::CatchingUp {
                committed: 10,
                behind: vec![
                    LearnerLag {
                        node: NodeId::new(4),
                        matched: Some(9),
                    },
                    LearnerLag {
                        node: NodeId::new(5),
                        matched: None,
                    },
                ],
            }),
            r#"{"state":"waiting","reason":"catching_up","committed":10,"behind":[{"node":4,"matched":9},{"node":5,"matched":null}]}"#,
        );
        json_round_trip(
            &ReconcileState::Waiting(WaitReason::GateClosed {
                nodes: ids(&[6, 2]),
            }),
            r#"{"state":"waiting","reason":"gate_closed","nodes":[2,6]}"#,
        );
        let epoch = |index| {
            PlacementEpoch::new(MetaLogId::new(RaftLogId {
                term: 3,
                leader: NodeId::new(1),
                index,
            }))
        };
        json_round_trip(
            &ReconcileState::StaleView {
                reported: epoch(7),
                current: epoch(8),
            },
            r#"{"state":"stale_view","reported":{"term":3,"leader":1,"index":7},"current":{"term":3,"leader":1,"index":8}}"#,
        );
        json_round_trip(&ReconcileState::Converged, r#"{"state":"converged"}"#);
    }

    #[test]
    fn admin_json_enums_decode_an_unknown_variant() {
        assert_eq!(
            serde_json::from_str::<TeardownStage>(r#""drain""#).unwrap(),
            TeardownStage::Unknown
        );
        assert_eq!(
            serde_json::from_str::<StepKind>(r#""rebalance""#).unwrap(),
            StepKind::Unknown
        );
        assert_eq!(
            serde_json::from_str::<StepFailureKind>(r#""throttled""#).unwrap(),
            StepFailureKind::Unknown
        );
        assert_eq!(
            serde_json::from_str::<WaitReason>(
                r#"{"reason":"snapshot_sending","nodes":[4],"behind":{"4":9}}"#
            )
            .unwrap(),
            WaitReason::Unknown
        );
        // A known state around an unknown reason keeps its state.
        assert_eq!(
            serde_json::from_str::<ReconcileState>(
                r#"{"state":"waiting","reason":"snapshot_sending","nodes":[4]}"#
            )
            .unwrap(),
            ReconcileState::Waiting(WaitReason::Unknown)
        );
        let status: ReconcileStatus = serde_json::from_str(
            r#"{"state":{"state":"throttled","limit":3},"since_ms":5,"progress_ms":6,"last_failure":{"step":"rebalance","kind":"throttled","at_ms":4},"attempts":2,"target_since":null,"view_applied":null}"#,
        )
        .unwrap();
        assert_eq!(status, ReconcileStatus {
            state: ReconcileState::Unknown,
            since_ms: 5,
            progress_ms: 6,
            last_failure: Some(StepFailure {
                step: StepKind::Unknown,
                kind: StepFailureKind::Unknown,
                at_ms: 4,
            }),
            attempts: 2,
            target_since: None,
            view_applied: None,
        });
    }

    /// A root above 2^53, which a JSON number would round.
    const ROOT: u64 = 0x9e37_79b9_7f4a_7c15;

    fn root(id: u64) -> RootId {
        RootId::new(std::num::NonZeroU64::new(id).unwrap())
    }

    #[test]
    fn a_placement_key_parses_what_it_displays() {
        let since = PlacementEpoch::new(MetaLogId::new(RaftLogId {
            term: 3,
            leader: NodeId::new(1),
            index: 57,
        }));
        assert_eq!(since.to_string(), "3-1-57");
        assert_eq!("3-1-57".parse::<PlacementEpoch>(), Ok(since));
        for invalid in [
            "", "3-1", "3-1-57-2", "3--57", "3-1-+57", "-3-1-57", "3-1-x",
        ] {
            assert!(
                matches!(invalid.parse::<PlacementEpoch>(), Err(InvalidRaftLogId { found }) if found == invalid),
                "{invalid:?} parsed"
            );
        }
    }

    #[test]
    fn a_root_id_is_16_lowercase_hex_digits_in_every_encoding() {
        let id = root(ROOT);
        assert_eq!(id.to_string(), "9e3779b97f4a7c15");
        assert_eq!("9e3779b97f4a7c15".parse::<RootId>(), Ok(id));
        assert_eq!("0000000000000001".parse::<RootId>(), Ok(root(1)));
        for invalid in [
            "",
            "0000000000000000",
            "9E3779B97F4A7C15",
            "9e3779b97f4a7c1",
            "9e3779b97f4a7c150",
            "+e3779b97f4a7c15",
            "9e3779b97f4a7c1g",
            "11400714819323198485",
        ] {
            assert!(
                matches!(invalid.parse::<RootId>(), Err(InvalidRootId { found }) if found == invalid),
                "{invalid:?} parsed"
            );
        }
        // Admin JSON writes the string, never a number, and decodes it back
        // exactly, also above 2^53.
        json_round_trip(&id, r#""9e3779b97f4a7c15""#);
        serde_json::from_str::<RootId>(&ROOT.to_string())
            .expect_err("a JSON number is not a root id");
        json_round_trip(
            &NodeRecord {
                lifecycle: NodeLifecycle::Active,
                since_ms: 7,
                root: RootSlot::Released(id),
            },
            r#"{"lifecycle":"active","since_ms":7,"root":{"released":"9e3779b97f4a7c15"}}"#,
        );
        json_round_trip(&RootSlot::Bound(id), r#"{"bound":"9e3779b97f4a7c15"}"#);
    }

    #[test]
    fn a_process_incarnation_writes_its_root_run_and_nonce() {
        let incarnation = ProcessIncarnation {
            root: root(ROOT),
            run: 0x0102_0304,
            nonce: 0xa0b0_c0d0,
        };
        let text = "9e3779b97f4a7c1501020304a0b0c0d0";
        assert_eq!(incarnation.to_string(), text);
        assert_eq!(text.parse::<ProcessIncarnation>(), Ok(incarnation));
        json_round_trip(&incarnation, &format!("\"{text}\""));
        for invalid in [
            "",
            "abcd",
            "0000000000000000000000000000abcd",
            "9E3779B97F4A7C1501020304A0B0C0D0",
            "\"9e3779b97f4a7c1501020304a0b0c0d",
            "g e3779b97f4a7c1501020304a0b0c0d",
            "9e3779b97f4a7c1501020304a0b0c0d0a",
        ] {
            assert!(
                matches!(invalid.parse::<ProcessIncarnation>(), Err(InvalidProcessIncarnation { found }) if found == invalid),
                "{invalid:?} parsed"
            );
        }
    }

    #[test]
    fn relation_compares_the_root_then_the_run_then_the_nonce() {
        let at = |id, run, nonce| ProcessIncarnation {
            root: root(id),
            run,
            nonce,
        };
        let this = at(ROOT, 5, 9);
        assert_eq!(this.relation(&this), Relation::Same);
        assert_eq!(this.relation(&at(ROOT, 4, 9)), Relation::Older);
        assert_eq!(this.relation(&at(ROOT, 6, 9)), Relation::Newer);
        assert_eq!(this.relation(&at(ROOT, 5, 8)), Relation::Duplicate);
        // A nonce never orders: an older run with a greater nonce is older.
        assert_eq!(this.relation(&at(ROOT, 4, u32::MAX)), Relation::Older);
        assert_eq!(this.relation(&at(ROOT, 6, 0)), Relation::Newer);
        // Another root is never older or newer, whatever its run.
        for run in [0, 5, 6, u32::MAX] {
            assert_eq!(this.relation(&at(1, run, 9)), Relation::OtherRoot);
        }
        assert_eq!(at(ROOT, 4, 9).relation(&this), Relation::Newer);
        assert_eq!(at(ROOT, 6, 9).relation(&this), Relation::Older);
    }

    #[test]
    fn an_admin_option_field_is_required_also_when_null() {
        let missing = r#"{"state":{"state":"converged"},"since_ms":5,"progress_ms":5,"last_failure":null,"attempts":2,"target_since":null}"#;
        serde_json::from_str::<ReconcileStatus>(missing)
            .expect_err("a missing `view_applied` does not decode");
        serde_json::from_str::<LearnerLag>(r#"{"node":4}"#)
            .expect_err("a missing `matched` does not decode");
    }
}

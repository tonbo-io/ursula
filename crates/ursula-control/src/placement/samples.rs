//! One sample of every variant of each kernel root, nested variants
//! included, with distinct field values, for the tests here and the
//! MessagePack fixtures of other crates (the `test-support` feature). Each
//! list is built behind an exhaustive match with no wildcard ([`every`]), so
//! a new variant does not compile until it has a sample. The constructors
//! build ids and sets, and `Driver` (tests only) applies commands at
//! increasing meta log ids.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::num::NonZeroU8;
use std::num::NonZeroU32;

use ursula_proto::placement::GenesisClaim;
use ursula_proto::placement::GroupCount;
use ursula_proto::placement::MembershipLogId;
use ursula_proto::placement::MetaLogId;
use ursula_proto::placement::NodeId;
use ursula_proto::placement::NodeLifecycle;
use ursula_proto::placement::NodeRecord;
use ursula_proto::placement::NodeSet;
use ursula_proto::placement::PlacementEpoch;
use ursula_proto::placement::RaftLogId;
use ursula_proto::placement::Registered;
use ursula_proto::placement::ReplicationFactor;
use ursula_proto::placement::VoterSet;
use ursula_shard::RaftGroupId;

use super::BootstrapProblem;
use super::Cluster;
use super::ClusterParams;
use super::ControlApplied;
use super::ControlCommand;
use super::ControlError;
use super::ControlPlaneState;
use super::ControlResponse;
use super::GroupPlacement;
use super::HistoryBegun;
use super::HistoryCause;
use super::LifecycleChange;
use super::Move;
use super::MoveLimit;
use super::Placements;
use super::Registration;
use super::Retirement;
use super::Target;

/// `[value, ...]` for one value per listed pattern, which must cover every
/// variant of `$ty`: the patterns form a match with no wildcard, and each
/// value must match its own pattern.
macro_rules! every {
    ($ty:ty { $($pattern:pat => $value:expr),+ $(,)? }) => {{
        let values: Vec<$ty> = vec![$({
            let value: $ty = $value;
            assert!(matches!(&value, $pattern), "a sample of another variant");
            value
        }),+];
        for value in &values {
            match value {
                $($pattern => {})+
            }
        }
        values
    }};
}

pub fn node(id: u64) -> NodeId {
    NodeId::new(id)
}

/// The voter set of `ids`, which must not be empty.
pub fn voters(ids: &[u64]) -> VoterSet {
    VoterSet::new(ids.iter().copied().map(NodeId::new).collect()).expect("a sample names voters")
}

/// The node set of `ids`, which must not be empty.
pub fn node_set(ids: &[u64]) -> NodeSet {
    NodeSet::new(ids.iter().copied().map(NodeId::new).collect()).expect("a sample names nodes")
}

pub fn ids(values: &[u64]) -> BTreeSet<NodeId> {
    values.iter().copied().map(NodeId::new).collect()
}

/// A meta entry of node 1's leadership in `term`.
pub fn meta(term: u64, index: u64) -> MetaLogId {
    MetaLogId::new(RaftLogId {
        term,
        leader: NodeId::new(1),
        index,
    })
}

/// A data group's membership entry of node 2's leadership in `term`.
pub fn membership(term: u64, index: u64) -> MembershipLogId {
    MembershipLogId::new(RaftLogId {
        term,
        leader: NodeId::new(2),
        index,
    })
}

/// Bootstrap parameters over `nodes`, with their default meta voters.
/// `groups` and `rf` must not be zero.
pub fn params(nodes: &[u64], groups: u32, rf: u8, limit: u32) -> ClusterParams {
    let nodes = node_set(nodes);
    ClusterParams {
        group_count: GroupCount::new(NonZeroU32::new(groups).expect("a sample has groups")),
        replication_factor: ReplicationFactor::new(
            NonZeroU8::new(rf).expect("a sample has voters"),
        ),
        meta_voters: super::default_meta_voters(&nodes),
        max_concurrent_moves: MoveLimit::new(limit),
    }
}

/// Applies commands at increasing meta log ids.
#[cfg(test)]
#[derive(Debug, Clone)]
pub(crate) struct Driver {
    pub(crate) state: ControlPlaneState,
    pub(crate) term: u64,
    pub(crate) index: u64,
}

#[cfg(test)]
impl Driver {
    pub(crate) fn new() -> Self {
        Self {
            state: ControlPlaneState::default(),
            term: 1,
            index: 0,
        }
    }

    /// A cluster of `nodes`, every group seeded and reported, with move
    /// limit 4.
    pub(crate) fn live(nodes: &[u64], groups: u32, rf: u8) -> Self {
        let mut driver = Self::bootstrapped(nodes, params(nodes, groups, rf, 4));
        for group in 0..groups {
            let group = RaftGroupId(group);
            let placement = driver.placement(group).clone();
            driver
                .apply(ControlCommand::ReportConverged {
                    group,
                    since: placement.since(),
                    voters: placement.target_voters().clone(),
                    membership: membership(1, u64::from(group.0)),
                    now_ms: 25,
                })
                .unwrap();
        }
        driver
    }

    /// Registers `nodes` and bootstraps them with `params`.
    pub(crate) fn bootstrapped(nodes: &[u64], params: ClusterParams) -> Self {
        let mut driver = Self::new();
        for id in nodes {
            driver
                .apply(ControlCommand::RegisterNode {
                    node: node(*id),
                    now_ms: 10,
                })
                .unwrap();
        }
        driver
            .apply(ControlCommand::Bootstrap {
                params,
                nodes: node_set(nodes),
                now_ms: 20,
            })
            .unwrap();
        driver
    }

    pub(crate) fn next_at(&mut self) -> MetaLogId {
        self.index = self.index.checked_add(1).unwrap();
        meta(self.term, self.index)
    }

    pub(crate) fn apply(&mut self, command: ControlCommand) -> ControlResponse {
        let at = self.next_at();
        self.state.apply(at, command)
    }

    /// Applies `command` and asserts that the state machine refused it
    /// without changing the state, and that `check` answers the same.
    pub(crate) fn refused(&mut self, command: ControlCommand) -> ControlError {
        let before = self.state.clone();
        let checked = self.state.check(&command);
        let response = self.apply(command);
        assert_eq!(self.state, before, "a refused command changed the state");
        let Err(error) = response else {
            panic!("accepted: {response:?}");
        };
        assert_eq!(checked, Err(error.clone()), "check and apply disagree");
        error
    }

    /// A history event in a new term.
    pub(crate) fn history(&mut self, cause: HistoryCause) -> HistoryBegun {
        self.term = self.term.checked_add(1).unwrap();
        let at = self.next_at();
        self.state.begin_history(at, cause)
    }

    pub(crate) fn cluster(&self) -> &Cluster {
        self.state.cluster().unwrap()
    }

    pub(crate) fn placement(&self, group: RaftGroupId) -> &GroupPlacement {
        self.cluster().group(group).unwrap()
    }

    pub(crate) fn since(&self, group: RaftGroupId) -> PlacementEpoch {
        self.placement(group).since()
    }

    pub(crate) fn request_move(&mut self, group: RaftGroupId, to: &[u64]) -> ControlResponse {
        let expected = self.since(group);
        self.apply(ControlCommand::RequestMove {
            group,
            expected,
            to: voters(to),
            now_ms: 30,
        })
    }

    pub(crate) fn report(&mut self, group: RaftGroupId, reported: &[u64]) -> ControlResponse {
        let since = self.since(group);
        let membership = membership(2, self.index);
        self.apply(ControlCommand::ReportConverged {
            group,
            since,
            voters: voters(reported),
            membership,
            now_ms: 35,
        })
    }

    pub(crate) fn lifecycle(&mut self, id: u64, change: LifecycleChange) -> ControlResponse {
        self.apply(ControlCommand::SetNodeLifecycle {
            node: node(id),
            change,
            now_ms: 40,
        })
    }
}

/// A bootstrapped state that sets every field: a seeded group, a stable
/// group, a moving group with an abandoned target and a retirement, over
/// nodes of every lifecycle. It holds every invariant of the kernel.
///
/// The groups are one per [`GroupPlacement`] and [`Target`] variant, in
/// group order, built behind an exhaustive match ([`every`]): a new variant
/// does not compile until it has a group here.
pub fn sample_state() -> ControlPlaneState {
    let placements = every!(GroupPlacement {
        GroupPlacement::Seeded { .. } => GroupPlacement::Seeded {
            since: PlacementEpoch::new(meta(3, 21)),
            voters: voters(&[1, 2]),
            initializer: node(2),
        },
        GroupPlacement::Live {
            target: Target::Stable { .. },
            ..
        } => GroupPlacement::Live {
            since: PlacementEpoch::new(meta(4, 22)),
            converged_at: membership(5, 23),
            target: Target::Stable {
                voters: voters(&[1, 3]),
            },
        },
        GroupPlacement::Live {
            target: Target::Moving(_),
            ..
        } => GroupPlacement::Live {
            since: PlacementEpoch::new(meta(6, 24)),
            converged_at: membership(7, 25),
            target: Target::Moving(Move {
                from: voters(&[1, 3]),
                to: voters(&[1, 2]),
                abandoned: ids(&[5]),
                since_ms: 26,
            }),
        },
    });
    let groups: BTreeMap<RaftGroupId, GroupPlacement> =
        (0..).map(RaftGroupId).zip(placements).collect();
    let group_count = u32::try_from(groups.len()).expect("a few sample groups");
    ControlPlaneState::Bootstrapped(Cluster {
        params: ClusterParams {
            group_count: GroupCount::new(NonZeroU32::new(group_count).expect("non-zero")),
            replication_factor: ReplicationFactor::new(NonZeroU8::new(2).expect("non-zero")),
            meta_voters: voters(&[1, 2]),
            max_concurrent_moves: MoveLimit::new(5),
        },
        nodes: BTreeMap::from([
            (node(1), NodeRecord {
                lifecycle: NodeLifecycle::Active,
                since_ms: 11,
            }),
            (node(2), NodeRecord {
                lifecycle: NodeLifecycle::Cordoned,
                since_ms: 12,
            }),
            (node(3), NodeRecord {
                lifecycle: NodeLifecycle::Decommissioning,
                since_ms: 13,
            }),
            (node(4), NodeRecord {
                lifecycle: NodeLifecycle::Removed,
                since_ms: 14,
            }),
            (node(5), NodeRecord {
                lifecycle: NodeLifecycle::Active,
                since_ms: 15,
            }),
        ]),
        groups: Placements(groups),
        // Group 1 is the stable group.
        retirements: BTreeMap::from([(
            RaftGroupId(1),
            BTreeMap::from([(node(2), Retirement {
                created_at: meta(8, 27),
            })]),
        )]),
    })
}

/// One state of every variant.
pub fn sample_states() -> Vec<ControlPlaneState> {
    every!(ControlPlaneState {
        ControlPlaneState::Forming { .. } => ControlPlaneState::Forming {
            registered: BTreeMap::from([(node(7), Registration { since_ms: 31 })]),
        },
        ControlPlaneState::FormationCut => ControlPlaneState::FormationCut,
        ControlPlaneState::Bootstrapped(..) => sample_state(),
    })
}

/// One command of every variant, and one lifecycle command per change.
pub fn sample_commands() -> Vec<ControlCommand> {
    every!(ControlCommand {
        ControlCommand::RegisterNode { .. } => ControlCommand::RegisterNode {
            node: node(9),
            now_ms: 41,
        },
        ControlCommand::Bootstrap { .. } => ControlCommand::Bootstrap {
            params: params(&[1, 2, 3], 4, 3, 2),
            nodes: node_set(&[1, 2, 3]),
            now_ms: 42,
        },
        ControlCommand::RequestMove { .. } => ControlCommand::RequestMove {
            group: RaftGroupId(5),
            expected: PlacementEpoch::new(meta(9, 43)),
            to: voters(&[2, 4]),
            now_ms: 44,
        },
        ControlCommand::ReportConverged { .. } => ControlCommand::ReportConverged {
            group: RaftGroupId(6),
            since: PlacementEpoch::new(meta(10, 45)),
            voters: voters(&[3, 5]),
            membership: membership(11, 46),
            now_ms: 50,
        },
        ControlCommand::SetNodeLifecycle {
            change: LifecycleChange::Cordon,
            ..
        } => ControlCommand::SetNodeLifecycle {
            node: node(13),
            change: LifecycleChange::Cordon,
            now_ms: 51,
        },
        ControlCommand::SetNodeLifecycle {
            change: LifecycleChange::Uncordon,
            ..
        } => ControlCommand::SetNodeLifecycle {
            node: node(14),
            change: LifecycleChange::Uncordon,
            now_ms: 52,
        },
        ControlCommand::SetNodeLifecycle {
            change: LifecycleChange::Decommission,
            ..
        } => ControlCommand::SetNodeLifecycle {
            node: node(8),
            change: LifecycleChange::Decommission,
            now_ms: 47,
        },
        ControlCommand::SetMoveLimit { .. } => ControlCommand::SetMoveLimit {
            limit: MoveLimit::new(48),
        },
        ControlCommand::ReportTornDown { .. } => ControlCommand::ReportTornDown {
            group: RaftGroupId(7),
            node: node(6),
            created_at: meta(12, 49),
        },
    })
}

/// One answer of every variant, accepted and refused, and one registration
/// answer per variant of [`Registered`].
pub fn sample_responses() -> Vec<ControlResponse> {
    let applied = every!(ControlApplied {
        ControlApplied::NodeRegistered(Registered::Forming) => {
            ControlApplied::NodeRegistered(Registered::Forming)
        },
        ControlApplied::NodeRegistered(Registered::Referenced(_)) => {
            ControlApplied::NodeRegistered(Registered::Referenced(NodeRecord {
                lifecycle: NodeLifecycle::Cordoned,
                since_ms: 51,
            }))
        },
        ControlApplied::NodeRegistered(Registered::Unreferenced(_)) => {
            ControlApplied::NodeRegistered(Registered::Unreferenced(NodeRecord {
                lifecycle: NodeLifecycle::Active,
                since_ms: 52,
            }))
        },
        ControlApplied::Bootstrapped => ControlApplied::Bootstrapped,
        ControlApplied::MoveAccepted { .. } => ControlApplied::MoveAccepted {
            since: PlacementEpoch::new(meta(13, 53)),
        },
        ControlApplied::Converged { .. } => ControlApplied::Converged {
            since: PlacementEpoch::new(meta(14, 54)),
            retired: ids(&[3]),
            removed: ids(&[4]),
        },
        ControlApplied::LifecycleChanged { .. } => ControlApplied::LifecycleChanged {
            lifecycle: NodeLifecycle::Removed,
        },
        ControlApplied::MoveLimitSet { .. } => ControlApplied::MoveLimitSet {
            limit: MoveLimit::new(55),
        },
        ControlApplied::TornDown => ControlApplied::TornDown,
        ControlApplied::NothingToTearDown => ControlApplied::NothingToTearDown,
    });
    applied
        .into_iter()
        .map(Ok)
        .chain(sample_errors().into_iter().map(Err))
        .collect()
}

/// One error of every variant, and one invalid bootstrap per problem.
pub fn sample_errors() -> Vec<ControlError> {
    let mut errors = every!(ControlError {
        ControlError::NotBootstrapped => ControlError::NotBootstrapped,
        ControlError::AlreadyBootstrapped => ControlError::AlreadyBootstrapped,
        ControlError::FormationCut => ControlError::FormationCut,
        ControlError::InvalidBootstrap { .. } => ControlError::InvalidBootstrap {
            problem: BootstrapProblem::MetaVoterNotANode { node: node(10) },
        },
        ControlError::UnknownGroup { .. } => ControlError::UnknownGroup {
            group: RaftGroupId(8),
        },
        ControlError::UnknownNode { .. } => ControlError::UnknownNode { node: node(11) },
        ControlError::NodeRemoved { .. } => ControlError::NodeRemoved { node: node(12) },
        ControlError::StalePlacement { .. } => ControlError::StalePlacement {
            group: RaftGroupId(9),
            current: Box::new(GroupPlacement::Seeded {
                since: PlacementEpoch::new(meta(15, 56)),
                voters: voters(&[13]),
                initializer: node(13),
            }),
        },
        ControlError::GroupNotLive { .. } => ControlError::GroupNotLive {
            group: RaftGroupId(10),
        },
        ControlError::NotMoving { .. } => ControlError::NotMoving {
            group: RaftGroupId(11),
        },
        ControlError::NoChange { .. } => ControlError::NoChange {
            group: RaftGroupId(12),
        },
        ControlError::ReplicationFactorChange { .. } => ControlError::ReplicationFactorChange {
            group: RaftGroupId(13),
            current: 3,
            requested: 4,
        },
        ControlError::IneligibleTarget { .. } => ControlError::IneligibleTarget {
            group: RaftGroupId(14),
            node: node(24),
            lifecycle: NodeLifecycle::Decommissioning,
        },
        ControlError::RetirementPending { .. } => ControlError::RetirementPending {
            group: RaftGroupId(15),
            node: node(25),
        },
        ControlError::MoveLimit { .. } => ControlError::MoveLimit {
            limit: 57,
            moving: 58,
        },
        ControlError::NotTarget { .. } => ControlError::NotTarget {
            group: RaftGroupId(16),
            reported: voters(&[16, 17]),
        },
        ControlError::MetaVoter { .. } => ControlError::MetaVoter { node: node(18) },
        ControlError::Decommissioning { .. } => ControlError::Decommissioning { node: node(19) },
    });
    errors.extend(
        every!(BootstrapProblem {
            BootstrapProblem::GroupCountAboveMax { .. } => BootstrapProblem::GroupCountAboveMax {
                count: GroupCount::new(NonZeroU32::new(5_000).expect("non-zero")),
            },
            BootstrapProblem::MetaVoterNotANode { .. } => BootstrapProblem::MetaVoterNotANode {
                node: node(20),
            },
            BootstrapProblem::ReplicationFactorAboveNodes => {
                BootstrapProblem::ReplicationFactorAboveNodes
            },
        })
        .into_iter()
        .map(|problem| ControlError::InvalidBootstrap { problem }),
    );
    errors
}

/// A genesis claim whose sets and counts differ from each other's.
pub fn sample_genesis_claim() -> GenesisClaim {
    GenesisClaim {
        meta_voters: voters(&[31, 32, 33]),
        peers: node_set(&[31, 32, 33, 34, 35]),
        group_count: GroupCount::new(NonZeroU32::new(16).expect("non-zero")),
        replication_factor: ReplicationFactor::new(NonZeroU8::new(4).expect("non-zero")),
    }
}

/// One cause of every variant.
pub fn sample_history_causes() -> Vec<HistoryCause> {
    every!(HistoryCause {
        HistoryCause::Genesis => HistoryCause::Genesis,
        HistoryCause::Plan => HistoryCause::Plan,
    })
}

/// One history answer of every variant.
pub fn sample_history_answers() -> Vec<HistoryBegun> {
    every!(HistoryBegun {
        HistoryBegun::Forming => HistoryBegun::Forming,
        HistoryBegun::FormationCut => HistoryBegun::FormationCut,
        HistoryBegun::Rebased { .. } => HistoryBegun::Rebased {
            dropped: BTreeMap::from([(RaftGroupId(17), ids(&[20, 21]))]),
        },
    })
}

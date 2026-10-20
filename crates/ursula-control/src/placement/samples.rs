//! Test helpers: ids, a driver that applies commands at increasing meta log
//! ids, and one sample of every variant of each replicated root, with
//! distinct field values.

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
use super::MaintenanceAuthority;
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

pub(super) fn node(id: u64) -> NodeId {
    NodeId::new(id)
}

pub(super) fn voters(ids: &[u64]) -> VoterSet {
    VoterSet::new(ids.iter().copied().map(NodeId::new).collect()).unwrap()
}

pub(super) fn node_set(ids: &[u64]) -> NodeSet {
    NodeSet::new(ids.iter().copied().map(NodeId::new).collect()).unwrap()
}

pub(super) fn ids(values: &[u64]) -> BTreeSet<NodeId> {
    values.iter().copied().map(NodeId::new).collect()
}

pub(super) fn meta(term: u64, index: u64) -> MetaLogId {
    MetaLogId::new(RaftLogId {
        term,
        leader: NodeId::new(1),
        index,
    })
}

pub(super) fn membership(term: u64, index: u64) -> MembershipLogId {
    MembershipLogId::new(RaftLogId {
        term,
        leader: NodeId::new(2),
        index,
    })
}

pub(super) fn params(
    nodes: &[u64],
    groups: u32,
    rf: u8,
    limit: u32,
    maintenance: MaintenanceAuthority,
) -> ClusterParams {
    let nodes = node_set(nodes);
    ClusterParams {
        group_count: GroupCount::new(NonZeroU32::new(groups).unwrap()),
        replication_factor: ReplicationFactor::new(NonZeroU8::new(rf).unwrap()),
        meta_voters: super::default_meta_voters(&nodes),
        max_concurrent_moves: MoveLimit::new(limit),
        maintenance,
    }
}

/// Applies commands at increasing meta log ids.
#[derive(Debug, Clone)]
pub(super) struct Driver {
    pub(super) state: ControlPlaneState,
    pub(super) term: u64,
    pub(super) index: u64,
}

impl Driver {
    pub(super) fn new() -> Self {
        Self {
            state: ControlPlaneState::default(),
            term: 1,
            index: 0,
        }
    }

    /// A cluster of `nodes`, every group seeded and reported, with move
    /// limit 4.
    pub(super) fn live(nodes: &[u64], groups: u32, rf: u8) -> Self {
        let mut driver = Self::bootstrapped(
            nodes,
            params(nodes, groups, rf, 4, MaintenanceAuthority::Placement),
        );
        for group in 0..groups {
            let group = RaftGroupId(group);
            let placement = driver.placement(group).clone();
            driver
                .apply(ControlCommand::ReportConverged {
                    group,
                    since: placement.since(),
                    voters: placement.target_voters().clone(),
                    membership: membership(1, u64::from(group.0)),
                })
                .unwrap();
        }
        driver
    }

    /// Registers `nodes` and bootstraps them with `params`.
    pub(super) fn bootstrapped(nodes: &[u64], params: ClusterParams) -> Self {
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

    pub(super) fn next_at(&mut self) -> MetaLogId {
        self.index = self.index.checked_add(1).unwrap();
        meta(self.term, self.index)
    }

    pub(super) fn apply(&mut self, command: ControlCommand) -> ControlResponse {
        let at = self.next_at();
        self.state.apply(at, command)
    }

    /// Applies `command` and asserts that the state machine refused it
    /// without changing the state, and that `check` answers the same.
    pub(super) fn refused(&mut self, command: ControlCommand) -> ControlError {
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
    pub(super) fn history(&mut self, cause: HistoryCause) -> HistoryBegun {
        self.term = self.term.checked_add(1).unwrap();
        let at = self.next_at();
        self.state.begin_history(at, cause)
    }

    pub(super) fn cluster(&self) -> &Cluster {
        self.state.cluster().unwrap()
    }

    pub(super) fn placement(&self, group: RaftGroupId) -> &GroupPlacement {
        self.cluster().group(group).unwrap()
    }

    pub(super) fn since(&self, group: RaftGroupId) -> PlacementEpoch {
        self.placement(group).since()
    }

    pub(super) fn request_move(&mut self, group: RaftGroupId, to: &[u64]) -> ControlResponse {
        let expected = self.since(group);
        self.apply(ControlCommand::RequestMove {
            group,
            expected,
            to: voters(to),
            now_ms: 30,
        })
    }

    pub(super) fn report(&mut self, group: RaftGroupId, reported: &[u64]) -> ControlResponse {
        let since = self.since(group);
        let membership = membership(2, self.index);
        self.apply(ControlCommand::ReportConverged {
            group,
            since,
            voters: voters(reported),
            membership,
        })
    }

    pub(super) fn lifecycle(&mut self, id: u64, change: LifecycleChange) -> ControlResponse {
        self.apply(ControlCommand::SetNodeLifecycle {
            node: node(id),
            change,
            now_ms: 40,
        })
    }
}

/// A bootstrapped state that sets every field: a seeded group, a stable
/// group, a moving group and a retirement.
pub(super) fn sample_state() -> ControlPlaneState {
    ControlPlaneState::Bootstrapped(Cluster {
        params: ClusterParams {
            group_count: GroupCount::new(NonZeroU32::new(3).unwrap()),
            replication_factor: ReplicationFactor::new(NonZeroU8::new(2).unwrap()),
            meta_voters: voters(&[1, 2]),
            max_concurrent_moves: MoveLimit::new(5),
            maintenance: MaintenanceAuthority::Placement,
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
        ]),
        groups: Placements(BTreeMap::from([
            (RaftGroupId(0), GroupPlacement::Seeded {
                since: PlacementEpoch::new(meta(3, 21)),
                voters: voters(&[1, 2]),
                initializer: node(2),
            }),
            (RaftGroupId(1), GroupPlacement::Live {
                since: PlacementEpoch::new(meta(4, 22)),
                converged_at: membership(5, 23),
                target: Target::Stable {
                    voters: voters(&[1, 3]),
                },
            }),
            (RaftGroupId(2), GroupPlacement::Live {
                since: PlacementEpoch::new(meta(6, 24)),
                converged_at: membership(7, 25),
                target: Target::Moving(Move {
                    from: voters(&[1, 3]),
                    to: voters(&[1, 2]),
                    abandoned: ids(&[5]),
                    since_ms: 26,
                }),
            }),
        ])),
        retirements: BTreeMap::from([(
            RaftGroupId(1),
            BTreeMap::from([(node(2), Retirement {
                created_at: meta(8, 27),
            })]),
        )]),
    })
}

/// One state of every variant.
pub(super) fn sample_states() -> Vec<ControlPlaneState> {
    vec![
        ControlPlaneState::Forming {
            registered: BTreeMap::from([(node(7), Registration { since_ms: 31 })]),
        },
        ControlPlaneState::FormationCut,
        sample_state(),
    ]
}

/// One command of every variant.
pub(super) fn sample_commands() -> Vec<ControlCommand> {
    every!(ControlCommand {
        ControlCommand::RegisterNode { .. } => ControlCommand::RegisterNode {
            node: node(9),
            now_ms: 41,
        },
        ControlCommand::Bootstrap { .. } => ControlCommand::Bootstrap {
            params: params(&[1, 2, 3], 4, 3, 2, MaintenanceAuthority::Reservation),
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
        },
        ControlCommand::SetNodeLifecycle { .. } => ControlCommand::SetNodeLifecycle {
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

/// One answer of every variant, accepted and refused.
pub(super) fn sample_responses() -> Vec<ControlResponse> {
    let record = NodeRecord {
        lifecycle: NodeLifecycle::Cordoned,
        since_ms: 51,
    };
    let applied = vec![
        ControlApplied::NodeRegistered(Registered::Forming),
        ControlApplied::NodeRegistered(Registered::Referenced(record)),
        ControlApplied::NodeRegistered(Registered::Unreferenced(NodeRecord {
            lifecycle: NodeLifecycle::Active,
            since_ms: 52,
        })),
        ControlApplied::Bootstrapped,
        ControlApplied::MoveAccepted {
            since: PlacementEpoch::new(meta(13, 53)),
        },
        ControlApplied::Converged {
            since: PlacementEpoch::new(meta(14, 54)),
            retired: ids(&[3]),
            removed: ids(&[4]),
        },
        ControlApplied::LifecycleChanged {
            lifecycle: NodeLifecycle::Removed,
        },
        ControlApplied::MoveLimitSet {
            limit: MoveLimit::new(55),
        },
        ControlApplied::TornDown,
        ControlApplied::NothingToTearDown,
    ];
    let errors = sample_errors();
    applied
        .into_iter()
        .map(Ok)
        .chain(errors.into_iter().map(Err))
        .collect()
}

/// One error of every variant.
pub(super) fn sample_errors() -> Vec<ControlError> {
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
            node: node(14),
            lifecycle: NodeLifecycle::Decommissioning,
        },
        ControlError::RetirementPending { .. } => ControlError::RetirementPending {
            group: RaftGroupId(15),
            node: node(15),
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
        ControlError::ReservationAuthority => ControlError::ReservationAuthority,
        ControlError::Decommissioning { .. } => ControlError::Decommissioning { node: node(19) },
    });
    errors.extend(
        every!(BootstrapProblem {
            BootstrapProblem::MetaVoterNotANode { .. } => BootstrapProblem::MetaVoterNotANode {
                node: node(20),
            },
            BootstrapProblem::ReplicationFactorAboveNodes => {
                BootstrapProblem::ReplicationFactorAboveNodes
            },
            BootstrapProblem::ReservationTopology => BootstrapProblem::ReservationTopology,
        })
        .into_iter()
        .map(|problem| ControlError::InvalidBootstrap { problem }),
    );
    errors
}

/// A genesis claim whose sets and counts differ from each other's.
pub(super) fn sample_genesis_claim() -> GenesisClaim {
    GenesisClaim {
        meta_voters: voters(&[31, 32, 33]),
        peers: node_set(&[31, 32, 33, 34, 35]),
        group_count: GroupCount::new(NonZeroU32::new(16).unwrap()),
        replication_factor: ReplicationFactor::new(NonZeroU8::new(4).unwrap()),
    }
}

/// One history answer of every variant.
pub(super) fn sample_history_answers() -> Vec<HistoryBegun> {
    vec![
        HistoryBegun::Forming,
        HistoryBegun::FormationCut,
        HistoryBegun::Rebased {
            dropped: BTreeMap::from([(RaftGroupId(17), ids(&[20, 21]))]),
        },
    ]
}

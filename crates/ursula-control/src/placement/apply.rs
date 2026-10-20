//! The dispatcher: one method per command, each validating completely
//! before it mutates.
//!
//! Every command is first planned against the unchanged state, which either
//! refuses it or yields a `Change` that names everything the command will
//! write. Only an accepted plan is committed, and committing cannot fail, so
//! a refused command leaves the state exactly as it was. [`ControlPlaneState::check`]
//! runs the planning step alone, which is also `ursulactl`'s pre-check.
//! Decisions read no clock: `at` is the applied entry's log id and `now_ms`
//! is display only.

use std::collections::BTreeMap;
use std::collections::BTreeSet;

use ursula_proto::placement::MembershipLogId;
use ursula_proto::placement::MetaLogId;
use ursula_proto::placement::NodeId;
use ursula_proto::placement::NodeLifecycle;
use ursula_proto::placement::NodeRecord;
use ursula_proto::placement::NodeSet;
use ursula_proto::placement::PlacementEpoch;
use ursula_proto::placement::Registered;
use ursula_proto::placement::VoterSet;
use ursula_shard::RaftGroupId;

use super::command::BootstrapProblem;
use super::command::ControlApplied;
use super::command::ControlCommand;
use super::command::ControlError;
use super::command::ControlResponse;
use super::command::HistoryBegun;
use super::command::HistoryCause;
use super::command::LifecycleChange;
use super::seed::initializer;
use super::seed::seed_voters;
use super::state::Cluster;
use super::state::ClusterParams;
use super::state::ControlPlaneState;
use super::state::GroupPlacement;
use super::state::MaintenanceAuthority;
use super::state::Move;
use super::state::MoveLimit;
use super::state::Placements;
use super::state::Registration;
use super::state::Retirement;
use super::state::Target;
use super::state::group_ids;

/// The node ids, meta voters and replication factor a cluster under the
/// startup reservation must have.
const RESERVATION_NODES: [u64; 3] = [1, 2, 3];
const RESERVATION_REPLICATION_FACTOR: u8 = 3;

impl ControlPlaneState {
    /// Applies `command` as the meta entry `at`: checks it, then mutates.
    /// A refused command changes nothing.
    pub fn apply(&mut self, at: MetaLogId, command: ControlCommand) -> ControlResponse {
        match self {
            Self::FormationCut => Err(ControlError::FormationCut),
            Self::Forming { registered } => match plan_forming(registered, command)? {
                FormingChange::Nothing(applied) => Ok(applied),
                FormingChange::Register { node, now_ms } => {
                    registered.insert(node, Registration { since_ms: now_ms });
                    Ok(ControlApplied::NodeRegistered(Registered::Forming))
                }
                FormingChange::Bootstrap {
                    params,
                    nodes,
                    now_ms,
                } => {
                    let cluster = bootstrap(at, registered, params, &nodes, now_ms);
                    *self = Self::Bootstrapped(cluster);
                    Ok(ControlApplied::Bootstrapped)
                }
            },
            Self::Bootstrapped(cluster) => {
                let change = cluster.plan(command)?;
                Ok(cluster.commit(at, change))
            }
        }
    }

    /// Whether [`Self::apply`] would accept `command`, with the error it
    /// would answer.
    pub fn check(&self, command: &ControlCommand) -> Result<(), ControlError> {
        let command = command.clone();
        match self {
            Self::FormationCut => Err(ControlError::FormationCut),
            Self::Forming { registered } => plan_forming(registered, command).map(|_change| ()),
            Self::Bootstrapped(cluster) => cluster.plan(command).map(|_change| ()),
        }
    }

    /// Applied with a meta history event at `at`: drops every retirement,
    /// moves every placement's key to `at`, and on a plan's event ends a
    /// formation. Nothing else changes, so nothing computed before the
    /// event applies after it.
    pub fn begin_history(&mut self, at: MetaLogId, cause: HistoryCause) -> HistoryBegun {
        match self {
            Self::Forming { .. } => match cause {
                HistoryCause::Genesis => HistoryBegun::Forming,
                HistoryCause::Plan => {
                    *self = Self::FormationCut;
                    HistoryBegun::FormationCut
                }
            },
            Self::FormationCut => HistoryBegun::FormationCut,
            Self::Bootstrapped(cluster) => {
                let since = PlacementEpoch::new(at);
                for placement in cluster.groups.0.values_mut() {
                    match placement {
                        GroupPlacement::Seeded { since: key, .. }
                        | GroupPlacement::Live { since: key, .. } => *key = since,
                    }
                }
                let dropped = std::mem::take(&mut cluster.retirements)
                    .into_iter()
                    .map(|(group, records)| (group, records.into_keys().collect()))
                    .collect();
                HistoryBegun::Rebased { dropped }
            }
        }
    }
}

/// What an accepted command writes before Bootstrap.
#[derive(Debug)]
enum FormingChange {
    /// The command is accepted and writes nothing.
    Nothing(ControlApplied),
    Register {
        node: NodeId,
        now_ms: u64,
    },
    Bootstrap {
        params: ClusterParams,
        nodes: NodeSet,
        now_ms: u64,
    },
}

/// Everything an accepted command writes to a cluster, computed before
/// anything is.
#[derive(Debug)]
enum Change {
    /// The command is accepted and writes nothing.
    Nothing(ControlApplied),
    /// A new node registers.
    Register {
        node: NodeId,
        now_ms: u64,
    },
    /// The group starts, retargets or cancels a move.
    Move {
        group: RaftGroupId,
        converged_at: MembershipLogId,
        moving: Move,
    },
    /// The group becomes stable on `voters`, retiring `retired`.
    Converge {
        group: RaftGroupId,
        converged_at: MembershipLogId,
        voters: VoterSet,
        retired: BTreeSet<NodeId>,
    },
    Lifecycle {
        node: NodeId,
        record: NodeRecord,
    },
    MoveLimit(MoveLimit),
    TornDown {
        group: RaftGroupId,
        node: NodeId,
    },
}

fn plan_forming(
    registered: &BTreeMap<NodeId, Registration>,
    command: ControlCommand,
) -> Result<FormingChange, ControlError> {
    match command {
        ControlCommand::RegisterNode { node, now_ms } => Ok(if registered.contains_key(&node) {
            FormingChange::Nothing(ControlApplied::NodeRegistered(Registered::Forming))
        } else {
            FormingChange::Register { node, now_ms }
        }),
        ControlCommand::Bootstrap {
            params,
            nodes,
            now_ms,
        } => {
            check_bootstrap(registered, &params, &nodes)?;
            Ok(FormingChange::Bootstrap {
                params,
                nodes,
                now_ms,
            })
        }
        ControlCommand::RequestMove { .. }
        | ControlCommand::ReportConverged { .. }
        | ControlCommand::SetNodeLifecycle { .. }
        | ControlCommand::SetMoveLimit { .. }
        | ControlCommand::ReportTornDown { .. } => Err(ControlError::NotBootstrapped),
    }
}

/// The cluster a checked `Bootstrap` at `at` creates: every registered node
/// active, every group seeded over `nodes`.
fn bootstrap(
    at: MetaLogId,
    registered: &BTreeMap<NodeId, Registration>,
    params: ClusterParams,
    nodes: &NodeSet,
    now_ms: u64,
) -> Cluster {
    let record = NodeRecord {
        lifecycle: NodeLifecycle::Active,
        since_ms: now_ms,
    };
    let since = PlacementEpoch::new(at);
    let groups = group_ids(params.group_count)
        .map(|group| {
            let placement = GroupPlacement::Seeded {
                since,
                voters: seed_voters(group, params.replication_factor, nodes),
                initializer: initializer(group, nodes),
            };
            (group, placement)
        })
        .collect();
    Cluster {
        nodes: registered.keys().map(|node| (*node, record)).collect(),
        params,
        groups: Placements(groups),
        retirements: BTreeMap::new(),
    }
}

fn check_bootstrap(
    registered: &BTreeMap<NodeId, Registration>,
    params: &ClusterParams,
    nodes: &NodeSet,
) -> Result<(), ControlError> {
    if let Some(node) = nodes.iter().find(|node| !registered.contains_key(node)) {
        return Err(ControlError::UnknownNode { node });
    }
    let invalid = |problem| Err(ControlError::InvalidBootstrap { problem });
    if let Some(node) = params
        .meta_voters
        .iter()
        .find(|node| !nodes.contains(*node))
    {
        return invalid(BootstrapProblem::MetaVoterNotANode { node });
    }
    if usize::from(params.replication_factor.get()) > nodes.len() {
        return invalid(BootstrapProblem::ReplicationFactorAboveNodes);
    }
    if params.maintenance == MaintenanceAuthority::Reservation {
        let reserved: BTreeSet<NodeId> = RESERVATION_NODES.map(NodeId::new).into();
        if nodes.to_set() != reserved
            || params.meta_voters.to_set() != reserved
            || params.replication_factor.get() != RESERVATION_REPLICATION_FACTOR
        {
            return invalid(BootstrapProblem::ReservationTopology);
        }
    }
    Ok(())
}

impl Cluster {
    fn plan(&self, command: ControlCommand) -> Result<Change, ControlError> {
        if let Some(group) = command.group()
            && self.group(group).is_none()
        {
            return Err(ControlError::UnknownGroup { group });
        }
        match command {
            ControlCommand::RegisterNode { node, now_ms } => self.plan_register(node, now_ms),
            ControlCommand::Bootstrap { .. } => Err(ControlError::AlreadyBootstrapped),
            ControlCommand::RequestMove {
                group,
                expected,
                to,
                now_ms,
            } => self.plan_move(group, expected, to, now_ms),
            ControlCommand::ReportConverged {
                group,
                since,
                voters,
                membership,
            } => self.plan_converge(group, since, voters, membership),
            ControlCommand::SetNodeLifecycle {
                node,
                change,
                now_ms,
            } => self.plan_lifecycle(node, change, now_ms),
            ControlCommand::SetMoveLimit { limit } => Ok(Change::MoveLimit(limit)),
            ControlCommand::ReportTornDown {
                group,
                node,
                created_at,
            } => self.plan_torn_down(group, node, created_at),
        }
    }

    fn plan_register(&self, node: NodeId, now_ms: u64) -> Result<Change, ControlError> {
        let Some(record) = self.node(node) else {
            return Ok(Change::Register { node, now_ms });
        };
        if record.lifecycle == NodeLifecycle::Removed {
            return Err(ControlError::NodeRemoved { node });
        }
        Ok(Change::Nothing(ControlApplied::NodeRegistered(
            self.registered(node, *record),
        )))
    }

    fn registered(&self, node: NodeId, record: NodeRecord) -> Registered {
        if self.has_any_role(node) {
            Registered::Referenced(record)
        } else {
            Registered::Unreferenced(record)
        }
    }

    fn plan_move(
        &self,
        group: RaftGroupId,
        expected: PlacementEpoch,
        to: VoterSet,
        now_ms: u64,
    ) -> Result<Change, ControlError> {
        if self.params.maintenance == MaintenanceAuthority::Reservation {
            return Err(ControlError::ReservationAuthority);
        }
        let placement = self.existing(group)?;
        let GroupPlacement::Live {
            since,
            converged_at,
            target,
        } = placement
        else {
            return Err(ControlError::GroupNotLive { group });
        };
        if *since != expected {
            return Err(stale(group, placement));
        }
        let current = placement.target_voters();
        if to.len() != current.len() {
            return Err(ControlError::ReplicationFactorChange {
                group,
                current: voter_count(current),
                requested: voter_count(&to),
            });
        }
        let placed_now = placement.current_voters();
        for node in to.iter() {
            self.check_target(group, node, placed_now)?;
        }
        let moving = match target {
            Target::Stable { voters } => {
                if to == *voters {
                    return Err(ControlError::NoChange { group });
                }
                let moving = self.moving();
                let limit = self.params.max_concurrent_moves.get();
                if moving >= limit {
                    return Err(ControlError::MoveLimit { limit, moving });
                }
                Move {
                    from: voters.clone(),
                    to,
                    abandoned: BTreeSet::new(),
                    since_ms: now_ms,
                }
            }
            Target::Moving(moving) => {
                if to == moving.to {
                    return Err(ControlError::NoChange { group });
                }
                let mut abandoned = moving.abandoned.clone();
                abandoned.extend(moving.to.iter());
                abandoned.retain(|node| !moving.from.contains(*node) && !to.contains(*node));
                Move {
                    from: moving.from.clone(),
                    to,
                    abandoned,
                    since_ms: moving.since_ms,
                }
            }
        };
        Ok(Change::Move {
            group,
            converged_at: *converged_at,
            moving,
        })
    }

    /// Whether `node` may be in a move target of `group` whose voters are
    /// now `placed_now`: registered, not removed, active unless it is one of
    /// them, and without a retirement for the group.
    fn check_target(
        &self,
        group: RaftGroupId,
        node: NodeId,
        placed_now: &VoterSet,
    ) -> Result<(), ControlError> {
        let record = self.node(node).ok_or(ControlError::UnknownNode { node })?;
        match record.lifecycle {
            NodeLifecycle::Removed => return Err(ControlError::NodeRemoved { node }),
            NodeLifecycle::Active => {}
            NodeLifecycle::Cordoned | NodeLifecycle::Decommissioning => {
                if !placed_now.contains(node) {
                    return Err(ControlError::IneligibleTarget {
                        group,
                        node,
                        lifecycle: record.lifecycle,
                    });
                }
            }
        }
        if self.retirement(group, node).is_some() {
            return Err(ControlError::RetirementPending { group, node });
        }
        Ok(())
    }

    fn plan_converge(
        &self,
        group: RaftGroupId,
        since: PlacementEpoch,
        voters: VoterSet,
        membership: MembershipLogId,
    ) -> Result<Change, ControlError> {
        let placement = self.existing(group)?;
        if placement.since() != since {
            return Err(stale(group, placement));
        }
        let retired = match placement {
            GroupPlacement::Seeded { .. } => {
                for node in voters.iter() {
                    let record = self.node(node).ok_or(ControlError::UnknownNode { node })?;
                    if record.lifecycle == NodeLifecycle::Removed {
                        return Err(ControlError::NodeRemoved { node });
                    }
                }
                if voters.len() != usize::from(self.params.replication_factor.get()) {
                    return Err(ControlError::NotTarget {
                        group,
                        reported: voters,
                    });
                }
                BTreeSet::new()
            }
            GroupPlacement::Live {
                target: Target::Stable { .. },
                ..
            } => return Err(ControlError::NotMoving { group }),
            GroupPlacement::Live {
                target: Target::Moving(moving),
                ..
            } => {
                if voters != moving.to {
                    return Err(ControlError::NotTarget {
                        group,
                        reported: voters,
                    });
                }
                moving
                    .from
                    .iter()
                    .chain(moving.abandoned.iter().copied())
                    .filter(|node| !moving.to.contains(*node))
                    .collect()
            }
        };
        Ok(Change::Converge {
            group,
            converged_at: membership,
            voters,
            retired,
        })
    }

    fn plan_lifecycle(
        &self,
        node: NodeId,
        change: LifecycleChange,
        now_ms: u64,
    ) -> Result<Change, ControlError> {
        let record = *self.node(node).ok_or(ControlError::UnknownNode { node })?;
        let lifecycle = match (record.lifecycle, change) {
            (NodeLifecycle::Removed, _) => return Err(ControlError::NodeRemoved { node }),
            (_, LifecycleChange::Decommission)
                if self.params.maintenance == MaintenanceAuthority::Reservation =>
            {
                return Err(ControlError::ReservationAuthority);
            }
            (_, LifecycleChange::Decommission) if self.params.meta_voters.contains(node) => {
                return Err(ControlError::MetaVoter { node });
            }
            (NodeLifecycle::Decommissioning, LifecycleChange::Cordon) => {
                return Err(ControlError::Decommissioning { node });
            }
            (NodeLifecycle::Active | NodeLifecycle::Cordoned, LifecycleChange::Cordon) => {
                NodeLifecycle::Cordoned
            }
            (_, LifecycleChange::Uncordon) => NodeLifecycle::Active,
            (_, LifecycleChange::Decommission) => NodeLifecycle::Decommissioning,
        };
        if lifecycle == record.lifecycle {
            return Ok(Change::Nothing(ControlApplied::LifecycleChanged {
                lifecycle,
            }));
        }
        Ok(Change::Lifecycle {
            node,
            record: NodeRecord {
                lifecycle,
                since_ms: now_ms,
            },
        })
    }

    fn plan_torn_down(
        &self,
        group: RaftGroupId,
        node: NodeId,
        created_at: MetaLogId,
    ) -> Result<Change, ControlError> {
        let record = self.node(node).ok_or(ControlError::UnknownNode { node })?;
        if record.lifecycle == NodeLifecycle::Removed {
            return Err(ControlError::NodeRemoved { node });
        }
        Ok(match self.retirement(group, node) {
            Some(retirement) if retirement.created_at == created_at => {
                Change::TornDown { group, node }
            }
            Some(_) | None => Change::Nothing(ControlApplied::NothingToTearDown),
        })
    }

    fn existing(&self, group: RaftGroupId) -> Result<&GroupPlacement, ControlError> {
        self.group(group)
            .ok_or(ControlError::UnknownGroup { group })
    }

    fn commit(&mut self, at: MetaLogId, change: Change) -> ControlApplied {
        let since = PlacementEpoch::new(at);
        match change {
            Change::Nothing(applied) => applied,
            Change::Register { node, now_ms } => {
                let record = NodeRecord {
                    lifecycle: NodeLifecycle::Active,
                    since_ms: now_ms,
                };
                self.nodes.insert(node, record);
                ControlApplied::NodeRegistered(Registered::Unreferenced(record))
            }
            Change::Move {
                group,
                converged_at,
                moving,
            } => {
                let placement = GroupPlacement::Live {
                    since,
                    converged_at,
                    target: Target::Moving(moving),
                };
                self.groups.0.insert(group, placement);
                ControlApplied::MoveAccepted { since }
            }
            Change::Converge {
                group,
                converged_at,
                voters,
                retired,
            } => {
                let placement = GroupPlacement::Live {
                    since,
                    converged_at,
                    target: Target::Stable { voters },
                };
                self.groups.0.insert(group, placement);
                if !retired.is_empty() {
                    let records = self.retirements.entry(group).or_default();
                    for node in &retired {
                        records.insert(*node, Retirement { created_at: at });
                    }
                }
                let removed = self.remove_roleless_decommissioning();
                let retired = retired.difference(&removed).copied().collect();
                ControlApplied::Converged {
                    since,
                    retired,
                    removed,
                }
            }
            Change::Lifecycle { node, record } => {
                self.nodes.insert(node, record);
                let removed = self.remove_roleless_decommissioning();
                let lifecycle = if removed.contains(&node) {
                    NodeLifecycle::Removed
                } else {
                    record.lifecycle
                };
                ControlApplied::LifecycleChanged { lifecycle }
            }
            Change::MoveLimit(limit) => {
                self.params.max_concurrent_moves = limit;
                ControlApplied::MoveLimitSet { limit }
            }
            Change::TornDown { group, node } => {
                if let Some(records) = self.retirements.get_mut(&group) {
                    records.remove(&node);
                    if records.is_empty() {
                        self.retirements.remove(&group);
                    }
                }
                ControlApplied::TornDown
            }
        }
    }

    /// Removes every decommissioning node left without a role, with its
    /// retirements. Removal destroys nothing: the node's replicas close as
    /// their groups answer.
    fn remove_roleless_decommissioning(&mut self) -> BTreeSet<NodeId> {
        let removed: BTreeSet<NodeId> = self
            .nodes
            .iter()
            .filter(|(node, record)| {
                record.lifecycle == NodeLifecycle::Decommissioning && !self.has_any_role(**node)
            })
            .map(|(node, _)| *node)
            .collect();
        for node in &removed {
            if let Some(record) = self.nodes.get_mut(node) {
                record.lifecycle = NodeLifecycle::Removed;
            }
        }
        if !removed.is_empty() {
            for records in self.retirements.values_mut() {
                records.retain(|node, _| !removed.contains(node));
            }
            self.retirements.retain(|_, records| !records.is_empty());
        }
        removed
    }
}

fn stale(group: RaftGroupId, placement: &GroupPlacement) -> ControlError {
    ControlError::StalePlacement {
        group,
        current: Box::new(placement.clone()),
    }
}

fn voter_count(voters: &VoterSet) -> u8 {
    u8::try_from(voters.len()).unwrap_or(u8::MAX)
}

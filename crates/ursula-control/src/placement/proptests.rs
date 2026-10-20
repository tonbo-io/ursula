//! Kernel property tests: random valid and invalid commands and history
//! events over 1 to 6 nodes, 1 to 4 groups and replication factors 1 to 3,
//! checked after every step against K1 to K16 of the design (issue #768):
//! refusals change nothing, placement keys move exactly on accepted moves,
//! converges and history events and never backwards, retirements never sit
//! beside a role, removal is terminal, and the rest. An accepted command
//! must also leave exactly the state the effect column of the command table
//! (2.3) describes, built here from that table rather than from the
//! dispatcher, and answer what that state says.

use std::collections::BTreeSet;

use proptest::prelude::ProptestConfig;
use proptest::test_runner::RngSeed;
use ursula_proto::placement::MetaLogId;
use ursula_proto::placement::NodeId;
use ursula_proto::placement::NodeLifecycle;
use ursula_proto::placement::NodeRecord;
use ursula_proto::placement::PlacementEpoch;
use ursula_proto::placement::Registered;
use ursula_proto::placement::VoterSet;
use ursula_shard::RaftGroupId;

use super::Cluster;
use super::ControlApplied;
use super::ControlCommand;
use super::ControlPlaneState;
use super::ControlResponse;
use super::GroupPlacement;
use super::HistoryBegun;
use super::HistoryCause;
use super::LifecycleChange;
use super::MaintenanceAuthority;
use super::Move;
use super::MoveLimit;
use super::Retirement;
use super::Target;
use super::group_ids;
use super::initializer;
use super::samples::Driver;
use super::samples::membership;
use super::samples::meta;
use super::samples::node;
use super::samples::params;
use super::seed_voters;

/// `x mod n` as an index.
fn index(x: u8, n: usize) -> usize {
    usize::from(x).checked_rem(n).unwrap()
}

/// Whether `x` falls in one of `n` buckets.
fn one_in(x: u8, n: u8) -> bool {
    x.checked_rem(n) == Some(0)
}

fn ensure(condition: bool, violation: impl FnOnce() -> String) -> Result<(), String> {
    if condition { Ok(()) } else { Err(violation()) }
}

/// What one step does.
#[derive(Debug, Clone)]
enum Step {
    Command(ControlCommand),
    History(HistoryCause),
}

/// What one step answered.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Outcome {
    Command(ControlResponse),
    History(HistoryBegun),
}

struct Run {
    driver: Driver,
    /// Nodes `1..=node_count` exist. One more id starts unregistered.
    node_count: u64,
    /// Bootstraps accepted so far.
    bootstraps: u32,
}

impl Run {
    fn new(node_count: u64) -> Self {
        Self {
            driver: Driver::new(),
            node_count,
            bootstraps: 0,
        }
    }

    /// Every id a command may name: the nodes and one unknown id.
    fn ids(&self) -> Vec<NodeId> {
        (1..=self.node_count.checked_add(1).unwrap())
            .map(node)
            .collect()
    }

    fn pick_node(&self, x: u8) -> NodeId {
        let ids = self.ids();
        ids[index(x, ids.len())]
    }

    /// A non-empty set of ids chosen by the bits of `mask`.
    fn voters_from_mask(&self, mask: u8) -> VoterSet {
        let chosen: Vec<NodeId> = self
            .ids()
            .into_iter()
            .enumerate()
            .filter(|(bit, _)| {
                (mask.checked_shr(u32::try_from(*bit).unwrap()).unwrap_or(0) & 1) == 1
            })
            .map(|(_, id)| id)
            .collect();
        VoterSet::from_first(chosen.first().copied().unwrap_or(node(1)), chosen)
    }

    fn cluster(&self) -> Option<&Cluster> {
        self.driver.state.cluster()
    }

    fn pick_group(&self, x: u8) -> RaftGroupId {
        let count = self
            .cluster()
            .map_or(1, |cluster| cluster.params().group_count.get());
        // One group id past the last.
        let ids = usize::try_from(count.checked_add(1).unwrap()).unwrap();
        RaftGroupId(u32::try_from(index(x, ids)).unwrap())
    }

    /// The placement key a command names: usually the current one.
    fn expected(&self, group: RaftGroupId, y: u8) -> PlacementEpoch {
        match self.cluster().and_then(|cluster| cluster.group(group)) {
            Some(placement) if !one_in(y, 6) => placement.since(),
            Some(_) | None => PlacementEpoch::new(meta(0, u64::from(y))),
        }
    }

    /// `voters` with one of them replaced by an id outside them.
    fn swap_one(&self, voters: &VoterSet, y: u8, z: u8) -> VoterSet {
        let current: Vec<NodeId> = voters.iter().collect();
        let out = current[index(y, current.len())];
        let candidates: Vec<NodeId> = self
            .ids()
            .into_iter()
            .filter(|id| !voters.contains(*id))
            .collect();
        if candidates.is_empty() {
            return voters.clone();
        }
        let incoming = candidates[index(z, candidates.len())];
        VoterSet::from_first(incoming, current.into_iter().filter(|id| *id != out))
    }

    fn generate(&self, op: u8, x: u8, y: u8, z: u8) -> Step {
        match op.checked_rem(13).unwrap() {
            0 => Step::Command(ControlCommand::RegisterNode {
                node: self.pick_node(x),
                now_ms: u64::from(y),
            }),
            1 => {
                let nodes = self.voters_from_mask(y);
                let ids: Vec<u64> = nodes.iter().map(NodeId::get).collect();
                let maintenance = if one_in(z, 8) {
                    MaintenanceAuthority::Reservation
                } else {
                    MaintenanceAuthority::Placement
                };
                let groups = u32::try_from(index(x, 4)).unwrap().checked_add(1).unwrap();
                let rf = u8::try_from(index(z, 3)).unwrap().checked_add(1).unwrap();
                let mut bootstrap = params(
                    &ids,
                    groups,
                    rf,
                    u32::from(x.checked_shr(2).unwrap()) & 3,
                    maintenance,
                );
                if one_in(y, 7) {
                    bootstrap.meta_voters = VoterSet::single(self.pick_node(z));
                }
                Step::Command(ControlCommand::Bootstrap {
                    params: bootstrap,
                    nodes: ursula_proto::placement::NodeSet::new(nodes.to_set()).unwrap(),
                    now_ms: u64::from(z),
                })
            }
            2..=5 => {
                let group = self.pick_group(x);
                let placement = self.cluster().and_then(|cluster| cluster.group(group));
                let to = match placement {
                    Some(placement) => match placement.moving() {
                        Some(moving) if one_in(z, 4) => moving.from.clone(),
                        Some(_) | None => self.swap_one(placement.target_voters(), y, z),
                    },
                    None => self.voters_from_mask(z),
                };
                Step::Command(ControlCommand::RequestMove {
                    group,
                    expected: self.expected(group, y),
                    to,
                    now_ms: u64::from(x),
                })
            }
            6..=8 => {
                let group = self.pick_group(x);
                let placement = self.cluster().and_then(|cluster| cluster.group(group));
                let voters = match placement {
                    Some(placement) if !one_in(z, 4) => placement.target_voters().clone(),
                    Some(_) | None => self.voters_from_mask(z),
                };
                Step::Command(ControlCommand::ReportConverged {
                    group,
                    since: self.expected(group, y),
                    voters,
                    membership: membership(u64::from(z), u64::from(x)),
                    now_ms: u64::from(y),
                })
            }
            9 => {
                let change = match index(y, 3) {
                    0 => LifecycleChange::Cordon,
                    1 => LifecycleChange::Uncordon,
                    _ => LifecycleChange::Decommission,
                };
                Step::Command(ControlCommand::SetNodeLifecycle {
                    node: self.pick_node(x),
                    change,
                    now_ms: u64::from(z),
                })
            }
            10 => Step::Command(ControlCommand::SetMoveLimit {
                limit: MoveLimit::new(u32::from(y) & 3),
            }),
            11 => {
                let records: Vec<(RaftGroupId, NodeId, MetaLogId)> = self
                    .cluster()
                    .map(|cluster| {
                        cluster
                            .retirements()
                            .iter()
                            .flat_map(|(group, records)| {
                                records
                                    .iter()
                                    .map(move |(node, record)| (*group, *node, record.created_at))
                            })
                            .collect()
                    })
                    .unwrap_or_default();
                let (group, node, created_at) = if records.is_empty() || one_in(z, 5) {
                    (self.pick_group(x), self.pick_node(y), meta(0, u64::from(z)))
                } else {
                    let (group, node, created_at) = records[index(x, records.len())];
                    let created_at = if one_in(y, 4) {
                        meta(0, u64::from(y))
                    } else {
                        created_at
                    };
                    (group, node, created_at)
                };
                Step::Command(ControlCommand::ReportTornDown {
                    group,
                    node,
                    created_at,
                })
            }
            _ => Step::History(if one_in(x, 4) {
                HistoryCause::Plan
            } else {
                HistoryCause::Genesis
            }),
        }
    }

    /// Runs `step` and checks every property against the states around it.
    fn step(&mut self, step: Step) -> Result<(), String> {
        let before = self.driver.state.clone();
        if matches!(step, Step::History(_)) {
            self.driver.term = self.driver.term.checked_add(1).unwrap();
        }
        let at = self.driver.next_at();
        let outcome = match &step {
            Step::Command(command) => {
                let checked = before.check(command);
                let response = self.driver.state.apply(at, command.clone());
                // K1: `check` is the planning step of `apply`.
                ensure(checked == response.clone().map(|_applied| ()), || {
                    format!("K1: check answered {checked:?}, apply {response:?}")
                })?;
                Outcome::Command(response)
            }
            Step::History(cause) => Outcome::History(self.driver.state.begin_history(at, *cause)),
        };
        let after = &self.driver.state;
        // K10: the dispatcher is a function of the state, the entry and the command.
        let mut again = before.clone();
        let replayed = match &step {
            Step::Command(command) => Outcome::Command(again.apply(at, command.clone())),
            Step::History(cause) => Outcome::History(again.begin_history(at, *cause)),
        };
        ensure(replayed == outcome && again == *after, || {
            "K10: replaying the step gave another answer or state".to_owned()
        })?;
        if outcome == Outcome::Command(Ok(ControlApplied::Bootstrapped)) {
            self.bootstraps = self.bootstraps.checked_add(1).unwrap();
        }
        ensure(self.bootstraps <= 1, || "K11: two bootstraps".to_owned())?;
        check_transition(&before, after, at, &step, &outcome)?;
        invariants(after)
    }
}

/// Properties of one state.
fn invariants(state: &ControlPlaneState) -> Result<(), String> {
    let Some(cluster) = state.cluster() else {
        return Ok(());
    };
    let rf = usize::from(cluster.params().replication_factor.get());
    let groups: Vec<RaftGroupId> = cluster.groups().iter().map(|(group, _)| group).collect();
    let expected: Vec<RaftGroupId> = group_ids(cluster.params().group_count).collect();
    ensure(groups == expected, || {
        format!("K9: placements {groups:?}, cluster groups {expected:?}")
    })?;
    for (group, placement) in cluster.groups().iter() {
        let mut named: Vec<NodeId> = placement.current_voters().iter().collect();
        named.extend(placement.target_voters().iter());
        match placement {
            GroupPlacement::Seeded {
                voters,
                initializer,
                ..
            } => {
                ensure(voters.len() == rf, || {
                    format!("K9: {group:?} seeded on {voters:?}")
                })?;
                ensure(voters.contains(*initializer), || {
                    format!("K11: {group:?} initializer {initializer} is no seed voter")
                })?;
            }
            GroupPlacement::Live {
                target: Target::Stable { voters },
                ..
            } => ensure(voters.len() == rf, || {
                format!("K9: {group:?} stable on {voters:?}")
            })?,
            GroupPlacement::Live {
                target: Target::Moving(moving),
                ..
            } => {
                ensure(moving.from.len() == rf && moving.to.len() == rf, || {
                    format!("K9: {group:?} moves {moving:?}")
                })?;
                ensure(
                    moving
                        .abandoned
                        .iter()
                        .all(|id| !moving.from.contains(*id) && !moving.to.contains(*id)),
                    || format!("K9: {group:?} abandons a voter: {moving:?}"),
                )?;
                named.extend(moving.abandoned.iter().copied());
            }
        }
        for id in named {
            ensure(cluster.node(id).is_some(), || {
                format!("K9: {group:?} names unregistered node {id}")
            })?;
        }
    }
    for (group, records) in cluster.retirements() {
        ensure(!records.is_empty(), || {
            format!("K9: {group:?} keeps an empty record map")
        })?;
        for id in records.keys() {
            ensure(cluster.role(*group, *id).is_none(), || {
                format!("K7: node {id} has a record and a role in {group:?}")
            })?;
            ensure(cluster.node(*id).is_some(), || {
                format!("K9: record of unregistered node {id}")
            })?;
        }
    }
    for (id, record) in cluster.nodes() {
        let has_role = cluster.has_any_role(*id);
        let has_record = cluster
            .retirements()
            .values()
            .any(|records| records.contains_key(id));
        match record.lifecycle {
            NodeLifecycle::Removed => ensure(!has_role && !has_record, || {
                format!("K5: removed node {id} keeps a role or record")
            })?,
            NodeLifecycle::Decommissioning => ensure(has_role, || {
                format!("K6: decommissioning node {id} has no role and is not removed")
            })?,
            NodeLifecycle::Active | NodeLifecycle::Cordoned => {}
        }
    }
    for id in cluster.params().meta_voters.iter() {
        let lifecycle = cluster.node(id).map(|record| record.lifecycle);
        ensure(
            !matches!(
                lifecycle,
                Some(NodeLifecycle::Decommissioning | NodeLifecycle::Removed)
            ),
            || format!("K12: meta voter {id} is {lifecycle:?}"),
        )?;
    }
    Ok(())
}

/// Properties of one step.
fn check_transition(
    before: &ControlPlaneState,
    after: &ControlPlaneState,
    at: MetaLogId,
    step: &Step,
    outcome: &Outcome,
) -> Result<(), String> {
    let since = PlacementEpoch::new(at);
    match (step, outcome) {
        (Step::Command(_), Outcome::Command(Err(error))) => {
            // K1: a refused command leaves the state equal, by its encoding too.
            ensure(before == after, || {
                format!("K1: refused with {error:?} and changed the state")
            })?;
            let encoded =
                |state: &ControlPlaneState| ursula_stream::msgpack::encode(state).unwrap();
            ensure(encoded(before) == encoded(after), || {
                "K1: a refusal changed the encoding".to_owned()
            })
        }
        (Step::History(cause), Outcome::History(_)) => {
            // K16: the event drops every record, moves every key and ends a
            // formation on a plan, nothing else.
            let expected = match before {
                ControlPlaneState::Forming { .. } if *cause == HistoryCause::Plan => {
                    ControlPlaneState::FormationCut
                }
                ControlPlaneState::Forming { .. } | ControlPlaneState::FormationCut => {
                    before.clone()
                }
                ControlPlaneState::Bootstrapped(cluster) => {
                    let mut cluster = cluster.clone();
                    cluster.retirements.clear();
                    for placement in cluster.groups.0.values_mut() {
                        match placement {
                            GroupPlacement::Seeded { since: key, .. }
                            | GroupPlacement::Live { since: key, .. } => *key = since,
                        }
                    }
                    ControlPlaneState::Bootstrapped(cluster)
                }
            };
            ensure(expected == *after, || {
                "K16: the history event changed more".to_owned()
            })
        }
        (Step::Command(command), Outcome::Command(Ok(applied))) => {
            check_accepted(before, after, at, command, applied)
        }
        (Step::Command(_), Outcome::History(_)) | (Step::History(_), Outcome::Command(_)) => {
            Err("a step answered as another kind".to_owned())
        }
    }
}

/// Properties of an accepted command.
fn check_accepted(
    before: &ControlPlaneState,
    after: &ControlPlaneState,
    at: MetaLogId,
    command: &ControlCommand,
    applied: &ControlApplied,
) -> Result<(), String> {
    let since = PlacementEpoch::new(at);
    let (ControlPlaneState::Bootstrapped(old), ControlPlaneState::Bootstrapped(new)) =
        (before, after)
    else {
        return check_forming(before, after, since, command, applied);
    };
    // The effect column of 2.3: every placement, node, retirement and
    // parameter, so a command that changes too little fails as one that
    // changes too much. K8 and K14 are part of it.
    let expected = expected_cluster(old, at, command)?;
    ensure(expected == *new, || {
        format!("2.3: {command:?} left {new:?}, the table says {expected:?}")
    })?;
    check_answer(old, new, at, command, applied)?;
    // K13: the reservation refuses moves and decommissions.
    if old.params().maintenance == MaintenanceAuthority::Reservation {
        ensure(
            !matches!(
                command,
                ControlCommand::RequestMove { .. }
                    | ControlCommand::SetNodeLifecycle {
                        change: LifecycleChange::Decommission,
                        ..
                    }
            ),
            || format!("K13: the reservation accepted {command:?}"),
        )?;
    }
    for (group, placement) in new.groups().iter() {
        let Some(previous) = old.group(group) else {
            return Err(format!("K9: {group:?} appeared"));
        };
        let named = command.group() == Some(group);
        let changed_key = placement.since() != previous.since();
        // K2: the key moves exactly on an accepted move or converge of the
        // group, to the entry that applied it, and never backwards.
        let moves_key = named
            && matches!(
                command,
                ControlCommand::RequestMove { .. } | ControlCommand::ReportConverged { .. }
            );
        ensure(changed_key == moves_key, || {
            format!(
                "K2: {group:?} key {} -> {} on {command:?}",
                previous.since(),
                placement.since()
            )
        })?;
        ensure(!changed_key || placement.since() == since, || {
            format!(
                "K2: {group:?} key {} is not the entry {since}",
                placement.since()
            )
        })?;
        ensure(placement.since() >= previous.since(), || {
            format!("K2: {group:?} key went backwards")
        })?;
        if !named && placement != previous {
            return Err(format!("{group:?} changed on {command:?}"));
        }
        check_group_transition(old, group, previous, placement, command)?;
        // K4: every move can be cancelled.
        if let Some(moving) = placement.moving()
            && moving.from != moving.to
            && new.params().maintenance == MaintenanceAuthority::Placement
        {
            let cancel = ControlCommand::RequestMove {
                group,
                expected: placement.since(),
                to: moving.from.clone(),
                now_ms: 0,
            };
            ensure(after.check(&cancel).is_ok(), || {
                format!("K4: {group:?} cannot cancel: {:?}", after.check(&cancel))
            })?;
        }
    }
    // K15: a move start keeps the moving count within the limit.
    if new.moving() > old.moving() {
        ensure(
            new.moving() <= new.params().max_concurrent_moves.get(),
            || {
                format!(
                    "K15: {} moving over limit {}",
                    new.moving(),
                    new.params().max_concurrent_moves.get()
                )
            },
        )?;
    }
    // K5 and K6: removal is terminal, and only a decommissioning node, or
    // one decommissioned by this command, is removed.
    for (id, record) in old.nodes() {
        let lifecycle = new.node(*id).map(|record| record.lifecycle);
        if record.lifecycle == NodeLifecycle::Removed {
            ensure(lifecycle == Some(NodeLifecycle::Removed), || {
                format!("K5: removed node {id} came back as {lifecycle:?}")
            })?;
        } else if lifecycle == Some(NodeLifecycle::Removed) {
            let decommissioned_now = matches!(
                command,
                ControlCommand::SetNodeLifecycle {
                    node,
                    change: LifecycleChange::Decommission,
                    ..
                } if node == id
            );
            ensure(
                record.lifecycle == NodeLifecycle::Decommissioning || decommissioned_now,
                || format!("K6: node {id} went from {:?} to removed", record.lifecycle),
            )?;
        }
    }
    // K6: a record's `since_ms` is the time of the entry that put the node
    // in its lifecycle, a removal by a converge included.
    for (id, record) in new.nodes() {
        let entered = old
            .node(*id)
            .is_none_or(|previous| previous.lifecycle != record.lifecycle);
        if entered {
            ensure(Some(record.since_ms) == now_ms_of(command), || {
                format!("K6: node {id} entered {record:?} on {command:?}")
            })?;
        }
    }
    Ok(())
}

/// The time a command carries, if any.
fn now_ms_of(command: &ControlCommand) -> Option<u64> {
    match command {
        ControlCommand::RegisterNode { now_ms, .. }
        | ControlCommand::Bootstrap { now_ms, .. }
        | ControlCommand::RequestMove { now_ms, .. }
        | ControlCommand::ReportConverged { now_ms, .. }
        | ControlCommand::SetNodeLifecycle { now_ms, .. } => Some(*now_ms),
        ControlCommand::SetMoveLimit { .. } | ControlCommand::ReportTornDown { .. } => None,
    }
}

/// K3 for one group.
fn check_group_transition(
    old: &Cluster,
    group: RaftGroupId,
    previous: &GroupPlacement,
    placement: &GroupPlacement,
    command: &ControlCommand,
) -> Result<(), String> {
    let stable = matches!(placement, GroupPlacement::Live {
        target: Target::Stable { .. },
        ..
    });
    match previous {
        GroupPlacement::Live {
            target: Target::Moving(moving),
            since: key,
            ..
        } if stable => {
            // K3: a move ends only by a report of its target at its key.
            let reported = matches!(
                command,
                ControlCommand::ReportConverged { group: g, since, voters, .. }
                    if *g == group && since == key && *voters == moving.to
            );
            ensure(reported, || {
                format!("K3: {group:?} stopped moving on {command:?}")
            })
        }
        GroupPlacement::Seeded { since: key, .. }
            if !matches!(placement, GroupPlacement::Seeded { .. }) =>
        {
            // K3: a seeded group forms only by a report of `rf` registered,
            // non-removed voters at its key.
            let ControlCommand::ReportConverged {
                group: g,
                since,
                voters,
                ..
            } = command
            else {
                return Err(format!("K3: {group:?} formed on {command:?}"));
            };
            ensure(*g == group && since == key, || {
                format!("K3: {group:?} formed on {command:?}")
            })?;
            ensure(
                voters.len() == usize::from(old.params().replication_factor.get())
                    && voters.iter().all(|id| {
                        old.node(id)
                            .is_some_and(|record| record.lifecycle != NodeLifecycle::Removed)
                    }),
                || format!("K3: {group:?} formed on {voters:?}"),
            )
        }
        GroupPlacement::Seeded { .. } | GroupPlacement::Live { .. } => Ok(()),
    }
}

/// The cluster an accepted `command` applied at `at` leaves, by the effect
/// column of the command table (2.3).
fn expected_cluster(
    old: &Cluster,
    at: MetaLogId,
    command: &ControlCommand,
) -> Result<Cluster, String> {
    let since = PlacementEpoch::new(at);
    let mut expected = old.clone();
    match command {
        ControlCommand::RegisterNode { node, now_ms } => {
            // A new id joins `Active`. A known id changes nothing, whatever
            // its lifecycle.
            expected.nodes.entry(*node).or_insert(NodeRecord {
                lifecycle: NodeLifecycle::Active,
                since_ms: *now_ms,
            });
        }
        ControlCommand::Bootstrap { .. } => {
            return Err(format!("K11: a bootstrapped cluster accepted {command:?}"));
        }
        ControlCommand::RequestMove {
            group, to, now_ms, ..
        } => {
            let Some(GroupPlacement::Live {
                converged_at,
                target,
                ..
            }) = old.group(*group)
            else {
                return Err(format!("{group:?} accepted a move while not live"));
            };
            let moving = match target {
                Target::Stable { voters } => Move {
                    from: voters.clone(),
                    to: to.clone(),
                    abandoned: BTreeSet::new(),
                    since_ms: *now_ms,
                },
                // `abandoned := (abandoned ∪ old to) \ (from ∪ to)`.
                Target::Moving(moving) => Move {
                    from: moving.from.clone(),
                    to: to.clone(),
                    abandoned: moving
                        .abandoned
                        .iter()
                        .copied()
                        .chain(moving.to.iter())
                        .filter(|id| !moving.from.contains(*id) && !to.contains(*id))
                        .collect(),
                    since_ms: moving.since_ms,
                },
            };
            expected.groups.0.insert(*group, GroupPlacement::Live {
                since,
                converged_at: *converged_at,
                target: Target::Moving(moving),
            });
        }
        ControlCommand::ReportConverged {
            group,
            voters,
            membership,
            now_ms,
            ..
        } => {
            // A seed node outside `voters` gets no record. K8: a move
            // retires `(from ∪ abandoned) \ to`.
            let retired: BTreeSet<NodeId> = match old.group(*group) {
                Some(GroupPlacement::Seeded { .. }) => BTreeSet::new(),
                Some(GroupPlacement::Live {
                    target: Target::Moving(moving),
                    ..
                }) => moving
                    .from
                    .iter()
                    .chain(moving.abandoned.iter().copied())
                    .filter(|id| !moving.to.contains(*id))
                    .collect(),
                other => return Err(format!("{group:?} accepted a report from {other:?}")),
            };
            expected.groups.0.insert(*group, GroupPlacement::Live {
                since,
                converged_at: *membership,
                target: Target::Stable {
                    voters: voters.clone(),
                },
            });
            for id in retired {
                expected
                    .retirements
                    .entry(*group)
                    .or_default()
                    .insert(id, Retirement { created_at: at });
            }
            let roleless: Vec<NodeId> = expected
                .nodes
                .iter()
                .filter(|(id, record)| {
                    record.lifecycle == NodeLifecycle::Decommissioning
                        && !expected.has_any_role(**id)
                })
                .map(|(id, _)| *id)
                .collect();
            for id in roleless {
                remove(&mut expected, id, *now_ms);
            }
        }
        ControlCommand::SetNodeLifecycle {
            node,
            change,
            now_ms,
        } => {
            let Some(record) = old.node(*node) else {
                return Err(format!("accepted {command:?} for an unregistered node"));
            };
            let lifecycle = match (change, record.lifecycle) {
                (LifecycleChange::Cordon, NodeLifecycle::Active) => NodeLifecycle::Cordoned,
                (
                    LifecycleChange::Uncordon,
                    NodeLifecycle::Cordoned | NodeLifecycle::Decommissioning,
                ) => NodeLifecycle::Active,
                (
                    LifecycleChange::Decommission,
                    NodeLifecycle::Active | NodeLifecycle::Cordoned,
                ) => NodeLifecycle::Decommissioning,
                // Repeats are no-ops.
                (LifecycleChange::Cordon, NodeLifecycle::Cordoned)
                | (LifecycleChange::Uncordon, NodeLifecycle::Active)
                | (LifecycleChange::Decommission, NodeLifecycle::Decommissioning) => {
                    record.lifecycle
                }
                (_, lifecycle) => {
                    return Err(format!("accepted {command:?} for a {lifecycle:?} node"));
                }
            };
            if lifecycle != record.lifecycle {
                expected.nodes.insert(*node, NodeRecord {
                    lifecycle,
                    since_ms: *now_ms,
                });
                // A node without a role is removed at once.
                if lifecycle == NodeLifecycle::Decommissioning && !expected.has_any_role(*node) {
                    remove(&mut expected, *node, *now_ms);
                }
            }
        }
        ControlCommand::SetMoveLimit { limit } => expected.params.max_concurrent_moves = *limit,
        ControlCommand::ReportTornDown {
            group,
            node,
            created_at,
        } => {
            // K14: only the matching record goes.
            if old
                .retirement(*group, *node)
                .is_some_and(|record| record.created_at == *created_at)
                && let Some(records) = expected.retirements.get_mut(group)
            {
                records.remove(node);
                if records.is_empty() {
                    expected.retirements.remove(group);
                }
            }
        }
    }
    Ok(expected)
}

/// Marks `node` removed as of `now_ms` and drops its retirements.
fn remove(cluster: &mut Cluster, node: NodeId, now_ms: u64) {
    cluster.nodes.insert(node, NodeRecord {
        lifecycle: NodeLifecycle::Removed,
        since_ms: now_ms,
    });
    for records in cluster.retirements.values_mut() {
        records.remove(&node);
    }
    cluster.retirements.retain(|_, records| !records.is_empty());
}

/// The answer of an accepted command, read from the state it left.
fn check_answer(
    old: &Cluster,
    new: &Cluster,
    at: MetaLogId,
    command: &ControlCommand,
    applied: &ControlApplied,
) -> Result<(), String> {
    let since = PlacementEpoch::new(at);
    let record_of = |node: NodeId| {
        new.node(node)
            .copied()
            .ok_or_else(|| format!("{command:?} left node {node} unregistered"))
    };
    let answer = match command {
        ControlCommand::RegisterNode { node, .. } => {
            let record = record_of(*node)?;
            ControlApplied::NodeRegistered(if new.has_any_role(*node) {
                Registered::Referenced(record)
            } else {
                Registered::Unreferenced(record)
            })
        }
        ControlCommand::Bootstrap { .. } => {
            return Err(format!("K11: a bootstrapped cluster accepted {command:?}"));
        }
        ControlCommand::RequestMove { .. } => ControlApplied::MoveAccepted { since },
        ControlCommand::ReportConverged { group, .. } => ControlApplied::Converged {
            since,
            retired: new
                .retirements()
                .get(group)
                .map(|records| {
                    records
                        .iter()
                        .filter(|(_, record)| record.created_at == at)
                        .map(|(id, _)| *id)
                        .collect()
                })
                .unwrap_or_default(),
            removed: new
                .nodes()
                .iter()
                .filter(|(id, record)| {
                    record.lifecycle == NodeLifecycle::Removed
                        && old
                            .node(**id)
                            .is_some_and(|before| before.lifecycle != NodeLifecycle::Removed)
                })
                .map(|(id, _)| *id)
                .collect(),
        },
        ControlCommand::SetNodeLifecycle { node, .. } => ControlApplied::LifecycleChanged {
            lifecycle: record_of(*node)?.lifecycle,
        },
        ControlCommand::SetMoveLimit { limit } => ControlApplied::MoveLimitSet { limit: *limit },
        ControlCommand::ReportTornDown {
            group,
            node,
            created_at,
        } => {
            if old
                .retirement(*group, *node)
                .is_some_and(|record| record.created_at == *created_at)
            {
                ControlApplied::TornDown
            } else {
                ControlApplied::NothingToTearDown
            }
        }
    };
    ensure(answer == *applied, || {
        format!("{command:?} answered {applied:?}, its state says {answer:?}")
    })
}

/// Properties of an accepted command before Bootstrap.
fn check_forming(
    before: &ControlPlaneState,
    after: &ControlPlaneState,
    since: PlacementEpoch,
    command: &ControlCommand,
    applied: &ControlApplied,
) -> Result<(), String> {
    match (before, after, command) {
        (
            ControlPlaneState::Forming { .. },
            ControlPlaneState::Bootstrapped(cluster),
            ControlCommand::Bootstrap { params, nodes, .. },
        ) => {
            // K11: one Bootstrap, every group seeded by the seed rule.
            ensure(*applied == ControlApplied::Bootstrapped, || {
                format!("K11: bootstrap answered {applied:?}")
            })?;
            for group in group_ids(params.group_count) {
                let expected = GroupPlacement::Seeded {
                    since,
                    voters: seed_voters(group, params.replication_factor, nodes),
                    initializer: initializer(group, nodes),
                };
                ensure(cluster.group(group) == Some(&expected), || {
                    format!("K11: {group:?} seeded as {:?}", cluster.group(group))
                })?;
            }
            Ok(())
        }
        (
            ControlPlaneState::Forming { registered: old },
            ControlPlaneState::Forming { registered: new },
            ControlCommand::RegisterNode { node, .. },
        ) => {
            // A registration adds its node and changes no other.
            ensure(
                new.contains_key(node)
                    && old.keys().all(|id| new.contains_key(id))
                    && new.iter().all(|(id, registration)| {
                        old.get(id)
                            .map_or(id == node, |previous| previous == registration)
                    }),
                || "a registration changed another node".to_owned(),
            )
        }
        _ => Err(format!("{command:?} accepted from {before:?} to {after:?}")),
    }
}

#[test]
fn every_sample_state_holds_the_state_invariants() {
    for state in super::samples::sample_states() {
        if let Err(violation) = invariants(&state) {
            panic!("{state:?}: {violation}");
        }
    }
}

proptest::proptest! {
    #![proptest_config(ProptestConfig {
        cases: 256,
        rng_seed: RngSeed::Fixed(768),
        // The seed is fixed, so a failure reproduces without a regression file.
        failure_persistence: None,
        ..ProptestConfig::default()
    })]

    #[test]
    fn the_kernel_keeps_its_invariants_over_random_commands_and_history_events(
        node_count in 1_u64..=6,
        group_count in 1_u32..=4,
        rf in 1_u8..=3,
        limit in 0_u32..=3,
        reservation in proptest::bool::weighted(0.1),
        bootstrap_first in proptest::bool::weighted(0.9),
        steps in proptest::collection::vec(
            (0_u8..13, proptest::num::u8::ANY, proptest::num::u8::ANY, proptest::num::u8::ANY),
            1..200,
        ),
    ) {
        let mut run = Run::new(node_count);
        let ids: Vec<u64> = (1..=node_count).collect();
        let mut prelude = vec![Step::History(HistoryCause::Genesis)];
        prelude.extend(ids.iter().map(|id| Step::Command(ControlCommand::RegisterNode {
            node: node(*id),
            now_ms: 1,
        })));
        if bootstrap_first {
            let rf = rf.min(u8::try_from(node_count).unwrap());
            let maintenance = if reservation {
                MaintenanceAuthority::Reservation
            } else {
                MaintenanceAuthority::Placement
            };
            prelude.push(Step::Command(ControlCommand::Bootstrap {
                params: params(&ids, group_count, rf, limit, maintenance),
                nodes: super::samples::node_set(&ids),
                now_ms: 2,
            }));
        }
        for step in prelude {
            if let Err(violation) = run.step(step.clone()) {
                proptest::prop_assert!(false, "{step:?}: {violation}");
            }
        }
        for (op, x, y, z) in steps {
            let step = run.generate(op, x, y, z);
            if let Err(violation) = run.step(step.clone()) {
                proptest::prop_assert!(false, "{step:?}: {violation}");
            }
        }
        // K10: the final state round trips through both encodings.
        let state = &run.driver.state;
        let bytes = ursula_stream::msgpack::encode(state).unwrap();
        let decoded: ControlPlaneState = ursula_stream::msgpack::decode(&bytes).unwrap();
        proptest::prop_assert_eq!(&decoded, state);
        let json = serde_json::to_string(state).unwrap();
        let decoded: ControlPlaneState = serde_json::from_str(&json).unwrap();
        proptest::prop_assert_eq!(&decoded, state);
    }
}

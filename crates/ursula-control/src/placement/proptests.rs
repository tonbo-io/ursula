//! Kernel property tests: random valid and invalid commands and history
//! events over 1 to 6 nodes, 1 to 4 groups and replication factors 1 to 3,
//! checked after every step against K1 to K12 and K14 to K19 of the design
//! (issue #768):
//! refusals change nothing, placement keys move exactly on accepted moves,
//! converges and history events and never backwards, retirements never sit
//! beside a role, removal is terminal, a root slot changes only as its
//! table says, and the rest. An accepted command
//! must also leave exactly the state the effect column of the command table
//! (2.3) describes, built here from that table rather than from the
//! dispatcher, and answer what that state says.

use std::collections::BTreeMap;
use std::collections::BTreeSet;

use proptest::prelude::ProptestConfig;
use proptest::test_runner::RngSeed;
use ursula_proto::placement::MetaLogId;
use ursula_proto::placement::NodeId;
use ursula_proto::placement::NodeLifecycle;
use ursula_proto::placement::NodeRecord;
use ursula_proto::placement::PlacementEpoch;
use ursula_proto::placement::Registered;
use ursula_proto::placement::RootId;
use ursula_proto::placement::RootSlot;
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
use super::samples::register;
use super::samples::root;
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

    /// One of the three roots node `id` may run on, its first one
    /// `root(id)`, which [`register`] registers.
    fn pick_root(id: NodeId, x: u8) -> RootId {
        let offset = u64::try_from(index(x, 3))
            .unwrap()
            .checked_mul(100)
            .unwrap();
        root(id.get().checked_add(offset).unwrap())
    }

    /// The root node `id`'s slot names, if it has one.
    fn slot_root(&self, id: NodeId) -> Option<RootId> {
        slots(&self.driver.state)
            .get(&id)
            .map(|(slot, _)| slot.root())
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

    /// The step `(op, x, y, z)` chooses from the current state: mostly
    /// commands a correct caller would send, some stale or invalid.
    fn generate(&self, op: u8, x: u8, y: u8, z: u8) -> Step {
        match op.checked_rem(14).unwrap() {
            0 => {
                let node = self.pick_node(x);
                Step::Command(ControlCommand::RegisterNode {
                    node,
                    root: Self::pick_root(node, y),
                    empty: one_in(z, 2),
                    now_ms: u64::from(y),
                })
            }
            1 => {
                let nodes = self.voters_from_mask(y);
                let ids: Vec<u64> = nodes.iter().map(NodeId::get).collect();
                let groups = u32::try_from(index(x, 4)).unwrap().checked_add(1).unwrap();
                let rf = u8::try_from(index(z, 3)).unwrap().checked_add(1).unwrap();
                let mut bootstrap =
                    params(&ids, groups, rf, u32::from(x.checked_shr(2).unwrap()) & 3);
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
            12 => {
                // Mostly the root the slot names, so most releases apply.
                let node = self.pick_node(x);
                let root = match self.slot_root(node) {
                    Some(root) if !one_in(y, 4) => root,
                    Some(_) | None => Self::pick_root(node, z),
                };
                Step::Command(ControlCommand::ReleaseRoot { node, root })
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
        check_slots(&before, after, &step)?;
        invariants(after)
    }
}

/// A state reachable from a bootstrapped cluster of nodes `1..=node_count`,
/// every group seeded or, with `live`, reported, by the steps
/// `(op, x, y, z)` choose, refused ones included: moves, cancels,
/// retargets, converges, lifecycles, teardown reports and history events.
/// The planners' property tests plan from such states.
pub(super) fn reachable(
    node_count: u64,
    group_count: u32,
    rf: u8,
    live: bool,
    steps: &[(u8, u8, u8, u8)],
) -> Driver {
    let ids: Vec<u64> = (1..=node_count).collect();
    let rf = rf.min(u8::try_from(node_count).unwrap());
    let driver = if live {
        Driver::live(&ids, group_count, rf)
    } else {
        Driver::bootstrapped(&ids, params(&ids, group_count, rf, 4))
    };
    let mut run = Run {
        driver,
        node_count,
        bootstraps: 1,
    };
    for (op, x, y, z) in steps.iter().copied() {
        let step = run.generate(op, x, y, z);
        if matches!(step, Step::History(_)) {
            run.driver.term = run.driver.term.checked_add(1).unwrap();
        }
        let at = run.driver.next_at();
        match step {
            Step::Command(command) => {
                let _response = run.driver.state.apply(at, command);
            }
            Step::History(cause) => {
                let _begun = run.driver.state.begin_history(at, cause);
            }
        }
    }
    run.driver
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
        (Step::History(cause), Outcome::History(answer)) => {
            // K16: the event drops every record, moves every key and ends a
            // formation on a plan, nothing else, and answers what it dropped.
            let (expected, expected_answer) = match before {
                ControlPlaneState::Forming { .. } => match cause {
                    HistoryCause::Plan => {
                        (ControlPlaneState::FormationCut, HistoryBegun::FormationCut)
                    }
                    HistoryCause::Genesis => (before.clone(), HistoryBegun::Forming),
                },
                ControlPlaneState::FormationCut => (before.clone(), HistoryBegun::FormationCut),
                ControlPlaneState::Bootstrapped(cluster) => {
                    let mut cluster = cluster.clone();
                    let dropped = std::mem::take(&mut cluster.retirements)
                        .into_iter()
                        .map(|(group, records)| (group, records.into_keys().collect()))
                        .collect();
                    for placement in cluster.groups.0.values_mut() {
                        match placement {
                            GroupPlacement::Seeded { since: key, .. }
                            | GroupPlacement::Live { since: key, .. } => *key = since,
                        }
                    }
                    (
                        ControlPlaneState::Bootstrapped(cluster),
                        HistoryBegun::Rebased { dropped },
                    )
                }
            };
            ensure(expected == *after, || {
                "K16: the history event changed more".to_owned()
            })?;
            ensure(expected_answer == *answer, || {
                format!("K16: the history event answered {answer:?}, not {expected_answer:?}")
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
        ControlCommand::SetMoveLimit { .. }
        | ControlCommand::ReportTornDown { .. }
        | ControlCommand::ReleaseRoot { .. } => None,
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
        ControlCommand::RegisterNode {
            node,
            root,
            empty,
            now_ms,
        } => match expected.nodes.get_mut(node) {
            // A new id joins `Active` on its root.
            None => {
                expected.nodes.insert(*node, NodeRecord {
                    lifecycle: NodeLifecycle::Active,
                    since_ms: *now_ms,
                    root: RootSlot::Bound(*root),
                });
            }
            // A known id keeps its lifecycle and time, and changes its slot
            // only when an empty root takes a released one.
            Some(record) => {
                if let RootSlot::Released(released) = record.root
                    && released != *root
                    && *empty
                {
                    record.root = RootSlot::Bound(*root);
                }
            }
        },
        ControlCommand::ReleaseRoot { node, root } => {
            let Some(record) = expected.nodes.get_mut(node) else {
                return Err(format!("accepted {command:?} for an unregistered node"));
            };
            record.root = RootSlot::Released(*root);
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
                    root: record.root,
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
    if let Some(record) = cluster.nodes.get_mut(&node) {
        *record = NodeRecord {
            lifecycle: NodeLifecycle::Removed,
            since_ms: now_ms,
            root: record.root,
        };
    }
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
        ControlCommand::ReleaseRoot { node, .. } => ControlApplied::RootReleased(record_of(*node)?),
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
            ControlCommand::RegisterNode { node, .. } | ControlCommand::ReleaseRoot { node, .. },
        ) => {
            // A registration adds its node and changes no other, and a
            // known node's registration or release changes at most its slot
            // (K17 checks how), never its time.
            ensure(
                new.contains_key(node)
                    && old.keys().all(|id| new.contains_key(id))
                    && new.iter().all(|(id, registration)| {
                        old.get(id).map_or(id == node, |previous| {
                            previous == registration
                                || (id == node && previous.since_ms == registration.since_ms)
                        })
                    }),
                || "a registration changed another node".to_owned(),
            )?;
            let answer = match command {
                ControlCommand::ReleaseRoot { .. } => {
                    let registration = new[node];
                    ControlApplied::RootReleased(NodeRecord {
                        lifecycle: NodeLifecycle::Active,
                        since_ms: registration.since_ms,
                        root: registration.root,
                    })
                }
                _ => ControlApplied::NodeRegistered(Registered::Forming),
            };
            ensure(*applied == answer, || {
                format!("{command:?} answered {applied:?}, not {answer:?}")
            })
        }
        _ => Err(format!("{command:?} accepted from {before:?} to {after:?}")),
    }
}

/// Every node's slot and `since_ms`: the registrations' before Bootstrap,
/// the records' after, none after a formation cut.
fn slots(state: &ControlPlaneState) -> BTreeMap<NodeId, (RootSlot, u64)> {
    match state {
        ControlPlaneState::Forming { registered } => registered
            .iter()
            .map(|(id, registration)| (*id, (registration.root, registration.since_ms)))
            .collect(),
        ControlPlaneState::FormationCut => BTreeMap::new(),
        ControlPlaneState::Bootstrapped(cluster) => cluster
            .nodes()
            .iter()
            .map(|(id, record)| (*id, (record.root, record.since_ms)))
            .collect(),
    }
}

/// K17 to K19 for one step, before and after Bootstrap: a slot changes only
/// as the slot table of 2.3 says, by the command that names its node; a
/// root registered as holding state never takes a released slot; and the
/// root a released slot names never binds again. One slot per id remembers
/// only the latest release: a root released earlier is kept out by the
/// release rule (its disk revoked), not by the kernel, so an empty root
/// under an earlier released id may bind again and is not checked here. A
/// slot change leaves the time.
fn check_slots(
    before: &ControlPlaneState,
    after: &ControlPlaneState,
    step: &Step,
) -> Result<(), String> {
    let old = slots(before);
    let new = slots(after);
    if !matches!(after, ControlPlaneState::FormationCut) {
        ensure(old.keys().all(|id| new.contains_key(id)), || {
            "K17: a slot disappeared".to_owned()
        })?;
    }
    for (id, (slot, since_ms)) in &new {
        let previous = old.get(id).copied();
        if let (RootSlot::Bound(bound), Some((RootSlot::Released(released), _))) = (slot, previous)
        {
            ensure(*bound != released, || {
                format!("K19: node {id} bound its released root {bound} on {step:?}")
            })?;
        }
        if previous.map(|(slot, _)| slot) == Some(*slot) {
            continue;
        }
        let command = match step {
            Step::Command(command) => Some(command),
            Step::History(_) => None,
        };
        if let (
            Some(ControlCommand::RegisterNode { empty: false, .. }),
            Some((RootSlot::Released(_), _)),
        ) = (command, previous)
        {
            return Err(format!(
                "K18: node {id} took a released slot with a root that holds state"
            ));
        }
        let allowed = match (command, previous, slot) {
            (
                Some(ControlCommand::RegisterNode { node, root, .. }),
                None,
                RootSlot::Bound(bound),
            ) => node == id && root == bound,
            (
                Some(ControlCommand::RegisterNode {
                    node,
                    root,
                    empty: true,
                    ..
                }),
                Some((RootSlot::Released(was), _)),
                RootSlot::Bound(bound),
            ) => node == id && root == bound && was != *root,
            (
                Some(ControlCommand::ReleaseRoot { node, root }),
                Some((RootSlot::Bound(was), _)),
                RootSlot::Released(now),
            ) => node == id && root == now && was == *root,
            _ => false,
        };
        ensure(allowed, || {
            format!("K17: node {id}'s slot went from {previous:?} to {slot:?} on {step:?}")
        })?;
        if let Some((_, previous_since)) = previous {
            ensure(previous_since == *since_ms, || {
                format!("K6: node {id}'s slot change moved its time on {step:?}")
            })?;
        }
    }
    Ok(())
}

#[test]
fn every_sample_state_holds_the_state_invariants() {
    for state in super::samples::sample_states() {
        if let Err(violation) = invariants(&state) {
            panic!("{state:?}: {violation}");
        }
    }
}

/// Registers nodes `1..=node_count` after a genesis event, bootstraps them
/// when `bootstrap` holds parameters, then runs the generated steps,
/// checking every property after each one.
fn drive(
    run: &mut Run,
    bootstrap: Option<super::ClusterParams>,
    steps: &[(u8, u8, u8, u8)],
) -> Result<(), String> {
    let ids: Vec<u64> = (1..=run.node_count).collect();
    let mut prelude = vec![Step::History(HistoryCause::Genesis)];
    prelude.extend(ids.iter().map(|id| Step::Command(register(*id, 1))));
    if let Some(params) = bootstrap {
        prelude.push(Step::Command(ControlCommand::Bootstrap {
            params,
            nodes: super::samples::node_set(&ids),
            now_ms: 2,
        }));
    }
    for step in prelude {
        run.step(step.clone())
            .map_err(|violation| format!("{step:?}: {violation}"))?;
    }
    for (op, x, y, z) in steps.iter().copied() {
        let step = run.generate(op, x, y, z);
        run.step(step.clone())
            .map_err(|violation| format!("{step:?}: {violation}"))?;
    }
    Ok(())
}

fn steps_strategy(
    len: std::ops::Range<usize>,
) -> impl proptest::strategy::Strategy<Value = Vec<(u8, u8, u8, u8)>> {
    proptest::collection::vec(
        (
            0_u8..14,
            proptest::num::u8::ANY,
            proptest::num::u8::ANY,
            proptest::num::u8::ANY,
        ),
        len,
    )
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
        bootstrap_first in proptest::bool::weighted(0.9),
        steps in steps_strategy(1..200),
    ) {
        let mut run = Run::new(node_count);
        let bootstrap = bootstrap_first.then(|| {
            let rf = rf.min(u8::try_from(node_count).unwrap());
            let ids: Vec<u64> = (1..=node_count).collect();
            params(&ids, group_count, rf, limit)
        });
        if let Err(violation) = drive(&mut run, bootstrap, &steps) {
            proptest::prop_assert!(false, "{violation}");
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

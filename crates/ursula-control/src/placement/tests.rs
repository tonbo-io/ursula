//! Unit tests of the kernel: one per refusal, the lifecycle of a move, the
//! history event, and the encodings of every root.

use std::collections::BTreeMap;
use std::collections::BTreeSet;

use ursula_proto::placement::MAX_GROUP_COUNT;
use ursula_proto::placement::NodeLifecycle;
use ursula_proto::placement::NodeRecord;
use ursula_proto::placement::PlacementEpoch;
use ursula_proto::placement::Registered;
use ursula_proto::placement::ReplicaRole;
use ursula_proto::placement::RootSlot;
use ursula_shard::RaftGroupId;

use super::BlockReason;
use super::BlockedMove;
use super::BootstrapProblem;
use super::ClusterView;
use super::ControlApplied;
use super::ControlCommand;
use super::ControlError;
use super::ControlPlaneState;
use super::ControlResponse;
use super::GroupPlacement;
use super::HistoryBegun;
use super::HistoryCause;
use super::LifecycleChange;
use super::MetaDamageKind;
use super::MetaLeaderReport;
use super::MetaReplicaStatus;
use super::MetaRole;
use super::MoveLimit;
use super::MoveLimitRequest;
use super::MoveRequest;
use super::NodeEligibility;
use super::PlacementApiError;
use super::PlacementApiReason;
use super::PlacementStatus;
use super::RegistrationStatus;
use super::ReplaceDiskRequest;
use super::Retirement;
use super::Target;
use super::samples::Driver;
use super::samples::ids;
use super::samples::membership;
use super::samples::meta;
use super::samples::node;
use super::samples::node_set;
use super::samples::params;
use super::samples::register;
use super::samples::root;
use super::samples::sample_commands;
use super::samples::sample_errors;
use super::samples::sample_genesis_claim;
use super::samples::sample_history_answers;
use super::samples::sample_history_causes;
use super::samples::sample_responses;
use super::samples::sample_state;
use super::samples::sample_states;
use super::samples::voters;

const G0: RaftGroupId = RaftGroupId(0);
const G1: RaftGroupId = RaftGroupId(1);

fn forming(nodes: &[u64]) -> Driver {
    let mut driver = Driver::new();
    for id in nodes {
        driver.apply(register(*id, 1)).unwrap();
    }
    driver
}

fn bootstrap(driver: &mut Driver, nodes: &[u64], rf: u8) -> ControlResponse {
    driver.apply(ControlCommand::Bootstrap {
        params: params(nodes, 2, rf, 4),
        nodes: node_set(nodes),
        now_ms: 2,
    })
}

#[test]
fn not_bootstrapped_refuses_every_placement_command_while_forming() {
    let mut driver = forming(&[1]);
    let error = driver.refused(ControlCommand::SetMoveLimit {
        limit: MoveLimit::new(1),
    });
    assert!(matches!(error, ControlError::NotBootstrapped));
}

#[test]
fn already_bootstrapped_refuses_a_second_bootstrap() {
    let mut driver = Driver::live(&[1, 2, 3], 2, 3);
    let error = driver.refused(ControlCommand::Bootstrap {
        params: params(&[1, 2, 3], 2, 3, 4),
        nodes: node_set(&[1, 2, 3]),
        now_ms: 3,
    });
    assert!(matches!(error, ControlError::AlreadyBootstrapped));
}

#[test]
fn formation_cut_refuses_everything_after_a_plans_history_event() {
    let mut driver = forming(&[1, 2, 3]);
    assert_eq!(driver.history(HistoryCause::Genesis), HistoryBegun::Forming);
    assert_eq!(
        driver.history(HistoryCause::Plan),
        HistoryBegun::FormationCut
    );
    assert_eq!(driver.state, ControlPlaneState::FormationCut);
    let error = driver.refused(ControlCommand::Bootstrap {
        params: params(&[1, 2, 3], 2, 3, 4),
        nodes: node_set(&[1, 2, 3]),
        now_ms: 3,
    });
    assert!(matches!(error, ControlError::FormationCut));
    let error = driver.refused(register(4, 3));
    assert!(matches!(error, ControlError::FormationCut));
    assert_eq!(
        driver.history(HistoryCause::Genesis),
        HistoryBegun::FormationCut
    );
}

#[test]
fn invalid_bootstrap_names_its_problem() {
    let mut driver = forming(&[1, 2, 3]);
    let mut with_voters = params(&[1, 2, 3], 2, 3, 4);
    with_voters.meta_voters = voters(&[1, 4]);
    let error = driver.refused(ControlCommand::Bootstrap {
        params: with_voters,
        nodes: node_set(&[1, 2, 3]),
        now_ms: 3,
    });
    assert!(matches!(
        error,
        ControlError::InvalidBootstrap {
            problem: BootstrapProblem::MetaVoterNotANode { node: n }
        } if n == node(4)
    ));

    let above = MAX_GROUP_COUNT.checked_add(1).unwrap();
    let over = params(&[1, 2, 3], above, 3, 4);
    let error = driver.refused(ControlCommand::Bootstrap {
        params: over,
        nodes: node_set(&[1, 2, 3]),
        now_ms: 3,
    });
    assert!(matches!(
        error,
        ControlError::InvalidBootstrap {
            problem: BootstrapProblem::GroupCountAboveMax { count }
        } if count.get() == above
    ));
    let at_bound = ControlCommand::Bootstrap {
        params: params(&[1, 2, 3], MAX_GROUP_COUNT, 3, 4),
        nodes: node_set(&[1, 2, 3]),
        now_ms: 3,
    };
    assert_eq!(driver.state.check(&at_bound), Ok(()));

    let mut two = forming(&[1, 2]);
    let error = two.refused(ControlCommand::Bootstrap {
        params: params(&[1, 2], 2, 3, 4),
        nodes: node_set(&[1, 2]),
        now_ms: 3,
    });
    assert!(matches!(error, ControlError::InvalidBootstrap {
        problem: BootstrapProblem::ReplicationFactorAboveNodes
    }));
}

#[test]
fn unknown_node_refuses_a_bootstrap_over_an_unregistered_node() {
    let mut driver = forming(&[1, 2]);
    let error = driver.refused(ControlCommand::Bootstrap {
        params: params(&[1, 2, 3], 2, 3, 4),
        nodes: node_set(&[1, 2, 3]),
        now_ms: 3,
    });
    assert!(matches!(error, ControlError::UnknownNode { node: n } if n == node(3)));
}

#[test]
fn unknown_group_refuses_a_command_naming_a_group_outside_the_cluster() {
    let mut driver = Driver::live(&[1, 2, 3], 2, 3);
    let error = driver.refused(ControlCommand::ReportTornDown {
        group: RaftGroupId(2),
        node: node(1),
        created_at: meta(1, 1),
    });
    assert!(matches!(error, ControlError::UnknownGroup { group } if group == RaftGroupId(2)));
}

#[test]
fn node_removed_refuses_a_removed_nodes_registration_and_placement() {
    let mut driver = Driver::live(&[1, 2, 3, 4], 1, 3);
    // Group 0 is seeded on {1, 2, 3}: node 4 has no role and is removed at once.
    assert_eq!(
        driver.lifecycle(4, LifecycleChange::Decommission),
        Ok(ControlApplied::LifecycleChanged {
            lifecycle: NodeLifecycle::Removed
        })
    );
    let error = driver.refused(register(4, 5));
    assert!(matches!(error, ControlError::NodeRemoved { node: n } if n == node(4)));
    let expected = driver.since(G0);
    let error = driver.refused(ControlCommand::RequestMove {
        group: G0,
        expected,
        to: voters(&[1, 2, 4]),
        now_ms: 5,
    });
    assert!(matches!(error, ControlError::NodeRemoved { node: n } if n == node(4)));
}

#[test]
fn stale_placement_refuses_a_move_or_report_naming_an_older_placement() {
    let mut driver = Driver::live(&[1, 2, 3, 4], 1, 3);
    let old = driver.since(G0);
    driver.request_move(G0, &[1, 2, 4]).unwrap();
    let error = driver.refused(ControlCommand::RequestMove {
        group: G0,
        expected: old,
        to: voters(&[1, 3, 4]),
        now_ms: 5,
    });
    let ControlError::StalePlacement { group, current } = error else {
        panic!("{error:?}");
    };
    assert_eq!(group, G0);
    assert_eq!(*current, driver.placement(G0).clone());
    let error = driver.refused(ControlCommand::ReportConverged {
        group: G0,
        since: old,
        voters: voters(&[1, 2, 4]),
        membership: membership(3, 3),
        now_ms: 6,
    });
    assert!(matches!(error, ControlError::StalePlacement { .. }));
}

#[test]
fn group_not_live_refuses_moving_a_seeded_group() {
    let mut driver = Driver::bootstrapped(&[1, 2, 3, 4], params(&[1, 2, 3, 4], 1, 3, 4));
    let error = driver.refused(ControlCommand::RequestMove {
        group: G0,
        expected: driver.since(G0),
        to: voters(&[1, 2, 4]),
        now_ms: 5,
    });
    assert!(matches!(error, ControlError::GroupNotLive { group } if group == G0));
}

#[test]
fn not_moving_refuses_a_report_for_a_stable_group() {
    let mut driver = Driver::live(&[1, 2, 3], 1, 3);
    let error = driver.refused(ControlCommand::ReportConverged {
        group: G0,
        since: driver.since(G0),
        voters: voters(&[1, 2, 3]),
        membership: membership(3, 3),
        now_ms: 6,
    });
    assert!(matches!(error, ControlError::NotMoving { group } if group == G0));
}

#[test]
fn no_change_refuses_a_move_to_the_current_target() {
    let mut driver = Driver::live(&[1, 2, 3, 4], 1, 3);
    let error = driver.refused(ControlCommand::RequestMove {
        group: G0,
        expected: driver.since(G0),
        to: voters(&[1, 2, 3]),
        now_ms: 5,
    });
    assert!(matches!(error, ControlError::NoChange { group } if group == G0));
    driver.request_move(G0, &[1, 2, 4]).unwrap();
    let error = driver.refused(ControlCommand::RequestMove {
        group: G0,
        expected: driver.since(G0),
        to: voters(&[1, 2, 4]),
        now_ms: 6,
    });
    assert!(matches!(error, ControlError::NoChange { .. }));
}

#[test]
fn replication_factor_change_refuses_a_target_of_another_size() {
    let mut driver = Driver::live(&[1, 2, 3, 4], 1, 3);
    let error = driver.refused(ControlCommand::RequestMove {
        group: G0,
        expected: driver.since(G0),
        to: voters(&[1, 2, 3, 4]),
        now_ms: 5,
    });
    assert!(matches!(error, ControlError::ReplicationFactorChange {
        current: 3,
        requested: 4,
        ..
    }));
}

#[test]
fn ineligible_target_refuses_a_cordoned_node_that_is_not_a_voter_yet() {
    let mut driver = Driver::live(&[1, 2, 3, 4, 5], 1, 3);
    driver.lifecycle(4, LifecycleChange::Cordon).unwrap();
    driver.lifecycle(3, LifecycleChange::Cordon).unwrap();
    let error = driver.refused(ControlCommand::RequestMove {
        group: G0,
        expected: driver.since(G0),
        to: voters(&[1, 2, 4]),
        now_ms: 5,
    });
    assert!(matches!(
        error,
        ControlError::IneligibleTarget {
            node: n,
            lifecycle: NodeLifecycle::Cordoned,
            ..
        } if n == node(4)
    ));
    // A cordoned voter stays eligible in its own group.
    driver.request_move(G0, &[1, 3, 5]).unwrap();
}

#[test]
fn ineligible_target_refuses_keeping_a_cordoned_incoming_node_and_the_move_still_cancels() {
    let mut driver = Driver::live(&[1, 2, 3, 4, 5], 1, 3);
    driver.request_move(G0, &[1, 2, 4]).unwrap();
    driver.lifecycle(4, LifecycleChange::Cordon).unwrap();
    // Node 4 is only a target of the move, not a voter of its source: a
    // retarget may not keep it once it is cordoned.
    let error = driver.refused(ControlCommand::RequestMove {
        group: G0,
        expected: driver.since(G0),
        to: voters(&[1, 4, 5]),
        now_ms: 5,
    });
    assert!(matches!(
        error,
        ControlError::IneligibleTarget {
            group,
            node: n,
            lifecycle: NodeLifecycle::Cordoned,
        } if group == G0 && n == node(4)
    ));
    assert_eq!(
        error.to_string(),
        "node 4 is cordoned and cannot join group 0"
    );
    // The cancel names only the source's voters, so it is accepted.
    let accepted = driver.request_move(G0, &[1, 2, 3]).unwrap();
    assert_eq!(accepted, ControlApplied::MoveAccepted {
        since: driver.since(G0)
    });
    assert_eq!(
        driver.cluster().role(G0, node(4)),
        Some(ReplicaRole::Abandoned)
    );
}

#[test]
fn retirement_pending_refuses_placing_a_node_before_its_teardown_report() {
    let mut driver = Driver::live(&[1, 2, 3, 4], 1, 3);
    driver.request_move(G0, &[1, 2, 4]).unwrap();
    let converged = driver.report(G0, &[1, 2, 4]).unwrap();
    assert!(matches!(
        &converged,
        ControlApplied::Converged { retired, removed, .. } if *retired == ids(&[3]) && removed.is_empty()
    ));
    let error = driver.refused(ControlCommand::RequestMove {
        group: G0,
        expected: driver.since(G0),
        to: voters(&[1, 2, 3]),
        now_ms: 5,
    });
    assert!(matches!(error, ControlError::RetirementPending { node: n, .. } if n == node(3)));
    let created_at = driver.cluster().retirement(G0, node(3)).unwrap().created_at;
    assert_eq!(
        driver.apply(ControlCommand::ReportTornDown {
            group: G0,
            node: node(3),
            created_at,
        }),
        Ok(ControlApplied::TornDown)
    );
    driver.request_move(G0, &[1, 2, 3]).unwrap();
}

#[test]
fn move_limit_refuses_a_new_move_but_not_a_retarget_or_cancel() {
    let mut driver = Driver::live(&[1, 2, 3, 4, 5], 2, 3);
    driver
        .apply(ControlCommand::SetMoveLimit {
            limit: MoveLimit::new(1),
        })
        .unwrap();
    driver.request_move(G0, &[1, 2, 4]).unwrap();
    let error = driver.refused(ControlCommand::RequestMove {
        group: G1,
        expected: driver.since(G1),
        to: voters(&[2, 3, 5]),
        now_ms: 5,
    });
    assert!(matches!(error, ControlError::MoveLimit {
        limit: 1,
        moving: 1
    }));
    driver.request_move(G0, &[1, 2, 5]).unwrap();
    driver.request_move(G0, &[1, 2, 3]).unwrap();
}

#[test]
fn not_target_refuses_a_report_of_other_voters() {
    let mut driver = Driver::live(&[1, 2, 3, 4], 1, 3);
    driver.request_move(G0, &[1, 2, 4]).unwrap();
    let error = driver.refused(ControlCommand::ReportConverged {
        group: G0,
        since: driver.since(G0),
        voters: voters(&[1, 2, 3]),
        membership: membership(3, 3),
        now_ms: 6,
    });
    assert!(
        matches!(error, ControlError::NotTarget { reported, .. } if reported == voters(&[1, 2, 3]))
    );
    // A seeded group reports `rf` voters.
    let mut seeded = Driver::bootstrapped(&[1, 2, 3], params(&[1, 2, 3], 1, 3, 4));
    let error = seeded.refused(ControlCommand::ReportConverged {
        group: G0,
        since: seeded.since(G0),
        voters: voters(&[1, 2]),
        membership: membership(3, 3),
        now_ms: 6,
    });
    assert!(matches!(error, ControlError::NotTarget { .. }));
}

#[test]
fn meta_voter_refuses_decommissioning_a_meta_voter() {
    let mut driver = Driver::live(&[1, 2, 3, 4], 1, 3);
    let error = driver.refused(ControlCommand::SetNodeLifecycle {
        node: node(2),
        change: LifecycleChange::Decommission,
        now_ms: 5,
    });
    assert!(matches!(error, ControlError::MetaVoter { node: n } if n == node(2)));
}

#[test]
fn decommissioning_refuses_cordoning_a_decommissioning_node() {
    let mut driver = Driver::live(&[1, 2, 3, 4], 1, 3);
    driver.request_move(G0, &[1, 2, 4]).unwrap();
    // Node 4 is incoming, so its decommission keeps it until the move ends.
    assert_eq!(
        driver.lifecycle(4, LifecycleChange::Decommission),
        Ok(ControlApplied::LifecycleChanged {
            lifecycle: NodeLifecycle::Decommissioning
        })
    );
    let error = driver.refused(ControlCommand::SetNodeLifecycle {
        node: node(4),
        change: LifecycleChange::Cordon,
        now_ms: 5,
    });
    assert!(matches!(error, ControlError::Decommissioning { node: n } if n == node(4)));
}

/// Node `id`'s slot and `since_ms`: its registration's before Bootstrap,
/// its record's after.
fn slot_of(driver: &Driver, id: u64) -> (RootSlot, u64) {
    match &driver.state {
        ControlPlaneState::Forming { registered } => {
            let registration = registered[&node(id)];
            (registration.root, registration.since_ms)
        }
        ControlPlaneState::Bootstrapped(cluster) => {
            let record = cluster.node(node(id)).unwrap();
            (record.root, record.since_ms)
        }
        ControlPlaneState::FormationCut => panic!("no slots after a formation cut"),
    }
}

fn register_root(id: u64, root_id: u64, empty: bool) -> ControlCommand {
    ControlCommand::RegisterNode {
        node: node(id),
        root: root(root_id),
        empty,
        now_ms: 70,
    }
}

fn release(id: u64, root_id: u64) -> ControlCommand {
    ControlCommand::ReleaseRoot {
        node: node(id),
        root: root(root_id),
    }
}

/// The slot table of 2.3 before Bootstrap and after, node 1 being a meta
/// voter: K17 (a slot changes only as the table says), K18 (a root that
/// holds state never takes a released slot) and K19 (the root a released
/// slot names never binds again).
#[test]
fn other_root_refuses_a_root_its_slot_does_not_take() {
    for bootstrapped in [false, true] {
        let mut driver = forming(&[1, 2, 3]);
        if bootstrapped {
            bootstrap(&mut driver, &[1, 2, 3], 3).unwrap();
        }
        let (bound, since_ms) = slot_of(&driver, 1);
        assert_eq!(bound, RootSlot::Bound(root(1)));
        // Its own root again changes nothing.
        driver.apply(register(1, 71)).unwrap();
        assert_eq!(slot_of(&driver, 1), (bound, since_ms));
        // Another root, empty or not, while the slot is bound.
        for empty in [false, true] {
            let error = driver.refused(register_root(1, 11, empty));
            assert!(
                matches!(error, ControlError::OtherRoot { node: n, slot } if n == node(1) && slot == bound),
                "{error:?}"
            );
            assert_eq!(error.requeue(), super::Requeue::Retry);
        }
        driver.apply(release(1, 1)).unwrap();
        let released = RootSlot::Released(root(1));
        assert_eq!(slot_of(&driver, 1), (released, since_ms));
        // K19: the root the released slot names never binds again, empty or not.
        for empty in [false, true] {
            let error = driver.refused(register_root(1, 1, empty));
            assert!(
                matches!(error, ControlError::OtherRoot { slot, .. } if slot == released),
                "{error:?}"
            );
        }
        // K18: a root that holds state never takes the released slot.
        let error = driver.refused(register_root(1, 11, false));
        assert!(
            matches!(error, ControlError::OtherRoot { slot, .. } if slot == released),
            "{error:?}"
        );
        // An empty root takes it, and the slot change dates nothing.
        let registered = driver.apply(register_root(1, 11, true)).unwrap();
        assert!(matches!(registered, ControlApplied::NodeRegistered(_)));
        assert_eq!(slot_of(&driver, 1), (RootSlot::Bound(root(11)), since_ms));
        // Once bound, the new root's later starts register as it holds
        // state, and the old root stays refused.
        driver.apply(register_root(1, 11, false)).unwrap();
        let error = driver.refused(register_root(1, 1, true));
        assert!(
            matches!(error, ControlError::OtherRoot { slot, .. } if slot == RootSlot::Bound(root(11))),
            "{error:?}"
        );
        // No other node's slot moved.
        assert_eq!(slot_of(&driver, 2).0, RootSlot::Bound(root(2)));
    }
    // A removed node is refused as removed whatever its slot.
    let mut driver = Driver::live(&[1, 2, 3, 4], 1, 3);
    driver.lifecycle(4, LifecycleChange::Decommission).unwrap();
    let error = driver.refused(register_root(4, 41, true));
    assert!(matches!(error, ControlError::NodeRemoved { node: n } if n == node(4)));
}

#[test]
fn stale_root_refuses_a_release_of_another_root() {
    for bootstrapped in [false, true] {
        let mut driver = forming(&[1, 2, 3]);
        if bootstrapped {
            bootstrap(&mut driver, &[1, 2, 3], 3).unwrap();
        }
        let (_, since_ms) = slot_of(&driver, 2);
        let error = driver.refused(release(2, 12));
        assert!(
            matches!(error, ControlError::StaleRoot { node: n, slot } if n == node(2) && slot == RootSlot::Bound(root(2))),
            "{error:?}"
        );
        assert_eq!(error.requeue(), super::Requeue::ConfirmView);
        // The release answers the record, and a repeat changes nothing.
        let record = NodeRecord {
            lifecycle: NodeLifecycle::Active,
            since_ms,
            root: RootSlot::Released(root(2)),
        };
        assert_eq!(
            driver.apply(release(2, 2)),
            Ok(ControlApplied::RootReleased(record))
        );
        let before = driver.state.clone();
        assert_eq!(
            driver.apply(release(2, 2)),
            Ok(ControlApplied::RootReleased(record))
        );
        assert_eq!(driver.state, before);
        let error = driver.refused(release(2, 12));
        assert!(
            matches!(error, ControlError::StaleRoot { slot, .. } if slot == RootSlot::Released(root(2))),
            "{error:?}"
        );
        let error = driver.refused(release(9, 9));
        assert!(matches!(error, ControlError::UnknownNode { node: n } if n == node(9)));
    }
    // Bootstrap copies each registration's slot.
    let mut driver = forming(&[1, 2, 3]);
    driver.apply(release(3, 3)).unwrap();
    bootstrap(&mut driver, &[1, 2, 3], 3).unwrap();
    assert_eq!(slot_of(&driver, 3).0, RootSlot::Released(root(3)));
    assert_eq!(slot_of(&driver, 1).0, RootSlot::Bound(root(1)));
    // A release keeps the lifecycle, and a removed node's is refused.
    // Group 0 is seeded on {1, 2, 3}: node 4 has no role.
    let mut driver = Driver::live(&[1, 2, 3, 4], 1, 3);
    driver.lifecycle(4, LifecycleChange::Cordon).unwrap();
    assert!(matches!(
        driver.apply(release(4, 4)),
        Ok(ControlApplied::RootReleased(NodeRecord {
            lifecycle: NodeLifecycle::Cordoned,
            ..
        }))
    ));
    driver.lifecycle(4, LifecycleChange::Decommission).unwrap();
    assert_eq!(slot_of(&driver, 4).0, RootSlot::Released(root(4)));
    let error = driver.refused(release(4, 4));
    assert!(matches!(error, ControlError::NodeRemoved { node: n } if n == node(4)));
}

#[test]
fn every_error_variant_has_a_unit_test_named_after_it() {
    // One name per variant, in a match that fails to compile when a variant
    // is added. Each names the test of this file that refuses with it: a
    // `fn` whose name starts with it.
    let names: BTreeSet<&str> = sample_errors()
        .into_iter()
        .map(|error| match error {
            ControlError::NotBootstrapped => "not_bootstrapped",
            ControlError::AlreadyBootstrapped => "already_bootstrapped",
            ControlError::FormationCut => "formation_cut",
            ControlError::InvalidBootstrap { .. } => "invalid_bootstrap",
            ControlError::UnknownGroup { .. } => "unknown_group",
            ControlError::UnknownNode { .. } => "unknown_node",
            ControlError::NodeRemoved { .. } => "node_removed",
            ControlError::StalePlacement { .. } => "stale_placement",
            ControlError::GroupNotLive { .. } => "group_not_live",
            ControlError::NotMoving { .. } => "not_moving",
            ControlError::NoChange { .. } => "no_change",
            ControlError::ReplicationFactorChange { .. } => "replication_factor_change",
            ControlError::IneligibleTarget { .. } => "ineligible_target",
            ControlError::RetirementPending { .. } => "retirement_pending",
            ControlError::MoveLimit { .. } => "move_limit",
            ControlError::NotTarget { .. } => "not_target",
            ControlError::MetaVoter { .. } => "meta_voter",
            ControlError::Decommissioning { .. } => "decommissioning",
            ControlError::OtherRoot { .. } => "other_root",
            ControlError::StaleRoot { .. } => "stale_root",
        })
        .collect();
    let source = include_str!("tests.rs");
    for name in names {
        assert!(
            source.contains(&format!("fn {name}_")),
            "no test is named after ControlError's {name} variant"
        );
    }
}

#[test]
fn bootstrap_seeds_every_group_and_activates_every_registered_node() {
    let mut driver = forming(&[1, 2, 3, 4]);
    assert_eq!(
        driver.apply(register(2, 9)),
        Ok(ControlApplied::NodeRegistered(Registered::Forming))
    );
    bootstrap(&mut driver, &[1, 2, 3, 4], 3).unwrap();
    let at = PlacementEpoch::new(meta(1, driver.index));
    let cluster = driver.cluster();
    assert_eq!(cluster.nodes().len(), 4);
    assert!(
        cluster
            .nodes()
            .values()
            .all(|record| record.lifecycle == NodeLifecycle::Active)
    );
    assert_eq!(
        cluster.group(G0),
        Some(&GroupPlacement::Seeded {
            since: at,
            voters: voters(&[1, 2, 3]),
            initializer: node(1),
        })
    );
    assert_eq!(
        cluster.group(G1),
        Some(&GroupPlacement::Seeded {
            since: at,
            voters: voters(&[2, 3, 4]),
            initializer: node(2),
        })
    );
    assert_eq!(cluster.groups().len(), 2);
}

#[test]
fn a_move_retargets_cancels_and_retires_what_left() {
    let mut driver = Driver::live(&[1, 2, 3, 4, 5], 1, 3);
    driver.request_move(G0, &[1, 2, 4]).unwrap();
    assert_eq!(
        driver.cluster().role(G0, node(3)),
        Some(ReplicaRole::Outgoing)
    );
    assert_eq!(
        driver.cluster().role(G0, node(4)),
        Some(ReplicaRole::Incoming)
    );
    assert_eq!(driver.cluster().role(G0, node(1)), Some(ReplicaRole::Voter));
    // Retarget: node 4 becomes an abandoned earlier target.
    driver.request_move(G0, &[1, 2, 5]).unwrap();
    assert_eq!(
        driver.cluster().role(G0, node(4)),
        Some(ReplicaRole::Abandoned)
    );
    // Cancel: back to the source, node 5 abandoned too.
    let accepted = driver.request_move(G0, &[1, 2, 3]).unwrap();
    assert_eq!(accepted, ControlApplied::MoveAccepted {
        since: driver.since(G0)
    });
    let moving = driver.placement(G0).moving().unwrap().clone();
    assert_eq!(moving.abandoned, ids(&[4, 5]));
    assert_eq!(moving.from, voters(&[1, 2, 3]));
    // The cancel converges at once, retiring both abandoned targets.
    let converged = driver.report(G0, &[1, 2, 3]).unwrap();
    assert!(matches!(
        &converged,
        ControlApplied::Converged { retired, .. } if *retired == ids(&[4, 5])
    ));
    let created_at = meta(1, driver.index);
    assert_eq!(
        driver.cluster().retirements().get(&G0),
        Some(&BTreeMap::from([
            (node(4), Retirement { created_at }),
            (node(5), Retirement { created_at }),
        ]))
    );
    assert!(matches!(
        driver.placement(G0),
        GroupPlacement::Live {
            target: Target::Stable { voters: v },
            ..
        } if *v == voters(&[1, 2, 3])
    ));
    // A report of another retirement does nothing.
    assert_eq!(
        driver.apply(ControlCommand::ReportTornDown {
            group: G0,
            node: node(4),
            created_at: meta(9, 9),
        }),
        Ok(ControlApplied::NothingToTearDown)
    );
}

#[test]
fn a_decommissioning_node_is_removed_by_the_converge_that_takes_its_last_role() {
    let mut driver = Driver::live(&[1, 2, 3, 4, 5], 1, 3);
    driver.request_move(G0, &[1, 2, 4]).unwrap();
    driver.lifecycle(4, LifecycleChange::Decommission).unwrap();
    // Node 5 holds nothing and goes at once.
    assert_eq!(
        driver.lifecycle(5, LifecycleChange::Decommission),
        Ok(ControlApplied::LifecycleChanged {
            lifecycle: NodeLifecycle::Removed
        })
    );
    // Cancelling the move leaves node 4 abandoned: still a role.
    driver.request_move(G0, &[1, 2, 3]).unwrap();
    assert_eq!(
        driver.cluster().node(node(4)).unwrap().lifecycle,
        NodeLifecycle::Decommissioning
    );
    let converged = driver.report(G0, &[1, 2, 3]).unwrap();
    assert!(matches!(
        &converged,
        ControlApplied::Converged { retired, removed, .. } if retired.is_empty() && *removed == ids(&[4])
    ));
    // A record's time is when the node entered its lifecycle: node 4 by
    // the converge (`now_ms` 35), node 5 by its decommission (40).
    assert_eq!(
        driver.cluster().node(node(4)),
        Some(&NodeRecord {
            lifecycle: NodeLifecycle::Removed,
            since_ms: 35,
            root: RootSlot::Bound(root(4)),
        })
    );
    assert_eq!(
        driver.cluster().node(node(5)),
        Some(&NodeRecord {
            lifecycle: NodeLifecycle::Removed,
            since_ms: 40,
            root: RootSlot::Bound(root(5)),
        })
    );
    assert!(driver.cluster().retirements().is_empty());
    // Uncordon brings a decommissioning node back only while it has a role.
    let error = driver.refused(ControlCommand::SetNodeLifecycle {
        node: node(4),
        change: LifecycleChange::Uncordon,
        now_ms: 7,
    });
    assert!(matches!(error, ControlError::NodeRemoved { .. }));
}

#[test]
fn a_registration_answers_whether_the_node_has_a_role() {
    let mut driver = Driver::live(&[1, 2, 3, 4], 1, 3);
    let answer = |driver: &mut Driver, id| driver.apply(register(id, 8));
    assert!(matches!(
        answer(&mut driver, 1),
        Ok(ControlApplied::NodeRegistered(Registered::Referenced(_)))
    ));
    assert!(matches!(
        answer(&mut driver, 4),
        Ok(ControlApplied::NodeRegistered(Registered::Unreferenced(_)))
    ));
    assert!(matches!(
        answer(&mut driver, 9),
        Ok(ControlApplied::NodeRegistered(Registered::Unreferenced(record))) if record.lifecycle == NodeLifecycle::Active
    ));
    assert_eq!(driver.cluster().nodes().len(), 5);
}

#[test]
fn uncordon_returns_a_cordoned_or_decommissioning_node_to_active() {
    let mut driver = Driver::live(&[1, 2, 3, 4, 5], 1, 3);
    driver.lifecycle(5, LifecycleChange::Cordon).unwrap();
    assert_eq!(
        driver.lifecycle(5, LifecycleChange::Uncordon),
        Ok(ControlApplied::LifecycleChanged {
            lifecycle: NodeLifecycle::Active
        })
    );
    assert_eq!(
        driver.cluster().node(node(5)).unwrap().lifecycle,
        NodeLifecycle::Active
    );
    // Node 4 is incoming, so it stays decommissioning until uncordoned.
    driver.request_move(G0, &[1, 2, 4]).unwrap();
    driver.lifecycle(4, LifecycleChange::Decommission).unwrap();
    assert_eq!(
        driver.lifecycle(4, LifecycleChange::Uncordon),
        Ok(ControlApplied::LifecycleChanged {
            lifecycle: NodeLifecycle::Active
        })
    );
    assert_eq!(
        driver.cluster().node(node(4)).unwrap().lifecycle,
        NodeLifecycle::Active
    );
    // The aborted decommission removes nothing when the node's role ends.
    driver.request_move(G0, &[1, 2, 3]).unwrap();
    let converged = driver.report(G0, &[1, 2, 3]).unwrap();
    assert!(matches!(
        &converged,
        ControlApplied::Converged { retired, removed, .. } if *retired == ids(&[4]) && removed.is_empty()
    ));
    assert_eq!(
        driver.cluster().node(node(4)).unwrap().lifecycle,
        NodeLifecycle::Active
    );
}

#[test]
fn a_known_node_registering_again_keeps_its_cordon_or_decommission() {
    let mut driver = Driver::live(&[1, 2, 3, 4, 5], 1, 3);
    driver.request_move(G0, &[1, 2, 4]).unwrap();
    driver.lifecycle(4, LifecycleChange::Decommission).unwrap();
    driver.lifecycle(5, LifecycleChange::Cordon).unwrap();
    let before = driver.state.clone();
    // Nodes register at every start, which must not undo either change.
    assert!(matches!(
        driver.apply(register(4, 50)),
        Ok(ControlApplied::NodeRegistered(Registered::Referenced(record)))
            if record.lifecycle == NodeLifecycle::Decommissioning
    ));
    assert!(matches!(
        driver.apply(register(5, 50)),
        Ok(ControlApplied::NodeRegistered(Registered::Unreferenced(record)))
            if record.lifecycle == NodeLifecycle::Cordoned
    ));
    assert_eq!(driver.state, before);
}

#[test]
fn a_history_event_drops_every_retirement_and_moves_every_key() {
    let mut driver = Driver::live(&[1, 2, 3, 4], 2, 3);
    driver.request_move(G0, &[1, 2, 4]).unwrap();
    driver.report(G0, &[1, 2, 4]).unwrap();
    driver.request_move(G1, &[1, 3, 4]).unwrap();
    let before = driver.cluster().clone();
    let answer = driver.history(HistoryCause::Plan);
    assert_eq!(answer, HistoryBegun::Rebased {
        dropped: BTreeMap::from([(G0, ids(&[3]))])
    });
    let at = PlacementEpoch::new(meta(driver.term, driver.index));
    let after = driver.cluster();
    assert!(after.retirements().is_empty());
    for (group, placement) in after.groups().iter() {
        assert_eq!(placement.since(), at);
        assert_eq!(
            placement.target_voters(),
            before.group(group).unwrap().target_voters()
        );
    }
    assert_eq!(after.nodes(), before.nodes());
    assert_eq!(after.params(), before.params());
    // The old key no longer moves the group.
    let error = driver.refused(ControlCommand::ReportConverged {
        group: G1,
        since: before.group(G1).unwrap().since(),
        voters: voters(&[1, 3, 4]),
        membership: membership(4, 4),
        now_ms: 6,
    });
    assert!(matches!(error, ControlError::StalePlacement { .. }));
}

#[test]
fn every_root_round_trips_through_messagepack_and_json() {
    fn round_trip<T>(value: &T)
    where T: serde::Serialize + serde::de::DeserializeOwned + PartialEq + std::fmt::Debug {
        let bytes = ursula_stream::msgpack::encode(value).unwrap();
        let decoded: T = ursula_stream::msgpack::decode(&bytes).unwrap();
        assert_eq!(&decoded, value);
        assert_eq!(ursula_stream::msgpack::encode(&decoded).unwrap(), bytes);
        let json = serde_json::to_string(value).unwrap();
        let decoded: T = serde_json::from_str(&json).unwrap();
        assert_eq!(&decoded, value, "{json}");
    }
    for state in sample_states() {
        round_trip(&state);
    }
    for command in sample_commands() {
        round_trip(&command);
    }
    for response in sample_responses() {
        round_trip(&response);
    }
    for answer in sample_history_answers() {
        round_trip(&answer);
    }
    for cause in sample_history_causes() {
        round_trip(&cause);
    }
    round_trip(&sample_genesis_claim());
    let ControlPlaneState::Bootstrapped(cluster) = sample_state() else {
        panic!("the sample state is bootstrapped");
    };
    round_trip(&ClusterView::Bootstrapped(cluster.clone()));
    round_trip(&ClusterView::MetaGenesis {
        unanswered: ids(&[2]),
        genesis_mismatch: BTreeMap::from([(node(3), sample_genesis_claim())]),
    });
    round_trip(&ClusterView::Registering {
        registered: BTreeMap::from([
            (node(1), RootSlot::Bound(root(1))),
            (node(4), RootSlot::Released(root(4))),
        ]),
        waiting_for: ids(&[2, 3]),
    });
    // A root above 2^53 travels as its hex string.
    let high = ursula_proto::placement::RootId::new(
        std::num::NonZeroU64::new(0x9e37_79b9_7f4a_7c15).unwrap(),
    );
    round_trip(&ReplaceDiskRequest { expect_root: high });
    assert_eq!(
        serde_json::to_string(&ReplaceDiskRequest { expect_root: high }).unwrap(),
        r#"{"expect_root":"9e3779b97f4a7c15"}"#
    );
    assert_eq!(
        serde_json::to_string(&RegistrationStatus::RootRefused {
            slot: RootSlot::Bound(high)
        })
        .unwrap(),
        r#"{"root_refused":{"slot":{"bound":"9e3779b97f4a7c15"}}}"#
    );
    round_trip(&ClusterView::MetaCatchingUp);
    round_trip(&ClusterView::FormationCut);
    round_trip(&MoveRequest {
        expected: PlacementEpoch::new(meta(2, 3)),
        to: voters(&[4, 5, 6]),
    });
    round_trip(&MoveLimitRequest {
        limit: MoveLimit::new(2),
    });
    for error in sample_errors() {
        round_trip(&PlacementApiError {
            reason: PlacementApiReason::Refused(error),
            write_effect: ursula_proto::placement::WriteEffect::NotApplied,
            current: cluster.group(RaftGroupId(2)).cloned(),
            message: "refused".to_owned(),
        });
    }
    round_trip(&MetaLeaderReport {
        leader: node(2),
        leader_since_ms: 71,
        eligibility: BTreeMap::from([
            (node(1), NodeEligibility::Eligible),
            (node(3), NodeEligibility::Unknown),
            (node(4), NodeEligibility::DiskPressure),
        ]),
        decommission_blocked: vec![
            BlockedMove {
                group: RaftGroupId(3),
                reason: BlockReason::NoEligibleTarget,
            },
            BlockedMove {
                group: RaftGroupId(4),
                reason: BlockReason::TargetsUnderDiskPressure,
            },
            BlockedMove {
                group: RaftGroupId(5),
                reason: BlockReason::GroupNotLive,
            },
        ],
        sweep_pending: BTreeMap::from([(
            node(6),
            BTreeSet::from([RaftGroupId(1), RaftGroupId(7)]),
        )]),
        last_bootstrap_refusal: Some(ControlError::InvalidBootstrap {
            problem: BootstrapProblem::ReplicationFactorAboveNodes,
        }),
        oldest_move_age_ms: Some(72),
    });
    round_trip(&MetaLeaderReport {
        leader: node(1),
        leader_since_ms: 0,
        eligibility: BTreeMap::new(),
        decommission_blocked: Vec::new(),
        sweep_pending: BTreeMap::new(),
        last_bootstrap_refusal: None,
        oldest_move_age_ms: None,
    });
    for status in sample_placement_statuses() {
        round_trip(&status);
    }
    for kind in [
        MetaDamageKind::Journal,
        MetaDamageKind::Pointer,
        MetaDamageKind::Restore,
        MetaDamageKind::Start,
        MetaDamageKind::CommitScreen,
    ] {
        // A new kind fails to compile here until it is sampled.
        match kind {
            MetaDamageKind::Journal
            | MetaDamageKind::Pointer
            | MetaDamageKind::Restore
            | MetaDamageKind::Start
            | MetaDamageKind::CommitScreen => round_trip(&kind),
        }
    }
    for reason in [
        PlacementApiReason::NoLeader,
        PlacementApiReason::ForwardFailed { leader: node(2) },
        PlacementApiReason::LeaderRefused { leader: node(3) },
        PlacementApiReason::Stopped,
        PlacementApiReason::Unconfirmed,
    ] {
        round_trip(&PlacementApiError {
            reason,
            write_effect: ursula_proto::placement::WriteEffect::MayHaveApplied,
            current: None,
            message: String::new(),
        });
    }
}

/// Every variant of the placement status's enums, sampled through
/// exhaustive matches, so a new variant fails to compile until it is.
fn sample_placement_statuses() -> Vec<PlacementStatus> {
    let ControlPlaneState::Bootstrapped(cluster) = sample_state() else {
        panic!("the sample state is bootstrapped");
    };
    let record = NodeRecord {
        lifecycle: NodeLifecycle::Cordoned,
        since_ms: 81,
        root: RootSlot::Bound(super::samples::wide_root(0x81)),
    };
    let metas = [
        MetaReplicaStatus::Running {
            role: MetaRole::Voter,
            leader: Some(node(2)),
        },
        MetaReplicaStatus::Running {
            role: MetaRole::Learner,
            leader: None,
        },
        MetaReplicaStatus::Running {
            role: MetaRole::NotMember,
            leader: Some(node(4)),
        },
        MetaReplicaStatus::Damaged {
            kind: MetaDamageKind::CommitScreen,
            message: "refused for good".to_owned(),
        },
        MetaReplicaStatus::Absent,
    ];
    let registrations = [
        RegistrationStatus::Pending,
        RegistrationStatus::Answered(Registered::Forming),
        RegistrationStatus::Answered(Registered::Referenced(record)),
        RegistrationStatus::Answered(Registered::Unreferenced(record)),
        RegistrationStatus::RootRefused {
            slot: RootSlot::Released(root(6)),
        },
        RegistrationStatus::Refused(ControlError::FormationCut),
        RegistrationStatus::Removed,
    ];
    let views = [
        ClusterView::MetaGenesis {
            unanswered: ids(&[5]),
            genesis_mismatch: BTreeMap::from([(node(6), sample_genesis_claim())]),
        },
        ClusterView::MetaCatchingUp,
        ClusterView::Registering {
            registered: BTreeMap::from([(node(1), RootSlot::Bound(root(1)))]),
            waiting_for: ids(&[3]),
        },
        ClusterView::FormationCut,
        ClusterView::Bootstrapped(cluster),
    ];
    let mut statuses = Vec::new();
    let mut paired_metas = metas.iter().cycle();
    let mut paired_registrations = registrations.iter().cycle();
    let mut applied = true;
    for view in views {
        match &view {
            ClusterView::MetaGenesis { .. }
            | ClusterView::MetaCatchingUp
            | ClusterView::Registering { .. }
            | ClusterView::FormationCut
            | ClusterView::Bootstrapped(_) => {}
        }
        let replica = paired_metas.next().unwrap().clone();
        match &replica {
            MetaReplicaStatus::Running {
                role: MetaRole::Voter | MetaRole::Learner | MetaRole::NotMember,
                ..
            }
            | MetaReplicaStatus::Damaged { .. }
            | MetaReplicaStatus::Absent => {}
        }
        let registration = paired_registrations.next().unwrap().clone();
        match &registration {
            RegistrationStatus::Pending
            | RegistrationStatus::Answered(
                Registered::Forming | Registered::Referenced(_) | Registered::Unreferenced(_),
            )
            | RegistrationStatus::RootRefused { .. }
            | RegistrationStatus::Refused(_)
            | RegistrationStatus::Removed => {}
        }
        statuses.push(PlacementStatus {
            node: node(7),
            view,
            applied: applied.then(|| meta(3, 9)),
            meta: replica,
            registration,
            last_bootstrap_refusal: (!applied).then_some(ControlError::AlreadyBootstrapped),
        });
        applied = !applied;
    }
    // The variants the views did not pair with.
    for replica in metas {
        statuses.push(PlacementStatus {
            node: node(8),
            view: ClusterView::MetaCatchingUp,
            applied: None,
            meta: replica,
            registration: RegistrationStatus::Pending,
            last_bootstrap_refusal: None,
        });
    }
    for registration in registrations {
        statuses.push(PlacementStatus {
            node: node(9),
            view: ClusterView::FormationCut,
            applied: Some(meta(4, 10)),
            meta: MetaReplicaStatus::Absent,
            registration,
            last_bootstrap_refusal: None,
        });
    }
    statuses
}

#[test]
fn a_placement_status_refuses_a_missing_option() {
    let status = PlacementStatus {
        node: node(1),
        view: ClusterView::MetaCatchingUp,
        applied: None,
        meta: MetaReplicaStatus::Running {
            role: MetaRole::Voter,
            leader: None,
        },
        registration: RegistrationStatus::Pending,
        last_bootstrap_refusal: None,
    };
    let json = serde_json::to_value(&status).unwrap();
    // An `Option` field is required, also when `null`.
    for path in [&["applied"][..], &["last_bootstrap_refusal"], &[
        "meta", "running", "leader",
    ]] {
        let mut missing = json.clone();
        let (last, parents) = path.split_last().unwrap();
        let mut object = &mut missing;
        for key in parents {
            object = object.get_mut(*key).unwrap();
        }
        assert!(object.as_object_mut().unwrap().remove(*last).is_some());
        serde_json::from_value::<PlacementStatus>(missing)
            .expect_err(&format!("a missing `{}` does not decode", path.join(".")));
    }
}

#[test]
fn planner_enums_in_admin_json_decode_an_unknown_variant() {
    assert_eq!(
        serde_json::from_str::<NodeEligibility>(r#""draining""#).unwrap(),
        NodeEligibility::Unknown
    );
    assert_eq!(
        serde_json::from_str::<BlockReason>(r#""move_limit""#).unwrap(),
        BlockReason::Unknown
    );
    let report: MetaLeaderReport = serde_json::from_str(
        r#"{"leader":2,"leader_since_ms":7,"eligibility":{"3":"draining","4":"eligible"},"decommission_blocked":[{"group":5,"reason":"move_limit"}],"sweep_pending":{},"last_bootstrap_refusal":null,"oldest_move_age_ms":null}"#,
    )
    .unwrap();
    assert_eq!(
        report.eligibility,
        BTreeMap::from([
            (node(3), NodeEligibility::Unknown),
            (node(4), NodeEligibility::Eligible),
        ])
    );
    assert_eq!(report.decommission_blocked, vec![BlockedMove {
        group: RaftGroupId(5),
        reason: BlockReason::Unknown,
    }]);
    // An `Option` field is required, also when `null`.
    serde_json::from_str::<MetaLeaderReport>(
        r#"{"leader":2,"leader_since_ms":7,"eligibility":{},"decommission_blocked":[],"sweep_pending":{},"last_bootstrap_refusal":null}"#,
    )
    .expect_err("a missing `oldest_move_age_ms` does not decode");
}

#[test]
fn placements_encode_as_the_plain_map() {
    let ControlPlaneState::Bootstrapped(cluster) = sample_state() else {
        panic!("the sample state is bootstrapped");
    };
    let placements = cluster.groups();
    let map = &placements.0;
    assert_eq!(
        ursula_stream::msgpack::encode(placements).unwrap(),
        ursula_stream::msgpack::encode(map).unwrap()
    );
    assert_eq!(
        serde_json::to_string(placements).unwrap(),
        serde_json::to_string(map).unwrap()
    );
}

#[test]
fn a_set_of_voters_is_never_empty_in_the_encoding() {
    let empty: BTreeSet<u64> = BTreeSet::new();
    let bytes = ursula_stream::msgpack::encode(&empty).unwrap();
    ursula_stream::msgpack::decode::<ursula_proto::placement::VoterSet>(&bytes)
        .expect_err("an empty set does not decode");
}

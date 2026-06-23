use std::collections::BTreeMap;
use std::collections::BTreeSet;

use ursula_shard::RaftGroupId;

use crate::ClusterNode;
use crate::GroupPlacementView;
use crate::NodeState;
use crate::PlacementNode;

fn set(values: impl IntoIterator<Item = u64>) -> BTreeSet<u64> {
    values.into_iter().collect()
}

fn placement_node(node_id: u64, state: NodeState) -> PlacementNode {
    PlacementNode {
        node_id,
        client_url: format!("http://node{node_id}:4491"),
        cluster_url: format!("http://node{node_id}:4492"),
        state,
    }
}

#[test]
fn placement_view_distinguishes_hosting_from_client_traffic() {
    let view = GroupPlacementView {
        raft_group_id: RaftGroupId(1),
        voters: set([1, 2]),
        learners: set([3]),
        draining: set([2]),
        epoch: 7,
        nodes: BTreeMap::from([
            (1, placement_node(1, NodeState::Active)),
            (2, placement_node(2, NodeState::Active)),
            (3, placement_node(3, NodeState::Active)),
        ]),
    };

    assert!(view.hosts(1));
    assert!(view.hosts(3));
    assert!(!view.hosts(4));
    assert!(view.serves_client_traffic(1));
    assert!(!view.serves_client_traffic(2));
    assert!(!view.serves_client_traffic(3));
}

#[test]
fn placement_view_selects_active_non_draining_voter_for_redirect() {
    let view = GroupPlacementView {
        raft_group_id: RaftGroupId(1),
        voters: set([1, 2, 3]),
        learners: BTreeSet::new(),
        draining: set([2]),
        epoch: 1,
        nodes: BTreeMap::from([
            (1, placement_node(1, NodeState::Active)),
            (2, placement_node(2, NodeState::Active)),
            (3, placement_node(3, NodeState::Disabled)),
        ]),
    };

    assert_eq!(
        view.active_voter_client_url(Some(1)),
        Some((1, "http://node1:4491".to_owned()))
    );
    assert_eq!(
        view.active_voter_client_url(None),
        Some((1, "http://node1:4491".to_owned()))
    );
}

#[test]
fn cluster_node_active_state_is_migration_eligible() {
    let node = ClusterNode {
        node_id: 5,
        client_url: "http://node5:4491".to_owned(),
        cluster_url: "http://node5:4492".to_owned(),
        state: NodeState::Active,
        registered_at_ms: 10,
        updated_at_ms: 10,
        labels: BTreeMap::new(),
    };

    assert!(node.state.is_migration_eligible());
    assert!(!NodeState::Draining.is_migration_eligible());
    assert!(!NodeState::Disabled.is_migration_eligible());
    assert!(!NodeState::Removed.is_migration_eligible());
}

use crate::ControlCommand;
use crate::ControlPlaneState;
use crate::ControlResponse;

#[test]
fn control_command_display_names_variants() {
    let cases = [
        (
            ControlCommand::RegisterNode {
                node_id: 1,
                client_url: "http://node1:4491".to_owned(),
                cluster_url: "http://node1:4492".to_owned(),
                labels: BTreeMap::new(),
                now_ms: 10,
            },
            "register_node",
        ),
        (
            ControlCommand::SetNodeState {
                node_id: 1,
                state: NodeState::Active,
                now_ms: 10,
            },
            "set_node_state",
        ),
        (
            ControlCommand::SeedPlacement {
                raft_group_id: RaftGroupId(1),
                voters: set([1, 2, 3]),
                now_ms: 10,
            },
            "seed_placement",
        ),
        (
            ControlCommand::BeginMigration {
                raft_group_id: RaftGroupId(1),
                target_voters: set([2, 3, 4]),
                retain_removed: true,
                now_ms: 10,
            },
            "begin_migration",
        ),
        (
            ControlCommand::AdvanceMigration {
                migration_id: 1,
                phase: crate::MigrationPhase::AddingLearners,
                now_ms: 10,
            },
            "advance_migration",
        ),
        (
            ControlCommand::SetLearnerStatus {
                migration_id: 1,
                node_id: 4,
                status: crate::LearnerStatus::CaughtUp,
                now_ms: 10,
            },
            "set_learner_status",
        ),
        (
            ControlCommand::RecordMigrationError {
                migration_id: 1,
                error: "timeout".to_owned(),
                now_ms: 10,
            },
            "record_migration_error",
        ),
        (
            ControlCommand::CommitPlacement {
                raft_group_id: RaftGroupId(1),
                voters: set([2, 3, 4]),
                learners: set([1]),
                draining: set([1]),
                now_ms: 10,
            },
            "commit_placement",
        ),
        (
            ControlCommand::FinishMigration {
                migration_id: 1,
                success: true,
                now_ms: 10,
            },
            "finish_migration",
        ),
        (
            ControlCommand::EvictLearner {
                raft_group_id: RaftGroupId(1),
                node_id: 1,
                now_ms: 10,
            },
            "evict_learner",
        ),
    ];

    for (command, expected) in cases {
        assert_eq!(command.to_string(), expected);
    }
}

#[test]
fn control_response_display_names_variants() {
    let cases = [
        (ControlResponse::Ok, "ok"),
        (
            ControlResponse::MigrationStarted { migration_id: 1 },
            "migration_started",
        ),
        (
            ControlResponse::Rejected {
                reason: "invalid".to_owned(),
            },
            "rejected",
        ),
    ];

    for (response, expected) in cases {
        assert_eq!(response.to_string(), expected);
    }
}

#[test]
fn register_node_command_persists_addresses_and_active_state() {
    let mut state = ControlPlaneState::default();

    let response = state.apply(ControlCommand::RegisterNode {
        node_id: 5,
        client_url: "http://node5:4491/".to_owned(),
        cluster_url: "http://node5:4492/".to_owned(),
        labels: BTreeMap::from([("az".to_owned(), "a".to_owned())]),
        now_ms: 10,
    });

    assert_eq!(response, ControlResponse::Ok);
    let node = state.nodes.get(&5).expect("node registered");
    assert_eq!(node.client_url, "http://node5:4491");
    assert_eq!(node.cluster_url, "http://node5:4492");
    assert_eq!(node.state, NodeState::Active);
    assert_eq!(node.registered_at_ms, 10);
    assert_eq!(node.updated_at_ms, 10);
    assert_eq!(node.labels.get("az").map(String::as_str), Some("a"));
}

#[test]
fn register_node_command_preserves_non_removed_state_on_update() {
    let mut state = ControlPlaneState::default();

    assert_eq!(
        state.apply(ControlCommand::RegisterNode {
            node_id: 5,
            client_url: "http://node5:4491".to_owned(),
            cluster_url: "http://node5:4492".to_owned(),
            labels: BTreeMap::new(),
            now_ms: 10,
        }),
        ControlResponse::Ok
    );
    assert_eq!(
        state.apply(ControlCommand::SetNodeState {
            node_id: 5,
            state: NodeState::Draining,
            now_ms: 20,
        }),
        ControlResponse::Ok
    );

    assert_eq!(
        state.apply(ControlCommand::RegisterNode {
            node_id: 5,
            client_url: "http://node5-new:4491".to_owned(),
            cluster_url: "http://node5-new:4492".to_owned(),
            labels: BTreeMap::new(),
            now_ms: 30,
        }),
        ControlResponse::Ok
    );

    let node = state.nodes.get(&5).expect("node exists");
    assert_eq!(node.client_url, "http://node5-new:4491");
    assert_eq!(node.cluster_url, "http://node5-new:4492");
    assert_eq!(node.state, NodeState::Draining);
    assert_eq!(node.registered_at_ms, 10);
    assert_eq!(node.updated_at_ms, 30);
}

#[test]
fn seed_placement_records_initial_voters_without_bumping_epoch() {
    let mut state = ControlPlaneState::default();

    let response = state.apply(ControlCommand::SeedPlacement {
        raft_group_id: RaftGroupId(1),
        voters: set([1, 2, 3]),
        now_ms: 20,
    });

    assert_eq!(response, ControlResponse::Ok);
    let placement = state.placements.get(&RaftGroupId(1)).expect("placement");
    assert_eq!(placement.voters, set([1, 2, 3]));
    assert_eq!(placement.learners, BTreeSet::new());
    assert_eq!(placement.draining, BTreeSet::new());
    assert_eq!(placement.epoch, 0);
    assert_eq!(placement.updated_at_ms, 20);
}

#[test]
fn placement_view_from_state_includes_voters_learners_and_draining_nodes() {
    let mut state = ControlPlaneState::default();
    for node_id in 1..=3 {
        state.apply(ControlCommand::RegisterNode {
            node_id,
            client_url: format!("http://node{node_id}:4491"),
            cluster_url: format!("http://node{node_id}:4492"),
            labels: BTreeMap::new(),
            now_ms: 10,
        });
    }
    state.apply(ControlCommand::CommitPlacement {
        raft_group_id: RaftGroupId(1),
        voters: set([1]),
        learners: set([2]),
        draining: set([3]),
        now_ms: 30,
    });

    let view = state.placement_view(RaftGroupId(1)).expect("view exists");

    assert_eq!(view.voters, set([1]));
    assert_eq!(view.learners, set([2]));
    assert_eq!(view.draining, set([3]));
    assert_eq!(view.nodes.len(), 3);
}

fn register_active_nodes(state: &mut ControlPlaneState, nodes: impl IntoIterator<Item = u64>) {
    for node_id in nodes {
        assert_eq!(
            state.apply(ControlCommand::RegisterNode {
                node_id,
                client_url: format!("http://node{node_id}:4491"),
                cluster_url: format!("http://node{node_id}:4492"),
                labels: BTreeMap::new(),
                now_ms: 10,
            }),
            ControlResponse::Ok
        );
    }
}

#[test]
fn control_plane_lifecycle_registers_migrates_commits_and_finishes_group() {
    let mut state = ControlPlaneState::new(crate::MetaConfig::default());

    register_active_nodes(&mut state, 1..=4);
    assert_eq!(
        state.apply(ControlCommand::SeedPlacement {
            raft_group_id: RaftGroupId(7),
            voters: set([1, 2, 3]),
            now_ms: 20,
        }),
        ControlResponse::Ok
    );

    assert_eq!(
        state.apply(ControlCommand::BeginMigration {
            raft_group_id: RaftGroupId(7),
            target_voters: set([2, 3, 4]),
            retain_removed: true,
            now_ms: 30,
        }),
        ControlResponse::MigrationStarted { migration_id: 1 }
    );
    let migration = state.active_migration().expect("active migration");
    assert_eq!(migration.from_voters, set([1, 2, 3]));
    assert_eq!(migration.target_voters, set([2, 3, 4]));
    assert_eq!(migration.added_nodes, set([4]));
    assert_eq!(migration.removed_voters, set([1]));

    assert_eq!(
        state.apply(ControlCommand::CommitPlacement {
            raft_group_id: RaftGroupId(7),
            voters: set([2, 3, 4]),
            learners: set([1]),
            draining: set([1]),
            now_ms: 40,
        }),
        ControlResponse::Ok
    );
    assert_eq!(
        state.apply(ControlCommand::FinishMigration {
            migration_id: 1,
            success: true,
            now_ms: 50,
        }),
        ControlResponse::Ok
    );

    assert_eq!(state.active_migration, None);
    let placement = state.placements.get(&RaftGroupId(7)).expect("placement");
    assert_eq!(placement.voters, set([2, 3, 4]));
    assert_eq!(placement.learners, set([1]));
    assert_eq!(placement.draining, set([1]));
    assert_eq!(placement.epoch, 1);

    let migration = state.migrations.get(&1).expect("migration history");
    assert_eq!(migration.phase, crate::MigrationPhase::Succeeded);
    assert_eq!(migration.updated_at_ms, 50);
}

#[test]
fn begin_migration_records_added_and_removed_nodes() {
    let mut state = ControlPlaneState::new(crate::MetaConfig::default());
    register_active_nodes(&mut state, 1..=5);
    state.apply(ControlCommand::SeedPlacement {
        raft_group_id: RaftGroupId(1),
        voters: set([1, 2, 3]),
        now_ms: 20,
    });

    let response = state.apply(ControlCommand::BeginMigration {
        raft_group_id: RaftGroupId(1),
        target_voters: set([2, 4, 5]),
        retain_removed: true,
        now_ms: 30,
    });

    assert_eq!(response, ControlResponse::MigrationStarted {
        migration_id: 1
    });
    assert_eq!(state.active_migration, Some(1));
    let migration = state.migrations.get(&1).expect("migration");
    assert_eq!(migration.from_voters, set([1, 2, 3]));
    assert_eq!(migration.target_voters, set([2, 4, 5]));
    assert_eq!(migration.added_nodes, set([4, 5]));
    assert_eq!(migration.removed_voters, set([1, 3]));
    assert_eq!(migration.phase, crate::MigrationPhase::Validating);
    assert!(migration.retain_removed);
}

#[test]
fn migration_lock_rejects_second_running_migration() {
    let mut state = ControlPlaneState::new(crate::MetaConfig::default());
    register_active_nodes(&mut state, 1..=4);
    state.apply(ControlCommand::SeedPlacement {
        raft_group_id: RaftGroupId(1),
        voters: set([1, 2, 3]),
        now_ms: 20,
    });
    state.apply(ControlCommand::SeedPlacement {
        raft_group_id: RaftGroupId(2),
        voters: set([1, 2, 3]),
        now_ms: 20,
    });
    state.apply(ControlCommand::BeginMigration {
        raft_group_id: RaftGroupId(1),
        target_voters: set([2, 3, 4]),
        retain_removed: true,
        now_ms: 30,
    });

    let response = state.apply(ControlCommand::BeginMigration {
        raft_group_id: RaftGroupId(2),
        target_voters: set([2, 3, 4]),
        retain_removed: true,
        now_ms: 31,
    });

    assert_eq!(response, ControlResponse::Rejected {
        reason: "migration 1 is already running".to_owned(),
    });
}

#[test]
fn finish_migration_releases_lock_and_commit_records_retained_learners() {
    let mut state = ControlPlaneState::new(crate::MetaConfig::default());
    register_active_nodes(&mut state, 1..=4);
    state.apply(ControlCommand::SeedPlacement {
        raft_group_id: RaftGroupId(1),
        voters: set([1, 2, 3]),
        now_ms: 20,
    });
    state.apply(ControlCommand::BeginMigration {
        raft_group_id: RaftGroupId(1),
        target_voters: set([2, 3, 4]),
        retain_removed: true,
        now_ms: 30,
    });

    assert_eq!(
        state.apply(ControlCommand::CommitPlacement {
            raft_group_id: RaftGroupId(1),
            voters: set([2, 3, 4]),
            learners: set([1]),
            draining: set([1]),
            now_ms: 40,
        }),
        ControlResponse::Ok
    );
    assert_eq!(
        state.apply(ControlCommand::FinishMigration {
            migration_id: 1,
            success: true,
            now_ms: 41,
        }),
        ControlResponse::Ok
    );

    let placement = state.placements.get(&RaftGroupId(1)).expect("placement");
    assert_eq!(placement.voters, set([2, 3, 4]));
    assert_eq!(placement.learners, set([1]));
    assert_eq!(placement.draining, set([1]));
    assert_eq!(placement.epoch, 1);
    assert_eq!(state.active_migration, None);
    assert_eq!(
        state.migrations.get(&1).expect("migration").phase,
        crate::MigrationPhase::Succeeded
    );
}

#[test]
fn advance_migration_rejects_terminal_phases_so_finish_releases_lock() {
    let mut state = ControlPlaneState::new(crate::MetaConfig::default());
    register_active_nodes(&mut state, 1..=4);
    state.apply(ControlCommand::SeedPlacement {
        raft_group_id: RaftGroupId(1),
        voters: set([1, 2, 3]),
        now_ms: 20,
    });
    state.apply(ControlCommand::BeginMigration {
        raft_group_id: RaftGroupId(1),
        target_voters: set([2, 3, 4]),
        retain_removed: true,
        now_ms: 30,
    });

    assert_eq!(
        state.apply(ControlCommand::AdvanceMigration {
            migration_id: 1,
            phase: crate::MigrationPhase::Succeeded,
            now_ms: 40,
        }),
        ControlResponse::Rejected {
            reason: "migration 1 must finish through FinishMigration".to_owned(),
        }
    );
    assert_eq!(state.active_migration, Some(1));
    assert_eq!(
        state.migrations.get(&1).expect("migration").phase,
        crate::MigrationPhase::Validating
    );
}

#[test]
fn learner_status_rejects_nodes_that_were_not_added_as_learners() {
    let mut state = ControlPlaneState::new(crate::MetaConfig::default());
    register_active_nodes(&mut state, 1..=4);
    state.apply(ControlCommand::SeedPlacement {
        raft_group_id: RaftGroupId(1),
        voters: set([1, 2, 3]),
        now_ms: 20,
    });
    state.apply(ControlCommand::BeginMigration {
        raft_group_id: RaftGroupId(1),
        target_voters: set([2, 3, 4]),
        retain_removed: true,
        now_ms: 30,
    });

    assert_eq!(
        state.apply(ControlCommand::SetLearnerStatus {
            migration_id: 1,
            node_id: 2,
            status: crate::LearnerStatus::CaughtUp,
            now_ms: 40,
        }),
        ControlResponse::Rejected {
            reason: "node 2 is not an added learner for migration 1".to_owned(),
        }
    );
    assert_eq!(
        state.apply(ControlCommand::SetLearnerStatus {
            migration_id: 1,
            node_id: 4,
            status: crate::LearnerStatus::CaughtUp,
            now_ms: 41,
        }),
        ControlResponse::Ok
    );
}

#[test]
fn finish_migration_rejects_inactive_history_records() {
    let mut state = ControlPlaneState::new(crate::MetaConfig::default());
    register_active_nodes(&mut state, 1..=4);
    state.apply(ControlCommand::SeedPlacement {
        raft_group_id: RaftGroupId(1),
        voters: set([1, 2, 3]),
        now_ms: 20,
    });
    state.apply(ControlCommand::SeedPlacement {
        raft_group_id: RaftGroupId(2),
        voters: set([1, 2, 3]),
        now_ms: 20,
    });
    state.apply(ControlCommand::BeginMigration {
        raft_group_id: RaftGroupId(1),
        target_voters: set([2, 3, 4]),
        retain_removed: true,
        now_ms: 30,
    });
    assert_eq!(
        state.apply(ControlCommand::FinishMigration {
            migration_id: 1,
            success: true,
            now_ms: 40,
        }),
        ControlResponse::Ok
    );
    state.apply(ControlCommand::BeginMigration {
        raft_group_id: RaftGroupId(2),
        target_voters: set([1, 3, 4]),
        retain_removed: true,
        now_ms: 50,
    });

    assert_eq!(
        state.apply(ControlCommand::FinishMigration {
            migration_id: 1,
            success: false,
            now_ms: 60,
        }),
        ControlResponse::Rejected {
            reason: "migration 1 is not active".to_owned(),
        }
    );
    assert_eq!(
        state.migrations.get(&1).expect("migration").phase,
        crate::MigrationPhase::Succeeded
    );
    assert_eq!(state.active_migration, Some(2));
}

#[test]
fn commit_placement_rejects_inconsistent_node_sets() {
    let mut state = ControlPlaneState::new(crate::MetaConfig::default());
    register_active_nodes(&mut state, [1, 2]);

    assert_eq!(
        state.apply(ControlCommand::CommitPlacement {
            raft_group_id: RaftGroupId(1),
            voters: set([1]),
            learners: set([1]),
            draining: BTreeSet::new(),
            now_ms: 20,
        }),
        ControlResponse::Rejected {
            reason: "node 1 cannot be both voter and learner".to_owned(),
        }
    );
    assert_eq!(
        state.apply(ControlCommand::CommitPlacement {
            raft_group_id: RaftGroupId(1),
            voters: set([1]),
            learners: BTreeSet::new(),
            draining: set([9]),
            now_ms: 21,
        }),
        ControlResponse::Rejected {
            reason: "draining node 9 is not registered".to_owned(),
        }
    );
    assert_eq!(
        state.apply(ControlCommand::CommitPlacement {
            raft_group_id: RaftGroupId(1),
            voters: set([1, 9]),
            learners: BTreeSet::new(),
            draining: BTreeSet::new(),
            now_ms: 22,
        }),
        ControlResponse::Rejected {
            reason: "voter node 9 is not registered".to_owned(),
        }
    );
}

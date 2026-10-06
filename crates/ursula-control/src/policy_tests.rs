use std::collections::BTreeMap;
use std::collections::BTreeSet;

use ursula_shard::RaftGroupId;

use crate::ControlCommand;
use crate::ControlPlaneState;
use crate::ControlResponse;
use crate::GroupPlacementPolicy;
use crate::GroupPolicyOverride;
use crate::MigrationPhase;
use crate::NodeState;
use crate::PlacementPolicy;
use crate::ReplicationFactor;

fn voters(ids: &[u64]) -> BTreeSet<u64> {
    ids.iter().copied().collect()
}

fn existing() -> ControlPlaneState {
    let mut state = ControlPlaneState::default();
    for (id, zone) in [(1, "a"), (2, "b"), (3, "c"), (4, "a"), (5, "b"), (6, "c")] {
        assert_eq!(
            state.apply(ControlCommand::RegisterNode {
                node_id: id,
                client_url: format!("http://node{id}:4437"),
                cluster_url: format!("http://node{id}:4439"),
                labels: BTreeMap::from([("zone".to_owned(), zone.to_owned())]),
                now_ms: 1,
            }),
            ControlResponse::Ok
        );
    }
    assert_eq!(
        state.apply(ControlCommand::SeedPlacement {
            raft_group_id: RaftGroupId(0),
            voters: voters(&[1, 2, 3]),
            now_ms: 2,
        }),
        ControlResponse::Ok
    );
    state
}

fn adopt(policy: PlacementPolicy, group_count: u32) -> ControlCommand {
    ControlCommand::AdoptPlacementPolicy {
        policy,
        group_count,
        now_ms: 3,
    }
}

fn managed() -> ControlPlaneState {
    let mut state = existing();
    assert_eq!(
        state.apply(adopt(PlacementPolicy::default(), 1)),
        ControlResponse::Ok
    );
    state
}

fn rejected_unchanged(state: &mut ControlPlaneState, command: ControlCommand) {
    let before = state.clone();
    assert!(state.apply(command).is_rejected());
    assert_eq!(
        *state, before,
        "a rejection must not partially change control state"
    );
}

fn begin(target: &[u64]) -> ControlCommand {
    ControlCommand::BeginMigration {
        raft_group_id: RaftGroupId(0),
        target_voters: voters(target),
        retain_removed: false,
        now_ms: 4,
    }
}

fn begin_rf(rf: ReplicationFactor, target: &[u64]) -> ControlCommand {
    ControlCommand::BeginPolicyMigration {
        raft_group_id: RaftGroupId(0),
        target_policy: GroupPlacementPolicy {
            replication_factor: rf,
            ..GroupPlacementPolicy::default()
        },
        target_voters: voters(target),
        retain_removed: false,
        now_ms: 4,
    }
}

fn phase(state: &mut ControlPlaneState, phase: MigrationPhase) {
    assert_eq!(
        state.apply(ControlCommand::AdvanceMigration {
            migration_id: state.active_migration.unwrap(),
            phase,
            now_ms: 5,
        }),
        ControlResponse::Ok
    );
}

fn commit(target: &[u64]) -> ControlCommand {
    ControlCommand::CommitPlacement {
        raft_group_id: RaftGroupId(0),
        voters: voters(target),
        learners: BTreeSet::new(),
        draining: BTreeSet::new(),
        now_ms: 6,
    }
}

#[test]
fn managed_rf_serde_accepts_only_three_or_five_in_json_msgpack_and_toml() {
    for (rf, count, quorum) in [
        (ReplicationFactor::Three, 3, 2),
        (ReplicationFactor::Five, 5, 3),
    ] {
        assert_eq!(rf.voter_count(), count);
        assert_eq!(rf.quorum(), quorum);
        assert_eq!(rf.tolerated_failures(), count - quorum);
        assert_eq!(serde_json::to_string(&rf).unwrap(), count.to_string());
        let wire = rmp_serde::to_vec_named(&rf).unwrap();
        assert_eq!(
            rmp_serde::from_slice::<ReplicationFactor>(&wire).unwrap(),
            rf
        );
    }
    for value in [0, 1, 2, 4, 6, u32::MAX] {
        assert!(ReplicationFactor::try_from(value).is_err());
        assert!(serde_json::from_str::<ReplicationFactor>(&value.to_string()).is_err());
        let wire = rmp_serde::to_vec(&value).unwrap();
        assert!(rmp_serde::from_slice::<ReplicationFactor>(&wire).is_err());
        assert!(
            toml::from_str::<PlacementPolicy>(&format!("default_replication_factor = {value}"))
                .is_err()
        );
    }
    let policy: PlacementPolicy = toml::from_str(
        "default_replication_factor = 3\nfailure_domain = 'zone'\nsurvive_failure_domains = 1\n[[group_overrides]]\nraft_group_id = 1\nreplication_factor = 5"
    ).unwrap();
    policy.validate(2).unwrap();
    assert_eq!(
        policy.resolve(RaftGroupId(0)).replication_factor,
        ReplicationFactor::Three
    );
    assert_eq!(
        policy.resolve(RaftGroupId(1)).replication_factor,
        ReplicationFactor::Five
    );
    assert_eq!(
        toml::from_str::<PlacementPolicy>(&toml::to_string(&policy).unwrap()).unwrap(),
        policy
    );
    assert!(toml::from_str::<PlacementPolicy>("default_replicaton_factor = 5").is_err());
}

#[test]
fn every_three_domain_assignment_matches_majority_after_one_domain_loss() {
    // Enumerate 27 RF3 and 243 RF5 assignments. Compute survival directly by
    // removing each domain, independently of the implementation's count bound.
    for rf in [ReplicationFactor::Three, ReplicationFactor::Five] {
        let policy = GroupPlacementPolicy {
            replication_factor: rf,
            ..GroupPlacementPolicy::default()
        };
        for assignment in 0..3_usize.pow(rf.voter_count() as u32) {
            let mut state = existing();
            let mut code = assignment;
            for id in 1..=rf.voter_count() as u64 {
                state
                    .nodes
                    .get_mut(&id)
                    .unwrap()
                    .labels
                    .insert("zone".to_owned(), (code % 3).to_string());
                code /= 3;
            }
            let voters: BTreeSet<_> = (1..=rf.voter_count() as u64).collect();
            let survives = (0..3).all(|lost| {
                voters
                    .iter()
                    .filter(|id| state.nodes[id].labels["zone"] != lost.to_string())
                    .count()
                    >= rf.quorum()
            });
            assert_eq!(
                policy.validate_voters(&voters, &state.nodes).is_ok(),
                survives
            );
        }
    }
}

#[test]
fn placement_rejects_insufficient_voters_missing_labels_and_unsupported_domain_policy() {
    let mut state = existing();
    let rf5 = GroupPlacementPolicy {
        replication_factor: ReplicationFactor::Five,
        ..GroupPlacementPolicy::default()
    };
    assert!(
        rf5.validate_voters(&voters(&[1, 2, 3]), &state.nodes)
            .is_err()
    );
    assert!(
        rf5.validate_voters(&voters(&[1, 2, 3, 4, 99]), &state.nodes)
            .is_err()
    );
    assert!(
        rf5.validate_voters(&voters(&[1, 2, 3, 4, 5]), &state.nodes)
            .is_ok()
    ); // 2/2/1
    state
        .nodes
        .get_mut(&5)
        .unwrap()
        .labels
        .insert("zone".to_owned(), "a".to_owned());
    assert!(
        rf5.validate_voters(&voters(&[1, 2, 3, 4, 5]), &state.nodes)
            .is_err()
    ); // 3/1/1
    for label in [None, Some(""), Some(" a ")] {
        let node = state.nodes.get_mut(&1).unwrap();
        node.labels.clear();
        if let Some(label) = label {
            node.labels.insert("zone".to_owned(), label.to_owned());
        }
        assert!(
            GroupPlacementPolicy::default()
                .validate_voters(&voters(&[1, 2, 3]), &state.nodes)
                .is_err()
        );
    }
    for survive in [0, 2, u32::MAX] {
        assert!(
            GroupPlacementPolicy {
                survive_failure_domains: survive,
                ..GroupPlacementPolicy::default()
            }
            .validate()
            .is_err()
        );
    }
    for key in ["", " ", " zone "] {
        assert!(
            GroupPlacementPolicy {
                failure_domain: key.to_owned(),
                ..GroupPlacementPolicy::default()
            }
            .validate()
            .is_err()
        );
    }
}

#[test]
fn adoption_resolves_mixed_rf_without_changing_placements_or_meta_voters() {
    let mut state = existing();
    state.config.initial_meta_voters = voters(&[1, 2, 3]);
    state.apply(ControlCommand::SeedPlacement {
        raft_group_id: RaftGroupId(1),
        voters: voters(&[1, 2, 3, 4, 5]),
        now_ms: 2,
    });
    let before = state.placements.clone();
    let policy = PlacementPolicy {
        group_overrides: vec![GroupPolicyOverride {
            raft_group_id: RaftGroupId(1),
            replication_factor: ReplicationFactor::Five,
        }],
        ..PlacementPolicy::default()
    };
    assert_eq!(state.apply(adopt(policy.clone(), 2)), ControlResponse::Ok);
    assert_eq!(state.placements, before);
    assert_eq!(state.config.initial_meta_voters, voters(&[1, 2, 3]));
    assert_eq!(
        state
            .placement_view(RaftGroupId(1))
            .unwrap()
            .policy
            .unwrap()
            .replication_factor,
        ReplicationFactor::Five
    );
    let before = state.clone();
    assert_eq!(state.apply(adopt(policy, 2)), ControlResponse::Ok);
    assert_eq!(state, before);
    rejected_unchanged(
        &mut state,
        adopt(
            PlacementPolicy {
                default_replication_factor: ReplicationFactor::Five,
                ..PlacementPolicy::default()
            },
            2,
        ),
    );
    rejected_unchanged(&mut state, adopt(PlacementPolicy::default(), 3));
    rejected_unchanged(&mut state, ControlCommand::SeedPlacement {
        raft_group_id: RaftGroupId(0),
        voters: voters(&[1, 2, 3, 4, 5]),
        now_ms: 8,
    });
}

#[test]
fn adoption_rejects_partial_layout_rf_drift_duplicates_and_unsettled_state_atomically() {
    let mut state = existing();
    rejected_unchanged(&mut state, adopt(PlacementPolicy::default(), 0));
    rejected_unchanged(&mut state, adopt(PlacementPolicy::default(), 2));
    rejected_unchanged(
        &mut state,
        adopt(
            PlacementPolicy {
                default_replication_factor: ReplicationFactor::Five,
                ..PlacementPolicy::default()
            },
            1,
        ),
    );
    let entry = GroupPolicyOverride {
        raft_group_id: RaftGroupId(0),
        replication_factor: ReplicationFactor::Three,
    };
    rejected_unchanged(
        &mut state,
        adopt(
            PlacementPolicy {
                group_overrides: vec![entry.clone(), entry],
                ..PlacementPolicy::default()
            },
            1,
        ),
    );
    rejected_unchanged(
        &mut state,
        adopt(
            PlacementPolicy {
                group_overrides: vec![GroupPolicyOverride {
                    raft_group_id: RaftGroupId(1),
                    replication_factor: ReplicationFactor::Three,
                }],
                ..PlacementPolicy::default()
            },
            1,
        ),
    );
    state
        .placements
        .get_mut(&RaftGroupId(0))
        .unwrap()
        .learners
        .insert(4);
    rejected_unchanged(&mut state, adopt(PlacementPolicy::default(), 1));
    state
        .placements
        .get_mut(&RaftGroupId(0))
        .unwrap()
        .learners
        .clear();
    state.apply(begin(&[1, 2, 6]));
    rejected_unchanged(&mut state, adopt(PlacementPolicy::default(), 1));
}

#[test]
fn ordinary_rebalance_preserves_rf_and_explicit_policy_intent_survives_serialization() {
    let mut state = managed();
    rejected_unchanged(&mut state, begin(&[1, 2, 3, 4, 5]));
    rejected_unchanged(&mut state, begin(&[1, 2]));
    rejected_unchanged(&mut state, begin(&[1, 2, 4])); // two voters in zone a
    assert_eq!(
        state.apply(begin_rf(ReplicationFactor::Five, &[1, 2, 3, 4, 5])),
        ControlResponse::MigrationStarted { migration_id: 1 }
    );
    let intent = state.active_migration().unwrap();
    assert_eq!(
        intent.from_policy.as_ref().unwrap().replication_factor,
        ReplicationFactor::Three
    );
    assert_eq!(
        intent.target_policy.as_ref().unwrap().replication_factor,
        ReplicationFactor::Five
    );
    assert_eq!(
        state.managed_placement.as_ref().unwrap().groups[&RaftGroupId(0)].replication_factor,
        ReplicationFactor::Three
    );
    assert_eq!(
        serde_json::from_slice::<ControlPlaneState>(&serde_json::to_vec(&state).unwrap()).unwrap(),
        state
    );
    assert_eq!(
        rmp_serde::from_slice::<ControlPlaneState>(&rmp_serde::to_vec_named(&state).unwrap())
            .unwrap(),
        state
    );
}

#[test]
fn policy_is_published_with_matching_placement_and_rf_reduction_is_explicit() {
    let mut state = managed();
    rejected_unchanged(&mut state, commit(&[1, 2, 3, 4, 5]));
    state.apply(begin_rf(ReplicationFactor::Five, &[1, 2, 3, 4, 5]));
    rejected_unchanged(&mut state, commit(&[1, 2, 3, 4, 5]));
    rejected_unchanged(&mut state, ControlCommand::FinishMigration {
        migration_id: 1,
        success: true,
        now_ms: 6,
    });
    phase(&mut state, MigrationPhase::CommittingPlacement);
    rejected_unchanged(&mut state, commit(&[1, 2, 3]));
    rejected_unchanged(&mut state, ControlCommand::FinishMigration {
        migration_id: 1,
        success: false,
        now_ms: 6,
    });
    assert_eq!(state.apply(commit(&[1, 2, 3, 4, 5])), ControlResponse::Ok);
    phase(&mut state, MigrationPhase::Finalizing);
    assert_eq!(
        state.apply(ControlCommand::FinishMigration {
            migration_id: 1,
            success: true,
            now_ms: 7
        }),
        ControlResponse::Ok
    );
    assert_eq!(state.placements[&RaftGroupId(0)].epoch, 1);
    assert_eq!(
        state.managed_placement.as_ref().unwrap().groups[&RaftGroupId(0)].replication_factor,
        ReplicationFactor::Five
    );
    // Replaying the original bootstrap policy never resets the resolved RF.
    let before = state.clone();
    assert_eq!(
        state.apply(adopt(PlacementPolicy::default(), 1)),
        ControlResponse::Ok
    );
    assert_eq!(state, before);
    rejected_unchanged(&mut state, begin(&[1, 2, 3]));
    assert_eq!(
        state.apply(begin_rf(ReplicationFactor::Three, &[1, 2, 3])),
        ControlResponse::MigrationStarted { migration_id: 2 }
    );
    phase(&mut state, MigrationPhase::CommittingPlacement);
    assert_eq!(state.apply(commit(&[1, 2, 3])), ControlResponse::Ok);
    phase(&mut state, MigrationPhase::Finalizing);
    assert_eq!(
        state.apply(ControlCommand::FinishMigration {
            migration_id: 2,
            success: true,
            now_ms: 8
        }),
        ControlResponse::Ok
    );
    assert_eq!(state.placements[&RaftGroupId(0)].epoch, 2);
}

#[test]
fn draining_source_serves_retained_groups_but_receives_no_new_allocations() {
    let mut state = managed();
    state.apply(ControlCommand::SetNodeState {
        node_id: 1,
        state: NodeState::Draining,
        now_ms: 4,
    });
    state.apply(ControlCommand::SetNodeState {
        node_id: 4,
        state: NodeState::Draining,
        now_ms: 4,
    });
    assert!(
        state
            .placement_view(RaftGroupId(0))
            .unwrap()
            .serves_client_traffic(1)
    );
    rejected_unchanged(&mut state, begin(&[4, 2, 3]));
    assert_eq!(
        state.apply(begin(&[1, 2, 6])),
        ControlResponse::MigrationStarted { migration_id: 1 }
    );
    phase(&mut state, MigrationPhase::CommittingPlacement);
    assert_eq!(state.apply(commit(&[1, 2, 6])), ControlResponse::Ok);
    assert!(
        state
            .placement_view(RaftGroupId(0))
            .unwrap()
            .serves_client_traffic(1)
    );
    state
        .placements
        .get_mut(&RaftGroupId(0))
        .unwrap()
        .draining
        .insert(1);
    assert!(
        !state
            .placement_view(RaftGroupId(0))
            .unwrap()
            .serves_client_traffic(1)
    );
}

#[test]
fn managed_node_identity_and_failure_domain_labels_cannot_drift_or_be_reused() {
    let mut state = managed();
    rejected_unchanged(&mut state, ControlCommand::RegisterNode {
        node_id: 1,
        client_url: "http://other:4437".to_owned(),
        cluster_url: "http://node1:4439".to_owned(),
        labels: BTreeMap::from([("zone".to_owned(), "a".to_owned())]),
        now_ms: 4,
    });
    rejected_unchanged(&mut state, ControlCommand::RegisterNode {
        node_id: 1,
        client_url: "http://node1:4437".to_owned(),
        cluster_url: "http://node1:4439".to_owned(),
        labels: BTreeMap::from([("zone".to_owned(), "b".to_owned())]),
        now_ms: 4,
    });
    rejected_unchanged(&mut state, ControlCommand::SetNodeState {
        node_id: 1,
        state: NodeState::Removed,
        now_ms: 4,
    });
    state.config.initial_meta_voters.insert(6);
    rejected_unchanged(&mut state, ControlCommand::SetNodeState {
        node_id: 6,
        state: NodeState::Removed,
        now_ms: 4,
    });
    assert_eq!(
        state.apply(ControlCommand::SetNodeState {
            node_id: 4,
            state: NodeState::Removed,
            now_ms: 4
        }),
        ControlResponse::Ok
    );
    rejected_unchanged(&mut state, ControlCommand::SetNodeState {
        node_id: 4,
        state: NodeState::Active,
        now_ms: 5,
    });
    rejected_unchanged(&mut state, ControlCommand::RegisterNode {
        node_id: 4,
        client_url: "http://node4:4437".to_owned(),
        cluster_url: "http://node4:4439".to_owned(),
        labels: BTreeMap::from([("zone".to_owned(), "a".to_owned())]),
        now_ms: 5,
    });
}

#[test]
fn cluster_default_five_stays_five_as_the_node_directory_grows() {
    let mut state = existing();
    state.placements.get_mut(&RaftGroupId(0)).unwrap().voters = voters(&[1, 2, 3, 4, 5]);
    let policy = PlacementPolicy {
        default_replication_factor: ReplicationFactor::Five,
        ..PlacementPolicy::default()
    };
    assert_eq!(state.apply(adopt(policy, 1)), ControlResponse::Ok);
    assert_eq!(state.config.default_replication_factor, 5);
    let before = state.placements.clone();
    assert_eq!(
        state.apply(ControlCommand::RegisterNode {
            node_id: 7,
            client_url: "http://node7:4437".to_owned(),
            cluster_url: "http://node7:4439".to_owned(),
            labels: BTreeMap::from([("zone".to_owned(), "d".to_owned())]),
            now_ms: 4,
        }),
        ControlResponse::Ok
    );
    assert_eq!(state.placements, before);
    rejected_unchanged(&mut state, begin(&[1, 2, 3, 4, 5, 6, 7]));
    assert_eq!(
        state.apply(begin(&[1, 2, 3, 4, 7])),
        ControlResponse::MigrationStarted { migration_id: 1 }
    );
    assert_eq!(
        state
            .active_migration()
            .unwrap()
            .target_policy
            .as_ref()
            .unwrap()
            .replication_factor,
        ReplicationFactor::Five
    );
}

#[test]
fn changing_only_the_failure_domain_advances_the_projection_epoch() {
    let mut state = existing();
    for node in state.nodes.values_mut() {
        node.labels
            .insert("rack".to_owned(), node.node_id.to_string());
    }
    state.apply(adopt(PlacementPolicy::default(), 1));
    let target_policy = GroupPlacementPolicy {
        failure_domain: "rack".to_owned(),
        ..GroupPlacementPolicy::default()
    };
    assert_eq!(
        state.apply(ControlCommand::BeginPolicyMigration {
            raft_group_id: RaftGroupId(0),
            target_policy: target_policy.clone(),
            target_voters: voters(&[1, 2, 3]),
            retain_removed: false,
            now_ms: 4,
        }),
        ControlResponse::MigrationStarted { migration_id: 1 }
    );
    phase(&mut state, MigrationPhase::CommittingPlacement);
    assert_eq!(state.apply(commit(&[1, 2, 3])), ControlResponse::Ok);
    assert_eq!(
        state.placement_view(RaftGroupId(0)).unwrap().policy,
        Some(target_policy)
    );
    assert_eq!(state.placements[&RaftGroupId(0)].epoch, 1);
    // Repeated publication of the same target does not keep incrementing it.
    assert_eq!(state.apply(commit(&[1, 2, 3])), ControlResponse::Ok);
    assert_eq!(state.placements[&RaftGroupId(0)].epoch, 1);
}

#[test]
fn legacy_snapshots_without_managed_fields_remain_static_until_explicit_adoption() {
    let mut state = existing();
    state.apply(begin(&[1, 2])); // legacy/static explicit RF2 remains supported
    let mut json = serde_json::to_value(&state).unwrap();
    json.as_object_mut().unwrap().remove("managed_placement");
    for intent in json["migrations"].as_object_mut().unwrap().values_mut() {
        intent.as_object_mut().unwrap().remove("from_policy");
        intent.as_object_mut().unwrap().remove("target_policy");
    }
    let recovered: ControlPlaneState = serde_json::from_value(json.clone()).unwrap();
    assert_eq!(recovered, state);
    let recovered: ControlPlaneState =
        rmp_serde::from_slice(&rmp_serde::to_vec_named(&state).unwrap()).unwrap();
    assert_eq!(recovered, state);
    rejected_unchanged(
        &mut state,
        begin_rf(ReplicationFactor::Five, &[1, 2, 3, 4, 5]),
    );
}

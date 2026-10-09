use std::collections::BTreeMap;
use std::collections::BTreeSet;

use ursula_shard::RaftGroupId;

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
fn only_active_nodes_accept_new_replicas() {
    assert!(NodeState::Active.accepts_new_replicas());
    assert!(!NodeState::Draining.accepts_new_replicas());
    assert!(!NodeState::Disabled.accepts_new_replicas());
    assert!(!NodeState::Removed.accepts_new_replicas());
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
            ControlResponse::Operation(Ok(crate::OperationOutcome::Completed)),
            "operation",
        ),
        (
            ControlResponse::Rejected {
                reason: crate::ControlError::UnknownNode { node_id: 99 },
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
    register_active_nodes(&mut state, [1, 2, 3]);

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
    state
        .placements
        .insert(RaftGroupId(1), crate::DataGroupPlacement {
            raft_group_id: RaftGroupId(1),
            voters: set([1]),
            learners: set([2]),
            draining: set([3]),
            epoch: 1,
            updated_at_ms: 30,
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
fn seeding_never_overwrites_existing_placement_or_accepts_unknown_voters() {
    let mut state = ControlPlaneState::default();
    register_active_nodes(&mut state, [1, 2]);
    let seed = |voters| ControlCommand::SeedPlacement {
        raft_group_id: RaftGroupId(1),
        voters,
        now_ms: 20,
    };
    assert_eq!(state.apply(seed(set([1, 9]))), ControlResponse::Rejected {
        reason: crate::ControlError::UnknownNode { node_id: 9 },
    });
    assert!(state.placements.is_empty());
    assert_eq!(state.apply(seed(set([1]))), ControlResponse::Ok);
    let before = state.clone();
    assert_eq!(state.apply(seed(set([1]))), ControlResponse::Ok);
    assert_eq!(state, before);
    assert_eq!(state.apply(seed(set([2]))), ControlResponse::Rejected {
        reason: crate::ControlError::PlacementExists {
            raft_group_id: RaftGroupId(1)
        },
    });
    assert_eq!(state, before);
}

#[test]
fn removed_nodes_cannot_be_resurrected_by_registration() {
    let mut state = ControlPlaneState::default();
    register_active_nodes(&mut state, [1]);
    assert_eq!(
        state.apply(ControlCommand::SetNodeState {
            node_id: 1,
            state: NodeState::Removed,
            now_ms: 20,
        }),
        ControlResponse::Rejected {
            reason: crate::ControlError::RemovalRequiresOperation { node_id: 1 }
        }
    );
    // A completed decommission leaves this terminal tombstone.
    state.nodes.get_mut(&1).unwrap().state = NodeState::Removed;
    let before = state.clone();
    assert_eq!(
        state.apply(ControlCommand::RegisterNode {
            node_id: 1,
            client_url: "http://replacement".into(),
            cluster_url: "http://replacement".into(),
            labels: BTreeMap::new(),
            now_ms: 30,
        }),
        ControlResponse::Rejected {
            reason: crate::ControlError::RemovedNode { node_id: 1 }
        }
    );
    assert_eq!(state, before);
}

#[test]
fn legacy_migration_snapshot_cannot_silently_become_an_idle_new_authority() {
    let mut serialized = serde_json::to_value(ControlPlaneState::default()).unwrap();
    serialized
        .as_object_mut()
        .unwrap()
        .insert("active_migration".into(), 1.into());
    serde_json::from_value::<ControlPlaneState>(serialized)
        .expect_err("legacy migration state requires an explicit storage cutover");
}

#[test]
fn one_dispatcher_owns_membership_from_intent_through_placement() {
    use crate::OperationCommand;
    use crate::OperationOutcome;
    let mut state = ControlPlaneState::default();
    register_active_nodes(&mut state, [1, 2, 3, 4]);
    assert_eq!(
        state.apply(ControlCommand::SeedPlacement {
            raft_group_id: RaftGroupId(0),
            voters: set([1, 2, 3]),
            now_ms: 10
        }),
        ControlResponse::Ok
    );
    let mut participants = BTreeMap::new();
    for node_id in 1..=4 {
        let ControlResponse::Operation(Ok(OperationOutcome::ProcessClaimed(identity))) = state
            .apply(ControlCommand::Operation {
                command: OperationCommand::ClaimProcess {
                    node_id,
                    expected_epoch: 0,
                    incarnation: crate::ProcessIncarnation::from_bits(u128::from(node_id)),
                },
                now_ms: 10,
            })
        else {
            panic!("claim");
        };
        participants.insert(node_id, identity);
    }
    let target = crate::ReplicaIdentity {
        generation: 1,
        incarnation: crate::ProcessIncarnation::from_bits(4),
    };
    assert_eq!(
        state.apply(ControlCommand::Operation {
            command: OperationCommand::RegisterReplica {
                node_id: 4,
                process: participants[&4].clone(),
                identity: target.clone()
            },
            now_ms: 10
        }),
        ControlResponse::Operation(Ok(OperationOutcome::ReplicaRegistered))
    );
    let ControlResponse::Operation(Ok(OperationOutcome::Acquired(token))) =
        state.apply(ControlCommand::Operation {
            command: OperationCommand::Begin {
                kind: crate::OperationKind::MoveReplicas {
                    source: 1,
                    target: 4,
                    groups: BTreeSet::from([RaftGroupId(0)]),
                },
                executor: crate::ProcessIncarnation::from_bits(100),
                participants: participants.clone(),
                meta_voters: set([1, 2, 3]),
            },
            now_ms: 10,
        })
    else {
        panic!("begin");
    };
    let before = state.clone();
    assert_eq!(
        state.apply(ControlCommand::SeedPlacement {
            raft_group_id: RaftGroupId(0),
            voters: set([2, 3, 4]),
            now_ms: 10
        }),
        ControlResponse::Operation(Err(crate::OperationError::Busy))
    );
    assert_eq!(state, before);
    assert_eq!(
        state.apply(ControlCommand::Operation {
            command: OperationCommand::Complete {
                token: token.clone()
            },
            now_ms: 10
        }),
        ControlResponse::Operation(Err(crate::OperationError::MissingEvidence {
            raft_group_id: RaftGroupId(0)
        }))
    );
    assert_eq!(state, before);

    let ControlResponse::Operation(Ok(OperationOutcome::ActionPrepared(receipt))) =
        state.apply(ControlCommand::Operation {
            command: OperationCommand::PrepareAction {
                token: token.clone(),
                group: RaftGroupId(0),
                leader: 2,
                action: crate::MembershipAction::InstallReplicaIdentity {
                    node_id: 4,
                    identity: target.clone(),
                },
            },
            now_ms: 10,
        })
    else {
        panic!("admission action");
    };
    assert_eq!(
        state.apply(ControlCommand::Operation {
            command: OperationCommand::MarkActionDispatched {
                token: token.clone(),
                sequence: receipt.sequence
            },
            now_ms: 10
        }),
        ControlResponse::Operation(Ok(OperationOutcome::ActionOutcome(
            crate::ActionOutcome::Unknown
        )))
    );
    assert_eq!(
        state.apply(ControlCommand::Operation {
            command: OperationCommand::Observe {
                token: token.clone(),
                evidence: crate::PrefixEvidence {
                    raft_group_id: RaftGroupId(0),
                    leader: 2,
                    term: 3,
                    committed_index: 99,
                    voters: set([1, 2, 3]),
                    joint: false,
                    replicas: participants
                        .iter()
                        .filter(|(id, _)| **id != 4)
                        .map(|(id, process)| (*id, crate::ReplicaEvidence {
                            process: process.clone(),
                            applied_index: 99,
                            installed_replica_identities: BTreeMap::from([(4, target.clone())])
                        }))
                        .collect(),
                    observed_at_ms: 10
                }
            },
            now_ms: 10
        }),
        ControlResponse::Operation(Ok(OperationOutcome::EvidenceRecorded))
    );
    assert_eq!(
        state.apply(ControlCommand::Operation {
            command: OperationCommand::FinishReplicaFence {
                token: token.clone(),
                sequence: receipt.sequence,
                committed_index: 99
            },
            now_ms: 10
        }),
        ControlResponse::Operation(Ok(OperationOutcome::ActionOutcome(
            crate::ActionOutcome::Completed
        )))
    );
    assert_eq!(
        state.apply(ControlCommand::Operation {
            command: OperationCommand::Observe {
                token: token.clone(),
                evidence: crate::PrefixEvidence {
                    raft_group_id: RaftGroupId(0),
                    leader: 2,
                    term: 3,
                    committed_index: 100,
                    voters: set([2, 3, 4]),
                    joint: false,
                    replicas: participants
                        .into_iter()
                        .filter(|(id, _)| *id != 1)
                        .map(|(id, process)| (id, crate::ReplicaEvidence {
                            process,
                            applied_index: 100,
                            installed_replica_identities: BTreeMap::new()
                        }))
                        .collect(),
                    observed_at_ms: 10,
                }
            },
            now_ms: 10,
        }),
        ControlResponse::Operation(Ok(OperationOutcome::EvidenceRecorded))
    );
    assert_eq!(
        state.apply(ControlCommand::Operation {
            command: OperationCommand::Complete { token },
            now_ms: 10
        }),
        ControlResponse::Operation(Ok(OperationOutcome::Completed))
    );
    assert_eq!(state.placements[&RaftGroupId(0)].voters, set([2, 3, 4]));
    assert_eq!(state.placements[&RaftGroupId(0)].epoch, 1);
}

/// Registers nodes, seeds group 0 on voters 1-3, and claims a process and a
/// generation-1 replica for every node.
fn ready_cluster(nodes: &[u64]) -> ControlPlaneState {
    use crate::OperationCommand;
    use crate::OperationOutcome;
    let mut state = ControlPlaneState::default();
    register_active_nodes(&mut state, nodes.iter().copied());
    assert_eq!(
        state.apply(ControlCommand::SeedPlacement {
            raft_group_id: RaftGroupId(0),
            voters: set([1, 2, 3]),
            now_ms: 10
        }),
        ControlResponse::Ok
    );
    for node_id in nodes {
        let incarnation = crate::ProcessIncarnation::from_bits(u128::from(*node_id));
        let ControlResponse::Operation(Ok(OperationOutcome::ProcessClaimed(process))) = state
            .apply(ControlCommand::Operation {
                command: OperationCommand::ClaimProcess {
                    node_id: *node_id,
                    expected_epoch: 0,
                    incarnation: incarnation.clone(),
                },
                now_ms: 10,
            })
        else {
            panic!("claim");
        };
        assert_eq!(
            state.apply(ControlCommand::Operation {
                command: OperationCommand::RegisterReplica {
                    node_id: *node_id,
                    process,
                    identity: crate::ReplicaIdentity {
                        generation: 1,
                        incarnation,
                    },
                },
                now_ms: 10,
            }),
            ControlResponse::Operation(Ok(OperationOutcome::ReplicaRegistered))
        );
    }
    state
}

fn begin_operation(state: &mut ControlPlaneState, kind: crate::OperationKind) -> ControlResponse {
    let participants = state
        .operations
        .processes
        .iter()
        .filter_map(|(node_id, process)| match process {
            crate::ProcessState::Active(identity) => Some((*node_id, identity.clone())),
            crate::ProcessState::Retired { .. } => None,
        })
        .filter(|(node_id, _)| *node_id <= 3 || kind_targets(&kind).contains(node_id))
        .collect();
    state.apply(ControlCommand::Operation {
        command: crate::OperationCommand::Begin {
            kind,
            executor: crate::ProcessIncarnation::from_bits(100),
            participants,
            meta_voters: set([1, 2, 3]),
        },
        now_ms: 10,
    })
}

fn kind_targets(kind: &crate::OperationKind) -> BTreeSet<u64> {
    match kind {
        crate::OperationKind::MoveReplicas { target, .. } => set([*target]),
        crate::OperationKind::RebuildReplica { .. } => BTreeSet::new(),
        crate::OperationKind::DecommissionNode { replacements, .. } => {
            replacements.values().copied().collect()
        }
    }
}

#[test]
fn new_replicas_need_active_nodes_while_any_unremoved_source_can_drain() {
    let mut state = ready_cluster(&[1, 2, 3, 4]);
    let set_state = |state: &mut ControlPlaneState, node_id, node_state| {
        assert_eq!(
            state.apply(ControlCommand::SetNodeState {
                node_id,
                state: node_state,
                now_ms: 10,
            }),
            ControlResponse::Ok
        );
    };
    let move_to_four = |source| crate::OperationKind::MoveReplicas {
        source,
        target: 4,
        groups: BTreeSet::from([RaftGroupId(0)]),
    };

    // A disabled source can still be drained.
    set_state(&mut state, 1, NodeState::Disabled);
    let ControlResponse::Operation(Ok(crate::OperationOutcome::Acquired(token))) =
        begin_operation(&mut state, move_to_four(1))
    else {
        panic!("a disabled source can be drained");
    };
    assert_eq!(
        state.apply(ControlCommand::Operation {
            command: crate::OperationCommand::Abort { token },
            now_ms: 10,
        }),
        ControlResponse::Operation(Ok(crate::OperationOutcome::Aborted))
    );

    // New voters and learners need an active node.
    for (node_state, kind) in [
        (NodeState::Draining, move_to_four(2)),
        (
            NodeState::Disabled,
            crate::OperationKind::DecommissionNode {
                node_id: 2,
                replacements: BTreeMap::from([(RaftGroupId(0), 4)]),
            },
        ),
    ] {
        set_state(&mut state, 4, node_state);
        let before = state.clone();
        assert_eq!(
            begin_operation(&mut state, kind),
            ControlResponse::Operation(Err(crate::OperationError::IneligibleNode {
                node_id: 4,
                state: node_state,
            }))
        );
        assert_eq!(state, before);
    }
    // A rebuilt replica rejoins as a new voter.
    let before = state.clone();
    assert_eq!(
        begin_operation(&mut state, crate::OperationKind::RebuildReplica {
            node_id: 1
        }),
        ControlResponse::Operation(Err(crate::OperationError::IneligibleNode {
            node_id: 1,
            state: NodeState::Disabled,
        }))
    );
    assert_eq!(state, before);
}

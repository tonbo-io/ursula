use std::collections::BTreeSet;

use super::*;
use crate::NodeState;
use crate::ReplicaIdentity;
use crate::model::NodeStates;

fn identity(epoch: u64, node: u64) -> ProcessIdentity {
    ProcessIdentity {
        epoch,
        incarnation: ProcessIncarnation::from_bits(u128::from(node)),
    }
}

/// The durable replica `setup` registers for `node`.
fn replica(node: u64) -> ReplicaIdentity {
    ReplicaIdentity {
        generation: 1,
        incarnation: ProcessIncarnation::from_bits(u128::from(node)),
    }
}

fn setup() -> (
    OperationState,
    NodeStates,
    BTreeMap<RaftGroupId, DataGroupPlacement>,
) {
    let nodes: NodeStates = [1, 2, 3, 4].map(|id| (id, NodeState::Active)).into();
    let state = OperationState {
        processes: nodes
            .keys()
            .map(|id| (*id, ProcessState::Active(identity(1, *id))))
            .collect(),
        replicas: nodes
            .keys()
            .map(|id| {
                (*id, ReplicaState::Active {
                    identity: replica(*id),
                    installed_groups: BTreeMap::new(),
                })
            })
            .collect(),
        ..OperationState::default()
    };
    let placement = DataGroupPlacement {
        raft_group_id: RaftGroupId(0),
        voters: BTreeSet::from([1, 2, 3]),
        epoch: 1,
        updated_at_ms: 0,
    };
    (state, nodes, BTreeMap::from([(RaftGroupId(0), placement)]))
}

fn begin(
    state: &mut OperationState,
    nodes: &NodeStates,
    placements: &mut BTreeMap<RaftGroupId, DataGroupPlacement>,
    kind: OperationKind,
    ids: &[u64],
) -> OperationToken {
    let command = OperationCommand::Begin {
        kind,
        executor: ProcessIncarnation::from_bits(99),
        participants: ids.iter().map(|id| (*id, identity(1, *id))).collect(),
    };
    let OperationOutcome::Acquired(token) = state.apply(command, 10, nodes, placements).unwrap()
    else {
        panic!("acquisition result");
    };
    token
}

fn evidence(voters: &[u64], replicas: &[u64], index: u64) -> PrefixEvidence {
    PrefixEvidence {
        raft_group_id: RaftGroupId(0),
        leader: 2,
        term: 1,
        committed_index: index,
        voters: voters.iter().copied().collect(),
        joint: false,
        replicas: replicas
            .iter()
            .map(|id| {
                (*id, ReplicaEvidence {
                    process: identity(1, *id),
                    applied_index: index,
                    installed_replica_identities: BTreeMap::new(),
                })
            })
            .collect(),
        observed_at_ms: 10,
    }
}

#[test]
fn replica_fence_requires_every_survivor_not_only_a_majority() {
    let (mut state, mut nodes, mut placements) = setup();
    nodes.insert(5, NodeState::Active);
    state
        .processes
        .insert(5, ProcessState::Active(identity(1, 5)));
    placements.get_mut(&RaftGroupId(0)).unwrap().voters = BTreeSet::from([1, 2, 3, 4]);
    let admitted = ReplicaIdentity {
        generation: 1,
        incarnation: ProcessIncarnation::from_bits(55),
    };
    state
        .apply(
            OperationCommand::RegisterReplica {
                node_id: 5,
                process: identity(1, 5),
                identity: admitted.clone(),
            },
            10,
            &nodes,
            &mut placements,
        )
        .unwrap();
    let token = begin(
        &mut state,
        &nodes,
        &mut placements,
        OperationKind::MoveReplicas {
            source: 1,
            target: 5,
            groups: BTreeSet::from([RaftGroupId(0)]),
        },
        &[1, 2, 3, 4, 5],
    );
    let OperationOutcome::ActionPrepared(action) = state
        .apply(
            OperationCommand::PrepareAction {
                token: token.clone(),
                group: RaftGroupId(0),
                leader: 2,
                action: MembershipAction::InstallReplicaIdentity {
                    node_id: 5,
                    identity: admitted.clone(),
                },
            },
            10,
            &nodes,
            &mut placements,
        )
        .unwrap()
    else {
        panic!("action receipt");
    };
    dispatch_pending(&mut state, &nodes, &mut placements);
    for replicas in [&[1, 2, 3][..], &[1, 2, 3, 4][..]] {
        let mut proof = evidence(&[1, 2, 3, 4], replicas, 100);
        for replica in proof.replicas.values_mut() {
            replica
                .installed_replica_identities
                .insert(5, admitted.clone());
        }
        state
            .apply(
                OperationCommand::Observe {
                    token: token.clone(),
                    evidence: proof,
                },
                10,
                &nodes,
                &mut placements,
            )
            .unwrap();
        let finished = state.apply(
            OperationCommand::FinishReplicaFence {
                token: token.clone(),
                sequence: action.sequence,
                committed_index: 100,
            },
            10,
            &nodes,
            &mut placements,
        );
        if replicas.len() == 3 {
            assert!(matches!(
                finished,
                Err(OperationError::MissingEvidence {
                    raft_group_id: RaftGroupId(0),
                })
            ));
            assert_eq!(
                state
                    .active
                    .as_ref()
                    .unwrap()
                    .pending_action
                    .as_ref()
                    .map(PendingAction::receipt),
                Some(&action)
            );
            assert!(matches!(state.replicas.get(&5), Some(ReplicaState::Active {
                installed_groups, ..
            }) if installed_groups.is_empty()));
        } else {
            assert_eq!(
                finished.unwrap(),
                OperationOutcome::ActionOutcome(ActionOutcome::Completed)
            );
            assert!(matches!(state.replicas.get(&5), Some(ReplicaState::Active {
                installed_groups, ..
            }) if installed_groups.get(&RaftGroupId(0)) == Some(&100)));
        }
    }
}

#[test]
fn rebuild_retirement_requires_survivors_and_fences_the_old_process() {
    let (mut state, nodes, mut placements) = setup();
    state
        .apply(
            OperationCommand::RegisterReplica {
                node_id: 1,
                process: identity(1, 1),
                identity: ReplicaIdentity {
                    generation: 1,
                    incarnation: ProcessIncarnation::from_bits(1),
                },
            },
            10,
            &nodes,
            &mut placements,
        )
        .unwrap();
    let token = begin(
        &mut state,
        &nodes,
        &mut placements,
        OperationKind::RebuildReplica { node_id: 1 },
        &[1, 2, 3],
    );
    let retire = OperationCommand::RetireSource {
        token: token.clone(),
    };
    assert!(matches!(
        state.apply(retire.clone(), 10, &nodes, &mut placements),
        Err(OperationError::MissingEvidence { .. })
    ));
    state
        .apply(
            OperationCommand::Observe {
                token: token.clone(),
                evidence: evidence(&[1, 2, 3], &[2, 3], 100),
            },
            10,
            &nodes,
            &mut placements,
        )
        .unwrap();
    let OperationOutcome::ActionPrepared(receipt) = state
        .apply(
            OperationCommand::PrepareAction {
                token: token.clone(),
                group: RaftGroupId(0),
                leader: 2,
                action: MembershipAction::RetireReplica,
            },
            10,
            &nodes,
            &mut placements,
        )
        .unwrap()
    else {
        panic!("retire receipt");
    };
    dispatch_pending(&mut state, &nodes, &mut placements);
    state
        .apply(
            OperationCommand::FinishAction {
                token: token.clone(),
                sequence: receipt.sequence,
            },
            10,
            &nodes,
            &mut placements,
        )
        .unwrap();
    state
        .apply(
            OperationCommand::Observe {
                token: token.clone(),
                evidence: evidence(&[2, 3], &[2, 3], 101),
            },
            10,
            &nodes,
            &mut placements,
        )
        .unwrap();
    state.apply(retire, 10, &nodes, &mut placements).unwrap();
    assert!(!state.accepts_process(1, &identity(1, 1)));
    assert!(matches!(
        state.apply(
            OperationCommand::ClaimProcess {
                node_id: 4,
                expected_epoch: 1,
                incarnation: ProcessIncarnation::from_bits(44)
            },
            11,
            &nodes,
            &mut placements
        ),
        Err(OperationError::Busy)
    ));
    let OperationOutcome::ProcessClaimed(replacement) = state
        .apply(
            OperationCommand::ClaimProcess {
                node_id: 1,
                expected_epoch: 1,
                incarnation: ProcessIncarnation::from_bits(11),
            },
            11,
            &nodes,
            &mut placements,
        )
        .unwrap()
    else {
        panic!("claim result");
    };
    assert_eq!(replacement.epoch, 2);
    assert!(state.accepts_process(1, &replacement));
    let replacement_replica = ReplicaIdentity {
        generation: 2,
        incarnation: ProcessIncarnation::from_bits(11),
    };
    state
        .apply(
            OperationCommand::RegisterReplica {
                node_id: 1,
                process: replacement.clone(),
                identity: replacement_replica.clone(),
            },
            11,
            &nodes,
            &mut placements,
        )
        .unwrap();
    install_admission(
        &mut state,
        &nodes,
        &mut placements,
        &token,
        1,
        replacement_replica.clone(),
        &[2, 3],
    );
    state
        .apply(
            OperationCommand::ActivateReplica {
                token: token.clone(),
                node_id: 1,
                identity: replacement_replica,
            },
            11,
            &nodes,
            &mut placements,
        )
        .unwrap();
    let mut proof = evidence(&[1, 2, 3], &[1, 2, 3], 101);
    proof.replicas.get_mut(&1).unwrap().process = replacement;
    state
        .apply(
            OperationCommand::Observe {
                token: token.clone(),
                evidence: proof,
            },
            11,
            &nodes,
            &mut placements,
        )
        .unwrap();
    state
        .apply(
            OperationCommand::Complete { token },
            11,
            &nodes,
            &mut placements,
        )
        .unwrap();
    assert!(state.active.is_none());
    assert_eq!(placements[&RaftGroupId(0)].epoch, 2);
}

#[test]
fn takeover_invalidates_executor_and_evidence_but_retains_prefix_floor() {
    let (mut state, nodes, mut placements) = setup();
    let target_identity = ReplicaIdentity {
        generation: 1,
        incarnation: ProcessIncarnation::from_bits(4),
    };
    state
        .apply(
            OperationCommand::RegisterReplica {
                node_id: 4,
                process: identity(1, 4),
                identity: target_identity.clone(),
            },
            10,
            &nodes,
            &mut placements,
        )
        .unwrap();
    let token = begin(
        &mut state,
        &nodes,
        &mut placements,
        OperationKind::MoveReplicas {
            source: 1,
            target: 4,
            groups: BTreeSet::from([RaftGroupId(0)]),
        },
        &[1, 2, 3, 4],
    );
    install_admission(
        &mut state,
        &nodes,
        &mut placements,
        &token,
        4,
        target_identity,
        &[1, 2, 3],
    );
    state
        .apply(
            OperationCommand::Observe {
                token: token.clone(),
                evidence: evidence(&[2, 3, 4], &[2, 3, 4], 100),
            },
            10,
            &nodes,
            &mut placements,
        )
        .unwrap();
    let OperationOutcome::Acquired(next) = state
        .apply(
            OperationCommand::TakeOver {
                expected: token.clone(),
                executor: ProcessIncarnation::from_bits(100),
            },
            11,
            &nodes,
            &mut placements,
        )
        .unwrap()
    else {
        panic!("takeover result");
    };
    assert_eq!(next.generation, ExecutorGeneration(2));
    assert!(matches!(
        state.apply(
            OperationCommand::Complete { token },
            11,
            &nodes,
            &mut placements
        ),
        Err(OperationError::StaleExecutor)
    ));
    assert!(matches!(
        state.apply(
            OperationCommand::Complete {
                token: next.clone()
            },
            11,
            &nodes,
            &mut placements
        ),
        Err(OperationError::MissingEvidence { .. })
    ));
    assert!(matches!(
        state.apply(
            OperationCommand::Observe {
                token: next.clone(),
                evidence: evidence(&[2, 3, 4], &[2, 3, 4], 99)
            },
            11,
            &nodes,
            &mut placements
        ),
        Err(OperationError::MissingEvidence { .. })
    ));
    state
        .apply(
            OperationCommand::Observe {
                token: next.clone(),
                evidence: evidence(&[2, 3, 4], &[2, 3, 4], 101),
            },
            11,
            &nodes,
            &mut placements,
        )
        .unwrap();
    state
        .apply(
            OperationCommand::Complete { token: next },
            11,
            &nodes,
            &mut placements,
        )
        .unwrap();
    assert_eq!(
        placements[&RaftGroupId(0)].voters,
        BTreeSet::from([2, 3, 4])
    );
    assert!(
        state.accepts_process(1, &identity(1, 1)),
        "moving replicas does not retire the whole process"
    );
}

#[test]
fn stale_joint_or_incomplete_proof_never_allows_retirement() {
    let (mut state, nodes, mut placements) = setup();
    let token = begin(
        &mut state,
        &nodes,
        &mut placements,
        OperationKind::RebuildReplica { node_id: 1 },
        &[1, 2, 3],
    );
    let mut proof = evidence(&[1, 2, 3], &[2], 100);
    proof.joint = true;
    assert!(matches!(
        state.apply(
            OperationCommand::Observe {
                token: token.clone(),
                evidence: proof
            },
            10,
            &nodes,
            &mut placements
        ),
        Err(OperationError::MissingEvidence { .. })
    ));
    state
        .apply(
            OperationCommand::Observe {
                token: token.clone(),
                evidence: evidence(&[1, 2, 3], &[2], 100),
            },
            10,
            &nodes,
            &mut placements,
        )
        .unwrap();
    assert!(matches!(
        state.apply(
            OperationCommand::RetireSource {
                token: token.clone()
            },
            10,
            &nodes,
            &mut placements
        ),
        Err(OperationError::MissingEvidence { .. })
    ));
    state
        .apply(
            OperationCommand::Observe {
                token: token.clone(),
                evidence: evidence(&[1, 2, 3], &[2, 3], 100),
            },
            10,
            &nodes,
            &mut placements,
        )
        .unwrap();
    assert!(matches!(
        state.apply(
            OperationCommand::RetireSource { token },
            40_011,
            &nodes,
            &mut placements
        ),
        Err(OperationError::MissingEvidence { .. })
    ));
    assert!(state.accepts_process(1, &identity(1, 1)));
}

#[test]
fn decommission_cannot_reclaim_retired_identity() {
    let (mut state, nodes, mut placements) = setup();
    let target_identity = ReplicaIdentity {
        generation: 1,
        incarnation: ProcessIncarnation::from_bits(4),
    };
    state
        .apply(
            OperationCommand::RegisterReplica {
                node_id: 4,
                process: identity(1, 4),
                identity: target_identity.clone(),
            },
            10,
            &nodes,
            &mut placements,
        )
        .unwrap();
    let token = begin(
        &mut state,
        &nodes,
        &mut placements,
        OperationKind::DecommissionNode {
            node_id: 1,
            replacements: BTreeMap::from([(RaftGroupId(0), 4)]),
        },
        &[1, 2, 3, 4],
    );
    install_admission(
        &mut state,
        &nodes,
        &mut placements,
        &token,
        4,
        target_identity,
        &[1, 2, 3],
    );
    state
        .apply(
            OperationCommand::Observe {
                token: token.clone(),
                evidence: evidence(&[1, 2, 3], &[2, 3], 100),
            },
            10,
            &nodes,
            &mut placements,
        )
        .unwrap();
    assert_eq!(
        state.apply(
            OperationCommand::RetireSource {
                token: token.clone()
            },
            10,
            &nodes,
            &mut placements
        ),
        Err(OperationError::MissingEvidence {
            raft_group_id: RaftGroupId(0)
        })
    );
    state
        .apply(
            OperationCommand::Observe {
                token: token.clone(),
                evidence: evidence(&[2, 3, 4], &[2, 3, 4], 100),
            },
            10,
            &nodes,
            &mut placements,
        )
        .unwrap();
    state
        .apply(
            OperationCommand::RetireSource {
                token: token.clone(),
            },
            10,
            &nodes,
            &mut placements,
        )
        .unwrap();
    state
        .apply(
            OperationCommand::Observe {
                token: token.clone(),
                evidence: evidence(&[2, 3, 4], &[2, 3, 4], 100),
            },
            10,
            &nodes,
            &mut placements,
        )
        .unwrap();
    state
        .apply(
            OperationCommand::Complete { token },
            10,
            &nodes,
            &mut placements,
        )
        .unwrap();
    assert!(matches!(
        state.apply(
            OperationCommand::ClaimProcess {
                node_id: 1,
                expected_epoch: 1,
                incarnation: ProcessIncarnation::from_bits(11)
            },
            11,
            &nodes,
            &mut placements
        ),
        Err(OperationError::InvalidTransition)
    ));
}
#[test]
fn unresolved_action_survives_takeover_and_blocks_completion() {
    let (mut state, nodes, mut placements) = setup();
    let token = begin(
        &mut state,
        &nodes,
        &mut placements,
        OperationKind::MoveReplicas {
            source: 1,
            target: 4,
            groups: BTreeSet::from([RaftGroupId(0)]),
        },
        &[1, 2, 3, 4],
    );
    let OperationOutcome::ActionPrepared(action) = state
        .apply(
            OperationCommand::PrepareAction {
                token: token.clone(),
                group: RaftGroupId(0),
                leader: 2,
                action: MembershipAction::AddLearner { node_id: 4 },
            },
            10,
            &nodes,
            &mut placements,
        )
        .unwrap()
    else {
        panic!("action receipt");
    };
    dispatch_pending(&mut state, &nodes, &mut placements);
    let OperationOutcome::Acquired(next) = state
        .apply(
            OperationCommand::TakeOver {
                expected: token.clone(),
                executor: ProcessIncarnation::from_bits(100),
            },
            11,
            &nodes,
            &mut placements,
        )
        .unwrap()
    else {
        panic!("takeover");
    };
    assert_eq!(
        state.active.as_ref().unwrap().pending_action,
        Some(PendingAction::OutcomeUnknown(action.clone()))
    );
    assert!(matches!(
        state.apply(
            OperationCommand::Complete {
                token: next.clone()
            },
            11,
            &nodes,
            &mut placements
        ),
        Err(OperationError::Busy)
    ));
    assert!(matches!(
        state.apply(
            OperationCommand::FinishAction {
                token,
                sequence: action.sequence
            },
            11,
            &nodes,
            &mut placements
        ),
        Err(OperationError::StaleExecutor)
    ));
    state
        .apply(
            OperationCommand::FinishAction {
                token: next,
                sequence: action.sequence,
            },
            11,
            &nodes,
            &mut placements,
        )
        .unwrap();
    assert!(state.active.as_ref().unwrap().pending_action.is_none());
}

#[test]
fn drained_action_reassigns_without_retiring_the_healthy_process() {
    let (mut state, nodes, mut placements) = setup();
    let token = begin(
        &mut state,
        &nodes,
        &mut placements,
        OperationKind::MoveReplicas {
            source: 1,
            target: 4,
            groups: BTreeSet::from([RaftGroupId(0)]),
        },
        &[1, 2, 3, 4],
    );
    let OperationOutcome::ActionPrepared(receipt) = state
        .apply(
            OperationCommand::PrepareAction {
                token: token.clone(),
                group: RaftGroupId(0),
                leader: 2,
                action: MembershipAction::AddLearner { node_id: 4 },
            },
            10,
            &nodes,
            &mut placements,
        )
        .unwrap()
    else {
        panic!("receipt");
    };
    dispatch_pending(&mut state, &nodes, &mut placements);
    let mut wrong = receipt.clone();
    wrong.sequence = ActionSequence(100);
    assert!(matches!(
        state.apply(
            OperationCommand::ReassignAction {
                token: token.clone(),
                leader: 3,
                drained: Some(wrong)
            },
            10,
            &nodes,
            &mut placements
        ),
        Err(OperationError::InvalidTransition)
    ));
    let OperationOutcome::ActionPrepared(next) = state
        .apply(
            OperationCommand::ReassignAction {
                token,
                leader: 3,
                drained: Some(receipt.clone()),
            },
            10,
            &nodes,
            &mut placements,
        )
        .unwrap()
    else {
        panic!("replacement receipt");
    };
    dispatch_pending(&mut state, &nodes, &mut placements);
    assert_eq!(next.action, receipt.action);
    assert_eq!(next.sequence, receipt.sequence.checked_add(1).unwrap());
    assert_eq!(next.leader, 3);
    assert!(state.accepts_process(2, &receipt.process));
}

#[test]
fn participant_restart_does_not_prove_an_unknown_action_was_drained() {
    let (mut state, nodes, mut placements) = setup();
    let replica = replica(2);
    state
        .apply(
            OperationCommand::RegisterReplica {
                node_id: 2,
                process: identity(1, 2),
                identity: replica.clone(),
            },
            10,
            &nodes,
            &mut placements,
        )
        .unwrap();
    let token = begin(
        &mut state,
        &nodes,
        &mut placements,
        OperationKind::MoveReplicas {
            source: 1,
            target: 4,
            groups: BTreeSet::from([RaftGroupId(0)]),
        },
        &[1, 2, 3, 4],
    );
    let OperationOutcome::ActionPrepared(action) = state
        .apply(
            OperationCommand::PrepareAction {
                token: token.clone(),
                group: RaftGroupId(0),
                leader: 2,
                action: MembershipAction::AddLearner { node_id: 4 },
            },
            10,
            &nodes,
            &mut placements,
        )
        .unwrap()
    else {
        panic!("receipt");
    };
    dispatch_pending(&mut state, &nodes, &mut placements);
    state
        .apply(
            OperationCommand::RestartProcess {
                node_id: 2,
                previous: identity(1, 2),
                incarnation: ProcessIncarnation::from_bits(222),
                replica,
            },
            11,
            &nodes,
            &mut placements,
        )
        .unwrap();
    let before = state.clone();
    assert!(matches!(state.apply(OperationCommand::ReassignAction {
        token, leader: 3, drained: None,
    }, 11, &nodes, &mut placements), Err(OperationError::ActionOutcomeUnknown { sequence }) if sequence == action.sequence));
    assert_eq!(state, before);
    assert_eq!(
        state.active.as_ref().unwrap().pending_action,
        Some(PendingAction::OutcomeUnknown(action))
    );
}

proptest::proptest! {
    #[test]
    fn rejected_commands_preserve_the_entire_replicated_state(actions in proptest::collection::vec(0_u8..7, 1..64)) {
        let (mut state, nodes, mut placements) = setup();
        let token = begin(&mut state, &nodes, &mut placements, OperationKind::RebuildReplica { node_id: 1 }, &[1, 2, 3]);
        for action in actions {
            let command = match action {
                0 => OperationCommand::ClaimProcess { node_id: 4, expected_epoch: 1, incarnation: ProcessIncarnation::from_bits(100) },
                1 => OperationCommand::RetireSource { token: token.clone() },
                2 => OperationCommand::Complete { token: token.clone() },
                3 => OperationCommand::RegisterReplica { node_id: 1, process: identity(1, 1), identity: ReplicaIdentity { generation: 2, incarnation: ProcessIncarnation::from_bits(100) } },
                4 => OperationCommand::ActivateReplica { token: token.clone(), node_id: 1, identity: ReplicaIdentity { generation: 2, incarnation: ProcessIncarnation::from_bits(100) } },
                5 => OperationCommand::FinishReplicaFence { token: token.clone(), sequence: ActionSequence(1), committed_index: 100 },
                _ => OperationCommand::TakeOver { expected: OperationToken { generation: token.generation.checked_add(1).unwrap(), ..token.clone() }, executor: ProcessIncarnation::from_bits(100) },
            };
            let before = state.clone();
            let before_placements = placements.clone();
            state.apply(command, 10, &nodes, &mut placements).expect_err("invalid transition");
            proptest::prop_assert_eq!(&state, &before);
            proptest::prop_assert_eq!(&placements, &before_placements);
        }
        let encoded = serde_json::to_vec(&state).unwrap();
        let restored: OperationState = serde_json::from_slice(&encoded).unwrap();
        proptest::prop_assert_eq!(state, restored);
    }
}

fn dispatch_pending(
    state: &mut OperationState,
    nodes: &NodeStates,
    placements: &mut BTreeMap<RaftGroupId, DataGroupPlacement>,
) {
    let operation = state.active.as_ref().unwrap();
    let token = operation.token.clone();
    let sequence = operation
        .pending_action
        .as_ref()
        .unwrap()
        .receipt()
        .sequence;
    assert_eq!(
        state
            .apply(
                OperationCommand::MarkActionDispatched { token, sequence },
                10,
                nodes,
                placements
            )
            .unwrap(),
        OperationOutcome::ActionOutcome(ActionOutcome::Unknown)
    );
}

#[test]
fn prepared_action_cannot_dispatch_after_its_process_restarts() {
    let (mut state, nodes, mut placements) = setup();
    let replica = replica(2);
    state
        .apply(
            OperationCommand::RegisterReplica {
                node_id: 2,
                process: identity(1, 2),
                identity: replica.clone(),
            },
            10,
            &nodes,
            &mut placements,
        )
        .unwrap();
    let token = begin(
        &mut state,
        &nodes,
        &mut placements,
        OperationKind::MoveReplicas {
            source: 1,
            target: 4,
            groups: BTreeSet::from([RaftGroupId(0)]),
        },
        &[1, 2, 3, 4],
    );
    let OperationOutcome::ActionPrepared(action) = state
        .apply(
            OperationCommand::PrepareAction {
                token: token.clone(),
                group: RaftGroupId(0),
                leader: 2,
                action: MembershipAction::AddLearner { node_id: 4 },
            },
            10,
            &nodes,
            &mut placements,
        )
        .unwrap()
    else {
        panic!("receipt");
    };
    state
        .apply(
            OperationCommand::RestartProcess {
                node_id: 2,
                previous: identity(1, 2),
                incarnation: ProcessIncarnation::from_bits(222),
                replica,
            },
            11,
            &nodes,
            &mut placements,
        )
        .unwrap();
    let before = state.clone();
    assert_eq!(
        state.apply(
            OperationCommand::MarkActionDispatched {
                token: token.clone(),
                sequence: action.sequence
            },
            11,
            &nodes,
            &mut placements
        ),
        Err(OperationError::ProcessChanged { node_id: 2 })
    );
    assert_eq!(state, before);
    assert_eq!(
        state
            .apply(
                OperationCommand::CancelPreparedAction {
                    token,
                    sequence: action.sequence
                },
                11,
                &nodes,
                &mut placements
            )
            .unwrap(),
        OperationOutcome::ActionOutcome(ActionOutcome::NotDispatched)
    );
}

proptest::proptest! {
    #[test]
    fn action_protocol_matches_single_dispatch_model(steps in proptest::collection::vec(0_u8..5, 1..100)) {
        let (mut state, nodes, mut placements) = setup();
        let mut token = begin(&mut state, &nodes, &mut placements, OperationKind::MoveReplicas {
            source: 1, target: 4, groups: BTreeSet::from([RaftGroupId(0)]),
        }, &[1, 2, 3, 4]);
        // Independent reference: absent, prepared, or effect outcome unknown.
        let mut model = 0;
        for step in steps {
            let sequence = state.active.as_ref().unwrap().pending_action.as_ref().map_or(ActionSequence(0), |pending| pending.receipt().sequence);
            let command = match step {
                0 => OperationCommand::PrepareAction { token: token.clone(), group: RaftGroupId(0), leader: 2, action: MembershipAction::AddLearner { node_id: 4 } },
                1 => OperationCommand::MarkActionDispatched { token: token.clone(), sequence },
                2 => OperationCommand::CancelPreparedAction { token: token.clone(), sequence },
                3 => OperationCommand::FinishAction { token: token.clone(), sequence },
                _ => OperationCommand::TakeOver { expected: token.clone(), executor: ProcessIncarnation::from_bits(100) },
            };
            let before = state.clone();
            let result = state.apply(command, 10, &nodes, &mut placements);
            let permitted = match step { 0 | 4 => true, 1 | 2 => model == 1, _ => model == 2 };
            proptest::prop_assert_eq!(result.is_ok(), permitted);
            if permitted {
                match step { 0 if model == 0 => model = 1, 1 => model = 2, 2 | 3 => model = 0, _ => {} }
                if let Ok(OperationOutcome::Acquired(next)) = result { token = next; }
            } else { proptest::prop_assert_eq!(&state, &before); }
            let actual = match state.active.as_ref().unwrap().pending_action { None => 0, Some(PendingAction::Prepared(_)) => 1, Some(PendingAction::OutcomeUnknown(_)) => 2 };
            proptest::prop_assert_eq!(actual, model);
            let encoded = serde_json::to_vec(&state).unwrap();
            state = serde_json::from_slice(&encoded).unwrap();
        }
    }
}

fn install_admission(
    state: &mut OperationState,
    nodes: &NodeStates,
    placements: &mut BTreeMap<RaftGroupId, DataGroupPlacement>,
    token: &OperationToken,
    node_id: NodeId,
    identity: ReplicaIdentity,
    voters: &[NodeId],
) {
    let OperationOutcome::ActionPrepared(action) = state
        .apply(
            OperationCommand::PrepareAction {
                token: token.clone(),
                group: RaftGroupId(0),
                leader: 2,
                action: MembershipAction::InstallReplicaIdentity {
                    node_id,
                    identity: identity.clone(),
                },
            },
            11,
            nodes,
            placements,
        )
        .unwrap()
    else {
        panic!("admission receipt");
    };
    dispatch_pending(state, nodes, placements);
    let index = state
        .active
        .as_ref()
        .unwrap()
        .prefix_floor
        .get(&RaftGroupId(0))
        .copied()
        .unwrap_or(100);
    let mut proof = evidence(voters, voters, index);
    for replica in proof.replicas.values_mut() {
        replica
            .installed_replica_identities
            .insert(node_id, identity.clone());
    }
    state
        .apply(
            OperationCommand::Observe {
                token: token.clone(),
                evidence: proof,
            },
            11,
            nodes,
            placements,
        )
        .unwrap();
    state
        .apply(
            OperationCommand::FinishReplicaFence {
                token: token.clone(),
                sequence: action.sequence,
                committed_index: index,
            },
            11,
            nodes,
            placements,
        )
        .unwrap();
}

#[test]
fn reassignment_applies_the_prepare_policy_to_the_new_leader() {
    let (mut state, nodes, mut placements) = setup();
    let target = ReplicaIdentity {
        generation: 1,
        incarnation: ProcessIncarnation::from_bits(4),
    };
    state
        .apply(
            OperationCommand::RegisterReplica {
                node_id: 4,
                process: identity(1, 4),
                identity: target.clone(),
            },
            10,
            &nodes,
            &mut placements,
        )
        .unwrap();
    let token = begin(
        &mut state,
        &nodes,
        &mut placements,
        OperationKind::MoveReplicas {
            source: 1,
            target: 4,
            groups: BTreeSet::from([RaftGroupId(0)]),
        },
        &[1, 2, 3, 4],
    );
    let action = MembershipAction::InstallReplicaIdentity {
        node_id: 4,
        identity: target,
    };
    let OperationOutcome::ActionPrepared(receipt) = state
        .apply(
            OperationCommand::PrepareAction {
                token: token.clone(),
                group: RaftGroupId(0),
                leader: 2,
                action,
            },
            10,
            &nodes,
            &mut placements,
        )
        .unwrap()
    else {
        panic!("admission receipt");
    };
    dispatch_pending(&mut state, &nodes, &mut placements);
    let before = state.clone();
    // A prepare would refuse the admitted node as its own fence leader.
    assert_eq!(
        state.apply(
            OperationCommand::ReassignAction {
                token: token.clone(),
                leader: 4,
                drained: Some(receipt.clone()),
            },
            10,
            &nodes,
            &mut placements,
        ),
        Err(OperationError::ReplicaChanged { node_id: 4 })
    );
    assert_eq!(state, before);
    let stale = OperationToken {
        generation: token.generation.checked_add(1).unwrap(),
        ..token
    };
    assert_eq!(
        state.apply(
            OperationCommand::ReassignAction {
                token: stale,
                leader: 3,
                drained: None,
            },
            10,
            &nodes,
            &mut placements,
        ),
        Err(OperationError::StaleExecutor)
    );
    assert_eq!(state, before);
}

fn move_one_to_four(
    state: &mut OperationState,
    nodes: &NodeStates,
    placements: &mut BTreeMap<RaftGroupId, DataGroupPlacement>,
) -> OperationToken {
    begin(
        state,
        nodes,
        placements,
        OperationKind::MoveReplicas {
            source: 1,
            target: 4,
            groups: BTreeSet::from([RaftGroupId(0)]),
        },
        &[1, 2, 3, 4],
    )
}

fn prepare(
    state: &mut OperationState,
    nodes: &NodeStates,
    placements: &mut BTreeMap<RaftGroupId, DataGroupPlacement>,
    token: &OperationToken,
    action: MembershipAction,
) -> OperationAction {
    let OperationOutcome::ActionPrepared(receipt) = state
        .apply(
            OperationCommand::PrepareAction {
                token: token.clone(),
                group: RaftGroupId(0),
                leader: 2,
                action,
            },
            10,
            nodes,
            placements,
        )
        .unwrap()
    else {
        panic!("action receipt");
    };
    receipt
}

#[test]
fn begin_requires_an_active_replica_on_every_joining_node() {
    let (mut state, nodes, mut placements) = setup();
    state.replicas.remove(&4);
    let before = state.clone();
    let kinds = [
        OperationKind::MoveReplicas {
            source: 1,
            target: 4,
            groups: BTreeSet::from([RaftGroupId(0)]),
        },
        OperationKind::DecommissionNode {
            node_id: 1,
            replacements: BTreeMap::from([(RaftGroupId(0), 4)]),
        },
    ];
    for kind in kinds {
        assert_eq!(
            state.apply(
                OperationCommand::Begin {
                    kind,
                    executor: ProcessIncarnation::from_bits(99),
                    participants: [1, 2, 3, 4].map(|id| (id, identity(1, id))).into(),
                },
                10,
                &nodes,
                &mut placements,
            ),
            Err(OperationError::InactiveReplica { node_id: 4 })
        );
        assert_eq!(state, before);
    }
    state.replicas.insert(1, ReplicaState::Retired(replica(1)));
    let before = state.clone();
    assert_eq!(
        state.apply(
            OperationCommand::Begin {
                kind: OperationKind::RebuildReplica { node_id: 1 },
                executor: ProcessIncarnation::from_bits(99),
                participants: [1, 2, 3].map(|id| (id, identity(1, id))).into(),
            },
            10,
            &nodes,
            &mut placements,
        ),
        Err(OperationError::InactiveReplica { node_id: 1 })
    );
    assert_eq!(state, before);
}

#[test]
fn abort_discards_only_work_that_cannot_change_membership() {
    let (mut state, nodes, mut placements) = setup();
    let before = (state.processes.clone(), placements.clone());
    let token = move_one_to_four(&mut state, &nodes, &mut placements);
    // A dispatched effect that cannot change membership may be left unresolved.
    prepare(
        &mut state,
        &nodes,
        &mut placements,
        &token,
        MembershipAction::PrepareReplica,
    );
    dispatch_pending(&mut state, &nodes, &mut placements);
    assert_eq!(
        state.apply(
            OperationCommand::Abort {
                token: token.clone()
            },
            10,
            &nodes,
            &mut placements
        ),
        Ok(OperationOutcome::Aborted)
    );
    assert!(state.active.is_none());
    assert_eq!((state.processes.clone(), placements.clone()), before);
    assert_eq!(
        state.apply(
            OperationCommand::Abort { token },
            10,
            &nodes,
            &mut placements
        ),
        Err(OperationError::StaleExecutor)
    );

    // A prepared membership transition was never dispatched, so it is discarded too.
    let token = move_one_to_four(&mut state, &nodes, &mut placements);
    let learner = MembershipAction::AddLearner { node_id: 4 };
    prepare(&mut state, &nodes, &mut placements, &token, learner.clone());
    assert_eq!(
        state.apply(
            OperationCommand::Abort { token },
            10,
            &nodes,
            &mut placements
        ),
        Ok(OperationOutcome::Aborted)
    );

    // Dispatching one is the point of no return.
    let token = move_one_to_four(&mut state, &nodes, &mut placements);
    let receipt = prepare(&mut state, &nodes, &mut placements, &token, learner);
    dispatch_pending(&mut state, &nodes, &mut placements);
    let phase = OperationPhase::Reconfiguring {
        since: receipt.sequence,
    };
    assert_eq!(state.active.as_ref().unwrap().phase, phase);
    state
        .apply(
            OperationCommand::FinishAction {
                token: token.clone(),
                sequence: receipt.sequence,
            },
            10,
            &nodes,
            &mut placements,
        )
        .unwrap();
    let reconfiguring = state.clone();
    assert_eq!(
        state.apply(
            OperationCommand::Abort { token },
            10,
            &nodes,
            &mut placements
        ),
        Err(OperationError::Irreversible { phase })
    );
    assert_eq!(state, reconfiguring);
}

#[test]
fn abort_is_refused_after_the_source_retires() {
    let (mut state, nodes, mut placements) = setup();
    let token = begin(
        &mut state,
        &nodes,
        &mut placements,
        OperationKind::RebuildReplica { node_id: 1 },
        &[1, 2, 3],
    );
    state
        .apply(
            OperationCommand::Observe {
                token: token.clone(),
                evidence: evidence(&[1, 2, 3], &[2, 3], 100),
            },
            10,
            &nodes,
            &mut placements,
        )
        .unwrap();
    state
        .apply(
            OperationCommand::RetireSource {
                token: token.clone(),
            },
            10,
            &nodes,
            &mut placements,
        )
        .unwrap();
    let retired = state.clone();
    assert_eq!(
        state.apply(
            OperationCommand::Abort { token },
            10,
            &nodes,
            &mut placements
        ),
        Err(OperationError::Irreversible {
            phase: OperationPhase::Retired
        })
    );
    assert_eq!(state, retired);
}

#[test]
fn a_replaced_target_blocks_the_operation_observably() {
    let (mut state, nodes, mut placements) = setup();
    let token = move_one_to_four(&mut state, &nodes, &mut placements);
    // The source is not needed for completion, so its claim stays refused.
    assert_eq!(
        state.apply(
            OperationCommand::ClaimProcess {
                node_id: 1,
                expected_epoch: 1,
                incarnation: ProcessIncarnation::from_bits(11),
            },
            10,
            &nodes,
            &mut placements,
        ),
        Err(OperationError::Busy)
    );
    let receipt = prepare(
        &mut state,
        &nodes,
        &mut placements,
        &token,
        MembershipAction::AddLearner { node_id: 4 },
    );
    dispatch_pending(&mut state, &nodes, &mut placements);
    // The target lost its disk: it claims a new process instead of restarting.
    let OperationOutcome::ProcessClaimed(claimed) = state
        .apply(
            OperationCommand::ClaimProcess {
                node_id: 4,
                expected_epoch: 1,
                incarnation: ProcessIncarnation::from_bits(44),
            },
            11,
            &nodes,
            &mut placements,
        )
        .unwrap()
    else {
        panic!("claim");
    };
    let block = OperationBlock::ParticipantReplaced {
        pinned: identity(1, 4),
        claimed,
    };
    let operation = state.active.as_ref().unwrap();
    assert_eq!(operation.blocked.get(&4), Some(&block));
    assert_eq!(operation.participants.get(&4), Some(&identity(1, 4)));
    let blocked = Err(OperationError::Blocked {
        node_id: 4,
        block: block.clone(),
    });
    state
        .apply(
            OperationCommand::FinishAction {
                token: token.clone(),
                sequence: receipt.sequence,
            },
            11,
            &nodes,
            &mut placements,
        )
        .unwrap();
    let before = state.clone();
    for command in [
        OperationCommand::Complete {
            token: token.clone(),
        },
        OperationCommand::PrepareAction {
            token: token.clone(),
            group: RaftGroupId(0),
            leader: 2,
            action: MembershipAction::ChangeVoters,
        },
    ] {
        assert_eq!(state.apply(command, 11, &nodes, &mut placements), blocked);
        assert_eq!(state, before);
    }
    assert!(matches!(
        state.apply(
            OperationCommand::Abort { token },
            11,
            &nodes,
            &mut placements
        ),
        Err(OperationError::Irreversible { .. })
    ));
}

#[test]
fn a_blocked_operation_can_still_abort_before_the_point_of_no_return() {
    let (mut state, nodes, mut placements) = setup();
    let token = move_one_to_four(&mut state, &nodes, &mut placements);
    state
        .apply(
            OperationCommand::ClaimProcess {
                node_id: 3,
                expected_epoch: 1,
                incarnation: ProcessIncarnation::from_bits(33),
            },
            10,
            &nodes,
            &mut placements,
        )
        .unwrap();
    assert!(state.active.as_ref().unwrap().blocked.contains_key(&3));
    assert_eq!(
        state.apply(
            OperationCommand::Abort { token },
            10,
            &nodes,
            &mut placements
        ),
        Ok(OperationOutcome::Aborted)
    );
    assert!(matches!(
        state.processes.get(&3),
        Some(ProcessState::Active(ProcessIdentity { epoch: 2, .. }))
    ));
}

//! Replacement activation must follow durable evidence from every data quorum.

use std::collections::BTreeMap;
use std::collections::BTreeSet;

use ursula_proto::admin::ProcessIncarnation;
use ursula_proto::admin::ReplicaIdentity;
use ursula_shard::RaftGroupId;

use crate::DataGroupPlacement;
use crate::MembershipAction;
use crate::OperationCommand;
use crate::OperationError;
use crate::OperationKind;
use crate::OperationOutcome;
use crate::OperationState;
use crate::OperationToken;
use crate::PrefixEvidence;
use crate::ProcessIdentity;
use crate::ProcessState;
use crate::ReplicaEvidence;
use crate::ReplicaState;

fn process(epoch: u64, node: u64) -> ProcessIdentity {
    ProcessIdentity {
        epoch,
        incarnation: ProcessIncarnation::from_bits(u128::from(node)),
    }
}

fn replica(generation: u64, bits: u128) -> ReplicaIdentity {
    ReplicaIdentity {
        generation,
        incarnation: ProcessIncarnation::from_bits(bits),
    }
}

struct Rig {
    state: OperationState,
    nodes: BTreeSet<u64>,
    placements: BTreeMap<RaftGroupId, DataGroupPlacement>,
}

impl Rig {
    fn new() -> Self {
        let nodes = BTreeSet::from([1, 2, 3]);
        let mut rig = Self {
            state: OperationState {
                processes: nodes
                    .iter()
                    .map(|id| (*id, ProcessState::Active(process(1, *id))))
                    .collect(),
                ..Default::default()
            },
            placements: [RaftGroupId(0), RaftGroupId(1)]
                .into_iter()
                .map(|group| {
                    (group, DataGroupPlacement {
                        raft_group_id: group,
                        voters: nodes.clone(),
                        learners: BTreeSet::new(),
                        draining: BTreeSet::new(),
                        epoch: 1,
                        updated_at_ms: 0,
                    })
                })
                .collect(),
            nodes,
        };
        for id in [1, 2, 3] {
            rig.apply(OperationCommand::RegisterReplica {
                node_id: id,
                process: process(1, id),
                identity: replica(1, u128::from(id)),
            })
            .unwrap();
        }
        rig
    }

    fn apply(&mut self, command: OperationCommand) -> Result<OperationOutcome, OperationError> {
        self.state
            .apply(command, 10, &self.nodes, &mut self.placements)
    }

    fn observe(&mut self, token: &OperationToken, group: RaftGroupId, installed: &[u64]) {
        self.apply(OperationCommand::Observe {
            token: token.clone(),
            evidence: PrefixEvidence {
                raft_group_id: group,
                leader: 2,
                term: 1,
                committed_index: 100,
                voters: BTreeSet::from([2, 3]),
                joint: false,
                replicas: [2, 3]
                    .into_iter()
                    .map(|node| {
                        (node, ReplicaEvidence {
                            process: process(1, node),
                            applied_index: 100,
                            installed_replica_identities: if installed.contains(&node) {
                                BTreeMap::from([(1, replica(2, 11))])
                            } else {
                                BTreeMap::new()
                            },
                        })
                    })
                    .collect(),
                observed_at_ms: 10,
            },
        })
        .unwrap();
    }

    fn replacement(&mut self) -> OperationToken {
        let OperationOutcome::Acquired(token) = self
            .apply(OperationCommand::Begin {
                kind: OperationKind::RebuildReplica { node_id: 1 },
                executor: ProcessIncarnation::from_bits(99),
                participants: [1, 2, 3]
                    .into_iter()
                    .map(|id| (id, process(1, id)))
                    .collect(),
                meta_voters: self.nodes.clone(),
            })
            .unwrap()
        else {
            panic!("operation token")
        };
        for group in [RaftGroupId(0), RaftGroupId(1)] {
            self.observe(&token, group, &[]);
        }
        self.apply(OperationCommand::RetireSource {
            token: token.clone(),
        })
        .unwrap();
        assert_eq!(
            self.state.replicas.get(&1),
            Some(&ReplicaState::Retired(replica(1, 1)))
        );
        self.apply(OperationCommand::ClaimProcess {
            node_id: 1,
            expected_epoch: 1,
            incarnation: ProcessIncarnation::from_bits(11),
        })
        .unwrap();
        self.apply(OperationCommand::RegisterReplica {
            node_id: 1,
            process: process(2, 11),
            identity: replica(2, 11),
        })
        .unwrap();
        token
    }
}

#[test]
fn ordinary_restart_preserves_replica_identity_and_rejects_unplanned_new_wal() {
    let mut rig = Rig::new();
    rig.apply(OperationCommand::ClaimProcess {
        node_id: 1,
        expected_epoch: 1,
        incarnation: ProcessIncarnation::from_bits(11),
    })
    .unwrap();
    rig.apply(OperationCommand::RegisterReplica {
        node_id: 1,
        process: process(2, 11),
        identity: replica(1, 1),
    })
    .unwrap();
    let before = rig.state.clone();
    assert_eq!(
        rig.apply(OperationCommand::RegisterReplica {
            node_id: 1,
            process: process(2, 11),
            identity: replica(2, 11),
        }),
        Err(OperationError::ReplicaChanged { node_id: 1 })
    );
    assert_eq!(rig.state, before);
}

#[test]
fn replacement_requires_durable_quorum_evidence_for_every_group_before_promotion() {
    let mut rig = Rig::new();
    let token = rig.replacement();
    // Restart while only the meta listener is up: boot pins change, the pending
    // WAL identity and eventual quorum installation obligation do not.
    rig.apply(OperationCommand::ClaimProcess {
        node_id: 1,
        expected_epoch: 2,
        incarnation: ProcessIncarnation::from_bits(111),
    })
    .unwrap();
    rig.apply(OperationCommand::RegisterReplica {
        node_id: 1,
        process: process(3, 111),
        identity: replica(2, 11),
    })
    .unwrap();
    let activate = OperationCommand::ActivateReplica {
        token: token.clone(),
        node_id: 1,
        identity: replica(2, 11),
    };
    for group in [RaftGroupId(0), RaftGroupId(1)] {
        assert_eq!(
            rig.apply(activate.clone()),
            Err(OperationError::ReplicaChanged { node_id: 1 })
        );
        assert_eq!(
            rig.apply(OperationCommand::PrepareAction {
                token: token.clone(),
                group,
                leader: 2,
                action: MembershipAction::ChangeVoters,
            }),
            Err(OperationError::ReplicaChanged { node_id: 1 })
        );
        let OperationOutcome::ActionPrepared(receipt) = rig
            .apply(OperationCommand::PrepareAction {
                token: token.clone(),
                group,
                leader: 2,
                action: MembershipAction::InstallReplicaIdentity {
                    node_id: 1,
                    identity: replica(2, 11),
                },
            })
            .unwrap()
        else {
            panic!("fence receipt")
        };
        let finish = OperationCommand::FinishReplicaFence {
            token: token.clone(),
            sequence: receipt.sequence,
            committed_index: 100,
        };
        assert_eq!(
            rig.apply(OperationCommand::FinishAction {
                token: token.clone(),
                sequence: receipt.sequence,
            }),
            Err(OperationError::InvalidTransition)
        );
        rig.observe(&token, group, &[2]);
        let before = rig.state.clone();
        assert_eq!(
            rig.apply(finish.clone()),
            Err(OperationError::MissingEvidence {
                raft_group_id: group
            })
        );
        assert_eq!(rig.state, before);
        rig.observe(&token, group, &[2, 3]);
        assert_eq!(
            rig.apply(OperationCommand::FinishReplicaFence {
                token: token.clone(),
                sequence: receipt.sequence,
                committed_index: 101,
            }),
            Err(OperationError::MissingEvidence {
                raft_group_id: group
            })
        );
        rig.apply(finish).unwrap();
        // A meta snapshot/restart between groups must preserve partial progress.
        rig.state = serde_json::from_slice(&serde_json::to_vec(&rig.state).unwrap()).unwrap();
    }
    rig.apply(activate.clone()).unwrap();
    rig.apply(activate).unwrap();
    assert_eq!(
        rig.state.replicas.get(&1),
        Some(&ReplicaState::Active {
            identity: replica(2, 11),
            installed_groups: BTreeMap::from([(RaftGroupId(0), 100), (RaftGroupId(1), 100)])
        })
    );
    rig.apply(OperationCommand::PrepareAction {
        token,
        group: RaftGroupId(0),
        leader: 2,
        action: MembershipAction::ChangeVoters,
    })
    .unwrap();
}

#[test]
fn participant_restarts_preserve_unresolved_actions_and_require_same_wal_identity() {
    for restarted_node in [1, 2, 3] {
        let mut rig = Rig::new();
        let OperationOutcome::Acquired(token) = rig
            .apply(OperationCommand::Begin {
                kind: OperationKind::RebuildReplica { node_id: 1 },
                executor: ProcessIncarnation::from_bits(99),
                participants: [1, 2, 3]
                    .into_iter()
                    .map(|id| (id, process(1, id)))
                    .collect(),
                meta_voters: rig.nodes.clone(),
            })
            .unwrap()
        else {
            panic!("operation token")
        };
        let OperationOutcome::ActionPrepared(receipt) = rig
            .apply(OperationCommand::PrepareAction {
                token: token.clone(),
                group: RaftGroupId(0),
                leader: 2,
                action: MembershipAction::RetireReplica,
            })
            .unwrap()
        else {
            panic!("action receipt")
        };
        let before = rig.state.clone();
        assert_eq!(
            rig.apply(OperationCommand::RestartProcess {
                node_id: restarted_node,
                previous: process(1, restarted_node),
                incarnation: ProcessIncarnation::from_bits(100),
                replica: replica(2, 100),
            }),
            Err(OperationError::ReplicaChanged {
                node_id: restarted_node
            })
        );
        assert_eq!(rig.state, before);
        rig.apply(OperationCommand::RestartProcess {
            node_id: restarted_node,
            previous: process(1, restarted_node),
            incarnation: ProcessIncarnation::from_bits(100),
            replica: replica(1, u128::from(restarted_node)),
        })
        .unwrap();
        let operation = rig.state.active.as_ref().unwrap();
        assert_eq!(
            operation.participants.get(&restarted_node),
            Some(&process(2, 100))
        );
        assert_eq!(operation.pending_action.as_ref(), Some(&receipt));
        if restarted_node == 2 {
            let OperationOutcome::ActionPrepared(reassigned) = rig
                .apply(OperationCommand::ReassignAction {
                    token,
                    leader: 2,
                    drained: None,
                })
                .unwrap()
            else {
                panic!("reassigned receipt")
            };
            assert_eq!(reassigned.process, process(2, 100));
            assert!(reassigned.sequence > receipt.sequence);
        }
    }
}

#[test]
fn joining_non_meta_voter_needs_group_admission_before_promotion() {
    let mut rig = Rig::new();
    rig.nodes.insert(4);
    rig.state
        .processes
        .insert(4, ProcessState::Active(process(1, 4)));
    rig.apply(OperationCommand::RegisterReplica {
        node_id: 4,
        process: process(1, 4),
        identity: replica(1, 4),
    })
    .unwrap();
    let OperationOutcome::Acquired(token) = rig
        .apply(OperationCommand::Begin {
            kind: OperationKind::MoveReplicas {
                source: 3,
                target: 4,
                groups: BTreeSet::from([RaftGroupId(0)]),
            },
            executor: ProcessIncarnation::from_bits(99),
            participants: [1, 2, 3, 4]
                .into_iter()
                .map(|node| (node, process(1, node)))
                .collect(),
            meta_voters: BTreeSet::from([1, 2, 3]),
        })
        .unwrap()
    else {
        panic!("move token")
    };
    assert_eq!(
        rig.apply(OperationCommand::PrepareAction {
            token: token.clone(),
            group: RaftGroupId(0),
            leader: 2,
            action: MembershipAction::ChangeVoters,
        }),
        Err(OperationError::ReplicaChanged { node_id: 4 })
    );
    let OperationOutcome::ActionPrepared(receipt) = rig
        .apply(OperationCommand::PrepareAction {
            token: token.clone(),
            group: RaftGroupId(0),
            leader: 2,
            action: MembershipAction::InstallReplicaIdentity {
                node_id: 4,
                identity: replica(1, 4),
            },
        })
        .unwrap()
    else {
        panic!("admission receipt")
    };
    rig.apply(OperationCommand::Observe {
        token: token.clone(),
        evidence: PrefixEvidence {
            raft_group_id: RaftGroupId(0),
            leader: 2,
            term: 1,
            committed_index: 100,
            voters: BTreeSet::from([1, 2, 3]),
            joint: false,
            observed_at_ms: 10,
            replicas: [1, 2, 3]
                .into_iter()
                .map(|node| {
                    (node, ReplicaEvidence {
                        process: process(1, node),
                        applied_index: 100,
                        installed_replica_identities: BTreeMap::from([(4, replica(1, 4))]),
                    })
                })
                .collect(),
        },
    })
    .unwrap();
    rig.apply(OperationCommand::FinishReplicaFence {
        token: token.clone(),
        sequence: receipt.sequence,
        committed_index: 100,
    })
    .unwrap();
    let OperationOutcome::ActionPrepared(receipt) = rig
        .apply(OperationCommand::PrepareAction {
            token: token.clone(),
            group: RaftGroupId(0),
            leader: 2,
            action: MembershipAction::ChangeVoters,
        })
        .unwrap()
    else {
        panic!("promotion receipt")
    };
    rig.apply(OperationCommand::FinishAction {
        token: token.clone(),
        sequence: receipt.sequence,
    })
    .unwrap();
    rig.apply(OperationCommand::Observe {
        token: token.clone(),
        evidence: PrefixEvidence {
            raft_group_id: RaftGroupId(0),
            leader: 2,
            term: 1,
            committed_index: 101,
            voters: BTreeSet::from([1, 2, 4]),
            joint: false,
            observed_at_ms: 10,
            replicas: [1, 2, 4]
                .into_iter()
                .map(|node| {
                    (node, ReplicaEvidence {
                        process: process(1, node),
                        applied_index: 101,
                        installed_replica_identities: BTreeMap::from([(4, replica(1, 4))]),
                    })
                })
                .collect(),
        },
    })
    .unwrap();
    rig.apply(OperationCommand::Complete { token }).unwrap();
    // Data hosting never implies membership in the separate meta voter set.
    rig.apply(OperationCommand::Begin {
        kind: OperationKind::MoveReplicas {
            source: 4,
            target: 3,
            groups: BTreeSet::from([RaftGroupId(0)]),
        },
        executor: ProcessIncarnation::from_bits(101),
        participants: [1, 2, 3, 4]
            .into_iter()
            .map(|node| (node, process(1, node)))
            .collect(),
        meta_voters: BTreeSet::from([1, 2, 3]),
    })
    .unwrap();
}

use super::CompletedReceiverMutation;
use super::MembershipLogId;
use super::MigrationToken;
use super::PendingReceiverMutation;
use super::ProcessIncarnation;
use super::RaftGroupId;
use super::ReceiverFencePhase;
use super::ReceiverFenceRecord;
use super::ReceiverLedger;
use super::ReceiverMutationKind;
use super::ReceiverProcess;
use super::ReplicaAssignment;
use super::ReplicaAssignmentPhase;
use super::ReplicaMutationResult;
use super::ReplicaRetirementEvidence;

fn token(generation: u64) -> MigrationToken {
    MigrationToken {
        migration_id: 1,
        generation,
        executor: ReceiverProcess {
            node_id: 1,
            incarnation: ProcessIncarnation::from_bits(100),
        },
    }
}

fn active() -> ReceiverLedger {
    ReceiverLedger {
        assignments_seeded: true,
        high_water_generation: 7,
        fence: Some(ReceiverFenceRecord {
            token: token(7),
            process: ProcessIncarnation::from_bits(10),
            phase: ReceiverFencePhase::Active,
        }),
        ..ReceiverLedger::default()
    }
}

fn preparing() -> ReceiverLedger {
    let mut ledger = active();
    ledger
        .assignments
        .insert(RaftGroupId(0), ReplicaAssignment {
            epoch: 0,
            migration_id: 1,
            generation: 7,
            phase: ReplicaAssignmentPhase::Preparing,
        });
    ledger.pending = Some(PendingReceiverMutation {
        token: token(7),
        raft_group_id: RaftGroupId(0),
        request_id: "prepare".to_owned(),
        process: ProcessIncarnation::from_bits(10),
        operation: ReceiverMutationKind::PrepareReplica { epoch: 0 },
    });
    ledger
}

fn prepared(pending: &ReceiverLedger) -> ReceiverLedger {
    let mut ledger = pending.clone();
    let request = ledger.pending.take().unwrap();
    ledger.assignments.get_mut(&RaftGroupId(0)).unwrap().phase = ReplicaAssignmentPhase::Hosted;
    ledger.completed = Some(CompletedReceiverMutation {
        request: request.clone(),
        result: ReplicaMutationResult::Prepared {
            process: ReceiverProcess {
                node_id: 2,
                incarnation: request.process,
            },
        },
    });
    ledger
}

#[test]
fn replica_pending_cannot_be_forgotten_replaced_or_completed_without_matching_assignment() {
    let pending = preparing();
    assert!(active().validate_successor(&pending, 2).is_ok());
    let mut forgotten = pending.clone();
    forgotten.pending = None;
    assert!(pending.validate_successor(&forgotten, 2).is_err());
    let mut replaced = pending.clone();
    replaced.pending.as_mut().unwrap().request_id = "another-request".to_owned();
    assert!(pending.validate_successor(&replaced, 2).is_err());
    let ready = prepared(&pending);
    assert!(pending.validate_successor(&ready, 2).is_ok());
    let mut unhosted = ready.clone();
    unhosted.assignments.get_mut(&RaftGroupId(0)).unwrap().phase =
        ReplicaAssignmentPhase::Preparing;
    assert!(pending.validate_successor(&unhosted, 2).is_err());
    assert!(unhosted.validate(2).is_err());
    let mut wrong_process = ready.clone();
    let ReplicaMutationResult::Prepared { process } =
        &mut wrong_process.completed.as_mut().unwrap().result
    else {
        unreachable!()
    };
    process.incarnation = ProcessIncarnation::from_bits(11);
    assert!(pending.validate_successor(&wrong_process, 2).is_err());
    let mut reused = ready.clone();
    reused.pending = Some(pending.pending.clone().unwrap());
    reused.pending.as_mut().unwrap().request_id = "forgotten-old-key".to_owned();
    assert!(ready.validate_successor(&reused, 2).is_err());
    let mut rewritten_receipt = ready.clone();
    rewritten_receipt
        .completed
        .as_mut()
        .unwrap()
        .request
        .request_id = "rewritten".to_owned();
    assert!(ready.validate_successor(&rewritten_receipt, 2).is_err());
}

#[test]
fn replica_pending_rebind_requires_new_activation_and_preserves_operation_epoch_and_key() {
    let pending = preparing();
    let mut activating = pending.clone();
    activating.high_water_generation = 8;
    activating.fence = Some(ReceiverFenceRecord {
        token: token(8),
        process: ProcessIncarnation::from_bits(11),
        phase: ReceiverFencePhase::Activating,
    });
    assert!(pending.validate_successor(&activating, 2).is_ok());
    let mut rebinding = activating.clone();
    let request = rebinding.pending.as_mut().unwrap();
    request.token = token(8);
    request.process = ProcessIncarnation::from_bits(11);
    rebinding
        .assignments
        .get_mut(&RaftGroupId(0))
        .unwrap()
        .generation = 8;
    assert!(activating.validate_successor(&rebinding, 2).is_ok());
    for field in ["epoch", "key", "intent", "group", "active"] {
        let mut changed = rebinding.clone();
        let request = changed.pending.as_mut().unwrap();
        match field {
            "epoch" => request.operation = ReceiverMutationKind::PrepareReplica { epoch: 1 },
            "key" => request.request_id = "replacement".to_owned(),
            "intent" => request.token.migration_id = 2,
            "group" => request.raft_group_id = RaftGroupId(1),
            "active" => changed.fence.as_mut().unwrap().phase = ReceiverFencePhase::Active,
            _ => unreachable!(),
        }
        assert!(
            activating.validate_successor(&changed, 2).is_err(),
            "{field}"
        );
    }
    let mut same_generation = pending.clone();
    same_generation.pending.as_mut().unwrap().process = ProcessIncarnation::from_bits(11);
    same_generation.fence.as_mut().unwrap().process = ProcessIncarnation::from_bits(11);
    assert!(pending.validate_successor(&same_generation, 2).is_err());
    let ready = prepared(&rebinding);
    assert!(rebinding.validate_successor(&ready, 2).is_ok());
    let mut active = ready.clone();
    active.fence.as_mut().unwrap().phase = ReceiverFencePhase::Active;
    assert!(ready.validate_successor(&active, 2).is_ok());
}

#[test]
fn replica_release_receipt_requires_matching_witness_and_complete_retirement() {
    let mut pending = preparing();
    let witness = MembershipLogId {
        term: 3,
        node_id: 1,
        index: 17,
    };
    pending.assignments.get_mut(&RaftGroupId(0)).unwrap().phase = ReplicaAssignmentPhase::Retiring;
    pending.assignments.get_mut(&RaftGroupId(0)).unwrap().epoch = 1;
    pending.pending.as_mut().unwrap().operation = ReceiverMutationKind::ReleaseReplica {
        epoch: 1,
        membership_log_id: witness.clone(),
    };
    assert!(!pending.may_restore(RaftGroupId(0)));
    let mut retired = pending.clone();
    let request = retired.pending.take().unwrap();
    retired.assignments.get_mut(&RaftGroupId(0)).unwrap().phase = ReplicaAssignmentPhase::Retired;
    retired.completed = Some(CompletedReceiverMutation {
        request,
        result: ReplicaMutationResult::Released {
            evidence: ReplicaRetirementEvidence {
                process: ReceiverProcess {
                    node_id: 2,
                    incarnation: ProcessIncarnation::from_bits(10),
                },
                placement_epoch: 1,
                membership_log_id: witness,
                work_drained: true,
                snapshot_references_retired: true,
                local_records_reclaimed: true,
            },
        },
    });
    assert!(pending.validate_successor(&retired, 2).is_ok());
    for field in ["epoch", "witness", "work", "references", "records"] {
        let mut incomplete = retired.clone();
        let ReplicaMutationResult::Released { evidence } =
            &mut incomplete.completed.as_mut().unwrap().result
        else {
            unreachable!()
        };
        match field {
            "epoch" => evidence.placement_epoch = 2,
            "witness" => evidence.membership_log_id.index = 18,
            "work" => evidence.work_drained = false,
            "references" => evidence.snapshot_references_retired = false,
            "records" => evidence.local_records_reclaimed = false,
            _ => unreachable!(),
        }
        assert!(
            pending.validate_successor(&incomplete, 2).is_err(),
            "{field}"
        );
    }
    let mut rehosted = retired.clone();
    rehosted.assignments.get_mut(&RaftGroupId(0)).unwrap().phase = ReplicaAssignmentPhase::Hosted;
    assert!(retired.validate_successor(&rehosted, 2).is_err());
}

fn membership_pending(id: &str, ledger: &ReceiverLedger) -> ReceiverLedger {
    let mut pending = ledger.clone();
    pending.pending = Some(PendingReceiverMutation {
        token: token(7),
        raft_group_id: RaftGroupId(0),
        request_id: id.to_owned(),
        process: ProcessIncarnation::from_bits(10),
        operation: super::ReceiverMutationKind::ManagedMembership {
            step: super::MembershipStep::ChangeVoters {
                epoch: 0,
                target_voters: [1, 3, 4].into(),
            },
        },
    });
    pending
}

fn membership_complete(pending: &ReceiverLedger, joint: bool) -> ReceiverLedger {
    let mut ready = pending.clone();
    let request = ready.pending.take().unwrap();
    ready.membership_completed.insert(
        request.request_id.clone(),
        super::CompletedMembershipMutation {
            request,
            process: ReceiverProcess {
                node_id: 2,
                incarnation: ProcessIncarnation::from_bits(10),
            },
            configuration: crate::CommittedGroupConfiguration {
                raft_group_id: RaftGroupId(0),
                leader_id: 1,
                leader_term: 2,
                membership_log_id: MembershipLogId {
                    term: 2,
                    node_id: 1,
                    index: 17,
                },
                applied_log_id: MembershipLogId {
                    term: 2,
                    node_id: 1,
                    index: 18,
                },
                voter_sets: if joint {
                    vec![[1, 2, 3].into(), [1, 3, 4].into()]
                } else {
                    vec![[1, 3, 4].into()]
                },
                learners: Default::default(),
                nodes: if joint {
                    (1..=4)
                        .map(|id| (id, format!("http://node{id}:4440")))
                        .collect()
                } else {
                    [1, 3, 4]
                        .map(|id| (id, format!("http://node{id}:4440")))
                        .into()
                },
            },
            outcome: if joint {
                super::MembershipOutcome::Reconciled
            } else {
                super::MembershipOutcome::Applied
            },
        },
    );
    ready
}

#[test]
fn membership_receipts_preserve_keys_and_bound_generation_state_without_rewriting_replies() {
    let mut ledger = active();
    for index in 0..super::MAX_MEMBERSHIP_RECEIPTS {
        let pending = membership_pending(&format!("membership-{index}"), &ledger);
        assert!(ledger.validate_successor(&pending, 2).is_ok());
        let ready = membership_complete(&pending, false);
        assert!(pending.validate_successor(&ready, 2).is_ok());
        ledger = ready;
    }
    assert!(
        ledger
            .validate_successor(&membership_pending("overflow", &ledger), 2)
            .is_err()
    );
    let mut forgotten = ledger.clone();
    forgotten.membership_completed.clear();
    assert!(ledger.validate_successor(&forgotten, 2).is_err());
    let mut rewritten = ledger.clone();
    rewritten
        .membership_completed
        .get_mut("membership-0")
        .unwrap()
        .outcome = super::MembershipOutcome::Reconciled;
    assert!(ledger.validate_successor(&rewritten, 2).is_err());
    let mut next_generation = forgotten;
    next_generation.high_water_generation = 8;
    next_generation.fence.as_mut().unwrap().token = token(8);
    next_generation.fence.as_mut().unwrap().phase = ReceiverFencePhase::Activating;
    next_generation.fence.as_mut().unwrap().process = ProcessIncarnation::from_bits(11);
    assert!(ledger.validate_successor(&next_generation, 2).is_ok());
}

#[test]
fn membership_reconciliation_reports_joint_state_without_claiming_the_voter_change_applied() {
    let pending = membership_pending("interrupted-joint", &active());
    let reconciled = membership_complete(&pending, true);
    assert!(pending.validate_successor(&reconciled, 2).is_ok());
    let mut false_success = reconciled.clone();
    false_success
        .membership_completed
        .get_mut("interrupted-joint")
        .unwrap()
        .outcome = super::MembershipOutcome::Applied;
    assert!(pending.validate_successor(&false_success, 2).is_err());
    let mut forged = reconciled.clone();
    forged
        .membership_completed
        .get_mut("interrupted-joint")
        .unwrap()
        .request
        .request_id = "other-key".to_owned();
    assert!(pending.validate_successor(&forged, 2).is_err());
    let mut cleared = pending.clone();
    cleared.pending = None;
    assert!(pending.validate_successor(&cleared, 2).is_err());
}

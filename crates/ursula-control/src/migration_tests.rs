use std::collections::BTreeMap;
use std::collections::BTreeSet;

use ursula_proto::admin::ProcessIncarnation;
use ursula_shard::RaftGroupId;

use crate::ClusterBootstrap;
use crate::ClusterId;
use crate::ClusterIdentity;
use crate::ControlCommand;
use crate::ControlPlaneState;
use crate::ControlResponse;
use crate::FinalMembershipEvidence;
use crate::GroupPlacementPolicy;
use crate::MembershipLogId;
use crate::MigrationPhase;
use crate::MigrationRequest;
use crate::MigrationToken;
use crate::MigrationUpdate;
use crate::NodeRegistration;
use crate::PlacementPolicy;
use crate::ReceiverProcess;
use crate::ReplicaAppliedEvidence;
use crate::ReplicaRetirementEvidence;
use crate::ReplicationFactor;
use crate::RoutingHashVersion;
use crate::VerifiedGroupMembership;

fn log(index: u64) -> MembershipLogId {
    MembershipLogId {
        term: 1,
        node_id: 1,
        index,
    }
}
fn process(node_id: u64) -> ReceiverProcess {
    ReceiverProcess {
        node_id,
        incarnation: ProcessIncarnation::from_bits(u128::from(node_id)),
    }
}

fn fixture(rf: ReplicationFactor) -> ControlPlaneState {
    let nodes = (1..=6)
        .map(|id| {
            (id, NodeRegistration {
                node_id: id,
                client_url: format!("http://node{id}:4437"),
                cluster_url: format!("http://node{id}:4439"),
                admin_url: format!("http://node{id}:4438"),
                labels: BTreeMap::from([("zone".to_owned(), ((id - 1) % 3).to_string())]),
            })
        })
        .collect();
    let voters = (1..=rf.voter_count() as u64).collect::<BTreeSet<_>>();
    let bootstrap = ClusterBootstrap {
        identity: ClusterIdentity {
            cluster_id: ClusterId::try_from("intent-test".to_owned()).unwrap(),
            group_count: 1,
            core_count: 1,
            routing_hash: RoutingHashVersion::Fnv1a64BucketSlashStreamV1,
        },
        initial_meta_voters: [1, 2, 3].into(),
        nodes,
        voters: BTreeMap::from([(RaftGroupId(0), voters.clone())]),
        placement: PlacementPolicy {
            default_replication_factor: rf,
            ..Default::default()
        },
    };
    let mut state = ControlPlaneState::default();
    assert_eq!(
        state.apply(ControlCommand::BootstrapCluster {
            bootstrap,
            memberships: BTreeMap::from([(RaftGroupId(0), VerifiedGroupMembership {
                voters,
                learners: BTreeSet::new(),
                log_id: log(1)
            })]),
            now_ms: 1
        }),
        ControlResponse::Ok
    );
    state
}

fn new_request(
    state: &ControlPlaneState,
    key: &str,
    target: &[u64],
    rf: Option<ReplicationFactor>,
) -> MigrationRequest {
    let placement = &state.placements[&RaftGroupId(0)];
    let source = state
        .migrations
        .values()
        .filter_map(|m| m.managed.as_ref())
        .filter_map(|m| m.final_membership.as_ref())
        .map(|e| e.membership.clone())
        .next_back()
        .unwrap_or_else(|| {
            state.cluster_bootstrap.as_ref().unwrap().memberships[&RaftGroupId(0)].clone()
        });
    MigrationRequest {
        operation_key: key.to_owned(),
        raft_group_id: RaftGroupId(0),
        expected_epoch: placement.epoch,
        source_membership: source,
        target_voters: target.iter().copied().collect(),
        target_policy: rf.map(|rf| GroupPlacementPolicy {
            replication_factor: rf,
            ..Default::default()
        }),
    }
}

fn submit(state: &mut ControlPlaneState, request: MigrationRequest) -> u64 {
    let ControlResponse::MigrationStarted { migration_id } =
        state.apply(ControlCommand::SubmitMigration { request, now_ms: 2 })
    else {
        panic!("submit failed");
    };
    migration_id
}

fn claim(state: &mut ControlPlaneState, id: u64, from: u64, key: u128) -> MigrationToken {
    let ControlResponse::ExecutorClaimed { token } =
        state.apply(ControlCommand::ClaimMigrationExecutor {
            migration_id: id,
            expected_generation: from,
            claim_key: ProcessIncarnation::from_bits(key),
            executor: process(1),
            now_ms: 3,
        })
    else {
        panic!("claim failed");
    };
    token
}

fn update_command(
    state: &ControlPlaneState,
    token: &MigrationToken,
    update: MigrationUpdate,
) -> ControlCommand {
    ControlCommand::UpdateMigration {
        token: token.clone(),
        expected_revision: state.migrations[&token.migration_id]
            .managed
            .as_ref()
            .unwrap()
            .revision,
        update,
        now_ms: 4,
    }
}

fn update(state: &mut ControlPlaneState, token: &MigrationToken, action: MigrationUpdate) {
    let command = update_command(state, token, action);
    assert_eq!(state.apply(command), ControlResponse::Ok);
    state.validate_migration_state().unwrap();
}

fn reject(state: &mut ControlPlaneState, command: ControlCommand) {
    let before = state.clone();
    assert!(state.apply(command).is_rejected());
    assert_eq!(*state, before, "rejection must be atomic");
}

fn peers(state: &ControlPlaneState, token: &MigrationToken) -> BTreeMap<u64, ProcessIncarnation> {
    let migration = &state.migrations[&token.migration_id];
    migration
        .from_voters
        .union(&migration.target_voters)
        .map(|id| (*id, process(*id).incarnation))
        .collect()
}

fn barrier(state: &mut ControlPlaneState, token: &MigrationToken) {
    update(state, token, MigrationUpdate::AuthorizeReceivers);
    let processes = peers(state, token);
    update(state, token, MigrationUpdate::CertifyReceivers {
        processes,
    });
}

fn ready_learners(state: &mut ControlPlaneState, token: &MigrationToken) {
    let migration = state.migrations[&token.migration_id].clone();
    for id in &migration.added_nodes {
        update(state, token, MigrationUpdate::RecordPrepared {
            process: process(*id),
        });
    }
    let prefix = migration
        .managed
        .as_ref()
        .unwrap()
        .catchup_prefix
        .clone()
        .unwrap_or_else(|| {
            log(migration
                .managed
                .as_ref()
                .unwrap()
                .request
                .source_membership
                .log_id
                .index
                + 3)
        });
    update(state, token, MigrationUpdate::CapturePrefix {
        prefix: prefix.clone(),
    });
    for id in &migration.added_nodes {
        update(state, token, MigrationUpdate::RecordLearner {
            evidence: ReplicaAppliedEvidence {
                process: process(*id),
                applied_log_id: prefix.clone(),
            },
        });
    }
}

fn final_evidence(state: &ControlPlaneState, token: &MigrationToken) -> FinalMembershipEvidence {
    let migration = &state.migrations[&token.migration_id];
    let index = migration
        .managed
        .as_ref()
        .unwrap()
        .request
        .source_membership
        .log_id
        .index;
    FinalMembershipEvidence {
        membership: VerifiedGroupMembership {
            voters: migration.target_voters.clone(),
            learners: BTreeSet::new(),
            log_id: log(index + 10),
        },
        committed_prefix: log(index + 12),
        replicas: migration
            .target_voters
            .iter()
            .map(|id| {
                (*id, ReplicaAppliedEvidence {
                    process: process(*id),
                    applied_log_id: log(index + 13),
                })
            })
            .collect(),
    }
}

fn verified(state: &mut ControlPlaneState, token: &MigrationToken) {
    barrier(state, token);
    ready_learners(state, token);
    update(state, token, MigrationUpdate::AuthorizeMembership);
    let evidence = final_evidence(state, token);
    update(state, token, MigrationUpdate::VerifyMembership { evidence });
}

fn cleanup(state: &mut ControlPlaneState, token: &MigrationToken) {
    let migration = state.migrations[&token.migration_id].clone();
    let managed = migration.managed.as_ref().unwrap();
    for id in &migration.removed_voters {
        update(state, token, MigrationUpdate::RecordReleased {
            evidence: ReplicaRetirementEvidence {
                process: process(*id),
                placement_epoch: managed.published_epoch.unwrap(),
                membership_log_id: managed
                    .final_membership
                    .as_ref()
                    .unwrap()
                    .membership
                    .log_id
                    .clone(),
                work_drained: true,
                snapshot_references_retired: true,
                local_records_reclaimed: true,
            },
        });
    }
    let processes = peers(state, token);
    update(state, token, MigrationUpdate::RetireReceivers { processes });
    update(state, token, MigrationUpdate::Finish);
}

#[test]
fn managed_replacements_and_explicit_rf_cycle_require_full_evidence_and_cleanup() {
    for (rf, target) in [
        (ReplicationFactor::Three, vec![2, 3, 4]),
        (ReplicationFactor::Five, vec![2, 3, 4, 5, 6]),
    ] {
        let mut state = fixture(rf);
        let request = new_request(&state, "replace", &target, None);
        let id = submit(&mut state, request.clone());
        let token = claim(&mut state, id, 0, 100);
        verified(&mut state, &token);
        update(&mut state, &token, MigrationUpdate::PublishPlacement);
        assert_eq!(state.placements[&RaftGroupId(0)].epoch, 1);
        assert_eq!(state.placements[&RaftGroupId(0)].draining, [1].into());
        cleanup(&mut state, &token);
        assert!(state.active_migration.is_none());
        assert!(state.placements[&RaftGroupId(0)].draining.is_empty());
        assert_eq!(submit(&mut state, request), id);
        assert_eq!(
            state.managed_placement.as_ref().unwrap().groups[&RaftGroupId(0)].replication_factor,
            rf
        );
    }
    let mut state = fixture(ReplicationFactor::Three);
    for (key, target, rf) in [
        ("expand", vec![1, 2, 3, 4, 5], ReplicationFactor::Five),
        ("contract", vec![1, 2, 3], ReplicationFactor::Three),
    ] {
        let request = new_request(&state, key, &target, Some(rf));
        let id = submit(&mut state, request);
        let token = claim(&mut state, id, 0, 100 + u128::from(id));
        assert_eq!(
            token.generation, id,
            "generations persist across operations"
        );
        verified(&mut state, &token);
        update(&mut state, &token, MigrationUpdate::PublishPlacement);
        cleanup(&mut state, &token);
        assert_eq!(
            state.placements[&RaftGroupId(0)].voters.len(),
            rf.voter_count()
        );
        assert_eq!(
            state.managed_placement.as_ref().unwrap().groups[&RaftGroupId(0)].replication_factor,
            rf
        );
    }
    assert_eq!(state.placements[&RaftGroupId(0)].epoch, 2);
}

#[test]
fn intent_keys_epoch_cas_source_certificates_and_legacy_bypasses_are_checked_atomically() {
    let mut state = fixture(ReplicationFactor::Three);
    let valid = new_request(&state, "replace", &[2, 3, 4], None);
    let mut bad = valid.clone();
    bad.expected_epoch = 9;
    reject(&mut state, ControlCommand::SubmitMigration {
        request: bad,
        now_ms: 2,
    });
    let mut bad = valid.clone();
    bad.source_membership.log_id.node_id = 2;
    reject(&mut state, ControlCommand::SubmitMigration {
        request: bad,
        now_ms: 2,
    });
    let mut bad = valid.clone();
    bad.target_voters = [1, 2, 3, 4, 5].into();
    reject(&mut state, ControlCommand::SubmitMigration {
        request: bad,
        now_ms: 2,
    });
    let id = submit(&mut state, valid.clone());
    assert_eq!(submit(&mut state, valid.clone()), id);
    let mut bad = valid;
    bad.target_voters = [1, 2, 6].into();
    reject(&mut state, ControlCommand::SubmitMigration {
        request: bad,
        now_ms: 2,
    });
    for command in [
        ControlCommand::AdvanceMigration {
            migration_id: id,
            phase: MigrationPhase::CommittingPlacement,
            now_ms: 3,
        },
        ControlCommand::CommitPlacement {
            raft_group_id: RaftGroupId(0),
            voters: [2, 3, 4].into(),
            learners: BTreeSet::new(),
            draining: BTreeSet::new(),
            now_ms: 3,
        },
        ControlCommand::FinishMigration {
            migration_id: id,
            success: false,
            now_ms: 3,
        },
        ControlCommand::EvictLearner {
            raft_group_id: RaftGroupId(0),
            node_id: 1,
            now_ms: 3,
        },
    ] {
        reject(&mut state, command);
    }
}

#[test]
fn generation_and_revision_reject_delayed_executors_and_replay_lost_replies() {
    let mut state = fixture(ReplicationFactor::Three);
    let request = new_request(&state, "replace", &[2, 3, 4], None);
    let id = submit(&mut state, request);
    let token = claim(&mut state, id, 0, 100);
    assert_eq!(claim(&mut state, id, 0, 100), token);
    let action = update_command(&state, &token, MigrationUpdate::AuthorizeReceivers);
    assert_eq!(state.apply(action.clone()), ControlResponse::Ok);
    let after = state.clone();
    assert_eq!(state.apply(action.clone()), ControlResponse::Ok);
    assert_eq!(state, after);
    let processes = peers(&state, &token);
    update(&mut state, &token, MigrationUpdate::CertifyReceivers {
        processes,
    });
    reject(&mut state, action);
    let new = claim(&mut state, id, token.generation, 200);
    assert!(new.generation > token.generation);
    let old = update_command(&state, &token, MigrationUpdate::RecordError {
        reason: "delayed".to_owned(),
    });
    reject(&mut state, old);
    assert!(
        state.migrations[&id]
            .managed
            .as_ref()
            .unwrap()
            .receivers
            .is_empty()
    );
    let before = state.clone();
    let command = update_command(&state, &new, MigrationUpdate::AuthorizeMembership);
    reject(&mut state, command);
    assert_eq!(state, before);
    verified(&mut state, &new);
}

#[test]
fn receiver_and_fixed_prefix_evidence_cannot_skip_preparation_or_use_an_old_process() {
    let mut state = fixture(ReplicationFactor::Three);
    let request = new_request(&state, "replace", &[2, 3, 4], None);
    let id = submit(&mut state, request);
    let token = claim(&mut state, id, 0, 100);
    for action in [
        MigrationUpdate::Finish,
        MigrationUpdate::PublishPlacement,
        MigrationUpdate::AuthorizeMembership,
        MigrationUpdate::CapturePrefix { prefix: log(4) },
    ] {
        let command = update_command(&state, &token, action);
        reject(&mut state, command);
    }
    update(&mut state, &token, MigrationUpdate::AuthorizeReceivers);
    let mut missing = peers(&state, &token);
    missing.remove(&1);
    let command = update_command(&state, &token, MigrationUpdate::CertifyReceivers {
        processes: missing,
    });
    reject(&mut state, command);
    let processes = peers(&state, &token);
    update(&mut state, &token, MigrationUpdate::CertifyReceivers {
        processes,
    });
    let command = update_command(&state, &token, MigrationUpdate::RecordPrepared {
        process: ReceiverProcess {
            node_id: 4,
            incarnation: ProcessIncarnation::from_bits(999),
        },
    });
    reject(&mut state, command);
    update(&mut state, &token, MigrationUpdate::RecordPrepared {
        process: process(4),
    });
    update(&mut state, &token, MigrationUpdate::CapturePrefix {
        prefix: log(4),
    });
    for applied in [
        log(3),
        MembershipLogId {
            term: 0,
            node_id: 1,
            index: 5,
        },
        MembershipLogId {
            term: 1,
            node_id: 2,
            index: 4,
        },
    ] {
        let command = update_command(&state, &token, MigrationUpdate::RecordLearner {
            evidence: ReplicaAppliedEvidence {
                process: process(4),
                applied_log_id: applied,
            },
        });
        reject(&mut state, command);
    }
    let command = update_command(&state, &token, MigrationUpdate::CapturePrefix {
        prefix: log(5),
    });
    reject(&mut state, command);
    update(&mut state, &token, MigrationUpdate::RecordLearner {
        evidence: ReplicaAppliedEvidence {
            process: process(4),
            applied_log_id: log(4),
        },
    });
    update(&mut state, &token, MigrationUpdate::AuthorizeMembership);
}

#[test]
fn final_proof_epoch_and_cleanup_must_match_the_current_intent_and_generation() {
    let mut state = fixture(ReplicationFactor::Three);
    let request = new_request(&state, "replace", &[2, 3, 4], None);
    let id = submit(&mut state, request);
    let token = claim(&mut state, id, 0, 100);
    verified(&mut state, &token);
    let valid = final_evidence(&state, &token);
    let mut variants = Vec::new();
    let mut bad = valid.clone();
    bad.membership.learners.insert(1);
    variants.push(bad);
    let mut bad = valid.clone();
    bad.membership.log_id = log(1);
    variants.push(bad);
    let mut bad = valid.clone();
    bad.replicas.remove(&4);
    variants.push(bad);
    let mut bad = valid.clone();
    bad.replicas.get_mut(&4).unwrap().applied_log_id = log(11);
    variants.push(bad);
    let mut bad = valid;
    bad.replicas.get_mut(&4).unwrap().process.incarnation = ProcessIncarnation::from_bits(999);
    variants.push(bad);
    for evidence in variants {
        let command = update_command(&state, &token, MigrationUpdate::VerifyMembership {
            evidence,
        });
        reject(&mut state, command);
    }
    state.placements.get_mut(&RaftGroupId(0)).unwrap().epoch += 1;
    let command = update_command(&state, &token, MigrationUpdate::PublishPlacement);
    reject(&mut state, command);
    state.placements.get_mut(&RaftGroupId(0)).unwrap().epoch -= 1;
    let publish = update_command(&state, &token, MigrationUpdate::PublishPlacement);
    assert_eq!(state.apply(publish.clone()), ControlResponse::Ok);
    assert_eq!(state.apply(publish), ControlResponse::Ok);
    let command = update_command(&state, &token, MigrationUpdate::Finish);
    reject(&mut state, command);
    let new = claim(&mut state, id, token.generation, 200);
    barrier(&mut state, &new);
    let mut evidence = ReplicaRetirementEvidence {
        process: process(1),
        placement_epoch: 1,
        membership_log_id: log(11),
        work_drained: true,
        snapshot_references_retired: true,
        local_records_reclaimed: true,
    };
    let command = update_command(&state, &new, MigrationUpdate::RecordReleased {
        evidence: evidence.clone(),
    });
    reject(&mut state, command);
    let proof = final_evidence(&state, &new);
    update(&mut state, &new, MigrationUpdate::VerifyMembership {
        evidence: proof,
    });
    evidence.local_records_reclaimed = false;
    let command = update_command(&state, &new, MigrationUpdate::RecordReleased { evidence });
    reject(&mut state, command);
    cleanup(&mut state, &new);
}

#[test]
fn cancellation_is_allowed_only_before_authorizing_possible_receiver_side_effects() {
    let mut state = fixture(ReplicationFactor::Three);
    let request = new_request(&state, "cancel", &[2, 3, 4], None);
    let id = submit(&mut state, request);
    let token = claim(&mut state, id, 0, 100);
    update(&mut state, &token, MigrationUpdate::Cancel);
    assert!(state.active_migration.is_none());
    let request = new_request(&state, "replace", &[2, 3, 4], None);
    let id = submit(&mut state, request);
    let token = claim(&mut state, id, 0, 200);
    update(&mut state, &token, MigrationUpdate::AuthorizeReceivers);
    let command = update_command(&state, &token, MigrationUpdate::Cancel);
    reject(&mut state, command);
    update(&mut state, &token, MigrationUpdate::RecordError {
        reason: "lost activation reply".to_owned(),
    });
    assert_eq!(state.active_migration, Some(id));
}

#[test]
fn snapshots_preserve_recovery_authority_and_exhausted_counters_never_reuse_ids() {
    let mut state = fixture(ReplicationFactor::Three);
    let request = new_request(&state, "replace", &[2, 3, 4], None);
    let mut exhausted = state.clone();
    exhausted.next_migration_id = u64::MAX;
    reject(&mut exhausted, ControlCommand::SubmitMigration {
        request: request.clone(),
        now_ms: 2,
    });
    let id = submit(&mut state, request);
    let token = claim(&mut state, id, 0, 100);
    verified(&mut state, &token);
    assert_eq!(
        serde_json::from_slice::<ControlPlaneState>(&serde_json::to_vec(&state).unwrap()).unwrap(),
        state
    );
    assert_eq!(
        rmp_serde::from_slice::<ControlPlaneState>(&rmp_serde::to_vec_named(&state).unwrap())
            .unwrap(),
        state
    );
    let mut exhausted = state.clone();
    exhausted.next_executor_generation = u64::MAX;
    reject(&mut exhausted, ControlCommand::ClaimMigrationExecutor {
        migration_id: id,
        expected_generation: token.generation,
        claim_key: ProcessIncarnation::from_bits(200),
        executor: process(1),
        now_ms: 5,
    });
    let mut legacy = serde_json::to_value(ControlPlaneState::default()).unwrap();
    legacy
        .as_object_mut()
        .unwrap()
        .remove("next_executor_generation");
    assert_eq!(
        serde_json::from_value::<ControlPlaneState>(legacy)
            .unwrap()
            .next_executor_generation,
        1
    );
}

#[test]
fn recovery_rejects_conflicting_authority_and_incomplete_terminal_records() {
    let mut state = fixture(ReplicationFactor::Three);
    let request = new_request(&state, "replace", &[2, 3, 4], None);
    let id = submit(&mut state, request);
    let token = claim(&mut state, id, 0, 100);
    verified(&mut state, &token);
    state.validate_migration_state().unwrap();
    let mut variants = Vec::new();
    let mut bad = state.clone();
    bad.next_executor_generation = token.generation;
    variants.push(bad);
    let mut bad = state.clone();
    bad.active_migration = None;
    variants.push(bad);
    let mut bad = state.clone();
    bad.migrations.get_mut(&id).unwrap().managed = None;
    variants.push(bad);
    let mut bad = state.clone();
    bad.migrations.get_mut(&id).unwrap().phase = MigrationPhase::Succeeded;
    variants.push(bad);
    let mut bad = state.clone();
    bad.migrations
        .get_mut(&id)
        .unwrap()
        .managed
        .as_mut()
        .unwrap()
        .receivers
        .remove(&1);
    variants.push(bad);
    let mut bad = state;
    bad.migrations
        .get_mut(&id)
        .unwrap()
        .managed
        .as_mut()
        .unwrap()
        .published_epoch = Some(10);
    variants.push(bad);
    for bad in variants {
        assert!(bad.validate_migration_state().is_err());
    }
}

use serde_json::json;
use ursula_proto::admin::ProcessIncarnation;

use super::cell;
use super::document;
use super::host_publication;
use super::observation;
use super::request;
use crate::reservation::ConfigMapSnapshot;
use crate::reservation::HostRequest;
use crate::reservation::HostTerminationObservation;
use crate::reservation::OwnershipRequest;
use crate::reservation::ProgressRequest;
use crate::reservation::Reservation;
use crate::reservation::ReservationRequest;
use crate::reservation::SurvivingPrefixObservation;

fn reserved() -> Reservation {
    let saved = Reservation::initial(cell())
        .unwrap()
        .publish_hosts(host_publication())
        .unwrap();
    saved
        .recover_host(HostRequest::ReserveHostRecovery {
            operation_id: format!("{:032x}", 1),
            executor_id: format!("{:032x}", 10),
            node_id: 1,
            process_plan: saved.hosts().unwrap().process_plan.clone(),
            now_ms: 1600,
        })
        .unwrap()
}

fn survivors(state: &Reservation, started_ms: u64, index: u64) -> SurvivingPrefixObservation {
    let mut inner = observation(state, false, started_ms, index);
    inner.verification.applied.remove(&1);
    inner.verification.process_incarnations.remove(&1);
    SurvivingPrefixObservation {
        started_ms,
        completed_ms: inner.completed_ms,
        verification: crate::quorum::SurvivingQuorumVerification {
            excluded_voter_id: 1,
            configured_voter_ids: [1, 2, 3].into_iter().collect(),
            surviving_voter_ids: [2, 3].into_iter().collect(),
            full_redundancy_restored: false,
            verification: inner.verification,
        },
    }
}

fn admit(state: &Reservation) -> Reservation {
    state
        .recover_host(HostRequest::AdmitHostTermination {
            fence: state.operation().unwrap().fence.clone(),
            now_ms: 1900,
            observation: survivors(state, 1700, 60),
        })
        .unwrap()
}

fn terminated() -> Reservation {
    let state = admit(&reserved());
    state
        .recover_host(HostRequest::RecordHostTermination {
            fence: state.operation().unwrap().fence.clone(),
            now_ms: 2200,
            observation: HostTerminationObservation {
                started_ms: 2000,
                completed_ms: 2100,
                provider_instance: "instance-1".into(),
                terminal_state: "terminated".into(),
            },
        })
        .unwrap()
}

fn original_intent(state: &Reservation) -> Reservation {
    state
        .recover_host(HostRequest::AdmitFencedPodRetirement {
            fence: state.operation().unwrap().fence.clone(),
            pod: None,
            node: None,
        })
        .unwrap()
}

fn binding(state: &Reservation, uid: &str) -> HostRequest {
    let mut plan = state.operation().unwrap().process_plan.clone();
    plan[0].expected_process_incarnation = Some(ProcessIncarnation::from_bits(100));
    HostRequest::BindHostReplacement {
        fence: state.operation().unwrap().fence.clone(),
        process_plan: plan,
        pod: json!({"kind":"Pod", "metadata":{"namespace":"ursula","name":"voters-0","uid":uid,"ownerReferences":[{"kind":"StatefulSet","uid":"statefulset-uid","controller":true}]},"spec":{"nodeName":"host-1"}}),
        node: json!({"kind":"Node","metadata":{"name":"host-1","uid":"new-node-1","labels":{"topology.kubernetes.io/zone":"zone-1"}},"spec":{"providerID":"new-instance-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}),
    }
}

fn retirement(state: &Reservation, uid: &str) -> HostRequest {
    let fixture = host_publication();
    let mut pod = fixture.pods[0].clone();
    pod["metadata"]["uid"] = json!(uid);
    pod["metadata"]["deletionTimestamp"] = json!("already-deleting");
    HostRequest::AdmitFencedPodRetirement {
        fence: state.operation().unwrap().fence.clone(),
        pod: Some(pod),
        node: Some(fixture.nodes[0].clone()),
    }
}

fn complete(state: &Reservation) -> HostRequest {
    HostRequest::CompleteHostReplacement {
        fence: state.operation().unwrap().fence.clone(),
        now_ms: 3000,
        observation: observation(state, true, 2800, 70),
    }
}

#[test]
fn host_ownership_requires_prefault_catalog_and_uses_the_same_global_exclusion() {
    let saved = reserved();
    assert!(
        Reservation::initial(cell())
            .unwrap()
            .recover_host(HostRequest::ReserveHostRecovery {
                operation_id: format!("{:032x}", 1),
                executor_id: format!("{:032x}", 10),
                node_id: 1,
                process_plan: saved.operation().unwrap().process_plan.clone(),
                now_ms: 1600
            })
            .is_err()
    );
    assert!(saved.propose(request(2, 20, 2)).is_err());
    assert!(
        saved
            .recover_host(HostRequest::ReserveHostRecovery {
                operation_id: format!("{:032x}", 2),
                executor_id: format!("{:032x}", 20),
                node_id: 2,
                process_plan: saved.operation().unwrap().process_plan.clone(),
                now_ms: 2000
            })
            .is_err()
    );
    assert!(
        saved
            .progress(ProgressRequest::AdmitPodDeletion {
                fence: saved.operation().unwrap().fence.clone(),
                now_ms: 1900,
                observation: observation(&saved, false, 1700, 60)
            })
            .is_err()
    );
    assert_eq!(
        saved
            .operation()
            .unwrap()
            .host
            .as_ref()
            .unwrap()
            .source_host
            .source
            .provider_instance,
        "instance-1"
    );
    assert_eq!(
        saved.operation().unwrap().host.as_ref().unwrap().stage(),
        "host-reserved"
    );
}

#[test]
fn minority_admission_requires_fresh_certified_survivors_and_the_acknowledged_floor() {
    let state = reserved();
    for case in 0..12 {
        let mut proof = survivors(&state, 1700, 60);
        match case {
            0 => proof.verification.excluded_voter_id = 2,
            1 => {
                proof.verification.configured_voter_ids.remove(&1);
            }
            2 => proof.verification.full_redundancy_restored = true,
            3 => {
                proof.verification.verification.applied.remove(&3);
            }
            4 => proof.verification.verification.participation_certified = false,
            5 => {
                proof
                    .verification
                    .verification
                    .maintenance_executor_certified = false
            }
            6 => {
                proof
                    .verification
                    .verification
                    .process_incarnations
                    .insert(2, ProcessIncarnation::from_bits(999));
            }
            7 => {
                proof
                    .verification
                    .verification
                    .prefixes
                    .get_mut(&1)
                    .unwrap()
                    .leader_id = 1;
            }
            8 => {
                proof.verification.verification.prefixes.remove(&1);
            }
            9 => proof.started_ms = 1200,
            10 => proof.completed_ms = 2500,
            _ => {
                for prefix in proof.verification.verification.prefixes.values_mut() {
                    prefix.required_applied_index = 49;
                }
            }
        }
        assert!(
            state
                .recover_host(HostRequest::AdmitHostTermination {
                    fence: state.operation().unwrap().fence.clone(),
                    now_ms: 1900,
                    observation: proof
                })
                .is_err(),
            "case {case}"
        );
    }
    let admitted = admit(&state);
    assert_eq!(
        admitted.operation().unwrap().host.as_ref().unwrap().stage(),
        "host-termination-admitted"
    );
}

#[test]
fn stopped_missing_unknown_or_wrong_provider_is_never_a_host_fence() {
    let state = admit(&reserved());
    for terminal_state in [
        "stopped",
        "shutting-down",
        "not-found",
        "unknown",
        "running",
        "",
    ] {
        assert!(
            state
                .recover_host(HostRequest::RecordHostTermination {
                    fence: state.operation().unwrap().fence.clone(),
                    now_ms: 2200,
                    observation: HostTerminationObservation {
                        started_ms: 2000,
                        completed_ms: 2100,
                        provider_instance: "instance-1".into(),
                        terminal_state: terminal_state.into()
                    }
                })
                .is_err()
        );
    }
    for (instance, start, end, now) in [
        ("new-instance-1", 2000, 2100, 2200),
        ("instance-1", 1600, 1700, 2200),
        ("instance-1", 2000, 2300, 2200),
        ("instance-1", 2000, 2100, 100_000),
    ] {
        assert!(
            state
                .recover_host(HostRequest::RecordHostTermination {
                    fence: state.operation().unwrap().fence.clone(),
                    now_ms: now,
                    observation: HostTerminationObservation {
                        started_ms: start,
                        completed_ms: end,
                        provider_instance: instance.into(),
                        terminal_state: "terminated".into()
                    }
                })
                .is_err()
        );
    }
    assert!(original_intent_result(&state).is_err());
    assert!(state.recover_host(binding(&state, "replacement")).is_err());
    let terminal = terminated();
    assert_eq!(
        terminal.operation().unwrap().host.as_ref().unwrap().stage(),
        "host-terminated"
    );
    assert!(
        terminal
            .recover_host(HostRequest::RecordHostTermination {
                fence: terminal.operation().unwrap().fence.clone(),
                now_ms: 2200,
                observation: terminal
                    .operation()
                    .unwrap()
                    .host
                    .as_ref()
                    .unwrap()
                    .termination
                    .clone()
                    .unwrap()
            })
            .is_err()
    );
}

fn original_intent_result(state: &Reservation) -> anyhow::Result<Reservation> {
    state.recover_host(HostRequest::AdmitFencedPodRetirement {
        fence: state.operation().unwrap().fence.clone(),
        pod: None,
        node: None,
    })
}

#[test]
fn force_intent_and_binding_race_cannot_leave_a_bound_pod_subject_to_stale_deletion() {
    let state = original_intent(&terminated());
    let mut raw = document();
    raw["data"]["reservation"] = json!(serde_json::to_string(&state).unwrap());
    let store = ConfigMapSnapshot::parse(raw, &cell()).unwrap();
    let retirement_proposal = store
        .transition(ReservationRequest::Host(retirement(
            &state,
            "candidate-uid",
        )))
        .unwrap();
    let binding_proposal = store
        .transition(ReservationRequest::Host(binding(&state, "candidate-uid")))
        .unwrap();
    for (winner, other) in [
        (&retirement_proposal, &binding_proposal),
        (&binding_proposal, &retirement_proposal),
    ] {
        let mut response = winner.document().clone();
        response["metadata"]["resourceVersion"] = json!("rv-two");
        let accepted = store.acknowledge(winner, response.clone()).unwrap();
        assert!(store.acknowledge(other, response).is_err());
        let state = accepted.state();
        if state.operation().unwrap().replacement.is_some() {
            assert!(
                state
                    .recover_host(retirement(state, "candidate-uid"))
                    .is_err()
            );
        } else {
            assert!(state.recover_host(binding(state, "candidate-uid")).is_err());
        }
    }
}

#[test]
fn extra_retirement_requires_the_exact_old_node_and_never_uses_a_recreated_name() {
    let state = original_intent(&terminated());
    for case in 0..4 {
        let mut action = retirement(&state, "stale-pod");
        if let HostRequest::AdmitFencedPodRetirement { pod, node, .. } = &mut action {
            match case {
                0 => *node = None,
                1 => node.as_mut().unwrap()["metadata"]["uid"] = json!("new-node-same-name"),
                2 => node.as_mut().unwrap()["spec"]["providerID"] = json!("new-instance"),
                _ => pod.as_mut().unwrap()["spec"]["nodeName"] = json!("other-host"),
            }
        }
        assert!(state.recover_host(action).is_err(), "case {case}");
    }
    let with_stale = state.recover_host(retirement(&state, "stale-pod")).unwrap();
    assert!(
        with_stale
            .operation()
            .unwrap()
            .host
            .as_ref()
            .unwrap()
            .pod_retirement_intents
            .contains("stale-pod")
    );
    assert!(
        with_stale
            .recover_host(binding(&with_stale, "stale-pod"))
            .is_err()
    );
    // The original UID is also unsafe to force-delete if a recreated same-name
    // Node now owns the Pod. Recording its tombstone without API objects is
    // separate from authenticating a currently observed Pod's physical owner.
    let mut original = retirement(&state, "pod-1");
    if let HostRequest::AdmitFencedPodRetirement { node, .. } = &mut original {
        node.as_mut().unwrap()["metadata"]["uid"] = json!("recreated-node");
    }
    assert!(state.recover_host(original).is_err());
}

#[test]
fn retirement_history_is_bounded_and_cannot_be_pruned_by_takeover() {
    let mut state = original_intent(&terminated());
    for index in 0..31 {
        state = state
            .recover_host(retirement(&state, &format!("stale-{index}")))
            .unwrap();
    }
    assert!(
        state
            .recover_host(retirement(&state, "over-bound"))
            .is_err()
    );
    let taken = state
        .propose(OwnershipRequest::Takeover {
            operation_id: state.operation().unwrap().fence.reservation_id().into(),
            executor_id: format!("{:032x}", 20),
            now_ms: 2300,
        })
        .unwrap();
    assert_eq!(
        taken
            .operation()
            .unwrap()
            .host
            .as_ref()
            .unwrap()
            .pod_retirement_intents
            .len(),
        32
    );
    assert!(
        taken
            .recover_host(retirement(&taken, "over-bound"))
            .is_err()
    );
}

#[test]
fn initially_refreshed_survivor_boots_require_new_proof_and_remain_fixed() {
    let catalogued = Reservation::initial(cell())
        .unwrap()
        .publish_hosts(host_publication())
        .unwrap();
    let mut plan = catalogued.hosts().unwrap().process_plan.clone();
    plan[1].expected_process_incarnation = Some(ProcessIncarnation::from_bits(200));
    let state = catalogued
        .recover_host(HostRequest::ReserveHostRecovery {
            operation_id: format!("{:032x}", 1),
            executor_id: format!("{:032x}", 10),
            node_id: 1,
            process_plan: plan,
            now_ms: 1600,
        })
        .unwrap();
    let mut stale = survivors(&state, 1700, 60);
    stale
        .verification
        .verification
        .process_incarnations
        .insert(2, ProcessIncarnation::from_bits(2));
    assert!(
        state
            .recover_host(HostRequest::AdmitHostTermination {
                fence: state.operation().unwrap().fence.clone(),
                now_ms: 1900,
                observation: stale,
            })
            .is_err()
    );
    let admitted = admit(&state);
    let taken = admitted
        .propose(OwnershipRequest::Takeover {
            operation_id: admitted.operation().unwrap().fence.reservation_id().into(),
            executor_id: format!("{:032x}", 20),
            now_ms: 2000,
        })
        .unwrap();
    assert_eq!(
        taken.operation().unwrap().process_plan[1].expected_process_incarnation,
        Some(ProcessIncarnation::from_bits(200))
    );
}

#[test]
fn replacement_cannot_share_old_or_surviving_hosts_or_refresh_a_survivor() {
    let state = original_intent(&terminated());
    for case in 0..6 {
        let mut action = binding(&state, "replacement");
        if let HostRequest::BindHostReplacement {
            node, process_plan, ..
        } = &mut action
        {
            match case {
                0 => node["metadata"]["uid"] = json!("node-1"),
                1 => node["spec"]["providerID"] = json!("instance-1"),
                2 => node["metadata"]["labels"]["topology.kubernetes.io/zone"] = json!("zone-2"),
                3 => node["metadata"]["uid"] = json!("node-2"),
                4 => {
                    process_plan[1].expected_process_incarnation =
                        Some(ProcessIncarnation::from_bits(999))
                }
                _ => node["status"]["conditions"][0]["status"] = json!("False"),
            }
        }
        assert!(state.recover_host(action).is_err(), "case {case}");
    }
    let bound = state.recover_host(binding(&state, "replacement")).unwrap();
    assert!(bound.recover_host(binding(&bound, "another-pod")).is_err());
    assert!(original_intent_result(&bound).is_err());
}

#[test]
fn takeover_retains_terminal_provider_intent_and_every_stale_delete_uid() {
    let state = original_intent(&terminated());
    let state = state.recover_host(retirement(&state, "stale-pod")).unwrap();
    let old = state.operation().unwrap();
    let resumed = state
        .propose(OwnershipRequest::Takeover {
            operation_id: old.fence.reservation_id().into(),
            executor_id: format!("{:032x}", 20),
            now_ms: 2300,
        })
        .unwrap();
    assert_eq!(
        serde_json::to_value(&old.host).unwrap(),
        serde_json::to_value(&resumed.operation().unwrap().host).unwrap()
    );
    assert_eq!(resumed.generation(), 2);
    assert!(
        resumed
            .recover_host(binding(&state, "replacement"))
            .is_err()
    );
    assert!(resumed.propose(request(2, 30, 2)).is_err());
    let bound = resumed
        .recover_host(binding(&resumed, "replacement"))
        .unwrap();
    let done = bound.recover_host(complete(&bound)).unwrap();
    assert!(done.operation().is_none());
    assert!(
        done.completion()
            .unwrap()
            .host
            .as_ref()
            .unwrap()
            .pod_retirement_intents
            .contains("stale-pod")
    );
    assert_eq!(
        done.hosts()
            .unwrap()
            .voter(1)
            .unwrap()
            .source
            .provider_instance,
        "new-instance-1"
    );
    let mut next = request(2, 30, 2);
    if let OwnershipRequest::Reserve {
        process_plan,
        now_ms,
        ..
    } = &mut next
    {
        *process_plan = done.hosts().unwrap().process_plan.clone();
        *now_ms = 3100;
    }
    done.propose(next).unwrap();
}

#[test]
fn host_completion_requires_all_three_current_retired_processes_and_nonregressing_prefixes() {
    let state = original_intent(&terminated());
    let bound = state.recover_host(binding(&state, "replacement")).unwrap();
    for case in 0..6 {
        let mut action = complete(&bound);
        if let HostRequest::CompleteHostReplacement { observation, .. } = &mut action {
            match case {
                0 => {
                    observation.verification.applied.remove(&1);
                }
                1 => {
                    observation
                        .verification
                        .maintenance_executor_retired_certified = false;
                    observation.verification.maintenance_executor_certified = true;
                }
                2 => {
                    observation
                        .verification
                        .process_incarnations
                        .insert(3, ProcessIncarnation::from_bits(999));
                }
                3 => {
                    for prefix in observation.verification.prefixes.values_mut() {
                        prefix.required_applied_index = 59;
                    }
                }
                4 => observation.started_ms = 1000,
                _ => observation.started_ms = 2050,
            }
        }
        assert!(bound.recover_host(action).is_err(), "case {case}");
    }
    let completed = bound.recover_host(complete(&bound)).unwrap();
    completed.validate().unwrap();
    let encoded = serde_json::to_vec(&completed).unwrap();
    serde_json::from_slice::<Reservation>(&encoded)
        .unwrap()
        .validate()
        .unwrap();
}

#[test]
fn every_host_request_round_trips_through_json_integer_map_keys_and_revalidated_state() {
    let mut state = reserved();
    let actions = [
        HostRequest::AdmitHostTermination {
            fence: state.operation().unwrap().fence.clone(),
            now_ms: 1900,
            observation: survivors(&state, 1700, 60),
        },
        HostRequest::RecordHostTermination {
            fence: state.operation().unwrap().fence.clone(),
            now_ms: 2200,
            observation: HostTerminationObservation {
                started_ms: 2000,
                completed_ms: 2100,
                provider_instance: "instance-1".into(),
                terminal_state: "terminated".into(),
            },
        },
        HostRequest::AdmitFencedPodRetirement {
            fence: state.operation().unwrap().fence.clone(),
            pod: None,
            node: None,
        },
        binding(&state, "replacement"),
    ];
    for action in actions {
        let encoded = serde_json::to_vec(&ReservationRequest::Host(action)).unwrap();
        let parsed: ReservationRequest = serde_json::from_slice(&encoded).unwrap();
        let ReservationRequest::Host(action) = parsed else {
            unreachable!()
        };
        state = state.recover_host(action).unwrap();
        state = serde_json::from_slice(&serde_json::to_vec(&state).unwrap()).unwrap();
        state.validate().unwrap();
    }
    state.recover_host(complete(&state)).unwrap();
}

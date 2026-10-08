use serde_json::json;
use ursula_proto::admin::ProcessIncarnation;

use super::binding;
use super::complete;
use super::original_intent;
use super::survivors;
use super::terminated;
use crate::reservation::ConfigMapSnapshot;
use crate::reservation::HostRequest;
use crate::reservation::HostTerminationObservation;
use crate::reservation::OwnershipRequest;
use crate::reservation::Reservation;
use crate::reservation::ReservationRequest;

fn bound() -> Reservation {
    let state = original_intent(&terminated());
    state
        .recover_host(binding(&state, "failed-candidate"))
        .unwrap()
}

fn admit(state: &Reservation) -> Reservation {
    state
        .recover_host(HostRequest::AdmitReplacementTermination {
            fence: state.operation().unwrap().fence.clone(),
            candidate: state.operation().unwrap().replacement.clone().unwrap(),
            now_ms: 3400,
            observation: survivors(state, 3200, 100),
        })
        .unwrap()
}

fn record(state: &Reservation) -> Reservation {
    state
        .recover_host(HostRequest::RecordReplacementTermination {
            fence: state.operation().unwrap().fence.clone(),
            candidate: state.operation().unwrap().replacement.clone().unwrap(),
            now_ms: 3700,
            observation: HostTerminationObservation {
                started_ms: 3500,
                completed_ms: 3600,
                provider_instance: "new-instance-1".into(),
                terminal_state: "terminated".into(),
            },
        })
        .unwrap()
}

fn restage(state: &Reservation) -> HostRequest {
    HostRequest::RestageHostReplacement {
        fence: state.operation().unwrap().fence.clone(),
        candidate: state.operation().unwrap().replacement.clone().unwrap(),
        now_ms: 3800,
    }
}

fn rebound(state: &Reservation) -> Reservation {
    let mut request = binding(state, "next-candidate");
    if let HostRequest::BindHostReplacement {
        process_plan, node, ..
    } = &mut request
    {
        process_plan[0].expected_process_incarnation = Some(ProcessIncarnation::from_bits(101));
        node["metadata"]["uid"] = json!("next-node");
        node["spec"]["providerID"] = json!("next-instance");
    }
    state.recover_host(request).unwrap()
}

#[test]
fn bound_candidate_cannot_be_cleared_until_exact_irreversible_fence_is_recorded() {
    let state = bound();
    state.recover_host(restage(&state)).unwrap_err();
    let admitted = admit(&state);
    admitted.recover_host(restage(&admitted)).unwrap_err();
    assert!(
        admitted.recover_host(complete(&admitted)).is_err(),
        "asynchronous termination intent must forbid release"
    );
    for (provider, status) in [
        ("instance-1", "terminated"),
        ("new-instance-1", "stopped"),
        ("new-instance-1", "shutting-down"),
        ("new-instance-1", "not-found"),
    ] {
        let action = HostRequest::RecordReplacementTermination {
            fence: admitted.operation().unwrap().fence.clone(),
            candidate: admitted.operation().unwrap().replacement.clone().unwrap(),
            now_ms: 3700,
            observation: HostTerminationObservation {
                started_ms: 3500,
                completed_ms: 3600,
                provider_instance: provider.into(),
                terminal_state: status.into(),
            },
        };
        admitted.recover_host(action).unwrap_err();
    }
    let fenced = record(&admitted);
    fenced.recover_host(complete(&fenced)).unwrap_err();
    let next = fenced.recover_host(restage(&fenced)).unwrap();
    assert!(next.operation().unwrap().replacement.is_none());
    let host = next.operation().unwrap().host.as_ref().unwrap();
    assert_eq!(host.retired_replacements.len(), 1);
    assert!(host.pod_retirement_intents.contains("failed-candidate"));
    assert!(host.pod_retirement_intents.contains("pod-1"));
    assert_eq!(
        host.retained_replacement_prefix
            .as_ref()
            .unwrap()
            .verification
            .verification
            .prefixes[&0]
            .required_applied_index,
        100
    );
}

#[test]
fn candidate_retirement_requires_current_fresh_survivors_and_retains_the_new_prefix_floor() {
    let state = bound();
    for mutation in 0..5 {
        let mut observation = survivors(&state, 3200, 100);
        match mutation {
            0 => {
                observation
                    .verification
                    .verification
                    .participation_certified = false;
            }
            1 => {
                observation
                    .verification
                    .verification
                    .process_incarnations
                    .insert(2, ProcessIncarnation::from_bits(999));
            }
            2 => {
                observation
                    .verification
                    .verification
                    .prefixes
                    .get_mut(&0)
                    .unwrap()
                    .required_applied_index = 59;
            }
            3 => {
                observation.started_ms = 1500;
            }
            _ => {
                observation.verification.excluded_voter_id = 2;
            }
        }
        state
            .recover_host(HostRequest::AdmitReplacementTermination {
                fence: state.operation().unwrap().fence.clone(),
                candidate: state.operation().unwrap().replacement.clone().unwrap(),
                now_ms: 3400,
                observation,
            })
            .unwrap_err();
    }
    let next = record(&admit(&state));
    let next = next.recover_host(restage(&next)).unwrap();
    let next = rebound(&next);
    let mut finish = complete(&next);
    if let HostRequest::CompleteHostReplacement {
        now_ms,
        observation,
        ..
    } = &mut finish
    {
        *now_ms = 5000;
        *observation = super::super::observation(&next, true, 4500, 99);
    }
    assert!(
        next.recover_host(finish.clone()).is_err(),
        "latest survivor floor must survive unbinding"
    );
    if let HostRequest::CompleteHostReplacement { observation, .. } = &mut finish {
        *observation = super::super::observation(&next, true, 4500, 120);
    }
    let done = next.recover_host(finish).unwrap();
    done.validate().unwrap();
    assert_eq!(
        done.completion()
            .unwrap()
            .host
            .as_ref()
            .unwrap()
            .retired_replacements
            .len(),
        1
    );
}

#[test]
fn stale_delete_or_previous_physical_boot_can_never_bind_after_restaging() {
    let state = record(&admit(&bound()));
    let next = state.recover_host(restage(&state)).unwrap();
    next.recover_host(binding(&next, "failed-candidate"))
        .unwrap_err();
    for reuse in 0..3 {
        let mut action = binding(&next, "different-pod");
        if let HostRequest::BindHostReplacement {
            node, process_plan, ..
        } = &mut action
        {
            node["metadata"]["uid"] = json!("different-node");
            node["spec"]["providerID"] = json!("different-instance");
            process_plan[0].expected_process_incarnation = Some(ProcessIncarnation::from_bits(101));
            match reuse {
                0 => node["metadata"]["uid"] = json!("new-node-1"),
                1 => node["spec"]["providerID"] = json!("new-instance-1"),
                _ => {
                    process_plan[0].expected_process_incarnation =
                        Some(ProcessIncarnation::from_bits(100))
                }
            }
        }
        next.recover_host(action).unwrap_err();
    }
    rebound(&next).validate().unwrap();
}

#[test]
fn takeover_keeps_candidate_intent_receipt_floor_and_delete_history() {
    let fenced = record(&admit(&bound()));
    let old = restage(&fenced);
    let taken = fenced
        .propose(OwnershipRequest::Takeover {
            operation_id: format!("{:032x}", 1),
            executor_id: format!("{:032x}", 99),
            now_ms: 3800,
        })
        .unwrap();
    taken.recover_host(old).unwrap_err();
    let restaged = taken.recover_host(restage(&taken)).unwrap();
    let taken = restaged
        .propose(OwnershipRequest::Takeover {
            operation_id: format!("{:032x}", 1),
            executor_id: format!("{:032x}", 98),
            now_ms: 3900,
        })
        .unwrap();
    assert_eq!(
        taken
            .operation()
            .unwrap()
            .host
            .as_ref()
            .unwrap()
            .retired_replacements
            .len(),
        1
    );
    assert!(
        taken
            .operation()
            .unwrap()
            .host
            .as_ref()
            .unwrap()
            .pod_retirement_intents
            .contains("failed-candidate")
    );
    taken.validate().unwrap();
}

#[test]
fn restaged_state_preserves_schema_and_cannot_prune_its_floor() {
    let fenced = record(&admit(&bound()));
    let action = restage(&fenced);
    let bytes = serde_json::to_vec(&ReservationRequest::Host(action)).unwrap();
    let ReservationRequest::Host(action) = serde_json::from_slice(&bytes).unwrap() else {
        unreachable!()
    };
    let next = fenced.recover_host(action).unwrap();
    let encoded = serde_json::to_value(&next).unwrap();
    assert_eq!(
        encoded["version"],
        super::super::super::RESERVATION_SCHEMA_VERSION
    );
    serde_json::from_value::<Reservation>(encoded.clone())
        .unwrap()
        .validate()
        .unwrap();
    for mutation in 0..3 {
        let mut tampered = encoded.clone();
        match mutation {
            0 => tampered["hosts"] = serde_json::Value::Null,
            1 => tampered["operation"]["host"]["pod_retirement_intents"] = json!(["pod-1"]),
            _ => {
                tampered["operation"]["host"]["retained_replacement_prefix"] =
                    serde_json::Value::Null
            }
        }
        serde_json::from_value::<Reservation>(tampered)
            .unwrap()
            .validate()
            .unwrap_err();
    }
}

#[test]
fn delayed_actions_cannot_retarget_a_later_candidate_in_the_same_generation() {
    let first = bound();
    let old_candidate = first.operation().unwrap().replacement.clone().unwrap();
    let fenced = record(&admit(&first));
    let next = rebound(&fenced.recover_host(restage(&fenced)).unwrap());
    let action = HostRequest::AdmitReplacementTermination {
        fence: next.operation().unwrap().fence.clone(),
        candidate: old_candidate.clone(),
        now_ms: 4300,
        observation: survivors(&next, 4000, 120),
    };
    assert!(
        next.recover_host(action)
            .unwrap_err()
            .to_string()
            .contains("candidate identity")
    );
    let action = HostRequest::RestageHostReplacement {
        fence: next.operation().unwrap().fence.clone(),
        candidate: old_candidate,
        now_ms: 4300,
    };
    assert!(
        next.recover_host(action)
            .unwrap_err()
            .to_string()
            .contains("candidate identity")
    );
}

#[test]
fn repeated_candidate_loss_is_bounded_before_another_provider_intent_can_be_admitted() {
    let mut state = bound();
    for i in 0..8_u64 {
        let start = 3200 + i * 1000;
        let candidate = state.operation().unwrap().replacement.clone().unwrap();
        state = state
            .recover_host(HostRequest::AdmitReplacementTermination {
                fence: state.operation().unwrap().fence.clone(),
                candidate: candidate.clone(),
                now_ms: start + 200,
                observation: survivors(&state, start, 100 + i),
            })
            .unwrap();
        state = state
            .recover_host(HostRequest::RecordReplacementTermination {
                fence: state.operation().unwrap().fence.clone(),
                candidate: candidate.clone(),
                now_ms: start + 500,
                observation: HostTerminationObservation {
                    started_ms: start + 300,
                    completed_ms: start + 400,
                    provider_instance: candidate.provider_instance.clone(),
                    terminal_state: "terminated".into(),
                },
            })
            .unwrap();
        state = state
            .recover_host(HostRequest::RestageHostReplacement {
                fence: state.operation().unwrap().fence.clone(),
                candidate,
                now_ms: start + 600,
            })
            .unwrap();
        let mut action = binding(&state, &format!("candidate-{i}"));
        if let HostRequest::BindHostReplacement {
            node, process_plan, ..
        } = &mut action
        {
            node["metadata"]["uid"] = json!(format!("candidate-node-{i}"));
            node["spec"]["providerID"] = json!(format!("candidate-instance-{i}"));
            process_plan[0].expected_process_incarnation =
                Some(ProcessIncarnation::from_bits(200 + u128::from(i)));
        }
        state = state.recover_host(action).unwrap();
    }
    let action = HostRequest::AdmitReplacementTermination {
        fence: state.operation().unwrap().fence.clone(),
        candidate: state.operation().unwrap().replacement.clone().unwrap(),
        now_ms: 12000,
        observation: survivors(&state, 11800, 200),
    };
    assert!(
        state
            .recover_host(action)
            .unwrap_err()
            .to_string()
            .contains("budget")
    );
    assert_eq!(
        state
            .operation()
            .unwrap()
            .host
            .as_ref()
            .unwrap()
            .retired_replacements
            .len(),
        8
    );
}

#[test]
fn restaging_requires_exact_committed_cas_and_then_allows_only_one_new_startup_boot() {
    let fenced = record(&admit(&bound()));
    let mut raw = super::super::document();
    raw["data"]["reservation"] = json!(serde_json::to_string(&fenced).unwrap());
    let before = ConfigMapSnapshot::parse(raw, &super::super::cell()).unwrap();
    let proposal = before
        .transition(ReservationRequest::Host(restage(&fenced)))
        .unwrap();
    assert!(before.state().operation().unwrap().replacement.is_some());
    let mut response = proposal.document().clone();
    before.acknowledge(&proposal, response.clone()).unwrap_err();
    response["metadata"]["resourceVersion"] = json!("restaged-rv");
    let mut recreated = response.clone();
    recreated["metadata"]["uid"] = json!("recreated-store");
    before.acknowledge(&proposal, recreated).unwrap_err();
    let next = before.acknowledge(&proposal, response).unwrap();

    let HostRequest::BindHostReplacement {
        mut pod, mut node, ..
    } = binding(next.state(), "failed-candidate")
    else {
        unreachable!()
    };
    next.propose_startup(
        1,
        "failed-candidate",
        &pod,
        &node,
        ProcessIncarnation::from_bits(101),
    )
    .unwrap_err();
    pod["metadata"]["uid"] = json!("next-pod");
    node["metadata"]["uid"] = json!("next-node");
    node["spec"]["providerID"] = json!("next-instance");
    let first = next
        .propose_startup(
            1,
            "next-pod",
            &pod,
            &node,
            ProcessIncarnation::from_bits(101),
        )
        .unwrap()
        .unwrap();
    let second = next
        .propose_startup(
            1,
            "next-pod",
            &pod,
            &node,
            ProcessIncarnation::from_bits(102),
        )
        .unwrap()
        .unwrap();
    let mut response = first.document().clone();
    response["metadata"]["resourceVersion"] = json!("next-boot-rv");
    next.acknowledge(&second, response.clone()).unwrap_err();
    let accepted = next.acknowledge(&first, response).unwrap();
    accepted
        .propose_startup(
            1,
            "next-pod",
            &pod,
            &node,
            ProcessIncarnation::from_bits(102),
        )
        .unwrap_err();
    assert_eq!(
        accepted
            .state()
            .operation()
            .unwrap()
            .host
            .as_ref()
            .unwrap()
            .retired_replacements
            .len(),
        1
    );
}

#[test]
fn extra_pod_retirement_requires_the_recorded_irreversibly_fenced_physical_host() {
    let fenced = record(&admit(&bound()));
    let next = fenced.recover_host(restage(&fenced)).unwrap();
    let HostRequest::BindHostReplacement { mut pod, node, .. } =
        binding(&next, "extra-retired-pod")
    else {
        unreachable!()
    };
    pod["metadata"]["deletionTimestamp"] = json!("already-deleting");
    let action = HostRequest::AdmitFencedPodRetirement {
        fence: next.operation().unwrap().fence.clone(),
        pod: Some(pod.clone()),
        node: Some(node.clone()),
    };
    let recorded = next.recover_host(action.clone()).unwrap();
    assert!(
        recorded
            .operation()
            .unwrap()
            .host
            .as_ref()
            .unwrap()
            .pod_retirement_intents
            .contains("extra-retired-pod")
    );
    for mutation in 0..3 {
        let mut changed = action.clone();
        if let HostRequest::AdmitFencedPodRetirement { pod, node, .. } = &mut changed {
            match mutation {
                0 => node.as_mut().unwrap()["metadata"]["uid"] = json!("recreated-node"),
                1 => node.as_mut().unwrap()["spec"]["providerID"] = json!("healthy-instance"),
                _ => {
                    node.as_mut().unwrap()["metadata"]["name"] = json!("different-host");
                    pod.as_mut().unwrap()["spec"]["nodeName"] = json!("different-host");
                }
            }
        }
        next.recover_host(changed).unwrap_err();
    }
    let bound = rebound(&recorded);
    bound.recover_host(action).unwrap_err();
}

#[test]
fn candidate_intent_cannot_deserialize_into_another_bound_identity() {
    let admitted = admit(&bound());
    let mut tampered = serde_json::to_value(admitted).unwrap();
    tampered["operation"]["host"]["replacement_retirement"]["candidate"]["pod_uid"] =
        json!("another-candidate");
    serde_json::from_value::<Reservation>(tampered)
        .unwrap()
        .validate()
        .unwrap_err();
}

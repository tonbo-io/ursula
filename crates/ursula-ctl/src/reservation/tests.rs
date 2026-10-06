use serde_json::json;
use ursula_proto::admin::ProcessIncarnation;

use super::CellIdentity;
use super::ConfigMapSnapshot;
use super::OwnershipRequest;
use super::Reservation;
use super::SourceIdentity;
use super::Value;
use crate::NodeInfo;

fn cell() -> CellIdentity {
    CellIdentity {
        namespace: "ursula".to_owned(),
        namespace_uid: "namespace-uid".to_owned(),
        statefulset: "voters".to_owned(),
        statefulset_uid: "statefulset-uid".to_owned(),
        group_count: 256,
        core_count: 2,
        voter_ids: [1, 2, 3].into_iter().collect(),
    }
}

fn request(operation: u128, executor: u128, target: u64) -> OwnershipRequest {
    let nodes = (1..=3)
        .map(|id| NodeInfo {
            id,
            host: format!("voter-{id}"),
            admin_url: format!("http://127.0.0.1:{}", 1000 + id).parse().unwrap(),
            http_url: Some(format!("http://voter-{id}:4437").parse().unwrap()),
            metrics_url: None,
            expected_process_incarnation: Some(ProcessIncarnation::from_bits(u128::from(id))),
            expected_maintenance_fence: None,
        })
        .collect();
    OwnershipRequest::Reserve {
        operation_id: format!("{operation:032x}"),
        executor_id: format!("{executor:032x}"),
        now_ms: 1000,
        source: SourceIdentity {
            node_id: target,
            pod_name: format!("voters-{}", target.checked_sub(1).unwrap()),
            pod_uid: format!("pod-{target}"),
            node_uid: format!("node-{target}"),
            provider_instance: format!("instance-{target}"),
            process_incarnation: ProcessIncarnation::from_bits(u128::from(target)),
        },
        process_plan: nodes,
    }
}

fn document() -> Value {
    json!({"apiVersion": "v1", "kind": "ConfigMap", "metadata": {
        "name": "voters-maintenance", "namespace": "ursula", "uid": "store-uid", "resourceVersion": "rv-one",
        "annotations": {"retained": "value"}, "labels": {"cell": "test"}
    }, "data": {"reservation": serde_json::to_string(&Reservation::initial(cell()).unwrap()).unwrap(), "other": "preserved"}})
}

#[test]
fn concurrent_owners_cannot_both_acknowledge_one_store_revision() {
    let before = ConfigMapSnapshot::parse(document(), &cell()).unwrap();
    let winner = before.propose(request(1, 10, 1)).unwrap();
    let loser = before.propose(request(2, 20, 2)).unwrap();
    assert_eq!(winner.document()["metadata"]["resourceVersion"], "rv-one");
    assert_eq!(winner.document()["metadata"]["uid"], "store-uid");
    let mut response = winner.document().clone();
    response["metadata"]["resourceVersion"] = json!("rv-two");
    let acquired = before.acknowledge(&winner, response.clone()).unwrap();
    assert_eq!(acquired.state().generation(), 1);
    assert_eq!(acquired.state().operation().unwrap().source.node_id, 1);
    assert!(before.acknowledge(&loser, response).is_err());
    assert!(acquired.propose(request(2, 20, 2)).is_err());
    assert_eq!(winner.document()["data"]["other"], "preserved");
    assert_eq!(
        winner.document()["metadata"]["annotations"]["retained"],
        "value"
    );
}

#[tokio::test]
async fn two_proposals_over_mock_http_have_one_successful_compare_and_swap() {
    use std::sync::Arc;
    use std::sync::Mutex;

    use axum::Json;
    use axum::Router;
    use axum::http::StatusCode;
    use axum::response::IntoResponse;

    let stored = Arc::new(Mutex::new(document()));
    let database = stored.clone();
    let app = Router::new().route(
        "/configmap",
        axum::routing::put(move |Json(offered): Json<Value>| {
            let database = database.clone();
            async move {
                let mut current = database.lock().unwrap();
                if offered["metadata"]["uid"] != current["metadata"]["uid"]
                    || offered["metadata"]["resourceVersion"]
                        != current["metadata"]["resourceVersion"]
                {
                    return (StatusCode::CONFLICT, Json(current.clone())).into_response();
                }
                *current = offered;
                current["metadata"]["resourceVersion"] = json!("rv-two");
                (StatusCode::OK, Json(current.clone())).into_response()
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let snapshot = ConfigMapSnapshot::parse(document(), &cell()).unwrap();
    let first = snapshot.propose(request(1, 10, 1)).unwrap();
    let second = snapshot.propose(request(2, 20, 2)).unwrap();
    let client = reqwest::Client::new();
    let url = format!("http://{address}/configmap");
    let (first_result, second_result) = tokio::join!(
        client.put(&url).json(first.document()).send(),
        client.put(&url).json(second.document()).send(),
    );
    let first_result = first_result.unwrap();
    let second_result = second_result.unwrap();
    let statuses = [first_result.status(), second_result.status()];
    assert_eq!(
        statuses
            .iter()
            .filter(|status| **status == reqwest::StatusCode::OK)
            .count(),
        1
    );
    assert_eq!(
        statuses
            .iter()
            .filter(|status| **status == reqwest::StatusCode::CONFLICT)
            .count(),
        1
    );
    let first_body: Value = first_result.json().await.unwrap();
    let second_body: Value = second_result.json().await.unwrap();
    assert_eq!(
        snapshot.acknowledge(&first, first_body).is_ok(),
        statuses[0].is_success()
    );
    assert_eq!(
        snapshot.acknowledge(&second, second_body).is_ok(),
        statuses[1].is_success()
    );
    let committed = ConfigMapSnapshot::parse(stored.lock().unwrap().clone(), &cell()).unwrap();
    assert!(committed.propose(request(3, 30, 3)).is_err());
    server.abort();
}

#[test]
fn takeover_preserves_source_and_processes_and_advances_only_executor_authority() {
    let initial = Reservation::initial(cell()).unwrap();
    let reserved = initial.propose(request(1, 10, 3)).unwrap();
    for (operation, executor) in [(2, 20), (1, 10)] {
        assert!(
            reserved
                .propose(OwnershipRequest::Takeover {
                    operation_id: format!("{operation:032x}"),
                    executor_id: format!("{executor:032x}"),
                    now_ms: 2000,
                })
                .is_err()
        );
    }
    let resumed = reserved
        .propose(OwnershipRequest::Takeover {
            operation_id: format!("{:032x}", 1),
            executor_id: format!("{:032x}", 20),
            now_ms: 2000,
        })
        .unwrap();
    assert_eq!(resumed.generation(), 2);
    let old = reserved.operation().unwrap();
    let new = resumed.operation().unwrap();
    assert_eq!(new.source, old.source);
    assert_eq!(new.fence.reservation_id(), old.fence.reservation_id());
    assert_ne!(new.fence.executor_id(), old.fence.executor_id());
    for (old_node, new_node) in old.process_plan.iter().zip(&new.process_plan) {
        assert_eq!(
            old_node.expected_process_incarnation,
            new_node.expected_process_incarnation
        );
        assert_eq!(old_node.admin_url, new_node.admin_url);
        assert_eq!(
            new_node.expected_maintenance_fence.as_ref(),
            Some(&new.fence)
        );
    }
}

#[test]
fn acknowledgement_requires_original_store_identity_and_exact_committed_state() {
    let before = ConfigMapSnapshot::parse(document(), &cell()).unwrap();
    let proposal = before.propose(request(1, 10, 1)).unwrap();
    assert!(
        before
            .acknowledge(&proposal, proposal.document().clone())
            .is_err()
    );
    let mut recreated = proposal.document().clone();
    recreated["metadata"]["uid"] = json!("another-store");
    recreated["metadata"]["resourceVersion"] = json!("rv-two");
    assert!(before.acknowledge(&proposal, recreated).is_err());
    let mut overwritten = proposal.document().clone();
    overwritten["metadata"]["resourceVersion"] = json!("rv-two");
    overwritten["data"]["other"] = json!("changed");
    assert!(before.acknowledge(&proposal, overwritten).is_err());
    let mut newer_document = document();
    newer_document["metadata"]["resourceVersion"] = json!("rv-other");
    let newer = ConfigMapSnapshot::parse(newer_document, &cell()).unwrap();
    let mut original_response = proposal.document().clone();
    original_response["metadata"]["resourceVersion"] = json!("rv-two");
    assert!(newer.acknowledge(&proposal, original_response).is_err());
}

#[test]
fn ephemeral_deleting_misdirected_or_missing_stores_fail_closed() {
    for (field, value) in [
        ("uid", json!("")),
        ("resourceVersion", json!("")),
        ("namespace", json!("another")),
        ("name", json!("another")),
        ("deletionTimestamp", json!("now")),
        ("ownerReferences", json!([{"uid": "job"}])),
        ("annotations", json!({"helm.sh/hook": "post-upgrade"})),
    ] {
        let mut raw = document();
        raw["metadata"][field] = value;
        assert!(ConfigMapSnapshot::parse(raw, &cell()).is_err());
    }
    let mut wrong_cell = cell();
    wrong_cell.statefulset_uid = "new-lifetime".to_owned();
    assert!(ConfigMapSnapshot::parse(document(), &wrong_cell).is_err());
    let mut missing = document();
    missing["data"]
        .as_object_mut()
        .unwrap()
        .remove("reservation");
    assert!(ConfigMapSnapshot::parse(missing, &cell()).is_err());
}

#[test]
fn incomplete_duplicate_or_rebound_process_plans_cannot_reserve() {
    for case in 0..5 {
        let mut action = request(1, 10, 1);
        let OwnershipRequest::Reserve {
            source,
            process_plan,
            ..
        } = &mut action
        else {
            unreachable!()
        };
        match case {
            0 => {
                process_plan.pop();
            }
            1 => process_plan[1].id = 1,
            2 => process_plan[1].expected_process_incarnation = None,
            3 => source.process_incarnation = ProcessIncarnation::from_bits(99),
            _ => source.pod_name = "voters-2".to_owned(),
        }
        assert!(
            Reservation::initial(cell())
                .unwrap()
                .propose(action)
                .is_err()
        );
    }
}

#[test]
fn clearing_an_active_operation_or_wrapping_generation_never_opens_a_new_source() {
    let mut reserved = Reservation::initial(cell())
        .unwrap()
        .propose(request(1, 10, 1))
        .unwrap();
    reserved.operation = None;
    assert!(reserved.validate().is_err());
    let mut reserved = Reservation::initial(cell())
        .unwrap()
        .propose(request(1, 10, 1))
        .unwrap();
    reserved.generation = u64::MAX;
    let operation = reserved.operation.as_mut().unwrap();
    operation.fence = ursula_proto::admin::MaintenanceFence::new(
        format!("{:032x}", 1),
        format!("{:032x}", 10),
        u64::MAX,
    )
    .unwrap();
    for node in &mut operation.process_plan {
        node.expected_maintenance_fence = Some(operation.fence.clone());
    }
    reserved.validate().unwrap();
    let error = reserved
        .propose(OwnershipRequest::Takeover {
            operation_id: format!("{:032x}", 1),
            executor_id: format!("{:032x}", 20),
            now_ms: 2000,
        })
        .unwrap_err();
    assert!(error.to_string().contains("exhausted"));
}
fn observation(
    state: &Reservation,
    retired: bool,
    start: u64,
    index: u64,
) -> super::PrefixObservation {
    use std::collections::BTreeMap;
    let operation = state.operation().unwrap();
    let prefixes = (0..state.cell.group_count)
        .map(|group| {
            (group, ursula_raft::QuorumPrefix {
                raft_group_id: group,
                leader_id: 2,
                leader_term: 5,
                required_applied_index: index,
            })
        })
        .collect();
    super::PrefixObservation {
        started_ms: start,
        completed_ms: start + 100,
        verification: crate::quorum::QuorumVerification {
            version: 3,
            participation_certified: true,
            process_incarnations_certified: true,
            process_incarnations: operation
                .process_plan
                .iter()
                .map(|node| (node.id, node.expected_process_incarnation.clone().unwrap()))
                .collect(),
            maintenance_executor_certified: !retired,
            maintenance_executor_retired_certified: retired,
            maintenance_fence: Some(operation.fence.clone()),
            prefixes,
            applied: (1..=3)
                .map(|node| {
                    (
                        node,
                        (0..state.cell.group_count)
                            .map(|group| (group, index))
                            .collect::<BTreeMap<_, _>>(),
                    )
                })
                .collect(),
        },
    }
}

fn admitted() -> Reservation {
    let mut inventory = cell();
    inventory.group_count = 2;
    let reserved = Reservation::initial(inventory)
        .unwrap()
        .propose(request(1, 10, 1))
        .unwrap();
    reserved
        .progress(super::ProgressRequest::AdmitPodDeletion {
            fence: reserved.operation().unwrap().fence.clone(),
            now_ms: 1500,
            observation: observation(&reserved, false, 1200, 50),
        })
        .unwrap()
}

fn binding(state: &Reservation) -> super::ProgressRequest {
    let mut plan = state.operation().unwrap().process_plan.clone();
    plan[0].expected_process_incarnation = Some(ProcessIncarnation::from_bits(100));
    super::ProgressRequest::BindPodReplacement {
        fence: state.operation().unwrap().fence.clone(),
        pod: json!({"kind":"Pod", "metadata":{"namespace":"ursula", "name":"voters-0", "uid":"replacement-pod", "ownerReferences":[{"kind":"StatefulSet", "uid":"statefulset-uid", "controller":true}]}, "spec":{"nodeName":"host-1"}}),
        node: json!({"kind":"Node", "metadata":{"name":"host-1", "uid":"node-1"}, "spec":{"providerID":"instance-1"}}),
        process_plan: plan,
    }
}

#[test]
fn release_requires_retired_original_uid_and_all_retired_current_prefixes() {
    let active = admitted();
    let complete = super::ProgressRequest::CompletePodReplacement {
        fence: active.operation().unwrap().fence.clone(),
        now_ms: 2000,
        observation: observation(&active, true, 1700, 60),
    };
    assert!(active.progress(complete).is_err()); // A container restart is insufficient.
    let rebound = active.progress(binding(&active)).unwrap();
    assert!(rebound.progress(binding(&rebound)).is_err());
    for case in 0..7 {
        let mut proof = observation(&rebound, true, 1700, 60);
        match case {
            0 => {
                proof.verification.maintenance_executor_retired_certified = false;
            }
            1 => {
                proof.verification.applied.remove(&3);
            }
            2 => {
                proof
                    .verification
                    .applied
                    .get_mut(&3)
                    .unwrap()
                    .insert(1, 49);
            }
            3 => {
                proof
                    .verification
                    .prefixes
                    .get_mut(&1)
                    .unwrap()
                    .required_applied_index = 49;
            }
            4 => {
                proof
                    .verification
                    .process_incarnations
                    .insert(2, ProcessIncarnation::from_bits(999));
            }
            5 => {
                proof.started_ms = 900;
            }
            _ => {
                proof.verification.prefixes.remove(&1);
            }
        }
        assert!(
            rebound
                .progress(super::ProgressRequest::CompletePodReplacement {
                    fence: rebound.operation().unwrap().fence.clone(),
                    now_ms: 2000,
                    observation: proof
                })
                .is_err()
        );
    }
    let completed = rebound
        .progress(super::ProgressRequest::CompletePodReplacement {
            fence: rebound.operation().unwrap().fence.clone(),
            now_ms: 2000,
            observation: observation(&rebound, true, 1700, 60),
        })
        .unwrap();
    assert!(completed.operation().is_none());
    assert_eq!(completed.generation(), 1);
    assert!(completed.propose(request(2, 20, 2)).is_err()); // Old target process cannot reopen maintenance.
    let mut next_request = request(2, 20, 2);
    if let OwnershipRequest::Reserve {
        process_plan,
        now_ms,
        ..
    } = &mut next_request
    {
        process_plan[0].expected_process_incarnation = Some(ProcessIncarnation::from_bits(100));
        *now_ms = 2000;
    }
    let next = completed.propose(next_request).unwrap();
    assert_eq!(next.generation(), 2);
    assert_eq!(next.operation().unwrap().source.node_id, 2);
}

#[test]
fn takeover_cannot_discard_admitted_source_or_replay_old_executor_proofs() {
    let active = admitted();
    let old_binding = binding(&active);
    let takeover = active
        .propose(OwnershipRequest::Takeover {
            operation_id: format!("{:032x}", 1),
            executor_id: format!("{:032x}", 20),
            now_ms: 2000,
        })
        .unwrap();
    assert_eq!(
        takeover
            .operation()
            .unwrap()
            .admission
            .as_ref()
            .unwrap()
            .verification
            .prefixes[&0]
            .required_applied_index,
        50
    );
    assert!(takeover.progress(old_binding).is_err());
    assert!(takeover.propose(request(3, 30, 2)).is_err());
    let mut wrong_pod = binding(&takeover);
    if let super::ProgressRequest::BindPodReplacement { pod, .. } = &mut wrong_pod {
        pod["metadata"]["uid"] = json!("pod-1");
    }
    assert!(takeover.progress(wrong_pod).is_err());
    let rebound = takeover.progress(binding(&takeover)).unwrap();
    let old_proof = observation(&active, true, 2100, 60);
    assert!(
        rebound
            .progress(super::ProgressRequest::CompletePodReplacement {
                fence: rebound.operation().unwrap().fence.clone(),
                now_ms: 2400,
                observation: old_proof
            })
            .is_err()
    );
    let complete = rebound
        .progress(super::ProgressRequest::CompletePodReplacement {
            fence: rebound.operation().unwrap().fence.clone(),
            now_ms: 2400,
            observation: observation(&rebound, true, 2100, 60),
        })
        .unwrap();
    assert_eq!(complete.generation(), 2);
}

#[test]
fn stale_future_or_incomplete_admission_cannot_authorize_pod_deletion() {
    let state = Reservation::initial(cell())
        .unwrap()
        .propose(request(1, 10, 1))
        .unwrap();
    for case in 0..5 {
        let mut proof = observation(&state, false, 1200, 50);
        let mut now_ms = 1500;
        match case {
            0 => now_ms = 100_000,
            1 => now_ms = 1250,
            2 => proof.started_ms = 999,
            3 => {
                proof.verification.applied.get_mut(&2).unwrap().remove(&255);
            }
            _ => proof
                .verification
                .process_incarnations
                .insert(2, ProcessIncarnation::from_bits(999))
                .map(|_| ())
                .unwrap(),
        }
        assert!(
            state
                .progress(super::ProgressRequest::AdmitPodDeletion {
                    fence: state.operation().unwrap().fence.clone(),
                    now_ms,
                    observation: proof
                })
                .is_err()
        );
    }
}

#[test]
fn oversized_declared_inventory_cannot_allocate_missing_group_evidence() {
    let mut inventory = cell();
    inventory.group_count = 2;
    let mut reserved = Reservation::initial(inventory)
        .unwrap()
        .propose(request(1, 10, 1))
        .unwrap();
    let proof = observation(&reserved, false, 1200, 50);
    reserved.cell.group_count = u32::MAX;
    assert!(
        reserved
            .progress(super::ProgressRequest::AdmitPodDeletion {
                fence: reserved.operation().unwrap().fence.clone(),
                now_ms: 1500,
                observation: proof,
            })
            .is_err()
    );
}

#[test]
fn captured_api_identity_rejects_misdirected_or_deleting_objects() {
    let namespace =
        json!({"kind":"Namespace", "metadata":{"name":"ursula", "uid":"namespace-uid"}});
    let sts = json!({"kind":"StatefulSet", "metadata":{"namespace":"ursula", "name":"voters", "uid":"statefulset-uid"}, "spec":{"replicas":3}});
    assert_eq!(
        CellIdentity::capture(&namespace, &sts, 256, 2).unwrap(),
        cell()
    );
    for (path, value) in [
        ("/spec/replicas", json!(2)),
        ("/metadata/namespace", json!("other")),
        ("/metadata/uid", json!("")),
    ] {
        let mut invalid = sts.clone();
        *invalid.pointer_mut(path).unwrap() = value;
        assert!(CellIdentity::capture(&namespace, &invalid, 256, 2).is_err());
    }
    let active = admitted();
    let super::ProgressRequest::BindPodReplacement {
        pod,
        node,
        process_plan,
        ..
    } = binding(&active)
    else {
        unreachable!()
    };
    let captured = SourceIdentity::capture(&active.cell, 1, &pod, &node, &process_plan).unwrap();
    assert_eq!(captured.node_uid, "node-1");
    assert_eq!(
        captured.process_incarnation,
        ProcessIncarnation::from_bits(100)
    );
    for case in 0..6 {
        let mut invalid_pod = pod.clone();
        let mut invalid_node = node.clone();
        let mut invalid_plan = process_plan.clone();
        match case {
            0 => {
                invalid_pod["metadata"]["ownerReferences"][0]["uid"] =
                    json!("recreated-statefulset")
            }
            1 => invalid_pod["metadata"]["deletionTimestamp"] = json!("now"),
            2 => invalid_node["metadata"]["deletionTimestamp"] = json!("now"),
            3 => invalid_node["metadata"]["name"] = json!("other-host"),
            4 => invalid_node["spec"]["providerID"] = json!(""),
            _ => invalid_plan[0].expected_process_incarnation = None,
        }
        assert!(
            SourceIdentity::capture(&active.cell, 1, &invalid_pod, &invalid_node, &invalid_plan)
                .is_err()
        );
    }
}

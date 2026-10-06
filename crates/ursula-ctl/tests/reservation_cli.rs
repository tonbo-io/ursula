//! Actual offline CLI process checks. No Kubernetes or provider operation.
use std::process::Command;

use serde_json::Value;
use serde_json::json;
use ursula_ctl::NodeInfo;
use ursula_ctl::reservation::CellIdentity;
use ursula_ctl::reservation::OwnershipRequest;
use ursula_ctl::reservation::PrefixObservation;
use ursula_ctl::reservation::ProgressRequest;
use ursula_ctl::reservation::Reservation;
use ursula_ctl::reservation::SourceIdentity;
use ursula_proto::admin::ProcessIncarnation;

#[test]
fn proposal_is_not_a_receipt_and_conflicting_committed_state_cannot_be_adopted() {
    let directory = tempfile::tempdir().unwrap();
    let cell = CellIdentity {
        namespace: "test".into(),
        namespace_uid: "namespace-uid".into(),
        statefulset: "voters".into(),
        statefulset_uid: "statefulset-uid".into(),
        group_count: 256,
        core_count: 2,
        voter_ids: [1, 2, 3].into_iter().collect(),
    };
    let plan = (1..=3)
        .map(|id| NodeInfo {
            id,
            host: format!("voters-{id}"),
            admin_url: format!("http://127.0.0.1:{}", 1000 + id).parse().unwrap(),
            http_url: None,
            metrics_url: None,
            expected_process_incarnation: Some(ProcessIncarnation::from_bits(u128::from(id))),
            expected_maintenance_fence: None,
        })
        .collect();
    let request = OwnershipRequest::Reserve {
        operation_id: format!("{:032x}", 1),
        executor_id: format!("{:032x}", 10),
        now_ms: 1000,
        source: SourceIdentity {
            node_id: 1,
            pod_name: "voters-0".into(),
            pod_uid: "pod-1".into(),
            node_uid: "node-1".into(),
            provider_instance: "instance-1".into(),
            process_incarnation: ProcessIncarnation::from_bits(1),
        },
        process_plan: plan,
    };
    let initial = json!({"apiVersion":"v1", "kind":"ConfigMap", "metadata":{"namespace":"test", "name":"voters-maintenance", "uid":"store-uid", "resourceVersion":"first"}, "data":{"reservation":serde_json::to_string(&Reservation::initial(cell.clone()).unwrap()).unwrap()}});
    for (name, value) in [
        ("cell", serde_json::to_value(cell).unwrap()),
        ("snapshot", initial),
        ("request", serde_json::to_value(request).unwrap()),
    ] {
        std::fs::write(
            directory.path().join(name),
            serde_json::to_vec(&value).unwrap(),
        )
        .unwrap();
    }
    let run = |subcommand: &str, response: Option<&Value>| {
        let mut command = Command::new(env!("CARGO_BIN_EXE_ursulactl"));
        command.arg(subcommand);
        for name in ["cell", "snapshot", "request"] {
            command
                .arg(format!("--{name}"))
                .arg(directory.path().join(name));
        }
        if let Some(value) = response {
            let path = directory.path().join("response");
            std::fs::write(&path, serde_json::to_vec(value).unwrap()).unwrap();
            command.arg("--response").arg(path);
        }
        command.output().unwrap()
    };
    let result = run("reservation-propose", None);
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    let proposal: Value = serde_json::from_slice(&result.stdout).unwrap();
    assert_eq!(proposal["metadata"]["resourceVersion"], "first");
    assert!(
        !run("reservation-acknowledge", Some(&proposal))
            .status
            .success()
    );
    let mut accepted = proposal.clone();
    accepted["metadata"]["resourceVersion"] = json!("second");
    let result = run("reservation-acknowledge", Some(&accepted));
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    let receipt: Value = serde_json::from_slice(&result.stdout).unwrap();
    assert_eq!(receipt["reservation_proposal_acknowledged"], true);
    assert_eq!(receipt["disruption_authorized"], false);
    assert_eq!(receipt["physical_hosts_fenced"], false);
    assert_eq!(receipt["nodes"].as_array().unwrap().len(), 3);
    let mut other_state: Value =
        serde_json::from_str(accepted["data"]["reservation"].as_str().unwrap()).unwrap();
    other_state["operation"]["source"]["pod_uid"] = json!("different-source");
    accepted["data"]["reservation"] = json!(serde_json::to_string(&other_state).unwrap());
    assert!(
        !run("reservation-acknowledge", Some(&accepted))
            .status
            .success()
    );
    // Persist and consume actual progress JSON through the same CLI, with
    // synthetic prefix evidence. This is not a live Raft or Kubernetes test.
    let mut stored = proposal;
    stored["metadata"]["resourceVersion"] = json!("second");
    let state: Reservation =
        serde_json::from_str(stored["data"]["reservation"].as_str().unwrap()).unwrap();
    let fence = state.operation().unwrap().fence.clone();
    let proof = |state: &Reservation, retired: bool, started_ms: u64| PrefixObservation {
        started_ms,
        completed_ms: started_ms + 100,
        verification: ursula_ctl::quorum::QuorumVerification {
            version: 3,
            participation_certified: true,
            process_incarnations_certified: true,
            process_incarnations: state
                .operation()
                .unwrap()
                .process_plan
                .iter()
                .map(|node| (node.id, node.expected_process_incarnation.clone().unwrap()))
                .collect(),
            maintenance_executor_certified: !retired,
            maintenance_executor_retired_certified: retired,
            maintenance_fence: Some(fence.clone()),
            prefixes: (0..256)
                .map(|group| {
                    (group, ursula_raft::QuorumPrefix {
                        raft_group_id: group,
                        leader_id: 2,
                        leader_term: 1,
                        required_applied_index: 50,
                    })
                })
                .collect(),
            applied: (1..=3)
                .map(|node| (node, (0..256).map(|group| (group, 50)).collect()))
                .collect(),
        },
    };
    let advance = |stored: &mut Value, request: ProgressRequest, revision: &str| {
        std::fs::write(
            directory.path().join("snapshot"),
            serde_json::to_vec(stored).unwrap(),
        )
        .unwrap();
        std::fs::write(
            directory.path().join("request"),
            serde_json::to_vec(&request).unwrap(),
        )
        .unwrap();
        let offered = run("reservation-propose", None);
        assert!(
            offered.status.success(),
            "{}",
            String::from_utf8_lossy(&offered.stderr)
        );
        let mut response: Value = serde_json::from_slice(&offered.stdout).unwrap();
        response["metadata"]["resourceVersion"] = json!(revision);
        let accepted = run("reservation-acknowledge", Some(&response));
        assert!(
            accepted.status.success(),
            "{}",
            String::from_utf8_lossy(&accepted.stderr)
        );
        *stored = response;
        serde_json::from_slice::<Value>(&accepted.stdout).unwrap()
    };
    advance(
        &mut stored,
        ProgressRequest::AdmitPodDeletion {
            fence: fence.clone(),
            now_ms: 1500,
            observation: proof(&state, false, 1200),
        },
        "third",
    );
    let mut replacement_plan = state.operation().unwrap().process_plan.clone();
    replacement_plan[0].expected_process_incarnation = Some(ProcessIncarnation::from_bits(100));
    advance(
        &mut stored,
        ProgressRequest::BindPodReplacement {
            fence: fence.clone(),
            process_plan: replacement_plan,
            pod: json!({"kind":"Pod", "metadata":{"namespace":"test", "name":"voters-0", "uid":"replacement-pod", "ownerReferences":[{"kind":"StatefulSet", "uid":"statefulset-uid", "controller":true}]}, "spec":{"nodeName":"host-1"}}),
            node: json!({"kind":"Node", "metadata":{"name":"host-1", "uid":"node-1"}, "spec":{"providerID":"instance-1"}}),
        },
        "fourth",
    );
    let rebound: Reservation =
        serde_json::from_str(stored["data"]["reservation"].as_str().unwrap()).unwrap();
    let completed = advance(
        &mut stored,
        ProgressRequest::CompletePodReplacement {
            fence: fence.clone(),
            now_ms: 2000,
            observation: proof(&rebound, true, 1700),
        },
        "fifth",
    );
    assert!(completed["nodes"].is_null());
    assert!(completed["reservation"]["operation"].is_null());
    assert_eq!(completed["reservation"]["generation"], 1);
    assert_eq!(
        completed["reservation"]["completion"]["replacement"]["pod_uid"],
        "replacement-pod"
    );
}

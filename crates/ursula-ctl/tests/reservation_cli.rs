//! Actual offline CLI process checks. No Kubernetes or provider operation.
#![expect(
    clippy::unwrap_used,
    clippy::indexing_slicing,
    reason = "integration tests assert by panicking, as clippy.toml allows for unit tests"
)]
#![expect(
    clippy::arithmetic_side_effects,
    reason = "pre-existing arithmetic debt; see Known debt in AGENTS.md"
)]
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

fn epoch_ms() -> u64 {
    u64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis(),
    )
    .unwrap()
}

fn file(
    directory: &std::path::Path,
    name: &str,
    value: &impl serde::Serialize,
) -> std::path::PathBuf {
    let path = directory.join(name);
    std::fs::write(&path, serde_json::to_vec(value).unwrap()).unwrap();
    path
}

fn build_host_request(
    directory: &std::path::Path,
    action: &str,
    arguments: &[(&str, String)],
) -> Value {
    let mut command = Command::new(env!("CARGO_BIN_EXE_ursulactl"));
    command.args(["reservation-request", action]);
    for (name, value) in arguments {
        command.arg(format!("--{name}")).arg(value);
    }
    let result = command.output().unwrap();
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    let request: Value = serde_json::from_slice(&result.stdout).unwrap();
    file(directory, "host-request", &request);
    request
}

fn commit_host_request(directory: &std::path::Path, snapshot: &Value, request: &Value) -> Value {
    file(directory, "host-snapshot", snapshot);
    file(directory, "host-request", request);
    let run = |action: &str, response: bool| {
        let mut command = Command::new(env!("CARGO_BIN_EXE_ursulactl"));
        command.arg(action);
        for (name, path) in [
            ("cell", "cell"),
            ("snapshot", "host-snapshot"),
            ("request", "host-request"),
        ] {
            command.arg(format!("--{name}")).arg(directory.join(path));
        }
        if response {
            command
                .arg("--response")
                .arg(directory.join("host-response"));
        }
        command.output().unwrap()
    };
    let proposed = run("reservation-propose", false);
    assert!(
        proposed.status.success(),
        "{}",
        String::from_utf8_lossy(&proposed.stderr)
    );
    let mut response: Value = serde_json::from_slice(&proposed.stdout).unwrap();
    assert_eq!(response["metadata"]["uid"], snapshot["metadata"]["uid"]);
    // Opaque RV, deliberately nonnumeric; it only must differ after replace.
    response["metadata"]["resourceVersion"] = json!(format!(
        "{}-next",
        snapshot["metadata"]["resourceVersion"].as_str().unwrap()
    ));
    file(directory, "host-response", &response);
    let acknowledged = run("reservation-acknowledge", true);
    assert!(
        acknowledged.status.success(),
        "{}",
        String::from_utf8_lossy(&acknowledged.stderr)
    );
    let receipt: Value = serde_json::from_slice(&acknowledged.stdout).unwrap();
    assert_eq!(receipt["disruption_authorized"], false);
    assert_eq!(receipt["physical_hosts_fenced"], false);
    let saved: Reservation = serde_json::from_value(receipt["reservation"].clone()).unwrap();
    let projections = match saved.operation() {
        None => vec![("operation-kind", "idle"), ("stage", "idle")],
        Some(operation) => vec![
            ("operation-kind", "host-recovery"),
            ("stage", operation.host.as_ref().unwrap().stage()),
            (
                "source-provider-instance",
                operation.source.provider_instance.as_str(),
            ),
            ("source-node-uid", operation.source.node_uid.as_str()),
            (
                "source-node-name",
                operation
                    .host
                    .as_ref()
                    .unwrap()
                    .source_host
                    .node_name
                    .as_str(),
            ),
        ],
    };
    for (field, expected) in projections {
        let result = Command::new(env!("CARGO_BIN_EXE_ursulactl"))
            .args(["reservation-read", "--field", field])
            .arg("--cell")
            .arg(directory.join("cell"))
            .arg("--snapshot")
            .arg(directory.join("host-response"))
            .output()
            .unwrap();
        assert!(
            result.status.success(),
            "{}",
            String::from_utf8_lossy(&result.stderr)
        );
        assert_eq!(String::from_utf8(result.stdout).unwrap().trim(), expected);
    }
    response
}

#[test]
fn healthy_inventory_and_host_recovery_round_trip_through_actual_cli_without_physical_authority() {
    run_host_cli(false);
}

#[test]
fn interrupted_candidate_round_trips_through_actual_cli_and_exact_cas_receipts() {
    run_host_cli(true);
}

fn run_host_cli(interrupt_candidate: bool) {
    let directory = tempfile::tempdir().unwrap();
    let cell = CellIdentity {
        namespace: "test".into(),
        namespace_uid: "namespace-uid".into(),
        statefulset: "voters".into(),
        statefulset_uid: "statefulset-uid".into(),
        group_count: 2,
        core_count: 1,
        voter_ids: [1, 2, 3].into_iter().collect(),
    };
    let now = u64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis(),
    )
    .unwrap();
    let plan = (1_u64..=3)
        .map(|id| NodeInfo {
            id,
            host: format!("voters-{id}"),
            admin_url: format!("http://127.0.0.1:{}", 1000 + id).parse().unwrap(),
            http_url: None,
            metrics_url: None,
            expected_process_incarnation: Some(ProcessIncarnation::from_bits(u128::from(id))),
            expected_maintenance_fence: None,
        })
        .collect::<Vec<_>>();
    let proof = PrefixObservation {
        started_ms: now,
        completed_ms: now,
        verification: ursula_ctl::quorum::QuorumVerification {
            version: 3,
            participation_certified: true,
            process_incarnations_certified: true,
            process_incarnations: plan
                .iter()
                .map(|node| (node.id, node.expected_process_incarnation.clone().unwrap()))
                .collect(),
            maintenance_executor_certified: false,
            maintenance_executor_retired_certified: false,
            maintenance_fence: None,
            prefixes: (0..2)
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
                .map(|id| (id, [(0, 50), (1, 50)].into_iter().collect()))
                .collect(),
        },
    };
    let pods = json!({"kind":"PodList", "items":(1..=3).map(|id|json!({"kind":"Pod", "metadata":{
        "namespace":"test", "name":format!("voters-{}",id-1), "uid":format!("pod-{id}"),
        "ownerReferences":[{"kind":"StatefulSet", "uid":"statefulset-uid", "controller":true}]},
        "spec":{"nodeName":format!("host-{id}")}, "status":{"conditions":[{"type":"Ready", "status":"True"}]}})).collect::<Vec<_>>()});
    let nodes = json!({"kind":"NodeList", "items":(1..=3).map(|id|json!({"kind":"Node", "metadata":{
        "name":format!("host-{id}"), "uid":format!("node-{id}"), "labels":{"topology.kubernetes.io/zone":format!("zone-{id}")}},
        "spec":{"providerID":format!("instance-{id}")}, "status":{"conditions":[{"type":"Ready", "status":"True"}]}})).collect::<Vec<_>>()});
    let snapshot = json!({"apiVersion":"v1", "kind":"ConfigMap", "metadata":{"namespace":"test", "name":"voters-maintenance", "uid":"store-uid", "resourceVersion":"first"}, "data":{"reservation":serde_json::to_string(&Reservation::initial(cell.clone()).unwrap()).unwrap()}});
    for (name, value) in [
        ("cell", serde_json::to_value(&cell).unwrap()),
        ("snapshot", snapshot),
        ("pods", pods),
        ("nodes", nodes),
        ("config", json!({"nodes":plan})),
        ("observation", serde_json::to_value(&proof).unwrap()),
    ] {
        std::fs::write(
            directory.path().join(name),
            serde_json::to_vec(&value).unwrap(),
        )
        .unwrap();
    }
    let builder = Command::new(env!("CARGO_BIN_EXE_ursulactl"))
        .args(["reservation-request", "publish-host-inventory"])
        .arg("--pods")
        .arg(directory.path().join("pods"))
        .arg("--nodes")
        .arg(directory.path().join("nodes"))
        .arg("--config")
        .arg(directory.path().join("config"))
        .arg("--observation")
        .arg(directory.path().join("observation"))
        .output()
        .unwrap();
    assert!(
        builder.status.success(),
        "{}",
        String::from_utf8_lossy(&builder.stderr)
    );
    std::fs::write(directory.path().join("request"), builder.stdout).unwrap();
    let propose = Command::new(env!("CARGO_BIN_EXE_ursulactl"))
        .arg("reservation-propose")
        .arg("--cell")
        .arg(directory.path().join("cell"))
        .arg("--snapshot")
        .arg(directory.path().join("snapshot"))
        .arg("--request")
        .arg(directory.path().join("request"))
        .output()
        .unwrap();
    assert!(
        propose.status.success(),
        "{}",
        String::from_utf8_lossy(&propose.stderr)
    );
    let mut response: Value = serde_json::from_slice(&propose.stdout).unwrap();
    response["metadata"]["resourceVersion"] = json!("second");
    std::fs::write(
        directory.path().join("response"),
        serde_json::to_vec(&response).unwrap(),
    )
    .unwrap();
    let acknowledge = Command::new(env!("CARGO_BIN_EXE_ursulactl"))
        .arg("reservation-acknowledge")
        .arg("--cell")
        .arg(directory.path().join("cell"))
        .arg("--snapshot")
        .arg(directory.path().join("snapshot"))
        .arg("--request")
        .arg(directory.path().join("request"))
        .arg("--response")
        .arg(directory.path().join("response"))
        .output()
        .unwrap();
    assert!(
        acknowledge.status.success(),
        "{}",
        String::from_utf8_lossy(&acknowledge.stderr)
    );
    let receipt: Value = serde_json::from_slice(&acknowledge.stdout).unwrap();
    assert_eq!(receipt["disruption_authorized"], false);
    assert_eq!(receipt["physical_hosts_fenced"], false);
    assert_eq!(receipt["reservation"]["generation"], 0);
    assert!(receipt["reservation"]["operation"].is_null());
    let read = Command::new(env!("CARGO_BIN_EXE_ursulactl"))
        .arg("reservation-read")
        .arg("--cell")
        .arg(directory.path().join("cell"))
        .arg("--snapshot")
        .arg(directory.path().join("response"))
        .args(["--field", "hosts"])
        .output()
        .unwrap();
    assert!(
        read.status.success(),
        "{}",
        String::from_utf8_lossy(&read.stderr)
    );
    assert_eq!(
        serde_json::from_slice::<Value>(&read.stdout).unwrap()["voters"]
            .as_array()
            .unwrap()
            .len(),
        3
    );
    // Continue through the actual offline host builders and whole-CAS receipts.
    // These supplied observations are fixtures, never provider credentials.
    let dir = directory.path();
    let request = build_host_request(dir, "reserve-host-recovery", &[
        ("operation-id", format!("{:032x}", 1)),
        ("executor-id", format!("{:032x}", 10)),
        ("node-id", "1".into()),
        ("config", dir.join("config").display().to_string()),
    ]);
    let mut committed = commit_host_request(dir, &response, &request);
    let reserved: Reservation =
        serde_json::from_str(committed["data"]["reservation"].as_str().unwrap()).unwrap();
    let operation = reserved.operation().unwrap();
    let fence = file(dir, "host-fence", &operation.fence)
        .display()
        .to_string();
    let mut active_proof = proof.clone();
    active_proof.started_ms = epoch_ms();
    active_proof.completed_ms = active_proof.started_ms;
    active_proof.verification.maintenance_executor_certified = true;
    active_proof.verification.maintenance_fence = Some(operation.fence.clone());
    active_proof.verification.applied.remove(&1);
    active_proof.verification.process_incarnations.remove(&1);
    let survivors = ursula_ctl::reservation::SurvivingPrefixObservation {
        started_ms: active_proof.started_ms,
        completed_ms: active_proof.completed_ms,
        verification: ursula_ctl::quorum::SurvivingQuorumVerification {
            excluded_voter_id: 1,
            configured_voter_ids: [1, 2, 3].into_iter().collect(),
            surviving_voter_ids: [2, 3].into_iter().collect(),
            full_redundancy_restored: false,
            verification: active_proof.verification,
        },
    };
    let observation = file(dir, "host-observation", &survivors)
        .display()
        .to_string();
    let request = build_host_request(dir, "admit-host-termination", &[
        ("fence", fence.clone()),
        ("observation", observation.clone()),
    ]);
    committed = commit_host_request(dir, &committed, &request);
    let now = epoch_ms();
    file(
        dir,
        "host-observation",
        &ursula_ctl::reservation::HostTerminationObservation {
            started_ms: now,
            completed_ms: now,
            provider_instance: "instance-1".into(),
            terminal_state: "terminated".into(),
        },
    );
    let request = build_host_request(dir, "record-host-termination", &[
        ("fence", fence.clone()),
        ("observation", observation.clone()),
    ]);
    committed = commit_host_request(dir, &committed, &request);
    let request = build_host_request(dir, "admit-fenced-pod-retirement", &[(
        "fence",
        fence.clone(),
    )]);
    committed = commit_host_request(dir, &committed, &request);
    let mut replacement_plan = operation.process_plan.clone();
    replacement_plan[0].expected_process_incarnation = Some(ProcessIncarnation::from_bits(100));
    file(dir, "host-config", &json!({"nodes":replacement_plan}));
    let pod = file(dir, "host-pod", &json!({"kind":"Pod","metadata":{"namespace":"test","name":"voters-0","uid":"replacement-pod","ownerReferences":[{"kind":"StatefulSet","uid":"statefulset-uid","controller":true}]},"spec":{"nodeName":"replacement-host"}})).display().to_string();
    let node = file(dir, "host-node", &json!({"kind":"Node","metadata":{"name":"replacement-host","uid":"replacement-node","labels":{"topology.kubernetes.io/zone":"zone-1"}},"spec":{"providerID":"replacement-instance"},"status":{"conditions":[{"type":"Ready","status":"True"}]}})).display().to_string();
    let request = build_host_request(dir, "bind-host-replacement", &[
        ("fence", fence.clone()),
        ("pod-object", pod),
        ("node-object", node),
        ("config", dir.join("host-config").display().to_string()),
    ]);
    committed = commit_host_request(dir, &committed, &request);
    let mut final_boot = ProcessIncarnation::from_bits(100);
    if interrupt_candidate {
        let bound: Reservation =
            serde_json::from_str(committed["data"]["reservation"].as_str().unwrap()).unwrap();
        let candidate = file(
            dir,
            "candidate",
            bound.operation().unwrap().replacement.as_ref().unwrap(),
        )
        .display()
        .to_string();
        let mut fresh = survivors.clone();
        fresh.started_ms = epoch_ms();
        fresh.completed_ms = fresh.started_ms;
        file(dir, "host-observation", &fresh);
        let request = build_host_request(dir, "admit-replacement-termination", &[
            ("fence", fence.clone()),
            ("candidate", candidate.clone()),
            ("observation", observation.clone()),
        ]);
        committed = commit_host_request(dir, &committed, &request);
        let now = epoch_ms();
        file(
            dir,
            "host-observation",
            &ursula_ctl::reservation::HostTerminationObservation {
                started_ms: now,
                completed_ms: now,
                provider_instance: "replacement-instance".into(),
                terminal_state: "terminated".into(),
            },
        );
        let request = build_host_request(dir, "record-replacement-termination", &[
            ("fence", fence.clone()),
            ("candidate", candidate.clone()),
            ("observation", observation.clone()),
        ]);
        committed = commit_host_request(dir, &committed, &request);
        let request = build_host_request(dir, "restage-host-replacement", &[
            ("fence", fence.clone()),
            ("candidate", candidate),
        ]);
        committed = commit_host_request(dir, &committed, &request);
        let restaged: Reservation =
            serde_json::from_str(committed["data"]["reservation"].as_str().unwrap()).unwrap();
        assert!(restaged.operation().unwrap().replacement.is_none());
        assert!(
            restaged
                .operation()
                .unwrap()
                .host
                .as_ref()
                .unwrap()
                .pod_retirement_intents
                .contains("replacement-pod")
        );
        let mut plan = restaged.operation().unwrap().process_plan.clone();
        final_boot = ProcessIncarnation::from_bits(101);
        plan[0].expected_process_incarnation = Some(final_boot.clone());
        file(dir, "host-config", &json!({"nodes":plan}));
        file(
            dir,
            "host-pod",
            &json!({"kind":"Pod","metadata":{"namespace":"test","name":"voters-0","uid":"restaged-pod","ownerReferences":[{"kind":"StatefulSet","uid":"statefulset-uid","controller":true}]},"spec":{"nodeName":"restaged-host"}}),
        );
        file(
            dir,
            "host-node",
            &json!({"kind":"Node","metadata":{"name":"restaged-host","uid":"restaged-node","labels":{"topology.kubernetes.io/zone":"zone-1"}},"spec":{"providerID":"restaged-instance"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}),
        );
        let request = build_host_request(dir, "bind-host-replacement", &[
            ("fence", fence.clone()),
            ("pod-object", dir.join("host-pod").display().to_string()),
            ("node-object", dir.join("host-node").display().to_string()),
            ("config", dir.join("host-config").display().to_string()),
        ]);
        committed = commit_host_request(dir, &committed, &request);
    }
    let mut finished = proof;
    finished.started_ms = epoch_ms();
    finished.completed_ms = finished.started_ms;
    finished
        .verification
        .process_incarnations
        .insert(1, final_boot);
    finished.verification.maintenance_fence = Some(operation.fence.clone());
    finished.verification.maintenance_executor_retired_certified = true;
    file(dir, "host-observation", &finished);
    let request = build_host_request(dir, "complete-host-replacement", &[
        ("fence", fence),
        ("observation", observation),
    ]);
    committed = commit_host_request(dir, &committed, &request);
    let final_state: Reservation =
        serde_json::from_str(committed["data"]["reservation"].as_str().unwrap()).unwrap();
    final_state.validate().unwrap();
    assert!(final_state.operation().is_none());
    assert_eq!(final_state.generation(), 1);
    assert_eq!(
        final_state.hosts().unwrap().voters[0]
            .source
            .provider_instance,
        if interrupt_candidate {
            "restaged-instance"
        } else {
            "replacement-instance"
        }
    );
    assert!(
        final_state
            .completion()
            .unwrap()
            .host
            .as_ref()
            .unwrap()
            .pod_retirement_intents
            .contains("pod-1")
    );
}

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
    let view_path = directory.path().join("view-snapshot");
    std::fs::write(&view_path, serde_json::to_vec(&accepted).unwrap()).unwrap();
    let read_field = |field: &str| {
        Command::new(env!("CARGO_BIN_EXE_ursulactl"))
            .arg("reservation-read")
            .arg("--cell")
            .arg(directory.path().join("cell"))
            .arg("--snapshot")
            .arg(&view_path)
            .arg("--field")
            .arg(field)
            .output()
            .unwrap()
    };
    for (field, expected) in [
        ("stage", "reserved"),
        ("source-node-id", "1"),
        ("source-pod-uid", "pod-1"),
    ] {
        let read = read_field(field);
        assert!(
            read.status.success(),
            "{}",
            String::from_utf8_lossy(&read.stderr)
        );
        assert_eq!(String::from_utf8(read.stdout).unwrap().trim(), expected);
    }
    let read = read_field("manifest");
    assert!(read.status.success());
    assert_eq!(
        serde_json::from_slice::<Value>(&read.stdout).unwrap()["nodes"],
        receipt["nodes"]
    );
    assert!(!read_field("replacement-pod-uid").status.success());
    let initial: Value =
        serde_json::from_slice(&std::fs::read(directory.path().join("snapshot")).unwrap()).unwrap();
    std::fs::write(&view_path, serde_json::to_vec(&initial).unwrap()).unwrap();
    assert_eq!(
        String::from_utf8(read_field("stage").stdout)
            .unwrap()
            .trim(),
        "idle"
    );
    assert!(!read_field("manifest").status.success());
    // Real CLI identity capture from synthetic full API objects.
    let namespace_path = directory.path().join("namespace");
    let sts_path = directory.path().join("statefulset");
    let pod_path = directory.path().join("pod");
    let node_path = directory.path().join("node");
    let plan_path = directory.path().join("plan");
    let namespace = json!({"kind":"Namespace", "metadata":{"name":"test", "uid":"namespace-uid"}});
    let mut sts = json!({"kind":"StatefulSet", "metadata":{"namespace":"test", "name":"voters", "uid":"statefulset-uid"}, "spec":{"replicas":3}});
    let pod = json!({"kind":"Pod", "metadata":{"namespace":"test", "name":"voters-0", "uid":"pod-1", "ownerReferences":[{"kind":"StatefulSet", "uid":"statefulset-uid", "controller":true}]}, "spec":{"nodeName":"host-1"}});
    let node = json!({"kind":"Node", "metadata":{"name":"host-1", "uid":"node-1"}, "spec":{"providerID":"instance-1"}});
    for (path, value) in [
        (&namespace_path, &namespace),
        (&sts_path, &sts),
        (&pod_path, &pod),
        (&node_path, &node),
    ] {
        std::fs::write(path, serde_json::to_vec(value).unwrap()).unwrap();
    }
    std::fs::write(
        &plan_path,
        serde_json::to_vec(&json!({"nodes":receipt["nodes"]})).unwrap(),
    )
    .unwrap();
    let capture_cell = || {
        Command::new(env!("CARGO_BIN_EXE_ursulactl"))
            .arg("reservation-cell")
            .arg("--namespace-object")
            .arg(&namespace_path)
            .arg("--statefulset-object")
            .arg(&sts_path)
            .args(["--group-count", "256", "--core-count", "2"])
            .output()
            .unwrap()
    };
    let result = capture_cell();
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    let captured_cell: Value = serde_json::from_slice(&result.stdout).unwrap();
    assert_eq!(
        captured_cell,
        serde_json::from_slice::<Value>(&std::fs::read(directory.path().join("cell")).unwrap())
            .unwrap()
    );
    let result = Command::new(env!("CARGO_BIN_EXE_ursulactl"))
        .arg("reservation-source")
        .arg("--cell")
        .arg(directory.path().join("cell"))
        .arg("--pod-object")
        .arg(&pod_path)
        .arg("--node-object")
        .arg(&node_path)
        .arg("--config")
        .arg(&plan_path)
        .args(["--node-id", "1"])
        .output()
        .unwrap();
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    assert_eq!(
        serde_json::from_slice::<Value>(&result.stdout).unwrap(),
        receipt["reservation"]["operation"]["source"]
    );
    sts["spec"]["replicas"] = json!(2);
    std::fs::write(&sts_path, serde_json::to_vec(&sts).unwrap()).unwrap();
    assert!(!capture_cell().status.success());
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

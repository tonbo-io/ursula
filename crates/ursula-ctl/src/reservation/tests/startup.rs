use serde_json::json;
use ursula_proto::admin::ProcessIncarnation;

use super::cell;
use super::document;
use super::host_publication;
use super::host_recovery::original_intent;
use super::host_recovery::terminated;
use crate::reservation::ConfigMapSnapshot;
use crate::reservation::HostRequest;
use crate::reservation::Reservation;

fn snapshot(state: &Reservation) -> ConfigMapSnapshot {
    let mut raw = document();
    raw["data"]["reservation"] = json!(serde_json::to_string(state).unwrap());
    ConfigMapSnapshot::parse(raw, &cell()).unwrap()
}

#[test]
fn idle_startup_requires_exact_catalogued_physical_pod_and_live_owner() {
    let fixture = host_publication();
    let state = Reservation::initial(cell())
        .unwrap()
        .publish_hosts(fixture.clone())
        .unwrap();
    let before = snapshot(&state);
    assert!(
        before
            .propose_startup(
                1,
                "pod-1",
                &fixture.pods[0],
                &fixture.nodes[0],
                ProcessIncarnation::from_bits(99)
            )
            .unwrap()
            .is_none()
    );
    for case in 0..6 {
        let mut pod = fixture.pods[0].clone();
        let mut node = fixture.nodes[0].clone();
        let mut uid = "pod-1";
        match case {
            0 => uid = "different-downward-uid",
            1 => pod["metadata"]["uid"] = json!("different-pod"),
            2 => node["metadata"]["uid"] = json!("recreated-node"),
            3 => node["spec"]["providerID"] = json!("other-instance"),
            4 => pod["metadata"]["deletionTimestamp"] = json!("deleting"),
            _ => pod["metadata"]["ownerReferences"] = json!([]),
        }
        assert!(
            before
                .propose_startup(1, uid, &pod, &node, ProcessIncarnation::from_bits(99))
                .is_err(),
            "case {case}"
        );
    }
    assert!(
        snapshot(&Reservation::initial(cell()).unwrap())
            .propose_startup(
                1,
                "pod-1",
                &fixture.pods[0],
                &fixture.nodes[0],
                ProcessIncarnation::from_bits(99)
            )
            .is_err()
    );
}

#[test]
fn host_startup_commits_one_boot_before_transport_and_never_refreshes_it() {
    let state = original_intent(&terminated());
    let mut fixture = host_publication();
    let pod = &mut fixture.pods[0];
    pod["metadata"]["uid"] = json!("replacement");
    let node = &mut fixture.nodes[0];
    node["metadata"]["uid"] = json!("replacement-node");
    node["spec"]["providerID"] = json!("replacement-instance");
    let before = snapshot(&state);
    let claim = before
        .propose_startup(
            1,
            "replacement",
            pod,
            node,
            ProcessIncarnation::from_bits(100),
        )
        .unwrap()
        .unwrap();
    let competing = before
        .propose_startup(
            1,
            "replacement",
            pod,
            node,
            ProcessIncarnation::from_bits(101),
        )
        .unwrap()
        .unwrap();
    let mut response = claim.document().clone();
    response["metadata"]["resourceVersion"] = json!("next-rv");
    assert!(before.acknowledge(&competing, response.clone()).is_err());
    let accepted = before.acknowledge(&claim, response).unwrap();
    assert_eq!(
        accepted.state().operation().unwrap().process_plan[0].expected_process_incarnation,
        Some(ProcessIncarnation::from_bits(100))
    );
    assert!(
        accepted
            .propose_startup(
                1,
                "replacement",
                pod,
                node,
                ProcessIncarnation::from_bits(101)
            )
            .is_err()
    );
    let original = host_publication();
    assert!(
        before
            .propose_startup(
                1,
                "pod-1",
                &original.pods[0],
                &original.nodes[0],
                ProcessIncarnation::from_bits(100)
            )
            .is_err()
    );
    assert!(
        before
            .propose_startup(
                2,
                "pod-2",
                &original.pods[1],
                &original.nodes[1],
                ProcessIncarnation::from_bits(200)
            )
            .is_err()
    );
    let no_retirement = snapshot(&terminated());
    assert!(
        no_retirement
            .propose_startup(
                1,
                "replacement",
                pod,
                node,
                ProcessIncarnation::from_bits(100)
            )
            .is_err()
    );
    let poisoned = state
        .recover_host(HostRequest::AdmitFencedPodRetirement {
            fence: state.operation().unwrap().fence.clone(),
            pod: Some(pod.clone()),
            node: Some(original.nodes[0].clone()),
        })
        .unwrap();
    assert!(
        snapshot(&poisoned)
            .propose_startup(
                1,
                "replacement",
                pod,
                node,
                ProcessIncarnation::from_bits(100)
            )
            .is_err()
    );
}

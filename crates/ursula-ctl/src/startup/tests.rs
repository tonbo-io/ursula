#![expect(
    clippy::arithmetic_side_effects,
    reason = "pre-existing arithmetic debt; see Known debt in AGENTS.md"
)]
#![expect(
    clippy::assertions_on_result_states,
    reason = "pre-existing result-state assertion debt; see Known debt in AGENTS.md"
)]
use std::sync::Arc;

use axum::Json;
use axum::Router;
use axum::body::Bytes;
use axum::extract::State;
use axum::http::HeaderMap;
use axum::http::Method;
use axum::http::StatusCode;
use axum::http::Uri;
use axum::response::IntoResponse;
use axum::response::Response;
use serde_json::json;
use tokio::sync::Mutex;

use super::*;

struct Fixture {
    cell: CellIdentity,
    raw: Value,
    pod: Value,
    node: Value,
    mode: &'static str,
    puts: usize,
    pod_reads: usize,
}

async fn handle(
    State(shared): State<Arc<Mutex<Fixture>>>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    assert_eq!(
        headers.get("authorization").unwrap(),
        "Bearer fixture-token"
    );
    let mut db = shared.lock().await;
    if db.mode == "forbidden" {
        return StatusCode::FORBIDDEN.into_response();
    }
    let value = if uri.path().contains("/configmaps/") {
        if method == Method::PUT {
            db.puts += 1;
            if db.mode == "conflict" {
                return StatusCode::CONFLICT.into_response();
            }
            let mut offered: Value = serde_json::from_slice(&body).unwrap();
            assert_eq!(
                offered.pointer("/metadata/resourceVersion"),
                db.raw.pointer("/metadata/resourceVersion")
            );
            assert_eq!(
                offered.pointer("/metadata/uid"),
                db.raw.pointer("/metadata/uid")
            );
            offered["metadata"]["resourceVersion"] = json!("committed-rv");
            db.raw = offered;
            if db.mode == "ambiguous" {
                return StatusCode::INTERNAL_SERVER_ERROR.into_response();
            }
        } else {
            assert_eq!(method, Method::GET);
        }
        let mut response = db.raw.clone();
        if method == Method::PUT && db.mode == "wrong-receipt" {
            response["metadata"]["uid"] = json!("recreated-store");
        }
        response
    } else if uri.path().contains("/statefulsets/") {
        assert_eq!(method, Method::GET);
        json!({"kind":"StatefulSet","metadata":{"name":db.cell.statefulset,"namespace":db.cell.namespace,"uid":db.cell.statefulset_uid},"spec":{"replicas":3}})
    } else if uri.path().contains("/pods/") {
        assert_eq!(method, Method::GET);
        db.pod_reads += 1;
        let mut pod = db.pod.clone();
        if (db.mode == "changed-before" && db.pod_reads >= 2)
            || (db.mode == "changed-after" && db.pod_reads >= 3)
        {
            pod["metadata"]["uid"] = json!("different-current-pod");
        }
        pod
    } else if uri.path().contains("/nodes/") {
        assert_eq!(method, Method::GET);
        let mut node = db.node.clone();
        if db.mode == "wrong-physical-owner" {
            node["spec"]["providerID"] = json!("aws:///zone-1/i-456");
        }
        node
    } else {
        assert_eq!(method, Method::GET);
        assert_eq!(
            uri.path(),
            format!("/api/v1/namespaces/{}", db.cell.namespace)
        );
        json!({"kind":"Namespace","metadata":{"name":db.cell.namespace,"uid":db.cell.namespace_uid}})
    };
    Json(value).into_response()
}

async fn fixture(
    mode: &'static str,
) -> (
    Api,
    StartupIdentity,
    Arc<Mutex<Fixture>>,
    tokio::task::JoinHandle<()>,
) {
    let (mut state, mut captured, mut raw) = if mode == "idle-completed" {
        crate::reservation::tests::completed_startup_fixture()
    } else {
        crate::reservation::tests::startup_fixture()
    };
    let cell = state.cell().clone();
    if mode == "idle" {
        captured.nodes[0]["spec"]["providerID"] = json!("aws:///zone-1/i-123");
        state = crate::reservation::Reservation::initial(cell.clone())
            .unwrap()
            .publish_hosts(captured.clone())
            .unwrap();
    }
    raw["data"]["reservation"] = json!(serde_json::to_string(&state).unwrap());
    let mut pod = captured.pods[0].clone();
    let mut node = captured.nodes[0].clone();
    if !matches!(mode, "idle" | "idle-completed") {
        pod["metadata"]["uid"] = json!("replacement");
        pod["status"]["conditions"][0]["status"] = json!("False");
        node["metadata"]["uid"] = json!("replacement-node");
    }
    node["spec"]["providerID"] = json!("aws:///zone-1/i-123");
    let shared = Arc::new(Mutex::new(Fixture {
        cell: cell.clone(),
        raw,
        pod,
        node,
        mode,
        puts: 0,
        pod_reads: 0,
    }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}/", listener.local_addr().unwrap())
        .parse()
        .unwrap();
    let router = Router::new().fallback(handle).with_state(shared.clone());
    let server = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    let api = Api {
        client: Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .unwrap(),
        endpoint,
        token: "fixture-token".into(),
        local_host: LocalEc2Identity {
            instance_id: "i-123".into(),
        },
    };
    let identity = StartupIdentity {
        namespace: cell.namespace,
        statefulset: cell.statefulset,
        pod_name: "voters-0".into(),
        pod_uid: if mode == "idle" {
            "pod-1"
        } else {
            "replacement"
        }
        .into(),
        node_id: 1,
        group_count: cell.group_count,
        core_count: cell.core_count,
        process_incarnation: ProcessIncarnation::from_bits(if mode == "idle-completed" {
            101
        } else {
            100
        }),
    };
    (api, identity, shared, server)
}

#[tokio::test]
async fn idle_original_owner_starts_closed_without_writing_or_inventing_a_fence() {
    let (api, identity, shared, server) = fixture("idle").await;
    let admission = api.admit(&identity).await.unwrap();
    assert_eq!(admission.process_incarnation, identity.process_incarnation);
    assert_eq!(
        admission.maintenance_fence,
        MaintenanceFenceState::AwaitingReservation
    );
    assert!(!admission.start_maintenance_drained());
    assert_eq!(shared.lock().await.puts, 0);
    server.abort();
}

#[tokio::test]
async fn idle_restart_returns_the_last_persistent_retired_generation() {
    let (api, identity, shared, server) = fixture("idle-completed").await;
    let admission = api.admit(&identity).await.unwrap();
    let db = shared.lock().await;
    let saved = ConfigMapSnapshot::parse(db.raw.clone(), &db.cell).unwrap();
    assert_eq!(
        admission.maintenance_fence,
        MaintenanceFenceState::Retired {
            fence: saved.state().completion().unwrap().fence.clone()
        }
    );
    assert_eq!(admission.process_incarnation, identity.process_incarnation);
    assert!(!admission.start_maintenance_drained());
    assert_eq!(db.puts, 0);
    server.abort();
}

#[tokio::test]
async fn startup_http_claim_commits_one_nonce_and_rejects_a_second_boot() {
    let (api, mut identity, shared, server) = fixture("normal").await;
    let admitted = api.admit(&identity).await.unwrap();
    assert_eq!(admitted.process_incarnation, identity.process_incarnation);
    let db = shared.lock().await;
    let saved = ConfigMapSnapshot::parse(db.raw.clone(), &db.cell).unwrap();
    assert_eq!(
        admitted.maintenance_fence,
        MaintenanceFenceState::Activating {
            fence: saved.state().operation().unwrap().fence.clone()
        }
    );
    assert!(admitted.start_maintenance_drained());
    drop(db);
    identity.process_incarnation = ProcessIncarnation::from_bits(101);
    assert!(api.admit(&identity).await.is_err());
    assert_eq!(shared.lock().await.puts, 1);
    server.abort();
}

#[tokio::test]
async fn failed_conflicting_ambiguous_or_changed_startup_never_returns_permission() {
    for mode in [
        "forbidden",
        "conflict",
        "ambiguous",
        "wrong-receipt",
        "changed-before",
        "changed-after",
        "wrong-physical-owner",
    ] {
        let (api, mut identity, shared, server) = fixture(mode).await;
        assert!(api.admit(&identity).await.is_err(), "{mode}");
        let db = shared.lock().await;
        assert_eq!(
            db.puts,
            usize::from(!matches!(
                mode,
                "forbidden" | "changed-before" | "wrong-physical-owner"
            )),
            "{mode}"
        );
        if matches!(mode, "ambiguous" | "wrong-receipt" | "changed-after") {
            let saved = ConfigMapSnapshot::parse(db.raw.clone(), &db.cell).unwrap();
            assert_eq!(
                saved.state().operation().unwrap().process_plan[0].expected_process_incarnation,
                Some(ProcessIncarnation::from_bits(100))
            );
        }
        drop(db);
        identity.process_incarnation = ProcessIncarnation::from_bits(101);
        if matches!(mode, "ambiguous" | "wrong-receipt" | "changed-after") {
            assert!(api.admit(&identity).await.is_err(), "{mode}");
            assert_eq!(shared.lock().await.puts, 1);
        }
        server.abort();
    }
}

#[test]
fn kernel_identity_is_bounded_and_cannot_be_replaced_by_a_node_label() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("instance-id");
    assert!(LocalEc2Identity::from_nitro_file(&path).is_err());
    for invalid in [
        "",
        "instance-1",
        "i-ABC",
        "i-123/evil",
        &format!("i-123{}", " ".repeat(128)),
    ] {
        std::fs::write(&path, invalid).unwrap();
        assert!(LocalEc2Identity::from_nitro_file(&path).is_err());
    }
    std::fs::write(&path, "i-123\n").unwrap();
    let local = LocalEc2Identity::from_nitro_file(&path).unwrap();
    let mut node = json!({"spec":{"providerID":"aws:///zone-1/i-123"},"metadata":{"labels":{"topology.kubernetes.io/zone":"zone-1"}}});
    local.verify_node(&node).unwrap();
    node["spec"]["providerID"] = json!("aws:///zone-1/i-456");
    assert!(local.verify_node(&node).is_err());
}

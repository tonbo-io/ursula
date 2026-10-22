//! The front door fences an external write's upload to the leadership that
//! admitted it (bounded-stream-state F14j), on a single-voter Raft group
//! whose leader steps down and leads again in a later term while the
//! write's payload is uploading.

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::PoisonError;
use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::http::Request;
use axum::http::StatusCode;
use axum::http::header::CONTENT_TYPE;
use axum::http::header::RETRY_AFTER;
use tokio::sync::Notify;
use tower::ServiceExt;
use ursula_runtime::ColdStoreEvent;
use ursula_runtime::ColdStoreFaultEffect;
use ursula_runtime::ColdStoreOperation;
use ursula_shard::RaftGroupId;

use super::*;

const OCTET: &str = "application/octet-stream";

/// The stream's next offset, as `HEAD` names it.
async fn next_offset(app: &Router) -> String {
    let head = app
        .clone()
        .oneshot(
            Request::head("/fence/s")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(head.status(), StatusCode::OK);
    head.headers()
        .get(ursula_headers::names::STREAM_NEXT_OFFSET)
        .and_then(|value| value.to_str().ok())
        .expect("a next offset")
        .to_owned()
}

/// An external append admitted under one leadership, whose payload finishes
/// uploading only after a re-election of the same node, is refused before
/// anything of it is proposed: it answers `503` with `Retry-After`, the
/// node deletes the payload, and the stream stays empty.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_external_append_whose_leadership_ended_during_its_put_answers_503_and_deletes_its_payload()
 {
    let raft_root = std::env::temp_dir().join(format!(
        "ursula-upload-fence-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("after the epoch")
            .as_nanos()
    ));
    crate::tests::remove_test_path(&raft_root);
    let mut config = ursula_config::UrsulaConfig::default();
    config.runtime.core_count = 1;
    config.raft.group_count = Some(1);
    config.storage.cold.backend = ursula_config::ColdBackend::Memory;
    let spawned = crate::bootstrap::spawn_runtime_with_placement(
        &config,
        crate::tests::single_raft_node(&raft_root, 1),
        ursula_raft::FixedPlacement::single(1),
    )
    .expect("runtime");
    // No meta runs here, so the test settles the fresh root's binding.
    spawned.bind_without_meta();
    let runtime = spawned.runtime;
    let raft_wal = spawned.raft_wal.expect("a durable runtime starts its WAL");
    let registry = spawned.raft_registry.expect("registry");
    runtime.warm_all_groups().await.expect("warm");
    let raft = registry.get(RaftGroupId(0)).expect("group 0");
    raft.wait(Some(Duration::from_secs(5)))
        .current_leader(1, "node 1 leads")
        .await
        .expect("leader");
    let cold_store = runtime.cold_store().expect("a memory cold store");
    let state = HttpState::with_raft_cluster(runtime.clone(), registry.clone())
        .with_external_payload_min_bytes(1);
    let app = router_with_http_state(state);
    let created = app
        .clone()
        .oneshot(
            Request::put("/fence/s")
                .header(CONTENT_TYPE, OCTET)
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(created.status(), StatusCode::CREATED);
    let empty = next_offset(&app).await;

    // The external payload's PUT waits at a gate the test opens.
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    cold_store.set_fault_policy(|context| {
        (context.operation == ColdStoreOperation::WriteChunk && context.path.contains("/external/"))
            .then(|| ColdStoreFaultEffect::delay(Duration::from_millis(1)))
    });
    {
        let entered = entered.clone();
        let release = release.clone();
        cold_store.set_delay_fn(move |_delay| {
            let entered = entered.clone();
            let release = release.clone();
            async move {
                entered.notify_one();
                release.notified().await;
            }
        });
    }
    let seen = Arc::new(Mutex::new((Vec::new(), Vec::new())));
    {
        let seen = seen.clone();
        cold_store.set_observer(move |event| {
            let mut seen = seen.lock().unwrap_or_else(PoisonError::into_inner);
            match event {
                ColdStoreEvent::WriteChunkComplete { path, .. } if path.contains("/external/") => {
                    seen.0.push(path);
                }
                ColdStoreEvent::DeleteChunkComplete { path } => seen.1.push(path),
                _ => {}
            }
        });
    }

    let append = tokio::spawn(
        app.clone().oneshot(
            Request::post("/fence/s")
                .header(CONTENT_TYPE, OCTET)
                .body(Body::from(vec![b'x'; 64]))
                .expect("request"),
        ),
    );
    entered.notified().await;
    // End the leadership that admitted the append: the node steps down to a
    // node outside its single-voter membership and leads again.
    let term = registry
        .leadership(RaftGroupId(0))
        .expect("leadership")
        .term;
    let stepped_down = raft
        .step_down_and_campaign(2)
        .await
        .expect("step down and campaign");
    assert!(matches!(stepped_down, ursula_raft::StepDown::Campaigned));
    raft.wait(Some(Duration::from_secs(5)))
        .metrics(
            |metrics| metrics.current_term > term && metrics.state == openraft::ServerState::Leader,
            "node 1 leads a later term",
        )
        .await
        .expect("a later term");
    release.notify_one();

    let refused = append.await.expect("the append task").expect("response");
    assert_eq!(refused.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert!(refused.headers().contains_key(RETRY_AFTER));
    let (written, deleted) = seen.lock().unwrap_or_else(PoisonError::into_inner).clone();
    assert_eq!(written.len(), 1, "one staged payload: {written:?}");
    assert_eq!(deleted, written, "the refused payload is deleted");
    // Nothing of the refused append applied: the stream stays empty.
    assert_eq!(next_offset(&app).await, empty, "the stream stays empty");

    // A repeat is admitted under the new leadership and applies.
    cold_store.clear_fault_policy();
    let repeated = app
        .clone()
        .oneshot(
            Request::post("/fence/s")
                .header(CONTENT_TYPE, OCTET)
                .body(Body::from(vec![b'x'; 64]))
                .expect("request"),
        )
        .await
        .expect("response");
    assert!(repeated.status().is_success(), "{:?}", repeated.status());
    crate::tests::shutdown_test_wal(&runtime, &raft_wal).await;
    crate::tests::remove_test_path(&raft_root);
}

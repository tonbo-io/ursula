//! `POST /__ursula/usage/streams` and `POST /__ursula/usage/streams/ack`
//! (U-C): admin port only, typed request and answer, and a stream created
//! and deleted between two polls listed as ended.

use axum::Router;
use axum::body::Body;
use axum::http::Request;
use axum::http::StatusCode;
use axum::http::header::CONTENT_TYPE;
use axum::response::Response;
use tower::ServiceExt;
use ursula_proto::admin::GroupStreamUsageReport;
use ursula_proto::admin::SchemaVersion;
use ursula_proto::admin::StreamUsageAck;
use ursula_proto::admin::StreamUsageAckOutcome;
use ursula_proto::admin::StreamUsageAckReport;
use ursula_proto::admin::StreamUsageAckRequest;
use ursula_proto::admin::StreamUsageCursor;
use ursula_proto::admin::StreamUsageEnd;
use ursula_proto::admin::StreamUsageLedgerState;
use ursula_proto::admin::StreamUsageReport;
use ursula_proto::admin::StreamUsageRequest;
use ursula_proto::admin::UsageReport;

use crate::HttpState;
use crate::IngressAdmission;
use crate::Topology;
use crate::admin_router;
use crate::client_router_with_admission;
use crate::spawn_runtime;

fn routers() -> (Router, Router, String) {
    let mut config = ursula_config::UrsulaConfig::default();
    config.runtime.core_count = 1;
    config.raft.group_count = Some(1);
    let runtime = spawn_runtime(&config, Topology::Development { group_count: 1 })
        .expect("runtime")
        .runtime;
    let state = HttpState::new(runtime);
    let incarnation = state.process_incarnation.to_string();
    (
        client_router_with_admission(state.clone(), IngressAdmission::default()),
        admin_router(state),
        incarnation,
    )
}

async fn send(app: &Router, method: &str, uri: &str, body: String) -> Response {
    send_as(app, method, uri, body, None).await
}

/// Sends with `incarnation` in the admin precondition header, if any.
async fn send_as(
    app: &Router,
    method: &str,
    uri: &str,
    body: String,
    incarnation: Option<&str>,
) -> Response {
    let mut request = Request::builder()
        .method(method)
        .uri(uri)
        .header(CONTENT_TYPE, "application/json");
    if let Some(incarnation) = incarnation {
        request = request.header(ursula_proto::admin::PROCESS_INCARNATION_HEADER, incarnation);
    }
    app.clone()
        .oneshot(request.body(Body::from(body)).expect("request"))
        .await
        .expect("response")
}

async fn json<T: serde::de::DeserializeOwned>(response: Response) -> T {
    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    serde_json::from_slice(&body).expect("typed answer")
}

async fn poll(admin: &Router, after_seq: u64) -> GroupStreamUsageReport {
    let request = StreamUsageRequest {
        version: SchemaVersion,
        cursors: vec![StreamUsageCursor {
            raft_group_id: 0,
            after_seq,
        }],
        limit: 100,
    };
    let response = send(
        admin,
        "POST",
        "/__ursula/usage/streams",
        serde_json::to_string(&request).expect("request JSON"),
    )
    .await;
    let mut report: StreamUsageReport = json(response).await;
    assert_eq!(report.write_unit_bytes, 10 * 1024);
    assert_eq!(report.groups.len(), 1);
    report.groups.remove(0)
}

async fn ack(
    (admin, incarnation): (&Router, &str),
    history: u64,
    through_seq: u64,
) -> StreamUsageAckOutcome {
    let request = StreamUsageAckRequest {
        version: SchemaVersion,
        groups: vec![StreamUsageAck {
            raft_group_id: 0,
            history,
            through_seq,
        }],
    };
    let response = send_as(
        admin,
        "POST",
        "/__ursula/usage/streams/ack",
        serde_json::to_string(&request).expect("request JSON"),
        Some(incarnation),
    )
    .await;
    let mut report: StreamUsageAckReport = json(response).await;
    report.groups.remove(0).outcome
}

async fn create(client: &Router, uri: &str, body: &str) {
    let response = send(client, "PUT", uri, body.to_owned()).await;
    assert_eq!(response.status(), StatusCode::CREATED);
}

async fn bucket_totals(app: &Router) -> serde_json::Value {
    let response = send(app, "GET", "/__ursula/usage", String::new()).await;
    let report: UsageReport = json(response).await;
    let (buckets, _complete) = UsageReport::bucket_usage(&[report]);
    serde_json::to_value(buckets).expect("JSON")
}

#[tokio::test]
async fn a_stream_created_and_deleted_between_two_polls_is_exported_as_ended() {
    let (client, admin, incarnation) = routers();
    let acker = (&admin, incarnation.as_str());
    create(&client, "/tenant-a/kept", r#"{"n":1}"#).await;

    // Before the first acknowledgement only bucket totals exist.
    let first = poll(&admin, 0).await;
    assert_eq!(first.ledger, StreamUsageLedgerState::NotStarted);
    assert!(first.complete && first.streams.is_empty());
    assert_eq!(
        ack(acker, first.history, 0).await,
        StreamUsageAckOutcome::Started { usage_seq: 1 }
    );
    let started = poll(&admin, 0).await;
    assert_eq!(started.streams.len(), 1);
    assert_eq!(started.streams[0].stream_id, "kept");
    assert_eq!(started.streams[0].ended, None);
    let cursor = started.through;
    let totals = bucket_totals(&admin).await;

    // Between two polls: a stream comes and goes.
    create(&client, "/tenant-a/brief", r#"{"n":2}"#).await;
    let response = send(&client, "DELETE", "/tenant-a/brief", String::new()).await;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    let second = poll(&admin, cursor).await;
    assert!(second.complete);
    assert_eq!(second.streams.len(), 1);
    let ended = &second.streams[0];
    assert_eq!(
        (ended.bucket_id.as_str(), ended.stream_id.as_str()),
        ("tenant-a", "brief")
    );
    assert_eq!(ended.ended, Some(StreamUsageEnd::Deleted));
    assert_eq!(ended.committed_write_units, 1);
    assert_eq!(ended.committed_records, 1);
    assert_eq!(ended.content_type, "application/json");
    assert!(ended.incarnation > 0);

    // The bucket total counts the ended stream before and after its
    // acknowledgement.
    let with_ended = bucket_totals(&admin).await;
    assert_eq!(
        with_ended["tenant-a"]["committed_write_units"],
        totals["tenant-a"]["committed_write_units"]
            .as_u64()
            .unwrap()
            .saturating_add(1)
    );
    assert_eq!(
        ack(acker, second.history, second.through).await,
        StreamUsageAckOutcome::Acknowledged {
            acked_through: second.through,
            folded: 1
        }
    );
    assert!(poll(&admin, cursor).await.streams.is_empty());
    assert_eq!(bucket_totals(&admin).await, with_ended);

    // An acknowledgement of another history changes nothing.
    assert_eq!(
        ack(acker, second.history.wrapping_add(1), u64::MAX).await,
        StreamUsageAckOutcome::OtherHistory {
            history: second.history
        }
    );
}

#[tokio::test]
async fn the_export_is_served_on_the_admin_listener_only() {
    let (client, admin, _incarnation) = routers();
    let body = serde_json::to_string(&StreamUsageRequest {
        version: SchemaVersion,
        cursors: Vec::new(),
        limit: 10,
    })
    .expect("JSON");
    for path in ["/__ursula/usage/streams", "/__ursula/usage/streams/ack"] {
        let response = send(&client, "POST", path, body.clone()).await;
        assert!(
            response.status().is_client_error(),
            "{path}: {}",
            response.status()
        );
    }
    let response = send(&admin, "POST", "/__ursula/usage/streams", body).await;
    assert_eq!(response.status(), StatusCode::OK);

    // A limit outside 1 to 10,000 is refused.
    for limit in [0, 10_001] {
        let body = serde_json::to_string(&StreamUsageRequest {
            version: SchemaVersion,
            cursors: Vec::new(),
            limit,
        })
        .expect("JSON");
        let response = send(&admin, "POST", "/__ursula/usage/streams", body).await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }
    // The acknowledgement is a mutation: it takes the observed process
    // incarnation, as every admin mutation does.
    let ack_body = r#"{"version":1,"groups":[]}"#.to_owned();
    let response = send(&admin, "POST", "/__ursula/usage/streams/ack", ack_body).await;
    assert_eq!(response.status(), StatusCode::PRECONDITION_REQUIRED);
    // Another schema version is refused.
    let response = send(
        &admin,
        "POST",
        "/__ursula/usage/streams",
        r#"{"version":2,"cursors":[],"limit":10}"#.to_owned(),
    )
    .await;
    assert!(response.status().is_client_error());
}

//! Bounded-state F5 cleanup rule: a staged external payload is deleted only
//! after a definite rejection, never after an ambiguous failure that may
//! follow a committed proposal.

use std::sync::Arc;
use std::sync::Mutex;

use axum::body::Body;
use axum::http::Request;
use tower::ServiceExt;
use ursula_runtime::AppendExternalRequest;
use ursula_runtime::AppendRequest;
use ursula_runtime::CloseStreamRequest;
use ursula_runtime::ColdStore;
use ursula_runtime::ColdStoreEvent;
use ursula_runtime::ColdWriteAdmission;
use ursula_runtime::CreateStreamExternalRequest;
use ursula_runtime::CreateStreamRequest;
use ursula_runtime::DeleteStreamRequest;
use ursula_runtime::GroupAppendFuture;
use ursula_runtime::GroupBucketUsageFuture;
use ursula_runtime::GroupCloseStreamFuture;
use ursula_runtime::GroupCreateStreamFuture;
use ursula_runtime::GroupDeleteStreamFuture;
use ursula_runtime::GroupEngine;
use ursula_runtime::GroupEngineCreateFuture;
use ursula_runtime::GroupEngineError;
use ursula_runtime::GroupEngineFactory;
use ursula_runtime::GroupEngineMetrics;
use ursula_runtime::GroupHeadStreamFuture;
use ursula_runtime::GroupInstallSnapshotFuture;
use ursula_runtime::GroupReadStreamFuture;
use ursula_runtime::GroupSnapshot;
use ursula_runtime::GroupSnapshotFuture;
use ursula_runtime::GroupTouchStreamAccessFuture;
use ursula_runtime::HeadStreamRequest;
use ursula_runtime::InMemoryGroupEngine;
use ursula_runtime::ReadStreamRequest;
use ursula_runtime::RuntimeConfig;
use ursula_runtime::RuntimeError;
use ursula_runtime::ShardRuntime;
use ursula_runtime::StreamErrorCode;
use ursula_shard::BucketStreamId;
use ursula_shard::CoreId;
use ursula_shard::RaftGroupId;
use ursula_shard::ShardPlacement;

use super::*;

/// An in-memory engine whose external appends and creates commit and then
/// lose their response: the failure a client sees after a post-proposal
/// transport error or leader crash, when the write may well have committed.
struct LostResponseEngine {
    inner: InMemoryGroupEngine,
}

impl GroupEngine for LostResponseEngine {
    fn create_stream<'a>(
        &'a mut self,
        request: CreateStreamRequest,
        placement: ShardPlacement,
        admission: ColdWriteAdmission,
    ) -> GroupCreateStreamFuture<'a> {
        self.inner.create_stream(request, placement, admission)
    }

    fn create_stream_external<'a>(
        &'a mut self,
        request: CreateStreamExternalRequest,
        placement: ShardPlacement,
    ) -> GroupCreateStreamFuture<'a> {
        Box::pin(async move {
            self.inner
                .create_stream_external(request, placement)
                .await?;
            Err(GroupEngineError::new(
                "injected: response lost after commit",
            ))
        })
    }

    fn head_stream<'a>(
        &'a mut self,
        request: HeadStreamRequest,
        placement: ShardPlacement,
    ) -> GroupHeadStreamFuture<'a> {
        self.inner.head_stream(request, placement)
    }

    fn bucket_usage<'a>(&'a mut self, placement: ShardPlacement) -> GroupBucketUsageFuture<'a> {
        self.inner.bucket_usage(placement)
    }

    fn read_stream<'a>(
        &'a mut self,
        request: ReadStreamRequest,
        placement: ShardPlacement,
    ) -> GroupReadStreamFuture<'a> {
        self.inner.read_stream(request, placement)
    }

    fn touch_stream_access<'a>(
        &'a mut self,
        stream_id: BucketStreamId,
        now_ms: u64,
        renew_ttl: bool,
        placement: ShardPlacement,
    ) -> GroupTouchStreamAccessFuture<'a> {
        self.inner
            .touch_stream_access(stream_id, now_ms, renew_ttl, placement)
    }

    fn close_stream<'a>(
        &'a mut self,
        request: CloseStreamRequest,
        placement: ShardPlacement,
    ) -> GroupCloseStreamFuture<'a> {
        self.inner.close_stream(request, placement)
    }

    fn delete_stream<'a>(
        &'a mut self,
        request: DeleteStreamRequest,
        placement: ShardPlacement,
    ) -> GroupDeleteStreamFuture<'a> {
        self.inner.delete_stream(request, placement)
    }

    fn append<'a>(
        &'a mut self,
        request: AppendRequest,
        placement: ShardPlacement,
        admission: ColdWriteAdmission,
    ) -> GroupAppendFuture<'a> {
        self.inner.append(request, placement, admission)
    }

    fn append_external<'a>(
        &'a mut self,
        request: AppendExternalRequest,
        placement: ShardPlacement,
    ) -> GroupAppendFuture<'a> {
        Box::pin(async move {
            self.inner.append_external(request, placement).await?;
            Err(GroupEngineError::new(
                "injected: response lost after commit",
            ))
        })
    }

    fn snapshot<'a>(&'a mut self, placement: ShardPlacement) -> GroupSnapshotFuture<'a> {
        self.inner.snapshot(placement)
    }

    fn install_snapshot<'a>(
        &'a mut self,
        snapshot: GroupSnapshot,
    ) -> GroupInstallSnapshotFuture<'a> {
        self.inner.install_snapshot(snapshot)
    }
}

struct LostResponseEngineFactory {
    cold_store: Arc<ColdStore>,
}

impl GroupEngineFactory for LostResponseEngineFactory {
    fn create<'a>(
        &'a self,
        _placement: ShardPlacement,
        _metrics: GroupEngineMetrics,
    ) -> GroupEngineCreateFuture<'a> {
        let inner = InMemoryGroupEngine::with_cold_store(self.cold_store.clone());
        Box::pin(async move {
            let engine: Box<dyn GroupEngine> = Box::new(LostResponseEngine { inner });
            Ok(engine)
        })
    }
}

type Events = Arc<Mutex<Vec<ColdStoreEvent>>>;

fn observe(cold_store: &ColdStore) -> Events {
    let events = Events::default();
    let observed = events.clone();
    cold_store.set_observer(move |event| {
        observed.lock().expect("events lock").push(event);
    });
    events
}

fn staged_external_paths(events: &Events) -> Vec<String> {
    events
        .lock()
        .expect("events lock")
        .iter()
        .filter_map(|event| match event {
            ColdStoreEvent::WriteChunkComplete { path, .. } if path.contains("/external/") => {
                Some(path.clone())
            }
            _ => None,
        })
        .collect()
}

fn deleted_paths(events: &Events) -> Vec<String> {
    events
        .lock()
        .expect("events lock")
        .iter()
        .filter_map(|event| match event {
            ColdStoreEvent::DeleteChunkBegin { path } => Some(path.clone()),
            _ => None,
        })
        .collect()
}

async fn send(
    app: &Router,
    method: &str,
    uri: &str,
    headers: &[(&str, &str)],
    body: Vec<u8>,
) -> Response {
    let mut request = Request::builder().method(method).uri(uri);
    for (name, value) in headers {
        request = request.header(*name, *value);
    }
    app.clone()
        .oneshot(request.body(Body::from(body)).expect("request"))
        .await
        .expect("response")
}

const LARGE: usize = 1024 * 1024;

#[tokio::test]
async fn ambiguous_external_append_failure_keeps_the_staged_object() {
    let cold_store = Arc::new(ColdStore::memory().expect("memory cold store"));
    let runtime = ShardRuntime::spawn_with_engine_factory_and_cold_store(
        RuntimeConfig::new(1, 1),
        LostResponseEngineFactory {
            cold_store: cold_store.clone(),
        },
        Some(cold_store.clone()),
    )
    .expect("runtime");
    let app = router(runtime);
    let response = send(
        &app,
        "PUT",
        "/benchcmp/ambiguous",
        &[("content-type", "application/octet-stream")],
        Vec::new(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::CREATED);

    let events = observe(&cold_store);
    let response = send(
        &app,
        "POST",
        "/benchcmp/ambiguous",
        &[("content-type", "application/octet-stream")],
        vec![b'a'; LARGE],
    )
    .await;
    assert!(response.status().is_server_error(), "{}", response.status());
    let staged = staged_external_paths(&events);
    assert_eq!(staged.len(), 1);
    assert!(deleted_paths(&events).is_empty());

    // The append did commit; its bytes must stay readable.
    let response = send(
        &app,
        "GET",
        &format!("/benchcmp/ambiguous?offset=0&max_bytes={LARGE}"),
        &[],
        Vec::new(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    assert_eq!(body.len(), LARGE);
}

#[tokio::test]
async fn ambiguous_external_create_failure_keeps_the_staged_object() {
    let cold_store = Arc::new(ColdStore::memory().expect("memory cold store"));
    let runtime = ShardRuntime::spawn_with_engine_factory_and_cold_store(
        RuntimeConfig::new(1, 1),
        LostResponseEngineFactory {
            cold_store: cold_store.clone(),
        },
        Some(cold_store.clone()),
    )
    .expect("runtime");
    let app = router(runtime);
    let events = observe(&cold_store);
    let response = send(
        &app,
        "PUT",
        "/benchcmp/ambiguous-create",
        &[("content-type", "application/octet-stream")],
        vec![b'c'; LARGE],
    )
    .await;
    assert!(response.status().is_server_error(), "{}", response.status());
    assert_eq!(staged_external_paths(&events).len(), 1);
    assert!(deleted_paths(&events).is_empty());
}

#[tokio::test]
async fn rejected_external_append_deletes_the_staged_object() {
    let cold_store = Arc::new(ColdStore::memory().expect("memory cold store"));
    let runtime = ShardRuntime::spawn_with_engine_factory_and_cold_store(
        RuntimeConfig::new(1, 1),
        ursula_runtime::InMemoryGroupEngineFactory::with_cold_store(Some(cold_store.clone())),
        Some(cold_store.clone()),
    )
    .expect("runtime");
    let app = router(runtime);
    let response = send(
        &app,
        "PUT",
        "/benchcmp/rejected",
        &[("content-type", "application/octet-stream")],
        Vec::new(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::CREATED);
    let response = send(
        &app,
        "POST",
        "/benchcmp/rejected",
        &[
            ("content-type", "application/octet-stream"),
            ("stream-seq", "5"),
        ],
        b"ab".to_vec(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    let events = observe(&cold_store);
    let response = send(
        &app,
        "POST",
        "/benchcmp/rejected",
        &[
            ("content-type", "application/octet-stream"),
            ("stream-seq", "1"),
        ],
        vec![b'r'; LARGE],
    )
    .await;
    assert_eq!(response.status(), StatusCode::CONFLICT);
    let staged = staged_external_paths(&events);
    assert_eq!(staged.len(), 1);
    assert_eq!(deleted_paths(&events), staged);
}

#[test]
fn cleanup_rule_classifies_runtime_errors() {
    let placement = ShardPlacement {
        core_id: CoreId(0),
        shard_id: ursula_shard::ShardId(0),
        raft_group_id: RaftGroupId(0),
    };
    let engine = |error: GroupEngineError| RuntimeError::GroupEngine {
        core_id: placement.core_id,
        raft_group_id: placement.raft_group_id,
        error,
    };
    assert!(staged_external_definitely_unreferenced(&engine(
        GroupEngineError::stream(StreamErrorCode::StreamSeqConflict, "seq")
    )));
    assert!(!staged_external_definitely_unreferenced(&engine(
        GroupEngineError::new("raft write timed out")
    )));
    assert!(!staged_external_definitely_unreferenced(
        &RuntimeError::ResponseDropped { core_id: CoreId(0) }
    ));
    assert!(!staged_external_definitely_unreferenced(
        &RuntimeError::ColdStoreIo {
            message: "write page".to_owned()
        }
    ));
    // RT1: OpenRaft reports ForwardToLeader from a dropped responder after
    // step-down or log purge, when the entry may already have committed. Only
    // the local pre-proposal leadership check is a definite rejection.
    assert!(!staged_external_definitely_unreferenced(&engine(
        GroupEngineError::forward_to_leader("client_write responder", Some(2), None)
    )));
    assert!(staged_external_definitely_unreferenced(&engine(
        GroupEngineError::forward_to_leader_before_proposal("not leader", Some(2), None)
    )));
    assert!(staged_external_definitely_unreferenced(
        &RuntimeError::GroupNotHosted {
            core_id: CoreId(0),
            raft_group_id: RaftGroupId(0),
        }
    ));
}

/// A deduplicated retry never references its own staged object: the
/// original append committed with another one. The cleanup rule deletes it
/// at once instead of leaving it to the day-long orphan-sweep grace.
#[tokio::test]
async fn deduplicated_external_append_deletes_its_staged_object() {
    let cold_store = Arc::new(ColdStore::memory().expect("memory cold store"));
    let runtime = ShardRuntime::spawn_with_engine_factory_and_cold_store(
        RuntimeConfig::new(1, 1),
        ursula_runtime::InMemoryGroupEngineFactory::with_cold_store(Some(cold_store.clone())),
        Some(cold_store.clone()),
    )
    .expect("runtime");
    let app = router(runtime);
    let response = send(
        &app,
        "PUT",
        "/benchcmp/dedup",
        &[("content-type", "application/octet-stream")],
        Vec::new(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::CREATED);
    let producer = [
        ("content-type", "application/octet-stream"),
        ("producer-id", "writer"),
        ("producer-epoch", "0"),
        ("producer-seq", "0"),
    ];
    let response = send(&app, "POST", "/benchcmp/dedup", &producer, vec![
        b'd';
        LARGE
    ])
    .await;
    assert!(response.status().is_success(), "{}", response.status());

    let events = observe(&cold_store);
    let response = send(&app, "POST", "/benchcmp/dedup", &producer, vec![
        b'd';
        LARGE
    ])
    .await;
    assert!(response.status().is_success(), "{}", response.status());
    let staged = staged_external_paths(&events);
    assert_eq!(staged.len(), 1, "the retry staged its own object");
    assert_eq!(deleted_paths(&events), staged);

    // The original append's bytes stay readable.
    let response = send(
        &app,
        "GET",
        &format!("/benchcmp/dedup?offset=0&max_bytes={LARGE}"),
        &[],
        Vec::new(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    assert_eq!(body.len(), LARGE);
}

/// A create of a live stream never applies its initial payload, so the
/// object it staged is referenced by nothing and is deleted at once.
#[tokio::test]
async fn create_of_a_live_stream_deletes_its_staged_object() {
    let cold_store = Arc::new(ColdStore::memory().expect("memory cold store"));
    let runtime = ShardRuntime::spawn_with_engine_factory_and_cold_store(
        RuntimeConfig::new(1, 1),
        ursula_runtime::InMemoryGroupEngineFactory::with_cold_store(Some(cold_store.clone())),
        Some(cold_store.clone()),
    )
    .expect("runtime");
    let app = router(runtime);
    let headers = [("content-type", "application/octet-stream")];
    let response = send(&app, "PUT", "/benchcmp/live", &headers, vec![b'l'; LARGE]).await;
    assert_eq!(response.status(), StatusCode::CREATED);

    let events = observe(&cold_store);
    let response = send(&app, "PUT", "/benchcmp/live", &headers, vec![b'l'; LARGE]).await;
    assert!(response.status().is_success(), "{}", response.status());
    let staged = staged_external_paths(&events);
    assert_eq!(staged.len(), 1, "the repeated create staged its own object");
    assert_eq!(deleted_paths(&events), staged);
}

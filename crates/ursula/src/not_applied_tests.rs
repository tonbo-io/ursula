//! `x-ursula-not-applied`: ingress admission marks the refusals it makes
//! before a request reaches a handler, a write that proposes one command
//! marks the runtime's refusals raised before that command was proposed, and
//! each marked refusal applied nothing. The refusals left unmarked on purpose
//! are pinned here, and so is the rule that a node never sets the gateway's
//! vouch, `x-ursula-not-applied-checked`, which [`send`] checks on every
//! response. The pressure refusals are pinned with their tests in `tests.rs`,
//! and so are the redirects of a real follower and a real non-voter, with the
//! static gRPC cluster tests. Those tests check the vouch too.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::future::Future;
use std::num::NonZeroU64;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::body::Bytes;
use axum::body::to_bytes;
use axum::http::Request;
use axum::http::StatusCode;
use axum::http::header::CONTENT_LENGTH;
use axum::http::header::LOCATION;
use axum::response::Response;
use tower::ServiceExt;
use ursula_gateway::Gateway;
use ursula_gateway::GatewayConfig;
use ursula_gateway::uploads::UploadLimits;
use ursula_proto::admin::ApplyFailureKind;
use ursula_proto::headers::NOT_APPLIED_CHECKED_HEADER;
use ursula_proto::headers::NOT_APPLIED_HEADER;
use ursula_raft::RaftGroupHandleRegistry;
use ursula_runtime::AdvanceRetentionRequest;
use ursula_runtime::AppendExternalRequest;
use ursula_runtime::AppendRequest;
use ursula_runtime::CloseStreamRequest;
use ursula_runtime::ColdStore;
use ursula_runtime::ColdWriteAdmission;
use ursula_runtime::CreateStreamExternalRequest;
use ursula_runtime::CreateStreamRequest;
use ursula_runtime::DeleteStreamRequest;
use ursula_runtime::GroupAdvanceRetentionFuture;
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
use ursula_runtime::GroupInfraError;
use ursula_runtime::GroupInstallSnapshotFuture;
use ursula_runtime::GroupPublishSnapshotFuture;
use ursula_runtime::GroupReadStreamFuture;
use ursula_runtime::GroupSnapshot;
use ursula_runtime::GroupSnapshotFuture;
use ursula_runtime::GroupTouchStreamAccessFuture;
use ursula_runtime::HeadStreamRequest;
use ursula_runtime::InMemoryGroupEngine;
use ursula_runtime::PublishSnapshotRequest;
use ursula_runtime::ReadStreamRequest;
use ursula_runtime::RuntimeConfig;
use ursula_runtime::ShardRuntime;
use ursula_runtime::StreamErrorCode;
use ursula_shard::BucketStreamId;
use ursula_shard::RaftGroupId;
use ursula_shard::ShardPlacement;

use crate::HEADER_STREAM_CLOSED;
use crate::HEADER_STREAM_NEXT_OFFSET;
use crate::HttpState;
use crate::IngressAdmission;
use crate::MAX_HTTP_BODY_BYTES;
use crate::client_router_with_admission;
use crate::router;
use crate::router_with_static_raft_cluster;
use crate::staging_cleanup_tests::deleted_paths;
use crate::staging_cleanup_tests::observe;
use crate::staging_cleanup_tests::staged_external_paths;

const STREAM: &str = "/benchcmp/committed";
const PEER: &str = "http://127.0.0.1:4437";
const TEXT: (&str, &str) = ("content-type", "text/plain");

/// A node with a cold store stages a body of at least this many bytes there
/// as an external payload before it proposes the write.
const EXTERNAL_PAYLOAD_BYTES: usize = 4;

/// How the test engine fails writes.
#[derive(Clone)]
enum WriteFailure {
    /// Commits every append, then answers it with the error, as a node does
    /// when the response to a committed proposal is lost or overtaken by a
    /// leader change. Every other write passes.
    AppendAfterCommit(GroupEngineError),
    /// Answers every write with the error and proposes nothing. Reads pass.
    Refuse(GroupEngineError),
}

/// The future of a write that [`FailingEngine`] refuses.
type Refused<'a, T> = Pin<Box<dyn Future<Output = Result<T, GroupEngineError>> + Send + 'a>>;

fn refused<'a, T: 'a>(error: GroupEngineError) -> Refused<'a, T> {
    Box::pin(async move { Err(error) })
}

/// An in-memory engine whose writes fail as `failure` says.
struct FailingEngine {
    inner: InMemoryGroupEngine,
    failure: WriteFailure,
}

impl FailingEngine {
    /// The error every write gets, when the engine refuses writes.
    fn refusal(&self) -> Option<GroupEngineError> {
        match &self.failure {
            WriteFailure::Refuse(error) => Some(error.clone()),
            WriteFailure::AppendAfterCommit(_) => None,
        }
    }
}

impl GroupEngine for FailingEngine {
    fn create_stream<'a>(
        &'a mut self,
        request: CreateStreamRequest,
        placement: ShardPlacement,
        admission: ColdWriteAdmission,
    ) -> GroupCreateStreamFuture<'a> {
        match self.refusal() {
            Some(error) => refused(error),
            None => self.inner.create_stream(request, placement, admission),
        }
    }

    fn create_stream_external<'a>(
        &'a mut self,
        request: CreateStreamExternalRequest,
        placement: ShardPlacement,
    ) -> GroupCreateStreamFuture<'a> {
        match self.refusal() {
            Some(error) => refused(error),
            None => self.inner.create_stream_external(request, placement),
        }
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

    fn publish_snapshot<'a>(
        &'a mut self,
        request: PublishSnapshotRequest,
        placement: ShardPlacement,
    ) -> GroupPublishSnapshotFuture<'a> {
        match self.refusal() {
            Some(error) => refused(error),
            None => self.inner.publish_snapshot(request, placement),
        }
    }

    fn advance_retention<'a>(
        &'a mut self,
        request: AdvanceRetentionRequest,
        placement: ShardPlacement,
    ) -> GroupAdvanceRetentionFuture<'a> {
        match self.refusal() {
            Some(error) => refused(error),
            None => self.inner.advance_retention(request, placement),
        }
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
        match self.refusal() {
            Some(error) => refused(error),
            None => self.inner.close_stream(request, placement),
        }
    }

    fn delete_stream<'a>(
        &'a mut self,
        request: DeleteStreamRequest,
        placement: ShardPlacement,
    ) -> GroupDeleteStreamFuture<'a> {
        match self.refusal() {
            Some(error) => refused(error),
            None => self.inner.delete_stream(request, placement),
        }
    }

    fn append<'a>(
        &'a mut self,
        request: AppendRequest,
        placement: ShardPlacement,
        admission: ColdWriteAdmission,
    ) -> GroupAppendFuture<'a> {
        let failure = self.failure.clone();
        Box::pin(async move {
            match failure {
                WriteFailure::AppendAfterCommit(error) => {
                    self.inner.append(request, placement, admission).await?;
                    Err(error)
                }
                WriteFailure::Refuse(error) => Err(error),
            }
        })
    }

    fn append_external<'a>(
        &'a mut self,
        request: AppendExternalRequest,
        placement: ShardPlacement,
    ) -> GroupAppendFuture<'a> {
        match self.refusal() {
            Some(error) => refused(error),
            None => self.inner.append_external(request, placement),
        }
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

struct FailingFactory {
    failure: WriteFailure,
}

impl GroupEngineFactory for FailingFactory {
    fn create<'a>(
        &'a self,
        _placement: ShardPlacement,
        _metrics: GroupEngineMetrics,
    ) -> GroupEngineCreateFuture<'a> {
        let engine: Box<dyn GroupEngine> = Box::new(FailingEngine {
            inner: InMemoryGroupEngine::default(),
            failure: self.failure.clone(),
        });
        Box::pin(async move { Ok(engine) })
    }
}

/// The engines of a node outside every group's voter set: it hosts none.
struct HostsNoGroup;

impl GroupEngineFactory for HostsNoGroup {
    fn hosts_group(&self, _placement: ShardPlacement) -> bool {
        false
    }

    fn create<'a>(
        &'a self,
        placement: ShardPlacement,
        _metrics: GroupEngineMetrics,
    ) -> GroupEngineCreateFuture<'a> {
        Box::pin(async move {
            Err(GroupEngineError::new(format!(
                "group {} is not hosted here",
                placement.raft_group_id.0
            )))
        })
    }
}

/// What a test node knows of other nodes.
#[derive(Clone, Copy)]
enum Peers {
    /// Nothing: the node runs without peers.
    None,
    /// Node 1 at [`PEER`], so a forward to node 1 answers a redirect there.
    Leader,
    /// Node 1 at [`PEER`], the only voter of group 0. The node is node 2, so
    /// a group it does not host is redirected there.
    GroupHost,
}

/// A node over `factory` that knows the nodes `peers` names. With a
/// `cold_store`, it stages a body of at least [`EXTERNAL_PAYLOAD_BYTES`]
/// there before it proposes the write.
fn node(
    factory: impl GroupEngineFactory,
    peers: Peers,
    cold_store: Option<Arc<ColdStore>>,
) -> Router {
    let runtime = ShardRuntime::spawn_with_engine_factory_and_cold_store(
        RuntimeConfig::new(1, 1),
        factory,
        cold_store,
    )
    .expect("runtime");
    let registry = RaftGroupHandleRegistry::default();
    let peer = [(1, PEER.to_owned())];
    let state = match peers {
        Peers::None => HttpState::new(runtime),
        Peers::Leader => HttpState::with_static_raft_cluster(runtime, registry, peer),
        Peers::GroupHost => HttpState::with_static_raft_cluster_topology(
            runtime,
            registry,
            2,
            peer,
            BTreeMap::from([(RaftGroupId(0), BTreeSet::from([1]))]),
        ),
    };
    client_router_with_admission(
        state.with_external_payload_min_bytes(EXTERNAL_PAYLOAD_BYTES),
        IngressAdmission::default(),
    )
}

/// A node whose writes fail as `failure` says.
fn node_failing(failure: WriteFailure, peers: Peers) -> Router {
    node(FailingFactory { failure }, peers, None)
}

/// A [`node`] with a cold store of its own, which it also returns.
fn node_with_cold_store(
    factory: impl GroupEngineFactory,
    peers: Peers,
) -> (Router, Arc<ColdStore>) {
    let cold_store = Arc::new(ColdStore::memory().expect("memory cold store"));
    (node(factory, peers, Some(cold_store.clone())), cold_store)
}

/// A node with the production admission and an in-flight write-body budget
/// of `bytes`.
fn node_with_body_budget(bytes: u64) -> Router {
    let runtime = ShardRuntime::spawn(RuntimeConfig::new(1, 1)).expect("runtime");
    client_router_with_admission(
        HttpState::new(runtime),
        IngressAdmission::new(&ursula_config::ServerConfig {
            http_inflight_body_size: ursula_config::HumanSize::bytes(bytes),
            ..ursula_config::ServerConfig::default()
        }),
    )
}

/// Sends one request to `app`. Every response a test here gets passes
/// through this helper, and none may carry the gateway's vouch.
async fn send(
    app: &Router,
    method: &str,
    uri: &str,
    headers: &[(&str, &str)],
    body: &'static str,
) -> Response {
    let mut request = Request::builder().method(method).uri(uri);
    for (name, value) in headers {
        request = request.header(*name, *value);
    }
    let response = app
        .clone()
        .oneshot(request.body(Body::from(body)).expect("request"))
        .await
        .expect("response");
    assert!(
        !response.headers().contains_key(NOT_APPLIED_CHECKED_HEADER),
        "{method} {uri} answered {}: a node never sets the vouch",
        response.status()
    );
    response
}

async fn create(app: &Router) {
    let created = send(app, "PUT", STREAM, &[TEXT], "").await;
    assert_eq!(created.status(), StatusCode::CREATED);
}

/// Everything the stream holds, read from its start.
async fn stream_content(app: &Router) -> Bytes {
    let read = send(app, "GET", &format!("{STREAM}?offset=-1"), &[], "").await;
    assert_eq!(read.status(), StatusCode::OK);
    to_bytes(read.into_body(), usize::MAX).await.expect("body")
}

fn not_applied(response: &Response) -> Option<&str> {
    response
        .headers()
        .get(NOT_APPLIED_HEADER)
        .map(|value| value.to_str().expect("an ASCII token"))
}

fn location(response: &Response) -> Option<&str> {
    response
        .headers()
        .get(LOCATION)
        .map(|value| value.to_str().expect("an ASCII URL"))
}

/// Admission refuses a body over the route's limit from its `Content-Length`,
/// before reading it, so the `413` applied nothing.
#[tokio::test]
async fn admission_marks_an_oversized_body_as_not_applied() {
    let app = router(ShardRuntime::spawn(RuntimeConfig::new(1, 1)).expect("runtime"));
    create(&app).await;
    let oversized = MAX_HTTP_BODY_BYTES.saturating_add(1).to_string();

    let response = send(
        &app,
        "POST",
        STREAM,
        &[TEXT, (CONTENT_LENGTH.as_str(), &oversized)],
        "abc",
    )
    .await;

    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(not_applied(&response), Some("RequestBodyTooLarge"));
    assert_eq!(stream_content(&app).await, "");
}

/// Admission refuses a body that the in-flight write-body budget has no room
/// for, before the handler reads it, so the `503` applied nothing.
#[tokio::test]
async fn admission_marks_an_exhausted_body_budget_as_not_applied() {
    let app = node_with_body_budget(4);
    create(&app).await;

    let refused = send(&app, "POST", STREAM, &[TEXT], "abcde").await;

    assert_eq!(refused.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(not_applied(&refused), Some("IngressBodyBytesLimitReached"));
    assert_eq!(stream_content(&app).await, "");
    // A body that fits is appended, without the marker.
    let appended = send(&app, "POST", STREAM, &[TEXT], "abcd").await;
    assert_eq!(appended.status(), StatusCode::NO_CONTENT);
    assert_eq!(not_applied(&appended), None);
    assert_eq!(stream_content(&app).await, "abcd");
}

/// An answer that can follow a committed proposal never carries the marker:
/// a client that took it as "applied nothing" and reused the producer
/// sequence for other data would have that data acknowledged as a duplicate
/// of the committed append. That includes the redirect to a known leader:
/// the gateway keeps a leader's marker after a redirect only when the
/// redirect carried one too.
#[tokio::test]
async fn an_answer_that_can_follow_a_proposal_carries_no_marker() {
    let cases = [
        (
            "outcome unknown",
            GroupEngineError::Infra(GroupInfraError::OutcomeUnknown),
            Peers::None,
            StatusCode::SERVICE_UNAVAILABLE,
            None,
        ),
        (
            "apply stopped",
            GroupEngineError::Infra(GroupInfraError::ApplyStopped {
                raft_group_id: RaftGroupId(0),
                term: 1,
                index: 1,
                kind: ApplyFailureKind::Panic,
            }),
            Peers::None,
            StatusCode::SERVICE_UNAVAILABLE,
            None,
        ),
        // OpenRaft reports a forward after `client_write` (RT1). With no
        // known leader the node answers the retryable 503.
        (
            "forward after client_write, leader unknown",
            GroupEngineError::forward_to_leader("client_write responder", None, None),
            Peers::Leader,
            StatusCode::SERVICE_UNAVAILABLE,
            None,
        ),
        // A node without peers names no leader and answers 500.
        (
            "forward after client_write, no peers",
            GroupEngineError::forward_to_leader("client_write responder", None, None),
            Peers::None,
            StatusCode::INTERNAL_SERVER_ERROR,
            None,
        ),
        // With a known leader it redirects there.
        (
            "forward after client_write, leader known",
            GroupEngineError::forward_to_leader("client_write responder", Some(1), None),
            Peers::Leader,
            StatusCode::TEMPORARY_REDIRECT,
            Some(format!("{PEER}{STREAM}")),
        ),
    ];
    for (case, error, peers, status, redirect) in cases {
        let app = node_failing(WriteFailure::AppendAfterCommit(error), peers);
        create(&app).await;

        let appended = send(&app, "POST", STREAM, &[TEXT], "abc").await;

        assert_eq!(appended.status(), status, "{case}");
        assert_eq!(location(&appended), redirect.as_deref(), "{case}");
        assert_eq!(not_applied(&appended), None, "{case}");
        // The append did commit.
        assert_eq!(stream_content(&app).await, "abc", "{case}");
    }

    // Apply refuses a write with a stream error, such as a `Stream-Seq` that
    // is not greater than the stored one, after its command reached the Raft
    // log. So the `409` carries no marker, although the cleanup rule deletes
    // a staged body, because apply refuses the write on every replica alike.
    let (app, cold_store) = node_with_cold_store(
        FailingFactory {
            failure: WriteFailure::Refuse(GroupEngineError::stream(
                StreamErrorCode::StreamSeqConflict,
                "Stream-Seq is not greater than the stored one",
            )),
        },
        Peers::None,
    );
    for write in ONE_COMMAND_WRITES {
        let events = observe(&cold_store);
        let response = send(&app, write.method, REFUSED, write.headers, write.body).await;
        let name = write.name;
        assert_eq!(response.status(), StatusCode::CONFLICT, "{name}");
        assert_eq!(not_applied(&response), None, "{name}");
        let staged = staged_external_paths(&events);
        assert_eq!(staged.len(), usize::from(write.external()), "{name}");
        assert_eq!(deleted_paths(&events), staged, "{name}");
    }
}

/// The stream every refused write below addresses.
const REFUSED: &str = "/benchcmp/refused";

/// A write of [`REFUSED`] that proposes one command for its request.
struct OneCommandWrite {
    name: &'static str,
    method: &'static str,
    headers: &'static [(&'static str, &'static str)],
    body: &'static str,
}

impl OneCommandWrite {
    /// Whether a node with a cold store stages the body there first.
    fn external(&self) -> bool {
        self.body.len() >= EXTERNAL_PAYLOAD_BYTES
    }
}

const ONE_COMMAND_WRITES: [OneCommandWrite; 6] = [
    OneCommandWrite {
        name: "create",
        method: "PUT",
        headers: &[TEXT],
        body: "abc",
    },
    OneCommandWrite {
        name: "create with an external payload",
        method: "PUT",
        headers: &[TEXT],
        body: "abcd",
    },
    OneCommandWrite {
        name: "append",
        method: "POST",
        headers: &[TEXT],
        body: "abc",
    },
    OneCommandWrite {
        name: "append with an external payload",
        method: "POST",
        headers: &[TEXT],
        body: "abcd",
    },
    OneCommandWrite {
        name: "close",
        method: "POST",
        headers: &[(HEADER_STREAM_CLOSED, "true")],
        body: "",
    },
    OneCommandWrite {
        name: "delete",
        method: "DELETE",
        headers: &[],
        body: "",
    },
];

/// Snapshot and retention `PUT`s, which stay unmarked. They need no marker:
/// repeating a publish with the same body, or an advance to the same offset,
/// is harmless, so a client retries an attempt whose outcome is unknown.
/// Their handlers can also propose twice. When apply refused an offset whose
/// JSON message boundary it could not check, they propose again, so a
/// refusal of the second proposal follows a first one that reached the Raft
/// log.
const SNAPSHOT_AND_RETENTION_PUTS: [(&str, &str, &str); 2] = [
    ("snapshot", "/benchcmp/refused/snapshot/0", "sn"),
    ("retention", "/benchcmp/refused/retention/0", ""),
];

/// A write refused before its one command was proposed applied nothing, so
/// its answer carries the marker, a redirect included: a node that found it
/// does not lead the group (`NotLeader`), and a node that does not host the
/// group (`GroupNotHosted`). A write with an external payload staged the
/// body first, which proposes nothing, and the refusal deletes it again. No
/// answer carries the vouch.
#[tokio::test]
async fn a_refusal_before_proposal_marks_every_one_command_write() {
    let refusing = |error| FailingFactory {
        failure: WriteFailure::Refuse(error),
    };
    let not_leader = |leader| {
        refusing(GroupEngineError::forward_to_leader_before_proposal(
            "not the leader",
            leader,
            None,
        ))
    };
    let cases = [
        (
            "leader known",
            node_with_cold_store(not_leader(Some(1)), Peers::Leader),
            StatusCode::TEMPORARY_REDIRECT,
            "NotLeader",
        ),
        (
            "leader unknown",
            node_with_cold_store(not_leader(None), Peers::Leader),
            StatusCode::SERVICE_UNAVAILABLE,
            "NotLeader",
        ),
        // A node without peers names no leader and answers 500, as for any
        // forward. Its leadership check still ran before any proposal.
        (
            "no peers",
            node_with_cold_store(not_leader(None), Peers::None),
            StatusCode::INTERNAL_SERVER_ERROR,
            "NotLeader",
        ),
        (
            "group hosted by a peer",
            node_with_cold_store(HostsNoGroup, Peers::GroupHost),
            StatusCode::TEMPORARY_REDIRECT,
            "GroupNotHosted",
        ),
        (
            "group hosted by no known node",
            node_with_cold_store(HostsNoGroup, Peers::None),
            StatusCode::SERVICE_UNAVAILABLE,
            "GroupNotHosted",
        ),
    ];
    for (case, (app, cold_store), status, token) in cases {
        let redirect =
            (status == StatusCode::TEMPORARY_REDIRECT).then(|| format!("{PEER}{REFUSED}"));
        for write in ONE_COMMAND_WRITES {
            let events = observe(&cold_store);
            let response = send(&app, write.method, REFUSED, write.headers, write.body).await;
            let name = write.name;
            assert_eq!(response.status(), status, "{case}: {name}");
            assert_eq!(location(&response), redirect.as_deref(), "{case}: {name}");
            assert_eq!(not_applied(&response), Some(token), "{case}: {name}");
            let staged = staged_external_paths(&events);
            assert_eq!(
                staged.len(),
                usize::from(write.external()),
                "{case}: {name}"
            );
            assert_eq!(deleted_paths(&events), staged, "{case}: {name}");
        }
        for (name, uri, body) in SNAPSHOT_AND_RETENTION_PUTS {
            let response = send(&app, "PUT", uri, &[], body).await;
            assert_eq!(response.status(), status, "{case}: {name}");
            assert_eq!(not_applied(&response), None, "{case}: {name}");
        }
    }
}

/// The runtime's backpressure refuses a create or an append before it
/// proposes it, so the refused write applied nothing and its `503` carries
/// the marker.
#[tokio::test]
async fn backpressure_refusals_are_marked_not_applied() {
    let uri = "/benchcmp/pressured";
    let created_uri = "/benchcmp/pressured-create";
    for (token, config) in [
        // Two bytes exceed a one-byte limit on the group's uncommitted Raft
        // bytes, and on its hot bytes.
        (
            "RaftUncommittedBackpressure",
            RuntimeConfig::new(1, 1).with_raft_max_uncommitted_bytes_per_group(Some(1)),
        ),
        (
            "ColdBackpressure",
            RuntimeConfig::new(1, 1).with_cold_max_hot_bytes_per_group(Some(1)),
        ),
    ] {
        let app = router(ShardRuntime::spawn(config).expect("runtime"));
        // An empty create adds no bytes and passes.
        let created = send(&app, "PUT", uri, &[TEXT], "").await;
        assert_eq!(created.status(), StatusCode::CREATED, "{token}");
        assert_eq!(not_applied(&created), None, "{token}");

        let appended = send(&app, "POST", uri, &[TEXT], "ab").await;
        let refused_create = send(&app, "PUT", created_uri, &[TEXT], "ab").await;
        for (write, response) in [("append", appended), ("create", refused_create)] {
            assert_eq!(
                response.status(),
                StatusCode::SERVICE_UNAVAILABLE,
                "{token}: {write}"
            );
            assert_eq!(not_applied(&response), Some(token), "{token}: {write}");
        }

        // Neither applied anything.
        let head = send(&app, "HEAD", uri, &[], "").await;
        assert_eq!(head.status(), StatusCode::OK, "{token}");
        assert_eq!(
            head.headers()
                .get(HEADER_STREAM_NEXT_OFFSET)
                .map(|value| value.to_str().expect("an ASCII offset")),
            Some("00000000000000000000"),
            "{token}"
        );
        let head = send(&app, "HEAD", created_uri, &[], "").await;
        assert_eq!(head.status(), StatusCode::NOT_FOUND, "{token}");
    }
}

/// Only a gateway vouches for the marker. A node that set
/// `x-ursula-not-applied-checked` itself would have its marker trusted after
/// redirects that a gateway older than 0.7.3, or a client's HTTP library,
/// follows without checking the redirect.
///
/// [`send`] checks every response in this module for the vouch, among them
/// the runtime refusals and the answers that can follow a proposal above.
/// This test adds a plain create, append and `404`, and the budget refusal
/// and the size refusal. The `WalDiskPressure` and `RaftLogPressure` tests in
/// `tests.rs` check their refusals.
#[tokio::test]
async fn a_node_never_sets_the_vouch() {
    let app = node_with_body_budget(4);
    let oversized = MAX_HTTP_BODY_BYTES.saturating_add(1).to_string();
    let cases = [
        (
            "create",
            send(&app, "PUT", STREAM, &[TEXT], "").await,
            StatusCode::CREATED,
            None,
        ),
        (
            "append",
            send(&app, "POST", STREAM, &[TEXT], "abc").await,
            StatusCode::NO_CONTENT,
            None,
        ),
        (
            "budget refusal",
            send(&app, "POST", STREAM, &[TEXT], "abcde").await,
            StatusCode::SERVICE_UNAVAILABLE,
            Some("IngressBodyBytesLimitReached"),
        ),
        (
            "size refusal",
            send(
                &app,
                "POST",
                STREAM,
                &[TEXT, (CONTENT_LENGTH.as_str(), &oversized)],
                "abc",
            )
            .await,
            StatusCode::PAYLOAD_TOO_LARGE,
            Some("RequestBodyTooLarge"),
        ),
        (
            "missing stream",
            send(&app, "GET", "/benchcmp/missing?offset=-1", &[], "").await,
            StatusCode::NOT_FOUND,
            None,
        ),
    ];

    // Each request got the answer it stands for.
    for (case, response, status, marker) in &cases {
        assert_eq!(response.status(), *status, "{case}");
        assert_eq!(not_applied(response), *marker, "{case}");
    }
}

/// Serves `app` on a fresh loopback port.
async fn serve(app: Router) -> (String, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind a loopback port");
    let url = format!("http://{}", listener.local_addr().expect("local addr"));
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.expect("serve");
    });
    (url, server)
}

/// The part of the gateway's metrics these tests read.
#[derive(serde::Deserialize)]
struct GatewayRedirects {
    leader_redirects: u64,
}

async fn leader_redirects(gateway: &Gateway) -> u64 {
    let request = Request::get("/__ursula/gateway/metrics")
        .body(Body::empty())
        .expect("request");
    let body = to_bytes(gateway.handle(request).await.into_body(), usize::MAX)
        .await
        .expect("metrics body");
    serde_json::from_slice::<GatewayRedirects>(&body)
        .expect("gateway metrics")
        .leader_redirects
}

/// Under sustained writes a leader refuses them at admission while a
/// follower still admits them, and a gateway with both as upstreams sends an
/// attempt to either. Behind a follower's redirect raised before proposal the
/// leader's refusal keeps its marker, so the client gets a marked `503` with
/// the gateway's vouch whichever node the attempt reached first. Behind a
/// redirect that can follow a proposal the gateway drops the marker and
/// still vouches.
#[tokio::test]
async fn a_gateway_keeps_a_leader_refusal_marked_behind_a_redirect_raised_before_proposal() {
    let coordinator = ursula_raft::SnapshotBuildCoordinator::new(1);
    assert_eq!(coordinator.observe_log_bytes(300, 200, 100), Some(true));
    let (leader, leader_server) = serve(client_router_with_admission(
        HttpState::new(ShardRuntime::spawn(RuntimeConfig::new(1, 1)).expect("runtime")),
        IngressAdmission::default().with_raft_log_pressure(Some(coordinator)),
    ))
    .await;
    let cases = [
        (
            "raised before proposal",
            GroupEngineError::forward_to_leader_before_proposal("not leader", Some(2), None),
            Some("RaftLogPressure"),
        ),
        (
            "reported after client_write",
            GroupEngineError::forward_to_leader("client_write responder", Some(2), None),
            None,
        ),
    ];
    for (redirect, error, expected) in cases {
        let (follower, follower_server) = serve(router_with_static_raft_cluster(
            ShardRuntime::spawn_with_engine_factory(RuntimeConfig::new(1, 1), FailingFactory {
                failure: WriteFailure::Refuse(error),
            })
            .expect("runtime"),
            RaftGroupHandleRegistry::default(),
            [(2, leader.clone())],
        ))
        .await;
        let gateway = Gateway::new(GatewayConfig {
            listen: "127.0.0.1:0".parse().expect("listen address"),
            upstreams: vec![follower, leader.clone()],
            response_header_timeout: Duration::from_secs(5),
            connect_timeout: Duration::from_secs(1),
            upstream_tcp_user_timeout_secs: NonZeroU64::new(5).expect("non-zero"),
            uploads: UploadLimits::default(),
            raft_group_count: None,
            cors_allowed_origins: Vec::new(),
            usage_chunk_bytes: None,
        });
        // The gateway picks an upstream at random. Send attempts until one
        // goes through the follower: each does with probability 1/2.
        let mut followed = false;
        for _attempt in 0..64 {
            let redirects = leader_redirects(&gateway).await;
            let request = Request::post("/benchcmp/stream")
                .header(TEXT.0, TEXT.1)
                .body(Body::from("abc"))
                .expect("request");
            let response = gateway.handle(request).await;
            assert_eq!(
                response.status(),
                StatusCode::SERVICE_UNAVAILABLE,
                "{redirect}"
            );
            assert_eq!(
                response
                    .headers()
                    .get(NOT_APPLIED_CHECKED_HEADER)
                    .map(|value| value.as_bytes()),
                Some(&b"1"[..]),
                "{redirect}"
            );
            if leader_redirects(&gateway).await == redirects {
                // The leader's own refusal.
                assert_eq!(
                    not_applied(&response),
                    Some("RaftLogPressure"),
                    "{redirect}"
                );
                continue;
            }
            assert_eq!(not_applied(&response), expected, "{redirect}");
            followed = true;
            break;
        }
        assert!(followed, "{redirect}: no attempt went through the follower");
        follower_server.abort();
    }
    leader_server.abort();
}

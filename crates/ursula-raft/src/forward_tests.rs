//! Leader forwarding against a leader that stopped answering.
//!
//! A host that loses power leaves its peers' TCP connections open: nothing
//! resets them, so a forwarded RPC waits for the kernel's retransmission
//! timeout (about 16 minutes on Linux). [`SilentPeer`] reproduces that on
//! loopback: it accepts every connection and never reads, writes or closes.
//! [`WedgedPeer`] is the other case: a peer whose HTTP/2 stack still answers
//! PINGs but whose calls never complete.

use std::pin::Pin;
use std::time::Instant;

use futures_util::Stream;
use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio::sync::watch;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::Request;
use tonic::Response;
use tonic::Status;
use ursula_runtime::GroupEngineCreateFuture;
use ursula_runtime::GroupEngineMetrics;
use ursula_runtime::LeadershipReason;
use ursula_runtime::LeadershipRefusal;
use ursula_runtime::Proposal;
use ursula_runtime::RuntimeError;

use super::*;
use crate::raft_internal_proto as pb;
use crate::raft_internal_proto::raft_internal_server::RaftInternal;

/// How long a forwarded RPC to a silent leader may take, with slack for a
/// loaded test host: the HTTP/2 keepalive interval plus its timeout.
const SILENT_LEADER_BOUND: Duration = Duration::from_secs(8);

/// A peer whose host lost power: connections complete and then hang.
struct SilentPeer {
    url: String,
    accepted: watch::Receiver<usize>,
    task: tokio::task::JoinHandle<()>,
}

impl SilentPeer {
    async fn bind() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind the silent peer");
        let url = format!(
            "http://{}",
            listener.local_addr().expect("silent peer address")
        );
        let (accepted_tx, accepted) = watch::channel(0);
        let task = tokio::spawn(async move {
            let mut held: Vec<TcpStream> = Vec::new();
            while let Ok((socket, _)) = listener.accept().await {
                held.push(socket);
                accepted_tx.send_replace(held.len());
            }
        });
        Self {
            url,
            accepted,
            task,
        }
    }

    /// Waits until a connection to this peer is held open.
    async fn wait_connected(&mut self) {
        tokio::time::timeout(
            Duration::from_secs(5),
            self.accepted.wait_for(|accepted| *accepted > 0),
        )
        .await
        .expect("the forward connects to the silent leader")
        .expect("silent peer task");
    }
}

impl Drop for SilentPeer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// A three-node in-process cluster whose members all advertise `url`, so
/// any forward from a follower dials it. Returns the engines, the leader's
/// index and a stream created and appended through the leader.
async fn cluster_advertising(
    name: &str,
    url: &str,
) -> (
    Vec<RaftGroupEngine>,
    usize,
    ursula_shard::BucketStreamId,
    Vec<tempfile::TempDir>,
) {
    let (_registry, engines, leader_id, wal_roots) =
        build_three_node_cluster_at(name, None, |_| url.to_owned()).await;
    let leader_index = leader_id
        .checked_sub(1)
        .and_then(|index| usize::try_from(index).ok())
        .expect("node ids start at 1");
    let stream_id = bsid(name);
    create_stream_via_raft(&engines[leader_index], stream_id.clone()).await;
    engines[leader_index]
        .raft
        .client_write(append_command(stream_id.clone(), b"acked"))
        .await
        .expect("append through the leader");
    (engines, leader_index, stream_id, wal_roots)
}

/// Waits until `engine` follows a known leader.
async fn wait_for_leader(engine: &RaftGroupEngine) {
    engine
        .raft
        .wait(Some(Duration::from_secs(5)))
        .metrics(
            |metrics| metrics.current_leader.is_some(),
            "follower learns the leader",
        )
        .await
        .expect("follower learns the leader");
}

#[track_caller]
fn assert_leader_unknown(what: &str, err: &GroupEngineError, url: &str) {
    assert!(
        matches!(
            err,
            GroupEngineError::NotLeader(LeadershipRefusal {
                leader: None,
                reason: LeadershipReason::LeaderUnreachable,
                ..
            })
        ),
        "{what}: expected the retryable leader-unknown answer, got {err:?}"
    );
    assert!(
        !err.to_string().contains(url),
        "{what}: the answer leaks the leader address: {err}"
    );
}

/// The blocker of the 0.7 chaos run: a follower's forwarded read, HEAD and
/// bucket purge to a leader whose host lost power each fail within seconds
/// as leader-unknown (HTTP 503, retry), instead of hanging until the kernel
/// gives up on the connection.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn forwards_to_a_silent_leader_fail_within_the_transport_bound() {
    let peer = SilentPeer::bind().await;
    let (mut engines, leader_index, stream_id, _wal_roots) =
        cluster_advertising("forward-silent-leader", &peer.url).await;
    let follower = &mut engines[(leader_index + 1) % 3];
    wait_for_leader(follower).await;

    let started = Instant::now();
    let read = tokio::time::timeout(
        SILENT_LEADER_BOUND,
        follower.read_stream(
            ReadStreamRequest {
                leader_only: true,
                ..read_req(stream_id.clone(), 64)
            },
            placement(),
        ),
    )
    .await
    .expect("a forwarded read to a silent leader is bounded")
    .expect_err("a silent leader answers nothing");
    assert_leader_unknown("forwarded read", &read, &peer.url);
    assert!(started.elapsed() < SILENT_LEADER_BOUND);
    assert!(
        !crate::forward::has_leader_channel(&peer.url),
        "the failed channel is dropped, so the next forward reconnects"
    );

    let head = tokio::time::timeout(
        SILENT_LEADER_BOUND,
        follower.head_stream(
            HeadStreamRequest {
                stream_id: stream_id.clone(),
                now_ms: 0,
                linearizable: true,
                read_index: None,
            },
            placement(),
        ),
    )
    .await
    .expect("a forwarded HEAD to a silent leader is bounded")
    .expect_err("a silent leader answers nothing");
    assert_leader_unknown("forwarded HEAD", &head, &peer.url);

    let purge = tokio::time::timeout(
        SILENT_LEADER_BOUND,
        follower.purge_bucket(stream_id.bucket_id.clone(), placement()),
    )
    .await
    .expect("a forwarded purge to a silent leader is bounded")
    .expect_err("a silent leader answers nothing");
    assert_leader_unknown("forwarded purge", &purge, &peer.url);
    // The purge reached the leader's socket: it may have committed there.
    assert!(
        matches!(
            purge.not_leader(),
            Some(LeadershipRefusal {
                proposal: Proposal::MayHaveCommitted,
                ..
            })
        ),
        "{purge:?}"
    );

    shutdown_all(&engines).await;
}

/// A peer whose process is wedged: its HTTP/2 stack answers PINGs, so the
/// connection stays up, but no call ever completes.
#[derive(Clone, Copy)]
struct WedgedPeer;

#[tonic::async_trait]
impl RaftInternal for WedgedPeer {
    type AppendStreamStream =
        Pin<Box<dyn Stream<Item = Result<pb::RaftAppendStreamResponse, Status>> + Send>>;

    async fn append(
        &self,
        _request: Request<pb::RaftRpcEnvelopeV1>,
    ) -> Result<Response<pb::RaftRpcAckV1>, Status> {
        std::future::pending().await
    }

    async fn append_stream(
        &self,
        _request: Request<tonic::Streaming<pb::RaftAppendStreamRequest>>,
    ) -> Result<Response<Self::AppendStreamStream>, Status> {
        std::future::pending().await
    }

    async fn vote(
        &self,
        _request: Request<pb::RaftRpcEnvelopeV1>,
    ) -> Result<Response<pb::RaftRpcAckV1>, Status> {
        std::future::pending().await
    }

    async fn full_snapshot(
        &self,
        _request: Request<pb::RaftFullSnapshotRequestV1>,
    ) -> Result<Response<pb::RaftFullSnapshotAckV1>, Status> {
        std::future::pending().await
    }

    async fn group_write(
        &self,
        _request: Request<pb::GroupWriteRequestV1>,
    ) -> Result<Response<pb::GroupWriteResponseV1>, Status> {
        std::future::pending().await
    }

    async fn group_read(
        &self,
        _request: Request<pb::GroupReadRequestV1>,
    ) -> Result<Response<pb::GroupReadResponseV1>, Status> {
        std::future::pending().await
    }

    async fn rejoin_barrier(
        &self,
        _request: Request<pb::RejoinBarrierRequestV1>,
    ) -> Result<Response<pb::RejoinBarrierResponseV1>, Status> {
        std::future::pending().await
    }

    async fn transfer_leader(
        &self,
        _request: Request<pb::RaftTransferLeaderRequestV1>,
    ) -> Result<Response<pb::RaftTransferLeaderAckV1>, Status> {
        std::future::pending().await
    }
}

/// HTTP/2 keepalive cannot end a call to a leader that answers PINGs; the
/// forwarding deadline does, with the same retryable answer.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_forwarded_read_to_a_wedged_leader_ends_at_the_deadline() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind the wedged peer");
    let url = format!(
        "http://{}",
        listener.local_addr().expect("wedged peer address")
    );
    let server = tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(crate::grpc::raft_grpc_server(
                WedgedPeer,
                &crate::peer_cluster::test_peer_cluster(1),
            ))
            .serve_with_incoming(TcpListenerStream::new(listener)),
    );
    let (mut engines, leader_index, stream_id, _wal_roots) =
        cluster_advertising("forward-wedged-leader", &url).await;
    let follower = &mut engines[(leader_index + 1) % 3];
    wait_for_leader(follower).await;

    let started = Instant::now();
    let read = tokio::time::timeout(
        Duration::from_secs(20),
        follower.read_stream(
            ReadStreamRequest {
                leader_only: true,
                ..read_req(stream_id, 64)
            },
            placement(),
        ),
    )
    .await
    .expect("a forwarded read to a wedged leader is bounded")
    .expect_err("a wedged leader answers nothing");
    let elapsed = started.elapsed();
    assert_leader_unknown("forwarded read", &read, &url);
    assert!(
        elapsed >= SILENT_LEADER_BOUND,
        "the PINGs kept the connection up, so the deadline ended the call ({elapsed:?})"
    );
    assert!(!crate::forward::has_leader_channel(&url));

    server.abort();
    shutdown_all(&engines).await;
}

#[track_caller]
fn assert_runtime_leader_unknown(what: &str, err: &RuntimeError, url: &str) {
    match err {
        RuntimeError::GroupEngine { error, .. } => assert_leader_unknown(what, error, url),
        other => panic!("{what}: expected a group engine error, got {other:?}"),
    }
}

/// Hands the runtime one prebuilt engine for its only group.
struct PrebuiltEngine(std::sync::Mutex<Option<RaftGroupEngine>>);

impl GroupEngineFactory for PrebuiltEngine {
    fn create<'a>(
        &'a self,
        _placement: ShardPlacement,
        _metrics: GroupEngineMetrics,
    ) -> GroupEngineCreateFuture<'a> {
        let engine = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        Box::pin(async move {
            let engine = engine.ok_or_else(|| GroupEngineError::new("engine handed out"))?;
            Ok(Box::new(engine) as Box<dyn GroupEngine>)
        })
    }
}

/// The group actor runs one command at a time. A forwarded read waiting on
/// a silent leader must not hold it: in the chaos run every later command of
/// the group (appends after this node won the election, HEADs, state gauges)
/// queued behind one such read for 16 minutes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_forwarded_read_waiting_on_a_silent_leader_does_not_hold_the_group_actor() {
    let mut peer = SilentPeer::bind().await;
    let (mut engines, leader_index, stream_id, _wal_roots) =
        cluster_advertising("forward-silent-actor", &peer.url).await;
    let follower = engines.remove((leader_index + 1) % 3);
    wait_for_leader(&follower).await;
    let runtime = ShardRuntime::spawn_with_engine_factory(
        hosted_config(1, 1),
        PrebuiltEngine(std::sync::Mutex::new(Some(follower))),
    )
    .expect("spawn the runtime over the follower");

    let started = Instant::now();
    let read = tokio::spawn({
        let runtime = runtime.clone();
        let request = ReadStreamRequest {
            leader_only: true,
            ..read_req(stream_id.clone(), 64)
        };
        async move { runtime.read_stream(request).await }
    });
    peer.wait_connected().await;
    let head = tokio::spawn({
        let runtime = runtime.clone();
        let request = HeadStreamRequest {
            stream_id: stream_id.clone(),
            now_ms: 0,
            linearizable: true,
            read_index: None,
        };
        async move { runtime.head_stream(request).await }
    });

    // The read and the HEAD are waiting on the silent leader. The group's
    // next command is answered at once (a follower redirects an append).
    let append = tokio::time::timeout(
        Duration::from_secs(1),
        runtime.append(AppendRequest::from_bytes(
            stream_id.clone(),
            b"next".to_vec(),
        )),
    )
    .await
    .expect("the append does not wait behind the forwarded read")
    .expect_err("a follower does not append");
    assert!(
        append.not_leader().is_some(),
        "a follower redirects the append: {append:?}"
    );
    assert!(
        !read.is_finished() && !head.is_finished(),
        "the forwarded read and HEAD are still waiting on the silent leader"
    );

    let read = tokio::time::timeout(SILENT_LEADER_BOUND, read)
        .await
        .expect("the forwarded read is bounded")
        .expect("read task")
        .expect_err("a silent leader answers nothing");
    assert_runtime_leader_unknown("runtime read", &read, &peer.url);
    let head = tokio::time::timeout(SILENT_LEADER_BOUND, head)
        .await
        .expect("the forwarded HEAD is bounded")
        .expect("HEAD task")
        .expect_err("a silent leader answers nothing");
    assert_runtime_leader_unknown("runtime HEAD", &head, &peer.url);
    assert!(started.elapsed() < SILENT_LEADER_BOUND);

    shutdown_all(&engines).await;
}

/// A follower's forwards to a leader of another cluster (a node replaced at
/// the leader's address with another `server.cluster_id`) are refused
/// before anything runs there. The client gets the retryable 503 of a
/// leader this node cannot use, the purge included, and the follower
/// records the leader under its node id.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn forwards_to_a_leader_of_another_cluster_are_refused_before_they_run() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind the foreign peer");
    let url = format!(
        "http://{}",
        listener.local_addr().expect("foreign peer address")
    );
    let other = ursula_headers::ClusterId::new("other-cluster").unwrap();
    let foreign = crate::peer_cluster::PeerCluster::default();
    foreign.bind(
        crate::peer_cluster::PeerIdentity::new(9, other.clone()),
        std::collections::BTreeMap::new(),
    );
    let server = tokio::spawn(
        tonic::transport::Server::builder()
            // A wedged peer: only the cluster-id check answers.
            .add_service(crate::grpc::raft_grpc_server(WedgedPeer, &foreign))
            .serve_with_incoming(TcpListenerStream::new(listener)),
    );
    let (mut engines, leader_index, stream_id, _wal_roots) =
        cluster_advertising("forward-other-cluster", &url).await;
    let leader_id = u64::try_from(leader_index).unwrap() + 1;
    let follower = &mut engines[(leader_index + 1) % 3];
    wait_for_leader(follower).await;

    let read = follower
        .read_stream(
            ReadStreamRequest {
                leader_only: true,
                ..read_req(stream_id.clone(), 64)
            },
            placement(),
        )
        .await
        .expect_err("a leader of another cluster serves nothing");
    assert!(
        matches!(
            read.not_leader(),
            Some(LeadershipRefusal {
                leader: None,
                proposal: Proposal::NotProposed,
                reason: LeadershipReason::LeaderInOtherCluster,
            })
        ),
        "{read:?}"
    );
    let purge = follower
        .purge_bucket(stream_id.bucket_id.clone(), placement())
        .await
        .expect_err("a leader of another cluster serves nothing");
    // Refused before the body: nothing was proposed there.
    assert!(
        matches!(
            purge.not_leader(),
            Some(LeadershipRefusal {
                leader: None,
                proposal: Proposal::NotProposed,
                reason: LeadershipReason::LeaderInOtherCluster,
            })
        ),
        "{purge:?}"
    );
    let peer_cluster = follower.peer_cluster.as_ref().expect("a gRPC replica");
    assert_eq!(peer_cluster.mismatches(), vec![
        crate::peer_cluster::PeerClusterMismatch {
            node_id: leader_id,
            cluster_id: other,
        }
    ]);
    assert_eq!(peer_cluster.refusals(), 2);
    assert_eq!(foreign.refusals(), 2);

    server.abort();
    shutdown_all(&engines).await;
}

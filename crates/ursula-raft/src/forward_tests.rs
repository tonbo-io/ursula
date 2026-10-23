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

    async fn pre_vote(
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

    async fn genesis_probe(
        &self,
        _request: Request<pb::GenesisProbeRequestV1>,
    ) -> Result<Response<pb::GenesisProbeAckV1>, Status> {
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
        placement: ShardPlacement,
        _metrics: GroupEngineMetrics,
    ) -> GroupEngineCreateFuture<'a> {
        let engine = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        Box::pin(async move {
            // The one engine was handed out: the group runs nowhere else.
            let engine = engine.ok_or(GroupEngineError::Infra(
                GroupInfraError::RaftGroupNotRegistered {
                    group: RaftTarget::Data(placement.raft_group_id),
                },
            ))?;
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

/// A follower whose `raft.peers` does not list the leader forwards nothing
/// to it: it refuses naming the leader in its term, which the client plane
/// answers without a redirect, since it has no address for it either.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_follower_forwards_nothing_to_a_leader_raft_peers_does_not_list() {
    let (mut engines, leader_index, stream_id, _wal_roots) =
        cluster_advertising("forward-unlisted-leader", "http://127.0.0.1:1").await;
    let leader_id = u64::try_from(leader_index).unwrap() + 1;
    let follower_index = (leader_index + 1) % 3;
    let follower_id = u64::try_from(follower_index).unwrap() + 1;
    let follower = &mut engines[follower_index];
    wait_for_leader(follower).await;
    follower
        .peer_cluster
        .as_ref()
        .expect("a gRPC replica")
        .bind(
            crate::peer_cluster::test_identity(follower_id),
            crate::PeerDirectory::new([(
                follower_id,
                crate::PeerEndpoints::shared("http://127.0.0.1:1"),
            )]),
        );
    assert!(follower.leader_route().await.is_none());

    let read = follower
        .read_stream(
            ReadStreamRequest {
                leader_only: true,
                ..read_req(stream_id.clone(), 64)
            },
            placement(),
        )
        .await
        .expect_err("a follower serves no leader-only read");
    assert!(
        matches!(
            read.not_leader(),
            Some(LeadershipRefusal {
                leader: Some(ursula_runtime::KnownLeader { node_id, .. }),
                proposal: Proposal::NotProposed,
                reason: LeadershipReason::Follower,
            }) if *node_id == leader_id
        ),
        "{read:?}"
    );

    shutdown_all(&engines).await;
}

/// A follower's forwards to a leader whose address serves another node of
/// the same cluster now are refused as misdelivered before anything runs
/// there. The client gets the retryable leader-unknown answer with nothing
/// proposed, the purge included, never a forward the leader refused
/// (`ForwardFailed` with `FAILED_PRECONDITION`), and nothing is recorded as
/// a refusal of another cluster.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_forward_to_an_address_that_serves_another_node_is_leader_unknown() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind the other node");
    let url = format!(
        "http://{}",
        listener.local_addr().expect("other node address")
    );
    // Node 9 of the same cluster, hosting no group: it refuses every
    // request for another node as misdelivered.
    let other_node = RaftGroupHandleRegistry::default();
    other_node.peer_cluster().bind(
        crate::peer_cluster::test_identity(9),
        crate::PeerDirectory::default(),
    );
    let server = tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(crate::grpc::raft_grpc_service(other_node.clone()))
            .serve_with_incoming(TcpListenerStream::new(listener)),
    );
    let (mut engines, leader_index, stream_id, _wal_roots) =
        cluster_advertising("forward-other-node", &url).await;
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
        .expect_err("another node serves nothing for the leader");
    assert_leader_unknown("forwarded read", &read, &url);
    assert!(
        matches!(
            read.not_leader(),
            Some(LeadershipRefusal {
                proposal: Proposal::NotProposed,
                ..
            })
        ),
        "{read:?}"
    );
    let purge = follower
        .purge_bucket(stream_id.bucket_id.clone(), placement())
        .await
        .expect_err("another node serves nothing for the leader");
    assert_leader_unknown("forwarded purge", &purge, &url);
    // Refused before the body: nothing was proposed there.
    assert!(
        matches!(
            purge.not_leader(),
            Some(LeadershipRefusal {
                proposal: Proposal::NotProposed,
                ..
            })
        ),
        "{purge:?}"
    );
    for error in [&read, &purge] {
        assert!(
            !matches!(
                error.infra(),
                Some(ursula_runtime::GroupInfraError::ForwardFailed { .. })
            ),
            "a misdelivery is no refused forward: {error:?}"
        );
    }
    let peer_cluster = follower.peer_cluster.as_ref().expect("a gRPC replica");
    assert!(peer_cluster.mismatches().is_empty());
    assert_eq!(peer_cluster.refusals(), 0);
    // Each misdelivery is counted for the leader, and drops the forwarding
    // channel to its address: the next forward connects afresh to the
    // address `raft.peers` lists then.
    let leader_id = u64::try_from(leader_index)
        .ok()
        .and_then(|index| index.checked_add(1))
        .expect("node ids start at 1");
    assert!(
        peer_cluster.misdeliveries(leader_id) >= 2,
        "{}",
        peer_cluster.misdeliveries(leader_id)
    );
    assert!(!crate::forward::has_leader_channel(&url));

    server.abort();
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
        crate::PeerDirectory::default(),
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

/// How each fence outcome of a forward classifies (rules 24): refused
/// before the leader's handler, or not sent, is not proposed; an answer
/// discarded after the handler may have run leaves a write's outcome
/// unknown, so a stale view never turns a commit into a duplicate retry, and
/// so does a write answer the leader's fence stopped after the write ran. A
/// barrier the fence refused or discarded is no answer, asked again.
#[test]
fn fence_outcomes_of_a_forward_say_whether_the_leader_may_have_run_it() {
    use ursula_runtime::ForwardedRpc;
    use ursula_runtime::WriteEffect;

    use crate::incarnation::Discarded;
    use crate::incarnation::Judgement;
    use crate::target::FenceRefusal;
    let address = || "http://10.0.0.2:4437".to_owned();
    let stale = Discarded::Stale {
        judgement: Judgement::Older,
    };
    for (error, effect) in [
        (
            ForwardError::Fenced {
                rpc: ForwardedRpc::GroupWrite,
            },
            WriteEffect::NotProposed,
        ),
        (
            ForwardError::OtherIncarnation {
                rpc: ForwardedRpc::GroupWrite,
                address: address(),
                refusal: FenceRefusal::StaleTarget,
            },
            WriteEffect::NotProposed,
        ),
        (
            ForwardError::Discarded {
                rpc: ForwardedRpc::GroupWrite,
                address: address(),
                discarded: stale,
            },
            WriteEffect::Unknown,
        ),
        (
            ForwardError::Discarded {
                rpc: ForwardedRpc::GroupRead,
                address: address(),
                discarded: stale,
            },
            WriteEffect::NotProposed,
        ),
    ] {
        assert_eq!(error.write_effect(), effect, "{error:?}");
        assert_eq!(
            error.status(),
            ursula_runtime::ErrorStatus::Temporary,
            "{error:?}"
        );
    }

    // The leader fenced while it ran the write: its answer is refused after
    // the handler, never as a fence refusal before it, on both planes.
    let after_write = || {
        crate::grpc::fenced_after_write(crate::peer_cluster::PeerClusterError::Fence {
            node_id: 2,
            refusal: FenceRefusal::Fenced,
        })
    };
    assert_eq!(crate::target::fence_refusal(&after_write(), 2), None);
    assert_eq!(crate::target::refusing_node(&after_write(), 2), None);
    assert!(crate::peer_cluster::refusing_cluster(&after_write()).is_none());
    assert!(crate::forward::is_transport_failure(&after_write()));
    let fenced_after_write = || ForwardError::Transport {
        rpc: ForwardedRpc::GroupWrite,
        address: address(),
        source: after_write(),
    };
    assert_eq!(fenced_after_write().write_effect(), WriteEffect::Unknown);
    assert_eq!(
        crate::meta::MetaForwardError::Transport(fenced_after_write()).write_effect(),
        WriteEffect::Unknown
    );
    assert!(matches!(
        crate::meta::confirm::BarrierFailure::from(
            crate::grpc::RecoveryProbeError::OtherIncarnation
        ),
        crate::meta::confirm::BarrierFailure::Unreachable
    ));
}

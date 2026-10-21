//! The meta plane's seams to another node, over real gRPC and in process,
//! against a meta replica on a real journal: the guard on a forwarded
//! `RegisterNode` and the genesis probe's answers.

use std::num::NonZeroU8;
use std::num::NonZeroU64;
use std::sync::Arc;
use std::time::Duration;

use openraft::rt::WatchReceiver;
use tokio_stream::wrappers::TcpListenerStream;
use ursula_control::HistoryCause;
use ursula_control::samples;
use ursula_proto::placement::GenesisClaim;
use ursula_proto::placement::NodeId;
use ursula_proto::placement::ReplicationFactor;
use ursula_runtime::ErrorStatus;
use ursula_runtime::GroupEngineError;
use ursula_runtime::GroupInfraError;
use ursula_runtime::UnproposableCommand;
use ursula_runtime::WriteEffect;
use ursula_stream::HistoryLeader;

use super::MetaCommand;
use super::MetaForwardError;
use super::genesis::GenesisProbe;
use super::tests::control;
use super::tests::form_alone;
use super::tests::register;
use super::tests::start_wal;
use crate::ForwardError;
use crate::PeerDirectory;
use crate::PeerEndpoints;
use crate::PeerGroupLog;
use crate::RaftGroupHandleRegistry;
use crate::RaftTarget;
use crate::RecoveryTransport;
use crate::grpc::CoreRaftTransport;
use crate::grpc::RaftGrpcService;
use crate::in_process::forward_control_in_process;
use crate::in_process::genesis_probe_in_process;
use crate::peer_cluster::test_identity;
use crate::peer_cluster::test_peer_cluster_with;
use crate::recovery_transport::GrpcRecoveryTransport;
use crate::types::RaftCommand;

/// The meta leader's node id ([`form_alone`] forms node 1's replica).
const LEADER: u64 = 1;
/// The node that forwards to the leader and probes it.
const SENDER: u64 = 3;
/// A node that runs no meta replica.
const NO_META: u64 = 2;
/// A node whose binary has no genesis probe.
const OLD_BINARY: u64 = 4;

/// Serves `registry` over gRPC on a new loopback listener, behind
/// production's cluster check, as node `id`, until the task is aborted.
async fn serve(
    id: u64,
    registry: &RaftGroupHandleRegistry,
) -> (String, tokio::task::JoinHandle<()>) {
    registry
        .peer_cluster()
        .bind(test_identity(id), PeerDirectory::default());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let server = crate::grpc::raft_grpc_server(
        RaftGrpcService::new(registry.clone()),
        registry.peer_cluster(),
    );
    let task = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(server)
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    });
    (endpoint, task)
}

/// A server whose binary has no Raft RPC at all, as one before the genesis
/// probe answers it: `UNIMPLEMENTED`.
async fn serve_old_binary() -> (String, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let task = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_routes(tonic::service::Routes::default())
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    });
    (endpoint, task)
}

/// [`SENDER`]'s recovery transport, with `peers` as its `raft.peers`.
fn sender(peers: Vec<(u64, String)>) -> GrpcRecoveryTransport {
    let peers = PeerDirectory::new(
        peers
            .into_iter()
            .map(|(id, endpoint)| (id, PeerEndpoints::shared(endpoint.as_str()))),
    );
    GrpcRecoveryTransport {
        transport: Arc::new(CoreRaftTransport::new(
            test_identity(SENDER),
            test_peer_cluster_with(SENDER, peers),
        )),
        target: RaftTarget::Meta,
        timeout: Duration::from_secs(5),
    }
}

/// Asserts that `error` is the leader's refusal of the request itself, for
/// good and with nothing proposed, as the guard's refusal reads on either
/// seam.
fn assert_refused_for_good(seam: &str, error: &MetaForwardError) {
    match error {
        MetaForwardError::Transport(ForwardError::Refused { source, .. }) => {
            assert_eq!(source.code(), tonic::Code::InvalidArgument, "{seam}");
        }
        other => panic!("{seam}: the leader refuses the request itself: {other:?}"),
    }
    assert_eq!(error.write_effect(), WriteEffect::NotProposed, "{seam}");
    assert_eq!(error.status(), ErrorStatus::Permanent, "{seam}");
}

/// A node speaks only for itself (rules item 23, D8). Forwarded to the meta
/// leader over gRPC, where the cluster check admitted the sender, a
/// `RegisterNode` for the sender proposes and applies, and one for another
/// node is refused before anything is proposed, for good: not retryable,
/// nothing proposed. The in-process seam answers the same refusal with the
/// same classification. The forwarded write both seams share refuses a
/// meta history event (`NewHistory`) and a stream command (`OtherPlane`)
/// before proposing them too. Without the guard, the registration of node
/// 4 would propose and the log would move.
#[tokio::test]
async fn a_register_node_for_another_node_is_refused_for_good_on_both_seams() {
    let dir = tempfile::tempdir().unwrap();
    let registry = RaftGroupHandleRegistry::default();
    let wal = start_wal(dir.path());
    let replica = form_alone(&wal, &registry).await;
    let (endpoint, server) = serve(LEADER, &registry).await;
    let transport = sender(vec![(LEADER, endpoint)]);
    let last_index = || replica.raft().metrics().borrow_watched().last_log_index;

    let registered = transport
        .forward_control(LEADER, register(SENDER))
        .await
        .expect("the leader answers a registration of the sender");
    assert!(registered.is_ok(), "{registered:?}");
    let before = last_index();
    let grpc = transport
        .forward_control(LEADER, register(4))
        .await
        .expect_err("node 3 does not register node 4");
    assert_refused_for_good("gRPC", &grpc);
    let in_process =
        forward_control_in_process(&registry, LEADER, register(4), NodeId::new(SENDER))
            .await
            .expect_err("node 3 does not register node 4");
    assert_refused_for_good("in process", &in_process);
    assert_eq!(last_index(), before, "nothing was proposed");

    let history = RaftCommand::Meta(MetaCommand::NewHistory {
        history: NonZeroU64::new(0xb2).unwrap(),
        previous: 0,
        kept: 0,
        leader: HistoryLeader {
            term: 1,
            node_id: 1,
        },
        cause: HistoryCause::Genesis,
    });
    let stream = RaftCommand::Stream(ursula_stream::StreamCommand::PurgeBucket {
        bucket_id: "b".to_owned(),
    });
    for (command, refusal) in [
        (history, UnproposableCommand::NewHistory),
        (stream, UnproposableCommand::OtherPlane),
    ] {
        let answers = replica
            .active()
            .write_forwarded(vec![command], NodeId::new(SENDER))
            .await
            .expect("the meta group answers the write");
        assert!(
            matches!(
                answers.as_slice(),
                [Err(GroupEngineError::Infra(GroupInfraError::InvalidRaftCommand { command }))]
                    if *command == refusal
            ),
            "{refusal:?}: {answers:?}"
        );
    }
    let answers = replica
        .active()
        .write_forwarded(vec![control(register(SENDER))], NodeId::new(SENDER))
        .await
        .expect("the control: a registration of the sender is written");
    assert!(matches!(answers.as_slice(), [Ok(_)]), "{answers:?}");
    assert!(last_index() > before, "only the control proposed");

    server.abort();
    replica.shutdown().await.unwrap();
}

/// The genesis probe over gRPC and in process: a node that claims what the
/// prober claims answers its meta replica's answer to the probe vote (a
/// formed replica: initialized), one that claims other values names them,
/// a node without a meta replica gives no answer, and a binary without the
/// genesis probe is not empty.
#[tokio::test]
async fn the_genesis_probe_answers_vote_mismatch_or_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let registry = RaftGroupHandleRegistry::default();
    let wal = start_wal(dir.path());
    let replica = form_alone(&wal, &registry).await;
    let (leader, leader_server) = serve(LEADER, &registry).await;
    let (no_meta, no_meta_server) = serve(NO_META, &RaftGroupHandleRegistry::default()).await;
    let (old_binary, old_server) = serve_old_binary().await;
    let transport = sender(vec![
        (LEADER, leader),
        (NO_META, no_meta),
        (OLD_BINARY, old_binary),
    ]);
    let probe = |claim| GenesisProbe {
        claim,
        vote: crate::rejoin::bootstrap_probe_vote(),
    };
    let same = probe(samples::sample_genesis_claim());
    let other_claim = GenesisClaim {
        replication_factor: ReplicationFactor::new(NonZeroU8::new(1).unwrap()),
        ..samples::sample_genesis_claim()
    };
    let other = probe(other_claim);

    let mismatch = Some(PeerGroupLog::GenesisMismatch {
        claim: samples::sample_genesis_claim(),
    });
    for (probe, expected) in [(&same, Some(PeerGroupLog::Initialized)), (&other, mismatch)] {
        assert_eq!(
            transport.genesis_probe(LEADER, probe).await,
            expected,
            "gRPC"
        );
        assert_eq!(
            genesis_probe_in_process(&registry, probe.clone()).await,
            expected,
            "in process"
        );
    }
    assert_eq!(transport.genesis_probe(NO_META, &same).await, None);
    assert_eq!(
        genesis_probe_in_process(&RaftGroupHandleRegistry::default(), same.clone()).await,
        None
    );
    assert_eq!(
        transport.genesis_probe(OLD_BINARY, &same).await,
        Some(PeerGroupLog::Initialized),
        "a binary without the genesis probe is of another format epoch, not empty"
    );

    leader_server.abort();
    no_meta_server.abort();
    old_server.abort();
    replica.shutdown().await.unwrap();
}

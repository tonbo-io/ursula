//! The meta runtime on a real journal. Formation across nodes, claim
//! mismatches, meta learners, plan restarts and damage on a leader's append
//! or install run end to end, in the meta DST family and the integration
//! tests.

use std::num::NonZeroU8;
use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::Duration;

use ursula_control::ClusterView;
use ursula_control::MetaReplicaStatus;
use ursula_control::MetaRole;
use ursula_control::MoveLimit;
use ursula_control::RegistrationStatus;
use ursula_proto::placement::GenesisClaim;
use ursula_proto::placement::GroupCount;
use ursula_proto::placement::NodeId;
use ursula_proto::placement::NodeSet;
use ursula_proto::placement::ReplicationFactor;
use ursula_proto::placement::ViewStage;
use ursula_proto::placement::VoterSet;

use super::formation::FormationParams;
use super::handle::MetaHandle;
use super::runtime::MetaJournal;
use super::runtime::MetaRuntime;
use super::runtime::MetaRuntimeConfig;
use super::runtime::MetaRuntimeState;
use super::tests::start_wal;
use crate::RaftGroupHandleRegistry;
use crate::RaftTarget;
use crate::grpc::CoreRaftTransport;
use crate::recovery_transport::GrpcRecoveryTransport;

/// A node whose `raft.peers` is itself forms meta at once: its genesis
/// driver initializes the meta group with no peer to ask, the history
/// driver proposes the genesis event, the node registers itself, and as
/// the meta leader with every peer registered it bootstraps the cluster
/// with meta's committed voters and the effective replication factor. A
/// runtime missing any of these tasks never shows the bootstrapped view.
#[tokio::test]
async fn a_single_node_forms_meta_at_once() {
    let dir = tempfile::tempdir().unwrap();
    let wal = start_wal(dir.path());
    let registry = RaftGroupHandleRegistry::default();
    let me = NodeId::new(1);
    let claim = GenesisClaim {
        meta_voters: VoterSet::single(me),
        peers: NodeSet::single(me),
        group_count: GroupCount::new(NonZeroU32::new(4).unwrap()),
        // Above the node count: the Bootstrap takes the effective factor.
        replication_factor: ReplicationFactor::new(NonZeroU8::new(3).unwrap()),
    };
    let metrics = ursula_runtime::RuntimeMetrics::new(1, 1).group_engine_metrics();
    let journal = MetaJournal {
        opened: wal.open_meta(metrics.clone()),
        metrics,
    };
    let transport = GrpcRecoveryTransport {
        transport: Arc::new(CoreRaftTransport::default()),
        target: RaftTarget::Meta,
        timeout: Duration::from_secs(1),
    };
    let runtime = MetaRuntime::start(
        MetaRuntimeConfig {
            node: me.get(),
            claim: claim.clone(),
            formation: FormationParams {
                max_concurrent_moves: MoveLimit::new(2),
            },
            engine: crate::RaftEngineConfig::default(),
            snapshots: None,
            // A single node has no peer to shed.
            wal_disk_sheds: Arc::new(crate::InProcessMetaRegistry::default()),
        },
        journal,
        wal.clone(),
        registry.clone(),
        crate::SingleNodeRaftNetworkFactory,
        transport.clone(),
    )
    .await;
    assert_eq!(runtime.state(), MetaRuntimeState::Running);

    let mut watch = registry.placement_watch();
    let view = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let view = watch.current();
            if view.cluster().is_some() {
                return view;
            }
            assert!(watch.changed().await, "the publisher outlives the test");
        }
    })
    .await
    .expect("the cluster bootstraps");
    let cluster = view.cluster().unwrap();
    assert_eq!(cluster.params().meta_voters, VoterSet::single(me));
    assert_eq!(cluster.params().group_count, claim.group_count);
    assert_eq!(cluster.params().replication_factor.get(), 1);
    assert_eq!(cluster.params().max_concurrent_moves, MoveLimit::new(2));
    assert_eq!(cluster.nodes().keys().copied().collect::<Vec<_>>(), vec![
        me
    ]);
    assert_eq!(cluster.groups().len(), 4);
    let ViewStage::Applied { history, applied } = view.stage else {
        panic!("a bootstrapped view applied its genesis");
    };
    assert!(history < applied, "the genesis came before the Bootstrap");
    // The status shows the bootstrapped cluster, this node the meta voter
    // that leads, and its registration answered.
    let status = runtime.status_reader().placement_status();
    assert_eq!(status.node, me);
    assert_eq!(status.view, ClusterView::Bootstrapped(cluster.clone()));
    assert_eq!(status.applied, Some(applied));
    assert_eq!(status.meta, MetaReplicaStatus::Running {
        role: MetaRole::Voter,
        leader: Some(me),
    });
    assert!(matches!(
        status.registration,
        RegistrationStatus::Answered(_)
    ));
    assert_eq!(status.last_bootstrap_refusal, None);
    assert!(runtime.status().genesis.borrow().unanswered.is_empty());
    // Running meta marks no shed.
    assert!(
        !registry
            .leadership_shed_state()
            .contains(crate::LeadershipShedState::PLACEMENT_UNAVAILABLE)
    );

    // The leader confirms its own view.
    let confirmed = MetaHandle::new(me, registry.clone(), transport, &claim)
        .confirm_view()
        .await
        .unwrap();
    assert_eq!(confirmed.stamp().history, history);
    let confirmed = runtime
        .status_reader()
        .confirmed_placement_status()
        .await
        .unwrap();
    assert_eq!(confirmed.view, status.view);

    runtime.shutdown().await.unwrap();
    assert!(registry.meta().is_none());
    wal.shutdown().await.unwrap();
}

//! Local installed-fence regressions and opt-in admission measurement.

use std::collections::BTreeMap;
use std::sync::Arc;

use ursula_proto::admin::ProcessIncarnation;
use ursula_proto::admin::ReplicaIdentity;
use ursula_shard::CoreId;
use ursula_shard::RaftGroupId;
use ursula_shard::ShardId;
use ursula_shard::ShardPlacement;

use crate::grpc::FencedProcess;
use crate::grpc::validate_process_fence;

fn identity(generation: u64, bits: u128) -> ReplicaIdentity {
    ReplicaIdentity {
        generation,
        incarnation: ProcessIncarnation::from_bits(bits),
    }
}

async fn fixture() -> (
    tempfile::TempDir,
    crate::RaftGroupEngine,
    crate::RaftGroupHandleRegistry,
) {
    let root = tempfile::tempdir().expect("valid test fixture");
    let placement = ShardPlacement {
        core_id: CoreId(0),
        shard_id: ShardId(0),
        raft_group_id: RaftGroupId(0),
    };
    let wal = crate::log_store::RaftWal::start(
        root.path(),
        ursula_config::WalFsync::Always,
        &ursula_shard::StaticShardMap::new(1, 1).expect("valid test fixture"),
    )
    .expect("valid test fixture");
    let registry = crate::RaftGroupHandleRegistry::default();
    registry.set_replica_authority(1, identity(1, 1));
    registry.set_replica_genesis(BTreeMap::from([(1, identity(1, 1)), (2, identity(1, 2))]));
    let engine = crate::RaftGroupEngine::new_single_node(
        placement,
        1,
        openraft::BasicNode::new("local"),
        Arc::new(
            openraft::Config {
                heartbeat_interval: 10,
                election_timeout_min: 30,
                election_timeout_max: 60,
                ..Default::default()
            }
            .validate()
            .expect("valid test fixture"),
        ),
        wal.open(
            placement,
            ursula_runtime::RuntimeMetrics::new(1, 1).group_engine_metrics(),
        )
        .expect("valid test fixture"),
        crate::RaftGroupEngineOptions {
            process_authority: Some(registry.clone()),
            snapshot_metadata_path: Some(root.path().join("group-0.snapshot.json")),
            ..Default::default()
        },
    )
    .await
    .expect("valid test fixture");
    registry.register_engine(&engine, None);
    (root, engine, registry)
}

#[tokio::test]
async fn installed_replica_fence_rejects_old_identity_without_meta_reads() {
    let (root, engine, registry) = fixture().await;
    let sender = |identity| {
        crate::codec::encode_wire(&FencedProcess {
            node_id: 2,
            identity,
        })
    };
    let old = sender(identity(1, 2));
    validate_process_fence(&registry, 0, &old).await.unwrap();
    let higher = sender(identity(2, 3));
    assert_eq!(
        validate_process_fence(&registry, 0, &higher)
            .await
            .unwrap_err()
            .code(),
        tonic::Code::FailedPrecondition
    );
    let index = registry
        .install_replica_identity(RaftGroupId(0), 2, Some(identity(1, 2)), identity(2, 3))
        .await
        .unwrap();
    assert!(index > 0);
    assert_eq!(
        validate_process_fence(&registry, 0, &old)
            .await
            .unwrap_err()
            .code(),
        tonic::Code::FailedPrecondition
    );
    validate_process_fence(&registry, 0, &higher).await.unwrap();
    // Retrying the same committed replacement is idempotent even with the old CAS expectation.
    registry
        .install_replica_identity(RaftGroupId(0), 2, Some(identity(1, 2)), identity(2, 3))
        .await
        .unwrap();
    let restored = crate::replica_fence::ReplicaFences::load(Some(
        &root.path().join("group-0.snapshot.replicas.json"),
    ))
    .unwrap();
    assert!(restored.accepts(2, &identity(2, 3)));
    engine.shutdown().await.unwrap();
}

#[tokio::test]
async fn initialized_wal_with_missing_fence_map_cannot_seed_from_meta() {
    let (root, engine, registry) = fixture().await;
    engine.shutdown().await.unwrap();
    drop(engine);
    drop(registry);
    std::fs::remove_file(root.path().join("group-0.snapshot.replicas.json")).unwrap();
    let placement = ShardPlacement {
        core_id: CoreId(0),
        shard_id: ShardId(0),
        raft_group_id: RaftGroupId(0),
    };
    let wal = crate::log_store::RaftWal::start(
        root.path(),
        ursula_config::WalFsync::Always,
        &ursula_shard::StaticShardMap::new(1, 1).unwrap(),
    )
    .unwrap();
    let registry = crate::RaftGroupHandleRegistry::default();
    registry.set_replica_authority(1, identity(1, 1));
    registry.set_replica_genesis(BTreeMap::from([(1, identity(1, 1)), (4, identity(1, 4))]));
    let result = crate::RaftGroupEngine::new_single_node(
        placement,
        1,
        openraft::BasicNode::new("local"),
        Arc::new(openraft::Config::default().validate().unwrap()),
        wal.open(
            placement,
            ursula_runtime::RuntimeMetrics::new(1, 1).group_engine_metrics(),
        )
        .unwrap(),
        crate::RaftGroupEngineOptions {
            process_authority: Some(registry),
            snapshot_metadata_path: Some(root.path().join("group-0.snapshot.json")),
            ..Default::default()
        },
    )
    .await;
    assert!(matches!(
        result,
        Err(ursula_runtime::GroupEngineError::Infra(
            ursula_runtime::GroupInfraError::ReplicaFenceMissingMetadata {
                group: RaftGroupId(0)
            }
        ))
    ));
    assert!(!root.path().join("group-0.snapshot.replicas.json").exists());
    wal.shutdown().await.unwrap();
}

#[tokio::test]
async fn ordinary_writes_cannot_install_replica_identity() {
    let (_root, engine, registry) = fixture().await;
    let command = ursula_runtime::GroupWriteCommand::InstallReplicaIdentity {
        node_id: 2,
        expected: Some(identity(1, 2)),
        replacement: identity(2, 3),
    };
    let direct = engine.write(command.clone()).await.unwrap_err();
    assert!(matches!(
        direct,
        ursula_runtime::GroupEngineError::Infra(
            ursula_runtime::GroupInfraError::ReplicaFenceRequiresRaft
        )
    ));
    let forwarded = crate::forward::write_commands_on_raft(engine.raft.clone(), vec![command])
        .await
        .unwrap_err();
    assert!(matches!(
        forwarded,
        ursula_runtime::GroupEngineError::Infra(
            ursula_runtime::GroupInfraError::ReplicaFenceRequiresRaft
        )
    ));
    assert_eq!(
        registry.installed_replica_identity(RaftGroupId(0), 2),
        Some(identity(1, 2))
    );
    engine.shutdown().await.unwrap();
}

#[tokio::test]
#[ignore = "opt-in local admission CPU measurement; no network latency claim"]
async fn measure_installed_replica_fence() {
    let (_root, engine, registry) = fixture().await;
    let sender = crate::codec::encode_wire(&FencedProcess {
        node_id: 2,
        identity: identity(1, 2),
    });
    let started = crate::rt::time::Instant::now();
    for _ in 0..32 {
        for result in futures_util::future::join_all(
            (0..256).map(|_| validate_process_fence(&registry, 0, &sender)),
        )
        .await
        {
            result.unwrap();
        }
    }
    #[derive(serde::Serialize)]
    struct Measurement {
        checks: usize,
        concurrency: usize,
        elapsed_micros: u128,
    }
    let report = Measurement {
        checks: 8192,
        concurrency: 256,
        elapsed_micros: started.elapsed().as_micros(),
    };
    std::fs::write(
        std::env::var("URSULA_FENCE_MEASURE_REPORT").expect("report path"),
        serde_json::to_vec_pretty(&report).unwrap(),
    )
    .unwrap();
    engine.shutdown().await.unwrap();
}

#[tokio::test]
async fn installed_data_group_keeps_writes_and_reads_after_meta_shutdown() {
    let (_root, engine, registry) = fixture().await;
    let meta_root = tempfile::tempdir().unwrap();
    let meta = crate::MetaRaftHandle::new_durable(
        1,
        meta_root.path().to_owned(),
        Arc::new(openraft::Config::default()),
    )
    .await
    .unwrap();
    registry.set_process_authority(
        1,
        ursula_control::ProcessIdentity {
            epoch: 9,
            incarnation: ProcessIncarnation::from_bits(99),
        },
        meta.clone(),
    );
    meta.shutdown().await.unwrap();
    let sender = crate::codec::encode_wire(&FencedProcess {
        node_id: 2,
        identity: identity(1, 2),
    });
    validate_process_fence(&registry, 0, &sender).await.unwrap();
    engine
        .write(ursula_runtime::GroupWriteCommand::Stream(
            ursula_stream::StreamCommand::CreateStream {
                stream_id: ursula_shard::BucketStreamId::new("meta-outage", "events"),
                content_type: "application/octet-stream".to_owned(),
                initial_payload: bytes::Bytes::from_static(b"survives-meta-outage"),
                close_after: false,
                stream_seq: None,
                producer: None,
                stream_ttl_seconds: None,
                stream_expires_at_ms: None,
                now_ms: 1,
            },
        ))
        .await
        .unwrap();
    engine.read_barrier.round().await.unwrap();
    // A still-known identity removed from voting membership cannot disturb terms.
    use crate::raft_internal_proto::raft_internal_server::RaftInternal;
    let service = crate::grpc::RaftGrpcService::new(registry.clone());
    let request = crate::UrsulaVoteRequest::new(crate::UrsulaVote::new(999, 2), None);
    let rejected = service
        .vote(tonic::Request::new(
            crate::raft_internal_proto::RaftRpcEnvelopeV1 {
                protocol_version: crate::grpc::RAFT_GRPC_PROTOCOL_VERSION,
                node_id: 1,
                raft_group_id: 0,
                payload: crate::codec::encode_wire(&request),
                process_identity: sender,
            },
        ))
        .await
        .unwrap_err();
    assert_eq!(rejected.code(), tonic::Code::FailedPrecondition);
    engine.shutdown().await.unwrap();
}

#[tokio::test]
async fn removing_incumbent_leader_preserves_ack_but_rejects_new_vote() {
    use openraft::vote::RaftLeaderId;

    use crate::raft_internal_proto::raft_internal_server::RaftInternal;
    let root = tempfile::tempdir().unwrap();
    let placement = ShardPlacement {
        core_id: CoreId(0),
        shard_id: ShardId(0),
        raft_group_id: RaftGroupId(0),
    };
    let wal = crate::log_store::RaftWal::start(
        root.path(),
        ursula_config::WalFsync::Always,
        &ursula_shard::StaticShardMap::new(1, 1).unwrap(),
    )
    .unwrap();
    let registry = crate::RaftGroupHandleRegistry::default();
    registry.set_replica_authority(2, identity(1, 2));
    registry.set_replica_genesis(BTreeMap::from([
        (1, identity(1, 1)),
        (2, identity(1, 2)),
        (3, identity(1, 3)),
    ]));
    let engine = crate::RaftGroupEngine::new_node(
        placement,
        2,
        Arc::new(
            openraft::Config {
                enable_elect: false,
                enable_tick: false,
                ..Default::default()
            }
            .validate()
            .unwrap(),
        ),
        crate::grpc::GrpcRaftNetworkFactory::new(Arc::default(), RaftGroupId(0)),
        wal.open(
            placement,
            ursula_runtime::RuntimeMetrics::new(1, 1).group_engine_metrics(),
        )
        .unwrap(),
        crate::RaftGroupEngineOptions {
            process_authority: Some(registry.clone()),
            snapshot_metadata_path: Some(root.path().join("group-0.snapshot.json")),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    registry.register_engine(&engine, None);
    let service = crate::grpc::RaftGrpcService::new(registry.clone());
    let sender = crate::codec::encode_wire(&FencedProcess {
        node_id: 1,
        identity: identity(1, 1),
    });
    let vote = crate::UrsulaVote::new_committed(1, 1);
    type LeaderId = <crate::UrsulaRaftTypeConfig as openraft::RaftTypeConfig>::LeaderId;
    let log = |index| openraft::LogId {
        leader_id: LeaderId::new(1, 1),
        index,
    };
    let configs = [
        vec![std::collections::BTreeSet::from([1, 2, 3])],
        vec![
            std::collections::BTreeSet::from([1, 2, 3]),
            std::collections::BTreeSet::from([2, 3]),
        ],
        vec![std::collections::BTreeSet::from([2, 3])],
    ];
    for (index, configs) in configs.into_iter().enumerate() {
        let index = u64::try_from(index).unwrap();
        let previous = index.checked_sub(1).map(log);
        let entry = openraft::alias::EntryOf::<crate::UrsulaRaftTypeConfig> {
            log_id: log(index),
            payload: openraft::EntryPayload::Membership(
                openraft::Membership::new(
                    configs,
                    BTreeMap::from([
                        (1, openraft::BasicNode::new("http://leader")),
                        (2, openraft::BasicNode::new("http://follower")),
                        (3, openraft::BasicNode::new("http://next-leader")),
                    ]),
                )
                .unwrap(),
            ),
        };
        let request = crate::UrsulaAppendEntriesRequest {
            vote,
            prev_log_id: previous,
            entries: vec![entry],
            leader_commit: previous,
        };
        let response = service
            .append(tonic::Request::new(
                crate::raft_internal_proto::RaftRpcEnvelopeV1 {
                    protocol_version: crate::grpc::RAFT_GRPC_PROTOCOL_VERSION,
                    raft_group_id: 0,
                    node_id: 2,
                    payload: crate::codec::encode_wire(&request),
                    process_identity: sender.clone(),
                },
            ))
            .await
            .unwrap();
        let ack: openraft::raft::AppendEntriesResponse<crate::UrsulaRaftTypeConfig> =
            rmp_serde::from_slice(&response.into_inner().payload).unwrap();
        assert!(matches!(
            ack,
            openraft::raft::AppendEntriesResponse::Success
        ));
    }
    engine
        .raft_handle()
        .wait(Some(std::time::Duration::from_secs(2)))
        .metrics(
            |metrics| {
                metrics
                    .membership_config
                    .membership()
                    .voter_ids()
                    .eq([2, 3])
            },
            "uniform membership removes incumbent",
        )
        .await
        .unwrap();
    assert!(!registry.replica_sender_is_voter(RaftGroupId(0), 1));
    for (candidate, allowed) in [
        (vote, true),
        (crate::UrsulaVote::new(1, 1), false),
        (crate::UrsulaVote::new_committed(2, 1), false),
    ] {
        let request = crate::UrsulaAppendEntriesRequest {
            vote: candidate,
            prev_log_id: Some(log(2)),
            entries: vec![],
            leader_commit: Some(log(2)),
        };
        let response = service
            .append(tonic::Request::new(
                crate::raft_internal_proto::RaftRpcEnvelopeV1 {
                    protocol_version: crate::grpc::RAFT_GRPC_PROTOCOL_VERSION,
                    raft_group_id: 0,
                    node_id: 2,
                    payload: crate::codec::encode_wire(&request),
                    process_identity: sender.clone(),
                },
            ))
            .await;
        if allowed {
            response.unwrap();
        } else {
            assert_eq!(
                response.unwrap_err().code(),
                tonic::Code::FailedPrecondition
            );
        }
    }
    engine
        .raft_handle()
        .wait(Some(std::time::Duration::from_secs(2)))
        .applied_index_at_least(Some(2), "removal committed")
        .await
        .unwrap();
    for candidate in [crate::UrsulaVote::new(1, 1), crate::UrsulaVote::new(2, 1)] {
        let request = crate::UrsulaVoteRequest {
            vote: candidate,
            last_log_id: Some(log(2)),
        };
        let rejected = service
            .vote(tonic::Request::new(
                crate::raft_internal_proto::RaftRpcEnvelopeV1 {
                    protocol_version: crate::grpc::RAFT_GRPC_PROTOCOL_VERSION,
                    raft_group_id: 0,
                    node_id: 2,
                    payload: crate::codec::encode_wire(&request),
                    process_identity: sender.clone(),
                },
            ))
            .await
            .unwrap_err();
        assert_eq!(rejected.code(), tonic::Code::FailedPrecondition);
    }
    // A candidate Vote can be refused while the previous leader lease is
    // valid. Deliver the next committed leader's Append instead: this tests
    // the exact accepted-vote boundary independently of election timing.
    let next_vote = crate::UrsulaVote::new_committed(2, 3);
    let next_request = crate::UrsulaAppendEntriesRequest {
        vote: next_vote,
        prev_log_id: Some(log(2)),
        entries: vec![],
        leader_commit: Some(log(2)),
    };
    let response = service
        .append(tonic::Request::new(
            crate::raft_internal_proto::RaftRpcEnvelopeV1 {
                protocol_version: crate::grpc::RAFT_GRPC_PROTOCOL_VERSION,
                raft_group_id: 0,
                node_id: 2,
                payload: crate::codec::encode_wire(&next_request),
                process_identity: crate::codec::encode_wire(&FencedProcess {
                    node_id: 3,
                    identity: identity(1, 3),
                }),
            },
        ))
        .await
        .unwrap();
    let ack: openraft::raft::AppendEntriesResponse<crate::UrsulaRaftTypeConfig> =
        rmp_serde::from_slice(&response.into_inner().payload).unwrap();
    assert!(matches!(
        ack,
        openraft::raft::AppendEntriesResponse::Success
    ));
    engine
        .raft_handle()
        .wait(Some(std::time::Duration::from_secs(2)))
        .metrics(
            |metrics| metrics.vote == next_vote,
            "new voter supersedes incumbent",
        )
        .await
        .unwrap();
    let stale = crate::UrsulaAppendEntriesRequest {
        vote,
        prev_log_id: Some(log(2)),
        entries: vec![],
        leader_commit: Some(log(2)),
    };
    let rejected = service
        .append(tonic::Request::new(
            crate::raft_internal_proto::RaftRpcEnvelopeV1 {
                protocol_version: crate::grpc::RAFT_GRPC_PROTOCOL_VERSION,
                raft_group_id: 0,
                node_id: 2,
                payload: crate::codec::encode_wire(&stale),
                process_identity: sender,
            },
        ))
        .await
        .unwrap_err();
    assert_eq!(rejected.code(), tonic::Code::FailedPrecondition);
    engine.shutdown().await.unwrap();
}

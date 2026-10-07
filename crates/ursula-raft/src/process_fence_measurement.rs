//! Opt-in measurement of the production data-RPC process check.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Instant;

use openraft::BasicNode;
use openraft::Config;
use serde::Serialize;
use ursula_control::ControlCommand;
use ursula_control::OperationCommand;
use ursula_control::ProcessIdentity;
use ursula_proto::admin::ProcessIncarnation;
use ursula_shard::RaftGroupId;

use crate::MetaRaftHandle;
use crate::RaftGroupHandleRegistry;
use crate::grpc::FencedProcess;
use crate::grpc::validate_process_fence;
use crate::log_store::MetaTestLogStore;

#[derive(Serialize)]
struct Measurement {
    groups: u32,
    concurrency: usize,
    checks: usize,
    full_state_wire_bytes: usize,
    process_projection_wire_bytes: usize,
    elapsed_micros: u128,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "opt-in benchmark; single-node in-memory metadata measures CPU floor, not network quorum latency"]
async fn measure_meta_process_fence_256_groups() {
    let mut rows = Vec::new();
    for groups in [0, 256] {
        let config = Arc::new(
            Config {
                cluster_name: format!("process-fence-measure-{groups}"),
                heartbeat_interval: 10,
                election_timeout_min: 30,
                election_timeout_max: 60,
                ..Default::default()
            }
            .validate()
            .unwrap(),
        );
        let handle = MetaRaftHandle::new_single_node_with_log_store(
            1,
            BasicNode::new("meta-local"),
            config,
            MetaTestLogStore::shared(),
        )
        .await
        .unwrap();
        for node_id in 1..=3 {
            handle
                .write(ControlCommand::RegisterNode {
                    node_id,
                    client_url: format!("http://node-{node_id}:4437"),
                    cluster_url: format!("http://node-{node_id}:4437"),
                    labels: BTreeMap::new(),
                    now_ms: 0,
                })
                .await
                .unwrap();
            handle
                .write(ControlCommand::Operation {
                    command: OperationCommand::ClaimProcess {
                        node_id,
                        expected_epoch: 0,
                        incarnation: ProcessIncarnation::from_bits(u128::from(node_id)),
                    },
                    now_ms: 0,
                })
                .await
                .unwrap();
        }
        for group in 0..groups {
            handle
                .write(ControlCommand::SeedPlacement {
                    raft_group_id: RaftGroupId(group),
                    voters: BTreeSet::from([1, 2, 3]),
                    now_ms: 0,
                })
                .await
                .unwrap();
        }
        let state = handle.read_linearizable_state().await.unwrap();
        let full_state_wire_bytes = rmp_serde::to_vec_named(&state).unwrap().len();
        let process_projection_wire_bytes = rmp_serde::to_vec_named(&state.operations.processes)
            .unwrap()
            .len();
        let registry = RaftGroupHandleRegistry::default();
        registry.set_process_authority(
            1,
            ProcessIdentity {
                epoch: 1,
                incarnation: ProcessIncarnation::from_bits(1),
            },
            handle.clone(),
        );
        let sender = rmp_serde::to_vec_named(&FencedProcess {
            node_id: 2,
            identity: ProcessIdentity {
                epoch: 1,
                incarnation: ProcessIncarnation::from_bits(2),
            },
        })
        .unwrap();
        for concurrency in [1, 256] {
            let waves = if concurrency == 1 {
                256_usize
            } else {
                32_usize
            };
            let started = Instant::now();
            for _ in 0..waves {
                let checks = (0..concurrency).map(|_| validate_process_fence(&registry, &sender));
                for result in futures_util::future::join_all(checks).await {
                    result.unwrap();
                }
            }
            rows.push(Measurement {
                groups,
                concurrency,
                checks: waves.checked_mul(concurrency).unwrap(),
                full_state_wire_bytes,
                process_projection_wire_bytes,
                elapsed_micros: started.elapsed().as_micros(),
            });
        }
        handle.shutdown().await.unwrap();
    }
    let path = std::env::var("URSULA_FENCE_MEASURE_REPORT").expect("measurement report path");
    std::fs::write(path, serde_json::to_vec_pretty(&rows).unwrap()).unwrap();
}

#[tokio::test]
async fn process_projection_never_reuses_authority_after_a_completed_claim() {
    let config = Arc::new(
        Config {
            cluster_name: "process-projection-epoch".to_owned(),
            heartbeat_interval: 10,
            election_timeout_min: 30,
            election_timeout_max: 60,
            ..Default::default()
        }
        .validate()
        .unwrap(),
    );
    let handle = MetaRaftHandle::new_single_node_with_log_store(
        1,
        BasicNode::new("meta-local"),
        config,
        MetaTestLogStore::shared(),
    )
    .await
    .unwrap();
    handle
        .write(ControlCommand::RegisterNode {
            node_id: 1,
            client_url: "http://one".to_owned(),
            cluster_url: "http://one".to_owned(),
            labels: BTreeMap::new(),
            now_ms: 0,
        })
        .await
        .unwrap();
    let claim = |expected_epoch, incarnation| ControlCommand::Operation {
        command: OperationCommand::ClaimProcess {
            node_id: 1,
            expected_epoch,
            incarnation: ProcessIncarnation::from_bits(incarnation),
        },
        now_ms: 0,
    };
    handle.write(claim(0, 1)).await.unwrap();
    let old = ProcessIdentity {
        epoch: 1,
        incarnation: ProcessIncarnation::from_bits(1),
    };
    let registry = RaftGroupHandleRegistry::default();
    registry.set_process_authority(1, old.clone(), handle.clone());
    let old_sender = rmp_serde::to_vec_named(&FencedProcess {
        node_id: 1,
        identity: old,
    })
    .unwrap();
    for result in futures_util::future::join_all(
        (0..256).map(|_| validate_process_fence(&registry, &old_sender)),
    )
    .await
    {
        result.unwrap();
    }
    handle.write(claim(1, 2)).await.unwrap();
    let error = validate_process_fence(&registry, &old_sender)
        .await
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    let current = ProcessIdentity {
        epoch: 2,
        incarnation: ProcessIncarnation::from_bits(2),
    };
    registry.set_process_authority(1, current.clone(), handle.clone());
    let sender = rmp_serde::to_vec_named(&FencedProcess {
        node_id: 1,
        identity: current,
    })
    .unwrap();
    validate_process_fence(&registry, &sender).await.unwrap();
    assert_eq!(
        validate_process_fence(&registry, &old_sender)
            .await
            .unwrap_err()
            .code(),
        tonic::Code::FailedPrecondition
    );
    handle.shutdown().await.unwrap();
}

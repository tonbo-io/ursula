//! Native meta authority durability, follower forwarding and process epochs.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use openraft::BasicNode;
use openraft::rt::WatchReceiver;
use openraft::storage::RaftLogStorage;
use tokio::sync::oneshot;
use tokio_stream::wrappers::TcpListenerStream;
use ursula_control::ControlCommand;
use ursula_control::ControlResponse;
use ursula_control::OperationCommand;
use ursula_control::OperationOutcome;
use ursula_proto::admin::ProcessIncarnation;

use crate::MetaRaftHandle;

fn config() -> Arc<openraft::Config> {
    Arc::new(
        openraft::Config {
            snapshot_policy: openraft::SnapshotPolicy::Never,
            max_in_snapshot_log_to_keep: 0,
            ..Default::default()
        }
        .validate()
        .expect("valid meta test config"),
    )
}
fn claim(node_id: u64, expected_epoch: u64, boot: u128) -> ControlCommand {
    ControlCommand::Operation {
        command: OperationCommand::ClaimProcess {
            node_id,
            expected_epoch,
            incarnation: ProcessIncarnation::from_bits(boot),
        },
        now_ms: 1,
    }
}

#[tokio::test]
async fn meta_disk_replays_process_epoch_after_snapshot_and_refuses_missing_log() {
    let root = tempfile::tempdir().unwrap();
    let handle = MetaRaftHandle::new_durable(1, root.path().to_owned(), config())
        .await
        .unwrap();
    handle
        .initialize_membership(BTreeMap::from([(1, BasicNode::new("http://local"))]))
        .await
        .unwrap();
    handle
        .wait_for_current_leader(1, Duration::from_secs(3))
        .await
        .unwrap();
    handle
        .register_node(
            crate::MetaNodeRegistration::new(1, "http://client", "http://cluster"),
            0,
        )
        .await
        .unwrap();
    let first = handle.write(claim(1, 0, 11)).await.unwrap();
    let ControlResponse::Operation(Ok(OperationOutcome::ProcessClaimed(first))) = first else {
        panic!("claim refused");
    };
    let applied = handle
        .raft_handle()
        .metrics()
        .borrow_watched()
        .last_applied
        .unwrap();
    handle.raft_handle().trigger().snapshot().await.unwrap();
    handle
        .raft_handle()
        .wait(Some(Duration::from_secs(3)))
        .snapshot(applied, "durable meta snapshot")
        .await
        .unwrap();
    handle.shutdown().await.unwrap();
    drop(handle);
    let handle = MetaRaftHandle::new_durable(1, root.path().to_owned(), config())
        .await
        .unwrap();
    handle
        .wait_for_current_leader(1, Duration::from_secs(3))
        .await
        .unwrap();
    assert!(
        handle
            .read_linearizable_state()
            .await
            .unwrap()
            .operations
            .accepts_process(1, &first)
    );
    let second = handle.write(claim(1, 1, 22)).await.unwrap();
    let ControlResponse::Operation(Ok(OperationOutcome::ProcessClaimed(second))) = second else {
        panic!("replacement claim refused");
    };
    assert_eq!(second.epoch, 2);
    assert!(
        !handle
            .read_linearizable_state()
            .await
            .unwrap()
            .operations
            .accepts_process(1, &first)
    );
    handle.shutdown().await.unwrap();
    drop(handle);
    let snapshot_path = root.path().join("meta-snapshot.msgpack");
    let snapshot_bytes = std::fs::read(&snapshot_path).unwrap();
    let mut store = crate::MetaDiskLogStore::open(root.path().to_owned())
        .await
        .unwrap();
    store.purge(applied).await.unwrap();
    drop(store);
    std::fs::remove_file(&snapshot_path).unwrap();
    let missing = MetaRaftHandle::new_durable(1, root.path().to_owned(), config())
        .await
        .expect_err("purged meta history requires its published snapshot");
    assert_eq!(missing.operation(), "restore meta snapshot");
    std::fs::write(&snapshot_path, b"corrupt-meta-snapshot").unwrap();
    let corrupt = MetaRaftHandle::new_durable(1, root.path().to_owned(), config())
        .await
        .expect_err("corrupt meta snapshot fails closed");
    assert_eq!(corrupt.operation(), "restore meta snapshot");
    std::fs::write(&snapshot_path, snapshot_bytes).unwrap();
    std::fs::remove_file(root.path().join("meta-log.msgpack")).unwrap();
    MetaRaftHandle::new_durable(1, root.path().to_owned(), config())
        .await
        .expect_err("initialized meta WAL cannot silently reinitialize after missing log");
}

#[tokio::test]
async fn meta_grpc_follower_forwards_reads_writes_and_publishes_committed_watch() {
    let mut roots = Vec::new();
    let mut handles = Vec::new();
    let mut servers = Vec::new();
    let mut members = BTreeMap::new();
    for node_id in 1..=3 {
        let root = tempfile::tempdir().unwrap();
        let handle = MetaRaftHandle::new_durable(node_id, root.path().to_owned(), config())
            .await
            .unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        members.insert(
            node_id,
            BasicNode::new(format!("http://{}", listener.local_addr().unwrap())),
        );
        let (stop, stopped) = oneshot::channel();
        let service = crate::MetaGrpcService::new(handle.clone());
        let server = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(service)
                .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                    let _closed = stopped.await;
                })
                .await
                .unwrap();
        });
        roots.push(root);
        handles.push(handle);
        servers.push((stop, server));
    }
    handles[0].initialize_membership(members).await.unwrap();
    let elected = handles[0]
        .raft_handle()
        .wait(Some(Duration::from_secs(5)))
        .metrics(
            |metrics| metrics.current_leader.is_some(),
            "meta leader elected",
        )
        .await
        .unwrap()
        .current_leader
        .unwrap();
    for handle in &handles {
        handle
            .wait_for_current_leader(elected, Duration::from_secs(5))
            .await
            .unwrap();
    }
    let follower = handles
        .iter()
        .find(|handle| handle.raft_handle().metrics().borrow_watched().id != elected)
        .unwrap();
    follower
        .register_node(
            crate::MetaNodeRegistration::new(9, "http://client-9", "http://cluster-9"),
            0,
        )
        .await
        .unwrap();
    let response = follower.write(claim(9, 0, 9)).await.unwrap();
    let ControlResponse::Operation(Ok(OperationOutcome::ProcessClaimed(identity))) = response
    else {
        panic!("claim refused");
    };
    assert!(
        follower
            .read_linearizable_state()
            .await
            .unwrap()
            .operations
            .accepts_process(9, &identity)
    );
    let mut watch = follower.committed_state();
    tokio::time::timeout(Duration::from_secs(5), async {
        while !watch.borrow().operations.accepts_process(9, &identity) {
            watch.changed().await.unwrap();
        }
    })
    .await
    .unwrap();
    for handle in &handles {
        handle.shutdown().await.unwrap();
    }
    for (stop, server) in servers {
        stop.send(()).unwrap();
        server.await.unwrap();
    }
}

#[tokio::test]
async fn meta_boot_claim_supersedes_maintenance_identity() {
    let root = tempfile::tempdir().unwrap();
    let meta = MetaRaftHandle::new_durable(1, root.path().to_owned(), config())
        .await
        .unwrap();
    meta.initialize_membership(BTreeMap::from([(1, BasicNode::new("local"))]))
        .await
        .unwrap();
    meta.wait_for_current_leader(1, Duration::from_secs(3))
        .await
        .unwrap();
    for node in [1, 2] {
        meta.register_node(
            crate::MetaNodeRegistration::new(node, "http://client", "http://cluster"),
            0,
        )
        .await
        .unwrap();
    }
    let ControlResponse::Operation(Ok(OperationOutcome::ProcessClaimed(local))) =
        meta.write(claim(1, 0, 1)).await.unwrap()
    else {
        panic!("local claim");
    };
    let ControlResponse::Operation(Ok(OperationOutcome::ProcessClaimed(old))) =
        meta.write(claim(2, 0, 2)).await.unwrap()
    else {
        panic!("remote claim");
    };
    assert!(
        meta.read_linearizable_processes()
            .await
            .unwrap()
            .get(&1)
            .is_some_and(|state| state == &ursula_control::ProcessState::Active(local))
    );
    assert_eq!(
        meta.read_linearizable_processes().await.unwrap().get(&2),
        Some(&ursula_control::ProcessState::Active(old.clone()))
    );
    meta.write(claim(2, 1, 3)).await.unwrap();
    assert_ne!(
        meta.read_linearizable_processes().await.unwrap().get(&2),
        Some(&ursula_control::ProcessState::Active(old))
    );
    meta.shutdown().await.unwrap();
}

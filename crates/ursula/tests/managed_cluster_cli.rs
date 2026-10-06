//! Real-process managed adoption/restart with disk data WAL and independent meta.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::path::Path;
use std::process::Child;
use std::process::Command;
use std::process::Stdio;
use std::time::Duration;
use std::time::Instant;

use ursula_config::ControlConfig;
use ursula_config::HumanDuration;
use ursula_config::UrsulaConfig;
use ursula_control::ClusterId;
use ursula_control::ControlProjection;
use ursula_control::GroupPolicyOverride;
use ursula_control::NodeRegistration;
use ursula_control::PlacementPolicy;
use ursula_control::ReplicationFactor;
use ursula_shard::RaftGroupId;

struct Process {
    child: Child,
    log: std::path::PathBuf,
}
impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}
fn spawn(binary: &str, config: &Path, log: &Path) -> Process {
    let file = std::fs::File::create(log).unwrap();
    let child = Command::new(binary)
        .args(["--config", config.to_str().unwrap()])
        .env("RUST_LOG", "ursula=info,ursula_raft=info")
        .stdout(Stdio::from(file.try_clone().unwrap()))
        .stderr(Stdio::from(file))
        .spawn()
        .unwrap();
    Process {
        child,
        log: log.to_owned(),
    }
}
fn port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}
fn check_alive(processes: &mut [Process]) {
    for process in processes {
        if let Some(status) = process.child.try_wait().unwrap() {
            panic!(
                "node exited {status}: {}",
                std::fs::read_to_string(&process.log).unwrap()
            );
        }
    }
}
async fn ready(
    client: &reqwest::Client,
    nodes: &BTreeMap<u64, NodeRegistration>,
    processes: &mut [Process],
) {
    let deadline = Instant::now() + Duration::from_secs(90);
    loop {
        check_alive(processes);
        let mut all = true;
        for node in nodes.values() {
            all &= client
                .get(format!("{}/__ursula/ready", node.client_url))
                .send()
                .await
                .is_ok_and(|response| response.status().is_success());
        }
        if all {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "managed readiness timeout; logs: {:?}",
            processes
                .iter()
                .map(|process| std::fs::read_to_string(&process.log).unwrap())
                .collect::<Vec<_>>()
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}
async fn create(
    client: &reqwest::Client,
    nodes: &BTreeMap<u64, NodeRegistration>,
    name: &str,
    body: &str,
) {
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut last = Vec::new();
    loop {
        last.clear();
        for node in nodes.values() {
            let response = client
                .put(format!("{}/benchcmp/{name}", node.client_url))
                .body(body.to_owned())
                .send()
                .await;
            match response {
                Ok(response) if response.status().is_success() => return,
                Ok(response) => {
                    let status = response.status();
                    let body = response.text().await.unwrap_or_default();
                    last.push(format!("node {}: {status} {body}", node.node_id));
                }
                Err(error) => last.push(format!("node {}: {error}", node.node_id)),
            }
        }
        assert!(
            Instant::now() < deadline,
            "stream {name} creation timeout: {last:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}
async fn payload(client: &reqwest::Client, node: &NodeRegistration, name: &str, expected: &str) {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let response = client
            .get(format!(
                "{}/benchcmp/{name}?offset=0&max_bytes=4096",
                node.client_url
            ))
            .send()
            .await;
        if let Ok(response) = response
            && response.status().is_success()
        {
            let body = response.bytes().await.unwrap();
            if body.as_ref() == expected.as_bytes() {
                return;
            }
        }
        assert!(
            Instant::now() < deadline,
            "payload did not survive adoption/restart"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}
async fn projection(config: &UrsulaConfig) -> ControlProjection {
    let control = config.control.as_ref().unwrap();
    let identity = control.local_identity(config).unwrap().cluster;
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        for node in control
            .bootstrap_nodes
            .iter()
            .filter(|node| control.initial_meta_voters.contains(&node.node_id))
        {
            if let Ok(projection) = ursula_raft::read_control_projection(
                &identity,
                node.node_id,
                &node.cluster_url,
                Duration::from_secs(1),
            )
            .await
            {
                return projection;
            }
        }
        assert!(Instant::now() < deadline, "meta projection timeout");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn managed_cli_adopts_mixed_disk_groups_with_three_and_five_meta_voters_and_restarts() {
    let binary = env!("CARGO_BIN_EXE_ursula");
    for meta_count in [3_usize, 5] {
        let root = tempfile::tempdir().unwrap();
        let nodes = (1..=5)
            .map(|id| {
                (id, NodeRegistration {
                    node_id: id,
                    client_url: format!("http://127.0.0.1:{}", port()),
                    cluster_url: format!("http://127.0.0.1:{}", port()),
                    admin_url: format!("http://127.0.0.1:{}", port()),
                    labels: BTreeMap::from([("zone".to_owned(), ((id - 1) % 3).to_string())]),
                })
            })
            .collect::<BTreeMap<_, _>>();
        let mut configs = Vec::new();
        let mut paths = Vec::new();
        let mut processes = Vec::new();
        for node in nodes.values() {
            let mut config = UrsulaConfig::default();
            config.runtime.core_count = 1;
            config.server.listen = node.client_url.trim_start_matches("http://").to_owned();
            config.server.cluster_listen =
                Some(node.cluster_url.trim_start_matches("http://").to_owned());
            config.server.admin_listen = node.admin_url.trim_start_matches("http://").to_owned();
            config.raft.node_id = node.node_id;
            config.raft.group_count = 2;
            config.raft.init_membership = true;
            config.raft.init_membership_per_group = true;
            config.raft.wal.backend = ursula_config::WalBackend::Disk;
            config.raft.wal.path = Some(root.path().join(format!("data-{}", node.node_id)));
            config.raft.peers = nodes
                .values()
                .map(|node| ursula_config::RaftPeerConfig {
                    node_id: node.node_id,
                    url: node.cluster_url.clone(),
                })
                .collect();
            config.raft.groups = vec![
                ursula_config::RaftGroupConfig {
                    raft_group_id: 0,
                    voters: vec![1, 2, 3],
                },
                ursula_config::RaftGroupConfig {
                    raft_group_id: 1,
                    voters: vec![1, 2, 3, 4, 5],
                },
            ];
            config.validate().unwrap();
            let path = root.path().join(format!("node-{}.toml", node.node_id));
            std::fs::write(&path, toml::to_string_pretty(&config).unwrap()).unwrap();
            processes.push(spawn(
                binary,
                &path,
                &root.path().join(format!("static-{}.log", node.node_id)),
            ));
            configs.push(config);
            paths.push(path);
        }
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(Duration::from_secs(2))
            .build()
            .unwrap();
        ready(&client, &nodes, &mut processes).await;
        create(
            &client,
            &nodes,
            "managed-before",
            "acknowledged-before-adoption",
        )
        .await;
        drop(processes);
        let mut processes = Vec::new();
        for (index, config) in configs.iter_mut().enumerate() {
            config.raft.init_membership = false;
            config.raft.init_membership_per_group = false;
            config.control = Some(ControlConfig {
                cluster_id: ClusterId::try_from(format!("managed-cli-{meta_count}")).unwrap(),
                meta_journal_path: root
                    .path()
                    .join(format!("meta-{}/meta.wal", config.raft.node_id)),
                bootstrap_node_id: 1,
                initialize_meta_membership: true,
                initial_meta_voters: (1..=meta_count as u64).collect(),
                node: nodes[&config.raft.node_id].clone(),
                bootstrap_nodes: nodes.values().cloned().collect(),
                placement: if meta_count == 5 {
                    PlacementPolicy {
                        default_replication_factor: ReplicationFactor::Five,
                        group_overrides: vec![GroupPolicyOverride {
                            raft_group_id: RaftGroupId(0),
                            replication_factor: ReplicationFactor::Three,
                        }],
                        ..Default::default()
                    }
                } else {
                    PlacementPolicy {
                        group_overrides: vec![GroupPolicyOverride {
                            raft_group_id: RaftGroupId(1),
                            replication_factor: ReplicationFactor::Five,
                        }],
                        ..Default::default()
                    }
                },
                meta_snapshot_logs_since_last: 1,
                bootstrap_timeout: HumanDuration::sec(60),
                refresh_interval: HumanDuration::milli(100),
            });
            config.validate().unwrap();
            std::fs::write(&paths[index], toml::to_string_pretty(config).unwrap()).unwrap();
            processes.push(spawn(
                binary,
                &paths[index],
                &root
                    .path()
                    .join(format!("managed-{}.log", config.raft.node_id)),
            ));
        }
        ready(&client, &nodes, &mut processes).await;
        for node in nodes.values() {
            let response = client
                .post(format!("{}/__ursula/cluster-probe", node.cluster_url))
                .body(vec![0_u8; 64 * 1024])
                .send()
                .await
                .unwrap();
            assert_eq!(
                response.status(),
                reqwest::StatusCode::OK,
                "egress probes must reach the private plane"
            );
        }
        let before = projection(&configs[0]).await;
        assert_eq!(before.state.config.initial_meta_voters.len(), meta_count);
        assert_eq!(
            before.state.placements[&RaftGroupId(0)].voters,
            BTreeSet::from([1, 2, 3])
        );
        assert_eq!(
            before.state.placements[&RaftGroupId(1)].voters,
            BTreeSet::from([1, 2, 3, 4, 5])
        );
        create(
            &client,
            &nodes,
            "managed-after",
            "acknowledged-after-adoption",
        )
        .await;
        // Raw membership routes refuse mutations, even with the right process
        // incarnation. Managed operations must use the later fenced executor.
        for node in nodes.values() {
            let metrics: serde_json::Value = client
                .get(format!("{}/__ursula/metrics", node.admin_url))
                .send()
                .await
                .unwrap()
                .json()
                .await
                .unwrap();
            let response = client
                .post(format!("{}/__ursula/raft/0/membership", node.admin_url))
                .header(
                    ursula_proto::admin::PROCESS_INCARNATION_HEADER,
                    metrics["process_incarnation"].as_str().unwrap(),
                )
                .json(&serde_json::json!({"voters":[1,2,3],"retain":false}))
                .send()
                .await
                .unwrap();
            assert_eq!(response.status(), reqwest::StatusCode::CONFLICT);
        }
        let identity = configs[0]
            .control
            .as_ref()
            .unwrap()
            .local_identity(&configs[0])
            .unwrap()
            .cluster;
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let mut compacted = true;
            for id in 1..=meta_count as u64 {
                let status = ursula_raft::read_meta_replica_status(
                    &identity,
                    id,
                    &nodes[&id].cluster_url,
                    Duration::from_secs(1),
                )
                .await
                .unwrap();
                compacted &= status
                    .purged_log_index
                    .is_some_and(|index| index >= before.applied_log_id.index);
            }
            if compacted {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "meta did not persist and compact bootstrap snapshots"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        drop(processes);
        let mut processes = paths
            .iter()
            .enumerate()
            .map(|(index, path)| {
                spawn(
                    binary,
                    path,
                    &root.path().join(format!("restart-{}.log", index + 1)),
                )
            })
            .collect::<Vec<_>>();
        ready(&client, &nodes, &mut processes).await;
        let after = projection(&configs[0]).await;
        assert_eq!(
            after.state, before.state,
            "restart must preserve the persisted recipe, policies and placement"
        );
        let following = reqwest::Client::builder()
            .timeout(Duration::from_secs(2))
            .build()
            .unwrap();
        let map = ursula_shard::StaticShardMap::new(1, 2).unwrap();
        let name = (0..100)
            .map(|index| format!("managed-nonhost-{index}"))
            .find(|name| {
                map.locate(&ursula_shard::BucketStreamId::new("benchcmp", name.clone()))
                    .raft_group_id
                    == RaftGroupId(0)
            })
            .unwrap();
        create(&client, &nodes, &name, "non-hosting-front-door").await;
        let redirect = client
            .get(format!("{}/benchcmp/{name}?offset=0", nodes[&5].client_url))
            .send()
            .await
            .unwrap();
        assert_eq!(redirect.status(), reqwest::StatusCode::TEMPORARY_REDIRECT);
        let location = redirect.headers()[reqwest::header::LOCATION]
            .to_str()
            .unwrap();
        assert!(
            nodes
                .values()
                .any(|node| location.starts_with(&node.client_url))
        );
        assert!(
            nodes
                .values()
                .all(|node| !location.starts_with(&node.cluster_url))
        );
        payload(&following, &nodes[&5], &name, "non-hosting-front-door").await;
        // Stop the current meta leader, plus a second voter at RF5, while
        // preserving both data groups' quorum. Fresh control reads elect a new
        // leader and the established data path continues through its own quorum.
        let status = ursula_raft::read_meta_replica_status(
            &identity,
            1,
            &nodes[&1].cluster_url,
            Duration::from_secs(1),
        )
        .await
        .unwrap();
        let lost_leader = status.current_leader.unwrap();
        let mut lost = BTreeSet::from([lost_leader]);
        if meta_count == 5 {
            lost.insert(if lost_leader == 5 { 4 } else { 5 });
        }
        let read_node = nodes
            .values()
            .find(|node| !lost.contains(&node.node_id))
            .unwrap()
            .clone();
        for id in lost {
            let process = &mut processes[(id - 1) as usize];
            process.child.kill().unwrap();
            process.child.wait().unwrap();
        }
        assert_eq!(projection(&configs[0]).await.state, before.state);
        create(
            &client,
            &nodes,
            "managed-turnover",
            "acknowledged-with-meta-voter-loss",
        )
        .await;
        // Managed redirects use public client origins despite separate cluster
        // listeners, including requests landing on a non-hosting data node.
        let following = reqwest::Client::builder()
            .timeout(Duration::from_secs(2))
            .build()
            .unwrap();
        payload(
            &following,
            &read_node,
            "managed-turnover",
            "acknowledged-with-meta-voter-loss",
        )
        .await;
        payload(
            &following,
            &read_node,
            "managed-before",
            "acknowledged-before-adoption",
        )
        .await;
        payload(
            &following,
            &read_node,
            "managed-after",
            "acknowledged-after-adoption",
        )
        .await;
    }
}

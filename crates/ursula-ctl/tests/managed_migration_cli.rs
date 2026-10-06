//! Real Ursula and ursulactl processes, independent meta/data Raft and disk WAL.
//! Run after `cargo build -p ursula --bin ursula`; workspace integration builds
//! already provide that binary. Inline/local snapshots do not establish S3.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::path::Path;
use std::path::PathBuf;
use std::process::Child;
use std::process::Command;
use std::process::Stdio;
use std::sync::atomic::AtomicU16;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::time::Instant;

use ursula_config::ControlConfig;
use ursula_config::HumanDuration;
use ursula_config::UrsulaConfig;
use ursula_control::ControlProjection;
use ursula_control::GroupPolicyOverride;
use ursula_control::MigrationPhase;
use ursula_control::NodeRegistration;
use ursula_control::PlacementPolicy;
use ursula_control::ReplicationFactor;
use ursula_shard::BucketStreamId;
use ursula_shard::RaftGroupId;
use ursula_shard::StaticShardMap;

#[path = "managed_migration/joint_fault.rs"]
mod joint_fault;
#[path = "managed_migration/snapshot_fault.rs"]
mod snapshot_fault;
#[path = "managed_migration/snapshot_reply.rs"]
mod snapshot_reply;

struct Process {
    child: Child,
    log: PathBuf,
}

impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

struct Cluster {
    root: tempfile::TempDir,
    binary: PathBuf,
    nodes: BTreeMap<u64, NodeRegistration>,
    configs: BTreeMap<u64, UrsulaConfig>,
    processes: BTreeMap<u64, Process>,
    client: reqwest::Client,
    manifest: PathBuf,
    fault: Option<std::sync::Arc<joint_fault::Gate>>,
    _proxies: Vec<joint_fault::Proxy>,
}

fn port() -> u16 {
    static NEXT: AtomicU16 = AtomicU16::new(20000);
    loop {
        // Unique across concurrent fixtures and below the native runners'
        // default ephemeral range. An outbound probe from an early process
        // cannot claim another process's not-yet-bound listener port.
        let candidate = NEXT
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |port| {
                port.checked_add(1).filter(|next| *next <= 30000)
            })
            .expect("fixture listener port range exhausted");
        match std::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, candidate)) {
            Ok(_) => return candidate,
            Err(error) if error.kind() == std::io::ErrorKind::AddrInUse => {}
            Err(error) => panic!("reserve migration fixture port: {error}"),
        }
    }
}

impl Cluster {
    async fn new() -> Self {
        Self::new_with_s3(None).await
    }

    async fn new_with_s3(storage: Option<(ursula_config::S3Config, String)>) -> Self {
        Self::new_with_transport(storage, false).await
    }

    async fn new_with_transport(
        storage: Option<(ursula_config::S3Config, String)>,
        proxy: bool,
    ) -> Self {
        let cli = Path::new(env!("CARGO_BIN_EXE_ursulactl"));
        let binary = std::env::var_os("URSULA_BINARY")
            .map(PathBuf::from)
            .unwrap_or_else(|| cli.parent().unwrap().join("ursula"));
        assert!(
            binary.is_file(),
            "build the Ursula binary first: cargo build -p ursula --bin ursula"
        );
        let root = tempfile::tempdir().unwrap();
        let nodes: BTreeMap<_, _> = (1..=6)
            .map(|id| {
                (id, NodeRegistration {
                    node_id: id,
                    client_url: format!("http://127.0.0.1:{}", port()),
                    cluster_url: format!("http://127.0.0.1:{}", port()),
                    admin_url: format!("http://127.0.0.1:{}", port()),
                    labels: BTreeMap::from([("zone".to_owned(), ((id - 1) % 3).to_string())]),
                })
            })
            .collect();
        let fault = proxy.then(|| std::sync::Arc::new(joint_fault::Gate::default()));
        let mut proxies = Vec::new();
        let mut cluster_listeners = BTreeMap::new();
        for (id, node) in &nodes {
            let listener = if let Some(gate) = &fault {
                let listener = format!("127.0.0.1:{}", port());
                proxies.push(
                    joint_fault::Proxy::start(
                        &node.cluster_url,
                        &format!("http://{listener}"),
                        gate.clone(),
                    )
                    .await,
                );
                listener
            } else {
                node.cluster_url.trim_start_matches("http://").to_owned()
            };
            cluster_listeners.insert(*id, listener);
        }
        let configs = nodes
            .iter()
            .map(|(id, node)| {
                let mut config = UrsulaConfig::default();
                config.runtime.core_count = 1;
                config.server.listen = node.client_url.trim_start_matches("http://").to_owned();
                config.server.cluster_listen = Some(cluster_listeners[id].clone());
                config.server.admin_listen =
                    node.admin_url.trim_start_matches("http://").to_owned();
                config.raft.node_id = *id;
                config.raft.group_count = 2;
                config.raft.init_membership = true;
                config.raft.init_membership_per_group = true;
                config.raft.wal.backend = ursula_config::WalBackend::Disk;
                config.raft.wal.path = Some(root.path().join(format!("data-{id}")));
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
                if let Some((s3, prefix)) = &storage {
                    config.storage.cold.backend = ursula_config::ColdBackend::S3;
                    config.storage.cold.root = Some(prefix.clone());
                    config.storage.cold.s3 = Some(s3.clone());
                    config.storage.snapshot.backend = ursula_config::RaftSnapshotBackend::S3;
                    config.storage.snapshot.drive_interval = Some(HumanDuration::milli(0));
                    config.raft.snapshot_logs_since_last = 1;
                    config.raft.max_in_snapshot_log_to_keep = 0;
                }
                config.validate().unwrap();
                (*id, config)
            })
            .collect();
        let manifest = root.path().join("cluster.json");
        std::fs::write(
            &manifest,
            serde_json::to_vec(
                &serde_json::json!({"nodes": nodes.values().map(|node| serde_json::json!({
            "id": node.node_id, "admin_url": node.admin_url, "host": "127.0.0.1",
        })).collect::<Vec<_>>()}),
            )
            .unwrap(),
        )
        .unwrap();
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(2))
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .unwrap();
        let mut cluster = Self {
            root,
            binary,
            nodes,
            configs,
            processes: BTreeMap::new(),
            client,
            manifest,
            fault,
            _proxies: proxies,
        };
        for id in 1..=6 {
            cluster.start(id, "static");
        }
        cluster.ready().await;
        cluster.processes.clear();
        for (id, config) in &mut cluster.configs {
            config.raft.init_membership = false;
            config.raft.init_membership_per_group = false;
            config.control = Some(ControlConfig {
                cluster_id: "managed-migration-cli".to_owned().try_into().unwrap(),
                meta_journal_path: cluster.root.path().join(format!("meta-{id}/meta.wal")),
                bootstrap_node_id: 1,
                initialize_meta_membership: true,
                initial_meta_voters: vec![1, 2, 3],
                node: cluster.nodes[id].clone(),
                bootstrap_nodes: cluster.nodes.values().cloned().collect(),
                placement: PlacementPolicy {
                    group_overrides: vec![GroupPolicyOverride {
                        raft_group_id: RaftGroupId(1),
                        replication_factor: ReplicationFactor::Five,
                    }],
                    ..Default::default()
                },
                meta_snapshot_logs_since_last: 1,
                bootstrap_timeout: HumanDuration::sec(60),
                refresh_interval: HumanDuration::milli(100),
            });
            config.validate().unwrap();
        }
        for id in 1..=6 {
            cluster.start(id, "managed");
        }
        cluster.ready().await;
        cluster
    }

    fn start(&mut self, id: u64, phase: &str) {
        let path = self.root.path().join(format!("node-{id}.toml"));
        std::fs::write(&path, toml::to_string_pretty(&self.configs[&id]).unwrap()).unwrap();
        let log = self.root.path().join(format!("{phase}-{id}.log"));
        let file = std::fs::File::create(&log).unwrap();
        let child = Command::new(&self.binary)
            .arg("--config")
            .arg(path)
            .env("RUST_LOG", "ursula=info,ursula_raft=info")
            .stdout(Stdio::from(file.try_clone().unwrap()))
            .stderr(Stdio::from(file))
            .spawn()
            .unwrap();
        assert!(self.processes.insert(id, Process { child, log }).is_none());
    }

    fn alive(&mut self) {
        for process in self.processes.values_mut() {
            if let Some(status) = process.child.try_wait().unwrap() {
                panic!(
                    "server exited {status}: {}",
                    std::fs::read_to_string(&process.log).unwrap()
                );
            }
        }
    }

    async fn ready(&mut self) {
        let deadline = Instant::now() + Duration::from_secs(60);
        let mut diagnostics = Vec::new();
        loop {
            self.alive();
            diagnostics.clear();
            for id in self.processes.keys() {
                // Static mode has no serving-role contract for a node owning
                // zero configured replicas. Managed mode must certify it ready
                // from explicit assignment inventory, including after evacuation.
                if self.configs[id].control.is_none()
                    && !self.configs[id]
                        .raft
                        .groups
                        .iter()
                        .any(|group| group.voters.contains(id))
                {
                    continue;
                }
                match self
                    .client
                    .get(format!("{}/__ursula/ready", self.nodes[id].client_url))
                    .send()
                    .await
                {
                    Ok(response) if response.status().is_success() => {}
                    Ok(response) => {
                        diagnostics.push(format!("node {id}: {}", response.text().await.unwrap()))
                    }
                    Err(error) => diagnostics.push(format!("node {id}: {error}")),
                }
            }
            if diagnostics.is_empty() {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "readiness timeout: {diagnostics:?}"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    async fn cli_output(&self, arguments: &[&str]) -> std::process::Output {
        tokio::time::timeout(
            Duration::from_secs(50),
            tokio::process::Command::new(env!("CARGO_BIN_EXE_ursulactl"))
                .args(["operation"])
                .args(arguments)
                .arg("--config")
                .arg(&self.manifest)
                .kill_on_drop(true)
                .output(),
        )
        .await
        .expect("CLI deadline")
        .unwrap()
    }

    async fn cli(&self, arguments: &[&str]) -> serde_json::Value {
        let output = self.cli_output(arguments).await;
        assert!(
            output.status.success(),
            "CLI {arguments:?}: {} {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        serde_json::from_slice(&output.stdout).unwrap_or_else(|error| {
            panic!(
                "CLI {arguments:?} JSON {error}: {} {}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            )
        })
    }

    async fn view(&self) -> ControlProjection {
        tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                let output = self.cli_output(&["status"]).await;
                if output.status.success() {
                    return serde_json::from_slice(&output.stdout).unwrap();
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .expect("no fresh control projection through CLI")
    }

    async fn submit(&self, key: &str, epoch: u64, voters: &str, rf: Option<&str>) -> u64 {
        self.submit_group(0, key, epoch, voters, rf).await
    }

    async fn submit_group(
        &self,
        group: u32,
        key: &str,
        epoch: u64,
        voters: &str,
        rf: Option<&str>,
    ) -> u64 {
        let epoch = epoch.to_string();
        let group = group.to_string();
        let mut arguments = vec![
            "submit",
            "--operation-key",
            key,
            "--group",
            &group,
            "--expected-epoch",
            &epoch,
            "--voters",
            voters,
        ];
        if let Some(rf) = rf {
            arguments.extend(["--rf", rf]);
        }
        self.cli(&arguments).await["migration_id"].as_u64().unwrap()
    }

    async fn register(&self, node: &NodeRegistration) -> std::process::Output {
        let path = self.root.path().join("registration.json");
        std::fs::write(&path, serde_json::to_vec(node).unwrap()).unwrap();
        self.cli_output(&["register-node", "--registration", path.to_str().unwrap()])
            .await
    }

    fn provision(&mut self, node: NodeRegistration) {
        let id = node.node_id;
        assert!(!self.nodes.contains_key(&id));
        let mut config = self.configs[&6].clone();
        config.raft.node_id = id;
        config.raft.wal.path = Some(self.root.path().join(format!("data-{id}")));
        config.server.listen = node.client_url.trim_start_matches("http://").to_owned();
        config.server.cluster_listen =
            Some(node.cluster_url.trim_start_matches("http://").to_owned());
        config.server.admin_listen = node.admin_url.trim_start_matches("http://").to_owned();
        config.raft.peers.push(ursula_config::RaftPeerConfig {
            node_id: id,
            url: node.cluster_url.clone(),
        });
        let control = config.control.as_mut().unwrap();
        control.node = node.clone();
        control.meta_journal_path = self.root.path().join(format!("meta-{id}/meta.wal"));
        // Neither the original recipe nor old nodes' startup files are extended.
        assert!(
            !control
                .bootstrap_nodes
                .iter()
                .any(|node| node.node_id == id)
        );
        config.validate().unwrap();
        self.configs.insert(id, config);
        self.nodes.insert(id, node);
    }

    async fn configuration(&self, group: u32, voters: BTreeSet<u64>) -> u64 {
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            for id in &voters {
                if let Ok(actual) = ursula_raft::confirm_group_configuration(
                    RaftGroupId(group),
                    *id,
                    &self.nodes[id].cluster_url,
                    Duration::from_secs(2),
                )
                .await
                {
                    assert_eq!(actual.voter_sets, vec![voters.clone()]);
                    assert!(actual.learners.is_empty());
                    return actual.leader_id;
                }
            }
            assert!(
                Instant::now() < deadline,
                "no actual uniform group configuration"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    async fn start_gateway(&self, refresh_ms: u64) -> (Process, String) {
        let path = self.root.path().join("gateway-bootstrap.json");
        let view = self.view().await;
        std::fs::write(
            &path,
            serde_json::to_vec(&view.state.cluster_bootstrap.unwrap().recipe).unwrap(),
        )
        .unwrap();
        let address = format!("127.0.0.1:{}", port());
        let log = self.root.path().join(format!("gateway-{refresh_ms}.log"));
        let file = std::fs::File::create(&log).unwrap();
        let child = Command::new(&self.binary)
            .args(["gateway", "--listen", &address, "--managed-bootstrap"])
            .arg(path)
            .args(["--managed-refresh-ms", &refresh_ms.to_string()])
            .env("RUST_LOG", "ursula_gateway=debug")
            .stdout(Stdio::from(file.try_clone().unwrap()))
            .stderr(Stdio::from(file))
            .spawn()
            .unwrap();
        (Process { child, log }, format!("http://{address}"))
    }

    async fn gateway_payload(
        &mut self,
        gateway: &mut Process,
        origin: &str,
        name: &str,
        body: &str,
        write: bool,
    ) {
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            self.alive();
            if let Some(status) = gateway.child.try_wait().unwrap() {
                panic!(
                    "gateway exited {status}: {}",
                    std::fs::read_to_string(&gateway.log).unwrap()
                );
            }
            let request = if write {
                self.client
                    .put(format!("{origin}/benchcmp/{name}"))
                    .body(body.to_owned())
            } else {
                self.client
                    .get(format!("{origin}/benchcmp/{name}?offset=0&max_bytes=4096"))
            };
            if let Ok(response) = request.send().await
                && response.status().is_success()
                && (write || response.bytes().await.unwrap().as_ref() == body.as_bytes())
            {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "gateway request failed: {}",
                std::fs::read_to_string(&gateway.log).unwrap()
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    async fn wait(&self, id: u64) {
        let id = id.to_string();
        let operation = self
            .cli(&["resume", "--operation", &id, "--timeout-secs", "40"])
            .await;
        assert_eq!(operation["phase"], "Succeeded");
    }

    async fn write(&mut self, name: &str, body: &str) {
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            self.alive();
            for id in self.processes.keys() {
                if self
                    .client
                    .put(format!("{}/benchcmp/{name}", self.nodes[id].client_url))
                    .body(body.to_owned())
                    .send()
                    .await
                    .is_ok_and(|response| response.status().is_success())
                {
                    return;
                }
            }
            assert!(Instant::now() < deadline, "HTTP write timeout");
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    async fn payloads(&mut self, name: &str, body: &str) {
        let following = reqwest::Client::builder()
            .timeout(Duration::from_secs(2))
            .build()
            .unwrap();
        for id in self.processes.keys().copied().collect::<Vec<_>>() {
            let deadline = Instant::now() + Duration::from_secs(30);
            loop {
                self.alive();
                if let Ok(response) = following
                    .get(format!(
                        "{}/benchcmp/{name}?offset=0&max_bytes=4096",
                        self.nodes[&id].client_url
                    ))
                    .send()
                    .await
                    && response.status().is_success()
                    && response.bytes().await.unwrap().as_ref() == body.as_bytes()
                {
                    break;
                }
                assert!(
                    Instant::now() < deadline,
                    "node {id} could not read acknowledged payload {name}"
                );
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn binaries_join_outside_bootstrap_directory_and_restore_rf3_rf5() {
    let mut cluster = Cluster::new().await;
    let map = StaticShardMap::new(1, 2).unwrap();
    let names = [0, 1].map(|group| {
        (0..100)
            .map(|n| format!("join-group-{group}-{n}"))
            .find(|name| {
                map.locate(&BucketStreamId::new("benchcmp", name.clone()))
                    .raft_group_id
                    == RaftGroupId(group)
            })
            .unwrap()
    });
    for name in &names {
        cluster.write(name, "acknowledged-before-node-join").await;
    }
    let (mut gateway, gateway_origin) = cluster.start_gateway(600000).await;
    cluster
        .gateway_payload(
            &mut gateway,
            &gateway_origin,
            &names[0],
            "acknowledged-before-node-join",
            false,
        )
        .await;
    let node = NodeRegistration {
        node_id: 7,
        client_url: format!("http://127.0.0.1:{}", port()),
        cluster_url: format!("http://127.0.0.1:{}", port()),
        admin_url: format!("http://127.0.0.1:{}", port()),
        labels: BTreeMap::from([("zone".to_owned(), "2".to_owned())]),
    };
    let registered = cluster.register(&node).await;
    assert!(
        registered.status.success(),
        "{}",
        String::from_utf8_lossy(&registered.stderr)
    );
    let mut replay = node.clone();
    replay.admin_url.push('/');
    assert!(cluster.register(&replay).await.status.success());
    let mut conflict = node.clone();
    conflict
        .labels
        .insert("zone".to_owned(), "other".to_owned());
    assert!(!cluster.register(&conflict).await.status.success());
    let view = cluster.view().await;
    assert_eq!(view.state.nodes[&7].labels, node.labels);
    assert_eq!(
        view.state.config.initial_meta_voters,
        BTreeSet::from([1, 2, 3])
    );
    assert!(
        !view
            .state
            .cluster_bootstrap
            .as_ref()
            .unwrap()
            .recipe
            .nodes
            .contains_key(&7)
    );
    assert!(
        view.state
            .placements
            .values()
            .all(|placement| !placement.voters.contains(&7))
    );
    cluster.provision(node);
    cluster.start(7, "outside-directory-join");
    cluster.ready().await;
    for (group, voters) in [(0, "1,2,7"), (1, "1,2,4,5,7")] {
        let id = cluster
            .submit_group(
                group,
                &format!("outside-directory-{group}"),
                0,
                voters,
                None,
            )
            .await;
        cluster.wait(id).await;
        cluster
            .configuration(
                group,
                voters.split(',').map(|id| id.parse().unwrap()).collect(),
            )
            .await;
        cluster.ready().await;
        for name in &names {
            cluster
                .payloads(name, "acknowledged-before-node-join")
                .await;
        }
    }
    // The manifest still lists the original six nodes and no client origins.
    // Managed verification discovers node 7 and each group's own replica set.
    let verified = cluster.cli(&["verify-quorum"]).await;
    assert_eq!(
        verified["groups"]["0"]["required_majorities"],
        serde_json::json!([2])
    );
    assert_eq!(
        verified["groups"]["1"]["required_majorities"],
        serde_json::json!([3])
    );
    assert_eq!(verified["maintenance_eligible"], true);
    assert_eq!(verified["disruption_authorized"], false);
    assert!(verified["process_incarnations"].get("7").is_some());
    cluster.processes.remove(&4);
    cluster.processes.remove(&5);
    let survivors = cluster.cli(&["verify-quorum", "--exclude", "4,5"]).await;
    assert_eq!(
        survivors["groups"]["1"]["observed_per_set"],
        serde_json::json!([3])
    );
    assert_eq!(
        survivors["groups"]["1"]["configuration"]["voter_sets"],
        serde_json::json!([[1, 2, 4, 5, 7]])
    );
    assert_eq!(survivors["groups"]["1"]["full_redundancy_observed"], false);
    assert_eq!(survivors["maintenance_eligible"], false);
    let insufficient = cluster
        .cli_output(&["verify-quorum", "--exclude", "2,4,5"])
        .await;
    assert!(!insufficient.status.success());
    assert!(String::from_utf8_lossy(&insufficient.stderr).contains("constituent group quorum"));
    cluster.start(4, "rf5-survivor-return");
    cluster.start(5, "rf5-survivor-return");
    cluster.ready().await;
    // Both possible old leaders (1 and 2) are removed. Node 7 is the only
    // retained voter, so the supported executor hands leadership to it.
    let id = cluster
        .submit("gateway-new-node-leader", 1, "4,5,7", None)
        .await;
    cluster.wait(id).await;
    assert_eq!(cluster.configuration(0, BTreeSet::from([4, 5, 7])).await, 7);
    cluster.ready().await;
    for name in &names {
        cluster
            .gateway_payload(
                &mut gateway,
                &gateway_origin,
                name,
                "acknowledged-before-node-join",
                false,
            )
            .await;
    }
    let new_name = (0..100)
        .map(|n| format!("gateway-new-leader-{n}"))
        .find(|name| {
            map.locate(&BucketStreamId::new("benchcmp", name.clone()))
                .raft_group_id
                == RaftGroupId(0)
        })
        .unwrap();
    cluster
        .gateway_payload(
            &mut gateway,
            &gateway_origin,
            &new_name,
            "gateway-new-node-write",
            true,
        )
        .await;
    cluster
        .gateway_payload(
            &mut gateway,
            &gateway_origin,
            &new_name,
            "gateway-new-node-write",
            false,
        )
        .await;
    // Meta loses its majority while RF3 and RF5 retain their data majorities.
    let (mut periodic, periodic_origin) = cluster.start_gateway(100).await;
    cluster
        .gateway_payload(
            &mut periodic,
            &periodic_origin,
            &new_name,
            "gateway-new-node-write",
            false,
        )
        .await;
    let log_offset = std::fs::read(&periodic.log).unwrap().len();
    cluster.processes.remove(&1);
    cluster.processes.remove(&2);
    let view = cluster.configs[&3]
        .control
        .as_ref()
        .unwrap()
        .local_identity(&cluster.configs[&3])
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        if ursula_raft::read_control_projection(
            &view.cluster,
            3,
            &cluster.nodes[&3].cluster_url,
            Duration::from_secs(2),
        )
        .await
        .is_err()
        {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "metadata minority still returned a fresh projection"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    loop {
        let log = std::fs::read(&periodic.log).unwrap();
        if String::from_utf8_lossy(&log[log_offset..])
            .contains("gateway retains last managed routing directory")
        {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "periodic gateway did not observe unavailable meta quorum"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    for name in &names {
        cluster
            .gateway_payload(
                &mut gateway,
                &gateway_origin,
                name,
                "acknowledged-before-node-join",
                false,
            )
            .await;
    }
    cluster
        .gateway_payload(
            &mut gateway,
            &gateway_origin,
            &new_name,
            "gateway-new-node-write",
            false,
        )
        .await;
    let minority_name = (0..100)
        .map(|n| format!("gateway-meta-minority-{n}"))
        .find(|name| {
            map.locate(&BucketStreamId::new("benchcmp", name.clone()))
                .raft_group_id
                == RaftGroupId(0)
        })
        .unwrap();
    cluster
        .gateway_payload(
            &mut periodic,
            &periodic_origin,
            &minority_name,
            "gateway-write-under-meta-minority",
            true,
        )
        .await;
    cluster
        .gateway_payload(
            &mut periodic,
            &periodic_origin,
            &minority_name,
            "gateway-write-under-meta-minority",
            false,
        )
        .await;
    cluster
        .gateway_payload(
            &mut periodic,
            &periodic_origin,
            &names[1],
            "acknowledged-before-node-join",
            false,
        )
        .await;
    cluster.start(1, "meta-quorum-return");
    cluster.start(2, "meta-quorum-return");
    cluster.ready().await;
    let before = cluster.view().await;
    cluster.processes.clear();
    for id in 1..=7 {
        cluster.start(id, "joined-cluster-restart");
    }
    cluster.ready().await;
    assert_eq!(cluster.view().await.state, before.state);
    cluster.configuration(0, BTreeSet::from([4, 5, 7])).await;
    cluster
        .configuration(1, BTreeSet::from([1, 2, 4, 5, 7]))
        .await;
    for name in &names {
        cluster
            .payloads(name, "acknowledged-before-node-join")
            .await;
    }
    cluster
        .gateway_payload(
            &mut gateway,
            &gateway_origin,
            &new_name,
            "gateway-new-node-write",
            false,
        )
        .await;
    cluster
        .gateway_payload(
            &mut periodic,
            &periodic_origin,
            &minority_name,
            "gateway-write-under-meta-minority",
            false,
        )
        .await;
    cluster
        .write("after-new-node-restart", "acknowledged-after-node-join")
        .await;
    cluster
        .payloads("after-new-node-restart", "acknowledged-after-node-join")
        .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn binaries_resume_migration_after_controller_and_destination_restart_and_rf_changes() {
    let mut cluster = Cluster::new().await;
    let map = StaticShardMap::new(1, 2).unwrap();
    let names = [0, 1].map(|group| {
        (0..100)
            .map(|n| format!("binary-group-{group}-{n}"))
            .find(|name| {
                map.locate(&BucketStreamId::new("benchcmp", name.clone()))
                    .raft_group_id
                    == RaftGroupId(group)
            })
            .unwrap()
    });
    cluster
        .write(&names[0], "acknowledged-before-migration")
        .await;
    cluster
        .write(&names[1], "neighbor-group-stays-readable")
        .await;
    cluster.processes.remove(&5);
    let id = cluster.submit("binary-replacement", 0, "1,3,5", None).await;
    let deadline = Instant::now() + Duration::from_secs(20);
    let owner = loop {
        let view = cluster.view().await;
        let migration = &view.state.migrations[&id];
        assert!(migration.is_running());
        if let Some(assignment) = &migration.managed.as_ref().unwrap().executor {
            break assignment.token.clone();
        }
        assert!(
            Instant::now() < deadline,
            "executor did not claim accepted intent"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    cluster.processes.remove(&owner.executor.node_id);
    // Keep both processes down until a new real meta leader takes ownership.
    // RF3 retains two source voters; the RF5 neighbor retains three.
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let view = cluster.view().await;
        let current = view.state.migrations[&id]
            .managed
            .as_ref()
            .unwrap()
            .executor
            .as_ref()
            .unwrap();
        if current.token.generation > owner.generation {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "executor generation did not advance after controller loss"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let during_names = [0, 1].map(|group| {
        (0..100)
            .map(|n| format!("fault-group-{group}-{n}"))
            .find(|name| {
                map.locate(&BucketStreamId::new("benchcmp", name.clone()))
                    .raft_group_id
                    == RaftGroupId(group)
            })
            .unwrap()
    });
    for name in &during_names {
        cluster
            .write(name, "acknowledged-during-controller-loss")
            .await;
    }
    cluster.start(owner.executor.node_id, "controller-restart");
    cluster.start(5, "destination-restart");
    cluster.wait(id).await;
    assert_eq!(
        cluster.submit("binary-replacement", 0, "1,3,5", None).await,
        id
    );
    let view = cluster.view().await;
    assert_eq!(view.state.migrations[&id].phase, MigrationPhase::Succeeded);
    let managed = view.state.migrations[&id].managed.as_ref().unwrap();
    assert!(managed.executor.as_ref().unwrap().token.generation > owner.generation);
    assert_ne!(
        managed.receivers[&owner.executor.node_id],
        owner.executor.incarnation
    );
    assert_eq!(
        view.state.placements[&RaftGroupId(0)].voters,
        BTreeSet::from([1, 3, 5])
    );
    cluster.ready().await;
    cluster
        .payloads(&names[0], "acknowledged-before-migration")
        .await;
    cluster
        .payloads(&names[1], "neighbor-group-stays-readable")
        .await;
    for (epoch, rf, voters) in [(1, "5", "1,2,3,4,5"), (2, "3", "2,4,6")] {
        let id = cluster
            .submit(&format!("binary-policy-{epoch}"), epoch, voters, Some(rf))
            .await;
        cluster.wait(id).await;
        cluster.ready().await;
        cluster
            .payloads(&names[0], "acknowledged-before-migration")
            .await;
        cluster
            .payloads(&names[1], "neighbor-group-stays-readable")
            .await;
    }
    let before = cluster.view().await;
    for name in &during_names {
        cluster
            .payloads(name, "acknowledged-during-controller-loss")
            .await;
    }
    assert_eq!(
        before.state.placements[&RaftGroupId(0)].voters,
        BTreeSet::from([2, 4, 6])
    );
    assert_eq!(before.state.placements[&RaftGroupId(0)].epoch, 3);
    cluster.processes.clear();
    for id in 1..=6 {
        cluster.start(id, "settled-restart");
    }
    cluster.ready().await;
    assert_eq!(cluster.view().await.state, before.state);
    for name in &during_names {
        cluster
            .payloads(name, "acknowledged-during-controller-loss")
            .await;
    }
    cluster
        .payloads(&names[0], "acknowledged-before-migration")
        .await;
    cluster
        .payloads(&names[1], "neighbor-group-stays-readable")
        .await;
    let new_name = (0..100)
        .map(|n| format!("post-migration-{n}"))
        .find(|name| {
            map.locate(&BucketStreamId::new("benchcmp", name.clone()))
                .raft_group_id
                == RaftGroupId(0)
        })
        .unwrap();
    cluster
        .write(&new_name, "acknowledged-after-settled-restart")
        .await;
    cluster
        .payloads(&new_name, "acknowledged-after-settled-restart")
        .await;
}

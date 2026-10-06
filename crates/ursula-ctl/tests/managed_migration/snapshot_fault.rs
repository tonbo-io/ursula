//! Opt-in native-process S3 snapshot fault acceptance. MinIO is an actual S3
//! server, not an in-memory SnapshotStore. This does not certify AWS behavior.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::process::Command;
use std::process::Stdio;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU32;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::time::Instant;

use axum::body::Body;
use axum::body::to_bytes;
use axum::extract::Request;
use axum::extract::State;
use axum::http::Method;
use axum::response::Response;
use tokio::sync::Notify;
use ursula_config::S3Config;
use ursula_config::S3ServerSideEncryption;
use ursula_control::NodeRegistration;
use ursula_control::ReceiverLedger;
use ursula_control::ReplicaAssignmentPhase;
use ursula_shard::BucketStreamId;
use ursula_shard::RaftGroupId;
use ursula_shard::StaticShardMap;

use super::Cluster;
use super::Process;
use super::port;

const TEST_USER: &str = "ursula-snapshot-test";
const TEST_PASSWORD: &str = "ursula-snapshot-test-password";

struct Minio {
    process: Process,
    _root: tempfile::TempDir,
    storage: S3Config,
}

impl Minio {
    fn operator(&self, root: &str) -> opendal::Operator {
        let builder = opendal::services::S3::default()
            .bucket(self.storage.bucket.as_deref().unwrap())
            .region("us-east-1")
            .endpoint(self.storage.endpoint.as_deref().unwrap())
            .root(root)
            .access_key_id(TEST_USER)
            .secret_access_key(TEST_PASSWORD);
        opendal::Operator::new(builder).unwrap().finish().layer(
            opendal::layers::TimeoutLayer::new()
                .with_timeout(Duration::from_secs(10))
                .with_io_timeout(Duration::from_secs(10)),
        )
    }

    async fn start() -> Self {
        let root = tempfile::tempdir().unwrap();
        let endpoint = format!("http://127.0.0.1:{}", port());
        let console = format!("127.0.0.1:{}", port());
        let log = root.path().join("minio.log");
        let file = std::fs::File::create(&log).unwrap();
        let binary = std::env::var_os("URSULA_MINIO_BINARY").unwrap_or_else(|| "minio".into());
        let child = Command::new(binary)
            .arg("server")
            .arg("--address")
            .arg(endpoint.trim_start_matches("http://"))
            .arg("--console-address")
            .arg(console)
            .arg(root.path().join("data"))
            .env("MINIO_ROOT_USER", TEST_USER)
            .env("MINIO_ROOT_PASSWORD", TEST_PASSWORD)
            .stdout(Stdio::from(file.try_clone().unwrap()))
            .stderr(Stdio::from(file))
            .spawn()
            .expect("native MinIO required (or set URSULA_MINIO_BINARY)");
        let mut process = Process { child, log };
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(2))
            .build()
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            assert!(
                process.child.try_wait().unwrap().is_none(),
                "MinIO exited: {}",
                std::fs::read_to_string(&process.log).unwrap()
            );
            if client
                .get(format!("{endpoint}/minio/health/ready"))
                .send()
                .await
                .is_ok_and(|response| response.status().is_success())
            {
                break;
            }
            assert!(Instant::now() < deadline, "MinIO startup timeout");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        let aws = std::env::var_os("URSULA_AWS_CLI").unwrap_or_else(|| "aws".into());
        let bucket = "ursula-managed-snapshot-test";
        let mut command = tokio::process::Command::new(aws);
        command
            .args([
                "--endpoint-url",
                &endpoint,
                "--region",
                "us-east-1",
                "s3api",
                "create-bucket",
                "--bucket",
                bucket,
            ])
            .env("AWS_ACCESS_KEY_ID", TEST_USER)
            .env("AWS_SECRET_ACCESS_KEY", TEST_PASSWORD)
            .env_remove("AWS_SESSION_TOKEN")
            .env_remove("AWS_PROFILE")
            .env_remove("AWS_WEB_IDENTITY_TOKEN_FILE")
            .env("AWS_EC2_METADATA_DISABLED", "true")
            .env("AWS_PAGER", "")
            .kill_on_drop(true);
        let result = tokio::time::timeout(Duration::from_secs(20), command.output())
            .await
            .expect("local test bucket creation timeout")
            .expect("AWS CLI required to create the local test bucket");
        assert!(
            result.status.success(),
            "create local bucket: {}",
            String::from_utf8_lossy(&result.stderr)
        );
        Self {
            process,
            _root: root,
            storage: S3Config {
                bucket: Some(bucket.to_owned()),
                region: Some("us-east-1".to_owned()),
                endpoint: Some(endpoint),
                access_key_id: Some(TEST_USER.to_owned()),
                secret_access_key: Some(TEST_PASSWORD.to_owned()),
                server_side_encryption: S3ServerSideEncryption::None,
                ..Default::default()
            },
        }
    }
}

struct SnapshotGate {
    group: AtomicU32,
    blocked: AtomicBool,
    entered: Notify,
    release: Notify,
    paths: Mutex<Vec<String>>,
    endpoint: String,
    client: reqwest::Client,
}

impl SnapshotGate {
    fn pause(&self, group: u32) {
        self.group.store(group, Ordering::SeqCst);
        self.paths.lock().unwrap().clear();
        self.blocked.store(true, Ordering::SeqCst);
    }
    fn resume(&self) {
        self.blocked.store(false, Ordering::SeqCst);
        self.release.notify_waiters();
    }
}

struct Proxy {
    task: tokio::task::JoinHandle<()>,
    gate: Arc<SnapshotGate>,
    endpoint: String,
}

impl Drop for Proxy {
    fn drop(&mut self) {
        self.gate.resume();
        self.task.abort();
    }
}

impl Proxy {
    async fn start(endpoint: String) -> Self {
        let gate = Arc::new(SnapshotGate {
            group: AtomicU32::new(0),
            blocked: AtomicBool::new(false),
            entered: Notify::new(),
            release: Notify::new(),
            paths: Mutex::new(Vec::new()),
            endpoint,
            client: reqwest::Client::builder()
                .redirect(reqwest::redirect::Policy::none())
                .timeout(Duration::from_secs(10))
                .build()
                .unwrap(),
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let router = axum::Router::new()
            .fallback(forward)
            .with_state(gate.clone());
        let task = tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });
        Self {
            task,
            gate,
            endpoint,
        }
    }
}

async fn forward(State(gate): State<Arc<SnapshotGate>>, request: Request) -> Response {
    let (parts, body) = request.into_parts();
    let wait = gate.release.notified();
    let path = parts.uri.path();
    if parts.method == Method::GET
        && path.ends_with(".snap")
        && path.contains(&format!("/group-{}/", gate.group.load(Ordering::SeqCst)))
        && gate.blocked.load(Ordering::SeqCst)
    {
        gate.paths.lock().unwrap().push(path.to_owned());
        gate.entered.notify_one();
        wait.await;
    }
    let body = to_bytes(body, 64 * 1024 * 1024).await.unwrap();
    // Preserve the signed Host header. MinIO validates the original SigV4
    // request; forwarding only changes the transport destination.
    let result = gate
        .client
        .request(parts.method, format!("{}{}", gate.endpoint, parts.uri))
        .headers(parts.headers)
        .body(body)
        .send()
        .await;
    match result {
        Ok(response) => {
            let status = response.status();
            let headers = response.headers().clone();
            let bytes = response.bytes().await.unwrap();
            let mut output = Response::new(Body::from(bytes));
            *output.status_mut() = status;
            *output.headers_mut() = headers;
            output
        }
        Err(error) => Response::builder()
            .status(502)
            .body(Body::from(error.to_string()))
            .unwrap(),
    }
}

async fn receiver(cluster: &Cluster, id: u64) -> ReceiverLedger {
    cluster
        .client
        .get(format!(
            "{}/__ursula/control/receiver",
            cluster.nodes[&id].admin_url
        ))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap()
}

async fn incarnation(cluster: &Cluster, id: u64) -> serde_json::Value {
    let status: serde_json::Value = cluster
        .client
        .get(format!(
            "{}/__ursula/control/receiver/process",
            cluster.nodes[&id].admin_url
        ))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    status["process"].clone()
}

async fn references_settled(store: &opendal::Operator, group: u32, voters: &BTreeSet<u64>) {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let mut unresolved = Vec::new();
        for node in 1..=7 {
            let path = format!("snapshots/group-{group}/references/node-{node}.json");
            let reference = match store.read(&path).await {
                Ok(bytes) => {
                    Some(serde_json::from_slice::<serde_json::Value>(&bytes.to_vec()).unwrap())
                }
                Err(error) if error.kind() == opendal::ErrorKind::NotFound => None,
                Err(error) => panic!("read reference: {error}"),
            };
            let key = reference.as_ref().and_then(|reference| {
                assert_eq!(reference["version"], ursula_stream::FORMAT_EPOCH);
                assert_eq!(reference["node_id"], node);
                assert_eq!(reference["raft_group_id"], group);
                reference["snapshot_key"].as_str()
            });
            if voters.contains(&node) && key.is_none() || !voters.contains(&node) && key.is_some() {
                unresolved.push(format!("node {node} reference {reference:?}"));
            }
            if let Some(key) = key {
                assert!(key.starts_with(&format!("snapshots/group-{group}/objects/")));
                assert!(store.stat(key).await.unwrap().content_length() > 0);
            }
            let pins = store
                .list_with(&format!("snapshots/group-{group}/references/pins/{node}/"))
                .recursive(true)
                .await
                .unwrap();
            let mut count = 0;
            for pin in pins
                .into_iter()
                .filter(|entry| entry.metadata().mode().is_file())
            {
                count += 1;
                let bytes = store.read(pin.path()).await.unwrap();
                let pin: serde_json::Value = serde_json::from_slice(&bytes.to_vec()).unwrap();
                assert_eq!(pin["node_id"], node);
                assert_eq!(pin["raft_group_id"], group);
                if !voters.contains(&node) || pin["snapshot_key"].as_str() != key {
                    unresolved.push(format!("node {node} abandoned pin {pin:?}"));
                }
            }
            if count > usize::from(voters.contains(&node)) {
                unresolved.push(format!("node {node} retains {count} pins"));
            }
        }
        if unresolved.is_empty() {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "snapshot reference cleanup: {unresolved:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires native MinIO and AWS CLI; run explicitly with --ignored"]
async fn binaries_resume_rf3_rf5_after_s3_snapshot_download_and_controller_crashes() {
    let mut minio = Minio::start().await;
    let mut cluster = Cluster::new_with_s3(Some((
        minio.storage.clone(),
        "managed-snapshot-fault".to_owned(),
    )))
    .await;
    let proxy = Proxy::start(minio.storage.endpoint.clone().unwrap()).await;
    let store = minio.operator("managed-snapshot-fault");
    let map = StaticShardMap::new(1, 2).unwrap();
    let mut payloads = Vec::new();
    for group in 0..2 {
        // At least eight commands per group force external snapshots and purge
        // the prefix before a new learner can receive it through ordinary logs.
        for ordinal in 0..8 {
            let name = (0..100)
                .map(|salt| format!("snapshot-pre-{group}-{ordinal}-{salt}"))
                .find(|name| {
                    map.locate(&BucketStreamId::new("benchcmp", name.clone()))
                        .raft_group_id
                        == RaftGroupId(group)
                })
                .unwrap();
            let body = format!("acknowledged-before-snapshot-{group}-{ordinal}");
            cluster.write(&name, &body).await;
            payloads.push((name, body));
        }
    }
    let node = NodeRegistration {
        node_id: 7,
        client_url: format!("http://127.0.0.1:{}", port()),
        cluster_url: format!("http://127.0.0.1:{}", port()),
        admin_url: format!("http://127.0.0.1:{}", port()),
        labels: BTreeMap::from([("zone".to_owned(), "2".to_owned())]),
    };
    assert!(cluster.register(&node).await.status.success());
    cluster.provision(node);
    cluster
        .configs
        .get_mut(&7)
        .unwrap()
        .storage
        .cold
        .s3
        .as_mut()
        .unwrap()
        .endpoint = Some(proxy.endpoint.clone());
    cluster.start(7, "snapshot-destination-join");
    cluster.ready().await;
    for (group, voters) in [(0, "1,2,7"), (1, "1,2,4,5,7")] {
        proxy.gate.pause(group);
        let id = cluster
            .submit_group(group, &format!("snapshot-fault-{group}"), 0, voters, None)
            .await;
        tokio::time::timeout(Duration::from_secs(40), async {
            loop {
                let entered = proxy.gate.entered.notified();
                if !proxy.gate.paths.lock().unwrap().is_empty() {
                    break;
                }
                entered.await;
            }
        })
        .await
        .expect("destination never downloaded a purged-prefix S3 snapshot");
        assert!(!proxy.gate.paths.lock().unwrap().is_empty());
        let download_paths = proxy.gate.paths.lock().unwrap().clone();
        let pins = store
            .list_with(&format!("snapshots/group-{group}/references/pins/7/"))
            .recursive(true)
            .await
            .unwrap();
        let mut protected = false;
        for entry in pins
            .into_iter()
            .filter(|entry| entry.metadata().mode().is_file())
        {
            let pin: serde_json::Value =
                serde_json::from_slice(&store.read(entry.path()).await.unwrap().to_vec()).unwrap();
            let key = pin["snapshot_key"].as_str().unwrap();
            if download_paths.iter().any(|path| path.ends_with(key)) {
                assert!(store.stat(key).await.unwrap().content_length() > 0);
                protected = true;
            }
        }
        assert!(
            protected,
            "snapshot GET started without a durable target pin"
        );
        let view = cluster.view().await;
        let migration = &view.state.migrations[&id];
        assert!(migration.is_running());
        let managed = migration.managed.as_ref().unwrap();
        assert!(managed.catchup_prefix.is_some());
        let token = managed.executor.as_ref().unwrap().token.clone();
        let old_process = incarnation(&cluster, 7).await;
        assert_eq!(
            receiver(&cluster, 7).await.assignments[&RaftGroupId(group)].phase,
            ReplicaAssignmentPhase::Hosted
        );
        cluster.processes.remove(&7);
        let controller = (group == 0).then_some(token.executor.node_id);
        if let Some(controller) = controller {
            cluster.processes.remove(&controller);
        }
        if controller.is_some() {
            let deadline = Instant::now() + Duration::from_secs(20);
            loop {
                let fresh = cluster.view().await;
                if fresh.state.migrations[&id]
                    .managed
                    .as_ref()
                    .unwrap()
                    .executor
                    .as_ref()
                    .unwrap()
                    .token
                    .generation
                    > token.generation
                {
                    break;
                }
                assert!(
                    Instant::now() < deadline,
                    "no controller takeover during snapshot install"
                );
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
        let name = (0..100)
            .map(|salt| format!("snapshot-fault-write-{group}-{salt}"))
            .find(|name| {
                map.locate(&BucketStreamId::new("benchcmp", name.clone()))
                    .raft_group_id
                    == RaftGroupId(group)
            })
            .unwrap();
        let body = format!("acknowledged-during-snapshot-fault-{group}");
        cluster.write(&name, &body).await;
        payloads.push((name, body));
        cluster.start(7, "snapshot-destination-restart");
        if let Some(controller) = controller {
            cluster.start(controller, "snapshot-controller-restart");
        }
        proxy.gate.resume();
        cluster.wait(id).await;
        cluster.ready().await;
        let settled = cluster.view().await;
        assert_eq!(
            settled.state.migrations[&id]
                .managed
                .as_ref()
                .unwrap()
                .published_epoch,
            Some(1)
        );
        assert!(
            settled.state.migrations[&id]
                .managed
                .as_ref()
                .unwrap()
                .executor
                .as_ref()
                .unwrap()
                .token
                .generation
                > token.generation
        );
        assert_ne!(incarnation(&cluster, 7).await, old_process);
        cluster
            .configuration(
                group,
                voters.split(',').map(|id| id.parse().unwrap()).collect(),
            )
            .await;
        references_settled(
            &store,
            group,
            &voters.split(',').map(|id| id.parse().unwrap()).collect(),
        )
        .await;
        for (name, body) in &payloads {
            cluster.payloads(name, body).await;
        }
    }
    let before = cluster.view().await;
    cluster.processes.clear();
    for id in 1..=7 {
        cluster.start(id, "snapshot-final-restart");
    }
    cluster.ready().await;
    assert_eq!(cluster.view().await.state, before.state);
    references_settled(&store, 0, &BTreeSet::from([1, 2, 7])).await;
    references_settled(&store, 1, &BTreeSet::from([1, 2, 4, 5, 7])).await;
    for (name, body) in &payloads {
        cluster.payloads(name, body).await;
    }
    let evidence = cluster.cli(&["verify-quorum"]).await;
    assert_eq!(evidence["maintenance_eligible"], true);
    assert_eq!(
        evidence["groups"]["0"]["configuration"]["voter_sets"],
        serde_json::json!([[1, 2, 7]])
    );
    assert_eq!(
        evidence["groups"]["1"]["configuration"]["voter_sets"],
        serde_json::json!([[1, 2, 4, 5, 7]])
    );
    cluster.processes.clear();
    assert!(minio.process.child.try_wait().unwrap().is_none());
}

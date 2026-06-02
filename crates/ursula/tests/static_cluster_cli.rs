use std::collections::HashSet;
use std::net::TcpListener;
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::sync::{Mutex, OnceLock};
use std::time::Duration;

use ursula_runtime::ColdStore;

struct ChildGuard {
    child: Child,
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cli_static_grpc_raft_cluster_forwards_follower_writes() {
    let Some(binary) = option_env!("CARGO_BIN_EXE_ursula") else {
        tracing::warn!("CARGO_BIN_EXE_ursula is not set; skipping CLI cluster smoke test");
        return;
    };
    let ports = [free_port(), free_port(), free_port()];
    let peers: Vec<(u64, String)> = ports
        .iter()
        .enumerate()
        .map(|(index, port)| {
            (
                u64::try_from(index + 1).expect("node id fits u64"),
                format!("http://127.0.0.1:{port}"),
            )
        })
        .collect();

    let children = vec![
        spawn_node(binary, 2, ports[1], &peers, false),
        spawn_node(binary, 3, ports[2], &peers, false),
        spawn_node(binary, 1, ports[0], &peers, true),
    ];

    let client = reqwest::Client::new();
    for (_, base_url) in &peers {
        wait_until_ready(&client, base_url).await;
    }

    let response = put_with_body_until_created(
        &client,
        &format!("{}/benchcmp/cli-follower-forward", peers[1].1),
        "cli-forward-payload",
    )
    .await;
    assert_eq!(
        response
            .headers()
            .get("stream-next-offset")
            .and_then(|value| value.to_str().ok()),
        Some("00000000000000000019")
    );

    let payload = read_until_replicated(
        &client,
        &format!(
            "{}/benchcmp/cli-follower-forward?offset=0&max_bytes=64",
            peers[2].1
        ),
    )
    .await;
    assert_eq!(payload, b"cli-forward-payload");

    drop(children);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cli_static_grpc_raft_log_dir_recovers_after_restart() {
    let Some(binary) = option_env!("CARGO_BIN_EXE_ursula") else {
        tracing::warn!("CARGO_BIN_EXE_ursula is not set; skipping CLI durable restart smoke test");
        return;
    };
    let port = free_port();
    let base_url = format!("http://127.0.0.1:{port}");
    let root = std::env::temp_dir().join(format!(
        "ursula-cli-durable-restart-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system time after unix epoch")
            .as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).expect("create temp root");
    let config_path = root.join("cluster.json");
    let log_dir = root.join("raft-log");

    write_single_node_cluster_config(&config_path, &base_url, true);
    {
        let _child = spawn_node_with_cluster_config(binary, port, 1, &config_path, &log_dir);
        let client = reqwest::Client::new();
        wait_until_ready(&client, &base_url).await;
        put_until_created(&client, &format!("{base_url}/benchcmp/cli-durable-restart")).await;
        post_until_no_content(
            &client,
            &format!("{base_url}/benchcmp/cli-durable-restart"),
            "cli-durable-payload",
        )
        .await;
    }

    let journal_path = log_dir.join("core-0").join("journal.bin");
    assert!(journal_path.exists(), "core journal should exist");
    assert!(
        std::fs::metadata(&journal_path)
            .expect("core journal metadata")
            .len()
            > 0,
        "core journal should contain records"
    );

    write_single_node_cluster_config(&config_path, &base_url, false);
    {
        let _child = spawn_node_with_cluster_config(binary, port, 1, &config_path, &log_dir);
        let client = reqwest::Client::new();
        wait_until_ready(&client, &base_url).await;
        let payload = read_until_replicated(
            &client,
            &format!("{base_url}/benchcmp/cli-durable-restart?offset=0&max_bytes=64"),
        )
        .await;
        assert_eq!(payload, b"cli-durable-payload");
    }

    std::fs::remove_dir_all(&root).expect("remove temp root");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cli_static_grpc_raft_log_dir_replicates_between_nodes() {
    let Some(binary) = option_env!("CARGO_BIN_EXE_ursula") else {
        tracing::warn!("CARGO_BIN_EXE_ursula is not set; skipping CLI durable cluster smoke test");
        return;
    };
    let ports = [free_port(), free_port(), free_port()];
    let peers: Vec<(u64, String)> = ports
        .iter()
        .enumerate()
        .map(|(index, port)| {
            (
                u64::try_from(index + 1).expect("node id fits u64"),
                format!("http://127.0.0.1:{port}"),
            )
        })
        .collect();
    let root = std::env::temp_dir().join(format!(
        "ursula-cli-durable-cluster-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system time after unix epoch")
            .as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).expect("create temp root");

    let mut configs = Vec::new();
    for (node_id, _) in &peers {
        let config_path = root.join(format!("node-{node_id}.json"));
        write_cluster_config(&config_path, *node_id, &peers, *node_id == 1);
        configs.push(config_path);
    }

    let children = vec![
        spawn_node_with_cluster_config(binary, ports[1], 4, &configs[1], &root.join("node-2-log")),
        spawn_node_with_cluster_config(binary, ports[2], 4, &configs[2], &root.join("node-3-log")),
        spawn_node_with_cluster_config(binary, ports[0], 4, &configs[0], &root.join("node-1-log")),
    ];

    let client = reqwest::Client::new();
    for (_, base_url) in &peers {
        wait_until_ready(&client, base_url).await;
    }
    put_until_created(
        &client,
        &format!("{}/benchcmp/cli-durable-cluster", peers[0].1),
    )
    .await;
    post_until_no_content(
        &client,
        &format!("{}/benchcmp/cli-durable-cluster", peers[0].1),
        "cli-durable-cluster-payload",
    )
    .await;

    let payload = read_until_replicated(
        &client,
        &format!(
            "{}/benchcmp/cli-durable-cluster?offset=0&max_bytes=64",
            peers[2].1
        ),
    )
    .await;
    assert_eq!(payload, b"cli-durable-cluster-payload");

    // raft replication is acked once a quorum (leader + one follower) has the
    // entry. The remaining follower may still be flushing to its journal when
    // the client-side read on peers[2] returns — particularly on slower CI
    // disks. Poll for the journal up to ~5s per node before asserting, so the
    // test only fails when a node truly never persists, not when it persists
    // a beat later than the read.
    for node_id in 1..=3 {
        let journal_path = root
            .join(format!("node-{node_id}-log"))
            .join("core-0")
            .join("journal.bin");
        let mut last_len = 0u64;
        for _ in 0..100 {
            if journal_path.exists() {
                last_len = std::fs::metadata(&journal_path)
                    .expect("node journal metadata")
                    .len();
                if last_len > 0 {
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert!(
            journal_path.exists(),
            "node {node_id} journal should exist after polling for ~5s",
        );
        assert!(
            last_len > 0,
            "node {node_id} journal should contain records after polling for ~5s (saw len={last_len})",
        );
    }

    drop(children);
    std::fs::remove_dir_all(&root).expect("remove temp root");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cli_static_grpc_raft_log_dir_installs_snapshot_for_late_learner() {
    let Some(binary) = option_env!("CARGO_BIN_EXE_ursula") else {
        tracing::warn!(
            "CARGO_BIN_EXE_ursula is not set; skipping CLI durable late learner smoke test"
        );
        return;
    };
    let ports = [free_port(), free_port(), free_port()];
    let peers: Vec<(u64, String)> = ports
        .iter()
        .enumerate()
        .map(|(index, port)| {
            (
                u64::try_from(index + 1).expect("node id fits u64"),
                format!("http://127.0.0.1:{port}"),
            )
        })
        .collect();
    let initial_peers = peers[..2].to_vec();
    let root = std::env::temp_dir().join(format!(
        "ursula-cli-durable-late-learner-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system time after unix epoch")
            .as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).expect("create temp root");

    let node1_config = root.join("node-1.json");
    let node2_config = root.join("node-2.json");
    let node3_config = root.join("node-3.json");
    write_cluster_config(&node1_config, 1, &initial_peers, true);
    write_cluster_config(&node2_config, 2, &initial_peers, false);
    write_cluster_config(&node3_config, 3, &peers, false);

    let mut children = vec![
        spawn_node_with_cluster_config(
            binary,
            ports[1],
            1,
            &node2_config,
            &root.join("node-2-log"),
        ),
        spawn_node_with_cluster_config(
            binary,
            ports[0],
            1,
            &node1_config,
            &root.join("node-1-log"),
        ),
    ];

    let client = reqwest::Client::new();
    wait_until_ready(&client, &peers[0].1).await;
    wait_until_ready(&client, &peers[1].1).await;
    put_until_created(
        &client,
        &format!("{}/benchcmp/cli-late-learner", peers[0].1),
    )
    .await;
    post_until_no_content(
        &client,
        &format!("{}/benchcmp/cli-late-learner", peers[0].1),
        "cli-late-learner-payload",
    )
    .await;
    let follower_payload = read_until_replicated(
        &client,
        &format!(
            "{}/benchcmp/cli-late-learner?offset=0&max_bytes=64",
            peers[1].1
        ),
    )
    .await;
    assert_eq!(follower_payload, b"cli-late-learner-payload");

    let snapshot = client
        .post(format!("{}/__ursula/raft/0/snapshot", peers[0].1))
        .send()
        .await
        .expect("trigger leader snapshot");
    assert_eq!(snapshot.status(), reqwest::StatusCode::OK);
    let snapshot_body = snapshot.text().await.expect("snapshot response body");
    let snapshot_body: serde_json::Value =
        serde_json::from_str(&snapshot_body).expect("parse snapshot response");
    let snapshot_index = snapshot_body
        .get("snapshot_index")
        .and_then(serde_json::Value::as_u64)
        .expect("snapshot index");

    let purge = client
        .post(format!(
            "{}/__ursula/raft/0/purge?upto={snapshot_index}",
            peers[0].1
        ))
        .send()
        .await
        .expect("trigger leader purge");
    assert_eq!(purge.status(), reqwest::StatusCode::OK);

    children.push(spawn_node_with_cluster_config(
        binary,
        ports[2],
        1,
        &node3_config,
        &root.join("node-3-log"),
    ));
    wait_until_ready(&client, &peers[2].1).await;

    let add_learner = client
        .post(format!(
            "{}/__ursula/raft/0/learners/3?addr={}",
            peers[0].1, peers[2].1
        ))
        .send()
        .await
        .expect("add late learner");
    assert_eq!(add_learner.status(), reqwest::StatusCode::OK);

    wait_metrics_contains(
        &client,
        &peers[2].1,
        &format!("\"snapshot_index\":{snapshot_index}"),
    )
    .await;
    wait_metrics_contains_all(
        &client,
        &peers[2].1,
        &[
            format!("\"snapshot_index\":{snapshot_index}"),
            "\"learner_ids\":[3]".to_owned(),
        ],
    )
    .await;

    let late_payload = read_until_replicated(
        &client,
        &format!(
            "{}/benchcmp/cli-late-learner?offset=0&max_bytes=64",
            peers[2].1
        ),
    )
    .await;
    assert_eq!(late_payload, b"cli-late-learner-payload");

    drop(children);
    std::fs::remove_dir_all(&root).expect("remove temp root");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cli_static_grpc_raft_log_dir_recovers_replicated_s3_cold_manifest_after_restart() {
    if std::env::var("URSULA_COLD_S3_INTEGRATION").ok().as_deref() != Some("1") {
        tracing::warn!(
            "skipping CLI S3 cold-manifest restart integration; set URSULA_COLD_S3_INTEGRATION=1 and URSULA_COLD_S3_BUCKET"
        );
        return;
    }
    let Some(binary) = option_env!("CARGO_BIN_EXE_ursula") else {
        tracing::warn!(
            "CARGO_BIN_EXE_ursula is not set; skipping CLI S3 cold cluster restart smoke test"
        );
        return;
    };
    let suffix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system time after unix epoch")
        .as_nanos();
    let cold_root = format!("ursula-cli-s3-cold-restart/{suffix}");
    let cold_store =
        ColdStore::s3_from_env_with_root(Some(&cold_root)).expect("S3 cold store from env");
    cold_store
        .remove_all("")
        .await
        .expect("clear S3 cold test root before run");

    let ports = [free_port(), free_port(), free_port()];
    let peers: Vec<(u64, String)> = ports
        .iter()
        .enumerate()
        .map(|(index, port)| {
            (
                u64::try_from(index + 1).expect("node id fits u64"),
                format!("http://127.0.0.1:{port}"),
            )
        })
        .collect();
    let root = std::env::temp_dir().join(format!(
        "ursula-cli-s3-cold-cluster-restart-{}-{suffix}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).expect("create temp root");

    let mut configs = Vec::new();
    for (node_id, _) in &peers {
        let config_path = root.join(format!("node-{node_id}.json"));
        write_cluster_config(&config_path, *node_id, &peers, *node_id == 1);
        configs.push(config_path);
    }

    {
        let children = vec![
            spawn_node_with_cluster_config_and_cold_s3(
                binary,
                ports[1],
                1,
                &configs[1],
                &root.join("node-2-log"),
                &cold_root,
            ),
            spawn_node_with_cluster_config_and_cold_s3(
                binary,
                ports[2],
                1,
                &configs[2],
                &root.join("node-3-log"),
                &cold_root,
            ),
            spawn_node_with_cluster_config_and_cold_s3(
                binary,
                ports[0],
                1,
                &configs[0],
                &root.join("node-1-log"),
                &cold_root,
            ),
        ];

        let client = reqwest::Client::new();
        for (_, base_url) in &peers {
            wait_until_ready(&client, base_url).await;
        }
        put_until_created(
            &client,
            &format!("{}/benchcmp/cli-s3-cold-restart", peers[0].1),
        )
        .await;
        post_until_no_content(
            &client,
            &format!("{}/benchcmp/cli-s3-cold-restart", peers[0].1),
            "cli-s3-cold-restart-payload",
        )
        .await;
        flush_stream_until_cold_hot_bytes_zero(
            &client,
            &peers[0].1,
            "benchcmp",
            "cli-s3-cold-restart",
        )
        .await;
        let payload = read_until_replicated(
            &client,
            &format!(
                "{}/benchcmp/cli-s3-cold-restart?offset=0&max_bytes=64",
                peers[2].1
            ),
        )
        .await;
        assert_eq!(payload, b"cli-s3-cold-restart-payload");
        drop(children);
    }

    for (node_id, _) in &peers {
        write_cluster_config(
            &configs[usize::try_from(*node_id - 1).expect("node index fits usize")],
            *node_id,
            &peers,
            false,
        );
    }

    {
        let children = vec![
            spawn_node_with_cluster_config_and_cold_s3(
                binary,
                ports[1],
                1,
                &configs[1],
                &root.join("node-2-log"),
                &cold_root,
            ),
            spawn_node_with_cluster_config_and_cold_s3(
                binary,
                ports[2],
                1,
                &configs[2],
                &root.join("node-3-log"),
                &cold_root,
            ),
            spawn_node_with_cluster_config_and_cold_s3(
                binary,
                ports[0],
                1,
                &configs[0],
                &root.join("node-1-log"),
                &cold_root,
            ),
        ];
        let client = reqwest::Client::new();
        for (_, base_url) in &peers {
            wait_until_ready(&client, base_url).await;
        }
        let payload = read_until_replicated(
            &client,
            &format!(
                "{}/benchcmp/cli-s3-cold-restart?offset=0&max_bytes=64",
                peers[2].1
            ),
        )
        .await;
        assert_eq!(payload, b"cli-s3-cold-restart-payload");
        drop(children);
    }

    cold_store
        .remove_all("")
        .await
        .expect("cleanup S3 cold test root");
    std::fs::remove_dir_all(&root).expect("remove temp root");
}

fn free_port() -> u16 {
    static RESERVED_PORTS: OnceLock<Mutex<HashSet<u16>>> = OnceLock::new();

    let reserved_ports = RESERVED_PORTS.get_or_init(|| Mutex::new(HashSet::new()));
    for _ in 0..100 {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind free port");
        let port = listener.local_addr().expect("local addr").port();
        let mut reserved_ports = reserved_ports.lock().expect("reserved port lock poisoned");
        if reserved_ports.insert(port) {
            return port;
        }
    }

    panic!("failed to reserve a unique local port");
}

fn spawn_node(
    binary: &str,
    node_id: u64,
    port: u16,
    peers: &[(u64, String)],
    init_membership: bool,
) -> ChildGuard {
    let mut command = Command::new(binary);
    command
        .arg("--listen")
        .arg(format!("127.0.0.1:{port}"))
        .arg("--core-count")
        .arg("1")
        .arg("--raft-group-count")
        .arg("4")
        .arg("--raft-memory")
        .arg("--raft-node-id")
        .arg(node_id.to_string())
        .env("URSULA_COLD_BACKEND", "memory")
        .env("URSULA_COLD_FLUSH_INTERVAL_MS", "0")
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    for (peer_id, peer_url) in peers {
        command
            .arg("--raft-peer")
            .arg(format!("{peer_id}={peer_url}"));
    }
    if init_membership {
        command.arg("--raft-init-membership");
    }
    ChildGuard {
        child: command.spawn().expect("spawn ursula node"),
    }
}

fn spawn_node_with_cluster_config(
    binary: &str,
    port: u16,
    raft_group_count: usize,
    config_path: &Path,
    log_dir: &Path,
) -> ChildGuard {
    let mut command = Command::new(binary);
    command
        .arg("--listen")
        .arg(format!("127.0.0.1:{port}"))
        .arg("--core-count")
        .arg("1")
        .arg("--raft-group-count")
        .arg(raft_group_count.to_string())
        .arg("--raft-log-dir")
        .arg(log_dir)
        .arg("--raft-cluster-config")
        .arg(config_path)
        .env("URSULA_COLD_BACKEND", "memory")
        .env("URSULA_COLD_FLUSH_INTERVAL_MS", "0")
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    ChildGuard {
        child: command.spawn().expect("spawn durable ursula node"),
    }
}

fn spawn_node_with_cluster_config_and_cold_s3(
    binary: &str,
    port: u16,
    raft_group_count: usize,
    config_path: &Path,
    log_dir: &Path,
    cold_root: &str,
) -> ChildGuard {
    let mut command = Command::new(binary);
    command
        .arg("--listen")
        .arg(format!("127.0.0.1:{port}"))
        .arg("--core-count")
        .arg("1")
        .arg("--raft-group-count")
        .arg(raft_group_count.to_string())
        .arg("--raft-log-dir")
        .arg(log_dir)
        .arg("--raft-cluster-config")
        .arg(config_path)
        .env("URSULA_COLD_BACKEND", "s3")
        .env("URSULA_COLD_ROOT", cold_root)
        .env("URSULA_COLD_FLUSH_INTERVAL_MS", "0")
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    for name in [
        "URSULA_COLD_S3_BUCKET",
        "URSULA_COLD_S3_REGION",
        "URSULA_COLD_S3_ENDPOINT",
        "URSULA_COLD_S3_ACCESS_KEY_ID",
        "URSULA_COLD_S3_SECRET_ACCESS_KEY",
        "URSULA_COLD_S3_SESSION_TOKEN",
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "AWS_SESSION_TOKEN",
        "AWS_REGION",
        "AWS_DEFAULT_REGION",
    ] {
        if let Ok(value) = std::env::var(name) {
            command.env(name, value);
        }
    }
    ChildGuard {
        child: command.spawn().expect("spawn durable S3 cold ursula node"),
    }
}

fn write_single_node_cluster_config(path: &Path, base_url: &str, init_membership: bool) {
    write_cluster_config(path, 1, &[(1, base_url.to_owned())], init_membership);
}

fn write_cluster_config(path: &Path, node_id: u64, peers: &[(u64, String)], init_membership: bool) {
    let peers = peers
        .iter()
        .map(|(node_id, url)| serde_json::json!({"node_id": node_id, "url": url}))
        .collect::<Vec<_>>();
    let body = serde_json::json!({
        "node_id": node_id,
        "init_membership": init_membership,
        "peers": peers
    });
    std::fs::write(
        path,
        serde_json::to_vec_pretty(&body).expect("encode cluster config"),
    )
    .expect("write cluster config");
}

async fn wait_until_ready(client: &reqwest::Client, base_url: &str) {
    let mut last_error = String::from("no attempts made");
    for _ in 0..300 {
        match client
            .get(format!("{base_url}/__ursula/metrics"))
            .send()
            .await
        {
            Ok(response) if response.status().is_success() => return,
            Ok(response) => {
                last_error = format!("HTTP {}", response.status());
            }
            Err(error) => {
                last_error = error.to_string();
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("node {base_url} did not become ready: {last_error}");
}

async fn put_until_created(client: &reqwest::Client, url: &str) {
    put_with_content_type_until_created(client, url, "text/plain").await;
}

async fn put_with_content_type_until_created(
    client: &reqwest::Client,
    url: &str,
    content_type: &'static str,
) {
    for _ in 0..100 {
        if let Ok(response) = client
            .put(url)
            .header("content-type", content_type)
            .send()
            .await
            && response.status() == reqwest::StatusCode::CREATED
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("create did not succeed at {url}");
}

async fn put_with_body_until_created(
    client: &reqwest::Client,
    url: &str,
    payload: &'static str,
) -> reqwest::Response {
    for _ in 0..100 {
        if let Ok(response) = client
            .put(url)
            .header("content-type", "text/plain")
            .body(payload)
            .send()
            .await
            && response.status() == reqwest::StatusCode::CREATED
        {
            return response;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("create with payload did not succeed at {url}");
}

async fn post_until_no_content(client: &reqwest::Client, url: &str, payload: &'static str) {
    for _ in 0..100 {
        if let Ok(response) = client
            .post(url)
            .header("content-type", "text/plain")
            .body(payload)
            .send()
            .await
            && response.status() == reqwest::StatusCode::NO_CONTENT
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("append did not succeed at {url}");
}

async fn read_until_replicated(client: &reqwest::Client, url: &str) -> Vec<u8> {
    for _ in 0..100 {
        if let Ok(response) = client.get(url).send().await
            && response.status().is_success()
        {
            let payload = response.bytes().await.expect("read replicated payload");
            if !payload.is_empty() {
                return payload.to_vec();
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("replicated payload did not become readable at {url}");
}

async fn wait_metrics_contains(client: &reqwest::Client, base_url: &str, needle: &str) -> String {
    let mut last = String::new();
    for _ in 0..100 {
        if let Ok(response) = client
            .get(format!("{base_url}/__ursula/metrics"))
            .send()
            .await
            && response.status().is_success()
        {
            last = response.text().await.expect("metrics body");
            if last.contains(needle) {
                return last;
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("metrics from {base_url} did not contain {needle}: {last}");
}

async fn wait_metrics_contains_all(
    client: &reqwest::Client,
    base_url: &str,
    needles: &[String],
) -> String {
    let mut last = String::new();
    for _ in 0..100 {
        if let Ok(response) = client
            .get(format!("{base_url}/__ursula/metrics"))
            .send()
            .await
            && response.status().is_success()
        {
            last = response.text().await.expect("metrics body");
            if needles.iter().all(|needle| last.contains(needle)) {
                return last;
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("metrics from {base_url} did not contain all {needles:?}: {last}");
}

async fn flush_stream_until_cold_hot_bytes_zero(
    client: &reqwest::Client,
    base_url: &str,
    bucket: &str,
    stream: &str,
) {
    for _ in 0..100 {
        let flush = client
            .post(format!(
                "{base_url}/__ursula/flush-cold/{bucket}/{stream}?min_hot_bytes=1&max_bytes=4"
            ))
            .send()
            .await
            .expect("send cold flush request");
        assert!(
            flush.status() == reqwest::StatusCode::OK
                || flush.status() == reqwest::StatusCode::NO_CONTENT,
            "unexpected cold flush status: {}",
            flush.status()
        );

        let metrics = client
            .get(format!("{base_url}/__ursula/metrics"))
            .send()
            .await
            .expect("read metrics")
            .text()
            .await
            .expect("metrics body");
        let metrics: serde_json::Value =
            serde_json::from_str(&metrics).expect("parse metrics json");
        if metrics
            .get("cold_hot_bytes")
            .and_then(serde_json::Value::as_u64)
            == Some(0)
        {
            return;
        }

        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("cold hot bytes did not drain for {bucket}/{stream}");
}

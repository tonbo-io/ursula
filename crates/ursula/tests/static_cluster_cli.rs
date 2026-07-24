use std::collections::HashSet;
use std::fs::File;
use std::net::TcpListener;
use std::path::Path;
use std::path::PathBuf;
use std::process::Child;
use std::process::Command;
use std::process::Stdio;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::OnceLock;
use std::time::Duration;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use ursula_runtime::ColdStore;

struct ChildGuard {
    child: Child,
    label: String,
    stderr_path: PathBuf,
    config_path: Option<PathBuf>,
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        if !std::thread::panicking() {
            let _ = std::fs::remove_file(&self.stderr_path);
            if let Some(config) = &self.config_path {
                let _ = std::fs::remove_file(config);
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cli_sigterm_drains_listeners_and_exits_cleanly() {
    let _guard = static_cluster_cli_test_guard().await;
    let Some(binary) = option_env!("CARGO_BIN_EXE_ursula") else {
        tracing::warn!("CARGO_BIN_EXE_ursula is not set; skipping SIGTERM smoke test");
        return;
    };
    let port = free_port();
    let base_url = format!("http://127.0.0.1:{port}");
    let root = std::env::temp_dir().join(format!(
        "ursula-cli-sigterm-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system time after unix epoch")
            .as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).expect("create temp root");
    let config_path = root.join("cluster.toml");
    let log_dir = root.join("raft-log");
    write_single_node_cluster_config(&config_path, port, 1, 1, &base_url, true, &log_dir);

    let mut child = spawn_node_with_cluster_config(binary, &config_path);
    let client = reqwest::Client::new();
    wait_until_ready(&client, &base_url, std::slice::from_mut(&mut child)).await;

    let pid = child.child.id();
    let kill_status = std::process::Command::new("kill")
        .arg("-TERM")
        .arg(pid.to_string())
        .status()
        .expect("send SIGTERM");
    assert!(kill_status.success(), "kill -TERM failed: {kill_status}");

    // The server drains its listeners and must exit 0 well inside the 20s
    // forced-exit grace period; a SIGKILL'd or crashed exit fails the test.
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    let exit_status = loop {
        if let Some(status) = child.child.try_wait().expect("poll child") {
            break status;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "node did not exit within 30s of SIGTERM"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    assert!(
        exit_status.success(),
        "expected clean exit after SIGTERM, got {exit_status}"
    );

    std::fs::remove_dir_all(&root).expect("remove temp root");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cli_static_grpc_raft_cluster_forwards_follower_writes() {
    let _guard = static_cluster_cli_test_guard().await;
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
    let mut children = children;
    for (_, base_url) in &peers {
        wait_until_ready(&client, base_url, &mut children).await;
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
async fn cli_static_grpc_raft_log_dir_recovers_with_bootstrap_enabled_after_restart() {
    let _guard = static_cluster_cli_test_guard().await;
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
    let config_path = root.join("cluster.toml");
    let log_dir = root.join("raft-log");

    write_single_node_cluster_config(&config_path, port, 1, 1, &base_url, true, &log_dir);
    {
        let mut child = spawn_node_with_cluster_config(binary, &config_path);
        let client = reqwest::Client::new();
        wait_until_ready(&client, &base_url, std::slice::from_mut(&mut child)).await;
        put_until_created(&client, &format!("{base_url}/benchcmp/cli-durable-restart")).await;
        post_until_no_content(
            &client,
            &format!("{base_url}/benchcmp/cli-durable-restart"),
            "cli-durable-payload",
        )
        .await;
    }

    let journal_path = log_dir.join("raft-log").join("core-0").join("journal.bin");
    assert!(journal_path.exists(), "core journal should exist");
    assert!(
        std::fs::metadata(&journal_path)
            .expect("core journal metadata")
            .len()
            > 0,
        "core journal should contain records"
    );

    write_single_node_cluster_config(&config_path, port, 1, 1, &base_url, true, &log_dir);
    {
        let mut child = spawn_node_with_cluster_config(binary, &config_path);
        let client = reqwest::Client::new();
        wait_until_ready(&client, &base_url, std::slice::from_mut(&mut child)).await;
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
    let _guard = static_cluster_cli_test_guard().await;
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
    for (index, (node_id, _)) in peers.iter().enumerate() {
        let config_path = root.join(format!("node-{node_id}.toml"));
        let log_dir = root.join(format!("node-{node_id}-log"));
        write_cluster_config(
            &config_path,
            ports[index],
            *node_id,
            4,
            &peers,
            *node_id == 1,
            &log_dir,
        );
        configs.push(config_path);
    }

    let mut children = vec![
        spawn_node_with_cluster_config(binary, &configs[1]),
        spawn_node_with_cluster_config(binary, &configs[2]),
        spawn_node_with_cluster_config(binary, &configs[0]),
    ];

    let client = reqwest::Client::new();
    for (_, base_url) in &peers {
        wait_until_ready(&client, base_url, &mut children).await;
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
            .join("raft-log")
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
    let _guard = static_cluster_cli_test_guard().await;
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

    let node1_config = root.join("node-1.toml");
    let node2_config = root.join("node-2.toml");
    let node3_config = root.join("node-3.toml");
    let node1_admin_port = write_cluster_config(
        &node1_config,
        ports[0],
        1,
        1,
        &initial_peers,
        true,
        &root.join("node-1-log"),
    );
    let node1_admin = format!("http://127.0.0.1:{node1_admin_port}");
    write_cluster_config(
        &node2_config,
        ports[1],
        2,
        1,
        &initial_peers,
        false,
        &root.join("node-2-log"),
    );
    write_cluster_config(
        &node3_config,
        ports[2],
        3,
        1,
        &peers,
        false,
        &root.join("node-3-log"),
    );

    let mut children = vec![
        spawn_node_with_cluster_config(binary, &node2_config),
        spawn_node_with_cluster_config(binary, &node1_config),
    ];

    let client = reqwest::Client::new();
    wait_until_ready(&client, &peers[0].1, &mut children).await;
    wait_until_ready(&client, &peers[1].1, &mut children).await;
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
        .post(format!("{node1_admin}/__ursula/raft/0/snapshot"))
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
            "{node1_admin}/__ursula/raft/0/purge?upto={snapshot_index}"
        ))
        .send()
        .await
        .expect("trigger leader purge");
    assert_eq!(purge.status(), reqwest::StatusCode::OK);

    children.push(spawn_node_with_cluster_config(binary, &node3_config));
    wait_until_ready(&client, &peers[2].1, &mut children).await;

    let add_learner = client
        .post(format!(
            "{node1_admin}/__ursula/raft/0/learners/3?addr={}",
            peers[2].1
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
    wait_metrics_contains_all(&client, &peers[2].1, &[
        format!("\"snapshot_index\":{snapshot_index}"),
        "\"learner_ids\":[3]".to_owned(),
    ])
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
    let _guard = static_cluster_cli_test_guard().await;
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
    let mut admin_ports = Vec::new();
    for (index, (node_id, _)) in peers.iter().enumerate() {
        let config_path = root.join(format!("node-{node_id}.toml"));
        let log_dir = root.join(format!("node-{node_id}-log"));
        admin_ports.push(write_node_toml(
            &config_path,
            ports[index],
            *node_id,
            1,
            &peers,
            *node_id == 1,
            "disk",
            Some(&log_dir),
            "s3",
            Some(&cold_root),
        ));
        configs.push(config_path);
    }
    let node1_admin = format!("http://127.0.0.1:{}", admin_ports[0]);

    let config = ursula_config::load_config(Some(&configs[0]), None, None)
        .unwrap_or_else(|err| panic!("load config for cold store: {err}"));
    let cold_store = Arc::new(
        ColdStore::try_new(&config.storage.cold)
            .unwrap_or_else(|err| panic!("cold store creation failed: {err}")),
    );
    cold_store
        .remove_all("")
        .await
        .expect("clear S3 cold test root before run");

    {
        let mut children = vec![
            spawn_node_with_cluster_config_and_cold_s3(binary, &configs[1]),
            spawn_node_with_cluster_config_and_cold_s3(binary, &configs[2]),
            spawn_node_with_cluster_config_and_cold_s3(binary, &configs[0]),
        ];

        let client = reqwest::Client::new();
        for (_, base_url) in &peers {
            wait_until_ready(&client, base_url, &mut children).await;
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
            &node1_admin,
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

    for (index, (node_id, _)) in peers.iter().enumerate() {
        let log_dir = root.join(format!("node-{node_id}-log"));
        write_node_toml(
            &configs[index],
            ports[index],
            *node_id,
            1,
            &peers,
            false,
            "disk",
            Some(&log_dir),
            "s3",
            Some(&cold_root),
        );
    }

    {
        let mut children = vec![
            spawn_node_with_cluster_config_and_cold_s3(binary, &configs[1]),
            spawn_node_with_cluster_config_and_cold_s3(binary, &configs[2]),
            spawn_node_with_cluster_config_and_cold_s3(binary, &configs[0]),
        ];
        let client = reqwest::Client::new();
        for (_, base_url) in &peers {
            wait_until_ready(&client, base_url, &mut children).await;
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

async fn static_cluster_cli_test_guard() -> tokio::sync::MutexGuard<'static, ()> {
    // These tests spawn real Ursula clusters on localhost. Keep them serial so
    // small CI runners do not race several multi-process clusters at once, and
    // so a child-process startup failure is attributable to one test.
    static TEST_LOCK: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();
    TEST_LOCK
        .get_or_init(|| tokio::sync::Mutex::new(()))
        .lock()
        .await
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

fn write_node_toml(
    path: &Path,
    port: u16,
    node_id: u64,
    raft_group_count: usize,
    peers: &[(u64, String)],
    init_membership: bool,
    wal_backend: &str,
    wal_path: Option<&Path>,
    cold_backend: &str,
    cold_root: Option<&str>,
) -> u16 {
    use std::fmt::Write;

    let admin_port = free_port();
    let mut config = String::new();

    writeln!(
        config,
        r#"[server]
listen = "127.0.0.1:{port}"
admin_listen = "127.0.0.1:{admin_port}"
"#
    )
    .unwrap();

    writeln!(
        config,
        r#"[runtime]
core_count = 1
"#
    )
    .unwrap();

    writeln!(
        config,
        r#"[raft]
node_id = {node_id}
group_count = {raft_group_count}
init_membership = {init_membership}
init_membership_per_group = false
"#
    )
    .unwrap();

    writeln!(
        config,
        r#"[raft.wal]
backend = "{wal_backend}""#
    )
    .unwrap();
    if let Some(p) = wal_path {
        writeln!(config, r#"path = "{}""#, p.display()).unwrap();
    }
    config.push('\n');

    for (peer_id, peer_url) in peers {
        writeln!(
            config,
            r#"[[raft.peers]]
node_id = {peer_id}
url = "{peer_url}"
"#
        )
        .unwrap();
    }

    writeln!(
        config,
        r#"[storage.cold]
backend = "{cold_backend}"
flush_interval = "1s"
gc_interval = "1s"
"#
    )
    .unwrap();

    if let Some(root) = cold_root {
        writeln!(config, r#"root = "{root}""#).unwrap();
    }

    if cold_backend == "s3" {
        writeln!(
            config,
            r#"
[storage.cold.s3]"#
        )
        .unwrap();
        for name in [
            "URSULA_COLD_S3_BUCKET",
            "URSULA_COLD_S3_REGION",
            "URSULA_COLD_S3_ENDPOINT",
            "URSULA_COLD_S3_ACCESS_KEY_ID",
            "URSULA_COLD_S3_SECRET_ACCESS_KEY",
            "URSULA_COLD_S3_SESSION_TOKEN",
        ] {
            if let Ok(value) = std::env::var(name) {
                let key = name
                    .strip_prefix("URSULA_COLD_S3_")
                    .unwrap_or(name)
                    .to_ascii_lowercase();
                let escaped = toml::Value::String(value).to_string();
                writeln!(config, "{key} = {escaped}").unwrap();
            }
        }
    }

    std::fs::write(path, config).expect("write node toml config");
    admin_port
}

fn spawn_node(
    binary: &str,
    node_id: u64,
    port: u16,
    peers: &[(u64, String)],
    init_membership: bool,
) -> ChildGuard {
    let config_path = std::env::temp_dir().join(format!(
        "ursula-node-{node_id}-{port}-{}.toml",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system time after unix epoch")
            .as_nanos()
    ));
    write_node_toml(
        &config_path,
        port,
        node_id,
        4,
        peers,
        init_membership,
        "memory",
        None,
        "memory",
        None,
    );
    let mut command = Command::new(binary);
    command.arg("server").arg("--config").arg(&config_path);
    let mut guard = spawn_child(command, format!("memory-node-{node_id}-{port}"));
    guard.config_path = Some(config_path);
    guard
}

fn spawn_node_with_cluster_config(binary: &str, config_path: &Path) -> ChildGuard {
    let mut command = Command::new(binary);
    command.arg("server").arg("--config").arg(config_path);
    spawn_child(command, child_label("durable-node", config_path))
}

fn spawn_node_with_cluster_config_and_cold_s3(binary: &str, config_path: &Path) -> ChildGuard {
    let mut command = Command::new(binary);
    command.arg("server").arg("--config").arg(config_path);
    for name in [
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
    spawn_child(command, child_label("s3-node", config_path))
}

fn child_label(prefix: &str, config_path: &Path) -> String {
    let config_name = config_path
        .file_stem()
        .and_then(|value| value.to_str())
        .unwrap_or("node");
    format!("{prefix}-{config_name}")
}

fn spawn_child(mut command: Command, label: String) -> ChildGuard {
    let stderr_path = child_stderr_path(&label);
    let stderr = File::create(&stderr_path).unwrap_or_else(|err| {
        panic!(
            "create stderr log for {label} at {} failed: {err}",
            stderr_path.display()
        )
    });
    command.stdout(Stdio::null()).stderr(Stdio::from(stderr));
    let child = command.spawn().unwrap_or_else(|err| {
        panic!(
            "spawn {label} failed; stderr log {}: {err}",
            stderr_path.display()
        )
    });
    ChildGuard {
        child,
        label,
        stderr_path,
        config_path: None,
    }
}

fn child_stderr_path(label: &str) -> PathBuf {
    let safe_label = label
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '-' {
                ch
            } else {
                '-'
            }
        })
        .collect::<String>();
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time after unix epoch")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "ursula-static-cluster-cli-{}-{nanos}-{safe_label}.stderr.log",
        std::process::id()
    ))
}

fn child_stderr_tail(child: &ChildGuard) -> String {
    match std::fs::read_to_string(&child.stderr_path) {
        Ok(content) if content.is_empty() => "<stderr empty>".to_owned(),
        Ok(content) if content.len() <= 4096 => content,
        Ok(content) => {
            let min_index = content.len().saturating_sub(4096);
            let start = content
                .char_indices()
                .map(|(index, _)| index)
                .find(|index| *index >= min_index)
                .unwrap_or(0);
            content[start..].to_owned()
        }
        Err(err) => format!("<failed to read {}: {err}>", child.stderr_path.display()),
    }
}

fn child_report(child: &ChildGuard) -> String {
    format!(
        "{} pid={} stderr={}:\n{}",
        child.label,
        child.child.id(),
        child.stderr_path.display(),
        child_stderr_tail(child)
    )
}

fn exited_child_report(children: &mut [ChildGuard]) -> Option<String> {
    for child in children {
        match child.child.try_wait() {
            Ok(Some(status)) => {
                return Some(format!(
                    "{} exited with {status}; {}",
                    child.label,
                    child_report(child)
                ));
            }
            Ok(None) => {}
            Err(err) => {
                return Some(format!(
                    "{} try_wait failed: {err}; {}",
                    child.label,
                    child_report(child)
                ));
            }
        }
    }
    None
}

fn write_single_node_cluster_config(
    path: &Path,
    port: u16,
    node_id: u64,
    raft_group_count: usize,
    base_url: &str,
    init_membership: bool,
    log_dir: &Path,
) -> u16 {
    let admin_port = write_cluster_config(
        path,
        port,
        node_id,
        raft_group_count,
        &[(node_id, base_url.to_owned())],
        init_membership,
        log_dir,
    );
    let config = std::fs::read_to_string(path).expect("read single-node cluster config");
    std::fs::write(
        path,
        config.replace(
            "init_membership_per_group = false",
            "init_membership_per_group = true",
        ),
    )
    .expect("enable per-group membership initialization");
    admin_port
}

fn write_cluster_config(
    path: &Path,
    port: u16,
    node_id: u64,
    raft_group_count: usize,
    peers: &[(u64, String)],
    init_membership: bool,
    log_dir: &Path,
) -> u16 {
    write_node_toml(
        path,
        port,
        node_id,
        raft_group_count,
        peers,
        init_membership,
        "disk",
        Some(log_dir),
        "memory",
        None,
    )
}

async fn wait_until_ready(client: &reqwest::Client, base_url: &str, children: &mut [ChildGuard]) {
    let mut last_error = String::from("no attempts made");
    for _ in 0..300 {
        if let Some(report) = exited_child_report(children) {
            panic!(
                "node {base_url} did not become ready because a child exited; \
                 last readiness error: {last_error}\n{report}"
            );
        }
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
    let reports = children
        .iter()
        .map(child_report)
        .collect::<Vec<_>>()
        .join("\n---\n");
    panic!("node {base_url} did not become ready: {last_error}\nchild diagnostics:\n{reports}");
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

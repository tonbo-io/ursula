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

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cli_startup_admission_precedes_transport_and_publishes_its_fresh_boot() {
    use std::os::unix::fs::PermissionsExt;
    let _guard = static_cluster_cli_test_guard().await;
    let directory = tempfile::tempdir().unwrap();
    let server = directory.path().join("ursula");
    std::fs::copy(env!("CARGO_BIN_EXE_ursula"), &server).unwrap();
    let helper = directory.path().join("ursulactl");
    std::fs::write(
        &helper,
        r#"#!/bin/sh
set -eu
while [ "$#" -gt 0 ]; do
  if [ "$1" = --process-incarnation ]; then
    printf '%s' "$2" > "$CAPTURE_BOOT"
    boot="$2"
    break
  fi
  shift
done
if [ "$STARTUP_REFUSE" = true ]; then
  echo 'fixture admission refused' >&2
  exit 9
fi
printf '{"process_incarnation":"%s","maintenance_fence":%s}\n' "${STARTUP_RETURN_BOOT:-$boot}" "$STARTUP_FENCE"
"#,
    )
    .unwrap();
    std::fs::set_permissions(&helper, std::fs::Permissions::from_mode(0o755)).unwrap();
    let ports = [free_port(), free_port(), free_port()];
    let port = ports[0];
    let url = format!("http://127.0.0.1:{port}");
    let peers: Vec<_> = ports
        .iter()
        .enumerate()
        .map(|(index, port)| (index as u64 + 1, format!("http://127.0.0.1:{port}")))
        .collect();
    let config = directory.path().join("cluster.toml");
    let logs = directory.path().join("raft-log");
    let admin_port = write_cluster_config(&config, port, 1, 1, &peers, true, &logs);
    let admin_url = format!("http://127.0.0.1:{admin_port}");
    let captured = directory.path().join("boot");
    let fence = |generation| {
        ursula_proto::admin::MaintenanceFence::new(
            format!("{:032x}", 1),
            format!("{generation:032x}"),
            generation,
        )
        .unwrap()
    };
    let retired = serde_json::to_string(&ursula_proto::admin::MaintenanceFenceState::Retired {
        fence: fence(7),
    })
    .unwrap();
    let start = |refuse: bool, authority: &str, returned_boot: &str| {
        let mut command = Command::new(&server);
        command
            .args(["server", "--config"])
            .arg(&config)
            .env("URSULA_STARTUP_RESERVATION", "true")
            .env("CAPTURE_BOOT", &captured)
            .env("STARTUP_REFUSE", refuse.to_string())
            .env("STARTUP_FENCE", authority)
            .env("STARTUP_RETURN_BOOT", returned_boot);
        spawn_child(command, format!("startup-admission-{port}-{refuse}"))
    };
    let mut rejected = start(true, &retired, "");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        if let Some(exit) = rejected.child.try_wait().unwrap() {
            assert!(!exit.success());
            break;
        }
        assert!(tokio::time::Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(
        !logs.exists(),
        "no format stamp or Raft log may precede admission"
    );
    assert!(std::net::TcpStream::connect(("127.0.0.1", port)).is_err());
    assert!(std::net::TcpStream::connect(("127.0.0.1", admin_port)).is_err());
    let rejected_boot = std::fs::read_to_string(&captured).unwrap();
    assert_eq!(rejected_boot.len(), 32);
    drop(rejected);
    for (authority, returned_boot) in [
        ("{\"state\":\"unclaimed\"}".to_owned(), ""),
        (
            serde_json::to_string(&ursula_proto::admin::MaintenanceFenceState::Active {
                fence: fence(7),
            })
            .unwrap(),
            "",
        ),
        (retired.clone(), "00000000000000000000000000000000"),
    ] {
        let mut rejected = start(false, &authority, returned_boot);
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            if let Some(exit) = rejected.child.try_wait().unwrap() {
                assert!(!exit.success());
                break;
            }
            assert!(tokio::time::Instant::now() < deadline);
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(!logs.exists());
        assert!(std::net::TcpStream::connect(("127.0.0.1", port)).is_err());
        assert!(std::net::TcpStream::connect(("127.0.0.1", admin_port)).is_err());
    }
    let mut survivors = Vec::new();
    for (index, (node_id, _)) in peers.iter().enumerate().skip(1) {
        let survivor_config = directory.path().join(format!("node-{node_id}.toml"));
        write_cluster_config(
            &survivor_config,
            ports[index],
            *node_id,
            1,
            &peers,
            false,
            &directory.path().join(format!("node-{node_id}-log")),
        );
        survivors.push(spawn_node_with_cluster_config(
            server.to_str().unwrap(),
            &survivor_config,
        ));
    }
    let mut accepted = start(false, &retired, "");
    let client = reqwest::Client::new();
    wait_until_ready(&client, &url, std::slice::from_mut(&mut accepted)).await;
    for (_, survivor_url) in peers.iter().skip(1) {
        wait_until_ready(&client, survivor_url, &mut survivors).await;
    }
    let metrics: serde_json::Value = client
        .get(format!("{url}/__ursula/metrics"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let admitted_boot = std::fs::read_to_string(&captured).unwrap();
    assert_ne!(admitted_boot, rejected_boot);
    assert_eq!(
        metrics["process_incarnation"].as_str(),
        Some(admitted_boot.as_str())
    );
    assert_eq!(
        metrics["maintenance_fence"],
        serde_json::from_str::<serde_json::Value>(&retired).unwrap()
    );
    let mutate = |token: Option<String>| {
        let mut request = client
            .delete(format!("{admin_url}/__ursula/leadership-shed/maintenance"))
            .header(
                ursula_proto::admin::PROCESS_INCARNATION_HEADER,
                &admitted_boot,
            );
        if let Some(token) = token {
            request = request.header(ursula_proto::admin::MAINTENANCE_FENCE_HEADER, token);
        }
        request
    };
    assert_eq!(
        mutate(None).send().await.unwrap().status(),
        reqwest::StatusCode::PRECONDITION_FAILED
    );
    for generation in [6, 7, 8] {
        let response = client
            .post(format!("{admin_url}/__ursula/maintenance/fence/activate"))
            .header(
                ursula_proto::admin::PROCESS_INCARNATION_HEADER,
                &admitted_boot,
            )
            .json(&fence(generation))
            .send()
            .await
            .unwrap();
        assert_eq!(
            response.status(),
            if generation == 8 {
                reqwest::StatusCode::OK
            } else {
                reqwest::StatusCode::PRECONDITION_FAILED
            }
        );
    }
    assert_eq!(
        mutate(None).send().await.unwrap().status(),
        reqwest::StatusCode::PRECONDITION_REQUIRED
    );
    assert_eq!(
        mutate(Some(fence(7).header_value()))
            .send()
            .await
            .unwrap()
            .status(),
        reqwest::StatusCode::PRECONDITION_FAILED
    );
    assert_eq!(
        mutate(Some(fence(8).header_value()))
            .send()
            .await
            .unwrap()
            .status(),
        reqwest::StatusCode::OK
    );
    drop(accepted);
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

    let mut stream_sessions = 0_u64;
    let mut stream_requests = 0_u64;
    for (_, base_url) in &peers {
        let metrics: serde_json::Value = client
            .get(format!("{base_url}/__ursula/metrics"))
            .send()
            .await
            .expect("request cluster metrics")
            .error_for_status()
            .expect("cluster metrics status")
            .json()
            .await
            .expect("decode cluster metrics");
        stream_sessions = stream_sessions.saturating_add(
            metrics["raft_grpc_append_stream_sessions_opened"]
                .as_u64()
                .unwrap_or(0),
        );
        stream_requests = stream_requests.saturating_add(
            metrics["raft_grpc_append_stream_requests"]
                .as_u64()
                .unwrap_or(0),
        );
    }
    assert!(
        stream_sessions > 0,
        "the static cluster should open a multiplexed append stream"
    );
    assert!(
        stream_requests > 0,
        "the static cluster should replicate through the append stream"
    );

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
        let metrics: serde_json::Value = client
            .get(format!("{base_url}/__ursula/metrics"))
            .send()
            .await
            .expect("request recovery metrics")
            .json()
            .await
            .expect("decode recovery metrics");
        for field in [
            "wal_recovery_records",
            "wal_recovery_bytes",
            "wal_recovery_live_entries",
        ] {
            assert!(
                metrics
                    .get(field)
                    .and_then(serde_json::Value::as_u64)
                    .is_some_and(|value| value > 0),
                "{field} should report durable restart work: {metrics}"
            );
        }
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

    // Restart one follower, append while it is absent, then prove the same
    // disk WAL can reopen and catch up without duplicate application.
    drop(children.remove(1));
    post_until_no_content(
        &client,
        &format!("{}/benchcmp/cli-durable-cluster", peers[0].1),
        "-after-follower-restart",
    )
    .await;
    children.push(spawn_node_with_cluster_config(binary, &configs[2]));
    wait_until_ready(&client, &peers[2].1, &mut children).await;
    let recovered_payload = read_until_matches(
        &client,
        &format!(
            "{}/benchcmp/cli-durable-cluster?offset=0&max_bytes=128",
            peers[2].1
        ),
        b"cli-durable-cluster-payload-after-follower-restart",
    )
    .await;
    assert_eq!(
        recovered_payload,
        b"cli-durable-cluster-payload-after-follower-restart"
    );

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

    let snapshot = admin_test_post(&client, format!("{node1_admin}/__ursula/raft/0/snapshot"))
        .await
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

    let purge = admin_test_post(
        &client,
        format!("{node1_admin}/__ursula/raft/0/purge?upto={snapshot_index}"),
    )
    .await
    .send()
    .await
    .expect("trigger leader purge");
    assert_eq!(purge.status(), reqwest::StatusCode::OK);

    children.push(spawn_node_with_cluster_config(binary, &node3_config));
    wait_until_ready(&client, &peers[2].1, &mut children).await;

    let add_learner = admin_test_post(
        &client,
        format!(
            "{node1_admin}/__ursula/raft/0/learners/3?addr={}",
            peers[2].1
        ),
    )
    .await
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
    // CI scopes each job's credentials to URSULA_S3_PREFIX.
    let cold_root = match std::env::var("URSULA_S3_PREFIX") {
        Ok(prefix) => format!("{prefix}/ursula-cli-s3-cold-restart/{suffix}"),
        Err(_) => format!("ursula-cli-s3-cold-restart/{suffix}"),
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cli_restarted_memory_voter_heals_itself_and_repair_is_idempotent() {
    // A memory-WAL voter restarted empty must not re-run per-group Initialize
    // for the groups it bootstraps (2 and 5 of 6 for node 3): it must never
    // report itself a voter with nothing applied. The leaders see it lost the
    // entries it had acknowledged and rebuild it through remove, learner,
    // catch-up and promote with no operator. `repair_restarted_voter`, which
    // a prepared rollout still runs, then finds nothing left to do.
    let _guard = static_cluster_cli_test_guard().await;
    let Some(binary) = option_env!("CARGO_BIN_EXE_ursula") else {
        tracing::warn!("CARGO_BIN_EXE_ursula is not set; skipping restart self-heal test");
        return;
    };
    let ports = [free_port(), free_port(), free_port()];
    let public = |node_id: u64| format!("http://127.0.0.1:{}", ports[(node_id - 1) as usize]);
    let peers = [1_u64, 2, 3]
        .into_iter()
        .map(|node_id| (node_id, public(node_id)))
        .collect::<Vec<_>>();

    let mut children = Vec::new();
    let mut nodes = Vec::new();
    for node_id in [1_u64, 2, 3] {
        let port = ports[(node_id - 1) as usize];
        let (child, admin_port) = spawn_per_group_memory_node(binary, node_id, port, &peers, false);
        children.push(child);
        nodes.push(ctl_node(node_id, admin_port, &public(node_id)));
    }
    let client = reqwest::Client::new();
    for node_id in [1_u64, 2, 3] {
        wait_until_ready(&client, &public(node_id), &mut children).await;
    }
    for index in 0..6 {
        put_until_created(
            &client,
            &format!("{}/benchcmp/restart-repair-{index}", public(1)),
        )
        .await;
    }

    let ctl = ursula_ctl::MetricsClient::new(Duration::from_secs(5)).expect("ctl client");
    nodes = ctl
        .pin_nodes(&nodes, None, false)
        .await
        .expect("save original process plan");
    let now_ms = || {
        u64::try_from(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis(),
        )
        .unwrap()
    };
    let observation = |started_ms, verification| ursula_ctl::reservation::PrefixObservation {
        started_ms,
        completed_ms: now_ms(),
        verification,
    };
    // Real Raft observations must also satisfy the shared reservation policy.
    // Physical identities below describe this native fixture, not Kubernetes
    // or provider fencing. ChildGuard waits for the original process to exit.
    let options = ursula_ctl::quorum::QuorumVerificationOptions {
        group_count: 6,
        core_count: 1,
        timeout: Duration::from_secs(15),
        poll_interval: Duration::from_millis(100),
        allow_legacy_eligibility: false,
    };
    ursula_ctl::wait_cluster_ready(
        "pre-fault host inventory",
        &nodes,
        &ctl,
        Duration::from_secs(60),
        Duration::from_millis(100),
        16,
    )
    .await
    .expect("capture requires full startup recovery, not listener readiness");
    let inventory_started = now_ms();
    let inventory_proof = ursula_ctl::quorum::verify_quorum(&nodes, &ctl, &options)
        .await
        .expect("fresh pre-fault host inventory proof");
    let inventory_observation = observation(inventory_started, inventory_proof);
    let mut reservation =
        ursula_ctl::reservation::Reservation::initial(ursula_ctl::reservation::CellIdentity {
            namespace: "native".into(),
            namespace_uid: "native-namespace".into(),
            statefulset: "voters".into(),
            statefulset_uid: "native-sts".into(),
            group_count: 6,
            core_count: 1,
            voter_ids: [1, 2, 3].into_iter().collect(),
        })
        .unwrap()
        .publish_hosts(native_host_inventory(&nodes, inventory_observation))
        .expect("capture all original host identities in the maintenance store")
        .propose(ursula_ctl::reservation::OwnershipRequest::Reserve {
            operation_id: format!("{:032x}", 1),
            executor_id: format!("{:032x}", 2),
            now_ms: now_ms(),
            process_plan: nodes.clone(),
            source: ursula_ctl::reservation::SourceIdentity {
                node_id: 3,
                pod_name: "voters-2".into(),
                pod_uid: "native-original".into(),
                node_uid: "native-node-3".into(),
                provider_instance: "native-instance-3".into(),
                process_incarnation: nodes[2].expected_process_incarnation.clone().unwrap(),
            },
        })
        .unwrap();
    let executor = reservation.operation().unwrap().fence.clone();
    for node in &mut nodes {
        node.expected_maintenance_fence = Some(executor.clone());
    }
    for node in &nodes {
        ctl.set_maintenance_fence(node, false)
            .await
            .expect("activate executor");
    }
    for index in 0..6 {
        post_until_no_content(
            &client,
            &format!("{}/benchcmp/restart-repair-{index}", public(1)),
            "executor-fence-tail",
        )
        .await;
    }

    ursula_ctl::wait_cluster_ready(
        "reservation admission",
        &nodes,
        &ctl,
        Duration::from_secs(60),
        Duration::from_millis(100),
        16,
    )
    .await
    .expect("listener startup alone is not Raft eligibility");
    let started_ms = now_ms();
    let admitted = observation(
        started_ms,
        ursula_ctl::quorum::verify_quorum(&nodes, &ctl, &options)
            .await
            .unwrap(),
    );
    reservation = reservation
        .progress(ursula_ctl::reservation::ProgressRequest::AdmitPodDeletion {
            fence: executor.clone(),
            now_ms: now_ms(),
            observation: admitted,
        })
        .expect("live all-active admission accepted by shared policy");

    let drain_options = ursula_ctl::DrainOptions {
        drain_timeout: Duration::from_secs(60),
        ready_timeout: Duration::from_secs(60),
        poll_interval: Duration::from_millis(200),
        lag_tolerance: 16,
        dry_run: false,
    };
    let outcome = ursula_ctl::drain_node(&nodes, &nodes[2], &ctl, &drain_options)
        .await
        .expect("drain node 3");
    assert!(
        matches!(outcome, ursula_ctl::DrainOutcome::Drained),
        "{outcome:?}; metrics={:?}; children={}",
        ctl.try_fetch_cluster(&nodes).await,
        children
            .iter()
            .map(child_report)
            .collect::<Vec<_>>()
            .join("\n")
    );
    ursula_ctl::prepare_restart(&nodes, &nodes[2], &ctl, &drain_options)
        .await
        .expect("prepare node 3 restart");

    let survivors = ursula_ctl::quorum::verify_surviving_quorum(&nodes, 3, &ctl, &options)
        .await
        .expect("prepared source leaves both pinned survivors eligible");
    assert!(survivors.verification.maintenance_executor_certified);
    assert!(!survivors.full_redundancy_restored);

    // The chart reuses its Pod-bound tunnel addresses. Preserve the same
    // native listening configuration too: shared binding permits only a boot
    // change, never refreshing a target URL alongside its incarnation.
    let replacement_config = children[2].config_path.clone().unwrap();
    let replacement_text = std::fs::read_to_string(&replacement_config).unwrap();
    drop(children.pop());
    std::fs::write(&replacement_config, replacement_text).unwrap();
    let mut command = Command::new(binary);
    command
        .arg("server")
        .arg("--config")
        .arg(&replacement_config)
        .env("URSULA_START_MAINTENANCE_DRAINED", "true");
    let mut child = spawn_child(command, format!("replacement-node-3-{}", ports[2]));
    child.config_path = Some(replacement_config);
    children.push(child);
    let retired_identity = nodes[2].expected_process_incarnation.clone();
    wait_until_ready(&client, &public(3), &mut children).await;

    let stale_clear = ctl
        .set_maintenance_drain(&nodes[2], false)
        .await
        .expect_err("old process must not clear replacement drain");
    assert!(stale_clear.to_string().contains("412"), "{stale_clear}");
    let replacement_shed: serde_json::Value = client
        .get(format!("{}/__ursula/leadership-shed", public(3)))
        .send()
        .await
        .expect("replacement policy")
        .json()
        .await
        .expect("policy JSON");
    assert_eq!(replacement_shed["state"], "maintenance-drain");
    assert!(
        ctl.fetch_node(&nodes[2]).await.is_err(),
        "old operation must not refresh its identity"
    );
    let ctl = ursula_ctl::MetricsClient::new(Duration::from_secs(5))
        .expect("new admitted repair operation");
    nodes = ctl
        .pin_nodes(&nodes, Some(3), false)
        .await
        .expect("bind only admitted replacement");
    assert_ne!(nodes[2].expected_process_incarnation, retired_identity);
    assert!(
        nodes
            .iter()
            .all(|node| node.expected_maintenance_fence.as_ref() == Some(&executor))
    );
    reservation = reservation
        .progress(
            ursula_ctl::reservation::ProgressRequest::BindPodReplacement {
                fence: executor.clone(),
                process_plan: nodes.clone(),
                pod: serde_json::json!({"kind":"Pod", "metadata":{"namespace":"native",
            "name":"voters-2", "uid":"native-replacement", "ownerReferences":[{
                "kind":"StatefulSet", "uid":"native-sts", "controller":true}]},
            "spec":{"nodeName":"native-host-3"}}),
                node: serde_json::json!({"kind":"Node", "metadata":{"name":"native-host-3",
            "uid":"native-node-3", "labels":{"topology.kubernetes.io/zone":"native-zone-3"}}, "spec":{"providerID":"native-instance-3"}}),
            },
        )
        .expect("only the irreversibly retired target process may bind");
    for node in &nodes {
        ctl.set_maintenance_fence(node, false)
            .await
            .expect("activate only the new process; survivors stay fixed");
    }

    // Node 3 heals by itself; it is never an empty voter on the way.
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    loop {
        let snapshot = ctl.fetch_cluster(&nodes).await.expect("fetch cluster");
        for group_id in 0..6_u64 {
            let target = snapshot.node(3).and_then(|view| view.group(group_id));
            let peer_committed = snapshot
                .peer_views(group_id, 3)
                .values()
                .filter_map(|group| group.committed_index)
                .max();
            let empty_voter = target.is_some_and(|group| {
                group.voter_ids.contains(&3) && group.last_applied_index.is_none()
            }) && peer_committed.is_some_and(|committed| committed > 0);
            assert!(
                !empty_voter,
                "node 3 reported itself an empty voter of group {group_id}: {snapshot:?}"
            );
        }
        if ursula_ctl::plan::check_readiness(&snapshot, 3, 0).all_ready {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "node 3 never healed back to a caught-up voter: {snapshot:?}"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    let repair_options = ursula_ctl::MembershipRepairOptions {
        max_concurrency: 6,
        operation_timeout: Duration::from_secs(5),
        operation_reconcile_timeout: Duration::from_secs(30),
        stall_timeout: Duration::from_secs(30),
        ready_timeout: Duration::from_secs(60),
        poll_interval: Duration::from_millis(200),
    };
    ursula_ctl::repair_restarted_voter(&nodes, &nodes[2], &ctl, &drain_options, &repair_options)
        .await
        .expect("repair after self-heal is a no-op");

    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        let snapshot = ctl.fetch_cluster(&nodes).await.expect("fetch cluster");
        let report = ursula_ctl::plan::check_readiness(&snapshot, 3, 0);
        if report.all_ready {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "repair left node 3 behind: {report:?}"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    for index in 0..6 {
        read_until_matches(
            &client,
            &format!(
                "{}/benchcmp/restart-repair-{index}?offset=0&max_bytes=64",
                public(3)
            ),
            b"executor-fence-tail",
        )
        .await;
    }

    let proof = ursula_ctl::quorum::verify_quorum(&nodes, &ctl, &options)
        .await
        .expect("current process full prefix proof");
    assert!(proof.maintenance_executor_certified);
    assert!(proof.process_incarnations_certified);
    for node in &nodes {
        ctl.set_maintenance_fence(node, true)
            .await
            .expect("retire executor");
    }
    let started_ms = now_ms();
    let proof = ursula_ctl::quorum::verify_quorum(&nodes, &ctl, &options)
        .await
        .expect("fresh proof after all executors retired");
    assert!(proof.maintenance_executor_retired_certified);
    assert!(!proof.maintenance_executor_certified);
    reservation = reservation
        .progress(
            ursula_ctl::reservation::ProgressRequest::CompletePodReplacement {
                fence: executor,
                now_ms: now_ms(),
                observation: observation(started_ms, proof),
            },
        )
        .expect("live all-retired prefixes release only the bound replacement");
    assert!(reservation.operation().is_none());
    assert_eq!(reservation.generation(), 1);
    assert_eq!(
        reservation
            .hosts()
            .unwrap()
            .voter(3)
            .unwrap()
            .source
            .pod_uid,
        "native-replacement"
    );
    assert_eq!(
        reservation
            .hosts()
            .unwrap()
            .voter(3)
            .unwrap()
            .source
            .provider_instance,
        "native-instance-3"
    );
    assert_eq!(
        reservation.completion().unwrap().replacement.pod_uid,
        "native-replacement"
    );
    assert!(ctl.set_maintenance_drain(&nodes[2], false).await.is_err());
    drop(children);
}

fn native_host_inventory(
    nodes: &[ursula_ctl::NodeInfo],
    observation: ursula_ctl::reservation::PrefixObservation,
) -> ursula_ctl::reservation::PublishHostInventory {
    ursula_ctl::reservation::PublishHostInventory {
        now_ms: native_epoch_ms(), process_plan:nodes.to_vec(), observation,
        pods:(1..=3).map(|id|serde_json::json!({"kind":"Pod","metadata":{
            "namespace":"native","name":format!("voters-{}",id-1),
            "uid":if id==3 {"native-original".to_owned()}else{format!("native-pod-{id}")},
            "ownerReferences":[{"kind":"StatefulSet","uid":"native-sts","controller":true}]},
            "spec":{"nodeName":format!("native-host-{id}")},"status":{"conditions":[{"type":"Ready","status":"True"}]}})).collect(),
        nodes:(1..=3).map(|id|serde_json::json!({"kind":"Node","metadata":{
            "name":format!("native-host-{id}"),"uid":format!("native-node-{id}"),"labels":{"topology.kubernetes.io/zone":format!("native-zone-{id}")}},
            "spec":{"providerID":format!("native-instance-{id}")},"status":{"conditions":[{"type":"Ready","status":"True"}]}})).collect(),
    }
}

fn native_epoch_ms() -> u64 {
    u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis(),
    )
    .unwrap()
}

async fn append_idempotent_until_acked(
    client: &reqwest::Client,
    url: &str,
    seq: u64,
    payload: &'static str,
) -> String {
    for _ in 0..100 {
        if let Ok(response) = client
            .post(url)
            .header("content-type", "text/plain")
            .header("producer-id", "native-host-writer")
            .header("producer-epoch", "0")
            .header("producer-seq", seq.to_string())
            .body(payload)
            .send()
            .await
            && response.status() == reqwest::StatusCode::NO_CONTENT
        {
            return response
                .headers()
                .get("stream-next-offset")
                .unwrap()
                .to_str()
                .unwrap()
                .to_owned();
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("idempotent append did not succeed at {url}, seq {seq}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cli_host_recovery_keeps_ack_tail_across_abrupt_memory_voter_loss() {
    use ursula_ctl::reservation::HostRequest;
    use ursula_ctl::reservation::HostTerminationObservation;
    use ursula_ctl::reservation::OwnershipRequest;
    use ursula_ctl::reservation::PrefixObservation;
    use ursula_ctl::reservation::SurvivingPrefixObservation;
    let _guard = static_cluster_cli_test_guard().await;
    let binary = env!("CARGO_BIN_EXE_ursula");
    let ports = [free_port(), free_port(), free_port()];
    let public = |id: u64| format!("http://127.0.0.1:{}", ports[(id - 1) as usize]);
    let peers = (1..=3).map(|id| (id, public(id))).collect::<Vec<_>>();
    let mut children = Vec::new();
    let mut nodes = Vec::new();
    for id in 1..=3 {
        let (child, admin) =
            spawn_per_group_memory_node(binary, id, ports[(id - 1) as usize], &peers, false);
        children.push(child);
        nodes.push(ctl_node(id, admin, &public(id)));
    }
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap();
    for id in 1..=3 {
        wait_until_ready(&client, &public(id), &mut children).await;
    }
    for index in 0..6 {
        put_until_created(
            &client,
            &format!("{}/benchcmp/host-recovery-{index}", public(1)),
        )
        .await;
    }
    let ctl = ursula_ctl::MetricsClient::new(Duration::from_secs(5)).unwrap();
    nodes = ctl.pin_nodes(&nodes, None, false).await.unwrap();
    ursula_ctl::wait_cluster_ready(
        "host recovery prefault",
        &nodes,
        &ctl,
        Duration::from_secs(60),
        Duration::from_millis(100),
        16,
    )
    .await
    .unwrap();
    for index in 0..6 {
        append_idempotent_until_acked(
            &client,
            &format!("{}/benchcmp/host-recovery-{index}", public(1)),
            0,
            "before-fault",
        )
        .await;
    }
    let options = ursula_ctl::quorum::QuorumVerificationOptions {
        group_count: 6,
        core_count: 1,
        timeout: Duration::from_secs(15),
        poll_interval: Duration::from_millis(100),
        allow_legacy_eligibility: false,
    };
    let started_ms = native_epoch_ms();
    let proof = ursula_ctl::quorum::verify_quorum(&nodes, &ctl, &options)
        .await
        .unwrap();
    let prefault = PrefixObservation {
        started_ms,
        completed_ms: native_epoch_ms(),
        verification: proof,
    };
    let mut state =
        ursula_ctl::reservation::Reservation::initial(ursula_ctl::reservation::CellIdentity {
            namespace: "native".into(),
            namespace_uid: "native-namespace".into(),
            statefulset: "voters".into(),
            statefulset_uid: "native-sts".into(),
            group_count: 6,
            core_count: 1,
            voter_ids: [1, 2, 3].into_iter().collect(),
        })
        .unwrap()
        .publish_hosts(native_host_inventory(&nodes, prefault))
        .unwrap();
    // A real abrupt process stop; no drain, quiesce or SIGTERM handoff.
    let replacement_config = children[2].config_path.clone().unwrap();
    let replacement_text = std::fs::read_to_string(&replacement_config).unwrap();
    children[2].child.kill().unwrap();
    children[2].child.wait().unwrap();
    state = state
        .recover_host(HostRequest::ReserveHostRecovery {
            operation_id: format!("{:032x}", 100),
            executor_id: format!("{:032x}", 200),
            node_id: 3,
            process_plan: nodes.clone(),
            now_ms: native_epoch_ms(),
        })
        .unwrap();
    nodes = state.operation().unwrap().process_plan.clone();
    for node in nodes.iter().filter(|node| node.id != 3) {
        ctl.set_maintenance_fence(node, false).await.unwrap();
    }
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    let admitted = loop {
        let started_ms = native_epoch_ms();
        match ursula_ctl::quorum::verify_surviving_quorum(&nodes, 3, &ctl, &options).await {
            Ok(verification) => {
                break SurvivingPrefixObservation {
                    started_ms,
                    completed_ms: native_epoch_ms(),
                    verification,
                };
            }
            Err(error) => {
                assert!(
                    std::time::Instant::now() < deadline,
                    "survivor proof: {error}"
                );
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    };
    assert!(!admitted.verification.full_redundancy_restored);
    state = state
        .recover_host(HostRequest::AdmitHostTermination {
            fence: state.operation().unwrap().fence.clone(),
            now_ms: native_epoch_ms(),
            observation: admitted,
        })
        .unwrap();
    let started_ms = native_epoch_ms();
    assert!(children[2].child.try_wait().unwrap().is_some());
    let terminated = HostTerminationObservation {
        started_ms,
        completed_ms: native_epoch_ms(),
        provider_instance: "native-instance-3".into(),
        terminal_state: "terminated".into(),
    };
    // The native child's exit is irreversible. Node/provider metadata here is
    // synthetic; this cannot qualify Kubernetes force deletion or AWS fencing.
    state = state
        .recover_host(HostRequest::RecordHostTermination {
            fence: state.operation().unwrap().fence.clone(),
            now_ms: native_epoch_ms(),
            observation: terminated,
        })
        .unwrap();
    state = state
        .recover_host(HostRequest::AdmitFencedPodRetirement {
            fence: state.operation().unwrap().fence.clone(),
            pod: None,
            node: None,
        })
        .unwrap();
    state = state
        .propose(OwnershipRequest::Takeover {
            operation_id: state.operation().unwrap().fence.reservation_id().into(),
            executor_id: format!("{:032x}", 300),
            now_ms: native_epoch_ms(),
        })
        .unwrap();
    nodes = state.operation().unwrap().process_plan.clone();
    for node in nodes.iter().filter(|node| node.id != 3) {
        ctl.set_maintenance_fence(node, false).await.unwrap();
    }
    let mut offsets = Vec::new();
    for index in 0..6 {
        offsets.push(
            append_idempotent_until_acked(
                &client,
                &format!("{}/benchcmp/host-recovery-{index}", public(1)),
                1,
                "during-recovery",
            )
            .await,
        );
    }
    drop(children.pop());
    std::fs::write(&replacement_config, replacement_text).unwrap();
    let mut command = Command::new(binary);
    command
        .arg("server")
        .arg("--config")
        .arg(&replacement_config)
        .env("URSULA_START_MAINTENANCE_DRAINED", "true");
    let mut child = spawn_child(command, format!("host-replacement-{}", ports[2]));
    child.config_path = Some(replacement_config);
    children.push(child);
    wait_until_ready(&client, &public(3), &mut children).await;
    assert!(ctl.fetch_node(&nodes[2]).await.is_err());
    // Each client pins fetched boot identities for its lifetime. The durable
    // reservation, rather than the old transport cache, admits this new boot.
    let ctl = ursula_ctl::MetricsClient::new(Duration::from_secs(5)).unwrap();
    nodes = ctl.pin_nodes(&nodes, Some(3), false).await.unwrap();
    state=state.recover_host(HostRequest::BindHostReplacement {fence:state.operation().unwrap().fence.clone(),process_plan:nodes.clone(),
        pod:serde_json::json!({"kind":"Pod","metadata":{"namespace":"native","name":"voters-2","uid":"native-host-replacement","ownerReferences":[{"kind":"StatefulSet","uid":"native-sts","controller":true}]},"spec":{"nodeName":"native-host-3-replacement"}}),
        node:serde_json::json!({"kind":"Node","metadata":{"name":"native-host-3-replacement","uid":"native-node-3-replacement","labels":{"topology.kubernetes.io/zone":"native-zone-3"}},"spec":{"providerID":"native-instance-3-replacement"},"status":{"conditions":[{"type":"Ready","status":"True"}]}})
    }).unwrap();
    for node in &nodes {
        ctl.set_maintenance_fence(node, false).await.unwrap();
    }
    ursula_ctl::repair_restarted_voter(
        &nodes,
        &nodes[2],
        &ctl,
        &ursula_ctl::DrainOptions {
            drain_timeout: Duration::from_secs(60),
            ready_timeout: Duration::from_secs(60),
            poll_interval: Duration::from_millis(100),
            ..Default::default()
        },
        &ursula_ctl::MembershipRepairOptions {
            max_concurrency: 6,
            operation_timeout: Duration::from_secs(5),
            operation_reconcile_timeout: Duration::from_secs(30),
            stall_timeout: Duration::from_secs(30),
            ready_timeout: Duration::from_secs(60),
            poll_interval: Duration::from_millis(100),
        },
    )
    .await
    .unwrap();
    ursula_ctl::finish_prepared_restart(&nodes, &nodes[2], &ctl)
        .await
        .unwrap();
    ursula_ctl::wait_cluster_ready(
        "host replacement recovered",
        &nodes,
        &ctl,
        Duration::from_secs(60),
        Duration::from_millis(100),
        0,
    )
    .await
    .unwrap();
    for (index, offset) in offsets.iter().enumerate() {
        let url = format!("{}/benchcmp/host-recovery-{index}", public(3));
        assert_eq!(
            append_idempotent_until_acked(&client, &url, 1, "during-recovery").await,
            *offset,
            "replayed ACK offset must survive"
        );
        for id in 1..=3 {
            read_until_matches(
                &client,
                &format!(
                    "{}/benchcmp/host-recovery-{index}?offset=0&max_bytes=64",
                    public(id)
                ),
                b"before-faultduring-recovery",
            )
            .await;
        }
    }
    for node in &nodes {
        ctl.set_maintenance_fence(node, true).await.unwrap();
    }
    let completion_deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    let completion = loop {
        let started_ms = native_epoch_ms();
        match ursula_ctl::quorum::verify_quorum(&nodes, &ctl, &options).await {
            Ok(proof) => {
                assert!(proof.maintenance_executor_retired_certified);
                break PrefixObservation {
                    started_ms,
                    completed_ms: native_epoch_ms(),
                    verification: proof,
                };
            }
            Err(error) => {
                assert!(state.operation().is_some());
                assert!(tokio::time::Instant::now() < completion_deadline, "{error}");
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    };
    state = state
        .recover_host(HostRequest::CompleteHostReplacement {
            fence: state.operation().unwrap().fence.clone(),
            now_ms: native_epoch_ms(),
            observation: completion,
        })
        .unwrap();
    assert!(state.operation().is_none());
    assert_eq!(state.generation(), 2);
    assert_eq!(
        state
            .hosts()
            .unwrap()
            .voter(3)
            .unwrap()
            .source
            .provider_instance,
        "native-instance-3-replacement"
    );
    assert!(
        state
            .completion()
            .unwrap()
            .host
            .as_ref()
            .unwrap()
            .pod_retirement_intents
            .contains("native-original")
    );
    drop(children);
}

fn spawn_per_group_memory_node(
    binary: &str,
    node_id: u64,
    port: u16,
    peers: &[(u64, String)],
    start_drained: bool,
) -> (ChildGuard, u16) {
    let config_path = std::env::temp_dir().join(format!(
        "ursula-per-group-node-{node_id}-{port}-{}.toml",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time after unix epoch")
            .as_nanos()
    ));
    let admin_port = write_node_toml(
        &config_path,
        port,
        node_id,
        6,
        peers,
        true,
        "memory",
        None,
        "memory",
        None,
    );
    let config = std::fs::read_to_string(&config_path).expect("read node config");
    std::fs::write(
        &config_path,
        config.replace(
            "init_membership_per_group = false",
            "init_membership_per_group = true",
        ),
    )
    .expect("enable per-group membership initialization");
    let mut command = Command::new(binary);
    command.arg("server").arg("--config").arg(&config_path);
    command.env("RUST_LOG", "info");
    if start_drained {
        command.env("URSULA_START_MAINTENANCE_DRAINED", "true");
    }
    let mut guard = spawn_child(command, format!("per-group-node-{node_id}-{port}"));
    guard.config_path = Some(config_path);
    (guard, admin_port)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cli_sigterm_hands_off_memory_leaders_and_bounds_quorum_loss() {
    let _guard = static_cluster_cli_test_guard().await;
    let binary = env!("CARGO_BIN_EXE_ursula");
    let ports = [free_port(), free_port(), free_port()];
    let peers = ports
        .iter()
        .enumerate()
        .map(|(index, port)| ((index + 1) as u64, format!("http://127.0.0.1:{port}")))
        .collect::<Vec<_>>();
    let mut children = Vec::new();
    let mut nodes = Vec::new();
    for (node_id, url) in &peers {
        let (child, admin_port) = spawn_per_group_memory_node(
            binary,
            *node_id,
            ports[(*node_id - 1) as usize],
            &peers,
            false,
        );
        children.push(child);
        nodes.push(ctl_node(*node_id, admin_port, url));
    }
    let client = reqwest::Client::new();
    for (_, url) in &peers {
        wait_until_ready(&client, url, &mut children).await;
    }
    for index in 0..6 {
        put_with_body_until_created(
            &client,
            &format!("{}/benchcmp/sigterm-{index}", peers[0].1),
            "acknowledged-before-sigterm",
        )
        .await;
    }
    let ctl = ursula_ctl::MetricsClient::new(Duration::from_secs(2)).expect("ctl client");
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    let snapshot = loop {
        let snapshot = ctl.fetch_cluster(&nodes).await.expect("fetch cluster");
        if nodes.iter().all(|node| {
            snapshot.node(node.id).is_some_and(|view| {
                view.groups.len() == 6
                    && view
                        .groups
                        .iter()
                        .all(|group| group.current_leader.is_some())
            }) && ursula_ctl::plan::check_readiness(&snapshot, node.id, 0).all_ready
        }) {
            break snapshot;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "cluster not caught up: {snapshot:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    let leader_count = |node_id| {
        snapshot
            .node(node_id)
            .unwrap()
            .groups
            .iter()
            .filter(|group| group.current_leader == Some(node_id))
            .count()
    };
    let index = (0..nodes.len())
        .max_by_key(|index| leader_count(nodes[*index].id))
        .unwrap();
    assert!(leader_count(nodes[index].id) > 0);
    let mut outgoing = children.remove(index);
    nodes.remove(index);
    sigterm_and_wait_for_clean_exit(&mut outgoing).await;
    let log = std::fs::read_to_string(&outgoing.stderr_path).expect("shutdown log");
    // This event is emitted only after the outgoing process has observed zero
    // local leaders, before it shuts down its Raft transport. A normal election
    // after an abrupt exit cannot satisfy this assertion.
    assert!(
        log.contains("shutdown leadership handoff complete"),
        "{log}"
    );
    for node in &nodes {
        for index in 0..6 {
            let payload = read_until_replicated(
                &client,
                &format!(
                    "{}/benchcmp/sigterm-{index}?offset=0&max_bytes=64",
                    node.http_url
                        .as_ref()
                        .unwrap()
                        .as_str()
                        .trim_end_matches('/')
                ),
            )
            .await;
            assert_eq!(payload, b"acknowledged-before-sigterm");
        }
    }

    // With the terminated voter gone, kill another voter without a handoff.
    // The last process cannot transfer leadership, but must still exit promptly.
    let snapshot = ctl.fetch_cluster(&nodes).await.expect("fetch survivors");
    let leader_index = (0..nodes.len())
        .max_by_key(|index| {
            let node_id = nodes[*index].id;
            snapshot
                .node(node_id)
                .unwrap()
                .groups
                .iter()
                .filter(|group| group.current_leader == Some(node_id))
                .count()
        })
        .unwrap();
    drop(children.remove(1 - leader_index));
    sigterm_and_wait_for_clean_exit(&mut children[0]).await;
}

async fn sigterm_and_wait_for_clean_exit(child: &mut ChildGuard) {
    let status = Command::new("kill")
        .arg("-TERM")
        .arg(child.child.id().to_string())
        .status()
        .expect("send SIGTERM");
    assert!(status.success());
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        if let Some(status) = child.child.try_wait().expect("poll child") {
            assert!(
                status.success(),
                "unclean shutdown: {status}; {}",
                child_report(child)
            );
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "shutdown exceeded handoff budget: {}",
            child_report(child)
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

fn ctl_node(node_id: u64, admin_port: u16, public_url: &str) -> ursula_ctl::NodeInfo {
    ursula_ctl::NodeInfo {
        expected_process_incarnation: None,
        expected_maintenance_fence: None,
        id: node_id,
        admin_url: url::Url::parse(&format!("http://127.0.0.1:{admin_port}")).expect("admin url"),
        host: "127.0.0.1".to_owned(),
        http_url: Some(url::Url::parse(public_url).expect("public url")),
        metrics_url: None,
    }
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
    if wal_backend == "memory" && peers.len() > 1 {
        writeln!(config, "allow_volatile_multi_peer = true").unwrap();
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
        // MinIO without a KMS rejects the default SSE-S3 header.
        if let Ok(value) = std::env::var("URSULA_COLD_S3_SSE") {
            let escaped = toml::Value::String(value).to_string();
            writeln!(config, "server_side_encryption = {escaped}").unwrap();
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
    // Tracing's formatter writes to stdout; retain it alongside stderr so
    // shutdown ordering and child startup failures are observable in tests.
    let stdout = stderr.try_clone().expect("clone child log handle");
    command
        .stdout(Stdio::from(stdout))
        .stderr(Stdio::from(stderr));
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

async fn read_until_matches(client: &reqwest::Client, url: &str, expected: &[u8]) -> Vec<u8> {
    let mut last = Vec::new();
    for _ in 0..100 {
        if let Ok(response) = client.get(url).send().await
            && response.status().is_success()
        {
            last = response
                .bytes()
                .await
                .expect("read replicated payload")
                .to_vec();
            if last == expected {
                return last;
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("replicated payload at {url} did not converge: {last:?}");
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
        let flush = admin_test_post(
            client,
            format!("{base_url}/__ursula/flush-cold/{bucket}/{stream}?min_hot_bytes=1&max_bytes=4"),
        )
        .await
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

async fn admin_test_post(client: &reqwest::Client, url: String) -> reqwest::RequestBuilder {
    let mut metrics_url = reqwest::Url::parse(&url).expect("admin URL");
    metrics_url.set_path("/__ursula/metrics");
    metrics_url.set_query(None);
    let metrics: serde_json::Value = client
        .get(metrics_url)
        .send()
        .await
        .expect("metrics")
        .json()
        .await
        .expect("JSON");
    client.post(url).header(
        ursula_proto::admin::PROCESS_INCARNATION_HEADER,
        metrics["process_incarnation"].as_str().expect("identity"),
    )
}

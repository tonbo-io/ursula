#![expect(
    clippy::unwrap_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::string_slice,
    reason = "integration tests assert by panicking, as clippy.toml allows for unit tests"
)]
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

fn meta_auth_token_file(config: &Path) -> PathBuf {
    let path = config
        .parent()
        .expect("config parent")
        .join("meta-auth.token");
    if !path.exists() {
        std::fs::write(&path, format!("{:032x}", rand::random::<u128>()))
            .expect("write shared meta authentication token");
    }
    path
}

struct ChildGuard {
    child: Child,
    label: String,
    stderr_path: PathBuf,
    config_path: Option<PathBuf>,
}

impl Drop for ChildGuard {
    #[expect(
        clippy::let_underscore_must_use,
        reason = "Drop must not panic, and the child may already have exited"
    )]
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        if !std::thread::panicking() {
            remove_test_path(&self.stderr_path);
            if let Some(config) = &self.config_path {
                remove_test_path(config);
            }
        }
    }
}

#[cfg(unix)]
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
    remove_test_path(&root);
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
    let deadline = std::time::Instant::now()
        .checked_add(Duration::from_secs(30))
        .unwrap();
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

    // The graceful exit stopped the groups, synced the journals and recorded
    // a clean run, so the restart reads its journals strictly.
    let mut child = spawn_node_with_cluster_config(binary, &config_path);
    wait_until_ready(&client, &base_url, std::slice::from_mut(&mut child)).await;
    let recovery = wal_recovery(&client, &base_url).await;
    assert_eq!(recovery["previous_run"]["kind"], "clean", "{recovery}");
    assert_eq!(recovery["replay_mode"], "strict", "{recovery}");
    assert_eq!(recovery["recovery"]["state"], "normal", "{recovery}");

    // A killed process records nothing, so the next start reads it as a crash:
    // a process crash where the kernel reports a boot id, a host crash where
    // it does not. Under the default `fsync = "never"` a host crash may have
    // cost the node its unsynced tail, so it recovers.
    child.child.kill().expect("kill the node");
    child.child.wait().expect("reap the node");
    let mut child = spawn_node_with_cluster_config(binary, &config_path);
    wait_until_ready(&client, &base_url, std::slice::from_mut(&mut child)).await;
    let recovery = wal_recovery(&client, &base_url).await;
    let (expected_run, expected_state) = if cfg!(target_os = "linux") {
        ("process_crash", "normal")
    } else {
        ("host_crash", "recovering")
    };
    assert_eq!(recovery["previous_run"]["kind"], expected_run, "{recovery}");
    assert_eq!(recovery["recovery"]["state"], expected_state, "{recovery}");
    drop(child);

    std::fs::remove_dir_all(&root).expect("remove temp root");
}

/// How the node's Raft WAL opened, from its metrics JSON.
async fn wal_recovery(client: &reqwest::Client, base_url: &str) -> serde_json::Value {
    let metrics: serde_json::Value = client
        .get(format!("{base_url}/__ursula/metrics"))
        .send()
        .await
        .expect("metrics")
        .json()
        .await
        .expect("metrics JSON");
    metrics["wal_recovery"].clone()
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
        .zip(1_u64..)
        .map(|(port, node_id)| (node_id, format!("http://127.0.0.1:{port}")))
        .collect();

    let wal_root = tempfile::tempdir().expect("WAL root");
    let children = vec![
        spawn_node(binary, 2, ports[1], &peers, false, wal_root.path()),
        spawn_node(binary, 3, ports[2], &peers, false, wal_root.path()),
        spawn_node(binary, 1, ports[0], &peers, true, wal_root.path()),
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
    remove_test_path(&root);
    std::fs::create_dir_all(&root).expect("create temp root");
    let config_path = root.join("cluster.toml");
    let log_dir = root.join("raft-log");

    // Every exit here is a kill. Under `fsync = "always"` the restart serves
    // on every platform; under `never` a platform without a boot id reads the
    // kill as a host crash and the single voter waits for an operator
    // (`cli_single_voter_with_an_unknown_history_serves_after_accept_unsynced_loss`).
    write_single_node_cluster_config(&config_path, port, 1, 1, &base_url, true, &log_dir);
    set_wal_fsync(&config_path, "always");
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

    assert!(
        core_journal_record_bytes(&log_dir.join("raft-log").join("core-0")) > 0,
        "core journal should contain records"
    );

    // Configuration changes must exit before opening listeners or touching
    // the old run state. Restoring the original counts below must still read
    // the acknowledged payload.
    let wal_root = log_dir.join("raft-log");
    let before = std::fs::read(wal_root.join("run-state.bin")).unwrap();
    for key in ["core_count", "group_count"] {
        write_single_node_cluster_config(&config_path, port, 1, 1, &base_url, true, &log_dir);
        set_wal_fsync(&config_path, "always");
        let config = std::fs::read_to_string(&config_path).unwrap();
        std::fs::write(
            &config_path,
            config.replace(&format!("{key} = 1"), &format!("{key} = 2")),
        )
        .unwrap();
        let mut child = spawn_node_with_cluster_config(binary, &config_path);
        let status = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if let Some(status) = child.child.try_wait().unwrap() {
                    break status;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("changed WAL topology must refuse startup");
        assert!(!status.success(), "changed topology must not start");
        assert_eq!(
            std::fs::read(wal_root.join("run-state.bin")).unwrap(),
            before
        );
    }

    write_single_node_cluster_config(&config_path, port, 1, 1, &base_url, true, &log_dir);
    set_wal_fsync(&config_path, "always");
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

/// A single voter whose run state is gone while its journal holds records
/// cannot know whether it lost acknowledged writes: it comes back gated, its
/// group has no leader and readiness says why. Once the gate reports the
/// group stalled, an operator who accepts the loss of the unsynced tail for
/// the log the metrics show opens the gate, and the node serves again with
/// what its journal kept.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cli_single_voter_with_an_unknown_history_serves_after_accept_unsynced_loss() {
    let _guard = static_cluster_cli_test_guard().await;
    let Some(binary) = option_env!("CARGO_BIN_EXE_ursula") else {
        tracing::warn!("CARGO_BIN_EXE_ursula is not set; skipping CLI recovery gate test");
        return;
    };
    let port = free_port();
    let base_url = format!("http://127.0.0.1:{port}");
    let root = tempfile::tempdir().expect("temp root");
    let config_path = root.path().join("cluster.toml");
    let log_dir = root.path().join("raft-log");
    let admin_port =
        write_single_node_cluster_config(&config_path, port, 1, 1, &base_url, true, &log_dir);
    let admin_url = format!("http://127.0.0.1:{admin_port}");
    let client = reqwest::Client::new();
    let stream_url = format!("{base_url}/benchcmp/cli-unknown-history");
    {
        let mut child = spawn_node_with_cluster_config(binary, &config_path);
        wait_until_ready(&client, &base_url, std::slice::from_mut(&mut child)).await;
        put_until_created(&client, &stream_url).await;
        post_until_no_content(&client, &stream_url, "kept-by-the-page-cache").await;
        // Killed: the page cache keeps every write.
    }
    std::fs::remove_file(log_dir.join("raft-log").join("run-state.bin"))
        .expect("remove the run state");

    let mut child = spawn_node_with_cluster_config(binary, &config_path);
    wait_until_ready(&client, &base_url, std::slice::from_mut(&mut child)).await;
    let recovery = wal_recovery(&client, &base_url).await;
    assert_eq!(recovery["previous_run"]["kind"], "unrecorded", "{recovery}");
    assert_eq!(
        recovery["recovery"]["reason"], "unknown_history",
        "{recovery}"
    );
    let ready = client
        .get(format!("{base_url}/__ursula/ready"))
        .send()
        .await
        .expect("readiness");
    assert_eq!(ready.status(), reqwest::StatusCode::SERVICE_UNAVAILABLE);
    let ready: serde_json::Value = ready.json().await.expect("readiness JSON");
    assert_eq!(ready["reason"], "recovery_gate_closed", "{ready}");

    let accept_url = format!("{admin_url}/__ursula/raft/0/recovery/accept-unsynced-loss");
    let observe = || async {
        let metrics: serde_json::Value = client
            .get(format!("{admin_url}/__ursula/metrics"))
            .send()
            .await
            .expect("metrics")
            .json()
            .await
            .expect("metrics JSON");
        let group = &metrics["raft_groups"][0];
        ursula_proto::admin::AcceptUnsyncedLossRequest {
            expected_last_log_index: group["last_log_index"].as_u64(),
            expected_current_term: group["current_term"].as_u64().expect("current term"),
        }
    };
    // Not stalled yet: the gate may still open through a barrier.
    let early = admin_test_post(&client, accept_url.clone())
        .await
        .json(&observe().await)
        .send()
        .await
        .expect("accept before the stall");
    assert_eq!(early.status(), reqwest::StatusCode::CONFLICT);
    let stall_deadline = std::time::Instant::now()
        .checked_add(ursula_raft::RECOVERY_STALL_AFTER.saturating_add(Duration::from_secs(30)))
        .unwrap();
    loop {
        let ready: serde_json::Value = client
            .get(format!("{base_url}/__ursula/ready"))
            .send()
            .await
            .expect("readiness")
            .json()
            .await
            .expect("readiness JSON");
        if ready["reason"] == "recovery_stalled" {
            break;
        }
        assert!(
            std::time::Instant::now() < stall_deadline,
            "the gated single voter never reported its group stalled: {ready}"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }

    let response = admin_test_post(&client, accept_url)
        .await
        .json(&observe().await)
        .send()
        .await
        .expect("accept the unsynced loss");
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    let report: ursula_raft::AcceptUnsyncedLossReport =
        response.json().await.expect("typed report");
    assert_eq!(
        report.outcome,
        ursula_raft::AcceptUnsyncedLossOutcome::GateOpened
    );
    let payload =
        read_until_replicated(&client, &format!("{stream_url}?offset=0&max_bytes=64")).await;
    assert_eq!(payload, b"kept-by-the-page-cache");
    post_until_no_content(&client, &stream_url, "-after").await;
    drop(child);
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
        .zip(1_u64..)
        .map(|(port, node_id)| (node_id, format!("http://127.0.0.1:{port}")))
        .collect();
    let root = std::env::temp_dir().join(format!(
        "ursula-cli-durable-cluster-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system time after unix epoch")
            .as_nanos()
    ));
    remove_test_path(&root);
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
    let phase = std::cell::Cell::new("spawned");
    // Bound the entire exercise, including any HTTP request/body future. Do
    // not add request retries: an interrupted append has an unknown outcome.
    let exercise = tokio::time::timeout(Duration::from_secs(60), async {
    phase.set("initial readiness");
    for (_, base_url) in &peers {
        wait_until_ready(&client, base_url, &mut children).await;
    }
    phase.set("create stream");
    put_until_created(
        &client,
        &format!("{}/benchcmp/cli-durable-cluster", peers[0].1),
    )
    .await;
    phase.set("initial append");
    post_until_no_content(
        &client,
        &format!("{}/benchcmp/cli-durable-cluster", peers[0].1),
        "cli-durable-cluster-payload",
    )
    .await;

    phase.set("initial follower read");
    let payload = read_until_replicated(
        &client,
        &format!(
            "{}/benchcmp/cli-durable-cluster?offset=0&max_bytes=64",
            peers[2].1
        ),
    )
    .await;
    assert_eq!(payload, b"cli-durable-cluster-payload");

    // Metrics and forwarded reads do not prove local recovery is complete.
    // Capture applied (committed) prefixes before faulting this healthy cluster.
    phase.set("capture healthy cluster prefixes");
    let mut prefixes = std::collections::BTreeMap::<u64, u64>::new();
    for (_, url) in &peers {
        let metrics: ursula_proto::admin::NodeMetrics = client
            .get(format!("{url}/__ursula/metrics"))
            .send().await.expect("local metrics")
            .error_for_status().expect("metrics status")
            .json().await.expect("typed local metrics");
        for group in metrics.raft_groups {
            if let Some(index) = group.last_applied_index {
                prefixes.entry(group.raft_group_id)
                    .and_modify(|bound| *bound = (*bound).max(index))
                    .or_insert(index);
            }
        }
    }
    assert_eq!(prefixes.keys().copied().collect::<Vec<_>>(), vec![0, 1, 2, 3],
        "each initialized group must have an actual applied prefix");
    phase.set("all replicas locally caught up before fault");
    for (node_id, url) in &peers {
        loop {
            let metrics: ursula_proto::admin::NodeMetrics = client
                .get(format!("{url}/__ursula/metrics"))
                .send().await.expect("local metrics")
                .error_for_status().expect("metrics status")
                .json().await.expect("typed local metrics");
            let healthy = metrics.diagnostics.recovery_gates.as_ref()
                .is_some_and(|report| report.gated.is_empty())
                && prefixes.iter().all(|(group_id, bound)| metrics.raft_groups.iter()
                    .any(|group| group.raft_group_id == *group_id
                        && group.node_id == *node_id
                        && group.voter_ids == [1, 2, 3]
                        && group.last_applied_index.is_some_and(|index| index >= *bound)));
            if healthy { break; }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    // Restart one follower, append while it is absent, then prove the same
    // disk WAL can reopen and catch up without duplicate application.
    phase.set("stop follower3");
    drop(children.remove(1));
    phase.set("append while follower3 absent");
    post_until_no_content(
        &client,
        &format!("{}/benchcmp/cli-durable-cluster", peers[0].1),
        "-after-follower-restart",
    )
    .await;
    phase.set("restart follower3");
    children.push(spawn_node_with_cluster_config(binary, &configs[2]));
    wait_until_ready(&client, &peers[2].1, &mut children).await;
    phase.set("restarted follower3 catchup read");
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
    phase.set("journal persistence");
    for node_id in 1..=3 {
        let core_dir = root
            .join(format!("node-{node_id}-log"))
            .join("raft-log")
            .join("core-0");
        let mut last_len = 0u64;
        for _ in 0..100 {
            last_len = core_journal_record_bytes(&core_dir);
            if last_len > 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert!(
            last_len > 0,
            "node {node_id} journal should contain records after polling for ~5s (saw len={last_len})",
        );
    }

    }).await;
    if exercise.is_err() {
        let diagnostic = reqwest::Client::builder()
            .timeout(Duration::from_secs(2))
            .build()
            .expect("diagnostic client");
        let mut metrics = Vec::new();
        for (_, url) in &peers {
            let observed = match diagnostic
                .get(format!("{url}/__ursula/metrics"))
                .send()
                .await
            {
                Ok(response) => response.text().await,
                Err(error) => Err(error),
            };
            metrics.push((url, observed));
        }
        let reports = children.iter().map(child_report).collect::<Vec<_>>();
        panic!(
            "durable replication deadline in phase {}; metrics={metrics:#?}; children={reports:#?}",
            phase.get()
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
        .zip(1_u64..)
        .map(|(port, node_id)| (node_id, format!("http://127.0.0.1:{port}")))
        .collect();
    let meta_ports = [free_port(), free_port(), free_port()];
    let root = std::env::temp_dir().join(format!(
        "ursula-cli-durable-late-learner-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system time after unix epoch")
            .as_nanos()
    ));
    remove_test_path(&root);
    std::fs::create_dir_all(&root).expect("create temp root");

    let node1_config = root.join("node-1.toml");
    let node2_config = root.join("node-2.toml");
    let node3_config = root.join("node-3.toml");
    let node1_admin_port = write_cluster_config(
        &node1_config,
        ports[0],
        1,
        1,
        &peers,
        true,
        &root.join("node-1-log"),
    );
    let node1_admin = format!("http://127.0.0.1:{node1_admin_port}");
    let node2_admin_port = write_cluster_config(
        &node2_config,
        ports[1],
        2,
        1,
        &peers,
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

    // Every process joins meta genesis, but node3 initially owns no data group.
    // Its data replica is admitted only by the later durable Move intent.
    for (index, config) in [&node1_config, &node2_config, &node3_config]
        .iter()
        .enumerate()
    {
        let mut contents = std::fs::read_to_string(config)
            .unwrap()
            .replace("meta = { enabled = false }\n", "")
            .replace("flush_interval = \"1s\"", "flush_interval = \"1h\"");
        contents.push_str(&format!("\n[[raft.groups]]\nraft_group_id = 0\nvoters = [1, 2]\n\n[raft.meta]\nenabled = true\nlisten = \"127.0.0.1:{}\"\n", meta_ports[index]));
        let auth_token = meta_auth_token_file(config.as_ref());
        contents.push_str(&format!("auth_token_file = {:?}\n", auth_token));
        for (peer_index, port) in meta_ports.iter().enumerate() {
            let id = peer_index.saturating_add(1);
            contents.push_str(&format!(
                "\n[[raft.meta.peers]]\nnode_id = {id}\nurl = \"http://127.0.0.1:{port}\"\n"
            ));
        }
        std::fs::write(config, contents).unwrap();
    }
    let mut children = vec![
        spawn_node_with_cluster_config(binary, &node2_config),
        spawn_node_with_cluster_config(binary, &node1_config),
        spawn_node_with_cluster_config(binary, &node3_config),
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

    // Purge the other original owner too, so leadership movement cannot let
    // the target catch up solely through retained logs instead of a snapshot.
    let node2_admin = format!("http://127.0.0.1:{node2_admin_port}");
    let started = std::time::Instant::now();
    loop {
        let metrics: ursula_proto::admin::NodeMetrics = client
            .get(format!("{node2_admin}/__ursula/metrics"))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        if metrics.raft_groups.iter().any(|group| {
            group.raft_group_id == 0 && group.last_applied_index >= Some(snapshot_index)
        }) {
            break;
        }
        assert!(
            started.elapsed() < Duration::from_secs(15),
            "second owner did not apply snapshot prefix {snapshot_index}: {metrics:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let snapshot2 = admin_test_post(&client, format!("{node2_admin}/__ursula/raft/0/snapshot"))
        .await
        .send()
        .await
        .unwrap();
    let snapshot2_status = snapshot2.status();
    let snapshot2_body = snapshot2.text().await.unwrap();
    assert_eq!(
        snapshot2_status,
        reqwest::StatusCode::OK,
        "{snapshot2_body}"
    );
    let snapshot2_index =
        serde_json::from_str::<serde_json::Value>(&snapshot2_body).unwrap()["snapshot_index"]
            .as_u64()
            .unwrap();
    assert!(
        snapshot2_index >= snapshot_index,
        "snapshot2={snapshot2_body}, required={snapshot_index}"
    );
    let purge2 = admin_test_post(
        &client,
        format!("{node2_admin}/__ursula/raft/0/purge?upto={snapshot2_index}"),
    )
    .await
    .send()
    .await
    .unwrap();
    let purge2_status = purge2.status();
    let purge2_body = purge2.text().await.unwrap();
    assert_eq!(purge2_status, reqwest::StatusCode::OK, "{purge2_body}");
    wait_until_ready(&client, &peers[2].1, &mut children).await;
    let before: ursula_proto::admin::NodeMetrics = client
        .get(format!("{}/__ursula/metrics", peers[2].1))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(
        before.raft_groups.is_empty(),
        "target must begin with no data replica"
    );
    let begin = admin_test_post(&client, format!("{node1_admin}/__ursula/control/operation"))
        .await
        .json(&ursula_control::OperationRequest::Begin {
            kind: ursula_control::OperationKind::MoveReplicas {
                source: 2,
                target: 3,
                groups: std::collections::BTreeSet::from([ursula_shard::RaftGroupId(0)]),
            },
            executor: ursula_proto::admin::ProcessIncarnation::from_bits(42),
        })
        .send()
        .await
        .unwrap();
    assert_eq!(begin.status(), reqwest::StatusCode::OK);
    let response: ursula_control::ControlResponse = begin.json().await.unwrap();
    let ursula_control::ControlResponse::Operation(Ok(ursula_control::OperationOutcome::Acquired(
        token,
    ))) = response
    else {
        panic!("expected Move admission: {response:?}")
    };
    for request in [
        ursula_control::OperationRequest::Reconcile {
            token: token.clone(),
        },
        ursula_control::OperationRequest::CollectEvidence {
            token: token.clone(),
        },
        ursula_control::OperationRequest::Complete { token },
    ] {
        let response =
            admin_test_post(&client, format!("{node1_admin}/__ursula/control/operation"))
                .await
                .json(&request)
                .send()
                .await
                .unwrap();
        let status = response.status();
        let body = response.text().await.unwrap();
        assert_eq!(status, reqwest::StatusCode::OK, "{request:?}: {body}");
        assert!(
            matches!(
                serde_json::from_str::<ursula_control::ControlResponse>(&body).unwrap(),
                ursula_control::ControlResponse::Operation(Ok(_))
            ),
            "{body}"
        );
    }
    let after: ursula_proto::admin::NodeMetrics = client
        .get(format!("{}/__ursula/metrics", peers[2].1))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let group = after
        .raft_groups
        .iter()
        .find(|group| group.raft_group_id == 0)
        .unwrap();
    assert!(group.has_snapshot && group.snapshot_index >= Some(snapshot_index));
    assert_eq!(group.voter_ids, vec![1, 3]);

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
        .zip(1_u64..)
        .map(|(port, node_id)| (node_id, format!("http://127.0.0.1:{port}")))
        .collect();
    let root = std::env::temp_dir().join(format!(
        "ursula-cli-s3-cold-cluster-restart-{}-{suffix}",
        std::process::id()
    ));
    remove_test_path(&root);
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
            &log_dir,
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
            &log_dir,
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

fn spawn_per_group_node(
    binary: &str,
    node_id: u64,
    port: u16,
    peers: &[(u64, String)],
    start_drained: bool,
    wal_root: &Path,
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
        &node_wal_dir(wal_root, node_id),
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
async fn cli_sigterm_hands_off_leaders_and_bounds_quorum_loss() {
    let _guard = static_cluster_cli_test_guard().await;
    let binary = env!("CARGO_BIN_EXE_ursula");
    let ports = [free_port(), free_port(), free_port()];
    let peers = ports
        .iter()
        .zip(1_u64..)
        .map(|(port, node_id)| (node_id, format!("http://127.0.0.1:{port}")))
        .collect::<Vec<_>>();
    let wal_root = tempfile::tempdir().expect("WAL root");
    let mut children = Vec::new();
    let mut nodes = Vec::new();
    for (node_id, url) in &peers {
        let (child, admin_port) = spawn_per_group_node(
            binary,
            *node_id,
            node_port(&ports, *node_id),
            &peers,
            false,
            wal_root.path(),
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
    let deadline = std::time::Instant::now()
        .checked_add(Duration::from_secs(60))
        .unwrap();
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
    let deadline = std::time::Instant::now()
        .checked_add(Duration::from_secs(10))
        .unwrap();
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

/// Port of 1-based `node_id` in the three-node `ports` array.
fn node_port(ports: &[u16; 3], node_id: u64) -> u16 {
    ports[usize::try_from(node_id.checked_sub(1).unwrap()).unwrap()]
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
    wal_path: &Path,
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
meta = {{ enabled = false }}
"#
    )
    .unwrap();

    writeln!(
        config,
        r#"[raft.wal]
path = "{}"
"#,
        wal_path.display()
    )
    .unwrap();

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

/// The WAL directory of node `node_id` under a test's `wal_root`.
fn node_wal_dir(wal_root: &Path, node_id: u64) -> PathBuf {
    wal_root.join(format!("node-{node_id}"))
}

fn spawn_node(
    binary: &str,
    node_id: u64,
    port: u16,
    peers: &[(u64, String)],
    init_membership: bool,
    wal_root: &Path,
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
        &node_wal_dir(wal_root, node_id),
        "memory",
        None,
    );
    let mut command = Command::new(binary);
    command.arg("server").arg("--config").arg(&config_path);
    let mut guard = spawn_child(command, format!("node-{node_id}-{port}"));
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

/// Sets `raft.wal.fsync` in the node config at `path`.
fn set_wal_fsync(path: &Path, fsync: &str) {
    let config = std::fs::read_to_string(path).expect("read node config");
    assert!(
        config.contains("[raft.wal]\n"),
        "a node config with a WAL section"
    );
    std::fs::write(
        path,
        config.replace(
            "[raft.wal]\n",
            &format!("[raft.wal]\nfsync = \"{fsync}\"\n"),
        ),
    )
    .expect("write node config");
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
        log_dir,
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
    let mut last_failure = String::new();
    for _ in 0..100 {
        match client
            .put(url)
            .header("content-type", "text/plain")
            .body(payload)
            .send()
            .await
        {
            Ok(response) if response.status() == reqwest::StatusCode::CREATED => return response,
            Ok(response) => {
                let status = response.status();
                last_failure = format!("{status}: {:?}", response.text().await);
            }
            Err(error) => last_failure = format!("{error:?}"),
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("create with payload did not succeed at {url}: {last_failure}");
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

/// Remove a temporary test file or directory, tolerating its absence.
fn remove_test_path(path: impl AsRef<std::path::Path>) {
    let path = path.as_ref();
    let removed = if path.is_dir() {
        std::fs::remove_dir_all(path)
    } else {
        std::fs::remove_file(path)
    };
    if let Err(err) = removed
        && err.kind() != std::io::ErrorKind::NotFound
    {
        panic!("remove test path {}: {err}", path.display());
    }
}

/// The bytes the segments of the core journal in `core_dir` hold beyond
/// their headers; zero before the journal exists.
fn core_journal_record_bytes(core_dir: &Path) -> u64 {
    ursula_raft::wal::journal_segments(core_dir)
        .expect("list the core journal segments")
        .iter()
        .map(|(_, path)| {
            std::fs::metadata(path)
                .map(|metadata| metadata.len().saturating_sub(32))
                .unwrap_or(0)
        })
        .sum()
}

/// The common single-node TOML/CI shape needs only a WAL path. Independent
/// local servers auto-bootstrap private one-voter meta groups without4439 clashes.
#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cli_durable_standalone_automatically_bootstraps_meta_and_restarts() {
    let _guard = static_cluster_cli_test_guard().await;
    let root = tempfile::tempdir().unwrap();
    let binary = env!("CARGO_BIN_EXE_ursula");
    let mut configs = Vec::new();
    let mut urls = Vec::new();
    let mut admins = Vec::new();
    for node in [7, 9] {
        let port = free_port();
        let admin = free_port();
        let config = root.path().join(format!("standalone-{node}.toml"));
        let wal = root.path().join(format!("wal-{node}"));
        std::fs::write(
            &config,
            format!(
                r#"[server]
listen = "127.0.0.1:{port}"
admin_listen = "127.0.0.1:{admin}"
[runtime]
core_count = 1
[raft]
node_id = {node}
group_count = 1
[raft.wal]
path = "{}"
[storage.cold]
flush_interval = "1h"
"#,
                wal.display()
            ),
        )
        .unwrap();
        configs.push(config);
        urls.push(format!("http://127.0.0.1:{port}"));
        admins.push(format!("http://127.0.0.1:{admin}"));
    }
    let mut children: Vec<_> = configs
        .iter()
        .map(|path| spawn_node_with_cluster_config(binary, path))
        .collect();
    let client = reqwest::Client::new();
    for (index, url) in urls.iter().enumerate() {
        wait_until_ready(&client, url, &mut children).await;
        put_with_body_until_created(
            &client,
            &format!("{url}/standalone/persisted"),
            "durable-single",
        )
        .await;
        let state: ursula_control::ControlPlaneState = client
            .get(format!("{}/__ursula/control/state", admins[index]))
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(state.operations.processes.len(), 1);
        assert_eq!(state.placements.len(), 1);
    }
    sigterm_and_wait_for_clean_exit(&mut children[0]).await;
    children[0] = spawn_node_with_cluster_config(binary, &configs[0]);
    wait_until_ready(&client, &urls[0], &mut children).await;
    read_until_matches(
        &client,
        &format!("{}/standalone/persisted?offset=-1", urls[0]),
        b"durable-single",
    )
    .await;
    post_until_no_content(
        &client,
        &format!("{}/standalone/persisted", urls[0]),
        "-restarted",
    )
    .await;
    read_until_matches(
        &client,
        &format!("{}/standalone/persisted?offset=-1", urls[0]),
        b"durable-single-restarted",
    )
    .await;
}

/// Pre-identity WALs require logical backup/restore; startup cannot adopt them.
#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cli_meta_activation_rejects_legacy_identity_wals_without_modification() {
    use std::os::unix::fs::MetadataExt;

    let _guard = static_cluster_cli_test_guard().await;
    let binary = env!("CARGO_BIN_EXE_ursula");
    let root = tempfile::tempdir().unwrap();
    let ports = [free_port(), free_port(), free_port()];
    let meta_ports = [free_port(), free_port(), free_port()];
    let peers: Vec<_> = ports
        .iter()
        .zip(1_u64..)
        .map(|(port, node)| (node, format!("http://127.0.0.1:{port}")))
        .collect();
    let mut configs = Vec::new();
    let mut nodes = Vec::new();
    let mut wal_dirs = Vec::new();
    for (index, (node_id, base)) in peers.iter().enumerate() {
        let config = root.path().join(format!("node-{node_id}.toml"));
        let wal = root.path().join(format!("wal-{node_id}"));
        let admin = write_cluster_config(&config, ports[index], *node_id, 4, &peers, true, &wal);
        let contents = std::fs::read_to_string(&config)
            .unwrap()
            .replace(
                "init_membership_per_group = false",
                "init_membership_per_group = true",
            )
            .replace("flush_interval = \"1s\"", "flush_interval = \"1h\"");
        assert!(contents.contains("meta = { enabled = false }"));
        std::fs::write(&config, contents).unwrap();
        nodes.push(ctl_node(*node_id, admin, base));
        configs.push(config);
        wal_dirs.push(wal);
    }
    let mut children: Vec<_> = configs
        .iter()
        .map(|path| spawn_node_with_cluster_config(binary, path))
        .collect();
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(15))
        .build()
        .unwrap();
    for (_, base) in &peers {
        wait_until_ready(&client, base, &mut children).await;
    }
    let metrics_client = ursula_ctl::MetricsClient::new(Duration::from_secs(10)).unwrap();
    ursula_ctl::wait_ready(
        &metrics_client,
        &nodes,
        4,
        Duration::from_secs(60),
        Duration::from_millis(100),
    )
    .await
    .unwrap();
    let stream = format!("{}/cutover/preserved", peers[0].1);
    put_with_body_until_created(&client, &stream, "before-meta").await;
    let before_head = client.head(&stream).send().await.unwrap();
    assert_eq!(before_head.status(), reqwest::StatusCode::OK);
    let offset = before_head.headers()["stream-next-offset"]
        .to_str()
        .unwrap()
        .to_owned();
    assert_eq!(offset.parse::<usize>().unwrap(), b"before-meta".len());
    for (_, base) in &peers {
        read_until_matches(
            &client,
            &format!("{base}/cutover/preserved?offset=-1"),
            b"before-meta",
        )
        .await;
    }
    let identities: Vec<_> = wal_dirs
        .iter()
        .map(|wal| {
            assert!(!wal.join("meta-raft").exists());
            (
                std::fs::metadata(wal).unwrap().ino(),
                std::fs::read(wal.join("raft-log/FORMAT_EPOCH")).unwrap(),
            )
        })
        .collect();

    // Stop every old listener before starting any meta-enabled process. This
    // avoids the first upgraded pod waiting on peers without meta transport.
    for child in &mut children {
        assert!(
            Command::new("kill")
                .arg("-TERM")
                .arg(child.child.id().to_string())
                .status()
                .unwrap()
                .success()
        );
    }
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        let mut all_stopped = true;
        for child in &mut children {
            if let Some(status) = child.child.try_wait().unwrap() {
                assert!(
                    status.success(),
                    "unclean coordinated shutdown: {}",
                    child_report(child)
                );
            } else {
                all_stopped = false;
            }
        }
        if all_stopped {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "coordinated stop exceeded budget"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    fn wal_bytes(root: &Path) -> std::collections::BTreeMap<PathBuf, Vec<u8>> {
        fn visit(
            root: &Path,
            path: &Path,
            files: &mut std::collections::BTreeMap<PathBuf, Vec<u8>>,
        ) {
            for entry in std::fs::read_dir(path).unwrap() {
                let path = entry.unwrap().path();
                if path.is_dir() {
                    visit(root, &path, files);
                } else {
                    files.insert(
                        path.strip_prefix(root).unwrap().to_path_buf(),
                        std::fs::read(path).unwrap(),
                    );
                }
            }
        }
        let mut files = std::collections::BTreeMap::new();
        visit(root, root, &mut files);
        files
    }
    let preserved: Vec<_> = wal_dirs
        .iter()
        .map(|wal| wal_bytes(&wal.join("raft-log")))
        .collect();
    drop(children);
    for (index, config) in configs.iter().enumerate() {
        let mut contents = std::fs::read_to_string(config)
            .unwrap()
            .replace("meta = { enabled = false }\n", "");
        contents.push_str(&format!(
            "\n[raft.meta]\nenabled = true\nlisten = \"127.0.0.1:{}\"\n",
            meta_ports[index]
        ));
        let auth_token = meta_auth_token_file(config.as_ref());
        contents.push_str(&format!("auth_token_file = {:?}\n", auth_token));
        for (peer_index, port) in meta_ports.iter().enumerate() {
            let node_id = peer_index.saturating_add(1);
            contents.push_str(&format!(
                "\n[[raft.meta.peers]]\nnode_id = {node_id}\nurl = \"http://127.0.0.1:{port}\"\n"
            ));
        }
        std::fs::write(config, contents).unwrap();
    }
    let mut children: Vec<_> = configs
        .iter()
        .map(|path| spawn_node_with_cluster_config(binary, path))
        .collect();
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    for child in &mut children {
        loop {
            if let Some(status) = child.child.try_wait().unwrap() {
                assert!(!status.success(), "legacy WAL was silently adopted");
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "legacy startup did not reject: {}",
                child_report(child)
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
    for (index, wal) in wal_dirs.iter().enumerate() {
        assert_eq!(std::fs::metadata(wal).unwrap().ino(), identities[index].0);
        assert_eq!(
            std::fs::read(wal.join("raft-log/FORMAT_EPOCH")).unwrap(),
            identities[index].1
        );
        let after = wal_bytes(&wal.join("raft-log"));
        let before = &preserved[index];
        let changed = before
            .keys()
            .chain(after.keys())
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .filter(|path| before.get(*path) != after.get(*path))
            .map(|path| {
                (
                    path,
                    before.get(path).map(Vec::len),
                    after.get(path).map(Vec::len),
                )
            })
            .collect::<Vec<_>>();
        assert!(
            changed.is_empty(),
            "rejected startup changed legacy WAL files (path, before length, after length): {changed:?}"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cli_meta_authority_boot_restart_move_rebuild_decommission() {
    use std::collections::BTreeMap;
    use std::collections::BTreeSet;

    use lifecycle_support::operation;
    use lifecycle_support::state;
    use ursula_control::ControlResponse;
    use ursula_control::OperationKind;
    use ursula_control::OperationOutcome;
    use ursula_control::OperationRequest;
    use ursula_proto::admin::ProcessIncarnation;
    use ursula_shard::RaftGroupId;

    let _guard = static_cluster_cli_test_guard().await;
    let root = tempfile::tempdir().expect("drill root");
    let ports: Vec<_> = (0..4).map(|_| free_port()).collect();
    let meta_ports: Vec<_> = (0..4).map(|_| free_port()).collect();
    let peers: Vec<_> = ports
        .iter()
        .enumerate()
        .map(|(i, port)| {
            (
                u64::try_from(i).expect("node index").saturating_add(1),
                format!("http://127.0.0.1:{port}"),
            )
        })
        .collect();
    let mut configs = Vec::new();
    let mut admins = Vec::new();
    for (index, (node_id, _)) in peers.iter().enumerate() {
        let config = root.path().join(format!("node-{node_id}.toml"));
        let wal = root.path().join(format!("wal-{node_id}"));
        let bootstrap_peers = if *node_id == 4 {
            &peers[..]
        } else {
            &peers[..3]
        };
        let admin = write_cluster_config(
            &config,
            ports[index],
            *node_id,
            1,
            bootstrap_peers,
            *node_id != 4,
            &wal,
        );
        let mut contents = std::fs::read_to_string(&config)
            .expect("read generated config")
            .replace("meta = { enabled = false }\n", "")
            // This process drill exercises replicated WAL/snapshots. Each fixture's
            // memory cold backend is private; shared S3 recovery has its own suite.
            .replace("flush_interval = \"1s\"", "flush_interval = \"1h\"");
        contents.push_str(&format!("\n[[raft.groups]]\nraft_group_id = 0\nvoters = [1, 2, 3]\n\n[raft.meta]\nenabled = true\nlisten = \"127.0.0.1:{}\"\n", meta_ports[index]));
        let auth_token = meta_auth_token_file(config.as_ref());
        contents.push_str(&format!("auth_token_file = {:?}\n", auth_token));
        for (peer_index, port) in meta_ports
            .iter()
            .take(if *node_id == 4 { 4 } else { 3 })
            .enumerate()
        {
            let id = peer_index.saturating_add(1);
            contents.push_str(&format!(
                "\n[[raft.meta.peers]]\nnode_id = {id}\nurl = \"http://127.0.0.1:{port}\"\n"
            ));
        }
        std::fs::write(&config, contents).expect("write meta config");
        configs.push(config);
        admins.push(format!("http://127.0.0.1:{admin}"));
    }
    let binary = env!("CARGO_BIN_EXE_ursula");
    let mut children: Vec<_> = configs
        .iter()
        .take(3)
        .map(|config| spawn_node_with_cluster_config(binary, config))
        .collect();
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(60))
        .build()
        .expect("client");
    for (_, url) in peers.iter().take(3) {
        wait_until_ready(&client, url, &mut children).await;
    }
    let admin = &admins[0];
    let genesis = state(&client, admin, concat!(file!(), ":", line!())).await;
    assert_eq!(genesis.operations.processes.len(), 3);
    assert!(!genesis.nodes.contains_key(&4));
    let stream = format!("{}/meta/drill", peers[0].1);
    let read = format!("{stream}?offset=-1");
    put_with_body_until_created(&client, &stream, "durable-before-maintenance").await;
    // A coordinated clean restart preserves Never WAL durability. Killing all
    // voters on a platform without a boot ID correctly requires loss recovery.
    futures_util::future::join_all(children.iter_mut().map(sigterm_and_wait_for_clean_exit)).await;
    children = configs
        .iter()
        .take(3)
        .map(|config| spawn_node_with_cluster_config(binary, config))
        .collect();
    for (_, url) in peers.iter().take(3) {
        wait_until_ready(&client, url, &mut children).await;
    }
    let restarted_genesis = state(&client, admin, concat!(file!(), ":", line!())).await;
    assert_eq!(restarted_genesis.placements, genesis.placements);
    assert_eq!(
        restarted_genesis.operations.replicas,
        genesis.operations.replicas
    );
    for node_id in [1, 2, 3] {
        assert!(
            restarted_genesis.operations.processes[&node_id].epoch()
                > genesis.operations.processes[&node_id].epoch()
        );
    }
    read_until_matches(&client, &read, b"durable-before-maintenance").await;
    let response = admin_test_post(&client, format!("{admin}/__ursula/control/operation"))
        .await
        .json(&OperationRequest::JoinNode {
            node_id: 4,
            client_url: peers[3].1.clone(),
            cluster_url: peers[3].1.clone(),
            meta_url: format!("http://127.0.0.1:{}", meta_ports[3]),
        })
        .send()
        .await
        .unwrap();
    let status = response.status();
    let body = response.text().await.unwrap();
    assert!(status.is_success(), "join: {status}: {body}");
    assert_eq!(
        serde_json::from_str::<ControlResponse>(&body).unwrap(),
        ControlResponse::Ok
    );
    children.push(spawn_node_with_cluster_config(binary, &configs[3]));
    wait_until_ready(&client, &peers[3].1, &mut children).await;
    let initial = state(&client, admin, concat!(file!(), ":", line!())).await;
    assert_eq!(initial.operations.processes.len(), 4);
    assert_eq!(initial.placements, genesis.placements);
    read_until_matches(&client, &read, b"durable-before-maintenance").await;

    // An ordinary same-PV restart is a CAS boot claim, not a rebuild intent.
    children[1].child.kill().expect("stop node2");
    children[1].child.wait().expect("join node2");
    children[1] = spawn_node_with_cluster_config(binary, &configs[1]);
    wait_until_ready(&client, &peers[1].1, &mut children).await;
    let restarted = state(&client, admin, concat!(file!(), ":", line!())).await;
    assert!(restarted.operations.processes[&2].epoch() > initial.operations.processes[&2].epoch());
    assert_eq!(restarted.placements, initial.placements);
    read_until_matches(&client, &read, b"durable-before-maintenance").await;
    let started = std::time::Instant::now();
    loop {
        let metrics: ursula_proto::admin::NodeMetrics = client
            .get(format!("{}/__ursula/metrics", admins[1]))
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .await
            .unwrap();
        if metrics
            .diagnostics
            .recovery_gates
            .as_ref()
            .is_some_and(|report| report.gated.is_empty())
            && metrics
                .raft_groups
                .iter()
                .any(|group| group.raft_group_id == 0)
        {
            break;
        }
        assert!(
            started.elapsed() < Duration::from_secs(30),
            "node2 did not recover before maintenance: {metrics:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    for (sequence, kind) in [
        OperationKind::MoveReplicas {
            source: 3,
            target: 4,
            groups: BTreeSet::from([RaftGroupId(0)]),
        },
        OperationKind::RebuildReplica { node_id: 2 },
        OperationKind::MoveReplicas {
            source: 1,
            target: 3,
            groups: BTreeSet::from([RaftGroupId(0)]),
        },
        OperationKind::DecommissionNode {
            node_id: 3,
            replacements: BTreeMap::from([(RaftGroupId(0), 1)]),
        },
    ]
    .into_iter()
    .enumerate()
    {
        let required_leader = match &kind {
            OperationKind::RebuildReplica { .. } => Some(2_u64),
            // Returning node3 has missed node2's replacement fence. Force the
            // new identity to lead its catchup, rather than letting another
            // leader conceal an obsolete admission map.
            OperationKind::MoveReplicas {
                source: 1,
                target: 3,
                ..
            } => Some(2),
            // Preserve direct coverage of removing the incumbent leader.
            OperationKind::DecommissionNode { node_id: 3, .. } => Some(3),
            _ => None,
        };
        if let Some(required_leader) = required_leader {
            let started = std::time::Instant::now();
            loop {
                let metrics: ursula_proto::admin::NodeMetrics = client
                    .get(format!("{}/__ursula/metrics", admins[1]))
                    .send()
                    .await
                    .unwrap()
                    .json()
                    .await
                    .unwrap();
                let leader = metrics
                    .raft_groups
                    .iter()
                    .find(|group| group.raft_group_id == 0)
                    .and_then(|group| group.current_leader);
                if leader == Some(required_leader) {
                    break;
                }
                assert!(
                    started.elapsed() < Duration::from_secs(30),
                    "required node {required_leader} never became leader"
                );
                if let Some(leader) = leader {
                    let index = usize::try_from(leader).unwrap().checked_sub(1).unwrap();
                    let response = admin_test_post(
                        &client,
                        format!(
                            "{}/__ursula/raft/0/leader/transfer/{required_leader}",
                            admins[index]
                        ),
                    )
                    .await
                    .send()
                    .await
                    .unwrap();
                    assert!(
                        response.status().is_success()
                            || response.status() == reqwest::StatusCode::CONFLICT
                    );
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
        let outcome = operation(
            &client,
            admin,
            OperationRequest::Begin {
                kind: kind.clone(),
                executor: ProcessIncarnation::from_bits(
                    u128::try_from(sequence)
                        .expect("sequence")
                        .saturating_add(100),
                ),
            },
            &children,
        )
        .await;
        let OperationOutcome::Acquired(token) = outcome else {
            panic!("expected acquired operation")
        };
        if matches!(kind, OperationKind::MoveReplicas { source: 3, .. }) {
            // Same-WAL restarts of source, target and survivor during Preparing
            // must refresh boot pins without changing the durable replica token.
            for index in [2_usize, 3, 1] {
                let before = state(&client, admin, concat!(file!(), ":", line!())).await;
                children[index]
                    .child
                    .kill()
                    .expect("restart operation participant");
                children[index]
                    .child
                    .wait()
                    .expect("join operation participant");
                children[index] = spawn_node_with_cluster_config(binary, &configs[index]);
                wait_until_ready(&client, &peers[index].1, &mut children).await;
                let after = state(&client, admin, concat!(file!(), ":", line!())).await;
                let node_id = u64::try_from(index).unwrap().saturating_add(1);
                assert!(
                    after.operations.processes[&node_id].epoch()
                        > before.operations.processes[&node_id].epoch()
                );
                assert_eq!(
                    after.operations.replicas[&node_id],
                    before.operations.replicas[&node_id]
                );
                assert_eq!(after.operations.active.as_ref().unwrap().token, token);
                // Metrics HTTP availability does not mean a Never-mode crash
                // has passed its survivor barrier. Recover this participant
                // before removing another member of the same quorum.
                let process = match &after.operations.processes[&node_id] {
                    ursula_control::ProcessState::Active(process) => process,
                    other => panic!("restarted participant is not active: {other:?}"),
                };
                let started = std::time::Instant::now();
                loop {
                    let metrics: ursula_proto::admin::NodeMetrics = client
                        .get(format!("{}/__ursula/metrics", admins[index]))
                        .send()
                        .await
                        .unwrap()
                        .error_for_status()
                        .unwrap()
                        .json()
                        .await
                        .unwrap();
                    let recovery_ready = if node_id == 4 {
                        // This target has never joined a data membership. Its
                        // closed gate persists until Reconcile adds the learner.
                        metrics.raft_groups.iter().all(|group| {
                            group.voter_ids.is_empty()
                                && group.learner_ids.is_empty()
                                && group.last_log_index.is_none()
                        })
                    } else {
                        metrics
                            .diagnostics
                            .recovery_gates
                            .as_ref()
                            .is_some_and(|report| report.gated.is_empty())
                            && metrics
                                .raft_groups
                                .iter()
                                .any(|group| group.raft_group_id == 0)
                    };
                    if metrics.process_incarnation.as_ref() == Some(&process.incarnation)
                        && recovery_ready
                    {
                        break;
                    }
                    assert!(
                        started.elapsed() < Duration::from_secs(30),
                        "participant {node_id} did not recover: {metrics:?}"
                    );
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                read_until_matches(&client, &read, b"durable-before-maintenance").await;
            }
        }
        if matches!(kind, OperationKind::DecommissionNode { .. }) {
            // Populate and promote replacements while the source still serves.
            operation(
                &client,
                admin,
                OperationRequest::Reconcile {
                    token: token.clone(),
                },
                &children,
            )
            .await;
        }
        if !matches!(kind, OperationKind::MoveReplicas { .. }) {
            operation(
                &client,
                admin,
                OperationRequest::CollectEvidence {
                    token: token.clone(),
                },
                &children,
            )
            .await;
            operation(
                &client,
                admin,
                OperationRequest::RetireSource {
                    token: token.clone(),
                },
                &children,
            )
            .await;
        }
        if matches!(kind, OperationKind::RebuildReplica { .. }) {
            children[1].child.kill().expect("stop retired node2");
            children[1].child.wait().expect("join retired node2");
            let retired = state(
                &client,
                admin,
                "after stopping retired node2, before old-WAL restart",
            )
            .await;
            // Restarting the revoked old WAL is not a replacement. It must
            // fail before changing the retired process/replica authority.
            children[1] = spawn_node_with_cluster_config(binary, &configs[1]);
            let deadline = std::time::Instant::now() + Duration::from_secs(30);
            loop {
                if let Some(status) = children[1].child.try_wait().unwrap() {
                    assert!(!status.success(), "retired WAL unexpectedly booted");
                    break;
                }
                assert!(
                    std::time::Instant::now() < deadline,
                    "retired WAL did not fail closed"
                );
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            let rejected = state(&client, admin, concat!(file!(), ":", line!())).await;
            assert_eq!(
                rejected.operations.processes[&2],
                retired.operations.processes[&2]
            );
            assert_eq!(
                rejected.operations.replicas[&2],
                retired.operations.replicas[&2]
            );
            // Remove the entire retired PV, including meta votes/snapshots and
            // data journals. The meta survivor quorum must authorize a fresh
            // durable vote floor before this replica can participate again.
            std::fs::remove_dir_all(root.path().join("wal-2"))
                .expect("remove retired whole-node storage");
            children[1] = spawn_node_with_cluster_config(binary, &configs[1]);
            // A new WAL remains meta-only until Reconcile durably fences its
            // replacement identity on every survivor group and activates it.
            let deadline = std::time::Instant::now() + Duration::from_secs(30);
            loop {
                let observed = state(&client, admin, concat!(file!(), ":", line!())).await;
                if matches!(
                    observed.operations.replicas.get(&2),
                    Some(ursula_control::ReplicaState::Pending { .. })
                ) {
                    break;
                }
                assert!(
                    std::time::Instant::now() < deadline,
                    "replacement never registered pending identity"
                );
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            let pending = state(&client, admin, concat!(file!(), ":", line!())).await;
            children[1]
                .child
                .kill()
                .expect("restart pending replacement");
            children[1].child.wait().expect("join pending replacement");
            children[1] = spawn_node_with_cluster_config(binary, &configs[1]);
            let deadline = std::time::Instant::now() + Duration::from_secs(30);
            loop {
                let restarted = state(&client, admin, concat!(file!(), ":", line!())).await;
                if restarted.operations.processes[&2].epoch()
                    > pending.operations.processes[&2].epoch()
                {
                    assert_eq!(
                        restarted.operations.replicas[&2],
                        pending.operations.replicas[&2]
                    );
                    break;
                }
                assert!(
                    std::time::Instant::now() < deadline,
                    "pending replacement restart did not reclaim its boot"
                );
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
        operation(
            &client,
            admin,
            OperationRequest::Reconcile {
                token: token.clone(),
            },
            &children,
        )
        .await;
        if matches!(kind, OperationKind::RebuildReplica { .. }) {
            // A survivor may restart in Retired after the replacement has been
            // fenced and promoted, before final evidence is collected.
            let before = state(&client, admin, concat!(file!(), ":", line!())).await;
            children[3]
                .child
                .kill()
                .expect("restart retired-phase survivor");
            children[3]
                .child
                .wait()
                .expect("join retired-phase survivor");
            children[3] = spawn_node_with_cluster_config(binary, &configs[3]);
            wait_until_ready(&client, &peers[3].1, &mut children).await;
            let after = state(&client, admin, concat!(file!(), ":", line!())).await;
            assert!(
                after.operations.processes[&4].epoch() > before.operations.processes[&4].epoch()
            );
            assert_eq!(
                after.operations.replicas[&4],
                before.operations.replicas[&4]
            );
        }
        operation(
            &client,
            admin,
            OperationRequest::CollectEvidence {
                token: token.clone(),
            },
            &children,
        )
        .await;
        assert_eq!(
            operation(
                &client,
                admin,
                OperationRequest::Complete { token },
                &children
            )
            .await,
            OperationOutcome::Completed
        );
        read_until_matches(&client, &read, b"durable-before-maintenance").await;
        if matches!(kind, OperationKind::MoveReplicas { source: 3, .. }) {
            // The on-disk config still names voters 1/2/3. Committed meta placement
            // must reopen node4's new replica after an ordinary same-PV restart.
            children[3]
                .child
                .kill()
                .expect("restart newly placed node4");
            children[3].child.wait().expect("join moved node4");
            children[3] = spawn_node_with_cluster_config(binary, &configs[3]);
            wait_until_ready(&client, &peers[3].1, &mut children).await;
            for attempt in 0..100 {
                let metrics: ursula_proto::admin::NodeMetrics = client
                    .get(format!("{}/__ursula/metrics", peers[3].1))
                    .send()
                    .await
                    .unwrap()
                    .json()
                    .await
                    .unwrap();
                if metrics
                    .raft_groups
                    .iter()
                    .any(|group| group.raft_group_id == 0 && group.voter_ids.contains(&4))
                {
                    break;
                }
                assert!(
                    attempt < 99,
                    "committed moved replica did not reopen: {metrics:?}"
                );
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            read_until_matches(&client, &read, b"durable-before-maintenance").await;
        }
    }
    let completed = state(&client, admin, concat!(file!(), ":", line!())).await;
    assert!(completed.operations.active.is_none());
    assert_eq!(
        completed.placements[&RaftGroupId(0)].voters,
        BTreeSet::from([1, 2, 4])
    );
    assert_eq!(
        completed.nodes[&3].state,
        ursula_control::NodeState::Removed
    );
    assert!(
        completed.operations.processes[&2].epoch() > restarted.operations.processes[&2].epoch()
    );
    children[2]
        .child
        .kill()
        .expect("stop fully decommissioned node3");
    children[2].child.wait().expect("join removed node3");
    post_until_no_content(&client, &stream, "-after-decommission").await;
    read_until_matches(
        &client,
        &read,
        b"durable-before-maintenance-after-decommission",
    )
    .await;
}

mod lifecycle_support {
    use ursula_control::ControlPlaneState;
    use ursula_control::ControlResponse;
    use ursula_control::OperationOutcome;
    use ursula_control::OperationRequest;

    use super::*;
    pub(super) async fn state(
        client: &reqwest::Client,
        admin: &str,
        phase: &str,
    ) -> ControlPlaneState {
        read_state(client, admin, Duration::from_secs(30))
            .await
            .unwrap_or_else(|error| panic!("meta state in {phase} at {admin}: {error:#}"))
    }

    pub(super) async fn read_state(
        client: &reqwest::Client,
        admin: &str,
        budget: Duration,
    ) -> anyhow::Result<ControlPlaneState> {
        let mut last_unavailable = String::new();
        let result = tokio::time::timeout(budget, async {
            loop {
                let response = client
                    .get(format!("{admin}/__ursula/control/state"))
                    .send()
                    .await?;
                let status = response.status();
                let body = response.text().await?;
                if status == reqwest::StatusCode::SERVICE_UNAVAILABLE {
                    last_unavailable = body;
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }
                anyhow::ensure!(
                    status.is_success(),
                    "linearizable meta read returned {status}: {body}"
                );
                return Ok::<_, anyhow::Error>(serde_json::from_str(&body)?);
            }
        })
        .await;
        match result {
            Ok(result) => result,
            Err(error) => Err(anyhow::anyhow!(
                "linearizable meta read exceeded {budget:?}: {error}; last503={last_unavailable}"
            )),
        }
    }
    pub(super) async fn operation(
        client: &reqwest::Client,
        admin: &str,
        request: OperationRequest,
        children: &[ChildGuard],
    ) -> OperationOutcome {
        let operator = ursula_ctl::MetricsClient::new(Duration::from_secs(60)).unwrap();
        let node = ursula_ctl::NodeInfo {
            id: 1,
            admin_url: admin.parse().unwrap(),
            host: "127.0.0.1".into(),
            http_url: None,
            metrics_url: None,
            expected_process_incarnation: None,
        };
        match operator.submit_operation(&node, &request).await {
            Ok(ControlResponse::Operation(Ok(outcome))) => outcome,
            result => {
                let observed = client
                    .get(format!("{admin}/__ursula/control/state"))
                    .send()
                    .await;
                let snapshot = match observed {
                    Ok(response) => response.text().await,
                    Err(error) => Err(error),
                };
                let reports = children.iter().map(child_report).collect::<Vec<_>>();
                panic!("{request:?}: {result:?}; control={snapshot:?}; children={reports:#?}");
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cli_replica_replacement_waits_for_an_offline_survivor() {
    use lifecycle_support::operation;
    use lifecycle_support::state;
    use ursula_control::OperationKind;
    use ursula_control::OperationOutcome;
    use ursula_control::OperationRequest;
    use ursula_control::ReplicaState;
    use ursula_proto::admin::NodeMetrics;
    use ursula_proto::admin::ProcessIncarnation;
    use ursula_shard::RaftGroupId;

    let _guard = static_cluster_cli_test_guard().await;
    let root = tempfile::tempdir().unwrap();
    let ports: Vec<_> = (0..5).map(|_| free_port()).collect();
    let meta_ports: Vec<_> = (0..5).map(|_| free_port()).collect();
    let peers: Vec<_> = ports
        .iter()
        .zip(1_u64..)
        .map(|(port, id)| (id, format!("http://127.0.0.1:{port}")))
        .collect();
    let mut configs = Vec::new();
    let mut admins = Vec::new();
    for (index, (id, _)) in peers.iter().enumerate() {
        let config = root.path().join(format!("node-{id}.toml"));
        let admin = write_cluster_config(
            &config,
            ports[index],
            *id,
            1,
            &peers,
            true,
            &root.path().join(format!("wal-{id}")),
        );
        let mut contents = std::fs::read_to_string(&config)
            .unwrap()
            .replace("meta = { enabled = false }\n", "")
            .replace("flush_interval = \"1s\"", "flush_interval = \"1h\"");
        contents.push_str(&format!("\n[[raft.groups]]\nraft_group_id = 0\nvoters = [1, 2, 3, 4, 5]\n\n[raft.meta]\nenabled = true\nlisten = \"127.0.0.1:{}\"\nauth_token_file = {:?}\n", meta_ports[index], meta_auth_token_file(&config)));
        for (port, peer) in meta_ports.iter().zip(1_u64..) {
            contents.push_str(&format!(
                "\n[[raft.meta.peers]]\nnode_id = {peer}\nurl = \"http://127.0.0.1:{port}\"\n"
            ));
        }
        std::fs::write(&config, contents).unwrap();
        configs.push(config);
        admins.push(format!("http://127.0.0.1:{admin}"));
    }
    let binary = env!("CARGO_BIN_EXE_ursula");
    let mut children: Vec<_> = configs
        .iter()
        .map(|config| spawn_node_with_cluster_config(binary, config))
        .collect();
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(60))
        .build()
        .unwrap();
    let phase = std::cell::Cell::new("bootstrap");
    let exercise = tokio::time::timeout(Duration::from_secs(180), async {
        for (_, url) in &peers {
            wait_until_ready(&client, url, &mut children).await;
        }
        let stream = format!("{}/meta/offline-survivor", peers[0].1);
        put_with_body_until_created(&client, &stream, "before-replacement").await;
        let admin = &admins[0];
        let before = state(&client, admin, concat!(file!(), ":", line!())).await;
        let survivor_identity = before.operations.replicas[&5].clone();
        let OperationOutcome::Acquired(token) = operation(
            &client,
            admin,
            OperationRequest::Begin {
                kind: OperationKind::RebuildReplica { node_id: 2 },
                executor: ProcessIncarnation::from_bits(501),
            },
            &children,
        )
        .await
        else {
            panic!("begin rebuild");
        };
        operation(
            &client,
            admin,
            OperationRequest::CollectEvidence {
                token: token.clone(),
            },
            &children,
        )
        .await;
        operation(
            &client,
            admin,
            OperationRequest::RetireSource {
                token: token.clone(),
            },
            &children,
        )
        .await;
        children[1].child.kill().unwrap();
        children[1].child.wait().unwrap();
        std::fs::remove_dir_all(root.path().join("wal-2")).unwrap();
        children[1] = spawn_node_with_cluster_config(binary, &configs[1]);
        phase.set("replacement pending");
        let replacement = loop {
            let observed = state(&client, admin, concat!(file!(), ":", line!())).await;
            if let ReplicaState::Pending { replacement, .. } = &observed.operations.replicas[&2] {
                break replacement.clone();
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        };
        // Three of four survivors remain: enough for data and meta quorums,
        // but deliberately insufficient for the required all-survivor fence.
        children[4].child.kill().unwrap();
        children[4].child.wait().unwrap();
        let operator = ursula_ctl::MetricsClient::new(Duration::from_secs(60)).unwrap();
        let node = ursula_ctl::NodeInfo {
            id: 1,
            admin_url: admin.parse().unwrap(),
            host: "127.0.0.1".into(),
            http_url: None,
            metrics_url: None,
            expected_process_incarnation: None,
        };
        let request = OperationRequest::Reconcile { token: token.clone() };
        let mut reconcile = tokio::spawn(async move {
            operator.submit_operation(&node, &request).await
        });
        phase.set("quorum has fence but offline survivor blocks activation");
        loop {
            if reconcile.is_finished() {
                let result = (&mut reconcile).await;
                let observed = state(&client, admin, "background Reconcile terminated before fence quorum").await;
                let reports = children.iter().map(child_report).collect::<Vec<_>>();
                panic!("Reconcile ended before survivor fence installation: {result:?}; control={observed:?}; children={reports:#?}");
            }

            let mut installed = true;
            for index in [0, 2, 3] {
                let metrics: NodeMetrics = client
                    .get(format!("{}/__ursula/metrics", peers[index].1))
                    .send()
                    .await
                    .unwrap()
                    .json()
                    .await
                    .unwrap();
                installed &= metrics.raft_groups.iter().any(|group| {
                    group.raft_group_id == 0
                        && group.installed_replica_identities.get(&2) == Some(&replacement)
                });
            }
            if installed {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        let blocked = state(&client, admin, concat!(file!(), ":", line!())).await;
        assert!(
            matches!(&blocked.operations.replicas[&2], ReplicaState::Pending {
            replacement: current, installed_groups, ..
        } if current == &replacement && installed_groups.is_empty())
        );
        assert!(
            blocked
                .operations
                .active
                .as_ref()
                .unwrap()
                .pending_action
                .is_some()
        );
        phase.set("same-WAL survivor returns");
        children[4] = spawn_node_with_cluster_config(binary, &configs[4]);
        wait_until_ready(&client, &peers[4].1, &mut children).await;
        let reconciled = reconcile.await.expect("background Reconcile task");
        assert!(matches!(reconciled,
            Ok(ursula_control::ControlResponse::Operation(Ok(OperationOutcome::ActionFinished)))),
            "background Reconcile after survivor return: {reconciled:?}");
        operation(
            &client,
            admin,
            OperationRequest::Reconcile {
                token: token.clone(),
            },
            &children,
        )
        .await;
        let active = state(&client, admin, concat!(file!(), ":", line!())).await;
        assert_eq!(active.operations.replicas[&5], survivor_identity);
        let ReplicaState::Active {
            identity,
            installed_groups,
        } = &active.operations.replicas[&2]
        else {
            panic!("replacement not active");
        };
        assert_eq!(identity, &replacement);
        let required = installed_groups[&RaftGroupId(0)];
        phase.set("rebuilt replica becomes leader");
        loop {
            let metrics: NodeMetrics = client
                .get(format!("{}/__ursula/metrics", peers[0].1))
                .send()
                .await
                .unwrap()
                .json()
                .await
                .unwrap();
            let leader = metrics
                .raft_groups
                .iter()
                .find(|group| group.raft_group_id == 0)
                .and_then(|group| group.current_leader);
            if leader == Some(2) {
                break;
            }
            if let Some(leader) = leader {
                let index = usize::try_from(leader).unwrap().checked_sub(1).unwrap();
                let response = admin_test_post(
                    &client,
                    format!("{}/__ursula/raft/0/leader/transfer/2", admins[index]),
                )
                .await
                .send()
                .await
                .unwrap();
                assert!(
                    response.status().is_success()
                        || response.status() == reqwest::StatusCode::CONFLICT
                );
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        phase.set("survivor local prefix and installed identity");
        loop {
            let metrics: NodeMetrics = client
                .get(format!("{}/__ursula/metrics", peers[4].1))
                .send()
                .await
                .unwrap()
                .json()
                .await
                .unwrap();
            if metrics.raft_groups.iter().any(|group| {
                group.node_id == 5
                    && group.voter_ids == [1, 2, 3, 4, 5]
                    && group
                        .last_applied_index
                        .is_some_and(|index| index >= required)
                    && group.installed_replica_identities.get(&2) == Some(&replacement)
            }) && metrics
                .diagnostics
                .recovery_gates
                .as_ref()
                .is_some_and(|gates| gates.gated.is_empty())
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        operation(
            &client,
            admin,
            OperationRequest::CollectEvidence {
                token: token.clone(),
            },
            &children,
        )
        .await;
        assert_eq!(
            operation(
                &client,
                admin,
                OperationRequest::Complete { token },
                &children
            )
            .await,
            OperationOutcome::Completed
        );
        post_until_no_content(&client, &stream, "-after-replacement").await;
        read_until_matches(
            &client,
            &format!("{}/meta/offline-survivor?offset=-1", peers[4].1),
            b"before-replacement-after-replacement",
        )
        .await;
    })
    .await;
    if exercise.is_err() {
        let reports = children.iter().map(child_report).collect::<Vec<_>>();
        panic!(
            "offline-survivor deadline in {}; children={reports:#?}",
            phase.get()
        );
    }
}

#[tokio::test]
async fn meta_state_read_retries_only_503_with_one_deadline() {
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;

    use axum::http::StatusCode;

    for (status, recover) in [
        (StatusCode::SERVICE_UNAVAILABLE, true),
        (StatusCode::CONFLICT, false),
        (StatusCode::SERVICE_UNAVAILABLE, false),
    ] {
        let calls = Arc::new(AtomicUsize::new(0));
        let observed = calls.clone();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let router = axum::Router::new().route(
            "/__ursula/control/state",
            axum::routing::get(move || {
                let attempt = observed.fetch_add(1, Ordering::SeqCst);
                async move {
                    if recover && attempt > 0 {
                        (
                            StatusCode::OK,
                            serde_json::to_string(&ursula_control::ControlPlaneState::default())
                                .unwrap(),
                        )
                    } else {
                        (status, "fixture meta leader unavailable".to_owned())
                    }
                }
            }),
        );
        let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        let result = tokio::time::timeout(
            Duration::from_secs(1),
            lifecycle_support::read_state(
                &reqwest::Client::new(),
                &format!("http://{address}"),
                Duration::from_millis(250),
            ),
        )
        .await
        .expect("overall retry deadline must not reset");
        if recover {
            assert_eq!(
                result.unwrap(),
                ursula_control::ControlPlaneState::default()
            );
            assert_eq!(calls.load(Ordering::SeqCst), 2);
        } else {
            let error = result
                .expect_err("permanent or unavailable result must fail")
                .to_string();
            assert!(error.contains("fixture meta leader unavailable"), "{error}");
            if status == StatusCode::CONFLICT {
                assert_eq!(calls.load(Ordering::SeqCst), 1);
            } else {
                assert!(calls.load(Ordering::SeqCst) >= 2);
            }
        }
        server.abort();
        server.await.expect_err("mock listener stopped");
    }
}

#![expect(
    clippy::unwrap_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::string_slice,
    reason = "integration tests assert by panicking, as clippy.toml allows for unit tests"
)]
use std::collections::BTreeMap;
use std::collections::BTreeSet;
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

use ursula_control::Cluster;
use ursula_control::ClusterView;
use ursula_control::GroupPlacement;
use ursula_control::MetaDamageKind;
use ursula_control::MetaReplicaStatus;
use ursula_control::MetaRole;
use ursula_control::PlacementStatus;
use ursula_control::RegistrationStatus;
use ursula_proto::placement::NodeId;
use ursula_proto::placement::PlacementEpoch;
use ursula_proto::placement::Registered;
use ursula_runtime::ColdStore;
use ursula_shard::RaftGroupId;

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
    write_single_node_cluster_config(&config_path, port, 1, 1, &base_url, &log_dir);

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
    // it does not. A single voter defaults to `fsync = "always"`, under which
    // a host crash costs nothing acknowledged, so it opens normally either way.
    child.child.kill().expect("kill the node");
    child.child.wait().expect("reap the node");
    let mut child = spawn_node_with_cluster_config(binary, &config_path);
    wait_until_ready(&client, &base_url, std::slice::from_mut(&mut child)).await;
    let recovery = wal_recovery(&client, &base_url).await;
    let expected_run = if cfg!(target_os = "linux") {
        "process_crash"
    } else {
        "host_crash"
    };
    assert_eq!(recovery["previous_run"]["kind"], expected_run, "{recovery}");
    assert_eq!(recovery["recovery"]["state"], "normal", "{recovery}");
    assert_eq!(recovery["fsync"], "always", "{recovery}");
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
        spawn_node(binary, 2, ports[1], &peers, wal_root.path()),
        spawn_node(binary, 3, ports[2], &peers, wal_root.path()),
        spawn_node(binary, 1, ports[0], &peers, wal_root.path()),
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

    // Every node names the configured cluster on a HEAD, a missing stream's
    // included.
    for (_, base_url) in &peers {
        for path in ["benchcmp/cli-follower-forward", "benchcmp/cli-missing"] {
            let response = client
                .head(format!("{base_url}/{path}"))
                .send()
                .await
                .expect("send HEAD");
            assert_eq!(
                response
                    .headers()
                    .get("ursula-cluster-id")
                    .and_then(|value| value.to_str().ok()),
                Some("static-cluster-cli"),
                "{base_url}/{path}: {}",
                response.status()
            );
        }
    }

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

/// A cluster whose nodes serve the cluster plane on `server.cluster_listen`:
/// Raft replicates over the cluster ports, the peers' egress probe finds
/// them healthy, and a follower redirects a client to the leader's client
/// port, never to its cluster port.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cli_static_grpc_raft_cluster_splits_the_cluster_plane() {
    let _guard = static_cluster_cli_test_guard().await;
    let Some(binary) = option_env!("CARGO_BIN_EXE_ursula") else {
        tracing::warn!("CARGO_BIN_EXE_ursula is not set; skipping CLI split-plane test");
        return;
    };
    let planes: Vec<SplitPlanePorts> = (1_u64..=3)
        .map(|node_id| SplitPlanePorts {
            node_id,
            client: free_port(),
            cluster: free_port(),
            admin: free_port(),
        })
        .collect();
    let wal_root = tempfile::tempdir().expect("WAL root");
    let mut children = vec![
        spawn_split_plane_node(binary, &planes, 2, wal_root.path()),
        spawn_split_plane_node(binary, &planes, 3, wal_root.path()),
        spawn_split_plane_node(binary, &planes, 1, wal_root.path()),
    ];

    let client = reqwest::Client::new();
    for plane in &planes {
        wait_until_ready(&client, &plane.client_url(), &mut children).await;
    }
    put_until_created(&client, &format!("{}/split/plane", planes[1].client_url())).await;

    // Writes route before their body, so a follower answers a create with a
    // redirect to the group's leader.
    let no_redirect = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .expect("client without redirects");
    let client_urls: Vec<String> = planes.iter().map(SplitPlanePorts::client_url).collect();
    let mut locations = Vec::new();
    for _ in 0..200 {
        locations.clear();
        for plane in &planes {
            let response = no_redirect
                .put(format!("{}/split/plane", plane.client_url()))
                .header("content-type", "text/plain")
                .send()
                .await
                .expect("send create");
            if response.status() == reqwest::StatusCode::TEMPORARY_REDIRECT {
                let location = response.headers()["location"]
                    .to_str()
                    .expect("location header")
                    .to_owned();
                locations.push(location);
            }
        }
        if locations.len() == 2 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(locations.len(), 2, "two followers redirect: {locations:?}");
    for location in &locations {
        assert!(
            client_urls
                .iter()
                .any(|base| *location == format!("{base}/split/plane")),
            "a redirect names a client port, not a cluster port: {location}"
        );
    }

    let appended = put_with_body_until_created(
        &client,
        &format!("{}/split/replicated", planes[2].client_url()),
        "split-plane-payload",
    )
    .await;
    assert!(appended.status().is_success());
    let payload = read_until_replicated(
        &client,
        &format!(
            "{}/split/replicated?offset=0&max_bytes=64",
            planes[0].client_url()
        ),
    )
    .await;
    assert_eq!(payload, b"split-plane-payload");

    // The egress gate sheds after two failed 500 ms ticks. Give it several,
    // then every node must still be allowed to campaign: its peers answered
    // the probe on their cluster ports.
    tokio::time::sleep(Duration::from_secs(3)).await;
    for plane in &planes {
        let status: ursula_proto::admin::LeadershipShedStatus = client
            .get(format!("{}/__ursula/leadership-shed", plane.cluster_url()))
            .send()
            .await
            .expect("request leadership-shed state")
            .error_for_status()
            .expect("leadership-shed state on the cluster port")
            .json()
            .await
            .expect("decode leadership-shed state");
        assert!(
            status.should_campaign,
            "node {} sheds: {status:?}",
            plane.node_id
        );
        // The cluster port names its cluster, which its peers' polls check.
        assert_eq!(status.cluster_id.as_str(), "static-cluster-cli");
        let probe = client
            .post(format!("{}/__ursula/cluster-probe", plane.cluster_url()))
            .body(vec![0_u8; 1024])
            .send()
            .await
            .expect("send egress probe");
        assert_eq!(probe.status(), reqwest::StatusCode::OK);
        // Neither plane serves the other's routes.
        let append = client
            .put(format!("{}/split/cluster-port", plane.cluster_url()))
            .header("content-type", "text/plain")
            .send()
            .await
            .expect("send create to the cluster port");
        assert_eq!(append.status(), reqwest::StatusCode::NOT_FOUND);
        let shed = client
            .get(format!("{}/__ursula/leadership-shed", plane.client_url()))
            .send()
            .await
            .expect("request leadership-shed state on the client port");
        assert_ne!(shed.status(), reqwest::StatusCode::OK);
        let client_probe = client
            .post(format!("{}/__ursula/cluster-probe", plane.client_url()))
            .body(vec![0_u8; 1024])
            .send()
            .await
            .expect("send egress probe to the client port");
        assert_ne!(client_probe.status(), reqwest::StatusCode::OK);
    }

    drop(children);
}

/// A node whose cluster plane moved to a new port after its groups were
/// initialized starts and serves: peer addresses come from `raft.peers`
/// only, so nothing holds the old one.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cli_a_cluster_port_moved_after_initialization_serves() {
    let _guard = static_cluster_cli_test_guard().await;
    let Some(binary) = option_env!("CARGO_BIN_EXE_ursula") else {
        tracing::warn!("CARGO_BIN_EXE_ursula is not set; skipping CLI cluster-port test");
        return;
    };
    let root = tempfile::tempdir().expect("test root");
    let port = free_port();
    let base = format!("http://127.0.0.1:{port}");
    let config = root.path().join("server.toml");
    write_single_node_cluster_config(&config, port, 1, 2, &base, &root.path().join("wal"));
    let mut command = Command::new(binary);
    command.arg("server").arg("--config").arg(&config);
    let mut children = vec![spawn_child(command, "shared-port".to_owned())];
    let client = reqwest::Client::new();
    wait_until_ready(&client, &base, &mut children).await;
    put_with_body_until_created(&client, &format!("{base}/moved/port"), "before-the-move").await;
    sigterm_and_wait_for_clean_exit(&mut children[0]).await;
    drop(children);

    let cluster_port = free_port();
    assert_ne!(
        cluster_port, port,
        "a cluster port apart from the client port"
    );
    let cluster_base = format!("http://127.0.0.1:{cluster_port}");
    let shared = std::fs::read_to_string(&config).expect("read node config");
    let listen = format!("listen = \"127.0.0.1:{port}\"");
    let peer_url = format!("url = \"{base}\"");
    assert!(
        shared.contains(&listen) && shared.contains(&peer_url),
        "{shared}"
    );
    let split = shared
        .replace(
            &listen,
            &format!("{listen}\ncluster_listen = \"127.0.0.1:{cluster_port}\""),
        )
        .replace(
            &peer_url,
            &format!("url = \"{cluster_base}\"\nclient_url = \"{base}\""),
        );
    std::fs::write(&config, split).expect("write split-plane config");
    let mut command = Command::new(binary);
    command.arg("server").arg("--config").arg(&config);
    let mut children = vec![spawn_child(command, "moved-port".to_owned())];
    wait_until_ready(&client, &base, &mut children).await;
    let payload =
        read_until_replicated(&client, &format!("{base}/moved/port?offset=0&max_bytes=64")).await;
    assert_eq!(payload, b"before-the-move");
    let shed = client
        .get(format!("{cluster_base}/__ursula/leadership-shed"))
        .send()
        .await
        .expect("request leadership-shed state on the new cluster port");
    assert_eq!(shed.status(), reqwest::StatusCode::OK);
    sigterm_and_wait_for_clean_exit(&mut children[0]).await;
}

/// A node whose cluster port moved rejoins its cluster once every voter
/// restarted with the new `raft.peers`, with no admin step: every peer
/// dials the address its own configuration lists.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cli_a_node_moved_to_a_new_cluster_port_rejoins_after_a_full_restart() {
    let _guard = static_cluster_cli_test_guard().await;
    let Some(binary) = option_env!("CARGO_BIN_EXE_ursula") else {
        tracing::warn!("CARGO_BIN_EXE_ursula is not set; skipping CLI peer-move test");
        return;
    };
    let mut planes: Vec<SplitPlanePorts> = (1_u64..=3)
        .map(|node_id| SplitPlanePorts {
            node_id,
            client: free_port(),
            cluster: free_port(),
            admin: free_port(),
        })
        .collect();
    let wal_root = tempfile::tempdir().expect("WAL root");
    let spawn_all = |planes: &[SplitPlanePorts]| {
        vec![
            spawn_split_plane_node(binary, planes, 2, wal_root.path()),
            spawn_split_plane_node(binary, planes, 3, wal_root.path()),
            spawn_split_plane_node(binary, planes, 1, wal_root.path()),
        ]
    };
    let client = reqwest::Client::new();
    let mut children = spawn_all(&planes);
    for plane in &planes {
        wait_until_ready(&client, &plane.client_url(), &mut children).await;
    }
    put_with_body_until_created(
        &client,
        &format!("{}/moved/before", planes[0].client_url()),
        "before-the-move",
    )
    .await;
    let replicated = read_until_replicated(
        &client,
        &format!(
            "{}/moved/before?offset=0&max_bytes=64",
            planes[2].client_url()
        ),
    )
    .await;
    assert_eq!(replicated, b"before-the-move");

    // Stop every voter, move node 3's cluster port, start them all.
    for child in &children {
        let status = Command::new("kill")
            .arg("-TERM")
            .arg(child.child.id().to_string())
            .status()
            .expect("send SIGTERM");
        assert!(status.success());
    }
    for child in &mut children {
        let deadline = std::time::Instant::now()
            .checked_add(Duration::from_secs(30))
            .unwrap();
        while child.child.try_wait().expect("poll child").is_none() {
            assert!(
                std::time::Instant::now() < deadline,
                "a stopped voter did not exit: {}",
                child_report(child)
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
    drop(children);
    let old_cluster_url = planes[2].cluster_url();
    let old_cluster_port = planes[2].cluster;
    planes[2].cluster = free_port();
    assert_ne!(planes[2].cluster, old_cluster_port, "a new cluster port");
    let mut children = spawn_all(&planes);
    for plane in &planes {
        wait_until_ready(&client, &plane.client_url(), &mut children).await;
    }
    let nodes = planes
        .iter()
        .map(|plane| ctl_node(plane.node_id, plane.admin, &plane.cluster_url()))
        .collect::<Vec<_>>();
    let ctl = ursula_ctl::MetricsClient::new(Duration::from_secs(2)).unwrap();
    ursula_ctl::wait_cluster_ready(
        "every voter after the move",
        &nodes,
        &ctl,
        Duration::from_secs(60),
        Duration::from_millis(100),
        0,
    )
    .await
    .expect("the cluster reaches its full voter sets with no admin step");

    // Node 3 leads no group, so what it holds a leader sent to its new
    // address.
    let mut node3_leads = Vec::new();
    for _ in 0..100 {
        let view = ctl.fetch_node(&nodes[2]).await.expect("node 3 metrics");
        node3_leads = view
            .groups
            .iter()
            .filter(|group| group.current_leader == Some(3))
            .map(|group| group.raft_group_id)
            .collect::<Vec<_>>();
        if node3_leads.is_empty() {
            break;
        }
        for group in &node3_leads {
            let transferred = admin_test_post(
                &client,
                format!(
                    "http://127.0.0.1:{}/__ursula/raft/{group}/leader/transfer/1",
                    planes[2].admin
                ),
            )
            .await
            .send()
            .await
            .expect("send leader transfer");
            tracing::debug!(group, status = %transferred.status(), "leader transfer");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(node3_leads.is_empty(), "node 3 still leads {node3_leads:?}");

    let before = read_until_replicated(
        &client,
        &format!(
            "{}/moved/before?offset=0&max_bytes=64",
            planes[2].client_url()
        ),
    )
    .await;
    assert_eq!(before, b"before-the-move");
    put_with_body_until_created(
        &client,
        &format!("{}/moved/after", planes[0].client_url()),
        "after-the-move",
    )
    .await;
    let after = read_until_replicated(
        &client,
        &format!(
            "{}/moved/after?offset=0&max_bytes=64",
            planes[2].client_url()
        ),
    )
    .await;
    assert_eq!(after, b"after-the-move");

    let shed = client
        .get(format!(
            "{}/__ursula/leadership-shed",
            planes[2].cluster_url()
        ))
        .send()
        .await
        .expect("request leadership-shed state on the new cluster port");
    assert_eq!(shed.status(), reqwest::StatusCode::OK);
    assert!(
        client
            .get(format!("{old_cluster_url}/__ursula/leadership-shed"))
            .send()
            .await
            .is_err(),
        "nothing serves the old cluster port"
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
    write_single_node_cluster_config(&config_path, port, 1, 1, &base_url, &log_dir);
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
        write_single_node_cluster_config(&config_path, port, 1, 1, &base_url, &log_dir);
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

    write_single_node_cluster_config(&config_path, port, 1, 1, &base_url, &log_dir);
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

/// Exercise the production paths used by verify-cluster and wait --node
/// against a real single-voter server, not a synthetic metrics response.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cli_single_voter_passes_verify_cluster_and_wait_node() {
    let _guard = static_cluster_cli_test_guard().await;
    let port = free_port();
    let base_url = format!("http://127.0.0.1:{port}");
    let root = tempfile::tempdir().expect("single-voter root");
    let config = root.path().join("cluster.toml");
    let admin_port =
        write_single_node_cluster_config(&config, port, 1, 1, &base_url, &root.path().join("wal"));
    let mut child = spawn_node_with_cluster_config(env!("CARGO_BIN_EXE_ursula"), &config);
    let http = reqwest::Client::new();
    wait_until_ready(&http, &base_url, std::slice::from_mut(&mut child)).await;
    let stream = format!("{base_url}/benchcmp/single-voter-readiness");
    put_until_created(&http, &stream).await;
    post_until_no_content(&http, &stream, "committed-before-verification").await;
    let node = ctl_node(1, admin_port, &base_url);
    let nodes = std::slice::from_ref(&node);
    let client = ursula_ctl::MetricsClient::new(Duration::from_secs(2)).unwrap();
    ursula_ctl::wait_cluster_ready(
        "single-voter verification",
        nodes,
        &client,
        Duration::from_secs(5),
        Duration::from_millis(20),
        0,
    )
    .await
    .expect("verify-cluster accepts a caught-up single voter");
    let outcome = ursula_ctl::wait_node_ready(nodes, &node, &client, &ursula_ctl::CatchUpOptions {
        stall_timeout: Duration::from_secs(2),
        ready_timeout: Duration::from_secs(5),
        poll_interval: Duration::from_millis(20),
        lag_tolerance: 0,
    })
    .await
    .expect("wait --node observes the actual server");
    assert!(matches!(outcome, ursula_ctl::CatchUpOutcome::Ready));
}

/// A single voter whose run state is gone while its journal holds records
/// cannot know whether it lost acknowledged writes: it comes back gated, its
/// group has no leader and readiness says why. A recovery plan for the log
/// the metrics show elects it (#581 section 4.5): it starts a new history,
/// so its streams answer under new tokens, HEAD names the token each had
/// before and the offset below which its bytes survived, and a write that
/// names the old token answers `412`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cli_single_voter_with_an_unknown_history_serves_after_a_recovery_plan() {
    single_voter_recovery_plan(false).await;
}

/// Item 3 of the #612 review (#581 section 4.5): a group with one voter
/// never cuts. A cutting plan is refused, typed, and changes nothing; the
/// plan without a cut then recovers the group.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cli_single_voter_refuses_a_cutting_plan() {
    single_voter_recovery_plan(true).await;
}

/// m3 of the second #581 review: a plan that cuts runs its cut through each
/// node's own in-process restart of the group (the runtime's restart task,
/// the production engine factory and the shared registry). Three voters lose
/// their run state at once, come back gated and stall; the plan goes to its
/// leader first and then to the others with its plan term. The group comes
/// back under the plan, elects its leader and serves the kept log.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cli_three_voters_run_a_cutting_plan_through_their_in_process_restarts() {
    let _guard = static_cluster_cli_test_guard().await;
    let Some(binary) = option_env!("CARGO_BIN_EXE_ursula") else {
        tracing::warn!("CARGO_BIN_EXE_ursula is not set; skipping CLI recovery plan test");
        return;
    };
    let ports = [free_port(), free_port(), free_port()];
    let peers: Vec<(u64, String)> = ports
        .iter()
        .zip(1_u64..)
        .map(|(port, node_id)| (node_id, format!("http://127.0.0.1:{port}")))
        .collect();
    let root = tempfile::tempdir().expect("temp root");
    let mut configs = Vec::new();
    let mut admins = Vec::new();
    for (index, (node_id, _)) in peers.iter().enumerate() {
        let config_path = root.path().join(format!("node-{node_id}.toml"));
        let log_dir = root.path().join(format!("node-{node_id}-log"));
        let admin_port =
            write_cluster_config(&config_path, ports[index], *node_id, 1, &peers, &log_dir);
        configs.push(config_path);
        admins.push(format!("http://127.0.0.1:{admin_port}"));
    }
    let client = reqwest::Client::new();
    let stream_url = format!("{}/benchcmp/cli-three-voter-cut", peers[0].1);
    {
        let mut children = configs
            .iter()
            .map(|config| spawn_node_with_cluster_config(binary, config))
            .collect::<Vec<_>>();
        for (_, base_url) in &peers {
            wait_until_ready(&client, base_url, &mut children).await;
        }
        put_until_created(&client, &stream_url).await;
        post_until_no_content(&client, &stream_url, "kept-by-every-voter").await;
        read_until_replicated(
            &client,
            &format!(
                "{}/benchcmp/cli-three-voter-cut?offset=0&max_bytes=64",
                peers[2].1
            ),
        )
        .await;
        // Killed together: the page cache keeps every write, but no run
        // state is left, so every voter comes back gated.
    }
    for (node_id, _) in &peers {
        std::fs::remove_file(
            root.path()
                .join(format!("node-{node_id}-log"))
                .join("raft-log")
                .join("run-state.bin"),
        )
        .expect("remove the run state");
    }
    let mut children = configs
        .iter()
        .map(|config| spawn_node_with_cluster_config(binary, config))
        .collect::<Vec<_>>();
    for (_, base_url) in &peers {
        wait_until_ready(&client, base_url, &mut children).await;
    }
    let stall_deadline = std::time::Instant::now()
        .checked_add(ursula_raft::RECOVERY_STALL_AFTER.saturating_add(Duration::from_secs(30)))
        .unwrap();
    for (_, base_url) in &peers {
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
                "{base_url} never reported its group stalled: {ready}"
            );
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    }
    let mut views = Vec::new();
    for admin in &admins {
        let metrics: ursula_proto::admin::NodeMetrics = client
            .get(format!("{admin}/__ursula/metrics"))
            .send()
            .await
            .expect("metrics")
            .json()
            .await
            .expect("metrics JSON");
        let group = metrics.groups[0].clone();
        views.push(group);
    }
    // The member with the greatest committed index leads, and every log is
    // cut back to what it knew was committed.
    let leader = views
        .iter()
        .max_by_key(|group| (group.committed_index, group.node_id))
        .expect("a member")
        .clone();
    let point = leader
        .committed_index
        .expect("the leader knows its committed entry");
    let highest_term = views
        .iter()
        .map(|group| group.current_term)
        .max()
        .unwrap_or(0);
    let request = |group: &ursula_proto::admin::RaftGroupMetrics,
                   step: ursula_proto::admin::PlanStep| {
        ursula_proto::admin::RecoveryPlanRequest {
            point_entry: None,
            floor: ursula_proto::admin::PlanFloor::Seeded,
            leader: leader.node_id,
            point,
            cut: true,
            step,
            expected: ursula_proto::admin::ReplicaLogView {
                last_log_index: group.last_log_index,
                last_log_term: group.last_log_term,
                current_term: group.current_term,
            },
        }
    };
    let accept = |node_id: u64| {
        format!(
            "{}/__ursula/raft/0/recovery/accept-unsynced-loss",
            admins[usize::try_from(node_id.saturating_sub(1)).expect("node index")]
        )
    };
    let response = admin_test_post(&client, accept(leader.node_id))
        .await
        .json(&request(&leader, ursula_proto::admin::PlanStep::Lead {
            highest_term,
        }))
        .send()
        .await
        .expect("send the plan to its leader");
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    let report: ursula_proto::admin::RecoveryPlanReport =
        response.json().await.expect("typed report");
    for member in views.iter().filter(|group| group.node_id != leader.node_id) {
        let response = admin_test_post(&client, accept(member.node_id))
            .await
            .json(&request(member, ursula_proto::admin::PlanStep::Follow {
                leader_term: report.leader_term,
            }))
            .send()
            .await
            .expect("send the plan to a member");
        let status = response.status();
        if status != reqwest::StatusCode::OK {
            // The members that took it may have elected the leader already.
            let refused: ursula_proto::admin::RecoveryPlanRefused =
                response.json().await.expect("typed refusal");
            assert_eq!(
                refused.refusal,
                ursula_proto::admin::RecoveryPlanRefusal::FollowsLeader {
                    leader: leader.node_id
                },
                "{refused:?}"
            );
        }
    }
    let satisfied_deadline = std::time::Instant::now()
        .checked_add(Duration::from_secs(60))
        .unwrap();
    for admin in &admins {
        loop {
            let metrics: ursula_proto::admin::NodeMetrics = client
                .get(format!("{admin}/__ursula/metrics"))
                .send()
                .await
                .expect("metrics")
                .json()
                .await
                .expect("metrics JSON");
            let group = &metrics.groups[0];
            if group.recovery.is_none()
                && group
                    .history
                    .is_some_and(|history| history.event_index > point)
            {
                break;
            }
            assert!(
                std::time::Instant::now() < satisfied_deadline,
                "{admin}: the recovery plan was never satisfied: {:?} {:?}",
                group.history,
                group.recovery
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
    let payload = read_until_replicated(
        &client,
        &format!(
            "{}/benchcmp/cli-three-voter-cut?offset=0&max_bytes=64",
            peers[0].1
        ),
    )
    .await;
    assert_eq!(payload, b"kept-by-every-voter");
    drop(children);
}

async fn single_voter_recovery_plan(cut: bool) {
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
        write_single_node_cluster_config(&config_path, port, 1, 1, &base_url, &log_dir);
    let admin_url = format!("http://127.0.0.1:{admin_port}");
    let client = reqwest::Client::new();
    let stream_url = format!("{base_url}/benchcmp/cli-unknown-history");
    let old_token = {
        let mut child = spawn_node_with_cluster_config(binary, &config_path);
        wait_until_ready(&client, &base_url, std::slice::from_mut(&mut child)).await;
        put_until_created(&client, &stream_url).await;
        post_until_no_content(&client, &stream_url, "kept-by-the-page-cache").await;
        let head = client.head(&stream_url).send().await.expect("HEAD");
        let head = ursula_headers::StreamHead::parse(
            head.headers()
                .iter()
                .map(|(name, value)| (name.as_str(), value.as_bytes())),
        )
        .expect("a stream head");
        // Killed: the page cache keeps every write.
        head.incarnation()
    };
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
    // A lost run state may hide a predecessor that still runs, so the run
    // waits it out first, unready for `predecessor_wait`. Its gate driver
    // starts after the wait.
    let wait_deadline = std::time::Instant::now()
        .checked_add(
            ursula_raft::predecessor_wait::PREDECESSOR_WAIT.saturating_add(Duration::from_secs(30)),
        )
        .unwrap();
    let ready = loop {
        let ready = client
            .get(format!("{base_url}/__ursula/ready"))
            .send()
            .await
            .expect("readiness");
        assert_eq!(ready.status(), reqwest::StatusCode::SERVICE_UNAVAILABLE);
        let ready: serde_json::Value = ready.json().await.expect("readiness JSON");
        if ready["reason"] != "predecessor_wait" {
            break ready;
        }
        assert!(
            std::time::Instant::now() < wait_deadline,
            "the predecessor wait never ended: {ready}"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    };
    assert_eq!(ready["reason"], "recovery_gate_closed", "{ready}");

    // The plan waits for the stall: until then a leader's barrier may still
    // open the gate without losing anything.
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
    let accept_url = format!("{admin_url}/__ursula/raft/0/recovery/accept-unsynced-loss");
    let metrics: serde_json::Value = client
        .get(format!("{admin_url}/__ursula/metrics"))
        .send()
        .await
        .expect("metrics")
        .json()
        .await
        .expect("metrics JSON");
    let group = &metrics["raft_groups"][0];
    let expected = ursula_proto::admin::ReplicaLogView {
        last_log_index: group["last_log_index"].as_u64(),
        last_log_term: group["last_log_term"].as_u64(),
        current_term: group["current_term"].as_u64().expect("current term"),
    };
    // A single voter never cuts (#581 section 4.5): a cutting plan is
    // refused, typed, and changes nothing.
    if cut {
        let cutting = ursula_proto::admin::RecoveryPlanRequest {
            point_entry: None,
            floor: ursula_proto::admin::PlanFloor::Seeded,
            leader: 1,
            point: group["committed_index"]
                .as_u64()
                .expect("the single voter knows its committed entry"),
            cut: true,
            step: ursula_proto::admin::PlanStep::Lead {
                highest_term: expected.current_term,
            },
            expected,
        };
        let response = admin_test_post(&client, accept_url.clone())
            .await
            .json(&cutting)
            .send()
            .await
            .expect("send the cutting plan");
        assert_eq!(response.status(), reqwest::StatusCode::CONFLICT);
        let refused: ursula_proto::admin::RecoveryPlanRefused =
            response.json().await.expect("typed refusal");
        assert_eq!(
            refused.refusal,
            ursula_proto::admin::RecoveryPlanRefusal::SingleVoterCut
        );
    }
    let plan = ursula_proto::admin::RecoveryPlanRequest {
        point_entry: None,
        floor: ursula_proto::admin::PlanFloor::Seeded,
        leader: 1,
        point: expected.last_log_index.unwrap_or(0),
        cut: false,
        step: ursula_proto::admin::PlanStep::Lead {
            highest_term: expected.current_term,
        },
        expected,
    };
    let response = admin_test_post(&client, accept_url)
        .await
        .json(&plan)
        .send()
        .await
        .expect("send the recovery plan");
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    let report: ursula_proto::admin::RecoveryPlanReport =
        response.json().await.expect("typed report");
    assert_eq!(
        report.outcome,
        ursula_proto::admin::RecoveryPlanOutcome::Leads
    );
    let payload =
        read_until_replicated(&client, &format!("{stream_url}?offset=0&max_bytes=64")).await;
    assert_eq!(payload, b"kept-by-the-page-cache");
    // Until the plan's leader is elected the replica serves its old-history
    // state as a follower, and HEAD, which needs a leader, answers `503`.
    // Wait until the plan is satisfied and retired.
    let satisfied_deadline = std::time::Instant::now()
        .checked_add(Duration::from_secs(30))
        .unwrap();
    loop {
        let metrics: ursula_proto::admin::NodeMetrics = client
            .get(format!("{admin_url}/__ursula/metrics"))
            .send()
            .await
            .expect("metrics")
            .json()
            .await
            .expect("metrics JSON");
        let group = &metrics.groups[0];
        if group.recovery.is_none()
            && group
                .history
                .is_some_and(|history| history.event_index > plan.point)
        {
            break;
        }
        assert!(
            std::time::Instant::now() < satisfied_deadline,
            "the recovery plan was never satisfied: {:?} {:?}",
            group.history,
            group.recovery
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let head = client.head(&stream_url).send().await.expect("HEAD");
    let head = ursula_headers::StreamHead::parse(
        head.headers()
            .iter()
            .map(|(name, value)| (name.as_str(), value.as_bytes())),
    )
    .expect("a stream head");
    assert_ne!(
        head.incarnation(),
        old_token,
        "the history event changed the token"
    );
    assert_eq!(
        head.inherited(),
        Some(ursula_headers::Inherited {
            previous_incarnation: old_token,
            offset: ursula_headers::Offset::from_u64(22),
        })
    );
    let stale = client
        .post(&stream_url)
        .header("content-type", "text/plain")
        .header("stream-incarnation", old_token.to_string())
        .body("stale")
        .send()
        .await
        .expect("a write naming the old token");
    assert_eq!(stale.status(), reqwest::StatusCode::PRECONDITION_FAILED);
    post_until_no_content(&client, &stream_url, "-after").await;
    // The plan was satisfied and retired.
    let metrics: ursula_proto::admin::NodeMetrics = client
        .get(format!("{admin_url}/__ursula/metrics"))
        .send()
        .await
        .expect("metrics")
        .json()
        .await
        .expect("metrics JSON");
    assert!(
        metrics.groups[0]
            .history
            .is_some_and(|history| history.event_index > plan.point)
    );
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
        write_cluster_config(&config_path, ports[index], *node_id, 4, &peers, &log_dir);
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

    drop(children);
    std::fs::remove_dir_all(&root).expect("remove temp root");
}

/// A node configured with another `server.cluster_id` (#581 section 6.5):
/// a voter replaced with an empty WAL and a mistyped cluster id. Its peers
/// refuse its Raft traffic and it refuses theirs, both sides report the other
/// in readiness and count the refusals in metrics, and each logs the peer
/// once. The two nodes that agree keep serving.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cli_static_grpc_refuses_a_peer_configured_with_another_cluster_id() {
    let _guard = static_cluster_cli_test_guard().await;
    let Some(binary) = option_env!("CARGO_BIN_EXE_ursula") else {
        tracing::warn!("CARGO_BIN_EXE_ursula is not set; skipping CLI cluster id test");
        return;
    };
    const CLUSTER: &str = "static-cluster-cli";
    const OTHER: &str = "another-cluster";
    let ports = [free_port(), free_port(), free_port()];
    let peers: Vec<(u64, String)> = ports
        .iter()
        .zip(1_u64..)
        .map(|(port, node_id)| (node_id, format!("http://127.0.0.1:{port}")))
        .collect();
    let root = std::env::temp_dir().join(format!(
        "ursula-cli-cluster-id-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time after unix epoch")
            .as_nanos()
    ));
    remove_test_path(&root);
    std::fs::create_dir_all(&root).expect("create temp root");
    let mut configs = Vec::new();
    for (index, (node_id, _)) in peers.iter().enumerate() {
        let config_path = root.join(format!("node-{node_id}.toml"));
        write_cluster_config(
            &config_path,
            ports[index],
            *node_id,
            2,
            &peers,
            &root.join(format!("node-{node_id}-log")),
        );
        configs.push(config_path);
    }
    let mut children = configs
        .iter()
        .map(|config| spawn_node_logging_info(binary, config))
        .collect::<Vec<_>>();
    let client = reqwest::Client::new();
    for (_, base_url) in &peers {
        wait_until_ready(&client, base_url, &mut children).await;
    }
    let stream = format!("{}/benchcmp/cli-cluster-id", peers[0].1);
    put_until_created(&client, &stream).await;
    post_until_no_content(&client, &stream, "same-cluster").await;
    let replicated = read_until_replicated(
        &client,
        &format!(
            "{}/benchcmp/cli-cluster-id?offset=0&max_bytes=64",
            peers[2].1
        ),
    )
    .await;
    assert_eq!(replicated, b"same-cluster");
    // Every group serves on every node before node 3 goes. The write above
    // shows only the stream's group: group 0 bootstraps once all three
    // voters answer that they are empty, and the replaced node 3 never
    // answers for this cluster again.
    for peer in &peers {
        wait_readiness(
            &client,
            std::slice::from_ref(peer),
            &mut children,
            |ready| ready.ready,
        )
        .await;
    }

    // Node 3 comes back on an empty disk, configured for another cluster.
    drop(children.pop());
    remove_test_path(root.join("node-3-log"));
    let config = std::fs::read_to_string(&configs[2]).expect("read node 3 config");
    let other = config.replace(
        &format!("cluster_id = \"{CLUSTER}\""),
        &format!("cluster_id = \"{OTHER}\""),
    );
    assert_ne!(config, other, "node 3's config names the cluster id");
    std::fs::write(&configs[2], other).expect("write node 3 config");
    children.push(spawn_node_logging_info(binary, &configs[2]));
    wait_until_ready(&client, &peers[2].1, &mut children).await;

    let cluster = |id: &str| ursula_headers::ClusterId::new(id).unwrap();
    let mismatch = |node_id: u64, id: &str| ursula_proto::admin::PeerClusterMismatch {
        node_id,
        cluster_id: cluster(id),
    };
    // The leader of group 0, from the metrics of a node of the cluster. Its
    // appends to node 3 are refused there.
    let leader_id = wait_group_leader(&client, &peers[0].1, &peers, &mut children).await;
    let leader = peers
        .iter()
        .position(|(node_id, _)| *node_id == leader_id)
        .unwrap();
    assert!(leader < 2, "node 3 cannot lead: {leader_id}");
    let (_, status, leader_ready) =
        wait_readiness(&client, &peers[leader..=leader], &mut children, |ready| {
            ready.peer_cluster_mismatches.contains(&mismatch(3, OTHER))
        })
        .await;
    assert_eq!(leader_ready.cluster_id, cluster(CLUSTER));
    let (_, other_status, other_ready) =
        wait_readiness(&client, &peers[2..], &mut children, |ready| {
            ready
                .peer_cluster_mismatches
                .contains(&mismatch(leader_id, CLUSTER))
        })
        .await;
    assert_eq!(other_ready.cluster_id, cluster(OTHER));
    // Node 3 is unready, and readiness names its own cause: a peer of
    // another cluster is never the reason.
    assert_eq!(
        other_status,
        reqwest::StatusCode::SERVICE_UNAVAILABLE,
        "{other_ready:?}"
    );
    assert!(other_ready.reason.is_some(), "{other_ready:?}");

    // Nodes 1 and 2 are a quorum of their own cluster and keep serving:
    // the misconfigured peer does not take them out of service.
    post_until_no_content(&client, &stream, "-still-serving").await;
    let replicated = read_until_matches(
        &client,
        &format!(
            "{}/benchcmp/cli-cluster-id?offset=0&max_bytes=64",
            peers[1].1
        ),
        b"same-cluster-still-serving",
    )
    .await;
    assert_eq!(replicated, b"same-cluster-still-serving");
    let (_, leader_status, _) =
        wait_readiness(&client, &peers[leader..=leader], &mut children, |ready| {
            ready.ready && ready.peer_cluster_mismatches.contains(&mismatch(3, OTHER))
        })
        .await;
    assert_eq!(leader_status, reqwest::StatusCode::OK, "{status}");

    // Node 3 never took any of the cluster's Raft traffic: it applied
    // nothing, and it serves none of the stream.
    let other_metrics: ursula_proto::admin::NodeMetrics = client
        .get(format!("{}/__ursula/metrics", peers[2].1))
        .send()
        .await
        .expect("node 3 metrics")
        .json()
        .await
        .expect("node 3 metrics JSON");
    assert!(
        other_metrics
            .groups
            .iter()
            .all(|group| group.last_applied_index.unwrap_or(0) == 0
                && group.last_log_index.unwrap_or(0) == 0),
        "{other_metrics:?}"
    );
    // Its own answer is a refusal: without a placement view it names
    // another `raft.peers` node (`307 NotHosted`, design 9), or answers
    // `503`. The cluster id fences Raft traffic, not that hint, so the read
    // must not follow the redirect, whose target serves the stream.
    let no_redirect = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .expect("client without redirects");
    let read = no_redirect
        .get(format!(
            "{}/benchcmp/cli-cluster-id?offset=0&max_bytes=64",
            peers[2].1
        ))
        .send()
        .await
        .expect("read on node 3");
    let read_status = read.status();
    assert!(
        matches!(
            read_status,
            reqwest::StatusCode::TEMPORARY_REDIRECT | reqwest::StatusCode::SERVICE_UNAVAILABLE
        ),
        "node 3 answered its read with {read_status}"
    );
    let body = read.bytes().await.expect("read body on node 3");
    assert!(
        !body
            .windows(b"same-cluster".len())
            .any(|window| window == b"same-cluster"),
        "node 3 served the cluster's data: {read_status}"
    );

    // Metrics name each node's cluster, and the refusals keep coming while
    // the peer is misconfigured.
    for (node, id) in [(leader, CLUSTER), (2, OTHER)] {
        let refusals = || async {
            let metrics: serde_json::Value = client
                .get(format!("{}/__ursula/metrics", peers[node].1))
                .send()
                .await
                .expect("metrics")
                .json()
                .await
                .expect("metrics JSON");
            assert_eq!(metrics["cluster_id"], id, "{metrics}");
            assert!(
                metrics["peer_cluster_mismatches"]
                    .as_array()
                    .is_some_and(|peers| !peers.is_empty()),
                "{metrics}"
            );
            metrics["raft_grpc_peer_cluster_refusals"].as_u64().unwrap()
        };
        let first = refusals().await;
        let deadline = std::time::Instant::now()
            .checked_add(Duration::from_secs(10))
            .unwrap();
        while refusals().await <= first {
            assert!(
                std::time::Instant::now() < deadline,
                "{} counted no new refusal",
                peers[node].1
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    // Each side logs the other once, however often it retries.
    for (node, peer_id) in [(leader, 3), (2, leader_id)] {
        let log = strip_ansi(&std::fs::read_to_string(&children[node].stderr_path).unwrap());
        let logged = log
            .lines()
            .filter(|line| {
                line.contains("refusing Raft traffic with a peer of another cluster")
                    && line.contains(&format!("peer_node_id={peer_id} "))
            })
            .count();
        assert_eq!(logged, 1, "{}:\n{log}", peers[node].1);
    }

    drop(children);
    std::fs::remove_dir_all(&root).expect("remove temp root");
}

/// `text` without its ANSI escape sequences (the colors of a log line).
fn strip_ansi(text: &str) -> String {
    let mut plain = String::with_capacity(text.len());
    let mut chars = text.chars();
    while let Some(ch) = chars.next() {
        if ch == '\u{1b}' {
            // `ESC [ parameters final-byte`: skip through the final byte.
            for next in chars.by_ref() {
                if next.is_ascii_alphabetic() {
                    break;
                }
            }
        } else {
            plain.push(ch);
        }
    }
    plain
}

/// A node whose log filter is `info` whatever this process inherits, for a
/// test that reads its log.
fn spawn_node_logging_info(binary: &str, config_path: &Path) -> ChildGuard {
    let mut command = Command::new(binary);
    command
        .arg("server")
        .arg("--config")
        .arg(config_path)
        .env("RUST_LOG", "info");
    spawn_child(command, child_label("durable-node", config_path))
}

/// The leader of group 0 that `base_url`'s metrics name, once that node's
/// own metrics show it leads the group with a live quorum: a follower can
/// still name a leader that is gone.
async fn wait_group_leader(
    client: &reqwest::Client,
    base_url: &str,
    peers: &[(u64, String)],
    children: &mut [ChildGuard],
) -> u64 {
    let group_0 = |metrics: ursula_proto::admin::NodeMetrics| {
        metrics
            .groups
            .into_iter()
            .find(|group| group.raft_group_id == 0)
    };
    let deadline = std::time::Instant::now()
        .checked_add(Duration::from_secs(30))
        .unwrap();
    loop {
        if let Some(report) = exited_child_report(children) {
            panic!("a node exited while waiting for a leader:\n{report}");
        }
        let view = group_0(node_metrics(client, base_url).await);
        if let Some(leader) = view.as_ref().and_then(|group| group.current_leader)
            && let Some((_, leader_url)) = peers.iter().find(|(node_id, _)| *node_id == leader)
            && group_0(node_metrics(client, leader_url).await).is_some_and(|group| group.leads_live)
        {
            return leader;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "group 0 has no live leader: {view:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn node_metrics(
    client: &reqwest::Client,
    base_url: &str,
) -> ursula_proto::admin::NodeMetrics {
    client
        .get(format!("{base_url}/__ursula/metrics"))
        .send()
        .await
        .expect("metrics")
        .json()
        .await
        .expect("metrics JSON")
}

/// Polls the readiness of `nodes` until one's answer satisfies `done`, and
/// returns its index in `nodes` and its answer.
async fn wait_readiness(
    client: &reqwest::Client,
    nodes: &[(u64, String)],
    children: &mut [ChildGuard],
    done: impl Fn(&ursula_proto::admin::ServingReadiness) -> bool,
) -> (
    usize,
    reqwest::StatusCode,
    ursula_proto::admin::ServingReadiness,
) {
    let deadline = std::time::Instant::now()
        .checked_add(Duration::from_secs(30))
        .unwrap();
    loop {
        if let Some(report) = exited_child_report(children) {
            panic!("a node exited while waiting for readiness:\n{report}");
        }
        let mut answers = Vec::new();
        for (index, (_, base_url)) in nodes.iter().enumerate() {
            let response = client
                .get(format!("{base_url}/__ursula/ready"))
                .send()
                .await
                .expect("readiness");
            let status = response.status();
            let ready: ursula_proto::admin::ServingReadiness =
                response.json().await.expect("readiness JSON");
            if done(&ready) {
                return (index, status, ready);
            }
            answers.push(ready);
        }
        assert!(
            std::time::Instant::now() < deadline,
            "no node reported the awaited readiness: {answers:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// The bytes every record of the stream the deleted-data test removes
/// carries.
const DELETED_MARKER: &str = "deleted-data-marker-669;";

/// The `raft_groups[0]` progress of `base_url`'s metrics: last applied,
/// snapshot and purged index.
async fn group_zero_progress(
    client: &reqwest::Client,
    base_url: &str,
) -> (Option<u64>, Option<u64>, Option<u64>, serde_json::Value) {
    let metrics: serde_json::Value = client
        .get(format!("{base_url}/__ursula/metrics"))
        .send()
        .await
        .expect("read metrics")
        .json()
        .await
        .expect("metrics JSON");
    let group = &metrics["raft_groups"][0];
    let index = |key: &str| group[key].as_u64();
    (
        index("last_applied_index"),
        index("snapshot_index"),
        index("purged_index"),
        metrics,
    )
}

/// Every node-local snapshot file under `wal_dir` that holds
/// [`DELETED_MARKER`].
fn snapshot_files_with_marker(wal_dir: &Path) -> Vec<PathBuf> {
    files_with_marker(wal_dir, |name| name.contains(".snapshot.json"))
}

/// Every file under `wal_dir`, the journal segments among them, that holds
/// [`DELETED_MARKER`].
fn wal_files_with_marker(wal_dir: &Path) -> Vec<PathBuf> {
    files_with_marker(wal_dir, |_| true)
}

/// Every file under `dir` whose name `chosen` picks and that holds
/// [`DELETED_MARKER`].
fn files_with_marker(dir: &Path, chosen: fn(&str) -> bool) -> Vec<PathBuf> {
    fn walk(dir: &Path, chosen: fn(&str) -> bool, found: &mut Vec<PathBuf>) {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                walk(&path, chosen, found);
            } else if path
                .file_name()
                .and_then(|name| name.to_str())
                .is_some_and(chosen)
                && std::fs::read(&path).is_ok_and(|bytes| {
                    bytes
                        .windows(DELETED_MARKER.len())
                        .any(|window| window == DELETED_MARKER.as_bytes())
                })
            {
                found.push(path);
            }
        }
    }
    let mut found = Vec::new();
    walk(dir, chosen, &mut found);
    found
}

/// #669: within `raft.deleted_data_max_age` of a stream's deletion, each
/// replica's current snapshot no longer holds it, its Raft log is purged
/// through that snapshot, and no file in its Raft log directory holds the
/// stream's records: neither the snapshot file, which is the whole snapshot
/// with inline snapshots, nor a journal segment. Small segments, so the
/// records sit in sealed segments that quiet groups still pin.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cli_deleted_stream_leaves_snapshots_and_logs_within_the_bound() {
    let _guard = static_cluster_cli_test_guard().await;
    let Some(binary) = option_env!("CARGO_BIN_EXE_ursula") else {
        tracing::warn!("CARGO_BIN_EXE_ursula is not set; skipping the deleted-data CLI test");
        return;
    };
    let ports = [free_port(), free_port(), free_port()];
    let peers: Vec<(u64, String)> = ports
        .iter()
        .zip(1_u64..)
        .map(|(port, node_id)| (node_id, format!("http://127.0.0.1:{port}")))
        .collect();
    let root = std::env::temp_dir().join(format!(
        "ursula-cli-deleted-data-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time after unix epoch")
            .as_nanos()
    ));
    remove_test_path(&root);
    std::fs::create_dir_all(&root).expect("create temp root");
    let max_age = Duration::from_secs(90);
    let mut children = Vec::new();
    let mut admins = Vec::new();
    let mut wal_dirs = Vec::new();
    for (node_id, port) in (1_u64..).zip(ports) {
        let config = root.join(format!("node-{node_id}.toml"));
        let wal_dir = root.join(format!("node-{node_id}-log"));
        let admin_port = write_cluster_config(&config, port, node_id, 1, &peers, &wal_dir);
        let text = std::fs::read_to_string(&config).expect("read node config");
        assert!(text.contains("[raft.wal]\n"));
        assert!(text.contains("[raft]\n"));
        std::fs::write(
            &config,
            text.replace(
                "[raft]\n",
                "[raft]\ndeleted_data_max_age = \"90s\"\n\
                 install_snapshot_timeout = \"10s\"\n",
            )
            .replace("[raft.wal]\n", "[raft.wal]\nsegment_size = \"64KiB\"\n"),
        )
        .expect("write node config");
        admins.push(format!("http://127.0.0.1:{admin_port}"));
        wal_dirs.push(wal_dir);
        children.push(spawn_node_with_cluster_config(binary, &config));
    }
    let client = reqwest::Client::new();
    for (_, url) in &peers {
        wait_until_ready(&client, url, &mut children).await;
    }

    let stream = format!("{}/erased/marked", peers[0].1);
    put_until_created(&client, &stream).await;
    for _ in 0..3 {
        post_until_no_content(&client, &stream, DELETED_MARKER).await;
    }
    for (_, url) in &peers {
        let payload = read_until_matches(
            &client,
            &format!("{url}/erased/marked?offset=0&max_bytes=1024"),
            DELETED_MARKER.repeat(3).as_bytes(),
        )
        .await;
        assert_eq!(payload, DELETED_MARKER.repeat(3).as_bytes());
    }
    // Every replica's current snapshot holds the stream before it goes.
    for (admin, wal_dir) in admins.iter().zip(&wal_dirs) {
        let snapshot = admin_test_post(&client, format!("{admin}/__ursula/raft/0/snapshot"))
            .await
            .send()
            .await
            .expect("trigger a snapshot");
        assert_eq!(snapshot.status(), reqwest::StatusCode::OK);
        assert!(
            !snapshot_files_with_marker(wal_dir).is_empty(),
            "{admin}: the snapshot before the deletion holds the stream"
        );
    }

    let mut deleted = false;
    for _ in 0..100 {
        if let Ok(response) = client.delete(&stream).send().await
            && response.status().is_success()
        {
            deleted = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(deleted, "delete {stream}");
    let deleted_at = tokio::time::Instant::now();
    let mut removal_index = 0;
    for (_, url) in &peers {
        let (applied, ..) = group_zero_progress(&client, url).await;
        removal_index = removal_index.max(applied.expect("applied index"));
    }
    // The replicas owe a snapshot that drops the stream, and report it.
    for (_, url) in &peers {
        let mut reported = serde_json::Value::Null;
        for _ in 0..100 {
            let (.., metrics) = group_zero_progress(&client, url).await;
            reported = metrics["deleted_data"].clone();
            if reported
                .as_array()
                .is_some_and(|groups| groups.iter().any(|group| group["raft_group_id"] == 0))
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert!(
            reported
                .as_array()
                .is_some_and(|groups| groups.iter().any(|group| group["state"] == "owed")),
            "{url}: {reported}"
        );
    }

    let mut pending = (0..peers.len()).collect::<Vec<_>>();
    while !pending.is_empty() {
        assert!(
            deleted_at.elapsed() < max_age.saturating_add(Duration::from_secs(30)),
            "replicas {pending:?} still hold the deleted stream past the bound"
        );
        let mut still = Vec::new();
        for index in pending {
            let (applied, snapshot, purged, metrics) =
                group_zero_progress(&client, &peers[index].1).await;
            let scrubbed = applied >= Some(removal_index)
                && snapshot >= Some(removal_index)
                && purged >= snapshot
                && wal_files_with_marker(&wal_dirs[index]).is_empty()
                && metrics["raft_deleted_data_oldest_age_ms"] == 0;
            if !scrubbed {
                still.push(index);
            }
        }
        pending = still;
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    for (_, url) in &peers {
        let read = client
            .get(format!("{url}/erased/marked?offset=0"))
            .send()
            .await
            .expect("read the deleted stream");
        assert_eq!(read.status(), reqwest::StatusCode::NOT_FOUND);
    }

    drop(children);
    std::fs::remove_dir_all(&root).expect("remove temp root");
}

/// #669: a single node, which lists only itself in `raft.peers`, runs the
/// snapshot driver, and its group's leader sweeps expired streams in the
/// background. A stream whose TTL ran out and that nothing reads or writes
/// again leaves the node's snapshot, its Raft log and every file in its Raft
/// log directory within `raft.deleted_data_max_age` of the sweep, and a read
/// of it answers 404.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cli_single_node_expired_stream_leaves_its_files_within_the_bound() {
    let _guard = static_cluster_cli_test_guard().await;
    let Some(binary) = option_env!("CARGO_BIN_EXE_ursula") else {
        tracing::warn!("CARGO_BIN_EXE_ursula is not set; skipping the single-node TTL CLI test");
        return;
    };
    let port = free_port();
    let base_url = format!("http://127.0.0.1:{port}");
    let root = std::env::temp_dir().join(format!(
        "ursula-cli-single-node-ttl-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time after unix epoch")
            .as_nanos()
    ));
    remove_test_path(&root);
    std::fs::create_dir_all(&root).expect("create temp root");
    let max_age = Duration::from_secs(16);
    let config = root.join("node.toml");
    let wal_dir = root.join("node-log");
    let admin_port = write_single_node_cluster_config(&config, port, 1, 1, &base_url, &wal_dir);
    let text = std::fs::read_to_string(&config).expect("read node config");
    assert!(text.contains("[raft.wal]\n"));
    std::fs::write(
        &config,
        text.replace(
            "[raft]\n",
            "[raft]\ndeleted_data_max_age = \"16s\"\n\
             install_snapshot_timeout = \"2s\"\n",
        )
        .replace("[raft.wal]\n", "[raft.wal]\nsegment_size = \"64KiB\"\n"),
    )
    .expect("write node config");
    let admin = format!("http://127.0.0.1:{admin_port}");
    let mut children = vec![spawn_node_with_cluster_config(binary, &config)];
    let client = reqwest::Client::new();
    wait_until_ready(&client, &base_url, &mut children).await;

    let stream = format!("{base_url}/expiring/marked");
    let mut created = false;
    for _ in 0..100 {
        if let Ok(response) = client
            .put(&stream)
            .header("content-type", "text/plain")
            .header("Stream-TTL", "2")
            .send()
            .await
            && response.status() == reqwest::StatusCode::CREATED
        {
            created = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(created, "create {stream}");
    for _ in 0..3 {
        post_until_no_content(&client, &stream, DELETED_MARKER).await;
    }
    // A live stream in the same group, across several journal segments,
    // which the node must read back whole after the scrub and a restart.
    let live = format!("{base_url}/expiring/live");
    put_until_created(&client, &live).await;
    let mut written = Vec::new();
    for index in 0..96 {
        let record = format!("live-record-{index:03};{}\n", "x".repeat(1_000));
        let mut appended = false;
        for _ in 0..100 {
            if let Ok(response) = client
                .post(&live)
                .header("content-type", "text/plain")
                .body(record.clone())
                .send()
                .await
                && response.status() == reqwest::StatusCode::NO_CONTENT
            {
                appended = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert!(appended, "append {index} to {live}");
        written.extend_from_slice(record.as_bytes());
    }
    // The node's snapshot holds the stream before it expires.
    let snapshot = admin_test_post(&client, format!("{admin}/__ursula/raft/0/snapshot"))
        .await
        .send()
        .await
        .expect("trigger a snapshot");
    assert_eq!(snapshot.status(), reqwest::StatusCode::OK);
    assert!(
        !snapshot_files_with_marker(&wal_dir).is_empty(),
        "the snapshot before the expiry holds the stream"
    );

    // Nothing reads or writes the stream again: the leader's sweep removes
    // it within the TTL, the sweep's grace and its interval.
    let mut removal_index = None;
    for _ in 0..200 {
        let (applied, .., metrics) = group_zero_progress(&client, &base_url).await;
        if metrics["ttl_expiry_sweeps"]
            .as_u64()
            .is_some_and(|sweeps| sweeps > 0)
        {
            removal_index = applied;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let removal_index = removal_index.expect("the leader swept the expired stream");
    let swept_at = tokio::time::Instant::now();
    loop {
        assert!(
            swept_at.elapsed() < max_age.saturating_add(Duration::from_secs(10)),
            "the node still holds the expired stream past the bound"
        );
        let (_, snapshot, purged, metrics) = group_zero_progress(&client, &base_url).await;
        if snapshot >= Some(removal_index)
            && purged >= snapshot
            && wal_files_with_marker(&wal_dir).is_empty()
            && metrics["raft_deleted_data_oldest_age_ms"] == 0
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    let read = client
        .get(format!("{stream}?offset=0"))
        .send()
        .await
        .expect("read the expired stream");
    assert_eq!(read.status(), reqwest::StatusCode::NOT_FOUND);

    // A single node has no peer to repair from: after the purges and the
    // journal rewrites, a restart, here a kill, reads the live stream back
    // byte for byte.
    drop(children);
    let mut children = vec![spawn_node_with_cluster_config(binary, &config)];
    wait_until_ready(&client, &base_url, &mut children).await;
    let mut read_back = Vec::new();
    let mut offset = "0".to_owned();
    for _ in 0..200 {
        if read_back.len() >= written.len() {
            break;
        }
        let Ok(response) = client.get(format!("{live}?offset={offset}")).send().await else {
            tokio::time::sleep(Duration::from_millis(50)).await;
            continue;
        };
        if response.status() != reqwest::StatusCode::OK {
            tokio::time::sleep(Duration::from_millis(50)).await;
            continue;
        }
        let next = response
            .headers()
            .get("stream-next-offset")
            .and_then(|value| value.to_str().ok())
            .expect("a next offset")
            .to_owned();
        read_back.extend_from_slice(&response.bytes().await.expect("read the live stream"));
        offset = next;
    }
    assert_eq!(
        read_back.len(),
        written.len(),
        "the live stream after the restart"
    );
    assert!(read_back == written, "the live stream's bytes changed");

    drop(children);
    std::fs::remove_dir_all(&root).expect("remove temp root");
}

/// The S3 snapshot objects of group 0 that node `node_id` owns, under
/// `snapshots/node-{node_id}/group-0/` (#687), read through `cold_store`,
/// whose root is the snapshot store's.
async fn node_snapshot_objects(cold_store: &ColdStore, node_id: u64) -> Vec<String> {
    let dir = format!("snapshots/node-{node_id}/group-0/");
    cold_store
        .list_file_names(&dir)
        .await
        .expect("list the node's snapshot objects")
        .into_iter()
        .filter(|name| name.ends_with(".snap"))
        .map(|name| format!("{dir}{name}"))
        .collect()
}

/// The S3 snapshot objects of group 0 of nodes 1 to 3 that hold
/// [`DELETED_MARKER`], read through `cold_store`, whose root is the
/// snapshot store's.
async fn snapshot_objects_with_marker(cold_store: &ColdStore) -> Vec<String> {
    let mut objects = Vec::new();
    for node_id in 1..=3 {
        objects.extend(node_snapshot_objects(cold_store, node_id).await);
    }
    let mut found = Vec::new();
    for path in objects {
        // Its node's GC may delete it between the listing and here.
        let Ok(size) = cold_store.object_size(&path).await else {
            continue;
        };
        let Ok(stored) = cold_store
            .read_whole_object_range(&path, size, 0, usize::try_from(size).unwrap())
            .await
        else {
            continue;
        };
        let body = zstd::stream::decode_all(stored.as_slice()).expect("decode a snapshot object");
        if body
            .windows(DELETED_MARKER.len())
            .any(|window| window == DELETED_MARKER.as_bytes())
        {
            found.push(path);
        }
    }
    found
}

/// #669, with S3 snapshots: within `raft.deleted_data_max_age` of a
/// stream's deletion, snapshot GC has deleted every snapshot object of the
/// group that held it, the objects each replica replaced included. Each node
/// writes its objects under `snapshots/node-{id}/group-0/` and, once quiet,
/// keeps only its current one there (#687). The data stays hot, so only the
/// snapshots hold it. Runs on real S3 in CI.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cli_deleted_stream_leaves_s3_snapshot_objects_within_the_bound() {
    let _guard = static_cluster_cli_test_guard().await;
    if std::env::var("URSULA_COLD_S3_INTEGRATION").ok().as_deref() != Some("1") {
        tracing::warn!(
            "skipping the S3 snapshot GC test; set URSULA_COLD_S3_INTEGRATION=1 and URSULA_COLD_S3_BUCKET"
        );
        return;
    }
    let Some(binary) = option_env!("CARGO_BIN_EXE_ursula") else {
        tracing::warn!("CARGO_BIN_EXE_ursula is not set; skipping the S3 snapshot GC test");
        return;
    };
    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time after unix epoch")
        .as_nanos();
    // CI scopes each job's credentials to URSULA_S3_PREFIX.
    let cold_root = match std::env::var("URSULA_S3_PREFIX") {
        Ok(prefix) => format!("{prefix}/ursula-cli-s3-snapshot-gc/{suffix}"),
        Err(_) => format!("ursula-cli-s3-snapshot-gc/{suffix}"),
    };
    let ports = [free_port(), free_port(), free_port()];
    let peers: Vec<(u64, String)> = ports
        .iter()
        .zip(1_u64..)
        .map(|(port, node_id)| (node_id, format!("http://127.0.0.1:{port}")))
        .collect();
    let root = std::env::temp_dir().join(format!(
        "ursula-cli-s3-snapshot-gc-{}-{suffix}",
        std::process::id()
    ));
    remove_test_path(&root);
    std::fs::create_dir_all(&root).expect("create temp root");
    let max_age = Duration::from_secs(160);
    let mut configs = Vec::new();
    let mut admins = Vec::new();
    for (node_id, port) in (1_u64..).zip(ports) {
        let config = root.join(format!("node-{node_id}.toml"));
        let wal_dir = root.join(format!("node-{node_id}-log"));
        let admin_port = write_node_toml(
            &config,
            port,
            node_id,
            1,
            &peers,
            &wal_dir,
            "s3",
            Some(&cold_root),
        );
        let text = std::fs::read_to_string(&config).expect("read node config");
        assert!(text.contains("[raft]\n"));
        assert!(text.contains("flush_interval = \"1s\"\n"));
        // Keep the records hot, so that the snapshots hold them.
        std::fs::write(
            &config,
            text.replace(
                "[raft]\n",
                "[raft]\ndeleted_data_max_age = \"160s\"\n\
                 install_snapshot_timeout = \"20s\"\n",
            )
            .replace("flush_interval = \"1s\"\n", "flush_interval = \"1h\"\n")
                // A follower's install copy, up to four snapshot-store
                // requests of 5 s each with one retry against a flake of
                // the real store, fits inside the 20 s install timeout.
                + "\n[storage.snapshot]\ns3_timeout = \"1500ms\"\ns3_max_retries = 1\n",
        )
        .expect("write node config");
        admins.push(format!("http://127.0.0.1:{admin_port}"));
        configs.push(config);
    }
    let config = ursula_config::load_config(Some(&configs[0]), None, None)
        .unwrap_or_else(|err| panic!("load config for the cold store: {err}"));
    let cold_store = ColdStore::try_new(&config.storage.cold)
        .unwrap_or_else(|err| panic!("cold store creation failed: {err}"));
    let mut children = configs
        .iter()
        .map(|config| spawn_node_with_cluster_config_and_cold_s3(binary, config))
        .collect::<Vec<_>>();
    let client = reqwest::Client::new();
    for (_, url) in &peers {
        wait_until_ready(&client, url, &mut children).await;
    }

    let stream = format!("{}/erased/marked", peers[0].1);
    put_until_created(&client, &stream).await;
    for _ in 0..3 {
        post_until_no_content(&client, &stream, DELETED_MARKER).await;
    }
    for (_, url) in &peers {
        read_until_matches(
            &client,
            &format!("{url}/erased/marked?offset=0&max_bytes=1024"),
            DELETED_MARKER.repeat(3).as_bytes(),
        )
        .await;
    }
    // Every replica's current snapshot object holds the stream before it
    // goes.
    for admin in &admins {
        let snapshot = admin_test_post(&client, format!("{admin}/__ursula/raft/0/snapshot"))
            .await
            .send()
            .await
            .expect("trigger a snapshot");
        assert_eq!(snapshot.status(), reqwest::StatusCode::OK);
    }
    assert!(
        !snapshot_objects_with_marker(&cold_store).await.is_empty(),
        "the snapshot objects before the deletion hold the stream"
    );

    let mut deleted = false;
    for _ in 0..100 {
        if let Ok(response) = client.delete(&stream).send().await
            && response.status().is_success()
        {
            deleted = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(deleted, "delete {stream}");
    let deleted_at = tokio::time::Instant::now();
    loop {
        let left = snapshot_objects_with_marker(&cold_store).await;
        if left.is_empty() {
            break;
        }
        assert!(
            deleted_at.elapsed() < max_age.saturating_add(Duration::from_secs(30)),
            "snapshot objects {left:?} still hold the deleted stream past the bound"
        );
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
    let mut deleted_objects = 0_u64;
    for (_, url) in &peers {
        let (.., metrics) = group_zero_progress(&client, url).await;
        deleted_objects = deleted_objects.saturating_add(
            metrics["snapshot_gc_deleted_objects"]
                .as_u64()
                .expect("snapshot_gc_deleted_objects"),
        );
    }
    assert!(deleted_objects > 0, "snapshot GC deleted no object");

    // Once quiet, each node's directory holds only its current object: one
    // per node, which it owns. GC never deleted what a replica names, which
    // the restart below reads back.
    let quiet_by = tokio::time::Instant::now()
        .checked_add(Duration::from_secs(60))
        .expect("a deadline");
    for (node_id, _) in &peers {
        loop {
            let objects = node_snapshot_objects(&cold_store, *node_id).await;
            for object in &objects {
                let owner = ursula_runtime::OwnedKey::parse(
                    &ursula_runtime::SnapshotPrefix::try_from("snapshots").unwrap(),
                    object,
                )
                .unwrap_or_else(|err| panic!("{object} is not an owned key: {err}"))
                .owner();
                assert_eq!(owner.node_id(), *node_id, "{object}");
            }
            if objects.len() == 1 {
                break;
            }
            assert!(
                tokio::time::Instant::now() < quiet_by,
                "node {node_id}'s snapshot directory holds {objects:?} once quiet"
            );
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
    }
    // A restarted replica restores its current snapshot from the store and
    // serves group 0 again.
    let (applied_before, ..) = group_zero_progress(&client, &peers[2].1).await;
    drop(children.remove(2));
    children.push(spawn_node_with_cluster_config_and_cold_s3(
        binary,
        &configs[2],
    ));
    wait_until_ready(&client, &peers[2].1, &mut children).await;
    let mut served = false;
    for _ in 0..300 {
        if let Ok(response) = client
            .get(format!("{}/__ursula/metrics", peers[2].1))
            .send()
            .await
            && response.status().is_success()
        {
            let (applied, ..) = group_zero_progress(&client, &peers[2].1).await;
            if applied >= applied_before {
                served = true;
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(served, "the restarted replica serves group 0 again");
    let mut status = None;
    for _ in 0..100 {
        status = client
            .get(format!("{}/erased/marked?offset=0", peers[2].1))
            .send()
            .await
            .ok()
            .map(|response| response.status());
        if status == Some(reqwest::StatusCode::NOT_FOUND) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(
        status,
        Some(reqwest::StatusCode::NOT_FOUND),
        "the restarted replica reads the deleted stream as gone"
    );

    drop(children);
    cold_store
        .remove_all("")
        .await
        .expect("clear the S3 test root");
    std::fs::remove_dir_all(&root).expect("remove temp root");
}

/// An S3 failure as its kind, operation and path only. The store's own
/// message can name the assumed role or the bucket, and CI logs are public.
fn s3_failure(operation: &str, path: &str, err: &std::io::Error) -> String {
    format!("{operation} {path:?} failed: {:?}", err.kind())
}

/// Every file named `name` under `dir`, at any depth.
fn files_named(dir: &Path, name: &str) -> Vec<PathBuf> {
    fn walk(dir: &Path, name: &str, found: &mut Vec<PathBuf>) {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                walk(&path, name, found);
            } else if path.file_name().is_some_and(|file| file == name) {
                found.push(path);
            }
        }
    }
    let mut found = Vec::new();
    walk(dir, name, &mut found);
    found
}

/// The object keys that the snapshot record of data group 0 in `wal_dir`
/// (`group-0.snapshot.json` under a `raft-log/core-*` directory) names,
/// decoded as a restore decodes it: none before the node wrote a record,
/// else its one S3 key (#687). The meta group's own record, also
/// `group-0.snapshot.json` but under `raft-log/meta/`, is always inline
/// and is not looked at.
fn named_snapshot_keys(wal_dir: &Path) -> BTreeSet<String> {
    let cores = std::fs::read_dir(wal_dir.join("raft-log"))
        .into_iter()
        .flatten()
        .flatten()
        .map(|entry| entry.path())
        .filter(|path| {
            path.is_dir()
                && path
                    .file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| name.starts_with("core-"))
        });
    let records: Vec<PathBuf> = cores
        .flat_map(|core| files_named(&core, "group-0.snapshot.json"))
        .collect();
    assert!(records.len() <= 1, "{records:?}");
    let Some(record) = records.first() else {
        return BTreeSet::new();
    };
    // `None`: the node's directory was wiped since the walk, or the record
    // is of a retired generation and restores nothing.
    let Some(pointer) = ursula_raft::current_snapshot_pointer(record)
        .unwrap_or_else(|err| panic!("read the snapshot record {}: {err}", record.display()))
    else {
        return BTreeSet::new();
    };
    match pointer.location {
        ursula_runtime::SnapshotLocation::S3 { key, .. } => [key].into(),
        other => panic!(
            "the snapshot record {} names {other:?}, not an S3 object",
            record.display()
        ),
    }
}

/// What each of `nodes`, by id and WAL directory, names of group 0.
fn snapshot_names(nodes: &[(u64, PathBuf)]) -> Vec<ursula_runtime::snapshot_harness::NodeNames> {
    nodes
        .iter()
        .map(|(node_id, wal_dir)| {
            ursula_runtime::snapshot_harness::NodeNames::new(*node_id, named_snapshot_keys(wal_dir))
        })
        .collect()
}

/// What the bucket holds under each node's directory of group 0
/// (`{prefix}/node-{n}/group-0/`), and what each node's snapshot record
/// names. The names are read before and after the listing, and the reading
/// starts again when they changed in between, so the view and the names
/// belong to one point in time.
async fn snapshot_ownership(
    cold_store: &ColdStore,
    prefix: &ursula_runtime::SnapshotPrefix,
    nodes: &[(u64, PathBuf)],
) -> (
    ursula_runtime::snapshot_harness::SnapshotStoreView,
    Vec<ursula_runtime::snapshot_harness::NodeNames>,
) {
    for _ in 0..20 {
        let before = snapshot_names(nodes);
        let mut objects = BTreeMap::new();
        for (node_id, _) in nodes {
            let dir = format!(
                "{}group-0/",
                ursula_runtime::snapshot_harness::node_directory(prefix, *node_id)
            );
            for name in cold_store
                .list_file_names(&dir)
                .await
                .unwrap_or_else(|err| panic!("{}", s3_failure("list", &dir, &err)))
            {
                objects.insert(
                    format!("{dir}{name}"),
                    ursula_runtime::snapshot_harness::ViewObject::default(),
                );
            }
        }
        let after = snapshot_names(nodes);
        if before == after {
            let view = ursula_runtime::snapshot_harness::SnapshotStoreView {
                prefix: prefix.clone(),
                objects,
                deletes: Vec::new(),
            };
            return (view, after);
        }
    }
    panic!("the snapshot records kept changing while the bucket was listed");
}

/// Checks I1, every key a node names exists, on the bucket now.
async fn check_named_snapshot_objects_exist(
    cold_store: &ColdStore,
    prefix: &ursula_runtime::SnapshotPrefix,
    nodes: &[(u64, PathBuf)],
) -> Vec<ursula_runtime::snapshot_harness::NodeNames> {
    let (view, names) = snapshot_ownership(cold_store, prefix, nodes).await;
    if let Err(violation) = ursula_runtime::snapshot_harness::check_named_keys_exist(&view, &names)
    {
        panic!("{violation}: {view:?}, {names:?}");
    }
    names
}

/// Waits until I5 holds, each node's directory holding only the keys its
/// record names, and checks I1 on every reading. Returns the names.
async fn wait_quiet_snapshot_prefixes(
    cold_store: &ColdStore,
    prefix: &ursula_runtime::SnapshotPrefix,
    nodes: &[(u64, PathBuf)],
) -> Vec<ursula_runtime::snapshot_harness::NodeNames> {
    // A node's GC pass runs once a key leaves its holds, and right after
    // its restore, which removes what a wiped node wrote before the wipe,
    // and again one install timeout later.
    let deadline = tokio::time::Instant::now()
        .checked_add(Duration::from_secs(180))
        .expect("a deadline");
    loop {
        let (view, names) = snapshot_ownership(cold_store, prefix, nodes).await;
        if let Err(violation) =
            ursula_runtime::snapshot_harness::check_named_keys_exist(&view, &names)
        {
            panic!("{violation}: {view:?}, {names:?}");
        }
        match ursula_runtime::snapshot_harness::check_quiet_prefixes(&view, &names) {
            Ok(()) => return names,
            Err(violation) => assert!(
                tokio::time::Instant::now() < deadline,
                "{violation} once quiet: {view:?}, {names:?}"
            ),
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

/// The one key node `node_id` names, which it owns.
fn owned_named_key(
    names: &[ursula_runtime::snapshot_harness::NodeNames],
    prefix: &ursula_runtime::SnapshotPrefix,
    node_id: u64,
) -> String {
    let node = names
        .iter()
        .find(|node| node.node_id == node_id)
        .expect("the node's names");
    let mut keys = node.named.iter();
    let (Some(key), None) = (keys.next(), keys.next()) else {
        panic!("node {node_id} names {:?}, not one key", node.named);
    };
    let owner = ursula_runtime::OwnedKey::parse(prefix, key)
        .unwrap_or_else(|err| panic!("{key} is not an owned key: {err}"))
        .owner();
    assert_eq!(owner.node_id(), node_id, "{key}");
    assert_eq!(owner.raft_group_id().0, 0, "{key}");
    key.clone()
}

/// Waits until `base_url` serves group 0: it answers ready, names a leader
/// and applied at least `applied`.
async fn wait_serves_group_zero(
    client: &reqwest::Client,
    base_url: &str,
    applied: u64,
    children: &mut [ChildGuard],
) {
    let deadline = tokio::time::Instant::now()
        .checked_add(Duration::from_secs(120))
        .expect("a deadline");
    loop {
        if let Some(report) = exited_child_report(children) {
            panic!("a node exited while {base_url} caught up:\n{report}");
        }
        let ready = client
            .get(format!("{base_url}/__ursula/ready"))
            .send()
            .await
            .is_ok_and(|response| response.status() == reqwest::StatusCode::OK);
        let group = node_metrics(client, base_url)
            .await
            .groups
            .into_iter()
            .find(|group| group.raft_group_id == 0);
        if ready
            && group.as_ref().is_some_and(|group| {
                group.current_leader.is_some()
                    && group
                        .last_applied_index
                        .is_some_and(|index| index >= applied)
            })
        {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "{base_url} does not serve group 0 at index {applied}: ready {ready}, {group:?}"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// Asks node `admin`'s group 0 for a snapshot and returns its index.
async fn snapshot_group_zero(client: &reqwest::Client, admin: &str) -> u64 {
    let response = admin_test_post(client, format!("{admin}/__ursula/raft/0/snapshot"))
        .await
        .send()
        .await
        .expect("trigger a snapshot");
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    let snapshot: ursula_proto::admin::SnapshotResponse =
        response.json().await.expect("snapshot JSON");
    snapshot.snapshot_index.expect("a snapshot index")
}

/// The last applied index of group 0 on `base_url`.
async fn group_zero_applied(client: &reqwest::Client, base_url: &str) -> u64 {
    group_zero_progress(client, base_url)
        .await
        .0
        .expect("group 0 applied an entry")
}

/// #687, on real S3: a node whose disk was lost installs the leader's
/// snapshot by writing its own copy under `snapshots/node-3/group-0/`, and
/// every node, the wiped one last, restores the object its record names
/// after a restart. At every reading every key a node names exists (I1),
/// and once quiet each node's directory holds only what it names (I5).
/// Runs on real S3 in CI.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cli_a_wiped_node_installs_its_own_snapshot_copy_and_every_node_restores_from_s3() {
    let _guard = static_cluster_cli_test_guard().await;
    if std::env::var("URSULA_COLD_S3_INTEGRATION").ok().as_deref() != Some("1") {
        tracing::warn!(
            "skipping the S3 owned snapshot test; set URSULA_COLD_S3_INTEGRATION=1 and URSULA_COLD_S3_BUCKET"
        );
        return;
    }
    let Some(binary) = option_env!("CARGO_BIN_EXE_ursula") else {
        tracing::warn!("CARGO_BIN_EXE_ursula is not set; skipping the S3 owned snapshot test");
        return;
    };
    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time after unix epoch")
        .as_nanos();
    // CI scopes each job's credentials to URSULA_S3_PREFIX.
    let cold_root = match std::env::var("URSULA_S3_PREFIX") {
        Ok(prefix) => format!("{prefix}/ursula-cli-s3-snapshot-owned/{suffix}"),
        Err(_) => format!("ursula-cli-s3-snapshot-owned/{suffix}"),
    };
    let ports = [free_port(), free_port(), free_port()];
    let peers: Vec<(u64, String)> = ports
        .iter()
        .zip(1_u64..)
        .map(|(port, node_id)| (node_id, format!("http://127.0.0.1:{port}")))
        .collect();
    let root = std::env::temp_dir().join(format!(
        "ursula-cli-s3-snapshot-owned-{}-{suffix}",
        std::process::id()
    ));
    remove_test_path(&root);
    std::fs::create_dir_all(&root).expect("create temp root");
    let mut configs = Vec::new();
    let mut admins = Vec::new();
    let mut nodes = Vec::new();
    for (node_id, port) in (1_u64..).zip(ports) {
        let config = root.join(format!("node-{node_id}.toml"));
        let wal_dir = root.join(format!("node-{node_id}-log"));
        let admin_port = write_node_toml(
            &config,
            port,
            node_id,
            1,
            &peers,
            &wal_dir,
            "s3",
            Some(&cold_root),
        );
        let text = std::fs::read_to_string(&config).expect("read node config");
        assert!(text.contains("[raft]\n"));
        std::fs::write(
            &config,
            text.replace(
                "[raft]\n",
                "[raft]\ninstall_snapshot_timeout = \"40s\"\n",
            )
                // A short request timeout, so a lost request fails fast.
                + "\n[storage.snapshot]\ns3_timeout = \"3s\"\ns3_max_retries = 1\n",
        )
        .expect("write node config");
        admins.push(format!("http://127.0.0.1:{admin_port}"));
        configs.push(config);
        nodes.push((node_id, wal_dir));
    }
    let config = ursula_config::load_config(Some(&configs[0]), None, None)
        .unwrap_or_else(|err| panic!("load config for the cold store: {err}"));
    config
        .validate()
        .unwrap_or_else(|err| panic!("the node config is valid: {err}"));
    // A follower's install copy, four snapshot-store requests of two 3 s
    // attempts and a backoff of at most 2 s (32 s), fits inside the
    // install timeout, as the server's startup check requires.
    assert!(
        config.storage.snapshot.install_copy_budget()
            <= config.raft.install_snapshot_timeout.as_duration()
    );
    let prefix = config.storage.snapshot.snapshot_prefix();
    let cold_store = ColdStore::try_new(&config.storage.cold)
        .unwrap_or_else(|err| panic!("{}", s3_failure("open the cold store", "", &err)));
    let mut children = configs
        .iter()
        .map(|config| spawn_node_with_cluster_config_and_cold_s3(binary, config))
        .collect::<Vec<_>>();
    let client = reqwest::Client::new();
    for (_, url) in &peers {
        wait_until_ready(&client, url, &mut children).await;
    }

    // 1. Every node snapshots group 0 into its own directory.
    let stream = format!("{}/owned/snapshots", peers[0].1);
    put_until_created(&client, &stream).await;
    for _ in 0..3 {
        post_until_no_content(&client, &stream, "before-the-wipe;").await;
    }
    let mut expected = "before-the-wipe;".repeat(3);
    for (_, url) in &peers {
        read_until_matches(
            &client,
            &format!("{url}/owned/snapshots?offset=0&max_bytes=1024"),
            expected.as_bytes(),
        )
        .await;
    }
    for admin in &admins {
        snapshot_group_zero(&client, admin).await;
    }

    // 2. Each node names one key of its own, which exists, and once quiet
    // its directory holds nothing else.
    let names = wait_quiet_snapshot_prefixes(&cold_store, &prefix, &nodes).await;
    for (node_id, _) in &nodes {
        owned_named_key(&names, &prefix, *node_id);
    }
    let pre_wipe = owned_named_key(&names, &prefix, 3);

    // 3. Node 3 loses its disk. Nodes 1 and 2 snapshot past a further write
    // and purge their logs through it, so node 3 can catch up only through
    // an installed snapshot. Its old objects stay in its directory.
    let lost_root = try_node_metrics(&client, &peers[2].1)
        .await
        .expect("node 3's metrics")
        .process_incarnation
        .root;
    drop(children.remove(2));
    remove_test_path(&nodes[2].1);
    post_until_no_content(&client, &stream, "while-wiped;").await;
    expected.push_str("while-wiped;");
    for (url, admin) in peers.iter().map(|(_, url)| url).zip(&admins).take(2) {
        read_until_matches(
            &client,
            &format!("{url}/owned/snapshots?offset=0&max_bytes=1024"),
            expected.as_bytes(),
        )
        .await;
        let index = snapshot_group_zero(&client, admin).await;
        let purge = admin_purge(
            &client,
            format!("{admin}/__ursula/raft/0/purge?upto={index}"),
        )
        .await;
        assert_eq!(purge.status(), reqwest::StatusCode::OK);
    }
    let leader = wait_group_leader(&client, &peers[0].1, &peers, &mut children).await;
    let leader_url = &peers[usize::try_from(leader.checked_sub(1).unwrap()).unwrap()].1;
    let leader_applied = group_zero_applied(&client, leader_url).await;
    children.push(spawn_node_with_cluster_config_and_cold_s3(
        binary,
        &configs[2],
    ));
    wait_until_ready(&client, &peers[2].1, &mut children).await;
    // The empty disk is refused while the lost one is bound.
    wait_root_refused(&client, &peers[2].1, &mut children).await;
    let ctl_nodes = peers
        .iter()
        .zip(&admins)
        .map(|((node_id, url), admin)| ursula_ctl::NodeInfo {
            id: *node_id,
            admin_url: url::Url::parse(admin).expect("admin url"),
            host: "127.0.0.1".to_owned(),
            http_url: Some(url::Url::parse(url).expect("public url")),
            metrics_url: None,
            client_url: None,
        })
        .collect::<Vec<_>>();
    replace_lost_disk(&ctl_nodes, 3, lost_root).await;

    // 4. Node 3 serves group 0 from its own copy of the leader's snapshot.
    wait_serves_group_zero(&client, &peers[2].1, leader_applied, &mut children).await;
    let names = check_named_snapshot_objects_exist(&cold_store, &prefix, &nodes).await;
    let copy = owned_named_key(&names, &prefix, 3);
    let leader_key = owned_named_key(&names, &prefix, leader);
    let index_of = |key: &str| {
        ursula_runtime::OwnedKey::parse(&prefix, key)
            .unwrap_or_else(|err| panic!("{key} is not an owned key: {err}"))
            .index()
    };
    assert_eq!(
        index_of(&copy),
        index_of(&leader_key),
        "node 3's {copy} is not a copy of the leader's {leader_key}"
    );

    // 5. The leader builds again. Once quiet, its directory holds only its
    // new object, and node 3's only its copy: its objects from before the
    // wipe are gone.
    post_until_no_content(&client, &stream, "after-the-install;").await;
    expected.push_str("after-the-install;");
    read_until_matches(
        &client,
        &format!("{leader_url}/owned/snapshots?offset=0&max_bytes=1024"),
        expected.as_bytes(),
    )
    .await;
    let leader_admin = &admins[usize::try_from(leader.checked_sub(1).unwrap()).unwrap()];
    snapshot_group_zero(&client, leader_admin).await;
    let names = wait_quiet_snapshot_prefixes(&cold_store, &prefix, &nodes).await;
    assert_ne!(
        owned_named_key(&names, &prefix, leader),
        leader_key,
        "the leader's new build replaced its object"
    );
    assert_eq!(owned_named_key(&names, &prefix, 3), copy);
    let (view, _) = snapshot_ownership(&cold_store, &prefix, &nodes).await;
    for (gone, whose) in [
        (&pre_wipe, "node 3's object from before the wipe"),
        (&leader_key, "the leader's replaced object"),
    ] {
        assert!(
            !view.objects.contains_key(gone),
            "{whose} {gone} is still in the bucket once quiet"
        );
    }

    // 6. Each node in turn, node 3 last, restarts, restores the object its
    // record names and serves group 0 again. No record changes, and every
    // named key exists after each restart.
    for (index, (node_id, _)) in nodes.iter().enumerate() {
        wait_group_leader(&client, &peers[index].1, &peers, &mut children).await;
        let applied = group_zero_applied(&client, &peers[index].1).await;
        drop(children.remove(index));
        children.insert(
            index,
            spawn_node_with_cluster_config_and_cold_s3(binary, &configs[index]),
        );
        wait_until_ready(&client, &peers[index].1, &mut children).await;
        wait_serves_group_zero(&client, &peers[index].1, applied, &mut children).await;
        let restored = check_named_snapshot_objects_exist(&cold_store, &prefix, &nodes).await;
        assert_eq!(restored, names, "node {node_id}'s restart changed a record");
    }
    let quiet = wait_quiet_snapshot_prefixes(&cold_store, &prefix, &nodes).await;
    assert_eq!(quiet, names);

    // 7. Every node reads the whole stream.
    for (_, url) in &peers {
        read_until_matches(
            &client,
            &format!("{url}/owned/snapshots?offset=0&max_bytes=1024"),
            expected.as_bytes(),
        )
        .await;
    }

    drop(children);
    cold_store
        .remove_all("")
        .await
        .unwrap_or_else(|err| panic!("{}", s3_failure("clear the test root", "", &err)));
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

/// Stands in front of node 3's Raft endpoint and preserves HTTP/2 bodies and
/// trailers. It can delay only snapshot transfer. Metrics and readiness use
/// the server's real listeners directly.
struct RaftProxy {
    url: String,
    release: tokio::sync::watch::Sender<bool>,
    blocked: tokio::sync::watch::Receiver<bool>,
    task: tokio::task::JoinHandle<()>,
}

impl RaftProxy {
    async fn start(target: &str) -> Self {
        use tower::ServiceExt;
        // A reserved port: an ephemeral one could be a port `free_port`
        // already handed to a node that has not bound it yet.
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", free_port()))
            .await
            .unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let channel = tonic::transport::Endpoint::from_shared(target.to_owned())
            .unwrap()
            .connect_lazy();
        let (release, released) = tokio::sync::watch::channel(true);
        let (blocked_tx, blocked) = tokio::sync::watch::channel(false);
        let proxy = tower::service_fn(move |request: axum::http::Request<axum::body::Body>| {
            let channel = channel.clone();
            let mut released = released.clone();
            let blocked_tx = blocked_tx.clone();
            async move {
                if request.uri().path() == ursula_raft::RAFT_GRPC_FULL_SNAPSHOT_PATH {
                    if !*released.borrow() {
                        blocked_tx.send_replace(true);
                    }
                    released.wait_for(|ready| *ready).await.unwrap();
                }
                let response = match channel.oneshot(request.map(tonic::body::boxed)).await {
                    Ok(response) => response.map(axum::body::Body::new),
                    Err(_transport) => axum::http::Response::builder()
                        .status(axum::http::StatusCode::BAD_GATEWAY)
                        .body(axum::body::Body::empty())
                        .unwrap(),
                };
                Ok::<_, std::convert::Infallible>(response)
            }
        });
        let task = tokio::spawn(async move {
            axum::serve(listener, axum::Router::new().fallback_service(proxy))
                .await
                .unwrap();
        });
        Self {
            url,
            release,
            blocked,
            task,
        }
    }
}

impl Drop for RaftProxy {
    fn drop(&mut self) {
        self.release.send_replace(true);
        self.task.abort();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cli_voter_replaced_with_an_empty_wal_rejoins_once_its_lost_disk_is_released() {
    // A voter replaced on an empty WAL directory (its disk was lost) must not
    // re-run per-group Initialize for the groups it bootstraps (2 and 5 of 6
    // for node 3): it must never become eligible to vote with nothing applied.
    // The meta group refuses the empty root under the live id until an
    // operator releases the lost one (`ursulactl node replace-disk`). Then
    // the leaders see it lost the entries it had acknowledged and rewind it
    // in place: it stays a voter and catches up with no further operator
    // step.
    let _guard = static_cluster_cli_test_guard().await;
    let Some(binary) = option_env!("CARGO_BIN_EXE_ursula") else {
        tracing::warn!("CARGO_BIN_EXE_ursula is not set; skipping the empty-WAL voter test");
        return;
    };
    let ports = [free_port(), free_port(), free_port()];
    let public = |node_id: u64| format!("http://127.0.0.1:{}", node_port(&ports, node_id));
    let mut snapshot_proxy = RaftProxy::start(&public(3)).await;
    let peers = [1_u64, 2, 3]
        .into_iter()
        .map(|node_id| {
            (
                node_id,
                if node_id == 3 {
                    snapshot_proxy.url.clone()
                } else {
                    public(node_id)
                },
            )
        })
        .collect::<Vec<_>>();

    let wal_root = tempfile::tempdir().expect("WAL root");
    let mut children = Vec::new();
    let mut nodes = Vec::new();
    for node_id in [1_u64, 2, 3] {
        let port = node_port(&ports, node_id);
        let (child, admin_port) =
            spawn_per_group_node(binary, node_id, port, &peers, wal_root.path());
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
    let options = ursula_ctl::quorum::QuorumVerificationOptions {
        group_count: 6,
        timeout: Duration::from_secs(15),
        poll_interval: Duration::from_millis(100),
    };
    ursula_ctl::wait_cluster_ready(
        "after startup",
        &nodes,
        &ctl,
        Duration::from_secs(60),
        Duration::from_millis(100),
        16,
    )
    .await
    .expect("full startup recovery, not listener readiness");
    // Force the wiped replica to catch up through a snapshot. The proxy holds
    // that transfer until we have observed it gated while it is still a voter.
    let before_wipe = ctl.fetch_cluster(&nodes).await.unwrap();
    let lost_root = before_wipe.node(3).unwrap().process_incarnation.root;
    for view in before_wipe.per_node.iter().filter(|view| view.node.id != 3) {
        for group in &view.groups {
            let root = format!(
                "{}/__ursula/raft/{}",
                view.node.admin_url.as_str().trim_end_matches('/'),
                group.raft_group_id
            );
            let response = admin_test_post(&client, format!("{root}/snapshot"))
                .await
                .send()
                .await
                .unwrap();
            assert_eq!(response.status(), reqwest::StatusCode::OK);
            let result: ursula_proto::admin::SnapshotResponse = response.json().await.unwrap();
            let index = result.snapshot_index.unwrap();
            let response = admin_purge(&client, format!("{root}/purge?upto={index}")).await;
            assert_eq!(response.status(), reqwest::StatusCode::OK);
        }
    }
    for index in 0..6 {
        post_until_no_content(
            &client,
            &format!("{}/benchcmp/restart-repair-{index}", public(1)),
            "pre-fault-tail",
        )
        .await;
    }

    ursula_ctl::wait_cluster_ready(
        "before the fault",
        &nodes,
        &ctl,
        Duration::from_secs(60),
        Duration::from_millis(100),
        16,
    )
    .await
    .expect("listener startup alone is not Raft eligibility");

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
    // The replacement keeps the node's listening configuration.
    let replacement_config = children[2].config_path.clone().unwrap();
    let replacement_text = std::fs::read_to_string(&replacement_config).unwrap();
    drop(children.pop());
    let survivors = ursula_ctl::quorum::verify_surviving_quorum(&nodes, 3, &ctl, &options)
        .await
        .expect("with the drained source stopped, both survivors are eligible");
    assert!(!survivors.full_redundancy_restored);
    // The replacement comes back on an empty WAL: the source's disk is lost.
    snapshot_proxy.release.send_replace(false);
    remove_test_path(node_wal_dir(wal_root.path(), 3));
    std::fs::write(&replacement_config, replacement_text).unwrap();
    let mut command = Command::new(binary);
    command
        .arg("server")
        .arg("--config")
        .arg(&replacement_config);
    let mut child = spawn_child(command, format!("replacement-node-3-{}", ports[2]));
    child.config_path = Some(replacement_config);
    children.push(child);
    wait_until_ready(&client, &public(3), &mut children).await;
    // The empty disk is refused while the lost one is bound, and binds once
    // it is released.
    wait_root_refused(&client, &public(3), &mut children).await;
    replace_lost_disk(&nodes, 3, lost_root).await;

    tokio::time::timeout(
        Duration::from_secs(60),
        snapshot_proxy.blocked.wait_for(|blocked| *blocked),
    )
    .await
    .expect("wiped voter must request the compacted snapshot")
    .unwrap();

    // Observe the actual empty-WAL recovery before allowing another maintenance
    // operation: the wiped node stays a voter of every group, its gates stay
    // closed while it cannot catch up, and the survivors serve.
    let readiness_client = ursula_ctl::MetricsClient::new(Duration::from_secs(5)).unwrap();
    let began = std::time::Instant::now();
    loop {
        let snapshot = readiness_client.fetch_cluster(&nodes).await.unwrap();
        let mut serving = Vec::new();
        for node in &nodes {
            let response = client
                .get(format!("{}/__ursula/ready", public(node.id)))
                .send()
                .await
                .unwrap();
            let status = response.status();
            let report: ursula_proto::admin::ServingReadiness = response.json().await.unwrap();
            serving.push((status, report));
        }
        // No heal step removes a voter that lost its log.
        for view in snapshot.per_node.iter().filter(|view| view.node.id != 3) {
            for group in &view.groups {
                assert!(
                    group.voter_ids.contains(&3) && group.learner_ids.is_empty(),
                    "node {} demoted the wiped voter in group {}: {snapshot:?}",
                    view.node.id,
                    group.raft_group_id
                );
            }
        }
        let target = snapshot.node(3).unwrap();
        let target_gated = (0..6_u64).all(|group_id| {
            target.group(group_id).is_some_and(|group| {
                group.recovery_gate != ursula_proto::admin::RecoveryGateStatus::Open
            })
        });
        let target_unready = target
            .raft_maintenance
            .as_ref()
            .is_some_and(|report| !report.ready());
        if target_gated
            && target_unready
            && serving[0].0 == reqwest::StatusCode::OK
            && serving[1].0 == reqwest::StatusCode::OK
        {
            assert_eq!(serving[2].0, reqwest::StatusCode::SERVICE_UNAVAILABLE);
            assert!(matches!(
                serving[2].1.reason,
                Some(
                    ursula_proto::admin::ServingReadinessReason::RaftReplicaUnready
                        | ursula_proto::admin::ServingReadinessReason::RecoveryGateClosed
                        | ursula_proto::admin::ServingReadinessReason::RootRefused
                        | ursula_proto::admin::ServingReadinessReason::PredecessorWait
                )
            ));
            // The gated replica's own report blocks another maintenance.
            assert!(matches!(
                ursula_ctl::observe::check_maintenance_snapshot(&snapshot, 3, 6),
                Err(ursula_ctl::observe::ReadinessRefusal::Maintenance { .. })
            ));
            let refusal = ursula_ctl::wait_ready(
                &readiness_client,
                &nodes,
                6,
                Duration::ZERO,
                Duration::from_millis(10),
            )
            .await
            .unwrap_err();
            assert!(matches!(
                refusal.downcast_ref::<ursula_ctl::observe::ReadinessRefusal>(),
                Some(ursula_ctl::observe::ReadinessRefusal::Maintenance { .. })
            ));
            break;
        }
        assert!(
            began.elapsed() < Duration::from_secs(30),
            "did not observe a gated empty-WAL voter with serving survivors: {snapshot:?}, {serving:?}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    snapshot_proxy.release.send_replace(true);

    let stale_clear = ctl
        .set_maintenance_drain(&nodes[2], false)
        .await
        .expect_err("the old process incarnation is refused");
    assert!(stale_clear.to_string().contains("412"), "{stale_clear}");
    assert!(
        ctl.fetch_node(&nodes[2]).await.is_err(),
        "the old operation must not refresh its identity"
    );
    let ctl = ursula_ctl::MetricsClient::new(Duration::from_secs(5))
        .expect("a new operation observes the replacement");

    // Snapshot metadata can restore historical voter membership before apply.
    // Such a replica must still have its actual vote gate closed. Check both
    // fields in one observation, since recovery can advance between HTTP reads.
    // The serving endpoint was independently checked while snapshots were held.
    let deadline = std::time::Instant::now()
        .checked_add(Duration::from_secs(60))
        .unwrap();
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
                group.voter_ids.contains(&3)
                    && group.last_applied_index.is_none()
                    && group.maintenance.recovery_ready
            }) && peer_committed.is_some_and(|committed| committed > 0);
            assert!(
                !empty_voter,
                "node 3 opened its vote gate as an empty voter of group {group_id}: {snapshot:?}"
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

    // `wait` sees the same caught-up voter.
    let outcome =
        ursula_ctl::wait_node_ready(&nodes, &nodes[2], &ctl, &ursula_ctl::CatchUpOptions {
            stall_timeout: Duration::from_secs(30),
            ready_timeout: Duration::from_secs(60),
            poll_interval: Duration::from_millis(200),
            lag_tolerance: 0,
        })
        .await
        .expect("wait for node 3");
    assert!(
        matches!(outcome, ursula_ctl::CatchUpOutcome::Ready),
        "{outcome:?}"
    );

    for index in 0..6 {
        read_until_matches(
            &client,
            &format!(
                "{}/benchcmp/restart-repair-{index}?offset=0&max_bytes=64",
                public(3)
            ),
            b"pre-fault-tail",
        )
        .await;
    }

    // The rejoined voter takes leaderships back. The proof needs every
    // group's leader known on every node, so it waits for that to settle.
    wait_leadership_balanced(&ctl, &nodes, 6).await;
    let proof = ursula_ctl::quorum::verify_quorum(&nodes, &ctl, &options)
        .await
        .expect("current process full prefix proof");
    assert_eq!(proof.process_incarnations.len(), nodes.len());
    drop(children);
}

/// Waits until the leadership balancer has nothing left to move: in two
/// samples in a row every node names the same leader for each of the
/// `group_count` groups, and no node leads more than its fair share. Each
/// handoff to a rejoined voter is an election that leaves the group without
/// a known leader for a moment.
async fn wait_leadership_balanced(
    ctl: &ursula_ctl::MetricsClient,
    nodes: &[ursula_ctl::NodeInfo],
    group_count: u64,
) {
    let fair = group_count.div_ceil(u64::try_from(nodes.len()).unwrap());
    let deadline = std::time::Instant::now()
        .checked_add(Duration::from_secs(60))
        .unwrap();
    let mut streak = 0_u32;
    loop {
        let snapshot = ctl.fetch_cluster(nodes).await.expect("fetch cluster");
        let mut led = BTreeMap::<u64, u64>::new();
        let agreed = (0..group_count).all(|group_id| {
            let leaders = snapshot
                .per_node
                .iter()
                .map(|view| view.group(group_id).and_then(|group| group.current_leader))
                .collect::<BTreeSet<_>>();
            match leaders.into_iter().collect::<Vec<_>>().as_slice() {
                [Some(leader)] => {
                    let count = led.entry(*leader).or_default();
                    *count = count.saturating_add(1);
                    true
                }
                _ => false,
            }
        });
        let balanced = agreed
            && snapshot.per_node.len() == nodes.len()
            && led.values().all(|count| *count <= fair);
        streak = if balanced {
            streak.saturating_add(1)
        } else {
            0
        };
        if streak >= 2 {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "leadership never settled at {fair} groups per node: {led:?}, {snapshot:?}"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// Waits until the node at `base_url`, started on an empty disk under its
/// live id, reports `root_refused`: the meta group binds the id to the lost
/// root until an operator releases it.
async fn wait_root_refused(client: &reqwest::Client, base_url: &str, children: &mut [ChildGuard]) {
    let deadline = std::time::Instant::now()
        .checked_add(Duration::from_secs(60))
        .unwrap();
    let mut last = String::from("no attempts made");
    loop {
        if let Some(report) = exited_child_report(children) {
            panic!("node {base_url} exited before its root was refused: {last}\n{report}");
        }
        match client
            .get(format!("{base_url}/__ursula/ready"))
            .send()
            .await
        {
            Ok(response) => {
                let status = response.status();
                match response
                    .json::<ursula_proto::admin::ServingReadiness>()
                    .await
                {
                    Ok(ready)
                        if ready.reason
                            == Some(ursula_proto::admin::ServingReadinessReason::RootRefused) =>
                    {
                        assert_eq!(status, reqwest::StatusCode::SERVICE_UNAVAILABLE);
                        return;
                    }
                    Ok(ready) => last = format!("{status}: {ready:?}"),
                    Err(error) => last = format!("{status}: {error}"),
                }
            }
            Err(error) => last = error.to_string(),
        }
        assert!(
            std::time::Instant::now() < deadline,
            "node {base_url} never reported root_refused: {last}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

fn spawn_per_group_node(
    binary: &str,
    node_id: u64,
    port: u16,
    peers: &[(u64, String)],
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
        &node_wal_dir(wal_root, node_id),
        "memory",
        None,
    );
    let mut command = Command::new(binary);
    command.arg("server").arg("--config").arg(&config_path);
    command.env("RUST_LOG", "info");
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

/// The data groups of every meta test cluster.
const META_TEST_GROUPS: usize = 4;
/// How long a meta test waits for one condition.
const META_WAIT: Duration = Duration::from_secs(120);
/// The meta voters every meta test configures.
const META_VOTERS: &[u64] = &[1, 2, 3];

/// One node of a meta test cluster. Its ports, config file and WAL
/// directory stay the same across its restarts.
struct MetaNode {
    id: u64,
    port: u16,
    admin_port: u16,
    config: PathBuf,
    wal: PathBuf,
    /// The cold-store root of an S3-backed node, `None` for memory.
    cold_root: Option<String>,
}

impl MetaNode {
    /// Nodes 1 to `count`, with their files under `root`.
    fn cluster(root: &Path, count: u64) -> Vec<Self> {
        (1..=count)
            .map(|id| Self {
                id,
                port: free_port(),
                admin_port: free_port(),
                config: root.join(format!("node-{id}.toml")),
                wal: root.join(format!("node-{id}-log")),
                cold_root: None,
            })
            .collect()
    }

    fn url(&self) -> String {
        format!("http://127.0.0.1:{}", self.port)
    }

    fn admin(&self) -> String {
        format!("http://127.0.0.1:{}", self.admin_port)
    }

    fn peer(&self) -> (u64, String) {
        (self.id, self.url())
    }

    /// The meta journal's directory, `raft-log/meta/` under the WAL root.
    fn meta_dir(&self) -> PathBuf {
        self.wal.join("raft-log").join("meta")
    }

    fn ctl(&self) -> ursula_ctl::NodeInfo {
        ctl_node(self.id, self.admin_port, &self.url())
    }

    /// Writes the node's config with `peers` as its `raft.peers`. Each data
    /// group's seed initializer initializes it.
    fn write(&self, peers: &[(u64, String)], meta: MetaToml<'_>) {
        NodeToml {
            port: self.port,
            admin_port: self.admin_port,
            node_id: self.id,
            raft_group_count: META_TEST_GROUPS,
            peers,
            wal_path: &self.wal,
            cold_backend: if self.cold_root.is_some() {
                "s3"
            } else {
                "memory"
            },
            cold_root: self.cold_root.as_deref(),
            meta,
        }
        .write(&self.config);
    }

    fn spawn(&self, binary: &str) -> ChildGuard {
        if self.cold_root.is_some() {
            spawn_node_with_cluster_config_and_cold_s3(binary, &self.config)
        } else {
            spawn_node_with_cluster_config(binary, &self.config)
        }
    }
}

fn peers_of(nodes: &[MetaNode]) -> Vec<(u64, String)> {
    nodes.iter().map(MetaNode::peer).collect()
}

/// The running processes of a meta test cluster, by node id.
#[derive(Default)]
struct Processes(BTreeMap<u64, ChildGuard>);

impl Processes {
    fn start(&mut self, binary: &str, node: &MetaNode) {
        let running = self.0.insert(node.id, node.spawn(binary));
        assert!(running.is_none(), "node {} already runs", node.id);
    }

    /// Stops node `id` by SIGTERM and waits for its clean exit.
    async fn stop(&mut self, id: u64) {
        let mut child = self.0.remove(&id).expect("the node runs");
        sigterm_and_wait_for_clean_exit(&mut child).await;
    }

    /// Kills node `id`: it hands nothing off.
    fn kill(&mut self, id: u64) {
        drop(self.0.remove(&id).expect("the node runs"));
    }

    fn exited(&mut self) -> Option<String> {
        self.0.values_mut().find_map(exited_report)
    }

    fn reports(&self) -> String {
        self.0
            .values()
            .map(child_report)
            .collect::<Vec<_>>()
            .join("\n---\n")
    }
}

/// Polls `probe` until it answers `Ok`, failing once a node of `procs`
/// exited or after [`META_WAIT`], with the probe's last `Err`.
async fn eventually<T>(
    procs: &mut Processes,
    what: &str,
    mut probe: impl AsyncFnMut() -> Result<T, String>,
) -> T {
    let deadline = std::time::Instant::now().checked_add(META_WAIT).unwrap();
    loop {
        if let Some(report) = procs.exited() {
            panic!("a node exited while waiting until {what}:\n{report}");
        }
        let last = match probe().await {
            Ok(found) => return found,
            Err(last) => last,
        };
        assert!(
            std::time::Instant::now() < deadline,
            "never {what}: {last}\n{}",
            procs.reports()
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// Node `node`'s placement status from its client listener.
async fn placement_status(
    client: &reqwest::Client,
    node: &MetaNode,
) -> Result<PlacementStatus, String> {
    let url = format!("{}/__ursula/placement", node.url());
    let response = client
        .get(&url)
        .send()
        .await
        .map_err(|err| format!("GET {url}: {err}"))?;
    if response.status() != reqwest::StatusCode::OK {
        return Err(format!("GET {url}: {}", response.status()));
    }
    Ok(response.json().await.expect("placement status JSON"))
}

/// Node `node`'s placement status confirmed by a meta read barrier, on its
/// admin listener.
async fn confirmed_placement_status(
    client: &reqwest::Client,
    node: &MetaNode,
) -> Result<PlacementStatus, String> {
    let url = format!("{}/__ursula/placement?confirmed=true", node.admin());
    let response = client
        .get(&url)
        .send()
        .await
        .map_err(|err| format!("GET {url}: {err}"))?;
    if response.status() != reqwest::StatusCode::OK {
        let status = response.status();
        let refusal: ursula_control::PlacementApiError =
            response.json().await.expect("placement refusal JSON");
        return Err(format!("GET {url}: {status} {refusal:?}"));
    }
    Ok(response.json().await.expect("placement status JSON"))
}

/// Waits until node `node`'s placement status satisfies `done`, and
/// returns it.
async fn wait_placement(
    client: &reqwest::Client,
    procs: &mut Processes,
    node: &MetaNode,
    what: &str,
    done: impl Fn(&PlacementStatus) -> bool,
) -> PlacementStatus {
    let what = format!("node {} {what}", node.id);
    eventually(procs, &what, async || {
        let status = placement_status(client, node).await?;
        if done(&status) {
            Ok(status)
        } else {
            Err(format!("{status:?}"))
        }
    })
    .await
}

/// Waits until every node of `nodes` shows the bootstrapped cluster and
/// knows a meta leader, and returns their statuses.
async fn wait_bootstrapped(
    client: &reqwest::Client,
    procs: &mut Processes,
    nodes: &[MetaNode],
) -> Vec<PlacementStatus> {
    let mut statuses = Vec::new();
    for node in nodes {
        let status = wait_placement(
            client,
            procs,
            node,
            "shows the bootstrapped cluster",
            |status| bootstrapped(status).is_some() && meta_leader(status).is_some(),
        )
        .await;
        statuses.push(status);
    }
    statuses
}

fn bootstrapped(status: &PlacementStatus) -> Option<&Cluster> {
    match &status.view {
        ClusterView::Bootstrapped(cluster) => Some(cluster),
        _ => None,
    }
}

/// The bootstrapped cluster once every group's leader reported its
/// voters: every group `Live` and not moving. Nothing changes a placement
/// after that until a move, so views read then compare.
fn settled(status: &PlacementStatus) -> Option<&Cluster> {
    bootstrapped(status).filter(|cluster| {
        cluster.groups().iter().all(|(_, placement)| {
            matches!(placement, GroupPlacement::Live { .. }) && placement.moving().is_none()
        })
    })
}

/// [`wait_bootstrapped`], until every node's view is [`settled`].
async fn wait_settled(
    client: &reqwest::Client,
    procs: &mut Processes,
    nodes: &[MetaNode],
) -> Vec<PlacementStatus> {
    let mut statuses = Vec::new();
    for node in nodes {
        let status = wait_placement(client, procs, node, "shows every group live", |status| {
            settled(status).is_some() && meta_leader(status).is_some()
        })
        .await;
        statuses.push(status);
    }
    statuses
}

/// The meta leader a node's running meta replica knows.
fn meta_leader(status: &PlacementStatus) -> Option<u64> {
    match &status.meta {
        MetaReplicaStatus::Running { leader, .. } => leader.map(NodeId::get),
        MetaReplicaStatus::Damaged { .. } | MetaReplicaStatus::Absent => None,
    }
}

fn meta_role(status: &PlacementStatus) -> Option<MetaRole> {
    match &status.meta {
        MetaReplicaStatus::Running { role, .. } => Some(*role),
        MetaReplicaStatus::Damaged { .. } | MetaReplicaStatus::Absent => None,
    }
}

/// Every group's placement key.
fn sinces(cluster: &Cluster) -> BTreeMap<RaftGroupId, PlacementEpoch> {
    cluster
        .groups()
        .iter()
        .map(|(group, placement)| (group, placement.since()))
        .collect()
}

fn node_ids(ids: impl Iterator<Item = NodeId>) -> BTreeSet<u64> {
    ids.map(NodeId::get).collect()
}

/// How many data groups node `node_id` leads with a live quorum, by its
/// own metrics.
fn leads(metrics: &ursula_proto::admin::NodeMetrics, node_id: u64) -> usize {
    metrics
        .groups
        .iter()
        .filter(|group| group.current_leader == Some(node_id) && group.leads_live)
        .count()
}

/// The metrics of the node at `base_url`, or why they did not answer,
/// for a poll that may start before the node listens.
async fn try_node_metrics(
    client: &reqwest::Client,
    base_url: &str,
) -> Result<ursula_proto::admin::NodeMetrics, String> {
    let url = format!("{base_url}/__ursula/metrics");
    let response = client
        .get(&url)
        .send()
        .await
        .map_err(|err| format!("GET {url}: {err}"))?;
    if response.status() != reqwest::StatusCode::OK {
        return Err(format!("GET {url}: {}", response.status()));
    }
    response
        .json()
        .await
        .map_err(|err| format!("GET {url}: {err}"))
}

/// The metrics of `node` once it runs a replica of every group, as its
/// hosting driver opens each group the cluster places on it.
async fn placed_replicas(
    client: &reqwest::Client,
    procs: &mut Processes,
    node: &MetaNode,
) -> ursula_proto::admin::NodeMetrics {
    let what = format!("node {} runs its placed replicas", node.id);
    eventually(procs, &what, async || {
        let metrics = try_node_metrics(client, &node.url()).await?;
        if metrics.groups.len() == META_TEST_GROUPS {
            Ok(metrics)
        } else {
            Err(format!("{:?}", metrics.groups))
        }
    })
    .await
}

/// Asserts that every data replica node `node` reports, if it runs any,
/// took part in nothing: no vote floor (term 0, where a barrier would adopt
/// its leader's), no entry and a closed recovery gate.
fn assert_data_held(metrics: &ursula_proto::admin::NodeMetrics, node: u64) {
    for group in &metrics.groups {
        assert_eq!(
            group.current_term, 0,
            "node {node}, no vote floor: {group:?}"
        );
        assert_eq!(
            group.last_log_index, None,
            "node {node}, no entry: {group:?}"
        );
        assert_ne!(
            group.recovery_gate,
            ursula_proto::admin::RecoveryGateStatus::Open,
            "node {node}, a closed gate: {group:?}"
        );
    }
}

/// Node `node`'s participation, from its cluster listener.
async fn leadership_shed(
    client: &reqwest::Client,
    node: &MetaNode,
) -> ursula_proto::admin::LeadershipShedStatus {
    client
        .get(format!("{}/__ursula/leadership-shed", node.url()))
        .send()
        .await
        .expect("leadership-shed status")
        .json()
        .await
        .expect("leadership-shed JSON")
}

fn sheds_placement_unavailable(shed: &ursula_proto::admin::LeadershipShedStatus) -> bool {
    shed.state
        .split('|')
        .any(|reason| reason == ursula_raft::LeadershipShedReason::PlacementUnavailable.as_str())
}

/// S1 (#768): three nodes form the meta group over gRPC and bootstrap the
/// cluster while the data groups form and serve. Each node is a meta voter
/// of one bootstrapped cluster that seeds every group. Each group's leader
/// reports the voters it formed on, so every group turns live on its seed
/// voters (S3, design 5.7), and a follower's view confirmed by a meta read
/// barrier, which the meta leader answers over gRPC, is that cluster. `ursulactl drain` of the meta leader hands
/// its meta leadership away with its data leaderships. A full restart keeps
/// every placement's `since`, and each node's registration is then
/// answered by the meta leader, through a forward on the followers.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cli_three_nodes_form_meta_and_keep_serving() {
    let _guard = static_cluster_cli_test_guard().await;
    let Some(binary) = option_env!("CARGO_BIN_EXE_ursula") else {
        tracing::warn!("CARGO_BIN_EXE_ursula is not set; skipping the meta test");
        return;
    };
    let root = tempfile::tempdir().expect("temp root");
    let nodes = MetaNode::cluster(root.path(), 3);
    let peers = peers_of(&nodes);
    let meta = MetaToml {
        meta_voters: Some(META_VOTERS),
        ..MetaToml::default()
    };
    let client = reqwest::Client::new();
    let mut procs = Processes::default();
    for node in &nodes {
        node.write(&peers, meta);
        procs.start(binary, node);
    }

    // 1. One bootstrapped cluster on every node, every node a meta voter.
    // Right after the bootstrap each group is seeded, or its leader already
    // reported its voters. Once every leader reported, every group is live
    // on the voters it was seeded on, and every node's view is the same.
    let formed = wait_bootstrapped(&client, &mut procs, &nodes).await;
    let seeded = bootstrapped(&formed[0]).expect("bootstrapped").clone();
    assert!(
        seeded
            .groups()
            .iter()
            .all(|(_, placement)| placement.moving().is_none()),
        "{seeded:?}"
    );
    let formed = wait_settled(&client, &mut procs, &nodes).await;
    let cluster = settled(&formed[0]).expect("settled").clone();
    assert_eq!(
        node_ids(cluster.params().meta_voters.iter()),
        BTreeSet::from([1, 2, 3])
    );
    assert_eq!(
        node_ids(cluster.nodes().keys().copied()),
        BTreeSet::from([1, 2, 3])
    );
    assert_eq!(cluster.groups().len(), META_TEST_GROUPS);
    for (group, placement) in seeded.groups().iter() {
        if let GroupPlacement::Seeded { voters, .. } = placement {
            assert_eq!(
                cluster.group(group).map(GroupPlacement::current_voters),
                Some(voters),
                "{cluster:?}"
            );
        }
    }
    for status in &formed {
        assert_eq!(bootstrapped(status), Some(&cluster), "{status:?}");
        assert_eq!(meta_role(status), Some(MetaRole::Voter), "{status:?}");
        assert!(
            matches!(status.registration, RegistrationStatus::Answered(_)),
            "{status:?}"
        );
        assert_eq!(status.last_bootstrap_refusal, None, "{status:?}");
    }
    for node in &nodes {
        let what = format!("node {} confirms its view", node.id);
        let confirmed = eventually(&mut procs, &what, async || {
            confirmed_placement_status(&client, node).await
        })
        .await;
        assert_eq!(bootstrapped(&confirmed), Some(&cluster), "{confirmed:?}");
    }

    // 2. The data groups serve.
    let stream = format!("{}/meta-cli/serving", nodes[1].url());
    put_until_created(&client, &stream).await;
    post_until_no_content(&client, &stream, "served-with-meta;").await;
    let read = |node: &MetaNode| format!("{}/meta-cli/serving?offset=0&max_bytes=64", node.url());
    read_until_matches(&client, &read(&nodes[2]), b"served-with-meta;").await;

    // 3. Draining the meta leader moves meta's leadership off it.
    let ctl = ursula_ctl::MetricsClient::new(Duration::from_secs(5)).expect("ctl client");
    let ctl_nodes = nodes.iter().map(MetaNode::ctl).collect::<Vec<_>>();
    let before = ctl.fetch_cluster(&ctl_nodes).await.expect("metrics");
    let target = &before
        .per_node
        .iter()
        .find(|view| view.leads_meta())
        .expect("a node leads meta")
        .node;
    let leader = target.id;
    let outcome = ursula_ctl::drain_node(&ctl_nodes, target, &ctl, &ursula_ctl::DrainOptions {
        drain_timeout: Duration::from_secs(60),
        ready_timeout: Duration::from_secs(60),
        poll_interval: Duration::from_millis(200),
        lag_tolerance: 16,
        dry_run: false,
    })
    .await
    .expect("drain the meta leader");
    assert!(
        matches!(outcome, ursula_ctl::DrainOutcome::Drained),
        "{outcome:?}"
    );
    let drained = ctl.fetch_cluster(&ctl_nodes).await.expect("metrics");
    assert!(!drained.meta_led_by(leader));
    assert!(drained.groups_reported_led_by(leader).is_empty());
    let other = nodes.iter().find(|node| node.id != leader).unwrap();
    wait_placement(
        &client,
        &mut procs,
        other,
        "names another meta leader",
        |status| meta_leader(status).is_some_and(|new| new != leader),
    )
    .await;
    ursula_ctl::undrain_node(&ctl, target)
        .await
        .expect("undrain the node");

    // 4. A full restart keeps every placement's key, and every node's
    // registration is answered as a node with roles.
    for node in &nodes {
        procs.stop(node.id).await;
    }
    for node in &nodes {
        procs.start(binary, node);
    }
    for status in wait_settled(&client, &mut procs, &nodes).await {
        let restarted = bootstrapped(&status).unwrap();
        assert_eq!(sinces(restarted), sinces(&cluster), "{status:?}");
        assert_eq!(restarted.params(), cluster.params());
    }
    for node in &nodes {
        wait_placement(
            &client,
            &mut procs,
            node,
            "registers with its roles",
            |status| {
                matches!(
                    status.registration,
                    RegistrationStatus::Answered(Registered::Referenced(_))
                )
            },
        )
        .await;
    }
    let what = "the stream reads back after the restart";
    eventually(&mut procs, what, async || {
        let body = client
            .get(read(&nodes[0]))
            .send()
            .await
            .map_err(|err| err.to_string())?
            .bytes()
            .await
            .map_err(|err| err.to_string())?;
        if body.as_ref() == b"served-with-meta;" {
            Ok(())
        } else {
            Err(format!("{body:?}"))
        }
    })
    .await;
}

/// Integration 11's meta half (#768): five nodes with meta voters 1 to 3.
/// Nodes 1 to 3 form meta and bootstrap over the `raft.peers` they list.
/// Nodes 4 and 5 start before those `raft.peers` list them: they register,
/// and the meta group records them, while no meta voter can dial them yet.
/// Once nodes 1 to 3 restart one at a time with the full `raft.peers`, the
/// meta leader replicates to nodes 4 and 5 as meta learners, and their
/// views reach the bootstrapped cluster. Placement decides what each node
/// hosts: every group stays on the genesis peers it was seeded on, though
/// `raft.peers` grew, and nodes 4 and 5, in no group's seeds, host nothing
/// and route a write to a node that hosts its group, naming each of its
/// voters in turn.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cli_five_nodes_with_meta_voters_one_to_three_bootstrap_and_late_nodes_learn() {
    let _guard = static_cluster_cli_test_guard().await;
    let Some(binary) = option_env!("CARGO_BIN_EXE_ursula") else {
        tracing::warn!("CARGO_BIN_EXE_ursula is not set; skipping the meta test");
        return;
    };
    let root = tempfile::tempdir().expect("temp root");
    let nodes = MetaNode::cluster(root.path(), 5);
    let all_peers = peers_of(&nodes);
    let old_peers = peers_of(&nodes[..3]);
    let meta = MetaToml {
        meta_voters: Some(META_VOTERS),
        ..MetaToml::default()
    };
    let client = reqwest::Client::new();
    let mut procs = Processes::default();
    for node in &nodes[..3] {
        node.write(&old_peers, meta);
        procs.start(binary, node);
    }
    let formed = wait_settled(&client, &mut procs, &nodes[..3]).await;
    let cluster = settled(&formed[0]).expect("settled").clone();
    assert_eq!(
        node_ids(cluster.nodes().keys().copied()),
        BTreeSet::from([1, 2, 3])
    );

    // 1. The late nodes register, and the meta group records them.
    for node in &nodes[3..] {
        node.write(&all_peers, meta);
        procs.start(binary, node);
    }
    for node in &nodes[3..] {
        wait_placement(
            &client,
            &mut procs,
            node,
            "registers without a role",
            |status| {
                matches!(
                    status.registration,
                    RegistrationStatus::Answered(Registered::Unreferenced(_))
                )
            },
        )
        .await;
    }
    wait_placement(
        &client,
        &mut procs,
        &nodes[0],
        "records nodes 4 and 5",
        |status| {
            bootstrapped(status).is_some_and(|cluster| {
                node_ids(cluster.nodes().keys().copied()) == BTreeSet::from([1, 2, 3, 4, 5])
            })
        },
    )
    .await;

    // 2. Nodes 1 to 3 restart one at a time with the full `raft.peers`.
    for node in nodes[..3].iter().rev() {
        procs.stop(node.id).await;
        node.write(&all_peers, meta);
        procs.start(binary, node);
        wait_placement(
            &client,
            &mut procs,
            node,
            "rejoins as a meta voter",
            |status| {
                bootstrapped(status).is_some()
                    && meta_role(status) == Some(MetaRole::Voter)
                    && meta_leader(status).is_some()
            },
        )
        .await;
    }

    // 3. Nodes 4 and 5 learn the cluster as meta learners.
    for node in &nodes[3..] {
        let status = wait_placement(
            &client,
            &mut procs,
            node,
            "learns as a meta learner",
            |status| settled(status).is_some() && meta_role(status) == Some(MetaRole::Learner),
        )
        .await;
        let learned = settled(&status).unwrap();
        assert_eq!(learned.params(), cluster.params());
        assert_eq!(sinces(learned), sinces(&cluster));
    }
    // Meta's membership keeps its voters and adds the late nodes as learners.
    let meta_metrics = node_metrics(&client, &nodes[0].url())
        .await
        .meta
        .expect("node 1 runs a meta replica");
    assert_eq!(
        meta_metrics.voter_configs,
        vec![BTreeSet::from([1, 2, 3])],
        "{meta_metrics:?}"
    );
    assert!(
        [4, 5]
            .iter()
            .all(|id| meta_metrics.learner_ids.contains(id)),
        "{meta_metrics:?}"
    );

    // 4. Nodes 1 to 3 run every group, and the late nodes none. A seed
    // rule read over the grown `raft.peers` would place groups on nodes 4
    // and 5 instead.
    for node in &nodes[..3] {
        placed_replicas(&client, &mut procs, node).await;
    }
    for node in &nodes[3..] {
        let metrics = node_metrics(&client, &node.url()).await;
        assert!(metrics.groups.is_empty(), "node {}: {metrics:?}", node.id);
    }
    // A write to a late node redirects to a node its view places the
    // group on, typed as `NotHosted`, and lands there.
    let no_redirect = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .expect("client without redirects");
    let stream = format!("{}/meta-cli/late-node", nodes[3].url());
    let redirect = eventually(&mut procs, "node 4 redirects a write", async || {
        let response = no_redirect
            .put(&stream)
            .header("content-type", "text/plain")
            .send()
            .await
            .map_err(|err| format!("PUT {stream}: {err}"))?;
        if response.status() == reqwest::StatusCode::TEMPORARY_REDIRECT {
            Ok(response)
        } else {
            Err(format!("PUT {stream}: {}", response.status()))
        }
    })
    .await;
    let routing = ursula_headers::RoutingAnswer::parse(
        redirect.status().as_u16(),
        redirect
            .headers()
            .iter()
            .map(|(name, value)| (name.as_str(), value.as_bytes())),
    );
    assert!(
        matches!(
            routing,
            Ok(Some(ursula_headers::RoutingAnswer::NotHosted { .. }))
        ),
        "{routing:?}"
    );
    // Each answer names the next seed voter, never the other late node: the
    // hint takes the first tier of the leader candidates in node 4's view,
    // round robin, so three answers name the three seed voters.
    let mut answers = vec![redirect];
    for _ in 1..3 {
        answers.push(
            no_redirect
                .put(&stream)
                .header("content-type", "text/plain")
                .send()
                .await
                .expect("PUT to node 4"),
        );
    }
    let named: BTreeSet<u64> = answers
        .iter()
        .map(|answer| {
            assert_eq!(answer.status(), reqwest::StatusCode::TEMPORARY_REDIRECT);
            let location = answer.headers()["location"].to_str().expect("a location");
            nodes[..3]
                .iter()
                .find(|node| location == format!("{}/meta-cli/late-node", node.url()))
                .unwrap_or_else(|| panic!("the redirect names a seeded node: {location}"))
                .id
        })
        .collect();
    assert_eq!(named, BTreeSet::from([1, 2, 3]));
    put_until_created(&client, &stream).await;
    let metrics = node_metrics(&client, &nodes[3].url()).await;
    assert!(metrics.groups.is_empty(), "node 4: {metrics:?}");
}

/// Placement-driven hosting where the nodes outnumber the replication
/// factor (#768, 4.1 to 4.3): four nodes at replication factor 3 each host
/// the three groups whose seed voters list them, and no other. A node's own
/// opens hold its one core's journal while it plans, so a plan may read the
/// core's other groups as unknown and try to open the group it is not
/// placed in: that open fails, and the node never runs the group. After a
/// restart, a node opens what its journal holds and only that.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cli_four_nodes_host_only_the_groups_meta_seeds_on_them() {
    let _guard = static_cluster_cli_test_guard().await;
    let Some(binary) = option_env!("CARGO_BIN_EXE_ursula") else {
        tracing::warn!("CARGO_BIN_EXE_ursula is not set; skipping the meta test");
        return;
    };
    let root = tempfile::tempdir().expect("temp root");
    let nodes = MetaNode::cluster(root.path(), 4);
    let peers = peers_of(&nodes);
    let meta = MetaToml {
        meta_voters: Some(META_VOTERS),
        ..MetaToml::default()
    };
    let client = reqwest::Client::new();
    let mut procs = Processes::default();
    for node in &nodes {
        node.write(&peers, meta);
        procs.start(binary, node);
    }
    let formed = wait_bootstrapped(&client, &mut procs, &nodes).await;
    let cluster = bootstrapped(&formed[0]).expect("bootstrapped").clone();
    let seeded = |node: &MetaNode| -> BTreeSet<u64> {
        cluster
            .groups()
            .iter()
            .filter(|(_, placement)| placement.current_voters().contains(NodeId::new(node.id)))
            .map(|(group, _)| u64::from(group.0))
            .collect()
    };
    for node in &nodes {
        assert_eq!(seeded(node).len(), 3, "node {}: {cluster:?}", node.id);
        runs_exactly(&client, &mut procs, node, &seeded(node)).await;
    }
    let stream = format!("{}/meta-cli/four-nodes", nodes[0].url());
    put_until_created(&client, &stream).await;
    post_until_no_content(&client, &stream, "placed;").await;

    procs.stop(1).await;
    procs.start(binary, &nodes[0]);
    runs_exactly(&client, &mut procs, &nodes[0], &seeded(&nodes[0])).await;
    let read = format!(
        "{}/meta-cli/four-nodes?offset=0&max_bytes=64",
        nodes[0].url()
    );
    read_until_matches(&client, &read, b"placed;").await;
}

/// S2(n) (#768): at replication factor 3 each of five nodes hosts only
/// some of the groups, and the node-wide passes and the cluster-wide admin
/// answers still cover every group. One node's purge removes the bucket
/// from every group, through the leaders of the groups it hosts no replica
/// of. The nodes' usage reports, kept per group by the newest replica,
/// count every stream once. `backup-create` exports every group, and its
/// restore into the wiped cluster imports every group. Each group's leader
/// flushes its hot bytes to the cold tier, though its node's cold flush
/// pass meets groups the node does not host.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cli_five_nodes_at_rf_three_cover_every_group_in_node_wide_passes() {
    let _guard = static_cluster_cli_test_guard().await;
    let Some(binary) = option_env!("CARGO_BIN_EXE_ursula") else {
        tracing::warn!("CARGO_BIN_EXE_ursula is not set; skipping the meta test");
        return;
    };
    let root = tempfile::tempdir().expect("temp root");
    let nodes = MetaNode::cluster(root.path(), 5);
    let peers = peers_of(&nodes);
    let mut meta = MetaToml {
        meta_voters: Some(META_VOTERS),
        replication_factor: Some(3),
        ..MetaToml::default()
    };
    let client = reqwest::Client::new();
    let mut procs = Processes::default();
    for node in &nodes {
        node.write(&peers, meta);
        procs.start(binary, node);
    }
    let settled_view = wait_settled(&client, &mut procs, &nodes).await;
    let cluster = settled(&settled_view[0]).expect("settled").clone();
    let hosted = |node: &MetaNode| -> BTreeSet<u32> {
        cluster
            .groups()
            .iter()
            .filter(|(_, placement)| placement.current_voters().contains(NodeId::new(node.id)))
            .map(|(group, _)| group.0)
            .collect()
    };
    // Twelve replicas on five nodes: the node with the fewest hosts at
    // most two of the four groups.
    let purger = nodes
        .iter()
        .min_by_key(|node| hosted(node).len())
        .expect("five nodes");
    assert!(
        hosted(purger).len() < META_TEST_GROUPS,
        "node {} hosts every group: {cluster:?}",
        purger.id
    );
    let every_group: Vec<u32> = (0..u32::try_from(META_TEST_GROUPS).unwrap()).collect();
    let streams = stream_per_group("wide");
    for stream in &streams {
        let url = format!("{}/wide/{stream}", purger.url());
        put_until_created(&client, &url).await;
        post_until_no_content(&client, &url, "before;").await;
    }

    // Every node reports exactly the groups it hosts, and the newest
    // replica of each group counts its one stream.
    for node in &nodes {
        let what = format!("node {} reports the usage of its groups", node.id);
        eventually(&mut procs, &what, async || {
            let report = usage_report(&client, node).await?;
            let groups: BTreeSet<u32> = report
                .groups
                .iter()
                .map(|group| group.raft_group_id)
                .collect();
            if groups == hosted(node) {
                Ok(())
            } else {
                Err(format!("reports {groups:?}"))
            }
        })
        .await;
    }
    wait_bucket_streams(&client, &mut procs, &nodes, "wide", 4).await;

    let backup_store = ursula_ctl::backup::BackupStore::open(
        root.path().join("backup").to_str().expect("a UTF-8 path"),
    )
    .expect("backup store");
    let manifest =
        ursula_ctl::backup::create(&backup_client(&nodes), &backup_store, 1_760_000_000_000)
            .await
            .expect("backup-create");
    assert_eq!(
        manifest
            .groups
            .iter()
            .map(|group| (group.raft_group_id, group.streams))
            .collect::<Vec<_>>(),
        every_group
            .iter()
            .map(|group| (*group, 1))
            .collect::<Vec<_>>(),
        "every group exported with its stream"
    );

    let purge = purge_bucket(&client, &mut procs, purger, "wide").await;
    assert_eq!(purge.removed_streams, 4, "{purge:?}");
    assert_eq!(purge.groups_with_streams, every_group, "{purge:?}");
    wait_bucket_streams(&client, &mut procs, &nodes, "wide", 0).await;

    // A wiped cluster, whose cold flush takes any hot byte.
    for node in &nodes {
        procs.kill(node.id);
        remove_test_path(&node.wal);
    }
    meta.flush_min_hot_bytes = Some(1);
    for node in &nodes {
        node.write(&peers, meta);
        procs.start(binary, node);
    }
    wait_settled(&client, &mut procs, &nodes).await;
    let restored = ursula_ctl::backup::restore(&backup_client(&nodes), &backup_store)
        .await
        .expect("restore");
    assert_eq!(
        (restored.groups, restored.streams, restored.already()),
        (4, 4, 0)
    );
    assert_eq!(
        restored
            .imports
            .iter()
            .map(|import| import.raft_group_id)
            .collect::<Vec<_>>(),
        every_group
    );
    wait_bucket_streams(&client, &mut procs, &nodes, "wide", 4).await;

    for stream in &streams {
        let url = format!("{}/wide/{stream}", purger.url());
        post_until_no_content(&client, &url, "after;").await;
    }
    eventually(&mut procs, "every group's leader flushed it", async || {
        let mut flushed = BTreeSet::new();
        for node in &nodes {
            let metrics = runtime_metrics(&client, node).await?;
            for group in &every_group {
                let index = usize::try_from(*group).unwrap();
                let hot = metrics["per_group_cold_hot_bytes"][index].as_u64();
                let peak = metrics["per_group_cold_hot_bytes_max"][index].as_u64();
                match (hot, peak) {
                    (Some(0), Some(peak)) if peak > 0 => {
                        flushed.insert(*group);
                    }
                    (Some(0), Some(_)) => {}
                    _ => return Err(format!("node {} holds {hot:?} of group {group}", node.id)),
                }
            }
        }
        if flushed.len() == META_TEST_GROUPS {
            Ok(())
        } else {
            Err(format!("flushed {flushed:?}"))
        }
    })
    .await;
}

/// One stream name of each data group, as `bucket`'s streams hash to the
/// groups of a meta test cluster.
fn stream_per_group(bucket: &str) -> Vec<String> {
    let map = ursula_shard::StaticShardMap::new(1, META_TEST_GROUPS).expect("shard map");
    let mut streams: BTreeMap<u32, String> = BTreeMap::new();
    for candidate in 0_u32.. {
        let stream = format!("s{candidate}");
        let group = map
            .locate(&ursula_shard::BucketStreamId::new(bucket, &stream))
            .raft_group_id;
        streams.entry(group.0).or_insert(stream);
        if streams.len() == META_TEST_GROUPS {
            break;
        }
    }
    streams.into_values().collect()
}

/// Node `node`'s usage report, from its client listener.
async fn usage_report(
    client: &reqwest::Client,
    node: &MetaNode,
) -> Result<ursula_proto::admin::UsageReport, String> {
    let url = format!("{}/__ursula/usage", node.url());
    let response = client
        .get(&url)
        .send()
        .await
        .map_err(|err| format!("GET {url}: {err}"))?;
    if response.status() != reqwest::StatusCode::OK {
        return Err(format!("GET {url}: {}", response.status()));
    }
    response
        .json()
        .await
        .map_err(|err| format!("GET {url}: {err}"))
}

/// Waits until the usage reports of `nodes`, as a collector keeps them per
/// group, name every group and count `streams` streams of `bucket`.
async fn wait_bucket_streams(
    client: &reqwest::Client,
    procs: &mut Processes,
    nodes: &[MetaNode],
    bucket: &str,
    streams: u64,
) {
    let what = format!("the usage reports count {streams} streams of {bucket}");
    eventually(procs, &what, async || {
        let mut reports = Vec::new();
        for node in nodes {
            reports.push(usage_report(client, node).await?);
        }
        let (buckets, complete) = ursula_proto::admin::UsageReport::bucket_usage(&reports);
        let counted = buckets.get(bucket).map_or(0, |usage| usage.stream_count);
        if complete && counted == streams {
            Ok(())
        } else {
            Err(format!("complete {complete}, {buckets:?}"))
        }
    })
    .await;
}

/// The `ursulactl` backup client of `nodes`.
fn backup_client(nodes: &[MetaNode]) -> ursula_ctl::backup::BackupClient {
    ursula_ctl::backup::BackupClient::new(
        ursula_ctl::MetricsClient::new(Duration::from_secs(10)).expect("metrics client"),
        nodes.iter().map(MetaNode::ctl).collect(),
    )
    .expect("backup client")
}

/// Node `node`'s `/__ursula/metrics` as JSON, its runtime counters
/// included.
async fn runtime_metrics(
    client: &reqwest::Client,
    node: &MetaNode,
) -> Result<serde_json::Value, String> {
    let url = format!("{}/__ursula/metrics", node.url());
    client
        .get(&url)
        .send()
        .await
        .map_err(|err| format!("GET {url}: {err}"))?
        .json()
        .await
        .map_err(|err| format!("GET {url}: {err}"))
}

/// Purges `bucket` through `node`'s admin listener, again while a group's
/// leader is unknown (`503`).
async fn purge_bucket(
    client: &reqwest::Client,
    procs: &mut Processes,
    node: &MetaNode,
    bucket: &str,
) -> ursula_proto::admin::BucketPurgeReport {
    let url = format!("{}/__ursula/purge/{bucket}", node.admin());
    let what = format!("node {} purges {bucket}", node.id);
    eventually(procs, &what, async || {
        let metrics = runtime_metrics(client, node).await?;
        let incarnation = metrics["process_incarnation"]
            .as_str()
            .ok_or_else(|| format!("no process incarnation: {metrics}"))?
            .to_owned();
        let response = client
            .delete(&url)
            .header(ursula_proto::admin::PROCESS_INCARNATION_HEADER, incarnation)
            .send()
            .await
            .map_err(|err| format!("DELETE {url}: {err}"))?;
        match response.status() {
            reqwest::StatusCode::OK => response
                .json()
                .await
                .map_err(|err| format!("DELETE {url}: {err}")),
            reqwest::StatusCode::SERVICE_UNAVAILABLE => Err(format!("DELETE {url}: 503")),
            status => panic!(
                "DELETE {url}: {status} {}",
                response.text().await.unwrap_or_default()
            ),
        }
    })
    .await
}

/// Waits until `node` runs a replica of exactly the groups `groups`.
async fn runs_exactly(
    client: &reqwest::Client,
    procs: &mut Processes,
    node: &MetaNode,
    groups: &BTreeSet<u64>,
) {
    let what = format!("node {} runs exactly {groups:?}", node.id);
    eventually(procs, &what, async || {
        let metrics = try_node_metrics(client, &node.url()).await?;
        let running: BTreeSet<u64> = metrics
            .groups
            .iter()
            .map(|group| group.raft_group_id)
            .collect();
        if running == *groups {
            Ok(())
        } else {
            Err(format!("runs {running:?}"))
        }
    })
    .await;
}

/// S1 acceptance (#768): a node whose meta journal does not read runs no
/// meta replica, reports `Damaged { kind: journal }` and
/// `placement-unavailable`, advertises on `/__ursula/leadership-shed` that
/// it accepts no leadership transfer (the stance a peer reads before a
/// handoff), and keeps serving data. Elected leader of every group while the only other node
/// left is drained, it hands each group away once that node takes
/// transfers again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cli_a_node_with_damaged_meta_serves_data_and_sheds_its_leaderships() {
    let _guard = static_cluster_cli_test_guard().await;
    let Some(binary) = option_env!("CARGO_BIN_EXE_ursula") else {
        tracing::warn!("CARGO_BIN_EXE_ursula is not set; skipping the meta test");
        return;
    };
    let root = tempfile::tempdir().expect("temp root");
    let nodes = MetaNode::cluster(root.path(), 3);
    let peers = peers_of(&nodes);
    let meta = MetaToml {
        meta_voters: Some(META_VOTERS),
        ..MetaToml::default()
    };
    let client = reqwest::Client::new();
    let mut procs = Processes::default();
    for node in &nodes {
        node.write(&peers, meta);
        procs.start(binary, node);
    }
    wait_bootstrapped(&client, &mut procs, &nodes).await;
    let stream = |node: &MetaNode| format!("{}/meta-cli/damaged", node.url());
    let read = |node: &MetaNode| format!("{}?offset=0&max_bytes=256", stream(node));
    put_until_created(&client, &stream(&nodes[0])).await;
    post_until_no_content(&client, &stream(&nodes[0]), "before-the-damage;").await;

    // 1. Node 3 is drained, which hands its leaderships away. Node 2 stops,
    // and its shutdown hands its leaderships to node 1, the only node left
    // that takes transfers, so node 1 leads every data group. The seed
    // initializers rotate over the nodes, so each node led some groups
    // before. Node 2's meta journal's metadata no longer reads.
    let ctl = ursula_ctl::MetricsClient::new(Duration::from_secs(5)).expect("ctl client");
    let ctl_nodes = nodes.iter().map(MetaNode::ctl).collect::<Vec<_>>();
    let outcome = ursula_ctl::drain_node(
        &ctl_nodes,
        &nodes[2].ctl(),
        &ctl,
        &ursula_ctl::DrainOptions {
            drain_timeout: Duration::from_secs(60),
            ready_timeout: Duration::from_secs(60),
            poll_interval: Duration::from_millis(200),
            lag_tolerance: 16,
            dry_run: false,
        },
    )
    .await
    .expect("drain node 3");
    assert!(
        matches!(outcome, ursula_ctl::DrainOutcome::Drained),
        "{outcome:?}"
    );
    procs.stop(2).await;
    std::fs::write(
        nodes[1].meta_dir().join("journal.meta"),
        b"not a meta journal",
    )
    .expect("damage the meta journal");
    eventually(&mut procs, "node 1 leads every data group", async || {
        let led = leads(&try_node_metrics(&client, &nodes[0].url()).await?, 1);
        if led == META_TEST_GROUPS {
            Ok(())
        } else {
            Err(format!("node 1 leads {led} groups"))
        }
    })
    .await;

    // 2. Node 2 starts without a meta replica: it may campaign, advertises
    // that it accepts no transfer, and serves data through node 1.
    procs.start(binary, &nodes[1]);
    let damaged = wait_placement(
        &client,
        &mut procs,
        &nodes[1],
        "reports its meta damaged",
        |status| {
            matches!(status.meta, MetaReplicaStatus::Damaged {
                kind: MetaDamageKind::Journal,
                ..
            })
        },
    )
    .await;
    assert_eq!(damaged.applied, None, "{damaged:?}");
    let shed = eventually(
        &mut procs,
        "node 2 marks placement-unavailable",
        async || {
            let shed = leadership_shed(&client, &nodes[1]).await;
            if sheds_placement_unavailable(&shed) {
                Ok(shed)
            } else {
                Err(format!("{shed:?}"))
            }
        },
    )
    .await;
    assert!(shed.should_campaign, "{shed:?}");
    assert!(!shed.should_accept_transfer, "{shed:?}");
    post_until_no_content(&client, &stream(&nodes[1]), "through-the-damaged-node;").await;
    read_until_matches(
        &client,
        &read(&nodes[1]),
        b"before-the-damage;through-the-damaged-node;",
    )
    .await;

    // 3. Node 1 dies. Node 3 may not campaign, so node 2 wins every group,
    // and keeps them while no node takes a transfer.
    procs.kill(1);
    eventually(&mut procs, "node 2 leads every data group", async || {
        let led = leads(&try_node_metrics(&client, &nodes[1].url()).await?, 2);
        if led == META_TEST_GROUPS {
            Ok(())
        } else {
            Err(format!("node 2 leads {led} groups"))
        }
    })
    .await;
    post_until_no_content(&client, &stream(&nodes[1]), "led-by-the-damaged-node;").await;

    // 4. Node 3 takes transfers again: node 2 hands every group to it.
    ctl.set_maintenance_drain(&nodes[2].ctl(), false)
        .await
        .expect("undrain node 3");
    eventually(
        &mut procs,
        "node 2 hands every data group to node 3",
        async || {
            let kept = leads(&try_node_metrics(&client, &nodes[1].url()).await?, 2);
            let taken = leads(&try_node_metrics(&client, &nodes[2].url()).await?, 3);
            if kept == 0 && taken == META_TEST_GROUPS {
                Ok(())
            } else {
                Err(format!("node 2 leads {kept} groups, node 3 {taken}"))
            }
        },
    )
    .await;
    post_until_no_content(&client, &stream(&nodes[1]), "after-the-shed;").await;
    read_until_matches(
        &client,
        &read(&nodes[2]),
        b"before-the-damage;through-the-damaged-node;led-by-the-damaged-node;after-the-shed;",
    )
    .await;
}

/// S1, r1 H1 (#768): while node 3 claims another replication factor, the
/// genesis node names node 3 and its claim and meta does not form. No
/// view places a group, so no node hosts a data replica: no data group
/// forms while meta waits. Node 3 restarts with
/// the fixed claim while node 1's `raft.peers` names node 2's address for
/// node 3: node 2 refuses the misdelivered genesis probes as meant for
/// another node, which leave node 3 unanswered and are never read as a
/// claim or a format mismatch, so meta still does not form. Node 1
/// restarts with the right address, meta forms and bootstraps, and the
/// data groups form and serve.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cli_a_genesis_claim_mismatch_holds_meta_until_fixed() {
    let _guard = static_cluster_cli_test_guard().await;
    let Some(binary) = option_env!("CARGO_BIN_EXE_ursula") else {
        tracing::warn!("CARGO_BIN_EXE_ursula is not set; skipping the meta test");
        return;
    };
    let root = tempfile::tempdir().expect("temp root");
    let nodes = MetaNode::cluster(root.path(), 3);
    let peers = peers_of(&nodes);
    let meta = MetaToml {
        meta_voters: Some(META_VOTERS),
        ..MetaToml::default()
    };
    let client = reqwest::Client::new();
    let mut procs = Processes::default();
    for node in &nodes {
        let claim = if node.id == 3 {
            MetaToml {
                replication_factor: Some(2),
                ..meta
            }
        } else {
            meta
        };
        node.write(&peers, claim);
        procs.start(binary, node);
    }
    let no_meta = |status: &PlacementStatus| {
        matches!(status.view, ClusterView::MetaGenesis { .. }) && status.applied.is_none()
    };

    // 1. The genesis node names node 3's claim, and no data group forms.
    let held = wait_placement(
        &client,
        &mut procs,
        &nodes[0],
        "names node 3's claim",
        |status| {
            matches!(
                &status.view,
                ClusterView::MetaGenesis { genesis_mismatch, .. }
                    if node_ids(genesis_mismatch.keys().copied()) == BTreeSet::from([3])
            )
        },
    )
    .await;
    let ClusterView::MetaGenesis {
        genesis_mismatch, ..
    } = &held.view
    else {
        panic!("the wait matched a genesis view");
    };
    let claim = &genesis_mismatch[&NodeId::new(3)];
    assert_eq!(claim.replication_factor.get(), 2, "{claim:?}");
    assert_eq!(
        node_ids(claim.meta_voters.iter()),
        BTreeSet::from([1, 2, 3])
    );
    // The data groups would have formed within these 5 s if a node hosted
    // a replica before a view placed it.
    for node in &nodes {
        wait_placement(&client, &mut procs, node, "listens without meta", no_meta).await;
    }
    for _ in 0..10 {
        tokio::time::sleep(Duration::from_millis(500)).await;
        for node in &nodes {
            let status = placement_status(&client, node)
                .await
                .expect("placement status");
            assert!(no_meta(&status), "{status:?}");
            let metrics = try_node_metrics(&client, &node.url())
                .await
                .expect("node metrics");
            assert!(metrics.groups.is_empty(), "node {}: {metrics:?}", node.id);
        }
    }

    // 2. Node 3 claims the same values again, but node 1's genesis probes
    // of node 3 reach node 2. Node 1 stops first, so it never sees node 3's
    // fixed claim at node 3's address.
    procs.stop(1).await;
    procs.stop(3).await;
    nodes[2].write(&peers, meta);
    procs.start(binary, &nodes[2]);
    let misdelivering = peers
        .iter()
        .map(|(id, url)| {
            if *id == 3 {
                (3, nodes[1].url())
            } else {
                (*id, url.clone())
            }
        })
        .collect::<Vec<_>>();
    nodes[0].write(&misdelivering, meta);
    procs.start(binary, &nodes[0]);
    wait_placement(
        &client,
        &mut procs,
        &nodes[0],
        "waits for node 3 alone",
        |status| {
            matches!(
                &status.view,
                ClusterView::MetaGenesis { unanswered, genesis_mismatch }
                    if genesis_mismatch.is_empty()
                        && node_ids(unanswered.iter().copied()) == BTreeSet::from([3])
            )
        },
    )
    .await;
    for _ in 0..10 {
        tokio::time::sleep(Duration::from_millis(500)).await;
        for node in &nodes {
            let status = placement_status(&client, node)
                .await
                .expect("placement status");
            assert!(no_meta(&status), "{status:?}");
        }
    }
    for node in &nodes[..2] {
        let ready: ursula_proto::admin::ServingReadiness = client
            .get(format!("{}/__ursula/ready", node.url()))
            .send()
            .await
            .expect("readiness")
            .json()
            .await
            .expect("readiness JSON");
        assert!(!ready.format_epoch_mismatch, "node {}: {ready:?}", node.id);
    }

    // 3. Node 1 dials node 3 again: meta forms and bootstraps.
    procs.stop(1).await;
    nodes[0].write(&peers, meta);
    procs.start(binary, &nodes[0]);
    let formed = wait_settled(&client, &mut procs, &nodes).await;
    let cluster = settled(&formed[0]).unwrap();
    assert_eq!(cluster.params().replication_factor.get(), 3);
    assert_eq!(
        node_ids(cluster.nodes().keys().copied()),
        BTreeSet::from([1, 2, 3])
    );
    for status in &formed {
        assert_eq!(bootstrapped(status), Some(cluster), "{status:?}");
    }

    // Meta bound every root, so the data groups form and serve.
    let stream = |node: &MetaNode| format!("{}/meta-cli/claims", node.url());
    let create = stream(&nodes[1]);
    eventually(&mut procs, "the data groups serve", async || {
        let response = client
            .put(&create)
            .header("content-type", "text/plain")
            .send()
            .await
            .map_err(|err| format!("PUT {create}: {err}"))?;
        if response.status().is_success() {
            Ok(())
        } else {
            Err(format!("PUT {create}: {}", response.status()))
        }
    })
    .await;
    post_until_no_content(&client, &stream(&nodes[1]), "once-meta-formed;").await;
    read_until_matches(
        &client,
        &format!("{}?offset=0&max_bytes=64", stream(&nodes[2])),
        b"once-meta-formed;",
    )
    .await;
}

/// f2 (#768): every node loses its run state while stopped, as the
/// cutting plan test of a data group does, so each meta replica opens
/// recovering and keeps its vote. Every meta gate stalls and meta has no
/// leader. `ursulactl recover-group --target meta --accept-loss` plans
/// meta, meta answers again on every node, and the plan's history event
/// keys every placement anew. The plan's restart of the meta replica marks
/// no node `placement-unavailable` (r1 L12). The data groups are gated
/// too and stay so: the test claims nothing about them.
///
/// A replica that lost its meta journal lost its vote with it, and refuses
/// a plan (`missing_vote_floor`) until a live leader proves its vote, so
/// a meta group where a majority of the voters lost their journals is not
/// recovered by a plan. This test does not claim it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cli_meta_recovers_by_a_plan_through_recover_group_target_meta() {
    let _guard = static_cluster_cli_test_guard().await;
    let Some(binary) = option_env!("CARGO_BIN_EXE_ursula") else {
        tracing::warn!("CARGO_BIN_EXE_ursula is not set; skipping the meta test");
        return;
    };
    let root = tempfile::tempdir().expect("temp root");
    let nodes = MetaNode::cluster(root.path(), 3);
    let peers = peers_of(&nodes);
    let meta = MetaToml {
        meta_voters: Some(META_VOTERS),
        ..MetaToml::default()
    };
    let client = reqwest::Client::new();
    let mut procs = Processes::default();
    for node in &nodes {
        node.write(&peers, meta);
        procs.start(binary, node);
    }
    let formed = wait_bootstrapped(&client, &mut procs, &nodes).await;
    let before = bootstrapped(&formed[0]).unwrap().clone();

    // 1. Every node stops and loses its run state, so how its last run
    // ended is unknown.
    for node in &nodes {
        procs.stop(node.id).await;
    }
    for node in &nodes {
        remove_test_path(node.wal.join("raft-log").join("run-state.bin"));
    }
    for node in &nodes {
        procs.start(binary, node);
    }

    // 2. Every meta gate stalls and no node leads meta. The nodes were
    // just started, so the poll tolerates a node that does not listen yet.
    let replicas = eventually(&mut procs, "every meta gate stalls", async || {
        let mut replicas = Vec::new();
        for node in &nodes {
            let replica = try_node_metrics(&client, &node.url()).await?.meta;
            let gate = replica.as_ref().map(|replica| replica.recovery_gate);
            if gate != Some(ursula_proto::admin::RecoveryGateStatus::Stalled) {
                return Err(format!("node {}: {replica:?}", node.id));
            }
            replicas.push((node.id, replica));
        }
        Ok(replicas)
    })
    .await;
    for (id, replica) in &replicas {
        assert!(
            replica.as_ref().is_some_and(|replica| !replica.leads_live),
            "node {id}: {replica:?}"
        );
    }

    // 3. The meta plan, as `ursulactl recover-group --target meta
    // --accept-loss` sends it.
    let ctl = ursula_ctl::MetricsClient::new(Duration::from_secs(5)).expect("ctl client");
    let ctl_nodes = nodes.iter().map(MetaNode::ctl).collect::<Vec<_>>();
    let outcome = ursula_ctl::recovery::recover_group(
        &ctl,
        &ctl_nodes,
        ursula_ctl::recovery::RecoverTarget::Meta,
        &ursula_ctl::recovery::RecoverOptions {
            max_plans: 3,
            settle_timeout: Duration::from_secs(120),
            poll_interval: Duration::from_secs(1),
            accept_timeout: Duration::from_secs(60),
            accept_loss: true,
        },
    )
    .await
    .expect("the meta plan");
    let ursula_ctl::recovery::RecoverOutcome::Satisfied { plan, stopped, .. } = outcome else {
        panic!("the meta plan was not satisfied: {outcome:?}");
    };
    assert!(META_VOTERS.contains(&plan.leader), "{plan:?}");
    assert!(stopped.is_empty(), "{stopped:?}");

    // 4. Meta answers on every node, each placement keyed at one new
    // epoch, the plan's history event.
    let recovered = wait_bootstrapped(&client, &mut procs, &nodes).await;
    let after = sinces(bootstrapped(&recovered[0]).unwrap());
    let epochs = after.values().collect::<BTreeSet<_>>();
    assert_eq!(epochs.len(), 1, "{after:?}");
    for (group, since) in sinces(&before) {
        assert_ne!(
            after.get(&group),
            Some(&since),
            "group {group:?} kept its key"
        );
    }
    for status in &recovered {
        let cluster = bootstrapped(status).unwrap();
        assert_eq!(sinces(cluster), after, "{status:?}");
        assert_eq!(cluster.params(), before.params());
        assert!(cluster.retirements().is_empty(), "{status:?}");
        assert_eq!(meta_role(status), Some(MetaRole::Voter), "{status:?}");
    }
    for node in &nodes {
        let shed = leadership_shed(&client, node).await;
        assert!(
            !sheds_placement_unavailable(&shed),
            "node {}: {shed:?}",
            node.id
        );
    }
}

/// S1(a), D7 (#768): on nodes whose snapshot store is S3, a meta snapshot
/// is recorded inline, and meta puts no object in the node's S3 directory
/// of group 0, where only data group 0's record names objects. A meta
/// record rewritten to name an S3 object is refused at the restore as
/// `Damaged { kind: pointer }`, and the node keeps serving data. Runs on
/// real S3 in CI.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cli_an_s3_backed_node_refuses_a_non_inline_meta_record() {
    let _guard = static_cluster_cli_test_guard().await;
    if std::env::var("URSULA_COLD_S3_INTEGRATION").ok().as_deref() != Some("1") {
        tracing::warn!(
            "skipping the S3 meta record test; set URSULA_COLD_S3_INTEGRATION=1 and URSULA_COLD_S3_BUCKET"
        );
        return;
    }
    let Some(binary) = option_env!("CARGO_BIN_EXE_ursula") else {
        tracing::warn!("CARGO_BIN_EXE_ursula is not set; skipping the S3 meta record test");
        return;
    };
    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time after unix epoch")
        .as_nanos();
    // CI scopes each job's credentials to URSULA_S3_PREFIX.
    let cold_root = match std::env::var("URSULA_S3_PREFIX") {
        Ok(prefix) => format!("{prefix}/ursula-cli-s3-meta-inline/{suffix}"),
        Err(_) => format!("ursula-cli-s3-meta-inline/{suffix}"),
    };
    let root = tempfile::tempdir().expect("temp root");
    let mut nodes = MetaNode::cluster(root.path(), 3);
    for node in &mut nodes {
        node.cold_root = Some(cold_root.clone());
    }
    let peers = peers_of(&nodes);
    let meta = MetaToml {
        meta_voters: Some(META_VOTERS),
        ..MetaToml::default()
    };
    for node in &nodes {
        node.write(&peers, meta);
    }
    let config = ursula_config::load_config(Some(&nodes[0].config), None, None)
        .unwrap_or_else(|err| panic!("load config for the cold store: {err}"));
    config
        .validate()
        .unwrap_or_else(|err| panic!("the node config is valid: {err}"));
    let prefix = config.storage.snapshot.snapshot_prefix();
    let cold_store = ColdStore::try_new(&config.storage.cold)
        .unwrap_or_else(|err| panic!("{}", s3_failure("open the cold store", "", &err)));
    let client = reqwest::Client::new();
    let mut procs = Processes::default();
    for node in &nodes {
        procs.start(binary, node);
    }
    wait_bootstrapped(&client, &mut procs, &nodes).await;
    let stream = |node: &MetaNode| format!("{}/meta-cli/inline", node.url());
    put_until_created(&client, &stream(&nodes[0])).await;
    post_until_no_content(&client, &stream(&nodes[0]), "with-an-s3-store;").await;

    // 1. Every node's meta snapshot record names its snapshot inline.
    let record = |node: &MetaNode| node.meta_dir().join("group-0.snapshot.json");
    for node in &nodes {
        let response = admin_test_post(
            &client,
            format!("{}/__ursula/raft/meta/snapshot", node.admin()),
        )
        .await
        .send()
        .await
        .expect("snapshot meta");
        assert_eq!(response.status(), reqwest::StatusCode::OK);
        let what = format!("node {} records its meta snapshot", node.id);
        let pointer = eventually(&mut procs, &what, async || {
            ursula_raft::current_snapshot_pointer(&record(node))
                .map_err(|err| err.to_string())?
                .ok_or_else(|| "no current snapshot record yet".to_owned())
        })
        .await;
        assert!(
            matches!(
                pointer.location,
                ursula_runtime::SnapshotLocation::Inline { .. }
            ),
            "node {}: {pointer:?}",
            node.id
        );
    }

    // 2. Each node's S3 directory of group 0 holds only what data group 0's
    // record names.
    let wal_dirs = nodes
        .iter()
        .map(|node| (node.id, node.wal.clone()))
        .collect::<Vec<_>>();
    let (view, names) = snapshot_ownership(&cold_store, &prefix, &wal_dirs).await;
    for key in view.objects.keys() {
        assert!(
            names.iter().any(|node| node.named.contains(key)),
            "{key} is no object data group 0 names: {names:?}"
        );
    }

    // 3. Node 3's record is rewritten to name an object of its own S3
    // directory: its restore refuses it, and it keeps serving data.
    procs.stop(3).await;
    let bytes = std::fs::read(record(&nodes[2])).expect("read node 3's meta record");
    let pointer = ursula_runtime::SnapshotPointer {
        snapshot_id: "meta-cli-not-inline".to_owned(),
        location: ursula_runtime::SnapshotLocation::S3 {
            key: format!(
                "{}group-0/00000000000000000009-0123456789abcdef.snap",
                ursula_runtime::snapshot_harness::node_directory(&prefix, 3)
            ),
            size_bytes: 4096,
            stored_size_bytes: 1234,
            compression: ursula_runtime::SnapshotCompression::Zstd,
        },
    };
    std::fs::write(
        record(&nodes[2]),
        ursula_raft::snapshot_record_with_pointer(&bytes, &pointer)
            .expect("re-encode node 3's meta record"),
    )
    .expect("write node 3's meta record");
    procs.start(binary, &nodes[2]);
    wait_placement(
        &client,
        &mut procs,
        &nodes[2],
        "refuses its meta record",
        |status| {
            matches!(status.meta, MetaReplicaStatus::Damaged {
                kind: MetaDamageKind::Pointer,
                ..
            })
        },
    )
    .await;
    post_until_no_content(&client, &stream(&nodes[2]), "after-the-refusal;").await;
    read_until_matches(
        &client,
        &format!("{}?offset=0&max_bytes=64", stream(&nodes[0])),
        b"with-an-s3-store;after-the-refusal;",
    )
    .await;

    drop(procs);
    cold_store
        .remove_all("")
        .await
        .unwrap_or_else(|err| panic!("{}", s3_failure("clear the test root", "", &err)));
}

/// Node `node`'s readiness from its client listener: the status and the
/// report.
async fn readiness(
    client: &reqwest::Client,
    node: &MetaNode,
) -> Result<(reqwest::StatusCode, ursula_proto::admin::ServingReadiness), String> {
    let url = format!("{}/__ursula/ready", node.url());
    let response = client
        .get(&url)
        .send()
        .await
        .map_err(|err| format!("GET {url}: {err}"))?;
    let status = response.status();
    let report = response
        .json()
        .await
        .map_err(|err| format!("GET {url}: {err}"))?;
    Ok((status, report))
}

/// Waits until node `node` reports readiness `reason` (`None`: ready).
async fn wait_readiness_reason(
    client: &reqwest::Client,
    procs: &mut Processes,
    node: &MetaNode,
    reason: Option<ursula_proto::admin::ServingReadinessReason>,
) -> ursula_proto::admin::ServingReadiness {
    let what = format!("node {} reports readiness {reason:?}", node.id);
    eventually(procs, &what, async || {
        let (status, report) = readiness(client, node).await?;
        let expected = if reason.is_none() {
            reqwest::StatusCode::OK
        } else {
            reqwest::StatusCode::SERVICE_UNAVAILABLE
        };
        if report.reason == reason && status == expected {
            Ok(report)
        } else {
            Err(format!("{status} {report:?}"))
        }
    })
    .await
}

/// The root node `node` is bound to in `cluster`.
fn bound_root(cluster: &Cluster, node: u64) -> ursula_proto::placement::RootId {
    match cluster.node(NodeId::new(node)).map(|record| record.root) {
        Some(ursula_proto::placement::RootSlot::Bound(root)) => root,
        other => panic!("node {node} is not bound: {other:?}"),
    }
}

/// IT-3 (#768): node 3 loses its disk and starts on an empty WAL under its
/// live id. The meta group refuses the new root for the one it binds the
/// id to: node 3 reports `root_refused` naming that root, while nodes 1 and
/// 2 serve. Its data replicas take part in nothing: the empty WAL holds no
/// group, so the node creates a replica only where a confirmed view places
/// one, and
/// any it runs is held out from its open. None took a vote floor (its term
/// stays 0, the term a barrier would adopt is the leader's), none holds a
/// log entry, and every data gate stays closed. Its meta
/// replica is not held before the refusal, and the test claims nothing
/// about it. A crash and restart of node 3 while it is refused changes
/// nothing: its identity file still records the root unbound, so it is
/// refused, not superseded, and its data stays held, whatever its meta
/// replica wrote in the first run. `ursulactl node replace-disk`
/// refuses a node that still runs on the root it names, then releases the
/// old root through another node's admin plane. Node 3 binds its new root
/// with no restart, reports `predecessor_wait`, and rejoins: it becomes
/// ready and reads every write. A second release of the old root is
/// refused as stale.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cli_an_empty_disk_under_a_live_id_binds_after_replace_disk() {
    use ursula_proto::admin::ServingReadinessReason;
    use ursula_proto::placement::RootSlot;

    let _guard = static_cluster_cli_test_guard().await;
    let Some(binary) = option_env!("CARGO_BIN_EXE_ursula") else {
        tracing::warn!("CARGO_BIN_EXE_ursula is not set; skipping the replace-disk test");
        return;
    };
    let root = tempfile::tempdir().expect("temp root");
    let nodes = MetaNode::cluster(root.path(), 3);
    let peers = peers_of(&nodes);
    let meta = MetaToml {
        meta_voters: Some(META_VOTERS),
        ..MetaToml::default()
    };
    let client = reqwest::Client::new();
    let mut procs = Processes::default();
    for node in &nodes {
        node.write(&peers, meta);
        procs.start(binary, node);
    }
    let formed = wait_bootstrapped(&client, &mut procs, &nodes).await;
    let cluster = bootstrapped(&formed[0]).expect("bootstrapped").clone();
    let old = bound_root(&cluster, 3);
    let stream = format!("{}/replace-disk/served", nodes[0].url());
    put_until_created(&client, &stream).await;
    post_until_no_content(&client, &stream, "before-the-loss;").await;
    let read =
        |node: &MetaNode| format!("{}/replace-disk/served?offset=0&max_bytes=128", node.url());

    // 1. Node 3 loses its disk and starts on an empty WAL.
    procs.kill(3);
    remove_test_path(&nodes[2].wal);
    procs.start(binary, &nodes[2]);
    wait_placement(
        &client,
        &mut procs,
        &nodes[2],
        "is refused for the root its id is bound to",
        |status| {
            status.registration
                == RegistrationStatus::RootRefused {
                    slot: RootSlot::Bound(old),
                }
        },
    )
    .await;
    wait_readiness_reason(
        &client,
        &mut procs,
        &nodes[2],
        Some(ServingReadinessReason::RootRefused),
    )
    .await;
    // The healthy nodes serve meanwhile.
    post_until_no_content(&client, &stream, "while-refused;").await;
    read_until_matches(&client, &read(&nodes[1]), b"before-the-loss;while-refused;").await;
    // Node 3's data replicas took part in nothing, though their leaders
    // replicated a write since it started.
    let held = try_node_metrics(&client, &nodes[2].url())
        .await
        .expect("node 3 reports its replicas");
    assert_data_held(&held, 3);
    // A restart while refused: still refused and held, never superseded.
    procs.kill(3);
    procs.start(binary, &nodes[2]);
    wait_placement(
        &client,
        &mut procs,
        &nodes[2],
        "is still refused after a restart",
        |status| {
            status.registration
                == RegistrationStatus::RootRefused {
                    slot: RootSlot::Bound(old),
                }
        },
    )
    .await;
    wait_readiness_reason(
        &client,
        &mut procs,
        &nodes[2],
        Some(ServingReadinessReason::RootRefused),
    )
    .await;
    post_until_no_content(&client, &stream, "while-restarted;").await;
    let held = try_node_metrics(&client, &nodes[2].url())
        .await
        .expect("node 3 reports its replicas after the restart");
    assert_data_held(&held, 3);

    // 2. The guard refuses a node that still runs on the root it names.
    let ctl = ursula_ctl::MetricsClient::new(Duration::from_secs(5)).expect("ctl client");
    let ctl_nodes = nodes.iter().map(MetaNode::ctl).collect::<Vec<_>>();
    let refused =
        ursula_ctl::placement::replace_disk(&ctl, &ctl_nodes, 1, bound_root(&cluster, 1), false)
            .await
            .expect_err("node 1 still runs on its root");
    assert!(
        matches!(
            refused.downcast_ref::<ursula_ctl::placement::ReplaceDiskRefusal>(),
            Some(ursula_ctl::placement::ReplaceDiskRefusal::RunsOnRoot { node: 1, .. })
        ),
        "{refused:?}"
    );
    // The release of node 3's old root goes through another node.
    let outcome = ursula_ctl::placement::replace_disk(&ctl, &ctl_nodes, 3, old, false)
        .await
        .expect("replace node 3's disk");
    assert_eq!(outcome.before.slot, RootSlot::Bound(old));
    assert_ne!(outcome.before.via.id, 3);
    assert_eq!(outcome.record.root, RootSlot::Released(old));

    // 3. Node 3 binds its new root with no restart, waits out its
    // predecessor, and rejoins.
    let bound = wait_placement(
        &client,
        &mut procs,
        &nodes[2],
        "binds its new root",
        |status| matches!(status.registration, RegistrationStatus::Answered(_)),
    )
    .await;
    let RegistrationStatus::Answered(Registered::Referenced(record)) = bound.registration else {
        panic!("node 3 keeps its roles: {bound:?}");
    };
    assert!(
        matches!(record.root, RootSlot::Bound(new) if new != old),
        "{record:?}"
    );
    wait_readiness_reason(
        &client,
        &mut procs,
        &nodes[2],
        Some(ServingReadinessReason::PredecessorWait),
    )
    .await;
    wait_readiness_reason(&client, &mut procs, &nodes[2], None).await;
    post_until_no_content(&client, &stream, "after-the-replacement;").await;
    read_until_matches(
        &client,
        &read(&nodes[2]),
        b"before-the-loss;while-refused;while-restarted;after-the-replacement;",
    )
    .await;

    // 4. The old root never binds again, and its release is now stale.
    let stale = ursula_ctl::placement::replace_disk(&ctl, &ctl_nodes, 3, old, true)
        .await
        .expect_err("the slot names the new root");
    assert!(
        matches!(
            stale.downcast_ref::<ursula_ctl::placement::ReplaceDiskRefusal>(),
            Some(ursula_ctl::placement::ReplaceDiskRefusal::Refused { status, refusal, .. })
                if *status == reqwest::StatusCode::CONFLICT
                    && matches!(
                        refusal.reason,
                        ursula_control::PlacementApiReason::Refused(
                            ursula_control::ControlError::StaleRoot { .. }
                        )
                    )
        ),
        "{stale:?}"
    );
}

/// IT-4 (#768): a WAL root opens only under the node id that created it.
/// Node 2 started on node 1's WAL refuses to start, naming both ids, and
/// leaves the root as it was.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cli_a_wal_root_started_under_another_node_id_is_refused() {
    let _guard = static_cluster_cli_test_guard().await;
    let Some(binary) = option_env!("CARGO_BIN_EXE_ursula") else {
        tracing::warn!("CARGO_BIN_EXE_ursula is not set; skipping the other-node test");
        return;
    };
    let root = tempfile::tempdir().expect("temp root");
    let log_dir = root.path().join("raft-log");
    let config_path = root.path().join("node.toml");
    let port = free_port();
    let base_url = format!("http://127.0.0.1:{port}");
    write_single_node_cluster_config(&config_path, port, 1, 1, &base_url, &log_dir);
    {
        let mut child = spawn_node_with_cluster_config(binary, &config_path);
        let client = reqwest::Client::new();
        wait_until_ready(&client, &base_url, std::slice::from_mut(&mut child)).await;
        sigterm_and_wait_for_clean_exit(&mut child).await;
    }
    let topology = log_dir.join("raft-log").join("topology.bin");
    let before = std::fs::read(&topology).expect("node 1 created the WAL topology");

    write_single_node_cluster_config(&config_path, port, 2, 1, &base_url, &log_dir);
    let mut child = spawn_node_with_cluster_config(binary, &config_path);
    let status = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            if let Some(status) = child.child.try_wait().unwrap() {
                break status;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("node 2 on node 1's WAL must refuse to start");
    assert!(!status.success(), "node 2 must not start on node 1's WAL");
    let report = child_stderr_tail(&child);
    assert!(
        report.contains("belongs to node 1, but raft.node_id is 2"),
        "{report}"
    );
    assert_eq!(std::fs::read(&topology).unwrap(), before);
}

/// IT-2 (#768 addendum, section 7): restarts on the same boot owe no
/// predecessor wait. A SIGKILL of every node is a crash of each process on
/// its own mount: each next run reads `process_crash`, finds its
/// predecessor gone and serves with every write kept. Node 3, stopped with
/// SIGTERM and started again under write load, catches up within a bound.
/// It may lead some groups when it stops: the leaders of the others, and of
/// those once they move, hold pooled AppendStream sessions that name its old
/// process (D25).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cli_restarts_on_the_same_boot_owe_no_predecessor_wait() {
    let _guard = static_cluster_cli_test_guard().await;
    let Some(binary) = option_env!("CARGO_BIN_EXE_ursula") else {
        tracing::warn!("CARGO_BIN_EXE_ursula is not set; skipping the same-boot restart test");
        return;
    };
    let root = tempfile::tempdir().expect("temp root");
    let nodes = MetaNode::cluster(root.path(), 3);
    let peers = peers_of(&nodes);
    let meta = MetaToml {
        meta_voters: Some(META_VOTERS),
        ..MetaToml::default()
    };
    let client = reqwest::Client::new();
    let mut procs = Processes::default();
    for node in &nodes {
        node.write(&peers, meta);
        procs.start(binary, node);
    }
    wait_bootstrapped(&client, &mut procs, &nodes).await;
    let stream = format!("{}/same-boot/restarts", nodes[0].url());
    put_until_created(&client, &stream).await;
    post_until_no_content(&client, &stream, "before-the-kill;").await;

    // 1. Every process is killed, and each next run owes no wait.
    for node in &nodes {
        procs.kill(node.id);
    }
    for node in &nodes {
        procs.start(binary, node);
    }
    // Where the kernel reports no boot id, a killed process reads as a host
    // crash, whose successor waits.
    let (run, predecessor) = if cfg!(target_os = "linux") {
        ("process_crash", "gone")
    } else {
        ("host_crash", "may_live")
    };
    for node in &nodes {
        wait_readiness_reason(&client, &mut procs, node, None).await;
        let recovery = wal_recovery(&client, &node.url()).await;
        assert_eq!(recovery["previous_run"]["kind"], run, "{recovery}");
        assert_eq!(recovery["predecessor"], predecessor, "{recovery}");
    }
    let read =
        |node: &MetaNode| format!("{}/same-boot/restarts?offset=0&max_bytes=65536", node.url());
    read_until_matches(&client, &read(&nodes[1]), b"before-the-kill;").await;

    // 2. Under write load, node 3 stops by SIGTERM and starts again.
    let load = WriteLoad::start(&client, &stream, "load");
    tokio::time::sleep(Duration::from_secs(1)).await;
    procs.stop(3).await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    procs.start(binary, &nodes[2]);
    let restarted = std::time::Instant::now();
    let bound = Duration::from_secs(20);
    wait_readiness_reason(&client, &mut procs, &nodes[2], None).await;
    let recovery = wal_recovery(&client, &nodes[2].url()).await;
    assert_eq!(recovery["previous_run"]["kind"], "clean", "{recovery}");
    assert_eq!(recovery["predecessor"], "gone", "{recovery}");
    tokio::time::sleep(Duration::from_secs(2)).await;
    let mut expected = vec!["before-the-kill;".to_owned()];
    let loaded = load.stop().await;
    assert!(!loaded.is_empty(), "the write load was acknowledged");
    expected.extend(loaded);
    // What node 3 applies reaches each group's committed index within the
    // bound, through the leaders that streamed to its old process. A node
    // whose metrics fail reports nothing, so the check waits until some
    // leader reported, rather than pass with nothing to compare.
    let committed = eventually(
        &mut procs,
        "a leader reports its committed index",
        async || {
            let committed = leaders_committed(&client, &nodes).await;
            if committed.is_empty() {
                Err("no leader reported a committed index".to_owned())
            } else {
                Ok(committed)
            }
        },
    )
    .await;
    eventually(
        &mut procs,
        "node 3 caught up after its restart",
        async || {
            let metrics = try_node_metrics(&client, &nodes[2].url()).await?;
            let behind = committed
                .iter()
                .filter(|(group, index)| {
                    metrics
                        .groups
                        .iter()
                        .find(|metrics| metrics.raft_group_id == **group)
                        .and_then(|metrics| metrics.last_applied_index)
                        < Some(**index)
                })
                .collect::<Vec<_>>();
            if behind.is_empty() {
                Ok(())
            } else {
                Err(format!("groups behind their committed index: {behind:?}"))
            }
        },
    )
    .await;
    assert!(
        restarted.elapsed() < bound,
        "node 3 caught up {:?} after it started",
        restarted.elapsed()
    );
    let on_node_1 = read_until_holding(&client, &read(&nodes[0]), &expected).await;
    read_until_matches(&client, &read(&nodes[2]), &on_node_1).await;
}

/// Reads `url` until it holds each of `writes` in order, among writes whose
/// outcome is unknown, and answers what it read.
async fn read_until_holding(client: &reqwest::Client, url: &str, writes: &[String]) -> Vec<u8> {
    let holds = |bytes: &[u8]| {
        let mut rest = String::from_utf8_lossy(bytes).into_owned();
        writes.iter().all(|write| match rest.find(write.as_str()) {
            Some(at) => {
                rest = rest.split_off(at.saturating_add(write.len()));
                true
            }
            None => false,
        })
    };
    // 15 s: after a leader is killed, an election and a read on a loaded
    // runner.
    let mut last = Vec::new();
    for _ in 0..300 {
        if let Ok(response) = client.get(url).send().await
            && response.status().is_success()
        {
            last = response.bytes().await.expect("read the stream").to_vec();
            if holds(&last) {
                return last;
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!(
        "{url} does not hold the {} acknowledged writes: {:?}",
        writes.len(),
        String::from_utf8_lossy(&last)
    );
}

/// IT-1 (#768 addendum, section 7): a replaced process behind a relay, over
/// real gRPC with pooled channels and AppendStream sessions. Nodes 1 and 2
/// reach node 3 through a relay. Pointed at node 2, an address that serves
/// another node, the relay fences nobody and writes go on. Then node 3's
/// process is stopped with SIGSTOP, its root released by `ursulactl node
/// replace-disk` and a replacement started on an empty WAL, which the
/// relay's new connections reach while its open ones stay on the old
/// process, under write load, and the old process is continued. That breaks
/// the release rule while every peer reaches meta. The continued old
/// process reports `superseded`, the replacement becomes ready after its
/// wait, and with node 1 killed no acknowledged write is lost.
///
/// It does not show that no leader counts the old process's answers. Node 2
/// acknowledges throughout, so every write commits on a leader and node 2
/// whether or not the old process's answers count, and node 1 is killed
/// whether or not it leads the stream's group.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cli_a_replaced_process_behind_a_relay_is_superseded_and_loses_no_write() {
    use ursula_proto::admin::ServingReadinessReason;

    let _guard = static_cluster_cli_test_guard().await;
    let Some(binary) = option_env!("CARGO_BIN_EXE_ursula") else {
        tracing::warn!("CARGO_BIN_EXE_ursula is not set; skipping the relayed replacement test");
        return;
    };
    let root = tempfile::tempdir().expect("temp root");
    let mut nodes = MetaNode::cluster(root.path(), 3);
    let relay = Relay::start(nodes[2].port).await;
    // Each node reaches node 3 through the relay, and itself directly.
    let peers_for = |nodes: &[MetaNode], me: &MetaNode| -> Vec<(u64, String)> {
        nodes
            .iter()
            .map(|node| match (node.id, me.id) {
                (3, 3) => me.peer(),
                (3, _) => (3, relay.url()),
                _ => node.peer(),
            })
            .collect()
    };
    let meta = MetaToml {
        meta_voters: Some(META_VOTERS),
        ..MetaToml::default()
    };
    let client = reqwest::Client::new();
    let mut procs = Processes::default();
    for node in &nodes {
        node.write(&peers_for(&nodes, node), meta);
        procs.start(binary, node);
    }
    let formed = wait_bootstrapped(&client, &mut procs, &nodes).await;
    let cluster = bootstrapped(&formed[0]).expect("bootstrapped").clone();
    let stream = format!("{}/relayed/replacement", nodes[0].url());
    put_until_created(&client, &stream).await;
    post_until_no_content(&client, &stream, "formed;").await;
    let mut acknowledged = vec!["formed;".to_owned()];
    let read = |node: &MetaNode| {
        format!(
            "{}/relayed/replacement?offset=0&max_bytes=65536",
            node.url()
        )
    };

    // 1. The relay serves node 2 as node 3: nobody fences, writes go on.
    relay.point(nodes[1].port, Open::Closed);
    let load = WriteLoad::start(&client, &stream, "misdelivered");
    tokio::time::sleep(Duration::from_secs(3)).await;
    let misdelivered = load.stop().await;
    assert!(
        !misdelivered.is_empty(),
        "writes go on while the relay serves node 2"
    );
    acknowledged.extend(misdelivered);
    for node in &nodes {
        let (_, report) = readiness(&client, node).await.expect("readiness");
        assert!(
            !matches!(
                report.reason,
                Some(ServingReadinessReason::Superseded | ServingReadinessReason::DiskLeaseLost)
            ),
            "node {} fenced itself: {report:?}",
            node.id
        );
    }
    relay.point(nodes[2].port, Open::Closed);
    wait_readiness_reason(&client, &mut procs, &nodes[2], None).await;

    // 2. Node 3's old process stops with its WAL intact, its root is
    // released, and a replacement starts on an empty WAL behind the relay.
    let old_root = bound_root(&cluster, 3);
    let old = procs.0.remove(&3).expect("node 3 runs");
    signal(&old, "-STOP");
    let ctl = ursula_ctl::MetricsClient::new(Duration::from_secs(5)).expect("ctl client");
    let ctl_nodes = nodes.iter().map(MetaNode::ctl).collect::<Vec<_>>();
    let released = ursula_ctl::placement::replace_disk(&ctl, &ctl_nodes, 3, old_root, true)
        .await
        .expect("release node 3's root while its old process is stopped");
    assert_eq!(
        released.record.root,
        ursula_proto::placement::RootSlot::Released(old_root)
    );
    let old_node = nodes.pop().expect("node 3");
    let replacement = MetaNode {
        id: 3,
        port: free_port(),
        admin_port: free_port(),
        config: root.path().join("node-3-replacement.toml"),
        wal: root.path().join("node-3-replacement-log"),
        cold_root: None,
    };
    nodes.push(replacement);
    let replacement = &nodes[2];
    replacement.write(&peers_for(&nodes, replacement), meta);
    procs.start(binary, replacement);
    relay.point(replacement.port, Open::Kept);
    let load = WriteLoad::start(&client, &stream, "replaced");
    signal(&old, "-CONT");

    // 3. The continued old process reports `superseded`, and the
    // replacement serves once it waited out its predecessor.
    // A fenced process answers every client request `503`, so its
    // readiness is read on its admin listener.
    let mut old_procs = Processes::default();
    old_procs.0.insert(3, old);
    eventually(
        &mut old_procs,
        "the old process reports superseded",
        async || {
            let url = format!("{}/__ursula/ready", old_node.admin());
            let report: ursula_proto::admin::ServingReadiness = client
                .get(&url)
                .send()
                .await
                .map_err(|err| format!("GET {url}: {err}"))?
                .json()
                .await
                .map_err(|err| format!("GET {url}: {err}"))?;
            if report.reason == Some(ServingReadinessReason::Superseded) {
                Ok(())
            } else {
                Err(format!("{report:?}"))
            }
        },
    )
    .await;
    wait_readiness_reason(&client, &mut procs, replacement, None).await;
    let replaced = load.stop().await;
    assert!(!replaced.is_empty(), "writes go on around the replacement");
    acknowledged.extend(replaced);

    // 4. Node 1 is killed: no acknowledged write is lost, on node 2 or on
    // the replacement.
    procs.kill(1);
    read_until_holding(&client, &read(&nodes[1]), &acknowledged).await;
    read_until_holding(&client, &read(&nodes[2]), &acknowledged).await;
}

/// Whether a relay's open connections stay where they were opened.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Open {
    Kept,
    Closed,
}

/// A TCP relay on a port of its own: each connection goes to the port it
/// points at when the connection opens.
struct Relay {
    port: u16,
    target: Arc<Mutex<u16>>,
    closed: tokio::sync::watch::Sender<u64>,
    task: tokio::task::JoinHandle<()>,
}

impl Relay {
    async fn start(target: u16) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind the relay");
        let port = listener.local_addr().expect("relay address").port();
        let target = Arc::new(Mutex::new(target));
        let (closed, closes) = tokio::sync::watch::channel(0_u64);
        let current = Arc::clone(&target);
        let task = tokio::spawn(async move {
            while let Ok((mut inbound, _)) = listener.accept().await {
                let to = *current.lock().unwrap();
                let mut closes = closes.clone();
                closes.mark_unchanged();
                tokio::spawn(async move {
                    let Ok(mut outbound) = tokio::net::TcpStream::connect(("127.0.0.1", to)).await
                    else {
                        return;
                    };
                    tokio::select! {
                        _copied = tokio::io::copy_bidirectional(&mut inbound, &mut outbound) => {}
                        _closed = closes.changed() => {}
                    }
                });
            }
        });
        Self {
            port,
            target,
            closed,
            task,
        }
    }

    fn url(&self) -> String {
        format!("http://127.0.0.1:{}", self.port)
    }

    /// Sends the relay's new connections to `target`, and closes its open
    /// ones unless `open` keeps them.
    fn point(&self, target: u16, open: Open) {
        *self.target.lock().unwrap() = target;
        if open == Open::Closed {
            self.closed
                .send_modify(|closes| *closes = closes.saturating_add(1));
        }
    }
}

impl Drop for Relay {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// Appends to a stream every 20 ms until stopped, keeping each write that
/// was acknowledged.
struct WriteLoad {
    stop: Arc<std::sync::atomic::AtomicBool>,
    task: tokio::task::JoinHandle<Vec<String>>,
}

impl WriteLoad {
    /// Writes `{prefix}-{n};` to `stream`.
    fn start(client: &reqwest::Client, stream: &str, prefix: &'static str) -> Self {
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let task = {
            let client = client.clone();
            let stream = stream.to_owned();
            let stop = Arc::clone(&stop);
            tokio::spawn(async move {
                let mut acknowledged = Vec::new();
                for write in 0_u32.. {
                    if stop.load(std::sync::atomic::Ordering::Acquire) {
                        break;
                    }
                    let payload = format!("{prefix}-{write};");
                    let response = client
                        .post(&stream)
                        .header("content-type", "text/plain")
                        .body(payload.clone())
                        .send()
                        .await;
                    if response
                        .is_ok_and(|response| response.status() == reqwest::StatusCode::NO_CONTENT)
                    {
                        acknowledged.push(payload);
                    }
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
                acknowledged
            })
        };
        Self { stop, task }
    }

    /// Stops the load once its write in flight answered, and answers the
    /// acknowledged writes in order.
    async fn stop(self) -> Vec<String> {
        self.stop.store(true, std::sync::atomic::Ordering::Release);
        self.task.await.expect("the write load ends")
    }
}

/// Sends `signal` (`-STOP`, `-CONT`) to `child`.
fn signal(child: &ChildGuard, signal: &str) {
    let status = Command::new("kill")
        .arg(signal)
        .arg(child.child.id().to_string())
        .status()
        .expect("send a signal");
    assert!(status.success(), "kill {signal}");
}

/// The committed index of each data group, as its leader among `nodes`
/// reports it.
async fn leaders_committed(client: &reqwest::Client, nodes: &[MetaNode]) -> BTreeMap<u64, u64> {
    let mut committed = BTreeMap::new();
    for node in nodes {
        let Ok(metrics) = try_node_metrics(client, &node.url()).await else {
            continue;
        };
        for group in &metrics.groups {
            if group.current_leader == Some(node.id)
                && let Some(index) = group.committed_index
            {
                committed.insert(group.raft_group_id, index);
            }
        }
    }
    committed
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

/// Releases `root`, node `node`'s lost disk, through another node, as
/// `ursulactl node replace-disk` does once the disk is revoked.
async fn replace_lost_disk(
    nodes: &[ursula_ctl::NodeInfo],
    node: u64,
    root: ursula_proto::placement::RootId,
) {
    let ctl = ursula_ctl::MetricsClient::new(Duration::from_secs(5)).expect("ctl client");
    let deadline = std::time::Instant::now()
        .checked_add(Duration::from_secs(60))
        .unwrap();
    loop {
        match ursula_ctl::placement::replace_disk(&ctl, nodes, node, root, false).await {
            Ok(outcome) => {
                assert_eq!(
                    outcome.record.root,
                    ursula_proto::placement::RootSlot::Released(root)
                );
                return;
            }
            Err(error) => assert!(
                std::time::Instant::now() < deadline,
                "replace-disk of node {node}: {error:?}"
            ),
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

fn ctl_node(node_id: u64, admin_port: u16, public_url: &str) -> ursula_ctl::NodeInfo {
    ursula_ctl::NodeInfo {
        id: node_id,
        admin_url: url::Url::parse(&format!("http://127.0.0.1:{admin_port}")).expect("admin url"),
        host: "127.0.0.1".to_owned(),
        http_url: Some(url::Url::parse(public_url).expect("public url")),
        metrics_url: None,
        client_url: None,
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

/// Port of 1-based `node_id` in `ports`, one per node.
fn node_port(ports: &[u16], node_id: u64) -> u16 {
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
    wal_path: &Path,
    cold_backend: &str,
    cold_root: Option<&str>,
) -> u16 {
    let admin_port = free_port();
    NodeToml {
        port,
        admin_port,
        node_id,
        raft_group_count,
        peers,
        wal_path,
        cold_backend,
        cold_root,
        meta: MetaToml::default(),
    }
    .write(path);
    admin_port
}

/// The meta, placement and cold-flush keys of a node config. `None` leaves
/// a key out, so the server's default applies.
#[derive(Debug, Clone, Copy, Default)]
struct MetaToml<'a> {
    /// `raft.meta_voters`.
    meta_voters: Option<&'a [u64]>,
    /// `raft.placement.replication_factor`.
    replication_factor: Option<u8>,
    /// `storage.cold.flush_min_hot_size`, in bytes.
    flush_min_hot_bytes: Option<u64>,
}

/// One node's config file, of a cluster of any number of nodes.
struct NodeToml<'a> {
    port: u16,
    admin_port: u16,
    node_id: u64,
    raft_group_count: usize,
    peers: &'a [(u64, String)],
    wal_path: &'a Path,
    cold_backend: &'a str,
    cold_root: Option<&'a str>,
    meta: MetaToml<'a>,
}

impl NodeToml<'_> {
    fn write(&self, path: &Path) {
        use std::fmt::Write;

        let Self {
            port,
            admin_port,
            node_id,
            raft_group_count,
            peers,
            wal_path,
            cold_backend,
            cold_root,
            meta,
        } = self;
        let mut config = String::new();

        writeln!(
            config,
            r#"[server]
listen = "127.0.0.1:{port}"
admin_listen = "127.0.0.1:{admin_port}"
cluster_id = "static-cluster-cli"
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
"#
        )
        .unwrap();
        if let Some(voters) = meta.meta_voters {
            writeln!(config, "meta_voters = {voters:?}\n").unwrap();
        }

        writeln!(
            config,
            r#"[raft.wal]
path = "{}"
"#,
            wal_path.display()
        )
        .unwrap();

        if let Some(factor) = meta.replication_factor {
            writeln!(config, "[raft.placement]\nreplication_factor = {factor}\n").unwrap();
        }

        for (peer_id, peer_url) in *peers {
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
        if let Some(bytes) = meta.flush_min_hot_bytes {
            writeln!(config, "flush_min_hot_size = {bytes}").unwrap();
        }

        if let Some(root) = cold_root {
            writeln!(config, r#"root = "{root}""#).unwrap();
        }

        if *cold_backend == "s3" {
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
    }
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

/// The client and cluster ports of one node of a split-plane cluster.
struct SplitPlanePorts {
    node_id: u64,
    client: u16,
    cluster: u16,
    admin: u16,
}

impl SplitPlanePorts {
    fn client_url(&self) -> String {
        format!("http://127.0.0.1:{}", self.client)
    }

    fn cluster_url(&self) -> String {
        format!("http://127.0.0.1:{}", self.cluster)
    }
}

/// Spawns node `node_id` of `planes` with `server.cluster_listen` set: its
/// peers dial the cluster ports and redirect clients to the client ports.
fn spawn_split_plane_node(
    binary: &str,
    planes: &[SplitPlanePorts],
    node_id: u64,
    wal_root: &Path,
) -> ChildGuard {
    use std::fmt::Write;

    let own = planes
        .iter()
        .find(|plane| plane.node_id == node_id)
        .expect("node in planes");
    let admin_port = own.admin;
    let mut config = format!(
        r#"[server]
listen = "127.0.0.1:{client}"
cluster_listen = "127.0.0.1:{cluster}"
admin_listen = "127.0.0.1:{admin_port}"
cluster_id = "static-cluster-cli"

[runtime]
core_count = 1

[raft]
node_id = {node_id}
group_count = 4

[raft.wal]
path = "{wal}"

[storage.cold]
backend = "memory"
"#,
        client = own.client,
        cluster = own.cluster,
        wal = node_wal_dir(wal_root, node_id).display(),
    );
    for plane in planes {
        writeln!(
            config,
            r#"
[[raft.peers]]
node_id = {}
url = "{}"
client_url = "{}""#,
            plane.node_id,
            plane.cluster_url(),
            plane.client_url()
        )
        .unwrap();
    }
    let config_path = std::env::temp_dir().join(format!(
        "ursula-split-node-{node_id}-{}-{}.toml",
        own.client,
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time after unix epoch")
            .as_nanos()
    ));
    std::fs::write(&config_path, config).expect("write split-plane node config");
    let mut command = Command::new(binary);
    command.arg("server").arg("--config").arg(&config_path);
    let mut guard = spawn_child(command, format!("split-node-{node_id}-{}", own.client));
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
    children.iter_mut().find_map(exited_report)
}

/// `child`'s report once it exited, `None` while it runs.
fn exited_report(child: &mut ChildGuard) -> Option<String> {
    match child.child.try_wait() {
        Ok(Some(status)) => Some(format!(
            "{} exited with {status}; {}",
            child.label,
            child_report(child)
        )),
        Ok(None) => None,
        Err(err) => Some(format!(
            "{} try_wait failed: {err}; {}",
            child.label,
            child_report(child)
        )),
    }
}

fn write_single_node_cluster_config(
    path: &Path,
    port: u16,
    node_id: u64,
    raft_group_count: usize,
    base_url: &str,
    log_dir: &Path,
) -> u16 {
    write_cluster_config(
        path,
        port,
        node_id,
        raft_group_count,
        &[(node_id, base_url.to_owned())],
        log_dir,
    )
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
    log_dir: &Path,
) -> u16 {
    write_node_toml(
        path,
        port,
        node_id,
        raft_group_count,
        peers,
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
    // 15 s: after a leader is killed, an election and a read on a loaded
    // runner.
    let mut last = Vec::new();
    for _ in 0..300 {
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

/// Asks for an admin purge, again while the group's snapshot work is not
/// idle: the route answers 503 with `Retry-After` then, as when the snapshot
/// driver builds the group (#669).
async fn admin_purge(client: &reqwest::Client, url: String) -> reqwest::Response {
    for _ in 0..100 {
        let response = admin_test_post(client, url.clone())
            .await
            .send()
            .await
            .expect("send an admin purge");
        if response.status() != reqwest::StatusCode::SERVICE_UNAVAILABLE {
            return response;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("the admin purge at {url} stayed unavailable");
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
    ursula_raft::wal::diagnostics::journal_segments(core_dir)
        .expect("list the core journal segments")
        .iter()
        .map(|(_, path)| {
            std::fs::metadata(path)
                .map(|metadata| metadata.len().saturating_sub(32))
                .unwrap_or(0)
        })
        .sum()
}

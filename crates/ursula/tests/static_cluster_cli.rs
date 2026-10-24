#![expect(
    clippy::unwrap_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::string_slice,
    reason = "integration tests assert by panicking, as clippy.toml allows for unit tests"
)]
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashMap;
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
use ursula_control::NodeEligibility;
use ursula_control::PlacementStatus;
use ursula_control::RegistrationStatus;
use ursula_control::Target;
use ursula_ctl::placement::moves::GroupEnd;
use ursula_ctl::placement::moves::Intent;
use ursula_ctl::placement::moves::MoveOptions;
use ursula_ctl::placement::moves::Replacement;
use ursula_ctl::placement::moves::run_moves;
use ursula_proto::admin::RaftGroupMetrics;
use ursula_proto::admin::RecoveryGateStatus;
use ursula_proto::placement::NodeId;
use ursula_proto::placement::PlacementEpoch;
use ursula_proto::placement::Registered;
use ursula_proto::placement::VoterSet;
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

    // The egress gate probes every 500 ms. Give it several ticks, then every
    // node's last probe must be clean: its peers answered the probe on their
    // cluster ports. A probe sent to the wrong port fails on every node, and
    // since no node then does better, none would shed, so the node's own
    // verdict is what shows it.
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
        assert_eq!(
            status.egress,
            ursula_proto::admin::EgressProbeStatus::Clean,
            "node {}'s egress probe: {status:?}",
            plane.node_id
        );
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
    // The wait covers the group turning live, once its leader reported its
    // voters to meta.
    ursula_ctl::wait_cluster_ready(
        "single-voter verification",
        nodes,
        &client,
        Duration::from_secs(30),
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

/// The bytes every record of a stream carries whose data a test checks is
/// gone from a node: the deleted-data tests' stream, and the stream of the
/// group the teardown test moves off its source.
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
    snapshot_group(client, admin, 0).await
}

/// Asks node `admin`'s replica of group `group` for a snapshot and returns
/// its index.
async fn snapshot_group(client: &reqwest::Client, admin: &str, group: u32) -> u64 {
    let response = admin_test_post(client, format!("{admin}/__ursula/raft/{group}/snapshot"))
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
/// trailers. It can delay snapshot transfer, and refuse every other request
/// as a peer it cannot reach. Metrics and readiness use the server's real
/// listeners directly.
struct RaftProxy {
    url: String,
    release: tokio::sync::watch::Sender<bool>,
    blocked: tokio::sync::watch::Receiver<bool>,
    /// While `true`, every request but a snapshot transfer answers `502`.
    refuse: tokio::sync::watch::Sender<bool>,
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
        let (refuse, refused) = tokio::sync::watch::channel(false);
        let bad_gateway = || {
            axum::http::Response::builder()
                .status(axum::http::StatusCode::BAD_GATEWAY)
                .body(axum::body::Body::empty())
                .unwrap()
        };
        let proxy = tower::service_fn(move |request: axum::http::Request<axum::body::Body>| {
            let channel = channel.clone();
            let mut released = released.clone();
            let blocked_tx = blocked_tx.clone();
            let refused = *refused.borrow();
            async move {
                if request.uri().path() == ursula_raft::RAFT_GRPC_FULL_SNAPSHOT_PATH {
                    if !*released.borrow() {
                        blocked_tx.send_replace(true);
                    }
                    released.wait_for(|ready| *ready).await.unwrap();
                } else if refused {
                    return Ok::<_, std::convert::Infallible>(bad_gateway());
                }
                let response = match channel.oneshot(request.map(tonic::body::boxed)).await {
                    Ok(response) => response.map(axum::body::Body::new),
                    Err(_transport) => bad_gateway(),
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
            refuse,
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
    // Every group runs on nodes 1 to 3 in the meta leader's view, which the
    // drain moved off node 3. With node 3 down the full proof refuses its
    // missing voter, and a proof that excludes node 2 leaves node 1 alone
    // and refuses node 3 too.
    let quorum_refusal = |error: anyhow::Error| {
        error
            .downcast_ref::<ursula_ctl::view::GroupRefusal>()
            .cloned()
            .unwrap_or_else(|| panic!("a typed group refusal: {error:#}"))
    };
    for refused in [
        ursula_ctl::quorum::verify_quorum(&nodes, &ctl, &options)
            .await
            .map(|_proof| ()),
        ursula_ctl::quorum::verify_surviving_quorum(&nodes, 2, &ctl, &options)
            .await
            .map(|_proof| ()),
    ] {
        assert!(matches!(
            quorum_refusal(refused.expect_err("node 3 is down")),
            ursula_ctl::view::GroupRefusal::Unobserved { node: 3, .. }
        ));
    }
    let survivors = ursula_ctl::quorum::verify_surviving_quorum(&nodes, 3, &ctl, &options)
        .await
        .expect("with the drained source stopped, both survivors are eligible");
    assert_eq!(survivors.surviving_node_ids, BTreeSet::from([1, 2]));
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
                ursula_ctl::observe::check_maintenance_snapshot(&snapshot),
                Err(ursula_ctl::observe::ReadinessRefusal::Maintenance { .. })
            ));
            let refusal = ursula_ctl::wait_ready(
                &readiness_client,
                &nodes,
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
        if ursula_ctl::plan::check_readiness(&snapshot, &(0..6).collect(), 3, 0).all_ready {
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
    // Every group is live on its three target voters again, as the meta
    // leader's view shows it.
    let (_ready, view) = ursula_ctl::wait_ready(
        &ctl,
        &nodes,
        Duration::from_secs(60),
        Duration::from_millis(100),
    )
    .await
    .expect("every group is live on nodes 1 to 3");
    assert_eq!(
        view.targets().expect("no group is seeded or moving"),
        (0..6)
            .map(|group| (group, BTreeSet::from([1, 2, 3])))
            .collect()
    );
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

/// A running cluster of three per-group nodes ([`spawn_per_group_node`]),
/// each ready, with its WAL root.
struct PerGroupCluster {
    children: Vec<ChildGuard>,
    nodes: Vec<ursula_ctl::NodeInfo>,
    peers: Vec<(u64, String)>,
    _wal_root: tempfile::TempDir,
}

impl PerGroupCluster {
    async fn start(binary: &str, client: &reqwest::Client) -> Self {
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
        for (_, url) in &peers {
            wait_until_ready(client, url, &mut children).await;
        }
        Self {
            children,
            nodes,
            peers,
            _wal_root: wal_root,
        }
    }
}

/// Waits until every one of `nodes` names a leader for each of the six
/// groups of a per-group cluster and is ready for maintenance, and answers
/// that view.
async fn wait_per_group_leaders(
    ctl: &ursula_ctl::MetricsClient,
    nodes: &[ursula_ctl::NodeInfo],
) -> ursula_ctl::ClusterSnapshot {
    let deadline = std::time::Instant::now()
        .checked_add(Duration::from_secs(60))
        .unwrap();
    loop {
        let snapshot = ctl.fetch_cluster(nodes).await.expect("fetch cluster");
        if nodes.iter().all(|node| {
            snapshot.node(node.id).is_some_and(|view| {
                view.groups.len() == 6
                    && view
                        .groups
                        .iter()
                        .all(|group| group.current_leader.is_some())
            }) && ursula_ctl::plan::check_readiness(&snapshot, &(0..6).collect(), node.id, 0)
                .all_ready
        }) {
            return snapshot;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "cluster not caught up: {snapshot:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cli_sigterm_hands_off_leaders_and_bounds_quorum_loss() {
    let _guard = static_cluster_cli_test_guard().await;
    let binary = env!("CARGO_BIN_EXE_ursula");
    let client = reqwest::Client::new();
    let PerGroupCluster {
        mut children,
        mut nodes,
        peers,
        _wal_root,
    } = PerGroupCluster::start(binary, &client).await;
    for index in 0..6 {
        put_with_body_until_created(
            &client,
            &format!("{}/benchcmp/sigterm-{index}", peers[0].1),
            "acknowledged-before-sigterm",
        )
        .await;
    }
    let ctl = ursula_ctl::MetricsClient::new(Duration::from_secs(2)).expect("ctl client");
    let snapshot = wait_per_group_leaders(&ctl, &nodes).await;
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

/// A voter that stops while no peer may take its leaderships cannot hand
/// them off: both peers answer that they are drained. Once its handoff
/// grace ends it steps down from them, so its peers, undrained then, elect
/// leaders while it still drains its listeners (a live read it serves
/// holds the drain open), not only once its Raft stops.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cli_sigterm_steps_down_from_leaderships_it_could_not_hand_off() {
    let _guard = static_cluster_cli_test_guard().await;
    let binary = env!("CARGO_BIN_EXE_ursula");
    let client = reqwest::Client::new();
    let PerGroupCluster {
        mut children,
        nodes,
        peers,
        _wal_root,
    } = PerGroupCluster::start(binary, &client).await;
    let map = ursula_shard::StaticShardMap::new(1, 6).expect("shard map");
    let streams: BTreeMap<u32, String> = (0_u32..)
        .map(|candidate| format!("step-down-{candidate}"))
        .map(|stream| {
            let group = map
                .locate(&ursula_shard::BucketStreamId::new("benchcmp", &stream))
                .raft_group_id;
            (group.0, stream)
        })
        .scan(BTreeMap::new(), |found, (group, stream)| {
            found.entry(group).or_insert(stream);
            Some(found.clone())
        })
        .find(|found| found.len() == 6)
        .expect("a stream of every group");
    for stream in streams.values() {
        put_with_body_until_created(
            &client,
            &format!("{}/benchcmp/{stream}", peers[0].1),
            "before-sigterm",
        )
        .await;
    }
    let ctl = ursula_ctl::MetricsClient::new(Duration::from_secs(2)).expect("ctl client");
    let snapshot = wait_per_group_leaders(&ctl, &nodes).await;
    let leader_of = |node_id: u64| -> Vec<u64> {
        snapshot
            .node(node_id)
            .unwrap()
            .groups
            .iter()
            .filter(|group| group.current_leader == Some(node_id))
            .map(|group| group.raft_group_id)
            .collect()
    };
    let index = (0..nodes.len())
        .max_by_key(|index| leader_of(nodes[*index].id).len())
        .unwrap();
    let outgoing = nodes[index].id;
    let led = leader_of(outgoing);
    let held_group = u32::try_from(*led.first().expect("the outgoing node leads a group")).unwrap();
    let peers_left: Vec<ursula_ctl::NodeInfo> = nodes
        .iter()
        .filter(|node| node.id != outgoing)
        .cloned()
        .collect();
    for peer in &peers_left {
        ctl.set_maintenance_drain(peer, true)
            .await
            .expect("drain a peer");
    }
    // A live read the outgoing node serves as its group's leader keeps its
    // listener drain going for the drain's whole bound.
    let live_read = client
        .get(format!(
            "{}/benchcmp/{}?offset=-1&live=sse",
            peers[index].1, streams[&held_group]
        ))
        .send()
        .await
        .expect("open a live read");
    assert_eq!(live_read.status(), reqwest::StatusCode::OK);
    let status = Command::new("kill")
        .arg("-TERM")
        .arg(children[index].child.id().to_string())
        .status()
        .expect("send SIGTERM");
    assert!(status.success());
    let log_path = children[index].stderr_path.clone();
    let read_log = || std::fs::read_to_string(&log_path).unwrap_or_default();
    let deadline = std::time::Instant::now()
        .checked_add(Duration::from_secs(15))
        .unwrap();
    while !read_log().contains("shutdown leadership handoff incomplete") {
        assert!(
            std::time::Instant::now() < deadline,
            "the handoff did not end incomplete: {}",
            read_log()
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let gave_up = std::time::Instant::now();
    for peer in &peers_left {
        ctl.set_maintenance_drain(peer, false)
            .await
            .expect("undrain a peer");
    }
    // An election timeout (1.5 to 3 s), a second election round and the
    // undrain round trip, inside the 10 s listener drain the live read holds
    // open, after which the stopping node's Raft stops anyway.
    let elected_by = gave_up.checked_add(Duration::from_secs(8)).unwrap();
    loop {
        let snapshot = ctl.fetch_cluster(&peers_left).await.expect("fetch peers");
        let moved = led.iter().all(|group_id| {
            snapshot.per_node.iter().all(|view| {
                view.group(*group_id)
                    .and_then(|group| group.current_leader)
                    .is_some_and(|leader| leader != outgoing)
            })
        });
        if moved {
            break;
        }
        assert!(
            std::time::Instant::now() < elected_by,
            "the peers still follow the stopping node: {snapshot:?}\n{}",
            read_log()
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    drop(live_read);
    let log = read_log();
    assert!(
        log.contains("shutdown: stepped down from the leaderships the handoff did not move"),
        "{log}"
    );
    assert!(
        log.contains("no peer can take over these leaderships now"),
        "{log}"
    );
    assert!(
        log.contains("these leaderships were not handed off"),
        "{log}"
    );
    let mut outgoing_child = children.remove(index);
    let exit_by = std::time::Instant::now()
        .checked_add(Duration::from_secs(20))
        .unwrap();
    let exit = loop {
        if let Some(status) = outgoing_child.child.try_wait().expect("poll child") {
            break status;
        }
        assert!(
            std::time::Instant::now() < exit_by,
            "the stopping node did not exit: {}",
            child_report(&outgoing_child)
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    assert!(exit.success(), "unclean shutdown: {exit}; {}", read_log());
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
        self.write_with_groups(peers, meta, META_TEST_GROUPS);
    }

    /// [`Self::write`], with `groups` data groups.
    fn write_with_groups(&self, peers: &[(u64, String)], meta: MetaToml<'_>, groups: usize) {
        NodeToml {
            port: self.port,
            admin_port: self.admin_port,
            node_id: self.id,
            raft_group_count: groups,
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

    /// [`Self::stop`], waiting up to `budget` for the exit.
    async fn stop_within(&mut self, id: u64, budget: Duration) {
        let mut child = self.0.remove(&id).expect("the node runs");
        sigterm_and_wait_for_clean_exit_within(&mut child, budget).await;
    }

    /// Kills node `id`: it hands nothing off.
    fn kill(&mut self, id: u64) {
        drop(self.0.remove(&id).expect("the node runs"));
    }

    /// Sends `signal` to node `id`: `-STOP` blackholes it, its sockets open
    /// and nothing answered, until `-CONT`.
    fn signal(&self, id: u64, which: &str) {
        signal(self.0.get(&id).expect("the node runs"), which);
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
    probe: impl AsyncFnMut() -> Result<T, String>,
) -> T {
    eventually_for(procs, what, META_WAIT, probe).await
}

/// [`eventually`], failing after `wait`.
async fn eventually_for<T>(
    procs: &mut Processes,
    what: &str,
    wait: Duration,
    mut probe: impl AsyncFnMut() -> Result<T, String>,
) -> T {
    let deadline = std::time::Instant::now().checked_add(wait).unwrap();
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

/// Every group's current voters.
fn current_voters(cluster: &Cluster) -> BTreeMap<RaftGroupId, BTreeSet<u64>> {
    cluster
        .groups()
        .iter()
        .map(|(group, placement)| (group, node_ids(placement.current_voters().iter())))
        .collect()
}

/// The groups whose current voters list node `node`.
fn placed_on(cluster: &Cluster, node: u64) -> BTreeSet<u64> {
    current_voters(cluster)
        .into_iter()
        .filter(|(_, voters)| voters.contains(&node))
        .map(|(group, _)| u64::from(group.0))
        .collect()
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

/// Whether `shed` lists `reason` among the reasons its node sheds for.
fn sheds(
    shed: &ursula_proto::admin::LeadershipShedStatus,
    reason: ursula_raft::LeadershipShedReason,
) -> bool {
    shed.state
        .split('|')
        .any(|listed| listed == reason.as_str())
}

fn sheds_placement_unavailable(shed: &ursula_proto::admin::LeadershipShedStatus) -> bool {
    sheds(
        shed,
        ursula_raft::LeadershipShedReason::PlacementUnavailable,
    )
}

/// Each node's participation, sampled every 500 ms from its own listener
/// while a phase of a test runs.
struct ParticipationSampler {
    stop: Arc<std::sync::atomic::AtomicBool>,
    task: tokio::task::JoinHandle<Vec<(u64, ursula_proto::admin::LeadershipShedStatus)>>,
}

impl ParticipationSampler {
    fn start(client: &reqwest::Client, nodes: &[MetaNode]) -> Self {
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let urls: Vec<(u64, String)> = nodes
            .iter()
            .map(|node| (node.id, format!("{}/__ursula/leadership-shed", node.url())))
            .collect();
        let task = {
            let client = client.clone();
            let stop = Arc::clone(&stop);
            tokio::spawn(async move {
                let mut samples = Vec::new();
                while !stop.load(std::sync::atomic::Ordering::Acquire) {
                    for (id, url) in &urls {
                        let Ok(response) = client.get(url).send().await else {
                            continue;
                        };
                        if let Ok(shed) = response.json().await {
                            samples.push((*id, shed));
                        }
                    }
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
                samples
            })
        };
        Self { stop, task }
    }

    /// Stops sampling, and answers every sample with the node it is of.
    async fn stop(self) -> Vec<(u64, ursula_proto::admin::LeadershipShedStatus)> {
        self.stop.store(true, std::sync::atomic::Ordering::Release);
        self.task.await.expect("the sampler ends")
    }
}

/// Asserts that over a uniform delay on every node's path, as `samples`
/// show it, every node of `nodes` reported a degraded egress probe and no
/// node ever shed for cluster egress: no peer does better, and every peer
/// still answers.
fn assert_degraded_without_egress_shed(
    nodes: &[MetaNode],
    samples: &[(u64, ursula_proto::admin::LeadershipShedStatus)],
) {
    for node in nodes {
        assert!(
            samples.iter().any(|(id, shed)| *id == node.id
                && shed.egress == ursula_proto::admin::EgressProbeStatus::Degraded),
            "node {} never reported a degraded egress probe over the delay: {samples:?}",
            node.id
        );
    }
    let shedding: Vec<&(u64, ursula_proto::admin::LeadershipShedStatus)> = samples
        .iter()
        .filter(|(_, shed)| sheds(shed, ursula_raft::LeadershipShedReason::ClusterEgress))
        .collect();
    assert!(
        shedding.is_empty(),
        "nodes shed for cluster egress over a uniform delay: {shedding:?}"
    );
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
/// `raft.peers` grew, and nodes 4 and 5, in no group's seeds, host nothing,
/// are ready with no group, and route a write to a node that hosts its
/// group, naming each of its voters in turn. A seed rule read over the
/// grown `raft.peers`, or a readiness that needs a replica, fails it.
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
    // Node 1's view, read after `raft.peers` grew, keeps every group on the
    // genesis peers.
    let grown = wait_placement(
        &client,
        &mut procs,
        &nodes[0],
        "shows every group live",
        |status| settled(status).is_some(),
    )
    .await;
    for (group, voters) in current_voters(settled(&grown).unwrap()) {
        assert_eq!(
            voters,
            BTreeSet::from([1, 2, 3]),
            "group {group:?}: {grown:?}"
        );
    }
    // The late nodes are ready, and their reports place them in no group.
    for node in &nodes[3..] {
        let ready = wait_readiness_reason(&client, &mut procs, node, None).await;
        let report = ready.raft_maintenance.expect("a maintenance report");
        assert!(report.ready(), "node {}: {report:?}", node.id);
        assert!(
            report.expected_groups.is_empty()
                && report.unplaced.is_empty()
                && report.group_issues.is_empty(),
            "node {}: {report:?}",
            node.id
        );
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

/// Integrations 2 and 6 (#768, design 15.3), on five nodes at replication
/// factor 3 with meta voters 1 to 3, so each node hosts some of the groups.
/// After every process is killed and started again, every node's view keeps
/// each group's voters and placement key, each node runs exactly the groups
/// placed on it, and every node reads every write. Then meta voters 1 and 2
/// are killed, which leaves meta no quorum: the group seeded on {3, 4, 5}
/// keeps taking writes, and node 4, killed and started again while meta
/// stays down, runs exactly the groups its journal holds, routes a write,
/// applies it on its own replica, and a read through it returns every write
/// of that group. A restart that hosts by anything but
/// the placement its journal and view hold fails it, and so does a data
/// group, or a restarted node's open, that needs meta to serve (G2).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cli_five_nodes_keep_their_placement_across_a_full_restart_and_serve_without_meta() {
    let _guard = static_cluster_cli_test_guard().await;
    let Some(binary) = option_env!("CARGO_BIN_EXE_ursula") else {
        tracing::warn!("CARGO_BIN_EXE_ursula is not set; skipping the meta test");
        return;
    };
    let root = tempfile::tempdir().expect("temp root");
    let nodes = MetaNode::cluster(root.path(), 5);
    let peers = peers_of(&nodes);
    let meta = MetaToml {
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
    let formed = wait_settled(&client, &mut procs, &nodes).await;
    let cluster = settled(&formed[0]).expect("settled").clone();
    for node in &nodes {
        runs_exactly(&client, &mut procs, node, &placed_on(&cluster, node.id)).await;
    }
    let streams = stream_per_group("placed");
    let url = |node: &MetaNode, stream: &str| format!("{}/placed/{stream}", node.url());
    let read =
        |node: &MetaNode, stream: &str| format!("{}?offset=0&max_bytes=256", url(node, stream));
    for stream in &streams {
        put_until_created(&client, &url(&nodes[0], stream)).await;
        post_until_no_content(&client, &url(&nodes[0], stream), "before-the-restart;").await;
    }

    // 1. Every process is killed and started again. The placement, the
    // hosting and the data are as before.
    for node in &nodes {
        procs.kill(node.id);
    }
    for node in &nodes {
        procs.start(binary, node);
    }
    for status in wait_settled(&client, &mut procs, &nodes).await {
        let restarted = settled(&status).unwrap();
        assert_eq!(restarted.params(), cluster.params(), "{status:?}");
        assert_eq!(sinces(restarted), sinces(&cluster), "{status:?}");
        assert_eq!(
            current_voters(restarted),
            current_voters(&cluster),
            "{status:?}"
        );
    }
    for node in &nodes {
        runs_exactly(&client, &mut procs, node, &placed_on(&cluster, node.id)).await;
    }
    for node in &nodes {
        for stream in &streams {
            read_until_matches(&client, &read(node, stream), b"before-the-restart;").await;
        }
    }

    // 2. Meta voters 1 and 2 die, and meta keeps no quorum. The group
    // seeded on {3, 4, 5} keeps taking writes.
    let (group, _) = current_voters(&cluster)
        .into_iter()
        .find(|(_, voters)| *voters == BTreeSet::from([3, 4, 5]))
        .unwrap_or_else(|| panic!("a group is seeded on 3, 4 and 5: {cluster:?}"));
    let stream = &streams[usize::try_from(group.0).unwrap()];
    procs.kill(1);
    procs.kill(2);
    post_until_no_content(&client, &url(&nodes[2], stream), "while-meta-is-down;").await;

    // 3. Node 4 restarts while meta stays down: it opens what its journal
    // holds, and serves once the group has a leader.
    procs.kill(4);
    procs.start(binary, &nodes[3]);
    runs_exactly(&client, &mut procs, &nodes[3], &placed_on(&cluster, 4)).await;
    let what = format!("node 4 knows the leader of group {}", group.0);
    eventually(&mut procs, &what, async || {
        let metrics = try_node_metrics(&client, &nodes[3].url()).await?;
        metrics
            .groups
            .iter()
            .find(|metrics| metrics.raft_group_id == u64::from(group.0))
            .and_then(|metrics| metrics.current_leader)
            .ok_or_else(|| format!("{:?}", metrics.groups))
    })
    .await;
    post_until_no_content(&client, &url(&nodes[3], stream), "after-node-4-restarted;").await;
    // The client follows redirects, and {3, 5} is a quorum without node 4,
    // so check node 4's own replica: it applies past the write. The leader
    // that acknowledged it is one of nodes 3 to 5, and its committed index
    // only grows, so the greatest of theirs covers the write.
    let group_metrics = |metrics: &ursula_proto::admin::NodeMetrics| {
        metrics
            .groups
            .iter()
            .find(|metrics| metrics.raft_group_id == u64::from(group.0))
            .cloned()
    };
    let mut written = None;
    for node in &nodes[2..] {
        let metrics = node_metrics(&client, &node.url()).await;
        written = written.max(group_metrics(&metrics).and_then(|group| group.committed_index));
    }
    let written = written.expect("the group's leader knows its committed entry");
    let what = format!("node 4 applies group {} up to index {written}", group.0);
    eventually(&mut procs, &what, async || {
        let metrics = try_node_metrics(&client, &nodes[3].url()).await?;
        let applied = group_metrics(&metrics).and_then(|group| group.last_applied_index);
        if applied >= Some(written) {
            Ok(())
        } else {
            Err(format!("node 4 applied {applied:?}"))
        }
    })
    .await;
    read_until_matches(
        &client,
        &read(&nodes[3], stream),
        b"before-the-restart;while-meta-is-down;after-node-4-restarted;",
    )
    .await;
}

/// How long a meta outage test watches the layout stay as it is: longer
/// than two ticks of the leadership balancer (5 s by default).
const STATIC_LAYOUT_WINDOW: Duration = Duration::from_secs(12);

/// The leader and term each live node's replica reports for each of
/// `groups`, by node and group.
async fn replica_leaders(
    client: &reqwest::Client,
    procs: &mut Processes,
    nodes: &[&MetaNode],
    groups: &BTreeSet<u64>,
) -> BTreeMap<(u64, u64), (Option<u64>, u64)> {
    let mut seen = BTreeMap::new();
    for node in nodes {
        let what = format!("node {} answers its metrics", node.id);
        let metrics = eventually(procs, &what, async || {
            try_node_metrics(client, &node.url()).await
        })
        .await;
        for group in metrics.groups {
            if groups.contains(&group.raft_group_id) {
                seen.insert(
                    (node.id, group.raft_group_id),
                    (group.current_leader, group.current_term),
                );
            }
        }
    }
    seen
}

/// Polls for [`STATIC_LAYOUT_WINDOW`] that every node of `nodes` answers
/// `/__ursula/ready` with `200` and that each of `groups` keeps the
/// leader and term every replica reported at the start, `leaders`.
async fn assert_layout_static(
    client: &reqwest::Client,
    procs: &mut Processes,
    nodes: &[&MetaNode],
    groups: &BTreeSet<u64>,
    leaders: &BTreeMap<(u64, u64), (Option<u64>, u64)>,
) {
    let deadline = std::time::Instant::now()
        .checked_add(STATIC_LAYOUT_WINDOW)
        .unwrap();
    while std::time::Instant::now() < deadline {
        if let Some(report) = procs.exited() {
            panic!("a node exited during the meta outage:\n{report}");
        }
        for node in nodes {
            let (status, ready) = readiness(client, node)
                .await
                .unwrap_or_else(|err| panic!("node {}'s readiness: {err}", node.id));
            assert_eq!(
                status,
                reqwest::StatusCode::OK,
                "node {} turned unready during the meta outage: {ready:?}",
                node.id
            );
        }
        let now = replica_leaders(client, procs, nodes, groups).await;
        assert_eq!(
            &now, leaders,
            "a leadership moved while the meta group had no quorum"
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

/// A meta outage leaves the layout static, and readiness reads only a
/// node's own state. Five nodes at replication factor 3 with meta voters 1
/// to 3.
///
/// 1. Meta voters 2 and 3 are frozen (`SIGSTOP`), so the meta group and
///    every group with two voters among them have no quorum. The groups
///    with a live quorum elect leaders on nodes 1, 4 and 5, and those
///    three nodes stay ready. For longer than two balancer ticks no
///    leadership of those groups moves and no term changes: the balancer
///    moves nothing without a meta read barrier.
/// 2. A live node that is a voter of a group with a quorum and of one
///    without restarts (`SIGTERM`, the normal handoff) while writes reach
///    the group with a quorum. It turns ready only once its replica of
///    that group holds what the group committed meanwhile and acknowledges
///    its leader, and its replica of the group without a leader waits only
///    the contact grace. No leadership moves to it, nor anywhere else, for
///    longer than two balancer ticks.
/// 3. Nodes 2 and 3 resume, and `ursulactl wait-ready` passes.
///
/// A node that turns unready because a group's quorum is lost on other
/// nodes, a balancer that moves leaderships without meta, or a restarted
/// node that turns ready before its replicas acknowledge their leaders
/// fails it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cli_a_meta_outage_keeps_the_layout_and_readiness_reads_only_the_node() {
    use ursula_proto::admin::LeaderAcknowledgement;

    let _guard = static_cluster_cli_test_guard().await;
    let Some(binary) = option_env!("CARGO_BIN_EXE_ursula") else {
        tracing::warn!("CARGO_BIN_EXE_ursula is not set; skipping the meta test");
        return;
    };
    let root = tempfile::tempdir().expect("temp root");
    let nodes = MetaNode::cluster(root.path(), 5);
    let peers = peers_of(&nodes);
    let meta = MetaToml {
        meta_voters: Some(META_VOTERS),
        replication_factor: Some(3),
        ..MetaToml::default()
    };
    let client = bounded_client();
    let mut procs = Processes::default();
    for node in &nodes {
        node.write(&peers, meta);
        procs.start(binary, node);
    }
    let formed = wait_settled(&client, &mut procs, &nodes).await;
    let cluster = settled(&formed[0]).expect("settled").clone();
    let streams = stream_per_group("outage");
    let url = |node: &MetaNode, stream: &str| format!("{}/outage/{stream}", node.url());
    for stream in &streams {
        put_until_created(&client, &url(&nodes[0], stream)).await;
        post_until_no_content(&client, &url(&nodes[0], stream), "before-the-outage;").await;
    }
    wait_leaders_settle(&client, &mut procs, &nodes).await;

    // The groups that keep a quorum on nodes 1, 4 and 5, and the ones that
    // lose it with nodes 2 and 3.
    let frozen = BTreeSet::from([2, 3]);
    let voters: BTreeMap<u64, BTreeSet<u64>> = current_voters(&cluster)
        .into_iter()
        .map(|(group, voters)| (u64::from(group.0), voters))
        .collect();
    let live_voters =
        |voters: &BTreeSet<u64>, down: &BTreeSet<u64>| voters.difference(down).count();
    let (with_quorum, without_quorum): (BTreeSet<u64>, BTreeSet<u64>) = voters
        .keys()
        .copied()
        .partition(|group| live_voters(&voters[group], &frozen) >= 2);
    assert!(
        !with_quorum.is_empty() && !without_quorum.is_empty(),
        "the seed placement {voters:?} gives groups of both kinds"
    );
    // A live node whose restart leaves every group with a quorum one, and
    // that is a voter of a group of each kind.
    let restarted = [1, 4, 5]
        .into_iter()
        .find(|node| {
            let down = frozen.iter().copied().chain([*node]).collect();
            with_quorum
                .iter()
                .all(|group| live_voters(&voters[group], &down) >= 2)
                && with_quorum.iter().any(|group| voters[group].contains(node))
                && without_quorum
                    .iter()
                    .any(|group| voters[group].contains(node))
        })
        .unwrap_or_else(|| panic!("no live node to restart in {voters:?}"));
    let live: Vec<&MetaNode> = nodes
        .iter()
        .filter(|node| !frozen.contains(&node.id))
        .collect();

    // 1. Two of the three meta voters freeze.
    for id in &frozen {
        procs.signal(*id, "-STOP");
    }
    eventually(
        &mut procs,
        "every group with a quorum has a live leader and nodes 1, 4 and 5 are ready",
        async || {
            for node in &live {
                let (status, ready) = readiness(&client, node).await?;
                if status != reqwest::StatusCode::OK {
                    return Err(format!("node {}: {status} {ready:?}", node.id));
                }
            }
            let mut led = BTreeSet::new();
            for node in &live {
                for group in try_node_metrics(&client, &node.url()).await?.groups {
                    if group.current_leader == Some(node.id) && group.leads_live {
                        led.insert(group.raft_group_id);
                    }
                }
            }
            if !with_quorum.is_subset(&led) {
                return Err(format!(
                    "live leaders of {led:?}, not of all {with_quorum:?}"
                ));
            }
            Ok(())
        },
    )
    .await;
    let leaders = replica_leaders(&client, &mut procs, &live, &with_quorum).await;
    assert_layout_static(&client, &mut procs, &live, &with_quorum, &leaders).await;

    // 2. A live node restarts during the outage while the groups with a
    // quorum take writes.
    // Its shutdown hands off what it can within the handoff grace, steps
    // down from the rest and drains connections that the frozen peers never
    // close for up to the drain bound, all within the 20 s shutdown grace.
    procs.stop_within(restarted, Duration::from_secs(25)).await;
    let others: Vec<&MetaNode> = live
        .iter()
        .copied()
        .filter(|node| node.id != restarted)
        .collect();
    let mut committed: BTreeMap<u64, u64> = BTreeMap::new();
    for group in &with_quorum {
        let stream = &streams[usize::try_from(*group).unwrap()];
        post_until_no_content(&client, &url(others[0], stream), "while-a-node-is-down;").await;
    }
    for node in &others {
        let metrics = node_metrics(&client, &node.url()).await;
        for group in metrics.groups {
            if with_quorum.contains(&group.raft_group_id)
                && let Some(index) = group.committed_index
            {
                let known = committed.entry(group.raft_group_id).or_default();
                *known = (*known).max(index);
            }
        }
    }
    let node = node_of(&nodes, restarted);
    procs.start(binary, node);
    let first_ready = eventually(
        &mut procs,
        &format!("node {restarted} turns ready during the meta outage"),
        async || {
            let (status, ready) = readiness(&client, node).await?;
            if status != reqwest::StatusCode::OK {
                return Err(format!("{status} {ready:?}"));
            }
            try_node_metrics(&client, &node.url()).await
        },
    )
    .await;
    for group in &first_ready.groups {
        let id = group.raft_group_id;
        if !voters[&id].contains(&restarted) {
            continue;
        }
        let acknowledgement = group.maintenance.leader_acknowledgement;
        if with_quorum.contains(&id) {
            assert_eq!(
                acknowledgement,
                LeaderAcknowledgement::Acknowledged,
                "node {restarted} turned ready before its replica of group {id} acknowledged \
                 its leader: {group:?}"
            );
            assert!(
                group.last_log_index >= committed.get(&id).copied(),
                "node {restarted} turned ready before its replica of group {id} held what the \
                 group committed while it was down ({:?}): {group:?}",
                committed.get(&id)
            );
        } else {
            assert_eq!(
                acknowledgement,
                LeaderAcknowledgement::NoLeaderContact,
                "group {id} has no leader to acknowledge: {group:?}"
            );
        }
    }
    let leaders = replica_leaders(&client, &mut procs, &live, &with_quorum).await;
    assert!(
        leaders
            .values()
            .all(|(leader, _term)| *leader != Some(restarted)),
        "a leadership moved to the restarted node: {leaders:?}"
    );
    assert_layout_static(&client, &mut procs, &live, &with_quorum, &leaders).await;

    // 3. The meta voters resume, and the cluster is ready again.
    for id in &frozen {
        procs.signal(*id, "-CONT");
    }
    let manifest = ctl_manifest(&nodes, &[]);
    ursula_ctl::wait_ready(
        &ctl_client(),
        &manifest,
        META_WAIT,
        Duration::from_millis(500),
    )
    .await
    .expect("the cluster is ready once meta has its quorum again");
}

/// Integration 7's single-node part (#768, design 12.1): a durable single
/// node resolves an unset `raft.wal.fsync` to `always` and reports it, and a
/// kill restarts it unattended: it becomes ready with no recovery plan,
/// opens normally and reads its acknowledged write. Set to `never`, it
/// reports `never`. A kernel that reports a boot id reads the kill as a
/// process crash, so the host-crash behaviour of each policy stays with the
/// power-loss DST.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cli_a_durable_single_node_reports_its_fsync_and_restarts_unattended() {
    let _guard = static_cluster_cli_test_guard().await;
    let Some(binary) = option_env!("CARGO_BIN_EXE_ursula") else {
        tracing::warn!("CARGO_BIN_EXE_ursula is not set; skipping the single-node test");
        return;
    };
    let root = tempfile::tempdir().expect("temp root");
    let nodes = MetaNode::cluster(root.path(), 1);
    let node = &nodes[0];
    let client = reqwest::Client::new();
    let mut procs = Processes::default();
    node.write(&peers_of(&nodes), MetaToml::default());
    procs.start(binary, node);
    wait_readiness_reason(&client, &mut procs, node, None).await;
    let recovery = wal_recovery(&client, &node.url()).await;
    assert_eq!(recovery["fsync"], "always", "{recovery}");
    let stream = format!("{}/single/durable", node.url());
    let read = format!("{stream}?offset=0&max_bytes=64");
    put_until_created(&client, &stream).await;
    post_until_no_content(&client, &stream, "before-the-kill;").await;

    // 1. A kill, and a restart no operator attends.
    procs.kill(1);
    procs.start(binary, node);
    wait_readiness_reason(&client, &mut procs, node, None).await;
    let recovery = wal_recovery(&client, &node.url()).await;
    let crash = if cfg!(target_os = "linux") {
        "process_crash"
    } else {
        "host_crash"
    };
    assert_eq!(recovery["previous_run"]["kind"], crash, "{recovery}");
    assert_eq!(recovery["recovery"]["state"], "normal", "{recovery}");
    assert_eq!(recovery["fsync"], "always", "{recovery}");
    read_until_matches(&client, &read, b"before-the-kill;").await;

    // 2. An explicit `never` is what the node reports.
    procs.stop(1).await;
    set_wal_fsync(&node.config, "never");
    procs.start(binary, node);
    wait_readiness_reason(&client, &mut procs, node, None).await;
    let recovery = wal_recovery(&client, &node.url()).await;
    assert_eq!(recovery["fsync"], "never", "{recovery}");
    assert_eq!(recovery["previous_run"]["kind"], "clean", "{recovery}");
    post_until_no_content(&client, &stream, "under-never;").await;
    read_until_matches(&client, &read, b"before-the-kill;under-never;").await;
}

/// S2(n) (#768): at replication factor 3 each of five nodes hosts only
/// some of the groups, and the node-wide passes and the cluster-wide admin
/// answers still cover every group. One node's purge removes the bucket
/// from every group, through the leaders of the groups it hosts no replica
/// of. The nodes' usage reports, kept per group by the newest replica,
/// count every stream once, and each replica of a group reports the same
/// snapshot bytes. `backup-create` exports every group, and its
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

    // Each stream publishes a snapshot. Its group's three replicas each
    // report the body's bytes once they have applied it: the value is
    // derived from applied state, the same on every replica.
    for stream in &streams {
        let url = format!(
            "{}/wide/{stream}/snapshot/{:020}",
            purger.url(),
            "before;".len()
        );
        put_snapshot_until_no_content(&client, &url, "state").await;
    }
    eventually(
        &mut procs,
        "every replica reports the snapshot bytes",
        async || {
            let mut replicas = BTreeMap::<u32, Vec<u64>>::new();
            for node in &nodes {
                for group in usage_report(&client, node).await?.groups {
                    let bytes = group
                        .buckets
                        .get("wide")
                        .map_or(0, |usage| usage.snapshot_bytes);
                    replicas.entry(group.raft_group_id).or_default().push(bytes);
                }
            }
            let expected: Vec<u64> = vec![5; 3];
            if replicas.len() == META_TEST_GROUPS
                && replicas.values().all(|bytes| *bytes == expected)
            {
                Ok(())
            } else {
                Err(format!("{replicas:?}"))
            }
        },
    )
    .await;

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
    // The restored streams' snapshots count again, derived from the
    // imported state.
    eventually(
        &mut procs,
        "the restored snapshot bytes count",
        async || {
            let mut reports = Vec::new();
            for node in &nodes {
                reports.push(usage_report(&client, node).await?);
            }
            let (buckets, complete) = ursula_proto::admin::UsageReport::bucket_usage(&reports);
            let bytes = buckets.get("wide").map_or(0, |usage| usage.snapshot_bytes);
            if complete && bytes == 20 {
                Ok(())
            } else {
                Err(format!("complete {complete}, {buckets:?}"))
            }
        },
    )
    .await;

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
/// groups of a meta test cluster. `bucket` must be a valid bucket ID, or
/// every create in it answers `400`.
fn stream_per_group(bucket: &str) -> Vec<String> {
    streams_by_group(bucket).into_values().collect()
}

/// [`stream_per_group`], by the data group each stream hashes to.
fn streams_by_group(bucket: &str) -> BTreeMap<u32, String> {
    streams_by_group_of(bucket, META_TEST_GROUPS)
}

/// [`streams_by_group`] in a cluster of `groups` data groups.
fn streams_by_group_of(bucket: &str, groups: usize) -> BTreeMap<u32, String> {
    ursula_runtime::validate_bucket_id(bucket)
        .unwrap_or_else(|err| panic!("bucket {bucket:?}: {err}"));
    let map = ursula_shard::StaticShardMap::new(1, groups).expect("shard map");
    let mut streams: BTreeMap<u32, String> = BTreeMap::new();
    for candidate in 0_u32.. {
        let stream = format!("s{candidate}");
        let group = map
            .locate(&ursula_shard::BucketStreamId::new(bucket, &stream))
            .raft_group_id;
        streams.entry(group.0).or_insert(stream);
        if streams.len() == groups {
            break;
        }
    }
    streams
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

/// How long every data group's leader must stay put before the balancer
/// counts as settled: three of its 5 s ticks and a margin.
const BALANCE_SETTLED: Duration = Duration::from_secs(16);

/// A client whose every request ends within 10 s, so that a request to a
/// stopped node fails instead of hanging.
fn bounded_client() -> reqwest::Client {
    reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .expect("HTTP client")
}

fn voter_set(ids: &BTreeSet<u64>) -> VoterSet {
    VoterSet::new(ids.iter().copied().map(NodeId::new).collect()).expect("a voter set")
}

/// Node `id` of `nodes`.
fn node_of(nodes: &[MetaNode], id: u64) -> &MetaNode {
    nodes
        .iter()
        .find(|node| node.id == id)
        .unwrap_or_else(|| panic!("node {id} is in the cluster"))
}

/// The `ursulactl` manifest of `nodes` but the stopped or dead ones of
/// `skip`.
fn ctl_manifest(nodes: &[MetaNode], skip: &[u64]) -> Vec<ursula_ctl::NodeInfo> {
    nodes
        .iter()
        .filter(|node| !skip.contains(&node.id))
        .map(MetaNode::ctl)
        .collect()
}

/// A fresh `ursulactl` client: each pins the process incarnation of the
/// nodes it writes to, and the tests restart nodes.
fn ctl_client() -> ursula_ctl::MetricsClient {
    ursula_ctl::MetricsClient::new(Duration::from_secs(5)).expect("ctl client")
}

/// Whether a [`placement_move`] may add a node the meta leader does not
/// report live (`--allow-not-live`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NewVoters {
    Live,
    MayBeDown,
}

/// `ursulactl placement move` of `group` by `intent` (a cancel with
/// [`Intent::Cancel`]) over the nodes of `nodes` but `skip`, with `--wait`
/// when `wait`. Answers how the group ended and what the command printed.
async fn placement_move(
    nodes: &[MetaNode],
    skip: &[u64],
    group: RaftGroupId,
    intent: Intent,
    new_voters: NewVoters,
    wait: bool,
) -> (GroupEnd, String) {
    let options = MoveOptions {
        groups: vec![group],
        intent,
        expect: None,
        retarget: false,
        allow_not_live: new_voters == NewVoters::MayBeDown,
        wait,
        timeout: META_WAIT,
        poll_interval: Duration::from_millis(500),
    };
    let mut out = Vec::new();
    let ends = run_moves(
        &ctl_client(),
        &ctl_manifest(nodes, skip),
        &options,
        &mut out,
    )
    .await
    .expect("placement move");
    let out = String::from_utf8_lossy(&out).into_owned();
    let end = ends
        .get(&group)
        .cloned()
        .unwrap_or_else(|| panic!("no end for group {}: {out}", group.0));
    (end, out)
}

/// [`placement_move`] with `--wait`, which must end with the group stable
/// at `to`.
async fn move_converges(
    nodes: &[MetaNode],
    skip: &[u64],
    group: RaftGroupId,
    intent: Intent,
    to: &BTreeSet<u64>,
) {
    let (end, out) = placement_move(nodes, skip, group, intent, NewVoters::Live, true).await;
    assert_eq!(end, GroupEnd::Converged { to: voter_set(to) }, "{out}");
}

/// Waits until the meta leader's report lists each of `ids` `Eligible`, read
/// as `ursulactl placement move` reads it (`PlacementStatus.leader_report`)
/// over the nodes of `nodes` but `skip`. A move without `--allow-not-live`
/// refuses a node the report leaves out, and the leader lists a node only a
/// pass after it holds both the node's acknowledgement within
/// `LIVE_WITHIN` and its WAL-disk state, which it asks for every few
/// seconds. So a node that just resumed, or any node under a meta leader
/// elected a moment ago, is not listed yet, and a move must wait for the
/// report instead of racing it.
async fn wait_eligible(procs: &mut Processes, nodes: &[MetaNode], skip: &[u64], ids: &[u64]) {
    let ctl = ctl_client();
    let manifest = ctl_manifest(nodes, skip);
    let what = format!("the meta leader reports nodes {ids:?} eligible");
    eventually(procs, &what, async || {
        let status = ursula_ctl::view::leader_status(&ctl, &manifest)
            .await
            .map_err(|err| err.to_string())?
            .map_err(|refusal| refusal.to_string())?;
        let report = status
            .leader_report
            .ok_or_else(|| format!("node {} publishes no leader report", status.node))?;
        let missing: Vec<u64> = ids
            .iter()
            .copied()
            .filter(|id| {
                report.eligibility.get(&NodeId::new(*id)) != Some(&NodeEligibility::Eligible)
            })
            .collect();
        if missing.is_empty() {
            Ok(())
        } else {
            Err(format!("nodes {missing:?} are not eligible: {report:?}"))
        }
    })
    .await;
}

fn replace(old: u64, new: u64) -> Intent {
    Intent::Replace(Replacement {
        old: NodeId::new(old),
        new: NodeId::new(new),
    })
}

/// The bootstrapped cluster a meta read barrier confirmed on the first
/// node of `nodes` but `skip` that answers one.
async fn confirmed_cluster(
    client: &reqwest::Client,
    procs: &mut Processes,
    nodes: &[MetaNode],
    skip: &[u64],
) -> Cluster {
    eventually(procs, "a node confirms a bootstrapped view", async || {
        let mut last = String::from("no node asked");
        for node in nodes.iter().filter(|node| !skip.contains(&node.id)) {
            match confirmed_placement_status(client, node).await {
                Ok(status) => match bootstrapped(&status) {
                    Some(cluster) => return Ok(cluster.clone()),
                    None => last = format!("{status:?}"),
                },
                Err(err) => last = err,
            }
        }
        Err(last)
    })
    .await
}

/// The meta leader node `via` names, once that node names itself.
async fn known_meta_leader(
    client: &reqwest::Client,
    procs: &mut Processes,
    nodes: &[MetaNode],
    via: &MetaNode,
) -> u64 {
    let what = format!("node {} names a meta leader that names itself", via.id);
    eventually(procs, &what, async || {
        let status = placement_status(client, via).await?;
        let leader = meta_leader(&status).ok_or_else(|| format!("{status:?}"))?;
        let own = placement_status(client, node_of(nodes, leader)).await?;
        if meta_leader(&own) == Some(leader) {
            Ok(leader)
        } else {
            Err(format!("{own:?}"))
        }
    })
    .await
}

/// Node `node`'s replica of `group`, from its metrics.
async fn replica_of(
    client: &reqwest::Client,
    node: &MetaNode,
    group: RaftGroupId,
) -> Result<RaftGroupMetrics, String> {
    try_node_metrics(client, &node.url())
        .await?
        .groups
        .into_iter()
        .find(|replica| replica.raft_group_id == u64::from(group.0))
        .ok_or_else(|| format!("node {} runs no replica of group {}", node.id, group.0))
}

/// The replica of `group` that leads it with a live quorum, among those
/// of the nodes of `nodes` in `among`.
async fn leading_replica(
    client: &reqwest::Client,
    nodes: &[MetaNode],
    among: &BTreeSet<u64>,
    group: RaftGroupId,
) -> Result<RaftGroupMetrics, String> {
    let mut seen = Vec::new();
    for node in nodes.iter().filter(|node| among.contains(&node.id)) {
        match replica_of(client, node, group).await {
            Ok(replica) if replica.current_leader == Some(node.id) && replica.leads_live => {
                return Ok(replica);
            }
            Ok(replica) => seen.push(format!("{replica:?}")),
            Err(err) => seen.push(err),
        }
    }
    Err(format!(
        "no live leader of group {} on {among:?}: {seen:?}",
        group.0
    ))
}

/// Waits until node `node` runs a replica of every group `cluster`
/// places on it as a voter, each knowing a leader with its gate open.
async fn wait_serves_placed(
    client: &reqwest::Client,
    procs: &mut Processes,
    node: &MetaNode,
    cluster: &Cluster,
) {
    let placed = placed_on(cluster, node.id);
    let what = format!("node {} serves its groups {placed:?}", node.id);
    eventually(procs, &what, async || {
        let metrics = try_node_metrics(client, &node.url()).await?;
        for group in &placed {
            let serves = metrics.groups.iter().any(|replica| {
                replica.raft_group_id == *group
                    && replica.current_leader.is_some()
                    && replica.recovery_gate == RecoveryGateStatus::Open
            });
            if !serves {
                return Err(format!("group {group}: {:?}", metrics.groups));
            }
        }
        Ok(())
    })
    .await;
}

/// Creates the stream at `url`.
async fn create_stream(client: &reqwest::Client, procs: &mut Processes, url: &str) {
    let what = format!("{url} is created");
    eventually(procs, &what, async || {
        let response = client
            .put(url)
            .header("content-type", "text/plain")
            .send()
            .await
            .map_err(|err| format!("PUT {url}: {err}"))?;
        match response.status() {
            reqwest::StatusCode::CREATED | reqwest::StatusCode::OK => Ok(()),
            status => {
                let body = response
                    .text()
                    .await
                    .unwrap_or_else(|err| format!("<unreadable body: {err}>"));
                Err(format!("PUT {url}: {status}: {body}"))
            }
        }
    })
    .await;
}

/// Starts `ursula gateway` in front of every node of `nodes`, and answers
/// it with its URL.
fn spawn_gateway(binary: &str, nodes: &[MetaNode]) -> (ChildGuard, String) {
    let upstreams: Vec<String> = nodes.iter().map(MetaNode::url).collect();
    spawn_gateway_over(binary, &upstreams)
}

/// Starts `ursula gateway`, with its default timeouts, in front of
/// `upstreams`, the client URLs of a cluster of [`META_TEST_GROUPS`] groups,
/// and answers it with its URL.
fn spawn_gateway_over(binary: &str, upstreams: &[String]) -> (ChildGuard, String) {
    let port = free_port();
    let mut command = Command::new(binary);
    command
        .arg("gateway")
        .arg("--listen")
        .arg(format!("127.0.0.1:{port}"))
        .arg("--raft-group-count")
        .arg(META_TEST_GROUPS.to_string());
    for upstream in upstreams {
        command.arg("--upstream").arg(upstream);
    }
    let gateway = spawn_child(command, format!("gateway-{port}"));
    (gateway, format!("http://127.0.0.1:{port}"))
}

/// How the gateway answered one write of a [`GatewayLoad`].
#[derive(Debug, Clone, PartialEq, Eq)]
enum LoadAnswer {
    Acknowledged,
    /// `503` with `Retry-After`, and the code its error body names.
    Retryable(Option<ursula_headers::ErrorCode>),
    /// The gateway's `504` `UPSTREAM_TIMEOUT`: the node it sent the write to
    /// answered nothing within its header timeout, so the write may have
    /// applied.
    TimedOut,
    /// Any other status.
    Failed(u16),
    /// No answer within the client's 10 s, or no connection to the gateway.
    Unanswered,
}

/// One write of a [`GatewayLoad`].
#[derive(Debug, Clone)]
struct LoadWrite {
    stream: String,
    payload: String,
    answer: LoadAnswer,
    /// When the write was sent, and how long its answer took.
    sent: tokio::time::Instant,
    took: Duration,
}

/// Appends `{prefix}-{n};` to its streams in turn every 20 ms until
/// stopped, and keeps every write with how it was answered. Each write
/// waits at most 10 s for its answer. Once stopped it goes on until it has
/// an acknowledged write, at most [`LOAD_ACK_GRACE`] longer: a move can
/// converge within it.
/// How long a stopped [`GatewayLoad`] goes on for its first acknowledged
/// write: three of the client's 10 s timeouts.
const LOAD_ACK_GRACE: Duration = Duration::from_secs(30);

struct GatewayLoad {
    stop: Arc<std::sync::atomic::AtomicBool>,
    task: tokio::task::JoinHandle<Vec<LoadWrite>>,
}

impl GatewayLoad {
    fn start(streams: Vec<String>, prefix: &'static str) -> Self {
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let task = {
            let stop = Arc::clone(&stop);
            tokio::spawn(async move {
                let client = bounded_client();
                let mut writes: Vec<LoadWrite> = Vec::new();
                let mut stopped_at = None;
                for (write, stream) in streams.iter().cycle().enumerate() {
                    if stop.load(std::sync::atomic::Ordering::Acquire) {
                        let since = *stopped_at.get_or_insert_with(tokio::time::Instant::now);
                        if writes
                            .iter()
                            .any(|write| write.answer == LoadAnswer::Acknowledged)
                            || since.elapsed() >= LOAD_ACK_GRACE
                        {
                            break;
                        }
                    }
                    let payload = format!("{prefix}-{write};");
                    let sent_at = tokio::time::Instant::now();
                    let sent = client
                        .post(stream)
                        .header("content-type", "text/plain")
                        .body(payload.clone())
                        .send()
                        .await;
                    let answer = match sent {
                        Err(_unanswered) => LoadAnswer::Unanswered,
                        Ok(response) => {
                            let status = response.status();
                            let retry = is_retryable_refusal(status, response.headers());
                            let body = response.bytes().await.unwrap_or_default();
                            let code = ursula_headers::ErrorBody::parse(&body)
                                .ok()
                                .map(|body| body.code);
                            match status {
                                reqwest::StatusCode::NO_CONTENT => LoadAnswer::Acknowledged,
                                reqwest::StatusCode::SERVICE_UNAVAILABLE if retry => {
                                    LoadAnswer::Retryable(code)
                                }
                                reqwest::StatusCode::GATEWAY_TIMEOUT
                                    if code == Some(ursula_headers::ErrorCode::UpstreamTimeout) =>
                                {
                                    LoadAnswer::TimedOut
                                }
                                status => LoadAnswer::Failed(status.as_u16()),
                            }
                        }
                    };
                    writes.push(LoadWrite {
                        stream: stream.clone(),
                        payload,
                        answer,
                        sent: sent_at,
                        took: sent_at.elapsed(),
                    });
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
                writes
            })
        };
        Self { stop, task }
    }

    /// Stops the load once its write in flight is answered and it has an
    /// acknowledged write (at most [`LOAD_ACK_GRACE`] after the stop), and
    /// answers every write it sent.
    async fn stop(self) -> Vec<LoadWrite> {
        self.stop.store(true, std::sync::atomic::Ordering::Release);
        self.task.await.expect("the write load ends")
    }
}

/// Whether an answer of `status` with `headers` is the retryable refusal
/// the gateway and the nodes answer a write that may be sent again: a `503`
/// with `Retry-After`.
fn is_retryable_refusal(status: reqwest::StatusCode, headers: &reqwest::header::HeaderMap) -> bool {
    status == reqwest::StatusCode::SERVICE_UNAVAILABLE
        && headers.contains_key(reqwest::header::RETRY_AFTER)
}

/// Asserts that the gateway answered each of `writes` within the client's
/// timeout, with `204` or with a `503` and `Retry-After`, or with its `504`
/// when `timed_out` allows that, and acknowledged some.
fn assert_only_retryable(writes: &[LoadWrite], timed_out: bool) {
    let failed: Vec<&LoadWrite> = writes
        .iter()
        .filter(|write| match write.answer {
            LoadAnswer::Acknowledged | LoadAnswer::Retryable(_) => false,
            LoadAnswer::TimedOut => !timed_out,
            LoadAnswer::Failed(_) | LoadAnswer::Unanswered => true,
        })
        .collect();
    assert!(
        failed.is_empty(),
        "{} of {} writes failed: {failed:?}",
        failed.len(),
        writes.len()
    );
    assert!(
        writes
            .iter()
            .any(|write| write.answer == LoadAnswer::Acknowledged),
        "no write was acknowledged: {writes:?}"
    );
}

/// Reads `stream` once its load stopped, until it holds every write the
/// load acknowledged, and checks it against the load's `writes`: it holds
/// nothing the load did not send, nothing twice, every acknowledged write in
/// the order sent, and no write whose `503` proves it applied nothing
/// ([`ursula_headers::write_outcome::proves_refusal`]). A `503` whose body
/// names no code proves nothing, so such a write may be held.
async fn check_load_read(client: &reqwest::Client, stream: &str, writes: &[LoadWrite]) {
    let writes: Vec<&LoadWrite> = writes
        .iter()
        .filter(|write| write.stream == stream)
        .collect();
    let acknowledged: Vec<String> = writes
        .iter()
        .filter(|write| write.answer == LoadAnswer::Acknowledged)
        .map(|write| write.payload.clone())
        .collect();
    let read = read_until_holding(
        client,
        &format!("{stream}?offset=0&max_bytes=8388608"),
        &acknowledged,
    )
    .await;
    let sent: HashMap<&str, &LoadAnswer> = writes
        .iter()
        .map(|write| (write.payload.trim_end_matches(';'), &write.answer))
        .collect();
    let text = String::from_utf8_lossy(&read);
    let mut held = HashSet::new();
    let mut in_order = Vec::new();
    for token in text.split(';').filter(|token| !token.is_empty()) {
        let answer = sent
            .get(token)
            .unwrap_or_else(|| panic!("{stream} holds {token:?}, which the load never sent"));
        assert!(held.insert(token), "{stream} holds {token:?} twice");
        match answer {
            LoadAnswer::Acknowledged => in_order.push(format!("{token};")),
            LoadAnswer::Retryable(code) => assert!(
                !code
                    .as_ref()
                    .is_some_and(|code| ursula_headers::write_outcome::proves_refusal(503, code)),
                "{stream} holds {token:?}, whose {code:?} proves it applied nothing"
            ),
            LoadAnswer::TimedOut | LoadAnswer::Failed(_) | LoadAnswer::Unanswered => {}
        }
    }
    assert_eq!(
        in_order, acknowledged,
        "{stream} holds the acknowledged writes out of order"
    );
}

/// Waits until every data group of `nodes` has one leader that each of its
/// replicas reports and that leads with a live quorum, and the leaders
/// stay the same for [`BALANCE_SETTLED`]. Answers each group's leader.
async fn wait_leaders_settle(
    client: &reqwest::Client,
    procs: &mut Processes,
    nodes: &[MetaNode],
) -> BTreeMap<u64, u64> {
    let mut stable: Option<(BTreeMap<u64, u64>, std::time::Instant)> = None;
    eventually(procs, "the balancer settles", async || {
        let mut reported: BTreeMap<u64, BTreeSet<Option<u64>>> = BTreeMap::new();
        let mut live = BTreeSet::new();
        for node in nodes {
            for replica in try_node_metrics(client, &node.url()).await?.groups {
                if replica.current_leader == Some(node.id) && replica.leads_live {
                    live.insert(replica.raft_group_id);
                }
                reported
                    .entry(replica.raft_group_id)
                    .or_default()
                    .insert(replica.current_leader);
            }
        }
        let mut leaders = BTreeMap::new();
        for (group, named) in &reported {
            match named.iter().collect::<Vec<_>>().as_slice() {
                [Some(leader)] if live.contains(group) => leaders.insert(*group, *leader),
                _ => return Err(format!("group {group}: leaders {named:?}")),
            };
        }
        if leaders.len() != META_TEST_GROUPS {
            return Err(format!("leaders of {leaders:?}"));
        }
        match &stable {
            Some((seen, since)) if *seen == leaders => {
                if since.elapsed() >= BALANCE_SETTLED {
                    Ok(leaders)
                } else {
                    Err(format!("{leaders:?} for {:?}", since.elapsed()))
                }
            }
            _ => {
                stable = Some((leaders.clone(), std::time::Instant::now()));
                Err(format!("leaders moved to {leaders:?}"))
            }
        }
    })
    .await
}

/// S3(i), integrations 1 (without its teardown checks), 3, 4, 10 and 13
/// (#768, design 15.3), in phases on one cluster: nodes 1 to 4 at
/// replication factor 3 with meta voters 1 to 3, and node 5 added by a
/// rolling `raft.peers` update. Every move and cancel goes through
/// `ursula_ctl::placement` calls.
///
/// 1. With node 5 stopped, `move --replace --allow-not-live` puts node 5 in
///    place of a meta voter of the group placed off the meta leader (the
///    other moves onto a live node leave the liveness check on). The
///    group's leader adds node 5 as a learner and waits. The meta leader is
///    killed there, node 5 continues, and the move converges under the next
///    meta leader: the placement is stable at the target with a new
///    `converged_at`, and node 5 serves the group as a voter. A move that
///    needs the meta leader it was requested under fails it.
/// 2. Once the moved-off source no longer counts its replica as a voter,
///    a read sent to it is answered with a `2xx` at least once, and every
///    `2xx` holds the write made after the move. A dropped voter that
///    answers a read from its own stale state fails it. Refusals pass: a
///    dropped voter that campaigns knows no leader. The client follows
///    redirects, so once the source tore its replica down, the `2xx` comes
///    from a node its `307` names: the phase shows that no read sent to the
///    source is stale, not which path answered it.
/// 3. The group placed on {1, 2, 3} moves to {1, 4, 5} with node 5 stopped
///    (`--allow-not-live`), so its leader promotes node 4 and waits for
///    node 5. Node 4 is killed once a uniform membership lists it, and a
///    cancel converges back on {1, 2, 3} with nodes 4 and 5 down. A cancel
///    that needs its dead target or the learner that never caught up fails
///    it.
/// 4. Through a gateway, each of the two groups not moved yet replaces its
///    leader by node 5 under write load. The lowest-id voter that stays is
///    killed for the first group and stopped (`SIGSTOP`, a blackhole) for
///    the second, before the load starts. With it dead, the gateway answers
///    every write `204` or a `503` with `Retry-After`. With it stopped, the
///    gateway's probes eject it, and a write the gateway sends it before
///    that gets the gateway's `504` at its header timeout, well within the
///    load's 10 s, and its outcome is unknown: no write goes unanswered.
///    Each stream holds every acknowledged write once, in order, and no
///    write whose `503` code proves it applied nothing. A leader that hands
///    off only to the dead or stopped voter, or a proven refusal that
///    applied, fails it.
/// 5. Node 4, a meta learner, restarts with its meta snapshot record
///    corrupted and reports its meta damaged. The moved group's staying meta
///    voter is drained and node 5 killed, which leaves node 4 the group's
///    only voter that may campaign, and it leads the group. A move of node
///    5's place onto the node the group was placed off is accepted while
///    node 4 leads, and converges once the drain is cleared: node 4 hands the
///    group off and ends leading nothing. A damaged leader that keeps its
///    leadership fails it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cli_moves_and_a_cancel_converge_through_kills_stops_and_damaged_meta() {
    use ursula_proto::admin::RaftMaintenanceReport;

    let _guard = static_cluster_cli_test_guard().await;
    let Some(binary) = option_env!("CARGO_BIN_EXE_ursula") else {
        tracing::warn!("CARGO_BIN_EXE_ursula is not set; skipping the move test");
        return;
    };
    let root = tempfile::tempdir().expect("temp root");
    let nodes = MetaNode::cluster(root.path(), 5);
    let meta = MetaToml {
        meta_voters: Some(META_VOTERS),
        replication_factor: Some(3),
        ..MetaToml::default()
    };
    let client = bounded_client();
    let mut procs = Processes::default();
    let first = peers_of(&nodes[..4]);
    for node in &nodes[..4] {
        node.write(&first, meta);
        procs.start(binary, node);
    }
    wait_settled(&client, &mut procs, &nodes[..4]).await;

    // Node 5 joins: it registers, and nodes 1 to 4 restart one at a time
    // with the full `raft.peers`, as an operator adds a node.
    let all = peers_of(&nodes);
    nodes[4].write(&all, meta);
    procs.start(binary, &nodes[4]);
    wait_placement(
        &client,
        &mut procs,
        &nodes[4],
        "registers without a role",
        |status| {
            matches!(
                status.registration,
                RegistrationStatus::Answered(Registered::Unreferenced(_))
            )
        },
    )
    .await;
    for node in &nodes[..4] {
        procs.stop(node.id).await;
        node.write(&all, meta);
        procs.start(binary, node);
        wait_readiness_reason(&client, &mut procs, node, None).await;
    }
    wait_settled(&client, &mut procs, &nodes).await;
    let streams = stream_per_group("moves");
    let url = |node: &MetaNode, group: RaftGroupId| {
        format!(
            "{}/moves/{}",
            node.url(),
            streams[usize::try_from(group.0).unwrap()]
        )
    };
    let read = |node: &MetaNode, group: RaftGroupId| {
        format!("{}?offset=0&max_bytes=256", url(node, group))
    };
    for group in (0..u32::try_from(META_TEST_GROUPS).unwrap()).map(RaftGroupId) {
        put_until_created(&client, &url(&nodes[0], group)).await;
        post_until_no_content(&client, &url(&nodes[0], group), "before-the-moves;").await;
    }

    // 1. The group placed off the meta leader moves one of its meta voters
    // onto node 5, which is stopped, so the move waits with node 5 a learner.
    let cluster = confirmed_cluster(&client, &mut procs, &nodes, &[]).await;
    let first_meta_leader = known_meta_leader(&client, &mut procs, &nodes, &nodes[3]).await;
    let (moved, from) = current_voters(&cluster)
        .into_iter()
        .find(|(_, voters)| !voters.contains(&first_meta_leader))
        .unwrap_or_else(|| panic!("a group is placed off node {first_meta_leader}: {cluster:?}"));
    let source = *from.iter().find(|id| **id != 4).expect("a meta voter");
    let staying = *from
        .iter()
        .find(|id| **id != 4 && **id != source)
        .expect("a second meta voter");
    let moved_to = BTreeSet::from([staying, 4, 5]);
    let converged_before = match cluster.group(moved) {
        Some(GroupPlacement::Live { converged_at, .. }) => *converged_at,
        other => panic!("group {moved:?} is not live: {other:?}"),
    };
    procs.signal(5, "-STOP");
    let (end, out) = placement_move(
        &nodes,
        &[5],
        moved,
        replace(source, 5),
        NewVoters::MayBeDown,
        false,
    )
    .await;
    assert!(matches!(end, GroupEnd::Accepted { .. }), "{end:?}\n{out}");
    let what = format!("group {} adds node 5 as a learner", moved.0);
    eventually(&mut procs, &what, async || {
        let leader = leading_replica(&client, &nodes, &from, moved).await?;
        let voter = leader
            .voter_configs
            .iter()
            .any(|voters| voters.contains(&5));
        if leader.learner_ids.contains(&5) && !voter {
            Ok(())
        } else {
            Err(format!("{leader:?}"))
        }
    })
    .await;
    let killed_meta_leader = known_meta_leader(&client, &mut procs, &nodes, &nodes[3]).await;
    procs.kill(killed_meta_leader);
    procs.signal(5, "-CONT");
    move_converges(
        &nodes,
        &[killed_meta_leader],
        moved,
        replace(source, 5),
        &moved_to,
    )
    .await;
    let after = confirmed_cluster(&client, &mut procs, &nodes, &[killed_meta_leader]).await;
    match after.group(moved) {
        Some(GroupPlacement::Live {
            converged_at,
            target: Target::Stable { voters },
            ..
        }) => {
            assert_eq!(node_ids(voters.iter()), moved_to, "{after:?}");
            assert_ne!(*converged_at, converged_before, "{after:?}");
        }
        other => panic!("group {moved:?} is not stable: {other:?}"),
    }
    let what = format!("node 5 is a voter of group {}", moved.0);
    eventually(&mut procs, &what, async || {
        let replica = replica_of(&client, &nodes[4], moved).await?;
        if replica.voter_configs == vec![moved_to.clone()] && replica.current_leader.is_some() {
            Ok(())
        } else {
            Err(format!("{replica:?}"))
        }
    })
    .await;
    read_until_matches(&client, &read(&nodes[4], moved), b"before-the-moves;").await;
    procs.start(binary, node_of(&nodes, killed_meta_leader));
    wait_placement(
        &client,
        &mut procs,
        node_of(&nodes, killed_meta_leader),
        "rejoins as a meta voter",
        |status| meta_role(status) == Some(MetaRole::Voter) && meta_leader(status).is_some(),
    )
    .await;

    // 2. The moved-off source, once its replica does not count as a voter,
    // never answers a read without the write made after the move.
    post_until_no_content(&client, &url(&nodes[4], moved), "after-the-move;").await;
    let source_node = node_of(&nodes, source);
    let what = format!(
        "node {source}'s replica of group {} does not count",
        moved.0
    );
    eventually(&mut procs, &what, async || {
        let (_, ready) = readiness(&client, source_node).await?;
        let report: RaftMaintenanceReport = ready
            .raft_maintenance
            .ok_or_else(|| "no maintenance report".to_owned())?;
        let placed = report.expected_groups.contains_key(&moved.0);
        let counts = report
            .unplaced
            .get(&moved.0)
            .is_some_and(|unplaced| unplaced.counts_as_voter);
        if placed || counts {
            Err(format!("{report:?}"))
        } else {
            Ok(())
        }
    })
    .await;
    let fresh = b"before-the-moves;after-the-move;";
    let what = format!("node {source} answers a read of group {}", moved.0);
    eventually(&mut procs, &what, async || {
        let response = client
            .get(read(source_node, moved))
            .send()
            .await
            .map_err(|err| err.to_string())?;
        if !response.status().is_success() {
            return Err(format!("{}", response.status()));
        }
        let body = response.bytes().await.map_err(|err| err.to_string())?;
        assert_eq!(
            body.as_ref(),
            fresh,
            "node {source} answered a stale read of group {}",
            moved.0
        );
        Ok(())
    })
    .await;
    for _ in 0..20 {
        if let Ok(response) = client.get(read(source_node, moved)).send().await
            && response.status().is_success()
        {
            let body = response.bytes().await.expect("read the stream");
            assert_eq!(
                body.as_ref(),
                fresh,
                "node {source} answered a stale read of group {}",
                moved.0
            );
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // 3. The group on {1, 2, 3} moves to {1, 4, 5} with node 5 stopped:
    // node 4 is promoted and the move waits for node 5. Node 4 dies once a
    // uniform membership lists it, and the cancel converges without it.
    let seeded = BTreeSet::from([1, 2, 3]);
    let (cancelled, _) = current_voters(&cluster)
        .into_iter()
        .find(|(_, voters)| *voters == seeded)
        .unwrap_or_else(|| panic!("a group is placed on 1, 2 and 3: {cluster:?}"));
    procs.signal(5, "-STOP");
    let (end, out) = placement_move(
        &nodes,
        &[5],
        cancelled,
        Intent::To(voter_set(&BTreeSet::from([1, 4, 5]))),
        NewVoters::MayBeDown,
        false,
    )
    .await;
    assert!(matches!(end, GroupEnd::Accepted { .. }), "{end:?}\n{out}");
    let what = format!("a uniform membership of group {} lists node 4", cancelled.0);
    eventually(&mut procs, &what, async || {
        let leader =
            leading_replica(&client, &nodes, &BTreeSet::from([1, 2, 3, 4]), cancelled).await?;
        match leader.voter_configs.as_slice() {
            [voters] if voters.contains(&4) => Ok(()),
            _ => Err(format!("{leader:?}")),
        }
    })
    .await;
    procs.kill(4);
    move_converges(&nodes, &[4, 5], cancelled, Intent::Cancel, &seeded).await;
    procs.signal(5, "-CONT");
    procs.start(binary, &nodes[3]);
    let cluster = confirmed_cluster(&client, &mut procs, &nodes, &[]).await;
    assert_eq!(
        cluster.group(cancelled).map(GroupPlacement::moving),
        Some(None),
        "{cluster:?}"
    );
    for node in &nodes[3..] {
        wait_serves_placed(&client, &mut procs, node, &cluster).await;
    }
    post_until_no_content(&client, &url(&nodes[0], cancelled), "after-the-cancel;").await;
    read_until_matches(
        &client,
        &read(&nodes[1], cancelled),
        b"before-the-moves;after-the-cancel;",
    )
    .await;

    // 4. Through a gateway, each group not moved yet hands its leader's
    // place to node 5 under write load, with the lowest-id voter that stays
    // killed for the first and stopped for the second.
    let (_gateway, gateway) = spawn_gateway(binary, &nodes);
    let gateway_streams = stream_per_group("gateway");
    let unmoved: Vec<(RaftGroupId, BTreeSet<u64>)> = current_voters(&cluster)
        .into_iter()
        .filter(|(group, _)| *group != moved && *group != cancelled)
        .collect();
    assert_eq!(unmoved.len(), 2, "{cluster:?}");
    for ((group, voters), blackholed) in unmoved.into_iter().zip([false, true]) {
        let stream = format!(
            "{gateway}/gateway/{}",
            gateway_streams[usize::try_from(group.0).unwrap()]
        );
        create_stream(&client, &mut procs, &stream).await;
        let what = format!("group {} has a live leader", group.0);
        let leader = eventually(&mut procs, &what, async || {
            leading_replica(&client, &nodes, &voters, group).await
        })
        .await
        .node_id;
        let stays: BTreeSet<u64> = voters.iter().copied().filter(|id| *id != leader).collect();
        let lowest = *stays.first().expect("two voters stay");
        // Before the load starts, so that no write is in flight to it.
        if blackholed {
            procs.signal(lowest, "-STOP");
        } else {
            procs.kill(lowest);
        }
        // Node 5 was stopped in phase 3, and the voter just killed or
        // stopped may have led meta.
        wait_eligible(&mut procs, &nodes, &[lowest], &[5]).await;
        let load = GatewayLoad::start(vec![stream.clone()], "through-the-gateway");
        let mut to = stays.clone();
        to.insert(5);
        move_converges(&nodes, &[lowest], group, replace(leader, 5), &to).await;
        let writes = load.stop().await;
        if blackholed {
            procs.signal(lowest, "-CONT");
        } else {
            procs.start(binary, node_of(&nodes, lowest));
        }
        assert_only_retryable(&writes, blackholed);
        check_load_read(&client, &stream, &writes).await;
        let cluster = confirmed_cluster(&client, &mut procs, &nodes, &[]).await;
        wait_serves_placed(&client, &mut procs, node_of(&nodes, lowest), &cluster).await;
    }

    // 5. Node 4 restarts with its meta snapshot record corrupted.
    let damaged = &nodes[3];
    let response = admin_test_post(
        &client,
        format!("{}/__ursula/raft/meta/snapshot", damaged.admin()),
    )
    .await
    .send()
    .await
    .expect("snapshot meta");
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    let record = damaged.meta_dir().join("group-0.snapshot.json");
    eventually(&mut procs, "node 4 records its meta snapshot", async || {
        ursula_raft::current_snapshot_pointer(&record)
            .map_err(|err| err.to_string())?
            .ok_or_else(|| "no current snapshot record yet".to_owned())
    })
    .await;
    procs.stop(4).await;
    std::fs::write(&record, b"not a meta snapshot record").expect("corrupt the meta record");
    procs.start(binary, damaged);
    wait_placement(
        &client,
        &mut procs,
        damaged,
        "reports its meta damaged",
        |status| {
            matches!(status.meta, MetaReplicaStatus::Damaged {
                kind: MetaDamageKind::Restore,
                ..
            })
        },
    )
    .await;
    eventually(
        &mut procs,
        "node 4 marks placement-unavailable",
        async || {
            let shed = leadership_shed(&client, damaged).await;
            if sheds_placement_unavailable(&shed) {
                Ok(())
            } else {
                Err(format!("{shed:?}"))
            }
        },
    )
    .await;
    // The moved group's third voter is drained and node 5 dies: node 4 is
    // the only voter that may campaign. The drain mark only stops the
    // third voter from campaigning and taking transfers, and a leadership
    // it holds stays, so it hands that one to node 5 first: node 4, without
    // meta, takes no transfer.
    let ctl = ctl_client();
    let drained = node_of(&nodes, staying);
    ctl.set_maintenance_drain(&drained.ctl(), true)
        .await
        .expect("drain the third voter");
    let what = format!("node {staying} hands group {} off", moved.0);
    eventually(&mut procs, &what, async || {
        let Err(led) = leading_replica(&client, &nodes, &BTreeSet::from([4, 5]), moved).await
        else {
            return Ok(());
        };
        let answer = ctl
            .transfer_leader(&drained.ctl(), u64::from(moved.0), 5)
            .await
            .map_err(|err| format!("{led}; transfer to node 5: {err:#}"))?;
        Err(format!("{led}; transfer to node 5: {answer:?}"))
    })
    .await;
    procs.kill(5);
    let what = format!("node 4 leads group {}", moved.0);
    eventually(&mut procs, &what, async || {
        leading_replica(&client, &nodes, &BTreeSet::from([4]), moved).await
    })
    .await;
    // Node 5's place goes to the node the group was placed off, the one
    // node of 1 to 3 the group has no retirement for.
    wait_eligible(&mut procs, &nodes, &[5], &[first_meta_leader]).await;
    let (end, out) = placement_move(
        &nodes,
        &[5],
        moved,
        replace(5, first_meta_leader),
        NewVoters::Live,
        false,
    )
    .await;
    assert!(matches!(end, GroupEnd::Accepted { .. }), "{end:?}\n{out}");
    ctl.set_maintenance_drain(&drained.ctl(), false)
        .await
        .expect("clear the drain");
    move_converges(
        &nodes,
        &[5],
        moved,
        replace(5, first_meta_leader),
        &BTreeSet::from([staying, 4, first_meta_leader]),
    )
    .await;
    eventually(&mut procs, "node 4 leads no data group", async || {
        let led = leads(&try_node_metrics(&client, &damaged.url()).await?, 4);
        if led == 0 {
            Ok(())
        } else {
            Err(format!("node 4 leads {led} groups"))
        }
    })
    .await;
    read_until_matches(
        &client,
        &read(damaged, moved),
        b"before-the-moves;after-the-move;",
    )
    .await;
}

/// S3(i), integration 7's growth (#768, design 15.3): a durable single
/// node with meta voter 1 bootstraps at replication factor 1, and grows to
/// three nodes: nodes 2 and 3 join with the same cluster id and meta
/// voters, and node 1 restarts with the grown `raft.peers`. Every node's
/// view is the single node's cluster, with its params and placement keys,
/// so the growth bootstraps nothing anew. `ursulactl placement move` moves
/// group 0 onto node 2 and group 1 onto node 3, each the only voter that
/// serves the group's earlier writes and takes new ones, and the other
/// groups stay on node 1 under their keys. Node 3, whose
/// `raft.placement.replication_factor` is left at its default of 3, reports
/// `replication_factor_mismatch`, and nodes 1 and 2, configured to 1, do
/// not. A growth that re-forms the cluster, an RF 1 move that never hands
/// the only voter's place over, or a node whose configured factor differs
/// from the cluster's that reports no mismatch (or one that does not
/// differ that reports one), fails it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cli_a_durable_single_node_grows_to_three_and_moves_its_groups() {
    use ursula_proto::admin::RaftMaintenanceIssue;

    let _guard = static_cluster_cli_test_guard().await;
    let Some(binary) = option_env!("CARGO_BIN_EXE_ursula") else {
        tracing::warn!("CARGO_BIN_EXE_ursula is not set; skipping the growth test");
        return;
    };
    let root = tempfile::tempdir().expect("temp root");
    let nodes = MetaNode::cluster(root.path(), 3);
    let at_one = MetaToml {
        meta_voters: Some(&[1]),
        replication_factor: Some(1),
        ..MetaToml::default()
    };
    let at_default = MetaToml {
        meta_voters: Some(&[1]),
        ..MetaToml::default()
    };
    let client = bounded_client();
    let mut procs = Processes::default();
    nodes[0].write(&peers_of(&nodes[..1]), at_one);
    procs.start(binary, &nodes[0]);
    let formed = wait_settled(&client, &mut procs, &nodes[..1]).await;
    let single = settled(&formed[0]).expect("settled").clone();
    assert_eq!(single.params().replication_factor.get(), 1, "{single:?}");
    for (group, voters) in current_voters(&single) {
        assert_eq!(voters, BTreeSet::from([1]), "group {group:?}: {single:?}");
    }
    let streams = stream_per_group("grown");
    let url = |node: &MetaNode, group: u32| {
        format!(
            "{}/grown/{}",
            node.url(),
            streams[usize::try_from(group).unwrap()]
        )
    };
    for group in 0..u32::try_from(META_TEST_GROUPS).unwrap() {
        put_until_created(&client, &url(&nodes[0], group)).await;
        post_until_no_content(&client, &url(&nodes[0], group), "before-the-growth;").await;
    }

    // 1. Nodes 2 and 3 join, and node 1 restarts with the grown peers.
    let all = peers_of(&nodes);
    nodes[1].write(&all, at_one);
    nodes[2].write(&all, at_default);
    for node in &nodes[1..] {
        procs.start(binary, node);
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
    procs.stop(1).await;
    nodes[0].write(&all, at_one);
    procs.start(binary, &nodes[0]);
    for status in wait_settled(&client, &mut procs, &nodes).await {
        let grown = settled(&status).unwrap();
        assert_eq!(grown.params(), single.params(), "{status:?}");
        assert_eq!(sinces(grown), sinces(&single), "{status:?}");
        assert_eq!(
            node_ids(grown.nodes().keys().copied()),
            BTreeSet::from([1, 2, 3]),
            "{status:?}"
        );
    }

    // 2. Group 0 moves onto node 2 and group 1 onto node 3.
    for (group, to) in [(0_u32, 2_u64), (1, 3)] {
        let to_set = BTreeSet::from([to]);
        // Node 1, meta's only voter, restarted above.
        wait_eligible(&mut procs, &nodes, &[], &[to]).await;
        move_converges(
            &nodes,
            &[],
            RaftGroupId(group),
            Intent::To(voter_set(&to_set)),
            &to_set,
        )
        .await;
        let target = node_of(&nodes, to);
        let what = format!("node {to} is group {group}'s only voter and leads it");
        eventually(&mut procs, &what, async || {
            let replica = replica_of(&client, target, RaftGroupId(group)).await?;
            if replica.voter_configs == vec![to_set.clone()] && replica.current_leader == Some(to) {
                Ok(())
            } else {
                Err(format!("{replica:?}"))
            }
        })
        .await;
        let read = format!("{}?offset=0&max_bytes=256", url(target, group));
        read_until_matches(&client, &read, b"before-the-growth;").await;
        post_until_no_content(&client, &url(target, group), "after-the-move;").await;
        read_until_matches(&client, &read, b"before-the-growth;after-the-move;").await;
    }
    let cluster = confirmed_cluster(&client, &mut procs, &nodes, &[]).await;
    let keys = sinces(&single);
    for (group, voters) in current_voters(&cluster) {
        let expected = match group.0 {
            0 => BTreeSet::from([2]),
            1 => BTreeSet::from([3]),
            _ => {
                assert_eq!(
                    cluster.group(group).map(GroupPlacement::since),
                    keys.get(&group).copied(),
                    "{cluster:?}"
                );
                BTreeSet::from([1])
            }
        };
        assert_eq!(voters, expected, "group {group:?}: {cluster:?}");
        assert_eq!(
            cluster.group(group).map(GroupPlacement::moving),
            Some(None),
            "{cluster:?}"
        );
    }

    // 3. Only node 3, left at the default factor, reports the mismatch.
    for node in &nodes {
        let what = format!("node {} reports its node issues", node.id);
        let issues = eventually(&mut procs, &what, async || {
            let (_, ready) = readiness(&client, node).await?;
            ready
                .raft_maintenance
                .map(|report| report.node_issues)
                .ok_or_else(|| "no maintenance report".to_owned())
        })
        .await;
        assert_eq!(
            issues.contains(&RaftMaintenanceIssue::ReplicationFactorMismatch),
            node.id == 3,
            "node {}: {issues:?}",
            node.id
        );
    }
}

/// S3(i), integration 9 (#768, design 15.3), and the certified floor of
/// S3(f) on the server: five nodes at replication factor 3 with meta
/// voters 1 to 3, behind a gateway that writes to a stream of every group.
///
/// 1. Each node in turn stops, `ursulactl verify-survivors` with it
///    excluded passes once the survivors lead every group, and the node
///    restarts behind `ursulactl wait`. The gateway answers every
///    write `204` or a `503` with `Retry-After`, and each stream holds every
///    acknowledged write once, in order, and no write whose `503` code
///    proves it applied nothing. A producer appends to a stream of every
///    group through the gateway too, and the gateway answers each of its
///    attempts with a receipt or a `503` with `Retry-After`, never its
///    `504`: a stopping node
///    refuses new connections once its listener closes, which ejects it,
///    and still answers the appends it accepted, so the gateway waits for
///    those answers rather than sending them again elsewhere. Each of those
///    streams holds each seq once, in order. Then the leaderships settle: no
///    leader changes for three balancer ticks. A restart that drops writes,
///    a proven refusal that applied, an append cut short at a draining
///    node's ejection, or a balancer that never stops moving leaderships
///    fails it.
/// 2. A group moves to {x, 4, 5}, x a meta voter, so meta keeps a quorum
///    when its three voters are killed and lose their run state. The group
///    stalls. A cutting plan whose `certified` floor names an entry above
///    the plan leader's log is refused `floor_not_held` and changes
///    nothing. The cutting plan under the floor `ursulactl recover-group`
///    reads from the confirmed view, the move's `converged_at`, is
///    satisfied on every voter: the group keeps the moved membership, serves
///    its write, and its placement keeps its key and `converged_at`. A
///    server that takes a plan without the certified entry, or a data plan
///    that rewrites the placement, fails it. Every commit marker here lies
///    at or above the certified entry, so a cut below it is not exercised.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cli_a_rolling_restart_loses_no_write_and_a_moved_group_keeps_its_certified_membership() {
    use ursula_proto::admin::PlanFloor;
    use ursula_proto::admin::PlanStep;
    use ursula_proto::admin::RecoveryPlanRefusal;
    use ursula_proto::admin::RecoveryPlanRefused;
    use ursula_proto::admin::RecoveryPlanReport;
    use ursula_proto::admin::RecoveryPlanRequest;
    use ursula_proto::admin::ReplicaLogView;
    use ursula_proto::placement::MembershipLogId;
    use ursula_proto::placement::RaftLogId;

    let _guard = static_cluster_cli_test_guard().await;
    let Some(binary) = option_env!("CARGO_BIN_EXE_ursula") else {
        tracing::warn!("CARGO_BIN_EXE_ursula is not set; skipping the rolling restart test");
        return;
    };
    let root = tempfile::tempdir().expect("temp root");
    let nodes = MetaNode::cluster(root.path(), 5);
    let peers = peers_of(&nodes);
    let meta = MetaToml {
        meta_voters: Some(META_VOTERS),
        replication_factor: Some(3),
        ..MetaToml::default()
    };
    let client = bounded_client();
    let mut procs = Processes::default();
    for node in &nodes {
        node.write(&peers, meta);
        procs.start(binary, node);
    }
    wait_settled(&client, &mut procs, &nodes).await;
    for node in &nodes {
        wait_readiness_reason(&client, &mut procs, node, None).await;
    }
    let (_gateway, gateway) = spawn_gateway(binary, &nodes);
    let streams: Vec<String> = stream_per_group("rolling")
        .iter()
        .map(|stream| format!("{gateway}/rolling/{stream}"))
        .collect();
    for stream in &streams {
        create_stream(&client, &mut procs, stream).await;
    }
    let keyed_streams: Vec<String> = stream_per_group("keyed")
        .iter()
        .map(|stream| format!("{gateway}/keyed/{stream}"))
        .collect();
    for stream in &keyed_streams {
        create_stream(&client, &mut procs, stream).await;
    }

    // 1. A rolling restart under write load.
    let load = GatewayLoad::start(streams.clone(), "rolling");
    let keyed_loads: Vec<(String, ProducerLoad)> = keyed_streams
        .iter()
        .map(|stream| {
            (
                stream.clone(),
                ProducerLoad::start(vec![stream.clone()], "keyed"),
            )
        })
        .collect();
    let manifest = ctl_manifest(&nodes, &[]);
    for node in &nodes {
        // `verify-survivors` observes the others once the node is down. It
        // refuses while a group's survivors still name the stopped node as
        // leader, so it is retried, as an operator would, until they
        // elected another.
        procs.stop(node.id).await;
        let what = format!("verify-survivors passes without node {}", node.id);
        eventually(&mut procs, &what, async || {
            ursula_ctl::quorum::verify_surviving_quorum(
                &manifest,
                node.id,
                &ctl_client(),
                &ursula_ctl::quorum::QuorumVerificationOptions {
                    timeout: Duration::from_secs(10),
                    poll_interval: Duration::from_millis(200),
                },
            )
            .await
            .map_err(|err| format!("{err:?}"))
        })
        .await;
        procs.start(binary, node);
        let outcome = ursula_ctl::wait_node_ready(
            &manifest,
            &node.ctl(),
            &ctl_client(),
            &ursula_ctl::CatchUpOptions {
                stall_timeout: Duration::from_secs(60),
                ready_timeout: META_WAIT,
                poll_interval: Duration::from_millis(200),
                lag_tolerance: 16,
            },
        )
        .await
        .unwrap_or_else(|err| panic!("wait for node {}: {err:?}", node.id));
        assert!(
            matches!(outcome, ursula_ctl::CatchUpOutcome::Ready),
            "node {}: {outcome:?}",
            node.id
        );
    }
    let writes = load.stop().await;
    assert_only_retryable(&writes, false);
    for stream in &streams {
        check_load_read(&client, stream, &writes).await;
    }
    for (stream, keyed) in keyed_loads {
        let attempts = keyed.stop().await;
        let unsettled: Vec<&ProducerAttempt> = attempts
            .iter()
            .filter(|attempt| !attempt.outcome.holds() && !attempt.retryable)
            .collect();
        assert!(
            unsettled.is_empty(),
            "{stream}: attempts answered neither a receipt nor a 503 with Retry-After: {unsettled:?}"
        );
        let held = attempts
            .iter()
            .rev()
            .find(|attempt| attempt.outcome.holds())
            .unwrap_or_else(|| panic!("{stream}: no append held: {attempts:?}"));
        check_producer_read(&client, &stream, "keyed", held.seq).await;
    }
    wait_leaders_settle(&client, &mut procs, &nodes).await;

    // 2. A group moves onto {x, 4, 5}, x a meta voter.
    let cluster = confirmed_cluster(&client, &mut procs, &nodes, &[]).await;
    let (group, from, to) = current_voters(&cluster)
        .into_iter()
        .find_map(|(group, voters)| {
            let kept = voters
                .iter()
                .copied()
                .find(|id| META_VOTERS.contains(id))
                .unwrap_or(1);
            let to = BTreeSet::from([kept, 4, 5]);
            (to != voters).then_some((group, voters, to))
        })
        .unwrap_or_else(|| panic!("a group is placed off some {{x, 4, 5}}: {cluster:?}"));
    let added: Vec<u64> = to.difference(&from).copied().collect();
    wait_eligible(&mut procs, &nodes, &[], &added).await;
    move_converges(&nodes, &[], group, Intent::To(voter_set(&to)), &to).await;
    // The move changed which nodes may lead the group, so the balancer may
    // still hand leaderships on. A create or append it races can answer
    // `OUTCOME_UNKNOWN` and still apply: a retried create then answers `200`,
    // which `create_stream` takes, and a retried append lands twice, while
    // the cut below must keep exactly one copy. So the writes wait until the
    // leaders settled again.
    wait_leaders_settle(&client, &mut procs, &nodes).await;
    let kept = *to.first().expect("three voters");
    let cut_stream = format!(
        "{}/cutover/{}",
        node_of(&nodes, kept).url(),
        stream_per_group("cutover")[usize::try_from(group.0).unwrap()]
    );
    create_stream(&client, &mut procs, &cut_stream).await;
    post_until_no_content(&client, &cut_stream, "before-the-cut;").await;
    let moved = confirmed_cluster(&client, &mut procs, &nodes, &[]).await;
    let placement = moved.group(group).cloned().expect("the group is placed");
    let floor =
        ursula_ctl::recovery::group_floor(group.0, Some(&ClusterView::Bootstrapped(moved.clone())))
            .expect("a live group's floor");
    let GroupPlacement::Live { converged_at, .. } = &placement else {
        panic!("group {group:?} is not live: {placement:?}");
    };
    let converged_at = *converged_at;
    assert_eq!(floor.plan_floor(), PlanFloor::Certified(converged_at));

    // Its voters die together and lose their run state: every replica of
    // the group comes back gated and stalls.
    for id in &to {
        procs.kill(*id);
    }
    for id in &to {
        remove_test_path(
            node_of(&nodes, *id)
                .wal
                .join("raft-log")
                .join("run-state.bin"),
        );
    }
    for id in &to {
        procs.start(binary, node_of(&nodes, *id));
    }
    let replicas = eventually(
        &mut procs,
        "every replica of the group stalls",
        async || {
            let mut replicas = Vec::new();
            for id in &to {
                let replica = replica_of(&client, node_of(&nodes, *id), group).await?;
                if replica.recovery_gate != RecoveryGateStatus::Stalled {
                    return Err(format!("{replica:?}"));
                }
                replicas.push(replica);
            }
            Ok(replicas)
        },
    )
    .await;
    let leader = replicas
        .iter()
        .max_by_key(|replica| (replica.committed_index, replica.node_id))
        .expect("three replicas")
        .clone();
    let point = leader
        .committed_index
        .expect("the leader knows its committed entry");
    let highest_term = replicas
        .iter()
        .map(|replica| replica.current_term)
        .max()
        .unwrap_or(0);
    let request =
        |replica: &RaftGroupMetrics, floor: PlanFloor, step: PlanStep| RecoveryPlanRequest {
            point_entry: None,
            floor,
            leader: leader.node_id,
            point,
            cut: true,
            step,
            expected: ReplicaLogView {
                last_log_index: replica.last_log_index,
                last_log_term: replica.last_log_term,
                current_term: replica.current_term,
            },
        };
    let accept = |id: u64| {
        format!(
            "{}/__ursula/raft/{}/recovery/accept-unsynced-loss",
            node_of(&nodes, id).admin(),
            group.0
        )
    };

    // A certified entry above the leader's log: refused, nothing changed.
    let above = MembershipLogId::new(RaftLogId {
        index: leader
            .last_log_index
            .unwrap_or(point)
            .max(point)
            .checked_add(1)
            .unwrap(),
        ..converged_at.get()
    });
    let response = admin_test_post(&client, accept(leader.node_id))
        .await
        .json(&request(
            &leader,
            PlanFloor::Certified(above),
            PlanStep::Lead { highest_term },
        ))
        .send()
        .await
        .expect("send the plan above the leader's log");
    assert_ne!(response.status(), reqwest::StatusCode::OK);
    let refused: RecoveryPlanRefused = response.json().await.expect("typed refusal");
    assert_eq!(
        refused.refusal,
        RecoveryPlanRefusal::FloorNotHeld,
        "{refused:?}"
    );
    let unchanged = replica_of(&client, node_of(&nodes, leader.node_id), group)
        .await
        .expect("the leader's replica");
    assert_eq!(
        unchanged.recovery_gate,
        RecoveryGateStatus::Stalled,
        "{unchanged:?}"
    );
    assert_eq!(unchanged.recovery, None, "{unchanged:?}");

    // The plan under the floor the tool reads: the leader first.
    let response = admin_test_post(&client, accept(leader.node_id))
        .await
        .json(&request(&leader, floor.plan_floor(), PlanStep::Lead {
            highest_term,
        }))
        .send()
        .await
        .expect("send the plan to its leader");
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    let report: RecoveryPlanReport = response.json().await.expect("typed report");
    for member in replicas
        .iter()
        .filter(|replica| replica.node_id != leader.node_id)
    {
        let response = admin_test_post(&client, accept(member.node_id))
            .await
            .json(&request(member, floor.plan_floor(), PlanStep::Follow {
                leader_term: report.leader_term,
            }))
            .send()
            .await
            .expect("send the plan to a member");
        if response.status() != reqwest::StatusCode::OK {
            // The members that took it may have elected the leader already.
            let refused: RecoveryPlanRefused = response.json().await.expect("typed refusal");
            assert_eq!(
                refused.refusal,
                RecoveryPlanRefusal::FollowsLeader {
                    leader: leader.node_id
                },
                "{refused:?}"
            );
        }
    }
    eventually(
        &mut procs,
        "the plan is satisfied on every voter",
        async || {
            for id in &to {
                let replica = replica_of(&client, node_of(&nodes, *id), group).await?;
                let satisfied = replica.recovery.is_none()
                    && replica
                        .history
                        .is_some_and(|history| history.event_index > point);
                if !satisfied || replica.voter_configs != vec![to.clone()] {
                    return Err(format!("{replica:?}"));
                }
            }
            Ok(())
        },
    )
    .await;
    let after = confirmed_cluster(&client, &mut procs, &nodes, &[]).await;
    assert_eq!(after.group(group), Some(&placement), "{after:?}");
    read_until_matches(
        &client,
        &format!("{cut_stream}?offset=0&max_bytes=64"),
        b"before-the-cut;",
    )
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

/// How long one step of the teardown and decommission test may take on a
/// loaded runner. A teardown asks its group whether the node is a member
/// at its first step and again before it retires the log, up to
/// [`ursula_raft::hosting::ASK_INTERVAL`] apart, and a decommission moves
/// the node's groups one after another under the move limit.
const LIFECYCLE_WAIT: Duration = Duration::from_secs(300);

/// How long the teardown test waits for a retired log's frames to leave
/// its source's journal: its `raft.deleted_data_max_age` of 160 s, a quarter
/// of which is the age at which a segment holding dead frames is given back,
/// and a margin.
const JOURNAL_RECLAIM_WAIT: Duration = Duration::from_secs(220);

/// Rewrites `node`'s config for the teardown test, as the S3 deleted-data
/// test does: a journal segment that holds dead frames is given back once
/// they are 40 s old (`raft.deleted_data_max_age` of 160 s), segments are
/// 64 KiB so that a few writes seal them, and a follower's install copy
/// fits inside a 20 s `raft.install_snapshot_timeout`.
///
/// The test applies it to the torn-down source only, the one node whose
/// journal it inspects.
fn bound_dead_journal_data(node: &MetaNode) {
    let text = std::fs::read_to_string(&node.config).expect("read node config");
    assert!(
        text.contains("[raft]\n") && text.contains("[raft.wal]\n"),
        "{text}"
    );
    let text = text
        .replace(
            "[raft]\n",
            "[raft]\ndeleted_data_max_age = \"160s\"\ninstall_snapshot_timeout = \"20s\"\n",
        )
        .replace("[raft.wal]\n", "[raft.wal]\nsegment_size = \"64KiB\"\n")
        + "\n[storage.snapshot]\ns3_timeout = \"1500ms\"\ns3_max_retries = 1\n";
    std::fs::write(&node.config, text).expect("write node config");
}

/// How long phase 4 of the teardown and decommission test gives one meta
/// leadership's first sweep pass before it starts another leadership. A
/// pass asks each listed group at most a few seconds per peer.
const SWEEP_PASS_WAIT: Duration = Duration::from_secs(60);

/// Asks meta leader `from` to hand the meta leadership alone to the next
/// meta voter, so a new meta leadership begins while data leaderships stay
/// where they are.
async fn try_hand_meta_on(
    client: &reqwest::Client,
    nodes: &[MetaNode],
    from: u64,
) -> Result<(), String> {
    use ursula_proto::admin::TransferLeaderResponse;

    if !META_VOTERS.contains(&from) {
        return Err(format!("meta leader {from} is no meta voter"));
    }
    let to = META_VOTERS
        .iter()
        .cycle()
        .skip_while(|voter| **voter != from)
        .nth(1)
        .copied()
        .expect("another meta voter");
    let leader = nodes
        .iter()
        .find(|node| node.id == from)
        .ok_or_else(|| format!("no node {from}"))?;
    let url = format!("{}/__ursula/raft/meta/leader/transfer/{to}", leader.admin());
    let response = try_admin_test_post(client, url.clone())
        .await?
        .send()
        .await
        .map_err(|err| format!("POST {url}: {err}"))?;
    let status = response.status();
    let body = response
        .text()
        .await
        .map_err(|err| format!("POST {url}: {err}"))?;
    match serde_json::from_str::<TransferLeaderResponse>(&body) {
        Ok(answer) if status.is_success() && answer.transferred => Ok(()),
        _ => Err(format!("POST {url}: {status} {body}")),
    }
}

/// The snapshot objects node `node` owns of group `group`, under
/// `{prefix}/node-{node}/group-{group}/`.
async fn owned_objects(
    cold_store: &ColdStore,
    prefix: &ursula_runtime::SnapshotPrefix,
    node: u64,
    group: u32,
) -> Result<Vec<String>, String> {
    let dir = format!(
        "{}group-{group}/",
        ursula_runtime::snapshot_harness::node_directory(prefix, node)
    );
    let names = cold_store
        .list_file_names(&dir)
        .await
        .map_err(|err| s3_failure("list", &dir, &err))?;
    Ok(names
        .into_iter()
        .map(|name| format!("{dir}{name}"))
        .collect())
}

/// Every group of which node `node` owns a snapshot object.
async fn groups_with_objects(
    cold_store: &ColdStore,
    prefix: &ursula_runtime::SnapshotPrefix,
    node: u64,
) -> Result<BTreeSet<u32>, String> {
    let mut groups = BTreeSet::new();
    for group in 0..u32::try_from(META_TEST_GROUPS).unwrap() {
        if !owned_objects(cold_store, prefix, node, group)
            .await?
            .is_empty()
        {
            groups.insert(group);
        }
    }
    Ok(groups)
}

/// The snapshot records of data group `group` under `wal`, in its core
/// journals' directories (`raft-log/core-*`). The meta journal's record is
/// left out.
fn data_snapshot_records(wal: &Path, group: u32) -> Vec<PathBuf> {
    let name = format!("group-{group}.snapshot.json");
    std::fs::read_dir(wal.join("raft-log"))
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
        })
        .flat_map(|core| files_named(&core, &name))
        .collect()
}

/// The bootstrapped cluster a meta read barrier confirmed on the first of
/// `nodes` but `skip` that answers one.
async fn try_confirmed_cluster(
    client: &reqwest::Client,
    nodes: &[MetaNode],
    skip: &[u64],
) -> Result<Cluster, String> {
    let mut last = String::from("no node asked");
    for node in nodes.iter().filter(|node| !skip.contains(&node.id)) {
        match confirmed_placement_status(client, node).await {
            Ok(status) => match bootstrapped(&status) {
                Some(cluster) => return Ok(cluster.clone()),
                None => last = format!("{status:?}"),
            },
            Err(err) => last = err,
        }
    }
    Err(last)
}

/// [`try_confirmed_cluster`], until a node answers.
async fn wait_confirmed_cluster(
    client: &reqwest::Client,
    procs: &mut Processes,
    nodes: &[MetaNode],
    skip: &[u64],
) -> Cluster {
    eventually(procs, "a node confirms a bootstrapped view", async || {
        try_confirmed_cluster(client, nodes, skip).await
    })
    .await
}

fn lifecycle_of(cluster: &Cluster, node: u64) -> Option<ursula_proto::placement::NodeLifecycle> {
    cluster
        .node(NodeId::new(node))
        .map(|record| record.lifecycle)
}

/// Appends `body` to the stream at `url`, again until an append answers
/// `204`.
async fn append_until_done(client: &reqwest::Client, procs: &mut Processes, url: &str, body: &str) {
    let what = format!("{url} takes an append");
    eventually(procs, &what, async || {
        let response = client
            .post(url)
            .header("content-type", "text/plain")
            .body(body.to_owned())
            .send()
            .await
            .map_err(|err| format!("POST {url}: {err}"))?;
        if response.status() == reqwest::StatusCode::NO_CONTENT {
            Ok(())
        } else {
            Err(format!("POST {url}: {}", response.status()))
        }
    })
    .await;
}

/// Waits until a read of `url` answers a body that holds `needle`.
async fn wait_read_holds(client: &reqwest::Client, procs: &mut Processes, url: &str, needle: &str) {
    let what = format!("{url} holds {needle:?}");
    eventually(procs, &what, async || {
        let response = client
            .get(url)
            .send()
            .await
            .map_err(|err| format!("GET {url}: {err}"))?;
        if !response.status().is_success() {
            return Err(format!("GET {url}: {}", response.status()));
        }
        let body = response
            .bytes()
            .await
            .map_err(|err| format!("GET {url}: {err}"))?;
        if body
            .windows(needle.len())
            .any(|window| window == needle.as_bytes())
        {
            Ok(())
        } else {
            Err(format!("GET {url}: {}", String::from_utf8_lossy(&body)))
        }
    })
    .await;
}

/// Whether removed node `node` runs no data replica, keeps none open as
/// unplaced, reports `node_removed` while it answers ready, and sheds
/// `placement-unavailable`. Its metrics listing a replica of a group of
/// `never` fails the test.
async fn removed_and_closed(
    client: &reqwest::Client,
    node: &MetaNode,
    never: &BTreeSet<u32>,
) -> Result<(), String> {
    use ursula_proto::admin::RaftMaintenanceIssue;

    let metrics = try_node_metrics(client, &node.url()).await?;
    let running: BTreeSet<u64> = metrics
        .groups
        .iter()
        .map(|group| group.raft_group_id)
        .collect();
    if let Some(group) = running
        .iter()
        .find(|group| never.iter().any(|swept| u64::from(*swept) == **group))
    {
        panic!(
            "removed node {} runs a replica of group {group}, whose snapshot objects the sweep \
             took: {:?}",
            node.id, metrics.groups
        );
    }
    if !running.is_empty() {
        return Err(format!("runs replicas of {running:?}"));
    }
    let (status, ready) = readiness(client, node).await?;
    let report = ready
        .raft_maintenance
        .as_ref()
        .ok_or_else(|| "no maintenance report".to_owned())?;
    if status != reqwest::StatusCode::OK
        || !report
            .node_issues
            .contains(&RaftMaintenanceIssue::NodeRemoved)
        || !report.unplaced.is_empty()
    {
        return Err(format!("{status} {ready:?}"));
    }
    let shed = leadership_shed(client, node).await;
    if !sheds_placement_unavailable(&shed) {
        return Err(format!("{shed:?}"));
    }
    Ok(())
}

/// Waits until [`removed_and_closed`] holds for node `node` on every poll
/// for `hold`.
async fn wait_removed_and_closed(
    client: &reqwest::Client,
    procs: &mut Processes,
    node: &MetaNode,
    never: &BTreeSet<u32>,
    hold: Duration,
) {
    let what = format!("removed node {} closes every replica", node.id);
    let mut since: Option<std::time::Instant> = None;
    eventually_for(
        procs,
        &what,
        LIFECYCLE_WAIT,
        async || match removed_and_closed(client, node, never).await {
            Ok(()) => {
                let closed = since.get_or_insert_with(std::time::Instant::now).elapsed();
                if closed >= hold {
                    Ok(())
                } else {
                    Err(format!("closed for {closed:?} of {hold:?}"))
                }
            }
            Err(err) => {
                since = None;
                Err(err)
            }
        },
    )
    .await;
}

/// Waits until node `node` reports readiness `superseded` and runs no data
/// replica, on every poll for `hold`. Its metrics listing a replica of a
/// group of `never` fails the test.
async fn wait_superseded_and_closed(
    client: &reqwest::Client,
    procs: &mut Processes,
    node: &MetaNode,
    never: &BTreeSet<u32>,
    hold: Duration,
) {
    let what = format!("node {} is superseded and runs nothing", node.id);
    let mut since: Option<std::time::Instant> = None;
    eventually_for(procs, &what, LIFECYCLE_WAIT, async || {
        let metrics = try_node_metrics(client, &node.admin()).await?;
        if let Some(group) = metrics.groups.iter().find(|group| {
            never
                .iter()
                .any(|swept| u64::from(*swept) == group.raft_group_id)
        }) {
            panic!(
                "node {} runs a replica of group {}, whose snapshot objects the pass took",
                node.id, group.raft_group_id
            );
        }
        let ready = admin_get_json::<ursula_proto::admin::ServingReadiness>(
            client,
            node,
            "/__ursula/ready",
        )
        .await?;
        let superseded =
            ready.reason == Some(ursula_proto::admin::ServingReadinessReason::Superseded);
        if !superseded || !metrics.groups.is_empty() {
            since = None;
            return Err(format!("{ready:?} {:?}", metrics.groups));
        }
        let closed = since.get_or_insert_with(std::time::Instant::now).elapsed();
        if closed >= hold {
            Ok(())
        } else {
            Err(format!("closed for {closed:?} of {hold:?}"))
        }
    })
    .await;
}

/// Asserts that a move of the first group of `cluster` that puts node
/// `removed`, removed or vacant, in place of the group's lowest voter is
/// refused through `via` with `409` and `expected`, applying nothing.
async fn assert_move_refused_as_removed(
    via: &ursula_ctl::NodeInfo,
    cluster: &Cluster,
    removed: u64,
    expected: ursula_control::ControlError,
) {
    use ursula_control::PlacementApiReason;
    use ursula_ctl::placement::WriteAnswer;

    let (group, placement) = cluster.groups().iter().next().expect("a group");
    let voters = placement.current_voters();
    let mut to = voters.to_set();
    to.remove(&voters.min());
    to.insert(NodeId::new(removed));
    let request = ursula_control::MoveRequest {
        expected: placement.since(),
        to: ursula_proto::placement::VoterSet::new(to).expect("a voter set"),
    };
    let ctl = ursula_ctl::MetricsClient::new(Duration::from_secs(5)).expect("ctl client");
    let path = format!("/__ursula/placement/groups/{}/move", group.0);
    match ursula_ctl::placement::post_write::<_, ursula_control::MoveResponse>(
        &ctl, via, &path, &request,
    )
    .await
    {
        WriteAnswer::Refused { status, refusal } => {
            assert_eq!(status, reqwest::StatusCode::CONFLICT, "{refusal:?}");
            assert!(
                matches!(&refusal.reason, PlacementApiReason::Refused(error) if *error == expected),
                "{refusal:?}"
            );
            assert_eq!(
                refusal.write_effect,
                ursula_proto::placement::WriteEffect::NotApplied,
                "{refusal:?}"
            );
        }
        other => panic!("a move naming removed node {removed} answered {other:?}"),
    }
}

/// S4(b) and S5 (#768), integration 1's teardown half and 5 (design 15.3),
/// in phases on one cluster of five nodes at replication factor 3 with meta
/// voters 1 to 3, on S3. Every move and lifecycle change goes through
/// `ursula_ctl` library calls over the admin plane. Runs on real S3 in CI.
///
/// 1. Group 1, seeded on nodes 2 to 4, takes a write that node 4's journal
///    holds, and node 4 snapshots it, which writes its snapshot record and
///    its own object of the group. `move --replace 4=5` converges, and node
///    4 tears its replica down: the retirement leaves the view and node 4
///    runs no replica of the group. Then node 4 keeps no snapshot record and
///    no object of the group, and once later writes to its other groups
///    sealed the segments that held the write, no file of its WAL directory
///    holds it, while the group still serves it. A teardown that keeps the
///    record or the object, or a retirement that leaves the log's frames
///    live, fails it.
/// 2. `verify-removed` refuses node 4 while it is active. `node cordon` and
///    `node uncordon` round-trip, and node 4 snapshots group 2. `node
///    decommission --wait` ends with node 4 removed and without a role.
///    Node 4 then runs no data replica, keeps none open, reports
///    `node_removed` while it answers ready and sheds
///    `placement-unavailable`. A move naming it answers `409 node_removed`
///    and applies nothing, and `verify-removed` passes. A removed node that
///    keeps a replica open fails it.
/// 3. Node 5 snapshots group 1, so it records and owns an object of the
///    group. It is killed, and the groups it holds a snapshot record and an
///    object of are those the sweep takes. Node 5 is decommissioned: it is
///    removed once its last group converged without it, and every group
///    ends on nodes 1 to 3 and takes writes.
/// 4. The meta leader hands the meta leadership alone to another meta
///    voter, so data leaderships stay put, and the new leader's sweep runs
///    at once (a pass runs when a leadership begins). A pass that leaves a
///    group or confirms no view passes again only after the sweep
///    interval, so while objects remain the test hands meta on again after
///    a pass left a group, or after a minute. Both removed nodes' snapshot
///    directories end empty. Node 5 is dead, so only the sweep deletes its object of group
///    1, and a sweep that keeps it fails the test. Node 4's directory ends
///    empty too, but node 4 was decommissioning, not removed, while its
///    groups moved away, so its own teardowns may have deleted its objects
///    before the sweep ran: node 4's half proves nothing of the sweep.
///    Node 5 is dead, so its removal completes and its id turns vacant,
///    while node 4's waits for its running process.
/// 5. Node 5 restarts with its old disk, which its vacant id no longer
///    binds: it reports `superseded` and runs no data replica, and stays so
///    for longer than its asker's interval. Its metrics never list a replica
///    of a group whose object the sweep took, group 1 among them, while the
///    test polls them. A move naming it answers `409 node_vacant`,
///    `verify-removed` passes for nodes 4 and 5, and `verify-joinable` waits
///    for node 4. Every group still serves.
/// 6. A new disk under id 5 binds a new root, takes a group the old disk
///    holds no snapshot record of, and stores its own object of it. The old
///    disk starts again where it reaches S3 and no meta leader, past its
///    predecessor wait and a GC pass, and deletes nothing of the new disk's
///    (decision 15, B1). The new disk restarts with the group.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cli_a_torn_down_source_and_decommissioned_nodes_keep_nothing_of_their_groups() {
    use ursula_ctl::placement::lifecycle::LifecycleEnd;
    use ursula_ctl::placement::lifecycle::LifecycleOptions;
    use ursula_ctl::placement::lifecycle::Removal;
    use ursula_ctl::placement::lifecycle::RemovalRefusal;
    use ursula_ctl::placement::moves::GroupEnd;
    use ursula_ctl::placement::moves::Intent;
    use ursula_ctl::placement::moves::MoveOptions;
    use ursula_ctl::placement::moves::Replacement;
    use ursula_proto::placement::NodeLifecycle;

    let _guard = static_cluster_cli_test_guard().await;
    if std::env::var("URSULA_COLD_S3_INTEGRATION").ok().as_deref() != Some("1") {
        tracing::warn!(
            "skipping the S3 teardown and decommission test; set URSULA_COLD_S3_INTEGRATION=1 and URSULA_COLD_S3_BUCKET"
        );
        return;
    }
    let Some(binary) = option_env!("CARGO_BIN_EXE_ursula") else {
        tracing::warn!("CARGO_BIN_EXE_ursula is not set; skipping the teardown test");
        return;
    };
    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time after unix epoch")
        .as_nanos();
    // CI scopes each job's credentials to URSULA_S3_PREFIX.
    let cold_root = match std::env::var("URSULA_S3_PREFIX") {
        Ok(prefix) => format!("{prefix}/ursula-cli-s3-lifecycle/{suffix}"),
        Err(_) => format!("ursula-cli-s3-lifecycle/{suffix}"),
    };
    let root = tempfile::tempdir().expect("temp root");
    let mut nodes = MetaNode::cluster(root.path(), 5);
    for node in &mut nodes {
        node.cold_root = Some(cold_root.clone());
    }
    let nodes = nodes;
    let peers = peers_of(&nodes);
    let meta = MetaToml {
        meta_voters: Some(META_VOTERS),
        replication_factor: Some(3),
        ..MetaToml::default()
    };
    for node in &nodes {
        node.write(&peers, meta);
    }
    bound_dead_journal_data(&nodes[3]);
    let config = ursula_config::load_config(Some(&nodes[0].config), None, None)
        .unwrap_or_else(|err| panic!("load config for the cold store: {err}"));
    config
        .validate()
        .unwrap_or_else(|err| panic!("the node config is valid: {err}"));
    let prefix = config.storage.snapshot.snapshot_prefix();
    let cold_store = ColdStore::try_new(&config.storage.cold)
        .unwrap_or_else(|err| panic!("{}", s3_failure("open the cold store", "", &err)));
    // Every request ends within 10 s, so a request to a killed node fails
    // instead of hanging.
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .expect("HTTP client");
    // A fresh client per command: each pins the process incarnation of the
    // nodes it writes to, and node 5 restarts.
    let ctl = || ursula_ctl::MetricsClient::new(Duration::from_secs(5)).expect("ctl client");
    let manifest = |skip: &[u64]| -> Vec<ursula_ctl::NodeInfo> {
        nodes
            .iter()
            .filter(|node| !skip.contains(&node.id))
            .map(MetaNode::ctl)
            .collect()
    };
    let lifecycle_options = |ids: &[u64], wait: bool| LifecycleOptions {
        nodes: ids.iter().copied().map(NodeId::new).collect(),
        wait,
        timeout: LIFECYCLE_WAIT,
        poll_interval: Duration::from_millis(500),
    };
    let mut procs = Processes::default();
    for node in &nodes {
        procs.start(binary, node);
    }
    wait_settled(&client, &mut procs, &nodes).await;
    let groups = u32::try_from(META_TEST_GROUPS).unwrap();
    let streams = stream_per_group("lifecycle");
    let url = |node: &MetaNode, group: u32| {
        format!(
            "{}/lifecycle/{}",
            node.url(),
            streams[usize::try_from(group).unwrap()]
        )
    };
    let read =
        |node: &MetaNode, group: u32| format!("{}?offset=0&max_bytes=1048576", url(node, group));
    for group in 0..groups {
        put_until_created(&client, &url(&nodes[0], group)).await;
    }

    // 1. Group 1 moves off node 4, which tears its replica down.
    let moved = RaftGroupId(1);
    let source = &nodes[3];
    let cluster = wait_confirmed_cluster(&client, &mut procs, &nodes, &[]).await;
    assert_eq!(
        current_voters(&cluster).get(&moved),
        Some(&BTreeSet::from([2, 3, 4])),
        "{cluster:?}"
    );
    post_until_no_content(&client, &url(&nodes[0], 1), DELETED_MARKER).await;
    eventually(
        &mut procs,
        "node 4's journal holds group 1's write",
        async || {
            if wal_files_with_marker(&source.wal).is_empty() {
                Err("no file holds it".to_owned())
            } else {
                Ok(())
            }
        },
    )
    .await;
    snapshot_group(&client, &source.admin(), 1).await;
    eventually(
        &mut procs,
        "node 4 records and stores its snapshot of group 1",
        async || {
            let objects = owned_objects(&cold_store, &prefix, 4, 1).await?;
            let records = data_snapshot_records(&source.wal, 1);
            if objects.is_empty() || records.is_empty() {
                Err(format!("objects {objects:?}, records {records:?}"))
            } else {
                Ok(())
            }
        },
    )
    .await;
    let mut out = Vec::new();
    let ends = ursula_ctl::placement::moves::run_moves(
        &ctl(),
        &manifest(&[]),
        &MoveOptions {
            groups: vec![moved],
            intent: Intent::Replace(Replacement {
                old: NodeId::new(4),
                new: NodeId::new(5),
            }),
            expect: None,
            retarget: false,
            allow_not_live: false,
            wait: true,
            timeout: LIFECYCLE_WAIT,
            poll_interval: Duration::from_millis(500),
        },
        &mut out,
    )
    .await
    .expect("move group 1 off node 4");
    let out = String::from_utf8_lossy(&out);
    assert!(
        matches!(ends.get(&moved), Some(GroupEnd::Converged { .. })),
        "{ends:?}\n{out}"
    );
    let cluster = wait_confirmed_cluster(&client, &mut procs, &nodes, &[]).await;
    assert_eq!(
        current_voters(&cluster).get(&moved),
        Some(&BTreeSet::from([2, 3, 5])),
        "{cluster:?}"
    );
    eventually_for(
        &mut procs,
        "node 4 tears down its replica of group 1",
        LIFECYCLE_WAIT,
        async || {
            let cluster = try_confirmed_cluster(&client, &nodes, &[]).await?;
            if cluster.retirement(moved, NodeId::new(4)).is_some() {
                return Err(format!("retirements {:?}", cluster.retirements()));
            }
            let metrics = try_node_metrics(&client, &source.url()).await?;
            if metrics.groups.iter().any(|group| group.raft_group_id == 1) {
                return Err(format!("{:?}", metrics.groups));
            }
            Ok(())
        },
    )
    .await;
    eventually_for(
        &mut procs,
        "node 4 keeps no snapshot record or object of group 1",
        LIFECYCLE_WAIT,
        async || {
            let objects = owned_objects(&cold_store, &prefix, 4, 1).await?;
            let records = data_snapshot_records(&source.wal, 1);
            if objects.is_empty() && records.is_empty() {
                Ok(())
            } else {
                Err(format!("objects {objects:?}, records {records:?}"))
            }
        },
    )
    .await;
    // Writes to node 4's other groups, 2 and 3, seal the segments that hold
    // group 1's frames.
    let filler = "f".repeat(8192);
    for group in [2, 3] {
        for _ in 0..16 {
            append_until_done(&client, &mut procs, &url(&nodes[0], group), &filler).await;
        }
    }
    eventually_for(
        &mut procs,
        "no file of node 4's WAL directory holds group 1's write",
        JOURNAL_RECLAIM_WAIT,
        async || {
            let found = wal_files_with_marker(&source.wal);
            if found.is_empty() {
                Ok(())
            } else {
                Err(format!("{found:?}"))
            }
        },
    )
    .await;
    wait_read_holds(&client, &mut procs, &read(&nodes[4], 1), DELETED_MARKER).await;

    // 2. Node 4, live, is decommissioned and closes every replica.
    let refusal = ursula_ctl::placement::lifecycle::verify_removal(
        &ctl(),
        &manifest(&[]),
        &BTreeSet::from([NodeId::new(4)]),
        Removal::Leaving,
        META_WAIT,
        Duration::from_millis(500),
        &mut Vec::new(),
    )
    .await
    .expect_err("verify-removed refuses an active node");
    assert!(
        matches!(
            refusal.downcast_ref::<RemovalRefusal>(),
            Some(RemovalRefusal::NotRemoved {
                node,
                lifecycle: NodeLifecycle::Active,
            }) if *node == NodeId::new(4)
        ),
        "{refusal:?}"
    );
    for (change, lifecycle) in [
        (
            ursula_control::LifecycleChange::Cordon,
            NodeLifecycle::Cordoned,
        ),
        (
            ursula_control::LifecycleChange::Uncordon,
            NodeLifecycle::Active,
        ),
    ] {
        let mut out = Vec::new();
        let end = ursula_ctl::placement::lifecycle::set_lifecycles(
            &ctl(),
            &manifest(&[]),
            change,
            &lifecycle_options(&[4], false),
            &mut out,
        )
        .await
        .unwrap_or_else(|err| panic!("{change:?} node 4: {err:?}"));
        assert_eq!(end, LifecycleEnd::Done, "{}", String::from_utf8_lossy(&out));
        let cluster = wait_confirmed_cluster(&client, &mut procs, &nodes, &[]).await;
        assert_eq!(lifecycle_of(&cluster, 4), Some(lifecycle), "{cluster:?}");
    }
    snapshot_group(&client, &source.admin(), 2).await;
    eventually(
        &mut procs,
        "node 4 stores its snapshot of group 2",
        async || {
            if owned_objects(&cold_store, &prefix, 4, 2).await?.is_empty() {
                Err("no object".to_owned())
            } else {
                Ok(())
            }
        },
    )
    .await;
    let mut out = Vec::new();
    let end = ursula_ctl::placement::lifecycle::decommission(
        &ctl(),
        &manifest(&[]),
        &lifecycle_options(&[4], true),
        &mut out,
    )
    .await
    .expect("decommission node 4");
    let out = String::from_utf8_lossy(&out);
    assert_eq!(end, LifecycleEnd::Done, "{out}");
    assert!(
        out.contains(ursula_ctl::placement::lifecycle::IRREVERSIBLE),
        "{out}"
    );
    let cluster = wait_confirmed_cluster(&client, &mut procs, &nodes, &[]).await;
    assert_eq!(
        lifecycle_of(&cluster, 4),
        Some(NodeLifecycle::Removed),
        "{cluster:?}"
    );
    assert!(!cluster.has_any_role(NodeId::new(4)), "{cluster:?}");
    wait_removed_and_closed(
        &client,
        &mut procs,
        source,
        &BTreeSet::new(),
        Duration::ZERO,
    )
    .await;
    assert_move_refused_as_removed(
        &nodes[0].ctl(),
        &cluster,
        4,
        ursula_control::ControlError::NodeRemoved {
            node: NodeId::new(4),
        },
    )
    .await;
    ursula_ctl::placement::lifecycle::verify_removal(
        &ctl(),
        &manifest(&[]),
        &BTreeSet::from([NodeId::new(4)]),
        Removal::Leaving,
        META_WAIT,
        Duration::from_millis(500),
        &mut Vec::new(),
    )
    .await
    .expect("verify-removed passes for node 4");

    // 3. Node 5 records and owns a snapshot of group 1, is killed and is
    // decommissioned.
    let dead = &nodes[4];
    let held = placed_on(&cluster, 5);
    assert!(held.contains(&1), "{cluster:?}");
    snapshot_group(&client, &dead.admin(), 1).await;
    eventually(
        &mut procs,
        "node 5 records and stores its snapshot of group 1",
        async || {
            let objects = owned_objects(&cold_store, &prefix, 5, 1).await?;
            let records = data_snapshot_records(&dead.wal, 1);
            if objects.is_empty() || records.is_empty() {
                Err(format!("objects {objects:?}, records {records:?}"))
            } else {
                Ok(())
            }
        },
    )
    .await;
    procs.kill(5);
    // The groups whose snapshot record on node 5's disk names an object the
    // sweep takes. An object without a record (a build cut short by the
    // kill) is left out: its group opens from the log.
    let objects = eventually(&mut procs, "list node 5's snapshot objects", async || {
        groups_with_objects(&cold_store, &prefix, 5).await
    })
    .await;
    let swept: BTreeSet<u32> = objects
        .into_iter()
        .filter(|group| !data_snapshot_records(&dead.wal, *group).is_empty())
        .collect();
    assert!(swept.contains(&1), "{swept:?}");
    let mut out = Vec::new();
    let end = ursula_ctl::placement::lifecycle::decommission(
        &ctl(),
        &manifest(&[5]),
        &lifecycle_options(&[5], true),
        &mut out,
    )
    .await
    .expect("decommission node 5");
    assert_eq!(end, LifecycleEnd::Done, "{}", String::from_utf8_lossy(&out));
    let cluster = wait_confirmed_cluster(&client, &mut procs, &nodes, &[5]).await;
    assert_eq!(
        lifecycle_of(&cluster, 5),
        Some(NodeLifecycle::Removed),
        "{cluster:?}"
    );
    for (group, voters) in current_voters(&cluster) {
        assert_eq!(
            voters,
            BTreeSet::from([1, 2, 3]),
            "group {}: {cluster:?}",
            group.0
        );
    }
    for group in 0..groups {
        append_until_done(&client, &mut procs, &url(&nodes[0], group), "after-node-5;").await;
        wait_read_holds(
            &client,
            &mut procs,
            &read(&nodes[1], group),
            "after-node-5;",
        )
        .await;
    }

    // 4. Another meta voter leads meta, and its pass takes both removed
    // nodes' objects. Node 5 is dead, so its removal completes and its id
    // turns vacant. Node 4 still runs, so its removal waits for its process.
    let members = manifest(&[4, 5]);
    // The leader the test last handed meta away from, and when.
    let mut handed: Option<(NodeId, std::time::Instant)> = None;
    eventually_for(
        &mut procs,
        "the pass takes the removed nodes' snapshot objects",
        LIFECYCLE_WAIT,
        async || {
            let status = ursula_ctl::view::leader_status(&ctl(), &members)
                .await
                .map_err(|err| format!("{err:?}"))?
                .map_err(|refusal| format!("{refusal:?}"))?;
            let vacant = bootstrapped(&status).and_then(|cluster| lifecycle_of(cluster, 5))
                == Some(NodeLifecycle::Vacant);
            let report = status
                .leader_report
                .ok_or_else(|| "no leader report".to_owned())?;
            let mut left = BTreeMap::new();
            for node in [4, 5] {
                let groups = groups_with_objects(&cold_store, &prefix, node).await?;
                if !groups.is_empty() {
                    left.insert(node, groups);
                }
            }
            let waits_for_its_process =
                report
                    .removal_pending
                    .get(&NodeId::new(4))
                    .is_some_and(|pending| {
                        pending.process_answers
                            && pending.directory == ursula_control::RemovalDirectory::Swept
                            && pending.unconfirmed.is_empty()
                            && !pending.meta_member
                    });
            if left.is_empty()
                && vacant
                && waits_for_its_process
                && report.removal_pending.len() == 1
            {
                return Ok(());
            }
            // A new leadership passes at once. Its pass left a group, or
            // it has had a minute: start another leadership.
            let pass_again = handed.is_none_or(|(from, at)| {
                at.elapsed() >= SWEEP_PASS_WAIT || (report.leader != from && !left.is_empty())
            });
            if pass_again {
                try_hand_meta_on(&client, &nodes, report.leader.get()).await?;
                handed = Some((report.leader, std::time::Instant::now()));
            }
            Err(format!(
                "objects of groups {left:?}, node 5 vacant {vacant}, removal pending {:?} on meta \
                 leader {}",
                report.removal_pending,
                report.leader.get()
            ))
        },
    )
    .await;
    let old5 = match wait_confirmed_cluster(&client, &mut procs, &nodes, &[5])
        .await
        .node(NodeId::new(5))
        .map(|record| record.root)
    {
        Some(ursula_proto::placement::RootSlot::Released(root)) => root,
        other => panic!("node 5's id is vacant on its released root: {other:?}"),
    };

    // 5. Node 5 restarts with its old disk, which its vacant id no longer
    // binds: it is superseded and runs nothing, never a replica of a group
    // whose object the pass took.
    procs.start(binary, dead);
    wait_superseded_and_closed(
        &client,
        &mut procs,
        dead,
        &swept,
        ursula_raft::hosting::ASK_INTERVAL
            .checked_add(Duration::from_secs(5))
            .unwrap(),
    )
    .await;
    let cluster = wait_confirmed_cluster(&client, &mut procs, &nodes, &[]).await;
    assert_move_refused_as_removed(
        &nodes[0].ctl(),
        &cluster,
        5,
        ursula_control::ControlError::NodeVacant {
            node: NodeId::new(5),
        },
    )
    .await;
    ursula_ctl::placement::lifecycle::verify_removal(
        &ctl(),
        &manifest(&[]),
        &BTreeSet::from([NodeId::new(4), NodeId::new(5)]),
        Removal::Leaving,
        META_WAIT,
        Duration::from_millis(500),
        &mut Vec::new(),
    )
    .await
    .expect("verify-removed passes for removed node 4 and vacant node 5");
    let refusal = ursula_ctl::placement::lifecycle::verify_removal(
        &ctl(),
        &manifest(&[]),
        &BTreeSet::from([NodeId::new(4)]),
        Removal::Joining,
        Duration::from_secs(5),
        Duration::from_millis(500),
        &mut Vec::new(),
    )
    .await
    .expect_err("verify-joinable waits while node 4's process runs");
    assert!(
        matches!(
            refusal.downcast_ref::<RemovalRefusal>(),
            Some(RemovalRefusal::RemovalPending { node, .. }) if *node == NodeId::new(4)
        ),
        "{refusal:?}"
    );
    for group in 0..groups {
        append_until_done(
            &client,
            &mut procs,
            &url(&nodes[0], group),
            "after-restart;",
        )
        .await;
        wait_read_holds(
            &client,
            &mut procs,
            &read(&nodes[2], group),
            "after-restart;",
        )
        .await;
    }

    // 6. A new disk under id 5 takes a group the old disk holds no snapshot
    // record of, and stores its own snapshot object of it. The old disk
    // then starts where it reaches S3 and no meta leader: its start pass
    // finds the object in the directory of a group it does not hold, or its
    // replica restores the group from its log and owes a GC pass, and
    // either deletes nothing, since meta never answered the run. The new
    // disk restarts with the group from its object.
    procs.kill(5);
    let old_disk = MetaNode {
        id: 5,
        port: dead.port,
        admin_port: dead.admin_port,
        config: root.path().join("node-5-old.toml"),
        wal: root.path().join("node-5-old-log"),
        cold_root: Some(cold_root.clone()),
    };
    std::fs::rename(&dead.wal, &old_disk.wal).expect("move node 5's old disk aside");
    // Every peer but itself at an address nothing listens on.
    let unreachable: Vec<(u64, String)> = nodes
        .iter()
        .map(|node| {
            if node.id == 5 {
                node.peer()
            } else {
                (node.id, format!("http://127.0.0.1:{}", free_port()))
            }
        })
        .collect();
    old_disk.write(&unreachable, meta);
    let log_only = (0..groups)
        .find(|group| data_snapshot_records(&old_disk.wal, *group).is_empty())
        .expect("node 5's old disk holds no snapshot record of some group");
    let reused = RaftGroupId(log_only);
    procs.start(binary, dead);
    wait_placement(
        &client,
        &mut procs,
        dead,
        "binds a new root under its vacant id",
        |status| {
            matches!(
                &status.registration,
                RegistrationStatus::Answered(Registered::Unreferenced(record))
                    if record.root != ursula_proto::placement::RootSlot::Released(old5)
            )
        },
    )
    .await;
    wait_eligible(&mut procs, &nodes, &[4], &[5]).await;
    let cluster = wait_confirmed_cluster(&client, &mut procs, &nodes, &[]).await;
    let voters = current_voters(&cluster)
        .get(&reused)
        .cloned()
        .expect("the group has voters");
    let out_voter = voters.iter().copied().max().expect("a voter");
    let mut to = voters.clone();
    to.remove(&out_voter);
    to.insert(5);
    move_converges(&nodes, &[4], reused, replace(out_voter, 5), &to).await;
    snapshot_group(&client, &dead.admin(), log_only).await;
    let named = eventually(
        &mut procs,
        "the new disk records and stores its own snapshot of the group",
        async || {
            let objects = owned_objects(&cold_store, &prefix, 5, log_only).await?;
            if objects.is_empty() || data_snapshot_records(&dead.wal, log_only).is_empty() {
                Err(format!("objects {objects:?}"))
            } else {
                Ok(objects)
            }
        },
    )
    .await;
    procs.kill(5);
    procs.start(binary, &old_disk);
    // Past the predecessor wait the old disk owes, which refuses its start
    // pass and its GC pass after the restore, and past the next attempt of
    // each, 30 s later (the snapshot GC retry), the new disk's object stays.
    let kept_until = std::time::Instant::now()
        .checked_add(ursula_raft::predecessor_wait::PREDECESSOR_WAIT)
        .and_then(|after| after.checked_add(Duration::from_secs(30 + 20)))
        .unwrap();
    while std::time::Instant::now() < kept_until {
        let objects = owned_objects(&cold_store, &prefix, 5, log_only)
            .await
            .unwrap_or_else(|err| panic!("list node 5's objects: {err}"));
        assert!(
            named.iter().all(|object| objects.contains(object)),
            "the old disk deleted the new disk's objects {named:?}, left {objects:?}"
        );
        if let Some(exited) = procs.exited() {
            panic!("a node exited: {exited}");
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    let status = placement_status(&client, &old_disk)
        .await
        .expect("the old disk answers its placement status");
    assert_eq!(
        status.registration,
        RegistrationStatus::Pending,
        "{status:?}"
    );
    procs.kill(5);
    procs.start(binary, dead);
    let cluster = wait_confirmed_cluster(&client, &mut procs, &nodes, &[4]).await;
    wait_serves_placed(&client, &mut procs, dead, &cluster).await;
    append_until_done(
        &client,
        &mut procs,
        &url(&nodes[0], log_only),
        "after-the-old-disk;",
    )
    .await;
    wait_read_holds(
        &client,
        &mut procs,
        &read(dead, log_only),
        "after-the-old-disk;",
    )
    .await;

    drop(procs);
    cold_store
        .remove_all("")
        .await
        .unwrap_or_else(|err| panic!("{}", s3_failure("clear the test root", "", &err)));
}

/// Decision 15 (#768): a removed node id runs again on a new disk once its
/// removal completed, on one cluster of five nodes at replication factor 3
/// with meta voters 1 to 3 and no S3, so the snapshot half of a removal is
/// trivial. Every lifecycle change and move goes through `ursula_ctl`.
///
/// 1. Every group takes a write. `node decommission --node 5 --wait` ends
///    with node 5 removed.
/// 2. Two voters of a group that node 4 votes in, node 4 among them and at
///    most one meta voter, are killed: that group has no live leader. Node
///    5 still runs, so its removal waits for its process. The meta
///    leadership moves, the new leader asks every group again, and reports
///    the leaderless group unconfirmed, so no removal completes. Node 5 is
///    killed and its WAL directory is moved aside (the old disk).
/// 3. Node 5 starts on an empty WAL and keeps running. Its registration and
///    its readiness report `removal_pending` (503), and its data replicas
///    take part in nothing. `verify-joinable` times out naming the
///    leaderless group, which the meta leader's report lists as unconfirmed,
///    with no process of the old root answering: the new disk answers at
///    node 5's address on another root.
/// 4. The two nodes restart. The same process of the new disk binds a new
///    root once the removal completed, with no restart, and while it waits
///    the meta leader never counts it a process of the old root.
///    `verify-joinable` passes and the meta leader reports no pending
///    removal. (The S3 test of decision 15 starts the old disk on the
///    vacant id.)
/// 5. The new disk waits out its predecessor and turns ready with no group.
///    Two groups move onto it, one of them is led by it, and every group
///    takes a write that node 5 reads.
/// 6. Node 5 is killed and the old disk starts: it is refused for the new
///    root and superseded, while the groups on node 5 keep taking writes.
///    Then the new disk rejoins with its log and serves its groups.
///
/// A removal that never completes, a removed id that never binds again, an
/// early disk that binds before the completion or only after a restart, or
/// an old disk that takes part again fails it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cli_a_removed_id_runs_again_on_a_new_disk() {
    use ursula_ctl::placement::lifecycle::LifecycleEnd;
    use ursula_ctl::placement::lifecycle::LifecycleOptions;
    use ursula_ctl::placement::lifecycle::Removal;
    use ursula_ctl::placement::lifecycle::RemovalRefusal;
    use ursula_proto::admin::ServingReadinessReason;
    use ursula_proto::placement::NodeLifecycle;
    use ursula_proto::placement::RootSlot;

    let _guard = static_cluster_cli_test_guard().await;
    let Some(binary) = option_env!("CARGO_BIN_EXE_ursula") else {
        tracing::warn!("CARGO_BIN_EXE_ursula is not set; skipping the reused id test");
        return;
    };
    let root = tempfile::tempdir().expect("temp root");
    let nodes = MetaNode::cluster(root.path(), 5);
    let peers = peers_of(&nodes);
    let meta = MetaToml {
        meta_voters: Some(META_VOTERS),
        replication_factor: Some(3),
        ..MetaToml::default()
    };
    for node in &nodes {
        node.write(&peers, meta);
    }
    // The old disk of node 5: its ports, its own config and WAL directory.
    let old_disk = MetaNode {
        id: 5,
        port: nodes[4].port,
        admin_port: nodes[4].admin_port,
        config: root.path().join("node-5-old.toml"),
        wal: root.path().join("node-5-old-log"),
        cold_root: None,
    };
    old_disk.write(&peers, meta);
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .expect("HTTP client");
    let lifecycle_options = |ids: &[u64], wait: bool| LifecycleOptions {
        nodes: ids.iter().copied().map(NodeId::new).collect(),
        wait,
        timeout: LIFECYCLE_WAIT,
        poll_interval: Duration::from_millis(500),
    };
    let mut procs = Processes::default();
    for node in &nodes {
        procs.start(binary, node);
    }
    wait_settled(&client, &mut procs, &nodes).await;
    let groups = u32::try_from(META_TEST_GROUPS).unwrap();
    let streams = stream_per_group("reuse");
    let url = |node: &MetaNode, group: u32| {
        format!(
            "{}/reuse/{}",
            node.url(),
            streams[usize::try_from(group).unwrap()]
        )
    };
    let read =
        |node: &MetaNode, group: u32| format!("{}?offset=0&max_bytes=1048576", url(node, group));
    for group in 0..groups {
        put_until_created(&client, &url(&nodes[0], group)).await;
        append_until_done(&client, &mut procs, &url(&nodes[0], group), "before;").await;
    }
    let cluster = wait_confirmed_cluster(&client, &mut procs, &nodes, &[]).await;
    let old = bound_root(&cluster, 5);

    // 1. Node 5 is decommissioned.
    let mut out = Vec::new();
    let end = ursula_ctl::placement::lifecycle::decommission(
        &ctl_client(),
        &ctl_manifest(&nodes, &[]),
        &lifecycle_options(&[5], true),
        &mut out,
    )
    .await
    .expect("decommission node 5");
    assert_eq!(end, LifecycleEnd::Done, "{}", String::from_utf8_lossy(&out));
    let cluster = wait_confirmed_cluster(&client, &mut procs, &nodes, &[]).await;
    assert_eq!(
        lifecycle_of(&cluster, 5),
        Some(NodeLifecycle::Removed),
        "{cluster:?}"
    );

    // 2. A group loses its live leader, then node 5 and its disk go.
    let (blocked, voters) = current_voters(&cluster)
        .into_iter()
        .find(|(_, voters)| voters.contains(&4))
        .expect("node 4 votes in a group");
    let partner = voters
        .iter()
        .copied()
        .filter(|voter| *voter != 4)
        .max()
        .expect("a second voter");
    for down in [4, partner] {
        procs.kill(down);
    }
    let down: Vec<u64> = [4, partner].into();
    // A meta leadership keeps the groups that confirmed node 5 no member,
    // and node 5's process still answering kept its removal from
    // completing. A new meta leadership asks every group again, and the
    // group without a leader confirms nothing.
    let alive_voters: Vec<u64> = META_VOTERS
        .iter()
        .copied()
        .filter(|voter| !down.contains(voter))
        .collect();
    let first = known_meta_leader(
        &client,
        &mut procs,
        &nodes,
        node_of(&nodes, alive_voters[0]),
    )
    .await;
    let to = alive_voters
        .iter()
        .copied()
        .find(|voter| *voter != first)
        .expect("a second meta voter runs");
    eventually(&mut procs, "the meta leadership moves", async || {
        let status = placement_status(&client, node_of(&nodes, to)).await?;
        if meta_leader(&status) == Some(to) {
            return Ok(());
        }
        let url = format!(
            "{}/__ursula/raft/meta/leader/transfer/{to}",
            node_of(&nodes, first).admin()
        );
        let response = try_admin_test_post(&client, url.clone())
            .await?
            .send()
            .await
            .map_err(|err| format!("POST {url}: {err}"))?;
        Err(format!("POST {url}: {}", response.status()))
    })
    .await;
    let members = ctl_manifest(&nodes, &down);
    let what = format!("the meta leader reports group {} unconfirmed", blocked.0);
    eventually(&mut procs, &what, async || {
        let status = ursula_ctl::view::leader_status(&ctl_client(), &members)
            .await
            .map_err(|err| err.to_string())?
            .map_err(|refusal| refusal.to_string())?;
        let pending = status
            .leader_report
            .as_ref()
            .and_then(|report| report.removal_pending.get(&NodeId::new(5)));
        match pending {
            Some(pending) if pending.unconfirmed.contains(&blocked) && pending.process_answers => {
                Ok(())
            }
            other => Err(format!("{other:?}")),
        }
    })
    .await;
    procs.kill(5);
    std::fs::rename(&nodes[4].wal, &old_disk.wal).expect("move node 5's old disk aside");

    // 3. A new disk under id 5 waits for the removal.
    procs.start(binary, &nodes[4]);
    wait_placement(
        &client,
        &mut procs,
        &nodes[4],
        "waits for its id's removal",
        |status| status.registration == RegistrationStatus::RemovalPending,
    )
    .await;
    wait_readiness_reason(
        &client,
        &mut procs,
        &nodes[4],
        Some(ServingReadinessReason::RemovalPending),
    )
    .await;
    let held = try_node_metrics(&client, &nodes[4].url())
        .await
        .expect("the new disk reports its replicas");
    assert_data_held(&held, 5);
    let what = format!("verify-joinable names group {} as unconfirmed", blocked.0);
    eventually(&mut procs, &what, async || {
        let mut out = Vec::new();
        let refusal = ursula_ctl::placement::lifecycle::verify_removal(
            &ctl_client(),
            &members,
            &BTreeSet::from([NodeId::new(5)]),
            Removal::Joining,
            Duration::from_secs(5),
            Duration::from_millis(500),
            &mut out,
        )
        .await
        .err()
        .ok_or_else(|| "verify-joinable passed while a group has no leader".to_owned())?;
        match refusal.downcast_ref::<RemovalRefusal>() {
            Some(RemovalRefusal::RemovalPending {
                node,
                pending: Some(pending),
            }) if *node == NodeId::new(5)
                && pending.root == old
                && pending.unconfirmed.contains(&blocked)
                && !pending.process_answers =>
            {
                Ok(())
            }
            _ => Err(format!("{refusal:?}")),
        }
    })
    .await;

    // 4. The removal completes once every group has a live leader, and the
    // new disk, still running, binds a new root.
    for id in down {
        procs.start(binary, node_of(&nodes, id));
    }
    let record = eventually_for(
        &mut procs,
        "the running new disk binds a new root",
        LIFECYCLE_WAIT,
        async || {
            let report = ursula_ctl::view::leader_status(&ctl_client(), &ctl_manifest(&nodes, &[]))
                .await
                .ok()
                .and_then(Result::ok)
                .and_then(|status| status.leader_report);
            if let Some(pending) = report
                .as_ref()
                .and_then(|report| report.removal_pending.get(&NodeId::new(5)))
            {
                assert!(
                    !pending.process_answers,
                    "the meta leader counts the new disk a process of the old root: {pending:?}"
                );
            }
            match placement_status(&client, &nodes[4]).await?.registration {
                RegistrationStatus::Answered(Registered::Unreferenced(record)) => Ok(record),
                RegistrationStatus::Answered(other) => {
                    panic!("the new life holds no role yet: {other:?}")
                }
                other => Err(format!("{other:?}, removal pending {report:?}")),
            }
        },
    )
    .await;
    let new = match record.root {
        RootSlot::Bound(new) if new != old => new,
        other => panic!("the new disk binds a new root: {other:?}"),
    };
    assert_eq!(record.lifecycle, NodeLifecycle::Active, "{record:?}");
    let mut out = Vec::new();
    ursula_ctl::placement::lifecycle::verify_removal(
        &ctl_client(),
        &ctl_manifest(&nodes, &[5]),
        &BTreeSet::from([NodeId::new(5)]),
        Removal::Joining,
        LIFECYCLE_WAIT,
        Duration::from_millis(500),
        &mut out,
    )
    .await
    .unwrap_or_else(|err| {
        panic!(
            "verify-joinable passes once node 5's removal completed: {err:?}\n{}",
            String::from_utf8_lossy(&out)
        )
    });
    eventually(
        &mut procs,
        "the meta leader reports no pending removal",
        async || {
            let status = ursula_ctl::view::leader_status(&ctl_client(), &ctl_manifest(&nodes, &[]))
                .await
                .map_err(|err| err.to_string())?
                .map_err(|refusal| refusal.to_string())?;
            match status.leader_report {
                Some(report) if report.removal_pending.is_empty() => Ok(()),
                other => Err(format!("{other:?}")),
            }
        },
    )
    .await;

    // 5. The new life turns ready and takes groups.
    wait_readiness_reason(&client, &mut procs, &nodes[4], None).await;
    wait_eligible(&mut procs, &nodes, &[], &[5]).await;
    let cluster = wait_confirmed_cluster(&client, &mut procs, &nodes, &[]).await;
    let mut moved = Vec::new();
    for (group, voters) in current_voters(&cluster).into_iter().take(2) {
        let out_voter = voters.iter().copied().max().expect("a voter");
        let mut to = voters.clone();
        to.remove(&out_voter);
        to.insert(5);
        move_converges(&nodes, &[], group, replace(out_voter, 5), &to).await;
        moved.push((group, to));
    }
    let (led, led_voters) = moved.first().cloned().expect("a moved group");
    let what = format!("node 5 leads group {}", led.0);
    eventually(&mut procs, &what, async || {
        let replica = replica_of(&client, &nodes[4], led).await?;
        if replica.current_leader == Some(5) && replica.leads_live {
            return Ok(());
        }
        let leader = leading_replica(&client, &nodes, &led_voters, led).await?;
        let from = node_of(&nodes, leader.node_id);
        let transfer = format!("{}/__ursula/raft/{}/leader/transfer/5", from.admin(), led.0);
        let response = try_admin_test_post(&client, transfer.clone())
            .await?
            .send()
            .await
            .map_err(|err| format!("POST {transfer}: {err}"))?;
        Err(format!(
            "asked node {} to hand over: {}",
            leader.node_id,
            response.status()
        ))
    })
    .await;
    for group in 0..groups {
        append_until_done(&client, &mut procs, &url(&nodes[0], group), "after-reuse;").await;
    }
    for (group, _) in &moved {
        wait_read_holds(
            &client,
            &mut procs,
            &read(&nodes[4], group.0),
            "after-reuse;",
        )
        .await;
    }

    // 6. The old disk is refused for the new root, and the new disk rejoins.
    procs.kill(5);
    procs.start(binary, &old_disk);
    wait_superseded_for(&client, &mut procs, &old_disk, RootSlot::Bound(new)).await;
    for group in 0..groups {
        append_until_done(
            &client,
            &mut procs,
            &url(&nodes[0], group),
            "old-disk-again;",
        )
        .await;
    }
    procs.kill(5);
    procs.start(binary, &nodes[4]);
    let cluster = wait_confirmed_cluster(&client, &mut procs, &nodes, &[]).await;
    wait_serves_placed(&client, &mut procs, &nodes[4], &cluster).await;
    wait_readiness_reason(&client, &mut procs, &nodes[4], None).await;
    for (group, _) in &moved {
        wait_read_holds(
            &client,
            &mut procs,
            &read(&nodes[4], group.0),
            "before;after-reuse;old-disk-again;",
        )
        .await;
    }
}

/// Waits until node `node`'s process is superseded for `slot`: its
/// registration refused for another root and readiness `superseded`, read
/// on its admin listener, since the client listener answers a fenced node
/// `503`.
async fn wait_superseded_for(
    client: &reqwest::Client,
    procs: &mut Processes,
    node: &MetaNode,
    slot: ursula_proto::placement::RootSlot,
) {
    let what = format!("node {} is superseded for {slot}", node.id);
    eventually(procs, &what, async || {
        let status = admin_get_json::<PlacementStatus>(client, node, "/__ursula/placement").await?;
        let refused = RegistrationStatus::Refused(ursula_control::ControlError::OtherRoot {
            node: NodeId::new(node.id),
            slot,
        });
        let ready = admin_get_json::<ursula_proto::admin::ServingReadiness>(
            client,
            node,
            "/__ursula/ready",
        )
        .await?;
        if status.registration == refused
            && ready.reason == Some(ursula_proto::admin::ServingReadinessReason::Superseded)
        {
            Ok(())
        } else {
            Err(format!("{:?} {ready:?}", status.registration))
        }
    })
    .await;
}

/// `path` of node `node`'s admin listener as JSON, whatever its status.
async fn admin_get_json<T: serde::de::DeserializeOwned>(
    client: &reqwest::Client,
    node: &MetaNode,
    path: &str,
) -> Result<T, String> {
    let url = format!("{}{path}", node.admin());
    client
        .get(&url)
        .send()
        .await
        .map_err(|err| format!("GET {url}: {err}"))?
        .json()
        .await
        .map_err(|err| format!("GET {url}: {err}"))
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

/// An empty disk under the id of the running meta leader (#768): a second
/// process of that id starts on an empty WAL at an address no peer dials,
/// with the cluster's `raft.peers`, while the id's process leads meta. No
/// meta leader ever sends to it, and every peer it asks names its own id as
/// the leader, so it reaches the leader only at the address `raft.peers`
/// lists for its id. It is refused for the root the id is bound to, its
/// data replicas take part in nothing, and the healthy nodes stay
/// registered under the same meta leader. A registration that took a
/// redirect to its own id for its own replica never reaches the leader and
/// stays pending, which fails it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cli_an_empty_disk_under_the_meta_leaders_id_is_refused() {
    use ursula_proto::placement::RootSlot;

    let _guard = static_cluster_cli_test_guard().await;
    let Some(binary) = option_env!("CARGO_BIN_EXE_ursula") else {
        tracing::warn!("CARGO_BIN_EXE_ursula is not set; skipping the meta leader's new disk test");
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
    let leader = known_meta_leader(&client, &mut procs, &nodes, &nodes[0]).await;
    let bound = bound_root(&cluster, leader);

    // 1. A second process of the leader's id, on an empty WAL.
    let stranger = MetaNode {
        id: leader,
        port: free_port(),
        admin_port: free_port(),
        config: root.path().join(format!("node-{leader}-stranger.toml")),
        wal: root.path().join(format!("node-{leader}-stranger-log")),
        cold_root: None,
    };
    stranger.write(&peers, meta);
    let mut strangers = Processes::default();
    strangers.start(binary, &stranger);
    wait_placement(
        &client,
        &mut strangers,
        &stranger,
        "is refused for the root its id is bound to",
        |status| {
            status.registration
                == RegistrationStatus::RootRefused {
                    slot: RootSlot::Bound(bound),
                }
        },
    )
    .await;
    let held = try_node_metrics(&client, &stranger.url())
        .await
        .expect("the second process reports its replicas");
    assert_data_held(&held, leader);

    // 2. The healthy nodes stay registered, and the id's process still leads.
    for node in &nodes {
        let status = placement_status(&client, node)
            .await
            .expect("a healthy node's placement status");
        assert!(
            matches!(status.registration, RegistrationStatus::Answered(_)),
            "node {} was refused beside the second process: {status:?}",
            node.id
        );
    }
    assert_eq!(
        known_meta_leader(&client, &mut procs, &nodes, &nodes[0]).await,
        leader
    );
    strangers.kill(leader);
}

/// Integration 12 (#768, design 10): a wiped voter is not ready before its
/// groups. Node 3's Raft traffic goes through a proxy, and every group's
/// leaders purge their logs through a snapshot, so a wiped replica catches
/// up only by an install. Node 3 loses its disk while it is down,
/// `ursulactl node replace-disk` releases the lost root, and node 3 starts
/// on an empty WAL while the proxy refuses every Raft request to it. It
/// binds its new root through its own registration, but no meta entry
/// reaches it: its report names `placement_unknown` and places it in no
/// group. The proxy then passes everything but the installs. From the
/// first report that places a group on node 3 until the installs pass,
/// every such group is listed with `missing_group` or `recovery_barrier`,
/// never `missing_group` after `recovery_barrier`, the report is not
/// serving-ready and the node answers `503`, and every group reaches
/// `recovery_barrier` while installs are held. Once the installs pass,
/// node 3 becomes ready with each group a voter that counts and no group
/// issue, and reads every write. A report keyed by the replicas a node
/// runs, under which a wiped voter is a node with no group, fails it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cli_a_wiped_voter_is_not_ready_before_its_groups() {
    use ursula_proto::admin::RaftMaintenanceIssue as Issue;
    use ursula_proto::placement::ReplicaRole;

    let _guard = static_cluster_cli_test_guard().await;
    let Some(binary) = option_env!("CARGO_BIN_EXE_ursula") else {
        tracing::warn!("CARGO_BIN_EXE_ursula is not set; skipping the wiped-voter test");
        return;
    };
    let root = tempfile::tempdir().expect("temp root");
    let nodes = MetaNode::cluster(root.path(), 3);
    let proxy = RaftProxy::start(&nodes[2].url()).await;
    let peers = nodes
        .iter()
        .map(|node| match node.id {
            3 => (3, proxy.url.clone()),
            _ => node.peer(),
        })
        .collect::<Vec<_>>();
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
    let formed = wait_settled(&client, &mut procs, &nodes).await;
    let cluster = settled(&formed[0]).expect("settled").clone();
    let placed: BTreeSet<u32> = placed_on(&cluster, 3)
        .into_iter()
        .map(|group| u32::try_from(group).unwrap())
        .collect();
    assert_eq!(placed.len(), META_TEST_GROUPS, "{cluster:?}");
    let lost = bound_root(&cluster, 3);
    let streams = stream_per_group("wiped");
    let url = |node: &MetaNode, stream: &str| format!("{}/wiped/{stream}", node.url());
    for stream in &streams {
        put_until_created(&client, &url(&nodes[0], stream)).await;
        post_until_no_content(&client, &url(&nodes[0], stream), "before-the-loss;").await;
    }
    for node in &nodes[..2] {
        for group in &placed {
            let raft = format!("{}/__ursula/raft/{group}", node.admin());
            let response = admin_test_post(&client, format!("{raft}/snapshot"))
                .await
                .send()
                .await
                .expect("snapshot the group");
            assert_eq!(response.status(), reqwest::StatusCode::OK);
            let snapshot: ursula_proto::admin::SnapshotResponse =
                response.json().await.expect("snapshot JSON");
            let index = snapshot.snapshot_index.expect("a snapshot index");
            let response = admin_purge(&client, format!("{raft}/purge?upto={index}")).await;
            assert_eq!(response.status(), reqwest::StatusCode::OK);
        }
    }
    for stream in &streams {
        post_until_no_content(&client, &url(&nodes[0], stream), "after-the-purge;").await;
    }

    // 1. Node 3 loses its disk while it is down, and the lost root is
    // released.
    procs.kill(3);
    remove_test_path(&nodes[2].wal);
    let ctl_nodes = nodes.iter().map(MetaNode::ctl).collect::<Vec<_>>();
    replace_lost_disk(&ctl_nodes, 3, lost).await;

    // 2. No Raft request reaches node 3 on its empty WAL. It binds its new
    // root, and knows no placement.
    proxy.release.send_replace(false);
    proxy.refuse.send_replace(true);
    procs.start(binary, &nodes[2]);
    wait_placement(
        &client,
        &mut procs,
        &nodes[2],
        "binds its new root",
        |status| {
            matches!(
                status.registration,
                RegistrationStatus::Answered(Registered::Referenced(_))
            )
        },
    )
    .await;
    let (status, ready) = readiness(&client, &nodes[2])
        .await
        .expect("node 3's readiness");
    assert_eq!(
        status,
        reqwest::StatusCode::SERVICE_UNAVAILABLE,
        "{ready:?}"
    );
    let report = ready.raft_maintenance.expect("a maintenance report");
    assert!(
        report.node_issues.contains(&Issue::PlacementUnknown),
        "{report:?}"
    );
    assert!(report.expected_groups.is_empty(), "{report:?}");
    assert!(!report.serving_ready(), "{report:?}");

    // 3. Raft requests pass again, but installs wait. Each group placed on
    // node 3 blocks until its install passes.
    proxy.refuse.send_replace(false);
    let mut barriers = BTreeSet::new();
    eventually(
        &mut procs,
        "every group of node 3 reports recovery_barrier",
        async || {
            let (status, ready) = readiness(&client, &nodes[2]).await?;
            let report = ready
                .raft_maintenance
                .ok_or_else(|| "no maintenance report".to_owned())?;
            assert_eq!(
                status,
                reqwest::StatusCode::SERVICE_UNAVAILABLE,
                "node 3 is ready before its groups: {report:?}"
            );
            assert!(!report.serving_ready(), "{report:?}");
            for group in report.expected_groups.keys() {
                let issues = report
                    .group_issues
                    .get(group)
                    .map_or(&[][..], Vec::as_slice);
                let missing = issues.contains(&Issue::MissingGroup);
                let barrier = issues.contains(&Issue::RecoveryBarrier);
                assert!(
                    missing || barrier,
                    "group {group} blocks nothing on node 3: {report:?}"
                );
                assert!(
                    !(missing && barriers.contains(group)),
                    "group {group} is missing after its replica ran: {report:?}"
                );
                if barrier {
                    barriers.insert(*group);
                }
            }
            let waiting = placed
                .iter()
                .filter(|group| {
                    !report
                        .group_issues
                        .get(*group)
                        .is_some_and(|issues| issues.contains(&Issue::RecoveryBarrier))
                })
                .collect::<Vec<_>>();
            if waiting.is_empty() {
                Ok(())
            } else {
                Err(format!(
                    "groups without recovery_barrier {waiting:?}: {report:?}"
                ))
            }
        },
    )
    .await;
    // A group reports `recovery_barrier` as soon as its replica opens with
    // the gate closed, which can be before any leader has sent the held
    // install, so wait for one to arrive.
    tokio::time::timeout(
        META_WAIT,
        proxy.blocked.clone().wait_for(|blocked| *blocked),
    )
    .await
    .expect("an install is held")
    .unwrap();

    // 4. The installs pass: node 3 catches up and becomes ready.
    proxy.release.send_replace(true);
    let ready = wait_readiness_reason(&client, &mut procs, &nodes[2], None).await;
    let report = ready.raft_maintenance.expect("a maintenance report");
    assert!(report.ready(), "{report:?}");
    assert_eq!(
        report
            .expected_groups
            .keys()
            .copied()
            .collect::<BTreeSet<_>>(),
        placed,
        "{report:?}"
    );
    assert!(
        report
            .expected_groups
            .values()
            .all(|expected| expected.role == ReplicaRole::Voter && expected.counts_as_voter),
        "{report:?}"
    );
    assert!(report.group_issues.is_empty(), "{report:?}");
    for stream in &streams {
        read_until_matches(
            &client,
            &format!("{}?offset=0&max_bytes=64", url(&nodes[2], stream)),
            b"before-the-loss;after-the-purge;",
        )
        .await;
    }
}

/// One-way delay of every link in the first phase of
/// [`cli_slow_links_slow_writes_instead_of_stopping_them`]: a round trip of
/// 400 ms, longer than 0.9's 250 ms heartbeat interval and shorter than the
/// 600 ms one.
const SLOW_LINK_IDLE_ONE_WAY: Duration = Duration::from_millis(200);
/// One-way delay of every link in the second phase: a round trip of 800 ms,
/// longer than the 600 ms heartbeat interval, which OpenRaft passes each
/// Append call as its option, and shorter than the 1.5 s minimum election
/// timeout.
const SLOW_LINK_BUSY_ONE_WAY: Duration = Duration::from_millis(400);
/// A cluster group's leader lease, its `election_timeout_max`.
const SLOW_LINK_LEADER_LEASE: Duration = Duration::from_secs(3);

/// A TCP link in front of a node's listener that holds every byte it
/// carries for the same one-way delay in each direction, as a `netem delay`
/// on a host's interface does. The delay starts at zero and applies to the
/// bytes read after it changes. Bytes keep their order.
struct DelayedLink {
    url: String,
    delay: tokio::sync::watch::Sender<Duration>,
    task: tokio::task::JoinHandle<()>,
}

impl DelayedLink {
    async fn start(target: &str) -> Self {
        let target = target.trim_start_matches("http://").to_owned();
        // A reserved port, as for `RaftProxy`.
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", free_port()))
            .await
            .unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let (delay, delays) = tokio::sync::watch::channel(Duration::ZERO);
        let task = tokio::spawn(async move {
            // Dropped with the task, which aborts every connection.
            let mut connections = tokio::task::JoinSet::new();
            loop {
                // A listener that fails to accept ends the link.
                let Ok((inbound, _peer)) = listener.accept().await else {
                    return;
                };
                while connections.try_join_next().is_some() {}
                let target = target.clone();
                let delays = delays.clone();
                connections.spawn(async move {
                    let Ok(outbound) = tokio::net::TcpStream::connect(&target).await else {
                        return;
                    };
                    inbound.set_nodelay(true).unwrap();
                    outbound.set_nodelay(true).unwrap();
                    let (inbound_read, inbound_write) = inbound.into_split();
                    let (outbound_read, outbound_write) = outbound.into_split();
                    tokio::join!(
                        delayed_copy(inbound_read, outbound_write, delays.clone()),
                        delayed_copy(outbound_read, inbound_write, delays),
                    );
                });
            }
        });
        Self { url, delay, task }
    }

    fn set_delay(&self, delay: Duration) {
        self.delay.send_replace(delay);
    }
}

impl Drop for DelayedLink {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// Copies `from` to `to`, writing each chunk once the delay `delays` held
/// when it was read has passed.
async fn delayed_copy(
    mut from: tokio::net::tcp::OwnedReadHalf,
    mut to: tokio::net::tcp::OwnedWriteHalf,
    delays: tokio::sync::watch::Receiver<Duration>,
) {
    use tokio::io::AsyncReadExt;
    use tokio::io::AsyncWriteExt;

    let (chunks, mut due) =
        tokio::sync::mpsc::unbounded_channel::<(tokio::time::Instant, Vec<u8>)>();
    let read = async move {
        let mut buffer = vec![0_u8; 64 * 1024];
        loop {
            let read = match from.read(&mut buffer).await {
                Ok(0) | Err(_) => break,
                Ok(read) => read,
            };
            let at = tokio::time::Instant::now()
                .checked_add(*delays.borrow())
                .unwrap();
            if chunks.send((at, buffer[..read].to_vec())).is_err() {
                break;
            }
        }
    };
    let write = async move {
        while let Some((at, chunk)) = due.recv().await {
            tokio::time::sleep_until(at).await;
            if to.write_all(&chunk).await.is_err() {
                return;
            }
        }
        // The peer reads the end of the stream where this side's ended.
        to.shutdown().await.unwrap_or_default();
    };
    tokio::join!(read, write);
}

/// Each group's `(leader, term)` as every node of `nodes` reports it.
async fn group_leaders_and_terms(
    client: &reqwest::Client,
    procs: &mut Processes,
    nodes: &[MetaNode],
) -> BTreeMap<u64, BTreeSet<(Option<u64>, u64)>> {
    let mut views: BTreeMap<u64, BTreeSet<(Option<u64>, u64)>> = BTreeMap::new();
    for node in nodes {
        let what = format!("node {} answers its metrics", node.id);
        let metrics = eventually(procs, &what, async || {
            try_node_metrics(client, &node.url()).await
        })
        .await;
        for group in metrics.groups {
            views
                .entry(group.raft_group_id)
                .or_default()
                .insert((group.current_leader, group.current_term));
        }
    }
    views
}

/// Appends `payload` to `url` once: how long its `204` took, or why the
/// append was not acknowledged.
async fn append_once(
    client: &reqwest::Client,
    url: &str,
    payload: &'static str,
) -> Result<Duration, String> {
    let started = std::time::Instant::now();
    let response = client
        .post(url)
        .header("content-type", "text/plain")
        .body(payload)
        .send()
        .await
        .map_err(|err| format!("append {payload} to {url} failed: {err}"))?;
    let status = response.status();
    if status != reqwest::StatusCode::NO_CONTENT {
        return Err(format!(
            "append {payload} to {url}: {status} {:?}",
            response.text().await
        ));
    }
    Ok(started.elapsed())
}

/// Checks `appended`, the outcome of one append of `payload` to `url`: it
/// was acknowledged, and after at least `round_trip`.
fn check_slow_append(
    url: &str,
    payload: &str,
    round_trip: Duration,
    appended: Result<Duration, String>,
) -> Result<(), String> {
    let took = appended?;
    if took < round_trip {
        return Err(format!(
            "append {payload} to {url} took {took:?}, less than a round trip of {round_trip:?}"
        ));
    }
    Ok(())
}

/// A slow link slows replication instead of stopping it. Once every group's
/// leadership settled, every node's Raft traffic passes a link that holds
/// each byte for a one-way delay, in two phases, and no group changes its
/// leader or term in either.
///
/// - At [`SLOW_LINK_IDLE_ONE_WAY`] every group stays idle for longer than
///   [`SLOW_LINK_LEADER_LEASE`], so only heartbeat answers keep a leader's
///   lease. The first write to each group is then acknowledged. With a
///   heartbeat interval shorter than the round trip, every heartbeat goes
///   unanswered in time, the lease lapses and the write is refused.
/// - At [`SLOW_LINK_BUSY_ONE_WAY`] one writer per group appends back to back,
///   and every append is acknowledged at its first attempt. A transport that
///   waits for an Append's answer only the heartbeat interval OpenRaft gives
///   the call as its option fails every append over such links. This also
///   pins that OpenRaft puts no timeout of its own on replication calls.
///
/// Linearizable reads, back at the first delay, return every write.
///
/// The nodes' cluster-egress gates run at their defaults, so every node
/// reports a failed probe in both phases. No node sheds at any sample, since
/// no peer does better and every peer still answers: a gate that shed on
/// its own probe alone would move every leadership off every node, and each
/// would refuse the others' handoffs.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cli_slow_links_slow_writes_instead_of_stopping_them() {
    let _guard = static_cluster_cli_test_guard().await;
    let Some(binary) = option_env!("CARGO_BIN_EXE_ursula") else {
        tracing::warn!("CARGO_BIN_EXE_ursula is not set; skipping the slow-link test");
        return;
    };
    let root = tempfile::tempdir().expect("temp root");
    let nodes = MetaNode::cluster(root.path(), 3);
    let mut links = Vec::new();
    for node in &nodes {
        links.push(DelayedLink::start(&node.url()).await);
    }
    let peers = nodes
        .iter()
        .zip(&links)
        .map(|(node, link)| (node.id, link.url.clone()))
        .collect::<Vec<_>>();
    let meta = MetaToml {
        meta_voters: Some(META_VOTERS),
        ..MetaToml::default()
    };
    let client = bounded_client();
    let mut procs = Processes::default();
    for node in &nodes {
        node.write(&peers, meta);
        procs.start(binary, node);
    }
    wait_settled(&client, &mut procs, &nodes).await;
    let streams = stream_per_group("slowlink");
    let url = |stream: &str| format!("{}/slowlink/{stream}", nodes[0].url());
    for stream in &streams {
        put_until_created(&client, &url(stream)).await;
        post_until_no_content(&client, &url(stream), "fast;").await;
    }
    wait_leaders_settle(&client, &mut procs, &nodes).await;
    let settled_leaders = group_leaders_and_terms(&client, &mut procs, &nodes).await;
    assert!(
        settled_leaders.values().all(|views| views.len() == 1),
        "{settled_leaders:?}"
    );

    // Idle groups: only heartbeats renew the leases.
    for link in &links {
        link.set_delay(SLOW_LINK_IDLE_ONE_WAY);
    }
    let sampler = ParticipationSampler::start(&client, &nodes);
    tokio::time::sleep(
        SLOW_LINK_LEADER_LEASE
            .checked_add(Duration::from_secs(1))
            .unwrap(),
    )
    .await;
    let idle_round_trip = SLOW_LINK_IDLE_ONE_WAY.checked_mul(2).unwrap();
    for stream in &streams {
        let url = url(stream);
        let appended = append_once(&client, &url, "idle;").await;
        if let Err(err) = check_slow_append(&url, "idle;", idle_round_trip, appended) {
            panic!("{err}\n{}", procs.reports());
        }
    }

    // Busy groups: append answers renew the leases, and heartbeats go
    // unanswered in time.
    for link in &links {
        link.set_delay(SLOW_LINK_BUSY_ONE_WAY);
    }
    let busy_round_trip = SLOW_LINK_BUSY_ONE_WAY.checked_mul(2).unwrap();
    let mut writers = tokio::task::JoinSet::new();
    for stream in &streams {
        let client = client.clone();
        let url = url(stream);
        writers.spawn(async move {
            for payload in ["busy-1;", "busy-2;", "busy-3;"] {
                let appended = append_once(&client, &url, payload).await;
                check_slow_append(&url, payload, busy_round_trip, appended)?;
            }
            Ok::<(), String>(())
        });
    }
    while let Some(writer) = writers.join_next().await {
        if let Err(err) = writer.expect("a writer task") {
            panic!("{err}\n{}", procs.reports());
        }
    }
    assert_degraded_without_egress_shed(&nodes, &sampler.stop().await);
    assert_eq!(
        group_leaders_and_terms(&client, &mut procs, &nodes).await,
        settled_leaders,
        "a group changed its leader or term over the slow links"
    );

    // A ReadIndex read needs heartbeat answers within the heartbeat interval.
    for link in &links {
        link.set_delay(SLOW_LINK_IDLE_ONE_WAY);
    }
    for stream in &streams {
        read_until_matches(
            &client,
            &format!("{}?offset=0&max_bytes=64", url(stream)),
            b"fast;idle;busy-1;busy-2;busy-3;",
        )
        .await;
    }
}

/// One-way delay of every link but the incoming learner's in
/// [`cli_a_move_over_slow_links_converges_under_steady_writes`]: a round
/// trip of 500 ms, under the 600 ms heartbeat interval.
const SLOW_MOVE_ONE_WAY: Duration = Duration::from_millis(250);
/// One-way delay of the incoming learner's own link: it acknowledges each
/// entry 300 ms after the voters do, so under steady writes the commit index
/// is ahead of its matched index at nearly every sample.
const SLOW_MOVE_LEARNER_ONE_WAY: Duration = Duration::from_millis(400);
/// Writers that append back to back to the moving group, so its commit
/// index moves every few tens of milliseconds.
const SLOW_MOVE_WRITERS: usize = 16;

/// A move finishes over a slow cluster plane under steady writes. Four
/// nodes at replication factor 3 settle, every link then holds each byte
/// for [`SLOW_MOVE_ONE_WAY`] and the link of the one node outside a group
/// for [`SLOW_MOVE_LEARNER_ONE_WAY`], and that node replaces a follower of
/// the group while [`SLOW_MOVE_WRITERS`] writers append to it. The move
/// converges and the node serves the group as a voter.
///
/// - The learner's matched index trails the commit index at nearly every
///   sample, so a leader that promotes only a learner that matched the
///   commit index of the moment never promotes it. Each round of the
///   catch-up rule ends one learner round trip after it starts.
/// - Each participation answer, the learner's recovery gate among them,
///   takes a round trip of 500 ms or 800 ms, which a peer query that gave
///   up after 500 ms never gets.
/// - The cluster-egress gates run at their defaults, so every node reports
///   a failed probe. No node sheds for it at any sample through the move,
///   since no peer does better and every peer still answers.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cli_a_move_over_slow_links_converges_under_steady_writes() {
    let _guard = static_cluster_cli_test_guard().await;
    let Some(binary) = option_env!("CARGO_BIN_EXE_ursula") else {
        tracing::warn!("CARGO_BIN_EXE_ursula is not set; skipping the slow move test");
        return;
    };
    let root = tempfile::tempdir().expect("temp root");
    let nodes = MetaNode::cluster(root.path(), 4);
    let mut links = Vec::new();
    for node in &nodes {
        links.push(DelayedLink::start(&node.url()).await);
    }
    let peers = nodes
        .iter()
        .zip(&links)
        .map(|(node, link)| (node.id, link.url.clone()))
        .collect::<Vec<_>>();
    let meta = MetaToml {
        meta_voters: Some(META_VOTERS),
        replication_factor: Some(3),
        ..MetaToml::default()
    };
    let client = bounded_client();
    let mut procs = Processes::default();
    for node in &nodes {
        node.write(&peers, meta);
        procs.start(binary, node);
    }
    wait_settled(&client, &mut procs, &nodes).await;
    let cluster = confirmed_cluster(&client, &mut procs, &nodes, &[]).await;
    let (group, from) = current_voters(&cluster)
        .into_iter()
        .next()
        .unwrap_or_else(|| panic!("no group: {cluster:?}"));
    let learner = nodes
        .iter()
        .map(|node| node.id)
        .find(|id| !from.contains(id))
        .unwrap_or_else(|| panic!("group {group:?} is placed on every node: {cluster:?}"));
    let stream = stream_per_group("slowmove").swap_remove(usize::try_from(group.0).unwrap());
    let url = |node: &MetaNode| format!("{}/slowmove/{stream}", node.url());
    put_until_created(&client, &url(&nodes[0])).await;
    post_until_no_content(&client, &url(&nodes[0]), "fast;").await;
    let leaders = wait_leaders_settle(&client, &mut procs, &nodes).await;
    let leader = leaders[&u64::from(group.0)];
    let source = *from
        .iter()
        .find(|id| **id != leader)
        .expect("a follower to replace");
    let to: BTreeSet<u64> = from
        .iter()
        .copied()
        .filter(|id| *id != source)
        .chain([learner])
        .collect();

    for (node, link) in nodes.iter().zip(&links) {
        link.set_delay(if node.id == learner {
            SLOW_MOVE_LEARNER_ONE_WAY
        } else {
            SLOW_MOVE_ONE_WAY
        });
    }
    let sampler = ParticipationSampler::start(&client, &nodes);
    let writing = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true));
    let mut writers = tokio::task::JoinSet::new();
    for _ in 0..SLOW_MOVE_WRITERS {
        let client = client.clone();
        let url = url(node_of(&nodes, leader));
        let writing = writing.clone();
        writers.spawn(async move {
            let mut acknowledged = 0_u64;
            while writing.load(std::sync::atomic::Ordering::SeqCst) {
                if append_once(&client, &url, "steady;").await.is_ok() {
                    acknowledged = acknowledged.checked_add(1).unwrap();
                } else {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            }
            acknowledged
        });
    }
    // The learner may not be listed live yet when the move is asked: the
    // meta leader's report trails the delay, and the move's own liveness
    // check is not what this test pins.
    let (end, out) = placement_move(
        &nodes,
        &[],
        group,
        replace(source, learner),
        NewVoters::MayBeDown,
        true,
    )
    .await;
    writing.store(false, std::sync::atomic::Ordering::SeqCst);
    let samples = sampler.stop().await;
    let mut acknowledged = 0_u64;
    while let Some(writer) = writers.join_next().await {
        let writer = writer.expect("a writer task");
        assert!(writer > 0, "a writer had no append acknowledged\n{out}");
        acknowledged = acknowledged.checked_add(writer).unwrap();
    }
    assert_eq!(
        end,
        GroupEnd::Converged { to: voter_set(&to) },
        "{out}\n{}",
        procs.reports()
    );
    let target = node_of(&nodes, learner);
    let what = format!("node {learner} is a voter of group {}", group.0);
    eventually(&mut procs, &what, async || {
        let replica = replica_of(&client, target, group).await?;
        if replica.voter_configs == vec![to.clone()] {
            Ok(())
        } else {
            Err(format!("{replica:?}"))
        }
    })
    .await;
    assert_degraded_without_egress_shed(&nodes, &samples);
    tracing::info!(
        acknowledged,
        "slow move: appends acknowledged during the move"
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
    // whose metrics fail reports nothing, so the check waits until a leader
    // reported every group, rather than pass with a group left out.
    let committed = eventually(
        &mut procs,
        "every group's leader reports its committed index",
        async || {
            let committed = leaders_committed(&client, &nodes).await;
            if committed.len() == META_TEST_GROUPS {
                Ok(committed)
            } else {
                Err(format!(
                    "leaders reported the committed index of groups {:?}, of {META_TEST_GROUPS}",
                    committed.keys().collect::<Vec<_>>()
                ))
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

/// How long a group may go without a leader while one of its voters
/// restarts cleanly: one election timeout, the maximum (3 s), as OpenRaft
/// draws each timeout between its minimum and maximum.
const CLEAN_RESTART_LEADERLESS_WITHIN: Duration = Duration::from_secs(3);

/// A clean restart of a voter costs no election beyond its handoff. The
/// leadership balancer is off, so nothing else moves a leadership. A node
/// that leads some groups and follows others stops by SIGTERM, hands its
/// leaderships off, the meta group's included, and starts again. Then
/// every group it led is one term above its term before the stop, the
/// handoff's election, every other group keeps its term and its leader,
/// and no group, data or meta, goes without a leader for longer than
/// [`CLEAN_RESTART_LEADERLESS_WITHIN`].
///
/// The restarted replicas' election timers may fire before their leaders'
/// first heartbeats reach them. The test makes sure they do: its peers
/// reach the restarted node through relays, and the node's relay holds
/// every connection until the node has sent pre-votes. Each restarted
/// replica's pre-vote is refused by its peers, which still hold their
/// leader's lease, and leaves the term alone. Without pre-vote it raises
/// its term, its leader steps down at its next append to it, and the
/// group elects again: two terms more in each group the node follows.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cli_a_clean_restart_costs_no_election_beyond_its_handoff() {
    let _guard = static_cluster_cli_test_guard().await;
    let Some(binary) = option_env!("CARGO_BIN_EXE_ursula") else {
        tracing::warn!("CARGO_BIN_EXE_ursula is not set; skipping the clean restart test");
        return;
    };
    let root = tempfile::tempdir().expect("temp root");
    let nodes = MetaNode::cluster(root.path(), 3);
    // Each node reaches its peers through their relays, and itself
    // directly.
    let mut relays = BTreeMap::new();
    for node in &nodes {
        relays.insert(node.id, Relay::start(node.port).await);
    }
    let peers_for = |me: &MetaNode| -> Vec<(u64, String)> {
        nodes
            .iter()
            .map(|node| {
                if node.id == me.id {
                    node.peer()
                } else {
                    (node.id, relays[&node.id].url())
                }
            })
            .collect()
    };
    let meta = MetaToml {
        meta_voters: Some(META_VOTERS),
        leadership_balance_interval: Some("0s"),
        ..MetaToml::default()
    };
    let client = reqwest::Client::new();
    let mut procs = Processes::default();
    for node in &nodes {
        node.write(&peers_for(node), meta);
        procs.start(binary, node);
    }
    wait_bootstrapped(&client, &mut procs, &nodes).await;
    let before = settled_leaders_and_terms(&client, &mut procs, &nodes).await;
    let data_led = |id: u64| {
        before
            .iter()
            .filter(|(group, (leader, _))| {
                matches!(group, ReplicatedGroup::Data(_)) && *leader == id
            })
            .count()
    };
    // A node that both leads and follows data groups when there is one,
    // so the restart covers a handoff and the groups it follows.
    let restarting = nodes
        .iter()
        .map(|node| node.id)
        .max_by_key(|id| {
            let led = data_led(*id);
            (led > 0 && led < META_TEST_GROUPS, led)
        })
        .expect("a node of the cluster");
    assert!(
        data_led(restarting) > 0,
        "no node leads a data group: {before:?}"
    );
    let index = nodes
        .iter()
        .position(|node| node.id == restarting)
        .expect("the restarting node");

    let sampler = LeaderlessSampler::start(&nodes);
    tokio::time::sleep(Duration::from_secs(1)).await;
    let mut outgoing = procs.0.remove(&restarting).expect("the node runs");
    sigterm_and_wait_for_clean_exit(&mut outgoing).await;
    let log = std::fs::read_to_string(&outgoing.stderr_path).expect("shutdown log");
    assert!(
        log.contains("shutdown leadership handoff complete"),
        "node {restarting} did not hand off every leadership: {log}"
    );
    drop(outgoing);
    // No append of its leaders reaches the restarted node until it has
    // sent pre-votes, which go out through its peers' relays.
    let sink = Sink::start().await;
    relays[&restarting].point(sink.port, Open::Closed);
    tokio::time::sleep(Duration::from_millis(500)).await;
    procs.start(binary, &nodes[index]);
    eventually(
        &mut procs,
        "the restarted node sends pre-votes while no leader reaches it",
        async || {
            let sent = pre_votes_sent(&client, &nodes[index]).await?;
            if sent > 0 {
                Ok(())
            } else {
                Err("no pre-vote sent yet".to_owned())
            }
        },
    )
    .await;
    relays[&restarting].point(nodes[index].port, Open::Closed);
    wait_readiness_reason(&client, &mut procs, &nodes[index], None).await;
    // Two election timeouts more: a restarted replica that campaigns has
    // done so by then.
    tokio::time::sleep(CLEAN_RESTART_LEADERLESS_WITHIN.saturating_mul(2)).await;
    let leaderless = sampler.stop().await;
    let after = settled_leaders_and_terms(&client, &mut procs, &nodes).await;

    let expected: BTreeMap<ReplicatedGroup, u64> = before
        .iter()
        .map(|(group, (leader, term))| {
            let elections = u64::from(*leader == restarting);
            (*group, term.saturating_add(elections))
        })
        .collect();
    let terms: BTreeMap<ReplicatedGroup, u64> = after
        .iter()
        .map(|(group, (_, term))| (*group, *term))
        .collect();
    assert_eq!(
        terms, expected,
        "node {restarting}'s restart cost elections beyond its handoff: before {before:?}, \
         after {after:?}"
    );
    for (group, (leader, _)) in &before {
        if *leader != restarting {
            assert_eq!(
                after[group].0, *leader,
                "{group:?} changed its leader in node {restarting}'s restart: before {before:?}, \
                 after {after:?}"
            );
        }
    }
    assert_eq!(
        leaderless.keys().copied().collect::<BTreeSet<_>>(),
        before.keys().copied().collect::<BTreeSet<_>>(),
        "the sampler saw every group"
    );
    let long: Vec<(&ReplicatedGroup, &Duration)> = leaderless
        .iter()
        .filter(|(_, gap)| **gap >= CLEAN_RESTART_LEADERLESS_WITHIN)
        .collect();
    assert!(
        long.is_empty(),
        "groups without a leader for {CLEAN_RESTART_LEADERLESS_WITHIN:?} or longer in node \
         {restarting}'s restart: {long:?}"
    );
}

/// The data groups of [`cli_the_balancer_hands_a_restarted_node_leaderships_it_can_take`]:
/// two per node of three, so a node that restarts leaves both other nodes
/// above their fair share, and the balancer moves leaderships back to it.
const BALANCED_RESTART_GROUPS: usize = 6;

/// How long a write may take from its first attempt until it holds, and how
/// long a group may go without a leader, while a node restarts and the
/// leadership balancer moves leaderships back to it. A handoff the target
/// takes costs one election round. One the target refuses, or one that
/// does not complete, used to leave its group without a serving leader for
/// an election timeout and a pre-vote round (1.5 s or more).
const BALANCED_RESTART_WITHIN: Duration = Duration::from_secs(1);

/// A leadership balancer hands a restarted node only leaderships it can
/// take, and a group whose handoff does not complete keeps a leader. Three
/// nodes run [`BALANCED_RESTART_GROUPS`] groups with the balancer ticking
/// every second, two leaderships each once it settled. Under a producer
/// load to every group, sent to the two nodes that stay up, one node stops
/// by SIGTERM, hands its leaderships off, and starts again. The other two
/// then lead three groups each, above their share, and the balancer moves
/// two back to the restarted node, which it may do only once that node is
/// serving-ready and its replica of the group acknowledged its leader
/// again. Throughout, no group goes without a leader for
/// [`BALANCED_RESTART_WITHIN`], and every write holds within it of its
/// first attempt. A balancer that hands a leadership to the node before its
/// replica may take one leaves the group to its old leader, which waits
/// for the transfer, and fails it, as does a leader that does not lead
/// again at once when its target refuses.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cli_the_balancer_hands_a_restarted_node_leaderships_it_can_take() {
    let _guard = static_cluster_cli_test_guard().await;
    let Some(binary) = option_env!("CARGO_BIN_EXE_ursula") else {
        tracing::warn!("CARGO_BIN_EXE_ursula is not set; skipping the balanced restart test");
        return;
    };
    let root = tempfile::tempdir().expect("temp root");
    let nodes = MetaNode::cluster(root.path(), 3);
    let meta = MetaToml {
        meta_voters: Some(META_VOTERS),
        leadership_balance_interval: Some("1s"),
        ..MetaToml::default()
    };
    let client = reqwest::Client::new();
    let mut procs = Processes::default();
    for node in &nodes {
        node.write_with_groups(&peers_of(&nodes), meta, BALANCED_RESTART_GROUPS);
        procs.start(binary, node);
    }
    wait_bootstrapped(&client, &mut procs, &nodes).await;
    let ctl = ctl_client();
    let ctl_nodes: Vec<ursula_ctl::NodeInfo> = nodes.iter().map(MetaNode::ctl).collect();
    let groups = u64::try_from(BALANCED_RESTART_GROUPS).unwrap();
    wait_leadership_balanced(&ctl, &ctl_nodes, groups).await;

    let restarting = &nodes[2];
    let staying: Vec<&MetaNode> = nodes
        .iter()
        .filter(|node| node.id != restarting.id)
        .collect();
    let mut loads = Vec::new();
    for (group, stream) in streams_by_group_of("balanced", BALANCED_RESTART_GROUPS) {
        let urls: Vec<String> = staying
            .iter()
            .map(|node| format!("{}/balanced/{stream}", node.url()))
            .collect();
        create_stream(&client, &mut procs, &urls[0]).await;
        loads.push((group, ProducerLoad::start(urls, "balanced")));
    }
    let sampler = LeaderlessSampler::start(&nodes);
    tokio::time::sleep(Duration::from_secs(1)).await;

    procs.stop(restarting.id).await;
    procs.start(binary, restarting);
    let fair = BALANCED_RESTART_GROUPS.div_ceil(nodes.len());
    let what = format!("node {} leads its {fair} groups again", restarting.id);
    eventually(&mut procs, &what, async || {
        let metrics = try_node_metrics(&client, &restarting.url()).await?;
        let led = leads(&metrics, restarting.id);
        if led >= fair {
            Ok(())
        } else {
            Err(format!("node {} leads {led} groups", restarting.id))
        }
    })
    .await;
    // The balancer settles, and any election it set off ends. A client
    // pins each node's process incarnation, so the restarted node needs a
    // new one.
    let ctl = ctl_client();
    wait_leadership_balanced(&ctl, &ctl_nodes, groups).await;
    tokio::time::sleep(Duration::from_secs(2)).await;
    let leaderless = sampler.stop().await;
    let mut slow = Vec::new();
    for (group, load) in loads {
        let attempts = load.stop().await;
        let mut first_sent = BTreeMap::<u64, tokio::time::Instant>::new();
        let mut held = 0_usize;
        for attempt in &attempts {
            let first = *first_sent.entry(attempt.seq).or_insert(attempt.sent);
            if attempt.outcome.holds() {
                held = held.saturating_add(1);
                let took = attempt
                    .sent
                    .saturating_duration_since(first)
                    .saturating_add(attempt.took);
                if took >= BALANCED_RESTART_WITHIN {
                    slow.push((group, attempt.seq, took));
                }
            }
        }
        assert!(held > 0, "no write of group {group} held: {attempts:?}");
    }
    assert!(
        slow.is_empty(),
        "writes held {BALANCED_RESTART_WITHIN:?} or longer after their first attempt (group, \
         seq, took): {slow:?}\n{}",
        procs.reports()
    );
    let long: Vec<(&ReplicatedGroup, &Duration)> = leaderless
        .iter()
        .filter(|(_, gap)| **gap >= BALANCED_RESTART_WITHIN)
        .collect();
    assert!(
        long.is_empty(),
        "groups without a leader for {BALANCED_RESTART_WITHIN:?} or longer while node {} \
         restarted: {long:?}\n{}",
        restarting.id,
        procs.reports()
    );
}

/// How long a follower may keep naming a leader it no longer hears: the
/// 3 s a leader's last append counts as live, with a margin for a loaded
/// runner.
const SILENT_LEADER_FORGOTTEN_WITHIN: Duration = Duration::from_secs(10);

/// A follower names only a leader it hears. A node of a 3-node cluster
/// that follows some data group outlives the group's leader and the third
/// node, both killed. Its pre-votes reach no peer, so it never raises a
/// term, and OpenRaft keeps naming the killed leader for as long as the
/// node hears no other. Within [`SILENT_LEADER_FORGOTTEN_WITHIN`] the node
/// answers a write of every group `503 NO_LEADER` with `Retry-After`, never
/// a redirect to a killed node: a group a killed node led knows no leader,
/// and one it led itself has lapsed. Its metrics name no leader of a group
/// a killed node led, and its maintenance report lists `LeaderUnknown` for
/// each. It stays so while the quorum is gone.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cli_a_follower_whose_leader_and_quorum_are_gone_names_no_leader() {
    let _guard = static_cluster_cli_test_guard().await;
    let Some(binary) = option_env!("CARGO_BIN_EXE_ursula") else {
        tracing::warn!("CARGO_BIN_EXE_ursula is not set; skipping the lost quorum test");
        return;
    };
    let root = tempfile::tempdir().expect("temp root");
    let nodes = MetaNode::cluster(root.path(), 3);
    let peers = peers_of(&nodes);
    let meta = MetaToml {
        meta_voters: Some(META_VOTERS),
        leadership_balance_interval: Some("0s"),
        ..MetaToml::default()
    };
    let client = bounded_client();
    let mut procs = Processes::default();
    for node in &nodes {
        node.write(&peers, meta);
        procs.start(binary, node);
    }
    wait_bootstrapped(&client, &mut procs, &nodes).await;
    let streams = streams_by_group("quorumless");
    let url = |node: &MetaNode, stream: &str| format!("{}/quorumless/{stream}", node.url());
    for stream in streams.values() {
        put_until_created(&client, &url(&nodes[0], stream)).await;
        post_until_no_content(&client, &url(&nodes[0], stream), "before;").await;
    }
    let before = settled_leaders_and_terms(&client, &mut procs, &nodes).await;
    let led_by = |group: u32| before[&ReplicatedGroup::Data(u64::from(group))].0;
    // A data group's leader goes, and so does one more node: the node left
    // follows that group.
    let (_, gone_leader) = streams
        .keys()
        .map(|group| (*group, led_by(*group)))
        .next()
        .expect("a data group");
    let survivor = nodes
        .iter()
        .find(|node| node.id != gone_leader)
        .expect("a node besides the leader");
    let followed: BTreeSet<u32> = streams
        .keys()
        .copied()
        .filter(|group| led_by(*group) != survivor.id)
        .collect();
    let killed: Vec<u64> = nodes
        .iter()
        .map(|node| node.id)
        .filter(|id| *id != survivor.id)
        .collect();
    for id in &killed {
        procs.kill(*id);
    }

    let direct = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(Duration::from_secs(10))
        .build()
        .expect("HTTP client");
    let names_no_leader = async || {
        quorumless_follower_names_no_leader(&client, &direct, survivor, &streams, &followed, &url)
            .await
    };
    eventually_for(
        &mut procs,
        "the survivor names no leader it does not hear",
        SILENT_LEADER_FORGOTTEN_WITHIN,
        names_no_leader,
    )
    .await;
    // Longer than an election timeout: a node that still named a killed
    // leader, or campaigned, would show it by now.
    tokio::time::sleep(Duration::from_secs(4)).await;
    if let Err(found) =
        quorumless_follower_names_no_leader(&client, &direct, survivor, &streams, &followed, &url)
            .await
    {
        panic!(
            "node {} named a leader it does not hear once nodes {killed:?} were killed: {found}",
            survivor.id
        );
    }
}

/// Whether `node`, alone of its cluster, answers a write of each of
/// `streams` `503 NO_LEADER` with `Retry-After` (through `direct`, which
/// follows no redirect), names no leader of the `followed` groups in its
/// metrics, and lists `LeaderUnknown` for each of them in its maintenance
/// report.
async fn quorumless_follower_names_no_leader(
    client: &reqwest::Client,
    direct: &reqwest::Client,
    node: &MetaNode,
    streams: &BTreeMap<u32, String>,
    followed: &BTreeSet<u32>,
    url: &impl Fn(&MetaNode, &str) -> String,
) -> Result<(), String> {
    for (group, stream) in streams {
        let response = direct
            .post(url(node, stream))
            .header("content-type", "text/plain")
            .body("after;")
            .send()
            .await
            .map_err(|err| format!("group {group}: {err}"))?;
        let status = response.status();
        let retry = response
            .headers()
            .contains_key(reqwest::header::RETRY_AFTER);
        let body = response.bytes().await.unwrap_or_default();
        let code = ursula_headers::ErrorBody::parse(&body)
            .ok()
            .map(|body| body.code);
        if status != reqwest::StatusCode::SERVICE_UNAVAILABLE
            || !retry
            || code != Some(ursula_headers::ErrorCode::NoLeader)
        {
            return Err(format!(
                "group {group}: a write answered {status}, Retry-After {retry}, {code:?}"
            ));
        }
    }
    let metrics = try_node_metrics(client, &node.url()).await?;
    for replica in &metrics.groups {
        let group = u32::try_from(replica.raft_group_id).expect("a group id");
        if followed.contains(&group) && replica.current_leader.is_some() {
            return Err(format!("group {group}: names leader {replica:?}"));
        }
    }
    let (_, ready) = readiness(client, node).await?;
    let report = ready
        .raft_maintenance
        .ok_or_else(|| "no maintenance report".to_owned())?;
    for group in followed {
        let issues = report.group_issues.get(group);
        if !issues.is_some_and(|issues| {
            issues.contains(&ursula_proto::admin::RaftMaintenanceIssue::LeaderUnknown)
        }) {
            return Err(format!("group {group}: issues {issues:?}"));
        }
    }
    Ok(())
}

/// How many PreVote RPCs node `node` sent in this run
/// (`raft_grpc_pre_vote_requests`).
async fn pre_votes_sent(client: &reqwest::Client, node: &MetaNode) -> Result<u64, String> {
    let url = format!("{}/__ursula/metrics", node.url());
    let response = client
        .get(&url)
        .send()
        .await
        .map_err(|err| format!("GET {url}: {err}"))?;
    let metrics: serde_json::Value = response
        .json()
        .await
        .map_err(|err| format!("GET {url}: {err}"))?;
    metrics["raft_grpc_pre_vote_requests"]
        .as_u64()
        .ok_or_else(|| format!("GET {url}: no pre-vote counter"))
}

/// A Raft group of a meta test cluster: the meta group or a data group.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum ReplicatedGroup {
    Meta,
    Data(u64),
}

/// The replicas `metrics` reports, the meta replica's included, by group.
fn replicas_by_group(
    metrics: &ursula_proto::admin::NodeMetrics,
) -> impl Iterator<Item = (ReplicatedGroup, &RaftGroupMetrics)> {
    metrics
        .meta
        .iter()
        .map(|meta| (ReplicatedGroup::Meta, meta))
        .chain(
            metrics
                .groups
                .iter()
                .map(|group| (ReplicatedGroup::Data(group.raft_group_id), group)),
        )
}

/// Each group's `(leader, term)`, the meta group's included, once every
/// node of `nodes` runs a replica of each and names the same leader and
/// term for it in two samples in a row.
async fn settled_leaders_and_terms(
    client: &reqwest::Client,
    procs: &mut Processes,
    nodes: &[MetaNode],
) -> BTreeMap<ReplicatedGroup, (u64, u64)> {
    let mut last = None;
    eventually(
        procs,
        "every group settles on one leader and term",
        async || {
            let mut views: BTreeMap<ReplicatedGroup, Vec<(Option<u64>, u64)>> = BTreeMap::new();
            for node in nodes {
                let metrics = try_node_metrics(client, &node.url()).await?;
                for (group, replica) in replicas_by_group(&metrics) {
                    views
                        .entry(group)
                        .or_default()
                        .push((replica.current_leader, replica.current_term));
                }
            }
            let mut settled = BTreeMap::new();
            for (group, view) in &views {
                match view.as_slice() {
                    [(Some(leader), term), rest @ ..]
                        if rest.len().saturating_add(1) == nodes.len()
                            && rest.iter().all(|other| *other == (Some(*leader), *term)) =>
                    {
                        settled.insert(*group, (*leader, *term));
                    }
                    _ => return Err(format!("{group:?} is not settled: {views:?}")),
                }
            }
            if settled.len() != META_TEST_GROUPS.saturating_add(1) {
                return Err(format!("not every group runs on every node: {views:?}"));
            }
            if last.as_ref() == Some(&settled) {
                Ok(settled)
            } else {
                let found = format!("a first sample: {settled:?}");
                last = Some(settled);
                Err(found)
            }
        },
    )
    .await
}

/// For each group, the longest stretch in which no node of a meta test
/// cluster claimed its leadership, sampled from every node's metrics about
/// every 100 ms while a phase of a test runs. A node that does not answer
/// a sample claims nothing in it.
struct LeaderlessSampler {
    stop: Arc<std::sync::atomic::AtomicBool>,
    task: tokio::task::JoinHandle<BTreeMap<ReplicatedGroup, Duration>>,
}

impl LeaderlessSampler {
    fn start(nodes: &[MetaNode]) -> Self {
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(1))
            .build()
            .expect("HTTP client");
        let urls: Vec<String> = nodes.iter().map(MetaNode::url).collect();
        let task = {
            let stop = Arc::clone(&stop);
            tokio::spawn(async move {
                let mut since = BTreeMap::<ReplicatedGroup, std::time::Instant>::new();
                let mut longest = BTreeMap::<ReplicatedGroup, Duration>::new();
                while !stop.load(std::sync::atomic::Ordering::Acquire) {
                    let mut led = BTreeMap::<ReplicatedGroup, bool>::new();
                    for url in &urls {
                        let Ok(metrics) = try_node_metrics(&client, url).await else {
                            continue;
                        };
                        for (group, replica) in replicas_by_group(&metrics) {
                            let claims = replica.current_leader == Some(replica.node_id);
                            *led.entry(group).or_default() |= claims;
                        }
                    }
                    let now = std::time::Instant::now();
                    for (group, claimed) in led {
                        let gap = longest.entry(group).or_default();
                        if claimed {
                            if let Some(start) = since.remove(&group) {
                                *gap = (*gap).max(now.saturating_duration_since(start));
                            }
                        } else {
                            since.entry(group).or_insert(now);
                        }
                    }
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                let now = std::time::Instant::now();
                for (group, start) in since {
                    let gap = longest.entry(group).or_default();
                    *gap = (*gap).max(now.saturating_duration_since(start));
                }
                longest
            })
        };
        Self { stop, task }
    }

    /// Stops sampling, and answers each group's longest stretch without a
    /// leader.
    async fn stop(self) -> BTreeMap<ReplicatedGroup, Duration> {
        self.stop.store(true, std::sync::atomic::Ordering::Release);
        self.task.await.expect("the sampler ends")
    }
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

/// A port that accepts every connection and never answers on it, as a peer
/// behind a blackhole does.
struct Sink {
    port: u16,
    task: tokio::task::JoinHandle<()>,
}

impl Sink {
    async fn start() -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind the sink");
        let port = listener.local_addr().expect("sink address").port();
        let task = tokio::spawn(async move {
            let mut held = Vec::new();
            while let Ok((socket, _)) = listener.accept().await {
                held.push(socket);
            }
        });
        Self { port, task }
    }
}

impl Drop for Sink {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// The relays of a split-plane cluster: each node dials each peer's cluster
/// plane through a relay of its own, keyed by `(from, to)`, so one node can
/// be cut off from its peers, both ways, while its client plane serves.
struct PeerRelays(BTreeMap<(u64, u64), Relay>);

impl PeerRelays {
    async fn start(planes: &[SplitPlanePorts]) -> Self {
        let mut relays = BTreeMap::new();
        for from in planes {
            for to in planes.iter().filter(|to| to.node_id != from.node_id) {
                relays.insert((from.node_id, to.node_id), Relay::start(to.cluster).await);
            }
        }
        Self(relays)
    }

    /// The URL node `from` dials node `to`'s cluster plane at.
    fn url(&self, from: u64, to: u64) -> String {
        self.0
            .get(&(from, to))
            .unwrap_or_else(|| panic!("a relay from node {from} to node {to}"))
            .url()
    }

    /// Cuts node `id` off from its peers: its relays and theirs to it go to
    /// `sink`, and their open connections close.
    fn cut(&self, id: u64, sink: &Sink) {
        for ((from, to), relay) in &self.0 {
            if *from == id || *to == id {
                relay.point(sink.port, Open::Closed);
            }
        }
    }

    /// Reconnects every node to its peers.
    fn heal(&self, planes: &[SplitPlanePorts]) {
        for ((_, to), relay) in &self.0 {
            let plane = planes
                .iter()
                .find(|plane| plane.node_id == *to)
                .expect("the relay's node");
            relay.point(plane.cluster, Open::Closed);
        }
    }
}

/// The node among `among` of `planes` whose replica of `group` leads it with
/// a quorum that acknowledged it within the last 3 s.
async fn live_leader(
    client: &reqwest::Client,
    planes: &[SplitPlanePorts],
    among: &[u64],
    group: u64,
) -> Result<u64, String> {
    let mut seen = Vec::new();
    for plane in planes.iter().filter(|plane| among.contains(&plane.node_id)) {
        let replica = try_node_metrics(client, &plane.client_url())
            .await?
            .groups
            .into_iter()
            .find(|replica| replica.raft_group_id == group);
        match replica {
            Some(replica)
                if replica.current_leader == Some(plane.node_id) && replica.leads_live =>
            {
                return Ok(plane.node_id);
            }
            other => seen.push(format!("node {}: {other:?}", plane.node_id)),
        }
    }
    Err(format!("no live leader of group {group}: {seen:?}"))
}

/// The longest a node takes to answer a write while it cannot reach a
/// quorum: its leader lease (3 s), the wake-up after it and a margin for a
/// loaded runner, well within the client's 10 s.
const LAPSED_LEADER_ANSWERS_WITHIN: Duration = Duration::from_secs(6);

/// The longest a write takes through the gateway when the node it sends the
/// write to is stopped: the gateway's 5 s header timeout, the repeat on
/// another node and a margin, within the client's 10 s.
const STOPPED_LEADER_ANSWERS_WITHIN: Duration = Duration::from_secs(8);

/// One attempt of a [`ProducerLoad`].
#[derive(Debug, Clone)]
struct ProducerAttempt {
    seq: u64,
    sent: tokio::time::Instant,
    took: Duration,
    /// Whether the answer was a retryable refusal
    /// ([`is_retryable_refusal`]).
    retryable: bool,
    outcome: ursula_headers::write_outcome::WriteOutcome,
}

/// Appends `{prefix}-{seq};` to a stream as producer `prefix` at epoch 0,
/// one write at a time, every 20 ms until stopped. A write whose answer does
/// not say that it holds is sent again with the same seq and payload, as a
/// producer client does, so the stream holds each seq once and in order.
/// Each attempt goes to the next URL of the stream in turn, as a client
/// behind a load balancer over several gateways sends its retries.
struct ProducerLoad {
    stop: Arc<std::sync::atomic::AtomicBool>,
    task: tokio::task::JoinHandle<Vec<ProducerAttempt>>,
}

impl ProducerLoad {
    fn start(urls: Vec<String>, prefix: &'static str) -> Self {
        use ursula_headers::names;
        use ursula_headers::write_outcome::WriteKind;
        use ursula_headers::write_outcome::WriteOutcome;
        use ursula_headers::write_outcome::classify;

        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let task = {
            let stop = Arc::clone(&stop);
            tokio::spawn(async move {
                let client = bounded_client();
                let mut attempts: Vec<ProducerAttempt> = Vec::new();
                let mut seq = 0_u64;
                for stream in urls.iter().cycle() {
                    if stop.load(std::sync::atomic::Ordering::Acquire) {
                        break;
                    }
                    let sent = tokio::time::Instant::now();
                    let answer = client
                        .post(stream)
                        .header("content-type", "text/plain")
                        .header(names::PRODUCER_ID, prefix)
                        .header(names::PRODUCER_EPOCH, "0")
                        .header(names::PRODUCER_SEQ, seq.to_string())
                        .body(format!("{prefix}-{seq};"))
                        .send()
                        .await;
                    let (retryable, outcome) = match answer {
                        Err(_unanswered) => (false, WriteOutcome::NO_ANSWER),
                        Ok(response) => {
                            let retryable =
                                is_retryable_refusal(response.status(), response.headers());
                            let status = response.status().as_u16();
                            let headers = response.headers().clone();
                            let body = response.bytes().await.unwrap_or_default();
                            let outcome = classify(
                                WriteKind::Producer,
                                status,
                                headers
                                    .iter()
                                    .map(|(name, value)| (name.as_str(), value.as_bytes())),
                                &body,
                            );
                            (retryable, outcome)
                        }
                    };
                    let holds = outcome.holds();
                    attempts.push(ProducerAttempt {
                        seq,
                        sent,
                        took: sent.elapsed(),
                        retryable,
                        outcome,
                    });
                    if holds {
                        seq = seq.saturating_add(1);
                    }
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
                attempts
            })
        };
        Self { stop, task }
    }

    /// Stops the load once its write in flight answered, and answers every
    /// attempt.
    async fn stop(self) -> Vec<ProducerAttempt> {
        self.stop.store(true, std::sync::atomic::Ordering::Release);
        self.task.await.expect("the producer load ends")
    }
}

/// Reads `stream`, which a [`ProducerLoad`] of `prefix` wrote to, until it
/// holds every seq up to `held`, one the load acknowledged, and checks that
/// it holds each seq once, in order, from 0. A write after `held` that the
/// load sent but saw no receipt for may be held too.
async fn check_producer_read(client: &reqwest::Client, stream: &str, prefix: &str, held: u64) {
    let acknowledged: Vec<String> = (0..=held).map(|seq| format!("{prefix}-{seq};")).collect();
    let read = read_until_holding(
        client,
        &format!("{stream}?offset=0&max_bytes=8388608"),
        &acknowledged,
    )
    .await;
    let held: Vec<String> = String::from_utf8_lossy(&read)
        .split(';')
        .filter(|token| !token.is_empty())
        .map(str::to_owned)
        .collect();
    let in_order: Vec<String> = (0..held.len())
        .map(|seq| format!("{prefix}-{seq}"))
        .collect();
    assert_eq!(held, in_order, "{stream} holds each seq once, in order");
}

/// A group's leader that its peers cannot reach, or that answers nothing at
/// all, holds no write until the client gives up. Three nodes split their
/// planes, and each reaches each peer's cluster plane through a relay of its
/// own ([`PeerRelays`]).
///
/// 1. The leader of a group is cut off from its peers, both ways, into a
///    port that never answers, while its client plane serves. Under a load
///    sent to it and one through a gateway, its peers elect a new leader.
///    It answers every write within [`LAPSED_LEADER_ANSWERS_WITHIN`]: a
///    write it proposed before the cut, which may still commit, `503
///    OUTCOME_UNKNOWN` once its leader lease ends, and later ones `503
///    NO_LEADER`. Through the gateway every write is answered as promptly,
///    and the new leader acknowledges writes while the cut lasts. Once it
///    heals, each stream holds every acknowledged write once, in order, and
///    no write whose `503` proves it applied nothing. A leader that holds
///    writes for as long as it cannot reach a quorum fails it. No peer
///    answers the cut-off node, so it reports a degraded probe, sheds for
///    cluster egress on its first such tick and stops campaigning. Its
///    pre-votes reach no peer, so before the shed too it raises no term of
///    a group it follows: no term of it is more than one above its term at
///    the cut, which leaves room for a leadership move that reached it as
///    the cut began, and none of the group it led is above the new
///    leader's. Its terms then hold for longer than an election timeout
///    while the cut lasts, and after the heal it follows each group's
///    leader.
/// 2. The group's leader is stopped (`SIGSTOP`) under a producer load
///    through the gateway. A write sent to it gets no answer within the
///    gateway's 5 s header timeout, and the gateway sends it again, with
///    its key, to another node, which answers it from the new leader, or,
///    while its peers still name the stopped node, ends with the `504` of
///    the first hop without going back to it. Every attempt is answered
///    within [`STOPPED_LEADER_ANSWERS_WITHIN`], a write after the stop
///    holds, and the stream holds each seq once, in order. A gateway that
///    waits out the client's timeout on the stopped node fails it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cli_a_leader_cut_off_or_stopped_answers_writes_well_within_the_client_timeout() {
    let _guard = static_cluster_cli_test_guard().await;
    let Some(binary) = option_env!("CARGO_BIN_EXE_ursula") else {
        tracing::warn!("CARGO_BIN_EXE_ursula is not set; skipping the cut-off leader test");
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
    let relays = PeerRelays::start(&planes).await;
    let sink = Sink::start().await;
    let wal_root = tempfile::tempdir().expect("WAL root");
    let mut procs = Processes::default();
    for plane in &planes {
        let id = plane.node_id;
        let child = spawn_split_plane_node_dialing(binary, &planes, id, wal_root.path(), |peer| {
            relays.url(id, peer.node_id)
        });
        procs.0.insert(id, child);
    }
    let client = bounded_client();
    let all: Vec<u64> = planes.iter().map(|plane| plane.node_id).collect();
    let upstreams: Vec<String> = planes.iter().map(SplitPlanePorts::client_url).collect();
    let (_gateway, gateway) = spawn_gateway_over(binary, &upstreams);
    let group = 0_usize;
    let group_id = u64::try_from(group).unwrap();
    let client_url = |id: u64| {
        planes
            .iter()
            .find(|plane| plane.node_id == id)
            .expect("a node of the cluster")
            .client_url()
    };

    // 1. The group's leader is cut off from its peers.
    let leader = eventually(&mut procs, "the group has a live leader", async || {
        live_leader(&client, &planes, &all, group_id).await
    })
    .await;
    let direct = format!(
        "{}/cutdirect/{}",
        client_url(leader),
        stream_per_group("cutdirect")[group]
    );
    let through = format!(
        "{gateway}/cutgateway/{}",
        stream_per_group("cutgateway")[group]
    );
    create_stream(&client, &mut procs, &direct).await;
    create_stream(&client, &mut procs, &through).await;
    let direct_load = GatewayLoad::start(vec![direct.clone()], "direct");
    let gateway_load = GatewayLoad::start(vec![through.clone()], "gateway");
    tokio::time::sleep(Duration::from_secs(1)).await;
    let terms_of = async |id: u64| -> BTreeMap<u64, u64> {
        let metrics = try_node_metrics(&client, &client_url(id))
            .await
            .unwrap_or_else(|err| panic!("node {id}'s metrics: {err}"));
        metrics
            .groups
            .iter()
            .map(|group| (group.raft_group_id, group.current_term))
            .collect()
    };
    let cut_terms = terms_of(leader).await;
    let cut = tokio::time::Instant::now();
    relays.cut(leader, &sink);
    let others: Vec<u64> = all.iter().copied().filter(|id| *id != leader).collect();
    let successor = eventually(&mut procs, "the other nodes elect a leader", async || {
        live_leader(&client, &planes, &others, group_id).await
    })
    .await;
    let elected = tokio::time::Instant::now();
    // No peer answers the cut-off node, so its egress gate sheds and stops
    // its elections. It must not raise a term while the cut lasts: a term
    // above its group's leader's deposes that leader once the cut heals.
    // Its pre-votes reach no peer, so it never campaigns.
    let cut_plane = planes
        .iter()
        .find(|plane| plane.node_id == leader)
        .expect("a node of the cluster");
    let what = format!("node {leader} sheds for cluster egress while cut off");
    eventually(&mut procs, &what, async || {
        let shed: ursula_proto::admin::LeadershipShedStatus = client
            .get(format!(
                "{}/__ursula/leadership-shed",
                cut_plane.cluster_url()
            ))
            .send()
            .await
            .map_err(|err| err.to_string())?
            .json()
            .await
            .map_err(|err| err.to_string())?;
        if shed.egress == ursula_proto::admin::EgressProbeStatus::Degraded
            && sheds(&shed, ursula_raft::LeadershipShedReason::ClusterEgress)
            && !shed.should_campaign
        {
            Ok(())
        } else {
            Err(format!("{shed:?}"))
        }
    })
    .await;
    let shed_terms = terms_of(leader).await;
    // A follower's pre-vote, at the earliest 779 ms after the cut, gets no
    // grant, so it does not campaign before the gate sheds, at most 2 s
    // after the cut.
    let raised: Vec<(u64, u64, u64)> = shed_terms
        .iter()
        .map(|(group, term)| (*group, cut_terms[group], *term))
        .filter(|(_, at_cut, shed)| *shed > at_cut.saturating_add(1))
        .collect();
    assert!(
        raised.is_empty(),
        "node {leader} campaigned in a group before it shed (group, term at the cut, term at \
         the shed): {raised:?}"
    );
    // Longer than the 3 s maximum election timeout: a node that still
    // campaigned would raise the term of every group it follows.
    tokio::time::sleep(Duration::from_secs(4)).await;
    assert_eq!(
        terms_of(leader).await,
        shed_terms,
        "node {leader} raised a group's term while cut off and shedding"
    );
    // The balancer may have moved the group between the other two nodes
    // since the election.
    let holder = eventually(&mut procs, "the other nodes lead the group", async || {
        live_leader(&client, &planes, &others, group_id).await
    })
    .await;
    let holder_term = terms_of(holder).await[&group_id];
    // The healed node meets the new leader with the term it kept through
    // the cut. Its first append from the leader brings it to the leader's
    // term only while that term is not below its own: a term above the
    // leader's deposes the leader at the heal.
    let cut_term = terms_of(leader).await[&group_id];
    assert!(
        cut_term <= holder_term,
        "node {leader} holds term {cut_term} of group {group_id}, above its new leader's \
         {holder_term}"
    );
    let healed = tokio::time::Instant::now();
    relays.heal(&planes);
    let direct_writes = direct_load.stop().await;
    let gateway_writes = gateway_load.stop().await;
    // The healed node still sheds after the heal (its gate clears after six
    // clean ticks), so it neither campaigns nor takes a leadership from the
    // balancer, and in every group it follows a leader among the other
    // nodes at that leader's term. The balancer may still move a group
    // between the other two nodes, which raises the term, so the check
    // names neither the leader nor the term.
    let replica_of = async |id: u64, group: u64| {
        try_node_metrics(&client, &client_url(id))
            .await?
            .groups
            .into_iter()
            .find(|replica| replica.raft_group_id == group)
            .ok_or_else(|| format!("node {id} runs no replica of group {group}"))
    };
    for group in shed_terms.keys().copied() {
        let what = format!("node {leader} follows group {group}'s leader after the heal");
        eventually(&mut procs, &what, async || {
            let healed_replica = replica_of(leader, group).await?;
            let new_leader = live_leader(&client, &planes, &others, group).await?;
            let leading = replica_of(new_leader, group).await?;
            if healed_replica.current_leader == Some(new_leader)
                && healed_replica.current_term == leading.current_term
                && (group != group_id || leading.current_term >= holder_term)
            {
                Ok(())
            } else {
                Err(format!(
                    "node {leader}: {healed_replica:?}, node {new_leader}: {leading:?}"
                ))
            }
        })
        .await;
    }
    for (writes, via) in [
        (&direct_writes, "the cut-off leader"),
        (&gateway_writes, "the gateway"),
    ] {
        assert_only_retryable(writes, false);
        let slow: Vec<&LoadWrite> = writes
            .iter()
            .filter(|write| write.took >= LAPSED_LEADER_ANSWERS_WITHIN)
            .collect();
        assert!(slow.is_empty(), "{via} answered writes late: {slow:?}");
    }
    // A write the cut-off leader proposed in the cut, sent to it directly or
    // through the gateway, ends `OUTCOME_UNKNOWN` once its lease ends
    // (`until_lapsed` of ursula-raft), before the heal. A write queued behind
    // it was never proposed and ends `NO_LEADER`, and so does every later
    // one, at the front door before its body.
    let answered_at = |write: &LoadWrite| write.sent.checked_add(write.took);
    let in_cut = |write: &&LoadWrite| {
        write.sent < healed && answered_at(write).is_some_and(|answered| answered >= cut)
    };
    let unknown = LoadAnswer::Retryable(Some(ursula_headers::ErrorCode::OutcomeUnknown));
    let refused = LoadAnswer::Retryable(Some(ursula_headers::ErrorCode::NoLeader));
    assert!(
        direct_writes
            .iter()
            .chain(gateway_writes.iter())
            .filter(in_cut)
            .any(|write| write.answer == unknown
                && answered_at(write).is_some_and(|answered| answered < healed)),
        "node {leader} ended no write it proposed in the cut as unknown before the heal: \
         {direct_writes:?} {gateway_writes:?}"
    );
    let mut direct_in_cut = direct_writes
        .iter()
        .filter(in_cut)
        .skip_while(|write| write.answer == LoadAnswer::Acknowledged);
    let held = direct_in_cut.next().unwrap_or_else(|| {
        panic!("node {leader} refused no write while cut off: {direct_writes:?}")
    });
    assert!(
        (held.answer == unknown || held.answer == refused)
            && answered_at(held).is_some_and(|answered| answered < healed),
        "node {leader}'s first unacknowledged write in the cut: {held:?}"
    );
    let not_refused: Vec<&LoadWrite> = direct_in_cut
        .filter(|write| write.answer != refused)
        .collect();
    assert!(
        not_refused.is_empty(),
        "node {leader} answered writes after its lease ended other than NO_LEADER: \
         {not_refused:?}"
    );
    assert!(
        gateway_writes.iter().any(|write| write.sent >= elected
            && write.sent < healed
            && write.answer == LoadAnswer::Acknowledged),
        "node {successor} acknowledged no write through the gateway while node {leader} was \
         cut off: {gateway_writes:?}"
    );
    check_load_read(&client, &direct, &direct_writes).await;
    check_load_read(&client, &through, &gateway_writes).await;

    // 2. The group's leader is stopped.
    let leader = eventually(
        &mut procs,
        "the group has a live leader again",
        async || live_leader(&client, &planes, &all, group_id).await,
    )
    .await;
    let stream = format!("{gateway}/stopped/{}", stream_per_group("stopped")[group]);
    create_stream(&client, &mut procs, &stream).await;
    let load = ProducerLoad::start(vec![stream.clone()], "stopped");
    tokio::time::sleep(Duration::from_secs(1)).await;
    let stopped = tokio::time::Instant::now();
    procs.signal(leader, "-STOP");
    let others: Vec<u64> = all.iter().copied().filter(|id| *id != leader).collect();
    eventually(&mut procs, "the other nodes elect a leader", async || {
        live_leader(&client, &planes, &others, group_id).await
    })
    .await;
    tokio::time::sleep(Duration::from_secs(6)).await;
    let attempts = load.stop().await;
    procs.signal(leader, "-CONT");
    let slow: Vec<&ProducerAttempt> = attempts
        .iter()
        .filter(|attempt| attempt.took >= STOPPED_LEADER_ANSWERS_WITHIN)
        .collect();
    assert!(slow.is_empty(), "writes answered late: {slow:?}");
    let held = attempts
        .iter()
        .find(|attempt| attempt.sent >= stopped && attempt.outcome.holds())
        .unwrap_or_else(|| panic!("no write after node {leader} stopped holds: {attempts:?}"));
    check_producer_read(&client, &stream, "stopped", held.seq).await;
}

/// The longest a gateway's probes take to eject a node that stops
/// answering: a probe interval (250 ms) and a probe timeout (1.5 s), and a
/// margin for a loaded runner.
const PROBES_EJECT_WITHIN: Duration = Duration::from_secs(3);

/// The gateway's default `--response-header-timeout`.
const GATEWAY_HEADER_TIMEOUT: Duration = Duration::from_secs(5);

/// How many upstreams the gateway at `gateway` leaves out of selection now,
/// from its metrics.
async fn gateway_ejected(client: &reqwest::Client, gateway: &str) -> Result<u64, String> {
    let url = format!("{gateway}/__ursula/gateway/metrics");
    let metrics: serde_json::Value = client
        .get(&url)
        .send()
        .await
        .map_err(|err| format!("GET {url}: {err}"))?
        .json()
        .await
        .map_err(|err| format!("GET {url}: {err}"))?;
    metrics["upstream_ejected"]
        .as_u64()
        .ok_or_else(|| format!("GET {url}: no upstream_ejected in {metrics}"))
}

/// Polls every gateway of `gateways` until each reports `ejected` upstreams
/// left out of selection, and answers how long each took from `since`.
/// Fails after `within`.
async fn wait_gateways_eject(
    client: &reqwest::Client,
    gateways: &[&str],
    ejected: u64,
    since: tokio::time::Instant,
    within: Duration,
) -> Vec<Duration> {
    let mut took: Vec<Option<Duration>> = vec![None; gateways.len()];
    loop {
        for (gateway, took) in gateways.iter().zip(took.iter_mut()) {
            if took.is_none() && gateway_ejected(client, gateway).await == Ok(ejected) {
                *took = Some(since.elapsed());
            }
        }
        if took.iter().all(Option::is_some) {
            return took.into_iter().flatten().collect();
        }
        assert!(
            since.elapsed() < within,
            "not every gateway reports {ejected} upstreams ejected within {within:?}: {took:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Every gateway in front of a cluster leaves a stopped node out of
/// selection on its own, whether or not requests reach it, so a write that
/// a client retries on another gateway does not wait for the node again.
/// Three nodes behind three gateways:
///
/// 1. A producer appends to a stream of a group through gateways A and B in
///    turn, each retry to the other one, as a client behind a load balancer
///    over the gateways does. Gateway C gets no request.
/// 2. The group's leader is stopped (`SIGSTOP`). Each of the three gateways
///    reports it ejected within [`PROBES_EJECT_WITHIN`] of the stop, C
///    included, and the other nodes elect a leader.
/// 3. No attempt waits out the gateway's header timeout: an attempt in
///    flight to the stopped node when a gateway's probes eject it goes on at
///    once to another node, and an attempt sent once the gateways ejected
///    the node never goes there. A write after the stop holds. A gateway
///    that ejects a node only once a request to it timed out, or that waits
///    out the timeout of an attempt in flight to a node its probes ejected,
///    fails it.
/// 4. Once the node continues (`SIGCONT`), every gateway readmits it within
///    [`PROBES_EJECT_WITHIN`]. The stream holds each seq once, in order.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cli_every_gateway_ejects_a_stopped_node_and_a_retry_does_not_wait_for_it_again() {
    let _guard = static_cluster_cli_test_guard().await;
    let Some(binary) = option_env!("CARGO_BIN_EXE_ursula") else {
        tracing::warn!("CARGO_BIN_EXE_ursula is not set; skipping the gateway ejection test");
        return;
    };
    let root = tempfile::tempdir().expect("temp root");
    let nodes = MetaNode::cluster(root.path(), 3);
    let peers = peers_of(&nodes);
    let meta = MetaToml {
        meta_voters: Some(META_VOTERS),
        replication_factor: Some(3),
        ..MetaToml::default()
    };
    let client = bounded_client();
    let mut procs = Processes::default();
    for node in &nodes {
        node.write(&peers, meta);
        procs.start(binary, node);
    }
    wait_settled(&client, &mut procs, &nodes).await;
    for node in &nodes {
        wait_readiness_reason(&client, &mut procs, node, None).await;
    }
    let (_gateway_a, a) = spawn_gateway(binary, &nodes);
    let (_gateway_b, b) = spawn_gateway(binary, &nodes);
    let (_gateway_c, c) = spawn_gateway(binary, &nodes);
    let group = 0_usize;
    let stream = &stream_per_group("frozen")[group];
    let through = |gateway: &str| format!("{gateway}/frozen/{stream}");
    create_stream(&client, &mut procs, &through(&a)).await;
    let leaders = wait_leaders_settle(&client, &mut procs, &nodes).await;
    let leader = leaders[&u64::try_from(group).unwrap()];
    for gateway in [&a, &b, &c] {
        assert_eq!(gateway_ejected(&client, gateway).await, Ok(0), "{gateway}");
    }

    // 1. and 2. The leader stops under the load.
    let load = ProducerLoad::start(vec![through(&a), through(&b)], "frozen");
    tokio::time::sleep(Duration::from_secs(1)).await;
    let stopped = tokio::time::Instant::now();
    procs.signal(leader, "-STOP");
    let ejected =
        wait_gateways_eject(&client, &[&a, &b, &c], 1, stopped, PROBES_EJECT_WITHIN).await;
    let others: Vec<&MetaNode> = nodes.iter().filter(|node| node.id != leader).collect();
    eventually(&mut procs, "the other nodes elect a leader", async || {
        for node in &others {
            let replica = try_node_metrics(&client, &node.url())
                .await?
                .groups
                .into_iter()
                .find(|replica| replica.raft_group_id == u64::try_from(group).unwrap());
            if replica.is_some_and(|replica| {
                replica.current_leader == Some(node.id) && replica.leads_live
            }) {
                return Ok(node.id);
            }
        }
        Err(format!(
            "no leader among {:?} yet",
            others.iter().map(|node| node.id).collect::<Vec<_>>()
        ))
    })
    .await;
    tokio::time::sleep(Duration::from_secs(3)).await;
    let attempts = load.stop().await;

    // 3. No attempt waits out the header timeout for the stopped node.
    let waited: Vec<&ProducerAttempt> = attempts
        .iter()
        .filter(|attempt| attempt.took >= GATEWAY_HEADER_TIMEOUT)
        .collect();
    assert!(
        waited.is_empty(),
        "attempts waited out the header timeout though every gateway ejected node {leader} \
         within {ejected:?} of its stop: {waited:?}"
    );
    let slow: Vec<&ProducerAttempt> = attempts
        .iter()
        .filter(|attempt| attempt.took >= STOPPED_LEADER_ANSWERS_WITHIN)
        .collect();
    assert!(slow.is_empty(), "writes answered late: {slow:?}");
    let held = attempts
        .iter()
        .find(|attempt| attempt.sent >= stopped && attempt.outcome.holds())
        .unwrap_or_else(|| panic!("no write after node {leader} stopped holds: {attempts:?}"));
    let held_seq = held.seq;

    // 4. The node continues and every gateway readmits it.
    let continued = tokio::time::Instant::now();
    procs.signal(leader, "-CONT");
    wait_gateways_eject(&client, &[&a, &b, &c], 0, continued, PROBES_EJECT_WITHIN).await;
    check_producer_read(&client, &through(&a), "frozen", held_seq).await;
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
    sigterm_and_wait_for_clean_exit_within(child, Duration::from_secs(10)).await;
}

/// [`sigterm_and_wait_for_clean_exit`], failing after `budget`.
async fn sigterm_and_wait_for_clean_exit_within(child: &mut ChildGuard, budget: Duration) {
    let status = Command::new("kill")
        .arg("-TERM")
        .arg(child.child.id().to_string())
        .status()
        .expect("send SIGTERM");
    assert!(status.success());
    let deadline = std::time::Instant::now().checked_add(budget).unwrap();
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

/// The meta, placement and cold-flush keys of a node config.
/// `None` leaves a key out, so the server's default applies.
#[derive(Debug, Clone, Copy, Default)]
struct MetaToml<'a> {
    /// `raft.meta_voters`.
    meta_voters: Option<&'a [u64]>,
    /// `raft.placement.replication_factor`.
    replication_factor: Option<u8>,
    /// `storage.cold.flush_min_hot_size`, in bytes.
    flush_min_hot_bytes: Option<u64>,
    /// `governance.leadership_balance.interval`: `"0s"` turns the
    /// leadership balancer off.
    leadership_balance_interval: Option<&'a str>,
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

        if let Some(interval) = meta.leadership_balance_interval {
            writeln!(
                config,
                "\n[governance.leadership_balance]\ninterval = \"{interval}\""
            )
            .unwrap();
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
    spawn_split_plane_node_dialing(binary, planes, node_id, wal_root, |plane| {
        plane.cluster_url()
    })
}

/// [`spawn_split_plane_node`], whose node dials each peer's cluster plane
/// at `cluster_url(peer)` and its own at its cluster port.
fn spawn_split_plane_node_dialing(
    binary: &str,
    planes: &[SplitPlanePorts],
    node_id: u64,
    wal_root: &Path,
    cluster_url: impl Fn(&SplitPlanePorts) -> String,
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
        let url = if plane.node_id == node_id {
            plane.cluster_url()
        } else {
            cluster_url(plane)
        };
        writeln!(
            config,
            r#"
[[raft.peers]]
node_id = {}
url = "{url}"
client_url = "{}""#,
            plane.node_id,
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

async fn put_snapshot_until_no_content(client: &reqwest::Client, url: &str, body: &'static str) {
    for _ in 0..100 {
        if let Ok(response) = client
            .put(url)
            .header("content-type", "text/plain")
            .body(body)
            .send()
            .await
            && response.status() == reqwest::StatusCode::NO_CONTENT
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("snapshot publish did not succeed at {url}");
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
    try_admin_test_post(client, url)
        .await
        .unwrap_or_else(|err| panic!("{err}"))
}

/// [`admin_test_post`], with a node that does not answer its metrics as an
/// `Err`, for a polled probe.
async fn try_admin_test_post(
    client: &reqwest::Client,
    url: String,
) -> Result<reqwest::RequestBuilder, String> {
    let mut metrics_url =
        reqwest::Url::parse(&url).map_err(|err| format!("admin URL {url}: {err}"))?;
    metrics_url.set_path("/__ursula/metrics");
    metrics_url.set_query(None);
    let metrics: serde_json::Value = client
        .get(metrics_url.clone())
        .send()
        .await
        .map_err(|err| format!("GET {metrics_url}: {err}"))?
        .json()
        .await
        .map_err(|err| format!("GET {metrics_url}: {err}"))?;
    let incarnation = metrics["process_incarnation"]
        .as_str()
        .ok_or_else(|| format!("GET {metrics_url}: no process incarnation"))?
        .to_owned();
    Ok(client
        .post(url)
        .header(ursula_proto::admin::PROCESS_INCARNATION_HEADER, incarnation))
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

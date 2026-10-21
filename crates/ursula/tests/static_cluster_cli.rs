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

use ursula_runtime::ColdStore;

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
    let deadline = tokio::time::Instant::now()
        .checked_add(Duration::from_secs(10))
        .unwrap();
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
    std::net::TcpStream::connect(("127.0.0.1", port))
        .expect_err("the HTTP listener must not be bound before admission");
    std::net::TcpStream::connect(("127.0.0.1", admin_port))
        .expect_err("the admin listener must not be bound before admission");
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
        let deadline = tokio::time::Instant::now()
            .checked_add(Duration::from_secs(10))
            .unwrap();
        loop {
            if let Some(exit) = rejected.child.try_wait().unwrap() {
                assert!(!exit.success());
                break;
            }
            assert!(tokio::time::Instant::now() < deadline);
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(!logs.exists());
        std::net::TcpStream::connect(("127.0.0.1", port))
            .expect_err("the HTTP listener must not be bound after a refused admission");
        std::net::TcpStream::connect(("127.0.0.1", admin_port))
            .expect_err("the admin listener must not be bound after a refused admission");
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
        spawn_split_plane_node(binary, &planes, 2, false, wal_root.path()),
        spawn_split_plane_node(binary, &planes, 3, false, wal_root.path()),
        spawn_split_plane_node(binary, &planes, 1, true, wal_root.path()),
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
    write_single_node_cluster_config(&config, port, 1, 2, &base, true, &root.path().join("wal"));
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
            spawn_split_plane_node(binary, planes, 2, false, wal_root.path()),
            spawn_split_plane_node(binary, planes, 3, false, wal_root.path()),
            spawn_split_plane_node(binary, planes, 1, true, wal_root.path()),
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

/// Exercise the production paths used by verify-cluster and wait --node
/// against a real single-voter server, not a synthetic metrics response.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cli_single_voter_passes_verify_cluster_and_wait_node() {
    let _guard = static_cluster_cli_test_guard().await;
    let port = free_port();
    let base_url = format!("http://127.0.0.1:{port}");
    let root = tempfile::tempdir().expect("single-voter root");
    let config = root.path().join("cluster.toml");
    let admin_port = write_single_node_cluster_config(
        &config,
        port,
        1,
        1,
        &base_url,
        true,
        &root.path().join("wal"),
    );
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
        let admin_port = write_cluster_config(
            &config_path,
            ports[index],
            *node_id,
            1,
            &peers,
            *node_id == 1,
            &log_dir,
        );
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
        write_single_node_cluster_config(&config_path, port, 1, 1, &base_url, true, &log_dir);
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
    let ready = client
        .get(format!("{base_url}/__ursula/ready"))
        .send()
        .await
        .expect("readiness");
    assert_eq!(ready.status(), reqwest::StatusCode::SERVICE_UNAVAILABLE);
    let ready: serde_json::Value = ready.json().await.expect("readiness JSON");
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
            *node_id == 1,
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
    let read = client
        .get(format!(
            "{}/benchcmp/cli-cluster-id?offset=0&max_bytes=64",
            peers[2].1
        ))
        .send()
        .await
        .expect("read on node 3");
    let read_status = read.status();
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
        let admin_port =
            write_cluster_config(&config, port, node_id, 1, &peers, node_id == 1, &wal_dir);
        let text = std::fs::read_to_string(&config).expect("read node config");
        assert!(text.contains("[raft.wal]\n"));
        assert!(text.contains("init_membership_per_group = false\n"));
        std::fs::write(
            &config,
            text.replace(
                "init_membership_per_group = false\n",
                "init_membership_per_group = false\ndeleted_data_max_age = \"90s\"\n\
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

/// #669: a node without `raft.peers` runs the snapshot driver, and its
/// group's leader sweeps expired streams in the background. A stream whose
/// TTL ran out and that nothing reads or writes again leaves the node's
/// snapshot, its Raft log and every file in its Raft log directory within
/// `raft.deleted_data_max_age` of the sweep, and a read of it answers 404.
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
    let admin_port = write_cluster_config(&config, port, 1, 1, &[], false, &wal_dir);
    let text = std::fs::read_to_string(&config).expect("read node config");
    assert!(text.contains("[raft.wal]\n"));
    std::fs::write(
        &config,
        text.replace(
            "init_membership_per_group = false\n",
            "init_membership_per_group = false\ndeleted_data_max_age = \"16s\"\n\
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
            node_id == 1,
            &wal_dir,
            "s3",
            Some(&cold_root),
        );
        let text = std::fs::read_to_string(&config).expect("read node config");
        assert!(text.contains("init_membership_per_group = false\n"));
        assert!(text.contains("flush_interval = \"1s\"\n"));
        // Keep the records hot, so that the snapshots hold them.
        std::fs::write(
            &config,
            text.replace(
                "init_membership_per_group = false\n",
                "init_membership_per_group = false\ndeleted_data_max_age = \"160s\"\n\
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

/// The object keys that the snapshot record of group 0 in `wal_dir`
/// (`group-0.snapshot.json`) names, decoded as a restore decodes it: none
/// before the node wrote a record, else its one S3 key (#687).
fn named_snapshot_keys(wal_dir: &Path) -> BTreeSet<String> {
    let records = files_named(wal_dir, "group-0.snapshot.json");
    assert!(records.len() <= 1, "{records:?}");
    let Some(record) = records.first() else {
        return BTreeSet::new();
    };
    let bytes = match std::fs::read(record) {
        Ok(bytes) => bytes,
        // The node's directory was wiped since the walk.
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            return BTreeSet::new();
        }
        Err(err) => panic!("read the snapshot record {}: {err}", record.display()),
    };
    let pointer = ursula_raft::persisted_snapshot_pointer(&bytes)
        .unwrap_or_else(|err| panic!("decode the snapshot record {}: {err}", record.display()));
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
            node_id == 1,
            &wal_dir,
            "s3",
            Some(&cold_root),
        );
        let text = std::fs::read_to_string(&config).expect("read node config");
        assert!(text.contains("init_membership_per_group = false\n"));
        std::fs::write(
            &config,
            text.replace(
                "init_membership_per_group = false\n",
                "init_membership_per_group = false\ninstall_snapshot_timeout = \"40s\"\n",
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

/// Stands in front of node 3's Raft endpoint and preserves HTTP/2 bodies and
/// trailers. It can delay only snapshot transfer, or partition the node from
/// every inbound RPC. Metrics and readiness use the server's real listeners
/// directly.
struct RaftProxy {
    url: String,
    release: tokio::sync::watch::Sender<bool>,
    blocked: tokio::sync::watch::Receiver<bool>,
    reachable: Arc<tokio::sync::RwLock<bool>>,
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
        let reachable = Arc::new(tokio::sync::RwLock::new(true));
        let gate = Arc::clone(&reachable);
        let proxy = tower::service_fn(move |request: axum::http::Request<axum::body::Body>| {
            let channel = channel.clone();
            let mut released = released.clone();
            let blocked_tx = blocked_tx.clone();
            let gate = Arc::clone(&gate);
            async move {
                if request.uri().path() == ursula_raft::RAFT_GRPC_FULL_SNAPSHOT_PATH {
                    if !*released.borrow() {
                        blocked_tx.send_replace(true);
                    }
                    released.wait_for(|ready| *ready).await.unwrap();
                }
                let unreachable = || {
                    axum::http::Response::builder()
                        .status(axum::http::StatusCode::BAD_GATEWAY)
                        .body(axum::body::Body::empty())
                        .unwrap()
                };
                // Held until the target answers, so `partition` waits for
                // every RPC that it lets through.
                let reachable = gate.read().await;
                let response = if *reachable {
                    match channel.oneshot(request.map(tonic::body::boxed)).await {
                        Ok(response) => response.map(axum::body::Body::new),
                        Err(_transport) => unreachable(),
                    }
                } else {
                    unreachable()
                };
                drop(reachable);
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
            reachable,
            task,
        }
    }

    /// Refuses every later RPC, as for an unreachable node. Returns once the
    /// RPCs it let through before have been answered, so none of them reaches
    /// a process started afterwards.
    async fn partition(&self) {
        *self.reachable.write().await = false;
    }

    async fn heal(&self) {
        *self.reachable.write().await = true;
    }
}

impl Drop for RaftProxy {
    fn drop(&mut self) {
        self.release.send_replace(true);
        self.task.abort();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cli_voter_replaced_with_an_empty_wal_heals_itself() {
    // A voter replaced on an empty WAL directory (its disk was lost) must not
    // re-run per-group Initialize for the groups it bootstraps (2 and 5 of 6
    // for node 3): it must never become eligible to vote with nothing applied.
    // The leaders see it lost the entries it had acknowledged and rewind it in
    // place: it stays a voter and catches up with no operator.
    let _guard = static_cluster_cli_test_guard().await;
    let Some(binary) = option_env!("CARGO_BIN_EXE_ursula") else {
        tracing::warn!("CARGO_BIN_EXE_ursula is not set; skipping restart self-heal test");
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
            spawn_per_group_node(binary, node_id, port, &peers, false, wal_root.path());
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
        .pin_nodes(&nodes, None)
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
        timeout: Duration::from_secs(15),
        poll_interval: Duration::from_millis(100),
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
    // Force the wiped replica to catch up through a snapshot. The proxy holds
    // that transfer until we have observed it gated while it is still a voter.
    let before_wipe = ctl.fetch_cluster(&nodes).await.unwrap();
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
    // The chart reuses its Pod-bound tunnel addresses. Preserve the same
    // native listening configuration too: shared binding permits only a boot
    // change, never refreshing a target URL alongside its incarnation.
    let replacement_config = children[2].config_path.clone().unwrap();
    let replacement_text = std::fs::read_to_string(&replacement_config).unwrap();
    drop(children.pop());
    let survivors = ursula_ctl::quorum::verify_surviving_quorum(&nodes, 3, &ctl, &options)
        .await
        .expect("with the drained source stopped, both survivors are eligible");
    assert!(survivors.verification.maintenance_executor_certified);
    assert!(!survivors.full_redundancy_restored);
    // The replacement comes back on an empty WAL: the source's disk is lost.
    snapshot_proxy.release.send_replace(false);
    remove_test_path(node_wal_dir(wal_root.path(), 3));
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

    tokio::time::timeout(
        Duration::from_secs(30),
        snapshot_proxy.blocked.wait_for(|blocked| *blocked),
    )
    .await
    .expect("wiped voter must request the compacted snapshot")
    .unwrap();

    // Observe the actual empty-WAL recovery before allowing another maintenance
    // operation: the wiped node stays a voter of every group, its gates stay
    // closed while it cannot catch up, and the survivors serve.
    let mut readiness_nodes = nodes.clone();
    for node in &mut readiness_nodes {
        node.expected_process_incarnation = None;
        node.expected_maintenance_fence = None;
    }
    let readiness_client = ursula_ctl::MetricsClient::new(Duration::from_secs(5)).unwrap();
    let began = std::time::Instant::now();
    loop {
        let snapshot = readiness_client
            .fetch_cluster(&readiness_nodes)
            .await
            .unwrap();
        let mut serving = Vec::new();
        for node in &readiness_nodes {
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
                )
            ));
            // The gated replica's own report blocks another maintenance.
            assert!(matches!(
                ursula_ctl::observe::check_maintenance_snapshot(&snapshot, 3, 6),
                Err(ursula_ctl::observe::ReadinessRefusal::Maintenance { .. })
            ));
            let refusal = ursula_ctl::wait_ready(
                &readiness_client,
                &readiness_nodes,
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
        .pin_nodes(&nodes, Some(3))
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
            b"executor-fence-tail",
        )
        .await;
    }

    let proof = ursula_ctl::quorum::verify_quorum(&nodes, &ctl, &options)
        .await
        .expect("current process full prefix proof");
    assert!(proof.maintenance_executor_certified);
    assert_eq!(proof.process_incarnations.len(), nodes.len());
    for node in &nodes {
        assert_eq!(
            proof.process_incarnations.get(&node.id),
            node.expected_process_incarnation.as_ref()
        );
    }
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
    // The observation's completion time is read before the request's now, so
    // it can never be in the request's future.
    let retired = observation(started_ms, proof);
    reservation = reservation
        .progress(
            ursula_ctl::reservation::ProgressRequest::CompletePodReplacement {
                fence: executor,
                now_ms: now_ms(),
                observation: retired,
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

/// Each survivor's membership log index, by node and group.
async fn survivor_membership_indexes(
    ctl: &ursula_ctl::MetricsClient,
    survivors: &[ursula_ctl::NodeInfo],
) -> std::collections::BTreeMap<(u64, u64), Option<u64>> {
    ctl.fetch_cluster(survivors)
        .await
        .expect("fetch the survivors")
        .per_node
        .iter()
        .flat_map(|view| {
            view.groups.iter().map(move |group| {
                (
                    (view.node.id, group.raft_group_id),
                    group.maintenance.membership_log_index,
                )
            })
        })
        .collect()
}

fn native_host_inventory(
    nodes: &[ursula_ctl::NodeInfo],
    observation: ursula_ctl::reservation::PrefixObservation,
) -> ursula_ctl::reservation::PublishHostInventory {
    ursula_ctl::reservation::PublishHostInventory {
        now_ms: native_epoch_ms(), process_plan:nodes.to_vec(), observation,
        pods:(1_u64..=3).map(|id|serde_json::json!({"kind":"Pod","metadata":{
            "namespace":"native","name":format!("voters-{}",id.checked_sub(1).unwrap()),
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
async fn cli_host_recovery_keeps_ack_tail_across_abrupt_voter_loss() {
    run_cli_host_recovery(CandidateFate::Recovers).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cli_host_recovery_restages_a_bound_candidate_without_losing_acks() {
    run_cli_host_recovery(CandidateFate::DiesUnreached).await;
}

/// #698: the candidate dies after its group leaders saw its conflict and
/// rewound it. It stays a voter of every group, so the survivors still
/// prove the candidate's loss and the replacement is restaged.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cli_host_recovery_restages_a_candidate_that_dies_after_its_leaders_rewound_it() {
    run_cli_host_recovery(CandidateFate::DiesAfterItsConflict).await;
}

/// What happens to the bound replacement candidate of a host recovery.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CandidateFate {
    /// It catches up and the replacement completes.
    Recovers,
    /// It is killed before its group leaders reach it.
    DiesUnreached,
    /// It is killed once every group's leader saw its conflict, rewound it
    /// and replicated to it.
    DiesAfterItsConflict,
}

async fn run_cli_host_recovery(candidate_fate: CandidateFate) {
    use ursula_ctl::reservation::HostRequest;
    use ursula_ctl::reservation::HostTerminationObservation;
    use ursula_ctl::reservation::OwnershipRequest;
    use ursula_ctl::reservation::PrefixObservation;
    use ursula_ctl::reservation::SurvivingPrefixObservation;
    let _guard = static_cluster_cli_test_guard().await;
    let binary = env!("CARGO_BIN_EXE_ursula");
    let ports = [free_port(), free_port(), free_port()];
    let public = |id: u64| format!("http://127.0.0.1:{}", node_port(&ports, id));
    // Node 3's peers reach its Raft endpoint only through this proxy, so the
    // test decides whether its group leaders can contact a candidate.
    let node_3_raft = RaftProxy::start(&public(3)).await;
    let peers = (1..=3)
        .map(|id| {
            (
                id,
                if id == 3 {
                    node_3_raft.url.clone()
                } else {
                    public(id)
                },
            )
        })
        .collect::<Vec<_>>();
    let wal_root = tempfile::tempdir().expect("WAL root");
    let mut children = Vec::new();
    let mut nodes = Vec::new();
    for id in 1..=3 {
        let (child, admin) = spawn_per_group_node(
            binary,
            id,
            node_port(&ports, id),
            &peers,
            false,
            wal_root.path(),
        );
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
    nodes = ctl.pin_nodes(&nodes, None).await.unwrap();
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
        timeout: Duration::from_secs(15),
        poll_interval: Duration::from_millis(100),
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
    let deadline = std::time::Instant::now()
        .checked_add(Duration::from_secs(60))
        .unwrap();
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
    // A candidate that dies first: its leaders must not reach it until it has
    // been killed.
    if candidate_fate == CandidateFate::DiesUnreached {
        node_3_raft.partition().await;
    }
    // The survivors' memberships before the candidate starts: no heal step
    // changes them while it catches up (#698).
    let survivors_before = nodes
        .iter()
        .filter(|node| node.id != 3)
        .cloned()
        .collect::<Vec<_>>();
    let memberships_before = survivor_membership_indexes(&ctl, &survivors_before).await;
    // The replacement host starts with an empty WAL.
    remove_test_path(node_wal_dir(wal_root.path(), 3));
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
    ctl.fetch_node(&nodes[2])
        .await
        .expect_err("a restarted node must not match its pinned process incarnation");
    // Each client pins fetched boot identities for its lifetime. The durable
    // reservation, rather than the old transport cache, admits this new boot.
    let ctl = ursula_ctl::MetricsClient::new(Duration::from_secs(5)).unwrap();
    nodes = ctl.pin_nodes(&nodes, Some(3)).await.unwrap();
    state=state.recover_host(HostRequest::BindHostReplacement {fence:state.operation().unwrap().fence.clone(),process_plan:nodes.clone(),
        pod:serde_json::json!({"kind":"Pod","metadata":{"namespace":"native","name":"voters-2","uid":"native-host-replacement","ownerReferences":[{"kind":"StatefulSet","uid":"native-sts","controller":true}]},"spec":{"nodeName":"native-host-3-replacement"}}),
        node:serde_json::json!({"kind":"Node","metadata":{"name":"native-host-3-replacement","uid":"native-node-3-replacement","labels":{"topology.kubernetes.io/zone":"native-zone-3"}},"spec":{"providerID":"native-instance-3-replacement"},"status":{"conditions":[{"type":"Ready","status":"True"}]}})
    }).unwrap();
    for node in &nodes {
        ctl.set_maintenance_fence(node, false).await.unwrap();
    }
    let mut last_sequence = 1;
    let mut last_payload = "during-recovery";
    let mut expected_payload = b"before-faultduring-recovery".to_vec();
    let ctl = if candidate_fate == CandidateFate::Recovers {
        ctl
    } else {
        if candidate_fate == CandidateFate::DiesAfterItsConflict {
            // The candidate's empty log conflicts below what its leaders had
            // matched of the node it replaces. A leader sends it entries only
            // once it rewound it, and it stays a voter meanwhile: no
            // survivor's membership changes until the kill.
            let survivors = nodes
                .iter()
                .filter(|node| node.id != 3)
                .cloned()
                .collect::<Vec<_>>();
            let deadline = tokio::time::Instant::now()
                .checked_add(Duration::from_secs(60))
                .unwrap();
            loop {
                assert_eq!(
                    survivor_membership_indexes(&ctl, &survivors).await,
                    memberships_before,
                    "a survivor changed its membership while the candidate caught up"
                );
                let rewound = ctl.fetch_node(&nodes[2]).await.is_ok_and(|view| {
                    (0..6_u64).all(|group_id| {
                        view.group(group_id)
                            .is_some_and(|group| group.last_log_index.is_some())
                    })
                });
                if rewound {
                    break;
                }
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "the candidate's leaders never replicated to it: {:?}",
                    ctl.try_fetch_cluster(&nodes).await
                );
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            assert_eq!(
                survivor_membership_indexes(&ctl, &survivors).await,
                memberships_before,
                "a survivor changed its membership while the candidate caught up"
            );
        }
        let candidate = state.operation().unwrap().replacement.clone().unwrap();
        let config = children[2].config_path.clone().unwrap();
        let text = std::fs::read_to_string(&config).unwrap();
        // A second real SIGKILL, in the same unrecovered voter slot, after
        // persistent binding. Never clear that binding merely on timeout.
        children[2].child.kill().unwrap();
        children[2].child.wait().unwrap();
        node_3_raft.heal().await;
        let deadline = tokio::time::Instant::now()
            .checked_add(Duration::from_secs(60))
            .unwrap();
        let survivor_observation = loop {
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
                    if tokio::time::Instant::now() >= deadline {
                        let survivors = nodes
                            .iter()
                            .filter(|node| node.id != 3)
                            .cloned()
                            .collect::<Vec<_>>();
                        let evidence = ctl.try_fetch_cluster(&survivors).await;
                        panic!("candidate-loss proof: {error:#}; survivor evidence: {evidence:?}");
                    }
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
        };
        state = state
            .recover_host(HostRequest::AdmitReplacementTermination {
                fence: state.operation().unwrap().fence.clone(),
                candidate: candidate.clone(),
                now_ms: native_epoch_ms(),
                observation: survivor_observation,
            })
            .unwrap();
        state
            .recover_host(HostRequest::RestageHostReplacement {
                fence: state.operation().unwrap().fence.clone(),
                candidate: candidate.clone(),
                now_ms: native_epoch_ms(),
            })
            .expect_err("restaging must be refused once the replacement's termination is admitted");
        let started_ms = native_epoch_ms();
        assert!(children[2].child.try_wait().unwrap().is_some());
        state = state
            .recover_host(HostRequest::RecordReplacementTermination {
                fence: state.operation().unwrap().fence.clone(),
                candidate: candidate.clone(),
                observation: HostTerminationObservation {
                    started_ms,
                    completed_ms: native_epoch_ms(),
                    provider_instance: candidate.provider_instance.clone(),
                    terminal_state: "terminated".into(),
                },
                // Read after the observation, so its completion time can never
                // be in the request's future.
                now_ms: native_epoch_ms(),
            })
            .unwrap();
        state = state
            .recover_host(HostRequest::RestageHostReplacement {
                fence: state.operation().unwrap().fence.clone(),
                candidate,
                now_ms: native_epoch_ms(),
            })
            .unwrap();
        assert!(state.operation().unwrap().replacement.is_none());
        assert!(
            state
                .operation()
                .unwrap()
                .host
                .as_ref()
                .unwrap()
                .pod_retirement_intents
                .contains("native-host-replacement")
        );
        // Both original and post-fault ACKs remain on the same two survivors.
        last_sequence = 2;
        last_payload = "after-candidate-loss";
        expected_payload.extend_from_slice(last_payload.as_bytes());
        for (index, offset) in offsets.iter_mut().enumerate() {
            let previous: u64 = offset.parse().unwrap();
            *offset = append_idempotent_until_acked(
                &client,
                &format!("{}/benchcmp/host-recovery-{index}", public(1)),
                last_sequence,
                last_payload,
            )
            .await;
            assert_eq!(
                offset.parse::<u64>().unwrap(),
                previous
                    .checked_add(u64::try_from(last_payload.len()).unwrap())
                    .unwrap()
            );
        }
        drop(children.pop());
        // The restaged replacement starts on another empty WAL.
        remove_test_path(node_wal_dir(wal_root.path(), 3));
        std::fs::write(&config, text).unwrap();
        let mut command = Command::new(binary);
        command
            .arg("server")
            .arg("--config")
            .arg(&config)
            .env("URSULA_START_MAINTENANCE_DRAINED", "true");
        let mut child = spawn_child(command, format!("host-restaged-{}", ports[2]));
        child.config_path = Some(config);
        children.push(child);
        wait_until_ready(&client, &public(3), &mut children).await;
        let ctl = ursula_ctl::MetricsClient::new(Duration::from_secs(5)).unwrap();
        nodes = ctl
            .pin_nodes(&state.operation().unwrap().process_plan, Some(3))
            .await
            .unwrap();
        state = state.recover_host(HostRequest::BindHostReplacement { fence: state.operation().unwrap().fence.clone(), process_plan: nodes.clone(),
            pod: serde_json::json!({"kind":"Pod","metadata":{"namespace":"native","name":"voters-2","uid":"native-host-restaged","ownerReferences":[{"kind":"StatefulSet","uid":"native-sts","controller":true}]},"spec":{"nodeName":"native-host-3-restaged"}}),
            node: serde_json::json!({"kind":"Node","metadata":{"name":"native-host-3-restaged","uid":"native-node-3-restaged","labels":{"topology.kubernetes.io/zone":"native-zone-3"}},"spec":{"providerID":"native-instance-3-restaged"},"status":{"conditions":[{"type":"Ready","status":"True"}]}})
        }).unwrap();
        for node in &nodes {
            ctl.set_maintenance_fence(node, false).await.unwrap();
        }
        ctl
    };
    // The leaders catch the replacement up by themselves; the operator waits
    // for it and lifts its startup drain.
    let outcome =
        ursula_ctl::wait_node_ready(&nodes, &nodes[2], &ctl, &ursula_ctl::CatchUpOptions {
            stall_timeout: Duration::from_secs(30),
            ready_timeout: Duration::from_secs(60),
            poll_interval: Duration::from_millis(100),
            lag_tolerance: 16,
        })
        .await
        .unwrap();
    assert!(
        matches!(outcome, ursula_ctl::CatchUpOutcome::Ready),
        "{outcome:?}"
    );
    ursula_ctl::undrain_node(&ctl, &nodes[2]).await.unwrap();
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
            append_idempotent_until_acked(&client, &url, last_sequence, last_payload).await,
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
                &expected_payload,
            )
            .await;
        }
    }
    for node in &nodes {
        ctl.set_maintenance_fence(node, true).await.unwrap();
    }
    let completion_deadline = tokio::time::Instant::now()
        .checked_add(Duration::from_secs(60))
        .unwrap();
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
        if candidate_fate == CandidateFate::Recovers {
            "native-instance-3-replacement"
        } else {
            "native-instance-3-restaged"
        }
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
        expected_maintenance_fence: None,
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
init_membership = {init_membership}
init_membership_per_group = false
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
    init_membership: bool,
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
init_membership = {init_membership}
init_membership_per_group = false

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

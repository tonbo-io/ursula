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
async fn cli_wait_ready_uses_the_chart_generated_admin_tunnel_through_startup() {
    use ursula_ctl::provider::NodeProvider;
    let _guard = static_cluster_cli_test_guard().await;
    let root = tempfile::tempdir().unwrap();
    let port = free_port();
    let base = format!("http://127.0.0.1:{port}");
    let config = root.path().join("server.toml");
    let admin_port = write_single_node_cluster_config(
        &config,
        port,
        1,
        1,
        &base,
        true,
        &root.path().join("wal"),
    );
    let manifest = root.path().join("cluster.json");
    // Execute the production function embedded in Helm's rollout ConfigMap,
    // rather than duplicating its manifest or stubbing strict_verify.
    let script =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../../charts/ursula/files/graceful-rollout.sh");
    let generated = Command::new("sh")
        .args([
            "-c",
            ". \"$1\"; MANIFEST=\"$2\"; write_manifest",
            "chart-manifest",
        ])
        .arg(script)
        .arg(&manifest)
        .env("ROLLOUT_SOURCE_ONLY", "1")
        .env("NAMESPACE", "readiness-test")
        .env("STATEFULSET", "ursula")
        .env("REPLICAS", "1")
        .env("EXPECTED_GROUPS", "1")
        .env("TARGET_IMAGE", "local-readiness-test")
        // The node shares one port between its client and cluster planes.
        .env("CLUSTER_PORT", port.to_string())
        .env("FORWARD_PORT_BASE", admin_port.to_string())
        .output()
        .unwrap();
    assert!(
        generated.status.success(),
        "{}",
        String::from_utf8_lossy(&generated.stderr)
    );
    let provider = ursula_ctl::provider::StaticNodeProvider::from_path(&manifest).unwrap();
    let nodes = provider.list_nodes().await.unwrap();
    assert_eq!(nodes.len(), 1);
    let node = &nodes[0];
    assert_eq!(
        node.http_url.as_ref().unwrap().host_str(),
        Some("ursula-0.ursula-headless.readiness-test.svc.cluster.local")
    );
    assert_eq!(node.http_url.as_ref().unwrap().port(), Some(port));
    assert_eq!(node.metrics_url.as_ref().unwrap().port(), Some(admin_port));
    assert_eq!(node.admin_url.port(), Some(admin_port));
    assert_ne!(port, admin_port);
    // The cluster DNS name of http_url does not resolve here. Like the hook,
    // wait-ready must observe metrics and readiness through the admin tunnel.
    let ctl = ursula_ctl::MetricsClient::new(Duration::from_secs(1)).unwrap();
    let wait = ursula_ctl::observe::wait_ready(
        &ctl,
        &nodes,
        1,
        Duration::from_secs(30),
        Duration::from_millis(25),
    );
    let start_server = async {
        tokio::time::sleep(Duration::from_millis(250)).await;
        let mut command = Command::new(env!("CARGO_BIN_EXE_ursula"));
        command.arg("server").arg("--config").arg(&config);
        spawn_child(command, "chart-readiness".to_owned())
    };
    let (result, child) = tokio::join!(wait, start_server);
    assert_eq!(
        result
            .unwrap_or_else(|error| panic!("wait-ready: {error:#}; {}", child_report(&child)))
            .per_node
            .len(),
        1
    );
    for plane in [port, admin_port] {
        let response = reqwest::get(format!("http://127.0.0.1:{plane}/__ursula/ready"))
            .await
            .unwrap();
        assert_eq!(response.status(), reqwest::StatusCode::OK);
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

/// A node whose groups recorded its Raft url at their initialization refuses
/// to start once that url moved to a new cluster port, since its peers would
/// keep dialing the recorded one.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cli_refuses_a_cluster_port_moved_after_initialization() {
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
    put_until_created(&client, &format!("{base}/moved/port")).await;
    sigterm_and_wait_for_clean_exit(&mut children[0]).await;
    drop(children);

    let cluster_port = free_port();
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
            &format!("url = \"http://127.0.0.1:{cluster_port}\"\nclient_url = \"{base}\""),
        );
    std::fs::write(&config, split).expect("write split-plane config");
    let mut command = Command::new(binary);
    command.arg("server").arg("--config").arg(&config);
    let mut moved = spawn_child(command, "moved-port".to_owned());
    let deadline = std::time::Instant::now()
        .checked_add(Duration::from_secs(60))
        .unwrap();
    let status = loop {
        if let Some(status) = moved.child.try_wait().expect("poll child") {
            break status;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "a moved cluster port must refuse to start: {}",
            child_report(&moved)
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    assert!(!status.success(), "{}", child_report(&moved));
    let stderr = child_stderr_tail(&moved);
    assert!(
        stderr.contains(&format!("records node 1 at {base}"))
            && stderr.contains("cannot change after its groups are initialized"),
        "{stderr}"
    );
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
    let leader_id = wait_group_leader(&client, &peers[0].1, &mut children).await;
    let leader = peers
        .iter()
        .position(|(node_id, _)| *node_id == leader_id)
        .unwrap();
    assert!(leader < 2, "node 3 cannot lead: {leader_id}");
    let (_, status, leader_ready) =
        wait_any_peer_cluster_mismatch(&client, &peers[leader..=leader], &mut children, |ready| {
            ready.peer_cluster_mismatches.contains(&mismatch(3, OTHER))
        })
        .await;
    assert_eq!(leader_ready.cluster_id, cluster(CLUSTER));
    let (_, other_status, other_ready) =
        wait_any_peer_cluster_mismatch(&client, &peers[2..], &mut children, |ready| {
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
        wait_any_peer_cluster_mismatch(&client, &peers[leader..=leader], &mut children, |ready| {
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

/// The leader of group 0 as `base_url`'s metrics report it, once known.
async fn wait_group_leader(
    client: &reqwest::Client,
    base_url: &str,
    children: &mut [ChildGuard],
) -> u64 {
    let deadline = std::time::Instant::now()
        .checked_add(Duration::from_secs(30))
        .unwrap();
    loop {
        if let Some(report) = exited_child_report(children) {
            panic!("a node exited while waiting for a leader:\n{report}");
        }
        let metrics: ursula_proto::admin::NodeMetrics = client
            .get(format!("{base_url}/__ursula/metrics"))
            .send()
            .await
            .expect("metrics")
            .json()
            .await
            .expect("metrics JSON");
        if let Some(leader) = metrics
            .groups
            .iter()
            .find(|group| group.raft_group_id == 0)
            .and_then(|group| group.current_leader)
        {
            return leader;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "group 0 has no leader: {metrics:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Polls the readiness of `nodes` until one's answer satisfies `done`, and
/// returns its index in `nodes` and its answer.
async fn wait_any_peer_cluster_mismatch(
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
            panic!("a node exited while waiting for a peer cluster mismatch:\n{report}");
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
            "no node reported the peer cluster mismatch: {answers:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
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
    let initial_peers = peers[..2].to_vec();
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

    let purge = admin_purge(
        &client,
        format!("{node1_admin}/__ursula/raft/0/purge?upto={snapshot_index}"),
    )
    .await;
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

    drop(children);
    std::fs::remove_dir_all(&root).expect("remove temp root");
}

/// The S3 snapshot objects of group 0 under the snapshot prefix that hold
/// [`DELETED_MARKER`], read through `cold_store`, whose root is the
/// snapshot store's.
async fn snapshot_objects_with_marker(cold_store: &ColdStore) -> Vec<String> {
    let dir = "snapshots/group-0/objects/";
    let mut found = Vec::new();
    for name in cold_store
        .list_file_names(dir)
        .await
        .expect("list the snapshot objects")
    {
        if !name.ends_with(".snap") {
            continue;
        }
        let path = format!("{dir}{name}");
        // Another replica's GC may delete it between the listing and here.
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
/// group that held it, the objects each replica replaced included. The data
/// stays hot, so only the snapshots hold it. Runs on real S3 in CI.
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
    let max_age = Duration::from_secs(90);
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
                "init_membership_per_group = false\ndeleted_data_max_age = \"90s\"\n\
                 install_snapshot_timeout = \"10s\"\n",
            )
            .replace("flush_interval = \"1s\"\n", "flush_interval = \"1h\"\n"),
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
    // goes, and every voter has published its reference.
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

    // GC never deleted what a replica still names: the object each
    // replica's reference names exists.
    for (node_id, _) in &peers {
        let reference_path = format!("snapshots/group-0/references/node-{node_id}.json");
        let size = cold_store
            .object_size(&reference_path)
            .await
            .unwrap_or_else(|err| panic!("node {node_id} has no snapshot reference: {err}"));
        let bytes = cold_store
            .read_whole_object_range(&reference_path, size, 0, usize::try_from(size).unwrap())
            .await
            .expect("read a snapshot reference");
        let reference: serde_json::Value =
            serde_json::from_slice(&bytes).expect("a snapshot reference record");
        let key = reference["snapshot_key"]
            .as_str()
            .unwrap_or_else(|| panic!("node {node_id} names no S3 snapshot: {reference}"));
        cold_store
            .object_size(key)
            .await
            .unwrap_or_else(|err| panic!("node {node_id}'s current snapshot {key} is gone: {err}"));
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

/// Preserve HTTP/2 bodies and trailers while delaying only snapshot transfer.
/// Metrics and readiness use the server's real listeners directly.
struct SnapshotProxy {
    url: String,
    release: tokio::sync::watch::Sender<bool>,
    blocked: tokio::sync::watch::Receiver<bool>,
    task: tokio::task::JoinHandle<()>,
}

impl SnapshotProxy {
    async fn start(target: &str) -> Self {
        use tower::ServiceExt;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
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
                let response = channel.oneshot(request.map(tonic::body::boxed)).await;
                Ok::<_, std::convert::Infallible>(match response {
                    Ok(response) => response.map(axum::body::Body::new),
                    Err(_transport) => axum::http::Response::builder()
                        .status(axum::http::StatusCode::BAD_GATEWAY)
                        .body(axum::body::Body::empty())
                        .unwrap(),
                })
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

impl Drop for SnapshotProxy {
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
    // The leaders see it lost the entries it had acknowledged and rebuild it
    // through remove, learner, catch-up and promote with no operator.
    let _guard = static_cluster_cli_test_guard().await;
    let Some(binary) = option_env!("CARGO_BIN_EXE_ursula") else {
        tracing::warn!("CARGO_BIN_EXE_ursula is not set; skipping restart self-heal test");
        return;
    };
    let ports = [free_port(), free_port(), free_port()];
    let public = |node_id: u64| format!("http://127.0.0.1:{}", node_port(&ports, node_id));
    let mut snapshot_proxy = SnapshotProxy::start(&public(3)).await;
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
    // that transfer until we have observed the real automatic learner phase.
    // Membership changes still come exclusively from the production heal driver.
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
    // operation. This exercises startup gates and learner catch-up, not a live demotion.
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
        let target_report = snapshot.node(3).unwrap().raft_maintenance.as_ref().unwrap();
        let target_is_learner =
            target_report.group_issues.values().flatten().any(|issue| {
                *issue == ursula_proto::admin::RaftMaintenanceIssue::LocalReplicaNotVoter
            });
        if target_is_learner
            && snapshot.per_node.iter().all(|view| {
                view.raft_maintenance
                    .as_ref()
                    .is_some_and(|report| !report.ready())
            })
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
            for view in &snapshot.per_node {
                let local = ursula_ctl::metrics::ClusterSnapshot {
                    per_node: vec![view.clone()],
                };
                assert!(matches!(
                    ursula_ctl::observe::check_maintenance_snapshot(&local, 1, 6),
                    Err(ursula_ctl::observe::ReadinessRefusal::Maintenance { .. })
                ));
            }
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
            "did not observe gated empty-WAL learner with serving survivors: {snapshot:?}, {serving:?}"
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

    // `wait` sees the same rebuilt voter.
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
    run_cli_host_recovery(false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cli_host_recovery_restages_a_bound_candidate_without_losing_acks() {
    run_cli_host_recovery(true).await;
}

async fn run_cli_host_recovery(interrupt_candidate: bool) {
    use ursula_ctl::reservation::HostRequest;
    use ursula_ctl::reservation::HostTerminationObservation;
    use ursula_ctl::reservation::OwnershipRequest;
    use ursula_ctl::reservation::PrefixObservation;
    use ursula_ctl::reservation::SurvivingPrefixObservation;
    let _guard = static_cluster_cli_test_guard().await;
    let binary = env!("CARGO_BIN_EXE_ursula");
    let ports = [free_port(), free_port(), free_port()];
    let public = |id: u64| format!("http://127.0.0.1:{}", node_port(&ports, id));
    let peers = (1..=3).map(|id| (id, public(id))).collect::<Vec<_>>();
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
    let ctl = if interrupt_candidate {
        let candidate = state.operation().unwrap().replacement.clone().unwrap();
        let config = children[2].config_path.clone().unwrap();
        let text = std::fs::read_to_string(&config).unwrap();
        // A second real SIGKILL, in the same unrecovered voter slot, after
        // persistent binding. Never clear that binding merely on timeout.
        children[2].child.kill().unwrap();
        children[2].child.wait().unwrap();
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
                now_ms: native_epoch_ms(),
                observation: HostTerminationObservation {
                    started_ms,
                    completed_ms: native_epoch_ms(),
                    provider_instance: candidate.provider_instance.clone(),
                    terminal_state: "terminated".into(),
                },
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
    } else {
        ctl
    };
    // The leaders rebuild the replacement by themselves; the operator waits
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
        if interrupt_candidate {
            "native-instance-3-restaged"
        } else {
            "native-instance-3-replacement"
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
    let admin_port = free_port();
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

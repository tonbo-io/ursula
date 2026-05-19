use std::net::TcpListener;
use std::path::Path;
use std::process::{Child, Command, Stdio};
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
    let Some(binary) = option_env!("CARGO_BIN_EXE_ursula-http") else {
        eprintln!("CARGO_BIN_EXE_ursula-http is not set; skipping CLI cluster smoke test");
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
    let Some(binary) = option_env!("CARGO_BIN_EXE_ursula-http") else {
        eprintln!("CARGO_BIN_EXE_ursula-http is not set; skipping CLI durable restart smoke test");
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
async fn cli_static_grpc_raft_log_dir_recovers_cold_manifest_after_restart() {
    let Some(binary) = option_env!("CARGO_BIN_EXE_ursula-http") else {
        eprintln!(
            "CARGO_BIN_EXE_ursula-http is not set; skipping CLI durable cold restart smoke test"
        );
        return;
    };
    let port = free_port();
    let base_url = format!("http://127.0.0.1:{port}");
    let root = std::env::temp_dir().join(format!(
        "ursula-cli-durable-cold-restart-{}-{}",
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
    let cold_root = root.join("cold");

    write_single_node_cluster_config(&config_path, &base_url, true);
    {
        let _child = spawn_node_with_cluster_config_and_cold_fs(
            binary,
            port,
            1,
            &config_path,
            &log_dir,
            &cold_root,
        );
        let client = reqwest::Client::new();
        wait_until_ready(&client, &base_url).await;
        put_until_created(&client, &format!("{base_url}/benchcmp/cli-cold-restart")).await;
        post_until_no_content(
            &client,
            &format!("{base_url}/benchcmp/cli-cold-restart"),
            "cli-cold-restart-payload",
        )
        .await;
        flush_stream_until_cold_hot_bytes_zero(&client, &base_url, "benchcmp", "cli-cold-restart")
            .await;
        assert!(
            contains_regular_file(&cold_root),
            "cold fs root should contain uploaded chunk before restart"
        );
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
        let _child = spawn_node_with_cluster_config_and_cold_fs(
            binary,
            port,
            1,
            &config_path,
            &log_dir,
            &cold_root,
        );
        let client = reqwest::Client::new();
        wait_until_ready(&client, &base_url).await;
        let payload = read_until_replicated(
            &client,
            &format!("{base_url}/benchcmp/cli-cold-restart?offset=0&max_bytes=64"),
        )
        .await;
        assert_eq!(payload, b"cli-cold-restart-payload");
    }

    std::fs::remove_dir_all(&root).expect("remove temp root");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cli_static_grpc_raft_log_dir_replicates_between_nodes() {
    let Some(binary) = option_env!("CARGO_BIN_EXE_ursula-http") else {
        eprintln!("CARGO_BIN_EXE_ursula-http is not set; skipping CLI durable cluster smoke test");
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

    for node_id in 1..=3 {
        let journal_path = root
            .join(format!("node-{node_id}-log"))
            .join("core-0")
            .join("journal.bin");
        assert!(journal_path.exists(), "node {node_id} journal should exist");
        assert!(
            std::fs::metadata(&journal_path)
                .expect("node journal metadata")
                .len()
                > 0,
            "node {node_id} journal should contain records"
        );
    }

    drop(children);
    std::fs::remove_dir_all(&root).expect("remove temp root");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cli_static_grpc_raft_log_dir_installs_snapshot_for_late_learner() {
    let Some(binary) = option_env!("CARGO_BIN_EXE_ursula-http") else {
        eprintln!(
            "CARGO_BIN_EXE_ursula-http is not set; skipping CLI durable late learner smoke test"
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
async fn cli_static_grpc_raft_log_dir_replicates_cold_manifest() {
    let Some(binary) = option_env!("CARGO_BIN_EXE_ursula-http") else {
        eprintln!("CARGO_BIN_EXE_ursula-http is not set; skipping CLI durable cold smoke test");
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
        "ursula-cli-durable-cold-cluster-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system time after unix epoch")
            .as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).expect("create temp root");
    let cold_root = root.join("cold");

    let mut configs = Vec::new();
    for (node_id, _) in &peers {
        let config_path = root.join(format!("node-{node_id}.json"));
        write_cluster_config(&config_path, *node_id, &peers, *node_id == 1);
        configs.push(config_path);
    }

    let children = vec![
        spawn_node_with_cluster_config_and_cold_fs(
            binary,
            ports[1],
            1,
            &configs[1],
            &root.join("node-2-log"),
            &cold_root,
        ),
        spawn_node_with_cluster_config_and_cold_fs(
            binary,
            ports[2],
            1,
            &configs[2],
            &root.join("node-3-log"),
            &cold_root,
        ),
        spawn_node_with_cluster_config_and_cold_fs(
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
        &format!("{}/benchcmp/cli-durable-cold", peers[0].1),
    )
    .await;
    post_until_no_content(
        &client,
        &format!("{}/benchcmp/cli-durable-cold", peers[0].1),
        "cli-durable-cold-payload",
    )
    .await;
    flush_stream_until_cold_hot_bytes_zero(&client, &peers[0].1, "benchcmp", "cli-durable-cold")
        .await;

    let payload = read_until_replicated(
        &client,
        &format!(
            "{}/benchcmp/cli-durable-cold?offset=0&max_bytes=64",
            peers[2].1
        ),
    )
    .await;
    assert_eq!(payload, b"cli-durable-cold-payload");

    let metrics = client
        .get(format!("{}/__ursula/metrics", peers[0].1))
        .send()
        .await
        .expect("read metrics")
        .text()
        .await
        .expect("metrics body");
    assert!(
        !metrics.contains("\"cold_flush_publishes\":0"),
        "leader should report cold publishes: {metrics}"
    );
    assert!(
        contains_regular_file(&cold_root),
        "cold fs root should contain uploaded chunk"
    );
    for node_id in 1..=3 {
        let journal_path = root
            .join(format!("node-{node_id}-log"))
            .join("core-0")
            .join("journal.bin");
        assert!(journal_path.exists(), "node {node_id} journal should exist");
        assert!(
            std::fs::metadata(&journal_path)
                .expect("node journal metadata")
                .len()
                > 0,
            "node {node_id} journal should contain records"
        );
    }

    drop(children);
    std::fs::remove_dir_all(&root).expect("remove temp root");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cli_static_grpc_raft_log_dir_recovers_replicated_cold_manifest_after_restart() {
    let Some(binary) = option_env!("CARGO_BIN_EXE_ursula-http") else {
        eprintln!(
            "CARGO_BIN_EXE_ursula-http is not set; skipping CLI durable cold cluster restart smoke test"
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
    let root = std::env::temp_dir().join(format!(
        "ursula-cli-durable-cold-cluster-restart-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system time after unix epoch")
            .as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).expect("create temp root");
    let cold_root = root.join("cold");

    let mut configs = Vec::new();
    for (node_id, _) in &peers {
        let config_path = root.join(format!("node-{node_id}.json"));
        write_cluster_config(&config_path, *node_id, &peers, *node_id == 1);
        configs.push(config_path);
    }

    {
        let children = vec![
            spawn_node_with_cluster_config_and_cold_fs(
                binary,
                ports[1],
                1,
                &configs[1],
                &root.join("node-2-log"),
                &cold_root,
            ),
            spawn_node_with_cluster_config_and_cold_fs(
                binary,
                ports[2],
                1,
                &configs[2],
                &root.join("node-3-log"),
                &cold_root,
            ),
            spawn_node_with_cluster_config_and_cold_fs(
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
            &format!("{}/benchcmp/cli-durable-cold-restart", peers[0].1),
        )
        .await;
        post_until_no_content(
            &client,
            &format!("{}/benchcmp/cli-durable-cold-restart", peers[0].1),
            "cli-durable-cold-restart-payload",
        )
        .await;
        flush_stream_until_cold_hot_bytes_zero(
            &client,
            &peers[0].1,
            "benchcmp",
            "cli-durable-cold-restart",
        )
        .await;
        let payload = read_until_replicated(
            &client,
            &format!(
                "{}/benchcmp/cli-durable-cold-restart?offset=0&max_bytes=64",
                peers[2].1
            ),
        )
        .await;
        assert_eq!(payload, b"cli-durable-cold-restart-payload");
        drop(children);
    }

    for node_id in 1..=3 {
        let journal_path = root
            .join(format!("node-{node_id}-log"))
            .join("core-0")
            .join("journal.bin");
        assert!(journal_path.exists(), "node {node_id} journal should exist");
        assert!(
            std::fs::metadata(&journal_path)
                .expect("node journal metadata")
                .len()
                > 0,
            "node {node_id} journal should contain records"
        );
    }
    assert!(
        contains_regular_file(&cold_root),
        "cold fs root should contain uploaded chunk before restart"
    );

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
            spawn_node_with_cluster_config_and_cold_fs(
                binary,
                ports[1],
                1,
                &configs[1],
                &root.join("node-2-log"),
                &cold_root,
            ),
            spawn_node_with_cluster_config_and_cold_fs(
                binary,
                ports[2],
                1,
                &configs[2],
                &root.join("node-3-log"),
                &cold_root,
            ),
            spawn_node_with_cluster_config_and_cold_fs(
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
                "{}/benchcmp/cli-durable-cold-restart?offset=0&max_bytes=64",
                peers[2].1
            ),
        )
        .await;
        assert_eq!(payload, b"cli-durable-cold-restart-payload");
        drop(children);
    }

    std::fs::remove_dir_all(&root).expect("remove temp root");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cli_static_grpc_raft_log_dir_background_cold_flush_bounds_hot_bytes_during_writes() {
    let Some(binary) = option_env!("CARGO_BIN_EXE_ursula-http") else {
        eprintln!(
            "CARGO_BIN_EXE_ursula-http is not set; skipping CLI durable cold steady-state test"
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
    let root = std::env::temp_dir().join(format!(
        "ursula-cli-durable-cold-steady-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system time after unix epoch")
            .as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).expect("create temp root");
    let cold_root = root.join("cold");

    let mut configs = Vec::new();
    for (node_id, _) in &peers {
        let config_path = root.join(format!("node-{node_id}.json"));
        write_cluster_config(&config_path, *node_id, &peers, *node_id == 1);
        configs.push(config_path);
    }

    let chunk = vec![b'x'; 1024];
    let expected = chunk.repeat(48);
    {
        let children = vec![
            spawn_node_with_cluster_config_and_cold_fs_background(
                binary,
                ports[1],
                2,
                &configs[1],
                &root.join("node-2-log"),
                &cold_root,
            ),
            spawn_node_with_cluster_config_and_cold_fs_background(
                binary,
                ports[2],
                2,
                &configs[2],
                &root.join("node-3-log"),
                &cold_root,
            ),
            spawn_node_with_cluster_config_and_cold_fs_background(
                binary,
                ports[0],
                2,
                &configs[0],
                &root.join("node-1-log"),
                &cold_root,
            ),
        ];

        let client = reqwest::Client::new();
        for (_, base_url) in &peers {
            wait_until_ready(&client, base_url).await;
        }
        put_with_content_type_until_created(
            &client,
            &format!("{}/benchcmp/cli-durable-cold-steady", peers[0].1),
            "application/octet-stream",
        )
        .await;
        for _ in 0..48 {
            post_bytes_until_no_content(
                &client,
                &format!("{}/benchcmp/cli-durable-cold-steady", peers[0].1),
                chunk.clone(),
            )
            .await;
            wait_cold_hot_bytes_at_most(&client, &peers[0].1, 8 * 1024).await;
        }
        wait_cold_hot_bytes_at_most(&client, &peers[0].1, 0).await;
        assert!(
            contains_regular_file(&cold_root),
            "cold fs root should contain background-flushed chunks"
        );

        let payload = read_until_replicated(
            &client,
            &format!(
                "{}/benchcmp/cli-durable-cold-steady?offset=0&max_bytes={}",
                peers[2].1,
                expected.len()
            ),
        )
        .await;
        assert_eq!(payload, expected);
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
            spawn_node_with_cluster_config_and_cold_fs(
                binary,
                ports[1],
                2,
                &configs[1],
                &root.join("node-2-log"),
                &cold_root,
            ),
            spawn_node_with_cluster_config_and_cold_fs(
                binary,
                ports[2],
                2,
                &configs[2],
                &root.join("node-3-log"),
                &cold_root,
            ),
            spawn_node_with_cluster_config_and_cold_fs(
                binary,
                ports[0],
                2,
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
                "{}/benchcmp/cli-durable-cold-steady?offset=0&max_bytes={}",
                peers[2].1,
                expected.len()
            ),
        )
        .await;
        assert_eq!(payload, expected);
        drop(children);
    }

    std::fs::remove_dir_all(&root).expect("remove temp root");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cli_static_grpc_raft_log_dir_recovers_replicated_s3_cold_manifest_after_restart() {
    if std::env::var("URSULA_COLD_S3_INTEGRATION").ok().as_deref() != Some("1") {
        eprintln!(
            "skipping CLI S3 cold-manifest restart integration; set URSULA_COLD_S3_INTEGRATION=1 and URSULA_COLD_S3_BUCKET"
        );
        return;
    }
    let Some(binary) = option_env!("CARGO_BIN_EXE_ursula-http") else {
        eprintln!(
            "CARGO_BIN_EXE_ursula-http is not set; skipping CLI S3 cold cluster restart smoke test"
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
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind free port");
    listener.local_addr().expect("local addr").port()
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
        child: command.spawn().expect("spawn ursula-http node"),
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
        child: command.spawn().expect("spawn durable ursula-http node"),
    }
}

fn spawn_node_with_cluster_config_and_cold_fs(
    binary: &str,
    port: u16,
    raft_group_count: usize,
    config_path: &Path,
    log_dir: &Path,
    cold_root: &Path,
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
        .env("URSULA_COLD_BACKEND", "fs")
        .env("URSULA_COLD_ROOT", cold_root)
        .env("URSULA_COLD_FLUSH_INTERVAL_MS", "0")
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    ChildGuard {
        child: command
            .spawn()
            .expect("spawn durable cold ursula-http node"),
    }
}

fn spawn_node_with_cluster_config_and_cold_fs_background(
    binary: &str,
    port: u16,
    raft_group_count: usize,
    config_path: &Path,
    log_dir: &Path,
    cold_root: &Path,
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
        .env("URSULA_COLD_BACKEND", "fs")
        .env("URSULA_COLD_ROOT", cold_root)
        .env("URSULA_COLD_FLUSH_INTERVAL_MS", "10")
        .env("URSULA_COLD_FLUSH_MIN_HOT_BYTES", "1")
        .env("URSULA_COLD_FLUSH_MAX_BYTES", "4096")
        .env("URSULA_COLD_FLUSH_MAX_CONCURRENCY", "2")
        .env(
            "URSULA_COLD_MAX_HOT_BYTES_PER_GROUP",
            (16 * 1024).to_string(),
        )
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    ChildGuard {
        child: command
            .spawn()
            .expect("spawn durable background-cold ursula-http node"),
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
        child: command
            .spawn()
            .expect("spawn durable S3 cold ursula-http node"),
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
    for _ in 0..100 {
        if let Ok(response) = client
            .get(format!("{base_url}/__ursula/metrics"))
            .send()
            .await
            && response.status().is_success()
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("node {base_url} did not become ready");
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

async fn post_bytes_until_no_content(client: &reqwest::Client, url: &str, payload: Vec<u8>) {
    let mut last_status = None;
    let mut last_body = String::new();
    for _ in 0..100 {
        if let Ok(response) = client
            .post(url)
            .header("content-type", "application/octet-stream")
            .body(payload.clone())
            .send()
            .await
        {
            let status = response.status();
            if status == reqwest::StatusCode::NO_CONTENT {
                return;
            }
            last_status = Some(status);
            last_body = response.text().await.unwrap_or_default();
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("append bytes did not succeed at {url}: last_status={last_status:?} body={last_body}");
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

async fn wait_cold_hot_bytes_at_most(client: &reqwest::Client, base_url: &str, max_bytes: u64) {
    let mut last = String::new();
    for _ in 0..200 {
        if let Ok(response) = client
            .get(format!("{base_url}/__ursula/metrics"))
            .send()
            .await
            && response.status().is_success()
        {
            last = response.text().await.expect("metrics body");
            let metrics: serde_json::Value =
                serde_json::from_str(&last).expect("parse metrics json");
            if metrics
                .get("cold_hot_bytes")
                .and_then(serde_json::Value::as_u64)
                .is_some_and(|value| value <= max_bytes)
            {
                return;
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("cold_hot_bytes did not fall below {max_bytes}: {last}");
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

fn contains_regular_file(path: &Path) -> bool {
    let Ok(entries) = std::fs::read_dir(path) else {
        return false;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_file() {
            return true;
        }
        if path.is_dir() && contains_regular_file(&path) {
            return true;
        }
    }
    false
}

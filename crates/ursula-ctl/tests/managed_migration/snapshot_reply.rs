//! Hold an actual successful native snapshot response after the destination
//! has installed its pointer and published its S3 reference. Never synthesize
//! a Raft acknowledgement; callers can lose the genuine response at a crash.

use std::sync::Mutex;
use std::time::Duration;

use openraft::alias::SnapshotMetaOf;
use tokio::sync::Notify;
use ursula_raft::UrsulaRaftTypeConfig;
use ursula_shard::RaftGroupId;

use super::Cluster;

pub(super) type SnapshotMeta = SnapshotMetaOf<UrsulaRaftTypeConfig>;

struct Armed {
    group: u32,
    installed: Option<SnapshotMeta>,
}

#[derive(Default)]
pub(super) struct Gate {
    state: Mutex<Option<Armed>>,
    entered: Notify,
    released: Notify,
}

impl Gate {
    pub(super) fn arm(&self, group: u32) {
        *self.state.lock().unwrap() = Some(Armed {
            group,
            installed: None,
        });
    }

    pub(super) fn resume(&self) {
        *self.state.lock().unwrap() = None;
        self.released.notify_waiters();
    }

    pub(super) async fn hold_response(&self, group: u32, metadata: &[u8]) {
        {
            let mut state = self.state.lock().unwrap();
            let Some(armed) = state.as_mut().filter(|armed| armed.group == group) else {
                return;
            };
            if armed.installed.is_none() {
                let meta: SnapshotMeta = rmp_serde::from_slice(metadata).unwrap();
                assert!(meta.last_log_id.is_some(), "empty installed snapshot");
                armed.installed = Some(meta);
            }
            self.entered.notify_one();
        }
        loop {
            let released = self.released.notified();
            if !self
                .state
                .lock()
                .unwrap()
                .as_ref()
                .is_some_and(|armed| armed.group == group)
            {
                return;
            }
            released.await;
        }
    }

    pub(super) async fn boundary(&self) -> SnapshotMeta {
        tokio::time::timeout(Duration::from_secs(40), async {
            loop {
                let entered = self.entered.notified();
                if let Some(meta) = self
                    .state
                    .lock()
                    .unwrap()
                    .as_ref()
                    .and_then(|armed| armed.installed.clone())
                {
                    return meta;
                }
                entered.await;
            }
        })
        .await
        .expect("no successful native snapshot response was withheld")
    }
}

#[derive(serde::Deserialize)]
struct LocalSnapshot {
    meta: SnapshotMeta,
}

pub(super) async fn assert_installed(
    cluster: &Cluster,
    store: &opendal::Operator,
    group: u32,
    installed: &SnapshotMeta,
) {
    let prefix = installed.last_log_id.unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let metrics: serde_json::Value = cluster
            .client
            .get(format!("{}/__ursula/metrics", cluster.nodes[&7].admin_url))
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .await
            .unwrap();
        let applied = metrics["raft_groups"]
            .as_array()
            .unwrap()
            .iter()
            .find(|metrics| metrics["raft_group_id"] == group)
            .and_then(|metrics| metrics["last_applied_index"].as_u64());
        if applied.is_some_and(|index| index >= prefix.index) {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "installed snapshot not applied: {metrics}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let path = cluster.configs[&7]
        .raft
        .wal
        .resolved_path()
        .unwrap()
        .join("core-0")
        .join(format!("group-{group}.snapshot.json"));
    // Decode the real persisted binary envelope. Unknown pointer bytes are
    // skipped here; the external reference/object is checked independently.
    let saved: LocalSnapshot = rmp_serde::from_slice(&std::fs::read(path).unwrap()).unwrap();
    assert!(saved.meta.last_log_id >= Some(prefix));
    let bytes = store
        .read(&format!("snapshots/group-{group}/references/node-7.json"))
        .await
        .unwrap();
    let reference: serde_json::Value = serde_json::from_slice(&bytes.to_vec()).unwrap();
    assert_eq!(reference["node_id"], 7);
    assert_eq!(reference["raft_group_id"], group);
    let key = reference["snapshot_key"].as_str().unwrap();
    assert!(store.stat(key).await.unwrap().content_length() > 0);
    assert!(!store.read(key).await.unwrap().is_empty());
    let view = cluster.view().await;
    assert_eq!(view.state.placements[&RaftGroupId(group)].epoch, 0);
    assert!(view.state.active_migration().unwrap().is_running());
}

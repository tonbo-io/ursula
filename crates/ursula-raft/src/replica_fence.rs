//! Durable, group-local admission for WAL-lifetime replica identities.

use std::collections::BTreeMap;
use std::io;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;

use serde::Deserialize;
use serde::Serialize;
use ursula_proto::admin::ReplicaIdentity;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
pub enum ReplicaFenceError {
    #[error("replica identity compare-and-set failed for node {node_id}")]
    Conflict { node_id: u64 },
    #[error("replica identity generation did not advance for node {node_id}")]
    Generation { node_id: u64 },
}

#[derive(Debug, Default)]
pub(crate) struct ReplicaFences {
    state: Mutex<PersistedReplicaFences>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct PersistedReplicaFences {
    identities: BTreeMap<u64, ReplicaIdentity>,
    required_fence_index: u64,
}

impl ReplicaFences {
    pub(crate) fn snapshot(&self) -> BTreeMap<u64, ReplicaIdentity> {
        self.state
            .lock()
            .expect("replica fence mutex")
            .identities
            .clone()
    }

    pub(crate) fn required_index(&self) -> u64 {
        self.state
            .lock()
            .expect("replica fence mutex")
            .required_fence_index
    }

    pub(crate) fn accepts(&self, node: u64, identity: &ReplicaIdentity) -> bool {
        self.state
            .lock()
            .expect("replica fence mutex")
            .identities
            .get(&node)
            == Some(identity)
    }

    pub(crate) fn path(snapshot_path: Option<&PathBuf>) -> Option<PathBuf> {
        snapshot_path.map(|path| path.with_extension("replicas.json"))
    }

    pub(crate) fn load(path: Option<&PathBuf>) -> io::Result<Arc<Self>> {
        let state = match path {
            Some(path) => match std::fs::read(path) {
                Ok(bytes) => {
                    ursula_runtime::decode_snapshot_envelope(&bytes).map_err(io::Error::other)?
                }
                Err(error) if error.kind() == io::ErrorKind::NotFound => {
                    PersistedReplicaFences::default()
                }
                Err(error) => return Err(error),
            },
            None => PersistedReplicaFences::default(),
        };
        Ok(Arc::new(Self {
            state: Mutex::new(state),
        }))
    }

    pub(crate) fn replacement(
        &self,
        node_id: u64,
        expected: Option<&ReplicaIdentity>,
        replacement: ReplicaIdentity,
    ) -> Result<BTreeMap<u64, ReplicaIdentity>, ReplicaFenceError> {
        let mut identities = self.snapshot();
        let previous = identities.get(&node_id);
        if previous == Some(&replacement) {
            return Ok(identities);
        }
        if previous != expected {
            return Err(ReplicaFenceError::Conflict { node_id });
        }
        if previous.is_some_and(|old| replacement.generation <= old.generation) {
            return Err(ReplicaFenceError::Generation { node_id });
        }
        identities.insert(node_id, replacement);
        Ok(identities)
    }

    /// A snapshot may lag an already fsynced fence after a crash. Never lower it.
    pub(crate) fn merge(
        &self,
        restored: BTreeMap<u64, ReplicaIdentity>,
    ) -> Result<BTreeMap<u64, ReplicaIdentity>, ReplicaFenceError> {
        let mut merged = self.snapshot();
        for (node, identity) in restored {
            if let Some(previous) = merged.get(&node) {
                if previous.generation > identity.generation {
                    continue;
                }
                if previous.generation == identity.generation && previous != &identity {
                    return Err(ReplicaFenceError::Conflict { node_id: node });
                }
            }
            merged.insert(node, identity);
        }
        Ok(merged)
    }

    pub(crate) async fn persist(
        self: &Arc<Self>,
        serial: Arc<crate::rt::sync::Mutex<()>>,
        path: Option<PathBuf>,
        identities: BTreeMap<u64, ReplicaIdentity>,
        required_fence_index: u64,
    ) -> io::Result<()> {
        let required_fence_index = self.required_index().max(required_fence_index);
        let state = PersistedReplicaFences {
            identities,
            required_fence_index,
        };
        let fences = self.clone();
        crate::state_machine::snapshot_metadata_work(serial, move || {
            if let Some(path) = path {
                if let Some(parent) = path.parent() {
                    std::fs::create_dir_all(parent)?;
                }
                let bytes =
                    ursula_runtime::encode_binary_envelope(&state).map_err(io::Error::other)?;
                let temporary = path.with_extension("tmp");
                {
                    use std::io::Write;
                    let mut file = std::fs::File::create(&temporary)?;
                    file.write_all(&bytes)?;
                    file.sync_all()?;
                }
                std::fs::rename(temporary, &path)?;
                if let Some(parent) = path.parent() {
                    std::fs::File::open(parent)?.sync_all()?;
                }
            }
            *fences.state.lock().expect("replica fence mutex") = state;
            Ok(())
        })
        .await
    }
}

//! Durable, group-local admission for WAL-lifetime replica identities.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
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
    #[error("recovery membership conflicts at certified prefix {index}")]
    RecoveryMembership { index: u64 },
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum ReplicaFencePersistenceError {
    #[error("replica fence I/O failed: {0}")]
    Io(#[from] io::Error),
    #[error("replica fence validation failed: {0}")]
    Fence(#[from] ReplicaFenceError),
    #[error("replica fence encoding failed: {0}")]
    Codec(#[from] ursula_runtime::SnapshotStoreError),
    #[cfg(not(madsim))]
    #[error("replica fence publication task stopped: {0}")]
    Task(#[source] tokio::task::JoinError),
}

impl ReplicaFencePersistenceError {
    /// OpenRaft's storage trait mandates io::Error; retain the typed source at
    /// that boundary while internal recovery/startup APIs remain typed.
    fn into_storage_io(self) -> io::Error {
        match self {
            Self::Io(error) => error,
            error => io::Error::new(io::ErrorKind::InvalidData, error),
        }
    }
}

#[derive(Debug, Default)]
pub(crate) struct ReplicaFences {
    state: Mutex<PersistedReplicaFences>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ReplicaRecoveryMembership {
    pub voters: BTreeSet<u64>,
    pub required_index: u64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct PersistedReplicaFences {
    identities: BTreeMap<u64, ReplicaIdentity>,
    required_fence_index: u64,
    #[serde(default)]
    recovery_membership: Option<ReplicaRecoveryMembership>,
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

    pub(crate) fn recovery_membership(&self) -> Option<ReplicaRecoveryMembership> {
        self.state
            .lock()
            .expect("replica fence mutex")
            .recovery_membership
            .clone()
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
        self.persist_with_recovery(serial, path, identities, required_fence_index, None)
            .await
            .map_err(ReplicaFencePersistenceError::into_storage_io)
    }

    pub(crate) async fn persist_with_recovery(
        self: &Arc<Self>,
        serial: Arc<crate::rt::sync::Mutex<()>>,
        path: Option<PathBuf>,
        identities: BTreeMap<u64, ReplicaIdentity>,
        required_fence_index: u64,
        recovery: Option<ReplicaRecoveryMembership>,
    ) -> Result<(), ReplicaFencePersistenceError> {
        let fences = self.clone();
        let serial = serial.lock_owned().await;
        let work = move || -> Result<(), ReplicaFencePersistenceError> {
            let _serial = serial;
            // Merge only after acquiring the publication guard. A queued older
            // snapshot/restore cannot overwrite a newer fsynced identity or gate.
            let previous = fences.state.lock().expect("replica fence mutex").clone();
            let mut merged = previous.identities;
            for (node, identity) in identities {
                if let Some(old) = merged.get(&node) {
                    if old.generation > identity.generation {
                        continue;
                    }
                    if old.generation == identity.generation && old != &identity {
                        return Err(ReplicaFenceError::Conflict { node_id: node }.into());
                    }
                }
                merged.insert(node, identity);
            }
            let recovery_membership = match (previous.recovery_membership, recovery) {
                (Some(previous), Some(next)) if previous.required_index > next.required_index => {
                    Some(previous)
                }
                (Some(previous), Some(next))
                    if previous.required_index == next.required_index && previous != next =>
                {
                    return Err(ReplicaFenceError::RecoveryMembership {
                        index: next.required_index,
                    }
                    .into());
                }
                (_, Some(next)) => Some(next),
                (previous, None) => previous,
            };
            let state = PersistedReplicaFences {
                identities: merged,
                required_fence_index: previous.required_fence_index.max(required_fence_index),
                recovery_membership,
            };
            if let Some(path) = path {
                if let Some(parent) = path.parent() {
                    std::fs::create_dir_all(parent)?;
                }
                let bytes = ursula_runtime::encode_binary_envelope(&state)?;
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
        };
        #[cfg(not(madsim))]
        {
            tokio::task::spawn_blocking(work)
                .await
                .map_err(ReplicaFencePersistenceError::Task)?
        }
        #[cfg(madsim)]
        {
            work()
        }
    }
}

#[cfg(all(test, not(madsim)))]
mod tests {
    use super::*;

    #[tokio::test]
    async fn queued_older_publication_cannot_replace_newer_recovery_certificate() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("replicas.json");
        let fences = Arc::new(ReplicaFences::default());
        let serial = Arc::new(crate::rt::sync::Mutex::new(()));
        let held = serial.clone().lock_owned().await;
        let identity = |generation| ReplicaIdentity {
            generation,
            incarnation: ursula_proto::admin::ProcessIncarnation::from_bits(u128::from(generation)),
        };
        let certificate = ReplicaRecoveryMembership {
            voters: BTreeSet::from([1, 2, 4]),
            required_index: 20,
        };
        let mut newer = Box::pin(fences.persist_with_recovery(
            serial.clone(),
            Some(path.clone()),
            BTreeMap::from([(2, identity(5))]),
            20,
            Some(certificate.clone()),
        ));
        let mut older = Box::pin(fences.persist(
            serial,
            Some(path.clone()),
            BTreeMap::from([(2, identity(1))]),
            10,
        ));
        assert!(futures_util::poll!(&mut newer).is_pending());
        assert!(futures_util::poll!(&mut older).is_pending());
        drop(held);
        let (newer, older) = futures_util::join!(newer, older);
        newer.unwrap();
        older.unwrap();
        let restored = ReplicaFences::load(Some(&path)).unwrap();
        assert!(restored.accepts(2, &identity(5)));
        assert_eq!(restored.required_index(), 20);
        assert_eq!(restored.recovery_membership(), Some(certificate));
        let bytes = std::fs::read(&path).unwrap();
        let conflict = restored
            .persist_with_recovery(
                Arc::default(),
                Some(path.clone()),
                BTreeMap::from([(2, ReplicaIdentity {
                    generation: 5,
                    incarnation: ursula_proto::admin::ProcessIncarnation::from_bits(99),
                })]),
                20,
                None,
            )
            .await
            .unwrap_err();
        assert!(matches!(
            conflict,
            ReplicaFencePersistenceError::Fence(ReplicaFenceError::Conflict { node_id: 2 })
        ));
        let conflict = restored
            .persist_with_recovery(
                Arc::default(),
                Some(path.clone()),
                BTreeMap::new(),
                20,
                Some(ReplicaRecoveryMembership {
                    voters: BTreeSet::from([1, 2]),
                    required_index: 20,
                }),
            )
            .await
            .unwrap_err();
        assert!(matches!(
            conflict,
            ReplicaFencePersistenceError::Fence(ReplicaFenceError::RecoveryMembership {
                index: 20
            })
        ));
        assert_eq!(std::fs::read(&path).unwrap(), bytes);
    }
}

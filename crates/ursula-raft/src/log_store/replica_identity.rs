//! A replica identity belongs to one WAL lifetime, not one process boot.
//! The separate lock is retained through meta startup and data shutdown; all
//! publications use the WAL's checksummed file/fsync/rename/directory protocol.

use std::io;
use std::path::Path;
use std::path::PathBuf;

use serde::Deserialize;
use serde::Serialize;
use ursula_proto::admin::ProcessIncarnation;
use ursula_proto::admin::ReplicaIdentity;

use super::disk::Disk;
use super::disk::DiskLock;
use super::disk::JournalDisk;
use super::disk::LockAttempt;
use super::disk::create_dir_all_durable;
use super::journal::JournalError;
use super::state_file;
use super::state_file::StateFileError;
use super::state_file::StateFileKind;

pub const REPLICA_IDENTITY_FILE: &str = "replica-identity.bin";
const REQUIRED_FILE: &str = "replica-identity.required";

#[derive(Debug, Clone, Serialize, Deserialize)]
enum Admission {
    Pending(ProcessIncarnation),
    Bound(ReplicaIdentity),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Record {
    node_id: u64,
    admission: Admission,
}

#[derive(Debug, thiserror::Error)]
pub enum ReplicaIdentityError {
    #[error("replica identity filesystem operation at '{}': {source}", path.display())]
    Io {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("another process owns replica identity at '{}'", path.display())]
    Locked { path: PathBuf },
    #[error("existing WAL at '{}' lacks its replica identity; explicit recovery is required", root.display())]
    MissingForExistingWal { root: PathBuf },
    #[error("replica identity belongs to node {stored}, not configured node {configured}")]
    WrongNode { stored: u64, configured: u64 },
    #[error("replica identity generation must be nonzero")]
    InvalidGeneration,
    #[error("read replica identity: {0}")]
    Read(#[from] StateFileError),
    #[error("persist replica identity: {0}")]
    Persist(#[from] JournalError),
}

#[derive(Debug)]
pub struct ReplicaIdentityStore {
    path: PathBuf,
    record: Record,
    _lock: DiskLock,
}

impl ReplicaIdentityStore {
    /// `fresh_token` is used only for a completely fresh WAL. An existing
    /// root never changes identity merely because its server restarted.
    pub fn open(
        root: &Path,
        node_id: u64,
        fresh_token: ProcessIncarnation,
    ) -> Result<Self, ReplicaIdentityError> {
        // Reject unsupported existing data without even creating our lock
        // file. Recheck after acquiring the lock before admitting a fresh WAL.
        if state_file::read::<Record>(
            StateFileKind::ReplicaIdentity,
            &root.join(REPLICA_IDENTITY_FILE),
        )?
        .is_none()
            && (state_file::read::<u64>(
                StateFileKind::ReplicaIdentityRequired,
                &root.join(REQUIRED_FILE),
            )?
            .is_some()
                || has_wal_history(root)?)
        {
            return Err(ReplicaIdentityError::MissingForExistingWal {
                root: root.to_owned(),
            });
        }
        create_dir_all_durable(root).map_err(|source| ReplicaIdentityError::Io {
            path: root.to_owned(),
            source,
        })?;
        let lock_path = root.join("replica-identity.lock");
        let lock = match Disk::try_lock(&lock_path).map_err(|source| ReplicaIdentityError::Io {
            path: lock_path.clone(),
            source,
        })? {
            LockAttempt::Acquired(lock) => lock,
            LockAttempt::Held { .. } => {
                return Err(ReplicaIdentityError::Locked { path: lock_path });
            }
        };
        let path = root.join(REPLICA_IDENTITY_FILE);
        let required_path = root.join(REQUIRED_FILE);
        let required =
            state_file::read::<u64>(StateFileKind::ReplicaIdentityRequired, &required_path)?;
        if let Some(stored) = required
            && stored != node_id
        {
            return Err(ReplicaIdentityError::WrongNode {
                stored,
                configured: node_id,
            });
        }
        let record = match state_file::read::<Record>(StateFileKind::ReplicaIdentity, &path)? {
            Some(record) => record,
            None => {
                if required.is_some() || has_wal_history(root)? {
                    return Err(ReplicaIdentityError::MissingForExistingWal {
                        root: root.to_owned(),
                    });
                }
                let record = Record {
                    node_id,
                    admission: Admission::Pending(fresh_token),
                };
                state_file::write(
                    StateFileKind::ReplicaIdentity,
                    &path,
                    &path.with_extension("tmp"),
                    &record,
                )?;
                record
            }
        };
        if record.node_id != node_id {
            return Err(ReplicaIdentityError::WrongNode {
                stored: record.node_id,
                configured: node_id,
            });
        }
        if required.is_none() {
            state_file::write(
                StateFileKind::ReplicaIdentityRequired,
                &required_path,
                &required_path.with_extension("tmp"),
                &node_id,
            )?;
        }
        Ok(Self {
            path,
            record,
            _lock: lock,
        })
    }

    pub fn identity(&self) -> Option<&ReplicaIdentity> {
        match &self.record.admission {
            Admission::Pending(_) => None,
            Admission::Bound(identity) => Some(identity),
        }
    }

    /// Bind the initial token to a generation assigned by the meta authority.
    /// A new boot's process epoch never changes an already bound replica.
    pub fn bind_initial_generation(
        &mut self,
        generation: u64,
    ) -> Result<ReplicaIdentity, ReplicaIdentityError> {
        let incarnation = match &self.record.admission {
            Admission::Bound(identity) => return Ok(identity.clone()),
            Admission::Pending(incarnation) => incarnation.clone(),
        };
        if generation == 0 {
            return Err(ReplicaIdentityError::InvalidGeneration);
        }
        let identity = ReplicaIdentity {
            generation,
            incarnation: incarnation.clone(),
        };
        let record = Record {
            node_id: self.record.node_id,
            admission: Admission::Bound(identity.clone()),
        };
        state_file::write(
            StateFileKind::ReplicaIdentity,
            &self.path,
            &self.path.with_extension("tmp"),
            &record,
        )?;
        self.record = record;
        Ok(identity)
    }
}

fn has_wal_history(root: &Path) -> Result<bool, ReplicaIdentityError> {
    let entries = match Disk::read_dir(root) {
        Ok(entries) => entries,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(source) => {
            return Err(ReplicaIdentityError::Io {
                path: root.to_owned(),
                source,
            });
        }
    };
    Ok(entries.iter().any(|entry| {
        entry
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| {
                name == super::run_state::RUN_STATE_FILE
                    || name == "topology.bin"
                    || name == "meta-raft"
                    || name.starts_with("core-")
            })
    }))
}

#[cfg(all(test, not(madsim)))]
mod tests {
    use super::ProcessIncarnation;
    use super::REPLICA_IDENTITY_FILE;
    use super::ReplicaIdentityError;
    use super::ReplicaIdentityStore;

    #[test]
    fn replica_identity_survives_restarts_and_rejects_missing_or_wrong_history() {
        let root = tempfile::tempdir().unwrap();
        // The server's format preflight runs before identity publication.
        std::fs::write(root.path().join("FORMAT_EPOCH"), b"3").unwrap();
        let mut store =
            ReplicaIdentityStore::open(root.path(), 7, ProcessIncarnation::from_bits(1)).unwrap();
        assert!(matches!(
            ReplicaIdentityStore::open(root.path(), 7, ProcessIncarnation::from_bits(2)),
            Err(ReplicaIdentityError::Locked { .. })
        ));
        let identity = store.bind_initial_generation(10).unwrap();
        drop(store);
        let mut reopened =
            ReplicaIdentityStore::open(root.path(), 7, ProcessIncarnation::from_bits(3)).unwrap();
        assert_eq!(reopened.identity(), Some(&identity));
        assert_eq!(reopened.bind_initial_generation(20).unwrap(), identity);
        drop(reopened);
        assert!(matches!(
            ReplicaIdentityStore::open(root.path(), 8, ProcessIncarnation::from_bits(4)),
            Err(ReplicaIdentityError::WrongNode { .. })
        ));
        std::fs::create_dir(root.path().join("core-0")).unwrap();
        std::fs::remove_file(root.path().join(REPLICA_IDENTITY_FILE)).unwrap();
        assert!(matches!(
            ReplicaIdentityStore::open(root.path(), 7, ProcessIncarnation::from_bits(5)),
            Err(ReplicaIdentityError::MissingForExistingWal { .. })
        ));
    }

    #[test]
    fn legacy_history_without_identity_is_rejected_without_modification() {
        let root = tempfile::tempdir().unwrap();
        let core = root.path().join("core-0");
        std::fs::create_dir(&core).unwrap();
        let history = core.join("sentinel");
        std::fs::write(&history, b"existing WAL bytes").unwrap();
        assert!(matches!(
            ReplicaIdentityStore::open(root.path(), 1, ProcessIncarnation::from_bits(1)),
            Err(ReplicaIdentityError::MissingForExistingWal { .. })
        ));
        assert_eq!(std::fs::read(history).unwrap(), b"existing WAL bytes");
        assert!(!root.path().join(REPLICA_IDENTITY_FILE).exists());
        assert!(!root.path().join(super::REQUIRED_FILE).exists());
        assert!(!root.path().join("replica-identity.lock").exists());
    }

    #[test]
    fn corrupt_identity_is_not_replaced() {
        let root = tempfile::tempdir().unwrap();
        drop(ReplicaIdentityStore::open(root.path(), 1, ProcessIncarnation::from_bits(1)).unwrap());
        std::fs::write(root.path().join(REPLICA_IDENTITY_FILE), b"corrupt").unwrap();
        assert!(matches!(
            ReplicaIdentityStore::open(root.path(), 1, ProcessIncarnation::from_bits(2)),
            Err(ReplicaIdentityError::Read(_))
        ));
    }
}

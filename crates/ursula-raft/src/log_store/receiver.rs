//! Checksummed receiver checkpoint; its own immutable identity binding detects
//! missing history and its lock survives atomic checkpoint replacement.

#[cfg(not(madsim))]
use std::fs;
use std::io;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;

use serde::Deserialize;
use serde::Serialize;
use ursula_control::MetaLocalIdentity;
use ursula_control::ReceiverLedger;
#[cfg(not(madsim))]
use ursula_runtime::journal;

use super::JournalLock;
#[cfg(not(madsim))]
use super::WireCodec;
use super::meta::replace_journal;
use super::spawn_log_store_blocking;

const MAX_CHECKPOINT_BYTES: u64 = 16 * 1024 * 1024;

#[derive(Clone, Serialize, Deserialize)]
struct Checkpoint {
    identity: MetaLocalIdentity,
    ledger: ReceiverLedger,
}

struct Inner {
    ledger: ReceiverLedger,
    failed: bool,
}

pub struct ManagedReceiverStore {
    identity: MetaLocalIdentity,
    path: PathBuf,
    inner: Mutex<Inner>,
    _lock: JournalLock,
}

impl ManagedReceiverStore {
    #[cfg(madsim)]
    pub async fn open(_path: PathBuf, _identity: MetaLocalIdentity) -> io::Result<Arc<Self>> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "receiver files are unavailable in simulation",
        ))
    }

    #[cfg(not(madsim))]
    pub async fn open(path: PathBuf, identity: MetaLocalIdentity) -> io::Result<Arc<Self>> {
        spawn_log_store_blocking(None, move || {
            let identity = identity.normalize().map_err(invalid)?;
            let lock = JournalLock::acquire(&path)?;
            let mut name = path.as_os_str().to_owned();
            name.push(".identity");
            let binding = PathBuf::from(name);
            let bound = binding.exists();
            if bound {
                let saved: MetaLocalIdentity = read_one(&binding)?;
                if saved != identity {
                    return Err(invalid("receiver identity differs"));
                }
                if !path.exists() {
                    return Err(invalid("receiver checkpoint is missing beside its binding"));
                }
            }
            let ledger = if path.exists() {
                let checkpoint: Checkpoint = read_one(&path)?;
                if checkpoint.identity != identity {
                    return Err(invalid("receiver checkpoint identity differs"));
                }
                checkpoint
                    .ledger
                    .validate(identity.cluster.group_count)
                    .map_err(invalid)?;
                validate_receipt_node(&checkpoint.ledger, identity.node.node_id)?;
                if !bound && checkpoint.ledger != ReceiverLedger::default() {
                    return Err(invalid("cannot bind existing receiver authority"));
                }
                checkpoint.ledger
            } else {
                let ledger = ReceiverLedger::default();
                replace_journal(&path, [Checkpoint {
                    identity: identity.clone(),
                    ledger: ledger.clone(),
                }])?;
                ledger
            };
            // A crash before binding can retry only the identical empty
            // checkpoint. Authority cannot be admitted until open succeeds.
            if !bound {
                replace_journal(&binding, [identity.clone()])?;
            }
            Ok(Arc::new(Self {
                identity,
                path,
                inner: Mutex::new(Inner {
                    ledger,
                    failed: false,
                }),
                _lock: lock,
            }))
        })
        .await
    }

    pub fn identity(&self) -> &MetaLocalIdentity {
        &self.identity
    }

    pub fn snapshot(&self) -> io::Result<ReceiverLedger> {
        let inner = self
            .inner
            .lock()
            .map_err(|_| invalid("receiver store mutex poisoned"))?;
        if inner.failed {
            return Err(invalid("receiver storage failed; reopen to recover"));
        }
        Ok(inner.ledger.clone())
    }

    pub async fn persist(
        self: &Arc<Self>,
        mut ledger: ReceiverLedger,
    ) -> io::Result<ReceiverLedger> {
        let store = self.clone();
        spawn_log_store_blocking(None, move || {
            let mut inner = store
                .inner
                .lock()
                .map_err(|_| invalid("receiver store mutex poisoned"))?;
            if inner.failed {
                return Err(invalid("receiver storage failed; reopen to recover"));
            }
            if inner.ledger == ledger {
                return Ok(ledger);
            }
            if inner.ledger.revision != ledger.revision {
                return Err(invalid("receiver checkpoint revision CAS failed"));
            }
            ledger.revision = ledger
                .revision
                .checked_add(1)
                .ok_or_else(|| invalid("receiver checkpoint revision exhausted"))?;
            inner
                .ledger
                .validate_successor(&ledger, store.identity.cluster.group_count)
                .map_err(invalid)?;
            validate_receipt_node(&ledger, store.identity.node.node_id)?;
            let checkpoint = Checkpoint {
                identity: store.identity.clone(),
                ledger: ledger.clone(),
            };
            if crate::codec::encode_wire(&checkpoint).len() as u64
                > MAX_CHECKPOINT_BYTES.saturating_sub(64)
            {
                return Err(invalid("receiver checkpoint exceeds its bounded size"));
            }
            let result = replace_journal(&store.path, [checkpoint]);
            if result.is_err() {
                inner.failed = true;
            }
            result?;
            inner.ledger = ledger.clone();
            Ok(ledger)
        })
        .await
    }
}

fn validate_receipt_node(ledger: &ReceiverLedger, node_id: u64) -> io::Result<()> {
    if let Some(receipt) = &ledger.completed {
        let actual = match &receipt.result {
            ursula_control::ReplicaMutationResult::Prepared { process } => process.node_id,
            ursula_control::ReplicaMutationResult::Released { evidence } => {
                evidence.process.node_id
            }
        };
        if actual != node_id {
            return Err(invalid("receiver receipt differs from its bound node"));
        }
    }
    Ok(())
}

fn invalid(reason: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, reason.into())
}

#[cfg(not(madsim))]
fn read_one<T: serde::de::DeserializeOwned + Serialize>(path: &std::path::Path) -> io::Result<T> {
    if fs::metadata(path)?.len() > MAX_CHECKPOINT_BYTES {
        return Err(invalid("receiver file exceeds its bounded size"));
    }
    let bytes = fs::read(path)?;
    let (records, valid) = journal::decode_frames::<WireCodec<T>>(&bytes)?;
    if valid != bytes.len() || records.len() != 1 {
        return Err(invalid(
            "receiver file is not one complete checksummed record",
        ));
    }
    records
        .into_iter()
        .next()
        .ok_or_else(|| invalid("missing receiver record"))
}

#[cfg(all(test, not(madsim)))]
#[path = "receiver_tests.rs"]
mod tests;

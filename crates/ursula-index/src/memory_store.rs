//! In-memory conditional object store (design §6.1 U21, §11.7).
//!
//! The simulator runs the keyed engine against this store. Faults and
//! mutation observation are not part of it: they are the one hook mechanism
//! of every store, [`ObjectStore::with_hooks`] with [`ObjectHooks`]
//! (latency, failures, spurious CAS conflicts, ambiguous mutations). The
//! store's operations never suspend, so the hooks see its mutations in the
//! exact order they take effect.
//!
//! Entity tags are content digests, as with the filesystem store, and
//! modification times come from the injected [`Clock`].
//!
//! [`ObjectHooks`]: crate::ObjectHooks

use std::collections::BTreeMap;
use std::ops::Range;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::MutexGuard;
use std::sync::PoisonError;
use std::time::Duration;
use std::time::SystemTime;

use crate::IndexError;
use crate::clock::Clock;
use crate::object_store::ConditionalWrite;
use crate::object_store::ObjectInfo;
use crate::object_store::ObjectStore;
use crate::object_store::StoredObject;
use crate::object_store::digest;

#[derive(Clone, Debug)]
struct MemoryObject {
    bytes: Arc<[u8]>,
    etag: String,
    modified_ms: u64,
}

/// A process-local conditional object store. Clones share the objects.
#[derive(Clone)]
pub struct MemoryObjectStore {
    objects: Arc<Mutex<BTreeMap<String, MemoryObject>>>,
    clock: Arc<dyn Clock>,
}

impl std::fmt::Debug for MemoryObjectStore {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("MemoryObjectStore")
            .field("objects", &lock(&self.objects).len())
            .finish_non_exhaustive()
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

impl MemoryObjectStore {
    /// An empty store whose modification times come from `clock`.
    pub fn new(clock: Arc<dyn Clock>) -> Self {
        Self {
            objects: Arc::new(Mutex::new(BTreeMap::new())),
            clock,
        }
    }

    /// Key and modification time (clock milliseconds) of every object, in
    /// key order.
    pub fn snapshot(&self) -> Vec<(String, u64)> {
        lock(&self.objects)
            .iter()
            .map(|(key, object)| (key.clone(), object.modified_ms))
            .collect()
    }

    /// The bytes of `key` (for checkers).
    pub fn object_bytes(&self, key: &str) -> Option<Vec<u8>> {
        lock(&self.objects)
            .get(key)
            .map(|object| object.bytes.to_vec())
    }

    pub(crate) fn get(&self, key: &str) -> Option<StoredObject> {
        lock(&self.objects).get(key).map(|object| StoredObject {
            bytes: object.bytes.to_vec(),
            etag: object.etag.clone(),
        })
    }

    pub(crate) fn stat(&self, key: &str) -> Option<ObjectInfo> {
        lock(&self.objects).get(key).map(|object| ObjectInfo {
            key: key.to_owned(),
            modified: modified(object),
        })
    }

    pub(crate) fn get_range(
        &self,
        key: &str,
        range: Range<u64>,
    ) -> Result<Option<Vec<u8>>, IndexError> {
        let objects = lock(&self.objects);
        let Some(object) = objects.get(key) else {
            return Ok(None);
        };
        let start = usize::try_from(range.start);
        let end = usize::try_from(range.end);
        match (start, end) {
            (Ok(start), Ok(end)) => object
                .bytes
                .get(start..end)
                .map(|bytes| Some(bytes.to_vec()))
                .ok_or_else(|| IndexError::InvalidPartLayout(key.to_owned())),
            _ => Err(IndexError::InvalidPartLayout(key.to_owned())),
        }
    }

    fn install(&self, objects: &mut BTreeMap<String, MemoryObject>, key: &str, bytes: &[u8]) {
        objects.insert(key.to_owned(), MemoryObject {
            bytes: Arc::from(bytes),
            etag: digest(bytes),
            modified_ms: self.clock.now_ms(),
        });
    }

    pub(crate) fn put_if_absent(&self, key: &str, bytes: &[u8]) -> ConditionalWrite {
        let mut objects = lock(&self.objects);
        if objects.contains_key(key) {
            return ConditionalWrite::Conflict;
        }
        self.install(&mut objects, key, bytes);
        ConditionalWrite::Written
    }

    pub(crate) fn compare_and_swap(
        &self,
        key: &str,
        expected_etag: &str,
        bytes: &[u8],
    ) -> ConditionalWrite {
        let mut objects = lock(&self.objects);
        if objects
            .get(key)
            .is_none_or(|object| object.etag != expected_etag)
        {
            return ConditionalWrite::Conflict;
        }
        self.install(&mut objects, key, bytes);
        ConditionalWrite::Written
    }

    pub(crate) fn list(&self, prefix: &str) -> Vec<ObjectInfo> {
        lock(&self.objects)
            .range(prefix.to_owned()..)
            .take_while(|(key, _)| key.starts_with(prefix))
            .map(|(key, object)| ObjectInfo {
                key: key.clone(),
                modified: modified(object),
            })
            .collect()
    }

    /// Deletes `key`; returns whether it existed.
    pub(crate) fn delete(&self, key: &str) -> bool {
        lock(&self.objects).remove(key).is_some()
    }
}

fn modified(object: &MemoryObject) -> Option<SystemTime> {
    SystemTime::UNIX_EPOCH.checked_add(Duration::from_millis(object.modified_ms))
}

impl From<MemoryObjectStore> for ObjectStore {
    fn from(value: MemoryObjectStore) -> Self {
        Self::Memory(value)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicU64;
    use std::sync::atomic::Ordering;

    use super::*;
    use crate::object_store::AppliedChange;
    use crate::object_store::FaultDecision;
    use crate::object_store::ObjectFault;
    use crate::object_store::ObjectHooks;
    use crate::object_store::ObjectOp;

    struct FixedClock(AtomicU64);

    impl Clock for FixedClock {
        fn now_ms(&self) -> u64 {
            self.0.load(Ordering::SeqCst)
        }
    }

    struct Script {
        faults: Mutex<Vec<ObjectFault>>,
        applied: Mutex<Vec<String>>,
    }

    impl ObjectHooks for Script {
        fn decide(&self, op: ObjectOp, _key: &str) -> FaultDecision {
            let fault = if op.is_mutation() {
                lock(&self.faults).pop().unwrap_or_default()
            } else {
                ObjectFault::None
            };
            FaultDecision {
                delay: Duration::ZERO,
                fault,
            }
        }

        fn applied(&self, change: AppliedChange<'_>) {
            let entry = match change {
                AppliedChange::Put { key, .. } => format!("put {key}"),
                AppliedChange::Delete { key } => format!("delete {key}"),
            };
            lock(&self.applied).push(entry);
        }
    }

    #[tokio::test]
    async fn conditional_writes_list_and_faults() {
        let clock = Arc::new(FixedClock(AtomicU64::new(5_000)));
        let script = Arc::new(Script {
            faults: Mutex::new(Vec::new()),
            applied: Mutex::new(Vec::new()),
        });
        let store = MemoryObjectStore::new(clock.clone());
        let as_object = ObjectStore::from(store.clone()).with_hooks(script.clone());

        assert_eq!(
            as_object.put_if_absent("a/x", b"one").await.unwrap(),
            ConditionalWrite::Written
        );
        assert_eq!(
            as_object.put_if_absent("a/x", b"two").await.unwrap(),
            ConditionalWrite::Conflict
        );
        let current = as_object.get("a/x").await.unwrap().unwrap();
        assert_eq!(current.bytes, b"one");
        assert_eq!(
            as_object
                .compare_and_swap("a/x", "stale", b"two")
                .await
                .unwrap(),
            ConditionalWrite::Conflict
        );
        assert_eq!(
            as_object
                .compare_and_swap("a/x", &current.etag, b"two")
                .await
                .unwrap(),
            ConditionalWrite::Written
        );
        assert_eq!(
            as_object.get_range("a/x", 1..3).await.unwrap().unwrap(),
            b"wo"
        );

        // A spurious conflict has no effect; an ambiguous write takes effect
        // and fails; a failed delete has no effect.
        let etag = as_object.get("a/x").await.unwrap().unwrap().etag;
        *lock(&script.faults) = vec![
            ObjectFault::Fail,
            ObjectFault::Ambiguous,
            ObjectFault::Conflict,
        ];
        assert_eq!(
            as_object
                .compare_and_swap("a/x", &etag, b"three")
                .await
                .unwrap(),
            ConditionalWrite::Conflict
        );
        as_object.put_if_absent("a/y", b"y").await.unwrap_err();
        assert!(as_object.get("a/y").await.unwrap().is_some());
        as_object.delete("a/y").await.unwrap_err();
        assert!(as_object.get("a/y").await.unwrap().is_some());

        clock.0.store(9_000, Ordering::SeqCst);
        as_object.put_if_absent("b/z", b"z").await.unwrap();
        let listed = as_object.list("a/").await.unwrap();
        let keys: Vec<&str> = listed.iter().map(|info| info.key.as_str()).collect();
        assert_eq!(keys, ["a/x", "a/y"]);
        assert_eq!(
            listed[0].modified,
            SystemTime::UNIX_EPOCH.checked_add(Duration::from_millis(5_000))
        );
        as_object.delete("a/y").await.unwrap();
        as_object.delete("a/y").await.unwrap();
        assert_eq!(*lock(&script.applied), [
            "put a/x",
            "put a/x",
            "put a/y",
            "put b/z",
            "delete a/y"
        ]);
        assert_eq!(store.snapshot().len(), 2);
    }
}

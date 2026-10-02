//! In-memory conditional object store with deterministic fault hooks
//! (design §6.1 U21, §11.7).
//!
//! The simulator runs the keyed engine against this store. Every operation
//! first asks the [`MemoryStoreHooks`] for a [`FaultDecision`]: a latency
//! (slept on the task seam, so it is virtual time under `cfg(madsim)`) and a
//! fault — a failure without effect, a conditional write that reports a
//! conflict without effect, or an ambiguous write or delete that takes
//! effect and then reports a failure. Applied mutations are reported to the
//! hooks synchronously, in the order they take effect, which lets a checker
//! observe every published `CURRENT` and every deletion.
//!
//! Entity tags are content digests, as with the filesystem store, and
//! modification times come from the injected [`Clock`].

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

/// An object-store operation, as seen by [`MemoryStoreHooks::decide`].
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ObjectOp {
    /// Whole-object read with its entity tag.
    Get,
    /// Byte-range read.
    GetRange,
    /// Create-only write.
    PutIfAbsent,
    /// Write conditional on the current entity tag.
    CompareAndSwap,
    /// Prefix listing.
    List,
    /// Deletion.
    Delete,
}

impl ObjectOp {
    /// Whether the operation mutates the store.
    pub fn is_mutation(self) -> bool {
        matches!(
            self,
            Self::PutIfAbsent | Self::CompareAndSwap | Self::Delete
        )
    }
}

/// The fault injected into one operation.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum ObjectFault {
    /// The operation runs normally.
    #[default]
    None,
    /// The operation fails without effect.
    Fail,
    /// A conditional write reports a conflict without effect (a spurious
    /// precondition failure); other operations run normally.
    Conflict,
    /// A mutation takes effect and then reports a failure (a lost response);
    /// reads fail without effect.
    Ambiguous,
}

/// Latency and fault of one operation.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct FaultDecision {
    /// Delay before the operation runs.
    pub delay: Duration,
    /// The injected fault.
    pub fault: ObjectFault,
}

/// A mutation that took effect.
#[derive(Clone, Copy, Debug)]
pub enum AppliedChange<'a> {
    /// `key` now holds `bytes`.
    Put {
        /// Object key.
        key: &'a str,
        /// The written bytes.
        bytes: &'a [u8],
    },
    /// `key`, which existed, was deleted.
    Delete {
        /// Object key.
        key: &'a str,
    },
}

/// Fault injection and mutation observation for a [`MemoryObjectStore`].
pub trait MemoryStoreHooks: Send + Sync {
    /// The latency and fault of the next operation `op` on `key` (the
    /// prefix for [`ObjectOp::List`]).
    fn decide(&self, op: ObjectOp, key: &str) -> FaultDecision {
        let _unused = (op, key);
        FaultDecision::default()
    }

    /// Called under the store lock right after a mutation takes effect.
    fn applied(&self, change: AppliedChange<'_>) {
        let _unused = change;
    }
}

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
    hooks: Option<Arc<dyn MemoryStoreHooks>>,
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

fn injected(op: ObjectOp, key: &str) -> IndexError {
    IndexError::ObjectStore(format!("injected {op:?} failure on `{key}`"))
}

impl MemoryObjectStore {
    /// An empty store whose modification times come from `clock`.
    pub fn new(clock: Arc<dyn Clock>) -> Self {
        Self {
            objects: Arc::new(Mutex::new(BTreeMap::new())),
            clock,
            hooks: None,
        }
    }

    /// A handle over the same objects whose operations go through `hooks`.
    #[must_use]
    pub fn with_hooks(&self, hooks: Arc<dyn MemoryStoreHooks>) -> Self {
        Self {
            objects: Arc::clone(&self.objects),
            clock: Arc::clone(&self.clock),
            hooks: Some(hooks),
        }
    }

    /// A handle over the same objects without hooks (no faults, no
    /// observation), for checkers.
    #[must_use]
    pub fn without_hooks(&self) -> Self {
        Self {
            objects: Arc::clone(&self.objects),
            clock: Arc::clone(&self.clock),
            hooks: None,
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

    /// The bytes of `key`, bypassing hooks (for checkers).
    pub fn object_bytes(&self, key: &str) -> Option<Vec<u8>> {
        lock(&self.objects)
            .get(key)
            .map(|object| object.bytes.to_vec())
    }

    async fn decide(&self, op: ObjectOp, key: &str) -> ObjectFault {
        let Some(hooks) = &self.hooks else {
            return ObjectFault::None;
        };
        let decision = hooks.decide(op, key);
        if !decision.delay.is_zero() {
            crate::rt::time::sleep(decision.delay).await;
        }
        decision.fault
    }

    fn notify(&self, change: AppliedChange<'_>) {
        if let Some(hooks) = &self.hooks {
            hooks.applied(change);
        }
    }

    pub(crate) async fn get(&self, key: &str) -> Result<Option<StoredObject>, IndexError> {
        match self.decide(ObjectOp::Get, key).await {
            ObjectFault::Fail | ObjectFault::Ambiguous => Err(injected(ObjectOp::Get, key)),
            ObjectFault::None | ObjectFault::Conflict => {
                Ok(lock(&self.objects).get(key).map(|object| StoredObject {
                    bytes: object.bytes.to_vec(),
                    etag: object.etag.clone(),
                }))
            }
        }
    }

    pub(crate) async fn get_range(
        &self,
        key: &str,
        range: Range<u64>,
    ) -> Result<Option<Vec<u8>>, IndexError> {
        if matches!(
            self.decide(ObjectOp::GetRange, key).await,
            ObjectFault::Fail | ObjectFault::Ambiguous
        ) {
            return Err(injected(ObjectOp::GetRange, key));
        }
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
        self.notify(AppliedChange::Put { key, bytes });
    }

    async fn conditional_write(
        &self,
        op: ObjectOp,
        key: &str,
        bytes: &[u8],
        may_write: impl FnOnce(Option<&MemoryObject>) -> bool,
    ) -> Result<ConditionalWrite, IndexError> {
        let fault = self.decide(op, key).await;
        match fault {
            ObjectFault::Fail => return Err(injected(op, key)),
            ObjectFault::Conflict => return Ok(ConditionalWrite::Conflict),
            ObjectFault::None | ObjectFault::Ambiguous => {}
        }
        let mut objects = lock(&self.objects);
        if !may_write(objects.get(key)) {
            return Ok(ConditionalWrite::Conflict);
        }
        self.install(&mut objects, key, bytes);
        drop(objects);
        if fault == ObjectFault::Ambiguous {
            return Err(injected(op, key));
        }
        Ok(ConditionalWrite::Written)
    }

    pub(crate) async fn put_if_absent(
        &self,
        key: &str,
        bytes: &[u8],
    ) -> Result<ConditionalWrite, IndexError> {
        self.conditional_write(ObjectOp::PutIfAbsent, key, bytes, |current| {
            current.is_none()
        })
        .await
    }

    pub(crate) async fn compare_and_swap(
        &self,
        key: &str,
        expected_etag: &str,
        bytes: &[u8],
    ) -> Result<ConditionalWrite, IndexError> {
        self.conditional_write(ObjectOp::CompareAndSwap, key, bytes, |current| {
            current.is_some_and(|object| object.etag == expected_etag)
        })
        .await
    }

    pub(crate) async fn list(&self, prefix: &str) -> Result<Vec<ObjectInfo>, IndexError> {
        if matches!(
            self.decide(ObjectOp::List, prefix).await,
            ObjectFault::Fail | ObjectFault::Ambiguous
        ) {
            return Err(injected(ObjectOp::List, prefix));
        }
        Ok(lock(&self.objects)
            .range(prefix.to_owned()..)
            .take_while(|(key, _)| key.starts_with(prefix))
            .map(|(key, object)| ObjectInfo {
                key: key.clone(),
                modified: SystemTime::UNIX_EPOCH
                    .checked_add(Duration::from_millis(object.modified_ms)),
            })
            .collect())
    }

    pub(crate) async fn delete(&self, key: &str) -> Result<(), IndexError> {
        let fault = self.decide(ObjectOp::Delete, key).await;
        if fault == ObjectFault::Fail {
            return Err(injected(ObjectOp::Delete, key));
        }
        let mut objects = lock(&self.objects);
        if objects.remove(key).is_some() {
            self.notify(AppliedChange::Delete { key });
        }
        drop(objects);
        if fault == ObjectFault::Ambiguous {
            return Err(injected(ObjectOp::Delete, key));
        }
        Ok(())
    }
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

    impl MemoryStoreHooks for Script {
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
        let store = MemoryObjectStore::new(clock.clone()).with_hooks(script.clone());
        let as_object = ObjectStore::from(store.clone());

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
        assert_eq!(store.without_hooks().snapshot().len(), 2);
    }
}

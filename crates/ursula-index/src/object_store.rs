use std::fs;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;
use std::io::Write;
use std::ops::Range;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
#[cfg(test)]
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::SystemTime;

use futures_util::FutureExt;
use futures_util::future::BoxFuture;
use opendal::ErrorKind;
use opendal::Operator;

use crate::IndexError;

#[derive(Clone, Debug)]
pub(crate) struct StoredObject {
    pub bytes: Vec<u8>,
    pub etag: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ConditionalWrite {
    Written,
    Conflict,
}

#[derive(Clone, Debug)]
pub(crate) struct ObjectInfo {
    pub key: String,
    pub modified: Option<SystemTime>,
}

/// One object-store operation, as seen by an [`ObjectFaults`] hook.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ObjectOp {
    /// A whole-object read (an S3 HEAD plus a conditional GET).
    Get,
    /// A ranged read (one GET).
    GetRange,
    /// An entity-tag lookup (one HEAD).
    Head,
    /// A put-if-absent or compare-and-swap write (one PUT).
    Put,
    /// A recursive listing (one LIST per page).
    List,
    /// A delete (one DELETE).
    Delete,
}

/// Fault injection for object-store operations (crash-injection tests):
/// consulted before every operation of a store built with
/// [`ObjectStore::with_faults`]; an error fails the operation without
/// performing it.
pub trait ObjectFaults: Send + Sync {
    /// Called before `op` on `key` (the prefix for [`ObjectOp::List`]).
    fn check(&self, op: ObjectOp, key: &str) -> Result<(), IndexError>;
}

/// Object-store requests by S3 request class, counted as the S3 backend
/// issues them: a whole-object read is a HEAD and a GET, a ranged read a
/// GET, a conditional write a PUT, a listing a LIST (per call), a delete a
/// DELETE.
#[derive(Debug, Default)]
pub struct ObjectRequestCounters {
    get: AtomicU64,
    head: AtomicU64,
    put: AtomicU64,
    list: AtomicU64,
    delete: AtomicU64,
}

/// A snapshot of [`ObjectRequestCounters`].
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, serde::Serialize)]
pub struct ObjectRequestCounts {
    /// GET requests.
    pub get: u64,
    /// HEAD requests.
    pub head: u64,
    /// PUT requests.
    pub put: u64,
    /// LIST requests.
    pub list: u64,
    /// DELETE requests.
    pub delete: u64,
}

impl ObjectRequestCounts {
    /// PUT-class requests (PUT, COPY, POST, LIST).
    pub fn put_class(&self) -> u64 {
        self.put.saturating_add(self.list)
    }

    /// GET-class requests (GET, HEAD and the rest).
    pub fn get_class(&self) -> u64 {
        self.get.saturating_add(self.head)
    }
}

impl ObjectRequestCounters {
    fn record(&self, op: ObjectOp) {
        let bump = |counter: &AtomicU64| {
            counter.fetch_add(1, Ordering::Relaxed);
        };
        match op {
            ObjectOp::Get => {
                bump(&self.head);
                bump(&self.get);
            }
            ObjectOp::GetRange => bump(&self.get),
            ObjectOp::Head => bump(&self.head),
            ObjectOp::Put => bump(&self.put),
            ObjectOp::List => bump(&self.list),
            ObjectOp::Delete => bump(&self.delete),
        }
    }

    /// The counts so far.
    pub fn snapshot(&self) -> ObjectRequestCounts {
        ObjectRequestCounts {
            get: self.get.load(Ordering::Relaxed),
            head: self.head.load(Ordering::Relaxed),
            put: self.put.load(Ordering::Relaxed),
            list: self.list.load(Ordering::Relaxed),
            delete: self.delete.load(Ordering::Relaxed),
        }
    }
}

/// A store whose requests are counted and, optionally, fault-injected.
pub struct ObservedStore {
    inner: ObjectStore,
    counters: Option<Arc<ObjectRequestCounters>>,
    faults: Option<Arc<dyn ObjectFaults>>,
}

impl ObservedStore {
    fn before(&self, op: ObjectOp, key: &str) -> Result<(), IndexError> {
        if let Some(faults) = &self.faults {
            faults.check(op, key)?;
        }
        if let Some(counters) = &self.counters {
            counters.record(op);
        }
        Ok(())
    }
}

/// A conditional-write object backend. Opaque outside this crate: construct
/// one via [`From`] on [`FsObjectStore`] or [`S3ObjectStore`] and hand it to
/// [`crate::EventIndex::open`] or [`crate::IndexCatalog::new`].
#[derive(Clone)]
pub enum ObjectStore {
    Fs(FsObjectStore),
    S3(S3ObjectStore),
    /// A counted and possibly fault-injected view of another store.
    Observed(Arc<ObservedStore>),
}

impl ObjectStore {
    /// This store with its requests also counted in `counters`.
    pub fn counted(self, counters: Arc<ObjectRequestCounters>) -> Self {
        Self::Observed(Arc::new(ObservedStore {
            inner: self,
            counters: Some(counters),
            faults: None,
        }))
    }

    /// This store with every operation first checked by `faults`.
    pub fn with_faults(self, faults: Arc<dyn ObjectFaults>) -> Self {
        Self::Observed(Arc::new(ObservedStore {
            inner: self,
            counters: None,
            faults: Some(faults),
        }))
    }

    pub(crate) fn get<'a>(
        &'a self,
        key: &'a str,
    ) -> BoxFuture<'a, Result<Option<StoredObject>, IndexError>> {
        async move {
            match self {
                Self::Fs(store) => store.get(key),
                Self::S3(store) => store.get(key).await,
                Self::Observed(store) => {
                    store.before(ObjectOp::Get, key)?;
                    store.inner.get(key).await
                }
            }
        }
        .boxed()
    }

    /// The entity tag of `key`, or `None` when it is absent.
    pub(crate) fn head<'a>(
        &'a self,
        key: &'a str,
    ) -> BoxFuture<'a, Result<Option<String>, IndexError>> {
        async move {
            match self {
                Self::Fs(store) => Ok(store.get(key)?.map(|object| object.etag)),
                Self::S3(store) => store.head(key).await,
                Self::Observed(store) => {
                    store.before(ObjectOp::Head, key)?;
                    store.inner.head(key).await
                }
            }
        }
        .boxed()
    }

    pub(crate) fn get_range<'a>(
        &'a self,
        key: &'a str,
        range: Range<u64>,
    ) -> BoxFuture<'a, Result<Option<Vec<u8>>, IndexError>> {
        async move {
            match self {
                Self::Fs(store) => store.get_range(key, range),
                Self::S3(store) => store.get_range(key, range).await,
                Self::Observed(store) => {
                    store.before(ObjectOp::GetRange, key)?;
                    store.inner.get_range(key, range).await
                }
            }
        }
        .boxed()
    }

    pub(crate) fn put_if_absent<'a>(
        &'a self,
        key: &'a str,
        bytes: &'a [u8],
    ) -> BoxFuture<'a, Result<ConditionalWrite, IndexError>> {
        async move {
            match self {
                Self::Fs(store) => store.put_if_absent(key, bytes),
                Self::S3(store) => store.put_if_absent(key, bytes).await,
                Self::Observed(store) => {
                    store.before(ObjectOp::Put, key)?;
                    store.inner.put_if_absent(key, bytes).await
                }
            }
        }
        .boxed()
    }

    pub(crate) fn compare_and_swap<'a>(
        &'a self,
        key: &'a str,
        expected_etag: &'a str,
        bytes: &'a [u8],
    ) -> BoxFuture<'a, Result<ConditionalWrite, IndexError>> {
        async move {
            match self {
                Self::Fs(store) => store.compare_and_swap(key, expected_etag, bytes),
                Self::S3(store) => store.compare_and_swap(key, expected_etag, bytes).await,
                Self::Observed(store) => {
                    store.before(ObjectOp::Put, key)?;
                    store
                        .inner
                        .compare_and_swap(key, expected_etag, bytes)
                        .await
                }
            }
        }
        .boxed()
    }

    pub(crate) fn list<'a>(
        &'a self,
        prefix: &'a str,
    ) -> BoxFuture<'a, Result<Vec<ObjectInfo>, IndexError>> {
        async move {
            match self {
                Self::Fs(store) => store.list(prefix),
                Self::S3(store) => store.list(prefix).await,
                Self::Observed(store) => {
                    store.before(ObjectOp::List, prefix)?;
                    store.inner.list(prefix).await
                }
            }
        }
        .boxed()
    }

    pub(crate) fn delete<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<(), IndexError>> {
        async move {
            match self {
                Self::Fs(store) => store.delete(key),
                Self::S3(store) => store.delete(key).await,
                Self::Observed(store) => {
                    store.before(ObjectOp::Delete, key)?;
                    store.inner.delete(key).await
                }
            }
        }
        .boxed()
    }

    /// Delete every object in this store's configured namespace.
    pub async fn delete_all(&self) -> Result<usize, IndexError> {
        let objects = self.list("").await?;
        let count = objects.len();
        for object in objects {
            self.delete(&object.key).await?;
        }
        Ok(count)
    }
}

#[derive(Clone, Debug)]
pub struct FsObjectStore {
    root: PathBuf,
    #[cfg(test)]
    range_reads: Arc<AtomicUsize>,
    #[cfg(test)]
    range_read_bytes: Arc<AtomicU64>,
}

impl FsObjectStore {
    pub fn new(root: impl AsRef<Path>) -> Result<Self, IndexError> {
        let root = root.as_ref().to_path_buf();
        fs::create_dir_all(&root)?;
        Ok(Self {
            root,
            #[cfg(test)]
            range_reads: Arc::new(AtomicUsize::new(0)),
            #[cfg(test)]
            range_read_bytes: Arc::new(AtomicU64::new(0)),
        })
    }

    fn path(&self, key: &str) -> Result<PathBuf, IndexError> {
        if key.is_empty() || key.starts_with('/') || key.split('/').any(|part| part == "..") {
            return Err(IndexError::InvalidObjectKey(key.to_owned()));
        }
        Ok(self.root.join(key))
    }

    fn get(&self, key: &str) -> Result<Option<StoredObject>, IndexError> {
        let path = self.path(key)?;
        match fs::read(path) {
            Ok(bytes) => Ok(Some(StoredObject {
                etag: digest(&bytes),
                bytes,
            })),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    fn get_range(&self, key: &str, range: Range<u64>) -> Result<Option<Vec<u8>>, IndexError> {
        #[cfg(test)]
        self.range_reads.fetch_add(1, Ordering::Relaxed);
        #[cfg(test)]
        self.range_read_bytes
            .fetch_add(range.end.saturating_sub(range.start), Ordering::Relaxed);
        let path = self.path(key)?;
        let mut file = match File::open(path) {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        let length = range
            .end
            .checked_sub(range.start)
            .ok_or_else(|| IndexError::InvalidPartLayout(key.to_owned()))?;
        let length = usize::try_from(length)
            .map_err(|_error| IndexError::InvalidPartLayout(key.to_owned()))?;
        file.seek(SeekFrom::Start(range.start))?;
        let mut bytes = vec![0_u8; length];
        file.read_exact(&mut bytes)?;
        Ok(Some(bytes))
    }

    #[cfg(test)]
    pub(crate) fn range_read_count(&self) -> usize {
        self.range_reads.load(Ordering::Relaxed)
    }

    #[cfg(test)]
    pub(crate) fn range_read_bytes(&self) -> u64 {
        self.range_read_bytes.load(Ordering::Relaxed)
    }

    /// Take the per-key advisory lock, re-check `may_write` under it, then
    /// atomically install `bytes` via a synced temporary file.
    fn write_locked(
        &self,
        key: &str,
        bytes: &[u8],
        lock_extension: &str,
        may_write: impl FnOnce(&Path) -> Result<bool, IndexError>,
    ) -> Result<ConditionalWrite, IndexError> {
        let path = self.path(key)?;
        let parent = path
            .parent()
            .ok_or_else(|| IndexError::InvalidObjectKey(key.to_owned()))?;
        fs::create_dir_all(parent)?;
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(path.with_extension(lock_extension))?;
        lock.lock()?;
        if !may_write(&path)? {
            return Ok(ConditionalWrite::Conflict);
        }
        let temporary = path.with_extension(format!("{}.tmp", digest(bytes)));
        let mut file = File::create(&temporary)?;
        file.write_all(bytes)?;
        file.sync_all()?;
        fs::rename(&temporary, &path)?;
        sync_parent(&path)?;
        Ok(ConditionalWrite::Written)
    }

    fn put_if_absent(&self, key: &str, bytes: &[u8]) -> Result<ConditionalWrite, IndexError> {
        self.write_locked(key, bytes, "create-lock", |path| Ok(!path.exists()))
    }

    fn compare_and_swap(
        &self,
        key: &str,
        expected_etag: &str,
        bytes: &[u8],
    ) -> Result<ConditionalWrite, IndexError> {
        self.write_locked(key, bytes, "cas-lock", |path| match fs::read(path) {
            Ok(current) => Ok(digest(&current) == expected_etag),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(error) => Err(error.into()),
        })
    }

    fn list(&self, prefix: &str) -> Result<Vec<ObjectInfo>, IndexError> {
        let root = if prefix.is_empty() {
            self.root.clone()
        } else {
            self.path(prefix)?
        };
        if !root.exists() {
            return Ok(Vec::new());
        }
        let mut pending = vec![root];
        let mut objects = Vec::new();
        while let Some(directory) = pending.pop() {
            for entry in fs::read_dir(directory)? {
                let entry = entry?;
                let path = entry.path();
                let metadata = entry.metadata()?;
                if metadata.is_dir() {
                    pending.push(path);
                    continue;
                }
                let key = path
                    .strip_prefix(&self.root)
                    .map_err(|_error| IndexError::InvalidObjectKey(prefix.to_owned()))?
                    .to_string_lossy()
                    .replace(std::path::MAIN_SEPARATOR, "/");
                objects.push(ObjectInfo {
                    key,
                    modified: metadata.modified().ok(),
                });
            }
        }
        Ok(objects)
    }

    fn delete(&self, key: &str) -> Result<(), IndexError> {
        match fs::remove_file(self.path(key)?) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error.into()),
        }
    }
}

impl From<FsObjectStore> for ObjectStore {
    fn from(value: FsObjectStore) -> Self {
        Self::Fs(value)
    }
}

#[derive(Clone, Debug)]
pub struct S3ObjectStoreConfig {
    pub bucket: String,
    pub root: String,
    pub region: Option<String>,
    pub endpoint: Option<String>,
}

#[derive(Clone)]
pub struct S3ObjectStore {
    operator: Operator,
}

impl std::fmt::Debug for S3ObjectStore {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("S3ObjectStore")
            .finish_non_exhaustive()
    }
}

impl S3ObjectStore {
    pub fn new(config: S3ObjectStoreConfig) -> Result<Self, IndexError> {
        if config.bucket.is_empty() {
            return Err(IndexError::InvalidConfig("S3 bucket must not be empty"));
        }
        let mut builder = opendal::services::S3::default()
            .bucket(&config.bucket)
            .root(&config.root);
        if let Some(region) = config.region {
            builder = builder.region(&region);
        }
        if let Some(endpoint) = config.endpoint {
            builder = builder.endpoint(&endpoint);
        }
        let operator = Operator::new(builder).map_err(object_error)?.finish();
        Ok(Self { operator })
    }

    async fn get(&self, key: &str) -> Result<Option<StoredObject>, IndexError> {
        for _attempt in 0..3 {
            let metadata = match self.operator.stat(key).await {
                Ok(metadata) => metadata,
                Err(error) if error.kind() == ErrorKind::NotFound => return Ok(None),
                Err(error) => return Err(object_error(error)),
            };
            let etag = metadata
                .etag()
                .ok_or_else(|| IndexError::MissingEtag(key.to_owned()))?
                .to_owned();
            match self.operator.read_with(key).if_match(&etag).await {
                Ok(bytes) => {
                    return Ok(Some(StoredObject {
                        bytes: bytes.to_vec(),
                        etag,
                    }));
                }
                Err(error) if error.kind() == ErrorKind::ConditionNotMatch => continue,
                Err(error) if error.kind() == ErrorKind::NotFound => continue,
                Err(error) => return Err(object_error(error)),
            }
        }
        Err(IndexError::ObjectStore(format!(
            "object `{key}` changed during three consecutive reads"
        )))
    }

    async fn head(&self, key: &str) -> Result<Option<String>, IndexError> {
        match self.operator.stat(key).await {
            Ok(metadata) => metadata
                .etag()
                .map(|etag| Some(etag.to_owned()))
                .ok_or_else(|| IndexError::MissingEtag(key.to_owned())),
            Err(error) if error.kind() == ErrorKind::NotFound => Ok(None),
            Err(error) => Err(object_error(error)),
        }
    }

    async fn get_range(&self, key: &str, range: Range<u64>) -> Result<Option<Vec<u8>>, IndexError> {
        match self.operator.read_with(key).range(range).await {
            Ok(bytes) => Ok(Some(bytes.to_vec())),
            Err(error) if error.kind() == ErrorKind::NotFound => Ok(None),
            Err(error) => Err(object_error(error)),
        }
    }

    async fn put_if_absent(&self, key: &str, bytes: &[u8]) -> Result<ConditionalWrite, IndexError> {
        match self
            .operator
            .write_with(key, bytes.to_vec())
            .if_not_exists(true)
            .await
        {
            Ok(()) => Ok(ConditionalWrite::Written),
            Err(error) if error.kind() == ErrorKind::ConditionNotMatch => {
                Ok(ConditionalWrite::Conflict)
            }
            Err(error) => Err(object_error(error)),
        }
    }

    async fn compare_and_swap(
        &self,
        key: &str,
        expected_etag: &str,
        bytes: &[u8],
    ) -> Result<ConditionalWrite, IndexError> {
        match self
            .operator
            .write_with(key, bytes.to_vec())
            .if_match(expected_etag)
            .await
        {
            Ok(()) => Ok(ConditionalWrite::Written),
            Err(error) if error.kind() == ErrorKind::ConditionNotMatch => {
                Ok(ConditionalWrite::Conflict)
            }
            Err(error) => Err(object_error(error)),
        }
    }

    async fn list(&self, prefix: &str) -> Result<Vec<ObjectInfo>, IndexError> {
        let entries = self
            .operator
            .list_with(prefix)
            .recursive(true)
            .await
            .map_err(object_error)?;
        let mut objects = Vec::new();
        for entry in entries {
            if !entry.metadata().mode().is_file() {
                continue;
            }
            let modified = match entry.metadata().last_modified() {
                Some(modified) => Some(modified.into()),
                None => self
                    .operator
                    .stat(entry.path())
                    .await
                    .map_err(object_error)?
                    .last_modified()
                    .map(Into::into),
            };
            objects.push(ObjectInfo {
                key: entry.path().to_owned(),
                modified,
            });
        }
        Ok(objects)
    }

    async fn delete(&self, key: &str) -> Result<(), IndexError> {
        self.operator.delete(key).await.map_err(object_error)
    }
}

impl From<S3ObjectStore> for ObjectStore {
    fn from(value: S3ObjectStore) -> Self {
        Self::S3(value)
    }
}

pub(crate) fn digest(bytes: &[u8]) -> String {
    blake3::hash(bytes).to_hex().to_string()
}

fn sync_parent(path: &Path) -> Result<(), IndexError> {
    if let Some(parent) = path.parent() {
        File::open(parent)?.sync_all()?;
    }
    Ok(())
}

fn object_error(error: opendal::Error) -> IndexError {
    IndexError::ObjectStore(error.to_string())
}

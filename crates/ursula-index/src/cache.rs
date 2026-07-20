use std::collections::HashMap;
use std::fs;
use std::fs::File;
use std::io::Write;
use std::ops::Deref;
use std::ops::Range;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::Weak;
use std::time::SystemTime;

use bytes::Bytes;
use foyer::BlockEngineConfig;
use foyer::Cache;
use foyer::CacheBuilder;
use foyer::DeviceBuilder;
use foyer::FsDeviceBuilder;
use foyer::HybridCache;
use foyer::HybridCacheBuilder;
use futures_util::FutureExt;
use futures_util::future::BoxFuture;
use parquet::arrow::arrow_reader::ArrowReaderOptions;
use parquet::arrow::async_reader::AsyncFileReader;
use parquet::errors::ParquetError;
use parquet::file::metadata::ParquetMetaData;
use parquet::file::metadata::ParquetMetaDataReader;
use tokio::sync::Mutex as AsyncMutex;
use tokio::sync::OnceCell;

use crate::IndexError;
use crate::manifest::PartMeta;
use crate::object_store::ObjectStore;
use crate::object_store::digest;
use crate::part;

const MIN_CACHE_BYTES: u64 = 16 * 1024 * 1024;

#[derive(Clone, Debug)]
pub(crate) struct LocalCache {
    root: PathBuf,
    max_bytes: u64,
    pins: Arc<Mutex<HashMap<PathBuf, usize>>>,
    budget: Arc<AsyncMutex<CacheBudget>>,
    installs: Arc<AsyncMutex<HashMap<PathBuf, Weak<AsyncMutex<()>>>>>,
}

#[derive(Debug, Default)]
struct CacheBudget {
    reserved_bytes: u64,
}

pub(crate) struct MaterializedPart {
    path: PathBuf,
    pins: Arc<Mutex<HashMap<PathBuf, usize>>>,
}

impl Deref for MaterializedPart {
    type Target = Path;

    fn deref(&self) -> &Self::Target {
        &self.path
    }
}

impl Drop for MaterializedPart {
    fn drop(&mut self) {
        let Ok(mut pins) = self.pins.lock() else {
            return;
        };
        let Some(count) = pins.get_mut(&self.path) else {
            return;
        };
        *count = count.saturating_sub(1);
        if *count == 0 {
            pins.remove(&self.path);
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) enum IndexCaches {
    Serving {
        parts: LocalCache,
        ranges: VerifiedRangeCache,
    },
    Maintenance {
        parts: LocalCache,
    },
}

#[derive(Clone, Debug)]
pub(crate) struct VerifiedRangeCache {
    root: PathBuf,
    max_bytes: u64,
    memory_bytes: usize,
    cache: Arc<OnceCell<HybridCache<String, Bytes>>>,
    layouts: Cache<String, Arc<part::PartLayout>>,
}

pub(crate) struct VerifiedParquetReader {
    store: ObjectStore,
    cache: VerifiedRangeCache,
    layout: Arc<part::PartLayout>,
}

#[derive(Debug, thiserror::Error)]
#[error("{0}")]
struct RangeReadError(IndexError);

#[derive(Clone, Debug)]
pub struct EventIndexCache(pub(crate) IndexCaches);

impl EventIndexCache {
    pub fn serving(root: impl AsRef<Path>, max_bytes: u64) -> Result<Self, IndexError> {
        Self::check_budget(max_bytes)?;
        let root = root.as_ref();
        let range_bytes = max_bytes.saturating_mul(3) / 4;
        let part_bytes = max_bytes.saturating_sub(range_bytes);
        Ok(Self(IndexCaches::Serving {
            parts: LocalCache::new(root.join("parts"), part_bytes)?,
            ranges: VerifiedRangeCache::new(root.join("ranges"), range_bytes)?,
        }))
    }

    pub fn maintenance(root: impl AsRef<Path>, max_bytes: u64) -> Result<Self, IndexError> {
        Self::check_budget(max_bytes)?;
        Ok(Self(IndexCaches::Maintenance {
            parts: LocalCache::new(root, max_bytes)?,
        }))
    }

    fn check_budget(max_bytes: u64) -> Result<(), IndexError> {
        if max_bytes < MIN_CACHE_BYTES {
            return Err(IndexError::InvalidConfig(
                "cache_max_bytes must be at least 16 MiB",
            ));
        }
        Ok(())
    }
}

impl IndexCaches {
    pub(crate) fn parts(&self) -> &LocalCache {
        match self {
            Self::Serving { parts, .. } | Self::Maintenance { parts } => parts,
        }
    }

    pub(crate) fn ranges(&self) -> Result<&VerifiedRangeCache, IndexError> {
        match self {
            Self::Serving { ranges, .. } => Ok(ranges),
            Self::Maintenance { .. } => Err(IndexError::InvalidConfig(
                "maintenance cache cannot serve queries",
            )),
        }
    }
}

impl VerifiedRangeCache {
    fn new(root: PathBuf, max_bytes: u64) -> Result<Self, IndexError> {
        fs::create_dir_all(&root)?;
        let total_memory_bytes = max_bytes
            .saturating_div(8)
            .clamp(1024 * 1024, 128 * 1024 * 1024);
        let layout_memory_bytes = total_memory_bytes.saturating_div(8);
        let range_memory_bytes = total_memory_bytes.saturating_sub(layout_memory_bytes);
        let layout_memory_bytes = usize::try_from(layout_memory_bytes)
            .map_err(|_error| IndexError::InvalidConfig("range cache is too large"))?;
        let memory_bytes = usize::try_from(range_memory_bytes)
            .map_err(|_error| IndexError::InvalidConfig("range cache is too large"))?;
        let layouts = CacheBuilder::new(layout_memory_bytes)
            .with_weighter(|key: &String, layout: &Arc<part::PartLayout>| {
                key.len().saturating_add(
                    layout
                        .units
                        .len()
                        .saturating_mul(std::mem::size_of::<part::PartUnit>()),
                )
            })
            .build();
        Ok(Self {
            root,
            max_bytes,
            memory_bytes,
            cache: Arc::new(OnceCell::new()),
            layouts,
        })
    }

    async fn cache(&self) -> Result<&HybridCache<String, Bytes>, IndexError> {
        self.cache
            .get_or_try_init(|| async {
                let storage_bytes = usize::try_from(self.max_bytes)
                    .map_err(|_error| IndexError::InvalidConfig("range cache is too large"))?;
                let device = FsDeviceBuilder::new(&self.root)
                    .with_capacity(storage_bytes)
                    .build()
                    .map_err(|error| IndexError::ObjectStore(error.to_string()))?;
                HybridCacheBuilder::new()
                    .with_name("ursula-index-parquet-ranges")
                    .with_flush_on_close(false)
                    .memory(self.memory_bytes)
                    .with_weighter(|key: &String, value: &Bytes| {
                        key.len().saturating_add(value.len())
                    })
                    .storage()
                    .with_engine_config(BlockEngineConfig::new(device))
                    .build()
                    .await
                    .map_err(|error| IndexError::ObjectStore(error.to_string()))
            })
            .await
    }

    pub(crate) async fn layout(
        &self,
        store: &ObjectStore,
        meta: &PartMeta,
    ) -> Result<Arc<part::PartLayout>, IndexError> {
        if let Some(layout) = self.layouts.get(&meta.layout_key) {
            return Ok(Arc::clone(layout.value()));
        }
        let object = store
            .get(&meta.layout_key)
            .await?
            .ok_or_else(|| IndexError::MissingObject(meta.layout_key.clone()))?;
        validate_content_addressed_object(&meta.layout_key, "layouts/", ".json", &object.bytes)?;
        let layout: part::PartLayout = serde_json::from_slice(&object.bytes)?;
        validate_layout(meta, &layout)?;
        let layout = Arc::new(layout);
        Ok(Arc::clone(
            self.layouts.insert(meta.layout_key.clone(), layout).value(),
        ))
    }

    async fn read_unit(
        &self,
        store: &ObjectStore,
        part_key: &str,
        unit: &part::PartUnit,
    ) -> Result<Bytes, IndexError> {
        let cache_key = format!("{}:{}-{}:{}", part_key, unit.start, unit.end, unit.hash);
        let fetch_store = store.clone();
        let fetch_key = part_key.to_owned();
        let fetch_unit = unit.clone();
        let entry = self
            .cache()
            .await?
            .get_or_fetch(&cache_key, move || async move {
                let bytes = fetch_store
                    .get_range(&fetch_key, fetch_unit.start..fetch_unit.end)
                    .await
                    .map_err(RangeReadError)?
                    .ok_or_else(|| RangeReadError(IndexError::MissingObject(fetch_key.clone())))?;
                if digest(&bytes) != fetch_unit.hash {
                    return Err(RangeReadError(IndexError::ObjectHashMismatch(fetch_key)));
                }
                Ok::<_, RangeReadError>(Bytes::from(bytes))
            })
            .await
            .map_err(|error| IndexError::ObjectStore(error.to_string()))?;
        let bytes = Bytes::clone(entry.value());
        if digest(&bytes) != unit.hash {
            return Err(IndexError::ObjectHashMismatch(part_key.to_owned()));
        }
        Ok(bytes)
    }
}

impl VerifiedParquetReader {
    pub(crate) fn new(
        store: ObjectStore,
        cache: VerifiedRangeCache,
        layout: Arc<part::PartLayout>,
    ) -> Self {
        Self {
            store,
            cache,
            layout,
        }
    }

    async fn read(&self, range: Range<u64>) -> Result<Bytes, IndexError> {
        if range.start > range.end || range.end > self.layout.bytes {
            return Err(IndexError::InvalidPartLayout(self.layout.part_key.clone()));
        }
        if range.is_empty() {
            return Ok(Bytes::new());
        }
        let capacity = usize::try_from(range.end.saturating_sub(range.start))
            .map_err(|_error| IndexError::InvalidPartLayout(self.layout.part_key.clone()))?;
        let mut result = Vec::with_capacity(capacity);
        for unit in self
            .layout
            .units
            .iter()
            .filter(|unit| unit.end > range.start && unit.start < range.end)
        {
            let bytes = self
                .cache
                .read_unit(&self.store, &self.layout.part_key, unit)
                .await?;
            let slice_start = usize::try_from(range.start.saturating_sub(unit.start))
                .map_err(|_error| IndexError::InvalidPartLayout(self.layout.part_key.clone()))?;
            let slice_end = usize::try_from(range.end.min(unit.end).saturating_sub(unit.start))
                .map_err(|_error| IndexError::InvalidPartLayout(self.layout.part_key.clone()))?;
            let slice = bytes
                .get(slice_start..slice_end)
                .ok_or_else(|| IndexError::InvalidPartLayout(self.layout.part_key.clone()))?;
            result.extend_from_slice(slice);
        }
        if result.len() != capacity {
            return Err(IndexError::InvalidPartLayout(self.layout.part_key.clone()));
        }
        Ok(Bytes::from(result))
    }
}

impl AsyncFileReader for VerifiedParquetReader {
    fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, parquet::errors::Result<Bytes>> {
        async move {
            self.read(range)
                .await
                .map_err(|error| ParquetError::External(Box::new(error)))
        }
        .boxed()
    }

    fn get_byte_ranges(
        &mut self,
        ranges: Vec<Range<u64>>,
    ) -> BoxFuture<'_, parquet::errors::Result<Vec<Bytes>>> {
        async move {
            let reader = &*self;
            futures_util::future::try_join_all(ranges.into_iter().map(|range| async move {
                reader
                    .read(range)
                    .await
                    .map_err(|error| ParquetError::External(Box::new(error)))
            }))
            .await
        }
        .boxed()
    }

    fn get_metadata<'a>(
        &'a mut self,
        options: Option<&'a ArrowReaderOptions>,
    ) -> BoxFuture<'a, parquet::errors::Result<Arc<ParquetMetaData>>> {
        async move {
            let metadata_options = options.map(|options| options.metadata_options().clone());
            let mut reader = ParquetMetaDataReader::new().with_metadata_options(metadata_options);
            if let Some(options) = options {
                reader = reader
                    .with_column_index_policy(options.column_index_policy())
                    .with_offset_index_policy(options.offset_index_policy());
            }
            let file_bytes = self.layout.bytes;
            reader
                .load_and_finish(&mut *self, file_bytes)
                .await
                .map(Arc::new)
        }
        .boxed()
    }
}

impl LocalCache {
    fn new(root: impl AsRef<Path>, max_bytes: u64) -> Result<Self, IndexError> {
        if max_bytes == 0 {
            return Err(IndexError::InvalidConfig(
                "cache_max_bytes must be positive",
            ));
        }
        let root = root.as_ref().to_path_buf();
        fs::create_dir_all(&root)?;
        Ok(Self {
            root,
            max_bytes,
            pins: Arc::new(Mutex::new(HashMap::new())),
            budget: Arc::new(AsyncMutex::new(CacheBudget::default())),
            installs: Arc::new(AsyncMutex::new(HashMap::new())),
        })
    }

    /// Directory that holds cached whole parts; also used for scratch files
    /// that must live on the same filesystem.
    pub(crate) fn directory(&self) -> &Path {
        &self.root
    }

    pub(crate) async fn materialize(
        &self,
        store: &ObjectStore,
        meta: &PartMeta,
    ) -> Result<MaterializedPart, IndexError> {
        fs::create_dir_all(&self.root)?;
        let path = self
            .root
            .join(format!("{}.parquet", digest(meta.key.as_bytes())));
        if let Some(pinned) = self.pinned_if_valid(&path, meta).await? {
            return Ok(pinned);
        }
        let install_lock = self.install_lock(&path).await;
        let _install_guard = install_lock.lock().await;
        if let Some(pinned) = self.pinned_if_valid(&path, meta).await? {
            return Ok(pinned);
        }
        let object = store
            .get(&meta.key)
            .await?
            .ok_or_else(|| IndexError::MissingObject(meta.key.clone()))?;
        let object_key = meta.key.clone();
        let expected_bytes = meta.bytes;
        let bytes = tokio::task::spawn_blocking(move || {
            validate_downloaded_part(&object_key, expected_bytes, &object.bytes)?;
            Ok::<_, IndexError>(object.bytes)
        })
        .await
        .map_err(|_error| IndexError::WorkerFailed)??;
        self.reserve(meta.bytes, &path).await?;
        let installation_pin = match self.pin(path.clone()) {
            Ok(pin) => pin,
            Err(error) => {
                self.release_reservation(meta.bytes).await;
                return Err(error);
            }
        };
        let root = self.root.clone();
        let install_path = path.clone();
        let install = match tokio::task::spawn_blocking(move || {
            let temporary = root.join(format!("{}.tmp", digest(&bytes)));
            let result = (|| -> Result<(), IndexError> {
                let mut file = File::create(&temporary)?;
                file.write_all(&bytes)?;
                file.sync_all()?;
                fs::rename(&temporary, &install_path)?;
                part::validate(&install_path)?;
                Ok(())
            })();
            if result.is_err() {
                let _remove = fs::remove_file(&temporary);
                let _remove = fs::remove_file(&install_path);
            }
            result
        })
        .await
        {
            Ok(result) => result,
            Err(_error) => Err(IndexError::WorkerFailed),
        };
        match install {
            Ok(()) => {
                self.release_reservation(meta.bytes).await;
                Ok(installation_pin)
            }
            Err(error) => {
                drop(installation_pin);
                self.release_reservation(meta.bytes).await;
                Err(error)
            }
        }
    }

    /// Pin the cached file and return the pin only while the file validates
    /// against `meta`.
    async fn pinned_if_valid(
        &self,
        path: &Path,
        meta: &PartMeta,
    ) -> Result<Option<MaterializedPart>, IndexError> {
        let pinned = self.pin(path.to_path_buf())?;
        if validate_cached_part(path.to_path_buf(), meta.clone()).await? {
            return Ok(Some(pinned));
        }
        drop(pinned);
        Ok(None)
    }

    fn pin(&self, path: PathBuf) -> Result<MaterializedPart, IndexError> {
        let mut pins = self
            .pins
            .lock()
            .map_err(|_error| IndexError::LockPoisoned)?;
        Ok(pin_locked(&mut pins, path, &self.pins))
    }

    async fn install_lock(&self, path: &Path) -> Arc<AsyncMutex<()>> {
        let mut installs = self.installs.lock().await;
        installs.retain(|_path, install| install.strong_count() > 0);
        if let Some(install) = installs.get(path).and_then(Weak::upgrade) {
            return install;
        }
        let install = Arc::new(AsyncMutex::new(()));
        installs.insert(path.to_path_buf(), Arc::downgrade(&install));
        install
    }

    async fn reserve(&self, incoming: u64, protected: &Path) -> Result<(), IndexError> {
        if incoming > self.max_bytes {
            return Err(IndexError::CacheCapacity {
                capacity: self.max_bytes,
                object_size: incoming,
            });
        }
        let mut budget = self.budget.lock().await;
        let pins = Arc::clone(&self.pins);
        let root = self.root.clone();
        let max_bytes = self.max_bytes;
        let reserved_bytes = budget.reserved_bytes;
        let protected = protected.to_path_buf();
        tokio::task::spawn_blocking(move || {
            make_cache_room(
                &root,
                max_bytes,
                incoming,
                reserved_bytes,
                Some(&protected),
                &pins,
            )
        })
        .await
        .map_err(|_error| IndexError::WorkerFailed)??;
        budget.reserved_bytes = budget.reserved_bytes.saturating_add(incoming);
        Ok(())
    }

    async fn release_reservation(&self, bytes: u64) {
        let mut budget = self.budget.lock().await;
        budget.reserved_bytes = budget.reserved_bytes.saturating_sub(bytes);
    }
}

async fn validate_cached_part(path: PathBuf, meta: PartMeta) -> Result<bool, IndexError> {
    tokio::task::spawn_blocking(move || valid_cached_part(&path, &meta))
        .await
        .map_err(|_error| IndexError::WorkerFailed)?
}

fn valid_cached_part(path: &Path, meta: &PartMeta) -> Result<bool, IndexError> {
    let metadata = match fs::metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(error.into()),
    };
    if metadata.len() != meta.bytes {
        fs::remove_file(path)?;
        return Ok(false);
    }
    let expected_hash = meta
        .key
        .strip_prefix("parts/")
        .and_then(|value| value.strip_suffix(".parquet"))
        .ok_or_else(|| IndexError::InvalidObjectKey(meta.key.clone()))?;
    if digest(&fs::read(path)?) != expected_hash {
        fs::remove_file(path)?;
        return Ok(false);
    }
    match part::validate(path) {
        Ok(()) => Ok(true),
        Err(_) => {
            fs::remove_file(path)?;
            Ok(false)
        }
    }
}

fn validate_downloaded_part(
    key: &str,
    expected_bytes: u64,
    bytes: &[u8],
) -> Result<(), IndexError> {
    if u64::try_from(bytes.len()).ok() != Some(expected_bytes) {
        return Err(IndexError::PartSizeMismatch {
            file: key.to_owned(),
            expected: expected_bytes,
            actual: u64::try_from(bytes.len()).unwrap_or(u64::MAX),
        });
    }
    validate_content_addressed_object(key, "parts/", ".parquet", bytes)
}

fn validate_content_addressed_object(
    key: &str,
    prefix: &str,
    suffix: &str,
    bytes: &[u8],
) -> Result<(), IndexError> {
    let expected_hash = key
        .strip_prefix(prefix)
        .and_then(|value| value.strip_suffix(suffix))
        .ok_or_else(|| IndexError::InvalidObjectKey(key.to_owned()))?;
    if digest(bytes) != expected_hash {
        return Err(IndexError::ObjectHashMismatch(key.to_owned()));
    }
    Ok(())
}

fn validate_layout(meta: &PartMeta, layout: &part::PartLayout) -> Result<(), IndexError> {
    if layout.version != 1
        || layout.part_key != meta.key
        || layout.bytes != meta.bytes
        || layout.units.is_empty()
    {
        return Err(IndexError::InvalidPartLayout(meta.key.clone()));
    }
    let mut cursor = 0_u64;
    for unit in &layout.units {
        if unit.start != cursor || unit.start >= unit.end || unit.end > layout.bytes {
            return Err(IndexError::InvalidPartLayout(meta.key.clone()));
        }
        cursor = unit.end;
    }
    if cursor != layout.bytes {
        return Err(IndexError::InvalidPartLayout(meta.key.clone()));
    }
    Ok(())
}

fn make_cache_room(
    root: &Path,
    max_bytes: u64,
    incoming: u64,
    reserved: u64,
    protected: Option<&Path>,
    pins: &Arc<Mutex<HashMap<PathBuf, usize>>>,
) -> Result<(), IndexError> {
    let mut files = Vec::new();
    let mut total = 0_u64;
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        let path = entry.path();
        if !path.is_file() || protected.is_some_and(|protected| protected == path) {
            continue;
        }
        let metadata = entry.metadata()?;
        total = total.saturating_add(metadata.len());
        if pins
            .lock()
            .map_err(|_error| IndexError::LockPoisoned)?
            .contains_key(&path)
        {
            continue;
        }
        files.push((
            metadata.modified().unwrap_or(SystemTime::UNIX_EPOCH),
            metadata.len(),
            path,
        ));
    }
    files.sort_unstable_by_key(|(modified, _, _)| *modified);
    for (_, bytes, path) in files {
        if total.saturating_add(reserved).saturating_add(incoming) <= max_bytes {
            break;
        }
        if remove_cache_file_if_unpinned(&path, pins)? {
            total = total.saturating_sub(bytes);
        }
    }
    if total.saturating_add(reserved).saturating_add(incoming) > max_bytes {
        return Err(IndexError::CacheCapacity {
            capacity: max_bytes,
            object_size: incoming,
        });
    }
    Ok(())
}

fn remove_cache_file_if_unpinned(
    path: &Path,
    pins: &Arc<Mutex<HashMap<PathBuf, usize>>>,
) -> Result<bool, IndexError> {
    let pins = pins.lock().map_err(|_error| IndexError::LockPoisoned)?;
    if pins.contains_key(path) {
        return Ok(false);
    }
    match fs::remove_file(path) {
        Ok(()) => Ok(true),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error.into()),
    }
}

fn pin_locked(
    pins: &mut HashMap<PathBuf, usize>,
    path: PathBuf,
    shared_pins: &Arc<Mutex<HashMap<PathBuf, usize>>>,
) -> MaterializedPart {
    let count = pins.entry(path.clone()).or_default();
    *count = count.saturating_add(1);
    MaterializedPart {
        path,
        pins: Arc::clone(shared_pins),
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use crate::EventIndexCache;
    use crate::IndexError;
    use crate::cache::LocalCache;
    use crate::cache::VerifiedRangeCache;
    use crate::cache::pin_locked;
    use crate::cache::remove_cache_file_if_unpinned;
    use crate::object_store::ConditionalWrite;
    use crate::object_store::FsObjectStore;
    use crate::object_store::ObjectStore;
    use crate::object_store::digest;
    use crate::part::PartUnit;

    #[tokio::test]
    #[expect(
        clippy::panic_in_result_fn,
        reason = "the test combines fallible setup with assertions"
    )]
    async fn shared_cache_never_evicts_a_pinned_part_or_exceeds_its_budget() -> anyhow::Result<()> {
        let directory = tempfile::TempDir::new()?;
        let cache = LocalCache::new(directory.path(), 10)?;
        let path = directory.path().join("part.parquet");
        fs::write(&path, [0_u8; 6])?;
        let guard = {
            let mut pins = cache
                .pins
                .lock()
                .map_err(|_error| anyhow::anyhow!("cache pin lock poisoned"))?;
            pin_locked(&mut pins, path.clone(), &cache.pins)
        };
        let incoming = directory.path().join("incoming.parquet");
        let error = cache
            .reserve(6, &incoming)
            .await
            .expect_err("a pinned part must not be evicted to exceed the cache bound");
        assert!(matches!(error, IndexError::CacheCapacity { .. }));
        assert!(path.exists());

        drop(guard);
        cache.reserve(6, &incoming).await?;
        cache.release_reservation(6).await;
        assert!(!path.exists());
        Ok(())
    }

    #[test]
    #[expect(
        clippy::panic_in_result_fn,
        reason = "the test combines fallible setup with assertions"
    )]
    fn eviction_rechecks_a_new_pin_immediately_before_unlink() -> anyhow::Result<()> {
        let directory = tempfile::TempDir::new()?;
        let cache = LocalCache::new(directory.path(), 10)?;
        let path = directory.path().join("part.parquet");
        fs::write(&path, [0_u8; 6])?;
        let guard = cache.pin(path.clone())?;

        assert!(!remove_cache_file_if_unpinned(&path, &cache.pins)?);
        assert!(path.exists());
        drop(guard);
        assert!(remove_cache_file_if_unpinned(&path, &cache.pins)?);
        assert!(!path.exists());
        Ok(())
    }

    #[tokio::test]
    #[expect(
        clippy::panic_in_result_fn,
        reason = "the test combines fallible setup with assertions"
    )]
    async fn verified_range_cache_deduplicates_fetches_and_rejects_corruption() -> anyhow::Result<()>
    {
        let objects = tempfile::TempDir::new()?;
        let cache_dir = tempfile::TempDir::new()?;
        let fs_store = FsObjectStore::new(objects.path())?;
        let store = ObjectStore::from(fs_store.clone());
        let cache = VerifiedRangeCache::new(cache_dir.path().to_path_buf(), 16 * 1024 * 1024)?;
        let bytes = b"one verified parquet-native unit".to_vec();
        let key = format!("parts/{}.parquet", digest(&bytes));
        assert_eq!(
            store.put_if_absent(&key, &bytes).await?,
            ConditionalWrite::Written
        );
        let unit = PartUnit {
            start: 0,
            end: u64::try_from(bytes.len())?,
            hash: digest(&bytes),
        };
        let (left, right) = tokio::join!(
            cache.read_unit(&store, &key, &unit),
            cache.read_unit(&store, &key, &unit)
        );
        let left = left?;
        assert_eq!(left, bytes);
        let right = right?;
        assert_eq!(right, bytes);
        assert!(std::ptr::eq(left.as_ptr(), right.as_ptr()));
        assert_eq!(fs_store.range_read_count(), 1);

        let corrupt_key = "parts/corrupt.parquet";
        assert_eq!(
            store.put_if_absent(corrupt_key, b"corrupt").await?,
            ConditionalWrite::Written
        );
        let corrupt_unit = PartUnit {
            start: 0,
            end: 7,
            hash: digest(b"expected"),
        };
        let _first_error = cache
            .read_unit(&store, corrupt_key, &corrupt_unit)
            .await
            .expect_err("corrupt bytes must not enter the range cache");
        let _second_error = cache
            .read_unit(&store, corrupt_key, &corrupt_unit)
            .await
            .expect_err("a corrupt failed fetch must not become a cache hit");
        assert_eq!(fs_store.range_read_count(), 3);
        Ok(())
    }

    #[test]
    #[expect(
        clippy::panic_in_result_fn,
        reason = "the test combines fallible setup with assertions"
    )]
    fn cache_roles_assign_the_full_maintenance_budget_to_parts() -> anyhow::Result<()> {
        let serving_dir = tempfile::TempDir::new()?;
        let maintenance_dir = tempfile::TempDir::new()?;
        let serving = EventIndexCache::serving(serving_dir.path(), 16 * 1024 * 1024)?;
        let maintenance = EventIndexCache::maintenance(maintenance_dir.path(), 16 * 1024 * 1024)?;
        assert_eq!(serving.0.parts().max_bytes, 4 * 1024 * 1024);
        assert_eq!(maintenance.0.parts().max_bytes, 16 * 1024 * 1024);
        let _ranges = serving.0.ranges()?;
        let _error = maintenance
            .0
            .ranges()
            .expect_err("maintenance caches must not carry a range cache");
        Ok(())
    }
}

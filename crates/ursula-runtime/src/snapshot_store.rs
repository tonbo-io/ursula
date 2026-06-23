//! Pluggable backends for raft state-machine snapshot bytes.
//!
//! Decouples "what a snapshot contains" from "where the bytes live". The raft
//! state machine asks a [`SnapshotStore`] to persist serialized snapshot bytes
//! and gets back a [`SnapshotLocation`]; only a [`SnapshotPointer`] then rides
//! openraft's `SnapshotData`. The receiver decodes the pointer and pulls the
//! actual bytes back through the same backend.
//!
//! Default backend [`InlineSnapshotStore`] keeps bytes inside the pointer
//! itself, preserving today's "snapshot rides through openraft" behavior.
//! [`LocalSnapshotStore`] persists to the filesystem. The S3 backend is added
//! in a follow-up PR and reuses the cold-store opendal client.

use std::fmt::Debug;
use std::future::Future;
use std::io;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use crossbeam_utils::CachePadded;
use serde::Deserialize;
use serde::Serialize;

/// Identifier the store uses to derive a key/path for a snapshot blob.
///
/// `snapshot_id` is the openraft-provided id (group + leader + log index),
/// guaranteed unique per snapshot build attempt.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SnapshotKey {
    pub raft_group_id: u32,
    pub snapshot_id: String,
}

fn unique_snapshot_leaf(snapshot_id: &str) -> String {
    static COUNTER: CachePadded<AtomicU64> = CachePadded::new(AtomicU64::new(0));
    let nonce_nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let nonce_seq = COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{snapshot_id}-{nonce_nanos:032}-{nonce_seq:020}.snap")
}

/// Where a snapshot blob lives. Carried in [`SnapshotPointer`] over openraft.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SnapshotLocation {
    /// Bytes live inline in the location. Round-trips through openraft with no
    /// external store touch — matches the legacy in-memory snapshot shape.
    Inline {
        #[serde(with = "serde_bytes_vec")]
        bytes: Vec<u8>,
    },
    /// Bytes live on the local filesystem at `path` (dev / single-host).
    Local { path: PathBuf, size_bytes: u64 },
    /// Bytes live in an object storage backend at `key` (S3-compatible).
    S3 { key: String, size_bytes: u64 },
}

impl SnapshotLocation {
    pub fn size_hint(&self) -> u64 {
        match self {
            Self::Inline { bytes } => bytes.len() as u64,
            Self::Local { size_bytes, .. } => *size_bytes,
            Self::S3 { size_bytes, .. } => *size_bytes,
        }
    }
}

mod serde_bytes_vec {
    use serde::Deserialize;
    use serde::Deserializer;
    use serde::Serializer;

    pub fn serialize<S: Serializer>(bytes: &[u8], ser: S) -> Result<S::Ok, S::Error> {
        ser.serialize_bytes(bytes)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(de: D) -> Result<Vec<u8>, D::Error> {
        // Accept both `bytes` (efficient binary) and the JSON-array fallback
        // that serde_json uses by default; we go through Vec<u8> directly.
        Vec::<u8>::deserialize(de)
    }
}

/// Reference shipped through openraft `SnapshotData`. Tiny when the backend
/// stores the actual bytes out of line.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotPointer {
    pub snapshot_id: String,
    pub location: SnapshotLocation,
}

impl SnapshotPointer {
    pub fn encode(&self) -> Result<Vec<u8>, SnapshotStoreError> {
        serde_json::to_vec(self).map_err(|err| SnapshotStoreError::Serialize(err.to_string()))
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, SnapshotStoreError> {
        serde_json::from_slice(bytes)
            .map_err(|err| SnapshotStoreError::Deserialize(err.to_string()))
    }
}

#[derive(Debug)]
pub enum SnapshotStoreError {
    Backend(String),
    NotFound(String),
    Integrity(String),
    Serialize(String),
    Deserialize(String),
    Io(io::Error),
}

impl std::fmt::Display for SnapshotStoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Backend(m) => write!(f, "snapshot store backend: {m}"),
            Self::NotFound(m) => write!(f, "snapshot not found: {m}"),
            Self::Integrity(m) => write!(f, "snapshot integrity: {m}"),
            Self::Serialize(m) => write!(f, "snapshot serialize: {m}"),
            Self::Deserialize(m) => write!(f, "snapshot deserialize: {m}"),
            Self::Io(err) => write!(f, "snapshot io: {err}"),
        }
    }
}

impl std::error::Error for SnapshotStoreError {}

impl From<io::Error> for SnapshotStoreError {
    fn from(err: io::Error) -> Self {
        Self::Io(err)
    }
}

impl SnapshotStoreError {
    pub fn into_io(self) -> io::Error {
        match self {
            Self::Io(err) => err,
            other => io::Error::other(other.to_string()),
        }
    }
}

pub type SnapshotStoreFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, SnapshotStoreError>> + Send + 'a>>;

pub trait SnapshotStore: Send + Sync + Debug {
    /// Persist a snapshot blob and return its location. Stores own naming and
    /// MAY ignore parts of `key` (Inline does).
    fn upload<'a>(
        &'a self,
        key: SnapshotKey,
        bytes: Vec<u8>,
    ) -> SnapshotStoreFuture<'a, SnapshotLocation>;

    /// Retrieve a snapshot blob given its location.
    fn download<'a>(&'a self, location: &'a SnapshotLocation) -> SnapshotStoreFuture<'a, Vec<u8>>;

    /// Best-effort delete; missing is not an error.
    fn delete<'a>(&'a self, location: &'a SnapshotLocation) -> SnapshotStoreFuture<'a, ()>;

    /// Best-effort prune of retired snapshots for one Raft group. Backends
    /// with an external namespace should derive retired objects by listing the
    /// group's snapshot prefix and keep the published `current` object plus
    /// `retain_latest` older objects. Inline snapshots have no external
    /// lifecycle, so the default is a no-op.
    fn prune_retired<'a>(
        &'a self,
        _raft_group_id: u32,
        _current: &'a SnapshotLocation,
        _retain_latest: usize,
    ) -> SnapshotStoreFuture<'a, ()> {
        Box::pin(async move { Ok(()) })
    }

    /// Lightweight liveness probe for the backend, used by the snapshot driver
    /// to detect local S3 loss WITHOUT triggering a `build_snapshot` (whose
    /// failure openraft treats as fatal). The default is "always healthy":
    /// in-memory and local-filesystem backends cannot be remotely unavailable.
    /// The S3 backend overrides this with a cheap `stat`.
    fn health_check(&self) -> SnapshotStoreFuture<'_, ()> {
        Box::pin(async move { Ok(()) })
    }

    /// Verify that a freshly-uploaded snapshot is actually retrievable from
    /// the backend. Called immediately after `upload` returns Ok, before the
    /// new pointer is published. Catches silent partial-success modes
    /// (multipart upload Init/Part Ok but Complete failed, opendal retry
    /// returning Ok on cached state, etc.) that would otherwise leave
    /// `current_snapshot` pointing at a 404. Default no-op for backends that
    /// can't lie about persistence (Inline keeps bytes in the pointer; Local
    /// uses a single fs syscall whose Ok means present). The S3 backend
    /// overrides this with a `stat` round-trip.
    fn verify_uploaded<'a>(
        &'a self,
        _location: &'a SnapshotLocation,
    ) -> SnapshotStoreFuture<'a, ()> {
        Box::pin(async move { Ok(()) })
    }
}

pub type SharedSnapshotStore = Arc<dyn SnapshotStore>;

/// Default backend used when none is wired: bytes ride inline in the pointer.
pub fn default_snapshot_store() -> SharedSnapshotStore {
    Arc::new(InlineSnapshotStore)
}

/// Bytes live inside the pointer. Equivalent to today's in-memory snapshot.
#[derive(Debug, Default, Clone, Copy)]
pub struct InlineSnapshotStore;

impl SnapshotStore for InlineSnapshotStore {
    fn upload<'a>(
        &'a self,
        _key: SnapshotKey,
        bytes: Vec<u8>,
    ) -> SnapshotStoreFuture<'a, SnapshotLocation> {
        Box::pin(async move { Ok(SnapshotLocation::Inline { bytes }) })
    }

    fn download<'a>(&'a self, location: &'a SnapshotLocation) -> SnapshotStoreFuture<'a, Vec<u8>> {
        Box::pin(async move {
            match location {
                SnapshotLocation::Inline { bytes } => Ok(bytes.clone()),
                other => Err(SnapshotStoreError::Backend(format!(
                    "inline snapshot store cannot download {other:?}"
                ))),
            }
        })
    }

    fn delete<'a>(&'a self, _location: &'a SnapshotLocation) -> SnapshotStoreFuture<'a, ()> {
        Box::pin(async move { Ok(()) })
    }
}

#[cfg(not(madsim))]
mod s3 {
    use opendal::Operator;
    use opendal::Scheme;

    use super::SnapshotKey;
    use super::SnapshotLocation;
    use super::SnapshotStore;
    use super::SnapshotStoreError;
    use super::SnapshotStoreFuture;
    use super::unique_snapshot_leaf;

    /// Bytes live in an opendal-managed S3 bucket under `{prefix}/group-{gid}/`.
    pub struct S3SnapshotStore {
        operator: Operator,
        prefix: String,
    }

    impl std::fmt::Debug for S3SnapshotStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("S3SnapshotStore")
                .field("prefix", &self.prefix)
                .finish_non_exhaustive()
        }
    }

    impl S3SnapshotStore {
        pub fn new(operator: Operator, prefix: impl Into<String>) -> Self {
            let mut prefix = prefix.into();
            while prefix.ends_with('/') {
                prefix.pop();
            }
            Self { operator, prefix }
        }

        /// In-memory opendal operator under `prefix`, for tests.
        pub fn memory_for_tests(prefix: impl Into<String>) -> Result<Self, SnapshotStoreError> {
            let operator = Operator::via_iter(Scheme::Memory, [])
                .map_err(|err| SnapshotStoreError::Backend(err.to_string()))?;
            Ok(Self::new(operator, prefix))
        }

        /// Build an S3 snapshot store from a [`ColdConfig`].
        /// Snapshot blobs share the cold bucket/credentials and use `prefix`
        /// (defaults to `snapshots`) for separation.
        pub fn try_new(
            config: &crate::ColdConfig,
            prefix: impl Into<String>,
        ) -> Result<Self, SnapshotStoreError> {
            let s3 = config.s3.as_ref().ok_or_else(|| {
                SnapshotStoreError::Backend("S3 config is required for snapshot s3 backend".into())
            })?;
            let bucket = s3.bucket.as_deref().ok_or_else(|| {
                SnapshotStoreError::Backend("S3 bucket is required for snapshot s3 backend".into())
            })?;
            if bucket.trim().is_empty() {
                return Err(SnapshotStoreError::Backend(
                    "snapshot s3 bucket must not be empty".into(),
                ));
            }
            let mut builder = opendal::services::S3::default().bucket(bucket);
            if let Some(root) = config.root.as_deref()
                && !root.trim().is_empty()
            {
                builder = builder.root(root);
            }
            if let Some(region) = s3.region.as_deref()
                && !region.trim().is_empty()
            {
                builder = builder.region(region);
            }
            if let Some(endpoint) = s3.endpoint.as_deref()
                && !endpoint.trim().is_empty()
            {
                builder = builder.endpoint(endpoint);
            }
            if let Some(access) = s3.access_key_id.as_deref()
                && !access.trim().is_empty()
            {
                builder = builder.access_key_id(access);
            }
            if let Some(secret) = s3.secret_access_key.as_deref()
                && !secret.trim().is_empty()
            {
                builder = builder.secret_access_key(secret);
            }
            if let Some(token) = s3.session_token.as_deref()
                && !token.trim().is_empty()
            {
                builder = builder.session_token(token);
            }
            let operator = crate::cold_store::with_s3_resilience(
                Operator::new(builder)
                    .map_err(|err| SnapshotStoreError::Backend(err.to_string()))?
                    .finish(),
                s3.timeout.as_duration(),
                s3.max_retries,
            );
            Ok(Self::new(operator, prefix))
        }

        /// Build a per-attempt-unique S3 key. The openraft `snapshot_id` is
        /// derived from `last_applied_log_id`, so two builds during an
        /// apply-idle window compute the SAME snapshot_id. If the S3 key also
        /// matched, two distinct published pointers could alias one physical
        /// object. A nanosecond + per-process counter suffix keeps the physical
        /// S3 key unique per upload attempt without changing the openraft-visible
        /// snapshot_id.
        fn object_key(&self, key: &SnapshotKey) -> String {
            format!(
                "{}/group-{}/{}",
                self.prefix,
                key.raft_group_id,
                unique_snapshot_leaf(&key.snapshot_id),
            )
        }

        fn group_prefix(&self, raft_group_id: u32) -> String {
            format!("{}/group-{raft_group_id}/", self.prefix)
        }
    }

    impl SnapshotStore for S3SnapshotStore {
        fn upload<'a>(
            &'a self,
            key: SnapshotKey,
            bytes: Vec<u8>,
        ) -> SnapshotStoreFuture<'a, SnapshotLocation> {
            Box::pin(async move {
                let object_key = self.object_key(&key);
                let size_bytes = bytes.len() as u64;
                self.operator
                    .write(&object_key, bytes)
                    .await
                    .map_err(|err| SnapshotStoreError::Backend(err.to_string()))?;
                Ok(SnapshotLocation::S3 {
                    key: object_key,
                    size_bytes,
                })
            })
        }

        fn download<'a>(
            &'a self,
            location: &'a SnapshotLocation,
        ) -> SnapshotStoreFuture<'a, Vec<u8>> {
            Box::pin(async move {
                let SnapshotLocation::S3 { key, size_bytes } = location else {
                    return Err(SnapshotStoreError::Backend(format!(
                        "s3 snapshot store cannot download {location:?}"
                    )));
                };
                let buf = self.operator.read(key).await.map_err(|err| {
                    if matches!(err.kind(), opendal::ErrorKind::NotFound) {
                        SnapshotStoreError::NotFound(format!("s3 snapshot missing at {key}"))
                    } else {
                        SnapshotStoreError::Backend(err.to_string())
                    }
                })?;
                let bytes = buf.to_vec();
                if bytes.len() as u64 != *size_bytes {
                    return Err(SnapshotStoreError::Integrity(format!(
                        "s3 snapshot {key} size {} != expected {}",
                        bytes.len(),
                        size_bytes
                    )));
                }
                Ok(bytes)
            })
        }

        fn delete<'a>(&'a self, location: &'a SnapshotLocation) -> SnapshotStoreFuture<'a, ()> {
            Box::pin(async move {
                let SnapshotLocation::S3 { key, .. } = location else {
                    return Ok(());
                };
                match self.operator.delete(key).await {
                    Ok(()) => Ok(()),
                    Err(err) if matches!(err.kind(), opendal::ErrorKind::NotFound) => Ok(()),
                    Err(err) => Err(SnapshotStoreError::Backend(err.to_string())),
                }
            })
        }

        fn prune_retired<'a>(
            &'a self,
            raft_group_id: u32,
            current: &'a SnapshotLocation,
            retain_latest: usize,
        ) -> SnapshotStoreFuture<'a, ()> {
            Box::pin(async move {
                let SnapshotLocation::S3 {
                    key: current_key, ..
                } = current
                else {
                    return Ok(());
                };
                let prefix = self.group_prefix(raft_group_id);
                let mut keys = self
                    .operator
                    .list(&prefix)
                    .await
                    .map_err(|err| SnapshotStoreError::Backend(err.to_string()))?
                    .into_iter()
                    .filter(|entry| entry.metadata().is_file())
                    .map(|entry| entry.path().to_owned())
                    .filter(|key| key.ends_with(".snap") && key != current_key)
                    .collect::<Vec<_>>();
                keys.sort();
                let delete_count = keys.len().saturating_sub(retain_latest);
                for key in keys.into_iter().take(delete_count) {
                    match self.operator.delete(&key).await {
                        Ok(()) => {}
                        Err(err) if matches!(err.kind(), opendal::ErrorKind::NotFound) => {}
                        Err(err) => return Err(SnapshotStoreError::Backend(err.to_string())),
                    }
                }
                Ok(())
            })
        }

        fn verify_uploaded<'a>(
            &'a self,
            location: &'a SnapshotLocation,
        ) -> SnapshotStoreFuture<'a, ()> {
            Box::pin(async move {
                let SnapshotLocation::S3 { key, size_bytes } = location else {
                    return Ok(());
                };
                let meta = self.operator.stat(key).await.map_err(|err| {
                    if matches!(err.kind(), opendal::ErrorKind::NotFound) {
                        SnapshotStoreError::NotFound(format!(
                            "s3 snapshot upload verification failed: {key} not present after upload"
                        ))
                    } else {
                        SnapshotStoreError::Backend(err.to_string())
                    }
                })?;
                let actual = meta.content_length();
                if actual != *size_bytes {
                    return Err(SnapshotStoreError::Integrity(format!(
                        "s3 snapshot {key} size mismatch post-upload: stat={actual} expected={size_bytes}"
                    )));
                }
                Ok(())
            })
        }

        fn health_check(&self) -> SnapshotStoreFuture<'_, ()> {
            Box::pin(async move {
                // A `stat` on a probe key is a single cheap round-trip that goes
                // through the same TimeoutLayer/RetryLayer as real writes, so it
                // reports unreachable S3 (timeout / connection error) without
                // building a snapshot. `NotFound` means S3 answered — healthy.
                let probe = format!("{}/.health-probe", self.prefix);
                match self.operator.stat(&probe).await {
                    Ok(_) => Ok(()),
                    Err(err) if matches!(err.kind(), opendal::ErrorKind::NotFound) => Ok(()),
                    Err(err) => Err(SnapshotStoreError::Backend(err.to_string())),
                }
            })
        }
    }
}

#[cfg(not(madsim))]
pub use s3::S3SnapshotStore;

/// Pick a snapshot store from a typed `ursula_config::RaftSnapshotConfig`. Returns `None`
/// when the backend is "inline" (the default) so callers can fall back to
/// [`default_snapshot_store`] without instantiating anything.
pub fn snapshot_store_from_config(
    cfg: &ursula_config::RaftSnapshotConfig,
    cold_cfg: &crate::ColdConfig,
) -> Result<Option<SharedSnapshotStore>, SnapshotStoreError> {
    let _ = cold_cfg;
    match cfg.backend {
        ursula_config::RaftSnapshotBackend::Inline => Ok(None),
        #[cfg(not(madsim))]
        ursula_config::RaftSnapshotBackend::Local => {
            let root = cfg.local_root.as_ref().ok_or_else(|| {
                SnapshotStoreError::Backend("snapshot local_root required for local backend".into())
            })?;
            let root_str = root.to_string_lossy();
            if root_str.trim().is_empty() {
                return Err(SnapshotStoreError::Backend(
                    "snapshot local_root must not be empty".into(),
                ));
            }
            Ok(Some(Arc::new(LocalSnapshotStore::new(root))))
        }
        #[cfg(not(madsim))]
        ursula_config::RaftSnapshotBackend::S3 => {
            let prefix = cfg.s3_prefix.as_deref().unwrap_or("snapshots");
            Ok(Some(Arc::new(S3SnapshotStore::try_new(cold_cfg, prefix)?)))
        }
        #[cfg(madsim)]
        ursula_config::RaftSnapshotBackend::Local | ursula_config::RaftSnapshotBackend::S3 => {
            Err(SnapshotStoreError::Backend(format!(
                "snapshot backend {:?} has no I/O under madsim; use 'inline'",
                cfg.backend
            )))
        }
    }
}

#[cfg(not(madsim))]
mod local {
    use std::io;
    use std::path::PathBuf;

    use super::SnapshotKey;
    use super::SnapshotLocation;
    use super::SnapshotStore;
    use super::SnapshotStoreError;
    use super::SnapshotStoreFuture;
    use super::unique_snapshot_leaf;

    /// Bytes live on the local filesystem under a root directory.
    #[derive(Debug, Clone)]
    pub struct LocalSnapshotStore {
        root: PathBuf,
    }

    impl LocalSnapshotStore {
        pub fn new(root: impl Into<PathBuf>) -> Self {
            Self { root: root.into() }
        }

        fn path_for(&self, key: SnapshotKey) -> PathBuf {
            self.root
                .join(format!("group-{}", key.raft_group_id))
                .join(unique_snapshot_leaf(&key.snapshot_id))
        }

        fn group_dir(&self, raft_group_id: u32) -> PathBuf {
            self.root.join(format!("group-{raft_group_id}"))
        }
    }

    impl SnapshotStore for LocalSnapshotStore {
        fn upload<'a>(
            &'a self,
            key: SnapshotKey,
            bytes: Vec<u8>,
        ) -> SnapshotStoreFuture<'a, SnapshotLocation> {
            Box::pin(async move {
                let path = self.path_for(key);
                if let Some(parent) = path.parent() {
                    tokio::fs::create_dir_all(parent).await?;
                }
                let size_bytes = bytes.len() as u64;
                tokio::fs::write(&path, &bytes).await?;
                Ok(SnapshotLocation::Local { path, size_bytes })
            })
        }

        fn download<'a>(
            &'a self,
            location: &'a SnapshotLocation,
        ) -> SnapshotStoreFuture<'a, Vec<u8>> {
            Box::pin(async move {
                let SnapshotLocation::Local { path, size_bytes } = location else {
                    return Err(SnapshotStoreError::Backend(format!(
                        "local snapshot store cannot download {location:?}"
                    )));
                };
                let bytes = tokio::fs::read(path).await.map_err(|err| {
                    if err.kind() == io::ErrorKind::NotFound {
                        SnapshotStoreError::NotFound(format!(
                            "local snapshot missing at {}",
                            path.display()
                        ))
                    } else {
                        SnapshotStoreError::Io(err)
                    }
                })?;
                if bytes.len() as u64 != *size_bytes {
                    return Err(SnapshotStoreError::Integrity(format!(
                        "local snapshot at {} size {} != expected {}",
                        path.display(),
                        bytes.len(),
                        size_bytes
                    )));
                }
                Ok(bytes)
            })
        }

        fn delete<'a>(&'a self, location: &'a SnapshotLocation) -> SnapshotStoreFuture<'a, ()> {
            Box::pin(async move {
                let SnapshotLocation::Local { path, .. } = location else {
                    return Ok(());
                };
                match tokio::fs::remove_file(path).await {
                    Ok(()) => Ok(()),
                    Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(()),
                    Err(err) => Err(SnapshotStoreError::Io(err)),
                }
            })
        }

        fn prune_retired<'a>(
            &'a self,
            raft_group_id: u32,
            current: &'a SnapshotLocation,
            retain_latest: usize,
        ) -> SnapshotStoreFuture<'a, ()> {
            Box::pin(async move {
                let SnapshotLocation::Local {
                    path: current_path, ..
                } = current
                else {
                    return Ok(());
                };
                let dir = self.group_dir(raft_group_id);
                let mut read_dir = match tokio::fs::read_dir(&dir).await {
                    Ok(read_dir) => read_dir,
                    Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(()),
                    Err(err) => return Err(SnapshotStoreError::Io(err)),
                };
                let mut paths = Vec::new();
                while let Some(entry) = read_dir
                    .next_entry()
                    .await
                    .map_err(SnapshotStoreError::Io)?
                {
                    let path = entry.path();
                    if path == *current_path {
                        continue;
                    }
                    if path.extension().and_then(|ext| ext.to_str()) == Some("snap") {
                        paths.push(path);
                    }
                }
                paths.sort();
                let delete_count = paths.len().saturating_sub(retain_latest);
                for path in paths.into_iter().take(delete_count) {
                    match tokio::fs::remove_file(&path).await {
                        Ok(()) => {}
                        Err(err) if err.kind() == io::ErrorKind::NotFound => {}
                        Err(err) => return Err(SnapshotStoreError::Io(err)),
                    }
                }
                Ok(())
            })
        }
    }
}

#[cfg(not(madsim))]
pub use local::LocalSnapshotStore;

#[cfg(test)]
mod tests {
    use super::*;

    fn test_key(raft_group_id: u32, snapshot_id: &str) -> SnapshotKey {
        SnapshotKey {
            raft_group_id,
            snapshot_id: snapshot_id.to_owned(),
        }
    }

    #[tokio::test]
    async fn inline_roundtrip() {
        let store = InlineSnapshotStore;
        let key = test_key(0, "group-0-T1-N1-100");
        let loc = store.upload(key, b"hello world".to_vec()).await.unwrap();
        assert!(matches!(loc, SnapshotLocation::Inline { .. }));
        let bytes = store.download(&loc).await.unwrap();
        assert_eq!(bytes, b"hello world");
        store.delete(&loc).await.unwrap();
    }

    #[tokio::test]
    async fn inline_rejects_other_location() {
        let store = InlineSnapshotStore;
        let loc = SnapshotLocation::Local {
            path: PathBuf::from("/tmp/nope"),
            size_bytes: 4,
        };
        assert!(matches!(
            store.download(&loc).await,
            Err(SnapshotStoreError::Backend(_))
        ));
    }

    #[test]
    fn pointer_encode_decode_inline() {
        let pointer = SnapshotPointer {
            snapshot_id: "group-0-1-100".into(),
            location: SnapshotLocation::Inline {
                bytes: vec![1, 2, 3, 4],
            },
        };
        let bytes = pointer.encode().unwrap();
        let back = SnapshotPointer::decode(&bytes).unwrap();
        assert_eq!(back.snapshot_id, pointer.snapshot_id);
        match back.location {
            SnapshotLocation::Inline { bytes } => assert_eq!(bytes, vec![1, 2, 3, 4]),
            other => panic!("unexpected location: {other:?}"),
        }
    }

    #[test]
    fn pointer_encode_decode_local() {
        let pointer = SnapshotPointer {
            snapshot_id: "group-7-2-500".into(),
            location: SnapshotLocation::Local {
                path: PathBuf::from("/var/snap/group-7-term-2-log-500.snap"),
                size_bytes: 12345,
            },
        };
        let bytes = pointer.encode().unwrap();
        let back = SnapshotPointer::decode(&bytes).unwrap();
        assert_eq!(back.snapshot_id, pointer.snapshot_id);
        assert_eq!(back.location.size_hint(), 12345);
    }

    #[cfg(not(madsim))]
    #[tokio::test]
    async fn local_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalSnapshotStore::new(dir.path());
        let key = test_key(7, "group-7-T2-N1-500");
        let loc = store
            .upload(key, b"some snapshot bytes".to_vec())
            .await
            .unwrap();
        let bytes = store.download(&loc).await.unwrap();
        assert_eq!(bytes, b"some snapshot bytes");
        store.delete(&loc).await.unwrap();
        let again = store.download(&loc).await;
        assert!(matches!(again, Err(SnapshotStoreError::NotFound(_))));
        // Second delete is a no-op.
        store.delete(&loc).await.unwrap();
    }

    #[cfg(not(madsim))]
    #[tokio::test]
    async fn local_two_uploads_with_same_snapshot_id_get_different_paths() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalSnapshotStore::new(dir.path());
        let key1 = test_key(4, "group-4-T18-N3-264150");
        let key2 = test_key(4, "group-4-T18-N3-264150");
        let loc1 = store.upload(key1, b"body1".to_vec()).await.unwrap();
        let loc2 = store.upload(key2, b"body2".to_vec()).await.unwrap();
        let (path1, path2) = match (&loc1, &loc2) {
            (
                SnapshotLocation::Local { path: path1, .. },
                SnapshotLocation::Local { path: path2, .. },
            ) => (path1.clone(), path2.clone()),
            _ => panic!("expected local locations"),
        };
        assert_ne!(
            path1, path2,
            "same snapshot_id must yield distinct local paths"
        );
        assert_eq!(store.download(&loc1).await.unwrap(), b"body1");
        assert_eq!(store.download(&loc2).await.unwrap(), b"body2");
    }

    #[cfg(not(madsim))]
    #[tokio::test]
    async fn local_prune_retired_keeps_current_and_latest_retired() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalSnapshotStore::new(dir.path());
        let loc1 = store
            .upload(test_key(7, "group-7-T1-N1-1"), b"one".to_vec())
            .await
            .unwrap();
        let loc2 = store
            .upload(test_key(7, "group-7-T1-N1-2"), b"two".to_vec())
            .await
            .unwrap();
        let loc3 = store
            .upload(test_key(7, "group-7-T1-N1-3"), b"three".to_vec())
            .await
            .unwrap();

        store.prune_retired(7, &loc3, 1).await.unwrap();

        assert!(matches!(
            store.download(&loc1).await,
            Err(SnapshotStoreError::NotFound(_))
        ));
        assert_eq!(store.download(&loc2).await.unwrap(), b"two");
        assert_eq!(store.download(&loc3).await.unwrap(), b"three");
    }

    #[cfg(not(madsim))]
    #[tokio::test]
    async fn s3_memory_roundtrip() {
        let store = S3SnapshotStore::memory_for_tests("snapshots").unwrap();
        let key = test_key(3, "group-3-T5-N2-9876");
        let loc = store
            .upload(key, b"raw snapshot bytes".to_vec())
            .await
            .unwrap();
        match &loc {
            SnapshotLocation::S3 { key, size_bytes } => {
                assert!(key.starts_with("snapshots/group-3/"));
                assert_eq!(*size_bytes, b"raw snapshot bytes".len() as u64);
            }
            other => panic!("expected S3 location, got {other:?}"),
        }
        let bytes = store.download(&loc).await.unwrap();
        assert_eq!(bytes, b"raw snapshot bytes");
        store.delete(&loc).await.unwrap();
        assert!(matches!(
            store.download(&loc).await,
            Err(SnapshotStoreError::NotFound(_))
        ));
        // Second delete is a no-op.
        store.delete(&loc).await.unwrap();
    }

    #[cfg(not(madsim))]
    #[tokio::test]
    async fn s3_two_uploads_with_same_snapshot_id_get_different_keys() {
        // Regression for the 2026-05-31 wedge: snapshot_id is derived from
        // last_applied_log_id, so two builds during apply-idle compute the
        // same id. The S3 object key must still be unique per attempt so
        // distinct published pointers never alias one physical object.
        let store = S3SnapshotStore::memory_for_tests("snapshots").unwrap();
        let key1 = test_key(4, "group-4-T18-N3-264150");
        let key2 = test_key(4, "group-4-T18-N3-264150");
        let loc1 = store.upload(key1, b"body1".to_vec()).await.unwrap();
        let loc2 = store.upload(key2, b"body2".to_vec()).await.unwrap();
        let (k1, k2) = match (&loc1, &loc2) {
            (SnapshotLocation::S3 { key: k1, .. }, SnapshotLocation::S3 { key: k2, .. }) => {
                (k1.clone(), k2.clone())
            }
            _ => panic!("expected S3 locations"),
        };
        assert_ne!(k1, k2, "same snapshot_id must yield distinct S3 keys");
        // Both stay independently readable — deleting one must not nuke the
        // other (the self-GC failure mode).
        assert_eq!(store.download(&loc1).await.unwrap(), b"body1");
        assert_eq!(store.download(&loc2).await.unwrap(), b"body2");
        store.delete(&loc1).await.unwrap();
        assert!(matches!(
            store.download(&loc1).await,
            Err(SnapshotStoreError::NotFound(_))
        ));
        assert_eq!(store.download(&loc2).await.unwrap(), b"body2");
    }

    #[cfg(not(madsim))]
    #[tokio::test]
    async fn s3_verify_uploaded_catches_missing_object() {
        let store = S3SnapshotStore::memory_for_tests("snapshots").unwrap();
        let key = test_key(2, "group-2-T1-N1-7");
        let loc = store.upload(key, b"payload".to_vec()).await.unwrap();
        // Round-trip after a real upload: must succeed.
        store.verify_uploaded(&loc).await.unwrap();
        // Same location, after an out-of-band delete: must report missing so
        // the snapshot build path can fail fast instead of publishing a
        // pointer to a 404.
        store.delete(&loc).await.unwrap();
        let err = store.verify_uploaded(&loc).await.unwrap_err();
        assert!(
            matches!(err, SnapshotStoreError::NotFound(_)),
            "expected NotFound after delete, got {err:?}"
        );
    }

    #[cfg(not(madsim))]
    #[tokio::test]
    async fn local_integrity_detects_size_mismatch() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalSnapshotStore::new(dir.path());
        let key = test_key(1, "group-1-T1-N1-1");
        let loc = store.upload(key, b"abcd".to_vec()).await.unwrap();
        let SnapshotLocation::Local { path, .. } = &loc else {
            unreachable!()
        };
        tokio::fs::write(path, b"abcde").await.unwrap();
        let result = store.download(&loc).await;
        assert!(matches!(result, Err(SnapshotStoreError::Integrity(_))));
    }
}

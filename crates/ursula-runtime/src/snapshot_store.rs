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

use serde::{Deserialize, Serialize};

/// Identifier the store uses to derive a key/path for a snapshot blob.
///
/// `snapshot_id` is the openraft-provided id (group + leader + log index),
/// guaranteed unique per snapshot build attempt.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SnapshotKey {
    pub raft_group_id: u32,
    pub snapshot_id: String,
}

impl SnapshotKey {
    /// Canonical leaf filename for filesystem / object key derivation.
    pub fn filename(&self) -> String {
        format!("{}.snap", self.snapshot_id)
    }
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
    use serde::{Deserialize, Deserializer, Serializer};

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

    /// Lightweight liveness probe for the backend, used by the snapshot driver
    /// to detect local S3 loss WITHOUT triggering a `build_snapshot` (whose
    /// failure openraft treats as fatal). The default is "always healthy":
    /// in-memory and local-filesystem backends cannot be remotely unavailable.
    /// The S3 backend overrides this with a cheap `stat`.
    fn health_check(&self) -> SnapshotStoreFuture<'_, ()> {
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
    use super::*;

    use opendal::{Operator, Scheme};

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

        /// Build an S3 operator from `URSULA_SNAPSHOT_S3_*` env, falling back to
        /// the cold-store's `URSULA_COLD_S3_*` for shared bucket/credentials.
        /// Returns the configured prefix from `URSULA_SNAPSHOT_S3_PREFIX`
        /// (defaults to `snapshots`).
        pub fn s3_from_env() -> Result<Self, SnapshotStoreError> {
            let bucket = env_first(&["URSULA_SNAPSHOT_S3_BUCKET", "URSULA_COLD_S3_BUCKET"])
                .ok_or_else(|| {
                    SnapshotStoreError::Backend(
                        "URSULA_SNAPSHOT_S3_BUCKET (or URSULA_COLD_S3_BUCKET) is required for \
                         snapshot s3 backend"
                            .into(),
                    )
                })?;
            if bucket.trim().is_empty() {
                return Err(SnapshotStoreError::Backend(
                    "snapshot s3 bucket must not be empty".into(),
                ));
            }
            let mut builder = opendal::services::S3::default().bucket(&bucket);
            // Root pins all blobs into a snapshot-only sub-tree of the bucket,
            // letting the cold store reuse the same bucket with different keys.
            if let Some(root) = env_first(&["URSULA_SNAPSHOT_S3_ROOT", "URSULA_COLD_ROOT"])
                && !root.trim().is_empty()
            {
                builder = builder.root(&root);
            }
            if let Some(region) = env_first(&["URSULA_SNAPSHOT_S3_REGION", "URSULA_COLD_S3_REGION"])
                && !region.trim().is_empty()
            {
                builder = builder.region(&region);
            }
            if let Some(endpoint) =
                env_first(&["URSULA_SNAPSHOT_S3_ENDPOINT", "URSULA_COLD_S3_ENDPOINT"])
                && !endpoint.trim().is_empty()
            {
                builder = builder.endpoint(&endpoint);
            }
            if let Some(access) = env_first(&[
                "URSULA_SNAPSHOT_S3_ACCESS_KEY_ID",
                "URSULA_COLD_S3_ACCESS_KEY_ID",
            ]) && !access.trim().is_empty()
            {
                builder = builder.access_key_id(&access);
            }
            if let Some(secret) = env_first(&[
                "URSULA_SNAPSHOT_S3_SECRET_ACCESS_KEY",
                "URSULA_COLD_S3_SECRET_ACCESS_KEY",
            ]) && !secret.trim().is_empty()
            {
                builder = builder.secret_access_key(&secret);
            }
            if let Some(token) = env_first(&[
                "URSULA_SNAPSHOT_S3_SESSION_TOKEN",
                "URSULA_COLD_S3_SESSION_TOKEN",
            ]) && !token.trim().is_empty()
            {
                builder = builder.session_token(&token);
            }
            let operator = crate::cold_store::with_s3_resilience(
                Operator::new(builder)
                    .map_err(|err| SnapshotStoreError::Backend(err.to_string()))?
                    .finish(),
            );
            let prefix = std::env::var("URSULA_SNAPSHOT_S3_PREFIX")
                .unwrap_or_else(|_| "snapshots".to_owned());
            Ok(Self::new(operator, prefix))
        }

        fn object_key(&self, key: &SnapshotKey) -> String {
            format!(
                "{}/group-{}/{}",
                self.prefix,
                key.raft_group_id,
                key.filename(),
            )
        }
    }

    fn env_first(names: &[&str]) -> Option<String> {
        for name in names {
            if let Ok(v) = std::env::var(name) {
                return Some(v);
            }
        }
        None
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

/// Pick a snapshot store from env. Returns `None` when the backend is
/// "inline" (the default) so callers can fall back to [`default_snapshot_store`]
/// without instantiating anything.
///
/// Recognized values for `URSULA_SNAPSHOT_BACKEND`: `inline`, `local`, `s3`.
/// Under `madsim`, only `inline` is recognized; the others have no I/O.
pub fn snapshot_store_from_env() -> Result<Option<SharedSnapshotStore>, SnapshotStoreError> {
    let backend = std::env::var("URSULA_SNAPSHOT_BACKEND")
        .unwrap_or_else(|_| "inline".to_owned())
        .to_ascii_lowercase();
    match backend.as_str() {
        "inline" | "default" | "" => Ok(None),
        #[cfg(not(madsim))]
        "local" => {
            let root = std::env::var("URSULA_SNAPSHOT_LOCAL_ROOT").map_err(|_| {
                SnapshotStoreError::Backend(
                    "URSULA_SNAPSHOT_LOCAL_ROOT is required for snapshot local backend".into(),
                )
            })?;
            if root.trim().is_empty() {
                return Err(SnapshotStoreError::Backend(
                    "URSULA_SNAPSHOT_LOCAL_ROOT must not be empty".into(),
                ));
            }
            Ok(Some(Arc::new(LocalSnapshotStore::new(root))))
        }
        #[cfg(not(madsim))]
        "s3" => Ok(Some(Arc::new(S3SnapshotStore::s3_from_env()?))),
        #[cfg(madsim)]
        "local" | "s3" => Err(SnapshotStoreError::Backend(format!(
            "URSULA_SNAPSHOT_BACKEND '{backend}' has no I/O under madsim; use 'inline'"
        ))),
        other => Err(SnapshotStoreError::Backend(format!(
            "unsupported URSULA_SNAPSHOT_BACKEND '{other}' (expected inline | local | s3)"
        ))),
    }
}

#[cfg(not(madsim))]
mod local {
    use super::*;

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
                .join(key.filename())
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
    async fn snapshot_store_from_env_inline_default() {
        // No env set → inline default.
        // Sanity check: clear any backend var that might leak from the host env.
        // SAFETY: tests run single-threaded for env, and the value is restored
        // below. The harness is expected to be single-threaded for env-based
        // tests anyway.
        let prev = std::env::var("URSULA_SNAPSHOT_BACKEND").ok();
        // SAFETY: removing/setting env vars in a test guarded by single env
        // mutation per test; the workspace test harness is multi-threaded but
        // this test only inspects the absence path.
        unsafe {
            std::env::remove_var("URSULA_SNAPSHOT_BACKEND");
        }
        let result = snapshot_store_from_env().unwrap();
        assert!(result.is_none());
        if let Some(prev) = prev {
            unsafe {
                std::env::set_var("URSULA_SNAPSHOT_BACKEND", prev);
            }
        }
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

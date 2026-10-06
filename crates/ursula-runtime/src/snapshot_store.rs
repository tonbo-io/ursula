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
//! The S3 backend reuses the cold-store opendal client.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::fmt::Debug;
use std::future::Future;
use std::io;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;

use bytes::Bytes;
use serde::Deserialize;
use serde::Serialize;

/// Node identities that may persist an external snapshot pointer for each
/// group. S3 pruning is enabled only after every expected voter has published
/// its current reference, so pruning fails closed while any voter has not.
#[derive(Debug, Clone)]
pub struct SnapshotReferenceConfig {
    pub node_id: u64,
    pub default_voters: BTreeSet<u64>,
    pub per_group_voters: BTreeMap<u32, BTreeSet<u64>>,
}

impl SnapshotReferenceConfig {
    fn voters_for(&self, raft_group_id: u32) -> &BTreeSet<u64> {
        self.per_group_voters
            .get(&raft_group_id)
            .unwrap_or(&self.default_voters)
    }
}

/// Identifier the store uses to derive a key/path for a snapshot blob.
///
/// `snapshot_id` is the openraft-provided id (group + leader + log index).
/// Repeated builds at the same applied index may reuse it, so stores must not
/// treat it as a unique physical-object identity.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SnapshotKey {
    pub raft_group_id: u32,
    pub snapshot_id: String,
}

/// Where a snapshot blob lives. Carried in [`SnapshotPointer`] over openraft.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SnapshotLocation {
    /// Bytes live inline in the location. Round-trips through openraft with no
    /// external store touch.
    Inline {
        #[serde(with = "serde_bytes")]
        bytes: Vec<u8>,
    },
    /// Bytes live on the local filesystem at `path` (dev / single-host).
    Local { path: PathBuf, size_bytes: u64 },
    /// Bytes live in an object storage backend at `key` (S3-compatible).
    S3 {
        key: String,
        /// Logical snapshot size after decompression.
        size_bytes: u64,
        /// Physical object size in S3.
        stored_size_bytes: u64,
        /// Compression applied to the S3 object body.
        compression: SnapshotCompression,
        /// The object key is content-addressed and may be referenced by
        /// several replicas or snapshot pointers. Shared objects must only be
        /// removed by reference-aware pruning.
        shared_object: bool,
    },
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SnapshotCompression {
    #[default]
    None,
    Zstd,
}

impl SnapshotLocation {
    pub fn size_hint(&self) -> u64 {
        match self {
            Self::Inline { bytes } => bytes.len() as u64,
            Self::Local { size_bytes, .. } => *size_bytes,
            Self::S3 { size_bytes, .. } => *size_bytes,
        }
    }

    pub fn stored_size_hint(&self) -> u64 {
        match self {
            Self::Inline { bytes } => bytes.len() as u64,
            Self::Local { size_bytes, .. } => *size_bytes,
            Self::S3 {
                stored_size_bytes, ..
            } => *stored_size_bytes,
        }
    }

    pub fn compression(&self) -> SnapshotCompression {
        match self {
            Self::S3 { compression, .. } => *compression,
            Self::Inline { .. } | Self::Local { .. } => SnapshotCompression::None,
        }
    }
}

/// Reference shipped through openraft `SnapshotData`. Tiny when the backend
/// stores the actual bytes out of line.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotPointer {
    pub snapshot_id: String,
    pub location: SnapshotLocation,
}

/// Encodes `value` as the F12a binary snapshot envelope (MessagePack map
/// with named fields), the only envelope Ursula writes.
pub fn encode_binary_envelope<T: Serialize>(value: &T) -> Result<Vec<u8>, SnapshotStoreError> {
    rmp_serde::to_vec_named(value).map_err(|err| SnapshotStoreError::Serialize(err.to_string()))
}

/// Decodes the F12a binary snapshot envelope. A leading `{` marks the JSON
/// envelope of Ursula 0.5.x, which is refused (format epoch 2, E6).
pub fn decode_snapshot_envelope<T: serde::de::DeserializeOwned>(
    bytes: &[u8],
) -> Result<T, SnapshotStoreError> {
    if bytes.first() == Some(&b'{') {
        return Err(SnapshotStoreError::Deserialize(
            ursula_stream::format_epoch_refusal(
                "snapshot pointer",
                "uses the JSON envelope of Ursula 0.5.x and earlier (format epoch 1)",
            ),
        ));
    }
    rmp_serde::from_slice(bytes).map_err(|err| SnapshotStoreError::Deserialize(err.to_string()))
}

impl SnapshotPointer {
    /// Encodes the F12a binary envelope.
    pub fn encode_binary(&self) -> Result<Vec<u8>, SnapshotStoreError> {
        encode_binary_envelope(self)
    }

    /// Decodes the F12a binary envelope; refuses JSON (E6).
    pub fn decode(bytes: &[u8]) -> Result<Self, SnapshotStoreError> {
        decode_snapshot_envelope(bytes)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum SnapshotStoreError {
    #[error("snapshot store backend: {0}")]
    Backend(String),
    #[error("snapshot not found: {0}")]
    NotFound(String),
    #[error("snapshot integrity: {0}")]
    Integrity(String),
    #[error("snapshot serialize: {0}")]
    Serialize(String),
    #[error("snapshot deserialize: {0}")]
    Deserialize(String),
    #[error("snapshot io: {0}")]
    Io(#[from] io::Error),
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
pub type SnapshotBytesIterator = Box<dyn Iterator<Item = Result<Bytes, SnapshotStoreError>> + Send>;

pub trait SnapshotStore: Send + Sync + Debug {
    /// Persist a snapshot blob and return its location. Stores own naming and
    /// MAY ignore parts of `key` (Inline does).
    fn upload<'a>(
        &'a self,
        key: SnapshotKey,
        bytes: Bytes,
    ) -> SnapshotStoreFuture<'a, SnapshotLocation>;

    /// Persist snapshot bytes from an incremental producer.
    fn upload_iter<'a>(
        &'a self,
        key: SnapshotKey,
        chunks: SnapshotBytesIterator,
    ) -> SnapshotStoreFuture<'a, SnapshotLocation> {
        Box::pin(async move {
            let mut bytes = Vec::new();
            for chunk in chunks {
                bytes.extend_from_slice(chunk?.as_ref());
            }
            self.upload(key, Bytes::from(bytes)).await
        })
    }

    /// Retrieve a snapshot blob given its location.
    fn download<'a>(&'a self, location: &'a SnapshotLocation) -> SnapshotStoreFuture<'a, Vec<u8>>;

    /// Best-effort delete; missing is not an error.
    fn delete<'a>(&'a self, location: &'a SnapshotLocation) -> SnapshotStoreFuture<'a, ()>;

    /// Best-effort prune of retired snapshots for one Raft group. Backends may
    /// only delete objects that cannot still be referenced by an OpenRaft
    /// snapshot pointer. Inline snapshots have no external lifecycle, so the
    /// default is a no-op.
    fn prune_retired<'a>(
        &'a self,
        _raft_group_id: u32,
        _current: &'a SnapshotLocation,
        _retain_latest: usize,
    ) -> SnapshotStoreFuture<'a, ()> {
        Box::pin(async move { Ok(()) })
    }

    /// Publish this node's current durable pointer. External stores use these
    /// references to prove that an object is unreachable before deleting it;
    /// callers pin an incoming pointer before changing local metadata and
    /// publish the current reference after that transition. The current pin
    /// remains protected if this PUT fails or briefly lags another transition.
    fn publish_reference<'a>(
        &'a self,
        _raft_group_id: u32,
        _location: &'a SnapshotLocation,
    ) -> SnapshotStoreFuture<'a, ()> {
        Box::pin(async move { Ok(()) })
    }

    /// Protect an incoming external pointer without replacing this node's
    /// current reference. Complete before installing or publishing the pointer.
    /// Pins use the existing reference record format so older pruners retain them.
    /// A backend enabling external snapshot GC must implement this operation
    /// and pin reconciliation; the no-op default is for stores without GC.
    fn pin_reference<'a>(
        &'a self,
        _raft_group_id: u32,
        _location: &'a SnapshotLocation,
    ) -> SnapshotStoreFuture<'a, ()> {
        Box::pin(async move { Ok(()) })
    }

    /// Remove this node's obsolete pins, keeping both its current external
    /// pointer and every prepared operation that could still publish a pointer.
    /// Callers serialize this with pin creation. Startup reconciliation must
    /// include the durable restored pointer before removing abandoned pins.
    fn reconcile_reference_pins<'a>(
        &'a self,
        _raft_group_id: u32,
        _retained: &'a [SnapshotLocation],
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
        bytes: Bytes,
    ) -> SnapshotStoreFuture<'a, SnapshotLocation> {
        Box::pin(async move {
            Ok(SnapshotLocation::Inline {
                bytes: bytes.to_vec(),
            })
        })
    }

    fn upload_iter<'a>(
        &'a self,
        _key: SnapshotKey,
        chunks: SnapshotBytesIterator,
    ) -> SnapshotStoreFuture<'a, SnapshotLocation> {
        Box::pin(async move {
            let bytes = collect_inline_snapshot(chunks).await?;
            Ok(SnapshotLocation::Inline { bytes })
        })
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
async fn collect_inline_snapshot(
    chunks: SnapshotBytesIterator,
) -> Result<Vec<u8>, SnapshotStoreError> {
    tokio::task::spawn_blocking(move || collect_snapshot_chunks(chunks))
        .await
        .map_err(|err| {
            SnapshotStoreError::Io(io::Error::other(format!(
                "join inline snapshot encoder: {err}"
            )))
        })?
}

#[cfg(madsim)]
async fn collect_inline_snapshot(
    chunks: SnapshotBytesIterator,
) -> Result<Vec<u8>, SnapshotStoreError> {
    collect_snapshot_chunks(chunks)
}

fn collect_snapshot_chunks(chunks: SnapshotBytesIterator) -> Result<Vec<u8>, SnapshotStoreError> {
    let mut bytes = Vec::new();
    for chunk in chunks {
        bytes.extend_from_slice(chunk?.as_ref());
    }
    Ok(bytes)
}

#[cfg(not(madsim))]
mod s3 {
    use std::collections::HashSet;
    use std::io;
    use std::io::Write;
    use std::time::Duration;
    use std::time::SystemTime;

    use bytes::Bytes;
    use opendal::ErrorKind;
    use opendal::Operator;
    use opendal::Scheme;

    use super::SnapshotBytesIterator;
    use super::SnapshotCompression;
    use super::SnapshotKey;
    use super::SnapshotLocation;
    use super::SnapshotReferenceConfig;
    use super::SnapshotStore;
    use super::SnapshotStoreError;
    use super::SnapshotStoreFuture;

    const S3_SNAPSHOT_ZSTD_LEVEL: i32 = 3;
    const S3_SNAPSHOT_GC_GRACE: Duration = Duration::from_secs(60 * 60);
    const SNAPSHOT_REFERENCE_VERSION: u32 = ursula_stream::FORMAT_EPOCH;

    #[derive(serde::Deserialize, serde::Serialize)]
    struct SnapshotReference {
        version: u32,
        node_id: u64,
        raft_group_id: u32,
        snapshot_key: Option<String>,
    }

    /// Bytes live in an opendal-managed S3 bucket under `{prefix}/group-{gid}/`.
    pub struct S3SnapshotStore {
        operator: Operator,
        prefix: String,
        references: Option<SnapshotReferenceConfig>,
        gc_grace: Duration,
    }

    impl std::fmt::Debug for S3SnapshotStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("S3SnapshotStore")
                .field("prefix", &self.prefix)
                .field("references", &self.references)
                .field("gc_grace", &self.gc_grace)
                .finish_non_exhaustive()
        }
    }

    impl S3SnapshotStore {
        pub fn new(operator: Operator, prefix: impl Into<String>) -> Self {
            let mut prefix = prefix.into();
            while prefix.ends_with('/') {
                prefix.pop();
            }
            Self {
                operator,
                prefix,
                references: None,
                gc_grace: S3_SNAPSHOT_GC_GRACE,
            }
        }

        pub fn with_references(mut self, references: SnapshotReferenceConfig) -> Self {
            self.references = Some(references);
            self
        }

        #[cfg(test)]
        pub(crate) fn with_gc_grace_for_tests(mut self, gc_grace: Duration) -> Self {
            self.gc_grace = gc_grace;
            self
        }

        /// In-memory opendal operator under `prefix`, for tests.
        pub fn memory_for_tests(prefix: impl Into<String>) -> Result<Self, SnapshotStoreError> {
            let operator = Operator::via_iter(Scheme::Memory, [])
                .map_err(|err| SnapshotStoreError::Backend(err.to_string()))?;
            Ok(Self::new(operator, prefix))
        }

        #[cfg(test)]
        pub(crate) async fn write_raw_for_tests(
            &self,
            key: &str,
            bytes: Vec<u8>,
        ) -> Result<(), SnapshotStoreError> {
            self.operator
                .write(key, bytes)
                .await
                .map_err(|err| SnapshotStoreError::Backend(err.to_string()))
        }

        #[cfg(test)]
        pub(crate) async fn delete_raw_for_tests(
            &self,
            key: &str,
        ) -> Result<(), SnapshotStoreError> {
            self.operator
                .delete(key)
                .await
                .map_err(|err| SnapshotStoreError::Backend(err.to_string()))
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
            // Backup/snapshot objects inherit the cold tier's encryption
            // posture (#149).
            let (builder, _encryption) = crate::cold_store::apply_s3_encryption(builder, s3)
                .map_err(|err| SnapshotStoreError::Backend(err.to_string()))?;
            let operator = crate::cold_store::with_s3_resilience(
                Operator::new(builder)
                    .map_err(|err| SnapshotStoreError::Backend(err.to_string()))?
                    .finish(),
                s3.timeout.as_duration(),
                s3.max_retries,
            );
            Ok(Self::new(operator, prefix))
        }

        fn object_key(&self, key: &SnapshotKey, digest: blake3::Hash) -> String {
            format!(
                "{}/group-{}/objects/{}.snap",
                self.prefix,
                key.raft_group_id,
                digest.to_hex(),
            )
        }

        fn group_prefix(&self, raft_group_id: u32) -> String {
            format!("{}/group-{raft_group_id}/", self.prefix)
        }

        fn reference_key(&self, raft_group_id: u32, node_id: u64) -> String {
            format!(
                "{}references/node-{node_id}.json",
                self.group_prefix(raft_group_id)
            )
        }

        async fn write_content_once(
            &self,
            object_key: &str,
            stored_bytes: Vec<u8>,
        ) -> Result<u64, SnapshotStoreError> {
            let stored_size_bytes = stored_bytes.len() as u64;
            if self
                .operator
                .info()
                .full_capability()
                .write_with_if_not_exists
            {
                match self
                    .operator
                    .write_with(object_key, stored_bytes)
                    .if_not_exists(true)
                    .await
                {
                    Ok(_) => {}
                    Err(err)
                        if matches!(
                            err.kind(),
                            ErrorKind::AlreadyExists | ErrorKind::ConditionNotMatch
                        ) =>
                    {
                        let metadata =
                            self.operator.stat(object_key).await.map_err(|stat_error| {
                                SnapshotStoreError::Backend(format!(
                                    "stat shared s3 snapshot after create race: {stat_error}"
                                ))
                            })?;
                        if metadata.content_length() != stored_size_bytes {
                            return Err(SnapshotStoreError::Integrity(format!(
                                "shared s3 snapshot {object_key} size {} != expected {stored_size_bytes}",
                                metadata.content_length()
                            )));
                        }
                    }
                    Err(err) => return Err(SnapshotStoreError::Backend(err.to_string())),
                }
            } else {
                // The production S3 backend supports conditional creation. This
                // fallback keeps capability-limited test backends useful.
                match self.operator.stat(object_key).await {
                    Ok(metadata) => {
                        if metadata.content_length() != stored_size_bytes {
                            return Err(SnapshotStoreError::Integrity(format!(
                                "shared snapshot {object_key} size {} != expected {stored_size_bytes}",
                                metadata.content_length()
                            )));
                        }
                    }
                    Err(err) if matches!(err.kind(), ErrorKind::NotFound) => {
                        self.operator
                            .write(object_key, stored_bytes)
                            .await
                            .map_err(|write_error| {
                                SnapshotStoreError::Backend(write_error.to_string())
                            })?;
                    }
                    Err(err) => return Err(SnapshotStoreError::Backend(err.to_string())),
                }
            }
            Ok(stored_size_bytes)
        }
    }

    fn compress_snapshot_chunks(
        chunks: SnapshotBytesIterator,
    ) -> Result<(Vec<u8>, u64, blake3::Hash), SnapshotStoreError> {
        let mut size_bytes = 0u64;
        let mut encoder = zstd::stream::write::Encoder::new(Vec::new(), S3_SNAPSHOT_ZSTD_LEVEL)
            .map_err(|err| SnapshotStoreError::Backend(format!("compress s3 snapshot: {err}")))?;
        for chunk in chunks {
            let chunk = chunk?;
            size_bytes = size_bytes.checked_add(chunk.len() as u64).ok_or_else(|| {
                SnapshotStoreError::Integrity("s3 snapshot size overflows u64".to_owned())
            })?;
            encoder.write_all(&chunk).map_err(|err| {
                SnapshotStoreError::Backend(format!("compress s3 snapshot: {err}"))
            })?;
        }
        let stored_bytes = encoder.finish().map_err(|err| {
            SnapshotStoreError::Backend(format!("finish s3 snapshot compression: {err}"))
        })?;
        let digest = blake3::hash(&stored_bytes);
        Ok((stored_bytes, size_bytes, digest))
    }

    impl SnapshotStore for S3SnapshotStore {
        fn pin_reference<'a>(
            &'a self,
            raft_group_id: u32,
            location: &'a SnapshotLocation,
        ) -> SnapshotStoreFuture<'a, ()> {
            Box::pin(async move {
                let (Some(references), SnapshotLocation::S3 { key, .. }) =
                    (&self.references, location)
                else {
                    return Ok(());
                };
                if !key.starts_with(&self.group_prefix(raft_group_id)) || !key.ends_with(".snap") {
                    return Err(SnapshotStoreError::Integrity(
                        "snapshot pin points outside group namespace".to_owned(),
                    ));
                }
                let pin_key = format!(
                    "{}references/pins/{}/{}.json",
                    self.group_prefix(raft_group_id),
                    references.node_id,
                    blake3::hash(key.as_bytes()).to_hex(),
                );
                let record = serde_json::to_vec(&SnapshotReference {
                    version: SNAPSHOT_REFERENCE_VERSION,
                    node_id: references.node_id,
                    raft_group_id,
                    snapshot_key: Some(key.clone()),
                })
                .map_err(|err| SnapshotStoreError::Serialize(err.to_string()))?;
                self.operator
                    .write(&pin_key, record)
                    .await
                    .map_err(|err| SnapshotStoreError::Backend(err.to_string()))
            })
        }

        fn reconcile_reference_pins<'a>(
            &'a self,
            raft_group_id: u32,
            retained: &'a [SnapshotLocation],
        ) -> SnapshotStoreFuture<'a, ()> {
            Box::pin(async move {
                let Some(references) = &self.references else {
                    return Ok(());
                };
                let prefix = format!(
                    "{}references/pins/{}/",
                    self.group_prefix(raft_group_id),
                    references.node_id
                );
                let retained: HashSet<_> = retained
                    .iter()
                    .filter_map(|location| match location {
                        SnapshotLocation::S3 { key, .. } => Some(key.as_str()),
                        _ => None,
                    })
                    .collect();
                let entries = self
                    .operator
                    .list_with(&prefix)
                    .recursive(true)
                    .await
                    .map_err(|err| SnapshotStoreError::Backend(err.to_string()))?;
                for entry in entries {
                    if !entry.metadata().mode().is_file() || !entry.path().ends_with(".json") {
                        continue;
                    }
                    let bytes = self
                        .operator
                        .read(entry.path())
                        .await
                        .map_err(|err| SnapshotStoreError::Backend(err.to_string()))?;
                    let reference: SnapshotReference = serde_json::from_slice(&bytes.to_vec())
                        .map_err(|err| SnapshotStoreError::Deserialize(err.to_string()))?;
                    if reference.version != SNAPSHOT_REFERENCE_VERSION
                        || reference.node_id != references.node_id
                        || reference.raft_group_id != raft_group_id
                    {
                        return Err(SnapshotStoreError::Integrity(
                            "invalid snapshot pin".to_owned(),
                        ));
                    }
                    if reference
                        .snapshot_key
                        .as_deref()
                        .is_none_or(|key| !retained.contains(key))
                    {
                        self.operator
                            .delete(entry.path())
                            .await
                            .map_err(|err| SnapshotStoreError::Backend(err.to_string()))?;
                    }
                }
                Ok(())
            })
        }

        fn upload<'a>(
            &'a self,
            key: SnapshotKey,
            bytes: Bytes,
        ) -> SnapshotStoreFuture<'a, SnapshotLocation> {
            Box::pin(async move {
                let size_bytes = bytes.len() as u64;
                let stored_bytes =
                    zstd::bulk::compress(&bytes, S3_SNAPSHOT_ZSTD_LEVEL).map_err(|err| {
                        SnapshotStoreError::Backend(format!("compress s3 snapshot: {err}"))
                    })?;
                let object_key = self.object_key(&key, blake3::hash(&stored_bytes));
                let stored_size_bytes = self.write_content_once(&object_key, stored_bytes).await?;
                Ok(SnapshotLocation::S3 {
                    key: object_key,
                    size_bytes,
                    stored_size_bytes,
                    compression: SnapshotCompression::Zstd,
                    shared_object: true,
                })
            })
        }

        fn upload_iter<'a>(
            &'a self,
            key: SnapshotKey,
            chunks: SnapshotBytesIterator,
        ) -> SnapshotStoreFuture<'a, SnapshotLocation> {
            Box::pin(async move {
                let encoded = tokio::task::spawn_blocking(move || compress_snapshot_chunks(chunks))
                    .await
                    .map_err(|err| {
                        SnapshotStoreError::Io(io::Error::other(format!(
                            "join s3 snapshot encoder: {err}"
                        )))
                    })??;
                let (stored_bytes, size_bytes, digest) = encoded;
                let object_key = self.object_key(&key, digest);
                let stored_size_bytes = self.write_content_once(&object_key, stored_bytes).await?;
                Ok(SnapshotLocation::S3 {
                    key: object_key,
                    size_bytes,
                    stored_size_bytes,
                    compression: SnapshotCompression::Zstd,
                    shared_object: true,
                })
            })
        }

        fn download<'a>(
            &'a self,
            location: &'a SnapshotLocation,
        ) -> SnapshotStoreFuture<'a, Vec<u8>> {
            Box::pin(async move {
                let SnapshotLocation::S3 {
                    key, size_bytes, ..
                } = location
                else {
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
                let stored_bytes = buf.to_vec();
                let expected_stored_size = location.stored_size_hint();
                if stored_bytes.len() as u64 != expected_stored_size {
                    return Err(SnapshotStoreError::Integrity(format!(
                        "s3 snapshot {key} stored size {} != expected {}",
                        stored_bytes.len(),
                        expected_stored_size
                    )));
                }
                let bytes = match location.compression() {
                    SnapshotCompression::None => stored_bytes,
                    SnapshotCompression::Zstd => zstd::bulk::decompress(
                        &stored_bytes,
                        usize::try_from(*size_bytes).map_err(|_overflow| {
                            SnapshotStoreError::Integrity(format!(
                                "s3 snapshot {key} logical size {size_bytes} does not fit usize"
                            ))
                        })?,
                    )
                    .map_err(|err| {
                        SnapshotStoreError::Integrity(format!(
                            "decompress s3 snapshot {key}: {err}"
                        ))
                    })?,
                };
                if bytes.len() as u64 != *size_bytes {
                    return Err(SnapshotStoreError::Integrity(format!(
                        "s3 snapshot {key} logical size {} != expected {}",
                        bytes.len(),
                        size_bytes
                    )));
                }
                Ok(bytes)
            })
        }

        fn delete<'a>(&'a self, location: &'a SnapshotLocation) -> SnapshotStoreFuture<'a, ()> {
            Box::pin(async move {
                let SnapshotLocation::S3 {
                    key, shared_object, ..
                } = location
                else {
                    return Ok(());
                };
                if *shared_object {
                    return Ok(());
                }
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
                let Some(references) = &self.references else {
                    return Ok(());
                };
                let expected_voters = references.voters_for(raft_group_id);
                if expected_voters.is_empty() {
                    return Ok(());
                }
                let group_prefix = self.group_prefix(raft_group_id);
                let mut retained = HashSet::from([current_key.clone()]);
                for node_id in expected_voters {
                    let reference_key = self.reference_key(raft_group_id, *node_id);
                    let reference_bytes = match self.operator.read(&reference_key).await {
                        Ok(bytes) => bytes,
                        Err(error) if matches!(error.kind(), opendal::ErrorKind::NotFound) => {
                            tracing::debug!(
                                raft_group_id,
                                node_id,
                                "deferring S3 snapshot pruning until every voter publishes a reference"
                            );
                            return Ok(());
                        }
                        Err(error) => {
                            return Err(SnapshotStoreError::Backend(error.to_string()));
                        }
                    };
                    let reference: SnapshotReference =
                        serde_json::from_slice(&reference_bytes.to_vec())
                            .map_err(|error| SnapshotStoreError::Deserialize(error.to_string()))?;
                    if reference.version != SNAPSHOT_REFERENCE_VERSION
                        || reference.node_id != *node_id
                        || reference.raft_group_id != raft_group_id
                    {
                        return Err(SnapshotStoreError::Integrity(format!(
                            "invalid S3 snapshot reference {reference_key}"
                        )));
                    }
                    if let Some(key) = reference.snapshot_key {
                        if !key.starts_with(&group_prefix) || !key.ends_with(".snap") {
                            return Err(SnapshotStoreError::Integrity(format!(
                                "S3 snapshot reference {reference_key} points outside group namespace"
                            )));
                        }
                        retained.insert(key);
                    }
                }
                let cutoff = SystemTime::now()
                    .checked_sub(self.gc_grace)
                    .unwrap_or(SystemTime::UNIX_EPOCH);
                let entries = self
                    .operator
                    .list_with(&group_prefix)
                    .recursive(true)
                    .await
                    .map_err(|error| SnapshotStoreError::Backend(error.to_string()))?;
                for entry in &entries {
                    if !entry.metadata().mode().is_file()
                        || !entry
                            .path()
                            .starts_with(&format!("{group_prefix}references/"))
                        || !entry.path().ends_with(".json")
                    {
                        continue;
                    }
                    let bytes = self
                        .operator
                        .read(entry.path())
                        .await
                        .map_err(|error| SnapshotStoreError::Backend(error.to_string()))?;
                    let reference: SnapshotReference = serde_json::from_slice(&bytes.to_vec())
                        .map_err(|error| SnapshotStoreError::Deserialize(error.to_string()))?;
                    if reference.version != SNAPSHOT_REFERENCE_VERSION
                        || reference.raft_group_id != raft_group_id
                    {
                        return Err(SnapshotStoreError::Integrity(format!(
                            "invalid S3 snapshot reference {}",
                            entry.path()
                        )));
                    }
                    if let Some(key) = reference.snapshot_key {
                        if !key.starts_with(&group_prefix) || !key.ends_with(".snap") {
                            return Err(SnapshotStoreError::Integrity(format!(
                                "S3 snapshot reference {} points outside group namespace",
                                entry.path()
                            )));
                        }
                        retained.insert(key);
                    }
                }
                let mut retired = Vec::new();
                for entry in entries {
                    if !entry.metadata().mode().is_file()
                        || !entry.path().ends_with(".snap")
                        || retained.contains(entry.path())
                    {
                        continue;
                    }
                    let modified = match entry.metadata().last_modified() {
                        Some(modified) => Some(modified.into()),
                        None => self
                            .operator
                            .stat(entry.path())
                            .await
                            .map_err(|error| SnapshotStoreError::Backend(error.to_string()))?
                            .last_modified()
                            .map(Into::into)
                            .or_else(|| self.gc_grace.is_zero().then_some(SystemTime::UNIX_EPOCH)),
                    };
                    if let Some(modified) = modified
                        && modified <= cutoff
                    {
                        retired.push((modified, entry.path().to_owned()));
                    }
                }
                retired.sort_unstable_by(|left, right| right.cmp(left));
                let mut deleted = 0_usize;
                for (_modified, key) in retired.into_iter().skip(retain_latest) {
                    self.operator
                        .delete(&key)
                        .await
                        .map_err(|error| SnapshotStoreError::Backend(error.to_string()))?;
                    deleted = deleted.saturating_add(1);
                }
                if deleted > 0 {
                    tracing::info!(
                        raft_group_id,
                        deleted,
                        retained = retained.len(),
                        "pruned unreachable S3 snapshot objects"
                    );
                }
                Ok(())
            })
        }

        fn publish_reference<'a>(
            &'a self,
            raft_group_id: u32,
            location: &'a SnapshotLocation,
        ) -> SnapshotStoreFuture<'a, ()> {
            Box::pin(async move {
                let Some(references) = &self.references else {
                    return Ok(());
                };
                let snapshot_key = match location {
                    SnapshotLocation::S3 { key, .. } => {
                        let group_prefix = self.group_prefix(raft_group_id);
                        if !key.starts_with(&group_prefix) || !key.ends_with(".snap") {
                            return Err(SnapshotStoreError::Integrity(format!(
                                "S3 snapshot key {key} is outside group {raft_group_id} namespace"
                            )));
                        }
                        Some(key.clone())
                    }
                    SnapshotLocation::Inline { .. } | SnapshotLocation::Local { .. } => None,
                };
                let reference = serde_json::to_vec(&SnapshotReference {
                    version: SNAPSHOT_REFERENCE_VERSION,
                    node_id: references.node_id,
                    raft_group_id,
                    snapshot_key,
                })
                .map_err(|error| SnapshotStoreError::Serialize(error.to_string()))?;
                self.operator
                    .write(
                        &self.reference_key(raft_group_id, references.node_id),
                        reference,
                    )
                    .await
                    .map_err(|error| SnapshotStoreError::Backend(error.to_string()))
            })
        }

        fn verify_uploaded<'a>(
            &'a self,
            location: &'a SnapshotLocation,
        ) -> SnapshotStoreFuture<'a, ()> {
            Box::pin(async move {
                let SnapshotLocation::S3 { key, .. } = location else {
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
                let expected = location.stored_size_hint();
                if actual != expected {
                    return Err(SnapshotStoreError::Integrity(format!(
                        "s3 snapshot {key} stored size mismatch post-upload: stat={actual} expected={expected}"
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
    references: SnapshotReferenceConfig,
) -> Result<Option<SharedSnapshotStore>, SnapshotStoreError> {
    match resolved_snapshot_backend(cfg.backend, cold_cfg.backend) {
        ursula_config::RaftSnapshotBackend::Auto | ursula_config::RaftSnapshotBackend::Inline => {
            Ok(None)
        }
        #[cfg(not(madsim))]
        ursula_config::RaftSnapshotBackend::S3 => {
            // `try_new` configures the OpenDAL operator with `cold_cfg.root`, so
            // this namespace must stay relative to that root.
            let prefix = snapshot_namespace(cfg);
            Ok(Some(Arc::new(
                S3SnapshotStore::try_new(cold_cfg, &prefix)?.with_references(references),
            )))
        }
        #[cfg(madsim)]
        ursula_config::RaftSnapshotBackend::S3 => Err(SnapshotStoreError::Backend(format!(
            "snapshot backend {:?} has no I/O under madsim; use 'inline'",
            cfg.backend
        ))),
    }
}

/// The backend a node runs (bounded-stream-state F12b): `auto` picks S3
/// snapshots whenever the cold store is S3 and inline otherwise. Under
/// madsim, which has no S3 I/O, `auto` stays inline.
pub fn resolved_snapshot_backend(
    configured: ursula_config::RaftSnapshotBackend,
    cold_backend: ursula_config::config::ColdBackend,
) -> ursula_config::RaftSnapshotBackend {
    if cfg!(madsim) && configured == ursula_config::RaftSnapshotBackend::Auto {
        return ursula_config::RaftSnapshotBackend::Inline;
    }
    configured.resolve(cold_backend)
}

#[cfg(not(madsim))]
fn snapshot_namespace(cfg: &ursula_config::RaftSnapshotConfig) -> String {
    cfg.s3_prefix
        .as_deref()
        .unwrap_or("snapshots")
        .trim_matches('/')
        .to_owned()
}

#[cfg(test)]
#[expect(
    clippy::assertions_on_result_states,
    reason = "pre-existing result-state assertion debt; see Known debt in AGENTS.md"
)]
mod tests {
    use super::*;

    fn test_key(raft_group_id: u32, snapshot_id: &str) -> SnapshotKey {
        SnapshotKey {
            raft_group_id,
            snapshot_id: snapshot_id.to_owned(),
        }
    }

    #[cfg(not(madsim))]
    #[test]
    fn snapshot_namespace_stays_relative_to_the_cold_root() {
        let config = ursula_config::RaftSnapshotConfig {
            backend: ursula_config::RaftSnapshotBackend::S3,
            s3_prefix: Some("/snapshots/".to_owned()),
            ..Default::default()
        };

        assert_eq!(snapshot_namespace(&config), "snapshots");
    }

    #[tokio::test]
    async fn inline_roundtrip() {
        let store = InlineSnapshotStore;
        let key = test_key(0, "group-0-T1-N1-100");
        let loc = store
            .upload(key, b"hello world".to_vec().into())
            .await
            .unwrap();
        assert!(matches!(loc, SnapshotLocation::Inline { .. }));
        let bytes = store.download(&loc).await.unwrap();
        assert_eq!(bytes, b"hello world");
        store.delete(&loc).await.unwrap();
    }

    #[cfg(not(madsim))]
    #[tokio::test]
    async fn inline_iterator_is_consumed_off_the_async_worker() {
        let store = InlineSnapshotStore;
        let caller = std::thread::current().id();
        let (thread_tx, thread_rx) = std::sync::mpsc::channel();
        let chunks = Box::new(std::iter::once_with(move || {
            thread_tx.send(std::thread::current().id()).unwrap();
            Ok(Bytes::from_static(b"snapshot"))
        }));

        let location = store
            .upload_iter(test_key(0, "offloaded"), chunks)
            .await
            .unwrap();

        assert_ne!(thread_rx.recv().unwrap(), caller);
        assert_eq!(location, SnapshotLocation::Inline {
            bytes: b"snapshot".to_vec()
        });
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
    fn binary_pointer_carries_inline_bytes_as_bin_and_decodes() {
        let payload = (0..=255_u8).cycle().take(4096).collect::<Vec<_>>();
        let pointer = SnapshotPointer {
            snapshot_id: "group-0-1-100".into(),
            location: SnapshotLocation::Inline {
                bytes: payload.clone(),
            },
        };
        let binary = pointer.encode_binary().unwrap();
        // The binary envelope stores every byte once.
        assert!(binary.len() < payload.len() + 128, "{}", binary.len());
        let back = SnapshotPointer::decode(&binary).unwrap();
        assert_eq!(back.snapshot_id, pointer.snapshot_id);
        assert_eq!(back.location, pointer.location);
    }

    #[test]
    fn binary_pointer_round_trips_external_locations() {
        for location in [
            SnapshotLocation::Local {
                path: PathBuf::from("/var/snap/group-7.snap"),
                size_bytes: 12,
            },
            SnapshotLocation::S3 {
                key: "snapshots/group-7/a.snap".into(),
                size_bytes: 123,
                stored_size_bytes: 45,
                compression: SnapshotCompression::Zstd,
                shared_object: true,
            },
        ] {
            let pointer = SnapshotPointer {
                snapshot_id: "group-7-2-500".into(),
                location,
            };
            let bytes = pointer.encode_binary().unwrap();
            assert_eq!(
                SnapshotPointer::decode(&bytes).unwrap().location,
                pointer.location
            );
        }
    }

    #[test]
    fn pointer_encode_decode_inline() {
        let pointer = SnapshotPointer {
            snapshot_id: "group-0-1-100".into(),
            location: SnapshotLocation::Inline {
                bytes: vec![1, 2, 3, 4],
            },
        };
        let bytes = pointer.encode_binary().unwrap();
        let back = SnapshotPointer::decode(&bytes).unwrap();
        assert_eq!(back.snapshot_id, pointer.snapshot_id);
        match back.location {
            SnapshotLocation::Inline { bytes } => assert_eq!(bytes, vec![1, 2, 3, 4]),
            other => panic!("unexpected location: {other:?}"),
        }
    }

    /// F12a: every location round-trips through the binary envelope, inline
    /// bytes travel as one MessagePack `bin`, and the JSON envelope of Ursula
    /// 0.5.x is refused (format epoch 2, E6).
    #[test]
    fn pointer_decodes_the_binary_envelope_and_refuses_the_legacy_json() {
        let pointers = [
            SnapshotPointer {
                snapshot_id: "group-0-1-100".into(),
                location: SnapshotLocation::Inline {
                    bytes: (0..=255).collect(),
                },
            },
            SnapshotPointer {
                snapshot_id: "group-7-2-500".into(),
                location: SnapshotLocation::Local {
                    path: PathBuf::from("/var/snap/group-7.snap"),
                    size_bytes: 12345,
                },
            },
            SnapshotPointer {
                snapshot_id: "group-3-4-900".into(),
                location: SnapshotLocation::S3 {
                    key: "snapshots/group-3/abc.snap".into(),
                    size_bytes: 77,
                    stored_size_bytes: 40,
                    compression: SnapshotCompression::Zstd,
                    shared_object: true,
                },
            },
        ];
        for pointer in pointers {
            let binary = pointer.encode_binary().unwrap();
            let back = SnapshotPointer::decode(&binary).unwrap();
            assert_eq!(back.snapshot_id, pointer.snapshot_id);
            assert_eq!(back.location, pointer.location);
            let json = serde_json::to_vec(&pointer).unwrap();
            let error = SnapshotPointer::decode(&json).expect_err("E6");
            assert!(
                error.to_string().contains("JSON envelope of Ursula 0.5.x"),
                "{error}"
            );
        }
        let inline = SnapshotPointer {
            snapshot_id: "g".into(),
            location: SnapshotLocation::Inline {
                bytes: vec![200; 4096],
            },
        };
        let binary = inline.encode_binary().unwrap().len();
        assert!(binary < 4096 + 64, "binary inline bytes are not amplified");
        assert!(SnapshotPointer::decode(b"\x00garbage").is_err());
        assert!(SnapshotPointer::decode(b" \n{\"snapshot_id\":1}").is_err());
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
        let bytes = pointer.encode_binary().unwrap();
        let back = SnapshotPointer::decode(&bytes).unwrap();
        assert_eq!(back.snapshot_id, pointer.snapshot_id);
        assert_eq!(back.location.size_hint(), 12345);
    }

    #[cfg(not(madsim))]
    #[tokio::test]
    async fn s3_memory_roundtrip() {
        let store = S3SnapshotStore::memory_for_tests("snapshots").unwrap();
        let key = test_key(3, "group-3-T5-N2-9876");
        let payload = b"raw snapshot bytes".repeat(64);
        let loc = store.upload(key, payload.clone().into()).await.unwrap();
        match &loc {
            SnapshotLocation::S3 {
                key,
                size_bytes,
                stored_size_bytes,
                compression,
                shared_object,
            } => {
                assert!(key.starts_with("snapshots/group-3/objects/"));
                assert_eq!(*size_bytes, payload.len() as u64);
                assert_eq!(*compression, SnapshotCompression::Zstd);
                assert!(*shared_object);
                assert!(*stored_size_bytes < *size_bytes);
            }
            other => panic!("expected S3 location, got {other:?}"),
        }
        let bytes = store.download(&loc).await.unwrap();
        assert_eq!(bytes, payload);
        // A content-addressed object may already be referenced by another
        // replica. Eager local cleanup must not remove it.
        store.delete(&loc).await.unwrap();
        assert_eq!(store.download(&loc).await.unwrap(), payload);
        store.delete(&loc).await.unwrap();
    }

    #[cfg(not(madsim))]
    #[tokio::test]
    async fn s3_pending_pins_protect_install_and_only_retire_this_nodes_obsolete_pins() {
        let operator = opendal::Operator::via_iter(opendal::Scheme::Memory, []).unwrap();
        let store = S3SnapshotStore::new(operator.clone(), "snapshot-pin-gc")
            .with_references(SnapshotReferenceConfig {
                node_id: 1,
                default_voters: BTreeSet::from([1]),
                per_group_voters: BTreeMap::new(),
            })
            .with_gc_grace_for_tests(std::time::Duration::ZERO);
        let other = S3SnapshotStore::new(operator, "snapshot-pin-gc").with_references(
            SnapshotReferenceConfig {
                node_id: 2,
                default_voters: BTreeSet::from([1]),
                per_group_voters: BTreeMap::new(),
            },
        );
        let old = store
            .upload(test_key(7, "old"), b"old".to_vec().into())
            .await
            .unwrap();
        let incoming = store
            .upload(test_key(7, "incoming"), b"incoming".to_vec().into())
            .await
            .unwrap();
        let abandoned = store
            .upload(test_key(7, "abandoned"), b"abandoned".to_vec().into())
            .await
            .unwrap();
        let other_pin = store
            .upload(test_key(7, "other"), b"other".to_vec().into())
            .await
            .unwrap();
        store.publish_reference(7, &old).await.unwrap();
        store.pin_reference(7, &incoming).await.unwrap();
        store.pin_reference(7, &abandoned).await.unwrap();
        other.pin_reference(7, &other_pin).await.unwrap();
        // The unchanged GC reader understands these version-two records,
        // while the primary still names the old pointer during installation
        // or after a failed primary PUT. Zero grace makes protection explicit.
        store.prune_retired(7, &old, 0).await.unwrap();
        assert_eq!(store.download(&old).await.unwrap(), b"old");
        assert_eq!(store.download(&incoming).await.unwrap(), b"incoming");
        assert_eq!(store.download(&abandoned).await.unwrap(), b"abandoned");
        assert_eq!(store.download(&other_pin).await.unwrap(), b"other");
        store
            .reconcile_reference_pins(7, std::slice::from_ref(&incoming))
            .await
            .unwrap();
        store.prune_retired(7, &old, 0).await.unwrap();
        assert!(matches!(
            store.download(&abandoned).await,
            Err(SnapshotStoreError::NotFound(_))
        ));
        assert_eq!(store.download(&incoming).await.unwrap(), b"incoming");
        assert_eq!(store.download(&other_pin).await.unwrap(), b"other");
        store.publish_reference(7, &incoming).await.unwrap();
        store.prune_retired(7, &incoming, 0).await.unwrap();
        assert!(matches!(
            store.download(&old).await,
            Err(SnapshotStoreError::NotFound(_))
        ));
        assert_eq!(store.download(&incoming).await.unwrap(), b"incoming");
        assert_eq!(store.download(&other_pin).await.unwrap(), b"other");
    }

    #[cfg(not(madsim))]
    #[tokio::test]
    async fn s3_iterator_upload_is_compressed_and_offloaded() {
        let store = S3SnapshotStore::memory_for_tests("snapshots").unwrap();
        let caller = std::thread::current().id();
        let (thread_tx, thread_rx) = std::sync::mpsc::channel();
        let chunks = Box::new(std::iter::once_with(move || {
            thread_tx.send(std::thread::current().id()).unwrap();
            Ok(Bytes::from(vec![b'x'; 64 * 1024]))
        }));

        let location = store
            .upload_iter(test_key(9, "group-9-T1-N1-1"), chunks)
            .await
            .unwrap();

        assert_ne!(thread_rx.recv().unwrap(), caller);
        assert_eq!(location.compression(), SnapshotCompression::Zstd);
        assert!(location.stored_size_hint() < location.size_hint());
        assert_eq!(store.download(&location).await.unwrap(), vec![
            b'x';
            64 * 1024
        ]);
    }

    #[cfg(not(madsim))]
    #[tokio::test]
    async fn s3_snapshot_keys_are_content_addressed() {
        let store = S3SnapshotStore::memory_for_tests("snapshots").unwrap();
        let key1 = test_key(4, "group-4-T18-N3-264150");
        let key2 = test_key(4, "group-4-T18-N3-264150");
        let key3 = test_key(4, "group-4-T18-N3-264151");
        let loc1 = store.upload(key1, b"body1".to_vec().into()).await.unwrap();
        let loc2 = store.upload(key2, b"body2".to_vec().into()).await.unwrap();
        let loc3 = store.upload(key3, b"body1".to_vec().into()).await.unwrap();
        let (k1, k2, k3) = match (&loc1, &loc2, &loc3) {
            (
                SnapshotLocation::S3 { key: k1, .. },
                SnapshotLocation::S3 { key: k2, .. },
                SnapshotLocation::S3 { key: k3, .. },
            ) => (k1.clone(), k2.clone(), k3.clone()),
            _ => panic!("expected S3 locations"),
        };
        assert_ne!(k1, k2, "different bytes must never alias");
        assert_eq!(k1, k3, "identical bytes in one group must share an object");
        assert_eq!(store.download(&loc1).await.unwrap(), b"body1");
        assert_eq!(store.download(&loc2).await.unwrap(), b"body2");
        assert_eq!(store.download(&loc3).await.unwrap(), b"body1");
        store.delete(&loc1).await.unwrap();
        assert_eq!(store.download(&loc3).await.unwrap(), b"body1");
        assert_eq!(store.download(&loc2).await.unwrap(), b"body2");
    }

    #[cfg(not(madsim))]
    #[tokio::test]
    async fn s3_verify_uploaded_catches_missing_object() {
        let store = S3SnapshotStore::memory_for_tests("snapshots").unwrap();
        let key = test_key(2, "group-2-T1-N1-7");
        let loc = store.upload(key, b"payload".to_vec().into()).await.unwrap();
        // Round-trip after a real upload: must succeed.
        store.verify_uploaded(&loc).await.unwrap();
        // Same location, after an out-of-band delete: must report missing so
        // the snapshot build path can fail fast instead of publishing a
        // pointer to a 404.
        let SnapshotLocation::S3 { key, .. } = &loc else {
            panic!("expected S3 location")
        };
        store.delete_raw_for_tests(key).await.unwrap();
        let err = store.verify_uploaded(&loc).await.unwrap_err();
        assert!(
            matches!(err, SnapshotStoreError::NotFound(_)),
            "expected NotFound after delete, got {err:?}"
        );
    }

    #[cfg(not(madsim))]
    #[tokio::test]
    async fn s3_pruning_waits_for_every_voter_and_preserves_their_references() {
        use std::collections::BTreeMap;
        use std::collections::BTreeSet;
        use std::time::Duration;

        let references = SnapshotReferenceConfig {
            node_id: 1,
            default_voters: BTreeSet::from([1, 2, 3]),
            per_group_voters: BTreeMap::new(),
        };
        let store = S3SnapshotStore::memory_for_tests("snapshots")
            .unwrap()
            .with_references(references)
            .with_gc_grace_for_tests(Duration::ZERO);
        let retired = store
            .upload(test_key(7, "retired"), b"retired".to_vec().into())
            .await
            .unwrap();
        let node_two = store
            .upload(test_key(7, "node-two"), b"node-two".to_vec().into())
            .await
            .unwrap();
        let node_three = store
            .upload(test_key(7, "node-three"), b"node-three".to_vec().into())
            .await
            .unwrap();
        let current = store
            .upload(test_key(7, "current"), b"current".to_vec().into())
            .await
            .unwrap();
        store.publish_reference(7, &current).await.unwrap();

        store.prune_retired(7, &current, 0).await.unwrap();
        assert_eq!(store.download(&retired).await.unwrap(), b"retired");

        for (node_id, location) in [(2, &node_two), (3, &node_three)] {
            let SnapshotLocation::S3 { key, .. } = location else {
                panic!("expected S3 location")
            };
            store
                .write_raw_for_tests(
                    &format!("snapshots/group-7/references/node-{node_id}.json"),
                    serde_json::to_vec(&serde_json::json!({
                        // References carry the format epoch (version 2).
                        "version": ursula_stream::FORMAT_EPOCH,
                        "node_id": node_id,
                        "raft_group_id": 7,
                        "snapshot_key": key,
                    }))
                    .unwrap(),
                )
                .await
                .unwrap();
        }
        store.prune_retired(7, &current, 0).await.unwrap();
        assert!(matches!(
            store.download(&retired).await,
            Err(SnapshotStoreError::NotFound(_))
        ));
        assert_eq!(store.download(&node_two).await.unwrap(), b"node-two");
        assert_eq!(store.download(&node_three).await.unwrap(), b"node-three");
        assert_eq!(store.download(&current).await.unwrap(), b"current");
    }
}

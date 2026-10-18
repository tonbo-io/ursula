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
//!
//! Owned objects (#687). A node writes the objects it may commit under
//! `{prefix}/node-{node}/group-{gid}/` ([`OwnedKey`]), each under a fresh
//! name, and only that node deletes them. [`SnapshotStore::stage`] draws the
//! name without I/O, [`SnapshotStore::write_staged`] creates the object only
//! if no object has the name, and [`SnapshotStore::list_own`] and
//! [`SnapshotStore::delete_own`] serve the owner's GC. [`OwnedPointer`] is a
//! pointer this node may commit.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::fmt;
use std::fmt::Debug;
use std::future::Future;
use std::io;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;

use bytes::Bytes;
use serde::Deserialize;
use serde::Serialize;

use crate::SnapshotStoreOperation;

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

/// The kind of a [`SnapshotLocation`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SnapshotLocationKind {
    Inline,
    Local,
    S3,
}

impl fmt::Display for SnapshotLocationKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Inline => "inline",
            Self::Local => "local",
            Self::S3 => "S3",
        })
    }
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

    pub fn kind(&self) -> SnapshotLocationKind {
        match self {
            Self::Inline { .. } => SnapshotLocationKind::Inline,
            Self::Local { .. } => SnapshotLocationKind::Local,
            Self::S3 { .. } => SnapshotLocationKind::S3,
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

/// The node that owns a group's snapshot objects: the only node that writes
/// or deletes them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SnapshotOwner {
    pub node_id: u64,
    pub raft_group_id: u32,
}

impl SnapshotOwner {
    /// The directory of the owner's objects under `prefix`:
    /// `{prefix}/node-{node}/group-{gid}/`.
    pub fn directory(&self, prefix: &str) -> String {
        format!(
            "{prefix}/node-{}/group-{}/",
            self.node_id, self.raft_group_id
        )
    }
}

/// The key of an owned snapshot object,
/// `{prefix}/node-{node}/group-{gid}/{index:020}-{nonce:016x}.snap`. `index`
/// is the snapshot's last log index, so a group's objects list in log
/// order, and `nonce` is drawn at random for every write, so no two writes
/// share a name.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct OwnedKey {
    pub owner: SnapshotOwner,
    pub index: u64,
    pub nonce: u64,
}

/// Why a key is not an [`OwnedKey`] under a prefix.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OwnedKeyProblem {
    /// The key does not start with the prefix as a whole path component.
    OutsidePrefix,
    /// The key does not have the three components `node-{node}`,
    /// `group-{gid}` and the object name below the prefix.
    Shape,
    /// The node component is not `node-{node}` with a canonical decimal id.
    Node,
    /// The group component is not `group-{gid}` with a canonical decimal id.
    Group,
    /// The object name is not `{index:020}-{nonce:016x}.snap`.
    Name,
}

/// A key that is not an owned object key under `prefix`.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{key} is not an owned snapshot object key under {prefix}: {problem:?}")]
pub struct OwnedKeyError {
    pub key: String,
    pub prefix: String,
    pub problem: OwnedKeyProblem,
}

impl OwnedKey {
    /// The full object key under `prefix`.
    pub fn render(&self, prefix: &str) -> String {
        format!(
            "{}{:020}-{:016x}.snap",
            self.owner.directory(prefix),
            self.index,
            self.nonce
        )
    }

    /// Parses `key` under `prefix` component by component: the prefix must
    /// end at a `/`, and every number must be in the form [`Self::render`]
    /// writes, so `node-1` never matches `node-10` and a parsed key renders
    /// back to the same text.
    pub fn parse(prefix: &str, key: &str) -> Result<Self, OwnedKeyError> {
        let refuse = |problem| OwnedKeyError {
            key: key.to_owned(),
            prefix: prefix.to_owned(),
            problem,
        };
        let rest = key
            .strip_prefix(prefix)
            .and_then(|rest| rest.strip_prefix('/'))
            .ok_or_else(|| refuse(OwnedKeyProblem::OutsidePrefix))?;
        let mut components = rest.split('/');
        let (Some(node), Some(group), Some(name), None) = (
            components.next(),
            components.next(),
            components.next(),
            components.next(),
        ) else {
            return Err(refuse(OwnedKeyProblem::Shape));
        };
        let node_id = node
            .strip_prefix("node-")
            .and_then(canonical_decimal::<u64>)
            .ok_or_else(|| refuse(OwnedKeyProblem::Node))?;
        let raft_group_id = group
            .strip_prefix("group-")
            .and_then(canonical_decimal::<u32>)
            .ok_or_else(|| refuse(OwnedKeyProblem::Group))?;
        let (index, nonce) = name
            .strip_suffix(".snap")
            .and_then(|stem| stem.split_once('-'))
            .and_then(|(index, nonce)| Some((padded_index(index)?, padded_nonce(nonce)?)))
            .ok_or_else(|| refuse(OwnedKeyProblem::Name))?;
        Ok(Self {
            owner: SnapshotOwner {
                node_id,
                raft_group_id,
            },
            index,
            nonce,
        })
    }
}

/// `text` as a decimal number written without sign or leading zeros.
fn canonical_decimal<T: std::str::FromStr + fmt::Display>(text: &str) -> Option<T> {
    let value = text.parse::<T>().ok()?;
    (value.to_string() == text).then_some(value)
}

/// `text` as the 20-digit zero-padded index of an object name.
fn padded_index(text: &str) -> Option<u64> {
    if text.len() != 20 || !text.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    let index = text.parse::<u64>().ok()?;
    (format!("{index:020}") == text).then_some(index)
}

/// `text` as the 16-digit lowercase hexadecimal nonce of an object name.
fn padded_nonce(text: &str) -> Option<u64> {
    if text.len() != 16
        || !text
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return None;
    }
    u64::from_str_radix(text, 16).ok()
}

/// The keys a node may commit for one group: inline pointers, and S3 keys
/// under its own directory of the store's prefix.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OwnedNamespace {
    pub prefix: String,
    pub owner: SnapshotOwner,
}

/// Why a pointer is not an [`OwnedPointer`].
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum NotOwned {
    /// The pointer names an S3 key outside the node's own directory.
    #[error("snapshot key {key} lies outside the owned directory {directory}")]
    ForeignKey { key: String, directory: String },
    /// The pointer names a local file, which no node owns in a store.
    #[error("snapshot pointer names the local file {path:?}")]
    LocalPath { path: PathBuf },
}

/// A snapshot pointer this node may commit: inline, or an S3 key that
/// parses as an [`OwnedKey`] of this node and group. It is built only by
/// [`TryFrom`], which refuses every other pointer.
#[derive(Debug, Clone)]
pub struct OwnedPointer(SnapshotPointer);

impl OwnedPointer {
    pub fn pointer(&self) -> &SnapshotPointer {
        &self.0
    }

    pub fn into_pointer(self) -> SnapshotPointer {
        self.0
    }
}

impl TryFrom<(SnapshotPointer, &OwnedNamespace)> for OwnedPointer {
    type Error = NotOwned;

    fn try_from(
        (pointer, namespace): (SnapshotPointer, &OwnedNamespace),
    ) -> Result<Self, Self::Error> {
        match &pointer.location {
            SnapshotLocation::Inline { .. } => Ok(Self(pointer)),
            SnapshotLocation::Local { path, .. } => Err(NotOwned::LocalPath { path: path.clone() }),
            SnapshotLocation::S3 { key, .. } => match OwnedKey::parse(&namespace.prefix, key) {
                Ok(owned) if owned.owner == namespace.owner => Ok(Self(pointer)),
                Ok(_) | Err(_) => Err(NotOwned::ForeignKey {
                    key: key.clone(),
                    directory: namespace.owner.directory(&namespace.prefix),
                }),
            },
        }
    }
}

/// A snapshot body made ready for its store with no I/O
/// ([`SnapshotStore::stage`]): inline, or compressed under a freshly drawn
/// [`OwnedKey`]. Its [`StagedSnapshot::location`] is known before the write.
pub enum StagedSnapshot {
    Inline { bytes: Vec<u8> },
    Object(StagedObject),
}

/// The object of a [`StagedSnapshot`].
pub struct StagedObject {
    pub key: OwnedKey,
    /// The location the write creates: an S3 key that no reference shares.
    pub location: SnapshotLocation,
    stored: Vec<u8>,
}

impl StagedSnapshot {
    /// Where the snapshot lives once written.
    pub fn location(&self) -> SnapshotLocation {
        match self {
            Self::Inline { bytes } => SnapshotLocation::Inline {
                bytes: bytes.clone(),
            },
            Self::Object(object) => object.location.clone(),
        }
    }

    /// The owned key of a staged object.
    pub fn key(&self) -> Option<&OwnedKey> {
        match self {
            Self::Inline { .. } => None,
            Self::Object(object) => Some(&object.key),
        }
    }
}

impl Debug for StagedSnapshot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Inline { bytes } => f
                .debug_struct("Inline")
                .field("bytes", &bytes.len())
                .finish(),
            Self::Object(object) => f
                .debug_struct("Object")
                .field("key", &object.key)
                .field("location", &object.location)
                .field("stored", &object.stored.len())
                .finish(),
        }
    }
}

/// What a listing of an owner's directory holds ([`SnapshotStore::list_own`]).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct OwnListing {
    /// The objects that parse as the owner's keys, in key order.
    pub owned: Vec<OwnedKey>,
    /// Every other file in the directory.
    pub foreign: Vec<String>,
}

/// The backend of a [`SnapshotStore`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SnapshotBackend {
    Inline,
    S3,
}

impl fmt::Display for SnapshotBackend {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Inline => "inline",
            Self::S3 => "S3",
        })
    }
}

/// What is missing from the config of an S3 snapshot store.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SnapshotStoreConfigProblem {
    /// The cold store has no S3 section.
    NoS3Config,
    /// The S3 section names no bucket.
    NoBucket,
    /// The S3 bucket is empty.
    EmptyBucket,
    /// The S3 backend was chosen in a simulation, which has no S3 I/O.
    NoS3UnderSimulation,
}

impl fmt::Display for SnapshotStoreConfigProblem {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::NoS3Config => "S3 config is required for the snapshot S3 backend",
            Self::NoBucket => "an S3 bucket is required for the snapshot S3 backend",
            Self::EmptyBucket => "the snapshot S3 bucket must not be empty",
            Self::NoS3UnderSimulation => {
                "the snapshot S3 backend has no I/O under madsim; use 'inline'"
            }
        })
    }
}

#[derive(Debug, thiserror::Error)]
pub enum SnapshotStoreError {
    /// The object is not in the store.
    #[error("snapshot object {key} not found")]
    NotFound { key: String },
    /// A write that creates an object only if none has its name found one.
    #[error("snapshot object {key} already exists")]
    NameCollision { key: String },
    /// The stored object's size is not the pointer's.
    #[error("snapshot object {key} stores {actual} bytes, expected {expected}")]
    SizeMismatch {
        key: String,
        expected: u64,
        actual: u64,
    },
    /// The backend failed `operation` on `key`.
    #[error("snapshot store {operation} of {key} failed")]
    Backend {
        operation: SnapshotStoreOperation,
        key: String,
        #[source]
        source: Box<opendal::Error>,
    },
    /// The decompressed object's size is not the pointer's.
    #[error("snapshot object {key} decompresses to {actual} bytes, expected {expected}")]
    LogicalSizeMismatch {
        key: String,
        expected: u64,
        actual: u64,
    },
    /// The stored object does not decompress, or its logical size does not
    /// fit in memory.
    #[error("snapshot object {key} does not decompress")]
    Decompress {
        key: String,
        #[source]
        source: io::Error,
    },
    /// Compressing a snapshot body failed.
    #[error("compress a snapshot body")]
    Compress(#[source] io::Error),
    /// A snapshot body has more bytes than a `u64` counts.
    #[error("a snapshot body is larger than u64::MAX bytes")]
    BodyTooLarge,
    /// The store does not hold snapshots at such a location.
    #[error("the {store} snapshot store holds no {location} snapshot")]
    UnsupportedLocation {
        store: SnapshotBackend,
        location: SnapshotLocationKind,
    },
    /// The backend cannot create an object only if none has its name, so
    /// the store refuses to write `key` rather than overwrite an object.
    #[error("the snapshot store cannot create {key} only if it is absent")]
    ConditionalCreateUnsupported { key: String },
    /// The S3 snapshot store's config is incomplete.
    #[error("snapshot store config: {problem}")]
    Config { problem: SnapshotStoreConfigProblem },
    /// The S3 operator did not build.
    #[error("build the S3 snapshot store")]
    Open(#[source] Box<opendal::Error>),
    /// The cold tier's S3 encryption did not apply to the snapshot store.
    #[error("apply the cold tier's S3 encryption to the snapshot store")]
    Encryption(#[source] io::Error),
    /// A key lies outside the group's namespace of the store.
    #[error("snapshot key {key} lies outside the namespace of group {raft_group_id}")]
    OutsideGroup { raft_group_id: u32, key: String },
    /// A reference record names another version, node or group.
    #[error("snapshot reference record {record} is not this group's or node's")]
    InvalidReference { record: String },
    /// A reference record did not encode.
    #[error("encode a snapshot reference record")]
    ReferenceEncode(#[source] serde_json::Error),
    /// A reference record did not decode.
    #[error("decode snapshot reference record {record}")]
    ReferenceDecode {
        record: String,
        #[source]
        source: serde_json::Error,
    },
    #[error("snapshot serialize: {0}")]
    Serialize(String),
    #[error("snapshot deserialize: {0}")]
    Deserialize(String),
    /// A required field of a snapshot frame is absent.
    #[error("snapshot deserialize: missing {field}")]
    MissingField { field: &'static str },
    /// A producer record lacks a field every writer sets.
    #[error("snapshot producer '{producer_id}' has no {field}")]
    ProducerFieldMissing {
        producer_id: String,
        field: &'static str,
    },
    /// A producer record's digest is not 32 bytes.
    #[error("snapshot producer '{producer_id}' {field} is {len} bytes, not 32")]
    ProducerDigestLength {
        producer_id: String,
        field: &'static str,
        len: usize,
    },
    /// The group's history event names history id 0, which no proposer
    /// draws.
    #[error("snapshot history event has history id 0")]
    HistoryIdZero,
    /// The digest of the backup a group was restored from is not 32 bytes.
    #[error("snapshot history event's backup digest is {len} bytes, not 32")]
    BackupDigestLength { len: usize },
    /// A group snapshot whose leading frame names another format epoch than
    /// [`ursula_stream::FORMAT_EPOCH`], or that has no such frame
    /// (`found: None`, Ursula 0.5.x and earlier).
    #[error("{}", group_snapshot_epoch_refusal(*.found))]
    UnsupportedVersion { found: Option<u32> },
    #[error("snapshot io: {0}")]
    Io(#[from] io::Error),
}

fn group_snapshot_epoch_refusal(found: Option<u32>) -> String {
    let found = found.map_or_else(
        || "has no leading format-epoch frame (Ursula 0.5.x or earlier, format epoch 1)".to_owned(),
        |epoch| format!("is format epoch {epoch}"),
    );
    ursula_stream::format_epoch_refusal("snapshot", &found)
}

impl SnapshotStoreError {
    /// A backend failure of `operation` on `key`.
    pub fn backend(
        operation: SnapshotStoreOperation,
        key: impl Into<String>,
        source: opendal::Error,
    ) -> Self {
        Self::Backend {
            operation,
            key: key.into(),
            source: Box::new(source),
        }
    }

    /// The I/O error for a caller that speaks `io::Error`. It carries this
    /// error itself as its source, so the caller can still tell its variant
    /// apart by downcasting.
    pub fn into_io(self) -> io::Error {
        match self {
            Self::Io(err) => err,
            other => io::Error::other(other),
        }
    }
}

/// What one [`SnapshotStore::prune_retired`] pass did (#669).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SnapshotPrune {
    /// The store keeps no external objects of the group under reference
    /// control: it is inline, or it has no voters to check.
    Unmanaged,
    /// A configured voter of the group has published no reference, so the
    /// pass deleted nothing. Pruning fails closed: that voter may hold any
    /// of the group's objects as its current snapshot.
    VoterWithoutReference { node_id: u64 },
    /// The pass deleted `deleted` objects.
    Pruned { deleted: u64 },
}

impl SnapshotPrune {
    /// The objects the pass deleted.
    pub fn deleted(self) -> u64 {
        match self {
            Self::Pruned { deleted } => deleted,
            Self::Unmanaged | Self::VoterWithoutReference { .. } => 0,
        }
    }

    /// Whether a voter without a reference held the pass back.
    pub fn skipped_unreferenced_voter(self) -> bool {
        matches!(self, Self::VoterWithoutReference { .. })
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

    /// Makes the snapshot of `owner`'s group at log index `index` ready to
    /// write, with no I/O: an external store compresses it and draws a fresh
    /// [`OwnedKey`] for it, so the caller can hold the key before the write.
    /// The default keeps the bytes inline.
    fn stage<'a>(
        &'a self,
        _owner: SnapshotOwner,
        _index: u64,
        chunks: SnapshotBytesIterator,
    ) -> SnapshotStoreFuture<'a, StagedSnapshot> {
        Box::pin(async move {
            Ok(StagedSnapshot::Inline {
                bytes: collect_inline_snapshot(chunks).await?,
            })
        })
    }

    /// Writes a staged snapshot and returns its location. An object is
    /// created only if no object has its name
    /// ([`SnapshotStoreError::NameCollision`] otherwise), and never
    /// overwrites one. The default holds inline snapshots only.
    fn write_staged<'a>(
        &'a self,
        staged: StagedSnapshot,
    ) -> SnapshotStoreFuture<'a, SnapshotLocation> {
        Box::pin(async move {
            match staged {
                StagedSnapshot::Inline { bytes } => Ok(SnapshotLocation::Inline { bytes }),
                StagedSnapshot::Object(object) => Err(SnapshotStoreError::UnsupportedLocation {
                    store: SnapshotBackend::Inline,
                    location: object.location.kind(),
                }),
            }
        })
    }

    /// Lists `owner`'s directory of the store. A store without external
    /// objects has none.
    fn list_own<'a>(&'a self, _owner: SnapshotOwner) -> SnapshotStoreFuture<'a, OwnListing> {
        Box::pin(async move { Ok(OwnListing::default()) })
    }

    /// Deletes an owned object; a missing one is not an error. A store
    /// without external objects has nothing to delete.
    fn delete_own<'a>(&'a self, _key: OwnedKey) -> SnapshotStoreFuture<'a, ()> {
        Box::pin(async move { Ok(()) })
    }

    /// Retrieve a snapshot blob given its location.
    fn download<'a>(&'a self, location: &'a SnapshotLocation) -> SnapshotStoreFuture<'a, Vec<u8>>;

    /// Best-effort delete; missing is not an error.
    fn delete<'a>(&'a self, location: &'a SnapshotLocation) -> SnapshotStoreFuture<'a, ()>;

    /// Deletes the retired snapshot objects of one Raft group: every object
    /// older than the store's GC grace that `current`, no node's reference
    /// and no node's pin names (#669). Backends may only delete objects that
    /// cannot still be referenced by an OpenRaft snapshot pointer. It runs
    /// whatever `current` is, an inline current included. Inline snapshots
    /// have no external lifecycle, so the default deletes nothing.
    fn prune_retired<'a>(
        &'a self,
        _raft_group_id: u32,
        _current: &'a SnapshotLocation,
    ) -> SnapshotStoreFuture<'a, SnapshotPrune> {
        Box::pin(async move { Ok(SnapshotPrune::Unmanaged) })
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
                other => Err(SnapshotStoreError::UnsupportedLocation {
                    store: SnapshotBackend::Inline,
                    location: other.kind(),
                }),
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

    use super::OwnListing;
    use super::OwnedKey;
    use super::OwnedNamespace;
    use super::SnapshotBackend;
    use super::SnapshotBytesIterator;
    use super::SnapshotCompression;
    use super::SnapshotKey;
    use super::SnapshotLocation;
    use super::SnapshotOwner;
    use super::SnapshotPrune;
    use super::SnapshotReferenceConfig;
    use super::SnapshotStore;
    use super::SnapshotStoreConfigProblem;
    use super::SnapshotStoreError;
    use super::SnapshotStoreFuture;
    use super::StagedObject;
    use super::StagedSnapshot;
    use crate::SnapshotStoreOperation;

    const S3_SNAPSHOT_ZSTD_LEVEL: i32 = 3;
    /// The GC grace of a store built without one ([`S3SnapshotStore::with_gc_grace`]).
    const S3_SNAPSHOT_GC_GRACE: Duration = Duration::from_secs(60 * 60);
    const SNAPSHOT_REFERENCE_VERSION: u32 = ursula_stream::FORMAT_EPOCH;

    #[derive(serde::Deserialize, serde::Serialize)]
    struct SnapshotReference {
        version: u32,
        node_id: u64,
        raft_group_id: u32,
        snapshot_key: Option<String>,
    }

    /// How a store creates an object only if no object has its name.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum ConditionalCreate {
        /// The backend must support it, or the write is refused.
        Required,
        /// A HEAD before the write stands in for it, on an in-memory
        /// operator of a test that has no concurrent writer of the name.
        EmulatedForTests,
    }

    /// Bytes live in an opendal-managed S3 bucket under `{prefix}/group-{gid}/`.
    pub struct S3SnapshotStore {
        operator: Operator,
        prefix: String,
        references: Option<SnapshotReferenceConfig>,
        gc_grace: Duration,
        conditional_create: ConditionalCreate,
    }

    impl std::fmt::Debug for S3SnapshotStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("S3SnapshotStore")
                .field("prefix", &self.prefix)
                .field("references", &self.references)
                .field("gc_grace", &self.gc_grace)
                .field("conditional_create", &self.conditional_create)
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
                conditional_create: ConditionalCreate::Required,
            }
        }

        pub fn with_references(mut self, references: SnapshotReferenceConfig) -> Self {
            self.references = Some(references);
            self
        }

        /// How old an object must be before pruning may delete it (#669).
        /// A server passes `RaftConfig::snapshot_gc_grace`, a quarter of
        /// `raft.deleted_data_max_age` capped at the longer of an hour and two
        /// `raft.install_snapshot_timeout`s, the time a replica keeps the
        /// object it replaced pinned. An object nobody
        /// pins, such as the upload of a build a newer install overtook, is
        /// then deleted once it is that old.
        ///
        /// An object's age is its store modification time against this
        /// node's clock, and a build pins its upload only after the PUT
        /// returns. Both assume that the nodes' clocks agree with the
        /// store's, and that an upload completes, well within the grace.
        pub fn with_gc_grace(mut self, gc_grace: Duration) -> Self {
            self.gc_grace = gc_grace;
            self
        }

        /// In-memory opendal operator under `prefix`, for tests. The
        /// in-memory operator cannot create an object only if it is absent,
        /// so [`SnapshotStore::write_staged`] checks for the name first,
        /// which no concurrent writer of a test races.
        pub fn memory_for_tests(prefix: impl Into<String>) -> Result<Self, SnapshotStoreError> {
            let operator = Operator::via_iter(Scheme::Memory, [])
                .map_err(|err| SnapshotStoreError::Open(Box::new(err)))?;
            let mut store = Self::new(operator, prefix);
            store.conditional_create = ConditionalCreate::EmulatedForTests;
            Ok(store)
        }

        /// The keys this store lets `owner` commit.
        pub fn namespace(&self, owner: SnapshotOwner) -> OwnedNamespace {
            OwnedNamespace {
                prefix: self.prefix.clone(),
                owner,
            }
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
                .map_err(|err| SnapshotStoreError::backend(SnapshotStoreOperation::Put, key, err))
        }

        #[cfg(test)]
        pub(crate) async fn delete_raw_for_tests(
            &self,
            key: &str,
        ) -> Result<(), SnapshotStoreError> {
            self.operator.delete(key).await.map_err(|err| {
                SnapshotStoreError::backend(SnapshotStoreOperation::Delete, key, err)
            })
        }

        /// Build an S3 snapshot store from a [`ColdConfig`].
        /// Snapshot blobs share the cold bucket/credentials and use `prefix`
        /// (defaults to `snapshots`) for separation.
        pub fn try_new(
            config: &crate::ColdConfig,
            prefix: impl Into<String>,
        ) -> Result<Self, SnapshotStoreError> {
            let config_problem = |problem| SnapshotStoreError::Config { problem };
            let s3 = config
                .s3
                .as_ref()
                .ok_or_else(|| config_problem(SnapshotStoreConfigProblem::NoS3Config))?;
            let bucket = s3
                .bucket
                .as_deref()
                .ok_or_else(|| config_problem(SnapshotStoreConfigProblem::NoBucket))?;
            if bucket.trim().is_empty() {
                return Err(config_problem(SnapshotStoreConfigProblem::EmptyBucket));
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
                .map_err(SnapshotStoreError::Encryption)?;
            let operator = crate::cold_store::with_s3_resilience(
                Operator::new(builder)
                    .map_err(|err| SnapshotStoreError::Open(Box::new(err)))?
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

        /// Writes a content-addressed object, or keeps the object already
        /// under its name when its size matches.
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
                                SnapshotStoreError::backend(
                                    SnapshotStoreOperation::Head,
                                    object_key,
                                    stat_error,
                                )
                            })?;
                        if metadata.content_length() != stored_size_bytes {
                            return Err(SnapshotStoreError::SizeMismatch {
                                key: object_key.to_owned(),
                                expected: stored_size_bytes,
                                actual: metadata.content_length(),
                            });
                        }
                    }
                    Err(err) => {
                        return Err(SnapshotStoreError::backend(
                            SnapshotStoreOperation::Put,
                            object_key,
                            err,
                        ));
                    }
                }
            } else {
                // The production S3 backend supports conditional creation. This
                // fallback keeps capability-limited test backends useful.
                match self.operator.stat(object_key).await {
                    Ok(metadata) => {
                        if metadata.content_length() != stored_size_bytes {
                            return Err(SnapshotStoreError::SizeMismatch {
                                key: object_key.to_owned(),
                                expected: stored_size_bytes,
                                actual: metadata.content_length(),
                            });
                        }
                    }
                    Err(err) if matches!(err.kind(), ErrorKind::NotFound) => {
                        self.operator
                            .write(object_key, stored_bytes)
                            .await
                            .map_err(|write_error| {
                                SnapshotStoreError::backend(
                                    SnapshotStoreOperation::Put,
                                    object_key,
                                    write_error,
                                )
                            })?;
                    }
                    Err(err) => {
                        return Err(SnapshotStoreError::backend(
                            SnapshotStoreOperation::Head,
                            object_key,
                            err,
                        ));
                    }
                }
            }
            Ok(stored_size_bytes)
        }

        /// Creates `key` only if no object has the name: a found object is
        /// a [`SnapshotStoreError::NameCollision`], never reused or
        /// overwritten. A backend that cannot create conditionally is
        /// refused, but for the emulation of a test store.
        async fn create_object(
            &self,
            key: &str,
            stored_bytes: Vec<u8>,
        ) -> Result<(), SnapshotStoreError> {
            if self
                .operator
                .info()
                .full_capability()
                .write_with_if_not_exists
            {
                return match self
                    .operator
                    .write_with(key, stored_bytes)
                    .if_not_exists(true)
                    .await
                {
                    Ok(_) => Ok(()),
                    Err(err)
                        if matches!(
                            err.kind(),
                            ErrorKind::AlreadyExists | ErrorKind::ConditionNotMatch
                        ) =>
                    {
                        Err(SnapshotStoreError::NameCollision {
                            key: key.to_owned(),
                        })
                    }
                    Err(err) => Err(SnapshotStoreError::backend(
                        SnapshotStoreOperation::Put,
                        key,
                        err,
                    )),
                };
            }
            match self.conditional_create {
                ConditionalCreate::Required => {
                    Err(SnapshotStoreError::ConditionalCreateUnsupported {
                        key: key.to_owned(),
                    })
                }
                ConditionalCreate::EmulatedForTests => match self.operator.stat(key).await {
                    Ok(_) => Err(SnapshotStoreError::NameCollision {
                        key: key.to_owned(),
                    }),
                    Err(err) if matches!(err.kind(), ErrorKind::NotFound) => {
                        self.operator.write(key, stored_bytes).await.map_err(|err| {
                            SnapshotStoreError::backend(SnapshotStoreOperation::Put, key, err)
                        })
                    }
                    Err(err) => Err(SnapshotStoreError::backend(
                        SnapshotStoreOperation::Head,
                        key,
                        err,
                    )),
                },
            }
        }

        /// `key` checked against group `raft_group_id`'s namespace of the
        /// reference protocol.
        fn group_object<'k>(
            &self,
            raft_group_id: u32,
            key: &'k str,
        ) -> Result<&'k str, SnapshotStoreError> {
            if key.starts_with(&self.group_prefix(raft_group_id)) && key.ends_with(".snap") {
                Ok(key)
            } else {
                Err(SnapshotStoreError::OutsideGroup {
                    raft_group_id,
                    key: key.to_owned(),
                })
            }
        }
    }

    fn compress_snapshot_chunks(
        chunks: SnapshotBytesIterator,
    ) -> Result<(Vec<u8>, u64), SnapshotStoreError> {
        let mut size_bytes = 0u64;
        let mut encoder = zstd::stream::write::Encoder::new(Vec::new(), S3_SNAPSHOT_ZSTD_LEVEL)
            .map_err(SnapshotStoreError::Compress)?;
        for chunk in chunks {
            let chunk = chunk?;
            size_bytes = size_bytes
                .checked_add(chunk.len() as u64)
                .ok_or(SnapshotStoreError::BodyTooLarge)?;
            encoder
                .write_all(&chunk)
                .map_err(SnapshotStoreError::Compress)?;
        }
        let stored_bytes = encoder.finish().map_err(SnapshotStoreError::Compress)?;
        Ok((stored_bytes, size_bytes))
    }

    /// Compresses `chunks` off the async worker.
    async fn compress_off_worker(
        chunks: SnapshotBytesIterator,
    ) -> Result<(Vec<u8>, u64), SnapshotStoreError> {
        tokio::task::spawn_blocking(move || compress_snapshot_chunks(chunks))
            .await
            .map_err(|err| {
                SnapshotStoreError::Io(io::Error::other(format!("join s3 snapshot encoder: {err}")))
            })?
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
                self.group_object(raft_group_id, key)?;
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
                .map_err(SnapshotStoreError::ReferenceEncode)?;
                self.operator.write(&pin_key, record).await.map_err(|err| {
                    SnapshotStoreError::backend(SnapshotStoreOperation::Put, &pin_key, err)
                })
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
                    .map_err(|err| {
                        SnapshotStoreError::backend(SnapshotStoreOperation::List, &prefix, err)
                    })?;
                for entry in entries {
                    if !entry.metadata().mode().is_file() || !entry.path().ends_with(".json") {
                        continue;
                    }
                    let bytes = self.operator.read(entry.path()).await.map_err(|err| {
                        SnapshotStoreError::backend(SnapshotStoreOperation::Get, entry.path(), err)
                    })?;
                    let reference: SnapshotReference = serde_json::from_slice(&bytes.to_vec())
                        .map_err(|source| SnapshotStoreError::ReferenceDecode {
                            record: entry.path().to_owned(),
                            source,
                        })?;
                    if reference.version != SNAPSHOT_REFERENCE_VERSION
                        || reference.node_id != references.node_id
                        || reference.raft_group_id != raft_group_id
                    {
                        return Err(SnapshotStoreError::InvalidReference {
                            record: entry.path().to_owned(),
                        });
                    }
                    if reference
                        .snapshot_key
                        .as_deref()
                        .is_none_or(|key| !retained.contains(key))
                    {
                        self.operator.delete(entry.path()).await.map_err(|err| {
                            SnapshotStoreError::backend(
                                SnapshotStoreOperation::Delete,
                                entry.path(),
                                err,
                            )
                        })?;
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
                let stored_bytes = zstd::bulk::compress(&bytes, S3_SNAPSHOT_ZSTD_LEVEL)
                    .map_err(SnapshotStoreError::Compress)?;
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
                let (stored_bytes, size_bytes) = compress_off_worker(chunks).await?;
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

        fn stage<'a>(
            &'a self,
            owner: SnapshotOwner,
            index: u64,
            chunks: SnapshotBytesIterator,
        ) -> SnapshotStoreFuture<'a, StagedSnapshot> {
            Box::pin(async move {
                let (stored, size_bytes) = compress_off_worker(chunks).await?;
                let key = OwnedKey {
                    owner,
                    index,
                    nonce: rand::random(),
                };
                let location = SnapshotLocation::S3 {
                    key: key.render(&self.prefix),
                    size_bytes,
                    stored_size_bytes: stored.len() as u64,
                    compression: SnapshotCompression::Zstd,
                    shared_object: false,
                };
                Ok(StagedSnapshot::Object(StagedObject {
                    key,
                    location,
                    stored,
                }))
            })
        }

        fn write_staged<'a>(
            &'a self,
            staged: StagedSnapshot,
        ) -> SnapshotStoreFuture<'a, SnapshotLocation> {
            Box::pin(async move {
                match staged {
                    StagedSnapshot::Inline { bytes } => Ok(SnapshotLocation::Inline { bytes }),
                    StagedSnapshot::Object(StagedObject {
                        key,
                        location,
                        stored,
                    }) => {
                        self.create_object(&key.render(&self.prefix), stored)
                            .await?;
                        Ok(location)
                    }
                }
            })
        }

        fn list_own<'a>(&'a self, owner: SnapshotOwner) -> SnapshotStoreFuture<'a, OwnListing> {
            Box::pin(async move {
                let directory = owner.directory(&self.prefix);
                let entries = self
                    .operator
                    .list_with(&directory)
                    .recursive(true)
                    .await
                    .map_err(|err| {
                        SnapshotStoreError::backend(SnapshotStoreOperation::List, &directory, err)
                    })?;
                let mut listing = OwnListing::default();
                for entry in entries {
                    if !entry.metadata().mode().is_file() {
                        continue;
                    }
                    match OwnedKey::parse(&self.prefix, entry.path()) {
                        Ok(key) if key.owner == owner => listing.owned.push(key),
                        Ok(_) | Err(_) => listing.foreign.push(entry.path().to_owned()),
                    }
                }
                listing.owned.sort_unstable();
                listing.foreign.sort_unstable();
                Ok(listing)
            })
        }

        fn delete_own<'a>(&'a self, key: OwnedKey) -> SnapshotStoreFuture<'a, ()> {
            Box::pin(async move {
                let key = key.render(&self.prefix);
                match self.operator.delete(&key).await {
                    Ok(()) => Ok(()),
                    Err(err) if matches!(err.kind(), ErrorKind::NotFound) => Ok(()),
                    Err(err) => Err(SnapshotStoreError::backend(
                        SnapshotStoreOperation::Delete,
                        key,
                        err,
                    )),
                }
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
                    return Err(SnapshotStoreError::UnsupportedLocation {
                        store: SnapshotBackend::S3,
                        location: location.kind(),
                    });
                };
                let buf = self.operator.read(key).await.map_err(|err| {
                    if matches!(err.kind(), ErrorKind::NotFound) {
                        SnapshotStoreError::NotFound { key: key.clone() }
                    } else {
                        SnapshotStoreError::backend(SnapshotStoreOperation::Get, key, err)
                    }
                })?;
                let stored_bytes = buf.to_vec();
                let expected_stored_size = location.stored_size_hint();
                if stored_bytes.len() as u64 != expected_stored_size {
                    return Err(SnapshotStoreError::SizeMismatch {
                        key: key.clone(),
                        expected: expected_stored_size,
                        actual: stored_bytes.len() as u64,
                    });
                }
                let bytes = match location.compression() {
                    SnapshotCompression::None => stored_bytes,
                    SnapshotCompression::Zstd => zstd::bulk::decompress(
                        &stored_bytes,
                        usize::try_from(*size_bytes).map_err(|source| {
                            SnapshotStoreError::Decompress {
                                key: key.clone(),
                                source: io::Error::new(io::ErrorKind::InvalidData, source),
                            }
                        })?,
                    )
                    .map_err(|source| SnapshotStoreError::Decompress {
                        key: key.clone(),
                        source,
                    })?,
                };
                if bytes.len() as u64 != *size_bytes {
                    return Err(SnapshotStoreError::LogicalSizeMismatch {
                        key: key.clone(),
                        expected: *size_bytes,
                        actual: bytes.len() as u64,
                    });
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
                    Err(err) if matches!(err.kind(), ErrorKind::NotFound) => Ok(()),
                    Err(err) => Err(SnapshotStoreError::backend(
                        SnapshotStoreOperation::Delete,
                        key,
                        err,
                    )),
                }
            })
        }

        /// Deletes every `.snap` object of the group older than the GC grace
        /// that `current`, no reference record and no pin names (#669). No
        /// count of retired objects is kept: a replica pins the object it
        /// replaced for the grace, which covers a send of its pointer that
        /// started before the switch. It runs with an inline `current` too.
        ///
        /// Fails closed: while a configured voter of the group has published
        /// no reference, nothing is deleted, since that voter may hold any of
        /// the group's objects. Every reference record found is honoured,
        /// including one a node that left the cluster wrote, whose object
        /// stays until an operator deletes that record.
        fn prune_retired<'a>(
            &'a self,
            raft_group_id: u32,
            current: &'a SnapshotLocation,
        ) -> SnapshotStoreFuture<'a, SnapshotPrune> {
            Box::pin(async move {
                let Some(references) = &self.references else {
                    return Ok(SnapshotPrune::Unmanaged);
                };
                let expected_voters = references.voters_for(raft_group_id);
                if expected_voters.is_empty() {
                    return Ok(SnapshotPrune::Unmanaged);
                }
                let group_prefix = self.group_prefix(raft_group_id);
                let mut retained = HashSet::new();
                if let SnapshotLocation::S3 { key, .. } = current {
                    retained.insert(key.clone());
                }
                for node_id in expected_voters {
                    let reference_key = self.reference_key(raft_group_id, *node_id);
                    let reference_bytes = match self.operator.read(&reference_key).await {
                        Ok(bytes) => bytes,
                        Err(error) if matches!(error.kind(), ErrorKind::NotFound) => {
                            tracing::debug!(
                                raft_group_id,
                                node_id,
                                "deferring S3 snapshot pruning until every voter publishes a reference"
                            );
                            return Ok(SnapshotPrune::VoterWithoutReference { node_id: *node_id });
                        }
                        Err(error) => {
                            return Err(SnapshotStoreError::backend(
                                SnapshotStoreOperation::Get,
                                reference_key,
                                error,
                            ));
                        }
                    };
                    let reference: SnapshotReference =
                        serde_json::from_slice(&reference_bytes.to_vec()).map_err(|source| {
                            SnapshotStoreError::ReferenceDecode {
                                record: reference_key.clone(),
                                source,
                            }
                        })?;
                    if reference.version != SNAPSHOT_REFERENCE_VERSION
                        || reference.node_id != *node_id
                        || reference.raft_group_id != raft_group_id
                    {
                        return Err(SnapshotStoreError::InvalidReference {
                            record: reference_key,
                        });
                    }
                    if let Some(key) = reference.snapshot_key {
                        self.group_object(raft_group_id, &key)?;
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
                    .map_err(|error| {
                        SnapshotStoreError::backend(
                            SnapshotStoreOperation::List,
                            &group_prefix,
                            error,
                        )
                    })?;
                let references_prefix = format!("{group_prefix}references/");
                for entry in &entries {
                    if !entry.metadata().mode().is_file()
                        || !entry.path().starts_with(&references_prefix)
                        || !entry.path().ends_with(".json")
                    {
                        continue;
                    }
                    let bytes = match self.operator.read(entry.path()).await {
                        Ok(bytes) => bytes,
                        // A pin its node released after the listing (#490):
                        // it protects nothing any more.
                        Err(error) if matches!(error.kind(), ErrorKind::NotFound) => {
                            continue;
                        }
                        Err(error) => {
                            return Err(SnapshotStoreError::backend(
                                SnapshotStoreOperation::Get,
                                entry.path(),
                                error,
                            ));
                        }
                    };
                    let reference: SnapshotReference = serde_json::from_slice(&bytes.to_vec())
                        .map_err(|source| SnapshotStoreError::ReferenceDecode {
                            record: entry.path().to_owned(),
                            source,
                        })?;
                    if reference.version != SNAPSHOT_REFERENCE_VERSION
                        || reference.raft_group_id != raft_group_id
                    {
                        return Err(SnapshotStoreError::InvalidReference {
                            record: entry.path().to_owned(),
                        });
                    }
                    if let Some(key) = reference.snapshot_key {
                        self.group_object(raft_group_id, &key)?;
                        retained.insert(key);
                    }
                }
                let mut deleted = 0_u64;
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
                            .map_err(|error| {
                                SnapshotStoreError::backend(
                                    SnapshotStoreOperation::Head,
                                    entry.path(),
                                    error,
                                )
                            })?
                            .last_modified()
                            .map(Into::into)
                            .or_else(|| self.gc_grace.is_zero().then_some(SystemTime::UNIX_EPOCH)),
                    };
                    if !modified.is_some_and(|modified: SystemTime| modified <= cutoff) {
                        continue;
                    }
                    match self.operator.delete(entry.path()).await {
                        Ok(()) => {}
                        // Another replica's pass deleted it first.
                        Err(error) if matches!(error.kind(), ErrorKind::NotFound) => {
                            continue;
                        }
                        Err(error) => {
                            return Err(SnapshotStoreError::backend(
                                SnapshotStoreOperation::Delete,
                                entry.path(),
                                error,
                            ));
                        }
                    }
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
                Ok(SnapshotPrune::Pruned { deleted })
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
                        Some(self.group_object(raft_group_id, key)?.to_owned())
                    }
                    SnapshotLocation::Inline { .. } | SnapshotLocation::Local { .. } => None,
                };
                let reference = serde_json::to_vec(&SnapshotReference {
                    version: SNAPSHOT_REFERENCE_VERSION,
                    node_id: references.node_id,
                    raft_group_id,
                    snapshot_key,
                })
                .map_err(SnapshotStoreError::ReferenceEncode)?;
                let reference_key = self.reference_key(raft_group_id, references.node_id);
                self.operator
                    .write(&reference_key, reference)
                    .await
                    .map_err(|error| {
                        SnapshotStoreError::backend(
                            SnapshotStoreOperation::Put,
                            reference_key,
                            error,
                        )
                    })
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
                    if matches!(err.kind(), ErrorKind::NotFound) {
                        SnapshotStoreError::NotFound { key: key.clone() }
                    } else {
                        SnapshotStoreError::backend(SnapshotStoreOperation::Head, key, err)
                    }
                })?;
                let actual = meta.content_length();
                let expected = location.stored_size_hint();
                if actual != expected {
                    return Err(SnapshotStoreError::SizeMismatch {
                        key: key.clone(),
                        expected,
                        actual,
                    });
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
                    Err(err) if matches!(err.kind(), ErrorKind::NotFound) => Ok(()),
                    Err(err) => Err(SnapshotStoreError::backend(
                        SnapshotStoreOperation::Head,
                        probe,
                        err,
                    )),
                }
            })
        }
    }
}

#[cfg(not(madsim))]
pub use s3::S3SnapshotStore;

/// Pick a snapshot store from a typed `ursula_config::RaftSnapshotConfig`. Returns `None`
/// when the backend is "inline" (the default) so callers can fall back to
/// [`default_snapshot_store`] without instantiating anything. `gc_grace` is
/// how old an external object must be before pruning may delete it
/// ([`S3SnapshotStore::with_gc_grace`]).
pub fn snapshot_store_from_config(
    cfg: &ursula_config::RaftSnapshotConfig,
    cold_cfg: &crate::ColdConfig,
    references: SnapshotReferenceConfig,
    gc_grace: std::time::Duration,
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
                S3SnapshotStore::try_new(cold_cfg, &prefix)?
                    .with_references(references)
                    .with_gc_grace(gc_grace),
            )))
        }
        #[cfg(madsim)]
        ursula_config::RaftSnapshotBackend::S3 => {
            // No S3 store under madsim, so no references or grace to apply.
            let _no_store = (references, gc_grace);
            Err(SnapshotStoreError::Config {
                problem: SnapshotStoreConfigProblem::NoS3UnderSimulation,
            })
        }
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
            Err(SnapshotStoreError::UnsupportedLocation {
                store: SnapshotBackend::Inline,
                location: SnapshotLocationKind::Local,
            })
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
        let garbage = SnapshotPointer::decode(b"\x00garbage");
        assert!(
            matches!(garbage, Err(SnapshotStoreError::Deserialize(_))),
            "{garbage:?}"
        );
        let json_like = SnapshotPointer::decode(b" \n{\"snapshot_id\":1}");
        assert!(
            matches!(json_like, Err(SnapshotStoreError::Deserialize(_))),
            "{json_like:?}"
        );
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
            .with_gc_grace(std::time::Duration::ZERO);
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
        store.prune_retired(7, &old).await.unwrap();
        assert_eq!(store.download(&old).await.unwrap(), b"old");
        assert_eq!(store.download(&incoming).await.unwrap(), b"incoming");
        assert_eq!(store.download(&abandoned).await.unwrap(), b"abandoned");
        assert_eq!(store.download(&other_pin).await.unwrap(), b"other");
        store
            .reconcile_reference_pins(7, std::slice::from_ref(&incoming))
            .await
            .unwrap();
        store.prune_retired(7, &old).await.unwrap();
        assert!(matches!(
            store.download(&abandoned).await,
            Err(SnapshotStoreError::NotFound { .. })
        ));
        assert_eq!(store.download(&incoming).await.unwrap(), b"incoming");
        assert_eq!(store.download(&other_pin).await.unwrap(), b"other");
        store.publish_reference(7, &incoming).await.unwrap();
        store.prune_retired(7, &incoming).await.unwrap();
        assert!(matches!(
            store.download(&old).await,
            Err(SnapshotStoreError::NotFound { .. })
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
            matches!(err, SnapshotStoreError::NotFound { .. }),
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
            .with_gc_grace(Duration::ZERO);
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

        assert_eq!(
            store.prune_retired(7, &current).await.unwrap(),
            SnapshotPrune::VoterWithoutReference { node_id: 2 }
        );
        assert_eq!(store.download(&retired).await.unwrap(), b"retired");

        for (node_id, location) in [(2, &node_two), (3, &node_three)] {
            let SnapshotLocation::S3 { key, .. } = location else {
                panic!("expected S3 location")
            };
            store
                .write_raw_for_tests(
                    &format!("snapshots/group-7/references/node-{node_id}.json"),
                    serde_json::to_vec(&serde_json::json!({
                        // References carry the format epoch.
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
        assert_eq!(
            store.prune_retired(7, &current).await.unwrap(),
            SnapshotPrune::Pruned { deleted: 1 }
        );
        assert!(matches!(
            store.download(&retired).await,
            Err(SnapshotStoreError::NotFound { .. })
        ));
        assert_eq!(store.download(&node_two).await.unwrap(), b"node-two");
        assert_eq!(store.download(&node_three).await.unwrap(), b"node-three");
        assert_eq!(store.download(&current).await.unwrap(), b"current");
    }

    #[cfg(not(madsim))]
    fn gc_store(voters: &[u64], gc_grace: std::time::Duration) -> S3SnapshotStore {
        S3SnapshotStore::memory_for_tests("snapshots")
            .unwrap()
            .with_references(SnapshotReferenceConfig {
                node_id: 1,
                default_voters: voters.iter().copied().collect(),
                per_group_voters: BTreeMap::new(),
            })
            .with_gc_grace(gc_grace)
    }

    #[cfg(not(madsim))]
    async fn write_reference(store: &S3SnapshotStore, path: &str, node_id: u64, key: Option<&str>) {
        store
            .write_raw_for_tests(
                path,
                serde_json::to_vec(&serde_json::json!({
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

    #[cfg(not(madsim))]
    fn s3_key(location: &SnapshotLocation) -> &str {
        let SnapshotLocation::S3 { key, .. } = location else {
            panic!("expected S3 location")
        };
        key
    }

    #[cfg(not(madsim))]
    fn is_gone(result: Result<Vec<u8>, SnapshotStoreError>) -> bool {
        matches!(result, Err(SnapshotStoreError::NotFound { .. }))
    }

    /// #669: no count of retired objects is kept. Every object past the
    /// grace that nothing names goes, the newest of them too.
    #[cfg(not(madsim))]
    #[tokio::test]
    async fn s3_pruning_keeps_no_retired_object_nothing_names() {
        let store = gc_store(&[1], std::time::Duration::ZERO);
        let mut retired = Vec::new();
        for id in ["first", "second", "newest-retired"] {
            retired.push(
                store
                    .upload(test_key(7, id), id.as_bytes().to_vec().into())
                    .await
                    .unwrap(),
            );
        }
        let current = store
            .upload(test_key(7, "current"), b"current".to_vec().into())
            .await
            .unwrap();
        store.publish_reference(7, &current).await.unwrap();

        assert_eq!(
            store.prune_retired(7, &current).await.unwrap(),
            SnapshotPrune::Pruned { deleted: 3 }
        );
        for location in &retired {
            assert!(is_gone(store.download(location).await));
        }
        assert_eq!(store.download(&current).await.unwrap(), b"current");
        // A second pass finds nothing left to delete.
        assert_eq!(
            store.prune_retired(7, &current).await.unwrap(),
            SnapshotPrune::Pruned { deleted: 0 }
        );
    }

    /// #669: a replica whose current snapshot is inline, after an inline
    /// fallback, still deletes the objects it no longer uses.
    #[cfg(not(madsim))]
    #[tokio::test]
    async fn s3_pruning_runs_while_the_current_snapshot_is_inline() {
        let store = gc_store(&[1], std::time::Duration::ZERO);
        let old = store
            .upload(test_key(7, "old"), b"old".to_vec().into())
            .await
            .unwrap();
        store.publish_reference(7, &old).await.unwrap();
        let inline = SnapshotLocation::Inline {
            bytes: b"inline".to_vec(),
        };
        store.publish_reference(7, &inline).await.unwrap();

        assert_eq!(
            store.prune_retired(7, &inline).await.unwrap(),
            SnapshotPrune::Pruned { deleted: 1 }
        );
        assert!(is_gone(store.download(&old).await));
    }

    /// #669: a pin keeps its object past the grace until its node drops it.
    #[cfg(not(madsim))]
    #[tokio::test]
    async fn s3_pruning_spares_a_pinned_object_until_the_pin_goes() {
        let store = gc_store(&[1], std::time::Duration::ZERO);
        let retiring = store
            .upload(test_key(7, "retiring"), b"retiring".to_vec().into())
            .await
            .unwrap();
        let current = store
            .upload(test_key(7, "current"), b"current".to_vec().into())
            .await
            .unwrap();
        store.pin_reference(7, &retiring).await.unwrap();
        store.pin_reference(7, &current).await.unwrap();
        store.publish_reference(7, &current).await.unwrap();

        assert_eq!(
            store.prune_retired(7, &current).await.unwrap(),
            SnapshotPrune::Pruned { deleted: 0 }
        );
        assert_eq!(store.download(&retiring).await.unwrap(), b"retiring");
        store
            .reconcile_reference_pins(7, std::slice::from_ref(&current))
            .await
            .unwrap();
        assert_eq!(
            store.prune_retired(7, &current).await.unwrap(),
            SnapshotPrune::Pruned { deleted: 1 }
        );
        assert!(is_gone(store.download(&retiring).await));
        assert_eq!(store.download(&current).await.unwrap(), b"current");
    }

    /// #669: the object a voter that is down still names stays, and so does
    /// the one a node that left the cluster names in the reference it left.
    #[cfg(not(madsim))]
    #[tokio::test]
    async fn s3_pruning_keeps_what_a_down_voter_or_a_departed_node_names() {
        let store = gc_store(&[1, 2], std::time::Duration::ZERO);
        let down = store
            .upload(test_key(7, "down"), b"down".to_vec().into())
            .await
            .unwrap();
        let departed = store
            .upload(test_key(7, "departed"), b"departed".to_vec().into())
            .await
            .unwrap();
        let retired = store
            .upload(test_key(7, "retired"), b"retired".to_vec().into())
            .await
            .unwrap();
        let current = store
            .upload(test_key(7, "current"), b"current".to_vec().into())
            .await
            .unwrap();
        store.publish_reference(7, &current).await.unwrap();
        write_reference(
            &store,
            "snapshots/group-7/references/node-2.json",
            2,
            Some(s3_key(&down)),
        )
        .await;
        write_reference(
            &store,
            "snapshots/group-7/references/node-9.json",
            9,
            Some(s3_key(&departed)),
        )
        .await;

        assert_eq!(
            store.prune_retired(7, &current).await.unwrap(),
            SnapshotPrune::Pruned { deleted: 1 }
        );
        assert!(is_gone(store.download(&retired).await));
        assert_eq!(store.download(&down).await.unwrap(), b"down");
        assert_eq!(store.download(&departed).await.unwrap(), b"departed");
        assert_eq!(store.download(&current).await.unwrap(), b"current");
    }

    /// #669: pruning fails closed while a configured voter has published no
    /// reference, inline current or not.
    #[cfg(not(madsim))]
    #[tokio::test]
    async fn s3_pruning_deletes_nothing_while_a_voter_has_no_reference() {
        let store = gc_store(&[1, 2, 3], std::time::Duration::ZERO);
        let retired = store
            .upload(test_key(7, "retired"), b"retired".to_vec().into())
            .await
            .unwrap();
        let current = store
            .upload(test_key(7, "current"), b"current".to_vec().into())
            .await
            .unwrap();
        store.publish_reference(7, &current).await.unwrap();
        write_reference(&store, "snapshots/group-7/references/node-3.json", 3, None).await;

        assert_eq!(
            store.prune_retired(7, &current).await.unwrap(),
            SnapshotPrune::VoterWithoutReference { node_id: 2 }
        );
        let inline = SnapshotLocation::Inline { bytes: Vec::new() };
        assert_eq!(
            store.prune_retired(7, &inline).await.unwrap(),
            SnapshotPrune::VoterWithoutReference { node_id: 2 }
        );
        assert_eq!(store.download(&retired).await.unwrap(), b"retired");
    }

    /// #669: an object younger than the grace stays even when nothing names
    /// it, such as the upload of a build that a newer install overtook. The
    /// in-memory operator records no modification time, so this runs on a
    /// local directory.
    #[cfg(not(madsim))]
    #[tokio::test]
    async fn s3_pruning_spares_an_object_younger_than_the_grace() {
        let root = tempfile::tempdir().unwrap();
        let operator = opendal::Operator::new(
            opendal::services::Fs::default().root(root.path().to_str().unwrap()),
        )
        .unwrap()
        .finish();
        let store = S3SnapshotStore::new(operator, "snapshots")
            .with_references(SnapshotReferenceConfig {
                node_id: 1,
                default_voters: BTreeSet::from([1]),
                per_group_voters: BTreeMap::new(),
            })
            .with_gc_grace(std::time::Duration::from_secs(60 * 60));
        let young = store
            .upload(test_key(7, "young"), b"young".to_vec().into())
            .await
            .unwrap();
        let current = store
            .upload(test_key(7, "current"), b"current".to_vec().into())
            .await
            .unwrap();
        store.publish_reference(7, &current).await.unwrap();

        assert_eq!(
            store.prune_retired(7, &current).await.unwrap(),
            SnapshotPrune::Pruned { deleted: 0 }
        );
        assert_eq!(store.download(&young).await.unwrap(), b"young");
    }

    fn owner(node_id: u64, raft_group_id: u32) -> SnapshotOwner {
        SnapshotOwner {
            node_id,
            raft_group_id,
        }
    }

    fn body(bytes: &'static [u8]) -> SnapshotBytesIterator {
        Box::new(std::iter::once(Ok(Bytes::from_static(bytes))))
    }

    /// An owned key renders as `{prefix}/node-{node}/group-{gid}/{index:020}-{nonce:016x}.snap`
    /// and parses back to itself.
    #[test]
    fn an_owned_key_renders_and_parses_back() {
        let key = OwnedKey {
            owner: owner(1, 7),
            index: 42,
            nonce: 0xab,
        };
        let rendered = key.render("snapshots");
        assert_eq!(
            rendered,
            "snapshots/node-1/group-7/00000000000000000042-00000000000000ab.snap"
        );
        assert_eq!(OwnedKey::parse("snapshots", &rendered), Ok(key));
        let largest = OwnedKey {
            owner: owner(u64::MAX, u32::MAX),
            index: u64::MAX,
            nonce: u64::MAX,
        };
        assert_eq!(
            OwnedKey::parse("p", &largest.render("p")),
            Ok(largest),
            "every field round-trips at its maximum"
        );
    }

    /// The parser compares components, never text prefixes: `node-1` never
    /// matches `node-10`, a prefix must end at a `/`, and every number must
    /// be in the form a render writes.
    #[test]
    fn the_owned_key_parser_compares_components() {
        let problem =
            |prefix: &str, key: &str| OwnedKey::parse(prefix, key).expect_err(key).problem;
        let name = "00000000000000000042-00000000000000ab.snap";
        let node_ten = OwnedKey::parse("snapshots", &format!("snapshots/node-10/group-7/{name}"))
            .expect("node 10's key parses");
        assert_eq!(node_ten.owner, owner(10, 7));
        assert_ne!(node_ten.owner, owner(1, 7));
        for (prefix, key, expected) in [
            (
                "snap",
                format!("snapshots/node-1/group-7/{name}"),
                OwnedKeyProblem::OutsidePrefix,
            ),
            (
                "snapshots",
                format!("other/node-1/group-7/{name}"),
                OwnedKeyProblem::OutsidePrefix,
            ),
            (
                "snapshots",
                format!("snapshots/node-1/{name}"),
                OwnedKeyProblem::Shape,
            ),
            (
                "snapshots",
                format!("snapshots/node-1/group-7/x/{name}"),
                OwnedKeyProblem::Shape,
            ),
            (
                "snapshots",
                format!("snapshots/node-01/group-7/{name}"),
                OwnedKeyProblem::Node,
            ),
            (
                "snapshots",
                format!("snapshots/node-+1/group-7/{name}"),
                OwnedKeyProblem::Node,
            ),
            (
                "snapshots",
                format!("snapshots/nodes-1/group-7/{name}"),
                OwnedKeyProblem::Node,
            ),
            (
                "snapshots",
                format!("snapshots/node-1/group-07/{name}"),
                OwnedKeyProblem::Group,
            ),
            (
                "snapshots",
                format!("snapshots/node-1/group-4294967296/{name}"),
                OwnedKeyProblem::Group,
            ),
            (
                "snapshots",
                "snapshots/node-1/group-7/42-00000000000000ab.snap".to_owned(),
                OwnedKeyProblem::Name,
            ),
            (
                "snapshots",
                "snapshots/node-1/group-7/00000000000000000042-00000000000000AB.snap".to_owned(),
                OwnedKeyProblem::Name,
            ),
            (
                "snapshots",
                "snapshots/node-1/group-7/00000000000000000042-00000000000000ab.json".to_owned(),
                OwnedKeyProblem::Name,
            ),
            (
                "snapshots",
                "snapshots/node-1/group-7/99999999999999999999-00000000000000ab.snap".to_owned(),
                OwnedKeyProblem::Name,
            ),
            (
                "snapshots",
                "snapshots/group-7/objects/abc.snap".to_owned(),
                OwnedKeyProblem::Node,
            ),
        ] {
            assert_eq!(problem(prefix, &key), expected, "{key}");
        }
    }

    /// A pointer is owned when it is inline or names a key of this node and
    /// group, and refused otherwise.
    #[test]
    fn an_owned_pointer_is_inline_or_under_its_own_directory() {
        let namespace = OwnedNamespace {
            prefix: "snapshots".to_owned(),
            owner: owner(1, 7),
        };
        let s3 = |key: String| SnapshotPointer {
            snapshot_id: "s".to_owned(),
            location: SnapshotLocation::S3 {
                key,
                size_bytes: 1,
                stored_size_bytes: 1,
                compression: SnapshotCompression::Zstd,
                shared_object: false,
            },
        };
        let own = OwnedKey {
            owner: owner(1, 7),
            index: 3,
            nonce: 4,
        }
        .render("snapshots");
        let owned = OwnedPointer::try_from((s3(own.clone()), &namespace)).expect("own key");
        assert_eq!(owned.pointer().location, s3(own).location);
        OwnedPointer::try_from((
            SnapshotPointer {
                snapshot_id: "s".to_owned(),
                location: SnapshotLocation::Inline { bytes: vec![1] },
            },
            &namespace,
        ))
        .expect("an inline pointer is owned");
        for foreign in [
            OwnedKey {
                owner: owner(10, 7),
                index: 3,
                nonce: 4,
            }
            .render("snapshots"),
            OwnedKey {
                owner: owner(1, 8),
                index: 3,
                nonce: 4,
            }
            .render("snapshots"),
            "snapshots/group-7/objects/abc.snap".to_owned(),
        ] {
            let refused = OwnedPointer::try_from((s3(foreign.clone()), &namespace))
                .expect_err("a foreign key");
            assert_eq!(refused, NotOwned::ForeignKey {
                key: foreign,
                directory: "snapshots/node-1/group-7/".to_owned(),
            });
        }
        let local = OwnedPointer::try_from((
            SnapshotPointer {
                snapshot_id: "s".to_owned(),
                location: SnapshotLocation::Local {
                    path: PathBuf::from("/tmp/s.snap"),
                    size_bytes: 1,
                },
            },
            &namespace,
        ));
        assert!(matches!(local, Err(NotOwned::LocalPath { .. })));
    }

    /// `into_io` keeps the store error as the I/O error's source, so the
    /// caller can still tell the variant apart.
    #[test]
    fn into_io_keeps_the_typed_store_error() {
        let io = SnapshotStoreError::NotFound {
            key: "snapshots/k.snap".to_owned(),
        }
        .into_io();
        assert!(matches!(
            io.get_ref()
                .and_then(|source| source.downcast_ref::<SnapshotStoreError>()),
            Some(SnapshotStoreError::NotFound { key }) if key == "snapshots/k.snap"
        ));
        let raw = io::Error::new(io::ErrorKind::PermissionDenied, "raw");
        assert_eq!(
            SnapshotStoreError::Io(raw).into_io().kind(),
            io::ErrorKind::PermissionDenied,
            "an I/O error passes through"
        );
    }

    /// The inline store stages and writes inline, lists nothing and deletes
    /// nothing.
    #[tokio::test]
    async fn the_inline_store_stages_inline() {
        let store = InlineSnapshotStore;
        let staged = store.stage(owner(1, 7), 3, body(b"inline")).await.unwrap();
        assert!(staged.key().is_none());
        assert_eq!(
            store.write_staged(staged).await.unwrap(),
            SnapshotLocation::Inline {
                bytes: b"inline".to_vec()
            }
        );
        assert_eq!(
            store.list_own(owner(1, 7)).await.unwrap(),
            OwnListing::default()
        );
    }

    /// `stage` draws a fresh owned key and does no I/O. `write_staged`
    /// creates the object, and the location it returns is the one the stage
    /// named before the write.
    #[cfg(not(madsim))]
    #[tokio::test]
    async fn s3_stage_names_a_fresh_owned_key_and_write_staged_creates_it() {
        let store = S3SnapshotStore::memory_for_tests("snapshots").unwrap();
        let payload = b"owned snapshot body".repeat(64);
        let chunks: SnapshotBytesIterator =
            Box::new(std::iter::once(Ok(Bytes::from(payload.clone()))));
        let staged = store.stage(owner(1, 7), 42, chunks).await.unwrap();
        let key = *staged.key().expect("an S3 stage names an object");
        assert_eq!((key.owner, key.index), (owner(1, 7), 42));
        let location = staged.location();
        let SnapshotLocation::S3 {
            key: rendered,
            size_bytes,
            stored_size_bytes,
            compression,
            shared_object,
        } = &location
        else {
            panic!("expected an S3 location, got {location:?}");
        };
        assert_eq!(rendered, &key.render("snapshots"));
        assert_eq!(*size_bytes, payload.len() as u64);
        assert!(*stored_size_bytes < *size_bytes);
        assert_eq!(*compression, SnapshotCompression::Zstd);
        assert!(!*shared_object, "an owned object is never shared");
        assert!(
            matches!(
                store.download(&location).await,
                Err(SnapshotStoreError::NotFound { .. })
            ),
            "the stage wrote nothing"
        );
        assert_eq!(store.write_staged(staged).await.unwrap(), location);
        assert_eq!(store.download(&location).await.unwrap(), payload);
        store.verify_uploaded(&location).await.unwrap();

        let other = store.stage(owner(1, 7), 42, body(b"x")).await.unwrap();
        assert_ne!(
            other.key().map(|other| other.nonce),
            Some(key.nonce),
            "every stage draws its own name"
        );
    }

    /// `write_staged` never reuses or overwrites an object: a name that
    /// exists is a collision, and the object keeps its bytes.
    #[cfg(not(madsim))]
    #[tokio::test]
    async fn s3_write_staged_refuses_a_name_that_exists() {
        let store = S3SnapshotStore::memory_for_tests("snapshots").unwrap();
        let staged = store.stage(owner(1, 7), 1, body(b"first")).await.unwrap();
        let location = staged.location();
        let SnapshotLocation::S3 { key, .. } = &location else {
            panic!("expected an S3 location");
        };
        store
            .write_raw_for_tests(key, b"squatter".to_vec())
            .await
            .unwrap();
        let refused = store.write_staged(staged).await.unwrap_err();
        assert!(
            matches!(&refused, SnapshotStoreError::NameCollision { key: collided } if collided == key),
            "{refused:?}"
        );
        assert!(matches!(
            store.download(&location).await,
            Err(SnapshotStoreError::SizeMismatch { .. })
        ));
    }

    /// Outside the test store, a backend that cannot create an object only
    /// if it is absent is refused rather than overwritten blindly.
    #[cfg(not(madsim))]
    #[tokio::test]
    async fn s3_write_staged_refuses_a_backend_without_conditional_create() {
        let operator = opendal::Operator::via_iter(opendal::Scheme::Memory, []).unwrap();
        let store = S3SnapshotStore::new(operator, "snapshots");
        let staged = store.stage(owner(1, 7), 1, body(b"body")).await.unwrap();
        let location = staged.location();
        let refused = store.write_staged(staged).await.unwrap_err();
        assert!(
            matches!(
                refused,
                SnapshotStoreError::ConditionalCreateUnsupported { .. }
            ),
            "{refused:?}"
        );
        assert!(matches!(
            store.download(&location).await,
            Err(SnapshotStoreError::NotFound { .. })
        ));
    }

    /// `list_own` lists only the owner's directory, never node 10's for
    /// node 1, and reports a file there that is no owned key apart.
    /// `delete_own` deletes an owned object and takes a missing one.
    #[cfg(not(madsim))]
    #[tokio::test]
    async fn s3_list_own_and_delete_own_stay_in_the_owners_directory() {
        let store = S3SnapshotStore::memory_for_tests("snapshots").unwrap();
        let mut written = Vec::new();
        for (node_id, index) in [(1, 5), (1, 3), (10, 4)] {
            let staged = store
                .stage(owner(node_id, 7), index, body(b"body"))
                .await
                .unwrap();
            written.push(*staged.key().unwrap());
            store.write_staged(staged).await.unwrap();
        }
        let other_group = store.stage(owner(1, 8), 9, body(b"body")).await.unwrap();
        store.write_staged(other_group).await.unwrap();
        store
            .write_raw_for_tests("snapshots/node-1/group-7/stray.json", b"{}".to_vec())
            .await
            .unwrap();

        let listing = store.list_own(owner(1, 7)).await.unwrap();
        let mut own = vec![written[1], written[0]];
        own.sort_unstable();
        assert_eq!(listing.owned, own);
        assert_eq!(listing.foreign, vec![
            "snapshots/node-1/group-7/stray.json".to_owned()
        ]);
        assert_eq!(store.list_own(owner(10, 7)).await.unwrap().owned, vec![
            written[2]
        ]);

        store.delete_own(written[0]).await.unwrap();
        store.delete_own(written[0]).await.unwrap();
        assert_eq!(store.list_own(owner(1, 7)).await.unwrap().owned, vec![
            written[1]
        ]);
        assert_eq!(
            store.list_own(owner(10, 7)).await.unwrap().owned,
            vec![written[2]],
            "node 1's delete leaves node 10's object"
        );
    }
}

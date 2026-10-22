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
//! [`SnapshotStore::delete_own`] serve the owner's GC.
//! [`SnapshotStore::list_node_groups`] names the groups a node's directory
//! holds objects of, for the objects of a replica the node tore down or
//! never ran (design 7.3). [`OwnedPointer`] is a
//! pointer this node may commit. Nothing else lives in the store: no
//! reference or pin records, and no object any node shares. A node that
//! installs a peer's snapshot writes its own copy before it commits it, and
//! what a node may still use is held in its memory, never in the store.

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
use ursula_shard::RaftGroupId;

use crate::SnapshotStoreOperation;

#[cfg(any(madsim, test, feature = "test-support"))]
pub mod harness;

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

/// Encodes `value` as the F12a binary snapshot envelope (compact
/// MessagePack, [`ursula_stream::msgpack`]), the only envelope Ursula writes.
pub fn encode_binary_envelope<T: Serialize>(value: &T) -> Result<Vec<u8>, SnapshotStoreError> {
    ursula_stream::msgpack::encode(value)
        .map_err(|err| SnapshotStoreError::Serialize(err.to_string()))
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
    ursula_stream::msgpack::decode(bytes)
        .map_err(|err| SnapshotStoreError::Deserialize(err.to_string()))
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

pub use ursula_config::SnapshotPrefix;

/// The node that owns a group's snapshot objects: the only node that writes
/// or deletes them. A store knows its own node, so only it names an owner
/// it acts for. A parsed key names the owner it was written by.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SnapshotOwner {
    node_id: u64,
    raft_group_id: RaftGroupId,
}

impl SnapshotOwner {
    pub fn node_id(&self) -> u64 {
        self.node_id
    }

    pub fn raft_group_id(&self) -> RaftGroupId {
        self.raft_group_id
    }

    /// The directory of the owner's objects under `prefix`:
    /// `{prefix}/node-{node}/group-{gid}/`.
    pub fn directory(&self, prefix: &SnapshotPrefix) -> String {
        format!(
            "{prefix}/node-{}/group-{}/",
            self.node_id, self.raft_group_id.0
        )
    }
}

/// The key of an owned snapshot object,
/// `{prefix}/node-{node}/group-{gid}/{index:020}-{nonce:016x}.snap`. `index`
/// is the snapshot's last log index, so a group's objects list in log
/// order, and `nonce` is drawn for every write, so no two writes share a
/// name. Parsing a key names whoever wrote it. Only the store that owns it
/// hands it out as an [`OwnedObject`], the one form a delete takes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct OwnedKey {
    owner: SnapshotOwner,
    index: u64,
    nonce: u64,
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
    pub prefix: SnapshotPrefix,
    pub problem: OwnedKeyProblem,
}

impl OwnedKey {
    pub fn owner(&self) -> SnapshotOwner {
        self.owner
    }

    pub fn index(&self) -> u64 {
        self.index
    }

    pub fn nonce(&self) -> u64 {
        self.nonce
    }

    /// The full object key under `prefix`.
    pub fn render(&self, prefix: &SnapshotPrefix) -> String {
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
    pub fn parse(prefix: &SnapshotPrefix, key: &str) -> Result<Self, OwnedKeyError> {
        let refuse = |problem| OwnedKeyError {
            key: key.to_owned(),
            prefix: prefix.clone(),
            problem,
        };
        let rest = key
            .strip_prefix(prefix.as_str())
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
                raft_group_id: RaftGroupId(raft_group_id),
            },
            index,
            nonce,
        })
    }
}

/// The groups of which `paths` hold one of `node`'s keys under `prefix`.
pub(crate) fn node_groups<'a>(
    prefix: &SnapshotPrefix,
    node: u64,
    paths: impl Iterator<Item = &'a str>,
) -> BTreeSet<RaftGroupId> {
    paths
        .filter_map(|path| OwnedKey::parse(prefix, path).ok())
        .filter(|key| key.owner.node_id == node)
        .map(|key| key.owner.raft_group_id)
        .collect()
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

/// An object of this node, as its store listed it ([`SnapshotStore::list_own`]):
/// the only form [`SnapshotStore::delete_own`] takes, so a delete of another
/// node's object cannot be written.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct OwnedObject {
    key: OwnedKey,
    path: String,
}

impl OwnedObject {
    pub fn key(&self) -> &OwnedKey {
        &self.key
    }

    /// The object's full key in the store.
    pub fn path(&self) -> &str {
        &self.path
    }
}

/// The keys a node may commit for one group, as its store names them
/// ([`SnapshotStore::namespace`]): inline pointers, and, in a store with
/// external objects, the S3 keys under the node's own directory.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OwnedNamespace {
    directory: Option<OwnedDirectory>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct OwnedDirectory {
    prefix: SnapshotPrefix,
    owner: SnapshotOwner,
}

impl OwnedNamespace {
    /// The namespace of a store without external objects: inline pointers
    /// only.
    fn inline_only() -> Self {
        Self { directory: None }
    }

    /// The directory of the node's objects, if its store holds any.
    pub fn directory(&self) -> Option<String> {
        self.directory
            .as_ref()
            .map(|directory| directory.owner.directory(&directory.prefix))
    }
}

/// Why a pointer is not an [`OwnedPointer`].
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum NotOwned {
    /// The pointer names an S3 key outside the node's own directory.
    #[error("snapshot key {key} lies outside the owned directory {directory}")]
    ForeignKey { key: String, directory: String },
    /// The pointer names an S3 key, and the node's store holds no external
    /// objects.
    #[error("snapshot key {key} names an object, and this node's store holds none")]
    NoObjectStore { key: String },
    /// The pointer names a local file, which no node owns in a store.
    #[error("snapshot pointer names the local file {path:?}")]
    LocalPath { path: PathBuf },
}

/// A location this node may commit, as a write made it
/// ([`SnapshotStore::write_staged`]): inline, or the owned key its stage
/// drew. Only a store's write builds one, from the staged snapshot itself,
/// so it never names an object the write did not create.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OwnedLocation(SnapshotLocation);

impl OwnedLocation {
    pub fn location(&self) -> &SnapshotLocation {
        &self.0
    }

    pub fn into_location(self) -> SnapshotLocation {
        self.0
    }
}

/// A snapshot pointer this node may commit: inline, or an S3 key that
/// parses as an [`OwnedKey`] of this node and group. A build or an install
/// copy makes one from the [`OwnedLocation`] its write returned, and a
/// restore parses one with [`TryFrom`], which refuses every other pointer.
#[derive(Debug, Clone)]
pub struct OwnedPointer(SnapshotPointer);

impl OwnedPointer {
    /// The pointer named `snapshot_id` to what a write created.
    pub fn new(snapshot_id: String, location: OwnedLocation) -> Self {
        Self(SnapshotPointer {
            snapshot_id,
            location: location.0,
        })
    }

    /// An inline pointer, which every node may commit.
    pub fn inline(snapshot_id: String, bytes: Vec<u8>) -> Self {
        Self(SnapshotPointer {
            snapshot_id,
            location: SnapshotLocation::Inline { bytes },
        })
    }

    /// `pointer`, whoever owns what it names, for a simulation's negative
    /// control of the install copy. No other build can name a foreign
    /// object as this node's own.
    #[cfg(madsim)]
    pub fn unowned_for_simulation(pointer: SnapshotPointer) -> Self {
        Self(pointer)
    }

    pub fn pointer(&self) -> &SnapshotPointer {
        &self.0
    }

    pub fn location(&self) -> &SnapshotLocation {
        &self.0.location
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
        let key = match &pointer.location {
            SnapshotLocation::Inline { .. } => return Ok(Self(pointer)),
            SnapshotLocation::Local { path, .. } => {
                return Err(NotOwned::LocalPath { path: path.clone() });
            }
            SnapshotLocation::S3 { key, .. } => key,
        };
        let Some(directory) = &namespace.directory else {
            return Err(NotOwned::NoObjectStore { key: key.clone() });
        };
        match OwnedKey::parse(&directory.prefix, key) {
            Ok(owned) if owned.owner == directory.owner => Ok(Self(pointer)),
            Ok(_) | Err(_) => Err(NotOwned::ForeignKey {
                key: key.clone(),
                directory: directory.owner.directory(&directory.prefix),
            }),
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

/// The object of a [`StagedSnapshot`]. Its one location is rendered from
/// its key, so what the write creates and what it returns cannot differ.
pub struct StagedObject {
    key: OwnedKey,
    prefix: SnapshotPrefix,
    size_bytes: u64,
    stored: Bytes,
}

impl StagedObject {
    pub fn key(&self) -> &OwnedKey {
        &self.key
    }

    /// The object's full key in the store.
    pub fn path(&self) -> String {
        self.key.render(&self.prefix)
    }

    /// The location the write creates: an owned key of this node.
    pub fn location(&self) -> SnapshotLocation {
        SnapshotLocation::S3 {
            key: self.path(),
            size_bytes: self.size_bytes,
            stored_size_bytes: self.stored.len() as u64,
            compression: SnapshotCompression::Zstd,
        }
    }
}

impl StagedSnapshot {
    /// Where the snapshot lives once written.
    pub fn location(&self) -> SnapshotLocation {
        match self {
            Self::Inline { bytes } => SnapshotLocation::Inline {
                bytes: bytes.clone(),
            },
            Self::Object(object) => object.location(),
        }
    }

    /// The owned key of a staged object.
    pub fn key(&self) -> Option<&OwnedKey> {
        match self {
            Self::Inline { .. } => None,
            Self::Object(object) => Some(object.key()),
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
                .field("location", &object.location())
                .finish(),
        }
    }
}

/// A snapshot's body as its store holds it: the stored bytes, compressed or
/// not, with the logical size and compression its pointer names
/// ([`SnapshotStore::read_stored`]). An install copy writes these bytes as
/// they are ([`SnapshotStore::stage_stored`]), and [`StoredSnapshot::body`]
/// decompresses them.
#[derive(Debug, Clone)]
pub struct StoredSnapshot {
    /// The object's key, for errors; `None` for an inline body.
    key: Option<String>,
    stored: Bytes,
    size_bytes: u64,
    compression: SnapshotCompression,
}

impl StoredSnapshot {
    /// An inline body, stored uncompressed.
    pub fn inline(bytes: Vec<u8>) -> Self {
        let size_bytes = bytes.len() as u64;
        Self {
            key: None,
            stored: Bytes::from(bytes),
            size_bytes,
            compression: SnapshotCompression::None,
        }
    }

    /// The object at `location`, whose stored bytes a GET answered,
    /// checked against the stored size the location names.
    pub fn of_object(
        location: &SnapshotLocation,
        stored: Bytes,
    ) -> Result<Self, SnapshotStoreError> {
        let key = match location {
            SnapshotLocation::S3 { key, .. } => key.clone(),
            SnapshotLocation::Local { path, .. } => path.display().to_string(),
            SnapshotLocation::Inline { bytes } => return Ok(Self::inline(bytes.clone())),
        };
        let expected = location.stored_size_hint();
        if stored.len() as u64 != expected {
            return Err(SnapshotStoreError::SizeMismatch {
                key,
                expected,
                actual: stored.len() as u64,
            });
        }
        Ok(Self {
            key: Some(key),
            stored,
            size_bytes: location.size_hint(),
            compression: location.compression(),
        })
    }

    /// The decompressed body, checked against the logical size.
    pub fn body(&self) -> Result<Vec<u8>, SnapshotStoreError> {
        let key = || self.key.clone().unwrap_or_default();
        let bytes = match self.compression {
            SnapshotCompression::None => self.stored.to_vec(),
            SnapshotCompression::Zstd => zstd::bulk::decompress(
                &self.stored,
                usize::try_from(self.size_bytes).map_err(|_overflow| {
                    SnapshotStoreError::LogicalSizeUnaddressable {
                        key: key(),
                        size_bytes: self.size_bytes,
                    }
                })?,
            )
            .map_err(|source| SnapshotStoreError::Decompress { key: key(), source })?,
        };
        if bytes.len() as u64 != self.size_bytes {
            return Err(SnapshotStoreError::LogicalSizeMismatch {
                key: key(),
                expected: self.size_bytes,
                actual: bytes.len() as u64,
            });
        }
        Ok(bytes)
    }

    /// The stored bytes and logical size of a body stored zstd-compressed,
    /// which an owned object stores as it is.
    fn into_compressed(self) -> Result<(Bytes, u64), Self> {
        match self.compression {
            SnapshotCompression::Zstd => Ok((self.stored, self.size_bytes)),
            SnapshotCompression::None => Err(self),
        }
    }
}

/// What a listing of a node's directory for a group holds
/// ([`SnapshotStore::list_own`]).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct OwnListing {
    /// The objects that parse as the node's keys, in key order.
    pub owned: Vec<OwnedObject>,
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
#[derive(Debug, Clone, PartialEq, Eq)]
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
        match self {
            Self::NoS3Config => f.write_str("S3 config is required for the snapshot S3 backend"),
            Self::NoBucket => f.write_str("an S3 bucket is required for the snapshot S3 backend"),
            Self::EmptyBucket => f.write_str("the snapshot S3 bucket must not be empty"),
            Self::NoS3UnderSimulation => {
                f.write_str("the snapshot S3 backend has no I/O under madsim, use 'inline'")
            }
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum SnapshotStoreError {
    /// The object is not in the store.
    #[error("snapshot object {key} not found")]
    NotFound { key: String },
    /// A write that creates an object only if none has its name found one.
    /// Behind the store's retries, a create whose answer was lost and that
    /// is sent again finds its own object, so it is answered this way too.
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
    /// The stored object does not decompress.
    #[error("snapshot object {key} does not decompress")]
    Decompress {
        key: String,
        #[source]
        source: io::Error,
    },
    /// The pointer's logical size does not fit in this node's memory.
    #[error("snapshot object {key} is {size_bytes} bytes, more than this node addresses")]
    LogicalSizeUnaddressable { key: String, size_bytes: u64 },

    /// An object was handed to a store that does not own it.
    #[error("snapshot object {path} is not this node's")]
    NotOwnedHere { path: String },
    /// The task that collects or compresses a snapshot body ended without
    /// an answer.
    #[error("the snapshot body's encoder task ended")]
    Encoder(#[source] tokio::task::JoinError),
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
    /// A snapshot prefix is refused.
    #[error(transparent)]
    InvalidPrefix(#[from] ursula_config::SnapshotPrefixError),
    /// The S3 snapshot store's config is incomplete.
    #[error("snapshot store config: {problem}")]
    Config { problem: SnapshotStoreConfigProblem },
    /// The S3 operator did not build.
    #[error("build the S3 snapshot store")]
    Open(#[source] Box<opendal::Error>),
    /// The cold tier's S3 encryption did not apply to the snapshot store.
    #[error("apply the cold tier's S3 encryption to the snapshot store")]
    Encryption(#[source] io::Error),
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
    /// The node fence refused the PUT or DELETE before it was sent
    /// ([`FencedSnapshotStore`]).
    #[error(transparent)]
    Fenced(#[from] crate::node_fence::FencedWrite),
    #[error("snapshot io")]
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

pub type SnapshotStoreFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, SnapshotStoreError>> + Send + 'a>>;
pub type SnapshotBytesIterator = Box<dyn Iterator<Item = Result<Bytes, SnapshotStoreError>> + Send>;

pub trait SnapshotStore: Send + Sync + Debug {
    /// The keys this store's node may commit for `raft_group_id`.
    fn namespace(&self, raft_group_id: RaftGroupId) -> OwnedNamespace;

    /// Makes the snapshot of `raft_group_id` at log index `index` ready to
    /// write, with no I/O: an external store compresses it and draws a fresh
    /// [`OwnedKey`] of its node for it, so the caller can hold the key before
    /// the write.
    fn stage<'a>(
        &'a self,
        raft_group_id: RaftGroupId,
        index: u64,
        chunks: SnapshotBytesIterator,
    ) -> SnapshotStoreFuture<'a, StagedSnapshot>;

    /// Makes a body as another store holds it ready to write under a fresh
    /// owned key, as [`Self::stage`] does: an install copy writes the stored
    /// bytes as they are, without decompressing and compressing them again.
    /// The default decompresses the body and stages it.
    fn stage_stored<'a>(
        &'a self,
        raft_group_id: RaftGroupId,
        index: u64,
        stored: StoredSnapshot,
    ) -> SnapshotStoreFuture<'a, StagedSnapshot> {
        Box::pin(async move {
            let body = Bytes::from(stored.body()?);
            self.stage(raft_group_id, index, Box::new(std::iter::once(Ok(body))))
                .await
        })
    }

    /// Writes a staged snapshot and returns the location it created, which
    /// is the staged one. An object is created only if no object has its
    /// name ([`SnapshotStoreError::NameCollision`] otherwise), and never
    /// overwrites one.
    fn write_staged<'a>(&'a self, staged: StagedSnapshot)
    -> SnapshotStoreFuture<'a, OwnedLocation>;

    /// Lists this store's node's directory for `raft_group_id`.
    fn list_own<'a>(&'a self, raft_group_id: RaftGroupId) -> SnapshotStoreFuture<'a, OwnListing>;

    /// Deletes an object this store listed as its own. A missing one is not
    /// an error.
    fn delete_own<'a>(&'a self, object: OwnedObject) -> SnapshotStoreFuture<'a, ()>;

    /// The groups whose directory under `node`'s holds an object that
    /// parses as one of `node`'s keys ([`OwnedKey`]), so a pass over each
    /// with [`Self::list_own`] finds every object `node` wrote. The default
    /// is a store that holds no object: it names no group.
    fn list_node_groups<'a>(
        &'a self,
        _node: u64,
    ) -> SnapshotStoreFuture<'a, BTreeSet<RaftGroupId>> {
        Box::pin(async move { Ok(BTreeSet::new()) })
    }

    /// Reads the stored body at `location`, checked against its stored
    /// size.
    fn read_stored<'a>(
        &'a self,
        location: &'a SnapshotLocation,
    ) -> SnapshotStoreFuture<'a, StoredSnapshot>;

    /// The decompressed body at `location`.
    fn download<'a>(&'a self, location: &'a SnapshotLocation) -> SnapshotStoreFuture<'a, Vec<u8>> {
        Box::pin(async move { self.read_stored(location).await?.body() })
    }

    /// Lightweight liveness probe for the backend, used by the snapshot driver
    /// to detect local S3 loss WITHOUT triggering a `build_snapshot` (whose
    /// failure openraft treats as fatal). The default is "always healthy":
    /// in-memory and local-filesystem backends cannot be remotely unavailable.
    /// The S3 backend overrides this with a cheap `stat`.
    fn health_check(&self) -> SnapshotStoreFuture<'_, ()> {
        Box::pin(async move { Ok(()) })
    }

    /// Verify that a freshly-written snapshot is actually retrievable from
    /// the backend. Called immediately after `write_staged` returns Ok, before
    /// the new pointer is published. Catches silent partial-success modes
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

/// A snapshot store whose object PUTs and DELETEs are sent only while the
/// node fence admits them and no predecessor wait is owed. The check runs
/// before each store call, after the LIST or state read it rests on
/// returned (the store's own retries of one call are not checked again), so a garbage pass that began inside the lease deletes nothing
/// once it ended.
#[derive(Debug)]
pub struct FencedSnapshotStore {
    inner: SharedSnapshotStore,
    fence: crate::NodeFence,
    /// Whether the fence admitted a write of each group when its last LIST
    /// began, noted only while a negative control turns off
    /// `Defence::ObjectWriteCheck`, whose DELETEs read it instead of the
    /// fence.
    listed: std::sync::Mutex<std::collections::BTreeMap<RaftGroupId, bool>>,
}

impl FencedSnapshotStore {
    pub fn wrap(inner: SharedSnapshotStore, fence: crate::NodeFence) -> SharedSnapshotStore {
        Arc::new(Self {
            inner,
            fence,
            listed: std::sync::Mutex::new(std::collections::BTreeMap::new()),
        })
    }

    fn check(&self, operation: &'static str, path: &str) -> Result<(), SnapshotStoreError> {
        Ok(self
            .fence
            .check_write(crate::node_fence::FencedObject::Snapshot, operation, path)?)
    }

    fn listed(&self) -> std::sync::MutexGuard<'_, std::collections::BTreeMap<RaftGroupId, bool>> {
        self.listed
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// The check before a DELETE of `object`. A negative control turns it
    /// off (`Defence::ObjectWriteCheck`), and the DELETE then follows what
    /// the fence said when its pass's LIST began.
    fn check_delete(&self, object: &OwnedObject) -> Result<(), SnapshotStoreError> {
        use ursula_stream::sim_controls::Defence;
        let checked = self.check("DELETE", object.path());
        if checked.is_err() && ursula_stream::sim_controls::off(Defence::ObjectWriteCheck) {
            let group = object.key().owner().raft_group_id();
            if self.listed().get(&group).copied().unwrap_or(false) {
                ursula_stream::sim_controls::note_fired(Defence::ObjectWriteCheck);
                return Ok(());
            }
        }
        checked
    }
}

impl SnapshotStore for FencedSnapshotStore {
    fn namespace(&self, raft_group_id: RaftGroupId) -> OwnedNamespace {
        self.inner.namespace(raft_group_id)
    }

    fn stage<'a>(
        &'a self,
        raft_group_id: RaftGroupId,
        index: u64,
        chunks: SnapshotBytesIterator,
    ) -> SnapshotStoreFuture<'a, StagedSnapshot> {
        self.inner.stage(raft_group_id, index, chunks)
    }

    fn stage_stored<'a>(
        &'a self,
        raft_group_id: RaftGroupId,
        index: u64,
        stored: StoredSnapshot,
    ) -> SnapshotStoreFuture<'a, StagedSnapshot> {
        self.inner.stage_stored(raft_group_id, index, stored)
    }

    fn write_staged<'a>(
        &'a self,
        staged: StagedSnapshot,
    ) -> SnapshotStoreFuture<'a, OwnedLocation> {
        Box::pin(async move {
            if let StagedSnapshot::Object(object) = &staged {
                self.check("PUT", &object.path())?;
            }
            self.inner.write_staged(staged).await
        })
    }

    fn list_own<'a>(&'a self, raft_group_id: RaftGroupId) -> SnapshotStoreFuture<'a, OwnListing> {
        if ursula_stream::sim_controls::off(ursula_stream::sim_controls::Defence::ObjectWriteCheck)
        {
            let admitted = self.check("LIST", "").is_ok();
            self.listed().insert(raft_group_id, admitted);
        }
        self.inner.list_own(raft_group_id)
    }

    fn delete_own<'a>(&'a self, object: OwnedObject) -> SnapshotStoreFuture<'a, ()> {
        Box::pin(async move {
            self.check_delete(&object)?;
            self.inner.delete_own(object).await
        })
    }

    fn list_node_groups<'a>(&'a self, node: u64) -> SnapshotStoreFuture<'a, BTreeSet<RaftGroupId>> {
        self.inner.list_node_groups(node)
    }

    fn read_stored<'a>(
        &'a self,
        location: &'a SnapshotLocation,
    ) -> SnapshotStoreFuture<'a, StoredSnapshot> {
        self.inner.read_stored(location)
    }

    fn download<'a>(&'a self, location: &'a SnapshotLocation) -> SnapshotStoreFuture<'a, Vec<u8>> {
        self.inner.download(location)
    }

    fn health_check(&self) -> SnapshotStoreFuture<'_, ()> {
        self.inner.health_check()
    }

    fn verify_uploaded<'a>(
        &'a self,
        location: &'a SnapshotLocation,
    ) -> SnapshotStoreFuture<'a, ()> {
        self.inner.verify_uploaded(location)
    }
}

/// Default backend used when none is wired: bytes ride inline in the pointer.
pub fn default_snapshot_store() -> SharedSnapshotStore {
    Arc::new(InlineSnapshotStore)
}

/// Bytes live inside the pointer. Equivalent to today's in-memory snapshot.
#[derive(Debug, Default, Clone, Copy)]
pub struct InlineSnapshotStore;

impl SnapshotStore for InlineSnapshotStore {
    fn read_stored<'a>(
        &'a self,
        location: &'a SnapshotLocation,
    ) -> SnapshotStoreFuture<'a, StoredSnapshot> {
        Box::pin(async move {
            match location {
                SnapshotLocation::Inline { bytes } => Ok(StoredSnapshot::inline(bytes.clone())),
                other => Err(SnapshotStoreError::UnsupportedLocation {
                    store: SnapshotBackend::Inline,
                    location: other.kind(),
                }),
            }
        })
    }

    /// Inline pointers only.
    fn namespace(&self, _raft_group_id: RaftGroupId) -> OwnedNamespace {
        OwnedNamespace::inline_only()
    }

    /// Keeps the bytes inline.
    fn stage<'a>(
        &'a self,
        _raft_group_id: RaftGroupId,
        _index: u64,
        chunks: SnapshotBytesIterator,
    ) -> SnapshotStoreFuture<'a, StagedSnapshot> {
        Box::pin(async move {
            Ok(StagedSnapshot::Inline {
                bytes: collect_inline_snapshot(chunks).await?,
            })
        })
    }

    /// Holds inline snapshots only.
    fn write_staged<'a>(
        &'a self,
        staged: StagedSnapshot,
    ) -> SnapshotStoreFuture<'a, OwnedLocation> {
        Box::pin(async move {
            match staged {
                StagedSnapshot::Inline { bytes } => {
                    Ok(OwnedLocation(SnapshotLocation::Inline { bytes }))
                }
                StagedSnapshot::Object(object) => Err(SnapshotStoreError::UnsupportedLocation {
                    store: SnapshotBackend::Inline,
                    location: object.location().kind(),
                }),
            }
        })
    }

    /// Holds no object.
    fn list_own<'a>(&'a self, _raft_group_id: RaftGroupId) -> SnapshotStoreFuture<'a, OwnListing> {
        Box::pin(async move { Ok(OwnListing::default()) })
    }

    /// Owns no object, so every object is another store's.
    fn delete_own<'a>(&'a self, object: OwnedObject) -> SnapshotStoreFuture<'a, ()> {
        Box::pin(async move { Err(SnapshotStoreError::NotOwnedHere { path: object.path }) })
    }
}

/// The owned-object methods of [`SnapshotStore`] for a test store that
/// holds no object of its own: each answers as [`InlineSnapshotStore`]'s
/// does. Expand it inside the store's `impl SnapshotStore`.
#[cfg(any(test, feature = "test-support"))]
#[macro_export]
macro_rules! inline_owned_snapshot_methods {
    () => {
        fn namespace(&self, raft_group_id: ::ursula_shard::RaftGroupId) -> $crate::OwnedNamespace {
            $crate::SnapshotStore::namespace(&$crate::InlineSnapshotStore, raft_group_id)
        }

        fn stage<'a>(
            &'a self,
            raft_group_id: ::ursula_shard::RaftGroupId,
            index: u64,
            chunks: $crate::SnapshotBytesIterator,
        ) -> $crate::SnapshotStoreFuture<'a, $crate::StagedSnapshot> {
            static INLINE: $crate::InlineSnapshotStore = $crate::InlineSnapshotStore;
            $crate::SnapshotStore::stage(&INLINE, raft_group_id, index, chunks)
        }

        fn write_staged<'a>(
            &'a self,
            staged: $crate::StagedSnapshot,
        ) -> $crate::SnapshotStoreFuture<'a, $crate::OwnedLocation> {
            static INLINE: $crate::InlineSnapshotStore = $crate::InlineSnapshotStore;
            $crate::SnapshotStore::write_staged(&INLINE, staged)
        }

        fn list_own<'a>(
            &'a self,
            raft_group_id: ::ursula_shard::RaftGroupId,
        ) -> $crate::SnapshotStoreFuture<'a, $crate::OwnListing> {
            static INLINE: $crate::InlineSnapshotStore = $crate::InlineSnapshotStore;
            $crate::SnapshotStore::list_own(&INLINE, raft_group_id)
        }

        fn delete_own<'a>(
            &'a self,
            object: $crate::OwnedObject,
        ) -> $crate::SnapshotStoreFuture<'a, ()> {
            static INLINE: $crate::InlineSnapshotStore = $crate::InlineSnapshotStore;
            $crate::SnapshotStore::delete_own(&INLINE, object)
        }
    };
}

#[cfg(not(madsim))]
async fn collect_inline_snapshot(
    chunks: SnapshotBytesIterator,
) -> Result<Vec<u8>, SnapshotStoreError> {
    tokio::task::spawn_blocking(move || collect_snapshot_chunks(chunks))
        .await
        .map_err(SnapshotStoreError::Encoder)?
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
    use std::collections::BTreeSet;
    use std::io::Write;

    use bytes::Bytes;
    use opendal::ErrorKind;
    use opendal::Operator;
    #[cfg(any(test, feature = "test-support"))]
    use opendal::Scheme;
    use ursula_shard::RaftGroupId;

    use super::OwnListing;
    use super::OwnedDirectory;
    use super::OwnedKey;
    use super::OwnedLocation;
    use super::OwnedNamespace;
    use super::OwnedObject;
    use super::SnapshotBackend;
    use super::SnapshotBytesIterator;
    use super::SnapshotLocation;
    use super::SnapshotOwner;
    use super::SnapshotPrefix;
    use super::SnapshotStore;
    use super::SnapshotStoreConfigProblem;
    use super::SnapshotStoreError;
    use super::SnapshotStoreFuture;
    use super::StagedObject;
    use super::StagedSnapshot;
    use super::StoredSnapshot;
    use super::node_groups;
    use crate::SnapshotStoreOperation;

    const S3_SNAPSHOT_ZSTD_LEVEL: i32 = 3;

    /// How a store creates an object only if no object has its name.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum ConditionalCreate {
        /// The backend must support it, or the write is refused.
        Required,
        /// A HEAD before the write stands in for it, on an in-memory
        /// operator of a test that has no concurrent writer of the name.
        #[cfg(any(test, feature = "test-support"))]
        EmulatedForTests,
    }

    /// Bytes live in an opendal-managed S3 bucket, each node's under
    /// `{prefix}/node-{node}/`.
    pub struct S3SnapshotStore {
        operator: Operator,
        prefix: SnapshotPrefix,
        /// The node this store acts for: the owner of every object it
        /// stages, lists or deletes.
        node_id: u64,
        conditional_create: ConditionalCreate,
        /// Draws the nonce of every staged object.
        nonces: fn() -> u64,
    }

    impl std::fmt::Debug for S3SnapshotStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("S3SnapshotStore")
                .field("prefix", &self.prefix)
                .field("node_id", &self.node_id)
                .field("conditional_create", &self.conditional_create)
                .finish_non_exhaustive()
        }
    }

    impl S3SnapshotStore {
        pub fn new(operator: Operator, prefix: SnapshotPrefix, node_id: u64) -> Self {
            Self {
                operator,
                prefix,
                node_id,
                conditional_create: ConditionalCreate::Required,
                nonces: rand::random,
            }
        }

        /// Draws the nonces of staged objects from `nonces` instead of at
        /// random, for a test that makes two stages draw the same name.
        #[cfg(test)]
        pub(crate) fn with_nonces(mut self, nonces: fn() -> u64) -> Self {
            self.nonces = nonces;
            self
        }

        /// This store's node as the owner of `raft_group_id`'s objects.
        fn owner(&self, raft_group_id: RaftGroupId) -> SnapshotOwner {
            SnapshotOwner {
                node_id: self.node_id,
                raft_group_id,
            }
        }

        /// `stored`, zstd bytes of `size_bytes` logical bytes, staged under
        /// a freshly drawn key of this node.
        fn staged_object(
            &self,
            raft_group_id: RaftGroupId,
            index: u64,
            stored: Bytes,
            size_bytes: u64,
        ) -> StagedSnapshot {
            StagedSnapshot::Object(StagedObject {
                key: OwnedKey {
                    owner: self.owner(raft_group_id),
                    index,
                    nonce: (self.nonces)(),
                },
                prefix: self.prefix.clone(),
                size_bytes,
                stored,
            })
        }

        /// In-memory opendal operator under `prefix`, for tests. The
        /// in-memory operator cannot create an object only if it is absent,
        /// so [`SnapshotStore::write_staged`] checks for the name first,
        /// which no concurrent writer of a test races.
        #[cfg(any(test, feature = "test-support"))]
        pub fn memory_for_tests(prefix: &str, node_id: u64) -> Result<Self, SnapshotStoreError> {
            let operator = Operator::via_iter(Scheme::Memory, [])
                .map_err(|err| SnapshotStoreError::Open(Box::new(err)))?;
            let mut store = Self::new(operator, SnapshotPrefix::try_from(prefix)?, node_id);
            store.conditional_create = ConditionalCreate::EmulatedForTests;
            Ok(store)
        }

        /// A store of node `node_id` on this store's operator and prefix, for
        /// tests of several nodes that share one bucket.
        #[cfg(any(test, feature = "test-support"))]
        pub fn sibling_for_tests(&self, node_id: u64) -> Self {
            let mut sibling = Self::new(self.operator.clone(), self.prefix.clone(), node_id);
            sibling.conditional_create = self.conditional_create;
            sibling
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

        /// Build an S3 snapshot store from a [`ColdConfig`] and the
        /// snapshot config. Snapshot blobs share the cold bucket and
        /// credentials, and use the snapshot config's prefix (`snapshots`
        /// by default) and its own request timeout and retries.
        pub fn try_new(
            config: &crate::ColdConfig,
            snapshot: &ursula_config::RaftSnapshotConfig,
            node_id: u64,
        ) -> Result<Self, SnapshotStoreError> {
            let prefix = snapshot.snapshot_prefix();
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
            // The snapshot store's own request budget, which keeps a
            // follower's install copy inside `raft.install_snapshot_timeout`
            // (`RaftSnapshotConfig::install_copy_budget`), not the cold
            // store's.
            let operator = crate::cold_store::with_s3_resilience(
                Operator::new(builder)
                    .map_err(|err| SnapshotStoreError::Open(Box::new(err)))?
                    .finish(),
                snapshot.s3_timeout.as_duration(),
                snapshot.s3_max_retries,
            );
            Ok(Self::new(operator, prefix, node_id))
        }

        /// Creates `key` only if no object has the name: a found object is
        /// a [`SnapshotStoreError::NameCollision`], never reused or
        /// overwritten. A backend that cannot create conditionally is
        /// refused, but for the emulation of a test store.
        async fn create_object(
            &self,
            key: &str,
            stored_bytes: Bytes,
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
                #[cfg(any(test, feature = "test-support"))]
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

    /// Compresses `chunks` off the async worker, for an owned object, whose
    /// name is drawn and not hashed.
    async fn compress_off_worker(
        chunks: SnapshotBytesIterator,
    ) -> Result<(Vec<u8>, u64), SnapshotStoreError> {
        tokio::task::spawn_blocking(move || compress_snapshot_chunks(chunks))
            .await
            .map_err(SnapshotStoreError::Encoder)?
    }

    impl SnapshotStore for S3SnapshotStore {
        fn namespace(&self, raft_group_id: RaftGroupId) -> OwnedNamespace {
            OwnedNamespace {
                directory: Some(OwnedDirectory {
                    prefix: self.prefix.clone(),
                    owner: self.owner(raft_group_id),
                }),
            }
        }

        fn stage<'a>(
            &'a self,
            raft_group_id: RaftGroupId,
            index: u64,
            chunks: SnapshotBytesIterator,
        ) -> SnapshotStoreFuture<'a, StagedSnapshot> {
            Box::pin(async move {
                let (stored, size_bytes) = compress_off_worker(chunks).await?;
                Ok(self.staged_object(raft_group_id, index, Bytes::from(stored), size_bytes))
            })
        }

        /// Stages zstd bytes as they are, under a fresh name. A body stored
        /// uncompressed is compressed first.
        fn stage_stored<'a>(
            &'a self,
            raft_group_id: RaftGroupId,
            index: u64,
            stored: StoredSnapshot,
        ) -> SnapshotStoreFuture<'a, StagedSnapshot> {
            Box::pin(async move {
                match stored.into_compressed() {
                    Ok((stored, size_bytes)) => {
                        Ok(self.staged_object(raft_group_id, index, stored, size_bytes))
                    }
                    Err(uncompressed) => {
                        let body = Bytes::from(uncompressed.body()?);
                        self.stage(raft_group_id, index, Box::new(std::iter::once(Ok(body))))
                            .await
                    }
                }
            })
        }

        fn write_staged<'a>(
            &'a self,
            staged: StagedSnapshot,
        ) -> SnapshotStoreFuture<'a, OwnedLocation> {
            Box::pin(async move {
                match staged {
                    StagedSnapshot::Inline { bytes } => {
                        Ok(OwnedLocation(SnapshotLocation::Inline { bytes }))
                    }
                    StagedSnapshot::Object(object) => {
                        if object.prefix != self.prefix || object.key.owner.node_id != self.node_id
                        {
                            return Err(SnapshotStoreError::NotOwnedHere {
                                path: object.path(),
                            });
                        }
                        let location = OwnedLocation(object.location());
                        let path = object.path();
                        self.create_object(&path, object.stored).await?;
                        Ok(location)
                    }
                }
            })
        }

        fn list_own<'a>(
            &'a self,
            raft_group_id: RaftGroupId,
        ) -> SnapshotStoreFuture<'a, OwnListing> {
            Box::pin(async move {
                let owner = self.owner(raft_group_id);
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
                        Ok(key) if key.owner == owner => listing.owned.push(OwnedObject {
                            key,
                            path: entry.path().to_owned(),
                        }),
                        Ok(_) | Err(_) => listing.foreign.push(entry.path().to_owned()),
                    }
                }
                listing.owned.sort_unstable();
                listing.foreign.sort_unstable();
                Ok(listing)
            })
        }

        fn delete_own<'a>(&'a self, object: OwnedObject) -> SnapshotStoreFuture<'a, ()> {
            Box::pin(async move {
                let owned = OwnedKey::parse(&self.prefix, &object.path)
                    .is_ok_and(|key| key == object.key && key.owner.node_id == self.node_id);
                if !owned {
                    return Err(SnapshotStoreError::NotOwnedHere { path: object.path });
                }
                match self.operator.delete(&object.path).await {
                    Ok(()) => Ok(()),
                    Err(err) if matches!(err.kind(), ErrorKind::NotFound) => Ok(()),
                    Err(err) => Err(SnapshotStoreError::backend(
                        SnapshotStoreOperation::Delete,
                        object.path,
                        err,
                    )),
                }
            })
        }

        fn list_node_groups<'a>(
            &'a self,
            node: u64,
        ) -> SnapshotStoreFuture<'a, BTreeSet<RaftGroupId>> {
            Box::pin(async move {
                let directory = format!("{}/node-{node}/", self.prefix);
                let entries = self
                    .operator
                    .list_with(&directory)
                    .recursive(true)
                    .await
                    .map_err(|err| {
                        SnapshotStoreError::backend(SnapshotStoreOperation::List, &directory, err)
                    })?;
                Ok(node_groups(
                    &self.prefix,
                    node,
                    entries
                        .iter()
                        .filter(|entry| entry.metadata().mode().is_file())
                        .map(|entry| entry.path()),
                ))
            })
        }

        fn read_stored<'a>(
            &'a self,
            location: &'a SnapshotLocation,
        ) -> SnapshotStoreFuture<'a, StoredSnapshot> {
            Box::pin(async move {
                let SnapshotLocation::S3 { key, .. } = location else {
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
                StoredSnapshot::of_object(location, buf.to_bytes())
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
/// [`default_snapshot_store`] without instantiating anything. `node_id` is
/// the node the store acts for, the owner of every object it writes.
pub fn snapshot_store_from_config(
    cfg: &ursula_config::RaftSnapshotConfig,
    cold_cfg: &crate::ColdConfig,
    node_id: u64,
) -> Result<Option<SharedSnapshotStore>, SnapshotStoreError> {
    match resolved_snapshot_backend(cfg.backend, cold_cfg.backend) {
        ursula_config::RaftSnapshotBackend::Auto | ursula_config::RaftSnapshotBackend::Inline => {
            Ok(None)
        }
        #[cfg(not(madsim))]
        ursula_config::RaftSnapshotBackend::S3 => {
            // `try_new` configures the OpenDAL operator with `cold_cfg.root`, so
            // this namespace must stay relative to that root.
            Ok(Some(Arc::new(S3SnapshotStore::try_new(
                cold_cfg, cfg, node_id,
            )?)))
        }
        #[cfg(madsim)]
        ursula_config::RaftSnapshotBackend::S3 => {
            // No S3 store under madsim, so no node to act for.
            let _no_store = node_id;
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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn inline_roundtrip() {
        let store = InlineSnapshotStore;
        let staged = store
            .stage(RaftGroupId(0), 100, body(b"hello world"))
            .await
            .unwrap();
        let loc = store.write_staged(staged).await.unwrap().into_location();
        assert!(matches!(loc, SnapshotLocation::Inline { .. }));
        let bytes = store.download(&loc).await.unwrap();
        assert_eq!(bytes, b"hello world");
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

        let staged = store.stage(RaftGroupId(0), 1, chunks).await.unwrap();

        assert_ne!(thread_rx.recv().unwrap(), caller);
        assert_eq!(staged.location(), SnapshotLocation::Inline {
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
                key: "snapshots/node-1/group-7/00000000000000000500-0000000000000001.snap".into(),
                size_bytes: 123,
                stored_size_bytes: 45,
                compression: SnapshotCompression::Zstd,
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
                    key: "snapshots/node-1/group-3/00000000000000000900-0000000000000abc.snap"
                        .into(),
                    size_bytes: 77,
                    stored_size_bytes: 40,
                    compression: SnapshotCompression::Zstd,
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

    /// An S3 location of format epoch 6 names its kind, key, sizes and
    /// compression, and no `shared_object` flag: no node shares an object.
    #[test]
    fn an_s3_pointer_carries_no_shared_object_field() {
        let pointer = SnapshotPointer {
            snapshot_id: "group-3-4-900".into(),
            location: SnapshotLocation::S3 {
                key: "snapshots/node-1/group-3/00000000000000000900-0000000000000abc.snap".into(),
                size_bytes: 77,
                stored_size_bytes: 40,
                compression: SnapshotCompression::Zstd,
            },
        };
        let json = serde_json::to_value(&pointer).unwrap();
        assert_eq!(json["location"].as_object().unwrap().len(), 5, "{json}");
        assert!(json["location"].get("shared_object").is_none(), "{json}");
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

    /// A staged object round-trips through the store compressed, under an
    /// owned key of the store's node.
    #[cfg(not(madsim))]
    #[tokio::test]
    async fn s3_memory_roundtrip() {
        let store = S3SnapshotStore::memory_for_tests("snapshots", 1).unwrap();
        let payload = b"raw snapshot bytes".repeat(64);
        let staged = store
            .stage(
                RaftGroupId(3),
                9876,
                Box::new(std::iter::once(Ok(Bytes::from(payload.clone())))),
            )
            .await
            .unwrap();
        let loc = store.write_staged(staged).await.unwrap().into_location();
        match &loc {
            SnapshotLocation::S3 {
                key,
                size_bytes,
                stored_size_bytes,
                compression,
            } => {
                assert!(key.starts_with("snapshots/node-1/group-3/00000000000000009876-"));
                assert_eq!(*size_bytes, payload.len() as u64);
                assert_eq!(*compression, SnapshotCompression::Zstd);
                assert!(*stored_size_bytes < *size_bytes);
            }
            other => panic!("expected S3 location, got {other:?}"),
        }
        assert_eq!(store.download(&loc).await.unwrap(), payload);
    }

    #[cfg(not(madsim))]
    #[tokio::test]
    async fn s3_stage_compresses_off_the_async_worker() {
        let store = S3SnapshotStore::memory_for_tests("snapshots", 1).unwrap();
        let caller = std::thread::current().id();
        let (thread_tx, thread_rx) = std::sync::mpsc::channel();
        let chunks = Box::new(std::iter::once_with(move || {
            thread_tx.send(std::thread::current().id()).unwrap();
            Ok(Bytes::from(vec![b'x'; 64 * 1024]))
        }));

        let staged = store.stage(RaftGroupId(9), 1, chunks).await.unwrap();

        assert_ne!(thread_rx.recv().unwrap(), caller);
        let location = store.write_staged(staged).await.unwrap().into_location();
        assert_eq!(location.compression(), SnapshotCompression::Zstd);
        assert!(location.stored_size_hint() < location.size_hint());
        assert_eq!(store.download(&location).await.unwrap(), vec![
            b'x';
            64 * 1024
        ]);
    }

    #[cfg(not(madsim))]
    #[tokio::test]
    async fn s3_verify_uploaded_catches_missing_object() {
        let store = S3SnapshotStore::memory_for_tests("snapshots", 1).unwrap();
        let staged = store
            .stage(RaftGroupId(2), 7, body(b"payload"))
            .await
            .unwrap();
        let loc = store.write_staged(staged).await.unwrap().into_location();
        // Round-trip after a real write: must succeed.
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

    fn owner(node_id: u64, raft_group_id: u32) -> SnapshotOwner {
        SnapshotOwner {
            node_id,
            raft_group_id: RaftGroupId(raft_group_id),
        }
    }

    fn prefix(text: &str) -> SnapshotPrefix {
        SnapshotPrefix::try_from(text).unwrap()
    }

    fn body(bytes: &'static [u8]) -> SnapshotBytesIterator {
        Box::new(std::iter::once(Ok(Bytes::from_static(bytes))))
    }

    /// A prefix is trimmed of its `/`s, so no owned key renders with a
    /// leading or doubled `/` (the config refuses what cannot be trimmed).
    #[test]
    fn an_owned_key_renders_under_a_trimmed_prefix() {
        assert_eq!(prefix("snapshots").as_str(), "snapshots");
        assert_eq!(prefix("/snapshots/").as_str(), "snapshots");
        assert_eq!(prefix("a/b/").as_str(), "a/b");
        let key = OwnedKey {
            owner: owner(1, 7),
            index: 1,
            nonce: 2,
        };
        let trailing = prefix("snapshots/");
        assert_eq!(
            key.render(&trailing),
            "snapshots/node-1/group-7/00000000000000000001-0000000000000002.snap"
        );
        assert_eq!(OwnedKey::parse(&trailing, &key.render(&trailing)), Ok(key));
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
        let snapshots = prefix("snapshots");
        let rendered = key.render(&snapshots);
        assert_eq!(
            rendered,
            "snapshots/node-1/group-7/00000000000000000042-00000000000000ab.snap"
        );
        assert_eq!(OwnedKey::parse(&snapshots, &rendered), Ok(key));
        let largest = OwnedKey {
            owner: owner(u64::MAX, u32::MAX),
            index: u64::MAX,
            nonce: u64::MAX,
        };
        assert_eq!(
            OwnedKey::parse(&prefix("p"), &largest.render(&prefix("p"))),
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
            |under: &str, key: &str| OwnedKey::parse(&prefix(under), key).expect_err(key).problem;
        let name = "00000000000000000042-00000000000000ab.snap";
        let node_ten = OwnedKey::parse(
            &prefix("snapshots"),
            &format!("snapshots/node-10/group-7/{name}"),
        )
        .expect("node 10's key parses");
        assert_eq!(node_ten.owner(), owner(10, 7));
        assert_ne!(node_ten.owner(), owner(1, 7));
        for (under, key, expected) in [
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
                format!("/node-1/group-7/{name}"),
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
            assert_eq!(problem(under, &key), expected, "{key}");
        }
    }

    #[cfg(not(madsim))]
    fn node_store(node_id: u64) -> S3SnapshotStore {
        S3SnapshotStore::memory_for_tests("snapshots", node_id).unwrap()
    }

    /// A store owns inline pointers and the keys of its own node and group,
    /// and refuses every other pointer. A store without external objects
    /// owns inline pointers only.
    #[cfg(not(madsim))]
    #[test]
    fn an_owned_pointer_is_inline_or_under_its_own_directory() {
        let namespace = node_store(1).namespace(RaftGroupId(7));
        assert_eq!(
            namespace.directory().as_deref(),
            Some("snapshots/node-1/group-7/")
        );
        let s3 = |key: String| SnapshotPointer {
            snapshot_id: "s".to_owned(),
            location: SnapshotLocation::S3 {
                key,
                size_bytes: 1,
                stored_size_bytes: 1,
                compression: SnapshotCompression::Zstd,
            },
        };
        let inline = SnapshotPointer {
            snapshot_id: "s".to_owned(),
            location: SnapshotLocation::Inline { bytes: vec![1] },
        };
        let key_of = |node_id, group| {
            OwnedKey {
                owner: owner(node_id, group),
                index: 3,
                nonce: 4,
            }
            .render(&prefix("snapshots"))
        };
        let own = key_of(1, 7);
        let owned = OwnedPointer::try_from((s3(own.clone()), &namespace)).expect("own key");
        assert_eq!(owned.pointer().location, s3(own.clone()).location);
        OwnedPointer::try_from((inline.clone(), &namespace)).expect("an inline pointer is owned");
        for foreign in [
            key_of(10, 7),
            key_of(1, 8),
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

        let inline_only = InlineSnapshotStore.namespace(RaftGroupId(7));
        assert_eq!(inline_only.directory(), None);
        OwnedPointer::try_from((inline, &inline_only)).expect("an inline pointer is owned");
        assert!(matches!(
            OwnedPointer::try_from((s3(own), &inline_only)),
            Err(NotOwned::NoObjectStore { .. })
        ));
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

    /// The inline store stages and writes inline, and lists nothing.
    #[tokio::test]
    async fn the_inline_store_stages_inline() {
        let store = InlineSnapshotStore;
        let staged = store
            .stage(RaftGroupId(7), 3, body(b"inline"))
            .await
            .unwrap();
        assert!(staged.key().is_none());
        assert_eq!(
            store.write_staged(staged).await.unwrap().into_location(),
            SnapshotLocation::Inline {
                bytes: b"inline".to_vec()
            }
        );
        assert_eq!(
            store.list_own(RaftGroupId(7)).await.unwrap(),
            OwnListing::default()
        );
    }

    /// `stage` draws a fresh key of the store's node and does no I/O.
    /// `write_staged` creates the object, and the location it returns is the
    /// one the stage named before the write.
    #[cfg(not(madsim))]
    #[tokio::test]
    async fn s3_stage_names_a_fresh_owned_key_and_write_staged_creates_it() {
        let store = node_store(1);
        let payload = b"owned snapshot body".repeat(64);
        let chunks: SnapshotBytesIterator =
            Box::new(std::iter::once(Ok(Bytes::from(payload.clone()))));
        let staged = store.stage(RaftGroupId(7), 42, chunks).await.unwrap();
        let key = *staged.key().expect("an S3 stage names an object");
        assert_eq!((key.owner(), key.index()), (owner(1, 7), 42));
        let location = staged.location();
        let SnapshotLocation::S3 {
            key: rendered,
            size_bytes,
            stored_size_bytes,
            compression,
        } = &location
        else {
            panic!("expected an S3 location, got {location:?}");
        };
        assert_eq!(rendered, &key.render(&prefix("snapshots")));
        assert_eq!(*size_bytes, payload.len() as u64);
        assert!(*stored_size_bytes < *size_bytes);
        assert_eq!(*compression, SnapshotCompression::Zstd);
        assert!(
            matches!(
                store.download(&location).await,
                Err(SnapshotStoreError::NotFound { .. })
            ),
            "the stage wrote nothing"
        );
        assert_eq!(
            store.write_staged(staged).await.unwrap().into_location(),
            location
        );
        assert_eq!(store.download(&location).await.unwrap(), payload);
        store.verify_uploaded(&location).await.unwrap();

        let other = store.stage(RaftGroupId(7), 42, body(b"x")).await.unwrap();
        assert_ne!(
            other.key().map(OwnedKey::nonce),
            Some(key.nonce()),
            "every stage draws its own name"
        );
    }

    /// `write_staged` never reuses or overwrites an object: a name that
    /// exists is a collision, and the object keeps its bytes.
    #[cfg(not(madsim))]
    #[tokio::test]
    async fn s3_write_staged_refuses_a_name_that_exists() {
        let store = node_store(1).with_nonces(|| 7);
        let first = store
            .stage(RaftGroupId(7), 1, body(b"first"))
            .await
            .unwrap();
        let location = store.write_staged(first).await.unwrap().into_location();
        let again = store
            .stage(RaftGroupId(7), 1, body(b"second"))
            .await
            .unwrap();
        assert_eq!(again.location().kind(), SnapshotLocationKind::S3);
        let refused = store.write_staged(again).await.unwrap_err();
        let SnapshotLocation::S3 { key, .. } = &location else {
            panic!("expected an S3 location");
        };
        assert!(
            matches!(&refused, SnapshotStoreError::NameCollision { key: collided } if collided == key),
            "{refused:?}"
        );
        assert_eq!(store.download(&location).await.unwrap(), b"first");
    }

    /// Outside the test store, a backend that cannot create an object only
    /// if it is absent is refused rather than overwritten blindly.
    #[cfg(not(madsim))]
    #[tokio::test]
    async fn s3_write_staged_refuses_a_backend_without_conditional_create() {
        let operator = opendal::Operator::via_iter(opendal::Scheme::Memory, []).unwrap();
        let store = S3SnapshotStore::new(operator, prefix("snapshots"), 1);
        let staged = store.stage(RaftGroupId(7), 1, body(b"body")).await.unwrap();
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

    /// `list_own` lists only the store's node directory, never node 10's
    /// for node 1, and reports a file there that is no owned key apart.
    /// `delete_own` deletes a listed object, takes a missing one, and
    /// refuses an object another node's store listed.
    #[cfg(not(madsim))]
    #[tokio::test]
    async fn s3_list_own_and_delete_own_stay_in_the_owners_directory() {
        let one = node_store(1);
        let ten = one.sibling_for_tests(10);
        let mut written = Vec::new();
        for (store, index) in [(&one, 5), (&one, 3), (&ten, 4)] {
            let staged = store
                .stage(RaftGroupId(7), index, body(b"body"))
                .await
                .unwrap();
            written.push(*staged.key().unwrap());
            store.write_staged(staged).await.unwrap();
        }
        let other_group = one.stage(RaftGroupId(8), 9, body(b"body")).await.unwrap();
        one.write_staged(other_group).await.unwrap();
        one.write_raw_for_tests("snapshots/node-1/group-7/stray.json", b"{}".to_vec())
            .await
            .unwrap();

        let listing = one.list_own(RaftGroupId(7)).await.unwrap();
        let keys = |listing: &OwnListing| -> Vec<OwnedKey> {
            listing.owned.iter().map(|object| *object.key()).collect()
        };
        let mut own = vec![written[1], written[0]];
        own.sort_unstable();
        assert_eq!(keys(&listing), own);
        assert_eq!(listing.foreign, vec![
            "snapshots/node-1/group-7/stray.json".to_owned()
        ]);
        let tens = ten.list_own(RaftGroupId(7)).await.unwrap();
        assert_eq!(keys(&tens), vec![written[2]]);

        assert!(
            matches!(
                one.delete_own(tens.owned[0].clone()).await,
                Err(SnapshotStoreError::NotOwnedHere { .. })
            ),
            "node 1's store refuses node 10's object"
        );
        let first = listing
            .owned
            .iter()
            .find(|object| *object.key() == written[0])
            .unwrap()
            .clone();
        one.delete_own(first.clone()).await.unwrap();
        one.delete_own(first).await.unwrap();
        assert_eq!(keys(&one.list_own(RaftGroupId(7)).await.unwrap()), vec![
            written[1]
        ]);
        assert_eq!(
            keys(&ten.list_own(RaftGroupId(7)).await.unwrap()),
            vec![written[2]],
            "node 1's deletes leave node 10's object"
        );
    }
}

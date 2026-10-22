//! Checkers of the snapshot ownership invariants (#687), for a simulation
//! to run at every step.
//!
//! Each reads a [`SnapshotStoreView`], what the store holds and the DELETEs
//! it served, and what each node names, which the caller supplies through
//! [`NodeSnapshotNames`]. The checkers assume nothing about how a node
//! decides what it names, so they hold for any protocol that keeps the
//! invariants:
//!
//! - I1 ([`check_named_keys_exist`]): every key a node's metadata names
//!   exists.
//! - I2 ([`check_no_foreign_delete`]): no node deletes outside its own
//!   directory, but the removed-node sweep, which deletes only keys of the
//!   other node it sweeps.
//! - I3 ([`check_committed_reads_found`]): no restore, no committed
//!   install and no read inside a send's RPC sees `NotFound`.
//! - I4 ([`check_superseded_without_marker`]): past the bound, no object
//!   that no node names holds the marker, orphans included.
//! - I5 ([`check_quiet_prefixes`]): once quiet, each node's directory
//!   holds only the keys its metadata names.
//!
//! A checker answers the first violation it finds, in key order.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::fmt;
use std::time::Duration;

use super::is_owned_by;
use super::node_directory;
use crate::snapshot_store::SnapshotPrefix;
use crate::snapshot_store::SnapshotStoreError;

/// What a store holds, and every DELETE it served or refused.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SnapshotStoreView {
    pub prefix: SnapshotPrefix,
    /// Every object by key.
    pub objects: BTreeMap<String, ViewObject>,
    pub deletes: Vec<StoreDelete>,
}

/// One object of a [`SnapshotStoreView`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ViewObject {
    /// The logical (decompressed) bytes.
    pub bytes: Vec<u8>,
    /// How long ago the object landed in the store.
    pub age: Duration,
}

/// A DELETE of `path` that node `node_id` sent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoreDelete {
    pub node_id: u64,
    pub path: String,
    pub kind: DeleteKind,
}

/// Which store call sent a DELETE.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeleteKind {
    /// A delete of an object the deleting node owns, or any delete outside
    /// the store's methods.
    Own,
    /// The removed-node sweep's delete of an object of node `of`
    /// (`SnapshotStore::delete_node_object`).
    Swept { of: u64 },
}

/// What one node names: the object keys its durable snapshot metadata
/// names, in every group. The caller reads them from the node's state, by
/// whatever protocol the node runs.
pub trait NodeSnapshotNames {
    fn node_id(&self) -> u64;

    /// The object keys the node's durable metadata names.
    fn named_keys(&self) -> BTreeSet<String>;
}

/// A plain [`NodeSnapshotNames`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct NodeNames {
    pub node_id: u64,
    pub named: BTreeSet<String>,
}

impl NodeNames {
    pub fn new(node_id: u64, named: impl IntoIterator<Item = String>) -> Self {
        Self {
            node_id,
            named: named.into_iter().collect(),
        }
    }
}

impl NodeSnapshotNames for NodeNames {
    fn node_id(&self) -> u64 {
        self.node_id
    }

    fn named_keys(&self) -> BTreeSet<String> {
        self.named.clone()
    }
}

/// Why a node read a snapshot object it then relies on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommittedReadPurpose {
    /// A restore at engine open.
    Restore,
    /// A snapshot install the node committed.
    CommittedInstall,
    /// A read of the object a send names, inside the send's RPC, while its
    /// sender holds the object.
    SentObject,
}

impl fmt::Display for CommittedReadPurpose {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Restore => "restore",
            Self::CommittedInstall => "committed install",
            Self::SentObject => "read of an object a send holds",
        })
    }
}

/// How a read answered.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadOutcome {
    Found,
    NotFound,
    /// Any other failure, which I3 does not cover.
    Failed,
}

impl ReadOutcome {
    /// The outcome of a store read's `result`.
    pub fn of<T>(result: &Result<T, SnapshotStoreError>) -> Self {
        match result {
            Ok(_) => Self::Found,
            Err(SnapshotStoreError::NotFound { .. }) => Self::NotFound,
            Err(_) => Self::Failed,
        }
    }
}

/// A read of `path` by node `node_id` for `purpose`, as the caller
/// recorded it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommittedRead {
    pub node_id: u64,
    pub path: String,
    pub purpose: CommittedReadPurpose,
    pub outcome: ReadOutcome,
}

/// A violated invariant, with what violated it.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SnapshotInvariantViolation {
    #[error("I1: node {node_id} names {path}, which the store does not hold")]
    NamedKeyMissing { node_id: u64, path: String },
    #[error("I2: node {node_id} deleted {path}, outside its own directory and no sweep's key")]
    ForeignDelete { node_id: u64, path: String },
    #[error("I3: node {node_id} found no object at {path} in a {purpose}")]
    CommittedReadNotFound {
        node_id: u64,
        path: String,
        purpose: CommittedReadPurpose,
    },
    #[error("I4: {path}, which no node names, holds the marker after {age:?}, past {bound:?}")]
    SupersededHoldsMarker {
        path: String,
        age: Duration,
        bound: Duration,
    },
    #[error("I5: node {node_id}'s directory holds {path}, which its metadata does not name")]
    UnnamedObject { node_id: u64, path: String },
}

/// I1: every key a node's metadata names exists in the store.
pub fn check_named_keys_exist<'n, N: NodeSnapshotNames + 'n>(
    view: &SnapshotStoreView,
    nodes: impl IntoIterator<Item = &'n N>,
) -> Result<(), SnapshotInvariantViolation> {
    for node in nodes {
        if let Some(path) = node
            .named_keys()
            .into_iter()
            .find(|path| !view.objects.contains_key(path))
        {
            return Err(SnapshotInvariantViolation::NamedKeyMissing {
                node_id: node.node_id(),
                path,
            });
        }
    }
    Ok(())
}

/// I2: every DELETE the store saw targets a key of the deleting node, or,
/// sent by the removed-node sweep, a key of the other node it sweeps.
pub fn check_no_foreign_delete(view: &SnapshotStoreView) -> Result<(), SnapshotInvariantViolation> {
    let admitted = |delete: &StoreDelete| match delete.kind {
        DeleteKind::Own => is_owned_by(&view.prefix, delete.node_id, &delete.path),
        DeleteKind::Swept { of } => {
            of != delete.node_id && is_owned_by(&view.prefix, of, &delete.path)
        }
    };
    match view.deletes.iter().find(|delete| !admitted(delete)) {
        Some(delete) => Err(SnapshotInvariantViolation::ForeignDelete {
            node_id: delete.node_id,
            path: delete.path.clone(),
        }),
        None => Ok(()),
    }
}

/// I3: no restore, no committed install and no read inside a send's RPC
/// read `NotFound`.
pub fn check_committed_reads_found<'r>(
    reads: impl IntoIterator<Item = &'r CommittedRead>,
) -> Result<(), SnapshotInvariantViolation> {
    match reads
        .into_iter()
        .find(|read| read.outcome == ReadOutcome::NotFound)
    {
        Some(read) => Err(SnapshotInvariantViolation::CommittedReadNotFound {
            node_id: read.node_id,
            path: read.path.clone(),
            purpose: read.purpose,
        }),
        None => Ok(()),
    }
}

/// I4: no object that no node names holds the marker longer than
/// `bound`. That covers a replaced current as well as an orphan, such as
/// a late PUT that landed after its build gave up or the object of an
/// abandoned build. An object is aged from the later of its landing and
/// its supersession: `superseded` maps a key a node named to how long ago
/// it stopped naming it, and a key missing from it is aged from its
/// landing. `holds_marker` reads an object's logical bytes. With stale
/// LISTs on, pass the bound plus their `stale_for`.
pub fn check_superseded_without_marker<'n, N: NodeSnapshotNames + 'n>(
    view: &SnapshotStoreView,
    nodes: impl IntoIterator<Item = &'n N>,
    superseded: &BTreeMap<String, Duration>,
    bound: Duration,
    holds_marker: impl Fn(&[u8]) -> bool,
) -> Result<(), SnapshotInvariantViolation> {
    let named: BTreeSet<String> = nodes
        .into_iter()
        .flat_map(|node| node.named_keys())
        .collect();
    for (path, object) in &view.objects {
        if named.contains(path) {
            continue;
        }
        let age = superseded
            .get(path)
            .map_or(object.age, |since| object.age.min(*since));
        if age > bound && holds_marker(object.bytes.as_slice()) {
            return Err(SnapshotInvariantViolation::SupersededHoldsMarker {
                path: path.clone(),
                age,
                bound,
            });
        }
    }
    Ok(())
}

/// I5, once quiet: each node's directory holds only keys its metadata
/// names. Run it after the store settled and every node's passes ran.
pub fn check_quiet_prefixes<'n, N: NodeSnapshotNames + 'n>(
    view: &SnapshotStoreView,
    nodes: impl IntoIterator<Item = &'n N>,
) -> Result<(), SnapshotInvariantViolation> {
    for node in nodes {
        let directory = node_directory(&view.prefix, node.node_id());
        let named = node.named_keys();
        if let Some(path) = view
            .objects
            .keys()
            .filter(|path| path.starts_with(&directory))
            .find(|path| !named.contains(*path))
        {
            return Err(SnapshotInvariantViolation::UnnamedObject {
                node_id: node.node_id(),
                path: path.clone(),
            });
        }
    }
    Ok(())
}

/// Whether `bytes` contains `marker`, for [`check_superseded_without_marker`].
pub fn contains_marker(bytes: &[u8], marker: &[u8]) -> bool {
    !marker.is_empty() && bytes.windows(marker.len()).any(|window| window == marker)
}

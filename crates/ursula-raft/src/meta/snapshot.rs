//! The meta group's snapshot: the standard snapshot record
//! (`group-0.snapshot.json` under `raft-log/meta/`) whose pointer is always
//! [`SnapshotLocation::Inline`], with the whole [`MetaState`] as its bytes.
//!
//! The meta group never writes its snapshot to the configured snapshot
//! store: an S3 store would put its objects under `node-{me}/group-0/`, the
//! owned directory of data group 0, whose garbage collection and owned
//! object pass delete by data group 0's holds. So the pointer a meta
//! snapshot record or a meta full snapshot carries is refused unless it is
//! inline, whatever `storage.snapshot.backend` says, and its bytes are
//! bounded: the state is held whole behind every placement view, rewritten
//! whole at every meta snapshot and sent whole in one install.

use ursula_proto::placement::MAX_GROUP_COUNT;
use ursula_runtime::SnapshotLocation;
use ursula_runtime::SnapshotLocationKind;
use ursula_runtime::SnapshotPointer;

use super::MetaState;

/// The bytes of meta state allowed per data group: 4 KiB.
///
/// A placement takes about 100 bytes at replication factor 3 in compact
/// MessagePack (two log ids, the voter sets of a move and their framing),
/// and a moving placement with full-width ids at replication factor 7 with
/// an abandoned target about 300. A retirement takes about 40 bytes (a node
/// id and a meta log id), and a node record about 20. 4 KiB per group
/// leaves room for every group to move while owing a retirement of each of
/// its earlier voters, and for node records of many times the ids a cluster
/// of [`MAX_GROUP_COUNT`] groups places them over.
pub const META_SNAPSHOT_STATE_BYTES_PER_GROUP: u64 = 4 * 1024;

/// The most bytes a meta snapshot's state may encode to: 4 KiB for each of
/// at most [`MAX_GROUP_COUNT`] groups, 16 MiB, against about 400 KiB for
/// 4,096 groups at replication factor 3. It stays far below the 256 MiB a
/// Raft gRPC message may carry, so a meta install always fits one
/// `FullSnapshot` RPC. A build whose state exceeds it is refused rather than
/// written, and a pointer that names more is refused rather than restored.
///
/// The bound is part of format epoch 6 and fixed for the epoch, because a
/// node refuses a meta record or install above it. A patch that raised it
/// would build meta snapshots that an earlier patch of the same minor
/// refuses to install during a rolling upgrade.
pub const META_SNAPSHOT_MAX_STATE_BYTES: u64 =
    match (MAX_GROUP_COUNT as u64).checked_mul(META_SNAPSHOT_STATE_BYTES_PER_GROUP) {
        Some(bytes) => bytes,
        None => u64::MAX,
    };

const _: () = assert!(META_SNAPSHOT_MAX_STATE_BYTES == 16 * 1024 * 1024);
const _: () =
    assert!(META_SNAPSHOT_MAX_STATE_BYTES < crate::grpc::RAFT_GRPC_MAX_MESSAGE_BYTES as u64);

/// Why a meta snapshot pointer is refused.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum MetaSnapshotError {
    /// The pointer names an object of a snapshot store. The meta group's
    /// snapshot is always inline, so the pointer is not one of its own.
    #[error("the meta group's snapshot is inline, and this pointer names a {kind} object")]
    NotInline { kind: SnapshotLocationKind },
    /// The state is larger than [`META_SNAPSHOT_MAX_STATE_BYTES`].
    #[error("the meta group's snapshot state takes {bytes} bytes, above its bound of {bound}")]
    TooLarge { bytes: u64, bound: u64 },
}

/// The pointer of a meta snapshot: inline, its state within
/// [`META_SNAPSHOT_MAX_STATE_BYTES`]. Every meta snapshot pointer is parsed
/// into one before it is written, restored or installed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetaSnapshotPointer {
    snapshot_id: String,
    state: Vec<u8>,
}

impl MetaSnapshotPointer {
    /// The pointer of `state` in the snapshot `snapshot_id`, refused when
    /// the state's encoding exceeds the bound.
    pub fn new(snapshot_id: String, state: &MetaState) -> Result<Self, MetaSnapshotError> {
        Self::checked(snapshot_id, crate::codec::encode_wire(state).to_vec())
    }

    fn checked(snapshot_id: String, state: Vec<u8>) -> Result<Self, MetaSnapshotError> {
        let bytes = u64::try_from(state.len()).unwrap_or(u64::MAX);
        if bytes > META_SNAPSHOT_MAX_STATE_BYTES {
            return Err(MetaSnapshotError::TooLarge {
                bytes,
                bound: META_SNAPSHOT_MAX_STATE_BYTES,
            });
        }
        Ok(Self { snapshot_id, state })
    }

    pub fn snapshot_id(&self) -> &str {
        &self.snapshot_id
    }

    /// The encoded state.
    pub fn state_bytes(&self) -> &[u8] {
        &self.state
    }

    /// The state the pointer holds.
    pub fn state(&self) -> Result<MetaState, ursula_stream::msgpack::DecodeError> {
        ursula_stream::msgpack::decode(&self.state)
    }

    /// The snapshot store's pointer, as the snapshot record and a full
    /// snapshot carry it.
    pub fn into_pointer(self) -> SnapshotPointer {
        SnapshotPointer {
            snapshot_id: self.snapshot_id,
            location: SnapshotLocation::Inline { bytes: self.state },
        }
    }
}

impl TryFrom<SnapshotPointer> for MetaSnapshotPointer {
    type Error = MetaSnapshotError;

    /// Refuses a pointer that is not inline, and an inline one above the
    /// bound.
    fn try_from(pointer: SnapshotPointer) -> Result<Self, Self::Error> {
        match pointer.location {
            SnapshotLocation::Inline { bytes } => Self::checked(pointer.snapshot_id, bytes),
            location @ (SnapshotLocation::Local { .. } | SnapshotLocation::S3 { .. }) => {
                Err(MetaSnapshotError::NotInline {
                    kind: location.kind(),
                })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use ursula_control::ControlCommand;
    use ursula_control::ControlPlaneState;
    use ursula_control::MaintenanceAuthority;
    use ursula_control::samples;
    use ursula_runtime::SnapshotCompression;
    use ursula_runtime::SnapshotLocation;
    use ursula_runtime::SnapshotLocationKind;
    use ursula_runtime::SnapshotPointer;

    use super::META_SNAPSHOT_MAX_STATE_BYTES;
    use super::MetaSnapshotError;
    use super::MetaSnapshotPointer;
    use crate::meta::MetaState;

    fn pointer(location: SnapshotLocation) -> SnapshotPointer {
        SnapshotPointer {
            snapshot_id: "group-0-7-2-19".to_owned(),
            location,
        }
    }

    /// A node backed by S3 (or a local store) still restores and installs
    /// only an inline meta snapshot: a pointer to a store's object is
    /// refused, whatever the store.
    #[test]
    fn a_meta_snapshot_pointer_that_is_not_inline_is_refused() {
        let s3 = pointer(SnapshotLocation::S3 {
            key: "snapshots/node-2/group-0/00000000000000000019-0123456789abcdef.snap".to_owned(),
            size_bytes: 4096,
            stored_size_bytes: 1234,
            compression: SnapshotCompression::Zstd,
        });
        assert_eq!(
            MetaSnapshotPointer::try_from(s3),
            Err(MetaSnapshotError::NotInline {
                kind: SnapshotLocationKind::S3,
            })
        );
        let local = pointer(SnapshotLocation::Local {
            path: "/var/lib/ursula/snapshots/group-0.snap".into(),
            size_bytes: 512,
        });
        assert_eq!(
            MetaSnapshotPointer::try_from(local),
            Err(MetaSnapshotError::NotInline {
                kind: SnapshotLocationKind::Local,
            })
        );
    }

    #[test]
    fn an_inline_meta_snapshot_round_trips_through_its_pointer() {
        let state = MetaState {
            history: None,
            control: samples::sample_state(),
        };
        let parsed = MetaSnapshotPointer::new("group-0-7-2-19".to_owned(), &state)
            .expect("a sample state is within the bound");
        assert_eq!(parsed.state().expect("the state decodes"), state);
        let pointer = parsed.clone().into_pointer();
        assert!(matches!(pointer.location, SnapshotLocation::Inline { .. }));
        assert_eq!(MetaSnapshotPointer::try_from(pointer), Ok(parsed));
    }

    /// The bound is checked where a pointer is parsed, so a record or an
    /// install above it is refused, not restored.
    #[test]
    fn an_inline_meta_snapshot_above_the_bound_is_refused() {
        let bound = usize::try_from(META_SNAPSHOT_MAX_STATE_BYTES).expect("fits");
        let at_bound = pointer(SnapshotLocation::Inline {
            bytes: vec![0; bound],
        });
        MetaSnapshotPointer::try_from(at_bound).expect("a state at the bound is accepted");
        let above = pointer(SnapshotLocation::Inline {
            bytes: vec![0; bound.saturating_add(1)],
        });
        assert_eq!(
            MetaSnapshotPointer::try_from(above),
            Err(MetaSnapshotError::TooLarge {
                bytes: META_SNAPSHOT_MAX_STATE_BYTES.saturating_add(1),
                bound: META_SNAPSHOT_MAX_STATE_BYTES,
            })
        );
    }

    /// A cluster of the most groups a cluster may have, bootstrapped at
    /// replication factor 3, takes a small share of the bound.
    #[test]
    fn the_largest_bootstrapped_cluster_is_far_within_the_bound() {
        let nodes = [1, 2, 3, 4, 5];
        let mut control = ControlPlaneState::default();
        let mut index = 0_u64;
        let mut at = || {
            index = index.saturating_add(1);
            samples::meta(1, index)
        };
        for node in nodes {
            control
                .apply(at(), ControlCommand::RegisterNode {
                    node: samples::node(node),
                    now_ms: 1,
                })
                .expect("a node registers");
        }
        control
            .apply(at(), ControlCommand::Bootstrap {
                params: samples::params(
                    &nodes,
                    ursula_proto::placement::MAX_GROUP_COUNT,
                    3,
                    4,
                    MaintenanceAuthority::Placement,
                ),
                nodes: samples::node_set(&nodes),
                now_ms: 2,
            })
            .expect("the cluster bootstraps");
        let state = MetaState {
            history: None,
            control,
        };
        let parsed = MetaSnapshotPointer::new("group-0-1-1-6".to_owned(), &state)
            .expect("the largest cluster fits");
        let bytes = u64::try_from(parsed.state_bytes().len()).expect("fits");
        assert!(
            bytes.saturating_mul(16) < META_SNAPSHOT_MAX_STATE_BYTES,
            "{bytes} bytes for the largest bootstrapped cluster"
        );
    }
}

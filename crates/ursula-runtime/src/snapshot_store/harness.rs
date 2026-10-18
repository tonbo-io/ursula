//! Test infrastructure for snapshot object ownership (#687): a faulty
//! in-memory store, a store wrapper that pauses operations at named gates,
//! and checkers of the ownership invariants. Nothing here runs in a
//! production build. It compiles under `cfg(madsim)`, where the S3 store
//! does not exist, under `cfg(test)`, and with the `test-support` feature.
//!
//! Module map:
//!
//! - [`memory`]: [`MemorySnapshotBucket`], one in-memory bucket that every
//!   node shares, and [`MemorySnapshotStore`], a node's store on it. Each
//!   request waits a drawn delay before and after it acts. PUTs can fail to
//!   the client and land anyway, LISTs can omit recent keys and repeat
//!   deleted ones, creates honour If-None-Match, nonces come from the seed,
//!   and a DELETE outside the deleting node's directory panics (I2). Its GC
//!   is P3's owner pass: it keeps no reference records.
//! - [`gated`]: [`GatedSnapshotStore`], which wraps a node's store and
//!   pauses each owned-key operation before or after it acts until a test
//!   releases it, so a test can force one order of a build against a GC
//!   pass. A waiter that is dropped gives up its turn.
//! - [`invariants`]: I1 to I5 over a [`SnapshotStoreView`] and what each
//!   node names, which the caller supplies.

pub mod gated;
pub mod invariants;
pub mod memory;
#[cfg(test)]
mod tests;

pub use gated::Gate;
pub use gated::GateError;
pub use gated::GateEvent;
pub use gated::GatePass;
pub use gated::GatePoint;
pub use gated::GatedSnapshotStore;
pub use gated::SnapshotStoreGates;
pub use gated::StoreOp;
pub use invariants::CommittedRead;
pub use invariants::CommittedReadPurpose;
pub use invariants::NodeNames;
pub use invariants::NodeSnapshotNames;
pub use invariants::ReadOutcome;
pub use invariants::SnapshotInvariantViolation;
pub use invariants::SnapshotStoreView;
pub use invariants::StoreDelete;
pub use invariants::ViewObject;
pub use invariants::check_committed_reads_found;
pub use invariants::check_named_keys_exist;
pub use invariants::check_no_foreign_delete;
pub use invariants::check_quiet_prefixes;
pub use invariants::check_superseded_without_marker;
pub use invariants::contains_marker;
pub use memory::DelayRange;
pub use memory::DeleteOutcome;
pub use memory::MemorySnapshotBucket;
pub use memory::MemorySnapshotStore;
pub use memory::PerMille;
pub use memory::PutOutcome;
pub use memory::SnapshotStoreEvent;
pub use memory::SnapshotStoreFaults;

/// The directory of every object of `node_id` under `prefix`, across its
/// groups: `{prefix}/node-{node}/`.
pub fn node_directory(prefix: &super::SnapshotPrefix, node_id: u64) -> String {
    format!("{prefix}/node-{node_id}/")
}

/// Whether `path` is an object key of `node_id` under `prefix`, by the one
/// owned-key parser ([`super::OwnedKey::parse`]).
pub fn is_owned_by(prefix: &super::SnapshotPrefix, node_id: u64, path: &str) -> bool {
    super::OwnedKey::parse(prefix, path).is_ok_and(|key| key.owner().node_id() == node_id)
}

/// A small seeded generator (SplitMix64). The harness draws every fault,
/// delay and nonce from it, never from a thread or process RNG, so a seed
/// replays the same draws in the same order of requests.
#[derive(Debug, Clone)]
pub(crate) struct SplitMix64(u64);

impl SplitMix64 {
    pub(crate) fn new(seed: u64) -> Self {
        Self(seed)
    }

    pub(crate) fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ z.wrapping_shr(30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ z.wrapping_shr(27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ z.wrapping_shr(31)
    }

    /// A draw below `bound`, or 0 when `bound` is 0.
    pub(crate) fn below(&mut self, bound: u64) -> u64 {
        self.next_u64().checked_rem(bound).unwrap_or(0)
    }
}

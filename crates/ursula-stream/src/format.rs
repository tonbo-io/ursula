//! Format epoch: the one version number for everything Ursula persists or
//! sends between nodes.
//!
//! Epoch 1 is the format of Ursula 0.5.x and earlier, and of main builds
//! before format epoch 2 landed. Epoch 2 is the format of Ursula 0.6. A binary
//! reads exactly one epoch: it refuses data and peers from any other epoch
//! loudly, before it writes anything, and there is no in-place upgrade.
//!
//! Every version number that a 0.5.x binary checks is set equal to
//! [`FORMAT_EPOCH`]: the Raft WAL journal header, the Raft gRPC protocol, the
//! S3 snapshot references and the backup format. The data directory and the
//! object-storage namespace carry a marker with the epoch, and every snapshot
//! starts with a format-epoch frame.
//!
//! Policy:
//!
//! - Within an epoch, a release must not change apply-time semantics,
//!   persisted formats or the Raft RPC protocol.
//! - Any change that does so bumps [`FORMAT_EPOCH`] and requires a fresh
//!   install: new object-storage prefixes and empty data directories. If a
//!   future release needs an apply-time change under a rolling upgrade, it adds
//!   a narrowly scoped gate at that point.
//! - Epoch 2 is frozen when 0.6.0 is tagged. Main builds before the tag are
//!   not supported on each other's data; every deployment of such a build
//!   starts from fresh prefixes and empty data directories.

/// The format epoch this binary reads and writes.
pub const FORMAT_EPOCH: u32 = 2;

/// The shared tail of every format-epoch refusal: what this binary reads and
/// that old data has no in-place upgrade. Messages name epochs and "0.5.x",
/// never the binary's own crate version.
pub fn format_epoch_refusal(subject: &str, found: &str) -> String {
    format!(
        "{subject} {found}; this binary reads format epoch {FORMAT_EPOCH} only. There is no \
         in-place upgrade from Ursula 0.5.x (format epoch 1): install fresh, or run the release \
         that wrote it"
    )
}

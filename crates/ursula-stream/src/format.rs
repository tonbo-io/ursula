//! Format epoch: the one version number for everything Ursula persists or
//! sends between nodes.
//!
//! Epoch 1 is the format of Ursula 0.5.x and earlier, and of main builds
//! before format epoch 2 landed. Epoch 2 is the format of Ursula 0.6. Epoch 3
//! checksums each Raft WAL frame's length together with a per-file generation
//! sequence (`docs/architecture/single-raft-wal.md`). A binary reads exactly
//! one epoch: it refuses data, backups and peers from any other epoch loudly,
//! before it writes anything, and it does not convert them.
//!
//! Every version number that an earlier binary checks is set equal to
//! [`FORMAT_EPOCH`]: the Raft WAL journal header and state files, the Raft
//! gRPC protocol, group snapshots, the S3 snapshot references, stream
//! snapshots and the backup format. The data directory and the
//! object-storage namespace carry a marker with the epoch, and every group
//! snapshot starts with a format-epoch frame. Cold-index pages carry a page
//! version of their own.
//!
//! Policy:
//!
//! - Within an epoch, a release must not change apply-time semantics,
//!   persisted formats or the Raft RPC protocol.
//! - Any change that does so bumps [`FORMAT_EPOCH`] and requires a fresh
//!   install: new object-storage prefixes and empty data directories. If a
//!   future release needs an apply-time change under a rolling upgrade, it adds
//!   a narrowly scoped gate at that point.
//! - Epoch 2 was frozen when 0.6.0 was tagged. Epoch 3 is frozen when the
//!   first release that writes it is tagged. Main builds before that tag are
//!   not supported on each other's data; every deployment of such a build
//!   starts from fresh prefixes and empty data directories.

/// The format epoch this binary reads and writes.
pub const FORMAT_EPOCH: u32 = 3;

/// The text of every refusal of data of another format epoch:
/// `{subject} {found}` and the epoch this binary reads. Messages name epochs,
/// never the binary's own crate version.
pub fn format_epoch_refusal(subject: &str, found: &str) -> String {
    format!("{subject} {found}; this binary reads format epoch {FORMAT_EPOCH} only")
}

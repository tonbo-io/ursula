//! Format epoch: the one version number for everything Ursula persists or
//! sends between nodes.
//!
//! Epoch 1 is the format of Ursula 0.5.x and earlier, and of main builds
//! before format epoch 2 landed. Epoch 2 is the format of Ursula 0.6. Epoch 3
//! checksums each Raft WAL frame's length together with the sequence number of
//! the journal segment that holds it (`docs/architecture/raft-wal.md`); it is
//! the format of Ursula 0.7. Epoch 4 is the format of Ursula 0.8: write keys
//! with a producer record of one receipt, closed-epoch reports, write digests
//! and the per-stream epoch floor. A binary reads exactly one epoch: it refuses data, backups and peers from any
//! other epoch loudly, before it writes anything, and it does not convert them.
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
//! - There is no migration between epochs. Data of an older epoch moves by
//!   export and re-import through the applications that own it; neither the
//!   server nor `ursulactl` converts it. 0.8 refuses every epoch-3 data
//!   directory, object-storage prefix, peer, snapshot and backup.
//! - Epoch 2 was frozen when 0.6.0 was tagged, epoch 3 when 0.7.0 was tagged.
//!   Epoch 4 is frozen when 0.8.0 is tagged. Main builds before that tag are
//!   not supported on each other's data; every deployment of such a build
//!   starts from fresh prefixes and empty data directories.

/// The format epoch this binary reads and writes.
pub const FORMAT_EPOCH: u32 = 4;

/// The text of every refusal of data of another format epoch:
/// `{subject} {found}`, the epoch this binary reads, and that nothing converts
/// between epochs. Messages name epochs, never the binary's own crate version.
pub fn format_epoch_refusal(subject: &str, found: &str) -> String {
    format!(
        "{subject} {found}; this binary reads format epoch {FORMAT_EPOCH} only and does not \
         convert other epochs (move data by export and re-import)"
    )
}

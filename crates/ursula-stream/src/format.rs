//! Format epoch: the one version number for everything Ursula persists or
//! sends between nodes, and the backup format, which moves only when what a
//! backup holds changes.
//!
//! Epoch 1 is the format of Ursula 0.5.x and earlier, and of main builds
//! before format epoch 2 landed. Epoch 2 is the format of Ursula 0.6. Epoch 3
//! checksums each Raft WAL frame's length together with the sequence number of
//! the journal segment that holds it (`docs/architecture/raft-wal.md`); it is
//! the format of Ursula 0.7. Epoch 4 is the format of Ursula 0.8: write keys
//! with a producer record of one receipt, closed-epoch reports, write digests
//! and the per-stream epoch floor, and history identity: the group's history
//! event, the history a producer record was claimed in, and what a stream
//! inherited from the history before. Epoch 5 is the format of Ursula 0.9:
//! the `ExpireStreams` command, a clock on `DeleteStream`, and writes that
//! remove only the stream they name. A binary reads exactly one epoch: it refuses data and peers from any other
//! epoch loudly, before it writes anything, and it does not convert them.
//!
//! Every version number that an earlier binary checks is set equal to
//! [`FORMAT_EPOCH`]: the Raft WAL journal header and state files, the Raft
//! gRPC protocol, group snapshots and the S3 snapshot references. The data
//! directory and the object-storage namespace carry a marker with the epoch,
//! and every group snapshot starts with a format-epoch frame. Cold-index
//! pages carry a page version of their own.
//!
//! A backup carries [`BACKUP_FORMAT_VERSION`] instead. It covers the
//! manifest, each group's `StreamSnapshot` in named MessagePack with every
//! type it contains, and the cold objects a backup references (cold-index
//! pages and the cold key layout), in encoding and in meaning. It is the
//! format epoch that last changed one of them, so it never exceeds
//! [`FORMAT_EPOCH`]. Backup format 4 is written by 0.8 and 0.9.
//!
//! Policy:
//!
//! - Within an epoch, a release must not change apply-time semantics,
//!   persisted formats or the Raft RPC protocol.
//! - Any change that does so bumps [`FORMAT_EPOCH`] and requires a fresh
//!   install: new object-storage prefixes and empty data directories. If a
//!   future release needs an apply-time change under a rolling upgrade, it adds
//!   a narrowly scoped gate at that point.
//! - A change to what a backup holds also sets [`BACKUP_FORMAT_VERSION`] to
//!   the new epoch, and the release notes say that backups of the earlier
//!   format no longer restore.
//! - There is no migration between epochs. Data of an older epoch moves by
//!   `ursulactl` backup and restore when its backup format is this binary's,
//!   and otherwise by export and re-import through the applications that own
//!   it; neither the server nor `ursulactl` converts it. 0.9 refuses every
//!   epoch-4 data directory, object-storage prefix, peer and group snapshot,
//!   and restores epoch-4 backups.
//! - Epoch 2 was frozen when 0.6.0 was tagged, epoch 3 when 0.7.0 was tagged,
//!   epoch 4 when 0.8.0 was tagged. Epoch 5 is frozen when 0.9.0 is tagged.
//!   Main builds before that tag are not supported on each other's data;
//!   every deployment of such a build starts from fresh prefixes and empty
//!   data directories.

/// The format epoch this binary reads and writes.
pub const FORMAT_EPOCH: u32 = 5;

/// The backup format this binary reads and writes: the format epoch that last
/// changed what a backup holds.
pub const BACKUP_FORMAT_VERSION: u32 = 4;

const _: () = assert!(BACKUP_FORMAT_VERSION <= FORMAT_EPOCH);

/// The text of every refusal of data of another format epoch:
/// `{subject} {found}`, the epoch this binary reads, and that nothing converts
/// between epochs. Messages name epochs, never the binary's own crate version.
pub fn format_epoch_refusal(subject: &str, found: &str) -> String {
    format!(
        "{subject} {found}; this binary reads format epoch {FORMAT_EPOCH} only and does not \
         convert other epochs (move data by export and re-import)"
    )
}

/// The text of every refusal of a backup of another backup format:
/// `{subject} {found}` and the backup format this binary reads and writes.
pub fn backup_format_refusal(subject: &str, found: &str) -> String {
    format!(
        "{subject} {found}; this binary reads and writes backup format \
         {BACKUP_FORMAT_VERSION} only and does not convert other formats"
    )
}

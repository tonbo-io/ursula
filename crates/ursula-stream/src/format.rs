//! The version of everything Ursula persists or sends between nodes, and the
//! rule for changing each one.
//!
//! **Rule.** Every artifact has its own version. A release bumps an
//! artifact's version only when that artifact's encoding or meaning changes,
//! and leaves every other version alone.
//!
//! | Artifact                                                   | Version                        | 0.6 | 0.7 |
//! | ---------------------------------------------------------- | ------------------------------ | --- | --- |
//! | Raft WAL: journal segment headers and WAL state files      | [`RAFT_WAL_VERSION`]           | 2   | 3   |
//! | Raft group snapshots: the leading version frame            | [`GROUP_SNAPSHOT_VERSION`]     | 2   | 2   |
//! | S3 snapshot references and pins: `version`                 | [`SNAPSHOT_REFERENCE_VERSION`] | 2   | 2   |
//! | Cold-index pages: the page header                          | [`COLD_INDEX_PAGE_VERSION`]    | 2   | 2   |
//! | Stream snapshots: backup group objects, snapshot import    | [`STREAM_SNAPSHOT_VERSION`]    | 2   | 2   |
//! | Backup manifest, `/__ursula/backup/info`: `format_version` | [`BACKUP_FORMAT_VERSION`]      | 2   | 2   |
//!
//! Cold chunks and external payloads carry no version of their own: they
//! hold stream bytes as written, and the cold-index pages and stream state
//! that name them carry the versions.
//!
//! **Format epoch.** [`FORMAT_EPOCH`] is not the version of an encoding. It
//! names the releases that can share one cluster's state, and three things
//! carry it:
//!
//! - the Raft WAL directory marker `{raft.wal.path}/raft-log/FORMAT_EPOCH`,
//! - the object-storage marker `URSULA_FORMAT_EPOCH`,
//! - the Raft gRPC protocol version ([`RAFT_GRPC_PROTOCOL_VERSION`]) of every
//!   RPC. Ursula 0.5 and 0.6 compare it with their own epoch, so it stays
//!   equal to the epoch.
//!
//! A binary runs one epoch. It refuses WAL directories, object-storage
//! prefixes and peers of another epoch before it writes anything, and there
//! is no in-place upgrade. A release bumps the epoch when nodes of the
//! previous release could no longer share its cluster: when it bumps the
//! version of an artifact that a cluster keeps to itself (the Raft WAL, group
//! snapshots, snapshot references), or changes the Raft gRPC protocol in a
//! way the previous release cannot answer.
//!
//! **Between epochs.** Data moves to a new epoch through a backup:
//! `ursulactl backup-create` on the old cluster, a copy of its cold objects
//! under the same keys, and `ursulactl restore` into a new cluster. Stream
//! snapshots, the backup format, cold-index pages and cold objects therefore
//! cross from one epoch to the next. An epoch bump leaves their versions
//! alone. A release that bumps one of them keeps reading the previous
//! version, so the release before it keeps an upgrade path.
//!
//! History:
//!
//! - Epoch 1: Ursula 0.5.x and earlier, and main builds before epoch 2.
//! - Epoch 2: Ursula 0.6.x. The WAL, group snapshot, snapshot reference,
//!   stream snapshot and backup versions were all the epoch, 2.
//! - Epoch 3: Ursula 0.7. The Raft WAL checksums each frame's length together
//!   with its segment's sequence (`docs/architecture/single-raft-wal.md`), and
//!   the WAL directory holds new run-state, metadata and topology files, so
//!   the WAL version is 3. No other encoding changed and every other version
//!   stays at 2: 0.7 restores 0.6 backups and reads 0.6 cold objects. The new
//!   snapshot reference pins are records of version 2. The Raft
//!   gRPC protocol keeps every 0.6 message and adds `RejoinBarrier`, which a
//!   node falls back from when its peer answers `UNIMPLEMENTED`. Its version
//!   moved to 3 with the epoch.
//!
//! Policy:
//!
//! - Within an epoch, a release must not change apply-time semantics,
//!   persisted formats or the Raft RPC protocol. If a future release needs an
//!   apply-time change under a rolling upgrade, it adds a narrowly scoped gate
//!   at that point.
//! - Epoch 2 was frozen when 0.6.0 was tagged. Epoch 3 is frozen when the
//!   first release that writes it is tagged. Main builds before that tag are
//!   not supported on each other's data; every deployment of such a build
//!   starts from fresh prefixes and empty data directories.

/// The format epoch: which releases can share one cluster's WAL
/// directories, object-storage prefix and Raft gRPC peers.
pub const FORMAT_EPOCH: u32 = 3;

/// The Raft gRPC protocol version every RPC carries. It is the format epoch,
/// because Ursula 0.5 and 0.6 compare it with theirs.
pub const RAFT_GRPC_PROTOCOL_VERSION: u32 = FORMAT_EPOCH;

/// The version of the Raft WAL: the header of every journal segment and of
/// every WAL state file (core metadata, run state, topology). Version 3 since
/// Ursula 0.7.
pub const RAFT_WAL_VERSION: u16 = 3;

/// The version in the leading frame of every Raft group snapshot, in a
/// snapshot store or sent to a peer. Unchanged since Ursula 0.6.
pub const GROUP_SNAPSHOT_VERSION: u32 = 2;

/// The version of every S3 snapshot reference and pin record. Unchanged since
/// Ursula 0.6.
pub const SNAPSHOT_REFERENCE_VERSION: u32 = 2;

/// The version in the header of every cold-index page. Unchanged since
/// Ursula 0.6.
pub const COLD_INDEX_PAGE_VERSION: u16 = 2;

/// The version of the MessagePack [`crate::StreamSnapshot`] encoding: a
/// backup's group objects and the snapshot import command. Unchanged since
/// Ursula 0.6.
pub const STREAM_SNAPSHOT_VERSION: u32 = 2;

/// The version of the backup layout: `manifest.json` plus one stream snapshot
/// object per Raft group. `ursulactl` writes it into the manifest, and a
/// server reports the version it exports and imports in
/// `/__ursula/backup/info`. Unchanged since Ursula 0.6.
pub const BACKUP_FORMAT_VERSION: u32 = 2;

/// The operations guide section that moves data from the previous release
/// (Ursula 0.6) to this one. Refusals of older data point there.
pub const UPGRADE_GUIDE_URL: &str = "https://ursula.tonbo.io/docs/operations#upgrading-from-06";

/// The text of a refusal of cluster data that another release wrote:
/// `{subject} {found}`, what this binary reads, and how data of the previous
/// release moves to it. Messages name versions and the releases that wrote
/// them, never the binary's own crate version.
pub fn other_release_refusal(subject: &str, found: &str, reads: &str) -> String {
    format!(
        "{subject} {found}; this binary reads {reads} only, and there is no in-place upgrade. \
         Move the data of an Ursula 0.6 cluster to a new cluster with ursulactl backup-create \
         and restore ({UPGRADE_GUIDE_URL}), or run the release that wrote it"
    )
}

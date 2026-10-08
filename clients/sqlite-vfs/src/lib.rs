//! SQLite loadable extension: a shim VFS ("ursula", registered as the default) over "unix" that
//! replicates every WAL commit of an attached database to an Ursula stream *before* any of the
//! transaction's frames reach the local `-wal` file. The stream is the source of truth; the local
//! db file and WAL are a cache of it.
//!
//! * `SELECT ursula_attach(path, stream_url)` takes the host lock `<db>-ursula.lock` (held for the
//!   process lifetime), catches the file up from the stream, claims the stream for this owner (a
//!   new producer epoch, which fences every earlier owner), catches up to the claim and attaches the
//!   file. It requires that no connection to the file is open. Returns the stream offset applied.
//!   Offsets are the server's strings (`Stream-Next-Offset`), opaque: kept as they came, compared
//!   only lexicographically (the protocol orders them that way), never computed; `-1` is the
//!   protocol's "beginning of the stream", and stands for "none" where an offset may be absent.
//!   A db file with a sidecar (`<db>-ursula`: it is the cache of a stream) opens only while an
//!   attach of it has succeeded in this process: before that, after a failed attach, or in a
//!   process that never attached it, opening it fails (SQLITE_CANTOPEN), so it is never written
//!   unreplicated. A file without a sidecar passes through to "unix".
//! * Stream: `application/octet-stream`, one self-delimiting frame per append (see [`frame`]):
//!   a commit carries the db size after it and the transaction's final page images; a claim carries
//!   the owner's producer epoch. Appends use the idempotent producer (`Producer-Id`
//!   `sqlite-ursula-vfs/<Stream-Incarnation>` per stream incarnation, `Producer-Epoch` per owner,
//!   `Producer-Seq` per append; commits also carry `Stream-Seq`, see `stream_seq`): an append
//!   whose outcome is unknown is retried with the same sequence until the server answers (a
//!   duplicate is acknowledged without being applied twice);
//!   a 403 with `Producer-Epoch` means another owner claimed the stream (an authorizer's 401 or
//!   403 is a refused token, never a fence; see `auth`). Every request after attach's first
//!   `HEAD` carries that `HEAD`'s `Stream-Incarnation` as a precondition, which the server checks
//!   atomically with the request (a write at Raft apply): nothing of an owner reaches a stream
//!   deleted and recreated at its path, and the server's 412 fences and poisons the owner (an
//!   attach that meets one rebuilds against the new stream; see the design doc, §6).
//! * WAL writes of an attached database go to a per-database overlay (reads and the file size see
//!   it). The write of the commit frame's page data (the frame whose header carries a non-zero
//!   "db size after commit") is the commit point: the transaction's final page images become one
//!   frame, appended. On an ack the overlay is written to the local WAL and every later write of
//!   that transaction (checksum rewrites, padding) goes straight to it; when the write transaction
//!   ends (the WAL write lock is released) the sidecar `<db>-ursula` (stream offset and epoch
//!   reflected locally) advances. On a rejection or an exhausted retry budget the overlay is
//!   dropped, the write fails with SQLITE_IOERR_WRITE (SQLite rolls the transaction back) and the
//!   database is poisoned until it is re-attached. The overlay belongs to the write transaction: it
//!   is cleared whenever the WAL write lock is taken or released, so a rolled-back transaction's
//!   spilled frames never shadow a later one's.
//! * Durability is the stream's acknowledgement alone: the local files (db, -wal, -shm, sidecar)
//!   are a cache, not fsynced on the commit path (`xSync` of an attached database is a no-op,
//!   whatever `PRAGMA synchronous` says). The sidecar records the kernel's boot id, the stream's
//!   incarnation, the db file's inode and what it needs of the local WAL (`WalClaim`); attach
//!   trusts the local files only when all four check out (written since this boot, from this
//!   incarnation of the stream, into this file, and the WAL still holds the claimed frames) and
//!   otherwise discards them and rebuilds from the latest snapshot and the tail (from a stream
//!   deleted and recreated at the path, whatever its length). The db file alone is fsynced,
//!   rarely, so a crash-consistent image of the files (a disk snapshot) is either
//!   verifiably complete or rejected: before the WAL starts a new generation or is truncated to
//!   nothing, and at attach before a sidecar that relies on it. Attach never opens local files
//!   through SQLite before replaying onto them: it folds the WAL into the db file itself.
//! * Snapshots and retention (see [`snapshot`]): once the log since the latest snapshot (frame
//!   bytes counted locally: replayed at attach, appended since, carried in the sidecar) exceeds the
//!   database size (and `URSULA_VFS_SNAPSHOT_MIN_BYTES`, default 8 MiB), a background thread per
//!   attached database checkpoints the local WAL through a private connection, pins the result with
//!   a read transaction (no commit may land in between; otherwise it tries again later), copies the
//!   db file's pages, publishes them at the stream offset they reflect, reads the snapshot back and
//!   only then advances the stream's retention to the *previous* snapshot's offset. Attach installs
//!   the latest snapshot when the local file is missing or behind it, then replays the tail.
//! * `SELECT ursula_status(path)` returns
//!   `{"offset","epoch","poisoned","fenced","reason","snapshot","retained","local","installed"}`
//!   (offsets as strings; `local`: the stream offset of the local state attach started from, `-1`
//!   when it rebuilt the file; `installed`: the offset of the snapshot attach installed, `-1` for
//!   none; `snapshot`, `retained`: `-1` for none);
//!   `SELECT ursula_stats(path)` drains per-commit, per-checkpoint and per-snapshot numbers (bench).
//!
//! Test hook: `URSULA_VFS_ABORT_AFTER_ACK=<n>` aborts the process right after the n-th acknowledged
//! commit of an attachment, before the local WAL write. `URSULA_VFS_RETRY_MS` bounds every retry
//! loop: appends with an unknown outcome, idempotent PUTs, and reads answered 429/503 (default
//! 30000). `URSULA_VFS_SNAPSHOT_MIN_BYTES` sets the smallest log (bytes since the latest snapshot)
//! that triggers a snapshot.
//!
//! Module map:
//!
//! - [`frame`]: the stream's frames (commits and claims).
//! - [`snapshot`]: snapshot bodies.
//! - `attach`: `ursula_attach`, from the local files' trust decision to the bound attachment.
//! - `auth`: the bearer token every request carries.
//! - `claim`: claiming a stream (fencing earlier owners) and re-claiming an expired producer.
//! - `client`: the stream over blocking HTTP, with the retries.
//! - `config`: settings and test hooks from the environment.
//! - `db`: attached databases and the process-wide registry.
//! - `error`: the error type, and the classes callers act on (gone, recreated, fenced).
//! - `extension`: the entry point and the SQL functions.
//! - `host`: the host's SQLite API, the "unix" VFS and private connections.
//! - `local`: the sidecar, the boot id, and the db file locks.
//! - `snapshotter`: the snapshot thread, snapshots and retention.
//! - `status`: `ursula_status` and `ursula_stats`.
//! - `vfs`: the VFS file methods and the commit path.
//! - `wal`: the local WAL's format, recovery scan and fold.

mod attach;
mod auth;
mod claim;
mod client;
mod config;
mod db;
mod error;
mod extension;
pub mod frame;
mod host;
mod local;
pub mod snapshot;
mod snapshotter;
mod status;
mod vfs;
mod wal;

pub use extension::sqlite3_extension_init;

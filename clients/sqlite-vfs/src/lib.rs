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
//!   unreplicated. A file without a sidecar passes through to "unix", unless its last attach in this
//!   process failed (an attach refused for the file as it stands before it changed anything,
//!   `in_use`, `unreplicated_file` or `other_stream`, is no failed attach). Attach also refuses,
//!   changing nothing, while the process holds the db file,
//!   its -wal, -shm or -journal open outside this VFS (a SQLite connection opened before the
//!   extension was loaded or through another VFS, read-only ones included), whose commits would
//!   never reach the stream. Writes outside the extension after an attach are not detected (the
//!   design doc's §7). A fenced owner's new read transactions fail too (SQLITE_IOERR_READ).
//!   `SELECT ursula_attach(path, stream_url, 'read_only')` catches the file up the same way but
//!   claims nothing and never writes the stream (no claim, commit, snapshot or retention move): it
//!   fences no owner, works while a layer in front of the stream refuses writes, and every write to
//!   the file fails with SQLITE_READONLY. An attach refused with 402 fails with SQLITE_AUTH, and
//!   one whose claim is refused before it reached the stream in any other way (see below) with
//!   SQLITE_IOERR_AUTH: nothing was claimed.
//! * Stream: `application/octet-stream`, one self-delimiting frame per append (see [`frame`]):
//!   a commit carries the db size after it and the transaction's final page images; a claim carries
//!   the owner's producer epoch. Appends use the idempotent producer (`Producer-Id`
//!   `sqlite-ursula-vfs/<Stream-Incarnation>` per stream incarnation, `Producer-Epoch` per owner,
//!   `Producer-Seq` per append; commits also carry `Stream-Seq`, see `stream_seq`): an append
//!   whose outcome is unknown is retried with the same sequence until the server answers (a
//!   duplicate is acknowledged without being applied twice);
//!   a 403 with `Producer-Epoch` means another owner claimed the stream (an authorizer's 401 or
//!   403 is a refused token, never a fence; see `auth`, and neither is a 402: a layer in front of
//!   the stream refusing it, see below). Every request after attach's first
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
//!   database is poisoned until it is re-attached. A commit refused before it reached the stream
//!   fails the transaction alone with SQLITE_IOERR_AUTH: a 402 from a layer in front of the stream
//!   (an authorizer, a quota or billing service) or a 4xx marked `x-ursula-not-applied` (an
//!   authorizer's 403 among them) at once, and 401s, 429s and 5xx marked `x-ursula-not-applied`
//!   (the server, which refuses a write before it proposes it under write pressure or while it
//!   knows no leader for the stream, a gateway that refused it before passing it on) once the
//!   retry budget runs out on them. A marker counts only with the vouch
//!   `x-ursula-not-applied-checked`, which Ursula's gateway adds from 0.7.3 on and a node never
//!   sends (see `client::applied_nothing`): without a gateway, only a 401, 402 or 429 counts.
//!   Nothing of it reached the stream, so the database is not poisoned, reads go on from the local
//!   files, and the next commit is sent as usual (the epoch and the claim stay valid). Unless an
//!   attempt of the append may have reached the stream (no answer, a 307, or a 5xx without a
//!   vouched marker): its outcome is then unknown, and the commit poisons as on an exhausted
//!   budget (see `client::append`). So does a 403 without `Producer-Epoch` or a vouched marker,
//!   which a filtering proxy may send in place of the stream's own answer. A refused commit that
//!   was applied after all (a layer in front of the stream broke the rule of design doc §2) is
//!   usually caught at the next commit, which goes out with its sequence and is answered as its
//!   duplicate: that commit poisons (`Db::append_commit`). Unless an attempt of that commit got no
//!   answer, or an answer that proves nothing, first: it is then acknowledged, with a warning
//!   (`refused_commit_unchecked`). Design doc §2 lists what else the check misses, such as an
//!   attach before the next commit. The overlay belongs to the write transaction: it is
//!   cleared whenever the WAL write lock is taken or released, so a rolled-back transaction's
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
//!   db file's pages, fsyncs the WAL and the db file (a write-back error poisons the database, and
//!   nothing is published), publishes the pages at the stream offset they reflect, reads the
//!   snapshot back and only then advances the stream's retention to the *previous* snapshot's
//!   offset. A publish or retention move refused with 402 holds snapshots until the next
//!   acknowledged commit, like a commit refused by a layer that refuses other writes too, and
//!   unlike one refused as `too_large`. A failed attempt waits before the next, twice as long
//!   after each failure, up to an hour while the body is too large and five minutes otherwise,
//!   give or take a fifth, unless the database shrinks by more than a quarter (a body over a size
//!   limit fails the same way until then). A large body that fails while the stream still answers
//!   a `HEAD` below 500 is too large, and otherwise it is an outage, after which the first
//!   acknowledged commit brings the next attempt forward to within 30 s.
//!   Attach installs the latest snapshot when the local file is missing or behind it, then replays
//!   the tail.
//! * `SELECT ursula_status(path)` returns
//!   `{"offset","epoch","poisoned","fenced","reason","read_only","payment_required",
//!   "payment_reason","writes_refused","refused_kind","refused_reason","snapshot","retained",
//!   "local","installed","attach_ms","commits","append_retries","log_bytes","snapshot_due_bytes",
//!   "snapshot_age_ms","snapshot_failures","snapshot_error","snapshot_bytes","error"}` (offsets as
//!   strings;
//!   `reason`: the first poison's, which a later one never replaces; `fenced`: a fence was seen
//!   since, first or later, never reset; `writes_refused`: the latest write was refused before it
//!   reached the stream, until a commit is acknowledged (never while poisoned), with what refused
//!   it in `refused_kind` (`payment_required`, `unauthorized`, `forbidden`, `rate_limited`,
//!   `not_applied` or `too_large`) and why in `refused_reason` (the layer's explanation, or the
//!   marker's token when it sent no body, and always for `not_applied`, and for `too_large` the
//!   frame's size and limit, or the token of the `413`'s marker);
//!   `payment_required`, `payment_reason`: the latest refusal a layer answered, when it is a 402
//!   (a transaction refused as `too_large` leaves them); `local`: the stream offset of
//!   the local state attach started from, `-1` when it rebuilt the file; `installed`: the offset of
//!   the snapshot attach installed, `-1` for none; `snapshot`, `retained`: `-1` for none;
//!   `snapshot_bytes`: the compressed size of the latest snapshot body built, published or not;
//!   `error`: why writes fail now, `{"kind","action","message"}` or null (see `error::ErrorKind`);
//!   the rest are the owner's health, see `status`);
//!   `SELECT ursula_stats(path)` drains per-commit, per-checkpoint and per-snapshot numbers (bench):
//!   at most the newest 8,192 of each kind between drains, and `dropped` counts the rest.
//!
//! Every SQL function's error reads `<function>: <kind>: <message>`, the kind a stable token
//! (`error::ErrorKind`) with one action (`error::Action`); `attach_failed` names the failure it
//! wraps next (`<function>: attach_failed: <kind>: <message>`).
//!
//! Test hook: `URSULA_VFS_ABORT_AFTER_ACK=<n>` aborts the process right after the n-th acknowledged
//! commit of an attachment, before the local WAL write. `URSULA_VFS_RETRY_MS` bounds every retry
//! loop: appends with an unknown outcome, idempotent PUTs, and reads answered 429, 503 or 307 or
//! that got no answer (default 30000). No request follows a redirect: a 307 is sent again to the
//! same URL, and any other redirect fails the request. `URSULA_VFS_SNAPSHOT_MIN_BYTES` sets the
//! smallest log (bytes since the latest snapshot) that triggers a snapshot. `URSULA_VFS_IDLE_MS`
//! sets how long a connection is kept for reuse after its last request (default 90000, at most
//! 15000 for snapshot transfers). `URSULA_VFS_MAX_FRAME_BYTES` sets the largest commit frame sent,
//! in bytes (32 MiB by default, from 64 KiB to 32 MiB: a value outside is taken to the nearer
//! bound, and one that is not a number of bytes is ignored, both logged once as
//! `setting_adjusted`). A larger transaction is refused as `too_large` without being sent. Test
//! hook: `URSULA_VFS_SNAPSHOT_RETRY_MS` sets the wait after a first failed snapshot (100 ms, at
//! least 1 ms).
//!
//! Module map:
//!
//! - [`codec`]: the 0.8 formats (`USQ2` frames, `USL1` log segments, `USS3` snapshot bodies) and
//!   the digests the server checks, not yet used by the extension.
//! - [`frame`]: the stream's frames (commits and claims).
//! - [`snapshot`]: snapshot bodies.
//! - `attach`: `ursula_attach`, from the local files' trust decision to the bound attachment.
//! - `auth`: the bearer token every request carries.
//! - `claim`: claiming a stream (fencing earlier owners) and re-claiming an expired producer.
//! - `client`: the stream over blocking HTTP, with the retries.
//! - `config`: settings and test hooks from the environment.
//! - `db`: attached databases (their poison and fence flag) and the process-wide registry.
//! - `descriptors`: the scan for descriptors of this process on a database's files outside the
//!   extension, which attach refuses.
//! - `error`: the error type, the classes the code acts on (gone, recreated, fenced, refused,
//!   local damage), and the kinds and actions callers outside the extension see (`ErrorKind`,
//!   `Action`).
//! - `extension`: the entry point and the SQL functions.
//! - `guard`: panics stop at the C entry points instead of aborting the host process.
//! - [`ids`]: lineages, activations, positions and commit ids.
//! - `host`: the host's SQLite API, the "unix" VFS and private connections.
//! - `local`: the sidecar, the boot id, and the db file locks.
//! - `log`: log lines (logfmt) on the host's stderr.
//! - [`model`]: a model of the 0.8 server written from #581, in process and over HTTP (tests and
//!   the `model` feature, which builds the `ursula-vfs-model` binary).
//! - `net`: the HTTP agents' DNS cache, and the failures that prove a request never went out.
//! - `snapshotter`: the snapshot thread, snapshots and retention.
//! - `stats`: the measurements `ursula_stats` drains, at most 8,192 of each kind between drains.
//! - `status`: `ursula_status` and `ursula_stats`.
//! - `transport`: one attempt of a 0.8 request, over HTTP or to the model in process.
//! - `vfs`: the VFS file methods and the commit path.
//! - `wal`: the local WAL's format, recovery scan and fold.
//! - `wire`: the 0.8 requests and answers as values, shared by the transports and the model.

mod attach;
mod auth;
mod claim;
mod client;
pub mod codec;
mod config;
mod db;
mod descriptors;
mod error;
mod extension;
pub mod frame;
mod guard;
mod host;
pub mod ids;
mod local;
mod log;
#[cfg(any(test, feature = "model"))]
pub mod model;
mod net;
pub mod snapshot;
mod snapshotter;
mod stats;
mod status;
#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "unwired until the 0.8 driver (PR5) sends through it"
    )
)]
mod transport;
mod vfs;
mod wal;
#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "unwired until the 0.8 driver (PR5) sends through it"
    )
)]
mod wire;

pub use extension::sqlite3_extension_init;

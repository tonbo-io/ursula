//! SQLite loadable extension: a shim VFS ("ursula", registered as the default) over "unix" for
//! databases bound to Ursula streams (design: SQLite VFS 0.8, #582). A commit is local: at
//! SQLite's commit point the VFS appends the transaction's final pages to a log the extension owns
//! (one write, no fsync, no network), and a process-wide driver replicates the log to the stream in
//! the background, one append in flight, resent unchanged until a definite answer. Durable means
//! acknowledged: `ursula_synced` is the output gate.
//!
//! * `SELECT ursula_open(url, dir, options_json, timeout_ms)` binds the stream at `url`: it takes
//!   the stream's root in `dir` (`<dir>/<label>-<hash32>/`, the user cache's `sqlite-ursula/0.8` by
//!   default), materializes or builds its generation, takes the stream over and reconciles, and
//!   returns `{done, path, name, state}`; connections then open `path` with plain SQLite. With
//!   `timeout_ms` 0 it returns `{done: false, request}` at once, and `ursula_events` reports the
//!   outcome. A database file of the extension opens only while bound. After the last connection
//!   closes, the binding drains and retires (one `ursula_open` handed out retires too if no
//!   connection opens it within a minute), and its root unlocks. A host reopens through
//!   `ursula_open`, which takes a retiring binding back: a plain open of the path is refused once
//!   the binding retired.
//! * `SELECT ursula_synced(path, timeout_ms)` resolves once every commit logged before the call is
//!   acknowledged and a round trip that started after it found this owner still the owner.
//! * `SELECT ursula_events(cursor, timeout_ms)`: completions, acknowledgements, conditions, resets.
//! * `ursula_status(path)`, `ursula_stats(path)`, `ursula_cancel(request)`,
//!   `ursula_set_token(path, token)`.
//!
//! A write refused at admission (`backlog_full`) or one the extension does not capture
//! (`unsupported`) fails its transaction alone with `SQLITE_IOERR_AUTH`. A reset (another owner,
//! an unconfirmed commit, a failed fsync) fails every later statement of the database:
//! `SQLITE_IOERR_READ` for a read, `SQLITE_IOERR_WRITE` for a write. Every SQL function's error
//! reads `<function>: <kind>: <message>`.
//!
//! Settings: `URSULA_VFS_TOKEN_FILE`, `URSULA_VFS_CA_FILE`, `URSULA_VFS_IDLE_MS`,
//! `URSULA_VFS_MIN_UPLOAD_KBPS`, `URSULA_VFS_MAX_REQUEST_BYTES`. Test hooks:
//! `URSULA_VFS_ABORT_AT=<step>[:<n>]` (`config::Step`), `URSULA_VFS_PANIC_IN=<entry>`,
//! `URSULA_VFS_FAIL_SYNC=<path suffix>`, `URSULA_VFS_SNAPSHOT_DUE_BYTES=<bytes>`.
//!
//! Each bound database has a snapshot thread: past `max(db size, 8 MiB)` of acknowledged frame
//! bytes it publishes a `USS3` snapshot keyed by its stream offset and digest, and moves
//! retention one snapshot behind. Builds install the latest snapshot, then read the tail.
//!
//! Module map:
//!
//! - [`codec`]: the formats (`USQ2` frames, `USL1` log segments, `USS3` snapshot bodies) and the
//!   digests the server checks.
//! - `applier`: applies commits to a db file in batches of final page images.
//! - `auth`: the bearer token every request carries.
//! - `capture`: the WAL's layout, the shape of a write transaction's WAL writes, and its final page
//!   images at the commit point.
//! - `config`: settings and test hooks from the environment.
//! - `db`: bound databases (their commit-path state) and the process-wide registry.
//! - `driver`: the process-wide driver of the replicators and opens: a deterministic core and its
//!   runtime (a driver thread and blocking HTTP workers).
//! - `descriptors`: the scan for descriptors of this process on a database's files outside the
//!   extension, which an open refuses.
//! - `error`: the kinds and actions callers outside the extension see (`ErrorKind`, `Action`), and
//!   the SQL functions' error type.
//! - `extension`: the entry point, the SQLite version gate and the SQL functions.
//! - `generation`: the canonical stream path, a stream's locked root directory and the
//!   generations in it.
//! - `guard`: panics stop at the C entry points instead of aborting the host process.
//! - [`ids`]: lineages, activations, positions and commit ids.
//! - `host`: the host's SQLite API, the "unix" VFS and a private connection.
//! - `log`: log lines (logfmt) on the host's stderr.
//! - `materialize`: brings a generation's db file to the end of its log, and catches it up with
//!   frames from the stream.
//! - [`model`]: a model of the 0.8 server written from #581, in process and over HTTP (tests and
//!   the `model` feature, which builds the `ursula-vfs-model` binary).
//! - `net`: the HTTP agents' DNS cache, and the failures that prove a request never went out.
//! - `open`: opening a stream (design §5.5, the rows M1 supports) up to its takeover, run by the
//!   driver.
//! - `options`: `ursula_open`'s `options_json`.
//! - `publication`: whether SQLite published a logged commit, from the calls on its main db handle.
//! - `replicator`: the replicator state machine of one bound database.
//! - `replog`: the commit log, `USL1` segments in a generation's `log/`.
//! - `snapshotter`: the snapshot thread of a bound database: snapshots keyed by offset and digest,
//!   retention one snapshot behind.
//! - `stats`: the measurements `ursula_stats` drains, at most 8,192 of each kind between drains.
//! - `status`: the JSON the SQL functions return.
//! - `transport`: one attempt of a request, over HTTP or to the model in process.
//! - `vfs`: the VFS file methods and the commit path.
//! - `wire`: the requests and answers as values, shared by the transports and the model.

mod applier;
mod auth;
mod capture;
pub mod codec;
mod config;
mod db;
mod descriptors;
mod driver;
mod error;
mod extension;
mod generation;
mod guard;
mod host;
pub mod ids;
mod log;
mod materialize;
#[cfg(any(test, feature = "model"))]
pub mod model;
mod net;
mod open;
mod options;
mod publication;
mod replicator;
mod replog;
mod snapshotter;
mod stats;
mod status;
mod transport;
mod vfs;
mod wire;

#[cfg(test)]
mod testdir;

pub use extension::sqlite3_extension_init;

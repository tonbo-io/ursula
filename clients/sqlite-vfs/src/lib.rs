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
//!   outcome. After the last connection closes, the binding drains and retires, and its root
//!   unlocks. A path `ursula_open` handed out stays openable until a connection opens it, or for a
//!   minute. A host reopens through `ursula_open`, which takes a retiring binding back: a plain
//!   open of the path is refused once the binding retired. A stream binds under one `dir` at a
//!   time in the process: `ursula_open` under another one waits for a binding nothing of the host
//!   holds to retire, and is refused `bound_elsewhere` otherwise (`in_use` when it has commits not
//!   acknowledged, or does not retire in time).
//! * With `read_only` in `options_json`, `ursula_open` claims nothing. In a process that owns the
//!   stream it returns the owner's binding. Otherwise it binds a reader: the stream as of the open,
//!   built or caught up in a `reader-<n>/` directory of the root, whose connections open read-only
//!   (writes fail with `SQLITE_READONLY`), whose `ursula_synced` resolves at once, and which
//!   retires as soon as its last connection closes. On the owner's binding the extension does not
//!   make a connection read-only: a write there is the owning process's own. A host of the SQL
//!   functions opens the path `ursula_open` returned for `read_only` with
//!   `SQLITE_OPEN_READONLY`, as the TypeScript package does.
//! * Outside the extension a database file is not a database: on disk its bytes 0..16 hold a
//!   marker in place of SQLite's magic (the VFS translates them for a bound file), from the moment
//!   its generation is published, since a generation without a commit is published with the page
//!   1 of an empty database. Its WAL is `db-uwal`, a name SQLite never probes. SQLite in another
//!   process, a connection opened before the extension loaded, `?vfs=unix` and the `sqlite3` shell
//!   fail with `SQLITE_NOTADB`, reads and writes alike, and change no file.
//! * A local generation that a history change, a replaced db file or a recreate (`recreate` in
//!   `options_json`) would discard is moved whole into the root's `salvage/`, as a plain SQLite
//!   database with a `manifest.json`, and never read or deleted again: `ursula_status` lists it.
//! * `SELECT ursula_synced(path, timeout_ms)` resolves once every commit logged before the call is
//!   acknowledged and a round trip that started after it found this owner still the owner.
//! * `SELECT ursula_events(cursor, timeout_ms)`: completions, acknowledgements, conditions, resets.
//! * `SELECT ursula_import(url, file, options_json, timeout_ms)` imports a plain SQLite file of
//!   4096-byte pages into a new stream at `url`, as its first snapshot, and returns
//!   `{done, offset, digest, pages, bytes}`. The file is only read. A stream that exists and is
//!   not empty is refused `stream_state`, and nothing is published on it. Until the import's
//!   snapshot is published, opens of the stream refuse `stream_state`, and after an import that
//!   failed once it claimed the stream they do until the stream is deleted or recreated.
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
//! `URSULA_VFS_FAIL_SYNC=<path suffix>`, `URSULA_VFS_SNAPSHOT_DUE_BYTES=<bytes>`,
//! `URSULA_VFS_SNAPSHOT_HOLD=<dir>`.
//!
//! Each bound database has a snapshot thread: past `max(db size, 8 MiB)` of acknowledged frame
//! bytes it publishes a `USS3` snapshot keyed by its stream offset and digest, and moves
//! retention one snapshot behind. Builds install the latest snapshot, then read the tail.
//!
//! Module map:
//!
//! - [`codec`]: the formats (`USQ2` frames, `USL1` log segments, `USS3` snapshot bodies, the
//!   page-1 marker) and the digests the server checks.
//! - `applier`: applies commits to a db file in batches of final page images, page 1 marked.
//! - `auth`: the bearer token every request carries.
//! - `capture`: the WAL's layout, the shape of a write transaction's WAL writes, and its final page
//!   images at the commit point.
//! - `config`: settings and test hooks from the environment.
//! - `db`: bound databases (their commit-path state) and the process-wide registry.
//! - `driver`: the process-wide driver of the replicators and opens: a deterministic core and its
//!   runtime (a driver thread and blocking HTTP workers).
//! - `error`: the kinds and actions callers outside the extension see (`ErrorKind`, `Action`), and
//!   the SQL functions' error type.
//! - `extension`: the entry point, the SQLite version gate and the SQL functions.
//! - `generation`: the canonical stream path, a stream's locked root directory and its reader
//!   directories, and the generations in them (`gen-<id32>` directories).
//! - `guard`: panics stop at the C entry points instead of aborting the host process.
//! - `import`: `ursula_import`, a plain SQLite file imported as the first snapshot of a new
//!   stream.
//! - [`ids`]: lineages, generation directory ids, activations, positions and commit ids.
//! - `host`: the host's SQLite API, the "unix" VFS and a private connection.
//! - `json`: the flat JSON objects the extension writes and reads (status, options, manifests).
//! - `log`: log lines (logfmt) on the host's stderr.
//! - `materialize`: brings a generation's db file to the end of its log, and catches it up with
//!   frames from the stream.
//! - [`model`]: a model of the 0.8 server written from #581, in process and over HTTP (tests and
//!   the `model` feature, which builds the `ursula-vfs-model` binary).
//! - `net`: the HTTP agents' DNS cache, and the failures that prove a request never went out.
//! - `open`: opening a stream (design §5.5) up to its takeover, or a reader's bind, run by the
//!   driver.
//! - `options`: `ursula_open`'s `options_json`.
//! - `publication`: whether SQLite published a logged commit, from the calls on its main db handle.
//! - `replicator`: the replicator state machine of one bound database.
//! - `replog`: the commit log, `USL1` segments in a generation's `log/`.
//! - `salvage`: generations a history change, a replaced db file or a recreate would discard,
//!   moved aside into the root's `salvage/` with a `manifest.json`.
//! - `snapshotter`: the snapshot thread of a bound database: snapshots keyed by offset and digest,
//!   retention one snapshot behind.
//! - `stats`: the measurements `ursula_stats` drains, at most 8,192 of each kind between drains.
//! - `status`: the JSON the SQL functions return.
//! - `transport`: one attempt of a request, over HTTP or to the model in process.
//! - `vfs`: the VFS file methods (the page-1 marker, the `db-uwal` WAL), its `xAccess` and
//!   `xDelete`, and the commit path.
//! - `wire`: the requests and answers as values, shared by the transports and the model.

mod applier;
mod auth;
mod capture;
pub mod codec;
mod config;
mod db;
mod driver;
mod error;
mod extension;
mod generation;
mod guard;
mod host;
pub mod ids;
mod import;
mod json;
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
mod salvage;
mod snapshotter;
mod stats;
mod status;
mod transport;
mod vfs;
mod wire;

#[cfg(test)]
mod testdir;

pub use extension::sqlite3_extension_init;

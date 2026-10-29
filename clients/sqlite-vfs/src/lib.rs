//! SQLite loadable extension: a shim VFS ("ursula", registered as the default) over "unix" for
//! databases bound to Ursula streams (docs/architecture/sqlite-vfs.md). A commit is local: at
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
//! * `SELECT ursula_classify(message, errcode, path)` says what a failure a host met is: `{kind,
//!   action, class, message, fix, request_id}` for a SQL function's error, or for a statement's
//!   I/O error on the database at `path`, and `null` for SQLite's own errors.
//! * `SELECT tonbo_resolve(name_or_url, options_json, timeout_ms)` resolves the name of a Tonbo
//!   database into its stream URL with the account's API key, and fetches the key's first Streams
//!   token before it returns `{done, url, name, key_tokens}` (`tonbo`). A failure is its outcome
//!   too, `{done: true, error: {kind, action, class, message, fix, request_id}}`. With
//!   `timeout_ms` 0 it returns `{done: false, request}` at once, a `resolved` event ends it, and a
//!   call with the same arguments then takes its outcome. Only the SQL functions call into Tonbo,
//!   and the JSON rendering shows what it returns: the open, the registry and the driver never do.
//!
//! A write refused at admission (`backlog_full`), one the extension does not capture
//! (`unsupported`) or a commit above the commit limit (`too_large`) fails its transaction alone with
//! `SQLITE_IOERR_AUTH`. A reset (another owner, an unconfirmed commit, a failed fsync) fails every
//! later statement of the database: `SQLITE_IOERR_READ` for a read, `SQLITE_IOERR_WRITE` for a
//! write. Every SQL function's error reads `<function>: <kind>: <message>`, which names the kind
//! alone. Wherever else a kind is named (events, status, `ursula_cancel`, `ursula_classify`), its
//! action (`retry`, `reopen`, `stop`, `fix`) and its class (`write_refused`, `reset`,
//! `unavailable`, `setup`), the error a host throws, are named beside it, and a reset or a failed
//! `synced()` carries the message a host shows for it.
//!
//! Settings: `URSULA_VFS_TOKEN_FILE`, `URSULA_VFS_CA_FILE`, `URSULA_VFS_IDLE_MS`,
//! `URSULA_VFS_MIN_UPLOAD_KBPS`, `URSULA_VFS_MAX_REQUEST_BYTES`, `URSULA_VFS_MAX_COMMIT_BYTES`.
//! `tonbo_resolve` reads `TONBO_API_KEY`, `TONBO_API_ORIGIN`, `TONBO_AUTH_ORIGIN`, and
//! `XDG_CONFIG_HOME` or `HOME` for the `tonbo login` file, at each call.
//! Test hooks: `URSULA_VFS_ABORT_AT=<step>[:<n>]` (`config::Step`), `URSULA_VFS_PANIC_IN=<entry>`,
//! `URSULA_VFS_FAIL_SYNC=<path suffix>`, `URSULA_VFS_SNAPSHOT_DUE_BYTES=<bytes>`,
//! `URSULA_VFS_SNAPSHOT_HOLD=<dir>`, `URSULA_VFS_HOST_DIR=<absolute dir>` (the host directory when
//! the compiled-in `/var/lib/sqlite-ursula/host` does not exist).
//!
//! An owner `ursula_open` records its cache directory in the host directory, when there is one,
//! before it claims anything, and fails `local_setup` when it cannot (`hostdir`, design: Hosts).
//! A host reads every root of the recorded directories, and checks every open root lock, with
//! [`inspection::inspect`], which the `ursula-vfs-host` binary runs, and removes the records of
//! directories that no longer exist with [`prune::prune`]. While a host holds the host
//! directory's `hold` file ([`hold::take`], `ursula-vfs-host hold`), a write transaction of an
//! owner binding waits before it takes the WAL write lock, for at most 60 s, so the logs drain.
//! The wait itself returns no `SQLITE_BUSY` and calls no busy handler; afterwards the request
//! meets what other writers did meanwhile, as a slow transaction would.
//!
//! A host drains the orphans on its disk, the roots no process holds that still hold commits the
//! stream has not acknowledged, with [`drain::drain`], which `ursula-vfs-host drain` runs. Its open
//! only continues a root's own claim behind the pre-check and never takes the stream over, and it
//! opens no connection, so it loads no SQLite.
//!
//! A child forked after its parent started the extension (its first `ursula_open`, `ursula_events`
//! or `ursula_import`) inherits what the extension made without the threads that serve it: there
//! every SQL function and every I/O on a file of a bound database fails `forked` (`fix`: open after
//! the fork), closing such a file touches nothing the parent holds, the wal-index regions of bound
//! databases are private copies, and files of no bound database pass through (`fork`, design:
//! Forks).
//!
//! Each binding opened by a host of the SQL functions has a snapshot thread: past
//! `max(db size, 8 MiB)` of acknowledged frame bytes that thread publishes a `USS3` snapshot keyed
//! by its stream offset and digest, and moves retention one snapshot behind. A drain's binding has
//! none. Builds install the latest snapshot, then read the tail.
//!
//! Comments cite `design: <Heading>` for a heading of `docs/architecture/sqlite-vfs.md` and
//! `extensions: <Heading>` for a heading of the extensions specification.
//!
//! Module map:
//!
//! - [`codec`]: the formats (`USQ2` frames, `USL1` log segments, `USS3` snapshot bodies, the
//!   page-1 marker) and the digests the server checks.
//! - `applier`: applies commits to a db file in batches of final page images, page 1 marked.
//! - `auth`: the bearer token every request carries: the token set, the token file, or an API
//!   key's exchanges, which a refresher thread keeps fresh.
//! - `capture`: the WAL's layout, the shape of a write transaction's WAL writes, and its final page
//!   images at the commit point.
//! - `config`: settings and test hooks from the environment.
//! - `db`: bound databases (their commit-path state) and the process-wide registry.
//! - [`drain`]: a host's drain of the orphan roots on its disk: the inspection's walk and read, an
//!   open of each orphan that continues its own claim, and the outcome its bytes decide.
//! - `driver`: the process-wide driver of the replicators and opens: a deterministic core and its
//!   runtime (a driver thread and blocking HTTP workers).
//! - `error`: the kinds, actions and classes callers outside the extension see (`ErrorKind`,
//!   `Action`, `ErrorClass`), the one classification of a failure a host met (`classify`), and the
//!   SQL functions' error type.
//! - `extension`: the entry point, the SQLite version gate and the SQL functions.
//! - `fork`: the fork guard: a child forked after its parent started the extension fails `forked`
//!   instead of using what the parent made, and gets private copies of bound wal-index regions.
//! - `generation`: the canonical stream path, a stream's locked root directory and its reader
//!   directories, and the generations in them (`gen-<id32>` directories).
//! - `guard`: panics stop at the C entry points instead of aborting the host process.
//! - [`hold`]: the host's hold on write transactions: the wait of a write transaction before its
//!   WAL write lock while a host holds `<host dir>/hold`, and the host's exclusive lock on it.
//! - `import`: `ursula_import`, a plain SQLite file imported as the first snapshot of a new
//!   stream.
//! - [`ids`]: lineages, generation directory ids, activations, positions and commit ids.
//! - `host`: the host's SQLite API, the "unix" VFS and a private connection.
//! - `hostdir`: the host directory, where an owner open records its cache directory for a host
//!   that snapshots or moves the process.
//! - [`inspection`]: a host's read of every recorded root and every open root lock: the
//!   unacknowledged bytes on disk, and whether the read is complete.
//! - `json`: the flat JSON objects the extension writes and reads (status, options, manifests,
//!   token answers, whose nested members it skips, and one level down for the Management API).
//! - `log`: log lines (logfmt) on the host's stderr.
//! - `materialize`: brings a generation's db file to the end of its log, and catches it up with
//!   frames from the stream.
//! - [`model`]: a model of the 0.8 server written from the protocol's specifications, in process
//!   and over HTTP (tests and the `model` feature, which builds the `ursula-vfs-model` binary).
//! - `net`: the HTTP agents' DNS cache, and the failures that prove a request never went out.
//! - `open`: opening a stream (design: Open) up to its takeover, or a reader's bind, run by the
//!   driver.
//! - `options`: `ursula_open`'s `options_json`.
//! - [`prune`]: a host's removal of the records of cache directories that no longer exist, and of
//!   the temp files of writers that no longer run, after a complete inspection pass of its own.
//! - `publication`: whether SQLite published a logged commit, from the calls on its main db handle.
//! - `replicator`: the replicator state machine of one bound database.
//! - `replog`: the commit log, `USL1` segments in a generation's `log/`.
//! - `salvage`: generations a history change, a replaced db file or a recreate would discard,
//!   moved aside into the root's `salvage/` with a `manifest.json`.
//! - `snapshotter`: the snapshot thread of a bound database: snapshots keyed by offset and digest,
//!   retention one snapshot behind.
//! - `stats`: the measurements `ursula_stats` drains, at most 8,192 of each kind between drains.
//! - `status`: the JSON the SQL functions return.
//! - `tonbo`: `tonbo_resolve`, a Tonbo database's stream URL by name, the key from the option,
//!   the environment or the `tonbo login` file, and the key's first Streams token.
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
pub mod drain;
mod driver;
mod error;
mod extension;
mod fork;
mod generation;
mod guard;
pub mod hold;
mod host;
mod hostdir;
pub mod ids;
mod import;
pub mod inspection;
mod json;
mod log;
mod materialize;
#[cfg(any(test, feature = "model"))]
pub mod model;
mod net;
mod open;
mod options;
pub mod prune;
mod publication;
mod replicator;
mod replog;
mod salvage;
mod snapshotter;
mod stats;
mod status;
mod tonbo;
mod transport;
mod vfs;
mod wire;

#[cfg(test)]
mod testdir;

pub use extension::sqlite3_extension_init;

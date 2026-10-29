# sqlite-ursula

SQLite databases replicated through [Ursula](https://ursula.tonbo.io) streams, for the stdlib `sqlite3` module of CPython 3.11 or later. The Python peer of the npm package [`@tonbo/sqlite-ursula`](https://github.com/tonbo-io/ursula/tree/main/clients/sqlite-ursula).

```python
import sqlite_ursula

db = sqlite_ursula.open("https://streams.example.com/acme/notes")
db.execute("CREATE TABLE IF NOT EXISTS notes(id INTEGER PRIMARY KEY, body TEXT)")
db.execute("INSERT INTO notes(body) VALUES (?)", ("hello",))
db.commit()  # committed on this host
db.synced()  # this database's commits are in the stream, and this process still owns it
send(reply)
```

**The one rule:** before anything outside the process acts on what you wrote or read (a response, a queue acknowledgement, a call to another service), call `db.synced()`.

A commit is local. The extension appends it to a log it owns and replicates the log to the stream in the background, on threads of its own. `synced()` returns once the stream holds every commit the process made on that database before the call, and a round trip that started after the call found this process still the owner. Scripts call it before they exit.

## Hosts

The extension loads into the `sqlite3` module's own SQLite and becomes its default VFS, so every `sqlite3.connect(path)` of a bound database in the process replicates, a framework's included. That takes a Python whose `sqlite3` loads extensions and runs a SQLite the extension supports. The extension's refusal of an older one names the version it needs.

| Python | hosts the extension |
| --- | --- |
| uv-managed CPython (`uv python install`), Homebrew, conda-forge | yes |
| `python:3.x-alpine` (Alpine 3.23 or newer) | yes, on x64 |
| `python:3.x-slim`, Debian and Ubuntu `apt`, RHEL and UBI, Amazon Linux, Colab | no: SQLite older than the extension supports |
| the python.org macOS installer, Apple's `/usr/bin/python3`, pyenv's default build | no: built without loadable extensions |
| Windows | no: use WSL |

On a Python that cannot host it, `open()` raises `UrsulaSetupError` with kind `host_unsupported`, whose `fix` says what to do for that reason: a Python that can host it, WSL on Windows, or `SQLITE_URSULA_VFS` where the install carries no library. `python -m sqlite_ursula` runs the same check and prints what it found.

### Docker

Switch the base image in one line:

```diff
-FROM python:3.13-slim
+FROM python:3.13-alpine
```

[`recipes/Dockerfile`](https://github.com/tonbo-io/ursula/blob/main/clients/sqlite-ursula-python/recipes/Dockerfile) is a complete example. To keep a Debian base, or on arm64, run a uv-managed CPython instead, as [`recipes/Dockerfile.uv`](https://github.com/tonbo-io/ursula/blob/main/clients/sqlite-ursula-python/recipes/Dockerfile.uv) does.

On macOS, use uv or Homebrew.

## open

`open(url, *, token, dir, read_only, recreate, max_pending_bytes, timeout, busy_timeout, **connect_kwargs)` returns an `UrsulaConnection`, a `sqlite3.Connection` subclass, so every library that takes one takes it.

- The first open of a stream in the process binds its cache in `dir` (by default `<user cache>/sqlite-ursula/0.8`), catches it up and claims the stream: the last open that reaches the stream owns it. Later opens in the process return another connection on the same binding, with no network. Concurrent opens from any thread share one.
- `token` is the bearer token requests carry. One token per process: `set_token(token)` replaces it, and `set_token(None)` goes back to `URSULA_VFS_TOKEN_FILE`.
- The open gives up after `timeout` seconds (30 by default) with `UrsulaUnavailableError`, whose kind is the last condition it met. Ctrl-C cancels it too.
- `busy_timeout` is `sqlite3.connect`'s `timeout`, 5 seconds by default, since a binding's connections share its files. The other keywords go to `sqlite3.connect`. `check_same_thread` is `False` unless given.
- `autocommit=False` (Python 3.12 or newer) is refused with `invalid_argument`. In that mode the connection is always in a transaction: CPython's own `ROLLBACK` after a refused `COMMIT` would hide the refusal, and a connection kept open would hold a read snapshot that stops checkpoints. Use the default transaction control, `isolation_level=None` or `autocommit=True`.
- `read_only=True` claims nothing and never fences the owner, and writes fail with `SQLITE_READONLY`.
- `db.close()` closes the connection. Once a binding's last connection closes, replication drains in the background and the binding retires. Keep `db` open while a framework uses `db.path`.

`UrsulaConnection` adds `name` (the stream's path, such as `/acme/notes`), `path` (the local database file, which frameworks open by path), `synced(timeout=None)`, `status()` and `on_reset(callback)` / `off_reset(callback)`.

```python
db = sqlite_ursula.open(url)
other = sqlite3.connect(db.path)  # a framework's own connection: it replicates too
```

`synced()` covers every connection of the process on this database (`db.path`), a framework's included, and no other: gate each database an output depends on. A negative or non-numeric `timeout` is `invalid_argument`. It raises `TimeoutError` after `timeout` seconds while the commits keep replicating, and `UrsulaResetError` on a reset. Network failures never raise from it: it waits. Ctrl-C interrupts it.

`import_sqlite(url, file, *, token, timeout)` makes a plain SQLite file with 4096-byte pages the first snapshot of a new stream, then `open()` it as any other.

## Errors

Four classes with `kind` and `action` (`retry`, `reopen`, `stop`, `fix`), whose cause (`__cause__`) is the SQLite error they replace. Branch on `action`. The extension says which failure is which (`ursula_classify`).

| class | raised by | kinds |
| --- | --- | --- |
| `UrsulaWriteRefusedError` | a write or COMMIT, nothing committed | `backlog_full` (`retry` after `synced()`), `unsupported`, `too_large` |
| `UrsulaResetError` | every statement after a reset, `synced()`, `on_reset` callbacks | `fenced` (`stop`), `stale`, `rewound`, `deleted`, `local_damage`, `stream_state`, `internal` |
| `UrsulaUnavailableError` | `open()` past its timeout | the last condition: `unreachable`, `unavailable`, `rate_limited`, `unauthorized`, `payment_required`, `forbidden`, or `cancelled` |
| `UrsulaSetupError` | `open()`, `import_sqlite()`, the calls the package guards in a forked child ([Forks](#forks)) | `host_unsupported`, `forked`, `invalid_url`, `invalid_argument`, `not_found`, `in_use`, `bound_elsewhere`, `local_setup`, and the rest of the extension's |

`UrsulaWriteRefusedError` and `UrsulaResetError` are also `sqlite3.OperationalError`, as the error they replace was, so existing `except sqlite3.OperationalError` clauses keep catching them, and `sqlite_errorcode` and `sqlite_errorname` are SQLite's. A transaction helper's `ROLLBACK` after one, which finds no transaction since SQLite rolled it back, does not hide it.

A reset rejects the database's pending `synced()` calls first, then runs its `on_reset` callbacks once, on a thread of the package's.

## Forks

Open databases after a fork, never before: the extension's replication does not survive `fork()`. In a child forked after its parent opened a database, `open()`, `import_sqlite()` and `set_token()` raise `forked`, and so do the statements, transactions, `synced()`, `status()` and `on_reset()` of an inherited `UrsulaConnection`, whose `close()` does nothing. Its other `sqlite3.Connection` methods (`backup`, `blobopen`, `serialize` and the like) and a framework's own connections on `db.path` are not the package's to guard: the extension fails their I/O on a bound file in the child. Importing the package before the fork is fine. Use gunicorn's `post_fork` hook, Celery's `worker_process_init` signal, or multiprocessing's `spawn` or `forkserver` start method.

## The extension

The package loads the extension its wheel carries, or the one `SQLITE_URSULA_VFS` names, built from [`clients/sqlite-vfs`](https://github.com/tonbo-io/ursula/tree/main/clients/sqlite-vfs). Wheels carry it for Linux x64 and arm64 with glibc 2.28 or newer, Linux x64 with musl, and macOS 11 or newer on arm64. Elsewhere the installer picks the wheel without a library, and `open()` raises `host_unsupported` unless `SQLITE_URSULA_VFS` names one. The design is [`docs/architecture/sqlite-vfs.md`](https://github.com/tonbo-io/ursula/blob/main/docs/architecture/sqlite-vfs.md).

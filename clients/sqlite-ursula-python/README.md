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

**The one rule:** before anything outside the process acts on what you wrote or read (a response, a queue acknowledgement, a call to another service), call `db.synced()`, `await db.asynced()`, or wrap the work in `with gate(db)` or `async with agate(db)`.

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

`UrsulaConnection` adds `name` (the stream's path, such as `/acme/notes`), `path` (the local database file, which frameworks open by path), `synced(timeout=None)`, `asynced(timeout=None)` ([asyncio](#asyncio)), `status()` and `on_reset(callback)` / `off_reset(callback)`.

```python
db = sqlite_ursula.open(url)
other = sqlite3.connect(db.path)  # a framework's own connection: it replicates too
```

`synced()` covers every connection of the process on this database (`db.path`), a framework's included, and no other: gate each database an output depends on. A negative or non-numeric `timeout` is `invalid_argument`. It raises `TimeoutError` after `timeout` seconds while the commits keep replicating, and `UrsulaResetError` on a reset. Network failures never raise from it: it waits. Ctrl-C interrupts it.

`import_sqlite(url, file, *, token, timeout)` makes a plain SQLite file with 4096-byte pages the first snapshot of a new stream, then `open()` it as any other.

## asyncio

`aopen()` takes the arguments `open()` takes and returns the same `UrsulaConnection`, and `db.asynced(timeout=None)` is `synced()`. Both wait on the event loop instead of blocking it, and neither uses the loop's default executor.

```python
db = await sqlite_ursula.aopen(url)
db.execute("INSERT INTO notes(body) VALUES (?)", ("hello",))
db.commit()
await db.asynced()
send(reply)
```

Cancelling `asynced()`, as `asyncio.timeout` around it does, ends the wait while the commits keep replicating. Cancelling `aopen()` cancels the bind, so it never goes on to claim the stream. Statements still run on the calling thread, as they do with `sqlite3`.

## Gates

`gate(*dbs, timeout=None)` and `agate(*dbs, timeout=None)` wrap a block of work: leaving the block waits for a `synced()` call made after it ended, on every database given, before the result or the block's error goes on.

```python
with sqlite_ursula.gate(db):
    reply = handle(request)
send(reply)

async with sqlite_ursula.agate(db, other):
    reply = await handle(request)
await send(reply)
```

- A block that only read waits too, since it may have read another connection's pending commit.
- An `UrsulaError` the block raises passes at once, as the block raised it, also when a library wrapped it (as `__cause__`, as SQLAlchemy's `.orig`, or in an `ExceptionGroup`). So does a raw `sqlite3` error of a framework's own connection that the extension classifies, also wrapped. With one database, a bare raw error that the extension names a refused write or a reset raises as that `UrsulaError`, which is a `sqlite3.OperationalError` too.
- Every other error waits as a result does, then passes. Ctrl-C and cancellations pass at once.
- The wait covers every database at once. A reset during it raises the `UrsulaResetError` instead of the outcome, and a failure on one database ends the wait on the others.
- `timeout` bounds the wait, not the block, and raises `TimeoutError`.

Effects inside the block, such as a call to another service, are not gated: call `db.synced()` before them.

## Tonbo

`sqlite_ursula.tonbo` opens a database of a Tonbo account by name, with a Tonbo API key. It is the peer of the npm entry `@tonbo/sqlite-ursula/tonbo`, and the extension does the work for both.

```python
from sqlite_ursula import tonbo

db = tonbo.open("notes")  # or the database's https:// URL, which skips the lookup
db = await tonbo.aopen("notes")
```

- `open(name_or_url, *, api_key, api_origin, auth_origin, token, **options)` takes the root `open()`'s arguments and returns the same `UrsulaConnection`. `timeout` (30 s by default) bounds the whole open, the lookup included. `aopen()` is the same for asyncio, and cancelling it cancels the lookup when no other call shares it.
- The key comes from `api_key`, else `TONBO_API_KEY`, else the login `tonbo login` saves in `${XDG_CONFIG_HOME:-~/.config}/tonbo/credentials.json`, which must be a regular file, not a symlink, with no group or other permission bits (such as `0600`).
- Streams requests carry tokens the extension exchanges for the key and refreshes in the background, beside replication. The open waits for the first one. An explicit `token`, or `URSULA_VFS_TOKEN_FILE`, serves Streams requests instead: then opening a URL needs no key. The process holds one key for Streams tokens.
- `api_origin` (or `TONBO_API_ORIGIN`) and `auth_origin` (or `TONBO_AUTH_ORIGIN`) name other origins for the Management API and the token endpoint. Each must be `https://`, or `http://` to a loopback host.
- `resolve_database(name_or_url, *, api_key, api_origin, auth_origin, token, timeout)` and `aresolve_database()` resolve a database without opening it, for another opener: `ResolvedDatabase(url, name, token)`. Concurrent calls with the same arguments, from any thread or task, share one lookup.
- `api_key`, `api_origin` and `auth_origin` must be strings when given. No key or token is logged, and no error message or `repr` carries one.

Its errors are `TonboError`, an `UrsulaError` whose action is `fix`, with `kind`, `fix` (the variable to set or the command to run, which ends the message) and `request_id` (the Tonbo API's, when it answered):

| kind | when | fix |
| --- | --- | --- |
| `missing_credentials` | no `api_key`, no `TONBO_API_KEY` and no login file | `Set TONBO_API_KEY, or run: tonbo login` |
| `credentials_refused` | the login file is a symlink, not a regular file, or open to group or others | `Run: chmod 600 <path>`, or replace the file |
| `invalid_api_key` | the API refused the key, the key cannot read the database, or the login expired | a valid key for the key's source, or `Run: tonbo login` |
| `database_not_found` | the account has no database of that name, or it is being deleted | `Run: tonbo database create <name>` |

Past its timeout while the Tonbo API did not answer, or while the database was still being created, `open()` raises `UrsulaUnavailableError` (`unreachable`, `unavailable` or `rate_limited`, action `retry`). A call that gives up while other calls share its lookup leaves the lookup to them and raises `cancelled`, and the last one cancels it. An origin it cannot use, an answer outside the APIs' contracts or a second key for Streams tokens raise `UrsulaSetupError` `invalid_argument`. A stream URL it cannot take, or one that would get the key's Streams tokens over plain `http://` to another host, raises `invalid_url`, an empty name `invalid_name`, and a login file it cannot read `local_setup`.

## Errors

Four classes with `kind` and `action` (`retry`, `reopen`, `stop`, `fix`), whose cause (`__cause__`) is the SQLite error they replace. Branch on `action`. The extension says which failure is which (`ursula_classify`).

| class | raised by | kinds |
| --- | --- | --- |
| `UrsulaWriteRefusedError` | a write or COMMIT, nothing committed, and a gate for a framework connection's refusal | `backlog_full` (`retry` after `synced()`), `unsupported`, `too_large` |
| `UrsulaResetError` | every statement after a reset, `synced()`, `asynced()`, a gate, `on_reset` callbacks | `fenced` (`stop`), `stale`, `rewound`, `deleted`, `local_damage`, `stream_state`, `internal` |
| `UrsulaUnavailableError` | `open()` or `aopen()` past its timeout | the last condition: `unreachable`, `unavailable`, `rate_limited`, `unauthorized`, `payment_required`, `forbidden`, or `cancelled` |
| `UrsulaSetupError` | `open()`, `aopen()`, `import_sqlite()`, the calls the package guards in a forked child ([Forks](#forks)) | `host_unsupported`, `forked`, `invalid_url`, `invalid_argument`, `not_found`, `in_use`, `bound_elsewhere`, `local_setup`, and the rest of the extension's |

`UrsulaWriteRefusedError` and `UrsulaResetError` are also `sqlite3.OperationalError`, as the error they replace was, so existing `except sqlite3.OperationalError` clauses keep catching them, and `sqlite_errorcode` and `sqlite_errorname` are SQLite's. A transaction helper's `ROLLBACK` after one, which finds no transaction since SQLite rolled it back, does not hide it.

A reset rejects the database's pending `synced()` calls first, then runs its `on_reset` callbacks once, on a thread of the package's.

## Forks

Open databases after a fork, never before: the extension's replication does not survive `fork()`. In a child forked after its parent opened a database, `open()`, `aopen()`, `import_sqlite()` and `set_token()` raise `forked`, and so do the statements, transactions, `synced()`, `asynced()`, `status()` and `on_reset()` of an inherited `UrsulaConnection`, whose `close()` does nothing. Its other `sqlite3.Connection` methods (`backup`, `blobopen`, `serialize` and the like) and a framework's own connections on `db.path` are not the package's to guard: the extension fails their I/O on a bound file in the child. Importing the package before the fork is fine. Use gunicorn's `post_fork` hook, Celery's `worker_process_init` signal, or multiprocessing's `spawn` or `forkserver` start method.

## The extension

The package loads the extension its wheel carries, or the one `SQLITE_URSULA_VFS` names, built from [`clients/sqlite-vfs`](https://github.com/tonbo-io/ursula/tree/main/clients/sqlite-vfs). Wheels carry it for Linux x64 and arm64 with glibc 2.28 or newer, Linux x64 with musl, and macOS 11 or newer on arm64. Elsewhere the installer picks the wheel without a library, and `open()` raises `host_unsupported` unless `SQLITE_URSULA_VFS` names one. The design is [`docs/architecture/sqlite-vfs.md`](https://github.com/tonbo-io/ursula/blob/main/docs/architecture/sqlite-vfs.md).

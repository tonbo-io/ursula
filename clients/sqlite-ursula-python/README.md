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

[`recipes/Dockerfile`](https://github.com/tonbo-io/ursula/blob/main/clients/sqlite-ursula-python/recipes/Dockerfile) is a complete example. To keep a Debian base, on arm64, or with the [`langgraph` extra](#langgraph), which does not install on Alpine, run a uv-managed CPython instead, as [`recipes/Dockerfile.uv`](https://github.com/tonbo-io/ursula/blob/main/clients/sqlite-ursula-python/recipes/Dockerfile.uv) does.

On macOS, use uv or Homebrew.

## open

`open(url, *, token, dir, read_only, recreate, max_pending_bytes, timeout, busy_timeout, **connect_kwargs)` returns an `UrsulaConnection`, a `sqlite3.Connection` subclass, so every library that takes one takes it.

- The first open of a stream in the process binds its cache in `dir` (by default `<user cache>/sqlite-ursula/0.8`), catches it up and claims the stream: the last open that reaches the stream owns it. Later opens in the process return another connection on the same binding, with no network. Concurrent opens from any thread share one.
- `token` is the bearer token requests carry, or a [token provider](#token-providers) for tokens that expire. One token per process: `set_token(token)` replaces it, and `set_token(None)` goes back to `URSULA_VFS_TOKEN_FILE`.
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

`import_sqlite(url, file, *, token, timeout)` makes a plain SQLite file with 4096-byte pages the first snapshot of a new stream, then `open()` it as any other. Its `token` is a string: the import blocks its thread, so nothing could refresh it meanwhile.

## Token providers

For short-lived tokens, pass a provider as `token` instead of running a refresher: a synchronous callable that takes the keywords `previous` and `timeout` and returns a `BearerToken(token, expires_at)`. `expires_at` is seconds since the epoch, as `time.time()` and a JWT's `exp` count them, or a timezone-aware `datetime`.

```python
import json
import time
import urllib.request

import sqlite_ursula
from sqlite_ursula import BearerToken


def token(*, previous: str | None, timeout: float) -> BearerToken:
    with urllib.request.urlopen("https://auth.example.com/token", timeout=timeout) as r:
        answer = json.load(r)
    return BearerToken(answer["access_token"], time.time() + answer["expires_in"])


db = sqlite_ursula.open("https://ursula.example.com/acme/notes", token=token)
```

- The package calls the provider before an open's first request when it holds no valid token, again after 70 to 80% of each token's lifetime, and again at once when a request was refused with `401`. Concurrent opens share one call, and a later open gets a valid token held at once.
- Each call runs on a daemon thread of the package, `sqlite-ursula-token`, never on your event loop: `aopen()` takes the same provider and waits for its token on the loop. So the provider is synchronous: an async function is `invalid_argument` at the open.
- Each call is handed `previous`, the token the provider last handed out, exactly as it handed it out, when the package will not take it again: the current one at a scheduled refresh, or the one the server refused after a `401`. It is `None` before the first call and after the provider stopped, except that a token an open's bind was refused under, or one a database still bound and blocked was refused under, stays `previous` across a stop, until the provider hands out another one or a string token or `set_token()` stops the provider. When `previous` is set, hand out a different token, and refill a cache that holds it. The extension takes an equal token as no change, so an answer equal to `previous`, compared without surrounding whitespace, counts as a failed call. A failed call is retried with backoff, and by the expiry of `previous` at the latest, so a cache that hands its token back until it expires is asked again as it turns over. From that expiry on only the failures since it count, so the backoff starts again at 1 s. When the provider hands back the token a database's replication was refused under, the package takes it again once each database that reported the refusal is closed and retired, with every commit acknowledged, or no longer blocked. A retry already scheduled then runs at once. A token that never changes belongs in a string `token` or in `URSULA_VFS_TOKEN_FILE`, not in a provider.
- `timeout` is the 30 s the package waits for the answer. A call that has not returned by then counts as failed, and a call cut off by a stop (a string token, `set_token()`, the last database closed) has its answer discarded, or never reaches the provider when it had not started. Either is left to finish on its thread, and the next call runs on a thread of its own.
- An exception whose `action` is `"fix"` fails for good, and the package calls the provider again only for the next open. An open waiting for the token raises it when it is an `UrsulaError`, such as `UrsulaUnavailableError("unauthorized: the key was revoked", "unauthorized", "fix")`, or the package's class it derives from, with the same message, kind and action and it as the `__cause__`, when it is a subclass of your own. Any other such exception, say a library's own, is the `__cause__` of an `UrsulaUnavailableError` `unauthorized` (action `fix`). Other exceptions are retried with backoff, from 1 s up to 30 s, while the token held keeps serving. An open whose `timeout` passes while it waits raises `UrsulaUnavailableError` `unavailable` (action `retry`), whose `__cause__` is the provider's last error, or none when no call failed. A token that is not printable ASCII without spaces, a naive `datetime`, or an `expires_at` that is not seconds since the epoch fails the open with `invalid_argument`, and so does a provider that does not take the keywords `previous` and `timeout`, before any call.
- One provider per process. Another one while databases the process opened are open is `invalid_argument`. An open with a string token, or `set_token()`, stops the provider, and so does closing the last database. A forked child starts with no provider.
- No token is logged, and the `repr` of a `BearerToken` leaves it out.

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

## LangGraph

`sqlite_ursula.langgraph` holds LangGraph's SQLite checkpointers and stores on a replicated database. Install it with the `langgraph` extra, which pins `langgraph-checkpoint-sqlite` to the releases CI tests (3.0.3 up to 3.2):

```sh
pip install 'sqlite-ursula[langgraph]'
```

```python
import sqlite_ursula
from sqlite_ursula.langgraph import UrsulaSaver

db = sqlite_ursula.open(url)
with UrsulaSaver(db) as saver:
    graph = builder.compile(checkpointer=saver)
    reply = graph.invoke(inputs, {"configurable": {"thread_id": "1"}})
send(reply)  # every checkpoint of the run is in the stream
```

| class | LangGraph's | closed by |
| --- | --- | --- |
| `UrsulaSaver(db)` | `SqliteSaver` | `close()` or `with` |
| `await AsyncUrsulaSaver.open(db)` | `AsyncSqliteSaver` | `aclose()` or `async with` |
| `UrsulaStore(db)` | `SqliteStore` | `close()` or `with` |
| `await AsyncUrsulaStore.open(db)` | `AsyncSqliteStore` | `aclose()` or `async with` |

- Each class opens a connection of its own on `db`'s binding, the async ones through aiosqlite, and closing it closes that connection. LangGraph writes checkpoints from threads of its own while the graph's nodes run, so the application keeps `db` for its own statements. The savers' connections keep SQLite's default transaction control, which makes each checkpoint, each task's writes and each deleted thread one transaction, whatever `db`'s is. The stores' connections open no transactions of their own, since `SqliteStore` runs its own `BEGIN` and `COMMIT`. Each connection is bound as `db` is: read-only when `db` is, with `db`'s busy timeout, and its errors are the package's.
- Every write returns once `db.synced()` (or `asynced()`) returned after it: a checkpoint, a task's writes, a deleted thread, a store put or delete, a store get or search that refreshes TTLs (`refresh_on_read`), a TTL sweep. LangGraph waits for a run's checkpoint writes before `invoke` returns, so the return is gated in every durability mode. With `durability="sync"`, each step also waits for the one before it, so what a node does rests on acknowledged checkpoints.
- Reads do not wait, and neither does the setup that creates the tables, which LangGraph runs before the first read: the next write's wait covers it. `AsyncUrsulaStore` waits in `aput` and `adelete`, not in the task that runs its batches, so a read never waits behind another call's write. Its `abatch` and `batch`, which take operations, are LangGraph's own: call `db.asynced()` after them.
- `stream()` and `astream()` yield a step's chunks before that step's checkpoint is acknowledged, and only the end of the iteration is gated. Call `db.synced()` or `await db.asynced()` before acting on a chunk.
- A write refused with `backlog_full` committed nothing: it waits for `synced()` and runs again. That is safe because each of these writes is one transaction. An `AsyncUrsulaStore` read batched with a refused write runs again at once. LangGraph's setup commits its statements one by one, and a store's migrations are not safe to run twice, so there each refused statement waits and runs again by itself. In the async adapters that wait blocks aiosqlite's thread until the stream acknowledges the commits pending before it, even when the caller of the setup is cancelled.
- A reset raises the `UrsulaResetError`, from a write or a read. Keep `db` open while the graph runs.
- `sqlite-vec`, which `langgraph-checkpoint-sqlite` requires, has no wheel for musl, so the extra does not install on Alpine. Use a Debian base with a uv-managed CPython, as [`recipes/Dockerfile.uv`](https://github.com/tonbo-io/ursula/blob/main/clients/sqlite-ursula-python/recipes/Dockerfile.uv) does.

## Errors

Four classes with `kind` and `action` (`retry`, `reopen`, `stop`, `fix`), whose cause (`__cause__`) is the SQLite error they replace. Branch on `action`. The extension says which failure is which (`ursula_classify`).

| class | raised by | kinds |
| --- | --- | --- |
| `UrsulaWriteRefusedError` | a write or COMMIT, nothing committed, and a gate for a framework connection's refusal | `backlog_full` (`retry` after `synced()`), `unsupported`, `too_large` |
| `UrsulaResetError` | every statement after a reset, `synced()`, `asynced()`, a gate, `on_reset` callbacks | `fenced` (`stop`), `stale`, `rewound`, `deleted`, `local_damage`, `stream_state`, `internal` |
| `UrsulaUnavailableError` | `open()` or `aopen()` past its timeout | the last condition: `unreachable`, `unavailable`, `rate_limited`, `unauthorized`, `payment_required`, `forbidden`, or `cancelled` |
| `UrsulaSetupError` | `open()`, `aopen()`, `import_sqlite()`, the calls the package guards in a forked child ([Forks](#forks)) | `host_unsupported`, `forked`, `invalid_url`, `invalid_argument`, `not_found`, `in_use`, `bound_elsewhere`, `local_setup`, and the rest of the extension's |

`UrsulaWriteRefusedError` and `UrsulaResetError` are also `sqlite3.OperationalError`, as the error they replace was, so existing `except sqlite3.OperationalError` clauses keep catching them, and `sqlite_errorcode` and `sqlite_errorname` are SQLite's. A transaction helper's `ROLLBACK` after one, or its `COMMIT` in a `finally` block, which finds no transaction since SQLite rolled it back, does not hide it.

A reset rejects the database's pending `synced()` calls first, then runs its `on_reset` callbacks once, on a thread of the package's.

## Forks

Open databases after a fork, never before: the extension's replication does not survive `fork()`. In a child forked after its parent opened a database, `open()`, `aopen()`, `import_sqlite()` and `set_token()` raise `forked`, and so do the statements, transactions, `synced()`, `asynced()`, `status()` and `on_reset()` of an inherited `UrsulaConnection`, whose `close()` does nothing. Its other `sqlite3.Connection` methods (`backup`, `blobopen`, `serialize` and the like) and a framework's own connections on `db.path` are not the package's to guard: the extension fails their I/O on a bound file in the child. Importing the package before the fork is fine. Use gunicorn's `post_fork` hook, Celery's `worker_process_init` signal, or multiprocessing's `spawn` or `forkserver` start method.

## The extension

The package loads the extension its wheel carries, or the one `SQLITE_URSULA_VFS` names, built from [`clients/sqlite-vfs`](https://github.com/tonbo-io/ursula/tree/main/clients/sqlite-vfs). Wheels carry it for Linux x64 and arm64 with glibc 2.28 or newer, Linux x64 with musl, and macOS 11 or newer on arm64. Elsewhere the installer picks the wheel without a library, and `open()` raises `host_unsupported` unless `SQLITE_URSULA_VFS` names one. The design is [`docs/architecture/sqlite-vfs.md`](https://github.com/tonbo-io/ursula/blob/main/docs/architecture/sqlite-vfs.md).

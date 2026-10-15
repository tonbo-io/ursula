# @tonbo/sqlite-ursula

SQLite databases replicated through [Ursula](https://ursula.tonbo.io) streams, for Node.js 22.19 or later.

This is the 0.8 development line.

```ts
import { open } from "@tonbo/sqlite-ursula";

const db = await open("https://streams.example.com/acme/notes");
db.exec("CREATE TABLE IF NOT EXISTS notes(id INTEGER PRIMARY KEY, body TEXT)");
db.prepare("INSERT INTO notes(body) VALUES (?)").run("hello"); // committed on this host
await db.synced(); // in the stream, and this process still owns it
res.end("saved");
```

**The one rule:** before anything outside the process acts on what you wrote or read (a response, a queue acknowledgement, a call to another service), `await db.synced()`.

A commit is local. The extension appends it to a log it owns and replicates the log to the stream in the background. `synced()` resolves once the stream holds every commit made before the call, and a round trip that started after the call found this process still the owner. It never blocks the event loop, and it keeps the process alive while it waits. Scripts await it before they exit.

| the developer | what happens |
| --- | --- |
| runs a statement or COMMIT | local work: SQLite, plus one write to the log |
| awaits `synced()` | one append round trip while writing, one leader read when idle |
| loses the stream | commits continue, `synced()` waits, past `maxPendingBytes` writes fail with `backlog_full` |
| is taken over | statements and `synced()` throw `UrsulaResetError` (`fenced`), `on("reset")` listeners run |
| crashes | commits the stream never acknowledged come back from the log at the next open and are resent |

## open

`open(url, { dir, token, signal, maxPendingBytes, recreate, ...sqlite })` returns a promise of an `UrsulaDatabase`, a `node:sqlite` `DatabaseSync` subclass, so drizzle, kysely and Pi's driver take it. The rest of the options go to `DatabaseSync`.

- The first open of a stream in the process binds its cache in `dir` (by default `<user cache>/sqlite-ursula/0.8`), catches it up and claims the stream: the last open that reaches the stream owns it. Later opens in the process return another connection on the same binding, with no network. Concurrent opens share one. Worker threads call `open` too.
- The open gives up when `signal` fires, after 30 s without one.
- Several connections share one binding and its files, so node:sqlite's busy `timeout` defaults to 5000 ms. Pass `timeout` to change it.
- A local cache the open cannot keep is moved aside when it holds data. That happens after a history change of the stream that lost commits the cache acknowledged (a restore to an earlier point), after any history change while the cache holds commits it has recorded no acknowledgement for (made offline, or acknowledged just before a crash), when its db file is not its own, and with `recreate: true` when the stream is missing. A cache that holds no data is replaced with nothing moved aside. A moved cache goes into `salvage/` in the stream's cache directory as a plain SQLite database with a `manifest.json`, `status().salvage` lists it, and the open builds a new cache from the stream. Without `recreate`, a missing stream over a cache that holds data fails with `not_found`.
- `db.close()` is synchronous. Once a binding's last connection closes, replication drains in the background and the binding retires. Open it again with `open()`.

`UrsulaDatabase` adds `name` (the stream's path, such as `/acme/notes`), `synced({ signal })`, `status()` and `on("reset", listener)` / `off("reset", listener)`.

## Errors

Every error the package throws for a failure of the extension has a `kind`, an `action` (`retry`, `reopen`, `stop` or `fix`) and a `cause`. Branch on `action`. Network failures never throw from statements or `synced()`.

| class | thrown by | kinds |
| --- | --- | --- |
| `UrsulaWriteRefusedError` | a write or COMMIT, nothing committed | `backlog_full` (retry after `synced()`), `unsupported` (fix) |
| `UrsulaResetError` | statements after a reset, `synced()`, `reset` listeners | `fenced`, `stale`, `rewound`, `deleted`, `local_damage`, `stream_state`, `internal` |
| `UrsulaUnavailableError` | `open()` when its signal fired first | the last condition (`unreachable`, `unavailable`, `rate_limited`, `unauthorized`, `payment_required`, `forbidden`), or `cancelled` when it met none yet |
| `UrsulaSetupError` | `open()`, and `synced()` or `status()` of a database that retired | `invalid_url`, `invalid_name`, `invalid_argument`, `not_found`, `in_use`, `bound_elsewhere`, `local_setup`, `stream_format`, `server_too_old`, `wrong_cluster`, `stream_state`, `claim_races`, `not_bound`, `internal` |

After a reset, pending `synced()` calls reject first, then the `reset` listeners run once. Every statement of the database throws the reset, reads included, except reads in a read transaction it had open, which keep its snapshot. A write in it still fails. `open()` the stream again, or stop when the action is `stop`. The new open need not wait for the old database to close: it works on a copy of the local cache, which carries every commit the old one made. Close the old database once you no longer need it.

## Pi Durable

`@tonbo/sqlite-ursula/pi` runs Pi Durable's official node `SqliteStorage` on a replicated database:

```ts
import { openUrsulaPiStorage } from "@tonbo/sqlite-ursula/pi";

const storage = await openUrsulaPiStorage("https://streams.example.com/acme/agent", { closeTimeoutMs: 10_000 });
```

Every transaction and statement resolves only after `synced()`, so a Pi commit resolves once the stream holds it. `backlog_full` throws Pi's `StorageRejected`, so the session continues. A reset throws the `UrsulaResetError` itself, so the session stops. `close` waits up to `closeTimeoutMs` for `synced()`. `@earendil-works/pi-durable` is an optional peer dependency, from 1.0.0 up to but not including 1.2.0.

## SQL functions

Other hosts call the extension's SQL functions on the connection `loadUrsulaVfs()` returns: `ursula_open`, `ursula_synced`, `ursula_events`, `ursula_cancel`, `ursula_status`, `ursula_stats` and `ursula_set_token`. A host reopens through `ursula_open` after closing: once a database retired, a plain `new DatabaseSync(path)` of its path is refused until `ursula_open` binds it again.

A cache's files are not SQLite files outside the extension. The db file holds a marker in place of SQLite's header magic, and its WAL is `db-uwal`. The `sqlite3` shell, node:sqlite without the extension and a connection opened before the extension loaded fail with `SQLITE_NOTADB` (`file is not a database`) and change nothing. To read a database with plain SQLite, export it from a connection `open()` returned, with `VACUUM INTO`.

0.8 servers and streams do not read 0.7 data, and 0.8 does not read 0.7 caches.

## Platforms

The extension comes prebuilt: npm installs the build for your platform, `@tonbo/sqlite-ursula-<platform>`, as an optional dependency. It is prebuilt for:

- Linux x64 and arm64 with glibc 2.28 or later, such as Debian 11, Ubuntu 20.04, Amazon Linux 2023 and RHEL 8.
- Linux x64 with musl, such as Alpine.
- macOS 11 or later on Apple silicon.

On any other platform, build the extension yourself and set `SQLITE_URSULA_VFS`. The extension refuses to load into SQLite older than 3.50.4.

## SQLITE_URSULA_VFS

When `SQLITE_URSULA_VFS` is set, the package loads the library at that path instead of the prebuilt one. Passing a path to `loadUrsulaVfs(path)` before any other call does the same. To build the extension from source (Rust):

```bash
git clone https://github.com/tonbo-io/ursula
cd ursula/clients/sqlite-vfs
cargo build --release
export SQLITE_URSULA_VFS=$PWD/target/release/libsqlite_ursula_vfs.so   # .dylib on macOS
```

## License

Apache-2.0

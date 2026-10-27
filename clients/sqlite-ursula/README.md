# @tonbo/sqlite-ursula

SQLite databases replicated through [Ursula](https://ursula.tonbo.io) streams, for Node.js 22.19 or later.

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

`open(url, { dir, token, signal, maxPendingBytes, recreate, readOnly, ...sqlite })` returns a promise of an `UrsulaDatabase`, a `node:sqlite` `DatabaseSync` subclass, so drizzle, kysely and Pi's driver take it. The rest of the options go to `DatabaseSync`.

- The first open of a stream in the process binds its cache in `dir` (by default `<user cache>/sqlite-ursula/0.8`), catches it up and claims the stream: the last open that reaches the stream owns it. Later opens in the process return another connection on the same binding, with no network. Concurrent opens share one. Worker threads call `open` too.
- `token` is the bearer token requests carry, or a provider that hands out tokens: see [Tokens](#tokens).
- The open gives up when `signal` fires, after 30 s without one.
- Several connections share one binding and its files, so node:sqlite's busy `timeout` defaults to 5000 ms. Pass `timeout` to change it.
- A local cache the open cannot keep is moved aside when it holds data. That happens after a history change of the stream that lost commits the cache acknowledged (a restore to an earlier point), after any history change while the cache holds commits it has recorded no acknowledgement for (made offline, or acknowledged just before a crash), when its db file is not its own, and with `recreate: true` when the stream is missing. A cache that holds no data is replaced with nothing moved aside. A moved cache goes into `salvage/` in the stream's cache directory as a plain SQLite database with a `manifest.json`, `status().salvage` lists it, and the open builds a new cache from the stream. Without `recreate`, a missing stream over a cache that holds data fails with `not_found`.
- `db.close()` is synchronous. Once a binding's last connection closes, replication drains in the background and the binding retires. Open it again with `open()`.
- `readOnly: true` claims nothing and never fences the owner, and writes fail with `SQLITE_READONLY` (8). In a process that owns the stream it returns a read-only connection on the owner's binding, which sees the owner's latest commits, acknowledged or not, so its `synced()` waits as the owner's does. In any other process it builds or catches up a reader cache of its own (`role: "reader"`, `reader-<n>/` in the stream's cache directory, one per reader process) to the stream's tail at the open, and reads the stream as of then: it does not follow later commits, and its `synced()` resolves at once. A closed stream opens too. A reader cache built on another deployment fails with `wrong_cluster`, as an owner's cache does, and an empty one builds. Close every read-only connection of the process and open again to read newer commits. When the stream is unreachable it opens an earlier reader cache as it is, and otherwise waits until the signal fires. A missing stream fails with `not_found`. `readOnly` with `recreate` fails with `invalid_argument`.

`UrsulaDatabase` adds `name` (the stream's path, such as `/acme/notes`), `synced({ signal })`, `status()` and `on("reset", listener)` / `off("reset", listener)`.

## Tokens

`token` is a string, or a provider: an async function that returns `{ token, expiresAt }`, where `expiresAt` is a `Date` or milliseconds since the epoch. The package then keeps the token fresh in the background, with no separate refresher process.

```ts
import { open, UrsulaUnavailableError } from "@tonbo/sqlite-ursula";

const db = await open("https://streams.example.com/acme/notes", {
  token: async ({ signal }) => {
    const r = await fetch(exchangeUrl, { method: "POST", headers: { authorization: `Bearer ${apiKey}` }, signal });
    if (r.status === 401) throw new UrsulaUnavailableError("unauthorized: the exchange refused the key", "unauthorized");
    if (!r.ok) throw new Error(`the exchange answered ${r.status}`);
    const { access_token, expires_in } = await r.json();
    return { token: access_token, expiresAt: Date.now() + expires_in * 1000 };
  },
});
```

- The package calls the provider before the open's first request, again after 70 to 80% of each token's lifetime, and again at once when the extension reports a request refused with `401`. Concurrent calls share one, and a later open with the same provider reuses its token. `signal` fires when the package stops waiting for the answer, after 30 s.
- A provider that throws an `UrsulaError` whose `action` is `fix` has failed for good: the open rejects with that error, and the package stops calling the provider until the next `open()` with one. Every other error is retried with backoff, from 1 s up to 30 s, and an unexpired token keeps serving meanwhile.
- An open whose `signal` fires before the provider handed out a token rejects with `UrsulaUnavailableError` `unauthorized`, whose action is `retry` and whose `cause` is the provider's last error. A token that is not printable ASCII without spaces, or an `expiresAt` that is not a time, fails the open with `UrsulaSetupError` `invalid_argument`.
- After the open, a provider that failed for good leaves its last token in place. Once the server refuses it, `status().replication.condition` is `unauthorized`, and commits continue until the backlog fills.
- A request refused twice blocks replication as `unauthorized`, and the extension sends it again every 30 s, so a token the provider hands out after a `401` applies within 30 s.
- The token is one per process. The last open that names a token sets it: a later open with another provider takes over the databases this one served, and an open with a string stops it. A function lives on one JavaScript thread, so each thread runs its own provider: give the threads of a process the same kind of token.
- The provider's timers never keep the process alive. It stops once no database opened with a provider on its thread is bound.
- Plain string tokens and `URSULA_VFS_TOKEN_FILE` work as before. No token is logged.

## Gating outputs

`gate(db, work, { signal })` runs `work` and settles with its outcome only once a `synced()` call made after `work` settled resolved. It applies the one rule to whatever acts on the outcome: a fetch-style handler's `Response`, a job processor's promise that a queue framework acknowledges, a message you acknowledge yourself.

```ts
import { gate, open } from "@tonbo/sqlite-ursula";

const db = await open("https://streams.example.com/acme/notes");

// A fetch-style handler: the status, the headers and every body chunk wait.
const handler = (request: Request) => gate(db, () => router.fetch(request), { signal: request.signal });

// A queue you acknowledge yourself.
for await (const message of subscription) {
  await gate(db, () => applyMessage(db, message)); // records message.id in the same transaction
  await message.ack(); // a crash before this redelivers, and applyMessage skips an id it has seen
}
```

- `db` is one database or an array of them. With several, the gate waits for all of them, in parallel.
- Reads wait too: a result may have read another call's pending commit, or state this process is about to lose to a takeover.
- `work`'s errors wait as its results do, except an `UrsulaError`, which is thrown at once, also when a library wrapped it (as a `cause`, or in an `AggregateError`): a refused write committed nothing, and a reset is what the wait would report. A reset during the wait rejects with the `UrsulaResetError`, and the outcome is dropped.
- When `work` returns a `Response` with a body, the gate resolves with one of the same status and headers whose body passes each chunk, and its end, on only once a `synced()` call made after the chunk was read resolved. One call covers every chunk read while the previous call was in flight, so a stream pays one round trip per batch, not per chunk, and the gate reads ahead at most 64 KiB plus one chunk. A reset errors the body.
- `signal` bounds the wait for the outcome, which for a `Response` is its status and headers: the gate rejects with `signal.reason` and cancels a returned body, and the commits keep replicating. It does not cut short a body that has started. Cancel the body to stop it.
- A `node:http` response is not gated by itself: write it after the gate, as in `res.end(await gate(db, () => save(req)))`.

What the gate does not cover:

- Effects inside `work` before it returns, such as an outbound call or a manual acknowledgement in the middle of a handler. `await db.synced()` before them.
- WebSocket frames, raw sockets, timers, background jobs and child processes.
- Exactly-once effects. A crash after the release but before an acknowledgement runs the work again, so record message ids and idempotency keys in the same transaction as the work.
- Anything beyond what `synced()` itself means: the stream holds the commit, with the durability the server gives it.

## Import

`importSqlite(url, file, { token, timeoutMs })` imports a plain SQLite database file into a new stream, as the stream's first snapshot, and returns `{ offset, digest, pages, bytes }`. Then `open(url)` it as any other stream.

- `token` is a string, for the import alone. The import blocks its thread, so no provider could refresh a token meanwhile: pass `(await provider({ signal })).token`.
- The file must have 4096-byte pages, with no `-wal` beside it and no hot journal. `VACUUM INTO` writes such a copy (after `PRAGMA page_size = 4096` when the database has other pages). The file is only read.
- The stream must be missing, or empty. A stream of another content type fails with `stream_format`, anything else with `stream_state`, and nothing is published. A server of another release line fails with `protocol_mismatch`, and an answer without `Ursula-Protocol` and `Ursula-Cluster-Id` with `no_server_identity`.
- While the import runs, opens of the stream fail with `stream_state`. An import that fails after it claimed the stream leaves a stream that refuses opens and imports with `stream_state` until it is deleted or recreated. `UrsulaUnavailableError` from an import says `fix` for that reason.
- A snapshot above the request limit goes up in parts, which needs a cold store on the server. Without one, a file whose compressed snapshot is above the limit fails with `invalid_argument`, naming the limit.
- The call blocks its thread until the import is done, for at most `timeoutMs` (10 minutes by default).

## Errors

Every error the package throws for a failure of the extension has a `kind`, an `action` (`retry`, `reopen`, `stop` or `fix`) and a `cause`. Branch on `action`. Network failures never throw from statements or `synced()`.

| class | thrown by | kinds |
| --- | --- | --- |
| `UrsulaWriteRefusedError` | a write or COMMIT, nothing committed | `backlog_full` (retry after `synced()`), `unsupported` (fix), `too_large` (fix: the commit's pages take more than `URSULA_VFS_MAX_COMMIT_BYTES`, 128 MiB by default, so split the transaction or raise the limit, at most 1 GiB) |
| `UrsulaResetError` | statements after a reset, `synced()`, `reset` listeners | `fenced`, `stale`, `rewound`, `deleted`, `local_damage`, `stream_state`, `internal` |
| `UrsulaUnavailableError` | `open()` when its signal fired first, also before a token provider handed out a token (`unauthorized`, action `retry`) | the last condition (`unreachable`, `unavailable`, `rate_limited`, `unauthorized`, `payment_required`, `forbidden`), or `cancelled` when it met none yet |
| `UrsulaSetupError` | `open()`, `importSqlite()`, and `synced()` or `status()` of a database that retired | `invalid_url`, `invalid_argument`, `not_found`, `in_use`, `bound_elsewhere`, `local_setup`, `stream_format`, `no_server_identity`, `protocol_mismatch`, `wrong_cluster`, `stream_state`, `claim_races`, `not_bound`, `internal` |

After a reset, pending `synced()` calls reject first, then the `reset` listeners run once. Every statement of the database throws the reset, reads included, except reads in a read transaction it had open, which keep its snapshot. A write in it still fails. `open()` the stream again, or stop when the action is `stop`. The new open need not wait for the old database to close: it works on a copy of the local cache, which carries every commit the old one made. Close the old database once you no longer need it.

## Pi Durable

`@tonbo/sqlite-ursula/pi` runs Pi Durable's official node `SqliteStorage` on a replicated database:

```ts
import { openUrsulaPiStorage } from "@tonbo/sqlite-ursula/pi";

const storage = await openUrsulaPiStorage("https://streams.example.com/acme/agent", { closeTimeoutMs: 10_000 });
```

Every transaction and statement resolves only after `synced()`, so a Pi commit resolves once the stream holds it. A commit refused with `backlog_full` commits nothing. From Pi 1.2 it waits for `synced()` and runs again, since Pi expects a storage to retry its own transient failures. On Pi 1.0 and 1.1 it throws Pi's `StorageRejected`, so the session continues. A reset throws the `UrsulaResetError` itself, so the session stops. `close` waits up to `closeTimeoutMs` for `synced()`. `@earendil-works/pi-durable` is an optional peer dependency, from 1.0.0 up to but not including 1.4.0.

## SQL functions

Other hosts call the extension's SQL functions on the connection `loadUrsulaVfs()` returns: `ursula_open`, `ursula_synced`, `ursula_events`, `ursula_cancel`, `ursula_status`, `ursula_stats`, `ursula_set_token` and `ursula_import(url, file, options_json, timeout_ms)`, whose `timeout_ms` must be positive. A host reopens through `ursula_open` after closing: once a database retired, a plain `new DatabaseSync(path)` of its path is refused until `ursula_open` binds it again.

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

# @tonbo/sqlite-ursula

SQLite databases replicated through [Ursula](https://ursula.tonbo.io) streams, for Node.js.

`open` needs `@tonbo/sqlite-ursula` 0.7.5 or later. With 0.7.4 and earlier, [attach the file yourself](#attach-a-file-yourself).

Start a single Ursula node in a terminal of its own. It keeps nothing across restarts.

```bash
docker run --rm -p 4437:4437 ghcr.io/tonbo-io/ursula:0.7.4
```

Install the package, which needs Node.js 22.19 or later, and run:

```bash
npm install @tonbo/sqlite-ursula
```

```ts
import { open } from "@tonbo/sqlite-ursula";

const db = open("http://127.0.0.1:4437/demo/notes");
db.exec("CREATE TABLE IF NOT EXISTS notes(id INTEGER PRIMARY KEY, body TEXT)");
db.prepare("INSERT INTO notes(body) VALUES (?)").run("hello"); // returns once the stream holds it
console.log(db.prepare("SELECT id, body FROM notes").all());
db.close();
```

The local copy outlives the node: it stays in your cache directory (see [Options and local files](#options-and-local-files)). After you restart the node, its stream is gone, and `open` refuses to put an empty stream over a local copy that holds data (`not_found`). Delete `demo-notes-*` in `~/Library/Caches/sqlite-ursula` (`~/.cache/sqlite-ursula` on Linux), or open another stream.

`open` loads the `sqlite-ursula-vfs` SQLite extension into `node:sqlite`, catches a local copy of the database up from the stream, claims the stream for this process and returns the database. Every commit is appended to the stream before it reaches the local file, so any host can rebuild the database from the stream: the same code on another host opens the same database. The database is a `node:sqlite` `DatabaseSync`, so libraries that take one take it, and your code keeps using SQLite as before. The package has no runtime dependencies. Its `@tonbo/sqlite-ursula/pi` entry opens Pi Durable's official `SqliteStorage` on a replicated file (see [Pi Durable](#pi-durable)).

## Errors

From 0.7.5, a failure of the extension throws an `UrsulaError`. Its `action` says what to do, its `kind` names what failed, and its `cause` is the error the failure surfaced as. Every other error, such as a constraint violation or a syntax error, is `node:sqlite`'s own, unchanged.

| `action` | Class | What it means for you |
| --- | --- | --- |
| `retry` | `UrsulaWriteRefusedError`, and `UrsulaPaymentRequiredError` for a `402` | Nothing reached the stream, and the database stays usable. Run it again later on the same database: the statement, or the whole transaction if it was one, since SQLite rolled the transaction back. When `open` itself failed, call `open` again later. |
| `reattach` | `UrsulaReattachError` | The local file may no longer match the stream, for example after a commit whose outcome is unknown. Close the database and call `open` again, which replays the stream. The database's own `close()` and `open()` methods attach nothing. |
| `stop` | `UrsulaFencedError` | Another owner took the stream over. Stop using the database, and never reopen it in a loop: every `open` takes the stream back from the other owner. Exit, but only where nothing starts this process on the stream again, see [Rules](#rules). |
| `fix` | `UrsulaSetupError` | The request cannot succeed as made, such as a stream URL the server refuses, or a database the process has open already. The message says what to change. |

```ts
import { UrsulaError } from "@tonbo/sqlite-ursula";

try {
  db.prepare("INSERT INTO notes(body) VALUES (?)").run("hello");
} catch (error) {
  if (!(error instanceof UrsulaError)) throw error; // SQLite's own error, such as a constraint violation
  if (error.action === "stop") process.exit(1); // another owner took over, see Rules for what may restart us
  throw error; // retry: try again later. reattach: close and open again. fix: change the setup.
}
```

Branch on `action`. The classes group the failures by what happened, and two of them can carry another action. A transaction too large for one append is an `UrsulaWriteRefusedError` whose `action` is `fix`: the same transaction never goes through, so split it. A commit that poisoned the file for a cause you must fix first, such as leaving WAL mode or meeting a redirect, is an `UrsulaReplicationError` whose `action` is `fix`. `UrsulaFencedError` and `UrsulaReattachError` are `UrsulaReplicationError`s too, whose `fenced` is `true` and `false`. When `cause` is SQLite's error, its `code`, `errcode` and `errstr` are copied to the `UrsulaError`, so code that reads them keeps working. The [design doc](https://github.com/tonbo-io/ursula/blob/main/docs/architecture/sqlite-vfs.md#errors) lists every kind.

`open` throws these errors, and so do the database's `exec`, `prepare` and `applyChangeset`, its statements' `run`, `get`, `all` and `iterate`, and `attach` and `status`. SQLite rolls a transaction back when the extension fails it, so the ROLLBACK that a transaction helper runs next (yours, or a library's such as drizzle's) does not replace the `UrsulaError` with "cannot rollback". Other methods throw `node:sqlite`'s errors as they are: a tag store, a session, `backup()`, and a second replicated file used through SQL `ATTACH DATABASE`. Read `status(db.location()).error` for what failed there. `status` of a path whose attach a `402` refused reports kind `refused`: only `attach` itself throws `UrsulaPaymentRequiredError`. Through `openUrsulaPiStorage` (see [Pi Durable](#pi-durable)) a commit the stream did not take rejects with `UrsulaFencedError` or `UrsulaReattachError`, which are `UrsulaReplicationError`s as before, and a refused commit with `UrsulaWriteRefusedError`.

## Rules

- **One owner per stream.** `open` claims the stream for the process, which fences every earlier owner, on this host or another. The fenced owner's next commit fails with `UrsulaFencedError`, and from then on so do its reads.
- **Take a database over by opening it, and keep the old owner from taking it back.** To move a database to another host or process, open it there. The old owner must then stop, as the `stop` action says, and nothing may start it on the stream again: a restarted old owner takes the stream back with its `open`. A scheduler that restarts an exited process in place does exactly that. Under Kubernetes, stop the old pod before the new one opens the stream (the `Recreate` strategy, or a StatefulSet with one replica), or keep the fenced process running and unready until it is terminated.
- **One database per stream in a process.** Open a stream once and share the database. Opening it again while the database is open fails with `in_use`, and opening it at another `path` takes the stream over from the first database. Close it first to open the stream again.
- **The local file is a cache.** One process owns a local file at a time (a lock file next to it enforces this on the host), and another process that opens the same file fails with `in_use` until the first exits. Never write the file outside the extension: such writes never reach the stream, and are not detected.
- **WAL mode, 4 KiB pages.** `open` leaves the file in WAL mode, so you set nothing. Do not change `journal_mode` or the page size, and do not use `locking_mode=EXCLUSIVE`.

[SQLite on Ursula](https://ursula.tonbo.io/docs/examples/sqlite-vfs) has the full rules, what the cluster needs, and what fencing means for your writes. Read it before you rely on the package.

## Options and local files

`open(streamUrl, options)` takes `node:sqlite`'s own options (`timeout`, `readBigInts` and the rest), which it checks before it attaches anything, and:

- `readOnly`: read the database without claiming its stream. It never writes the stream, so it does not fence the owner, and every write fails with `node:sqlite`'s own error. A read-only open keeps a file of its own (`-ro1`, `-ro2` and so on), so it does not hold up the owner on its host, or another reader.
- `path`: the local file to keep the database in, a plain file path. A relative one is made absolute when `open` runs. `:memory:`, `""` and `file:` URIs are refused (`invalid_argument`): SQLite would not open them as the attached file.
- `dir`: the directory of a file named after the stream URL, used when `path` is not given. `open` creates it when it is missing. By default it is `sqlite-ursula` in your cache directory: `$XDG_CACHE_HOME` when set, `~/Library/Caches` on macOS and `~/.cache` elsewhere.

The stream URL names the stream alone: one with a query or a fragment is refused (`invalid_url`). The file's name is `<bucket>-<stream>-<hash>.db`, where `<hash>` is the first 32 hex digits of the SHA-256 of the stream the URL names: its scheme, host, port and path, with the path's percent-encoding normalized, and a trailing `/` and userinfo left out. Every spelling of one stream URL shares a file, two different stream URLs never do, and the name is a safe file name for any bucket and stream id. Still spell a stream's name one way: the file records the spelling it was first attached with, and refuses another percent-encoding of the name as the cache of another stream (`other_stream`). Two URLs that reach one stream through different hosts, such as a gateway's and a node's, or `localhost` and `127.0.0.1`, map to two files. The label before the hash only makes the name readable: the last two segments of the URL's path, lowercased, with every run of characters other than `a-z`, `0-9`, `_` and `-` replaced by `_`, cut to 64 characters. [Local files](https://ursula.tonbo.io/docs/examples/sqlite-vfs#local-files) gives the whole rule.

The local file is a cache of the stream. After a reboot, or on another host, the database is rebuilt from the stream, so keep the directory on a local disk, and point `dir` at a volume for a container that should not rebuild after each restart. When a stream goes missing while its local file holds data, `open` refuses (`not_found`) and keeps the file, which may be the only copy left: do not open the stream under another URL, which would start an empty stream there, until you have copied the rows out of the file.

## Attach a file yourself

`open` is `attach` followed by `new DatabaseSync(path)`. You can make the two steps yourself, to open more than one connection to a database, or to pass the file to code that opens it. With 0.7.4 and earlier, which have no `open`, this is the way:

```ts
import { DatabaseSync } from "node:sqlite";
import { attach } from "@tonbo/sqlite-ursula";

attach("/data/app.db", "https://ursula.example.com/my-bucket/app-db"); // no connection to the file may be open yet
const db = new DatabaseSync("/data/app.db");
```

The extension is loaded once per process and becomes SQLite's default VFS there. Files that were never attached are not affected, unless an attach of them failed in the process: such a file opens only after an attach of it succeeds. An attach refused because of the file itself, such as a database that holds data but was never replicated, is not a failed attach. Attach refuses while the process has the file open outside the extension, read-only or not, such as through a connection opened before the extension was loaded. It checks the process's open files on Linux with `/proc` mounted and on macOS, and misses a file opened through another hard link or mount. Writes to an attached file outside the extension are not detected and never reach the stream, so never make them.

A connection you open yourself throws SQLite's errors as they are: a failed commit is a `disk I/O error`, and `status(file).error` says what failed (`kind`) and what to do (`action`). SQLite rolls the transaction back on such an error, so run a ROLLBACK after it only while `db.isTransaction` is `true`: otherwise its "cannot rollback" replaces the error. When a commit is refused before it reaches the stream, that commit fails alone, with `errcode` 7178, and `status(file)` names the refusal. Examples are a `402 Payment Required` from a quota or billing service in front of the stream, and any refusal marked `x-ursula-not-applied` that `ursula gateway` 0.7.3 or later vouches for with `x-ursula-not-applied-checked`, such as a server that refuses writes for a while under sustained load or a gateway whose authorizer denies the write. Without such a gateway, the mark counts for nothing, and only a `401`, `402` or `429` fails a commit alone. The file is not poisoned, reads keep working, and the next commit succeeds once the refusals stop. Through `open`'s database the refused commit throws `UrsulaWriteRefusedError` (`UrsulaPaymentRequiredError` for a `402`).

Turn off retries and redirect following for writes in any proxy or service mesh in front of Ursula. Istio before 1.24, for one, retries a `503` twice by default, POSTs included, and 1.24 to 1.26 does the same under `compatibilityVersion=1.23` (see [redirects and retries](https://ursula.tonbo.io/docs/api/overview#redirects-and-retries)). A retry can turn a write the stream applied into a refusal. The extension then usually poisons the file at the next commit, which needs a new attach. In the cases the documentation lists, no error follows: the refused write is in the stream, and the next commit may be acknowledged without reaching it.

`attach(file, url, { readOnly: true })` reads a database without claiming its stream. It never writes the stream, so it does not fence the owner, and every write to the file fails.

## Pi Durable

Install Pi Durable next to the package:

```bash
npm install @tonbo/sqlite-ursula @earendil-works/pi-durable
```

```ts
import { openUrsulaPiStorage } from "@tonbo/sqlite-ursula/pi";

const storage = await openUrsulaPiStorage("/data/harness-1.db", "https://ursula.example.com/pi/harness-1");
```

Hand `storage` to the Pi harness, and close it with `storage.close(ctx)` on shutdown. `openUrsulaPiStorage` attaches the file to the stream, which claims the stream for this process, and then opens the file with Pi's node driver. The error classes, such as `UrsulaReplicationError` and `UrsulaFencedError`, come from `@tonbo/sqlite-ursula`.

The package declares `@earendil-works/pi-durable` as an optional peer dependency in the range `>=1.0.0 <1.2.0`, and npm reports a mismatch when your version is outside it. On 1.0.0 and on 1.1.0, CI runs Pi's storage conformance suite through the extension, and the tests of how `openUrsulaPiStorage` reports fenced and refused commits.

## Versions

Within a minor release (0.7.x), the formats the extension writes to the stream and to the local files do not change. Any 0.7.x extension can attach a stream that another 0.7.x extension wrote, resume from the local files that extension left on the host, and fence the stream's owner, so you can upgrade owners one at a time. The API of the package and of the extension can still change in a patch release, as it did in 0.7.3. The package and its prebuilt extension share a version. If you load your own build with `SQLITE_URSULA_VFS`, upgrade it together with the package. A new minor release may also change the formats. Either way, the [release notes](https://ursula.tonbo.io/docs/release-notes) say how to upgrade. The [design doc](https://github.com/tonbo-io/ursula/blob/main/docs/architecture/sqlite-vfs.md#7-limits) lists the known limits.

## TLS and credentials

`https://` stream URLs trust the certificate authorities bundled with the extension, or only those in the PEM bundle `URSULA_VFS_CA_FILE` names. Behind `ursula gateway` with access control, set the bearer token with `setToken(token)` (refresh it before it expires), or name a file that holds it in `URSULA_VFS_TOKEN_FILE` (read again whenever it changes). See [TLS and credentials](https://ursula.tonbo.io/docs/examples/sqlite-vfs#tls-and-credentials).

## Platforms

The extension comes prebuilt: npm installs the build for your platform, `@tonbo/sqlite-ursula-<platform>`, as an optional dependency. It is prebuilt for:

- Linux x64 and arm64 with glibc 2.28 or later, such as Debian 11, Ubuntu 20.04, Amazon Linux 2023 and RHEL 8.
- Linux x64 with musl, such as Alpine.
- macOS 11 or later on Apple silicon.

On any other platform, build the extension yourself and set `SQLITE_URSULA_VFS`.

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

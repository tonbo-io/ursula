# @tonbo/sqlite-ursula

SQLite databases replicated through [Ursula](https://ursula.tonbo.io) streams, for Node.js.

The package loads the `sqlite-ursula-vfs` SQLite extension into `node:sqlite`. Once a file is attached to a stream, every commit is appended to the stream before it reaches the local file, and any host can rebuild the database from the stream. Your code keeps using SQLite as before. The package has no runtime dependencies. Its `@tonbo/sqlite-ursula/pi` entry opens Pi Durable's official `SqliteStorage` on a replicated file.

Within a minor release (0.7.x), the formats the extension writes to the stream and to the local files do not change. Any 0.7.x extension can attach a stream that another 0.7.x extension wrote, resume from the local files that extension left on the host, and fence the stream's owner, so you can upgrade owners one at a time. The API of the package and of the extension can still change in a patch release, as it did in 0.7.3. The package and its prebuilt extension share a version. If you load your own build with `SQLITE_URSULA_VFS`, upgrade it together with the package. A new minor release may also change the formats. Either way, the [release notes](https://ursula.tonbo.io/docs/release-notes) say how to upgrade. The [design doc](https://github.com/tonbo-io/ursula/blob/main/docs/architecture/sqlite-vfs.md#7-limits) lists the known limits.

## Install

```bash
npm install @tonbo/sqlite-ursula
```

It needs Node.js 22.19 or later. The extension comes prebuilt: npm installs the build for your platform, `@tonbo/sqlite-ursula-<platform>`, as an optional dependency.

## Use

Attach the file first and then open it with plain `node:sqlite` as usual:

```ts
import { DatabaseSync } from "node:sqlite";
import { attach } from "@tonbo/sqlite-ursula";

attach("/data/app.db", "https://ursula.example.com/my-bucket/app-db");
const db = new DatabaseSync("/data/app.db");
db.exec("PRAGMA journal_mode=WAL");
```

The extension is loaded once per process and becomes SQLite's default VFS there. Files that were never attached are not affected, unless an attach of them failed in the process: such a file opens only after an attach of it succeeds. An attach refused because of the file itself, such as a database that holds data but was never replicated, is not a failed attach. Attach refuses while the process has the file open outside the extension, read-only or not, such as through a connection opened before the extension was loaded. It checks the process's open files on Linux with `/proc` mounted and on macOS, and misses a file opened through another hard link or mount. Writes to an attached file outside the extension are not detected and never reach the stream, so never make them.

When a commit is refused before it reaches the stream, that commit fails alone. Examples are a `402 Payment Required` from a quota or billing service in front of the stream, and any refusal marked `x-ursula-not-applied` that `ursula gateway` 0.7.3 or later vouches for with `x-ursula-not-applied-checked`, such as a server that refuses writes for a while under sustained load or a gateway whose authorizer denies the write. Without such a gateway, the mark counts for nothing, and only a `401`, `402` or `429` fails a commit alone. The file is not poisoned, reads keep working, and the next commit succeeds once the refusals stop. With `node:sqlite` the commit throws SQLite's error with `errcode` 7178, and `status(file)` names the refusal. Through `openUrsulaPiStorage` the refused commit rejects with `UrsulaWriteRefusedError` (`UrsulaPaymentRequiredError` for a `402`).

Turn off retries and redirect following for writes in any proxy or service mesh in front of Ursula. Istio before 1.24, for one, retries a `503` twice by default, POSTs included, and 1.24 to 1.26 does the same under `compatibilityVersion=1.23` (see [redirects and retries](https://ursula.tonbo.io/docs/api/overview#redirects-and-retries)). A retry can turn a write the stream applied into a refusal. The extension then usually poisons the file at the next commit, which needs a new attach. In the cases the documentation lists, no error follows: the refused write is in the stream, and the next commit may be acknowledged without reaching it.

`attach(file, url, { readOnly: true })` reads a database without claiming its stream. It never writes the stream, so it does not fence the owner, and every write to the file fails.

Read [SQLite on Ursula](https://ursula.tonbo.io/docs/examples/sqlite-vfs) before you rely on it. It covers what the cluster needs, the rules (WAL mode only, one process per file), and what fencing means for your writes.

## Pi Durable

Install Pi Durable next to the package:

```bash
npm install @tonbo/sqlite-ursula @earendil-works/pi-durable
```

```ts
import { openUrsulaPiStorage } from "@tonbo/sqlite-ursula/pi";

const storage = await openUrsulaPiStorage("/data/harness-1.db", "https://ursula.example.com/pi/harness-1");
```

Hand `storage` to the Pi harness, and close it with `storage.close(ctx)` on shutdown. `openUrsulaPiStorage` attaches the file to the stream, which claims the stream for this process, and then opens the file with Pi's node driver. The error classes, such as `UrsulaReplicationError`, come from `@tonbo/sqlite-ursula`.

The package declares `@earendil-works/pi-durable` as an optional peer dependency in the range `>=1.0.0 <1.2.0`, and npm reports a mismatch when your version is outside it. On 1.0.0 and on 1.1.0, CI runs Pi's storage conformance suite through the extension, and the tests of how `openUrsulaPiStorage` reports fenced and refused commits.

## TLS and credentials

`https://` stream URLs trust the certificate authorities bundled with the extension, or only those in the PEM bundle `URSULA_VFS_CA_FILE` names. Behind `ursula gateway` with access control, set the bearer token with `setToken(token)` (refresh it before it expires), or name a file that holds it in `URSULA_VFS_TOKEN_FILE` (read again whenever it changes). See [TLS and credentials](https://ursula.tonbo.io/docs/examples/sqlite-vfs#tls-and-credentials).

## Platforms

The extension is prebuilt for:

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

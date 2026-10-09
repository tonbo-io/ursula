# @tonbo/sqlite-ursula

SQLite databases replicated through [Ursula](https://ursula.tonbo.io) streams, for Node.js.

Experimental: the extension speaks plain HTTP without credentials, and its formats and API may change in a minor release. What it still needs for production use is tracked in [#468](https://github.com/tonbo-io/ursula/issues/468).

The package loads the `sqlite-ursula-vfs` SQLite extension into `node:sqlite`. Once a file is attached to a stream, every commit is appended to the stream before it reaches the local file, and any host can rebuild the database from the stream. Your code keeps using SQLite as before. `openUrsulaPiStorage` opens Pi Durable's official `SqliteStorage` on a replicated file.

## Install

```bash
npm install @tonbo/sqlite-ursula
```

It needs Node.js 22.19 or later. The extension comes prebuilt: npm installs the build for your platform, `@tonbo/sqlite-ursula-<platform>`, as an optional dependency.

## Use

```ts
import { openUrsulaPiStorage } from "@tonbo/sqlite-ursula";

const storage = await openUrsulaPiStorage("/data/harness-1.db", "https://ursula.example.com/pi/harness-1");
```

Hand `storage` to the Pi harness, and close it with `storage.close(ctx)` on shutdown. `openUrsulaPiStorage` attaches the file to the stream, which claims the stream for this process, and then opens the file with Pi's node driver.

With plain `node:sqlite`, attach the file first and then open it as usual:

```ts
import { DatabaseSync } from "node:sqlite";
import { attach } from "@tonbo/sqlite-ursula";

attach("/data/app.db", "https://ursula.example.com/my-bucket/app-db");
const db = new DatabaseSync("/data/app.db");
db.exec("PRAGMA journal_mode=WAL");
```

The extension is loaded once per process and becomes SQLite's default VFS there. Files that were never attached are not affected.

Read [SQLite on Ursula](https://ursula.tonbo.io/docs/examples/sqlite-vfs) before you rely on it. It covers what the cluster needs, the rules (WAL mode only, one process per file), and what fencing means for your writes.

## TLS and credentials

`https://` stream URLs trust the certificate authorities bundled with the extension, or only those in the PEM bundle `URSULA_VFS_CA_FILE` names. Behind `ursula gateway` with access control, set the bearer token with `setToken(token)`, or name a file that holds it in `URSULA_VFS_TOKEN_FILE` (read again whenever it changes). See [TLS and credentials](https://ursula.tonbo.io/docs/examples/sqlite-vfs#tls-and-credentials).

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

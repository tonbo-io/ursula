# @tonbo/sqlite-ursula

SQLite databases replicated through [Ursula](https://ursula.tonbo.io) streams, for Node.js 22.19 or later.

This is the 0.8 development line. A commit is local: the extension appends it to a log it owns and replicates the log to the stream in the background. `ursula_synced` resolves once the stream holds every commit made before the call. The asynchronous `open`, `synced()` and reset listeners of the 0.8 TypeScript API are not in this package yet. Until they are, call the extension's SQL functions on the connection `loadUrsulaVfs()` returns:

```ts
import { DatabaseSync } from "node:sqlite";
import { loadUrsulaVfs } from "@tonbo/sqlite-ursula";

const control = loadUrsulaVfs();
const call = (sql: string, ...args: (string | number)[]) =>
	JSON.parse(Object.values(control.prepare(sql).get(...args) as object)[0] as string);

const { path } = call("SELECT ursula_open(?, NULL, '{}', 30000)", "https://streams.example.com/acme/notes");
const db = new DatabaseSync(path);
db.exec("CREATE TABLE IF NOT EXISTS notes(id INTEGER PRIMARY KEY, body TEXT)");
db.prepare("INSERT INTO notes(body) VALUES (?)").run("hello"); // committed on this host
call("SELECT ursula_synced(?, 10000)", path); // in the stream
```

After closing every connection, reopen through `ursula_open`: once its last connection closes, a database retires and its files unlock, and a plain `new DatabaseSync(path)` of the path is refused once it retired, until `ursula_open` binds it again.

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

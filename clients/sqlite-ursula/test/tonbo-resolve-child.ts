// A process of its own for test/tonbo.e2e.ts, run by Node from this source (type stripping):
// resolves the Tonbo database argv[2] with the extension's `tonbo_resolve`, blocking, under the
// environment its parent set (the origins and TONBO_API_KEY), opens the stream URL it returned in
// the cache root argv[3] with `ursula_open` and no token, commits, waits for `ursula_synced`, and
// prints the resolve's answer and whether it synced as one JSON line. No token was set in this
// process, so the extension's requests carry the Streams token the resolve fetched for the key.
import { DatabaseSync } from "node:sqlite";
import { loadUrsulaVfs } from "../src/index.ts";

const [name, dir] = process.argv.slice(2) as [string, string];
const control = loadUrsulaVfs();

/** One of the extension's SQL functions: its JSON answer. */
function call<T>(sql: string, ...args: string[]): T {
	const row = control.prepare(sql).get(...args) as Record<string, string>;
	return JSON.parse(Object.values(row)[0] as string) as T;
}

const resolved = call<{ done: boolean; url: string; name: string | null }>("SELECT tonbo_resolve(?, '{}', 20000)", name);
const opened = call<{ path: string }>("SELECT ursula_open(?, ?, '{}', 20000)", resolved.url, dir);
const db = new DatabaseSync(opened.path);
db.exec("CREATE TABLE t(x); INSERT INTO t VALUES (1)");
const synced = call<{ synced: boolean }>("SELECT ursula_synced(?, 20000)", opened.path);
db.close();
process.stdout.write(`${JSON.stringify({ resolved, synced: synced.synced })}\n`);

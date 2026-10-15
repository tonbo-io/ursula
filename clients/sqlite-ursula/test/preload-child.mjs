// A process for test/outsiders.e2e.ts: opens the db file argv[5] of a cache another process left
// with plain node:sqlite *before* it loads the extension (argv[2]), so that connection is outside
// it, then binds the stream at argv[3] in the cache root argv[4] with `ursula_open`, which reuses
// that db file, and opens it again as a bound connection. It prints one JSON line:
//   {"before", "bound", "after", "rows", "synced", "path"}
// `before` and `after` are the early connection's schema reads before and after the bind (each
// {"ok"} or {"ok": false, "error", "errcode"}), `rows` the bound connection's rows of t after it
// inserted one more, and `synced` the `ursula_synced` result.
import { DatabaseSync } from "node:sqlite";

const [ext, url, dir, early] = process.argv.slice(2);
const say = (m) => process.stdout.write(`${JSON.stringify(m)}\n`);
const read = (db) => {
	try {
		db.prepare("SELECT count(*) AS n FROM sqlite_schema").get();
		return { ok: true };
	} catch (error) {
		return { ok: false, error: String(error), errcode: error.errcode };
	}
};

const outside = new DatabaseSync(early);
const before = read(outside);
const control = new DatabaseSync(":memory:", { allowExtension: true });
control.loadExtension(ext);
const call = (sql, ...args) => JSON.parse(Object.values(control.prepare(sql).get(...args))[0]);
const opened = call("SELECT ursula_open(?, ?, '{}', 20000)", url, dir);
const db = new DatabaseSync(opened.path);
db.exec("INSERT INTO t VALUES (2)");
const after = read(outside);
const rows = db.prepare("SELECT x FROM t ORDER BY x").all();
const synced = call("SELECT ursula_synced(?, ?)", opened.path, 10_000);
outside.close();
db.close();
say({ before, after, rows, synced, path: opened.path });

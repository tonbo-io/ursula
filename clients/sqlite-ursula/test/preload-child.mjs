// A process for test/outsiders.e2e.ts: opens the db file argv[5] of a cache another process left
// with plain node:sqlite *before* it loads the extension (argv[2]), so that connection is outside
// it, then binds the stream at argv[3] in the cache root argv[4] with `ursula_open`, which reuses
// that db file, and opens it again as a bound connection. It prints one JSON line:
//   {"before", "after", "rows", "synced", "path"}
// `before` is the early connection's schema read before the bind and `after` the statements it
// runs after the bind, the bound connection having inserted one more row (each {"sql", "ok"} or
// {"sql", "ok": false, "error", "errcode"}), `rows` the bound connection's rows of t then, and
// `synced` the `ursula_synced` result.
import { DatabaseSync } from "node:sqlite";

const [ext, url, dir, early] = process.argv.slice(2);
const say = (m) => process.stdout.write(`${JSON.stringify(m)}\n`);
const run = (db, sql) => {
	try {
		db.prepare(sql).all();
		return { sql, ok: true };
	} catch (error) {
		return { sql, ok: false, error: String(error), errcode: error.errcode };
	}
};
const STATEMENTS = ["SELECT count(*) FROM sqlite_schema", "INSERT INTO t VALUES (3)", "PRAGMA journal_mode=DELETE", "VACUUM"];

const outside = new DatabaseSync(early);
const before = run(outside, STATEMENTS[0]);
const control = new DatabaseSync(":memory:", { allowExtension: true });
control.loadExtension(ext);
const call = (sql, ...args) => JSON.parse(Object.values(control.prepare(sql).get(...args))[0]);
const opened = call("SELECT ursula_open(?, ?, '{}', 20000)", url, dir);
const db = new DatabaseSync(opened.path);
db.exec("INSERT INTO t VALUES (2)");
const after = STATEMENTS.map((sql) => run(outside, sql));
const rows = db.prepare("SELECT x FROM t ORDER BY x").all();
const synced = call("SELECT ursula_synced(?, ?)", opened.path, 10_000);
outside.close();
db.close();
say({ before, after, rows, synced, path: opened.path });

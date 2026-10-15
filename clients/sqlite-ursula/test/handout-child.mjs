// A second process for test/handout.e2e.ts: loads the extension (argv[2]), binds the stream at
// argv[3] in the cache root argv[4] with `ursula_open`, opens the database with plain node:sqlite,
// prints `{"ok": true, "opened": <ursula_open's result>}` (or `{"ok": false, ...}`), then runs one
// JSON command per stdin line and prints one JSON line for each:
//   {"exec": sql} | {"all": sql} -> {"ok": true[, "rows"]} or {"ok": false, "error", "errcode"}
//   {"status": true} | {"synced": ms} -> the function's JSON, for the database bound last
//   {"close": true} closes the database
//   {"bind": dir[, "timeout": ms]} calls `ursula_open` for the stream in the cache root `dir`
//     with `timeout_ms` `ms` (20000 by default), opening nothing -> {"ok": true, "opened"}
//   {"connect": true} opens the database `bind` bound last
//   {"wait": path} waits, at most 20 s, until the file `path` exists -> {"ok": true, "found"}
//   {"exit": true} exits.
import { existsSync } from "node:fs";
import { createInterface } from "node:readline";
import { DatabaseSync } from "node:sqlite";

const [ext, url, dir] = process.argv.slice(2);
const say = (m) => process.stdout.write(`${JSON.stringify(m)}\n`);
const control = new DatabaseSync(":memory:", { allowExtension: true });
control.loadExtension(ext);
const call = (sql, ...args) => JSON.parse(Object.values(control.prepare(sql).get(...args))[0]);
const failure = (error) => ({ ok: false, error: String(error), errcode: error.errcode });
const bind = (cacheDir, timeout = 20000) => call("SELECT ursula_open(?, ?, '{}', ?)", url, cacheDir, timeout);
const sleeper = new Int32Array(new SharedArrayBuffer(4));

let opened;
try {
	opened = bind(dir);
} catch (error) {
	say(failure(error));
	process.exit(1);
}
let db = new DatabaseSync(opened.path);
say({ ok: true, opened });

for await (const line of createInterface({ input: process.stdin })) {
	const cmd = JSON.parse(line);
	try {
		if (cmd.exec !== undefined) {
			db.exec(cmd.exec);
			say({ ok: true });
		} else if (cmd.all !== undefined) {
			say({ ok: true, rows: db.prepare(cmd.all).all() });
		} else if (cmd.status) {
			say({ ok: true, status: call("SELECT ursula_status(?)", opened.path) });
		} else if (cmd.synced !== undefined) {
			say({ ok: true, synced: call("SELECT ursula_synced(?, ?)", opened.path, cmd.synced) });
		} else if (cmd.close) {
			db.close();
			say({ ok: true });
		} else if (cmd.bind !== undefined) {
			opened = bind(cmd.bind, cmd.timeout);
			say({ ok: true, opened });
		} else if (cmd.connect) {
			db = new DatabaseSync(opened.path);
			say({ ok: true });
		} else if (cmd.wait !== undefined) {
			const until = Date.now() + 20_000;
			while (!existsSync(cmd.wait) && Date.now() < until) Atomics.wait(sleeper, 0, 0, 10);
			say({ ok: true, found: existsSync(cmd.wait) });
		} else if (cmd.exit) {
			say({ ok: true });
			process.exit(0);
		}
	} catch (error) {
		say(failure(error));
	}
}

// A second process for test/core.e2e.ts: loads the extension (argv[2]), binds the stream at argv[3]
// in the cache root argv[4] with `ursula_open`, opens the database with plain node:sqlite, prints
// `{"opened": <ursula_open's result>}` (or `{"error": ...}`), then runs one JSON command per stdin
// line and prints one JSON line for each:
//   {"exec": sql} | {"all": sql} -> {"ok": true[, "rows"]} or {"ok": false, "error", "errcode"}
//   {"status": true} | {"synced": ms} | {"events": true} -> the function's JSON
//   {"close": true} closes the database, {"exit": true} exits.
//   {"outside": vfs, "sql": [statements]} opens the database's file as `file:<path>?vfs=<vfs>`,
//   outside the extension, and runs each statement -> {"ok": true, "uri", "results"}, `results`
//   holding {"sql", "ok"[, "error", "errcode"]} per statement, and `uri` saying whether this Node
//   honours URI file names (a probe file opened as one).
//   {"reopen": true} calls `ursula_open` again, with every connection kept, and opens a third
//   connection on what it bound -> {"ok": true, "opened"}.
// `"conn": 2` runs `exec` or `all` on a second connection to the database, `"conn": 3` on the
// reopened one, and `status` and `synced` with `"conn": 3` ask about the reopened database.
import { existsSync } from "node:fs";
import { join } from "node:path";
import { createInterface } from "node:readline";
import { DatabaseSync } from "node:sqlite";

const [ext, url, dir] = process.argv.slice(2);
const say = (m) => process.stdout.write(`${JSON.stringify(m)}\n`);
const control = new DatabaseSync(":memory:", { allowExtension: true });
control.loadExtension(ext);
const call = (sql, ...args) => JSON.parse(Object.values(control.prepare(sql).get(...args))[0]);
const failure = (error) => ({ ok: false, error: String(error), errcode: error.errcode });

let opened;
try {
	opened = call("SELECT ursula_open(?, ?, '{}', 20000)", url, dir);
} catch (error) {
	say(failure(error));
	process.exit(1);
}
const db = new DatabaseSync(opened.path);
let second;
let reopened;
let third;
const conn = (cmd) => (cmd.conn === 3 ? third : cmd.conn === 2 ? (second ??= new DatabaseSync(opened.path)) : db);
const pathOf = (cmd) => (cmd.conn === 3 ? reopened.path : opened.path);
say({ ok: true, opened });

let cursor = 0;
for await (const line of createInterface({ input: process.stdin })) {
	const cmd = JSON.parse(line);
	try {
		if (cmd.exec !== undefined) {
			conn(cmd).exec(cmd.exec);
			say({ ok: true });
		} else if (cmd.all !== undefined) {
			say({ ok: true, rows: conn(cmd).prepare(cmd.all).all() });
		} else if (cmd.status) {
			say({ ok: true, status: call("SELECT ursula_status(?)", pathOf(cmd)) });
		} else if (cmd.synced !== undefined) {
			say({ ok: true, synced: call("SELECT ursula_synced(?, ?)", pathOf(cmd), cmd.synced) });
		} else if (cmd.reopen) {
			reopened = call("SELECT ursula_open(?, ?, '{}', 20000)", url, dir);
			third = new DatabaseSync(reopened.path);
			say({ ok: true, opened: reopened });
		} else if (cmd.events) {
			const got = call("SELECT ursula_events(?, 0)", cursor);
			cursor = got.cursor;
			say({ ok: true, events: got.events });
		} else if (cmd.outside !== undefined) {
			// A URI names the probe in the cache root: honoured, the probe file exists after.
			const probe = join(dir, `uri-probe-${process.pid}.db`);
			try {
				new DatabaseSync(`file:${probe}?vfs=${cmd.outside}`).close();
			} catch {}
			const uri = existsSync(probe);
			const other = new DatabaseSync(`file:${opened.path}?vfs=${cmd.outside}`);
			const results = cmd.sql.map((sql) => {
				try {
					other.prepare(sql).all();
					return { sql, ok: true };
				} catch (error) {
					return { sql, ...failure(error) };
				}
			});
			other.close();
			say({ ok: true, uri, results });
		} else if (cmd.close) {
			third?.close();
			second?.close();
			db.close();
			say({ ok: true });
		} else if (cmd.exit) {
			say({ ok: true });
			process.exit(0);
		}
	} catch (error) {
		say(failure(error));
	}
}

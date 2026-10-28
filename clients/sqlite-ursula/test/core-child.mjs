// A second process for test/core.e2e.ts: loads the extension (argv[2]), binds the stream at argv[3]
// in the cache root argv[4] with `ursula_open`, opens the database with plain node:sqlite, prints
// `{"opened": <ursula_open's result>}` (or `{"error": ...}`), then runs one JSON command per stdin
// line and prints one JSON line for each:
//   {"exec": sql} | {"all": sql} -> {"ok": true[, "rows"]} or {"ok": false, "error", "errcode",
//   "classified"}, `classified` being what `ursula_classify` makes of the failure on the database
//   the command ran on (`null` for SQLite's own failures)
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
//   {"writer": ms} starts a worker thread that commits one row to table `w` (which the caller
//   created) on a connection of its own every `ms` milliseconds, busy_timeout 0, until the process
//   exits -> {"ok": true}. {"progress": true} -> {"ok": true, "committed", "errors", "error"}: the
//   writer's commits that returned, the ones that failed and the last failure.
// argv[5], when given, is `ursula_open`'s options JSON (`{}` otherwise): with `read_only` the
// connections open read-only, as the package's `open` opens them, except `"conn": 4`, a
// connection that asks for read-write whatever the options say.
import { existsSync } from "node:fs";
import { join } from "node:path";
import { createInterface } from "node:readline";
import { DatabaseSync } from "node:sqlite";
import { Worker } from "node:worker_threads";

const [ext, url, dir, options = "{}"] = process.argv.slice(2);
const readOnly = JSON.parse(options).read_only === true;
const say = (m) => process.stdout.write(`${JSON.stringify(m)}\n`);
const control = new DatabaseSync(":memory:", { allowExtension: true });
control.loadExtension(ext);
const call = (sql, ...args) => JSON.parse(Object.values(control.prepare(sql).get(...args))[0]);
const failure = (error) => ({ ok: false, error: String(error), errcode: error.errcode });
/** What `ursula_classify` makes of `error`, a statement's on the database at `path` or a SQL function's. */
const classified = (error, path) => call("SELECT ursula_classify(?, ?, ?)", String(error?.message ?? error), error?.errcode ?? null, path ?? null);

let opened;
try {
	opened = call("SELECT ursula_open(?, ?, ?, 20000)", url, dir, options);
} catch (error) {
	say(failure(error));
	process.exit(1);
}
const db = new DatabaseSync(opened.path, { readOnly });
let second;
let reopened;
let third;
let fourth;
const conn = (cmd) =>
	cmd.conn === 4
		? (fourth ??= new DatabaseSync(opened.path))
		: cmd.conn === 3
			? third
			: cmd.conn === 2
				? (second ??= new DatabaseSync(opened.path, { readOnly }))
				: db;
const pathOf = (cmd) => (cmd.conn === 3 ? reopened.path : opened.path);
say({ ok: true, opened });

/** The writer thread's counters: commits that returned, commits that failed. */
const progress = new Int32Array(new SharedArrayBuffer(8));
let writerError = null;
// Runs on a thread of its own, so it may block in the VFS while this thread answers.
const WRITER = `
const { workerData, parentPort } = require("node:worker_threads");
const { DatabaseSync } = require("node:sqlite");
const { path, every, progress } = workerData;
const counts = new Int32Array(progress);
const pause = new Int32Array(new SharedArrayBuffer(4));
const db = new DatabaseSync(path);
db.exec("PRAGMA busy_timeout=0");
const insert = db.prepare("INSERT INTO w(n) VALUES (?)");
for (;;) {
	try {
		insert.run(Atomics.load(counts, 0) + 1);
		Atomics.add(counts, 0, 1);
	} catch (error) {
		Atomics.add(counts, 1, 1);
		parentPort.postMessage(String(error));
	}
	Atomics.wait(pause, 0, 0, every);
}
`;

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
			reopened = call("SELECT ursula_open(?, ?, ?, 20000)", url, dir, options);
			third = new DatabaseSync(reopened.path, { readOnly });
			say({ ok: true, opened: reopened });
		} else if (cmd.writer !== undefined) {
			const worker = new Worker(WRITER, { eval: true, workerData: { path: opened.path, every: cmd.writer, progress: progress.buffer } });
			worker.on("message", (m) => {
				writerError = m;
			});
			worker.unref();
			say({ ok: true });
		} else if (cmd.progress) {
			say({ ok: true, committed: Atomics.load(progress, 0), errors: Atomics.load(progress, 1), error: writerError });
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
			fourth?.close();
			third?.close();
			second?.close();
			db.close();
			say({ ok: true });
		} else if (cmd.exit) {
			say({ ok: true });
			process.exit(0);
		}
	} catch (error) {
		// A failure to classify is reported, so the test sees it instead of a child that died.
		let named;
		try {
			named = classified(error, pathOf(cmd));
		} catch (failed) {
			named = { error: String(failed) };
		}
		say({ ...failure(error), classified: named });
	}
}

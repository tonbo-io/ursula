// Child process for the crash, fencing and snapshot tests: loads the extension, attaches FILE to URL
// (in the mode CHILD_ATTACH_MODE names, when set), opens it with plain node:sqlite in WAL mode and runs
// each SQL argument as one exec (`@sleep:<ms>` sleeps instead, `@wait:<path>` waits until the path
// exists, `@query:<sql>` prints the rows, `@status` prints `ursula_status`), printing a JSON line per
// step. Then stays alive until killed, unless CHILD_EXIT=1.
import { existsSync } from "node:fs";
import { DatabaseSync } from "node:sqlite";

const [ext, file, url, ...sqls] = process.argv.slice(2);
const say = (m) => process.stdout.write(`${JSON.stringify(m)}\n`);
const control = new DatabaseSync(":memory:", { allowExtension: true });
control.loadExtension(ext);
const mode = process.env.CHILD_ATTACH_MODE;
const tail = (mode === undefined ? control.prepare("SELECT ursula_attach(?, ?) AS n").get(file, url) : control.prepare("SELECT ursula_attach(?, ?, ?) AS n").get(file, url, mode)).n;
const status = () => JSON.parse(control.prepare("SELECT ursula_status(?) AS s").get(file).s);
const db = new DatabaseSync(file);
db.exec("PRAGMA journal_mode=WAL");
say({ attached: tail });
for (const [step, sql] of sqls.entries()) {
	say({ step, phase: "start" });
	if (sql.startsWith("@sleep:")) {
		Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, Number(sql.slice(7)));
		say({ step, ok: true });
		continue;
	}
	if (sql.startsWith("@wait:")) {
		while (!existsSync(sql.slice(6))) Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 50);
		say({ step, ok: true });
		continue;
	}
	if (sql.startsWith("@query:")) {
		say({ step, ok: true, rows: db.prepare(sql.slice(7)).all() });
		continue;
	}
	if (sql === "@status") {
		say({ step, ok: true, status: status() });
		continue;
	}
	try {
		db.exec(sql);
		say({ step, ok: true });
	} catch (error) {
		say({ step, ok: false, error: String(error), errcode: error.errcode, errstr: error.errstr });
	}
}
const stats = JSON.parse(control.prepare("SELECT ursula_stats(?) AS s").get(file).s);
const last = status();
const { commits, append_retries, attach_ms, log_bytes, snapshot_due_bytes, snapshot_age_ms, snapshot_failures, snapshot_error } = last;
say({
	done: true,
	attempts: stats.commits.map((c) => c.attempts),
	poisoned: last.poisoned,
	fenced: last.fenced,
	reason: last.reason,
	offset: last.offset,
	epoch: last.epoch,
	payment_required: last.payment_required,
	payment_reason: last.payment_reason,
	read_only: last.read_only,
	snapshots: stats.snapshots.length,
	snapshot: last.snapshot,
	retained: last.retained,
	installed: last.installed,
	local: last.local,
	health: { commits, append_retries, attach_ms, log_bytes, snapshot_due_bytes, snapshot_age_ms, snapshot_failures, snapshot_error },
});
if (process.env.CHILD_EXIT === "1") {
	db.close();
	process.exit(0);
}
setInterval(() => {}, 1000);

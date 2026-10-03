// Child process for the crash, fencing and snapshot tests: loads the extension, attaches FILE to URL,
// opens it with plain node:sqlite in WAL mode and runs each SQL argument as one exec (`@sleep:<ms>`
// sleeps instead), printing a JSON line per step. Then stays alive until killed, unless CHILD_EXIT=1.
import { DatabaseSync } from "node:sqlite";

const [ext, file, url, ...sqls] = process.argv.slice(2);
const say = (m) => process.stdout.write(`${JSON.stringify(m)}\n`);
const control = new DatabaseSync(":memory:", { allowExtension: true });
control.loadExtension(ext);
const tail = control.prepare("SELECT ursula_attach(?, ?) AS n").get(file, url).n;
const db = new DatabaseSync(file);
db.exec("PRAGMA journal_mode=WAL");
say({ attached: Number(tail) });
for (const [step, sql] of sqls.entries()) {
	say({ step, phase: "start" });
	if (sql.startsWith("@sleep:")) {
		Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, Number(sql.slice(7)));
		say({ step, ok: true });
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
const status = JSON.parse(control.prepare("SELECT ursula_status(?) AS s").get(file).s);
say({ done: true, attempts: stats.commits.map((c) => c.attempts), poisoned: status.poisoned, offset: status.offset, epoch: status.epoch, snapshots: stats.snapshots.length, snapshot: status.snapshot, retained: status.retained });
if (process.env.CHILD_EXIT === "1") {
	db.close();
	process.exit(0);
}
setInterval(() => {}, 1000);

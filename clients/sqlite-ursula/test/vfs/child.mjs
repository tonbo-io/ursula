// Child process for the crash and fencing tests: loads the extension, attaches FILE to URL, opens it
// with plain node:sqlite in WAL mode and runs each SQL argument as one exec, printing a JSON line per
// step. Then stays alive until killed, unless CHILD_EXIT=1.
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
	try {
		db.exec(sql);
		say({ step, ok: true });
	} catch (error) {
		say({ step, ok: false, error: String(error), errcode: error.errcode, errstr: error.errstr });
	}
}
say({ done: true });
if (process.env.CHILD_EXIT === "1") {
	db.close();
	process.exit(0);
}
setInterval(() => {}, 1000);

// The process test/crash.e2e.ts kills: loads the extension (argv[2]), binds the stream at argv[3] in
// the cache root argv[4] with `ursula_open`, then commits rows 1 to argv[5] of `t(i, pad)`, one per
// transaction, with `PRAGMA wal_autocheckpoint` at argv[6] pages so checkpoints (F1) and new WAL
// generations (F2) come every few commits. Before the first row it creates the table and waits for
// `ursula_synced`. After each COMMIT returns it writes `{"committed": i}`, synchronously, so the line
// is out before the next commit starts: the last line is the last commit the application saw
// succeed. URSULA_VFS_ABORT_AT, set by the test, aborts the process at its step.
import { writeSync } from "node:fs";
import { DatabaseSync } from "node:sqlite";

const [ext, url, dir, rowsArg, autocheckpoint] = process.argv.slice(2);
const say = (m) => writeSync(1, `${JSON.stringify(m)}\n`);
const control = new DatabaseSync(":memory:", { allowExtension: true });
control.loadExtension(ext);
const call = (sql, ...args) => JSON.parse(Object.values(control.prepare(sql).get(...args))[0]);

const opened = call("SELECT ursula_open(?, ?, '{}', 20000)", url, dir);
const db = new DatabaseSync(opened.path);
db.exec(`PRAGMA wal_autocheckpoint=${Number(autocheckpoint)}`);
db.exec("CREATE TABLE IF NOT EXISTS t(i INTEGER PRIMARY KEY, pad BLOB)");
const ready = call("SELECT ursula_synced(?, 20000)", opened.path);
say({ ready: ready.synced });
const insert = db.prepare("INSERT INTO t VALUES (?, randomblob(3000))");
for (let i = 1; i <= Number(rowsArg); i++) {
	insert.run(i);
	say({ committed: i });
}
say({ done: true });
db.close();

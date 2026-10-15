// The process test/crash.e2e.ts kills: loads the extension (argv[2]), binds the stream at argv[3]
// in the cache root argv[4] with `ursula_open` (the table exists already: a setup process created
// it), prints `{"opened": path}`, then commits transactions 1 to argv[5]. Transaction i inserts
// ROWS_PER_COMMIT rows of 3000 random bytes, each on a page of its own, and sets the counter row to
// i. After each COMMIT returns it writes `{"committed": i}`, synchronously, so the line is out before
// anything else runs: the last line is the last commit the application saw succeed.
//
// argv[6] picks how the WAL is checkpointed:
// - `auto:<pages>`: `PRAGMA wal_autocheckpoint=<pages>`, so checkpoints (F1) run after a commit
//   every few commits and the next transaction starts a new WAL generation (F2).
// - `truncate:<n>`: no autocheckpoint, and `PRAGMA wal_checkpoint(TRUNCATE)` after every n-th
//   commit, which checkpoints (F1) and truncates the WAL to nothing (F2).
// URSULA_VFS_ABORT_AT, set by the test, kills the process at its step.
import { writeSync } from "node:fs";
import { DatabaseSync } from "node:sqlite";

const ROWS_PER_COMMIT = 3;

const [ext, url, dir, commitsArg, checkpoints] = process.argv.slice(2);
const [mode, every] = checkpoints.split(":");
const say = (m) => writeSync(1, `${JSON.stringify(m)}\n`);
const control = new DatabaseSync(":memory:", { allowExtension: true });
control.loadExtension(ext);
const call = (sql, ...args) => JSON.parse(Object.values(control.prepare(sql).get(...args))[0]);

const opened = call("SELECT ursula_open(?, ?, '{}', 20000)", url, dir);
const db = new DatabaseSync(opened.path);
db.exec(`PRAGMA wal_autocheckpoint=${mode === "auto" ? Number(every) : 0}`);
say({ opened: opened.path });
const insert = db.prepare("INSERT INTO t(i, pad) VALUES (?, randomblob(3000))");
const count = db.prepare("UPDATE counter SET n = ?");
for (let i = 1; i <= Number(commitsArg); i++) {
	db.exec("BEGIN");
	for (let r = 0; r < ROWS_PER_COMMIT; r++) insert.run(i);
	count.run(i);
	db.exec("COMMIT");
	say({ committed: i });
	if (mode === "truncate" && i % Number(every) === 0) db.exec("PRAGMA wal_checkpoint(TRUNCATE)");
}
say({ done: true });
db.close();

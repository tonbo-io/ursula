// Smoke test of the packed packages (.github/workflows/npm-publish.yml), run in a scratch project
// where `npm install ./*.tgz` installed @tonbo/sqlite-ursula and this platform's extension package:
// with SQLITE_URSULA_VFS unset, loadUrsulaVfs() loads the extension from that package, the extension
// registers its SQL functions, and a plain database works through its VFS. No server is needed.
import assert from "node:assert/strict";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import { loadUrsulaVfs } from "@tonbo/sqlite-ursula";

delete process.env.SQLITE_URSULA_VFS;
const control = loadUrsulaVfs();
const functions = control
	.prepare("SELECT DISTINCT name FROM pragma_function_list WHERE name LIKE 'ursula!_%' ESCAPE '!' ORDER BY name")
	.all()
	.map((row) => row.name);
assert.deepEqual(functions, ["ursula_attach", "ursula_stats", "ursula_status"]);

// The extension made its VFS SQLite's default: an unattached database passes through it unchanged.
const db = new DatabaseSync(join(mkdtempSync(join(tmpdir(), "smoke-")), "plain.db"));
db.exec("PRAGMA journal_mode=WAL; CREATE TABLE t(x); INSERT INTO t VALUES (42)");
assert.equal(db.prepare("SELECT x FROM t").get().x, 42);
db.close();

console.log(`ok: ${process.platform}-${process.arch}, Node ${process.version}, ${functions.join(", ")}`);

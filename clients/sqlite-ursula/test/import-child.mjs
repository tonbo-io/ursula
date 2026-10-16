// A process of its own for test/import.e2e.ts, so the environment it gets (such as
// URSULA_VFS_MAX_REQUEST_BYTES) is read by an extension loaded for it alone: loads the extension
// (argv[2]), calls `ursula_import(url, file, '{}', 120000)` with the stream URL argv[3] and the file
// argv[4], prints `{"ok": true, "imported": <its result>}` or `{"ok": false, "error", "errcode"}`,
// and exits.
import { DatabaseSync } from "node:sqlite";

const [ext, url, file] = process.argv.slice(2);
const say = (m) => process.stdout.write(`${JSON.stringify(m)}\n`);
const control = new DatabaseSync(":memory:", { allowExtension: true });
control.loadExtension(ext);
try {
	const text = Object.values(control.prepare("SELECT ursula_import(?, ?, '{}', 120000)").get(url, file))[0];
	say({ ok: true, imported: JSON.parse(text) });
} catch (error) {
	say({ ok: false, error: String(error), errcode: error.errcode });
}

// A process of its own for test/host.e2e.ts, run by Node from this source (type stripping), so the
// extension it loads reads the environment it was started with (URSULA_VFS_HOST_DIR): opens the
// stream at argv[3] in the cache root argv[4] with the package's `open` (argv[2] is the extension,
// which `loadUrsulaVfs` takes from SQLITE_URSULA_VFS), commits once, awaits `synced()`, closes the
// database and prints `{"ok": true}`, or prints `{"ok": false}` with the error's class, kind,
// action and message. Then it exits.
import { writeSync } from "node:fs";
import { loadUrsulaVfs, open, type UrsulaError } from "../src/index.ts";

const [url, dir] = process.argv.slice(3) as [string, string];
const say = (m: Record<string, unknown>): void => {
	writeSync(1, `${JSON.stringify(m)}\n`);
};
loadUrsulaVfs();
try {
	const db = await open(url, { dir });
	db.exec("CREATE TABLE IF NOT EXISTS t(x); INSERT INTO t VALUES (1)");
	await db.synced();
	db.close();
	say({ ok: true });
} catch (error) {
	const e = error as UrsulaError;
	say({ ok: false, name: e.name, kind: e.kind, action: e.action, message: e.message });
}
process.exit(0);

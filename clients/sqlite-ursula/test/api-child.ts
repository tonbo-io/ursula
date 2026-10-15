// A process of its own for test/synced.e2e.ts, run by Node from this source (type stripping): opens
// the stream at argv[2] in the cache root argv[3] with the package's `open`, commits, then:
// - `gate`: holds the next append's answer back for 2 s at the model (argv[4] is its URL), awaits
//   `db.synced()` and prints `synced`;
// - `idle`: awaits `db.synced()` and prints `idle`.
// It never closes the database or exits by itself: the process ends when nothing keeps it alive.
import { loadUrsulaVfs, open } from "../src/index.ts";

const [url, dir, mode, model] = process.argv.slice(2) as [string, string, string, string];
loadUrsulaVfs();
const db = await open(url, { dir });
db.exec("CREATE TABLE IF NOT EXISTS t(x)");
if (mode === "gate") {
	await db.synced();
	await fetch(`${model}/__model/faults?times=1&method=POST`, { method: "POST", body: "delay 2000" });
	db.exec("INSERT INTO t VALUES (1)");
	const started = Date.now();
	await db.synced();
	process.stdout.write(`synced ${Date.now() - started}\n`);
} else {
	await db.synced();
	process.stdout.write("idle\n");
}

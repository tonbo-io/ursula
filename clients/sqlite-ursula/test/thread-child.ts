// A worker thread for test/workers.e2e.ts, run by Node from this source (type stripping), so it has
// its own instance of the package and its own pump. workerData: `{ ext, url, dir, value }`. It opens
// the stream, commits `value`, awaits `synced()` and posts `{ opened: { location, rows } }`. Then it
// answers the parent's messages:
// - `"reset"`: awaits the next reset, then posts `{ reset: { listener, synced, statement } }`, the
//   kinds its listener, a `synced()` and a statement got;
// - `"close"`: closes the database and posts `{ closed: true }`, then nothing keeps the thread alive.
import { parentPort, workerData } from "node:worker_threads";
import { loadUrsulaVfs, open, type UrsulaResetError } from "../src/index.ts";

const { ext, url, dir, value } = workerData as { ext: string; url: string; dir: string; value: number };
const port = parentPort;
if (port === null) throw new Error("thread-child runs as a worker thread");
loadUrsulaVfs(ext);
const db = await open(url, { dir });
db.exec("CREATE TABLE IF NOT EXISTS t(x)");
db.prepare("INSERT INTO t VALUES (?)").run(value);
await db.synced();
const heard = new Promise<UrsulaResetError>((resolve) => db.on("reset", resolve));
port.postMessage({ opened: { location: db.location(), rows: db.prepare("SELECT x FROM t ORDER BY x").all() } });

const kindOf = (error: unknown): string => String((error as { kind?: unknown }).kind);
port.on("message", async (message: string) => {
	if (message === "reset") {
		const synced = await db.synced().then(
			() => "resolved",
			(error: unknown) => kindOf(error),
		);
		const listener = kindOf(await heard);
		let statement = "none";
		try {
			db.prepare("SELECT x FROM t").all();
		} catch (error) {
			statement = kindOf(error);
		}
		port.postMessage({ reset: { listener, synced, statement } });
	} else if (message === "close") {
		db.close();
		port.postMessage({ closed: true });
		port.close();
	}
});

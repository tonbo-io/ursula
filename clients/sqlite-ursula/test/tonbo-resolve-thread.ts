// A worker thread for test/tonbo.e2e.ts, run by Node from this source (type stripping): it calls the
// extension's `tonbo_resolve` blocking, on a control connection of its own in the test's process, so
// the call joins the resolves the test's thread started, while that thread serves the stand-in.
// workerData: `{ ext, nameOrUrl, options, timeoutMs }`. It posts `"calling"` as it makes the call,
// then `{ answer }` or `{ error }` (the extension's message), after which nothing keeps it alive.
import { parentPort, workerData } from "node:worker_threads";
import { loadUrsulaVfs } from "../src/index.ts";

const { ext, nameOrUrl, options, timeoutMs } = workerData as { ext: string; nameOrUrl: string; options: string; timeoutMs: number | bigint };
const port = parentPort;
if (port === null) throw new Error("tonbo-resolve-thread runs as a worker thread");
const control = loadUrsulaVfs(ext);
const resolve = control.prepare("SELECT tonbo_resolve(?, ?, ?)");
port.postMessage("calling");
try {
	const row = resolve.get(nameOrUrl, options, timeoutMs) as Record<string, string>;
	port.postMessage({ answer: JSON.parse(Object.values(row)[0] as string) as unknown });
} catch (error) {
	port.postMessage({ error: (error as Error).message });
}
port.close();

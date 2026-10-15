// The pump's worker thread (design §8.7): loads the extension the process already holds, so it shares
// its registry and driver, and loops on `SELECT ursula_events(cursor, 1000)`, which blocks this thread
// for at most a second, never the one that started it. It posts every batch of events to that thread
// (pump.ts) as `{ events }`, or `{ error }` once if the extension fails, then stops. It also stops
// within a second of its starting thread's exit.
//
// Standalone on purpose: Node runs it as its own module (from the built .js, or from this .ts with
// Node's type stripping under the tests), so it imports nothing of the package.
import { DatabaseSync } from "node:sqlite";
import { parentPort, workerData } from "node:worker_threads";

const { library, cursor: start, stop } = workerData as { library: string; cursor: number; stop: Int32Array };
const port = parentPort;
if (port === null) throw new Error("pump-worker runs as a worker thread");

const control = new DatabaseSync(":memory:", { allowExtension: true });
control.loadExtension(library);
const poll = control.prepare("SELECT ursula_events(?, 1000) AS got");

let cursor = start;
try {
	// The starting thread sets `stop` when it exits: Node's termination of a worker does not reliably
	// interrupt this loop, and the process would wait for it.
	while (Atomics.load(stop, 0) === 0) {
		const { got } = poll.get(cursor) as { got: string };
		const batch = JSON.parse(got) as { cursor: number; events: unknown[] };
		cursor = batch.cursor;
		if (batch.events.length > 0) port.postMessage({ events: batch.events });
	}
} catch (error) {
	port.postMessage({ error: error instanceof Error ? error.message : String(error) });
}

// `db.synced()` against the model server (E2E_SERVER=model): it resolves once the stream holds every
// commit made before the call, waits without blocking the event loop while the stream holds its
// answer back, gives up at its signal, and keeps the process alive while it waits. An idle process
// exits on its own.
import { spawn } from "node:child_process";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import { loadUrsulaVfs, open } from "../src/index.ts";
import { pump } from "../src/pump.ts";
import { ext, fault, frames, freshDir, onModel, rejection, stream, ursulaUrl } from "./kit.ts";

loadUrsulaVfs(ext());

/** Runs test/api-child.ts in `mode`: its output, exit code and how long it took to exit after its output. */
function child(mode: "gate" | "idle", url: string, dir: string): Promise<{ out: string; code: number | null; ms: number }> {
	let started = Date.now();
	const proc = spawn(process.execPath, ["--no-warnings", join(import.meta.dirname, "api-child.ts"), url, dir, mode, ursulaUrl()], { stdio: ["ignore", "pipe", "inherit"] });
	let out = "";
	proc.stdout.on("data", (chunk: Buffer) => {
		out += chunk.toString();
		// From its last line: how long the process took to end after its work was done.
		started = Date.now();
	});
	return new Promise((resolve) => proc.once("exit", (code) => resolve({ out, code, ms: Date.now() - started })));
}

describe.skipIf(!onModel())("synced()", () => {
	it("resolves once the stream holds every commit made before the call", async () => {
		const s = stream("synced");
		const db = await open(s.url, { dir: freshDir() });
		db.exec("CREATE TABLE t(x)");
		for (let i = 0; i < 5; i++) db.prepare("INSERT INTO t VALUES (?)").run(i);
		await db.synced();
		const { commits } = db.status();
		expect(commits.acknowledged).toBeGreaterThanOrEqual(commits.logged);
		expect(commits).toMatchObject({ pending: 0, pending_bytes: 0 });
		expect(await frames(s.path)).toEqual({ claims: 1, commits: 6 });
		// With nothing new to send it still makes a round trip, and resolves.
		await db.synced();
		db.close();
	});

	it("waits without blocking the event loop while the stream holds its answer back", async () => {
		const db = await open(stream("wait").url, { dir: freshDir() });
		db.exec("CREATE TABLE t(x)");
		await db.synced();
		await fault("delay 1000", 1, "POST");
		db.exec("INSERT INTO t VALUES (1)");
		let ticks = 0;
		const timer = setInterval(() => (ticks += 1), 10);
		const started = Date.now();
		let done = false;
		const waits = Promise.all([db.synced(), db.synced(), db.synced()]).then(() => (done = true));
		await new Promise((r) => setTimeout(r, 300));
		expect(done).toBe(false);
		await waits;
		clearInterval(timer);
		expect(Date.now() - started).toBeGreaterThanOrEqual(900);
		expect(ticks).toBeGreaterThan(30);
		db.close();
	});

	it("resolves thousands of concurrent calls through one watermark while the stream holds its answer back", async () => {
		const db = await open(stream("many").url, { dir: freshDir() });
		db.exec("CREATE TABLE t(x)");
		await db.synced();
		await fault("delay 1000", 1, "POST");
		db.exec("INSERT INTO t VALUES (1)");
		const waits: Promise<void>[] = [];
		for (let i = 0; i < 5_000; i++) {
			waits.push(db.synced());
			if (i % 500 === 0) db.exec(`INSERT INTO t VALUES (${i})`);
		}
		await Promise.all(waits);
		expect(db.status().commits).toMatchObject({ pending: 0 });
		db.close();
	});

	it("asks again for what a gap in the events may have dropped", async () => {
		const db = await open(stream("gap").url, { dir: freshDir() });
		db.exec("CREATE TABLE t(x)");
		await db.synced();
		await fault("delay 500", 1, "POST");
		db.exec("INSERT INTO t VALUES (1)");
		const waits = Array.from({ length: 10 }, () => db.synced());
		// As the worker reports one when events fell out of the queue before it read them.
		pump().handle([{ event: "gap", dropped: 1 }]);
		await Promise.all(waits);
		expect(db.status().commits).toMatchObject({ pending: 0 });
		db.close();
	});

	it("rejects with its signal's reason, while the commits keep replicating", async () => {
		const s = stream("signal");
		const db = await open(s.url, { dir: freshDir() });
		db.exec("CREATE TABLE t(x)");
		await db.synced();
		await fault("delay 1000", 1, "POST");
		db.exec("INSERT INTO t VALUES (1)");
		const error = await rejection(db.synced({ signal: AbortSignal.timeout(100) }));
		expect(error).toBeInstanceOf(DOMException);
		expect((error as DOMException).name).toBe("TimeoutError");
		const reason = new Error("gave up");
		expect(await rejection(db.synced({ signal: AbortSignal.abort(reason) }))).toBe(reason);
		await db.synced();
		expect(await frames(s.path)).toEqual({ claims: 1, commits: 2 });
		db.close();
	});

	it("keeps the process alive while it waits: `await db.synced()` never ends the process unresolved", async () => {
		const s = stream("alive");
		const r = await child("gate", s.url, freshDir());
		expect(r.code).toBe(0);
		const waited = /^synced (\d+)$/m.exec(r.out);
		expect(waited, r.out).not.toBeNull();
		expect(Number(waited?.[1])).toBeGreaterThanOrEqual(1500);
		expect(await frames(s.path)).toEqual({ claims: 1, commits: 2 });
	});

	it("lets an idle process exit on its own, its database left open", async () => {
		const r = await child("idle", stream("idle").url, freshDir());
		expect(r.out).toBe("idle\n");
		expect(r.code).toBe(0);
		expect(r.ms).toBeLessThan(2_000);
	});
});

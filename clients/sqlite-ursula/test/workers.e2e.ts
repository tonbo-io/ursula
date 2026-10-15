// Worker threads against the e2e server (test/stack.ts): every JavaScript thread calls `open`, and
// the opens of one stream in a process share its binding, whichever thread made them. Each thread
// runs its own pump: its `synced()` resolves and its listeners hear a reset. A worker thread whose
// database closed exits on its own.
import { spawn } from "node:child_process";
import { join } from "node:path";
import { createInterface } from "node:readline";
import { Worker } from "node:worker_threads";
import { describe, expect, it } from "vitest";
import { loadUrsulaVfs, open, UrsulaResetError } from "../src/index.ts";
import { ext, frames, freshDir, rejection, stream } from "./kit.ts";

loadUrsulaVfs(ext());

/** A worker thread running test/thread-child.ts on `url` in `dir`, which commits `value`. */
function thread(url: string, dir: string, value: number) {
	const worker = new Worker(join(import.meta.dirname, "thread-child.ts"), { workerData: { ext: ext(), url, dir, value }, execArgv: ["--no-warnings"] });
	const queue: Record<string, unknown>[] = [];
	const waiting: { resolve: (m: Record<string, unknown>) => void; reject: (e: unknown) => void }[] = [];
	let failure: unknown;
	worker.on("message", (m: Record<string, unknown>) => {
		const w = waiting.shift();
		if (w === undefined) queue.push(m);
		else w.resolve(m);
	});
	// A failure of the thread fails what waits on it at once.
	worker.on("error", (error) => {
		failure = error;
		for (const w of waiting.splice(0)) w.reject(error);
	});
	const exited = new Promise<number>((r) => worker.once("exit", r));
	const next = (): Promise<Record<string, unknown>> => {
		if (failure !== undefined) return Promise.reject(failure);
		const m = queue.shift();
		return m === undefined ? new Promise((resolve, reject) => waiting.push({ resolve, reject })) : Promise.resolve(m);
	};
	return {
		next,
		exited,
		send: (message: string) => worker.postMessage(message),
		terminate: () => worker.terminate(),
	};
}

describe("worker threads", () => {
	it("open the stream the main thread holds: one binding, one claim, synced() in each thread", async () => {
		const s = stream("threads");
		const dir = freshDir();
		const db = await open(s.url, { dir });
		db.exec("CREATE TABLE t(x)");
		db.exec("INSERT INTO t VALUES (0)");
		const a = thread(s.url, dir, 1);
		const b = thread(s.url, dir, 2);
		const [fromA, fromB] = [(await a.next()).opened, (await b.next()).opened] as { location: string; rows: unknown[] }[];
		expect(fromA?.location).toBe(db.location());
		expect(fromB?.location).toBe(db.location());
		await db.synced();
		expect(db.prepare("SELECT x FROM t ORDER BY x").all()).toEqual([{ x: 0 }, { x: 1 }, { x: 2 }]);
		expect(await frames(s.path)).toEqual({ claims: 1, commits: 4 });
		for (const t of [a, b]) {
			t.send("close");
			expect(await t.next()).toEqual({ closed: true });
			// Its pump is unref'd and its database closed: the thread ends by itself.
			expect(await t.exited).toBe(0);
		}
		db.close();
	});

	it("share one open when the threads open at once", async () => {
		const s = stream("together");
		const dir = freshDir();
		const threads = [thread(s.url, dir, 1), thread(s.url, dir, 2)];
		const db = await open(s.url, { dir });
		const opened = await Promise.all(threads.map(async (t) => (await t.next()).opened as { location: string }));
		for (const o of opened) expect(o.location).toBe(db.location());
		expect((await frames(s.path)).claims).toBe(1);
		for (const t of threads) {
			t.send("close");
			await t.next();
			await t.exited;
		}
		db.close();
	});

	it("hear a reset in every thread: synced() rejects, the listeners run, statements throw", async () => {
		const s = stream("reset");
		const dir = freshDir();
		const db = await open(s.url, { dir });
		const t = thread(s.url, dir, 1);
		await t.next();
		const heard = new Promise<UrsulaResetError>((resolve) => db.on("reset", resolve));
		// Another process takes the stream over (test/core-child.mjs).
		const taker = spawn(process.execPath, ["--no-warnings", join(import.meta.dirname, "core-child.mjs"), ext(), s.url, freshDir()], { stdio: ["pipe", "pipe", "inherit"] });
		const lines = createInterface({ input: taker.stdout as NodeJS.ReadableStream })[Symbol.asyncIterator]();
		expect(JSON.parse((await lines.next()).value as string)).toMatchObject({ ok: true });
		t.send("reset");
		expect(await t.next()).toEqual({ reset: { listener: "fenced", synced: "fenced", statement: "fenced" } });
		expect(await heard).toBeInstanceOf(UrsulaResetError);
		expect(await rejection(db.synced())).toMatchObject({ kind: "fenced" });
		t.send("close");
		await t.next();
		await t.exited;
		db.close();
		const exited = new Promise((r) => taker.once("exit", r));
		taker.stdin?.write(`${JSON.stringify({ exit: true })}\n`);
		await exited;
	});
});

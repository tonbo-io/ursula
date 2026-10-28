// Resets against the e2e server (test/stack.ts): another process takes the stream over, and the old
// owner's pending `synced()` calls reject with an UrsulaResetError (`fenced`, `stop`), then its
// `reset` listeners run once, and every statement of it throws the reset, reads included. A new
// `open()` takes the stream back.
import { type ChildProcess, spawn } from "node:child_process";
import { join } from "node:path";
import { createInterface } from "node:readline";
import { describe, expect, it } from "vitest";
import { loadUrsulaVfs, open, type UrsulaDatabase, UrsulaResetError } from "../src/index.ts";
import { ext, freshDir, rejection, stream, thrown } from "./kit.ts";

loadUrsulaVfs(ext());

/** Another process that opens `url` in `dir` (test/core-child.mjs), which takes the stream over. */
async function takeOver(url: string, dir: string): Promise<{ exec(sql: string): Promise<void>; stop(): Promise<void> }> {
	const proc: ChildProcess = spawn(process.execPath, ["--no-warnings", join(import.meta.dirname, "core-child.mjs"), ext(), url, dir], { stdio: ["pipe", "pipe", "inherit"] });
	const lines = createInterface({ input: proc.stdout as NodeJS.ReadableStream })[Symbol.asyncIterator]();
	const next = async (): Promise<Record<string, unknown>> => JSON.parse((await lines.next()).value as string) as Record<string, unknown>;
	expect(await next()).toMatchObject({ ok: true });
	return {
		async exec(sql) {
			proc.stdin?.write(`${JSON.stringify({ exec: sql })}\n`);
			expect(await next()).toMatchObject({ ok: true });
			proc.stdin?.write(`${JSON.stringify({ synced: 10_000 })}\n`);
			expect(await next()).toMatchObject({ ok: true });
		},
		async stop() {
			const exited = new Promise((r) => proc.once("exit", r));
			proc.stdin?.write(`${JSON.stringify({ exit: true })}\n`);
			await exited;
		},
	};
}

/** The first reset `db` reports to its listeners. */
const nextReset = (db: UrsulaDatabase): Promise<UrsulaResetError> => new Promise((resolve) => db.on("reset", resolve));

describe("resets", () => {
	it("a takeover rejects pending synced() calls, then runs the listeners once, and fails every statement", async () => {
		const s = stream("takeover");
		const dir = freshDir();
		const owner = await open(s.url, { dir });
		const second = await open(s.url, { dir });
		owner.exec("CREATE TABLE t(x)");
		owner.exec("INSERT INTO t VALUES (1)");
		await owner.synced();
		const order: string[] = [];
		const heard: UrsulaResetError[] = [];
		owner.on("reset", (error) => {
			order.push("listener");
			heard.push(error);
		});
		const removed = (): void => {
			order.push("removed");
		};
		owner.on("reset", removed).off("reset", removed);
		const secondHeard = nextReset(second);

		const taker = await takeOver(s.url, freshDir());
		const waiting = owner.synced().catch((error: unknown) => {
			order.push("synced");
			return error;
		});
		const error = await waiting;
		expect(error).toBeInstanceOf(UrsulaResetError);
		expect(error).toMatchObject({ kind: "fenced", action: "stop" });
		expect(await secondHeard).toMatchObject({ kind: "fenced" });
		expect(order).toEqual(["synced", "listener"]);
		expect(heard).toHaveLength(1);
		expect(heard[0]).toBeInstanceOf(UrsulaResetError);

		const read = thrown(() => owner.prepare("SELECT x FROM t").all());
		expect(read).toBeInstanceOf(UrsulaResetError);
		expect(read).toMatchObject({ kind: "fenced", action: "stop", errcode: 266 });
		// A write that starts its own transaction fails at its read (266), one inside a read
		// transaction opened before the reset at its write (778).
		const write = thrown(() => second.exec("INSERT INTO t VALUES (2)"));
		expect(write).toBeInstanceOf(UrsulaResetError);
		expect(write).toMatchObject({ kind: "fenced", errcode: 266 });
		expect(await rejection(owner.synced())).toMatchObject({ kind: "fenced" });
		expect(owner.status().reset).toMatchObject({ kind: "fenced", action: "stop", class: "reset" });

		await taker.exec("INSERT INTO t VALUES (3)");
		await taker.stop();
		owner.close();
		second.close();
		// A new open takes the stream back, and reads what the taker wrote.
		const back = await open(s.url, { dir });
		expect(back.prepare("SELECT x FROM t ORDER BY x").all()).toEqual([{ x: 1 }, { x: 3 }]);
		back.exec("INSERT INTO t VALUES (4)");
		await back.synced();
		expect(heard).toHaveLength(1);
		back.close();
	});

	it("runs the listeners when a commit's append finds the takeover, with no synced() pending", async () => {
		const s = stream("append");
		const owner = await open(s.url, { dir: freshDir() });
		owner.exec("CREATE TABLE t(x)");
		await owner.synced();
		const heard = nextReset(owner);
		const taker = await takeOver(s.url, freshDir());
		owner.exec("INSERT INTO t VALUES (1)");
		expect(await heard).toMatchObject({ kind: "fenced", action: "stop" });
		expect(thrown(() => owner.exec("INSERT INTO t VALUES (2)"))).toBeInstanceOf(UrsulaResetError);
		await taker.stop();
		owner.close();
	});

	it("refuses an event name it does not have", async () => {
		const db = await open(stream("events").url, { dir: freshDir() });
		expect(() => db.on("close" as "reset", () => {})).toThrow(TypeError);
		db.close();
	});
});

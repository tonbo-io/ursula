// Copies of a live cache (design: Clones, Claims, Pre-check, Reconcile), against the e2e server
// (test/stack.ts): a cache directory copied while its owner runs, as a host that clones a disk or
// restores a snapshot of it does, and opened by another process.
//
// - A copy opened while the stream answers takes it over, and the original resets `fenced` at its
//   next commit or `synced()`. Every row the original synced survives.
// - Two copies that open with one `activation` send one claim and share its epoch. The first
//   divergent commit fences exactly one of them, and the other goes on.
// - An offline copy whose original kept writing finds the original's commits in its pre-check and
//   resets `stale` with nothing sent.
// - An offline copy of an idle original holds the original's claim in its log, so the pre-check
//   finds nothing foreign: it claims the next epoch, goes live and fences the original.
// - A copy behind retention, opened while the stream answers, does not take the original's
//   snapshot as covering its own commits. It rebuilds from it into a new generation.
//
// In every case no commit id reaches the stream twice.
import { randomBytes } from "node:crypto";
import { cpSync, readdirSync } from "node:fs";
import { join } from "node:path";
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it } from "vitest";
import { loadUrsulaVfs, open } from "../src/index.ts";
import { Child, directUrl, ext, freshDir, fromStream, frames, gate, killChildren, retired, stream, streamBytes } from "./kit.ts";

loadUrsulaVfs(ext());

afterEach(killChildren);

const sleep = (ms: number): Promise<void> => new Promise((r) => setTimeout(r, ms));

interface Status {
	state: string;
	epoch: number;
	path: string;
	reset: { kind: string; action: string } | null;
	attach: { local: string; snapshot_offset: number | null };
	commits: { logged: number; acknowledged: number };
	snapshot: { offset: number | null };
}

/** A copy of the cache root `dir`, taken as it is, its owner running or not. */
function copyOf(dir: string): string {
	const copy = freshDir();
	cpSync(dir, copy, { recursive: true });
	return copy;
}

/** The generation directories of the one stream cached in `dir`. */
function generations(dir: string): string[] {
	const [root, ...others] = readdirSync(dir);
	expect(others).toEqual([]);
	return readdirSync(join(dir, root as string)).filter((name) => name.startsWith("gen-"));
}

async function status(c: Child): Promise<Status> {
	const reply = await c.send({ status: true });
	expect(reply.ok, JSON.stringify(reply)).toBe(true);
	return reply.status as Status;
}

/** Polls `c`'s status until it reset, for at most 30 s. */
async function resetOf(c: Child): Promise<Status> {
	const deadline = Date.now() + 30_000;
	for (;;) {
		const st = await status(c);
		if (st.reset !== null) return st;
		if (Date.now() > deadline) throw new Error(`it never reset: ${JSON.stringify(st)}`);
		await sleep(100);
	}
}

/** `c`'s `synced()` resolves. */
async function synced(c: Child): Promise<void> {
	expect(await c.send({ synced: 30_000 })).toMatchObject({ ok: true, synced: { synced: true } });
}

/** `c`'s `synced()` rejects with the reset `kind`. */
async function rejects(c: Child, kind: string): Promise<void> {
	const reply = await c.send({ synced: 30_000 });
	expect(reply.ok, JSON.stringify(reply)).toBe(false);
	expect(String(reply.error)).toContain(`ursula_synced: ${kind}: `);
}

/** The stream's retained offset and tail, from a HEAD. */
async function head(path: string): Promise<{ retained: number; tail: number }> {
	const r = await fetch(`${directUrl()}${path}`, { method: "HEAD" });
	expect(r.status).toBe(200);
	const tail = r.headers.get("stream-next-offset");
	expect(tail).not.toBeNull();
	return { retained: Number(r.headers.get("stream-retained-offset") ?? "0"), tail: Number(tail) };
}

/** No commit id `(lineage, position)` is in the stream at `path` twice, as far as it is retained. */
async function commitsOnce(path: string): Promise<void> {
	const { retained } = await head(path);
	const bytes = await streamBytes(path, retained === 0 ? "-1" : String(retained));
	const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
	const ids: string[] = [];
	for (let at = 0; at < bytes.length; at += 13 + view.getUint32(at + 5, true)) {
		expect(new TextDecoder().decode(bytes.subarray(at, at + 4))).toBe("USQ2");
		if (bytes[at + 4] !== 2) continue;
		const lineage = Buffer.from(bytes.subarray(at + 13, at + 29)).toString("hex");
		ids.push(`${lineage}:${view.getBigUint64(at + 29, true)}`);
	}
	expect(ids.length).toBeGreaterThan(0);
	expect(ids.filter((id, i) => ids.indexOf(id) !== i)).toEqual([]);
}

describe("copies of a live cache", () => {
	it("takes over from the original when opened by another process, and the original is fenced at its next commit", async () => {
		const s = stream("clone-live");
		const dir = freshDir();
		const original = new Child(s.url, dir);
		expect((await original.opened()).state).toBe("live");
		await original.exec("CREATE TABLE t(x); INSERT INTO t VALUES (1)");
		await original.exec("INSERT INTO t VALUES (2)");
		await synced(original);

		// Copied at once, with the original bound: its latest ACK may trail what it synced.
		const copy = new Child(s.url, copyOf(dir));
		expect((await copy.opened()).state).toBe("live");
		expect(await copy.rows("SELECT x FROM t ORDER BY x")).toEqual([{ x: 1 }, { x: 2 }]);
		expect((await status(copy)).epoch).toBeGreaterThan((await status(original)).epoch);

		await original.exec("INSERT INTO t VALUES (3)");
		expect((await resetOf(original)).reset).toEqual({ kind: "fenced", action: "stop" });
		await rejects(original, "fenced");
		await original.stop();

		await copy.exec("INSERT INTO t VALUES (4)");
		await synced(copy);
		expect(await status(copy)).toMatchObject({ state: "live", reset: null });
		await copy.stop();
		expect(await fromStream(s.url, "SELECT x FROM t ORDER BY x")).toEqual([{ x: 1 }, { x: 2 }, { x: 4 }]);
		await commitsOnce(s.path);
	});

	it("shares one epoch between two copies opened with one activation, and their first divergent commit fences exactly one", async () => {
		const s = stream("clone-activation");
		const dir = freshDir();
		const first = await open(s.url, { dir });
		const location = first.location() as string;
		try {
			first.exec("CREATE TABLE t(x); INSERT INTO t VALUES (1)");
			await first.synced();
		} finally {
			first.close();
		}
		await retired(location);
		const before = await frames(s.path);

		const options = JSON.stringify({ activation: randomBytes(16).toString("hex") });
		const a = new Child(s.url, copyOf(dir), {}, "core-child.mjs", [options]);
		expect((await a.opened()).state).toBe("live");
		const b = new Child(s.url, copyOf(dir), {}, "core-child.mjs", [options]);
		expect((await b.opened()).state).toBe("live");
		// Identical claims: the server kept one frame and answered the second as a repeat.
		const [sa, sb] = [await status(a), await status(b)];
		expect(sb.epoch).toBe(sa.epoch);
		expect(sa).toMatchObject({ state: "live", reset: null });
		expect(sb).toMatchObject({ state: "live", reset: null });
		expect(await frames(s.path)).toEqual({ claims: before.claims + 1, commits: before.commits });
		// Neither committed: both read and confirm the same state.
		await synced(a);
		await synced(b);
		expect(await a.rows("SELECT x FROM t ORDER BY x")).toEqual([{ x: 1 }]);
		expect(await b.rows("SELECT x FROM t ORDER BY x")).toEqual([{ x: 1 }]);

		// The first divergent commit: one record under one key with two bodies. The first to land wins.
		await a.exec("INSERT INTO t VALUES (2)");
		await synced(a);
		await b.exec("INSERT INTO t VALUES (3)");
		await rejects(b, "fenced");
		expect((await resetOf(b)).reset).toEqual({ kind: "fenced", action: "stop" });
		await b.stop();

		await a.exec("INSERT INTO t VALUES (4)");
		await synced(a);
		expect(await status(a)).toMatchObject({ state: "live", reset: null, epoch: sa.epoch });
		await a.stop();
		expect(await fromStream(s.url, "SELECT x FROM t ORDER BY x")).toEqual([{ x: 1 }, { x: 2 }, { x: 4 }]);
		await commitsOnce(s.path);
	});

	describe("offline", () => {
		let front: Awaited<ReturnType<typeof gate>>;
		beforeAll(async () => {
			front = await gate(directUrl());
		});
		afterAll(async () => {
			await front?.stop();
		});
		beforeEach(async () => {
			await mode("forward");
		});

		async function mode(m: "forward" | "refuse"): Promise<void> {
			const r = await fetch(`${front.control}/mode`, { method: "PUT", body: JSON.stringify({ mode: m }) });
			expect(r.status, await r.text()).toBe(204);
		}

		/** A process that opens the cache in `dir` through the refusing gate: provisional, having run `sql`. */
		async function offline(path: string, dir: string, sql: string): Promise<Child> {
			await mode("refuse");
			const child = new Child(`${front.url}${path}`, dir);
			expect((await child.opened()).state).toBe("provisional");
			await child.exec(sql);
			expect(await child.send({ synced: 500 })).toMatchObject({ ok: true, synced: { synced: false } });
			return child;
		}

		it("resets `stale` with nothing sent when its original kept writing", async () => {
			const s = stream("clone-stale");
			const dir = freshDir();
			// The original reaches the stream directly, never through the gate.
			const original = new Child(s.url, dir);
			await original.opened();
			await original.exec("CREATE TABLE t(x); INSERT INTO t VALUES (1)");
			await synced(original);

			const copy = await offline(s.path, copyOf(dir), "INSERT INTO t VALUES (9)");
			await original.exec("INSERT INTO t VALUES (2)");
			await original.exec("INSERT INTO t VALUES (3)");
			await synced(original);
			const before = await frames(s.path);

			await mode("forward");
			expect((await resetOf(copy)).reset).toEqual({ kind: "stale", action: "reopen" });
			await rejects(copy, "stale");
			await copy.stop();
			expect(await frames(s.path)).toEqual(before);

			await original.exec("INSERT INTO t VALUES (4)");
			await synced(original);
			expect(await status(original)).toMatchObject({ state: "live", reset: null });
			await original.stop();
			expect(await fromStream(s.url, "SELECT x FROM t ORDER BY x")).toEqual([{ x: 1 }, { x: 2 }, { x: 3 }, { x: 4 }]);
			await commitsOnce(s.path);
		});

		for (const wrote of [true, false]) {
			it(`goes live over an idle original${wrote ? "" : " that never wrote"} and fences it`, async () => {
				const s = stream(wrote ? "clone-idle" : "clone-idle-empty");
				const dir = freshDir();
				const original = new Child(s.url, dir);
				await original.opened();
				if (wrote) await original.exec("CREATE TABLE t(x); INSERT INTO t VALUES (1)");
				await synced(original);
				const epoch = (await status(original)).epoch;

				const copy = await offline(s.path, copyOf(dir), wrote ? "INSERT INTO t VALUES (2)" : "CREATE TABLE t(x); INSERT INTO t VALUES (2)");
				const before = await frames(s.path);
				await mode("forward");
				await synced(copy);
				// The pre-check found nothing foreign, the resent claim was a repeat, and exactly one
				// continuation of the original's claim landed.
				expect(await status(copy)).toMatchObject({ state: "live", reset: null, epoch: epoch + 1 });
				expect((await frames(s.path)).claims).toBe(before.claims + 1);

				// The original's output gate finds the copy's claim past its tail, with no commit of its own.
				await rejects(original, "fenced");
				expect((await resetOf(original)).reset).toEqual({ kind: "fenced", action: "stop" });
				expect(await original.send({ exec: "INSERT INTO t VALUES (3)" })).toMatchObject({ ok: false });
				await original.stop();

				await copy.exec("INSERT INTO t VALUES (4)");
				await synced(copy);
				expect(await status(copy)).toMatchObject({ state: "live", reset: null });
				await copy.stop();
				const expected = wrote ? [{ x: 1 }, { x: 2 }, { x: 4 }] : [{ x: 2 }, { x: 4 }];
				expect(await fromStream(s.url, "SELECT x FROM t ORDER BY x")).toEqual(expected);
				await commitsOnce(s.path);
			});
		}
	});

	it("rebuilds a copy that fell behind retention, never trusting the original's snapshot", async () => {
		const s = stream("clone-retention");
		const dir = freshDir();
		// Due after any acknowledged byte: one snapshot per commit transaction, retention one behind.
		const original = new Child(s.url, dir, { URSULA_VFS_SNAPSHOT_DUE_BYTES: "1" });
		await original.opened();
		await original.exec("CREATE TABLE t(x, pad); INSERT INTO t VALUES (1, randomblob(2000))");
		await synced(original);
		// The first snapshot fsyncs BASE and ACK before it publishes: copied after, the copy holds
		// an exact ACK and no checkpoint in progress.
		const deadline = Date.now() + 30_000;
		for (;;) {
			const st = await status(original);
			if (st.snapshot.offset !== null && st.commits.acknowledged === st.commits.logged) break;
			expect(Date.now(), `no snapshot: ${JSON.stringify(st)}`).toBeLessThan(deadline);
			await sleep(50);
		}
		const copied = copyOf(dir);
		const [generation, ...others] = generations(copied);
		expect(others).toEqual([]);
		const { tail } = await head(s.path);

		// The original writes on until retention passes everything the copy holds.
		const writes = Date.now() + 60_000;
		for (let x = 2; (await head(s.path)).retained <= tail; x++) {
			expect(Date.now(), "retention never passed the copy").toBeLessThan(writes);
			await original.exec(`INSERT INTO t VALUES (${x}, randomblob(2000))`);
			await synced(original);
			await sleep(300);
		}
		const rows = await original.rows("SELECT x, hex(pad) AS pad FROM t ORDER BY x");

		const copy = new Child(s.url, copied);
		const opened = await copy.opened();
		expect(opened.state).toBe("live");
		// Rebuilt into a new generation from the latest snapshot and the tail.
		expect(opened.path).not.toContain(generation as string);
		const attach = (await status(copy)).attach;
		expect(attach.local).toBe("built");
		expect(attach.snapshot_offset).toBeGreaterThan(tail);
		expect(await copy.rows("SELECT x, hex(pad) AS pad FROM t ORDER BY x")).toEqual(rows);

		await original.exec("INSERT INTO t VALUES (-1, NULL)");
		expect((await resetOf(original)).reset).toEqual({ kind: "fenced", action: "stop" });
		await original.stop();

		await copy.exec("INSERT INTO t VALUES (0, NULL)");
		await synced(copy);
		expect(await status(copy)).toMatchObject({ state: "live", reset: null });
		await copy.stop();
		const built = await fromStream(s.url, "SELECT x, hex(pad) AS pad FROM t ORDER BY x");
		expect(built).toEqual([{ x: 0, pad: "" }, ...rows]);
		expect(await fromStream(s.url, "PRAGMA integrity_check")).toEqual([{ integrity_check: "ok" }]);
		await commitsOnce(s.path);
	});
});

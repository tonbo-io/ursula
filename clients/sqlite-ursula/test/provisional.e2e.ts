// Provisional opens (design: Open): the stream is out of reach behind test/gate.mjs in `refuse`
// mode, so every connection to it fails at once. An open with a trusted local cache that claimed
// the stream before resolves at once, `provisional`: reads come from the cache, commits are local,
// and `synced()` waits with the condition `retrying`. Once the gate forwards again, a HEAD and a
// check of the stream from the cache's acknowledged state to the tail find no other owner there
// (design: Pre-check), the claim applies, `synced()` resolves and the database is `live`. If
// another owner took the stream over meanwhile, the database resets `fenced` with nothing sent,
// none of its provisional commits reaches the stream, and that owner goes on, however many offline
// opens came before. An owner that claimed and never wrote goes live the same way. Without a
// cache the open waits for the stream and fails `unreachable` when its signal fires.
//
// S6 (test/stale.e2e.ts has S1, S2 and S5): a cache another host took over before the outage
// opens provisionally and serves its stale rows. That exposure is pinned on purpose: only code
// that waits for `synced()` is protected from it. Its commits stay local, `synced()` rejects
// `fenced` once the stream answers, and the next explicit open rebuilds.
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it } from "vitest";
import { loadUrsulaVfs, open, type UrsulaDatabase, type UrsulaResetError, UrsulaUnavailableError } from "../src/index.ts";
import { Child, directUrl, ext, freshDir, frames, fromStream, gate, killChildren, rejection, stream } from "./kit.ts";

loadUrsulaVfs(ext());

const sleep = (ms: number): Promise<void> => new Promise((r) => setTimeout(r, ms));

/** How long a provisional open may take: the extension's connect limit is 5 s. */
const OPEN_LIMIT_MS = 6_000;

/** The databases a test opened: closed after it, whatever happened. */
const opened = new Set<UrsulaDatabase>();
afterEach(async () => {
	for (const d of opened) if (d.isOpen) d.close();
	opened.clear();
	await killChildren();
});

/** Whether `promise` settled. */
function watch(promise: Promise<unknown>): () => boolean {
	let settled = false;
	promise.then(
		() => {
			settled = true;
		},
		() => {
			settled = true;
		},
	);
	return () => settled;
}

/** What `f` returns once it returns something, polled for at most 30 s. */
async function until<T>(f: () => T | undefined): Promise<T> {
	const deadline = Date.now() + 30_000;
	for (;;) {
		const got = f();
		if (got !== undefined) return got;
		if (Date.now() > deadline) throw new Error("it never happened within 30 s");
		await sleep(50);
	}
}

describe("provisional opens", () => {
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

	/** A cache in `dir` that claimed the stream at `path` through the gate and holds row 1, closed. */
	async function cached(path: string, dir: string): Promise<void> {
		const first = await open(`${front.url}${path}`, { dir });
		first.exec("CREATE TABLE t(x); INSERT INTO t VALUES (1)");
		await first.synced();
		first.close();
	}

	/** Opens the cache in `dir` with the stream out of reach: it resolves provisional within the limit. */
	async function openProvisionally(path: string, dir: string): Promise<{ db: UrsulaDatabase; resets: UrsulaResetError[] }> {
		await mode("refuse");
		const started = Date.now();
		const db = await open(`${front.url}${path}`, { dir });
		opened.add(db);
		const took = Date.now() - started;
		expect(took, `the open took ${took} ms`).toBeLessThan(OPEN_LIMIT_MS);
		expect(db.status()).toMatchObject({ state: "provisional", reset: null });
		const resets: UrsulaResetError[] = [];
		db.on("reset", (e) => resets.push(e));
		return { db, resets };
	}

	it("opens a trusted cache provisionally: reads and commits work, synced() waits, then it goes live", async () => {
		const s = stream("provisional");
		const dir = freshDir();
		await cached(s.path, dir);
		const { db, resets } = await openProvisionally(s.path, dir);
		expect(db.prepare("SELECT x FROM t").all()).toEqual([{ x: 1 }]);
		db.exec("INSERT INTO t VALUES (2)");
		expect(db.prepare("SELECT x FROM t ORDER BY x").all()).toEqual([{ x: 1 }, { x: 2 }]);
		const gated = db.synced();
		const settled = watch(gated);
		await until(() => (db.status().replication.condition === "retrying" ? true : undefined));
		await sleep(1_000);
		expect(settled()).toBe(false);
		expect(db.status()).toMatchObject({ state: "provisional", reset: null });
		expect(db.status().commits.pending).toBeGreaterThan(0);

		await mode("forward");
		await gated;
		const st = db.status();
		expect(st).toMatchObject({ state: "live", reset: null });
		expect(st.commits.acknowledged).toBe(st.commits.logged);
		expect(st.replication.condition).toBeNull();
		expect(resets).toEqual([]);
		db.close();
		expect(await fromStream(s.url, "SELECT x FROM t ORDER BY x")).toEqual([{ x: 1 }, { x: 2 }]);
	});

	it("goes live over an owner that never wrote", async () => {
		const s = stream("provisional-idle");
		const dir = freshDir();
		// The owner claims the stream and closes without a commit: its ACK sits at its claim's end.
		const first = await open(`${front.url}${s.path}`, { dir });
		await first.synced();
		first.close();
		const { db, resets } = await openProvisionally(s.path, dir);
		const gated = db.synced();
		await mode("forward");
		await gated;
		expect(db.status()).toMatchObject({ state: "live", reset: null });
		db.exec("CREATE TABLE t(x); INSERT INTO t VALUES (1)");
		await db.synced();
		expect(db.status()).toMatchObject({ state: "live", reset: null });
		expect(resets).toEqual([]);
		db.close();
		expect(await fromStream(s.url, "SELECT x FROM t")).toEqual([{ x: 1 }]);
	});

	it("resets `fenced` when another owner took the stream over during the outage, and its commits never reach the stream", async () => {
		const s = stream("provisional-fenced");
		const dir = freshDir();
		await cached(s.path, dir);
		const { db, resets } = await openProvisionally(s.path, dir);
		db.exec("INSERT INTO t VALUES (2)");
		const gated = db.synced();
		// Another process, which reaches the stream, takes it over and commits.
		const other = new Child(s.url, freshDir());
		await other.opened();
		await other.exec("INSERT INTO t VALUES (3)");
		expect(await other.send({ synced: 30_000 })).toMatchObject({ ok: true, synced: { synced: true } });

		await mode("forward");
		expect(await rejection(gated)).toMatchObject({ kind: "fenced" });
		await until(() => (resets.length > 0 ? true : undefined));
		expect(resets).toHaveLength(1);
		expect(resets[0]).toMatchObject({ kind: "fenced" });
		expect(db.status().reset).toMatchObject({ kind: "fenced" });
		// The owner the explicit open installed survives the provisional one.
		await expectLive(other, 4);
		await other.stop();
		expect(await fromStream(s.url, "SELECT x FROM t ORDER BY x")).toEqual([{ x: 1 }, { x: 3 }, { x: 4 }]);
	});

	/** A process that opens the cache in `dir` through the refusing gate: provisional, with `row` committed. */
	async function offline(path: string, dir: string, row: number): Promise<Child> {
		const child = new Child(`${front.url}${path}`, dir);
		expect((await child.opened()).state).toBe("provisional");
		await child.exec(`INSERT INTO t VALUES (${row})`);
		return child;
	}

	/** `child` commits `row`, its `synced()` resolves and it is live. */
	async function expectLive(child: Child, row: number): Promise<void> {
		await child.exec(`INSERT INTO t VALUES (${row})`);
		expect(await child.send({ synced: 30_000 })).toMatchObject({ ok: true, synced: { synced: true } });
		expect(await child.send({ status: true })).toMatchObject({ ok: true, status: { state: "live", reset: null } });
	}

	it("goes live after two offline opens in a row, with the commits of both", async () => {
		const s = stream("provisional-twice");
		const dir = freshDir();
		await cached(s.path, dir);
		await mode("refuse");
		const first = await offline(s.path, dir, 2);
		await first.stop();
		const second = await offline(s.path, dir, 3);
		expect(await second.send({ synced: 500 })).toMatchObject({ ok: true, synced: { synced: false } });
		await mode("forward");
		await expectLive(second, 4);
		await second.stop();
		expect(await fromStream(s.url, "SELECT x FROM t ORDER BY x")).toEqual([{ x: 1 }, { x: 2 }, { x: 3 }, { x: 4 }]);
	});

	it("never fences an owner an explicit open installed, after two offline opens in a row", async () => {
		const s = stream("provisional-twice-fenced");
		const dir = freshDir();
		await cached(s.path, dir);
		await mode("refuse");
		const first = await offline(s.path, dir, 2);
		await first.stop();
		const second = await offline(s.path, dir, 3);
		const other = new Child(s.url, freshDir());
		await other.opened();
		await expectLive(other, 4);

		await mode("forward");
		const deadline = Date.now() + 30_000;
		let status: unknown;
		for (;;) {
			status = (await second.send({ status: true })).status;
			if ((status as { reset: unknown }).reset !== null || Date.now() > deadline) break;
			await sleep(100);
		}
		expect(status).toMatchObject({ state: "reset", reset: { kind: "fenced" } });
		await second.stop();
		await expectLive(other, 5);
		await other.stop();
		expect(await fromStream(s.url, "SELECT x FROM t ORDER BY x")).toEqual([{ x: 1 }, { x: 4 }, { x: 5 }]);
	});

	it("S6: serves the stale rows of a cache another host took over, keeps its commits local, rejects synced() `fenced` once the stream answers, and rebuilds at the next open", async () => {
		const s = stream("provisional-stale");
		const dir = freshDir();
		const a = new Child(`${front.url}${s.path}`, dir);
		expect((await a.opened()).state).toBe("live");
		await a.exec("CREATE TABLE t(x); INSERT INTO t VALUES (1), (2)");
		expect(await a.send({ synced: 30_000 })).toMatchObject({ ok: true, synced: { synced: true } });
		await a.stop();
		// Another host, a fresh root, takes the stream over and changes the rows.
		const b = new Child(s.url, freshDir());
		await b.opened();
		await b.exec("DELETE FROM t WHERE x = 1; INSERT INTO t VALUES (3)");
		expect(await b.send({ synced: 30_000 })).toMatchObject({ ok: true, synced: { synced: true } });
		await b.stop();
		const current = [{ x: 2 }, { x: 3 }];
		const stale = [{ x: 1 }, { x: 2 }];
		const before = await frames(s.path);

		await mode("refuse");
		const started = Date.now();
		const c = new Child(`${front.url}${s.path}`, dir);
		expect((await c.opened()).state).toBe("provisional");
		const took = Date.now() - started;
		expect(took, `the open took ${took} ms, the process start included`).toBeLessThan(OPEN_LIMIT_MS);
		// The exposure: rows the stream no longer holds, served while provisional.
		expect(await c.rows("SELECT x FROM t ORDER BY x")).toEqual(stale);
		await c.exec("INSERT INTO t VALUES (9)");
		expect(await c.send({ synced: 500 })).toMatchObject({ ok: true, synced: { synced: false } });
		const deadline = Date.now() + 30_000;
		for (;;) {
			const st = (await c.send({ status: true })).status as { state: string; replication: { condition: string | null }; commits: { pending: number } };
			if (st.replication.condition === "retrying") {
				expect(st.state).toBe("provisional");
				expect(st.commits.pending).toBeGreaterThan(0);
				break;
			}
			expect(Date.now(), `never retrying: ${JSON.stringify(st)}`).toBeLessThan(deadline);
			await sleep(50);
		}
		expect(await frames(s.path)).toEqual(before);

		await mode("forward");
		const gated = await c.send({ synced: 30_000 });
		expect(gated.ok, JSON.stringify(gated)).toBe(false);
		expect(String(gated.error)).toContain("ursula_synced: fenced: ");
		expect(await c.send({ status: true })).toMatchObject({ ok: true, status: { state: "reset", reset: { kind: "fenced" } } });
		await c.stop();
		expect(await frames(s.path), "a provisional commit reached the stream").toEqual(before);

		// The next explicit open discards the local tail and rebuilds.
		const reopened = new Child(s.url, dir);
		expect((await reopened.opened()).state).toBe("live");
		expect(await reopened.send({ status: true })).toMatchObject({ ok: true, status: { state: "live", reset: null, salvage: [], attach: { local: "built" } } });
		expect(await reopened.rows("SELECT x FROM t ORDER BY x")).toEqual(current);
		expect(await reopened.rows("PRAGMA integrity_check")).toEqual([{ integrity_check: "ok" }]);
		await reopened.stop();
		expect(await fromStream(s.url, "SELECT x FROM t ORDER BY x")).toEqual(current);
	});

	it("fails `unreachable` when its signal fires with no cache to open", async () => {
		const s = stream("provisional-none");
		await mode("refuse");
		const e = await rejection(open(`${front.url}${s.path}`, { dir: freshDir(), signal: AbortSignal.timeout(3_000) }));
		expect(e).toBeInstanceOf(UrsulaUnavailableError);
		expect(e).toMatchObject({ kind: "unreachable" });
	});
});

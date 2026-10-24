// Stale caches reopened across hosts (design: Open, Reconcile, Materialize, Clones), against the
// e2e server (test/stack.ts). A host is a process with a cache root of its own: the VFS uses no
// host identity, so a second root on the same runner is a faithful second host, and a copied root
// stands in for a restored disk snapshot. Each case reopens a cache that another host's writes
// made stale, as a Machine whose host-local cache or retained disk is reused does:
//
// - S1: the cache came back with nothing pending. It catches up in place, the same generation,
//   or rebuilds into a new one when the catch-up is larger than the database.
// - S2: the cache came back after a SIGKILL with a tail the stream never got (test/gate.mjs held
//   it). The tail is discarded and the generation rebuilt; S2b goes through the `410` path once
//   retention passed the cache, and rebuilds from the latest snapshot.
// - S5: a copy of the cache (an old disk snapshot) catches up and commits under the commit id
//   the original's unsent tail holds. The original's reopen rebuilds and never sends that tail.
//
// S6, a stale cache opened while the stream is out of reach, is in test/provisional.e2e.ts.
//
// The oracles, after every live reopen:
// 1. With no writer running, the database's rows equal the seeded model, and a fresh build from
//    the stream (a new process with a new root) equals the database.
// 2. No commit id is in the stream twice. No frame of the superseded lineage is above its
//    acknowledged position, or after the claim of the activation that took the stream over.
// 3. `integrity_check` passes and nothing is salvaged.
// 4. Every row whose `synced()` resolved is there, unless a later writer changed it: the model
//    applies exactly the synced transactions, so 1 checks it.
// Each case first asserts that the rows the stale cache held differ from the model after the
// other host's writes, so no case passes vacuously.
//
// Every wait is a handshake (a `synced()`, a status, the gate's held count, a HEAD), never a
// fixed sleep. The workload is seeded: the seed is printed, and STALE_SEED replays it.
//
// On the real server a last test reports, without asserting, how long a fresh build, a stale
// catch-up and a stale rebuild take to open at 1 MiB and, on one node, 16 MiB.
import { randomInt } from "node:crypto";
import { cpSync } from "node:fs";
import { dirname } from "node:path";
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, inject, it } from "vitest";
import { Child, commitsOnce, directUrl, fromStream, freshDir, gate, head, insert, killChildren, onModel, type OwnedFrame, ownedFrames, report, stream } from "./kit.ts";

afterEach(killChildren);

const SEED = process.env.STALE_SEED === undefined ? randomInt(2 ** 31) : Number(process.env.STALE_SEED);
report(`test/stale.e2e.ts: seed ${SEED} (STALE_SEED=${SEED} replays it)`);

/** The platform defaults of a Machine's VFS (D11): a 16 MiB backlog and a 16 MiB commit limit. */
const MACHINE_ENV = { URSULA_VFS_MAX_COMMIT_BYTES: String(16 << 20) };
const MACHINE_OPTIONS = JSON.stringify({ max_pending_bytes: 16 << 20 });

/** A host: a process that binds `url` in the cache root `dir` with the Machine defaults. */
function host(url: string, dir: string, env: NodeJS.ProcessEnv = {}): Child {
	return new Child(url, dir, { ...MACHINE_ENV, ...env }, "core-child.mjs", [MACHINE_OPTIONS]);
}

/** A copy of the cache root `dir`, as a restored disk snapshot holds it. */
function copyOf(dir: string): string {
	const copy = freshDir();
	cpSync(dir, copy, { recursive: true });
	return copy;
}

interface Status {
	state: string;
	epoch: number;
	path: string;
	lineage: string;
	reset: { kind: string; action: string } | null;
	attach: { local: string; snapshot_offset: number | null };
	commits: { logged: number; acknowledged: number; pending: number };
	snapshot: { offset: number | null };
	salvage: unknown[];
}

async function status(c: Child): Promise<Status> {
	const reply = await c.send({ status: true });
	expect(reply.ok, JSON.stringify(reply)).toBe(true);
	return reply.status as Status;
}

/** `c`'s `synced()` resolves. */
async function synced(c: Child): Promise<void> {
	expect(await c.send({ synced: 30_000 })).toMatchObject({ ok: true, synced: { synced: true } });
}

/** What `f` resolves to once it is not `undefined`, polled for at most 30 s. */
async function until<T>(what: string, f: () => Promise<T | undefined>): Promise<T> {
	const deadline = Date.now() + 30_000;
	for (;;) {
		const got = await f();
		if (got !== undefined) return got;
		expect(Date.now(), `${what} never happened within 30 s`).toBeLessThan(deadline);
		await new Promise((r) => setTimeout(r, 50));
	}
}

/** Closes `c`'s connections, waits until its binding retired (its exact ACK written) and exits. */
async function closeCleanly(c: Child): Promise<void> {
	expect(await c.send({ close: true })).toMatchObject({ ok: true });
	await until("the retire", async () => ((await status(c)).state === "retired" ? true : undefined));
	await c.stop();
}

/** The generation directory of a db path `ursula_open` returned. */
const generationOf = (path: string): string => dirname(path);

/** A seeded PRNG (mulberry32): the same seed, the same draws. */
function prng(seed: number): () => number {
	let a = seed >>> 0;
	return () => {
		a = (a + 0x6d2b79f5) >>> 0;
		let t = a;
		t = Math.imul(t ^ (t >>> 15), t | 1);
		t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
		return ((t ^ (t >>> 14)) >>> 0) / 2 ** 32;
	};
}

/** A test's own draws: the run's seed and the test's label. */
function drawsFor(label: string): () => number {
	let h = 2166136261;
	for (const ch of label) h = Math.imul(h ^ ch.charCodeAt(0), 16777619);
	return prng((SEED ^ h) >>> 0);
}

interface Row {
	i: number;
	v: string;
	n: number;
}

const SCHEMA = "CREATE TABLE t(i INTEGER PRIMARY KEY, v TEXT NOT NULL, pad BLOB NOT NULL)";
const ROWS = "SELECT i, v, length(pad) AS n FROM t ORDER BY i";

/**
 * The table `t` as the stream should hold it: every transaction a test applies once its
 * `synced()` resolved, and none it expects to be lost. Rows are drawn from the test's seed.
 */
class Model {
	private readonly rows = new Map<number, { v: string; n: number }>();
	private next = 1;
	private readonly draw: () => number;
	constructor(draw: () => number) {
		this.draw = draw;
	}

	/** `count` distinct ids drawn from the rows, without `except`. */
	private pick(count: number, except: ReadonlySet<number> = new Set()): number[] {
		const ids = [...this.rows.keys()].filter((i) => !except.has(i));
		for (let k = ids.length - 1; k > 0; k--) {
			const j = Math.floor(this.draw() * (k + 1));
			[ids[k], ids[j]] = [ids[j] as number, ids[k] as number];
		}
		expect(ids.length, "too few rows to draw from").toBeGreaterThanOrEqual(count);
		return ids.slice(0, count);
	}

	/**
	 * One transaction tagged `tag`: `updates` drawn rows get it as `v`, `deletes` drawn rows go, and
	 * `inserts` new rows carry `pad` random bytes. Applied to the model only with `apply`, so a
	 * transaction a test expects to vanish leaves it as it was.
	 */
	tx(tag: string, shape: { updates?: number; deletes?: number; inserts?: number; pad?: number }, apply = true): string {
		const updated = this.pick(shape.updates ?? 0);
		const deleted = this.pick(shape.deletes ?? 0, new Set(updated));
		const inserted = Array.from({ length: shape.inserts ?? 0 }, () => this.next++);
		const pad = shape.pad ?? 16;
		const sql = ["BEGIN"];
		if (updated.length > 0) sql.push(`UPDATE t SET v = '${tag}' WHERE i IN (${updated.join(", ")})`);
		if (deleted.length > 0) sql.push(`DELETE FROM t WHERE i IN (${deleted.join(", ")})`);
		if (inserted.length > 0) sql.push(`INSERT INTO t VALUES ${inserted.map((i) => `(${i}, '${tag}', randomblob(${pad}))`).join(", ")}`);
		sql.push("COMMIT");
		if (apply) {
			for (const i of updated) (this.rows.get(i) as { v: string }).v = tag;
			for (const i of deleted) this.rows.delete(i);
			for (const i of inserted) this.rows.set(i, { v: tag, n: pad });
		}
		return sql.join("; ");
	}

	dump(): Row[] {
		return [...this.rows.entries()].sort(([a], [b]) => a - b).map(([i, { v, n }]) => ({ i, v, n }));
	}
}

/** The rows of `c`'s database. */
async function rowsOf(c: Child): Promise<Row[]> {
	return (await c.rows(ROWS)) as Row[];
}

/**
 * Oracles 1 and 3 on a live reopen, with no other writer running: its rows equal the model's,
 * integrity holds, nothing is salvaged or pending. Then the next commit syncs and, once it
 * exited, a fresh build from the stream equals the database (oracle 1), and no commit id is in
 * the stream twice (oracle 2).
 */
async function converged(reopened: Child, url: string, path: string, model: Model, label: string): Promise<void> {
	const st = await status(reopened);
	expect(st, label).toMatchObject({ state: "live", reset: null, salvage: [] });
	expect(st.commits.pending, label).toBe(0);
	expect(await rowsOf(reopened), `${label}: the reopened rows`).toEqual(model.dump());
	expect(await reopened.rows("PRAGMA integrity_check")).toEqual([{ integrity_check: "ok" }]);
	await reopened.exec(model.tx(`${label}-next`, { updates: 1, inserts: 1 }));
	await synced(reopened);
	const final = await rowsOf(reopened);
	expect(final).toEqual(model.dump());
	await reopened.stop();
	expect(await fromStream(url, ROWS), `${label}: a fresh build`).toEqual(final);
	await commitsOnce(path);
}

/** Every claim activation the stream at `path` holds now. */
async function claimsOf(path: string): Promise<Set<string>> {
	return new Set((await ownedFrames(path)).flatMap((f) => (f.kind === "claim" ? [f.activation] : [])));
}

const isData = (f: OwnedFrame): f is Extract<OwnedFrame, { kind: "commit" | "part" }> => f.kind !== "claim";

/**
 * Oracle 2 for a superseded owner of `lineage` that had `acknowledged` acknowledged: none of its
 * frames is above that, and none follows the first claim of an activation not in `known` (the
 * takeover), when retention kept that claim. `takeoverRetained` says it must have.
 */
async function nothingSuperseded(path: string, lineage: string, acknowledged: number, known: ReadonlySet<string>, takeoverRetained: boolean): Promise<void> {
	const fs = await ownedFrames(path);
	expect(fs.filter(isData).filter((f) => f.lineage === lineage && f.position > acknowledged)).toEqual([]);
	const takeover = fs.findIndex((f) => f.kind === "claim" && !known.has(f.activation));
	if (takeoverRetained) expect(takeover, "the takeover's claim").toBeGreaterThanOrEqual(0);
	if (takeover >= 0)
		expect(
			fs
				.slice(takeover + 1)
				.filter(isData)
				.filter((f) => f.lineage === lineage),
		).toEqual([]);
}

describe("stale caches reopened across hosts", () => {
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

	async function mode(m: "forward" | "blackhole"): Promise<void> {
		const r = await fetch(`${front.control}/mode`, { method: "PUT", body: JSON.stringify({ mode: m }) });
		expect(r.status, await r.text()).toBe(204);
	}

	async function held(): Promise<number> {
		return ((await (await fetch(`${front.control}/stats`)).json()) as { held: number }).held;
	}

	/**
	 * Runs `sql` on `c`, which binds through the blackholed gate: the commit is local, and its
	 * append is held, never forwarded. Returns once the gate holds it and `c` counts it pending,
	 * with `c`'s status and, unless `dump` is false (a table other than the cases' `t`), its rows
	 * then.
	 */
	async function unsent(c: Child, sql: string, dump = true): Promise<{ st: Status; rows: Row[] }> {
		const before = await held();
		await mode("blackhole");
		await c.exec(sql);
		await until("the held append", async () => ((await held()) > before ? true : undefined));
		const st = await until("the pending commit", async () => {
			const s = await status(c);
			return s.commits.pending > 0 ? s : undefined;
		});
		return { st, rows: dump ? await rowsOf(c) : [] };
	}

	for (const larger of [false, true]) {
		it(`S1: a cache that came back with nothing pending ${larger ? "rebuilds when the catch-up is larger than the database" : "catches up in place"}`, async () => {
			const label = larger ? "s1-larger" : "s1";
			const model = new Model(drawsFor(label));
			const s = stream(label);
			const dirA = freshDir();
			// Host A writes about 400 KiB and closes: its exact ACK is on disk.
			const a = host(s.url, dirA);
			const first = await a.opened();
			expect(first.state).toBe("live");
			await a.exec(SCHEMA);
			await a.exec(model.tx("a", { inserts: 6, pad: 64 << 10 }));
			await a.exec(model.tx("a-small", { inserts: 6 }));
			await synced(a);
			const stale = await rowsOf(a);
			await closeCleanly(a);

			// Host B, a fresh root, updates and deletes A's rows and adds its own: at most about
			// 150 KiB of pages, or 640 KiB of new rows, more than A's database.
			const b = host(s.url, freshDir());
			await b.opened();
			await b.exec(model.tx("b", larger ? { updates: 2, deletes: 1, inserts: 10, pad: 64 << 10 } : { updates: 2, deletes: 1, inserts: 2 }));
			await synced(b);
			await b.stop();
			expect(stale, "the stale cache already held the other host's writes").not.toEqual(model.dump());

			const reopened = host(s.url, dirA);
			const again = await reopened.opened();
			expect(again.state).toBe("live");
			const st = await status(reopened);
			if (larger) {
				expect(st.attach.local).toBe("built");
				expect(generationOf(again.path)).not.toBe(generationOf(first.path));
			} else {
				// An owner's open reports a catch-up in place as `resumed` (only a reader's reports
				// `caught_up`): the same generation, now holding the other host's commits.
				expect(st.attach.local).toBe("resumed");
				expect(generationOf(again.path)).toBe(generationOf(first.path));
			}
			await converged(reopened, s.url, s.path, model, label);
		});
	}

	for (const behindRetention of [false, true]) {
		it(`S2${behindRetention ? "b" : ""}: a cache killed with an unsent tail rebuilds without it after another host wrote${behindRetention ? ", from the latest snapshot once retention passed it" : ""}`, async () => {
			const label = behindRetention ? "s2b" : "s2";
			const model = new Model(drawsFor(label));
			const s = stream(label);
			const dirA = freshDir();
			// Host A binds through the gate, so its appends can be held.
			const a = host(`${front.url}${s.path}`, dirA);
			const first = await a.opened();
			await a.exec(SCHEMA);
			await a.exec(model.tx("a", { inserts: 4, pad: 8 << 10 }));
			await a.exec(model.tx("a-small", { inserts: 6 }));
			await synced(a);
			const { tail } = await head(s.path);

			// X: committed locally, never forwarded, then the host dies.
			const x = await unsent(a, model.tx("x", { updates: 2, deletes: 1, inserts: 2 }, false));
			await a.stop("SIGKILL");
			await mode("forward");
			const known = await claimsOf(s.path);

			// Host B, a fresh root, writes Y. For S2b with a snapshot due every 4 KiB, until
			// retention passed everything A acknowledged.
			const b = host(s.url, freshDir(), behindRetention ? { URSULA_VFS_SNAPSHOT_DUE_BYTES: "4096" } : {});
			await b.opened();
			await b.exec(model.tx("y", { updates: 2, deletes: 1, inserts: 2 }));
			await synced(b);
			if (behindRetention) {
				const deadline = Date.now() + 60_000;
				for (let k = 0; (await head(s.path)).retained <= tail; k++) {
					expect(Date.now(), "retention never passed the stale cache").toBeLessThan(deadline);
					await b.exec(model.tx(`y${k}`, { updates: 1, inserts: 1, pad: 8 << 10 }));
					await synced(b);
				}
			}
			await b.stop();
			expect(x.rows, "the stale cache already held the other host's writes").not.toEqual(model.dump());

			const reopened = host(s.url, dirA);
			const again = await reopened.opened();
			expect(again.state).toBe("live");
			const st = await status(reopened);
			// The discarded tail always rebuilds, into a new lineage and generation.
			expect(st.attach.local).toBe("built");
			expect(st.lineage).not.toBe(x.st.lineage);
			expect(generationOf(again.path)).not.toBe(generationOf(first.path));
			if (behindRetention) {
				const { snapshot } = await head(s.path);
				expect(snapshot).not.toBeNull();
				expect(st.attach.snapshot_offset).toBe(snapshot);
			}
			await nothingSuperseded(s.path, x.st.lineage, x.st.commits.acknowledged, known, !behindRetention);
			await converged(reopened, s.url, s.path, model, label);
		});
	}

	it("S5: an old disk snapshot catches up and commits under the id of the original's unsent tail, and the original rebuilds without sending it", async () => {
		const model = new Model(drawsFor("s5"));
		const s = stream("s5");
		const dirA = freshDir();
		// r1, closed cleanly, then the root is copied: the old disk snapshot C.
		const a = host(`${front.url}${s.path}`, dirA);
		await a.opened();
		await a.exec(SCHEMA);
		await a.exec(model.tx("r1", { inserts: 4, pad: 64 << 10 }));
		await synced(a);
		await closeCleanly(a);
		const dirC = copyOf(dirA);

		// The original goes on: r2 synced (a few pages, far smaller than the database).
		const resumed = host(`${front.url}${s.path}`, dirA);
		await resumed.opened();
		await resumed.exec(model.tx("r2", { updates: 1, inserts: 1 }));
		await synced(resumed);
		await resumed.stop("SIGKILL");

		// A commit id's position is its record's place in the log, CLAIM, ACK and BASE records
		// included, so C's next commit lands on the original's next one only if both logs hold as
		// many records. Probes find both positions: a copy of a root opens, commits through the
		// blackholed gate and dies, leaving the stream unchanged but for a claim.
		//
		// An open's catch-up logs nothing when the stream gained nothing since the root's last ACK,
		// one ACK when it gained only claims, and each new commit plus an ACK and a BASE otherwise.
		// The open that commits r3 and C's open both find new claims (each other's and the probes'),
		// so each measurement first lets a throwaway copy of the original claim the stream: the
		// copy's probe then finds that claim and the original's probe the copy's, as the real opens
		// will. Measured alike, every open of a root adds three records to it (the catch-up's ACK,
		// its CLAIM and one more), whether it closes cleanly or is killed (CI runs 40111986003 and
		// 40113019791), so opens alone keep the gap modulo three. A synced commit by the original
		// shifts it by four or five: the original logs three records for the open and three for the
		// commit, and the copy's catch-up grows by one commit (or by a commit and a BASE once it had
		// only claims to catch up). So while the gap is not a multiple of three the original commits
		// once more, and then the root that is behind is padded with opens until both positions
		// meet. C's first padding open, if any, catches the original's commits up in place.
		const nextCommit = async (dir: string): Promise<number> => {
			const probe = host(`${front.url}${s.path}`, copyOf(dir));
			expect((await probe.opened()).state).toBe("live");
			const { st } = await unsent(probe, "INSERT INTO t VALUES (-1, 'probe', x'00')");
			await probe.stop("SIGKILL");
			await mode("forward");
			return st.commits.logged;
		};
		const claimOnce = async (dir: string): Promise<void> => {
			const throwaway = host(`${front.url}${s.path}`, copyOf(dir));
			expect((await throwaway.opened()).state).toBe("live");
			await throwaway.stop("SIGKILL");
		};
		let q = 0;
		let shifts = 0;
		for (let steps = 0; ; steps++) {
			await claimOnce(dirA);
			q = await nextCommit(dirC);
			const p = await nextCommit(dirA);
			report(`S5: after ${steps} steps (${shifts} commits by the original) the copy commits next at ${q}, the original at ${p}`);
			if (p === q) break;
			expect(steps, "the steps never met both positions").toBeLessThan(12);
			if ((p - q) % 3 !== 0) {
				expect(shifts, `the original's commits never made the gap a multiple of three (copy ${q}, original ${p})`).toBeLessThan(2);
				shifts++;
				const shift = host(`${front.url}${s.path}`, dirA);
				expect((await shift.opened()).state).toBe("live");
				await shift.exec(model.tx(`shift${shifts}`, { updates: 1, inserts: 1 }));
				await synced(shift);
				await closeCleanly(shift);
				continue;
			}
			const pad = host(`${front.url}${s.path}`, p < q ? dirA : dirC);
			expect((await pad.opened()).state).toBe("live");
			await closeCleanly(pad);
		}

		// r3: held by the gate, at position q, and the host dies.
		const original = host(`${front.url}${s.path}`, dirA);
		await original.opened();
		const r3 = await unsent(original, model.tx("r3", { updates: 1, inserts: 1 }, false));
		// The collision's precondition, before r4 depends on it: r3 sits where C commits next.
		expect(r3.st.commits.logged, "r3's position, where the copy commits next").toBe(q);
		await original.stop("SIGKILL");
		await mode("forward");

		// C opens: it catches r2 up in place and commits r4, the next position of the same lineage,
		// so r4's commit id is r3's.
		const c = host(s.url, dirC);
		expect((await c.opened()).state).toBe("live");
		const stC = await status(c);
		// Caught up in place, which an owner reports as `resumed`: the copy's lineage, not a rebuild.
		expect(stC.attach.local).toBe("resumed");
		expect(stC.lineage).toBe(r3.st.lineage);
		expect(await rowsOf(c)).toEqual(model.dump());
		await c.exec(model.tx("r4", { updates: 1, inserts: 1 }));
		await synced(c);
		expect((await status(c)).commits.logged, "r4 takes r3's commit id").toBe(r3.st.commits.logged);
		const r4 = (await ownedFrames(s.path)).filter(isData).filter((f) => f.lineage === r3.st.lineage && f.position === r3.st.commits.logged);
		expect(r4).toHaveLength(1);
		await c.stop();
		expect(r3.rows, "the original already held r4").not.toEqual(model.dump());

		// The original's reopen finds its pending id with other bytes: a divergence, never an
		// acknowledgement, so it rebuilds and r3 is gone.
		const reopened = host(s.url, dirA);
		expect((await reopened.opened()).state).toBe("live");
		const st = await status(reopened);
		expect(st.attach.local).toBe("built");
		expect(st.lineage).not.toBe(r3.st.lineage);
		await converged(reopened, s.url, s.path, model, "s5");
	});

	// Not asserted: the open time of a Machine's reuse of a cache, for the cache-placement decision.
	// Each open includes starting a Node process and loading the extension.
	const nodes = inject("ursulaNodes").length;
	describe.skipIf(onModel())("open times (reported, not asserted)", () => {
		// The three-node leg has no cold tier, so its databases stay small.
		for (const mib of nodes === 1 ? [1, 16] : [1]) {
			it(`${mib} MiB: a fresh build, a stale catch-up and a stale rebuild`, async () => {
				const s = stream(`timing-${mib}`);
				const dirA = freshDir();
				const a = host(s.url, dirA);
				await a.opened();
				await a.exec("CREATE TABLE t(i INTEGER PRIMARY KEY, b BLOB)");
				// 1 MiB per transaction, each synced: never near the 16 MiB backlog.
				for (let k = 0; k < mib; k++) {
					await a.exec(insert(k * 16, 16));
					await synced(a);
				}
				await closeCleanly(a);

				const timed = async (c: Child, attach: string): Promise<number> => {
					const started = Date.now();
					expect((await c.opened()).state).toBe("live");
					const took = Date.now() - started;
					expect((await status(c)).attach.local).toBe(attach);
					return took;
				};
				// A fresh build, which commits one row: A's cache is now behind by one small commit.
				const fresh = host(s.url, freshDir());
				const freshMs = await timed(fresh, "built");
				await fresh.exec("INSERT INTO t VALUES (-1, NULL)");
				await synced(fresh);
				await fresh.stop();
				// A stale catch-up, through the gate, which then holds a commit and the host dies.
				// An owner reports a catch-up in place as `resumed`.
				const caught = host(`${front.url}${s.path}`, dirA);
				const catchUpMs = await timed(caught, "resumed");
				await unsent(caught, "INSERT INTO t VALUES (-2, NULL)", false);
				await caught.stop("SIGKILL");
				await mode("forward");
				const other = host(s.url, freshDir());
				await other.opened();
				await other.exec("INSERT INTO t VALUES (-3, NULL)");
				await synced(other);
				await other.stop();
				// A stale rebuild: the discarded tail always rebuilds.
				const rebuilt = host(s.url, dirA);
				const rebuildMs = await timed(rebuilt, "built");
				await rebuilt.stop();
				report(`stale-cache open times, ${mib} MiB, ${nodes} node${nodes === 1 ? "" : "s"}: fresh build ${freshMs} ms, stale catch-up ${catchUpMs} ms, stale rebuild ${rebuildMs} ms (process start included)`);
			}, 600_000);
		}
	});
});

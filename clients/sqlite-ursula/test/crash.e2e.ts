// The crash matrix (design: Crashes and Verification, M1 exit criterion), against the e2e server
// (test/stack.ts): a process committing transactions of several rows each (test/crash-child.mjs) is
// killed with SIGKILL by URSULA_VFS_ABORT_AT (config::Step) at every step of the commit path
// (design: Capture), of F1 (a checkpoint, design: Fsyncs) and of F2 (a new WAL generation, and
// the WAL truncated to nothing, design: Fsyncs). After each crash the next open of the same cache
// root must need no manual step, resume the same generation (no salvage, no rebuild, no reset),
// hold every transaction up to the last one the application saw commit, or that one plus the one
// the crash interrupted once it was logged, whole, catch the stream up, and equal a fresh build
// from the stream.
//
// The second matrix kills at the steps of M2's durability paths (design: Log, Materialize, History,
// F3): a re-key while bound and at open (`rekey_rolled`, `rekey_claimed`), a publish that salvages
// (`salvage_marked`, `publish_renamed`, `salvage_moved`), the first publish of a fresh open
// (`publish_renamed`), a trim (`trim_unlinked`) and an open's cut of a torn log tail
// (`cut_removed`). test/reopen.e2e.ts kills a copy at `copy_db` and `copy_log`. After each crash
// the next open needs no manual step, equals a fresh build from the stream once `synced()`
// resolves, leaves exactly one generation and only complete salvages, and every commit the
// application saw is in the stream or in a salvage.
import { spawnSync } from "node:child_process";
import { appendFileSync, readdirSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { Child, directUrl, fromStream, freshDir, killChildren, onModel, stream } from "./kit.ts";

afterEach(killChildren);

/** The transactions the crash leaves: the last one committed (`last`), or the one after it (`next`). */
type Expect = "last" | "next";

/** How the child checkpoints (test/crash-child.mjs): every few pages, or a TRUNCATE checkpoint every few commits. */
type Checkpoints = "auto:4" | "truncate:3";

/**
 * Each step, the checkpoint mode, the occurrences it kills at (`<step>:<n>`), and where the reopen
 * must land. A setup process creates the tables, so the child's first transaction is occurrence 1.
 * - Before the COMMIT record (admission, capture, encode): the interrupted commit never happened.
 * - From the record on (logged, forward, phasetwo): the log replays it, so it is there.
 * - F1 under autocheckpoint runs after a commit returned to SQLite: that commit is logged.
 * - F2 under autocheckpoint runs at the first WAL write of the next transaction: that one is not.
 *   Occurrence 1 is the first transaction's new WAL: the setup process's close emptied it.
 * - A TRUNCATE checkpoint runs after its commit returned to the application: F1 and F2 both land
 *   on that commit. Its F2 occurrences alternate: the odd ones are a transaction's new WAL, the even
 *   ones the truncate to nothing (the WAL is still full when they kill), so the matrix takes those.
 */
const MATRIX: readonly (readonly [step: string, checkpoints: Checkpoints, occurrences: readonly number[], expect: Expect])[] = [
	["admission", "auto:4", [1, 2, 7, 20], "last"],
	["capture", "auto:4", [1, 2, 7, 20], "last"],
	["encode", "auto:4", [1, 2, 7, 20], "last"],
	["logged", "auto:4", [1, 2, 7, 20], "next"],
	["forward", "auto:4", [1, 2, 7, 20], "next"],
	["phasetwo", "auto:4", [1, 2, 7, 20], "next"],
	["checkpoint_start", "auto:4", [1, 2, 6], "next"],
	["checkpoint_synced", "auto:4", [1, 2, 6], "next"],
	["checkpoint_page", "auto:4", [1, 2, 6], "next"],
	["base_start", "auto:4", [1, 2, 6], "last"],
	["base_synced", "auto:4", [1, 2, 6], "last"],
	["based", "auto:4", [1, 2, 6], "last"],
	["checkpoint_start", "truncate:3", [1, 2], "last"],
	["checkpoint_synced", "truncate:3", [1, 2], "last"],
	["checkpoint_page", "truncate:3", [1, 2], "last"],
	["base_start", "truncate:3", [2, 4], "last"],
	["base_synced", "truncate:3", [2, 4], "last"],
	["based", "truncate:3", [2, 4], "last"],
];

/** Transactions the child commits at most: far past every kill point. */
const COMMITS = 40;
/** Rows each transaction inserts (test/crash-child.mjs). */
const ROWS_PER_COMMIT = 3;
const ROWS_SQL = "SELECT i, hex(pad) AS pad FROM t ORDER BY i, rowid";

interface Status {
	attach: { local: string; snapshot_offset: number | null };
	reset: unknown;
}

describe("crash matrix (URSULA_VFS_ABORT_AT)", () => {
	for (const [step, checkpoints, occurrences, expected] of MATRIX) {
		for (const n of occurrences) {
			it(`${step}:${n} (${checkpoints})`, async () => {
				const s = stream(`crash-${step}-${n}`);
				const dir = freshDir();
				const setup = new Child(s.url, dir);
				await setup.opened();
				await setup.exec("CREATE TABLE t(i INTEGER NOT NULL, pad BLOB); CREATE TABLE counter(n INTEGER NOT NULL); INSERT INTO counter VALUES (0)");
				expect((await setup.send({ synced: 20_000 })).synced).toMatchObject({ synced: true });
				await setup.stop();

				const doomed = new Child(s.url, dir, { URSULA_VFS_ABORT_AT: `${step}:${n}` }, "crash-child.mjs", [String(COMMITS), checkpoints]);
				let last = 0;
				let path: string | undefined;
				for (;;) {
					const line = await doomed.line();
					if (line === undefined) break;
					if (typeof line.opened === "string") path = line.opened;
					if (typeof line.committed === "number") last = line.committed;
					expect(line.done, `${step}:${n} never killed the child`).toBeUndefined();
				}
				const { signal } = await doomed.exit;
				expect(signal, `the child ended without the kill at ${step}:${n}`).toBe("SIGKILL");
				expect(path, "the child was killed before it opened").toBeDefined();

				// The next open of the cache: no manual step, the same generation resumed.
				const reopened = new Child(s.url, dir);
				expect((await reopened.opened()).path).toBe(path);
				const st = (await reopened.send({ status: true })).status as Status;
				expect(st.attach).toEqual({ local: "resumed", snapshot_offset: null });
				expect(st.reset).toBeNull();
				const want = expected === "last" ? last : last + 1;
				const [shape] = (await reopened.rows("SELECT count(*) AS rows, count(DISTINCT i) AS commits, coalesce(max(i), 0) AS max, (SELECT n FROM counter) AS counter FROM t")) as [
					{ rows: number; commits: number; max: number; counter: number },
				];
				// Every transaction whole: all its rows and the counter, none torn.
				expect(shape).toEqual({ rows: ROWS_PER_COMMIT * want, commits: want, max: want, counter: want });
				expect(await reopened.rows("PRAGMA integrity_check")).toEqual([{ integrity_check: "ok" }]);
				const local = await reopened.rows(ROWS_SQL);
				expect((await reopened.send({ synced: 20_000 })).synced).toMatchObject({ synced: true });
				// It goes on committing.
				await reopened.exec(`INSERT INTO t VALUES (${want + 1}, x'00')`);
				expect((await reopened.send({ synced: 20_000 })).synced).toMatchObject({ synced: true });
				const events = (await reopened.send({ events: true })).events as { event: string }[];
				expect(events.filter((e) => e.event === "reset")).toEqual([]);
				await reopened.stop();
				const built = await fromStream(s.url, ROWS_SQL);
				expect(built.slice(0, -1)).toEqual(local);
				expect(built.at(-1)).toEqual({ i: want + 1, pad: "00" });
			});
		}
	}
});

const sleep = (ms: number): Promise<void> => new Promise((r) => setTimeout(r, ms));

/** A history event of the model, restoring `checkpoint` if given. */
async function history(checkpoint?: string): Promise<void> {
	const query = checkpoint === undefined ? "" : `?restore=${checkpoint}`;
	const r = await fetch(`${directUrl()}/__model/history${query}`, { method: "POST" });
	expect(r.ok, await r.text()).toBe(true);
}

/** A checkpoint of every stream the model holds: its id. */
async function checkpoint(): Promise<string> {
	const r = await fetch(`${directUrl()}/__model/checkpoint`, { method: "POST" });
	const id = (await r.text()).trim();
	expect(r.ok, id).toBe(true);
	return id;
}

async function synced(child: Child): Promise<void> {
	expect((await child.send({ synced: 20_000 })).synced).toMatchObject({ synced: true });
}

/** The root directory of the one stream cached in `dir`. */
function rootOf(dir: string): string {
	const [root, ...others] = readdirSync(dir);
	expect(others).toEqual([]);
	return join(dir, root as string);
}

/** The rows `sql` returns from the SQLite file at `path`, read by node:sqlite without the extension in a process of its own. */
function plainRows(path: string, sql: string): unknown[] {
	const script = `
		import { DatabaseSync } from "node:sqlite";
		const db = new DatabaseSync(process.argv[1], { readOnly: true });
		console.log(JSON.stringify(db.prepare(process.argv[2]).all()));
		db.close();`;
	const r = spawnSync(process.execPath, ["--no-warnings", "--input-type=module", "-e", script, path, sql], { encoding: "utf8" });
	expect(r.status, r.stderr).toBe(0);
	return JSON.parse(r.stdout.trim()) as unknown[];
}

/** Binds `url` in `dir` in a child, runs `work`, then closes the database and waits for its binding to retire, which leaves an exact ACK. */
async function closedOwner(url: string, dir: string, work: (child: Child) => Promise<void>): Promise<void> {
	const child = new Child(url, dir);
	await child.opened();
	await work(child);
	expect((await child.send({ close: true })).ok).toBe(true);
	const deadline = Date.now() + 20_000;
	for (;;) {
		const events = (await child.send({ events: true })).events as { event: string }[];
		if (events.some((e) => e.event === "retired")) break;
		expect(Date.now(), "the binding never retired").toBeLessThan(deadline);
		await sleep(20);
	}
	await child.stop();
}

/** The child dies by its URSULA_VFS_ABORT_AT `step`. */
async function killedAt(child: Child, step: string): Promise<void> {
	const ended = await Promise.race([child.exit, sleep(60_000).then(() => ({ signal: "timeout" }))]);
	expect(ended.signal, `never killed at ${step}`).toBe("SIGKILL");
	await child.stop();
}

const TABLE = "CREATE TABLE IF NOT EXISTS t(x INTEGER NOT NULL, pad BLOB)";
/** The row the reopen commits: it goes on committing. */
const AFTER = 1000;

/**
 * The next open of `dir` after a crash: no manual step and no reset, `synced()` resolves, it
 * commits, and its rows equal a fresh build from the stream. The root holds exactly one
 * generation, every salvage is complete and opens as plain SQLite, every commit in `seen` is in
 * the stream or in a salvage, and the rows hold nothing but `seen`, `maybe` (a commit the crash
 * interrupted) and the reopen's own. Returns the rows the salvages hold.
 */
async function recovered(url: string, dir: string, seen: readonly number[], maybe: readonly number[] = []): Promise<number[]> {
	const next = new Child(url, dir);
	await next.opened();
	const st = (await next.send({ status: true })).status as { reset: unknown };
	expect(st.reset).toBeNull();
	await synced(next);
	expect(await next.rows("PRAGMA integrity_check")).toEqual([{ integrity_check: "ok" }]);
	await next.exec(`${TABLE}; INSERT INTO t(x) VALUES (${AFTER})`);
	await synced(next);
	const local = (await next.rows("SELECT x FROM t ORDER BY x")) as { x: number }[];
	const root = rootOf(dir);
	const generations = readdirSync(root).filter((name) => name.startsWith("gen-"));
	expect(generations).toHaveLength(1);
	expect(generations[0]).toMatch(/^gen-[0-9a-f]{32}$/);
	const salvaged: number[] = [];
	const salvage = join(root, "salvage");
	for (const name of readdirSync(root).includes("salvage") ? readdirSync(salvage) : []) {
		const manifest = JSON.parse(readFileSync(join(salvage, name, "manifest.json"), "utf8")) as { state: string };
		expect(manifest.state, name).toBe("complete");
		for (const row of plainRows(join(salvage, name, "db"), "SELECT x FROM t ORDER BY x") as { x: number }[]) salvaged.push(row.x);
	}
	await next.stop();
	expect(await fromStream(url, "SELECT x FROM t ORDER BY x")).toEqual(local);
	const kept = new Set([...local.map((r) => r.x), ...salvaged]);
	for (const x of seen) expect(kept.has(x), `commit ${x} is neither in the stream nor in a salvage`).toBe(true);
	const allowed = new Set([...seen, ...maybe, AFTER]);
	for (const { x } of local) expect(allowed.has(x), `row ${x} was never committed`).toBe(true);
	return salvaged;
}

describe("crash matrix of the M2 durability steps (URSULA_VFS_ABORT_AT)", () => {
	for (const step of ["rekey_rolled", "rekey_claimed"]) {
		it.skipIf(!onModel())(`${step}, a re-key while bound`, async () => {
			const s = stream(`m2-${step}-bound`);
			const dir = freshDir();
			const doomed = new Child(s.url, dir, { URSULA_VFS_ABORT_AT: step });
			await doomed.opened();
			await doomed.exec(`${TABLE}; INSERT INTO t(x) VALUES (1)`);
			await synced(doomed);
			await history();
			// The commit meets the new token, and the re-key it starts dies at `step`.
			const reply = await doomed.send({ exec: "INSERT INTO t(x) VALUES (2)" }).catch(() => undefined);
			await killedAt(doomed, step);
			await recovered(s.url, dir, reply?.ok === true ? [1, 2] : [1], [2]);
		});

		it.skipIf(!onModel())(`${step}, a re-key at open`, async () => {
			const s = stream(`m2-${step}-open`);
			const dir = freshDir();
			await closedOwner(s.url, dir, async (child) => {
				await child.exec(`${TABLE}; INSERT INTO t(x) VALUES (1)`);
				await synced(child);
			});
			await history();
			await killedAt(new Child(s.url, dir, { URSULA_VFS_ABORT_AT: step }), step);
			await recovered(s.url, dir, [1]);
		});
	}

	for (const step of ["salvage_marked", "publish_renamed", "salvage_moved"]) {
		it.skipIf(!onModel())(`${step}, a salvage at open after a lossy restore`, async () => {
			const s = stream(`m2-${step}`);
			const dir = freshDir();
			let saved = "";
			await closedOwner(s.url, dir, async (child) => {
				await child.exec(TABLE);
				await synced(child);
				saved = await checkpoint();
				await child.exec("INSERT INTO t(x) VALUES (1)");
				await synced(child);
			});
			await history(saved);
			await killedAt(new Child(s.url, dir, { URSULA_VFS_ABORT_AT: step }), step);
			// The restore lost commit 1: the salvage holds it.
			expect(await recovered(s.url, dir, [1])).toEqual([1]);
		});
	}

	it("publish_renamed, a fresh open's first publish", async () => {
		const s = stream("m2-publish-fresh");
		const dir = freshDir();
		await killedAt(new Child(s.url, dir, { URSULA_VFS_ABORT_AT: "publish_renamed" }), "publish_renamed");
		expect(await recovered(s.url, dir, [])).toEqual([]);
	});

	it("trim_unlinked, a trim of segments below the ACK and the BASE", async () => {
		const s = stream("m2-trim");
		const dir = freshDir();
		const doomed = new Child(s.url, dir, { URSULA_VFS_ABORT_AT: "trim_unlinked" });
		await doomed.opened();
		await doomed.exec(TABLE);
		// 1 MB commits: the log rolls at 16 MiB, and checkpoints move the BASE past the first segment.
		const seen: number[] = [];
		let x = 0;
		while (x < 30) {
			x += 1;
			const reply = await doomed.send({ exec: `INSERT INTO t VALUES (${x}, randomblob(1000000))` }).catch(() => undefined);
			if (reply?.ok !== true) break;
			seen.push(x);
		}
		await killedAt(doomed, "trim_unlinked");
		await recovered(s.url, dir, seen, [x]);
	});

	it("cut_removed, an open cutting a torn record off the log", async () => {
		const s = stream("m2-cut");
		const dir = freshDir();
		await closedOwner(s.url, dir, async (child) => {
			await child.exec(`${TABLE}; INSERT INTO t(x) VALUES (1); INSERT INTO t(x) VALUES (2)`);
			await synced(child);
		});
		// A torn record at the log's tail, as a power loss can leave one.
		const root = rootOf(dir);
		const [generation] = readdirSync(root).filter((name) => name.startsWith("gen-"));
		const log = join(root, generation as string, "log");
		const last = readdirSync(log)
			.filter((name) => name.endsWith(".usl"))
			.sort()
			.at(-1) as string;
		appendFileSync(join(log, last), Buffer.alloc(64, 0xff));
		await killedAt(new Child(s.url, dir, { URSULA_VFS_ABORT_AT: "cut_removed" }), "cut_removed");
		await recovered(s.url, dir, [1, 2]);
	});
});

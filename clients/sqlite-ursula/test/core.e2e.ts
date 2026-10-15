// The 0.8 core through its SQL functions on plain node:sqlite, against the e2e server
// (test/stack.ts): commits are local and replicate in the background, `ursula_synced` is the output
// gate, PHASETWO confirms each commit, and a reset fails every statement after it.
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import { afterEach, describe, expect, it } from "vitest";
import { loadUrsulaVfs } from "../src/index.ts";
import { Child, fromStream, frames, freshDir, killChildren, type Opened, stream, ursulaUrl } from "./kit.ts";

const SQLITE_IOERR_READ = 266;
const SQLITE_IOERR_WRITE = 778;
const SQLITE_IOERR_AUTH = 7178;

const ext = process.env.SQLITE_URSULA_VFS ?? "";

interface Status {
	name: string;
	state: string;
	epoch: number | null;
	commits: { logged: number; confirmed: number; acknowledged: number; pending: number; pending_bytes: number };
	replication: { state: string; condition: string | null };
	snapshot: { offset: number | null; age_ms: number | null; failures: number; error: string | null; log_bytes: number; due_bytes: number };
	refused: { kind: string; action: string; message: string } | null;
	reset: { kind: string; action: string } | null;
}

/** One SQL function's JSON result on the control connection. */
function call<T>(sql: string, ...args: (string | number)[]): T {
	const row = loadUrsulaVfs(ext).prepare(sql).get(...args) as Record<string, string>;
	return JSON.parse(Object.values(row)[0] as string) as T;
}

const open = (url: string, dir: string): Opened => call<Opened>("SELECT ursula_open(?, ?, '{}', 20000)", url, dir);
const status = (path: string): Status => call<Status>("SELECT ursula_status(?)", path);
const synced = (path: string, ms = 10_000): { synced: boolean; target: number; acknowledged: number } => call("SELECT ursula_synced(?, ?)", path, ms);

let cursor = 0;
/** Waits for an event of this process that `matches`, for at most 20 s. */
function waitEvent(matches: (event: Record<string, unknown>) => boolean): Record<string, unknown> {
	const deadline = Date.now() + 20_000;
	while (Date.now() < deadline) {
		const got = call<{ cursor: number; events: Record<string, unknown>[] }>("SELECT ursula_events(?, 1000)", cursor);
		cursor = got.cursor;
		const found = got.events.find(matches);
		if (found !== undefined) return found;
	}
	throw new Error("no such event within 20 s");
}

afterEach(killChildren);

/** The stream's latest snapshot offset and retained offset, from a HEAD. */
async function retention(path: string): Promise<{ snapshot: number | null; retained: number }> {
	const r = await fetch(`${ursulaUrl()}${path}`, { method: "HEAD" });
	const snapshot = r.headers.get("stream-snapshot-offset");
	return { snapshot: snapshot === null ? null : Number(snapshot), retained: Number(r.headers.get("stream-retained-offset") ?? "0") };
}

/** Polls the child's status until `ready` holds, for at most 20 s. */
async function waitStatus(c: Child, ready: (s: Status) => boolean): Promise<Status> {
	const deadline = Date.now() + 20_000;
	let last: Status | undefined;
	while (Date.now() < deadline) {
		last = (await c.send({ status: true })).status as Status;
		if (ready(last)) return last;
		await new Promise((r) => setTimeout(r, 50));
	}
	throw new Error(`status never got there: ${JSON.stringify(last)}`);
}

describe("the 0.8 core on node:sqlite", () => {
	it("loads into the running SQLite and registers the 0.8 functions only", () => {
		const control = loadUrsulaVfs(ext);
		const names = control
			.prepare("SELECT DISTINCT name FROM pragma_function_list WHERE name LIKE 'ursula!_%' ESCAPE '!' ORDER BY name")
			.all()
			.map((row) => (row as { name: string }).name);
		expect(names).toEqual(["ursula_cancel", "ursula_events", "ursula_open", "ursula_set_token", "ursula_stats", "ursula_status", "ursula_synced"]);
		expect(() => call("SELECT ursula_status('/nowhere/db')")).toThrow(/^ursula_status: not_bound: /);
		// A database the extension did not bind passes through its VFS unchanged.
		const plain = new DatabaseSync(join(freshDir(), "plain.db"));
		plain.exec("PRAGMA journal_mode=WAL; CREATE TABLE t(x); INSERT INTO t VALUES (42)");
		expect(plain.prepare("SELECT x FROM t").get()).toEqual({ x: 42 });
		plain.close();
	});

	it("commits locally, confirms each commit, and synced() waits for the stream to hold them", async () => {
		const s = stream("commit");
		const dir = freshDir();
		const opened = open(s.url, dir);
		expect(opened).toMatchObject({ done: true, name: s.path, state: "live" });
		const db = new DatabaseSync(opened.path);
		db.exec("CREATE TABLE t(x)");
		for (let i = 1; i <= 3; i++) db.prepare("INSERT INTO t VALUES (?)").run(i);
		const before = status(opened.path);
		expect(before.state).toBe("live");
		expect(before.commits.confirmed).toBe(before.commits.logged);
		const s1 = synced(opened.path);
		expect(s1.synced).toBe(true);
		expect(s1.acknowledged).toBeGreaterThanOrEqual(s1.target);
		expect(status(opened.path).commits).toMatchObject({ pending: 0, pending_bytes: 0 });
		expect(await frames(s.path)).toEqual({ claims: 1, commits: 4 });
		const stats = call<{ commits: { bytes: number; pages: number }[] }>("SELECT ursula_stats(?)", opened.path);
		expect(stats.commits.length).toBe(4);
		db.close();
		// The last connection closed: the binding drains, retires and unlocks its cache, which
		// another process then opens.
		waitEvent((e) => e.event === "retired" && e.path === opened.path);
		const next = new Child(s.url, dir);
		await next.opened();
		expect(await next.rows("SELECT x FROM t ORDER BY x")).toEqual([{ x: 1 }, { x: 2 }, { x: 3 }]);
		await next.stop();
		expect(await fromStream(s.url, "SELECT x FROM t ORDER BY x")).toEqual([{ x: 1 }, { x: 2 }, { x: 3 }]);
	});

	it("keeps its cache locked while a connection reopened right after a close holds it", async () => {
		const s = stream("relock");
		const dir = freshDir();
		const first = open(s.url, dir);
		const a = new DatabaseSync(first.path);
		a.exec("CREATE TABLE t(x); INSERT INTO t VALUES (1)");
		a.close();
		// Reopened at once through ursula_open: the binding is taken back, or bound anew if it
		// retired meanwhile. Either way this process holds the cache while the connection lives.
		const again = open(s.url, dir);
		const b = new DatabaseSync(again.path);
		expect(b.prepare("SELECT x FROM t").all()).toEqual([{ x: 1 }]);
		const intruder = new Child(s.url, dir);
		const refused = await intruder.first();
		expect(refused.ok).toBe(false);
		expect(String(refused.error)).toMatch(/ursula_open: in_use: /);
		await intruder.stop();
		b.exec("INSERT INTO t VALUES (2)");
		expect(synced(again.path).synced).toBe(true);
		b.close();
		waitEvent((e) => e.event === "retired" && e.path === again.path);
		const next = new Child(s.url, dir);
		await next.opened();
		expect(await next.rows("SELECT x FROM t ORDER BY x")).toEqual([{ x: 1 }, { x: 2 }]);
		await next.stop();
	});

	it("reopens its cache in a second process: materialize, take over, reconcile", async () => {
		const s = stream("reopen");
		const dir = freshDir();
		const first = new Child(s.url, dir);
		await first.opened();
		await first.exec("CREATE TABLE t(x); INSERT INTO t VALUES (1)");
		await first.exec("INSERT INTO t VALUES (2)");
		expect((await first.send({ synced: 10_000 })).synced).toMatchObject({ synced: true });
		await first.stop();
		const second = new Child(s.url, dir);
		await second.opened();
		expect(await second.rows("SELECT x FROM t ORDER BY x")).toEqual([{ x: 1 }, { x: 2 }]);
		await second.exec("INSERT INTO t VALUES (3)");
		expect((await second.send({ synced: 10_000 })).synced).toMatchObject({ synced: true });
		const st = (await second.send({ status: true })).status as Status;
		expect(st.epoch).toBe(2);
		await second.stop();
		expect(await frames(s.path)).toEqual({ claims: 2, commits: 4 });
		expect(await fromStream(s.url, "SELECT x FROM t ORDER BY x")).toEqual([{ x: 1 }, { x: 2 }, { x: 3 }]);
	});

	it("keeps every commit a SIGKILL interrupts: the reopen sends what never reached the stream", async () => {
		const s = stream("sigkill");
		const dir = freshDir();
		const doomed = new Child(s.url, dir);
		await doomed.opened();
		await doomed.exec("CREATE TABLE t(x)");
		for (let i = 1; i <= 20; i++) await doomed.exec(`INSERT INTO t VALUES (${i})`);
		const committed = await doomed.rows("SELECT x FROM t ORDER BY x");
		await doomed.stop("SIGKILL");
		const reopened = new Child(s.url, dir);
		await reopened.opened();
		expect(await reopened.rows("SELECT x FROM t ORDER BY x")).toEqual(committed);
		expect((await reopened.send({ synced: 10_000 })).synced).toMatchObject({ synced: true });
		await reopened.stop();
		expect(await fromStream(s.url, "SELECT x FROM t ORDER BY x")).toEqual(committed);
	});

	it("resets a database another process took over: 266 for reads, 778 for writes, a fenced event", async () => {
		const s = stream("takeover");
		const owner = new Child(s.url, freshDir());
		await owner.opened();
		await owner.exec("CREATE TABLE t(x); INSERT INTO t VALUES (1)");
		expect((await owner.send({ synced: 10_000 })).synced).toMatchObject({ synced: true });
		const taker = new Child(s.url, freshDir());
		await taker.opened();
		expect(await taker.rows("SELECT x FROM t")).toEqual([{ x: 1 }]);
		// A read transaction of the old owner open across the reset keeps its snapshot, but its write
		// fails. The gate's probe finds the new owner's claim and resets the old owner.
		expect(await owner.send({ conn: 2, exec: "BEGIN; SELECT count(*) FROM t" })).toMatchObject({ ok: true });
		const gate = await owner.send({ synced: 10_000 });
		expect(gate.ok).toBe(false);
		expect(String(gate.error)).toMatch(/ursula_synced: fenced: /);
		expect(await owner.send({ conn: 2, all: "SELECT x FROM t" })).toMatchObject({ ok: true, rows: [{ x: 1 }] });
		const write = await owner.send({ conn: 2, exec: "INSERT INTO t VALUES (2); COMMIT" });
		expect(write).toMatchObject({ ok: false, errcode: SQLITE_IOERR_WRITE });
		const read = await owner.send({ all: "SELECT x FROM t" });
		expect(read).toMatchObject({ ok: false, errcode: SQLITE_IOERR_READ });
		const st = (await owner.send({ status: true })).status as Status;
		expect(st.reset).toEqual({ kind: "fenced", action: "stop" });
		const events = (await owner.send({ events: true })).events as Record<string, unknown>[];
		expect(events).toContainEqual(expect.objectContaining({ event: "reset", kind: "fenced", action: "stop" }));
		await taker.exec("INSERT INTO t VALUES (4)");
		expect((await taker.send({ synced: 10_000 })).synced).toMatchObject({ synced: true });
		await taker.stop();
		await owner.stop();
		expect(await fromStream(s.url, "SELECT x FROM t ORDER BY x")).toEqual([{ x: 1 }, { x: 4 }]);
	});

	describe("PHASETWO confirms every commit (the prototype's scenarios)", () => {
		it("basic: one connection's commits", () => {
			const s = stream("basic");
			const opened = open(s.url, freshDir());
			const db = new DatabaseSync(opened.path);
			db.exec("CREATE TABLE t(x)");
			for (let i = 0; i < 5; i++) {
				db.prepare("INSERT INTO t VALUES (?)").run(i);
				const c = status(opened.path).commits;
				expect(c.confirmed).toBe(c.logged);
			}
			expect(status(opened.path).reset).toBeNull();
			db.close();
		});

		it("spill: a transaction that outgrows the page cache before its commit", async () => {
			const s = stream("spill");
			const opened = open(s.url, freshDir());
			const db = new DatabaseSync(opened.path);
			db.exec("PRAGMA cache_size=2; CREATE TABLE t(x)");
			db.exec("BEGIN");
			for (let i = 0; i < 200; i++) db.prepare("INSERT INTO t VALUES (randomblob(1000))").run();
			db.exec("COMMIT");
			const c = status(opened.path).commits;
			expect(c.confirmed).toBe(c.logged);
			expect(synced(opened.path).synced).toBe(true);
			db.close();
			expect(await fromStream(s.url, "SELECT count(*) AS n FROM t")).toEqual([{ n: 200 }]);
		});

		it("multi: two connections committing in turn", () => {
			const s = stream("multi");
			const opened = open(s.url, freshDir());
			const a = new DatabaseSync(opened.path);
			const b = new DatabaseSync(opened.path);
			a.exec("CREATE TABLE t(x)");
			for (let i = 0; i < 6; i++) {
				(i % 2 === 0 ? a : b).prepare("INSERT INTO t VALUES (?)").run(i);
				const c = status(opened.path).commits;
				expect(c.confirmed).toBe(c.logged);
			}
			expect(b.prepare("SELECT count(*) AS n FROM t").get()).toEqual({ n: 6 });
			a.close();
			b.close();
		});

		it("attachfail: a commit published locally whose attached database fails phase one is unconfirmed, and the reopen commits it", async () => {
			const s = stream("attachfail");
			const dir = freshDir();
			const other = join(freshDir(), "other.db");
			const failing = new Child(s.url, dir, { URSULA_VFS_FAIL_SYNC: "other.db-journal" });
			await failing.opened();
			await failing.exec("CREATE TABLE t(x)");
			await failing.exec(`ATTACH '${other}' AS other; PRAGMA other.synchronous=OFF; CREATE TABLE other.u(y)`);
			await failing.exec("PRAGMA other.synchronous=FULL");
			const commit = await failing.send({ exec: "BEGIN; INSERT INTO t VALUES (1); INSERT INTO other.u VALUES (1); COMMIT" });
			expect(commit.ok).toBe(false);
			const st = (await failing.send({ status: true })).status as Status;
			expect(st.reset).toEqual({ kind: "local_damage", action: "reopen" });
			expect(await failing.send({ all: "SELECT x FROM t" })).toMatchObject({ ok: false, errcode: SQLITE_IOERR_READ });
			await failing.stop();
			const reopened = new Child(s.url, dir);
			await reopened.opened();
			expect(await reopened.rows("SELECT x FROM t")).toEqual([{ x: 1 }]);
			expect((await reopened.send({ synced: 10_000 })).synced).toMatchObject({ synced: true });
			await reopened.stop();
			expect(await fromStream(s.url, "SELECT x FROM t")).toEqual([{ x: 1 }]);
		});
	});

	it("captures a spilled transaction rolled back to a savepoint: only what it committed reaches the stream", async () => {
		const s = stream("savepoint");
		const opened = open(s.url, freshDir());
		const db = new DatabaseSync(opened.path);
		db.exec("PRAGMA cache_size=2; CREATE TABLE t(x, pad)");
		db.exec("BEGIN; INSERT INTO t VALUES (1, randomblob(3000)); SAVEPOINT s");
		for (let i = 100; i < 300; i++) db.prepare("INSERT INTO t VALUES (?, randomblob(3000))").run(i);
		db.exec("ROLLBACK TO s; RELEASE s; INSERT INTO t VALUES (2, randomblob(3000)); COMMIT");
		const c = status(opened.path).commits;
		expect(c.confirmed).toBe(c.logged);
		expect(db.prepare("SELECT x FROM t ORDER BY x").all()).toEqual([{ x: 1 }, { x: 2 }]);
		expect(synced(opened.path).synced).toBe(true);
		db.close();
		expect(await fromStream(s.url, "SELECT x FROM t ORDER BY x")).toEqual([{ x: 1 }, { x: 2 }]);
		expect(await fromStream(s.url, "PRAGMA integrity_check")).toEqual([{ integrity_check: "ok" }]);
	});

	it("publishes snapshots, keeps retention one behind, and a fresh directory builds from the latest", async () => {
		const s = stream("snapshot");
		// Due after any acknowledged byte: one snapshot per commit transaction.
		const owner = new Child(s.url, freshDir(), { URSULA_VFS_SNAPSHOT_DUE_BYTES: "1" });
		await owner.opened();
		await owner.exec("BEGIN; CREATE TABLE t(x, pad); WITH RECURSIVE n(v) AS (SELECT 1 UNION ALL SELECT v + 1 FROM n WHERE v < 20) INSERT INTO t SELECT v, randomblob(2000) FROM n; COMMIT");
		expect((await owner.send({ synced: 10_000 })).synced).toMatchObject({ synced: true });
		const first = await waitStatus(owner, (st) => st.snapshot.offset !== null && st.snapshot.log_bytes === 0);
		expect(first.snapshot.error).toBeNull();
		expect(await retention(s.path)).toEqual({ snapshot: first.snapshot.offset, retained: 0 });
		await owner.exec("WITH RECURSIVE n(v) AS (SELECT 21 UNION ALL SELECT v + 1 FROM n WHERE v < 40) INSERT INTO t SELECT v, randomblob(2000) FROM n");
		expect((await owner.send({ synced: 10_000 })).synced).toMatchObject({ synced: true });
		const second = await waitStatus(owner, (st) => (st.snapshot.offset ?? 0) > (first.snapshot.offset ?? 0) && st.snapshot.log_bytes === 0);
		// Retention trails one snapshot behind.
		expect(await retention(s.path)).toEqual({ snapshot: second.snapshot.offset, retained: first.snapshot.offset });
		const rows = await owner.rows("SELECT x, hex(pad) AS pad FROM t ORDER BY x");
		await owner.stop();
		// Below retention, only the snapshot and the tail after it can build a fresh directory.
		expect(await fromStream(s.url, "SELECT x, hex(pad) AS pad FROM t ORDER BY x")).toEqual(rows);
		expect(await fromStream(s.url, "PRAGMA integrity_check")).toEqual([{ integrity_check: "ok" }]);
	});

	it("does not publish a snapshot body above the request limit and reports it in snapshot.error", async () => {
		const s = stream("oversized");
		const owner = new Child(s.url, freshDir(), { URSULA_VFS_SNAPSHOT_DUE_BYTES: "100000", URSULA_VFS_MAX_REQUEST_BYTES: "32768" });
		await owner.opened();
		await owner.exec("CREATE TABLE t(x, pad)");
		for (let i = 1; i <= 40; i++) await owner.exec(`INSERT INTO t VALUES (${i}, randomblob(3000))`);
		expect((await owner.send({ synced: 10_000 })).synced).toMatchObject({ synced: true });
		const st = await waitStatus(owner, (st) => st.snapshot.error !== null);
		expect(st.snapshot.error).toMatch(/^request_too_large: /);
		expect(st.snapshot.offset).toBeNull();
		expect(st.reset).toBeNull();
		expect(await retention(s.path)).toEqual({ snapshot: null, retained: 0 });
		await owner.stop();
	});

	it("refuses a write in EXCLUSIVE locking mode alone, as unsupported", () => {
		const s = stream("exclusive");
		const opened = open(s.url, freshDir());
		const db = new DatabaseSync(opened.path);
		db.exec("CREATE TABLE t(x)");
		db.exec("PRAGMA locking_mode=EXCLUSIVE");
		let errcode: number | undefined;
		try {
			db.exec("INSERT INTO t VALUES (1)");
		} catch (error) {
			errcode = (error as { errcode?: number }).errcode;
		}
		expect(errcode).toBe(SQLITE_IOERR_AUTH);
		const st = status(opened.path);
		expect(st.refused?.kind).toBe("unsupported");
		expect(st.reset).toBeNull();
		db.close();
	});
});

// `open()` against the e2e server (test/stack.ts): the first open of a stream binds and claims it,
// later and concurrent opens in the process share the binding, its signal cancels it, and its
// failures and refused writes are typed.
import { DatabaseSync, type SQLInputValue } from "node:sqlite";
import { sql } from "drizzle-orm";
import { drizzle } from "drizzle-orm/node-sqlite";
import { Kysely, SqliteDialect, type SqliteDatabase } from "kysely";
import { describe, expect, it } from "vitest";
import { loadUrsulaVfs, open, UrsulaDatabase, UrsulaError, UrsulaSetupError, UrsulaUnavailableError, UrsulaWriteRefusedError } from "../src/index.ts";
import { ext, fault, foreignAppend, frames, freshDir, rejection, stream, thrown } from "./kit.ts";

loadUrsulaVfs(ext());

describe("open()", () => {
	it("binds and claims the stream, and returns a DatabaseSync named after it", async () => {
		const s = stream("bind");
		const db = await open(s.url, { dir: freshDir() });
		expect(db).toBeInstanceOf(UrsulaDatabase);
		expect(db).toBeInstanceOf(DatabaseSync);
		expect(db.name).toBe(s.path);
		expect(db.location()).toBe(db.status().path);
		expect(db.status()).toMatchObject({ name: s.path, state: "live", role: "owner", reset: null });
		db.exec("CREATE TABLE t(x); INSERT INTO t VALUES (1)");
		await db.synced();
		expect(db.prepare("SELECT x FROM t").all()).toEqual([{ x: 1 }]);
		// One claim, and one commit per autocommitted statement.
		expect(await frames(s.path)).toEqual({ claims: 1, commits: 2 });
		db.close();
	});

	it("returns another connection on the same binding for a later open, with no claim", async () => {
		const s = stream("again");
		const dir = freshDir();
		const a = await open(s.url, { dir });
		a.exec("CREATE TABLE t(x); INSERT INTO t VALUES (1)");
		const b = await open(s.url, { dir });
		expect(b).not.toBe(a);
		expect(b.location()).toBe(a.location());
		expect(b.prepare("SELECT x FROM t").all()).toEqual([{ x: 1 }]);
		b.exec("INSERT INTO t VALUES (2)");
		await b.synced();
		expect(await frames(s.path)).toEqual({ claims: 1, commits: 3 });
		a.close();
		b.close();
	});

	it("shares one open between concurrent calls", async () => {
		const s = stream("concurrent");
		const dir = freshDir();
		// The open's first answer comes late, so the later calls start while it is in flight.
		await fault("delay 500", 1);
		let firstDone = false;
		const first = open(s.url, { dir }).then((db) => {
			firstDone = true;
			return db;
		});
		await new Promise((r) => setTimeout(r, 100));
		expect(firstDone).toBe(false);
		const [a, b, c] = await Promise.all([first, open(s.url, { dir }), open(s.url, { dir })]);
		expect(new Set([a, b, c]).size).toBe(3);
		expect(new Set([a.location(), b.location(), c.location()]).size).toBe(1);
		a.exec("CREATE TABLE t(x)");
		await c.synced();
		expect(await frames(s.path)).toEqual({ claims: 1, commits: 1 });
		for (const db of [a, b, c]) db.close();
	});

	it("fails every concurrent call with the one failure of the open they share", async () => {
		const s = stream("concurrent-failure");
		// Another writer's bytes in the stream: no 0.8 database, so the open fails.
		const created = await fetch(s.url, { method: "PUT", headers: { "content-type": "application/octet-stream" } });
		expect(created.ok, await created.text()).toBe(true);
		await foreignAppend(s.path, "not a frame");
		const dir = freshDir();
		const outcomes = await Promise.allSettled([open(s.url, { dir }), open(s.url, { dir }), open(s.url, { dir })]);
		const errors = outcomes.map((o) => (o.status === "rejected" ? o.reason : o.value));
		expect(errors[0]).toBeInstanceOf(UrsulaSetupError);
		expect(errors[1]).toBe(errors[0]);
		expect(errors[2]).toBe(errors[0]);
	});

	it("reopens through open() after the last connection closed", async () => {
		const s = stream("reopen");
		const dir = freshDir();
		const a = await open(s.url, { dir });
		a.exec("CREATE TABLE t(x); INSERT INTO t VALUES (1)");
		await a.synced();
		a.close();
		const b = await open(s.url, { dir });
		expect(b.prepare("SELECT x FROM t").all()).toEqual([{ x: 1 }]);
		b.close();
	});

	it("passes node:sqlite's options on", async () => {
		const s = stream("options");
		const db = await open(s.url, { dir: freshDir(), readBigInts: true });
		expect(db.prepare("SELECT 1 AS one").get()).toEqual({ one: 1n });
		db.close();
	});

	it("refuses a URL it cannot take, as a setup error to fix", async () => {
		const error = await rejection(open("not a url", { dir: freshDir() }));
		expect(error).toBeInstanceOf(UrsulaSetupError);
		expect(error).toMatchObject({ kind: "invalid_url", action: "fix" });
		expect(await rejection(open(stream("bad").url, { maxPendingBytes: -1 }))).toMatchObject({ kind: "invalid_argument" });
	});

	it("cancels at its signal: an unavailable error, and the stream is not held", async () => {
		const s = stream("cancel");
		const dir = freshDir();
		// The claim's answers are held back longer than the signal waits.
		await fault("delay 3000", 4);
		const error = await rejection(open(s.url, { dir, signal: AbortSignal.timeout(300) }));
		expect(error).toBeInstanceOf(UrsulaUnavailableError);
		expect(error).toMatchObject({ kind: "cancelled", action: "retry" });
		expect((error as UrsulaError).cause).toBeInstanceOf(DOMException);
		expect(await rejection(open(s.url, { dir, signal: AbortSignal.abort() }))).toMatchObject({ kind: "cancelled" });
		await new Promise((r) => setTimeout(r, 3500));
		const db = await open(s.url, { dir });
		db.exec("CREATE TABLE t(x)");
		await db.synced();
		db.close();
	});

	it("reports the last condition the open met when its signal fires", async () => {
		const error = await rejection(open("http://127.0.0.1:9/e2e/unreachable", { dir: freshDir(), signal: AbortSignal.timeout(1500) }));
		expect(error).toBeInstanceOf(UrsulaUnavailableError);
		expect(error).toMatchObject({ kind: "unreachable", action: "retry" });
		expect((error as UrsulaError).cause).toBeInstanceOf(DOMException);
	});

	it("honors a signal longer than 30 s", { timeout: 120_000 }, async () => {
		const s = stream("long");
		// The open's first three answers come 12 s late each: past the default 30 s in all.
		await fault("delay 12000", 3);
		const started = Date.now();
		const db = await open(s.url, { dir: freshDir(), signal: AbortSignal.timeout(90_000) });
		expect(Date.now() - started).toBeGreaterThanOrEqual(31_000);
		db.close();
	});

	it("defaults node:sqlite's busy timeout to 5 s, and keeps the one passed", async () => {
		const s = stream("busy");
		const dir = freshDir();
		const a = await open(s.url, { dir });
		const b = await open(s.url, { dir, timeout: 50 });
		const busy = (db: DatabaseSync): number => (db.prepare("PRAGMA busy_timeout").get() as { timeout: number }).timeout;
		expect(busy(a)).toBe(5000);
		expect(busy(b)).toBe(50);
		a.close();
		b.close();
	});

	it("runs under Kysely, whose errors are the database's", async () => {
		const s = stream("kysely");
		const db = await open(s.url, { dir: freshDir(), maxPendingBytes: 1 });
		// Kysely's SqliteDialect speaks better-sqlite3 (parameters as one array, `reader`): the
		// adapter any node:sqlite DatabaseSync needs.
		const database: SqliteDatabase = {
			close: () => db.close(),
			prepare: (sql) => {
				const statement = db.prepare(sql);
				return {
					reader: statement.columns().length > 0,
					all: (parameters) => statement.all(...(parameters as SQLInputValue[])),
					run: (parameters) => statement.run(...(parameters as SQLInputValue[])),
					iterate: (parameters) => statement.iterate(...(parameters as SQLInputValue[])),
				};
			},
		};
		const k = new Kysely<{ t: { x: number } }>({ dialect: new SqliteDialect({ database }) });
		await k.schema.createTable("t").addColumn("x", "integer").execute();
		// With a 1-byte bound, a commit while this one is unacknowledged is refused (backlog_full).
		await db.synced();
		await k.insertInto("t").values([{ x: 1 }, { x: 2 }]).execute();
		await db.synced();
		expect(await k.selectFrom("t").select("x").orderBy("x").execute()).toEqual([{ x: 1 }, { x: 2 }]);
		await fault("delay 1000", 1, "POST");
		await k.insertInto("t").values({ x: 3 }).execute();
		const refused = await rejection(k.insertInto("t").values({ x: 4 }).execute());
		expect(refused).toBeInstanceOf(UrsulaWriteRefusedError);
		await db.synced();
		await k.destroy();
	});

	it("refuses a write past maxPendingBytes with nothing committed, and keeps the error across a helper's ROLLBACK", async () => {
		const s = stream("backlog");
		const db = await open(s.url, { dir: freshDir(), maxPendingBytes: 1 });
		db.exec("CREATE TABLE t(x)");
		await db.synced();
		// The next append's answer comes late: its commit stays pending, and holds the backlog full.
		await fault("delay 1500", 1, "POST");
		db.exec("INSERT INTO t VALUES (1)");
		const refused = thrown(() => db.exec("INSERT INTO t VALUES (2)"));
		expect(refused).toBeInstanceOf(UrsulaWriteRefusedError);
		expect(refused).toMatchObject({ kind: "backlog_full", action: "retry", errcode: 7178 });
		// A transaction helper's ROLLBACK after the refused COMMIT does not hide it.
		const orm = drizzle({ client: db });
		const helper = thrown(() => orm.transaction((tx) => void tx.run(sql`INSERT INTO t VALUES (3)`)));
		const found = (e: unknown): unknown => (e instanceof UrsulaError ? e : (e as { cause?: unknown }).cause);
		expect(found(helper)).toBeInstanceOf(UrsulaWriteRefusedError);
		const manual = thrown(() => {
			try {
				db.exec("BEGIN; INSERT INTO t VALUES (4); COMMIT");
			} catch (error) {
				db.prepare("ROLLBACK").run();
				throw error;
			}
		});
		expect(manual).toMatchObject({ kind: "backlog_full" });
		await db.synced();
		db.exec("INSERT INTO t VALUES (5)");
		await db.synced();
		expect(db.prepare("SELECT x FROM t ORDER BY x").all()).toEqual([{ x: 1 }, { x: 5 }]);
		db.close();
	});

	it("refuses an unsupported write alone, and leaves SQLite's own errors as they are", async () => {
		const db = await open(stream("unsupported").url, { dir: freshDir() });
		db.exec("CREATE TABLE t(x UNIQUE); INSERT INTO t VALUES (1)");
		const constraint = thrown(() => db.exec("INSERT INTO t VALUES (1)"));
		expect(constraint).not.toBeInstanceOf(UrsulaError);
		db.exec("PRAGMA locking_mode=EXCLUSIVE");
		expect(thrown(() => db.exec("INSERT INTO t VALUES (2)"))).toMatchObject({ kind: "unsupported", action: "fix" });
		db.close();
	});
});

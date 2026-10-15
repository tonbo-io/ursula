// Read-only opens (design D6), against the e2e server (test/stack.ts). A reader in a process with no
// binding of the stream builds a reader cache of its own in `reader-<n>` beside the owner's lock and
// reads the stream as of its open: it claims nothing, so the owner is never fenced and no claim
// frame appears, its writes fail with SQLITE_READONLY and `synced()` resolves at once. A reader in
// the owner's process reads the owner's binding instead, and sees its latest writes. A missing
// stream is `not_found`, an unreachable one opens an earlier reader cache as it is (or waits until
// the signal fires without one), and `recreate` cannot go with `readOnly`.
import { sep } from "node:path";
import { DatabaseSync } from "node:sqlite";
import { afterEach, describe, expect, it } from "vitest";
import { loadUrsulaVfs, open, type UrsulaStatus, UrsulaSetupError, UrsulaUnavailableError } from "../src/index.ts";
import { Child, closeStream, ext, freshDir, frames, gate, killChildren, rejection, retired, stream, thrown, ursulaUrl } from "./kit.ts";

loadUrsulaVfs(ext());

afterEach(killChildren);

const SQLITE_READONLY = 8;
const SQLITE_NOTADB = 26;
const READ_ONLY = JSON.stringify({ read_only: true });

/** A reader process of the stream at `url` with the cache root `dir`, opened. */
async function reader(url: string, dir: string): Promise<{ child: Child; path: string }> {
	const child = new Child(url, dir, {}, "core-child.mjs", [READ_ONLY]);
	const opened = await child.opened();
	return { child, path: opened.path };
}

async function statusOf(child: Child): Promise<UrsulaStatus> {
	const reply = await child.send({ status: true });
	expect(reply.ok, JSON.stringify(reply)).toBe(true);
	return reply.status as UrsulaStatus;
}


/** `PUT /mode` on the gate's control port. */
async function mode(control: string, m: "forward" | "refuse"): Promise<void> {
	const r = await fetch(`${control}/mode`, { method: "PUT", body: JSON.stringify({ mode: m }) });
	expect(r.status).toBe(204);
}

describe("read-only opens", () => {
	it("a reader in another process gets reader-0, reads the stream as of its open, claims nothing, refuses writes and syncs at once", async () => {
		const s = stream("reader");
		const dir = freshDir();
		const owner = await open(s.url, { dir });
		owner.exec("CREATE TABLE t(x)");
		owner.exec("INSERT INTO t VALUES (1)");
		await owner.synced();
		const before = await frames(s.path);

		const first = await reader(s.url, dir);
		expect(first.path).toContain(`${sep}reader-0${sep}`);
		expect(await first.child.rows("SELECT x FROM t")).toEqual([{ x: 1 }]);
		const st = await statusOf(first.child);
		expect(st).toMatchObject({ role: "reader", state: "live", epoch: null, attach: { local: "built" }, replication: { state: "none" } });

		const write = await first.child.send({ exec: "INSERT INTO t VALUES (9)" });
		expect(write).toMatchObject({ ok: false, errcode: SQLITE_READONLY });
		// A connection that asks for read-write gets a read-only db file all the same: no write, no
		// VACUUM and no checkpoint reaches the reader's files.
		for (const sql of ["INSERT INTO t VALUES (9)", "VACUUM", "PRAGMA wal_checkpoint(TRUNCATE)"]) {
			const refused = await first.child.send({ exec: sql, conn: 4 });
			expect(refused, sql).toMatchObject({ ok: false, errcode: SQLITE_READONLY });
		}
		expect(await first.child.rows("SELECT x FROM t")).toEqual([{ x: 1 }]);
		const started = Date.now();
		const synced = await first.child.send({ synced: 10_000 });
		expect(synced.synced).toMatchObject({ synced: true });
		expect(Date.now() - started).toBeLessThan(2_000);

		// The owner goes on unfenced, and the reader keeps the stream as of its open.
		owner.exec("INSERT INTO t VALUES (2)");
		await owner.synced();
		expect(owner.status().reset).toBeNull();
		expect(await first.child.rows("SELECT x FROM t ORDER BY x")).toEqual([{ x: 1 }]);
		const after = await frames(s.path);
		expect(after.claims).toBe(before.claims);
		expect(after.commits).toBe(before.commits + 1);

		// A second reader process takes reader-1 and reads the stream as of its own open.
		const second = await reader(s.url, dir);
		expect(second.path).toContain(`${sep}reader-1${sep}`);
		expect(await second.child.rows("SELECT x FROM t ORDER BY x")).toEqual([{ x: 1 }, { x: 2 }]);
		await second.child.stop();
		await first.child.stop();
		// A reader cache's page 1 is marked like the owner's: SQLite outside the extension (this
		// process has no binding of the file) finds no database in it.
		const outside = new DatabaseSync(first.path, { readOnly: true });
		expect(thrown(() => outside.prepare("SELECT count(*) FROM sqlite_schema").all())).toMatchObject({ errcode: SQLITE_NOTADB });
		outside.close();

		// Once the first closed, the next reader takes reader-0 again and catches it up in place.
		owner.exec("INSERT INTO t VALUES (3)");
		await owner.synced();
		const again = await reader(s.url, dir);
		expect(again.path).toBe(first.path);
		expect((await statusOf(again.child)).attach.local).toBe("caught_up");
		expect(await again.child.rows("SELECT x FROM t ORDER BY x")).toEqual([{ x: 1 }, { x: 2 }, { x: 3 }]);
		await again.child.stop();
		expect((await frames(s.path)).claims).toBe(before.claims);
		expect(owner.status().reset).toBeNull();
		owner.close();
	});

	it("a reader opens a closed stream, which status says, and an owner's open refuses it", async () => {
		const s = stream("reader-closed");
		const dir = freshDir();
		const owner = await open(s.url, { dir });
		owner.exec("CREATE TABLE t(x); INSERT INTO t VALUES (1)");
		await owner.synced();
		expect(owner.status().closed).toBe(false);
		const ownerPath = owner.boundPath;
		owner.close();
		await retired(ownerPath);
		await closeStream(s.path);

		const reader = await open(s.url, { dir: freshDir(), readOnly: true });
		expect(reader.status()).toMatchObject({ role: "reader", closed: true, attach: { local: "built" } });
		expect(reader.prepare("SELECT x FROM t").all()).toEqual([{ x: 1 }]);
		await reader.synced();
		reader.close();
		const refused = await rejection(open(s.url, { dir }));
		expect(refused).toBeInstanceOf(UrsulaSetupError);
		expect(refused).toMatchObject({ kind: "stream_state" });
	});

	it("a read-only open of a missing stream is not_found", async () => {
		const s = stream("reader-missing");
		const error = await rejection(open(s.url, { dir: freshDir(), readOnly: true }));
		expect(error).toBeInstanceOf(UrsulaSetupError);
		expect(error).toMatchObject({ kind: "not_found", action: "fix" });
	});

	it("readOnly with recreate is invalid_argument", async () => {
		const s = stream("reader-recreate");
		const error = await rejection(open(s.url, { dir: freshDir(), readOnly: true, recreate: true }));
		expect(error).toBeInstanceOf(UrsulaSetupError);
		expect(error).toMatchObject({ kind: "invalid_argument", action: "fix" });
	});

	it("a read-only open in the owner's process reads the owner's binding and sees its latest writes", async () => {
		const s = stream("reader-beside");
		const dir = freshDir();
		const owner = await open(s.url, { dir });
		owner.exec("CREATE TABLE t(x)");
		owner.exec("INSERT INTO t VALUES (1)");
		const ro = await open(s.url, { dir, readOnly: true });
		expect(ro.boundPath).toBe(owner.boundPath);
		expect(ro.status().role).toBe("owner");
		expect(ro.prepare("SELECT x FROM t").all()).toEqual([{ x: 1 }]);
		// Not yet in the stream: the read-only connection reads the owner's own files.
		owner.exec("INSERT INTO t VALUES (2)");
		expect(ro.prepare("SELECT x FROM t ORDER BY x").all()).toEqual([{ x: 1 }, { x: 2 }]);
		const refused = thrown(() => ro.exec("INSERT INTO t VALUES (9)"));
		expect(refused).toMatchObject({ errcode: SQLITE_READONLY });
		await ro.synced();
		ro.close();
		// The owner is untouched by the read-only connection's close.
		owner.exec("INSERT INTO t VALUES (3)");
		await owner.synced();
		expect(owner.status()).toMatchObject({ role: "owner", state: "live", reset: null });
		const ownerPath = owner.boundPath;
		owner.close();
		await retired(ownerPath);

		// With no binding left in the process, a read-only open is a reader of its own, and its
		// next open, after more commits elsewhere, catches up.
		const reader = await open(s.url, { dir, readOnly: true });
		expect(reader.status()).toMatchObject({ role: "reader", attach: { local: "built" } });
		expect(reader.boundPath).toContain(`${sep}reader-0${sep}`);
		expect(reader.prepare("SELECT x FROM t ORDER BY x").all()).toEqual([{ x: 1 }, { x: 2 }, { x: 3 }]);
		const readerPath = reader.boundPath;
		reader.close();
		await retired(readerPath);
		const writer = new Child(s.url, freshDir());
		await writer.opened();
		await writer.exec("INSERT INTO t VALUES (4)");
		expect((await writer.send({ synced: 10_000 })).synced).toMatchObject({ synced: true });
		await writer.stop();
		const again = await open(s.url, { dir, readOnly: true });
		expect(again.boundPath).toBe(readerPath);
		expect(again.status()).toMatchObject({ role: "reader", attach: { local: "caught_up" } });
		expect(again.prepare("SELECT x FROM t ORDER BY x").all()).toEqual([{ x: 1 }, { x: 2 }, { x: 3 }, { x: 4 }]);
		await again.synced();
		again.close();
	});

	it("an unreachable stream opens an earlier reader cache as it is, and waits until the signal fires without one", async () => {
		const g = await gate(ursulaUrl());
		try {
			const s = stream("reader-unreachable");
			const url = `${g.url}${s.path}`;
			const dir = freshDir();
			const owner = await open(s.url, { dir });
			owner.exec("CREATE TABLE t(x)");
			owner.exec("INSERT INTO t VALUES (1)");
			await owner.synced();
			const first = await reader(url, dir);
			await first.child.stop();
			owner.exec("INSERT INTO t VALUES (2)");
			await owner.synced();

			await mode(g.control, "refuse");
			const started = Date.now();
			const offline = await reader(url, dir);
			expect(Date.now() - started).toBeLessThan(6_000);
			expect(offline.path).toBe(first.path);
			expect((await statusOf(offline.child)).attach.local).toBe("resumed");
			// The stream as of the reader's last open.
			expect(await offline.child.rows("SELECT x FROM t ORDER BY x")).toEqual([{ x: 1 }]);
			await offline.child.stop();

			// A stream this process does not own (it would read the owner's binding), with no cache.
			const other = `${g.url}${stream("reader-unreachable-none").path}`;
			const error = await rejection(open(other, { dir: freshDir(), readOnly: true, signal: AbortSignal.timeout(3_000) }));
			expect(error).toBeInstanceOf(UrsulaUnavailableError);
			expect(error).toMatchObject({ kind: "unreachable", action: "retry" });
			owner.close();
		} finally {
			await g.stop();
		}
	});
});

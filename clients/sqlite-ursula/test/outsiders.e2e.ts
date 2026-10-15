// SQLite outside the extension cannot open a cache, against the e2e server (test/stack.ts).
// On disk bytes 0..16 of a generation's db file hold the page-1 marker in place of SQLite's magic,
// and its WAL is `db-uwal`, so node:sqlite without the extension, the sqlite3 shell, `?vfs=unix` in
// the owner's process and a connection opened before the extension loaded all fail with
// SQLITE_NOTADB, and change no file of the generation. The owner keeps working throughout.
import { spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import { readdirSync, readFileSync, statSync, writeFileSync } from "node:fs";
import { dirname, join, relative } from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { Child, freshDir, killChildren, stream } from "./kit.ts";

const SQLITE_NOTADB = 26;
const SQLITE_IOERR_READ = 266;

afterEach(killChildren);

/** Every file under `dir` (the generation directory), by relative path, with its SHA-256. */
function hashes(dir: string): Record<string, string> {
	const out: Record<string, string> = {};
	const walk = (at: string): void => {
		for (const name of readdirSync(at)) {
			const path = join(at, name);
			if (statSync(path).isDirectory()) walk(path);
			else out[relative(dir, path)] = createHash("sha256").update(readFileSync(path)).digest("hex");
		}
	};
	walk(dir);
	return out;
}

/** Asserts the generation directory holds what it held, and never a `db-wal` or `db-journal`. */
function unchanged(dir: string, before: Record<string, string>, label: string): void {
	const now = hashes(dir);
	expect(Object.keys(now), label).not.toContain("db-wal");
	expect(Object.keys(now), label).not.toContain("db-journal");
	expect(now, label).toEqual(before);
}

/** A schema read of `path` by node:sqlite without the extension, in a process of its own. */
function plainNode(path: string, readOnly: boolean): { ok: boolean; errcode?: number; error?: string } {
	const script = `
		import { DatabaseSync } from "node:sqlite";
		const db = new DatabaseSync(process.argv[1], { readOnly: ${readOnly} });
		try {
			db.prepare("SELECT count(*) FROM sqlite_schema").get();
			console.log(JSON.stringify({ ok: true }));
		} catch (error) {
			console.log(JSON.stringify({ ok: false, errcode: error.errcode, error: String(error) }));
		} finally {
			db.close();
		}`;
	const r = spawnSync(process.execPath, ["--no-warnings", "--input-type=module", "-e", script, path], { encoding: "utf8" });
	expect(r.status, r.stderr).toBe(0);
	return JSON.parse(r.stdout.trim()) as { ok: boolean; errcode?: number; error?: string };
}

/** Whether the sqlite3 shell is on PATH. */
const sqlite3 = spawnSync("sqlite3", ["-version"], { encoding: "utf8" }).status === 0;

/** An owner that bound `url` in a fresh cache, created t and synced: it then stays idle. */
async function idleOwner(url: string): Promise<{ owner: Child; path: string; gen: string; dir: string }> {
	const dir = freshDir();
	const owner = new Child(url, dir);
	const opened = await owner.opened();
	await owner.exec("CREATE TABLE t(x)");
	await owner.exec("INSERT INTO t VALUES (1)");
	expect(await owner.send({ synced: 10_000 })).toMatchObject({ ok: true, synced: { synced: true } });
	return { owner, path: opened.path, gen: dirname(opened.path), dir };
}

describe("SQLite outside the extension", () => {
	it("finds no database in a cache, and changes none of its files", async () => {
		const s = stream("outsiders");
		const { owner, path, gen } = await idleOwner(s.url);
		expect(readdirSync(gen).sort()).toEqual(expect.arrayContaining(["db", "db-uwal", "log"]));
		expect(readFileSync(path).subarray(0, 8).toString()).toBe("Ursula08");
		const before = hashes(gen);

		// node:sqlite without the extension, in another process. `SELECT 1` reads no page.
		for (const readOnly of [false, true]) {
			const plain = plainNode(path, readOnly);
			expect(plain, JSON.stringify(plain)).toMatchObject({ ok: false, errcode: SQLITE_NOTADB });
			unchanged(gen, before, `node:sqlite, readOnly ${readOnly}`);
		}

		// The sqlite3 shell.
		if (sqlite3) {
			const r = spawnSync("sqlite3", ["-batch", "-bail", "-init", "/dev/null", path, "SELECT count(*) FROM sqlite_schema"], { encoding: "utf8" });
			expect(r.status).not.toBe(0);
			expect(r.stderr).toMatch(/file is not a database/);
			unchanged(gen, before, "sqlite3");
		} else {
			console.warn("sqlite3 is not on PATH: its case is skipped");
		}

		// `?vfs=unix` in the owner's own process, beside its bound connections.
		const unix = await owner.send({ outside: "unix" });
		if (unix.uri === true) {
			expect(unix, JSON.stringify(unix)).toMatchObject({ ok: false, errcode: SQLITE_NOTADB });
			unchanged(gen, before, "?vfs=unix");
		} else {
			console.warn(`this Node opens "file:" names as file names, not URIs: the ?vfs=unix case is skipped (${JSON.stringify(unix)})`);
		}

		// The owner never noticed: it commits, and synced() resolves with no reset.
		await owner.exec("INSERT INTO t VALUES (2)");
		expect(await owner.send({ synced: 10_000 })).toMatchObject({ ok: true, synced: { synced: true } });
		expect(await owner.rows("SELECT x FROM t ORDER BY x")).toEqual([{ x: 1 }, { x: 2 }]);
		expect(((await owner.send({ status: true })).status as { reset: unknown }).reset).toBeNull();

		// SQLite's magic written over the marker while bound: the next connection's read of page 1
		// finds no marker, and the binding resets `local_damage`.
		const db = readFileSync(path);
		db.write("SQLite format 3\0", 0, "latin1");
		writeFileSync(path, db);
		const fresh = await owner.send({ all: "SELECT x FROM t", conn: 2 });
		expect(fresh, JSON.stringify(fresh)).toMatchObject({ ok: false, errcode: SQLITE_IOERR_READ });
		const status = (await owner.send({ status: true })).status as { reset: { kind: string } | null };
		expect(status.reset).toMatchObject({ kind: "local_damage" });
		await owner.stop();
	});

	it("refuses a connection opened before the extension loaded, while the bound one works", async () => {
		const s = stream("preload");
		const { owner, path, gen, dir } = await idleOwner(s.url);
		await owner.stop();
		const preload = new Child(s.url, dir, {}, "preload-child.mjs", [path]);
		const got = (await preload.first()) as {
			before: { ok: boolean; errcode?: number };
			after: { ok: boolean; errcode?: number };
			rows: unknown[];
			synced: { synced: boolean };
			path: string;
		};
		expect(got.path).toBe(path);
		expect(got.before, JSON.stringify(got)).toMatchObject({ ok: false, errcode: SQLITE_NOTADB });
		expect(got.after, JSON.stringify(got)).toMatchObject({ ok: false, errcode: SQLITE_NOTADB });
		expect(got.rows).toEqual([{ x: 1 }, { x: 2 }]);
		expect(got.synced.synced).toBe(true);
		await preload.exit;
		await preload.stop();
		const files = readdirSync(gen);
		expect(files).not.toContain("db-wal");
		expect(files).not.toContain("db-journal");
	});
});

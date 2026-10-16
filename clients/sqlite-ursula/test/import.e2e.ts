// `ursula_import` (clients/sqlite-vfs/src/import.rs): a plain SQLite file becomes the first snapshot
// of a new stream. Against the e2e server (test/stack.ts), with a cold store (the model's set for
// the test, the real server's in memory):
// - A plain database of a few MiB, written by `VACUUM INTO`, imported under a 64 KiB request limit
//   (so its snapshot goes up in parts), opens in a fresh cache root with the same rows, an
//   integrity check that passes, the snapshot as where it was built from, and an epoch above the
//   import's. A commit then reaches `synced()`.
// - A second import into that stream fails with `stream_state` and changes nothing.
// - A file with 8192-byte pages, or with a `-wal` beside it, is refused before anything is sent:
//   the stream is never created.
import { existsSync } from "node:fs";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import { afterEach, describe, expect, it } from "vitest";
import { Child, directUrl, freshDir, killChildren, onModel, stream } from "./kit.ts";

afterEach(killChildren);

const ROWS_SQL = "SELECT i, length(b) AS n, hex(substr(b, 1, 64)) AS head, hex(substr(b, -64)) AS tail FROM t ORDER BY i";

/** A plain database at `<dir>/<name>`, written by `VACUUM INTO` from one of `rows` rows of 64 KiB of random bytes, in pages of `pageSize`. */
function plainDb(dir: string, name: string, rows: number, pageSize = 4096): string {
	const source = new DatabaseSync(join(dir, `${name}.source`));
	source.exec(`PRAGMA page_size = ${pageSize}; CREATE TABLE t(i INTEGER PRIMARY KEY, b BLOB)`);
	const values = Array.from({ length: rows }, (_, k) => `(${k + 1}, randomblob(65536))`).join(", ");
	source.exec(`INSERT INTO t VALUES ${values}`);
	const file = join(dir, name);
	source.prepare("VACUUM INTO ?").run(file);
	source.close();
	return file;
}

function rowsOf(file: string): unknown[] {
	const db = new DatabaseSync(file, { readOnly: true });
	try {
		return db
			.prepare(ROWS_SQL)
			.all()
			.map((row) => ({ ...row }));
	} finally {
		db.close();
	}
}

interface Imported {
	offset: number;
	digest: string;
	pages: number;
	bytes: number;
}

/** Runs `ursula_import` of `file` into `url` in a process of its own, with `env`. */
async function runImport(url: string, file: string, env: NodeJS.ProcessEnv = {}): Promise<Record<string, unknown>> {
	const c = new Child(url, file, env, "import-child.mjs");
	const reply = await c.first();
	await c.stop();
	return reply;
}

/** The kind of a failed import's error: `ursula_import: <kind>: ...`. */
function kindOf(reply: Record<string, unknown>): string | undefined {
	expect(reply.ok, JSON.stringify(reply)).toBe(false);
	return /ursula_import: ([a-z_]+): /.exec(String(reply.error))?.[1];
}

/** The stream's HEAD: its status, tail and snapshot. */
async function head(path: string): Promise<{ status: number; tail: string | null; snapshot: string | null }> {
	const r = await fetch(`${directUrl()}${path}`, { method: "HEAD" });
	return { status: r.status, tail: r.headers.get("stream-next-offset"), snapshot: r.headers.get("stream-snapshot-offset") };
}

/** On the model, a cold store for the test's run (`fn`): snapshots in parts. */
async function withColdStore<T>(fn: () => Promise<T>): Promise<T> {
	if (!onModel()) return fn();
	const set = async (cold: "cold" | "none"): Promise<void> => {
		const r = await fetch(`${directUrl()}/__model/limits?max_request_bytes=${32 << 20}&max_snapshot_bytes=${cold}`, { method: "PUT" });
		expect(r.status).toBe(204);
	};
	await set("cold");
	try {
		return await fn();
	} finally {
		await set("none");
	}
}

describe("ursula_import", () => {
	it("imports a plain database as the first snapshot of a new stream, in parts under a 64 KiB limit", async () => {
		await withColdStore(async () => {
			const s = stream("import");
			const file = plainDb(freshDir(), "plain.db", 48);
			const expected = rowsOf(file);
			expect((await head(s.path)).status).toBe(404);
			const reply = await runImport(s.url, file, { URSULA_VFS_MAX_REQUEST_BYTES: "65536" });
			expect(reply.ok, JSON.stringify(reply)).toBe(true);
			const imported = reply.imported as Imported;
			expect(imported.offset).toBeGreaterThan(0);
			// Above the request limit: only parts could carry it.
			expect(imported.bytes).toBeGreaterThan(65536);
			expect(await head(s.path)).toEqual({ status: 200, tail: expect.any(String), snapshot: expect.any(String) });
			expect(rowsOf(file), "the source is unchanged").toEqual(expected);

			const owner = new Child(s.url, freshDir());
			await owner.opened();
			expect(await owner.rows(ROWS_SQL)).toEqual(expected);
			expect(await owner.rows("PRAGMA integrity_check")).toEqual([{ integrity_check: "ok" }]);
			expect((await owner.send({ synced: 60_000 })).synced).toMatchObject({ synced: true });
			const status = (await owner.send({ status: true })).status as { epoch: number | null; attach: { snapshot_offset: number | null } };
			expect(status.attach.snapshot_offset).toBe(imported.offset);
			expect(status.epoch).toBeGreaterThanOrEqual(2);
			await owner.exec("INSERT INTO t VALUES (1000, x'01')");
			expect((await owner.send({ synced: 60_000 })).synced).toMatchObject({ synced: true });
			await owner.stop();

			const settled = await head(s.path);
			const again = await runImport(s.url, file);
			expect(kindOf(again)).toBe("stream_state");
			expect(await head(s.path)).toEqual(settled);
		});
	});

	it("refuses a file with 8192-byte pages before anything is sent", async () => {
		const s = stream("import-8k");
		const file = plainDb(freshDir(), "big-pages.db", 2, 8192);
		expect(kindOf(await runImport(s.url, file))).toBe("invalid_argument");
		expect((await head(s.path)).status).toBe(404);
	});

	it("refuses a file with a -wal beside it before anything is sent", async () => {
		const s = stream("import-wal");
		const file = join(freshDir(), "live.db");
		const live = new DatabaseSync(file);
		try {
			live.exec("PRAGMA journal_mode = WAL; CREATE TABLE t(x); INSERT INTO t VALUES (1)");
			expect(existsSync(`${file}-wal`)).toBe(true);
			const reply = await runImport(s.url, file);
			expect(kindOf(reply)).toBe("invalid_argument");
			expect(String(reply.error)).toContain("-wal");
		} finally {
			live.close();
		}
		expect((await head(s.path)).status).toBe(404);
	});
});

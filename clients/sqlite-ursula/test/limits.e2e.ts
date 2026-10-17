// The commit limit (design: Capture), against the e2e server (test/stack.ts): under
// URSULA_VFS_MAX_COMMIT_BYTES=1048576 a transaction of about 2 MiB of pages fails alone at its
// COMMIT as `too_large` (SQLITE_IOERR_AUTH, `fix`), nothing of it logged or sent, and the database
// stays usable: the next transaction below the limit commits, and a fresh build equals it. With a
// small page cache the refused transaction spills frames into the WAL first: it still rolls back
// whole, and the next commit writes over the spilled frames.
import { afterEach, describe, expect, it } from "vitest";
import { open, type UrsulaDatabase, UrsulaWriteRefusedError } from "../src/index.ts";
import { freshDir, frames, fromStream, insert, parts, ROWS_SQL, stream, thrown } from "./kit.ts";

// The extension reads the setting once per process, at the first open: set it before any open.
// Each test file runs in its own process.
process.env.URSULA_VFS_MAX_COMMIT_BYTES ??= String(1 << 20);

const opened = new Set<UrsulaDatabase>();
afterEach(() => {
	for (const d of opened) if (d.isOpen) d.close();
	opened.clear();
});

/** A ~2 MiB transaction refused as `too_large`, then a ~512 KiB one committed, after `setup` on a fresh database. */
async function refusedThenCommitted(label: string, setup: string): Promise<void> {
	const s = stream(label);
	const db = await open(s.url, { dir: freshDir() });
	opened.add(db);
	db.exec("CREATE TABLE t(i INTEGER PRIMARY KEY, b BLOB)");
	await db.synced();
	if (setup !== "") db.exec(setup);
	const before = db.status();
	const sent = await frames(s.path);

	const refused = thrown(() => db.exec(insert(1, 32)));
	expect(refused).toBeInstanceOf(UrsulaWriteRefusedError);
	expect(refused).toMatchObject({ kind: "too_large", action: "fix", errcode: 7178 });
	expect((refused as Error).message).toContain("split the transaction, or raise URSULA_VFS_MAX_COMMIT_BYTES");
	const after = db.status();
	expect(after.refused).toMatchObject({ kind: "too_large", action: "fix" });
	expect(after.reset).toBeNull();
	expect(after.commits.logged).toBe(before.commits.logged);
	expect(db.prepare("SELECT count(*) AS n FROM t").get()).toEqual({ n: 0 });

	db.exec(insert(1, 8));
	await db.synced();
	expect(db.status().refused).toBeNull();
	const local = db.prepare(ROWS_SQL).all();
	expect(local).toHaveLength(8);
	db.close();
	expect(await frames(s.path)).toEqual({ claims: sent.claims, commits: sent.commits + 1 });
	expect(await parts(s.path)).toBe(0);
	expect(await fromStream(s.url, ROWS_SQL)).toEqual(local);
}

describe("the commit limit", () => {
	it("refuses a transaction above the commit limit alone, as too_large, and commits the next", async () => {
		await refusedThenCommitted("too-large", "");
	});

	it("rolls a refused transaction that spilled back whole, and the next commit writes over its frames", async () => {
		// 16 pages of cache: the ~2 MiB transaction spills most of its pages into the WAL before its COMMIT.
		await refusedThenCommitted("too-large-spilled", "PRAGMA cache_size = 16");
	});
});

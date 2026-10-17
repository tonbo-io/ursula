// The commit limit (design: Capture), against the e2e server (test/stack.ts): under
// URSULA_VFS_MAX_COMMIT_BYTES=1048576 a transaction of about 2 MiB of pages fails alone at its
// COMMIT as `too_large` (SQLITE_IOERR_AUTH, `fix`), nothing of it logged or sent, and the database
// stays usable: the next transaction below the limit commits, and a fresh build equals it.
import { afterEach, describe, expect, it } from "vitest";
import { open, type UrsulaDatabase, UrsulaWriteRefusedError } from "../src/index.ts";
import { freshDir, frames, fromStream, insert, parts, ROWS_SQL, stream, thrown } from "./kit.ts";

// Read once, when the extension first needs it: before it loads. Each test file runs in its own
// process.
process.env.URSULA_VFS_MAX_COMMIT_BYTES ??= String(1 << 20);

const opened = new Set<UrsulaDatabase>();
afterEach(() => {
	for (const d of opened) if (d.isOpen) d.close();
	opened.clear();
});

describe("the commit limit", () => {
	it("refuses a transaction above the commit limit alone, as too_large, and commits the next", async () => {
		const s = stream("too-large");
		const db = await open(s.url, { dir: freshDir() });
		opened.add(db);
		db.exec("CREATE TABLE t(i INTEGER PRIMARY KEY, b BLOB)");
		await db.synced();
		const before = db.status();
		const sent = await frames(s.path);

		const refused = thrown(() => db.exec(insert(1, 32)));
		expect(refused).toBeInstanceOf(UrsulaWriteRefusedError);
		expect(refused).toMatchObject({ kind: "too_large", action: "fix", errcode: 7178 });
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
	});
});

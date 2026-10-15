// The crash matrix (design §5.7 and §12, M1 exit criterion), against the e2e server
// (test/stack.ts): a process committing transactions of several rows each (test/crash-child.mjs) is
// killed with SIGKILL by URSULA_VFS_ABORT_AT (config::Step) at every step of the commit path
// (§4.1), of F1 (a checkpoint, §4.3) and of F2 (a new WAL generation, and the WAL truncated to
// nothing, §4.3). After each crash the next open of the same cache root must need no manual step,
// resume the same generation (no salvage, no rebuild, no reset), hold every transaction up to the
// last one the application saw commit, or that one plus the one the crash interrupted once it was
// logged, whole, catch the stream up, and equal a fresh build from the stream.
import { afterEach, describe, expect, it } from "vitest";
import { Child, fromStream, freshDir, killChildren, stream } from "./kit.ts";

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

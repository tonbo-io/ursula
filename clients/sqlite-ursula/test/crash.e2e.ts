// The crash matrix (design §5.7, M1 exit criterion), against the model server (E2E_SERVER=model):
// a process committing one row per transaction is aborted by URSULA_VFS_ABORT_AT (config::Step) at
// every step of the commit path (§4.1), of F1 (a checkpoint, §4.3) and of F2 (a new WAL generation,
// §4.3). After each crash the next open of the same cache root must need no manual step, hold the
// rows up to the last commit the application saw succeed, or that one plus the commit the crash
// interrupted once it was logged, catch the stream up, and equal a fresh build from the stream.
import { afterEach, describe, expect, it } from "vitest";
import { Child, fromStream, freshDir, killChildren, onModel, stream } from "./kit.ts";

afterEach(killChildren);

/** The rows the crash leaves: the last committed row (`last`), the one after it (`next`), or either. */
type Expect = "last" | "next" | "either";

/**
 * Each step, the occurrences it aborts at (`<step>:<n>`), and where the reopen must land. The first
 * write transaction of the child creates the table, so the commit path's `n` counts it.
 * - Before the COMMIT record (admission, capture, encode): the interrupted commit never happened.
 * - From the record on (logged, forward, phasetwo): the log replays it, so it is there.
 * - F1 runs in the autocheckpoint after a commit returned to SQLite: that commit is logged.
 * - F2 runs at the first WAL write of the next transaction: that one is not. Its first occurrence
 *   is the new WAL of the transaction that creates the table, so the matrix starts at the second.
 * The hook aborts (SIGABRT): like SIGKILL, nothing of the process runs after it.
 */
const MATRIX: readonly (readonly [step: string, occurrences: readonly number[], expect: Expect])[] = [
	["admission", [2, 3, 8, 21], "last"],
	["capture", [2, 3, 8, 21], "last"],
	["encode", [2, 3, 8, 21], "last"],
	["logged", [2, 3, 8, 21], "next"],
	["forward", [2, 3, 8, 21], "next"],
	["phasetwo", [2, 3, 8, 21], "next"],
	["checkpoint_start", [1, 2, 6], "next"],
	["checkpoint_synced", [1, 2, 6], "next"],
	["checkpoint_page", [1, 2, 6], "next"],
	["base_start", [2, 3, 7], "last"],
	["base_synced", [2, 3, 7], "last"],
	["based", [2, 3, 7], "last"],
];

/** Rows the child commits at most: far past every abort point. */
const ROWS = 40;
/** `PRAGMA wal_autocheckpoint`: a 3000-byte row takes one page, so a checkpoint every few commits. */
const AUTOCHECKPOINT = 4;
const ROWS_SQL = "SELECT i, hex(pad) AS pad FROM t ORDER BY i";

describe.skipIf(!onModel())("crash matrix (URSULA_VFS_ABORT_AT)", () => {
	for (const [step, occurrences, expected] of MATRIX) {
		for (const n of occurrences) {
			it(`${step}:${n}`, async () => {
				const s = stream(`crash-${step}-${n}`);
				const dir = freshDir();
				const doomed = new Child(s.url, dir, { URSULA_VFS_ABORT_AT: `${step}:${n}` }, "crash-child.mjs", [String(ROWS), String(AUTOCHECKPOINT)]);
				let last = 0;
				let ready = false;
				for (;;) {
					const line = await doomed.line();
					if (line === undefined) break;
					if (line.ready === true) ready = true;
					if (typeof line.committed === "number") last = line.committed;
					expect(line.done, `${step}:${n} never aborted`).toBeUndefined();
				}
				const { signal } = await doomed.exit;
				expect(signal, `the child ended without the abort at ${step}:${n}`).toBe("SIGABRT");
				expect(ready, "the abort came before the table was created and synced").toBe(true);

				// The next open of the cache: no manual step.
				const reopened = new Child(s.url, dir);
				await reopened.opened();
				const [{ count, max }] = (await reopened.rows("SELECT count(*) AS count, coalesce(max(i), 0) AS max FROM t")) as [{ count: number; max: number }];
				expect(count, "rows 1 to max, none missing").toBe(max);
				if (expected === "last") expect(max).toBe(last);
				else if (expected === "next") expect(max).toBe(last + 1);
				else expect([last, last + 1]).toContain(max);
				expect(await reopened.rows("PRAGMA integrity_check")).toEqual([{ integrity_check: "ok" }]);
				const local = await reopened.rows(ROWS_SQL);
				expect((await reopened.send({ synced: 20_000 })).synced).toMatchObject({ synced: true });
				// It goes on committing.
				await reopened.exec(`INSERT INTO t VALUES (${max + 1}, x'00')`);
				expect((await reopened.send({ synced: 20_000 })).synced).toMatchObject({ synced: true });
				await reopened.stop();
				const built = await fromStream(s.url, ROWS_SQL);
				expect(built.slice(0, -1)).toEqual(local);
				expect(built.at(-1)).toEqual({ i: max + 1, pad: "00" });
			});
		}
	}
});

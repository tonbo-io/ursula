// Test 3: two processes on two files of one stream. B catches up and commits; A's next commit fails
// with SQLITE_IOERR and never reaches A's file; A is poisoned until re-attached; B is unaffected.
import { expect, it } from "vitest";
import { attach } from "../../src/vfs.ts";
import { freshFile } from "../helpers.ts";
import { integrity, openPlain, runChild, streamPath, ursulaUrl, walContains } from "./kit.ts";

const xs = (db: ReturnType<typeof openPlain>): string[] => (db.prepare("SELECT x FROM t ORDER BY x").all() as { x: string }[]).map((r) => r.x);
const attempt = (f: () => void): string => {
	try {
		f();
		return "ok";
	} catch (error) {
		const e = error as { message: string; errcode?: number; errstr?: string };
		return `${e.message} (errcode ${e.errcode})`;
	}
};

it("a stale writer's commit fails, stays out of its file, and the other writer is unaffected", async () => {
	const url = ursulaUrl() + streamPath();
	const fileA = freshFile();
	const fileB = freshFile();
	attach(fileA, url);
	const a = openPlain(fileA);
	a.exec("CREATE TABLE t(x TEXT)");
	a.exec("INSERT INTO t VALUES ('a1')");

	const b = runChild(fileB, url, ["INSERT INTO t VALUES ('b1')"], { CHILD_EXIT: "1" });
	expect((await b.exited).code).toBe(0);
	expect(b.lines.find((l) => l.attached !== undefined)?.attached).toBe(2);
	expect(b.lines.find((l) => l.step === 0 && l.ok !== undefined)?.ok).toBe(true);

	const failed = attempt(() => a.exec("INSERT INTO t VALUES ('a2')"));
	console.log(`A autocommit after B: ${failed}`);
	expect(failed).toMatch(/disk I\/O error/);
	expect(xs(a)).toEqual(["a1"]);
	expect(walContains(fileA, "a2")).toBe(false);
	// Connection state after a failed commit, with an explicit transaction.
	const begin = attempt(() => a.exec("BEGIN IMMEDIATE"));
	const insert = attempt(() => a.exec("INSERT INTO t VALUES ('a3')"));
	const commit = attempt(() => a.exec("COMMIT"));
	const inTx = a.isTransaction;
	const rollback = attempt(() => a.exec("ROLLBACK"));
	console.log(`A poisoned, explicit tx: BEGIN ${begin}; INSERT ${insert}; COMMIT ${commit}; isTransaction after COMMIT ${inTx}; ROLLBACK ${rollback}`);
	expect(commit).toMatch(/disk I\/O error/);
	expect(xs(a)).toEqual(["a1"]);
	expect(integrity(a)).toBe("ok");
	a.close();

	// B's file (reopened here) and a fresh rebuild both hold a1, b1 — and nothing of A's attempts.
	expect(attach(fileB, url)).toBe(3);
	const rb = openPlain(fileB);
	expect(xs(rb)).toEqual(["a1", "b1"]);
	expect(integrity(rb)).toBe("ok");
	rb.close();
	const fresh = freshFile();
	attach(fresh, url);
	const rf = openPlain(fresh);
	expect(xs(rf)).toEqual(["a1", "b1"]);
	rf.close();

	// A re-attached catches up and can write again.
	expect(attach(fileA, url)).toBe(3);
	const a2 = openPlain(fileA);
	expect(xs(a2)).toEqual(["a1", "b1"]);
	a2.exec("INSERT INTO t VALUES ('a4')");
	expect(xs(a2)).toEqual(["a1", "a4", "b1"]);
	a2.close();
});

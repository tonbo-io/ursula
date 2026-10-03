// Producer fencing: a new owner's claim (a new producer epoch) fences every earlier owner.
import { expect, it } from "vitest";
import { attach, openUrsulaPiStorage, status, UrsulaReplicationError } from "../src/index.ts";
import { openHarness } from "./harness-kit.ts";
import { ctx, freshFile } from "./helpers.ts";
import { integrity, openPlain, runChild, streamPath, ursulaUrl, walContains } from "./kit.ts";

const xs = (db: ReturnType<typeof openPlain>): string[] => (db.prepare("SELECT x FROM t ORDER BY x").all() as { x: string }[]).map((r) => r.x);
const attempt = (f: () => void): string => {
	try {
		f();
		return "ok";
	} catch (error) {
		const e = error as { message: string; errcode?: number };
		return `${e.message} (errcode ${e.errcode})`;
	}
};

it("a fenced writer's commit fails, stays out of its file, and the new owner is unaffected", async () => {
	const url = ursulaUrl() + streamPath();
	const fileA = freshFile();
	const fileB = freshFile();
	attach(fileA, url);
	const a = openPlain(fileA);
	a.exec("CREATE TABLE t(x TEXT)");
	a.exec("INSERT INTO t VALUES ('a1')");

	const b = runChild(fileB, url, ["INSERT INTO t VALUES ('b1')"], { CHILD_EXIT: "1" });
	expect((await b.exited).code).toBe(0);
	expect(b.lines.find((l) => l.step === 0 && l.ok !== undefined)?.ok).toBe(true);

	expect(attempt(() => a.exec("INSERT INTO t VALUES ('a2')"))).toMatch(/disk I\/O error/);
	expect(status(fileA)).toMatchObject({ poisoned: true, fenced: true });
	expect(xs(a)).toEqual(["a1"]);
	expect(walContains(fileA, "a2")).toBe(false);
	expect(integrity(a)).toBe("ok");
	a.close();

	// A fresh rebuild holds a1, b1 and nothing of A's attempt; A re-attached catches up and writes again.
	const fresh = freshFile();
	attach(fresh, url);
	const rf = openPlain(fresh);
	expect(xs(rf)).toEqual(["a1", "b1"]);
	rf.close();
	attach(fileA, url);
	const a2 = openPlain(fileA);
	expect(xs(a2)).toEqual(["a1", "b1"]);
	a2.exec("INSERT INTO t VALUES ('a3')");
	expect(xs(a2)).toEqual(["a1", "a3", "b1"]);
	a2.close();
});

it("Pi: a fenced commit rejects with UrsulaReplicationError", async () => {
	const url = ursulaUrl() + streamPath();
	const stale = await openUrsulaPiStorage(freshFile(), url);
	const owner = await openUrsulaPiStorage(freshFile(), url);
	const error = await openHarness(stale).then(
		() => undefined,
		(e: unknown) => e,
	);
	expect(error).toBeInstanceOf(UrsulaReplicationError);
	expect((error as UrsulaReplicationError).fenced).toBe(true);
	const { harness } = await openHarness(owner); // the new owner commits
	await harness.close(ctx);
	await stale.close(ctx);
	await owner.close(ctx);
});

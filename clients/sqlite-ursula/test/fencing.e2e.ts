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

// Regression (#345): owners appended under one Producer-Id whatever the stream's incarnation, so an
// owner of a deleted stream could keep committing into the stream recreated at its path.
it("an owner of a deleted stream is fenced at its next commit; nothing of it reaches the recreated stream", async () => {
	const url = ursulaUrl() + streamPath();
	const fileA = freshFile();
	attach(fileA, url);
	const a = openPlain(fileA);
	a.exec("CREATE TABLE t(x TEXT)");
	a.exec("INSERT INTO t VALUES ('a1')");
	expect((await fetch(url, { method: "DELETE" })).ok).toBe(true);
	const fileB = freshFile();
	attach(fileB, url);
	const b = openPlain(fileB);
	b.exec("CREATE TABLE t(x TEXT)");
	b.exec("INSERT INTO t VALUES ('b1')");
	b.close();
	const tail = async () => (await fetch(url, { method: "HEAD" })).headers.get("stream-next-offset");
	const before = await tail();
	expect(attempt(() => a.exec("INSERT INTO t VALUES ('a2')"))).toMatch(/disk I\/O error/);
	expect(status(fileA)).toMatchObject({ poisoned: true, fenced: true });
	expect(status(fileA).reason).toMatch(/deleted and recreated/);
	a.close();
	expect(await tail()).toBe(before);
});

// Offsets are opaque, so the VFS does not check where its frame landed. Instead every commit carries
// a Stream-Seq above every earlier commit's: a writer outside the protocol that appends with a higher
// one fences the owner at its next commit (one without Stream-Seq goes unnoticed).
it("a foreign append with a higher Stream-Seq fences the owner at its next commit", async () => {
	const url = ursulaUrl() + streamPath();
	const file = freshFile();
	attach(file, url);
	const a = openPlain(file);
	a.exec("CREATE TABLE t(x TEXT)");
	const foreign = await fetch(url, { method: "POST", headers: { "content-type": "application/octet-stream", "stream-seq": "~" }, body: "foreign" });
	expect(foreign.ok).toBe(true);
	expect(attempt(() => a.exec("INSERT INTO t VALUES ('a1')"))).toMatch(/disk I\/O error/);
	expect(status(file)).toMatchObject({ poisoned: true, fenced: true });
	expect(status(file).reason).toMatch(/Stream-Seq/);
	expect(xs(a)).toEqual([]);
	a.close();
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

// Regression (#324 review): two owners claiming the same epoch both got a 2xx (the second as a
// duplicate of the first's claim) and both believed they owned it. A claim is verified by its nonce.
it("a claim answered as a duplicate of another owner's claim is lost, and the owner claims higher", async () => {
	const url = ursulaUrl() + streamPath();
	const fileA = freshFile();
	const a = await openUrsulaPiStorage(fileA, url);
	const epochA = status(fileA).epoch;
	const fileB = freshFile();
	const b = runChild(fileB, url, ["CREATE TABLE b(x TEXT)", "INSERT INTO b VALUES ('b1')"], { URSULA_VFS_FIRST_CLAIM_EPOCH: String(epochA), CHILD_EXIT: "1" });
	const done = await b.waitFor((l) => l.done === true);
	expect((await b.exited).code).toBe(0);
	expect(done).toMatchObject({ poisoned: false, epoch: epochA + 1 });
	const error = await openHarness(a).then(
		() => undefined,
		(e: unknown) => e,
	);
	expect(error).toBeInstanceOf(UrsulaReplicationError);
	expect((error as UrsulaReplicationError).fenced).toBe(true);
	await a.close(ctx);
	const fresh = freshFile();
	attach(fresh, url);
	const r = openPlain(fresh);
	expect(r.prepare("SELECT x FROM b").all()).toEqual([{ x: "b1" }]);
	r.close();
});

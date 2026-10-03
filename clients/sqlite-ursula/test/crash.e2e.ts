// The commit barrier: SIGKILL/abort around it in a child process, the cache-spill case, and an append
// whose outcome is unknown.
import { afterAll, beforeAll, beforeEach, expect, it } from "vitest";
import { attach, drainStats, status } from "../src/index.ts";
import { freshFile } from "./helpers.ts";
import { integrity, openPlain, runChild, StallProxy, streamPath, ursulaUrl, walContains } from "./kit.ts";

const SQL = ["CREATE TABLE t(x TEXT)", "INSERT INTO t VALUES ('M-one')", "INSERT INTO t VALUES ('M-two')"];
const rows = (file: string, where = ""): string[] => {
	const db = openPlain(file);
	try {
		expect(integrity(db)).toBe("ok");
		return (db.prepare(`SELECT x FROM t ${where} ORDER BY x`).all() as { x: string }[]).map((r) => r.x);
	} finally {
		db.close();
	}
};

let proxy: StallProxy;
beforeAll(async () => {
	proxy = await StallProxy.start(ursulaUrl());
});
beforeEach(() => proxy.reset());
afterAll(async () => {
	await proxy.close();
});

it("(a) killed while the append is in flight, before Ursula has it: absent, and the local WAL holds nothing of it", async () => {
	const path = streamPath();
	const file = freshFile();
	proxy.stallAfter = 3; // the claim, CREATE and M-one go through; M-two's append is held
	const child = runChild(file, proxy.url + path, SQL);
	await proxy.stalled;
	await child.waitFor((l) => l.step === 2 && l.phase === "start");
	child.proc.kill("SIGKILL");
	expect((await child.exited).signal).toBe("SIGKILL");
	expect(walContains(file, "M-one")).toBe(true);
	expect(walContains(file, "M-two")).toBe(false);
	attach(file, ursulaUrl() + path);
	expect(rows(file)).toEqual(["M-one"]);
});

it("(b) killed after the ack, before the local WAL write: present after re-attach (replayed)", async () => {
	const path = streamPath();
	const file = freshFile();
	const child = runChild(file, ursulaUrl() + path, SQL, { URSULA_VFS_ABORT_AFTER_ACK: "3" });
	const exit = await child.exited;
	expect(exit.signal).toBe("SIGABRT");
	expect(child.lines.some((l) => l.step === 2 && l.phase === "start")).toBe(true);
	expect(walContains(file, "M-one")).toBe(true);
	expect(walContains(file, "M-two")).toBe(false);
	attach(file, ursulaUrl() + path);
	expect(rows(file)).toEqual(["M-one", "M-two"]);
});

it("(c) killed after the commit: present", async () => {
	const path = streamPath();
	const file = freshFile();
	const child = runChild(file, ursulaUrl() + path, SQL);
	await child.waitFor((l) => l.done === true);
	expect(child.lines.filter((l) => l.ok === true)).toHaveLength(3);
	child.proc.kill("SIGKILL");
	await child.exited;
	expect(walContains(file, "M-two")).toBe(true);
	attach(file, ursulaUrl() + path);
	expect(rows(file)).toEqual(["M-one", "M-two"]);
});

// Regression (#322 review, P1-1): after a spilling transaction's commit frame, SQLite rewrites the
// checksums of its earlier frames; those writes stayed buffered while the sidecar advanced, so after
// a crash SQLite rejected the local WAL and attach skipped the stream record: the commit was lost.
it("(d) killed right after a spilling commit with in-place rewrites: present, locally and rebuilt", async () => {
	const path = streamPath();
	const file = freshFile();
	const spill = [
		"PRAGMA cache_size = 10",
		"BEGIN",
		"WITH RECURSIVE c(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM c WHERE i < 4000) INSERT INTO t SELECT 'spill-' || i || '-' || hex(zeroblob(250)) FROM c",
		"UPDATE t SET x = x || '!' WHERE rowid % 10 = 0",
		"COMMIT",
	].join(";\n");
	const child = runChild(file, ursulaUrl() + path, ["CREATE TABLE t(x TEXT)", spill]);
	await child.waitFor((l) => l.step === 1 && l.ok !== undefined);
	child.proc.kill("SIGKILL");
	await child.exited;
	expect(child.lines.find((l) => l.step === 1)?.ok).toBe(true);
	attach(file, ursulaUrl() + path);
	expect(rows(file, "WHERE x LIKE 'spill-%'")).toHaveLength(4000);
	expect(rows(file, "WHERE x LIKE '%!'")).toHaveLength(400);
	const fresh = freshFile();
	attach(fresh, ursulaUrl() + path);
	expect(rows(fresh, "WHERE x LIKE '%!'")).toHaveLength(400);
});

it("(e) an append whose answer is lost is retried with the same producer sequence and applied once", () => {
	const path = streamPath();
	const file = freshFile();
	proxy.dropAfter = 2; // the claim and CREATE are answered; M-one's first answer is cut
	attach(file, proxy.url + path);
	const db = openPlain(file);
	db.exec(SQL[0] as string);
	db.exec(SQL[1] as string);
	expect(proxy.dropped).toBe(1);
	expect(drainStats(file).commits.map((c) => c.attempts)).toEqual([1, 2]);
	// A second copy on the stream would have acknowledged a different offset and poisoned the file.
	expect(status(file).poisoned).toBe(false);
	db.close();
	const fresh = freshFile();
	attach(fresh, ursulaUrl() + path);
	expect(rows(fresh)).toEqual(["M-one"]);
});

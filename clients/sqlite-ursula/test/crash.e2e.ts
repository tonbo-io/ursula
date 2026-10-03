// The commit barrier: SIGKILL/abort around it in a child process, the cache-spill case, and an append
// whose outcome is unknown.
import { existsSync, readFileSync, renameSync, rmSync } from "node:fs";
import { afterAll, beforeAll, beforeEach, expect, it } from "vitest";
import { attach } from "../src/index.ts";
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
	const committed = await child.waitFor((l) => l.step === 1 && l.ok !== undefined);
	child.proc.kill("SIGKILL");
	await child.exited;
	expect(committed.ok).toBe(true);
	attach(file, ursulaUrl() + path);
	expect(rows(file, "WHERE x LIKE 'spill-%'")).toHaveLength(4000);
	expect(rows(file, "WHERE x LIKE '%!'")).toHaveLength(400);
	const fresh = freshFile();
	attach(fresh, ursulaUrl() + path);
	expect(rows(fresh, "WHERE x LIKE '%!'")).toHaveLength(400);
});

it("(e) an append whose answer is lost is retried with the same producer sequence and applied once", async () => {
	const path = streamPath();
	const file = freshFile();
	proxy.dropAfter = 2; // the claim and CREATE are answered; M-one's first answer is cut
	const child = runChild(file, proxy.url + path, SQL.slice(0, 2), { CHILD_EXIT: "1" });
	const done = await child.waitFor((l) => l.done === true);
	expect((await child.exited).code).toBe(0);
	expect(proxy.dropped).toBe(1);
	expect(done.attempts).toEqual([1, 2]);
	// A second copy on the stream would have acknowledged a different offset and poisoned the file.
	expect(done.poisoned).toBe(false);
	const fresh = freshFile();
	attach(fresh, ursulaUrl() + path);
	expect(rows(fresh)).toEqual(["M-one"]);
});

// Regression (#324 review, P2-1): SQLite can still fail an acknowledged transaction locally (here
// its FULL-sync of the WAL) and roll it back; the sidecar then must not cover it.
it("(f) a local failure after the ack poisons the file, keeps the sidecar behind, and re-attach replays the commit", async () => {
	const path = streamPath();
	const file = freshFile();
	const child = runChild(file, ursulaUrl() + path, ["PRAGMA synchronous = FULL", ...SQL], { URSULA_VFS_FAIL_POST_ACK: "3", CHILD_EXIT: "1" });
	const done = await child.waitFor((l) => l.done === true);
	await child.exited;
	expect(child.lines.find((l) => l.step === 3 && l.ok !== undefined)?.ok).toBe(false);
	expect(done.poisoned).toBe(true);
	const sidecar = Number(readFileSync(`${file}-ursula`, "utf8").split(" ")[0]);
	expect(sidecar).toBeLessThan(done.offset as number);
	attach(file, ursulaUrl() + path);
	expect(rows(file)).toEqual(["M-one", "M-two"]);
});

it("(g) a rate-limited append (429 + Retry-After) is retried with the same producer sequence", async () => {
	const path = streamPath();
	const file = freshFile();
	proxy.limitAt = 2; // the claim and CREATE go through; M-one's first attempt is answered 429
	const started = performance.now();
	const child = runChild(file, proxy.url + path, SQL.slice(0, 2), { CHILD_EXIT: "1" });
	const done = await child.waitFor((l) => l.done === true);
	expect((await child.exited).code).toBe(0);
	expect(done).toMatchObject({ attempts: [1, 2], poisoned: false });
	expect(performance.now() - started).toBeGreaterThan(1000); // waited for Retry-After
	const fresh = freshFile();
	attach(fresh, ursulaUrl() + path);
	expect(rows(fresh)).toEqual(["M-one"]);
});

// Regression (review of #324/#325): recovery rewrites pages in place; a crash after the new page 1
// but before the rest left a file SQLite rejects as malformed, and the next attach checkpointed it
// through SQLite first, so it could never recover. The recovery marker makes the next attach
// resume the replay without reading the file through SQLite.
it("(h) killed in the middle of a recovery's page writes: the next attach resumes it", async () => {
	const url = ursulaUrl() + streamPath();
	const file = freshFile();
	// Written by a child: this process keeps the host lock of every file it attaches.
	const writer = runChild(
		file,
		url,
		["CREATE TABLE t(x TEXT, y TEXT)", "WITH RECURSIVE c(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM c WHERE i < 500) INSERT INTO t SELECT 'row-' || i, hex(randomblob(40)) FROM c"],
		{ CHILD_EXIT: "1" },
	);
	expect((await writer.exited).code).toBe(0);
	// The stream moves on: an index (new pages, page 1 rewritten) and more rows.
	const other = freshFile();
	attach(other, url);
	const o = openPlain(other);
	o.exec("CREATE INDEX t_y ON t(y)");
	o.exec("INSERT INTO t VALUES ('late', 'z')");
	o.close();
	// Re-attaching the first file replays that; the child dies after the first page write (page 1).
	const child = runChild(file, url, [], { URSULA_VFS_ABORT_IN_REPLAY: "1" });
	expect((await child.exited).signal).toBe("SIGABRT");
	expect(readFileSync(`${file}-ursula`, "utf8")).toMatch(/recovering/);
	attach(file, url);
	const fresh = freshFile();
	attach(fresh, url);
	expect(Buffer.compare(readFileSync(file), readFileSync(fresh))).toBe(0);
	expect(rows(file, "WHERE x = 'late'")).toEqual(["late"]);
});

// Regression (P1): closing the last connection under synchronous=OFF checkpointed the WAL into the db
// file without syncing it and deleted the WAL; after a power loss that dropped those writes, re-attach
// trusted the sidecar and served the old file (integrity ok, the table gone).
it("(i) power loss after the last close under synchronous=OFF: the committed rows survive re-attach", async () => {
	const path = streamPath();
	const file = freshFile();
	const child = runChild(file, ursulaUrl() + path, ["PRAGMA synchronous = OFF", ...SQL], { URSULA_VFS_DURABLE_SHADOW: "1", CHILD_EXIT: "1" });
	const done = await child.waitFor((l) => l.done === true);
	expect((await child.exited).code).toBe(0);
	expect(done.poisoned).toBe(false);
	expect(existsSync(`${file}-wal`)).toBe(false); // the close checkpointed and deleted the WAL
	// Power loss: only the db file as of its last sync survives (the WAL is gone, the sidecar stays).
	renameSync(`${file}-ursula-durable`, file);
	rmSync(`${file}-shm`, { force: true });
	attach(file, ursulaUrl() + path);
	expect(rows(file)).toEqual(["M-one", "M-two"]);
});

// The local files are a cache of the stream, never fsynced: attach trusts them only when the sidecar
// says they were written in this boot (a process crash keeps the page cache) into this db file, and
// otherwise discards them and rebuilds from snapshot + tail. URSULA_VFS_TEST_BOOT_ID stands in for a
// reboot; the damage a power loss could do is applied by hand.
import { spawn } from "node:child_process";
import { readFileSync, renameSync, statSync, truncateSync, writeFileSync } from "node:fs";
import { expect, it } from "vitest";
import { attach, status } from "../src/index.ts";
import { freshFile } from "./helpers.ts";
import { integrity, openPlain, runChild, streamPath, ursulaUrl } from "./kit.ts";

// Row 2 is replaced 30 times with incompressible text, so the log outgrows the database and (with a
// 1-byte minimum) snapshots are taken; row 3 lands after them.
const FIRST = ["CREATE TABLE t(k INTEGER PRIMARY KEY, x TEXT)", "INSERT INTO t VALUES (1, 'first')"];
const REST = [...Array.from({ length: 30 }, (_, i) => `INSERT OR REPLACE INTO t VALUES (2, '${i}-' || hex(randomblob(600)))`), "INSERT INTO t VALUES (3, 'last')", "@sleep:1500"];
const ACKED = ["1:fir", "2:29-", "3:las"];

const rows = (file: string): string[] => {
	const db = openPlain(file);
	try {
		expect(integrity(db)).toBe("ok");
		return (db.prepare("SELECT k || ':' || substr(x, 1, 3) AS r FROM t ORDER BY k").all() as { r: string }[]).map((r) => r.r);
	} finally {
		db.close();
	}
};

/** An owner (in `boot`, when given) that wrote everything and was SIGKILLed with a WAL, and an older image of its db file. */
async function crashedOwner(boot?: string): Promise<{ url: string; file: string; older: Buffer; offset: number }> {
	const url = ursulaUrl() + streamPath();
	const file = freshFile();
	const env: Record<string, string> = boot === undefined ? {} : { URSULA_VFS_TEST_BOOT_ID: boot };
	const first = runChild(file, url, FIRST, { ...env, CHILD_EXIT: "1" });
	expect((await first.exited).code).toBe(0);
	const older = readFileSync(file);
	const child = runChild(file, url, REST, { ...env, URSULA_VFS_SNAPSHOT_MIN_BYTES: "1" });
	const done = await child.waitFor((l) => l.done === true);
	expect(done.snapshots).toBeGreaterThan(0);
	child.proc.kill("SIGKILL");
	await child.exited;
	return { url, file, older, offset: done.offset as number };
}

it("(a) after a reboot, files a power loss damaged are discarded and rebuilt from snapshot + tail", async () => {
	const { url, file, older } = await crashedOwner("before-the-reboot");
	// A plain connection in another process (no extension) holds the file open, idle.
	const reader = spawn(
		process.execPath,
		["-e", `const { DatabaseSync } = require("node:sqlite"); const db = new DatabaseSync(${JSON.stringify(file)}); db.prepare("SELECT count(*) FROM t").get(); console.log("open"); setInterval(() => {}, 1000);`],
		{ stdio: ["ignore", "pipe", "inherit"] },
	);
	await new Promise<void>((res) => reader.stdout?.once("data", () => res()));
	// The power loss: the db file's writes since the older image lost (same inode), the WAL cut mid-frame.
	writeFileSync(file, older);
	truncateSync(`${file}-wal`, Math.floor(statSync(`${file}-wal`).size / 2));
	// Never discarded under another process's feet.
	expect(() => attach(file, url)).toThrow(/open by another process/);
	reader.kill("SIGKILL");
	await new Promise((r) => reader.once("exit", r));
	attach(file, url);
	const s = status(file);
	expect(s.local).toBe(0);
	expect(s.installed).toBeGreaterThan(0);
	const fresh = freshFile();
	attach(fresh, url);
	expect(Buffer.compare(readFileSync(file), readFileSync(fresh))).toBe(0);
	expect(rows(file)).toEqual(ACKED);
	// The rebuilt file (a snapshot renamed over the old one) is trusted again in this boot.
	attach(file, url);
	expect(status(file)).toMatchObject({ local: s.offset, installed: 0 });
});

it("(b) in the same boot, a crashed owner's files are used as they are: no snapshot, no replay from scratch", async () => {
	const { url, file, offset } = await crashedOwner();
	expect(Number(readFileSync(`${file}-ursula`, "utf8").split(" ")[0])).toBe(offset);
	attach(file, url);
	expect(status(file)).toMatchObject({ local: offset, installed: 0 });
	expect(rows(file)).toEqual(ACKED);
});

// Same boot, but the db file was replaced (here by an older image renamed over it): the WAL left
// next to it belongs to the old file, so applying it would mix two databases.
it("(c) a db file replaced behind the extension's back is discarded and rebuilt", async () => {
	const { url, file, older } = await crashedOwner();
	writeFileSync(`${file}.restore`, older);
	renameSync(`${file}.restore`, file);
	attach(file, url);
	expect(status(file).local).toBe(0);
	expect(rows(file)).toEqual(ACKED);
});

// The files are only ever discarded when the sidecar proves they were ours: a file with content and
// no sidecar may be a database that was never replicated.
it("(d) a file with content but no sidecar is refused, not discarded", () => {
	const file = freshFile();
	const db = openPlain(file);
	db.exec("CREATE TABLE mine(x)");
	db.close();
	expect(() => attach(file, ursulaUrl() + streamPath())).toThrow(/no readable sidecar/);
	const kept = openPlain(file);
	expect(kept.prepare("SELECT count(*) AS n FROM sqlite_schema WHERE name = 'mine'").get()).toEqual({ n: 1 });
	kept.close();
});

// A cache of one stream must never replay another stream's frames on top of it, nor append page
// images built on it to a stream that does not hold its history.
it("(e) attaching the cache of one stream to another, or to its stream deleted and recreated, is refused", async () => {
	const url = ursulaUrl() + streamPath();
	const file = freshFile();
	const child = runChild(file, url, FIRST, { CHILD_EXIT: "1" });
	expect((await child.exited).code).toBe(0);
	expect(() => attach(file, ursulaUrl() + streamPath())).toThrow(/is a cache of stream/);
	expect((await fetch(url, { method: "DELETE" })).ok).toBe(true);
	expect(() => attach(file, url)).toThrow(/beyond the stream's end/);
});

// The local files are a cache of the stream: attach trusts them only when the sidecar says they were
// written in this boot, from this incarnation of the stream, into this db file and the local WAL
// still holds the frames the sidecar counts on, and otherwise discards them and rebuilds from
// snapshot + tail. A reboot is simulated by rewriting the sidecar's boot id to one this kernel never
// had; the damage a power loss or a restored disk image could do is applied by hand.
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

/** What a previous boot leaves behind: the sidecar records a boot id this kernel never had. */
const rebooted = (file: string): void => {
	const sidecar = `${file}-ursula`;
	writeFileSync(sidecar, readFileSync(sidecar, "utf8").replace(/ boot=\S+/, " boot=before-the-reboot"));
};

/** An owner that wrote everything and was SIGKILLed with a WAL, and an older image of its db file and sidecar. */
async function crashedOwner(): Promise<{ url: string; file: string; older: Buffer; olderSidecar: string; offset: number; snapshot: number }> {
	const url = ursulaUrl() + streamPath();
	const file = freshFile();
	const first = runChild(file, url, FIRST, { CHILD_EXIT: "1" });
	expect((await first.exited).code).toBe(0);
	const older = readFileSync(file);
	const olderSidecar = readFileSync(`${file}-ursula`, "utf8");
	const child = runChild(file, url, REST, { URSULA_VFS_SNAPSHOT_MIN_BYTES: "1" });
	const done = await child.waitFor((l) => l.done === true);
	expect(done.snapshots).toBeGreaterThan(0);
	child.proc.kill("SIGKILL");
	await child.exited;
	return { url, file, older, olderSidecar, offset: done.offset as number, snapshot: done.snapshot as number };
}

it("(a) after a reboot, files a power loss damaged are discarded and rebuilt from snapshot + tail", async () => {
	const { url, file, older, olderSidecar, snapshot } = await crashedOwner();
	// The power loss rolled the sidecar back with the db file, to an offset below the stream's
	// retention: the read check before discarding answers 410, which is no reason to keep the files.
	await fetch(`${url}/retention/${snapshot}`, { method: "PUT" });
	const retained = Number((await fetch(url, { method: "HEAD" })).headers.get("stream-retained-offset"));
	expect(retained).toBeGreaterThan(Number(olderSidecar.split(" ")[0]));
	writeFileSync(`${file}-ursula`, olderSidecar);
	rebooted(file);
	// A plain connection in another process (no extension) holds the file open, idle.
	const reader = spawn(
		process.execPath,
		["-e", `const { DatabaseSync } = require("node:sqlite"); const db = new DatabaseSync(${JSON.stringify(file)}); db.prepare("SELECT count(*) FROM t").get(); console.log("open"); setInterval(() => {}, 1000);`],
		{ stdio: ["ignore", "pipe", "inherit"] },
	);
	await new Promise<void>((res) => reader.stdout?.once("data", () => res()));
	// The power loss: the db file's writes since the older image lost and page 1 torn (same inode;
	// SQLite cannot open it, so attach must discard it unread), the WAL cut mid-frame.
	writeFileSync(file, Buffer.concat([Buffer.alloc(512), older.subarray(512)]));
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
	// The rebuilt file (a snapshot written over the emptied one) is trusted again in this boot.
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
it("(e) attaching the cache of one stream to another, or to its stream deleted and recreated shorter, is refused", async () => {
	const url = ursulaUrl() + streamPath();
	const file = freshFile();
	const child = runChild(file, url, FIRST, { CHILD_EXIT: "1" });
	expect((await child.exited).code).toBe(0);
	expect(() => attach(file, ursulaUrl() + streamPath())).toThrow(/is a cache of stream/);
	expect((await fetch(url, { method: "DELETE" })).ok).toBe(true);
	expect(() => attach(file, url)).toThrow(/beyond the stream's end/);
	// After a reboot too: the files are kept, not discarded for an empty database.
	rebooted(file);
	expect(() => attach(file, url)).toThrow(/beyond the stream's end/);
	expect(statSync(file).size).toBeGreaterThan(0);
});

/** Runs `sqls` in an owner that is SIGKILLed afterwards: its WAL stays as it is, never checkpointed. */
async function killedOwner(file: string, url: string, sqls: string[]): Promise<void> {
	const child = runChild(file, url, sqls);
	await child.waitFor((l) => l.done === true);
	child.proc.kill("SIGKILL");
	await child.exited;
}

/**
 * A crash-consistent image of the files on the same boot (a disk snapshot restored, a volume cloned
 * without a reboot): boot id and inode match, but any write never fsynced may be missing. Two owners
 * commit into one WAL generation; the WAL and sidecar the first one left are the older image.
 */
async function twoOwners(): Promise<{ url: string; file: string; wal: Buffer; sidecar: string }> {
	const url = ursulaUrl() + streamPath();
	const file = freshFile();
	await killedOwner(file, url, ["CREATE TABLE t(k INTEGER PRIMARY KEY, x TEXT)", "INSERT INTO t VALUES (1, 'first')"]);
	const image = { wal: readFileSync(`${file}-wal`), sidecar: readFileSync(`${file}-ursula`, "utf8") };
	await killedOwner(file, url, ["INSERT INTO t VALUES (2, 'second')", "INSERT INTO t VALUES (3, 'third')"]);
	return { url, file, ...image };
}

// The sidecar's rename reached the disk, the WAL frames it counts on did not. Trusting the files would
// lose rows 2 and 3 locally, and the next commit would append page images built without them, losing
// them from the stream itself.
it("(f) a same-boot image whose WAL is behind its sidecar is rejected and rebuilt; the stream stays intact", async () => {
	const { url, file, wal } = await twoOwners();
	writeFileSync(`${file}-wal`, wal);
	attach(file, url);
	expect(status(file).local).toBe(0);
	const db = openPlain(file);
	db.exec("INSERT INTO t VALUES (4, 'fourth')");
	db.close();
	const fresh = freshFile();
	attach(fresh, url);
	expect(rows(fresh)).toEqual(["1:fir", "2:sec", "3:thi", "4:fou"]);
});

// The WAL frames reached the disk, the sidecar's last renames did not: the files hold more than the
// sidecar says, and replaying from its offset is idempotent.
it("(g) a same-boot image whose WAL is ahead of its sidecar is trusted", async () => {
	const { url, file, sidecar } = await twoOwners();
	writeFileSync(`${file}-ursula`, sidecar);
	attach(file, url);
	expect(status(file)).toMatchObject({ local: Number(sidecar.split(" ")[0]), installed: 0 });
	expect(rows(file)).toEqual(["1:fir", "2:sec", "3:thi"]);
});

// Regression (VFS-P1): a stream deleted and recreated at the same path is another incarnation, even
// once it has grown past the file's offset; resuming the old database from that offset would carry
// its state into the new stream.
it("(h) the cache of a deleted stream is rebuilt from the stream recreated at its path", async () => {
	const url = ursulaUrl() + streamPath();
	const file = freshFile();
	attach(file, url);
	const db = openPlain(file);
	db.exec("CREATE TABLE t(k INTEGER PRIMARY KEY, x TEXT)");
	db.exec("INSERT INTO t VALUES (1, 'old')");
	db.close();
	const old = status(file).offset;
	expect((await fetch(url, { method: "DELETE" })).ok).toBe(true);
	const other = freshFile();
	attach(other, url);
	const o = openPlain(other);
	o.exec("CREATE TABLE t(k INTEGER PRIMARY KEY, x TEXT)");
	for (let k = 1; status(other).offset <= old; k++) o.exec(`INSERT INTO t VALUES (${k}, 'new')`);
	o.close();
	const recreated = rows(other);
	expect(recreated[0]).toBe("1:new");
	attach(file, url);
	expect(status(file).local).toBe(0);
	expect(rows(file)).toEqual(recreated);
});

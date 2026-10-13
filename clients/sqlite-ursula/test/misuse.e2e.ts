// Mistakes an application can make with the extension fail loudly, and every failure names a
// stable kind (design doc §1, errors): `<function>: <kind>: <message>` for a SQL function's error,
// and `error.kind` in `ursula_status`. These tests use node:sqlite and the extension directly.
import { spawn } from "node:child_process";
import { chmodSync, closeSync, existsSync, mkdirSync, openSync, readFileSync, statSync } from "node:fs";
import { dirname, join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import { afterAll, beforeAll, expect, it } from "vitest";
import { freshFile } from "./helpers.ts";
import { Gate, moveAside, runChild, StallProxy, streamPath, ursulaUrl, vfsPath } from "./kit.ts";

const SQLITE_IOERR_READ = 266;
const SQLITE_IOERR_WRITE = 778;
const SQLITE_IOERR_AUTH = 7178;

type StatusError = { kind: string; action: string; message: string };

const control = new DatabaseSync(":memory:", { allowExtension: true });
control.loadExtension(vfsPath());
const attach = (file: string, url: string): string => (control.prepare("SELECT ursula_attach(?, ?) AS n").get(file, url) as { n: string }).n;
const status = (file: string): Record<string, unknown> & { error: StatusError | null } =>
	JSON.parse((control.prepare("SELECT ursula_status(?) AS s").get(file) as { s: string }).s);
const open = (file: string): DatabaseSync => {
	const db = new DatabaseSync(file);
	db.exec("PRAGMA journal_mode=WAL");
	return db;
};
const failure = (f: () => unknown): { message: string; errcode?: number } => {
	try {
		f();
	} catch (error) {
		return error as { message: string; errcode?: number };
	}
	throw new Error("expected a failure");
};
const tables = (db: DatabaseSync): string[] =>
	(db.prepare("SELECT name FROM sqlite_schema WHERE type = 'table' ORDER BY name").all() as { name: string }[]).map((r) => r.name);
const integrity = (db: DatabaseSync): string => (db.prepare("PRAGMA integrity_check").get() as { integrity_check: string }).integrity_check;
/** The tables a fresh host attaching `url` sees, after checking its rebuild is sound. */
const tablesOf = (url: string): string[] => {
	const file = freshFile();
	attach(file, url);
	const db = open(file);
	try {
		expect(integrity(db)).toBe("ok");
		return tables(db);
	} finally {
		db.close();
	}
};
/**
 * Runs an ES module script in a child process with `args`, within a minute: its stdout parsed as
 * JSON. Asynchronous, so this process goes on serving the proxies the script may go through.
 */
const runScript = (script: string, args: readonly string[], env: Record<string, string> = {}): Promise<Record<string, unknown>> =>
	new Promise((resolve, reject) => {
		const child = spawn(process.execPath, ["--input-type=module", "-e", script, ...args], {
			env: { ...process.env, ...env },
			stdio: ["ignore", "pipe", "pipe"],
		});
		let out = "";
		let err = "";
		child.stdout.on("data", (chunk: Buffer) => {
			out += chunk.toString();
		});
		child.stderr.on("data", (chunk: Buffer) => {
			err += chunk.toString();
		});
		const timer = setTimeout(() => {
			child.kill("SIGKILL");
			reject(new Error(`the script timed out; stderr ${err}`));
		}, 60_000);
		child.once("exit", (code, signal) => {
			clearTimeout(timer);
			if (code !== 0) {
				reject(new Error(`the script exited ${code} ${signal}; stderr ${err}`));
				return;
			}
			try {
				resolve(JSON.parse(out) as Record<string, unknown>);
			} catch {
				reject(new Error(`the script printed ${out}; stderr ${err}`));
			}
		});
	});

let gate: Gate;
let proxy: StallProxy;
beforeAll(async () => {
	gate = await Gate.start(ursulaUrl());
	proxy = await StallProxy.start(ursulaUrl());
});
afterAll(async () => {
	await gate.close();
	await proxy.close();
});

const OUTSIDE = /^ursula_attach: in_use: .* is open in this process outside the extension \((the db file|its -wal|its -shm|its -journal) on descriptor \d+\): close every SQLite connection to it that the extension did not open .*read-only ones included/;

// Regression: a connection opened before the extension was loaded uses the "unix" VFS, and its
// commits never reached the stream, while attach succeeded and nothing failed. Any descriptor of the
// process on the file refuses the attach, one that only reads included.
it("attach refuses a file a connection opened before the extension was loaded, or any descriptor on it", async () => {
	const file = freshFile();
	const url = ursulaUrl() + streamPath();
	const script = `
		import { openSync, closeSync } from "node:fs";
		import { DatabaseSync } from "node:sqlite";
		const [ext, file, url] = process.argv.slice(1);
		const early = new DatabaseSync(file);
		const control = new DatabaseSync(":memory:", { allowExtension: true });
		control.loadExtension(ext);
		const attach = () => control.prepare("SELECT ursula_attach(?, ?) AS n").get(file, url).n;
		const refused = () => { try { attach(); return null; } catch (e) { return e.message; } };
		const refusals = [refused()];
		early.close();
		const reader = openSync(file, "r");
		refusals.push(refused());
		closeSync(reader);
		attach();
		const db = new DatabaseSync(file);
		db.exec("PRAGMA journal_mode=WAL; CREATE TABLE t(x)");
		db.close();
		console.log(JSON.stringify({ refusals }));
	`;
	const { refusals } = await runScript(script, [vfsPath(), file, url]);
	expect(refusals).toEqual([expect.stringMatching(OUTSIDE), expect.stringMatching(OUTSIDE)]);
	expect(tablesOf(url)).toEqual(["t"]);
});

// A read-only SQLite connection outside the extension shares the file's wal-index with the
// attachment, which an attach rewrites under it, and while it holds its lock SQLite keeps open the
// descriptors of connections closed meanwhile, the extension's own included: the attach is refused
// until it closes, and the refusal names it.
it("a read-only connection outside the extension refuses the attach until it closes", () => {
	const file = freshFile();
	const url = ursulaUrl() + streamPath();
	attach(file, url);
	const owner = open(file);
	owner.exec("CREATE TABLE t(x); INSERT INTO t VALUES (1)");
	const inspector = new DatabaseSync(`file:${file}?vfs=unix&mode=ro`);
	expect(inspector.prepare("SELECT count(*) AS n FROM t").get()).toEqual({ n: 1 });
	owner.exec("INSERT INTO t VALUES (2)");
	owner.close();
	expect(failure(() => attach(file, url)).message).toMatch(OUTSIDE);
	expect(status(file)).toMatchObject({ poisoned: false, error: null });
	inspector.close();
	attach(file, url);
	const again = open(file);
	again.exec("INSERT INTO t VALUES (3)");
	expect(again.prepare("SELECT count(*) AS n FROM t").get()).toEqual({ n: 3 });
	again.close();
	expect(status(file)).toMatchObject({ poisoned: false, error: null });
});

// A refused attach changes nothing: a bound path keeps its binding (and its replication) while a
// descriptor outside the extension stays open, and attaches once it is closed.
it("an attach refused for a descriptor outside the extension keeps the binding", () => {
	const file = freshFile();
	const url = ursulaUrl() + streamPath();
	attach(file, url);
	const db = open(file);
	db.exec("CREATE TABLE t(x)");
	db.close();
	const fd = openSync(file, "r");
	try {
		expect(failure(() => attach(file, url)).message).toMatch(/^ursula_attach: in_use: /);
		const again = open(file);
		again.exec("INSERT INTO t VALUES (1)");
		again.close();
		expect(status(file)).toMatchObject({ poisoned: false, error: null });
	} finally {
		closeSync(fd);
	}
	attach(file, url);
	const fresh = freshFile();
	attach(fresh, url);
	const replica = open(fresh);
	expect(replica.prepare("SELECT x FROM t").all()).toEqual([{ x: 1 }]);
	replica.close();
});

// A connection that bypasses the VFS after attach (here `?vfs=unix`) writes the same WAL: the
// owner's next write transaction finds the commit it did not make and poisons before anything else
// is acknowledged. Nothing of the foreign commit reaches the stream. The files may hold the only copy
// of it, so attach refuses them from then on (the action is `fix`) and keeps them; once the file is
// moved aside, the path attaches and is rebuilt from the stream.
it("a commit through a connection outside the extension poisons the owner, and attach refuses the file until it is moved aside", () => {
	const file = freshFile();
	const url = ursulaUrl() + streamPath();
	attach(file, url);
	const owner = open(file);
	owner.exec("CREATE TABLE t(x)");
	const outside = new DatabaseSync(`file:${file}?vfs=unix`);
	outside.exec("CREATE TABLE foreign_table(y); INSERT INTO foreign_table VALUES (randomblob(20000))");
	outside.close();
	const e = failure(() => owner.exec("INSERT INTO t VALUES (1)"));
	expect(e.errcode).toBe(SQLITE_IOERR_WRITE);
	expect(status(file)).toMatchObject({ poisoned: true, fenced: false, error: { kind: "foreign_write", action: "fix" } });
	expect(status(file).error?.message).toMatch(/attach refuses the local files from now on .*move the db file and its -wal aside first/);
	owner.close();
	expect(tablesOf(url)).toEqual(["t"]);
	expect(failure(() => attach(file, url)).message).toMatch(
		/^ursula_attach: unreplicated_file: .* may hold data that is in no stream \(a poison found them written by a connection outside the extension/,
	);
	const kept = new DatabaseSync(`file:${file}?vfs=unix&mode=ro`);
	expect(tables(kept)).toEqual(["foreign_table", "t"]);
	kept.close();
	moveAside(file);
	attach(file, url);
	expect(status(file)).toMatchObject({ local: "-1", poisoned: false, error: null });
	const again = open(file);
	expect(tables(again)).toEqual(["t"]);
	again.exec("CREATE TABLE u(z); INSERT INTO t VALUES (2)");
	again.close();
	expect(tablesOf(url)).toEqual(["t", "u"]);
});

// The commit outside the extension comes first: into the WAL attach left empty, before the owner
// has committed anything (with new salts), whether the owner had read the file or not.
it("a first commit outside the extension, before the owner's, poisons the owner", () => {
	for (const ownerReadFirst of [true, false]) {
		const file = freshFile();
		const url = ursulaUrl() + streamPath();
		attach(file, url);
		const owner = ownerReadFirst ? open(file) : undefined;
		const outside = new DatabaseSync(`file:${file}?vfs=unix`);
		outside.exec("PRAGMA journal_mode=WAL; CREATE TABLE foreign_table(y)");
		outside.close();
		const writer = owner ?? new DatabaseSync(file);
		expect(failure(() => writer.exec("CREATE TABLE t(x)")).errcode, `owner read first: ${ownerReadFirst}`).toBe(SQLITE_IOERR_WRITE);
		expect(status(file)).toMatchObject({ poisoned: true, error: { kind: "foreign_write" } });
		writer.close();
		expect(tablesOf(url)).toEqual([]);
	}
});

// The owner's connections all close after the commit outside the extension, so the next one
// rebuilds the wal-index from the WAL file (a recovery): the foreign commit shows there too.
it("a commit outside the extension shows after the owner reopens the file", () => {
	const file = freshFile();
	const url = ursulaUrl() + streamPath();
	attach(file, url);
	const owner = open(file);
	owner.exec("CREATE TABLE t(x)");
	const outside = new DatabaseSync(`file:${file}?vfs=unix`);
	outside.exec("CREATE TABLE foreign_table(y)");
	outside.close();
	owner.close();
	const reopened = new DatabaseSync(file);
	expect(failure(() => reopened.exec("INSERT INTO t VALUES (1)")).errcode).toBe(SQLITE_IOERR_WRITE);
	expect(status(file)).toMatchObject({ poisoned: true, error: { kind: "foreign_write" } });
	reopened.close();
	expect(tablesOf(url)).toEqual(["t"]);
});

// Regression: a failed attach (here a bucket id the server refuses) left the path openable without
// replication, the reason was lost (a HEAD has no body), and the file it then wrote refused the
// next attach ("no readable sidecar").
it("a failed attach names the server's reason, refuses opens, and blocks no later attach", () => {
	const file = freshFile();
	const bad = `${ursulaUrl()}/s2/kv1`;
	const e = failure(() => attach(file, bad));
	expect(e.message).toMatch(/^ursula_attach: invalid_name: head .*: 400 .*bucket_id must be 4 to 64 bytes, got 2 bytes/);
	expect(failure(() => open(file)).message).toMatch(/unable to open database file/);
	expect(failure(() => status(file)).message).toMatch(/^ursula_status: attach_failed: invalid_name: .* is not attached: its last attach failed \(head /);
	const url = ursulaUrl() + streamPath();
	attach(file, url);
	const db = open(file);
	db.exec("CREATE TABLE t(x)");
	db.close();
	expect(tablesOf(url)).toEqual(["t"]);
});

// A failed attach empties the db file it wrote into while it held no data: here the claim is
// refused after the attach replayed a stream with content into the file, which would refuse
// attaching the file to another stream ("a cache of stream ..."). The sidecar it wrote goes too, so
// nothing vouches for data written to the file later.
it("a failed attach of an empty file leaves no data that refuses a later attach elsewhere", async () => {
	const path = streamPath();
	const writer = freshFile();
	attach(writer, ursulaUrl() + path);
	const w = open(writer);
	w.exec("CREATE TABLE t(x); INSERT INTO t VALUES ('data')");
	w.close();
	const file = freshFile();
	await gate.refuse("quota exhausted", { methods: ["POST"] });
	try {
		const e = failure(() => attach(file, gate.url + path));
		expect(e.message).toMatch(/^ursula_attach: refused: /);
	} finally {
		await gate.reset();
	}
	expect(statSync(file).size).toBe(0);
	expect(existsSync(`${file}-ursula`)).toBe(false);
	const other = ursulaUrl() + streamPath();
	attach(file, other);
	expect(tablesOf(other)).toEqual([]);
});

// Regression: data a writer outside the extension put into a fresh path while its attach waited on
// the stream (here for the claim) was adopted as the stream's state when the stream had nothing to
// replay, and the sidecar the attach left vouched for it when the attach failed. Whether the attach
// then succeeds or fails, the data is refused and kept, and no sidecar is left next to it.
it("data an outside writer puts into a fresh path during its attach is refused, never adopted", async () => {
	const script = `
		import { DatabaseSync } from "node:sqlite";
		const [ext, file, url] = process.argv.slice(1);
		const control = new DatabaseSync(":memory:", { allowExtension: true });
		control.loadExtension(ext);
		let refused = null;
		try { control.prepare("SELECT ursula_attach(?, ?)").get(file, url); } catch (e) { refused = e.message; }
		console.log(JSON.stringify({ refused }));
	`;
	for (const claim of ["delayed", "unanswered"] as const) {
		const path = streamPath();
		const file = freshFile();
		proxy.reset();
		if (claim === "delayed") proxy.delayMs = 1500;
		else proxy.stallAfter = 0;
		const posted = proxy.nextPost();
		const attached = runScript(script, [vfsPath(), file, proxy.url + path], { URSULA_VFS_RETRY_MS: "1500" });
		await posted;
		const outside = new DatabaseSync(`file:${file}?vfs=unix`);
		outside.exec("PRAGMA journal_mode=WAL; CREATE TABLE mine(y); INSERT INTO mine VALUES ('outside')");
		outside.close();
		const { refused } = await attached;
		if (claim === "delayed") {
			expect(refused).toMatch(/^ursula_attach: unreplicated_file: .* was written by a writer outside the extension while it was being attached/);
		} else {
			expect(refused).toMatch(/^ursula_attach: /);
		}
		expect(existsSync(`${file}-ursula`), claim).toBe(false);
		expect(failure(() => attach(file, ursulaUrl() + path)).message, claim).toMatch(/^ursula_attach: unreplicated_file: .* has data but no readable sidecar/);
		const kept = new DatabaseSync(`file:${file}?vfs=unix&mode=ro`);
		expect(tables(kept), claim).toEqual(["mine"]);
		kept.close();
	}
	proxy.reset();
});

// Regression: a sidecar that claims no WAL frame vouched for whatever the db file held, so a commit
// outside the extension between two attachments (checkpointed into the db file, its WAL deleted) was
// resumed as the stream's state, and the owner's next commits made every copy rebuilt from the stream
// malformed. The claim records the db file as the extension left it: after a TRUNCATE checkpoint,
// after a close with journal_size_limit, and after an attach that never committed.
it("a write outside the extension between two attachments refuses files whose sidecar claims no WAL frame", () => {
	const url = ursulaUrl() + streamPath();
	const truncated = freshFile();
	attach(truncated, url);
	const owner = open(truncated);
	owner.exec("CREATE TABLE t(x); INSERT INTO t VALUES (1); PRAGMA wal_checkpoint(TRUNCATE)");
	owner.close();
	const limited = freshFile();
	attach(limited, url);
	const second = open(limited);
	second.exec("PRAGMA journal_size_limit=0; INSERT INTO t VALUES (2)");
	second.close();
	const idle = freshFile();
	attach(idle, url);
	open(idle).close();
	for (const file of [truncated, limited, idle]) {
		expect(readFileSync(`${file}-ursula`, "utf8"), file).toMatch(/ wal=[0-9a-f]{16}:0\n$/);
		const outside = new DatabaseSync(`file:${file}?vfs=unix`);
		outside.exec("CREATE TABLE foreign_table(y); INSERT INTO foreign_table VALUES ('not replicated')");
		outside.close();
		expect(failure(() => attach(file, url)).message, file).toMatch(
			/^ursula_attach: unreplicated_file: .* may hold data that is in no stream \(the db file changed since the extension last wrote it\)/,
		);
	}
	expect(tablesOf(url)).toEqual(["t"]);
});

// Regression: resuming files whose sidecar claims no WAL frame counted as keeping the WAL, which
// turned off the check of the db file until the first commit: a commit outside the extension right
// after such an attach (checkpointed into the db file, its WAL deleted) went unseen.
it("a commit outside the extension right after resuming files that claim no WAL frame poisons the owner", () => {
	const url = ursulaUrl() + streamPath();
	const file = freshFile();
	attach(file, url);
	const db = open(file);
	db.exec("CREATE TABLE t(x); PRAGMA wal_checkpoint(TRUNCATE)");
	db.close();
	const offset = status(file).offset;
	attach(file, url);
	expect(status(file).local).toBe(offset);
	const outside = new DatabaseSync(`file:${file}?vfs=unix`);
	outside.exec("CREATE TABLE mine(y); INSERT INTO mine VALUES (1)");
	outside.close();
	const owner = new DatabaseSync(file);
	expect(failure(() => owner.exec("CREATE TABLE u(z)")).errcode).toBe(SQLITE_IOERR_WRITE);
	expect(status(file)).toMatchObject({ poisoned: true, error: { kind: "foreign_write", action: "fix" } });
	owner.close();
	expect(tablesOf(url)).toEqual(["t"]);
});

// Regression: with synchronous=FULL and no power-safe overwrite (`psow=0`), SQLite pads each commit
// to a sector with copies of its commit frame, so the WAL's last commit frame is past the one the
// sidecar claims, and every attach in the same boot rebuilt the files. The copies are recognized.
it("files whose commits SQLite padded to a sector are resumed", () => {
	const url = ursulaUrl() + streamPath();
	const file = freshFile();
	attach(file, url);
	const db = new DatabaseSync(`file:${file}?psow=0`);
	db.exec("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; CREATE TABLE t(x)");
	for (let i = 0; i < 3; i++) db.exec(`INSERT INTO t VALUES (${i})`);
	db.close();
	const claimed = Number(/ wal=[0-9a-f]{16}:(\d+)\n$/.exec(readFileSync(`${file}-ursula`, "utf8"))?.[1]);
	const frames = (statSync(`${file}-wal`).size - 32) / (24 + 4096);
	expect(frames).toBeGreaterThan(claimed);
	const offset = status(file).offset;
	attach(file, url);
	expect(status(file)).toMatchObject({ local: offset, installed: "-1" });
	const again = open(file);
	expect(again.prepare("SELECT count(*) AS n FROM t").get()).toEqual({ n: 3 });
	again.close();
});

// Regression: when the sidecar could not record that a poison distrusted the files (here the
// directory refuses new files), only the log said so, and an attach in the same process trusted
// them. The process keeps it until an attach succeeds.
it.skipIf(process.getuid?.() === 0)("files a poison distrusted stay refused in the process when their sidecar could not say so", () => {
	const dir = join(dirname(freshFile()), `ro-${process.pid}-${Date.now()}`);
	mkdirSync(dir);
	const file = join(dir, "app.db");
	const url = ursulaUrl() + streamPath();
	attach(file, url);
	const owner = open(file);
	owner.exec("CREATE TABLE t(x)");
	const outside = new DatabaseSync(`file:${file}?vfs=unix`);
	outside.exec("CREATE TABLE foreign_table(y)");
	outside.close();
	chmodSync(dir, 0o555);
	try {
		expect(failure(() => owner.exec("INSERT INTO t VALUES (1)")).errcode).toBe(SQLITE_IOERR_WRITE);
	} finally {
		chmodSync(dir, 0o755);
	}
	expect(status(file)).toMatchObject({ poisoned: true, error: { kind: "foreign_write" } });
	expect(readFileSync(`${file}-ursula`, "utf8")).not.toMatch(/ boot=distrusted /);
	owner.close();
	expect(failure(() => attach(file, url)).message).toMatch(/^ursula_attach: unreplicated_file: .* \(a poison found them written by a connection outside the extension/);
	moveAside(file);
	attach(file, url);
	expect(status(file)).toMatchObject({ local: "-1", poisoned: false, error: null });
});

// Regression: a failed attach of files that held data left the sidecar saying an attach was in
// progress, which vouched for whatever the db file held later. The sidecar records the db file as the
// failed attach left it, so a write outside the extension before the next attach refuses the files.
it("a write outside the extension after a failed attach refuses the files the attach left", async () => {
	const path = streamPath();
	const file = freshFile();
	attach(file, ursulaUrl() + path);
	const db = open(file);
	db.exec("CREATE TABLE t(x); INSERT INTO t VALUES (1)");
	db.close();
	// The stream moves on, so the next attach of the file replays into it before it claims.
	const other = freshFile();
	attach(other, ursulaUrl() + path);
	const o = open(other);
	o.exec("INSERT INTO t VALUES (2)");
	o.close();
	await gate.refuse("quota exhausted", { methods: ["POST"] });
	try {
		expect(failure(() => attach(file, gate.url + path)).message).toMatch(/^ursula_attach: refused: /);
	} finally {
		await gate.reset();
	}
	expect(readFileSync(`${file}-ursula`, "utf8")).toMatch(/ wal=(?!0{16})[0-9a-f]{16}:0\n$/);
	const outside = new DatabaseSync(`file:${file}?vfs=unix`);
	outside.exec("CREATE TABLE foreign_table(y)");
	outside.close();
	expect(failure(() => attach(file, ursulaUrl() + path)).message).toMatch(
		/^ursula_attach: unreplicated_file: .* \(the db file changed since the extension last wrote it\)/,
	);
});

// A database that was never replicated is refused, never replicated over, and its kind says so.
it("a file with data and no sidecar is an unreplicated file", () => {
	const file = freshFile();
	const plain = new DatabaseSync(file);
	plain.exec("CREATE TABLE mine(x)");
	plain.close();
	const e = failure(() => attach(file, ursulaUrl() + streamPath()));
	expect(e.message).toMatch(/^ursula_attach: unreplicated_file: .* has data but no readable sidecar .*it was never replicated/);
	const kept = new DatabaseSync(`file:${file}?vfs=unix&mode=ro`);
	expect(tables(kept)).toEqual(["mine"]);
	kept.close();
});

it("an endpoint that does not answer is unreachable", () => {
	const e = failure(() => attach(freshFile(), `http://127.0.0.1:1${streamPath()}`));
	expect(e.message).toMatch(/^ursula_attach: unreachable: head http:\/\/127\.0\.0\.1:1\//);
});

// Regression: a fenced owner's commits failed, but its connections kept serving the state the new
// owner moved past, indefinitely. A read transaction already open keeps its snapshot until it ends.
it("a fenced owner's reads fail too", async () => {
	const url = ursulaUrl() + streamPath();
	const file = freshFile();
	attach(file, url);
	const a = open(file);
	a.exec("CREATE TABLE t(x); INSERT INTO t VALUES (1)");
	const b = runChild(freshFile(), url, ["INSERT INTO t VALUES (9)"], { CHILD_EXIT: "1" });
	expect((await b.exited).code).toBe(0);
	expect(failure(() => a.exec("INSERT INTO t VALUES (2)")).errcode).toBe(SQLITE_IOERR_WRITE);
	const read = failure(() => a.prepare("SELECT x FROM t").all());
	expect(read.errcode).toBe(SQLITE_IOERR_READ);
	expect(status(file)).toMatchObject({ poisoned: true, fenced: true, error: { kind: "fenced", action: "stop" } });
	// A connection opened afterwards reads nothing either.
	const later = new DatabaseSync(file);
	expect(failure(() => later.prepare("SELECT x FROM t").all()).errcode).toBe(SQLITE_IOERR_READ);
	later.close();
	a.close();
	// Attached again, the file holds the new owner's state.
	attach(file, url);
	const again = open(file);
	expect(again.prepare("SELECT x FROM t ORDER BY x").all()).toEqual([{ x: 1 }, { x: 9 }]);
	again.close();
});

// A poisoned owner that empties its WAL (Pi's close runs a TRUNCATE checkpoint) leaves files that may
// lag its offset, all of whose pages are its own: the checkpoint succeeds, and the next attach
// discards the files and rebuilds them from the stream.
it("a fenced owner's TRUNCATE checkpoint succeeds, and the next attach rebuilds the file", async () => {
	const url = ursulaUrl() + streamPath();
	const file = freshFile();
	attach(file, url);
	const a = open(file);
	a.exec("CREATE TABLE t(x); INSERT INTO t VALUES (1)");
	const b = runChild(freshFile(), url, ["INSERT INTO t VALUES (9)"], { CHILD_EXIT: "1" });
	expect((await b.exited).code).toBe(0);
	expect(failure(() => a.exec("INSERT INTO t VALUES (2)")).errcode).toBe(SQLITE_IOERR_WRITE);
	a.exec("PRAGMA wal_checkpoint(TRUNCATE)");
	a.close();
	expect(readFileSync(`${file}-ursula`, "utf8")).toMatch(/ boot=stale /);
	attach(file, url);
	expect(status(file).local).toBe("-1");
	const again = open(file);
	expect(again.prepare("SELECT x FROM t ORDER BY x").all()).toEqual([{ x: 1 }, { x: 9 }]);
	again.close();
});

// A read transaction's start checks the fence without the database's mutex, which a commit holds
// across its append: a reader on another thread whose pages are cached is not held up by a commit
// waiting for the stream. (A read of WAL frames still waits for it: design doc §4.)
it("a reader with cached pages is not held up by another thread's commit waiting for its append", async () => {
	const file = freshFile();
	proxy.reset();
	// The claim, CREATE TABLE and the first INSERT go through; the worker's commit is held.
	proxy.stallAfter = 3;
	const script = `
		import { DatabaseSync } from "node:sqlite";
		import { Worker } from "node:worker_threads";
		const [ext, file, url] = process.argv.slice(1);
		const control = new DatabaseSync(":memory:", { allowExtension: true });
		control.loadExtension(ext);
		control.prepare("SELECT ursula_attach(?, ?)").get(file, url);
		const db = new DatabaseSync(file);
		db.exec("PRAGMA journal_mode=WAL; CREATE TABLE t(x); INSERT INTO t VALUES (1)");
		const reader = new DatabaseSync(file);
		const count = () => reader.prepare("SELECT count(*) AS n FROM t").get().n;
		count();
		const worker = new Worker(
			'(async () => { const { DatabaseSync } = await import("node:sqlite");' +
			'const { parentPort, workerData } = await import("node:worker_threads");' +
			'const w = new DatabaseSync(workerData); parentPort.postMessage("committing");' +
			'try { w.exec("INSERT INTO t VALUES (2)"); } catch {} })();',
			{ eval: true, workerData: file },
		);
		await new Promise((res) => worker.on("message", res));
		await new Promise((res) => setTimeout(res, 1000));
		const started = performance.now();
		const n = count();
		console.log(JSON.stringify({ n, ms: performance.now() - started }));
		process.exit(0);
	`;
	try {
		const { n, ms } = await runScript(script, [vfsPath(), file, proxy.url + streamPath()], { URSULA_VFS_RETRY_MS: "1000" });
		expect(n).toBe(1);
		expect(ms).toBeLessThan(500);
	} finally {
		proxy.reset();
	}
});

// The three ways a commit fails stay apart in `ursula_status`: refused before it reached the stream
// (failed alone, retry on the same handle), unknown after the retry budget (poisoned, attach again),
// fenced (poisoned, stop).
it("a refused commit and an unknown outcome have kinds of their own", async () => {
	const path = streamPath();
	const file = freshFile();
	attach(file, gate.url + path);
	const db = open(file);
	db.exec("CREATE TABLE t(x)");
	await gate.refuse("quota exhausted", { methods: ["POST"] });
	try {
		expect(failure(() => db.exec("INSERT INTO t VALUES (1)")).errcode).toBe(SQLITE_IOERR_AUTH);
	} finally {
		await gate.reset();
	}
	expect(status(file)).toMatchObject({ poisoned: false, writes_refused: true, refused_kind: "payment_required", error: { kind: "refused", action: "retry" } });
	db.exec("INSERT INTO t VALUES (2)");
	expect(status(file).error).toBeNull();
	db.close();

	proxy.reset();
	proxy.stallAfter = 1; // the claim goes through, the commit's append is held unanswered
	const child = runChild(freshFile(), proxy.url + streamPath(), ["CREATE TABLE t(x)", "@status"], { URSULA_VFS_RETRY_MS: "500", CHILD_EXIT: "1" });
	const failed = await child.waitFor((l) => l.step === 0 && l.ok !== undefined, 60_000);
	expect(failed).toMatchObject({ ok: false, errcode: SQLITE_IOERR_WRITE });
	const shown = await child.waitFor((l) => l.step === 1 && l.ok === true, 60_000);
	expect(shown.status).toMatchObject({ poisoned: true, fenced: false, error: { kind: "outcome_unknown", action: "reattach" } });
	child.proc.kill("SIGKILL");
	await child.exited;
	proxy.reset();
});

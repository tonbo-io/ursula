// Mistakes an application can make with the extension fail loudly, and every failure names a
// stable kind (design doc §1, errors): `<function>: <kind>: <message>` for a SQL function's error,
// and `error.kind` in `ursula_status`. These tests use node:sqlite and the extension directly.
import { spawn } from "node:child_process";
import { closeSync, existsSync, openSync, statSync } from "node:fs";
import { DatabaseSync } from "node:sqlite";
import { afterAll, beforeAll, expect, it } from "vitest";
import { freshFile } from "./helpers.ts";
import { Gate, runChild, StallProxy, streamPath, ursulaUrl, vfsPath } from "./kit.ts";

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

// A failed attach that wrote nothing into the db file removes the sidecar it created, so nothing of
// it vouches for data written to the path later: here the claim of a new stream is refused. One that
// replayed pages into the file keeps it, and the next attach resumes from there (a refused attach is
// tried again later). Neither touches the db file, its -wal or -shm.
it("a failed attach removes the sidecar it created when it wrote nothing", async () => {
	const path = streamPath();
	const file = freshFile();
	await gate.refuse("quota exhausted", { methods: ["POST"] });
	try {
		const e = failure(() => attach(file, gate.url + path));
		expect(e.message).toMatch(/^ursula_attach: refused: /);
	} finally {
		await gate.reset();
	}
	expect(existsSync(`${file}-ursula`)).toBe(false);
	expect(existsSync(file) ? statSync(file).size : 0).toBe(0);
	attach(file, gate.url + path);
	const db = open(file);
	db.exec("CREATE TABLE t(x)");
	db.close();
	expect(tablesOf(ursulaUrl() + path)).toEqual(["t"]);
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

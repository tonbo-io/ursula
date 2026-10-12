// Mistakes an application can make with the extension fail loudly, and every failure names a
// stable kind (design doc §1, errors): `<function>: <kind>: <message>` for a SQL function's error,
// and `error.kind` in `ursula_status`. These tests use node:sqlite and the extension directly.
import { spawnSync } from "node:child_process";
import { existsSync } from "node:fs";
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
/** The tables a fresh host attaching `url` sees. */
const tablesOf = (url: string): string[] => {
	const file = freshFile();
	attach(file, url);
	const db = open(file);
	try {
		return (db.prepare("SELECT name FROM sqlite_schema WHERE type = 'table' ORDER BY name").all() as { name: string }[]).map((r) => r.name);
	} finally {
		db.close();
	}
};

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

// Regression: a connection opened before the extension was loaded uses the "unix" VFS, and its
// commits never reached the stream, while attach succeeded and nothing failed.
it("attach refuses a file a connection opened before the extension was loaded", () => {
	const file = freshFile();
	const url = ursulaUrl() + streamPath();
	const script = `
		import { DatabaseSync } from "node:sqlite";
		const [ext, file, url] = process.argv.slice(1);
		const early = new DatabaseSync(file);
		const control = new DatabaseSync(":memory:", { allowExtension: true });
		control.loadExtension(ext);
		const attach = () => control.prepare("SELECT ursula_attach(?, ?) AS n").get(file, url).n;
		let refused;
		try { attach(); } catch (e) { refused = e.message; }
		early.close();
		attach();
		const db = new DatabaseSync(file);
		db.exec("PRAGMA journal_mode=WAL; CREATE TABLE t(x)");
		db.close();
		console.log(JSON.stringify({ refused }));
	`;
	const run = spawnSync(process.execPath, ["--input-type=module", "-e", script, vfsPath(), file, url], { encoding: "utf8" });
	expect(run.status, run.stderr).toBe(0);
	const { refused } = JSON.parse(run.stdout) as { refused?: string };
	expect(refused).toMatch(/^ursula_attach: in_use: .* is open in this process outside the extension/);
	expect(tablesOf(url)).toEqual(["t"]);
});

// A connection that bypasses the VFS after attach (here `?vfs=unix`) writes the same WAL: the
// owner's next write transaction finds the commit it did not make and poisons before anything else
// is acknowledged. Nothing of the foreign commit reaches the stream.
it("a commit through a connection outside the extension poisons the owner at its next write", () => {
	const file = freshFile();
	const url = ursulaUrl() + streamPath();
	attach(file, url);
	const owner = open(file);
	owner.exec("CREATE TABLE t(x)");
	const outside = new DatabaseSync(`file:${file}?vfs=unix`);
	outside.exec("CREATE TABLE foreign_table(y)");
	outside.close();
	const e = failure(() => owner.exec("INSERT INTO t VALUES (1)"));
	expect(e.errcode).toBe(SQLITE_IOERR_WRITE);
	expect(status(file)).toMatchObject({ poisoned: true, fenced: false, error: { kind: "foreign_write", action: "reattach" } });
	owner.close();
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
	expect(failure(() => status(file)).message).toMatch(/^ursula_status: attach_failed: .* its last attach failed \(invalid_name: /);
	const url = ursulaUrl() + streamPath();
	attach(file, url);
	const db = open(file);
	db.exec("CREATE TABLE t(x)");
	db.close();
	expect(tablesOf(url)).toEqual(["t"]);
});

// A failed attach puts back the files of a path that held no data: here the claim is refused after
// the attach replayed a stream with content into the file and wrote its sidecar, which would refuse
// attaching the file to another stream ("a cache of stream ...").
it("a failed attach of an empty file leaves no files that refuse a later attach elsewhere", async () => {
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
	expect(existsSync(file) || existsSync(`${file}-ursula`)).toBe(false);
	const other = ursulaUrl() + streamPath();
	attach(file, other);
	expect(tablesOf(other)).toEqual([]);
});

it("an endpoint that does not answer is unreachable", () => {
	const e = failure(() => attach(freshFile(), `http://127.0.0.1:1${streamPath()}`));
	expect(e.message).toMatch(/^ursula_attach: unreachable: head http:\/\/127\.0\.0\.1:1\//);
});

// Regression: a fenced owner's commits failed, but its connections kept serving the state the new
// owner moved past, indefinitely.
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

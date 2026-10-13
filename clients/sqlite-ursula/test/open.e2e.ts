// `open(streamUrl)`: one call loads the extension, attaches a local file named after the URL (or the
// one given), and opens it. The database is a node:sqlite DatabaseSync whose statements throw an
// UrsulaError, which says what to do (`action`), for a failure of the extension, and every other error
// unchanged. The errors of `attach` and `status`, and of Pi's storage, come from the same mapping.
import { spawn } from "node:child_process";
import { mkdtempSync, realpathSync, statSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DatabaseSync, StatementSync } from "node:sqlite";
import { afterAll, afterEach, beforeAll, expect, it } from "vitest";
import {
	attach,
	open,
	status,
	UrsulaError,
	UrsulaFencedError,
	UrsulaPaymentRequiredError,
	UrsulaReplicationError,
	UrsulaSetupError,
	UrsulaWriteRefusedError,
} from "../src/index.ts";
import { localName } from "../src/local.ts";
import { freshFile } from "./helpers.ts";
import { Gate, runChild, StallProxy, streamPath, ursulaUrl } from "./kit.ts";

/** SQLITE_IOERR_WRITE: a commit the stream did not take. */
const POISONED = 778;
/** SQLITE_IOERR_READ: a read refused on a fenced file. */
const READ_REFUSED = 266;
/** SQLITE_IOERR_AUTH: a commit refused before it reached the stream. */
const COMMIT_REFUSED = 7178;
/** The vouch with which a gateway (Ursula's, from 0.7.3 on) makes the `x-ursula-not-applied` marker count. */
const VOUCH = { "x-ursula-not-applied-checked": "1" };

const dir = mkdtempSync(join(tmpdir(), "sqlite-ursula-open-"));
const src = new URL("../src/index.ts", import.meta.url).href;

/** The error `f` throws. */
const failure = (f: () => unknown): Error & Record<string, unknown> => {
	try {
		f();
	} catch (error) {
		return error as Error & Record<string, unknown>;
	}
	throw new Error("expected a failure");
};
const tables = (db: DatabaseSync): string[] =>
	(db.prepare("SELECT name FROM sqlite_schema WHERE type = 'table' ORDER BY name").all() as { name: string }[]).map((r) => r.name);

/** Runs an ES module script that imports the package's source (its first argument) in a child process, within 90 s: its stdout, parsed as JSON. */
const runScript = (script: string, args: readonly string[], env: Record<string, string> = {}): Promise<Record<string, unknown>> =>
	new Promise((resolve, reject) => {
		const child = spawn(process.execPath, ["--no-warnings", "--input-type=module", "-e", script, src, ...args], {
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
		}, 90_000);
		child.once("exit", (code, signal) => {
			clearTimeout(timer);
			try {
				if (code !== 0) throw new Error(`exited ${code} ${signal}`);
				resolve(JSON.parse(out) as Record<string, unknown>);
			} catch (error) {
				reject(new Error(`the script failed (${String(error)}): stdout ${out}; stderr ${err}`));
			}
		});
	});

let gate: Gate;
let proxy: StallProxy;
beforeAll(async () => {
	gate = await Gate.start(ursulaUrl());
	proxy = await StallProxy.start(ursulaUrl());
});
// A test that fails midway leaves the gate refusing: the next test starts with it forwarding.
afterEach(async () => {
	await gate.reset();
	proxy.reset();
});
afterAll(async () => {
	await gate.close();
	await proxy.close();
});

it("opens a replicated database from a stream URL alone, in a file named after the URL", () => {
	const url = ursulaUrl() + streamPath();
	const db = open(url, { dir });
	expect(db).toBeInstanceOf(DatabaseSync);
	const file = join(dir, localName(url));
	expect(db.location()).toBe(realpathSync(file));
	// WAL without asking: attach leaves the file in WAL mode.
	expect(db.prepare("PRAGMA journal_mode").get()).toEqual({ journal_mode: "wal" });
	db.exec("CREATE TABLE notes(body TEXT)");
	const insert = db.prepare("INSERT INTO notes VALUES (?)");
	expect(insert).toBeInstanceOf(StatementSync);
	insert.run("one");
	insert.run("two");
	expect(db.prepare("SELECT body FROM notes ORDER BY body").all()).toEqual([{ body: "one" }, { body: "two" }]);
	expect(db.prepare("SELECT count(*) AS n FROM notes").get()).toEqual({ n: 2 });
	expect([...db.prepare("SELECT body FROM notes ORDER BY body").iterate()]).toEqual([{ body: "one" }, { body: "two" }]);
	expect(status(db.location() as string)).toMatchObject({ poisoned: false, error: null, commits: 3 });
	db.close();

	// Another host, at the path it is given: it rebuilds the database from the stream.
	const copy = open(url, { path: freshFile() });
	expect(copy.prepare("SELECT body FROM notes ORDER BY body").all()).toEqual([{ body: "one" }, { body: "two" }]);
	copy.close();

	// The same URL is the same file: opened again, it resumes from the local files.
	const again = open(url, { dir });
	expect(again.location()).toBe(realpathSync(file));
	expect(status(file).local).not.toBe("-1");
	again.prepare("INSERT INTO notes VALUES (?)").run("three");
	expect(again.prepare("SELECT count(*) AS n FROM notes").get()).toEqual({ n: 3 });
	again.close();
});

it("keeps derived files in the user's cache directory by default", () => {
	const cache = mkdtempSync(join(tmpdir(), "sqlite-ursula-cache-"));
	const previous = process.env.XDG_CACHE_HOME;
	process.env.XDG_CACHE_HOME = cache;
	try {
		const url = ursulaUrl() + streamPath();
		const db = open(url);
		expect(db.location()).toBe(join(realpathSync(cache), "sqlite-ursula", localName(url)));
		expect(statSync(join(cache, "sqlite-ursula")).mode & 0o777).toBe(0o700);
		db.close();
	} finally {
		if (previous === undefined) delete process.env.XDG_CACHE_HOME;
		else process.env.XDG_CACHE_HOME = previous;
	}
});

it("derives a distinct, safe file name for every URL, and the same one for every spelling of a URL", () => {
	const base = "https://ursula.example.com";
	const urls = [
		`${base}/my-bucket/app-db`,
		// Names that differ in case alone get files of their own, on a case-insensitive file system too.
		`${base}/my-bucket/App-DB`,
		`${base}/my-bucket/app_db`,
		`${base}/my-bucket/app.db`,
		`${base}/my-bucket/app%2Edb`,
		`${base}/my-bucket/app db`,
		`${base}/my-bucket/app-db/`,
		// The same label as the first, from other segments.
		`${base}/my-bucket-app/db`,
		`${base}/prefix/my-bucket/app-db`,
		`${base}/other/app-db`,
		`http://ursula.example.com/my-bucket/app-db`,
		`${base}:8443/my-bucket/app-db`,
		`https://gateway.example.com/my-bucket/app-db`,
		`${base}/my-bucket/app-db?x=1`,
		`${base}/my-bucket/${"x".repeat(112)}`,
		`${base}/my-bucket/${"x".repeat(111)}y`,
		`${base}/my-bucket/日本語`,
		`${base}/my-bucket/-leading`,
		`${base}/my-bucket/.hidden`,
		`${base}/my-bucket/con`,
		`${base}/my-bucket/a:b*c<d>e|f"g\\h`,
		`${base}`,
	];
	// Random stream ids of what the server accepts: any characters but `/` and NUL, `..` excluded.
	const alphabet = [..."abcXYZ019_-.~ :*?<>|\"'\\%#@!$&()[]{}+=,;éß日本\u{1F600}\t"];
	let seed = 42;
	const random = (n: number): number => {
		seed = (seed * 1103515245 + 12345) % 2 ** 31;
		return seed % n;
	};
	for (let i = 0; i < 2000; i++) {
		const id = Array.from({ length: 1 + random(40) }, () => alphabet[random(alphabet.length)])
			.join("")
			.replaceAll("..", ".");
		urls.push(`${base}/my-bucket/${encodeURIComponent(id)}`);
	}
	const unique = [...new Set(urls.map((url) => new URL(url).href))];
	const names = unique.map(localName);
	expect(new Set(names).size).toBe(unique.length);
	for (const name of names) expect(name).toMatch(/^[a-z0-9_][a-z0-9_-]{0,63}-[0-9a-f]{32}\.db$/);
	expect(localName(`${base}/my-bucket/app-db`)).toMatch(/^my-bucket-app-db-[0-9a-f]{32}\.db$/);
	// Every spelling of one URL is one file.
	expect(localName("HTTPS://Ursula.Example.COM:443/my-bucket/app-db")).toBe(localName(`${base}/my-bucket/app-db`));
	expect(localName(`${base}/my-bucket/app db`)).toBe(localName(`${base}/my-bucket/app%20db`));
	expect(localName(`${base}/my-bucket/./app-db`)).toBe(localName(`${base}/my-bucket/app-db`));
});

it("a fenced owner's commits and reads throw UrsulaFencedError (stop)", async () => {
	const url = ursulaUrl() + streamPath();
	const db = open(url, { dir });
	db.exec("CREATE TABLE t(x)");
	db.prepare("INSERT INTO t VALUES (?)").run(1);
	const owner = runChild(freshFile(), url, ["INSERT INTO t VALUES (9)"], { CHILD_EXIT: "1" });
	expect((await owner.exited).code).toBe(0);

	const commit = failure(() => db.prepare("INSERT INTO t VALUES (?)").run(2));
	expect(commit).toBeInstanceOf(UrsulaFencedError);
	expect(commit).toBeInstanceOf(UrsulaReplicationError);
	expect(commit).toBeInstanceOf(UrsulaError);
	expect(commit).toMatchObject({ name: "UrsulaFencedError", kind: "fenced", action: "stop", fenced: true, code: "ERR_SQLITE_ERROR", errcode: POISONED });
	expect(commit.cause).toMatchObject({ code: "ERR_SQLITE_ERROR", errcode: POISONED });
	expect(commit.message).toMatch(/^commit not replicated: .*superseded.*\. Another owner took the stream over: stop using this database/);

	for (const read of [
		() => db.prepare("SELECT x FROM t").all(),
		() => db.prepare("SELECT x FROM t").get(),
		() => [...db.prepare("SELECT x FROM t").iterate()],
		() => db.exec("SELECT x FROM t"),
	]) {
		const refused = failure(read);
		expect(refused).toBeInstanceOf(UrsulaFencedError);
		expect(refused).toMatchObject({ kind: "fenced", action: "stop", errcode: READ_REFUSED });
		expect(refused.message).toMatch(/^read refused, the file is fenced: /);
	}
	db.close();
});

// The commit's append gets no answer within the retry budget: whether the stream holds it is
// unknown, so the file is poisoned until it is attached again, which replays the stream. The proxy
// runs in this process, so the owner runs in another one.
it("a commit whose outcome is unknown throws UrsulaReattachError (reattach), and opening the stream again replays it", async () => {
	const path = streamPath();
	const file = freshFile();
	proxy.stallAfter = 1; // the claim goes through, the commit's append is held unanswered
	const script = `
		const [src, through, direct, file] = process.argv.slice(1);
		const { open, UrsulaError, UrsulaReattachError, UrsulaReplicationError } = await import(src);
		const db = open(through, { path: file });
		let error;
		try {
			db.exec("CREATE TABLE t(x)");
		} catch (e) {
			error = { name: e.name, kind: e.kind, action: e.action, fenced: e.fenced, message: e.message, errcode: e.errcode, cause: e.cause?.errcode,
				ursula: e instanceof UrsulaError, replication: e instanceof UrsulaReplicationError, reattach: e instanceof UrsulaReattachError };
		}
		db.close();
		const again = open(direct, { path: file });
		const tables = again.prepare("SELECT name FROM sqlite_schema WHERE type = 'table'").all().map((r) => r.name);
		again.exec("CREATE TABLE u(y)");
		again.close();
		console.log(JSON.stringify({ error, tables }));
	`;
	const { error, tables: after } = await runScript(script, [proxy.url + path, ursulaUrl() + path, file], { URSULA_VFS_RETRY_MS: "500" });
	expect(error).toMatchObject({ name: "UrsulaReattachError", kind: "outcome_unknown", action: "reattach", fenced: false, errcode: POISONED, cause: POISONED });
	expect(error).toMatchObject({ ursula: true, replication: true, reattach: true });
	expect((error as { message: string }).message).toMatch(/^commit not replicated: .*\. Close the database and open it again, which replays the stream\.$/);
	// The held append never reached the stream.
	expect(after).toEqual([]);
	const copy = open(ursulaUrl() + path, { path: freshFile() });
	expect(tables(copy)).toEqual(["u"]);
	copy.close();
});

it("a commit refused with 402 throws UrsulaPaymentRequiredError (retry), and the same database commits once the layer accepts writes", async () => {
	const url = gate.url + streamPath();
	const db = open(url, { dir });
	db.exec("CREATE TABLE t(x UNIQUE)");
	db.exec("INSERT INTO t VALUES (1)");
	await gate.refuse("quota exhausted", { methods: ["POST"] });
	const refused = failure(() => db.prepare("INSERT INTO t VALUES (?)").run(2));
	expect(refused).toBeInstanceOf(UrsulaPaymentRequiredError);
	expect(refused).toBeInstanceOf(UrsulaWriteRefusedError);
	expect(refused).toBeInstanceOf(UrsulaError);
	expect(refused).not.toBeInstanceOf(UrsulaReplicationError);
	expect(refused).toMatchObject({ name: "UrsulaPaymentRequiredError", kind: "payment_required", action: "retry", reason: "quota exhausted", errcode: COMMIT_REFUSED });
	expect(refused.message).toBe("commit refused with 402 Payment Required in front of the stream: quota exhausted. Nothing of it reached the stream, and the database stays usable: run it again later.");
	// While writes are refused, a constraint violation is still SQLite's own error, and reads go on.
	const constraint = failure(() => db.exec("INSERT INTO t VALUES (1)"));
	expect(constraint).not.toBeInstanceOf(UrsulaError);
	expect(constraint).toMatchObject({ code: "ERR_SQLITE_ERROR", errcode: 2067, message: "UNIQUE constraint failed: t.x" });
	expect(db.prepare("SELECT x FROM t").all()).toEqual([{ x: 1 }]);
	// An attach the layer refuses claims nothing.
	const attachRefused = failure(() => open(gate.url + streamPath(), { dir }));
	expect(attachRefused).toBeInstanceOf(UrsulaPaymentRequiredError);
	expect(attachRefused).toMatchObject({ kind: "payment_required", action: "retry", reason: null, errcode: 23 });
	expect(attachRefused.message).toMatch(/^ursula_attach: refused: claim \S+: 402 Payment Required quota exhausted/);

	await gate.reset();
	db.prepare("INSERT INTO t VALUES (?)").run(2);
	expect(db.prepare("SELECT x FROM t ORDER BY x").all()).toEqual([{ x: 1 }, { x: 2 }]);
	expect(status(db.location() as string).error).toBeNull();
	db.close();
});

// A transaction too large for one append is refused before it reaches the stream like any other
// refusal, but the same transaction never goes through: its action is `fix`.
it("a transaction too large for a limit in front of the stream throws UrsulaWriteRefusedError (fix)", async () => {
	const db = open(gate.url + streamPath(), { dir });
	db.exec("CREATE TABLE b(x BLOB)");
	await gate.refuse("", { status: 413, headers: { "x-ursula-not-applied": "PayloadTooLarge", ...VOUCH }, methods: ["POST"], over: 65_536 });
	const refused = failure(() => db.exec("INSERT INTO b VALUES (randomblob(200000))"));
	expect(refused).toBeInstanceOf(UrsulaWriteRefusedError);
	expect(refused).not.toBeInstanceOf(UrsulaPaymentRequiredError);
	expect(refused).toMatchObject({ kind: "too_large", action: "fix", reason: "PayloadTooLarge", errcode: COMMIT_REFUSED });
	expect(refused.message).toMatch(/^commit refused before it reached the stream: too_large \(PayloadTooLarge\)\. The same transaction never fits in one append: split it into smaller ones\.$/);
	// Smaller ones go through.
	db.exec("INSERT INTO b VALUES (randomblob(1000))");
	expect(db.prepare("SELECT count(*) AS n FROM b").get()).toEqual({ n: 1 });
	db.close();
});

it("what cannot succeed as asked throws UrsulaSetupError (fix)", () => {
	const name = failure(() => open(`${ursulaUrl()}/s2/kv1`, { dir }));
	expect(name).toBeInstanceOf(UrsulaSetupError);
	expect(name).toMatchObject({ name: "UrsulaSetupError", kind: "invalid_name", action: "fix", errcode: 1 });
	expect(name.message).toMatch(/^ursula_attach: invalid_name: head .*: 400 .*bucket_id must be 4 to 64 bytes, got 2 bytes/);
	expect((name.cause as Error).message).toBe(name.message);

	const url = ursulaUrl() + streamPath();
	const db = open(url, { dir });
	const twice = failure(() => open(url, { dir }));
	expect(twice).toBeInstanceOf(UrsulaSetupError);
	expect(twice).toMatchObject({ kind: "in_use", action: "fix" });
	expect(twice.message).toMatch(/^ursula_attach: in_use: .* has open connections/);
	db.close();
	expect(failure(() => status(freshFile()))).toMatchObject({ name: "UrsulaSetupError", kind: "not_attached", action: "fix" });

	expect(failure(() => open("not a url", { dir }))).toMatchObject({ name: "UrsulaSetupError", kind: "invalid_url", action: "fix" });
	expect(failure(() => open("ftp://127.0.0.1/sqlite-e2e/x", { dir }))).toMatchObject({ name: "UrsulaSetupError", kind: "invalid_url", action: "fix" });
	const blocked = join(dir, "a-file");
	writeFileSync(blocked, "");
	const local = failure(() => open(url, { dir: join(blocked, "sub") }));
	expect(local).toBeInstanceOf(UrsulaSetupError);
	expect(local).toMatchObject({ kind: "local_setup", action: "fix" });
	expect(local.message).toMatch(/^open: local_setup: cannot create .*: pass a writable directory on a local disk as `dir`, or a file as `path`$/);
	// So does a failed attach's path, with the kind of its failure.
	const failed = freshFile();
	failure(() => attach(failed, `${ursulaUrl()}/s2/kv1`));
	expect(failure(() => status(failed))).toMatchObject({ name: "UrsulaSetupError", kind: "invalid_name", action: "fix" });
});

it("an endpoint that does not answer throws UrsulaWriteRefusedError (retry): nothing was claimed", () => {
	const e = failure(() => open(`http://127.0.0.1:1${streamPath()}`, { dir }));
	expect(e).toBeInstanceOf(UrsulaWriteRefusedError);
	expect(e).toMatchObject({ kind: "unreachable", action: "retry", reason: null });
	expect(e.message).toMatch(/^ursula_attach: unreachable: head http:\/\/127\.0\.0\.1:1\//);
});

it("an error that is not the extension's is node:sqlite's own, unchanged", () => {
	const url = ursulaUrl() + streamPath();
	const db = open(url, { dir });
	db.exec("CREATE TABLE t(x UNIQUE NOT NULL)");
	db.prepare("INSERT INTO t VALUES (?)").run(1);
	const unique = failure(() => db.prepare("INSERT INTO t VALUES (?)").run(1));
	expect(unique).not.toBeInstanceOf(UrsulaError);
	expect(unique).toMatchObject({ code: "ERR_SQLITE_ERROR", errcode: 2067, message: "UNIQUE constraint failed: t.x" });
	expect(failure(() => db.exec("INSERT INTO t VALUES (NULL)"))).toMatchObject({ errcode: 1299, message: "NOT NULL constraint failed: t.x" });
	const syntax = failure(() => db.exec("SELEC 1"));
	expect(syntax).not.toBeInstanceOf(UrsulaError);
	expect(syntax).toMatchObject({ errcode: 1, message: 'near "SELEC": syntax error' });
	expect(failure(() => db.prepare("SELECT * FROM missing"))).toMatchObject({ errcode: 1, message: "no such table: missing" });
	const binding = failure(() => db.prepare("SELECT ?").get(Symbol("x") as never));
	expect(binding).not.toBeInstanceOf(UrsulaError);
	expect(binding).toMatchObject({ code: "ERR_INVALID_ARG_TYPE" });
	expect(status(db.location() as string).error).toBeNull();
	db.close();

	// A read-only database: reads work, and a write fails with SQLite's own error.
	const reader = open(url, { path: freshFile(), readOnly: true });
	expect(reader.prepare("SELECT x FROM t").all()).toEqual([{ x: 1 }]);
	const write = failure(() => reader.exec("INSERT INTO t VALUES (2)"));
	expect(write).not.toBeInstanceOf(UrsulaError);
	expect(write).toMatchObject({ errcode: 8, message: "attempt to write a readonly database" });
	expect(status(reader.location() as string)).toMatchObject({ read_only: true, error: null });
	reader.close();
});

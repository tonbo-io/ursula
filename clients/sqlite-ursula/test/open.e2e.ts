// `open(streamUrl)`: one call loads the extension, attaches a local file named after the URL (or the
// one given), and opens it. The database is a node:sqlite DatabaseSync whose statements throw an
// UrsulaError, which says what to do (`action`), for a failure of the extension, and every other error
// unchanged. The errors of `attach` and `status`, and of Pi's storage, come from the same mapping.
import { spawn } from "node:child_process";
import { existsSync, mkdtempSync, realpathSync, statSync, writeFileSync } from "node:fs";
import { tmpdir, userInfo } from "node:os";
import { isAbsolute, join } from "node:path";
import { DatabaseSync, StatementSync } from "node:sqlite";
import { sql } from "drizzle-orm";
import { drizzle } from "drizzle-orm/node-sqlite";
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
import { defaultDir, localName } from "../src/local.ts";
import { openUrsulaPiStorage } from "../src/pi.ts";
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
/** The file `open` derives for `url` in the test's directory. */
const derived = (url: string, variant?: string): string => join(dir, localName(new URL(url), variant));
/** The UrsulaError among `error` and the errors it wraps (`cause`), as a library such as drizzle wraps them. */
const ursulaIn = (error: unknown): UrsulaError | undefined => {
	for (let e = error; e !== undefined && e !== null; e = (e as { cause?: unknown }).cause) if (e instanceof UrsulaError) return e;
	return undefined;
};
/** The usual transaction helper: BEGIN, the work, COMMIT, and on any error ROLLBACK and throw it. */
const transaction = (db: DatabaseSync, work: () => void): void => {
	db.exec("BEGIN");
	try {
		work();
		db.exec("COMMIT");
	} catch (error) {
		db.exec("ROLLBACK");
		throw error;
	}
};
/** The stream's state as the server reports it: 404 when it does not exist. */
const head = async (url: string): Promise<number> => (await fetch(url, { method: "HEAD" })).status;

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
	const file = derived(url);
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
		expect(db.location()).toBe(join(realpathSync(cache), "sqlite-ursula", localName(new URL(url))));
		expect(statSync(join(cache, "sqlite-ursula")).mode & 0o777).toBe(0o700);
		db.close();
	} finally {
		if (previous === undefined) delete process.env.XDG_CACHE_HOME;
		else process.env.XDG_CACHE_HOME = previous;
	}
});

// Regression (review of #579): an empty or relative HOME made the default directory relative, under
// whatever the current directory was.
it("takes the home directory from the user database when HOME is not an absolute path", () => {
	const saved = { HOME: process.env.HOME, XDG_CACHE_HOME: process.env.XDG_CACHE_HOME };
	delete process.env.XDG_CACHE_HOME;
	try {
		for (const home of ["", "relative/home"]) {
			process.env.HOME = home;
			const directory = defaultDir();
			expect(isAbsolute(directory), home).toBe(true);
			expect(directory.startsWith(userInfo().homedir), home).toBe(true);
		}
	} finally {
		for (const [name, value] of Object.entries(saved)) {
			if (value === undefined) delete process.env[name];
			else process.env[name] = value;
		}
	}
});

it("derives a distinct, safe file name for every stream, and the same one for every spelling of a stream's URL", () => {
	const base = "https://ursula.example.com";
	const name = (url: string, variant?: string): string => localName(new URL(url), variant);
	const streams = [
		`${base}/my-bucket/app-db`,
		// Names that differ in case alone get files of their own, on a case-insensitive file system too.
		`${base}/my-bucket/App-DB`,
		`${base}/my-bucket/app_db`,
		`${base}/my-bucket/app.db`,
		`${base}/my-bucket/app db`,
		`${base}/my-bucket/app%25db`,
		`${base}/my-bucket/app%zzdb`,
		// The same label as the first, from other segments.
		`${base}/my-bucket-app/db`,
		`${base}/prefix/my-bucket/app-db`,
		`${base}/my-bucket//app-db`,
		`${base}/other/app-db`,
		`http://ursula.example.com/my-bucket/app-db`,
		`${base}:8443/my-bucket/app-db`,
		`https://gateway.example.com/my-bucket/app-db`,
		`https://127.0.0.1/my-bucket/app-db`,
		`https://[::1]:4437/my-bucket/app-db`,
		`${base}/my-bucket/${"x".repeat(112)}`,
		`${base}/my-bucket/${"x".repeat(111)}y`,
		`${base}/my-bucket/日本語`,
		`${base}/my-bucket/a^b`,
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
	const ids = new Set<string>();
	for (let i = 0; i < 2000; i++) {
		ids.add(
			Array.from({ length: 1 + random(40) }, () => alphabet[random(alphabet.length)])
				.join("")
				.replaceAll("..", "."),
		);
	}
	for (const id of ids) streams.push(`${base}/rand-bucket/${encodeURIComponent(id)}`);
	const names = streams.map((url) => name(url));
	expect(new Set(names).size).toBe(streams.length);
	for (const n of names) expect(n).toMatch(/^[a-z0-9_][a-z0-9_-]{0,63}-[0-9a-f]{32}\.db$/);
	expect(name(`${base}/my-bucket/app-db`)).toMatch(/^my-bucket-app-db-[0-9a-f]{32}\.db$/);
	// Read-only opens keep files of their own.
	expect(name(`${base}/my-bucket/app-db`, "-ro1")).toBe(name(`${base}/my-bucket/app-db`).replace(/\.db$/, "-ro1.db"));
	// Every spelling of one stream's URL is one file: the extension and the server read them as one.
	for (const spelling of [
		"HTTPS://Ursula.Example.COM:443/my-bucket/app-db",
		`${base}/my-bucket/app-db/`,
		`${base}/my-bucket/app-db//`,
		`${base}/my-bucket/./app-db`,
		`${base}/my-bucket/%61pp-db`,
		`${base}/my-bucket/app%2Ddb`,
		"https://user:secret@ursula.example.com/my-bucket/app-db",
	]) {
		expect(name(spelling), spelling).toBe(name(`${base}/my-bucket/app-db`));
	}
	expect(name(`${base}/my-bucket/app%2Edb`)).toBe(name(`${base}/my-bucket/app.db`));
	expect(name(`${base}/my-bucket/app db`)).toBe(name(`${base}/my-bucket/app%20db`));
	expect(name(`${base}/my-bucket/a%5Eb`)).toBe(name(`${base}/my-bucket/a^b`));
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
	expect((error as { message: string }).message).toMatch(/^commit not replicated: .*\. Close the database and attach it again \(open\(\) does\), which replays the stream\.$/);
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
	expect(refused.message).toBe(
		"commit refused with 402 Payment Required in front of the stream: quota exhausted. Nothing of it reached the stream, and the database stays usable: run it again later, the whole transaction if it was one.",
	);
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
	expect(local.message).toMatch(/^local_setup: cannot create .*: pass a writable directory on a local disk as `dir`, or a file as `path`$/);
	// So does a failed attach's path, with the kind of its failure.
	const failed = freshFile();
	failure(() => attach(failed, `${ursulaUrl()}/s2/kv1`));
	expect(failure(() => status(failed))).toMatchObject({ name: "UrsulaSetupError", kind: "invalid_name", action: "fix" });
});

// An endpoint that cannot be reached at all. A commit whose append went out and got no answer is
// another matter: its outcome is unknown (the test above).
it("an endpoint that cannot be reached throws UrsulaWriteRefusedError (retry): nothing was claimed", () => {
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

// Regression (review of #579): SQLite reads `:memory:`, `""` and `file:` URIs differently from the
// extension, so the attach claimed the stream for one file while node:sqlite opened another
// database, whose writes never reached the stream.
it("refuses a path SQLite would not open as the attached file, before attaching anything", async () => {
	for (const path of [":memory:", "", "file:app.db", "file::memory:", "file:app.db?vfs=unix"]) {
		const url = ursulaUrl() + streamPath();
		const e = failure(() => open(url, { path }));
		expect(e, path).toBeInstanceOf(UrsulaSetupError);
		expect(e, path).toMatchObject({ kind: "invalid_argument", action: "fix" });
		await expect(openUrsulaPiStorage(path, url), path).rejects.toMatchObject({ name: "UrsulaSetupError", kind: "invalid_argument" });
		// Nothing was attached: the stream was never created, and no file appeared.
		expect(await head(url), path).toBe(404);
	}
	expect(existsSync(":memory:")).toBe(false);
	expect(existsSync("file:app.db")).toBe(false);
});

// Regression (review of #579): a relative path was looked up against the current directory at each
// failure, so after a chdir the failures of the extension came back as SQLite's raw errors.
it("a relative path or dir is made absolute once: after a chdir, a fence is still UrsulaFencedError", async () => {
	const work = mkdtempSync(join(tmpdir(), "sqlite-ursula-cwd-"));
	const elsewhere = mkdtempSync(join(tmpdir(), "sqlite-ursula-cwd-"));
	const script = `
		const [src, url, work, elsewhere] = process.argv.slice(1);
		const { open } = await import(src);
		const result = {};
		for (const [name, options] of [["path", { path: "rel.db" }], ["dir", { dir: "cache" }]]) {
			process.chdir(work);
			const db = open(url + "-" + name, options);
			db.exec("CREATE TABLE t(x)");
			process.chdir(elsewhere);
			// A second file on the same stream, which takes it over.
			open(url + "-" + name, { path: "taker-" + name + ".db" }).close();
			try {
				db.exec("INSERT INTO t VALUES (1)");
				result[name] = { location: db.location() };
			} catch (e) {
				result[name] = { location: db.location(), name: e.name, kind: e.kind, action: e.action };
			}
		}
		console.log(JSON.stringify(result));
	`;
	const result = await runScript(script, [ursulaUrl() + streamPath(), work, elsewhere]);
	expect(result.path).toMatchObject({ location: join(realpathSync(work), "rel.db"), name: "UrsulaFencedError", kind: "fenced", action: "stop" });
	expect(result.dir).toMatchObject({ name: "UrsulaFencedError", kind: "fenced", action: "stop" });
	expect((result.dir as { location: string }).location.startsWith(join(realpathSync(work), "cache"))).toBe(true);
});

// Regression (review of #579): the extension adds its own query to the stream URL, so a URL with a
// query or a fragment created the stream and appended a claim before failing with stream_state.
it("refuses a stream URL with a query or a fragment before sending anything", async () => {
	for (const suffix of ["?x=1", "#frag", "?"]) {
		const url = ursulaUrl() + streamPath();
		expect(failure(() => open(url + suffix, { dir })), suffix).toMatchObject({ name: "UrsulaSetupError", kind: "invalid_url", action: "fix" });
		expect(failure(() => attach(freshFile(), url + suffix)), suffix).toMatchObject({ name: "UrsulaSetupError", kind: "invalid_url" });
		expect(await head(url), suffix).toBe(404);
	}
});

// Regression (review of #579): the file was named after the URL's spelling, so another spelling of the
// same stream (a trailing `/`, which the extension drops) got a file of its own and took the stream
// over from the first database in the same process.
it("opens one file for every spelling of a stream's URL: a second spelling is in use, not a takeover", () => {
	const url = ursulaUrl() + streamPath();
	const db = open(url, { dir });
	db.exec("CREATE TABLE t(x)");
	expect(failure(() => open(`${url}/`, { dir }))).toMatchObject({ name: "UrsulaSetupError", kind: "in_use" });
	db.exec("INSERT INTO t VALUES (1)");
	db.close();
	const again = open(`${url}/`, { dir });
	expect(again.location()).toBe(realpathSync(derived(url)));
	expect(again.prepare("SELECT x FROM t").all()).toEqual([{ x: 1 }]);
	again.close();
	// Another percent-encoding of the name is the same file too, which recorded the first spelling.
	const encoded = url.replace(/\/vfs-/, "/%76fs-");
	expect(derived(encoded)).toBe(derived(url));
	expect(failure(() => open(encoded, { dir }))).toMatchObject({ name: "UrsulaSetupError", kind: "other_stream", action: "fix" });
});

// Regression (review of #579): a read-only open derived the owner's file, so a reader on the owner's
// host failed with in_use, and so did the owner's next start while a reader ran.
it("read-only opens keep files of their own: a reader next to the owner, and two readers, open", async () => {
	const url = ursulaUrl() + streamPath();
	// The owner, in another process, holds the file named after the URL.
	const owner = runChild(derived(url), url, ["CREATE TABLE t(x)", "INSERT INTO t VALUES (1)"]);
	try {
		await owner.waitFor((l) => l.step === 1 && l.ok === true);
		const first = open(url, { dir, readOnly: true });
		const second = open(url, { dir, readOnly: true });
		expect(first.location()).toBe(realpathSync(derived(url, "-ro1")));
		expect(second.location()).toBe(realpathSync(derived(url, "-ro2")));
		expect(first.prepare("SELECT x FROM t").all()).toEqual([{ x: 1 }]);
		expect(second.prepare("SELECT x FROM t").all()).toEqual([{ x: 1 }]);
		first.close();
		second.close();
		// A reader's file that nothing holds is used again.
		const third = open(url, { dir, readOnly: true });
		expect(third.location()).toBe(realpathSync(derived(url, "-ro1")));
		third.close();
	} finally {
		owner.proc.kill("SIGKILL");
		await owner.exited;
	}
});

// Regression (review of #579): SQLite rolls a transaction back on the I/O error of a failure of the
// extension, so the ROLLBACK a transaction helper runs next failed with "cannot rollback - no
// transaction is active", and that error replaced the UrsulaError.
it("a transaction helper's ROLLBACK after a failure keeps the UrsulaError: refused and fenced, by hand and through drizzle", async () => {
	const url = gate.url + streamPath();
	const db = open(url, { dir });
	db.exec("CREATE TABLE t(x)");
	const orm = drizzle({ client: db });
	await gate.refuse("quota exhausted", { methods: ["POST"] });
	const byHand = failure(() =>
		transaction(db, () => {
			db.exec("INSERT INTO t VALUES (1)");
			db.prepare("INSERT INTO t VALUES (?)").run(2);
		}),
	);
	expect(byHand).toBeInstanceOf(UrsulaPaymentRequiredError);
	expect(db.isTransaction).toBe(false);
	const viaDrizzle = failure(() => orm.transaction((tx) => void tx.run(sql`INSERT INTO t VALUES (3)`)));
	expect(ursulaIn(viaDrizzle)).toBeInstanceOf(UrsulaPaymentRequiredError);
	expect(viaDrizzle.message).toMatch(/commit/);
	const nested = failure(() => orm.transaction((tx) => tx.transaction((inner) => void inner.run(sql`INSERT INTO t VALUES (4)`))));
	expect(ursulaIn(nested)).toBeInstanceOf(UrsulaPaymentRequiredError);
	await gate.reset();
	// Once the layer accepts writes, the whole transactions run again.
	transaction(db, () => db.exec("INSERT INTO t VALUES (5)"));
	orm.transaction((tx) => void tx.run(sql`INSERT INTO t VALUES (6)`));
	expect(db.prepare("SELECT x FROM t ORDER BY x").all()).toEqual([{ x: 5 }, { x: 6 }]);
	// A ROLLBACK that follows no failure is SQLite's own error, as ever.
	expect(failure(() => db.exec("ROLLBACK"))).toMatchObject({ errcode: 1, message: "cannot rollback - no transaction is active" });
	db.close();

	const fencedUrl = ursulaUrl() + streamPath();
	const fenced = open(fencedUrl, { dir });
	fenced.exec("CREATE TABLE t(x)");
	const owner = runChild(freshFile(), fencedUrl, ["INSERT INTO t VALUES (9)"], { CHILD_EXIT: "1" });
	expect((await owner.exited).code).toBe(0);
	expect(failure(() => transaction(fenced, () => fenced.exec("INSERT INTO t VALUES (1)")))).toBeInstanceOf(UrsulaFencedError);
	const fencedOrm = drizzle({ client: fenced });
	expect(ursulaIn(failure(() => fencedOrm.transaction((tx) => void tx.run(sql`INSERT INTO t VALUES (2)`))))).toBeInstanceOf(UrsulaFencedError);
	// A read inside a savepoint fails: its ROLLBACK TO finds no savepoint, since the whole transaction is gone.
	const inSavepoint = failure(() => fencedOrm.transaction((tx) => tx.transaction((inner) => void inner.all(sql`SELECT x FROM t`))));
	expect(ursulaIn(inSavepoint)).toBeInstanceOf(UrsulaFencedError);
	fenced.close();
});

it("a transaction helper's ROLLBACK after an unknown outcome keeps the UrsulaReattachError", async () => {
	const path = streamPath();
	proxy.stallAfter = 1; // the claim goes through, every commit's append is held unanswered
	const script = `
		const [src, url, file] = process.argv.slice(1);
		const { open, UrsulaError } = await import(src);
		const { drizzle } = await import("drizzle-orm/node-sqlite");
		const { sql } = await import("drizzle-orm");
		const found = (error) => { for (let e = error; e; e = e.cause) if (e instanceof UrsulaError) return { name: e.name, kind: e.kind, action: e.action }; return { raw: String(error) }; };
		const db = open(url, { path: file });
		const result = {};
		try {
			drizzle({ client: db }).transaction((tx) => void tx.run(sql\`CREATE TABLE t(x)\`));
		} catch (e) {
			result.drizzle = found(e);
		}
		try {
			db.exec("BEGIN");
			try {
				db.exec("CREATE TABLE u(y)");
				db.exec("COMMIT");
			} catch (e) {
				db.exec("ROLLBACK");
				throw e;
			}
		} catch (e) {
			result.byHand = found(e);
		}
		console.log(JSON.stringify(result));
		process.exit(0);
	`;
	const result = await runScript(script, [proxy.url + path, freshFile()], { URSULA_VFS_RETRY_MS: "500" });
	expect(result.drizzle).toEqual({ name: "UrsulaReattachError", kind: "outcome_unknown", action: "reattach" });
	expect(result.byHand).toMatchObject({ name: "UrsulaReattachError", action: "reattach" });
});

// Regression (review of #579): prepare dropped its second argument, the statement's options that
// node:sqlite 24 takes.
it("passes every argument of prepare on to node:sqlite", () => {
	const db = open(ursulaUrl() + streamPath(), { dir });
	const original = DatabaseSync.prototype.prepare;
	const seen: unknown[][] = [];
	DatabaseSync.prototype.prepare = function (this: DatabaseSync, ...args: unknown[]) {
		seen.push(args);
		return (original as (...a: unknown[]) => StatementSync).apply(this, args);
	} as typeof original;
	try {
		(db.prepare as (...a: unknown[]) => StatementSync).call(db, "SELECT 1 AS x", { readBigInts: true });
	} finally {
		DatabaseSync.prototype.prepare = original;
	}
	expect(seen).toEqual([["SELECT 1 AS x", { readBigInts: true }]]);
	db.close();
});

it.runIf(Number(process.versions.node.split(".")[0]) >= 24)("applies node:sqlite 24's statement options", () => {
	const db = open(ursulaUrl() + streamPath(), { dir });
	const prepare = db.prepare as (...a: unknown[]) => StatementSync;
	expect(prepare.call(db, "SELECT 9007199254740993 AS x", { readBigInts: true }).get()).toEqual({ x: 9007199254740993n });
	expect(prepare.call(db, "SELECT 'a' AS y", { returnArrays: true }).all()).toEqual([["a"]]);
	db.close();
});

// Regression (review of #579): the constructor took a third argument and passed `undefined` options
// on, so a connection made through `db.constructor` failed, or turned every error into a TypeError.
it("a connection made through db.constructor works as a DatabaseSync", () => {
	const db = open(ursulaUrl() + streamPath(), { dir });
	db.exec("CREATE TABLE t(x UNIQUE); INSERT INTO t VALUES (1)");
	const Connection = db.constructor as typeof DatabaseSync;
	const second = new Connection(db.location() as string);
	expect(second.prepare("SELECT x FROM t").all()).toEqual([{ x: 1 }]);
	expect(failure(() => second.exec("INSERT INTO t VALUES (1)"))).toMatchObject({ errcode: 2067, message: "UNIQUE constraint failed: t.x" });
	second.exec("INSERT INTO t VALUES (2)");
	second.close();
	expect(db.prepare("SELECT count(*) AS n FROM t").get()).toEqual({ n: 2 });
	db.close();
});

// Regression (review of #579): node:sqlite checked its options only after the attach had claimed the
// stream, so a wrong option fenced the owner and then failed.
it("checks node:sqlite's options before attaching: a wrong one claims nothing", () => {
	const url = ursulaUrl() + streamPath();
	const db = open(url, { dir });
	db.exec("CREATE TABLE t(x)");
	const other = freshFile();
	const wrong = failure(() => open(url, { path: other, timeout: 1.5 }));
	expect(wrong).not.toBeInstanceOf(UrsulaError);
	expect(wrong).toMatchObject({ code: "ERR_INVALID_ARG_TYPE" });
	// The owner was not fenced, and the other file was never attached.
	db.exec("INSERT INTO t VALUES (1)");
	expect(failure(() => status(other))).toMatchObject({ name: "UrsulaSetupError", kind: "not_attached" });
	db.close();
});

// A panic inside the extension stops at its C entry point: the call fails with an SQLite error,
// and the host process goes on instead of aborting, which unwinding out of an `extern "C"`
// function would do. `URSULA_VFS_PANIC_IN=<entry>` makes one entry point panic on purpose. Each
// case runs in its own Node process, since the extension reads the setting once per process.
import { spawnSync } from "node:child_process";
import { expect, it } from "vitest";
import { freshFile } from "./helpers.ts";
import { streamPath, ursulaUrl, vfsPath } from "./kit.ts";

/** SQLITE_IOERR_WRITE: a failed write that poisons the file. */
const POISONED = 778;

/**
 * Runs `script` in a new Node process with the extension's path in `EXT`, and the given env. The
 * process must exit 0 without a signal (an aborting panic ends it with SIGABRT), and print a JSON
 * line last.
 */
const run = (script: string, env: Record<string, string>) => {
	const r = spawnSync(process.execPath, ["--input-type=module", "-e", script], {
		env: { ...process.env, EXT: vfsPath(), ...env },
		encoding: "utf8",
		timeout: 60_000,
	});
	expect(r.signal, r.stderr).toBeNull();
	expect(r.status, r.stderr).toBe(0);
	const last = r.stdout.trim().split("\n").at(-1);
	expect(last, r.stderr).toBeTruthy();
	return { stderr: r.stderr, out: JSON.parse(last as string) as Record<string, unknown> };
};

const panicked = (entry: string): RegExp => new RegExp(`event=panic entry=${entry} message="URSULA_VFS_PANIC_IN=${entry}"`);

it("a panic in a SQL function fails the call, and the process goes on", () => {
	for (const [fn, args] of [
		["ursula_status", "'/nonexistent'"],
		["ursula_stats", "'/nonexistent'"],
		["ursula_set_token", "'t'"],
		["ursula_attach", "'/nonexistent', 'http://127.0.0.1:1/b/s'"],
	] as const) {
		const r = run(
			`import { DatabaseSync } from "node:sqlite";
			const c = new DatabaseSync(":memory:", { allowExtension: true });
			c.loadExtension(process.env.EXT);
			const out = {};
			try { c.prepare("SELECT ${fn}(${args}) AS s").get(); out.call = "returned"; } catch (e) { out.call = String(e); }
			out.after = c.prepare("SELECT 41 + 1 AS n").get().n;
			console.log(JSON.stringify(out));`,
			{ URSULA_VFS_PANIC_IN: fn },
		);
		expect(r.out.call).toMatch(new RegExp(`${fn}: internal: ${fn} panicked inside the extension`));
		expect(r.out.after).toBe(42);
		expect(r.stderr).toMatch(panicked(fn));
	}
});

it("a panic in the extension's init fails the load with a message, and the process goes on", () => {
	const r = run(
		`import { DatabaseSync } from "node:sqlite";
		const c = new DatabaseSync(":memory:", { allowExtension: true });
		const out = {};
		try { c.loadExtension(process.env.EXT); out.load = "loaded"; } catch (e) { out.load = String(e); }
		out.after = c.prepare("SELECT 41 + 1 AS n").get().n;
		console.log(JSON.stringify(out));`,
		{ URSULA_VFS_PANIC_IN: "sqlite3_extension_init" },
	);
	expect(r.out.load).toMatch(/sqlite3_extension_init panicked inside the extension/);
	expect(r.out.after).toBe(42);
	expect(r.stderr).toMatch(panicked("sqlite3_extension_init"));
});

it("a panic in a file method fails it and poisons its file alone, and the process goes on", () => {
	const r = run(
		`import { DatabaseSync } from "node:sqlite";
		const c = new DatabaseSync(":memory:", { allowExtension: true });
		c.loadExtension(process.env.EXT);
		const status = (file) => JSON.parse(c.prepare("SELECT ursula_status(?) AS s").get(file).s);
		// B is attached first, and only read: the panic in A's write must leave it alone.
		c.prepare("SELECT ursula_attach(?, ?) AS n").get(process.env.FILE_B, process.env.URL_B);
		c.prepare("SELECT ursula_attach(?, ?) AS n").get(process.env.FILE, process.env.URL);
		const out = {};
		try {
			new DatabaseSync(process.env.FILE).exec("CREATE TABLE t(x TEXT)");
			out.write = "committed";
		} catch (e) {
			out.write = String(e);
			out.errcode = e.errcode;
		}
		out.status = status(process.env.FILE);
		out.b = new DatabaseSync(process.env.FILE_B).prepare("SELECT count(*) AS n FROM sqlite_schema").get().n;
		out.statusB = status(process.env.FILE_B);
		out.after = c.prepare("SELECT 41 + 1 AS n").get().n;
		console.log(JSON.stringify(out));`,
		{
			URSULA_VFS_PANIC_IN: "x_write",
			FILE: freshFile(),
			URL: ursulaUrl() + streamPath(),
			FILE_B: freshFile(),
			URL_B: ursulaUrl() + streamPath(),
		},
	);
	expect(r.out.write).toMatch(/disk I\/O error/);
	expect(r.out.errcode).toBe(POISONED);
	expect(r.out.status).toMatchObject({ poisoned: true, fenced: false, reason: "x_write panicked inside the extension" });
	expect(r.out.b).toBe(0);
	expect(r.out.statusB).toMatchObject({ poisoned: false, reason: null });
	expect(r.out.after).toBe(42);
	expect(r.stderr).toMatch(panicked("x_write"));
});

it("a panic while attaching fails the attach and leaves no attach in progress", () => {
	const file = freshFile();
	const url = ursulaUrl() + streamPath();
	// Attached once without the setting: the file has a sidecar and a row.
	const setup = (script: string) =>
		run(
			`import { DatabaseSync } from "node:sqlite";
			const c = new DatabaseSync(":memory:", { allowExtension: true });
			c.loadExtension(process.env.EXT);
			c.prepare("SELECT ursula_attach(?, ?) AS n").get(process.env.FILE, process.env.URL);
			const db = new DatabaseSync(process.env.FILE);
			${script}
			console.log(JSON.stringify({ rows: db.prepare("SELECT count(*) AS n FROM t").get().n }));`,
			{ FILE: file, URL: url },
		);
	expect(setup(`db.exec("CREATE TABLE t(x TEXT)"); db.exec("INSERT INTO t VALUES ('a1')");`).out.rows).toBe(1);
	const r = run(
		`import { DatabaseSync } from "node:sqlite";
		const c = new DatabaseSync(":memory:", { allowExtension: true });
		c.loadExtension(process.env.EXT);
		const attach = () => { try { c.prepare("SELECT ursula_attach(?, ?) AS n").get(process.env.FILE, process.env.URL); return "attached"; } catch (e) { return String(e); } };
		const out = { attach: attach() };
		try { new DatabaseSync(process.env.FILE).prepare("SELECT 1").get(); out.open = "opened"; } catch (e) { out.open = String(e); }
		try { c.prepare("SELECT ursula_status(?) AS s").get(process.env.FILE); out.status = "returned"; } catch (e) { out.status = String(e); }
		out.again = attach();
		console.log(JSON.stringify(out));`,
		{ URSULA_VFS_PANIC_IN: "attach_files", FILE: file, URL: url },
	);
	expect(r.out.attach).toMatch(/attach_files panicked inside the extension/);
	// Recorded as a failed attach, not one still in progress: the file, which has a sidecar, is
	// refused as after any failed attach ("unable to open"), not answered SQLITE_BUSY ("database is
	// locked"), and the next attach runs (and panics again here) instead of finding one in progress.
	expect(r.out.open).toMatch(/unable to open database file/);
	expect(r.out.status).toMatch(/attach_failed: internal: .* is not attached: its last attach failed \(attach_files panicked inside the extension\)/);
	expect(r.out.again).toMatch(/attach_files panicked inside the extension/);
	expect(r.stderr).toMatch(panicked("attach_files"));
	// Without the setting, the file attaches again, with its row.
	expect(setup("").out.rows).toBe(1);
});

it("a panic in xClose releases the file, which attaches again in the same process", () => {
	const r = run(
		`import { DatabaseSync } from "node:sqlite";
		const c = new DatabaseSync(":memory:", { allowExtension: true });
		c.loadExtension(process.env.EXT);
		const attach = () => { try { c.prepare("SELECT ursula_attach(?, ?) AS n").get(process.env.FILE, process.env.URL); return "attached"; } catch (e) { return String(e); } };
		const out = { first: attach() };
		const db = new DatabaseSync(process.env.FILE);
		db.exec("CREATE TABLE t(x TEXT)");
		db.exec("INSERT INTO t VALUES ('a1')");
		db.close();
		out.again = attach();
		out.rows = new DatabaseSync(process.env.FILE).prepare("SELECT count(*) AS n FROM t").get().n;
		console.log(JSON.stringify(out));`,
		{ URSULA_VFS_PANIC_IN: "x_close", FILE: freshFile(), URL: ursulaUrl() + streamPath() },
	);
	expect(r.out).toMatchObject({ first: "attached", again: "attached", rows: 1 });
	expect(r.stderr).toMatch(panicked("x_close"));
});

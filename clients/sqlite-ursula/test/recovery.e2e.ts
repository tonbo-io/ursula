// Attach is the only writer of a file it recovers: one owner per file on a host (lock file), and no
// other connection open while pages are rewritten.
import { spawn } from "node:child_process";
import { expect, it } from "vitest";
import { attach, loadUrsulaVfs, status } from "../src/index.ts";
import { freshFile } from "./helpers.ts";
import { integrity, openPlain, runChild, streamPath, ursulaUrl } from "./kit.ts";

const xs = (file: string): string[] => {
	const db = openPlain(file);
	try {
		expect(integrity(db)).toBe("ok");
		return (db.prepare("SELECT x FROM t ORDER BY x").all() as { x: string }[]).map((r) => r.x);
	} finally {
		db.close();
	}
};

it("a second process cannot attach a file another process has attached", async () => {
	const file = freshFile();
	const child = runChild(file, ursulaUrl() + streamPath(), ["CREATE TABLE t(x TEXT)"]);
	await child.waitFor((l) => l.done === true);
	expect(() => attach(file, ursulaUrl() + streamPath())).toThrow(/attached by another process/);
	child.proc.kill("SIGKILL");
	await child.exited;
});

// Regression (#322 review, P1-3): recovery ignored the checkpoint's result row and rewrote the db file
// while other connections could hold its old WAL and page cache.
it("attach refuses to recover while another connection has the file open", async () => {
	const url = ursulaUrl() + streamPath();
	const file = freshFile();
	attach(file, url);
	const db = openPlain(file);
	db.exec("CREATE TABLE t(x TEXT)");
	db.exec("INSERT INTO t VALUES ('f1')");
	expect(() => attach(file, url)).toThrow(/open connections/); // in this process
	db.close();
	// The stream moves past the file.
	const other = freshFile();
	attach(other, url);
	const o = openPlain(other);
	o.exec("INSERT INTO t VALUES ('o1')");
	o.close();
	// A plain connection in another process (no extension) holds the file open, idle.
	const reader = spawn(
		process.execPath,
		["-e", `const { DatabaseSync } = require("node:sqlite"); const db = new DatabaseSync(${JSON.stringify(file)}); db.prepare("SELECT count(*) FROM t").get(); console.log("open"); setInterval(() => {}, 1000);`],
		{ stdio: ["ignore", "pipe", "inherit"] },
	);
	await new Promise<void>((res) => reader.stdout?.once("data", () => res()));
	expect(() => attach(file, url)).toThrow(/open by another process/);
	reader.kill("SIGKILL");
	await new Promise((r) => reader.once("exit", r));
	attach(file, url);
	expect(xs(file)).toEqual(["f1", "o1"]);
});

// Regression (#345): a file with a sidecar passed through to the plain "unix" VFS without a
// binding: in a process that never attached it, and after a failed re-attach, which dropped the
// binding before it could fail. The fix must not keep the old binding either (the failed attach
// may have rewritten the files).
it("a file with a sidecar opens only while attached here: not before an attach, nor after a failed one", async () => {
	const path = streamPath();
	const url = ursulaUrl() + path;
	const file = freshFile();
	const child = runChild(file, url, ["CREATE TABLE t(x TEXT)", "INSERT INTO t VALUES ('r1')"], { CHILD_EXIT: "1" });
	expect((await child.exited).code).toBe(0);
	loadUrsulaVfs();
	expect(() => openPlain(file)).toThrow(/unable to open/);
	attach(file, url);
	const again = openPlain(file);
	again.exec("INSERT INTO t VALUES ('r2')");
	again.close();
	// A failed re-attach of the bound path drops its binding. Nothing listens on port 1: the HEAD
	// fails.
	expect(() => attach(file, `http://127.0.0.1:1${path}`)).toThrow(/head http:\/\/127\.0\.0\.1:1\//);
	expect(() => openPlain(file)).toThrow(/unable to open/);
	expect(() => status(file)).toThrow(/last attach failed/);
	attach(file, url);
	expect(xs(file)).toEqual(["r1", "r2"]);
	const back = openPlain(file);
	back.exec("INSERT INTO t VALUES ('r3')");
	back.close();
	const fresh = freshFile();
	attach(fresh, url);
	expect(xs(fresh)).toEqual(["r1", "r2", "r3"]);
});

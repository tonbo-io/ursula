// Test 2: SIGKILL/abort at the three points around the commit barrier, in a child process.
import { afterAll, beforeAll, expect, it } from "vitest";
import { attach } from "../../src/vfs.ts";
import { freshFile } from "../helpers.ts";
import { integrity, openPlain, runChild, StallProxy, streamPath, ursulaUrl, walContains } from "./kit.ts";

const SQL = ["CREATE TABLE t(x TEXT)", "INSERT INTO t VALUES ('M-one')", "INSERT INTO t VALUES ('M-two')"];
const rows = (file: string): string[] => {
	const db = openPlain(file);
	try {
		expect(integrity(db)).toBe("ok");
		return (db.prepare("SELECT x FROM t ORDER BY x").all() as { x: string }[]).map((r) => r.x);
	} finally {
		db.close();
	}
};

let proxy: StallProxy;
beforeAll(async () => {
	proxy = await StallProxy.start(ursulaUrl());
});
afterAll(async () => {
	await proxy.close();
});

it("(a) killed while the append is in flight, before Ursula has it: absent, and the local WAL holds nothing of it", async () => {
	const path = streamPath();
	const file = freshFile();
	proxy.stallAfter = 2; // CREATE and M-one go through; M-two's append is held
	const child = runChild(file, proxy.url + path, SQL);
	await proxy.stalled;
	await child.waitFor((l) => l.step === 2 && l.phase === "start");
	child.proc.kill("SIGKILL");
	expect((await child.exited).signal).toBe("SIGKILL");
	expect(walContains(file, "M-one")).toBe(true);
	expect(walContains(file, "M-two")).toBe(false);
	expect(attach(file, ursulaUrl() + path)).toBe(2);
	expect(rows(file)).toEqual(["M-one"]);
});

it("(b) killed after the ack, before the local WAL write: present after reopen (replayed)", async () => {
	const path = streamPath();
	const file = freshFile();
	const child = runChild(file, ursulaUrl() + path, SQL, { URSULA_VFS_ABORT_AFTER_ACK: "3" });
	const exit = await child.exited;
	expect(exit.signal).toBe("SIGABRT");
	expect(child.lines.some((l) => l.step === 2 && l.phase === "start")).toBe(true);
	expect(walContains(file, "M-one")).toBe(true);
	expect(walContains(file, "M-two")).toBe(false);
	expect(attach(file, ursulaUrl() + path)).toBe(3);
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
	expect(attach(file, ursulaUrl() + path)).toBe(3);
	expect(rows(file)).toEqual(["M-one", "M-two"]);
});

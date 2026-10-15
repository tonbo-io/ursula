// What `ursula_open` hands out while a snapshot attempt holds the extension's private connection,
// against the e2e server (test/stack.ts). Snapshots are due after every acknowledged byte, and the
// test hook URSULA_VFS_SNAPSHOT_HOLD keeps the attempt's private connection open until the test
// releases it, so the database's last connection closes and `ursula_open` runs while it is open:
// - reopened in the same cache, the path `ursula_open` handed out opens after the attempt ends
//   (the close of the private connection does not retire the binding it handed out);
// - reopened in a fresh cache, the open returns at once with `timeout_ms` 0 while the binding
//   nothing of the host holds any more waits for the attempt to end, then retires, and the open
//   binds in the fresh cache;
// - while a connection of the host is open, an open in another cache is refused
//   `bound_elsewhere`, naming the cache the stream is open in.
import { realpathSync, writeFileSync } from "node:fs";
import { basename, join } from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { Child, freshDir, killChildren, type Opened, stream } from "./kit.ts";

afterEach(killChildren);

/** An owner process on a fresh cache whose snapshot attempts hold their private connection in `hold`. */
async function owner(url: string, dir: string, hold: string): Promise<Child> {
	const child = new Child(url, dir, { URSULA_VFS_SNAPSHOT_DUE_BYTES: "1", URSULA_VFS_SNAPSHOT_HOLD: hold }, "handout-child.mjs");
	await child.opened();
	return child;
}

async function ok(child: Child, cmd: Record<string, unknown>): Promise<Record<string, unknown>> {
	const reply = await child.send(cmd);
	expect(reply.ok, `${JSON.stringify(cmd)}: ${JSON.stringify(reply)}`).toBe(true);
	return reply;
}

/** Commits a row, waits until the stream acknowledged it, then until a snapshot attempt holds its private connection. */
async function commitUntilHeld(child: Child, hold: string): Promise<void> {
	await child.exec("CREATE TABLE t(x); INSERT INTO t VALUES (1)");
	expect((await ok(child, { synced: 10_000 })).synced).toMatchObject({ synced: true });
	expect((await ok(child, { wait: join(hold, "held") })).found, "no snapshot attempt held its private connection").toBe(true);
}

describe("a database reopened while a snapshot attempt holds the private connection", () => {
	it("opens the path ursula_open handed out in the same cache once the attempt ends", async () => {
		const s = stream("handout-same");
		const dir = freshDir();
		const hold = freshDir();
		const child = await owner(s.url, dir, hold);
		const first = (await child.send({ status: true })).status as { path: string };
		await commitUntilHeld(child, hold);
		await ok(child, { close: true });
		const again = (await ok(child, { bind: dir })).opened as Opened;
		expect(again.path).toBe(first.path);
		writeFileSync(join(hold, "release"), "");
		expect((await ok(child, { wait: join(hold, "dropped") })).found).toBe(true);
		// Time for the driver to retire a binding the private connection's close left behind.
		await new Promise((r) => setTimeout(r, 500));
		expect(((await ok(child, { status: true })).status as { state: string }).state).toBe("live");
		await ok(child, { connect: true });
		expect(await child.rows("SELECT x FROM t")).toEqual([{ x: 1 }]);
		await child.exec("INSERT INTO t VALUES (2)");
		expect((await ok(child, { synced: 10_000 })).synced).toMatchObject({ synced: true });
		await child.stop();
	});

	it("binds in a fresh cache once the attempt ends", async () => {
		const s = stream("handout-fresh");
		const hold = freshDir();
		const child = await owner(s.url, freshDir(), hold);
		await commitUntilHeld(child, hold);
		await ok(child, { close: true });
		// The binding is given up and the open waits for the attempt to end: in the background, so a
		// call that returns at once does.
		const fresh = freshDir();
		expect((await ok(child, { bind: fresh, timeout: 0 })).opened).toMatchObject({ done: false });
		writeFileSync(join(hold, "release"), "");
		const again = (await ok(child, { bind: fresh })).opened as Opened;
		expect(again.path).toContain(basename(fresh));
		await ok(child, { connect: true });
		expect(await child.rows("SELECT x FROM t")).toEqual([{ x: 1 }]);
		const st = (await ok(child, { status: true })).status as { state: string; attach: { local: string } };
		expect(st).toMatchObject({ state: "live", attach: { local: "built" } });
		await child.stop();
	});

	it("is refused in another cache while a connection of the host is open, naming the cache it is open in", async () => {
		const s = stream("handout-in-use");
		const dir = freshDir();
		const hold = freshDir();
		writeFileSync(join(hold, "release"), "");
		const child = await owner(s.url, dir, hold);
		await child.exec("CREATE TABLE t(x); INSERT INTO t VALUES (1)");
		const other = freshDir();
		const refused = await child.send({ bind: other });
		expect(refused.ok).toBe(false);
		expect(String(refused.error)).toMatch(/ursula_open: bound_elsewhere: /);
		expect(String(refused.error)).toContain(`the stream is open in this process under ${realpathSync(dir)}, not ${realpathSync(other)}: connections are open on it (1)`);
		// The binding is untouched.
		await child.exec("INSERT INTO t VALUES (2)");
		expect(await child.rows("SELECT x FROM t ORDER BY x")).toEqual([{ x: 1 }, { x: 2 }]);
		await child.stop();
	});
});

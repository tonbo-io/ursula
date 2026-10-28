// Reopens beside a reset database's connections, against the e2e server (test/stack.ts): `open()`
// after a reset need not wait for the old connections (design: Materialize, Resets). It builds from
// a copy of their generation, so their open read transaction keeps its snapshot, their new
// statements fail, and the new database carries every commit the old one logged. Another process
// still gets `in_use` while either holds the cache, and a crash during the copy leaves one
// generation.
import { readdirSync } from "node:fs";
import { basename, dirname, join } from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { loadUrsulaVfs, open, UrsulaResetError } from "../src/index.ts";
import { Child, ext, freshDir, fromStream, killChildren, rejection, stream, thrown } from "./kit.ts";

loadUrsulaVfs(ext());

afterEach(killChildren);

const SQLITE_IOERR_READ = 266;
const SQLITE_IOERR_WRITE = 778;

interface Commits {
	logged: number;
	confirmed: number;
}

/** The root directory of the one stream cached in `dir`. */
function rootOf(dir: string): string {
	const [root, ...others] = readdirSync(dir);
	expect(others).toEqual([]);
	return join(dir, root as string);
}

/** What the root of the one stream cached in `dir` holds besides its lock, sorted. */
function entries(dir: string): string[] {
	return readdirSync(rootOf(dir))
		.filter((name) => name !== "lock")
		.sort();
}

/** Another process opening the cache in `dir` is refused `in_use`. */
async function refusedInUse(url: string, dir: string): Promise<void> {
	const intruder = new Child(url, dir);
	const refused = await intruder.first();
	expect(refused.ok).toBe(false);
	expect(String(refused.error)).toMatch(/ursula_open: in_use: /);
	await intruder.stop();
}

/**
 * A child whose next commit is published locally but unconfirmed (an attached database fails phase
 * one), which resets it `local_damage` with the commit in its log. `env` is added to the child's.
 */
async function damaged(url: string, dir: string, env: NodeJS.ProcessEnv = {}): Promise<Child> {
	const child = new Child(url, dir, { URSULA_VFS_FAIL_SYNC: "other.db-journal", ...env });
	await child.opened();
	await child.exec("CREATE TABLE t(x)");
	await child.exec(`ATTACH '${join(freshDir(), "other.db")}' AS other; PRAGMA other.synchronous=OFF; CREATE TABLE other.u(y)`);
	await child.exec("PRAGMA other.synchronous=FULL");
	const commit = await child.send({ exec: "BEGIN; INSERT INTO t VALUES (1); INSERT INTO other.u VALUES (1); COMMIT" });
	expect(commit.ok).toBe(false);
	const st = (await child.send({ status: true })).status as { reset: unknown };
	expect(st.reset).toEqual({ kind: "local_damage", action: "reopen", class: "reset" });
	return child;
}

describe("a reopen beside a reset database's connections", () => {
	it("succeeds after a fenced reset: the old read keeps its snapshot, its new statements fail, the new database commits", async () => {
		const s = stream("fenced");
		const dir = freshDir();
		const owner = await open(s.url, { dir });
		owner.exec("CREATE TABLE t(x)");
		owner.exec("INSERT INTO t VALUES (1)");
		await owner.synced();
		// A read transaction open across the reset and the reopen.
		owner.exec("BEGIN");
		expect(owner.prepare("SELECT x FROM t").all()).toEqual([{ x: 1 }]);

		const taker = new Child(s.url, freshDir());
		await taker.opened();
		await taker.exec("INSERT INTO t VALUES (2)");
		expect((await taker.send({ synced: 10_000 })).synced).toMatchObject({ synced: true });
		await taker.stop();
		expect(await rejection(owner.synced())).toMatchObject({ kind: "fenced", action: "stop" });

		const again = await open(s.url, { dir });
		const { commits } = again.status();
		expect(commits.confirmed).toBe(commits.logged);
		expect(again.prepare("SELECT x FROM t ORDER BY x").all()).toEqual([{ x: 1 }, { x: 2 }]);
		expect(again.location()).not.toBe(owner.location());
		// One generation remains, the copy: the original went to the trash under the old reader.
		expect(entries(dir)).toEqual([basename(dirname(again.location() as string))]);
		await refusedInUse(s.url, dir);

		// The old read transaction still reads its snapshot. A write in it fails at its commit (778),
		// which ends the transaction, and then a new read fails (266).
		expect(owner.prepare("SELECT x FROM t").all()).toEqual([{ x: 1 }]);
		const write = thrown(() => owner.exec("INSERT INTO t VALUES (9); COMMIT"));
		expect(write).toBeInstanceOf(UrsulaResetError);
		expect(write).toMatchObject({ kind: "fenced", errcode: SQLITE_IOERR_WRITE });
		expect(owner.isTransaction).toBe(false);
		const read = thrown(() => owner.prepare("SELECT x FROM t").all());
		expect(read).toBeInstanceOf(UrsulaResetError);
		expect(read).toMatchObject({ kind: "fenced", errcode: SQLITE_IOERR_READ });

		again.exec("INSERT INTO t VALUES (3)");
		await again.synced();
		owner.close();
		await refusedInUse(s.url, dir);
		again.close();
		expect(await fromStream(s.url, "SELECT x FROM t ORDER BY x")).toEqual([{ x: 1 }, { x: 2 }, { x: 3 }]);
	});

	it("replays the unconfirmed commit after a local_damage reset", async () => {
		const s = stream("damaged");
		const dir = freshDir();
		const child = await damaged(s.url, dir);
		expect(await child.send({ all: "SELECT x FROM t" })).toMatchObject({ ok: false, errcode: SQLITE_IOERR_READ });
		const reopened = await child.send({ reopen: true });
		expect(reopened).toMatchObject({ ok: true, opened: { done: true, state: "live" } });
		expect(await child.send({ conn: 3, all: "SELECT x FROM t" })).toEqual({ ok: true, rows: [{ x: 1 }] });
		const commits = ((await child.send({ conn: 3, status: true })).status as { commits: Commits }).commits;
		expect(commits.confirmed).toBe(commits.logged);
		expect((await child.send({ conn: 3, synced: 10_000 })).synced).toMatchObject({ synced: true });
		await refusedInUse(s.url, dir);
		expect(await child.send({ conn: 3, exec: "INSERT INTO t VALUES (2)" })).toEqual({ ok: true });
		expect((await child.send({ conn: 3, synced: 10_000 })).synced).toMatchObject({ synced: true });
		await child.stop();
		expect(await fromStream(s.url, "SELECT x FROM t ORDER BY x")).toEqual([{ x: 1 }, { x: 2 }]);
	});

	for (const step of ["copy_db", "copy_log"]) {
		it(`leaves one generation when the process dies during the copy (${step})`, async () => {
			const s = stream(step);
			const dir = freshDir();
			const child = await damaged(s.url, dir, { URSULA_VFS_ABORT_AT: step });
			const [before] = entries(dir);
			expect(await child.send({ reopen: true }).catch((error: unknown) => error)).toBeInstanceOf(Error);
			expect(await child.exit).toMatchObject({ signal: "SIGKILL" });
			// The generation, and the copy's `.tmp` as far as the copy got.
			const left = entries(dir);
			expect(left).toHaveLength(2);
			const tmp = left.find((name) => name.endsWith(".tmp")) as string;
			expect(left).toEqual([before, tmp].sort());
			expect(readdirSync(join(rootOf(dir), tmp)).sort()).toEqual(step === "copy_db" ? ["db"] : ["db", "log"]);
			const next = new Child(s.url, dir);
			await next.opened();
			expect(entries(dir)).toEqual([before]);
			expect(await next.rows("SELECT x FROM t")).toEqual([{ x: 1 }]);
			expect((await next.send({ synced: 10_000 })).synced).toMatchObject({ synced: true });
			await next.stop();
			expect(await fromStream(s.url, "SELECT x FROM t")).toEqual([{ x: 1 }]);
		});
	}
});

// The host directory (design: Hosts), with real child processes against the e2e server
// (test/stack.ts). An owner open records the real path of its cache directory in the host
// directory, as `dir-<key>`, before it claims anything. URSULA_VFS_HOST_DIR, the test hook, points
// each child at a temporary host directory. The hook is read only when the compiled-in
// /var/lib/sqlite-ursula/host does not exist, so the suite refuses to run on a runner that has it.
// - H1: an owner open leaves the record before its first commit, naming the real path of a `dir`
//   given through a symlink, with mode 0444. Another process that opens another stream in the same
//   `dir` leaves it as it is, and so do four processes that open four streams there at once.
// - H2: a read-only open and an import record nothing.
// - H3: with a regular file at the host directory's path, the open fails with `UrsulaSetupError`
//   `local_setup` (action `fix`), and nothing was created on the stream. With a directory there,
//   the next open succeeds.
// - H4: with no host directory the extension writes nothing there, as in every other suite.
// - H5: SIGKILL at each step of an open's record (URSULA_VFS_ABORT_AT `host_tmp_synced`,
//   `host_linked`, `host_dir_synced`), and at steps after it, leaves a complete record or none,
//   at most a `.tmp-*` beside it, and a root with no generation when the open stopped before it
//   built one. The next open needs no manual step and records the directory.
import { blake3 } from "@noble/hashes/blake3.js";
import { lstatSync, mkdirSync, readdirSync, readFileSync, realpathSync, rmSync, statSync, symlinkSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import { afterEach, beforeAll, describe, expect, it } from "vitest";
import { Child, directUrl, freshDir, killChildren, stream } from "./kit.ts";

afterEach(killChildren);

/** The compiled-in host directory, which would win over the hook. */
const DEFAULT = "/var/lib/sqlite-ursula/host";

beforeAll(() => {
	let found = true;
	try {
		lstatSync(DEFAULT);
	} catch {
		found = false;
	}
	if (found) throw new Error(`${DEFAULT} exists on this runner: the extension would record there, not in the suite's host directories`);
});

/** A new, empty host directory, and the environment that points a child at it. */
function hostDir(): { host: string; env: NodeJS.ProcessEnv } {
	const host = join(freshDir(), "host");
	mkdirSync(host);
	return { host, env: { URSULA_VFS_HOST_DIR: host } };
}

/** The record's name for the cache directory `dir`: `dir-` and the first 32 hex digits of the BLAKE3 of its real path. */
const recordName = (dir: string): string => `dir-${Buffer.from(blake3(new TextEncoder().encode(realpathSync(dir)))).toString("hex").slice(0, 32)}`;

/** The record of the cache directory `dir`. */
const recordLine = (dir: string): string => `${JSON.stringify({ v: 1, dir: realpathSync(dir) })}\n`;

/** The names in `dir`, sorted. */
const names = (dir: string): string[] => readdirSync(dir).sort();

/** The one stream root in the cache root `dir`, and its entries. */
function rootEntries(dir: string): string[] {
	const roots = readdirSync(dir);
	expect(roots).toHaveLength(1);
	return names(join(dir, roots[0] as string));
}

/** The status of a HEAD of the stream at `path`, outside the gate. */
async function headStatus(path: string): Promise<number> {
	return (await fetch(`${directUrl()}${path}`, { method: "HEAD" })).status;
}

/** Opens, commits and syncs in a process of its own with `env`, then exits: its record checked by the caller. */
async function ownerCommits(url: string, dir: string, env: NodeJS.ProcessEnv): Promise<void> {
	const c = new Child(url, dir, env);
	await c.opened();
	await c.exec("CREATE TABLE IF NOT EXISTS t(x); INSERT INTO t VALUES (1)");
	expect((await c.send({ synced: 20_000 })).synced).toMatchObject({ synced: true });
	await c.stop();
}

describe("host directory", () => {
	it("H1: an owner open records the real path of its cache directory before its first commit, once per directory", async () => {
		const { host, env } = hostDir();
		const real = freshDir();
		const alias = join(freshDir(), "cache");
		symlinkSync(real, alias);
		const name = recordName(real);

		const first = new Child(stream("host-h1a").url, alias, env);
		await first.opened();
		// Recorded before anything was committed.
		expect(names(host)).toEqual([name]);
		const record = join(host, name);
		expect(readFileSync(record, "utf8")).toBe(recordLine(real));
		const before = statSync(record);
		expect(before.mode & 0o7777).toBe(0o444);
		await first.exec("CREATE TABLE t(x); INSERT INTO t VALUES (1)");
		expect((await first.send({ synced: 20_000 })).synced).toMatchObject({ synced: true });

		// Another stream in the same directory, through its real path, while the first is open.
		await ownerCommits(stream("host-h1b").url, real, env);
		await first.stop();
		expect(names(host)).toEqual([name]);
		const after = statSync(record);
		expect(readFileSync(record, "utf8")).toBe(recordLine(real));
		expect([after.ino, after.mtimeMs]).toEqual([before.ino, before.mtimeMs]);

		// Four processes opening four streams in a fresh directory at once leave one record.
		const shared = freshDir();
		const racers = [0, 1, 2, 3].map((i) => new Child(stream(`host-h1-race${i}`).url, shared, env));
		await Promise.all(racers.map((c) => c.opened()));
		for (const c of racers) await c.stop();
		expect(names(host).sort()).toEqual([name, recordName(shared)].sort());
		expect(readFileSync(join(host, recordName(shared)), "utf8")).toBe(recordLine(shared));
	});

	it("H2: a read-only open and an import record nothing", async () => {
		const { host, env } = hostDir();
		const s = stream("host-h2");
		await ownerCommits(s.url, freshDir(), {});

		const reader = new Child(s.url, freshDir(), env, "core-child.mjs", ['{"read_only":true}']);
		await reader.opened();
		expect(await reader.rows("SELECT x FROM t")).toEqual([{ x: 1 }]);
		await reader.stop();

		const files = freshDir();
		const file = join(files, "plain.db");
		const plain = new DatabaseSync(file);
		plain.exec("CREATE TABLE t(x); INSERT INTO t VALUES (2)");
		plain.close();
		const importer = new Child(stream("host-h2-import").url, file, env, "import-child.mjs");
		const imported = await importer.first();
		await importer.stop();
		expect(imported.ok, JSON.stringify(imported)).toBe(true);

		expect(names(host)).toEqual([]);
	});

	it("H3: a host directory the open cannot record in fails it with local_setup, before anything is created on the stream", async () => {
		// A regular file in its place: no directory can be opened there, for root too.
		const host = join(freshDir(), "host");
		writeFileSync(host, "");
		const env = { URSULA_VFS_HOST_DIR: host };
		const dir = freshDir();
		const s = stream("host-h3");

		const refused = new Child(s.url, dir, env, "host-child.ts");
		const reply = await refused.first();
		await refused.stop();
		expect(reply).toMatchObject({ ok: false, name: "UrsulaSetupError", kind: "local_setup", action: "fix" });
		expect(String(reply.message)).toContain(`the host directory ${host}`);
		// Nothing was claimed or created: the stream does not exist.
		expect(await headStatus(s.path)).toBe(404);
		// The root lock was released with the open, and the root holds no generation.
		expect(rootEntries(dir)).toEqual(["lock"]);

		rmSync(host);
		mkdirSync(host);
		const opened = new Child(s.url, dir, env, "host-child.ts");
		expect(await opened.first()).toEqual({ ok: true });
		await opened.stop();
		expect(names(host)).toEqual([recordName(dir)]);
	});

	it("H4: without a host directory nothing is recorded", async () => {
		const parent = freshDir();
		const missing = join(parent, "host");
		await ownerCommits(stream("host-h4").url, freshDir(), { URSULA_VFS_HOST_DIR: missing });
		expect(names(parent)).toEqual([]);
	});
});

/**
 * Each step SIGKILLs the open (or the commit after it), with the cache root fresh (the open
 * publishes the record) or recorded by an earlier process (the open finds it), and what the host
 * directory holds after: a temp file, the record, or both.
 */
const CRASHES: readonly (readonly [step: string, cache: "fresh" | "recorded", left: "tmp" | "both" | "record"])[] = [
	["host_tmp_synced", "fresh", "tmp"],
	["host_linked", "fresh", "both"],
	["host_dir_synced", "fresh", "record"],
	["host_dir_synced", "recorded", "record"],
	["publish_renamed", "fresh", "record"],
	["logged", "recorded", "record"],
];

const TMP = /^\.tmp-\d+-[0-9a-f]{16}$/;

describe("crashes while an owner open records its cache directory (URSULA_VFS_ABORT_AT)", () => {
	for (const [step, cache, left] of CRASHES) {
		it(`${step} (${cache} cache directory)`, async () => {
			const { host, env } = hostDir();
			const dir = freshDir();
			const s = stream(`host-crash-${step}`);
			const name = recordName(dir);
			const line = recordLine(dir);
			if (cache === "recorded") {
				const setup = new Child(s.url, dir, env);
				await setup.opened();
				await setup.exec("CREATE TABLE t(i INTEGER NOT NULL, pad BLOB); CREATE TABLE counter(n INTEGER NOT NULL); INSERT INTO counter VALUES (0)");
				expect((await setup.send({ synced: 20_000 })).synced).toMatchObject({ synced: true });
				await setup.stop();
			}
			const recorded = cache === "recorded" ? statSync(join(host, name)).ino : undefined;

			const doomed = new Child(s.url, dir, { ...env, URSULA_VFS_ABORT_AT: step }, "crash-child.mjs", ["10", "auto:4"]);
			for (let out = await doomed.line(); out !== undefined; out = await doomed.line()) {
				expect(out.done, `${step} never killed the child`).toBeUndefined();
			}
			expect((await doomed.exit).signal, `the child ended without the kill at ${step}`).toBe("SIGKILL");

			const entries = names(host);
			const tmps = entries.filter((n) => TMP.test(n));
			if (left === "tmp") {
				// Killed before the link: a complete temp file, no record, nothing sent.
				expect(entries).toEqual(tmps);
				expect(tmps).toHaveLength(1);
			} else if (left === "both") {
				// Killed between the link and the unlink: the record and the temp file are one file.
				expect(tmps).toHaveLength(1);
				expect(entries).toEqual([...tmps, name]);
				expect(statSync(join(host, tmps[0] as string)).ino).toBe(statSync(join(host, name)).ino);
			} else {
				expect(entries).toEqual([name]);
			}
			for (const n of entries) {
				expect(readFileSync(join(host, n), "utf8")).toBe(line);
				expect(statSync(join(host, n)).mode & 0o7777).toBe(0o444);
			}
			if (recorded !== undefined) expect(statSync(join(host, name)).ino).toBe(recorded);
			if (step.startsWith("host_") && cache === "fresh") {
				// The open stopped before it built anything or asked the stream.
				expect(rootEntries(dir)).toEqual(["lock"]);
				expect(await headStatus(s.path)).toBe(404);
			}

			// The next open needs no manual step, records the directory, and commits.
			const reopened = new Child(s.url, dir, env);
			await reopened.opened();
			expect(names(host)).toEqual([...tmps, name]);
			expect(readFileSync(join(host, name), "utf8")).toBe(line);
			if (recorded !== undefined) expect(statSync(join(host, name)).ino).toBe(recorded);
			await reopened.exec("CREATE TABLE IF NOT EXISTS later(x); INSERT INTO later VALUES (1)");
			expect((await reopened.send({ synced: 20_000 })).synced).toMatchObject({ synced: true });
			await reopened.stop();
		});
	}
});

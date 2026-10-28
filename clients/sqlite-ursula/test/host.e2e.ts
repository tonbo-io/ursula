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
//
// The host's inspection, `ursula-vfs-host inspect` (URSULA_VFS_HOST, or the crate's release
// build), run with `--pid` set to the test's children (the test process when none runs):
// - H6: a crash at each step of the commit path and of a checkpoint (both checkpoint modes), with
//   the stream refused behind test/gate.mjs so nothing is acknowledged, leaves the bytes a
//   provisional reopen shows as `commits.pending_bytes`, and after the gate forwards and the reopen
//   closes, a clean inspection. So does a crash, with the stream forwarding, at each step of a
//   re-key while bound and at open, of a publish that salvages (and the salvaged commits are the
//   same before and after the reopen), of a fresh open's first publish, of a trim, and of an open's
//   cut of a torn tail, which the read cuts as the open does. A reopen whose generation holds no
//   CLAIM record waits for the stream instead of opening provisionally, and then nothing is
//   pending. A crash at each step of the record leaves an inspection that is complete and clean.
// - H7: a root whose log has another format is unreadable, a stray name in the host directory is
//   unrecognized, and either makes the inspection incomplete. A deleted cache directory is absent
//   and clean.
// - H8: an inspection changes no byte, name or time of the host directory or of a root.
// - H13: an orphan (a root with bytes no process holds) drains with a reopen and a clean close.
// - H14: a root held by a live process whose cache directory was renamed, or deleted, or whose root
//   directory was renamed out of the root-name grammar, is an unread lock until the process exits.
//   The renamed one is then hidden, the documented limit.
// - H15: a 0.11.0 extension, from its npm package, records nothing, and its root is read in a
//   scanned directory, its bytes counted. While it runs, its root shows `held`, and without the scan
//   its lock is unread. It runs on the 0.11 line only: a later line's server refuses a 0.11 copy.
import { blake3 } from "@noble/hashes/blake3.js";
import { execFile } from "node:child_process";
import { createHash } from "node:crypto";
import { appendFileSync, closeSync, existsSync, lstatSync, mkdirSync, openSync, readdirSync, readFileSync, realpathSync, renameSync, rmSync, statSync, symlinkSync, writeFileSync, writeSync } from "node:fs";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import { promisify } from "node:util";
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it } from "vitest";
import { Child, directUrl, freshDir, gate, killChildren, onModel, stream } from "./kit.ts";

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

/** `ursula-vfs-host`: URSULA_VFS_HOST, or the crate's release build. */
const hostBin = (): string => process.env.URSULA_VFS_HOST ?? join(import.meta.dirname, "../../sqlite-vfs/target/release/ursula-vfs-host");

const run = promisify(execFile);

const sleep = (ms: number): Promise<void> => new Promise((r) => setTimeout(r, ms));

/** A root as `ursula-vfs-host inspect` reports it. */
interface InspectedRoot {
	id: string;
	root: string;
	stream: string | null;
	cluster: string | null;
	state: "clean" | "pending" | "unreadable";
	held: boolean | null;
	unacknowledged_bytes: number | null;
	commits: number | null;
	salvaged: number | null;
	error: string | null;
}

/** What `ursula-vfs-host inspect` prints. */
interface Inspection {
	v: number;
	dir: string;
	complete: boolean;
	clean: boolean;
	incomplete: string | null;
	unacknowledged_bytes: number;
	dirs: { dir: string; from: "record" | "scan"; present: boolean; error: string | null }[];
	roots: InspectedRoot[];
	unread_locks: { pid: number; path: string }[];
	unrecognized: { name: string; error: string | null }[];
	error: string | null;
}

/** `ursula-vfs-host inspect` of the host directory `host`, its lock check limited to `pids`. */
async function inspect(host: string, pids: readonly number[], options: { scans?: readonly string[]; untilClean?: number } = {}): Promise<Inspection> {
	const args = ["inspect", "--dir", host, ...pids.flatMap((p) => ["--pid", String(p)]), ...(options.scans ?? []).flatMap((s) => ["--scan", s])];
	if (options.untilClean !== undefined) args.push("--until-clean", String(options.untilClean));
	const { stdout } = await run(hostBin(), args);
	return JSON.parse(stdout) as Inspection;
}

/** The one root an inspection lists. */
function onlyRoot(seen: Inspection): InspectedRoot {
	expect(seen.roots, JSON.stringify(seen)).toHaveLength(1);
	return seen.roots[0] as InspectedRoot;
}

/** Closes the database of `c`, waits for its binding to retire (a clean retire writes an exact ACK), and ends the process. */
async function closeAndRetire(c: Child): Promise<void> {
	expect((await c.send({ close: true })).ok).toBe(true);
	const deadline = Date.now() + 10_000;
	for (;;) {
		const reply = await c.send({ status: true });
		if ((reply.status as { state: string }).state === "retired") break;
		expect(Date.now(), "the binding never retired").toBeLessThan(deadline);
		await sleep(20);
	}
	await c.stop();
}

/** The tables test/crash-child.mjs writes, created by a process of its own that syncs and exits. */
async function crashTables(url: string, dir: string, env: NodeJS.ProcessEnv): Promise<void> {
	const setup = new Child(url, dir, env);
	await setup.opened();
	await setup.exec("CREATE TABLE t(i INTEGER NOT NULL, pad BLOB); CREATE TABLE counter(n INTEGER NOT NULL); INSERT INTO counter VALUES (0)");
	expect((await setup.send({ synced: 20_000 })).synced).toMatchObject({ synced: true });
	await setup.stop();
}

/** How test/crash-child.mjs checkpoints: every few pages, or a TRUNCATE checkpoint every few commits. */
type Checkpoints = "auto:4" | "truncate:3";

/** Runs test/crash-child.mjs until URSULA_VFS_ABORT_AT `abort` kills it. */
async function crash(url: string, dir: string, env: NodeJS.ProcessEnv, abort: string, checkpoints: Checkpoints = "auto:4"): Promise<void> {
	const doomed = new Child(url, dir, { ...env, URSULA_VFS_ABORT_AT: abort }, "crash-child.mjs", ["10", checkpoints]);
	for (let out = await doomed.line(); out !== undefined; out = await doomed.line()) {
		expect(out.done, `${abort} never killed the child`).toBeUndefined();
	}
	expect((await doomed.exit).signal, `the child ended without the kill at ${abort}`).toBe("SIGKILL");
}

/** Every entry under `dir` with its mode, size, inode and times (access times aside), and each file's digest. */
function tree(dir: string): string[] {
	const out: string[] = [];
	const walk = (path: string): void => {
		const st = lstatSync(path, { bigint: true });
		const line = `${path} ${st.mode} ${st.size} ${st.ino} ${st.mtimeNs} ${st.ctimeNs}`;
		if (st.isDirectory()) {
			out.push(line);
			for (const name of readdirSync(path).sort()) walk(join(path, name));
		} else {
			out.push(`${line} ${st.isFile() ? createHash("sha256").update(readFileSync(path)).digest("hex") : ""}`);
		}
	};
	walk(dir);
	return out;
}

/**
 * The commit path and checkpoint steps of test/crash.e2e.ts, each at an occurrence after the first
 * commit, with the checkpoint mode. A TRUNCATE checkpoint's even F2 occurrences are the truncate to
 * nothing.
 */
const COMMIT_STEPS: readonly (readonly [abort: string, checkpoints: Checkpoints])[] = [
	["admission:2", "auto:4"],
	["capture:2", "auto:4"],
	["encode:2", "auto:4"],
	["logged:2", "auto:4"],
	["forward:2", "auto:4"],
	["phasetwo:2", "auto:4"],
	["checkpoint_start:1", "auto:4"],
	["checkpoint_synced:1", "auto:4"],
	["checkpoint_page:1", "auto:4"],
	["base_start:1", "auto:4"],
	["base_synced:1", "auto:4"],
	["based:1", "auto:4"],
	["checkpoint_start:1", "truncate:3"],
	["checkpoint_synced:1", "truncate:3"],
	["checkpoint_page:1", "truncate:3"],
	["base_start:2", "truncate:3"],
	["base_synced:2", "truncate:3"],
	["based:2", "truncate:3"],
];

/** The table test/crash.e2e.ts's second matrix writes. */
const TABLE = "CREATE TABLE IF NOT EXISTS t(x INTEGER NOT NULL, pad BLOB)";

/** A history event of the model, restoring `checkpoint` if given. */
async function history(checkpoint?: string): Promise<void> {
	const query = checkpoint === undefined ? "" : `?restore=${checkpoint}`;
	const r = await fetch(`${directUrl()}/__model/history${query}`, { method: "POST" });
	expect(r.ok, await r.text()).toBe(true);
}

/** A checkpoint of every stream the model holds: its id. */
async function checkpoint(): Promise<string> {
	const r = await fetch(`${directUrl()}/__model/checkpoint`, { method: "POST" });
	const id = (await r.text()).trim();
	expect(r.ok, id).toBe(true);
	return id;
}

async function synced(child: Child): Promise<void> {
	expect((await child.send({ synced: 20_000 })).synced).toMatchObject({ synced: true });
}

/** The child dies by its URSULA_VFS_ABORT_AT `step`. */
async function killedAt(child: Child, step: string): Promise<void> {
	const ended = await Promise.race([child.exit, sleep(60_000).then(() => ({ signal: "timeout" }))]);
	expect(ended.signal, `never killed at ${step}`).toBe("SIGKILL");
	await child.stop();
}

/** Binds `url` in `dir` in a child, runs `work`, then closes the database and waits for its binding to retire. */
async function closedOwner(url: string, dir: string, env: NodeJS.ProcessEnv, work: (child: Child) => Promise<void>): Promise<void> {
	const child = new Child(url, dir, env);
	await child.opened();
	await work(child);
	await closeAndRetire(child);
}

describe("the host's inspection (ursula-vfs-host inspect)", () => {
	let front: Awaited<ReturnType<typeof gate>>;
	beforeAll(async () => {
		front = await gate(directUrl());
	});
	afterAll(async () => {
		await front?.stop();
	});
	beforeEach(async () => {
		await mode("forward");
	});

	async function mode(m: "forward" | "refuse"): Promise<void> {
		const r = await fetch(`${front.control}/mode`, { method: "PUT", body: JSON.stringify({ mode: m }) });
		expect(r.status, await r.text()).toBe(204);
	}

	/**
	 * After a crash in `dir`, with nothing running: a complete inspection whose bytes are the
	 * `commits.pending_bytes` of a reopen with the stream refused, which opens provisionally on a
	 * generation with a CLAIM record. Without one the reopen waits for the stream, fails at its
	 * deadline, and nothing is pending. Then a reopen with the stream forwarding drains the root to
	 * clean. Returns the root after the crash and after the drain.
	 */
	async function resendsWhatItRead(url: string, dir: string, host: string, env: NodeJS.ProcessEnv): Promise<{ crashed: InspectedRoot; drained: InspectedRoot }> {
		await mode("refuse");
		const seen = await inspect(host, [process.pid]);
		expect(seen, JSON.stringify(seen)).toMatchObject({ complete: true, incomplete: null, error: null, unread_locks: [] });
		const crashed = onlyRoot(seen);
		expect(crashed, JSON.stringify(seen)).toMatchObject({ held: false, error: null });
		expect(["clean", "pending"]).toContain(crashed.state);
		const offline = new Child(url, dir, env);
		const first = await offline.first();
		if (first.ok === true) {
			expect((first.opened as { state: string }).state).toBe("provisional");
			const status = (await offline.send({ status: true })).status as { commits: { pending_bytes: number } };
			expect(status.commits.pending_bytes).toBe(crashed.unacknowledged_bytes);
			const held = onlyRoot(await inspect(host, [offline.pid]));
			expect(held).toMatchObject({ held: true, unacknowledged_bytes: crashed.unacknowledged_bytes, salvaged: crashed.salvaged });
		} else {
			// No CLAIM record to continue: the generation holds no commit of its own.
			expect(crashed.unacknowledged_bytes, JSON.stringify(first)).toBe(0);
		}
		await offline.stop();

		await mode("forward");
		const next = new Child(url, dir, env);
		await next.opened();
		await synced(next);
		await closeAndRetire(next);
		const drained = await inspect(host, [process.pid], { untilClean: 5_000 });
		expect(drained, JSON.stringify(drained)).toMatchObject({ complete: true, clean: true, unacknowledged_bytes: 0 });
		return { crashed, drained: onlyRoot(drained) };
	}

	for (const [abort, checkpoints] of COMMIT_STEPS) {
		it(`H6: a crash at ${abort} (${checkpoints}) with the stream refused leaves the bytes the reopen resends, then drains to clean`, async () => {
			const { host, env } = hostDir();
			const dir = freshDir();
			const s = stream(`host-h6-${abort.replace(":", "-")}-${checkpoints.replace(":", "-")}`);
			const url = `${front.url}${s.path}`;
			await crashTables(url, dir, env);
			await mode("refuse");
			await crash(url, dir, env, abort, checkpoints);

			const seen = await inspect(host, [process.pid]);
			expect(seen, JSON.stringify(seen)).toMatchObject({ complete: true, clean: false, incomplete: null, error: null });
			const root = onlyRoot(seen);
			expect(root).toMatchObject({ state: "pending", held: false, stream: s.path, error: null });
			expect(root.unacknowledged_bytes).toBeGreaterThan(0);
			expect(seen.unacknowledged_bytes).toBe(root.unacknowledged_bytes);

			// The reopen reads the log by the same rule and binds provisionally: the stream is refused.
			const reopened = new Child(url, dir, env);
			expect((await reopened.opened()).state).toBe("provisional");
			const status = (await reopened.send({ status: true })).status as { commits: { pending_bytes: number } };
			expect(status.commits.pending_bytes).toBe(root.unacknowledged_bytes);
			const held = onlyRoot(await inspect(host, [reopened.pid]));
			expect(held).toMatchObject({ held: true, unacknowledged_bytes: root.unacknowledged_bytes });

			await mode("forward");
			expect((await reopened.send({ synced: 20_000 })).synced).toMatchObject({ synced: true });
			await closeAndRetire(reopened);
			const drained = await inspect(host, [process.pid], { untilClean: 5_000 });
			expect(drained, JSON.stringify(drained)).toMatchObject({ complete: true, clean: true, unacknowledged_bytes: 0 });
			expect(onlyRoot(drained).state).toBe("clean");
		});
	}

	for (const step of ["rekey_rolled", "rekey_claimed"]) {
		it.skipIf(!onModel())(`H6: a crash at ${step} in a re-key while bound leaves the bytes the reopen resends`, async () => {
			const { host, env } = hostDir();
			const dir = freshDir();
			const url = `${front.url}${stream(`host-h6-${step}-bound`).path}`;
			const doomed = new Child(url, dir, { ...env, URSULA_VFS_ABORT_AT: step });
			await doomed.opened();
			await doomed.exec(`${TABLE}; INSERT INTO t(x) VALUES (1)`);
			await synced(doomed);
			await history();
			// The commit meets the new token, and the re-key it starts dies at `step`.
			await doomed.send({ exec: "INSERT INTO t(x) VALUES (2)" }).catch(() => undefined);
			await killedAt(doomed, step);
			await resendsWhatItRead(url, dir, host, env);
		});

		it.skipIf(!onModel())(`H6: a crash at ${step} in a re-key at open leaves the bytes the reopen resends`, async () => {
			const { host, env } = hostDir();
			const dir = freshDir();
			const url = `${front.url}${stream(`host-h6-${step}-open`).path}`;
			await closedOwner(url, dir, env, async (child) => {
				await child.exec(`${TABLE}; INSERT INTO t(x) VALUES (1)`);
				await synced(child);
			});
			await history();
			await killedAt(new Child(url, dir, { ...env, URSULA_VFS_ABORT_AT: step }), step);
			await resendsWhatItRead(url, dir, host, env);
		});
	}

	for (const step of ["salvage_marked", "publish_renamed", "salvage_moved"]) {
		it.skipIf(!onModel())(`H6: a crash at ${step} in a salvage after a lossy restore leaves the bytes the reopen resends, and its salvaged commits`, async () => {
			const { host, env } = hostDir();
			const dir = freshDir();
			const url = `${front.url}${stream(`host-h6-${step}`).path}`;
			const owner = new Child(url, dir, env);
			await owner.opened();
			await owner.exec(TABLE);
			await synced(owner);
			const saved = await checkpoint();
			await owner.exec("INSERT INTO t(x) VALUES (1)");
			await synced(owner);
			// A commit the stream never acknowledges, which the salvage lists as unacknowledged.
			await mode("refuse");
			await owner.exec("INSERT INTO t(x) VALUES (2)");
			await owner.stop("SIGKILL");
			await mode("forward");
			expect(onlyRoot(await inspect(host, [process.pid])).unacknowledged_bytes).toBeGreaterThan(0);

			// The restore lost commit 1: the open salvages the generation and dies at `step`.
			await history(saved);
			await killedAt(new Child(url, dir, { ...env, URSULA_VFS_ABORT_AT: step }), step);
			const { crashed, drained } = await resendsWhatItRead(url, dir, host, env);
			if (step !== "salvage_marked") {
				// A superseded generation with its manifest, or the salvage it became: the next open
				// finishes the move, and the count stays.
				expect(crashed.salvaged).toBeGreaterThan(0);
				expect(drained.salvaged).toBe(crashed.salvaged);
			} else {
				// Marked, never published: the next open unmarks it, and the forwarding one salvages it.
				expect(crashed.salvaged).toBe(0);
				expect(drained.salvaged).toBeGreaterThan(0);
			}
		});
	}

	it("H6: a crash at publish_renamed in a fresh open's first publish leaves a clean root", async () => {
		const { host, env } = hostDir();
		const dir = freshDir();
		const url = `${front.url}${stream("host-h6-publish-fresh").path}`;
		await killedAt(new Child(url, dir, { ...env, URSULA_VFS_ABORT_AT: "publish_renamed" }), "publish_renamed");
		const { crashed } = await resendsWhatItRead(url, dir, host, env);
		expect(crashed).toMatchObject({ state: "clean", unacknowledged_bytes: 0, salvaged: 0 });
	});

	it("H6: a crash at trim_unlinked, a trim below the ACK and the BASE, leaves the bytes the reopen resends", async () => {
		const { host, env } = hostDir();
		const dir = freshDir();
		const url = `${front.url}${stream("host-h6-trim").path}`;
		const doomed = new Child(url, dir, { ...env, URSULA_VFS_ABORT_AT: "trim_unlinked" });
		await doomed.opened();
		await doomed.exec(TABLE);
		// 1 MB commits: the log rolls at 16 MiB, and checkpoints move the BASE past the first segment.
		for (let x = 1; x <= 30; x++) {
			const reply = await doomed.send({ exec: `INSERT INTO t VALUES (${x}, randomblob(1000000))` }).catch(() => undefined);
			if (reply?.ok !== true) break;
		}
		await killedAt(doomed, "trim_unlinked");
		await resendsWhatItRead(url, dir, host, env);
	});

	it("H6: a torn tail is cut by the read as by the open, and a crash at cut_removed leaves the bytes the reopen resends", async () => {
		const { host, env } = hostDir();
		const dir = freshDir();
		const url = `${front.url}${stream("host-h6-cut").path}`;
		const owner = new Child(url, dir, env);
		await owner.opened();
		await owner.exec(`${TABLE}; INSERT INTO t(x) VALUES (1)`);
		await synced(owner);
		await mode("refuse");
		await owner.exec("INSERT INTO t(x) VALUES (2)");
		await owner.stop("SIGKILL");
		await mode("forward");
		// A torn record at the log's tail, as a power loss can leave one.
		const root = onlyRoot(await inspect(host, [process.pid])).root;
		const generation = readdirSync(root).find((n) => /^gen-[0-9a-f]{32}$/.test(n));
		expect(generation).toBeDefined();
		const log = join(root, generation as string, "log");
		const last = readdirSync(log)
			.filter((n) => n.endsWith(".usl"))
			.sort()
			.at(-1) as string;
		appendFileSync(join(log, last), Buffer.alloc(64, 0xff));
		const torn = await inspect(host, [process.pid]);
		expect(torn, JSON.stringify(torn)).toMatchObject({ complete: true, clean: false });
		expect(onlyRoot(torn).unacknowledged_bytes).toBeGreaterThan(0);

		await killedAt(new Child(url, dir, { ...env, URSULA_VFS_ABORT_AT: "cut_removed" }), "cut_removed");
		const { crashed } = await resendsWhatItRead(url, dir, host, env);
		expect(crashed.unacknowledged_bytes).toBe(onlyRoot(torn).unacknowledged_bytes);
	});

	for (const step of ["host_tmp_synced", "host_linked", "host_dir_synced"]) {
		it(`H6: a crash at ${step} leaves an inspection that is complete and clean`, async () => {
			const { host, env } = hostDir();
			const dir = freshDir();
			await crash(stream(`host-h6-${step}`).url, dir, env, step);
			const seen = await inspect(host, [process.pid]);
			expect(seen, JSON.stringify(seen)).toMatchObject({ complete: true, clean: true, unrecognized: [], roots: [] });
		});
	}

	it("H7: an unreadable root and an unrecognized name make the inspection incomplete, and a deleted cache directory is clean", async () => {
		const { host, env } = hostDir();
		const dir = freshDir();
		await ownerCommits(stream("host-h7").url, dir, env);
		const seen = await inspect(host, [process.pid]);
		expect(seen, JSON.stringify(seen)).toMatchObject({ complete: true, dirs: [{ dir: realpathSync(dir), from: "record", present: true, error: null }] });
		const root = onlyRoot(seen);

		// Another format's magic in the first segment of the generation's log.
		const gen = readdirSync(root.root).find((n) => /^gen-[0-9a-f]{32}$/.test(n));
		expect(gen).toBeDefined();
		const log = join(root.root, gen as string, "log");
		const first = readdirSync(log).sort()[0] as string;
		const fd = openSync(join(log, first), "r+");
		writeSync(fd, Buffer.from("XXXX"), 0, 4, 0);
		closeSync(fd);
		const damaged = await inspect(host, [process.pid]);
		expect(damaged).toMatchObject({ complete: false, clean: false, incomplete: "unreadable" });
		expect(onlyRoot(damaged)).toMatchObject({ state: "unreadable", unacknowledged_bytes: null });
		expect(onlyRoot(damaged).error).not.toBeNull();

		writeFileSync(join(host, "stray"), "");
		const stray = await inspect(host, [process.pid]);
		// Unrecognized comes first.
		expect(stray).toMatchObject({ complete: false, incomplete: "unrecognized", unrecognized: [{ name: "stray", error: null }] });
		rmSync(join(host, "stray"));

		rmSync(dir, { recursive: true, force: true });
		const gone = await inspect(host, [process.pid]);
		expect(gone, JSON.stringify(gone)).toMatchObject({ complete: true, clean: true, roots: [], dirs: [{ present: false }] });
	});

	it("H8: an inspection changes nothing in the host directory or a root", async () => {
		const { host, env } = hostDir();
		const dir = freshDir();
		const s = stream("host-h8");
		const writer = new Child(`${front.url}${s.path}`, dir, env);
		await writer.opened();
		await writer.exec("CREATE TABLE t(x); INSERT INTO t VALUES (1)");
		expect((await writer.send({ synced: 20_000 })).synced).toMatchObject({ synced: true });
		await mode("refuse");
		await writer.exec("INSERT INTO t VALUES (2)");
		process.kill(writer.pid, "SIGSTOP");
		try {
			await sleep(200);
			const before = [...tree(host), ...tree(dir)];
			const seen = await inspect(host, [writer.pid]);
			expect(seen, JSON.stringify(seen)).toMatchObject({ complete: true, clean: false });
			expect(onlyRoot(seen)).toMatchObject({ held: true, state: "pending" });
			expect([...tree(host), ...tree(dir)]).toEqual(before);
		} finally {
			process.kill(writer.pid, "SIGCONT");
		}
		await writer.stop();
	});

	it("H13: an orphan's bytes drain with a reopen and a clean close", async () => {
		const { host, env } = hostDir();
		const dir = freshDir();
		const s = stream("host-h13");
		await crashTables(s.url, dir, env);
		await crash(s.url, dir, env, "logged:2");
		const orphan = onlyRoot(await inspect(host, [process.pid]));
		expect(orphan).toMatchObject({ state: "pending", held: false });
		expect(orphan.unacknowledged_bytes).toBeGreaterThan(0);

		const reopened = new Child(s.url, dir, env);
		await reopened.opened();
		expect((await reopened.send({ synced: 20_000 })).synced).toMatchObject({ synced: true });
		await closeAndRetire(reopened);
		const drained = await inspect(host, [process.pid], { untilClean: 5_000 });
		expect(drained, JSON.stringify(drained)).toMatchObject({ complete: true, clean: true });
		expect(onlyRoot(drained)).toMatchObject({ state: "clean", held: false, unacknowledged_bytes: 0 });
	});

	for (const how of ["renamed", "deleted", "root renamed"] as const) {
		it(`H14: a root held under a cache directory ${how === "root renamed" ? "whose root was renamed out of the root-name grammar" : how} meanwhile is an unread lock while its process runs`, async () => {
			const { host, env } = hostDir();
			const parent = realpathSync(freshDir());
			const dir = join(parent, "cache");
			mkdirSync(dir);
			const holder = new Child(stream(`host-h14-${how}`).url, dir, env);
			await holder.opened();
			await holder.exec("CREATE TABLE t(x); INSERT INTO t VALUES (1)");
			const before = await inspect(host, [holder.pid]);
			expect(before, JSON.stringify(before)).toMatchObject({ complete: true });
			const root = onlyRoot(before);
			expect(root.held).toBe(true);

			const moved = how === "root renamed" ? join(dir, "notes-old") : join(parent, "moved");
			if (how === "renamed") renameSync(dir, moved);
			else if (how === "root renamed") renameSync(join(dir, root.id), moved);
			else rmSync(dir, { recursive: true, force: true });
			const seen = await inspect(host, [holder.pid]);
			expect(seen, JSON.stringify(seen)).toMatchObject({ complete: false, clean: false, incomplete: "unread_lock", roots: [] });
			const lock = { renamed: join(moved, root.id, "lock"), "root renamed": join(moved, "lock"), deleted: `${join(dir, root.id, "lock")} (deleted)` }[how];
			expect(seen.unread_locks).toEqual([{ pid: holder.pid, path: lock }]);

			await holder.stop();
			const after = await inspect(host, [process.pid]);
			expect(after, JSON.stringify(after)).toMatchObject({ complete: true, roots: [], unread_locks: [], dirs: [{ present: how === "root renamed" }] });
			// The renamed directory, and whatever it holds, is out of the host's sight.
			expect(existsSync(moved)).toBe(how !== "deleted");
		});
	}

	// The npm copy is 0.11.0: a server or model of a later line refuses it (`protocol_mismatch`),
	// so this runs on the 0.11 line only.
	const line011 = (JSON.parse(readFileSync(join(import.meta.dirname, "../package.json"), "utf8")) as { version: string }).version.startsWith("0.11.");
	describe.skipIf(!line011)("H15: a 0.11.0 extension", () => {
		let older = "";
		beforeAll(async () => {
			const dest = freshDir();
			const platform = `linux-${process.arch === "arm64" ? "arm64" : "x64"}-gnu`;
			await run("npm", ["pack", `@tonbo/sqlite-ursula-${platform}@0.11.0`, "--pack-destination", dest]);
			const tarball = readdirSync(dest).find((n) => n.endsWith(".tgz"));
			expect(tarball).toBeDefined();
			await run("tar", ["-xzf", join(dest, tarball as string), "-C", dest]);
			older = join(dest, "package", "libsqlite_ursula_vfs.so");
			expect(existsSync(older)).toBe(true);
		});

		it("records nothing, and its root is read in a scanned directory and held while it runs", async () => {
			const { host, env } = hostDir();
			const dir = freshDir();
			const s = stream("host-h15");
			const copy = new Child(`${front.url}${s.path}`, dir, env, "core-child.mjs", [], older);
			await copy.opened();
			await copy.exec("CREATE TABLE t(x); INSERT INTO t VALUES (1)");
			expect((await copy.send({ synced: 20_000 })).synced).toMatchObject({ synced: true });
			// Its lazy ACK lands, so the bytes below are the refused commit's alone.
			expect(await inspect(host, [copy.pid], { scans: [dir], untilClean: 5_000 })).toMatchObject({ clean: true });
			await mode("refuse");
			await copy.exec("INSERT INTO t VALUES (2)");
			expect(names(host)).toEqual([]);

			// Not recorded and not scanned: its lock is unread.
			const unscanned = await inspect(host, [copy.pid]);
			expect(unscanned).toMatchObject({ complete: false, incomplete: "unread_lock", roots: [] });

			const running = await inspect(host, [copy.pid], { scans: [dir] });
			expect(running, JSON.stringify(running)).toMatchObject({ complete: true, clean: false, dirs: [{ from: "scan", present: true }] });
			const root = onlyRoot(running);
			expect(root).toMatchObject({ state: "pending", held: true, stream: s.path });
			expect(root.unacknowledged_bytes).toBeGreaterThan(0);

			await copy.stop("SIGKILL");
			const exited = onlyRoot(await inspect(host, [process.pid], { scans: [dir] }));
			expect(exited).toMatchObject({ state: "pending", held: false, unacknowledged_bytes: root.unacknowledged_bytes });
		});
	});
});

// A long outage (design §4.6, §5.7, M1 exit criterion), against the model server (E2E_SERVER=model)
// behind test/gate.mjs: the stream is cut off for OUTAGE_MS (60 s by default, 600 s in the soak
// workflow), as a blackhole (requests read and never answered) and as refused connections. Meanwhile
// commits go on, nothing resets, `synced()` waits, status says `retrying`, and a database with a
// small `maxPendingBytes` refuses writes with `backlog_full` and stays usable. After the heal both
// catch up in full: acknowledged equals logged, a snapshot that came due during the outage is published
// after it (URSULA_VFS_SNAPSHOT_DUE_BYTES), and the stream (snapshot and tail) equals the local
// database.
import { spawn } from "node:child_process";
import { appendFileSync } from "node:fs";
import { createInterface } from "node:readline";
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it } from "vitest";
import { loadUrsulaVfs, open, type UrsulaDatabase, type UrsulaResetError, type UrsulaStatus, UrsulaWriteRefusedError } from "../src/index.ts";
import { ext, fromStream, freshDir, onModel, stream, thrown, ursulaUrl } from "./kit.ts";

const OUTAGE_MS = Number(process.env.OUTAGE_MS ?? 60_000);
const MODES = (process.env.OUTAGE_MODES ?? "blackhole,refuse").split(",") as ("blackhole" | "refuse")[];
/** The small database's bound: a few of its 16 KiB commits of random bytes fill it. */
const SMALL_PENDING_BYTES = 256 * 1024;
/** How long the catch-up after the heal may take: a request held by a blackhole times out after 15 s, and the backoff is at most 5 s. */
const CATCH_UP_MS = 90_000;
/** One commit of the main database every this many ms during the outage. */
const COMMIT_EVERY_MS = 50;

// Read once, when the extension first needs it: before it loads.
process.env.URSULA_VFS_SNAPSHOT_DUE_BYTES ??= "65536";

const sleep = (ms: number): Promise<void> => new Promise((r) => setTimeout(r, ms));

/** test/gate.mjs in front of the server: its proxy and control URLs, and a stop. */
let gate: { url: string; control: string; stop: () => Promise<void> };

beforeAll(async () => {
	if (!onModel()) return;
	const proc = spawn(process.execPath, ["--no-warnings", new URL("./gate.mjs", import.meta.url).pathname, ursulaUrl()], { stdio: ["pipe", "pipe", "inherit"] });
	const lines = createInterface({ input: proc.stdout as NodeJS.ReadableStream })[Symbol.asyncIterator]();
	const { port, control } = JSON.parse((await lines.next()).value as string) as { port: number; control: number };
	const exited = new Promise((r) => proc.once("exit", r));
	gate = {
		url: `http://127.0.0.1:${port}`,
		control: `http://127.0.0.1:${control}`,
		stop: async () => {
			// It exits when its stdin closes.
			proc.stdin?.end();
			await exited;
		},
	};
});

afterAll(async () => {
	await gate?.stop();
});

/** The databases a test opened: closed after it, whatever happened. */
const opened = new Set<UrsulaDatabase>();
beforeEach(async () => {
	if (onModel()) await mode("forward");
});
afterEach(() => {
	for (const d of opened) if (d.isOpen) d.close();
	opened.clear();
});

async function mode(m: "forward" | "blackhole" | "refuse"): Promise<void> {
	const r = await fetch(`${gate.control}/mode`, { method: "PUT", body: JSON.stringify({ mode: m }) });
	expect(r.status, await r.text()).toBe(204);
}

/** Whether `promise` settled. */
function watch(promise: Promise<unknown>): () => boolean {
	let settled = false;
	promise.then(
		() => {
			settled = true;
		},
		() => {
			settled = true;
		},
	);
	return () => settled;
}

/** The gate's counters. */
async function gateStats(): Promise<{ held: number }> {
	return (await (await fetch(`${gate.control}/stats`)).json()) as { held: number };
}

/** `ursula_status` with the snapshot part the root entry's type leaves out. */
type Status = UrsulaStatus & { readonly snapshot: { readonly offset: number | null; readonly log_bytes: number; readonly error: string | null } };

/** What `f` returns once it returns something, polled for at most 30 s. */
async function until<T>(f: () => T | undefined): Promise<T> {
	const deadline = Date.now() + 30_000;
	for (;;) {
		const got = f();
		if (got !== undefined) return got;
		if (Date.now() > deadline) throw new Error("it never happened within 30 s");
		await sleep(50);
	}
}

/** Reports a line to the console and, in CI, to the job summary. */
function report(line: string): void {
	process.stderr.write(`${line}\n`);
	const summary = process.env.GITHUB_STEP_SUMMARY;
	if (summary !== undefined && summary.length > 0) appendFileSync(summary, `${line}\n`);
}

describe.skipIf(!onModel())(`an outage of ${OUTAGE_MS / 1000} s`, () => {
	for (const m of MODES) {
		it(
			`${m}: commits go on, nothing resets, backlog_full past a small bound, then a full catch-up`,
			async () => {
				loadUrsulaVfs(ext());
				const main = stream(`outage-${m}`);
				const small = stream(`outage-${m}-small`);
				const db = await open(`${gate.url}${main.path}`, { dir: freshDir() });
				opened.add(db);
				const tiny = await open(`${gate.url}${small.path}`, { dir: freshDir(), maxPendingBytes: SMALL_PENDING_BYTES });
				opened.add(tiny);
				const resets: UrsulaResetError[] = [];
				for (const d of [db, tiny]) d.on("reset", (e) => resets.push(e));
				db.exec("CREATE TABLE t(i INTEGER PRIMARY KEY, body TEXT)");
				tiny.exec("CREATE TABLE t(i INTEGER PRIMARY KEY, pad BLOB)");
				const insert = db.prepare("INSERT INTO t VALUES (?, ?)");
				const insertSmall = tiny.prepare("INSERT INTO t VALUES (?, randomblob(16384))");
				let i = 0;
				let j = 0;
				const commit = (): void => {
					i += 1;
					insert.run(i, `row ${i} `.repeat(8));
				};
				commit();
				insertSmall.run(++j);
				await Promise.all([db.synced(), tiny.synced()]);

				const before = await gateStats();
				await mode(m);
				const started = Date.now();
				commit();
				const gated = db.synced();
				const gatedSettled = watch(gated);
				let retryingAt: number | undefined;
				let backlogFullAt: number | undefined;
				let peakPending = 0;
				while (Date.now() - started < OUTAGE_MS) {
					commit();
					if (backlogFullAt === undefined) {
						try {
							insertSmall.run(j + 1);
							j += 1;
						} catch (error) {
							expect(error).toBeInstanceOf(UrsulaWriteRefusedError);
							expect(error).toMatchObject({ kind: "backlog_full", action: "retry" });
							backlogFullAt = Date.now() - started;
						}
					}
					const st = db.status();
					peakPending = Math.max(peakPending, st.commits.pending_bytes);
					expect(st.reset).toBeNull();
					if (st.replication.condition === "retrying") retryingAt ??= Date.now() - started;
					await sleep(COMMIT_EVERY_MS);
				}
				const outage = Date.now() - started;
				// During the outage: nothing reset, the gate never opened, status said retrying, and
				// the small database refused writes with nothing committed and stayed usable.
				expect(resets).toEqual([]);
				expect(gatedSettled()).toBe(false);
				expect(retryingAt, "status never said retrying").toBeDefined();
				expect(db.status().replication.condition).toBe("retrying");
				expect(db.status().commits.pending).toBeGreaterThan(0);
				expect(backlogFullAt, "the small database never refused a write").toBeDefined();
				const refusedAgain = thrown(() => insertSmall.run(j + 1));
				expect(refusedAgain).toMatchObject({ kind: "backlog_full" });
				expect(tiny.prepare("SELECT count(*) AS n FROM t").get()).toEqual({ n: j });
				expect(tiny.status().reset).toBeNull();
				const held = (await gateStats()).held - before.held;

				await mode("forward");
				const healed = Date.now();
				const signal = AbortSignal.timeout(CATCH_UP_MS);
				await Promise.all([gated, db.synced({ signal }), tiny.synced({ signal })]);
				const catchUp = Date.now() - healed;
				for (const d of [db, tiny]) {
					const { commits, replication, reset } = d.status();
					expect(commits.acknowledged).toBe(commits.logged);
					expect(commits).toMatchObject({ pending: 0, pending_bytes: 0 });
					expect(replication.condition).toBeNull();
					expect(reset).toBeNull();
				}
				// The small database takes writes again.
				insertSmall.run(++j);
				commit();
				await Promise.all([db.synced({ signal }), tiny.synced({ signal })]);
				expect(resets).toEqual([]);
				// The snapshot that came due during the outage is published.
				const snapshotted = await until(() => {
					const { snapshot } = db.status() as Status;
					return snapshot.offset !== null && snapshot.log_bytes < 65536 ? snapshot : undefined;
				});
				expect(snapshotted.error).toBeNull();

				const summary = "SELECT count(*) AS n, max(i) AS max, sum(length(body)) AS bytes, group_concat(i || ':' || body, ',') AS rows FROM (SELECT * FROM t ORDER BY i)";
				const local = db.prepare(summary).all();
				const smallLocal = tiny.prepare("SELECT count(*) AS n, group_concat(hex(substr(pad, 1, 16)), ',') AS heads FROM (SELECT * FROM t ORDER BY i)").all();
				expect(local).toMatchObject([{ n: i, max: i }]);
				db.close();
				tiny.close();
				expect(await fromStream(`${ursulaUrl()}${main.path}`, summary)).toEqual(local);
				expect(await fromStream(`${ursulaUrl()}${small.path}`, "SELECT count(*) AS n, group_concat(hex(substr(pad, 1, 16)), ',') AS heads FROM (SELECT * FROM t ORDER BY i)")).toEqual(smallLocal);
				report(
					`outage ${m}: ${(outage / 1000).toFixed(1)} s, ${i} commits (peak pending ${peakPending} bytes), retrying after ${retryingAt} ms, ` +
						`backlog_full after ${backlogFullAt} ms (${j} small commits), ${held} requests held by the gate, caught up ${catchUp} ms after the heal`,
				);
			},
			OUTAGE_MS + 4 * CATCH_UP_MS,
		);
	}
});

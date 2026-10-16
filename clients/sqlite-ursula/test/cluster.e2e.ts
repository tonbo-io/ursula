// Another deployment (design: Identity), against two model servers of their own (`--cluster-id
// cluster-a` and `cluster-b`, with distinct seeds, so their tokens differ) behind test/gate.mjs,
// which misroutes on `PUT /target`. A bound owner whose requests reach the other deployment blocks
// with `condition: wrong_cluster`, never resets, and goes on once the route is fixed. An open whose
// stream answers as another deployment than the one its cache was built on fails `wrong_cluster`
// (fix), sends nothing and moves no file.
import { createHash } from "node:crypto";
import { readdirSync, readFileSync, realpathSync, statSync } from "node:fs";
import { dirname, join } from "node:path";
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it } from "vitest";
import { loadUrsulaVfs, open, type UrsulaDatabase, type UrsulaResetError, UrsulaSetupError } from "../src/index.ts";
import { ext, freshDir, gate, modelServer, onModel, rejection } from "./kit.ts";

loadUrsulaVfs(ext());

const sleep = (ms: number): Promise<void> => new Promise((r) => setTimeout(r, ms));

let a: Awaited<ReturnType<typeof modelServer>>;
let b: Awaited<ReturnType<typeof modelServer>>;
let front: Awaited<ReturnType<typeof gate>>;

beforeAll(async () => {
	if (!onModel()) return;
	a = await modelServer("cluster-a", 11);
	b = await modelServer("cluster-b", 22);
	front = await gate(a.url);
});

afterAll(async () => {
	await front?.stop();
	await a?.stop();
	await b?.stop();
});

/** The databases a test opened: closed after it, whatever happened. */
const opened = new Set<UrsulaDatabase>();
beforeEach(async () => {
	if (onModel()) await route(a.url);
});
afterEach(() => {
	for (const d of opened) if (d.isOpen) d.close();
	opened.clear();
});

/** Sends the gate's requests to `url`. */
async function route(url: string): Promise<void> {
	const r = await fetch(`${front.control}/target`, { method: "PUT", body: JSON.stringify({ url }) });
	expect(r.status, await r.text()).toBe(204);
}

let n = 0;
function path(label: string): string {
	n += 1;
	return `/e2e/${label}-${process.pid}-${Date.now().toString(36)}-${n}`;
}

/** Every byte the server at `url` holds for `streamPath`, or `null` when it has no such stream. */
async function held(url: string, streamPath: string): Promise<number | null> {
	const r = await fetch(`${url}/__model/frames?path=${streamPath}`);
	// The model answers a control route about a stream it does not hold with a 400.
	if (r.status === 400) return null;
	expect(r.ok).toBe(true);
	return (await r.arrayBuffer()).byteLength;
}

/** Each file under `dir` with its digest. */
function hashes(dir: string): Record<string, string> {
	const out: Record<string, string> = {};
	const walk = (d: string): void => {
		for (const name of readdirSync(d)) {
			const p = join(d, name);
			if (statSync(p).isDirectory()) walk(p);
			else out[p] = createHash("sha256").update(readFileSync(p)).digest("hex");
		}
	};
	walk(dir);
	return out;
}

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

describe.skipIf(!onModel())("another deployment", () => {
	it("blocks a bound owner `wrong_cluster` while misrouted, with no reset, then goes on", { timeout: 120_000 }, async () => {
		const p = path("misroute-bound");
		const db = await open(`${front.url}${p}`, { dir: freshDir() });
		opened.add(db);
		const resets: UrsulaResetError[] = [];
		db.on("reset", (e) => resets.push(e));
		db.exec("CREATE TABLE t(x); INSERT INTO t VALUES (1)");
		await db.synced();

		await route(b.url);
		db.exec("INSERT INTO t VALUES (2)");
		let synced = false;
		const waiting = db.synced().then(() => {
			synced = true;
		});
		await until(() => (db.status().replication.condition === "wrong_cluster" ? true : undefined));
		expect(synced).toBe(false);
		expect(resets).toEqual([]);
		expect(db.status().reset).toBeNull();
		// Nothing landed on the other deployment.
		expect(await held(b.url, p)).toBeNull();
		// Reads and commits go on locally.
		db.exec("INSERT INTO t VALUES (3)");
		expect(db.prepare("SELECT count(*) AS n FROM t").get()).toEqual({ n: 3 });

		await route(a.url);
		// A blocked request is retried every 30 s.
		await waiting;
		await db.synced();
		expect(resets).toEqual([]);
		expect(db.status().replication.condition).toBeNull();
		expect(db.status().commits.pending).toBe(0);
		expect(await held(b.url, p)).toBeNull();
	});

	it("refuses an open whose stream answers as another deployment, `wrong_cluster` to fix, moving no file", async () => {
		const p = path("misroute-open");
		// The real path, as the extension names the files.
		const dir = realpathSync(freshDir());
		const first = await open(`${front.url}${p}`, { dir });
		first.exec("CREATE TABLE t(x); INSERT INTO t VALUES (1)");
		await first.synced();
		const generation = dirname(first.status().path);
		first.close();
		const directories = (): string[] => [...new Set(Object.keys(hashes(dir)).map((f) => dirname(f)))].sort();
		const before = directories();
		expect(before).toContain(generation);

		await route(b.url);
		const error = await rejection(open(`${front.url}${p}`, { dir }));
		expect(error).toBeInstanceOf(UrsulaSetupError);
		expect(error).toMatchObject({ kind: "wrong_cluster", action: "fix" });
		// No directory moved and nothing sent. The open materializes the generation before its HEAD
		// (design: Open), which folds the WAL into the db, so a second refusal finds every byte as the
		// first left it.
		expect(directories()).toEqual(before);
		const refused = hashes(dir);
		expect(await rejection(open(`${front.url}${p}`, { dir }))).toMatchObject({ kind: "wrong_cluster" });
		expect(hashes(dir)).toEqual(refused);
		expect(await held(b.url, p)).toBeNull();

		// Routed back, the cache opens as it was.
		await route(a.url);
		const back = await open(`${front.url}${p}`, { dir });
		opened.add(back);
		expect(back.status().attach.local).toBe("resumed");
		expect(dirname(back.status().path)).toBe(generation);
		expect(back.prepare("SELECT x FROM t").all()).toEqual([{ x: 1 }]);
	});
});

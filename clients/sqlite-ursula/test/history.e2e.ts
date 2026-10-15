// History events (design §6.2, W5) against the model server (E2E_SERVER=model): a history change
// of the stream that keeps every byte this owner acknowledged keeps its generation. The owner
// re-keys to the stream's new token and claims the next epoch under it, with no reset, while bound
// and at the next open. A restore that lost an acknowledged commit resets the owner `rewound`.
//
// The model's history events and restores apply to every stream it holds, so this file runs its
// tests one after another on the shared model, as every file does.
import { afterAll, afterEach, beforeAll, describe, expect, it } from "vitest";
import { loadUrsulaVfs, open, type UrsulaDatabase, type UrsulaResetError } from "../src/index.ts";
import { directUrl, ext, freshDir, fromStream, gate, onModel, rejection, stream } from "./kit.ts";

loadUrsulaVfs(ext());

const sleep = (ms: number): Promise<void> => new Promise((r) => setTimeout(r, ms));

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

/** The databases a test opened: closed after it, whatever happened. */
const opened = new Set<UrsulaDatabase>();
afterEach(() => {
	for (const d of opened) if (d.isOpen) d.close();
	opened.clear();
});

async function owner(url: string, dir: string): Promise<{ db: UrsulaDatabase; resets: UrsulaResetError[] }> {
	const db = await open(url, { dir });
	opened.add(db);
	const resets: UrsulaResetError[] = [];
	db.on("reset", (e) => resets.push(e));
	return { db, resets };
}

describe.skipIf(!onModel())("history events", () => {
	let front: Awaited<ReturnType<typeof gate>>;
	beforeAll(async () => {
		if (onModel()) front = await gate(directUrl());
	});
	afterAll(async () => {
		await front?.stop();
	});

	it("re-keys a bound owner across a history that keeps everything: no reset, a new token, the next epoch", async () => {
		const s = stream("history-bound");
		const { db, resets } = await owner(s.url, freshDir());
		db.exec("CREATE TABLE t(x); INSERT INTO t VALUES (1)");
		await db.synced();
		const before = db.status();
		expect(before.epoch).not.toBeNull();

		await history();
		db.exec("INSERT INTO t VALUES (2)");
		await db.synced();
		const after = db.status();
		expect(resets).toEqual([]);
		expect(after.reset).toBeNull();
		expect(after.incarnation).not.toBe(before.incarnation);
		expect(after.epoch).toBe((before.epoch as number) + 1);
		expect(after.lineage).toBe(before.lineage);
		expect(after.path).toBe(before.path);
		expect(await fromStream(s.url, "SELECT x FROM t ORDER BY x")).toEqual([{ x: 1 }, { x: 2 }]);
	});

	it("re-keys after a restore of a checkpoint taken after the last acknowledgement, and resends what it held", async () => {
		const s = stream("history-restore");
		const { db, resets } = await owner(`${front.url}${s.path}`, freshDir());
		db.exec("CREATE TABLE t(x); INSERT INTO t VALUES (1)");
		await db.synced();
		const before = db.status();
		const saved = await checkpoint();
		// The stream is out of reach: the next commit stays pending, then the restore happens.
		await mode("refuse");
		db.exec("INSERT INTO t VALUES (2)");
		await sleep(200);
		expect(db.status().commits.pending).toBeGreaterThan(0);
		await history(saved);
		await mode("forward");
		await db.synced();
		expect(resets).toEqual([]);
		const after = db.status();
		expect(after.incarnation).not.toBe(before.incarnation);
		expect(after.epoch).toBe((before.epoch as number) + 1);
		expect(after.commits.pending).toBe(0);
		expect(await fromStream(s.url, "SELECT x FROM t ORDER BY x")).toEqual([{ x: 1 }, { x: 2 }]);
	});

	it("reopens a closed owner's cache across a history that keeps everything, as `rekeyed`", async () => {
		const s = stream("history-reopen");
		const dir = freshDir();
		const first = await open(s.url, { dir });
		first.exec("CREATE TABLE t(x); INSERT INTO t VALUES (1)");
		await first.synced();
		const before = first.status();
		first.close();

		await history();
		const { db, resets } = await owner(s.url, dir);
		const after = db.status();
		expect(after.attach).toEqual({ local: "rekeyed", snapshot_offset: null });
		expect(after.path).toBe(before.path);
		expect(after.lineage).toBe(before.lineage);
		expect(after.incarnation).not.toBe(before.incarnation);
		expect(db.prepare("SELECT x FROM t").all()).toEqual([{ x: 1 }]);
		db.exec("INSERT INTO t VALUES (2)");
		await db.synced();
		expect(resets).toEqual([]);
		expect(await fromStream(s.url, "SELECT x FROM t ORDER BY x")).toEqual([{ x: 1 }, { x: 2 }]);
	});

	it("resets `rewound` when a restore lost an acknowledged commit", async () => {
		const s = stream("history-lossy");
		const { db, resets } = await owner(s.url, freshDir());
		db.exec("CREATE TABLE t(x)");
		await db.synced();
		const saved = await checkpoint();
		db.exec("INSERT INTO t VALUES (1)");
		await db.synced();

		await history(saved);
		const heard = new Promise<UrsulaResetError>((resolve) => db.on("reset", resolve));
		db.exec("INSERT INTO t VALUES (2)");
		expect(await heard).toMatchObject({ kind: "rewound" });
		expect(await rejection(db.synced())).toMatchObject({ kind: "rewound" });
		expect(resets).toHaveLength(1);
		expect(db.status().reset).toMatchObject({ kind: "rewound" });
	});

	async function mode(m: "forward" | "refuse"): Promise<void> {
		const r = await fetch(`${front.control}/mode`, { method: "PUT", body: JSON.stringify({ mode: m }) });
		expect(r.status, await r.text()).toBe(204);
	}
});

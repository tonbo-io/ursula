// Salvage (design §6.2, D3): a local cache the open cannot keep is moved whole into the cache
// root's `salvage/`, a plain SQLite database with its log and a manifest.json, and the open builds
// a new cache from the stream. A history change that may have lost what the cache holds salvages
// it (`history_changed`), a db file that is not the cache's own is salvaged as found (`replaced`),
// and with `recreate` a missing stream is created again over the cache (`recreated`).
//
// The history cases need the model's history events (E2E_SERVER=model). The model's history
// events apply to every stream it holds, so this file runs its tests one after another.
import { spawnSync } from "node:child_process";
import { copyFileSync, existsSync, readdirSync, readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { loadUrsulaVfs, open, type UrsulaDatabase, type UrsulaResetError, type UrsulaSalvage } from "../src/index.ts";
import { Child, directUrl, ext, freshDir, fromStream, killChildren, onModel, rejection, stream } from "./kit.ts";

loadUrsulaVfs(ext());

/** The databases a test opened: closed after it, whatever happened. */
const opened = new Set<UrsulaDatabase>();
afterEach(async () => {
	for (const d of opened) if (d.isOpen) d.close();
	opened.clear();
	await killChildren();
});

async function owner(url: string, dir: string, recreate?: boolean): Promise<UrsulaDatabase> {
	const db = await open(url, recreate === undefined ? { dir } : { dir, recreate });
	opened.add(db);
	return db;
}

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

/** Deletes the stream at `path`: the model's control route, or a DELETE at the server. */
async function deleteStream(path: string): Promise<void> {
	const r = onModel() ? await fetch(`${directUrl()}/__model/delete?path=${path}`, { method: "POST" }) : await fetch(`${directUrl()}${path}`, { method: "DELETE" });
	expect(r.ok, `${r.status} ${await r.text()}`).toBe(true);
}

/** The rows `sql` returns from the SQLite file at `path`, read by node:sqlite without the extension in a process of its own. */
function plainRows(path: string, sql: string): unknown[] {
	const script = `
		import { DatabaseSync } from "node:sqlite";
		const db = new DatabaseSync(process.argv[1], { readOnly: true });
		console.log(JSON.stringify(db.prepare(process.argv[2]).all()));
		db.close();`;
	const r = spawnSync(process.execPath, ["--no-warnings", "--input-type=module", "-e", script, path, sql], { encoding: "utf8" });
	expect(r.status, r.stderr).toBe(0);
	return JSON.parse(r.stdout.trim()) as unknown[];
}

/** The salvage `status().salvage` lists, the only one, with its manifest. */
function onlySalvage(db: UrsulaDatabase, reason: UrsulaSalvage["reason"]): { salvage: UrsulaSalvage; manifest: Record<string, unknown> } {
	const list = db.status().salvage;
	expect(list, JSON.stringify(list)).toHaveLength(1);
	const salvage = list[0] as UrsulaSalvage;
	expect(salvage.reason).toBe(reason);
	const manifest = JSON.parse(readFileSync(join(salvage.dir, "manifest.json"), "utf8")) as Record<string, unknown>;
	expect(manifest).toMatchObject({ format: 1, state: "complete", reason });
	expect(existsSync(join(salvage.dir, "log"))).toBe(true);
	return { salvage, manifest };
}

describe("salvage", () => {
	it.skipIf(!onModel())("salvages a cache whose history lost commits, as a plain SQLite database holding them, and builds anew", async () => {
		const s = stream("salvage-history");
		const dir = freshDir();
		const db = await owner(s.url, dir);
		db.exec("CREATE TABLE t(x)");
		await db.synced();
		const saved = await checkpoint();
		db.exec("INSERT INTO t VALUES (1)");
		await db.synced();
		const before = db.status();
		await history(saved);
		const heard = new Promise<UrsulaResetError>((resolve) => db.on("reset", resolve));
		db.exec("INSERT INTO t VALUES (2)");
		expect(await heard).toMatchObject({ kind: "rewound", action: "reopen" });
		expect(await rejection(db.synced())).toMatchObject({ kind: "rewound" });
		db.close();

		const again = await owner(s.url, dir);
		const status = again.status();
		expect(status.attach).toEqual({ local: "salvaged", snapshot_offset: null });
		expect(status.lineage).not.toBe(before.lineage);
		expect(status.incarnation).not.toBe(before.incarnation);
		expect(again.prepare("SELECT x FROM t").all()).toEqual([]);
		const { salvage, manifest } = onlySalvage(again, "history_changed");
		expect(manifest).toMatchObject({
			path: s.path,
			cluster_id: before.cluster_id,
			lineage: before.lineage,
			incarnation: before.incarnation,
			stream_incarnation: status.incarnation,
			plain: true,
		});
		expect(manifest.acknowledged_position).toBeGreaterThan(0);
		expect(typeof manifest.acknowledged_offset).toBe("number");
		expect(manifest.unacknowledged).toBe(1);
		expect(manifest.time).toMatch(/^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\dZ$/);
		expect(salvage).toMatchObject({ incarnation: before.incarnation, unacknowledged: 1, plain: true });
		// The rows the history lost are in the salvage, which SQLite opens without the extension.
		const db0 = join(salvage.dir, "db");
		expect(readFileSync(db0).subarray(0, 16).toString("latin1")).toBe("SQLite format 3\0");
		expect(plainRows(db0, "SELECT x FROM t ORDER BY x")).toEqual([{ x: 1 }, { x: 2 }]);
		expect(plainRows(db0, "PRAGMA integrity_check")).toEqual([{ integrity_check: "ok" }]);
		again.exec("INSERT INTO t VALUES (3)");
		await again.synced();
		expect(await fromStream(s.url, "SELECT x FROM t ORDER BY x")).toEqual([{ x: 3 }]);
	});

	it("refuses a missing stream over a cache holding data with `not_found`, and with `recreate` salvages it and creates the stream", async () => {
		const s = stream("salvage-recreate");
		const dir = freshDir();
		const db = await owner(s.url, dir);
		db.exec("CREATE TABLE t(x); INSERT INTO t VALUES (1)");
		await db.synced();
		const before = db.status();
		db.close();
		await deleteStream(s.path);

		expect(await rejection(owner(s.url, dir))).toMatchObject({ name: "UrsulaSetupError", kind: "not_found", action: "fix" });
		expect(existsSync(dirname(before.path))).toBe(true);

		const again = await owner(s.url, dir, true);
		const status = again.status();
		expect(status.attach).toEqual({ local: "salvaged", snapshot_offset: null });
		expect(status.epoch).toBe(1);
		expect(again.prepare("SELECT name FROM sqlite_schema").all()).toEqual([]);
		const { salvage } = onlySalvage(again, "recreated");
		expect(plainRows(join(salvage.dir, "db"), "SELECT x FROM t")).toEqual([{ x: 1 }]);
		again.exec("CREATE TABLE u(y); INSERT INTO u VALUES (7)");
		await again.synced();
		expect(await fromStream(s.url, "SELECT y FROM u")).toEqual([{ y: 7 }]);
	});

	it("salvages a plain SQLite file copied over the cache's db as `replaced`, file and log as found", async () => {
		const s = stream("salvage-replaced");
		const dir = freshDir();
		const db = await owner(s.url, dir);
		db.exec("CREATE TABLE t(x); INSERT INTO t VALUES (1)");
		await db.synced();
		const plain = join(freshDir(), "plain.db");
		db.exec(`VACUUM INTO '${plain}'`);
		const gen = dirname(db.status().path);
		const segments = readdirSync(join(gen, "log")).sort();
		db.close();
		copyFileSync(plain, join(gen, "db"));

		const again = await owner(s.url, dir);
		expect(again.status().attach).toEqual({ local: "salvaged", snapshot_offset: null });
		expect(again.prepare("SELECT x FROM t").all()).toEqual([{ x: 1 }]);
		const { salvage, manifest } = onlySalvage(again, "replaced");
		expect(manifest).toMatchObject({ plain: true, acknowledged_position: null, incarnation: null });
		expect(readFileSync(join(salvage.dir, "db"))).toEqual(readFileSync(plain));
		expect(readdirSync(join(salvage.dir, "log")).sort()).toEqual(segments);
	});

	it.skipIf(!onModel())("salvages a commit acknowledged behind a lagging ACK that a restore lost, with its row", async () => {
		const s = stream("salvage-lagging-ack");
		const dir = freshDir();
		const child = new Child(s.url, dir);
		await child.opened();
		await child.exec("CREATE TABLE t(x)");
		expect(await child.send({ synced: 10_000 })).toMatchObject({ ok: true, synced: { synced: true } });
		const saved = await checkpoint();
		// Killed right after the acknowledgement: an ACK record names it only when the ACK interval
		// since the last one had passed. Without one (#611 B2) the log holds an acknowledged commit
		// after its latest ACK, and the restore inherits the log by that ACK. Either way the reopen
		// never re-keys: it salvages the commit.
		await child.exec("INSERT INTO t VALUES (1)");
		expect(await child.send({ synced: 10_000 })).toMatchObject({ ok: true, synced: { synced: true } });
		await child.stop("SIGKILL");
		expect(await fromStream(s.url, "SELECT x FROM t")).toEqual([{ x: 1 }]);
		await history(saved);

		const again = await owner(s.url, dir);
		expect(again.status().attach).toEqual({ local: "salvaged", snapshot_offset: null });
		expect(again.prepare("SELECT x FROM t").all()).toEqual([]);
		const { salvage } = onlySalvage(again, "history_changed");
		expect(plainRows(join(salvage.dir, "db"), "SELECT x FROM t")).toEqual([{ x: 1 }]);
	});
});

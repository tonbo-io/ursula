// History events against a real Ursula (E2E_SERVER=ursula, one node): the restore flow an operator
// runs (docs: Backup and disaster recovery). A restore exports the stream's Raft group, stops the
// node, wipes its Raft WAL, starts it again on the same ports and imports the export with its
// BLAKE3 digest (`x-ursula-backup-blake3`), which starts a new history of every stream in the
// group. The node is one of this file's own (test/stack.ts's single-node config, `server.cluster_id`
// set, one group), so its restores touch no other suite's streams.
//
// - A restore that keeps everything the owner acknowledged re-keys a bound owner, with no reset.
// - A restore that lost an acknowledged commit resets the owner `rewound`, and the reopen salvages.
// - Two restores with no write between, then a reopen of a closed owner: `rekeyed`.
import { spawnSync } from "node:child_process";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { blake3 } from "@noble/hashes/blake3.js";
import { afterAll, afterEach, beforeAll, describe, expect, it } from "vitest";
import { loadUrsulaVfs, open, type UrsulaDatabase, type UrsulaResetError } from "../src/index.ts";
import { ext, freshDir, fromStream, onModel, rejection } from "./kit.ts";
import { freePort, Proc, singleNodeToml, ursulaBin, waitReady, waitWritable } from "./stack.ts";

const skip = onModel() || process.env.E2E_NODES === "3";
if (!skip) loadUrsulaVfs(ext());

const BUCKET = "sqlite-e2e";

/** A one-node, one-group Ursula this file restores. */
class Node {
	private proc?: Proc;
	private dir = "";
	private port = 0;
	private adminPort = 0;

	get url(): string {
		return `http://127.0.0.1:${this.port}`;
	}

	private get admin(): string {
		return `http://127.0.0.1:${this.adminPort}`;
	}

	async init(): Promise<void> {
		this.dir = mkdtempSync(join(tmpdir(), "sqlite-ursula-history-"));
		this.port = await freePort();
		this.adminPort = await freePort();
		const toml = singleNodeToml({ port: this.port, adminPort: this.adminPort, dir: this.dir, cores: 1, groups: 1, cold: "memory" });
		writeFileSync(join(this.dir, "ursula.toml"), toml);
		await this.start();
		const proc = this.proc as Proc;
		await waitWritable(this.url, () => proc.logs());
	}

	private async start(): Promise<void> {
		this.proc = new Proc(ursulaBin(), ["server", "--config", join(this.dir, "ursula.toml")], this.dir, {}, "history-node");
		await waitReady(`${this.admin}/__ursula/backup/info`, this.proc);
	}

	async stop(): Promise<void> {
		await this.proc?.stop();
	}

	async remove(): Promise<void> {
		await this.stop();
		if (this.dir !== "") rmSync(this.dir, { recursive: true, force: true });
	}

	private async exportGroup(): Promise<Buffer> {
		const r = await fetch(`${this.admin}/__ursula/backup/group/0`);
		expect(r.status, "export").toBe(200);
		return Buffer.from(await r.arrayBuffer());
	}

	/** The import of `backup`, retried while the node answers 503 (no leader yet). */
	private async importGroup(backup: Buffer): Promise<{ status: number; body: string }> {
		const digest = Buffer.from(blake3(backup)).toString("hex");
		const deadline = Date.now() + 60_000;
		for (;;) {
			const metrics = (await (await fetch(`${this.admin}/__ursula/metrics`)).json()) as { process_incarnation: string };
			const r = await fetch(`${this.admin}/__ursula/backup/group/0/import`, {
				method: "POST",
				headers: { "content-type": "application/x-msgpack", "x-ursula-backup-blake3": digest, "x-ursula-process-incarnation": metrics.process_incarnation },
				body: backup,
			});
			const answer = { status: r.status, body: await r.text() };
			if (answer.status !== 503 || Date.now() > deadline) return answer;
			await new Promise((resolve) => setTimeout(resolve, 200));
		}
	}

	/** Restores the group as it is now (`backup` omitted) or as `backup` holds it: stop, wipe the Raft WAL, start, import. */
	async restore(backup?: Buffer): Promise<void> {
		const from = backup ?? (await this.exportGroup());
		await this.stop();
		rmSync(join(this.dir, "wal"), { recursive: true, force: true });
		await this.start();
		const r = await this.importGroup(from);
		expect(r.status, r.body).toBe(200);
	}

	/** A backup of the group now, for a later {@link restore}. */
	backup(): Promise<Buffer> {
		return this.exportGroup();
	}

	async head(path: string): Promise<{ incarnation: string; previous: string | null; inherited: string | null; next: string }> {
		const r = await fetch(`${this.url}${path}`, { method: "HEAD" });
		expect(r.status, `HEAD ${path}`).toBe(200);
		return {
			incarnation: r.headers.get("stream-incarnation") as string,
			previous: r.headers.get("stream-previous-incarnation"),
			inherited: r.headers.get("stream-inherited-offset"),
			next: r.headers.get("stream-next-offset") as string,
		};
	}
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

describe.skipIf(skip)("history events on a real Ursula (backup restore)", () => {
	const node = new Node();
	const opened = new Set<UrsulaDatabase>();
	let n = 0;
	const streamPath = (label: string): string => `/${BUCKET}/${label}-${process.pid}-${Date.now().toString(36)}-${++n}`;

	async function owner(path: string, dir: string): Promise<{ db: UrsulaDatabase; resets: UrsulaResetError[] }> {
		const db = await open(`${node.url}${path}`, { dir });
		opened.add(db);
		const resets: UrsulaResetError[] = [];
		db.on("reset", (e) => resets.push(e));
		return { db, resets };
	}

	beforeAll(() => node.init());
	afterEach(() => {
		for (const d of opened) if (d.isOpen) d.close();
		opened.clear();
	});
	afterAll(() => node.remove());

	it("re-keys a bound owner across a restore that keeps everything: no reset, the new token, the next epoch", async () => {
		const path = streamPath("bound");
		const { db, resets } = await owner(path, freshDir());
		db.exec("CREATE TABLE t(x); INSERT INTO t VALUES (1)");
		await db.synced();
		const before = db.status();
		const h0 = await node.head(path);
		expect(before.incarnation).toBe(h0.incarnation);
		await node.restore();
		const h1 = await node.head(path);
		expect(h1.previous).toBe(h0.incarnation);
		expect(h1.inherited).toBe(h0.next);
		db.exec("INSERT INTO t VALUES (2)");
		await db.synced();
		const after = db.status();
		expect(resets).toEqual([]);
		expect(after.reset).toBeNull();
		expect(after.incarnation).toBe(h1.incarnation);
		expect(after.epoch).toBe((before.epoch as number) + 1);
		expect(after.lineage).toBe(before.lineage);
		expect(await fromStream(`${node.url}${path}`, "SELECT x FROM t ORDER BY x")).toEqual([{ x: 1 }, { x: 2 }]);
	});

	it("resets `rewound` when a restore lost an acknowledged commit, and the reopen salvages it", async () => {
		const path = streamPath("lossy");
		const dir = freshDir();
		const { db } = await owner(path, dir);
		db.exec("CREATE TABLE t(x)");
		await db.synced();
		const backup = await node.backup();
		db.exec("INSERT INTO t VALUES (1)");
		await db.synced();
		const before = db.status();
		await node.restore(backup);
		const heard = new Promise<UrsulaResetError>((resolve) => db.on("reset", resolve));
		db.exec("INSERT INTO t VALUES (2)");
		expect(await heard).toMatchObject({ kind: "rewound", action: "reopen" });
		expect(await rejection(db.synced())).toMatchObject({ kind: "rewound" });
		db.close();

		const { db: again, resets } = await owner(path, dir);
		const status = again.status();
		expect(status.attach).toEqual({ local: "salvaged", snapshot_offset: null });
		expect(status.lineage).not.toBe(before.lineage);
		expect(again.prepare("SELECT x FROM t").all()).toEqual([]);
		const [salvage, ...others] = status.salvage;
		expect(others).toEqual([]);
		expect(salvage).toMatchObject({ reason: "history_changed", incarnation: before.incarnation, plain: true });
		expect(plainRows(join(salvage?.dir as string, "db"), "SELECT x FROM t ORDER BY x")).toEqual([{ x: 1 }, { x: 2 }]);
		again.exec("INSERT INTO t VALUES (3)");
		await again.synced();
		expect(resets).toEqual([]);
		expect(await fromStream(`${node.url}${path}`, "SELECT x FROM t ORDER BY x")).toEqual([{ x: 3 }]);
	});

	it("reopens a closed owner's cache as `rekeyed` after two restores with no write between", async () => {
		const path = streamPath("double");
		const dir = freshDir();
		const first = await open(`${node.url}${path}`, { dir });
		first.exec("CREATE TABLE t(x); INSERT INTO t VALUES (1)");
		await first.synced();
		const before = first.status();
		first.close();
		const hA = await node.head(path);
		expect(hA.incarnation).toBe(before.incarnation);
		await node.restore();
		await node.restore();
		const hC = await node.head(path);
		// The token from before both: no write landed between them.
		expect(hC.previous).toBe(hA.incarnation);
		expect(hC.inherited).toBe(hA.next);
		const { db, resets } = await owner(path, dir);
		const after = db.status();
		expect(after.attach).toEqual({ local: "rekeyed", snapshot_offset: null });
		expect(after.lineage).toBe(before.lineage);
		expect(after.incarnation).toBe(hC.incarnation);
		expect(db.prepare("SELECT x FROM t").all()).toEqual([{ x: 1 }]);
		db.exec("INSERT INTO t VALUES (2)");
		await db.synced();
		expect(resets).toEqual([]);
		expect(db.status().epoch).toBe((before.epoch as number) + 1);
		expect(await fromStream(`${node.url}${path}`, "SELECT x FROM t ORDER BY x")).toEqual([{ x: 1 }, { x: 2 }]);
	});
});

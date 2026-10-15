// Pi Durable on the replicated database (openUrsulaPiStorage), against the model server
// (E2E_SERVER=model): Pi's storage conformance on its official node driver in three modes (direct,
// close and reopen in the same cache after every commit, reopen in a fresh cache after every commit,
// which rebuilds from the stream), then the adapter's mapping: `backlog_full` is Pi's
// StorageRejected, a reset the UrsulaResetError itself, and `close` gives up waiting at
// `closeTimeoutMs`.
import { spawn } from "node:child_process";
import { join } from "node:path";
import { createInterface } from "node:readline";
import type { Context } from "@earendil-works/chord";
import { BACKGROUND_CONTEXT } from "@earendil-works/chord/context";
import { type ConversationId, type EntryId, type Id, type Seq, type Storage, StorageRejected, type StorageWrite } from "@earendil-works/pi-durable";
import type { SqliteDatabase } from "@earendil-works/pi-durable/storage/sqlite";
import { registerStorageConformance } from "@earendil-works/pi-durable/testing";
import { describe, expect, it } from "vitest";
import { loadUrsulaVfs, UrsulaResetError, UrsulaWriteRefusedError } from "../src/index.ts";
import { openUrsulaPiStorage } from "../src/pi.ts";
import { ext, fault, freshDir, onModel, rejection, stream } from "./kit.ts";

loadUrsulaVfs(ext());
const ctx: Context = BACKGROUND_CONTEXT;

/** Closes and reopens the underlying storage after every commit, failed commits included. */
class ReopeningStorage implements Storage {
	private current: Storage;
	private closed = false;
	private readonly reopen: () => Promise<Storage>;
	private constructor(current: Storage, reopen: () => Promise<Storage>) {
		this.current = current;
		this.reopen = reopen;
	}
	static async open(reopen: () => Promise<Storage>): Promise<ReopeningStorage> {
		return new ReopeningStorage(await reopen(), reopen);
	}
	async commit(writes: readonly StorageWrite[], c: Context): Promise<Seq> {
		if (this.closed) return this.current.commit(writes, c);
		try {
			return await this.current.commit(writes, c);
		} finally {
			await this.current.close(ctx);
			this.current = await this.reopen();
		}
	}
	mintId<I extends Id<string>>(): Promise<I> {
		return this.current.mintId<I>();
	}
	conversation: Storage["conversation"] = (id, c) => this.current.conversation(id, c);
	scanConversations: Storage["scanConversations"] = (q, l, cu, c) => this.current.scanConversations(q, l, cu, c);
	entry(id: EntryId, c: Context): ReturnType<Storage["entry"]>;
	entry(conversationId: ConversationId, id: EntryId, c: Context): ReturnType<Storage["entry"]>;
	entry(a: number, b: number | Context, c?: Context): ReturnType<Storage["entry"]> {
		if (c === undefined) return this.current.entry(a as EntryId, b as Context);
		return this.current.entry(a as ConversationId, b as EntryId, c);
	}
	findLatestHeadMarker: Storage["findLatestHeadMarker"] = (a, b, c) => this.current.findLatestHeadMarker(a, b, c);
	scanEntries: Storage["scanEntries"] = (q, l, cu, c) => this.current.scanEntries(q, l, cu, c);
	task: Storage["task"] = (id, c) => this.current.task(id, c);
	scanTasks: Storage["scanTasks"] = (q, l, cu, c) => this.current.scanTasks(q, l, cu, c);
	submission: Storage["submission"] = (id, c) => this.current.submission(id, c);
	scanSubmissions: Storage["scanSubmissions"] = (q, l, cu, c) => this.current.scanSubmissions(q, l, cu, c);
	submissionByRequest: Storage["submissionByRequest"] = (a, b, c) => this.current.submissionByRequest(a, b, c);
	findDocument: Storage["findDocument"] = (a, b, c) => this.current.findDocument(a, b, c);
	document: Storage["document"] = (a, b, c) => this.current.document(a, b, c);
	scanDocuments: Storage["scanDocuments"] = (q, l, cu, c) => this.current.scanDocuments(q, l, cu, c);
	async close(c: Context): Promise<void> {
		if (this.closed) return;
		this.closed = true;
		await this.current.close(c);
	}
}

/** The SQLite facade under a storage openUrsulaPiStorage made: the wrapped node driver. */
const facade = (storage: Storage): SqliteDatabase => (storage as unknown as { db: SqliteDatabase }).db;

const conformance = (name: string, make: () => Promise<Storage>) =>
	registerStorageConformance({ describe, expect, it }, `pi on sqlite-ursula: ${name}`, async (use) => {
		const storage = await make();
		try {
			await use(storage);
		} finally {
			await storage.close(ctx);
		}
	});

if (onModel()) {
	conformance("direct", () => openUrsulaPiStorage(stream("pi").url, { dir: freshDir() }));
	conformance("close and reopen in the same cache after every commit", () => {
		const { url } = stream("pi-reopen");
		const dir = freshDir();
		return ReopeningStorage.open(() => openUrsulaPiStorage(url, { dir }));
	});
	conformance("reopen in a fresh cache after every commit", () => {
		const { url } = stream("pi-fresh");
		return ReopeningStorage.open(() => openUrsulaPiStorage(url, { dir: freshDir() }));
	});
}

describe.skipIf(!onModel())("openUrsulaPiStorage", () => {
	it("throws backlog_full as Pi's StorageRejected, nothing committed, and the storage goes on", async () => {
		const storage = await openUrsulaPiStorage(stream("pi-backlog").url, { dir: freshDir(), maxPendingBytes: 1 });
		const db = facade(storage);
		await db.exec("CREATE TABLE t(x)");
		await fault("delay 1500", 1, "POST");
		// The first commit runs at once and waits for the stream, which holds the backlog full.
		const first = db.run("INSERT INTO t VALUES (1)");
		const refused = await rejection(db.run("INSERT INTO t VALUES (2)"));
		expect(refused).toBeInstanceOf(StorageRejected);
		expect((refused as StorageRejected).cause).toBeInstanceOf(UrsulaWriteRefusedError);
		expect((refused as StorageRejected).cause).toMatchObject({ kind: "backlog_full", action: "retry" });
		const inTransaction = await rejection(db.transaction((tx) => tx.run("INSERT INTO t VALUES (3)")));
		expect(inTransaction).toBeInstanceOf(StorageRejected);
		await first;
		await db.run("INSERT INTO t VALUES (4)");
		expect(await db.all("SELECT x FROM t ORDER BY x")).toEqual([{ x: 1 }, { x: 4 }]);
		await storage.close(ctx);
	});

	it("throws a reset as the UrsulaResetError itself, and closes", async () => {
		const s = stream("pi-reset");
		const storage = await openUrsulaPiStorage(s.url, { dir: freshDir() });
		const taker = spawn(process.execPath, ["--no-warnings", join(import.meta.dirname, "core-child.mjs"), ext(), s.url, freshDir()], { stdio: ["pipe", "pipe", "inherit"] });
		const lines = createInterface({ input: taker.stdout as NodeJS.ReadableStream })[Symbol.asyncIterator]();
		expect(JSON.parse((await lines.next()).value as string)).toMatchObject({ ok: true });
		const read = await rejection(storage.conversation(await storage.mintId<ConversationId>(), ctx));
		expect(read).toBeInstanceOf(UrsulaResetError);
		expect(read).toMatchObject({ kind: "fenced", action: "stop" });
		const write = await rejection(facade(storage).exec("CREATE TABLE u(y)"));
		expect(write).toBeInstanceOf(UrsulaResetError);
		await storage.close(ctx);
		const exited = new Promise((r) => taker.once("exit", r));
		taker.stdin?.write(`${JSON.stringify({ exit: true })}\n`);
		await exited;
	});

	it("waits for synced() on close, at most closeTimeoutMs", async () => {
		const closeAfter = async (closeTimeoutMs: number | undefined): Promise<number> => {
			const storage = await openUrsulaPiStorage(stream("pi-close").url, { dir: freshDir(), ...(closeTimeoutMs === undefined ? {} : { closeTimeoutMs }) });
			await facade(storage).exec("CREATE TABLE t(x)");
			// The round trip of close's synced() comes back after a second.
			await fault("delay 1000", 1);
			const started = Date.now();
			await storage.close(ctx);
			return Date.now() - started;
		};
		expect(await closeAfter(undefined)).toBeGreaterThanOrEqual(900);
		expect(await closeAfter(200)).toBeLessThan(800);
	});
});

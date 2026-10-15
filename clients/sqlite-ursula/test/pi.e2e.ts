// Pi Durable on the replicated database (openUrsulaPiStorage), against the model server
// (E2E_SERVER=model): the adapter's mapping, which Pi's conformance (test/conformance.e2e.ts) never
// triggers: `backlog_full` is Pi's StorageRejected, a reset the UrsulaResetError itself, and `close`
// gives up waiting at `closeTimeoutMs`.
import type { Context } from "@earendil-works/chord";
import { BACKGROUND_CONTEXT } from "@earendil-works/chord/context";
import { type ConversationId, type Storage, StorageRejected } from "@earendil-works/pi-durable";
import type { SqliteDatabase } from "@earendil-works/pi-durable/storage/sqlite";
import { afterEach, describe, expect, it } from "vitest";
import { loadUrsulaVfs, UrsulaResetError, UrsulaWriteRefusedError } from "../src/index.ts";
import { openUrsulaPiStorage } from "../src/pi.ts";
import { Child, ext, fault, freshDir, killChildren, onModel, rejection, stream } from "./kit.ts";

loadUrsulaVfs(ext());
const ctx: Context = BACKGROUND_CONTEXT;
afterEach(killChildren);

/** The SQLite facade under a storage openUrsulaPiStorage made: the wrapped node driver. */
const facade = (storage: Storage): SqliteDatabase => (storage as unknown as { db: SqliteDatabase }).db;

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
		const taker = new Child(s.url, freshDir());
		await taker.opened();
		const read = await rejection(storage.conversation(await storage.mintId<ConversationId>(), ctx));
		expect(read).toBeInstanceOf(UrsulaResetError);
		expect(read).toMatchObject({ kind: "fenced", action: "stop" });
		const write = await rejection(facade(storage).exec("CREATE TABLE u(y)"));
		expect(write).toBeInstanceOf(UrsulaResetError);
		await storage.close(ctx);
		await taker.stop();
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

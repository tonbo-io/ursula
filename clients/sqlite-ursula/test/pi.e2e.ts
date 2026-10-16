// Pi Durable on the replicated database (openUrsulaPiStorage), against the e2e server
// (test/stack.ts): the adapter's mapping, which Pi's conformance (test/conformance.e2e.ts) never
// triggers: `backlog_full` runs again after `synced()` from Pi 1.2 and is Pi's StorageRejected up
// to 1.1, a reset is the UrsulaResetError itself, and `close` gives up waiting at `closeTimeoutMs`.
import type { Context } from "@earendil-works/chord";
import { BACKGROUND_CONTEXT } from "@earendil-works/chord/context";
import * as durable from "@earendil-works/pi-durable";
import type { ConversationId, Storage } from "@earendil-works/pi-durable";
import type { SqliteDatabase } from "@earendil-works/pi-durable/storage/sqlite";
import { afterEach, describe, expect, it } from "vitest";
import { loadUrsulaVfs, UrsulaResetError, type UrsulaStatus, UrsulaWriteRefusedError } from "../src/index.ts";
import { openUrsulaPiStorage } from "../src/pi.ts";
import { Child, ext, fault, freshDir, killChildren, rejection, stream } from "./kit.ts";

const control = loadUrsulaVfs(ext());
/** Pi's StorageRejected, which Pi 1.2 removed. */
const StorageRejected = (durable as Record<string, unknown>).StorageRejected as (new (message: string) => Error) | undefined;
const ctx: Context = BACKGROUND_CONTEXT;
afterEach(killChildren);

/** The SQLite facade under a storage openUrsulaPiStorage made: the wrapped node driver. */
const facade = (storage: Storage): SqliteDatabase => (storage as unknown as { db: SqliteDatabase }).db;
/** `ursula_status` of the database under a storage openUrsulaPiStorage made. */
const status = (storage: Storage): UrsulaStatus => {
	const path = (storage as unknown as { db: { database: { location(): string } } }).db.database.location();
	return JSON.parse(Object.values(control.prepare("SELECT ursula_status(?)").get(path) as Record<string, string>)[0] as string) as UrsulaStatus;
};

describe("openUrsulaPiStorage", () => {
	it.runIf(StorageRejected === undefined)("runs a commit refused with backlog_full again once synced() drained the backlog (Pi 1.2 on)", async () => {
		const storage = await openUrsulaPiStorage(stream("pi-backlog").url, { dir: freshDir(), maxPendingBytes: 1 });
		const db = facade(storage);
		await db.exec("CREATE TABLE t(x)");
		await fault("delay 1500", 1, "POST");
		const order: number[] = [];
		const done = (n: number) => () => order.push(n);
		// The first commit runs at once and waits for the stream, which holds the backlog full.
		const first = db.run("INSERT INTO t VALUES (1)").then(done(1));
		const second = db.run("INSERT INTO t VALUES (2)").then(done(2));
		const third = db.transaction((tx) => tx.run("INSERT INTO t VALUES (3)")).then(done(3));
		// The stream holds the first commit for 1.5 s: the others were refused and wait meanwhile.
		await new Promise((r) => setTimeout(r, 300));
		expect(status(storage).refused).toMatchObject({ kind: "backlog_full", action: "retry" });
		expect(order).toEqual([]);
		await Promise.all([first, second, third]);
		expect(order[0]).toBe(1);
		await db.run("INSERT INTO t VALUES (4)");
		expect(await db.all("SELECT x FROM t ORDER BY x")).toEqual([{ x: 1 }, { x: 2 }, { x: 3 }, { x: 4 }]);
		const { commits } = status(storage);
		expect(commits.acknowledged).toBe(commits.logged);
		await storage.close(ctx);
	});

	it.runIf(StorageRejected !== undefined)("throws backlog_full as Pi's StorageRejected, nothing committed, and the storage goes on (Pi up to 1.1)", async () => {
		const Rejected = StorageRejected as new (message: string) => Error;
		const storage = await openUrsulaPiStorage(stream("pi-backlog").url, { dir: freshDir(), maxPendingBytes: 1 });
		const db = facade(storage);
		await db.exec("CREATE TABLE t(x)");
		await fault("delay 1500", 1, "POST");
		// The first commit runs at once and waits for the stream, which holds the backlog full.
		const first = db.run("INSERT INTO t VALUES (1)");
		const refused = await rejection(db.run("INSERT INTO t VALUES (2)"));
		expect(refused).toBeInstanceOf(Rejected);
		expect((refused as Error).cause).toBeInstanceOf(UrsulaWriteRefusedError);
		expect((refused as Error).cause).toMatchObject({ kind: "backlog_full", action: "retry" });
		const inTransaction = await rejection(db.transaction((tx) => tx.run("INSERT INTO t VALUES (3)")));
		expect(inTransaction).toBeInstanceOf(Rejected);
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

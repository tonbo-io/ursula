// Pi Durable's storage conformance on openUrsulaPiStorage (M1 exit criterion), against the e2e
// server (test/stack.ts), in three modes: direct, close and reopen in the same cache after every
// commit, and reopen in a fresh cache after every commit, which rebuilds from the stream.
//
// On synced(): every Pi commit resolves only once `synced()` did, and each storage checks that. When a
// commit resolves with no other commit of the storage in flight, `ursula_status` must show every
// commit the binding logged acknowledged by the stream, nothing pending.
//
// Snapshots are due after 4 KiB of commits (URSULA_VFS_SNAPSHOT_DUE_BYTES), so the fresh caches of
// the third mode build from a snapshot and the tail after it whenever one was published, which
// status reports in `attach.snapshot_offset`. A last case makes sure of it: it waits until HEAD
// names a snapshot, then opens a fresh cache and checks that it was built from that snapshot.
import type { Context } from "@earendil-works/chord";
import { BACKGROUND_CONTEXT } from "@earendil-works/chord/context";
import type { ConversationId, EntryId, Id, Seq, Storage, StorageWrite } from "@earendil-works/pi-durable";
import type { SqliteDatabase } from "@earendil-works/pi-durable/storage/sqlite";
import { registerStorageConformance } from "@earendil-works/pi-durable/testing";
import { afterAll, describe, expect, it } from "vitest";
import { loadUrsulaVfs, type UrsulaStatus } from "../src/index.ts";
import { openUrsulaPiStorage } from "../src/pi.ts";
import { directUrl, ext, freshDir, stream } from "./kit.ts";

// Read once, when the extension first needs it: before it loads.
process.env.URSULA_VFS_SNAPSHOT_DUE_BYTES ??= "4096";
const control = loadUrsulaVfs(ext());
const ctx: Context = BACKGROUND_CONTEXT;

/** Commits resolved with none other in flight, each checked against `ursula_status`, by mode. */
const checked = new Map<string, number>();
/** Opens, by mode and by where the open got the local database (`attach`). */
const opens = new Map<string, Record<string, number>>();

/** The SQLite facade under a storage openUrsulaPiStorage made: the wrapped node driver. */
const facade = (storage: Storage): SqliteDatabase => (storage as unknown as { db: SqliteDatabase }).db;

/** The local file a storage openUrsulaPiStorage made runs Pi's node driver on. */
const location = (storage: Storage): string => (storage as unknown as { db: { database: { location(): string } } }).db.database.location();

const status = (path: string): UrsulaStatus => JSON.parse(Object.values(control.prepare("SELECT ursula_status(?)").get(path) as Record<string, string>)[0] as string) as UrsulaStatus;

/**
 * `storage`, whose commits are checked on resolving: with no other commit in flight, every commit
 * the binding logged is acknowledged by the stream, nothing pending.
 */
function onSynced(storage: Storage, mode: string): Storage {
	const path = location(storage);
	const { attach } = status(path);
	const from = attach.local === "resumed" ? "resumed" : attach.snapshot_offset === null ? "built from the start" : "built from a snapshot";
	const counts = opens.get(mode) ?? {};
	counts[from] = (counts[from] ?? 0) + 1;
	opens.set(mode, counts);
	let inflight = 0;
	const commit = storage.commit.bind(storage);
	storage.commit = async (writes: readonly StorageWrite[], c: Context): Promise<Seq> => {
		inflight += 1;
		try {
			const seq = await commit(writes, c);
			if (inflight === 1) {
				const { commits } = status(path);
				expect(commits.acknowledged, "a Pi commit resolved before the stream acknowledged it").toBe(commits.logged);
				expect(commits).toMatchObject({ pending: 0, pending_bytes: 0 });
				checked.set(mode, (checked.get(mode) ?? 0) + 1);
			}
			return seq;
		} finally {
			inflight -= 1;
		}
	};
	return storage;
}

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

const conformance = (mode: string, make: () => Promise<Storage>) =>
	registerStorageConformance({ describe, expect, it }, `pi on sqlite-ursula: ${mode}`, async (use) => {
		const storage = await make();
		try {
			await use(storage);
		} finally {
			await storage.close(ctx);
		}
	});

const MODES = {
	direct: "direct",
	reopen: "close and reopen in the same cache after every commit",
	fresh: "reopen in a fresh cache after every commit",
} as const;

conformance(MODES.direct, async () => onSynced(await openUrsulaPiStorage(stream("pi").url, { dir: freshDir() }), MODES.direct));
conformance(MODES.reopen, () => {
	const { url } = stream("pi-reopen");
	const dir = freshDir();
	return ReopeningStorage.open(async () => onSynced(await openUrsulaPiStorage(url, { dir }), MODES.reopen));
});
conformance(MODES.fresh, () => {
	const { url } = stream("pi-fresh");
	return ReopeningStorage.open(async () => onSynced(await openUrsulaPiStorage(url, { dir: freshDir() }), MODES.fresh));
});
afterAll(() => {
	process.stderr.write(`conformance on synced(): commits checked ${JSON.stringify(Object.fromEntries(checked))}, opens ${JSON.stringify(Object.fromEntries(opens))}\n`);
	// Every mode checked commits against the stream.
	for (const mode of Object.values(MODES)) expect(checked.get(mode) ?? 0, mode).toBeGreaterThan(10);
});

describe("a fresh cache", () => {
	it("builds from the snapshot HEAD names, and the tail after it", async () => {
		const { url, path: name } = stream("pi-snapshot");
		const owner = await openUrsulaPiStorage(url, { dir: freshDir() });
		const db = facade(owner);
		await db.exec("CREATE TABLE pad(i INTEGER PRIMARY KEY, b BLOB)");
		for (let i = 1; i <= 8; i++) await db.run("INSERT INTO pad VALUES (?, randomblob(3000))", i);
		// With snapshots due every 4 KiB, more keep coming while the owner commits: wait until the
		// owner's latest published snapshot is the one HEAD names and no other is due. Not
		// `log_bytes === 0`: the bytes after the last snapshot stay below the threshold, so no
		// snapshot is due for them and they never drop to 0.
		const head = async (): Promise<{ snapshot: number | null; next: number }> => {
			const r = await fetch(`${directUrl()}${name}`, { method: "HEAD" });
			const snapshot = r.headers.get("stream-snapshot-offset");
			return { snapshot: snapshot === null ? null : Number(snapshot), next: Number(r.headers.get("stream-next-offset")) };
		};
		const snapshots = (): UrsulaStatus["snapshot"] => status(location(owner)).snapshot;
		let settled = false;
		for (const deadline = Date.now() + 20_000; !settled && Date.now() < deadline; ) {
			const s = snapshots();
			settled = s.offset !== null && s.offset === (await head()).snapshot && s.log_bytes <= s.due_bytes;
			if (!settled) await new Promise((r) => setTimeout(r, 50));
		}
		expect(settled, `no snapshot settled within 20 s: ${JSON.stringify(snapshots())}`).toBe(true);
		// A commit after it: the tail the build reads.
		await db.run("INSERT INTO pad VALUES (9, x'00')");
		const rows = await db.all("SELECT i, hex(b) AS b FROM pad ORDER BY i");
		await owner.close(ctx);
		// The snapshot the build reads is the one HEAD names now, with a tail after it.
		const { snapshot, next } = await head();
		expect(snapshot).not.toBeNull();
		expect(next).toBeGreaterThan(snapshot as number);
		const fresh = onSynced(await openUrsulaPiStorage(url, { dir: freshDir() }), "snapshot");
		try {
			expect(status(location(fresh)).attach).toEqual({ local: "built", snapshot_offset: snapshot });
			expect(await facade(fresh).all("SELECT i, hex(b) AS b FROM pad ORDER BY i")).toEqual(rows);
			expect(await facade(fresh).all("PRAGMA integrity_check")).toEqual([{ integrity_check: "ok" }]);
		} finally {
			await fresh.close(ctx);
		}
	});
});

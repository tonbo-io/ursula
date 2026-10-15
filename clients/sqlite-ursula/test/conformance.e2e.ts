// Pi Durable's storage conformance on openUrsulaPiStorage (M1 exit criterion), against the model
// server (E2E_SERVER=model), in three modes: direct, close and reopen in the same cache after every
// commit, and reopen in a fresh cache after every commit, which rebuilds from the stream.
//
// On synced(): every Pi commit resolves only once `synced()` did, and each storage checks that. When a
// commit resolves with no other commit of the storage in flight, `ursula_status` must show every
// commit the binding logged acknowledged by the stream, nothing pending.
//
// Snapshots are due after 4 KiB of commits (URSULA_VFS_SNAPSHOT_DUE_BYTES), so the fresh caches of
// the third mode build from a snapshot and the tail after it.
import type { Context } from "@earendil-works/chord";
import { BACKGROUND_CONTEXT } from "@earendil-works/chord/context";
import type { ConversationId, EntryId, Id, Seq, Storage, StorageWrite } from "@earendil-works/pi-durable";
import { registerStorageConformance } from "@earendil-works/pi-durable/testing";
import { afterAll, describe, expect, it } from "vitest";
import { loadUrsulaVfs, type UrsulaStatus } from "../src/index.ts";
import { openUrsulaPiStorage } from "../src/pi.ts";
import { ext, freshDir, onModel, stream } from "./kit.ts";

// Read once, when the extension first needs it: before it loads.
process.env.URSULA_VFS_SNAPSHOT_DUE_BYTES ??= "4096";
const control = loadUrsulaVfs(ext());
const ctx: Context = BACKGROUND_CONTEXT;

/** Commits resolved with none other in flight, each checked against `ursula_status`, by mode. */
const checked = new Map<string, number>();
/** Of those, the ones whose binding had a published snapshot, by mode. */
const snapshotted = new Map<string, number>();

/** `ursula_status` with the snapshot part the root entry's type leaves out. */
type Status = UrsulaStatus & { readonly snapshot: { readonly offset: number | null } };

/** The local file a storage openUrsulaPiStorage made runs Pi's node driver on. */
const location = (storage: Storage): string => (storage as unknown as { db: { database: { location(): string } } }).db.database.location();

const status = (path: string): Status => JSON.parse(Object.values(control.prepare("SELECT ursula_status(?)").get(path) as Record<string, string>)[0] as string) as Status;

/**
 * `storage`, whose commits are checked on resolving: with no other commit in flight, every commit
 * the binding logged is acknowledged by the stream, nothing pending.
 */
function onSynced(storage: Storage, mode: string): Storage {
	const path = location(storage);
	let inflight = 0;
	const commit = storage.commit.bind(storage);
	storage.commit = async (writes: readonly StorageWrite[], c: Context): Promise<Seq> => {
		inflight += 1;
		try {
			const seq = await commit(writes, c);
			if (inflight === 1) {
				const { commits, snapshot } = status(path);
				expect(commits.acknowledged, "a Pi commit resolved before the stream acknowledged it").toBe(commits.logged);
				expect(commits).toMatchObject({ pending: 0, pending_bytes: 0 });
				checked.set(mode, (checked.get(mode) ?? 0) + 1);
				if (snapshot.offset !== null) snapshotted.set(mode, (snapshotted.get(mode) ?? 0) + 1);
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

if (onModel()) {
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
		process.stderr.write(`conformance on synced(): commits checked ${JSON.stringify(Object.fromEntries(checked))}, with a snapshot ${JSON.stringify(Object.fromEntries(snapshotted))}\n`);
		// Every mode checked commits against the stream.
		for (const mode of Object.values(MODES)) expect(checked.get(mode) ?? 0, mode).toBeGreaterThan(10);
		// And snapshots were published and built from.
		expect(snapshotted.get(MODES.fresh) ?? 0).toBeGreaterThan(0);
	});
}

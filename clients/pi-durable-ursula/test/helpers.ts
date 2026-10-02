import type { Context } from "@earendil-works/chord";
import { BACKGROUND_CONTEXT } from "@earendil-works/chord/context";
import type { ConversationId, EntryId, Id, Seq, Storage, StorageWrite } from "@earendil-works/pi-durable";
import { FakeUrsula } from "../src/fake/index.ts";
import { type Clock, type Timing, UrsulaStorage, type UrsulaStorageOptions } from "../src/storage.ts";

export const ctx: Context = BACKGROUND_CONTEXT;

/** Fast timing for tests; deadlines stay ordered like the defaults. */
export const FAST: Partial<Timing> = {
	activityWindowMs: 60,
	claimDeadlineMs: 1500,
	commitDeadlineMs: 3000,
	openDeadlineMs: 3000,
	closeMarkerDeadlineMs: 300,
	backoffBaseMs: 1,
	backoffMaxMs: 20,
};

/**
 * A virtual clock: `sleep(ms)` advances `now()` by `ms` and resolves on the next macrotask, so
 * backoff and deadlines keep their arithmetic while tests never wait on the wall clock. Time moves
 * only when someone sleeps.
 */
export function virtualClock(start = 1_000_000): Clock & { elapsed(): number } {
	let t = start;
	return {
		now: () => t,
		sleep: (ms) => {
			t += Math.max(0, ms);
			return new Promise<void>((resolve) => setImmediate(resolve));
		},
		elapsed: () => t - start,
	};
}

let counter = 0;
export const freshPath = (): string => `/b/harness-${process.pid}-${counter++}`;

export function openOn(fake: FakeUrsula, path: string, options: Partial<UrsulaStorageOptions> = {}): Promise<UrsulaStorage> {
	return UrsulaStorage.open({
		log: fake.logTransport(path),
		keyedState: fake.keyedStateTransport(path),
		timing: { ...FAST, ...options.timing },
		host: "test",
		pid: 1,
		...options,
	});
}

/** Closes and reopens the underlying UrsulaStorage after every commit, failed commits included. */
export class ReopeningStorage implements Storage {
	private current: UrsulaStorage;
	private closed = false;
	private readonly reopen: () => Promise<UrsulaStorage>;
	private constructor(current: UrsulaStorage, reopen: () => Promise<UrsulaStorage>) {
		this.current = current;
		this.reopen = reopen;
	}
	static async open(reopen: () => Promise<UrsulaStorage>): Promise<ReopeningStorage> {
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

export { FakeUrsula };

import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { Context } from "@earendil-works/chord";
import { BACKGROUND_CONTEXT } from "@earendil-works/chord/context";
import type { ConversationId, EntryId, Id, Seq, Storage, StorageWrite } from "@earendil-works/pi-durable";

export const ctx: Context = BACKGROUND_CONTEXT;

const dir = mkdtempSync(join(tmpdir(), "sqlite-ursula-"));
let counter = 0;
/** A fresh, nonexistent SQLite file path. */
export const freshFile = (): string => join(dir, `db-${counter++}.sqlite`);

/** Closes and reopens the underlying storage after every commit, failed commits included. */
export class ReopeningStorage implements Storage {
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

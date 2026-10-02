// Read plans of Pi's Storage methods (design §4.5) over an abstract ordered state view.
//
// Every function here is a pure synchronous pass over a `StateView` (see state-store.ts): it may be
// aborted and re-run by a partial-state store. Values are stored as JSON text and parsed fresh on
// every read, so returned objects are always detached (I22).
import type { JsonValue } from "@earendil-works/chord";
import { applyImmutableBatches, type Op } from "@earendil-works/chord/delta";
import type {
	ConversationId,
	ConversationQuery,
	ConversationRecord,
	Cursor,
	DocumentAddress,
	DocumentId,
	DocumentPoint,
	DocumentQuery,
	DocumentRecord,
	EntryId,
	EntryQuery,
	EntryRecord,
	JsonObject,
	Page,
	Seq,
	StoredDocument,
	SubmissionId,
	SubmissionQuery,
	SubmissionRecord,
	TaskId,
	TaskQuery,
	TaskRecord,
} from "@earendil-works/pi-durable";
import {
	type DocumentAddressLike,
	K,
	SUBMISSION_STATUS_CODE,
	TABLE_OF_TAG,
	TASK_STATUS_CODE,
	type TableName,
	type TableTag,
} from "./families.ts";
import type { StateView } from "./state-store.ts";
import { keySuccessor, readU64, readU64Desc, strinc, u64, u64desc } from "./tuple.ts";

export type StoredTask = TaskRecord<JsonValue, JsonValue, JsonValue>;
/** Value of the `x/{id}` row. */
export interface RegistryRow {
	readonly t: TableTag;
	readonly c?: number;
}
/** Value of the `d/{id}` row. */
export interface DocumentRow {
	readonly record: DocumentRecord;
	readonly version: number;
}
/** Value of `e/` and `e.h/` rows: commitSeq is an explicit field (§4.3 design notes). */
export interface EntryRow {
	readonly seq: number;
	readonly entry: EntryRecord;
}
/** Value of `s.r/` rows; `requestId` is present when the key is digested. */
export interface RequestRow {
	readonly id: number;
	readonly requestId?: string;
}

const parse = <T>(text: string): T => JSON.parse(text) as T;
const brand = <T>(n: number): T => n as T;

export function value<T>(view: StateView, key: string): T | undefined {
	const text = view.get(key);
	return text === undefined ? undefined : parse<T>(text);
}

/** Rows of `[prefix, strinc(prefix))`, optionally starting at `from` (when `from > prefix`). */
export function* prefixScan(view: StateView, prefix: string, from?: string): Generator<readonly [string, string]> {
	yield* view.scan(from !== undefined && from > prefix ? from : prefix, strinc(prefix));
}

export function tableOf(view: StateView, id: number): TableName | undefined {
	const x = value<RegistryRow>(view, K.x(id));
	return x === undefined ? undefined : TABLE_OF_TAG[x.t];
}

export const documentRow = (view: StateView, id: number): DocumentRow | undefined =>
	value<DocumentRow>(view, K.d(id));

export const isCurrentOnly = (r: { scope: DocumentRecord["scope"]; history?: string }): boolean =>
	r.scope.kind !== "conversation" || r.history === "latest";

export const isAliveAt = (record: DocumentRecord, at: DocumentPoint): boolean => {
	if (at === "current") return record.retiredAt === undefined;
	return record.createdAt <= at && (record.retiredAt === undefined || at < record.retiredAt);
};

function cursorId(cursor: Cursor | undefined): number | undefined {
	const after = cursor?.after;
	if (after === undefined) return undefined;
	if (typeof after !== "number" || !Number.isSafeInteger(after)) throw new TypeError("Invalid storage cursor");
	return after;
}

function page<T extends { readonly id: number }>(values: T[], limit: number): Page<T, Cursor> {
	const items = values.slice(0, limit);
	if (values.length <= limit) return { items };
	return { items, next: { after: (items.at(-1) as T).id } };
}

/** Start key for "after id" inside a `prefix ‖ u64(id)` family. */
const afterKey = (prefix: string, after: number | undefined): string | undefined =>
	after === undefined ? undefined : keySuccessor(prefix + u64(after));

const idSuffix = (key: string): number => Number(readU64(key, key.length - 8));

function mustConversation(view: StateView, id: number): ConversationRecord {
	const conversation = value<ConversationRecord>(view, K.c(id));
	if (conversation === undefined) throw new Error(`Unknown conversation: ${id}`);
	return conversation;
}

// ---------------------------------------------------------------- documents

/** §4.5 findDocument plan, also the planner's address-occupancy check. */
export function findDocumentAt(view: StateView, address: DocumentAddressLike, at: DocumentPoint): DocumentRecord | undefined {
	const prefix = K.daAddress(address);
	const from = at === "current" ? undefined : prefix + u64desc(at);
	for (const [, text] of prefixScan(view, prefix, from)) {
		const r = parse<DocumentRecord>(text);
		// Full kind/key comparison: digest forms and NUL-extensions share the prefix (§4.2).
		if (r.kind !== address.kind || r.key !== address.key) continue;
		if (r.retiredAt !== undefined && r.retiredAt === r.createdAt) continue; // empty lifetime
		if (isAliveAt(r, at)) return r;
		return undefined; // first non-empty dead incarnation: stop
	}
	return undefined;
}

/** §4.5 document plan. */
export function materializeDocument(view: StateView, id: number, at: DocumentPoint): StoredDocument | undefined {
	const row = documentRow(view, id);
	if (row === undefined) return undefined;
	if (at !== "current" && isCurrentOnly(row.record)) {
		throw new Error(`Document ${id} does not retain historical content`);
	}
	if (!isAliveAt(row.record, at)) return undefined;
	const basePrefix = K.db(id);
	let base: { seq: number; version: number; value: JsonObject } | undefined;
	for (const [k, text] of prefixScan(view, basePrefix, at === "current" ? undefined : basePrefix + u64desc(at))) {
		const b = parse<{ version: number; value: JsonObject }>(text);
		base = { seq: Number(readU64Desc(k, basePrefix.length)), version: b.version, value: b.value };
		break;
	}
	if (base === undefined) throw new Error(`Document ${id} is missing a required base`);
	const deltas: (readonly Op[])[] = [];
	const end = at === "current" ? strinc(K.dr(id)) : keySuccessor(K.dr(id, at));
	for (const [, text] of view.scan(K.dr(id, base.seq + 1), end)) {
		const d = parse<{ version: number; ops: Op[] }>(text);
		if (d.version !== base.version) throw new Error(`Document ${id} crosses a stored version boundary without a base`);
		deltas.push(d.ops);
	}
	const materialized = applyImmutableBatches(base.value, deltas) as JsonObject;
	return { record: row.record, version: base.version, value: materialized, deltasSinceBase: deltas.length };
}

export const findDocument = (view: StateView, address: DocumentAddress, at: DocumentPoint): DocumentRecord | undefined =>
	findDocumentAt(view, address, at);

export const document = (view: StateView, id: DocumentId, at: DocumentPoint): StoredDocument | undefined =>
	materializeDocument(view, id, at);

export function scanDocuments(view: StateView, query: DocumentQuery, limit: number, cursor: Cursor | undefined): Page<DocumentRecord, Cursor> {
	const prefix = K.ds(query.scope);
	const values: DocumentRecord[] = [];
	for (const [, text] of prefixScan(view, prefix, afterKey(prefix, cursorId(cursor)))) {
		const r = parse<DocumentRecord>(text);
		if (query.kind !== undefined && r.kind !== query.kind) continue;
		if (!isAliveAt(r, query.at)) continue;
		values.push(r);
		if (values.length > limit) break;
	}
	return page(values, limit);
}

// ---------------------------------------------------------------- conversations and entries

export const conversation = (view: StateView, id: ConversationId): ConversationRecord | undefined =>
	value<ConversationRecord>(view, K.c(id));

export function scanConversations(view: StateView, query: ConversationQuery, limit: number, cursor: Cursor | undefined): Page<ConversationRecord, Cursor> {
	const prefix =
		query.ownerTaskId !== undefined
			? K.cot(query.ownerTaskId)
			: query.ownerConversationId !== undefined
				? K.coc(query.ownerConversationId)
				: K.c();
	const values: ConversationRecord[] = [];
	for (const [, text] of prefixScan(view, prefix, afterKey(prefix, cursorId(cursor)))) {
		const r = parse<ConversationRecord>(text);
		if (query.ownerConversationId !== undefined && r.owner?.conversationId !== query.ownerConversationId) continue;
		if (query.ownerTaskId !== undefined && r.owner?.taskId !== query.ownerTaskId) continue;
		values.push(r);
		if (values.length > limit) break;
	}
	return page(values, limit);
}

export type EntryResult = { readonly entry: EntryRecord; readonly commitSeq: Seq } | undefined;

function entryAt(view: StateView, conversationId: number, id: number): EntryResult {
	const row = value<EntryRow>(view, K.e(conversationId, id));
	return row === undefined ? undefined : { entry: row.entry, commitSeq: brand<Seq>(row.seq) };
}

export function entryById(view: StateView, id: EntryId): EntryResult {
	const x = value<RegistryRow>(view, K.x(id));
	if (x?.t !== "e" || x.c === undefined) return undefined;
	return entryAt(view, x.c, id);
}

/** Entry visible through `conversationId`'s fork ancestry. */
export function entryInConversation(view: StateView, conversationId: ConversationId, id: EntryId): EntryResult {
	let conv = mustConversation(view, conversationId);
	const x = value<RegistryRow>(view, K.x(id));
	if (x?.t !== "e" || x.c === undefined) return undefined;
	let cap = Number.POSITIVE_INFINITY;
	while (conv.id !== x.c) {
		if (conv.parent === undefined) return undefined;
		cap = Math.min(cap, conv.parent.at);
		conv = mustConversation(view, conv.parent.conversationId);
	}
	if (id > cap) return undefined;
	return entryAt(view, x.c, id);
}

export function findLatestHeadMarker(
	view: StateView,
	conversationId: ConversationId,
	cutoff: EntryId | undefined,
): (EntryRecord & { readonly head: EntryId }) | undefined {
	let conv = mustConversation(view, conversationId);
	let upper = cutoff ?? Number.MAX_SAFE_INTEGER;
	for (;;) {
		const prefix = K.eh(conv.id);
		for (const [, text] of prefixScan(view, prefix, prefix + u64desc(upper))) {
			return parse<{ entry: EntryRecord & { readonly head: EntryId } }>(text).entry;
		}
		if (conv.parent === undefined) return undefined;
		upper = Math.min(upper, conv.parent.at);
		conv = mustConversation(view, conv.parent.conversationId);
	}
}

export function scanEntries(view: StateView, query: EntryQuery, limit: number, cursor: Cursor | undefined): Page<EntryRecord, Cursor> {
	let conv = mustConversation(view, query.conversationId);
	const after = cursorId(cursor);
	let upper = Math.min(query.maxEntryId ?? Number.MAX_SAFE_INTEGER, after === undefined ? Number.MAX_SAFE_INTEGER : after - 1);
	const lo = query.minEntryId ?? 0;
	const values: EntryRecord[] = [];
	for (;;) {
		if (upper >= lo) {
			const prefix = K.e(conv.id);
			for (const [, text] of view.scan(prefix + u64desc(upper), keySuccessor(prefix + u64desc(lo)))) {
				values.push(parse<EntryRow>(text).entry);
				if (values.length > limit) break;
			}
		}
		if (values.length > limit || conv.parent === undefined) break;
		upper = Math.min(upper, conv.parent.at);
		if (upper < lo) break;
		conv = mustConversation(view, conv.parent.conversationId);
	}
	return page(values, limit);
}

// ---------------------------------------------------------------- tasks and submissions

export const task = (view: StateView, id: TaskId): StoredTask | undefined => value<StoredTask>(view, K.t(id));

export function scanTasks(view: StateView, query: TaskQuery, limit: number, cursor: Cursor | undefined): Page<StoredTask, Cursor> {
	const code = query.status === undefined ? undefined : TASK_STATUS_CODE[query.status];
	let prefix: string;
	let keyOnly = false;
	if (code !== undefined) prefix = K.ts(code);
	else if (query.conversationId !== undefined) {
		prefix = K.tc(query.conversationId);
		keyOnly = true;
	} else if (query.kind !== undefined) {
		prefix = K.tk(query.kind);
		keyOnly = true;
	} else prefix = K.t();
	const values: StoredTask[] = [];
	for (const [k, text] of prefixScan(view, prefix, afterKey(prefix, cursorId(cursor)))) {
		const r = keyOnly ? value<StoredTask>(view, K.t(idSuffix(k))) : parse<StoredTask>(text);
		if (r === undefined) throw new Error(`Task index row without a task: ${idSuffix(k)}`);
		if (query.status !== undefined && r.state.status !== query.status) continue;
		if (query.conversationId !== undefined && r.conversationId !== query.conversationId) continue;
		if (query.kind !== undefined && r.kind !== query.kind) continue;
		if (query.abortRequested !== undefined && r.abortRequested !== query.abortRequested) continue;
		if (query.background !== undefined && r.background !== query.background) continue;
		values.push(r);
		if (values.length > limit) break;
	}
	return page(values, limit);
}

export const submission = (view: StateView, id: SubmissionId): SubmissionRecord | undefined =>
	value<SubmissionRecord>(view, K.s(id));

export function scanSubmissions(view: StateView, query: SubmissionQuery, limit: number, cursor: Cursor | undefined): Page<SubmissionRecord, Cursor> {
	const code = query.status === undefined ? undefined : SUBMISSION_STATUS_CODE[query.status];
	let prefix: string;
	let keyOnly = false;
	if (code !== undefined) prefix = K.ss(code);
	else if (query.conversationId !== undefined) {
		prefix = K.sc(query.conversationId);
		keyOnly = true;
	} else prefix = K.s();
	const values: SubmissionRecord[] = [];
	for (const [k, text] of prefixScan(view, prefix, afterKey(prefix, cursorId(cursor)))) {
		const r = keyOnly ? value<SubmissionRecord>(view, K.s(idSuffix(k))) : parse<SubmissionRecord>(text);
		if (r === undefined) throw new Error(`Submission index row without a submission: ${idSuffix(k)}`);
		if (query.status !== undefined && r.status !== query.status) continue;
		if (query.conversationId !== undefined && r.conversationId !== query.conversationId) continue;
		values.push(r);
		if (values.length > limit) break;
	}
	return page(values, limit);
}

export function submissionByRequest(view: StateView, conversationId: ConversationId, requestId: string): SubmissionRecord | undefined {
	const pointer = value<RequestRow>(view, K.sr(conversationId, requestId));
	// A digested key carries the full requestId; a mismatch is a miss (§4.2).
	if (pointer === undefined || (pointer.requestId !== undefined && pointer.requestId !== requestId)) return undefined;
	return value<SubmissionRecord>(view, K.s(pointer.id));
}

/** The `m/` family values the owner reads. */
export const meta = <T>(view: StateView, name: string): T | undefined => value<T>(view, K.m(name));

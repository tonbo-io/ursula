// Commit planner: StorageWrite[] → one keyed-batch-v1 record (design §3.3 step 2–3, §4.4, §7.7).
//
// Validation is ported from MemoryStorage.prepareCommit (pi: memory.ts:250-311, 679-761) with
// identical error classes and messages. `document.copy` is materialized here. The planner is a pure
// synchronous pass over a StateView; it never applies anything (write-through happens only after the
// append is confirmed).
import {
	type DocumentContent,
	type DocumentCreate,
	type DocumentRecord,
	type StorageWrite,
	StorageRejected,
	type SubmissionRecord,
} from "@earendil-works/pi-durable";
import { FORMAT_VALUE, K, META, SUBMISSION_STATUS_CODE, TASK_STATUS_CODE, type TableName } from "./families.ts";
import { encodeRecord, jsonDepth, type KeyedOp, utf8 } from "./keyed-batch.ts";
import {
	type DocumentRow,
	documentRow,
	findDocumentAt,
	isCurrentOnly,
	materializeDocument,
	type RequestRow,
	type StoredTask,
	tableOf,
	value,
} from "./pi-layer.ts";
import { LIMITS } from "./protocol.ts";
import { type StateView, ViewAbort } from "./state-store.ts";
import { isLongStr, strinc } from "./tuple.ts";

export interface PlanInput {
	/** The record ordinal this commit will occupy: the applied tail. Every Seq-valued field is N. */
	readonly seq: number;
	/** The owner's epoch, carried as `"o"`. */
	readonly epoch: number;
	/** Local mint counter. */
	readonly nextId: number;
	/** Value of `m/next_id` in state(tail), 0 when absent. */
	readonly persistedNextId: number;
}

export interface PlannedCommit {
	readonly seq: number;
	readonly ops: readonly KeyedOp[];
	readonly text: string;
	readonly bytes: Uint8Array;
	/** `max(nextId, written IDs + 1)`: adopted only after the append is confirmed. */
	readonly nextId: number;
	/** The persisted tracker after this commit. */
	readonly persistedNextId: number;
}

type DocAction = { create?: DocumentCreate; content?: DocumentContent; retire: boolean };
const enc = (v: unknown): string => JSON.stringify(v) as string;

export function planCommit(view: StateView, writes: readonly StorageWrite[], input: PlanInput): PlannedCommit {
	const N = input.seq;
	const detached = resolveDocumentCopies(view, writes.map((w) => JSON.parse(JSON.stringify(w)) as StorageWrite));
	checkGlobalIds(view, detached);
	const actions = prepareDocumentActions(detached);
	checkDocumentActions(view, actions);

	const ops: KeyedOp[] = [];
	const put = (key: string, v: string): void => {
		ops.push({ op: "p", key, value: v });
	};
	const del = (key: string): void => {
		ops.push({ op: "d", key });
	};
	const range = (start: string, end: string): void => {
		ops.push({ op: "x", start, end });
	};
	const taskOverlay = new Map<number, StoredTask>();
	const subOverlay = new Map<number, SubmissionRecord>();
	const requestOverlay = new Map<string, number | null>();
	let candidate = input.nextId;

	for (const w of detached) {
		switch (w.type) {
			case "conversation": {
				const R = w.value;
				const text = enc(R);
				put(K.c(R.id), text);
				if (R.owner !== undefined) {
					put(K.coc(R.owner.conversationId, R.id), text);
					put(K.cot(R.owner.taskId, R.id), text);
				}
				put(K.x(R.id), enc({ t: "c" }));
				candidate = Math.max(candidate, R.id + 1);
				break;
			}
			case "entry": {
				const R = w.value;
				const text = `{"seq":${N},"entry":${enc(R)}}`;
				put(K.e(R.conversationId, R.id), text);
				if (R.head !== undefined) put(K.eh(R.conversationId, R.id), text);
				put(K.x(R.id), enc({ t: "e", c: R.conversationId }));
				candidate = Math.max(candidate, R.id + 1);
				break;
			}
			case "task": {
				const R = w.value as StoredTask;
				const prev = taskOverlay.get(R.id) ?? value<StoredTask>(view, K.t(R.id));
				const code = TASK_STATUS_CODE[R.state.status];
				const text = enc(R);
				put(K.t(R.id), text);
				if (code !== undefined) put(K.ts(code, R.id), text);
				const prevCode = prev === undefined ? undefined : TASK_STATUS_CODE[prev.state.status];
				if (prev !== undefined && prevCode !== undefined && (prev.state.status !== R.state.status || code === undefined)) {
					del(K.ts(prevCode, R.id));
				}
				if (prev === undefined) {
					put(K.x(R.id), enc({ t: "t" }));
					put(K.tc(R.conversationId, R.id), "null");
					put(K.tk(R.kind, R.id), "null");
				} else {
					if (prev.conversationId !== R.conversationId) {
						del(K.tc(prev.conversationId, R.id));
						put(K.tc(R.conversationId, R.id), "null");
					}
					if (prev.kind !== R.kind) {
						del(K.tk(prev.kind, R.id));
						put(K.tk(R.kind, R.id), "null");
					}
				}
				taskOverlay.set(R.id, R);
				candidate = Math.max(candidate, R.id + 1);
				break;
			}
			case "submission": {
				const R = w.value;
				const prev = subOverlay.get(R.id) ?? value<SubmissionRecord>(view, K.s(R.id));
				const code = SUBMISSION_STATUS_CODE[R.status];
				const text = enc(R);
				put(K.s(R.id), text);
				if (code !== undefined) put(K.ss(code, R.id), text);
				const prevCode = prev === undefined ? undefined : SUBMISSION_STATUS_CODE[prev.status];
				if (prev !== undefined && prevCode !== undefined && (prev.status !== R.status || code === undefined)) {
					del(K.ss(prevCode, R.id));
				}
				if (prev === undefined) {
					put(K.x(R.id), enc({ t: "s" }));
					put(K.sc(R.conversationId, R.id), "null");
				} else if (prev.conversationId !== R.conversationId) {
					del(K.sc(prev.conversationId, R.id));
					put(K.sc(R.conversationId, R.id), "null");
				}
				if (prev?.requestId !== undefined) {
					// Delete the old pointer only while it still points at this submission (pi: memory.ts:377-392).
					const k = K.sr(prev.conversationId, prev.requestId);
					const current = requestOverlay.has(k) ? requestOverlay.get(k) : value<RequestRow>(view, k)?.id;
					if (current === R.id) {
						del(k);
						requestOverlay.set(k, null);
					}
				}
				if (R.requestId !== undefined) {
					const k = K.sr(R.conversationId, R.requestId);
					put(k, enc(isLongStr(R.requestId) ? { id: R.id, requestId: R.requestId } : { id: R.id }));
					requestOverlay.set(k, R.id);
				}
				subOverlay.set(R.id, R);
				candidate = Math.max(candidate, R.id + 1);
				break;
			}
			default:
				break;
		}
	}

	for (const [id, action] of actions) {
		if (action.create !== undefined) {
			const content = action.content as Extract<DocumentContent, { kind: "base" }>;
			const R = { ...action.create, createdAt: N, ...(action.retire ? { retiredAt: N } : {}) } as DocumentRecord;
			const text = enc(R);
			put(K.d(id), enc({ record: R, version: content.version }));
			put(K.da(R), text);
			put(K.ds(R.scope, id), text);
			put(K.db(id, N), enc({ version: content.version, value: content.value }));
			put(K.x(id), enc({ t: "d" }));
			if (action.retire && isCurrentOnly(R)) {
				range(K.db(id), strinc(K.db(id)));
				range(K.dr(id), strinc(K.dr(id)));
			}
			candidate = Math.max(candidate, id + 1);
			continue;
		}
		const row = documentRow(view, id) as DocumentRow;
		let version = row.version;
		if (action.content?.kind === "base") {
			put(K.db(id, N), enc({ version: action.content.version, value: action.content.value }));
			if (action.content.version !== row.version) {
				version = action.content.version;
				put(K.d(id), enc({ record: row.record, version }));
			}
			if (isCurrentOnly(row.record)) {
				// Together these equal MemoryStorage's `revisions = [new base]`.
				if (N > 0) range(K.db(id, N - 1), strinc(K.db(id)));
				if (N > 0) range(K.dr(id, 0), K.dr(id, N));
			}
		} else if (action.content?.kind === "delta") {
			put(K.dr(id, N), enc({ version: action.content.version, ops: action.content.ops }));
		}
		if (action.retire) {
			const R = { ...row.record, retiredAt: N } as DocumentRecord;
			const text = enc(R);
			put(K.d(id), enc({ record: R, version }));
			put(K.da(R), text);
			put(K.ds(R.scope, id), text);
			if (isCurrentOnly(R)) {
				range(K.db(id), strinc(K.db(id)));
				range(K.dr(id), strinc(K.dr(id)));
			}
		}
	}

	let persistedNextId = input.persistedNextId;
	if (candidate > persistedNextId) {
		put(K.m(META.nextId), enc(candidate));
		persistedNextId = candidate;
	}
	return finalize(N, input.epoch, ops, Math.max(input.nextId, candidate), persistedNextId);
}

/** Encode and run the pre-checks that mirror every server limit (§3.3 step 2, §7.6). */
export function finalize(seq: number, epoch: number, ops: readonly KeyedOp[], nextId: number, persistedNextId: number): PlannedCommit {
	for (const op of ops) {
		const keys = op.op === "x" ? [op.start, op.end] : [op.key];
		for (const k of keys) {
			if (k.length < 1 || k.length > LIMITS.maxKeyOctets) {
				throw new StorageRejected(`Commit key of ${k.length} octets exceeds the ${LIMITS.maxKeyOctets}-octet limit`);
			}
		}
	}
	const text = encodeRecord(epoch, ops);
	const depth = jsonDepth(text);
	if (depth > LIMITS.maxJsonDepth) {
		throw new StorageRejected(`Commit record nesting depth ${depth} exceeds the limit of ${LIMITS.maxJsonDepth}`);
	}
	const bytes = utf8(text);
	if (bytes.length > LIMITS.maxRecordBytes) {
		throw new StorageRejected(`Commit record of ${bytes.length} bytes exceeds the ${LIMITS.maxRecordBytes}-byte limit`);
	}
	return { seq, ops, text, bytes, nextId, persistedNextId };
}

/** Owner claim record (§3.6 step 6); at N = 0 it is also the genesis that writes `m/format`. */
export interface OwnerClaim {
	readonly epoch: number;
	readonly nonce: string;
	readonly host: string;
	readonly pid: number;
	readonly opened_at_ms: number;
	readonly mode: "fence" | "fail-if-active";
	readonly closed_at_ms?: number;
}

export function planClaim(seq: number, owner: OwnerClaim): PlannedCommit {
	const ops: KeyedOp[] = [];
	if (seq === 0) ops.push({ op: "p", key: K.m(META.format), value: enc(FORMAT_VALUE) });
	ops.push({ op: "p", key: K.m(META.owner), value: enc(owner) });
	return finalize(seq, seq, ops, 0, 0);
}

/** Close marker (§3.7): `m/owner` with `closed_at_ms`, carried under the owner's epoch. */
export function planCloseMarker(seq: number, owner: OwnerClaim, closedAtMs: number): PlannedCommit {
	return finalize(seq, owner.epoch, [{ op: "p", key: K.m(META.owner), value: enc({ ...owner, closed_at_ms: closedAtMs }) }], 0, 0);
}

// ---------------------------------------------------------------- MemoryStorage-ported validation

function resolveDocumentCopies(view: StateView, writes: StorageWrite[]): StorageWrite[] {
	if (!writes.some((w) => w.type === "document.copy")) return writes;
	const changed = new Set<number>();
	for (const w of writes) {
		if (w.type === "document.create" || w.type === "document.copy") changed.add(w.record.id);
		else if (w.type === "document.change" || w.type === "document.retire") changed.add(w.id);
	}
	return writes.map((w) => {
		if (w.type !== "document.copy") return w;
		try {
			if (changed.has(w.source.id)) throw new Error(`Fork source document ${w.source.id} is changed in the copy batch`);
			const stored = materializeDocument(view, w.source.id, w.source.at);
			if (stored === undefined) throw new Error(`Fork source document ${w.source.id} cannot be read`);
			if (
				stored.record.scope.kind !== "conversation" ||
				w.record.scope.kind !== "conversation" ||
				stored.record.kind !== w.record.kind ||
				stored.record.key !== w.record.key ||
				stored.record.history !== w.record.history ||
				stored.record.fork !== w.record.fork
			) {
				throw new Error(`Fork source document ${w.source.id} does not match the copied record`);
			}
			return { type: "document.create", record: w.record, content: { kind: "base", version: stored.version, value: stored.value } };
		} catch (error) {
			// A partial-state view's abort is not a copy failure: let the store fetch and re-run.
			if (error instanceof StorageRejected || error instanceof ViewAbort) throw error;
			throw new StorageRejected(`Document copy ${w.record.id} was rejected`, { cause: error });
		}
	});
}

function checkGlobalIds(view: StateView, writes: readonly StorageWrite[]): void {
	const claimed = new Map<number, TableName>();
	for (const w of writes) {
		if (w.type === "document.change" || w.type === "document.retire") continue;
		const isDocument = w.type === "document.create" || w.type === "document.copy";
		const table: TableName = isDocument ? "document" : (w.type as TableName);
		const id = isDocument ? w.record.id : w.value.id;
		const existing = tableOf(view, id);
		const earlier = claimed.get(id);
		if (table === "conversation" || table === "entry" || table === "document") {
			if (existing !== undefined) throw new Error(`ID ${id} already belongs to ${existing}`);
			if (earlier !== undefined) throw new Error(`ID ${id} is written more than once`);
		} else {
			if (existing !== undefined && existing !== table) throw new Error(`ID ${id} already belongs to ${existing}`);
			if (earlier !== undefined && earlier !== table) throw new Error(`ID ${id} is written as two record types`);
		}
		claimed.set(id, table);
	}
}

function prepareDocumentActions(writes: readonly StorageWrite[]): Map<number, DocAction> {
	const actions = new Map<number, DocAction>();
	for (const w of writes) {
		if (w.type !== "document.create" && w.type !== "document.change" && w.type !== "document.retire") continue;
		const id = w.type === "document.create" ? w.record.id : w.id;
		let a = actions.get(id);
		if (a === undefined) {
			a = { retire: false };
			actions.set(id, a);
		}
		if (w.type === "document.create") {
			if (a.create !== undefined || a.content !== undefined) throw new Error(`Document ${id} has more than one content command`);
			a.create = w.record;
			a.content = w.content;
		} else if (w.type === "document.change") {
			if (a.content !== undefined) throw new Error(`Document ${id} has more than one content command`);
			a.content = w.content;
		} else {
			if (a.retire) throw new Error(`Document ${id} is retired more than once`);
			a.retire = true;
		}
	}
	return actions;
}

function checkDocumentActions(view: StateView, actions: ReadonlyMap<number, DocAction>): void {
	const live = new Map<string, number>();
	for (const [id, a] of actions) {
		const existing = documentRow(view, id);
		if (a.create === undefined && existing === undefined) throw new Error(`Unknown document: ${id}`);
		if (a.create !== undefined && existing !== undefined) throw new Error(`Document ${id} already exists`);
		if (existing?.record.retiredAt !== undefined) throw new Error(`Document ${id} is retired`);
		if (a.content?.kind === "delta") {
			if (existing === undefined) throw new Error(`Document ${id} delta has no base`);
			if (existing.version !== a.content.version) throw new Error(`Document ${id} version transition requires a base`);
		}
		const rec = (a.create ?? existing?.record) as DocumentRecord | DocumentCreate;
		const key = K.daAddress(rec);
		let n = live.get(key);
		const current = findDocumentAt(view, rec, "current");
		if (n === undefined) n = current === undefined ? 0 : 1;
		if (a.retire && current?.id === id) n--;
		if (a.create !== undefined && !a.retire) n++;
		live.set(key, n);
	}
	for (const n of live.values()) if (n > 1) throw new Error("Document address already has a current incarnation");
}

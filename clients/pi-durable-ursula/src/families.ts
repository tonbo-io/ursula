// Pi key families (design §4.3). Every key is `tag ‖ components` built with the tuple layer.
// † components (IDs) are what complete-at-mint (§7.3) keys on in the bounded owner.
import type { DocumentRecord } from "@earendil-works/pi-durable";
import { str, u64, u64desc, u8 } from "./tuple.ts";

export const TAG = {
	m: 0x01,
	x: 0x02,
	c: 0x10,
	coc: 0x11,
	cot: 0x12,
	e: 0x20,
	eh: 0x21,
	t: 0x30,
	ts: 0x31,
	tc: 0x32,
	tk: 0x33,
	s: 0x40,
	ss: 0x41,
	sr: 0x42,
	sc: 0x43,
	d: 0x50,
	da: 0x51,
	ds: 0x52,
	db: 0x53,
	dr: 0x54,
} as const;

/** Live task statuses only; terminal tasks have no `t.s` row. */
export const TASK_STATUS_CODE: Readonly<Record<string, number | undefined>> = {
	pending: 1,
	running: 2,
	waiting: 3,
	completing: 4,
};
/** Unsettled submission statuses only; settled submissions have no `s.s` row. */
export const SUBMISSION_STATUS_CODE: Readonly<Record<string, number | undefined>> = { queued: 1, placed: 2 };
export const SCOPE_CODE = { session: 1, conversation: 2, task: 3 } as const;

/** Values of the `x/{id}` registry row. */
export type TableTag = "c" | "e" | "t" | "s" | "d";
export type TableName = "conversation" | "entry" | "task" | "submission" | "document";
export const TABLE_OF_TAG: Readonly<Record<TableTag, TableName>> = {
	c: "conversation",
	e: "entry",
	t: "task",
	s: "submission",
	d: "document",
};

/** `m/` metadata names. */
export const META = { nextId: "next_id", owner: "owner", format: "format" } as const;

/** The Pi schema version written to `m/format` at genesis. */
export const FORMAT_VALUE = { pi_durable_keyed: 1, tuple: 1 } as const;

const tag = (t: number): string => String.fromCharCode(t);
const opt = (id: number | undefined, enc: (x: number) => string): string => (id === undefined ? "" : enc(id));

type Scope = DocumentRecord["scope"];
export interface DocumentAddressLike {
	readonly kind: string;
	readonly scope: Scope;
	readonly key?: string;
}

export function scopeKey(scope: Scope): string {
	const owner = scope.kind === "session" ? 0 : scope.kind === "conversation" ? scope.conversationId : scope.taskId;
	return u8(SCOPE_CODE[scope.kind]) + u64(owner);
}

/** Key builders. Omitting the trailing ID component yields the family prefix for scans. */
export const K = {
	m: (name: string): string => tag(TAG.m) + str(name),
	x: (id: number): string => tag(TAG.x) + u64(id),
	c: (id?: number): string => tag(TAG.c) + opt(id, u64),
	coc: (ownerConversation: number, id?: number): string => tag(TAG.coc) + u64(ownerConversation) + opt(id, u64),
	cot: (ownerTask: number, id?: number): string => tag(TAG.cot) + u64(ownerTask) + opt(id, u64),
	e: (conversation: number, id?: number): string => tag(TAG.e) + u64(conversation) + opt(id, u64desc),
	eh: (conversation: number, id?: number): string => tag(TAG.eh) + u64(conversation) + opt(id, u64desc),
	t: (id?: number): string => tag(TAG.t) + opt(id, u64),
	ts: (code: number, id?: number): string => tag(TAG.ts) + u8(code) + opt(id, u64),
	tc: (conversation: number, id?: number): string => tag(TAG.tc) + u64(conversation) + opt(id, u64),
	tk: (kind: string, id?: number): string => tag(TAG.tk) + str(kind) + opt(id, u64),
	s: (id?: number): string => tag(TAG.s) + opt(id, u64),
	ss: (code: number, id?: number): string => tag(TAG.ss) + u8(code) + opt(id, u64),
	sr: (conversation: number, requestId: string): string => tag(TAG.sr) + u64(conversation) + str(requestId),
	sc: (conversation: number, id?: number): string => tag(TAG.sc) + u64(conversation) + opt(id, u64),
	d: (id: number): string => tag(TAG.d) + u64(id),
	/** `d.a` address prefix: scope, kind, singleton/family flag, key. */
	daAddress: (a: DocumentAddressLike): string =>
		tag(TAG.da) + scopeKey(a.scope) + str(a.kind) + u8(a.key === undefined ? 0 : 1) + str(a.key ?? ""),
	da: (r: DocumentRecord): string => K.daAddress(r) + u64desc(r.createdAt) + u64(r.id),
	ds: (scope: Scope, id?: number): string => tag(TAG.ds) + scopeKey(scope) + opt(id, u64),
	db: (id: number, seq?: number): string => tag(TAG.db) + u64(id) + opt(seq, u64desc),
	dr: (id: number, seq?: number): string => tag(TAG.dr) + u64(id) + opt(seq, u64),
};

// Bounded-owner policies over the Pi key schema: fetch widening (§4.5) and open preload (§3.6 step 4,
// §7.4). Both only decide WHICH ranges to read; LocalStore keeps every covered range exact.
import { K, scopeKey, TAG } from "./families.ts";
import type { LocalStore } from "./local-store/index.ts";
import type { EntryRow } from "./pi-layer.ts";
import type { StateView } from "./state-store.ts";
import { idSuffix, readU64Desc, strinc } from "./tuple.ts";

const tag = (t: number): string => String.fromCharCode(t);

/** Families whose keys start with `tag ‖ u64(scope ID)`, read as per-scope ranges. */
const SCOPED = new Set<number>([TAG.e, TAG.eh, TAG.sr, TAG.db, TAG.dr]);

/**
 * Widen a keyed-state fetch (LocalStore `widen`): a scan inside one scope of `e`, `e.h`, `s.r`, `d.b`
 * or `d.r` reads on to the end of the scope, so later reads of the same conversation or document
 * stay local; a point miss on `s.r/{conv}/{requestId}` fetches the whole `s.r/{conv}/` prefix (§4.5:
 * about 70 B per requestId, after which requestId lookups in that conversation are local).
 */
export function widenFetch(lo: string, hi: string, point: boolean): { lo: string; hi: string } | undefined {
	if (lo.length < 9) return undefined;
	const t = lo.charCodeAt(0);
	if (!SCOPED.has(t)) return undefined;
	const scope = lo.slice(0, 9);
	const end = strinc(scope);
	if (point) return t === TAG.sr ? { lo: scope, hi: end } : undefined;
	return hi <= end ? { lo, hi: end } : undefined;
}

/** Visit up to `max` rows of `[lo, hi)` so the pass covers them; returns the values. */
function touch(view: StateView, lo: string, hi: string, max = Number.POSITIVE_INFINITY): string[] {
	const out: string[] = [];
	if (max <= 0) return out;
	for (const [, text] of view.scan(lo, hi)) {
		out.push(text);
		if (out.length >= max) break;
	}
	return out;
}

const prefixOf = (p: string): [string, string] => [p, strinc(p)];

/** At most this many root-conversation documents are preloaded. */
const ROOT_DOCUMENTS = 64;

/** `d/{id}` and the newest base in parallel, then the deltas after that base (the current-document plan, §4.5). */
async function preloadDocument(store: LocalStore, id: number): Promise<void> {
	const basePrefix = K.db(id);
	const [, base] = await Promise.all([
		store.read((v) => v.get(K.d(id))),
		store.read((v) => {
			for (const [k] of v.scan(...prefixOf(basePrefix))) return Number(readU64Desc(k, basePrefix.length));
			return undefined;
		}),
	]);
	if (base !== undefined) await store.read((v) => touch(v, K.dr(id, base + 1), strinc(K.dr(id))));
}

interface LiveRow {
	readonly key: string;
	readonly id: number;
	readonly text: string;
}

/**
 * Open preload (§3.6 step 4), every read at `min_through_record = E` and merged per §3.5:
 * - the live set, `t.s/` and `s.s/`, with derived point rows `t/{id}`, `s/{id}` and `x/{id}` (§7.4);
 * - session-scope `d.s/` and `d.a/`;
 * - for each live task T: `d.s/{task T}`, `d.a/{task T}` and `c.ot/{T}/`;
 * - `c/` of every conversation referenced by live tasks or unsettled submissions, along owner and
 *   fork chains until closed;
 * - the root conversation: `c/1`, its newest `e.h/1` marker and its newest `e/1` page (257 rows);
 * - the root conversation's live documents (`pi.agent`, `pi.inbox`, ...): `d.s/{conversation 1}`,
 *   `d.a/{conversation 1}`, and per document `d/{id}`, its newest `d.b` base and the `d.r` deltas
 *   after it, which the first `viewState` and the first turn read (§9.3).
 *
 * Independent reads run in parallel, so open waits for about four keyed-state round trips whatever
 * the history length (§9.3: open is flat in history length and conversation count).
 *
 * It must run before anything else uses the store (open, before the claim): derived rows are
 * installed only while the tail is still the one the live-set read observed.
 */
export async function preload(store: LocalStore, rootConversation = 1): Promise<void> {
	const rows = (t: number): Promise<{ rows: LiveRow[]; tail: number }> =>
		store.read((v) => {
			const out: LiveRow[] = [];
			for (const [key, text] of v.scan(tag(t), tag(t + 1))) out.push({ key, id: idSuffix(key), text });
			return { rows: out, tail: store.tail };
		});
	const [tasks, submissions] = await Promise.all([rows(TAG.ts), rows(TAG.ss)]);
	const live = { tasks: tasks.rows, submissions: submissions.rows };
	if (tasks.tail === store.tail && submissions.tail === store.tail) {
		for (const r of live.tasks) {
			store.installDerived(K.t(r.id), r.text, r.key);
			store.installDerived(K.x(r.id), '{"t":"t"}', r.key);
		}
		for (const r of live.submissions) {
			store.installDerived(K.s(r.id), r.text, r.key);
			store.installDerived(K.x(r.id), '{"t":"s"}', r.key);
		}
	}

	const conversations = new Set<number>([rootConversation]);
	const reads: Promise<unknown>[] = [];
	const scoped = (scope: Parameters<typeof scopeKey>[0]): void => {
		reads.push(store.read((v) => touch(v, ...prefixOf(K.ds(scope)))));
		reads.push(store.read((v) => touch(v, ...prefixOf(tag(TAG.da) + scopeKey(scope)))));
	};
	scoped({ kind: "session" });
	for (const r of live.tasks) {
		const task = JSON.parse(r.text) as { conversationId?: number };
		if (task.conversationId !== undefined) conversations.add(task.conversationId);
		scoped({ kind: "task", taskId: r.id } as Parameters<typeof scopeKey>[0]);
		reads.push(store.read((v) => touch(v, ...prefixOf(K.cot(r.id)))));
	}
	for (const r of live.submissions) {
		const sub = JSON.parse(r.text) as { conversationId?: number };
		if (sub.conversationId !== undefined) conversations.add(sub.conversationId);
	}
	// Root conversation: newest head marker and newest entry page.
	reads.push(store.read((v) => touch(v, ...prefixOf(K.eh(rootConversation)), 1)));
	reads.push(store.read((v) => touch(v, ...prefixOf(K.e(rootConversation)), 257).map((t) => (JSON.parse(t) as EntryRow).seq)));
	// Root conversation documents.
	const rootScope = { kind: "conversation", conversationId: rootConversation } as Parameters<typeof scopeKey>[0];
	reads.push(store.read((v) => touch(v, ...prefixOf(tag(TAG.da) + scopeKey(rootScope)))));
	reads.push(
		(async () => {
			const docs = await store.read((v) =>
				touch(v, ...prefixOf(K.ds(rootScope)), ROOT_DOCUMENTS)
					.map((t) => JSON.parse(t) as { id: number; retiredAt?: number })
					.filter((r) => r.retiredAt === undefined),
			);
			await Promise.all(docs.map((r) => preloadDocument(store, r.id)));
		})(),
	);
	// Conversations along owner and fork chains, one hop per round.
	reads.push(
		(async () => {
			const seen = new Set<number>();
			let frontier = [...conversations];
			while (frontier.length > 0) {
				const batch = frontier.filter((id) => !seen.has(id));
				for (const id of batch) seen.add(id);
				const records = await Promise.all(
					batch.map((id) => store.read((v) => v.get(K.c(id)))),
				);
				frontier = [];
				for (const text of records) {
					if (text === undefined) continue;
					const c = JSON.parse(text) as { owner?: { conversationId: number }; parent?: { conversationId: number } };
					if (c.owner !== undefined) frontier.push(c.owner.conversationId);
					if (c.parent !== undefined) frontier.push(c.parent.conversationId);
				}
			}
		})(),
	);
	await Promise.all(reads);
}

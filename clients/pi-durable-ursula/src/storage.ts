// UrsulaStorage: Pi's Storage on one keyed Ursula stream (design §3.3, §3.6, §3.7, §7.6–§7.8).
//
// M1 shape: a full-resident StateStore (E = 0, full replay from record 0 at open). The commit path,
// the stateful outcome policy with read-back, open with claim in both modes, close with a close
// marker, and poison semantics are the final ones.
import { randomBytes } from "node:crypto";
import { hostname } from "node:os";
import type { Context } from "@earendil-works/chord";
import {
	type ConversationId,
	type ConversationQuery,
	type ConversationRecord,
	type Cursor,
	type DocumentAddress,
	type DocumentId,
	type DocumentPoint,
	type DocumentQuery,
	type DocumentRecord,
	type EntryId,
	type EntryQuery,
	type EntryRecord,
	type Id,
	type Page,
	type Seq,
	type Storage,
	type StorageWrite,
	StorageRejected,
	type StoredDocument,
	type SubmissionId,
	type SubmissionQuery,
	type SubmissionRecord,
	type TaskId,
	type TaskQuery,
} from "@earendil-works/pi-durable";
import { ClaimTimeout, FencedError, OpenRefused, OwnershipActive, OwnershipContention } from "./errors.ts";
import { FORMAT_VALUE, K, META } from "./families.ts";
import { bytesEqual, fromUtf8, type KeyedOp, parseKeyedBatch } from "./keyed-batch.ts";
import * as pi from "./pi-layer.ts";
import { type OwnerClaim, type PlannedCommit, planClaim, planCloseMarker, planCommit } from "./planner.ts";
import { EXT_KEYED_BATCH, EXT_KEYED_STATE, extensionTokens, H, intHeader, retryAfterMs } from "./protocol.ts";
import { FullResidentStateStore, type StateStore, type StateView } from "./state-store.ts";
import { type HttpOutcome, type KeyedStateTransport, type LogTransport, type ReadRecordsOutcome, TransportError } from "./transport.ts";
import { b64 } from "./tuple.ts";

export type OpenMode = "fence" | "fail-if-active";

export interface Clock {
	now(): number;
	sleep(ms: number): Promise<void>;
}

export const systemClock: Clock = {
	now: () => Date.now(),
	sleep: (ms) => new Promise((resolve) => setTimeout(resolve, ms)),
};

export interface Timing {
	/** `fail-if-active` activity window W (§3.6, §13 Q4). */
	readonly activityWindowMs: number;
	/** Claim loop deadline (§3.6 step 6). */
	readonly claimDeadlineMs: number;
	/** Per-commit deadline before poisoning (§3.3). */
	readonly commitDeadlineMs: number;
	/** Open deadline for idempotent reads (§3.6 step 2). */
	readonly openDeadlineMs: number;
	/** Budget for appending the close marker (§3.7). */
	readonly closeMarkerDeadlineMs: number;
	readonly backoffBaseMs: number;
	readonly backoffMaxMs: number;
	/** P7 page size for replay when the node advertises `keyed-state-v1`. */
	readonly replayPageBytes: number;
	/** `max_records` page size for replay otherwise. */
	readonly replayPageRecords: number;
}

export const DEFAULT_TIMING: Timing = {
	activityWindowMs: 3000,
	claimDeadlineMs: 5000,
	commitDeadlineMs: 30_000,
	openDeadlineMs: 120_000,
	closeMarkerDeadlineMs: 2000,
	backoffBaseMs: 10,
	backoffMaxMs: 1000,
	replayPageBytes: 16 * 1024 * 1024,
	replayPageRecords: 1000,
};

export interface OwnerAlert {
	readonly kind: "server-limit";
	readonly status: number;
	readonly message: string;
}

export interface UrsulaStorageOptions {
	readonly log: LogTransport;
	/** Keyed-state resource. Optional in M1, where state is fully replayed from the log. */
	readonly keyedState?: KeyedStateTransport;
	/** Default `fail-if-active` (§13 Q4). */
	readonly mode?: OpenMode;
	/**
	 * Refuse to open unless the node advertises `keyed-batch-v1` (default true, §3.6 step 1). An ungated
	 * node silently accepts invalid batches, so only tests against a node without P2 turn this off.
	 */
	readonly requireKeyedBatch?: boolean;
	/** Refuse to open unless the node advertises `keyed-state-v1`. */
	readonly requireKeyedState?: boolean;
	readonly host?: string;
	readonly pid?: number;
	readonly timing?: Partial<Timing>;
	readonly clock?: Clock;
	/** Called when a first-attempt 400/413/422 shows that a pre-check missed a server limit. */
	readonly onAlert?: (alert: OwnerAlert) => void;
}

const is2xx = (s: number): boolean => s >= 200 && s < 300;
const isAuth = (s: number): boolean => s === 401 || s === 403;
const isGone = (s: number): boolean => s === 404 || s === 409 || s === 410;
const describe = (o: HttpOutcome): string => `${o.status}${o.message === undefined ? "" : ` (${o.message})`}`;

class Backoff {
	private attempt = 0;
	private readonly clock: Clock;
	private readonly timing: Timing;
	constructor(clock: Clock, timing: Timing) {
		this.clock = clock;
		this.timing = timing;
	}
	async wait(deadline: number, retryAfter?: number): Promise<void> {
		const exp = Math.min(this.timing.backoffMaxMs, this.timing.backoffBaseMs * 2 ** this.attempt++);
		const jittered = exp * (0.5 + Math.random() / 2);
		const ms = Math.max(0, Math.min(retryAfter ?? jittered, deadline - this.clock.now()));
		if (ms > 0) await this.clock.sleep(ms);
	}
}

interface ReplayedRecord {
	readonly ordinal: number;
	readonly bytes: Uint8Array;
	readonly ops: KeyedOp[];
}

/** True when a record carries an owner claim (a put of `m/owner` without `closed_at_ms`). */
function isClaim(ops: readonly KeyedOp[]): boolean {
	const ownerKey = K.m(META.owner);
	return ops.some((op) => op.op === "p" && op.key === ownerKey && (JSON.parse(op.value) as OwnerClaim).closed_at_ms === undefined);
}

/** Retry an idempotent read on transient outcomes (§7.6) until `deadline`, then throw `onDeadline()`. */
async function retryIdempotent<T extends HttpOutcome>(
	clock: Clock,
	timing: Timing,
	deadline: number,
	request: () => Promise<T>,
	onDeadline: (last: string) => Error,
): Promise<T> {
	const backoff = new Backoff(clock, timing);
	let last = "no attempt";
	for (;;) {
		if (clock.now() > deadline) throw onDeadline(last);
		let outcome: T;
		try {
			outcome = await request();
		} catch (error) {
			if (!(error instanceof TransportError)) throw error;
			last = error.message;
			await backoff.wait(deadline);
			continue;
		}
		if (outcome.status >= 500 || outcome.status === 429) {
			last = describe(outcome);
			await backoff.wait(deadline, retryAfterMs(outcome.headers));
			continue;
		}
		return outcome;
	}
}

export class UrsulaStorage implements Storage {
	private readonly log: LogTransport;
	private readonly keyedState: KeyedStateTransport | undefined;
	private readonly store: StateStore;
	private readonly timing: Timing;
	private readonly clock: Clock;
	private readonly onAlert: ((alert: OwnerAlert) => void) | undefined;
	private readonly p7: boolean;
	private readonly owner: OwnerClaim;
	private nextId: number;
	private persistedNextId: number;
	private chain: Promise<unknown> = Promise.resolve();
	private closing: Promise<void> | undefined;
	private poisoned: Error | undefined;

	private constructor(
		options: UrsulaStorageOptions,
		timing: Timing,
		clock: Clock,
		store: StateStore,
		owner: OwnerClaim,
		p7: boolean,
		nextId: number,
		persistedNextId: number,
	) {
		this.log = options.log;
		this.keyedState = options.keyedState;
		this.onAlert = options.onAlert;
		this.timing = timing;
		this.clock = clock;
		this.store = store;
		this.owner = owner;
		this.p7 = p7;
		this.nextId = nextId;
		this.persistedNextId = persistedNextId;
	}

	/** The owner's epoch: the ordinal of its claim record. */
	get epoch(): number {
		return this.owner.epoch;
	}
	/** Exclusive applied tail. */
	get tail(): number {
		return this.store.tail;
	}
	/** The error that poisoned this storage, if any. */
	get poison(): Error | undefined {
		return this.poisoned;
	}

	// ============================================================ open (§3.6)

	static async open(options: UrsulaStorageOptions): Promise<UrsulaStorage> {
		const timing: Timing = { ...DEFAULT_TIMING, ...options.timing };
		const clock = options.clock ?? systemClock;
		const mode: OpenMode = options.mode ?? "fail-if-active";
		const log = options.log;
		const deadline = clock.now() + timing.openDeadlineMs;
		const openError = (what: string) => (last: string) => new Error(`UrsulaStorage open: ${what} did not succeed before the deadline: ${last}`);

		// Step 1: HEAD, idempotent create on 404, capability check.
		let head = await retryIdempotent(clock, timing, deadline, () => log.head(), openError("HEAD"));
		if (head.status === 404) {
			const created = await retryIdempotent(clock, timing, deadline, () => log.create(), openError("create"));
			if (!is2xx(created.status)) throw new Error(`UrsulaStorage open: create failed with ${describe(created)}`);
			// HEAD again: an idempotent create may have met a concurrent creator's records.
			head = await retryIdempotent(clock, timing, deadline, () => log.head(), openError("HEAD"));
		}
		if (!is2xx(head.status)) throw new Error(`UrsulaStorage open: HEAD failed with ${describe(head)}`);
		const tokens = extensionTokens(head.headers);
		if (options.requireKeyedBatch !== false && !tokens.has(EXT_KEYED_BATCH)) {
			throw new OpenRefused(`the node does not advertise ${EXT_KEYED_BATCH} for this stream`);
		}
		if (options.requireKeyedState === true && !tokens.has(EXT_KEYED_STATE)) {
			throw new OpenRefused(`the node does not advertise ${EXT_KEYED_STATE} for this stream`);
		}
		const p7 = tokens.has(EXT_KEYED_STATE);
		const n0 = intHeader(head.headers, H.recordNext);
		if (n0 === undefined) throw new OpenRefused("HEAD did not return Stream-Record-Next");

		// Steps 2–3 (M1): full replay from record 0 into a full-resident store.
		const store = new FullResidentStateStore();
		await replay(log, store, p7, clock, timing, deadline);
		if (store.tail < n0) throw new Error(`UrsulaStorage open: replay ended at ${store.tail} below HEAD's ${n0}`);
		const activity = store.tail > n0;

		const format = store.readSync((v) => pi.meta<{ pi_durable_keyed?: number; tuple?: number }>(v, META.format));
		if (store.tail > 0 && format === undefined) throw new OpenRefused("the stream is not a Pi Durable keyed log (no m/format)");
		if (format !== undefined && ((format.pi_durable_keyed ?? 0) > FORMAT_VALUE.pi_durable_keyed || (format.tuple ?? 0) > FORMAT_VALUE.tuple)) {
			throw new OpenRefused(`the stream's format ${JSON.stringify(format)} is newer than this owner's`);
		}

		// Step 5: mode check. Nothing has been written yet.
		if (mode === "fail-if-active") {
			if (activity) throw new OwnershipActive(`records beyond ${n0} appeared during open: the current owner is writing`);
			const current = store.readSync((v) => pi.meta<OwnerClaim>(v, META.owner));
			if (current !== undefined && current.closed_at_ms === undefined) {
				const poll = await retryIdempotent(
					clock,
					timing,
					deadline,
					() => log.readRecords(store.tail, { longPollMs: timing.activityWindowMs }),
					openError("activity long-poll"),
				);
				if (poll.status === 200 && poll.records.length > 0) {
					throw new OwnershipActive(`the current owner (epoch ${current.epoch}) wrote within ${timing.activityWindowMs} ms`);
				}
				if (!is2xx(poll.status)) throw new Error(`UrsulaStorage open: activity long-poll failed with ${describe(poll)}`);
			}
		}

		// Step 6: claim.
		const owner = await claim(options, mode, log, store, p7, clock, timing);
		const nextIdRow = store.readSync((v) => pi.meta<number>(v, META.nextId));
		return new UrsulaStorage(options, timing, clock, store, owner, p7, nextIdRow ?? 2, nextIdRow ?? 0);
	}

	// ============================================================ commit (§3.3)

	async commit(writes: readonly StorageWrite[], _context: Context): Promise<Seq> {
		this.assertUsable();
		const run = this.chain.then(() => this.commitNow(writes));
		this.chain = run.catch(() => undefined);
		return run;
	}

	private async commitNow(writes: readonly StorageWrite[]): Promise<Seq> {
		// Accepted before close(): close waits for it, so only poison is re-checked here.
		this.assertNotPoisoned();
		const seq = this.store.tail;
		const plan = await this.store.read((view) =>
			planCommit(view, writes, { seq, epoch: this.owner.epoch, nextId: this.nextId, persistedNextId: this.persistedNextId }),
		);
		await this.appendWithPolicy(plan);
		this.store.apply(seq, plan.ops);
		this.nextId = Math.max(this.nextId, plan.nextId);
		this.persistedNextId = plan.persistedNextId;
		return seq as Seq;
	}

	/** The stateful outcome policy (§3.3). Resolves when record N holds this plan's bytes. */
	private async appendWithPolicy(plan: PlannedCommit): Promise<void> {
		const N = plan.seq;
		const deadline = this.clock.now() + this.timing.commitDeadlineMs;
		const backoff = new Backoff(this.clock, this.timing);
		let ambiguous = false;
		for (;;) {
			if (this.clock.now() > deadline) throw this.poisonWith(new Error(`commit at record ${N} did not resolve within ${this.timing.commitDeadlineMs} ms`));
			let outcome: HttpOutcome | undefined;
			try {
				outcome = await this.log.append(plan.bytes, N);
			} catch (error) {
				if (!(error instanceof TransportError)) throw this.poisonWith(asError(error));
				ambiguous = true;
				await backoff.wait(deadline);
			}
			if (outcome !== undefined) {
				const s = outcome.status;
				const retryAfter = retryAfterMs(outcome.headers);
				if (is2xx(s)) {
					const start = intHeader(outcome.headers, H.recordStart);
					if (start === N) return;
					throw this.poisonWith(new Error(`invariant violation: append matched at ${N} but Stream-Record-Start is ${start}`));
				}
				if (s === 412) {
					// Always read back: a 412 can be self-inflicted by a gateway replay of our own landed attempt.
				} else if (s === 400 || s === 413 || s === 422) {
					if (!ambiguous) {
						this.onAlert?.({ kind: "server-limit", status: s, message: outcome.message ?? "" });
						throw new StorageRejected(`Ursula rejected the commit with ${describe(outcome)}`);
					}
				} else if (s === 429 && retryAfter === undefined) {
					if (!ambiguous) throw new StorageRejected(`Ursula rejected the commit with ${describe(outcome)} (quota)`);
				} else if (isGone(s)) {
					if (!ambiguous) throw this.poisonWith(new Error(`the log is unavailable: append answered ${describe(outcome)}`));
					if ((await this.readBack(plan, deadline)) === "ours") return;
					throw this.poisonWith(new Error(`the log is unavailable: append answered ${describe(outcome)}`));
				} else if (isAuth(s)) {
					throw this.poisonWith(new Error(`append was not authorized: ${describe(outcome)}; refresh credentials and reopen`));
				} else {
					// 5xx, 429 with Retry-After, and anything unexpected: the outcome is unknown.
					ambiguous = true;
					await backoff.wait(deadline, retryAfter);
				}
			}
			if ((await this.readBack(plan, deadline)) === "ours") return;
			await backoff.wait(deadline);
		}
	}

	/** Read back record N (§3.3 read-back table). Resolves "ours" or "absent" (tail = N); poisons otherwise. */
	private async readBack(plan: PlannedCommit, deadline: number): Promise<"ours" | "absent"> {
		const N = plan.seq;
		const backoff = new Backoff(this.clock, this.timing);
		for (;;) {
			if (this.clock.now() > deadline) throw this.poisonWith(new Error(`read-back of record ${N} did not resolve within the commit deadline`));
			let outcome: ReadRecordsOutcome;
			try {
				outcome = await this.log.readRecords(N, {
					maxRecords: 1,
					leader: true,
					...(this.p7 ? { maxBytes: plan.bytes.length + 1 } : {}),
				});
			} catch (error) {
				if (!(error instanceof TransportError)) throw this.poisonWith(asError(error));
				await backoff.wait(deadline);
				continue;
			}
			const s = outcome.status;
			if (s === 200 || s === 204) {
				const first = outcome.records[0];
				if (first !== undefined) {
					if (bytesEqual(first, plan.bytes)) return "ours";
					throw this.poisonWith(new FencedError(`record ${N} was written by another owner`));
				}
				if (intHeader(outcome.headers, H.recordNext) === N) return "absent";
			} else if (s === 400) {
				const next = intHeader(outcome.headers, H.recordNext);
				if (next === undefined || next >= N) throw this.poisonWith(new Error(`read-back of record ${N} answered ${describe(outcome)}`));
				// A lagging ex-leader (tail < N): retry.
			} else if (isGone(s)) {
				throw this.poisonWith(new Error(`the log is unavailable: read-back answered ${describe(outcome)}`));
			} else if (isAuth(s)) {
				throw this.poisonWith(new Error(`read-back was not authorized: ${describe(outcome)}; refresh credentials and reopen`));
			}
			await backoff.wait(deadline, retryAfterMs(outcome.headers));
		}
	}

	// ============================================================ close (§3.7)

	async close(_context: Context): Promise<void> {
		if (this.closing === undefined) this.closing = this.closeNow();
		return this.closing;
	}

	private async closeNow(): Promise<void> {
		await this.chain;
		if (this.poisoned === undefined) {
			try {
				await this.appendCloseMarker();
			} catch {
				// The next fail-if-active open pays W once.
			}
		}
		if (this.keyedState !== undefined) {
			try {
				await this.keyedState.scan({ key: b64(K.m(META.owner)), minThroughRecord: this.store.tail, timeoutMs: 1 });
			} catch {
				// Finalize-on-close is best effort.
			}
		}
		this.store.close();
	}

	private async appendCloseMarker(): Promise<void> {
		const N = this.store.tail;
		const plan = planCloseMarker(N, this.owner, Date.now());
		const deadline = this.clock.now() + this.timing.closeMarkerDeadlineMs;
		const backoff = new Backoff(this.clock, this.timing);
		while (this.clock.now() <= deadline) {
			let ambiguous = false;
			try {
				const outcome = await this.log.append(plan.bytes, N);
				if (is2xx(outcome.status) && intHeader(outcome.headers, H.recordStart) === N) {
					this.store.apply(N, plan.ops);
					return;
				}
				if (outcome.status !== 412 && outcome.status < 500) return;
				ambiguous = outcome.status >= 500;
			} catch (error) {
				if (!(error instanceof TransportError)) return;
				ambiguous = true;
			}
			if (ambiguous) await backoff.wait(deadline);
			const read = await this.log.readRecords(N, { maxRecords: 1, leader: true });
			const first = read.records[0];
			if (first !== undefined) {
				if (bytesEqual(first, plan.bytes)) this.store.apply(N, plan.ops);
				return;
			}
			if (!ambiguous) return;
		}
	}

	// ============================================================ state checks

	private assertUsable(): void {
		if (this.closing !== undefined) throw new Error("UrsulaStorage is closed");
		this.assertNotPoisoned();
	}

	private assertNotPoisoned(): void {
		const p = this.poisoned;
		if (p !== undefined) {
			const message = `UrsulaStorage is poisoned: ${p.message}`;
			throw p instanceof FencedError ? new FencedError(message, { cause: p }) : new Error(message, { cause: p });
		}
	}

	private poisonWith(error: Error): Error {
		this.poisoned ??= error;
		return error;
	}

	private async read<T>(fn: (view: StateView) => T): Promise<T> {
		this.assertUsable();
		return this.store.read(fn);
	}

	// ============================================================ Storage reads (§4.5)

	async mintId<I extends Id<string>>(): Promise<I> {
		this.assertUsable();
		if (!Number.isSafeInteger(this.nextId)) throw new Error("ID space is exhausted");
		return this.nextId++ as I;
	}

	conversation(id: ConversationId, _context: Context): Promise<ConversationRecord | undefined> {
		return this.read((v) => pi.conversation(v, id));
	}

	scanConversations(query: ConversationQuery, limit: number, cursor: Cursor | undefined, _context: Context): Promise<Page<ConversationRecord, Cursor>> {
		return this.read((v) => pi.scanConversations(v, query, limit, cursor));
	}

	entry(id: EntryId, context: Context): Promise<pi.EntryResult>;
	entry(conversationId: ConversationId, id: EntryId, context: Context): Promise<pi.EntryResult>;
	async entry(a: number, b: number | Context, c?: Context): Promise<pi.EntryResult> {
		this.assertUsable();
		if (c === undefined) return this.read((v) => pi.entryById(v, a as EntryId));
		if (typeof b !== "number") throw new TypeError("Storage.entry() requires an entry ID");
		return this.read((v) => pi.entryInConversation(v, a as ConversationId, b as EntryId));
	}

	findLatestHeadMarker(
		conversationId: ConversationId,
		atOrBeforeEntryId: EntryId | undefined,
		_context: Context,
	): Promise<(EntryRecord & { readonly head: EntryId }) | undefined> {
		return this.read((v) => pi.findLatestHeadMarker(v, conversationId, atOrBeforeEntryId));
	}

	scanEntries(query: EntryQuery, limit: number, cursor: Cursor | undefined, _context: Context): Promise<Page<EntryRecord, Cursor>> {
		return this.read((v) => pi.scanEntries(v, query, limit, cursor));
	}

	task(id: TaskId, _context: Context): Promise<pi.StoredTask | undefined> {
		return this.read((v) => pi.task(v, id));
	}

	scanTasks(query: TaskQuery, limit: number, cursor: Cursor | undefined, _context: Context): Promise<Page<pi.StoredTask, Cursor>> {
		return this.read((v) => pi.scanTasks(v, query, limit, cursor));
	}

	submission(id: SubmissionId, _context: Context): Promise<SubmissionRecord | undefined> {
		return this.read((v) => pi.submission(v, id));
	}

	scanSubmissions(query: SubmissionQuery, limit: number, cursor: Cursor | undefined, _context: Context): Promise<Page<SubmissionRecord, Cursor>> {
		return this.read((v) => pi.scanSubmissions(v, query, limit, cursor));
	}

	submissionByRequest(conversationId: ConversationId, requestId: string, _context: Context): Promise<SubmissionRecord | undefined> {
		return this.read((v) => pi.submissionByRequest(v, conversationId, requestId));
	}

	findDocument(address: DocumentAddress, at: DocumentPoint, _context: Context): Promise<DocumentRecord | undefined> {
		return this.read((v) => pi.findDocument(v, address, at));
	}

	document(id: DocumentId, at: DocumentPoint, _context: Context): Promise<StoredDocument | undefined> {
		return this.read((v) => pi.document(v, id, at));
	}

	scanDocuments(query: DocumentQuery, limit: number, cursor: Cursor | undefined, _context: Context): Promise<Page<DocumentRecord, Cursor>> {
		return this.read((v) => pi.scanDocuments(v, query, limit, cursor));
	}
}

const asError = (e: unknown): Error => (e instanceof Error ? e : new Error(String(e)));

// ================================================================ open helpers

/** Replay records `[store.tail, tail)` into `store` until the log reports up to date. */
async function replay(
	log: LogTransport,
	store: FullResidentStateStore,
	p7: boolean,
	clock: Clock,
	timing: Timing,
	deadline: number,
): Promise<ReplayedRecord[]> {
	const out: ReplayedRecord[] = [];
	for (;;) {
		const from = store.tail;
		const page = await retryIdempotent(
			clock,
			timing,
			deadline,
			() => log.readRecords(from, p7 ? { maxBytes: timing.replayPageBytes } : { maxRecords: timing.replayPageRecords }),
			(last) => new Error(`UrsulaStorage open: replay from record ${from} did not succeed before the deadline: ${last}`),
		);
		if (page.status !== 200 && page.status !== 204) throw new Error(`UrsulaStorage open: replay from record ${from} failed with ${describe(page)}`);
		out.push(...applyPage(store, page));
		if (page.records.length === 0 || page.headers[H.upToDate] === "true") return out;
	}
}

function applyPage(store: FullResidentStateStore, page: ReadRecordsOutcome): ReplayedRecord[] {
	const start = intHeader(page.headers, H.recordStart) ?? store.tail;
	if (page.records.length > 0 && start !== store.tail) {
		throw new Error(`UrsulaStorage: log page starts at ${start}, expected ${store.tail}`);
	}
	const out: ReplayedRecord[] = [];
	for (const bytes of page.records) {
		const ordinal = store.tail;
		let ops: KeyedOp[];
		try {
			ops = parseKeyedBatch(fromUtf8(bytes));
		} catch (error) {
			throw new Error(`UrsulaStorage: record ${ordinal} is not a valid keyed batch`, { cause: error });
		}
		store.apply(ordinal, ops);
		out.push({ ordinal, bytes, ops });
	}
	return out;
}

/** The claim loop (§3.6 step 6, §7.8). Applies the claim to `store` on success. */
async function claim(
	options: UrsulaStorageOptions,
	mode: OpenMode,
	log: LogTransport,
	store: FullResidentStateStore,
	p7: boolean,
	clock: Clock,
	timing: Timing,
): Promise<OwnerClaim> {
	const nonce = randomBytes(16).toString("hex");
	const openedAt = Date.now();
	const deadline = clock.now() + timing.claimDeadlineMs;
	const timeout = (last: string): Error => new ClaimTimeout(`the claim did not resolve within ${timing.claimDeadlineMs} ms: ${last}`);
	const backoff = new Backoff(clock, timing);
	for (;;) {
		if (clock.now() > deadline) throw timeout("deadline");
		const N = store.tail;
		const owner: OwnerClaim = {
			epoch: N,
			nonce,
			host: options.host ?? hostname(),
			pid: options.pid ?? process.pid,
			opened_at_ms: openedAt,
			mode,
		};
		const plan = planClaim(N, owner);
		let outcome: HttpOutcome | undefined;
		try {
			outcome = await log.append(plan.bytes, N);
		} catch (error) {
			if (!(error instanceof TransportError)) throw error;
			await backoff.wait(deadline);
		}
		if (outcome !== undefined) {
			const s = outcome.status;
			if (is2xx(s)) {
				if (intHeader(outcome.headers, H.recordStart) !== N) throw new Error(`invariant violation: claim matched at ${N} but landed elsewhere`);
				store.apply(N, plan.ops);
				return owner;
			}
			if (s >= 500 || (s === 429 && retryAfterMs(outcome.headers) !== undefined)) {
				await backoff.wait(deadline, retryAfterMs(outcome.headers));
			} else if (s !== 412) {
				throw new Error(`UrsulaStorage open: claim append failed with ${describe(outcome)}`);
			}
		}
		// Resolve: our landed attempt, a resend at the same tail, or foreign records to replay.
		const page = await retryIdempotent(
			clock,
			timing,
			deadline,
			() => log.readRecords(N, { leader: true, ...(p7 ? { maxBytes: timing.replayPageBytes } : { maxRecords: timing.replayPageRecords }) }),
			timeout,
		);
		if (page.status === 400) continue; // a lagging node (tail < N): retry
		if (page.status !== 200 && page.status !== 204) throw new Error(`UrsulaStorage open: claim read-back failed with ${describe(page)}`);
		const first = page.records[0];
		if (first === undefined) continue; // absent and tail = N: resend
		if (bytesEqual(first, plan.bytes)) {
			store.apply(N, plan.ops);
			return owner;
		}
		for (const r of applyPage(store, page)) {
			if (isClaim(r.ops)) throw new OwnershipContention(`another owner claimed the log at record ${r.ordinal}`);
			if (mode === "fail-if-active") throw new OwnershipActive(`the current owner wrote record ${r.ordinal} during open`);
		}
	}
}

// UrsulaStorage: Pi's Storage on one keyed Ursula stream (design §3.3, §3.5–§3.7, §7.2–§7.9).
//
// Two state stores sit behind the StateStore contract:
// - bounded (M3, the default when the node serves keyed-state): a LocalStore holding the overlay
//   `[E, tail)` and a range cache of `state(tail)`. Open reads `m/` through keyed-state, replays
//   `[D, N0)` into the overlay, preloads the live set, then claims (§3.6). Misses read keyed-state
//   at `min_through_record = E` (§3.5). A background flush loop raises `E` (§7.9).
// - full-resident (M1; kept for tests and for nodes without `keyed-state-v1`): every record from 0
//   replayed into one materialized map.
// The commit path, the stateful outcome policy with read-back, the claim loop, close markers, and
// poison semantics are shared.
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
import { preload, widenFetch } from "./bounded.ts";
import { FlushLoop } from "./flush.ts";
import { LocalStore, OVERLAY_ALERT_BYTES } from "./local-store/index.ts";
import { OwnerMetrics, type OwnerMetricsSnapshot } from "./metrics.ts";
import { type OwnerClaim, type PlannedCommit, planClaim, planCloseMarker, planCommit } from "./planner.ts";
import { EXT_KEYED_BATCH, EXT_KEYED_STATE, extensionTokens, H, intHeader, retryAfterMs } from "./protocol.ts";
import { FullResidentStateStore, type StateStore, type StateView } from "./state-store.ts";
import {
	type HttpOutcome,
	type KeyedScanOutcome,
	type KeyedStateTransport,
	type LogTransport,
	type ReadRecordsOutcome,
	TransportError,
} from "./transport.ts";
import { b64, strinc } from "./tuple.ts";

export type OpenMode = "fence" | "fail-if-active";

/**
 * Time source of every deadline and backoff sleep of the owner. Tests inject a virtual clock so that
 * retries never sleep on the wall clock.
 */
export interface Clock {
	now(): number;
	/** Resolve after `ms`, or as soon as `signal` aborts (the timer is then released). */
	sleep(ms: number, signal?: AbortSignal): Promise<void>;
}

export const systemClock: Clock = {
	now: () => Date.now(),
	sleep: (ms, signal) =>
		new Promise((resolve) => {
			if (signal?.aborted === true) return resolve();
			const timer = setTimeout(done, ms);
			function done(): void {
				clearTimeout(timer);
				signal?.removeEventListener("abort", done);
				resolve();
			}
			signal?.addEventListener("abort", done, { once: true });
		}),
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
	/** Deadline of one Session-line keyed-state read before poisoning (§7.6). */
	readonly readDeadlineMs: number;
	/** `timeout_ms` of keyed-state waits: open's `m/` read and flush-waits (§3.6 step 2, §7.9). */
	readonly keyedWaitMs: number;
	/** Open's `m/` read asks for `D ≥ N0 − openLagRecords` (§3.6 step 2). */
	readonly openLagRecords: number;
	/** Replay at open is discarded and `D` re-read once it exceeds this many bytes (§3.6 step 3). */
	readonly openReplayCapBytes: number;
	/** Flush-wait triggers (§7.9): overlay bytes, overlay records, oldest record age. */
	readonly flushMaxBytes: number;
	readonly flushMaxRecords: number;
	readonly flushMaxAgeMs: number;
	/** Backoff cap of flush-wait retries (§7.6). */
	readonly flushBackoffMaxMs: number;
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
	readDeadlineMs: 30_000,
	keyedWaitMs: 60_000,
	openLagRecords: 50_000,
	openReplayCapBytes: 64 * 1024 * 1024,
	flushMaxBytes: 4 * 1024 * 1024,
	flushMaxRecords: 5000,
	flushMaxAgeMs: 10 * 60_000,
	flushBackoffMaxMs: 30_000,
};

/**
 * Which state store backs the owner. `auto` (default) is `bounded` when a keyed-state transport is
 * given and the node advertises `keyed-state-v1` for the stream, otherwise `full-resident`.
 */
export type StateStoreKind = "auto" | "bounded" | "full-resident";

export type OwnerAlert =
	/** A first-attempt 400/413/422 showed that a pre-check missed a server limit. */
	| { readonly kind: "server-limit"; readonly status: number; readonly message: string }
	/** The overlay (pinned records `[E, tail)`) grew past the alert size (§7.5: 64 MiB): keyed-state lags. */
	| { readonly kind: "overlay-size"; readonly bytes: number; readonly thresholdBytes: number };

export interface UrsulaStorageOptions {
	readonly log: LogTransport;
	/** Keyed-state resource. Required by the bounded store; the full-resident store uses it only to finalize on close. */
	readonly keyedState?: KeyedStateTransport;
	/** State store (default `auto`). `bounded` refuses to open unless the node serves keyed-state. */
	readonly stateStore?: StateStoreKind;
	/** Bounded store: cache budget in bytes (§7.5, default 64 MiB). */
	readonly cacheBudgetBytes?: number;
	/** Bounded store: overlay hard cap in bytes (§7.5, default 256 MiB). */
	readonly overlayCapBytes?: number;
	/** Bounded store: overlay size that raises an `overlay-size` alert (§7.5, default 64 MiB). */
	readonly overlayAlertBytes?: number;
	/**
	 * Owner metrics sink (§7.6). Share one across a host's opens to aggregate them; open-time events
	 * (contention, refusals) are counted here too. Default: a fresh sink per open.
	 */
	readonly metrics?: OwnerMetrics;
	/** Bounded store: `limit` of keyed-state range fetches (default 256). */
	readonly pageLimit?: number;
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
	/** Called on owner alerts: a missed server limit, or the overlay past its alert size. */
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
	private readonly local: LocalStore | undefined;
	private readonly flush: FlushLoop | undefined;
	private readonly timing: Timing;
	private readonly clock: Clock;
	private readonly onAlert: ((alert: OwnerAlert) => void) | undefined;
	private readonly p7: boolean;
	private readonly owner: OwnerClaim;
	private nextId: number;
	private persistedNextId: number;
	private chain: Promise<unknown> = Promise.resolve();
	/** The landing (append + apply) of the commit in flight, if any (§3.5 step 3). */
	private landing: Promise<void> | undefined;
	private closing: Promise<void> | undefined;
	private poisoned: Error | undefined;
	private readonly ownerMetrics: OwnerMetrics;
	private readonly countPoison: (error: Error) => void;

	private constructor(init: {
		options: UrsulaStorageOptions;
		timing: Timing;
		clock: Clock;
		store: StateStore;
		local: LocalStore | undefined;
		owner: OwnerClaim;
		p7: boolean;
		nextId: number;
		persistedNextId: number;
		hooks: OpenHooks;
		probe: OpenProbe;
	}) {
		this.log = init.options.log;
		this.keyedState = init.options.keyedState;
		this.onAlert = init.options.onAlert;
		this.timing = init.timing;
		this.clock = init.clock;
		this.store = init.store;
		this.local = init.local;
		this.owner = init.owner;
		this.p7 = init.p7;
		this.nextId = init.nextId;
		this.persistedNextId = init.persistedNextId;
		this.ownerMetrics = init.probe.metrics;
		this.countPoison = init.probe.countPoison;
		// From here on, Storage reads that miss the cache are Session-line remote reads.
		init.probe.line = true;
		// After the claim, a page above the tail waits for the in-flight commit; with none, another
		// writer exists and the store poisons with FencedError (§3.5 step 3, I19).
		init.hooks.ahead = () => this.landing;
		const local = init.local;
		const keyedState = init.options.keyedState;
		if (local !== undefined && keyedState !== undefined) {
			this.flush = new FlushLoop({
				local,
				keyedState,
				owner: init.owner,
				timing: { ...init.timing, flushWaitTimeoutMs: init.timing.keyedWaitMs },
				now: () => init.clock.now(),
				sleep: (ms, signal) => init.clock.sleep(ms, signal),
				inflight: () => this.landing,
				poison: (error) => {
					this.poisonWith(error);
				},
			});
		}
	}

	/** The owner's epoch: the ordinal of its claim record. */
	get epoch(): number {
		return this.owner.epoch;
	}
	/** Exclusive applied tail. */
	get tail(): number {
		return this.store.tail;
	}
	/** The error that poisoned this storage, if any (including the bounded store's). */
	get poison(): Error | undefined {
		return this.poisoned ?? this.local?.poisoned;
	}
	/** The bounded store, when this owner uses one (metrics and tests). */
	get localStore(): LocalStore | undefined {
		return this.local;
	}
	/** Flush loop metrics, when this owner uses the bounded store. */
	get flushMetrics(): FlushLoop["metrics"] | undefined {
		return this.flush?.metrics;
	}

	/** Owner metrics (§7.6): the counters of this owner's sink plus its gauges. */
	metrics(): OwnerMetricsSnapshot {
		const m = this.ownerMetrics;
		const local = this.local;
		return {
			sessionLineRemoteReads: m.sessionLineRemoteReads,
			openRemoteReads: m.openRemoteReads,
			remoteReadRetries: m.remoteReadRetries,
			remoteReadLatency: m.remoteReadLatency.snapshot(),
			poisons: m.poisons,
			fences: m.fences,
			contentions: m.contentions,
			activeRefusals: m.activeRefusals,
			claimTimeouts: m.claimTimeouts,
			overlayAlerts: m.overlayAlerts,
			overlayCapWaits: m.overlayCapWaits,
			pinnedBytes: local?.overlayBytes ?? 0,
			pinnedRecords: local?.overlayRecords ?? 0,
			cacheBytes: local?.cacheBytes ?? 0,
			overlayFloor: local?.overlayFloor ?? 0,
			tail: this.store.tail,
			flushWaits: this.flush?.metrics.flushWaits ?? 0,
			flushRetries: this.flush?.metrics.retries ?? 0,
			floorsRaised: this.flush?.metrics.floorsRaised ?? 0,
		};
	}

	// ============================================================ open (§3.6)

	static async open(options: UrsulaStorageOptions): Promise<UrsulaStorage> {
		const metrics = options.metrics ?? new OwnerMetrics();
		let counted = false;
		const probe: OpenProbe = {
			metrics,
			line: false,
			countPoison: (error) => {
				if (counted) return;
				counted = true;
				metrics.poisons++;
				if (error instanceof FencedError) metrics.fences++;
			},
		};
		try {
			return await UrsulaStorage.openWith(options, probe);
		} catch (error) {
			if (error instanceof OwnershipContention) metrics.contentions++;
			else if (error instanceof OwnershipActive) metrics.activeRefusals++;
			else if (error instanceof ClaimTimeout) metrics.claimTimeouts++;
			throw error;
		}
	}

	private static async openWith(options: UrsulaStorageOptions, probe: OpenProbe): Promise<UrsulaStorage> {
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
		const kind = options.stateStore ?? "auto";
		if ((options.requireKeyedState === true || kind === "bounded") && !tokens.has(EXT_KEYED_STATE)) {
			throw new OpenRefused(`the node does not advertise ${EXT_KEYED_STATE} for this stream`);
		}
		if (kind === "bounded" && options.keyedState === undefined) throw new OpenRefused("the bounded store needs a keyed-state transport");
		const p7 = tokens.has(EXT_KEYED_STATE);
		const n0 = intHeader(head.headers, H.recordNext);
		if (n0 === undefined) throw new OpenRefused("HEAD did not return Stream-Record-Next");
		const bounded = kind === "bounded" || (kind === "auto" && p7 && options.keyedState !== undefined);

		// Steps 2–4: state at the tail, and whether the current owner wrote during open.
		const hooks: OpenHooks = { ahead: () => undefined };
		let store: StateStore;
		let local: LocalStore | undefined;
		let activity: boolean;
		if (bounded) {
			const opened = await openBounded(options, mode, log, options.keyedState as KeyedStateTransport, n0, p7, clock, timing, deadline, hooks, probe);
			store = opened.store;
			local = opened.store;
			activity = opened.activity;
		} else {
			const full = new FullResidentStateStore();
			await replay(log, full, p7, clock, timing, deadline);
			if (full.tail < n0) throw new Error(`UrsulaStorage open: replay ended at ${full.tail} below HEAD's ${n0}`);
			store = full;
			activity = full.tail > n0;
		}
		try {
			const format = await store.read((v) => pi.meta<{ pi_durable_keyed?: number; tuple?: number }>(v, META.format));
			if (store.tail > 0 && format === undefined) throw new OpenRefused("the stream is not a Pi Durable keyed log (no m/format)");
			if (format !== undefined && ((format.pi_durable_keyed ?? 0) > FORMAT_VALUE.pi_durable_keyed || (format.tuple ?? 0) > FORMAT_VALUE.tuple)) {
				throw new OpenRefused(`the stream's format ${JSON.stringify(format)} is newer than this owner's`);
			}

			// Step 5: mode check. Nothing has been written yet.
			if (mode === "fail-if-active") {
				if (activity) throw new OwnershipActive(`records beyond ${n0} appeared during open: the current owner is writing`);
				const current = await store.read((v) => pi.meta<OwnerClaim>(v, META.owner));
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
			// From here on a page above the tail means another writer (until commits start, §3.5 step 3).
			hooks.ahead = () => undefined;
			// Step 7: nextId and the fresh floor F_fresh := m/next_id (§7.3).
			const nextIdRow = await store.read((v) => pi.meta<number>(v, META.nextId));
			local?.startFresh(nextIdRow ?? 2);
			return new UrsulaStorage({
				options,
				timing,
				clock,
				store,
				local,
				owner,
				p7,
				nextId: nextIdRow ?? 2,
				persistedNextId: nextIdRow ?? 0,
				hooks,
				probe,
			});
		} catch (error) {
			store.close();
			throw error;
		}
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
		await this.waitForOverlayRoom();
		const seq = this.store.tail;
		const plan = await this.store.read((view) =>
			planCommit(view, writes, { seq, epoch: this.owner.epoch, nextId: this.nextId, persistedNextId: this.persistedNextId }),
		);
		this.assertNotPoisoned();
		const landing = this.appendWithPolicy(plan).then(() => {
			this.store.apply(seq, plan.ops);
			this.nextId = Math.max(this.nextId, plan.nextId);
			this.persistedNextId = plan.persistedNextId;
			this.flush?.noteApplied(seq);
		});
		this.landing = landing.catch(() => undefined);
		try {
			await landing;
		} finally {
			this.landing = undefined;
		}
		return seq as Seq;
	}

	/** At the overlay hard cap, wait for the flush loop until the commit deadline, then poison (§7.5). */
	private async waitForOverlayRoom(): Promise<void> {
		const local = this.local;
		const flush = this.flush;
		if (local === undefined || flush === undefined || !local.overlayAtCap) return;
		const deadline = this.clock.now() + this.timing.commitDeadlineMs;
		this.ownerMetrics.overlayCapWaits++;
		while (local.overlayAtCap) {
			this.assertNotPoisoned();
			const left = deadline - this.clock.now();
			if (left <= 0) {
				throw this.poisonWith(new Error(`the overlay reached its ${local.overlayBytes}-byte cap and keyed-state did not catch up within ${this.timing.commitDeadlineMs} ms`));
			}
			await Promise.race([flush.urge(), this.clock.sleep(Math.min(left, this.timing.backoffMaxMs * 10))]);
		}
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
		if (this.poison === undefined) {
			const marker = this.appendCloseMarker().catch(() => undefined);
			this.landing = marker;
			try {
				// The next fail-if-active open pays W once when this fails.
				await marker;
			} finally {
				this.landing = undefined;
			}
		}
		if (this.keyedState !== undefined && this.poison === undefined) {
			try {
				// Finalize-on-close (§3.7): trigger ingestion up to the tail without waiting for it.
				await this.keyedState.scan({ key: b64(K.m(META.owner)), minThroughRecord: this.store.tail, timeoutMs: 1 });
			} catch {
				// Best effort.
			}
		}
		this.flush?.stop();
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
		const p = this.poison;
		if (p !== undefined) {
			const message = `UrsulaStorage is poisoned: ${p.message}`;
			throw p instanceof FencedError ? new FencedError(message, { cause: p }) : new Error(message, { cause: p });
		}
	}

	private poisonWith(error: Error): Error {
		if (this.poisoned === undefined) {
			this.poisoned = error;
			this.countPoison(error);
		}
		this.flush?.stop();
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

/**
 * What a LocalStore does with a keyed-state page above its tail. Before the claim (§3.6 step 4): in
 * `fence` mode, replay the log further; in `fail-if-active` mode, refuse with OwnershipActive. After
 * the claim: wait for the in-flight commit, or (undefined) poison with FencedError.
 */
interface OpenHooks {
	ahead: () => Promise<void> | undefined;
}

/** Metrics plumbing of one open: the sink, whether reads are on the Session line yet, and the poison counter. */
interface OpenProbe {
	readonly metrics: OwnerMetrics;
	/** False during open, true once the storage exists. */
	line: boolean;
	readonly countPoison: (error: Error) => void;
}

/** `m/` and `strinc(m/)`: the metadata family read at open (§3.6 step 2). */
const META_LO = K.m("").slice(0, 1);
const META_HI = strinc(META_LO);

/** Read the parsed records `[from, up to date)`, giving up (undefined) past `capBytes`. */
async function readLog(
	log: LogTransport,
	from: number,
	p7: boolean,
	clock: Clock,
	timing: Timing,
	deadline: number,
	capBytes: number,
): Promise<ReplayedRecord[] | undefined> {
	const out: ReplayedRecord[] = [];
	let next = from;
	let bytes = 0;
	for (;;) {
		const at = next;
		const page = await retryIdempotent(
			clock,
			timing,
			deadline,
			() => log.readRecords(at, p7 ? { maxBytes: timing.replayPageBytes } : { maxRecords: timing.replayPageRecords }),
			(last) => new Error(`UrsulaStorage open: replay from record ${at} did not succeed before the deadline: ${last}`),
		);
		if (page.status !== 200 && page.status !== 204) throw new Error(`UrsulaStorage open: replay from record ${at} failed with ${describe(page)}`);
		const start = intHeader(page.headers, H.recordStart) ?? at;
		if (page.records.length > 0 && start !== at) throw new Error(`UrsulaStorage: log page starts at ${start}, expected ${at}`);
		for (const recordBytes of page.records) {
			let ops: KeyedOp[];
			try {
				ops = parseKeyedBatch(fromUtf8(recordBytes));
			} catch (error) {
				throw new Error(`UrsulaStorage: record ${next} is not a valid keyed batch`, { cause: error });
			}
			out.push({ ordinal: next, bytes: recordBytes, ops });
			bytes += recordBytes.length;
			next++;
		}
		if (bytes > capBytes) return undefined;
		if (page.records.length === 0 || page.headers[H.upToDate] === "true") return out;
	}
}

/**
 * Open's `m/` read (§3.6 step 2): `GET keyed-state?start=m/&end=strinc(m/)&min_through_record=r`.
 * A 204 means keyed-state lags: retry until the open deadline, then fail with a retryable error.
 */
async function readMeta(keyedState: KeyedStateTransport, r: number, clock: Clock, timing: Timing, deadline: number): Promise<KeyedScanOutcome> {
	const backoff = new Backoff(clock, timing);
	let last = "no attempt";
	for (;;) {
		if (clock.now() > deadline) throw new Error(`UrsulaStorage open: keyed-state lag: no state at or above record ${r} before the deadline (${last})`);
		let outcome: KeyedScanOutcome | undefined;
		try {
			const wait = Math.max(1, Math.min(timing.keyedWaitMs, deadline - clock.now()));
			outcome = await keyedState.scan({ start: b64(META_LO), end: b64(META_HI), limit: 100, minThroughRecord: r, timeoutMs: wait });
		} catch (error) {
			if (!(error instanceof TransportError)) throw error;
			last = error.message;
		}
		if (outcome !== undefined) {
			const s = outcome.status;
			if (s === 200 && outcome.through !== undefined) return outcome;
			if (isAuth(s)) throw new Error(`UrsulaStorage open: keyed-state was not authorized: ${describe(outcome)}; refresh credentials and reopen`);
			if (s === 404) throw new OpenRefused(`keyed-state is not served for this stream: ${describe(outcome)}`);
			const lagging = s === 400 && (intHeader(outcome.headers, H.recordNext) ?? r) < r;
			if (s === 400 && !lagging) throw new Error(`UrsulaStorage open: keyed-state answered ${describe(outcome)}`);
			last = describe(outcome);
			if (s === 204) continue; // the server already waited
			await backoff.wait(deadline, retryAfterMs(outcome.headers));
			continue;
		}
		await backoff.wait(deadline);
	}
}

/**
 * Bounded open, steps 2–4 (§3.6): read `m/` at `D ≥ N0 − 50000`, replay `[D, N0)` into the overlay
 * (`E := D`), merge the `m/` page, and preload. `activity` reports records beyond `N0`.
 */
async function openBounded(
	options: UrsulaStorageOptions,
	mode: OpenMode,
	log: LogTransport,
	keyedState: KeyedStateTransport,
	n0: number,
	p7: boolean,
	clock: Clock,
	timing: Timing,
	deadline: number,
	hooks: OpenHooks,
	probe: OpenProbe,
): Promise<{ store: LocalStore; activity: boolean }> {
	let minThrough = Math.max(0, n0 - timing.openLagRecords);
	for (;;) {
		const meta = n0 > 0 ? await readMeta(keyedState, minThrough, clock, timing, deadline) : undefined;
		const d = meta?.through ?? 0;
		const records = await readLog(log, d, p7, clock, timing, deadline, timing.openReplayCapBytes);
		if (records === undefined) {
			// The replay outgrew its cap: ask keyed-state for a higher D (§3.6 step 3).
			minThrough = Math.max(minThrough + 1, n0);
			continue;
		}
		let activity = d > n0;
		const store: LocalStore = new LocalStore({
			keyedState,
			base: d,
			...(options.cacheBudgetBytes === undefined ? {} : { cacheBudgetBytes: options.cacheBudgetBytes }),
			...(options.overlayCapBytes === undefined ? {} : { overlayCapBytes: options.overlayCapBytes }),
			...(options.pageLimit === undefined ? {} : { pageLimit: options.pageLimit }),
			...(options.overlayAlertBytes === undefined ? {} : { overlayAlertBytes: options.overlayAlertBytes }),
			onOverlayAlert: (bytes) => {
				probe.metrics.overlayAlerts++;
				options.onAlert?.({ kind: "overlay-size", bytes, thresholdBytes: options.overlayAlertBytes ?? OVERLAY_ALERT_BYTES });
			},
			onRemoteRead: (latencyMs, transient) => {
				const m = probe.metrics;
				m.remoteReadLatency.record(latencyMs);
				if (probe.line) m.sessionLineRemoteReads++;
				else m.openRemoteReads++;
				if (transient) m.remoteReadRetries++;
			},
			onPoison: (error) => probe.countPoison(error),
			commitInFlight: () => hooks.ahead(),
			readDeadlineMs: timing.readDeadlineMs,
			now: () => clock.now(),
			backoff: (attempt, retryAfter) =>
				clock.sleep(retryAfter ?? Math.min(timing.backoffMaxMs, timing.backoffBaseMs * 2 ** Math.min(attempt, 30)) * (0.5 + Math.random() / 2)),
			widen: widenFetch,
		});
		try {
			for (const r of records) store.apply(r.ordinal, r.ops);
			activity ||= store.tail > n0;
			// Before the claim, a page above the replayed tail means the current owner is writing.
			let replaying: Promise<void> | undefined;
			hooks.ahead = () => {
				activity = true;
				if (mode === "fail-if-active") throw new OwnershipActive(`keyed-state reflects records beyond ${n0}: the current owner is writing`);
				replaying ??= replay(log, store, p7, clock, timing, deadline).then(
					() => {
						replaying = undefined;
					},
					(error: unknown) => {
						replaying = undefined;
						throw error;
					},
				);
				return replaying;
			};
			if (meta !== undefined) {
				// E = D and D ≤ the replayed tail, so the page is neither stale nor ahead.
				const merged = store.mergePage(META_LO, META_HI, meta);
				if (merged !== "merged") throw new Error(`UrsulaStorage open: the m/ page at ${String(meta.through)} could not be merged (${merged})`);
			}
			// A new log (state(0) plus the claim to come) has nothing to preload.
			if (store.tail > 0) await preload(store);
			return { store, activity };
		} catch (error) {
			store.close();
			throw error;
		}
	}
}

/** Replay records `[store.tail, tail)` into `store` until the log reports up to date. */
async function replay(
	log: LogTransport,
	store: StateStore,
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

function applyPage(store: StateStore, page: ReadRecordsOutcome): ReplayedRecord[] {
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
	store: StateStore,
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

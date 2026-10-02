// LocalStore: the bounded owner's StateStore (design §7.2–§7.5, read path §3.5).
//
// State held:
// - the overlay: parsed op lists of the confirmed records `[E, tail)` (the pinned set);
// - the cache: one ordered map of visible rows of `state(tail)`, exact on
//   (a) a set of disjoint explicit ranges and (b) the fresh set of complete-at-mint (§7.3);
//   every cache row lies in (a) or (b), so nothing outside them leaks into a read.
//
// Invariants (checked by the M0e model test): for every explicit range R, `cache|R = state(tail)|R`;
// for the fresh set S, `cache|S = state(tail)|S`; the overlay holds exactly the records `[E, tail)`.
//
// A read pass that reaches a key or range outside (a) ∪ (b) throws an internal miss; `read` then
// fetches that gap from keyed-state with `min_through_record = E`, merges the page synchronously on
// arrival (§3.5 step 3) and re-runs the pass. Ranges a read has touched or installed stay pinned
// until the read settles, so eviction between awaits cannot livelock it (§7.2 "Single-state reads").
import { FencedError } from "../errors.ts";
import type { KeyedOp } from "../keyed-batch.ts";
import { H, intHeader } from "../protocol.ts";
import type { StateStore, StateView } from "../state-store.ts";
import type { KeyedScanOutcome, KeyedScanRequest, KeyedStateTransport } from "../transport.ts";
import { TransportError } from "../transport.ts";
import { b64, keySuccessor, unb64 } from "../tuple.ts";
import { isFreshKey, isFreshRange } from "./fresh.ts";
import { SkipList } from "./skip-list.ts";

/** A visible row of the cache. */
export interface CachedRow {
	readonly record: number;
	readonly value: string;
}

/** Accounting overhead per cached row or overlay op, on top of key and value octets. */
export const ROW_OVERHEAD = 64;
const rowBytes = (key: string, value: string): number => key.length + value.length + ROW_OVERHEAD;
const opBytes = (op: KeyedOp): number =>
	ROW_OVERHEAD + (op.op === "p" ? op.key.length + op.value.length : op.op === "d" ? op.key.length : op.start.length + op.end.length);

/** An explicit covered range `[lo, hi)`; `hi` undefined means unbounded. */
interface CoveredRange {
	readonly lo: string;
	readonly hi: string | undefined;
	/** Number of reads holding this range pinned. */
	pins: number;
}

interface OverlayRecord {
	readonly ordinal: number;
	readonly ops: readonly KeyedOp[];
	readonly bytes: number;
}

/** Thrown from a view to abort a pass that reached `[lo, hi)` outside every covered region. */
class CacheMiss extends Error {
	readonly store: LocalStore;
	readonly lo: string;
	readonly hi: string;
	readonly point: boolean;
	constructor(store: LocalStore, lo: string, hi: string, point: boolean) {
		super("LocalStore: range not covered (internal; read passes must not catch this)");
		this.store = store;
		this.lo = lo;
		this.hi = hi;
		this.point = point;
	}
}

export interface LocalStoreOptions {
	readonly keyedState: KeyedStateTransport;
	/** Initial `E = tail`: records below it are only reachable through keyed-state. Default 0. */
	readonly base?: number;
	/** Initial `F_fresh` (§7.3): `m/next_id` at open. Default: no fresh coverage. */
	readonly freshFloor?: number;
	/** Cache budget in bytes (§7.5). Default 64 MiB. */
	readonly cacheBudgetBytes?: number;
	/** Overlay hard cap in bytes (§7.5). Default 256 MiB; `overlayAtCap` reports it. */
	readonly overlayCapBytes?: number;
	/** `limit` of range fetches (1..1000). Default 256. */
	readonly pageLimit?: number;
	/**
	 * Called when a page reflects records above `tail`. Returns a promise that settles once the
	 * in-flight commit has been applied (or failed), or undefined when no commit is in flight, in
	 * which case another writer exists and the store poisons with `FencedError` (§3.5 step 3).
	 */
	readonly commitInFlight?: () => Promise<void> | undefined;
	/** Waits before retry `attempt` (1-based) of a transient fetch failure. Default: capped exponential timer. */
	readonly backoff?: (attempt: number) => Promise<void>;
	/** Transient fetch failures tolerated per fetch before poisoning (§7.6). Default 12. */
	readonly maxRetries?: number;
}

const DEFAULT_BACKOFF = (attempt: number): Promise<void> =>
	new Promise((resolve) => setTimeout(resolve, Math.min(5000, 25 * 2 ** Math.min(attempt, 10))));

export class LocalStore implements StateStore {
	private readonly keyedState: KeyedStateTransport;
	private readonly cache = new SkipList<CachedRow>(0x51ed270b);
	/** Explicit covered ranges keyed by `lo`. */
	private readonly ranges = new SkipList<CoveredRange>(0x2545f491);
	/** LRU order of explicit ranges: Map iteration order, oldest first. */
	private readonly lru = new Map<CoveredRange, true>();
	/** Records `[E, tail)` live at `[overlayHead, length)`; dropped slots are cleared so their ops can be collected. */
	private overlay: (OverlayRecord | undefined)[] = [];
	private overlayHead = 0;
	private floor: number;
	private next: number;
	private fresh: number;
	private cacheBytesTotal = 0;
	private overlayBytesTotal = 0;
	private activeReads = 0;
	private closed = false;
	private poisonError: Error | undefined;
	private readonly options: LocalStoreOptions;
	/** Owner metrics (§7.6): remote reads and how their pages were handled. */
	readonly metrics = { remoteReads: 0, retries: 0, stalePagesDiscarded: 0, commitWaits: 0, pagesMerged: 0, rangesEvicted: 0 };

	constructor(options: LocalStoreOptions) {
		this.options = options;
		this.keyedState = options.keyedState;
		this.floor = options.base ?? 0;
		this.next = this.floor;
		this.fresh = options.freshFloor ?? Number.POSITIVE_INFINITY;
	}

	/** Exclusive: the store reflects records `[0, tail)`. */
	get tail(): number {
		return this.next;
	}

	/** `E`: the overlay holds records `[E, tail)`. */
	get overlayFloor(): number {
		return this.floor;
	}

	/** `F_fresh`. */
	get freshFloor(): number {
		return this.fresh;
	}

	get cacheBytes(): number {
		return this.cacheBytesTotal;
	}

	get overlayBytes(): number {
		return this.overlayBytesTotal;
	}

	get overlayRecords(): number {
		return this.overlay.length - this.overlayHead;
	}

	/** True when the overlay reached its hard cap: new commits must wait for the flush loop (§7.5). */
	get overlayAtCap(): boolean {
		return this.overlayBytesTotal >= (this.options.overlayCapBytes ?? 256 * 1024 * 1024);
	}

	get poisoned(): Error | undefined {
		return this.poisonError;
	}

	// ================================================================ writes

	/** Write-through of the confirmed record at `tail` (§7.2): appended to the overlay, applied to covered regions only. */
	apply(ordinal: number, ops: readonly KeyedOp[]): void {
		this.assertUsable();
		if (ordinal !== this.next) throw new Error(`StateStore apply out of order: ${ordinal} != ${this.next}`);
		let bytes = 0;
		for (const op of ops) {
			bytes += opBytes(op);
			if (op.op === "p") {
				if (this.coveredKey(op.key)) this.putRow(op.key, { record: ordinal, value: op.value });
				else this.deleteRow(op.key);
			} else if (op.op === "d") {
				this.deleteRow(op.key);
			} else {
				this.cache.deleteRange(op.start, op.end, (k, row) => {
					this.cacheBytesTotal -= rowBytes(k, row.value);
				});
			}
		}
		this.overlay.push({ ordinal, ops, bytes });
		this.overlayBytesTotal += bytes;
		this.next = ordinal + 1;
		this.enforceBudget();
	}

	/** A flush-wait returned `D_pub` (§7.2): `E := max(E, min(D_pub, tail))`, dropping the overlay prefix. */
	advanceFloor(published: number): void {
		const target = Math.min(published, this.next);
		if (target <= this.floor) return;
		for (let rec = this.overlay[this.overlayHead]; rec !== undefined && rec.ordinal < target; rec = this.overlay[this.overlayHead]) {
			this.overlayBytesTotal -= rec.bytes;
			this.overlay[this.overlayHead] = undefined;
			this.overlayHead++;
		}
		if (this.overlayHead > 1024 && this.overlayHead * 2 > this.overlay.length) {
			this.overlay = this.overlay.slice(this.overlayHead);
			this.overlayHead = 0;
		}
		this.floor = target;
	}

	// ================================================================ eviction (§7.5)

	/**
	 * Evict until the cache holds at most `maxBytes`: least recently used unpinned explicit ranges
	 * first, then fresh rows oldest-ID first by raising `F_fresh` (only while no read is running,
	 * because fresh coverage cannot be pinned).
	 */
	evict(maxBytes: number): void {
		if (this.closed) return;
		for (const range of this.lru.keys()) {
			if (this.cacheBytesTotal <= maxBytes) return;
			if (range.pins === 0) this.dropRange(range);
		}
		if (this.cacheBytesTotal > maxBytes && this.activeReads === 0) this.evictFresh(maxBytes);
	}

	/**
	 * Raise `F_fresh` to `floor` and drop the rows that were covered only by the old floor (§7.3).
	 * Ignored while a read is running.
	 */
	raiseFreshFloor(floor: number): void {
		if (this.closed || this.activeReads > 0 || !(floor > this.fresh)) return;
		this.fresh = floor;
		const drop: string[] = [];
		for (const [k] of this.cache.entries()) if (!isFreshKey(k, floor) && this.rangeAt(k) === undefined) drop.push(k);
		for (const k of drop) this.deleteRow(k);
	}

	private enforceBudget(): void {
		const budget = this.options.cacheBudgetBytes ?? 64 * 1024 * 1024;
		if (this.cacheBytesTotal > budget) this.evict(budget);
	}

	private evictFresh(maxBytes: number): void {
		if (!Number.isFinite(this.fresh)) return;
		// Each fresh-only row is kept by its largest ID component; raise F_fresh past the oldest ones.
		const ids: { id: number; bytes: number }[] = [];
		for (const [k, row] of this.cache.entries()) {
			if (this.rangeAt(k) !== undefined) continue;
			ids.push({ id: this.keepingId(k), bytes: rowBytes(k, row.value) });
		}
		ids.sort((a, b) => a.id - b.id);
		let total = this.cacheBytesTotal;
		let floor = this.fresh;
		for (const { id, bytes } of ids) {
			if (total <= maxBytes) break;
			floor = Math.max(floor, id + 1);
			total -= bytes;
		}
		this.raiseFreshFloor(floor);
	}

	/** The smallest floor at which `key` stops being fresh, minus one (binary search over the floor). */
	private keepingId(key: string): number {
		let lo = this.fresh;
		let hi = Number.MAX_SAFE_INTEGER;
		if (isFreshKey(key, hi)) return hi;
		while (lo + 1 < hi) {
			const mid = Math.floor(lo / 2 + hi / 2);
			if (isFreshKey(key, mid)) lo = mid;
			else hi = mid;
		}
		return lo;
	}

	private dropRange(range: CoveredRange): void {
		this.metrics.rangesEvicted++;
		this.ranges.delete(range.lo);
		this.lru.delete(range);
		const drop: string[] = [];
		for (const [k] of this.cache.range(range.lo, range.hi)) if (!isFreshKey(k, this.fresh)) drop.push(k);
		for (const k of drop) this.deleteRow(k);
	}

	// ================================================================ reads (§3.5)

	async read<T>(fn: (view: StateView) => T): Promise<T> {
		this.assertUsable();
		const pins = new Set<CoveredRange>();
		this.activeReads++;
		try {
			for (;;) {
				this.assertUsable();
				try {
					return fn(this.view(pins));
				} catch (error) {
					if (!(error instanceof CacheMiss) || error.store !== this) throw error;
					await this.fill(error, pins);
				}
			}
		} finally {
			this.activeReads--;
			for (const r of pins) r.pins--;
		}
	}

	close(): void {
		this.closed = true;
		this.overlay = [];
		this.overlayHead = 0;
		this.overlayBytesTotal = 0;
		this.cache.deleteRange("", undefined);
		this.ranges.deleteRange("", undefined);
		this.lru.clear();
		this.cacheBytesTotal = 0;
	}

	private view(pins: Set<CoveredRange>): StateView {
		const touch = (range: CoveredRange): void => {
			if (!pins.has(range)) {
				pins.add(range);
				range.pins++;
			}
			this.lru.delete(range);
			this.lru.set(range, true);
		};
		const store = this;
		return {
			get: (key) => {
				const range = this.rangeAt(key);
				if (range !== undefined) touch(range);
				else if (!isFreshKey(key, this.fresh)) throw new CacheMiss(this, key, keySuccessor(key), true);
				return this.cache.get(key)?.value;
			},
			scan: function* (start, end) {
				let pos = start;
				while (pos < end) {
					const range = store.rangeAt(pos);
					let segmentEnd: string;
					if (range !== undefined) {
						touch(range);
						segmentEnd = range.hi === undefined || range.hi > end ? end : range.hi;
					} else {
						const nextRange = store.ranges.ceiling(pos);
						segmentEnd = nextRange === undefined || nextRange[0] > end ? end : nextRange[0];
						if (!isFreshRange(pos, segmentEnd, store.fresh)) throw new CacheMiss(store, pos, segmentEnd, false);
					}
					for (const [k, row] of store.cache.range(pos, segmentEnd)) yield [k, row.value] as const;
					pos = segmentEnd;
				}
			},
		};
	}

	/** The explicit range containing `key`. */
	private rangeAt(key: string): CoveredRange | undefined {
		const f = this.ranges.floor(key);
		if (f === undefined) return undefined;
		const range = f[1];
		return range.hi === undefined || key < range.hi ? range : undefined;
	}

	private coveredKey(key: string): boolean {
		return this.rangeAt(key) !== undefined || isFreshKey(key, this.fresh);
	}

	/** Fetch `[miss.lo, miss.hi)` (or the point) until one acceptable page has been merged. */
	private async fill(miss: CacheMiss, pins: Set<CoveredRange>): Promise<void> {
		const limit = this.options.pageLimit ?? 256;
		const request: KeyedScanRequest = miss.point
			? { key: b64(miss.lo) }
			: { start: b64(miss.lo), end: b64(miss.hi), limit };
		let failures = 0;
		for (;;) {
			this.assertUsable();
			let outcome: KeyedScanOutcome | undefined;
			const r = this.floor;
			this.metrics.remoteReads++;
			try {
				outcome = await this.keyedState.scan({ ...request, minThroughRecord: r });
			} catch (error) {
				if (!(error instanceof TransportError)) throw this.poison(error instanceof Error ? error : new Error(String(error)));
			}
			this.assertUsable();
			const s = outcome?.status;
			// Transient (§7.6): no response, 204, 429, 5xx, and a 400 from a lagging node whose
			// Stream-Record-Next is below r (r = E never exceeds the acknowledged tail).
			const lagging = s === 400 && outcome !== undefined && (intHeader(outcome.headers, H.recordNext) ?? r) < r;
			if (outcome === undefined || lagging || s === 204 || s === 429 || (s !== undefined && s >= 500)) {
				failures++;
				this.metrics.retries++;
				if (failures > (this.options.maxRetries ?? 12)) {
					throw this.poison(new Error(`LocalStore: keyed-state read failed after ${failures} attempts (last status ${s ?? "none"})`));
				}
				await (this.options.backoff ?? DEFAULT_BACKOFF)(failures);
				continue;
			}
			if (s !== 200) throw this.poison(new Error(`LocalStore: keyed-state read answered ${s}${outcome.message ? `: ${outcome.message}` : ""}`));
			const through = outcome.through;
			if (through === undefined) throw this.poison(new Error("LocalStore: keyed-state 200 without Stream-Keyed-Through"));
			// §3.5 step 3, evaluated synchronously against the current E and tail.
			for (;;) {
				this.assertUsable();
				if (through < this.floor) {
					// E rose while the request was in flight: the overlay no longer reaches D_resp. Refetch.
					this.metrics.stalePagesDiscarded++;
					break;
				}
				if (through <= this.next) {
					this.metrics.pagesMerged++;
					this.merge(miss, outcome, through, pins);
					return;
				}
				const wait = this.options.commitInFlight?.();
				if (wait === undefined) {
					throw this.poison(new FencedError(`keyed-state reflects record ${through - 1} above the local tail ${this.next}: another writer exists`));
				}
				this.metrics.commitWaits++;
				await wait.catch(() => undefined);
			}
		}
	}

	/** Merge one page at `D_resp` into the cache: `fold(page rows, overlay [D_resp, tail))` over its covered range. */
	private merge(miss: CacheMiss, page: KeyedScanOutcome, through: number, pins: Set<CoveredRange>): void {
		const lo = miss.lo;
		let hi: string = miss.hi;
		if (!miss.point && page.after !== undefined) {
			const after = unb64(page.after);
			if (after === undefined || after < lo || after >= miss.hi) throw this.poison(new Error("LocalStore: Stream-Keyed-After outside the requested range"));
			hi = keySuccessor(after);
		}
		const merged = new SkipList<CachedRow>();
		let prev: string | undefined;
		for (const row of page.rows) {
			const key = unb64(row.key);
			if (key === undefined || key < lo || key >= hi || (prev !== undefined && key <= prev)) {
				throw this.poison(new Error("LocalStore: keyed-state row outside the page's range or out of order"));
			}
			prev = key;
			merged.set(key, { record: row.record, value: row.value });
		}
		for (let i = this.overlayHead + (through - this.floor); i < this.overlay.length; i++) {
			const rec = this.overlay[i];
			if (rec === undefined) throw this.poison(new Error(`LocalStore: overlay record ${through} was dropped (E = ${this.floor})`));
			for (const op of rec.ops) {
				if (op.op === "p") {
					if (op.key >= lo && op.key < hi) merged.set(op.key, { record: rec.ordinal, value: op.value });
				} else if (op.op === "d") {
					merged.delete(op.key);
				} else {
					merged.deleteRange(op.start, op.end);
				}
			}
		}
		// Install into the parts of [lo, hi) that no explicit range covers yet; covered parts already
		// hold the same rows, since both are exact state(tail).
		let pos = lo;
		while (pos < hi) {
			const range = this.rangeAt(pos);
			if (range !== undefined) {
				if (!pins.has(range)) {
					pins.add(range);
					range.pins++;
				}
				if (range.hi === undefined || range.hi >= hi) break;
				pos = range.hi;
				continue;
			}
			const nextRange = this.ranges.ceiling(pos);
			const gapEnd = nextRange === undefined || nextRange[0] > hi ? hi : nextRange[0];
			this.cache.deleteRange(pos, gapEnd, (k, row) => {
				this.cacheBytesTotal -= rowBytes(k, row.value);
			});
			for (const [k, row] of merged.range(pos, gapEnd)) this.putRow(k, row);
			const installed: CoveredRange = { lo: pos, hi: gapEnd, pins: 1 };
			pins.add(installed);
			this.ranges.set(pos, installed);
			this.lru.set(installed, true);
			pos = gapEnd;
		}
		this.enforceBudget();
	}

	// ================================================================ helpers

	private putRow(key: string, row: CachedRow): void {
		const prev = this.cache.set(key, row);
		if (prev !== undefined) this.cacheBytesTotal -= rowBytes(key, prev.value);
		this.cacheBytesTotal += rowBytes(key, row.value);
	}

	private deleteRow(key: string): void {
		const prev = this.cache.delete(key);
		if (prev !== undefined) this.cacheBytesTotal -= rowBytes(key, prev.value);
	}

	private poison(error: Error): Error {
		this.poisonError ??= error;
		return error;
	}

	private assertUsable(): void {
		if (this.poisonError !== undefined) throw this.poisonError;
		if (this.closed) throw new Error("UrsulaStorage is closed");
	}

	// ================================================================ inspection (tests, metrics)

	/** Explicit covered ranges, ascending. */
	coveredRanges(): { lo: string; hi: string | undefined; pinned: boolean }[] {
		return [...this.ranges.entries()].map(([, r]) => ({ lo: r.lo, hi: r.hi, pinned: r.pins > 0 }));
	}

	/** Every cached row, ascending. */
	cachedRows(): [string, CachedRow][] {
		return [...this.cache.entries()];
	}

	/** Ordinals held by the overlay, ascending. */
	overlayOrdinals(): number[] {
		return this.overlay.slice(this.overlayHead).map((r) => r?.ordinal ?? -1);
	}
}

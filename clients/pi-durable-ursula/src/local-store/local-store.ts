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
import { H, intHeader, retryAfterMs } from "../protocol.ts";
import { type StateStore, type StateView, ViewAbort } from "../state-store.ts";
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
/** Fixed per-range charge: the range object plus its skip-list node (§7.5 budget). */
export const RANGE_OVERHEAD = 64;
/** What an explicit covered range costs the cache budget: its bounds plus the fixed overhead. */
const rangeBytes = (r: { lo: string; hi: string | undefined }): number => RANGE_OVERHEAD + r.lo.length + (r.hi?.length ?? 0);
const opBytes = (op: KeyedOp): number =>
	ROW_OVERHEAD + (op.op === "p" ? op.key.length + op.value.length : op.op === "d" ? op.key.length : op.start.length + op.end.length);

/** An explicit covered range `[lo, hi)`; `hi` undefined means unbounded. */
interface CoveredRange {
	/** Mutable only while the range is unpinned and not in `ranges` under its old `lo` (shrink, coalesce). */
	lo: string;
	hi: string | undefined;
	/** Number of reads holding this range pinned. */
	pins: number;
	/** LRU tick of the last read that touched it. */
	used: number;
	/** The last key a read touched inside it: its hot end; eviction shrinks it from the other end (§7.5). */
	hot: string | undefined;
}

/** Values larger than this are evicted before any LRU range (§7.5). */
export const LARGE_VALUE_BYTES = 1024 * 1024;
/** Overlay size that raises the owner alert (§7.5: alert at 64 MiB, hard cap 256 MiB). */
export const OVERLAY_ALERT_BYTES = 64 * 1024 * 1024;

interface OverlayRecord {
	readonly ordinal: number;
	readonly ops: readonly KeyedOp[];
	readonly bytes: number;
}

/** Thrown from a view to abort a pass that reached `[lo, hi)` outside every covered region. */
class CacheMiss extends ViewAbort {
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
	/** Overlay size that calls `onOverlayAlert` (§7.5). Default 64 MiB. */
	readonly overlayAlertBytes?: number;
	/** Called once each time the overlay grows past `overlayAlertBytes`; re-armed when it drops below. */
	readonly onOverlayAlert?: (bytes: number) => void;
	/** Values larger than this many octets are evicted first (§7.5). Default 1 MiB. */
	readonly largeValueBytes?: number;
	/** Called after every keyed-state request of a read (§7.6 owner metrics): its latency, and whether it is retried. */
	readonly onRemoteRead?: (latencyMs: number, transient: boolean) => void;
	/** Called once, when the store poisons. */
	readonly onPoison?: (error: Error) => void;
	/** `limit` of range fetches (1..1000). Default 256. */
	readonly pageLimit?: number;
	/**
	 * Called when a page reflects records above `tail`. Returns a promise that resolves once the
	 * in-flight commit has been applied (or failed), or undefined when no commit is in flight, in
	 * which case another writer exists and the store poisons with `FencedError` (§3.5 step 3). It may
	 * throw, or return a promise that rejects, to abort the read with that error (open uses this
	 * before its claim, §3.6 step 4).
	 */
	readonly commitInFlight?: () => Promise<void> | undefined;
	/**
	 * Waits before retry `attempt` (1-based) of a transient fetch failure; `retryAfterMs` is the
	 * response's `Retry-After` when present (§7.6: honour it). Default: capped exponential timer.
	 */
	readonly backoff?: (attempt: number, retryAfterMs: number | undefined) => Promise<void>;
	/** Transient fetch failures tolerated per fetch before poisoning. Default: unlimited (the deadline governs). */
	readonly maxRetries?: number;
	/**
	 * Deadline of one fetch, in ms, measured with `now` (§7.6: Session-line reads retry transient
	 * failures for up to 30 s, then poison). Default 30 000.
	 */
	readonly readDeadlineMs?: number;
	/** Clock for `readDeadlineMs`. Default `Date.now`. */
	readonly now?: () => number;
	/**
	 * Widen a fetch for a miss at `[lo, hi)` (or the point `lo`) to a larger range that contains it,
	 * for example a whole per-conversation prefix (§4.5: the first requestId lookup in a conversation
	 * fetches all of `s.r/{conv}/`). The store clips the result to the gap around the miss, so the
	 * fetch never re-reads covered ranges and always makes progress. Default: no widening.
	 */
	readonly widen?: (lo: string, hi: string, point: boolean) => { readonly lo: string; readonly hi: string } | undefined;
}

const DEFAULT_BACKOFF = (attempt: number, retryAfterMs: number | undefined): Promise<void> =>
	new Promise((resolve) => setTimeout(resolve, retryAfterMs ?? Math.min(5000, 25 * 2 ** Math.min(attempt, 10))));

export class LocalStore implements StateStore {
	private readonly keyedState: KeyedStateTransport;
	private readonly cache = new SkipList<CachedRow>(0x51ed270b);
	/** Explicit covered ranges keyed by `lo`. */
	private readonly ranges = new SkipList<CoveredRange>(0x2545f491);
	/** LRU clock: each touch stamps the range with the next tick. */
	private tick = 0;
	/** Cached keys whose value exceeds `largeValueBytes`. */
	private readonly large = new Set<string>();
	private overlayAlerted = false;
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
	readonly metrics = {
		remoteReads: 0,
		retries: 0,
		stalePagesDiscarded: 0,
		commitWaits: 0,
		pagesMerged: 0,
		/** Explicit ranges dropped whole. */
		rangesEvicted: 0,
		/** Explicit ranges shrunk from their cold end (§7.5). */
		rangesShrunk: 0,
		/** Adjacent unpinned ranges coalesced into one (§7.5). */
		rangesCoalesced: 0,
		/** Rows with values over `largeValueBytes` evicted ahead of the LRU order (§7.5). */
		largeValuesEvicted: 0,
		/** Times the overlay grew past `overlayAlertBytes` (§7.5). */
		overlayAlerts: 0,
	};

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

	/** Bytes charged to the cache budget: cached rows plus explicit covered ranges' metadata (§7.5). */
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
					this.large.delete(k);
				});
			}
		}
		this.overlay.push({ ordinal, ops, bytes });
		this.overlayBytesTotal += bytes;
		this.next = ordinal + 1;
		if (!this.overlayAlerted && this.overlayBytesTotal >= (this.options.overlayAlertBytes ?? OVERLAY_ALERT_BYTES)) {
			this.overlayAlerted = true;
			this.metrics.overlayAlerts++;
			this.options.onOverlayAlert?.(this.overlayBytesTotal);
		}
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
		if (this.overlayBytesTotal < (this.options.overlayAlertBytes ?? OVERLAY_ALERT_BYTES)) this.overlayAlerted = false;
	}

	// ================================================================ eviction (§7.5)

	/**
	 * Evict until the cache holds at most `maxBytes` (§7.5):
	 * 1. rows of unpinned explicit ranges whose value exceeds `largeValueBytes`, largest first (the
	 *    range is split around the key: any sub-range of materialized state is still exact);
	 * 2. unpinned explicit ranges in LRU order, each shrunk from its cold end (away from the key a
	 *    read last touched) only as far as needed, and dropped once nothing of it is left;
	 * 3. fresh rows oldest-ID first by raising `F_fresh` (only while no read is running, because fresh
	 *    coverage cannot be pinned).
	 */
	evict(maxBytes: number): void {
		if (this.closed) return;
		if (this.cacheBytesTotal > maxBytes && this.large.size > 0) {
			const large = [...this.large]
				.map((key) => ({ key, bytes: this.cache.get(key)?.value.length ?? 0 }))
				.sort((a, b) => b.bytes - a.bytes);
			for (const { key } of large) {
				if (this.cacheBytesTotal <= maxBytes) return;
				const range = this.rangeAt(key);
				// Fresh-only rows leave through F_fresh; a pinned range is in use by a read.
				if (range === undefined || range.pins > 0 || isFreshKey(key, this.fresh)) continue;
				this.splitOut(range, key);
				this.metrics.largeValuesEvicted++;
			}
		}
		if (this.cacheBytesTotal > maxBytes) {
			const lru = [...this.ranges.entries()].map(([, r]) => r).filter((r) => r.pins === 0);
			lru.sort((a, b) => a.used - b.used);
			for (const range of lru) {
				if (this.cacheBytesTotal <= maxBytes) return;
				this.shrink(range, maxBytes);
			}
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

	/**
	 * Start complete-at-mint coverage at `floor` (§3.6 step 7: `F_fresh := m/next_id` after the claim).
	 * Allowed once, while no fresh coverage exists: it is sound only when no committed key has an ID
	 * component of `floor` or more, which the high-water rule of `m/next_id` guarantees (§7.3).
	 */
	startFresh(floor: number): void {
		this.assertUsable();
		if (Number.isFinite(this.fresh)) throw new Error(`LocalStore: fresh coverage already started at ${this.fresh}`);
		if (!Number.isFinite(floor) || floor < 0) throw new Error(`LocalStore: invalid fresh floor ${floor}`);
		// Defensive: a put in the overlay of a key that would be fresh but is not cached (a record
		// written without the high-water rule) raises the floor past it.
		let f = floor;
		for (let i = this.overlayHead; i < this.overlay.length; i++) {
			for (const op of this.overlay[i]?.ops ?? []) {
				if (op.op !== "p" || !isFreshKey(op.key, f) || this.rangeAt(op.key) !== undefined) continue;
				this.fresh = f;
				f = this.keepingId(op.key) + 1;
			}
		}
		this.fresh = f;
	}

	/**
	 * Merge a keyed-state page fetched by the caller for the range `[lo, hi)` (§3.5 step 3), for
	 * example open's `m/` read (§3.6 step 3). Returns "stale" when `D_resp < E` (the caller refetches)
	 * and "ahead" when `D_resp > tail` (the caller replays the log further, then merges again).
	 */
	mergePage(lo: string, hi: string, page: KeyedScanOutcome): "merged" | "stale" | "ahead" {
		this.assertUsable();
		const through = page.through;
		if (page.status !== 200 || through === undefined) throw new Error(`LocalStore: mergePage needs a 200 with Stream-Keyed-Through (got ${page.status})`);
		if (through < this.floor) return "stale";
		if (through > this.next) return "ahead";
		const pins = new Set<CoveredRange>();
		try {
			this.merge(lo, hi, false, page, through, pins);
		} finally {
			this.unpin(pins);
		}
		return "merged";
	}

	/**
	 * Install a derived point row (§7.4): `key` holds `value` in `state(tail)`, as the caller derived
	 * it from the row `source` during a read whose tail is still current (for example `t/{id}` from
	 * the covering `t.s/{st}/{id}` row, I25). Keys that are already covered are left alone. The row's
	 * informational `record` is the source row's.
	 */
	installDerived(key: string, value: string, source: string): void {
		this.assertUsable();
		if (this.coveredKey(key)) return;
		this.putRow(key, { record: this.cache.get(source)?.record ?? 0, value });
		const range: CoveredRange = { lo: key, hi: keySuccessor(key), pins: 0, used: ++this.tick, hot: key };
		this.addRange(range);
		this.coalesce(range);
		this.enforceBudget();
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
		this.removeRange(range);
		const drop: string[] = [];
		for (const [k] of this.cache.range(range.lo, range.hi)) if (!isFreshKey(k, this.fresh)) drop.push(k);
		for (const k of drop) this.deleteRow(k);
	}

	/** Remove `key` from the unpinned `range`, splitting it into `[lo, key)` and `(key, hi)`. */
	private splitOut(range: CoveredRange, key: string): void {
		this.removeRange(range);
		const after = keySuccessor(key);
		if (range.lo < key) this.addRange({ lo: range.lo, hi: key, pins: 0, used: range.used, hot: range.hot });
		if (range.hi === undefined || after < range.hi) this.addRange({ lo: after, hi: range.hi, pins: 0, used: range.used, hot: range.hot });
		this.deleteRow(key);
	}

	/**
	 * Shrink the unpinned `range` from its cold end until the cache holds at most `maxBytes` (§7.5):
	 * rows above its hot key go first, from the top down, then rows below it, from the bottom up;
	 * the range is dropped when that is not enough. What remains is a sub-range of materialized
	 * state, so it stays exact. Fresh-covered rows stay cached.
	 */
	private shrink(range: CoveredRange, maxBytes: number): void {
		const hot = range.hot !== undefined && range.hot >= range.lo && (range.hi === undefined || range.hot < range.hi) ? range.hot : range.lo;
		let hi = range.hi;
		const above = [...this.cache.range(keySuccessor(hot), range.hi)];
		for (let i = above.length - 1; i >= 0 && this.cacheBytesTotal > maxBytes; i--) {
			const k = (above[i] as [string, CachedRow])[0];
			hi = k;
			if (!isFreshKey(k, this.fresh)) this.deleteRow(k);
		}
		let lo = range.lo;
		if (this.cacheBytesTotal > maxBytes) {
			for (const [k] of [...this.cache.range(range.lo, hot)]) {
				if (this.cacheBytesTotal <= maxBytes) break;
				lo = keySuccessor(k);
				if (!isFreshKey(k, this.fresh)) this.deleteRow(k);
			}
		}
		if (this.cacheBytesTotal > maxBytes) {
			this.dropRange(range);
			return;
		}
		if (lo === range.lo && hi === range.hi) return;
		this.metrics.rangesShrunk++;
		this.removeRange(range);
		range.lo = lo;
		range.hi = hi;
		if (hi === undefined || lo < hi) this.addRange(range);
	}

	/** Merge the unpinned `range` with adjacent unpinned ranges (§7.5), keeping the newer LRU stamp and hot key. */
	private coalesce(range: CoveredRange): void {
		if (range.pins > 0 || this.ranges.get(range.lo) !== range) return;
		let merged = range;
		const left = this.ranges.lower(merged.lo)?.[1];
		if (left !== undefined && left.pins === 0 && left.hi === merged.lo) {
			this.removeRange(merged);
			this.absorb(left, merged);
			merged = left;
		}
		const right = merged.hi === undefined ? undefined : this.ranges.get(merged.hi);
		if (right !== undefined && right.pins === 0) {
			this.removeRange(right);
			this.absorb(merged, right);
		}
	}

	/** `into` (which precedes `from`) takes over `from`'s span. */
	private absorb(into: CoveredRange, from: CoveredRange): void {
		// `into` is re-added so its charge follows its new bound.
		this.removeRange(into);
		into.hi = from.hi;
		this.addRange(into);
		if (from.used > into.used) {
			into.used = from.used;
			into.hot = from.hot;
		}
		this.metrics.rangesCoalesced++;
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
			this.unpin(pins);
		}
	}

	/** Release a read's pins, then coalesce the ranges it leaves unpinned (§7.5). */
	private unpin(pins: Set<CoveredRange>): void {
		for (const r of pins) r.pins--;
		for (const r of pins) this.coalesce(r);
	}

	close(): void {
		this.closed = true;
		this.overlay = [];
		this.overlayHead = 0;
		this.overlayBytesTotal = 0;
		this.cache.deleteRange("", undefined);
		this.ranges.deleteRange("", undefined);
		this.large.clear();
		this.cacheBytesTotal = 0;
	}

	private view(pins: Set<CoveredRange>): StateView {
		const touch = (range: CoveredRange, key: string): void => {
			if (!pins.has(range)) {
				pins.add(range);
				range.pins++;
			}
			range.used = ++this.tick;
			range.hot = key;
		};
		const store = this;
		return {
			get: (key) => {
				const range = this.rangeAt(key);
				if (range !== undefined) touch(range, key);
				else if (!isFreshKey(key, this.fresh)) throw new CacheMiss(this, key, keySuccessor(key), true);
				return this.cache.get(key)?.value;
			},
			scan: function* (start, end) {
				let pos = start;
				while (pos < end) {
					const range = store.rangeAt(pos);
					let segmentEnd: string;
					if (range !== undefined) {
						touch(range, pos);
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

	/** The fetch range for a miss: the owner's widening, clipped to the gap around the miss. */
	private fetchRange(miss: CacheMiss): { lo: string; hi: string; point: boolean } {
		const wide = this.options.widen?.(miss.lo, miss.hi, miss.point);
		if (wide === undefined || !(wide.lo <= miss.lo && wide.hi >= miss.hi)) return { lo: miss.lo, hi: miss.hi, point: miss.point };
		let lo = wide.lo;
		let hi = wide.hi;
		// Do not reach back over a covered range: start right after the last one below the miss.
		const below = this.ranges.floor(miss.lo);
		if (below !== undefined) {
			const end = below[1].hi;
			if (end === undefined || end > miss.lo) return { lo: miss.lo, hi: miss.hi, point: miss.point };
			if (end > lo) lo = end;
		}
		// Do not reach forward into a covered range.
		const above = this.ranges.ceiling(keySuccessor(miss.lo));
		if (above !== undefined && above[0] < hi) hi = above[0];
		if (!(lo <= miss.lo && hi >= miss.hi && lo < hi)) return { lo: miss.lo, hi: miss.hi, point: miss.point };
		return { lo, hi, point: false };
	}

	/** Fetch the miss (widened, §4.5) until one acceptable page has been merged. */
	private async fill(miss: CacheMiss, pins: Set<CoveredRange>): Promise<void> {
		const limit = this.options.pageLimit ?? 256;
		const range = this.fetchRange(miss);
		const request: KeyedScanRequest = range.point
			? { key: b64(range.lo) }
			: { start: b64(range.lo), end: b64(range.hi), limit };
		const now = this.options.now ?? Date.now;
		const deadline = now() + (this.options.readDeadlineMs ?? 30_000);
		let failures = 0;
		for (;;) {
			this.assertUsable();
			let outcome: KeyedScanOutcome | undefined;
			const r = this.floor;
			this.metrics.remoteReads++;
			const started = performance.now();
			try {
				outcome = await this.keyedState.scan({ ...request, minThroughRecord: r });
			} catch (error) {
				if (!(error instanceof TransportError)) throw this.poison(error instanceof Error ? error : new Error(String(error)));
			}
			const latency = performance.now() - started;
			const s = outcome?.status;
			// Transient (§7.6): no response, 204, every 429 (the gateway's live-read limit sends no
			// Retry-After), 5xx, and a 400 from a lagging node whose Stream-Record-Next is below r
			// (r = E never exceeds the acknowledged tail).
			const lagging = s === 400 && outcome !== undefined && (intHeader(outcome.headers, H.recordNext) ?? r) < r;
			const transient = outcome === undefined || lagging || s === 204 || s === 429 || (s !== undefined && s >= 500);
			this.options.onRemoteRead?.(latency, transient);
			this.assertUsable();
			if (outcome === undefined || transient) {
				failures++;
				this.metrics.retries++;
				if (failures > (this.options.maxRetries ?? Number.POSITIVE_INFINITY) || now() >= deadline) {
					throw this.poison(new Error(`LocalStore: keyed-state read failed after ${failures} attempts (last status ${s ?? "none"})`));
				}
				await (this.options.backoff ?? DEFAULT_BACKOFF)(failures, outcome === undefined ? undefined : retryAfterMs(outcome.headers));
				continue;
			}
			if (s === 401 || s === 403) {
				throw this.poison(new Error(`LocalStore: keyed-state read was not authorized (${s}); refresh credentials and reopen`));
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
					this.merge(range.lo, range.hi, range.point, outcome, through, pins);
					return;
				}
				const wait = this.options.commitInFlight?.();
				if (wait === undefined) {
					throw this.poison(new FencedError(`keyed-state reflects record ${through - 1} above the local tail ${this.next}: another writer exists`));
				}
				this.metrics.commitWaits++;
				// A rejection aborts this read with that error: asking again could spin forever.
				await wait;
			}
		}
	}

	/** Merge one page at `D_resp` into the cache: `fold(page rows, overlay [D_resp, tail))` over its covered range. */
	private merge(lo: string, requestedHi: string, point: boolean, page: KeyedScanOutcome, through: number, pins: Set<CoveredRange>): void {
		let hi: string = requestedHi;
		if (!point && page.after !== undefined) {
			const after = unb64(page.after);
			if (after === undefined || after < lo || after >= requestedHi) throw this.poison(new Error("LocalStore: Stream-Keyed-After outside the requested range"));
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
				this.large.delete(k);
			});
			for (const [k, row] of merged.range(pos, gapEnd)) this.putRow(k, row);
			const installed: CoveredRange = { lo: pos, hi: gapEnd, pins: 1, used: ++this.tick, hot: pos };
			pins.add(installed);
			this.addRange(installed);
			pos = gapEnd;
		}
		this.enforceBudget();
	}

	// ================================================================ helpers

	/** Insert `range` under its `lo`, charging its metadata to the budget (§7.5). */
	private addRange(range: CoveredRange): void {
		this.ranges.set(range.lo, range);
		this.cacheBytesTotal += rangeBytes(range);
	}

	/** Remove `range` (under its current `lo`) and its charge. Mutate its bounds only while removed. */
	private removeRange(range: CoveredRange): void {
		if (this.ranges.get(range.lo) !== range) return;
		this.ranges.delete(range.lo);
		this.cacheBytesTotal -= rangeBytes(range);
	}

	private putRow(key: string, row: CachedRow): void {
		const prev = this.cache.set(key, row);
		if (prev !== undefined) this.cacheBytesTotal -= rowBytes(key, prev.value);
		this.cacheBytesTotal += rowBytes(key, row.value);
		if (row.value.length > (this.options.largeValueBytes ?? LARGE_VALUE_BYTES)) this.large.add(key);
		else if (prev !== undefined) this.large.delete(key);
	}

	private deleteRow(key: string): void {
		const prev = this.cache.delete(key);
		if (prev !== undefined) {
			this.cacheBytesTotal -= rowBytes(key, prev.value);
			this.large.delete(key);
		}
	}

	private poison(error: Error): Error {
		if (this.poisonError === undefined) {
			this.poisonError = error;
			this.options.onPoison?.(error);
		}
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

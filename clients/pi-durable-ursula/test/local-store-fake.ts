// M0e harness (design §10 M0e): a small fake keyed-state projection plus a deterministic scheduler.
//
// The projection folds the server log to a random, lagging, monotone D (P3.6), truncates range pages
// at random, and injects transient failures. Every scan is parked until the scheduler delivers it,
// and its page is computed either at request time or at delivery time, so a page may be stale by
// the time it is merged. The scheduler decides, between any two awaits of the store, whether to
// deliver a page, complete a flush-wait (raise E), evict, land or acknowledge a commit, or start a
// read.
import type { KeyedOp } from "../src/keyed-batch.ts";
import { OrderedMap } from "../src/ordered-map.ts";
import { H } from "../src/protocol.ts";
import { foldRecord, type Row } from "../src/state-store.ts";
import type { KeyedRow, KeyedScanOutcome, KeyedScanRequest, KeyedStateTransport } from "../src/transport.ts";
import { TransportError } from "../src/transport.ts";
import { b64, keySuccessor, unb64 } from "../src/tuple.ts";

/** Deterministic PRNG (mulberry32). */
export function prng(seed: number): { rnd: () => number; int: (n: number) => number; pick: <T>(xs: readonly T[]) => T; chance: (p: number) => boolean } {
	let a = seed >>> 0;
	const rnd = (): number => {
		a = (a + 0x6d2b79f5) >>> 0;
		let t = a;
		t = Math.imul(t ^ (t >>> 15), t | 1);
		t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
		return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
	};
	const int = (n: number): number => Math.floor(rnd() * n);
	return { rnd, int, pick: <T>(xs: readonly T[]): T => xs[int(xs.length)] as T, chance: (p) => rnd() < p };
}

interface Parked {
	readonly request: KeyedScanRequest;
	/** Set when the page was computed at request time. */
	early: KeyedScanOutcome | Error | undefined;
	readonly resolve: (o: KeyedScanOutcome) => void;
	readonly reject: (e: Error) => void;
}

export interface FaultRates {
	readonly unavailable: number;
	readonly noResponse: number;
	readonly timeout204: number;
}

export class FakeProjection implements KeyedStateTransport {
	/** The server log: every landed record, acknowledged or not. */
	readonly log: KeyedOp[][] = [];
	/** Published D (exclusive) and `state(D)`. */
	published = 0;
	private readonly state = new OrderedMap<Row>();
	readonly parked: Parked[] = [];
	requests = 0;
	private readonly r: ReturnType<typeof prng>;
	private readonly faults: FaultRates;

	constructor(r: ReturnType<typeof prng>, faults: FaultRates = { unavailable: 0.04, noResponse: 0.02, timeout204: 0.02 }) {
		this.r = r;
		this.faults = faults;
	}

	append(ops: KeyedOp[]): number {
		this.log.push(ops);
		return this.log.length - 1;
	}

	/** Fold the log up to `d` (never backwards). */
	publishTo(d: number): void {
		const to = Math.min(d, this.log.length);
		for (; this.published < to; this.published++) foldRecord(this.state, this.published, this.log[this.published] as KeyedOp[]);
	}

	/** Advance D to a random point in `[max(D, floor), log length]`. */
	lag(floor = 0): void {
		const lo = Math.max(this.published, floor);
		if (lo >= this.log.length) return this.publishTo(lo);
		this.publishTo(lo + this.r.int(this.log.length - lo + 1));
	}

	scan(request: KeyedScanRequest): Promise<KeyedScanOutcome> {
		this.requests++;
		return new Promise((resolve, reject) => {
			const p: Parked = { request, early: undefined, resolve, reject };
			if (this.r.chance(0.5)) p.early = this.compute(request);
			this.parked.push(p);
		});
	}

	/** Deliver one parked request (chosen by the scheduler). */
	deliver(index: number): void {
		const [p] = this.parked.splice(index, 1);
		if (p === undefined) return;
		const out = p.early ?? this.compute(p.request);
		if (out instanceof Error) p.reject(out);
		else p.resolve(out);
	}

	private compute(req: KeyedScanRequest): KeyedScanOutcome | Error {
		const f = this.faults;
		const roll = this.r.rnd();
		if (roll < f.noResponse) return new TransportError("connection", "fake reset");
		if (roll < f.noResponse + f.unavailable) return { status: 503, headers: { [H.retryAfter]: "0" }, rows: [] };
		const r = req.minThroughRecord ?? 0;
		if (r > this.log.length) return { status: 400, headers: { [H.recordNext]: String(this.log.length) }, rows: [] };
		if (roll < f.noResponse + f.unavailable + f.timeout204 && this.published < r) {
			return { status: 204, headers: { [H.keyedThrough]: String(this.published) }, rows: [], through: this.published };
		}
		this.lag(r);
		const through = this.published;
		const headers: Record<string, string> = { [H.keyedThrough]: String(through) };
		const rows: KeyedRow[] = [];
		if (req.key !== undefined) {
			const key = unb64(req.key) as string;
			const row = this.state.get(key);
			if (row !== undefined) rows.push({ key: req.key, record: row.record, value: row.value });
			return { status: 200, headers, rows, through };
		}
		const lo = req.start !== undefined ? (unb64(req.start) as string) : req.after !== undefined ? keySuccessor(unb64(req.after) as string) : "";
		const hi = req.end === undefined ? undefined : (unb64(req.end) as string);
		// Random truncation: a cut of 1..limit rows (the first row is always whole, P3.5).
		const limit = req.limit ?? 100;
		const cut = this.r.chance(0.5) ? limit : 1 + this.r.int(Math.min(limit, 4));
		let truncated = false;
		for (const [k, row] of this.state.range(lo, hi)) {
			if (rows.length >= cut) {
				truncated = true;
				break;
			}
			rows.push({ key: b64(k), record: row.record, value: row.value });
		}
		const last = rows.at(-1);
		// "MAY be present otherwise": sometimes name the last key even when the range is exhausted.
		if ((truncated || this.r.chance(0.1)) && last !== undefined) {
			headers[H.keyedAfter] = last.key;
			return { status: 200, headers, rows, through, after: last.key };
		}
		return { status: 200, headers, rows, through };
	}
}

/** Let every pending promise continuation run. Deterministic: no timers are involved. */
export const drain = (): Promise<void> => new Promise((resolve) => setImmediate(resolve));

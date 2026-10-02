// StateStore: the owner's view of `state(tail)` (design §3.5, §7.2).
//
// The Pi layer (pi-layer.ts) and the commit planner (planner.ts) never touch storage directly. They
// run as synchronous passes over a `StateView` handed out by `StateStore.read`. That contract is what
// lets the bounded owner of M3 replace the full-resident store below without touching the Pi layer:
//
// - `read(fn)` runs `fn` as ONE synchronous pass over `state(tail)` (I20). An implementation that
//   holds only part of the state (overlay [E, tail) + range cache, §7.2–§7.5) may abort the pass by
//   throwing from `view.get`/`view.scan` when a key or range is not covered, fetch that range from
//   keyed-state with `min_through_record = E`, merge it (§3.5 step 3), and run `fn` again. `fn`
//   must therefore be pure: no side effects other than its return value, and it must not catch
//   errors thrown by the view (Pi-layer code only throws its own validation errors).
// - `scan` is lazy: consumers stop at `limit + 1` matches, so a partial-state store only needs the
//   prefix of the range the pass actually reached.
// - `apply` is write-through of a confirmed record (after a 2xx or a successful read-back). Nothing
//   is applied at planning time, so a rejected or poisoned commit leaves no trace (§7.3).
//
// `FullResidentStateStore` is the M1 implementation (`E = 0`): every record from 0 is applied to
// one materialized ordered map, so every pass completes on the first run.
import type { KeyedOp } from "./keyed-batch.ts";
import { OrderedMap } from "./ordered-map.ts";

/** A visible row of `state(D)`: the ordinal of the record holding its last put, and its value text. */
export interface Row {
	readonly record: number;
	readonly value: string;
}

export interface StateView {
	/** Value text of `key` in `state(tail)`, or undefined when the key is not visible. */
	get(key: string): string | undefined;
	/** Visible rows in `[start, end)` in ascending octet order. Consumers may stop early. */
	scan(start: string, end: string): Iterable<readonly [key: string, value: string]>;
}

export interface StateStore {
	/** Exclusive: the store reflects records `[0, tail)`. */
	readonly tail: number;
	/** Run one synchronous, restartable read pass over `state(tail)`. */
	read<T>(fn: (view: StateView) => T): Promise<T>;
	/** Apply the confirmed record at ordinal `tail`; afterwards `tail = ordinal + 1`. */
	apply(ordinal: number, ops: readonly KeyedOp[]): void;
	/** Release memory; later reads reject. */
	close(): void;
}

/** Apply one record's ops in array order (§4.1 fold). */
export function foldRecord(map: OrderedMap<Row>, ordinal: number, ops: readonly KeyedOp[]): void {
	for (const op of ops) {
		if (op.op === "p") map.set(op.key, { record: ordinal, value: op.value });
		else if (op.op === "d") map.delete(op.key);
		else map.deleteRange(op.start, op.end);
	}
}

export class FullResidentStateStore implements StateStore {
	private readonly map = new OrderedMap<Row>();
	private next = 0;
	private closed = false;
	private readonly view: StateView;

	constructor() {
		const map = this.map;
		this.view = {
			get: (key) => map.get(key)?.value,
			scan: function* (start, end) {
				for (const [k, row] of map.range(start, end)) yield [k, row.value] as const;
			},
		};
	}

	get tail(): number {
		return this.next;
	}

	async read<T>(fn: (view: StateView) => T): Promise<T> {
		if (this.closed) throw new Error("UrsulaStorage is closed");
		return fn(this.view);
	}

	/** Synchronous variant for callers that already hold the store exclusively (open, planner tests). */
	readSync<T>(fn: (view: StateView) => T): T {
		if (this.closed) throw new Error("UrsulaStorage is closed");
		return fn(this.view);
	}

	apply(ordinal: number, ops: readonly KeyedOp[]): void {
		if (ordinal !== this.next) throw new Error(`StateStore apply out of order: ${ordinal} != ${this.next}`);
		foldRecord(this.map, ordinal, ops);
		this.next = ordinal + 1;
	}

	/** Every visible row, ascending: the applier-conformance view (I27). */
	rows(): [string, Row][] {
		return [...this.map.entries()];
	}

	close(): void {
		this.closed = true;
	}
}

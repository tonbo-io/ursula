// An ordered map over binary-string keys (octet order) with expected O(log n) point operations and
// O(log n + k) range operations: the bounded owner's replacement for the sorted-array OrderedMap
// (design §7.2 "Data structure"). A skip list rather than a B+tree: same asymptotics, a fraction of
// the code, and range deletion is a single splice per level.
//
// Levels come from a seeded xorshift generator, so the structure (and every test over it) is
// deterministic.

const MAX_LEVEL = 24;

interface Node<V> {
	readonly key: string;
	value: V;
	/** `next[i]` is the successor at level i; `next.length` is the node's height. */
	readonly next: (Node<V> | undefined)[];
}

export class SkipList<V> {
	private readonly head: Node<V> = { key: "", value: undefined as never, next: new Array(MAX_LEVEL).fill(undefined) };
	private level = 1;
	private count = 0;
	private seed: number;

	constructor(seed = 0x9e3779b9) {
		this.seed = seed | 0 || 1;
	}

	get size(): number {
		return this.count;
	}

	private randomLevel(): number {
		let lvl = 1;
		// xorshift32; two bits per level gives p = 1/4.
		let x = this.seed;
		x ^= x << 13;
		x ^= x >>> 17;
		x ^= x << 5;
		this.seed = x;
		let bits = x >>> 0;
		while (lvl < MAX_LEVEL && (bits & 3) === 0) {
			lvl++;
			bits >>>= 2;
			if (bits === 0) break;
		}
		return lvl;
	}

	/** Fills `update[i]` with the last node at level i whose key is < k; returns the level-0 successor. */
	private seek(k: string, update?: Node<V>[]): Node<V> | undefined {
		let x = this.head;
		for (let i = this.level - 1; i >= 0; i--) {
			for (let n = x.next[i]; n !== undefined && n.key < k; n = x.next[i]) x = n;
			if (update !== undefined) update[i] = x;
		}
		return x.next[0];
	}

	get(k: string): V | undefined {
		const n = this.seek(k);
		return n !== undefined && n.key === k ? n.value : undefined;
	}

	/** Sets `k`; returns the previous value. */
	set(k: string, v: V): V | undefined {
		const update: Node<V>[] = [];
		const n = this.seek(k, update);
		if (n !== undefined && n.key === k) {
			const prev = n.value;
			n.value = v;
			return prev;
		}
		const lvl = this.randomLevel();
		for (let i = this.level; i < lvl; i++) update[i] = this.head;
		if (lvl > this.level) this.level = lvl;
		const node: Node<V> = { key: k, value: v, next: new Array(lvl) };
		for (let i = 0; i < lvl; i++) {
			const u = update[i] as Node<V>;
			node.next[i] = u.next[i];
			u.next[i] = node;
		}
		this.count++;
		return undefined;
	}

	/** Deletes `k`; returns the removed value. */
	delete(k: string): V | undefined {
		const update: Node<V>[] = [];
		const n = this.seek(k, update);
		if (n === undefined || n.key !== k) return undefined;
		for (let i = 0; i < n.next.length; i++) (update[i] as Node<V>).next[i] = n.next[i];
		this.count--;
		while (this.level > 1 && this.head.next[this.level - 1] === undefined) this.level--;
		return n.value;
	}

	/** Deletes every key in `[start, end)` (`end` undefined: no upper bound), reporting each removal. */
	deleteRange(start: string, end: string | undefined, onRemove?: (key: string, value: V) => void): number {
		if (end !== undefined && !(start < end)) return 0;
		const update: Node<V>[] = [];
		let n = this.seek(start, update);
		let removed = 0;
		while (n !== undefined && (end === undefined || n.key < end)) {
			for (let i = 0; i < n.next.length; i++) {
				const u = update[i] as Node<V>;
				if (u.next[i] === n) u.next[i] = n.next[i];
			}
			onRemove?.(n.key, n.value);
			removed++;
			n = n.next[0];
		}
		this.count -= removed;
		while (this.level > 1 && this.head.next[this.level - 1] === undefined) this.level--;
		return removed;
	}

	/** The entry with the greatest key ≤ k. */
	floor(k: string): [string, V] | undefined {
		let x = this.head;
		for (let i = this.level - 1; i >= 0; i--) {
			for (let n = x.next[i]; n !== undefined && n.key <= k; n = x.next[i]) x = n;
		}
		return x === this.head ? undefined : [x.key, x.value];
	}

	/** The entry with the greatest key < k. */
	lower(k: string): [string, V] | undefined {
		const update: Node<V>[] = [];
		this.seek(k, update);
		const x = update[0];
		return x === undefined || x === this.head ? undefined : [x.key, x.value];
	}

	/** The entry with the smallest key ≥ k. */
	ceiling(k: string): [string, V] | undefined {
		const n = this.seek(k);
		return n === undefined ? undefined : [n.key, n.value];
	}

	/** Ascending iteration over `[start, end)`. Tolerates no mutation during iteration. */
	*range(start: string, end: string | undefined): Generator<[string, V]> {
		for (let n = this.seek(start); n !== undefined; n = n.next[0]) {
			if (end !== undefined && n.key >= end) return;
			yield [n.key, n.value];
		}
	}

	*entries(): Generator<[string, V]> {
		yield* this.range("", undefined);
	}
}
